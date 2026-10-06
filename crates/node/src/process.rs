//! Process-wide diagnostics, structured logging, cancellation, and exit policy.

mod backfill;
mod shutdown;

use std::{
    collections::BTreeMap,
    fs,
    io::IsTerminal,
    path::{Path, PathBuf},
    process::ExitCode,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use clap::Parser;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

pub(crate) use self::shutdown::ShutdownSignals;
use self::shutdown::{BackgroundTasks, forward_shutdown_signals, serve_until_shutdown};

use crate::{
    benchmark::{BenchmarkOptions, RealSourceBenchmarkOptions},
    cli::{
        BenchmarkCommand, Cli, Command, ConformanceCommand, DbCommand, E2eCommand, LogFormat,
        ProbeSource, ResetCommand, SourceCommand, SubscribeProtocol,
    },
    config::{
        ArtifactStorageBackend, Config, HistoryMaterialCoordinatorMode, ProcessorConfig,
        ValidationError,
    },
    processors::ProcessorRegistry,
};

/// Process outcome; each maps to a distinct exit code so scripts and agents
/// can tell failures apart without parsing stderr.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Exit {
    /// 0.
    Success,
    /// 1: a runtime failure.
    Failure,
    /// 3: `doctor` found the configuration unreadable or invalid. Exit 2 is
    /// clap's usage error.
    InvalidConfiguration,
    /// 124, as `timeout(1)`: `subscribe --once` ran out of time.
    TimedOut,
    /// 130, as a shell after SIGINT: `subscribe --once` was stopped by a
    /// signal before it printed a match.
    Interrupted,
}

pub(crate) fn select_processor_config<'a>(
    config: &'a Config,
    selector: &str,
) -> Result<&'a ProcessorConfig> {
    if let Some(configured) = config
        .processors
        .iter()
        .find(|configured| configured.instance == selector)
    {
        return Ok(configured);
    }
    let mut by_kind = config
        .processors
        .iter()
        .filter(|configured| configured.id == selector);
    let configured = by_kind
        .next()
        .with_context(|| format!("configuration does not enable processor {selector}"))?;
    if by_kind.next().is_some() {
        bail!("processor kind {selector} is ambiguous; select a processor instance");
    }
    Ok(configured)
}

fn historical_map_task_capacity(config: &Config) -> usize {
    if matches!(config.sources.live.kind, crate::config::LiveSourceKind::P2p) {
        config.budgets.mapper_concurrency.saturating_sub(1).max(1)
    } else {
        config.budgets.mapper_concurrency
    }
}

/// Per-open limits for a historical source read over `range`. The read may
/// acquire up to the temporary-disk budget over its life, and hold up to the
/// memory budget of raw input at once.
pub(crate) fn historical_source_budget(
    config: &Config,
    range: leani_primitives::BlockRange,
) -> leani_source_api::SourceBudget {
    leani_source_api::SourceBudget {
        max_input_bytes: config.budgets.temporary_disk_bytes,
        max_frame_bytes: config.budgets.memory_bytes.min(32 * 1_024 * 1_024),
        max_frames: range.len(),
        max_buffered_frames: config
            .budgets
            .mapper_concurrency
            .max(config.budgets.source_concurrency),
        max_in_flight_requests: config.budgets.source_concurrency,
        temporary_disk_bytes: config.budgets.temporary_disk_bytes,
        max_resident_bytes: config.budgets.memory_bytes,
    }
}

/// Limits for the live lane's subscription, which may also hold up to the
/// memory budget of raw input at once.
pub(crate) fn live_source_budget(config: &Config) -> leani_source_api::SourceBudget {
    leani_source_api::SourceBudget {
        max_input_bytes: config.budgets.memory_bytes,
        max_frame_bytes: config.budgets.memory_bytes.min(32 * 1_024 * 1_024),
        max_frames: 64,
        max_buffered_frames: 64,
        max_in_flight_requests: config.budgets.source_concurrency,
        temporary_disk_bytes: config.budgets.temporary_disk_bytes,
        max_resident_bytes: config.budgets.memory_bytes,
    }
}

/// Limits for a raw-history job's source reads: one segment's worth each,
/// holding up to the memory budget of raw input at once.
pub(crate) fn raw_history_source_budget(config: &Config) -> leani_source_api::SourceBudget {
    let raw = config.raw_history;
    leani_source_api::SourceBudget {
        max_input_bytes: raw.maximum_segment_logical_bytes.bytes(),
        max_frame_bytes: raw.maximum_frame_logical_bytes.bytes(),
        max_frames: raw.maximum_source_frames,
        max_buffered_frames: raw.maximum_buffered_frames,
        max_in_flight_requests: config.budgets.source_concurrency,
        temporary_disk_bytes: config.budgets.temporary_disk_bytes,
        max_resident_bytes: config.budgets.memory_bytes,
    }
}

/// Log what opening the raw-history store recovered. Retained segments it
/// dropped or found unavailable warn: their blocks are not served from it.
fn log_raw_history_recovery(recovery: leani_store_history::RecoveryReport) {
    if recovery.quarantined_corrupt_files > 0
        || recovery.missing_segments > 0
        || recovery.unavailable_segments > 0
    {
        warn!(
            ?recovery,
            "raw-history store opened without some retained segments"
        );
    } else if recovery != leani_store_history::RecoveryReport::default() {
        info!(?recovery, "raw-history store recovered interrupted work");
    }
}

pub(crate) fn configured_store_config(
    config: &Config,
    path: impl Into<std::path::PathBuf>,
) -> leani_store_sqlite::StoreConfig {
    let mut store = leani_store_sqlite::StoreConfig::new(path)
        .with_storage_budget(leani_store_sqlite::StoreStorageBudget {
            maximum_physical_bytes: config.budgets.store.maximum_physical_bytes.bytes(),
        })
        .with_artifact_budget(leani_store_sqlite::ArtifactStorageBudget {
            maximum_retained_bytes: config.budgets.artifacts.maximum_retained_bytes.bytes(),
            maximum_pending_bytes: config.budgets.artifacts.maximum_pending_bytes.bytes(),
        })
        .with_delivery_budget(leani_store_sqlite::DeliveryStorageBudget {
            maximum_retained_bytes: config.budgets.delivery.maximum_retained_bytes.bytes(),
            maximum_history_retained_bytes: config
                .budgets
                .delivery
                .maximum_history_retained_bytes
                .bytes(),
        });
    if config.artifact_storage.backend == ArtifactStorageBackend::TieredSegments {
        let artifacts = config.artifact_storage;
        store = store.with_artifact_segments(leani_store_sqlite::ArtifactSegmentStorageConfig {
            root: config.data_dir.join("processor-artifacts"),
            compression: artifacts.compression,
            target_blocks: artifacts.segment_target_blocks,
            maximum_artifact_logical_bytes: artifacts.maximum_artifact_logical_bytes.bytes(),
            maximum_segment_logical_bytes: artifacts.maximum_segment_logical_bytes.bytes(),
            maximum_segment_physical_bytes: artifacts.maximum_segment_physical_bytes.bytes(),
            // Segment files and SQLite share the same node-wide physical
            // ceiling; there is deliberately no second product budget.
            maximum_retained_physical_bytes: config.budgets.store.maximum_physical_bytes.bytes(),
        });
    }
    store
}

/// The descriptors a command registers after it opens the store, for
/// [`leani_store_sqlite::StoreConfig::with_processors`]: an older store that
/// holds one of them under an identity registration would refuse is then
/// refused before its upgrade.
pub(crate) fn processor_descriptors(
    processors: &[std::sync::Arc<dyn leani_processor_api::Processor>],
) -> Vec<leani_processor_api::ProcessorDescriptor> {
    processors
        .iter()
        .map(|processor| processor.descriptor().clone())
        .collect()
}

fn config_for_processor_descriptor<'a>(
    config: &'a Config,
    descriptor: &leani_processor_api::ProcessorDescriptor,
) -> Result<&'a ProcessorConfig> {
    config
        .processors
        .iter()
        .find(|configured| configured.instance == descriptor.instance.as_str())
        .with_context(|| {
            format!(
                "processor instance {} has no matching configuration",
                descriptor.instance
            )
        })
}

const HISTORICAL_BACKFILL_OUTCOME_KIND: &str = "historical_backfill_outcome";

#[derive(Clone, Debug, Deserialize, Serialize)]
struct StoredBackfillOutcome {
    job_id: String,
    finished_at_unix_ms: u64,
    report: Option<leani_runtime::BackfillReport>,
    error: Option<String>,
}

#[derive(Clone, Debug)]
struct OnDemandP2pBridge {
    source: leani_source_p2p::RethP2pSource,
    anchor: leani_source_p2p::P2pHistoryAnchor,
}

/// How often the durable job scheduler looks again at jobs that wait on
/// storage headroom or the P2P history bridge, or failed to resume for a
/// reason that may pass.
const DURABLE_JOB_RECHECK: Duration = Duration::from_secs(5);

/// How long a job waits for the P2P history bridge at most. Its first anchor
/// normally arrives seconds after a start. Past this, the job starts as it
/// did before the wait existed, so a recent one fails visibly instead of
/// waiting silently for an anchor that may never come.
const P2P_BRIDGE_WAIT_LIMIT: Duration = Duration::from_secs(120);

/// How long deleting a historical job waits for its task to end.
const HISTORICAL_JOB_STOP_WAIT: Duration = Duration::from_secs(10);

/// The task of one running historical job.
#[derive(Clone)]
struct RunningJob {
    cancellation: CancellationToken,
    /// Cancelled once the task has ended, after its last write for the job.
    ended: CancellationToken,
}

#[derive(Clone)]
struct NativeBackfillControl {
    config: Arc<Config>,
    store: leani_store_sqlite::SqliteStore,
    processors: Arc<Vec<Arc<dyn leani_processor_api::Processor>>>,
    cancellation: CancellationToken,
    tasks: Arc<tokio::sync::Mutex<BTreeMap<String, RunningJob>>>,
    /// Wakes the durable job scheduler: a job was created, or a job's task
    /// ended, which frees a slot and changes its job.
    jobs_changed: Arc<tokio::sync::Notify>,
    p2p_bridge: Arc<tokio::sync::RwLock<Option<OnDemandP2pBridge>>>,
    /// Whether the node's network lanes publish finalized anchors to
    /// `p2p_bridge`. A job the bridge would serve then waits for it, up to
    /// [`P2P_BRIDGE_WAIT_LIMIT`].
    p2p_bridge_expected: bool,
    /// When each waiting job's wait for the P2P history bridge began, or
    /// `None` once it ran out and the job may start without the bridge. Each
    /// wait, and each that runs out, is logged once.
    bridge_waits: Arc<std::sync::Mutex<BTreeMap<String, Option<tokio::time::Instant>>>>,
    raw_history_store: Option<leani_store_history::HistoryStore>,
    material_coordinator: Option<leani_runtime::HistoricalMaterialCoordinator>,
    pipeline_budget: leani_runtime::HistoricalPipelineBudget,
}

/// What one scheduler pass did with a durable job.
enum JobResume {
    /// Nothing more to do for now.
    Done,
    /// It waits for storage headroom, which no job change signals.
    WaitingForStorage,
    /// It waits, up to [`P2P_BRIDGE_WAIT_LIMIT`], for the P2P history
    /// bridge's finalized anchor to cover it, which no job change signals
    /// either.
    WaitingForBridge,
    /// Every backfill slot is taken; a task that ends wakes the scheduler.
    AtCapacity,
}

/// The historical runtime settings of a backfill over `sources` history
/// sources: the configured pipeline, and three attempts per source on a gap.
fn historical_runtime_config(
    config: &Config,
    sources: usize,
) -> leani_runtime::HistoricalRuntimeConfig {
    let pipeline = config.budgets.history_pipeline;
    leani_runtime::HistoricalRuntimeConfig {
        mapper_concurrency: config.budgets.mapper_concurrency,
        maximum_active_chunks: pipeline.maximum_active_chunks,
        maximum_mapped_bytes: pipeline.maximum_mapped_bytes.bytes(),
        commit_maximum_blocks: pipeline.commit.maximum_blocks,
        commit_maximum_changes: pipeline.commit.maximum_changes,
        commit_maximum_encoded_bytes: pipeline.commit.maximum_encoded_bytes.bytes(),
        commit_maximum_delay: Duration::from_millis(pipeline.commit.maximum_delay.milliseconds()),
        commit_target_writer_hold: Duration::from_millis(
            pipeline.commit.target_writer_hold.milliseconds(),
        ),
        max_attempts: u32::try_from(sources)
            .unwrap_or(u32::MAX)
            .saturating_mul(3)
            .max(leani_runtime::HistoricalRuntimeConfig::default().max_attempts),
        ..leani_runtime::HistoricalRuntimeConfig::default()
    }
}

impl std::fmt::Debug for NativeBackfillControl {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NativeBackfillControl")
            .field("processors", &self.processors.len())
            .field("cancelled", &self.cancellation.is_cancelled())
            .finish_non_exhaustive()
    }
}

impl NativeBackfillControl {
    fn new(
        config: Config,
        store: leani_store_sqlite::SqliteStore,
        processors: Vec<Arc<dyn leani_processor_api::Processor>>,
        cancellation: CancellationToken,
        raw_history_store: Option<leani_store_history::HistoryStore>,
        material_coordinator: Option<leani_runtime::HistoricalMaterialCoordinator>,
        pipeline_budget: leani_runtime::HistoricalPipelineBudget,
    ) -> Self {
        Self {
            config: Arc::new(config),
            store,
            processors: Arc::new(processors),
            cancellation,
            tasks: Arc::new(tokio::sync::Mutex::new(BTreeMap::new())),
            jobs_changed: Arc::new(tokio::sync::Notify::new()),
            p2p_bridge: Arc::new(tokio::sync::RwLock::new(None)),
            p2p_bridge_expected: false,
            bridge_waits: Arc::default(),
            raw_history_store,
            material_coordinator,
            pipeline_budget,
        }
    }

    /// Hold the jobs the on-demand P2P history bridge would serve until its
    /// finalized anchor covers them, up to [`P2P_BRIDGE_WAIT_LIMIT`], for a
    /// node whose network lanes publish that anchor. Set before the durable
    /// job scheduler's first pass.
    fn with_p2p_bridge_expected(mut self, expected: bool) -> Self {
        self.p2p_bridge_expected = expected;
        self
    }

    async fn update_p2p_bridge(
        &self,
        source: leani_source_p2p::RethP2pSource,
        anchor: leani_source_p2p::P2pHistoryAnchor,
    ) {
        let block = anchor.block.number.0;
        let mut current = self.p2p_bridge.write().await;
        if let Some(existing) = current.as_ref()
            && existing.anchor.block.number >= anchor.block.number
        {
            if existing.anchor.block.number == anchor.block.number
                && existing.anchor.block.hash != anchor.block.hash
            {
                warn!(
                    finalized_block = block,
                    current_hash = ?existing.anchor.block.hash,
                    incoming_hash = ?anchor.block.hash,
                    "ignoring conflicting on-demand P2P bridge anchor"
                );
            }
            return;
        }
        *current = Some(OnDemandP2pBridge { source, anchor });
        info!(
            finalized_block = block,
            "on-demand P2P history bridge anchor updated"
        );
    }

    async fn history_sources(
        &self,
        processor: &dyn leani_processor_api::Processor,
        requested: leani_primitives::BlockRange,
    ) -> Result<
        (
            Vec<Arc<dyn leani_source_api::HistorySource>>,
            leani_source_api::VerificationPolicy,
        ),
        leani_api::BackfillControlError,
    > {
        let bridge = self.p2p_bridge.read().await.clone();
        history_sources_with_bridge(
            &self.config,
            processor,
            self.raw_history_store.as_ref(),
            bridge.as_ref(),
            requested,
        )
        .map_err(|error| leani_api::BackfillControlError::Invalid(error.to_string()))
    }

    /// Whether `job` must wait before its sources are fixed, as
    /// [`bridge_wait`] decides, up to [`P2P_BRIDGE_WAIT_LIMIT`] after its
    /// first wait. That wait is logged, and so is its running out.
    async fn waits_for_p2p_bridge(
        &self,
        job: &leani_runtime::BackfillJob,
        configured: &ProcessorConfig,
    ) -> Result<bool, leani_api::BackfillControlError> {
        let bridge_anchor = self
            .p2p_bridge
            .read()
            .await
            .as_ref()
            .map(|bridge| bridge.anchor.block.number.0);
        // The fallback window ends at the bridge's anchor. Before the first
        // one, the finalized head stands in for it.
        let reference = match bridge_anchor {
            Some(anchor) => Some(anchor),
            None if self.p2p_bridge_expected => self
                .store
                .finalized_canonical_head(job.request.chain_id)
                .await
                .map_err(|error| leani_api::BackfillControlError::Internal(error.to_string()))?
                .map(|head| head.number.0),
            None => None,
        };
        let Some(reference) = reference else {
            return Ok(false);
        };
        let Some(through_block) = bridge_wait(
            self.p2p_bridge_expected,
            configured.require_retained_input,
            bridge_anchor,
            self.config
                .sources
                .live
                .history_fallback_start(reference, configured.start_block),
            job.request.range.end().0,
        ) else {
            // The bridge covers the job, or would not serve it.
            self.bridge_waits
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&job.id);
            return Ok(false);
        };
        let now = tokio::time::Instant::now();
        let mut waits = self
            .bridge_waits
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let waited = match waits.get(&job.id).copied() {
            None => {
                waits.insert(job.id.clone(), Some(now));
                drop(waits);
                info!(
                    job_id = %job.id,
                    through_block,
                    bridge_anchor,
                    "historical job waits for the P2P history bridge"
                );
                return Ok(true);
            }
            // Its wait ran out: it starts without the bridge.
            Some(None) => return Ok(false),
            Some(Some(since)) => now.duration_since(since),
        };
        if waited < P2P_BRIDGE_WAIT_LIMIT {
            return Ok(true);
        }
        waits.insert(job.id.clone(), None);
        drop(waits);
        warn!(
            job_id = %job.id,
            through_block,
            bridge_anchor,
            ?waited,
            "historical job starts without the P2P history bridge"
        );
        Ok(false)
    }

    fn processor(
        &self,
        selector: &str,
    ) -> Result<Arc<dyn leani_processor_api::Processor>, leani_api::BackfillControlError> {
        if let Some(processor) = self
            .processors
            .iter()
            .find(|processor| processor.descriptor().instance.as_str() == selector)
        {
            return Ok(processor.clone());
        }
        let mut by_kind = self
            .processors
            .iter()
            .filter(|processor| processor.descriptor().id.as_str() == selector);
        let processor = by_kind.next().cloned().ok_or_else(|| {
            leani_api::BackfillControlError::Invalid(format!(
                "processor {selector:?} is not configured"
            ))
        })?;
        if by_kind.next().is_some() {
            return Err(leani_api::BackfillControlError::Invalid(format!(
                "processor kind {selector:?} is ambiguous; select an instance"
            )));
        }
        Ok(processor)
    }

    fn validate_idempotency_key(key: &str) -> Result<(), leani_api::BackfillControlError> {
        if key.is_empty()
            || key.len() > 128
            || !key.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':')
            })
        {
            return Err(leani_api::BackfillControlError::Invalid(
                "idempotencyKey must contain 1-128 portable ASCII characters".to_owned(),
            ));
        }
        Ok(())
    }

    fn validate_historical_job_kind(
        record: &leani_store_sqlite::JobRecord,
    ) -> Result<(), leani_api::BackfillControlError> {
        if matches!(
            record.kind.as_str(),
            "materialization_job" | "backfill_subscription_job"
        ) {
            Ok(())
        } else {
            Err(leani_api::BackfillControlError::NotFound(record.id.clone()))
        }
    }

    fn historical_request_identity(
        request: &leani_api::CreateBackfillRequest,
        owner: leani_api::HistoricalWorkOwner,
        processor_instance: &str,
    ) -> Result<leani_primitives::BlockHash, leani_api::BackfillControlError> {
        let encoded =
            serde_json::to_vec(&(owner, processor_instance, request)).map_err(|error| {
                leani_api::BackfillControlError::Internal(format!(
                    "encode historical request identity: {error}"
                ))
            })?;
        Ok(leani_primitives::BlockHash::new(
            *blake3::hash(&encoded).as_bytes(),
        ))
    }

    fn normalized_request_ranges(
        request: &leani_api::CreateBackfillRequest,
        finalized_number: u64,
        finalized_hash: leani_primitives::BlockHash,
    ) -> Result<Vec<leani_primitives::BlockRange>, leani_api::BackfillControlError> {
        const MAX_REQUEST_RANGES: usize = 1_024;
        let mut ranges = if request.ranges.is_empty() {
            let (Some(from), Some(to)) = (request.from_block, request.to_block) else {
                return Err(leani_api::BackfillControlError::Invalid(
                    "provide either ranges or both fromBlock and toBlock".to_owned(),
                ));
            };
            vec![(from, to)]
        } else {
            if request.from_block.is_some() || request.to_block.is_some() {
                return Err(leani_api::BackfillControlError::Invalid(
                    "ranges cannot be combined with fromBlock or toBlock".to_owned(),
                ));
            }
            if request.ranges.len() > MAX_REQUEST_RANGES {
                return Err(leani_api::BackfillControlError::Invalid(format!(
                    "range set contains {} entries; maximum is {MAX_REQUEST_RANGES}",
                    request.ranges.len()
                )));
            }
            request
                .ranges
                .iter()
                .map(|range| (range.from_block, range.to_block))
                .collect::<Vec<_>>()
        };
        let mut ranges = ranges
            .drain(..)
            .map(|(from, upper)| {
                if from > finalized_number {
                    return Err(leani_api::BackfillControlError::RangeAfterFinalizedHead {
                        requested: from,
                        finalized: finalized_number,
                        finalized_hash,
                    });
                }
                let to = upper.resolve(finalized_number);
                if to > finalized_number {
                    return Err(leani_api::BackfillControlError::HistoryNotFinalized {
                        requested: to,
                        finalized: finalized_number,
                        finalized_hash,
                    });
                }
                leani_primitives::BlockRange::new(
                    leani_primitives::BlockNumber(from),
                    leani_primitives::BlockNumber(to),
                )
                .map_err(|error| leani_api::BackfillControlError::Invalid(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        ranges.sort_unstable_by_key(|range| (range.start(), range.end()));
        let mut normalized = Vec::<leani_primitives::BlockRange>::with_capacity(ranges.len());
        for range in ranges {
            if let Some(previous) = normalized.last_mut()
                && range.start().0 <= previous.end().0.saturating_add(1)
            {
                *previous = leani_primitives::BlockRange::new(
                    previous.start(),
                    previous.end().max(range.end()),
                )
                .map_err(|error| leani_api::BackfillControlError::Invalid(error.to_string()))?;
            } else {
                normalized.push(range);
            }
        }
        normalized.iter().try_fold(0_u64, |total, range| {
            total.checked_add(range.len()).ok_or_else(|| {
                leani_api::BackfillControlError::Invalid(
                    "requested range set block count overflows".to_owned(),
                )
            })
        })?;
        Ok(normalized)
    }

    fn api_mode(mode: leani_runtime::BackfillMode) -> leani_api::BackfillExecutionMode {
        match mode {
            leani_runtime::BackfillMode::FillMissing => {
                leani_api::BackfillExecutionMode::FillMissing
            }
            leani_runtime::BackfillMode::Recompute => leani_api::BackfillExecutionMode::Recompute,
        }
    }

    fn runtime_mode(mode: leani_api::BackfillExecutionMode) -> leani_runtime::BackfillMode {
        match mode {
            leani_api::BackfillExecutionMode::FillMissing => {
                leani_runtime::BackfillMode::FillMissing
            }
            leani_api::BackfillExecutionMode::Recompute => leani_runtime::BackfillMode::Recompute,
        }
    }

    fn api_state(state: leani_store_sqlite::JobState) -> leani_api::BackfillState {
        match state {
            leani_store_sqlite::JobState::Queued => leani_api::BackfillState::Queued,
            leani_store_sqlite::JobState::Running => leani_api::BackfillState::Running,
            leani_store_sqlite::JobState::StorageBackpressured => {
                leani_api::BackfillState::StorageBackpressured
            }
            leani_store_sqlite::JobState::Completed => leani_api::BackfillState::Completed,
            leani_store_sqlite::JobState::Failed => leani_api::BackfillState::Failed,
            leani_store_sqlite::JobState::Cancelled => leani_api::BackfillState::Cancelled,
        }
    }

    fn api_subscription_state(
        state: leani_store_sqlite::BackfillSubscriptionState,
    ) -> leani_api::BackfillState {
        match state {
            leani_store_sqlite::BackfillSubscriptionState::WaitingForConsumer => {
                leani_api::BackfillState::WaitingForConsumer
            }
            leani_store_sqlite::BackfillSubscriptionState::Queued => {
                leani_api::BackfillState::Queued
            }
            leani_store_sqlite::BackfillSubscriptionState::Running => {
                leani_api::BackfillState::Running
            }
            leani_store_sqlite::BackfillSubscriptionState::Backpressured => {
                leani_api::BackfillState::Backpressured
            }
            leani_store_sqlite::BackfillSubscriptionState::Draining => {
                leani_api::BackfillState::Draining
            }
            leani_store_sqlite::BackfillSubscriptionState::CompleteReclaimable => {
                leani_api::BackfillState::CompleteReclaimable
            }
            leani_store_sqlite::BackfillSubscriptionState::Cancelled => {
                leani_api::BackfillState::Cancelled
            }
            leani_store_sqlite::BackfillSubscriptionState::Failed => {
                leani_api::BackfillState::Failed
            }
        }
    }

    fn source_kind_name(kind: leani_primitives::SourceKind) -> &'static str {
        match kind {
            leani_primitives::SourceKind::PublicDataset => "public_dataset",
            leani_primitives::SourceKind::HistoryArchive => "history_archive",
            leani_primitives::SourceKind::RetainedHistory => "retained_history",
            leani_primitives::SourceKind::ExecutionP2p => "execution_p2p",
            leani_primitives::SourceKind::ConsensusP2p => "consensus_p2p",
            leani_primitives::SourceKind::BeaconApi => "beacon_api",
            leani_primitives::SourceKind::Synthetic => "synthetic",
        }
    }

    fn api_report(report: &leani_runtime::BackfillReport) -> leani_api::BackfillReport {
        let source_ids = if report.sources.is_empty() {
            report
                .source_id
                .split(',')
                .filter(|source| !source.is_empty())
                .map(ToOwned::to_owned)
                .collect()
        } else {
            report
                .sources
                .iter()
                .map(|source| source.source_id.clone())
                .collect()
        };
        leani_api::BackfillReport {
            source_ids,
            frames_mapped: report.frames_mapped,
            frames_committed: report.frames_committed,
            duplicate_frames: report.duplicate_frames,
            source_attempts: report.source_attempts,
            source_bytes: report.source_bytes,
            physical_source_bytes: report.physical_source_bytes,
            reused_source_bytes: report.reused_source_bytes,
            coalesced_frames: report.coalesced_frames,
            acquisition_ids: report.acquisition_ids.clone(),
            elapsed_milliseconds: report.elapsed_milliseconds,
            sources: report
                .sources
                .iter()
                .map(|source| leani_api::BackfillSourceReport {
                    source_id: source.source_id.clone(),
                    source_kind: Self::source_kind_name(source.source_kind).to_owned(),
                    attempts: source.attempts,
                    failures: source.failures,
                    frames_mapped: source.frames_mapped,
                    frames_committed: source.frames_committed,
                    duplicate_frames: source.duplicate_frames,
                    source_bytes: source.source_bytes,
                    physical_source_bytes: source.physical_source_bytes,
                    reused_source_bytes: source.reused_source_bytes,
                    coalesced_frames: source.coalesced_frames,
                    elapsed_milliseconds: source.elapsed_milliseconds,
                    last_error: source.last_error.clone(),
                })
                .collect(),
        }
    }

    fn outcome_id(job_id: &str) -> String {
        format!("{job_id}:outcome")
    }

    async fn outcome(
        &self,
        job_id: &str,
    ) -> Result<Option<StoredBackfillOutcome>, leani_api::BackfillControlError> {
        self.store
            .job(&Self::outcome_id(job_id))
            .await
            .map_err(|error| leani_api::BackfillControlError::Internal(error.to_string()))?
            .map(|record| {
                if record.kind != HISTORICAL_BACKFILL_OUTCOME_KIND {
                    return Err(leani_api::BackfillControlError::Internal(format!(
                        "backfill outcome {} has unexpected kind {}",
                        record.id, record.kind
                    )));
                }
                serde_json::from_slice(&record.payload).map_err(|error| {
                    leani_api::BackfillControlError::Internal(format!(
                        "backfill outcome {} is invalid: {error}",
                        record.id
                    ))
                })
            })
            .transpose()
    }

    async fn save_outcome(
        store: &leani_store_sqlite::SqliteStore,
        job_id: &str,
        state: leani_store_sqlite::JobState,
        report: Option<leani_runtime::BackfillReport>,
        error: Option<String>,
    ) -> Result<(), leani_store_sqlite::StoreError> {
        let finished_at_unix_ms = Self::now_milliseconds();
        let attempts = report.as_ref().map_or(0, |report| report.source_attempts);
        let outcome = StoredBackfillOutcome {
            job_id: job_id.to_owned(),
            finished_at_unix_ms,
            report,
            error,
        };
        let payload = serde_json::to_vec(&outcome).map_err(|error| {
            leani_store_sqlite::StoreError::Invariant(format!(
                "encode historical backfill outcome: {error}"
            ))
        })?;
        store
            .save_job(&leani_store_sqlite::JobRecord {
                id: Self::outcome_id(job_id),
                kind: HISTORICAL_BACKFILL_OUTCOME_KIND.to_owned(),
                state,
                payload,
                checkpoint: None,
                attempts,
                updated_at_unix_ms: finished_at_unix_ms,
            })
            .await
    }

    fn log_report(
        mode: &'static str,
        owner: leani_runtime::HistoricalJobOwner,
        report: &leani_runtime::BackfillReport,
    ) {
        let elapsed = report.elapsed_milliseconds.max(1);
        let requested_blocks = if report.requested_ranges.is_empty() {
            report.requested.len()
        } else {
            report
                .requested_ranges
                .iter()
                .fold(0_u64, |total, range| total.saturating_add(range.len()))
        };
        info!(
            backfill_mode = mode,
            historical_owner = ?owner,
            job_id = %report.job_id,
            processor = %report.processor_id,
            requested_from = report.requested.start().0,
            requested_to = report.requested.end().0,
            requested_blocks,
            source_ids = %report.source_id,
            source_attempts = report.source_attempts,
            frames_mapped = report.frames_mapped,
            frames_committed = report.frames_committed,
            duplicate_frames = report.duplicate_frames,
            input_bytes = report.source_bytes,
            physical_input_bytes = report.physical_source_bytes,
            reused_input_bytes = report.reused_source_bytes,
            coalesced_frames = report.coalesced_frames,
            acquisition_ids = ?report.acquisition_ids,
            elapsed_milliseconds = report.elapsed_milliseconds,
            frames_per_second_milli = report.frames_mapped.saturating_mul(1_000_000) / elapsed,
            input_bytes_per_second = report.source_bytes.saturating_mul(1_000) / elapsed,
            "processor historical backfill completed"
        );
        for source in &report.sources {
            info!(
                backfill_mode = mode,
                historical_owner = ?owner,
                job_id = %report.job_id,
                source_id = %source.source_id,
                source_kind = Self::source_kind_name(source.source_kind),
                attempts = source.attempts,
                failures = source.failures,
                frames_mapped = source.frames_mapped,
                frames_committed = source.frames_committed,
                duplicate_frames = source.duplicate_frames,
                input_bytes = source.source_bytes,
                physical_input_bytes = source.physical_source_bytes,
                reused_input_bytes = source.reused_source_bytes,
                coalesced_frames = source.coalesced_frames,
                elapsed_milliseconds = source.elapsed_milliseconds,
                last_error = source.last_error.as_deref().unwrap_or(""),
                "historical source contribution"
            );
        }
    }

    async fn record_success(
        store: &leani_store_sqlite::SqliteStore,
        mode: &'static str,
        owner: leani_runtime::HistoricalJobOwner,
        report: leani_runtime::BackfillReport,
    ) {
        Self::log_report(mode, owner, &report);
        let job_id = report.job_id.clone();
        if let Err(error) = Self::save_outcome(
            store,
            &job_id,
            leani_store_sqlite::JobState::Completed,
            Some(report),
            None,
        )
        .await
        {
            warn!(%job_id, %error, "failed to persist backfill outcome");
        }
        if owner == leani_runtime::HistoricalJobOwner::Subscription
            && let Err(error) = store
                .set_backfill_subscription_state(
                    &job_id,
                    leani_store_sqlite::BackfillSubscriptionState::Draining,
                    None,
                )
                .await
        {
            warn!(%job_id, %error, "failed to persist subscription draining state");
        }
    }

    async fn record_failure(
        store: &leani_store_sqlite::SqliteStore,
        mode: &'static str,
        job_id: &str,
        owner: leani_runtime::HistoricalJobOwner,
        state: leani_store_sqlite::JobState,
        error: Option<String>,
    ) {
        if state == leani_store_sqlite::JobState::Cancelled {
            info!(backfill_mode = mode, historical_owner = ?owner, %job_id, "processor historical backfill cancelled");
        } else if state == leani_store_sqlite::JobState::StorageBackpressured {
            warn!(
                backfill_mode = mode,
                historical_owner = ?owner,
                %job_id,
                error = error.as_deref().unwrap_or(""),
                "processor historical materialization paused at physical storage limit"
            );
        } else {
            warn!(
                backfill_mode = mode,
                historical_owner = ?owner,
                %job_id,
                error = error.as_deref().unwrap_or(""),
                "processor historical backfill failed"
            );
        }
        match store.job(job_id).await {
            Ok(Some(mut record)) => {
                record.state = state;
                record.updated_at_unix_ms = Self::now_milliseconds();
                if let Err(store_error) = store.save_job(&record).await {
                    warn!(
                        %job_id,
                        error = %store_error,
                        "failed to persist backfill scheduler state"
                    );
                }
            }
            Ok(None) => warn!(%job_id, "failed backfill has no durable scheduler record"),
            Err(store_error) => warn!(
                %job_id,
                error = %store_error,
                "failed to read scheduler record while persisting backfill failure"
            ),
        }
        if let Err(store_error) =
            Self::save_outcome(store, job_id, state, None, error.clone()).await
        {
            warn!(
                %job_id,
                error = %store_error,
                "failed to persist backfill outcome"
            );
        }
        if owner == leani_runtime::HistoricalJobOwner::Subscription {
            let subscription_state = if state == leani_store_sqlite::JobState::Cancelled {
                leani_store_sqlite::BackfillSubscriptionState::Cancelled
            } else {
                leani_store_sqlite::BackfillSubscriptionState::Failed
            };
            if let Err(store_error) = store
                .set_backfill_subscription_state(job_id, subscription_state, error.as_deref())
                .await
            {
                warn!(
                    %job_id,
                    error = %store_error,
                    "failed to persist backfill subscription outcome"
                );
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn status(
        &self,
        record: &leani_store_sqlite::JobRecord,
        outcome: Option<&StoredBackfillOutcome>,
    ) -> Result<leani_api::BackfillStatus, leani_api::BackfillControlError> {
        let job: leani_runtime::BackfillJob =
            serde_json::from_slice(&record.payload).map_err(|error| {
                leani_api::BackfillControlError::Internal(format!(
                    "durable backfill {} has an invalid payload: {error}",
                    record.id
                ))
            })?;
        let subscription = self
            .store
            .backfill_subscription_for_job(&record.id)
            .await
            .map_err(|error| leani_api::BackfillControlError::Internal(error.to_string()))?;
        let publication_revision = subscription
            .as_ref()
            .map(|subscription| subscription.publication_revision.to_string());
        let captured_finalized_target = Some(
            subscription
                .as_ref()
                .map_or(job.request.range.end().0, |subscription| {
                    subscription.captured_finalized_target.0
                }),
        );
        let ranges = job.requested_ranges().map_err(|error| {
            leani_api::BackfillControlError::Internal(format!(
                "durable backfill {} has an invalid range set: {error}",
                record.id
            ))
        })?;
        let requested_blocks = ranges
            .iter()
            .fold(0_u64, |total, range| total.saturating_add(range.len()));
        let processed_blocks = subscription
            .as_ref()
            .map(|subscription| subscription.processed_work_blocks)
            .or_else(|| {
                record.checkpoint.as_deref().and_then(|checkpoint| {
                    leani_runtime::historical_checkpoint_committed_blocks(checkpoint).ok()
                })
            })
            .or_else(|| {
                outcome
                    .and_then(|outcome| outcome.report.as_ref())
                    .map(|report| report.frames_committed)
            })
            .unwrap_or(0)
            .min(requested_blocks);
        // Only the job's own instance counts. `Self::processor` also selects
        // by kind, which would count another instance of the job's kind.
        let processor_configured = self
            .processors
            .iter()
            .any(|processor| processor.descriptor().instance.as_str() == job.processor_instance);
        Ok(leani_api::BackfillStatus {
            id: record.id.clone(),
            owner: match job.owner {
                leani_runtime::HistoricalJobOwner::Materialization => {
                    leani_api::HistoricalWorkOwner::Materialization
                }
                leani_runtime::HistoricalJobOwner::Subscription => {
                    leani_api::HistoricalWorkOwner::Subscription
                }
            },
            processor: job.processor_instance,
            processor_configured,
            consumer: subscription
                .as_ref()
                .map(|subscription| subscription.consumer_id.clone()),
            delivery_stream_id: job.delivery_stream_id,
            from_block: job.request.range.start().0,
            to_block: job.request.range.end().0,
            ranges: ranges
                .into_iter()
                .map(|range| leani_api::BackfillRange {
                    from_block: range.start().0,
                    to_block: range.end().0,
                })
                .collect(),
            requested_blocks,
            processed_blocks,
            remaining_blocks: requested_blocks.saturating_sub(processed_blocks),
            captured_finalized_target,
            mode: Self::api_mode(job.mode),
            batching: subscription.as_ref().map(|subscription| {
                let limits = subscription.delivery_batch_limits;
                leani_api::EffectiveBackfillBatching {
                    target_encoded_bytes: limits.target_encoded_bytes,
                    maximum_encoded_bytes: limits.maximum_encoded_bytes,
                    maximum_events: limits.maximum_events,
                    maximum_processed_blocks: limits.maximum_processed_blocks,
                    maximum_delay_ms: limits.maximum_delay_ms,
                    maximum_buffered_batches: limits.maximum_buffered_batches,
                    maximum_buffered_bytes: limits.maximum_buffered_bytes,
                    compression: match limits.compression {
                        leani_store_sqlite::BackfillDeliveryCompression::None => {
                            leani_api::DeliveryCompression::None
                        }
                        leani_store_sqlite::BackfillDeliveryCompression::Gzip => {
                            leani_api::DeliveryCompression::Gzip
                        }
                    },
                }
            }),
            publication_revision,
            state: subscription.as_ref().map_or_else(
                || Self::api_state(record.state),
                |subscription| Self::api_subscription_state(subscription.state),
            ),
            attempts: record.attempts,
            updated_at_unix_ms: record.updated_at_unix_ms,
            report: outcome
                .and_then(|outcome| outcome.report.as_ref())
                .map(Self::api_report),
            last_error: subscription.as_ref().map_or_else(
                || outcome.and_then(|outcome| outcome.error.clone()),
                |subscription| subscription.last_error.clone(),
            ),
        })
    }

    fn now_milliseconds() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX)
    }

    fn source_budget(&self, range: leani_primitives::BlockRange) -> leani_source_api::SourceBudget {
        historical_source_budget(&self.config, range)
    }

    async fn flush_tiered_artifacts(
        store: &leani_store_sqlite::SqliteStore,
        descriptor: &leani_processor_api::ProcessorDescriptor,
        ranges: &[leani_primitives::BlockRange],
        maximum_segments_per_pass: usize,
    ) -> Result<leani_store_sqlite::ArtifactTieringOutcome, leani_store_sqlite::StoreError> {
        let mut total = leani_store_sqlite::ArtifactTieringOutcome::default();
        for range in ranges {
            loop {
                let pass = store
                    .compact_processor_artifacts_to_segments(
                        descriptor,
                        *range,
                        maximum_segments_per_pass,
                        true,
                    )
                    .await?;
                total.segments = total.segments.saturating_add(pass.segments);
                total.artifacts = total.artifacts.saturating_add(pass.artifacts);
                total.logical_bytes = total.logical_bytes.saturating_add(pass.logical_bytes);
                total.inline_payload_bytes_reclaimed = total
                    .inline_payload_bytes_reclaimed
                    .saturating_add(pass.inline_payload_bytes_reclaimed);
                if pass.segments == 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        }
        Ok(total)
    }

    #[allow(clippy::too_many_lines)]
    async fn spawn(
        &self,
        job: leani_runtime::BackfillJob,
        processor: Arc<dyn leani_processor_api::Processor>,
    ) -> Result<(), leani_api::BackfillControlError> {
        if self.cancellation.is_cancelled() {
            return Err(leani_api::BackfillControlError::Unavailable(
                "node shutdown is in progress".to_owned(),
            ));
        }
        let mut tasks = self.tasks.lock().await;
        if tasks.contains_key(&job.id) {
            return Ok(());
        }
        if tasks.len() >= self.config.budgets.source_concurrency {
            return Err(leani_api::BackfillControlError::Unavailable(format!(
                "{} processor backfills are already active",
                tasks.len()
            )));
        }
        let (sources, verification_policy) = self
            .history_sources(processor.as_ref(), job.request.range)
            .await?;
        if job.request.verification_policy != verification_policy {
            return Err(leani_api::BackfillControlError::Conflict(
                "durable job verification policy differs from configured sources".to_owned(),
            ));
        }
        let tier_artifacts = self.config.artifact_storage.backend
            == ArtifactStorageBackend::TieredSegments
            && processor.descriptor().lifecycle.artifacts.mode
                == leani_processor_api::ArtifactPolicyMode::Full;
        let artifact_descriptor = processor.descriptor().clone();
        let artifact_ranges = if tier_artifacts {
            job.requested_ranges()
                .map_err(|error| leani_api::BackfillControlError::Invalid(error.to_string()))?
        } else {
            Vec::new()
        };
        let artifact_segments_per_pass = self.config.artifact_storage.maximum_segments_per_cycle;
        let runtime_config = historical_runtime_config(&self.config, sources.len());
        let runtime = leani_runtime::HistoricalRuntime::new_with_sources(
            self.store.clone(),
            sources,
            processor,
            runtime_config,
        )
        .map_err(|error| leani_api::BackfillControlError::Invalid(error.to_string()))?
        .with_pipeline_budget(self.pipeline_budget.clone());
        let runtime = if let Some(coordinator) = &self.material_coordinator {
            runtime.with_material_coordinator(coordinator.clone())
        } else {
            runtime
        };
        let token = self.cancellation.child_token();
        let ended = CancellationToken::new();
        tasks.insert(
            job.id.clone(),
            RunningJob {
                cancellation: token.clone(),
                ended: ended.clone(),
            },
        );
        drop(tasks);
        if job.owner == leani_runtime::HistoricalJobOwner::Subscription
            && let Err(error) = self
                .store
                .set_backfill_subscription_state(
                    &job.id,
                    leani_store_sqlite::BackfillSubscriptionState::Running,
                    None,
                )
                .await
        {
            self.tasks.lock().await.remove(&job.id);
            ended.cancel();
            return Err(leani_api::BackfillControlError::Internal(error.to_string()));
        }

        let budget = self.source_budget(job.request.range);
        let tasks = self.tasks.clone();
        let jobs_changed = self.jobs_changed.clone();
        let store = self.store.clone();
        let process_cancellation = self.cancellation.clone();
        let job_id = job.id.clone();
        let owner = job.owner;
        let job_task = (store.clone(), job_id.clone());
        let work = tokio::spawn(async move {
            match runtime.run(job, budget, token).await {
                Ok(report) => {
                    if tier_artifacts {
                        match Self::flush_tiered_artifacts(
                            &store,
                            &artifact_descriptor,
                            &artifact_ranges,
                            artifact_segments_per_pass,
                        )
                        .await
                        {
                            Ok(compacted) => {
                                info!(
                                    processor_instance = %artifact_descriptor.instance,
                                    segments = compacted.segments,
                                    artifacts = compacted.artifacts,
                                    logical_bytes = compacted.logical_bytes,
                                    reclaimed_inline_bytes = compacted.inline_payload_bytes_reclaimed,
                                    "flushed historical processor artifacts to segments"
                                );
                                Self::record_success(&store, "on_demand", owner, report).await;
                            }
                            Err(error) => {
                                Self::record_failure(
                                    &store,
                                    "on_demand",
                                    &job_id,
                                    owner,
                                    leani_store_sqlite::JobState::Failed,
                                    Some(format!("artifact segment flush failed: {error}")),
                                )
                                .await;
                            }
                        }
                    } else {
                        Self::record_success(&store, "on_demand", owner, report).await;
                    }
                }
                Err(leani_runtime::RuntimeError::Cancelled)
                    if process_cancellation.is_cancelled() =>
                {
                    info!(
                        %job_id,
                        historical_owner = ?owner,
                        "durable historical work suspended for node shutdown"
                    );
                }
                Err(leani_runtime::RuntimeError::Cancelled) => {
                    Self::record_failure(
                        &store,
                        "on_demand",
                        &job_id,
                        owner,
                        leani_store_sqlite::JobState::Cancelled,
                        None,
                    )
                    .await;
                }
                Err(
                    error @ leani_runtime::RuntimeError::Store(
                        leani_store_sqlite::StoreError::PhysicalStorageLimit { .. }
                        | leani_store_sqlite::StoreError::ArtifactStorageLimit { .. },
                    ),
                ) if owner == leani_runtime::HistoricalJobOwner::Materialization => {
                    Self::record_failure(
                        &store,
                        "on_demand",
                        &job_id,
                        owner,
                        leani_store_sqlite::JobState::StorageBackpressured,
                        Some(error.to_string()),
                    )
                    .await;
                }
                Err(error) => {
                    Self::record_failure(
                        &store,
                        "on_demand",
                        &job_id,
                        owner,
                        leani_store_sqlite::JobState::Failed,
                        Some(error.to_string()),
                    )
                    .await;
                }
            }
        });
        tokio::spawn(async move {
            let (store, job_id) = job_task;
            // Marks the job ended however its work ends: a deletion waits for it.
            let _ended = ended.drop_guard();
            // A panic fails the job, so that its slot frees and the scheduler
            // does not resume it into the same panic.
            if let Err(error) = work.await {
                Self::record_failure(
                    &store,
                    "on_demand",
                    &job_id,
                    owner,
                    leani_store_sqlite::JobState::Failed,
                    Some(format!("the job's task ended unexpectedly: {error}")),
                )
                .await;
            }
            tasks.lock().await.remove(&job_id);
            jobs_changed.notify_one();
        });
        Ok(())
    }

    /// One scheduler pass over the durable historical jobs. A job that cannot
    /// be read or resumed is skipped with a warning, so it never holds back
    /// the others. Returns whether a job waits on storage headroom or the P2P
    /// history bridge, or failed for a reason that may pass, so that the
    /// scheduler looks again even without a job change.
    async fn resume_durable_jobs(&self) -> Result<bool, leani_api::BackfillControlError> {
        let mut records = self
            .store
            .jobs(Some(
                leani_runtime::HistoricalJobOwner::Materialization.job_kind(),
            ))
            .await
            .map_err(|error| leani_api::BackfillControlError::Internal(error.to_string()))?;
        records.extend(
            self.store
                .jobs(Some(
                    leani_runtime::HistoricalJobOwner::Subscription.job_kind(),
                ))
                .await
                .map_err(|error| leani_api::BackfillControlError::Internal(error.to_string()))?,
        );
        records.sort_by_key(|record| record.updated_at_unix_ms);
        let mut recheck = false;
        for record in records {
            match self.resume_durable_job(&record).await {
                Ok(JobResume::Done) => {}
                Ok(JobResume::WaitingForStorage | JobResume::WaitingForBridge) => recheck = true,
                Ok(JobResume::AtCapacity) => break,
                Err(error) => {
                    warn!(
                        job_id = %record.id,
                        %error,
                        "durable historical job not resumed; resuming the others"
                    );
                    recheck |= matches!(error, leani_api::BackfillControlError::Internal(_));
                }
            }
        }
        Ok(recheck)
    }

    /// Resume one durable job, or move its completed subscription on.
    #[allow(clippy::too_many_lines)]
    async fn resume_durable_job(
        &self,
        record: &leani_store_sqlite::JobRecord,
    ) -> Result<JobResume, leani_api::BackfillControlError> {
        // Terminal jobs are skipped before their payload is decoded, so an
        // obsolete one costs nothing. Only a completed subscription still
        // delivers until its consumer acknowledged the completion.
        let subscription = match record.state {
            leani_store_sqlite::JobState::Queued
            | leani_store_sqlite::JobState::Running
            | leani_store_sqlite::JobState::StorageBackpressured => None,
            leani_store_sqlite::JobState::Completed
                if record.kind == leani_runtime::HistoricalJobOwner::Subscription.job_kind() =>
            {
                match self
                    .store
                    .backfill_subscription_for_job(&record.id)
                    .await
                    .map_err(|error| leani_api::BackfillControlError::Internal(error.to_string()))?
                {
                    Some(subscription)
                        if !matches!(
                            subscription.state,
                            leani_store_sqlite::BackfillSubscriptionState::CompleteReclaimable
                                | leani_store_sqlite::BackfillSubscriptionState::Cancelled
                                | leani_store_sqlite::BackfillSubscriptionState::Failed
                        ) =>
                    {
                        Some(subscription)
                    }
                    _ => return Ok(JobResume::Done),
                }
            }
            leani_store_sqlite::JobState::Completed
            | leani_store_sqlite::JobState::Failed
            | leani_store_sqlite::JobState::Cancelled => return Ok(JobResume::Done),
        };
        let job: leani_runtime::BackfillJob =
            serde_json::from_slice(&record.payload).map_err(|error| {
                leani_api::BackfillControlError::Invalid(format!(
                    "durable backfill {} has an invalid payload: {error}",
                    record.id
                ))
            })?;
        let processor = self.processor(&job.processor_instance)?;
        let configured = config_for_processor_descriptor(&self.config, processor.descriptor())
            .map_err(|error| leani_api::BackfillControlError::Invalid(error.to_string()))?;
        if job.owner == leani_runtime::HistoricalJobOwner::Materialization
            && configured.history_mode == crate::config::ProcessorHistoryMode::Automatic
        {
            // The hot/cold handoff owns this stable system job. It uses the
            // same durable materialization record, but must not also be
            // launched by the on-demand supervisor.
            return Ok(JobResume::Done);
        }
        if let Some(mut subscription) = subscription {
            if matches!(
                subscription.state,
                leani_store_sqlite::BackfillSubscriptionState::WaitingForConsumer
                    | leani_store_sqlite::BackfillSubscriptionState::Queued
                    | leani_store_sqlite::BackfillSubscriptionState::Running
                    | leani_store_sqlite::BackfillSubscriptionState::Backpressured
            ) {
                self.store
                    .append_backfill_completion(
                        processor.descriptor(),
                        &subscription.history_stream_id,
                        job.request.chain_id,
                        job.request.range.end(),
                    )
                    .await
                    .map_err(|error| {
                        leani_api::BackfillControlError::Internal(error.to_string())
                    })?;
                self.store
                    .set_backfill_subscription_state(
                        &record.id,
                        leani_store_sqlite::BackfillSubscriptionState::Draining,
                        None,
                    )
                    .await
                    .map_err(|error| {
                        leani_api::BackfillControlError::Internal(error.to_string())
                    })?;
                subscription = self
                    .store
                    .backfill_subscription_for_job(&record.id)
                    .await
                    .map_err(|error| leani_api::BackfillControlError::Internal(error.to_string()))?
                    .ok_or_else(|| {
                        leani_api::BackfillControlError::Internal(format!(
                            "durable subscription {} disappeared",
                            record.id
                        ))
                    })?;
            }
            if subscription.state == leani_store_sqlite::BackfillSubscriptionState::Draining
                && let Some(completion) = subscription.completion_sequence
                && let Some(consumer) = self
                    .store
                    .consumer_in_stream(
                        processor.descriptor(),
                        &subscription.history_stream_id,
                        &subscription.consumer_id,
                    )
                    .await
                    .map_err(|error| leani_api::BackfillControlError::Internal(error.to_string()))?
                && consumer.acknowledged_sequence >= completion
            {
                self.store
                    .mark_backfill_subscription_reclaimable(
                        &subscription.subscription_id,
                        consumer.acknowledged_sequence,
                    )
                    .await
                    .map_err(|error| {
                        leani_api::BackfillControlError::Internal(error.to_string())
                    })?;
            }
            return Ok(JobResume::Done);
        }
        if record.state == leani_store_sqlite::JobState::StorageBackpressured
            && !self
                .store
                .storage_below_low_water()
                .await
                .map_err(|error| leani_api::BackfillControlError::Internal(error.to_string()))?
        {
            return Ok(JobResume::WaitingForStorage);
        }
        if self.waits_for_p2p_bridge(&job, configured).await? {
            return Ok(JobResume::WaitingForBridge);
        }
        match self.spawn(job, processor).await {
            Ok(()) => Ok(JobResume::Done),
            Err(leani_api::BackfillControlError::Unavailable(message))
                if message.contains("processor backfills are already active") =>
            {
                Ok(JobResume::AtCapacity)
            }
            Err(error) => Err(error),
        }
    }

    /// Resume durable jobs at startup, and again only after a change: a job
    /// was created or a job's task ended. Jobs that wait on storage headroom
    /// or the P2P history bridge, or failed to resume for a reason that may
    /// pass, are looked at again every [`DURABLE_JOB_RECHECK`]. Passes run at
    /// most once a second.
    async fn supervise_durable_jobs(self: Arc<Self>) {
        loop {
            if self.cancellation.is_cancelled() {
                return;
            }
            let pass = tokio::time::Instant::now();
            let recheck = match self.resume_durable_jobs().await {
                Ok(recheck) => recheck,
                Err(error) => {
                    warn!(%error, "durable backfill scheduler pass failed");
                    true
                }
            };
            tokio::select! {
                () = self.cancellation.cancelled() => return,
                () = self.jobs_changed.notified() => {}
                () = tokio::time::sleep(DURABLE_JOB_RECHECK), if recheck => {}
            }
            tokio::select! {
                () = self.cancellation.cancelled() => return,
                () = tokio::time::sleep_until(pass + Duration::from_secs(1)) => {}
            }
        }
    }
}

#[async_trait::async_trait]
impl leani_runtime::FinalizedLiveGapRecovery for NativeBackfillControl {
    #[allow(clippy::too_many_lines)]
    async fn recover_chunk(
        &self,
        descriptor: &leani_processor_api::ProcessorDescriptor,
        range: leani_primitives::BlockRange,
    ) -> Result<Vec<leani_primitives::BlockFrame>, leani_runtime::RuntimeError> {
        use futures::StreamExt;

        let processor = self
            .processors
            .iter()
            .find(|processor| processor.descriptor().instance == descriptor.instance)
            .cloned()
            .ok_or_else(|| {
                leani_runtime::RuntimeError::InvalidConfig(format!(
                    "live-gap processor {} is not configured",
                    descriptor.instance
                ))
            })?;
        let (sources, verification_policy) = self
            .history_sources(processor.as_ref(), range)
            .await
            .map_err(|error| leani_runtime::RuntimeError::InvalidConfig(error.to_string()))?;
        let request = leani_runtime::BackfillJob::for_processor(
            format!(
                "live-recovery:{}:{}-{}",
                descriptor.instance,
                range.start().0,
                range.end().0
            ),
            processor.as_ref(),
            leani_primitives::ChainId(self.config.chain.chain_id),
            range,
            verification_policy,
        )?
        .request;
        let budget = self.source_budget(range).validate()?;
        let source_policy = leani_runtime::HistoricalSourcePolicy::from_sources(&sources);
        let cancellation = self.cancellation.child_token();
        let mut last_error = None;
        for source in sources {
            let attempt = async {
                let physical_request = self.material_coordinator.as_ref().map_or_else(
                    || request.clone(),
                    |coordinator| coordinator.physical_request(source.as_ref(), &request, budget),
                );
                let plan = source.plan(&physical_request).await?;
                plan.validate()?;
                let mut recovered = BTreeMap::new();
                for chunk in plan.chunks {
                    if chunk.range.end() < range.start() || chunk.range.start() > range.end() {
                        continue;
                    }
                    let stream = if let Some(coordinator) = &self.material_coordinator {
                        coordinator.open(
                            source_policy.clone(),
                            source.clone(),
                            &request,
                            chunk,
                            budget,
                            cancellation.clone(),
                        )?
                    } else {
                        leani_runtime::HistoricalMaterialCoordinator::standalone(
                            source.open(&chunk, budget, cancellation.clone()).await?,
                        )
                    };
                    tokio::pin!(stream);
                    while let Some(material) = stream.next().await {
                        let material = material?;
                        let frame = material.frame();
                        if frame.block.number >= range.start() && frame.block.number <= range.end()
                        {
                            if frame.finality != leani_primitives::Finality::Finalized {
                                return Err(leani_source_api::SourceError::CorruptFrame(format!(
                                    "live recovery source {} returned non-finalized block {}",
                                    source.descriptor().id,
                                    frame.block.number.0
                                )));
                            }
                            recovered.insert(frame.block.number.0, frame.clone());
                        }
                        material.acknowledge();
                    }
                }
                let frames = recovered.into_values().collect::<Vec<_>>();
                if u64::try_from(frames.len()).unwrap_or(u64::MAX) != range.len() {
                    return Err(leani_source_api::SourceError::Unavailable(format!(
                        "source {} recovered only {} of {} live-gap frames",
                        source.descriptor().id,
                        frames.len(),
                        range.len()
                    )));
                }
                Ok::<_, leani_source_api::SourceError>(frames)
            }
            .await;
            match attempt {
                Ok(frames) => return Ok(frames),
                Err(error) => {
                    warn!(
                        processor_instance = %descriptor.instance,
                        source_id = %source.descriptor().id,
                        from_block = range.start().0,
                        through_block = range.end().0,
                        %error,
                        "finalized live-gap recovery source failed; trying fallback"
                    );
                    last_error = Some(error);
                }
            }
        }
        Err(last_error
            .unwrap_or_else(|| {
                leani_source_api::SourceError::Unavailable(
                    "no historical source is available for live-gap recovery".to_owned(),
                )
            })
            .into())
    }
}

#[async_trait::async_trait]
impl leani_api::BackfillControl for NativeBackfillControl {
    fn historical_material_metrics(&self) -> Option<leani_api::HistoricalMaterialMetrics> {
        let snapshot = self.material_coordinator.as_ref()?.snapshot();
        Some(leani_api::HistoricalMaterialMetrics {
            acquisitions_started: snapshot.acquisitions_started,
            requests_coalesced: snapshot.requests_coalesced,
            requests_coalescible: snapshot.requests_coalescible,
            physical_frames: snapshot.physical_frames,
            physical_bytes: snapshot.physical_bytes,
            overfetched_frames: snapshot.overfetched_frames,
            overfetched_bytes: snapshot.overfetched_bytes,
            logical_frame_deliveries: snapshot.logical_frame_deliveries,
            active_acquisitions: snapshot.active_acquisitions,
            buffered_bytes: snapshot.buffered_bytes,
        })
    }

    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::too_many_lines
    )]
    async fn create_historical_work(
        &self,
        request: leani_api::CreateBackfillRequest,
        owner: leani_api::HistoricalWorkOwner,
    ) -> Result<leani_api::BackfillStatus, leani_api::BackfillControlError> {
        Self::validate_idempotency_key(&request.idempotency_key)?;
        let processor = self.processor(&request.processor)?;
        let configured = config_for_processor_descriptor(&self.config, processor.descriptor())
            .map_err(|error| leani_api::BackfillControlError::Internal(error.to_string()))?;
        match owner {
            leani_api::HistoricalWorkOwner::Materialization => {
                if configured.history_control != crate::config::ProcessorHistoryControl::NodeOwned {
                    return Err(leani_api::BackfillControlError::Conflict(
                        "processor history is owned by application subscriptions".to_owned(),
                    ));
                }
                if configured.history_mode != crate::config::ProcessorHistoryMode::OnDemand {
                    return Err(leani_api::BackfillControlError::Conflict(
                        "automatic_job_owns_history: explicit materialization requires on_demand history"
                            .to_owned(),
                    ));
                }
                if request.mode != leani_api::BackfillExecutionMode::FillMissing {
                    return Err(leani_api::BackfillControlError::Invalid(
                        "node-owned materialization currently supports fill_missing only"
                            .to_owned(),
                    ));
                }
                if request.consumer.is_some()
                    || request.limits.is_some()
                    || request.batching.is_some()
                {
                    return Err(leani_api::BackfillControlError::Invalid(
                        "materialization jobs do not accept consumer delivery settings".to_owned(),
                    ));
                }
            }
            leani_api::HistoricalWorkOwner::Subscription => {
                if configured.history_control
                    != crate::config::ProcessorHistoryControl::ApplicationSubscriptions
                {
                    return Err(leani_api::BackfillControlError::Conflict(
                        "processor history is owned by node materialization".to_owned(),
                    ));
                }
                if processor.descriptor().delivery_ordering
                    != leani_processor_api::DeliveryOrdering::BlockVersionedIdempotent
                {
                    return Err(leani_api::BackfillControlError::Invalid(
                        "application subscriptions currently require block-versioned delivery"
                            .to_owned(),
                    ));
                }
            }
        }
        if processor.descriptor().mode != leani_processor_api::ReductionMode::BlockLocal {
            return Err(leani_api::BackfillControlError::Invalid(
                "on-demand historical jobs currently require a block-local processor".to_owned(),
            ));
        }
        let minimum = match &processor.descriptor().start {
            leani_processor_api::StartPoint::Genesis => 0,
            leani_processor_api::StartPoint::Block(block) => block.0,
            leani_processor_api::StartPoint::ProcessorCheckpoint(_) => {
                return Err(leani_api::BackfillControlError::Invalid(
                    "checkpoint-seeded processors do not accept independent block ranges"
                        .to_owned(),
                ));
            }
        };
        let owner_prefix = match owner {
            leani_api::HistoricalWorkOwner::Materialization => "materialization",
            leani_api::HistoricalWorkOwner::Subscription => "subscription",
        };
        let id = format!(
            "{owner_prefix}:{}:{}",
            processor.descriptor().instance,
            request.idempotency_key
        );
        let request_identity = Self::historical_request_identity(
            &request,
            owner,
            processor.descriptor().instance.as_str(),
        )?;
        if let Some(record) = self
            .store
            .job(&id)
            .await
            .map_err(|error| leani_api::BackfillControlError::Internal(error.to_string()))?
        {
            Self::validate_historical_job_kind(&record)?;
            let stored_identity = self
                .store
                .historical_work_identity(&id)
                .await
                .map_err(|error| leani_api::BackfillControlError::Internal(error.to_string()))?;
            if stored_identity != Some(request_identity) {
                return Err(leani_api::BackfillControlError::Conflict(format!(
                    "idempotency key {:?} is already used by another request",
                    request.idempotency_key
                )));
            }
            let outcome = self.outcome(&record.id).await?;
            return self.status(&record, outcome.as_ref()).await;
        }
        // Only here: a subscription created before the credential rule
        // re-submits its credential and gets its status above. A new
        // subscription's consumer is refused before its history stream exists.
        if let Some(consumer) = request.consumer.as_ref() {
            leani_store_sqlite::validate_consumer_registration(
                &consumer.id,
                Duration::from_secs(consumer.lease_ttl_seconds),
            )
            .map_err(leani_api::BackfillControlError::Invalid)?;
        }
        if let Some(credential) = request
            .consumer
            .as_ref()
            .and_then(|consumer| consumer.credential.as_deref())
        {
            leani_store_sqlite::validate_consumer_credential(credential)
                .map_err(leani_api::BackfillControlError::Invalid)?;
        }
        let chain_id = leani_primitives::ChainId(self.config.chain.chain_id);
        let finalized_head = self
            .store
            .finalized_canonical_head(chain_id)
            .await
            .map_err(|error| leani_api::BackfillControlError::Internal(error.to_string()))?
            .ok_or_else(|| {
                leani_api::BackfillControlError::Unavailable(
                    "the chain finalized head is not available yet".to_owned(),
                )
            })?;
        let ranges = Self::normalized_request_ranges(
            &request,
            finalized_head.number.0,
            finalized_head.hash,
        )?;
        let mut preexisting_coverage = Vec::new();
        let mut recompute_gaps = Vec::new();
        for requested in &ranges {
            let covered = self
                .store
                .finalized_coverage(processor.descriptor(), *requested)
                .await
                .map_err(|error| leani_api::BackfillControlError::Internal(error.to_string()))?;
            if request.mode == leani_api::BackfillExecutionMode::Recompute {
                recompute_gaps.extend(
                    leani_source_api::coverage_gaps(*requested, &covered)
                        .into_iter()
                        .map(|gap| leani_api::BackfillRange {
                            from_block: gap.start().0,
                            to_block: gap.end().0,
                        }),
                );
            }
            preexisting_coverage.extend(covered);
        }
        if !recompute_gaps.is_empty() {
            return Err(leani_api::BackfillControlError::RecomputeCoverageMissing {
                gaps: recompute_gaps,
            });
        }
        let range = leani_primitives::BlockRange::new(
            ranges
                .first()
                .expect("normalized range set is non-empty")
                .start(),
            ranges
                .last()
                .expect("normalized range set is non-empty")
                .end(),
        )
        .map_err(|error| leani_api::BackfillControlError::Invalid(error.to_string()))?;
        if range.start().0 < minimum {
            return Err(leani_api::BackfillControlError::Invalid(format!(
                "requested block {} predates processor minimum {}",
                range.start().0,
                minimum
            )));
        }
        let requested_blocks = ranges
            .iter()
            .fold(0_u64, |total, range| total.saturating_add(range.len()));
        let (_, verification_policy) = self.history_sources(processor.as_ref(), range).await?;
        let mut job = leani_runtime::BackfillJob::for_processor_ranges(
            id.clone(),
            processor.as_ref(),
            chain_id,
            ranges.clone(),
            verification_policy,
        )
        .map_err(|error| leani_api::BackfillControlError::Invalid(error.to_string()))?;
        job.owner = match owner {
            leani_api::HistoricalWorkOwner::Materialization => {
                leani_runtime::HistoricalJobOwner::Materialization
            }
            leani_api::HistoricalWorkOwner::Subscription => {
                leani_runtime::HistoricalJobOwner::Subscription
            }
        };
        job.mode = Self::runtime_mode(request.mode);
        let (effective_block_limit, effective_byte_limit, resume_below_ratio_millionths) =
            if let Some(limits) = request.limits {
                if limits.max_unacknowledged_blocks == 0
                    || limits.max_unacknowledged_bytes == 0
                    || !limits.resume_below_ratio.is_finite()
                    || limits.resume_below_ratio <= 0.0
                    || limits.resume_below_ratio >= 1.0
                {
                    return Err(leani_api::BackfillControlError::Invalid(
                        "backfill limits must be positive and resumeBelowRatio must be between zero and one"
                            .to_owned(),
                    ));
                }
                let scaled_ratio = (limits.resume_below_ratio * 1_000_000.0).round();
                let ratio = u32::try_from(scaled_ratio as u64).map_err(|_| {
                    leani_api::BackfillControlError::Invalid(
                        "resumeBelowRatio is outside the supported precision".to_owned(),
                    )
                })?;
                if ratio == 0 || ratio >= 1_000_000 {
                    return Err(leani_api::BackfillControlError::Invalid(
                        "resumeBelowRatio must remain between zero and one at six-digit precision"
                            .to_owned(),
                    ));
                }
                (
                    limits.max_unacknowledged_blocks.min(16_384),
                    limits
                        .max_unacknowledged_bytes
                        .min(processor.descriptor().lifecycle.delivery.max_bytes),
                    ratio,
                )
            } else {
                (
                    requested_blocks.clamp(1, 16_384),
                    processor.descriptor().lifecycle.delivery.max_bytes,
                    750_000,
                )
            };
        let configured_batching = self.config.api.delivery.history_batches;
        let operator_batching = leani_store_sqlite::BackfillDeliveryBatchLimits {
            target_encoded_bytes: configured_batching.target_encoded_bytes.bytes(),
            maximum_encoded_bytes: configured_batching.maximum_encoded_bytes.bytes(),
            maximum_events: configured_batching.maximum_events,
            maximum_processed_blocks: configured_batching.maximum_processed_blocks,
            maximum_delay_ms: configured_batching.maximum_delay.milliseconds(),
            maximum_buffered_batches: configured_batching.maximum_buffered_batches,
            maximum_buffered_bytes: configured_batching.maximum_buffered_bytes.bytes(),
            compression: match configured_batching.compression {
                crate::config::DeliveryCompressionConfig::None => {
                    leani_store_sqlite::BackfillDeliveryCompression::None
                }
                crate::config::DeliveryCompressionConfig::Gzip => {
                    leani_store_sqlite::BackfillDeliveryCompression::Gzip
                }
            },
        };
        let delivery_batch_limits = request.batching.map_or(operator_batching, |batching| {
            leani_store_sqlite::BackfillDeliveryBatchLimits {
                target_encoded_bytes: batching
                    .target_encoded_bytes
                    .unwrap_or(operator_batching.target_encoded_bytes),
                maximum_encoded_bytes: batching
                    .maximum_encoded_bytes
                    .unwrap_or(operator_batching.maximum_encoded_bytes),
                maximum_events: batching
                    .maximum_events
                    .unwrap_or(operator_batching.maximum_events),
                maximum_processed_blocks: batching
                    .maximum_processed_blocks
                    .unwrap_or(operator_batching.maximum_processed_blocks),
                maximum_delay_ms: batching
                    .maximum_delay_ms
                    .unwrap_or(operator_batching.maximum_delay_ms),
                maximum_buffered_batches: operator_batching.maximum_buffered_batches,
                maximum_buffered_bytes: operator_batching.maximum_buffered_bytes,
                compression: batching.compression.map_or(
                    operator_batching.compression,
                    |compression| match compression {
                        leani_api::DeliveryCompression::None => {
                            leani_store_sqlite::BackfillDeliveryCompression::None
                        }
                        leani_api::DeliveryCompression::Gzip => {
                            leani_store_sqlite::BackfillDeliveryCompression::Gzip
                        }
                    },
                ),
            }
        });
        if delivery_batch_limits.target_encoded_bytes == 0
            || delivery_batch_limits.maximum_encoded_bytes
                < delivery_batch_limits.target_encoded_bytes
            || delivery_batch_limits.maximum_encoded_bytes > operator_batching.maximum_encoded_bytes
            || delivery_batch_limits.maximum_events == 0
            || delivery_batch_limits.maximum_events > operator_batching.maximum_events
            || delivery_batch_limits.maximum_processed_blocks == 0
            || delivery_batch_limits.maximum_processed_blocks
                > operator_batching.maximum_processed_blocks
            || delivery_batch_limits.maximum_delay_ms == 0
            || delivery_batch_limits.maximum_delay_ms > operator_batching.maximum_delay_ms
            || (delivery_batch_limits.compression
                == leani_store_sqlite::BackfillDeliveryCompression::Gzip
                && operator_batching.compression
                    == leani_store_sqlite::BackfillDeliveryCompression::None)
        {
            return Err(leani_api::BackfillControlError::Invalid(
                "backfill batching values must be positive, internally consistent, and no larger than operator maxima"
                    .to_owned(),
            ));
        }
        let mut subscription_consumer = None;
        if owner == leani_api::HistoricalWorkOwner::Subscription
            && processor.descriptor().delivery_ordering
                == leani_processor_api::DeliveryOrdering::BlockVersionedIdempotent
        {
            let configured_consumers = if let Some(consumer) = request.consumer.as_ref() {
                if consumer.role != leani_store_sqlite::ConsumerRole::Required {
                    return Err(leani_api::BackfillControlError::Invalid(
                        "an application-created backfill subscription requires a required consumer"
                            .to_owned(),
                    ));
                }
                vec![(
                    consumer.id.clone(),
                    consumer.role,
                    consumer.lease_ttl_seconds,
                    consumer.credential.clone(),
                )]
            } else {
                processor
                    .descriptor()
                    .lifecycle
                    .delivery
                    .consumers
                    .iter()
                    .map(|consumer| {
                        (
                            consumer.id.clone(),
                            if consumer.required {
                                leani_store_sqlite::ConsumerRole::Required
                            } else {
                                leani_store_sqlite::ConsumerRole::BestEffort
                            },
                            consumer.lease_ttl_seconds,
                            None,
                        )
                    })
                    .collect::<Vec<_>>()
            };
            if !configured_consumers
                .iter()
                .any(|(_, role, _, _)| *role == leani_store_sqlite::ConsumerRole::Required)
            {
                return Err(leani_api::BackfillControlError::Invalid(
                    "split backfill delivery requires at least one required consumer".to_owned(),
                ));
            }
            let stream = self
                .store
                .create_backfill_delivery_stream(processor.descriptor(), &id)
                .await
                .map_err(|error| leani_api::BackfillControlError::Internal(error.to_string()))?;
            for (consumer_id, role, lease_ttl_seconds, credential) in &configured_consumers {
                if let Some(existing) = self
                    .store
                    .consumer_in_stream(processor.descriptor(), &stream.stream_id, consumer_id)
                    .await
                    .map_err(|error| leani_api::BackfillControlError::Internal(error.to_string()))?
                {
                    if existing.role != *role {
                        return Err(leani_api::BackfillControlError::Conflict(format!(
                            "consumer {:?} already has role {:?} in stream {}",
                            consumer_id, existing.role, stream.stream_id
                        )));
                    }
                    if let Some(credential) = credential
                        && !self
                            .store
                            .consumer_credential_matches_in_stream(
                                processor.descriptor(),
                                &stream.stream_id,
                                consumer_id,
                                credential,
                            )
                            .await
                            .map_err(|error| {
                                leani_api::BackfillControlError::Internal(error.to_string())
                            })?
                    {
                        return Err(leani_api::BackfillControlError::Conflict(format!(
                            "consumer {consumer_id:?} credential differs from the existing subscription"
                        )));
                    }
                } else {
                    if let Some(credential) = credential {
                        self.store
                            .create_consumer_with_credential_in_stream(
                                processor.descriptor(),
                                &stream.stream_id,
                                consumer_id,
                                *role,
                                leani_store_sqlite::ConsumerStartPosition::EarliestRetained,
                                std::time::Duration::from_secs(*lease_ttl_seconds),
                                credential,
                            )
                            .await
                    } else {
                        self.store
                            .create_consumer_in_stream(
                                processor.descriptor(),
                                &stream.stream_id,
                                consumer_id,
                                *role,
                                leani_store_sqlite::ConsumerStartPosition::EarliestRetained,
                                std::time::Duration::from_secs(*lease_ttl_seconds),
                            )
                            .await
                    }
                    .map_err(|error| {
                        leani_api::BackfillControlError::Internal(error.to_string())
                    })?;
                }
            }
            let required_consumer = configured_consumers
                .iter()
                .find(|(_, role, _, _)| *role == leani_store_sqlite::ConsumerRole::Required)
                .expect("required consumer was validated");
            subscription_consumer = Some((stream.stream_id.clone(), required_consumer.0.clone()));
            job.delivery_stream_id = Some(stream.stream_id);
        } else if request.consumer.is_some()
            || request.limits.is_some()
            || request.batching.is_some()
        {
            return Err(leani_api::BackfillControlError::Invalid(
                "consumer and per-subscription limits require block-versioned delivery".to_owned(),
            ));
        }
        let payload = serde_json::to_vec(&job).map_err(|error| {
            leani_api::BackfillControlError::Internal(format!("encode backfill request: {error}"))
        })?;

        let requested_record = leani_store_sqlite::JobRecord {
            id: id.clone(),
            kind: job.owner.job_kind().to_owned(),
            state: leani_store_sqlite::JobState::Queued,
            payload: payload.clone(),
            checkpoint: None,
            attempts: 0,
            updated_at_unix_ms: Self::now_milliseconds(),
        };
        let record = if let Some((history_stream_id, consumer_id)) = subscription_consumer {
            self.store
                .create_backfill_subscription_job(
                    &leani_store_sqlite::BackfillSubscriptionRecord {
                        subscription_id: id.clone(),
                        job_id: id.clone(),
                        processor_instance: processor.descriptor().instance.to_string(),
                        history_stream_id,
                        mode: match request.mode {
                            leani_api::BackfillExecutionMode::FillMissing => {
                                leani_store_sqlite::BackfillSubscriptionMode::FillMissing
                            }
                            leani_api::BackfillExecutionMode::Recompute => {
                                leani_store_sqlite::BackfillSubscriptionMode::Recompute
                            }
                        },
                        publication_revision: 0,
                        state: leani_store_sqlite::BackfillSubscriptionState::Queued,
                        last_error: None,
                        consumer_id,
                        ranges: ranges.clone(),
                        range,
                        preexisting_coverage,
                        captured_finalized_target: finalized_head.number,
                        idempotency_key: request.idempotency_key.clone(),
                        effective_block_limit,
                        effective_byte_limit,
                        resume_below_ratio_millionths,
                        delivery_batch_limits,
                        initial_sequence: 0,
                        completion_sequence: None,
                        processed_work_blocks: 0,
                    },
                    &requested_record,
                    request_identity,
                )
                .await
                .map_err(|error| leani_api::BackfillControlError::Conflict(error.to_string()))?
        } else {
            self.store
                .create_historical_job(&requested_record, request_identity)
                .await
                .map_err(|error| leani_api::BackfillControlError::Conflict(error.to_string()))?
        };
        // The scheduler resumes a job this call does not start below.
        self.jobs_changed.notify_one();

        if matches!(
            record.state,
            leani_store_sqlite::JobState::Queued
                | leani_store_sqlite::JobState::Running
                | leani_store_sqlite::JobState::StorageBackpressured
        ) && (record.state != leani_store_sqlite::JobState::StorageBackpressured
            || self
                .store
                .storage_below_low_water()
                .await
                .map_err(|error| leani_api::BackfillControlError::Internal(error.to_string()))?)
        {
            let scheduled_job = serde_json::from_slice(&record.payload).map_err(|error| {
                leani_api::BackfillControlError::Internal(format!(
                    "decode scheduled historical job {}: {error}",
                    record.id
                ))
            })?;
            // As a resumed job does, it waits for the P2P history bridge to
            // cover it: its range may end above the bridge's latest anchor.
            if !self
                .waits_for_p2p_bridge(&scheduled_job, configured)
                .await?
            {
                self.spawn(scheduled_job, processor).await?;
            }
        }
        let outcome = self.outcome(&record.id).await?;
        self.status(&record, outcome.as_ref()).await
    }

    async fn list(
        &self,
        owner: Option<leani_api::HistoricalWorkOwner>,
    ) -> Result<Vec<leani_api::BackfillStatus>, leani_api::BackfillControlError> {
        let owners = match owner {
            Some(leani_api::HistoricalWorkOwner::Materialization) => {
                vec![leani_runtime::HistoricalJobOwner::Materialization]
            }
            Some(leani_api::HistoricalWorkOwner::Subscription) => {
                vec![leani_runtime::HistoricalJobOwner::Subscription]
            }
            None => vec![
                leani_runtime::HistoricalJobOwner::Materialization,
                leani_runtime::HistoricalJobOwner::Subscription,
            ],
        };
        let mut records = Vec::new();
        for owner in owners {
            records.extend(
                self.store
                    .jobs(Some(owner.job_kind()))
                    .await
                    .map_err(|error| {
                        leani_api::BackfillControlError::Internal(error.to_string())
                    })?,
            );
        }
        records.sort_by_key(|record| record.updated_at_unix_ms);
        let outcome_records = self
            .store
            .jobs(Some(HISTORICAL_BACKFILL_OUTCOME_KIND))
            .await
            .map_err(|error| leani_api::BackfillControlError::Internal(error.to_string()))?;
        let mut outcomes = BTreeMap::new();
        for record in outcome_records {
            let outcome: StoredBackfillOutcome =
                serde_json::from_slice(&record.payload).map_err(|error| {
                    leani_api::BackfillControlError::Internal(format!(
                        "backfill outcome {} is invalid: {error}",
                        record.id
                    ))
                })?;
            outcomes.insert(outcome.job_id.clone(), outcome);
        }
        let mut statuses = Vec::with_capacity(records.len());
        for record in &records {
            statuses.push(self.status(record, outcomes.get(&record.id)).await?);
        }
        Ok(statuses)
    }

    async fn inspect(
        &self,
        id: &str,
    ) -> Result<leani_api::BackfillStatus, leani_api::BackfillControlError> {
        let record = self
            .store
            .job(id)
            .await
            .map_err(|error| leani_api::BackfillControlError::Internal(error.to_string()))?
            .ok_or_else(|| leani_api::BackfillControlError::NotFound(id.to_owned()))?;
        Self::validate_historical_job_kind(&record)?;
        let outcome = self.outcome(&record.id).await?;
        self.status(&record, outcome.as_ref()).await
    }

    async fn cancel(
        &self,
        id: &str,
    ) -> Result<leani_api::BackfillStatus, leani_api::BackfillControlError> {
        let mut record = self
            .store
            .job(id)
            .await
            .map_err(|error| leani_api::BackfillControlError::Internal(error.to_string()))?
            .ok_or_else(|| leani_api::BackfillControlError::NotFound(id.to_owned()))?;
        Self::validate_historical_job_kind(&record)?;
        let job: leani_runtime::BackfillJob =
            serde_json::from_slice(&record.payload).map_err(|error| {
                leani_api::BackfillControlError::Internal(format!(
                    "durable historical job {id} has an invalid payload: {error}"
                ))
            })?;
        if let Some(running) = self.tasks.lock().await.get(id) {
            running.cancellation.cancel();
        }
        if matches!(
            record.state,
            leani_store_sqlite::JobState::Queued
                | leani_store_sqlite::JobState::Running
                | leani_store_sqlite::JobState::StorageBackpressured
        ) {
            record.state = leani_store_sqlite::JobState::Cancelled;
            record.updated_at_unix_ms = Self::now_milliseconds();
            self.store
                .save_job(&record)
                .await
                .map_err(|error| leani_api::BackfillControlError::Internal(error.to_string()))?;
        }
        // A completed job's subscription may still be draining: its consumer
        // has not acknowledged the completion, and never will once its
        // processor instance is gone. The store keeps a reclaimable,
        // cancelled, or failed subscription as it is.
        if job.owner == leani_runtime::HistoricalJobOwner::Subscription {
            self.store
                .set_backfill_subscription_state(
                    id,
                    leani_store_sqlite::BackfillSubscriptionState::Cancelled,
                    None,
                )
                .await
                .map_err(|error| leani_api::BackfillControlError::Internal(error.to_string()))?;
        }
        let outcome = self.outcome(&record.id).await?;
        self.status(&record, outcome.as_ref()).await
    }

    async fn retry(
        &self,
        id: &str,
    ) -> Result<leani_api::BackfillStatus, leani_api::BackfillControlError> {
        let record = self
            .store
            .job(id)
            .await
            .map_err(|error| leani_api::BackfillControlError::Internal(error.to_string()))?
            .ok_or_else(|| leani_api::BackfillControlError::NotFound(id.to_owned()))?;
        Self::validate_historical_job_kind(&record)?;
        let outcome = self.outcome(id).await?;
        let status = self.status(&record, outcome.as_ref()).await?;
        match status.state {
            leani_api::BackfillState::Failed => {}
            leani_api::BackfillState::CompleteReclaimable
            | leani_api::BackfillState::Completed
            | leani_api::BackfillState::Cancelled => {
                return Err(leani_api::BackfillControlError::Conflict(format!(
                    "historical job {id} in state {:?} can be retried only after it failed",
                    status.state
                )));
            }
            // Still active, for example after an earlier retry.
            _ => return Ok(status),
        }
        if status.owner != leani_api::HistoricalWorkOwner::Subscription {
            return Err(leani_api::BackfillControlError::Conflict(format!(
                "historical job {id} is not a backfill subscription; delete it and create it again"
            )));
        }
        // Refused before it is re-queued: a processor instance that is not
        // configured cannot run it, and the refusal must leave it failed. The
        // status counts only the job's own instance. `Self::processor` also
        // selects by kind, so it would run the job under another instance of
        // its kind.
        if !status.processor_configured {
            return Err(leani_api::BackfillControlError::Invalid(format!(
                "processor instance {:?} is not configured",
                status.processor
            )));
        }
        // The failed run's task may still be recording its failure, and a
        // late write would fail the retried job again. It is not cancelled:
        // that could record the failed job as cancelled.
        let running = self.tasks.lock().await.get(id).cloned();
        if let Some(running) = running
            && tokio::time::timeout(HISTORICAL_JOB_STOP_WAIT, running.ended.cancelled())
                .await
                .is_err()
        {
            return Err(leani_api::BackfillControlError::Unavailable(format!(
                "historical job {id} is still stopping; retry again"
            )));
        }
        self.store
            .retry_failed_backfill_subscription(id, &Self::outcome_id(id))
            .await
            .map_err(|error| leani_api::BackfillControlError::Internal(error.to_string()))?;
        let record = self
            .store
            .job(id)
            .await
            .map_err(|error| leani_api::BackfillControlError::Internal(error.to_string()))?
            .ok_or_else(|| leani_api::BackfillControlError::NotFound(id.to_owned()))?;
        // Start it now when a slot is free, as creation does; the scheduler
        // takes it up once one frees, storage allows, or the P2P history
        // bridge covers it.
        self.resume_durable_job(&record).await?;
        self.jobs_changed.notify_one();
        let outcome = self.outcome(id).await?;
        self.status(&record, outcome.as_ref()).await
    }

    async fn delete(
        &self,
        id: &str,
        unacknowledged: leani_store_sqlite::UnacknowledgedDelivery,
    ) -> Result<leani_api::HistoricalWorkDeletion, leani_api::BackfillControlError> {
        let record = self
            .store
            .job(id)
            .await
            .map_err(|error| leani_api::BackfillControlError::Internal(error.to_string()))?
            .ok_or_else(|| leani_api::BackfillControlError::NotFound(id.to_owned()))?;
        Self::validate_historical_job_kind(&record)?;
        let outcome = self.outcome(id).await?;
        let status = self.status(&record, outcome.as_ref()).await?;
        if !matches!(
            status.state,
            leani_api::BackfillState::CompleteReclaimable
                | leani_api::BackfillState::Completed
                | leani_api::BackfillState::Cancelled
                | leani_api::BackfillState::Failed
        ) {
            return Err(leani_api::BackfillControlError::Conflict(format!(
                "historical job {id} in state {:?} must finish or be cancelled before deletion",
                status.state
            )));
        }
        // A cancelled job's task may still be stopping. Its last writes for
        // the job, such as its outcome, would outlive a deletion before then.
        let running = self.tasks.lock().await.get(id).cloned();
        if let Some(running) = running {
            running.cancellation.cancel();
            if tokio::time::timeout(HISTORICAL_JOB_STOP_WAIT, running.ended.cancelled())
                .await
                .is_err()
            {
                return Err(leani_api::BackfillControlError::Unavailable(format!(
                    "historical job {id} is still stopping; retry the deletion"
                )));
            }
        }
        let subscription = status.owner == leani_api::HistoricalWorkOwner::Subscription;
        let deleted = self
            .store
            .delete_terminal_historical_work(
                id,
                &Self::outcome_id(id),
                subscription,
                unacknowledged,
            )
            .await
            .map_err(|error| match error {
                leani_store_sqlite::StoreError::HistoricalWorkNotDeletable { .. } => {
                    leani_api::BackfillControlError::Conflict(error.to_string())
                }
                _ => leani_api::BackfillControlError::Internal(error.to_string()),
            })?;
        self.bridge_waits
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(id);
        Ok(leani_api::HistoricalWorkDeletion {
            id: id.to_owned(),
            owner: status.owner,
            removed_jobs: deleted.jobs,
            removed_subscription_ranges: deleted.subscription_ranges,
            removed_consumers: deleted.consumers,
            removed_delivery_records: deleted.delivery_records,
            removed_delivery_streams: deleted.delivery_streams,
            removed_coverage_intervals: deleted.coverage_intervals,
            removed_coverage_segments: deleted.coverage_segments,
            removed_exact_coverage: deleted.exact_coverage,
            removed_applied_blocks: deleted.applied_blocks,
            removed_finalized_undo: deleted.finalized_undo,
            retained_processor_output: true,
            retained_live_stream: subscription,
        })
    }
}

#[derive(Clone)]
struct NativeRawHistoryControl {
    chain_id: leani_primitives::ChainId,
    merge_block: Option<leani_primitives::BlockNumber>,
    store: leani_store_history::HistoryStore,
    runner: leani_store_history::RawHistoryRunner,
    cancellation: CancellationToken,
    tasks: Arc<tokio::sync::Mutex<BTreeMap<String, CancellationToken>>>,
}

impl std::fmt::Debug for NativeRawHistoryControl {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NativeRawHistoryControl")
            .field("chain_id", &self.chain_id)
            .field("merge_block", &self.merge_block)
            .field("cancelled", &self.cancellation.is_cancelled())
            .finish_non_exhaustive()
    }
}

impl NativeRawHistoryControl {
    fn new(
        chain_id: leani_primitives::ChainId,
        merge_block: Option<leani_primitives::BlockNumber>,
        store: leani_store_history::HistoryStore,
        runner: leani_store_history::RawHistoryRunner,
        cancellation: CancellationToken,
    ) -> Self {
        Self {
            chain_id,
            merge_block,
            store,
            runner,
            cancellation,
            tasks: Arc::new(tokio::sync::Mutex::new(BTreeMap::new())),
        }
    }

    async fn spawn(
        &self,
        id: leani_store_history::RawHistoryJobId,
    ) -> Result<(), leani_api::RawHistoryControlError> {
        if self.cancellation.is_cancelled() {
            return Err(leani_api::RawHistoryControlError::Unavailable(
                "node shutdown is in progress".to_owned(),
            ));
        }
        let mut tasks = self.tasks.lock().await;
        if tasks.contains_key(id.as_str()) {
            return Ok(());
        }
        let token = self.cancellation.child_token();
        tasks.insert(id.as_str().to_owned(), token.clone());
        drop(tasks);
        let runner = self.runner.clone();
        let tasks = self.tasks.clone();
        let job_id = id.clone();
        tokio::spawn(async move {
            match Box::pin(runner.run(&job_id, token)).await {
                Ok(leani_store_history::RawHistoryRunOutcome::Complete(job)) => {
                    info!(job = %job.id.as_str(), segments = job.committed_segments, logical_bytes = job.committed_logical_bytes, physical_bytes = job.committed_physical_bytes, "raw-history job complete");
                }
                Ok(leani_store_history::RawHistoryRunOutcome::Cancelled(job)) => {
                    info!(job = %job.id.as_str(), "raw-history job cancelled");
                }
                Ok(leani_store_history::RawHistoryRunOutcome::Interrupted(job)) => {
                    info!(job = %job.id.as_str(), "raw-history job interrupted; durable progress will resume");
                }
                Ok(leani_store_history::RawHistoryRunOutcome::StorageBackpressured(job)) => {
                    warn!(job = %job.id.as_str(), error = ?job.last_error, "raw-history job paused at its storage limit");
                }
                Err(leani_store_history::RawHistoryRunError::SourcesUnavailable {
                    range,
                    reasons,
                }) => {
                    warn!(job = %job_id.as_str(), ?range, ?reasons, "raw-history sources temporarily unavailable; job remains resumable");
                }
                Err(error) => {
                    warn!(job = %job_id.as_str(), %error, "raw-history job stopped");
                }
            }
            tasks.lock().await.remove(job_id.as_str());
        });
        Ok(())
    }

    async fn resume_durable_jobs(&self) -> Result<(), leani_api::RawHistoryControlError> {
        for job in self
            .store
            .raw_history_jobs()
            .await
            .map_err(raw_history_control_error)?
        {
            if matches!(
                job.state,
                leani_store_history::RawHistoryJobState::Queued
                    | leani_store_history::RawHistoryJobState::Running
                    | leani_store_history::RawHistoryJobState::StorageBackpressured
            ) {
                self.spawn(job.id).await?;
            }
        }
        Ok(())
    }

    async fn supervise_durable_jobs(self: Arc<Self>) {
        loop {
            if self.cancellation.is_cancelled() {
                return;
            }
            if let Err(error) = self.resume_durable_jobs().await {
                warn!(%error, "raw-history scheduler pass failed");
            }
            tokio::select! {
                () = self.cancellation.cancelled() => return,
                () = tokio::time::sleep(Duration::from_secs(5)) => {}
            }
        }
    }
}

#[async_trait::async_trait]
impl leani_api::RawHistoryControl for NativeRawHistoryControl {
    async fn create(
        &self,
        request: leani_api::CreateRawHistoryJobRequest,
    ) -> Result<leani_store_history::RawHistoryJob, leani_api::RawHistoryControlError> {
        let ranges = request
            .ranges
            .iter()
            .map(|range| {
                leani_primitives::BlockRange::new(
                    leani_primitives::BlockNumber(range.from_block),
                    leani_primitives::BlockNumber(range.to_block),
                )
                .map_err(|error| leani_api::RawHistoryControlError::Invalid(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let id = leani_store_history::RawHistoryJobId::new(request.idempotency_key)
            .map_err(raw_history_control_error)?;
        let profile = match request.profile {
            leani_api::RawHistoryProfileName::ProcessorReuse => {
                leani_store_history::RawHistoryProfile::ProcessorReuse
            }
            leani_api::RawHistoryProfileName::PostMergeExecutionRpc => {
                let merge_block = self.merge_block.ok_or_else(|| {
                    leani_api::RawHistoryControlError::ProfileIncompatible(format!(
                        "chain {} has no configured Merge block",
                        self.chain_id.0
                    ))
                })?;
                if let Some(first) = ranges.iter().map(|range| range.start()).min()
                    && first < merge_block
                {
                    return Err(leani_api::RawHistoryControlError::ProfileIncompatible(
                        format!(
                            "post_merge_execution_rpc starts at Merge block {}; requested range starts at {}",
                            merge_block.0, first.0
                        ),
                    ));
                }
                leani_store_history::RawHistoryProfile::PostMergeExecutionRpc { merge_block }
            }
        };
        let job = self
            .store
            .create_raw_history_job(
                id,
                leani_store_history::RawHistoryJobSpec {
                    chain_id: self.chain_id,
                    ranges,
                    profile,
                    material: request.material,
                    required_capabilities: request.required_capabilities,
                    verification: request.verification,
                    minimum_trust: request.minimum_trust,
                    source_policy_digest: self.runner.source_policy_digest(),
                    retention: request.retention,
                    segment: request.segment,
                    indexes: request.indexes,
                },
            )
            .await
            .map_err(raw_history_control_error)?;
        if !job.state.is_terminal() {
            self.spawn(job.id.clone()).await?;
        }
        Ok(job)
    }

    async fn list(
        &self,
    ) -> Result<Vec<leani_store_history::RawHistoryJob>, leani_api::RawHistoryControlError> {
        self.store
            .raw_history_jobs()
            .await
            .map_err(raw_history_control_error)
    }

    async fn inspect(
        &self,
        id: &str,
    ) -> Result<leani_store_history::RawHistoryJob, leani_api::RawHistoryControlError> {
        let id = leani_store_history::RawHistoryJobId::new(id.to_owned())
            .map_err(raw_history_control_error)?;
        self.store
            .raw_history_job(&id)
            .await
            .map_err(raw_history_control_error)?
            .ok_or_else(|| leani_api::RawHistoryControlError::NotFound(id.as_str().to_owned()))
    }

    async fn cancel(
        &self,
        id: &str,
    ) -> Result<leani_store_history::RawHistoryJob, leani_api::RawHistoryControlError> {
        let id = leani_store_history::RawHistoryJobId::new(id.to_owned())
            .map_err(raw_history_control_error)?;
        if let Some(token) = self.tasks.lock().await.get(id.as_str()).cloned() {
            token.cancel();
        }
        self.store
            .cancel_raw_history_job(&id)
            .await
            .map_err(raw_history_control_error)
    }

    async fn delete(
        &self,
        id: &str,
    ) -> Result<leani_store_history::RawHistoryJobDeletion, leani_api::RawHistoryControlError> {
        let id = leani_store_history::RawHistoryJobId::new(id.to_owned())
            .map_err(raw_history_control_error)?;
        self.store
            .delete_raw_history_job(&id)
            .await
            .map_err(raw_history_control_error)
    }
}

#[allow(clippy::needless_pass_by_value)]
fn raw_history_control_error(
    error: leani_store_history::HistoryStoreError,
) -> leani_api::RawHistoryControlError {
    match error {
        leani_store_history::HistoryStoreError::InvalidJob(_)
        | leani_store_history::HistoryStoreError::InvalidOwnerId(_) => {
            leani_api::RawHistoryControlError::Invalid(error.to_string())
        }
        leani_store_history::HistoryStoreError::UnknownJob(_) => {
            leani_api::RawHistoryControlError::NotFound(error.to_string())
        }
        leani_store_history::HistoryStoreError::JobConflict(_)
        | leani_store_history::HistoryStoreError::JobState { .. } => {
            leani_api::RawHistoryControlError::Conflict(error.to_string())
        }
        leani_store_history::HistoryStoreError::LogicalBudget { .. }
        | leani_store_history::HistoryStoreError::PhysicalBudget { .. } => {
            leani_api::RawHistoryControlError::Unavailable(error.to_string())
        }
        _ => leani_api::RawHistoryControlError::Internal(error.to_string()),
    }
}

#[cfg(test)]
mod raw_history_control_tests {
    use std::{sync::Arc, time::Duration};

    use leani_api::RawHistoryControl as _;
    use leani_primitives::{
        BlockHash, BlockNumber, BlockRange, Capability, CapabilitySet, ChainId, TrustModel,
    };
    use leani_store_history::{
        Compression, HistoryStore, HistoryStoreConfig, RawHistoryIndexPolicy,
        RawHistoryMaterialProfile, RawHistoryRetention, RawHistoryRunner, RawHistorySegmentPolicy,
        RawHistorySourceSet, StorageBudget, StorageLimitAction, VerificationClass,
    };
    use leani_testkit::{
        ScriptedHistorySource, default_source_budget, fixture_frame, fixture_source_descriptor,
    };
    use tempfile::tempdir;
    use tokio_util::sync::CancellationToken;

    use super::NativeRawHistoryControl;
    use crate::config::{Config, ProcessorHistoryMode, VALID_CONFIG_TOML};

    #[tokio::test]
    async fn native_raw_control_runs_lists_and_explicitly_deletes_metadata() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(HistoryStoreConfig::new(directory.path()).with_budget(
            StorageBudget {
                maximum_logical_bytes: 16 * 1024 * 1024,
                maximum_physical_bytes: 16 * 1024 * 1024,
                maximum_frame_logical_bytes: 1024 * 1024,
                maximum_segment_logical_bytes: 4 * 1024 * 1024,
                maximum_segment_physical_bytes: 4 * 1024 * 1024,
            },
        ))
        .await
        .expect("store");
        let range = BlockRange::new(BlockNumber(700), BlockNumber(702)).expect("range");
        let mut parent = BlockHash::new([0x81; 32]);
        let frames = range
            .iter()
            .map(|number| {
                let frame = fixture_frame(number.0, parent);
                parent = frame.block.hash;
                frame
            })
            .collect();
        let source = Arc::new(ScriptedHistorySource::from_frames(
            fixture_source_descriptor("raw-control-source", range),
            frames,
        ));
        let source_set = RawHistorySourceSet::new(vec![source]).expect("source set");
        let runner = RawHistoryRunner::new(store.clone(), source_set, default_source_budget())
            .expect("runner");
        let control = NativeRawHistoryControl::new(
            ChainId(1),
            Some(BlockNumber(15_537_394)),
            store.clone(),
            runner,
            CancellationToken::new(),
        );
        let request = leani_api::CreateRawHistoryJobRequest {
            idempotency_key: "raw-control-job".to_owned(),
            ranges: vec![leani_api::BackfillRange {
                from_block: 700,
                to_block: 702,
            }],
            profile: leani_api::RawHistoryProfileName::ProcessorReuse,
            material: RawHistoryMaterialProfile::default(),
            required_capabilities: CapabilitySet::from_iter([
                Capability::Transactions,
                Capability::Receipts,
            ]),
            verification: VerificationClass::Cryptographic,
            minimum_trust: TrustModel::ProtocolVerified,
            retention: RawHistoryRetention::Full,
            segment: RawHistorySegmentPolicy {
                target_blocks: 3,
                maximum_logical_bytes: 1024 * 1024,
                maximum_physical_bytes: 1024 * 1024,
                compression: Compression::Snappy,
                on_limit: StorageLimitAction::Pause,
            },
            indexes: RawHistoryIndexPolicy::default(),
        };
        let mut incompatible = request.clone();
        incompatible.idempotency_key = "pre-merge-rpc-job".to_owned();
        incompatible.profile = leani_api::RawHistoryProfileName::PostMergeExecutionRpc;
        assert!(matches!(
            control.create(incompatible).await,
            Err(leani_api::RawHistoryControlError::ProfileIncompatible(_))
        ));
        // The job's own validation refuses the retention no job supports.
        let mut window = request.clone();
        window.idempotency_key = "window-job".to_owned();
        window.retention = RawHistoryRetention::Window { blocks: 64 };
        let refused = control.create(window).await;
        assert!(
            matches!(
                &refused,
                Err(leani_api::RawHistoryControlError::Invalid(message))
                    if message.contains("\"retention\": \"full\"")
            ),
            "{refused:?}"
        );
        let created = control.create(request).await.expect("create");
        let id = created.id;
        let complete = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let job = control.inspect(id.as_str()).await.expect("inspect");
                if job.state == leani_store_history::RawHistoryJobState::Complete {
                    break job;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("job completes");
        assert_eq!(complete.committed_segments, 1);
        assert_eq!(control.list().await.expect("list").len(), 1);
        let deleted = control.delete(id.as_str()).await.expect("delete");
        assert_eq!(deleted.jobs, 1);
        assert_eq!(deleted.owners, 1);
        assert_eq!(store.stats().await.expect("stats").closed_segments, 1);
    }

    #[tokio::test]
    async fn retained_only_processor_scheduling_excludes_external_sources() {
        use leani_processor_api::Processor as _;

        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(HistoryStoreConfig::new(directory.path()).with_budget(
            StorageBudget {
                maximum_logical_bytes: 16 * 1024 * 1024,
                maximum_physical_bytes: 16 * 1024 * 1024,
                maximum_frame_logical_bytes: 1024 * 1024,
                maximum_segment_logical_bytes: 4 * 1024 * 1024,
                maximum_segment_physical_bytes: 4 * 1024 * 1024,
            },
        ))
        .await
        .expect("store");
        let processor = leani_processor_blobs::BlobsProcessor::default();
        let mut config: Config = toml::from_str(VALID_CONFIG_TOML).expect("config");
        config.processors[0].instance = processor.descriptor().instance.to_string();
        config.processors[0].history_mode = ProcessorHistoryMode::OnDemand;
        config.processors[0].require_retained_input = true;
        config.raw_history.enabled = true;
        let (sources, policy) =
            super::configured_history_sources(&config, &processor, Some(&store))
                .expect("retained sources");
        assert_eq!(policy, leani_source_api::VerificationPolicy::TrustedDataset);
        assert!(!sources.is_empty());
        assert!(sources.iter().all(|source| {
            source.descriptor().kind == leani_primitives::SourceKind::RetainedHistory
        }));
    }

    /// Only its descriptor matters: the raw-history profile is derived from it.
    #[derive(Debug)]
    struct RequirementsOnly {
        descriptor: leani_processor_api::ProcessorDescriptor,
    }

    #[async_trait::async_trait]
    impl leani_processor_api::Processor for RequirementsOnly {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn descriptor(&self) -> &leani_processor_api::ProcessorDescriptor {
            &self.descriptor
        }

        async fn map(
            &self,
            _block: &leani_primitives::BlockFrame,
        ) -> Result<leani_processor_api::EncodedDelta, leani_processor_api::ProcessorError>
        {
            Err(leani_processor_api::ProcessorError::Input(
                "not mapped in this test".to_owned(),
            ))
        }

        async fn reduce(
            &self,
            _transaction: &mut dyn leani_processor_api::ReducerTransaction,
            _cursor: &leani_primitives::ProcessorCursor,
            _delta: &leani_processor_api::EncodedDelta,
        ) -> Result<leani_processor_api::DomainChanges, leani_processor_api::ProcessorError>
        {
            Err(leani_processor_api::ProcessorError::Input(
                "not reduced in this test".to_owned(),
            ))
        }
    }

    #[test]
    fn raw_history_profile_covers_every_requirement_like_the_backfill_request() {
        use leani_processor_api::Processor as _;

        let requirement = |filter| leani_processor_api::DataRequirement {
            capabilities: CapabilitySet::of(Capability::Transactions),
            log_fields: leani_primitives::LogFieldSet::NONE,
            allow_filtered: true,
            filter,
            minimum_finality: leani_primitives::Finality::Included,
        };
        let mut descriptor = leani_testkit::BlockLocalCounter::named("raw-profile")
            .descriptor()
            .clone();
        descriptor.requirements = vec![
            requirement(leani_primitives::FilterScope {
                senders: vec![leani_primitives::Address::new([0x55; 20])],
                ..leani_primitives::FilterScope::default()
            }),
            requirement(leani_primitives::FilterScope::default()),
        ];
        let processor = RequirementsOnly { descriptor };

        let profile = super::processor_raw_material_profile(&processor);

        for requirement in &processor.descriptor().requirements {
            assert!(
                profile.filters.scope.covers(&requirement.filter),
                "the profile narrows a requirement's filter: {:?}",
                profile.filters
            );
        }
        let request = leani_runtime::BackfillJob::for_processor(
            "raw-profile",
            &processor,
            ChainId(1),
            BlockRange::single(BlockNumber(0)),
            leani_source_api::VerificationPolicy::TrustedDataset,
        )
        .expect("backfill job")
        .request;
        assert_eq!(
            profile.shape_id(),
            RawHistoryMaterialProfile::from_request(&request).shape_id(),
            "the retained source must serve the processor's own backfill requests"
        );
    }
}

impl From<Exit> for ExitCode {
    fn from(value: Exit) -> Self {
        match value {
            Exit::Success => Self::SUCCESS,
            Exit::Failure => Self::FAILURE,
            Exit::InvalidConfiguration => Self::from(3),
            Exit::TimedOut => Self::from(124),
            Exit::Interrupted => Self::from(130),
        }
    }
}

/// Parse process arguments, install logging, and execute the selected command.
///
/// # Errors
///
/// Returns an error when logging, configuration loading, validation, or the
/// selected operation fails.
pub async fn run() -> Result<Exit> {
    Box::pin(run_with_registry(ProcessorRegistry::standard())).await
}

/// Parse process arguments and run with an application-supplied processor
/// registry.
///
/// # Errors
///
/// Returns an error when logging, configuration, registry validation, or the
/// selected operation fails.
pub async fn run_with_registry(registry: ProcessorRegistry) -> Result<Exit> {
    let cli = Cli::parse();
    let log_filter = cli.log_filter.as_deref().unwrap_or({
        if matches!(cli.command, Command::Subscribe { .. }) {
            "error,leani=warn"
        } else {
            "info"
        }
    });
    init_logging(cli.log_format, log_filter)?;
    Box::pin(run_cli_with_registry(cli, &registry)).await
}

/// Execute an already parsed command.
///
/// # Errors
///
/// Returns an error when configuration or command execution fails.
pub async fn run_cli(cli: Cli) -> Result<Exit> {
    Box::pin(run_cli_with_registry(cli, &ProcessorRegistry::standard())).await
}

/// Execute an already parsed command using an application-supplied processor
/// registry.
///
/// # Errors
///
/// Returns an error when configuration, processor construction, or command
/// execution fails.
#[allow(clippy::too_many_lines)]
pub async fn run_cli_with_registry(cli: Cli, registry: &ProcessorRegistry) -> Result<Exit> {
    let working_directory = std::env::current_dir().context("resolve current working directory")?;
    let requested_config = cli.config.clone();
    let config_path = resolve_config_path(cli.config.as_deref(), &working_directory);
    match cli.command {
        Command::Doctor { json } => doctor(&config_path, json, registry),
        Command::Serve => serve(&config_path, registry).await,
        Command::Init {
            protocol,
            targets,
            data_dir,
            checkpoint_url,
            checkpoint_quorum,
            yes,
        } => {
            crate::init::init(crate::init::InitOptions {
                protocol,
                targets,
                data_dir,
                checkpoint_urls: checkpoint_url,
                checkpoint_quorum,
                accept_checkpoint: yes,
                config_path: requested_config.unwrap_or_else(|| PathBuf::from("leani.toml")),
                working_directory,
            })
            .await
        }
        Command::Subscribe {
            protocol,
            targets,
            format,
            json,
            mode,
            endpoint,
            processor,
            token,
            finality,
            finality_source,
            checkpoint_url,
            checkpoint_quorum,
            yes,
            data_dir,
            once,
            timeout,
        } => {
            let processor = processor.unwrap_or_else(|| match protocol {
                SubscribeProtocol::Blocks => "block-summary".to_owned(),
                SubscribeProtocol::UniswapV3 => "uniswap-observations".to_owned(),
            });
            Box::pin(crate::subscribe::subscribe(
                crate::subscribe::SubscribeOptions {
                    protocol,
                    targets,
                    format: if json {
                        crate::cli::SubscribeFormat::Json
                    } else {
                        format
                    },
                    mode,
                    endpoint,
                    processor,
                    token,
                    finality,
                    finality_source,
                    checkpoint_urls: checkpoint_url,
                    checkpoint_quorum,
                    accept_checkpoint: yes,
                    data_dir,
                    once,
                    timeout,
                    requested_config,
                    working_directory,
                },
                registry,
            ))
            .await
        }
        Command::Reset { command } => match command {
            ResetCommand::All { data_dir, yes } => {
                let reset_config =
                    (requested_config.is_some() || config_path.is_file()).then_some(config_path);
                crate::local_state::reset_all(&crate::local_state::ResetAllOptions {
                    confirmed: yes,
                    config_path: reset_config,
                    data_dir,
                    working_directory,
                })
            }
            ResetCommand::Subscription {
                protocol,
                targets,
                finality,
                data_dir,
                yes,
            } => {
                crate::subscribe::reset_subscription(&crate::subscribe::ResetSubscriptionOptions {
                    protocol,
                    targets,
                    finality,
                    data_dir,
                    confirmed: yes,
                    requested_config,
                    working_directory,
                })
            }
        },
        Command::Backfill {
            processor,
            from_block,
            to_block,
            endpoint,
            token,
        } => {
            backfill::run(
                &config_path,
                processor.as_deref(),
                from_block,
                to_block,
                endpoint.as_ref(),
                token.as_deref(),
                registry,
            )
            .await
        }
        Command::Source {
            command: SourceCommand::Probe { source },
        } => probe_source(source, &config_path).await,
        Command::Conformance { command } => conformance(command, &config_path, registry).await,
        Command::Db { command } => db(command, &config_path, registry).await,
        Command::Benchmark {
            command,
            mode,
            profile,
            destination,
            postgres_schema,
            corpus,
            blocks,
            seed,
            chunk_blocks,
            artifact_segment_blocks,
            artifact_segment_compression,
            artifact_compaction_interval_ms,
            artifact_compaction_maximum_segments_per_cycle,
            warmups,
            runs,
            sample_interval_ms,
            consumer_delay_ms,
            consumer_reconnect_every_batches,
            consumer_drop_ack_response_once,
            concurrent_live_blocks,
            live_block_interval_ms,
            mapper_concurrency,
            maximum_active_chunks,
            maximum_mapped_bytes,
            commit_maximum_blocks,
            commit_maximum_changes,
            commit_maximum_encoded_bytes,
            commit_maximum_delay_ms,
            commit_target_writer_hold_ms,
            delivery_target_encoded_bytes,
            delivery_maximum_encoded_bytes,
            delivery_maximum_events,
            delivery_maximum_processed_blocks,
            delivery_maximum_delay_ms,
            delivery_maximum_buffered_batches,
            delivery_maximum_buffered_bytes,
            delivery_compression,
            report,
            samples_report,
        } => {
            if let Some(command) = command {
                return match *command {
                    BenchmarkCommand::Sweep {
                        manifest,
                        output_directory,
                    } => crate::benchmark::run_sweep(&manifest, &output_directory).await,
                    BenchmarkCommand::RealSource {
                        processor,
                        source_policy,
                        from_block,
                        to_block,
                        data_dir,
                        consumer_delay_ms,
                        timeout_seconds,
                        sample_interval_ms,
                        source_concurrency,
                        mapper_concurrency,
                        maximum_active_chunks,
                        expected_output_digest,
                        delivery_compression,
                        report,
                    } => {
                        Box::pin(crate::benchmark::run_real_source(
                            &config_path,
                            RealSourceBenchmarkOptions {
                                processor,
                                source_policy,
                                from_block,
                                to_block,
                                data_dir,
                                consumer_delay_ms,
                                timeout_seconds,
                                sample_interval_ms,
                                source_concurrency,
                                mapper_concurrency,
                                maximum_active_chunks,
                                expected_output_digest,
                                delivery_compression,
                                report,
                            },
                            registry,
                        ))
                        .await
                    }
                };
            }
            let profile = profile.context(
                "benchmark --profile is required for a direct run; sweep manifests provide it in baseArguments",
            )?;
            Box::pin(crate::benchmark::run(BenchmarkOptions {
                mode,
                profile,
                destination,
                postgres_schema,
                corpus,
                blocks,
                seed,
                chunk_blocks,
                artifact_segment_blocks,
                artifact_segment_compression,
                artifact_compaction_interval_ms,
                artifact_compaction_maximum_segments_per_cycle,
                warmups,
                runs,
                sample_interval_ms,
                consumer_delay_ms,
                consumer_reconnect_every_batches,
                consumer_drop_ack_response_once,
                concurrent_live_blocks,
                live_block_interval_ms,
                mapper_concurrency,
                maximum_active_chunks,
                maximum_mapped_bytes,
                commit_maximum_blocks,
                commit_maximum_changes,
                commit_maximum_encoded_bytes,
                commit_maximum_delay_ms,
                commit_target_writer_hold_ms,
                delivery_target_encoded_bytes,
                delivery_maximum_encoded_bytes,
                delivery_maximum_events,
                delivery_maximum_processed_blocks,
                delivery_maximum_delay_ms,
                delivery_maximum_buffered_batches,
                delivery_maximum_buffered_bytes,
                delivery_compression,
                report,
                samples_report,
            }))
            .await
        }
        Command::E2e { command } => e2e(command, &config_path, registry).await,
    }
}

fn resolve_config_path(explicit: Option<&Path>, working_directory: &Path) -> PathBuf {
    crate::local_state::configured_path(explicit, working_directory)
        .unwrap_or_else(|| working_directory.join("leani.toml"))
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RpcDifferentialReport {
    actual_frames: usize,
    expected_frames: Option<usize>,
    mismatched_indices: Vec<usize>,
    equivalent: Option<bool>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProcessorDifferentialReport {
    processor: String,
    left_source: String,
    right_source: String,
    left_frames: usize,
    right_frames: usize,
    compared_frames: usize,
    mismatched_indices: Vec<usize>,
    equivalent: bool,
}

async fn conformance(
    command: ConformanceCommand,
    config_path: &Path,
    registry: &ProcessorRegistry,
) -> Result<Exit> {
    match command {
        ConformanceCommand::Frames {
            left_source,
            left,
            right_source,
            right,
            capability,
            report,
        } => compare_frame_exports(
            left_source,
            &left,
            right_source,
            &right,
            capability,
            report.as_deref(),
        )?,
        ConformanceCommand::Processor {
            processor,
            left_source,
            left,
            right_source,
            right,
            report,
        } => {
            compare_processor_exports(
                config_path,
                processor,
                left_source,
                &left,
                right_source,
                &right,
                report.as_deref(),
                registry,
            )
            .await?;
        }
        ConformanceCommand::Rpc {
            frames,
            expected,
            output,
            report,
        } => compare_rpc_exports(
            &frames,
            expected.as_deref(),
            output.as_deref(),
            report.as_deref(),
        )?,
    }
    Ok(Exit::Success)
}

fn compare_frame_exports(
    left_source: String,
    left: &Path,
    right_source: String,
    right: &Path,
    capability: Vec<crate::cli::CompareCapability>,
    report: Option<&Path>,
) -> Result<()> {
    let left_frames = read_frames(left)?;
    let right_frames = read_frames(right)?;
    let capabilities = capability
        .into_iter()
        .map(crate::cli::CompareCapability::into_primitive)
        .collect();
    let comparison = leani_source_api::compare_frame_sequences(
        left_source,
        &left_frames,
        right_source,
        &right_frames,
        capabilities,
    );
    if let Some(path) = report {
        write_json(path, &comparison)?;
    }
    println!("{}", serde_json::to_string_pretty(&comparison)?);
    if !comparison.is_equivalent() {
        bail!("normalized source exports disagree");
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn compare_processor_exports(
    config_path: &Path,
    processor: String,
    left_source: String,
    left: &Path,
    right_source: String,
    right: &Path,
    report: Option<&Path>,
    registry: &ProcessorRegistry,
) -> Result<()> {
    let config = Config::load(config_path)
        .with_context(|| format!("load configuration {}", config_path.display()))?;
    let configured = select_processor_config(&config, &processor)?;
    let processor_impl = registry.instantiate(configured, config.chain.chain_id)?;
    let left_frames = read_frames(left)?;
    let right_frames = read_frames(right)?;
    let left_deltas =
        map_conformance_frames(processor_impl.as_ref(), &left_source, &left_frames).await?;
    let right_deltas =
        map_conformance_frames(processor_impl.as_ref(), &right_source, &right_frames).await?;
    let compared_frames = left_deltas.len().min(right_deltas.len());
    let mismatched_indices = left_deltas
        .iter()
        .zip(&right_deltas)
        .enumerate()
        .filter_map(|(index, (left, right))| (left != right).then_some(index))
        .chain(compared_frames..left_deltas.len().max(right_deltas.len()))
        .collect::<Vec<_>>();
    let differential = ProcessorDifferentialReport {
        processor,
        left_source,
        right_source,
        left_frames: left_deltas.len(),
        right_frames: right_deltas.len(),
        compared_frames,
        equivalent: mismatched_indices.is_empty(),
        mismatched_indices,
    };
    if let Some(path) = report {
        write_json(path, &differential)?;
    }
    println!("{}", serde_json::to_string_pretty(&differential)?);
    if !differential.equivalent {
        bail!("source exports produce different processor deltas");
    }
    Ok(())
}

fn compare_rpc_exports(
    frames: &Path,
    expected: Option<&Path>,
    output: Option<&Path>,
    report: Option<&Path>,
) -> Result<()> {
    let frames = read_frames(frames)?;
    // The served JSON-RPC prices blob gas with the checked mainnet schedule.
    let schedule = leani_processor_blobs::BlobSchedule::mainnet();
    let actual = frames
        .iter()
        .map(|frame| leani_rpc::rpc_compatibility_snapshot(frame, &schedule))
        .collect::<Result<Vec<_>, _>>()?;
    if let Some(path) = output {
        write_json(path, &actual)?;
    }
    let (expected_frames, mismatched_indices, equivalent) = if let Some(expected_path) = expected {
        let expected: Vec<leani_rpc::RpcCompatibilitySnapshot> = read_json(expected_path)?;
        let mismatched = actual
            .iter()
            .zip(&expected)
            .enumerate()
            .filter_map(|(index, (actual, expected))| (actual != expected).then_some(index))
            .chain(actual.len().min(expected.len())..actual.len().max(expected.len()))
            .collect::<Vec<_>>();
        (
            Some(expected.len()),
            mismatched.clone(),
            Some(mismatched.is_empty()),
        )
    } else {
        (None, Vec::new(), None)
    };
    let differential = RpcDifferentialReport {
        actual_frames: actual.len(),
        expected_frames,
        mismatched_indices,
        equivalent,
    };
    if let Some(path) = report {
        write_json(path, &differential)?;
    }
    println!("{}", serde_json::to_string_pretty(&differential)?);
    if differential.equivalent == Some(false) {
        bail!("reconstructed RPC snapshots disagree with the reference");
    }
    Ok(())
}

async fn map_conformance_frames(
    processor: &dyn leani_processor_api::Processor,
    source: &str,
    frames: &[leani_primitives::BlockFrame],
) -> Result<Vec<leani_processor_api::EncodedDelta>> {
    let mut deltas = Vec::with_capacity(frames.len());
    for frame in frames {
        frame.validate_shape().map_err(|error| {
            anyhow::anyhow!("{source} frame {} is invalid: {error}", frame.block.number)
        })?;
        for requirement in &processor.descriptor().requirements {
            requirement.validate_frame(frame).map_err(|error| {
                anyhow::anyhow!(
                    "{source} frame {} violates processor input: {error}",
                    frame.block.number
                )
            })?;
        }
        deltas.push(processor.map(frame).await.with_context(|| {
            format!(
                "map {source} frame {} with processor {}",
                frame.block.number,
                processor.descriptor().id
            )
        })?);
    }
    Ok(deltas)
}

/// Refuse a backfill of an ordered processor that does not continue its
/// applied history: the store moves an ordered processor's cursor to each
/// block it applies, so a range with a hole below it, or below applied
/// blocks, would reduce its history out of chain order.
async fn require_ordered_backfill_start(
    store: &leani_store_sqlite::SqliteStore,
    processor: &dyn leani_processor_api::Processor,
    configured: &ProcessorConfig,
    from: u64,
) -> Result<()> {
    if processor.descriptor().mode != leani_processor_api::ReductionMode::OrderedState {
        return Ok(());
    }
    let applied = store.processor_cursor(processor.descriptor()).await?;
    let next = applied.as_ref().map_or(configured.start_block, |cursor| {
        cursor.block_number.0.saturating_add(1)
    });
    if from != next {
        let progress = applied.map_or_else(
            || "has applied no block yet".to_owned(),
            |cursor| format!("has applied through block {}", cursor.block_number.0),
        );
        bail!(
            "processor {} is ordered: its history applies contiguously from start_block {} upward, and it {progress}; start this backfill at block {next}",
            configured.instance,
            configured.start_block
        );
    }
    Ok(())
}

/// The configured processor that `leani backfill --processor processor_id`
/// runs: one whose history the node owns and materializes on demand. The
/// node's own automatic job owns an automatic processor's history, and
/// application subscriptions own theirs.
fn backfill_processor_config<'a>(
    config: &'a Config,
    processor_id: &str,
) -> Result<&'a ProcessorConfig> {
    let configured = select_processor_config(config, processor_id)?;
    if configured.history_control != crate::config::ProcessorHistoryControl::NodeOwned {
        bail!(
            "processor {processor_id} history is owned by application subscriptions; create a backfill subscription through the API"
        );
    }
    if configured.history_mode != crate::config::ProcessorHistoryMode::OnDemand {
        bail!(
            "automatic_job_owns_history: processor {processor_id} uses automatic history; configure on_demand for explicit materialization ranges"
        );
    }
    Ok(configured)
}

async fn e2e(
    command: E2eCommand,
    config_path: &Path,
    registry: &ProcessorRegistry,
) -> Result<Exit> {
    match command {
        E2eCommand::Fixture {
            data_dir,
            blocks,
            report,
        } => fixture_e2e(&data_dir, blocks, report.as_deref()).await,
        E2eCommand::Mainnet {
            processor,
            from_block,
            data_dir,
            resume,
            minimum_follow_blocks,
            stable_seconds,
            max_head_age_seconds,
            timeout_seconds,
            report,
        } => {
            mainnet_e2e(
                config_path,
                MainnetE2eOptions {
                    processor,
                    from_block,
                    data_dir,
                    resume,
                    minimum_follow_blocks,
                    stable_seconds,
                    max_head_age_seconds,
                    timeout_seconds,
                    report,
                },
                registry,
            )
            .await
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct FixtureE2eReport {
    report_version: u32,
    status: &'static str,
    node_version: &'static str,
    blocks: u64,
    coverage: Vec<leani_primitives::BlockRange>,
    changes: usize,
    database_verified: bool,
    store: leani_store_sqlite::StoreStats,
    processor_store: leani_store_sqlite::ProcessorStoreStats,
}

async fn fixture_e2e(data_dir: &Path, blocks: u64, report: Option<&Path>) -> Result<Exit> {
    use std::sync::Arc;

    use leani_primitives::{BlockHash, BlockNumber, BlockRange, ChainId};
    use leani_processor_api::Processor;
    use leani_runtime::{BackfillJob, HistoricalRuntime, HistoricalRuntimeConfig};
    use leani_source_api::{SourceBudget, VerificationPolicy};
    use leani_store_sqlite::{SqliteStore, StoreConfig};
    use leani_testkit::{
        BlockLocalCounter, ScriptedHistorySource, fixture_frame, fixture_source_descriptor,
    };

    if !(1..=10_000).contains(&blocks) {
        bail!("fixture block count must be within 1..=10000");
    }
    let _data_dir_lock = crate::local_state::lock_runtime_directory(data_dir)?;
    let database = data_dir.join("leani.sqlite");
    if database.exists() {
        bail!(
            "{} already exists; choose a fresh fixture directory",
            database.display()
        );
    }
    std::fs::create_dir_all(data_dir)
        .with_context(|| format!("create fixture directory {}", data_dir.display()))?;
    let range = BlockRange::new(BlockNumber(1), BlockNumber(blocks))?;
    let mut parent = BlockHash::ZERO;
    let frames = range
        .iter()
        .map(|number| {
            let frame = fixture_frame(number.0, parent);
            parent = frame.block.hash;
            frame
        })
        .collect::<Vec<_>>();
    let source = Arc::new(ScriptedHistorySource::from_frames(
        fixture_source_descriptor("quickstart-fixture", range),
        frames,
    ));
    let processor = Arc::new(BlockLocalCounter::default());
    let store = SqliteStore::open(StoreConfig::new(database)).await?;
    let runtime = HistoricalRuntime::new(
        store.clone(),
        source,
        processor.clone(),
        HistoricalRuntimeConfig {
            mapper_concurrency: 4,
            ..HistoricalRuntimeConfig::default()
        },
    )?;
    let job = BackfillJob::for_processor(
        "quickstart-fixture",
        processor.as_ref(),
        ChainId(1),
        range,
        VerificationPolicy::CompleteCryptographic,
    )?;
    runtime
        .run(
            job,
            SourceBudget {
                max_input_bytes: 64 * 1_024 * 1_024,
                max_frame_bytes: 1024 * 1024,
                max_frames: blocks,
                max_buffered_frames: 8,
                max_in_flight_requests: 1,
                temporary_disk_bytes: 64 * 1_024 * 1_024,
                max_resident_bytes: 64 * 1_024 * 1_024,
            },
            CancellationToken::new(),
        )
        .await?;
    store.verify().await?;
    let coverage = store.coverage(processor.descriptor(), range).await?;
    let changes = store
        .changes(
            processor.descriptor(),
            ChainId(1),
            0,
            usize::try_from(blocks).unwrap_or(10_000),
        )
        .await?;
    let fixture_report = FixtureE2eReport {
        report_version: 1,
        status: "passed",
        node_version: env!("CARGO_PKG_VERSION"),
        blocks,
        coverage,
        changes: changes.len(),
        database_verified: true,
        store: store.stats().await?,
        processor_store: store.processor_stats(processor.descriptor()).await?,
    };
    if let Some(path) = report {
        write_json(path, &fixture_report)?;
    }
    println!("{}", serde_json::to_string_pretty(&fixture_report)?);
    Ok(Exit::Success)
}

#[derive(Debug)]
struct MainnetE2eOptions {
    processor: String,
    from_block: u64,
    data_dir: std::path::PathBuf,
    resume: bool,
    minimum_follow_blocks: u64,
    stable_seconds: u64,
    max_head_age_seconds: u64,
    timeout_seconds: u64,
    report: std::path::PathBuf,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct MainnetE2eReport {
    report_version: u32,
    status: &'static str,
    node_version: &'static str,
    git_commit: &'static str,
    started_at_unix_ms: u64,
    finished_at_unix_ms: u64,
    elapsed_milliseconds: u64,
    config_path: String,
    data_dir: String,
    processor: String,
    from_block: u64,
    minimum_follow_blocks: u64,
    stable_seconds: u64,
    max_head_age_seconds: u64,
    timeout_seconds: u64,
    observed_latest_block: Option<u64>,
    observed_head_age_seconds: Option<u64>,
    database_verified: bool,
    errors: Vec<String>,
    store: leani_store_sqlite::StoreStats,
    processors: Vec<MainnetE2eProcessorReport>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct MainnetE2eProcessorReport {
    descriptor: leani_processor_api::ProcessorDescriptor,
    cursor: Option<leani_primitives::ProcessorCursor>,
    coverage: Vec<leani_primitives::BlockRange>,
    store: leani_store_sqlite::ProcessorStoreStats,
    handoff: Option<leani_store_sqlite::HotColdHandoffRecord>,
    latest_block_timestamp: Option<u64>,
    latest_block_age_seconds: Option<u64>,
}

#[derive(Clone, Copy, Debug)]
struct MainnetE2eObservation {
    latest_block: u64,
    latest_block_age_seconds: u64,
}

#[allow(clippy::too_many_lines)]
async fn mainnet_e2e(
    config_path: &Path,
    options: MainnetE2eOptions,
    registry: &ProcessorRegistry,
) -> Result<Exit> {
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    use leani_api::ReadinessHandle;
    use leani_primitives::ChainId;
    use leani_store_sqlite::SqliteStore;

    if options.from_block == 0
        || options.minimum_follow_blocks == 0
        || options.stable_seconds == 0
        || options.max_head_age_seconds == 0
        || options.timeout_seconds <= options.stable_seconds
    {
        bail!(
            "Mainnet E2E block/freshness bounds must be non-zero and timeout must exceed stable time"
        );
    }
    // The run owns its data directory as a node does: never open a store,
    // peer store or identity that a node or another run is using.
    let _data_dir_lock = crate::local_state::lock_runtime_directory(&options.data_dir)?;
    let database_path = options.data_dir.join("leani.sqlite");
    if database_path.exists() && !options.resume {
        bail!(
            "{} already exists; pass --resume to reuse it",
            database_path.display()
        );
    }
    let mut config = Config::load(config_path)
        .with_context(|| format!("load configuration {}", config_path.display()))?;
    if config.chain.chain_id != 1
        || !matches!(config.sources.live.kind, crate::config::LiveSourceKind::P2p)
        || matches!(
            config.finality.kind,
            crate::config::FinalitySourceKind::Disabled
        )
    {
        bail!("Mainnet E2E requires chain ID 1, execution P2P, and verified finality");
    }
    let selected = select_processor_config(&config, &options.processor)?;
    let selected_key = selected.instance.clone();
    let selected_kind = selected.id.clone();
    config.processors.retain(|processor| {
        processor.instance == selected_key
            || (selected_kind != "blobs-money" && processor.id == "blobs-money")
    });
    for processor in &mut config.processors {
        processor.start_block = options.from_block;
    }
    config.data_dir.clone_from(&options.data_dir);
    let config = config.validate()?.into_inner();
    let processors = registry.instantiate_all(&config)?;
    for processor in &processors {
        configured_history_sources(&config, processor.as_ref(), None).with_context(|| {
            format!(
                "processor {} has no capable Mainnet history source",
                processor.descriptor().id
            )
        })?;
    }

    // A resumed run registers its processors in the store it reopens.
    let store = SqliteStore::open(
        configured_store_config(&config, &database_path)
            .with_processors(processor_descriptors(&processors)),
    )
    .await?;
    let readiness = ReadinessHandle::new(true, true);
    let rpc_readiness = leani_rpc::RpcReadiness::default();
    let network_telemetry = leani_source_api::NetworkTelemetry::default();
    let (committed_events, _) = tokio::sync::broadcast::channel(1_024);
    let cancellation = CancellationToken::new();
    let lane = {
        let config = config.clone();
        let store = store.clone();
        let processors = processors.clone();
        let handles = NetworkLaneHandles {
            readiness: readiness.clone(),
            rpc_readiness: rpc_readiness.clone(),
            committed_events: committed_events.clone(),
            network_telemetry: network_telemetry.clone(),
            cancellation: cancellation.clone(),
            backfill_control: None,
            verified_anchor: None,
            checkpoint_origin: leani_finality_beacon_api::CheckpointOrigin::Operator,
            attested_heads: leani_source_api::AttestedHeadPublisher::new(),
        };
        let live_source = execution_p2p_source(
            &config,
            handles.network_telemetry.clone(),
            Some(handles.attested_heads.subscribe()),
        )?;
        tokio::spawn(async move {
            Box::pin(run_network_lanes_once(
                &config,
                store,
                processors,
                handles,
                live_source,
            ))
            .await
        })
    };
    let started = Instant::now();
    let started_at_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX);
    let monitor = {
        let monitor = tokio::time::timeout(
            Duration::from_secs(options.timeout_seconds),
            monitor_mainnet_e2e(
                &store,
                &processors,
                ChainId(config.chain.chain_id),
                &options,
                &lane,
            ),
        );
        tokio::pin!(monitor);
        tokio::select! {
            result = &mut monitor => result
                .map_err(|_| {
                    anyhow::anyhow!(
                        "Mainnet E2E timed out after {} seconds",
                        options.timeout_seconds
                    )
                })
                .and_then(std::convert::identity),
            signal = shutdown_signal() => Err(match signal {
                Ok(()) => anyhow::anyhow!("Mainnet E2E interrupted by shutdown signal"),
                Err(error) => anyhow::anyhow!("Mainnet E2E signal handler failed: {error}"),
            }),
        }
    };

    cancellation.cancel();
    let lane_result = tokio::time::timeout(Duration::from_secs(30), lane)
        .await
        .map_err(|_| anyhow::anyhow!("network lane did not stop within 30 seconds"))
        .and_then(|result| {
            result
                .context("Mainnet E2E network task panicked")
                .and_then(std::convert::identity)
        });
    let database_result = store.verify().await;
    let mut errors = Vec::new();
    let observation = match monitor {
        Ok(observation) => Some(observation),
        Err(error) => {
            errors.push(error.to_string());
            None
        }
    };
    if let Err(error) = lane_result {
        errors.push(format!("network lane: {error:#}"));
    }
    if let Err(error) = &database_result {
        errors.push(format!("database verification: {error}"));
    }
    let processor_reports = collect_mainnet_e2e_processors(
        &store,
        &processors,
        ChainId(config.chain.chain_id),
        options.from_block,
    )
    .await?;
    let finished_at_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX);
    let report = MainnetE2eReport {
        report_version: 1,
        status: if errors.is_empty() {
            "passed"
        } else {
            "failed"
        },
        node_version: env!("CARGO_PKG_VERSION"),
        git_commit: option_env!("LEANI_GIT_COMMIT").unwrap_or("unknown"),
        started_at_unix_ms,
        finished_at_unix_ms,
        elapsed_milliseconds: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        config_path: config_path.display().to_string(),
        data_dir: options.data_dir.display().to_string(),
        processor: options.processor,
        from_block: options.from_block,
        minimum_follow_blocks: options.minimum_follow_blocks,
        stable_seconds: options.stable_seconds,
        max_head_age_seconds: options.max_head_age_seconds,
        timeout_seconds: options.timeout_seconds,
        observed_latest_block: observation.map(|observation| observation.latest_block),
        observed_head_age_seconds: observation
            .map(|observation| observation.latest_block_age_seconds),
        database_verified: database_result.is_ok(),
        errors,
        store: store.stats().await?,
        processors: processor_reports,
    };
    write_json(&options.report, &report)?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    if !report.errors.is_empty() {
        bail!("Mainnet E2E failed: {}", report.errors.join("; "));
    }
    Ok(Exit::Success)
}

async fn monitor_mainnet_e2e(
    store: &leani_store_sqlite::SqliteStore,
    processors: &[std::sync::Arc<dyn leani_processor_api::Processor>],
    chain_id: leani_primitives::ChainId,
    options: &MainnetE2eOptions,
    lane: &tokio::task::JoinHandle<Result<()>>,
) -> Result<MainnetE2eObservation> {
    let mut stable_since = None;
    loop {
        if lane.is_finished() {
            bail!("network lane exited before the convergence gate passed");
        }
        let observation = mainnet_e2e_observation(
            store,
            processors,
            chain_id,
            options.from_block,
            options.minimum_follow_blocks,
            options.max_head_age_seconds,
        )
        .await?;
        if let Some(observation) = observation {
            let since = stable_since.get_or_insert_with(std::time::Instant::now);
            if since.elapsed() >= std::time::Duration::from_secs(options.stable_seconds) {
                return Ok(observation);
            }
        } else {
            stable_since = None;
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}

async fn mainnet_e2e_observation(
    store: &leani_store_sqlite::SqliteStore,
    processors: &[std::sync::Arc<dyn leani_processor_api::Processor>],
    chain_id: leani_primitives::ChainId,
    from_block: u64,
    minimum_follow_blocks: u64,
    max_head_age_seconds: u64,
) -> Result<Option<MainnetE2eObservation>> {
    use std::time::{SystemTime, UNIX_EPOCH};

    use leani_primitives::{BlockNumber, BlockRange};
    use leani_store_sqlite::HotColdHandoffState;

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let mut latest_block = u64::MAX;
    let mut greatest_age = 0_u64;
    for processor in processors {
        let descriptor = processor.descriptor();
        let Some(handoff) = store.latest_hot_cold_handoff(descriptor).await? else {
            return Ok(None);
        };
        if handoff.state != HotColdHandoffState::Verified {
            return Ok(None);
        }
        let Some(cursor) = store.processor_cursor(descriptor).await? else {
            return Ok(None);
        };
        if cursor.block_number.0
            < handoff
                .overlap
                .end()
                .0
                .saturating_add(minimum_follow_blocks)
        {
            return Ok(None);
        }
        let required = BlockRange::new(BlockNumber(from_block), cursor.block_number)
            .map_err(anyhow::Error::msg)?;
        if store.coverage(descriptor, required).await? != vec![required] {
            return Ok(None);
        }
        if store.processor_stats(descriptor).await?.pending_deltas != 0 {
            return Ok(None);
        }
        let Some(frame) = store.recent_frame(chain_id, cursor.block_number).await? else {
            return Ok(None);
        };
        let age = now.saturating_sub(frame.block.timestamp);
        if age > max_head_age_seconds {
            return Ok(None);
        }
        latest_block = latest_block.min(cursor.block_number.0);
        greatest_age = greatest_age.max(age);
    }
    Ok(Some(MainnetE2eObservation {
        latest_block,
        latest_block_age_seconds: greatest_age,
    }))
}

async fn collect_mainnet_e2e_processors(
    store: &leani_store_sqlite::SqliteStore,
    processors: &[std::sync::Arc<dyn leani_processor_api::Processor>],
    chain_id: leani_primitives::ChainId,
    from_block: u64,
) -> Result<Vec<MainnetE2eProcessorReport>> {
    use std::time::{SystemTime, UNIX_EPOCH};

    use leani_primitives::{BlockNumber, BlockRange};

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let mut reports = Vec::with_capacity(processors.len());
    for processor in processors {
        let descriptor = processor.descriptor();
        let cursor = store.processor_cursor(descriptor).await?;
        let coverage = if let Some(cursor) = &cursor {
            store
                .coverage(
                    descriptor,
                    BlockRange::new(BlockNumber(from_block), cursor.block_number)
                        .map_err(anyhow::Error::msg)?,
                )
                .await?
        } else {
            Vec::new()
        };
        let latest = if let Some(cursor) = &cursor {
            store.recent_frame(chain_id, cursor.block_number).await?
        } else {
            None
        };
        reports.push(MainnetE2eProcessorReport {
            descriptor: descriptor.clone(),
            cursor,
            coverage,
            store: store.processor_stats(descriptor).await?,
            handoff: store.latest_hot_cold_handoff(descriptor).await?,
            latest_block_timestamp: latest.as_ref().map(|frame| frame.block.timestamp),
            latest_block_age_seconds: latest
                .as_ref()
                .map(|frame| now.saturating_sub(frame.block.timestamp)),
        });
    }
    Ok(reports)
}

/// The raw-history material shape of a processor's own backfill requests, so
/// its retained source serves exactly those requests: the filter covering
/// every requirement when all accept filtered material, as
/// `BackfillJob::for_processor` builds it, otherwise none.
fn processor_raw_material_profile(
    processor: &dyn leani_processor_api::Processor,
) -> leani_store_history::RawHistoryMaterialProfile {
    let requirements = &processor.descriptor().requirements;
    let allow_filtered = requirements
        .iter()
        .all(|requirement| requirement.allow_filtered);
    let filters = if allow_filtered {
        let scope = leani_runtime::covering_filter_scope(
            requirements.iter().map(|requirement| &requirement.filter),
        );
        leani_source_api::FilterSet {
            senders: scope.senders.clone(),
            recipients: scope.recipients.clone(),
            scope,
        }
    } else {
        leani_source_api::FilterSet::default()
    };
    leani_store_history::RawHistoryMaterialProfile {
        allow_filtered,
        projection: leani_source_api::FieldProjection::default(),
        log_fields: requirements
            .iter()
            .fold(leani_primitives::LogFieldSet::NONE, |all, requirement| {
                all.union(requirement.log_fields)
            }),
        filters,
    }
}

fn historical_services(
    config: &Config,
) -> Result<(
    leani_runtime::HistoricalPipelineBudget,
    Option<leani_runtime::HistoricalMaterialCoordinator>,
)> {
    let history_pipeline = config.budgets.history_pipeline;
    let pipeline_budget = leani_runtime::HistoricalPipelineBudget::new(
        history_pipeline.maximum_active_chunks,
        historical_map_task_capacity(config),
        history_pipeline.maximum_mapped_bytes.bytes(),
    )
    .map_err(anyhow::Error::msg)?;
    let history_material = config.budgets.history_material;
    let material_coordinator = match history_material.mode {
        HistoryMaterialCoordinatorMode::Disabled => None,
        HistoryMaterialCoordinatorMode::Observe | HistoryMaterialCoordinatorMode::Enabled => {
            let mode = match history_material.mode {
                HistoryMaterialCoordinatorMode::Observe => {
                    leani_runtime::HistoricalMaterialCoordinatorMode::Observe
                }
                HistoryMaterialCoordinatorMode::Enabled => {
                    leani_runtime::HistoricalMaterialCoordinatorMode::Enabled
                }
                HistoryMaterialCoordinatorMode::Disabled => unreachable!(),
            };
            Some(
                leani_runtime::HistoricalMaterialCoordinator::new_with_pipeline_budget(
                    leani_runtime::HistoricalMaterialCoordinatorConfig {
                        mode,
                        memory_bytes: history_material.memory_bytes.bytes(),
                        maximum_buffered_frames_per_acquisition: history_material
                            .maximum_buffered_frames_per_acquisition,
                        minimum_physical_chunk_blocks: history_material
                            .minimum_physical_chunk_blocks,
                        maximum_overfetch_ratio: history_material.maximum_overfetch_ratio,
                    },
                    &pipeline_budget,
                )
                .map_err(anyhow::Error::msg)?,
            )
        }
    };
    Ok((pipeline_budget, material_coordinator))
}

/// Static source discovery used by diagnostics and explicitly static runs.
pub(crate) fn configured_history_sources(
    config: &Config,
    processor: &dyn leani_processor_api::Processor,
    raw_history_store: Option<&leani_store_history::HistoryStore>,
) -> Result<(
    Vec<Arc<dyn leani_source_api::HistorySource>>,
    leani_source_api::VerificationPolicy,
)> {
    let (sources, policy, _) = configured_history_candidates(config, processor, raw_history_store)?;
    require_history_sources(&sources, processor)?;
    Ok((sources, policy))
}

fn require_history_sources(
    sources: &[Arc<dyn leani_source_api::HistorySource>],
    processor: &dyn leani_processor_api::Processor,
) -> Result<()> {
    if sources.is_empty() {
        bail!(
            "no implemented history source can satisfy processor {}; configure compatible history or enable P2P with verified finality",
            processor.descriptor().id
        );
    }
    Ok(())
}

/// The same source assembly for standalone CLI, API jobs and automatic
/// backfill. The bridge is added before deciding that no source is viable.
fn history_sources_with_bridge(
    config: &Config,
    processor: &dyn leani_processor_api::Processor,
    raw_history_store: Option<&leani_store_history::HistoryStore>,
    bridge: Option<&OnDemandP2pBridge>,
    requested: leani_primitives::BlockRange,
) -> Result<(
    Vec<Arc<dyn leani_source_api::HistorySource>>,
    leani_source_api::VerificationPolicy,
)> {
    // Once per process: live-gap recovery assembles sources for every chunk.
    static REPORTED: std::sync::Mutex<std::collections::BTreeSet<String>> =
        std::sync::Mutex::new(std::collections::BTreeSet::new());
    let (mut sources, policy, excluded) =
        configured_history_candidates(config, processor, raw_history_store)?;
    for reason in excluded {
        let reason = format!("{}: {reason}", processor.descriptor().instance);
        if REPORTED
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(reason.clone())
        {
            info!(%reason, "a configured history source cannot serve this processor");
        }
    }
    let configured = config_for_processor_descriptor(config, processor.descriptor())?;
    if !configured.require_retained_input
        && let Some(bridge) = bridge
    {
        let through = bridge.anchor.block.number.0;
        let start = config
            .sources
            .live
            .history_fallback_start(through, configured.start_block);
        if requested.end().0 >= start && requested.start().0 <= through {
            let available = leani_primitives::BlockRange::new(
                leani_primitives::BlockNumber(start),
                bridge.anchor.block.number,
            )?;
            sources.push(Arc::new(
                leani_source_p2p::RethP2pHistorySource::from_live_source(
                    bridge.source.clone(),
                    available,
                    bridge.anchor.clone(),
                )?,
            ));
        }
    }
    require_history_sources(&sources, processor)?;
    Ok((sources, policy))
}

/// The block the on-demand P2P history bridge must reach before a job
/// ending at `job_end` starts, or `None` when the job need not wait.
///
/// A job's sources are fixed for its run, so one that starts before the
/// bridge covers it never gets the bridge. The bridge serves a job that ends
/// at or after `fallback_start`, unless its processor reads retained input
/// only.
fn bridge_wait(
    expected: bool,
    require_retained_input: bool,
    bridge_anchor: Option<u64>,
    fallback_start: u64,
    job_end: u64,
) -> Option<u64> {
    if !expected
        || require_retained_input
        || job_end < fallback_start
        || bridge_anchor.is_some_and(|anchor| anchor >= job_end)
    {
        None
    } else {
        Some(job_end)
    }
}

type HistorySources = Vec<Arc<dyn leani_source_api::HistorySource>>;

/// Also returns why each configured source it left out cannot serve the
/// processor.
#[allow(clippy::too_many_lines)]
fn configured_history_candidates(
    config: &Config,
    processor: &dyn leani_processor_api::Processor,
    raw_history_store: Option<&leani_store_history::HistoryStore>,
) -> Result<(
    HistorySources,
    leani_source_api::VerificationPolicy,
    Vec<String>,
)> {
    use leani_source_api::{HistorySource, VerificationPolicy};
    use leani_source_archive::LocalArchiveSource;
    use leani_source_erae::{EraeConfig, EraeSource};
    use leani_source_xatu::{XatuBlobsHistorySource, XatuHistoryConfig};

    let requirements = &processor.descriptor().requirements;
    let required = requirements
        .iter()
        .fold(leani_primitives::CapabilitySet::NONE, |all, requirement| {
            all.union(requirement.capabilities)
        });
    let allow_filtered = requirements
        .iter()
        .all(|requirement| requirement.allow_filtered);
    let log_fields = requirements
        .iter()
        .fold(leani_primitives::LogFieldSet::NONE, |all, requirement| {
            all.union(requirement.log_fields)
        });
    let configured_processor = config_for_processor_descriptor(config, processor.descriptor())?;
    if configured_processor.require_retained_input && raw_history_store.is_none() {
        bail!(
            "processor {} requires retained input but raw_history is disabled",
            processor.descriptor().instance
        );
    }
    let mut candidates = config
        .sources
        .history
        .iter()
        .filter(|source| {
            matches!(
                source.kind,
                crate::config::HistorySourceKind::Xatu
                    | crate::config::HistorySourceKind::EraE
                    | crate::config::HistorySourceKind::Archive
            )
        })
        .collect::<Vec<_>>();
    candidates.sort_by_key(|source| source.priority);
    let mut selected = Vec::new();
    let mut excluded = Vec::new();
    let mut policy = VerificationPolicy::CompleteCryptographic;
    for configured in candidates {
        let source: std::sync::Arc<dyn HistorySource> = match configured.kind {
            crate::config::HistorySourceKind::Xatu => {
                let mut xatu = XatuHistoryConfig::public("mainnet")?;
                xatu.priority = configured.priority;
                if let Some(chunk_blocks) = configured.chunk_blocks {
                    xatu.chunk_blocks = chunk_blocks;
                }
                if let Some(chunk_blocks) = configured.blobs_chunk_blocks {
                    xatu.blobs_chunk_blocks = chunk_blocks;
                }
                if let Some(batch_rows) = configured.batch_rows {
                    xatu.batch_rows = batch_rows;
                }
                std::sync::Arc::new(XatuBlobsHistorySource::new(xatu)?)
            }
            crate::config::HistorySourceKind::Archive => {
                std::sync::Arc::new(LocalArchiveSource::open_manifest(
                    configured
                        .manifest
                        .as_deref()
                        .context("archive history source has no manifest")?,
                )?)
            }
            crate::config::HistorySourceKind::EraE => {
                if config.chain.chain_id != 1 {
                    bail!("native eraE history currently supports Ethereum mainnet only");
                }
                let mut erae = EraeConfig::public_mainnet()?;
                erae.id = leani_primitives::SourceId::new(&configured.id)?;
                erae.priority = configured.priority;
                if let Some(endpoint) = &configured.endpoint {
                    erae.base_url = endpoint.clone();
                }
                erae.allow_insecure_http = configured.allow_insecure_http;
                std::sync::Arc::new(EraeSource::new(erae)?)
            }
            // Validation refuses `parquet` sources.
            crate::config::HistorySourceKind::Parquet => continue,
        };
        let descriptor = source.descriptor();
        if !descriptor.capabilities.contains_all(required) {
            excluded.push(format!(
                "{}: it does not serve every material kind the processor requires",
                descriptor.id
            ));
        } else if !allow_filtered && !descriptor.complete_capabilities.contains_all(required) {
            excluded.push(format!(
                "{}: it serves the required material only filtered, and the processor reads whole blocks",
                descriptor.id
            ));
        } else {
            if matches!(
                configured.trust,
                crate::config::HistoryTrust::TrustedDataset
            ) {
                policy = VerificationPolicy::TrustedDataset;
            }
            selected.push(source);
        }
    }
    if let Some(store) = raw_history_store {
        requirements
            .first()
            .context("processor has no material requirements")?;
        let material = processor_raw_material_profile(processor);
        let retained: std::sync::Arc<dyn HistorySource> = std::sync::Arc::new(
            leani_store_history::RetainedHistorySource::new(
                store.clone(),
                leani_store_history::RetainedHistorySourceConfig::local(
                    leani_primitives::ChainId(config.chain.chain_id),
                    material.shape_id(),
                    required,
                    leani_store_history::VerificationClass::TrustedDataset,
                    leani_primitives::TrustModel::TrustedDataset,
                )
                .map(|config| {
                    config
                        .with_log_fields(log_fields)
                        .with_material_profile(material.clone())
                })
                .map_err(anyhow::Error::msg)?,
            )
            .map_err(anyhow::Error::msg)?,
        );
        if configured_processor.require_retained_input {
            selected.clear();
        }
        selected.insert(0, retained);
        if material.shape_id() != leani_store_history::MaterialShapeId::COMPLETE_EXECUTION {
            let mut complete_config = leani_store_history::RetainedHistorySourceConfig::local(
                leani_primitives::ChainId(config.chain.chain_id),
                leani_store_history::MaterialShapeId::COMPLETE_EXECUTION,
                required,
                leani_store_history::VerificationClass::TrustedDataset,
                leani_primitives::TrustModel::TrustedDataset,
            )
            .map_err(anyhow::Error::msg)?;
            complete_config.priority = 1;
            selected.insert(
                1,
                std::sync::Arc::new(
                    leani_store_history::RetainedHistorySource::new(store.clone(), complete_config)
                        .map_err(anyhow::Error::msg)?,
                ),
            );
        }
        policy = VerificationPolicy::TrustedDataset;
    }
    Ok((selected, policy, excluded))
}

fn retained_rpc_log_sources(
    chain_id: leani_primitives::ChainId,
    store: &leani_store_history::HistoryStore,
    processors: &[Arc<dyn leani_processor_api::Processor>],
) -> Result<Vec<Arc<dyn leani_source_api::HistorySource>>> {
    use leani_primitives::{Capability, CapabilitySet, LogFieldSet, TrustModel};
    use leani_store_history::{
        RawHistoryMaterialProfile, RetainedHistorySource, RetainedHistorySourceConfig,
        VerificationClass,
    };
    let mut profiles = vec![RawHistoryMaterialProfile {
        allow_filtered: true,
        ..RawHistoryMaterialProfile::default()
    }];
    for processor in processors {
        if processor
            .descriptor()
            .requirements
            .iter()
            .any(|requirement| requirement.capabilities.contains(Capability::Logs))
        {
            let profile = processor_raw_material_profile(processor.as_ref());
            if profile.allow_filtered && profile.log_fields.contains_all(LogFieldSet::ALL) {
                profiles.push(profile);
            }
        }
    }
    let mut shapes = std::collections::BTreeSet::new();
    let mut sources = Vec::new();
    for profile in profiles {
        let shape = profile.shape_id();
        if !shapes.insert(shape.0) {
            continue;
        }
        let config = RetainedHistorySourceConfig::local(
            chain_id,
            shape,
            CapabilitySet::of(Capability::Logs),
            VerificationClass::TrustedDataset,
            TrustModel::TrustedDataset,
        )?
        .with_material_profile(profile);
        sources.push(Arc::new(RetainedHistorySource::new(store.clone(), config)?)
            as Arc<dyn leani_source_api::HistorySource>);
    }
    Ok(sources)
}

fn configured_rpc_history_sources(
    config: &Config,
) -> Result<Vec<std::sync::Arc<dyn leani_source_api::HistorySource>>> {
    use leani_source_api::HistorySource;
    use leani_source_archive::LocalArchiveSource;
    use leani_source_erae::{EraeConfig, EraeSource};
    use leani_source_xatu::{XatuBlobsHistorySource, XatuHistoryConfig};

    let mut configured = config.sources.history.iter().collect::<Vec<_>>();
    configured.sort_by_key(|source| source.priority);
    let mut sources: Vec<std::sync::Arc<dyn HistorySource>> = Vec::new();
    for source in configured {
        match source.kind {
            crate::config::HistorySourceKind::Xatu => {
                let mut xatu = XatuHistoryConfig::public("mainnet")?;
                xatu.priority = source.priority;
                if let Some(chunk_blocks) = source.chunk_blocks {
                    xatu.chunk_blocks = chunk_blocks;
                }
                if let Some(chunk_blocks) = source.blobs_chunk_blocks {
                    xatu.blobs_chunk_blocks = chunk_blocks;
                }
                if let Some(batch_rows) = source.batch_rows {
                    xatu.batch_rows = batch_rows;
                }
                sources.push(std::sync::Arc::new(XatuBlobsHistorySource::new(xatu)?));
            }
            crate::config::HistorySourceKind::Archive => {
                sources.push(std::sync::Arc::new(LocalArchiveSource::open_manifest(
                    source
                        .manifest
                        .as_deref()
                        .context("archive history source has no manifest")?,
                )?));
            }
            crate::config::HistorySourceKind::EraE => {
                if config.chain.chain_id != 1 {
                    bail!("native eraE history currently supports Ethereum mainnet only");
                }
                let mut erae = EraeConfig::public_mainnet()?;
                erae.id = leani_primitives::SourceId::new(&source.id)?;
                erae.priority = source.priority;
                if let Some(endpoint) = &source.endpoint {
                    erae.base_url = endpoint.clone();
                }
                erae.allow_insecure_http = source.allow_insecure_http;
                sources.push(std::sync::Arc::new(EraeSource::new(erae)?));
            }
            // Validation refuses `parquet` sources.
            crate::config::HistorySourceKind::Parquet => {}
        }
    }
    if sources.is_empty() {
        bail!("on-demand RPC has no implemented configured history source");
    }
    Ok(sources)
}

async fn db(command: DbCommand, config_path: &Path, registry: &ProcessorRegistry) -> Result<Exit> {
    use leani_store_sqlite::SqliteStore;

    let config = Config::load(config_path)
        .with_context(|| format!("load configuration {}", config_path.display()))?
        .validate()
        .map_err(|errors| anyhow::anyhow!(errors))?
        .into_inner();
    let _data_dir_lock = crate::local_state::lock_runtime_directory(&config.data_dir)?;
    let database_path = config.data_dir.join("leani.sqlite");
    // A backup copies the store at its schema. Opening it as a store would
    // first upgrade an older one, which cannot be undone.
    if let DbCommand::Backup { destination } = &command {
        SqliteStore::backup(&database_path, destination)
            .await
            .with_context(|| format!("back up store {}", database_path.display()))?;
        println!(
            "{}",
            serde_json::json!({
                "ok": true,
                "database": database_path.display().to_string(),
                "backup": destination.display().to_string()
            })
        );
        return Ok(Exit::Success);
    }
    let store = SqliteStore::open(configured_store_config(&config, &database_path))
        .await
        .with_context(|| format!("open store {}", database_path.display()))?;
    match command {
        DbCommand::Inspect => {
            let mut processors = Vec::new();
            for configured in &config.processors {
                let processor = registry.instantiate(configured, config.chain.chain_id)?;
                processors.push(store.processor_stats(processor.descriptor()).await?);
            }
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "store": store.stats().await?,
                    "recent": store
                        .recent_stats(leani_primitives::ChainId(config.chain.chain_id))
                        .await?,
                    "processors": processors
                }))?
            );
        }
        DbCommand::Verify => {
            store.verify().await?;
            println!(
                "{}",
                serde_json::json!({
                    "ok": true,
                    "database": database_path.display().to_string()
                })
            );
        }
        DbCommand::Backup { .. } => unreachable!("a backup returns before the store opens"),
        DbCommand::Compact => {
            store.compact().await?;
            println!(
                "{}",
                serde_json::json!({
                    "ok": true,
                    "database": database_path.display().to_string()
                })
            );
        }
        DbCommand::PruneChanges { processor, before } => {
            let configured = select_processor_config(&config, &processor)?;
            let processor = registry.instantiate(configured, config.chain.chain_id)?;
            let outcome = store
                .prune_changes_before(processor.descriptor(), before)
                .await?;
            println!("{}", serde_json::to_string_pretty(&outcome)?);
        }
    }
    Ok(Exit::Success)
}

async fn probe_source(source: ProbeSource, config_path: &Path) -> Result<Exit> {
    match source {
        ProbeSource::Xatu {
            network,
            from_block,
            to_block,
            processor,
            from_date,
            to_date,
            concurrency,
            max_input_bytes,
            batch_rows,
            report,
            projection_output,
            expected_export,
        } => {
            probe_xatu(XatuProbeOptions {
                network,
                from_block,
                to_block,
                processor,
                dates: from_date.zip(to_date),
                concurrency,
                max_input_bytes,
                batch_rows,
                report,
                projection_output,
                expected_export,
            })
            .await
        }
        ProbeSource::Finality {
            checkpoint,
            checkpoint_slot,
            endpoints,
            minimum_agreement,
            report,
        } => {
            probe_finality(
                config_path,
                checkpoint.as_deref(),
                checkpoint_slot,
                endpoints,
                minimum_agreement,
                report.as_deref(),
            )
            .await
        }
        ProbeSource::P2p {
            from_block,
            to_block,
            expected_tip,
            minimum_peers,
            peer_wait_seconds,
            request_timeout_seconds,
            retries,
            retry_backoff_seconds,
            max_input_bytes,
            report,
            output,
        } => {
            probe_p2p(
                config_path,
                P2pProbeOptions {
                    from_block,
                    to_block,
                    expected_tip,
                    minimum_peers,
                    peer_wait_seconds,
                    request_timeout_seconds,
                    retries,
                    retry_backoff_seconds,
                    max_input_bytes,
                    report,
                    output,
                },
            )
            .await
        }
        ProbeSource::Erae {
            from_block,
            to_block,
            endpoint,
            max_input_bytes,
            report,
            output,
        } => {
            probe_erae(
                config_path,
                from_block,
                to_block,
                endpoint,
                max_input_bytes,
                report.as_deref(),
                output.as_deref(),
            )
            .await
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn probe_erae(
    config_path: &Path,
    from_block: u64,
    to_block: u64,
    endpoint: Option<url::Url>,
    max_input_bytes: u64,
    report_path: Option<&Path>,
    output_path: Option<&Path>,
) -> Result<Exit> {
    use leani_primitives::{BlockNumber, BlockRange};
    use leani_source_api::SourceBudget;
    use leani_source_erae::{EraeConfig, EraeSource};

    let config = Config::load(config_path)
        .with_context(|| format!("load configuration {}", config_path.display()))?;
    if config.chain.chain_id != 1 {
        bail!("eraE probe currently supports Ethereum mainnet only");
    }
    let range = BlockRange::new(BlockNumber(from_block), BlockNumber(to_block))?;
    let mut erae = EraeConfig::public_mainnet()?;
    if let Some(endpoint) = endpoint {
        erae.base_url = endpoint;
    }
    let source = EraeSource::new(erae)?;
    let buffered = usize::try_from(range.len().min(64)).unwrap_or(64);
    let (frames, report) = source
        .probe_range(
            range,
            SourceBudget {
                max_input_bytes,
                max_frame_bytes: max_input_bytes.min(32 * 1_024 * 1_024),
                max_frames: range.len(),
                max_buffered_frames: buffered.max(1),
                max_in_flight_requests: 1,
                temporary_disk_bytes: 1,
                max_resident_bytes: max_input_bytes,
            },
            CancellationToken::new(),
        )
        .await?;
    if let Some(path) = report_path {
        write_json(path, &report)?;
    }
    if let Some(path) = output_path {
        write_json(path, &frames)?;
    }
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(Exit::Success)
}

#[derive(Debug)]
struct P2pProbeOptions {
    from_block: u64,
    to_block: u64,
    expected_tip: Option<String>,
    minimum_peers: usize,
    peer_wait_seconds: u64,
    request_timeout_seconds: u64,
    retries: usize,
    retry_backoff_seconds: u64,
    max_input_bytes: u64,
    report: Option<std::path::PathBuf>,
    output: Option<std::path::PathBuf>,
}

fn p2p_probe_source(
    config: &Config,
    options: &P2pProbeOptions,
) -> Result<leani_source_p2p::RethP2pSource> {
    leani_source_p2p::RethP2pSource::mainnet(p2p_probe_config(config, options)?).map_err(Into::into)
}

fn p2p_probe_config(
    config: &Config,
    options: &P2pProbeOptions,
) -> Result<leani_source_p2p::RethP2pConfig> {
    use std::time::Duration;

    use leani_source_p2p::RethP2pConfig;

    let max_outbound_peers = config
        .sources
        .live
        .max_outbound_peers
        .max(options.minimum_peers);
    let trusted_peers = config
        .sources
        .live
        .trusted_peers
        .iter()
        .map(|peer| leani_source_p2p::parse_trusted_peer(peer))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(RethP2pConfig {
        minimum_peers: options.minimum_peers,
        body_serving_peer_target: config
            .sources
            .live
            .body_serving_peer_target
            .min(max_outbound_peers)
            .max(1),
        preferred_peers: config
            .sources
            .live
            .preferred_peers
            .max(options.minimum_peers)
            .min(max_outbound_peers),
        max_outbound_peers,
        max_concurrent_dials: config
            .sources
            .live
            .max_concurrent_dials
            .min(max_outbound_peers)
            .max(1),
        // The probe runs beside a live node without its data-directory lock,
        // so it must not contend for the node's ports, peer store or identity:
        // it uses ephemeral ports, an in-memory peer store and a throwaway key.
        listener_port: 0,
        discovery_port: 0,
        discv5_port: 0,
        enable_discv5: config.sources.live.enable_discv5,
        nat: leani_source_p2p::parse_nat_resolver(&config.sources.live.nat)?,
        trusted_peers,
        bootstrap_dns_tree: config.sources.live.bootstrap_dns_tree.clone(),
        peer_refill_interval: Duration::from_millis(config.sources.live.peer_refill_interval_ms),
        peer_recovery_timeout: Duration::from_secs(
            config.sources.live.peer_recovery_timeout_seconds,
        ),
        peer_wait_timeout: Duration::from_secs(options.peer_wait_seconds),
        request_timeout: Duration::from_secs(options.request_timeout_seconds),
        retries: options.retries,
        session_retries: 3,
        retry_backoff: Duration::from_secs(options.retry_backoff_seconds),
        retry_backoff_max: Duration::from_secs(config.sources.live.retry_backoff_max_seconds)
            .max(Duration::from_secs(options.retry_backoff_seconds)),
        persistent_retries: false,
        material_request_concurrency: config.sources.live.material_request_concurrency,
        material_request_blocks: config.sources.live.material_request_blocks,
        history_header_request_concurrency: config.sources.live.history_header_request_concurrency,
        history_header_request_blocks: config.sources.live.history_header_request_blocks,
        peer_store_path: None,
        secret_key_path: None,
        peer_store_max_entries: config.sources.live.peer_store_max_entries,
        peer_store_flush_interval: Duration::from_secs(
            config.sources.live.peer_store_flush_seconds,
        ),
        poll_interval: Duration::from_secs(2),
        max_reorg_depth: 64,
        network_telemetry: leani_source_api::NetworkTelemetry::default(),
    })
}

async fn probe_p2p(config_path: &Path, options: P2pProbeOptions) -> Result<Exit> {
    use leani_primitives::{BlockNumber, BlockRange};
    use leani_source_api::SourceBudget;
    use leani_source_p2p::parse_block_hash;

    let config = Config::load(config_path)
        .with_context(|| format!("load configuration {}", config_path.display()))?;
    if config.chain.chain_id != 1 {
        bail!("Reth P2P probe currently supports Ethereum mainnet only");
    }
    let range = BlockRange::new(
        BlockNumber(options.from_block),
        BlockNumber(options.to_block),
    )?;
    let expected_tip = options
        .expected_tip
        .as_deref()
        .map(parse_block_hash)
        .transpose()?;
    let source = p2p_probe_source(&config, &options)?;
    let buffered_frames =
        usize::try_from(range.len()).context("P2P range length does not fit this platform")?;
    let result = source
        .probe_fixed_range(
            range,
            expected_tip,
            SourceBudget {
                max_input_bytes: options.max_input_bytes,
                max_frame_bytes: options.max_input_bytes.min(32 * 1_024 * 1_024),
                max_frames: range.len(),
                max_buffered_frames: buffered_frames,
                max_in_flight_requests: 3,
                temporary_disk_bytes: 1,
                max_resident_bytes: options.max_input_bytes,
            },
            CancellationToken::new(),
        )
        .await;
    source.shutdown().await;
    let result = result?;
    if let Some(path) = options.output.as_deref() {
        write_json(path, &result)?;
    }
    let summary = serde_json::json!({
        "range": result.range,
        "expected_tip": result.expected_tip,
        "first_block": result.frames.first().map(|frame| frame.block),
        "last_block": result.frames.last().map(|frame| frame.block),
        "metrics": result.metrics,
        "output": options.output.as_ref().map(|path| path.display().to_string()),
    });
    if let Some(path) = options.report.as_deref() {
        write_json(path, &summary)?;
    }
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(Exit::Success)
}

async fn probe_finality(
    config_path: &Path,
    checkpoint_override: Option<&str>,
    checkpoint_slot_override: Option<u64>,
    endpoint_overrides: Vec<url::Url>,
    minimum_agreement: Option<usize>,
    report_path: Option<&Path>,
) -> Result<Exit> {
    use leani_finality_beacon_api::{BeaconApiConfig, VerifiedBeaconApi, parse_checkpoint_root};
    use leani_finality_consensus_p2p::VerifiedConsensusP2p;

    let config = Config::load(config_path)
        .with_context(|| format!("load configuration {}", config_path.display()))?;
    if config.chain.chain_id != 1 {
        bail!("verified finality probe currently supports Ethereum mainnet only");
    }
    let checkpoint =
        parse_checkpoint_root(checkpoint_override.unwrap_or(config.finality.checkpoint.as_str()))?;
    let checkpoint_slot = checkpoint_slot_override.unwrap_or(config.finality.checkpoint_slot);
    let trusted_checkpoint = leani_finality_beacon_api::TrustedCheckpoint {
        root: checkpoint,
        slot: (checkpoint_slot > 0).then_some(checkpoint_slot),
        origin: leani_finality_beacon_api::CheckpointOrigin::Operator,
    };
    // A probe starts from the node's persisted anchor when it applies, but
    // never writes it.
    let anchor_file = leani_finality_beacon_api::AnchorFile::ReadOnly(
        config
            .data_dir
            .join(leani_finality_beacon_api::FINALITY_ANCHOR_FILE),
    );
    let (accepted, encoded) = match config.finality.kind {
        crate::config::FinalitySourceKind::BeaconApi => {
            let endpoints = if endpoint_overrides.is_empty() {
                config.finality.endpoints
            } else {
                endpoint_overrides
            };
            let mut beacon_config = BeaconApiConfig::mainnet(endpoints);
            if let Some(minimum_agreement) = minimum_agreement {
                beacon_config.minimum_agreement = minimum_agreement;
            }
            beacon_config.anchor = anchor_file;
            let source = VerifiedBeaconApi::mainnet(beacon_config)?;
            let report = source.probe_root(trusted_checkpoint).await;
            (report.accepted, serde_json::to_string_pretty(&report)?)
        }
        crate::config::FinalitySourceKind::ConsensusP2p => {
            if !endpoint_overrides.is_empty() || minimum_agreement.is_some() {
                bail!(
                    "--endpoint and --minimum-agreement only apply to beacon_api finality probes"
                );
            }
            let mut p2p_config = consensus_p2p_config(&config.finality);
            p2p_config.anchor = anchor_file;
            let source = VerifiedConsensusP2p::mainnet(p2p_config)?;
            if checkpoint_slot == 0 {
                bail!(
                    "consensus_p2p finality probe requires --checkpoint-slot or finality.checkpoint_slot"
                );
            }
            let report = source.probe_checkpoint(trusted_checkpoint).await;
            (report.accepted, serde_json::to_string_pretty(&report)?)
        }
        crate::config::FinalitySourceKind::Disabled => {
            bail!("finality probe cannot run while finality.kind = \"disabled\"");
        }
    };
    if let Some(path) = report_path {
        write_text(path, &format!("{encoded}\n"))?;
        info!(path = %path.display(), "wrote finality probe report");
    } else {
        println!("{encoded}");
    }
    Ok(if accepted {
        Exit::Success
    } else {
        Exit::Failure
    })
}

fn consensus_p2p_config(
    finality: &crate::config::FinalityConfig,
) -> leani_finality_consensus_p2p::ConsensusP2pConfig {
    let mut config = leani_finality_consensus_p2p::ConsensusP2pConfig {
        discovery_port: finality.discovery_port,
        minimum_peers: finality.minimum_peers,
        ..leani_finality_consensus_p2p::ConsensusP2pConfig::default()
    };
    if !finality.bootnodes.is_empty() {
        config.bootnodes.clone_from(&finality.bootnodes);
    }
    config
}

#[derive(Debug)]
struct XatuProbeOptions {
    network: String,
    from_block: u64,
    to_block: u64,
    processor: String,
    dates: Option<(leani_source_xatu::XatuDate, leani_source_xatu::XatuDate)>,
    concurrency: usize,
    max_input_bytes: u64,
    batch_rows: usize,
    report: Option<std::path::PathBuf>,
    projection_output: Option<std::path::PathBuf>,
    expected_export: Option<std::path::PathBuf>,
}

#[derive(Debug, Serialize)]
struct XatuProbeReport {
    catalog: leani_source_xatu::CatalogProbeReport,
    projection: Option<leani_source_xatu::ProjectionMetrics>,
    projection_output: Option<String>,
    parity: Option<leani_processor_blobs::BlobsParityReport>,
}

#[derive(Debug, Serialize)]
struct XatuProjectionOutput<'a> {
    source: &'a leani_source_xatu::BlobsProjection,
    deltas: Vec<leani_processor_blobs::BlobsDelta>,
    compatibility: leani_processor_blobs::BlobsCompatibilityExport,
    parity: Option<leani_processor_blobs::BlobsParityReport>,
}

async fn probe_xatu(options: XatuProbeOptions) -> Result<Exit> {
    use leani_primitives::{BlockNumber, BlockRange};
    use leani_source_api::SourceBudget;
    use leani_source_xatu::{XatuBlobsProjector, XatuCatalog, XatuCatalogConfig};

    if options.processor != "blobs-money" {
        bail!(
            "unsupported Xatu probe processor {:?}; expected \"blobs-money\"",
            options.processor
        );
    }
    let range = BlockRange::new(
        BlockNumber(options.from_block),
        BlockNumber(options.to_block),
    )?;
    let catalog = XatuCatalog::new(XatuCatalogConfig::public(&options.network)?)?;
    let objects = xatu_probe_objects(&catalog, range, options.dates)?;
    let catalog_report = catalog.inspect(objects, options.concurrency).await?;
    let catalog_valid = catalog_report.is_valid();
    let mut projection_metrics = None;
    let mut projection_output = None;
    let mut parity = None;
    let mut parity_clean = true;
    if catalog_valid && let Some((start, end)) = options.dates {
        let projection = XatuBlobsProjector::new(catalog)
            .project(
                range,
                start,
                end,
                SourceBudget {
                    max_input_bytes: options.max_input_bytes,
                    max_frame_bytes: 16 * 1_024 * 1_024,
                    max_frames: range.len(),
                    max_buffered_frames: options.concurrency,
                    max_in_flight_requests: options.concurrency,
                    temporary_disk_bytes: 1,
                    max_resident_bytes: options.max_input_bytes,
                },
                options.batch_rows,
                CancellationToken::new(),
            )
            .await?;
        let processor = leani_processor_blobs::BlobsProcessor::default();
        let deltas = projection
            .frames
            .iter()
            .map(|frame| processor.derive(frame))
            .collect::<Result<Vec<_>, _>>()?;
        let compatibility = leani_processor_blobs::BlobsCompatibilityExport::from_deltas(&deltas);
        if let Some(path) = options.expected_export.as_deref() {
            let encoded = fs::read_to_string(path)
                .with_context(|| format!("failed to read {}", path.display()))?;
            let value: serde_json::Value = serde_json::from_str(&encoded)
                .with_context(|| format!("failed to parse {}", path.display()))?;
            let expected_value = value.get("compatibility").cloned().unwrap_or(value);
            let expected: leani_processor_blobs::BlobsCompatibilityExport =
                serde_json::from_value(expected_value)
                    .with_context(|| format!("failed to parse {}", path.display()))?;
            let report = expected.compare(&compatibility);
            parity_clean = report.is_clean();
            parity = Some(report);
        }
        if let Some(path) = options.projection_output.as_deref() {
            write_json(
                path,
                &XatuProjectionOutput {
                    source: &projection,
                    deltas,
                    compatibility,
                    parity: parity.clone(),
                },
            )?;
            projection_output = Some(path.display().to_string());
        }
        projection_metrics = Some(projection.metrics);
    }
    let report = XatuProbeReport {
        catalog: catalog_report,
        projection: projection_metrics,
        projection_output,
        parity,
    };
    let encoded = serde_json::to_string_pretty(&report)?;
    if let Some(path) = options.report.as_deref() {
        write_text(path, &format!("{encoded}\n"))?;
    }
    println!("{encoded}");
    Ok(if catalog_valid && parity_clean {
        Exit::Success
    } else {
        Exit::Failure
    })
}

fn xatu_probe_objects(
    catalog: &leani_source_xatu::XatuCatalog,
    range: leani_primitives::BlockRange,
    dates: Option<(leani_source_xatu::XatuDate, leani_source_xatu::XatuDate)>,
) -> Result<Vec<leani_source_xatu::CatalogObject>> {
    use leani_source_xatu::XatuTable;

    let mut objects = catalog.execution_objects(XatuTable::CanonicalExecutionBlock, range)?;
    objects.extend(catalog.execution_objects(XatuTable::CanonicalExecutionTransaction, range)?);
    if let Some((start, end)) = dates {
        objects.extend(catalog.daily_objects(XatuTable::CanonicalBeaconBlock, start, end)?);
        objects.extend(catalog.daily_objects(
            XatuTable::CanonicalBeaconBlockExecutionTransaction,
            start,
            end,
        )?);
        objects.extend(catalog.daily_objects(
            XatuTable::CanonicalBeaconBlockWithdrawal,
            start,
            end,
        )?);
    }
    Ok(objects)
}

fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    write_text(path, &format!("{}\n", serde_json::to_string_pretty(value)?))
}

fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T> {
    let encoded =
        fs::read_to_string(path).with_context(|| format!("read JSON {}", path.display()))?;
    serde_json::from_str(&encoded).with_context(|| format!("decode JSON {}", path.display()))
}

fn read_frames(path: &Path) -> Result<Vec<leani_primitives::BlockFrame>> {
    let value: serde_json::Value = read_json(path)?;
    let frames = if value.is_array() {
        value
    } else if let Some(frames) = value.get("frames") {
        frames.clone()
    } else if let Some(frames) = value.pointer("/source/frames") {
        frames.clone()
    } else {
        bail!(
            "{} contains neither a frame array nor a `frames`/`source.frames` field",
            path.display()
        );
    };
    serde_json::from_value(frames)
        .with_context(|| format!("decode normalized frames from {}", path.display()))
}

fn write_text(path: &Path, value: &str) -> Result<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    fs::write(path, value).with_context(|| format!("failed to write {}", path.display()))?;
    Ok(())
}

fn init_logging(format: LogFormat, filter: &str) -> Result<()> {
    let filter = EnvFilter::try_new(filter).context("invalid log filter")?;
    let registry = tracing_subscriber::registry().with(filter);
    match format {
        LogFormat::Pretty => registry
            .with(
                tracing_subscriber::fmt::layer()
                    .with_ansi(std::io::stderr().is_terminal())
                    .with_writer(std::io::stderr)
                    .with_target(true),
            )
            .try_init()
            .context("logging already initialized")?,
        LogFormat::Json => registry
            .with(
                tracing_subscriber::fmt::layer()
                    .json()
                    .with_writer(std::io::stderr)
                    .with_target(true),
            )
            .try_init()
            .context("logging already initialized")?,
    }
    Ok(())
}

#[derive(Debug, Serialize)]
struct DoctorReport<'a> {
    project: &'static str,
    version: &'static str,
    config_path: String,
    config_version: u32,
    chain: &'a str,
    chain_id: u64,
    data_dir: String,
    historical_sources: Vec<&'a str>,
    live_source: String,
    finality_source: String,
    processors: Vec<&'a str>,
    listeners: Listeners,
    valid: bool,
    errors: Vec<ValidationError>,
    warnings: Vec<String>,
}

#[derive(Debug, Serialize)]
struct Listeners {
    native_api: String,
    rpc_http: String,
    rpc_ws: String,
}

/// Warn when the anchor a restart would bootstrap from expires within three
/// days: after that, startup needs a refreshed `finality.checkpoint`.
fn finality_anchor_warnings(config: &Config, now: SystemTime) -> Vec<String> {
    use leani_finality_beacon_api::{
        DEFAULT_MAX_CHECKPOINT_AGE, FINALITY_ANCHOR_FILE, parse_checkpoint_root,
        resolve_start_anchor, slot_unix_seconds,
    };

    const WARNING_WINDOW: Duration = Duration::from_hours(3 * 24);
    if matches!(
        config.finality.kind,
        crate::config::FinalitySourceKind::Disabled
    ) {
        return Vec::new();
    }
    // An invalid checkpoint is already a validation error.
    let Ok(checkpoint_root) = parse_checkpoint_root(&config.finality.checkpoint) else {
        return Vec::new();
    };
    let anchor_path = config.data_dir.join(FINALITY_ANCHOR_FILE);
    let mut warnings = Vec::new();
    if let Ok(Some(persisted)) = leani_finality_beacon_api::read_finality_anchor(&anchor_path)
        && persisted.checkpoint_root != checkpoint_root
        && persisted.anchor.beacon_block_root != checkpoint_root
    {
        warnings.push(format!(
            "the persisted finality anchor at slot {} was verified from checkpoint 0x{}, not the configured finality.checkpoint {}; startup ignores it",
            persisted.anchor.beacon_slot,
            hex::encode(persisted.checkpoint_root),
            config.finality.checkpoint
        ));
    }
    let start = resolve_start_anchor(
        Some(&anchor_path),
        leani_finality_beacon_api::TrustedCheckpoint {
            root: checkpoint_root,
            slot: (config.finality.checkpoint_slot > 0).then_some(config.finality.checkpoint_slot),
            origin: leani_finality_beacon_api::CheckpointOrigin::Operator,
        },
        DEFAULT_MAX_CHECKPOINT_AGE,
        now,
    );
    let Some(slot) = start.slot else {
        return warnings;
    };
    let anchor = if start.persisted {
        format!("persisted finality anchor at slot {slot}")
    } else {
        format!("configured checkpoint at slot {slot}")
    };
    let expires = slot_unix_seconds(slot).saturating_add(DEFAULT_MAX_CHECKPOINT_AGE.as_secs());
    let now = now.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
    if expires <= now {
        warnings.push(format!(
            "{anchor} expired {} hours ago; startup fails until finality.checkpoint is refreshed",
            (now - expires) / 3_600
        ));
    } else if expires - now <= WARNING_WINDOW.as_secs() {
        warnings.push(format!(
            "{anchor} expires in {} hours; a later restart needs a refreshed finality.checkpoint",
            (expires - now) / 3_600
        ));
    }
    warnings
}

fn doctor(path: &Path, json: bool, registry: &ProcessorRegistry) -> Result<Exit> {
    // The report carries what loading would log.
    let working_directory = std::env::current_dir().ok();
    let (config, load_warnings) =
        match Config::load_with_warnings(path, working_directory.as_deref()) {
            Ok(loaded) => loaded,
            Err(error) => {
                let error = format!("{:#}", anyhow::Error::from(error));
                if json {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&serde_json::json!({
                            "project": leani_primitives::PROJECT_NAME,
                            "version": env!("CARGO_PKG_VERSION"),
                            "config_path": path.display().to_string(),
                            "valid": false,
                            "errors": [{ "field": "config", "message": error }],
                            "warnings": [],
                        }))?
                    );
                } else {
                    eprintln!("error: {error}");
                }
                return Ok(Exit::InvalidConfiguration);
            }
        };
    let mut errors = config.validation_errors();
    errors.extend(registry.validation_errors(&config));
    let mut warnings = finality_anchor_warnings(&config, SystemTime::now());
    warnings.extend(load_warnings);
    let report = DoctorReport {
        project: leani_primitives::PROJECT_NAME,
        version: env!("CARGO_PKG_VERSION"),
        config_path: path.display().to_string(),
        config_version: config.config_version,
        chain: &config.chain.name,
        chain_id: config.chain.chain_id,
        data_dir: config.data_dir.display().to_string(),
        historical_sources: config
            .sources
            .history
            .iter()
            .map(|source| source.id.as_str())
            .collect(),
        live_source: format!("{:?}", config.sources.live.kind),
        finality_source: format!("{:?}", config.finality.kind),
        processors: config
            .processors
            .iter()
            .map(|processor| processor.instance.as_str())
            .collect(),
        listeners: Listeners {
            native_api: config.api.bind.to_string(),
            rpc_http: config.rpc.http_bind.to_string(),
            rpc_ws: config.rpc.ws_bind.to_string(),
        },
        valid: errors.is_empty(),
        errors,
        warnings,
    };

    if json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!("{} {} configuration report", report.project, report.version);
        println!("config: {}", report.config_path);
        println!("chain: {} ({})", report.chain, report.chain_id);
        println!(
            "sources: history=[{}], live={}, finality={}",
            report.historical_sources.join(", "),
            report.live_source,
            report.finality_source
        );
        println!("processors: [{}]", report.processors.join(", "));
        println!(
            "listeners: api={}, rpc-http={}, rpc-ws={}",
            report.listeners.native_api, report.listeners.rpc_http, report.listeners.rpc_ws
        );
        if report.valid {
            println!("status: valid");
        } else {
            println!("status: invalid");
            for error in &report.errors {
                println!("- {}: {}", error.field, error.message);
            }
        }
        for warning in &report.warnings {
            println!("warning: {warning}");
        }
    }

    Ok(if report.valid {
        Exit::Success
    } else {
        Exit::InvalidConfiguration
    })
}

#[allow(clippy::too_many_lines)]
async fn serve(path: &Path, registry: &ProcessorRegistry) -> Result<Exit> {
    use std::sync::Arc;

    use leani_api::{ApiConfig as NativeApiConfig, ReadinessHandle};
    use leani_processor_blobs::{BlobSchedule, BlobsProcessor};
    use leani_store_sqlite::SqliteStore;

    let config = Config::load(path)?.validate()?;
    // Refuse on the environment and configuration before opening the store:
    // opening migrates it, and a refused start must leave an older release
    // able to open it.
    let bearer_token = config
        .get()
        .api
        .bearer_token_env
        .as_deref()
        .map(|name| api_bearer_token(name, std::env::var(name)))
        .transpose()?;
    let rpc_history_enabled = matches!(
        config.get().rpc.historical_mode,
        crate::config::HistoricalMode::OnDemand
    );
    let external_history_sources = if rpc_history_enabled || config.get().raw_history.enabled {
        configured_rpc_history_sources(config.get())?
    } else {
        Vec::new()
    };
    let _data_dir_lock = crate::local_state::lock_runtime_directory(&config.get().data_dir)?;
    let assembly = registry.instantiate_all_with_extensions(config.get())?;
    let processors = assembly.processors;
    let query_extensions = assembly.query_extensions;
    let store = SqliteStore::open(
        configured_store_config(config.get(), config.get().data_dir.join("leani.sqlite"))
            .with_processors(processor_descriptors(&processors)),
    )
    .await
    .map_err(crate::uniswap_markets::explain_compact_refusal)?;
    for processor in &processors {
        // Configured consumers reference the processor's default delivery
        // stream. A new store has neither record until the processor is
        // registered, so bootstrap the immutable processor/stream identity
        // before restoring or creating its consumers. Runtime registration is
        // deliberately idempotent and will verify the same identity later.
        store
            .register_processor(processor.descriptor())
            .await
            .map_err(crate::uniswap_markets::explain_compact_refusal)?;
        for configured_consumer in &processor.descriptor().lifecycle.delivery.consumers {
            let role = if configured_consumer.required {
                leani_store_sqlite::ConsumerRole::Required
            } else {
                leani_store_sqlite::ConsumerRole::BestEffort
            };
            if let Some(existing) = store
                .consumer(processor.descriptor(), &configured_consumer.id)
                .await?
            {
                if existing.role != role {
                    bail!(
                        "durable consumer {} for processor instance {} has role {:?}, expected {:?}",
                        configured_consumer.id,
                        processor.descriptor().instance,
                        existing.role,
                        role
                    );
                }
            } else {
                store
                    .create_consumer(
                        processor.descriptor(),
                        &configured_consumer.id,
                        role,
                        leani_store_sqlite::ConsumerStartPosition::EarliestRetained,
                        std::time::Duration::from_secs(configured_consumer.lease_ttl_seconds),
                    )
                    .await
                    .with_context(|| {
                        format!(
                            "register durable consumer {} for processor instance {}",
                            configured_consumer.id,
                            processor.descriptor().instance
                        )
                    })?;
            }
        }
    }
    let live_required = matches!(
        config.get().sources.live.kind,
        crate::config::LiveSourceKind::P2p
    );
    let finality_required = !matches!(
        config.get().finality.kind,
        crate::config::FinalitySourceKind::Disabled
    );
    let readiness = ReadinessHandle::new(live_required, finality_required);
    let rpc_readiness = leani_rpc::RpcReadiness::default();
    let network_telemetry = leani_source_api::NetworkTelemetry::default();
    let (committed_events, _) = tokio::sync::broadcast::channel(1_024);
    let cancellation = CancellationToken::new();
    let (raw_history_control, retained_history_source, raw_history_store) =
        if config.get().raw_history.enabled {
            let raw = config.get().raw_history;
            let mut store_config = leani_store_history::HistoryStoreConfig::new(
                config.get().data_dir.join("raw-history"),
            )
            .with_budget(leani_store_history::StorageBudget {
                maximum_logical_bytes: raw.maximum_logical_bytes.bytes(),
                maximum_physical_bytes: raw.maximum_physical_bytes.bytes(),
                maximum_frame_logical_bytes: raw.maximum_frame_logical_bytes.bytes(),
                maximum_segment_logical_bytes: raw.maximum_segment_logical_bytes.bytes(),
                maximum_segment_physical_bytes: raw.maximum_segment_physical_bytes.bytes(),
            });
            store_config.reader_connections = raw.reader_connections;
            let raw_store = leani_store_history::HistoryStore::open(store_config).await?;
            log_raw_history_recovery(raw_store.recovery_report());
            let source_set =
                leani_store_history::RawHistorySourceSet::new(external_history_sources.clone())
                    .map_err(anyhow::Error::msg)?;
            let runner = leani_store_history::RawHistoryRunner::new(
                raw_store.clone(),
                source_set,
                raw_history_source_budget(config.get()),
            )
            .map_err(anyhow::Error::msg)?;
            let retained = config
                .get()
                .chain
                .merge_block
                .map(leani_primitives::BlockNumber)
                .map(|merge_block| {
                    leani_store_history::RetainedHistorySource::new(
                        raw_store.clone(),
                        leani_store_history::RetainedHistorySourceConfig::local(
                            leani_primitives::ChainId(config.get().chain.chain_id),
                            leani_store_history::MaterialShapeId::COMPLETE_EXECUTION,
                            leani_primitives::CapabilitySet::from_iter([
                                leani_primitives::Capability::Header,
                                leani_primitives::Capability::Transactions,
                                leani_primitives::Capability::Receipts,
                                leani_primitives::Capability::Withdrawals,
                            ]),
                            leani_store_history::VerificationClass::TrustedDataset,
                            leani_primitives::TrustModel::TrustedDataset,
                        )
                        .map_err(anyhow::Error::msg)?
                        .requiring_profile(
                            leani_store_history::RawHistoryProfile::PostMergeExecutionRpc {
                                merge_block,
                            },
                        ),
                    )
                    .map(Arc::new)
                    .map(|source| source as Arc<dyn leani_source_api::HistorySource>)
                    .map_err(anyhow::Error::msg)
                })
                .transpose()?;
            let control = Arc::new(NativeRawHistoryControl::new(
                leani_primitives::ChainId(config.get().chain.chain_id),
                config
                    .get()
                    .chain
                    .merge_block
                    .map(leani_primitives::BlockNumber),
                raw_store.clone(),
                runner,
                cancellation.clone(),
            ));
            (Some(control), retained, Some(raw_store))
        } else {
            (None, None, None)
        };
    let history = if rpc_history_enabled {
        let history_concurrency =
            u64::try_from(config.get().budgets.source_concurrency).unwrap_or(u64::MAX);
        let per_request_memory = config
            .get()
            .budgets
            .memory_bytes
            .checked_div(history_concurrency)
            .unwrap_or(0)
            .max(1);
        let per_request_temporary_disk = config
            .get()
            .budgets
            .temporary_disk_bytes
            .checked_div(history_concurrency)
            .unwrap_or(0)
            .max(1);
        let mut rpc_sources = Vec::with_capacity(
            external_history_sources.len() + usize::from(retained_history_source.is_some()),
        );
        if let Some(retained) = retained_history_source {
            rpc_sources.push(retained);
        }
        if let Some(raw) = &raw_history_store {
            rpc_sources.extend(retained_rpc_log_sources(
                leani_primitives::ChainId(config.get().chain.chain_id),
                raw,
                &processors,
            )?);
        }
        rpc_sources.extend(external_history_sources);
        Some(
            leani_rpc::HistoricalRpc::new(
                rpc_sources,
                leani_rpc::HistoricalRpcConfig {
                    max_input_bytes: per_request_memory,
                    max_frame_bytes: per_request_memory.min(32 * 1_024 * 1_024),
                    max_buffered_frames: config.get().budgets.source_concurrency,
                    max_in_flight_requests: config.get().budgets.source_concurrency,
                    temporary_disk_bytes: per_request_temporary_disk,
                    ..leani_rpc::HistoricalRpcConfig::default()
                },
            )
            .map_err(anyhow::Error::msg)?,
        )
    } else {
        None
    };
    let (pipeline_budget, material_coordinator) = historical_services(config.get())?;
    let network_lanes = live_required && finality_required;
    // Only the network lanes publish the P2P history bridge's anchors, and
    // `run_network_lanes` gives up before the first one off Ethereum mainnet.
    let p2p_bridge_expected = network_lanes && config.get().chain.chain_id == 1;
    let backfill_control = Arc::new(
        NativeBackfillControl::new(
            config.get().clone(),
            store.clone(),
            processors.clone(),
            cancellation.clone(),
            raw_history_store,
            material_coordinator,
            pipeline_budget,
        )
        .with_p2p_bridge_expected(p2p_bridge_expected),
    );
    let mut background = BackgroundTasks::default();
    background.spawn("durable backfill scheduler", {
        let control = backfill_control.clone();
        async move {
            control.supervise_durable_jobs().await;
            Ok(())
        }
    });
    if let Some(control) = &raw_history_control {
        let control = control.clone();
        background.spawn("raw-history scheduler", async move {
            control.supervise_durable_jobs().await;
            Ok(())
        });
    }
    let on_demand_processors = config
        .get()
        .processors
        .iter()
        .zip(&processors)
        .filter(|(configured, _)| {
            matches!(
                configured.history_mode,
                crate::config::ProcessorHistoryMode::OnDemand
            )
        })
        .map(|(_, processor)| processor.descriptor().instance.to_string())
        .collect();
    let application_subscription_processors = config
        .get()
        .processors
        .iter()
        .zip(&processors)
        .filter(|(configured, _)| {
            configured.history_control
                == crate::config::ProcessorHistoryControl::ApplicationSubscriptions
        })
        .map(|(_, processor)| processor.descriptor().instance.to_string())
        .collect();
    let api = leani_api::router_with_processors(
        store.clone(),
        processors.clone(),
        query_extensions,
        NativeApiConfig {
            chain_id: leani_primitives::ChainId(config.get().chain.chain_id),
            history_batch_limits: leani_api::DeliveryBatchLimits {
                target_encoded_bytes: config
                    .get()
                    .api
                    .delivery
                    .history_batches
                    .target_encoded_bytes
                    .bytes(),
                maximum_encoded_bytes: config
                    .get()
                    .api
                    .delivery
                    .history_batches
                    .maximum_encoded_bytes
                    .bytes(),
                maximum_events: config.get().api.delivery.history_batches.maximum_events,
                maximum_processed_blocks: config
                    .get()
                    .api
                    .delivery
                    .history_batches
                    .maximum_processed_blocks,
                maximum_delay: std::time::Duration::from_millis(
                    config
                        .get()
                        .api
                        .delivery
                        .history_batches
                        .maximum_delay
                        .milliseconds(),
                ),
                maximum_buffered_batches: usize::try_from(
                    config
                        .get()
                        .api
                        .delivery
                        .history_batches
                        .maximum_buffered_batches,
                )
                .unwrap_or(usize::MAX),
                maximum_buffered_bytes: config
                    .get()
                    .api
                    .delivery
                    .history_batches
                    .maximum_buffered_bytes
                    .bytes(),
                compression: match config.get().api.delivery.history_batches.compression {
                    crate::config::DeliveryCompressionConfig::None => {
                        leani_api::DeliveryCompression::None
                    }
                    crate::config::DeliveryCompressionConfig::Gzip => {
                        leani_api::DeliveryCompression::Gzip
                    }
                },
            },
            live_batch_limits: leani_api::DeliveryBatchLimits {
                target_encoded_bytes: config
                    .get()
                    .api
                    .delivery
                    .live_batches
                    .target_encoded_bytes
                    .bytes(),
                maximum_encoded_bytes: config
                    .get()
                    .api
                    .delivery
                    .live_batches
                    .maximum_encoded_bytes
                    .bytes(),
                maximum_events: config.get().api.delivery.live_batches.maximum_events,
                maximum_processed_blocks: config
                    .get()
                    .api
                    .delivery
                    .live_batches
                    .maximum_processed_blocks,
                maximum_delay: std::time::Duration::from_millis(
                    config
                        .get()
                        .api
                        .delivery
                        .live_batches
                        .maximum_delay
                        .milliseconds(),
                ),
                maximum_buffered_batches: usize::try_from(
                    config
                        .get()
                        .api
                        .delivery
                        .live_batches
                        .maximum_buffered_batches,
                )
                .unwrap_or(usize::MAX),
                maximum_buffered_bytes: config
                    .get()
                    .api
                    .delivery
                    .live_batches
                    .maximum_buffered_bytes
                    .bytes(),
                compression: match config.get().api.delivery.live_batches.compression {
                    crate::config::DeliveryCompressionConfig::None => {
                        leani_api::DeliveryCompression::None
                    }
                    crate::config::DeliveryCompressionConfig::Gzip => {
                        leani_api::DeliveryCompression::Gzip
                    }
                },
            },
            bearer_token,
            // The bind address, like every IP address, needs no entry.
            allowed_hosts: config
                .get()
                .api
                .allowed_hosts
                .iter()
                .filter_map(|host| crate::config::allowed_host(host))
                .collect(),
            allowed_origins: config
                .get()
                .api
                .allowed_origins
                .iter()
                .filter_map(|origin| crate::config::allowed_origin(origin))
                .collect(),
            readiness: readiness.clone(),
            network_telemetry: network_telemetry.clone(),
            on_demand_processors,
            application_subscription_processors,
            backfill_control: Some(backfill_control.clone()),
            raw_history_control: raw_history_control
                .clone()
                .map(|control| control as Arc<dyn leani_api::RawHistoryControl>),
            shutdown: cancellation.clone(),
            ..NativeApiConfig::default()
        },
    )?;
    let websocket_sessions = tokio_util::task::TaskTracker::new();
    let rpc_settings = &config.get().rpc;
    let rpc_config = leani_rpc::RpcConfig {
        chain_id: leani_primitives::ChainId(config.get().chain.chain_id),
        readiness: rpc_readiness.clone(),
        websocket_enabled: true,
        history,
        allowed_origins: rpc_settings
            .allowed_origins
            .iter()
            .filter_map(|origin| crate::config::allowed_origin(origin))
            .collect(),
        max_batch_requests: rpc_settings.max_batch_requests,
        max_response_bytes: usize::try_from(rpc_settings.max_response_bytes.bytes())
            .unwrap_or(usize::MAX),
        max_log_results: rpc_settings.max_log_results,
        max_log_addresses: rpc_settings.max_log_addresses,
        max_log_topic_alternatives: rpc_settings.max_log_topic_alternatives,
        outbound_budget: leani_rpc::RpcOutboundBudget::new(
            usize::try_from(rpc_settings.max_outbound_bytes.bytes()).unwrap_or(usize::MAX),
        ),
        max_subscriptions_per_connection: rpc_settings.max_subscriptions_per_connection,
        max_websocket_connections: rpc_settings.max_websocket_connections,
        max_subscription_event_bytes: usize::try_from(
            rpc_settings.max_subscription_event_bytes.bytes(),
        )
        .unwrap_or(usize::MAX),
        shutdown: cancellation.clone(),
        websocket_sessions: websocket_sessions.clone(),
        ..leani_rpc::RpcConfig::default()
    };
    let rpc_progress = processors
        .iter()
        .find(|processor| processor.descriptor().id.as_str() == "blobs-money")
        .or_else(|| processors.first())
        .context("validated configuration has no processor")?
        .clone();
    let blob_schedule = Arc::new(
        processors
            .iter()
            .find_map(|processor| {
                processor
                    .as_any()
                    .downcast_ref::<BlobsProcessor>()
                    .map(|processor| processor.schedule().clone())
            })
            .unwrap_or_else(BlobSchedule::mainnet),
    );
    let rpc = leani_rpc::http_router_with_progress(
        store.clone(),
        rpc_progress.clone(),
        blob_schedule.clone(),
        rpc_config.clone(),
        committed_events.clone(),
    );
    let rpc_websocket = leani_rpc::websocket_router_with_progress(
        store.clone(),
        rpc_progress,
        blob_schedule,
        rpc_config,
        committed_events.clone(),
    );
    let api_listener = tokio::net::TcpListener::bind(config.get().api.bind)
        .await
        .with_context(|| format!("bind native API {}", config.get().api.bind))?;
    let rpc_listener = tokio::net::TcpListener::bind(config.get().rpc.http_bind)
        .await
        .with_context(|| format!("bind JSON-RPC {}", config.get().rpc.http_bind))?;
    let rpc_websocket_listener = tokio::net::TcpListener::bind(config.get().rpc.ws_bind)
        .await
        .with_context(|| format!("bind WebSocket JSON-RPC {}", config.get().rpc.ws_bind))?;
    // Installed before the listeners serve, so no signal finds them unset.
    let signals = ShutdownSignals::new();
    let signal_token = cancellation.clone();
    let signal = tokio::spawn(async move {
        match signals {
            Ok(signals) => {
                forward_shutdown_signals(signals.into_stream(), signal_token, |code| {
                    std::process::exit(code)
                })
                .await;
            }
            Err(error) => warn!(%error, "failed to install shutdown signal handler"),
        }
    });
    if network_lanes {
        let supervisor_config = config.get().clone();
        let supervisor_store = store.clone();
        let supervisor_processors = processors.clone();
        let supervisor_cancellation = cancellation.clone();
        let supervisor_handles = NetworkLaneHandles {
            readiness: readiness.clone(),
            rpc_readiness: rpc_readiness.clone(),
            committed_events: committed_events.clone(),
            network_telemetry: network_telemetry.clone(),
            cancellation: cancellation.clone(),
            backfill_control: Some(backfill_control),
            verified_anchor: None,
            checkpoint_origin: leani_finality_beacon_api::CheckpointOrigin::Operator,
            attested_heads: leani_source_api::AttestedHeadPublisher::new(),
        };
        background.spawn("network lane supervisor", async move {
            let end = Box::pin(supervise_network_lanes(
                supervisor_config,
                supervisor_store,
                supervisor_processors,
                supervisor_handles,
                None,
            ))
            .await?;
            match end {
                // Halted lanes stay stopped and not ready until the node
                // restarts, while the node keeps serving what it stored.
                NetworkLanesEnd::Halted => supervisor_cancellation.cancelled().await,
                // Only a new process recovers a wedged live lane: exit
                // non-zero so that the process supervisor restarts the node.
                NetworkLanesEnd::Wedged => bail!("the live network lane stalled"),
                NetworkLanesEnd::Cancelled => {}
            }
            Ok(())
        });
    }
    let tiered_artifact_processors = processors
        .iter()
        .filter(|processor| {
            processor.descriptor().lifecycle.artifacts.mode
                == leani_processor_api::ArtifactPolicyMode::Full
        })
        .cloned()
        .collect::<Vec<_>>();
    if config.get().artifact_storage.backend == ArtifactStorageBackend::TieredSegments
        && !tiered_artifact_processors.is_empty()
    {
        let artifact_store = store.clone();
        let artifact_config = config.get().artifact_storage;
        let artifact_cancellation = cancellation.clone();
        background.spawn("artifact compaction", async move {
            supervise_artifact_compaction(
                artifact_store,
                tiered_artifact_processors,
                artifact_config,
                artifact_cancellation,
            )
            .await;
            Ok(())
        });
    }
    let snapshot_store = store.clone();
    let snapshot_cancellation = cancellation.clone();
    background.spawn("query snapshot cleanup", async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(30));
        loop {
            tokio::select! {
                () = snapshot_cancellation.cancelled() => break,
                _ = ticker.tick() => {
                    if let Err(error) = snapshot_store.prune_expired_query_snapshots().await {
                        warn!(%error, "query snapshot cleanup failed");
                    }
                }
            }
        }
        Ok(())
    });
    for (processor, verification_segment_blocks) in maintained_processors(config.get(), &processors)
    {
        let pruner_store = store.clone();
        let pruner_cancellation = cancellation.clone();
        background.spawn(
            format!("maintenance of {}", processor.descriptor().instance),
            async move {
                supervise_processor_maintenance(
                    pruner_store,
                    processor,
                    verification_segment_blocks,
                    pruner_cancellation,
                )
                .await;
                Ok(())
            },
        );
    }

    info!(
        api = %config.get().api.bind,
        rpc_http = %config.get().rpc.http_bind,
        rpc_ws = %config.get().rpc.ws_bind,
        "native API and JSON-RPC listening"
    );
    if live_required ^ finality_required {
        warn!(
            live_required,
            finality_required,
            "live ingestion requires both execution P2P and verified finality; readiness remains false"
        );
    }
    let api_server = axum::serve(api_listener, api)
        .with_graceful_shutdown(cancellation.clone().cancelled_owned());
    let rpc_server = axum::serve(rpc_listener, rpc)
        .with_graceful_shutdown(cancellation.clone().cancelled_owned());
    let rpc_websocket_server = axum::serve(rpc_websocket_listener, rpc_websocket)
        .with_graceful_shutdown(cancellation.clone().cancelled_owned());
    let servers = async {
        let listeners = tokio::try_join!(api_server, rpc_server, rpc_websocket_server)
            .map(|_| ())
            .context("API server failed");
        // WebSocket connections outlive their listener's graceful shutdown:
        // wait for their going-away close frames too.
        websocket_sessions.close();
        websocket_sessions.wait().await;
        listeners
    };
    let served = serve_until_shutdown(servers, &mut background, &cancellation).await;
    signal.abort();
    served?;
    Ok(Exit::Success)
}

pub(crate) async fn supervise_artifact_compaction(
    store: leani_store_sqlite::SqliteStore,
    processors: Vec<std::sync::Arc<dyn leani_processor_api::Processor>>,
    config: crate::config::ArtifactStorageConfig,
    cancellation: CancellationToken,
) {
    let mut ticker = tokio::time::interval(std::time::Duration::from_millis(
        config.compaction_interval.milliseconds(),
    ));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut next_processor = 0_usize;
    loop {
        tokio::select! {
            () = cancellation.cancelled() => return,
            _ = ticker.tick() => {}
        }
        let mut compacted_segments = 0_usize;
        let mut idle_processors = 0_usize;
        let mut compacted_artifacts = 0_u64;
        let mut compacted_bytes = 0_u64;
        while compacted_segments < config.maximum_segments_per_cycle
            && idle_processors < processors.len()
        {
            let processor = &processors[next_processor];
            next_processor = next_processor.saturating_add(1) % processors.len();
            match store
                .compact_available_processor_artifacts_to_segments(processor.descriptor(), 1, false)
                .await
            {
                Ok(outcome) if outcome.segments > 0 => {
                    compacted_segments = compacted_segments.saturating_add(1);
                    compacted_artifacts = compacted_artifacts.saturating_add(outcome.artifacts);
                    compacted_bytes = compacted_bytes.saturating_add(outcome.logical_bytes);
                    idle_processors = 0;
                }
                Ok(_) => {
                    idle_processors = idle_processors.saturating_add(1);
                }
                Err(error) => {
                    warn!(
                        processor_instance = %processor.descriptor().instance,
                        %error,
                        "processor artifact background compaction failed"
                    );
                    idle_processors = idle_processors.saturating_add(1);
                }
            }
        }
        if compacted_segments > 0 {
            info!(
                segments = compacted_segments,
                artifacts = compacted_artifacts,
                logical_bytes = compacted_bytes,
                "compacted processor artifact write buffer"
            );
            if let Err(error) = store.reclaim_free_pages(1_024).await {
                warn!(%error, "failed to reclaim compacted artifact SQLite pages");
            }
        }
    }
}

/// The processors that [`supervise_processor_maintenance`] maintains, with
/// their coverage verification segment size: those that deliver changes,
/// whose delivery log it prunes, and block-local ones, whose finalized
/// coverage it compacts.
fn maintained_processors(
    config: &Config,
    processors: &[std::sync::Arc<dyn leani_processor_api::Processor>],
) -> Vec<(std::sync::Arc<dyn leani_processor_api::Processor>, u64)> {
    processors
        .iter()
        .zip(&config.processors)
        .filter(|(processor, _)| {
            processor.descriptor().lifecycle.delivery.mode
                != leani_processor_api::DeliveryPolicyMode::None
                || processor.descriptor().mode == leani_processor_api::ReductionMode::BlockLocal
        })
        .map(|(processor, configured)| {
            (
                processor.clone(),
                configured.coverage.verification_segment_blocks,
            )
        })
        .collect()
}

#[allow(clippy::too_many_lines)]
async fn supervise_processor_maintenance(
    store: leani_store_sqlite::SqliteStore,
    processor: std::sync::Arc<dyn leani_processor_api::Processor>,
    verification_segment_blocks: u64,
    cancellation: CancellationToken,
) {
    let interval = std::time::Duration::from_secs(
        processor
            .descriptor()
            .lifecycle
            .delivery
            .pruning
            .interval_seconds,
    );
    loop {
        tokio::select! {
            () = cancellation.cancelled() => return,
            () = tokio::time::sleep(interval) => {}
            () = store.wait_for_delivery_capacity_change() => {}
        }
        let finalized = match store.finalized_through(processor.descriptor()).await {
            Ok(Some(finalized)) => finalized,
            Ok(None) => continue,
            Err(error) => {
                warn!(
                    processor_instance = %processor.descriptor().instance,
                    %error,
                    "failed to inspect finalized delivery boundary"
                );
                continue;
            }
        };
        let streams = match store.delivery_streams(processor.descriptor()).await {
            Ok(streams) => streams,
            Err(error) => {
                warn!(
                    processor_instance = %processor.descriptor().instance,
                    %error,
                    "failed to list processor delivery streams"
                );
                continue;
            }
        };
        for stream in streams {
            match store
                .prune_delivery_changes_in_stream(
                    processor.descriptor(),
                    &stream.stream_id,
                    finalized,
                )
                .await
            {
                Ok(outcome) if outcome.deleted > 0 => {
                    info!(
                        processor_instance = %processor.descriptor().instance,
                        stream_id = %stream.stream_id,
                        stream_kind = ?stream.kind,
                        deleted = outcome.deleted,
                        effective_before = outcome.effective_before,
                        "pruned acknowledged delivery batch"
                    );
                    if let Err(error) = store.reclaim_free_pages(256).await {
                        warn!(
                            processor_instance = %processor.descriptor().instance,
                            %error,
                            "incremental delivery-space reclamation failed"
                        );
                    }
                }
                Ok(_) => {}
                Err(error) => warn!(
                    processor_instance = %processor.descriptor().instance,
                    stream_id = %stream.stream_id,
                    %error,
                    "delivery pruning pass failed"
                ),
            }
        }
        if processor.descriptor().mode == leani_processor_api::ReductionMode::BlockLocal {
            loop {
                match store
                    .compactable_finalized_coverage_blocks(processor.descriptor(), finalized)
                    .await
                {
                    Ok(compactable) if compactable < verification_segment_blocks => break,
                    Ok(_) => {}
                    Err(error) => {
                        warn!(
                            processor_instance = %processor.descriptor().instance,
                            %error,
                            "failed to inspect compactable finalized coverage"
                        );
                        break;
                    }
                }
                match store
                    .compact_finalized_coverage(
                        processor.descriptor(),
                        finalized,
                        verification_segment_blocks,
                        10_000,
                    )
                    .await
                {
                    Ok(outcome) if outcome.exact_coverage_deleted > 0 => {
                        info!(
                            processor_instance = %processor.descriptor().instance,
                            compacted_range = ?outcome.compacted_range,
                            segments = outcome.segments_created,
                            exact_coverage_deleted = outcome.exact_coverage_deleted,
                            applied_blocks_deleted = outcome.applied_blocks_deleted,
                            "compacted finalized block-local coverage metadata"
                        );
                        if let Err(error) = store.reclaim_free_pages(256).await {
                            warn!(
                                processor_instance = %processor.descriptor().instance,
                                %error,
                                "incremental coverage-space reclamation failed"
                            );
                        }
                        if cancellation.is_cancelled() {
                            return;
                        }
                        tokio::task::yield_now().await;
                    }
                    Ok(_) => break,
                    Err(error) => {
                        warn!(
                            processor_instance = %processor.descriptor().instance,
                            %error,
                            "finalized coverage compaction pass failed"
                        );
                        break;
                    }
                }
            }
        }
    }
}

/// Shortest native API bearer token `serve` accepts.
const MINIMUM_API_BEARER_TOKEN_CHARS: usize = 16;

/// The native API bearer token read from environment variable `name`. An
/// unset, non-Unicode, empty, or short value fails startup, as does one with
/// spaces, control characters, or non-ASCII characters. Leading or trailing
/// whitespace, other control characters, and non-ASCII characters could never
/// authenticate through an HTTP header; interior spaces and tabs could, but are
/// refused as well, so a token is one printable ASCII word. No error repeats
/// the value.
fn api_bearer_token(name: &str, value: Result<String, std::env::VarError>) -> Result<Arc<str>> {
    let token = match value {
        Ok(token) => token,
        Err(std::env::VarError::NotPresent) => {
            bail!("API bearer token environment variable {name} is not set")
        }
        Err(std::env::VarError::NotUnicode(_)) => {
            bail!("API bearer token environment variable {name} is not valid Unicode")
        }
    };
    if token.chars().count() < MINIMUM_API_BEARER_TOKEN_CHARS {
        bail!(
            "API bearer token in environment variable {name} must be at least {MINIMUM_API_BEARER_TOKEN_CHARS} characters"
        );
    }
    if !token.bytes().all(|byte| byte.is_ascii_graphic()) {
        bail!(
            "API bearer token in environment variable {name} must be printable ASCII without spaces"
        );
    }
    Ok(Arc::from(token))
}

async fn shutdown_signal() -> std::io::Result<()> {
    ShutdownSignals::new()?.recv().await
}

#[derive(Clone)]
struct NetworkLaneHandles {
    readiness: leani_api::ReadinessHandle,
    rpc_readiness: leani_rpc::RpcReadiness,
    committed_events: tokio::sync::broadcast::Sender<leani_source_api::ChainEvent>,
    network_telemetry: leani_source_api::NetworkTelemetry,
    cancellation: CancellationToken,
    backfill_control: Option<Arc<NativeBackfillControl>>,
    verified_anchor:
        Option<tokio::sync::watch::Sender<Option<leani_runtime::AppliedFinalityAnchor>>>,
    /// Where `finality.checkpoint` came from: the operator, or an anchor an
    /// embedded subscription verified before.
    checkpoint_origin: leani_finality_beacon_api::CheckpointOrigin,
    /// Publishes the attested heads the finality lane verifies. The
    /// execution live source only follows them, through a receiver.
    attested_heads: leani_source_api::AttestedHeadPublisher,
}

fn publish_verified_anchor(
    sender: &tokio::sync::watch::Sender<Option<leani_runtime::AppliedFinalityAnchor>>,
    anchor: leani_runtime::AppliedFinalityAnchor,
) -> Result<bool> {
    let mut conflict = false;
    let changed = sender.send_if_modified(|current| {
        if let Some(previous) = current {
            if anchor.beacon_slot < previous.beacon_slot {
                return false;
            }
            if anchor.beacon_slot == previous.beacon_slot {
                conflict = anchor.beacon_block_root != previous.beacon_block_root
                    || anchor.block.hash != previous.block.hash
                    || anchor.block.number != previous.block.number;
                return false;
            }
            if anchor.block.number < previous.block.number
                || (anchor.block.number == previous.block.number
                    && anchor.block.hash != previous.block.hash)
            {
                conflict = true;
                return false;
            }
        }
        *current = Some(anchor);
        true
    });
    if conflict {
        bail!(
            "verified finality anchor at Beacon slot {} contradicts the current anchor",
            anchor.beacon_slot
        );
    }
    Ok(changed)
}

/// Minimal network runtime used by commands that need live processing without
/// opening the node's API/RPC listeners or starting historical job machinery.
pub(crate) struct EmbeddedNetworkRuntime {
    pub(crate) readiness: leani_api::ReadinessHandle,
    pub(crate) verified_anchor:
        tokio::sync::watch::Receiver<Option<leani_runtime::AppliedFinalityAnchor>>,
    pub(crate) execution_source: std::sync::Arc<leani_source_p2p::RethP2pSource>,
    cancellation: CancellationToken,
    task: tokio::task::JoinHandle<()>,
    _data_dir_lock: crate::local_state::RuntimeDirectoryLock,
}

impl EmbeddedNetworkRuntime {
    #[must_use]
    pub(crate) fn is_finished(&self) -> bool {
        self.task.is_finished()
    }

    #[must_use]
    pub(crate) fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    pub(crate) async fn shutdown(mut self) {
        self.cancellation.cancel();
        let _ =
            tokio::time::timeout(Duration::from_secs(5), self.execution_source.shutdown()).await;
        if tokio::time::timeout(Duration::from_secs(1), &mut self.task)
            .await
            .is_err()
        {
            // Some networking internals finish their own peer-store flush and socket
            // teardown on a fixed timer. A CLI subscription must nevertheless
            // honor Ctrl-C promptly, so the supervisor is aborted after
            // cancellation. The abort can land between a live commit's or a
            // reorg's separately committed steps; startup reconciliation at the
            // next start repairs that partial commit.
            self.task.abort();
            let _ = (&mut self.task).await;
        }
    }
}

impl Drop for EmbeddedNetworkRuntime {
    fn drop(&mut self) {
        self.cancellation.cancel();
        self.task.abort();
    }
}

pub(crate) fn spawn_embedded_network_runtime(
    config: Config,
    store: leani_store_sqlite::SqliteStore,
    processors: Vec<std::sync::Arc<dyn leani_processor_api::Processor>>,
    data_dir_lock: crate::local_state::RuntimeDirectoryLock,
    checkpoint_origin: leani_finality_beacon_api::CheckpointOrigin,
) -> Result<EmbeddedNetworkRuntime> {
    let readiness = leani_api::ReadinessHandle::new(true, true);
    let cancellation = CancellationToken::new();
    let (committed_events, _) = tokio::sync::broadcast::channel(1_024);
    let (verified_anchor, verified_anchor_updates) = tokio::sync::watch::channel(None);
    let network_telemetry = leani_source_api::NetworkTelemetry::default();
    let attested_heads = leani_source_api::AttestedHeadPublisher::new();
    let execution_source = execution_p2p_source(
        &config,
        network_telemetry.clone(),
        Some(attested_heads.subscribe()),
    )?;
    let handles = NetworkLaneHandles {
        readiness: readiness.clone(),
        rpc_readiness: leani_rpc::RpcReadiness::default(),
        committed_events,
        network_telemetry,
        cancellation: cancellation.clone(),
        backfill_control: None,
        verified_anchor: Some(verified_anchor),
        checkpoint_origin,
        attested_heads,
    };
    // The node's delivery pruning and coverage compaction run beside the
    // lanes, as in `serve`: without them the delivery window fills and pauses
    // the subscription's processor. They stop with the lanes.
    let maintenance =
        futures::future::join_all(maintained_processors(&config, &processors).into_iter().map(
            |(processor, verification_segment_blocks)| {
                supervise_processor_maintenance(
                    store.clone(),
                    processor,
                    verification_segment_blocks,
                    cancellation.clone(),
                )
            },
        ));
    let lanes = Box::pin(supervise_network_lanes(
        config,
        store,
        processors,
        handles,
        Some(execution_source.clone()),
    ));
    // The task ends with the lanes, even when nothing needs maintenance.
    let task = tokio::spawn(async move {
        tokio::select! {
            _ = lanes => {}
            () = async {
                maintenance.await;
                std::future::pending::<()>().await;
            } => {}
        }
    });
    Ok(EmbeddedNetworkRuntime {
        readiness,
        verified_anchor: verified_anchor_updates,
        execution_source,
        cancellation,
        task,
        _data_dir_lock: data_dir_lock,
    })
}

/// The persistent execution P2P source. Live following needs
/// `attested_heads`, the receiver of the heads the finality lane verifies;
/// history-only uses, such as the P2P benchmark, pass none.
pub(crate) fn execution_p2p_source(
    config: &Config,
    network_telemetry: leani_source_api::NetworkTelemetry,
    attested_heads: Option<leani_source_api::AttestedHeadReceiver>,
) -> Result<std::sync::Arc<leani_source_p2p::RethP2pSource>> {
    let nat = leani_source_p2p::parse_nat_resolver(&config.sources.live.nat)?;
    let trusted_peers = config
        .sources
        .live
        .trusted_peers
        .iter()
        .map(|peer| leani_source_p2p::parse_trusted_peer(peer))
        .collect::<Result<Vec<_>, _>>()?;
    let p2p_config = leani_source_p2p::RethP2pConfig {
        minimum_peers: config.sources.live.minimum_peers,
        body_serving_peer_target: config.sources.live.body_serving_peer_target,
        preferred_peers: config.sources.live.preferred_peers,
        max_outbound_peers: config.sources.live.max_outbound_peers,
        max_concurrent_dials: config.sources.live.max_concurrent_dials,
        listener_port: config.sources.live.listener_port,
        discovery_port: config.sources.live.discovery_port,
        discv5_port: config.sources.live.discv5_port,
        enable_discv5: config.sources.live.enable_discv5,
        nat,
        trusted_peers,
        bootstrap_dns_tree: config.sources.live.bootstrap_dns_tree.clone(),
        peer_refill_interval: std::time::Duration::from_millis(
            config.sources.live.peer_refill_interval_ms,
        ),
        peer_recovery_timeout: std::time::Duration::from_secs(
            config.sources.live.peer_recovery_timeout_seconds,
        ),
        retry_backoff_max: std::time::Duration::from_secs(
            config.sources.live.retry_backoff_max_seconds,
        ),
        persistent_retries: config.sources.live.persistent_retries,
        material_request_concurrency: config.sources.live.material_request_concurrency,
        material_request_blocks: config.sources.live.material_request_blocks,
        request_timeout: std::time::Duration::from_secs(
            config.sources.live.request_timeout_seconds,
        ),
        retries: config.sources.live.request_retries,
        retry_backoff: std::time::Duration::from_millis(
            config.sources.live.request_retry_backoff_ms,
        ),
        history_header_request_concurrency: config.sources.live.history_header_request_concurrency,
        history_header_request_blocks: config.sources.live.history_header_request_blocks,
        peer_store_path: Some(config.data_dir.join("execution-network.sqlite")),
        secret_key_path: Some(config.data_dir.join("execution-p2p-secret")),
        peer_store_max_entries: config.sources.live.peer_store_max_entries,
        peer_store_flush_interval: std::time::Duration::from_secs(
            config.sources.live.peer_store_flush_seconds,
        ),
        network_telemetry,
        ..leani_source_p2p::RethP2pConfig::default()
    };
    let source = leani_source_p2p::RethP2pSource::mainnet(p2p_config)?;
    Ok(std::sync::Arc::new(match attested_heads {
        Some(heads) => source.with_attested_heads(heads),
        None => source,
    }))
}

/// Resolve a recent finalized execution anchor through the configured,
/// independently verified consensus source.
pub(crate) async fn verified_p2p_history_anchor(
    config: &Config,
) -> Result<leani_source_p2p::P2pHistoryAnchor> {
    p2p_history_anchor(
        config,
        leani_finality_beacon_api::AnchorFile::ReadOnly(
            config
                .data_dir
                .join(leani_finality_beacon_api::FINALITY_ANCHOR_FILE),
        ),
    )
    .await
}

async fn p2p_history_anchor(
    config: &Config,
    anchor_file: leani_finality_beacon_api::AnchorFile,
) -> Result<leani_source_p2p::P2pHistoryAnchor> {
    use leani_finality_beacon_api::{BeaconApiConfig, VerifiedBeaconApi, parse_checkpoint_root};
    use leani_finality_consensus_p2p::VerifiedConsensusP2p;
    use leani_primitives::{BlockHash, BlockNumber, BlockRef, ConsensusAnchor, Finality};

    if config.chain.chain_id != 1 {
        bail!("execution P2P historical fallback currently supports Ethereum mainnet only");
    }
    let checkpoint = leani_finality_beacon_api::TrustedCheckpoint {
        root: parse_checkpoint_root(&config.finality.checkpoint)?,
        slot: (config.finality.checkpoint_slot > 0).then_some(config.finality.checkpoint_slot),
        origin: leani_finality_beacon_api::CheckpointOrigin::Operator,
    };
    let selected = match config.finality.kind {
        crate::config::FinalitySourceKind::BeaconApi => {
            let mut finality = BeaconApiConfig::mainnet(config.finality.endpoints.clone());
            finality.minimum_agreement = config.finality.minimum_agreement;
            finality.anchor = anchor_file;
            let source = VerifiedBeaconApi::mainnet(finality)?;
            let report = source.probe_root(checkpoint).await;
            if !report.accepted {
                bail!(
                    "verified Beacon API finality quorum was not accepted: {}",
                    report.disagreements.join("; ")
                );
            }
            report
                .selected
                .context("accepted finality report omitted its selected anchor")?
        }
        crate::config::FinalitySourceKind::ConsensusP2p => {
            let mut p2p_config = consensus_p2p_config(&config.finality);
            p2p_config.anchor = anchor_file;
            let source = VerifiedConsensusP2p::mainnet(p2p_config)?;
            let report = source.probe_checkpoint(checkpoint).await;
            if !report.accepted {
                bail!(
                    "verified consensus P2P finality was not accepted: {}",
                    report.errors.join("; ")
                );
            }
            report
                .selected
                .context("accepted consensus P2P report omitted its selected anchor")?
        }
        crate::config::FinalitySourceKind::Disabled => {
            bail!("execution P2P history requires a verified finality source");
        }
    };
    Ok(leani_source_p2p::P2pHistoryAnchor {
        block: BlockRef {
            number: BlockNumber(selected.execution_block_number),
            hash: selected.execution_block_hash,
            parent_hash: BlockHash::ZERO,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        },
        consensus: ConsensusAnchor {
            finality: Finality::Finalized,
            execution_block_hash: selected.execution_block_hash,
            beacon_slot: selected.beacon_slot,
            beacon_block_root: selected.beacon_block_root,
        },
    })
}

/// Supervise the network lanes until the node shuts down or they halt. A
/// persistent execution P2P source that cannot be built is an error.
async fn supervise_network_lanes(
    config: Config,
    store: leani_store_sqlite::SqliteStore,
    processors: Vec<std::sync::Arc<dyn leani_processor_api::Processor>>,
    handles: NetworkLaneHandles,
    live_source: Option<std::sync::Arc<leani_source_p2p::RethP2pSource>>,
) -> Result<NetworkLanesEnd> {
    let live_source = match live_source {
        Some(source) => source,
        None => match execution_p2p_source(
            &config,
            handles.network_telemetry.clone(),
            Some(handles.attested_heads.subscribe()),
        ) {
            Ok(source) => source,
            Err(error) => {
                handles.network_telemetry.supervisor_halted(&error);
                warn!(%error, "failed to construct persistent execution P2P source");
                return Err(error.context("construct the persistent execution P2P source"));
            }
        },
    };
    let end = supervise_lane_runs(&handles, || {
        Box::pin(run_network_lanes_once(
            &config,
            store.clone(),
            processors.clone(),
            handles.clone(),
            live_source.clone(),
        ))
    })
    .await;
    handles.network_telemetry.supervisor_stopped();
    // The shutdown waits for the network state, which a hung manager build
    // can hold.
    if tokio::time::timeout(Duration::from_secs(10), live_source.shutdown())
        .await
        .is_err()
    {
        warn!("the execution P2P network did not shut down within 10 seconds");
    }
    Ok(end)
}

/// Occurrences of one contradiction between verified finality and retained
/// unfinalized blocks after which the network lanes halt instead of
/// restarting again: the restarts before it did not repair it.
const FINALITY_REORG_HALT_OCCURRENCE: usize = 3;

/// Delay before the network lanes restart after a failure. It doubles with
/// each further failure, up to a minute.
const INITIAL_LANE_BACKOFF: Duration = Duration::from_secs(1);

/// A network-lane run that stayed up this long, the longest backoff, was
/// healthy: its failure restarts the lanes after [`INITIAL_LANE_BACKOFF`].
const HEALTHY_LANE_RUN: Duration = Duration::from_mins(1);

/// How the network lane supervisor stopped.
#[derive(Debug, Eq, PartialEq)]
enum NetworkLanesEnd {
    /// The node shuts down.
    Cancelled,
    /// The lanes halted: they stay stopped and not ready until the node
    /// restarts.
    Halted,
    /// The live lane stalled; only a new process recovers it.
    Wedged,
}

/// The live lane stayed not ready while verified finality was ready.
#[derive(Debug, thiserror::Error)]
#[error(
    "the live lane stayed not ready for {}s while verified finality was ready",
    timeout.as_secs()
)]
struct LiveStalled {
    timeout: Duration,
}

/// Resolve once the live lane has been not ready, while verified finality
/// was ready, for `timeout` without a break. Without finality, the network
/// itself is likely down, and a restart would not help. A full recent-frame
/// store also keeps the lane not ready, but the runtime's own limit restarts
/// the lanes for it, so that wait does not count.
async fn live_stall(
    mut live: tokio::sync::watch::Receiver<bool>,
    mut finality: tokio::sync::watch::Receiver<bool>,
    mut storage_full: tokio::sync::watch::Receiver<bool>,
    timeout: Duration,
) -> LiveStalled {
    let mut deadline = None;
    loop {
        let stalled = *finality.borrow_and_update()
            && !*live.borrow_and_update()
            && !*storage_full.borrow_and_update();
        deadline = match (stalled, deadline) {
            (false, _) => None,
            (true, None) => Some(tokio::time::Instant::now() + timeout),
            (true, deadline) => deadline,
        };
        tokio::select! {
            () = tokio::time::sleep_until(deadline.unwrap_or_else(tokio::time::Instant::now)),
                if deadline.is_some() => return LiveStalled { timeout },
            Ok(()) = live.changed() => {}
            Ok(()) = finality.changed() => {}
            Ok(()) = storage_full.changed() => {}
            else => std::future::pending().await,
        }
    }
}

/// Drops the network lanes' readiness when dropped.
struct LaneReadinessGuard<'a>(&'a NetworkLaneHandles);

impl Drop for LaneReadinessGuard<'_> {
    fn drop(&mut self) {
        self.0.readiness.set_live_ready(false);
        self.0.rpc_readiness.set_live_ready(false);
        self.0.readiness.set_finality_ready(false);
    }
}

/// Run the network lanes with `run_once` until cancelled. A failed run
/// restarts after a bounded exponential backoff, unless it halts or wedges
/// (see [`network_lane_failure`]): the lanes then stay stopped and not
/// ready, with the error, until the node restarts.
///
/// However the supervisor stops, even by a panic unwinding out of a run or
/// with its task aborted, readiness drops.
async fn supervise_lane_runs<F, Fut>(
    handles: &NetworkLaneHandles,
    mut run_once: F,
) -> NetworkLanesEnd
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let _not_ready = LaneReadinessGuard(handles);
    let mut retry = INITIAL_LANE_BACKOFF;
    let mut reorgs = FinalityReorgRestarts::default();
    while !handles.cancellation.is_cancelled() {
        handles.readiness.set_live_ready(false);
        handles.rpc_readiness.set_live_ready(false);
        handles.readiness.set_finality_ready(false);
        handles.network_telemetry.supervisor_running();
        let started = tokio::time::Instant::now();
        let result = run_once().await;
        if handles.cancellation.is_cancelled() {
            break;
        }
        let failure = match network_lane_failure(&result, &mut reorgs) {
            LaneFailure::Wedge(failure) => {
                tracing::error!(
                    error = %failure,
                    "the live lane stalled while verified finality stayed ready; the node exits so that it restarts"
                );
                handles.network_telemetry.supervisor_halted(&failure);
                return NetworkLanesEnd::Wedged;
            }
            LaneFailure::Halt(failure) => {
                tracing::error!(
                    error = %failure,
                    "verified finality contradicts the retained canonical chain; network lanes halt until the node restarts"
                );
                handles.network_telemetry.supervisor_halted(&failure);
                return NetworkLanesEnd::Halted;
            }
            LaneFailure::Heal(failure) => {
                warn!(
                    error = %failure,
                    "verified finality contradicts retained unfinalized blocks; restarting the network lanes to revert them"
                );
                failure
            }
            LaneFailure::Restart(failure) => {
                warn!(
                    error = %failure,
                    "required network lane failed closed"
                );
                failure
            }
        };
        // Only the backoff starts over after a healthy run. The count of a
        // recurring finality contradiction follows finality, not time: it
        // resets only when a contradiction names another finalized block.
        if started.elapsed() >= HEALTHY_LANE_RUN {
            retry = INITIAL_LANE_BACKOFF;
        }
        handles.network_telemetry.supervisor_backoff(failure, retry);
        tokio::select! {
            () = handles.cancellation.cancelled() => break,
            () = tokio::time::sleep(retry) => {}
        }
        retry = retry
            .saturating_mul(2)
            .min(std::time::Duration::from_mins(1));
    }
    NetworkLanesEnd::Cancelled
}

/// How the supervisor handles a failed network-lane run.
#[derive(Debug, Eq, PartialEq)]
enum LaneFailure {
    /// Restart after the backoff.
    Restart(String),
    /// Restart after the backoff: startup reverts the retained unfinalized
    /// blocks that verified finality contradicts.
    Heal(String),
    /// Stop, not ready, until the node restarts.
    Halt(String),
    /// Stop, and exit the node so that its process supervisor restarts it.
    Wedge(String),
}

/// The finalized block whose contradiction with retained unfinalized blocks
/// last restarted the network lanes, and how often it has.
#[derive(Debug, Default)]
struct FinalityReorgRestarts {
    finalized: Option<(leani_primitives::BlockNumber, leani_primitives::BlockHash)>,
    occurrences: usize,
}

/// Decide how the supervisor handles `result`, a failed network-lane run.
///
/// A contradiction with finalized history halts. A contradiction with
/// retained unfinalized blocks heals: the restart reverts them. The same one
/// again, before finality moves to another block, heals again, until its
/// `FINALITY_REORG_HALT_OCCURRENCE`th occurrence halts, so a persistent fault
/// cannot restart the lanes forever. A stalled live lane wedges: a lane
/// restart cannot heal the process-wide execution network it stalled in.
/// Anything else restarts.
fn network_lane_failure(result: &Result<()>, reorgs: &mut FinalityReorgRestarts) -> LaneFailure {
    let error = match result {
        Ok(()) => {
            return LaneFailure::Restart("required network lane ended unexpectedly".to_owned());
        }
        Err(error) => error,
    };
    let failure = format!("{error:#}");
    if error
        .chain()
        .any(<dyn std::error::Error>::is::<LiveStalled>)
    {
        return LaneFailure::Wedge(failure);
    }
    if network_lane_halts(error) {
        return LaneFailure::Halt(failure);
    }
    let Some(finalized) = finality_reorg(error) else {
        return LaneFailure::Restart(failure);
    };
    reorgs.occurrences = if reorgs.finalized == Some(finalized) {
        reorgs.occurrences.saturating_add(1)
    } else {
        1
    };
    reorgs.finalized = Some(finalized);
    if reorgs.occurrences >= FINALITY_REORG_HALT_OCCURRENCE {
        return LaneFailure::Halt(format!(
            "{failure}; it recurred after {} restarts of the network lanes",
            reorgs.occurrences - 1
        ));
    }
    LaneFailure::Heal(failure)
}

/// Whether a failed network-lane run must halt rather than restart: verified
/// finality contradicted finalized canonical history, which no restart
/// repairs.
fn network_lane_halts(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        matches!(
            cause.downcast_ref::<leani_runtime::RuntimeError>(),
            Some(leani_runtime::RuntimeError::FinalityContradiction { .. })
        )
    })
}

/// The finalized block that retained unfinalized blocks contradicted, when
/// that failed the network-lane run.
fn finality_reorg(
    error: &anyhow::Error,
) -> Option<(leani_primitives::BlockNumber, leani_primitives::BlockHash)> {
    error.chain().find_map(
        |cause| match cause.downcast_ref::<leani_runtime::RuntimeError>() {
            Some(leani_runtime::RuntimeError::FinalityReorg {
                block, finalized, ..
            }) => Some((*block, *finalized)),
            _ => None,
        },
    )
}

/// The automatic cold backfills of one network-lane run, named for the log.
#[derive(Default)]
struct ColdBackfills {
    tasks: tokio::task::JoinSet<Result<leani_store_sqlite::HotColdHandoffRecord>>,
    names: std::collections::HashMap<tokio::task::Id, String>,
}

impl ColdBackfills {
    fn spawn(
        &mut self,
        name: impl Into<String>,
        backfill: impl Future<Output = Result<leani_store_sqlite::HotColdHandoffRecord>>
        + Send
        + 'static,
    ) {
        let id = self.tasks.spawn(backfill).id();
        self.names.insert(id, name.into());
    }

    fn len(&self) -> usize {
        self.tasks.len()
    }

    /// Wait for the next backfill to end, as [`tokio::task::JoinSet::join_next`]
    /// does.
    async fn join_next(
        &mut self,
    ) -> Option<Result<Result<leani_store_sqlite::HotColdHandoffRecord>, tokio::task::JoinError>>
    {
        let ended = self.tasks.join_next_with_id().await?;
        self.names.remove(&match &ended {
            Ok((id, _)) => *id,
            Err(error) => error.id(),
        });
        Some(ended.map(|(_, backfill)| backfill))
    }
}

/// Run the network lanes once. However the run ends, even by an early error,
/// its cold backfills are cancelled and joined before it returns, so none of
/// them outlives it into the next run's startup reconciliation.
async fn run_network_lanes_once(
    config: &Config,
    store: leani_store_sqlite::SqliteStore,
    processors: Vec<std::sync::Arc<dyn leani_processor_api::Processor>>,
    handles: NetworkLaneHandles,
    live_source: std::sync::Arc<leani_source_p2p::RethP2pSource>,
) -> Result<()> {
    let cancellation = handles.cancellation.clone();
    with_cold_backfills(&cancellation, async move |lane_cancellation, backfills| {
        Box::pin(run_network_lanes(
            config,
            store,
            processors,
            handles,
            live_source,
            lane_cancellation,
            backfills,
        ))
        .await
    })
    .await
}

/// Run `lanes` with a set for the cold backfills it spawns, cancelled by the
/// lanes' own token. Once `lanes` ends, however it ends, the backfills are
/// cancelled and joined before this returns; a panic of `lanes` resumes
/// after the join. If this future is dropped instead, the drop guard cancels
/// them and dropping the set aborts them.
async fn with_cold_backfills(
    cancellation: &CancellationToken,
    lanes: impl AsyncFnOnce(&CancellationToken, &mut ColdBackfills) -> Result<()>,
) -> Result<()> {
    use futures::FutureExt as _;

    let lane_cancellation = cancellation.child_token();
    let _cancel_backfills = lane_cancellation.clone().drop_guard();
    let mut backfills = ColdBackfills::default();
    let result = std::panic::AssertUnwindSafe(lanes(&lane_cancellation, &mut backfills))
        .catch_unwind()
        .await;
    lane_cancellation.cancel();
    let mut still_running = tokio::time::interval_at(
        tokio::time::Instant::now() + COLD_BACKFILL_STOP_WARNING,
        COLD_BACKFILL_STOP_WARNING,
    );
    loop {
        let backfill = tokio::select! {
            backfill = backfills.join_next() => backfill,
            _ = still_running.tick() => {
                warn!(
                    backfills = ?backfills.names.values().collect::<Vec<_>>(),
                    "cancelled automatic cold backfills are still running; the network lanes wait for them"
                );
                continue;
            }
        };
        let Some(backfill) = backfill else {
            break;
        };
        if let Err(error) = backfill
            && error.is_panic()
        {
            warn!(%error, "automatic cold backfill task panicked");
        }
    }
    result.unwrap_or_else(|panic| std::panic::resume_unwind(panic))
}

/// How often the network lanes name the cancelled cold backfills they still
/// wait for.
const COLD_BACKFILL_STOP_WARNING: Duration = Duration::from_secs(10);

#[allow(clippy::too_many_lines)]
async fn run_network_lanes(
    config: &Config,
    store: leani_store_sqlite::SqliteStore,
    processors: Vec<std::sync::Arc<dyn leani_processor_api::Processor>>,
    handles: NetworkLaneHandles,
    live_source: std::sync::Arc<leani_source_p2p::RethP2pSource>,
    lane_cancellation: &CancellationToken,
    backfills: &mut ColdBackfills,
) -> Result<()> {
    use std::{
        sync::Arc,
        time::{Instant, SystemTime, UNIX_EPOCH},
    };

    use leani_finality_beacon_api::{
        BeaconApiConfig, VerifiedBeaconApi, VerifiedFinalityAnchor, parse_checkpoint_root,
    };
    use leani_finality_consensus_p2p::VerifiedConsensusP2p;
    use leani_primitives::{BlockHash, BlockNumber, BlockRef, ChainId, ConsensusAnchor, Finality};
    use leani_runtime::{
        SharedFinalityRuntime, SharedFinalityRuntimeConfig, SharedLiveRuntime,
        SharedLiveRuntimeConfig,
    };
    use leani_source_api::{ConsensusCheckpoint, FinalitySource};
    use leani_source_p2p::P2pHistoryAnchor;

    let NetworkLaneHandles {
        readiness,
        rpc_readiness,
        committed_events,
        network_telemetry: _,
        cancellation,
        backfill_control,
        verified_anchor,
        checkpoint_origin,
        attested_heads,
    } = handles;
    if config.chain.chain_id != 1 {
        bail!("the direct P2P/finality lane currently supports Ethereum mainnet only");
    }
    if !matches!(config.sources.live.kind, crate::config::LiveSourceKind::P2p) {
        bail!("network supervisor requires p2p live ingestion");
    }
    let startup_started = Instant::now();
    let chain_id = ChainId(config.chain.chain_id);
    let retained_warmup_head = retained_canonical_tip(&store, chain_id).await?;
    if let Some(head) = retained_warmup_head {
        spawn_execution_peer_warmup(
            live_source.clone(),
            head,
            cancellation.clone(),
            startup_started,
        );
    }
    let checkpoint_root = parse_checkpoint_root(&config.finality.checkpoint)?;
    let trusted_checkpoint = leani_finality_beacon_api::TrustedCheckpoint {
        root: checkpoint_root,
        slot: (config.finality.checkpoint_slot > 0).then_some(config.finality.checkpoint_slot),
        origin: checkpoint_origin,
    };
    // Verified finality persists its newest anchor here, so a restart does
    // not depend on the configured checkpoint's age.
    let anchor_file = leani_finality_beacon_api::AnchorFile::ReadWrite {
        path: config
            .data_dir
            .join(leani_finality_beacon_api::FINALITY_ANCHOR_FILE),
        write_failures: readiness.finality_anchor_write_failures(),
    };
    let (finality_source, selected, bootstrap): (
        Arc<dyn FinalitySource>,
        VerifiedFinalityAnchor,
        VerifiedFinalityAnchor,
    ) = match config.finality.kind {
        crate::config::FinalitySourceKind::BeaconApi => {
            let mut beacon_config = BeaconApiConfig::mainnet(config.finality.endpoints.clone());
            beacon_config.minimum_agreement = config.finality.minimum_agreement;
            beacon_config.anchor = anchor_file;
            // Verified optimistic updates bound what the live lane includes.
            let source = Arc::new(
                VerifiedBeaconApi::mainnet(beacon_config)?.with_attested_heads(attested_heads),
            );
            let probe = source.probe_root(trusted_checkpoint).await;
            if !probe.accepted {
                bail!(
                    "verified Beacon API finality quorum was not accepted: {}",
                    probe.disagreements.join("; ")
                );
            }
            let selected = probe
                .selected
                .context("accepted finality report omitted its selected anchor")?;
            let bootstrap = probe
                .checkpoint_anchor
                .context("accepted finality report omitted its checkpoint anchor")?;
            (source, selected, bootstrap)
        }
        crate::config::FinalitySourceKind::ConsensusP2p => {
            let mut p2p_config = consensus_p2p_config(&config.finality);
            p2p_config.anchor = anchor_file;
            let source = Arc::new(
                VerifiedConsensusP2p::mainnet(p2p_config)?.with_attested_heads(attested_heads),
            );
            let probe = source.probe_checkpoint(trusted_checkpoint).await;
            if !probe.accepted {
                bail!(
                    "verified consensus P2P finality was not accepted: {}",
                    probe.errors.join("; ")
                );
            }
            (
                source,
                probe
                    .selected
                    .context("accepted P2P report omitted its selected anchor")?,
                probe
                    .checkpoint_anchor
                    .context("accepted P2P report omitted its checkpoint anchor")?,
            )
        }
        crate::config::FinalitySourceKind::Disabled => {
            bail!("network supervisor requires a verified finality source");
        }
    };
    info!(
        elapsed_ms = u64::try_from(startup_started.elapsed().as_millis()).unwrap_or(u64::MAX),
        finalized_execution_block = selected.execution_block_number,
        bootstrap_slot = bootstrap.beacon_slot,
        "verified finality startup anchor resolved"
    );
    observe_finality_anchor(&readiness, selected.beacon_slot);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let live_anchor = BlockRef {
        number: BlockNumber(selected.execution_block_number),
        hash: selected.execution_block_hash,
        parent_hash: BlockHash::ZERO,
        timestamp: now,
    };
    // A cold store has no trustworthy execution head to advertise until the
    // independently verified finality probe completes. A warm store already
    // advertised its newer retained canonical tip, so do not downgrade it to
    // the older finalized anchor.
    if retained_warmup_head.is_none() {
        live_source.update_advertised_head(live_anchor).await;
        spawn_execution_peer_warmup(
            live_source.clone(),
            live_anchor,
            cancellation.clone(),
            startup_started,
        );
    }
    if let Some(verified_anchor) = &verified_anchor {
        publish_verified_anchor(
            verified_anchor,
            leani_runtime::AppliedFinalityAnchor {
                block: live_anchor,
                beacon_slot: selected.beacon_slot,
                beacon_block_root: selected.beacon_block_root,
            },
        )?;
    }
    let checkpoint = ConsensusCheckpoint {
        beacon_slot: bootstrap.beacon_slot,
        beacon_block_root: bootstrap.beacon_block_root,
        execution_block_hash: bootstrap.execution_block_hash,
        obtained_at_unix_seconds: now,
        source: if bootstrap.beacon_block_root == checkpoint_root {
            "configured weak-subjectivity checkpoint"
        } else {
            "persisted verified finality anchor"
        }
        .to_owned(),
    };

    let mut live_runtime = SharedLiveRuntime::new(
        store.clone(),
        live_source.clone(),
        processors.clone(),
        SharedLiveRuntimeConfig {
            max_reorg_depth: 64,
            pending_delta_bytes: config.budgets.pending_delta_bytes,
            sink_ids: Vec::new(),
            committed_events: Some(committed_events),
            recent_hard_bytes: config.budgets.recent_raw_hard_bytes,
            // Automatic history refills what the live lane skips; on-demand
            // history leaves it to whoever requests history, so tell them.
            live_gap_notices: config
                .processors
                .iter()
                .zip(&processors)
                .filter(|(configured, _)| {
                    configured.history_mode == crate::config::ProcessorHistoryMode::OnDemand
                })
                .map(|(_, processor)| processor.descriptor().instance.to_string())
                .collect(),
            ..SharedLiveRuntimeConfig::default()
        },
    )?;
    if let Some(control) = &backfill_control {
        live_runtime = live_runtime.with_finalized_gap_recovery(control.clone());
    }
    // Retained unfinalized blocks that do not link to the verified finalized
    // anchor, such as a branch reorged away during downtime, are reverted
    // before it is seeded; reconciliation below undoes their coverage.
    live_runtime
        .seed_finalized_anchor(live_anchor)
        .await
        .context("seed the verified finalized anchor")?;
    // A crash, abort, or lane restart can interrupt a live commit or reorg
    // between its separately committed steps; repair every processor against
    // the canonical chain before any lane or handoff check runs.
    let startup_reconciliation = live_runtime.reconcile_startup().await.context(
        "reconcile processor state with the canonical chain before opening network lanes",
    )?;
    info!(?startup_reconciliation, "startup reconciliation completed");
    let (applied_anchors, mut applied_anchor_updates) = tokio::sync::broadcast::channel(16);
    let finality_runtime = SharedFinalityRuntime::new(
        store.clone(),
        finality_source,
        processors.clone(),
        SharedFinalityRuntimeConfig {
            minimum_recent_blocks: config.rpc.minimum_recent_blocks,
            recent_soft_bytes: config.budgets.recent_raw_soft_bytes,
            recent_hard_bytes: config.budgets.recent_raw_hard_bytes,
        },
    )?
    .with_applied_anchors(applied_anchors);
    let overlap_blocks = config.sources.live.handoff_overlap_blocks;
    let overlap_from = selected
        .execution_block_number
        .saturating_sub(overlap_blocks.saturating_sub(1));
    let history_anchor = P2pHistoryAnchor {
        block: live_anchor,
        consensus: ConsensusAnchor {
            finality: Finality::Finalized,
            execution_block_hash: selected.execution_block_hash,
            beacon_slot: selected.beacon_slot,
            beacon_block_root: selected.beacon_block_root,
        },
    };
    if let Some(control) = &backfill_control {
        control
            .update_p2p_bridge(live_source.as_ref().clone(), history_anchor.clone())
            .await;
    }
    let p2p_bridge_updates = {
        let control = backfill_control.clone();
        let source = live_source.as_ref().clone();
        let verified_anchor = verified_anchor.clone();
        let readiness = readiness.clone();
        async move {
            loop {
                let applied = match applied_anchor_updates.recv().await {
                    Ok(applied) => applied,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!(
                            skipped,
                            "on-demand P2P bridge skipped superseded finality anchors"
                        );
                        continue;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        bail!("applied finality anchor channel closed");
                    }
                };
                if let Some(verified_anchor) = &verified_anchor {
                    publish_verified_anchor(verified_anchor, applied)?;
                }
                observe_finality_anchor(&readiness, applied.beacon_slot);
                if let Some(control) = &control {
                    control
                        .update_p2p_bridge(
                            source.clone(),
                            P2pHistoryAnchor {
                                block: applied.block,
                                consensus: ConsensusAnchor {
                                    finality: Finality::Finalized,
                                    execution_block_hash: applied.block.hash,
                                    beacon_slot: applied.beacon_slot,
                                    beacon_block_root: applied.beacon_block_root,
                                },
                            },
                        )
                        .await;
                }
            }
        }
    };
    let require_anchor_overlap = config.processors.iter().any(|configured| {
        !matches!(
            configured.history_mode,
            crate::config::ProcessorHistoryMode::OnDemand
        ) && configured.start_block <= selected.execution_block_number
    });
    spawn_cold_backfills(
        config,
        &store,
        &processors,
        selected.execution_block_number,
        overlap_from,
        selected.execution_block_hash,
        live_source.as_ref().clone(),
        history_anchor,
        backfill_control
            .as_ref()
            .and_then(|control| control.material_coordinator.clone()),
        if let Some(control) = &backfill_control {
            control.pipeline_budget.clone()
        } else {
            let history_pipeline = config.budgets.history_pipeline;
            leani_runtime::HistoricalPipelineBudget::new(
                history_pipeline.maximum_active_chunks,
                historical_map_task_capacity(config),
                history_pipeline.maximum_mapped_bytes.bytes(),
            )
            .map_err(anyhow::Error::msg)?
        },
        lane_cancellation,
        backfills,
    )
    .await?;
    let live_budget = live_source_budget(config);
    let (live_ready, mut live_ready_updates) = tokio::sync::watch::channel(false);
    let handoff_runtime = live_runtime.clone();
    let live_start = retained_live_start(
        &store,
        chain_id,
        live_anchor,
        overlap_blocks,
        64,
        require_anchor_overlap,
    )
    .await?;
    let live = live_runtime.run_with_readiness(
        live_start,
        live_budget,
        lane_cancellation.clone(),
        live_ready,
    );
    let (finality_ready, mut finality_ready_updates) = tokio::sync::watch::channel(false);
    let finality = finality_runtime.run_resilient_with_readiness(
        checkpoint,
        lane_cancellation.clone(),
        finality_ready,
    );
    let archive_reconciliations =
        run_archive_reconciliations(config, &store, &processors, lane_cancellation.clone());
    let handoffs = finish_cold_handoffs(&store, &handoff_runtime, &processors, backfills);
    let stall = live_stall(
        live_ready_updates.clone(),
        finality_ready_updates.clone(),
        handoff_runtime.recent_storage_full(),
        Duration::from_secs(config.sources.live.stall_timeout_seconds),
    );
    tokio::pin!(live);
    tokio::pin!(finality);
    tokio::pin!(p2p_bridge_updates);
    tokio::pin!(archive_reconciliations);
    tokio::pin!(handoffs);
    tokio::pin!(stall);
    let mut handoffs_verified = false;
    let mut handoffs_finished = false;
    let mut live_finished = false;
    let result = loop {
        tokio::select! {
            () = cancellation.cancelled() => break Ok(()),
            changed = live_ready_updates.changed() => {
                if changed.is_ok() {
                    let ready = *live_ready_updates.borrow();
                    readiness.set_live_ready(ready);
                    rpc_readiness.set_live_ready(ready);
                    info!(
                        ready,
                        elapsed_ms = u64::try_from(startup_started.elapsed().as_millis())
                            .unwrap_or(u64::MAX),
                        "execution live readiness changed"
                    );
                } else {
                    readiness.set_live_ready(false);
                    rpc_readiness.set_live_ready(false);
                    break Err(anyhow::anyhow!("live readiness channel closed"));
                }
            }
            changed = finality_ready_updates.changed() => {
                if changed.is_ok() {
                    let ready = *finality_ready_updates.borrow();
                    readiness.set_finality_ready(ready);
                    info!(
                        ready,
                        elapsed_ms = u64::try_from(startup_started.elapsed().as_millis())
                            .unwrap_or(u64::MAX),
                        "verified finality readiness changed"
                    );
                } else {
                    readiness.set_finality_ready(false);
                    break Err(anyhow::anyhow!("finality readiness channel closed"));
                }
            }
            result = &mut live => {
                live_finished = true;
                readiness.set_live_ready(false);
                rpc_readiness.set_live_ready(false);
                break result
                    .map(|report| {
                        info!(?report, "live network lane ended");
                    })
                    .map_err(Into::into);
            }
            result = &mut finality => {
                readiness.set_finality_ready(false);
                break result
                    .map(|report| {
                        info!(?report, "finality network lane ended");
                    })
                    .map_err(Into::into);
            }
            result = &mut p2p_bridge_updates => {
                break result.context("on-demand P2P bridge update lane ended");
            }
            result = &mut archive_reconciliations => {
                break result.context("archive/live reconciliation lane ended");
            }
            stalled = &mut stall => break Err(stalled.into()),
            result = &mut handoffs, if !handoffs_verified => {
                handoffs_finished = true;
                match result {
                    Ok(summary) if summary.failed.is_empty() => {
                        handoffs_verified = true;
                        info!(
                            handoffs = summary.verified.len(),
                            overlap_from,
                            overlap_to = selected.execution_block_number,
                            "all hot/cold handoffs verified"
                        );
                    }
                    Ok(summary) => {
                        handoffs_verified = true;
                        warn!(
                            verified = summary.verified.len(),
                            failed = ?summary.failed,
                            overlap_from,
                            overlap_to = selected.execution_block_number,
                            "hot/cold handoffs finished with failures; the others follow live"
                        );
                    }
                    Err(error) => break Err(error.context("hot/cold handoff failed closed")),
                }
            }
        }
    };
    lane_cancellation.cancel();
    stop_lanes(
        &result,
        async {
            if !live_finished {
                let _ = live.as_mut().await;
            }
        },
        async {
            if !handoffs_finished {
                let _ = handoffs.as_mut().await;
            }
        },
    )
    .await;
    readiness.set_live_ready(false);
    rpc_readiness.set_live_ready(false);
    readiness.set_finality_ready(false);
    result
}

/// Wait for the live lane and the handoffs of a run that `result` ended. A
/// suspended live commit holds the lane lock that handoff reconciliation and
/// parking take, so drive both to completion rather than handoffs alone.
/// After a stall, wait only as long as a shutdown: the stalled lane can wait
/// on a network build that ignores cancellation.
async fn stop_lanes(
    result: &Result<()>,
    live: impl Future<Output = ()>,
    handoffs: impl Future<Output = ()>,
) {
    let stop = async {
        tokio::join!(live, handoffs);
    };
    if !result.as_ref().is_err_and(|error| {
        error
            .chain()
            .any(<dyn std::error::Error>::is::<LiveStalled>)
    }) {
        return stop.await;
    }
    if tokio::time::timeout(shutdown::SHUTDOWN_DEADLINE, stop)
        .await
        .is_err()
    {
        warn!(
            deadline = ?shutdown::SHUTDOWN_DEADLINE,
            "the stalled live lane did not stop; the node exits without it"
        );
    }
}

/// Publish the newest verified finality anchor's slot time and the time a
/// restart can no longer bootstrap from it, for the anchor metrics.
fn observe_finality_anchor(readiness: &leani_api::ReadinessHandle, beacon_slot: u64) {
    let anchor = leani_finality_beacon_api::slot_unix_seconds(beacon_slot);
    readiness.set_finality_anchor(
        anchor,
        anchor.saturating_add(leani_finality_beacon_api::DEFAULT_MAX_CHECKPOINT_AGE.as_secs()),
    );
}

fn spawn_execution_peer_warmup(
    source: std::sync::Arc<leani_source_p2p::RethP2pSource>,
    advertised: leani_primitives::BlockRef,
    cancellation: CancellationToken,
    startup_started: std::time::Instant,
) {
    let warmup = tokio::spawn(async move {
        match source.warm_up(advertised, &cancellation).await {
            Ok(connected) => info!(
                elapsed_ms =
                    u64::try_from(startup_started.elapsed().as_millis()).unwrap_or(u64::MAX),
                advertised_block = advertised.number.0,
                connected,
                "execution peer warmup completed alongside node startup"
            ),
            Err(error) if cancellation.is_cancelled() => debug!(
                advertised_block = advertised.number.0,
                %error,
                "execution peer warmup cancelled"
            ),
            Err(error) => debug!(
                advertised_block = advertised.number.0,
                %error,
                "execution peer warmup did not complete; live startup continues retrying"
            ),
        }
    });
    std::mem::drop(warmup);
}

async fn retained_live_start(
    store: &leani_store_sqlite::SqliteStore,
    chain_id: leani_primitives::ChainId,
    anchor: leani_primitives::BlockRef,
    overlap_blocks: u64,
    max_reorg_depth: usize,
    require_anchor_overlap: bool,
) -> Result<leani_source_api::LiveStart> {
    use leani_primitives::BlockNumber;
    use leani_source_api::LiveStart;

    let fallback = || {
        if require_anchor_overlap {
            LiveStart::AnchoredOverlap {
                anchor,
                overlap_blocks,
            }
        } else {
            LiveStart::Block(anchor)
        }
    };
    let Some(bounds) = store.recent_canonical_bounds(chain_id).await? else {
        return Ok(fallback());
    };
    if bounds.start() > anchor.number || bounds.end() < anchor.number {
        return Ok(fallback());
    }
    let validation_blocks = bounds.end().0.saturating_sub(anchor.number.0);
    if validation_blocks > 4_096 {
        return Ok(fallback());
    }
    let mut verified = Vec::with_capacity(
        usize::try_from(validation_blocks.saturating_add(1)).unwrap_or_default(),
    );
    let mut previous = None;
    for number in anchor.number.0..=bounds.end().0 {
        let Some(frame) = store.recent_frame(chain_id, BlockNumber(number)).await? else {
            return Ok(fallback());
        };
        if number == anchor.number.0 && frame.block.hash != anchor.hash {
            anyhow::bail!(
                "retained canonical block {} conflicts with finalized execution anchor",
                anchor.number.0
            );
        }
        if let Some(parent) = previous
            && frame.block.parent_hash != parent
        {
            anyhow::bail!(
                "retained canonical chain is not parent-linked at block {}",
                frame.block.number.0
            );
        }
        previous = Some(frame.block.hash);
        verified.push(frame.block);
    }
    let retain = max_reorg_depth.saturating_add(1);
    let suffix_from = verified.len().saturating_sub(retain);
    let canonical = verified.split_off(suffix_from);
    let tip = canonical
        .last()
        .copied()
        .expect("anchor-inclusive retained suffix is non-empty");
    info!(
        finalized_anchor = anchor.number.0,
        validated_from = anchor.number.0,
        retained_from = canonical
            .first()
            .map_or(tip.number.0, |block| block.number.0),
        retained_tip = tip.number.0,
        "resuming execution P2P live ingestion from durable canonical material"
    );
    Ok(LiveStart::RetainedCanonical { canonical })
}

async fn retained_canonical_tip(
    store: &leani_store_sqlite::SqliteStore,
    chain_id: leani_primitives::ChainId,
) -> Result<Option<leani_primitives::BlockRef>> {
    let Some(bounds) = store.recent_canonical_bounds(chain_id).await? else {
        return Ok(None);
    };
    Ok(store
        .recent_frame(chain_id, bounds.end())
        .await?
        .map(|frame| frame.block))
}

#[allow(clippy::too_many_lines)]
async fn run_archive_reconciliations(
    config: &Config,
    store: &leani_store_sqlite::SqliteStore,
    processors: &[std::sync::Arc<dyn leani_processor_api::Processor>],
    cancellation: CancellationToken,
) -> Result<()> {
    use leani_primitives::{BlockNumber, BlockRange, ChainId};
    use leani_runtime::{RuntimeError, reconcile_archive_deltas};
    use leani_source_api::SourceError;
    use leani_store_sqlite::{ArchiveReconciliationState, HotColdHandoffState, StoreError};

    let chain_id = ChainId(config.chain.chain_id);
    let batch_blocks = config.sources.live.archive_reconciliation_blocks;
    let retry_interval =
        std::time::Duration::from_secs(config.sources.live.archive_reconciliation_interval_seconds);
    loop {
        if cancellation.is_cancelled() {
            return Ok(());
        }
        let mut made_progress = false;
        for processor in processors {
            let configured_processor =
                config_for_processor_descriptor(config, processor.descriptor())?;
            let Some(handoff) = store
                .latest_hot_cold_handoff(processor.descriptor())
                .await?
                .filter(|record| record.state == HotColdHandoffState::Verified)
            else {
                continue;
            };
            let last_verified = store
                .latest_verified_archive_reconciliation(processor.descriptor())
                .await?;
            // Revisit the entire P2P-eligible history range, not only blocks
            // observed after the initial handoff. Archive-derived blocks in
            // this range are harmlessly rechecked; any P2P-filled dataset gap
            // is therefore eventually independently reproduced.
            let audit_start = config
                .sources
                .live
                .history_fallback_start(handoff.overlap.end().0, configured_processor.start_block);
            let next = last_verified
                .as_ref()
                .map_or(audit_start, |record| {
                    record.overlap.end().0.saturating_add(1)
                })
                .max(audit_start);
            let Some(finalized) = store.finalized_through(processor.descriptor()).await? else {
                continue;
            };
            if next > finalized.0 {
                continue;
            }
            let end = finalized
                .0
                .min(next.saturating_add(batch_blocks.saturating_sub(1)));
            let range = BlockRange::new(BlockNumber(next), BlockNumber(end))?;
            let (sources, verification_policy) =
                configured_history_sources(config, processor.as_ref(), None).with_context(
                    || {
                        format!(
                            "construct archive reconciliation sources for {}",
                            processor.descriptor().id
                        )
                    },
                )?;
            let budget = historical_source_budget(config, range);
            let mut reconciled = None;
            for source in sources {
                let result = reconcile_archive_deltas(
                    store,
                    source.as_ref(),
                    processor.as_ref(),
                    chain_id,
                    range,
                    verification_policy,
                    budget,
                    cancellation.clone(),
                )
                .await;
                match result {
                    Ok(record) => {
                        if record.state != ArchiveReconciliationState::Verified {
                            bail!(
                                "archive reconciliation {} ended without a verified verdict",
                                record.id
                            );
                        }
                        reconciled = Some(record);
                        break;
                    }
                    Err(RuntimeError::Cancelled) if cancellation.is_cancelled() => return Ok(()),
                    // Another source may serve it; passes repeat, so debug only.
                    Err(RuntimeError::SourceCannotServe { source_id, detail }) => {
                        debug!(
                            processor = %processor.descriptor().id,
                            %source_id,
                            %detail,
                            "archive reconciliation source cannot serve this processor"
                        );
                    }
                    // Another source, or a larger budget, may serve the range.
                    Err(RuntimeError::Source(error @ SourceError::BudgetExceeded { .. })) => {
                        warn!(
                            processor = %processor.descriptor().id,
                            source = %source.descriptor().id,
                            from = range.start().0,
                            to = range.end().0,
                            %error,
                            "archive reconciliation read exceeded its source budget"
                        );
                    }
                    Err(
                        RuntimeError::Source(
                            SourceError::MissingRange(_)
                            | SourceError::MissingMaterial { .. }
                            | SourceError::Unavailable(_)
                            | SourceError::Disconnected(_)
                            | SourceError::Protocol(_),
                        )
                        | RuntimeError::IncompleteChunk { .. },
                    ) => {
                        warn!(
                            processor = %processor.descriptor().id,
                            source = %source.descriptor().id,
                            from = range.start().0,
                            to = range.end().0,
                            "archive has not produced a complete reconciliation range yet"
                        );
                    }
                    Err(
                        error @ (RuntimeError::Store(StoreError::ArchiveReconciliationMismatch {
                            ..
                        })
                        | RuntimeError::InvalidFrame(_)
                        | RuntimeError::Processor(_)),
                    ) => {
                        return Err(error.into());
                    }
                    Err(error) => {
                        return Err(error).with_context(|| {
                            format!(
                                "archive reconciliation failed for processor {} via {}",
                                processor.descriptor().id,
                                source.descriptor().id
                            )
                        });
                    }
                }
            }
            if let Some(record) = reconciled {
                made_progress = true;
                info!(
                    processor = %record.processor_id,
                    source = %record.source_id,
                    from = record.overlap.start().0,
                    to = record.overlap.end().0,
                    compared_blocks = record.compared_blocks,
                    "archive caught up and revalidated live processor deltas"
                );
            }
        }
        if made_progress {
            continue;
        }
        tokio::select! {
            () = cancellation.cancelled() => return Ok(()),
            () = tokio::time::sleep(retry_interval) => {}
        }
    }
}

/// One processor's automatic cold backfill, or the verification of its
/// hot/cold overlap, failed. The failure belongs to that processor alone.
#[derive(Debug)]
struct ColdHandoffFailure {
    processor: leani_processor_api::ProcessorDescriptor,
    handoff_id: String,
    detail: String,
}

impl std::fmt::Display for ColdHandoffFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "hot/cold handoff {} failed for processor {}: {}",
            self.handoff_id, self.processor.instance, self.detail
        )
    }
}

impl std::error::Error for ColdHandoffFailure {}

/// How the automatic hot/cold handoffs of one network-lane start ended.
#[derive(Debug)]
struct ColdHandoffSummary {
    verified: Vec<leani_store_sqlite::HotColdHandoffRecord>,
    /// Instances whose backfill or verification failed; their running lanes
    /// are parked where a retained frame allowed it.
    failed: Vec<String>,
}

/// Wait for every automatic cold backfill and its handoff verification, then
/// drain the ordered live deltas they released.
///
/// A processor whose backfill or verification failed has its handoff marked
/// failed and its live lane parked, unless the lane is already paused,
/// failed, or at a gap, or no frame is retained yet; the other processors'
/// handoffs and live lanes continue. Any other error fails the network lanes
/// as before.
async fn finish_cold_handoffs(
    store: &leani_store_sqlite::SqliteStore,
    live: &leani_runtime::SharedLiveRuntime,
    processors: &[std::sync::Arc<dyn leani_processor_api::Processor>],
    backfills: &mut ColdBackfills,
) -> Result<ColdHandoffSummary> {
    let mut records = Vec::with_capacity(backfills.len());
    let mut failed = std::collections::BTreeSet::new();
    while let Some(backfill) = backfills.join_next().await {
        let failure = match backfill.context("automatic cold backfill task panicked")? {
            Ok(record) => {
                records.push(record);
                continue;
            }
            Err(error) => error.downcast::<ColdHandoffFailure>()?,
        };
        store
            .fail_hot_cold_handoff(&failure.handoff_id, &failure.processor, &failure.detail)
            .await
            .with_context(|| format!("record the failed handoff {}", failure.handoff_id))?;
        let parked = live
            .park_processor_lane(&failure.processor, "hot_cold_handoff_failed")
            .await
            .with_context(|| format!("park the live lane of {}", failure.processor.instance))?;
        if parked {
            warn!(
                processor = %failure.processor.instance,
                handoff = %failure.handoff_id,
                detail = %failure.detail,
                "hot/cold handoff failed for one processor; its live lane is parked while the others continue"
            );
        } else {
            warn!(
                processor = %failure.processor.instance,
                handoff = %failure.handoff_id,
                detail = %failure.detail,
                "hot/cold handoff failed for one processor, whose live lane was not parked: \
                 already paused, failed, or at a gap, or no retained frame yet; its cold range \
                 stays uncovered until the next start's backfill"
            );
        }
        failed.insert(failure.processor.instance.to_string());
    }
    live.reconcile_pending()
        .await
        .context("drain ordered live deltas after hot/cold handoff")?;
    // Checked per instance: the drain's report counts processors by kind. One
    // row answers it, where the statistics would scan them all again.
    for processor in processors {
        let instance = processor.descriptor().instance.to_string();
        if failed.contains(&instance) {
            continue;
        }
        let pending = store
            .pending_deltas(processor.descriptor(), leani_primitives::BlockNumber(0), 1)
            .await
            .with_context(|| format!("look for pending deltas of {instance}"))?;
        if let Some(delta) = pending.first() {
            bail!(
                "processor {instance} retains pending deltas from block {} after verified handoff",
                delta.block.number.0
            );
        }
    }
    Ok(ColdHandoffSummary {
        verified: records,
        failed: failed.into_iter().collect(),
    })
}

/// Cancel the automatic jobs under `prefix` that earlier starts left
/// unfinished. Each start plans its own job through its finalized anchor,
/// whose range contains theirs; no task of an earlier start still runs them.
/// Only a range may follow the prefix: instance IDs can hold `:`, so a longer
/// ID belongs to another instance.
async fn supersede_automatic_jobs(
    store: &leani_store_sqlite::SqliteStore,
    prefix: &str,
    current: &str,
) -> Result<()> {
    use leani_store_sqlite::JobState;

    for mut job in store.jobs(None).await? {
        if job
            .id
            .strip_prefix(prefix)
            .is_some_and(|range| !range.contains(':'))
            && job.id != current
            && matches!(
                job.state,
                JobState::Queued | JobState::Running | JobState::StorageBackpressured
            )
        {
            job.state = JobState::Cancelled;
            store.save_job(&job).await?;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn spawn_cold_backfills(
    config: &Config,
    store: &leani_store_sqlite::SqliteStore,
    processors: &[std::sync::Arc<dyn leani_processor_api::Processor>],
    through: u64,
    overlap_from: u64,
    anchor_hash: leani_primitives::BlockHash,
    p2p_source: leani_source_p2p::RethP2pSource,
    history_anchor: leani_source_p2p::P2pHistoryAnchor,
    material_coordinator: Option<leani_runtime::HistoricalMaterialCoordinator>,
    pipeline_budget: leani_runtime::HistoricalPipelineBudget,
    cancellation: &CancellationToken,
    backfills: &mut ColdBackfills,
) -> Result<()> {
    use leani_primitives::{BlockNumber, BlockRange, ChainId};
    use leani_runtime::{BackfillJob, HistoricalRuntime};

    let mut startup_permits = material_coordinator
        .as_ref()
        .map(|coordinator| coordinator.startup_batch(processors.len()).into_iter());
    for (configured, processor) in config.processors.iter().zip(processors) {
        let startup_permit = startup_permits.as_mut().and_then(std::iter::Iterator::next);
        if matches!(
            configured.history_mode,
            crate::config::ProcessorHistoryMode::OnDemand
        ) {
            info!(
                processor = %configured.instance,
                "automatic processor backfill disabled; historical ranges are on demand"
            );
            continue;
        }
        if configured.start_block > through {
            continue;
        }
        let range = BlockRange::new(BlockNumber(configured.start_block), BlockNumber(through))?;
        let bridge = OnDemandP2pBridge {
            source: p2p_source.clone(),
            anchor: history_anchor.clone(),
        };
        let (source, verification_policy) =
            history_sources_with_bridge(config, processor.as_ref(), None, Some(&bridge), range)
                .with_context(|| {
                    format!("construct history sources for {}", configured.instance)
                })?;
        let overlap = BlockRange::new(
            BlockNumber(overlap_from.max(configured.start_block)),
            BlockNumber(through),
        )?;
        // Handoff and job IDs name the processor instance, so instances of
        // one kind never share them. Kinds hold no `:`, so no ID here equals
        // one an earlier release derived from the kind.
        let handoff_id = format!(
            "handoff:{}:{}:{}-{}",
            config.chain.chain_id,
            configured.instance,
            overlap.start().0,
            through
        );
        store
            .begin_hot_cold_handoff(
                &handoff_id,
                processor.descriptor(),
                ChainId(config.chain.chain_id),
                overlap,
                anchor_hash,
            )
            .await?;
        let runtime_config = historical_runtime_config(config, source.len());
        let runtime = match HistoricalRuntime::new_with_sources(
            store.clone(),
            source,
            processor.clone(),
            runtime_config,
        ) {
            Ok(runtime) => {
                let runtime = runtime.with_pipeline_budget(pipeline_budget.clone());
                if let Some(coordinator) = &material_coordinator {
                    runtime.with_material_coordinator(coordinator.clone())
                } else {
                    runtime
                }
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("construct automatic backfill for {}", configured.instance)
                });
            }
        };
        let runtime = if let Some(startup_permit) = startup_permit {
            runtime.with_material_startup_permit(startup_permit)
        } else {
            runtime
        };
        let job_prefix = format!(
            "automatic:{}:{}:",
            config.chain.chain_id, configured.instance
        );
        let job = match BackfillJob::for_processor(
            format!("{job_prefix}{}-{through}", configured.start_block),
            processor.as_ref(),
            ChainId(config.chain.chain_id),
            range,
            verification_policy,
        ) {
            Ok(job) => job,
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("plan automatic backfill for {}", configured.instance)
                });
            }
        };
        supersede_automatic_jobs(store, &job_prefix, &job.id).await?;
        let budget = historical_source_budget(config, range);
        let processor_id = configured.instance.clone();
        let processor_descriptor = processor.descriptor().clone();
        let handoff_store = store.clone();
        let chain_id = ChainId(config.chain.chain_id);
        let task_cancellation = cancellation.clone();
        let job_id = job.id.clone();
        backfills.spawn(job_id.clone(), async move {
            let report = match runtime.run(job, budget, task_cancellation.clone()).await {
                Ok(report) => report,
                Err(leani_runtime::RuntimeError::Cancelled) if task_cancellation.is_cancelled() => {
                    info!(
                        %job_id,
                        processor = %processor_id,
                        "automatic historical work suspended for node shutdown"
                    );
                    return Err(anyhow::anyhow!(
                        "automatic cold backfill suspended for node shutdown"
                    ));
                }
                Err(error) => {
                    let state = if matches!(error, leani_runtime::RuntimeError::Cancelled) {
                        leani_store_sqlite::JobState::Cancelled
                    } else {
                        leani_store_sqlite::JobState::Failed
                    };
                    let error_message = error.to_string();
                    NativeBackfillControl::record_failure(
                        &handoff_store,
                        "automatic",
                        &job_id,
                        leani_runtime::HistoricalJobOwner::Materialization,
                        state,
                        Some(error_message.clone()),
                    )
                    .await;
                    return Err(ColdHandoffFailure {
                        processor: processor_descriptor,
                        handoff_id,
                        detail: format!("automatic cold backfill failed: {error_message}"),
                    }
                    .into());
                }
            };
            NativeBackfillControl::record_success(
                &handoff_store,
                "automatic",
                leani_runtime::HistoricalJobOwner::Materialization,
                report,
            )
            .await;
            loop {
                if handoff_store
                    .canonical_coverage(chain_id, overlap)
                    .await
                    .with_context(|| format!("read live overlap for processor {processor_id}"))?
                    == vec![overlap]
                {
                    break;
                }
                tokio::select! {
                    () = task_cancellation.cancelled() => {
                        bail!("hot/cold overlap wait cancelled for processor {processor_id}");
                    }
                    () = tokio::time::sleep(std::time::Duration::from_millis(100)) => {}
                }
            }
            let record = match handoff_store
                .verify_hot_cold_handoff(
                    &handoff_id,
                    &processor_descriptor,
                    chain_id,
                    overlap,
                    anchor_hash,
                )
                .await
            {
                Ok(record) => record,
                Err(leani_store_sqlite::StoreError::HandoffMismatch { detail, .. }) => {
                    return Err(ColdHandoffFailure {
                        processor: processor_descriptor,
                        handoff_id,
                        detail: format!("hot/cold overlap verification failed: {detail}"),
                    }
                    .into());
                }
                Err(error) => {
                    return Err(anyhow::Error::new(error).context(format!(
                        "verify hot/cold overlap for processor {processor_id}"
                    )));
                }
            };
            info!(
                processor = %processor_id,
                overlap_from = overlap.start().0,
                overlap_to = overlap.end().0,
                compared_blocks = record.compared_blocks,
                "hot/cold handoff verified"
            );
            Ok(record)
        });
    }
    Ok(())
}

/// Run the process foundation until an external cancellation request arrives.
///
/// Individual services are added to the lifecycle supervisor in later
/// milestones; keeping this boundary explicit makes startup and shutdown
/// behavior testable from the first checkpoint.
pub async fn serve_until_cancelled(
    config: &crate::config::ValidatedConfig,
    cancellation: &CancellationToken,
) {
    info!(
        chain = %config.get().chain.name,
        chain_id = config.get().chain.chain_id,
        processors = config.get().processors.len(),
        "node process started"
    );
    cancellation.cancelled().await;
    info!("node process stopped");
}

#[cfg(test)]
mod tests {
    use clap::Parser;
    use leani_processor_api::{ArtifactPolicyMode, Processor};
    use leani_testkit::{BlockLocalCounter, fixture_frame};

    use super::{shutdown::SHUTDOWN_DEADLINE, *};

    #[test]
    fn history_reads_acquire_by_disk_and_hold_by_memory() {
        let mut config: Config =
            toml::from_str(crate::config::VALID_CONFIG_TOML).expect("configuration fixture");
        config.budgets.memory_bytes = 64 * 1_024 * 1_024;
        config.budgets.temporary_disk_bytes = 2 * 1_024 * 1_024 * 1_024;
        config.budgets.history_pipeline.maximum_active_chunks = 4;
        let range = leani_primitives::BlockRange::new(
            leani_primitives::BlockNumber(1),
            leani_primitives::BlockNumber(10),
        )
        .expect("range");
        let budget = historical_source_budget(&config, range);
        // Review I1: a read that streamed more than the memory budget in
        // total failed.
        assert_eq!(budget.max_input_bytes, config.budgets.temporary_disk_bytes);
        // Review 2 A: each read may hold the whole memory budget at once,
        // whichever lane it serves; a share of it failed real mainnet Xatu
        // row groups.
        for (lane, resident) in [
            ("backfill", budget.max_resident_bytes),
            ("live", live_source_budget(&config).max_resident_bytes),
            (
                "raw history",
                raw_history_source_budget(&config).max_resident_bytes,
            ),
        ] {
            assert_eq!(resident, config.budgets.memory_bytes, "{lane}");
        }
        assert!(budget.max_frame_bytes <= budget.max_resident_bytes);
        assert_eq!(budget.max_frames, 10);
    }

    #[tokio::test]
    async fn an_archive_read_may_acquire_more_than_memory_bytes() {
        use futures::StreamExt as _;
        use leani_source_api::HistorySource as _;

        let directory = tempfile::tempdir().expect("temporary directory");
        let chain = fixture_chain(64);
        let manifest = write_frame_archive(directory.path(), &chain[1..]);
        let object = fs::metadata(directory.path().join("frames.jsonl"))
            .expect("archive object")
            .len();
        let longest_line = chain[1..]
            .iter()
            .map(|frame| serde_json::to_vec(frame).expect("archive frame").len() + 1)
            .max()
            .expect("archive frames");
        let longest_line = u64::try_from(longest_line).expect("line length");
        let mut config: Config =
            toml::from_str(crate::config::VALID_CONFIG_TOML).expect("configuration fixture");
        // Room for two lines at once, far less than the object, and more
        // disk than it.
        config.budgets.memory_bytes = 2 * longest_line;
        config.budgets.temporary_disk_bytes = object * 4;
        assert!(object > config.budgets.memory_bytes);
        let range = leani_primitives::BlockRange::new(
            leani_primitives::BlockNumber(1),
            leani_primitives::BlockNumber(64),
        )
        .expect("range");
        let source =
            leani_source_archive::LocalArchiveSource::open_manifest(&manifest).expect("archive");
        let plan = source
            .plan(&leani_source_api::DataRequest {
                chain_id: leani_primitives::ChainId(1),
                range,
                required: leani_primitives::CapabilitySet::of(
                    leani_primitives::Capability::Transactions,
                ),
                allow_filtered: false,
                projection: leani_source_api::FieldProjection::default(),
                log_fields: leani_primitives::LogFieldSet::NONE,
                filters: leani_source_api::FilterSet::default(),
                minimum_finality: leani_primitives::Finality::Finalized,
                verification_policy: leani_source_api::VerificationPolicy::TrustedDataset,
            })
            .await
            .expect("plan");
        let read = |budget| {
            let source = &source;
            let plan = &plan;
            async move {
                let mut frames = 0_u64;
                for chunk in &plan.chunks {
                    let mut stream = source.open(chunk, budget, CancellationToken::new()).await?;
                    while let Some(frame) = stream.next().await {
                        frame?;
                        frames += 1;
                    }
                }
                Ok::<_, leani_source_api::SourceError>(frames)
            }
        };
        // Review I1: the read was refused for acquiring more than
        // `memory_bytes`, though it held one line at a time. Review 2 A: a
        // line within `memory_bytes` failed against a share of it.
        assert_eq!(
            read(historical_source_budget(&config, range))
                .await
                .expect("one line at a time fits the memory budget"),
            64
        );
        // A line larger than the memory budget fails the read, naming it.
        config.budgets.memory_bytes = longest_line - 1;
        let error = read(historical_source_budget(&config, range))
            .await
            .expect_err("a line exceeds the memory budget");
        assert!(
            matches!(
                error,
                leani_source_api::SourceError::BudgetExceeded {
                    resource: "resident_bytes",
                    ..
                }
            ),
            "{error}"
        );
        assert!(
            error.to_string().contains("budgets.memory_bytes"),
            "{error}"
        );
    }

    #[test]
    fn api_bearer_tokens_are_long_enough_and_never_echoed() {
        // Audit Auth-2: an empty or short token was accepted at startup.
        for token in ["", " ", "fifteen-chars-x", "short-secret"] {
            let error = api_bearer_token("LEANI_API_TOKEN", Ok(token.to_owned()))
                .expect_err("a short token fails startup");
            assert!(
                format!("{error:#}").contains("LEANI_API_TOKEN"),
                "{error:#}"
            );
            if !token.trim().is_empty() {
                assert!(!format!("{error:#}").contains(token), "{error:#}");
            }
        }
        let token = api_bearer_token("LEANI_API_TOKEN", Ok("sixteen-chars-ok".to_owned()))
            .expect("16 characters are enough");
        assert_eq!(token.as_ref(), "sixteen-chars-ok");

        let missing = api_bearer_token("LEANI_API_TOKEN", Err(std::env::VarError::NotPresent))
            .expect_err("an unset variable fails startup");
        assert!(format!("{missing:#}").contains("LEANI_API_TOKEN"));
        let binary = api_bearer_token(
            "LEANI_API_TOKEN",
            Err(std::env::VarError::NotUnicode("binary-secret-token".into())),
        )
        .expect_err("a non-Unicode variable fails startup");
        assert!(
            !format!("{binary:#}").contains("binary-secret-token"),
            "{binary:#}"
        );
    }

    #[test]
    fn api_bearer_tokens_are_printable_ascii_without_spaces() {
        // Review 1, minor 1: a token is printable ASCII without spaces. Most of
        // these could never authenticate; the interior spaces could, and are
        // refused all the same.
        for token in [
            "sixteen chars ok",
            " sixteen-chars-ok",
            "sixteen-chars-ok ",
            "sixteen-chars-ok\t",
            "sixteen-chars-\u{7f}ok",
            "sixteen-chars-ök",
        ] {
            let error = api_bearer_token("LEANI_API_TOKEN", Ok(token.to_owned()))
                .expect_err("an unusable token fails startup");
            let message = format!("{error:#}");
            assert!(message.contains("LEANI_API_TOKEN"), "{message}");
            assert!(!message.contains(token.trim()), "{message}");
        }
        api_bearer_token("LEANI_API_TOKEN", Ok("0123456789abcdef~!#$%".to_owned()))
            .expect("printable ASCII is accepted");
    }

    fn finalized_contradiction() -> anyhow::Error {
        anyhow::Error::from(leani_runtime::RuntimeError::FinalityContradiction {
            block: leani_primitives::BlockNumber(2),
            detail: "the finalized hash is 0xf2…, the canonical hash 0x02…".to_owned(),
        })
        .context("finality network lane")
    }

    fn unfinalized_contradiction(block: u64, finalized: u8) -> anyhow::Error {
        anyhow::Error::from(leani_runtime::RuntimeError::FinalityReorg {
            block: leani_primitives::BlockNumber(block),
            finalized: leani_primitives::BlockHash::new([finalized; 32]),
            detail: "the finalized hash is 0xf2…, the canonical hash 0x02…".to_owned(),
        })
        .context("finality network lane")
    }

    #[test]
    fn only_contradictions_of_finalized_history_or_persistent_ones_halt_the_network_lanes() {
        let mut reorgs = FinalityReorgRestarts::default();
        // A contradiction with finalized history halts at once.
        assert!(matches!(
            network_lane_failure(&Err(finalized_contradiction()), &mut reorgs),
            LaneFailure::Halt(_)
        ));
        // One with retained unfinalized blocks is a reorg the restart
        // reverts: the lanes restart, twice for the same finalized block,
        // and halt the third time.
        for _ in 0..2 {
            assert!(
                matches!(
                    network_lane_failure(&Err(unfinalized_contradiction(2, 0xf2)), &mut reorgs),
                    LaneFailure::Heal(_)
                ),
                "an unfinalized contradiction halted the network lanes"
            );
        }
        // Transient failures in between still restart, and do not reset the
        // count: finality has not moved.
        assert!(matches!(
            network_lane_failure(
                &Err(anyhow::anyhow!(
                    "verified Beacon API finality quorum was not accepted"
                )),
                &mut reorgs
            ),
            LaneFailure::Restart(_)
        ));
        let LaneFailure::Halt(failure) =
            network_lane_failure(&Err(unfinalized_contradiction(2, 0xf2)), &mut reorgs)
        else {
            panic!("a persistent contradiction restarted the network lanes a third time");
        };
        assert!(failure.contains("after 2 restarts"), "{failure}");
        // Once finality has moved to another block, a contradiction heals
        // again.
        assert!(matches!(
            network_lane_failure(&Err(unfinalized_contradiction(66, 0xf3)), &mut reorgs),
            LaneFailure::Heal(_)
        ));
        assert!(matches!(
            network_lane_failure(&Ok(()), &mut reorgs),
            LaneFailure::Restart(_)
        ));
    }

    fn lane_handles() -> NetworkLaneHandles {
        NetworkLaneHandles {
            readiness: leani_api::ReadinessHandle::new(true, true),
            rpc_readiness: leani_rpc::RpcReadiness::default(),
            committed_events: tokio::sync::broadcast::channel(1).0,
            network_telemetry: leani_source_api::NetworkTelemetry::default(),
            cancellation: CancellationToken::new(),
            backfill_control: None,
            verified_anchor: None,
            checkpoint_origin: leani_finality_beacon_api::CheckpointOrigin::Operator,
            attested_heads: leani_source_api::AttestedHeadPublisher::new(),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn the_supervisor_stops_restarting_once_the_network_lanes_halt() {
        // A contradiction with finalized history: one run, then a halt.
        let handles = lane_handles();
        let runs = std::sync::atomic::AtomicUsize::new(0);
        let end = tokio::time::timeout(
            Duration::from_mins(10),
            supervise_lane_runs(&handles, || {
                runs.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                async { Err(finalized_contradiction()) }
            }),
        )
        .await
        .expect("the supervisor kept restarting halted network lanes");
        assert_eq!(end, NetworkLanesEnd::Halted);
        assert_eq!(runs.load(std::sync::atomic::Ordering::SeqCst), 1);
        let supervisor = handles.network_telemetry.snapshot().supervisor;
        assert_eq!(
            supervisor.state,
            leani_source_api::NetworkSupervisorState::Stopped
        );
        assert_eq!(supervisor.retry_in_seconds, None);
        assert!(
            supervisor
                .last_error
                .is_some_and(|error| error.contains("verified finality contradicts")),
            "the halt records no explicit error"
        );
        assert!(!handles.readiness.is_ready());

        // The same unfinalized contradiction: two healing restarts, then a
        // halt.
        let handles = lane_handles();
        let runs = std::sync::atomic::AtomicUsize::new(0);
        let end = tokio::time::timeout(
            Duration::from_mins(10),
            supervise_lane_runs(&handles, || {
                runs.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                async { Err(unfinalized_contradiction(2, 0xf2)) }
            }),
        )
        .await
        .expect("a persistent contradiction restarted the network lanes forever");
        assert_eq!(end, NetworkLanesEnd::Halted);
        assert_eq!(runs.load(std::sync::atomic::Ordering::SeqCst), 3);
        assert_eq!(
            handles.network_telemetry.snapshot().supervisor.state,
            leani_source_api::NetworkSupervisorState::Stopped
        );

        // Anything else keeps restarting until cancelled.
        let handles = lane_handles();
        let runs = std::sync::atomic::AtomicUsize::new(0);
        let supervisor = supervise_lane_runs(&handles, || {
            if runs.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 4 {
                handles.cancellation.cancel();
            }
            async { Err(anyhow::anyhow!("execution peers unavailable")) }
        });
        let end = tokio::time::timeout(Duration::from_mins(10), supervisor)
            .await
            .expect("the supervisor stops once cancelled");
        assert_eq!(end, NetworkLanesEnd::Cancelled);
        assert_eq!(runs.load(std::sync::atomic::Ordering::SeqCst), 5);
    }

    #[tokio::test(start_paused = true)]
    async fn a_stalled_live_lane_wedges_the_supervisor() {
        // A lane restart cannot heal the process-wide execution P2P state
        // that stalled the live lane: the supervisor stops at once.
        let handles = lane_handles();
        let runs = std::sync::atomic::AtomicUsize::new(0);
        let end = tokio::time::timeout(
            Duration::from_mins(10),
            supervise_lane_runs(&handles, || {
                runs.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                async {
                    Err(anyhow::Error::new(LiveStalled {
                        timeout: Duration::from_mins(10),
                    }))
                }
            }),
        )
        .await
        .expect("the supervisor restarted a stalled live lane");
        assert_eq!(end, NetworkLanesEnd::Wedged);
        assert_eq!(runs.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(!handles.readiness.is_ready());
        assert_eq!(
            handles.network_telemetry.snapshot().supervisor.state,
            leani_source_api::NetworkSupervisorState::Stopped
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_stalled_run_stops_without_waiting_for_a_live_lane_that_ignores_cancellation() {
        // A live lane waiting on a hung network build ignores cancellation.
        // Waiting for it kept a stall from ever exiting the node.
        let stalled = Err(anyhow::Error::new(LiveStalled {
            timeout: Duration::from_mins(10),
        }));
        tokio::time::timeout(
            shutdown::SHUTDOWN_DEADLINE + Duration::from_secs(1),
            stop_lanes(&stalled, std::future::pending(), async {}),
        )
        .await
        .expect("a stalled run stops by the shutdown deadline");

        // Any other end drives a suspended live commit to completion.
        let live_finished = std::sync::atomic::AtomicBool::new(false);
        stop_lanes(
            &Err(anyhow::anyhow!("another failure")),
            async {
                tokio::time::sleep(Duration::from_mins(1)).await;
                live_finished.store(true, std::sync::atomic::Ordering::SeqCst);
            },
            async {},
        )
        .await;
        assert!(live_finished.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test(start_paused = true)]
    async fn a_live_lane_stalls_while_finality_is_ready_and_storage_has_room() {
        let (live, live_updates) = tokio::sync::watch::channel(false);
        let (finality, finality_updates) = tokio::sync::watch::channel(false);
        let (storage_full, storage_full_updates) = tokio::sync::watch::channel(false);
        let timeout = Duration::from_mins(10);
        let almost = Duration::from_secs(599);
        let stall = tokio::spawn(live_stall(
            live_updates,
            finality_updates,
            storage_full_updates,
            timeout,
        ));
        let settle = || tokio::time::sleep(Duration::from_millis(1));
        // Nothing ready, as while the network is down: no stall.
        tokio::time::sleep(Duration::from_hours(1)).await;
        assert!(!stall.is_finished(), "a node without finality stalled");
        // Finality ready, live not: the stall starts...
        finality.send(true).expect("finality watch");
        settle().await;
        tokio::time::sleep(almost).await;
        // ...and a live lane that recovers in time ends it.
        live.send(true).expect("live watch");
        settle().await;
        tokio::time::sleep(Duration::from_hours(1)).await;
        assert!(!stall.is_finished(), "a recovered live lane stalled");
        // Losing finality as well also ends it.
        live.send(false).expect("live watch");
        settle().await;
        tokio::time::sleep(almost).await;
        finality.send(false).expect("finality watch");
        settle().await;
        tokio::time::sleep(Duration::from_hours(1)).await;
        assert!(!stall.is_finished(), "a stall outlived verified finality");
        // A full recent-frame store holds the live lane back too, and the
        // runtime's own limit restarts the lanes for it.
        storage_full.send(true).expect("storage watch");
        finality.send(true).expect("finality watch");
        settle().await;
        tokio::time::sleep(Duration::from_hours(1)).await;
        assert!(!stall.is_finished(), "a storage wait counted as a stall");
        // A stall that lasts the timeout resolves.
        storage_full.send(false).expect("storage watch");
        let started = tokio::time::Instant::now();
        let stalled = tokio::time::timeout(timeout + Duration::from_secs(1), stall)
            .await
            .expect("the live lane stalled for the whole timeout")
            .expect("the stall watch");
        assert_eq!(started.elapsed(), timeout);
        assert_eq!(stalled.timeout, timeout);
    }

    #[test]
    fn doctor_warns_when_the_active_finality_anchor_expires_within_three_days() {
        use leani_finality_beacon_api::{
            FINALITY_ANCHOR_FILE, MAINNET_GENESIS_TIME, PersistedFinalityAnchor,
            VerifiedFinalityAnchor, persist_finality_anchor,
        };
        use leani_primitives::BlockHash;

        const DAY: u64 = 86_400;
        let directory = tempfile::tempdir().expect("temporary directory");
        let mut config: Config =
            toml::from_str(crate::config::VALID_CONFIG_TOML).expect("configuration fixture");
        config.data_dir = directory.path().to_path_buf();
        let checkpoint_slot = 10_000_000;
        config.finality.checkpoint_slot = checkpoint_slot;
        let checkpoint_time = MAINNET_GENESIS_TIME + checkpoint_slot * 12;
        let at = |seconds: u64| UNIX_EPOCH + Duration::from_secs(seconds);

        assert!(finality_anchor_warnings(&config, at(checkpoint_time + 10 * DAY)).is_empty());
        let warnings = finality_anchor_warnings(&config, at(checkpoint_time + 12 * DAY));
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(
            warnings[0].contains("configured checkpoint"),
            "{warnings:?}"
        );

        // A newer anchor verified from the configured checkpoint is active
        // instead, and it expires later.
        let root = checkpoint_root(&config);
        persist_finality_anchor(
            &directory.path().join(FINALITY_ANCHOR_FILE),
            &PersistedFinalityAnchor {
                anchor: VerifiedFinalityAnchor {
                    beacon_slot: checkpoint_slot + 10 * 7_200,
                    beacon_block_root: [0xaa; 32],
                    execution_block_number: 20_000_000,
                    execution_block_hash: BlockHash::new([0xbb; 32]),
                },
                checkpoint_root: root,
            },
        )
        .expect("persist anchor");
        assert!(finality_anchor_warnings(&config, at(checkpoint_time + 12 * DAY)).is_empty());
        let warnings = finality_anchor_warnings(&config, at(checkpoint_time + 22 * DAY));
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(warnings[0].contains("persisted"), "{warnings:?}");
        let expired = finality_anchor_warnings(&config, at(checkpoint_time + 30 * DAY));
        assert!(expired[0].contains("expired"), "{expired:?}");

        config.finality.kind = crate::config::FinalitySourceKind::Disabled;
        assert!(finality_anchor_warnings(&config, at(checkpoint_time + 30 * DAY)).is_empty());
    }

    #[test]
    fn doctor_warns_about_a_persisted_anchor_from_another_trust_root() {
        use leani_finality_beacon_api::{
            FINALITY_ANCHOR_FILE, MAINNET_GENESIS_TIME, PersistedFinalityAnchor,
            VerifiedFinalityAnchor, persist_finality_anchor,
        };
        use leani_primitives::BlockHash;

        let directory = tempfile::tempdir().expect("temporary directory");
        let mut config: Config =
            toml::from_str(crate::config::VALID_CONFIG_TOML).expect("configuration fixture");
        config.data_dir = directory.path().to_path_buf();
        let checkpoint_slot = 10_000_000;
        config.finality.checkpoint_slot = checkpoint_slot;
        persist_finality_anchor(
            &directory.path().join(FINALITY_ANCHOR_FILE),
            &PersistedFinalityAnchor {
                anchor: VerifiedFinalityAnchor {
                    beacon_slot: checkpoint_slot + 7_200,
                    beacon_block_root: [0xaa; 32],
                    execution_block_number: 20_000_000,
                    execution_block_hash: BlockHash::new([0xbb; 32]),
                },
                checkpoint_root: [0x44; 32],
            },
        )
        .expect("persist anchor");
        let now = UNIX_EPOCH + Duration::from_secs(MAINNET_GENESIS_TIME + checkpoint_slot * 12);
        let warnings = finality_anchor_warnings(&config, now);
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(
            warnings[0].contains(&format!("0x{}", hex::encode([0x44; 32])))
                && warnings[0].contains(&config.finality.checkpoint),
            "{warnings:?}"
        );
    }

    fn checkpoint_root(config: &Config) -> [u8; 32] {
        leani_finality_beacon_api::parse_checkpoint_root(&config.finality.checkpoint)
            .expect("checkpoint root")
    }

    #[test]
    fn verified_anchor_notifications_never_rewind_or_replace_a_conflicting_identity() {
        use leani_primitives::BlockHash;
        use leani_runtime::AppliedFinalityAnchor;

        let first = AppliedFinalityAnchor {
            block: fixture_frame(1, BlockHash::ZERO).block,
            beacon_slot: 100,
            beacon_block_root: [1; 32],
        };
        let next = AppliedFinalityAnchor {
            block: fixture_frame(2, first.block.hash).block,
            beacon_slot: 132,
            beacon_block_root: [2; 32],
        };
        let (sender, mut receiver) = tokio::sync::watch::channel(None);
        assert!(publish_verified_anchor(&sender, first).expect("initial anchor"));
        assert!(publish_verified_anchor(&sender, next).expect("newer anchor"));
        assert_eq!(*receiver.borrow_and_update(), Some(next));
        assert!(!publish_verified_anchor(&sender.clone(), first).expect("stale bootstrap"));
        assert!(!publish_verified_anchor(&sender, next).expect("duplicate anchor"));
        assert!(!receiver.has_changed().expect("no redundant notifications"));

        for conflict in [
            AppliedFinalityAnchor {
                beacon_block_root: [3; 32],
                ..next
            },
            AppliedFinalityAnchor {
                block: first.block,
                ..next
            },
            AppliedFinalityAnchor {
                beacon_slot: 164,
                block: first.block,
                ..next
            },
            AppliedFinalityAnchor {
                beacon_slot: 164,
                block: leani_primitives::BlockRef {
                    hash: first.block.hash,
                    ..next.block
                },
                ..next
            },
        ] {
            assert!(publish_verified_anchor(&sender, conflict).is_err());
            assert_eq!(*receiver.borrow(), Some(next));
            assert!(!receiver.has_changed().expect("conflict not published"));
        }
    }

    #[tokio::test]
    async fn a_start_supersedes_the_automatic_jobs_earlier_starts_left_unfinished() {
        use leani_store_sqlite::JobState;

        // Three restarts listed three running automatic jobs for one
        // processor, although only the newest could be executing.
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = leani_store_sqlite::SqliteStore::open(leani_store_sqlite::StoreConfig::new(
            directory.path().join("node.sqlite"),
        ))
        .await
        .expect("store");
        let jobs = [
            ("automatic:1:ledger:5-100", JobState::Running),
            ("automatic:1:ledger:5-120", JobState::Queued),
            ("automatic:1:ledger:5-90", JobState::Completed),
            ("automatic:1:ledger-two:5-120", JobState::Running),
            // Instance IDs may hold `:`, so `ledger:two` is another instance.
            ("automatic:1:ledger:two:5-120", JobState::Running),
            ("automatic:1:ledger:5-140", JobState::Running),
        ];
        for (id, state) in jobs {
            store
                .save_job(&leani_store_sqlite::JobRecord {
                    id: id.to_owned(),
                    kind: leani_runtime::HistoricalJobOwner::Materialization
                        .job_kind()
                        .to_owned(),
                    state,
                    payload: b"fixture".to_vec(),
                    checkpoint: None,
                    attempts: 1,
                    updated_at_unix_ms: 1,
                })
                .await
                .expect("save job");
        }

        supersede_automatic_jobs(&store, "automatic:1:ledger:", "automatic:1:ledger:5-140")
            .await
            .expect("supersede");

        let states = store
            .jobs(None)
            .await
            .expect("jobs")
            .into_iter()
            .map(|job| (job.id, job.state))
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(states["automatic:1:ledger:5-100"], JobState::Cancelled);
        assert_eq!(states["automatic:1:ledger:5-120"], JobState::Cancelled);
        assert_eq!(states["automatic:1:ledger:5-90"], JobState::Completed);
        assert_eq!(states["automatic:1:ledger-two:5-120"], JobState::Running);
        assert_eq!(states["automatic:1:ledger:two:5-120"], JobState::Running);
        assert_eq!(states["automatic:1:ledger:5-140"], JobState::Running);
    }

    #[tokio::test]
    async fn failed_backfill_updates_the_primary_scheduler_record() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = leani_store_sqlite::SqliteStore::open(leani_store_sqlite::StoreConfig::new(
            directory.path().join("node.sqlite"),
        ))
        .await
        .expect("store");
        store
            .save_job(&leani_store_sqlite::JobRecord {
                id: "terminal-failure".to_owned(),
                kind: leani_runtime::HistoricalJobOwner::Materialization
                    .job_kind()
                    .to_owned(),
                state: leani_store_sqlite::JobState::Running,
                payload: b"fixture".to_vec(),
                checkpoint: Some(b"checkpoint".to_vec()),
                attempts: 2,
                updated_at_unix_ms: 1,
            })
            .await
            .expect("running job");

        NativeBackfillControl::record_failure(
            &store,
            "fixture",
            "terminal-failure",
            leani_runtime::HistoricalJobOwner::Materialization,
            leani_store_sqlite::JobState::Failed,
            Some("permanent failure".to_owned()),
        )
        .await;

        assert_eq!(
            store
                .job("terminal-failure")
                .await
                .expect("primary job")
                .expect("primary job exists")
                .state,
            leani_store_sqlite::JobState::Failed
        );
        assert_eq!(
            store
                .job("terminal-failure:outcome")
                .await
                .expect("outcome")
                .expect("outcome exists")
                .state,
            leani_store_sqlite::JobState::Failed
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn short_consumer_credentials_are_refused_only_for_new_subscriptions() {
        // Review 1, Important 2: an application re-submits its subscription
        // at startup, and one created while 16 characters sufficed must still
        // get its status back. Only a new subscription meets the 32-character
        // rule, as a client error before any stream exists.
        use leani_api::BackfillControl as _;

        let directory = tempfile::tempdir().expect("temporary directory");
        let store = leani_store_sqlite::SqliteStore::open(leani_store_sqlite::StoreConfig::new(
            directory.path().join("node.sqlite"),
        ))
        .await
        .expect("store");
        let processor: Arc<dyn Processor> = Arc::new(BlockLocalCounter::default());
        let instance = processor.descriptor().instance.to_string();
        let mut config: Config =
            toml::from_str(crate::config::VALID_CONFIG_TOML).expect("configuration fixture");
        config.processors[0].instance.clone_from(&instance);
        config.processors[0].history_control =
            crate::config::ProcessorHistoryControl::ApplicationSubscriptions;
        let control = NativeBackfillControl::new(
            config,
            store.clone(),
            vec![processor.clone()],
            CancellationToken::new(),
            None,
            None,
            leani_runtime::HistoricalPipelineBudget::new(1, 1, 1_024).expect("pipeline budget"),
        );
        let request = |idempotency_key: &str| leani_api::CreateBackfillRequest {
            processor: instance.clone(),
            from_block: Some(1),
            to_block: Some(2.into()),
            ranges: Vec::new(),
            mode: leani_api::BackfillExecutionMode::FillMissing,
            consumer: Some(leani_api::CreateBackfillConsumerRequest {
                id: "destination".to_owned(),
                role: leani_store_sqlite::ConsumerRole::Required,
                lease_ttl_seconds: 60,
                credential: Some("twenty-char-secret-1".to_owned()),
            }),
            limits: None,
            batching: None,
            idempotency_key: idempotency_key.to_owned(),
        };

        // What an earlier release stored for such a request; the idempotent
        // path reads only the job and the request's identity.
        let before_upgrade = request("before-upgrade");
        let id = format!("subscription:{instance}:before-upgrade");
        let mut job = leani_runtime::BackfillJob::for_processor(
            id.clone(),
            processor.as_ref(),
            leani_primitives::ChainId(1),
            leani_primitives::BlockRange::new(
                leani_primitives::BlockNumber(1),
                leani_primitives::BlockNumber(2),
            )
            .expect("range"),
            leani_source_api::VerificationPolicy::TrustedDataset,
        )
        .expect("job");
        job.owner = leani_runtime::HistoricalJobOwner::Subscription;
        store
            .create_historical_job(
                &leani_store_sqlite::JobRecord {
                    id: id.clone(),
                    kind: job.owner.job_kind().to_owned(),
                    state: leani_store_sqlite::JobState::Queued,
                    payload: serde_json::to_vec(&job).expect("job payload"),
                    checkpoint: None,
                    attempts: 0,
                    updated_at_unix_ms: 1,
                },
                NativeBackfillControl::historical_request_identity(
                    &before_upgrade,
                    leani_api::HistoricalWorkOwner::Subscription,
                    &instance,
                )
                .expect("request identity"),
            )
            .await
            .expect("subscription from before the upgrade");
        let status = control
            .create_subscription(before_upgrade)
            .await
            .expect("a re-submission gets the stored status");
        assert_eq!(status.id, id);
        assert_eq!(status.state, leani_api::BackfillState::Queued);

        store
            .store_canonical_anchor(
                leani_primitives::ChainId(1),
                leani_primitives::BlockRef {
                    number: leani_primitives::BlockNumber(10),
                    hash: leani_primitives::BlockHash::new([10; 32]),
                    parent_hash: leani_primitives::BlockHash::new([9; 32]),
                    timestamp: 1,
                },
                leani_primitives::Finality::Finalized,
            )
            .await
            .expect("finalized head");
        let streams = store
            .delivery_streams(processor.descriptor())
            .await
            .expect("streams")
            .len();
        let refused = control.create_subscription(request("after-upgrade")).await;
        assert!(
            matches!(
                &refused,
                Err(leani_api::BackfillControlError::Invalid(message))
                    if message.contains("32 to 512")
            ),
            "{refused:?}"
        );
        assert_eq!(
            store
                .delivery_streams(processor.descriptor())
                .await
                .expect("streams")
                .len(),
            streams
        );
    }

    #[tokio::test]
    async fn tiered_artifact_supervisor_compacts_full_ranges_in_background() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let mut config: Config =
            toml::from_str(crate::config::VALID_CONFIG_TOML).expect("configuration fixture");
        config.data_dir = directory.path().to_path_buf();
        config.artifact_storage.backend = ArtifactStorageBackend::TieredSegments;
        config.artifact_storage.segment_target_blocks = 2;
        config.artifact_storage.maximum_segments_per_cycle = 2;
        let store = leani_store_sqlite::SqliteStore::open(configured_store_config(
            &config,
            directory.path().join("node.sqlite"),
        ))
        .await
        .expect("tiered store");
        let base = BlockLocalCounter::default();
        let mut lifecycle = base.descriptor().lifecycle.clone();
        lifecycle.artifacts.mode = ArtifactPolicyMode::Full;
        lifecycle.artifacts.window = None;
        let processor = std::sync::Arc::new(
            base.with_lifecycle(lifecycle)
                .with_output_none()
                .with_delivery_none(),
        );
        let descriptor = processor.descriptor().clone();
        let mut parent = leani_primitives::BlockHash::ZERO;
        for number in 1..=4 {
            let frame = fixture_frame(number, parent);
            let delta = processor.map(&frame).await.expect("map artifact");
            store
                .retain_finalized_artifact(
                    processor.descriptor(),
                    &delta,
                    leani_primitives::Finality::Finalized,
                )
                .await
                .expect("retain artifact");
            parent = frame.block.hash;
        }
        let cancellation = CancellationToken::new();
        let supervisor = tokio::spawn(supervise_artifact_compaction(
            store.clone(),
            vec![processor],
            config.artifact_storage,
            cancellation.clone(),
        ));
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if store
                    .processor_artifact_segment_stats()
                    .await
                    .is_some_and(|stats| stats.artifacts == 4)
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("background compaction completes");
        cancellation.cancel();
        supervisor.await.expect("compaction supervisor exits");
        assert_eq!(
            store
                .processor_artifact_stats(&descriptor)
                .await
                .expect("artifact stats")
                .artifacts,
            4
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn application_backfill_ranges_are_sorted_coalesced_and_bounded_by_work() {
        let request = leani_api::CreateBackfillRequest {
            processor: "fixture".to_owned(),
            from_block: None,
            to_block: None,
            ranges: vec![
                leani_api::CreateBackfillRangeRequest {
                    from_block: 10,
                    to_block: 12.into(),
                },
                leani_api::CreateBackfillRangeRequest {
                    from_block: 1,
                    to_block: 2.into(),
                },
                leani_api::CreateBackfillRangeRequest {
                    from_block: 3,
                    to_block: 4.into(),
                },
            ],
            mode: leani_api::BackfillExecutionMode::FillMissing,
            consumer: None,
            limits: None,
            batching: None,
            idempotency_key: "ranges".to_owned(),
        };
        assert_eq!(
            NativeBackfillControl::normalized_request_ranges(
                &request,
                12,
                leani_primitives::BlockHash::ZERO,
            )
            .expect("ranges"),
            vec![
                leani_primitives::BlockRange::new(
                    leani_primitives::BlockNumber(1),
                    leani_primitives::BlockNumber(4),
                )
                .expect("coalesced"),
                leani_primitives::BlockRange::new(
                    leani_primitives::BlockNumber(10),
                    leani_primitives::BlockNumber(12),
                )
                .expect("disjoint"),
            ]
        );

        let mut ambiguous = request.clone();
        ambiguous.from_block = Some(1);
        ambiguous.to_block = Some(12.into());
        assert!(matches!(
            NativeBackfillControl::normalized_request_ranges(
                &ambiguous,
                12,
                leani_primitives::BlockHash::ZERO,
            ),
            Err(leani_api::BackfillControlError::Invalid(message))
                if message.contains("cannot be combined")
        ));

        let through_finalized = leani_api::CreateBackfillRequest {
            processor: "fixture".to_owned(),
            from_block: Some(10),
            to_block: Some(leani_api::BackfillUpperBound::Finalized),
            ranges: Vec::new(),
            mode: leani_api::BackfillExecutionMode::FillMissing,
            consumer: None,
            limits: None,
            batching: None,
            idempotency_key: "finalized".to_owned(),
        };
        assert_eq!(
            NativeBackfillControl::normalized_request_ranges(
                &through_finalized,
                12,
                leani_primitives::BlockHash::ZERO,
            )
            .expect("resolve finalized target"),
            vec![
                leani_primitives::BlockRange::new(
                    leani_primitives::BlockNumber(10),
                    leani_primitives::BlockNumber(12),
                )
                .expect("resolved range")
            ]
        );
        let mut beyond = through_finalized.clone();
        beyond.to_block = Some(13.into());
        assert!(matches!(
            NativeBackfillControl::normalized_request_ranges(
                &beyond,
                12,
                leani_primitives::BlockHash::ZERO,
            ),
            Err(leani_api::BackfillControlError::HistoryNotFinalized {
                requested: 13,
                finalized: 12,
                ..
            })
        ));
        let mut after = through_finalized;
        after.from_block = Some(13);
        assert!(matches!(
            NativeBackfillControl::normalized_request_ranges(
                &after,
                12,
                leani_primitives::BlockHash::ZERO,
            ),
            Err(leani_api::BackfillControlError::RangeAfterFinalizedHead {
                requested: 13,
                finalized: 12,
                ..
            })
        ));
        let sentinel_identity = NativeBackfillControl::historical_request_identity(
            &after,
            leani_api::HistoricalWorkOwner::Subscription,
            "fixture-instance",
        )
        .expect("sentinel identity");
        assert_eq!(
            sentinel_identity,
            NativeBackfillControl::historical_request_identity(
                &after,
                leani_api::HistoricalWorkOwner::Subscription,
                "fixture-instance",
            )
            .expect("stable sentinel identity")
        );
        let mut numeric = after;
        numeric.to_block = Some(12.into());
        assert_ne!(
            sentinel_identity,
            NativeBackfillControl::historical_request_identity(
                &numeric,
                leani_api::HistoricalWorkOwner::Subscription,
                "fixture-instance",
            )
            .expect("numeric identity")
        );
    }

    #[tokio::test]
    async fn missing_configuration_fails_before_state_is_opened() {
        let cli = Cli::try_parse_from([
            "leani",
            "--config",
            "/definitely/missing/leani.toml",
            "serve",
        ])
        .expect("CLI parses");
        let error = run_cli(cli).await.expect_err("missing config fails");
        assert!(error.to_string().contains("failed to read configuration"));
    }

    #[test]
    fn configuration_discovery_uses_only_explicit_or_project_local_paths() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let workspace_config = directory.path().join("config/example.toml");
        std::fs::create_dir_all(workspace_config.parent().expect("config parent"))
            .expect("create config directory");
        std::fs::write(&workspace_config, "workspace").expect("write workspace config");
        assert_eq!(
            resolve_config_path(None, directory.path()),
            directory.path().join("leani.toml")
        );

        let local_config = directory.path().join("leani.toml");
        std::fs::write(&local_config, "local").expect("write local config");
        assert_eq!(resolve_config_path(None, directory.path()), local_config);
        assert_eq!(
            resolve_config_path(Some(Path::new("explicit.toml")), directory.path()),
            PathBuf::from("explicit.toml")
        );
    }

    #[tokio::test]
    async fn process_stops_when_cancelled() {
        let config = toml::from_str::<Config>(super::super::config::VALID_CONFIG_TOML)
            .expect("configuration parses")
            .validate()
            .expect("configuration validates");
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        tokio::time::timeout(
            std::time::Duration::from_millis(100),
            serve_until_cancelled(&config, &cancellation),
        )
        .await
        .expect("shutdown completes");
    }

    #[tokio::test]
    async fn embedded_runtime_shutdown_aborts_an_uncooperative_network_task() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let cancellation = CancellationToken::new();
        let (_, verified_anchor) = tokio::sync::watch::channel(None);
        let execution_source = std::sync::Arc::new(
            leani_source_p2p::RethP2pSource::mainnet(leani_source_p2p::RethP2pConfig::default())
                .expect("execution source"),
        );
        let runtime = EmbeddedNetworkRuntime {
            readiness: leani_api::ReadinessHandle::new(true, true),
            verified_anchor,
            execution_source,
            cancellation,
            task: tokio::spawn(std::future::pending()),
            _data_dir_lock: crate::local_state::lock_runtime_directory(directory.path())
                .expect("runtime directory lock"),
        };

        tokio::time::timeout(Duration::from_millis(1_500), runtime.shutdown())
            .await
            .expect("embedded shutdown is bounded");
    }

    #[tokio::test]
    async fn live_restart_uses_the_retained_canonical_tip_without_refetching_overlap() {
        use leani_primitives::{BlockHash, ChainId};
        use leani_source_api::LiveStart;
        use leani_store_sqlite::{SqliteStore, StoreConfig};

        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteStore::open(StoreConfig::new(directory.path().join("resume.sqlite")))
            .await
            .expect("store");
        let mut parent = BlockHash::ZERO;
        let mut frames = Vec::new();
        for number in 10..=13 {
            let frame = fixture_frame(number, parent);
            parent = frame.block.hash;
            store
                .store_recent_frame(&frame)
                .await
                .expect("recent frame");
            frames.push(frame);
        }
        let start = retained_live_start(&store, ChainId(1), frames[2].block, 3, 64, true)
            .await
            .expect("resume start");
        let LiveStart::RetainedCanonical { canonical } = start else {
            panic!("expected retained canonical resume");
        };
        assert_eq!(canonical.first(), Some(&frames[2].block));
        assert_eq!(canonical.last(), Some(&frames[3].block));
    }

    #[tokio::test]
    async fn live_restart_does_not_refetch_pruned_pre_anchor_overlap() {
        use leani_primitives::{BlockHash, ChainId};
        use leani_source_api::LiveStart;
        use leani_store_sqlite::{SqliteStore, StoreConfig};

        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteStore::open(StoreConfig::new(directory.path().join("resume.sqlite")))
            .await
            .expect("store");
        let mut parent = BlockHash::ZERO;
        let mut frames = Vec::new();
        for number in 11..=13 {
            let frame = fixture_frame(number, parent);
            parent = frame.block.hash;
            store
                .store_recent_frame(&frame)
                .await
                .expect("recent frame");
            frames.push(frame);
        }
        let start = retained_live_start(&store, ChainId(1), frames[1].block, 3, 64, true)
            .await
            .expect("resume start");
        let LiveStart::RetainedCanonical { canonical } = start else {
            panic!("expected retained canonical resume");
        };
        assert_eq!(canonical, vec![frames[1].block, frames[2].block]);
    }

    #[tokio::test]
    async fn live_restart_refetches_when_the_verified_anchor_is_absent() {
        use leani_primitives::{BlockHash, ChainId};
        use leani_source_api::LiveStart;
        use leani_store_sqlite::{SqliteStore, StoreConfig};

        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteStore::open(StoreConfig::new(directory.path().join("resume.sqlite")))
            .await
            .expect("store");
        let anchor = fixture_frame(10, BlockHash::ZERO).block;
        let frame = fixture_frame(11, BlockHash::ZERO);
        store
            .store_recent_frame(&frame)
            .await
            .expect("recent frame");
        let start = retained_live_start(&store, ChainId(1), anchor, 3, 64, true)
            .await
            .expect("resume start");
        assert_eq!(
            start,
            LiveStart::AnchoredOverlap {
                anchor,
                overlap_blocks: 3,
            }
        );
        let on_demand = retained_live_start(&store, ChainId(1), anchor, 3, 64, false)
            .await
            .expect("on-demand start");
        assert_eq!(on_demand, LiveStart::Block(anchor));
    }

    #[tokio::test]
    async fn mainnet_e2e_observation_requires_verified_continuous_fresh_head() {
        use std::{
            sync::Arc,
            time::{SystemTime, UNIX_EPOCH},
        };

        use leani_primitives::{BlockNumber, BlockRange, ChainId, ProcessorCursor};
        use leani_store_sqlite::{SqliteStore, StoreConfig};

        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteStore::open(StoreConfig::new(
            directory.path().join("e2e-observation.sqlite"),
        ))
        .await
        .expect("store");
        let processor = Arc::new(BlockLocalCounter::default());
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let mut parent = leani_primitives::BlockHash::ZERO;
        let mut frames = Vec::new();
        for number in 0..=4 {
            let mut frame = fixture_frame(number, parent);
            frame.block.timestamp = now.saturating_sub(1);
            parent = frame.block.hash;
            store
                .store_recent_frame(&frame)
                .await
                .expect("recent frame");
            let delta = processor.map(&frame).await.expect("map");
            store
                .apply(
                    processor.as_ref(),
                    ProcessorCursor {
                        processor_id: processor.descriptor().id.to_string(),
                        processor_version: processor.descriptor().version.to_string(),
                        chain_id: ChainId(1),
                        block_number: frame.block.number,
                        block_hash: frame.block.hash,
                        finality: frame.finality,
                        sequence: number.saturating_add(1),
                    },
                    &delta,
                    &[],
                )
                .await
                .expect("apply");
            frames.push(frame);
        }
        let overlap = BlockRange::new(BlockNumber(1), BlockNumber(2)).expect("overlap");
        store
            .verify_hot_cold_handoff(
                "observation-handoff",
                processor.descriptor(),
                ChainId(1),
                overlap,
                frames[2].block.hash,
            )
            .await
            .expect("verified handoff");
        let processors: Vec<Arc<dyn Processor>> = vec![processor];
        let observation = mainnet_e2e_observation(&store, &processors, ChainId(1), 0, 2, 60)
            .await
            .expect("observation")
            .expect("converged");
        assert_eq!(observation.latest_block, 4);
        assert!(observation.latest_block_age_seconds <= 2);
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn one_failed_cold_backfill_parks_only_its_processor() {
        use std::sync::Arc;

        use leani_primitives::{BlockNumber, BlockRange, ChainId};
        use leani_runtime::{SharedLiveRuntime, SharedLiveRuntimeConfig};
        use leani_source_api::{ChainEvent, LiveStart};
        use leani_store_sqlite::{
            HotColdHandoffState, ProcessorRunState, SqliteStore, StoreConfig,
        };
        use leani_testkit::{
            LiveStep, OrderedLedgerProcessor, ScriptedLiveSource, default_source_budget,
            fixture_source_descriptor,
        };

        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteStore::open(StoreConfig::new(directory.path().join("handoff.sqlite")))
            .await
            .expect("store");
        let mut parent = leani_primitives::BlockHash::ZERO;
        let mut chain = Vec::new();
        for number in 0..=3 {
            let frame = fixture_frame(number, parent);
            parent = frame.block.hash;
            chain.push(frame);
        }
        for frame in &chain[..=2] {
            store.store_recent_frame(frame).await.expect("recent frame");
        }
        // The ordered processor's history is far behind, so a parked lane
        // stays parked until a later backfill reaches its gap.
        let failed = Arc::new(OrderedLedgerProcessor::named("handoff-failed-ledger"));
        let verified = Arc::new(BlockLocalCounter::named("handoff-verified-counter"));
        let overlap = BlockRange::new(BlockNumber(1), BlockNumber(2)).expect("overlap");
        // Like an execution live source, it also supplies the headers the
        // ledger requests.
        let mut live_descriptor = fixture_source_descriptor(
            "handoff-live",
            BlockRange::new(BlockNumber(0), BlockNumber(3)).expect("range"),
        );
        live_descriptor.capabilities = live_descriptor
            .capabilities
            .with(leani_primitives::Capability::Header);
        live_descriptor.complete_capabilities = live_descriptor
            .complete_capabilities
            .with(leani_primitives::Capability::Header);
        let live = SharedLiveRuntime::new(
            store.clone(),
            Arc::new(ScriptedLiveSource::new(
                live_descriptor.clone(),
                vec![LiveStep::Event(ChainEvent::Block(Box::new(
                    chain[3].clone(),
                )))],
            )),
            vec![failed.clone(), verified.clone()],
            SharedLiveRuntimeConfig::default(),
        )
        .expect("live runtime");
        live.reconcile_pending()
            .await
            .expect("startup reconciliation");
        for (id, processor) in [
            ("handoff-failed", failed.descriptor()),
            ("handoff-verified", verified.descriptor()),
        ] {
            store
                .begin_hot_cold_handoff(id, processor, ChainId(1), overlap, chain[2].block.hash)
                .await
                .expect("begin handoff");
        }
        let verified_record = store
            .hot_cold_handoff("handoff-verified", verified.descriptor())
            .await
            .expect("handoff")
            .expect("record");
        let failure = ColdHandoffFailure {
            processor: failed.descriptor().clone(),
            handoff_id: "handoff-failed".to_owned(),
            detail: "automatic cold backfill failed: injected source exhaustion".to_owned(),
        };
        let mut backfills = ColdBackfills::default();
        backfills.spawn("automatic-failed", async move {
            Err(anyhow::Error::new(failure))
        });
        backfills.spawn("automatic-verified", async move { Ok(verified_record) });
        let processors: Vec<Arc<dyn Processor>> = vec![failed.clone(), verified.clone()];

        let summary = finish_cold_handoffs(&store, &live, &processors, &mut backfills)
            .await
            .expect("one processor's failed cold backfill does not fail the other handoffs");

        assert_eq!(summary.verified.len(), 1);
        assert_eq!(summary.verified[0].processor_id, "handoff-verified-counter");
        assert_eq!(
            summary.failed,
            vec![failed.descriptor().instance.to_string()]
        );
        let failed_handoff = store
            .hot_cold_handoff("handoff-failed", failed.descriptor())
            .await
            .expect("handoff")
            .expect("record");
        assert_eq!(failed_handoff.state, HotColdHandoffState::Failed);
        assert!(
            failed_handoff
                .failure
                .as_deref()
                .is_some_and(|failure| failure.contains("injected source exhaustion"))
        );
        let parked = store
            .processor_runtime_state(failed.descriptor())
            .await
            .expect("state");
        assert_eq!(parked.state, ProcessorRunState::Paused);
        assert_eq!(parked.reason.as_deref(), Some("hot_cold_handoff_failed"));
        assert_eq!(
            store
                .live_lane_gap(failed.descriptor())
                .await
                .expect("gap")
                .expect("parked at the live tip")
                .first_unapplied,
            chain[2].block
        );

        // The other processor keeps following live blocks.
        let report = live
            .run(
                LiveStart::Head,
                default_source_budget(),
                CancellationToken::new(),
            )
            .await
            .expect("live run");
        assert_eq!(report.processors["handoff-verified-counter"].applied, 1);
        assert_eq!(report.processors["handoff-failed-ledger"].applied, 0);
        assert_eq!(
            store
                .processor_runtime_state(verified.descriptor())
                .await
                .expect("state")
                .state,
            ProcessorRunState::Running
        );
        assert_eq!(
            store
                .processor_runtime_state(failed.descriptor())
                .await
                .expect("state")
                .state,
            ProcessorRunState::Paused
        );

        // Final review B9: without a retained frame the lane is not parked,
        // and the log said it was.
        let logs = CapturedLogs::default();
        let _logs = tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_ansi(false)
                .with_writer({
                    let logs = logs.clone();
                    move || logs.clone()
                })
                .finish(),
        );
        let bare = SqliteStore::open(StoreConfig::new(directory.path().join("bare.sqlite")))
            .await
            .expect("store without retained frames");
        let unparked = Arc::new(OrderedLedgerProcessor::named("handoff-unparked-ledger"));
        let bare_live = SharedLiveRuntime::new(
            bare.clone(),
            Arc::new(ScriptedLiveSource::new(live_descriptor, Vec::new())),
            vec![unparked.clone()],
            SharedLiveRuntimeConfig::default(),
        )
        .expect("live runtime");
        bare_live
            .reconcile_pending()
            .await
            .expect("startup reconciliation");
        bare.begin_hot_cold_handoff(
            "handoff-unparked",
            unparked.descriptor(),
            ChainId(1),
            overlap,
            chain[2].block.hash,
        )
        .await
        .expect("begin handoff");
        let failure = ColdHandoffFailure {
            processor: unparked.descriptor().clone(),
            handoff_id: "handoff-unparked".to_owned(),
            detail: "automatic cold backfill failed: injected source exhaustion".to_owned(),
        };
        let mut backfills = ColdBackfills::default();
        backfills.spawn("automatic-unparked", async move {
            Err(anyhow::Error::new(failure))
        });
        let processors: Vec<Arc<dyn Processor>> = vec![unparked.clone()];
        let summary = finish_cold_handoffs(&bare, &bare_live, &processors, &mut backfills)
            .await
            .expect("a failed cold backfill without a retained frame");
        assert_eq!(
            summary.failed,
            vec![unparked.descriptor().instance.to_string()]
        );
        assert_eq!(
            bare.hot_cold_handoff("handoff-unparked", unparked.descriptor())
                .await
                .expect("handoff")
                .expect("record")
                .state,
            HotColdHandoffState::Failed
        );
        assert_eq!(
            bare.processor_runtime_state(unparked.descriptor())
                .await
                .expect("state")
                .state,
            ProcessorRunState::Running
        );
        assert!(
            bare.live_lane_gap(unparked.descriptor())
                .await
                .expect("gap")
                .is_none()
        );
        let logged = logs.text();
        assert!(
            logged.contains("its cold range stays uncovered until the next start's backfill"),
            "{logged}"
        );
        assert!(!logged.contains("lane is parked"), "{logged}");
    }

    #[tokio::test]
    async fn a_compact_uniswap_node_names_its_routes_when_the_store_refuses_the_starter() {
        let temp = tempfile::tempdir().expect("temporary directory");
        let data_dir = temp.path().join("data");
        fs::create_dir_all(&data_dir).expect("data directory");
        let config_path = temp.path().join("leani.toml");
        fs::write(
            &config_path,
            format!(
                r#"config_version = 1
network = "ethereum-mainnet"
data_dir = "{}"

[finality]
checkpoint = "0x1111111111111111111111111111111111111111111111111111111111111111"
checkpoint_slot = 15000000
endpoints = ["https://ethereum-beacon-api.publicnode.com/"]

[uniswap]
markets = ["ETH/USDT"]
"#,
                data_dir.display()
            ),
        )
        .expect("write compact config");
        // An earlier release stored the starter processor with ETH/USDT
        // starting at the USDC/WETH pool's creation block.
        let mut markets =
            crate::uniswap_markets::resolve_markets(&["ETH/USDT".to_owned()]).expect("market");
        markets[0].start_block = 12_376_729;
        let earlier =
            crate::uniswap_markets::processor_config(&markets, "uniswap-observations", false)
                .expect("earlier starter");
        let earlier = ProcessorRegistry::standard()
            .instantiate(&earlier, 1)
            .expect("earlier processor");
        let config = Config::load(&config_path)
            .expect("load compact config")
            .validate()
            .expect("valid compact config");
        let store = leani_store_sqlite::SqliteStore::open(configured_store_config(
            config.get(),
            data_dir.join("leani.sqlite"),
        ))
        .await
        .expect("store");
        store
            .register_processor(earlier.descriptor())
            .await
            .expect("register the earlier starter");
        drop(store);

        let error = serve(&config_path, &ProcessorRegistry::standard())
            .await
            .expect_err("the store refuses the moved start block");
        let message = format!("{error:#}");
        assert!(
            message.ends_with(
                "processor instance uniswap-observations conflicts with its stored descriptor"
            ),
            "{message}"
        );
        assert!(message.contains("a new `data_dir`"), "{message}");
        assert!(message.contains("a new `instance`"), "{message}");
    }

    #[tokio::test]
    async fn the_windowed_profile_materializes_and_compacts_its_window() {
        // Final review U5: keyed evm-events output is ordered, so the
        // profile's processor refused materialization jobs and a rerun of
        // the quickstart backfill, and its coverage never compacted.
        use leani_api::BackfillControl as _;

        let repository = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let config = Config::load(&repository.join("config/modes/windowed.toml"))
            .expect("windowed profile")
            .validate()
            .expect("valid windowed profile")
            .into_inner();
        let processors = ProcessorRegistry::standard()
            .instantiate_all(&config)
            .expect("windowed processors");
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = leani_store_sqlite::SqliteStore::open(leani_store_sqlite::StoreConfig::new(
            directory.path().join("node.sqlite"),
        ))
        .await
        .expect("store");
        assert_eq!(
            maintained_processors(&config, &processors).len(),
            processors.len(),
            "a profile processor's coverage is never compacted"
        );
        for (processor, configured) in processors.iter().zip(&config.processors) {
            store
                .register_processor(processor.descriptor())
                .await
                .expect("register");
            store
                .compact_finalized_coverage(
                    processor.descriptor(),
                    leani_primitives::BlockNumber(configured.start_block),
                    configured.coverage.verification_segment_blocks,
                    1,
                )
                .await
                .expect("finalized coverage compacts");
            // The quickstart backfill runs again over its own range.
            require_ordered_backfill_start(
                &store,
                processor.as_ref(),
                configured,
                configured.start_block + 500,
            )
            .await
            .expect("a backfill reruns from any block");
        }
        let control = NativeBackfillControl::new(
            config.clone(),
            store.clone(),
            processors.clone(),
            CancellationToken::new(),
            None,
            None,
            leani_runtime::HistoricalPipelineBudget::new(1, 1, 1_024).expect("pipeline budget"),
        );
        for configured in &config.processors {
            let accepted = control
                .create_materialization(leani_api::CreateMaterializationRequest {
                    processor: configured.instance.clone(),
                    from_block: Some(configured.start_block),
                    to_block: Some((configured.start_block + 999).into()),
                    ranges: Vec::new(),
                    mode: leani_api::BackfillExecutionMode::FillMissing,
                    idempotency_key: "quickstart".to_owned(),
                })
                .await;
            // The job is valid; it waits only for a finalized head.
            assert!(
                matches!(&accepted, Err(leani_api::BackfillControlError::Unavailable(message))
                    if message.contains("finalized head")),
                "{accepted:?}"
            );
        }
    }

    #[test]
    fn the_container_profile_backfills_its_processor_on_demand() {
        // Final review ruling on report concern 5: the profile kept automatic
        // history with live following disabled, so nothing indexed its
        // processor, and `leani backfill` refused it as
        // `automatic_job_owns_history`.
        let repository = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let config = Config::load(&repository.join("deploy/container.toml"))
            .expect("container profile")
            .validate()
            .expect("valid container profile")
            .into_inner();
        // The runbook's command, after the image's entrypoint.
        let cli = Cli::try_parse_from([
            "leani",
            "backfill",
            "--config",
            "/etc/leani/node.toml",
            "--processor",
            "blobs-money",
            "--from-block",
            "19426589",
            "--to-block",
            "19427588",
        ])
        .expect("the documented backfill command parses");
        assert_eq!(
            cli.config.as_deref(),
            Some(Path::new("/etc/leani/node.toml"))
        );
        let Command::Backfill {
            processor: Some(processor),
            from_block: from,
            to_block: to,
            ..
        } = cli.command
        else {
            panic!("the documented command is a backfill with a processor");
        };
        let configured = backfill_processor_config(&config, &processor)
            .expect("backfill accepts the container profile's processor");
        assert_eq!(configured.instance, "blobs-container-1-5");
        assert!(configured.start_block <= from && from <= to);

        // History mode is node policy, not processor identity: the store
        // accepts the same instance either way.
        let registry = ProcessorRegistry::standard();
        let on_demand = registry
            .instantiate(configured, config.chain.chain_id)
            .expect("on-demand processor");
        let mut automatic = configured.clone();
        automatic.history_mode = crate::config::ProcessorHistoryMode::Automatic;
        let automatic = registry
            .instantiate(&automatic, config.chain.chain_id)
            .expect("automatic processor");
        assert_eq!(on_demand.descriptor(), automatic.descriptor());
    }

    #[test]
    fn p2p_probe_uses_an_ephemeral_identity_and_peer_store() {
        let mut config: Config =
            toml::from_str(crate::config::VALID_CONFIG_TOML).expect("configuration fixture");
        config.sources.live.listener_port = 30_303;
        config.sources.live.discovery_port = 30_303;
        config.sources.live.discv5_port = 30_304;
        let probe = p2p_probe_config(
            &config,
            &P2pProbeOptions {
                from_block: 25_000_000,
                to_block: 25_000_001,
                expected_tip: None,
                minimum_peers: 1,
                peer_wait_seconds: 1,
                request_timeout_seconds: 1,
                retries: 1,
                retry_backoff_seconds: 1,
                max_input_bytes: 1,
                report: None,
                output: None,
            },
        )
        .expect("probe configuration");
        // The probe never opens the node's peer store or identity, so it can
        // run beside a live node without its data-directory lock...
        assert_eq!(probe.peer_store_path, None);
        assert_eq!(probe.secret_key_path, None);
        // ...or contending for the node's fixed listener and discovery ports.
        assert_eq!(
            (probe.listener_port, probe.discovery_port, probe.discv5_port),
            (0, 0, 0)
        );
    }

    #[tokio::test]
    async fn mainnet_e2e_refuses_a_data_dir_another_process_holds() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let _node = crate::local_state::lock_runtime_directory(directory.path())
            .expect("a node holds the data directory");
        let error = mainnet_e2e(
            &directory.path().join("leani.toml"),
            MainnetE2eOptions {
                processor: "blobs-money".to_owned(),
                from_block: 1,
                data_dir: directory.path().to_path_buf(),
                resume: true,
                minimum_follow_blocks: 1,
                stable_seconds: 1,
                max_head_age_seconds: 1,
                timeout_seconds: 2,
                report: directory.path().join("report.json"),
            },
            &ProcessorRegistry::standard(),
        )
        .await
        .expect_err("a held data directory is refused");
        let message = format!("{error:#}");
        assert!(
            message.contains("already in use by another Leani process"),
            "{message}"
        );
    }

    /// Frames `0..=through` of one parent-linked fixture chain.
    fn fixture_chain(through: u64) -> Vec<leani_primitives::BlockFrame> {
        let mut parent = leani_primitives::BlockHash::ZERO;
        (0..=through)
            .map(|number| {
                let frame = fixture_frame(number, parent);
                parent = frame.block.hash;
                frame
            })
            .collect()
    }

    /// Write `frames` as a local normalized-frame archive and return its
    /// manifest, so backfills in tests never leave the machine.
    fn write_frame_archive(directory: &Path, frames: &[leani_primitives::BlockFrame]) -> PathBuf {
        let mut object = Vec::new();
        for frame in frames {
            serde_json::to_writer(&mut object, frame).expect("archive frame");
            object.push(b'\n');
        }
        fs::write(directory.join("frames.jsonl"), &object).expect("archive object");
        let capabilities = vec![
            leani_primitives::Capability::Transactions,
            leani_primitives::Capability::Receipts,
            leani_primitives::Capability::Logs,
        ];
        let manifest = leani_source_archive::ArchiveManifest {
            format_version: 1,
            id: "fixture-archive".to_owned(),
            chain_id: 1,
            schema_version: "normalized-frame-jsonl.v1".to_owned(),
            capabilities: capabilities.clone(),
            complete_capabilities: capabilities,
            finality: leani_source_api::FinalityModel::Finalized,
            objects: vec![leani_source_archive::ArchiveObject {
                from_block: frames.first().expect("archive frames").block.number.0,
                to_block: frames.last().expect("archive frames").block.number.0,
                path: "frames.jsonl".to_owned(),
                blake3: blake3::hash(&object).to_hex().to_string(),
                bytes: u64::try_from(object.len()).expect("archive object size"),
            }],
        };
        let path = directory.join("manifest.json");
        fs::write(
            &path,
            serde_json::to_vec(&manifest).expect("archive manifest"),
        )
        .expect("write archive manifest");
        path
    }

    #[test]
    fn a_relative_archive_manifest_is_beside_its_configuration_file() {
        // Review of Task 19: `manifest` resolved against the working
        // directory, while `data_dir` in the same file resolved against the
        // file's directory.
        let directory = tempfile::tempdir().expect("temporary directory");
        let node = directory.path().join("node");
        let archive = node.join("archives");
        fs::create_dir_all(&archive).expect("archive directory");
        let manifest = write_frame_archive(&archive, &fixture_chain(3));
        let config_path = node.join("leani.toml");
        fs::write(
            &config_path,
            crate::config::VALID_CONFIG_TOML.replace(
                "id = \"xatu\"\nkind = \"xatu\"",
                "id = \"fixture-archive\"\nkind = \"archive\"\nmanifest = \"./archives/manifest.json\"",
            ),
        )
        .expect("configuration");
        let config = Config::load(&config_path).expect("configuration loads");
        assert_eq!(
            config.sources.history[0].manifest.as_deref(),
            Some(manifest.as_path())
        );
        let sources = configured_rpc_history_sources(&config)
            .expect("the manifest beside the configuration opens");
        assert_eq!(sources.len(), 1);
    }

    /// The fixture configuration with `archive` as its only history source and
    /// one processor per `(kind, instance)`.
    fn archive_config(data_dir: &Path, archive: &Path, processors: &[(&str, &str)]) -> Config {
        let mut config: Config =
            toml::from_str(crate::config::VALID_CONFIG_TOML).expect("configuration fixture");
        config.data_dir = data_dir.to_path_buf();
        config.budgets.memory_bytes = 64 * 1_024 * 1_024;
        config.budgets.temporary_disk_bytes = 64 * 1_024 * 1_024;
        config.sources.history = vec![crate::config::HistorySourceConfig {
            id: "fixture-archive".to_owned(),
            kind: crate::config::HistorySourceKind::Archive,
            priority: 10,
            trust: crate::config::HistoryTrust::TrustedDataset,
            chunk_blocks: None,
            blobs_chunk_blocks: None,
            batch_rows: None,
            manifest: Some(archive.to_path_buf()),
            endpoint: None,
            allow_insecure_http: false,
        }];
        let template = config.processors[0].clone();
        config.processors = processors
            .iter()
            .map(|(kind, instance)| {
                let mut configured = template.clone();
                (*kind).clone_into(&mut configured.id);
                (*instance).clone_into(&mut configured.instance);
                configured
            })
            .collect();
        config
    }

    fn pipeline_budget(config: &Config) -> leani_runtime::HistoricalPipelineBudget {
        let pipeline = config.budgets.history_pipeline;
        leani_runtime::HistoricalPipelineBudget::new(
            pipeline.maximum_active_chunks,
            historical_map_task_capacity(config),
            pipeline.maximum_mapped_bytes.bytes(),
        )
        .expect("pipeline budget")
    }

    async fn finalized_head(
        store: &leani_store_sqlite::SqliteStore,
        frame: &leani_primitives::BlockFrame,
    ) {
        store
            .store_canonical_anchor(
                leani_primitives::ChainId(1),
                frame.block,
                leani_primitives::Finality::Finalized,
            )
            .await
            .expect("finalized head");
    }

    async fn wait_for_job_state(
        store: &leani_store_sqlite::SqliteStore,
        id: &str,
        state: leani_store_sqlite::JobState,
    ) {
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if store
                    .job(id)
                    .await
                    .expect("job")
                    .is_some_and(|record| record.state == state)
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("job {id} never reached {state:?}"));
    }

    /// The subscription's state is written after its job's, so a test that
    /// asserts it waits for it rather than for the job.
    async fn wait_for_subscription_state(
        control: &NativeBackfillControl,
        id: &str,
        state: leani_api::BackfillState,
    ) -> leani_api::BackfillStatus {
        use leani_api::BackfillControl as _;

        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let status = control.inspect(id).await.expect("subscription status");
                if status.state == state {
                    return status;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("subscription {id} never reached {state:?}"))
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn two_automatic_instances_of_one_kind_reach_live() {
        // Audit H19: the handoff and automatic job ids named the processor
        // kind, so a second instance of a kind collided with the first and
        // the network lanes failed at every start.
        use leani_primitives::{BlockNumber, BlockRange, ChainId};
        use leani_source_api::LiveStart;
        use leani_store_sqlite::{HotColdHandoffState, SqliteStore, StoreConfig};
        use leani_testkit::default_source_budget;

        let directory = tempfile::tempdir().expect("tempdir");
        let chain = fixture_chain(5);
        let manifest = write_frame_archive(directory.path(), &chain[1..=4]);
        let config = archive_config(
            directory.path(),
            &manifest,
            &[
                ("synthetic-counter", "counter-a"),
                ("synthetic-counter", "counter-b"),
            ],
        );
        let store = SqliteStore::open(StoreConfig::new(directory.path().join("node.sqlite")))
            .await
            .expect("store");
        let processors = [("counter-a", 0xa0), ("counter-b", 0xb0)]
            .into_iter()
            .map(|(instance, settings)| {
                Arc::new(BlockLocalCounter::default().with_instance(
                    leani_processor_api::ProcessorInstanceId::new(instance).expect("instance"),
                    leani_primitives::BlockHash::new([settings; 32]),
                )) as Arc<dyn Processor>
            })
            .collect::<Vec<_>>();
        // An earlier release named counter-a's unverified handoff by its kind.
        store
            .begin_hot_cold_handoff(
                "handoff-1-synthetic-counter-3-4",
                processors[0].descriptor(),
                ChainId(1),
                BlockRange::new(BlockNumber(3), BlockNumber(4)).expect("overlap"),
                chain[4].block.hash,
            )
            .await
            .expect("earlier handoff");
        let (summary, live) = hand_over_to_live(&config, &store, &processors, &chain).await;
        assert_eq!(summary.verified.len(), 2);
        assert!(summary.failed.is_empty(), "{:?}", summary.failed);
        for processor in &processors {
            assert_eq!(
                store
                    .latest_hot_cold_handoff(processor.descriptor())
                    .await
                    .expect("handoff")
                    .expect("handoff record")
                    .state,
                HotColdHandoffState::Verified
            );
        }
        // The kind-named handoff is superseded, not reused.
        assert_eq!(
            store
                .hot_cold_handoff(
                    "handoff-1-synthetic-counter-3-4",
                    processors[0].descriptor()
                )
                .await
                .expect("earlier handoff")
                .expect("earlier handoff record")
                .state,
            HotColdHandoffState::Failed
        );

        // Review 2 N5: an archive read over its memory budget ended the
        // reconciliation lane, and with it the network lanes.
        let mut starved = config.clone();
        starved.budgets.memory_bytes = 64;
        let starved_audit = CancellationToken::new();
        let starved_reconciliations = tokio::spawn({
            let store = store.clone();
            let processors = processors.clone();
            let audit = starved_audit.clone();
            async move { run_archive_reconciliations(&starved, &store, &processors, audit).await }
        });
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(
            !starved_reconciliations.is_finished(),
            "a budget failure ended the reconciliation lane"
        );
        starved_audit.cancel();
        starved_reconciliations
            .await
            .expect("archive reconciliation task")
            .expect("a budget failure leaves the lane waiting");

        // The archive audit reconciles both instances.
        reconcile_until_verified(&config, &store, &processors).await;

        // Both follow the live chain.
        live.run(
            LiveStart::Head,
            default_source_budget(),
            CancellationToken::new(),
        )
        .await
        .expect("live run");
        for processor in &processors {
            assert_eq!(
                store
                    .processor_cursor(processor.descriptor())
                    .await
                    .expect("cursor")
                    .expect("live cursor")
                    .block_number,
                BlockNumber(5),
                "{} did not follow live",
                processor.descriptor().instance
            );
        }
    }

    /// Backfill `processors` from `config`'s history over blocks 1 to 4 of
    /// `chain`, as a network-lane start does, and verify their hot/cold
    /// handoffs against a live lane that follows with block 5.
    async fn hand_over_to_live(
        config: &Config,
        store: &leani_store_sqlite::SqliteStore,
        processors: &[Arc<dyn Processor>],
        chain: &[leani_primitives::BlockFrame],
    ) -> (ColdHandoffSummary, leani_runtime::SharedLiveRuntime) {
        use leani_primitives::{BlockNumber, BlockRange};
        use leani_source_api::ChainEvent;
        use leani_testkit::{LiveStep, ScriptedLiveSource, fixture_source_descriptor};

        // The live lanes retain the overlap up to the finalized anchor.
        for frame in &chain[3..=4] {
            store.store_recent_frame(frame).await.expect("recent frame");
        }
        let anchor = chain[4].block;
        let history_anchor = leani_source_p2p::P2pHistoryAnchor {
            block: anchor,
            consensus: leani_primitives::ConsensusAnchor {
                finality: leani_primitives::Finality::Finalized,
                execution_block_hash: anchor.hash,
                beacon_slot: 1,
                beacon_block_root: [1; 32],
            },
        };
        // The P2P bridge is the last resort; the archive covers the range.
        let p2p =
            leani_source_p2p::RethP2pSource::mainnet(leani_source_p2p::RethP2pConfig::default())
                .expect("execution source");
        let cancellation = CancellationToken::new();
        let mut backfills = ColdBackfills::default();
        spawn_cold_backfills(
            config,
            store,
            processors,
            4,
            3,
            anchor.hash,
            p2p,
            history_anchor,
            None,
            pipeline_budget(config),
            &cancellation,
            &mut backfills,
        )
        .await
        .expect("each instance starts its own cold backfill");
        let live = leani_runtime::SharedLiveRuntime::new(
            store.clone(),
            Arc::new(ScriptedLiveSource::new(
                fixture_source_descriptor(
                    "two-instances-live",
                    BlockRange::new(BlockNumber(0), BlockNumber(5)).expect("range"),
                ),
                vec![LiveStep::Event(ChainEvent::Block(Box::new(
                    chain[5].clone(),
                )))],
            )),
            processors.to_vec(),
            leani_runtime::SharedLiveRuntimeConfig::default(),
        )
        .expect("live runtime");
        let summary = tokio::time::timeout(
            Duration::from_secs(30),
            finish_cold_handoffs(store, &live, processors, &mut backfills),
        )
        .await
        .expect("the handoffs finish")
        .expect("the handoffs verify");
        (summary, live)
    }

    /// Run the archive reconciliation lane until every processor has a
    /// verified reconciliation, failing if the lane stops first.
    async fn reconcile_until_verified(
        config: &Config,
        store: &leani_store_sqlite::SqliteStore,
        processors: &[Arc<dyn Processor>],
    ) {
        let audit = CancellationToken::new();
        let mut reconciliations = tokio::spawn({
            let config = config.clone();
            let store = store.clone();
            let processors = processors.to_vec();
            let audit = audit.clone();
            async move { run_archive_reconciliations(&config, &store, &processors, audit).await }
        });
        let reconciled = async {
            loop {
                let mut verified = 0;
                for processor in processors {
                    if store
                        .latest_verified_archive_reconciliation(processor.descriptor())
                        .await
                        .expect("archive reconciliation")
                        .is_some()
                    {
                        verified += 1;
                    }
                }
                if verified == processors.len() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        };
        tokio::select! {
            result = &mut reconciliations => {
                panic!("the archive reconciliation lane stopped: {result:?}");
            }
            reconciled = tokio::time::timeout(Duration::from_secs(30), reconciled) => {
                reconciled.expect("every processor reconciles");
            }
        }
        audit.cancel();
        reconciliations
            .await
            .expect("archive reconciliation task")
            .expect("archive reconciliation lane");
    }

    #[tokio::test]
    async fn archive_reconciliation_moves_past_a_source_that_refuses_the_request() {
        // Xatu refuses requests its descriptor admits, such as pre-Merge
        // blocks or receipts beyond blob transactions. The backfill moved on
        // to the archive, but reconciliation treated the refusal as fatal and
        // stopped every network lane.
        let directory = tempfile::tempdir().expect("tempdir");
        let chain = fixture_chain(5);
        let manifest = write_frame_archive(directory.path(), &chain[1..=4]);
        let mut config = archive_config(
            directory.path(),
            &manifest,
            &[("synthetic-counter", "counter-a")],
        );
        config.sources.history.insert(
            0,
            crate::config::HistorySourceConfig {
                id: "xatu".to_owned(),
                kind: crate::config::HistorySourceKind::Xatu,
                priority: 0,
                trust: crate::config::HistoryTrust::TrustedDataset,
                chunk_blocks: None,
                blobs_chunk_blocks: None,
                batch_rows: None,
                manifest: None,
                endpoint: None,
                allow_insecure_http: false,
            },
        );
        let store = leani_store_sqlite::SqliteStore::open(leani_store_sqlite::StoreConfig::new(
            directory.path().join("node.sqlite"),
        ))
        .await
        .expect("store");
        let processors = vec![Arc::new(
            BlockLocalCounter::default()
                .with_instance(
                    leani_processor_api::ProcessorInstanceId::new("counter-a").expect("instance"),
                    leani_primitives::BlockHash::new([0xa0; 32]),
                )
                .with_filtered_material(),
        ) as Arc<dyn Processor>];

        let (summary, _live) = hand_over_to_live(&config, &store, &processors, &chain).await;
        assert!(summary.failed.is_empty(), "{:?}", summary.failed);
        reconcile_until_verified(&config, &store, &processors).await;
    }

    #[tokio::test]
    async fn invalid_subscription_consumers_are_refused_before_their_stream_exists() {
        // Task 14 deferred: an invalid consumer ID, or a lease TTL of zero or
        // beyond an i64 of milliseconds, passed every check until the store
        // created the consumer, after the subscription's history stream
        // existed: 500, and the stream stayed.
        use leani_api::BackfillControl as _;

        let directory = tempfile::tempdir().expect("temporary directory");
        let chain = fixture_chain(10);
        let manifest = write_frame_archive(directory.path(), &chain[1..=10]);
        let processor: Arc<dyn Processor> = Arc::new(BlockLocalCounter::default().with_instance(
            leani_processor_api::ProcessorInstanceId::new("subscribed-counter").expect("instance"),
            leani_primitives::BlockHash::new([0x22; 32]),
        ));
        let instance = processor.descriptor().instance.to_string();
        let mut config = archive_config(
            directory.path(),
            &manifest,
            &[("synthetic-counter", instance.as_str())],
        );
        config.processors[0].history_mode = crate::config::ProcessorHistoryMode::OnDemand;
        config.processors[0].history_control =
            crate::config::ProcessorHistoryControl::ApplicationSubscriptions;
        let store = leani_store_sqlite::SqliteStore::open(leani_store_sqlite::StoreConfig::new(
            directory.path().join("node.sqlite"),
        ))
        .await
        .expect("store");
        finalized_head(&store, &chain[10]).await;
        let control = NativeBackfillControl::new(
            config,
            store.clone(),
            vec![processor.clone()],
            CancellationToken::new(),
            None,
            None,
            leani_runtime::HistoricalPipelineBudget::new(1, 1, 1_024).expect("pipeline budget"),
        );
        let streams = store
            .delivery_streams(processor.descriptor())
            .await
            .expect("streams")
            .len();
        for (key, consumer_id, lease_ttl_seconds, rule) in [
            ("unportable-consumer", "not portable!", 60, "consumer ID"),
            ("zero-lease", "destination", 0, "lease TTL"),
            ("huge-lease", "destination", u64::MAX, "lease TTL"),
        ] {
            let refused = control
                .create_subscription(leani_api::CreateBackfillRequest {
                    processor: instance.clone(),
                    from_block: Some(1),
                    to_block: Some(2.into()),
                    ranges: Vec::new(),
                    mode: leani_api::BackfillExecutionMode::FillMissing,
                    consumer: Some(leani_api::CreateBackfillConsumerRequest {
                        id: consumer_id.to_owned(),
                        role: leani_store_sqlite::ConsumerRole::Required,
                        lease_ttl_seconds,
                        credential: None,
                    }),
                    limits: None,
                    batching: None,
                    idempotency_key: key.to_owned(),
                })
                .await;
            assert!(
                matches!(
                    &refused,
                    Err(leani_api::BackfillControlError::Invalid(message))
                        if message.contains(rule)
                ),
                "{key}: {refused:?}"
            );
            assert_eq!(
                store
                    .delivery_streams(processor.descriptor())
                    .await
                    .expect("streams")
                    .len(),
                streams,
                "{key}: the refused subscription left its stream"
            );
        }
    }

    /// A block-local counter whose mapping takes a while, so its work is still
    /// in flight when a test acts on it.
    #[derive(Debug)]
    struct SlowCounter(BlockLocalCounter);

    #[async_trait::async_trait]
    impl Processor for SlowCounter {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn descriptor(&self) -> &leani_processor_api::ProcessorDescriptor {
            self.0.descriptor()
        }

        async fn map(
            &self,
            block: &leani_primitives::BlockFrame,
        ) -> Result<leani_processor_api::EncodedDelta, leani_processor_api::ProcessorError>
        {
            tokio::time::sleep(Duration::from_millis(300)).await;
            self.0.map(block).await
        }

        async fn reduce(
            &self,
            transaction: &mut dyn leani_processor_api::ReducerTransaction,
            cursor: &leani_primitives::ProcessorCursor,
            delta: &leani_processor_api::EncodedDelta,
        ) -> Result<leani_processor_api::DomainChanges, leani_processor_api::ProcessorError>
        {
            self.0.reduce(transaction, cursor, delta).await
        }
    }

    /// A block-local counter whose mapping panics.
    #[derive(Debug)]
    struct PanickingCounter(BlockLocalCounter);

    #[async_trait::async_trait]
    impl Processor for PanickingCounter {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn descriptor(&self) -> &leani_processor_api::ProcessorDescriptor {
            self.0.descriptor()
        }

        async fn map(
            &self,
            _block: &leani_primitives::BlockFrame,
        ) -> Result<leani_processor_api::EncodedDelta, leani_processor_api::ProcessorError>
        {
            panic!("the processor's mapping panicked")
        }

        async fn reduce(
            &self,
            transaction: &mut dyn leani_processor_api::ReducerTransaction,
            cursor: &leani_primitives::ProcessorCursor,
            delta: &leani_processor_api::EncodedDelta,
        ) -> Result<leani_processor_api::DomainChanges, leani_processor_api::ProcessorError>
        {
            self.0.reduce(transaction, cursor, delta).await
        }
    }

    /// An on-demand node over a local archive of blocks `1..=through`, with a
    /// finalized head at `through`.
    async fn on_demand_control(
        directory: &Path,
        processor: Arc<dyn Processor>,
        through: u64,
    ) -> (leani_store_sqlite::SqliteStore, Arc<NativeBackfillControl>) {
        on_demand_control_with(
            directory,
            processor,
            through,
            crate::config::ProcessorHistoryControl::NodeOwned,
        )
        .await
    }

    /// [`on_demand_control`] whose processor history is owned as given.
    async fn on_demand_control_with(
        directory: &Path,
        processor: Arc<dyn Processor>,
        through: u64,
        history_control: crate::config::ProcessorHistoryControl,
    ) -> (leani_store_sqlite::SqliteStore, Arc<NativeBackfillControl>) {
        let chain = fixture_chain(through);
        let manifest = write_frame_archive(directory, &chain[1..]);
        let instance = processor.descriptor().instance.to_string();
        let mut config = archive_config(
            directory,
            &manifest,
            &[("synthetic-counter", instance.as_str())],
        );
        config.processors[0].history_mode = crate::config::ProcessorHistoryMode::OnDemand;
        config.processors[0].history_control = history_control;
        let store = leani_store_sqlite::SqliteStore::open(leani_store_sqlite::StoreConfig::new(
            directory.join("node.sqlite"),
        ))
        .await
        .expect("store");
        finalized_head(
            &store,
            &chain[usize::try_from(through).expect("block index")],
        )
        .await;
        let budget = pipeline_budget(&config);
        let control = Arc::new(NativeBackfillControl::new(
            config,
            store.clone(),
            vec![processor],
            CancellationToken::new(),
            None,
            None,
            budget,
        ));
        (store, control)
    }

    fn materialization(instance: &str, key: &str) -> leani_api::CreateMaterializationRequest {
        leani_api::CreateMaterializationRequest {
            processor: instance.to_owned(),
            from_block: Some(1),
            to_block: Some(4.into()),
            ranges: Vec::new(),
            mode: leani_api::BackfillExecutionMode::FillMissing,
            idempotency_key: key.to_owned(),
        }
    }

    #[tokio::test]
    async fn deleting_a_cancelled_job_leaves_no_record_behind() {
        // Audit CLI-4: cancel marked the job cancelled at once, and a delete
        // right after it raced the task, whose own cancellation bookkeeping
        // then wrote the deleted job's outcome again.
        use leani_api::BackfillControl as _;

        let directory = tempfile::tempdir().expect("temporary directory");
        let processor: Arc<dyn Processor> = Arc::new(SlowCounter(BlockLocalCounter::default()));
        let instance = processor.descriptor().instance.to_string();
        let (store, control) = on_demand_control(directory.path(), processor, 4).await;
        let created = control
            .create_materialization(materialization(&instance, "cancel-then-delete"))
            .await
            .expect("materialization");
        tokio::time::sleep(Duration::from_millis(50)).await;
        control.cancel(&created.id).await.expect("cancel");
        control
            .delete(
                &created.id,
                leani_store_sqlite::UnacknowledgedDelivery::Protect,
            )
            .await
            .expect("delete");
        // The task's mapping takes 300 ms a block; by now it has ended.
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(
            store.job(&created.id).await.expect("job").is_none(),
            "the deleted job came back"
        );
        assert!(
            store
                .job(&format!("{}:outcome", created.id))
                .await
                .expect("outcome")
                .is_none(),
            "the deleted job's outcome came back"
        );
    }

    #[tokio::test]
    async fn a_panicking_historical_job_fails_and_frees_its_slot() {
        // Review of task 16b: a job's task freed its slot only when it
        // returned. A panic kept the slot, which counts toward
        // `source_concurrency`, and left the job running, for good.
        use leani_api::BackfillControl as _;

        let directory = tempfile::tempdir().expect("temporary directory");
        let processor: Arc<dyn Processor> =
            Arc::new(PanickingCounter(BlockLocalCounter::default()));
        let instance = processor.descriptor().instance.to_string();
        let (store, control) = on_demand_control(directory.path(), processor, 4).await;
        let created = control
            .create_materialization(materialization(&instance, "panics"))
            .await
            .expect("materialization");
        wait_for_job_state(&store, &created.id, leani_store_sqlite::JobState::Failed).await;
        tokio::time::timeout(Duration::from_secs(5), async {
            while !control.tasks.lock().await.is_empty() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the panicked job kept its slot");
    }

    /// Fails every mapping while `failing` is set.
    #[derive(Debug)]
    struct FailingCounter {
        inner: BlockLocalCounter,
        failing: std::sync::atomic::AtomicBool,
    }

    #[async_trait::async_trait]
    impl Processor for FailingCounter {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn descriptor(&self) -> &leani_processor_api::ProcessorDescriptor {
            self.inner.descriptor()
        }

        async fn map(
            &self,
            block: &leani_primitives::BlockFrame,
        ) -> Result<leani_processor_api::EncodedDelta, leani_processor_api::ProcessorError>
        {
            if self.failing.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(leani_processor_api::ProcessorError::Invariant(
                    "the processor's mapping failed".to_owned(),
                ));
            }
            self.inner.map(block).await
        }

        async fn reduce(
            &self,
            transaction: &mut dyn leani_processor_api::ReducerTransaction,
            cursor: &leani_primitives::ProcessorCursor,
            delta: &leani_processor_api::EncodedDelta,
        ) -> Result<leani_processor_api::DomainChanges, leani_processor_api::ProcessorError>
        {
            self.inner.reduce(transaction, cursor, delta).await
        }
    }

    #[tokio::test]
    async fn a_retried_subscription_runs_again_and_completes() {
        use leani_api::BackfillControl as _;

        let directory = tempfile::tempdir().expect("temporary directory");
        let processor = Arc::new(FailingCounter {
            // Subscription IDs name the instance, so it must be portable.
            inner: BlockLocalCounter::default().with_instance(
                leani_processor_api::ProcessorInstanceId::new("retried-counter").expect("instance"),
                leani_primitives::BlockHash::new([0x0c; 32]),
            ),
            failing: std::sync::atomic::AtomicBool::new(true),
        });
        let instance = processor.descriptor().instance.to_string();
        let (store, control) = on_demand_control_with(
            directory.path(),
            processor.clone(),
            4,
            crate::config::ProcessorHistoryControl::ApplicationSubscriptions,
        )
        .await;
        let created = control
            .create_subscription(leani_api::CreateBackfillRequest {
                processor: instance,
                from_block: Some(1),
                to_block: Some(4.into()),
                ranges: Vec::new(),
                mode: leani_api::BackfillExecutionMode::FillMissing,
                consumer: Some(leani_api::CreateBackfillConsumerRequest {
                    id: "destination".to_owned(),
                    role: leani_store_sqlite::ConsumerRole::Required,
                    lease_ttl_seconds: 60,
                    credential: None,
                }),
                limits: None,
                batching: None,
                idempotency_key: "retried".to_owned(),
            })
            .await
            .expect("subscription");
        let failed =
            wait_for_subscription_state(&control, &created.id, leani_api::BackfillState::Failed)
                .await;
        assert!(failed.last_error.is_some());

        processor
            .failing
            .store(false, std::sync::atomic::Ordering::SeqCst);
        let retried = control.retry(&created.id).await.expect("retry");
        assert_ne!(retried.state, leani_api::BackfillState::Failed);
        assert_eq!(retried.last_error, None);
        wait_for_job_state(&store, &created.id, leani_store_sqlite::JobState::Completed).await;
        let completed =
            wait_for_subscription_state(&control, &created.id, leani_api::BackfillState::Draining)
                .await;
        assert_eq!(completed.last_error, None);
        // Retrying a subscription that is no longer failed changes nothing.
        assert_eq!(
            control
                .retry(&created.id)
                .await
                .expect("retry an active subscription")
                .state,
            leani_api::BackfillState::Draining
        );
    }

    /// An on-demand node with a [`subscription`] that finished and drains:
    /// its consumer has acknowledged nothing, its completion included.
    async fn draining_subscription(
        directory: &Path,
    ) -> (
        leani_store_sqlite::SqliteStore,
        Arc<NativeBackfillControl>,
        Arc<dyn Processor>,
        String,
    ) {
        // Subscription IDs name the instance, so it must be portable.
        let processor: Arc<dyn Processor> = Arc::new(BlockLocalCounter::default().with_instance(
            leani_processor_api::ProcessorInstanceId::new("drained-counter").expect("instance"),
            leani_primitives::BlockHash::new([0x0d; 32]),
        ));
        draining_subscription_of(directory, processor).await
    }

    /// A [`draining_subscription`] of `processor`, whose instance name must be
    /// portable.
    async fn draining_subscription_of(
        directory: &Path,
        processor: Arc<dyn Processor>,
    ) -> (
        leani_store_sqlite::SqliteStore,
        Arc<NativeBackfillControl>,
        Arc<dyn Processor>,
        String,
    ) {
        use leani_api::BackfillControl as _;

        let instance = processor.descriptor().instance.to_string();
        let (store, control) = on_demand_control_with(
            directory,
            processor.clone(),
            4,
            crate::config::ProcessorHistoryControl::ApplicationSubscriptions,
        )
        .await;
        let created = control
            .create_subscription(subscription(&instance, "drained", 1, 4))
            .await
            .expect("subscription");
        wait_for_subscription_state(&control, &created.id, leani_api::BackfillState::Draining)
            .await;
        // Its task records the report after the last commit drains it.
        tokio::time::timeout(Duration::from_secs(5), async {
            while !control.tasks.lock().await.is_empty() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the finished job's task ends");
        (store, control, processor, created.id)
    }

    /// A control over `control`'s store on a node that no longer configures
    /// any processor instance, as after its operator replaced them.
    fn without_processors(control: &NativeBackfillControl) -> NativeBackfillControl {
        let mut config = (*control.config).clone();
        config.processors.clear();
        NativeBackfillControl::new(
            config,
            control.store.clone(),
            Vec::new(),
            CancellationToken::new(),
            None,
            None,
            control.pipeline_budget.clone(),
        )
    }

    /// A control over `control`'s store on a node that configures only
    /// `processor`, as after its operator replaced the instances before it.
    fn replaced_by(
        control: &NativeBackfillControl,
        processor: Arc<dyn Processor>,
    ) -> NativeBackfillControl {
        let mut config = (*control.config).clone();
        config.processors.truncate(1);
        config.processors[0].instance = processor.descriptor().instance.to_string();
        NativeBackfillControl::new(
            config,
            control.store.clone(),
            vec![processor],
            CancellationToken::new(),
            None,
            None,
            control.pipeline_budget.clone(),
        )
    }

    #[tokio::test]
    async fn cancelling_a_draining_subscription_lets_it_be_deleted() {
        // Integration feedback: cancel changed only a job that still ran. A
        // finished subscription drains until its consumer acknowledges the
        // completion, so it stayed draining, and deletion refused it.
        use leani_api::BackfillControl as _;

        let directory = tempfile::tempdir().expect("temporary directory");
        let (store, control, processor, id) = draining_subscription(directory.path()).await;
        let cancelled = control.cancel(&id).await.expect("cancel");
        assert_eq!(cancelled.state, leani_api::BackfillState::Cancelled);
        // The job keeps its completion and its report.
        assert!(cancelled.report.is_some());
        assert_eq!(
            store.job(&id).await.expect("job").expect("record").state,
            leani_store_sqlite::JobState::Completed
        );
        // Its consumer can still read every record the job committed.
        let records = store
            .consumer_changes_after_in_stream(
                processor.descriptor(),
                cancelled
                    .delivery_stream_id
                    .as_deref()
                    .expect("history stream"),
                leani_primitives::ChainId(1),
                "destination",
                0,
                100,
            )
            .await
            .expect("stream records");
        assert_eq!(
            records.last().map(|record| record.change.kind.as_str()),
            Some("system.backfill_complete")
        );
        // It has acknowledged none of them.
        let protected = control
            .delete(&id, leani_store_sqlite::UnacknowledgedDelivery::Protect)
            .await;
        assert!(
            matches!(protected, Err(leani_api::BackfillControlError::Conflict(_))),
            "{protected:?}"
        );
        control
            .delete(&id, leani_store_sqlite::UnacknowledgedDelivery::Discard)
            .await
            .expect("discarding delete");
        let deleted = control.inspect(&id).await;
        assert!(
            matches!(deleted, Err(leani_api::BackfillControlError::NotFound(_))),
            "{deleted:?}"
        );
    }

    #[tokio::test]
    async fn a_draining_subscription_of_a_removed_instance_can_be_cancelled_and_deleted() {
        // Integration feedback: once its processor instance was replaced, a
        // draining subscription's consumer could no longer read or
        // acknowledge its stream, so it drained, and held its records, for
        // good.
        use leani_api::BackfillControl as _;

        let directory = tempfile::tempdir().expect("temporary directory");
        let (_store, control, _processor, id) = draining_subscription(directory.path()).await;
        let removed = without_processors(&control);
        assert_eq!(
            removed.cancel(&id).await.expect("cancel").state,
            leani_api::BackfillState::Cancelled
        );
        let deletion = removed
            .delete(&id, leani_store_sqlite::UnacknowledgedDelivery::Discard)
            .await
            .expect("discarding delete");
        assert!(deletion.removed_delivery_records > 0, "{deletion:?}");
    }

    #[tokio::test]
    async fn failed_subscription_status_reports_its_durable_error() {
        use leani_api::BackfillControl as _;

        let directory = tempfile::tempdir().expect("temporary directory");
        let (store, control, _processor, id) = draining_subscription(directory.path()).await;
        // An acquisition outcome must not hide the subscription's later error.
        store
            .set_backfill_subscription_state(
                &id,
                leani_store_sqlite::BackfillSubscriptionState::Failed,
                Some("durable subscription failure"),
            )
            .await
            .expect("durable failure");

        let status = control.inspect(&id).await.expect("status");
        assert_eq!(status.state, leani_api::BackfillState::Failed);
        assert_eq!(
            status.last_error.as_deref(),
            Some("durable subscription failure")
        );
    }

    #[tokio::test]
    async fn retrying_a_failed_subscription_of_a_removed_instance_changes_nothing() {
        // Integration feedback: retry re-queued the subscription and cleared
        // its error before it found the processor instance missing, so the
        // refused retry still changed it. Review: this instance is named
        // after its kind, so its name still selected, as a kind, the instance
        // of that kind that replaced it, and the retried job ran under that.
        use leani_api::BackfillControl as _;

        let directory = tempfile::tempdir().expect("temporary directory");
        let processor = Arc::new(FailingCounter {
            inner: BlockLocalCounter::default().with_instance(
                leani_processor_api::ProcessorInstanceId::new("synthetic-counter")
                    .expect("instance"),
                leani_primitives::BlockHash::new([0x0f; 32]),
            ),
            failing: std::sync::atomic::AtomicBool::new(true),
        });
        assert_eq!(
            processor.descriptor().instance.as_str(),
            processor.descriptor().id.as_str()
        );
        let instance = processor.descriptor().instance.to_string();
        let (_store, control) = on_demand_control_with(
            directory.path(),
            processor,
            4,
            crate::config::ProcessorHistoryControl::ApplicationSubscriptions,
        )
        .await;
        let created = control
            .create_subscription(subscription(&instance, "failed", 1, 4))
            .await
            .expect("subscription");
        let failed =
            wait_for_subscription_state(&control, &created.id, leani_api::BackfillState::Failed)
                .await;
        assert!(failed.last_error.is_some());

        let replacement: Arc<dyn Processor> = Arc::new(BlockLocalCounter::default().with_instance(
            leani_processor_api::ProcessorInstanceId::new("replacement-counter").expect("instance"),
            leani_primitives::BlockHash::new([0x10; 32]),
        ));
        for removed in [
            replaced_by(&control, replacement),
            without_processors(&control),
        ] {
            let refused = removed.retry(&created.id).await;
            assert!(
                matches!(refused, Err(leani_api::BackfillControlError::Invalid(_))),
                "{refused:?}"
            );
            let unchanged = removed.inspect(&created.id).await.expect("status");
            assert_eq!(unchanged.state, leani_api::BackfillState::Failed);
            assert_eq!(unchanged.last_error, failed.last_error);
        }
    }

    #[tokio::test]
    async fn subscription_status_names_its_consumer_and_configured_processor() {
        // Integration feedback: an application told its own subscriptions
        // apart by parsing the consumer out of their job IDs.
        use leani_api::BackfillControl as _;

        let directory = tempfile::tempdir().expect("temporary directory");
        let (_store, control, _processor, id) = draining_subscription(directory.path()).await;
        let listed = control
            .list(Some(leani_api::HistoricalWorkOwner::Subscription))
            .await
            .expect("subscriptions");
        let [status] = listed.as_slice() else {
            panic!("one subscription: {listed:?}");
        };
        assert_eq!(status.id, id);
        assert_eq!(status.consumer.as_deref(), Some("destination"));
        assert!(status.processor_configured);
    }

    #[tokio::test]
    async fn status_of_a_removed_instance_reports_it_unconfigured() {
        // Integration feedback: only its stream's 404 told an application
        // that a subscription's processor instance was gone. This instance
        // is named after its kind: its name still selects, as a kind, the
        // instance of that kind that replaced it.
        use leani_api::BackfillControl as _;

        let directory = tempfile::tempdir().expect("temporary directory");
        let processor: Arc<dyn Processor> = Arc::new(BlockLocalCounter::default().with_instance(
            leani_processor_api::ProcessorInstanceId::new("synthetic-counter").expect("instance"),
            leani_primitives::BlockHash::new([0x0e; 32]),
        ));
        assert_eq!(
            processor.descriptor().instance.as_str(),
            processor.descriptor().id.as_str()
        );
        let (_store, control, _processor, id) =
            draining_subscription_of(directory.path(), processor).await;
        let replacement: Arc<dyn Processor> = Arc::new(BlockLocalCounter::default().with_instance(
            leani_processor_api::ProcessorInstanceId::new("replacement-counter").expect("instance"),
            leani_primitives::BlockHash::new([0x0f; 32]),
        ));
        for removed in [
            replaced_by(&control, replacement),
            without_processors(&control),
        ] {
            let status = removed.inspect(&id).await.expect("status");
            assert!(!status.processor_configured, "{status:?}");
            assert_eq!(status.consumer.as_deref(), Some("destination"));
        }
    }

    #[tokio::test]
    async fn a_materialization_job_status_names_no_consumer() {
        use leani_api::BackfillControl as _;

        let directory = tempfile::tempdir().expect("temporary directory");
        let processor: Arc<dyn Processor> = Arc::new(BlockLocalCounter::default());
        let instance = processor.descriptor().instance.to_string();
        let (store, control) = on_demand_control(directory.path(), processor, 4).await;
        let created = control
            .create_materialization(materialization(&instance, "no-consumer"))
            .await
            .expect("materialization");
        wait_for_job_state(&store, &created.id, leani_store_sqlite::JobState::Completed).await;
        let status = control.inspect(&created.id).await.expect("status");
        assert_eq!(status.consumer, None);
        assert!(status.processor_configured);
    }

    /// One connection to a backfill subscription's history stream over HTTP.
    struct HistoryStream {
        client: reqwest::Client,
        /// The URL of the consumer's routes.
        consumer: String,
        session: String,
        body: std::pin::Pin<Box<dyn futures::Stream<Item = reqwest::Result<bytes::Bytes>> + Send>>,
        buffered: Vec<u8>,
    }

    impl HistoryStream {
        /// Open the stream of the consumer at `consumer` and read its hello.
        async fn open(consumer: &str) -> Self {
            let client = reqwest::Client::builder()
                .no_proxy()
                .build()
                .expect("HTTP client");
            let response = client
                .get(format!("{consumer}/stream"))
                .send()
                .await
                .expect("stream response");
            assert_eq!(response.status(), reqwest::StatusCode::OK);
            let mut stream = Self {
                client,
                consumer: consumer.to_owned(),
                session: String::new(),
                body: Box::pin(response.bytes_stream()),
                buffered: Vec::new(),
            };
            let hello = stream.next().await.expect("hello");
            assert_eq!(hello["type"], "hello", "{hello}");
            hello["sessionToken"]
                .as_str()
                .expect("session token")
                .clone_into(&mut stream.session);
            stream
        }

        /// The next record, or `None` once the stream has ended.
        async fn next(&mut self) -> Option<serde_json::Value> {
            use futures::StreamExt as _;

            loop {
                if let Some(newline) = self.buffered.iter().position(|byte| *byte == b'\n') {
                    let line = self.buffered.drain(..=newline).collect::<Vec<_>>();
                    return Some(serde_json::from_slice(&line[..newline]).expect("NDJSON record"));
                }
                let chunk = tokio::time::timeout(Duration::from_secs(5), self.body.next())
                    .await
                    .expect("a record or the end of the stream in time")?;
                self.buffered
                    .extend_from_slice(&chunk.expect("stream bytes"));
            }
        }

        /// Acknowledge `cursor` with this stream's session token.
        async fn acknowledge(&self, cursor: &serde_json::Value) -> (reqwest::StatusCode, String) {
            let response = self
                .client
                .post(format!("{}/ack", self.consumer))
                .header("x-leani-consumer-session", &self.session)
                .json(&serde_json::json!({ "cursor": cursor }))
                .send()
                .await
                .expect("acknowledgement response");
            let status = response.status();
            (status, response.text().await.expect("acknowledgement body"))
        }
    }

    #[tokio::test]
    async fn reopening_a_completed_history_stream_resends_completion_and_ends() {
        // Integration feedback: opened again after its consumer acknowledged
        // the completion, a subscription's stream had nothing to deliver and
        // sent heartbeats for good, so the application inspected the
        // subscription on every heartbeat to learn that it was done.
        use leani_api::BackfillControl as _;

        let directory = tempfile::tempdir().expect("temporary directory");
        let (store, control, processor, id) = draining_subscription(directory.path()).await;
        let descriptor = processor.descriptor().clone();
        let api = leani_api::router_with_processors(
            store.clone(),
            vec![processor],
            Vec::new(),
            leani_api::ApiConfig {
                backfill_control: Some(control.clone()),
                // A stream that sends heartbeats instead of ending shows
                // within the bounded reads.
                heartbeat_interval: Duration::from_millis(50),
                ..leani_api::ApiConfig::default()
            },
        )
        .expect("router");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let consumer = format!(
            "http://{}/v1/backfill-subscriptions/{id}/consumers/destination",
            listener.local_addr().expect("listener address")
        );
        let server = tokio::spawn(async move {
            axum::serve(listener, api).await.expect("serve");
        });

        let mut first = HistoryStream::open(&consumer).await;
        let completion = loop {
            let record = first.next().await.expect("the completion");
            if record["type"] == "backfill_complete" {
                break record;
            }
            assert_eq!(record["type"], "batch", "{record}");
        };
        let (status, body) = first.acknowledge(&completion["cursor"]).await;
        assert_eq!(status, reqwest::StatusCode::OK, "{body}");
        // It ends once the acknowledgement lands, with heartbeats until then.
        while let Some(record) = first.next().await {
            assert_eq!(record["type"], "heartbeat", "{record}");
        }
        let completed = control.inspect(&id).await.expect("status");
        assert_eq!(
            completed.state,
            leani_api::BackfillState::CompleteReclaimable
        );

        // Opened again, it sends the same completion and ends the same way.
        let mut reopened = HistoryStream::open(&consumer).await;
        assert_eq!(reopened.next().await, Some(completion.clone()));
        assert_eq!(reopened.next().await, None);
        // Its session ended with it, and acknowledging the completion again
        // still succeeds.
        let stream_id = completed.delivery_stream_id.expect("history stream");
        tokio::time::timeout(Duration::from_secs(5), async {
            while store
                .consumer_in_stream(&descriptor, &stream_id, "destination")
                .await
                .expect("consumer")
                .expect("registered consumer")
                .lease_active
            {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the ended stream releases its session");
        let (status, body) = reopened.acknowledge(&completion["cursor"]).await;
        assert_eq!(status, reqwest::StatusCode::OK, "{body}");
        server.abort();
    }

    fn durable_job(
        processor: &dyn Processor,
        id: &str,
        instance: &str,
        state: leani_store_sqlite::JobState,
        updated_at_unix_ms: u64,
    ) -> leani_store_sqlite::JobRecord {
        let mut job = leani_runtime::BackfillJob::for_processor(
            id,
            processor,
            leani_primitives::ChainId(1),
            leani_primitives::BlockRange::new(
                leani_primitives::BlockNumber(1),
                leani_primitives::BlockNumber(4),
            )
            .expect("range"),
            leani_source_api::VerificationPolicy::TrustedDataset,
        )
        .expect("job");
        instance.clone_into(&mut job.processor_instance);
        leani_store_sqlite::JobRecord {
            id: id.to_owned(),
            kind: job.owner.job_kind().to_owned(),
            state,
            payload: serde_json::to_vec(&job).expect("job payload"),
            checkpoint: None,
            attempts: 0,
            updated_at_unix_ms,
        }
    }

    #[tokio::test]
    async fn one_bad_durable_job_does_not_stop_resumption() {
        // Audit M-N6: the first job the scheduler could not decode or resolve
        // ended the whole pass, so no job after it ever resumed.
        let directory = tempfile::tempdir().expect("temporary directory");
        let processor: Arc<dyn Processor> = Arc::new(BlockLocalCounter::default());
        let instance = processor.descriptor().instance.to_string();
        let (store, control) = on_demand_control(directory.path(), processor.clone(), 4).await;
        let valid = format!("materialization:{instance}:valid");
        let unreadable = |id: &str, state, updated_at_unix_ms| leani_store_sqlite::JobRecord {
            payload: b"not a durable job".to_vec(),
            ..durable_job(processor.as_ref(), id, &instance, state, updated_at_unix_ms)
        };
        for record in [
            unreadable(
                "materialization:finished-unreadable",
                leani_store_sqlite::JobState::Completed,
                1,
            ),
            unreadable(
                "materialization:queued-unreadable",
                leani_store_sqlite::JobState::Queued,
                2,
            ),
            durable_job(
                processor.as_ref(),
                "materialization:removed-processor",
                "removed-instance",
                leani_store_sqlite::JobState::Queued,
                3,
            ),
            durable_job(
                processor.as_ref(),
                &valid,
                &instance,
                leani_store_sqlite::JobState::Queued,
                4,
            ),
        ] {
            store.save_job(&record).await.expect("durable job");
        }

        control
            .resume_durable_jobs()
            .await
            .expect("one job never fails the whole pass");
        wait_for_job_state(&store, &valid, leani_store_sqlite::JobState::Completed).await;
        assert_eq!(
            store
                .job("materialization:removed-processor")
                .await
                .expect("job")
                .expect("record")
                .state,
            leani_store_sqlite::JobState::Queued
        );
    }

    #[tokio::test]
    async fn the_durable_job_scheduler_reloads_only_after_a_change() {
        // Audit M-N6: the scheduler reloaded and decoded every job every
        // second, whether anything had changed or not.
        use leani_api::BackfillControl as _;

        let directory = tempfile::tempdir().expect("temporary directory");
        let processor: Arc<dyn Processor> = Arc::new(BlockLocalCounter::default());
        let instance = processor.descriptor().instance.to_string();
        let (store, control) = on_demand_control(directory.path(), processor.clone(), 4).await;
        let supervisor = tokio::spawn(control.clone().supervise_durable_jobs());
        tokio::time::sleep(Duration::from_millis(200)).await;

        // A job nothing announced: no change the scheduler knows of.
        let unannounced = format!("materialization:{instance}:unannounced");
        store
            .save_job(&durable_job(
                processor.as_ref(),
                &unannounced,
                &instance,
                leani_store_sqlite::JobState::Queued,
                1,
            ))
            .await
            .expect("durable job");
        tokio::time::sleep(Duration::from_millis(2_500)).await;
        assert_eq!(
            store
                .job(&unannounced)
                .await
                .expect("job")
                .expect("record")
                .state,
            leani_store_sqlite::JobState::Queued,
            "the scheduler reloaded its jobs without a change"
        );

        // A job the node runs: when it ends, the scheduler looks again.
        control
            .create_materialization(materialization(&instance, "announced"))
            .await
            .expect("materialization");
        wait_for_job_state(
            &store,
            &unannounced,
            leani_store_sqlite::JobState::Completed,
        )
        .await;
        control.cancellation.cancel();
        supervisor.await.expect("scheduler stops");
    }

    #[test]
    fn a_job_waits_for_a_p2p_bridge_that_would_serve_it_until_its_anchor_covers_it() {
        // The bridge's fallback window starts at block 100.
        for (case, expected, retained_input, anchor, job_end, wait) in [
            ("not expected", false, false, None, 150, None),
            ("retained input only", true, true, None, 150, None),
            ("end below the window", true, false, None, 99, None),
            ("no anchor yet", true, false, None, 100, Some(100)),
            ("anchor < end", true, false, Some(149), 150, Some(150)),
            ("anchor == end", true, false, Some(150), 150, None),
            ("anchor > end", true, false, Some(151), 150, None),
        ] {
            assert_eq!(
                bridge_wait(expected, retained_input, anchor, 100, job_end),
                wait,
                "{case}"
            );
        }
    }

    /// [`on_demand_control_with`] for application subscriptions, on a node
    /// whose P2P live lane is to bring the on-demand P2P history bridge. The
    /// bridge falls back over the last `history_fallback_blocks` finalized
    /// blocks when given, else over every block from the processor's start.
    async fn bridge_expecting_control(
        directory: &Path,
        processor: Arc<dyn Processor>,
        through: u64,
        history_fallback_blocks: Option<u64>,
    ) -> Arc<NativeBackfillControl> {
        let (store, unbridged) = on_demand_control_with(
            directory,
            processor.clone(),
            through,
            crate::config::ProcessorHistoryControl::ApplicationSubscriptions,
        )
        .await;
        let mut config = Config::clone(&unbridged.config);
        config.sources.live.history_fallback_blocks = history_fallback_blocks;
        let budget = pipeline_budget(&config);
        Arc::new(
            NativeBackfillControl::new(
                config,
                store,
                vec![processor],
                CancellationToken::new(),
                None,
                None,
                budget,
            )
            .with_p2p_bridge_expected(true),
        )
    }

    /// A subscription to blocks `from..=to` of `instance`, for the required
    /// consumer `destination`.
    fn subscription(
        instance: &str,
        key: &str,
        from: u64,
        to: u64,
    ) -> leani_api::CreateBackfillRequest {
        leani_api::CreateBackfillRequest {
            processor: instance.to_owned(),
            from_block: Some(from),
            to_block: Some(to.into()),
            ranges: Vec::new(),
            mode: leani_api::BackfillExecutionMode::FillMissing,
            consumer: Some(leani_api::CreateBackfillConsumerRequest {
                id: "destination".to_owned(),
                role: leani_store_sqlite::ConsumerRole::Required,
                lease_ttl_seconds: 60,
                credential: None,
            }),
            limits: None,
            batching: None,
            idempotency_key: key.to_owned(),
        }
    }

    #[tokio::test]
    async fn a_recent_job_waits_for_the_p2p_bridge_instead_of_failing() {
        // Integration feedback: a node resumed a subscription over recent
        // blocks seconds before verified finality gave the P2P history bridge
        // its first anchor. A job's sources are fixed for its run, so it ran
        // on Xatu and eraE alone, which cannot serve recent blocks yet, used
        // up its attempts, and failed.
        use leani_api::BackfillControl as _;

        let logs = CapturedLogs::default();
        let _logs = tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_ansi(false)
                .with_writer({
                    let logs = logs.clone();
                    move || logs.clone()
                })
                .finish(),
        );
        let directory = tempfile::tempdir().expect("temporary directory");
        let processor: Arc<dyn Processor> = Arc::new(BlockLocalCounter::default().with_instance(
            leani_processor_api::ProcessorInstanceId::new("recent-counter").expect("instance"),
            leani_primitives::BlockHash::new([0x0d; 32]),
        ));
        let instance = processor.descriptor().instance.to_string();
        let control = bridge_expecting_control(directory.path(), processor, 4, None).await;
        let created = control
            .create_subscription(subscription(&instance, "recent", 1, 4))
            .await
            .expect("subscription");
        // Scheduler passes, as at a start before the bridge's first anchor.
        let rechecks = [
            control.resume_durable_jobs().await.expect("scheduler pass"),
            control.resume_durable_jobs().await.expect("scheduler pass"),
        ];
        let waiting = control
            .inspect(&created.id)
            .await
            .expect("subscription status");
        assert_eq!(waiting.state, leani_api::BackfillState::Queued);
        assert_eq!(waiting.attempts, 0);
        assert!(waiting.report.is_none(), "{:?}", waiting.report);
        assert_eq!(waiting.last_error, None);
        // No job change announces the bridge, so the scheduler looks again.
        assert_eq!(rechecks, [true, true]);
        let logged = logs.text();
        assert_eq!(
            logged
                .matches("historical job waits for the P2P history bridge")
                .count(),
            1,
            "{logged}"
        );

        // The job starts once the bridge's anchor covers its last block. The
        // archive serves it; the bridge is the last resort.
        let chain = fixture_chain(4);
        let execution_source =
            leani_source_p2p::RethP2pSource::mainnet(leani_source_p2p::RethP2pConfig::default())
                .expect("execution source");
        let anchor = |block: leani_primitives::BlockRef| leani_source_p2p::P2pHistoryAnchor {
            block,
            consensus: leani_primitives::ConsensusAnchor {
                finality: leani_primitives::Finality::Finalized,
                execution_block_hash: block.hash,
                beacon_slot: block.number.0,
                beacon_block_root: [1; 32],
            },
        };
        control
            .update_p2p_bridge(execution_source.clone(), anchor(chain[3].block))
            .await;
        assert!(control.resume_durable_jobs().await.expect("scheduler pass"));
        assert_eq!(
            control
                .inspect(&created.id)
                .await
                .expect("subscription status")
                .state,
            leani_api::BackfillState::Queued,
            "an anchor short of the job's last block started it"
        );
        control
            .update_p2p_bridge(execution_source, anchor(chain[4].block))
            .await;
        control.resume_durable_jobs().await.expect("scheduler pass");
        wait_for_subscription_state(&control, &created.id, leani_api::BackfillState::Draining)
            .await;
    }

    #[tokio::test]
    async fn an_old_job_does_not_wait_for_the_p2p_bridge() {
        // A job that ends below the bridge's fallback window never gets the
        // bridge, so it starts on the other sources at once, as before.
        use leani_api::BackfillControl as _;

        let directory = tempfile::tempdir().expect("temporary directory");
        let processor: Arc<dyn Processor> = Arc::new(BlockLocalCounter::default().with_instance(
            leani_processor_api::ProcessorInstanceId::new("old-counter").expect("instance"),
            leani_primitives::BlockHash::new([0x0e; 32]),
        ));
        let instance = processor.descriptor().instance.to_string();
        // Before the bridge's first anchor, its window is the last two blocks
        // through the finalized head: 3 and 4.
        let control = bridge_expecting_control(directory.path(), processor, 4, Some(2)).await;
        let old = control
            .create_subscription(subscription(&instance, "old", 1, 2))
            .await
            .expect("old subscription");
        let recent = control
            .create_subscription(subscription(&instance, "recent", 3, 4))
            .await
            .expect("recent subscription");
        assert_eq!(
            recent.state,
            leani_api::BackfillState::Queued,
            "a job in the window did not wait"
        );
        wait_for_subscription_state(&control, &old.id, leani_api::BackfillState::Draining).await;
    }

    /// Move tokio's clock ahead by `duration` at once, and let it run on in
    /// real time. The store's SQLite connections answer on threads of their
    /// own, so a clock left paused runs ahead into their pool timeouts.
    async fn skip_ahead(duration: Duration) {
        tokio::time::pause();
        tokio::time::advance(duration).await;
        tokio::time::resume();
    }

    #[tokio::test]
    async fn a_job_held_for_the_p2p_bridge_starts_without_it_after_the_bound() {
        // A node whose network lanes never publish an anchor, such as one
        // without verified finality, must not hold the jobs in the window
        // for good while their status says nothing.
        use leani_api::BackfillControl as _;

        let logs = CapturedLogs::default();
        let _logs = tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_ansi(false)
                .with_writer({
                    let logs = logs.clone();
                    move || logs.clone()
                })
                .finish(),
        );
        let directory = tempfile::tempdir().expect("temporary directory");
        // Its mapping takes a while, so the job still runs at the next pass.
        let processor: Arc<dyn Processor> =
            Arc::new(SlowCounter(BlockLocalCounter::default().with_instance(
                leani_processor_api::ProcessorInstanceId::new("bounded-counter").expect("instance"),
                leani_primitives::BlockHash::new([0x0f; 32]),
            )));
        let instance = processor.descriptor().instance.to_string();
        let control = bridge_expecting_control(directory.path(), processor, 4, None).await;
        let created = control
            .create_subscription(subscription(&instance, "bounded", 1, 4))
            .await
            .expect("subscription");

        // No anchor comes. Short of the bound, the job still waits.
        let margin = Duration::from_secs(5);
        skip_ahead(P2P_BRIDGE_WAIT_LIMIT.saturating_sub(margin)).await;
        assert!(
            control.resume_durable_jobs().await.expect("scheduler pass"),
            "the job stopped waiting before the bound"
        );
        // At the bound, it starts without the bridge, as before the wait.
        skip_ahead(margin).await;
        assert!(
            !control.resume_durable_jobs().await.expect("scheduler pass"),
            "the job still waits past the bound"
        );
        // While it runs, a pass neither holds it again nor warns again.
        assert!(!control.resume_durable_jobs().await.expect("scheduler pass"));
        wait_for_subscription_state(&control, &created.id, leani_api::BackfillState::Draining)
            .await;
        let logged = logs.text();
        for message in [
            "historical job waits for the P2P history bridge",
            "historical job starts without the P2P history bridge",
        ] {
            assert_eq!(logged.matches(message).count(), 1, "{message}:\n{logged}");
        }
    }

    #[tokio::test]
    async fn a_panicking_network_lane_run_drops_readiness() {
        // Audit M-N3: a panic unwound out of the lane supervisor and left
        // readiness as the lanes had last set it.
        let handles = lane_handles();
        let supervisor = tokio::spawn({
            let handles = handles.clone();
            async move {
                let runs = handles.clone();
                supervise_lane_runs(&handles, move || {
                    let handles = runs.clone();
                    async move {
                        handles.readiness.set_live_ready(true);
                        handles.rpc_readiness.set_live_ready(true);
                        handles.readiness.set_finality_ready(true);
                        assert!(handles.readiness.is_ready());
                        panic!("a bug in the network lanes");
                    }
                })
                .await
            }
        });
        let joined = supervisor.await;
        assert!(
            joined.as_ref().is_err_and(tokio::task::JoinError::is_panic),
            "{joined:?}"
        );
        assert!(!handles.readiness.is_ready(), "readiness stayed up");
        assert!(
            !handles.rpc_readiness.live_ready(),
            "RPC readiness stayed up"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn the_network_lane_backoff_resets_after_a_healthy_run() {
        // Audit M-N3: the backoff doubled up to a minute and never came back
        // down, so a node that had run well for days waited a minute to
        // restart after its next failure.
        let handles = lane_handles();
        let starts = std::sync::Mutex::new(Vec::new());
        tokio::time::timeout(
            Duration::from_hours(1),
            supervise_lane_runs(&handles, || {
                let run = {
                    let mut starts = starts.lock().expect("run starts");
                    starts.push(tokio::time::Instant::now());
                    starts.len()
                };
                let cancellation = handles.cancellation.clone();
                async move {
                    match run {
                        1..=4 => {}
                        // A run that stays up for ten minutes, then fails.
                        5 => tokio::time::sleep(Duration::from_mins(10)).await,
                        _ => cancellation.cancel(),
                    }
                    Err(anyhow::anyhow!("execution peers unavailable"))
                }
            }),
        )
        .await
        .expect("the supervisor stops once cancelled");
        let starts = starts.into_inner().expect("run starts");
        let gaps = starts
            .windows(2)
            .map(|pair| pair[1] - pair[0])
            .collect::<Vec<_>>();
        assert_eq!(
            gaps,
            [
                Duration::from_secs(1),
                Duration::from_secs(2),
                Duration::from_secs(4),
                Duration::from_secs(8),
                // After the healthy run the backoff starts over.
                Duration::from_mins(10) + Duration::from_secs(1),
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_healthy_run_does_not_reset_the_count_of_one_finality_contradiction() {
        // The backoff reset leaves Task 12's count alone: a contradiction of
        // the same finalized block after a long run is still the same fault,
        // because finality has not moved.
        let handles = lane_handles();
        let runs = std::sync::atomic::AtomicUsize::new(0);
        let end = tokio::time::timeout(
            Duration::from_hours(1),
            supervise_lane_runs(&handles, || {
                let run = runs.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                async move {
                    if run == 2 {
                        tokio::time::sleep(Duration::from_mins(10)).await;
                    }
                    Err(unfinalized_contradiction(2, 0xf2))
                }
            }),
        )
        .await
        .expect("the persistent contradiction halts");
        assert_eq!(end, NetworkLanesEnd::Halted);
        assert_eq!(runs.load(std::sync::atomic::Ordering::SeqCst), 3);
        assert_eq!(
            handles.network_telemetry.snapshot().supervisor.state,
            leani_source_api::NetworkSupervisorState::Stopped
        );
    }

    #[tokio::test]
    async fn embedded_subscriptions_prune_their_delivery_log() {
        // Audit M-N4: the embedded runtime never pruned its delivery log, so a
        // window-mode subscription paused for good once the log filled.
        use leani_primitives::{ChainId, ProcessorCursor};

        let directory = tempfile::tempdir().expect("temporary directory");
        let counter = BlockLocalCounter::default().with_delivery_max_bytes(150);
        let mut lifecycle = counter.descriptor().lifecycle.clone();
        lifecycle.delivery.pruning.interval_seconds = 1;
        lifecycle.delivery.pruning.minimum_batch_blocks = 1;
        lifecycle.delivery.pruning.minimum_batch_changes = 1;
        lifecycle.delivery.pruning.retain_finalized_blocks = 1;
        let processor: Arc<dyn Processor> = Arc::new(counter.with_lifecycle(lifecycle));
        let mut config: Config =
            toml::from_str(crate::config::VALID_CONFIG_TOML).expect("configuration fixture");
        config.data_dir = directory.path().to_path_buf();
        config.processors[0].instance = processor.descriptor().instance.to_string();
        // Refused at once on loopback, so the lanes keep restarting without
        // leaving the machine while the store is maintained.
        config.finality.endpoints =
            vec![url::Url::parse("http://127.0.0.1:1/").expect("finality endpoint")];
        let store = leani_store_sqlite::SqliteStore::open(leani_store_sqlite::StoreConfig::new(
            directory.path().join("leani.sqlite"),
        ))
        .await
        .expect("store");
        let mut paused = false;
        for frame in fixture_chain(20).into_iter().skip(1) {
            let delta = processor.map(&frame).await.expect("map");
            let applied = store
                .apply(
                    processor.as_ref(),
                    ProcessorCursor {
                        processor_id: processor.descriptor().id.to_string(),
                        processor_version: processor.descriptor().version.to_string(),
                        chain_id: ChainId(1),
                        block_number: frame.block.number,
                        block_hash: frame.block.hash,
                        finality: frame.finality,
                        sequence: frame.block.number.0,
                    },
                    &delta,
                    &[],
                )
                .await;
            if matches!(
                applied,
                Err(leani_store_sqlite::StoreError::DeliveryLimit { .. })
            ) {
                paused = true;
                break;
            }
            applied.expect("apply");
        }
        assert!(paused, "the delivery log never filled");
        assert_eq!(
            store
                .processor_runtime_state(processor.descriptor())
                .await
                .expect("state")
                .state,
            leani_store_sqlite::ProcessorRunState::Paused
        );

        let runtime = spawn_embedded_network_runtime(
            config,
            store.clone(),
            vec![processor.clone()],
            crate::local_state::lock_runtime_directory(directory.path())
                .expect("runtime directory lock"),
            leani_finality_beacon_api::CheckpointOrigin::Operator,
        )
        .expect("embedded runtime");
        let resumed = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if store
                    .processor_runtime_state(processor.descriptor())
                    .await
                    .expect("state")
                    .state
                    == leani_store_sqlite::ProcessorRunState::Running
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await;
        runtime.shutdown().await;
        resumed.expect("pruning the delivery log resumes the subscription's processor");
    }

    #[tokio::test]
    async fn no_cold_backfill_outlives_its_network_lane_run() {
        // Audit M-N1: an early `?` in a network-lane run dropped the handles
        // of the cold backfills it had spawned, which kept running into the
        // next run's startup reconciliation.
        let node = CancellationToken::new();
        let stopped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let run = with_cold_backfills(&node, async |lane_cancellation, backfills| {
            let lane_cancellation = lane_cancellation.clone();
            let stopped = stopped.clone();
            backfills.spawn("automatic-stopping", async move {
                lane_cancellation.cancelled().await;
                // Stopping takes a moment: the backfill saves its checkpoint.
                tokio::time::sleep(Duration::from_millis(50)).await;
                stopped.store(true, std::sync::atomic::Ordering::SeqCst);
                Err(anyhow::anyhow!(
                    "automatic cold backfill suspended for node shutdown"
                ))
            });
            Err(anyhow::anyhow!(
                "an early error after the backfills started"
            ))
        });
        let result = tokio::time::timeout(Duration::from_secs(5), run)
            .await
            .expect("the run ends");
        assert!(result.is_err(), "the run's own error is kept");
        assert!(
            stopped.load(std::sync::atomic::Ordering::SeqCst),
            "a cold backfill outlived its network-lane run"
        );
        assert!(
            !node.is_cancelled(),
            "only the run's own token was cancelled"
        );
    }

    #[tokio::test]
    async fn a_panicking_network_lane_run_still_joins_its_cold_backfills() {
        // Review of task 16b: a panic in a network-lane run skipped the join.
        // The drop guard cancelled the run's cold backfills, and dropping
        // their set aborted them wherever they were, without waiting.
        let stopped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let run = tokio::spawn({
            let stopped = stopped.clone();
            async move {
                with_cold_backfills(
                    &CancellationToken::new(),
                    async move |lane_cancellation, backfills| {
                        let lane_cancellation = lane_cancellation.clone();
                        backfills.spawn("automatic-stopping", async move {
                            lane_cancellation.cancelled().await;
                            // Stopping takes a moment: the backfill saves its
                            // checkpoint.
                            tokio::time::sleep(Duration::from_millis(50)).await;
                            stopped.store(true, std::sync::atomic::Ordering::SeqCst);
                            Err(anyhow::anyhow!(
                                "automatic cold backfill suspended for node shutdown"
                            ))
                        });
                        panic!("the network-lane run panicked after its backfills started")
                    },
                )
                .await
            }
        });
        let ended = tokio::time::timeout(Duration::from_secs(5), run)
            .await
            .expect("the run ends");
        assert!(
            ended.is_err_and(|error| error.is_panic()),
            "the panic reaches the supervisor"
        );
        assert!(
            stopped.load(std::sync::atomic::Ordering::SeqCst),
            "a cold backfill outlived its panicking network-lane run"
        );
    }

    /// Log output captured for a test's assertions.
    #[derive(Clone, Default)]
    struct CapturedLogs(Arc<std::sync::Mutex<Vec<u8>>>);

    impl CapturedLogs {
        fn text(&self) -> String {
            String::from_utf8_lossy(&self.0.lock().expect("captured logs")).into_owned()
        }
    }

    impl std::io::Write for CapturedLogs {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .expect("captured logs")
                .extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn the_network_lanes_name_a_cold_backfill_that_does_not_stop() {
        // Review of task 16b: after a network-lane run, the join waits for
        // every cold backfill without a bound, and one that ignored its
        // cancellation held up the next run without a word.
        let logs = CapturedLogs::default();
        let _logs = tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_ansi(false)
                .with_writer({
                    let logs = logs.clone();
                    move || logs.clone()
                })
                .finish(),
        );
        let node = CancellationToken::new();
        let release = CancellationToken::new();
        let run = with_cold_backfills(&node, async |_, backfills| {
            let release = release.clone();
            backfills.spawn("automatic:1:stubborn:1-4", async move {
                release.cancelled().await;
                Err(anyhow::anyhow!("stopped at last"))
            });
            Ok(())
        });
        tokio::pin!(run);
        assert!(
            tokio::time::timeout(Duration::from_secs(25), &mut run)
                .await
                .is_err(),
            "the join waits for the backfill"
        );
        let logged = logs.text();
        assert!(logged.contains("automatic:1:stubborn:1-4"), "{logged}");
        release.cancel();
        run.await.expect("the run's own result");
    }

    #[tokio::test]
    async fn a_cold_backfill_started_before_a_setup_error_stays_joinable() {
        // Audit M-N1: when a later processor's setup failed, the backfills
        // already started were dropped with the list that held them.
        let directory = tempfile::tempdir().expect("tempdir");
        let chain = fixture_chain(4);
        let manifest = write_frame_archive(directory.path(), &chain[1..=4]);
        let counter: Arc<dyn Processor> = Arc::new(BlockLocalCounter::default());
        // A retained-only ledger has no raw store. The bridge cannot override this policy.
        let ledger: Arc<dyn Processor> = Arc::new(leani_testkit::OrderedLedgerProcessor::named(
            "header-ledger",
        ));
        let mut config = archive_config(
            directory.path(),
            &manifest,
            &[
                ("synthetic-counter", counter.descriptor().instance.as_str()),
                ("header-ledger", ledger.descriptor().instance.as_str()),
            ],
        );
        config.processors[1].require_retained_input = true;
        let store = leani_store_sqlite::SqliteStore::open(leani_store_sqlite::StoreConfig::new(
            directory.path().join("node.sqlite"),
        ))
        .await
        .expect("store");
        let anchor = chain[4].block;
        let lanes = CancellationToken::new();
        let mut backfills = ColdBackfills::default();
        let error = spawn_cold_backfills(
            &config,
            &store,
            &[counter, ledger],
            4,
            3,
            anchor.hash,
            leani_source_p2p::RethP2pSource::mainnet(leani_source_p2p::RethP2pConfig::default())
                .expect("execution source"),
            leani_source_p2p::P2pHistoryAnchor {
                block: anchor,
                consensus: leani_primitives::ConsensusAnchor {
                    finality: leani_primitives::Finality::Finalized,
                    execution_block_hash: anchor.hash,
                    beacon_slot: 1,
                    beacon_block_root: [1; 32],
                },
            },
            None,
            pipeline_budget(&config),
            &lanes,
            &mut backfills,
        )
        .await
        .expect_err("the ledger has no cold source");
        assert!(format!("{error:#}").contains("header-ledger"), "{error:#}");
        assert_eq!(backfills.len(), 1, "the counter's backfill was dropped");
        lanes.cancel();
        tokio::time::timeout(Duration::from_secs(10), async {
            while backfills.join_next().await.is_some() {}
        })
        .await
        .expect("the started backfill stops when its lanes are cancelled");
    }

    #[test]
    fn a_standalone_backfill_runs_with_the_configured_historical_pipeline() {
        // Audit CLI-3: `leani backfill` ran with built-in pipeline defaults
        // instead of the configured ones the node's own backfills use.
        let mut config: Config =
            toml::from_str(crate::config::VALID_CONFIG_TOML).expect("configuration fixture");
        let pipeline = &mut config.budgets.history_pipeline;
        pipeline.maximum_active_chunks = 3;
        pipeline.maximum_mapped_bytes = crate::config::HumanBytes::from_bytes(7 * 1_024 * 1_024);
        pipeline.commit.maximum_blocks = 11;
        pipeline.commit.maximum_changes = 13;
        pipeline.commit.maximum_encoded_bytes = crate::config::HumanBytes::from_bytes(17 * 1_024);
        pipeline.commit.maximum_delay = crate::config::HumanMilliseconds::from_milliseconds(19);
        pipeline.commit.target_writer_hold =
            crate::config::HumanMilliseconds::from_milliseconds(23);

        let runtime = historical_runtime_config(&config, 4);

        assert_eq!(
            runtime.mapper_concurrency,
            config.budgets.mapper_concurrency
        );
        assert_eq!(runtime.maximum_active_chunks, 3);
        assert_eq!(runtime.maximum_mapped_bytes, 7 * 1_024 * 1_024);
        assert_eq!(runtime.commit_maximum_blocks, 11);
        assert_eq!(runtime.commit_maximum_changes, 13);
        assert_eq!(runtime.commit_maximum_encoded_bytes, 17 * 1_024);
        assert_eq!(runtime.commit_maximum_delay, Duration::from_millis(19));
        assert_eq!(runtime.commit_target_writer_hold, Duration::from_millis(23));
        // Three attempts per history source on a gap, as the node grants.
        assert_eq!(runtime.max_attempts, 12);
    }

    #[tokio::test]
    async fn a_standalone_backfill_applies_the_configured_historical_pipeline() {
        // Review of task 16b: the test above checks the helper, not that
        // `leani backfill` runs with it. A configured pipeline too small for
        // one mapped delta fails the command only if the command applies it.
        let directory = tempfile::tempdir().expect("temporary directory");
        let chain = fixture_chain(4);
        let manifest = write_frame_archive(directory.path(), &chain[1..]);
        let mut config = archive_config(
            directory.path(),
            &manifest,
            &[("transaction-stats", "transfers")],
        );
        config.sources.live.kind = crate::config::LiveSourceKind::Disabled;
        config.finality.kind = crate::config::FinalitySourceKind::Disabled;
        let configured = &mut config.processors[0];
        "1.0.0".clone_into(&mut configured.version);
        configured.history_mode = crate::config::ProcessorHistoryMode::OnDemand;
        configured.settings = toml::from_str(
            "from = \"0x0000000000000000000000000000000000000001\"\n\
             to = \"0x0000000000000000000000000000000000000002\"",
        )
        .expect("processor settings");
        config.budgets.history_pipeline.maximum_mapped_bytes =
            crate::config::HumanBytes::from_bytes(1);
        let config_path = directory.path().join("leani.toml");
        std::fs::write(
            &config_path,
            toml::to_string(&config).expect("configuration"),
        )
        .expect("configuration file");

        let error = backfill::run(
            &config_path,
            Some("transfers"),
            1,
            4,
            None,
            None,
            &crate::processors::ProcessorRegistry::standard(),
        )
        .await
        .expect_err("no mapped delta fits the configured pipeline");
        assert!(
            matches!(
                error.downcast_ref::<leani_runtime::RuntimeError>(),
                Some(leani_runtime::RuntimeError::MappedDeltaBudget { limit: 1, .. })
            ),
            "{error:#}"
        );
    }

    #[tokio::test]
    async fn an_ordered_backfill_continues_where_its_history_ends() {
        // Review of task 24: the store moves an ordered processor's cursor to
        // every block it applies, so a range with a hole below it, or below
        // blocks already applied, reduced its history out of chain order.
        let directory = tempfile::tempdir().expect("temporary directory");
        let chain = fixture_chain(6);
        let manifest = write_frame_archive(directory.path(), &chain[1..]);
        let mut config = archive_config(
            directory.path(),
            &manifest,
            &[("transaction-stats", "transfers")],
        );
        config.sources.live.kind = crate::config::LiveSourceKind::Disabled;
        config.finality.kind = crate::config::FinalitySourceKind::Disabled;
        let configured = &mut config.processors[0];
        "1.0.0".clone_into(&mut configured.version);
        configured.history_mode = crate::config::ProcessorHistoryMode::OnDemand;
        configured.settings = toml::from_str(
            "from = \"0x0000000000000000000000000000000000000001\"\n\
             to = \"0x0000000000000000000000000000000000000002\"",
        )
        .expect("processor settings");
        let config_path = directory.path().join("leani.toml");
        std::fs::write(
            &config_path,
            toml::to_string(&config).expect("configuration"),
        )
        .expect("configuration file");

        // With nothing applied, history starts at the start block, 1.
        let error = backfill_after_release(&config_path, 2, 3)
            .await
            .expect_err("a range above the start block leaves a hole");
        assert!(
            error.to_string().contains("start this backfill at block 1"),
            "{error:#}"
        );
        backfill_after_release(&config_path, 1, 3)
            .await
            .expect("the range from the start block");
        // A hole above the applied blocks, a range below them, and a rerun
        // from the start all start somewhere other than block 4.
        for (from, to) in [(5, 6), (2, 4), (1, 6)] {
            let error = backfill_after_release(&config_path, from, to)
                .await
                .expect_err("a range that does not continue the applied blocks");
            assert!(
                error.to_string().contains("start this backfill at block 4"),
                "{from}..={to}: {error:#}"
            );
        }
        backfill_after_release(&config_path, 4, 6)
            .await
            .expect("the range that continues the applied blocks");
    }

    /// `leani backfill` of the `transfers` instance, retried while its data
    /// directory is still locked: as `local_state::after_release` says, a
    /// child process another test spawns keeps a just-released lock until
    /// it execs.
    async fn backfill_after_release(config_path: &Path, from: u64, to: u64) -> Result<Exit> {
        let registry = crate::processors::ProcessorRegistry::standard();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            match backfill::run(
                config_path,
                Some("transfers"),
                from,
                to,
                None,
                None,
                &registry,
            )
            .await
            {
                Err(error)
                    if format!("{error:#}").contains("already in use")
                        && std::time::Instant::now() < deadline =>
                {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                result => return result,
            }
        }
    }

    #[tokio::test]
    async fn a_second_shutdown_signal_exits_at_once() {
        // Audit M-N2: a second Ctrl-C was ignored while the graceful shutdown
        // waited.
        let (signal, signals) = tokio::sync::mpsc::unbounded_channel::<()>();
        let signals = futures::stream::unfold(signals, |mut signals| async move {
            signals.recv().await.map(|()| ((), signals))
        });
        let cancellation = CancellationToken::new();
        let (exited, mut exit_code) = tokio::sync::mpsc::unbounded_channel();
        let forwarder = tokio::spawn(forward_shutdown_signals(
            signals,
            cancellation.clone(),
            move |code| exited.send(code).expect("exit code"),
        ));

        signal.send(()).expect("first signal");
        tokio::time::timeout(Duration::from_secs(5), cancellation.cancelled())
            .await
            .expect("the first signal starts the shutdown");
        tokio::task::yield_now().await;
        assert!(exit_code.try_recv().is_err(), "the first signal exited");

        // Undeliverable once nothing listens for signals any more.
        let _ = signal.send(());
        let code = tokio::time::timeout(Duration::from_secs(5), exit_code.recv())
            .await
            .expect("the second signal exits at once");
        // 130, never 0: a forced exit is not a clean one.
        assert_eq!(code, Some(130), "the second signal did not exit");
        forwarder.await.expect("signal forwarder");
    }

    /// A background task that runs until `cancellation`.
    fn steady_task(cancellation: &CancellationToken) -> impl Future<Output = Result<()>> + use<> {
        let cancellation = cancellation.clone();
        async move {
            cancellation.cancelled().await;
            Ok(())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_shutdown_abandons_connections_still_open_at_its_deadline() {
        // Audit M-N2: the graceful shutdown had no deadline, so one
        // connection that never closed held the node up forever.
        let cancellation = CancellationToken::new();
        let mut background = BackgroundTasks::default();
        background.spawn("steady", steady_task(&cancellation));
        cancellation.cancel();
        let started = tokio::time::Instant::now();
        // The listeners never finish: a connection stays open.
        tokio::time::timeout(
            SHUTDOWN_DEADLINE + Duration::from_secs(1),
            serve_until_shutdown(std::future::pending(), &mut background, &cancellation),
        )
        .await
        .expect("the shutdown ends at its deadline")
        .expect("an abandoned connection fails nothing");
        assert!(started.elapsed() >= SHUTDOWN_DEADLINE);
    }

    #[tokio::test(start_paused = true)]
    async fn a_background_task_that_ends_shuts_the_node_down() {
        // Audit M-N3: background tasks were fire-and-forget, so one that
        // stopped or panicked went unnoticed while the node kept serving.
        let endings: [(&str, futures::future::BoxFuture<'static, Result<()>>); 3] = [
            ("stops", Box::pin(async { Ok(()) })),
            (
                "fails",
                Box::pin(async { Err(anyhow::anyhow!("the store went away")) }),
            ),
            (
                "panics",
                Box::pin(futures::future::lazy(|_| -> Result<()> {
                    panic!("a bug in a background task")
                })),
            ),
        ];
        for (name, ending) in endings {
            let cancellation = CancellationToken::new();
            let mut background = BackgroundTasks::default();
            background.spawn("steady", steady_task(&cancellation));
            background.spawn(name, ending);
            let error = tokio::time::timeout(
                Duration::from_mins(1),
                serve_until_shutdown(steady_task(&cancellation), &mut background, &cancellation),
            )
            .await
            .unwrap_or_else(|_| panic!("the node kept running after `{name}` ended"))
            .expect_err("a background task that ends is fatal");
            assert!(
                cancellation.is_cancelled(),
                "{name}: the node did not shut down"
            );
            assert!(
                format!("{error:#}").contains(&format!("background task `{name}` ended")),
                "{error:#}"
            );
        }

        // After the shutdown began, a task that fails is only logged.
        let cancellation = CancellationToken::new();
        let mut background = BackgroundTasks::default();
        background.spawn("stopping", {
            let cancellation = cancellation.clone();
            async move {
                cancellation.cancelled().await;
                Err(anyhow::anyhow!("interrupted by the shutdown"))
            }
        });
        cancellation.cancel();
        serve_until_shutdown(std::future::ready(Ok(())), &mut background, &cancellation)
            .await
            .expect("a failure during the shutdown is not fatal");
    }
    #[tokio::test]
    async fn cli_and_node_assemble_the_same_verified_bridge_even_without_static_sources() {
        use leani_primitives::{BlockNumber, BlockRange, ChainId, ConsensusAnchor, Finality};
        let directory = tempfile::tempdir().unwrap();
        let mut config: Config = toml::from_str(crate::config::VALID_CONFIG_TOML).unwrap();
        config.data_dir = directory.path().to_path_buf();
        config.sources.history.clear();
        config.processors = vec![crate::block_summaries::processor_config(
            "blocks-test",
            false,
        )];
        config.processors[0].start_block = 26_000_000;
        let processor = ProcessorRegistry::standard()
            .instantiate(&config.processors[0], 1)
            .unwrap();
        let block =
            leani_testkit::fixture_frame(26_000_010, leani_primitives::BlockHash::ZERO).block;
        let bridge = OnDemandP2pBridge {
            source: execution_p2p_source(
                &config,
                leani_source_api::NetworkTelemetry::default(),
                None,
            )
            .unwrap()
            .as_ref()
            .clone(),
            anchor: leani_source_p2p::P2pHistoryAnchor {
                block,
                consensus: ConsensusAnchor {
                    finality: Finality::Finalized,
                    execution_block_hash: block.hash,
                    beacon_slot: 1,
                    beacon_block_root: [1; 32],
                },
            },
        };
        let range = BlockRange::new(BlockNumber(26_000_001), BlockNumber(26_000_005)).unwrap();
        let (cli_sources, cli_policy) =
            history_sources_with_bridge(&config, processor.as_ref(), None, Some(&bridge), range)
                .unwrap();
        let store = leani_store_sqlite::SqliteStore::open(leani_store_sqlite::StoreConfig::new(
            directory.path().join("node.sqlite"),
        ))
        .await
        .unwrap();
        store
            .store_canonical_anchor(ChainId(1), block, Finality::Finalized)
            .await
            .unwrap();
        let control = NativeBackfillControl::new(
            config.clone(),
            store,
            vec![processor.clone()],
            CancellationToken::new(),
            None,
            None,
            pipeline_budget(&config),
        );
        control
            .update_p2p_bridge(bridge.source, bridge.anchor)
            .await;
        let (api_sources, api_policy) = control
            .history_sources(processor.as_ref(), range)
            .await
            .unwrap();
        assert_eq!(cli_policy, api_policy);
        assert_eq!(cli_sources.len(), 1);
        let request = leani_source_api::DataRequest {
            chain_id: ChainId(1),
            range,
            required: processor.descriptor().requirements[0].capabilities,
            allow_filtered: true,
            projection: leani_source_api::FieldProjection::default(),
            log_fields: leani_primitives::LogFieldSet::NONE,
            filters: leani_source_api::FilterSet::default(),
            minimum_finality: Finality::Finalized,
            verification_policy: cli_policy,
        };
        assert_eq!(cli_sources[0].descriptor(), api_sources[0].descriptor());
        assert_eq!(
            cli_sources[0].plan(&request).await.unwrap(),
            api_sources[0].plan(&request).await.unwrap()
        );
        // The API admits the same bridge-only job instead of refusing it.
        let created = leani_api::BackfillControl::create_materialization(
            &control,
            leani_api::CreateMaterializationRequest {
                processor: "blocks-test".to_owned(),
                from_block: Some(26_000_001),
                to_block: Some(26_000_005.into()),
                ranges: Vec::new(),
                mode: leani_api::BackfillExecutionMode::FillMissing,
                idempotency_key: "bridge-only-api-job".to_owned(),
            },
        )
        .await;
        assert!(created.is_ok(), "{created:?}");
    }

    #[tokio::test]
    async fn cli_remote_backfill_uses_the_real_authenticated_node_api() {
        let directory = tempfile::tempdir().unwrap();
        let processor: Arc<dyn Processor> = Arc::new(BlockLocalCounter::default());
        let instance = processor.descriptor().instance.to_string();
        let (store, control) = on_demand_control(directory.path(), processor.clone(), 4).await;
        let token = "backfill-test-token-at-least-16-chars";
        let api = leani_api::router_with_processors(
            store.clone(),
            vec![processor.clone()],
            Vec::new(),
            leani_api::ApiConfig {
                bearer_token: Some(Arc::from(token)),
                backfill_control: Some(control.clone()),
                ..leani_api::ApiConfig::default()
            },
        )
        .unwrap();
        let router = axum::Router::new().nest("/prefix", api);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = url::Url::parse(&format!(
            "http://{}/prefix/",
            listener.local_addr().unwrap()
        ))
        .unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let result = tokio::time::timeout(
            Duration::from_secs(15),
            backfill::run(
                Path::new("unused-remote-config"),
                Some(&instance),
                1,
                4,
                Some(&endpoint),
                Some(token),
                &ProcessorRegistry::standard(),
            ),
        )
        .await
        .unwrap();
        // A wrong prefix gets axum's empty 404; the error must still say so.
        let wrong = endpoint.join("../elsewhere/").unwrap();
        let error = backfill::run(
            Path::new("unused-remote-config"),
            Some(&instance),
            1,
            4,
            Some(&wrong),
            Some(token),
            &ProcessorRegistry::standard(),
        )
        .await
        .unwrap_err();
        server.abort();
        assert!(format!("{error:#}").contains("404"), "{error:#}");
        assert_eq!(result.unwrap(), Exit::Success);
        assert_eq!(
            store
                .processor_cursor(processor.descriptor())
                .await
                .unwrap()
                .unwrap()
                .block_number
                .0,
            4
        );
        control.cancellation.cancel();
    }
}
