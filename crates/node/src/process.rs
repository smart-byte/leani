//! Process-wide diagnostics, structured logging, cancellation, and exit policy.

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Exit {
    Success,
    InvalidConfiguration,
    Failure,
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

#[derive(Clone)]
struct NativeBackfillControl {
    config: Arc<Config>,
    store: leani_store_sqlite::SqliteStore,
    processors: Arc<Vec<Arc<dyn leani_processor_api::Processor>>>,
    cancellation: CancellationToken,
    tasks: Arc<tokio::sync::Mutex<BTreeMap<String, CancellationToken>>>,
    p2p_bridge: Arc<tokio::sync::RwLock<Option<OnDemandP2pBridge>>>,
    raw_history_store: Option<leani_store_history::HistoryStore>,
    material_coordinator: Option<leani_runtime::HistoricalMaterialCoordinator>,
    pipeline_budget: leani_runtime::HistoricalPipelineBudget,
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
            p2p_bridge: Arc::new(tokio::sync::RwLock::new(None)),
            raw_history_store,
            material_coordinator,
            pipeline_budget,
        }
    }

    fn historical_runtime_config(
        &self,
        max_attempts: u32,
    ) -> leani_runtime::HistoricalRuntimeConfig {
        let pipeline = self.config.budgets.history_pipeline;
        leani_runtime::HistoricalRuntimeConfig {
            mapper_concurrency: self.config.budgets.mapper_concurrency,
            maximum_active_chunks: pipeline.maximum_active_chunks,
            maximum_mapped_bytes: pipeline.maximum_mapped_bytes.bytes(),
            commit_maximum_blocks: pipeline.commit.maximum_blocks,
            commit_maximum_changes: pipeline.commit.maximum_changes,
            commit_maximum_encoded_bytes: pipeline.commit.maximum_encoded_bytes.bytes(),
            commit_maximum_delay: Duration::from_millis(
                pipeline.commit.maximum_delay.milliseconds(),
            ),
            commit_target_writer_hold: Duration::from_millis(
                pipeline.commit.target_writer_hold.milliseconds(),
            ),
            max_attempts,
            ..leani_runtime::HistoricalRuntimeConfig::default()
        }
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
        let (mut sources, verification_policy) =
            configured_history_sources(&self.config, processor, self.raw_history_store.as_ref())
                .map_err(|error| leani_api::BackfillControlError::Invalid(error.to_string()))?;
        let configured = config_for_processor_descriptor(&self.config, processor.descriptor())
            .map_err(|error| leani_api::BackfillControlError::Invalid(error.to_string()))?;
        if configured.require_retained_input {
            return Ok((sources, verification_policy));
        }
        let Some(bridge) = self.p2p_bridge.read().await.clone() else {
            return Ok((sources, verification_policy));
        };
        let through = bridge.anchor.block.number.0;
        let bridge_start = self
            .config
            .sources
            .live
            .history_fallback_start(through, configured.start_block);
        if requested.end().0 < bridge_start || requested.start().0 > through {
            return Ok((sources, verification_policy));
        }
        let available = leani_primitives::BlockRange::new(
            leani_primitives::BlockNumber(bridge_start),
            leani_primitives::BlockNumber(through),
        )
        .map_err(|error| leani_api::BackfillControlError::Internal(error.to_string()))?;
        let source = leani_source_p2p::RethP2pHistorySource::from_live_source(
            bridge.source,
            available,
            bridge.anchor,
        )
        .map_err(|error| leani_api::BackfillControlError::Internal(error.to_string()))?;
        sources.push(Arc::new(source));
        Ok((sources, verification_policy))
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
            last_error: outcome.and_then(|outcome| outcome.error.clone()),
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
        leani_source_api::SourceBudget {
            max_input_bytes: self.config.budgets.temporary_disk_bytes,
            max_frame_bytes: self.config.budgets.memory_bytes.min(32 * 1_024 * 1_024),
            max_frames: range.len(),
            max_buffered_frames: self
                .config
                .budgets
                .mapper_concurrency
                .max(self.config.budgets.source_concurrency),
            max_in_flight_requests: self.config.budgets.source_concurrency,
            temporary_disk_bytes: self.config.budgets.temporary_disk_bytes,
        }
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
        let max_attempts = u32::try_from(sources.len())
            .unwrap_or(u32::MAX)
            .saturating_mul(3)
            .max(leani_runtime::HistoricalRuntimeConfig::default().max_attempts);
        let runtime = leani_runtime::HistoricalRuntime::new_with_sources(
            self.store.clone(),
            sources,
            processor,
            self.historical_runtime_config(max_attempts),
        )
        .map_err(|error| leani_api::BackfillControlError::Invalid(error.to_string()))?
        .with_pipeline_budget(self.pipeline_budget.clone());
        let runtime = if let Some(coordinator) = &self.material_coordinator {
            runtime.with_material_coordinator(coordinator.clone())
        } else {
            runtime
        };
        let token = self.cancellation.child_token();
        tasks.insert(job.id.clone(), token.clone());
        drop(tasks);
        if job.owner == leani_runtime::HistoricalJobOwner::Subscription {
            self.store
                .set_backfill_subscription_state(
                    &job.id,
                    leani_store_sqlite::BackfillSubscriptionState::Running,
                    None,
                )
                .await
                .map_err(|error| leani_api::BackfillControlError::Internal(error.to_string()))?;
        }

        let budget = self.source_budget(job.request.range);
        let tasks = self.tasks.clone();
        let store = self.store.clone();
        let process_cancellation = self.cancellation.clone();
        let job_id = job.id.clone();
        let owner = job.owner;
        tokio::spawn(async move {
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
            tasks.lock().await.remove(&job_id);
        });
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    async fn resume_durable_jobs(&self) -> Result<(), leani_api::BackfillControlError> {
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
        for record in records {
            let job: leani_runtime::BackfillJob =
                serde_json::from_slice(&record.payload).map_err(|error| {
                    leani_api::BackfillControlError::Internal(format!(
                        "durable backfill {} has an invalid payload: {error}",
                        record.id
                    ))
                })?;
            let processor = self.processor(&job.processor_instance)?;
            let configured = config_for_processor_descriptor(&self.config, processor.descriptor())
                .map_err(|error| leani_api::BackfillControlError::Internal(error.to_string()))?;
            if job.owner == leani_runtime::HistoricalJobOwner::Materialization
                && configured.history_mode == crate::config::ProcessorHistoryMode::Automatic
            {
                // The hot/cold handoff owns this stable system job. It uses the
                // same durable materialization record, but must not also be
                // launched by the on-demand supervisor.
                continue;
            }
            if record.state == leani_store_sqlite::JobState::Completed {
                if let Some(mut subscription) = self
                    .store
                    .backfill_subscription_for_job(&record.id)
                    .await
                    .map_err(|error| leani_api::BackfillControlError::Internal(error.to_string()))?
                {
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
                            .map_err(|error| {
                                leani_api::BackfillControlError::Internal(error.to_string())
                            })?
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
                            .map_err(|error| {
                                leani_api::BackfillControlError::Internal(error.to_string())
                            })?
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
                }
                continue;
            }
            if !matches!(
                record.state,
                leani_store_sqlite::JobState::Queued
                    | leani_store_sqlite::JobState::Running
                    | leani_store_sqlite::JobState::StorageBackpressured
            ) {
                continue;
            }
            if record.state == leani_store_sqlite::JobState::StorageBackpressured
                && !self
                    .store
                    .storage_below_low_water()
                    .await
                    .map_err(|error| leani_api::BackfillControlError::Internal(error.to_string()))?
            {
                continue;
            }
            match self.spawn(job, processor).await {
                Ok(()) => {}
                Err(leani_api::BackfillControlError::Unavailable(message))
                    if message.contains("processor backfills are already active") =>
                {
                    break;
                }
                Err(error) => return Err(error),
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
                warn!(%error, "durable backfill scheduler pass failed");
            }
            tokio::select! {
                () = self.cancellation.cancelled() => return,
                () = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
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
        let (_, verification_policy) = configured_history_sources(
            &self.config,
            processor.as_ref(),
            self.raw_history_store.as_ref(),
        )
        .map_err(|error| leani_api::BackfillControlError::Invalid(error.to_string()))?;
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
            self.spawn(scheduled_job, processor).await?;
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
        if let Some(token) = self.tasks.lock().await.get(id).cloned() {
            token.cancel();
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
            if job.owner == leani_runtime::HistoricalJobOwner::Subscription {
                self.store
                    .set_backfill_subscription_state(
                        id,
                        leani_store_sqlite::BackfillSubscriptionState::Cancelled,
                        None,
                    )
                    .await
                    .map_err(|error| {
                        leani_api::BackfillControlError::Internal(error.to_string())
                    })?;
            }
        }
        let outcome = self.outcome(&record.id).await?;
        self.status(&record, outcome.as_ref()).await
    }

    async fn delete(
        &self,
        id: &str,
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
        let subscription = status.owner == leani_api::HistoricalWorkOwner::Subscription;
        let deleted = self
            .store
            .delete_terminal_historical_work(id, &Self::outcome_id(id), subscription)
            .await
            .map_err(|error| match error {
                leani_store_sqlite::StoreError::HistoricalWorkNotDeletable { .. } => {
                    leani_api::BackfillControlError::Conflict(error.to_string())
                }
                _ => leani_api::BackfillControlError::Internal(error.to_string()),
            })?;
        self.tasks.lock().await.remove(id);
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
        if matches!(
            request.retention,
            leani_store_history::RawHistoryRetention::Window { .. }
        ) {
            return Err(leani_api::RawHistoryControlError::Invalid(
                "finite raw-history jobs currently require full retention; rolling window ownership is a separate post-RH2 policy"
                    .to_owned(),
            ));
        }
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
}

impl From<Exit> for ExitCode {
    fn from(value: Exit) -> Self {
        match value {
            Exit::Success => Self::SUCCESS,
            Exit::InvalidConfiguration => Self::from(2),
            Exit::Failure => Self::FAILURE,
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
    let log_filter = if matches!(cli.command, Command::Subscribe { .. })
        && cli.log_filter == "info"
        && std::env::var_os("LEANI_LOG").is_none()
    {
        "error,leani=warn"
    } else {
        &cli.log_filter
    };
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
        } => {
            let processor = processor.unwrap_or_else(|| match protocol {
                SubscribeProtocol::Blocks => "block-summary".to_owned(),
                SubscribeProtocol::UniswapV3 => "uniswap-observations".to_owned(),
            });
            Box::pin(crate::subscribe::subscribe(
                crate::subscribe::SubscribeOptions {
                    protocol,
                    targets,
                    format,
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
                    requested_config,
                    working_directory,
                },
                registry,
            ))
            .await
        }
        Command::Reset { command } => match command {
            ResetCommand::All { yes } => {
                let reset_config =
                    (requested_config.is_some() || config_path.is_file()).then_some(config_path);
                crate::local_state::reset_all(&crate::local_state::ResetAllOptions {
                    confirmed: yes,
                    config_path: reset_config,
                    working_directory,
                })
            }
            ResetCommand::Subscription {
                protocol,
                targets,
                finality,
                yes,
            } => {
                crate::subscribe::reset_subscription(&crate::subscribe::ResetSubscriptionOptions {
                    protocol,
                    targets,
                    finality,
                    confirmed: yes,
                    requested_config,
                    working_directory,
                })
            }
        },
        Command::Backfill {
            processor,
            from,
            to,
        } => backfill(&config_path, &processor, from, to, registry).await,
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
    if let Some(path) = explicit {
        return path.to_owned();
    }
    for candidate in ["leani.toml", "config/example.toml"] {
        let path = working_directory.join(candidate);
        if path.is_file() {
            return path;
        }
    }
    working_directory.join("leani.toml")
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
    let actual = frames
        .iter()
        .map(leani_rpc::rpc_compatibility_snapshot)
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

async fn backfill(
    config_path: &Path,
    processor_id: &str,
    from: u64,
    to: u64,
    registry: &ProcessorRegistry,
) -> Result<Exit> {
    use leani_primitives::{BlockNumber, BlockRange, ChainId};
    use leani_runtime::{BackfillJob, HistoricalRuntime, HistoricalRuntimeConfig};
    use leani_source_api::SourceBudget;
    use leani_store_sqlite::SqliteStore;

    let config = Config::load(config_path)
        .with_context(|| format!("load configuration {}", config_path.display()))?
        .validate()
        .map_err(|errors| anyhow::anyhow!(errors))?
        .into_inner();
    let configured = select_processor_config(&config, processor_id)?;
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
    let processor = registry.instantiate(configured, config.chain.chain_id)?;
    let range = BlockRange::new(BlockNumber(from), BlockNumber(to))?;
    let (sources, verification_policy) =
        configured_history_sources(&config, processor.as_ref(), None)?;
    let source_ids = sources
        .iter()
        .map(|source| source.descriptor().id.to_string())
        .collect::<Vec<_>>();
    info!(
        processor = %processor.descriptor().instance,
        requested_from = from,
        requested_to = to,
        requested_blocks = range.len(),
        source_ids = ?source_ids,
        "starting processor historical backfill"
    );
    let store = SqliteStore::open(configured_store_config(
        &config,
        config.data_dir.join("leani.sqlite"),
    ))
    .await?;
    let runtime = HistoricalRuntime::new_with_sources(
        store.clone(),
        sources,
        processor.clone(),
        HistoricalRuntimeConfig {
            mapper_concurrency: config.budgets.mapper_concurrency,
            ..HistoricalRuntimeConfig::default()
        },
    )?;
    let job = BackfillJob::for_processor(
        format!("{processor_id}-{}-{from}-{to}", config.chain.chain_id),
        processor.as_ref(),
        ChainId(config.chain.chain_id),
        range,
        verification_policy,
    )?;
    let report = runtime
        .run(
            job,
            SourceBudget {
                max_input_bytes: config.budgets.temporary_disk_bytes,
                max_frame_bytes: config.budgets.memory_bytes.min(32 * 1_024 * 1_024),
                max_frames: range.len(),
                max_buffered_frames: config
                    .budgets
                    .mapper_concurrency
                    .max(config.budgets.source_concurrency),
                max_in_flight_requests: config.budgets.source_concurrency,
                temporary_disk_bytes: config.budgets.temporary_disk_bytes,
            },
            CancellationToken::new(),
        )
        .await?;
    if config.artifact_storage.backend == ArtifactStorageBackend::TieredSegments
        && processor.descriptor().lifecycle.artifacts.mode
            == leani_processor_api::ArtifactPolicyMode::Full
    {
        let compacted = NativeBackfillControl::flush_tiered_artifacts(
            &store,
            processor.descriptor(),
            &[range],
            config.artifact_storage.maximum_segments_per_cycle,
        )
        .await?;
        info!(
            processor_instance = %processor.descriptor().instance,
            segments = compacted.segments,
            artifacts = compacted.artifacts,
            logical_bytes = compacted.logical_bytes,
            reclaimed_inline_bytes = compacted.inline_payload_bytes_reclaimed,
            "flushed CLI backfill artifacts to segments"
        );
    }
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(Exit::Success)
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

    let store = SqliteStore::open(configured_store_config(&config, &database_path)).await?;
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
        };
        let live_source = execution_p2p_source(&config, handles.network_telemetry.clone())?;
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

#[allow(clippy::too_many_lines)]
pub(crate) fn configured_history_sources(
    config: &Config,
    processor: &dyn leani_processor_api::Processor,
    raw_history_store: Option<&leani_store_history::HistoryStore>,
) -> Result<(
    Vec<std::sync::Arc<dyn leani_source_api::HistorySource>>,
    leani_source_api::VerificationPolicy,
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
                std::sync::Arc::new(EraeSource::new(erae)?)
            }
            crate::config::HistorySourceKind::Parquet => continue,
        };
        let descriptor = source.descriptor();
        let supplies = descriptor.capabilities.contains_all(required)
            && (allow_filtered || descriptor.complete_capabilities.contains_all(required));
        if supplies {
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
        let first = requirements
            .first()
            .context("processor has no material requirements")?;
        let material = leani_store_history::RawHistoryMaterialProfile {
            allow_filtered,
            projection: leani_source_api::FieldProjection::default(),
            log_fields,
            filters: leani_source_api::FilterSet {
                scope: first.filter.clone(),
                senders: first.filter.senders.clone(),
                recipients: first.filter.recipients.clone(),
            },
        };
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
                .map(|config| config.with_log_fields(log_fields))
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
    if selected.is_empty() {
        bail!(
            "no implemented history source can satisfy processor {} capabilities {:?}",
            processor.descriptor().id,
            required
        );
    }
    Ok((selected, policy))
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
                sources.push(std::sync::Arc::new(EraeSource::new(erae)?));
            }
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
    let database_path = config.data_dir.join("leani.sqlite");
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
        DbCommand::Backup { destination } => {
            store.backup(&destination).await?;
            println!(
                "{}",
                serde_json::json!({
                    "ok": true,
                    "database": database_path.display().to_string(),
                    "backup": destination.display().to_string()
                })
            );
        }
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
    leani_source_p2p::RethP2pSource::mainnet(RethP2pConfig {
        minimum_peers: options.minimum_peers,
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
        listener_port: config.sources.live.listener_port,
        discovery_port: config.sources.live.discovery_port,
        discv5_port: config.sources.live.discv5_port,
        enable_discv5: config.sources.live.enable_discv5,
        nat: leani_source_p2p::parse_nat_resolver(&config.sources.live.nat)?,
        trusted_peers,
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
        peer_cache_path: Some(config.data_dir.join("execution-peers.json")),
        secret_key_path: Some(config.data_dir.join("execution-p2p-secret")),
        peer_cache_max_entries: config.sources.live.peer_cache_max_entries,
        peer_cache_flush_interval: Duration::from_secs(
            config.sources.live.peer_cache_flush_seconds,
        ),
        poll_interval: Duration::from_secs(2),
        max_reorg_depth: 64,
        network_telemetry: leani_source_api::NetworkTelemetry::default(),
    })
    .map_err(Into::into)
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
            let source = VerifiedBeaconApi::mainnet(beacon_config)?;
            let report = source.probe_root(checkpoint).await;
            (report.accepted, serde_json::to_string_pretty(&report)?)
        }
        crate::config::FinalitySourceKind::ConsensusP2p => {
            if !endpoint_overrides.is_empty() || minimum_agreement.is_some() {
                bail!(
                    "--endpoint and --minimum-agreement only apply to beacon_api finality probes"
                );
            }
            let source = VerifiedConsensusP2p::mainnet(consensus_p2p_config(&config.finality))?;
            if checkpoint_slot == 0 {
                bail!(
                    "consensus_p2p finality probe requires --checkpoint-slot or finality.checkpoint_slot"
                );
            }
            let report = source.probe_checkpoint(checkpoint, checkpoint_slot).await;
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
}

#[derive(Debug, Serialize)]
struct Listeners {
    native_api: String,
    rpc_http: String,
    rpc_ws: String,
}

fn doctor(path: &Path, json: bool, registry: &ProcessorRegistry) -> Result<Exit> {
    let config = Config::load(path)?;
    let mut errors = config.validation_errors();
    errors.extend(registry.validation_errors(&config));
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
    let assembly = registry.instantiate_all_with_extensions(config.get())?;
    let processors = assembly.processors;
    let query_extensions = assembly.query_extensions;
    let store = SqliteStore::open(configured_store_config(
        config.get(),
        config.get().data_dir.join("leani.sqlite"),
    ))
    .await?;
    for processor in &processors {
        // Configured consumers reference the processor's default delivery
        // stream. A new store has neither record until the processor is
        // registered, so bootstrap the immutable processor/stream identity
        // before restoring or creating its consumers. Runtime registration is
        // deliberately idempotent and will verify the same identity later.
        store.register_processor(processor.descriptor()).await?;
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
    let bearer_token = config
        .get()
        .api
        .bearer_token_env
        .as_deref()
        .map(|name| {
            std::env::var(name)
                .with_context(|| format!("read API bearer token from environment variable {name}"))
        })
        .transpose()?
        .map(Arc::<str>::from);
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
    let rpc_history_enabled = matches!(
        config.get().rpc.historical_mode,
        crate::config::HistoricalMode::OnDemand
    );
    let external_history_sources = if rpc_history_enabled || config.get().raw_history.enabled {
        configured_rpc_history_sources(config.get())?
    } else {
        Vec::new()
    };
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
            let source_set =
                leani_store_history::RawHistorySourceSet::new(external_history_sources.clone())
                    .map_err(anyhow::Error::msg)?;
            let runner = leani_store_history::RawHistoryRunner::new(
                raw_store.clone(),
                source_set,
                leani_source_api::SourceBudget {
                    max_input_bytes: raw.maximum_segment_logical_bytes.bytes(),
                    max_frame_bytes: raw.maximum_frame_logical_bytes.bytes(),
                    max_frames: raw.maximum_source_frames,
                    max_buffered_frames: raw.maximum_buffered_frames,
                    max_in_flight_requests: config.get().budgets.source_concurrency,
                    temporary_disk_bytes: config.get().budgets.temporary_disk_bytes,
                },
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
    let history_pipeline = config.get().budgets.history_pipeline;
    let pipeline_budget = leani_runtime::HistoricalPipelineBudget::new(
        history_pipeline.maximum_active_chunks,
        historical_map_task_capacity(config.get()),
        history_pipeline.maximum_mapped_bytes.bytes(),
    )
    .map_err(anyhow::Error::msg)?;
    let history_material = config.get().budgets.history_material;
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
    let backfill_control = Arc::new(NativeBackfillControl::new(
        config.get().clone(),
        store.clone(),
        processors.clone(),
        cancellation.clone(),
        raw_history_store,
        material_coordinator,
        pipeline_budget,
    ));
    let durable_job_supervisor = tokio::spawn(backfill_control.clone().supervise_durable_jobs());
    let raw_history_supervisor = raw_history_control
        .as_ref()
        .map(|control| tokio::spawn(control.clone().supervise_durable_jobs()));
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
            readiness: readiness.clone(),
            network_telemetry: network_telemetry.clone(),
            on_demand_processors,
            application_subscription_processors,
            backfill_control: Some(backfill_control.clone()),
            raw_history_control: raw_history_control
                .clone()
                .map(|control| control as Arc<dyn leani_api::RawHistoryControl>),
            ..NativeApiConfig::default()
        },
    )?;
    let rpc_config = leani_rpc::RpcConfig {
        chain_id: leani_primitives::ChainId(config.get().chain.chain_id),
        readiness: rpc_readiness.clone(),
        websocket_enabled: true,
        history,
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
    let signal_token = cancellation.clone();
    let signal = tokio::spawn(async move {
        match shutdown_signal().await {
            Ok(()) => {
                info!("shutdown signal received");
                signal_token.cancel();
            }
            Err(error) => warn!(%error, "failed to install shutdown signal handler"),
        }
    });
    let network_supervisor = if live_required && finality_required {
        let supervisor_config = config.get().clone();
        let supervisor_store = store.clone();
        let supervisor_processors = processors.clone();
        let supervisor_handles = NetworkLaneHandles {
            readiness: readiness.clone(),
            rpc_readiness: rpc_readiness.clone(),
            committed_events: committed_events.clone(),
            network_telemetry: network_telemetry.clone(),
            cancellation: cancellation.clone(),
            backfill_control: Some(backfill_control),
            verified_anchor: None,
        };
        Some(tokio::spawn(async move {
            Box::pin(supervise_network_lanes(
                supervisor_config,
                supervisor_store,
                supervisor_processors,
                supervisor_handles,
                None,
            ))
            .await;
        }))
    } else {
        None
    };
    let tiered_artifact_processors = processors
        .iter()
        .filter(|processor| {
            processor.descriptor().lifecycle.artifacts.mode
                == leani_processor_api::ArtifactPolicyMode::Full
        })
        .cloned()
        .collect::<Vec<_>>();
    let artifact_compaction_supervisor = if config.get().artifact_storage.backend
        == ArtifactStorageBackend::TieredSegments
        && !tiered_artifact_processors.is_empty()
    {
        let artifact_store = store.clone();
        let artifact_config = config.get().artifact_storage;
        let artifact_cancellation = cancellation.clone();
        Some(tokio::spawn(async move {
            supervise_artifact_compaction(
                artifact_store,
                tiered_artifact_processors,
                artifact_config,
                artifact_cancellation,
            )
            .await;
        }))
    } else {
        None
    };
    let processor_maintenance = processors
        .iter()
        .zip(&config.get().processors)
        .filter(|(processor, _)| {
            processor.descriptor().lifecycle.delivery.mode
                != leani_processor_api::DeliveryPolicyMode::None
                || processor.descriptor().mode == leani_processor_api::ReductionMode::BlockLocal
        })
        .map(|(processor, configured)| {
            let processor = processor.clone();
            let verification_segment_blocks = configured.coverage.verification_segment_blocks;
            let pruner_store = store.clone();
            let pruner_cancellation = cancellation.clone();
            tokio::spawn(async move {
                supervise_processor_maintenance(
                    pruner_store,
                    processor,
                    verification_segment_blocks,
                    pruner_cancellation,
                )
                .await;
            })
        })
        .collect::<Vec<_>>();

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
    let server_result =
        tokio::try_join!(api_server, rpc_server, rpc_websocket_server).context("API server failed");
    cancellation.cancel();
    if let Some(network_supervisor) = network_supervisor {
        let _ = network_supervisor.await;
    }
    if let Some(artifact_compaction_supervisor) = artifact_compaction_supervisor {
        let _ = artifact_compaction_supervisor.await;
    }
    for maintenance in processor_maintenance {
        let _ = maintenance.await;
    }
    let _ = durable_job_supervisor.await;
    if let Some(raw_history_supervisor) = raw_history_supervisor {
        let _ = raw_history_supervisor.await;
    }
    signal.abort();
    server_result?;
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

#[cfg(unix)]
async fn shutdown_signal() -> std::io::Result<()> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result,
        _ = terminate.recv() => Ok(()),
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() -> std::io::Result<()> {
    tokio::signal::ctrl_c().await
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
        if tokio::time::timeout(Duration::from_secs(1), &mut self.task)
            .await
            .is_err()
        {
            // Some networking internals finish their own cache flush and socket
            // teardown on a fixed timer. A CLI subscription must nevertheless
            // honor Ctrl-C promptly; aborting the supervisor after cancellation
            // is safe because SQLite commits and peer-cache writes are atomic.
            self.task.abort();
            let _ = self.task.await;
        }
    }
}

pub(crate) fn spawn_embedded_network_runtime(
    config: Config,
    store: leani_store_sqlite::SqliteStore,
    processors: Vec<std::sync::Arc<dyn leani_processor_api::Processor>>,
) -> Result<EmbeddedNetworkRuntime> {
    let readiness = leani_api::ReadinessHandle::new(true, true);
    let cancellation = CancellationToken::new();
    let (committed_events, _) = tokio::sync::broadcast::channel(1_024);
    let (verified_anchor, verified_anchor_updates) = tokio::sync::watch::channel(None);
    let network_telemetry = leani_source_api::NetworkTelemetry::default();
    let execution_source = execution_p2p_source(&config, network_telemetry.clone())?;
    let handles = NetworkLaneHandles {
        readiness: readiness.clone(),
        rpc_readiness: leani_rpc::RpcReadiness::default(),
        committed_events,
        network_telemetry,
        cancellation: cancellation.clone(),
        backfill_control: None,
        verified_anchor: Some(verified_anchor),
    };
    let task = tokio::spawn(supervise_network_lanes(
        config,
        store,
        processors,
        handles,
        Some(execution_source.clone()),
    ));
    Ok(EmbeddedNetworkRuntime {
        readiness,
        verified_anchor: verified_anchor_updates,
        execution_source,
        cancellation,
        task,
    })
}

pub(crate) fn execution_p2p_source(
    config: &Config,
    network_telemetry: leani_source_api::NetworkTelemetry,
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
        preferred_peers: config.sources.live.preferred_peers,
        max_outbound_peers: config.sources.live.max_outbound_peers,
        max_concurrent_dials: config.sources.live.max_concurrent_dials,
        listener_port: config.sources.live.listener_port,
        discovery_port: config.sources.live.discovery_port,
        discv5_port: config.sources.live.discv5_port,
        enable_discv5: config.sources.live.enable_discv5,
        nat,
        trusted_peers,
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
        peer_cache_path: Some(config.data_dir.join("execution-peers.json")),
        secret_key_path: Some(config.data_dir.join("execution-p2p-secret")),
        peer_cache_max_entries: config.sources.live.peer_cache_max_entries,
        peer_cache_flush_interval: std::time::Duration::from_secs(
            config.sources.live.peer_cache_flush_seconds,
        ),
        network_telemetry,
        ..leani_source_p2p::RethP2pConfig::default()
    };
    Ok(std::sync::Arc::new(
        leani_source_p2p::RethP2pSource::mainnet(p2p_config)?,
    ))
}

/// Resolve a recent finalized execution anchor through the configured,
/// independently verified consensus source.
pub(crate) async fn verified_p2p_history_anchor(
    config: &Config,
) -> Result<leani_source_p2p::P2pHistoryAnchor> {
    use leani_finality_beacon_api::{BeaconApiConfig, VerifiedBeaconApi, parse_checkpoint_root};
    use leani_finality_consensus_p2p::VerifiedConsensusP2p;
    use leani_primitives::{BlockHash, BlockNumber, BlockRef, ConsensusAnchor, Finality};

    if config.chain.chain_id != 1 {
        bail!("execution P2P historical fallback currently supports Ethereum mainnet only");
    }
    let checkpoint = parse_checkpoint_root(&config.finality.checkpoint)?;
    let selected = match config.finality.kind {
        crate::config::FinalitySourceKind::BeaconApi => {
            let mut finality = BeaconApiConfig::mainnet(config.finality.endpoints.clone());
            finality.minimum_agreement = config.finality.minimum_agreement;
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
            let source = VerifiedConsensusP2p::mainnet(consensus_p2p_config(&config.finality))?;
            let report = source
                .probe_checkpoint(checkpoint, config.finality.checkpoint_slot)
                .await;
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

async fn supervise_network_lanes(
    config: Config,
    store: leani_store_sqlite::SqliteStore,
    processors: Vec<std::sync::Arc<dyn leani_processor_api::Processor>>,
    handles: NetworkLaneHandles,
    live_source: Option<std::sync::Arc<leani_source_p2p::RethP2pSource>>,
) {
    let live_source = match live_source {
        Some(source) => source,
        None => match execution_p2p_source(&config, handles.network_telemetry.clone()) {
            Ok(source) => source,
            Err(error) => {
                handles
                    .network_telemetry
                    .supervisor_backoff(&error, std::time::Duration::from_mins(1));
                warn!(%error, "failed to construct persistent execution P2P source");
                return;
            }
        },
    };
    let mut retry = std::time::Duration::from_secs(1);
    while !handles.cancellation.is_cancelled() {
        handles.readiness.set_live_ready(false);
        handles.rpc_readiness.set_live_ready(false);
        handles.readiness.set_finality_ready(false);
        handles.network_telemetry.supervisor_running();
        let result = Box::pin(run_network_lanes_once(
            &config,
            store.clone(),
            processors.clone(),
            handles.clone(),
            live_source.clone(),
        ))
        .await;
        if handles.cancellation.is_cancelled() {
            break;
        }
        let failure = match result {
            Ok(()) => {
                let failure = "required network lane ended unexpectedly".to_owned();
                warn!("{failure}");
                failure
            }
            Err(error) => {
                let failure = format!("{error:#}");
                warn!(
                    error = %failure,
                    "required network lane failed closed"
                );
                failure
            }
        };
        handles.network_telemetry.supervisor_backoff(failure, retry);
        tokio::select! {
            () = handles.cancellation.cancelled() => break,
            () = tokio::time::sleep(retry) => {}
        }
        retry = retry
            .saturating_mul(2)
            .min(std::time::Duration::from_mins(1));
    }
    handles.readiness.set_live_ready(false);
    handles.rpc_readiness.set_live_ready(false);
    handles.readiness.set_finality_ready(false);
    handles.network_telemetry.supervisor_stopped();
    live_source.shutdown().await;
}

#[allow(clippy::too_many_lines)]
async fn run_network_lanes_once(
    config: &Config,
    store: leani_store_sqlite::SqliteStore,
    processors: Vec<std::sync::Arc<dyn leani_processor_api::Processor>>,
    handles: NetworkLaneHandles,
    live_source: std::sync::Arc<leani_source_p2p::RethP2pSource>,
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
    use leani_source_api::{ConsensusCheckpoint, FinalitySource, SourceBudget};
    use leani_source_p2p::P2pHistoryAnchor;

    let NetworkLaneHandles {
        readiness,
        rpc_readiness,
        committed_events,
        network_telemetry: _,
        cancellation,
        backfill_control,
        verified_anchor,
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
    let (finality_source, selected, bootstrap): (
        Arc<dyn FinalitySource>,
        VerifiedFinalityAnchor,
        VerifiedFinalityAnchor,
    ) = match config.finality.kind {
        crate::config::FinalitySourceKind::BeaconApi => {
            let mut beacon_config = BeaconApiConfig::mainnet(config.finality.endpoints.clone());
            beacon_config.minimum_agreement = config.finality.minimum_agreement;
            let source = Arc::new(VerifiedBeaconApi::mainnet(beacon_config)?);
            let probe = source.probe_root(checkpoint_root).await;
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
                .endpoints
                .iter()
                .filter(|endpoint| endpoint.verified)
                .find_map(|endpoint| endpoint.checkpoint_anchor)
                .context("accepted finality report omitted its checkpoint anchor")?;
            (source, selected, bootstrap)
        }
        crate::config::FinalitySourceKind::ConsensusP2p => {
            let source = Arc::new(VerifiedConsensusP2p::mainnet(consensus_p2p_config(
                &config.finality,
            ))?);
            let probe = source
                .probe_checkpoint(checkpoint_root, config.finality.checkpoint_slot)
                .await;
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
        "verified finality startup anchor resolved"
    );
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
    // independently verified finality probe completes. Starting a peer manager
    // at genesis can make current peers reject our obsolete ETH status and then
    // hold startup behind the full peer timeout. Retained stores still overlap
    // discovery with finality, but refresh their advertised head immediately.
    live_source.update_advertised_head(live_anchor).await;
    if retained_warmup_head.is_none() {
        spawn_execution_peer_warmup(
            live_source.clone(),
            live_anchor,
            cancellation.clone(),
            startup_started,
        );
    }
    store
        .store_canonical_anchor(chain_id, live_anchor, Finality::Finalized)
        .await?;
    if let Some(verified_anchor) = &verified_anchor {
        verified_anchor.send_replace(Some(leani_runtime::AppliedFinalityAnchor {
            block: live_anchor,
            beacon_slot: selected.beacon_slot,
            beacon_block_root: selected.beacon_block_root,
        }));
    }
    let checkpoint = ConsensusCheckpoint {
        beacon_slot: bootstrap.beacon_slot,
        beacon_block_root: bootstrap.beacon_block_root,
        execution_block_hash: bootstrap.execution_block_hash,
        obtained_at_unix_seconds: now,
        source: "configured weak-subjectivity checkpoint".to_owned(),
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
        },
    )?;
    if let Some(control) = &backfill_control {
        live_runtime = live_runtime.with_finalized_gap_recovery(control.clone());
    }
    let startup_reconciliation = live_runtime
        .reconcile_pending()
        .await
        .context("reconcile durable live deltas before opening network lanes")?;
    info!(
        ?startup_reconciliation,
        "startup pending-delta reconciliation completed"
    );
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
    let lane_cancellation = cancellation.child_token();
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
                    verified_anchor.send_replace(Some(applied));
                }
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
    let backfills = spawn_cold_backfills(
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
        &lane_cancellation,
    )
    .await?;
    let live_budget = SourceBudget {
        max_input_bytes: config.budgets.memory_bytes,
        max_frame_bytes: config.budgets.memory_bytes.min(32 * 1_024 * 1_024),
        max_frames: 64,
        max_buffered_frames: 64,
        max_in_flight_requests: config.budgets.source_concurrency,
        temporary_disk_bytes: config.budgets.temporary_disk_bytes,
    };
    let (live_ready, mut live_ready_updates) = tokio::sync::watch::channel(false);
    let handoff_runtime = live_runtime.clone();
    let live_start = retained_live_start(
        &store,
        chain_id,
        live_anchor,
        overlap_blocks,
        64,
        !backfills.is_empty(),
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
    let handoffs = async move {
        let mut records = Vec::with_capacity(backfills.len());
        for backfill in backfills {
            records.push(
                backfill
                    .await
                    .context("automatic cold backfill task panicked")??,
            );
        }
        let reconciliation = handoff_runtime
            .reconcile_pending()
            .await
            .context("drain ordered live deltas after hot/cold handoff")?;
        for (processor, report) in reconciliation.processors {
            if report.pending != 0 {
                bail!(
                    "processor {processor} retains {} pending deltas after verified handoff",
                    report.pending
                );
            }
        }
        Ok::<_, anyhow::Error>(records)
    };
    tokio::pin!(live);
    tokio::pin!(finality);
    tokio::pin!(p2p_bridge_updates);
    tokio::pin!(archive_reconciliations);
    tokio::pin!(handoffs);
    let mut handoffs_verified = false;
    let mut handoffs_finished = false;
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
            result = &mut handoffs, if !handoffs_verified => {
                handoffs_finished = true;
                match result {
                    Ok(records) => {
                        handoffs_verified = true;
                        info!(
                            handoffs = records.len(),
                            overlap_from,
                            overlap_to = selected.execution_block_number,
                            "all hot/cold handoffs verified"
                        );
                    }
                    Err(error) => break Err(error.context("hot/cold handoff failed closed")),
                }
            }
        }
    };
    lane_cancellation.cancel();
    if !handoffs_finished {
        let _ = handoffs.as_mut().await;
    }
    readiness.set_live_ready(false);
    rpc_readiness.set_live_ready(false);
    readiness.set_finality_ready(false);
    result
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
    use leani_source_api::{SourceBudget, SourceError};
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
            let budget = SourceBudget {
                max_input_bytes: config.budgets.temporary_disk_bytes,
                max_frame_bytes: config.budgets.memory_bytes.min(32 * 1_024 * 1_024),
                max_frames: range.len(),
                max_buffered_frames: config
                    .budgets
                    .mapper_concurrency
                    .max(config.budgets.source_concurrency),
                max_in_flight_requests: config.budgets.source_concurrency,
                temporary_disk_bytes: config.budgets.temporary_disk_bytes,
            };
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
) -> Result<Vec<tokio::task::JoinHandle<Result<leani_store_sqlite::HotColdHandoffRecord>>>> {
    use leani_primitives::{BlockNumber, BlockRange, ChainId};
    use leani_runtime::{BackfillJob, HistoricalRuntime, HistoricalRuntimeConfig};
    use leani_source_api::SourceBudget;

    let mut tasks = Vec::new();
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
                processor = %configured.id,
                "automatic processor backfill disabled; historical ranges are on demand"
            );
            continue;
        }
        if configured.start_block > through {
            continue;
        }
        let (mut source, verification_policy) =
            match configured_history_sources(config, processor.as_ref(), None) {
                Ok(source) => source,
                Err(error) => {
                    warn!(
                        processor = %processor.descriptor().id,
                        %error,
                        "no cold source can satisfy configured live processor"
                    );
                    continue;
                }
            };
        let bridge_start = config
            .sources
            .live
            .history_fallback_start(through, configured.start_block);
        let bridge_range = BlockRange::new(BlockNumber(bridge_start), BlockNumber(through))?;
        source.push(std::sync::Arc::new(
            leani_source_p2p::RethP2pHistorySource::from_live_source(
                p2p_source.clone(),
                bridge_range,
                history_anchor.clone(),
            )
            .with_context(|| format!("construct P2P history bridge for {}", configured.id))?,
        ));
        let range = match BlockRange::new(BlockNumber(configured.start_block), BlockNumber(through))
        {
            Ok(range) => range,
            Err(error) => {
                warn!(%error, "invalid automatic backfill range");
                continue;
            }
        };
        let overlap = BlockRange::new(
            BlockNumber(overlap_from.max(configured.start_block)),
            BlockNumber(through),
        )?;
        let handoff_id = format!(
            "handoff-{}-{}-{}-{}",
            config.chain.chain_id,
            configured.id,
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
        let history_pipeline = config.budgets.history_pipeline;
        let runtime = match HistoricalRuntime::new_with_sources(
            store.clone(),
            source,
            processor.clone(),
            HistoricalRuntimeConfig {
                mapper_concurrency: config.budgets.mapper_concurrency,
                maximum_active_chunks: history_pipeline.maximum_active_chunks,
                maximum_mapped_bytes: history_pipeline.maximum_mapped_bytes.bytes(),
                commit_maximum_blocks: history_pipeline.commit.maximum_blocks,
                commit_maximum_changes: history_pipeline.commit.maximum_changes,
                commit_maximum_encoded_bytes: history_pipeline.commit.maximum_encoded_bytes.bytes(),
                commit_maximum_delay: Duration::from_millis(
                    history_pipeline.commit.maximum_delay.milliseconds(),
                ),
                commit_target_writer_hold: Duration::from_millis(
                    history_pipeline.commit.target_writer_hold.milliseconds(),
                ),
                ..HistoricalRuntimeConfig::default()
            },
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
                warn!(%error, "failed to construct automatic backfill");
                continue;
            }
        };
        let runtime = if let Some(startup_permit) = startup_permit {
            runtime.with_material_startup_permit(startup_permit)
        } else {
            runtime
        };
        let job = match BackfillJob::for_processor(
            format!(
                "serve-{}-{}-{}-{through}",
                config.chain.chain_id, configured.id, configured.start_block
            ),
            processor.as_ref(),
            ChainId(config.chain.chain_id),
            range,
            verification_policy,
        ) {
            Ok(job) => job,
            Err(error) => {
                warn!(%error, "failed to plan automatic backfill");
                continue;
            }
        };
        let budget = SourceBudget {
            max_input_bytes: config.budgets.temporary_disk_bytes,
            max_frame_bytes: config.budgets.memory_bytes.min(32 * 1_024 * 1_024),
            max_frames: range.len(),
            max_buffered_frames: config
                .budgets
                .mapper_concurrency
                .max(config.budgets.source_concurrency),
            max_in_flight_requests: config.budgets.source_concurrency,
            temporary_disk_bytes: config.budgets.temporary_disk_bytes,
        };
        let processor_id = configured.id.clone();
        let processor_descriptor = processor.descriptor().clone();
        let handoff_store = store.clone();
        let chain_id = ChainId(config.chain.chain_id);
        let task_cancellation = cancellation.clone();
        let job_id = job.id.clone();
        tasks.push(tokio::spawn(async move {
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
                        Some(error_message),
                    )
                    .await;
                    return Err(anyhow::Error::new(error)
                        .context(format!("automatic cold backfill failed for {processor_id}")));
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
            let record = handoff_store
                .verify_hot_cold_handoff(
                    &handoff_id,
                    &processor_descriptor,
                    chain_id,
                    overlap,
                    anchor_hash,
                )
                .await
                .with_context(|| format!("verify hot/cold overlap for processor {processor_id}"))?;
            info!(
                processor = %processor_id,
                overlap_from = overlap.start().0,
                overlap_to = overlap.end().0,
                compared_blocks = record.compared_blocks,
                "hot/cold handoff verified"
            );
            Ok(record)
        }));
    }
    Ok(tasks)
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

    use super::*;

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
    fn configuration_discovery_prefers_explicit_then_project_local_then_workspace() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let workspace_config = directory.path().join("config/example.toml");
        std::fs::create_dir_all(workspace_config.parent().expect("config parent"))
            .expect("create config directory");
        std::fs::write(&workspace_config, "workspace").expect("write workspace config");
        assert_eq!(
            resolve_config_path(None, directory.path()),
            workspace_config
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
}
