//! Bounded historical scheduling, deterministic mapping, durable reduction,
//! retry, cancellation, and restart.

mod historical_material;

pub use historical_material::{
    AcquisitionBudgetShape, AcquisitionShape, HistoricalMaterialCoordinator,
    HistoricalMaterialCoordinatorConfig, HistoricalMaterialCoordinatorMode,
    HistoricalMaterialFrame, HistoricalMaterialSnapshot, HistoricalMaterialStartupPermit,
    HistoricalSourcePolicy, HistoricalSourcePolicyEntry, MaterialShape,
};

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    sync::{Arc, Mutex as StdMutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use futures::StreamExt;
use leani_primitives::{
    BlockHash, BlockNumber, BlockRange, BlockRef, Finality, LogFieldSet, ProcessorCursor,
    SourceKind, TrustModel, capability::CapabilitySet,
};
use leani_processor_api::{
    DeliveryLimitAction, DeliveryOrdering, DeliveryPolicyMode, EncodedDelta, OutputPolicyMode,
    Processor, ProcessorDescriptor, ProcessorError, PublicationPolicy, ReductionMode, StartPoint,
};
use leani_source_api::{
    ChainEvent, ConsensusCheckpoint, DataRequest, FinalityEvent, FinalitySource, HistorySource,
    LiveSource, LiveStart, SourceBudget, SourceChunk, SourceError, VerificationPolicy,
    coverage_gaps,
};
use leani_store_artifacts::{ArtifactBatchSink, ArtifactSinkError};
use leani_store_sqlite::{
    ApplyOutcome, ArchiveReconciliationRecord, ArchiveReconciliationState,
    HistoricalArtifactTarget, HistoricalBatchItem, HistoricalBatchMode, HistoricalCommitLimits,
    JobRecord, JobState, ProcessorRunState, SqliteStore, StoreError,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::warn;

const FINALITY_ANCHOR_RETRY_INTERVAL: Duration = Duration::from_millis(250);
const FINALITY_RECONNECT_BASE: Duration = Duration::from_secs(1);
const FINALITY_RECONNECT_MAX: Duration = Duration::from_secs(30);

/// Default maximum blocks in one historical commit.
pub const HISTORICAL_COMMIT_MAX_BLOCKS: usize = 128;
/// Default flush delay for a partially filled historical commit.
pub const HISTORICAL_COMMIT_MAX_DELAY_MS: u64 = 50;
/// Default maximum domain changes in one historical commit.
pub const HISTORICAL_COMMIT_MAX_CHANGES: usize = 10_000;
/// Default maximum stable-encoded domain-change bytes in one historical commit.
pub const HISTORICAL_COMMIT_MAX_ENCODED_BYTES: u64 = 16 * 1024 * 1024;
/// Default target for the p95 historical `SQLite` writer hold time.
pub const HISTORICAL_COMMIT_TARGET_WRITER_HOLD_MS: u64 = 20;
/// Default maximum concurrently active physical source chunks per node.
pub const HISTORICAL_MAX_ACTIVE_CHUNKS: usize = 4;
/// Default maximum bytes retained by mapped historical deltas per node.
pub const HISTORICAL_MAX_MAPPED_BYTES: u64 = 128 * 1024 * 1024;
/// Byte quantum used by node-global historical commit deficit round-robin.
pub const HISTORICAL_FAIR_COMMIT_QUANTUM_BYTES: u64 = 1024 * 1024;

/// Bounds for one historical runtime.
#[derive(Clone, Debug)]
pub struct HistoricalRuntimeConfig {
    pub mapper_concurrency: usize,
    pub maximum_active_chunks: usize,
    pub maximum_mapped_bytes: u64,
    pub commit_maximum_blocks: usize,
    pub commit_maximum_changes: usize,
    pub commit_maximum_encoded_bytes: u64,
    pub commit_maximum_delay: Duration,
    pub commit_target_writer_hold: Duration,
    pub max_attempts: u32,
    pub retry_base: Duration,
    pub retry_max: Duration,
}

impl Default for HistoricalRuntimeConfig {
    fn default() -> Self {
        Self {
            mapper_concurrency: 4,
            maximum_active_chunks: HISTORICAL_MAX_ACTIVE_CHUNKS,
            maximum_mapped_bytes: HISTORICAL_MAX_MAPPED_BYTES,
            commit_maximum_blocks: HISTORICAL_COMMIT_MAX_BLOCKS,
            commit_maximum_changes: HISTORICAL_COMMIT_MAX_CHANGES,
            commit_maximum_encoded_bytes: HISTORICAL_COMMIT_MAX_ENCODED_BYTES,
            commit_maximum_delay: Duration::from_millis(HISTORICAL_COMMIT_MAX_DELAY_MS),
            commit_target_writer_hold: Duration::from_millis(
                HISTORICAL_COMMIT_TARGET_WRITER_HOLD_MS,
            ),
            max_attempts: 5,
            retry_base: Duration::from_secs(1),
            retry_max: Duration::from_secs(30),
        }
    }
}

impl HistoricalRuntimeConfig {
    fn validate(&self) -> Result<(), RuntimeError> {
        if self.mapper_concurrency == 0 {
            return Err(RuntimeError::InvalidConfig(
                "mapper concurrency must be greater than zero".to_owned(),
            ));
        }
        if self.maximum_active_chunks == 0 || self.maximum_active_chunks > u32::MAX as usize {
            return Err(RuntimeError::InvalidConfig(
                "maximum active chunks must be in 1..=4294967295".to_owned(),
            ));
        }
        if self.maximum_mapped_bytes == 0 || self.maximum_mapped_bytes > u64::from(u32::MAX) {
            return Err(RuntimeError::InvalidConfig(
                "maximum mapped bytes must be in 1..=4294967295".to_owned(),
            ));
        }
        if self.commit_maximum_blocks == 0
            || self.commit_maximum_changes == 0
            || self.commit_maximum_encoded_bytes == 0
            || self.commit_maximum_delay.is_zero()
            || self.commit_target_writer_hold.is_zero()
        {
            return Err(RuntimeError::InvalidConfig(
                "historical commit limits must be greater than zero".to_owned(),
            ));
        }
        if self.max_attempts == 0 {
            return Err(RuntimeError::InvalidConfig(
                "max attempts must be greater than zero".to_owned(),
            ));
        }
        if self.retry_base.is_zero() || self.retry_max < self.retry_base {
            return Err(RuntimeError::InvalidConfig(
                "retry durations must be non-zero and max must be at least base".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct HistoricalPipelineBudget {
    pub(crate) active_chunks: Arc<Semaphore>,
    map_tasks: Arc<Semaphore>,
    mapped_bytes: Arc<Semaphore>,
    maximum_active_chunks: usize,
    maximum_map_tasks: usize,
    maximum_mapped_bytes: u64,
    fair_commits: HistoricalFairCommitScheduler,
}

#[derive(Clone, Debug)]
struct HistoricalFairCommitScheduler {
    inner: Arc<HistoricalFairCommitInner>,
}

#[derive(Debug)]
struct HistoricalFairCommitInner {
    state: StdMutex<HistoricalFairCommitState>,
    changed: Notify,
    quantum_bytes: u64,
}

#[derive(Debug, Default)]
struct HistoricalFairCommitState {
    active: bool,
    selected: Option<String>,
    order: VecDeque<String>,
    jobs: HashMap<String, HistoricalFairCommitJob>,
}

#[derive(Debug, Default)]
struct HistoricalFairCommitJob {
    deficit_bytes: i128,
    registered: bool,
    waiting: bool,
}

struct HistoricalFairJobRegistration {
    scheduler: HistoricalFairCommitScheduler,
    job_id: String,
}

struct HistoricalFairCommitPermit {
    scheduler: HistoricalFairCommitScheduler,
    job_id: String,
    released: bool,
}

impl HistoricalFairCommitScheduler {
    fn new(quantum_bytes: u64) -> Self {
        Self {
            inner: Arc::new(HistoricalFairCommitInner {
                state: StdMutex::new(HistoricalFairCommitState::default()),
                changed: Notify::new(),
                quantum_bytes,
            }),
        }
    }

    fn register_job(&self, job_id: &str) -> Result<HistoricalFairJobRegistration, RuntimeError> {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let job = state.jobs.entry(job_id.to_owned()).or_default();
        if job.registered {
            return Err(RuntimeError::InvalidConfig(format!(
                "historical job {job_id:?} is already registered with the fair scheduler"
            )));
        }
        job.registered = true;
        Ok(HistoricalFairJobRegistration {
            scheduler: self.clone(),
            job_id: job_id.to_owned(),
        })
    }

    async fn acquire(
        &self,
        job_id: &str,
        cancellation: &CancellationToken,
    ) -> Result<HistoricalFairCommitPermit, RuntimeError> {
        {
            let mut state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let Some(job) = state.jobs.get_mut(job_id) else {
                return Err(RuntimeError::InvalidConfig(format!(
                    "historical job {job_id:?} has no fair scheduler registration"
                )));
            };
            if !job.registered {
                return Err(RuntimeError::InvalidConfig(format!(
                    "historical job {job_id:?} has an inactive fair scheduler registration"
                )));
            }
            if job.waiting {
                return Err(RuntimeError::InvalidConfig(format!(
                    "historical job {job_id:?} requested concurrent fair commit turns"
                )));
            }
            job.waiting = true;
            state.order.push_back(job_id.to_owned());
        }
        loop {
            let changed = self.inner.changed.notified();
            let mut wake_selected = false;
            {
                let mut state = self
                    .inner
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if !state.active && state.selected.is_none() {
                    state.selected = select_fair_commit_job(&mut state, self.inner.quantum_bytes);
                    wake_selected = state.selected.is_some();
                }
                if !state.active && state.selected.as_deref() == Some(job_id) {
                    state.active = true;
                    state.selected = None;
                    if let Some(job) = state.jobs.get_mut(job_id) {
                        job.waiting = false;
                    }
                    return Ok(HistoricalFairCommitPermit {
                        scheduler: self.clone(),
                        job_id: job_id.to_owned(),
                        released: false,
                    });
                }
            }
            if wake_selected {
                self.inner.changed.notify_waiters();
            }
            tokio::select! {
                () = cancellation.cancelled() => {
                    self.cancel_waiter(job_id);
                    return Err(RuntimeError::Cancelled);
                }
                () = changed => {}
            }
        }
    }

    fn cancel_waiter(&self, job_id: &str) {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.selected.as_deref() == Some(job_id) {
            state.selected = None;
        }
        state.order.retain(|queued| queued != job_id);
        if let Some(job) = state.jobs.get_mut(job_id) {
            job.waiting = false;
        }
        drop(state);
        self.inner.changed.notify_waiters();
    }

    fn release(&self, job_id: &str, charged_bytes: u64) {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        debug_assert!(state.active);
        state.active = false;
        if let Some(job) = state.jobs.get_mut(job_id) {
            job.deficit_bytes = job.deficit_bytes.saturating_sub(i128::from(charged_bytes));
        }
        drop(state);
        self.inner.changed.notify_waiters();
    }
}

impl Drop for HistoricalFairJobRegistration {
    fn drop(&mut self) {
        let mut state = self
            .scheduler
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        debug_assert_ne!(state.selected.as_deref(), Some(self.job_id.as_str()));
        state.order.retain(|queued| queued != &self.job_id);
        state.jobs.remove(&self.job_id);
        drop(state);
        self.scheduler.inner.changed.notify_waiters();
    }
}

fn select_fair_commit_job(
    state: &mut HistoricalFairCommitState,
    quantum_bytes: u64,
) -> Option<String> {
    while !state.order.is_empty() {
        let candidates = state.order.len();
        for _ in 0..candidates {
            let job_id = state.order.pop_front()?;
            let Some(job) = state.jobs.get_mut(&job_id) else {
                continue;
            };
            if !job.waiting {
                continue;
            }
            job.deficit_bytes = job.deficit_bytes.saturating_add(i128::from(quantum_bytes));
            if job.deficit_bytes > 0 {
                return Some(job_id);
            }
            state.order.push_back(job_id);
        }
    }
    None
}

impl HistoricalFairCommitPermit {
    fn complete(mut self, charged_bytes: u64) {
        self.scheduler.release(&self.job_id, charged_bytes.max(1));
        self.released = true;
    }
}

impl Drop for HistoricalFairCommitPermit {
    fn drop(&mut self) {
        if !self.released {
            self.scheduler.release(&self.job_id, 0);
        }
    }
}

impl HistoricalPipelineBudget {
    /// Construct a node-shareable history pipeline budget.
    ///
    /// # Errors
    ///
    /// Rejects zero values and values that cannot be represented by Tokio's
    /// weighted semaphore.
    pub fn new(
        maximum_active_chunks: usize,
        maximum_map_tasks: usize,
        maximum_mapped_bytes: u64,
    ) -> Result<Self, RuntimeError> {
        if maximum_active_chunks == 0 || maximum_active_chunks > u32::MAX as usize {
            return Err(RuntimeError::InvalidConfig(
                "maximum active chunks must be in 1..=4294967295".to_owned(),
            ));
        }
        if maximum_mapped_bytes == 0 || maximum_mapped_bytes > u64::from(u32::MAX) {
            return Err(RuntimeError::InvalidConfig(
                "maximum mapped bytes must be in 1..=4294967295".to_owned(),
            ));
        }
        if maximum_map_tasks == 0 || maximum_map_tasks > u32::MAX as usize {
            return Err(RuntimeError::InvalidConfig(
                "maximum historical map tasks must be in 1..=4294967295".to_owned(),
            ));
        }
        Ok(Self {
            active_chunks: Arc::new(Semaphore::new(maximum_active_chunks)),
            map_tasks: Arc::new(Semaphore::new(maximum_map_tasks)),
            mapped_bytes: Arc::new(Semaphore::new(
                usize::try_from(maximum_mapped_bytes).map_err(|_| {
                    RuntimeError::InvalidConfig(
                        "maximum mapped bytes exceed this platform's address space".to_owned(),
                    )
                })?,
            )),
            maximum_active_chunks,
            maximum_map_tasks,
            maximum_mapped_bytes,
            fair_commits: HistoricalFairCommitScheduler::new(HISTORICAL_FAIR_COMMIT_QUANTUM_BYTES),
        })
    }

    async fn acquire_active_chunk(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<OwnedSemaphorePermit, RuntimeError> {
        tokio::select! {
            () = cancellation.cancelled() => Err(RuntimeError::Cancelled),
            permit = self.active_chunks.clone().acquire_owned() => permit.map_err(|_| {
                RuntimeError::InvalidConfig("historical active-chunk budget closed".to_owned())
            }),
        }
    }

    async fn reserve_mapped(
        &self,
        bytes: u64,
        cancellation: &CancellationToken,
    ) -> Result<OwnedSemaphorePermit, RuntimeError> {
        let charged = bytes.max(1);
        if charged > self.maximum_mapped_bytes {
            return Err(RuntimeError::MappedDeltaBudget {
                limit: self.maximum_mapped_bytes,
                observed: charged,
            });
        }
        let charged = u32::try_from(charged).map_err(|_| RuntimeError::MappedDeltaBudget {
            limit: self.maximum_mapped_bytes,
            observed: charged,
        })?;
        tokio::select! {
            () = cancellation.cancelled() => Err(RuntimeError::Cancelled),
            permit = self.mapped_bytes.clone().acquire_many_owned(charged) => permit.map_err(|_| {
                RuntimeError::InvalidConfig("historical mapped-byte budget closed".to_owned())
            }),
        }
    }

    async fn acquire_map_task(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<OwnedSemaphorePermit, RuntimeError> {
        tokio::select! {
            () = cancellation.cancelled() => Err(RuntimeError::Cancelled),
            permit = self.map_tasks.clone().acquire_owned() => permit.map_err(|_| {
                RuntimeError::InvalidConfig("historical map-task budget closed".to_owned())
            }),
        }
    }

    async fn acquire_commit_turn(
        &self,
        job_id: &str,
        cancellation: &CancellationToken,
    ) -> Result<HistoricalFairCommitPermit, RuntimeError> {
        self.fair_commits.acquire(job_id, cancellation).await
    }

    fn register_fair_job(
        &self,
        job_id: &str,
    ) -> Result<HistoricalFairJobRegistration, RuntimeError> {
        self.fair_commits.register_job(job_id)
    }
}

impl std::fmt::Debug for HistoricalPipelineBudget {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HistoricalPipelineBudget")
            .field("maximum_active_chunks", &self.maximum_active_chunks)
            .field(
                "available_active_chunks",
                &self.active_chunks.available_permits(),
            )
            .field("maximum_map_tasks", &self.maximum_map_tasks)
            .field("available_map_tasks", &self.map_tasks.available_permits())
            .field("maximum_mapped_bytes", &self.maximum_mapped_bytes)
            .field(
                "available_mapped_bytes",
                &self.mapped_bytes.available_permits(),
            )
            .field("fair_commits", &self.fair_commits)
            .finish()
    }
}

/// Stable job input retained for restart/audit.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BackfillJob {
    pub id: String,
    pub owner: HistoricalJobOwner,
    pub processor_instance: String,
    pub mode: BackfillMode,
    /// Explicit history publication stream for split delivery. Legacy and
    /// unified jobs leave this unset and publish to the processor default.
    pub delivery_stream_id: Option<String>,
    /// Immutable normalized ranges requested by the durable execution owner.
    pub ranges: Vec<BlockRange>,
    pub request: DataRequest,
    pub sink_ids: Vec<String>,
}

/// Durable control-plane owner for one historical execution.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoricalJobOwner {
    #[default]
    Materialization,
    Subscription,
}

impl HistoricalJobOwner {
    #[must_use]
    pub const fn job_kind(self) -> &'static str {
        match self {
            Self::Materialization => "materialization_job",
            Self::Subscription => "backfill_subscription_job",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BackfillMode {
    #[default]
    FillMissing,
    Recompute,
}

impl BackfillJob {
    /// Build a processor-shaped request with no source-specific field
    /// projection.
    ///
    /// # Errors
    ///
    /// Returns an error when the processor has no requirements or the range is
    /// malformed.
    pub fn for_processor(
        id: impl Into<String>,
        processor: &dyn Processor,
        chain_id: leani_primitives::ChainId,
        range: BlockRange,
        verification_policy: VerificationPolicy,
    ) -> Result<Self, RuntimeError> {
        Self::for_processor_ranges(id, processor, chain_id, vec![range], verification_policy)
    }

    /// Build one processor-shaped request for a normalized set of disjoint
    /// application ranges.
    ///
    /// Overlapping and adjacent ranges are coalesced. The compact range set is
    /// retained in the durable job while `request.range` remains the bounding
    /// range for compatibility with existing source/job diagnostics.
    ///
    /// # Errors
    ///
    /// Returns an error when the range set is empty or the processor has no
    /// data requirements.
    pub fn for_processor_ranges(
        id: impl Into<String>,
        processor: &dyn Processor,
        chain_id: leani_primitives::ChainId,
        ranges: Vec<BlockRange>,
        verification_policy: VerificationPolicy,
    ) -> Result<Self, RuntimeError> {
        let ranges = normalize_backfill_ranges(ranges)?;
        let first_range = ranges.first().copied().ok_or_else(|| {
            RuntimeError::InvalidConfig("backfill range set must not be empty".to_owned())
        })?;
        let last_range = ranges.last().copied().ok_or_else(|| {
            RuntimeError::InvalidConfig("backfill range set must not be empty".to_owned())
        })?;
        let bounding_range = BlockRange::new(first_range.start(), last_range.end())
            .map_err(|error| RuntimeError::InvalidConfig(error.to_string()))?;
        let requirements = &processor.descriptor().requirements;
        requirements.first().ok_or_else(|| {
            RuntimeError::InvalidConfig("processor has no data requirements".to_owned())
        })?;
        let required = requirements
            .iter()
            .fold(CapabilitySet::NONE, |all, requirement| {
                all.union(requirement.capabilities)
            });
        let minimum_finality = requirements
            .iter()
            .fold(Finality::Included, |required, requirement| {
                required.max(requirement.minimum_finality)
            });
        let log_fields = requirements
            .iter()
            .fold(LogFieldSet::NONE, |all, requirement| {
                all.union(requirement.log_fields)
            });
        let allow_filtered = requirements
            .iter()
            .all(|requirement| requirement.allow_filtered);
        let filters = if allow_filtered {
            let mut scope = leani_primitives::FilterScope::default();
            for requirement in requirements {
                union_filter_scope(&mut scope, &requirement.filter);
            }
            leani_source_api::FilterSet {
                senders: scope.senders.clone(),
                recipients: scope.recipients.clone(),
                scope,
            }
        } else {
            leani_source_api::FilterSet::default()
        };
        Ok(Self {
            id: id.into(),
            owner: HistoricalJobOwner::Materialization,
            processor_instance: processor.descriptor().instance.to_string(),
            mode: BackfillMode::FillMissing,
            delivery_stream_id: None,
            ranges,
            request: DataRequest {
                chain_id,
                range: bounding_range,
                required,
                log_fields,
                allow_filtered,
                projection: leani_source_api::FieldProjection::default(),
                filters,
                minimum_finality,
                verification_policy,
            },
            sink_ids: Vec::new(),
        })
    }

    /// Resolve the immutable normalized range set.
    ///
    /// # Errors
    ///
    /// Returns an error when a durable payload contains a non-normalized set
    /// or a bounding range that does not describe it exactly.
    pub fn requested_ranges(&self) -> Result<Vec<BlockRange>, RuntimeError> {
        let normalized = normalize_backfill_ranges(self.ranges.clone())?;
        if normalized != self.ranges {
            return Err(RuntimeError::InvalidConfig(
                "durable backfill ranges are not normalized".to_owned(),
            ));
        }
        let first_range = normalized.first().copied().ok_or_else(|| {
            RuntimeError::InvalidConfig("durable backfill range set must not be empty".to_owned())
        })?;
        let last_range = normalized.last().copied().ok_or_else(|| {
            RuntimeError::InvalidConfig("durable backfill range set must not be empty".to_owned())
        })?;
        let bounding = BlockRange::new(first_range.start(), last_range.end())
            .map_err(|error| RuntimeError::InvalidConfig(error.to_string()))?;
        if bounding != self.request.range {
            return Err(RuntimeError::InvalidConfig(
                "durable backfill bounding range does not match its range set".to_owned(),
            ));
        }
        Ok(normalized)
    }
}

fn normalize_backfill_ranges(mut ranges: Vec<BlockRange>) -> Result<Vec<BlockRange>, RuntimeError> {
    if ranges.is_empty() {
        return Err(RuntimeError::InvalidConfig(
            "backfill range set must not be empty".to_owned(),
        ));
    }
    ranges.sort_unstable_by_key(|range| (range.start(), range.end()));
    let mut normalized = Vec::<BlockRange>::with_capacity(ranges.len());
    for range in ranges {
        if let Some(previous) = normalized.last_mut()
            && range.start().0 <= previous.end().0.saturating_add(1)
        {
            *previous = BlockRange::new(previous.start(), previous.end().max(range.end()))
                .map_err(|error| RuntimeError::InvalidConfig(error.to_string()))?;
        } else {
            normalized.push(range);
        }
    }
    Ok(normalized)
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
struct BackfillCheckpoint {
    frames_committed: u64,
    chunks_completed: u64,
    last_block: Option<BlockNumber>,
    last_hash: Option<BlockHash>,
}

/// Decode the committed-block counter from a durable historical checkpoint.
///
/// # Errors
///
/// Returns an error when the checkpoint encoding is invalid.
pub fn historical_checkpoint_committed_blocks(bytes: &[u8]) -> Result<u64, RuntimeError> {
    Ok(decode_checkpoint(bytes)?.frames_committed)
}

/// Reproducible historical execution report.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BackfillSourceReport {
    pub source_id: String,
    pub source_kind: SourceKind,
    pub attempts: u32,
    pub failures: u32,
    pub frames_mapped: u64,
    pub frames_committed: u64,
    pub duplicate_frames: u64,
    /// Normalized execution material consumed from this source. This excludes
    /// transport framing, compression differences, and protocol overhead.
    pub source_bytes: u64,
    /// Physical normalized material attributed once across shared consumers.
    #[serde(default)]
    pub physical_source_bytes: u64,
    /// Logical normalized material delivered from another consumer's shared
    /// acquisition.
    #[serde(default)]
    pub reused_source_bytes: u64,
    /// Frames whose physical acquisition was attributed to another consumer.
    #[serde(default)]
    pub coalesced_frames: u64,
    pub elapsed_milliseconds: u64,
    pub last_error: Option<String>,
}

/// Reproducible historical execution report.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BackfillReport {
    pub job_id: String,
    pub source_id: String,
    pub processor_id: String,
    pub requested: BlockRange,
    #[serde(default)]
    pub requested_ranges: Vec<BlockRange>,
    pub initial_coverage: Vec<BlockRange>,
    pub final_coverage: Vec<BlockRange>,
    pub frames_mapped: u64,
    pub frames_committed: u64,
    pub duplicate_frames: u64,
    pub source_attempts: u32,
    pub source_bytes: u64,
    /// Physical normalized material attributed once across shared consumers.
    #[serde(default)]
    pub physical_source_bytes: u64,
    /// Logical normalized material reused from shared acquisitions.
    #[serde(default)]
    pub reused_source_bytes: u64,
    #[serde(default)]
    pub coalesced_frames: u64,
    #[serde(default)]
    pub acquisition_ids: Vec<u64>,
    pub elapsed_milliseconds: u64,
    pub sources: Vec<BackfillSourceReport>,
}

/// Re-map a finalized archive range and compare its compact processor deltas
/// with the checksums originally committed from live execution P2P.
///
/// This comparison does not require retaining raw live frames. Source
/// transport/range failures remain retryable and leave the durable record
/// running; shape, identity, requirement, or checksum disagreements are
/// durably marked failed before returning.
///
/// # Errors
///
/// Returns an error for source unavailability, cancellation, processor/store
/// failures, incomplete streams, or any deterministic disagreement.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub async fn reconcile_archive_deltas(
    store: &SqliteStore,
    source: &dyn HistorySource,
    processor: &dyn Processor,
    chain_id: leani_primitives::ChainId,
    range: BlockRange,
    verification_policy: VerificationPolicy,
    budget: SourceBudget,
    cancellation: CancellationToken,
) -> Result<ArchiveReconciliationRecord, RuntimeError> {
    let budget = budget.validate()?;
    let job = BackfillJob::for_processor(
        format!(
            "archive-reconciliation-{}-{}-{}-{}",
            processor.descriptor().id,
            source.descriptor().id,
            range.start().0,
            range.end().0
        ),
        processor,
        chain_id,
        range,
        verification_policy,
    )?;
    let plan = source.plan(&job.request).await?;
    plan.validate()?;
    let anchor_hash = store
        .coverage_hash(processor.descriptor(), range.end())
        .await?
        .ok_or_else(|| {
            RuntimeError::InvalidFrame(format!(
                "processor has no committed anchor at block {}",
                range.end().0
            ))
        })?;
    let id = job.id;
    let source_id = source.descriptor().id.to_string();
    let existing = store
        .begin_archive_reconciliation(
            &id,
            processor.descriptor(),
            &source_id,
            chain_id,
            range,
            anchor_hash,
        )
        .await?;
    if existing.state == ArchiveReconciliationState::Verified {
        return Ok(existing);
    }

    let mut compared = 0_u64;
    let mut expected_number = range.start().0;
    for chunk in plan.chunks {
        let mut frames = source.open(&chunk, budget, cancellation.clone()).await?;
        while let Some(item) = frames.next().await {
            if cancellation.is_cancelled() {
                return Err(RuntimeError::Cancelled);
            }
            let frame = item?;
            let mismatch = if let Err(error) = frame.validate_shape() {
                Some(format!(
                    "archive frame {} has invalid shape: {error}",
                    frame.block.number.0
                ))
            } else if frame.block.number.0 != expected_number {
                Some(format!(
                    "archive sequence expected block {expected_number}, received {}",
                    frame.block.number.0
                ))
            } else {
                None
            };
            if let Some(detail) = mismatch {
                return Err(persist_archive_mismatch(
                    store,
                    &id,
                    processor,
                    &source_id,
                    chain_id,
                    range,
                    anchor_hash,
                    compared,
                    detail,
                )
                .await);
            }
            let stored_hash = store
                .coverage_hash(processor.descriptor(), frame.block.number)
                .await?;
            if stored_hash != Some(frame.block.hash) {
                let detail = format!(
                    "block {} identity differs: committed {:?}, archive {:?}",
                    frame.block.number.0, stored_hash, frame.block.hash
                );
                return Err(persist_archive_mismatch(
                    store,
                    &id,
                    processor,
                    &source_id,
                    chain_id,
                    range,
                    anchor_hash,
                    compared,
                    detail,
                )
                .await);
            }
            for requirement in &processor.descriptor().requirements {
                if let Err(error) = requirement.validate_frame(&frame) {
                    return Err(persist_archive_mismatch(
                        store,
                        &id,
                        processor,
                        &source_id,
                        chain_id,
                        range,
                        anchor_hash,
                        compared,
                        format!(
                            "archive frame {} violates processor requirements: {error}",
                            frame.block.number.0
                        ),
                    )
                    .await);
                }
            }
            let (delta, equivalent_checksums) =
                map_with_finality_variants(processor, &frame).await?;
            let stored_checksum = store
                .applied_delta_checksum(
                    processor.descriptor(),
                    frame.block.number,
                    frame.block.hash,
                )
                .await?;
            if !stored_checksum.is_some_and(|checksum| equivalent_checksums.contains(&checksum)) {
                let detail = format!(
                    "block {} mapped delta differs: committed {:?}, archive {:?}",
                    frame.block.number.0, stored_checksum, delta.checksum
                );
                return Err(persist_archive_mismatch(
                    store,
                    &id,
                    processor,
                    &source_id,
                    chain_id,
                    range,
                    anchor_hash,
                    compared,
                    detail,
                )
                .await);
            }
            compared = compared.saturating_add(1);
            expected_number = expected_number.saturating_add(1);
        }
        if expected_number != chunk.range.end().0.saturating_add(1) {
            return Err(RuntimeError::IncompleteChunk {
                expected_through: chunk.range.end(),
                next: BlockNumber(expected_number),
            });
        }
    }
    if compared != range.len() {
        return Err(RuntimeError::IncompleteChunk {
            expected_through: range.end(),
            next: BlockNumber(expected_number),
        });
    }
    store
        .finish_archive_reconciliation(
            &id,
            processor.descriptor(),
            &source_id,
            chain_id,
            range,
            anchor_hash,
            compared,
            None,
        )
        .await
        .map_err(Into::into)
}

#[allow(clippy::too_many_arguments)]
async fn persist_archive_mismatch(
    store: &SqliteStore,
    id: &str,
    processor: &dyn Processor,
    source_id: &str,
    chain_id: leani_primitives::ChainId,
    range: BlockRange,
    anchor_hash: BlockHash,
    compared: u64,
    detail: String,
) -> RuntimeError {
    match store
        .finish_archive_reconciliation(
            id,
            processor.descriptor(),
            source_id,
            chain_id,
            range,
            anchor_hash,
            compared,
            Some(detail.clone()),
        )
        .await
    {
        Err(error) => RuntimeError::Store(error),
        Ok(_) => RuntimeError::InvalidFrame(detail),
    }
}

#[derive(Debug)]
struct MappedFrame {
    delta: EncodedDelta,
    equivalent_checksums: Vec<BlockHash>,
    finality: Finality,
    estimated_bytes: u64,
    material: Option<HistoricalMaterialFrame>,
    _mapped_byte_permit: Option<OwnedSemaphorePermit>,
}

fn mapped_delta_bytes(delta: &EncodedDelta, equivalent_checksums: &[BlockHash]) -> u64 {
    let durable = delta.encode_durable().map_or(u64::MAX, |encoded| {
        u64::try_from(encoded.len()).unwrap_or(u64::MAX)
    });
    durable.saturating_add(
        u64::try_from(equivalent_checksums.len())
            .unwrap_or(u64::MAX)
            .saturating_mul(32),
    )
}

async fn map_with_finality_variants(
    processor: &dyn Processor,
    frame: &leani_primitives::BlockFrame,
) -> Result<(EncodedDelta, Vec<BlockHash>), ProcessorError> {
    let delta = processor.map(frame).await?;
    let mut equivalent_checksums = processor.finality_variant_checksums(&delta)?;
    if !equivalent_checksums.contains(&delta.checksum) {
        return Err(ProcessorError::Invariant(
            "finality variants must include the exact delta checksum".to_owned(),
        ));
    }
    for finality in [Finality::Included, Finality::Finalized] {
        if finality == frame.finality {
            continue;
        }
        let mut variant = frame.clone();
        variant.finality = finality;
        if let Ok(variant) = processor.map(&variant).await {
            equivalent_checksums.push(variant.checksum);
        }
    }
    equivalent_checksums.sort_unstable();
    equivalent_checksums.dedup();
    Ok((delta, equivalent_checksums))
}

async fn accept_existing_delta_variant(
    store: &SqliteStore,
    processor: &dyn Processor,
    mapped: &MappedFrame,
) -> Result<bool, RuntimeError> {
    let Some(stored_checksum) = store
        .applied_delta_checksum(
            processor.descriptor(),
            mapped.delta.block.number,
            mapped.delta.block.hash,
        )
        .await?
    else {
        return Ok(false);
    };
    if !mapped.equivalent_checksums.contains(&stored_checksum) {
        return Err(StoreError::ConflictingApply {
            block: mapped.delta.block.number,
        }
        .into());
    }
    store
        .delete_pending_delta(processor.descriptor(), mapped.delta.block)
        .await?;
    if mapped.finality == Finality::Finalized {
        store
            .mark_finalized(processor.descriptor(), mapped.delta.block.number)
            .await?;
    }
    Ok(true)
}

async fn commit_mapped_delta(
    store: &SqliteStore,
    processor: &dyn Processor,
    mapped: &MappedFrame,
    sink_ids: &[String],
    publish_changes: bool,
    delivery_stream_id: Option<&str>,
) -> Result<ApplyOutcome, RuntimeError> {
    if accept_existing_delta_variant(store, processor, mapped).await? {
        return Ok(ApplyOutcome::AlreadyApplied);
    }
    if let Err(error) = store
        .persist_delta(processor.descriptor(), &mapped.delta)
        .await
    {
        if matches!(error, StoreError::ConflictingPendingDelta(_)) {
            tokio::task::yield_now().await;
            if accept_existing_delta_variant(store, processor, mapped).await? {
                return Ok(ApplyOutcome::AlreadyApplied);
            }
        }
        return Err(error.into());
    }
    let sequence = store
        .processor_cursor(processor.descriptor())
        .await?
        .map_or(1, |cursor| cursor.sequence.saturating_add(1));
    let cursor = ProcessorCursor {
        processor_id: processor.descriptor().id.to_string(),
        processor_version: processor.descriptor().version.to_string(),
        chain_id: mapped.delta.chain_id,
        block_number: mapped.delta.block.number,
        block_hash: mapped.delta.block.hash,
        finality: mapped.finality,
        sequence,
    };
    let applied = if let Some(stream_id) = delivery_stream_id {
        store
            .apply_with_change_publication_to_stream(
                processor,
                cursor,
                &mapped.delta,
                sink_ids,
                publish_changes,
                stream_id,
            )
            .await
    } else {
        store
            .apply_with_change_publication(
                processor,
                cursor,
                &mapped.delta,
                sink_ids,
                publish_changes,
            )
            .await
    };
    match applied {
        Ok(outcome) => Ok(outcome),
        Err(error @ StoreError::ConflictingApply { .. }) => {
            if accept_existing_delta_variant(store, processor, mapped).await? {
                Ok(ApplyOutcome::AlreadyApplied)
            } else {
                Err(error.into())
            }
        }
        Err(error) => Err(error.into()),
    }
}

/// One source/processor/store historical execution lane.
#[derive(Clone)]
pub struct HistoricalRuntime {
    store: SqliteStore,
    sources: Arc<Vec<Arc<dyn HistorySource>>>,
    processor: Arc<dyn Processor>,
    config: HistoricalRuntimeConfig,
    pipeline_budget: HistoricalPipelineBudget,
    adaptive_commit: Arc<StdMutex<AdaptiveCommitState>>,
    material_coordinator: Option<HistoricalMaterialCoordinator>,
    material_startup_permit: Option<HistoricalMaterialStartupPermit>,
    artifact_sink: Option<Arc<dyn ArtifactBatchSink>>,
}

const ADAPTIVE_COMMIT_SAMPLE_WINDOW: usize = 20;

#[derive(Debug)]
struct AdaptiveCommitState {
    maximum_blocks: usize,
    writer_hold_micros: VecDeque<u64>,
}

impl AdaptiveCommitState {
    fn new(maximum_blocks: usize) -> Self {
        Self {
            maximum_blocks,
            writer_hold_micros: VecDeque::with_capacity(ADAPTIVE_COMMIT_SAMPLE_WINDOW),
        }
    }

    fn maximum_blocks(&self) -> usize {
        self.maximum_blocks.max(1)
    }

    fn observe(&mut self, hold_micros: u64, configured_maximum: usize, target: Duration) {
        if self.writer_hold_micros.len() == ADAPTIVE_COMMIT_SAMPLE_WINDOW {
            self.writer_hold_micros.pop_front();
        }
        self.writer_hold_micros.push_back(hold_micros);
        let target_micros = u64::try_from(target.as_micros()).unwrap_or(u64::MAX);
        let mut samples = self.writer_hold_micros.iter().copied().collect::<Vec<_>>();
        samples.sort_unstable();
        let p95_index = samples
            .len()
            .saturating_mul(95)
            .div_ceil(100)
            .saturating_sub(1);
        let p95 = samples[p95_index];
        if p95 > target_micros && self.maximum_blocks > 1 {
            self.maximum_blocks = (self.maximum_blocks / 2).max(1);
            self.writer_hold_micros.clear();
        } else if samples.len() == ADAPTIVE_COMMIT_SAMPLE_WINDOW
            && p95 < target_micros / 2
            && self.maximum_blocks < configured_maximum
        {
            self.maximum_blocks = self
                .maximum_blocks
                .saturating_mul(2)
                .min(configured_maximum)
                .max(1);
            self.writer_hold_micros.clear();
        }
    }
}

struct HistoricalMaterialStartupRegistration<'a> {
    permit: Option<&'a HistoricalMaterialStartupPermit>,
}

impl<'a> HistoricalMaterialStartupRegistration<'a> {
    const fn new(permit: Option<&'a HistoricalMaterialStartupPermit>) -> Self {
        Self { permit }
    }

    fn complete(&mut self) {
        if let Some(permit) = self.permit.take() {
            let _ = permit.arrive();
        }
    }
}

impl Drop for HistoricalMaterialStartupRegistration<'_> {
    fn drop(&mut self) {
        self.complete();
    }
}

impl std::fmt::Debug for HistoricalRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HistoricalRuntime")
            .field("store", &self.store)
            .field(
                "sources",
                &self
                    .sources
                    .iter()
                    .map(|source| source.descriptor())
                    .collect::<Vec<_>>(),
            )
            .field("processor", &self.processor.descriptor())
            .field("config", &self.config)
            .field("pipeline_budget", &self.pipeline_budget)
            .field(
                "adaptive_commit_maximum_blocks",
                &self
                    .adaptive_commit
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .maximum_blocks(),
            )
            .field("material_coordinator", &self.material_coordinator)
            .field("material_startup_permit", &self.material_startup_permit)
            .field("artifact_sink", &self.artifact_sink.is_some())
            .finish()
    }
}

impl HistoricalRuntime {
    /// Construct a bounded historical lane without opening its source.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid concurrency or retry settings.
    pub fn new(
        store: SqliteStore,
        source: Arc<dyn HistorySource>,
        processor: Arc<dyn Processor>,
        config: HistoricalRuntimeConfig,
    ) -> Result<Self, RuntimeError> {
        Self::new_with_sources(store, vec![source], processor, config)
    }

    /// Construct a bounded lane with ordered history-source failover.
    ///
    /// Sources are attempted by descriptor priority. After a retryable
    /// transport/range failure, durable coverage is recalculated and the next
    /// source starts at the first uncommitted block.
    ///
    /// # Errors
    ///
    /// Rejects an empty list, duplicate source IDs, mixed chains, or invalid
    /// runtime limits.
    pub fn new_with_sources(
        store: SqliteStore,
        mut sources: Vec<Arc<dyn HistorySource>>,
        processor: Arc<dyn Processor>,
        config: HistoricalRuntimeConfig,
    ) -> Result<Self, RuntimeError> {
        config.validate()?;
        if sources.is_empty() {
            return Err(RuntimeError::InvalidConfig(
                "historical runtime requires at least one source".to_owned(),
            ));
        }
        let source_ids = sources
            .iter()
            .map(|source| source.descriptor().id.clone())
            .collect::<BTreeSet<_>>();
        if source_ids.len() != sources.len() {
            return Err(RuntimeError::InvalidConfig(
                "historical source IDs must be unique".to_owned(),
            ));
        }
        let chain_id = sources[0].descriptor().chain_id;
        if sources
            .iter()
            .any(|source| source.descriptor().chain_id != chain_id)
        {
            return Err(RuntimeError::InvalidConfig(
                "historical sources must belong to one chain".to_owned(),
            ));
        }
        sources.sort_by_key(|source| source.descriptor().priority);
        let pipeline_budget = HistoricalPipelineBudget::new(
            config.maximum_active_chunks,
            config.mapper_concurrency,
            config.maximum_mapped_bytes,
        )?;
        let adaptive_commit = Arc::new(StdMutex::new(AdaptiveCommitState::new(
            config.commit_maximum_blocks,
        )));
        Ok(Self {
            store,
            sources: Arc::new(sources),
            processor,
            config,
            pipeline_budget,
            adaptive_commit,
            material_coordinator: None,
            material_startup_permit: None,
            artifact_sink: None,
        })
    }

    /// Route physical source chunks through one chain-level exact
    /// single-flight coordinator.
    #[must_use]
    pub fn with_material_coordinator(
        mut self,
        material_coordinator: HistoricalMaterialCoordinator,
    ) -> Self {
        self.material_coordinator = Some(material_coordinator);
        self
    }

    /// Share node-level active-chunk, map-task, and mapped-byte budgets across jobs.
    #[must_use]
    pub fn with_pipeline_budget(mut self, pipeline_budget: HistoricalPipelineBudget) -> Self {
        self.pipeline_budget = pipeline_budget;
        self
    }

    /// Delay automatic physical acquisition until every startup job has
    /// registered a first demand or exited without one.
    #[must_use]
    pub fn with_material_startup_permit(mut self, permit: HistoricalMaterialStartupPermit) -> Self {
        self.material_startup_permit = Some(permit);
        self
    }

    /// Route finalized materialization artifacts to a durable external sink.
    ///
    /// The sink commits before `SQLite` coverage and the scheduler checkpoint,
    /// so a crash can leave a safe idempotent artifact prefix but can never
    /// leave coverage ahead of artifact durability.
    ///
    /// # Errors
    ///
    /// Rejects a processor whose artifact lifecycle is disabled.
    pub fn with_artifact_sink(
        mut self,
        sink: Arc<dyn ArtifactBatchSink>,
    ) -> Result<Self, RuntimeError> {
        if matches!(
            self.processor.descriptor().lifecycle.artifacts.mode,
            leani_processor_api::ArtifactPolicyMode::None
        ) {
            return Err(RuntimeError::InvalidConfig(
                "external artifact sink requires processor artifact retention".to_owned(),
            ));
        }
        self.artifact_sink = Some(sink);
        Ok(self)
    }

    /// Fill missing processor coverage and atomically checkpoint every
    /// committed frame.
    ///
    /// Source chunks are never reported complete before every yielded frame is
    /// reduced. Retrying after a crash recalculates gaps from durable coverage;
    /// an already committed frame is therefore skipped or idempotently
    /// accepted.
    ///
    /// # Errors
    ///
    /// Returns an error for cancellation, invalid plans/frames, permanent
    /// source failures, processor/store failures, or exhausted transient
    /// retries.
    #[allow(clippy::too_many_lines)]
    pub async fn run(
        &self,
        job: BackfillJob,
        budget: SourceBudget,
        cancellation: CancellationToken,
    ) -> Result<BackfillReport, RuntimeError> {
        if job.id.is_empty() {
            return Err(RuntimeError::InvalidConfig(
                "job id must not be empty".to_owned(),
            ));
        }
        let _fair_job = self.pipeline_budget.register_fair_job(&job.id)?;
        let budget = budget.validate()?;
        if self.config.mapper_concurrency > budget.max_buffered_frames {
            return Err(RuntimeError::InvalidConfig(format!(
                "mapper concurrency {} exceeds buffered frame budget {}",
                self.config.mapper_concurrency, budget.max_buffered_frames
            )));
        }
        let first_source = self.sources.first().ok_or_else(|| {
            RuntimeError::InvalidConfig("historical runtime has no sources".to_owned())
        })?;
        let source_count = std::num::NonZeroUsize::new(self.sources.len())
            .ok_or_else(|| {
                RuntimeError::InvalidConfig("historical runtime has no sources".to_owned())
            })?
            .get();
        if first_source.descriptor().chain_id != job.request.chain_id {
            return Err(RuntimeError::InvalidConfig(
                "sources and request chains differ".to_owned(),
            ));
        }
        let descriptor = self.processor.descriptor();
        if job.mode == BackfillMode::Recompute && descriptor.mode != ReductionMode::BlockLocal {
            return Err(RuntimeError::InvalidConfig(
                "recompute backfills require a block-local processor".to_owned(),
            ));
        }
        let requested_ranges = job.requested_ranges()?;
        if job.mode == BackfillMode::Recompute {
            self.verify_compact_recompute_coverage(
                &job,
                &requested_ranges,
                budget,
                cancellation.clone(),
            )
            .await?;
        }
        let job_payload = serde_json::to_vec(&job)?;
        let initial_coverage = self
            .coverage_for_ranges(descriptor, &requested_ranges)
            .await?;
        let mut checkpoint = self
            .load_or_create_job(&job, &job_payload)
            .await?
            .checkpoint
            .as_deref()
            .map(decode_checkpoint)
            .transpose()?
            .unwrap_or_default();
        let started = Instant::now();
        let mut frames_mapped = 0_u64;
        let mut duplicate_frames = 0_u64;
        let mut attempts = 0_u32;
        let mut source_bytes = 0_u64;
        let mut physical_source_bytes = 0_u64;
        let mut reused_source_bytes = 0_u64;
        let mut coalesced_frames = 0_u64;
        let mut acquisition_ids = BTreeSet::new();
        let mut used_sources = BTreeSet::new();
        let mut source_reports = BTreeMap::<String, BackfillSourceReport>::new();
        let startup_permit = self.material_startup_permit.clone();

        loop {
            if cancellation.is_cancelled() {
                self.save_checkpoint(&job, &job_payload, JobState::Running, attempts, &checkpoint)
                    .await?;
                return Err(RuntimeError::Cancelled);
            }
            let coverage = self
                .coverage_for_ranges(descriptor, &requested_ranges)
                .await?;
            let remaining_ranges = match job.mode {
                BackfillMode::FillMissing if job.owner == HistoricalJobOwner::Subscription => {
                    self.subscription_fill_missing_ranges(&job, &requested_ranges)
                        .await?
                }
                BackfillMode::FillMissing => requested_ranges
                    .iter()
                    .flat_map(|range| coverage_gaps(*range, &coverage))
                    .collect::<Vec<_>>(),
                BackfillMode::Recompute => requested_ranges
                    .iter()
                    .filter_map(|range| {
                        let Some(last) = checkpoint.last_block else {
                            return Some(*range);
                        };
                        if last < range.start() {
                            Some(*range)
                        } else if last < range.end() {
                            BlockRange::new(BlockNumber(last.0.saturating_add(1)), range.end()).ok()
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>(),
            };
            let Some(gap) = remaining_ranges.first().copied() else {
                if let Some(stream_id) = job.delivery_stream_id.as_deref() {
                    let through_block = job.request.range.end();
                    self.store
                        .append_backfill_completion(
                            descriptor,
                            stream_id,
                            job.request.chain_id,
                            through_block,
                        )
                        .await?;
                }
                self.save_checkpoint(
                    &job,
                    &job_payload,
                    JobState::Completed,
                    attempts,
                    &checkpoint,
                )
                .await?;
                return Ok(BackfillReport {
                    job_id: job.id,
                    source_id: if used_sources.is_empty() {
                        first_source.descriptor().id.to_string()
                    } else {
                        used_sources.into_iter().collect::<Vec<_>>().join(",")
                    },
                    processor_id: descriptor.id.to_string(),
                    requested: job.request.range,
                    requested_ranges: requested_ranges.clone(),
                    initial_coverage,
                    final_coverage: coverage,
                    frames_mapped,
                    frames_committed: checkpoint.frames_committed,
                    duplicate_frames,
                    source_attempts: attempts,
                    source_bytes,
                    physical_source_bytes,
                    reused_source_bytes,
                    coalesced_frames,
                    acquisition_ids: acquisition_ids.into_iter().collect(),
                    elapsed_milliseconds: elapsed_milliseconds(started),
                    sources: source_reports.into_values().collect(),
                });
            };
            let completes_subscription = remaining_ranges.len() == 1;
            let expected_parent = if gap.start().0 == 0 {
                None
            } else {
                self.store
                    .coverage_hash(descriptor, BlockNumber(gap.start().0 - 1))
                    .await?
                    .or_else(|| {
                        checkpoint
                            .last_block
                            .zip(checkpoint.last_hash)
                            .filter(|(last, _)| last.0.saturating_add(1) == gap.start().0)
                            .map(|(_, hash)| hash)
                    })
            };
            let expected_successor_parent = gap.end().0.checked_add(1).map(BlockNumber);
            let expected_successor_parent = if let Some(successor) = expected_successor_parent {
                self.store
                    .coverage_parent_hash(descriptor, successor)
                    .await?
            } else {
                None
            };

            let mut request = job.request.clone();
            request.range = gap;
            let recent_started = Instant::now();
            let mapped_before = frames_mapped;
            let committed_before = checkpoint.frames_committed;
            let duplicates_before = duplicate_frames;
            let bytes_before = source_bytes;
            let reused_bytes_before = reused_source_bytes;
            if self
                .try_run_recent_gap(
                    &job,
                    &job_payload,
                    &request,
                    attempts,
                    &mut checkpoint,
                    &mut frames_mapped,
                    &mut duplicate_frames,
                    &mut source_bytes,
                    &mut physical_source_bytes,
                    &mut reused_source_bytes,
                    &mut coalesced_frames,
                    &mut acquisition_ids,
                    cancellation.clone(),
                    completes_subscription,
                    expected_parent,
                    expected_successor_parent,
                )
                .await?
            {
                const RECENT_STORE: &str = "recent-store";
                used_sources.insert(RECENT_STORE.to_owned());
                let recent_report = source_reports
                    .entry(RECENT_STORE.to_owned())
                    .or_insert_with(|| BackfillSourceReport {
                        source_id: RECENT_STORE.to_owned(),
                        source_kind: SourceKind::Synthetic,
                        attempts: 0,
                        failures: 0,
                        frames_mapped: 0,
                        frames_committed: 0,
                        duplicate_frames: 0,
                        source_bytes: 0,
                        physical_source_bytes: 0,
                        reused_source_bytes: 0,
                        coalesced_frames: 0,
                        elapsed_milliseconds: 0,
                        last_error: None,
                    });
                recent_report.frames_mapped = recent_report
                    .frames_mapped
                    .saturating_add(frames_mapped.saturating_sub(mapped_before));
                recent_report.frames_committed = recent_report
                    .frames_committed
                    .saturating_add(checkpoint.frames_committed.saturating_sub(committed_before));
                recent_report.duplicate_frames = recent_report
                    .duplicate_frames
                    .saturating_add(duplicate_frames.saturating_sub(duplicates_before));
                recent_report.source_bytes = recent_report
                    .source_bytes
                    .saturating_add(source_bytes.saturating_sub(bytes_before));
                recent_report.reused_source_bytes = recent_report
                    .reused_source_bytes
                    .saturating_add(reused_source_bytes.saturating_sub(reused_bytes_before));
                recent_report.elapsed_milliseconds = recent_report
                    .elapsed_milliseconds
                    .saturating_add(elapsed_milliseconds(recent_started));
                continue;
            }

            let source_index = usize::try_from(attempts)
                .unwrap_or(usize::MAX)
                .rem_euclid(source_count);
            let source = self.sources.get(source_index).cloned().ok_or_else(|| {
                RuntimeError::InvalidConfig("historical source index is invalid".to_owned())
            })?;
            let source_id = source.descriptor().id.to_string();
            used_sources.insert(source_id.clone());
            attempts = attempts.saturating_add(1);
            self.save_checkpoint(&job, &job_payload, JobState::Running, attempts, &checkpoint)
                .await?;
            let attempt_started = Instant::now();
            let mapped_before = frames_mapped;
            let committed_before = checkpoint.frames_committed;
            let duplicates_before = duplicate_frames;
            let bytes_before = source_bytes;
            let physical_bytes_before = physical_source_bytes;
            let reused_bytes_before = reused_source_bytes;
            let coalesced_before = coalesced_frames;
            let attempt = self
                .run_gap(
                    source.clone(),
                    &job,
                    &job_payload,
                    &request,
                    budget,
                    attempts,
                    &mut checkpoint,
                    &mut frames_mapped,
                    &mut duplicate_frames,
                    &mut source_bytes,
                    &mut physical_source_bytes,
                    &mut reused_source_bytes,
                    &mut coalesced_frames,
                    &mut acquisition_ids,
                    cancellation.clone(),
                    startup_permit.as_ref(),
                    completes_subscription,
                    expected_parent,
                    expected_successor_parent,
                )
                .await;
            let source_report =
                source_reports
                    .entry(source_id)
                    .or_insert_with(|| BackfillSourceReport {
                        source_id: source.descriptor().id.to_string(),
                        source_kind: source.descriptor().kind,
                        attempts: 0,
                        failures: 0,
                        frames_mapped: 0,
                        frames_committed: 0,
                        duplicate_frames: 0,
                        source_bytes: 0,
                        physical_source_bytes: 0,
                        reused_source_bytes: 0,
                        coalesced_frames: 0,
                        elapsed_milliseconds: 0,
                        last_error: None,
                    });
            source_report.attempts = source_report.attempts.saturating_add(1);
            source_report.frames_mapped = source_report
                .frames_mapped
                .saturating_add(frames_mapped.saturating_sub(mapped_before));
            source_report.frames_committed = source_report
                .frames_committed
                .saturating_add(checkpoint.frames_committed.saturating_sub(committed_before));
            source_report.duplicate_frames = source_report
                .duplicate_frames
                .saturating_add(duplicate_frames.saturating_sub(duplicates_before));
            source_report.source_bytes = source_report
                .source_bytes
                .saturating_add(source_bytes.saturating_sub(bytes_before));
            source_report.physical_source_bytes = source_report
                .physical_source_bytes
                .saturating_add(physical_source_bytes.saturating_sub(physical_bytes_before));
            source_report.reused_source_bytes = source_report
                .reused_source_bytes
                .saturating_add(reused_source_bytes.saturating_sub(reused_bytes_before));
            source_report.coalesced_frames = source_report
                .coalesced_frames
                .saturating_add(coalesced_frames.saturating_sub(coalesced_before));
            source_report.elapsed_milliseconds = source_report
                .elapsed_milliseconds
                .saturating_add(elapsed_milliseconds(attempt_started));
            if let Err(error) = &attempt
                && !matches!(error, RuntimeError::Cancelled)
            {
                source_report.failures = source_report.failures.saturating_add(1);
                source_report.last_error = Some(error.to_string());
            }
            match attempt {
                Ok(()) => {}
                Err(RuntimeError::Source(error))
                    if failover_source_error(&error) && attempts < self.config.max_attempts =>
                {
                    let delay =
                        retry_delay(self.config.retry_base, self.config.retry_max, attempts);
                    warn!(
                        job_id = %job.id,
                        attempt = attempts,
                        source_id = %source.descriptor().id,
                        ?delay,
                        error = %error,
                        "transient historical source failure"
                    );
                    tokio::select! {
                        () = cancellation.cancelled() => {
                            self.save_checkpoint(
                                &job,
                                &job_payload,
                                JobState::Running,
                                attempts,
                                &checkpoint,
                            ).await?;
                            return Err(RuntimeError::Cancelled);
                        }
                        () = tokio::time::sleep(delay) => {}
                    }
                }
                Err(error @ RuntimeError::Cancelled) => {
                    self.save_checkpoint(
                        &job,
                        &job_payload,
                        JobState::Running,
                        attempts,
                        &checkpoint,
                    )
                    .await?;
                    return Err(error);
                }
                Err(
                    error @ RuntimeError::Store(
                        StoreError::PhysicalStorageLimit { .. }
                        | StoreError::ArtifactStorageLimit { .. },
                    ),
                ) if job.owner == HistoricalJobOwner::Materialization => {
                    self.save_checkpoint(
                        &job,
                        &job_payload,
                        JobState::StorageBackpressured,
                        attempts,
                        &checkpoint,
                    )
                    .await?;
                    return Err(error);
                }
                Err(error) => {
                    self.save_checkpoint(
                        &job,
                        &job_payload,
                        JobState::Failed,
                        attempts,
                        &checkpoint,
                    )
                    .await?;
                    return Err(error);
                }
            }
        }
    }

    async fn coverage_for_ranges(
        &self,
        descriptor: &ProcessorDescriptor,
        ranges: &[BlockRange],
    ) -> Result<Vec<BlockRange>, RuntimeError> {
        let mut coverage = Vec::new();
        for range in ranges {
            coverage.extend(self.store.coverage(descriptor, *range).await?);
        }
        Ok(coverage)
    }

    async fn subscription_fill_missing_ranges(
        &self,
        job: &BackfillJob,
        requested_ranges: &[BlockRange],
    ) -> Result<Vec<BlockRange>, RuntimeError> {
        let subscription = self
            .store
            .backfill_subscription_for_job(&job.id)
            .await?
            .ok_or_else(|| {
                StoreError::Invariant(format!(
                    "subscription job {:?} has no durable subscription metadata",
                    job.id
                ))
            })?;
        let progress = self
            .store
            .backfill_subscription_range_progress(&job.id)
            .await?
            .ok_or_else(|| {
                StoreError::Invariant(format!(
                    "subscription job {:?} has no durable range progress",
                    job.id
                ))
            })?;
        if subscription.ranges != requested_ranges || progress.len() != requested_ranges.len() {
            return Err(StoreError::Invariant(format!(
                "subscription job {:?} durable ranges differ from its immutable payload",
                job.id
            ))
            .into());
        }

        let mut remaining = Vec::new();
        for (requested, progress) in requested_ranges.iter().zip(progress) {
            if progress.range != *requested {
                return Err(StoreError::Invariant(format!(
                    "subscription job {:?} range progress differs from its immutable payload",
                    job.id
                ))
                .into());
            }
            let work = coverage_gaps(*requested, &subscription.preexisting_coverage);
            let work_blocks = work.iter().try_fold(0_u64, |total, range| {
                total
                    .checked_add(range.len())
                    .ok_or(StoreError::Numeric("subscription work blocks"))
            })?;
            if progress.committed_work_blocks > work_blocks {
                return Err(StoreError::Invariant(format!(
                    "subscription job {:?} committed {} of {} creation-time work blocks in range {}..={}",
                    job.id,
                    progress.committed_work_blocks,
                    work_blocks,
                    requested.start().0,
                    requested.end().0,
                ))
                .into());
            }

            let mut committed = progress.committed_work_blocks;
            for range in work {
                if committed >= range.len() {
                    committed -= range.len();
                    continue;
                }
                let start = BlockNumber(
                    range
                        .start()
                        .0
                        .checked_add(committed)
                        .ok_or(StoreError::Numeric("remaining subscription range start"))?,
                );
                remaining.push(BlockRange::new(start, range.end()).map_err(|error| {
                    StoreError::Invariant(format!(
                        "subscription job {:?} has invalid remaining work: {error}",
                        job.id
                    ))
                })?);
                committed = 0;
            }
            if committed != 0 {
                return Err(StoreError::Invariant(format!(
                    "subscription job {:?} range progress could not be reconciled",
                    job.id
                ))
                .into());
            }
        }
        Ok(remaining)
    }

    async fn verify_compact_recompute_coverage(
        &self,
        job: &BackfillJob,
        requested_ranges: &[BlockRange],
        budget: SourceBudget,
        cancellation: CancellationToken,
    ) -> Result<(), RuntimeError> {
        let mut segments = BTreeMap::new();
        for requested in requested_ranges {
            for segment in self
                .store
                .finalized_coverage_segments(self.processor.descriptor(), *requested)
                .await?
            {
                segments
                    .entry((segment.interval_start, segment.range.start()))
                    .or_insert(segment);
            }
        }
        for segment in segments.into_values() {
            self.verify_compact_recompute_segment(job, segment, budget, cancellation.clone())
                .await?;
        }
        Ok(())
    }

    async fn verify_compact_recompute_segment(
        &self,
        job: &BackfillJob,
        segment: leani_store_sqlite::FinalizedCoverageSegment,
        mut budget: SourceBudget,
        cancellation: CancellationToken,
    ) -> Result<(), RuntimeError> {
        budget.max_frames = budget.max_frames.max(segment.range.len());
        let source_policy = HistoricalSourcePolicy::from_sources(self.sources.as_slice());
        let mut last_error = None;
        for source in self.sources.iter() {
            let mut request = job.request.clone();
            request.range = segment.range;
            let attempt = async {
                let physical_request = self.material_coordinator.as_ref().map_or_else(
                    || request.clone(),
                    |coordinator| coordinator.physical_request(source.as_ref(), &request, budget),
                );
                let plan = source.plan(&physical_request).await?;
                plan.validate()?;
                let mut expected_number = segment.range.start().0;
                let mut expected_parent = segment.start_parent_hash;
                for chunk in plan.chunks {
                    if chunk.range.end() < segment.range.start()
                        || chunk.range.start() > segment.range.end()
                    {
                        continue;
                    }
                    let _active_chunk = if self.material_coordinator.is_none() {
                        Some(
                            self.pipeline_budget
                                .acquire_active_chunk(&cancellation)
                                .await?,
                        )
                    } else {
                        None
                    };
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
                        HistoricalMaterialCoordinator::standalone(
                            source
                                .open(&chunk, budget, cancellation.clone())
                                .await?,
                        )
                    };
                    tokio::pin!(stream);
                    while let Some(material) = stream.next().await {
                        let material = material?;
                        let frame = material.frame();
                        frame
                            .validate_shape()
                            .map_err(|error| SourceError::CorruptFrame(error.to_owned()))?;
                        if frame.chain_id != request.chain_id
                            || frame.finality != Finality::Finalized
                            || frame.block.number.0 != expected_number
                            || frame.block.parent_hash != expected_parent
                        {
                            return Err(RuntimeError::Source(SourceError::CorruptFrame(format!(
                                "compact coverage verification diverged at expected block {expected_number}"
                            ))));
                        }
                        for requirement in &self.processor.descriptor().requirements {
                            requirement
                                .validate_frame(frame)
                                .map_err(|error| ProcessorError::Input(error.to_owned()))?;
                        }
                        expected_number = expected_number.saturating_add(1);
                        expected_parent = frame.block.hash;
                        material.acknowledge();
                    }
                }
                if expected_number != segment.range.end().0.saturating_add(1)
                    || expected_parent != segment.end_hash
                {
                    return Err(RuntimeError::Source(SourceError::CorruptFrame(format!(
                        "compact coverage segment {}..={} did not reach its retained end anchor",
                        segment.range.start().0,
                        segment.range.end().0
                    ))));
                }
                Ok(())
            }
            .await;
            match attempt {
                Ok(()) => return Ok(()),
                Err(RuntimeError::Cancelled) => return Err(RuntimeError::Cancelled),
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or_else(|| {
            RuntimeError::InvalidConfig(
                "compact coverage verification has no usable historical source".to_owned(),
            )
        }))
    }

    #[allow(clippy::too_many_arguments)]
    async fn try_run_recent_gap(
        &self,
        job: &BackfillJob,
        job_payload: &[u8],
        request: &DataRequest,
        attempts: u32,
        checkpoint: &mut BackfillCheckpoint,
        frames_mapped: &mut u64,
        duplicate_frames: &mut u64,
        source_bytes: &mut u64,
        physical_source_bytes: &mut u64,
        reused_source_bytes: &mut u64,
        coalesced_frames: &mut u64,
        acquisition_ids: &mut BTreeSet<u64>,
        cancellation: CancellationToken,
        completes_subscription: bool,
        expected_parent: Option<BlockHash>,
        expected_successor_parent: Option<BlockHash>,
    ) -> Result<bool, RuntimeError> {
        let Some(bounds) = self.store.recent_canonical_bounds(request.chain_id).await? else {
            return Ok(false);
        };
        if bounds.start() > request.range.start() || bounds.end() < request.range.end() {
            return Ok(false);
        }
        let minimum_trust = match request.verification_policy {
            VerificationPolicy::CompleteCryptographic => TrustModel::ProtocolVerified,
            VerificationPolicy::TrustedDataset => TrustModel::TrustedDataset,
            VerificationPolicy::BestEffort => TrustModel::Untrusted,
        };
        let initial_capacity = usize::try_from(request.range.len().min(4_096)).unwrap_or(4_096);
        let mut frames = Vec::with_capacity(initial_capacity);
        for number in request.range.start().0..=request.range.end().0 {
            let Some(frame) = self
                .store
                .recent_frame(request.chain_id, BlockNumber(number))
                .await?
            else {
                return Ok(false);
            };
            frame
                .validate_shape()
                .map_err(|error| SourceError::CorruptFrame(error.to_owned()))?;
            if frame.chain_id != request.chain_id
                || frame.block.number != BlockNumber(number)
                || !frame
                    .provenance
                    .iter()
                    .any(|provenance| provenance.trust >= minimum_trust)
                || self
                    .processor
                    .descriptor()
                    .requirements
                    .iter()
                    .any(|requirement| requirement.validate_frame(&frame).is_err())
            {
                return Ok(false);
            }
            frames.push(frame);
        }
        let last = self
            .run_material_chunk(
                HistoricalMaterialCoordinator::retained(frames),
                request.range,
                expected_parent,
                job,
                job_payload,
                attempts,
                checkpoint,
                frames_mapped,
                duplicate_frames,
                source_bytes,
                physical_source_bytes,
                reused_source_bytes,
                coalesced_frames,
                acquisition_ids,
                cancellation,
                completes_subscription,
                expected_successor_parent,
            )
            .await?;
        verify_successor_anchor(request.range, last, expected_successor_parent)?;
        checkpoint.chunks_completed = checkpoint.chunks_completed.saturating_add(1);
        self.save_checkpoint(job, job_payload, JobState::Running, attempts, checkpoint)
            .await?;
        Ok(true)
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn run_gap(
        &self,
        source: Arc<dyn HistorySource>,
        job: &BackfillJob,
        job_payload: &[u8],
        request: &DataRequest,
        budget: SourceBudget,
        attempts: u32,
        checkpoint: &mut BackfillCheckpoint,
        frames_mapped: &mut u64,
        duplicate_frames: &mut u64,
        source_bytes: &mut u64,
        physical_source_bytes: &mut u64,
        reused_source_bytes: &mut u64,
        coalesced_frames: &mut u64,
        acquisition_ids: &mut BTreeSet<u64>,
        cancellation: CancellationToken,
        startup_permit: Option<&HistoricalMaterialStartupPermit>,
        completes_subscription: bool,
        expected_parent: Option<BlockHash>,
        expected_successor_parent: Option<BlockHash>,
    ) -> Result<(), RuntimeError> {
        let mut startup_registration = HistoricalMaterialStartupRegistration::new(startup_permit);
        let source_policy = HistoricalSourcePolicy::from_sources(self.sources.as_slice());
        let physical_request = match (&self.material_coordinator, startup_permit) {
            (Some(coordinator), Some(startup_permit)) => {
                coordinator
                    .physical_request_after_startup_registration(
                        source_policy.clone(),
                        source.as_ref(),
                        request,
                        budget,
                        startup_permit,
                        &cancellation,
                    )
                    .await?
            }
            (Some(coordinator), None) => {
                coordinator.physical_request(source.as_ref(), request, budget)
            }
            (None, _) => request.clone(),
        };
        let plan = source.plan(&physical_request).await?;
        plan.validate()?;
        let startup_gate = startup_permit.map(HistoricalMaterialStartupPermit::gate);
        let mut prior_chunk_last = expected_parent;
        let mut planned = plan
            .chunks
            .into_iter()
            .filter_map(|chunk| {
                let logical_start = request.range.start().max(chunk.range.start());
                let logical_end = request.range.end().min(chunk.range.end());
                BlockRange::new(logical_start, logical_end)
                    .ok()
                    .map(|logical_range| (chunk, logical_range))
            })
            .collect::<VecDeque<_>>();
        if let Some(coordinator) = &self.material_coordinator {
            let acquisition_window = coordinator.acquisition_window(budget.max_in_flight_requests);
            let mut opened = VecDeque::<(
                SourceChunk,
                BlockRange,
                historical_material::HistoricalMaterialStream,
            )>::new();
            while opened.len() < acquisition_window
                && let Some((chunk, logical_range)) = planned.pop_front()
            {
                let stream = if let Some(startup_gate) = &startup_gate {
                    coordinator.open_after_startup_registration(
                        source_policy.clone(),
                        source.clone(),
                        request,
                        chunk.clone(),
                        budget,
                        cancellation.clone(),
                        startup_gate.clone(),
                    )?
                } else {
                    coordinator.open(
                        source_policy.clone(),
                        source.clone(),
                        request,
                        chunk.clone(),
                        budget,
                        cancellation.clone(),
                    )?
                };
                opened.push_back((chunk, logical_range, stream));
            }
            startup_registration.complete();
            while let Some((chunk, logical_range, stream)) = opened.pop_front() {
                let completes_subscription =
                    completes_subscription && opened.is_empty() && planned.is_empty();
                prior_chunk_last = self
                    .run_material_chunk(
                        stream,
                        logical_range,
                        chunk
                            .expected_parent
                            .filter(|_| chunk.range.start() == logical_range.start())
                            .filter(|source_parent| {
                                prior_chunk_last.is_none_or(|stored| stored == *source_parent)
                            })
                            .or(prior_chunk_last),
                        job,
                        job_payload,
                        attempts,
                        checkpoint,
                        frames_mapped,
                        duplicate_frames,
                        source_bytes,
                        physical_source_bytes,
                        reused_source_bytes,
                        coalesced_frames,
                        acquisition_ids,
                        cancellation.clone(),
                        completes_subscription,
                        (logical_range.end() == request.range.end())
                            .then_some(expected_successor_parent)
                            .flatten(),
                    )
                    .await?;
                checkpoint.chunks_completed = checkpoint.chunks_completed.saturating_add(1);
                self.save_checkpoint(job, job_payload, JobState::Running, attempts, checkpoint)
                    .await?;
                if let Some((next_chunk, next_logical_range)) = planned.pop_front() {
                    let next_stream = coordinator.open(
                        source_policy.clone(),
                        source.clone(),
                        request,
                        next_chunk.clone(),
                        budget,
                        cancellation.clone(),
                    )?;
                    opened.push_back((next_chunk, next_logical_range, next_stream));
                }
            }
            verify_successor_anchor(request.range, prior_chunk_last, expected_successor_parent)?;
            return Ok(());
        }

        startup_registration.complete();
        while let Some((chunk, logical_range)) = planned.pop_front() {
            let completes_subscription = completes_subscription && planned.is_empty();
            let _active_chunk = self
                .pipeline_budget
                .acquire_active_chunk(&cancellation)
                .await?;
            let stream = HistoricalMaterialCoordinator::standalone(
                source.open(&chunk, budget, cancellation.clone()).await?,
            );
            prior_chunk_last = self
                .run_material_chunk(
                    stream,
                    logical_range,
                    chunk
                        .expected_parent
                        .filter(|_| chunk.range.start() == logical_range.start())
                        .filter(|source_parent| {
                            prior_chunk_last.is_none_or(|stored| stored == *source_parent)
                        })
                        .or(prior_chunk_last),
                    job,
                    job_payload,
                    attempts,
                    checkpoint,
                    frames_mapped,
                    duplicate_frames,
                    source_bytes,
                    physical_source_bytes,
                    reused_source_bytes,
                    coalesced_frames,
                    acquisition_ids,
                    cancellation.clone(),
                    completes_subscription,
                    (logical_range.end() == request.range.end())
                        .then_some(expected_successor_parent)
                        .flatten(),
                )
                .await?;
            checkpoint.chunks_completed = checkpoint.chunks_completed.saturating_add(1);
            self.save_checkpoint(job, job_payload, JobState::Running, attempts, checkpoint)
                .await?;
        }
        verify_successor_anchor(request.range, prior_chunk_last, expected_successor_parent)?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn run_material_chunk(
        &self,
        stream: historical_material::HistoricalMaterialStream,
        range: BlockRange,
        mut expected_parent: Option<BlockHash>,
        job: &BackfillJob,
        job_payload: &[u8],
        attempts: u32,
        checkpoint: &mut BackfillCheckpoint,
        frames_mapped: &mut u64,
        duplicate_frames: &mut u64,
        source_bytes: &mut u64,
        physical_source_bytes: &mut u64,
        reused_source_bytes: &mut u64,
        coalesced_frames: &mut u64,
        acquisition_ids: &mut BTreeSet<u64>,
        cancellation: CancellationToken,
        completes_subscription: bool,
        expected_end_hash: Option<BlockHash>,
    ) -> Result<Option<BlockHash>, RuntimeError> {
        let descriptor = self.processor.descriptor();
        let subscription_microbatch = job.delivery_stream_id.is_some()
            && matches!(descriptor.lifecycle.output.mode, OutputPolicyMode::None);
        let materialization_microbatch = job.delivery_stream_id.is_none()
            && descriptor.lifecycle.delivery.mode == DeliveryPolicyMode::None
            && job.mode == BackfillMode::FillMissing;
        if subscription_microbatch || materialization_microbatch {
            return self
                .run_material_chunk_microbatched(
                    stream,
                    range,
                    expected_parent,
                    job,
                    job_payload,
                    attempts,
                    checkpoint,
                    frames_mapped,
                    duplicate_frames,
                    source_bytes,
                    physical_source_bytes,
                    reused_source_bytes,
                    coalesced_frames,
                    acquisition_ids,
                    cancellation,
                    completes_subscription,
                    expected_end_hash,
                )
                .await;
        }
        let mut next_number = range.start().0;
        let processor = self.processor.clone();
        let pipeline_budget = self.pipeline_budget.clone();
        let map_cancellation = cancellation.clone();
        let mapped = stream.map(move |item| {
            let validation = item.and_then(|material| {
                material
                    .frame()
                    .validate_shape()
                    .map_err(|error| SourceError::CorruptFrame(error.to_owned()))?;
                Ok(material)
            });
            let processor = processor.clone();
            let pipeline_budget = pipeline_budget.clone();
            let map_cancellation = map_cancellation.clone();
            async move {
                let material = validation?;
                let frame = material.frame();
                for requirement in &processor.descriptor().requirements {
                    requirement
                        .validate_frame(frame)
                        .map_err(|error| ProcessorError::Input(error.to_owned()))?;
                }
                let estimated_bytes = frame.estimated_heap_bytes();
                let finality = frame.finality;
                let _map_task = pipeline_budget.acquire_map_task(&map_cancellation).await?;
                let (delta, equivalent_checksums) =
                    map_with_finality_variants(processor.as_ref(), frame).await?;
                let mapped_byte_permit = pipeline_budget
                    .reserve_mapped(
                        mapped_delta_bytes(&delta, &equivalent_checksums),
                        &map_cancellation,
                    )
                    .await?;
                Ok::<_, RuntimeError>(MappedFrame {
                    delta,
                    equivalent_checksums,
                    finality,
                    estimated_bytes,
                    material: Some(material),
                    _mapped_byte_permit: Some(mapped_byte_permit),
                })
            }
        });
        let mut mapped = mapped.buffered(self.config.mapper_concurrency);
        while let Some(result) = mapped.next().await {
            if cancellation.is_cancelled() {
                return Err(RuntimeError::Cancelled);
            }
            let mapped = result?;
            if mapped.delta.block.number.0 != next_number {
                return Err(RuntimeError::Source(SourceError::CorruptFrame(format!(
                    "chunk expected block {next_number}, received {}",
                    mapped.delta.block.number.0
                ))));
            }
            if let Some(parent) = expected_parent
                && mapped.delta.block.parent_hash != parent
            {
                return Err(RuntimeError::Source(SourceError::CorruptFrame(format!(
                    "parent mismatch at block {}",
                    mapped.delta.block.number.0
                ))));
            }
            verify_range_end_hash(range, &mapped.delta, expected_end_hash)?;
            next_number = next_number.saturating_add(1);
            expected_parent = Some(mapped.delta.block.hash);
            *frames_mapped = frames_mapped.saturating_add(1);
            *source_bytes = source_bytes.saturating_add(mapped.estimated_bytes);
            let material = mapped
                .material
                .as_ref()
                .expect("historical mapped frames retain material attribution");
            if material.is_physical_source_delivery() {
                *physical_source_bytes =
                    physical_source_bytes.saturating_add(mapped.estimated_bytes);
            } else {
                *reused_source_bytes = reused_source_bytes.saturating_add(mapped.estimated_bytes);
                if material.is_coalesced_delivery() {
                    *coalesced_frames = coalesced_frames.saturating_add(1);
                }
            }
            if let Some(acquisition_id) = material.acquisition_id() {
                acquisition_ids.insert(acquisition_id);
            }
            let publish_changes = !matches!(
                self.processor.descriptor().publication,
                PublicationPolicy::TerminalOnly
            ) || mapped.delta.block.number == job.request.range.end();
            let mut backpressured = false;
            let outcome = loop {
                let fair_turn = self
                    .pipeline_budget
                    .acquire_commit_turn(&job.id, &cancellation)
                    .await?;
                let commit = self
                    .commit_mapped_frame(
                        &mapped,
                        &job.sink_ids,
                        publish_changes,
                        job.mode,
                        job.delivery_stream_id.as_deref(),
                    )
                    .await;
                match commit {
                    Err(RuntimeError::Store(StoreError::DeliveryLimit {
                        action: DeliveryLimitAction::Pause,
                        ..
                    })) if job.delivery_stream_id.is_some() => {
                        drop(fair_turn);
                        if !backpressured {
                            self.store
                                .set_backfill_subscription_state(
                                    &job.id,
                                    leani_store_sqlite::BackfillSubscriptionState::Backpressured,
                                    None,
                                )
                                .await?;
                            backpressured = true;
                        }
                        tokio::select! {
                            () = cancellation.cancelled() => {
                                return Err(RuntimeError::Cancelled);
                            }
                            () = self.store.wait_for_delivery_capacity_change() => {}
                            () = tokio::time::sleep(Duration::from_millis(250)) => {}
                        }
                    }
                    result => {
                        let outcome = result?;
                        fair_turn.complete(mapped.estimated_bytes.saturating_add(
                            mapped_delta_bytes(&mapped.delta, &mapped.equivalent_checksums),
                        ));
                        break outcome;
                    }
                }
            };
            if backpressured {
                self.store
                    .set_backfill_subscription_state(
                        &job.id,
                        leani_store_sqlite::BackfillSubscriptionState::Running,
                        None,
                    )
                    .await?;
            }
            match outcome {
                ApplyOutcome::Applied { .. } => {
                    checkpoint.frames_committed = checkpoint.frames_committed.saturating_add(1);
                }
                ApplyOutcome::AlreadyApplied => {
                    *duplicate_frames = duplicate_frames.saturating_add(1);
                }
            }
            material.acknowledge();
            checkpoint.last_block = Some(mapped.delta.block.number);
            checkpoint.last_hash = Some(mapped.delta.block.hash);
            self.save_checkpoint(job, job_payload, JobState::Running, attempts, checkpoint)
                .await?;
        }
        if next_number != range.end().0.saturating_add(1) {
            return Err(RuntimeError::IncompleteChunk {
                expected_through: range.end(),
                next: BlockNumber(next_number),
            });
        }
        Ok(expected_parent)
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn run_material_chunk_microbatched(
        &self,
        stream: historical_material::HistoricalMaterialStream,
        range: BlockRange,
        mut expected_parent: Option<BlockHash>,
        job: &BackfillJob,
        job_payload: &[u8],
        attempts: u32,
        checkpoint: &mut BackfillCheckpoint,
        frames_mapped: &mut u64,
        duplicate_frames: &mut u64,
        source_bytes: &mut u64,
        physical_source_bytes: &mut u64,
        reused_source_bytes: &mut u64,
        coalesced_frames: &mut u64,
        acquisition_ids: &mut BTreeSet<u64>,
        cancellation: CancellationToken,
        completes_subscription: bool,
        expected_end_hash: Option<BlockHash>,
    ) -> Result<Option<BlockHash>, RuntimeError> {
        let mut next_number = range.start().0;
        let processor = self.processor.clone();
        let pipeline_budget = self.pipeline_budget.clone();
        let map_cancellation = cancellation.clone();
        let mapped = stream.map(move |item| {
            let validation = item.and_then(|material| {
                material
                    .frame()
                    .validate_shape()
                    .map_err(|error| SourceError::CorruptFrame(error.to_owned()))?;
                Ok(material)
            });
            let processor = processor.clone();
            let pipeline_budget = pipeline_budget.clone();
            let map_cancellation = map_cancellation.clone();
            async move {
                let material = validation?;
                let frame = material.frame();
                for requirement in &processor.descriptor().requirements {
                    requirement
                        .validate_frame(frame)
                        .map_err(|error| ProcessorError::Input(error.to_owned()))?;
                }
                let estimated_bytes = frame.estimated_heap_bytes();
                let finality = frame.finality;
                let _map_task = pipeline_budget.acquire_map_task(&map_cancellation).await?;
                let (delta, equivalent_checksums) =
                    map_with_finality_variants(processor.as_ref(), frame).await?;
                let mapped_byte_permit = pipeline_budget
                    .reserve_mapped(
                        mapped_delta_bytes(&delta, &equivalent_checksums),
                        &map_cancellation,
                    )
                    .await?;
                Ok::<_, RuntimeError>(MappedFrame {
                    delta,
                    equivalent_checksums,
                    finality,
                    estimated_bytes,
                    material: Some(material),
                    _mapped_byte_permit: Some(mapped_byte_permit),
                })
            }
        });
        let mut mapped = Box::pin(mapped.buffered(self.config.mapper_concurrency));
        let mut stream_complete = false;
        while !stream_complete {
            if cancellation.is_cancelled() {
                return Err(RuntimeError::Cancelled);
            }
            let Some(first) = mapped.next().await else {
                break;
            };
            let mut results = vec![first];
            let flush_deadline = tokio::time::sleep(self.config.commit_maximum_delay);
            tokio::pin!(flush_deadline);
            let adaptive_maximum_blocks = self
                .adaptive_commit
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .maximum_blocks();
            while results.len() < adaptive_maximum_blocks {
                tokio::select! {
                    () = cancellation.cancelled() => {
                        return Err(RuntimeError::Cancelled);
                    }
                    () = &mut flush_deadline => break,
                    next = mapped.next() => {
                        if let Some(result) = next {
                            results.push(result);
                        } else {
                            stream_complete = true;
                            break;
                        }
                    }
                }
            }
            let mut mapped_batch = Vec::with_capacity(results.len());
            for result in results {
                let mapped = result?;
                if mapped.delta.block.number.0 != next_number {
                    return Err(RuntimeError::Source(SourceError::CorruptFrame(format!(
                        "chunk expected block {next_number}, received {}",
                        mapped.delta.block.number.0
                    ))));
                }
                if let Some(parent) = expected_parent
                    && mapped.delta.block.parent_hash != parent
                {
                    return Err(RuntimeError::Source(SourceError::CorruptFrame(format!(
                        "parent mismatch at block {}",
                        mapped.delta.block.number.0
                    ))));
                }
                verify_range_end_hash(range, &mapped.delta, expected_end_hash)?;
                next_number = next_number.saturating_add(1);
                expected_parent = Some(mapped.delta.block.hash);
                *frames_mapped = frames_mapped.saturating_add(1);
                *source_bytes = source_bytes.saturating_add(mapped.estimated_bytes);
                let material = mapped
                    .material
                    .as_ref()
                    .expect("historical mapped frames retain material attribution");
                if material.is_physical_source_delivery() {
                    *physical_source_bytes =
                        physical_source_bytes.saturating_add(mapped.estimated_bytes);
                } else {
                    *reused_source_bytes =
                        reused_source_bytes.saturating_add(mapped.estimated_bytes);
                    if material.is_coalesced_delivery() {
                        *coalesced_frames = coalesced_frames.saturating_add(1);
                    }
                }
                if let Some(acquisition_id) = material.acquisition_id() {
                    acquisition_ids.insert(acquisition_id);
                }
                mapped_batch.push(mapped);
            }
            let completes_subscription = completes_subscription
                && mapped_batch
                    .last()
                    .is_some_and(|mapped| mapped.delta.block.number == range.end());
            self.commit_microbatch_with_backpressure(
                mapped_batch,
                job,
                job_payload,
                attempts,
                checkpoint,
                duplicate_frames,
                cancellation.clone(),
                completes_subscription,
            )
            .await?;
        }
        if next_number != range.end().0.saturating_add(1) {
            return Err(RuntimeError::IncompleteChunk {
                expected_through: range.end(),
                next: BlockNumber(next_number),
            });
        }
        Ok(expected_parent)
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn commit_microbatch_with_backpressure(
        &self,
        mapped: Vec<MappedFrame>,
        job: &BackfillJob,
        job_payload: &[u8],
        attempts: u32,
        checkpoint: &mut BackfillCheckpoint,
        duplicate_frames: &mut u64,
        cancellation: CancellationToken,
        completes_subscription: bool,
    ) -> Result<(), RuntimeError> {
        let stream_id = job.delivery_stream_id.as_deref();
        let mut pending = VecDeque::from([(mapped, completes_subscription)]);
        let mut backpressured = false;
        while let Some((mut batch, completes_subscription)) = pending.pop_front() {
            let batch_blocks = u64::try_from(batch.len())
                .map_err(|_| RuntimeError::InvalidConfig("microbatch is too large".to_owned()))?;
            let final_mapped = batch
                .last()
                .expect("ready chunk never yields an empty microbatch");
            let mut proposed_checkpoint = checkpoint.clone();
            proposed_checkpoint.frames_committed = proposed_checkpoint
                .frames_committed
                .saturating_add(batch_blocks);
            proposed_checkpoint.last_block = Some(final_mapped.delta.block.number);
            proposed_checkpoint.last_hash = Some(final_mapped.delta.block.hash);
            let encoded_checkpoint = serde_json::to_vec(&proposed_checkpoint)?;
            let items = batch
                .iter()
                .map(|mapped| HistoricalBatchItem {
                    delta: mapped.delta.clone(),
                    finality: mapped.finality,
                    publish_changes: !matches!(
                        self.processor.descriptor().publication,
                        PublicationPolicy::TerminalOnly
                    ) || mapped.delta.block.number == job.request.range.end(),
                })
                .collect::<Vec<_>>();
            let mode = if job.mode == BackfillMode::Recompute {
                HistoricalBatchMode::RepublishVerifiedCompact
            } else {
                HistoricalBatchMode::Apply
            };
            let limits = HistoricalCommitLimits {
                maximum_changes: self.config.commit_maximum_changes,
                maximum_encoded_bytes: self.config.commit_maximum_encoded_bytes,
            };
            let batch_source_bytes = batch.iter().fold(0_u64, |total, mapped| {
                total.saturating_add(mapped.estimated_bytes)
            });
            let fair_turn = self
                .pipeline_budget
                .acquire_commit_turn(&job.id, &cancellation)
                .await?;
            let external_artifacts = if let Some(sink) = self.artifact_sink.as_ref() {
                if stream_id.is_some() || job.owner != HistoricalJobOwner::Materialization {
                    return Err(RuntimeError::InvalidConfig(
                        "external artifact sinks currently require a materialization job without a delivery stream"
                            .to_owned(),
                    ));
                }
                let deltas = items
                    .iter()
                    .map(|item| item.delta.clone())
                    .collect::<Vec<_>>();
                Some(
                    sink.retain_finalized_batch(self.processor.descriptor(), &deltas)
                        .await?,
                )
            } else {
                None
            };
            let commit = if let Some(stream_id) = stream_id {
                self.store
                    .commit_historical_microbatch(
                        self.processor.as_ref(),
                        mode,
                        &items,
                        &job.sink_ids,
                        stream_id,
                        &job.id,
                        &encoded_checkpoint,
                        attempts,
                        completes_subscription,
                        limits,
                    )
                    .await
            } else {
                self.store
                    .commit_historical_materialization_microbatch(
                        self.processor.as_ref(),
                        &items,
                        &job.id,
                        &encoded_checkpoint,
                        attempts,
                        if external_artifacts.is_some() {
                            HistoricalArtifactTarget::ExternalCommitted
                        } else {
                            HistoricalArtifactTarget::Sqlite
                        },
                        limits,
                    )
                    .await
            };
            match commit {
                Ok(outcome) => {
                    fair_turn.complete(
                        batch_source_bytes
                            .saturating_add(outcome.committed_output_bytes)
                            .saturating_add(
                                external_artifacts
                                    .as_ref()
                                    .map_or(0, |receipt| receipt.logical_bytes),
                            ),
                    );
                    self.adaptive_commit
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .observe(
                            outcome.writer_hold_micros,
                            self.config.commit_maximum_blocks,
                            self.config.commit_target_writer_hold,
                        );
                    if backpressured && !completes_subscription {
                        self.store
                            .set_backfill_subscription_state(
                                &job.id,
                                leani_store_sqlite::BackfillSubscriptionState::Running,
                                None,
                            )
                            .await?;
                        backpressured = false;
                    }
                    for mapped in &batch {
                        mapped
                            .material
                            .as_ref()
                            .expect("historical mapped frame has material")
                            .acknowledge();
                    }
                    *checkpoint = proposed_checkpoint;
                }
                Err(StoreError::HistoricalBatchRequiresFallback)
                    if external_artifacts.is_some() =>
                {
                    return Err(RuntimeError::InvalidConfig(
                        "external artifact batch intersected concurrently committed processor coverage; retry after replanning coverage"
                            .to_owned(),
                    ));
                }
                Err(StoreError::HistoricalBatchRequiresFallback) => {
                    drop(fair_turn);
                    for mapped in &batch {
                        let publish_changes = !matches!(
                            self.processor.descriptor().publication,
                            PublicationPolicy::TerminalOnly
                        ) || mapped.delta.block.number
                            == job.request.range.end();
                        let fair_turn = self
                            .pipeline_budget
                            .acquire_commit_turn(&job.id, &cancellation)
                            .await?;
                        let outcome = self
                            .commit_mapped_frame(
                                mapped,
                                &job.sink_ids,
                                publish_changes,
                                job.mode,
                                stream_id,
                            )
                            .await?;
                        fair_turn.complete(mapped.estimated_bytes.saturating_add(
                            mapped_delta_bytes(&mapped.delta, &mapped.equivalent_checksums),
                        ));
                        match outcome {
                            ApplyOutcome::Applied { .. } => {
                                checkpoint.frames_committed =
                                    checkpoint.frames_committed.saturating_add(1);
                            }
                            ApplyOutcome::AlreadyApplied => {
                                *duplicate_frames = duplicate_frames.saturating_add(1);
                            }
                        }
                        mapped
                            .material
                            .as_ref()
                            .expect("historical mapped frame has material")
                            .acknowledge();
                        checkpoint.last_block = Some(mapped.delta.block.number);
                        checkpoint.last_hash = Some(mapped.delta.block.hash);
                        self.save_checkpoint(
                            job,
                            job_payload,
                            JobState::Running,
                            attempts,
                            checkpoint,
                        )
                        .await?;
                    }
                }
                Err(
                    StoreError::HistoricalBatchLimit { .. }
                    | StoreError::ArtifactStorageLimit { .. }
                    | StoreError::DeliveryLimit {
                        action: DeliveryLimitAction::Pause,
                        ..
                    },
                ) if batch.len() > 1 => {
                    drop(fair_turn);
                    let second_half = batch.split_off(batch.len() / 2);
                    pending.push_front((second_half, completes_subscription));
                    pending.push_front((batch, false));
                }
                Err(StoreError::DeliveryLimit {
                    action: DeliveryLimitAction::Pause,
                    ..
                }) => {
                    drop(fair_turn);
                    if !backpressured {
                        self.store
                            .set_backfill_subscription_state(
                                &job.id,
                                leani_store_sqlite::BackfillSubscriptionState::Backpressured,
                                None,
                            )
                            .await?;
                        backpressured = true;
                    }
                    tokio::select! {
                        () = cancellation.cancelled() => {
                            return Err(RuntimeError::Cancelled);
                        }
                        () = self.store.wait_for_delivery_capacity_change() => {}
                        () = tokio::time::sleep(Duration::from_millis(250)) => {}
                    }
                    pending.push_front((batch, completes_subscription));
                }
                Err(StoreError::ArtifactStorageLimit { .. }) => {
                    drop(fair_turn);
                    if !backpressured {
                        self.store
                            .set_backfill_subscription_state(
                                &job.id,
                                leani_store_sqlite::BackfillSubscriptionState::Backpressured,
                                None,
                            )
                            .await?;
                        backpressured = true;
                    }
                    tokio::select! {
                        () = cancellation.cancelled() => {
                            return Err(RuntimeError::Cancelled);
                        }
                        () = tokio::time::sleep(Duration::from_millis(250)) => {}
                    }
                    pending.push_front((batch, completes_subscription));
                }
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }

    async fn commit_mapped_frame(
        &self,
        mapped: &MappedFrame,
        sink_ids: &[String],
        publish_changes: bool,
        mode: BackfillMode,
        delivery_stream_id: Option<&str>,
    ) -> Result<ApplyOutcome, RuntimeError> {
        if (mode == BackfillMode::Recompute || delivery_stream_id.is_some())
            && let Some(covered_hash) = self
                .store
                .coverage_hash(self.processor.descriptor(), mapped.delta.block.number)
                .await?
        {
            if covered_hash != mapped.delta.block.hash {
                return Err(StoreError::CanonicalConflict {
                    block: mapped.delta.block.number,
                    stored: covered_hash,
                    incoming: mapped.delta.block.hash,
                }
                .into());
            }
            let sequence = self
                .store
                .processor_cursor(self.processor.descriptor())
                .await?
                .map_or(1, |cursor| cursor.sequence.saturating_add(1));
            let cursor = ProcessorCursor {
                processor_id: self.processor.descriptor().id.to_string(),
                processor_version: self.processor.descriptor().version.to_string(),
                chain_id: mapped.delta.chain_id,
                block_number: mapped.delta.block.number,
                block_hash: mapped.delta.block.hash,
                finality: mapped.finality,
                sequence,
            };
            let replay = if let Some(stream_id) = delivery_stream_id {
                self.store
                    .republish_block_local_with_change_publication_to_stream(
                        self.processor.as_ref(),
                        cursor.clone(),
                        &mapped.delta,
                        sink_ids,
                        publish_changes,
                        stream_id,
                    )
                    .await?
            } else {
                if !publish_changes {
                    return Ok(ApplyOutcome::AlreadyApplied);
                }
                self.store
                    .republish_block_local(
                        self.processor.as_ref(),
                        cursor.clone(),
                        &mapped.delta,
                        sink_ids,
                    )
                    .await?
            };
            return Ok(ApplyOutcome::Applied {
                processor_cursor: cursor,
                first_change_sequence: replay.first_change_sequence,
                last_change_sequence: replay.last_change_sequence,
            });
        }
        commit_mapped_delta(
            &self.store,
            self.processor.as_ref(),
            mapped,
            sink_ids,
            publish_changes,
            delivery_stream_id,
        )
        .await
    }

    async fn load_or_create_job(
        &self,
        job: &BackfillJob,
        payload: &[u8],
    ) -> Result<JobRecord, RuntimeError> {
        if let Some(existing) = self.store.job(&job.id).await? {
            if existing.kind != job.owner.job_kind() || existing.payload != payload {
                return Err(RuntimeError::JobIdentity(job.id.clone()));
            }
            return Ok(existing);
        }
        let record = JobRecord {
            id: job.id.clone(),
            kind: job.owner.job_kind().to_owned(),
            state: JobState::Queued,
            payload: payload.to_vec(),
            checkpoint: None,
            attempts: 0,
            updated_at_unix_ms: now_milliseconds(),
        };
        self.store.save_job(&record).await?;
        Ok(record)
    }

    async fn save_checkpoint(
        &self,
        job: &BackfillJob,
        payload: &[u8],
        state: JobState,
        attempts: u32,
        checkpoint: &BackfillCheckpoint,
    ) -> Result<(), RuntimeError> {
        let checkpoint = serde_json::to_vec(checkpoint)?;
        self.store
            .save_job(&JobRecord {
                id: job.id.clone(),
                kind: job.owner.job_kind().to_owned(),
                state,
                payload: payload.to_vec(),
                checkpoint: Some(checkpoint),
                attempts,
                updated_at_unix_ms: now_milliseconds(),
            })
            .await?;
        Ok(())
    }
}

/// Bounded behavior for a live processor lane.
#[derive(Clone, Debug)]
pub struct LiveRuntimeConfig {
    pub max_reorg_depth: usize,
    pub sink_ids: Vec<String>,
}

impl Default for LiveRuntimeConfig {
    fn default() -> Self {
        Self {
            max_reorg_depth: 64,
            sink_ids: Vec::new(),
        }
    }
}

/// Finite report returned when a live source ends or cancellation is
/// requested. Production sources normally run until cancelled.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct LiveReport {
    pub blocks_applied: u64,
    pub blocks_reverted: u64,
    pub duplicates: u64,
    pub disconnects: u64,
    pub last_block: Option<BlockNumber>,
    pub last_hash: Option<BlockHash>,
}

/// Source-neutral live apply/reorg coordinator.
#[derive(Clone)]
pub struct LiveRuntime {
    store: SqliteStore,
    source: Arc<dyn LiveSource>,
    processor: Arc<dyn Processor>,
    config: LiveRuntimeConfig,
}

/// Resource and rollback bounds for one shared live source feeding many
/// processors.
#[derive(Clone, Debug)]
pub struct SharedLiveRuntimeConfig {
    pub max_reorg_depth: usize,
    pub pending_delta_bytes: u64,
    pub sink_ids: Vec<String>,
    /// Optional post-commit event fanout for transports such as Ethereum
    /// WebSocket subscriptions.
    pub committed_events: Option<tokio::sync::broadcast::Sender<ChainEvent>>,
}

impl Default for SharedLiveRuntimeConfig {
    fn default() -> Self {
        Self {
            max_reorg_depth: 64,
            pending_delta_bytes: 512 * 1_024 * 1_024,
            sink_ids: Vec::new(),
            committed_events: None,
        }
    }
}

/// Per-processor accounting for a shared live run.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct SharedProcessorLiveReport {
    pub applied: u64,
    pub reverted: u64,
    pub duplicates: u64,
    pub pending: u64,
}

/// Finite report returned when a shared live source ends.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct SharedLiveReport {
    pub chain_blocks: u64,
    pub reorgs: u64,
    pub disconnects: u64,
    pub last_block: Option<BlockNumber>,
    pub last_hash: Option<BlockHash>,
    pub processors: BTreeMap<String, SharedProcessorLiveReport>,
}

/// Bounded finalized-frame acquisition used when a paused live processor's
/// first unapplied block has aged out of the recent store.
#[async_trait::async_trait]
pub trait FinalizedLiveGapRecovery: std::fmt::Debug + Send + Sync {
    /// Return every finalized frame in `range`, in canonical ascending order.
    async fn recover_chunk(
        &self,
        processor: &ProcessorDescriptor,
        range: BlockRange,
    ) -> Result<Vec<leani_primitives::BlockFrame>, RuntimeError>;
}

#[derive(Debug)]
struct PreparedFrame {
    frame: leani_primitives::BlockFrame,
    deltas: Vec<Option<MappedFrame>>,
}

/// One persistent source subscription fanned out to every configured
/// processor.
#[derive(Clone)]
pub struct SharedLiveRuntime {
    store: SqliteStore,
    source: Arc<dyn LiveSource>,
    processors: Vec<Arc<dyn Processor>>,
    config: SharedLiveRuntimeConfig,
    finalized_gap_recovery: Option<Arc<dyn FinalizedLiveGapRecovery>>,
    unavailable_processors: Arc<StdMutex<HashSet<String>>>,
}

impl std::fmt::Debug for SharedLiveRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SharedLiveRuntime")
            .field("store", &self.store)
            .field("source", &self.source.descriptor())
            .field(
                "processors",
                &self
                    .processors
                    .iter()
                    .map(|processor| processor.descriptor())
                    .collect::<Vec<_>>(),
            )
            .field("config", &self.config)
            .field(
                "finalized_gap_recovery",
                &self.finalized_gap_recovery.as_ref().map(|_| "[AVAILABLE]"),
            )
            .field(
                "unavailable_processors",
                &self
                    .unavailable_processors
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
            )
            .finish()
    }
}

impl SharedLiveRuntime {
    /// Construct a shared live coordinator without opening the source.
    ///
    /// # Errors
    ///
    /// Rejects empty/duplicate processor sets, zero resource bounds, and
    /// unsupported ordered checkpoint starts.
    pub fn new(
        store: SqliteStore,
        source: Arc<dyn LiveSource>,
        processors: Vec<Arc<dyn Processor>>,
        config: SharedLiveRuntimeConfig,
    ) -> Result<Self, RuntimeError> {
        if processors.is_empty() {
            return Err(RuntimeError::InvalidConfig(
                "shared live runtime requires at least one processor".to_owned(),
            ));
        }
        if config.max_reorg_depth == 0 || config.pending_delta_bytes == 0 {
            return Err(RuntimeError::InvalidConfig(
                "shared live reorg and pending-delta bounds must be non-zero".to_owned(),
            ));
        }
        let mut identities = HashSet::new();
        for processor in &processors {
            let descriptor = processor.descriptor();
            if matches!(descriptor.publication, PublicationPolicy::TerminalOnly) {
                return Err(RuntimeError::InvalidConfig(format!(
                    "terminal-only processor {} requires a bounded historical job",
                    descriptor.id
                )));
            }
            let identity = (
                descriptor.id.to_string(),
                descriptor.version.to_string(),
                descriptor.config_hash,
            );
            if !identities.insert(identity) {
                return Err(RuntimeError::InvalidConfig(format!(
                    "processor {} is configured more than once",
                    descriptor.id
                )));
            }
            if descriptor.mode == ReductionMode::OrderedState
                && matches!(descriptor.start, StartPoint::ProcessorCheckpoint(_))
            {
                return Err(RuntimeError::InvalidConfig(format!(
                    "ordered processor {} needs a resolved numeric start",
                    descriptor.id
                )));
            }
        }
        Ok(Self {
            store,
            source,
            processors,
            config,
            finalized_gap_recovery: None,
            unavailable_processors: Arc::new(StdMutex::new(HashSet::new())),
        })
    }

    /// Supply bounded archive/P2P recovery for finalized live gaps that have
    /// aged out of the recent store.
    #[must_use]
    pub fn with_finalized_gap_recovery(
        mut self,
        recovery: Arc<dyn FinalizedLiveGapRecovery>,
    ) -> Self {
        self.finalized_gap_recovery = Some(recovery);
        self
    }

    /// Consume one verified stream, retaining recent frames and mapping them
    /// once per configured processor.
    ///
    /// Ordered processors that have not caught up from their declared start
    /// retain bounded deltas; block-local processors publish immediately.
    ///
    /// # Errors
    ///
    /// Fails closed on invalid frames, resource exhaustion, finalized/deep
    /// reorgs, processor/store failures, or source resets.
    pub async fn run(
        &self,
        start: LiveStart,
        budget: SourceBudget,
        cancellation: CancellationToken,
    ) -> Result<SharedLiveReport, RuntimeError> {
        Box::pin(self.run_inner(start, budget, cancellation, None)).await
    }

    /// Run while publishing whether the live source has a usable
    /// subscription.
    ///
    /// # Errors
    ///
    /// Returns the same fail-closed source, processor, store, and resource
    /// errors as [`Self::run`].
    pub async fn run_with_readiness(
        &self,
        start: LiveStart,
        budget: SourceBudget,
        cancellation: CancellationToken,
        readiness: tokio::sync::watch::Sender<bool>,
    ) -> Result<SharedLiveReport, RuntimeError> {
        Box::pin(self.run_inner(start, budget, cancellation, Some(readiness))).await
    }

    /// Apply every now-contiguous ordered delta retained while historical
    /// processing was behind the live lane.
    ///
    /// Calling this after cold coverage advances makes the hot/cold join
    /// deterministic even when no new head arrives at that exact moment.
    ///
    /// # Errors
    ///
    /// Returns an error for corrupt pending deltas, processor/store failures,
    /// or a non-contiguous ordered reducer transition.
    pub async fn reconcile_pending(&self) -> Result<SharedLiveReport, RuntimeError> {
        let mut report = SharedLiveReport::default();
        for processor in &self.processors {
            self.store
                .register_processor(processor.descriptor())
                .await?;
        }
        Box::pin(self.drain_pending(&mut report)).await?;
        for processor in &self.processors {
            let statistics = self.store.processor_stats(processor.descriptor()).await?;
            report
                .processors
                .entry(processor.descriptor().id.to_string())
                .or_default()
                .pending = statistics.pending_deltas;
        }
        Ok(report)
    }

    #[allow(clippy::too_many_lines)]
    async fn run_inner(
        &self,
        start: LiveStart,
        budget: SourceBudget,
        cancellation: CancellationToken,
        readiness: Option<tokio::sync::watch::Sender<bool>>,
    ) -> Result<SharedLiveReport, RuntimeError> {
        let budget = budget.validate()?;
        for processor in &self.processors {
            self.store
                .register_processor(processor.descriptor())
                .await?;
            let state = self
                .store
                .processor_runtime_state(processor.descriptor())
                .await?;
            let has_gap = self
                .store
                .live_lane_gap(processor.descriptor())
                .await?
                .is_some();
            if state.state != ProcessorRunState::Running || has_gap {
                self.unavailable_processors
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(processor.descriptor().instance.to_string());
            }
        }
        let mut available_processors = {
            let unavailable = self
                .unavailable_processors
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            self.processors
                .iter()
                .filter(|processor| !unavailable.contains(processor.descriptor().instance.as_str()))
                .cloned()
                .collect::<Vec<_>>()
        };
        if available_processors.is_empty() {
            // A parked lane can still be restored by a shallow reorg. Keep
            // enough source material flowing to observe and apply that
            // replacement even when every configured processor is currently
            // paused; ordinary frames remain unmapped while the lane is
            // unavailable.
            available_processors.clone_from(&self.processors);
        }
        let events = self
            .source
            .subscribe(
                compile_live_request(
                    &available_processors,
                    self.source.descriptor().chain_id,
                    &start,
                )?,
                start,
                budget,
                cancellation.clone(),
            )
            .await;
        let mut events = match events {
            Ok(events) => events,
            Err(error) => {
                signal_readiness(readiness.as_ref(), false);
                return Err(error.into());
            }
        };
        signal_readiness(readiness.as_ref(), true);
        let result = Box::pin(async {
            let mut report = SharedLiveReport::default();
            for processor in &self.processors {
                report
                    .processors
                    .entry(processor.descriptor().id.to_string())
                    .or_default();
            }
            loop {
                let event = tokio::select! {
                    () = cancellation.cancelled() => break,
                    () = self.store.wait_for_delivery_capacity_change() => {
                        Box::pin(self.drain_pending(&mut report)).await?;
                        continue;
                    }
                    event = events.next() => event,
                };
                let Some(event) = event else {
                    break;
                };
                match event? {
                    ChainEvent::Block(frame) => {
                        signal_readiness(readiness.as_ref(), true);
                        let frame = *frame;
                        let committed = frame.clone();
                        let prepared = self.prepare_frame(frame, false).await?;
                        Box::pin(self.commit_prepared(prepared, true, &mut report)).await?;
                        self.publish_committed(ChainEvent::Block(Box::new(committed)));
                        report.chain_blocks = report.chain_blocks.saturating_add(1);
                    }
                    ChainEvent::Reorg { reverted, applied } => {
                        signal_readiness(readiness.as_ref(), true);
                        let committed_applied = applied.clone();
                        Box::pin(self.apply_reorg(&reverted, applied, &mut report)).await?;
                        self.publish_committed(ChainEvent::Reorg {
                            reverted,
                            applied: committed_applied,
                        });
                        report.reorgs = report.reorgs.saturating_add(1);
                    }
                    ChainEvent::Disconnected { reason } => {
                        signal_readiness(readiness.as_ref(), false);
                        self.publish_committed(ChainEvent::Disconnected {
                            reason: reason.clone(),
                        });
                        report.disconnects = report.disconnects.saturating_add(1);
                        warn!(%reason, "shared live source reported a transient disconnect");
                    }
                    ChainEvent::Reset { last_valid, reason } => {
                        self.publish_committed(ChainEvent::Reset {
                            last_valid,
                            reason: reason.clone(),
                        });
                        return Err(RuntimeError::LiveReset { last_valid, reason });
                    }
                }
            }
            Box::pin(self.drain_pending(&mut report)).await?;
            for processor in &self.processors {
                let statistics = self.store.processor_stats(processor.descriptor()).await?;
                report
                    .processors
                    .entry(processor.descriptor().id.to_string())
                    .or_default()
                    .pending = statistics.pending_deltas;
            }
            Ok(report)
        })
        .await;
        signal_readiness(readiness.as_ref(), false);
        result
    }

    fn publish_committed(&self, event: ChainEvent) {
        if let Some(events) = &self.config.committed_events {
            let _ = events.send(event);
        }
    }

    async fn processor_live_lane_unavailable(
        &self,
        processor: &dyn Processor,
    ) -> Result<bool, RuntimeError> {
        let instance = processor.descriptor().instance.to_string();
        let known_unavailable = self
            .unavailable_processors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(&instance);
        if !known_unavailable {
            return Ok(false);
        }
        let state = self
            .store
            .processor_runtime_state(processor.descriptor())
            .await?;
        let has_gap = self
            .store
            .live_lane_gap(processor.descriptor())
            .await?
            .is_some();
        if state.state == ProcessorRunState::Running && !has_gap {
            self.unavailable_processors
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&instance);
            return Ok(false);
        }
        Ok(true)
    }

    async fn isolate_live_mapping_failure(
        &self,
        processor: &dyn Processor,
        block: BlockRef,
        error: &ProcessorError,
    ) -> Result<(), RuntimeError> {
        warn!(
            processor_instance = %processor.descriptor().instance,
            block = block.number.0,
            %error,
            "live processor mapping failed; isolating its lane while shared ingestion continues"
        );
        self.store
            .park_processor_live_lane_at(
                processor.descriptor(),
                block,
                "processor_live_mapping_failed",
                true,
            )
            .await?;
        self.unavailable_processors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(processor.descriptor().instance.to_string());
        Ok(())
    }

    async fn prepare_frame(
        &self,
        frame: leani_primitives::BlockFrame,
        map_unavailable: bool,
    ) -> Result<PreparedFrame, RuntimeError> {
        frame
            .validate_shape()
            .map_err(|error| RuntimeError::InvalidFrame(error.to_owned()))?;
        if frame.chain_id != self.source.descriptor().chain_id {
            return Err(RuntimeError::InvalidFrame(
                "live frame and source chains differ".to_owned(),
            ));
        }
        let mut deltas = Vec::with_capacity(self.processors.len());
        for processor in &self.processors {
            if !map_unavailable
                && self
                    .processor_live_lane_unavailable(processor.as_ref())
                    .await?
            {
                deltas.push(None);
                continue;
            }
            let mapped = async {
                for requirement in &processor.descriptor().requirements {
                    requirement
                        .validate_frame(&frame)
                        .map_err(|error| ProcessorError::Input(error.to_owned()))?;
                }
                map_with_finality_variants(processor.as_ref(), &frame).await
            }
            .await;
            match mapped {
                Ok((delta, equivalent_checksums)) => deltas.push(Some(MappedFrame {
                    delta,
                    equivalent_checksums,
                    finality: frame.finality,
                    estimated_bytes: 0,
                    material: None,
                    _mapped_byte_permit: None,
                })),
                Err(error) => {
                    self.isolate_live_mapping_failure(processor.as_ref(), frame.block, &error)
                        .await?;
                    deltas.push(None);
                }
            }
        }
        Ok(PreparedFrame { frame, deltas })
    }

    async fn commit_prepared(
        &self,
        prepared: PreparedFrame,
        retain_recent: bool,
        report: &mut SharedLiveReport,
    ) -> Result<(), RuntimeError> {
        if retain_recent {
            self.store.store_recent_frame(&prepared.frame).await?;
        }
        for (processor, mapped) in self.processors.iter().zip(&prepared.deltas) {
            let Some(mapped) = mapped else {
                continue;
            };
            let isolated_block_local = processor.descriptor().mode == ReductionMode::BlockLocal
                && processor.descriptor().delivery_ordering
                    == DeliveryOrdering::BlockVersionedIdempotent;
            if isolated_block_local {
                let state = self
                    .store
                    .processor_runtime_state(processor.descriptor())
                    .await?;
                if state.state == ProcessorRunState::Failed
                    || self
                        .store
                        .live_lane_gap(processor.descriptor())
                        .await?
                        .is_some()
                {
                    continue;
                }
            }
            let outcome = if processor.descriptor().mode == ReductionMode::BlockLocal {
                match commit_mapped_delta(
                    &self.store,
                    processor.as_ref(),
                    mapped,
                    &self.config.sink_ids,
                    true,
                    None,
                )
                .await
                {
                    Ok(outcome) => Some(outcome),
                    Err(error) if isolated_block_local && live_lane_isolatable_error(&error) => {
                        self.park_live_lane(processor.as_ref(), mapped, &error)
                            .await?;
                        None
                    }
                    Err(error) => return Err(error),
                }
            } else {
                match self.commit_ordered_mapped(processor.as_ref(), mapped).await {
                    Ok(outcome) => outcome,
                    Err(error) if live_lane_isolatable_error(&error) => {
                        self.park_live_lane(processor.as_ref(), mapped, &error)
                            .await?;
                        None
                    }
                    Err(error) => return Err(error),
                }
            };
            if let Some(outcome) = outcome {
                record_apply(
                    report
                        .processors
                        .entry(processor.descriptor().id.to_string())
                        .or_default(),
                    &outcome,
                );
            }
        }
        report.last_block = Some(prepared.frame.block.number);
        report.last_hash = Some(prepared.frame.block.hash);
        Box::pin(self.drain_pending(report)).await
    }

    async fn park_live_lane(
        &self,
        processor: &dyn Processor,
        mapped: &MappedFrame,
        error: &RuntimeError,
    ) -> Result<(), RuntimeError> {
        let encoded_bytes = u64::try_from(mapped.delta.encode_durable()?.len()).unwrap_or(u64::MAX);
        let mut current = 0_u64;
        for configured in &self.processors {
            current = current.saturating_add(
                self.store
                    .processor_stats(configured.descriptor())
                    .await?
                    .pending_delta_bytes,
            );
        }
        let persist_delta = encoded_bytes <= self.config.pending_delta_bytes
            && current.saturating_add(encoded_bytes) <= self.config.pending_delta_bytes;
        let (reason, required_delivery_bytes, failed) = match error {
            RuntimeError::Store(StoreError::DeliveryItemTooLarge { observed_bytes, .. }) => {
                ("single_block_exceeds_delivery_limit", *observed_bytes, true)
            }
            RuntimeError::Store(StoreError::DeliveryLimit { action, .. }) => (
                "delivery_spool_hard_limit",
                0,
                matches!(action, DeliveryLimitAction::Fail),
            ),
            RuntimeError::Store(StoreError::PhysicalStorageLimit { .. }) => {
                ("physical_store_hard_limit", 0, false)
            }
            RuntimeError::Store(StoreError::ArtifactStorageLimit { .. }) => {
                ("artifact_store_hard_limit", 0, false)
            }
            RuntimeError::Store(StoreError::ProcessorFailed(_)) => {
                ("processor_live_lane_failed", 0, true)
            }
            RuntimeError::Store(StoreError::ProcessorPaused { .. }) => {
                ("delivery_spool_hard_limit", 0, false)
            }
            RuntimeError::Processor(_) => ("processor_live_reduce_failed", 0, true),
            _ => {
                return Err(RuntimeError::InvalidConfig(format!(
                    "attempted to isolate unsupported live-lane error: {error}"
                )));
            }
        };
        let (reason, failed) = if persist_delta {
            (reason, failed)
        } else {
            ("live_gap_marker_exceeds_pending_delta_budget", true)
        };
        self.store
            .park_processor_live_lane(
                processor.descriptor(),
                &mapped.delta,
                reason,
                required_delivery_bytes,
                failed,
                persist_delta,
            )
            .await?;
        self.unavailable_processors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(processor.descriptor().instance.to_string());
        warn!(
            processor_instance = %processor.descriptor().instance,
            first_unapplied_block = mapped.delta.block.number.0,
            persist_delta,
            %error,
            "paused one live processor lane; shared ingestion and other processors continue"
        );
        Ok(())
    }

    async fn commit_ordered_mapped(
        &self,
        processor: &dyn Processor,
        mapped: &MappedFrame,
    ) -> Result<Option<ApplyOutcome>, RuntimeError> {
        if accept_existing_delta_variant(&self.store, processor, mapped).await? {
            return Ok(Some(ApplyOutcome::AlreadyApplied));
        }
        match self
            .apply_ordered_if_ready(processor, &mapped.delta, mapped.finality)
            .await
        {
            Ok(Some(outcome)) => return Ok(Some(outcome)),
            Ok(None) => {}
            Err(error @ RuntimeError::Store(StoreError::ConflictingApply { .. })) => {
                if accept_existing_delta_variant(&self.store, processor, mapped).await? {
                    return Ok(Some(ApplyOutcome::AlreadyApplied));
                }
                return Err(error);
            }
            Err(error) => return Err(error),
        }
        let encoded_bytes = u64::try_from(mapped.delta.encode_durable()?.len()).unwrap_or(u64::MAX);
        let mut pending_bytes = 0_u64;
        for configured in &self.processors {
            pending_bytes = pending_bytes.saturating_add(
                self.store
                    .processor_stats(configured.descriptor())
                    .await?
                    .pending_delta_bytes,
            );
        }
        if encoded_bytes > self.config.pending_delta_bytes
            || pending_bytes.saturating_add(encoded_bytes) > self.config.pending_delta_bytes
        {
            let failed = encoded_bytes > self.config.pending_delta_bytes;
            let reason = if failed {
                "single_delta_exceeds_pending_delta_limit"
            } else {
                "pending_delta_hard_limit"
            };
            self.store
                .park_processor_live_lane_at(
                    processor.descriptor(),
                    mapped.delta.block,
                    reason,
                    failed,
                )
                .await?;
            self.unavailable_processors
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(processor.descriptor().instance.to_string());
            warn!(
                processor_instance = %processor.descriptor().instance,
                first_unapplied_block = mapped.delta.block.number.0,
                pending_bytes,
                encoded_bytes,
                limit = self.config.pending_delta_bytes,
                reason,
                "parked one ordered processor at its bounded pending-delta limit"
            );
            return Ok(None);
        }
        if let Err(error) = self
            .store
            .persist_delta(processor.descriptor(), &mapped.delta)
            .await
        {
            if matches!(error, StoreError::ConflictingPendingDelta(_)) {
                tokio::task::yield_now().await;
                if accept_existing_delta_variant(&self.store, processor, mapped).await? {
                    return Ok(Some(ApplyOutcome::AlreadyApplied));
                }
            }
            return Err(error.into());
        }
        Ok(None)
    }

    async fn apply_delta(
        &self,
        processor: &dyn Processor,
        delta: &EncodedDelta,
        finality: Finality,
    ) -> Result<ApplyOutcome, RuntimeError> {
        let prior = self.store.processor_cursor(processor.descriptor()).await?;
        let cursor = ProcessorCursor {
            processor_id: processor.descriptor().id.to_string(),
            processor_version: processor.descriptor().version.to_string(),
            chain_id: delta.chain_id,
            block_number: delta.block.number,
            block_hash: delta.block.hash,
            finality,
            sequence: prior.map_or(1, |cursor| cursor.sequence.saturating_add(1)),
        };
        self.store
            .apply(processor, cursor, delta, &self.config.sink_ids)
            .await
            .map_err(Into::into)
    }

    async fn apply_recovered_delta(
        &self,
        processor: &dyn Processor,
        delta: &EncodedDelta,
    ) -> Result<ApplyOutcome, RuntimeError> {
        let prior = self.store.processor_cursor(processor.descriptor()).await?;
        let cursor = ProcessorCursor {
            processor_id: processor.descriptor().id.to_string(),
            processor_version: processor.descriptor().version.to_string(),
            chain_id: delta.chain_id,
            block_number: delta.block.number,
            block_hash: delta.block.hash,
            finality: Finality::Finalized,
            sequence: prior.map_or(1, |cursor| cursor.sequence.saturating_add(1)),
        };
        self.store
            .apply_live_recovery(processor, cursor, delta, &self.config.sink_ids)
            .await
            .map_err(Into::into)
    }

    async fn advance_or_complete_live_gap(
        &self,
        processor: &dyn Processor,
        applied: BlockRef,
    ) -> Result<(), RuntimeError> {
        let chain_id = self.source.descriptor().chain_id;
        let next_number = BlockNumber(applied.number.0.saturating_add(1));
        if let Some((next, _)) = self.store.canonical_block(chain_id, next_number).await? {
            if next.parent_hash != applied.hash {
                return Err(RuntimeError::InvalidReorg(format!(
                    "canonical live gap successor {} does not descend from block {}",
                    next.number.0, applied.number.0
                )));
            }
            self.store
                .advance_live_lane_gap(processor.descriptor(), applied, next)
                .await?;
        } else {
            self.store
                .complete_live_lane_gap(processor.descriptor(), applied)
                .await?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    async fn recover_finalized_live_gap(
        &self,
        processor: &dyn Processor,
        first: BlockRef,
        report: &mut SharedLiveReport,
    ) -> Result<bool, RuntimeError> {
        let chain_id = self.source.descriptor().chain_id;
        let Some((canonical, finality)) =
            self.store.canonical_block(chain_id, first.number).await?
        else {
            self.store
                .complete_live_lane_gap(processor.descriptor(), first)
                .await?;
            return Ok(true);
        };
        if canonical.hash != first.hash || canonical.parent_hash != first.parent_hash {
            self.store
                .fail_processor_live_lane(
                    processor.descriptor(),
                    "live_gap_canonical_identity_changed",
                )
                .await?;
            return Ok(false);
        }
        if finality != Finality::Finalized {
            self.store
                .fail_processor_live_lane(processor.descriptor(), "unfinalized_gap_unrecoverable")
                .await?;
            return Ok(false);
        }
        let Some(recovery) = &self.finalized_gap_recovery else {
            self.store
                .pause_processor_live_lane(
                    processor.descriptor(),
                    "finalized_gap_waiting_for_history_source",
                )
                .await?;
            return Ok(false);
        };
        let finalized_head = self
            .store
            .finalized_canonical_head(chain_id)
            .await?
            .ok_or_else(|| {
                RuntimeError::InvalidConfig(
                    "finalized live gap has no canonical finalized head".to_owned(),
                )
            })?;
        let through = BlockNumber(
            first
                .number
                .0
                .saturating_add(127)
                .min(finalized_head.number.0),
        );
        let range = BlockRange::new(first.number, through)
            .map_err(|error| RuntimeError::InvalidConfig(error.to_string()))?;
        let frames = match recovery.recover_chunk(processor.descriptor(), range).await {
            Ok(frames) => frames,
            Err(RuntimeError::Cancelled | RuntimeError::Source(SourceError::Cancelled)) => {
                return Err(RuntimeError::Cancelled);
            }
            Err(error) if live_gap_recovery_retryable_error(&error) => {
                warn!(
                    processor_instance = %processor.descriptor().instance,
                    from_block = range.start().0,
                    through_block = range.end().0,
                    %error,
                    "finalized live-gap recovery is temporarily unavailable; keeping the processor lane paused"
                );
                self.store
                    .pause_processor_live_lane(
                        processor.descriptor(),
                        "finalized_gap_recovery_unavailable",
                    )
                    .await?;
                return Ok(false);
            }
            Err(error) => {
                warn!(
                    processor_instance = %processor.descriptor().instance,
                    from_block = range.start().0,
                    through_block = range.end().0,
                    %error,
                    "finalized live-gap recovery failed validation; isolating the processor lane"
                );
                self.store
                    .fail_processor_live_lane(
                        processor.descriptor(),
                        "finalized_gap_recovery_failed",
                    )
                    .await?;
                return Ok(false);
            }
        };
        if u64::try_from(frames.len()).unwrap_or(u64::MAX) != range.len() {
            warn!(
                processor_instance = %processor.descriptor().instance,
                from_block = range.start().0,
                through_block = range.end().0,
                recovered_frames = frames.len(),
                "finalized live-gap recovery returned an incomplete chunk; keeping the processor lane paused"
            );
            self.store
                .pause_processor_live_lane(
                    processor.descriptor(),
                    "finalized_gap_recovery_incomplete",
                )
                .await?;
            return Ok(false);
        }
        let mut expected = first;
        for frame in frames {
            if let Err(error) = frame.validate_shape() {
                warn!(
                    processor_instance = %processor.descriptor().instance,
                    block = expected.number.0,
                    %error,
                    "finalized live-gap recovery returned a malformed frame; isolating the processor lane"
                );
                self.store
                    .fail_processor_live_lane(
                        processor.descriptor(),
                        "finalized_gap_recovery_invalid_frame",
                    )
                    .await?;
                return Ok(false);
            }
            if frame.chain_id != chain_id
                || frame.finality != Finality::Finalized
                || frame.block.number != expected.number
                || frame.block.hash != expected.hash
                || frame.block.parent_hash != expected.parent_hash
            {
                warn!(
                    processor_instance = %processor.descriptor().instance,
                    expected_block = expected.number.0,
                    received_block = frame.block.number.0,
                    "finalized live-gap recovery diverged from the durable canonical identity; isolating the processor lane"
                );
                self.store
                    .fail_processor_live_lane(
                        processor.descriptor(),
                        "finalized_gap_recovery_canonical_mismatch",
                    )
                    .await?;
                return Ok(false);
            }
            for requirement in &processor.descriptor().requirements {
                if let Err(error) = requirement.validate_frame(&frame) {
                    warn!(
                        processor_instance = %processor.descriptor().instance,
                        block = frame.block.number.0,
                        %error,
                        "finalized live-gap recovery cannot satisfy the processor input contract; isolating the processor lane"
                    );
                    self.store
                        .fail_processor_live_lane(
                            processor.descriptor(),
                            "finalized_gap_recovery_missing_material",
                        )
                        .await?;
                    return Ok(false);
                }
            }
            let (delta, equivalent_checksums) = match map_with_finality_variants(processor, &frame)
                .await
            {
                Ok(mapped) => mapped,
                Err(error) => {
                    warn!(
                        processor_instance = %processor.descriptor().instance,
                        block = frame.block.number.0,
                        %error,
                        "processor mapping failed during finalized live-gap recovery; isolating the processor lane"
                    );
                    self.store
                        .fail_processor_live_lane(
                            processor.descriptor(),
                            "finalized_gap_recovery_processor_failed",
                        )
                        .await?;
                    return Ok(false);
                }
            };
            if let Some(pending) = self
                .store
                .pending_deltas(processor.descriptor(), frame.block.number, 1)
                .await?
                .into_iter()
                .find(|pending| pending.block == frame.block)
                && pending.checksum != delta.checksum
                && !equivalent_checksums.contains(&pending.checksum)
            {
                self.store
                    .fail_processor_live_lane(
                        processor.descriptor(),
                        "finalized_gap_recovery_pending_conflict",
                    )
                    .await?;
                return Ok(false);
            }
            let outcome = match self.apply_recovered_delta(processor, &delta).await {
                Ok(outcome) => outcome,
                Err(error) if live_lane_isolatable_error(&error) => return Ok(false),
                Err(error) => return Err(error),
            };
            record_apply(
                report
                    .processors
                    .entry(processor.descriptor().id.to_string())
                    .or_default(),
                &outcome,
            );
            self.advance_or_complete_live_gap(processor, frame.block)
                .await?;
            let Some(gap) = self.store.live_lane_gap(processor.descriptor()).await? else {
                return Ok(true);
            };
            expected = gap.first_unapplied;
        }
        Ok(true)
    }

    async fn apply_ordered_if_ready(
        &self,
        processor: &dyn Processor,
        delta: &EncodedDelta,
        finality: Finality,
    ) -> Result<Option<ApplyOutcome>, RuntimeError> {
        let prior = self.store.processor_cursor(processor.descriptor()).await?;
        let ready = if let Some(prior) = &prior {
            delta.block.number.0 == prior.block_number.0.saturating_add(1)
                && delta.block.parent_hash == prior.block_hash
        } else {
            delta.block.number == processor_start(processor)?
        };
        if !ready {
            return Ok(None);
        }
        self.apply_delta(processor, delta, finality).await.map(Some)
    }

    #[allow(clippy::too_many_lines)]
    async fn drain_pending(&self, report: &mut SharedLiveReport) -> Result<(), RuntimeError> {
        for processor in &self.processors {
            if processor.descriptor().mode == ReductionMode::BlockLocal {
                loop {
                    let state = self
                        .store
                        .processor_runtime_state(processor.descriptor())
                        .await?;
                    if state.state == ProcessorRunState::Failed {
                        break;
                    }
                    let Some(gap) = self.store.live_lane_gap(processor.descriptor()).await? else {
                        // Pending block-local deltas without a live gap are
                        // stale crash/reconciliation artifacts only.
                        let candidates = self
                            .store
                            .pending_deltas(
                                processor.descriptor(),
                                processor_start(processor.as_ref())?,
                                64,
                            )
                            .await?;
                        if let Some(delta) = candidates.into_iter().next()
                            && self
                                .clear_applied_pending_variant(processor.as_ref(), &delta, report)
                                .await?
                        {
                            continue;
                        }
                        break;
                    };
                    let candidates = self
                        .store
                        .pending_deltas(processor.descriptor(), gap.first_unapplied.number, 64)
                        .await?;
                    if let Some(delta) = candidates
                        .into_iter()
                        .find(|delta| delta.block == gap.first_unapplied)
                    {
                        if self
                            .clear_applied_pending_variant(processor.as_ref(), &delta, report)
                            .await?
                        {
                            self.advance_or_complete_live_gap(
                                processor.as_ref(),
                                gap.first_unapplied,
                            )
                            .await?;
                            continue;
                        }
                        if let Some(frame) = self
                            .store
                            .recent_frame(delta.chain_id, delta.block.number)
                            .await?
                            .filter(|frame| frame.block == delta.block)
                        {
                            let outcome = match if frame.finality == Finality::Finalized {
                                self.apply_recovered_delta(processor.as_ref(), &delta).await
                            } else {
                                self.apply_delta(processor.as_ref(), &delta, frame.finality)
                                    .await
                            } {
                                Ok(outcome) => outcome,
                                Err(error) if live_lane_isolatable_error(&error) => break,
                                Err(error) => return Err(error),
                            };
                            record_apply(
                                report
                                    .processors
                                    .entry(processor.descriptor().id.to_string())
                                    .or_default(),
                                &outcome,
                            );
                            self.advance_or_complete_live_gap(processor.as_ref(), frame.block)
                                .await?;
                            continue;
                        }
                    }

                    if let Some(frame) = self
                        .store
                        .recent_frame(
                            self.source.descriptor().chain_id,
                            gap.first_unapplied.number,
                        )
                        .await?
                        .filter(|frame| frame.block == gap.first_unapplied)
                    {
                        for requirement in &processor.descriptor().requirements {
                            requirement
                                .validate_frame(&frame)
                                .map_err(|error| ProcessorError::Input(error.to_owned()))?;
                        }
                        let (delta, _) =
                            map_with_finality_variants(processor.as_ref(), &frame).await?;
                        let outcome = match if frame.finality == Finality::Finalized {
                            self.apply_recovered_delta(processor.as_ref(), &delta).await
                        } else {
                            self.apply_delta(processor.as_ref(), &delta, frame.finality)
                                .await
                        } {
                            Ok(outcome) => outcome,
                            Err(error) if live_lane_isolatable_error(&error) => break,
                            Err(error) => return Err(error),
                        };
                        record_apply(
                            report
                                .processors
                                .entry(processor.descriptor().id.to_string())
                                .or_default(),
                            &outcome,
                        );
                        self.advance_or_complete_live_gap(processor.as_ref(), frame.block)
                            .await?;
                        continue;
                    }
                    if !self
                        .recover_finalized_live_gap(processor.as_ref(), gap.first_unapplied, report)
                        .await?
                    {
                        break;
                    }
                }
                continue;
            }
            loop {
                if self
                    .store
                    .processor_runtime_state(processor.descriptor())
                    .await?
                    .state
                    == ProcessorRunState::Failed
                {
                    break;
                }
                let prior = self.store.processor_cursor(processor.descriptor()).await?;
                let expected = prior.as_ref().map_or_else(
                    || processor_start(processor.as_ref()),
                    |cursor| Ok(BlockNumber(cursor.block_number.0.saturating_add(1))),
                )?;
                // A restarted live overlap can persist deltas that are already
                // committed below the current cursor. Re-apply those exact
                // identities first: the store verifies their checksum and
                // atomically removes the stale pending row.
                let candidates = self
                    .store
                    .pending_deltas(
                        processor.descriptor(),
                        processor_start(processor.as_ref())?,
                        64,
                    )
                    .await?;
                if let Some(delta) = candidates
                    .iter()
                    .find(|delta| delta.block.number < expected)
                {
                    if !self
                        .clear_applied_pending_variant(processor.as_ref(), delta, report)
                        .await?
                    {
                        return Err(StoreError::ConflictingApply {
                            block: delta.block.number,
                        }
                        .into());
                    }
                    continue;
                }
                let candidate = candidates.into_iter().find(|delta| {
                    delta.block.number == expected
                        && prior
                            .as_ref()
                            .is_none_or(|cursor| delta.block.parent_hash == cursor.block_hash)
                });
                if let Some(delta) = candidate {
                    let outcome = match self
                        .apply_delta(processor.as_ref(), &delta, Finality::Included)
                        .await
                    {
                        Ok(outcome) => outcome,
                        Err(error) if live_lane_isolatable_error(&error) => break,
                        Err(error) => return Err(error),
                    };
                    record_apply(
                        report
                            .processors
                            .entry(processor.descriptor().id.to_string())
                            .or_default(),
                        &outcome,
                    );
                    if let Some(gap) = self.store.live_lane_gap(processor.descriptor()).await?
                        && gap.first_unapplied == delta.block
                    {
                        self.advance_or_complete_live_gap(processor.as_ref(), delta.block)
                            .await?;
                    }
                    continue;
                }

                let Some(gap) = self.store.live_lane_gap(processor.descriptor()).await? else {
                    break;
                };
                if gap.first_unapplied.number != expected
                    || prior
                        .as_ref()
                        .is_some_and(|cursor| gap.first_unapplied.parent_hash != cursor.block_hash)
                {
                    break;
                }
                if let Some(frame) = self
                    .store
                    .recent_frame(
                        self.source.descriptor().chain_id,
                        gap.first_unapplied.number,
                    )
                    .await?
                    .filter(|frame| frame.block == gap.first_unapplied)
                {
                    let mapped = async {
                        for requirement in &processor.descriptor().requirements {
                            requirement
                                .validate_frame(&frame)
                                .map_err(|error| ProcessorError::Input(error.to_owned()))?;
                        }
                        map_with_finality_variants(processor.as_ref(), &frame).await
                    }
                    .await;
                    let (delta, _) = match mapped {
                        Ok(mapped) => mapped,
                        Err(error) => {
                            self.isolate_live_mapping_failure(
                                processor.as_ref(),
                                frame.block,
                                &error,
                            )
                            .await?;
                            break;
                        }
                    };
                    let outcome = match if frame.finality == Finality::Finalized {
                        self.apply_recovered_delta(processor.as_ref(), &delta).await
                    } else {
                        self.apply_delta(processor.as_ref(), &delta, frame.finality)
                            .await
                    } {
                        Ok(outcome) => outcome,
                        Err(error) if live_lane_isolatable_error(&error) => break,
                        Err(error) => return Err(error),
                    };
                    record_apply(
                        report
                            .processors
                            .entry(processor.descriptor().id.to_string())
                            .or_default(),
                        &outcome,
                    );
                    self.advance_or_complete_live_gap(processor.as_ref(), frame.block)
                        .await?;
                    continue;
                }
                if !self
                    .recover_finalized_live_gap(processor.as_ref(), gap.first_unapplied, report)
                    .await?
                {
                    break;
                }
            }
        }
        Ok(())
    }

    async fn clear_applied_pending_variant(
        &self,
        processor: &dyn Processor,
        delta: &EncodedDelta,
        report: &mut SharedLiveReport,
    ) -> Result<bool, RuntimeError> {
        let Some(stored_checksum) = self
            .store
            .applied_delta_checksum(processor.descriptor(), delta.block.number, delta.block.hash)
            .await?
        else {
            return Ok(false);
        };
        let outcome = if stored_checksum == delta.checksum {
            self.apply_delta(processor, delta, Finality::Included)
                .await?
        } else {
            let mut equivalent_checksums = processor.finality_variant_checksums(delta)?;
            if !equivalent_checksums.contains(&delta.checksum) {
                return Err(ProcessorError::Invariant(
                    "finality variants must include the exact delta checksum".to_owned(),
                )
                .into());
            }
            let recent = self
                .store
                .recent_frame(delta.chain_id, delta.block.number)
                .await?
                .filter(|frame| frame.block == delta.block);
            if !equivalent_checksums.contains(&stored_checksum)
                && let Some(frame) = &recent
            {
                let (_, mapped_checksums) = map_with_finality_variants(processor, frame).await?;
                equivalent_checksums.extend(mapped_checksums);
                equivalent_checksums.sort_unstable();
                equivalent_checksums.dedup();
            }
            if !equivalent_checksums.contains(&stored_checksum) {
                return Err(StoreError::ConflictingApply {
                    block: delta.block.number,
                }
                .into());
            }
            self.store
                .delete_pending_delta(processor.descriptor(), delta.block)
                .await?;
            if recent.is_some_and(|frame| frame.finality == Finality::Finalized) {
                self.store
                    .mark_finalized(processor.descriptor(), delta.block.number)
                    .await?;
            }
            ApplyOutcome::AlreadyApplied
        };
        record_apply(
            report
                .processors
                .entry(processor.descriptor().id.to_string())
                .or_default(),
            &outcome,
        );
        Ok(true)
    }

    async fn apply_reorg(
        &self,
        reverted: &[leani_primitives::BlockRef],
        applied: Vec<leani_primitives::BlockFrame>,
        report: &mut SharedLiveReport,
    ) -> Result<(), RuntimeError> {
        if reverted.is_empty() || reverted.len() > self.config.max_reorg_depth {
            return Err(RuntimeError::InvalidReorg(format!(
                "reverted depth {} is outside 1..={}",
                reverted.len(),
                self.config.max_reorg_depth
            )));
        }
        let mut prepared = Vec::with_capacity(applied.len());
        for frame in applied {
            prepared.push(self.prepare_frame(frame, true).await?);
        }
        for processor in &self.processors {
            if let Some(finalized) = self.store.finalized_through(processor.descriptor()).await?
                && reverted.iter().any(|block| block.number <= finalized)
            {
                return Err(RuntimeError::InvalidReorg(format!(
                    "reorg crosses finalized processor {} at block {}",
                    processor.descriptor().id,
                    finalized.0
                )));
            }
        }
        let chain_id = self.source.descriptor().chain_id;
        self.store
            .reorg_recent_frames(
                chain_id,
                reverted,
                &prepared
                    .iter()
                    .map(|prepared| prepared.frame.clone())
                    .collect::<Vec<_>>(),
            )
            .await?;
        for (processor_index, processor) in self.processors.iter().enumerate() {
            if let Some(gap) = self.store.live_lane_gap(processor.descriptor()).await?
                && let Some(reverted_gap) =
                    reverted.iter().find(|block| **block == gap.first_unapplied)
                && let Some(replacement) = prepared
                    .iter()
                    .find(|prepared| prepared.frame.block.number == reverted_gap.number)
                && let Some(replacement_delta) = &replacement.deltas[processor_index]
            {
                self.store
                    .rebase_live_lane_gap(
                        processor.descriptor(),
                        *reverted_gap,
                        &replacement_delta.delta,
                    )
                    .await?;
            }
            for block in reverted {
                self.store
                    .delete_pending_delta(processor.descriptor(), *block)
                    .await?;
                if self
                    .store
                    .coverage_block_by_hash(processor.descriptor(), block.hash)
                    .await?
                    .is_some()
                {
                    self.store
                        .undo(
                            processor.descriptor(),
                            chain_id,
                            block.number,
                            block.hash,
                            &self.config.sink_ids,
                        )
                        .await?;
                    let processor_report = report
                        .processors
                        .entry(processor.descriptor().id.to_string())
                        .or_default();
                    processor_report.reverted = processor_report.reverted.saturating_add(1);
                }
            }
        }
        for frame in prepared {
            Box::pin(self.commit_prepared(frame, false, report)).await?;
            report.chain_blocks = report.chain_blocks.saturating_add(1);
        }
        Ok(())
    }
}

fn live_lane_isolatable_error(error: &RuntimeError) -> bool {
    matches!(
        error,
        RuntimeError::Processor(_)
            | RuntimeError::Store(
                StoreError::DeliveryLimit { .. }
                    | StoreError::ProcessorPaused { .. }
                    | StoreError::ProcessorFailed(_)
                    | StoreError::DeliveryItemTooLarge { .. }
                    | StoreError::PhysicalStorageLimit { .. }
                    | StoreError::ArtifactStorageLimit { .. }
            )
    )
}

fn live_gap_recovery_retryable_error(error: &RuntimeError) -> bool {
    matches!(
        error,
        RuntimeError::IncompleteChunk { .. }
            | RuntimeError::Source(
                SourceError::MissingRange(_)
                    | SourceError::MissingMaterial { .. }
                    | SourceError::Disconnected(_)
                    | SourceError::Unavailable(_)
            )
    )
}

fn processor_start(processor: &dyn Processor) -> Result<BlockNumber, RuntimeError> {
    match &processor.descriptor().start {
        StartPoint::Genesis => Ok(BlockNumber(0)),
        StartPoint::Block(block) => Ok(*block),
        StartPoint::ProcessorCheckpoint(checkpoint) => Err(RuntimeError::InvalidConfig(format!(
            "processor {} checkpoint {checkpoint} was not resolved",
            processor.descriptor().id
        ))),
    }
}

fn record_apply(report: &mut SharedProcessorLiveReport, outcome: &ApplyOutcome) {
    match outcome {
        ApplyOutcome::Applied { .. } => report.applied = report.applied.saturating_add(1),
        ApplyOutcome::AlreadyApplied => {
            report.duplicates = report.duplicates.saturating_add(1);
        }
    }
}

fn compile_live_request(
    processors: &[Arc<dyn Processor>],
    chain_id: leani_primitives::ChainId,
    start: &LiveStart,
) -> Result<DataRequest, RuntimeError> {
    let range = match start {
        LiveStart::Head => BlockRange::single(BlockNumber(0)),
        LiveStart::Block(block) => BlockRange::single(block.number),
        LiveStart::AnchoredOverlap {
            anchor,
            overlap_blocks,
        } => BlockRange::new(
            BlockNumber(
                anchor
                    .number
                    .0
                    .saturating_sub(overlap_blocks.saturating_sub(1)),
            ),
            anchor.number,
        )
        .map_err(|error| RuntimeError::InvalidConfig(error.to_string()))?,
        LiveStart::RetainedCanonical { canonical } => {
            let first = canonical.first().ok_or_else(|| {
                RuntimeError::InvalidConfig(
                    "retained live start requires a non-empty canonical suffix".to_owned(),
                )
            })?;
            let last = canonical.last().expect("non-empty canonical suffix");
            BlockRange::new(first.number, last.number)
                .map_err(|error| RuntimeError::InvalidConfig(error.to_string()))?
        }
        LiveStart::Cursor(cursor) => BlockRange::single(cursor.block_number),
    };
    let requirements = processors
        .iter()
        .flat_map(|processor| &processor.descriptor().requirements)
        .collect::<Vec<_>>();
    requirements.first().copied().ok_or_else(|| {
        RuntimeError::InvalidConfig("live processors require at least one data requirement".into())
    })?;
    let required = requirements
        .iter()
        .fold(CapabilitySet::NONE, |all, requirement| {
            all.union(requirement.capabilities)
        });
    let log_fields = requirements
        .iter()
        .fold(LogFieldSet::NONE, |all, requirement| {
            all.union(requirement.log_fields)
        });
    let minimum_finality = requirements
        .iter()
        .fold(Finality::Included, |all, requirement| {
            all.max(requirement.minimum_finality)
        });
    let allow_filtered = requirements
        .iter()
        .all(|requirement| requirement.allow_filtered);
    let filters = if allow_filtered {
        let mut scope = leani_primitives::FilterScope::default();
        for requirement in requirements {
            union_filter_scope(&mut scope, &requirement.filter);
        }
        leani_source_api::FilterSet {
            senders: scope.senders.clone(),
            recipients: scope.recipients.clone(),
            scope,
        }
    } else {
        leani_source_api::FilterSet::default()
    };
    Ok(DataRequest {
        chain_id,
        range,
        required,
        log_fields,
        allow_filtered,
        projection: leani_source_api::FieldProjection::default(),
        filters,
        minimum_finality,
        verification_policy: VerificationPolicy::CompleteCryptographic,
    })
}

fn union_filter_scope(
    retained: &mut leani_primitives::FilterScope,
    incoming: &leani_primitives::FilterScope,
) {
    fn extend_unique<T: Clone + Eq>(retained: &mut Vec<T>, incoming: &[T]) {
        for value in incoming {
            if !retained.contains(value) {
                retained.push(value.clone());
            }
        }
    }

    extend_unique(&mut retained.addresses, &incoming.addresses);
    extend_unique(
        &mut retained.transaction_hashes,
        &incoming.transaction_hashes,
    );
    extend_unique(&mut retained.transaction_types, &incoming.transaction_types);
    extend_unique(&mut retained.senders, &incoming.senders);
    extend_unique(&mut retained.recipients, &incoming.recipients);
    for topic in &incoming.topics {
        if let Some(existing) = retained
            .topics
            .iter_mut()
            .find(|existing| existing.position == topic.position)
        {
            extend_unique(&mut existing.alternatives, &topic.alternatives);
        } else {
            retained.topics.push(topic.clone());
        }
    }
}

fn signal_readiness(readiness: Option<&tokio::sync::watch::Sender<bool>>, ready: bool) {
    if let Some(readiness) = readiness {
        readiness.send_replace(ready);
    }
}

impl std::fmt::Debug for LiveRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LiveRuntime")
            .field("store", &self.store)
            .field("source", &self.source.descriptor())
            .field("processor", &self.processor.descriptor())
            .field("config", &self.config)
            .finish()
    }
}

impl LiveRuntime {
    /// Construct a live coordinator without opening the source.
    ///
    /// # Errors
    ///
    /// Returns an error for a zero reorg bound or chain mismatch.
    pub fn new(
        store: SqliteStore,
        source: Arc<dyn LiveSource>,
        processor: Arc<dyn Processor>,
        config: LiveRuntimeConfig,
    ) -> Result<Self, RuntimeError> {
        if config.max_reorg_depth == 0 {
            return Err(RuntimeError::InvalidConfig(
                "maximum reorg depth must be greater than zero".to_owned(),
            ));
        }
        if matches!(
            processor.descriptor().publication,
            PublicationPolicy::TerminalOnly
        ) {
            return Err(RuntimeError::InvalidConfig(
                "terminal-only processors require a bounded historical job".to_owned(),
            ));
        }
        Ok(Self {
            store,
            source,
            processor,
            config,
        })
    }

    /// Consume verified chain events until the source ends or cancellation is
    /// requested.
    ///
    /// # Errors
    ///
    /// Fails closed on gaps, parent mismatches, oversized reorgs, invalid
    /// source resets, processor failures, or durable overlap conflicts.
    pub async fn run(
        &self,
        start: LiveStart,
        budget: SourceBudget,
        cancellation: CancellationToken,
    ) -> Result<LiveReport, RuntimeError> {
        let budget = budget.validate()?;
        let mut events = self
            .source
            .subscribe(
                compile_live_request(
                    std::slice::from_ref(&self.processor),
                    self.source.descriptor().chain_id,
                    &start,
                )?,
                start,
                budget,
                cancellation.clone(),
            )
            .await?;
        let mut report = LiveReport::default();
        loop {
            let event = tokio::select! {
                () = cancellation.cancelled() => break,
                event = events.next() => event,
            };
            let Some(event) = event else {
                break;
            };
            match event? {
                ChainEvent::Block(frame) => match self.apply_frame(*frame).await? {
                    ApplyOutcome::Applied {
                        processor_cursor, ..
                    } => {
                        report.blocks_applied = report.blocks_applied.saturating_add(1);
                        report.last_block = Some(processor_cursor.block_number);
                        report.last_hash = Some(processor_cursor.block_hash);
                    }
                    ApplyOutcome::AlreadyApplied => {
                        report.duplicates = report.duplicates.saturating_add(1);
                    }
                },
                ChainEvent::Reorg { reverted, applied } => {
                    self.apply_reorg(&reverted, applied, &mut report).await?;
                }
                ChainEvent::Disconnected { reason } => {
                    report.disconnects = report.disconnects.saturating_add(1);
                    warn!(%reason, "live source reported a transient disconnect");
                }
                ChainEvent::Reset { last_valid, reason } => {
                    return Err(RuntimeError::LiveReset { last_valid, reason });
                }
            }
        }
        if let Some(cursor) = self
            .store
            .processor_cursor(self.processor.descriptor())
            .await?
        {
            report.last_block = Some(cursor.block_number);
            report.last_hash = Some(cursor.block_hash);
        }
        Ok(report)
    }

    async fn apply_frame(
        &self,
        frame: leani_primitives::BlockFrame,
    ) -> Result<ApplyOutcome, RuntimeError> {
        frame
            .validate_shape()
            .map_err(|error| RuntimeError::InvalidFrame(error.to_owned()))?;
        if frame.chain_id != self.source.descriptor().chain_id {
            return Err(RuntimeError::InvalidFrame(
                "live frame and source chains differ".to_owned(),
            ));
        }
        let prior = self
            .store
            .processor_cursor(self.processor.descriptor())
            .await?;
        if let Some(prior) = &prior {
            if frame.block.number == prior.block_number && frame.block.hash == prior.block_hash {
                return Ok(ApplyOutcome::AlreadyApplied);
            }
            let expected = prior.block_number.0.saturating_add(1);
            if frame.block.number.0 != expected || frame.block.parent_hash != prior.block_hash {
                return Err(RuntimeError::LiveGap {
                    expected: BlockNumber(expected),
                    received: frame.block,
                });
            }
        }
        let delta = self.processor.map(&frame).await?;
        let cursor = ProcessorCursor {
            processor_id: self.processor.descriptor().id.to_string(),
            processor_version: self.processor.descriptor().version.to_string(),
            chain_id: frame.chain_id,
            block_number: frame.block.number,
            block_hash: frame.block.hash,
            finality: frame.finality,
            sequence: prior.map_or(1, |cursor| cursor.sequence.saturating_add(1)),
        };
        self.store
            .apply(
                self.processor.as_ref(),
                cursor,
                &delta,
                &self.config.sink_ids,
            )
            .await
            .map_err(Into::into)
    }

    async fn apply_reorg(
        &self,
        reverted: &[leani_primitives::BlockRef],
        applied: Vec<leani_primitives::BlockFrame>,
        report: &mut LiveReport,
    ) -> Result<(), RuntimeError> {
        if reverted.is_empty() || reverted.len() > self.config.max_reorg_depth {
            return Err(RuntimeError::InvalidReorg(format!(
                "reverted depth {} is outside 1..={}",
                reverted.len(),
                self.config.max_reorg_depth
            )));
        }
        let mut cursor = self
            .store
            .processor_cursor(self.processor.descriptor())
            .await?
            .ok_or_else(|| RuntimeError::InvalidReorg("processor has no live cursor".to_owned()))?;
        for block in reverted {
            if block.number != cursor.block_number || block.hash != cursor.block_hash {
                return Err(RuntimeError::InvalidReorg(format!(
                    "expected reverted tip {:?}, received {:?}",
                    cursor.block_number, block.number
                )));
            }
            self.store
                .undo(
                    self.processor.descriptor(),
                    cursor.chain_id,
                    block.number,
                    block.hash,
                    &self.config.sink_ids,
                )
                .await?;
            report.blocks_reverted = report.blocks_reverted.saturating_add(1);
            let next = self
                .store
                .processor_cursor(self.processor.descriptor())
                .await?;
            if let Some(next) = next {
                cursor = next;
            } else if reverted.last() != Some(block) {
                return Err(RuntimeError::InvalidReorg(
                    "reorg removed the cursor before all declared blocks".to_owned(),
                ));
            }
        }
        for frame in applied {
            match self.apply_frame(frame).await? {
                ApplyOutcome::Applied {
                    processor_cursor, ..
                } => {
                    report.blocks_applied = report.blocks_applied.saturating_add(1);
                    report.last_block = Some(processor_cursor.block_number);
                    report.last_hash = Some(processor_cursor.block_hash);
                }
                ApplyOutcome::AlreadyApplied => {
                    report.duplicates = report.duplicates.saturating_add(1);
                }
            }
        }
        Ok(())
    }
}

/// Finality events applied to one processor namespace.
#[derive(Clone)]
pub struct FinalityRuntime {
    store: SqliteStore,
    source: Arc<dyn FinalitySource>,
    processor: Arc<dyn Processor>,
}

impl std::fmt::Debug for FinalityRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FinalityRuntime")
            .field("store", &self.store)
            .field("source", &self.source.descriptor())
            .field("processor", &self.processor.descriptor())
            .finish()
    }
}

/// Finite finality-stream report.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct FinalityReport {
    pub optimistic_events: u64,
    pub safe_events: u64,
    pub finalized_events: u64,
    pub finalized_through: Option<BlockNumber>,
}

impl FinalityRuntime {
    #[must_use]
    pub fn new(
        store: SqliteStore,
        source: Arc<dyn FinalitySource>,
        processor: Arc<dyn Processor>,
    ) -> Self {
        Self {
            store,
            source,
            processor,
        }
    }

    /// Consume verified finality events and make finalized undo rejection
    /// durable.
    ///
    /// # Errors
    ///
    /// Fails closed when the finality source disagrees, resets unexpectedly,
    /// or anchors a hash absent from processor coverage.
    pub async fn run(
        &self,
        checkpoint: ConsensusCheckpoint,
        cancellation: CancellationToken,
    ) -> Result<FinalityReport, RuntimeError> {
        let mut events = self
            .source
            .subscribe(checkpoint, cancellation.clone())
            .await?;
        let mut report = FinalityReport::default();
        loop {
            let event = tokio::select! {
                () = cancellation.cancelled() => break,
                event = events.next() => event,
            };
            let Some(event) = event else {
                break;
            };
            match event? {
                FinalityEvent::Optimistic { .. } => {
                    report.optimistic_events = report.optimistic_events.saturating_add(1);
                }
                FinalityEvent::Safe { .. } => {
                    report.safe_events = report.safe_events.saturating_add(1);
                }
                FinalityEvent::Finalized { block_hash, .. } => {
                    let through = self
                        .store
                        .coverage_block_by_hash(self.processor.descriptor(), block_hash)
                        .await?
                        .ok_or(RuntimeError::UnknownFinalizedAnchor(block_hash))?;
                    self.store
                        .mark_finalized(self.processor.descriptor(), through)
                        .await?;
                    report.finalized_events = report.finalized_events.saturating_add(1);
                    report.finalized_through = Some(
                        report
                            .finalized_through
                            .map_or(through, |old| old.max(through)),
                    );
                }
                FinalityEvent::Disagreement {
                    first,
                    second,
                    beacon_slot,
                } => {
                    return Err(RuntimeError::FinalityDisagreement {
                        first,
                        second,
                        beacon_slot,
                    });
                }
                FinalityEvent::Reset(checkpoint) => {
                    return Err(RuntimeError::FinalityReset(checkpoint));
                }
            }
        }
        Ok(report)
    }
}

/// Retention bounds applied whenever shared finality advances.
#[derive(Clone, Debug)]
pub struct SharedFinalityRuntimeConfig {
    pub minimum_recent_blocks: u64,
    pub recent_soft_bytes: u64,
    pub recent_hard_bytes: u64,
}

impl Default for SharedFinalityRuntimeConfig {
    fn default() -> Self {
        Self {
            minimum_recent_blocks: 128,
            recent_soft_bytes: 1024 * 1024 * 1024,
            recent_hard_bytes: 2 * 1024 * 1024 * 1024,
        }
    }
}

/// Finality progress shared across all configured processors.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct SharedFinalityReport {
    pub optimistic_events: u64,
    pub safe_events: u64,
    pub finalized_events: u64,
    pub finalized_through: Option<BlockNumber>,
    pub deferred_finalized_anchor: Option<BlockHash>,
    pub processor_finalized_through: BTreeMap<String, BlockNumber>,
    pub deferred_processors: BTreeMap<String, BlockHash>,
    pub pruned_recent_frames: u64,
    pub retained_recent_bytes: u64,
}

/// One consensus-verified finalized execution anchor after its canonical
/// execution block is present in recent storage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AppliedFinalityAnchor {
    pub block: BlockRef,
    pub beacon_slot: u64,
    pub beacon_block_root: [u8; 32],
}

/// One independently verified finality stream fanned out to recent storage and
/// every processor instance.
#[derive(Clone)]
pub struct SharedFinalityRuntime {
    store: SqliteStore,
    source: Arc<dyn FinalitySource>,
    processors: Vec<Arc<dyn Processor>>,
    config: SharedFinalityRuntimeConfig,
    applied_anchors: Option<tokio::sync::broadcast::Sender<AppliedFinalityAnchor>>,
}

impl std::fmt::Debug for SharedFinalityRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SharedFinalityRuntime")
            .field("store", &self.store)
            .field("source", &self.source.descriptor())
            .field(
                "processors",
                &self
                    .processors
                    .iter()
                    .map(|processor| processor.descriptor())
                    .collect::<Vec<_>>(),
            )
            .field("config", &self.config)
            .field("publishes_applied_anchors", &self.applied_anchors.is_some())
            .finish()
    }
}

impl SharedFinalityRuntime {
    /// Construct a shared finality coordinator.
    ///
    /// # Errors
    ///
    /// Rejects empty processors and invalid retention limits.
    pub fn new(
        store: SqliteStore,
        source: Arc<dyn FinalitySource>,
        processors: Vec<Arc<dyn Processor>>,
        config: SharedFinalityRuntimeConfig,
    ) -> Result<Self, RuntimeError> {
        if processors.is_empty() {
            return Err(RuntimeError::InvalidConfig(
                "shared finality runtime requires at least one processor".to_owned(),
            ));
        }
        if config.minimum_recent_blocks == 0
            || config.recent_soft_bytes == 0
            || config.recent_hard_bytes < config.recent_soft_bytes
        {
            return Err(RuntimeError::InvalidConfig(
                "shared finality recent limits are invalid".to_owned(),
            ));
        }
        Ok(Self {
            store,
            source,
            processors,
            config,
            applied_anchors: None,
        })
    }

    /// Publish consensus-verified anchors after their canonical execution
    /// material has been accepted locally.
    #[must_use]
    pub fn with_applied_anchors(
        mut self,
        sender: tokio::sync::broadcast::Sender<AppliedFinalityAnchor>,
    ) -> Self {
        self.applied_anchors = Some(sender);
        self
    }

    /// Consume verified finality and advance all processor/recent prefixes that
    /// already contain the exact execution hash.
    ///
    /// Processors still cold-backfilling defer the anchor instead of claiming
    /// finality for unknown coverage.
    ///
    /// # Errors
    ///
    /// A verified finalized hash that races ahead of execution ingestion is
    /// retained and retried until the exact canonical frame arrives.
    ///
    /// Fails closed on a finality contradiction/reset, store failure, or a
    /// hard retention-limit conflict.
    pub async fn run(
        &self,
        checkpoint: ConsensusCheckpoint,
        cancellation: CancellationToken,
    ) -> Result<SharedFinalityReport, RuntimeError> {
        let mut events = self
            .source
            .subscribe(checkpoint, cancellation.clone())
            .await?;
        let mut report = SharedFinalityReport::default();
        let mut pending_finalized = None;
        let mut retry = tokio::time::interval(FINALITY_ANCHOR_RETRY_INTERVAL);
        retry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            let event = tokio::select! {
                () = cancellation.cancelled() => break,
                _ = retry.tick(), if pending_finalized.is_some() => {
                    let Some((block_hash, beacon_slot, beacon_block_root)) = pending_finalized else {
                        continue;
                    };
                    if self
                        .apply_finalized(
                            block_hash,
                            beacon_slot,
                            beacon_block_root,
                            &mut report,
                        )
                        .await?
                    {
                        pending_finalized = None;
                    }
                    continue;
                }
                event = events.next() => event,
            };
            let Some(event) = event else {
                break;
            };
            match event? {
                FinalityEvent::Optimistic { .. } => {
                    report.optimistic_events = report.optimistic_events.saturating_add(1);
                }
                FinalityEvent::Safe { .. } => {
                    report.safe_events = report.safe_events.saturating_add(1);
                }
                FinalityEvent::Finalized {
                    block_hash,
                    beacon_slot,
                    beacon_block_root,
                } => {
                    pending_finalized = Some((block_hash, beacon_slot, beacon_block_root));
                    if self
                        .apply_finalized(block_hash, beacon_slot, beacon_block_root, &mut report)
                        .await?
                    {
                        pending_finalized = None;
                    }
                }
                FinalityEvent::Disagreement {
                    first,
                    second,
                    beacon_slot,
                } => {
                    return Err(RuntimeError::FinalityDisagreement {
                        first,
                        second,
                        beacon_slot,
                    });
                }
                FinalityEvent::Reset(checkpoint) => {
                    return Err(RuntimeError::FinalityReset(checkpoint));
                }
            }
        }
        Ok(report)
    }

    /// Keep verified finality independent from transient transport loss while
    /// publishing whether a currently verified subscription is usable.
    ///
    /// An unavailable or disconnected source clears readiness and is retried
    /// from the original weak-subjectivity checkpoint. Verification
    /// contradictions, protocol errors, store failures, and resource-limit
    /// failures still terminate the lane fail-closed.
    ///
    /// # Errors
    ///
    /// Returns non-transient source failures and the same fail-closed
    /// processor, store, finality, and retention errors as [`Self::run`].
    #[allow(clippy::too_many_lines)]
    pub async fn run_resilient_with_readiness(
        &self,
        checkpoint: ConsensusCheckpoint,
        cancellation: CancellationToken,
        readiness: tokio::sync::watch::Sender<bool>,
    ) -> Result<SharedFinalityReport, RuntimeError> {
        let mut report = SharedFinalityReport::default();
        let mut pending_finalized = None;
        let mut reconnect_attempt = 0_u32;

        loop {
            if cancellation.is_cancelled() {
                break;
            }
            signal_readiness(Some(&readiness), false);
            let mut events = match self
                .source
                .subscribe(checkpoint.clone(), cancellation.clone())
                .await
            {
                Ok(events) => {
                    reconnect_attempt = 0;
                    signal_readiness(Some(&readiness), true);
                    events
                }
                Err(SourceError::Cancelled) if cancellation.is_cancelled() => break,
                Err(error) if retryable_finality_source_error(&error) => {
                    reconnect_attempt = reconnect_attempt.saturating_add(1);
                    let delay = retry_delay(
                        FINALITY_RECONNECT_BASE,
                        FINALITY_RECONNECT_MAX,
                        reconnect_attempt,
                    );
                    warn!(
                        %error,
                        ?delay,
                        "verified finality source unavailable; retrying without restarting execution"
                    );
                    tokio::select! {
                        () = cancellation.cancelled() => break,
                        () = tokio::time::sleep(delay) => {}
                    }
                    continue;
                }
                Err(error) => return Err(error.into()),
            };

            let mut retry = tokio::time::interval(FINALITY_ANCHOR_RETRY_INTERVAL);
            retry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let disconnected = loop {
                let event = tokio::select! {
                    () = cancellation.cancelled() => {
                        signal_readiness(Some(&readiness), false);
                        return Ok(report);
                    }
                    _ = retry.tick(), if pending_finalized.is_some() => {
                        let Some((block_hash, beacon_slot, beacon_block_root)) = pending_finalized else {
                            continue;
                        };
                        if self
                            .apply_finalized(
                                block_hash,
                                beacon_slot,
                                beacon_block_root,
                                &mut report,
                            )
                            .await?
                        {
                            pending_finalized = None;
                        }
                        continue;
                    }
                    event = events.next() => event,
                };
                match event {
                    Some(Ok(FinalityEvent::Optimistic { .. })) => {
                        report.optimistic_events = report.optimistic_events.saturating_add(1);
                    }
                    Some(Ok(FinalityEvent::Safe { .. })) => {
                        report.safe_events = report.safe_events.saturating_add(1);
                    }
                    Some(Ok(FinalityEvent::Finalized {
                        block_hash,
                        beacon_slot,
                        beacon_block_root,
                    })) => {
                        pending_finalized = Some((block_hash, beacon_slot, beacon_block_root));
                        if self
                            .apply_finalized(
                                block_hash,
                                beacon_slot,
                                beacon_block_root,
                                &mut report,
                            )
                            .await?
                        {
                            pending_finalized = None;
                        }
                    }
                    Some(Ok(FinalityEvent::Disagreement {
                        first,
                        second,
                        beacon_slot,
                    })) => {
                        signal_readiness(Some(&readiness), false);
                        return Err(RuntimeError::FinalityDisagreement {
                            first,
                            second,
                            beacon_slot,
                        });
                    }
                    Some(Ok(FinalityEvent::Reset(checkpoint))) => {
                        signal_readiness(Some(&readiness), false);
                        return Err(RuntimeError::FinalityReset(checkpoint));
                    }
                    Some(Err(SourceError::Cancelled)) if cancellation.is_cancelled() => {
                        signal_readiness(Some(&readiness), false);
                        return Ok(report);
                    }
                    Some(Err(error)) if retryable_finality_source_error(&error) => break error,
                    Some(Err(error)) => {
                        signal_readiness(Some(&readiness), false);
                        return Err(error.into());
                    }
                    None => {
                        break SourceError::Disconnected(
                            "verified finality stream ended unexpectedly".to_owned(),
                        );
                    }
                }
            };

            signal_readiness(Some(&readiness), false);
            reconnect_attempt = reconnect_attempt.saturating_add(1);
            let delay = retry_delay(
                FINALITY_RECONNECT_BASE,
                FINALITY_RECONNECT_MAX,
                reconnect_attempt,
            );
            warn!(
                error = %disconnected,
                ?delay,
                "verified finality stream lost; retrying without restarting execution"
            );
            tokio::select! {
                () = cancellation.cancelled() => break,
                () = tokio::time::sleep(delay) => {}
            }
        }
        signal_readiness(Some(&readiness), false);
        Ok(report)
    }

    async fn apply_finalized(
        &self,
        block_hash: BlockHash,
        beacon_slot: u64,
        beacon_block_root: [u8; 32],
        report: &mut SharedFinalityReport,
    ) -> Result<bool, RuntimeError> {
        let chain_id = self.source.descriptor().chain_id;
        let Some(canonical) = self
            .store
            .canonical_block_by_hash(chain_id, block_hash)
            .await?
        else {
            report.deferred_finalized_anchor = Some(block_hash);
            return Ok(false);
        };
        report.deferred_finalized_anchor = None;
        for processor in &self.processors {
            let id = processor.descriptor().id.to_string();
            match self
                .store
                .coverage_block_by_hash(processor.descriptor(), block_hash)
                .await?
            {
                Some(through) if through == canonical.number => {
                    self.store
                        .mark_finalized(processor.descriptor(), through)
                        .await?;
                    report
                        .processor_finalized_through
                        .insert(id.clone(), through);
                    report.deferred_processors.remove(&id);
                }
                Some(through) => {
                    return Err(RuntimeError::InvalidReorg(format!(
                        "processor {id} resolves finalized hash at {}, canonical recent material at {}",
                        through.0, canonical.number.0
                    )));
                }
                None => {
                    report.deferred_processors.insert(id, block_hash);
                }
            }
        }
        self.store
            .mark_recent_finalized(chain_id, canonical.number, block_hash)
            .await?;
        let pruned = self
            .store
            .prune_recent_frames(
                chain_id,
                canonical.number,
                self.config.minimum_recent_blocks,
                self.config.recent_soft_bytes,
                self.config.recent_hard_bytes,
            )
            .await?;
        report.pruned_recent_frames = report
            .pruned_recent_frames
            .saturating_add(pruned.deleted_frames);
        report.retained_recent_bytes = pruned.retained_bytes;
        report.finalized_events = report.finalized_events.saturating_add(1);
        report.finalized_through = Some(
            report
                .finalized_through
                .map_or(canonical.number, |old| old.max(canonical.number)),
        );
        if let Some(sender) = &self.applied_anchors {
            let _ = sender.send(AppliedFinalityAnchor {
                block: canonical,
                beacon_slot,
                beacon_block_root,
            });
        }
        if pruned.hard_limit_exceeded {
            return Err(RuntimeError::RecentStorageBudget {
                limit: self.config.recent_hard_bytes,
                observed: pruned.retained_bytes,
            });
        }
        Ok(true)
    }
}

fn decode_checkpoint(bytes: &[u8]) -> Result<BackfillCheckpoint, RuntimeError> {
    serde_json::from_slice(bytes).map_err(Into::into)
}

fn retryable_source_error(error: &SourceError) -> bool {
    matches!(
        error,
        SourceError::Disconnected(_) | SourceError::Unavailable(_) | SourceError::Protocol(_)
    )
}

fn retryable_finality_source_error(error: &SourceError) -> bool {
    matches!(
        error,
        SourceError::Disconnected(_) | SourceError::Unavailable(_)
    )
}

fn failover_source_error(error: &SourceError) -> bool {
    retryable_source_error(error)
        || matches!(
            error,
            SourceError::MissingRange(_)
                | SourceError::IncompleteRange { .. }
                | SourceError::MissingMaterial { .. }
        )
}

fn verify_successor_anchor(
    range: BlockRange,
    last_hash: Option<BlockHash>,
    expected_successor_parent: Option<BlockHash>,
) -> Result<(), RuntimeError> {
    if let Some(expected) = expected_successor_parent
        && last_hash != Some(expected)
    {
        return Err(RuntimeError::Source(SourceError::CorruptFrame(format!(
            "range ending at block {} does not join the stored successor parent {expected:?}",
            range.end().0,
        ))));
    }
    Ok(())
}

fn verify_range_end_hash(
    range: BlockRange,
    delta: &EncodedDelta,
    expected_end_hash: Option<BlockHash>,
) -> Result<(), RuntimeError> {
    if delta.block.number == range.end()
        && let Some(expected) = expected_end_hash
        && delta.block.hash != expected
    {
        return Err(RuntimeError::Source(SourceError::CorruptFrame(format!(
            "range ending at block {} has hash {:?}, but stored successor expects {expected:?}",
            range.end().0,
            delta.block.hash,
        ))));
    }
    Ok(())
}

fn retry_delay(base: Duration, maximum: Duration, attempt: u32) -> Duration {
    let exponent = attempt.saturating_sub(1).min(31);
    base.saturating_mul(1_u32 << exponent).min(maximum)
}

fn elapsed_milliseconds(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn now_milliseconds() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("invalid runtime configuration: {0}")]
    InvalidConfig(String),
    #[error("historical runtime was cancelled")]
    Cancelled,
    #[error("source failed: {0}")]
    Source(#[from] SourceError),
    #[error("processor failed: {0}")]
    Processor(#[from] ProcessorError),
    #[error("store failed: {0}")]
    Store(#[from] StoreError),
    #[error("artifact sink failed: {0}")]
    ArtifactSink(#[from] ArtifactSinkError),
    #[error("job {0} already exists with different immutable input")]
    JobIdentity(String),
    #[error("chunk ended before block {expected_through}; next expected block is {next}")]
    IncompleteChunk {
        expected_through: BlockNumber,
        next: BlockNumber,
    },
    #[error("live frame failed validation: {0}")]
    InvalidFrame(String),
    #[error("live stream gap: expected block {expected}, received {received:?}")]
    LiveGap {
        expected: BlockNumber,
        received: leani_primitives::BlockRef,
    },
    #[error("invalid live reorg: {0}")]
    InvalidReorg(String),
    #[error("live source requested a reset at {last_valid:?}: {reason}")]
    LiveReset {
        last_valid: Option<leani_primitives::BlockRef>,
        reason: String,
    },
    #[error("pending live deltas use {observed} bytes; hard limit is {limit}")]
    PendingDeltaBudget { limit: u64, observed: u64 },
    #[error("one mapped historical delta uses {observed} bytes; hard limit is {limit}")]
    MappedDeltaBudget { limit: u64, observed: u64 },
    #[error("recent retained frames use {observed} bytes; hard limit is {limit}")]
    RecentStorageBudget { limit: u64, observed: u64 },
    #[error("finality source anchored unknown execution hash {0:?}")]
    UnknownFinalizedAnchor(BlockHash),
    #[error("finality sources disagree at beacon slot {beacon_slot}: {first:?} versus {second:?}")]
    FinalityDisagreement {
        first: BlockHash,
        second: BlockHash,
        beacon_slot: u64,
    },
    #[error("finality source requested checkpoint reset: {0:?}")]
    FinalityReset(ConsensusCheckpoint),
    #[error("runtime JSON encoding failed: {0}")]
    Json(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use leani_primitives::{Capability, ChainId, HeaderEnvelope, Material};
    use leani_processor_blobs::BlobsProcessor;
    use leani_source_api::{
        FieldProjection, FilterSet, SourceBudget, SourceDescriptor, VerificationPolicy,
    };
    use leani_store_artifacts::{
        ArtifactCompression, ArtifactSegmentLimits, ArtifactSegmentSink, ArtifactSegmentSinkConfig,
    };
    use leani_testkit::{
        BlockLocalCounter, FinalityStep, HistoryStep, LiveStep, OrderedLedgerProcessor,
        ScriptedChunk, ScriptedFinalitySource, ScriptedHistorySource, ScriptedLiveSource,
        default_source_budget, fixture_frame, fixture_source_descriptor,
    };
    use tower::ServiceExt;

    use super::*;
    use leani_store_sqlite::ChangeDirection;

    #[test]
    fn adaptive_history_commits_shrink_immediately_above_writer_target() {
        let mut state = AdaptiveCommitState::new(128);
        state.observe(20_001, 128, Duration::from_millis(20));
        assert_eq!(state.maximum_blocks(), 64);
        state.observe(20_001, 128, Duration::from_millis(20));
        assert_eq!(state.maximum_blocks(), 32);
    }

    #[test]
    fn adaptive_history_commits_grow_only_after_a_stable_low_latency_window() {
        let mut state = AdaptiveCommitState::new(128);
        state.maximum_blocks = 8;
        for _ in 0..ADAPTIVE_COMMIT_SAMPLE_WINDOW - 1 {
            state.observe(1_000, 128, Duration::from_millis(20));
        }
        assert_eq!(state.maximum_blocks(), 8);
        state.observe(1_000, 128, Duration::from_millis(20));
        assert_eq!(state.maximum_blocks(), 16);
    }

    #[derive(Clone, Debug)]
    struct RecoveringFinalitySource {
        descriptor: leani_source_api::SourceDescriptor,
        expected_checkpoint: ConsensusCheckpoint,
        subscriptions: Arc<AtomicUsize>,
    }

    #[derive(Debug)]
    struct StaticLiveGapRecovery {
        frames: Vec<leani_primitives::BlockFrame>,
    }

    #[derive(Debug)]
    struct FlakyLiveGapRecovery {
        frames: Vec<leani_primitives::BlockFrame>,
        attempts: AtomicUsize,
    }

    #[async_trait]
    impl FinalizedLiveGapRecovery for StaticLiveGapRecovery {
        async fn recover_chunk(
            &self,
            _processor: &ProcessorDescriptor,
            range: BlockRange,
        ) -> Result<Vec<leani_primitives::BlockFrame>, RuntimeError> {
            Ok(self
                .frames
                .iter()
                .filter(|frame| {
                    frame.block.number >= range.start() && frame.block.number <= range.end()
                })
                .cloned()
                .collect())
        }
    }

    #[async_trait]
    impl FinalizedLiveGapRecovery for FlakyLiveGapRecovery {
        async fn recover_chunk(
            &self,
            _processor: &ProcessorDescriptor,
            range: BlockRange,
        ) -> Result<Vec<leani_primitives::BlockFrame>, RuntimeError> {
            if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(SourceError::Unavailable(
                    "injected temporary live-gap archive outage".to_owned(),
                )
                .into());
            }
            Ok(self
                .frames
                .iter()
                .filter(|frame| {
                    frame.block.number >= range.start() && frame.block.number <= range.end()
                })
                .cloned()
                .collect())
        }
    }

    #[derive(Debug, Default)]
    struct FinalitySensitiveCounter {
        inner: BlockLocalCounter,
    }

    #[derive(Debug)]
    struct FailOnceCounter {
        inner: BlockLocalCounter,
        failures_remaining: AtomicUsize,
    }

    impl FailOnceCounter {
        fn named(name: &str) -> Self {
            Self {
                inner: BlockLocalCounter::named(name)
                    .with_split_delivery()
                    .with_output_none(),
                failures_remaining: AtomicUsize::new(1),
            }
        }
    }

    #[async_trait]
    impl Processor for FailOnceCounter {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn descriptor(&self) -> &ProcessorDescriptor {
            self.inner.descriptor()
        }

        async fn map(
            &self,
            block: &leani_primitives::BlockFrame,
        ) -> Result<EncodedDelta, ProcessorError> {
            if self
                .failures_remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                return Err(ProcessorError::Input(
                    "injected one-shot live mapping failure".to_owned(),
                ));
            }
            self.inner.map(block).await
        }

        async fn reduce(
            &self,
            transaction: &mut dyn leani_processor_api::ReducerTransaction,
            cursor: &ProcessorCursor,
            delta: &EncodedDelta,
        ) -> Result<leani_processor_api::DomainChanges, ProcessorError> {
            self.inner.reduce(transaction, cursor, delta).await
        }
    }

    #[async_trait]
    impl Processor for FinalitySensitiveCounter {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn descriptor(&self) -> &leani_processor_api::ProcessorDescriptor {
            self.inner.descriptor()
        }

        async fn map(
            &self,
            block: &leani_primitives::BlockFrame,
        ) -> Result<EncodedDelta, ProcessorError> {
            let mut payload = self.inner.map(block).await?.payload;
            payload.push(block.finality as u8);
            Ok(EncodedDelta::new(
                self.descriptor(),
                block.chain_id,
                block.block,
                payload,
            ))
        }

        fn finality_variant_checksums(
            &self,
            delta: &EncodedDelta,
        ) -> Result<Vec<BlockHash>, ProcessorError> {
            delta.validate(self.descriptor())?;
            let mut checksums = Vec::with_capacity(2);
            for finality in [Finality::Included, Finality::Finalized] {
                let mut payload = delta.payload.clone();
                let encoded_finality = payload
                    .last_mut()
                    .ok_or_else(|| ProcessorError::DeltaPayload("missing finality".to_owned()))?;
                *encoded_finality = finality as u8;
                checksums.push(
                    EncodedDelta::new(self.descriptor(), delta.chain_id, delta.block, payload)
                        .checksum,
                );
            }
            checksums.sort_unstable();
            checksums.dedup();
            Ok(checksums)
        }

        async fn reduce(
            &self,
            _transaction: &mut dyn leani_processor_api::ReducerTransaction,
            _cursor: &ProcessorCursor,
            _delta: &EncodedDelta,
        ) -> Result<leani_processor_api::DomainChanges, ProcessorError> {
            Ok(leani_processor_api::DomainChanges::default())
        }
    }

    #[async_trait]
    impl FinalitySource for RecoveringFinalitySource {
        fn descriptor(&self) -> &leani_source_api::SourceDescriptor {
            &self.descriptor
        }

        async fn subscribe(
            &self,
            checkpoint: ConsensusCheckpoint,
            _cancellation: CancellationToken,
        ) -> Result<leani_source_api::FinalityEventStream, SourceError> {
            if checkpoint != self.expected_checkpoint {
                return Err(SourceError::Protocol(
                    "weak-subjectivity checkpoint mismatch".to_owned(),
                ));
            }
            if self.subscriptions.fetch_add(1, Ordering::SeqCst) == 0 {
                return Ok(futures::stream::once(async {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                    Err(SourceError::Unavailable(
                        "injected temporary finality outage".to_owned(),
                    ))
                })
                .boxed());
            }
            Ok(futures::stream::pending().boxed())
        }
    }

    async fn store() -> (tempfile::TempDir, SqliteStore) {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteStore::open(leani_store_sqlite::StoreConfig::new(
            directory.path().join("node.sqlite"),
        ))
        .await
        .expect("store");
        (directory, store)
    }

    async fn wait_for_fair_scheduler_waiter(
        scheduler: &HistoricalFairCommitScheduler,
        job_id: &str,
    ) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let waiting = scheduler
                    .inner
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .jobs
                    .get(job_id)
                    .is_some_and(|job| job.waiting);
                if waiting {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("fair scheduler waiter registered");
    }

    #[tokio::test]
    async fn historical_commit_scheduler_charges_bytes_with_deficit_round_robin() {
        let scheduler = HistoricalFairCommitScheduler::new(1_024);
        let _seed_registration = scheduler.register_job("seed").expect("register seed");
        let _dense_registration = scheduler.register_job("dense").expect("register dense");
        let _sparse_registration = scheduler.register_job("sparse").expect("register sparse");
        let cancellation = CancellationToken::new();
        let seed = scheduler
            .acquire("seed", &cancellation)
            .await
            .expect("seed turn");
        let (admitted, mut admissions) = tokio::sync::mpsc::unbounded_channel();

        let dense_scheduler = scheduler.clone();
        let dense_cancellation = cancellation.clone();
        let dense_admitted = admitted.clone();
        let dense = tokio::spawn(async move {
            for _ in 0..2 {
                let permit = dense_scheduler
                    .acquire("dense", &dense_cancellation)
                    .await
                    .expect("dense turn");
                dense_admitted.send("dense").expect("record dense turn");
                permit.complete(4 * 1_024);
            }
        });
        wait_for_fair_scheduler_waiter(&scheduler, "dense").await;

        let sparse_scheduler = scheduler.clone();
        let sparse_cancellation = cancellation.clone();
        let sparse_admitted = admitted;
        let sparse = tokio::spawn(async move {
            for _ in 0..2 {
                let permit = sparse_scheduler
                    .acquire("sparse", &sparse_cancellation)
                    .await
                    .expect("sparse turn");
                sparse_admitted.send("sparse").expect("record sparse turn");
                permit.complete(1);
            }
        });
        wait_for_fair_scheduler_waiter(&scheduler, "sparse").await;
        seed.complete(1);

        let mut order = Vec::new();
        for _ in 0..4 {
            order.push(
                tokio::time::timeout(Duration::from_secs(1), admissions.recv())
                    .await
                    .expect("fair admission timeout")
                    .expect("fair admission channel"),
            );
        }
        dense.await.expect("dense task");
        sparse.await.expect("sparse task");
        assert_eq!(order, vec!["dense", "sparse", "sparse", "dense"]);
    }

    async fn externalized_subscription_job(
        store: &SqliteStore,
        processor: &dyn Processor,
        id: &str,
        range: BlockRange,
        mode: BackfillMode,
        revision: u64,
    ) -> BackfillJob {
        externalized_subscription_job_with_limits(
            store,
            processor,
            id,
            range,
            mode,
            revision,
            128,
            1024 * 1024,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn externalized_subscription_job_with_limits(
        store: &SqliteStore,
        processor: &dyn Processor,
        id: &str,
        range: BlockRange,
        mode: BackfillMode,
        revision: u64,
        effective_block_limit: u64,
        effective_byte_limit: u64,
    ) -> BackfillJob {
        store
            .register_processor(processor.descriptor())
            .await
            .expect("register processor");
        let stream_id = store
            .create_backfill_delivery_stream(processor.descriptor(), id)
            .await
            .expect("history stream")
            .stream_id;
        let consumer_id = format!("destination-{revision}");
        store
            .create_consumer_in_stream(
                processor.descriptor(),
                &stream_id,
                &consumer_id,
                leani_store_sqlite::ConsumerRole::Required,
                leani_store_sqlite::ConsumerStartPosition::EarliestRetained,
                Duration::from_secs(30),
            )
            .await
            .expect("consumer");
        let mut job = BackfillJob::for_processor(
            id,
            processor,
            ChainId(1),
            range,
            VerificationPolicy::CompleteCryptographic,
        )
        .expect("job");
        job.owner = HistoricalJobOwner::Subscription;
        job.mode = mode;
        job.delivery_stream_id = Some(stream_id.clone());
        let job_record = leani_store_sqlite::JobRecord {
            id: id.to_owned(),
            kind: job.owner.job_kind().to_owned(),
            state: leani_store_sqlite::JobState::Queued,
            payload: serde_json::to_vec(&job).expect("job payload"),
            checkpoint: None,
            attempts: 0,
            updated_at_unix_ms: 1,
        };
        store
            .create_backfill_subscription_job(
                &leani_store_sqlite::BackfillSubscriptionRecord {
                    subscription_id: id.to_owned(),
                    job_id: id.to_owned(),
                    processor_instance: processor.descriptor().instance.to_string(),
                    history_stream_id: stream_id,
                    mode: if mode == BackfillMode::Recompute {
                        leani_store_sqlite::BackfillSubscriptionMode::Recompute
                    } else {
                        leani_store_sqlite::BackfillSubscriptionMode::FillMissing
                    },
                    publication_revision: 0,
                    state: leani_store_sqlite::BackfillSubscriptionState::Queued,
                    consumer_id,
                    ranges: vec![range],
                    range,
                    preexisting_coverage: (mode == BackfillMode::Recompute)
                        .then_some(vec![range])
                        .unwrap_or_default(),
                    captured_finalized_target: range.end(),
                    idempotency_key: format!("{id}-fixture"),
                    effective_block_limit,
                    effective_byte_limit,
                    resume_below_ratio_millionths: 750_000,
                    delivery_batch_limits: leani_store_sqlite::BackfillDeliveryBatchLimits::default(
                    ),
                    initial_sequence: 0,
                    completion_sequence: None,
                    processed_work_blocks: 0,
                },
                &job_record,
                BlockHash::new([u8::try_from(revision.saturating_add(1)).unwrap_or(u8::MAX); 32]),
            )
            .await
            .expect("durable subscription");
        job
    }

    #[tokio::test]
    async fn coordinated_history_opens_later_chunks_before_processing_the_first() {
        let range = BlockRange::new(BlockNumber(1), BlockNumber(4)).expect("range");
        let all_frames = frames(range);
        let first_range = BlockRange::new(BlockNumber(1), BlockNumber(2)).expect("first range");
        let second_range = BlockRange::new(BlockNumber(3), BlockNumber(4)).expect("second range");
        let source = Arc::new(ScriptedHistorySource::new(
            fixture_source_descriptor("pipelined-history", range),
            vec![
                ScriptedChunk {
                    range: first_range,
                    schema_version: "fixture-v1".to_owned(),
                    estimated_bytes: None,
                    steps: std::iter::once(HistoryStep::Delay(Duration::from_millis(250)))
                        .chain(
                            all_frames[..2]
                                .iter()
                                .cloned()
                                .map(|frame| HistoryStep::Frame(Box::new(frame))),
                        )
                        .collect(),
                },
                ScriptedChunk {
                    range: second_range,
                    schema_version: "fixture-v1".to_owned(),
                    estimated_bytes: None,
                    steps: all_frames[2..]
                        .iter()
                        .cloned()
                        .map(|frame| HistoryStep::Frame(Box::new(frame)))
                        .collect(),
                },
            ],
        ));
        let pipeline_budget =
            HistoricalPipelineBudget::new(2, 2, 4 * 1_024 * 1_024).expect("pipeline budget");
        let coordinator = HistoricalMaterialCoordinator::new_with_pipeline_budget(
            HistoricalMaterialCoordinatorConfig {
                memory_bytes: 4 * 1_024 * 1_024,
                maximum_buffered_frames_per_acquisition: 8,
                ..HistoricalMaterialCoordinatorConfig::default()
            },
            &pipeline_budget,
        )
        .expect("coordinator");
        let (_directory, store) = store().await;
        let processor = Arc::new(BlockLocalCounter::default());
        let runtime = HistoricalRuntime::new(
            store,
            source.clone(),
            processor.clone(),
            HistoricalRuntimeConfig {
                mapper_concurrency: 2,
                ..HistoricalRuntimeConfig::default()
            },
        )
        .expect("runtime")
        .with_pipeline_budget(pipeline_budget)
        .with_material_coordinator(coordinator.clone());
        let job = BackfillJob::for_processor(
            "pipelined-acquisition",
            processor.as_ref(),
            ChainId(1),
            range,
            VerificationPolicy::CompleteCryptographic,
        )
        .expect("job");
        let mut budget = default_source_budget();
        budget.max_in_flight_requests = 2;
        let run =
            tokio::spawn(async move { runtime.run(job, budget, CancellationToken::new()).await });
        tokio::time::timeout(Duration::from_millis(100), async {
            while source.open_calls() < 2 {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("both physical chunks should open while the first is delayed");
        let report = run.await.expect("runtime task").expect("backfill");
        assert_eq!(report.frames_committed, 4);
    }

    #[test]
    fn filtered_processor_scopes_are_unionable_without_disabling_pushdown() {
        let first = leani_primitives::Address::new([0x11; 20]);
        let second = leani_primitives::Address::new([0x22; 20]);
        let mut retained = leani_primitives::FilterScope {
            addresses: vec![first],
            topics: vec![leani_primitives::TopicFilter {
                position: 0,
                alternatives: vec![[0x33; 32]],
            }],
            ..leani_primitives::FilterScope::default()
        };
        union_filter_scope(
            &mut retained,
            &leani_primitives::FilterScope {
                addresses: vec![second],
                topics: vec![leani_primitives::TopicFilter {
                    position: 0,
                    alternatives: vec![[0x44; 32]],
                }],
                ..leani_primitives::FilterScope::default()
            },
        );

        assert_eq!(retained.addresses, [first, second]);
        assert_eq!(retained.topics.len(), 1);
        assert_eq!(retained.topics[0].alternatives, [[0x33; 32], [0x44; 32]]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn coordinated_reorder_window_cannot_starve_the_next_chunk() {
        let range = BlockRange::new(BlockNumber(1), BlockNumber(6)).expect("range");
        let all_frames = frames(range);
        let source = Arc::new(ScriptedHistorySource::new(
            fixture_source_descriptor("ordered-acquisition-permits", range),
            vec![
                ScriptedChunk {
                    range: BlockRange::new(BlockNumber(1), BlockNumber(2)).expect("first range"),
                    schema_version: "fixture-v1".to_owned(),
                    estimated_bytes: None,
                    steps: all_frames[..2]
                        .iter()
                        .cloned()
                        .map(|frame| HistoryStep::Frame(Box::new(frame)))
                        .collect(),
                },
                ScriptedChunk {
                    range: BlockRange::new(BlockNumber(3), BlockNumber(4)).expect("second range"),
                    schema_version: "fixture-v1".to_owned(),
                    estimated_bytes: None,
                    steps: all_frames[2..4]
                        .iter()
                        .cloned()
                        .map(|frame| HistoryStep::Frame(Box::new(frame)))
                        .collect(),
                },
                ScriptedChunk {
                    range: BlockRange::new(BlockNumber(5), BlockNumber(6)).expect("third range"),
                    schema_version: "fixture-v1".to_owned(),
                    estimated_bytes: None,
                    steps: all_frames[4..]
                        .iter()
                        .cloned()
                        .map(|frame| HistoryStep::Frame(Box::new(frame)))
                        .collect(),
                },
            ],
        ));
        let pipeline_budget =
            HistoricalPipelineBudget::new(1, 1, 4 * 1_024 * 1_024).expect("pipeline budget");
        let coordinator = HistoricalMaterialCoordinator::new_with_pipeline_budget(
            HistoricalMaterialCoordinatorConfig {
                memory_bytes: 4 * 1_024 * 1_024,
                maximum_buffered_frames_per_acquisition: 1,
                ..HistoricalMaterialCoordinatorConfig::default()
            },
            &pipeline_budget,
        )
        .expect("coordinator");
        let (_directory, store) = store().await;
        let processor = Arc::new(BlockLocalCounter::default());
        let runtime = HistoricalRuntime::new(
            store,
            source.clone(),
            processor.clone(),
            HistoricalRuntimeConfig {
                mapper_concurrency: 1,
                ..HistoricalRuntimeConfig::default()
            },
        )
        .expect("runtime")
        .with_pipeline_budget(pipeline_budget)
        .with_material_coordinator(coordinator.clone());
        let job = BackfillJob::for_processor(
            "ordered-acquisition-permits",
            processor.as_ref(),
            ChainId(1),
            range,
            VerificationPolicy::CompleteCryptographic,
        )
        .expect("job");
        let mut budget = default_source_budget();
        budget.max_in_flight_requests = 3;

        let report = tokio::time::timeout(
            Duration::from_secs(2),
            runtime.run(job, budget, CancellationToken::new()),
        )
        .await
        .unwrap_or_else(|_| {
            panic!(
                "bounded reorder window stalled after {} source opens: {:?}",
                source.open_calls(),
                coordinator.snapshot()
            )
        })
        .expect("backfill");

        assert_eq!(report.frames_committed, range.len());
        assert_eq!(source.open_calls(), 3);
    }

    #[tokio::test]
    async fn shared_pipeline_map_slots_are_a_hard_node_wide_limit() {
        let budget = HistoricalPipelineBudget::new(2, 1, 1_024).expect("pipeline budget");
        let cancellation = CancellationToken::new();
        let first = budget
            .acquire_map_task(&cancellation)
            .await
            .expect("first map slot");
        assert!(
            tokio::time::timeout(
                Duration::from_millis(10),
                budget.acquire_map_task(&cancellation)
            )
            .await
            .is_err(),
            "a second job must not borrow an occupied global map slot"
        );
        drop(first);
        let _released = tokio::time::timeout(
            Duration::from_millis(100),
            budget.acquire_map_task(&cancellation),
        )
        .await
        .expect("released map slot becomes available")
        .expect("map slot remains open");
    }

    #[tokio::test]
    async fn one_mapped_delta_larger_than_the_global_budget_fails_fast() {
        let range = BlockRange::single(BlockNumber(1));
        let source = Arc::new(ScriptedHistorySource::from_frames(
            fixture_source_descriptor("oversized-mapped-history", range),
            frames(range),
        ));
        let processor = Arc::new(BlockLocalCounter::default());
        let (_directory, store) = store().await;
        let runtime = HistoricalRuntime::new(
            store,
            source,
            processor.clone(),
            HistoricalRuntimeConfig {
                mapper_concurrency: 1,
                maximum_mapped_bytes: 1,
                ..HistoricalRuntimeConfig::default()
            },
        )
        .expect("runtime");
        let job = BackfillJob::for_processor(
            "oversized-mapped-delta",
            processor.as_ref(),
            ChainId(1),
            range,
            VerificationPolicy::CompleteCryptographic,
        )
        .expect("job");
        let error = runtime
            .run(job, default_source_budget(), CancellationToken::new())
            .await
            .expect_err("mapped delta cannot ever fit");
        assert!(
            matches!(
                error,
                RuntimeError::MappedDeltaBudget {
                    limit: 1,
                    observed: 2..
                }
            ),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn externalized_history_splits_commits_at_the_exact_change_limit() {
        let range = BlockRange::new(BlockNumber(1), BlockNumber(10)).expect("range");
        let source = Arc::new(ScriptedHistorySource::from_frames(
            fixture_source_descriptor("microbatch-history", range),
            frames(range),
        ));
        let processor = Arc::new(
            BlockLocalCounter::default()
                .with_split_delivery()
                .with_output_none(),
        );
        let (_directory, store) = store().await;
        store
            .register_processor(processor.descriptor())
            .await
            .expect("register processor");
        let stream_id = store
            .create_backfill_delivery_stream(processor.descriptor(), "microbatch-job")
            .await
            .expect("history stream")
            .stream_id;
        store
            .create_consumer_in_stream(
                processor.descriptor(),
                &stream_id,
                "destination",
                leani_store_sqlite::ConsumerRole::Required,
                leani_store_sqlite::ConsumerStartPosition::EarliestRetained,
                Duration::from_secs(30),
            )
            .await
            .expect("consumer");
        let mut job = BackfillJob::for_processor(
            "microbatch-job",
            processor.as_ref(),
            ChainId(1),
            range,
            VerificationPolicy::CompleteCryptographic,
        )
        .expect("job");
        job.owner = HistoricalJobOwner::Subscription;
        job.delivery_stream_id = Some(stream_id.clone());
        let payload = serde_json::to_vec(&job).expect("job payload");
        let job_record = leani_store_sqlite::JobRecord {
            id: job.id.clone(),
            kind: job.owner.job_kind().to_owned(),
            state: leani_store_sqlite::JobState::Queued,
            payload,
            checkpoint: None,
            attempts: 0,
            updated_at_unix_ms: 1,
        };
        store
            .create_backfill_subscription_job(
                &leani_store_sqlite::BackfillSubscriptionRecord {
                    subscription_id: job.id.clone(),
                    job_id: job.id.clone(),
                    processor_instance: processor.descriptor().instance.to_string(),
                    history_stream_id: stream_id.clone(),
                    mode: leani_store_sqlite::BackfillSubscriptionMode::FillMissing,
                    publication_revision: 0,
                    state: leani_store_sqlite::BackfillSubscriptionState::Queued,
                    consumer_id: "destination".to_owned(),
                    ranges: vec![range],
                    range,
                    preexisting_coverage: Vec::new(),
                    captured_finalized_target: range.end(),
                    idempotency_key: "microbatch-fixture".to_owned(),
                    effective_block_limit: 128,
                    effective_byte_limit: 1024 * 1024,
                    resume_below_ratio_millionths: 750_000,
                    delivery_batch_limits: leani_store_sqlite::BackfillDeliveryBatchLimits::default(
                    ),
                    initial_sequence: 0,
                    completion_sequence: None,
                    processed_work_blocks: 0,
                },
                &job_record,
                leani_primitives::BlockHash::new([1; 32]),
            )
            .await
            .expect("durable subscription");
        let runtime = HistoricalRuntime::new(
            store.clone(),
            source,
            processor.clone(),
            HistoricalRuntimeConfig {
                mapper_concurrency: 2,
                commit_maximum_changes: 3,
                ..HistoricalRuntimeConfig::default()
            },
        )
        .expect("runtime");
        let report = runtime
            .run(job, default_source_budget(), CancellationToken::new())
            .await
            .expect("backfill");
        assert_eq!(report.frames_committed, 10);
        let changes = store
            .changes_in_stream(processor.descriptor(), &stream_id, ChainId(1), 0, 100)
            .await
            .expect("history changes");
        assert_eq!(
            changes
                .iter()
                .filter(|record| record.change.kind == "synthetic.counter")
                .count(),
            10
        );
        assert_eq!(
            changes
                .iter()
                .filter(|record| record.change.kind == "system.backfill_progress")
                .count(),
            4
        );
        assert_eq!(
            changes
                .iter()
                .filter(|record| record.change.kind == "system.backfill_complete")
                .count(),
            1
        );
        let completion_index = changes
            .iter()
            .position(|record| record.change.kind == "system.backfill_complete")
            .expect("completion record");
        assert_eq!(
            changes
                .get(completion_index.saturating_sub(1))
                .expect("final progress boundary")
                .change
                .kind,
            "system.backfill_progress"
        );
        let completion_sequence = changes[completion_index].cursor.sequence;
        let durable_subscription = store
            .backfill_subscription_for_job("microbatch-job")
            .await
            .expect("subscription")
            .expect("subscription exists");
        assert_eq!(durable_subscription.processed_work_blocks, 10);
        assert_eq!(
            durable_subscription.state,
            leani_store_sqlite::BackfillSubscriptionState::Draining
        );
        assert_eq!(
            durable_subscription.completion_sequence,
            Some(completion_sequence)
        );
        assert_eq!(
            store
                .job("microbatch-job")
                .await
                .expect("job")
                .expect("job exists")
                .state,
            leani_store_sqlite::JobState::Completed
        );
        assert_eq!(
            store
                .processor_stats(processor.descriptor())
                .await
                .expect("processor stats")
                .undo_records,
            0
        );
    }

    #[tokio::test]
    async fn overlapping_subscriptions_finish_their_creation_time_work_independently() {
        let range = BlockRange::new(BlockNumber(1), BlockNumber(4)).expect("range");
        let processor = Arc::new(
            BlockLocalCounter::default()
                .with_split_delivery()
                .with_output_none(),
        );
        let (_directory, store) = store().await;
        let first = externalized_subscription_job(
            &store,
            processor.as_ref(),
            "overlap-first",
            range,
            BackfillMode::FillMissing,
            0,
        )
        .await;
        let first_stream = first
            .delivery_stream_id
            .clone()
            .expect("first history stream");
        let second = externalized_subscription_job(
            &store,
            processor.as_ref(),
            "overlap-second",
            range,
            BackfillMode::FillMissing,
            1,
        )
        .await;

        for (job, source_id) in [(second, "overlap-winner"), (first, "overlap-finisher")] {
            let source = Arc::new(ScriptedHistorySource::from_frames(
                fixture_source_descriptor(source_id, range),
                frames(range),
            ));
            HistoricalRuntime::new(
                store.clone(),
                source,
                processor.clone(),
                HistoricalRuntimeConfig {
                    mapper_concurrency: 2,
                    ..HistoricalRuntimeConfig::default()
                },
            )
            .expect("runtime")
            .run(job, default_source_budget(), CancellationToken::new())
            .await
            .expect("overlapping subscription");
            if source_id == "overlap-winner" {
                let compacted = store
                    .compact_finalized_coverage(processor.descriptor(), range.end(), 2, range.len())
                    .await
                    .expect("defer compaction while overlapping work is active");
                assert_eq!(compacted.exact_coverage_deleted, 0);
            }
        }

        let first_changes = store
            .changes_in_stream(processor.descriptor(), &first_stream, ChainId(1), 0, 100)
            .await
            .expect("first subscription changes");
        assert_eq!(
            first_changes
                .iter()
                .filter(|record| record.change.kind == "synthetic.counter")
                .count(),
            usize::try_from(range.len()).expect("range length")
        );
        let completion = first_changes
            .iter()
            .find(|record| record.change.kind == "system.backfill_complete")
            .expect("first completion");
        let metadata =
            leani_store_sqlite::decode_backfill_completion_metadata(&completion.change.payload)
                .expect("completion metadata");
        assert_eq!(
            metadata.disposition,
            leani_store_sqlite::BackfillCompletionDisposition::PublishedAll
        );
        assert_eq!(metadata.covered_before_request_blocks, 0);
        assert_eq!(metadata.newly_processed_blocks, range.len());
        assert_eq!(metadata.republished_blocks, range.len());
        assert_eq!(
            store
                .backfill_subscription_range_progress("overlap-first")
                .await
                .expect("range progress")
                .expect("subscription progress"),
            vec![leani_store_sqlite::BackfillSubscriptionRangeProgress {
                range,
                committed_work_blocks: range.len(),
            }]
        );
        assert_eq!(
            store
                .job("overlap-first")
                .await
                .expect("job")
                .expect("first job")
                .state,
            JobState::Completed
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn disjoint_backfill_ranges_skip_unrequested_blocks_and_complete_once() {
        let first = BlockRange::new(BlockNumber(1), BlockNumber(2)).expect("first range");
        let second = BlockRange::new(BlockNumber(5), BlockNumber(6)).expect("second range");
        let bounding = BlockRange::new(first.start(), second.end()).expect("bounding range");
        let source = Arc::new(ScriptedHistorySource::from_frames(
            fixture_source_descriptor("disjoint-history", bounding),
            frames(bounding),
        ));
        let processor = Arc::new(
            BlockLocalCounter::default()
                .with_split_delivery()
                .with_output_none(),
        );
        let (_directory, store) = store().await;
        store
            .register_processor(processor.descriptor())
            .await
            .expect("register processor");
        let stream_id = store
            .create_backfill_delivery_stream(processor.descriptor(), "disjoint-job")
            .await
            .expect("history stream")
            .stream_id;
        store
            .create_consumer_in_stream(
                processor.descriptor(),
                &stream_id,
                "destination",
                leani_store_sqlite::ConsumerRole::Required,
                leani_store_sqlite::ConsumerStartPosition::EarliestRetained,
                Duration::from_secs(30),
            )
            .await
            .expect("consumer");
        let mut job = BackfillJob::for_processor_ranges(
            "disjoint-job",
            processor.as_ref(),
            ChainId(1),
            vec![second, first],
            VerificationPolicy::CompleteCryptographic,
        )
        .expect("job");
        job.owner = HistoricalJobOwner::Subscription;
        job.delivery_stream_id = Some(stream_id.clone());
        let payload = serde_json::to_vec(&job).expect("job payload");
        store
            .create_backfill_subscription_job(
                &leani_store_sqlite::BackfillSubscriptionRecord {
                    subscription_id: job.id.clone(),
                    job_id: job.id.clone(),
                    processor_instance: processor.descriptor().instance.to_string(),
                    history_stream_id: stream_id.clone(),
                    mode: leani_store_sqlite::BackfillSubscriptionMode::FillMissing,
                    publication_revision: 0,
                    state: leani_store_sqlite::BackfillSubscriptionState::Queued,
                    consumer_id: "destination".to_owned(),
                    ranges: vec![first, second],
                    range: bounding,
                    preexisting_coverage: Vec::new(),
                    captured_finalized_target: second.end(),
                    idempotency_key: "disjoint-fixture".to_owned(),
                    effective_block_limit: 128,
                    effective_byte_limit: 1024 * 1024,
                    resume_below_ratio_millionths: 750_000,
                    delivery_batch_limits: leani_store_sqlite::BackfillDeliveryBatchLimits::default(
                    ),
                    initial_sequence: 0,
                    completion_sequence: None,
                    processed_work_blocks: 0,
                },
                &leani_store_sqlite::JobRecord {
                    id: job.id.clone(),
                    kind: job.owner.job_kind().to_owned(),
                    state: leani_store_sqlite::JobState::Queued,
                    payload,
                    checkpoint: None,
                    attempts: 0,
                    updated_at_unix_ms: 1,
                },
                leani_primitives::BlockHash::new([2; 32]),
            )
            .await
            .expect("durable subscription");
        let runtime = HistoricalRuntime::new(
            store.clone(),
            source.clone(),
            processor.clone(),
            HistoricalRuntimeConfig {
                mapper_concurrency: 2,
                ..HistoricalRuntimeConfig::default()
            },
        )
        .expect("runtime");
        let report = runtime
            .run(
                job.clone(),
                default_source_budget(),
                CancellationToken::new(),
            )
            .await
            .expect("backfill");
        assert_eq!(report.requested_ranges, vec![first, second]);
        assert_eq!(report.frames_mapped, 4);
        assert_eq!(report.final_coverage, vec![first, second]);
        assert_eq!(
            store
                .coverage(processor.descriptor(), bounding)
                .await
                .expect("coverage"),
            vec![first, second]
        );
        let changes = store
            .changes_in_stream(processor.descriptor(), &stream_id, ChainId(1), 0, 100)
            .await
            .expect("history changes");
        assert_eq!(
            changes
                .iter()
                .filter(|record| record.change.kind == "synthetic.counter")
                .map(|record| record.block.number)
                .collect::<Vec<_>>(),
            vec![
                BlockNumber(1),
                BlockNumber(2),
                BlockNumber(5),
                BlockNumber(6)
            ]
        );
        assert_eq!(
            changes
                .iter()
                .filter(|record| record.change.kind == "system.backfill_progress")
                .count(),
            2
        );
        assert_eq!(
            changes
                .iter()
                .filter(|record| record.change.kind == "system.backfill_complete")
                .count(),
            1
        );
        let subscription = store
            .backfill_subscription_for_job("disjoint-job")
            .await
            .expect("subscription")
            .expect("subscription exists");
        assert_eq!(subscription.ranges, vec![first, second]);
        assert_eq!(subscription.processed_work_blocks, 4);
        let opens_after_completion = source.open_calls();
        let replay = runtime
            .run(job, default_source_budget(), CancellationToken::new())
            .await
            .expect("completed restart");
        assert_eq!(replay.frames_mapped, 0);
        assert_eq!(source.open_calls(), opens_after_completion);
        assert_eq!(
            store
                .changes_in_stream(processor.descriptor(), &stream_id, ChainId(1), 0, 100)
                .await
                .expect("history changes after restart")
                .iter()
                .filter(|record| record.change.kind == "system.backfill_complete")
                .count(),
            1
        );
    }

    fn frames(range: BlockRange) -> Vec<leani_primitives::BlockFrame> {
        let mut parent = BlockHash::ZERO;
        range
            .iter()
            .map(|number| {
                let frame = fixture_frame(number.0, parent);
                parent = frame.block.hash;
                frame
            })
            .collect()
    }

    fn included_frame(number: u64, parent: BlockHash) -> leani_primitives::BlockFrame {
        let mut frame = fixture_frame(number, parent);
        frame.finality = Finality::Included;
        frame
    }

    fn request(range: BlockRange) -> DataRequest {
        DataRequest {
            chain_id: ChainId(1),
            range,
            required: CapabilitySet::of(Capability::Transactions),
            allow_filtered: false,
            projection: FieldProjection::default(),
            log_fields: leani_primitives::LogFieldSet::NONE,
            filters: FilterSet::default(),
            minimum_finality: Finality::Included,
            verification_policy: VerificationPolicy::CompleteCryptographic,
        }
    }

    #[test]
    fn historical_material_identity_separates_slim_and_rpc_complete_logs() {
        let range = BlockRange::single(BlockNumber(1));
        let mut slim = request(range);
        slim.required = CapabilitySet::of(Capability::Logs);
        slim.log_fields = leani_primitives::LogFieldSet::NONE;
        let mut rpc_complete = slim.clone();
        rpc_complete.log_fields = leani_primitives::LogFieldSet::ALL;

        assert_ne!(
            historical_material::MaterialShape::from(&slim),
            historical_material::MaterialShape::from(&rpc_complete)
        );
    }

    fn e2e_hash(number: u64) -> BlockHash {
        let mut bytes = [0_u8; 32];
        bytes[..8].copy_from_slice(&number.to_be_bytes());
        bytes[8..16].copy_from_slice(&(!number).to_be_bytes());
        bytes[16..24].copy_from_slice(&number.rotate_left(17).to_be_bytes());
        bytes[24..].copy_from_slice(&number.rotate_right(11).to_be_bytes());
        bytes[31] ^= 0xa5;
        BlockHash::new(bytes)
    }

    fn e2e_chain(through: u64) -> Vec<leani_primitives::BlockFrame> {
        let mut parent = BlockHash::ZERO;
        (0..=through)
            .map(|number| {
                let mut frame = fixture_frame(number, parent);
                frame.block.hash = e2e_hash(number);
                frame.block.parent_hash = parent;
                frame.finality = Finality::Included;
                frame.header = Material::Complete(HeaderEnvelope {
                    rlp: None,
                    transactions_root: None,
                    receipts_root: None,
                    withdrawals_root: None,
                    gas_limit: None,
                    gas_used: None,
                    base_fee_per_gas: None,
                    blob_gas_used: None,
                    excess_blob_gas: None,
                    size_bytes: None,
                    consensus_size_bytes: None,
                    transaction_count: None,
                });
                parent = frame.block.hash;
                frame
            })
            .collect()
    }

    fn e2e_descriptor(id: &str, range: BlockRange) -> SourceDescriptor {
        let mut descriptor = fixture_source_descriptor(id, range);
        descriptor.capabilities = descriptor.capabilities.with(Capability::Header);
        descriptor.complete_capabilities =
            descriptor.complete_capabilities.with(Capability::Header);
        descriptor
    }

    fn e2e_budget(max_frames: u64) -> SourceBudget {
        SourceBudget {
            max_input_bytes: 256 * 1_024 * 1_024,
            max_frame_bytes: 1024 * 1024,
            max_frames,
            max_buffered_frames: 64,
            max_in_flight_requests: 4,
            temporary_disk_bytes: 1,
        }
    }

    async fn wait_for_recent_tip(store: &SqliteStore, tip: BlockNumber) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if store
                    .recent_stats(ChainId(1))
                    .await
                    .expect("recent stats")
                    .latest_block
                    == Some(tip)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("live lane reaches scripted tip");
    }

    fn assert_cancelled_live(result: Result<SharedLiveReport, RuntimeError>) {
        if let Err(error) = result {
            assert!(
                matches!(
                    error,
                    RuntimeError::Cancelled | RuntimeError::Source(SourceError::Cancelled)
                ),
                "unexpected live shutdown error: {error:?}"
            );
        }
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn archive_reconciliation_uses_durable_delta_checksums_after_raw_pruning() {
        let (_directory, store) = store().await;
        let processor = BlockLocalCounter::default();
        let range = BlockRange::new(BlockNumber(1), BlockNumber(3)).expect("range");
        let committed = frames(range);
        for (offset, frame) in committed.iter().enumerate() {
            let delta = processor.map(frame).await.expect("map live frame");
            store
                .apply(
                    &processor,
                    ProcessorCursor {
                        processor_id: processor.descriptor().id.to_string(),
                        processor_version: processor.descriptor().version.to_string(),
                        chain_id: frame.chain_id,
                        block_number: frame.block.number,
                        block_hash: frame.block.hash,
                        finality: Finality::Finalized,
                        sequence: u64::try_from(offset).expect("offset").saturating_add(1),
                    },
                    &delta,
                    &[],
                )
                .await
                .expect("commit live delta");
        }
        assert!(
            store
                .recent_canonical_bounds(ChainId(1))
                .await
                .expect("recent bounds")
                .is_none(),
            "test intentionally retains no raw live frames"
        );
        let archive = ScriptedHistorySource::from_frames(
            fixture_source_descriptor("archive-checksum", range),
            committed.clone(),
        );
        let verified = reconcile_archive_deltas(
            &store,
            &archive,
            &processor,
            ChainId(1),
            range,
            VerificationPolicy::CompleteCryptographic,
            default_source_budget(),
            CancellationToken::new(),
        )
        .await
        .expect("reconcile compact checksums");
        assert_eq!(verified.state, ArchiveReconciliationState::Verified);
        assert_eq!(verified.compared_blocks, range.len());

        let mismatch_range = BlockRange::single(BlockNumber(4));
        let fourth = fixture_frame(4, committed.last().expect("third").block.hash);
        let fourth_delta = processor.map(&fourth).await.expect("map fourth");
        store
            .apply(
                &processor,
                ProcessorCursor {
                    processor_id: processor.descriptor().id.to_string(),
                    processor_version: processor.descriptor().version.to_string(),
                    chain_id: fourth.chain_id,
                    block_number: fourth.block.number,
                    block_hash: fourth.block.hash,
                    finality: Finality::Finalized,
                    sequence: 4,
                },
                &fourth_delta,
                &[],
            )
            .await
            .expect("commit fourth delta");
        let mut archive_fourth = fourth;
        archive_fourth.block.timestamp = archive_fourth.block.timestamp.saturating_add(1);
        let mismatching_archive = ScriptedHistorySource::from_frames(
            fixture_source_descriptor("archive-mismatch", mismatch_range),
            vec![archive_fourth],
        );
        let error = reconcile_archive_deltas(
            &store,
            &mismatching_archive,
            &processor,
            ChainId(1),
            mismatch_range,
            VerificationPolicy::CompleteCryptographic,
            default_source_budget(),
            CancellationToken::new(),
        )
        .await
        .expect_err("delta disagreement fails closed");
        assert!(matches!(
            error,
            RuntimeError::Store(StoreError::ArchiveReconciliationMismatch { .. })
        ));
        let id = format!(
            "archive-reconciliation-{}-archive-mismatch-4-4",
            processor.descriptor().id
        );
        assert_eq!(
            store
                .archive_reconciliation(&id, processor.descriptor())
                .await
                .expect("failed record")
                .expect("record")
                .state,
            ArchiveReconciliationState::Failed
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[allow(clippy::too_many_lines)]
    async fn ten_thousand_block_hot_cold_restart_converges_and_serves_coverage() {
        const HISTORY_BLOCKS: u64 = 10_000;
        const ANCHOR: u64 = HISTORY_BLOCKS - 1;
        const OVERLAP_BLOCKS: u64 = 64;
        const LIVE_TIP: u64 = ANCHOR + 32;
        const CRASH_AFTER: usize = 250;

        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("hot-cold-e2e.sqlite");
        let mut store_config = leani_store_sqlite::StoreConfig::new(&path);
        store_config.durability = leani_store_sqlite::Durability::Normal;
        store_config.reader_connections = 8;
        let store = SqliteStore::open(store_config.clone())
            .await
            .expect("open first process");
        let all_frames = e2e_chain(LIVE_TIP);
        let history_range =
            BlockRange::new(BlockNumber(0), BlockNumber(ANCHOR)).expect("history range");
        let overlap_range = BlockRange::new(
            BlockNumber(ANCHOR - OVERLAP_BLOCKS + 1),
            BlockNumber(ANCHOR),
        )
        .expect("overlap range");
        let history_frames =
            all_frames[..usize::try_from(HISTORY_BLOCKS).expect("history length")].to_vec();
        let overlap_index = usize::try_from(overlap_range.start().0).expect("overlap index");

        let block_local = Arc::new(BlockLocalCounter::default());
        let ordered = Arc::new(OrderedLedgerProcessor::default());
        let processors: Vec<Arc<dyn Processor>> = vec![block_local.clone(), ordered.clone()];
        for processor in &processors {
            store
                .begin_hot_cold_handoff(
                    &format!("e2e-{}", processor.descriptor().id),
                    processor.descriptor(),
                    ChainId(1),
                    overlap_range,
                    all_frames[usize::try_from(ANCHOR).expect("anchor index")]
                        .block
                        .hash,
                )
                .await
                .expect("begin handoff");
        }

        // First process: capture live material, commit a partial historical
        // prefix, then fail as if the process died during the join.
        let first_live_steps = all_frames
            [overlap_index..=usize::try_from(ANCHOR + 4).expect("first live tip")]
            .iter()
            .cloned()
            .map(|frame| LiveStep::Event(ChainEvent::Block(Box::new(frame))))
            .chain(std::iter::once(LiveStep::Delay(Duration::from_mins(1))))
            .collect();
        let first_live_source = Arc::new(ScriptedLiveSource::new(
            e2e_descriptor(
                "e2e-live-first",
                BlockRange::new(overlap_range.start(), BlockNumber(ANCHOR + 4))
                    .expect("first live range"),
            ),
            first_live_steps,
        ));
        let first_live = SharedLiveRuntime::new(
            store.clone(),
            first_live_source,
            processors.clone(),
            SharedLiveRuntimeConfig {
                pending_delta_bytes: 64 * 1_024 * 1_024,
                ..SharedLiveRuntimeConfig::default()
            },
        )
        .expect("first live runtime");
        let first_cancellation = CancellationToken::new();
        let (first_ready, mut first_ready_updates) = tokio::sync::watch::channel(false);
        let first_live_task = {
            let runtime = first_live.clone();
            let cancellation = first_cancellation.clone();
            let anchor = all_frames[usize::try_from(ANCHOR).expect("anchor index")].block;
            tokio::spawn(async move {
                runtime
                    .run_with_readiness(
                        LiveStart::AnchoredOverlap {
                            anchor,
                            overlap_blocks: OVERLAP_BLOCKS,
                        },
                        e2e_budget(OVERLAP_BLOCKS + 5),
                        cancellation,
                        first_ready,
                    )
                    .await
            })
        };
        first_ready_updates
            .changed()
            .await
            .expect("first readiness update");
        assert!(*first_ready_updates.borrow());
        wait_for_recent_tip(&store, BlockNumber(ANCHOR + 4)).await;

        let failing_source = Arc::new(ScriptedHistorySource::new(
            e2e_descriptor("e2e-history-failing", history_range),
            vec![ScriptedChunk {
                range: history_range,
                schema_version: "fixture-v1".to_owned(),
                estimated_bytes: None,
                steps: history_frames[..CRASH_AFTER]
                    .iter()
                    .cloned()
                    .map(|frame| HistoryStep::Frame(Box::new(frame)))
                    .chain(std::iter::once(HistoryStep::Error(
                        SourceError::Unavailable("injected process crash".to_owned()),
                    )))
                    .collect(),
            }],
        ));
        let failed_config = HistoricalRuntimeConfig {
            mapper_concurrency: 16,
            max_attempts: 1,
            retry_base: Duration::from_millis(1),
            retry_max: Duration::from_millis(1),
            ..HistoricalRuntimeConfig::default()
        };
        let first_jobs = processors
            .iter()
            .map(|processor| {
                BackfillJob::for_processor(
                    format!("e2e-backfill-{}", processor.descriptor().id),
                    processor.as_ref(),
                    ChainId(1),
                    history_range,
                    VerificationPolicy::CompleteCryptographic,
                )
                .expect("first backfill job")
            })
            .collect::<Vec<_>>();
        let first_block_history = HistoricalRuntime::new(
            store.clone(),
            failing_source.clone(),
            block_local.clone(),
            failed_config.clone(),
        )
        .expect("first block-local history");
        let first_ordered_history = HistoricalRuntime::new(
            store.clone(),
            failing_source,
            ordered.clone(),
            failed_config,
        )
        .expect("first ordered history");
        let (block_failure, ordered_failure) = tokio::join!(
            first_block_history.run(
                first_jobs[0].clone(),
                e2e_budget(HISTORY_BLOCKS),
                CancellationToken::new()
            ),
            first_ordered_history.run(
                first_jobs[1].clone(),
                e2e_budget(HISTORY_BLOCKS),
                CancellationToken::new()
            )
        );
        assert!(block_failure.is_err());
        assert!(ordered_failure.is_err());
        first_cancellation.cancel();
        assert_cancelled_live(first_live_task.await.expect("first live task"));
        drop(first_live);
        drop(first_block_history);
        drop(first_ordered_history);
        drop(store);

        // Second process: reopen the same database, replay the overlap
        // idempotently, resume historical gaps, then drain the ordered head.
        let store = SqliteStore::open(store_config)
            .await
            .expect("reopen second process");
        for processor in &processors {
            assert_eq!(
                store
                    .hot_cold_handoff(
                        &format!("e2e-{}", processor.descriptor().id),
                        processor.descriptor()
                    )
                    .await
                    .expect("read running handoff")
                    .expect("handoff")
                    .state,
                leani_store_sqlite::HotColdHandoffState::Running
            );
        }
        let second_live_steps = all_frames[overlap_index..]
            .iter()
            .cloned()
            .map(|frame| LiveStep::Event(ChainEvent::Block(Box::new(frame))))
            .chain(std::iter::once(LiveStep::Delay(Duration::from_mins(1))))
            .collect();
        let second_live_source = Arc::new(ScriptedLiveSource::new(
            e2e_descriptor(
                "e2e-live-second",
                BlockRange::new(overlap_range.start(), BlockNumber(LIVE_TIP))
                    .expect("second live range"),
            ),
            second_live_steps,
        ));
        let second_live = SharedLiveRuntime::new(
            store.clone(),
            second_live_source,
            processors.clone(),
            SharedLiveRuntimeConfig {
                pending_delta_bytes: 64 * 1_024 * 1_024,
                ..SharedLiveRuntimeConfig::default()
            },
        )
        .expect("second live runtime");
        let second_cancellation = CancellationToken::new();
        let (second_ready, mut second_ready_updates) = tokio::sync::watch::channel(false);
        let second_live_task = {
            let runtime = second_live.clone();
            let cancellation = second_cancellation.clone();
            let anchor = all_frames[usize::try_from(ANCHOR).expect("anchor index")].block;
            tokio::spawn(async move {
                runtime
                    .run_with_readiness(
                        LiveStart::AnchoredOverlap {
                            anchor,
                            overlap_blocks: OVERLAP_BLOCKS,
                        },
                        e2e_budget(OVERLAP_BLOCKS + 32),
                        cancellation,
                        second_ready,
                    )
                    .await
            })
        };
        second_ready_updates
            .changed()
            .await
            .expect("second readiness update");
        assert!(*second_ready_updates.borrow());
        wait_for_recent_tip(&store, BlockNumber(LIVE_TIP)).await;

        let healthy_source: Arc<dyn HistorySource> = Arc::new(ScriptedHistorySource::from_frames(
            e2e_descriptor("e2e-history-healthy", history_range),
            history_frames,
        ));
        let healthy_config = HistoricalRuntimeConfig {
            mapper_concurrency: 32,
            max_attempts: 1,
            retry_base: Duration::from_millis(1),
            retry_max: Duration::from_millis(1),
            ..HistoricalRuntimeConfig::default()
        };
        let second_block_history = HistoricalRuntime::new(
            store.clone(),
            healthy_source.clone(),
            block_local.clone(),
            healthy_config.clone(),
        )
        .expect("second block-local history");
        let second_ordered_history = HistoricalRuntime::new(
            store.clone(),
            healthy_source,
            ordered.clone(),
            healthy_config,
        )
        .expect("second ordered history");
        let (block_report, ordered_report) = tokio::join!(
            second_block_history.run(
                first_jobs[0].clone(),
                e2e_budget(HISTORY_BLOCKS),
                CancellationToken::new()
            ),
            second_ordered_history.run(
                first_jobs[1].clone(),
                e2e_budget(HISTORY_BLOCKS),
                CancellationToken::new()
            )
        );
        let block_report = block_report.expect("block-local resume");
        let ordered_report = ordered_report.expect("ordered resume");
        assert_eq!(block_report.final_coverage, vec![history_range]);
        assert_eq!(ordered_report.final_coverage, vec![history_range]);
        let crash_prefix = u64::try_from(CRASH_AFTER - 1).expect("crash prefix");
        assert!(block_report.initial_coverage[0].end().0 >= crash_prefix);
        assert!(ordered_report.initial_coverage[0].end().0 >= crash_prefix);

        for processor in &processors {
            let verified = store
                .verify_hot_cold_handoff(
                    &format!("e2e-{}", processor.descriptor().id),
                    processor.descriptor(),
                    ChainId(1),
                    overlap_range,
                    all_frames[usize::try_from(ANCHOR).expect("anchor index")]
                        .block
                        .hash,
                )
                .await
                .expect("verify handoff");
            assert_eq!(
                verified.state,
                leani_store_sqlite::HotColdHandoffState::Verified
            );
            assert_eq!(verified.compared_blocks, OVERLAP_BLOCKS);
        }
        let reconciliation = second_live
            .reconcile_pending()
            .await
            .expect("reconcile ordered live deltas");
        assert_eq!(reconciliation.processors["synthetic-ledger"].pending, 0);

        let full_range =
            BlockRange::new(BlockNumber(0), BlockNumber(LIVE_TIP)).expect("full range");
        for processor in &processors {
            assert_eq!(
                store
                    .coverage(processor.descriptor(), full_range)
                    .await
                    .expect("complete coverage"),
                vec![full_range]
            );
            let statistics = store
                .processor_stats(processor.descriptor())
                .await
                .expect("processor stats");
            assert_eq!(statistics.applied_blocks, LIVE_TIP + 1);
            assert_eq!(statistics.pending_deltas, 0);
        }
        let recent = store.recent_stats(ChainId(1)).await.expect("recent stats");
        assert_eq!(recent.earliest_block, Some(overlap_range.start()));
        assert_eq!(recent.latest_block, Some(BlockNumber(LIVE_TIP)));
        assert_eq!(recent.frames, OVERLAP_BLOCKS + 32);
        store.verify().await.expect("verify complete store");

        let blobs = Arc::new(BlobsProcessor::default());
        let api_processors: Vec<Arc<dyn Processor>> =
            vec![blobs.clone(), block_local.clone(), ordered.clone()];
        let api = leani_api::router_with_processors(
            store.clone(),
            api_processors,
            Vec::new(),
            leani_api::ApiConfig::default(),
        )
        .expect("API router");
        let response = api
            .oneshot(
                Request::builder()
                    .uri("/v1/processors/synthetic-counter/status")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("API response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1024 * 1024)
            .await
            .expect("API body");
        let status: serde_json::Value = serde_json::from_slice(&body).expect("status JSON");
        assert_eq!(status["processedThrough"], LIVE_TIP);
        assert_eq!(status["complete"], true);
        assert_eq!(status["state"], "live");

        second_cancellation.cancel();
        assert_cancelled_live(second_live_task.await.expect("second live task"));
    }

    #[tokio::test]
    async fn historical_run_commits_coverage_and_resumes_idempotently() {
        let range = BlockRange::new(BlockNumber(1), BlockNumber(3)).expect("range");
        let source = Arc::new(ScriptedHistorySource::from_frames(
            fixture_source_descriptor("history", range),
            frames(range),
        ));
        let processor = Arc::new(BlockLocalCounter::default());
        let (_directory, store) = store().await;
        let runtime = HistoricalRuntime::new(
            store.clone(),
            source,
            processor.clone(),
            HistoricalRuntimeConfig {
                mapper_concurrency: 2,
                ..HistoricalRuntimeConfig::default()
            },
        )
        .expect("runtime");
        let job = BackfillJob {
            id: "backfill-1".to_owned(),
            owner: HistoricalJobOwner::Materialization,
            processor_instance: processor.descriptor().instance.to_string(),
            mode: BackfillMode::FillMissing,
            delivery_stream_id: None,
            ranges: vec![range],
            request: request(range),
            sink_ids: vec!["test".to_owned()],
        };
        let report = runtime
            .run(
                job.clone(),
                default_source_budget(),
                CancellationToken::new(),
            )
            .await
            .expect("run");
        assert_eq!(report.frames_committed, 3);
        assert_eq!(report.final_coverage, vec![range]);
        assert_eq!(
            store
                .changes(processor.descriptor(), ChainId(1), 0, 10)
                .await
                .expect("changes")
                .len(),
            3
        );
        let resumed = runtime
            .run(job, default_source_budget(), CancellationToken::new())
            .await
            .expect("resume");
        assert_eq!(resumed.frames_mapped, 0);
        assert_eq!(resumed.final_coverage, vec![range]);
        assert_eq!(
            store
                .job("backfill-1")
                .await
                .expect("job")
                .expect("exists")
                .state,
            JobState::Completed
        );
    }

    #[tokio::test]
    async fn fill_missing_rejects_a_range_that_does_not_join_stored_successor() {
        let full = BlockRange::new(BlockNumber(1), BlockNumber(3)).expect("full range");
        let correct = frames(full);
        let processor = Arc::new(BlockLocalCounter::default());
        let (_directory, store) = store().await;
        let seed_source = Arc::new(ScriptedHistorySource::from_frames(
            fixture_source_descriptor("edge-seed", full),
            correct.clone(),
        ));
        let seed_runtime = HistoricalRuntime::new(
            store.clone(),
            seed_source,
            processor.clone(),
            HistoricalRuntimeConfig {
                mapper_concurrency: 2,
                ..HistoricalRuntimeConfig::default()
            },
        )
        .expect("seed runtime");
        let seed = BackfillJob::for_processor_ranges(
            "edge-seed-job",
            processor.as_ref(),
            ChainId(1),
            vec![
                BlockRange::new(BlockNumber(1), BlockNumber(1)).expect("first"),
                BlockRange::new(BlockNumber(3), BlockNumber(3)).expect("third"),
            ],
            VerificationPolicy::CompleteCryptographic,
        )
        .expect("seed job");
        seed_runtime
            .run(seed, default_source_budget(), CancellationToken::new())
            .await
            .expect("seed disjoint coverage");

        let middle = BlockRange::new(BlockNumber(2), BlockNumber(2)).expect("middle");
        let mut wrong = correct[1].clone();
        wrong.block.hash = BlockHash::new([0x99; 32]);
        let wrong_source = Arc::new(ScriptedHistorySource::from_frames(
            fixture_source_descriptor("wrong-edge", middle),
            vec![wrong],
        ));
        let runtime = HistoricalRuntime::new(
            store.clone(),
            wrong_source,
            processor.clone(),
            HistoricalRuntimeConfig {
                mapper_concurrency: 2,
                ..HistoricalRuntimeConfig::default()
            },
        )
        .expect("runtime");
        let repair = BackfillJob::for_processor(
            "edge-repair",
            processor.as_ref(),
            ChainId(1),
            full,
            VerificationPolicy::CompleteCryptographic,
        )
        .expect("repair job");
        let error = runtime
            .run(repair, default_source_budget(), CancellationToken::new())
            .await
            .expect_err("wrong fork edge must fail");
        assert!(
            matches!(error, RuntimeError::Source(SourceError::CorruptFrame(_))),
            "{error:?}"
        );
        assert_eq!(
            store
                .coverage_hash(processor.descriptor(), BlockNumber(2))
                .await
                .expect("coverage"),
            None
        );
    }

    #[tokio::test]
    async fn node_owned_history_materializes_queryable_output_without_delivery_rows() {
        let range = BlockRange::new(BlockNumber(1), BlockNumber(3)).expect("range");
        let source = Arc::new(ScriptedHistorySource::from_frames(
            fixture_source_descriptor("history", range),
            frames(range),
        ));
        let processor = Arc::new(BlockLocalCounter::default().with_delivery_none());
        let (_directory, store) = store().await;
        let runtime = HistoricalRuntime::new(
            store.clone(),
            source,
            processor.clone(),
            HistoricalRuntimeConfig {
                mapper_concurrency: 2,
                ..HistoricalRuntimeConfig::default()
            },
        )
        .expect("runtime");
        let job = BackfillJob::for_processor(
            "sqlite-materialization",
            processor.as_ref(),
            ChainId(1),
            range,
            VerificationPolicy::CompleteCryptographic,
        )
        .expect("job");

        let report = runtime
            .run(job, default_source_budget(), CancellationToken::new())
            .await
            .expect("materialization");

        assert_eq!(report.frames_committed, range.len());
        assert_eq!(
            store
                .output_collections(processor.descriptor())
                .await
                .expect("output collections"),
            vec!["counter.blocks".to_owned()]
        );
        assert!(
            store
                .delivery_streams(processor.descriptor())
                .await
                .expect("delivery streams")
                .is_empty()
        );
        let statistics = store
            .processor_stats(processor.descriptor())
            .await
            .expect("processor statistics");
        assert_eq!(statistics.applied_blocks, range.len());
        assert_eq!(statistics.changes, 0);
        assert_eq!(statistics.outbox_records, 0);
    }

    #[tokio::test]
    async fn ordered_materialization_microbatch_matches_single_block_commits() {
        let range = BlockRange::new(BlockNumber(1), BlockNumber(12)).expect("range");
        let mut corpus = frames(range);
        for frame in &mut corpus {
            frame.header = Material::Complete(HeaderEnvelope {
                rlp: None,
                transactions_root: None,
                receipts_root: None,
                withdrawals_root: None,
                gas_limit: None,
                gas_used: None,
                base_fee_per_gas: None,
                blob_gas_used: None,
                excess_blob_gas: None,
                size_bytes: None,
                transaction_count: None,
                consensus_size_bytes: None,
            });
        }
        let processor = Arc::new(OrderedLedgerProcessor::default().with_delivery_none());
        let (_batched_directory, batched_store) = store().await;
        let (_isolated_directory, isolated_store) = store().await;

        for (name, target, commit_maximum_blocks) in [
            ("ordered-batched", batched_store.clone(), 12),
            ("ordered-isolated", isolated_store.clone(), 1),
        ] {
            let mut source_descriptor = fixture_source_descriptor(name, range);
            source_descriptor.capabilities =
                source_descriptor.capabilities.with(Capability::Header);
            source_descriptor.complete_capabilities = source_descriptor
                .complete_capabilities
                .with(Capability::Header);
            let runtime = HistoricalRuntime::new(
                target,
                Arc::new(ScriptedHistorySource::from_frames(
                    source_descriptor,
                    corpus.clone(),
                )),
                processor.clone(),
                HistoricalRuntimeConfig {
                    mapper_concurrency: 2,
                    commit_maximum_blocks,
                    ..HistoricalRuntimeConfig::default()
                },
            )
            .expect("ordered runtime");
            runtime
                .run(
                    BackfillJob::for_processor(
                        name,
                        processor.as_ref(),
                        ChainId(1),
                        range,
                        VerificationPolicy::CompleteCryptographic,
                    )
                    .expect("ordered job"),
                    default_source_budget(),
                    CancellationToken::new(),
                )
                .await
                .expect("ordered materialization");
        }

        assert_eq!(
            batched_store
                .entity(processor.descriptor(), "ledger", b"digest")
                .await
                .expect("batched digest"),
            isolated_store
                .entity(processor.descriptor(), "ledger", b"digest")
                .await
                .expect("isolated digest")
        );
        assert_eq!(
            batched_store
                .processor_cursor(processor.descriptor())
                .await
                .expect("batched cursor"),
            isolated_store
                .processor_cursor(processor.descriptor())
                .await
                .expect("isolated cursor")
        );
        assert_eq!(
            batched_store
                .coverage(processor.descriptor(), range)
                .await
                .expect("batched coverage"),
            isolated_store
                .coverage(processor.descriptor(), range)
                .await
                .expect("isolated coverage")
        );
    }

    #[tokio::test]
    async fn one_block_larger_than_the_commit_byte_limit_fails_fast() {
        let range = BlockRange::single(BlockNumber(1));
        let source = Arc::new(ScriptedHistorySource::from_frames(
            fixture_source_descriptor("oversized-commit", range),
            frames(range),
        ));
        let processor = Arc::new(BlockLocalCounter::default().with_delivery_none());
        let (_directory, store) = store().await;
        let runtime = HistoricalRuntime::new(
            store,
            source,
            processor.clone(),
            HistoricalRuntimeConfig {
                mapper_concurrency: 1,
                commit_maximum_encoded_bytes: 1,
                ..HistoricalRuntimeConfig::default()
            },
        )
        .expect("runtime");
        let job = BackfillJob::for_processor(
            "oversized-commit",
            processor.as_ref(),
            ChainId(1),
            range,
            VerificationPolicy::CompleteCryptographic,
        )
        .expect("job");
        let error = runtime
            .run(job, default_source_budget(), CancellationToken::new())
            .await
            .expect_err("one block cannot fit the commit byte limit");
        assert!(matches!(
            error,
            RuntimeError::Store(StoreError::HistoricalBatchLimit {
                maximum_encoded_bytes: 1,
                observed_encoded_bytes: 2..,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn historical_run_reuses_complete_recent_material_before_opening_a_source() {
        let range = BlockRange::new(BlockNumber(1), BlockNumber(3)).expect("range");
        let source = Arc::new(ScriptedHistorySource::from_frames(
            fixture_source_descriptor("unused-history", range),
            frames(range),
        ));
        let processor = Arc::new(BlockLocalCounter::default());
        let (_directory, store) = store().await;
        for mut frame in frames(range) {
            frame.header = Material::Complete(HeaderEnvelope {
                rlp: Some(vec![1, 2]),
                transactions_root: None,
                receipts_root: None,
                withdrawals_root: None,
                gas_limit: None,
                gas_used: None,
                base_fee_per_gas: None,
                blob_gas_used: None,
                excess_blob_gas: None,
                size_bytes: None,
                transaction_count: None,
                consensus_size_bytes: None,
            });
            frame.provenance.push(leani_primitives::Provenance {
                source_id: leani_primitives::SourceId::new("live-cache").expect("source ID"),
                source_kind: SourceKind::ExecutionP2p,
                trust: TrustModel::ProtocolVerified,
                range: Some(range),
                object: None,
                observed_at_unix_ms: 1,
                projection: Vec::new(),
            });
            store
                .store_recent_frame(&frame)
                .await
                .expect("recent frame");
        }
        let runtime = HistoricalRuntime::new(
            store,
            source.clone(),
            processor.clone(),
            HistoricalRuntimeConfig {
                mapper_concurrency: 2,
                ..HistoricalRuntimeConfig::default()
            },
        )
        .expect("runtime");
        let job = BackfillJob::for_processor(
            "recent-material",
            processor.as_ref(),
            ChainId(1),
            range,
            VerificationPolicy::CompleteCryptographic,
        )
        .expect("job");

        let report = runtime
            .run(job, default_source_budget(), CancellationToken::new())
            .await
            .expect("recent material backfill");

        assert_eq!(source.plan_calls(), 0);
        assert_eq!(source.open_calls(), 0);
        assert_eq!(report.source_attempts, 0);
        assert_eq!(report.source_id, "recent-store");
        assert_eq!(report.physical_source_bytes, 0);
        assert_eq!(report.reused_source_bytes, 6);
        assert_eq!(report.coalesced_frames, 0);
        assert_eq!(report.sources.len(), 1);
        assert_eq!(report.sources[0].source_id, "recent-store");
        assert_eq!(report.sources[0].attempts, 0);
        assert_eq!(report.final_coverage, vec![range]);
    }

    #[tokio::test]
    async fn historical_run_does_not_reuse_recent_material_with_insufficient_trust() {
        let range = BlockRange::new(BlockNumber(1), BlockNumber(1)).expect("range");
        let source = Arc::new(ScriptedHistorySource::from_frames(
            fixture_source_descriptor("verified-history", range),
            frames(range),
        ));
        let processor = Arc::new(BlockLocalCounter::default());
        let (_directory, store) = store().await;
        store
            .store_recent_frame(&frames(range).remove(0))
            .await
            .expect("unattributed recent frame");
        let runtime = HistoricalRuntime::new(
            store,
            source.clone(),
            processor.clone(),
            HistoricalRuntimeConfig {
                mapper_concurrency: 2,
                ..HistoricalRuntimeConfig::default()
            },
        )
        .expect("runtime");
        let job = BackfillJob::for_processor(
            "recent-material-trust-miss",
            processor.as_ref(),
            ChainId(1),
            range,
            VerificationPolicy::CompleteCryptographic,
        )
        .expect("job");

        let report = runtime
            .run(job, default_source_budget(), CancellationToken::new())
            .await
            .expect("source fallback");

        assert_eq!(source.plan_calls(), 1);
        assert_eq!(source.open_calls(), 1);
        assert_eq!(report.source_attempts, 1);
        assert_eq!(report.source_id, "verified-history");
        assert_eq!(report.final_coverage, vec![range]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[allow(clippy::too_many_lines)]
    async fn shared_historical_material_fetches_once_for_one_hundred_processors() {
        const PROCESSORS: usize = 100;

        let range = BlockRange::new(BlockNumber(0), BlockNumber(2)).expect("shared material range");
        let scripted = Arc::new(ScriptedHistorySource::new(
            fixture_source_descriptor("shared-history", range),
            vec![ScriptedChunk {
                range,
                schema_version: "fixture-v1".to_owned(),
                estimated_bytes: None,
                steps: std::iter::once(HistoryStep::Delay(Duration::from_millis(25)))
                    .chain(
                        frames(range)
                            .into_iter()
                            .map(|frame| HistoryStep::Frame(Box::new(frame))),
                    )
                    .collect(),
            }],
        ));
        let coordinator = HistoricalMaterialCoordinator::new(HistoricalMaterialCoordinatorConfig {
            mode: HistoricalMaterialCoordinatorMode::Enabled,
            memory_bytes: 4 * 1_024 * 1_024,
            maximum_buffered_frames_per_acquisition: 8,
            ..HistoricalMaterialCoordinatorConfig::default()
        })
        .expect("material coordinator");
        let permits = coordinator.startup_batch(PROCESSORS);
        let (_directory, store) = store().await;
        let mut processors = Vec::with_capacity(PROCESSORS);
        let mut runs = Vec::with_capacity(PROCESSORS);
        for (index, permit) in permits.into_iter().enumerate() {
            let processor: Arc<dyn Processor> = Arc::new(BlockLocalCounter::named(&format!(
                "shared-counter-{index:03}"
            )));
            let runtime = HistoricalRuntime::new(
                store.clone(),
                scripted.clone(),
                processor.clone(),
                HistoricalRuntimeConfig {
                    mapper_concurrency: 1,
                    max_attempts: 1,
                    retry_base: Duration::from_millis(1),
                    retry_max: Duration::from_millis(1),
                    ..HistoricalRuntimeConfig::default()
                },
            )
            .expect("historical runtime")
            .with_material_coordinator(coordinator.clone())
            .with_material_startup_permit(permit);
            let job = BackfillJob::for_processor(
                format!("shared-history-{index:03}"),
                processor.as_ref(),
                ChainId(1),
                range,
                VerificationPolicy::CompleteCryptographic,
            )
            .expect("backfill job");
            processors.push(processor);
            runs.push(async move {
                runtime
                    .run(job, default_source_budget(), CancellationToken::new())
                    .await
            });
        }

        let reports = futures::future::join_all(runs)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("shared historical runs");

        assert_eq!(scripted.open_calls(), 1);
        assert_eq!(reports.len(), PROCESSORS);
        let snapshot = coordinator.snapshot();
        assert_eq!(snapshot.acquisitions_started, 1);
        assert_eq!(snapshot.requests_coalesced, 99);
        assert_eq!(snapshot.physical_frames, range.len());
        assert_eq!(
            snapshot.logical_frame_deliveries,
            range
                .len()
                .saturating_mul(u64::try_from(PROCESSORS).expect("processor count"))
        );
        assert_eq!(
            reports
                .iter()
                .map(|report| report.physical_source_bytes)
                .sum::<u64>(),
            reports[0].source_bytes
        );
        assert_eq!(
            reports
                .iter()
                .map(|report| report.coalesced_frames)
                .sum::<u64>(),
            range.len().saturating_mul(99)
        );
        assert!(
            reports
                .iter()
                .all(|report| report.acquisition_ids.len() == 1)
        );
        for processor in processors {
            assert_eq!(
                store
                    .coverage(processor.descriptor(), range)
                    .await
                    .expect("processor coverage"),
                vec![range]
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn minimum_physical_chunk_combines_adjacent_startup_demands() {
        const PROCESSORS: usize = 8;

        let physical_range =
            BlockRange::new(BlockNumber(0), BlockNumber(7)).expect("physical range");
        let scripted = Arc::new(ScriptedHistorySource::from_frames(
            fixture_source_descriptor("normalized-history", physical_range),
            frames(physical_range),
        ));
        let coordinator = HistoricalMaterialCoordinator::new(HistoricalMaterialCoordinatorConfig {
            mode: HistoricalMaterialCoordinatorMode::Enabled,
            memory_bytes: 1_024 * 1_024,
            maximum_buffered_frames_per_acquisition: 8,
            minimum_physical_chunk_blocks: 8,
            maximum_overfetch_ratio: 1.25,
        })
        .expect("material coordinator");
        let permits = coordinator.startup_batch(PROCESSORS);
        let (_directory, store) = store().await;
        let mut processors = Vec::with_capacity(PROCESSORS);
        let mut runs = Vec::with_capacity(PROCESSORS);
        for (index, permit) in permits.into_iter().enumerate() {
            let processor = Arc::new(BlockLocalCounter::named(&format!(
                "normalized-counter-{index}"
            )));
            let logical_range = BlockRange::new(
                BlockNumber(u64::try_from(index).expect("processor index")),
                BlockNumber(u64::try_from(index).expect("processor index")),
            )
            .expect("logical range");
            let runtime = HistoricalRuntime::new(
                store.clone(),
                scripted.clone(),
                processor.clone(),
                HistoricalRuntimeConfig {
                    mapper_concurrency: 1,
                    ..HistoricalRuntimeConfig::default()
                },
            )
            .expect("runtime")
            .with_material_coordinator(coordinator.clone())
            .with_material_startup_permit(permit);
            let job = BackfillJob::for_processor(
                format!("normalized-history-{index}"),
                processor.as_ref(),
                ChainId(1),
                logical_range,
                VerificationPolicy::CompleteCryptographic,
            )
            .expect("backfill job");
            processors.push((processor, logical_range));
            runs.push(async move {
                runtime
                    .run(job, default_source_budget(), CancellationToken::new())
                    .await
            });
        }

        futures::future::join_all(runs)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("normalized historical runs");

        assert_eq!(scripted.open_calls(), 1);
        let snapshot = coordinator.snapshot();
        assert_eq!(snapshot.acquisitions_started, 1);
        assert_eq!(snapshot.requests_coalesced, 7);
        assert_eq!(snapshot.physical_frames, physical_range.len());
        assert_eq!(snapshot.logical_frame_deliveries, physical_range.len());
        assert_eq!(snapshot.overfetched_frames, 0);
        for (processor, logical_range) in processors {
            assert_eq!(
                store
                    .coverage(processor.descriptor(), logical_range)
                    .await
                    .expect("processor coverage"),
                vec![logical_range]
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn normalized_physical_chunk_routes_sparse_logical_demands() {
        let physical_range =
            BlockRange::new(BlockNumber(0), BlockNumber(7)).expect("physical range");
        let scripted = Arc::new(ScriptedHistorySource::from_frames(
            fixture_source_descriptor("sparse-normalized-history", physical_range),
            frames(physical_range),
        ));
        let coordinator = HistoricalMaterialCoordinator::new(HistoricalMaterialCoordinatorConfig {
            mode: HistoricalMaterialCoordinatorMode::Enabled,
            memory_bytes: 1_024 * 1_024,
            maximum_buffered_frames_per_acquisition: 8,
            minimum_physical_chunk_blocks: 8,
            maximum_overfetch_ratio: 8.0,
        })
        .expect("material coordinator");
        let mut permits = coordinator.startup_batch(2);
        let second_permit = permits.pop().expect("second permit");
        let first_permit = permits.pop().expect("first permit");
        let first_range = BlockRange::new(BlockNumber(0), BlockNumber(0)).expect("first range");
        let second_range = BlockRange::new(BlockNumber(7), BlockNumber(7)).expect("second range");
        let first_processor = Arc::new(BlockLocalCounter::named("sparse-counter-first"));
        let second_processor = Arc::new(BlockLocalCounter::named("sparse-counter-second"));
        let (_directory, store) = store().await;
        let first_runtime = HistoricalRuntime::new(
            store.clone(),
            scripted.clone(),
            first_processor.clone(),
            HistoricalRuntimeConfig {
                mapper_concurrency: 1,
                ..HistoricalRuntimeConfig::default()
            },
        )
        .expect("first runtime")
        .with_material_coordinator(coordinator.clone())
        .with_material_startup_permit(first_permit);
        let second_runtime = HistoricalRuntime::new(
            store.clone(),
            scripted.clone(),
            second_processor.clone(),
            HistoricalRuntimeConfig {
                mapper_concurrency: 1,
                ..HistoricalRuntimeConfig::default()
            },
        )
        .expect("second runtime")
        .with_material_coordinator(coordinator.clone())
        .with_material_startup_permit(second_permit);
        let first_job = BackfillJob::for_processor(
            "sparse-normalized-first",
            first_processor.as_ref(),
            ChainId(1),
            first_range,
            VerificationPolicy::CompleteCryptographic,
        )
        .expect("first job");
        let second_job = BackfillJob::for_processor(
            "sparse-normalized-second",
            second_processor.as_ref(),
            ChainId(1),
            second_range,
            VerificationPolicy::CompleteCryptographic,
        )
        .expect("second job");

        let (first, second) = tokio::join!(
            first_runtime.run(first_job, default_source_budget(), CancellationToken::new()),
            second_runtime.run(
                second_job,
                default_source_budget(),
                CancellationToken::new()
            ),
        );
        first.expect("first sparse run");
        second.expect("second sparse run");

        assert_eq!(scripted.open_calls(), 1);
        let snapshot = coordinator.snapshot();
        assert_eq!(snapshot.physical_frames, physical_range.len());
        assert_eq!(snapshot.logical_frame_deliveries, 2);
        assert_eq!(snapshot.overfetched_frames, 6);
        assert_eq!(
            store
                .coverage(first_processor.descriptor(), physical_range)
                .await
                .expect("first coverage"),
            vec![first_range]
        );
        assert_eq!(
            store
                .coverage(second_processor.descriptor(), physical_range)
                .await
                .expect("second coverage"),
            vec![second_range]
        );
    }

    #[tokio::test]
    async fn bounded_overfetch_never_advances_processor_coverage() {
        let available = BlockRange::new(BlockNumber(0), BlockNumber(7)).expect("available range");
        let logical_range = BlockRange::new(BlockNumber(0), BlockNumber(0)).expect("logical range");
        let scripted = Arc::new(ScriptedHistorySource::from_frames(
            fixture_source_descriptor("bounded-overfetch", available),
            frames(available),
        ));
        let coordinator = HistoricalMaterialCoordinator::new(HistoricalMaterialCoordinatorConfig {
            mode: HistoricalMaterialCoordinatorMode::Enabled,
            memory_bytes: 1_024 * 1_024,
            maximum_buffered_frames_per_acquisition: 8,
            minimum_physical_chunk_blocks: 8,
            maximum_overfetch_ratio: 4.0,
        })
        .expect("material coordinator");
        let processor = Arc::new(BlockLocalCounter::named("bounded-overfetch-counter"));
        let (_directory, store) = store().await;
        let runtime = HistoricalRuntime::new(
            store.clone(),
            scripted.clone(),
            processor.clone(),
            HistoricalRuntimeConfig {
                mapper_concurrency: 1,
                ..HistoricalRuntimeConfig::default()
            },
        )
        .expect("runtime")
        .with_material_coordinator(coordinator.clone());
        let job = BackfillJob::for_processor(
            "bounded-overfetch",
            processor.as_ref(),
            ChainId(1),
            logical_range,
            VerificationPolicy::CompleteCryptographic,
        )
        .expect("backfill job");

        let report = runtime
            .run(job, default_source_budget(), CancellationToken::new())
            .await
            .expect("bounded overfetch run");

        assert_eq!(scripted.open_calls(), 1);
        assert_eq!(report.frames_mapped, 1);
        assert_eq!(report.final_coverage, vec![logical_range]);
        assert_eq!(
            store
                .coverage(processor.descriptor(), available)
                .await
                .expect("processor coverage"),
            vec![logical_range]
        );
        let snapshot = coordinator.snapshot();
        assert_eq!(snapshot.physical_frames, 4);
        assert_eq!(snapshot.logical_frame_deliveries, 1);
        assert_eq!(snapshot.overfetched_frames, 3);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(clippy::too_many_lines)]
    async fn shared_historical_material_preserves_independent_ordered_state() {
        let range = BlockRange::new(BlockNumber(0), BlockNumber(3)).expect("ordered range");
        let mut descriptor = fixture_source_descriptor("shared-ordered-history", range);
        descriptor.capabilities = descriptor.capabilities.with(Capability::Header);
        descriptor.complete_capabilities =
            descriptor.complete_capabilities.with(Capability::Header);
        let mut source_frames = frames(range);
        for frame in &mut source_frames {
            frame.header = Material::Complete(HeaderEnvelope {
                rlp: None,
                transactions_root: None,
                receipts_root: None,
                withdrawals_root: None,
                gas_limit: None,
                gas_used: None,
                base_fee_per_gas: None,
                blob_gas_used: None,
                excess_blob_gas: None,
                size_bytes: None,
                transaction_count: None,
                consensus_size_bytes: None,
            });
        }
        let scripted = Arc::new(ScriptedHistorySource::new(
            descriptor,
            vec![ScriptedChunk {
                range,
                schema_version: "fixture-v1".to_owned(),
                estimated_bytes: None,
                steps: std::iter::once(HistoryStep::Delay(Duration::from_millis(10)))
                    .chain(
                        source_frames
                            .into_iter()
                            .map(|frame| HistoryStep::Frame(Box::new(frame))),
                    )
                    .collect(),
            }],
        ));
        let coordinator = HistoricalMaterialCoordinator::new(HistoricalMaterialCoordinatorConfig {
            mode: HistoricalMaterialCoordinatorMode::Enabled,
            memory_bytes: 1_024 * 1_024,
            maximum_buffered_frames_per_acquisition: 8,
            ..HistoricalMaterialCoordinatorConfig::default()
        })
        .expect("material coordinator");
        let mut permits = coordinator.startup_batch(2);
        let second_permit = permits.pop().expect("second startup permit");
        let first_permit = permits.pop().expect("first startup permit");
        let (_directory, store) = store().await;
        let first = Arc::new(OrderedLedgerProcessor::named("shared-ledger-first"));
        let second = Arc::new(OrderedLedgerProcessor::named("shared-ledger-second"));
        let first_runtime = HistoricalRuntime::new(
            store.clone(),
            scripted.clone(),
            first.clone(),
            HistoricalRuntimeConfig {
                mapper_concurrency: 1,
                ..HistoricalRuntimeConfig::default()
            },
        )
        .expect("first ordered runtime")
        .with_material_coordinator(coordinator.clone())
        .with_material_startup_permit(first_permit);
        let second_runtime = HistoricalRuntime::new(
            store.clone(),
            scripted.clone(),
            second.clone(),
            HistoricalRuntimeConfig {
                mapper_concurrency: 1,
                ..HistoricalRuntimeConfig::default()
            },
        )
        .expect("second ordered runtime")
        .with_material_coordinator(coordinator.clone())
        .with_material_startup_permit(second_permit);
        let first_job = BackfillJob::for_processor(
            "shared-ledger-first",
            first.as_ref(),
            ChainId(1),
            range,
            VerificationPolicy::CompleteCryptographic,
        )
        .expect("first ordered job");
        let second_job = BackfillJob::for_processor(
            "shared-ledger-second",
            second.as_ref(),
            ChainId(1),
            range,
            VerificationPolicy::CompleteCryptographic,
        )
        .expect("second ordered job");

        let (first_report, second_report) = tokio::join!(
            first_runtime.run(first_job, default_source_budget(), CancellationToken::new()),
            second_runtime.run(
                second_job,
                default_source_budget(),
                CancellationToken::new()
            ),
        );
        first_report.expect("first ordered backfill");
        second_report.expect("second ordered backfill");

        assert_eq!(scripted.open_calls(), 1);
        let first_digest = store
            .entity(first.descriptor(), "ledger", b"digest")
            .await
            .expect("first digest")
            .expect("first digest exists");
        let second_digest = store
            .entity(second.descriptor(), "ledger", b"digest")
            .await
            .expect("second digest")
            .expect("second digest exists");
        assert_eq!(first_digest, second_digest);
        assert_eq!(
            store
                .coverage(first.descriptor(), range)
                .await
                .expect("first ordered coverage"),
            vec![range]
        );
        assert_eq!(
            store
                .coverage(second.descriptor(), range)
                .await
                .expect("second ordered coverage"),
            vec![range]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(clippy::too_many_lines)]
    async fn overlapping_historical_ranges_fetch_the_union_once() {
        let available = BlockRange::new(BlockNumber(0), BlockNumber(6)).expect("available range");
        let first_range = BlockRange::new(BlockNumber(0), BlockNumber(4)).expect("first range");
        let second_range = BlockRange::new(BlockNumber(2), BlockNumber(6)).expect("second range");
        let scripted = Arc::new(ScriptedHistorySource::new(
            fixture_source_descriptor("overlapping-history", available),
            vec![ScriptedChunk {
                range: available,
                schema_version: "fixture-v1".to_owned(),
                estimated_bytes: None,
                steps: std::iter::once(HistoryStep::Delay(Duration::from_millis(25)))
                    .chain(
                        frames(available)
                            .into_iter()
                            .map(|frame| HistoryStep::Frame(Box::new(frame))),
                    )
                    .collect(),
            }],
        ));
        let coordinator = HistoricalMaterialCoordinator::new(HistoricalMaterialCoordinatorConfig {
            mode: HistoricalMaterialCoordinatorMode::Enabled,
            memory_bytes: 4 * 1_024 * 1_024,
            maximum_buffered_frames_per_acquisition: 8,
            ..HistoricalMaterialCoordinatorConfig::default()
        })
        .expect("material coordinator");
        let mut permits = coordinator.startup_batch(2);
        let second_permit = permits.pop().expect("second startup permit");
        let first_permit = permits.pop().expect("first startup permit");
        let (_directory, store) = store().await;
        let first_processor = Arc::new(BlockLocalCounter::named("overlap-first"));
        let second_processor = Arc::new(BlockLocalCounter::named("overlap-second"));
        let first_runtime = HistoricalRuntime::new(
            store.clone(),
            scripted.clone(),
            first_processor.clone(),
            HistoricalRuntimeConfig {
                mapper_concurrency: 1,
                ..HistoricalRuntimeConfig::default()
            },
        )
        .expect("first runtime")
        .with_material_coordinator(coordinator.clone())
        .with_material_startup_permit(first_permit);
        let second_runtime = HistoricalRuntime::new(
            store.clone(),
            scripted.clone(),
            second_processor.clone(),
            HistoricalRuntimeConfig {
                mapper_concurrency: 1,
                ..HistoricalRuntimeConfig::default()
            },
        )
        .expect("second runtime")
        .with_material_coordinator(coordinator.clone())
        .with_material_startup_permit(second_permit);
        let first_job = BackfillJob::for_processor(
            "overlap-first",
            first_processor.as_ref(),
            ChainId(1),
            first_range,
            VerificationPolicy::CompleteCryptographic,
        )
        .expect("first job");
        let second_job = BackfillJob::for_processor(
            "overlap-second",
            second_processor.as_ref(),
            ChainId(1),
            second_range,
            VerificationPolicy::CompleteCryptographic,
        )
        .expect("second job");

        let (first, second) = tokio::join!(
            first_runtime.run(first_job, default_source_budget(), CancellationToken::new()),
            second_runtime.run(
                second_job,
                default_source_budget(),
                CancellationToken::new()
            ),
        );
        let first = first.expect("first backfill");
        let second = second.expect("second backfill");

        assert_eq!(scripted.open_calls(), 1);
        assert_eq!(coordinator.snapshot().acquisitions_started, 1);
        assert_eq!(coordinator.snapshot().physical_frames, available.len());
        assert_eq!(
            coordinator.snapshot().logical_frame_deliveries,
            first_range.len().saturating_add(second_range.len())
        );
        assert_eq!(
            first
                .coalesced_frames
                .saturating_add(second.coalesced_frames),
            3
        );
        assert_eq!(
            store
                .coverage(first_processor.descriptor(), first_range)
                .await
                .expect("first coverage"),
            vec![first_range]
        );
        assert_eq!(
            store
                .coverage(second_processor.descriptor(), second_range)
                .await
                .expect("second coverage"),
            vec![second_range]
        );
    }

    #[tokio::test]
    async fn a_late_overlapping_job_joins_after_an_acquisition_prefix_is_released() {
        let available = BlockRange::new(BlockNumber(0), BlockNumber(6)).expect("available range");
        let first_range = BlockRange::new(BlockNumber(0), BlockNumber(4)).expect("first range");
        let second_range = BlockRange::new(BlockNumber(2), BlockNumber(6)).expect("second range");
        let scripted = Arc::new(ScriptedHistorySource::from_frames(
            fixture_source_descriptor("late-overlap", available),
            frames(available),
        ));
        let first_request = request(first_range);
        let second_request = request(second_range);
        let first_plan = scripted
            .plan(&first_request)
            .await
            .expect("first source plan");
        let second_plan = scripted
            .plan(&second_request)
            .await
            .expect("second source plan");
        let policy =
            HistoricalSourcePolicy::from_sources(&[scripted.clone() as Arc<dyn HistorySource>]);
        let coordinator = HistoricalMaterialCoordinator::new(HistoricalMaterialCoordinatorConfig {
            mode: HistoricalMaterialCoordinatorMode::Enabled,
            memory_bytes: 1_024 * 1_024,
            maximum_buffered_frames_per_acquisition: 8,
            ..HistoricalMaterialCoordinatorConfig::default()
        })
        .expect("material coordinator");
        let mut first = coordinator
            .open(
                policy.clone(),
                scripted.clone(),
                &first_request,
                first_plan.chunks[0].clone(),
                default_source_budget(),
                CancellationToken::new(),
            )
            .expect("first subscription");
        for expected in 0..2 {
            let material = first
                .next()
                .await
                .expect("first prefix frame")
                .expect("valid first prefix");
            assert_eq!(material.frame().block.number, BlockNumber(expected));
            material.acknowledge();
        }
        let second = coordinator
            .open(
                policy,
                scripted.clone(),
                &second_request,
                second_plan.chunks[0].clone(),
                default_source_budget(),
                CancellationToken::new(),
            )
            .expect("late subscription");

        let drain = |mut stream: historical_material::HistoricalMaterialStream| async move {
            let mut numbers = Vec::new();
            while let Some(item) = stream.next().await {
                let material = item.expect("shared frame");
                numbers.push(material.frame().block.number.0);
                material.acknowledge();
            }
            numbers
        };
        let (first_tail, second_all) = tokio::join!(drain(first), drain(second));

        assert_eq!(first_tail, vec![2, 3, 4]);
        assert_eq!(second_all, vec![2, 3, 4, 5, 6]);
        assert_eq!(scripted.open_calls(), 2);
        assert_eq!(coordinator.snapshot().physical_frames, available.len());
        assert_eq!(coordinator.snapshot().logical_frame_deliveries, 10);
    }

    #[tokio::test]
    async fn cancelling_one_material_subscriber_keeps_the_shared_source_alive() {
        let range = BlockRange::new(BlockNumber(0), BlockNumber(1)).expect("shared material range");
        let scripted = Arc::new(ScriptedHistorySource::new(
            fixture_source_descriptor("shared-cancellation", range),
            vec![ScriptedChunk {
                range,
                schema_version: "fixture-v1".to_owned(),
                estimated_bytes: None,
                steps: std::iter::once(HistoryStep::Delay(Duration::from_millis(10)))
                    .chain(
                        frames(range)
                            .into_iter()
                            .map(|frame| HistoryStep::Frame(Box::new(frame))),
                    )
                    .collect(),
            }],
        ));
        let request = request(range);
        let plan = scripted.plan(&request).await.expect("source plan");
        let chunk = plan.chunks[0].clone();
        let policy =
            HistoricalSourcePolicy::from_sources(&[scripted.clone() as Arc<dyn HistorySource>]);
        let coordinator = HistoricalMaterialCoordinator::new(HistoricalMaterialCoordinatorConfig {
            mode: HistoricalMaterialCoordinatorMode::Enabled,
            memory_bytes: 1_024 * 1_024,
            maximum_buffered_frames_per_acquisition: 4,
            ..HistoricalMaterialCoordinatorConfig::default()
        })
        .expect("material coordinator");
        let first_cancellation = CancellationToken::new();
        let mut first = coordinator
            .open(
                policy.clone(),
                scripted.clone(),
                &request,
                chunk.clone(),
                default_source_budget(),
                first_cancellation.clone(),
            )
            .expect("first subscription");
        let mut second = coordinator
            .open(
                policy,
                scripted.clone(),
                &request,
                chunk,
                default_source_budget(),
                CancellationToken::new(),
            )
            .expect("second subscription");

        first_cancellation.cancel();
        assert!(matches!(
            first.next().await,
            Some(Err(SourceError::Cancelled))
        ));
        drop(first);

        let mut received = 0_u64;
        while let Some(item) = second.next().await {
            let frame = item.expect("remaining subscriber frame");
            frame.acknowledge();
            received = received.saturating_add(1);
        }
        assert_eq!(received, range.len());
        assert_eq!(scripted.open_calls(), 1);
        assert_eq!(coordinator.snapshot().physical_frames, range.len());
    }

    #[tokio::test]
    async fn cancelling_the_final_material_subscriber_cancels_the_source() {
        let range = BlockRange::new(BlockNumber(0), BlockNumber(0)).expect("one-block range");
        let scripted = Arc::new(ScriptedHistorySource::new(
            fixture_source_descriptor("final-subscriber-cancellation", range),
            vec![ScriptedChunk {
                range,
                schema_version: "fixture-v1".to_owned(),
                estimated_bytes: None,
                steps: vec![
                    HistoryStep::Delay(Duration::from_secs(30)),
                    HistoryStep::Frame(Box::new(
                        frames(range).into_iter().next().expect("fixture frame"),
                    )),
                ],
            }],
        ));
        let request = request(range);
        let plan = scripted.plan(&request).await.expect("source plan");
        let policy =
            HistoricalSourcePolicy::from_sources(&[scripted.clone() as Arc<dyn HistorySource>]);
        let coordinator = HistoricalMaterialCoordinator::new(HistoricalMaterialCoordinatorConfig {
            mode: HistoricalMaterialCoordinatorMode::Enabled,
            memory_bytes: 1_024 * 1_024,
            maximum_buffered_frames_per_acquisition: 2,
            ..HistoricalMaterialCoordinatorConfig::default()
        })
        .expect("material coordinator");
        let subscription = coordinator
            .open(
                policy,
                scripted.clone(),
                &request,
                plan.chunks[0].clone(),
                default_source_budget(),
                CancellationToken::new(),
            )
            .expect("subscription");

        tokio::time::timeout(Duration::from_secs(1), async {
            while scripted.open_calls() == 0 || coordinator.snapshot().active_acquisitions == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("source acquisition starts");
        drop(subscription);
        tokio::time::timeout(Duration::from_secs(1), async {
            while coordinator.snapshot().active_acquisitions != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("source acquisition stops");

        assert_eq!(scripted.open_calls(), 1);
        assert_eq!(coordinator.snapshot().physical_frames, 0);
        assert_eq!(coordinator.snapshot().buffered_bytes, 0);
    }

    #[tokio::test]
    async fn historical_material_rejects_a_frame_larger_than_the_global_memory_budget() {
        let range = BlockRange::new(BlockNumber(0), BlockNumber(0)).expect("one-block range");
        let mut material_frames = frames(range);
        material_frames[0].header = Material::Complete(HeaderEnvelope {
            rlp: Some(vec![0_u8; 2]),
            transactions_root: None,
            receipts_root: None,
            withdrawals_root: None,
            gas_limit: None,
            gas_used: None,
            base_fee_per_gas: None,
            blob_gas_used: None,
            excess_blob_gas: None,
            size_bytes: None,
            transaction_count: None,
            consensus_size_bytes: None,
        });
        let scripted = Arc::new(ScriptedHistorySource::from_frames(
            fixture_source_descriptor("material-memory-budget", range),
            material_frames,
        ));
        let request = request(range);
        let plan = scripted.plan(&request).await.expect("source plan");
        let policy =
            HistoricalSourcePolicy::from_sources(&[scripted.clone() as Arc<dyn HistorySource>]);
        let coordinator = HistoricalMaterialCoordinator::new(HistoricalMaterialCoordinatorConfig {
            mode: HistoricalMaterialCoordinatorMode::Enabled,
            memory_bytes: 1,
            maximum_buffered_frames_per_acquisition: 2,
            ..HistoricalMaterialCoordinatorConfig::default()
        })
        .expect("material coordinator");
        let mut subscription = coordinator
            .open(
                policy,
                scripted,
                &request,
                plan.chunks[0].clone(),
                default_source_budget(),
                CancellationToken::new(),
            )
            .expect("subscription");

        match subscription.next().await {
            Some(Err(SourceError::BudgetExceeded {
                resource: "historical_material_memory",
                limit: 1,
                observed,
            })) if observed > 1 => {}
            other => panic!("expected historical material memory budget error, got {other:?}"),
        }
        assert_eq!(coordinator.snapshot().physical_frames, 1);
        assert!(coordinator.snapshot().physical_bytes > 1);
        assert_eq!(coordinator.snapshot().buffered_bytes, 0);
    }

    #[tokio::test]
    async fn incompatible_source_policies_do_not_share_an_acquisition() {
        let range = BlockRange::new(BlockNumber(0), BlockNumber(0)).expect("one-block range");
        let scripted = Arc::new(ScriptedHistorySource::from_frames(
            fixture_source_descriptor("source-policy", range),
            frames(range),
        ));
        let request = request(range);
        let plan = scripted.plan(&request).await.expect("source plan");
        let coordinator = HistoricalMaterialCoordinator::new(HistoricalMaterialCoordinatorConfig {
            mode: HistoricalMaterialCoordinatorMode::Enabled,
            memory_bytes: 1_024 * 1_024,
            maximum_buffered_frames_per_acquisition: 2,
            ..HistoricalMaterialCoordinatorConfig::default()
        })
        .expect("material coordinator");
        let first_policy =
            HistoricalSourcePolicy::from_sources(&[scripted.clone() as Arc<dyn HistorySource>]);
        let mut second_policy = first_policy.clone();
        second_policy.sources[0].priority = second_policy.sources[0].priority.saturating_add(1);
        let mut first = coordinator
            .open(
                first_policy,
                scripted.clone(),
                &request,
                plan.chunks[0].clone(),
                default_source_budget(),
                CancellationToken::new(),
            )
            .expect("first subscription");
        let mut second = coordinator
            .open(
                second_policy,
                scripted.clone(),
                &request,
                plan.chunks[0].clone(),
                default_source_budget(),
                CancellationToken::new(),
            )
            .expect("second subscription");

        let (first_frame, second_frame) = tokio::join!(first.next(), second.next());
        first_frame
            .expect("first item")
            .expect("first frame")
            .acknowledge();
        second_frame
            .expect("second item")
            .expect("second frame")
            .acknowledge();
        assert_eq!(scripted.open_calls(), 2);
        assert_eq!(coordinator.snapshot().acquisitions_started, 2);
        assert_eq!(coordinator.snapshot().requests_coalesced, 0);
    }

    #[tokio::test]
    async fn observe_mode_reports_compatible_requests_without_sharing_them() {
        let range = BlockRange::new(BlockNumber(0), BlockNumber(0)).expect("one-block range");
        let scripted = Arc::new(ScriptedHistorySource::new(
            fixture_source_descriptor("observe-material", range),
            vec![ScriptedChunk {
                range,
                schema_version: "fixture-v1".to_owned(),
                estimated_bytes: None,
                steps: vec![
                    HistoryStep::Delay(Duration::from_millis(10)),
                    HistoryStep::Frame(Box::new(frames(range).remove(0))),
                ],
            }],
        ));
        let request = request(range);
        let plan = scripted.plan(&request).await.expect("source plan");
        let policy =
            HistoricalSourcePolicy::from_sources(&[scripted.clone() as Arc<dyn HistorySource>]);
        let coordinator = HistoricalMaterialCoordinator::new(HistoricalMaterialCoordinatorConfig {
            mode: HistoricalMaterialCoordinatorMode::Observe,
            memory_bytes: 1_024 * 1_024,
            maximum_buffered_frames_per_acquisition: 2,
            ..HistoricalMaterialCoordinatorConfig::default()
        })
        .expect("material coordinator");
        let mut first = coordinator
            .open(
                policy.clone(),
                scripted.clone(),
                &request,
                plan.chunks[0].clone(),
                default_source_budget(),
                CancellationToken::new(),
            )
            .expect("first subscription");
        let mut second = coordinator
            .open(
                policy,
                scripted.clone(),
                &request,
                plan.chunks[0].clone(),
                default_source_budget(),
                CancellationToken::new(),
            )
            .expect("second subscription");

        let (first_frame, second_frame) = tokio::join!(first.next(), second.next());
        first_frame
            .expect("first item")
            .expect("first frame")
            .acknowledge();
        second_frame
            .expect("second item")
            .expect("second frame")
            .acknowledge();
        assert_eq!(scripted.open_calls(), 2);
        assert_eq!(coordinator.snapshot().acquisitions_started, 2);
        assert_eq!(coordinator.snapshot().requests_coalesced, 0);
        assert_eq!(coordinator.snapshot().requests_coalescible, 1);
    }

    #[tokio::test]
    async fn startup_registration_batch_blocks_physical_reads_until_every_job_arrives() {
        let range = BlockRange::new(BlockNumber(0), BlockNumber(0)).expect("one-block range");
        let scripted = Arc::new(ScriptedHistorySource::from_frames(
            fixture_source_descriptor("startup-registration", range),
            frames(range),
        ));
        let request = request(range);
        let plan = scripted.plan(&request).await.expect("source plan");
        let policy =
            HistoricalSourcePolicy::from_sources(&[scripted.clone() as Arc<dyn HistorySource>]);
        let coordinator = HistoricalMaterialCoordinator::new(HistoricalMaterialCoordinatorConfig {
            mode: HistoricalMaterialCoordinatorMode::Enabled,
            memory_bytes: 1_024 * 1_024,
            maximum_buffered_frames_per_acquisition: 2,
            ..HistoricalMaterialCoordinatorConfig::default()
        })
        .expect("material coordinator");
        let mut permits = coordinator.startup_batch(2);
        let second_permit = permits.pop().expect("second permit");
        let first_permit = permits.pop().expect("first permit");
        let first_gate = first_permit.gate();
        let mut first = coordinator
            .open_after_startup_registration(
                policy.clone(),
                scripted.clone(),
                &request,
                plan.chunks[0].clone(),
                default_source_budget(),
                CancellationToken::new(),
                first_gate,
            )
            .expect("first subscription");
        let _ = first_permit.arrive();
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(scripted.open_calls(), 0);

        let second_gate = second_permit.gate();
        let mut second = coordinator
            .open_after_startup_registration(
                policy,
                scripted.clone(),
                &request,
                plan.chunks[0].clone(),
                default_source_budget(),
                CancellationToken::new(),
                second_gate,
            )
            .expect("second subscription");
        let _ = second_permit.arrive();
        let (first_frame, second_frame) = tokio::join!(first.next(), second.next());
        first_frame
            .expect("first item")
            .expect("first frame")
            .acknowledge();
        second_frame
            .expect("second item")
            .expect("second frame")
            .acknowledge();

        assert_eq!(scripted.open_calls(), 1);
        assert_eq!(coordinator.snapshot().requests_coalesced, 1);
    }

    #[tokio::test]
    async fn recompute_backfill_republishes_complete_block_local_coverage() {
        let range = BlockRange::new(BlockNumber(1), BlockNumber(3)).expect("range");
        let source = Arc::new(ScriptedHistorySource::from_frames(
            fixture_source_descriptor("recompute-history", range),
            frames(range),
        ));
        let processor = Arc::new(BlockLocalCounter::default());
        let (_directory, store) = store().await;
        let runtime = HistoricalRuntime::new(
            store.clone(),
            source,
            processor.clone(),
            HistoricalRuntimeConfig {
                mapper_concurrency: 2,
                ..HistoricalRuntimeConfig::default()
            },
        )
        .expect("runtime");
        runtime
            .run(
                BackfillJob {
                    id: "recompute-seed".to_owned(),
                    owner: HistoricalJobOwner::Materialization,
                    processor_instance: processor.descriptor().instance.to_string(),
                    mode: BackfillMode::FillMissing,
                    delivery_stream_id: None,
                    ranges: vec![range],
                    request: request(range),
                    sink_ids: Vec::new(),
                },
                default_source_budget(),
                CancellationToken::new(),
            )
            .await
            .expect("seed coverage");
        let original_cursor = store
            .processor_cursor(processor.descriptor())
            .await
            .expect("cursor")
            .expect("processor cursor");

        let report = runtime
            .run(
                BackfillJob {
                    id: "recompute-replay".to_owned(),
                    owner: HistoricalJobOwner::Materialization,
                    processor_instance: processor.descriptor().instance.to_string(),
                    mode: BackfillMode::Recompute,
                    delivery_stream_id: None,
                    ranges: vec![range],
                    request: request(range),
                    sink_ids: Vec::new(),
                },
                default_source_budget(),
                CancellationToken::new(),
            )
            .await
            .expect("recompute");
        assert_eq!(report.frames_mapped, range.len());
        assert_eq!(report.frames_committed, range.len());
        assert_eq!(report.initial_coverage, vec![range]);
        assert_eq!(report.final_coverage, vec![range]);
        assert_eq!(
            store
                .changes(processor.descriptor(), ChainId(1), 0, 100)
                .await
                .expect("changes")
                .len(),
            usize::try_from(range.len().saturating_mul(2)).expect("change count")
        );
        assert_eq!(
            store
                .processor_cursor(processor.descriptor())
                .await
                .expect("cursor"),
            Some(original_cursor)
        );
    }

    #[tokio::test]
    async fn recompute_reacquires_and_verifies_compact_coverage_segments() {
        let range = BlockRange::new(BlockNumber(1), BlockNumber(4)).expect("range");
        let processor = Arc::new(
            BlockLocalCounter::default()
                .with_split_delivery()
                .with_output_none(),
        );
        let (_directory, store) = store().await;
        let seed_job = externalized_subscription_job(
            &store,
            processor.as_ref(),
            "compact-recompute-seed",
            range,
            BackfillMode::FillMissing,
            0,
        )
        .await;
        let seed_source = Arc::new(ScriptedHistorySource::from_frames(
            fixture_source_descriptor("compact-recompute-seed-source", range),
            frames(range),
        ));
        HistoricalRuntime::new(
            store.clone(),
            seed_source,
            processor.clone(),
            HistoricalRuntimeConfig {
                mapper_concurrency: 2,
                ..HistoricalRuntimeConfig::default()
            },
        )
        .expect("seed runtime")
        .run(seed_job, default_source_budget(), CancellationToken::new())
        .await
        .expect("seed exact coverage");
        let compacted = store
            .compact_finalized_coverage(processor.descriptor(), range.end(), 2, range.len())
            .await
            .expect("compact coverage");
        assert_eq!(compacted.exact_coverage_deleted, range.len());

        let recompute_job = externalized_subscription_job(
            &store,
            processor.as_ref(),
            "compact-recompute-replay",
            range,
            BackfillMode::Recompute,
            1,
        )
        .await;
        let recompute_stream = recompute_job
            .delivery_stream_id
            .clone()
            .expect("recompute stream");
        let recompute_source = Arc::new(ScriptedHistorySource::from_frames(
            fixture_source_descriptor("compact-recompute-source", range),
            frames(range),
        ));
        let report = HistoricalRuntime::new(
            store.clone(),
            recompute_source,
            processor.clone(),
            HistoricalRuntimeConfig {
                mapper_concurrency: 2,
                ..HistoricalRuntimeConfig::default()
            },
        )
        .expect("recompute runtime")
        .run(
            recompute_job,
            default_source_budget(),
            CancellationToken::new(),
        )
        .await
        .expect("verified compact recompute");
        assert_eq!(report.frames_committed, range.len());
        assert_eq!(report.initial_coverage, vec![range]);
        let changes = store
            .changes_in_stream(
                processor.descriptor(),
                &recompute_stream,
                ChainId(1),
                0,
                100,
            )
            .await
            .expect("recomputed delivery");
        assert_eq!(
            changes
                .iter()
                .filter(|record| record.change.kind == "synthetic.counter")
                .count(),
            usize::try_from(range.len()).expect("change count")
        );
    }

    #[tokio::test]
    async fn terminal_historical_job_spools_only_its_final_result() {
        let range = BlockRange::new(BlockNumber(1), BlockNumber(3)).expect("range");
        let source = Arc::new(ScriptedHistorySource::from_frames(
            fixture_source_descriptor("terminal-history", range),
            frames(range),
        ));
        let processor = Arc::new(
            BlockLocalCounter::default().with_publication(PublicationPolicy::TerminalOnly),
        );
        let (_directory, store) = store().await;
        let runtime = HistoricalRuntime::new(
            store.clone(),
            source,
            processor.clone(),
            HistoricalRuntimeConfig {
                mapper_concurrency: 2,
                ..HistoricalRuntimeConfig::default()
            },
        )
        .expect("runtime");
        runtime
            .run(
                BackfillJob {
                    id: "terminal-backfill".to_owned(),
                    owner: HistoricalJobOwner::Materialization,
                    processor_instance: processor.descriptor().instance.to_string(),
                    mode: BackfillMode::FillMissing,
                    delivery_stream_id: None,
                    ranges: vec![range],
                    request: request(range),
                    sink_ids: Vec::new(),
                },
                default_source_budget(),
                CancellationToken::new(),
            )
            .await
            .expect("terminal run");
        let changes = store
            .changes(processor.descriptor(), ChainId(1), 0, 10)
            .await
            .expect("changes");
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].block.number, range.end());
        assert_eq!(
            store
                .coverage(processor.descriptor(), range)
                .await
                .expect("coverage"),
            vec![range]
        );
        assert_eq!(
            store
                .job("terminal-backfill")
                .await
                .expect("job")
                .expect("record")
                .state,
            JobState::Completed
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn historical_materialization_commits_external_segments_before_coverage() {
        let range = BlockRange::new(BlockNumber(1), BlockNumber(5)).expect("range");
        let source_frames = frames(range);
        let source = Arc::new(ScriptedHistorySource::from_frames(
            fixture_source_descriptor("external-artifact-history", range),
            source_frames.clone(),
        ));
        let base = BlockLocalCounter::default();
        let mut lifecycle = base.descriptor().lifecycle.clone();
        lifecycle.artifacts.mode = leani_processor_api::ArtifactPolicyMode::Full;
        lifecycle.output.mode = OutputPolicyMode::None;
        lifecycle.delivery.mode = DeliveryPolicyMode::None;
        lifecycle.delivery.consumers.clear();
        let processor = Arc::new(base.with_lifecycle(lifecycle));
        let (_directory, store) = store().await;
        let artifact_directory = tempfile::tempdir().expect("artifact tempdir");
        let artifact_sink = Arc::new(
            ArtifactSegmentSink::open(
                artifact_directory.path(),
                ArtifactSegmentSinkConfig {
                    compression: ArtifactCompression::Snappy,
                    limits: ArtifactSegmentLimits {
                        maximum_artifact_logical_bytes: 1024 * 1024,
                        maximum_segment_logical_bytes: 16 * 1024 * 1024,
                        maximum_segment_physical_bytes: 16 * 1024 * 1024,
                    },
                    maximum_retained_physical_bytes: 32 * 1024 * 1024,
                },
            )
            .expect("artifact sink"),
        );
        let expected =
            futures::future::try_join_all(source_frames.iter().map(|frame| processor.map(frame)))
                .await
                .expect("map expected artifacts");
        artifact_sink
            .retain_finalized_batch(processor.descriptor(), &expected[..2])
            .await
            .expect("simulate segment commit before SQLite crash");
        assert!(
            store
                .processor_cursor(processor.descriptor())
                .await
                .expect("pre-run cursor")
                .is_none()
        );
        let runtime = HistoricalRuntime::new(
            store.clone(),
            source,
            processor.clone(),
            HistoricalRuntimeConfig {
                mapper_concurrency: 2,
                commit_maximum_blocks: 2,
                ..HistoricalRuntimeConfig::default()
            },
        )
        .expect("runtime")
        .with_artifact_sink(artifact_sink.clone())
        .expect("external artifact runtime");
        runtime
            .run(
                BackfillJob {
                    id: "external-artifact-backfill".to_owned(),
                    owner: HistoricalJobOwner::Materialization,
                    processor_instance: processor.descriptor().instance.to_string(),
                    mode: BackfillMode::FillMissing,
                    delivery_stream_id: None,
                    ranges: vec![range],
                    request: request(range),
                    sink_ids: Vec::new(),
                },
                default_source_budget(),
                CancellationToken::new(),
            )
            .await
            .expect("external artifact run");

        let segment_stats = artifact_sink.stats().await;
        // The adaptive writer may split the remaining three blocks as 2+1 or
        // 1+1+1 under a loaded test runner. The precommitted 1..=2 segment
        // must still be adopted rather than duplicated in either case.
        assert!((3..=4).contains(&segment_stats.segments));
        assert!(
            artifact_sink
                .retained_batch(
                    &processor.descriptor().instance.to_string(),
                    BlockRange::new(BlockNumber(1), BlockNumber(2)).expect("precommitted range"),
                )
                .await
                .is_some()
        );
        assert_eq!(segment_stats.artifacts, range.len());
        artifact_sink
            .verify(processor.descriptor())
            .await
            .expect("verify external artifacts");
        assert_eq!(
            artifact_sink
                .scan(processor.descriptor(), range, 10)
                .await
                .expect("scan external artifacts"),
            expected
        );
        let sqlite = store
            .processor_stats(processor.descriptor())
            .await
            .expect("SQLite stats");
        assert_eq!(sqlite.processor_artifacts, 0);
        assert_eq!(sqlite.entities, 0);
        assert_eq!(sqlite.applied_blocks, range.len());
        assert_eq!(
            store
                .coverage(processor.descriptor(), range)
                .await
                .expect("coverage"),
            vec![range]
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn historical_run_resumes_from_durable_coverage_after_process_reopen() {
        let range = BlockRange::new(BlockNumber(1), BlockNumber(3)).expect("range");
        let all_frames = frames(range);
        let descriptor = fixture_source_descriptor("history", range);
        let failed_source = Arc::new(ScriptedHistorySource::new(
            descriptor.clone(),
            vec![ScriptedChunk {
                range,
                schema_version: descriptor.schema_version.clone(),
                estimated_bytes: None,
                steps: vec![
                    HistoryStep::Frame(Box::new(all_frames[0].clone())),
                    HistoryStep::Error(SourceError::CorruptFrame(
                        "injected crash boundary".to_owned(),
                    )),
                ],
            }],
        ));
        let processor = Arc::new(BlockLocalCounter::default());
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("node.sqlite");
        let store = SqliteStore::open(leani_store_sqlite::StoreConfig::new(&path))
            .await
            .expect("store");
        let runtime = HistoricalRuntime::new(
            store.clone(),
            failed_source,
            processor.clone(),
            HistoricalRuntimeConfig {
                mapper_concurrency: 2,
                ..HistoricalRuntimeConfig::default()
            },
        )
        .expect("runtime");
        let job = BackfillJob {
            id: "reopen-backfill".to_owned(),
            owner: HistoricalJobOwner::Materialization,
            processor_instance: processor.descriptor().instance.to_string(),
            mode: BackfillMode::FillMissing,
            delivery_stream_id: None,
            ranges: vec![range],
            request: request(range),
            sink_ids: vec!["test".to_owned()],
        };
        let error = runtime
            .run(
                job.clone(),
                default_source_budget(),
                CancellationToken::new(),
            )
            .await
            .expect_err("injected failure");
        assert!(matches!(
            error,
            RuntimeError::Source(SourceError::CorruptFrame(_))
        ));
        assert_eq!(
            store
                .coverage(processor.descriptor(), range)
                .await
                .expect("partial coverage"),
            vec![BlockRange::single(BlockNumber(1))]
        );
        let epoch = store.epoch();
        drop(runtime);
        drop(store);

        let reopened = SqliteStore::open(leani_store_sqlite::StoreConfig::new(&path))
            .await
            .expect("reopen store");
        assert_eq!(reopened.epoch(), epoch);
        let healthy_source = Arc::new(ScriptedHistorySource::from_frames(descriptor, all_frames));
        let resumed_runtime = HistoricalRuntime::new(
            reopened.clone(),
            healthy_source,
            processor.clone(),
            HistoricalRuntimeConfig {
                mapper_concurrency: 2,
                ..HistoricalRuntimeConfig::default()
            },
        )
        .expect("resumed runtime");
        let report = resumed_runtime
            .run(job, default_source_budget(), CancellationToken::new())
            .await
            .expect("resume");
        assert_eq!(
            report.initial_coverage,
            vec![BlockRange::single(BlockNumber(1))]
        );
        assert_eq!(report.frames_mapped, 2);
        assert_eq!(report.frames_committed, 3);
        assert_eq!(report.final_coverage, vec![range]);
        assert_eq!(
            reopened
                .changes(processor.descriptor(), ChainId(1), 0, 10)
                .await
                .expect("changes")
                .len(),
            3
        );
        assert_eq!(
            reopened
                .job("reopen-backfill")
                .await
                .expect("job")
                .expect("durable job")
                .state,
            JobState::Completed
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn process_interruption_keeps_historical_job_resumable_after_reopen() {
        let range = BlockRange::new(BlockNumber(1), BlockNumber(3)).expect("range");
        let descriptor = fixture_source_descriptor("interruptible-history", range);
        let delayed_source = Arc::new(ScriptedHistorySource::new(
            descriptor.clone(),
            vec![ScriptedChunk {
                range,
                schema_version: descriptor.schema_version.clone(),
                estimated_bytes: None,
                steps: std::iter::once(HistoryStep::Delay(Duration::from_secs(30)))
                    .chain(
                        frames(range)
                            .into_iter()
                            .map(|frame| HistoryStep::Frame(Box::new(frame))),
                    )
                    .collect(),
            }],
        ));
        let processor = Arc::new(BlockLocalCounter::default());
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("node.sqlite");
        let store = SqliteStore::open(leani_store_sqlite::StoreConfig::new(&path))
            .await
            .expect("store");
        let runtime = HistoricalRuntime::new(
            store.clone(),
            delayed_source.clone(),
            processor.clone(),
            HistoricalRuntimeConfig {
                mapper_concurrency: 1,
                ..HistoricalRuntimeConfig::default()
            },
        )
        .expect("runtime");
        let job = BackfillJob {
            id: "interrupted-backfill".to_owned(),
            owner: HistoricalJobOwner::Materialization,
            processor_instance: processor.descriptor().instance.to_string(),
            mode: BackfillMode::FillMissing,
            delivery_stream_id: None,
            ranges: vec![range],
            request: request(range),
            sink_ids: Vec::new(),
        };
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let interrupted_job = job.clone();
        let task = tokio::spawn(async move {
            runtime
                .run(interrupted_job, default_source_budget(), task_cancellation)
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while delayed_source.open_calls() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("historical source opened");
        cancellation.cancel();
        assert!(matches!(
            task.await.expect("runtime task"),
            Err(RuntimeError::Cancelled)
        ));
        assert_eq!(
            store
                .job(&job.id)
                .await
                .expect("job")
                .expect("durable job")
                .state,
            JobState::Running
        );
        drop(store);

        let reopened = SqliteStore::open(leani_store_sqlite::StoreConfig::new(&path))
            .await
            .expect("reopen store");
        let healthy_source = Arc::new(ScriptedHistorySource::from_frames(
            descriptor,
            frames(range),
        ));
        let resumed = HistoricalRuntime::new(
            reopened.clone(),
            healthy_source,
            processor,
            HistoricalRuntimeConfig {
                mapper_concurrency: 1,
                ..HistoricalRuntimeConfig::default()
            },
        )
        .expect("resumed runtime")
        .run(
            job.clone(),
            default_source_budget(),
            CancellationToken::new(),
        )
        .await
        .expect("resumed backfill");
        assert_eq!(resumed.frames_committed, range.len());
        assert_eq!(
            reopened
                .job(&job.id)
                .await
                .expect("job")
                .expect("durable job")
                .state,
            JobState::Completed
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn backpressured_subscription_reopens_and_resumes_after_acknowledgement() {
        let range = BlockRange::new(BlockNumber(1), BlockNumber(6)).expect("range");
        let descriptor = fixture_source_descriptor("backpressured-history", range);
        let processor = Arc::new(
            BlockLocalCounter::default()
                .with_split_delivery()
                .with_output_none(),
        );
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("node.sqlite");
        let store = SqliteStore::open(leani_store_sqlite::StoreConfig::new(&path))
            .await
            .expect("store");
        let job = externalized_subscription_job_with_limits(
            &store,
            processor.as_ref(),
            "backpressured-restart",
            range,
            BackfillMode::FillMissing,
            0,
            2,
            64 * 1024 * 1024,
        )
        .await;
        let stream_id = job
            .delivery_stream_id
            .clone()
            .expect("history delivery stream");
        let first_source = Arc::new(ScriptedHistorySource::from_frames(
            descriptor.clone(),
            frames(range),
        ));
        let runtime = HistoricalRuntime::new(
            store.clone(),
            first_source,
            processor.clone(),
            HistoricalRuntimeConfig {
                mapper_concurrency: 1,
                commit_maximum_blocks: 1,
                commit_maximum_delay: Duration::from_mins(1),
                ..HistoricalRuntimeConfig::default()
            },
        )
        .expect("runtime");
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let interrupted_job = job.clone();
        let task = tokio::spawn(async move {
            runtime
                .run(interrupted_job, default_source_budget(), task_cancellation)
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let subscription = store
                    .backfill_subscription_for_job(&job.id)
                    .await
                    .expect("subscription")
                    .expect("durable subscription");
                if subscription.state
                    == leani_store_sqlite::BackfillSubscriptionState::Backpressured
                {
                    assert_eq!(subscription.processed_work_blocks, 2);
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("subscription reaches work-ahead limit");
        cancellation.cancel();
        assert!(matches!(
            task.await.expect("runtime task"),
            Err(RuntimeError::Cancelled)
        ));
        assert_eq!(
            store
                .job(&job.id)
                .await
                .expect("job")
                .expect("durable job")
                .state,
            JobState::Running
        );
        assert_eq!(
            store
                .coverage(processor.descriptor(), range)
                .await
                .expect("partial coverage"),
            vec![BlockRange::new(BlockNumber(1), BlockNumber(2)).expect("prefix")]
        );
        drop(store);

        let reopened = SqliteStore::open(leani_store_sqlite::StoreConfig::new(&path))
            .await
            .expect("reopen store");
        let committed = reopened
            .changes_in_stream(processor.descriptor(), &stream_id, ChainId(1), 0, 100)
            .await
            .expect("committed history prefix");
        let acknowledged = committed
            .iter()
            .rev()
            .find(|record| record.change.kind == "system.backfill_progress")
            .expect("progress boundary")
            .cursor
            .sequence;
        reopened
            .acknowledge_consumer_in_stream(
                processor.descriptor(),
                &stream_id,
                "destination-0",
                acknowledged,
            )
            .await
            .expect("acknowledge committed prefix");

        let resumed_source = Arc::new(ScriptedHistorySource::from_frames(
            descriptor,
            frames(range),
        ));
        let resumed_runtime = HistoricalRuntime::new(
            reopened.clone(),
            resumed_source.clone(),
            processor.clone(),
            HistoricalRuntimeConfig {
                mapper_concurrency: 1,
                commit_maximum_blocks: 1,
                commit_maximum_delay: Duration::from_mins(1),
                ..HistoricalRuntimeConfig::default()
            },
        )
        .expect("resumed runtime");
        let consumer_store = reopened.clone();
        let consumer_descriptor = processor.descriptor().clone();
        let consumer_stream = stream_id.clone();
        let consumer = tokio::spawn(async move {
            let mut after = acknowledged;
            loop {
                let changes = consumer_store
                    .changes_in_stream(
                        &consumer_descriptor,
                        &consumer_stream,
                        ChainId(1),
                        after,
                        100,
                    )
                    .await
                    .expect("consume resumed changes");
                if let Some(boundary) = changes.iter().rev().find(|record| {
                    matches!(
                        record.change.kind.as_str(),
                        "system.backfill_progress" | "system.backfill_complete"
                    )
                }) {
                    after = boundary.cursor.sequence;
                    consumer_store
                        .acknowledge_consumer_in_stream(
                            &consumer_descriptor,
                            &consumer_stream,
                            "destination-0",
                            after,
                        )
                        .await
                        .expect("acknowledge resumed boundary");
                    if boundary.change.kind == "system.backfill_complete" {
                        return after;
                    }
                } else {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }
        });
        let resumed = tokio::time::timeout(
            Duration::from_secs(2),
            resumed_runtime.run(
                job.clone(),
                default_source_budget(),
                CancellationToken::new(),
            ),
        )
        .await
        .expect("resumed backfill does not stall")
        .expect("resume after acknowledgement");
        let completion_ack = consumer.await.expect("consumer task");
        assert_eq!(
            resumed.initial_coverage,
            vec![BlockRange::new(BlockNumber(1), BlockNumber(2)).expect("prefix")]
        );
        assert_eq!(resumed.frames_mapped, 4);
        assert_eq!(resumed.frames_committed, range.len());
        assert_eq!(resumed_source.open_calls(), 1);
        let subscription = reopened
            .backfill_subscription_for_job(&job.id)
            .await
            .expect("subscription")
            .expect("durable subscription");
        assert_eq!(
            subscription.state,
            leani_store_sqlite::BackfillSubscriptionState::Draining
        );
        assert_eq!(subscription.processed_work_blocks, range.len());
        assert_eq!(subscription.completion_sequence, Some(completion_ack));
        let all_changes = reopened
            .changes_in_stream(processor.descriptor(), &stream_id, ChainId(1), 0, 100)
            .await
            .expect("complete history stream");
        assert_eq!(
            all_changes
                .iter()
                .filter(|record| record.change.kind == "synthetic.counter")
                .count(),
            usize::try_from(range.len()).expect("range length")
        );
        assert_eq!(
            all_changes
                .iter()
                .filter(|record| record.change.kind == "system.backfill_complete")
                .count(),
            1
        );
        drop(reopened);

        let draining_reopen = SqliteStore::open(leani_store_sqlite::StoreConfig::new(path))
            .await
            .expect("reopen draining subscription");
        assert_eq!(
            draining_reopen
                .backfill_subscription_for_job(&job.id)
                .await
                .expect("subscription")
                .expect("durable subscription")
                .state,
            leani_store_sqlite::BackfillSubscriptionState::Draining
        );
        assert!(
            draining_reopen
                .mark_backfill_subscription_reclaimable(&job.id, completion_ack)
                .await
                .expect("mark draining subscription reclaimable")
        );
        assert_eq!(
            draining_reopen
                .backfill_subscription_for_job(&job.id)
                .await
                .expect("subscription")
                .expect("durable subscription")
                .state,
            leani_store_sqlite::BackfillSubscriptionState::CompleteReclaimable
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn full_materialization_reopens_after_physical_limit_is_raised() {
        let range = BlockRange::new(BlockNumber(1), BlockNumber(3)).expect("range");
        let descriptor = fixture_source_descriptor("bounded-materialization", range);
        let processor = Arc::new(BlockLocalCounter::default().with_delivery_none());
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("node.sqlite");
        let bounded_store = SqliteStore::open(
            leani_store_sqlite::StoreConfig::new(&path)
                .with_storage_budget(leani_store_sqlite::StoreStorageBudget {
                    maximum_physical_bytes: 1,
                })
                .with_delivery_budget(leani_store_sqlite::DeliveryStorageBudget {
                    maximum_retained_bytes: 1,
                    maximum_history_retained_bytes: 1,
                }),
        )
        .await
        .expect("bounded store");
        let job = BackfillJob {
            id: "bounded-materialization".to_owned(),
            owner: HistoricalJobOwner::Materialization,
            processor_instance: processor.descriptor().instance.to_string(),
            mode: BackfillMode::FillMissing,
            delivery_stream_id: None,
            ranges: vec![range],
            request: request(range),
            sink_ids: Vec::new(),
        };
        let bounded_source = Arc::new(ScriptedHistorySource::from_frames(
            descriptor.clone(),
            frames(range),
        ));
        let error = HistoricalRuntime::new(
            bounded_store.clone(),
            bounded_source,
            processor.clone(),
            HistoricalRuntimeConfig {
                mapper_concurrency: 1,
                ..HistoricalRuntimeConfig::default()
            },
        )
        .expect("bounded runtime")
        .run(
            job.clone(),
            default_source_budget(),
            CancellationToken::new(),
        )
        .await
        .expect_err("physical budget pauses full materialization");
        assert!(matches!(
            error,
            RuntimeError::Store(StoreError::PhysicalStorageLimit { limit_bytes: 1, .. })
        ));
        assert_eq!(
            bounded_store
                .job(&job.id)
                .await
                .expect("job")
                .expect("durable job")
                .state,
            JobState::StorageBackpressured
        );
        assert!(
            bounded_store
                .coverage(processor.descriptor(), range)
                .await
                .expect("coverage")
                .is_empty()
        );
        let bounded_stats = bounded_store.stats().await.expect("bounded stats");
        assert_eq!(bounded_stats.entities, 0);
        assert_eq!(bounded_stats.changes, 0);
        assert_eq!(bounded_stats.history_delivery_retained_bytes, 0);
        drop(bounded_store);

        let reopened = SqliteStore::open(leani_store_sqlite::StoreConfig::new(path))
            .await
            .expect("reopen with raised physical budget");
        let resumed_source = Arc::new(ScriptedHistorySource::from_frames(
            descriptor,
            frames(range),
        ));
        let report = HistoricalRuntime::new(
            reopened.clone(),
            resumed_source,
            processor.clone(),
            HistoricalRuntimeConfig {
                mapper_concurrency: 1,
                ..HistoricalRuntimeConfig::default()
            },
        )
        .expect("resumed runtime")
        .run(
            job.clone(),
            default_source_budget(),
            CancellationToken::new(),
        )
        .await
        .expect("resume full materialization");
        assert_eq!(report.frames_committed, range.len());
        assert_eq!(
            reopened
                .job(&job.id)
                .await
                .expect("job")
                .expect("durable job")
                .state,
            JobState::Completed
        );
        let resumed_stats = reopened.stats().await.expect("resumed stats");
        assert_eq!(resumed_stats.entities, range.len());
        assert_eq!(resumed_stats.changes, 0);
        assert_eq!(resumed_stats.history_delivery_retained_bytes, 0);
    }

    #[tokio::test]
    async fn historical_run_fails_over_at_a_committed_chunk_boundary() {
        let range = BlockRange::new(BlockNumber(1), BlockNumber(3)).expect("range");
        let all_frames = frames(range);
        let mut primary_descriptor = fixture_source_descriptor("primary", range);
        primary_descriptor.priority = 0;
        let primary: Arc<dyn HistorySource> = Arc::new(ScriptedHistorySource::new(
            primary_descriptor.clone(),
            vec![ScriptedChunk {
                range,
                schema_version: primary_descriptor.schema_version,
                estimated_bytes: None,
                steps: vec![
                    HistoryStep::Frame(Box::new(all_frames[0].clone())),
                    HistoryStep::Error(SourceError::Unavailable(
                        "injected primary outage".to_owned(),
                    )),
                ],
            }],
        ));
        let mut fallback_descriptor = fixture_source_descriptor("fallback", range);
        fallback_descriptor.priority = 1;
        let fallback: Arc<dyn HistorySource> = Arc::new(ScriptedHistorySource::from_frames(
            fallback_descriptor,
            all_frames,
        ));
        let processor = Arc::new(BlockLocalCounter::default());
        let (_directory, store) = store().await;
        let runtime = HistoricalRuntime::new_with_sources(
            store.clone(),
            vec![fallback, primary],
            processor.clone(),
            HistoricalRuntimeConfig {
                mapper_concurrency: 2,
                max_attempts: 3,
                retry_base: Duration::from_millis(1),
                retry_max: Duration::from_millis(1),
                ..HistoricalRuntimeConfig::default()
            },
        )
        .expect("runtime");
        let report = runtime
            .run(
                BackfillJob {
                    id: "source-failover".to_owned(),
                    owner: HistoricalJobOwner::Materialization,
                    processor_instance: processor.descriptor().instance.to_string(),
                    mode: BackfillMode::FillMissing,
                    delivery_stream_id: None,
                    ranges: vec![range],
                    request: request(range),
                    sink_ids: Vec::new(),
                },
                default_source_budget(),
                CancellationToken::new(),
            )
            .await
            .expect("fallback completes");
        assert_eq!(report.source_id, "fallback,primary");
        assert_eq!(report.source_attempts, 2);
        assert_eq!(report.frames_committed, 3);
        assert_eq!(report.final_coverage, vec![range]);
        assert_eq!(report.sources.len(), 2);
        let primary = report
            .sources
            .iter()
            .find(|source| source.source_id == "primary")
            .expect("primary source report");
        assert_eq!(primary.attempts, 1);
        assert_eq!(primary.failures, 1);
        assert_eq!(primary.frames_committed, 1);
        assert!(primary.last_error.is_some());
        let fallback = report
            .sources
            .iter()
            .find(|source| source.source_id == "fallback")
            .expect("fallback source report");
        assert_eq!(fallback.attempts, 1);
        assert_eq!(fallback.failures, 0);
        assert_eq!(fallback.frames_committed, 2);
        assert_eq!(
            report
                .sources
                .iter()
                .map(|source| source.source_bytes)
                .sum::<u64>(),
            report.source_bytes
        );
        assert_eq!(
            store
                .changes(processor.descriptor(), ChainId(1), 0, 10)
                .await
                .expect("changes")
                .len(),
            3
        );
    }

    #[tokio::test]
    async fn historical_failover_rejects_a_cross_source_parent_break() {
        let range = BlockRange::new(BlockNumber(1), BlockNumber(3)).expect("range");
        let all_frames = frames(range);
        let mut primary_descriptor = fixture_source_descriptor("primary-parent", range);
        primary_descriptor.priority = 0;
        let primary: Arc<dyn HistorySource> = Arc::new(ScriptedHistorySource::new(
            primary_descriptor.clone(),
            vec![ScriptedChunk {
                range,
                schema_version: primary_descriptor.schema_version,
                estimated_bytes: None,
                steps: vec![
                    HistoryStep::Frame(Box::new(all_frames[0].clone())),
                    HistoryStep::Error(SourceError::Unavailable(
                        "injected primary outage".to_owned(),
                    )),
                ],
            }],
        ));
        let mut bad_frames = all_frames;
        bad_frames[1].block.parent_hash = BlockHash::new([0xff; 32]);
        let mut fallback_descriptor = fixture_source_descriptor("fallback-parent", range);
        fallback_descriptor.priority = 1;
        let fallback: Arc<dyn HistorySource> = Arc::new(ScriptedHistorySource::from_frames(
            fallback_descriptor,
            bad_frames,
        ));
        let processor = Arc::new(BlockLocalCounter::default());
        let (_directory, store) = store().await;
        let runtime = HistoricalRuntime::new_with_sources(
            store,
            vec![fallback, primary],
            processor.clone(),
            HistoricalRuntimeConfig {
                mapper_concurrency: 2,
                max_attempts: 3,
                retry_base: Duration::from_millis(1),
                retry_max: Duration::from_millis(1),
                ..HistoricalRuntimeConfig::default()
            },
        )
        .expect("runtime");
        let error = runtime
            .run(
                BackfillJob {
                    id: "source-parent-break".to_owned(),
                    owner: HistoricalJobOwner::Materialization,
                    processor_instance: processor.descriptor().instance.to_string(),
                    mode: BackfillMode::FillMissing,
                    delivery_stream_id: None,
                    ranges: vec![range],
                    request: request(range),
                    sink_ids: Vec::new(),
                },
                default_source_budget(),
                CancellationToken::new(),
            )
            .await
            .expect_err("cross-source parent break");
        assert!(error.to_string().contains("parent mismatch at block 2"));
    }

    #[tokio::test]
    async fn incomplete_chunk_never_claims_complete_coverage() {
        let range = BlockRange::new(BlockNumber(1), BlockNumber(3)).expect("range");
        let source = Arc::new(ScriptedHistorySource::new(
            fixture_source_descriptor("history", range),
            vec![ScriptedChunk {
                range,
                schema_version: "fixture-v1".to_owned(),
                estimated_bytes: None,
                steps: frames(BlockRange::new(BlockNumber(1), BlockNumber(2)).expect("short"))
                    .into_iter()
                    .map(|frame| HistoryStep::Frame(Box::new(frame)))
                    .collect(),
            }],
        ));
        let processor = Arc::new(BlockLocalCounter::default());
        let (_directory, store) = store().await;
        let runtime = HistoricalRuntime::new(
            store.clone(),
            source,
            processor.clone(),
            HistoricalRuntimeConfig {
                mapper_concurrency: 2,
                ..HistoricalRuntimeConfig::default()
            },
        )
        .expect("runtime");
        let error = runtime
            .run(
                BackfillJob {
                    id: "incomplete".to_owned(),
                    owner: HistoricalJobOwner::Materialization,
                    processor_instance: processor.descriptor().instance.to_string(),
                    mode: BackfillMode::FillMissing,
                    delivery_stream_id: None,
                    ranges: vec![range],
                    request: request(range),
                    sink_ids: Vec::new(),
                },
                SourceBudget {
                    max_buffered_frames: 4,
                    ..default_source_budget()
                },
                CancellationToken::new(),
            )
            .await
            .expect_err("incomplete");
        assert!(matches!(error, RuntimeError::IncompleteChunk { .. }));
        assert_eq!(
            store
                .coverage(processor.descriptor(), range)
                .await
                .expect("coverage"),
            vec![BlockRange::new(BlockNumber(1), BlockNumber(2)).expect("partial")]
        );
    }

    #[tokio::test]
    async fn historical_run_rejects_parent_break_across_source_chunks() {
        let range = BlockRange::new(BlockNumber(1), BlockNumber(2)).expect("range");
        let descriptor = fixture_source_descriptor("history", range);
        let first = fixture_frame(1, BlockHash::ZERO);
        let second = fixture_frame(2, BlockHash::new([0x55; 32]));
        let source = Arc::new(ScriptedHistorySource::new(
            descriptor.clone(),
            vec![
                ScriptedChunk {
                    range: BlockRange::single(BlockNumber(1)),
                    schema_version: descriptor.schema_version.clone(),
                    estimated_bytes: None,
                    steps: vec![HistoryStep::Frame(Box::new(first))],
                },
                ScriptedChunk {
                    range: BlockRange::single(BlockNumber(2)),
                    schema_version: descriptor.schema_version,
                    estimated_bytes: None,
                    steps: vec![HistoryStep::Frame(Box::new(second))],
                },
            ],
        ));
        let processor = Arc::new(BlockLocalCounter::default());
        let (_directory, store) = store().await;
        let runtime = HistoricalRuntime::new(
            store.clone(),
            source,
            processor.clone(),
            HistoricalRuntimeConfig {
                mapper_concurrency: 2,
                ..HistoricalRuntimeConfig::default()
            },
        )
        .expect("runtime");
        let error = runtime
            .run(
                BackfillJob {
                    id: "cross-chunk-parent-break".to_owned(),
                    owner: HistoricalJobOwner::Materialization,
                    processor_instance: processor.descriptor().instance.to_string(),
                    mode: BackfillMode::FillMissing,
                    delivery_stream_id: None,
                    ranges: vec![range],
                    request: request(range),
                    sink_ids: Vec::new(),
                },
                default_source_budget(),
                CancellationToken::new(),
            )
            .await
            .expect_err("parent break");
        assert!(
            matches!(
                &error,
                RuntimeError::Source(SourceError::CorruptFrame(message))
                    if message.contains("parent mismatch at block 2")
            ),
            "unexpected error: {error:?}"
        );
        assert_eq!(
            store
                .coverage(processor.descriptor(), range)
                .await
                .expect("coverage"),
            vec![BlockRange::single(BlockNumber(1))]
        );
    }

    #[tokio::test]
    async fn live_runtime_applies_and_reverses_a_shallow_fork() {
        let range = BlockRange::new(BlockNumber(1), BlockNumber(3)).expect("range");
        let first = included_frame(1, BlockHash::ZERO);
        let second = included_frame(2, first.block.hash);
        let third = included_frame(3, second.block.hash);
        let mut replacement = included_frame(3, second.block.hash);
        replacement.block.hash = BlockHash::new([0x33; 32]);
        let source = Arc::new(ScriptedLiveSource::new(
            fixture_source_descriptor("live", range),
            vec![
                LiveStep::Event(ChainEvent::Block(Box::new(first))),
                LiveStep::Event(ChainEvent::Block(Box::new(second))),
                LiveStep::Event(ChainEvent::Block(Box::new(third.clone()))),
                LiveStep::Event(ChainEvent::Reorg {
                    reverted: vec![third.block],
                    applied: vec![replacement.clone()],
                }),
            ],
        ));
        let processor = Arc::new(BlockLocalCounter::default());
        let (_directory, store) = store().await;
        let runtime = LiveRuntime::new(
            store.clone(),
            source,
            processor.clone(),
            LiveRuntimeConfig::default(),
        )
        .expect("runtime");
        let report = runtime
            .run(
                LiveStart::Head,
                default_source_budget(),
                CancellationToken::new(),
            )
            .await
            .expect("run");
        assert_eq!(report.blocks_applied, 4);
        assert_eq!(report.blocks_reverted, 1);
        assert_eq!(report.last_hash, Some(replacement.block.hash));
        assert_eq!(
            store
                .processor_cursor(processor.descriptor())
                .await
                .expect("cursor")
                .expect("present")
                .block_hash,
            replacement.block.hash
        );
        assert_eq!(
            store
                .changes(processor.descriptor(), ChainId(1), 0, 10)
                .await
                .expect("changes")
                .len(),
            5
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn shared_live_runtime_fans_out_and_reorgs_one_source_subscription() {
        let range = BlockRange::new(BlockNumber(0), BlockNumber(2)).expect("range");
        let first = live_fixture(0, BlockHash::ZERO);
        let second = live_fixture(1, first.block.hash);
        let third = live_fixture(2, second.block.hash);
        let mut replacement = live_fixture(2, second.block.hash);
        replacement.block.hash = BlockHash::new([0x42; 32]);
        let source = Arc::new(ScriptedLiveSource::new(
            fixture_source_descriptor("shared-live", range),
            vec![
                LiveStep::Event(ChainEvent::Block(Box::new(first))),
                LiveStep::Event(ChainEvent::Block(Box::new(second))),
                LiveStep::Event(ChainEvent::Block(Box::new(third.clone()))),
                LiveStep::Event(ChainEvent::Reorg {
                    reverted: vec![third.block],
                    applied: vec![replacement.clone()],
                }),
            ],
        ));
        let block_local = Arc::new(BlockLocalCounter::default());
        let ordered = Arc::new(OrderedLedgerProcessor::default());
        let processors: Vec<Arc<dyn Processor>> = vec![block_local.clone(), ordered.clone()];
        let (_directory, store) = store().await;
        let (committed_events, mut committed_event_receiver) = tokio::sync::broadcast::channel(8);
        let runtime = SharedLiveRuntime::new(
            store.clone(),
            source,
            processors.clone(),
            SharedLiveRuntimeConfig {
                pending_delta_bytes: 1_000_000,
                committed_events: Some(committed_events),
                ..SharedLiveRuntimeConfig::default()
            },
        )
        .expect("shared runtime");
        let report = runtime
            .run(
                LiveStart::Head,
                default_source_budget(),
                CancellationToken::new(),
            )
            .await
            .expect("run");
        assert_eq!(report.chain_blocks, 4);
        assert_eq!(report.reorgs, 1);
        assert!(matches!(
            committed_event_receiver.recv().await.expect("first event"),
            ChainEvent::Block(_)
        ));
        assert!(matches!(
            committed_event_receiver.recv().await.expect("second event"),
            ChainEvent::Block(_)
        ));
        assert!(matches!(
            committed_event_receiver.recv().await.expect("third event"),
            ChainEvent::Block(_)
        ));
        assert!(matches!(
            committed_event_receiver.recv().await.expect("reorg event"),
            ChainEvent::Reorg { .. }
        ));
        assert!(matches!(
            committed_event_receiver.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));
        for processor in &processors {
            let processor_report = report
                .processors
                .get(processor.descriptor().id.as_str())
                .expect("processor report");
            assert_eq!(processor_report.applied, 4);
            assert_eq!(processor_report.reverted, 1);
            assert_eq!(processor_report.pending, 0);
            assert_eq!(
                store
                    .processor_cursor(processor.descriptor())
                    .await
                    .expect("cursor")
                    .expect("present")
                    .block_hash,
                replacement.block.hash
            );
        }
        assert_eq!(
            store
                .recent_frame(ChainId(1), BlockNumber(2))
                .await
                .expect("recent")
                .expect("replacement")
                .block
                .hash,
            replacement.block.hash
        );
        let checkpoint = ConsensusCheckpoint {
            beacon_slot: 1,
            beacon_block_root: [1; 32],
            execution_block_hash: BlockHash::ZERO,
            obtained_at_unix_seconds: 1,
            source: "fixture".to_owned(),
        };
        let finality_source = Arc::new(ScriptedFinalitySource::new(
            fixture_source_descriptor("shared-finality", range),
            checkpoint.clone(),
            vec![FinalityStep::Event(FinalityEvent::Finalized {
                block_hash: replacement.block.hash,
                beacon_slot: 2,
                beacon_block_root: [2; 32],
            })],
        ));
        let finality_report = SharedFinalityRuntime::new(
            store.clone(),
            finality_source,
            processors.clone(),
            SharedFinalityRuntimeConfig {
                minimum_recent_blocks: 1,
                recent_soft_bytes: 1,
                recent_hard_bytes: 1_000_000,
            },
        )
        .expect("shared finality runtime")
        .run(checkpoint, CancellationToken::new())
        .await
        .expect("finality run");
        assert_eq!(finality_report.finalized_through, Some(BlockNumber(2)));
        assert!(finality_report.deferred_processors.is_empty());
        assert_eq!(finality_report.processor_finalized_through.len(), 2);
        assert_eq!(finality_report.pruned_recent_frames, 2);
        for processor in &processors {
            assert_eq!(
                store
                    .finalized_through(processor.descriptor())
                    .await
                    .expect("finalized"),
                Some(BlockNumber(2))
            );
        }
    }

    #[tokio::test]
    async fn blocked_block_versioned_live_lane_does_not_stop_other_processors() {
        let range = BlockRange::new(BlockNumber(0), BlockNumber(2)).expect("range");
        let first = live_fixture(0, BlockHash::ZERO);
        let second = live_fixture(1, first.block.hash);
        let third = live_fixture(2, second.block.hash);
        let source = Arc::new(ScriptedLiveSource::new(
            fixture_source_descriptor("isolated-live-lanes", range),
            vec![first.clone(), second, third.clone()]
                .into_iter()
                .map(|frame| LiveStep::Event(ChainEvent::Block(Box::new(frame))))
                .collect(),
        ));
        let blocked = Arc::new(
            BlockLocalCounter::named("blocked-counter")
                .with_split_delivery()
                .with_delivery_max_bytes(1),
        );
        let healthy = Arc::new(
            BlockLocalCounter::named("healthy-counter")
                .with_split_delivery()
                .with_output_none(),
        );
        let processors: Vec<Arc<dyn Processor>> = vec![blocked.clone(), healthy.clone()];
        let (_directory, store) = store().await;
        let report = SharedLiveRuntime::new(
            store.clone(),
            source,
            processors,
            SharedLiveRuntimeConfig::default(),
        )
        .expect("runtime")
        .run(
            LiveStart::Head,
            default_source_budget(),
            CancellationToken::new(),
        )
        .await
        .expect("one blocked lane must not stop shared ingestion");

        assert_eq!(report.chain_blocks, 3);
        assert_eq!(report.processors["blocked-counter"].applied, 0);
        assert_eq!(report.processors["blocked-counter"].pending, 1);
        assert_eq!(report.processors["healthy-counter"].applied, 3);
        assert_eq!(
            store
                .processor_cursor(healthy.descriptor())
                .await
                .expect("healthy cursor")
                .expect("healthy lane advanced")
                .block_number,
            third.block.number
        );
        assert!(
            store
                .processor_cursor(blocked.descriptor())
                .await
                .expect("blocked cursor")
                .is_none()
        );
        let blocked_state = store
            .processor_runtime_state(blocked.descriptor())
            .await
            .expect("blocked state");
        assert_eq!(blocked_state.state, ProcessorRunState::Failed);
        assert_eq!(
            blocked_state.reason.as_deref(),
            Some("single_block_exceeds_delivery_limit")
        );
        assert_eq!(
            store
                .recent_frame(ChainId(1), third.block.number)
                .await
                .expect("recent frame")
                .expect("shared source retained latest frame")
                .block
                .hash,
            third.block.hash
        );
    }

    #[tokio::test]
    async fn processor_mapping_failure_isolated_and_replays_after_explicit_reset() {
        let range = BlockRange::new(BlockNumber(0), BlockNumber(1)).expect("range");
        let first = live_fixture(0, BlockHash::ZERO);
        let second = live_fixture(1, first.block.hash);
        let source = Arc::new(ScriptedLiveSource::new(
            fixture_source_descriptor("mapping-failure-live", range),
            vec![first, second.clone()]
                .into_iter()
                .map(|frame| LiveStep::Event(ChainEvent::Block(Box::new(frame))))
                .collect(),
        ));
        let failing = Arc::new(FailOnceCounter::named("fail-once-counter"));
        let healthy = Arc::new(
            BlockLocalCounter::named("mapping-failure-healthy")
                .with_split_delivery()
                .with_output_none(),
        );
        let (_directory, store) = store().await;
        let runtime = SharedLiveRuntime::new(
            store.clone(),
            source,
            vec![failing.clone(), healthy.clone()],
            SharedLiveRuntimeConfig::default(),
        )
        .expect("runtime");
        let report = runtime
            .run(
                LiveStart::Head,
                default_source_budget(),
                CancellationToken::new(),
            )
            .await
            .expect("one processor mapping failure must not stop shared ingestion");

        assert_eq!(report.chain_blocks, 2);
        assert_eq!(report.processors["mapping-failure-healthy"].applied, 2);
        assert_eq!(report.processors["fail-once-counter"].applied, 0);
        assert_eq!(
            store
                .processor_runtime_state(failing.descriptor())
                .await
                .expect("failed state")
                .state,
            ProcessorRunState::Failed
        );
        assert_eq!(
            store
                .live_lane_gap(failing.descriptor())
                .await
                .expect("gap")
                .expect("durable mapping gap")
                .first_unapplied
                .number,
            BlockNumber(0)
        );
        assert_eq!(
            store
                .processor_cursor(healthy.descriptor())
                .await
                .expect("healthy cursor")
                .expect("healthy processor advanced")
                .block_number,
            second.block.number
        );

        store
            .reset_failed_live_lane(failing.descriptor())
            .await
            .expect("reset corrected processor lane");
        let recovered = runtime
            .reconcile_pending()
            .await
            .expect("replay retained canonical frames");
        assert_eq!(recovered.processors["fail-once-counter"].applied, 2);
        assert!(
            store
                .live_lane_gap(failing.descriptor())
                .await
                .expect("gap")
                .is_none()
        );
        assert_eq!(
            store
                .processor_cursor(failing.descriptor())
                .await
                .expect("recovered cursor")
                .expect("recovered processor advanced")
                .block_number,
            second.block.number
        );
    }

    #[tokio::test]
    async fn finalized_live_gap_reacquires_pruned_frames_and_marks_recovery_origin() {
        let range = BlockRange::new(BlockNumber(0), BlockNumber(2)).expect("range");
        let mut first = live_fixture(0, BlockHash::ZERO);
        first.finality = Finality::Finalized;
        let mut second = live_fixture(1, first.block.hash);
        second.finality = Finality::Finalized;
        let mut third = live_fixture(2, second.block.hash);
        third.finality = Finality::Finalized;
        let chain = vec![first.clone(), second, third.clone()];
        let processor = Arc::new(
            BlockLocalCounter::named("recovering-counter")
                .with_split_delivery()
                .with_output_none(),
        );
        let (_directory, store) = store().await;
        store
            .register_processor(processor.descriptor())
            .await
            .expect("register processor");
        for frame in &chain {
            store
                .store_recent_frame(frame)
                .await
                .expect("store canonical frame");
        }
        let first_delta = processor.map(&first).await.expect("map first");
        store
            .park_processor_live_lane(
                processor.descriptor(),
                &first_delta,
                "delivery_spool_hard_limit",
                0,
                false,
                true,
            )
            .await
            .expect("park lane");
        let prune = store
            .prune_recent_frames(ChainId(1), third.block.number, 1, 1, 1_000_000)
            .await
            .expect("prune old recent frames");
        assert_eq!(prune.deleted_frames, 2);
        assert!(
            store
                .recent_frame(ChainId(1), first.block.number)
                .await
                .expect("recent lookup")
                .is_none()
        );

        let source = Arc::new(ScriptedLiveSource::new(
            fixture_source_descriptor("recovery-live", range),
            Vec::new(),
        ));
        let runtime = SharedLiveRuntime::new(
            store.clone(),
            source,
            vec![processor.clone()],
            SharedLiveRuntimeConfig::default(),
        )
        .expect("runtime")
        .with_finalized_gap_recovery(Arc::new(StaticLiveGapRecovery { frames: chain }));
        let report = runtime
            .reconcile_pending()
            .await
            .expect("recover finalized gap");

        assert_eq!(report.processors["recovering-counter"].applied, 3);
        assert!(
            store
                .live_lane_gap(processor.descriptor())
                .await
                .expect("gap")
                .is_none()
        );
        assert_eq!(
            store
                .processor_runtime_state(processor.descriptor())
                .await
                .expect("state")
                .state,
            ProcessorRunState::Running
        );
        let changes = store
            .changes(processor.descriptor(), ChainId(1), 0, 10)
            .await
            .expect("changes");
        assert_eq!(changes.len(), 3);
        assert!(changes.iter().all(|record| {
            record.origin.kind == leani_store_sqlite::DeliveryOriginKind::LiveRecovery
                && record.finality == Finality::Finalized
        }));
    }

    #[tokio::test]
    async fn temporary_live_gap_recovery_failure_does_not_stop_shared_live_ingestion() {
        let range = BlockRange::new(BlockNumber(0), BlockNumber(4)).expect("range");
        let mut first = live_fixture(0, BlockHash::ZERO);
        first.finality = Finality::Finalized;
        let mut second = live_fixture(1, first.block.hash);
        second.finality = Finality::Finalized;
        let mut third = live_fixture(2, second.block.hash);
        third.finality = Finality::Finalized;
        let fourth = live_fixture(3, third.block.hash);
        let fifth = live_fixture(4, fourth.block.hash);
        let finalized = vec![first.clone(), second, third];
        let recovering = Arc::new(
            BlockLocalCounter::named("flaky-recovery-counter")
                .with_split_delivery()
                .with_output_none(),
        );
        let healthy = Arc::new(
            BlockLocalCounter::named("flaky-recovery-healthy")
                .with_split_delivery()
                .with_output_none(),
        );
        let (_directory, store) = store().await;
        store
            .register_processor(recovering.descriptor())
            .await
            .expect("register recovering processor");
        for frame in &finalized {
            store
                .store_recent_frame(frame)
                .await
                .expect("store finalized canonical frame");
        }
        let first_delta = recovering.map(&first).await.expect("map first");
        store
            .park_processor_live_lane(
                recovering.descriptor(),
                &first_delta,
                "delivery_spool_hard_limit",
                0,
                false,
                true,
            )
            .await
            .expect("park recovering lane");
        store
            .prune_recent_frames(ChainId(1), BlockNumber(2), 1, 1, 1_000_000)
            .await
            .expect("prune old frames");

        let source = Arc::new(ScriptedLiveSource::new(
            fixture_source_descriptor("flaky-recovery-live", range),
            vec![fourth, fifth]
                .into_iter()
                .map(|frame| LiveStep::Event(ChainEvent::Block(Box::new(frame))))
                .collect(),
        ));
        let recovery = Arc::new(FlakyLiveGapRecovery {
            frames: finalized,
            attempts: AtomicUsize::new(0),
        });
        let report = SharedLiveRuntime::new(
            store.clone(),
            source,
            vec![recovering.clone(), healthy.clone()],
            SharedLiveRuntimeConfig::default(),
        )
        .expect("runtime")
        .with_finalized_gap_recovery(recovery.clone())
        .run(
            LiveStart::Head,
            default_source_budget(),
            CancellationToken::new(),
        )
        .await
        .expect("temporary archive outage must not stop shared live ingestion");

        assert_eq!(recovery.attempts.load(Ordering::SeqCst), 2);
        assert_eq!(report.chain_blocks, 2);
        assert_eq!(report.processors["flaky-recovery-healthy"].applied, 2);
        assert_eq!(report.processors["flaky-recovery-counter"].applied, 5);
        assert!(
            store
                .live_lane_gap(recovering.descriptor())
                .await
                .expect("gap")
                .is_none()
        );
        assert_eq!(
            store
                .processor_runtime_state(recovering.descriptor())
                .await
                .expect("recovering state")
                .state,
            ProcessorRunState::Running
        );
    }

    #[tokio::test]
    async fn oversized_live_lane_requires_a_larger_limit_and_explicit_reset() {
        let range = BlockRange::single(BlockNumber(0));
        let frame = live_fixture(0, BlockHash::ZERO);
        let blocked = BlockLocalCounter::named("reset-counter")
            .with_split_delivery()
            .with_output_none()
            .with_delivery_max_bytes(1);
        let (_directory, store) = store().await;
        store
            .register_processor(blocked.descriptor())
            .await
            .expect("register blocked policy");
        store
            .store_recent_frame(&frame)
            .await
            .expect("store recent frame");
        let delta = blocked.map(&frame).await.expect("map");
        store
            .park_processor_live_lane(
                blocked.descriptor(),
                &delta,
                "single_block_exceeds_delivery_limit",
                8,
                true,
                true,
            )
            .await
            .expect("park failed lane");
        assert!(matches!(
            store.reset_failed_live_lane(blocked.descriptor()).await,
            Err(StoreError::DeliveryItemTooLarge {
                observed_bytes: 8,
                maximum_bytes: 1,
                ..
            })
        ));

        let raised = Arc::new(
            BlockLocalCounter::named("reset-counter")
                .with_split_delivery()
                .with_output_none()
                .with_delivery_max_bytes(1_024),
        );
        store
            .register_processor(raised.descriptor())
            .await
            .expect("operational lifecycle limits are mutable");
        store
            .reset_failed_live_lane(raised.descriptor())
            .await
            .expect("explicit reset after raising limit");
        let source = Arc::new(ScriptedLiveSource::new(
            fixture_source_descriptor("reset-live", range),
            Vec::new(),
        ));
        SharedLiveRuntime::new(
            store.clone(),
            source,
            vec![raised.clone()],
            SharedLiveRuntimeConfig::default(),
        )
        .expect("runtime")
        .reconcile_pending()
        .await
        .expect("replay after reset");
        assert!(
            store
                .live_lane_gap(raised.descriptor())
                .await
                .expect("gap")
                .is_none()
        );
        assert_eq!(
            store
                .processor_runtime_state(raised.descriptor())
                .await
                .expect("state")
                .state,
            ProcessorRunState::Running
        );
    }

    #[tokio::test]
    async fn parked_live_gap_rebases_to_a_shallow_reorg_replacement() {
        let range = BlockRange::new(BlockNumber(0), BlockNumber(1)).expect("range");
        let first = live_fixture(0, BlockHash::ZERO);
        let original = live_fixture(1, first.block.hash);
        let mut replacement = live_fixture(1, first.block.hash);
        replacement.block.hash = BlockHash::new([0x71; 32]);
        let processor = Arc::new(
            BlockLocalCounter::named("reorg-gap-counter")
                .with_split_delivery()
                .with_output_none(),
        );
        let (_directory, store) = store().await;
        store
            .store_recent_frame(&first)
            .await
            .expect("store ancestor");
        store
            .store_recent_frame(&original)
            .await
            .expect("store original");
        let first_delta = processor.map(&first).await.expect("map ancestor");
        store
            .apply(
                processor.as_ref(),
                ProcessorCursor {
                    processor_id: processor.descriptor().id.to_string(),
                    processor_version: processor.descriptor().version.to_string(),
                    chain_id: first.chain_id,
                    block_number: first.block.number,
                    block_hash: first.block.hash,
                    finality: first.finality,
                    sequence: 1,
                },
                &first_delta,
                &[],
            )
            .await
            .expect("commit ancestor");
        let original_delta = processor.map(&original).await.expect("map original");
        store
            .park_processor_live_lane(
                processor.descriptor(),
                &original_delta,
                "delivery_spool_hard_limit",
                0,
                false,
                true,
            )
            .await
            .expect("park original");

        SharedLiveRuntime::new(
            store.clone(),
            Arc::new(ScriptedLiveSource::new(
                fixture_source_descriptor("reorg-gap-live", range),
                vec![LiveStep::Event(ChainEvent::Reorg {
                    reverted: vec![original.block],
                    applied: vec![replacement.clone()],
                })],
            )),
            vec![processor.clone()],
            SharedLiveRuntimeConfig::default(),
        )
        .expect("runtime")
        .run(
            LiveStart::Head,
            default_source_budget(),
            CancellationToken::new(),
        )
        .await
        .expect("reorg does not strand parked lane");

        assert!(
            store
                .live_lane_gap(processor.descriptor())
                .await
                .expect("gap")
                .is_none()
        );
        assert_eq!(
            store
                .processor_cursor(processor.descriptor())
                .await
                .expect("cursor")
                .expect("cursor exists")
                .block_hash,
            replacement.block.hash
        );
        assert!(
            store
                .pending_deltas(processor.descriptor(), BlockNumber(0), 10)
                .await
                .expect("pending")
                .is_empty()
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn ordered_pending_limit_parks_one_lane_and_replays_from_recent_frames() {
        let range = BlockRange::new(BlockNumber(1), BlockNumber(2)).expect("range");
        let zero = live_fixture(0, BlockHash::ZERO);
        let first = live_fixture(1, zero.block.hash);
        let second = live_fixture(2, first.block.hash);
        let ordered = Arc::new(OrderedLedgerProcessor::default());
        let first_delta = ordered.map(&first).await.expect("map first pending delta");
        let pending_limit = u64::try_from(
            first_delta
                .encode_durable()
                .expect("encode first pending delta")
                .len(),
        )
        .expect("pending delta size");
        let healthy = Arc::new(
            BlockLocalCounter::named("ordered-limit-healthy")
                .with_split_delivery()
                .with_output_none(),
        );
        let (_directory, store) = store().await;
        store
            .store_recent_frame(&zero)
            .await
            .expect("retain ordered start frame");
        let runtime = SharedLiveRuntime::new(
            store.clone(),
            Arc::new(ScriptedLiveSource::new(
                fixture_source_descriptor("ordered-limit-live", range),
                vec![first, second.clone()]
                    .into_iter()
                    .map(|frame| LiveStep::Event(ChainEvent::Block(Box::new(frame))))
                    .collect(),
            )),
            vec![ordered.clone(), healthy.clone()],
            SharedLiveRuntimeConfig {
                pending_delta_bytes: pending_limit,
                ..SharedLiveRuntimeConfig::default()
            },
        )
        .expect("runtime");
        let report = runtime
            .run(
                LiveStart::Head,
                default_source_budget(),
                CancellationToken::new(),
            )
            .await
            .expect("ordered pending pressure must not stop shared ingestion");

        assert_eq!(report.chain_blocks, 2);
        assert_eq!(report.processors["ordered-limit-healthy"].applied, 2);
        assert_eq!(report.processors["synthetic-ledger"].applied, 0);
        assert_eq!(
            store
                .processor_runtime_state(ordered.descriptor())
                .await
                .expect("ordered state")
                .state,
            ProcessorRunState::Paused
        );
        assert_eq!(
            store
                .live_lane_gap(ordered.descriptor())
                .await
                .expect("ordered gap")
                .expect("ordered lane parked")
                .first_unapplied
                .number,
            BlockNumber(2)
        );
        assert_eq!(
            store
                .processor_stats(ordered.descriptor())
                .await
                .expect("ordered stats")
                .pending_deltas,
            1
        );

        let zero_delta = ordered.map(&zero).await.expect("map ordered start");
        store
            .apply(
                ordered.as_ref(),
                ProcessorCursor {
                    processor_id: ordered.descriptor().id.to_string(),
                    processor_version: ordered.descriptor().version.to_string(),
                    chain_id: zero.chain_id,
                    block_number: zero.block.number,
                    block_hash: zero.block.hash,
                    finality: zero.finality,
                    sequence: 1,
                },
                &zero_delta,
                &[],
            )
            .await
            .expect("historical catch-up reaches ordered start");
        let recovered = runtime
            .reconcile_pending()
            .await
            .expect("ordered lane drains pending and retained frames");
        assert_eq!(recovered.processors["synthetic-ledger"].applied, 2);
        assert!(
            store
                .live_lane_gap(ordered.descriptor())
                .await
                .expect("ordered gap")
                .is_none()
        );
        assert_eq!(
            store
                .processor_cursor(ordered.descriptor())
                .await
                .expect("ordered cursor")
                .expect("ordered lane caught up")
                .block_number,
            second.block.number
        );
    }

    #[tokio::test]
    async fn ordered_live_restart_removes_already_applied_overlap_deltas() {
        let range = BlockRange::new(BlockNumber(0), BlockNumber(2)).expect("range");
        let first = live_fixture(0, BlockHash::ZERO);
        let second = live_fixture(1, first.block.hash);
        let third = live_fixture(2, second.block.hash);
        let chain = vec![first, second, third];
        let processor = Arc::new(OrderedLedgerProcessor::default());
        let processors: Vec<Arc<dyn Processor>> = vec![processor.clone()];
        let (_directory, store) = store().await;
        let first_source = Arc::new(ScriptedLiveSource::new(
            fixture_source_descriptor("first-live", range),
            chain
                .iter()
                .cloned()
                .map(|frame| LiveStep::Event(ChainEvent::Block(Box::new(frame))))
                .collect(),
        ));
        let first_runtime = SharedLiveRuntime::new(
            store.clone(),
            first_source,
            processors.clone(),
            SharedLiveRuntimeConfig::default(),
        )
        .expect("first runtime");
        first_runtime
            .run(
                LiveStart::Head,
                default_source_budget(),
                CancellationToken::new(),
            )
            .await
            .expect("first run");

        let stale = processor.map(&chain[0]).await.expect("map stale overlap");
        store
            .persist_delta(processor.descriptor(), &stale)
            .await
            .expect("persist stale overlap");
        assert_eq!(
            store
                .processor_stats(processor.descriptor())
                .await
                .expect("stale statistics")
                .pending_deltas,
            1
        );
        let reconciliation = first_runtime
            .reconcile_pending()
            .await
            .expect("clean stale overlap");
        assert_eq!(reconciliation.processors["synthetic-ledger"].pending, 0);

        let replay_source = Arc::new(ScriptedLiveSource::new(
            fixture_source_descriptor("replayed-live", range),
            chain
                .into_iter()
                .map(|frame| LiveStep::Event(ChainEvent::Block(Box::new(frame))))
                .collect(),
        ));
        let replay = SharedLiveRuntime::new(
            store.clone(),
            replay_source,
            processors,
            SharedLiveRuntimeConfig::default(),
        )
        .expect("replay runtime")
        .run(
            LiveStart::Head,
            default_source_budget(),
            CancellationToken::new(),
        )
        .await
        .expect("replay");
        assert_eq!(replay.processors["synthetic-ledger"].duplicates, 3);
        assert_eq!(
            store
                .processor_stats(processor.descriptor())
                .await
                .expect("replay statistics")
                .pending_deltas,
            0
        );
    }

    #[tokio::test]
    async fn historical_finality_promotion_accepts_the_same_mapped_block() {
        let range = BlockRange::single(BlockNumber(0));
        let processor = Arc::new(FinalitySensitiveCounter::default());
        let source = Arc::new(ScriptedHistorySource::from_frames(
            fixture_source_descriptor("finality-promotion", range),
            Vec::new(),
        ));
        let (_directory, store) = store().await;
        let runtime = HistoricalRuntime::new(
            store.clone(),
            source,
            processor.clone(),
            HistoricalRuntimeConfig::default(),
        )
        .expect("historical runtime");
        let optimistic = live_fixture(0, BlockHash::ZERO);
        let (delta, equivalent_checksums) =
            map_with_finality_variants(processor.as_ref(), &optimistic)
                .await
                .expect("map optimistic");
        let optimistic_checksum = delta.checksum;
        assert!(matches!(
            runtime
                .commit_mapped_frame(
                    &MappedFrame {
                        delta,
                        equivalent_checksums,
                        finality: Finality::Included,
                        estimated_bytes: 0,
                        material: None,
                        _mapped_byte_permit: None,
                    },
                    &[],
                    true,
                    BackfillMode::FillMissing,
                    None,
                )
                .await
                .expect("commit optimistic"),
            ApplyOutcome::Applied { .. }
        ));

        let mut finalized = optimistic;
        finalized.finality = Finality::Finalized;
        let (delta, equivalent_checksums) =
            map_with_finality_variants(processor.as_ref(), &finalized)
                .await
                .expect("map finalized");
        assert_ne!(delta.checksum, optimistic_checksum);
        assert!(matches!(
            runtime
                .commit_mapped_frame(
                    &MappedFrame {
                        delta,
                        equivalent_checksums,
                        finality: Finality::Finalized,
                        estimated_bytes: 0,
                        material: None,
                        _mapped_byte_permit: None,
                    },
                    &[],
                    true,
                    BackfillMode::FillMissing,
                    None,
                )
                .await
                .expect("promote finalized"),
            ApplyOutcome::AlreadyApplied
        ));
        assert_eq!(
            store
                .finalized_through(processor.descriptor())
                .await
                .expect("finalized coverage"),
            Some(BlockNumber(0))
        );
        assert_eq!(
            store
                .processor_stats(processor.descriptor())
                .await
                .expect("statistics")
                .applied_blocks,
            1
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn shared_live_accepts_archive_first_finality_and_cleans_stale_block_local_deltas() {
        let range = BlockRange::single(BlockNumber(0));
        let processor = Arc::new(FinalitySensitiveCounter::default());
        let processors: Vec<Arc<dyn Processor>> = vec![processor.clone()];
        let (_directory, store) = store().await;
        let archive_runtime = HistoricalRuntime::new(
            store.clone(),
            Arc::new(ScriptedHistorySource::from_frames(
                fixture_source_descriptor("archive-first", range),
                Vec::new(),
            )),
            processor.clone(),
            HistoricalRuntimeConfig::default(),
        )
        .expect("archive runtime");
        let optimistic = live_fixture(0, BlockHash::ZERO);
        let mut finalized = optimistic.clone();
        finalized.finality = Finality::Finalized;
        let (delta, equivalent_checksums) =
            map_with_finality_variants(processor.as_ref(), &finalized)
                .await
                .expect("map finalized archive");
        archive_runtime
            .commit_mapped_frame(
                &MappedFrame {
                    delta,
                    equivalent_checksums,
                    finality: Finality::Finalized,
                    estimated_bytes: 0,
                    material: None,
                    _mapped_byte_permit: None,
                },
                &[],
                true,
                BackfillMode::FillMissing,
                None,
            )
            .await
            .expect("commit finalized archive");

        let live_runtime = SharedLiveRuntime::new(
            store.clone(),
            Arc::new(ScriptedLiveSource::new(
                fixture_source_descriptor("optimistic-live", range),
                vec![LiveStep::Event(ChainEvent::Block(Box::new(
                    optimistic.clone(),
                )))],
            )),
            processors,
            SharedLiveRuntimeConfig::default(),
        )
        .expect("live runtime");
        let stale = processor
            .map(&optimistic)
            .await
            .expect("map stale optimistic delta");
        store
            .persist_delta(processor.descriptor(), &stale)
            .await
            .expect("persist stale optimistic delta");
        assert_eq!(
            store
                .processor_stats(processor.descriptor())
                .await
                .expect("stale statistics")
                .pending_deltas,
            1
        );
        let reconciliation = live_runtime
            .reconcile_pending()
            .await
            .expect("reconcile stale block-local variant");
        assert_eq!(reconciliation.processors["synthetic-counter"].duplicates, 1);
        assert_eq!(reconciliation.processors["synthetic-counter"].pending, 0);
        assert!(
            store
                .recent_frame(ChainId(1), BlockNumber(0))
                .await
                .expect("recent lookup")
                .is_none(),
            "stale recovery must not depend on retained raw material"
        );

        let report = live_runtime
            .run(
                LiveStart::Head,
                default_source_budget(),
                CancellationToken::new(),
            )
            .await
            .expect("archive-first live overlap");
        assert_eq!(report.processors["synthetic-counter"].duplicates, 1);
        assert_eq!(
            store
                .processor_stats(processor.descriptor())
                .await
                .expect("clean statistics")
                .applied_blocks,
            1
        );
        assert_eq!(
            store
                .finalized_through(processor.descriptor())
                .await
                .expect("finalized"),
            Some(BlockNumber(0))
        );
    }

    #[tokio::test]
    async fn shared_finality_defers_an_execution_ingestion_race() {
        let range = BlockRange::single(BlockNumber(7));
        let frame = live_fixture(7, BlockHash::new([0x06; 32]));
        let checkpoint = ConsensusCheckpoint {
            beacon_slot: 1,
            beacon_block_root: [1; 32],
            execution_block_hash: BlockHash::ZERO,
            obtained_at_unix_seconds: 1,
            source: "fixture".to_owned(),
        };
        let source = Arc::new(ScriptedFinalitySource::new(
            fixture_source_descriptor("racing-finality", range),
            checkpoint.clone(),
            vec![
                FinalityStep::Event(FinalityEvent::Finalized {
                    block_hash: frame.block.hash,
                    beacon_slot: 2,
                    beacon_block_root: [2; 32],
                }),
                FinalityStep::Delay(Duration::from_secs(1)),
            ],
        ));
        let processor = Arc::new(BlockLocalCounter::default());
        let (_directory, store) = store().await;
        let (applied_anchors, mut applied_anchor_updates) = tokio::sync::broadcast::channel(1);
        let runtime = SharedFinalityRuntime::new(
            store.clone(),
            source,
            vec![processor],
            SharedFinalityRuntimeConfig {
                minimum_recent_blocks: 1,
                recent_soft_bytes: 1_000_000,
                recent_hard_bytes: 2_000_000,
            },
        )
        .expect("shared finality runtime")
        .with_applied_anchors(applied_anchors);
        let task = tokio::spawn(async move {
            runtime
                .run(checkpoint, CancellationToken::new())
                .await
                .expect("finality run")
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        store
            .store_recent_frame(&frame)
            .await
            .expect("execution lane frame");

        let report = task.await.expect("finality task");
        assert_eq!(report.finalized_through, Some(BlockNumber(7)));
        assert_eq!(report.deferred_finalized_anchor, None);
        assert_eq!(report.finalized_events, 1);
        assert_eq!(
            applied_anchor_updates.recv().await.expect("applied anchor"),
            AppliedFinalityAnchor {
                block: frame.block,
                beacon_slot: 2,
                beacon_block_root: [2; 32],
            }
        );
        assert!(
            store
                .reorg_recent_frames(ChainId(1), &[frame.block], &[])
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn shared_finality_recovers_without_restarting_the_execution_lane() {
        let range = BlockRange::single(BlockNumber(7));
        let checkpoint = ConsensusCheckpoint {
            beacon_slot: 1,
            beacon_block_root: [1; 32],
            execution_block_hash: BlockHash::ZERO,
            obtained_at_unix_seconds: 1,
            source: "fixture".to_owned(),
        };
        let subscriptions = Arc::new(AtomicUsize::new(0));
        let source = Arc::new(RecoveringFinalitySource {
            descriptor: fixture_source_descriptor("recovering-finality", range),
            expected_checkpoint: checkpoint.clone(),
            subscriptions: subscriptions.clone(),
        });
        let processor = Arc::new(BlockLocalCounter::default());
        let (_directory, store) = store().await;
        let runtime = SharedFinalityRuntime::new(
            store,
            source,
            vec![processor],
            SharedFinalityRuntimeConfig {
                minimum_recent_blocks: 1,
                recent_soft_bytes: 1_000_000,
                recent_hard_bytes: 2_000_000,
            },
        )
        .expect("shared finality runtime");
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let (ready, mut ready_updates) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(async move {
            runtime
                .run_resilient_with_readiness(checkpoint, task_cancellation, ready)
                .await
        });

        tokio::time::timeout(Duration::from_secs(3), async {
            while subscriptions.load(Ordering::SeqCst) < 2 || !*ready_updates.borrow() {
                ready_updates.changed().await.expect("readiness sender");
            }
        })
        .await
        .expect("finality source should recover");
        assert_eq!(subscriptions.load(Ordering::SeqCst), 2);

        cancellation.cancel();
        let report = task
            .await
            .expect("finality task")
            .expect("resilient finality");
        assert_eq!(report, SharedFinalityReport::default());
        assert!(!*ready_updates.borrow());
    }

    fn live_fixture(number: u64, parent: BlockHash) -> leani_primitives::BlockFrame {
        let mut frame = included_frame(number, parent);
        frame.header = leani_primitives::Material::Complete(leani_primitives::HeaderEnvelope {
            rlp: None,
            transactions_root: None,
            receipts_root: None,
            withdrawals_root: None,
            gas_limit: None,
            gas_used: None,
            base_fee_per_gas: None,
            blob_gas_used: None,
            excess_blob_gas: None,
            size_bytes: None,
            consensus_size_bytes: None,
            transaction_count: None,
        });
        frame
    }

    #[tokio::test]
    async fn live_runtime_rejects_a_parent_gap_before_mapping() {
        let range = BlockRange::new(BlockNumber(1), BlockNumber(3)).expect("range");
        let first = included_frame(1, BlockHash::ZERO);
        let third = included_frame(3, first.block.hash);
        let source = Arc::new(ScriptedLiveSource::new(
            fixture_source_descriptor("live-gap", range),
            vec![
                LiveStep::Event(ChainEvent::Block(Box::new(first))),
                LiveStep::Event(ChainEvent::Block(Box::new(third))),
            ],
        ));
        let processor = Arc::new(BlockLocalCounter::default());
        let (_directory, store) = store().await;
        let runtime = LiveRuntime::new(store, source, processor, LiveRuntimeConfig::default())
            .expect("runtime");
        assert!(matches!(
            runtime
                .run(
                    LiveStart::Head,
                    default_source_budget(),
                    CancellationToken::new()
                )
                .await,
            Err(RuntimeError::LiveGap {
                expected: BlockNumber(2),
                ..
            })
        ));
    }

    #[tokio::test]
    async fn finalized_only_processors_publish_nothing_before_verified_finality() {
        let range = BlockRange::new(BlockNumber(1), BlockNumber(3)).expect("range");
        let first = included_frame(1, BlockHash::ZERO);
        let second = included_frame(2, first.block.hash);
        let third = included_frame(3, second.block.hash);
        let live_source = Arc::new(ScriptedLiveSource::new(
            fixture_source_descriptor("live-finalized-only", range),
            vec![
                LiveStep::Event(ChainEvent::Block(Box::new(first))),
                LiveStep::Event(ChainEvent::Block(Box::new(second.clone()))),
                LiveStep::Event(ChainEvent::Block(Box::new(third))),
            ],
        ));
        let processor = Arc::new(
            BlockLocalCounter::default().with_publication(PublicationPolicy::FinalizedOnly),
        );
        let (_directory, store) = store().await;
        LiveRuntime::new(
            store.clone(),
            live_source,
            processor.clone(),
            LiveRuntimeConfig::default(),
        )
        .expect("live runtime")
        .run(
            LiveStart::Head,
            default_source_budget(),
            CancellationToken::new(),
        )
        .await
        .expect("live run");
        assert!(
            store
                .changes(processor.descriptor(), ChainId(1), 0, 100)
                .await
                .expect("changes before finality")
                .is_empty(),
            "included blocks must not be published by a finalized_only processor"
        );

        let checkpoint = ConsensusCheckpoint {
            beacon_slot: 1,
            beacon_block_root: [1; 32],
            execution_block_hash: first_hash_for_checkpoint(),
            obtained_at_unix_seconds: 1,
            source: "fixture".to_owned(),
        };
        let finality_source = Arc::new(ScriptedFinalitySource::new(
            fixture_source_descriptor("finality", range),
            checkpoint.clone(),
            vec![FinalityStep::Event(FinalityEvent::Finalized {
                block_hash: second.block.hash,
                beacon_slot: 2,
                beacon_block_root: [2; 32],
            })],
        ));
        FinalityRuntime::new(store.clone(), finality_source, processor.clone())
            .run(checkpoint, CancellationToken::new())
            .await
            .expect("finality");

        let changes = store
            .changes(processor.descriptor(), ChainId(1), 0, 100)
            .await
            .expect("changes after finality");
        let summary: Vec<_> = changes
            .iter()
            .map(|change| (change.block.number.0, change.finality, change.direction))
            .collect();
        assert_eq!(
            summary,
            vec![
                (1, Finality::Finalized, ChangeDirection::Apply),
                (2, Finality::Finalized, ChangeDirection::Apply),
                (2, Finality::Finalized, ChangeDirection::Finalized),
            ]
        );
    }

    #[tokio::test]
    async fn verified_finality_makes_the_anchored_prefix_irreversible() {
        let range = BlockRange::new(BlockNumber(1), BlockNumber(3)).expect("range");
        let first = included_frame(1, BlockHash::ZERO);
        let second = included_frame(2, first.block.hash);
        let third = included_frame(3, second.block.hash);
        let live_source = Arc::new(ScriptedLiveSource::new(
            fixture_source_descriptor("live-finality", range),
            vec![
                LiveStep::Event(ChainEvent::Block(Box::new(first))),
                LiveStep::Event(ChainEvent::Block(Box::new(second))),
                LiveStep::Event(ChainEvent::Block(Box::new(third.clone()))),
            ],
        ));
        let processor = Arc::new(BlockLocalCounter::default());
        let (_directory, store) = store().await;
        LiveRuntime::new(
            store.clone(),
            live_source,
            processor.clone(),
            LiveRuntimeConfig::default(),
        )
        .expect("live runtime")
        .run(
            LiveStart::Head,
            default_source_budget(),
            CancellationToken::new(),
        )
        .await
        .expect("live run");
        let checkpoint = ConsensusCheckpoint {
            beacon_slot: 1,
            beacon_block_root: [1; 32],
            execution_block_hash: first_hash_for_checkpoint(),
            obtained_at_unix_seconds: 1,
            source: "fixture".to_owned(),
        };
        let finality_source = Arc::new(ScriptedFinalitySource::new(
            fixture_source_descriptor("finality", range),
            checkpoint.clone(),
            vec![FinalityStep::Event(FinalityEvent::Finalized {
                block_hash: third.block.hash,
                beacon_slot: 3,
                beacon_block_root: [3; 32],
            })],
        ));
        let report = FinalityRuntime::new(store.clone(), finality_source, processor.clone())
            .run(checkpoint, CancellationToken::new())
            .await
            .expect("finality");
        assert_eq!(report.finalized_through, Some(BlockNumber(3)));
        assert_eq!(
            store
                .finalized_through(processor.descriptor())
                .await
                .expect("finalized"),
            Some(BlockNumber(3))
        );
        assert!(matches!(
            store
                .undo(
                    processor.descriptor(),
                    ChainId(1),
                    third.block.number,
                    third.block.hash,
                    &[]
                )
                .await,
            Err(StoreError::FinalizedUndo(BlockNumber(3)))
        ));
    }

    const fn first_hash_for_checkpoint() -> BlockHash {
        BlockHash::new([1; 32])
    }

    #[test]
    fn retry_backoff_is_exponential_and_capped() {
        let base = Duration::from_millis(10);
        let max = Duration::from_millis(25);
        assert_eq!(retry_delay(base, max, 1), Duration::from_millis(10));
        assert_eq!(retry_delay(base, max, 2), Duration::from_millis(20));
        assert_eq!(retry_delay(base, max, 3), max);
        assert_eq!(retry_delay(base, max, 100), max);
    }
}
