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
    DeliveryLimitAction, DeliveryPolicyMode, EncodedDelta, OutputPolicyMode, Processor,
    ProcessorDescriptor, ProcessorError, PublicationPolicy, ReductionMode, StartPoint,
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
use tracing::{debug, info, warn};

const FINALITY_ANCHOR_RETRY_INTERVAL: Duration = Duration::from_millis(250);
const RECENT_STORAGE_RETRY_INTERVAL: Duration = Duration::from_millis(250);
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
    /// Consecutive failed source attempts allowed on one gap before the job
    /// fails. A gap that commits blocks before failing starts a new budget.
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
        // A `run()` future dropped after being selected, but before claiming
        // its turn, must not leave every other job waiting for it.
        if state.selected.as_deref() == Some(self.job_id.as_str()) {
            state.selected = None;
        }
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
            let scope =
                covering_filter_scope(requirements.iter().map(|requirement| &requirement.filter));
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

/// How far one historical chunk got.
#[derive(Clone, Copy, Debug)]
enum ChunkOutcome {
    /// Every frame committed; the last committed block hash.
    Committed(Option<BlockHash>),
    /// A backpressure pause released the job's streams and chunk slots, and
    /// the frame it held then committed. The job replans the rest of its gap
    /// from durable progress.
    Released,
}

/// Release what a job paused for backpressure holds beyond the one mapped
/// frame whose commit it retries: the streams and chunk slots `release`
/// drops, the rest of its split microbatch with those frames' mapped-byte
/// permits, and the source frame, with its material memory, behind the
/// frame it keeps. The job maps the rest again when it replans.
fn release_while_paused(
    release: &mut (dyn FnMut() + Send),
    refused: &mut [MappedFrame],
    pending: &mut VecDeque<(Vec<MappedFrame>, bool)>,
) {
    release();
    pending.clear();
    for mapped in refused {
        mapped.material = None;
    }
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
            .mark_finalized(
                processor.descriptor(),
                mapped.delta.block.number,
                mapped.delta.block.hash,
            )
            .await?;
    }
    Ok(true)
}

/// Test-only crash points between the separately committed steps of a live
/// commit or reorg. A test arms a point for one processor instance and block,
/// and the runtime then fails there once with an error no lane isolates, so
/// the run ends as if the process had died before the next durable step.
#[cfg(test)]
mod failpoints {
    use std::sync::{Mutex, PoisonError};

    use leani_primitives::BlockNumber;
    use leani_processor_api::ProcessorDescriptor;

    use crate::RuntimeError;

    /// After the canonical recent write, before one processor's apply.
    pub(crate) const BEFORE_LIVE_APPLY: &str = "before_live_apply";
    /// After a delta's `persist_delta`, before its apply.
    pub(crate) const AFTER_PERSIST_DELTA: &str = "after_persist_delta";
    /// After a reorg's canonical switch, before one processor's undo.
    pub(crate) const BEFORE_REORG_UNDO: &str = "before_reorg_undo";
    /// After the store paused or failed a lane at its delivery limit, before
    /// the runtime records the lane's gap marker.
    pub(crate) const BEFORE_LIVE_PARK: &str = "before_live_park";

    static ARMED: Mutex<Vec<(&str, String, BlockNumber)>> = Mutex::new(Vec::new());

    pub(crate) fn arm(point: &'static str, processor: &ProcessorDescriptor, block: BlockNumber) {
        ARMED.lock().unwrap_or_else(PoisonError::into_inner).push((
            point,
            processor.instance.to_string(),
            block,
        ));
    }

    pub(crate) fn hit(
        point: &'static str,
        processor: &ProcessorDescriptor,
        block: BlockNumber,
    ) -> Result<(), RuntimeError> {
        let instance = processor.instance.to_string();
        let mut armed = ARMED.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(index) = armed
            .iter()
            .position(|armed| armed.0 == point && armed.1 == instance && armed.2 == block)
        else {
            return Ok(());
        };
        armed.swap_remove(index);
        Err(RuntimeError::InvalidConfig(format!(
            "injected crash at {point} for {instance} at block {}",
            block.0
        )))
    }
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
    #[cfg(test)]
    failpoints::hit(
        failpoints::AFTER_PERSIST_DELTA,
        processor.descriptor(),
        mapped.delta.block.number,
    )?;
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
        // Retries are budgeted per gap: consecutive failed source attempts
        // since the current gap last committed a block.
        let mut gap_failures = 0_u32;
        // Sources are in priority order. A gap starts on the first and moves
        // to the next only after a source error.
        let mut source_index = 0_usize;
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
                gap_failures = 0;
                continue;
            }

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
                // A failure after committing blocks opens a fresh budget, so
                // scattered transient errors never exhaust a long job.
                let progressed = checkpoint.frames_committed > committed_before
                    || duplicate_frames > duplicates_before;
                gap_failures = if progressed {
                    1
                } else {
                    gap_failures.saturating_add(1)
                };
            }
            match attempt {
                Ok(()) => {
                    gap_failures = 0;
                    source_index = 0;
                }
                Err(RuntimeError::Source(error))
                    if failover_source_error(&error) && gap_failures < self.config.max_attempts =>
                {
                    source_index = source_index.saturating_add(1) % source_count;
                    let delay =
                        retry_delay(self.config.retry_base, self.config.retry_max, gap_failures);
                    warn!(
                        job_id = %job.id,
                        attempt = gap_failures,
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

    /// Commit a gap from retained recent frames, streamed in bounded batches.
    ///
    /// Each batch holds an active-chunk permit, as a source chunk does, and
    /// reads at most one commit's worth of blocks (`commit_maximum_blocks`)
    /// and at most the pipeline's mapped-byte budget of encoded frames (at
    /// least one frame). Reuse stops at the first frame the job cannot use:
    /// with nothing committed that is `false`, and the job reads the gap from
    /// its sources; after a committed prefix it is `true`, and the job reads
    /// the rest of the gap, recomputed from its progress, from its sources.
    /// A backpressure pause also ends reuse with `true`, once the frame it
    /// held commits, so the paused job holds no chunk slot.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
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
        let batch_blocks = u64::try_from(self.config.commit_maximum_blocks)
            .unwrap_or(u64::MAX)
            .max(1);
        let mut next = request.range.start();
        let mut parent = expected_parent;
        let mut committed = false;
        loop {
            let batch = BlockRange::new(
                next,
                BlockNumber(
                    next.0
                        .saturating_add(batch_blocks - 1)
                        .min(request.range.end().0),
                ),
            )
            .map_err(|error| RuntimeError::InvalidConfig(error.to_string()))?;
            let mut active_chunk = Some(
                self.pipeline_budget
                    .acquire_active_chunk(&cancellation)
                    .await?,
            );
            let mut frames = self
                .store
                .recent_frames(
                    request.chain_id,
                    batch,
                    self.pipeline_budget.maximum_mapped_bytes,
                )
                .await?;
            let mut usable = 0;
            for (number, frame) in (next.0..).zip(&frames) {
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
                        .any(|requirement| requirement.validate_frame(frame).is_err())
                {
                    break;
                }
                usable += 1;
            }
            frames.truncate(usable);
            let Some(last) = frames.last().map(|frame| frame.block.number) else {
                return Ok(committed);
            };
            let range = BlockRange::new(next, last)
                .map_err(|error| RuntimeError::InvalidConfig(error.to_string()))?;
            let completes = range.end() == request.range.end();
            parent = match self
                .run_material_chunk(
                    HistoricalMaterialCoordinator::retained(frames),
                    range,
                    parent,
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
                    completes_subscription && completes,
                    expected_successor_parent.filter(|_| completes),
                    &mut || active_chunk = None,
                )
                .await?
            {
                ChunkOutcome::Committed(last) => last,
                ChunkOutcome::Released => return Ok(true),
            };
            checkpoint.chunks_completed = checkpoint.chunks_completed.saturating_add(1);
            self.save_checkpoint(job, job_payload, JobState::Running, attempts, checkpoint)
                .await?;
            committed = true;
            if completes {
                verify_successor_anchor(request.range, parent, expected_successor_parent)?;
                return Ok(true);
            }
            next = BlockNumber(last.0.saturating_add(1));
        }
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
                prior_chunk_last = match self
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
                        &mut || opened.clear(),
                    )
                    .await?
                {
                    ChunkOutcome::Committed(last) => last,
                    // The job replans the rest of the gap from its progress.
                    ChunkOutcome::Released => return Ok(()),
                };
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
            let mut active_chunk = Some(
                self.pipeline_budget
                    .acquire_active_chunk(&cancellation)
                    .await?,
            );
            let stream = HistoricalMaterialCoordinator::standalone(
                source.open(&chunk, budget, cancellation.clone()).await?,
            );
            prior_chunk_last = match self
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
                    &mut || active_chunk = None,
                )
                .await?
            {
                ChunkOutcome::Committed(last) => last,
                // The job replans the rest of the gap from its progress.
                ChunkOutcome::Released => return Ok(()),
            };
            checkpoint.chunks_completed = checkpoint.chunks_completed.saturating_add(1);
            self.save_checkpoint(job, job_payload, JobState::Running, attempts, checkpoint)
                .await?;
        }
        verify_successor_anchor(request.range, prior_chunk_last, expected_successor_parent)?;
        Ok(())
    }

    /// Map and commit one chunk's frames in block order.
    ///
    /// A job that pauses for delivery or artifact backpressure keeps only the
    /// one mapped frame whose commit was refused. Before it waits, it drops
    /// the rest of a split microbatch with its mapped-byte permits, this
    /// chunk's stream, buffered source frames, and shared acquisitions it
    /// reads, and calls `release` so the caller drops the streams and chunk
    /// slots it holds for the job. Other jobs, and other readers of a shared
    /// acquisition, keep running. Once that frame commits, the chunk ends as
    /// [`ChunkOutcome::Released`] and the job replans the rest of its gap
    /// from durable progress, reading and mapping the dropped frames again.
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
        release: &mut (dyn FnMut() + Send),
    ) -> Result<ChunkOutcome, RuntimeError> {
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
                    release,
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
        let mut mapped_frames = mapped.buffered(self.config.mapper_concurrency).boxed();
        let mut released = false;
        while let Some(result) = mapped_frames.next().await {
            if cancellation.is_cancelled() {
                return Err(RuntimeError::Cancelled);
            }
            let mut mapped = result?;
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
                        release();
                        mapped_frames = futures::stream::empty().boxed();
                        mapped.material = None;
                        released = true;
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
            if let Some(material) = &mapped.material {
                material.acknowledge();
            }
            checkpoint.last_block = Some(mapped.delta.block.number);
            checkpoint.last_hash = Some(mapped.delta.block.hash);
            self.save_checkpoint(job, job_payload, JobState::Running, attempts, checkpoint)
                .await?;
        }
        if released {
            return Ok(ChunkOutcome::Released);
        }
        if next_number != range.end().0.saturating_add(1) {
            return Err(RuntimeError::IncompleteChunk {
                expected_through: range.end(),
                next: BlockNumber(next_number),
            });
        }
        Ok(ChunkOutcome::Committed(expected_parent))
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
        release: &mut (dyn FnMut() + Send),
    ) -> Result<ChunkOutcome, RuntimeError> {
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
        let mut mapped = mapped.buffered(self.config.mapper_concurrency).boxed();
        let mut stream_complete = false;
        let mut released = false;
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
                &mut || {
                    mapped = futures::stream::empty().boxed();
                    released = true;
                    release();
                },
            )
            .await?;
        }
        if released {
            return Ok(ChunkOutcome::Released);
        }
        if next_number != range.end().0.saturating_add(1) {
            return Err(RuntimeError::IncompleteChunk {
                expected_through: range.end(),
                next: BlockNumber(next_number),
            });
        }
        Ok(ChunkOutcome::Committed(expected_parent))
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
        release: &mut (dyn FnMut() + Send),
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
                        if let Some(material) = &mapped.material {
                            material.acknowledge();
                        }
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
                        if let Some(material) = &mapped.material {
                            material.acknowledge();
                        }
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
                    release_while_paused(release, &mut batch, &mut pending);
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
                    release_while_paused(release, &mut batch, &mut pending);
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
    /// Hard limit on retained recent frames, in encoded bytes. At the limit
    /// live ingestion waits, with readiness down, until finality prunes.
    pub recent_hard_bytes: u64,
    /// Longest wait at `recent_hard_bytes` before the live lane fails, so its
    /// supervisor restarts it from a fresh finalized anchor that finality can
    /// prune through.
    pub recent_storage_stall_limit: Duration,
}

impl Default for SharedLiveRuntimeConfig {
    fn default() -> Self {
        Self {
            max_reorg_depth: 64,
            pending_delta_bytes: 512 * 1_024 * 1_024,
            sink_ids: Vec::new(),
            committed_events: None,
            recent_hard_bytes: 2 * 1024 * 1024 * 1024,
            recent_storage_stall_limit: Duration::from_mins(15),
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
    /// Serializes processor-lane work across clones: a live commit, a gap
    /// drain, or parking a lane. A reconcile on one clone therefore never
    /// races another clone's live loop over the same gap marker.
    lane_lock: Arc<tokio::sync::Mutex<()>>,
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
            .field("lane_busy", &self.lane_lock.try_lock().is_err())
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
        if config.max_reorg_depth == 0
            || config.pending_delta_bytes == 0
            || config.recent_hard_bytes == 0
            || config.recent_storage_stall_limit.is_zero()
        {
            return Err(RuntimeError::InvalidConfig(
                "shared live reorg, pending-delta, and recent-storage bounds must be non-zero"
                    .to_owned(),
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
            lane_lock: Arc::new(tokio::sync::Mutex::new(())),
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
    /// retain bounded deltas; block-local processors publish immediately. A
    /// failure that belongs to one processor, such as its reducer rejecting a
    /// block, parks only that processor's lane.
    ///
    /// # Errors
    ///
    /// Fails closed on invalid or non-contiguous frames, reorgs that are
    /// finalized, too deep, or below the canonical tip, a stall at the recent
    /// storage limit, store failures outside one processor's lane, or source
    /// resets.
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
        {
            let _lane = self.lane_lock.lock().await;
            Box::pin(self.drain_pending(&mut report)).await?;
        }
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

    /// Seed a verified finalized execution anchor before the live lanes open,
    /// first reverting every retained unfinalized canonical block that is not
    /// proven to be on its chain.
    ///
    /// After downtime, the retained tip may be on a branch the network
    /// reorged away, below an anchor it does not link to. Promotion goes by
    /// height, so finality would otherwise finalize that branch's coverage.
    /// Such blocks are handled as a reorg: the store reverts their canonical
    /// rows, and [`Self::reconcile_startup`], which must run next, undoes
    /// every processor's coverage of them. Returns the reverted blocks,
    /// highest first.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError::FinalityContradiction`] when a finalized
    /// canonical block holds another hash at the anchor's height, which no
    /// revert repairs, and store failures.
    pub async fn seed_finalized_anchor(
        &self,
        anchor: BlockRef,
    ) -> Result<Vec<BlockRef>, RuntimeError> {
        let chain_id = self.source.descriptor().chain_id;
        let _lane = self.lane_lock.lock().await;
        let reverted = self
            .store
            .revert_unproven_recent_blocks(chain_id, anchor)
            .await?;
        if let (Some(highest), Some(lowest)) = (reverted.first(), reverted.last()) {
            warn!(
                finalized_anchor = anchor.number.0,
                reverted_blocks = reverted.len(),
                highest_reverted = highest.number.0,
                lowest_reverted = lowest.number.0,
                "retained unfinalized blocks do not link to the verified finalized anchor; reverting them as a reorg"
            );
        }
        match self
            .store
            .store_canonical_anchor(chain_id, anchor, Finality::Finalized)
            .await
        {
            Ok(()) => Ok(reverted),
            Err(StoreError::CanonicalConflict { block, stored, .. }) => {
                Err(RuntimeError::FinalityContradiction {
                    block,
                    detail: format!(
                        "the finalized hash is {}, the finalized canonical hash {stored}",
                        anchor.hash
                    ),
                })
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Repair what an interrupted commit or reorg left in every processor's
    /// state, then drain like [`Self::reconcile_pending`]. Run it before the
    /// live lanes open.
    ///
    /// The canonical recent write and each processor's apply commit
    /// separately, as do a reorg's canonical switch and each processor's
    /// undo. A crash, abort, or error between them can leave a processor on
    /// a reverted branch, behind the canonical tip, or holding pending deltas
    /// that no drain applies. For every processor, reconciliation:
    ///
    /// 1. undoes its unfinalized blocks that are not canonical at their
    ///    height, newest first;
    /// 2. records the first unapplied block of a lane without a gap that
    ///    trails the retained canonical frames, so the drain replays them
    ///    through the normal gap path: a running lane pauses there
    ///    (`startup_reconciliation_replay`), and a lane the store paused or
    ///    failed at a delivery limit keeps its state until it resumes or is
    ///    reset;
    /// 3. deletes its pending deltas that it can never apply: below its
    ///    start, for a block that is not canonical, for a block it already
    ///    applied, or, for a block-local lane, below its first unapplied
    ///    block. The drain applies the rest.
    ///
    /// Each repair is logged, and the report counts undone blocks as
    /// reverted. A consistent store is left untouched, so a second run is a
    /// no-op.
    ///
    /// # Errors
    ///
    /// Returns store failures, and the errors of [`Self::reconcile_pending`].
    pub async fn reconcile_startup(&self) -> Result<SharedLiveReport, RuntimeError> {
        for processor in &self.processors {
            self.store
                .register_processor(processor.descriptor())
                .await?;
        }
        let mut undone = Vec::with_capacity(self.processors.len());
        {
            let _lane = self.lane_lock.lock().await;
            for processor in &self.processors {
                undone.push(self.repair_interrupted_commit(processor.as_ref()).await?);
            }
        }
        let mut report = self.reconcile_pending().await?;
        for (processor, undone) in self.processors.iter().zip(undone) {
            let processor_report = report
                .processors
                .entry(processor.descriptor().id.to_string())
                .or_default();
            processor_report.reverted = processor_report.reverted.saturating_add(undone);
        }
        Ok(report)
    }

    /// The per-processor repairs of [`Self::reconcile_startup`]. Returns how
    /// many blocks it undid.
    async fn repair_interrupted_commit(
        &self,
        processor: &dyn Processor,
    ) -> Result<u64, RuntimeError> {
        let descriptor = processor.descriptor();
        let chain_id = self.source.descriptor().chain_id;
        let reverted = self
            .store
            .noncanonical_unfinalized_blocks(descriptor, chain_id)
            .await?;
        for (number, hash) in &reverted {
            self.store
                .undo(descriptor, chain_id, *number, *hash, &self.config.sink_ids)
                .await?;
        }
        let start = processor_start(processor)?;
        let replay_from = self.park_lane_behind_retained_tip(processor, start).await?;
        // A block-local lane applies a pending delta only at its gap, so one
        // below its first unapplied block, such as an interrupted backfill
        // commit left, never applies; that backfill maps the block again.
        let keep_from = if descriptor.mode == ReductionMode::BlockLocal {
            match self.store.live_lane_gap(descriptor).await? {
                Some(gap) => gap.first_unapplied.number,
                None => self
                    .store
                    .processor_cursor(descriptor)
                    .await?
                    .map_or(start, |cursor| {
                        BlockNumber(cursor.block_number.0.saturating_add(1))
                    }),
            }
            .max(start)
        } else {
            start
        };
        let discarded = self
            .store
            .discard_unappliable_pending_deltas(descriptor, chain_id, keep_from)
            .await?;
        let undone = u64::try_from(reverted.len()).unwrap_or(u64::MAX);
        if undone != 0 || discarded != 0 || replay_from.is_some() {
            warn!(
                processor_instance = %descriptor.instance,
                undone_blocks = undone,
                highest_undone_block = ?reverted.first().map(|(number, _)| number.0),
                discarded_pending_deltas = discarded,
                replay_from_block = ?replay_from.map(|block| block.0),
                "startup reconciliation repaired processor state against the canonical chain"
            );
        }
        Ok(undone)
    }

    /// Record the first unapplied block of a lane without a gap marker that
    /// trails the retained canonical frames, so the drain replays them, and
    /// return that block.
    ///
    /// A running lane pauses there (`startup_reconciliation_replay`). A paused
    /// or failed lane keeps its state and reason: the store stopped it at a
    /// delivery limit, and an interruption kept the runtime from recording
    /// where, so it replays from there once it resumes or is reset. A lane
    /// failed for another reason, such as a finality contradiction, gets no
    /// marker.
    ///
    /// An ordered lane replays from the block after its cursor, or from its
    /// start, which must be retained; earlier blocks are history's, and the
    /// drain replays only a block that descends from the cursor. A
    /// block-local lane replays every retained block above its cursor, from
    /// the first one retained: a hole in the retained frames, such as blocks
    /// never retained over downtime, is its cold backfill's.
    async fn park_lane_behind_retained_tip(
        &self,
        processor: &dyn Processor,
        start: BlockNumber,
    ) -> Result<Option<BlockNumber>, RuntimeError> {
        let descriptor = processor.descriptor();
        let chain_id = self.source.descriptor().chain_id;
        if self.store.live_lane_gap(descriptor).await?.is_some() {
            return Ok(None);
        }
        let Some(retained) = self.store.recent_canonical_bounds(chain_id).await? else {
            return Ok(None);
        };
        let cursor = self.store.processor_cursor(descriptor).await?;
        let mut first = cursor.as_ref().map_or(start, |cursor| {
            BlockNumber(cursor.block_number.0.saturating_add(1))
        });
        if descriptor.mode == ReductionMode::BlockLocal {
            first = first.max(retained.start());
        }
        if first > retained.end() {
            return Ok(None);
        }
        let frame = if descriptor.mode == ReductionMode::BlockLocal {
            self.store.next_recent_frame(chain_id, first).await?
        } else {
            self.store.recent_frame(chain_id, first).await?
        };
        let Some(frame) = frame else {
            return Ok(None);
        };
        let state = self.store.processor_runtime_state(descriptor).await?;
        let reason = match state.state {
            ProcessorRunState::Running => None,
            ProcessorRunState::Paused | ProcessorRunState::Failed => state.reason.as_deref(),
        };
        if !self
            .store
            .park_processor_live_lane_at(
                descriptor,
                frame.block,
                reason.unwrap_or("startup_reconciliation_replay"),
                state.state == ProcessorRunState::Failed,
            )
            .await?
        {
            return Ok(None);
        }
        self.unavailable_processors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(descriptor.instance.to_string());
        Ok(Some(frame.block.number))
    }

    /// Park one processor's live lane after a failure outside the live stream,
    /// such as its cold backfill, while shared ingestion and every other lane
    /// continue.
    ///
    /// The lane pauses with its gap at the newest retained canonical frame,
    /// so its replay covers every block it skips from now on; replaying a
    /// block it already applied is idempotent. A block-local lane replays and
    /// resumes at the next drain, since its blocks do not depend on history.
    /// An ordered lane waits until its history reaches the gap. A lane that is
    /// already parked, or a store with no retained frame yet, is left as it
    /// is. Returns whether the lane was parked.
    ///
    /// # Errors
    ///
    /// Rejects a processor this runtime does not run, and returns store
    /// failures.
    pub async fn park_processor_lane(
        &self,
        descriptor: &ProcessorDescriptor,
        reason: &str,
    ) -> Result<bool, RuntimeError> {
        if !self
            .processors
            .iter()
            .any(|processor| processor.descriptor().instance == descriptor.instance)
        {
            return Err(RuntimeError::InvalidConfig(format!(
                "processor {} is not configured on this live runtime",
                descriptor.instance
            )));
        }
        let _lane = self.lane_lock.lock().await;
        if self.store.processor_runtime_state(descriptor).await?.state != ProcessorRunState::Running
            || self.store.live_lane_gap(descriptor).await?.is_some()
        {
            return Ok(false);
        }
        // The frame's own block reference: a canonical row seeded from a
        // finality anchor carries no parent hash to replay against.
        let chain_id = self.source.descriptor().chain_id;
        let Some(retained) = self.store.recent_canonical_bounds(chain_id).await? else {
            return Ok(false);
        };
        let Some(tip) = self
            .store
            .recent_frame(chain_id, retained.end())
            .await?
            .map(|frame| frame.block)
        else {
            return Ok(false);
        };
        self.store
            .park_processor_live_lane_at(descriptor, tip, reason, false)
            .await?;
        self.unavailable_processors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(descriptor.instance.to_string());
        warn!(
            processor_instance = %descriptor.instance,
            first_unapplied_block = tip.number.0,
            reason,
            "parked one live processor lane; shared ingestion and other processors continue"
        );
        Ok(true)
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
        // Request every configured lane's material, including lanes that are
        // parked now: they resume by replaying the frames retained meanwhile,
        // so those frames must carry their material too.
        let events = self
            .source
            .subscribe(
                compile_live_request(&self.processors, self.source.descriptor().chain_id, &start)?,
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
                        let _lane = self.lane_lock.lock().await;
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
                        self.verify_live_continuity(&frame).await?;
                        if !self
                            .retain_live_frame(&frame, readiness.as_ref(), &cancellation)
                            .await?
                        {
                            break;
                        }
                        let _lane = self.lane_lock.lock().await;
                        let committed = frame.clone();
                        let prepared = self.prepare_frame(frame, false).await?;
                        Box::pin(self.commit_prepared(prepared, &mut report)).await?;
                        self.publish_committed(ChainEvent::Block(Box::new(committed)));
                        report.chain_blocks = report.chain_blocks.saturating_add(1);
                    }
                    ChainEvent::Reorg { reverted, applied } => {
                        signal_readiness(readiness.as_ref(), true);
                        let _lane = self.lane_lock.lock().await;
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
            {
                let _lane = self.lane_lock.lock().await;
                Box::pin(self.drain_pending(&mut report)).await?;
            }
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

    /// Reject a live block that does not continue the retained canonical
    /// chain, before any processor maps it.
    ///
    /// A block at a retained height must be that retained block (a source may
    /// deliver it again), a new block must extend the canonical tip, and any
    /// block must name its retained parent. A source that breaks this has
    /// violated its protocol, and the error sends it through the reconnect
    /// path instead of applying the block.
    async fn verify_live_continuity(
        &self,
        frame: &leani_primitives::BlockFrame,
    ) -> Result<(), RuntimeError> {
        frame
            .validate_shape()
            .map_err(|error| RuntimeError::InvalidFrame(error.to_owned()))?;
        let chain_id = self.source.descriptor().chain_id;
        if frame.chain_id != chain_id {
            return Err(RuntimeError::InvalidFrame(
                "live frame and source chains differ".to_owned(),
            ));
        }
        let number = frame.block.number;
        if let Some((retained, _)) = self.store.canonical_block(chain_id, number).await? {
            if retained.hash != frame.block.hash {
                return Err(StoreError::CanonicalConflict {
                    block: number,
                    stored: retained.hash,
                    incoming: frame.block.hash,
                }
                .into());
            }
        } else if let Some(tip) = self.store.canonical_tip(chain_id).await?
            && number > tip.number
        {
            let expected = BlockNumber(tip.number.0.saturating_add(1));
            if number != expected || frame.block.parent_hash != tip.hash {
                return Err(RuntimeError::LiveGap {
                    expected,
                    received: frame.block,
                });
            }
            return Ok(());
        }
        if let Some(parent) = number.0.checked_sub(1)
            && let Some((parent, _)) = self
                .store
                .canonical_block(chain_id, BlockNumber(parent))
                .await?
            && parent.hash != frame.block.parent_hash
        {
            return Err(RuntimeError::LiveGap {
                expected: number,
                received: frame.block,
            });
        }
        Ok(())
    }

    /// Retain a live frame within the recent hard limit before any processor
    /// maps it, waiting while the limit is reached.
    ///
    /// Retained frames are the reorg and replay input, so none is dropped to
    /// make room: ingestion stops and readiness drops until finality prunes
    /// older frames. After `recent_storage_stall_limit` the lane fails, so its
    /// supervisor restarts it from a fresh finalized anchor. Returns `false`
    /// when cancelled while waiting.
    async fn retain_live_frame(
        &self,
        frame: &leani_primitives::BlockFrame,
        readiness: Option<&tokio::sync::watch::Sender<bool>>,
        cancellation: &CancellationToken,
    ) -> Result<bool, RuntimeError> {
        let mut full_since = None;
        loop {
            match self
                .store
                .store_recent_frame_within(frame, self.config.recent_hard_bytes)
                .await
            {
                Ok(()) => {
                    if full_since.is_some() {
                        signal_readiness(readiness, true);
                        info!(
                            block = frame.block.number.0,
                            "recent frame storage has room again; live ingestion resumes"
                        );
                    }
                    return Ok(true);
                }
                Err(StoreError::RecentStorageLimit {
                    limit_bytes,
                    projected_bytes,
                }) => {
                    let since = *full_since.get_or_insert_with(|| {
                        signal_readiness(readiness, false);
                        warn!(
                            reason = "recent_storage_full",
                            block = frame.block.number.0,
                            limit_bytes,
                            projected_bytes,
                            "recent frame storage is at its hard limit; live ingestion waits until finality prunes it"
                        );
                        Instant::now()
                    });
                    if since.elapsed() >= self.config.recent_storage_stall_limit {
                        return Err(RuntimeError::RecentStorageBudget {
                            limit: limit_bytes,
                            observed: projected_bytes,
                        });
                    }
                    tokio::select! {
                        () = cancellation.cancelled() => return Ok(false),
                        () = tokio::time::sleep(RECENT_STORAGE_RETRY_INTERVAL) => {}
                    }
                }
                Err(error) => return Err(error.into()),
            }
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
        // A lane the store paused at its delivery limit without a gap marker,
        // as a cold-backfill commit on its live stream can, is mapped again:
        // its next block commits as usual, so a refusal parks it where it
        // stopped instead of the lane skipping blocks.
        if state.state != ProcessorRunState::Failed && !has_gap {
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
            let unavailable = self
                .processor_live_lane_unavailable(processor.as_ref())
                .await?;
            if unavailable && !map_unavailable {
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
                // A parked lane is mapped only to rebase its gap onto a reorg
                // replacement. The reorg leaves its state alone; the lane's own
                // replay maps the canonical block again and isolates a failure
                // then.
                Err(error) if unavailable => {
                    debug!(
                        processor_instance = %processor.descriptor().instance,
                        block = frame.block.number.0,
                        %error,
                        "parked live lane cannot map a reorg replacement; its gap replay maps it again"
                    );
                    deltas.push(None);
                }
                Err(error) => {
                    self.isolate_live_mapping_failure(processor.as_ref(), frame.block, &error)
                        .await?;
                    deltas.push(None);
                }
            }
        }
        Ok(PreparedFrame { frame, deltas })
    }

    /// Commit one mapped frame to every lane. The caller has already retained
    /// the frame: a live block within the recent limit, a reorg replacement
    /// with its reorg.
    async fn commit_prepared(
        &self,
        prepared: PreparedFrame,
        report: &mut SharedLiveReport,
    ) -> Result<(), RuntimeError> {
        for (processor, mapped) in self.processors.iter().zip(&prepared.deltas) {
            #[cfg(test)]
            failpoints::hit(
                failpoints::BEFORE_LIVE_APPLY,
                processor.descriptor(),
                prepared.frame.block.number,
            )?;
            let Some(mapped) = mapped else {
                self.record_failed_lane_block(processor.as_ref(), prepared.frame.block)
                    .await?;
                continue;
            };
            // A failed lane, or one parked at a gap, is not committed directly,
            // whatever its mode or delivery ordering: its gap replay covers the
            // block, and a lane failed elsewhere (such as by finality) stays
            // frozen. It is not mapped again until it is available.
            if self
                .store
                .processor_runtime_state(processor.descriptor())
                .await?
                .state
                == ProcessorRunState::Failed
                || self
                    .store
                    .live_lane_gap(processor.descriptor())
                    .await?
                    .is_some()
            {
                self.unavailable_processors
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(processor.descriptor().instance.to_string());
                self.record_failed_lane_block(processor.as_ref(), prepared.frame.block)
                    .await?;
                continue;
            }
            let committed = if processor.descriptor().mode == ReductionMode::BlockLocal {
                commit_mapped_delta(
                    &self.store,
                    processor.as_ref(),
                    mapped,
                    &self.config.sink_ids,
                    true,
                    None,
                )
                .await
                .map(Some)
            } else {
                self.commit_ordered_mapped(processor.as_ref(), mapped).await
            };
            let outcome = match committed {
                Ok(outcome) => outcome,
                Err(error) if live_lane_isolatable_error(&error) => {
                    self.park_live_lane(processor.as_ref(), mapped, &error)
                        .await?;
                    None
                }
                Err(error) => return Err(error),
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

    /// Record `block` as the first unapplied block of a lane the store failed
    /// at its delivery limit, as a cold-backfill commit on the lane's live
    /// stream can, before anything recorded where the lane stopped: without
    /// that marker an operator reset has nothing to replay from. A block-local
    /// lane records a block it has not applied. An ordered lane records only
    /// the block right after its cursor, or its start: a block further ahead
    /// would put its gap above blocks it still needs from history.
    async fn record_failed_lane_block(
        &self,
        processor: &dyn Processor,
        block: BlockRef,
    ) -> Result<(), RuntimeError> {
        let descriptor = processor.descriptor();
        let state = self.store.processor_runtime_state(descriptor).await?;
        if state.state != ProcessorRunState::Failed
            || self.store.live_lane_gap(descriptor).await?.is_some()
        {
            return Ok(());
        }
        let first_unapplied = if descriptor.mode == ReductionMode::OrderedState {
            let next = match self.store.processor_cursor(descriptor).await? {
                Some(cursor) => BlockNumber(cursor.block_number.0.saturating_add(1)),
                None => processor_start(processor)?,
            };
            block.number == next
        } else {
            !self.lane_has_applied(processor, block).await?
        };
        // The store records a marker only for a failure it recorded itself
        // at a delivery limit; a lane failed for another reason is left alone.
        if first_unapplied
            && let Some(reason) = state.reason.as_deref()
            && self
                .store
                .park_processor_live_lane_at(descriptor, block, reason, true)
                .await?
        {
            warn!(
                processor_instance = %descriptor.instance,
                first_unapplied_block = block.number.0,
                reason,
                "recorded where a live lane the store failed stopped; an operator reset replays from there"
            );
        }
        Ok(())
    }

    async fn park_live_lane(
        &self,
        processor: &dyn Processor,
        mapped: &MappedFrame,
        error: &RuntimeError,
    ) -> Result<(), RuntimeError> {
        #[cfg(test)]
        failpoints::hit(
            failpoints::BEFORE_LIVE_PARK,
            processor.descriptor(),
            mapped.delta.block.number,
        )?;
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
        let (reason, required_delivery_bytes, failed) = live_lane_park_reason(error)?;
        let (reason, failed) = if persist_delta {
            (reason, failed)
        } else {
            ("live_gap_marker_exceeds_pending_delta_budget", true)
        };
        // The store decides atomically: a lane failed meanwhile, such as by
        // finality, keeps its first failure unchanged.
        let recorded = self
            .store
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
        if recorded {
            warn!(
                processor_instance = %processor.descriptor().instance,
                first_unapplied_block = mapped.delta.block.number.0,
                persist_delta,
                %error,
                "paused one live processor lane; shared ingestion and other processors continue"
            );
        } else {
            warn!(
                processor_instance = %processor.descriptor().instance,
                block = mapped.delta.block.number.0,
                %error,
                "live processor lane had already failed; keeping its first failure"
            );
        }
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

    /// The retained frame for a lane's first unapplied block, if the
    /// processor can read it.
    ///
    /// A retained frame carries only the material requested by the processors
    /// that were live when it arrived, so a lane that was paused then may find
    /// it filtered for others. Such a frame is a miss: the caller recovers the
    /// block like one that was never retained, instead of failing the lane.
    async fn replayable_recent_frame(
        &self,
        processor: &dyn Processor,
        first_unapplied: BlockRef,
    ) -> Result<Option<leani_primitives::BlockFrame>, RuntimeError> {
        let Some(frame) = self
            .store
            .recent_frame(self.source.descriptor().chain_id, first_unapplied.number)
            .await?
            .filter(|frame| same_block(frame.block, first_unapplied))
        else {
            return Ok(None);
        };
        for requirement in &processor.descriptor().requirements {
            if let Err(error) = requirement.validate_frame(&frame) {
                // Logged at debug: a lane waiting on this block retries every
                // drain, and its park reason already says what it waits for.
                debug!(
                    processor_instance = %processor.descriptor().instance,
                    block = first_unapplied.number.0,
                    %error,
                    "retained frame lacks material this processor requires; its live-gap replay treats the block as not retained"
                );
                return Ok(None);
            }
        }
        Ok(Some(frame))
    }

    /// Apply one live-gap block and move the gap past it.
    ///
    /// Every gap replay applies through here, so one isolation policy covers
    /// them all: a processor-local failure stops only this lane (see
    /// [`Self::isolate_replay_failure`]) and returns `false`.
    async fn apply_gap_delta(
        &self,
        processor: &dyn Processor,
        delta: &EncodedDelta,
        finality: Finality,
        report: &mut SharedLiveReport,
    ) -> Result<bool, RuntimeError> {
        let applied = if finality == Finality::Finalized {
            self.apply_recovered_delta(processor, delta).await
        } else {
            self.apply_delta(processor, delta, finality).await
        };
        let outcome = match applied {
            Ok(outcome) => outcome,
            Err(error) if live_lane_isolatable_error(&error) => {
                self.isolate_replay_failure(processor, delta.block, &error)
                    .await?;
                return Ok(false);
            }
            Err(error) => return Err(error),
        };
        record_apply(
            report
                .processors
                .entry(processor.descriptor().id.to_string())
                .or_default(),
            &outcome,
        );
        self.advance_or_complete_live_gap(processor, delta.block)
            .await
    }

    /// Map a retained frame for a lane's gap and apply it; `false` when the
    /// lane stopped.
    async fn replay_gap_frame(
        &self,
        processor: &dyn Processor,
        frame: &leani_primitives::BlockFrame,
        report: &mut SharedLiveReport,
    ) -> Result<bool, RuntimeError> {
        let (delta, _) = match map_with_finality_variants(processor, frame).await {
            Ok(mapped) => mapped,
            Err(error) => {
                self.isolate_live_mapping_failure(processor, frame.block, &error)
                    .await?;
                return Ok(false);
            }
        };
        self.apply_gap_delta(processor, &delta, frame.finality, report)
            .await
    }

    /// Stop one lane after a processor-local failure while it replays.
    ///
    /// Capacity and backpressure errors leave the lane paused at its gap, and
    /// a later drain retries it. Any other failure fails the lane at `block`
    /// with the reason a live commit records, until an operator resets it.
    async fn isolate_replay_failure(
        &self,
        processor: &dyn Processor,
        block: BlockRef,
        error: &RuntimeError,
    ) -> Result<(), RuntimeError> {
        let (reason, _, failed) = live_lane_park_reason(error)?;
        if !failed || matches!(error, RuntimeError::Store(StoreError::ProcessorFailed(_))) {
            return Ok(());
        }
        warn!(
            processor_instance = %processor.descriptor().instance,
            block = block.number.0,
            reason,
            %error,
            "live-gap replay failed; isolating the processor lane while shared ingestion continues"
        );
        self.store
            .park_processor_live_lane_at(processor.descriptor(), block, reason, true)
            .await?;
        self.unavailable_processors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(processor.descriptor().instance.to_string());
        Ok(())
    }

    /// Keep a lane paused at its gap for `reason`, writing only on a change so
    /// a lane that waits across many drains keeps its first pause time.
    async fn pause_live_gap(
        &self,
        processor: &dyn Processor,
        first_unapplied: BlockRef,
        reason: &str,
    ) -> Result<(), RuntimeError> {
        let state = self
            .store
            .processor_runtime_state(processor.descriptor())
            .await?;
        if state.state == ProcessorRunState::Paused && state.reason.as_deref() == Some(reason) {
            return Ok(());
        }
        warn!(
            processor_instance = %processor.descriptor().instance,
            first_unapplied_block = first_unapplied.number.0,
            reason,
            "pausing one live lane at its gap; shared ingestion and other processors continue"
        );
        self.pause_lane(processor, reason).await
    }

    /// Pause one lane for `reason`. A lane that another component, such as
    /// finality, failed meanwhile stays failed, which is not a live-lane error.
    async fn pause_lane(
        &self,
        processor: &dyn Processor,
        reason: &str,
    ) -> Result<(), RuntimeError> {
        match self
            .store
            .pause_processor_live_lane(processor.descriptor(), reason)
            .await
        {
            Ok(()) | Err(StoreError::ProcessorFailed(_)) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    /// The canonical block at `number`, taking its reference from the
    /// retained frame when there is one: a canonical row seeded from a
    /// finality anchor has no parent hash.
    async fn canonical_ref(&self, number: BlockNumber) -> Result<Option<BlockRef>, RuntimeError> {
        let chain_id = self.source.descriptor().chain_id;
        Ok(match self.store.recent_frame(chain_id, number).await? {
            Some(frame) => Some(frame.block),
            None => self
                .store
                .canonical_block(chain_id, number)
                .await?
                .map(|(block, _)| block),
        })
    }

    /// Whether a gap marker names a block on the canonical chain.
    async fn gap_marker_is_canonical(&self, marker: BlockRef) -> Result<bool, RuntimeError> {
        Ok(self
            .store
            .canonical_block(self.source.descriptor().chain_id, marker.number)
            .await?
            .is_some_and(|(canonical, _)| same_block(canonical, marker)))
    }

    /// Whether this lane has applied `block`, or never needs to: a block below
    /// its start point, as the new tip after a reorg that reverted every block
    /// the lane had applied.
    async fn lane_has_applied(
        &self,
        processor: &dyn Processor,
        block: BlockRef,
    ) -> Result<bool, RuntimeError> {
        Ok(self
            .store
            .coverage_block_by_hash(processor.descriptor(), block.hash)
            .await?
            == Some(block.number)
            || block.number < processor_start(processor)?)
    }

    /// Keep a failed lane's marker on the first block it has not applied.
    ///
    /// A failed lane is not replayed, but a reorg without a replacement branch
    /// leaves its marker on the new tip, which the lane has applied. Once the
    /// tip's canonical successor is retained, the marker moves onto it, so an
    /// operator reset replays from there.
    async fn follow_failed_lane_marker(
        &self,
        processor: &dyn Processor,
    ) -> Result<(), RuntimeError> {
        let Some(gap) = self.store.live_lane_gap(processor.descriptor()).await? else {
            return Ok(());
        };
        let marker = gap.first_unapplied;
        if !self.lane_has_applied(processor, marker).await? {
            return Ok(());
        }
        if let Some(next) = self
            .canonical_ref(BlockNumber(marker.number.0.saturating_add(1)))
            .await?
            && (next.parent_hash == BlockHash::ZERO || next.parent_hash == marker.hash)
        {
            self.store
                .advance_live_lane_gap(processor.descriptor(), marker, next)
                .await?;
        }
        Ok(())
    }

    /// Re-point a gap marker that is off the canonical chain, such as one an
    /// earlier version left on a reverted block, instead of completing or
    /// failing the lane.
    ///
    /// The marker moves to the highest canonical block below it that the lane
    /// has applied, or never needs to, within the reorg depth; the drain then
    /// moves past it. If the lane applied none of those blocks, it replays
    /// from the lowest. Returns `false` only when no canonical block is in
    /// reach.
    async fn repoint_orphaned_gap(
        &self,
        processor: &dyn Processor,
        marker: BlockRef,
    ) -> Result<bool, RuntimeError> {
        let chain_id = self.source.descriptor().chain_id;
        let Some(tip) = self.store.canonical_tip(chain_id).await? else {
            return Ok(false);
        };
        let top = marker.number.0.saturating_sub(1).min(tip.number.0);
        let floor =
            top.saturating_sub(u64::try_from(self.config.max_reorg_depth).unwrap_or(u64::MAX));
        let mut target = None;
        for height in (floor..=top).rev() {
            let Some(block) = self.canonical_ref(BlockNumber(height)).await? else {
                continue;
            };
            target = Some(block);
            if self.lane_has_applied(processor, block).await? {
                break;
            }
        }
        let Some(target) = target else {
            return Ok(false);
        };
        warn!(
            processor_instance = %processor.descriptor().instance,
            orphaned_block = marker.number.0,
            target_block = target.number.0,
            "live-gap marker is off the canonical chain; re-pointing it"
        );
        self.store
            .advance_live_lane_gap(processor.descriptor(), marker, target)
            .await?;
        Ok(true)
    }

    /// Move a lane's gap past `applied`, or complete it at the canonical tip.
    ///
    /// The successor's reference comes from its retained frame when there is
    /// one: a canonical row seeded from a finality anchor has no parent hash.
    /// A successor that does not descend from `applied` fails only this lane
    /// (`live_gap_canonical_identity_changed`) and returns `false`.
    async fn advance_or_complete_live_gap(
        &self,
        processor: &dyn Processor,
        applied: BlockRef,
    ) -> Result<bool, RuntimeError> {
        let next = self
            .canonical_ref(BlockNumber(applied.number.0.saturating_add(1)))
            .await?;
        let Some(next) = next else {
            self.store
                .complete_live_lane_gap(processor.descriptor(), applied)
                .await?;
            return Ok(true);
        };
        if next.parent_hash != BlockHash::ZERO && next.parent_hash != applied.hash {
            warn!(
                processor_instance = %processor.descriptor().instance,
                applied_block = applied.number.0,
                "canonical live-gap successor does not descend from the applied block; isolating the processor lane"
            );
            self.store
                .fail_processor_live_lane(
                    processor.descriptor(),
                    "live_gap_canonical_identity_changed",
                )
                .await?;
            self.unavailable_processors
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(processor.descriptor().instance.to_string());
            return Ok(false);
        }
        self.store
            .advance_live_lane_gap(processor.descriptor(), applied, next)
            .await?;
        Ok(true)
    }

    #[allow(clippy::too_many_lines)]
    async fn recover_finalized_live_gap(
        &self,
        processor: &dyn Processor,
        first: BlockRef,
        report: &mut SharedLiveReport,
    ) -> Result<bool, RuntimeError> {
        let chain_id = self.source.descriptor().chain_id;
        // A marker off the canonical chain is never completed or failed on
        // that basis: it is re-pointed onto the chain.
        let Some((_, finality)) = self
            .store
            .canonical_block(chain_id, first.number)
            .await?
            .filter(|(canonical, _)| same_block(*canonical, first))
        else {
            return self.repoint_orphaned_gap(processor, first).await;
        };
        if finality != Finality::Finalized {
            // No retained frame serves this block, and history serves only
            // finalized blocks. Stay paused: the first drain after finality
            // reaches the block recovers it from history.
            self.pause_live_gap(processor, first, "unfinalized_gap_waiting_for_finality")
                .await?;
            return Ok(false);
        }
        let Some(recovery) = &self.finalized_gap_recovery else {
            self.pause_lane(processor, "finalized_gap_waiting_for_history_source")
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
                self.pause_lane(processor, "finalized_gap_recovery_unavailable")
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
            self.pause_lane(processor, "finalized_gap_recovery_incomplete")
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
                || !same_block(frame.block, expected)
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
            if !self
                .apply_gap_delta(processor, &delta, Finality::Finalized, report)
                .await?
            {
                return Ok(false);
            }
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
                        self.follow_failed_lane_marker(processor.as_ref()).await?;
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
                                .settle_pending_variant(processor.as_ref(), &delta, report)
                                .await?
                                == Some(true)
                        {
                            continue;
                        }
                        break;
                    };
                    if !self.gap_marker_is_canonical(gap.first_unapplied).await? {
                        if self
                            .repoint_orphaned_gap(processor.as_ref(), gap.first_unapplied)
                            .await?
                        {
                            continue;
                        }
                        break;
                    }
                    let candidates = self
                        .store
                        .pending_deltas(processor.descriptor(), gap.first_unapplied.number, 64)
                        .await?;
                    if let Some(delta) = candidates
                        .into_iter()
                        .find(|delta| same_block(delta.block, gap.first_unapplied))
                    {
                        match self
                            .settle_pending_variant(processor.as_ref(), &delta, report)
                            .await?
                        {
                            Some(true) => {
                                if self
                                    .advance_or_complete_live_gap(
                                        processor.as_ref(),
                                        gap.first_unapplied,
                                    )
                                    .await?
                                {
                                    continue;
                                }
                                break;
                            }
                            Some(false) => {}
                            None => break,
                        }
                        if let Some(frame) = self
                            .store
                            .recent_frame(delta.chain_id, delta.block.number)
                            .await?
                            .filter(|frame| frame.block == delta.block)
                        {
                            if self
                                .apply_gap_delta(processor.as_ref(), &delta, frame.finality, report)
                                .await?
                            {
                                continue;
                            }
                            break;
                        }
                    }

                    // The lane already applied its gap block, or never needs
                    // to, as when a reorg left its gap on the new tip: move
                    // past it.
                    if self
                        .lane_has_applied(processor.as_ref(), gap.first_unapplied)
                        .await?
                    {
                        if self
                            .advance_or_complete_live_gap(processor.as_ref(), gap.first_unapplied)
                            .await?
                        {
                            continue;
                        }
                        break;
                    }
                    if let Some(frame) = self
                        .replayable_recent_frame(processor.as_ref(), gap.first_unapplied)
                        .await?
                    {
                        if self
                            .replay_gap_frame(processor.as_ref(), &frame, report)
                            .await?
                        {
                            continue;
                        }
                        break;
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
                    self.follow_failed_lane_marker(processor.as_ref()).await?;
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
                    match self
                        .settle_pending_variant(processor.as_ref(), delta, report)
                        .await?
                    {
                        Some(true) => continue,
                        // Below the cursor yet never applied at this hash:
                        // the delta contradicts the lane's applied history.
                        Some(false) => {
                            self.isolate_replay_failure(
                                processor.as_ref(),
                                delta.block,
                                &StoreError::ConflictingApply {
                                    block: delta.block.number,
                                }
                                .into(),
                            )
                            .await?;
                            break;
                        }
                        None => break,
                    }
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
                        Err(error) if live_lane_isolatable_error(&error) => {
                            self.isolate_replay_failure(processor.as_ref(), delta.block, &error)
                                .await?;
                            break;
                        }
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
                        && same_block(gap.first_unapplied, delta.block)
                        && !self
                            .advance_or_complete_live_gap(processor.as_ref(), delta.block)
                            .await?
                    {
                        break;
                    }
                    continue;
                }

                let Some(gap) = self.store.live_lane_gap(processor.descriptor()).await? else {
                    break;
                };
                if !self.gap_marker_is_canonical(gap.first_unapplied).await? {
                    if self
                        .repoint_orphaned_gap(processor.as_ref(), gap.first_unapplied)
                        .await?
                    {
                        continue;
                    }
                    break;
                }
                // History, such as a later backfill, already applied the
                // parked block, or a reorg left the gap on the new tip: move
                // the gap on instead of waiting forever for a replay the
                // cursor has passed.
                if gap.first_unapplied.number < expected
                    && self
                        .lane_has_applied(processor.as_ref(), gap.first_unapplied)
                        .await?
                {
                    if self
                        .advance_or_complete_live_gap(processor.as_ref(), gap.first_unapplied)
                        .await?
                    {
                        continue;
                    }
                    break;
                }
                // The gap block must descend from the cursor. A marker on a
                // seeded anchor row has no parent hash: the block's retained
                // frame supplies it, and without one the replay requires the
                // cursor, which history recovery checks the block against.
                let mut first = gap.first_unapplied;
                if first.parent_hash == BlockHash::ZERO
                    && let Some(cursor) = &prior
                {
                    first.parent_hash = self
                        .canonical_ref(first.number)
                        .await?
                        .map(|block| block.parent_hash)
                        .filter(|parent| *parent != BlockHash::ZERO)
                        .unwrap_or(cursor.block_hash);
                }
                if first.number != expected
                    || prior
                        .as_ref()
                        .is_some_and(|cursor| first.parent_hash != cursor.block_hash)
                {
                    break;
                }
                if let Some(frame) = self
                    .replayable_recent_frame(processor.as_ref(), first)
                    .await?
                {
                    if self
                        .replay_gap_frame(processor.as_ref(), &frame, report)
                        .await?
                    {
                        continue;
                    }
                    break;
                }
                if !self
                    .recover_finalized_live_gap(processor.as_ref(), first, report)
                    .await?
                {
                    break;
                }
            }
        }
        Ok(())
    }

    /// [`Self::clear_applied_pending_variant`] under the replay isolation
    /// policy: `Some(cleared)`, or `None` when a processor-local failure, such
    /// as a pending delta that conflicts with the applied one, stopped only
    /// this lane.
    async fn settle_pending_variant(
        &self,
        processor: &dyn Processor,
        delta: &EncodedDelta,
        report: &mut SharedLiveReport,
    ) -> Result<Option<bool>, RuntimeError> {
        match self
            .clear_applied_pending_variant(processor, delta, report)
            .await
        {
            Ok(cleared) => Ok(Some(cleared)),
            Err(error) if live_lane_isolatable_error(&error) => {
                self.isolate_replay_failure(processor, delta.block, &error)
                    .await?;
                Ok(None)
            }
            Err(error) => Err(error),
        }
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
                    .mark_finalized(processor.descriptor(), delta.block.number, delta.block.hash)
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

    /// Reject a reorg that does not first revert the canonical tip. `reverted`
    /// runs tip-first, so a reorg starting below the tip would leave canonical
    /// descendants of a block it replaces.
    async fn verify_reorg_tip(
        &self,
        reverted: &[leani_primitives::BlockRef],
    ) -> Result<(), RuntimeError> {
        let tip = self
            .store
            .canonical_tip(self.source.descriptor().chain_id)
            .await?;
        if let Some(first) = reverted.first()
            && tip.is_none_or(|tip| tip.number != first.number || tip.hash != first.hash)
        {
            return Err(RuntimeError::InvalidReorg(format!(
                "reorg first reverts block {}, but the canonical tip is {:?}",
                first.number.0,
                tip.map(|tip| tip.number.0)
            )));
        }
        Ok(())
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
        self.verify_reorg_tip(reverted).await?;
        let chain_id = self.source.descriptor().chain_id;
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
        // `reverted` runs tip-first, so its last block is the lowest. The
        // store has checked that the canonical ancestor, now the tip, is
        // retained right below it.
        let lowest_reverted = reverted.last().map_or(BlockNumber(0), |block| block.number);
        let ancestor = match lowest_reverted.0.checked_sub(1) {
            Some(number) => self.canonical_ref(BlockNumber(number)).await?,
            None => None,
        };
        for (processor_index, processor) in self.processors.iter().enumerate() {
            #[cfg(test)]
            failpoints::hit(
                failpoints::BEFORE_REORG_UNDO,
                processor.descriptor(),
                lowest_reverted,
            )?;
            if let Some(gap) = self.store.live_lane_gap(processor.descriptor()).await?
                && gap.first_unapplied.number >= lowest_reverted
            {
                self.rebase_gap_across_reorg(
                    processor.as_ref(),
                    gap.first_unapplied,
                    prepared
                        .first()
                        .map(|first| (first.frame.block, first.deltas[processor_index].as_ref())),
                    ancestor,
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
        if prepared.is_empty() {
            // With no replacement branch nothing is committed, so no commit
            // drains: drain here, so a lane whose gap moved onto the new tip
            // settles before the next block.
            return Box::pin(self.drain_pending(report)).await;
        }
        for frame in prepared {
            Box::pin(self.commit_prepared(frame, report)).await?;
            report.chain_blocks = report.chain_blocks.saturating_add(1);
        }
        Ok(())
    }

    /// Keep a lane's gap on the canonical chain across a reorg that reverts
    /// blocks at or below it.
    ///
    /// The reorg undoes every block the lane applied from the lowest reverted
    /// height up, so its gap restarts at the first replacement block, even
    /// when the gap sat higher: the lane must not skip replacement blocks
    /// below it. With no replacement branch the gap moves onto the new tip,
    /// the reorg's ancestor, which the lane has applied or which lies below
    /// its start; the drain moves past it, completing the gap or moving it
    /// onto the ancestor's successor once that block is retained. A paused
    /// block-local lane that never applied the ancestor, such as a seeded
    /// finality anchor that no live block delivered, skipped nothing the new
    /// chain still has, so its gap completes. An ordered lane still needs the
    /// ancestor in order, and a failed lane keeps it for a reset. Either way no
    /// marker stays on a height the new chain no longer has.
    async fn rebase_gap_across_reorg(
        &self,
        processor: &dyn Processor,
        marker: BlockRef,
        first_replacement: Option<(BlockRef, Option<&MappedFrame>)>,
        ancestor: Option<BlockRef>,
    ) -> Result<(), RuntimeError> {
        match first_replacement {
            Some((first, Some(replacement))) if marker.number == first.number => {
                self.store
                    .rebase_live_lane_gap(processor.descriptor(), marker, &replacement.delta)
                    .await?;
            }
            // A higher gap, or a parked lane that could not map the
            // replacement: its replay maps the canonical block.
            Some((first, _)) => {
                self.store
                    .advance_live_lane_gap(processor.descriptor(), marker, first)
                    .await?;
            }
            None => {
                let Some(ancestor) = ancestor else {
                    return Ok(());
                };
                if processor.descriptor().mode == ReductionMode::BlockLocal
                    && !self.lane_has_applied(processor, ancestor).await?
                    && self
                        .store
                        .processor_runtime_state(processor.descriptor())
                        .await?
                        .state
                        != ProcessorRunState::Failed
                {
                    self.store
                        .complete_live_lane_gap(processor.descriptor(), marker)
                        .await?;
                } else {
                    self.store
                        .advance_live_lane_gap(processor.descriptor(), marker, ancestor)
                        .await?;
                }
            }
        }
        Ok(())
    }
}

/// Whether a live-lane commit error belongs to one processor lane, so the lane
/// is parked while shared ingestion continues. The store runs a processor's
/// reducer, so its failures arrive as `StoreError::Processor`, and the
/// store's per-apply invariants as `StoreError::Invariant`. A delta that
/// conflicts with the one already applied for its block is `ConflictingApply`.
fn live_lane_isolatable_error(error: &RuntimeError) -> bool {
    matches!(
        error,
        RuntimeError::Processor(_)
            | RuntimeError::Store(
                StoreError::Processor(_)
                    | StoreError::Invariant(_)
                    | StoreError::ConflictingApply { .. }
                    | StoreError::DeliveryLimit { .. }
                    | StoreError::ProcessorPaused { .. }
                    | StoreError::ProcessorFailed(_)
                    | StoreError::DeliveryItemTooLarge { .. }
                    | StoreError::PhysicalStorageLimit { .. }
                    | StoreError::ArtifactStorageLimit { .. }
            )
    )
}

/// The durable reason, the delivery bytes the lane needs, and whether the lane
/// fails (and waits for an operator reset) rather than pauses, for one
/// isolatable live-lane error.
fn live_lane_park_reason(error: &RuntimeError) -> Result<(&'static str, u64, bool), RuntimeError> {
    Ok(match error {
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
        RuntimeError::Store(StoreError::Processor(_) | StoreError::Invariant(_)) => {
            ("processor_live_reduce_failed", 0, true)
        }
        // Processor code outside the reducer: mapping, finality variants, or
        // delta encoding.
        RuntimeError::Processor(_) => ("processor_live_mapping_failed", 0, true),
        RuntimeError::Store(StoreError::ConflictingApply { .. }) => {
            ("processor_live_delta_conflict", 0, true)
        }
        _ => {
            return Err(RuntimeError::InvalidConfig(format!(
                "attempted to isolate unsupported live-lane error: {error}"
            )));
        }
    })
}

/// Whether two references name the same block. The hash commits to the whole
/// header, so the number and hash decide. A zero parent hash is unknown, as on
/// a canonical row seeded from a finality anchor, and is not compared.
fn same_block(left: BlockRef, right: BlockRef) -> bool {
    left.number == right.number
        && left.hash == right.hash
        && (left.parent_hash == BlockHash::ZERO
            || right.parent_hash == BlockHash::ZERO
            || left.parent_hash == right.parent_hash)
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
        let scope =
            covering_filter_scope(requirements.iter().map(|requirement| &requirement.filter));
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

/// Narrowest pushdown scope that covers every one of `filters`.
///
/// The fold starts from the first filter because the default scope is a
/// wildcard, which would absorb every narrower filter. An empty `filters`
/// yields the default, unfiltered scope.
#[must_use]
pub fn covering_filter_scope<'a>(
    filters: impl IntoIterator<Item = &'a leani_primitives::FilterScope>,
) -> leani_primitives::FilterScope {
    let mut filters = filters.into_iter();
    let Some(first) = filters.next() else {
        return leani_primitives::FilterScope::default();
    };
    filters.fold(first.clone(), |mut scope, filter| {
        union_filter_scope(&mut scope, filter);
        scope
    })
}

/// Widen `retained` so it also covers everything `incoming` matches.
///
/// A missing block range and an empty list are wildcards, so a wildcard on
/// either side stays a wildcard. Two block ranges widen to their hull and two
/// lists to their union. A topic position stays constrained only when both
/// sides constrain it. The result may match more than either side, never less.
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

    fn union_values<T: Clone + Eq>(retained: &mut Vec<T>, incoming: &[T]) {
        if incoming.is_empty() {
            retained.clear();
        } else if !retained.is_empty() {
            extend_unique(retained, incoming);
        }
    }

    retained.block_range =
        retained
            .block_range
            .zip(incoming.block_range)
            .and_then(|(retained, incoming)| {
                BlockRange::new(
                    retained.start().min(incoming.start()),
                    retained.end().max(incoming.end()),
                )
                .ok()
            });
    union_values(&mut retained.addresses, &incoming.addresses);
    union_values(
        &mut retained.transaction_hashes,
        &incoming.transaction_hashes,
    );
    union_values(&mut retained.transaction_types, &incoming.transaction_types);
    union_values(&mut retained.senders, &incoming.senders);
    union_values(&mut retained.recipients, &incoming.recipients);
    retained.topics.retain(|topic| {
        incoming
            .topics
            .iter()
            .any(|other| other.position == topic.position)
    });
    for topic in &mut retained.topics {
        for other in incoming
            .topics
            .iter()
            .filter(|other| other.position == topic.position)
        {
            extend_unique(&mut topic.alternatives, &other.alternatives);
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
                FinalityEvent::Finalized {
                    block_number,
                    block_hash,
                    ..
                } => {
                    let through = self
                        .store
                        .coverage_block_by_hash(self.processor.descriptor(), block_hash)
                        .await?
                        .ok_or(RuntimeError::UnknownFinalizedAnchor(block_hash))?;
                    if through != block_number {
                        return Err(RuntimeError::FinalityContradiction {
                            block: block_number,
                            detail: format!(
                                "the processor covers the finalized hash at block {}",
                                through.0
                            ),
                        });
                    }
                    self.store
                        .mark_finalized(self.processor.descriptor(), through, block_hash)
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
    /// Processors whose last finality advance failed, with the error. Their
    /// failure did not stop the others. A coverage contradiction also fails
    /// the processor's lane; any other failure is retried at the next anchor.
    #[serde(default)]
    pub failed_processors: BTreeMap<String, String>,
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

/// A verified finalized execution block, as a finality event names it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FinalizedBlock {
    number: BlockNumber,
    hash: BlockHash,
    beacon_slot: u64,
    beacon_block_root: [u8; 32],
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
    /// finality for unknown coverage. A processor whose own finality update
    /// fails is recorded in the report and retried at the next anchor; the
    /// others advance and recent frames are pruned regardless. A processor
    /// whose coverage contradicts the anchor has its lane failed, and a failed
    /// lane is not finalized until an operator resets it.
    ///
    /// # Errors
    ///
    /// A verified finalized block that races ahead of execution ingestion is
    /// retained and retried until a canonical block at its height arrives.
    ///
    /// Fails closed on a finality contradiction/reset or a store failure
    /// outside one processor's finality update. A finalized hash that
    /// contradicts the canonical block at its height, or retained canonical
    /// blocks that do not link to it, is a [`RuntimeError::FinalityReorg`]
    /// when those blocks are unfinalized, and a
    /// [`RuntimeError::FinalityContradiction`] when they are finalized.
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
                    let Some(finalized) = pending_finalized else {
                        continue;
                    };
                    if self.apply_finalized(finalized, &mut report).await? {
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
                    block_number,
                    block_hash,
                    beacon_slot,
                    beacon_block_root,
                } => {
                    let finalized = FinalizedBlock {
                        number: block_number,
                        hash: block_hash,
                        beacon_slot,
                        beacon_block_root,
                    };
                    pending_finalized = Some(finalized);
                    if self.apply_finalized(finalized, &mut report).await? {
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
    /// contradictions, protocol errors, and store failures still terminate the
    /// lane fail-closed.
    ///
    /// # Errors
    ///
    /// Returns non-transient source failures and the same fail-closed store
    /// and finality errors as [`Self::run`].
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
                        let Some(finalized) = pending_finalized else {
                            continue;
                        };
                        // A contradiction halts the lane: readiness drops
                        // with the error.
                        if self
                            .apply_finalized(finalized, &mut report)
                            .await
                            .inspect_err(|_| signal_readiness(Some(&readiness), false))?
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
                        block_number,
                        block_hash,
                        beacon_slot,
                        beacon_block_root,
                    })) => {
                        let finalized = FinalizedBlock {
                            number: block_number,
                            hash: block_hash,
                            beacon_slot,
                            beacon_block_root,
                        };
                        pending_finalized = Some(finalized);
                        if self
                            .apply_finalized(finalized, &mut report)
                            .await
                            .inspect_err(|_| signal_readiness(Some(&readiness), false))?
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

    /// Apply a verified finalized block once the canonical chain reaches its
    /// height, returning `false` until then: the live lane has not caught up.
    async fn apply_finalized(
        &self,
        finalized: FinalizedBlock,
        report: &mut SharedFinalityReport,
    ) -> Result<bool, RuntimeError> {
        let chain_id = self.source.descriptor().chain_id;
        let Some(canonical) = self.finalize_canonical_prefix(finalized).await? else {
            report.deferred_finalized_anchor = Some(finalized.hash);
            return Ok(false);
        };
        report.deferred_finalized_anchor = None;
        let block_hash = finalized.hash;
        for processor in &self.processors {
            let id = processor.descriptor().id.to_string();
            // One processor's failure must not hold back finality or recent
            // pruning for the others: record it, and let the next anchor
            // retry that processor.
            match self
                .finalize_processor(processor.as_ref(), block_hash, canonical.number)
                .await
            {
                Ok(ProcessorFinality::Finalized(through)) => {
                    report
                        .processor_finalized_through
                        .insert(id.clone(), through);
                    report.deferred_processors.remove(&id);
                    report.failed_processors.remove(&id);
                }
                Ok(ProcessorFinality::Deferred) => {
                    report.failed_processors.remove(&id);
                    report.deferred_processors.insert(id, block_hash);
                }
                Ok(ProcessorFinality::LaneFailed) => {}
                Err(error) => {
                    warn!(
                        processor = %id,
                        finalized_block = canonical.number.0,
                        %error,
                        "processor finality failed; finality and recent pruning continue for the others"
                    );
                    report.failed_processors.insert(id, error.to_string());
                }
            }
        }
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
                beacon_slot: finalized.beacon_slot,
                beacon_block_root: finalized.beacon_block_root,
            });
        }
        if pruned.hard_limit_exceeded {
            // The live lane enforces the hard limit when it retains a frame and
            // waits for this pruning. Failing here would stop that pruning.
            warn!(
                limit = self.config.recent_hard_bytes,
                retained = pruned.retained_bytes,
                finalized_block = canonical.number.0,
                "recent frames stay above their hard limit after pruning; live ingestion waits for a later finality advance"
            );
        }
        Ok(true)
    }

    /// Finalize the retained canonical prefix through a verified finalized
    /// block, and return the block, or `None` while the canonical chain has
    /// not reached its height.
    ///
    /// The canonical block at that height must be the finalized one, and the
    /// store promotes only retained blocks that link to it by parent hash. It
    /// runs before any processor is finalized. A different canonical block
    /// there, or a retained block at a height the finalized chain passes
    /// through that is not its ancestor, contradicts finality (see
    /// [`finality_contradiction`]).
    async fn finalize_canonical_prefix(
        &self,
        finalized: FinalizedBlock,
    ) -> Result<Option<BlockRef>, RuntimeError> {
        let chain_id = self.source.descriptor().chain_id;
        let Some((canonical, finality)) = self
            .store
            .canonical_block(chain_id, finalized.number)
            .await?
        else {
            return Ok(None);
        };
        if canonical.hash != finalized.hash {
            return Err(finality_contradiction(
                finalized,
                finality == Finality::Finalized,
                format!(
                    "the finalized hash is {}, the canonical hash {}",
                    finalized.hash, canonical.hash
                ),
            ));
        }
        match self
            .store
            .mark_recent_finalized(chain_id, canonical.number, finalized.hash)
            .await
        {
            Ok(_) => Ok(Some(canonical)),
            Err(StoreError::UnlinkedFinalizedAncestry {
                block,
                finalized_row,
                ..
            }) => Err(finality_contradiction(
                finalized,
                finalized_row,
                format!("retained canonical block {} is not its ancestor", block.0),
            )),
            Err(error) => Err(error.into()),
        }
    }

    /// Advance one processor's finality to the canonical anchor block.
    ///
    /// Coverage that holds the anchor hash at another height contradicts the
    /// canonical chain. That is not transient, so it is checked first, even
    /// for a lane that is already failed for another reason: the lane fails
    /// with `processor_finality_conflict`, which overrides that earlier reason
    /// and which the store refuses to reset (`live_lane_requires_rebuild`), so
    /// no later anchor finalizes it through the contradicted height. So is
    /// coverage of another block at the anchor's height, which a block-local
    /// lane can hold; deferring it instead would let a later anchor promote
    /// it by height. Any other failed lane is skipped until an operator resets
    /// it. Other errors, such as a transient `mark_finalized` failure, leave
    /// the lane as it is for the next anchor to retry.
    async fn finalize_processor(
        &self,
        processor: &dyn Processor,
        block_hash: BlockHash,
        canonical: BlockNumber,
    ) -> Result<ProcessorFinality, RuntimeError> {
        let Some(through) = self
            .store
            .coverage_block_by_hash(processor.descriptor(), block_hash)
            .await?
        else {
            if let Some(covered) = self
                .store
                .coverage_hash(processor.descriptor(), canonical)
                .await?
            {
                self.store
                    .fail_processor_live_lane(processor.descriptor(), "processor_finality_conflict")
                    .await?;
                return Err(RuntimeError::InvalidReorg(format!(
                    "processor {} covers block {} as {covered}, not the finalized {block_hash}",
                    processor.descriptor().id,
                    canonical.0
                )));
            }
            return Ok(ProcessorFinality::Deferred);
        };
        if through != canonical {
            self.store
                .fail_processor_live_lane(processor.descriptor(), "processor_finality_conflict")
                .await?;
            return Err(RuntimeError::InvalidReorg(format!(
                "processor {} resolves finalized hash at {}, canonical recent material at {}",
                processor.descriptor().id,
                through.0,
                canonical.0
            )));
        }
        if self
            .store
            .processor_runtime_state(processor.descriptor())
            .await?
            .state
            == ProcessorRunState::Failed
        {
            return Ok(ProcessorFinality::LaneFailed);
        }
        self.store
            .mark_finalized(processor.descriptor(), through, block_hash)
            .await?;
        Ok(ProcessorFinality::Finalized(through))
    }
}

/// The error for verified finality that contradicts a retained canonical
/// block: finalized, it contradicts finalized history, which halts the live
/// lane; unfinalized, it is a reorg the live lane did not follow, which a
/// restart of the network lanes repairs, since startup reverts every
/// retained unfinalized block that does not link to the verified finalized
/// anchor.
fn finality_contradiction(
    finalized: FinalizedBlock,
    against_finalized: bool,
    detail: String,
) -> RuntimeError {
    if against_finalized {
        RuntimeError::FinalityContradiction {
            block: finalized.number,
            detail,
        }
    } else {
        RuntimeError::FinalityReorg {
            block: finalized.number,
            finalized: finalized.hash,
            detail,
        }
    }
}

/// How one processor took a finalized anchor.
enum ProcessorFinality {
    /// Finalized through the anchor block.
    Finalized(BlockNumber),
    /// Its coverage has not reached the anchor block yet.
    Deferred,
    /// Its lane is failed, so finality waits for an operator reset.
    LaneFailed,
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
    /// Verified finality contradicts finalized canonical history. No reorg
    /// repairs it, so the live lane halts.
    #[error(
        "verified finality contradicts finalized canonical history at block {}: {detail}; the live lane halts",
        block.0
    )]
    FinalityContradiction { block: BlockNumber, detail: String },
    /// Verified finality contradicts retained unfinalized canonical blocks: a
    /// reorg the live lane did not follow, such as one across a stall. A
    /// restart of the network lanes reverts them.
    #[error(
        "verified finality finalizes block {} as {finalized}, which retained unfinalized blocks contradict: {detail}; the network lanes restart to revert them",
        block.0
    )]
    FinalityReorg {
        block: BlockNumber,
        finalized: BlockHash,
        detail: String,
    },
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

    /// Counts whatever transaction material the frame carries, so reusing
    /// material filtered for another consumer would silently undercount.
    #[derive(Debug)]
    struct FilteredCounter {
        inner: BlockLocalCounter,
        descriptor: ProcessorDescriptor,
    }

    impl FilteredCounter {
        fn new(requirements: Vec<leani_processor_api::DataRequirement>) -> Self {
            Self::named("filtered-counter", requirements)
        }

        fn named(id: &str, requirements: Vec<leani_processor_api::DataRequirement>) -> Self {
            let inner = BlockLocalCounter::named(id);
            let mut descriptor = inner.descriptor().clone();
            descriptor.requirements = requirements;
            Self { inner, descriptor }
        }
    }

    #[async_trait]
    impl Processor for FilteredCounter {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn descriptor(&self) -> &ProcessorDescriptor {
            &self.descriptor
        }

        async fn map(
            &self,
            block: &leani_primitives::BlockFrame,
        ) -> Result<EncodedDelta, ProcessorError> {
            for requirement in &self.descriptor.requirements {
                requirement
                    .validate_frame(block)
                    .map_err(|error| ProcessorError::Input(error.to_owned()))?;
            }
            let count = block.transactions.as_present().map_or(0, Vec::len);
            let count = u64::try_from(count).map_err(|_| {
                ProcessorError::Invariant("transaction count exceeds u64".to_owned())
            })?;
            Ok(EncodedDelta::new(
                &self.descriptor,
                block.chain_id,
                block.block,
                count.to_be_bytes().to_vec(),
            ))
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

    /// Runs `inner` under replaced input requirements, which its mapper
    /// checks first like a production processor does.
    #[derive(Debug)]
    struct Requiring<P> {
        inner: P,
        descriptor: ProcessorDescriptor,
    }

    impl<P: Processor> Requiring<P> {
        fn new(inner: P, requirements: Vec<leani_processor_api::DataRequirement>) -> Self {
            let mut descriptor = inner.descriptor().clone();
            descriptor.requirements = requirements;
            Self { inner, descriptor }
        }
    }

    #[async_trait]
    impl<P: Processor + std::fmt::Debug + 'static> Processor for Requiring<P> {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn descriptor(&self) -> &ProcessorDescriptor {
            &self.descriptor
        }

        async fn map(
            &self,
            block: &leani_primitives::BlockFrame,
        ) -> Result<EncodedDelta, ProcessorError> {
            for requirement in &self.descriptor.requirements {
                requirement
                    .validate_frame(block)
                    .map_err(|error| ProcessorError::Input(error.to_owned()))?;
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

    /// Runs `inner`, but its reducer rejects one block the way a production
    /// reducer rejects input it cannot account for.
    #[derive(Debug)]
    struct FailingReduce<P> {
        inner: P,
        descriptor: ProcessorDescriptor,
        block: BlockNumber,
        failures_remaining: AtomicUsize,
    }

    impl<P: Processor> FailingReduce<P> {
        fn new(inner: P, block: BlockNumber) -> Self {
            let descriptor = inner.descriptor().clone();
            Self {
                inner,
                descriptor,
                block,
                failures_remaining: AtomicUsize::new(usize::MAX),
            }
        }

        fn with_delivery_ordering(
            mut self,
            ordering: leani_processor_api::DeliveryOrdering,
        ) -> Self {
            self.descriptor.delivery_ordering = ordering;
            self
        }

        /// Reject the block only the first time, as after an operator fix.
        fn failing_once(self) -> Self {
            self.failures_remaining.store(1, Ordering::SeqCst);
            self
        }

        fn starting_at(mut self, block: BlockNumber) -> Self {
            self.descriptor.start = StartPoint::Block(block);
            self
        }
    }

    #[async_trait]
    impl<P: Processor + std::fmt::Debug + 'static> Processor for FailingReduce<P> {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn descriptor(&self) -> &ProcessorDescriptor {
            &self.descriptor
        }

        async fn map(
            &self,
            block: &leani_primitives::BlockFrame,
        ) -> Result<EncodedDelta, ProcessorError> {
            self.inner.map(block).await
        }

        async fn reduce(
            &self,
            transaction: &mut dyn leani_processor_api::ReducerTransaction,
            cursor: &ProcessorCursor,
            delta: &EncodedDelta,
        ) -> Result<leani_processor_api::DomainChanges, ProcessorError> {
            if delta.block.number == self.block
                && self
                    .failures_remaining
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                        remaining.checked_sub(1)
                    })
                    .is_ok()
            {
                return Err(ProcessorError::Invariant(
                    "injected reducer failure".to_owned(),
                ));
            }
            self.inner.reduce(transaction, cursor, delta).await
        }
    }

    /// A counter that records every block it maps and can reject one.
    #[derive(Debug)]
    struct MapProbe {
        inner: BlockLocalCounter,
        fail_at: Option<BlockNumber>,
        mapped: StdMutex<Vec<BlockNumber>>,
    }

    impl MapProbe {
        fn new(id: &str, fail_at: Option<BlockNumber>) -> Self {
            Self {
                inner: BlockLocalCounter::named(id),
                fail_at,
                mapped: StdMutex::new(Vec::new()),
            }
        }

        fn mapped(&self, block: BlockNumber) -> bool {
            self.mapped
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .contains(&block)
        }
    }

    #[async_trait]
    impl Processor for MapProbe {
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
            self.mapped
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(block.block.number);
            if self.fail_at == Some(block.block.number) {
                return Err(ProcessorError::Input("injected mapping failure".to_owned()));
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

    /// A counter whose mapping of block 0 waits briefly for a second mapper,
    /// so two unserialized drains of the same gap both reach it.
    #[derive(Debug)]
    struct RendezvousCounter {
        inner: BlockLocalCounter,
        rendezvous: tokio::sync::Barrier,
    }

    #[async_trait]
    impl Processor for RendezvousCounter {
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
            if block.block.number == BlockNumber(0) {
                let _ =
                    tokio::time::timeout(Duration::from_millis(200), self.rendezvous.wait()).await;
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

    /// A live source that pushes the request's filter down like a production
    /// source: every frame's transactions arrive filtered to the request's
    /// scope. It records each request it serves.
    #[derive(Debug)]
    struct FilteringLiveSource {
        descriptor: SourceDescriptor,
        events: Vec<ChainEvent>,
        requests: StdMutex<Vec<DataRequest>>,
    }

    impl FilteringLiveSource {
        fn new(descriptor: SourceDescriptor, events: Vec<ChainEvent>) -> Self {
            Self {
                descriptor,
                events,
                requests: StdMutex::new(Vec::new()),
            }
        }

        fn requests(&self) -> Vec<DataRequest> {
            self.requests
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }
    }

    #[async_trait]
    impl LiveSource for FilteringLiveSource {
        fn descriptor(&self) -> &SourceDescriptor {
            &self.descriptor
        }

        async fn subscribe(
            &self,
            request: DataRequest,
            _start: LiveStart,
            _budget: SourceBudget,
            _cancellation: CancellationToken,
        ) -> Result<leani_source_api::ChainEventStream, SourceError> {
            let project = |mut frame: leani_primitives::BlockFrame| {
                if request.allow_filtered {
                    frame.transactions = Material::Filtered {
                        value: Vec::new(),
                        scope: request.filters.scope.clone(),
                        completeness: leani_primitives::Completeness::VerifiedPredicate,
                    };
                }
                frame
            };
            let events = self
                .events
                .iter()
                .cloned()
                .map(|event| {
                    Ok(match event {
                        ChainEvent::Block(frame) => ChainEvent::Block(Box::new(project(*frame))),
                        ChainEvent::Reorg { reverted, applied } => ChainEvent::Reorg {
                            reverted,
                            applied: applied.into_iter().map(project).collect(),
                        },
                        other => other,
                    })
                })
                .collect::<Vec<_>>();
            self.requests
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(request);
            Ok(futures::stream::iter(events).boxed())
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

    #[tokio::test]
    async fn dropping_a_selected_job_hands_the_commit_turn_to_the_next_job() {
        let scheduler = HistoricalFairCommitScheduler::new(1_024);
        let _holder_registration = scheduler.register_job("holder").expect("register holder");
        let dropped_registration = scheduler.register_job("dropped").expect("register dropped");
        let _waiter_registration = scheduler.register_job("waiter").expect("register waiter");
        let cancellation = CancellationToken::new();
        let holder = scheduler
            .acquire("holder", &cancellation)
            .await
            .expect("holder turn");

        // "dropped" queues first, so the holder's release selects it.
        let mut dropped_turn = Box::pin(scheduler.acquire("dropped", &cancellation));
        assert!(
            futures::FutureExt::now_or_never(&mut dropped_turn).is_none(),
            "dropped job waits behind the holder"
        );
        let waiter_scheduler = scheduler.clone();
        let waiter_cancellation = cancellation.clone();
        let waiter = tokio::spawn(async move {
            waiter_scheduler
                .acquire("waiter", &waiter_cancellation)
                .await
                .map(|turn| turn.complete(1))
        });
        wait_for_fair_scheduler_waiter(&scheduler, "waiter").await;
        holder.complete(1);
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let selected = scheduler
                    .inner
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .selected
                    .clone();
                if selected.as_deref() == Some("dropped") {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the released turn selects the dropped job");

        // Its `run()` future is dropped while selected, before it claims the turn.
        drop(dropped_turn);
        drop(dropped_registration);

        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("the remaining job gets a commit turn")
            .expect("waiter task")
            .expect("waiter turn");
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

    fn narrow_filter_scope() -> leani_primitives::FilterScope {
        leani_primitives::FilterScope {
            block_range: Some(BlockRange::new(BlockNumber(1), BlockNumber(5)).expect("range")),
            addresses: vec![leani_primitives::Address::new([0x11; 20])],
            topics: vec![leani_primitives::TopicFilter {
                position: 0,
                alternatives: vec![[0x33; 32]],
            }],
            transaction_hashes: vec![leani_primitives::TransactionHash::new([0x44; 32])],
            transaction_types: vec![3],
            senders: vec![leani_primitives::Address::new([0x55; 20])],
            recipients: vec![leani_primitives::Address::new([0x66; 20])],
        }
    }

    #[test]
    fn a_wildcard_on_either_side_of_a_filter_union_stays_a_wildcard() {
        let mut retained = narrow_filter_scope();
        union_filter_scope(&mut retained, &leani_primitives::FilterScope::default());
        assert_eq!(retained, leani_primitives::FilterScope::default());

        let mut retained = leani_primitives::FilterScope::default();
        union_filter_scope(&mut retained, &narrow_filter_scope());
        assert_eq!(retained, leani_primitives::FilterScope::default());
    }

    #[test]
    fn a_filter_union_narrows_only_what_both_sides_narrow() {
        let mut retained = leani_primitives::FilterScope {
            block_range: Some(BlockRange::new(BlockNumber(1), BlockNumber(5)).expect("range")),
            addresses: vec![leani_primitives::Address::new([0x11; 20])],
            topics: vec![leani_primitives::TopicFilter {
                position: 0,
                alternatives: vec![[0x33; 32]],
            }],
            ..leani_primitives::FilterScope::default()
        };
        let incoming = leani_primitives::FilterScope {
            block_range: Some(BlockRange::new(BlockNumber(10), BlockNumber(12)).expect("range")),
            topics: vec![leani_primitives::TopicFilter {
                position: 1,
                alternatives: vec![[0x44; 32]],
            }],
            senders: vec![leani_primitives::Address::new([0x55; 20])],
            ..leani_primitives::FilterScope::default()
        };
        union_filter_scope(&mut retained, &incoming);
        assert_eq!(
            retained,
            leani_primitives::FilterScope {
                block_range: Some(BlockRange::new(BlockNumber(1), BlockNumber(12)).expect("hull")),
                ..leani_primitives::FilterScope::default()
            }
        );
    }

    #[test]
    fn a_filter_union_covers_both_sides() {
        let other = leani_primitives::FilterScope {
            block_range: Some(BlockRange::new(BlockNumber(8), BlockNumber(9)).expect("range")),
            addresses: vec![leani_primitives::Address::new([0x12; 20])],
            topics: vec![
                leani_primitives::TopicFilter {
                    position: 0,
                    alternatives: vec![[0x34; 32], [0x35; 32]],
                },
                leani_primitives::TopicFilter {
                    position: 0,
                    alternatives: vec![[0x35; 32]],
                },
            ],
            transaction_hashes: vec![leani_primitives::TransactionHash::new([0x45; 32])],
            transaction_types: vec![2],
            senders: vec![leani_primitives::Address::new([0x56; 20])],
            recipients: vec![leani_primitives::Address::new([0x67; 20])],
        };
        let pairs = [
            (narrow_filter_scope(), other.clone()),
            (other, narrow_filter_scope()),
            (narrow_filter_scope(), narrow_filter_scope()),
            (
                narrow_filter_scope(),
                leani_primitives::FilterScope::default(),
            ),
        ];
        for (left, right) in pairs {
            let mut union = left.clone();
            union_filter_scope(&mut union, &right);
            assert!(union.covers(&left), "{union:?} must cover {left:?}");
            assert!(union.covers(&right), "{union:?} must cover {right:?}");
        }
    }

    #[test]
    fn processor_requests_push_down_a_filter_that_covers_every_requirement() {
        let sender = leani_primitives::Address::new([0x55; 20]);
        let requirement = |filter| leani_processor_api::DataRequirement {
            capabilities: CapabilitySet::of(Capability::Transactions),
            log_fields: LogFieldSet::NONE,
            allow_filtered: true,
            filter,
            minimum_finality: Finality::Included,
        };
        let sender_scope = leani_primitives::FilterScope {
            senders: vec![sender],
            ..leani_primitives::FilterScope::default()
        };
        let range = BlockRange::single(BlockNumber(1));

        let single = FilteredCounter::new(vec![requirement(sender_scope.clone())]);
        let job = BackfillJob::for_processor(
            "single-filter",
            &single,
            ChainId(1),
            range,
            VerificationPolicy::CompleteCryptographic,
        )
        .expect("job");
        assert!(job.request.allow_filtered);
        assert_eq!(job.request.filters.scope, sender_scope);
        assert_eq!(job.request.filters.senders, [sender]);

        let mixed = FilteredCounter::new(vec![
            requirement(sender_scope),
            requirement(leani_primitives::FilterScope::default()),
        ]);
        let job = BackfillJob::for_processor(
            "mixed-filter",
            &mixed,
            ChainId(1),
            range,
            VerificationPolicy::CompleteCryptographic,
        )
        .expect("job");
        assert_eq!(job.request.filters, FilterSet::default());
        let live = compile_live_request(
            &[Arc::new(mixed) as Arc<dyn Processor>],
            ChainId(1),
            &LiveStart::Head,
        )
        .expect("live request");
        assert_eq!(live.filters, FilterSet::default());
    }

    async fn store_recent_transactions_filtered_to(
        store: &SqliteStore,
        range: BlockRange,
        scope: &leani_primitives::FilterScope,
    ) {
        for mut frame in frames(range) {
            frame.transactions = Material::Filtered {
                value: Vec::new(),
                scope: scope.clone(),
                completeness: leani_primitives::Completeness::VerifiedPredicate,
            };
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
    }

    fn sender_requirement(
        sender: leani_primitives::Address,
    ) -> leani_processor_api::DataRequirement {
        leani_processor_api::DataRequirement {
            capabilities: CapabilitySet::of(Capability::Transactions),
            log_fields: LogFieldSet::NONE,
            allow_filtered: true,
            filter: leani_primitives::FilterScope {
                senders: vec![sender],
                ..leani_primitives::FilterScope::default()
            },
            minimum_finality: Finality::Included,
        }
    }

    fn sender_filtered_counter(sender: leani_primitives::Address) -> FilteredCounter {
        FilteredCounter::new(vec![sender_requirement(sender)])
    }

    #[tokio::test]
    async fn historical_run_does_not_reuse_recent_material_filtered_for_another_scope() {
        let range = BlockRange::new(BlockNumber(1), BlockNumber(3)).expect("range");
        let source = Arc::new(ScriptedHistorySource::from_frames(
            fixture_source_descriptor("filtered-history", range),
            frames(range),
        ));
        let sender = leani_primitives::Address::new([0x55; 20]);
        let processor = Arc::new(sender_filtered_counter(sender));
        let (_directory, store) = store().await;
        // Retained by the live runtime for another processor's sender.
        store_recent_transactions_filtered_to(
            &store,
            range,
            &leani_primitives::FilterScope {
                senders: vec![leani_primitives::Address::new([0x77; 20])],
                ..leani_primitives::FilterScope::default()
            },
        )
        .await;
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
            "recent-filter-miss",
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
        assert_eq!(report.source_id, "filtered-history");
        assert_eq!(report.frames_committed, range.len());
        assert_eq!(report.final_coverage, vec![range]);
    }

    #[tokio::test]
    async fn historical_run_reuses_recent_material_whose_filter_covers_the_requirement() {
        let range = BlockRange::new(BlockNumber(1), BlockNumber(3)).expect("range");
        let source = Arc::new(ScriptedHistorySource::from_frames(
            fixture_source_descriptor("unused-filtered-history", range),
            frames(range),
        ));
        let sender = leani_primitives::Address::new([0x55; 20]);
        let processor = Arc::new(sender_filtered_counter(sender));
        let (_directory, store) = store().await;
        store_recent_transactions_filtered_to(
            &store,
            range,
            &leani_primitives::FilterScope {
                senders: vec![leani_primitives::Address::new([0x77; 20]), sender],
                ..leani_primitives::FilterScope::default()
            },
        )
        .await;
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
            "recent-filter-hit",
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
        assert_eq!(report.source_id, "recent-store");
        assert_eq!(report.frames_committed, range.len());
        assert_eq!(report.final_coverage, vec![range]);
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
    #[allow(clippy::too_many_lines)]
    async fn read_ahead_chunks_leave_material_memory_for_the_chunk_being_read() {
        let range = BlockRange::new(BlockNumber(1), BlockNumber(4)).expect("range");
        // Header bytes give every frame the same nonzero retained size.
        let all_frames = frames(range)
            .into_iter()
            .map(|mut frame| {
                frame.header = Material::Complete(HeaderEnvelope {
                    rlp: Some(vec![0_u8; 64]),
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
                frame
            })
            .collect::<Vec<_>>();
        let frame_bytes = all_frames
            .iter()
            .map(leani_primitives::BlockFrame::estimated_heap_bytes)
            .collect::<Vec<_>>();
        assert!(
            frame_bytes[0] > 0 && frame_bytes.iter().all(|bytes| *bytes == frame_bytes[0]),
            "frames share one nonzero size: {frame_bytes:?}"
        );
        // The first chunk starts late, so the read-ahead chunk produces first.
        let source = Arc::new(ScriptedHistorySource::new(
            fixture_source_descriptor("read-ahead-memory", range),
            vec![
                ScriptedChunk {
                    range: BlockRange::new(BlockNumber(1), BlockNumber(2)).expect("first range"),
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
                    range: BlockRange::new(BlockNumber(3), BlockNumber(4)).expect("second range"),
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
                // Room for two frames less one byte: a frame the read-ahead
                // chunk buffers leaves no room for the chunk being read.
                memory_bytes: frame_bytes[0] * 2 - 1,
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
                mapper_concurrency: 1,
                ..HistoricalRuntimeConfig::default()
            },
        )
        .expect("runtime")
        .with_pipeline_budget(pipeline_budget)
        .with_material_coordinator(coordinator.clone());
        let job = BackfillJob::for_processor(
            "read-ahead-memory",
            processor.as_ref(),
            ChainId(1),
            range,
            VerificationPolicy::CompleteCryptographic,
        )
        .expect("job");
        let mut budget = default_source_budget();
        budget.max_in_flight_requests = 2;

        let report = tokio::time::timeout(
            Duration::from_secs(5),
            runtime.run(job, budget, CancellationToken::new()),
        )
        .await
        .unwrap_or_else(|_| {
            panic!(
                "read-ahead material starved the chunk being read after {} source opens: {:?}",
                source.open_calls(),
                coordinator.snapshot()
            )
        })
        .expect("backfill");

        assert_eq!(report.frames_committed, range.len());
        assert_eq!(source.open_calls(), 2);
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

    /// Runs a recent-reuse backfill of blocks `0..=9` and returns how many
    /// chunks it committed them in.
    async fn recent_gap_chunks(config: HistoricalRuntimeConfig) -> u64 {
        let range = BlockRange::new(BlockNumber(0), BlockNumber(9)).expect("range");
        let chain = e2e_chain(9)
            .into_iter()
            .map(|mut frame| {
                frame.provenance.push(leani_primitives::Provenance {
                    source_id: leani_primitives::SourceId::new("live-cache").expect("source ID"),
                    source_kind: SourceKind::ExecutionP2p,
                    trust: TrustModel::ProtocolVerified,
                    range: Some(range),
                    object: None,
                    observed_at_unix_ms: 1,
                    projection: Vec::new(),
                });
                frame
            })
            .collect::<Vec<_>>();
        let source = Arc::new(ScriptedHistorySource::from_frames(
            e2e_descriptor("unused-batched-history", range),
            chain.clone(),
        ));
        let processor = Arc::new(BlockLocalCounter::named("recent-batch-counter"));
        let (_directory, store) = store().await;
        for frame in &chain {
            store.store_recent_frame(frame).await.expect("recent frame");
        }
        let runtime = HistoricalRuntime::new(store.clone(), source.clone(), processor, config)
            .expect("runtime");
        let job = BackfillJob::for_processor(
            "recent-batches",
            runtime.processor.as_ref(),
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
        assert_eq!(report.source_id, "recent-store");
        assert_eq!(report.frames_committed, 10);
        assert_eq!(report.final_coverage, vec![range]);
        decode_checkpoint(
            &store
                .job("recent-batches")
                .await
                .expect("job")
                .expect("job record")
                .checkpoint
                .expect("checkpoint"),
        )
        .expect("decode checkpoint")
        .chunks_completed
    }

    #[tokio::test]
    async fn recent_gap_reuse_streams_bounded_batches() {
        // A batch holds at most one commit's worth of blocks...
        assert_eq!(
            recent_gap_chunks(HistoricalRuntimeConfig {
                mapper_concurrency: 2,
                commit_maximum_blocks: 4,
                ..HistoricalRuntimeConfig::default()
            })
            .await,
            3,
            "ten blocks stream as 4 + 4 + 2"
        );
        // ...and at most the pipeline's mapped-byte budget of frames.
        let frame = e2e_chain(0).remove(0);
        let mut measured = frame.clone();
        measured.provenance.push(leani_primitives::Provenance {
            source_id: leani_primitives::SourceId::new("live-cache").expect("source ID"),
            source_kind: SourceKind::ExecutionP2p,
            trust: TrustModel::ProtocolVerified,
            range: Some(BlockRange::new(BlockNumber(0), BlockNumber(9)).expect("range")),
            object: None,
            observed_at_unix_ms: 1,
            projection: Vec::new(),
        });
        let frame_bytes = encoded_recent_frame_bytes(&measured).await;
        assert_eq!(
            recent_gap_chunks(HistoricalRuntimeConfig {
                mapper_concurrency: 2,
                maximum_mapped_bytes: frame_bytes * 4,
                ..HistoricalRuntimeConfig::default()
            })
            .await,
            3,
            "ten blocks of four frames' budget stream as 4 + 4 + 2"
        );
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

    /// Plans like `inner`, but every frame stream it opens panics when first
    /// polled, as a buggy source adapter would.
    #[derive(Debug)]
    struct PanickingHistorySource {
        inner: ScriptedHistorySource,
    }

    #[async_trait]
    impl HistorySource for PanickingHistorySource {
        fn descriptor(&self) -> &SourceDescriptor {
            self.inner.descriptor()
        }

        async fn plan(
            &self,
            request: &DataRequest,
        ) -> Result<leani_source_api::SourcePlan, SourceError> {
            self.inner.plan(request).await
        }

        async fn open(
            &self,
            _chunk: &SourceChunk,
            _budget: SourceBudget,
            _cancellation: CancellationToken,
        ) -> Result<leani_source_api::BlockFrameStream, SourceError> {
            Ok(futures::stream::poll_fn(
                |_| -> std::task::Poll<Option<Result<leani_primitives::BlockFrame, SourceError>>> {
                    panic!("injected history source panic")
                },
            )
            .boxed())
        }
    }

    #[tokio::test]
    async fn a_panicking_history_source_fails_its_job_instead_of_hanging_it() {
        let range = BlockRange::new(BlockNumber(1), BlockNumber(2)).expect("range");
        let source = Arc::new(PanickingHistorySource {
            inner: ScriptedHistorySource::from_frames(
                fixture_source_descriptor("panicking-history", range),
                frames(range),
            ),
        });
        let coordinator =
            HistoricalMaterialCoordinator::new(HistoricalMaterialCoordinatorConfig::default())
                .expect("coordinator");
        let processor = Arc::new(BlockLocalCounter::default());
        let (_directory, store) = store().await;
        let runtime = HistoricalRuntime::new(
            store.clone(),
            source,
            processor.clone(),
            HistoricalRuntimeConfig {
                mapper_concurrency: 1,
                max_attempts: 1,
                ..HistoricalRuntimeConfig::default()
            },
        )
        .expect("runtime")
        .with_material_coordinator(coordinator.clone());
        let job = BackfillJob::for_processor(
            "panicking-history",
            processor.as_ref(),
            ChainId(1),
            range,
            VerificationPolicy::CompleteCryptographic,
        )
        .expect("job");

        let error = tokio::time::timeout(
            Duration::from_secs(5),
            runtime.run(job, default_source_budget(), CancellationToken::new()),
        )
        .await
        .expect("a panicking source must fail its job, not hang it")
        .expect_err("the job fails");

        assert!(
            matches!(
                &error,
                RuntimeError::Source(SourceError::Unavailable(message))
                    if message.contains("panicked")
            ),
            "unexpected error: {error:?}"
        );
        assert_eq!(
            store
                .job("panicking-history")
                .await
                .expect("job")
                .expect("durable job")
                .state,
            JobState::Failed
        );
        assert_eq!(coordinator.snapshot().active_acquisitions, 0);
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
    async fn recompute_microbatch_reuses_recent_frames_promoted_after_retention() {
        let range = BlockRange::new(BlockNumber(1), BlockNumber(3)).expect("range");
        let processor = Arc::new(
            BlockLocalCounter::default()
                .with_split_delivery()
                .with_output_none(),
        );
        let (_directory, store) = store().await;
        let config = HistoricalRuntimeConfig {
            mapper_concurrency: 2,
            ..HistoricalRuntimeConfig::default()
        };
        let seed_job = externalized_subscription_job(
            &store,
            processor.as_ref(),
            "promoted-recent-seed",
            range,
            BackfillMode::FillMissing,
            0,
        )
        .await;
        HistoricalRuntime::new(
            store.clone(),
            Arc::new(ScriptedHistorySource::from_frames(
                fixture_source_descriptor("promoted-recent-seed-source", range),
                frames(range),
            )),
            processor.clone(),
            config.clone(),
        )
        .expect("seed runtime")
        .run(seed_job, default_source_budget(), CancellationToken::new())
        .await
        .expect("seed exact coverage");

        // The live lane retained these frames while they were only included;
        // finality promoted them afterwards.
        let mut tip = None;
        for mut frame in frames(range) {
            frame.finality = Finality::Included;
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
                .expect("retain included frame");
            tip = Some(frame.block);
        }
        let tip = tip.expect("recent tip");
        store
            .mark_recent_finalized(ChainId(1), tip.number, tip.hash)
            .await
            .expect("promote retained frames");

        let recompute_job = externalized_subscription_job(
            &store,
            processor.as_ref(),
            "promoted-recent-recompute",
            range,
            BackfillMode::Recompute,
            1,
        )
        .await;
        let unused_source = Arc::new(ScriptedHistorySource::from_frames(
            fixture_source_descriptor("promoted-recent-unused-source", range),
            frames(range),
        ));
        let report = HistoricalRuntime::new(
            store.clone(),
            unused_source.clone(),
            processor.clone(),
            config,
        )
        .expect("recompute runtime")
        .run(
            recompute_job,
            default_source_budget(),
            CancellationToken::new(),
        )
        .await
        .expect("recompute over promoted recent frames");
        assert_eq!(unused_source.open_calls(), 0);
        assert_eq!(report.source_id, "recent-store");
        assert_eq!(report.frames_committed, range.len());
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
        // Each pause at the work-ahead limit releases the source stream, and
        // the job reopens the rest of the range once it can commit again.
        assert!(resumed_source.open_calls() >= 1);
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

    async fn wait_for_subscription_state(
        store: &SqliteStore,
        job_id: &str,
        state: leani_store_sqlite::BackfillSubscriptionState,
    ) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let current = store
                    .backfill_subscription_for_job(job_id)
                    .await
                    .expect("subscription")
                    .expect("durable subscription")
                    .state;
                if current == state {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("subscription {job_id} never reached {state:?}"));
    }

    /// Acknowledge a history stream at every progress boundary, as a consumer
    /// that keeps up does, and return the completion marker's sequence.
    async fn acknowledge_history_until_complete(
        store: SqliteStore,
        descriptor: ProcessorDescriptor,
        stream_id: String,
        consumer_id: &'static str,
    ) -> u64 {
        let mut after = 0;
        loop {
            let changes = store
                .changes_in_stream(&descriptor, &stream_id, ChainId(1), after, 100)
                .await
                .expect("consume history changes");
            if let Some(boundary) = changes.iter().rev().find(|record| {
                matches!(
                    record.change.kind.as_str(),
                    "system.backfill_progress" | "system.backfill_complete"
                )
            }) {
                after = boundary.cursor.sequence;
                store
                    .acknowledge_consumer_in_stream(&descriptor, &stream_id, consumer_id, after)
                    .await
                    .expect("acknowledge history boundary");
                if boundary.change.kind == "system.backfill_complete" {
                    return after;
                }
            } else {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
    }

    /// Assert that a resumed subscription delivered every block exactly once.
    async fn assert_subscription_delivered_once(
        store: &SqliteStore,
        processor: &dyn Processor,
        job_id: &str,
        stream_id: &str,
        range: BlockRange,
        completion: u64,
    ) {
        let changes = store
            .changes_in_stream(processor.descriptor(), stream_id, ChainId(1), 0, 100)
            .await
            .expect("history stream");
        assert_eq!(
            changes
                .iter()
                .filter(|record| record.change.kind == "synthetic.counter")
                .map(|record| record.block.number.0)
                .collect::<Vec<_>>(),
            range.iter().map(|number| number.0).collect::<Vec<_>>()
        );
        assert_eq!(
            changes
                .iter()
                .filter(|record| record.change.kind == "system.backfill_complete")
                .count(),
            1
        );
        let subscription = store
            .backfill_subscription_for_job(job_id)
            .await
            .expect("subscription")
            .expect("durable subscription");
        assert_eq!(
            subscription.state,
            leani_store_sqlite::BackfillSubscriptionState::Draining
        );
        assert_eq!(subscription.processed_work_blocks, range.len());
        assert_eq!(subscription.completion_sequence, Some(completion));
    }

    /// A subscription whose consumer stops acknowledging pauses at its
    /// work-ahead limit while it holds every node-wide chunk slot: the chunk
    /// it reads and, when coordinated, the chunk it opened ahead. The pause
    /// must free every slot, another job must get one, and the subscription
    /// must resume and finish once its consumer acknowledges.
    #[allow(clippy::too_many_lines)]
    async fn backpressured_job_releases_the_chunk_slot(coordinated: bool, microbatched: bool) {
        let paused_range = BlockRange::new(BlockNumber(1), BlockNumber(6)).expect("paused range");
        let other_range = BlockRange::new(BlockNumber(1), BlockNumber(3)).expect("other range");
        let slots = if coordinated { 2 } else { 1 };
        let pipeline_budget =
            HistoricalPipelineBudget::new(slots, 2, 4 * 1_024 * 1_024).expect("pipeline budget");
        let coordinator = coordinated.then(|| {
            HistoricalMaterialCoordinator::new_with_pipeline_budget(
                HistoricalMaterialCoordinatorConfig {
                    memory_bytes: 4 * 1_024 * 1_024,
                    maximum_buffered_frames_per_acquisition: 1,
                    ..HistoricalMaterialCoordinatorConfig::default()
                },
                &pipeline_budget,
            )
            .expect("coordinator")
        });
        let with_budgets = |runtime: HistoricalRuntime| {
            let runtime = runtime.with_pipeline_budget(pipeline_budget.clone());
            match &coordinator {
                Some(coordinator) => runtime.with_material_coordinator(coordinator.clone()),
                None => runtime,
            }
        };
        let config = HistoricalRuntimeConfig {
            mapper_concurrency: 1,
            commit_maximum_blocks: 1,
            commit_maximum_delay: Duration::from_mins(1),
            ..HistoricalRuntimeConfig::default()
        };
        let (_directory, store) = store().await;

        // Retained output makes a subscription commit block by block.
        let paused_processor = BlockLocalCounter::named("paused-subscriber").with_split_delivery();
        let paused_processor = Arc::new(if microbatched {
            paused_processor.with_output_none()
        } else {
            paused_processor
        });
        let paused_job = externalized_subscription_job_with_limits(
            &store,
            paused_processor.as_ref(),
            "paused-subscription",
            paused_range,
            BackfillMode::FillMissing,
            0,
            2,
            64 * 1024 * 1024,
        )
        .await;
        let stream_id = paused_job
            .delivery_stream_id
            .clone()
            .expect("history delivery stream");
        let paused_frames = frames(paused_range);
        let paused_source = Arc::new(ScriptedHistorySource::new(
            fixture_source_descriptor("paused-history", paused_range),
            [(1, 4), (5, 6)]
                .into_iter()
                .map(|(start, end)| ScriptedChunk {
                    range: BlockRange::new(BlockNumber(start), BlockNumber(end))
                        .expect("chunk range"),
                    schema_version: "fixture-v1".to_owned(),
                    estimated_bytes: None,
                    steps: paused_frames
                        .iter()
                        .filter(|frame| (start..=end).contains(&frame.block.number.0))
                        .cloned()
                        .map(|frame| HistoryStep::Frame(Box::new(frame)))
                        .collect(),
                })
                .collect(),
        ));
        let paused_runtime = with_budgets(
            HistoricalRuntime::new(
                store.clone(),
                paused_source.clone(),
                paused_processor.clone(),
                config.clone(),
            )
            .expect("paused runtime"),
        );
        let mut paused_budget = default_source_budget();
        paused_budget.max_in_flight_requests = slots;
        let paused = tokio::spawn(async move {
            paused_runtime
                .run(paused_job, paused_budget, CancellationToken::new())
                .await
        });
        wait_for_subscription_state(
            &store,
            "paused-subscription",
            leani_store_sqlite::BackfillSubscriptionState::Backpressured,
        )
        .await;
        tokio::time::timeout(Duration::from_secs(5), async {
            while pipeline_budget.active_chunks.available_permits() < slots {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("the paused job frees every chunk slot it holds");

        let other_processor =
            Arc::new(BlockLocalCounter::named("independent-job").with_delivery_none());
        let other_runtime = with_budgets(
            HistoricalRuntime::new(
                store.clone(),
                Arc::new(ScriptedHistorySource::from_frames(
                    fixture_source_descriptor("independent-history", other_range),
                    frames(other_range),
                )),
                other_processor.clone(),
                config,
            )
            .expect("independent runtime"),
        );
        let other_job = BackfillJob::for_processor(
            "independent-job",
            other_processor.as_ref(),
            ChainId(1),
            other_range,
            VerificationPolicy::CompleteCryptographic,
        )
        .expect("independent job");
        let other = tokio::time::timeout(
            Duration::from_secs(5),
            other_runtime.run(other_job, default_source_budget(), CancellationToken::new()),
        )
        .await
        .expect("another job gets the chunk slot while the subscription is paused")
        .expect("independent backfill");
        assert_eq!(other.frames_committed, other_range.len());

        let consumer = tokio::spawn(acknowledge_history_until_complete(
            store.clone(),
            paused_processor.descriptor().clone(),
            stream_id.clone(),
            "destination-0",
        ));
        let resumed = tokio::time::timeout(Duration::from_secs(10), paused)
            .await
            .expect("the paused job resumes once its consumer acknowledges")
            .expect("paused task")
            .expect("paused backfill");
        let completion = tokio::time::timeout(Duration::from_secs(5), consumer)
            .await
            .expect("consumer sees the completion marker")
            .expect("consumer task");
        assert_eq!(resumed.frames_committed, paused_range.len());
        assert!(
            paused_source.open_calls() > 1,
            "the paused job re-reads the frames it dropped when it paused"
        );
        assert_subscription_delivered_once(
            &store,
            paused_processor.as_ref(),
            "paused-subscription",
            &stream_id,
            paused_range,
            completion,
        )
        .await;
    }

    #[tokio::test]
    async fn a_backpressured_job_releases_its_coordinated_chunk_slots() {
        backpressured_job_releases_the_chunk_slot(true, true).await;
    }

    #[tokio::test]
    async fn a_backpressured_job_releases_its_direct_chunk_slot() {
        backpressured_job_releases_the_chunk_slot(false, true).await;
    }

    #[tokio::test]
    async fn a_backpressured_block_by_block_job_releases_its_chunk_slots() {
        backpressured_job_releases_the_chunk_slot(true, false).await;
    }

    /// A subscription that commits eight blocks at once pauses at its
    /// eight-block work-ahead limit with the rest of a split microbatch still
    /// mapped. While it waits it must hold at most the one mapped frame it
    /// retries, so another job can map, and it must still deliver every
    /// block once.
    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn a_backpressured_microbatch_keeps_one_mapped_frame_while_paused() {
        let paused_range = BlockRange::new(BlockNumber(1), BlockNumber(16)).expect("paused range");
        let other_range = BlockRange::new(BlockNumber(1), BlockNumber(3)).expect("other range");
        let paused_processor = Arc::new(
            BlockLocalCounter::named("paused-batcher")
                .with_split_delivery()
                .with_output_none(),
        );
        let other_processor =
            Arc::new(BlockLocalCounter::named("other-batcher").with_delivery_none());
        let paused_frames = frames(paused_range);
        let mut frame_bytes = Vec::with_capacity(paused_frames.len());
        for frame in &paused_frames {
            let (delta, checksums) = map_with_finality_variants(paused_processor.as_ref(), frame)
                .await
                .expect("map frame");
            frame_bytes.push(mapped_delta_bytes(&delta, &checksums));
        }
        let largest_frame = frame_bytes.iter().copied().max().expect("frames");
        // Room for exactly one microbatch of eight mapped frames.
        let budget_bytes = largest_frame * 8;
        let pipeline_budget =
            HistoricalPipelineBudget::new(2, 2, budget_bytes).expect("pipeline budget");
        let held_bytes = || {
            budget_bytes.saturating_sub(
                u64::try_from(pipeline_budget.mapped_bytes.available_permits())
                    .expect("available mapped bytes"),
            )
        };
        let config = HistoricalRuntimeConfig {
            mapper_concurrency: 1,
            commit_maximum_blocks: 8,
            commit_maximum_delay: Duration::from_mins(1),
            ..HistoricalRuntimeConfig::default()
        };
        let (_directory, store) = store().await;

        let paused_job = externalized_subscription_job_with_limits(
            &store,
            paused_processor.as_ref(),
            "paused-batcher",
            paused_range,
            BackfillMode::FillMissing,
            0,
            8,
            64 * 1024 * 1024,
        )
        .await;
        let stream_id = paused_job
            .delivery_stream_id
            .clone()
            .expect("history delivery stream");
        let paused_runtime = HistoricalRuntime::new(
            store.clone(),
            Arc::new(ScriptedHistorySource::from_frames(
                fixture_source_descriptor("paused-batch-history", paused_range),
                paused_frames,
            )),
            paused_processor.clone(),
            config.clone(),
        )
        .expect("paused runtime")
        .with_pipeline_budget(pipeline_budget.clone());
        let paused = tokio::spawn(async move {
            paused_runtime
                .run(
                    paused_job,
                    default_source_budget(),
                    CancellationToken::new(),
                )
                .await
        });
        // Blocks 1-8 commit. Blocks 9-16 are refused and split down to
        // block 9, with blocks 10-16 still mapped behind it.
        wait_for_subscription_state(
            &store,
            "paused-batcher",
            leani_store_sqlite::BackfillSubscriptionState::Backpressured,
        )
        .await;
        tokio::time::timeout(Duration::from_secs(5), async {
            while held_bytes() > largest_frame {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "the paused job holds {} mapped bytes; one frame is {largest_frame}",
                held_bytes()
            )
        });

        // Another job maps and commits its whole microbatch meanwhile.
        let other_runtime = HistoricalRuntime::new(
            store.clone(),
            Arc::new(ScriptedHistorySource::from_frames(
                fixture_source_descriptor("other-batch-history", other_range),
                frames(other_range),
            )),
            other_processor.clone(),
            config,
        )
        .expect("other runtime")
        .with_pipeline_budget(pipeline_budget.clone());
        let other_job = BackfillJob::for_processor(
            "other-batcher",
            other_processor.as_ref(),
            ChainId(1),
            other_range,
            VerificationPolicy::CompleteCryptographic,
        )
        .expect("other job");
        let other = tokio::time::timeout(
            Duration::from_secs(5),
            other_runtime.run(other_job, default_source_budget(), CancellationToken::new()),
        )
        .await
        .expect("another job maps while the subscription is paused")
        .expect("other backfill");
        assert_eq!(other.frames_committed, other_range.len());

        let consumer = tokio::spawn(acknowledge_history_until_complete(
            store.clone(),
            paused_processor.descriptor().clone(),
            stream_id.clone(),
            "destination-0",
        ));
        let resumed = tokio::time::timeout(Duration::from_secs(10), paused)
            .await
            .expect("the paused job resumes once its consumer acknowledges")
            .expect("paused task")
            .expect("paused backfill");
        let completion = tokio::time::timeout(Duration::from_secs(5), consumer)
            .await
            .expect("consumer sees the completion marker")
            .expect("consumer task");
        assert_eq!(resumed.frames_committed, paused_range.len());
        assert_subscription_delivered_once(
            &store,
            paused_processor.as_ref(),
            "paused-batcher",
            &stream_id,
            paused_range,
            completion,
        )
        .await;
        assert_eq!(held_bytes(), 0, "no mapped bytes stay reserved");
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn a_backpressured_job_detaches_from_an_acquisition_it_shares() {
        let range = BlockRange::new(BlockNumber(1), BlockNumber(6)).expect("range");
        let source = Arc::new(ScriptedHistorySource::from_frames(
            fixture_source_descriptor("shared-lagging-history", range),
            frames(range),
        ));
        let pipeline_budget =
            HistoricalPipelineBudget::new(2, 2, 4 * 1_024 * 1_024).expect("pipeline budget");
        let coordinator = HistoricalMaterialCoordinator::new_with_pipeline_budget(
            HistoricalMaterialCoordinatorConfig {
                memory_bytes: 4 * 1_024 * 1_024,
                maximum_buffered_frames_per_acquisition: 1,
                ..HistoricalMaterialCoordinatorConfig::default()
            },
            &pipeline_budget,
        )
        .expect("coordinator");
        // Both jobs register before either reads, so they share one acquisition.
        let mut permits = coordinator.startup_batch(2);
        let steady_permit = permits.pop().expect("steady permit");
        let lagging_permit = permits.pop().expect("lagging permit");
        let config = HistoricalRuntimeConfig {
            mapper_concurrency: 1,
            commit_maximum_blocks: 1,
            commit_maximum_delay: Duration::from_mins(1),
            ..HistoricalRuntimeConfig::default()
        };
        let (_directory, store) = store().await;

        let lagging_processor = Arc::new(
            BlockLocalCounter::named("lagging-subscriber")
                .with_split_delivery()
                .with_output_none(),
        );
        let lagging_job = externalized_subscription_job_with_limits(
            &store,
            lagging_processor.as_ref(),
            "lagging-subscription",
            range,
            BackfillMode::FillMissing,
            0,
            2,
            64 * 1024 * 1024,
        )
        .await;
        let stream_id = lagging_job
            .delivery_stream_id
            .clone()
            .expect("history delivery stream");
        let lagging_runtime = HistoricalRuntime::new(
            store.clone(),
            source.clone(),
            lagging_processor.clone(),
            config.clone(),
        )
        .expect("lagging runtime")
        .with_pipeline_budget(pipeline_budget.clone())
        .with_material_coordinator(coordinator.clone())
        .with_material_startup_permit(lagging_permit);
        let lagging = tokio::spawn(async move {
            lagging_runtime
                .run(
                    lagging_job,
                    default_source_budget(),
                    CancellationToken::new(),
                )
                .await
        });

        let steady_processor =
            Arc::new(BlockLocalCounter::named("steady-job").with_delivery_none());
        let steady_runtime = HistoricalRuntime::new(
            store.clone(),
            source.clone(),
            steady_processor.clone(),
            config,
        )
        .expect("steady runtime")
        .with_pipeline_budget(pipeline_budget)
        .with_material_coordinator(coordinator.clone())
        .with_material_startup_permit(steady_permit);
        let steady_job = BackfillJob::for_processor(
            "steady-job",
            steady_processor.as_ref(),
            ChainId(1),
            range,
            VerificationPolicy::CompleteCryptographic,
        )
        .expect("steady job");
        let steady = tokio::time::timeout(
            Duration::from_secs(5),
            steady_runtime.run(
                steady_job,
                default_source_budget(),
                CancellationToken::new(),
            ),
        )
        .await
        .unwrap_or_else(|_| {
            panic!(
                "the shared acquisition stalled behind the paused subscriber: {:?}",
                coordinator.snapshot()
            )
        })
        .expect("steady backfill");
        assert_eq!(steady.frames_committed, range.len());
        assert_eq!(
            coordinator.snapshot().requests_coalesced,
            1,
            "both jobs read one shared acquisition"
        );
        assert_eq!(
            store
                .backfill_subscription_for_job("lagging-subscription")
                .await
                .expect("subscription")
                .expect("durable subscription")
                .state,
            leani_store_sqlite::BackfillSubscriptionState::Backpressured,
            "the steady job finished while the other subscriber was paused"
        );

        let consumer = tokio::spawn(acknowledge_history_until_complete(
            store.clone(),
            lagging_processor.descriptor().clone(),
            stream_id.clone(),
            "destination-0",
        ));
        let resumed = tokio::time::timeout(Duration::from_secs(10), lagging)
            .await
            .expect("the paused job resumes once its consumer acknowledges")
            .expect("lagging task")
            .expect("lagging backfill");
        let completion = tokio::time::timeout(Duration::from_secs(5), consumer)
            .await
            .expect("consumer sees the completion marker")
            .expect("consumer task");
        assert_eq!(resumed.frames_committed, range.len());
        assert_subscription_delivered_once(
            &store,
            lagging_processor.as_ref(),
            "lagging-subscription",
            &stream_id,
            range,
            completion,
        )
        .await;
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

    /// Serves `inner`, except that the first open of a chunk starting at one
    /// of `failing_starts` fails with a transient outage.
    #[derive(Debug)]
    struct FlakyHistorySource {
        inner: ScriptedHistorySource,
        failing_starts: StdMutex<BTreeSet<BlockNumber>>,
    }

    impl FlakyHistorySource {
        fn new(
            inner: ScriptedHistorySource,
            failing_starts: impl IntoIterator<Item = BlockNumber>,
        ) -> Self {
            Self {
                inner,
                failing_starts: StdMutex::new(failing_starts.into_iter().collect()),
            }
        }
    }

    #[async_trait]
    impl HistorySource for FlakyHistorySource {
        fn descriptor(&self) -> &SourceDescriptor {
            self.inner.descriptor()
        }

        async fn plan(
            &self,
            request: &DataRequest,
        ) -> Result<leani_source_api::SourcePlan, SourceError> {
            self.inner.plan(request).await
        }

        async fn open(
            &self,
            chunk: &SourceChunk,
            budget: SourceBudget,
            cancellation: CancellationToken,
        ) -> Result<leani_source_api::BlockFrameStream, SourceError> {
            if self
                .failing_starts
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&chunk.range.start())
            {
                return Err(SourceError::Unavailable(format!(
                    "injected transient outage at block {}",
                    chunk.range.start().0
                )));
            }
            self.inner.open(chunk, budget, cancellation).await
        }
    }

    /// Two-block ranges with one unrequested block between them.
    fn spaced_ranges(count: u64) -> Vec<BlockRange> {
        (0..count)
            .map(|index| {
                let start = 1 + index * 3;
                BlockRange::new(BlockNumber(start), BlockNumber(start + 1)).expect("gap range")
            })
            .collect()
    }

    #[tokio::test]
    async fn scattered_transient_errors_retry_their_gap_without_failing_a_long_job() {
        let ranges = spaced_ranges(10);
        let bounding = BlockRange::new(ranges[0].start(), ranges[9].end()).expect("bounding");
        let source = Arc::new(FlakyHistorySource::new(
            ScriptedHistorySource::from_frames(
                fixture_source_descriptor("scattered-outages", bounding),
                frames(bounding),
            ),
            [ranges[1].start(), ranges[4].start(), ranges[7].start()],
        ));
        let processor = Arc::new(BlockLocalCounter::default());
        let (_directory, store) = store().await;
        let runtime = HistoricalRuntime::new(
            store,
            source,
            processor.clone(),
            HistoricalRuntimeConfig {
                mapper_concurrency: 1,
                max_attempts: 3,
                retry_base: Duration::from_millis(1),
                retry_max: Duration::from_millis(1),
                ..HistoricalRuntimeConfig::default()
            },
        )
        .expect("runtime");
        let job = BackfillJob::for_processor_ranges(
            "scattered-outages",
            processor.as_ref(),
            ChainId(1),
            ranges.clone(),
            VerificationPolicy::CompleteCryptographic,
        )
        .expect("job");

        let report = runtime
            .run(job, default_source_budget(), CancellationToken::new())
            .await
            .expect("three transient errors across ten gaps must not fail the job");

        assert_eq!(report.final_coverage, ranges);
        assert_eq!(report.frames_committed, 20);
        assert_eq!(report.source_attempts, 13);
        assert_eq!(report.sources[0].failures, 3);
    }

    #[tokio::test]
    async fn healthy_gaps_use_the_highest_priority_source_and_fail_over_only_on_error() {
        let ranges = spaced_ranges(4);
        let bounding = BlockRange::new(ranges[0].start(), ranges[3].end()).expect("bounding");
        let mut primary_descriptor = fixture_source_descriptor("primary-history", bounding);
        primary_descriptor.priority = 0;
        let primary: Arc<dyn HistorySource> = Arc::new(FlakyHistorySource::new(
            ScriptedHistorySource::from_frames(primary_descriptor, frames(bounding)),
            [ranges[1].start()],
        ));
        let mut fallback_descriptor = fixture_source_descriptor("fallback-history", bounding);
        fallback_descriptor.priority = 1;
        let fallback: Arc<dyn HistorySource> = Arc::new(ScriptedHistorySource::from_frames(
            fallback_descriptor,
            frames(bounding),
        ));
        let processor = Arc::new(BlockLocalCounter::default());
        let (_directory, store) = store().await;
        let runtime = HistoricalRuntime::new_with_sources(
            store,
            vec![fallback, primary],
            processor.clone(),
            HistoricalRuntimeConfig {
                mapper_concurrency: 1,
                max_attempts: 3,
                retry_base: Duration::from_millis(1),
                retry_max: Duration::from_millis(1),
                ..HistoricalRuntimeConfig::default()
            },
        )
        .expect("runtime");
        let job = BackfillJob::for_processor_ranges(
            "priority-sources",
            processor.as_ref(),
            ChainId(1),
            ranges.clone(),
            VerificationPolicy::CompleteCryptographic,
        )
        .expect("job");

        let report = runtime
            .run(job, default_source_budget(), CancellationToken::new())
            .await
            .expect("backfill");

        let usage = |id: &str| {
            report
                .sources
                .iter()
                .find(|source| source.source_id == id)
                .map(|source| (source.attempts, source.failures))
        };
        assert_eq!(
            usage("primary-history"),
            Some((4, 1)),
            "every gap starts on the highest-priority source"
        );
        assert_eq!(
            usage("fallback-history"),
            Some((1, 0)),
            "only the gap whose primary read failed falls over"
        );
        assert_eq!(report.final_coverage, ranges);
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
            e2e_descriptor("shared-live", range),
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
                block_number: replacement.block.number,
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
        // The store failed the lane itself; the park still records where a
        // reset replays from.
        assert_eq!(
            store
                .live_lane_gap(blocked.descriptor())
                .await
                .expect("gap")
                .map(|gap| gap.first_unapplied),
            Some(first.block)
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

    const REPLAY_SENDER: leani_primitives::Address = leani_primitives::Address::new([0x55; 20]);
    const OTHER_SENDER: leani_primitives::Address = leani_primitives::Address::new([0x77; 20]);

    /// Blocks 0..=2 with complete material, as history recovery returns them.
    fn live_chain(finality: Finality) -> Vec<leani_primitives::BlockFrame> {
        let mut parent = BlockHash::ZERO;
        (0..=2)
            .map(|number| {
                let mut frame = live_fixture(number, parent);
                frame.finality = finality;
                parent = frame.block.hash;
                frame
            })
            .collect()
    }

    /// Retains `chain` as a live lane compiled for another sender's processor
    /// would: transactions filtered to `OTHER_SENDER`.
    async fn retain_for_other_sender(store: &SqliteStore, chain: &[leani_primitives::BlockFrame]) {
        for frame in chain {
            let mut retained = frame.clone();
            retained.transactions = Material::Filtered {
                value: Vec::new(),
                scope: leani_primitives::FilterScope {
                    senders: vec![OTHER_SENDER],
                    ..leani_primitives::FilterScope::default()
                },
                completeness: leani_primitives::Completeness::VerifiedPredicate,
            };
            store
                .store_recent_frame(&retained)
                .await
                .expect("retain frame");
        }
    }

    async fn park_at_first_block(
        store: &SqliteStore,
        processor: &dyn Processor,
        chain: &[leani_primitives::BlockFrame],
    ) {
        store
            .park_processor_live_lane_at(
                processor.descriptor(),
                chain[0].block,
                "delivery_spool_hard_limit",
                false,
            )
            .await
            .expect("park lane");
    }

    async fn assert_parked_at(
        store: &SqliteStore,
        processor: &dyn Processor,
        first_unapplied: leani_primitives::BlockRef,
        state: ProcessorRunState,
        reason: &str,
    ) {
        let runtime_state = store
            .processor_runtime_state(processor.descriptor())
            .await
            .expect("state");
        assert_eq!(runtime_state.state, state);
        assert_eq!(runtime_state.reason.as_deref(), Some(reason));
        assert_eq!(
            store
                .live_lane_gap(processor.descriptor())
                .await
                .expect("gap")
                .expect("a parked lane keeps its gap")
                .first_unapplied,
            first_unapplied
        );
    }

    /// Blocks `0..=through` as a live source delivers them.
    fn live_blocks(through: u64) -> Vec<leani_primitives::BlockFrame> {
        let mut parent = BlockHash::ZERO;
        (0..=through)
            .map(|number| {
                let frame = live_fixture(number, parent);
                parent = frame.block.hash;
                frame
            })
            .collect()
    }

    fn block_events(frames: &[leani_primitives::BlockFrame]) -> Vec<LiveStep> {
        frames
            .iter()
            .cloned()
            .map(|frame| LiveStep::Event(ChainEvent::Block(Box::new(frame))))
            .collect()
    }

    async fn assert_gap_recovered(store: &SqliteStore, processor: &dyn Processor) {
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
    }

    #[tokio::test]
    async fn block_local_gap_replay_recovers_blocks_retained_without_its_filtered_material() {
        let range = BlockRange::new(BlockNumber(0), BlockNumber(2)).expect("range");
        let chain = live_chain(Finality::Finalized);
        let processor = Arc::new(Requiring::new(
            BlockLocalCounter::named("filtered-gap-counter")
                .with_split_delivery()
                .with_output_none(),
            vec![sender_requirement(REPLAY_SENDER)],
        ));
        let (_directory, store) = store().await;
        retain_for_other_sender(&store, &chain).await;
        park_at_first_block(&store, processor.as_ref(), &chain).await;

        let report = SharedLiveRuntime::new(
            store.clone(),
            Arc::new(ScriptedLiveSource::new(
                fixture_source_descriptor("filtered-gap-live", range),
                Vec::new(),
            )),
            vec![processor.clone()],
            SharedLiveRuntimeConfig::default(),
        )
        .expect("runtime")
        .with_finalized_gap_recovery(Arc::new(StaticLiveGapRecovery { frames: chain }))
        .reconcile_pending()
        .await
        .expect("a retained frame the processor cannot read is a miss, not a lane failure");

        assert_eq!(report.processors["filtered-gap-counter"].applied, 3);
        assert_gap_recovered(&store, processor.as_ref()).await;
    }

    #[tokio::test]
    async fn ordered_gap_replay_recovers_blocks_retained_without_its_filtered_material() {
        let range = BlockRange::new(BlockNumber(0), BlockNumber(2)).expect("range");
        let chain = live_chain(Finality::Finalized);
        let processor = Arc::new(Requiring::new(
            OrderedLedgerProcessor::named("filtered-gap-ledger"),
            vec![sender_requirement(REPLAY_SENDER)],
        ));
        let (_directory, store) = store().await;
        retain_for_other_sender(&store, &chain).await;
        park_at_first_block(&store, processor.as_ref(), &chain).await;

        let report = SharedLiveRuntime::new(
            store.clone(),
            Arc::new(ScriptedLiveSource::new(
                fixture_source_descriptor("filtered-gap-ledger-live", range),
                Vec::new(),
            )),
            vec![processor.clone()],
            SharedLiveRuntimeConfig::default(),
        )
        .expect("runtime")
        .with_finalized_gap_recovery(Arc::new(StaticLiveGapRecovery { frames: chain }))
        .reconcile_pending()
        .await
        .expect("ordered replay recovers instead of failing");

        assert_eq!(report.processors["filtered-gap-ledger"].applied, 3);
        assert_gap_recovered(&store, processor.as_ref()).await;
    }

    #[tokio::test]
    async fn unfinalized_gap_replay_pauses_until_finality_then_recovers_from_history() {
        let range = BlockRange::new(BlockNumber(0), BlockNumber(4)).expect("range");
        let chain = live_chain(Finality::Included);
        let fourth = live_fixture(3, chain[2].block.hash);
        let fifth = live_fixture(4, fourth.block.hash);
        let block_local = Arc::new(Requiring::new(
            BlockLocalCounter::named("unfinalized-gap-counter")
                .with_split_delivery()
                .with_output_none(),
            vec![sender_requirement(REPLAY_SENDER)],
        ));
        let ordered = Arc::new(Requiring::new(
            OrderedLedgerProcessor::named("unfinalized-gap-ledger"),
            vec![sender_requirement(REPLAY_SENDER)],
        ));
        let healthy = Arc::new(
            BlockLocalCounter::named("unfinalized-gap-healthy")
                .with_split_delivery()
                .with_output_none(),
        );
        let (_directory, store) = store().await;
        retain_for_other_sender(&store, &chain).await;
        park_at_first_block(&store, block_local.as_ref(), &chain).await;
        park_at_first_block(&store, ordered.as_ref(), &chain).await;

        let runtime = SharedLiveRuntime::new(
            store.clone(),
            Arc::new(ScriptedLiveSource::new(
                fixture_source_descriptor("unfinalized-gap-live", range),
                vec![fourth, fifth]
                    .into_iter()
                    .map(|frame| LiveStep::Event(ChainEvent::Block(Box::new(frame))))
                    .collect(),
            )),
            vec![block_local.clone(), ordered.clone(), healthy.clone()],
            SharedLiveRuntimeConfig::default(),
        )
        .expect("runtime")
        .with_finalized_gap_recovery(Arc::new(StaticLiveGapRecovery {
            frames: live_chain(Finality::Finalized),
        }));
        let report = runtime
            .run(
                LiveStart::Head,
                default_source_budget(),
                CancellationToken::new(),
            )
            .await
            .expect("an unreadable unfinalized gap pauses its processor, not the live lane");

        assert_eq!(report.chain_blocks, 2);
        assert_eq!(report.processors["unfinalized-gap-healthy"].applied, 2);
        for processor in [block_local.as_ref() as &dyn Processor, ordered.as_ref()] {
            assert_parked_at(
                &store,
                processor,
                chain[0].block,
                ProcessorRunState::Paused,
                "unfinalized_gap_waiting_for_finality",
            )
            .await;
        }

        // Finality reaches the gap, so history can serve it: the lanes heal
        // on the next drain without an operator.
        store
            .mark_recent_finalized(ChainId(1), chain[2].block.number, chain[2].block.hash)
            .await
            .expect("finalize the gap");
        let recovered = runtime
            .reconcile_pending()
            .await
            .expect("a finalized gap recovers from history");
        for processor in [block_local.as_ref() as &dyn Processor, ordered.as_ref()] {
            assert_eq!(
                recovered.processors[processor.descriptor().id.as_str()].applied,
                5
            );
            assert_gap_recovered(&store, processor).await;
        }
    }

    #[tokio::test]
    async fn block_local_gap_replay_isolates_a_mapping_failure_instead_of_failing_the_lane() {
        let range = BlockRange::new(BlockNumber(0), BlockNumber(2)).expect("range");
        let chain = live_chain(Finality::Included);
        let processor = Arc::new(FailOnceCounter::named("replay-mapping-failure"));
        let (_directory, store) = store().await;
        for frame in &chain {
            store.store_recent_frame(frame).await.expect("retain frame");
        }
        park_at_first_block(&store, processor.as_ref(), &chain).await;

        SharedLiveRuntime::new(
            store.clone(),
            Arc::new(ScriptedLiveSource::new(
                fixture_source_descriptor("replay-mapping-live", range),
                Vec::new(),
            )),
            vec![processor.clone()],
            SharedLiveRuntimeConfig::default(),
        )
        .expect("runtime")
        .reconcile_pending()
        .await
        .expect("a processor mapping failure isolates its lane, not the live lane");

        let state = store
            .processor_runtime_state(processor.descriptor())
            .await
            .expect("state");
        assert_eq!(state.state, ProcessorRunState::Failed);
        assert_eq!(
            state.reason.as_deref(),
            Some("processor_live_mapping_failed")
        );
    }

    #[tokio::test]
    async fn reducer_failure_parks_only_its_processor_while_shared_ingestion_continues() {
        let chain = live_blocks(3);
        let range = BlockRange::new(BlockNumber(0), BlockNumber(3)).expect("range");
        let block_local = Arc::new(FailingReduce::new(
            BlockLocalCounter::named("reduce-failure-counter"),
            BlockNumber(1),
        ));
        let ordered = Arc::new(FailingReduce::new(
            OrderedLedgerProcessor::named("reduce-failure-ledger"),
            BlockNumber(1),
        ));
        let healthy = Arc::new(BlockLocalCounter::named("reduce-failure-healthy"));
        let (_directory, store) = store().await;

        let report = SharedLiveRuntime::new(
            store.clone(),
            Arc::new(ScriptedLiveSource::new(
                e2e_descriptor("reduce-failure-live", range),
                block_events(&chain),
            )),
            vec![block_local.clone(), ordered.clone(), healthy.clone()],
            SharedLiveRuntimeConfig::default(),
        )
        .expect("runtime")
        .run(
            LiveStart::Head,
            default_source_budget(),
            CancellationToken::new(),
        )
        .await
        .expect("one processor's reducer failure must not stop shared ingestion");

        assert_eq!(report.chain_blocks, 4);
        assert_eq!(report.processors["reduce-failure-healthy"].applied, 4);
        for processor in [block_local.as_ref() as &dyn Processor, ordered.as_ref()] {
            assert_eq!(
                report.processors[processor.descriptor().id.as_str()].applied,
                1
            );
            assert_parked_at(
                &store,
                processor,
                chain[1].block,
                ProcessorRunState::Failed,
                "processor_live_reduce_failed",
            )
            .await;
        }
    }

    #[tokio::test]
    async fn a_canonical_block_local_reducer_failure_parks_only_its_lane() {
        let chain = live_blocks(2);
        let range = BlockRange::new(BlockNumber(0), BlockNumber(2)).expect("range");
        // Canonical delivery ordering is the SDK default for block-local
        // processors. The failing lane comes first, so a lane-wide failure
        // would also stop the healthy commit of the same block.
        let failing = Arc::new(
            FailingReduce::new(
                BlockLocalCounter::named("canonical-reduce-failure"),
                BlockNumber(1),
            )
            .with_delivery_ordering(leani_processor_api::DeliveryOrdering::Canonical),
        );
        let healthy = Arc::new(BlockLocalCounter::named("canonical-reduce-healthy"));
        let (_directory, store) = store().await;

        let report = SharedLiveRuntime::new(
            store.clone(),
            Arc::new(ScriptedLiveSource::new(
                fixture_source_descriptor("canonical-reduce-live", range),
                block_events(&chain),
            )),
            vec![failing.clone(), healthy.clone()],
            SharedLiveRuntimeConfig::default(),
        )
        .expect("runtime")
        .run(
            LiveStart::Head,
            default_source_budget(),
            CancellationToken::new(),
        )
        .await
        .expect("a canonical block-local reducer failure must not stop shared ingestion");

        assert_eq!(report.chain_blocks, 3);
        assert_eq!(report.processors["canonical-reduce-healthy"].applied, 3);
        assert_eq!(report.processors["canonical-reduce-failure"].applied, 1);
        assert_parked_at(
            &store,
            failing.as_ref(),
            chain[1].block,
            ProcessorRunState::Failed,
            "processor_live_reduce_failed",
        )
        .await;
    }

    #[tokio::test]
    async fn a_conflicting_pending_delta_fails_only_its_lane() {
        let chain = live_blocks(1);
        let range = BlockRange::new(BlockNumber(0), BlockNumber(1)).expect("range");
        let conflicted = Arc::new(OrderedLedgerProcessor::named("conflicting-ledger"));
        let healthy = Arc::new(BlockLocalCounter::named("conflict-healthy"));
        let (_directory, store) = store().await;
        // Block 0 is applied, but a pending delta for the same block carries
        // other content, and no retained frame shows it is a finality variant.
        apply_live_frame(&store, conflicted.as_ref(), &chain[0], 1).await;
        store
            .persist_delta(
                conflicted.descriptor(),
                &EncodedDelta::new(
                    conflicted.descriptor(),
                    ChainId(1),
                    chain[0].block,
                    vec![0xee; 32],
                ),
            )
            .await
            .expect("persist conflicting delta");
        let runtime = SharedLiveRuntime::new(
            store.clone(),
            Arc::new(ScriptedLiveSource::new(
                e2e_descriptor("conflict-live", range),
                block_events(&chain[1..]),
            )),
            vec![conflicted.clone(), healthy.clone()],
            SharedLiveRuntimeConfig::default(),
        )
        .expect("runtime");

        runtime
            .reconcile_pending()
            .await
            .expect("a conflicting pending delta fails its own lane, not the live lane");
        assert_parked_at(
            &store,
            conflicted.as_ref(),
            chain[0].block,
            ProcessorRunState::Failed,
            "processor_live_delta_conflict",
        )
        .await;
        let report = runtime
            .run(
                LiveStart::Head,
                default_source_budget(),
                CancellationToken::new(),
            )
            .await
            .expect("live run");
        assert_eq!(report.processors["conflict-healthy"].applied, 1);
        assert_eq!(report.processors["conflicting-ledger"].applied, 0);
    }

    #[tokio::test]
    async fn a_lane_parked_below_a_seeded_anchor_walks_past_it_once_history_passes() {
        let chain = live_blocks(9);
        let ordered = Arc::new(OrderedLedgerProcessor::named("below-anchor-ledger"));
        let runtime = |store: &SqliteStore| {
            SharedLiveRuntime::new(
                store.clone(),
                Arc::new(ScriptedLiveSource::new(
                    fixture_source_descriptor(
                        "below-anchor-live",
                        BlockRange::new(BlockNumber(0), BlockNumber(9)).expect("range"),
                    ),
                    Vec::new(),
                )),
                vec![ordered.clone()],
                SharedLiveRuntimeConfig::default(),
            )
            .expect("runtime")
        };
        let (_directory, store) = store().await;
        store
            .register_processor(ordered.descriptor())
            .await
            .expect("register");
        // The node seeds its finalized anchor, block 7, with no parent hash
        // and no frame, while retained frames end at block 4.
        store
            .store_canonical_anchor(
                ChainId(1),
                leani_primitives::BlockRef {
                    parent_hash: BlockHash::ZERO,
                    timestamp: 1,
                    ..chain[7].block
                },
                Finality::Finalized,
            )
            .await
            .expect("seed anchor");
        for frame in &chain[..=4] {
            store.store_recent_frame(frame).await.expect("retain frame");
        }
        let live = runtime(&store);
        assert!(
            live.park_processor_lane(ordered.descriptor(), "hot_cold_handoff_failed")
                .await
                .expect("park")
        );
        for frame in chain[5..].iter().filter(|frame| frame.block.number.0 != 7) {
            store.store_recent_frame(frame).await.expect("retain frame");
        }

        // History passes the parked block and the anchor.
        for (sequence, frame) in (1..).zip(&chain) {
            apply_live_frame(&store, ordered.as_ref(), frame, sequence).await;
        }
        live.reconcile_pending()
            .await
            .expect("the gap walks across the seeded anchor");
        assert_gap_recovered(&store, ordered.as_ref()).await;
        runtime(&store)
            .reconcile_pending()
            .await
            .expect("a restart reconciles cleanly");
    }

    #[tokio::test]
    async fn a_failed_canonical_delivery_lane_resets_through_the_api_and_resumes() {
        let chain = live_blocks(2);
        let range = BlockRange::new(BlockNumber(0), BlockNumber(2)).expect("range");
        // Ordered processors deliver in canonical order, so they have no
        // split live stream.
        let ordered = Arc::new(
            FailingReduce::new(
                OrderedLedgerProcessor::named("api-reset-ledger"),
                BlockNumber(1),
            )
            .failing_once(),
        );
        let (_directory, store) = store().await;
        let runtime = SharedLiveRuntime::new(
            store.clone(),
            Arc::new(ScriptedLiveSource::new(
                e2e_descriptor("api-reset-live", range),
                block_events(&chain),
            )),
            vec![ordered.clone()],
            SharedLiveRuntimeConfig::default(),
        )
        .expect("runtime");
        runtime
            .run(
                LiveStart::Head,
                default_source_budget(),
                CancellationToken::new(),
            )
            .await
            .expect("live run");
        assert_parked_at(
            &store,
            ordered.as_ref(),
            chain[1].block,
            ProcessorRunState::Failed,
            "processor_live_reduce_failed",
        )
        .await;

        let api = leani_api::router_with_processors(
            store.clone(),
            vec![ordered.clone() as Arc<dyn Processor>],
            Vec::new(),
            leani_api::ApiConfig::default(),
        )
        .expect("API router");
        let response = api
            .oneshot(
                Request::post("/admin/v1/processors/api-reset-ledger/lanes/live/reset")
                    .header("x-leani-request", "1")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("API response");
        let status = response.status();
        let body = to_bytes(response.into_body(), 1024 * 1024)
            .await
            .expect("API body");
        assert_eq!(
            status,
            StatusCode::OK,
            "reset refused: {}",
            String::from_utf8_lossy(&body)
        );

        runtime
            .reconcile_pending()
            .await
            .expect("the reset lane replays");
        assert_gap_recovered(&store, ordered.as_ref()).await;
        assert_eq!(
            store
                .processor_cursor(ordered.descriptor())
                .await
                .expect("cursor")
                .expect("replayed")
                .block_hash,
            chain[2].block.hash
        );
    }

    #[tokio::test]
    async fn a_lane_failed_outside_the_live_lane_keeps_its_failure_and_is_not_remapped() {
        let chain = live_blocks(6);
        let range = BlockRange::new(BlockNumber(0), BlockNumber(6)).expect("range");
        let mapped = Arc::new(MapProbe::new("externally-failed-mapped", None));
        let failing = Arc::new(MapProbe::new(
            "externally-failed-failing",
            Some(BlockNumber(5)),
        ));
        let healthy = Arc::new(BlockLocalCounter::named("externally-failed-healthy"));
        let (_directory, store) = store().await;
        let mut steps = block_events(&chain[..=4]);
        steps.push(LiveStep::Delay(Duration::from_secs(1)));
        steps.extend(block_events(&chain[5..]));
        let runtime = SharedLiveRuntime::new(
            store.clone(),
            Arc::new(ScriptedLiveSource::new(
                fixture_source_descriptor("externally-failed-live", range),
                steps,
            )),
            vec![mapped.clone(), failing.clone(), healthy.clone()],
            SharedLiveRuntimeConfig::default(),
        )
        .expect("runtime");
        let live = tokio::spawn(async move {
            runtime
                .run(
                    LiveStart::Head,
                    default_source_budget(),
                    CancellationToken::new(),
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(10), async {
            while store
                .processor_cursor(healthy.descriptor())
                .await
                .expect("cursor")
                .is_none_or(|cursor| cursor.block_number != BlockNumber(4))
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("block 4 committed");
        // Finality fails both lanes on a coverage contradiction while the
        // live lane is between blocks; it records no gap marker.
        for processor in [mapped.as_ref(), failing.as_ref()] {
            store
                .fail_processor_live_lane(processor.descriptor(), "processor_finality_conflict")
                .await
                .expect("fail lane");
        }
        live.await.expect("live task").expect("live run");

        for processor in [mapped.as_ref(), failing.as_ref()] {
            let state = store
                .processor_runtime_state(processor.descriptor())
                .await
                .expect("state");
            assert_eq!(state.state, ProcessorRunState::Failed);
            assert_eq!(
                state.reason.as_deref(),
                Some("processor_finality_conflict"),
                "{} keeps its first failure",
                processor.descriptor().id
            );
            assert!(
                store
                    .live_lane_gap(processor.descriptor())
                    .await
                    .expect("gap")
                    .is_none(),
                "no late marker is planted for {}",
                processor.descriptor().id
            );
        }
        // The first block after the failure is mapped before its commit is
        // skipped; later blocks are not mapped at all.
        assert!(!mapped.mapped(BlockNumber(6)));
        assert!(!failing.mapped(BlockNumber(6)));
        assert_eq!(
            store
                .processor_cursor(healthy.descriptor())
                .await
                .expect("cursor")
                .expect("healthy")
                .block_number,
            BlockNumber(6)
        );
    }

    /// Replaces blocks from `from` up with a branch of `length` blocks.
    fn replacement_branch(
        chain: &[leani_primitives::BlockFrame],
        from: usize,
        length: usize,
    ) -> Vec<leani_primitives::BlockFrame> {
        let mut parent = chain[from - 1].block.hash;
        (from..from + length)
            .map(|number| {
                let mut frame = live_fixture(u64::try_from(number).expect("height"), parent);
                frame.block.hash =
                    BlockHash::new([0xc0 | u8::try_from(number).expect("small height"); 32]);
                parent = frame.block.hash;
                frame
            })
            .collect()
    }

    /// Retains `chain`, applies it to `healthy`, and applies `applied` of it
    /// to `parked`, which is then paused with its gap at `gap`.
    async fn gapped_lane_before_reorg(
        store: &SqliteStore,
        chain: &[leani_primitives::BlockFrame],
        healthy: &dyn Processor,
        parked: &dyn Processor,
        applied: usize,
        gap: usize,
    ) {
        for (sequence, frame) in (1..).zip(chain) {
            store.store_recent_frame(frame).await.expect("retain frame");
            apply_live_frame(store, healthy, frame, sequence).await;
        }
        for (sequence, frame) in (1..).zip(&chain[..applied]) {
            apply_live_frame(store, parked, frame, sequence).await;
        }
        store
            .park_processor_live_lane_at(
                parked.descriptor(),
                chain[gap].block,
                "delivery_spool_hard_limit",
                false,
            )
            .await
            .expect("park lane");
    }

    async fn run_reorg(
        store: &SqliteStore,
        processors: Vec<Arc<dyn Processor>>,
        reverted: Vec<leani_primitives::BlockRef>,
        applied: Vec<leani_primitives::BlockFrame>,
    ) {
        let range = BlockRange::new(BlockNumber(0), BlockNumber(9)).expect("range");
        let report = SharedLiveRuntime::new(
            store.clone(),
            Arc::new(ScriptedLiveSource::new(
                e2e_descriptor("gapped-reorg-live", range),
                vec![LiveStep::Event(ChainEvent::Reorg { reverted, applied })],
            )),
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
        .expect("live run");
        assert_eq!(report.reorgs, 1);
    }

    #[tokio::test]
    async fn an_ordered_gapped_lane_resumes_after_a_reorg_below_its_marker() {
        let chain = live_blocks(3);
        let replacement = replacement_branch(&chain, 2, 2);
        let ordered = Arc::new(OrderedLedgerProcessor::named("deep-reorg-ledger"));
        let healthy = Arc::new(BlockLocalCounter::named("deep-reorg-healthy"));
        let (_directory, store) = store().await;
        // Applied through block 2, parked at block 3; the reorg replaces
        // blocks 2 and 3, so it also undoes block 2 for this lane.
        gapped_lane_before_reorg(&store, &chain, healthy.as_ref(), ordered.as_ref(), 3, 3).await;

        run_reorg(
            &store,
            vec![ordered.clone(), healthy.clone()],
            vec![chain[3].block, chain[2].block],
            replacement.clone(),
        )
        .await;

        assert_gap_recovered(&store, ordered.as_ref()).await;
        assert_eq!(
            store
                .processor_cursor(ordered.descriptor())
                .await
                .expect("cursor")
                .expect("resumed")
                .block_hash,
            replacement[1].block.hash
        );
    }

    #[tokio::test]
    async fn a_block_local_gapped_lane_applies_a_shortening_reorg_without_a_hole() {
        let chain = live_blocks(6);
        let replacement = replacement_branch(&chain, 4, 1);
        let block_local = Arc::new(BlockLocalCounter::named("shortening-reorg-counter"));
        let healthy = Arc::new(BlockLocalCounter::named("shortening-reorg-healthy"));
        let (_directory, store) = store().await;
        // Applied through block 4, parked at block 5; the new branch ends at
        // block 4, below the gap.
        gapped_lane_before_reorg(&store, &chain, healthy.as_ref(), block_local.as_ref(), 5, 5)
            .await;

        run_reorg(
            &store,
            vec![block_local.clone(), healthy.clone()],
            vec![chain[6].block, chain[5].block, chain[4].block],
            replacement.clone(),
        )
        .await;

        assert_gap_recovered(&store, block_local.as_ref()).await;
        assert_eq!(
            store
                .coverage_block_by_hash(block_local.descriptor(), replacement[0].block.hash)
                .await
                .expect("coverage"),
            Some(BlockNumber(4)),
            "the replacement block below the gap is applied"
        );
    }

    async fn rewind_probe_run(
        store: &SqliteStore,
        processors: Vec<Arc<dyn Processor>>,
        steps: Vec<LiveStep>,
    ) {
        let range = BlockRange::new(BlockNumber(0), BlockNumber(9)).expect("range");
        SharedLiveRuntime::new(
            store.clone(),
            Arc::new(ScriptedLiveSource::new(
                e2e_descriptor("rewind-probe-live", range),
                steps,
            )),
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
        .expect("live run");
    }

    async fn rewind_probe_describe(store: &SqliteStore, processor: &dyn Processor) -> String {
        let state = store
            .processor_runtime_state(processor.descriptor())
            .await
            .expect("state");
        let gap = store
            .live_lane_gap(processor.descriptor())
            .await
            .expect("gap");
        let cursor = store
            .processor_cursor(processor.descriptor())
            .await
            .expect("cursor");
        format!(
            "state={:?} reason={:?} gap={:?} cursor={:?}",
            state.state,
            state.reason,
            gap.map(|gap| (gap.first_unapplied.number.0, gap.first_unapplied.hash)),
            cursor.map(|cursor| (cursor.block_number.0, cursor.block_hash)),
        )
    }

    // Block-local lane applied through 3, parked at 4; the source rewinds to
    // block 2 (reverted [5, 4, 3], no replacement branch), then delivers 3'.
    #[tokio::test]
    async fn rewind_probe_block_local_gap_above_lowest_reverted() {
        let chain = live_blocks(5);
        let replacement = replacement_branch(&chain, 3, 1);
        let lane = Arc::new(BlockLocalCounter::named("rewind-probe-block-local"));
        let healthy = Arc::new(BlockLocalCounter::named("rewind-probe-healthy-a"));
        let (_directory, store) = store().await;
        gapped_lane_before_reorg(&store, &chain, healthy.as_ref(), lane.as_ref(), 4, 4).await;
        rewind_probe_run(
            &store,
            vec![lane.clone(), healthy.clone()],
            vec![
                LiveStep::Event(ChainEvent::Reorg {
                    reverted: vec![chain[5].block, chain[4].block, chain[3].block],
                    applied: Vec::new(),
                }),
                LiveStep::Event(ChainEvent::Block(Box::new(replacement[0].clone()))),
            ],
        )
        .await;
        let described = rewind_probe_describe(&store, lane.as_ref()).await;
        let lane_covers = store
            .coverage_block_by_hash(lane.descriptor(), replacement[0].block.hash)
            .await
            .expect("coverage");
        let healthy_covers = store
            .coverage_block_by_hash(healthy.descriptor(), replacement[0].block.hash)
            .await
            .expect("coverage");
        println!(
            "PROBE-A block-local gap above lowest reverted: {described}; lane covers 3': {lane_covers:?}; healthy covers 3': {healthy_covers:?}"
        );
        assert_eq!(healthy_covers, Some(BlockNumber(3)));
        assert_eq!(
            lane_covers,
            Some(BlockNumber(3)),
            "PROBE-A lane skipped 3': {described}"
        );
    }

    // Ordered lane applied through 3, parked at 4; the source rewinds to
    // block 2, then delivers 3' and 4'.
    #[tokio::test]
    async fn rewind_probe_ordered_gap_above_lowest_reverted() {
        let chain = live_blocks(5);
        let replacement = replacement_branch(&chain, 3, 2);
        let lane = Arc::new(OrderedLedgerProcessor::named("rewind-probe-ordered"));
        let healthy = Arc::new(BlockLocalCounter::named("rewind-probe-healthy-b"));
        let (_directory, store) = store().await;
        gapped_lane_before_reorg(&store, &chain, healthy.as_ref(), lane.as_ref(), 4, 4).await;
        rewind_probe_run(
            &store,
            vec![lane.clone(), healthy.clone()],
            vec![
                LiveStep::Event(ChainEvent::Reorg {
                    reverted: vec![chain[5].block, chain[4].block, chain[3].block],
                    applied: Vec::new(),
                }),
                LiveStep::Event(ChainEvent::Block(Box::new(replacement[0].clone()))),
                LiveStep::Event(ChainEvent::Block(Box::new(replacement[1].clone()))),
            ],
        )
        .await;
        let described = rewind_probe_describe(&store, lane.as_ref()).await;
        println!("PROBE-B ordered gap above lowest reverted: {described}");
        assert!(
            store
                .live_lane_gap(lane.descriptor())
                .await
                .expect("gap")
                .is_none(),
            "PROBE-B ordered lane stalls: {described}"
        );
    }

    // Block-local lane applied through 3, parked at 4; the source rewinds to
    // block 3 (reverted [4], no replacement branch), then delivers 4'.
    #[tokio::test]
    async fn rewind_probe_block_local_gap_at_lowest_reverted() {
        let chain = live_blocks(4);
        let replacement = replacement_branch(&chain, 4, 1);
        let lane = Arc::new(BlockLocalCounter::named("rewind-probe-block-local-at"));
        let healthy = Arc::new(BlockLocalCounter::named("rewind-probe-healthy-c"));
        let (_directory, store) = store().await;
        gapped_lane_before_reorg(&store, &chain, healthy.as_ref(), lane.as_ref(), 4, 4).await;
        rewind_probe_run(
            &store,
            vec![lane.clone(), healthy.clone()],
            vec![
                LiveStep::Event(ChainEvent::Reorg {
                    reverted: vec![chain[4].block],
                    applied: Vec::new(),
                }),
                LiveStep::Event(ChainEvent::Block(Box::new(replacement[0].clone()))),
            ],
        )
        .await;
        let described = rewind_probe_describe(&store, lane.as_ref()).await;
        let lane_covers = store
            .coverage_block_by_hash(lane.descriptor(), replacement[0].block.hash)
            .await
            .expect("coverage");
        println!(
            "PROBE-C block-local gap at lowest reverted: {described}; lane covers 4': {lane_covers:?}"
        );
        assert_eq!(
            lane_covers,
            Some(BlockNumber(4)),
            "PROBE-C lane did not apply 4': {described}"
        );
    }

    #[tokio::test]
    async fn gapped_lanes_rewound_without_a_replacement_branch_stay_on_the_chain() {
        let chain = live_blocks(5);
        let replacement = replacement_branch(&chain, 3, 1);
        let lane = Arc::new(BlockLocalCounter::named("rewind-failed-lane"));
        let paused = Arc::new(BlockLocalCounter::named("rewind-paused-lane"));
        let healthy = Arc::new(BlockLocalCounter::named("rewind-failed-healthy"));
        let (_directory, store) = store().await;
        gapped_lane_before_reorg(&store, &chain, healthy.as_ref(), lane.as_ref(), 4, 4).await;
        store
            .fail_processor_live_lane(lane.descriptor(), "processor_live_reduce_failed")
            .await
            .expect("fail lane");
        for (sequence, frame) in (1..).zip(&chain[..4]) {
            apply_live_frame(&store, paused.as_ref(), frame, sequence).await;
        }
        store
            .park_processor_live_lane_at(
                paused.descriptor(),
                chain[4].block,
                "delivery_spool_hard_limit",
                false,
            )
            .await
            .expect("park lane");
        let processors: Vec<Arc<dyn Processor>> =
            vec![lane.clone(), paused.clone(), healthy.clone()];
        rewind_probe_run(
            &store,
            processors.clone(),
            vec![LiveStep::Event(ChainEvent::Reorg {
                reverted: vec![chain[5].block, chain[4].block, chain[3].block],
                applied: Vec::new(),
            })],
        )
        .await;

        // Block 3 was undone, so both lanes have applied through the new tip,
        // block 2. The paused lane's gap completes at once. The failed lane's
        // gap points at that retained canonical block until its successor
        // arrives.
        assert_gap_recovered(&store, paused.as_ref()).await;
        assert_parked_at(
            &store,
            lane.as_ref(),
            chain[2].block,
            ProcessorRunState::Failed,
            "processor_live_reduce_failed",
        )
        .await;

        // Once the successor is retained, the marker moves onto it, the
        // first block the lane has not applied, and a reset replays it.
        rewind_probe_run(
            &store,
            processors.clone(),
            vec![LiveStep::Event(ChainEvent::Block(Box::new(
                replacement[0].clone(),
            )))],
        )
        .await;
        assert_eq!(
            store
                .coverage_block_by_hash(paused.descriptor(), replacement[0].block.hash)
                .await
                .expect("coverage"),
            Some(BlockNumber(3))
        );
        assert_parked_at(
            &store,
            lane.as_ref(),
            replacement[0].block,
            ProcessorRunState::Failed,
            "processor_live_reduce_failed",
        )
        .await;
        store
            .reset_failed_live_lane(lane.descriptor())
            .await
            .expect("reset");
        reduce_failure_runtime(&store, &processors, &[])
            .reconcile_pending()
            .await
            .expect("replay after reset");
        assert_gap_recovered(&store, lane.as_ref()).await;
        assert_eq!(
            store
                .coverage_block_by_hash(lane.descriptor(), replacement[0].block.hash)
                .await
                .expect("coverage"),
            Some(BlockNumber(3))
        );
    }

    #[tokio::test]
    async fn a_rewind_without_a_replacement_branch_settles_a_paused_lane_at_once() {
        let chain = live_blocks(5);
        let range = BlockRange::new(BlockNumber(0), BlockNumber(9)).expect("range");
        let paused = Arc::new(BlockLocalCounter::named("rewind-settle-paused"));
        let healthy = Arc::new(BlockLocalCounter::named("rewind-settle-healthy"));
        let (_directory, store) = store().await;
        gapped_lane_before_reorg(&store, &chain, healthy.as_ref(), paused.as_ref(), 4, 4).await;
        let runtime = SharedLiveRuntime::new(
            store.clone(),
            Arc::new(ScriptedLiveSource::new(
                fixture_source_descriptor("rewind-settle-live", range),
                vec![
                    LiveStep::Event(ChainEvent::Reorg {
                        reverted: vec![chain[5].block, chain[4].block, chain[3].block],
                        applied: Vec::new(),
                    }),
                    // No later block arrives meanwhile.
                    LiveStep::Delay(Duration::from_secs(30)),
                ],
            )),
            vec![paused.clone(), healthy.clone()],
            SharedLiveRuntimeConfig::default(),
        )
        .expect("runtime");
        let cancellation = CancellationToken::new();
        let live = tokio::spawn({
            let cancellation = cancellation.clone();
            async move {
                runtime
                    .run(LiveStart::Head, default_source_budget(), cancellation)
                    .await
            }
        });

        tokio::time::timeout(Duration::from_secs(10), async {
            while store
                .live_lane_gap(paused.descriptor())
                .await
                .expect("gap")
                .is_some()
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the paused lane settles on the new tip before any later block");
        cancellation.cancel();
        assert_cancelled_live(live.await.expect("live task"));
        assert_gap_recovered(&store, paused.as_ref()).await;
    }

    // Ordered lane that starts at block 3, applied block 3 and parked at
    // block 4; the source rewinds to block 2, below the lane's start, then
    // delivers 3' and 4'.
    #[tokio::test]
    async fn a_gapped_lane_whose_whole_history_is_reverted_resumes_at_its_start() {
        let chain = live_blocks(5);
        let replacement = replacement_branch(&chain, 3, 2);
        let lane = Arc::new(
            FailingReduce::new(
                OrderedLedgerProcessor::named("rewind-start-ledger"),
                BlockNumber(u64::MAX),
            )
            .starting_at(BlockNumber(3)),
        );
        let healthy = Arc::new(BlockLocalCounter::named("rewind-start-healthy"));
        let (_directory, store) = store().await;
        for (sequence, frame) in (1..).zip(&chain) {
            store.store_recent_frame(frame).await.expect("retain frame");
            apply_live_frame(&store, healthy.as_ref(), frame, sequence).await;
        }
        apply_live_frame(&store, lane.as_ref(), &chain[3], 1).await;
        store
            .park_processor_live_lane_at(
                lane.descriptor(),
                chain[4].block,
                "delivery_spool_hard_limit",
                false,
            )
            .await
            .expect("park lane");

        rewind_probe_run(
            &store,
            vec![lane.clone(), healthy.clone()],
            vec![
                LiveStep::Event(ChainEvent::Reorg {
                    reverted: vec![chain[5].block, chain[4].block, chain[3].block],
                    applied: Vec::new(),
                }),
                LiveStep::Event(ChainEvent::Block(Box::new(replacement[0].clone()))),
                LiveStep::Event(ChainEvent::Block(Box::new(replacement[1].clone()))),
            ],
        )
        .await;

        let described = rewind_probe_describe(&store, lane.as_ref()).await;
        for frame in &replacement {
            assert_eq!(
                store
                    .coverage_block_by_hash(lane.descriptor(), frame.block.hash)
                    .await
                    .expect("coverage"),
                Some(frame.block.number),
                "{described}"
            );
        }
        assert_gap_recovered(&store, lane.as_ref()).await;
    }

    #[tokio::test]
    async fn a_gap_marker_off_the_canonical_chain_is_re_pointed_instead_of_completed() {
        let chain = live_blocks(3);
        // Markers left off the canonical chain, as an earlier version could:
        // above the canonical tip, and on a replaced block at block 3.
        let above_tip = Arc::new(BlockLocalCounter::named("orphan-above-tip"));
        let replaced = Arc::new(BlockLocalCounter::named("orphan-replaced"));
        let ordered = Arc::new(OrderedLedgerProcessor::named("orphan-ordered-above-tip"));
        let (_directory, store) = store().await;
        for frame in &chain {
            store.store_recent_frame(frame).await.expect("retain frame");
        }
        let mut replaced_block = chain[3].block;
        replaced_block.hash = BlockHash::new([0xe3; 32]);
        let orphan_above_tip = live_fixture(5, BlockHash::new([0x44; 32])).block;
        let orphans: [(&dyn Processor, BlockRef); 3] = [
            (above_tip.as_ref(), orphan_above_tip),
            (replaced.as_ref(), replaced_block),
            (ordered.as_ref(), orphan_above_tip),
        ];
        for (lane, orphan) in orphans {
            for (sequence, frame) in (1..).zip(&chain[..=2]) {
                apply_live_frame(&store, lane, frame, sequence).await;
            }
            store
                .park_processor_live_lane_at(
                    lane.descriptor(),
                    orphan,
                    "delivery_spool_hard_limit",
                    false,
                )
                .await
                .expect("park lane");
        }
        let lanes: Vec<Arc<dyn Processor>> =
            vec![above_tip.clone(), replaced.clone(), ordered.clone()];
        let runtime = reduce_failure_runtime(&store, &lanes, &[]);

        // Finalized-gap recovery never completes an orphaned marker either: it
        // re-points it at the last canonical block the lane applied.
        assert!(
            runtime
                .recover_finalized_live_gap(
                    above_tip.as_ref(),
                    orphan_above_tip,
                    &mut SharedLiveReport::default(),
                )
                .await
                .expect("re-point")
        );
        assert_eq!(
            store
                .live_lane_gap(above_tip.descriptor())
                .await
                .expect("gap")
                .map(|gap| gap.first_unapplied),
            Some(chain[2].block)
        );
        runtime
            .reconcile_pending()
            .await
            .expect("an orphaned marker is re-pointed, not a lane failure");

        for (lane, _) in orphans {
            assert_gap_recovered(&store, lane).await;
            assert_eq!(
                store
                    .coverage_block_by_hash(lane.descriptor(), chain[3].block.hash)
                    .await
                    .expect("coverage"),
                Some(BlockNumber(3)),
                "{} replays the canonical block it had not applied",
                lane.descriptor().id
            );
        }
    }

    #[tokio::test]
    async fn pausing_a_lane_that_another_component_failed_is_benign() {
        let chain = live_blocks(1);
        let lane = Arc::new(BlockLocalCounter::named("pause-failed-lane"));
        let (_directory, store) = store().await;
        store
            .park_processor_live_lane_at(
                lane.descriptor(),
                chain[1].block,
                "delivery_spool_hard_limit",
                false,
            )
            .await
            .expect("park lane");
        store
            .fail_processor_live_lane(lane.descriptor(), "processor_finality_conflict")
            .await
            .expect("fail lane");
        let runtime = reduce_failure_runtime(&store, &[lane.clone() as Arc<dyn Processor>], &[]);

        runtime
            .pause_live_gap(
                lane.as_ref(),
                chain[1].block,
                "unfinalized_gap_waiting_for_finality",
            )
            .await
            .expect("pausing a failed lane is not a live-lane failure");
        let state = store
            .processor_runtime_state(lane.descriptor())
            .await
            .expect("state");
        assert_eq!(state.state, ProcessorRunState::Failed);
        assert_eq!(state.reason.as_deref(), Some("processor_finality_conflict"));
    }

    fn reduce_failure_runtime(
        store: &SqliteStore,
        processors: &[Arc<dyn Processor>],
        frames: &[leani_primitives::BlockFrame],
    ) -> SharedLiveRuntime {
        let range = BlockRange::new(BlockNumber(0), BlockNumber(4)).expect("range");
        SharedLiveRuntime::new(
            store.clone(),
            Arc::new(ScriptedLiveSource::new(
                e2e_descriptor("restart-reduce-live", range),
                block_events(frames),
            )),
            processors.to_vec(),
            SharedLiveRuntimeConfig::default(),
        )
        .expect("runtime")
    }

    /// Two block-local lanes and an ordered one, named for one test so an
    /// armed failpoint cannot fire in another.
    fn crash_lanes(test: &str) -> Vec<Arc<dyn Processor>> {
        vec![
            Arc::new(BlockLocalCounter::named(&format!("{test}-first"))),
            Arc::new(BlockLocalCounter::named(&format!("{test}-second"))),
            Arc::new(OrderedLedgerProcessor::named(&format!("{test}-ledger"))),
        ]
    }

    fn scripted_live(
        store: &SqliteStore,
        processors: &[Arc<dyn Processor>],
        steps: Vec<LiveStep>,
    ) -> SharedLiveRuntime {
        let range = BlockRange::new(BlockNumber(0), BlockNumber(9)).expect("range");
        SharedLiveRuntime::new(
            store.clone(),
            Arc::new(ScriptedLiveSource::new(
                e2e_descriptor("crash-live", range),
                steps,
            )),
            processors.to_vec(),
            SharedLiveRuntimeConfig::default(),
        )
        .expect("runtime")
    }

    async fn run_live(runtime: &SharedLiveRuntime) -> Result<SharedLiveReport, RuntimeError> {
        runtime
            .run(
                LiveStart::Head,
                default_source_budget(),
                CancellationToken::new(),
            )
            .await
    }

    /// What queries and consumers can observe of one processor, apart from
    /// change sequence numbers and write times.
    #[derive(Debug, PartialEq)]
    struct LaneState {
        cursor: Option<ProcessorCursor>,
        coverage: Vec<Option<BlockHash>>,
        entities: Vec<(Vec<u8>, Vec<u8>)>,
        changes: Vec<(
            BlockRef,
            Finality,
            ChangeDirection,
            leani_processor_api::DomainChange,
            leani_store_sqlite::DeliveryOrigin,
        )>,
        stats: leani_store_sqlite::ProcessorStoreStats,
        gap: Option<BlockRef>,
        state: (ProcessorRunState, Option<String>),
    }

    async fn lane_state(store: &SqliteStore, processor: &dyn Processor) -> LaneState {
        let descriptor = processor.descriptor();
        let mut coverage = Vec::new();
        for number in 0..=9 {
            coverage.push(
                store
                    .coverage_hash(descriptor, BlockNumber(number))
                    .await
                    .expect("coverage"),
            );
        }
        let mut entities = Vec::new();
        for collection in ["counter.blocks", "ledger"] {
            entities.extend(
                store
                    .scan_entities(descriptor, collection, None, 1_000)
                    .await
                    .expect("entities"),
            );
        }
        let state = store
            .processor_runtime_state(descriptor)
            .await
            .expect("state");
        LaneState {
            cursor: store.processor_cursor(descriptor).await.expect("cursor"),
            coverage,
            entities,
            changes: store
                .changes(descriptor, ChainId(1), 0, 1_000)
                .await
                .expect("changes")
                .into_iter()
                .map(|record| {
                    (
                        record.block,
                        record.finality,
                        record.direction,
                        record.change,
                        record.origin,
                    )
                })
                .collect(),
            stats: store.processor_stats(descriptor).await.expect("stats"),
            gap: store
                .live_lane_gap(descriptor)
                .await
                .expect("gap")
                .map(|gap| gap.first_unapplied),
            state: (state.state, state.reason),
        }
    }

    /// Run `before` into the armed failpoint, then restart like the process
    /// would: fresh runtime objects over the same store file run startup
    /// reconciliation, then follow `after`.
    async fn crash_then_restart(
        path: &std::path::Path,
        lanes: &dyn Fn() -> Vec<Arc<dyn Processor>>,
        before: Vec<LiveStep>,
        after: Vec<LiveStep>,
    ) -> (SqliteStore, SharedLiveReport) {
        let store = SqliteStore::open(leani_store_sqlite::StoreConfig::new(path))
            .await
            .expect("store");
        let crashed = run_live(&scripted_live(&store, &lanes(), before)).await;
        assert!(
            matches!(&crashed, Err(RuntimeError::InvalidConfig(message)) if message.starts_with("injected crash")),
            "the run ends at the injected crash: {crashed:?}"
        );
        drop(store);

        let store = SqliteStore::open(leani_store_sqlite::StoreConfig::new(path))
            .await
            .expect("reopen store");
        let restarted = scripted_live(&store, &lanes(), after);
        let report = restarted
            .reconcile_startup()
            .await
            .expect("startup reconciliation");
        run_live(&restarted)
            .await
            .expect("live run after the restart");
        (store, report)
    }

    async fn assert_like_clean_run(
        store: &SqliteStore,
        clean: &SqliteStore,
        lanes: &[Arc<dyn Processor>],
    ) {
        for lane in lanes {
            assert_eq!(
                lane_state(store, lane.as_ref()).await,
                lane_state(clean, lane.as_ref()).await,
                "{} ends as in a run without the crash",
                lane.descriptor().id
            );
        }
    }

    /// Startup reconciliation, on a store with nothing to repair, writes
    /// nothing, including lane state times.
    async fn assert_reconciliation_is_a_no_op(store: &SqliteStore, lanes: &[Arc<dyn Processor>]) {
        let mut before = Vec::new();
        for lane in lanes {
            before.push((
                lane_state(store, lane.as_ref()).await,
                store
                    .processor_runtime_state(lane.descriptor())
                    .await
                    .expect("state"),
                store.live_lane_gap(lane.descriptor()).await.expect("gap"),
            ));
        }
        let report = scripted_live(store, lanes, Vec::new())
            .reconcile_startup()
            .await
            .expect("startup reconciliation");
        for (lane, before) in lanes.iter().zip(before) {
            let processor_report = &report.processors[lane.descriptor().id.as_str()];
            assert_eq!(
                (processor_report.applied, processor_report.reverted),
                (0, 0),
                "{} needs no repair",
                lane.descriptor().id
            );
            assert_eq!(
                (
                    lane_state(store, lane.as_ref()).await,
                    store
                        .processor_runtime_state(lane.descriptor())
                        .await
                        .expect("state"),
                    store.live_lane_gap(lane.descriptor()).await.expect("gap"),
                ),
                before,
                "{} is untouched",
                lane.descriptor().id
            );
        }
    }

    #[tokio::test]
    async fn startup_reconciliation_replays_a_block_a_crash_kept_from_some_lanes() {
        let test = "crash-before-apply";
        let chain = live_blocks(4);
        let (_clean_directory, clean) = store().await;
        run_live(&scripted_live(
            &clean,
            &crash_lanes(test),
            block_events(&chain),
        ))
        .await
        .expect("clean run");

        // Block 2 is retained and the first lane applies it; the process dies
        // before the second lane and the ledger do.
        failpoints::arm(
            failpoints::BEFORE_LIVE_APPLY,
            crash_lanes(test)[1].descriptor(),
            BlockNumber(2),
        );
        let directory = tempfile::tempdir().expect("tempdir");
        let (store, report) = crash_then_restart(
            &directory.path().join("node.sqlite"),
            &|| crash_lanes(test),
            block_events(&chain[..=2]),
            block_events(&chain[3..]),
        )
        .await;

        assert_like_clean_run(&store, &clean, &crash_lanes(test)).await;
        assert_eq!(report.processors[&format!("{test}-second")].applied, 1);
        assert_eq!(report.processors[&format!("{test}-ledger")].applied, 1);
    }

    #[tokio::test]
    async fn startup_reconciliation_applies_a_delta_a_crash_left_pending() {
        let test = "crash-after-persist";
        let chain = live_blocks(4);
        let (_clean_directory, clean) = store().await;
        run_live(&scripted_live(
            &clean,
            &crash_lanes(test),
            block_events(&chain),
        ))
        .await
        .expect("clean run");

        // The second lane persists its block-2 delta; the process dies before
        // that delta applies, and before the ledger sees block 2.
        failpoints::arm(
            failpoints::AFTER_PERSIST_DELTA,
            crash_lanes(test)[1].descriptor(),
            BlockNumber(2),
        );
        let directory = tempfile::tempdir().expect("tempdir");
        let (store, report) = crash_then_restart(
            &directory.path().join("node.sqlite"),
            &|| crash_lanes(test),
            block_events(&chain[..=2]),
            block_events(&chain[3..]),
        )
        .await;

        // No orphan pending delta is left to hold back a hot/cold handoff.
        for lane in crash_lanes(test) {
            assert_eq!(
                report.processors[lane.descriptor().id.as_str()].pending,
                0,
                "{} keeps no pending delta",
                lane.descriptor().id
            );
        }
        assert_like_clean_run(&store, &clean, &crash_lanes(test)).await;
    }

    #[tokio::test]
    async fn startup_reconciliation_undoes_a_branch_a_crashed_reorg_left_applied() {
        let test = "crash-in-reorg";
        let chain = live_blocks(4);
        let replacement = replacement_branch(&chain, 3, 2);
        // A shortening reorg replaces blocks 3 and 4 with 3' alone; 4' follows.
        let reorg = LiveStep::Event(ChainEvent::Reorg {
            reverted: vec![chain[4].block, chain[3].block],
            applied: vec![replacement[0].clone()],
        });
        let mut steps = block_events(&chain);
        steps.push(reorg.clone());
        steps.extend(block_events(&replacement[1..]));
        let (_clean_directory, clean) = store().await;
        run_live(&scripted_live(&clean, &crash_lanes(test), steps))
            .await
            .expect("clean run");

        // The reorg switches the canonical chain and the first lane undoes the
        // old branch; the process dies before the second lane and the ledger
        // undo it, and before any lane applies 3'.
        failpoints::arm(
            failpoints::BEFORE_REORG_UNDO,
            crash_lanes(test)[1].descriptor(),
            BlockNumber(3),
        );
        let mut before = block_events(&chain);
        before.push(reorg);
        let directory = tempfile::tempdir().expect("tempdir");
        let (store, report) = crash_then_restart(
            &directory.path().join("node.sqlite"),
            &|| crash_lanes(test),
            before,
            block_events(&replacement[1..]),
        )
        .await;

        assert_eq!(report.processors[&format!("{test}-first")].reverted, 0);
        assert_eq!(report.processors[&format!("{test}-second")].reverted, 2);
        assert_eq!(report.processors[&format!("{test}-ledger")].reverted, 2);
        assert_like_clean_run(&store, &clean, &crash_lanes(test)).await;
        assert_reconciliation_is_a_no_op(&store, &crash_lanes(test)).await;
    }

    /// A block-local counter and an ordered ledger whose live delivery holds
    /// three blocks' changes (56 and 38 bytes each), so block 3 reaches their
    /// limits and takes `action`, and a counter without a limit.
    fn delivery_limited_lanes(test: &str, action: DeliveryLimitAction) -> Vec<Arc<dyn Processor>> {
        let limited = |lifecycle: &leani_processor_api::LifecyclePolicies, max_bytes| {
            let mut lifecycle = lifecycle.clone();
            lifecycle.delivery.max_bytes = max_bytes;
            lifecycle.delivery.on_limit = action;
            lifecycle
        };
        let counter = BlockLocalCounter::named(&format!("{test}-limited"));
        let ledger = OrderedLedgerProcessor::named(&format!("{test}-limited-ledger"));
        vec![
            Arc::new(
                counter
                    .clone()
                    .with_lifecycle(limited(&counter.descriptor().lifecycle, 180)),
            ),
            Arc::new(
                ledger
                    .clone()
                    .with_lifecycle(limited(&ledger.descriptor().lifecycle, 120)),
            ),
            Arc::new(BlockLocalCounter::named(&format!("{test}-healthy"))),
        ]
    }

    /// Consumers acknowledge, so pruning frees every lane's delivery capacity;
    /// an operator resets each failed lane; then the lanes drain.
    async fn free_delivery_and_drain(store: &SqliteStore, lanes: &[Arc<dyn Processor>]) {
        for lane in lanes {
            store
                .prune_changes_before(lane.descriptor(), u64::MAX)
                .await
                .expect("free delivery capacity");
            if store
                .processor_runtime_state(lane.descriptor())
                .await
                .expect("state")
                .state
                == ProcessorRunState::Failed
            {
                store
                    .reset_failed_live_lane(lane.descriptor())
                    .await
                    .expect("operator reset");
            }
        }
        scripted_live(store, lanes, Vec::new())
            .reconcile_pending()
            .await
            .expect("drain");
    }

    /// Live blocks 0..=4, where the limited lanes reach their delivery limits
    /// at block 3. The process dies after the store paused or failed
    /// `lanes[crashed]`, before the runtime recorded its gap marker.
    async fn crash_at_a_delivery_limit(
        test: &str,
        action: DeliveryLimitAction,
        state: ProcessorRunState,
        crashed: usize,
    ) {
        let chain = live_blocks(4);
        let lanes = || delivery_limited_lanes(test, action);
        let (_clean_directory, clean) = store().await;
        run_live(&scripted_live(&clean, &lanes(), block_events(&chain)))
            .await
            .expect("clean run");
        assert_parked_at(
            &clean,
            lanes()[crashed].as_ref(),
            chain[3].block,
            state,
            "delivery_spool_hard_limit",
        )
        .await;
        free_delivery_and_drain(&clean, &lanes()).await;

        failpoints::arm(
            failpoints::BEFORE_LIVE_PARK,
            lanes()[crashed].descriptor(),
            BlockNumber(3),
        );
        let directory = tempfile::tempdir().expect("tempdir");
        let (store, _) = crash_then_restart(
            &directory.path().join("node.sqlite"),
            &lanes,
            block_events(&chain[..=3]),
            block_events(&chain[4..]),
        )
        .await;

        // Reconciliation records where the lane stopped and keeps its state,
        // so it replays from there when it resumes or is reset.
        assert_parked_at(
            &store,
            lanes()[crashed].as_ref(),
            chain[3].block,
            state,
            "delivery_spool_hard_limit",
        )
        .await;
        assert_reconciliation_is_a_no_op(&store, &lanes()).await;
        free_delivery_and_drain(&store, &lanes()).await;
        assert_like_clean_run(&store, &clean, &lanes()).await;
    }

    #[tokio::test]
    async fn startup_reconciliation_records_where_a_crash_paused_a_lane_at_its_delivery_limit() {
        crash_at_a_delivery_limit(
            "crash-delivery-pause",
            DeliveryLimitAction::Pause,
            ProcessorRunState::Paused,
            0,
        )
        .await;
    }

    #[tokio::test]
    async fn startup_reconciliation_records_where_a_crash_failed_a_lane_at_its_delivery_limit() {
        crash_at_a_delivery_limit(
            "crash-delivery-fail",
            DeliveryLimitAction::Fail,
            ProcessorRunState::Failed,
            0,
        )
        .await;
    }

    #[tokio::test]
    async fn startup_reconciliation_records_where_a_crash_stopped_a_ledger_at_its_delivery_limit() {
        crash_at_a_delivery_limit(
            "crash-delivery-ordered",
            DeliveryLimitAction::Pause,
            ProcessorRunState::Paused,
            1,
        )
        .await;
    }

    /// A live source that sends `before`, then holds its stream until the
    /// test opens `gate` and sends `after`, so the test can change the store
    /// between two blocks of one live run.
    #[derive(Debug)]
    struct GatedLiveSource {
        descriptor: SourceDescriptor,
        before: Vec<leani_primitives::BlockFrame>,
        after: Vec<leani_primitives::BlockFrame>,
        gate: Arc<tokio::sync::Notify>,
    }

    #[async_trait]
    impl LiveSource for GatedLiveSource {
        fn descriptor(&self) -> &SourceDescriptor {
            &self.descriptor
        }

        async fn subscribe(
            &self,
            _request: DataRequest,
            _start: LiveStart,
            _budget: SourceBudget,
            _cancellation: CancellationToken,
        ) -> Result<leani_source_api::ChainEventStream, SourceError> {
            let events = |frames: Vec<leani_primitives::BlockFrame>| {
                futures::stream::iter(
                    frames
                        .into_iter()
                        .map(|frame| Ok::<_, SourceError>(ChainEvent::Block(Box::new(frame)))),
                )
            };
            let gate = Arc::clone(&self.gate);
            let after = self.after.clone();
            Ok(events(self.before.clone())
                .chain(
                    futures::stream::once(async move {
                        gate.notified().await;
                        events(after)
                    })
                    .flatten(),
                )
                .boxed())
        }
    }

    /// The writes the store makes when a commit on a lane's live stream that
    /// is not the lane's own, such as its cold backfill's, reaches the
    /// delivery limit: the lane pauses or fails, and nothing records where.
    async fn stop_limited_lanes(
        store: &SqliteStore,
        lanes: &[Arc<dyn Processor>],
        action: DeliveryLimitAction,
    ) {
        for lane in &lanes[..2] {
            if action == DeliveryLimitAction::Fail {
                store
                    .fail_processor_live_lane(lane.descriptor(), "delivery_spool_hard_limit")
                    .await
                    .expect("store-recorded failure");
            } else {
                store
                    .pause_processor_live_lane(lane.descriptor(), "delivery_spool_hard_limit")
                    .await
                    .expect("store-recorded pause");
            }
        }
    }

    /// Live blocks 0..=4, and between blocks 2 and 3 the store stops the
    /// limited lanes, which have applied through the tip. A run without a
    /// restart, and one restarted right after the stop, must end alike once
    /// delivery capacity frees up and failed lanes are reset.
    async fn stop_at_the_tip_then_restart(test: &str, action: DeliveryLimitAction) {
        let chain = live_blocks(4);
        let lanes = || delivery_limited_lanes(test, action);
        let (_clean_directory, clean) = store().await;
        let gate = Arc::new(tokio::sync::Notify::new());
        let runtime = SharedLiveRuntime::new(
            clean.clone(),
            Arc::new(GatedLiveSource {
                descriptor: fixture_source_descriptor(
                    "gated-live",
                    BlockRange::new(BlockNumber(0), BlockNumber(4)).expect("range"),
                ),
                before: chain[..=2].to_vec(),
                after: chain[3..].to_vec(),
                gate: Arc::clone(&gate),
            }),
            lanes(),
            SharedLiveRuntimeConfig::default(),
        )
        .expect("runtime");
        let live = tokio::spawn(async move { run_live(&runtime).await });
        tokio::time::timeout(Duration::from_secs(10), async {
            for lane in lanes() {
                while store_cursor(&clean, lane.as_ref()).await != Some(BlockNumber(2)) {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        })
        .await
        .expect("every lane applies block 2");
        stop_limited_lanes(&clean, &lanes(), action).await;
        gate.notify_one();
        live.await.expect("live task").expect("clean run");
        free_delivery_and_drain(&clean, &lanes()).await;

        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("node.sqlite");
        let store = SqliteStore::open(leani_store_sqlite::StoreConfig::new(&path))
            .await
            .expect("store");
        run_live(&scripted_live(&store, &lanes(), block_events(&chain[..=2])))
            .await
            .expect("live run before the restart");
        stop_limited_lanes(&store, &lanes(), action).await;
        drop(store);
        let store = SqliteStore::open(leani_store_sqlite::StoreConfig::new(&path))
            .await
            .expect("reopen store");
        let restarted = scripted_live(&store, &lanes(), block_events(&chain[3..]));
        restarted
            .reconcile_startup()
            .await
            .expect("startup reconciliation");
        run_live(&restarted)
            .await
            .expect("live run after the restart");
        free_delivery_and_drain(&store, &lanes()).await;
        assert_like_clean_run(&store, &clean, &lanes()).await;
    }

    async fn store_cursor(store: &SqliteStore, processor: &dyn Processor) -> Option<BlockNumber> {
        store
            .processor_cursor(processor.descriptor())
            .await
            .expect("cursor")
            .map(|cursor| cursor.block_number)
    }

    #[tokio::test]
    async fn a_lane_the_store_paused_at_the_tip_resumes_after_a_restart_without_a_hole() {
        stop_at_the_tip_then_restart("tip-delivery-pause", DeliveryLimitAction::Pause).await;
    }

    #[tokio::test]
    async fn a_lane_the_store_failed_at_the_tip_can_be_reset_after_a_restart() {
        stop_at_the_tip_then_restart("tip-delivery-fail", DeliveryLimitAction::Fail).await;
    }

    #[tokio::test]
    async fn a_known_unavailable_lane_the_store_paused_without_a_marker_is_mapped_again() {
        let counter = Arc::new(BlockLocalCounter::named("repaused-counter"));
        let (_directory, store) = store().await;
        store
            .register_processor(counter.descriptor())
            .await
            .expect("register");
        let runtime = reduce_failure_runtime(&store, &[counter.clone() as Arc<dyn Processor>], &[]);
        // The live lane parked this lane before, and its gap completed since.
        // A cold-backfill commit on its live stream then reached the delivery
        // limit, so the store paused it without a marker.
        runtime
            .unavailable_processors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(counter.descriptor().instance.to_string());
        store
            .pause_processor_live_lane(counter.descriptor(), "delivery_spool_hard_limit")
            .await
            .expect("store-recorded pause");

        assert!(
            !runtime
                .processor_live_lane_unavailable(counter.as_ref())
                .await
                .expect("availability"),
            "its next block commits, so a refusal parks it where it stopped"
        );
    }

    #[tokio::test]
    async fn a_failed_ledger_behind_its_history_gets_no_marker_ahead_of_its_cursor() {
        let chain = live_blocks(5);
        let ledger = Arc::new(OrderedLedgerProcessor::named("behind-failed-ledger"));
        let counter = Arc::new(BlockLocalCounter::named("behind-failed-counter"));
        let lanes: Vec<Arc<dyn Processor>> = vec![ledger.clone(), counter.clone()];
        let (_directory, store) = store().await;
        // Live frames reached block 4 and the counter applied them, while the
        // ledger's history reached only block 1. The store then failed both
        // lanes at their delivery limits, recording no marker.
        for (sequence, frame) in (1..).zip(&chain[..=4]) {
            store.store_recent_frame(frame).await.expect("retain frame");
            apply_live_frame(&store, counter.as_ref(), frame, sequence).await;
        }
        for (sequence, frame) in (1..).zip(&chain[..=1]) {
            apply_live_frame(&store, ledger.as_ref(), frame, sequence).await;
        }
        for lane in &lanes {
            store
                .fail_processor_live_lane(lane.descriptor(), "delivery_spool_hard_limit")
                .await
                .expect("store-recorded failure");
        }

        reduce_failure_runtime(&store, &lanes, &chain[5..])
            .run(
                LiveStart::Head,
                default_source_budget(),
                CancellationToken::new(),
            )
            .await
            .expect("live run");

        // The counter records block 5, which it has not applied. The ledger
        // records nothing: block 5 is not the block after its cursor.
        assert_parked_at(
            &store,
            counter.as_ref(),
            chain[5].block,
            ProcessorRunState::Failed,
            "delivery_spool_hard_limit",
        )
        .await;
        assert!(
            store
                .live_lane_gap(ledger.descriptor())
                .await
                .expect("gap")
                .is_none()
        );
    }

    #[tokio::test]
    async fn startup_reconciliation_replays_retained_frames_above_a_hole() {
        let chain = live_blocks(6);
        let counter = Arc::new(BlockLocalCounter::named("hole-above-counter"));
        let lanes: Vec<Arc<dyn Processor>> = vec![counter.clone()];
        let (_directory, store) = store().await;
        // Frames 0..=2 survive from before downtime; the node then seeded its
        // finalized anchor, block 5, and retained 5 and 6 from its overlap.
        // Nothing retained blocks 3 and 4, which the cold backfill covers.
        for (sequence, frame) in (1..).zip(&chain[..=2]) {
            store.store_recent_frame(frame).await.expect("retain frame");
            apply_live_frame(&store, counter.as_ref(), frame, sequence).await;
        }
        store
            .store_canonical_anchor(
                ChainId(1),
                BlockRef {
                    parent_hash: BlockHash::ZERO,
                    timestamp: 1,
                    ..chain[5].block
                },
                Finality::Finalized,
            )
            .await
            .expect("seed anchor");
        for frame in &chain[5..] {
            store.store_recent_frame(frame).await.expect("retain frame");
        }

        reduce_failure_runtime(&store, &lanes, &[])
            .reconcile_startup()
            .await
            .expect("startup reconciliation");

        let described = rewind_probe_describe(&store, counter.as_ref()).await;
        assert_eq!(
            store_cursor(&store, counter.as_ref()).await,
            Some(BlockNumber(6)),
            "the lane replays the frames retained above the hole: {described}"
        );
        assert_gap_recovered(&store, counter.as_ref()).await;
        for (number, covered) in [(3, false), (4, false), (5, true), (6, true)] {
            assert_eq!(
                store
                    .coverage_hash(counter.descriptor(), BlockNumber(number))
                    .await
                    .expect("coverage")
                    .is_some(),
                covered,
                "block {number}"
            );
        }
    }

    #[tokio::test]
    async fn startup_reconciliation_leaves_a_consistent_store_untouched() {
        let chain = live_blocks(4);
        let replacement = replacement_branch(&chain, 3, 2);
        let mut lanes = crash_lanes("consistent-restart");
        // A lane whose reducer rejects block 3 fails there, and the reorg
        // moves its gap onto 3' with a pending delta; it stays frozen.
        lanes.push(Arc::new(FailingReduce::new(
            BlockLocalCounter::named("consistent-restart-failed"),
            BlockNumber(3),
        )));
        let mut steps = block_events(&chain);
        steps.push(LiveStep::Event(ChainEvent::Reorg {
            reverted: vec![chain[4].block, chain[3].block],
            applied: replacement.clone(),
        }));
        let (_directory, store) = store().await;
        run_live(&scripted_live(&store, &lanes, steps))
            .await
            .expect("live run");
        assert_parked_at(
            &store,
            lanes[3].as_ref(),
            replacement[0].block,
            ProcessorRunState::Failed,
            "processor_live_reduce_failed",
        )
        .await;

        assert_reconciliation_is_a_no_op(&store, &lanes).await;
    }

    #[tokio::test]
    async fn startup_reconciliation_deletes_pending_deltas_no_lane_can_apply() {
        let chain = live_blocks(2);
        let ledger = Arc::new(OrderedLedgerProcessor::named("stale-pending-ledger"));
        let (_directory, store) = store().await;
        for (sequence, frame) in (1..).zip(&chain[..=1]) {
            store.store_recent_frame(frame).await.expect("retain frame");
            apply_live_frame(&store, ledger.as_ref(), frame, sequence).await;
        }
        store
            .store_recent_frame(&chain[2])
            .await
            .expect("retain frame");
        let delta = |block: BlockRef| {
            EncodedDelta::new(
                ledger.descriptor(),
                ChainId(1),
                block,
                block.hash.0.to_vec(),
            )
        };
        let mut reverted = chain[2].block;
        reverted.hash = BlockHash::new([0xd2; 32]);
        // A delta for a block the ledger applied, one for a block a reorg
        // replaced, and one it still needs.
        for block in [chain[1].block, reverted, chain[2].block] {
            store
                .persist_delta(ledger.descriptor(), &delta(block))
                .await
                .expect("persist delta");
        }

        let report = reduce_failure_runtime(&store, &[ledger.clone() as Arc<dyn Processor>], &[])
            .reconcile_startup()
            .await
            .expect("startup reconciliation");

        let described = rewind_probe_describe(&store, ledger.as_ref()).await;
        assert_eq!(
            report.processors["stale-pending-ledger"].pending, 0,
            "{described}"
        );
        assert_gap_recovered(&store, ledger.as_ref()).await;
        assert_eq!(report.processors["stale-pending-ledger"].applied, 1);
        assert_eq!(
            store
                .processor_cursor(ledger.descriptor())
                .await
                .expect("cursor")
                .expect("ledger cursor")
                .block_hash,
            chain[2].block.hash
        );
    }

    #[tokio::test]
    async fn startup_reconciliation_replays_retained_frames_to_lanes_that_applied_none() {
        let chain = live_blocks(4);
        // A block-local lane from genesis and an ordered lane from block 3,
        // neither of which applied a block, while frames 2..=4 are retained.
        let counter = Arc::new(BlockLocalCounter::named("late-counter"));
        let ledger = Arc::new(
            FailingReduce::new(
                OrderedLedgerProcessor::named("late-ledger"),
                BlockNumber(u64::MAX),
            )
            .starting_at(BlockNumber(3)),
        );
        let (_directory, store) = store().await;
        for frame in &chain[2..] {
            store.store_recent_frame(frame).await.expect("retain frame");
        }

        let lanes: Vec<Arc<dyn Processor>> = vec![counter.clone(), ledger.clone()];
        let report = reduce_failure_runtime(&store, &lanes, &[])
            .reconcile_startup()
            .await
            .expect("startup reconciliation");

        assert_eq!(report.processors["late-counter"].applied, 3);
        assert_eq!(report.processors["late-ledger"].applied, 2);
        for (lane, first) in lanes.iter().zip([2, 3]) {
            let lane = lane.as_ref();
            assert_gap_recovered(&store, lane).await;
            for frame in &chain[first..] {
                assert_eq!(
                    store
                        .coverage_hash(lane.descriptor(), frame.block.number)
                        .await
                        .expect("coverage"),
                    Some(frame.block.hash),
                    "{} replays block {}",
                    lane.descriptor().id,
                    frame.block.number.0
                );
            }
        }
    }

    /// Retain frames 0..=4 and apply blocks 3 and 4 to `counter` as the live
    /// lane would, ahead of its cold backfill.
    async fn retain_live_tip_before_backfill(
        store: &SqliteStore,
        counter: &Arc<BlockLocalCounter>,
    ) {
        for frame in live_blocks(4) {
            store
                .store_recent_frame(&frame)
                .await
                .expect("retain frame");
            if frame.block.number >= BlockNumber(3) {
                apply_live_frame(store, counter.as_ref(), &frame, frame.block.number.0).await;
            }
        }
    }

    /// The automatic cold backfill's job for `counter` over `range`, served
    /// from history.
    async fn run_counter_backfill(
        store: &SqliteStore,
        counter: &Arc<BlockLocalCounter>,
        range: BlockRange,
    ) -> Result<BackfillReport, RuntimeError> {
        HistoricalRuntime::new(
            store.clone(),
            Arc::new(ScriptedHistorySource::from_frames(
                fixture_source_descriptor("counter-backfill-history", range),
                frames(range),
            )),
            counter.clone(),
            HistoricalRuntimeConfig {
                mapper_concurrency: 2,
                ..HistoricalRuntimeConfig::default()
            },
        )
        .expect("runtime")
        .run(
            BackfillJob::for_processor(
                "counter-backfill",
                counter.as_ref(),
                ChainId(1),
                range,
                VerificationPolicy::CompleteCryptographic,
            )
            .expect("job"),
            default_source_budget(),
            CancellationToken::new(),
        )
        .await
    }

    #[tokio::test]
    async fn startup_reconciliation_deletes_a_delta_a_crashed_backfill_left_pending() {
        let range = BlockRange::new(BlockNumber(0), BlockNumber(4)).expect("range");
        let counter = Arc::new(BlockLocalCounter::named("crash-backfill-counter"));
        let lanes: Vec<Arc<dyn Processor>> = vec![counter.clone()];
        let (_clean_directory, clean) = store().await;
        retain_live_tip_before_backfill(&clean, &counter).await;
        run_counter_backfill(&clean, &counter, range)
            .await
            .expect("clean backfill");

        // The backfill persists block 1's delta; the process dies before that
        // delta applies.
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("node.sqlite");
        let store = SqliteStore::open(leani_store_sqlite::StoreConfig::new(&path))
            .await
            .expect("store");
        retain_live_tip_before_backfill(&store, &counter).await;
        failpoints::arm(
            failpoints::AFTER_PERSIST_DELTA,
            counter.descriptor(),
            BlockNumber(1),
        );
        let crashed = run_counter_backfill(&store, &counter, range).await;
        assert!(
            matches!(&crashed, Err(RuntimeError::InvalidConfig(message)) if message.starts_with("injected crash")),
            "the backfill ends at the injected crash: {crashed:?}"
        );
        drop(store);

        // The restart reconciles before its cold backfill runs again.
        let store = SqliteStore::open(leani_store_sqlite::StoreConfig::new(&path))
            .await
            .expect("reopen store");
        let report = reduce_failure_runtime(&store, &lanes, &[])
            .reconcile_startup()
            .await
            .expect("startup reconciliation");
        assert_eq!(
            report.processors["crash-backfill-counter"].pending, 0,
            "no orphan pending delta is left to hold back the hot/cold handoff"
        );
        run_counter_backfill(&store, &counter, range)
            .await
            .expect("backfill after the restart");
        assert_eq!(
            lane_state(&store, counter.as_ref()).await,
            lane_state(&clean, counter.as_ref()).await
        );
    }

    #[tokio::test]
    async fn a_hole_below_the_retained_window_is_left_to_the_cold_backfill() {
        let range = BlockRange::new(BlockNumber(1), BlockNumber(5)).expect("range");
        let history = frames(range);
        let counter = Arc::new(BlockLocalCounter::named("hole-backfill-counter"));
        let (_directory, store) = store().await;
        // Blocks 1..=5 are covered except block 3, and no frame is retained.
        for (sequence, frame) in (1..).zip(
            history
                .iter()
                .filter(|frame| frame.block.number != BlockNumber(3)),
        ) {
            apply_live_frame(&store, counter.as_ref(), frame, sequence).await;
        }
        reduce_failure_runtime(&store, &[counter.clone() as Arc<dyn Processor>], &[])
            .reconcile_startup()
            .await
            .expect("startup reconciliation");
        assert!(
            store
                .live_lane_gap(counter.descriptor())
                .await
                .expect("gap")
                .is_none(),
            "reconciliation has no retained frame to replay block 3 from"
        );

        // The automatic cold backfill plans its coverage gaps: block 3 alone.
        let report = run_counter_backfill(&store, &counter, range)
            .await
            .expect("cold backfill");
        assert_eq!(report.frames_committed, 1);
        assert_eq!(report.final_coverage, vec![range]);
    }

    #[tokio::test]
    async fn an_ordered_replay_onto_a_seeded_anchor_checks_its_parent_link() {
        let chain = live_blocks(2);
        let mut fork = live_fixture(1, chain[0].block.hash);
        fork.block.hash = BlockHash::new([0xf1; 32]);
        let ledger = Arc::new(OrderedLedgerProcessor::named("anchor-link-ledger"));
        let (_directory, store) = store().await;
        store
            .register_processor(ledger.descriptor())
            .await
            .expect("register");
        for frame in &chain[..=1] {
            store.store_recent_frame(frame).await.expect("retain frame");
        }
        // Block 2 is a seeded finality anchor, with no parent hash and no
        // frame. The ledger applied a block 1 that is not its parent.
        let anchor = BlockRef {
            parent_hash: BlockHash::ZERO,
            timestamp: 1,
            ..chain[2].block
        };
        store
            .store_canonical_anchor(ChainId(1), anchor, Finality::Finalized)
            .await
            .expect("seed anchor");
        apply_live_frame(&store, ledger.as_ref(), &chain[0], 1).await;
        apply_live_frame(&store, ledger.as_ref(), &fork, 2).await;
        store
            .park_processor_live_lane_at(
                ledger.descriptor(),
                anchor,
                "delivery_spool_hard_limit",
                false,
            )
            .await
            .expect("park lane");
        let mut finalized = chain[2].clone();
        finalized.finality = Finality::Finalized;

        reduce_failure_runtime(&store, &[ledger.clone() as Arc<dyn Processor>], &[])
            .with_finalized_gap_recovery(Arc::new(StaticLiveGapRecovery {
                frames: vec![finalized],
            }))
            .reconcile_pending()
            .await
            .expect("drain");

        assert!(
            store
                .coverage_hash(ledger.descriptor(), BlockNumber(2))
                .await
                .expect("coverage")
                .is_none(),
            "the ledger applies no block that does not descend from its cursor"
        );
        assert_parked_at(
            &store,
            ledger.as_ref(),
            anchor,
            ProcessorRunState::Failed,
            "finalized_gap_recovery_canonical_mismatch",
        )
        .await;
    }

    #[tokio::test]
    async fn a_rewind_onto_a_seeded_anchor_a_paused_lane_never_applied_completes_its_gap() {
        let chain = live_blocks(5);
        let lane = Arc::new(BlockLocalCounter::named("anchor-rewind-lane"));
        let ledger = Arc::new(OrderedLedgerProcessor::named("anchor-rewind-ledger"));
        let healthy = Arc::new(BlockLocalCounter::named("anchor-rewind-healthy"));
        let (_directory, store) = store().await;
        for processor in [lane.descriptor(), ledger.descriptor(), healthy.descriptor()] {
            store.register_processor(processor).await.expect("register");
        }
        // After downtime the node seeds its finalized anchor, block 2, with no
        // frame, and follows live blocks from there, so no lane saw block 2.
        let anchor = BlockRef {
            parent_hash: BlockHash::ZERO,
            timestamp: 1,
            ..chain[2].block
        };
        store
            .store_canonical_anchor(ChainId(1), anchor, Finality::Finalized)
            .await
            .expect("seed anchor");
        for (sequence, frame) in (1..).zip(&chain[3..]) {
            store.store_recent_frame(frame).await.expect("retain frame");
            apply_live_frame(&store, healthy.as_ref(), frame, sequence).await;
        }
        apply_live_frame(&store, lane.as_ref(), &chain[3], 1).await;
        store
            .park_processor_live_lane_at(
                lane.descriptor(),
                chain[4].block,
                "delivery_spool_hard_limit",
                false,
            )
            .await
            .expect("park lane");
        // The ledger's history has not reached the anchor yet, and its failed
        // cold backfill parked it at the live tip.
        store
            .park_processor_live_lane_at(
                ledger.descriptor(),
                chain[5].block,
                "hot_cold_handoff_failed",
                false,
            )
            .await
            .expect("park ledger");

        // The source rewinds to the anchor without a replacement branch.
        rewind_probe_run(
            &store,
            vec![lane.clone(), ledger.clone(), healthy.clone()],
            vec![LiveStep::Event(ChainEvent::Reorg {
                reverted: vec![chain[5].block, chain[4].block, chain[3].block],
                applied: Vec::new(),
            })],
        )
        .await;

        let described = rewind_probe_describe(&store, lane.as_ref()).await;
        assert!(
            store
                .live_lane_gap(lane.descriptor())
                .await
                .expect("gap")
                .is_none(),
            "the paused lane resumes at the new tip: {described}"
        );
        assert_gap_recovered(&store, lane.as_ref()).await;
        // The ledger still needs the anchor in order, so it waits there for
        // its history.
        assert_parked_at(
            &store,
            ledger.as_ref(),
            anchor,
            ProcessorRunState::Paused,
            "hot_cold_handoff_failed",
        )
        .await;
    }

    #[tokio::test]
    async fn a_delta_conflict_lane_resumes_after_a_restart_and_a_reset() {
        let chain = live_blocks(1);
        let ledger = Arc::new(OrderedLedgerProcessor::named("conflict-restart-ledger"));
        let lanes: Vec<Arc<dyn Processor>> = vec![ledger.clone()];
        let (_directory, store) = store().await;
        // Canonical blocks whose frames finality has pruned, so no retained
        // frame shows a pending delta to be a finality variant.
        for (sequence, frame) in (1..).zip(&chain) {
            store
                .store_canonical_anchor(ChainId(1), frame.block, Finality::Included)
                .await
                .expect("canonical block");
            apply_live_frame(&store, ledger.as_ref(), frame, sequence).await;
        }
        // A pending delta for an applied block carries other content.
        store
            .persist_delta(
                ledger.descriptor(),
                &EncodedDelta::new(
                    ledger.descriptor(),
                    ChainId(1),
                    chain[0].block,
                    vec![0xee; 32],
                ),
            )
            .await
            .expect("persist conflicting delta");
        reduce_failure_runtime(&store, &lanes, &[])
            .reconcile_pending()
            .await
            .expect("the conflict fails only its lane");
        assert_parked_at(
            &store,
            ledger.as_ref(),
            chain[0].block,
            ProcessorRunState::Failed,
            "processor_live_delta_conflict",
        )
        .await;

        // After investigating, the operator restarts the node and resets the
        // lane.
        let restarted = reduce_failure_runtime(&store, &lanes, &[]);
        restarted
            .reconcile_startup()
            .await
            .expect("startup reconciliation");
        store
            .reset_failed_live_lane(ledger.descriptor())
            .await
            .expect("reset");
        restarted
            .reconcile_pending()
            .await
            .expect("replay after the reset");

        let described = rewind_probe_describe(&store, ledger.as_ref()).await;
        assert!(
            store
                .pending_deltas(ledger.descriptor(), BlockNumber(0), 10)
                .await
                .expect("pending")
                .is_empty(),
            "startup reconciliation deletes the redundant delta: {described}"
        );
        assert_gap_recovered(&store, ledger.as_ref()).await;
    }

    #[tokio::test]
    async fn a_lane_parked_by_its_reducer_does_not_fail_the_live_lane_after_restart() {
        let chain = live_blocks(4);
        let failing = Arc::new(FailingReduce::new(
            OrderedLedgerProcessor::named("restart-reduce-ledger"),
            BlockNumber(1),
        ));
        let healthy = Arc::new(BlockLocalCounter::named("restart-reduce-healthy"));
        let processors: Vec<Arc<dyn Processor>> = vec![failing.clone(), healthy.clone()];
        let (_directory, store) = store().await;
        reduce_failure_runtime(&store, &processors, &chain[..=2])
            .run(
                LiveStart::Head,
                default_source_budget(),
                CancellationToken::new(),
            )
            .await
            .expect("the first run isolates the reducer failure");

        // A restarted node reconciles durable lanes before following new blocks.
        let restarted = reduce_failure_runtime(&store, &processors, &chain[3..]);
        restarted
            .reconcile_pending()
            .await
            .expect("startup reconciliation leaves the parked lane alone");
        let report = restarted
            .run(
                LiveStart::Head,
                default_source_budget(),
                CancellationToken::new(),
            )
            .await
            .expect("the parked lane does not fail the restarted live lane");
        assert_eq!(report.processors["restart-reduce-healthy"].applied, 2);
        assert_parked_at(
            &store,
            failing.as_ref(),
            chain[1].block,
            ProcessorRunState::Failed,
            "processor_live_reduce_failed",
        )
        .await;

        // An operator reset replays into the same reducer failure, which parks
        // the lane again instead of failing the live lane.
        store
            .reset_failed_live_lane(failing.descriptor())
            .await
            .expect("operator reset");
        restarted
            .reconcile_pending()
            .await
            .expect("replaying a block the reducer still rejects parks its lane again");
        assert_parked_at(
            &store,
            failing.as_ref(),
            chain[1].block,
            ProcessorRunState::Failed,
            "processor_live_reduce_failed",
        )
        .await;
    }

    #[tokio::test]
    async fn a_lane_parked_outside_the_live_stream_resumes_once_history_passes_its_gap() {
        let chain = live_blocks(7);
        let range = BlockRange::new(BlockNumber(0), BlockNumber(7)).expect("range");
        let ordered = Arc::new(OrderedLedgerProcessor::named("parked-behind-ledger"));
        let (_directory, store) = store().await;
        // Live blocks 5..=7 arrive while the ordered history from block 0 is
        // still behind, so their deltas wait as pending.
        let live = SharedLiveRuntime::new(
            store.clone(),
            Arc::new(ScriptedLiveSource::new(
                e2e_descriptor("parked-behind-live", range),
                block_events(&chain[5..]),
            )),
            vec![ordered.clone()],
            SharedLiveRuntimeConfig::default(),
        )
        .expect("runtime");
        live.run(
            LiveStart::Head,
            default_source_budget(),
            CancellationToken::new(),
        )
        .await
        .expect("live run");
        let pending = || async {
            store
                .processor_stats(ordered.descriptor())
                .await
                .expect("stats")
                .pending_deltas
        };
        assert_eq!(pending().await, 3);

        // Its cold backfill fails: the lane parks at the newest retained
        // frame, not at a finality anchor seeded above it without a parent.
        store
            .store_canonical_anchor(
                ChainId(1),
                leani_primitives::BlockRef {
                    number: BlockNumber(9),
                    hash: BlockHash::new([0x99; 32]),
                    parent_hash: BlockHash::ZERO,
                    timestamp: 1,
                },
                Finality::Finalized,
            )
            .await
            .expect("seed anchor");
        assert!(
            live.park_processor_lane(ordered.descriptor(), "hot_cold_handoff_failed")
                .await
                .expect("park")
        );
        assert_parked_at(
            &store,
            ordered.as_ref(),
            chain[7].block,
            ProcessorRunState::Paused,
            "hot_cold_handoff_failed",
        )
        .await;

        // A later backfill applies history past the parked block; the lane
        // then moves its gap on and resumes.
        for (sequence, frame) in (1..).zip(&chain) {
            apply_live_frame(&store, ordered.as_ref(), frame, sequence).await;
        }
        live.reconcile_pending().await.expect("reconcile");
        assert_gap_recovered(&store, ordered.as_ref()).await;
        assert_eq!(pending().await, 0);
    }

    #[tokio::test]
    async fn a_lane_paused_at_subscribe_receives_material_it_requires_after_resuming() {
        let range = BlockRange::new(BlockNumber(0), BlockNumber(2)).expect("range");
        let chain = live_chain(Finality::Included);
        let paused = Arc::new(FilteredCounter::named(
            "paused-at-subscribe",
            vec![sender_requirement(REPLAY_SENDER)],
        ));
        let live = Arc::new(FilteredCounter::named(
            "live-at-subscribe",
            vec![sender_requirement(OTHER_SENDER)],
        ));
        let (_directory, store) = store().await;
        park_at_first_block(&store, paused.as_ref(), &chain).await;
        let source = Arc::new(FilteringLiveSource::new(
            fixture_source_descriptor("paused-at-subscribe-live", range),
            chain
                .iter()
                .cloned()
                .map(|frame| ChainEvent::Block(Box::new(frame)))
                .collect(),
        ));

        let report = SharedLiveRuntime::new(
            store.clone(),
            source.clone(),
            vec![paused.clone(), live.clone()],
            SharedLiveRuntimeConfig::default(),
        )
        .expect("runtime")
        .run(
            LiveStart::Head,
            default_source_budget(),
            CancellationToken::new(),
        )
        .await
        .expect("live run");

        let requests = source.requests();
        assert_eq!(requests.len(), 1);
        for processor in [paused.as_ref(), live.as_ref()] {
            assert!(
                requests[0]
                    .filters
                    .scope
                    .covers(&processor.descriptor().requirements[0].filter),
                "the live request must cover {}",
                processor.descriptor().id
            );
        }
        assert_eq!(report.processors["paused-at-subscribe"].applied, 3);
        assert_eq!(report.processors["live-at-subscribe"].applied, 3);
        assert_gap_recovered(&store, paused.as_ref()).await;
    }

    #[tokio::test]
    async fn a_reorg_leaves_a_paused_lane_paused() {
        let range = BlockRange::new(BlockNumber(0), BlockNumber(2)).expect("range");
        let chain = live_chain(Finality::Included);
        let mut replacement = live_fixture(2, chain[1].block.hash);
        replacement.block.hash = BlockHash::new([0x72; 32]);
        // Parked ahead of its cursor, so it stays paused until history
        // reaches its gap.
        let paused = Arc::new(Requiring::new(
            OrderedLedgerProcessor::named("reorg-paused-ledger"),
            vec![sender_requirement(REPLAY_SENDER)],
        ));
        let live = Arc::new(FilteredCounter::named(
            "reorg-live-counter",
            vec![sender_requirement(OTHER_SENDER)],
        ));
        let (_directory, store) = store().await;
        // A lane parks at a block the store holds as canonical, so its marker
        // is on the canonical chain.
        store
            .store_canonical_anchor(ChainId(1), chain[1].block, Finality::Included)
            .await
            .expect("canonical block");
        store
            .park_processor_live_lane_at(
                paused.descriptor(),
                chain[1].block,
                "delivery_spool_hard_limit",
                false,
            )
            .await
            .expect("park lane");
        let mut events = chain
            .iter()
            .cloned()
            .map(|frame| ChainEvent::Block(Box::new(frame)))
            .collect::<Vec<_>>();
        events.push(ChainEvent::Reorg {
            reverted: vec![chain[2].block],
            applied: vec![replacement],
        });

        let report = SharedLiveRuntime::new(
            store.clone(),
            Arc::new(FilteringLiveSource::new(
                fixture_source_descriptor("reorg-paused-live", range),
                events,
            )),
            vec![paused.clone(), live.clone()],
            SharedLiveRuntimeConfig::default(),
        )
        .expect("runtime")
        .run(
            LiveStart::Head,
            default_source_budget(),
            CancellationToken::new(),
        )
        .await
        .expect("live run");

        assert_eq!(report.reorgs, 1);
        assert_eq!(report.processors["reorg-live-counter"].applied, 4);
        assert_parked_at(
            &store,
            paused.as_ref(),
            chain[1].block,
            ProcessorRunState::Paused,
            "delivery_spool_hard_limit",
        )
        .await;
    }

    #[tokio::test]
    async fn a_mapping_failure_during_a_reorg_does_not_fail_a_paused_lane() {
        let range = BlockRange::new(BlockNumber(0), BlockNumber(2)).expect("range");
        let chain = live_chain(Finality::Included);
        let mut replacement = live_fixture(2, chain[1].block.hash);
        replacement.block.hash = BlockHash::new([0x73; 32]);
        let paused = Arc::new(FailOnceCounter::named("reorg-mapping-paused"));
        let (_directory, store) = store().await;
        for frame in &chain {
            store.store_recent_frame(frame).await.expect("retain frame");
        }
        // Parked at the block the reorg reverts, so the reorg must move its
        // gap to the replacement even though mapping the replacement fails.
        store
            .park_processor_live_lane_at(
                paused.descriptor(),
                chain[2].block,
                "delivery_spool_hard_limit",
                false,
            )
            .await
            .expect("park lane");

        SharedLiveRuntime::new(
            store.clone(),
            Arc::new(ScriptedLiveSource::new(
                fixture_source_descriptor("reorg-mapping-live", range),
                vec![LiveStep::Event(ChainEvent::Reorg {
                    reverted: vec![chain[2].block],
                    applied: vec![replacement.clone()],
                })],
            )),
            vec![paused.clone()],
            SharedLiveRuntimeConfig::default(),
        )
        .expect("runtime")
        .run(
            LiveStart::Head,
            default_source_budget(),
            CancellationToken::new(),
        )
        .await
        .expect("live run");

        // The reorg left the lane paused; its own replay then recovered it.
        assert_gap_recovered(&store, paused.as_ref()).await;
        assert_eq!(
            store
                .processor_cursor(paused.descriptor())
                .await
                .expect("cursor")
                .expect("replayed")
                .block_hash,
            replacement.block.hash
        );
    }

    #[tokio::test]
    async fn shared_live_rejects_a_block_that_does_not_extend_the_canonical_tip() {
        let chain = live_blocks(2);
        let mut orphan = live_fixture(1, BlockHash::new([0x99; 32]));
        orphan.block.hash = BlockHash::new([0x98; 32]);
        for (name, next, expected) in [
            ("skips a height", chain[2].clone(), BlockNumber(1)),
            ("names another parent", orphan, BlockNumber(1)),
        ] {
            let range = BlockRange::new(BlockNumber(0), BlockNumber(2)).expect("range");
            let processor = Arc::new(BlockLocalCounter::named("continuity-counter"));
            let (_directory, store) = store().await;
            let result = SharedLiveRuntime::new(
                store.clone(),
                Arc::new(ScriptedLiveSource::new(
                    fixture_source_descriptor("continuity-live", range),
                    block_events(&[chain[0].clone(), next.clone()]),
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
            .await;

            assert!(
                matches!(
                    &result,
                    Err(RuntimeError::LiveGap { expected: gap, received })
                        if *gap == expected && *received == next.block
                ),
                "a block that {name} must be rejected: {result:?}"
            );
            assert!(
                store
                    .recent_frame(ChainId(1), next.block.number)
                    .await
                    .expect("recent lookup")
                    .is_none_or(|frame| frame.block != next.block),
                "a rejected block that {name} is not retained"
            );
            assert_eq!(
                store
                    .processor_cursor(processor.descriptor())
                    .await
                    .expect("cursor")
                    .expect("first block applied")
                    .block_number,
                BlockNumber(0)
            );
        }
    }

    #[tokio::test]
    async fn shared_live_rejects_a_reorg_that_does_not_revert_the_tip() {
        let range = BlockRange::new(BlockNumber(0), BlockNumber(2)).expect("range");
        let chain = live_blocks(2);
        let mut replacement = live_fixture(1, chain[0].block.hash);
        replacement.block.hash = BlockHash::new([0x61; 32]);
        let mut events = block_events(&chain);
        events.push(LiveStep::Event(ChainEvent::Reorg {
            reverted: vec![chain[1].block],
            applied: vec![replacement],
        }));
        let processor = Arc::new(BlockLocalCounter::named("reorg-tip-counter"));
        let (_directory, store) = store().await;

        let result = SharedLiveRuntime::new(
            store.clone(),
            Arc::new(ScriptedLiveSource::new(
                fixture_source_descriptor("reorg-tip-live", range),
                events,
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
        .await;

        assert!(
            matches!(result, Err(RuntimeError::InvalidReorg(_))),
            "a reorg below the tip must be rejected: {result:?}"
        );
        for frame in &chain {
            assert_eq!(
                store
                    .recent_frame(ChainId(1), frame.block.number)
                    .await
                    .expect("recent lookup")
                    .expect("canonical frame")
                    .block,
                frame.block
            );
        }
        assert_eq!(
            store
                .processor_cursor(processor.descriptor())
                .await
                .expect("cursor")
                .expect("cursor")
                .block_hash,
            chain[2].block.hash
        );
    }

    async fn encoded_recent_frame_bytes(frame: &leani_primitives::BlockFrame) -> u64 {
        let (_directory, scratch) = store().await;
        scratch
            .store_recent_frame(frame)
            .await
            .expect("measure frame");
        scratch
            .recent_stats(ChainId(1))
            .await
            .expect("recent stats")
            .encoded_bytes
    }

    #[tokio::test]
    async fn live_ingestion_waits_at_the_recent_hard_limit_until_finality_prunes() {
        let range = BlockRange::new(BlockNumber(0), BlockNumber(2)).expect("range");
        let chain = live_blocks(2);
        let limit = encoded_recent_frame_bytes(&chain[0]).await * 2;
        let processor = Arc::new(BlockLocalCounter::named("recent-limit-counter"));
        let (_directory, store) = store().await;
        let runtime = SharedLiveRuntime::new(
            store.clone(),
            Arc::new(ScriptedLiveSource::new(
                fixture_source_descriptor("recent-limit-live", range),
                block_events(&chain),
            )),
            vec![processor.clone()],
            SharedLiveRuntimeConfig {
                recent_hard_bytes: limit,
                ..SharedLiveRuntimeConfig::default()
            },
        )
        .expect("runtime");
        let (ready, readiness) = tokio::sync::watch::channel(false);
        let live = tokio::spawn(async move {
            runtime
                .run_with_readiness(
                    LiveStart::Head,
                    default_source_budget(),
                    CancellationToken::new(),
                    ready,
                )
                .await
        });

        // Two frames fill the limit. With no finality to prune them, the third
        // block waits and the live lane reports itself not ready.
        let recent_tip = || async {
            store
                .recent_stats(ChainId(1))
                .await
                .expect("recent stats")
                .latest_block
        };
        tokio::time::timeout(Duration::from_secs(10), async {
            while recent_tip().await != Some(BlockNumber(1)) || *readiness.borrow() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("ingestion stops at the recent hard limit");
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(recent_tip().await, Some(BlockNumber(1)));
        assert!(!*readiness.borrow());
        assert_eq!(
            store
                .processor_cursor(processor.descriptor())
                .await
                .expect("cursor")
                .expect("applied")
                .block_number,
            BlockNumber(1)
        );

        store
            .mark_recent_finalized(ChainId(1), BlockNumber(1), chain[1].block.hash)
            .await
            .expect("finalize");
        store
            .prune_recent_frames(ChainId(1), BlockNumber(1), 1, 1, limit)
            .await
            .expect("prune");
        let report = tokio::time::timeout(Duration::from_secs(10), live)
            .await
            .expect("ingestion resumes after pruning")
            .expect("live task")
            .expect("live run");

        assert_eq!(report.chain_blocks, 3);
        assert_eq!(report.processors["recent-limit-counter"].applied, 3);
        assert_eq!(recent_tip().await, Some(BlockNumber(2)));
    }

    #[tokio::test]
    async fn live_ingestion_fails_after_waiting_too_long_at_the_recent_hard_limit() {
        let range = BlockRange::new(BlockNumber(0), BlockNumber(2)).expect("range");
        let chain = live_blocks(2);
        let limit = encoded_recent_frame_bytes(&chain[0]).await * 2;
        let (_directory, store) = store().await;

        let result = SharedLiveRuntime::new(
            store.clone(),
            Arc::new(ScriptedLiveSource::new(
                fixture_source_descriptor("recent-stall-live", range),
                block_events(&chain),
            )),
            vec![Arc::new(BlockLocalCounter::named("recent-stall-counter"))],
            SharedLiveRuntimeConfig {
                recent_hard_bytes: limit,
                recent_storage_stall_limit: Duration::from_millis(300),
                ..SharedLiveRuntimeConfig::default()
            },
        )
        .expect("runtime")
        .run(
            LiveStart::Head,
            default_source_budget(),
            CancellationToken::new(),
        )
        .await;

        assert!(
            matches!(result, Err(RuntimeError::RecentStorageBudget { limit: observed_limit, .. }) if observed_limit == limit),
            "a stalled live lane fails for its supervisor to restart it: {result:?}"
        );
        assert_eq!(
            store
                .recent_stats(ChainId(1))
                .await
                .expect("recent stats")
                .latest_block,
            Some(BlockNumber(1)),
            "no unfinalized frame is dropped to make room"
        );
    }

    #[tokio::test]
    async fn concurrent_drains_of_one_live_gap_do_not_fail_the_lane() {
        let range = BlockRange::new(BlockNumber(0), BlockNumber(2)).expect("range");
        let chain = live_chain(Finality::Included);
        let processor = Arc::new(RendezvousCounter {
            inner: BlockLocalCounter::named("concurrent-gap-counter"),
            rendezvous: tokio::sync::Barrier::new(2),
        });
        let (_directory, store) = store().await;
        for frame in &chain {
            store.store_recent_frame(frame).await.expect("retain frame");
        }
        park_at_first_block(&store, processor.as_ref(), &chain).await;
        let live = SharedLiveRuntime::new(
            store.clone(),
            Arc::new(ScriptedLiveSource::new(
                fixture_source_descriptor("concurrent-gap-live", range),
                Vec::new(),
            )),
            vec![processor.clone()],
            SharedLiveRuntimeConfig::default(),
        )
        .expect("runtime");
        // The node reconciles on a clone while the live lane drains.
        let handoff = live.clone();

        let (first, second) = tokio::join!(live.reconcile_pending(), handoff.reconcile_pending());

        let first = first.expect("first drain");
        let second = second.expect("a concurrent drain of the same gap is not a lane failure");
        assert_eq!(
            first.processors["concurrent-gap-counter"].applied
                + second.processors["concurrent-gap-counter"].applied,
            3
        );
        assert_gap_recovered(&store, processor.as_ref()).await;
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
                e2e_descriptor("ordered-limit-live", range),
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
            e2e_descriptor("first-live", range),
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
            e2e_descriptor("replayed-live", range),
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
    async fn evm_events_replay_accepts_a_finality_variant_without_the_frame() {
        use leani_processor_evm_events::{
            EventDefinition, EventOutput, EvmEventsConfig, EvmEventsProcessor,
        };

        // Audit probe (M-P3): evm-events embeds finality in its delta, so a
        // stale included pending delta of a block applied as finalized was
        // a conflict once the block's frame had left the recent store.
        let processor = Arc::new(
            EvmEventsProcessor::new(EvmEventsConfig {
                start_block: BlockNumber(0),
                addresses: Vec::new(),
                events: vec![EventDefinition {
                    abi: "event Transfer(address indexed from, address indexed to, uint256 value)"
                        .to_owned(),
                    output: EventOutput {
                        collection: "events.transfers".to_owned(),
                        kind: "events.transfer".to_owned(),
                        key_fields: Vec::new(),
                        bucket_seconds: None,
                    },
                }],
            })
            .expect("processor"),
        );
        // The requirement's topic filter holds the event's topic zero.
        let transfer = processor.descriptor().requirements[0].filter.topics[0].alternatives[0];
        let mut from = [0_u8; 32];
        from[12..].fill(0x22);
        let mut value = vec![0_u8; 32];
        value[31] = 9;
        let mut included = included_frame(0, BlockHash::ZERO);
        included.logs = Material::Complete(vec![leani_primitives::Log {
            address: leani_primitives::Address::new([0x11; 20]),
            topics: vec![transfer, from, [0; 32]],
            data: value,
            transaction_hash: Some(leani_primitives::TransactionHash::new([0x44; 32])),
            transaction_index: 0,
            log_index: 0,
        }]);
        let mut finalized = included.clone();
        finalized.finality = Finality::Finalized;
        let (_directory, store) = store().await;
        let applied = processor.map(&finalized).await.expect("map finalized");
        store
            .apply(
                processor.as_ref(),
                ProcessorCursor {
                    processor_id: processor.descriptor().id.to_string(),
                    processor_version: processor.descriptor().version.to_string(),
                    chain_id: finalized.chain_id,
                    block_number: finalized.block.number,
                    block_hash: finalized.block.hash,
                    finality: Finality::Finalized,
                    sequence: 1,
                },
                &applied,
                &[],
            )
            .await
            .expect("apply the finalized block");
        let stale = processor.map(&included).await.expect("map included");
        assert_ne!(stale.checksum, applied.checksum);
        store
            .persist_delta(processor.descriptor(), &stale)
            .await
            .expect("persist the stale included delta");

        let processors: Vec<Arc<dyn Processor>> = vec![processor.clone()];
        let report = SharedLiveRuntime::new(
            store.clone(),
            Arc::new(ScriptedLiveSource::new(
                fixture_source_descriptor("evm-events-live", BlockRange::single(BlockNumber(0))),
                Vec::new(),
            )),
            processors,
            SharedLiveRuntimeConfig::default(),
        )
        .expect("live runtime")
        .reconcile_pending()
        .await
        .expect("reconcile the stale variant");
        assert!(
            store
                .recent_frame(ChainId(1), BlockNumber(0))
                .await
                .expect("recent lookup")
                .is_none()
        );
        assert_eq!(report.processors["evm-events"].duplicates, 1);
        assert_eq!(report.processors["evm-events"].pending, 0);
        assert_eq!(
            store
                .processor_runtime_state(processor.descriptor())
                .await
                .expect("state")
                .state,
            ProcessorRunState::Running
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
                    block_number: frame.block.number,
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

    async fn apply_live_frame(
        store: &SqliteStore,
        processor: &dyn Processor,
        frame: &leani_primitives::BlockFrame,
        sequence: u64,
    ) {
        let delta = processor.map(frame).await.expect("map");
        store
            .apply(
                processor,
                ProcessorCursor {
                    processor_id: processor.descriptor().id.to_string(),
                    processor_version: processor.descriptor().version.to_string(),
                    chain_id: frame.chain_id,
                    block_number: frame.block.number,
                    block_hash: frame.block.hash,
                    finality: frame.finality,
                    sequence,
                },
                &delta,
                &[],
            )
            .await
            .expect("apply");
    }

    #[tokio::test]
    async fn one_processor_finality_failure_does_not_stop_the_others_or_pruning() {
        let range = BlockRange::new(BlockNumber(0), BlockNumber(3)).expect("range");
        let chain = live_blocks(3);
        let healthy = Arc::new(BlockLocalCounter::named("finality-healthy"));
        let conflicting = Arc::new(BlockLocalCounter::named("finality-conflicting"));
        let (_directory, store) = store().await;
        for (sequence, frame) in (1..).zip(&chain) {
            store.store_recent_frame(frame).await.expect("retain frame");
            apply_live_frame(&store, healthy.as_ref(), frame, sequence).await;
        }
        // Corrupt coverage for one processor: it holds block 2's hash at
        // height 1, so finalizing that hash contradicts its coverage. It also
        // covers block 3, which a later anchor names.
        apply_live_frame(&store, conflicting.as_ref(), &chain[0], 1).await;
        let mut forged = live_fixture(1, chain[0].block.hash);
        forged.block.hash = chain[2].block.hash;
        apply_live_frame(&store, conflicting.as_ref(), &forged, 2).await;
        apply_live_frame(&store, conflicting.as_ref(), &chain[3], 3).await;
        let checkpoint = ConsensusCheckpoint {
            beacon_slot: 1,
            beacon_block_root: [1; 32],
            execution_block_hash: BlockHash::ZERO,
            obtained_at_unix_seconds: 1,
            source: "fixture".to_owned(),
        };
        let finalized = |block: &leani_primitives::BlockFrame, slot| {
            FinalityStep::Event(FinalityEvent::Finalized {
                block_number: block.block.number,
                block_hash: block.block.hash,
                beacon_slot: slot,
                beacon_block_root: [2; 32],
            })
        };

        let report = SharedFinalityRuntime::new(
            store.clone(),
            Arc::new(ScriptedFinalitySource::new(
                fixture_source_descriptor("isolated-finality", range),
                checkpoint.clone(),
                vec![finalized(&chain[2], 2), finalized(&chain[3], 3)],
            )),
            vec![conflicting.clone(), healthy.clone()],
            SharedFinalityRuntimeConfig {
                minimum_recent_blocks: 1,
                recent_soft_bytes: 1,
                recent_hard_bytes: 1_000_000,
            },
        )
        .expect("shared finality runtime")
        .run(checkpoint, CancellationToken::new())
        .await
        .expect("one processor's finality failure must not stop finality for the others");

        assert_eq!(report.finalized_through, Some(BlockNumber(3)));
        assert_eq!(
            report.processor_finalized_through.get("finality-healthy"),
            Some(&BlockNumber(3))
        );
        assert_eq!(
            store
                .finalized_through(healthy.descriptor())
                .await
                .expect("healthy finality"),
            Some(BlockNumber(3))
        );
        // The contradiction fails the processor's lane, so a later anchor
        // does not finalize it through the contradicted height.
        assert!(
            !report
                .processor_finalized_through
                .contains_key("finality-conflicting")
        );
        assert!(
            report.failed_processors["finality-conflicting"]
                .contains("resolves finalized hash at 1")
        );
        let state = store
            .processor_runtime_state(conflicting.descriptor())
            .await
            .expect("state");
        assert_eq!(state.state, ProcessorRunState::Failed);
        assert_eq!(state.reason.as_deref(), Some("processor_finality_conflict"));
        assert_eq!(
            store
                .finalized_through(conflicting.descriptor())
                .await
                .expect("conflicting finality"),
            None
        );
        assert_eq!(report.pruned_recent_frames, 3);
        assert!(
            store
                .recent_frame(ChainId(1), BlockNumber(0))
                .await
                .expect("recent lookup")
                .is_none()
        );
    }

    #[tokio::test]
    async fn a_finality_contradiction_is_recorded_for_a_lane_failed_for_another_reason() {
        let range = BlockRange::new(BlockNumber(0), BlockNumber(3)).expect("range");
        let chain = live_blocks(3);
        let healthy = Arc::new(BlockLocalCounter::named("hidden-conflict-healthy"));
        let conflicting = Arc::new(BlockLocalCounter::named("hidden-conflict-lane"));
        let (_directory, store) = store().await;
        for (sequence, frame) in (1..).zip(&chain) {
            store.store_recent_frame(frame).await.expect("retain frame");
            apply_live_frame(&store, healthy.as_ref(), frame, sequence).await;
        }
        apply_live_frame(&store, conflicting.as_ref(), &chain[0], 1).await;
        let mut forged = live_fixture(1, chain[0].block.hash);
        forged.block.hash = chain[2].block.hash;
        apply_live_frame(&store, conflicting.as_ref(), &forged, 2).await;
        apply_live_frame(&store, conflicting.as_ref(), &chain[3], 3).await;
        // The lane is already failed for another reason when finality reaches
        // the contradicted hash.
        store
            .park_processor_live_lane_at(
                conflicting.descriptor(),
                chain[2].block,
                "processor_live_reduce_failed",
                true,
            )
            .await
            .expect("fail lane");
        let checkpoint = ConsensusCheckpoint {
            beacon_slot: 1,
            beacon_block_root: [1; 32],
            execution_block_hash: BlockHash::ZERO,
            obtained_at_unix_seconds: 1,
            source: "fixture".to_owned(),
        };
        let finality = |steps| {
            SharedFinalityRuntime::new(
                store.clone(),
                Arc::new(ScriptedFinalitySource::new(
                    fixture_source_descriptor("hidden-conflict-finality", range),
                    checkpoint.clone(),
                    steps,
                )),
                vec![conflicting.clone(), healthy.clone()],
                SharedFinalityRuntimeConfig {
                    minimum_recent_blocks: 1,
                    recent_soft_bytes: 1,
                    recent_hard_bytes: 1_000_000,
                },
            )
            .expect("shared finality runtime")
        };
        let finalized = |block: &leani_primitives::BlockFrame, slot| {
            FinalityStep::Event(FinalityEvent::Finalized {
                block_number: block.block.number,
                block_hash: block.block.hash,
                beacon_slot: slot,
                beacon_block_root: [2; 32],
            })
        };

        let report = finality(vec![finalized(&chain[2], 2)])
            .run(checkpoint.clone(), CancellationToken::new())
            .await
            .expect("finality run");
        assert!(
            report.failed_processors["hidden-conflict-lane"]
                .contains("resolves finalized hash at 1")
        );
        let state = store
            .processor_runtime_state(conflicting.descriptor())
            .await
            .expect("state");
        assert_eq!(state.state, ProcessorRunState::Failed);
        assert_eq!(state.reason.as_deref(), Some("processor_finality_conflict"));

        // A contradicted lane needs a rebuild: a reset would let finality
        // advance it again.
        assert!(
            store
                .reset_failed_live_lane(conflicting.descriptor())
                .await
                .is_err()
        );
        finality(vec![finalized(&chain[3], 3)])
            .run(checkpoint, CancellationToken::new())
            .await
            .expect("later finality run");
        assert_eq!(
            store
                .finalized_through(conflicting.descriptor())
                .await
                .expect("conflicting finality"),
            None
        );
        assert_eq!(
            store
                .finalized_through(healthy.descriptor())
                .await
                .expect("healthy finality"),
            Some(BlockNumber(3))
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
                block_number: second.block.number,
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
                block_number: third.block.number,
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

    fn verified_finality_checkpoint() -> ConsensusCheckpoint {
        ConsensusCheckpoint {
            beacon_slot: 1,
            beacon_block_root: [1; 32],
            execution_block_hash: BlockHash::ZERO,
            obtained_at_unix_seconds: 1,
            source: "fixture".to_owned(),
        }
    }

    /// Verified finality that emits `steps` for `processors`.
    fn verified_finality(
        store: &SqliteStore,
        processors: &[Arc<dyn Processor>],
        steps: Vec<FinalityStep>,
    ) -> SharedFinalityRuntime {
        SharedFinalityRuntime::new(
            store.clone(),
            Arc::new(ScriptedFinalitySource::new(
                fixture_source_descriptor(
                    "verified-finality",
                    BlockRange::new(BlockNumber(0), BlockNumber(9)).expect("range"),
                ),
                verified_finality_checkpoint(),
                steps,
            )),
            processors.to_vec(),
            SharedFinalityRuntimeConfig {
                minimum_recent_blocks: 1,
                recent_soft_bytes: 1_000_000_000,
                recent_hard_bytes: 2_000_000_000,
            },
        )
        .expect("shared finality runtime")
    }

    fn finalized_at(number: u64, hash: BlockHash) -> FinalityStep {
        FinalityStep::Event(FinalityEvent::Finalized {
            block_number: BlockNumber(number),
            block_hash: hash,
            beacon_slot: number.saturating_add(1),
            beacon_block_root: [2; 32],
        })
    }

    #[tokio::test]
    async fn an_unfinalized_contradiction_restarts_the_lanes_and_heals() {
        let chain = live_blocks(3);
        let counter = Arc::new(BlockLocalCounter::named("contradicted-counter"));
        let lanes: Vec<Arc<dyn Processor>> = vec![counter.clone()];
        let (_directory, store) = store().await;
        run_live(&scripted_live(&store, &lanes, block_events(&chain)))
            .await
            .expect("live run");
        // Verified finality names another block at height 2 than the one the
        // lane followed, for example after the lane stalled across a reorg of
        // an attested head. Waiting cannot change that; a restart can.
        let finalized = BlockHash::new([0xf2; 32]);
        let (readiness, ready) = tokio::sync::watch::channel(true);
        let stopped = tokio::time::timeout(
            Duration::from_secs(10),
            verified_finality(&store, &lanes, vec![finalized_at(2, finalized)])
                .run_resilient_with_readiness(
                    verified_finality_checkpoint(),
                    CancellationToken::new(),
                    readiness,
                ),
        )
        .await
        .expect("a finality contradiction was deferred");
        assert!(
            matches!(
                stopped,
                Err(RuntimeError::FinalityReorg {
                    block: BlockNumber(2),
                    finalized: hash,
                    ..
                }) if hash == finalized
            ),
            "{stopped:?}"
        );
        assert!(!*ready.borrow(), "the stopped lane still reports ready");
        let (canonical, finality) = store
            .canonical_block(ChainId(1), BlockNumber(2))
            .await
            .expect("canonical lookup")
            .expect("canonical block");
        assert_eq!(canonical.hash, chain[2].block.hash);
        assert_ne!(finality, Finality::Finalized, "the contradiction promoted");
        assert_eq!(
            store
                .finalized_through(counter.descriptor())
                .await
                .expect("finalized coverage"),
            None
        );

        // The restart seeds the newly verified finalized anchor, which
        // reverts the retained blocks that contradict it, and undoes their
        // coverage. Finality then proceeds.
        let anchor = BlockRef {
            number: BlockNumber(2),
            hash: finalized,
            parent_hash: BlockHash::ZERO,
            timestamp: 1,
        };
        let restarted = scripted_live(&store, &lanes, Vec::new());
        assert_eq!(
            restarted
                .seed_finalized_anchor(anchor)
                .await
                .expect("seed the anchor")
                .len(),
            4
        );
        restarted
            .reconcile_startup()
            .await
            .expect("startup reconciliation");
        let report = verified_finality(&store, &lanes, vec![finalized_at(2, finalized)])
            .run(verified_finality_checkpoint(), CancellationToken::new())
            .await
            .expect("finality after the restart");
        assert_eq!(report.finalized_through, Some(BlockNumber(2)));
        assert_eq!(
            store
                .coverage_block_by_hash(counter.descriptor(), chain[2].block.hash)
                .await
                .expect("coverage lookup"),
            None,
            "the contradicted block kept its coverage"
        );
    }

    #[tokio::test]
    async fn contradictions_with_finalized_history_halt_and_unfinalized_ones_heal() {
        let counter = Arc::new(BlockLocalCounter::named("history-counter"));
        let lanes: Vec<Arc<dyn Processor>> = vec![counter.clone()];
        let run = |store: &SqliteStore, number: u64, hash: BlockHash| {
            let finality = verified_finality(store, &lanes, vec![finalized_at(number, hash)]);
            async move {
                finality
                    .run(verified_finality_checkpoint(), CancellationToken::new())
                    .await
            }
        };

        // A finalized canonical block with another hash than finality names.
        let (_directory, store) = store().await;
        let chain = live_blocks(2);
        for frame in &chain {
            store.store_recent_frame(frame).await.expect("retain");
        }
        run(&store, 2, chain[2].block.hash)
            .await
            .expect("finalize block 2");
        let halted = run(&store, 2, BlockHash::new([0xf2; 32])).await;
        assert!(
            matches!(
                halted,
                Err(RuntimeError::FinalityContradiction {
                    block: BlockNumber(2),
                    ..
                })
            ),
            "{halted:?}"
        );
        // Seeding such an anchor over it at startup halts too.
        let seeded = scripted_live(&store, &lanes, Vec::new())
            .seed_finalized_anchor(BlockRef {
                number: BlockNumber(2),
                hash: BlockHash::new([0xf2; 32]),
                parent_hash: BlockHash::ZERO,
                timestamp: 1,
            })
            .await;
        assert!(
            matches!(seeded, Err(RuntimeError::FinalityContradiction { .. })),
            "{seeded:?}"
        );

        // A finalized block below the finalized one that is not its ancestor.
        let (_directory, store) = self::store().await;
        for frame in [
            live_fixture(0, BlockHash::ZERO),
            live_fixture(1, BlockHash::ZERO),
        ] {
            store.store_recent_frame(&frame).await.expect("retain");
        }
        store
            .store_canonical_anchor(
                ChainId(1),
                BlockRef {
                    number: BlockNumber(2),
                    hash: BlockHash::new([0xa2; 32]),
                    parent_hash: BlockHash::ZERO,
                    timestamp: 2,
                },
                Finality::Finalized,
            )
            .await
            .expect("seed an anchor");
        let third = live_fixture(3, BlockHash::new([0x33; 32]));
        store.store_recent_frame(&third).await.expect("retain");
        let halted = run(&store, 3, third.block.hash).await;
        assert!(
            matches!(
                halted,
                Err(RuntimeError::FinalityContradiction {
                    block: BlockNumber(3),
                    ..
                })
            ),
            "{halted:?}"
        );

        // An unfinalized block below the finalized one that is not its
        // ancestor heals.
        let (_directory, store) = self::store().await;
        for frame in [
            live_fixture(0, BlockHash::ZERO),
            live_fixture(1, BlockHash::ZERO),
            live_fixture(2, BlockHash::new([0x55; 32])),
        ] {
            store.store_recent_frame(&frame).await.expect("retain");
        }
        let healed = run(&store, 2, live_fixture(2, BlockHash::ZERO).block.hash).await;
        assert!(
            matches!(
                healed,
                Err(RuntimeError::FinalityReorg {
                    block: BlockNumber(2),
                    ..
                })
            ),
            "{healed:?}"
        );
    }

    #[tokio::test]
    async fn a_lane_covering_another_block_at_the_finalized_height_fails_instead_of_deferring() {
        let chain = live_blocks(3);
        let counter = Arc::new(BlockLocalCounter::named("stale-coverage-counter"));
        let lanes: Vec<Arc<dyn Processor>> = vec![counter.clone()];
        let (_directory, store) = store().await;
        for frame in &chain {
            store.store_recent_frame(frame).await.expect("retain");
        }
        // The block-local lane covers another block at height 2 than the
        // canonical one: coverage whose undo never ran.
        let mut stale = chain[2].clone();
        stale.block.hash = BlockHash::new([0xee; 32]);
        for (sequence, frame) in (1..).zip([&chain[0], &chain[1], &stale]) {
            apply_live_frame(&store, counter.as_ref(), frame, sequence).await;
        }
        let report = verified_finality(&store, &lanes, vec![finalized_at(2, chain[2].block.hash)])
            .run(verified_finality_checkpoint(), CancellationToken::new())
            .await
            .expect("finality continues for the others");
        assert_eq!(report.finalized_through, Some(BlockNumber(2)));
        assert!(
            report
                .failed_processors
                .contains_key(counter.descriptor().id.as_str()),
            "the lane deferred: a later anchor would promote its stale coverage by height ({report:?})"
        );
        let lane = store
            .processor_runtime_state(counter.descriptor())
            .await
            .expect("state");
        assert_eq!(lane.state, ProcessorRunState::Failed);
        assert_eq!(lane.reason.as_deref(), Some("processor_finality_conflict"));
        assert_eq!(
            store
                .finalized_through(counter.descriptor())
                .await
                .expect("finalized coverage"),
            None
        );
    }

    #[tokio::test]
    async fn a_branch_reorged_away_during_downtime_is_undone_not_finalized() {
        // The lane followed branch A to block 5 before the node stopped.
        let branch_a = live_blocks(5);
        let counter = Arc::new(BlockLocalCounter::named("downtime-counter"));
        let ledger = Arc::new(OrderedLedgerProcessor::named("downtime-ledger"));
        let lanes: Vec<Arc<dyn Processor>> = vec![counter.clone(), ledger.clone()];
        let (_directory, store) = store().await;
        run_live(&scripted_live(&store, &lanes, block_events(&branch_a)))
            .await
            .expect("live run before the downtime");
        // Meanwhile the network reorged below block 5 and finalized block 8
        // on branch B, which none of the retained blocks links to.
        let anchor = BlockRef {
            number: BlockNumber(8),
            hash: BlockHash::new([0xb8; 32]),
            parent_hash: BlockHash::ZERO,
            timestamp: 1,
        };
        let restarted = scripted_live(&store, &lanes, Vec::new());
        let reverted = restarted
            .seed_finalized_anchor(anchor)
            .await
            .expect("seed the verified finalized anchor");
        assert_eq!(
            reverted
                .iter()
                .map(|block| block.number.0)
                .collect::<Vec<_>>(),
            [5, 4, 3, 2, 1, 0],
            "branch A is not reverted"
        );
        let report = restarted
            .reconcile_startup()
            .await
            .expect("startup reconciliation");
        // Branch A is undone for every lane, as a reorg undoes it...
        for lane in &lanes {
            assert_eq!(
                report.processors[lane.descriptor().id.as_str()].reverted,
                6,
                "{}",
                lane.descriptor().id
            );
            for frame in &branch_a {
                assert_eq!(
                    store
                        .coverage_block_by_hash(lane.descriptor(), frame.block.hash)
                        .await
                        .expect("coverage lookup"),
                    None,
                    "{} still covers block {}",
                    lane.descriptor().id,
                    frame.block.number.0
                );
            }
        }
        // ...and finality through the anchor finalizes none of it.
        let finality = verified_finality(&store, &lanes, vec![finalized_at(8, anchor.hash)])
            .run(verified_finality_checkpoint(), CancellationToken::new())
            .await
            .expect("finality run");
        assert_eq!(finality.finalized_through, Some(BlockNumber(8)));
        for lane in &lanes {
            assert_eq!(
                store
                    .finalized_through(lane.descriptor())
                    .await
                    .expect("finalized coverage"),
                None,
                "{}",
                lane.descriptor().id
            );
        }
        for frame in &branch_a {
            assert!(
                store
                    .canonical_block(ChainId(1), frame.block.number)
                    .await
                    .expect("canonical lookup")
                    .is_none(),
                "branch A block {} stayed canonical",
                frame.block.number.0
            );
        }
    }
}
