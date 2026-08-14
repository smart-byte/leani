//! Shared, bounded historical source acquisition.
//!
//! The coordinator deduplicates exact in-flight physical source chunks while
//! keeping processor execution, coverage, checkpoints, and failures
//! independent. It intentionally does not compose different capability or
//! predicate shapes; that optimization is layered on top of this exact
//! single-flight baseline.

use std::{
    collections::{HashMap, VecDeque},
    pin::Pin,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
};

use futures::{Stream, StreamExt};
use leani_primitives::{
    BlockFrame, BlockHash, BlockRange, CapabilitySet, ChainId, Finality, LogFieldSet,
};
use leani_source_api::{
    DataRequest, FieldProjection, FilterSet, HistorySource, SourceBudget, SourceChunk, SourceError,
    VerificationPolicy,
};
use serde::Serialize;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

/// Shared-frame stream returned to one historical processor runtime.
pub(crate) type HistoricalMaterialStream =
    Pin<Box<dyn Stream<Item = Result<HistoricalMaterialFrame, SourceError>> + Send + 'static>>;

/// Source-neutral material semantics, excluding the requested range.
///
/// Equality is deliberately conservative. Exact coalescing never assumes
/// predicate containment or compatible projections.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct MaterialShape {
    pub chain_id: ChainId,
    pub required: CapabilitySet,
    pub log_fields: LogFieldSet,
    pub allow_filtered: bool,
    pub projection: FieldProjection,
    pub filters: FilterSet,
    pub minimum_finality: Finality,
    pub verification_policy: VerificationPolicy,
}

impl From<&DataRequest> for MaterialShape {
    fn from(request: &DataRequest) -> Self {
        Self {
            chain_id: request.chain_id,
            required: request.required,
            log_fields: request.log_fields,
            allow_filtered: request.allow_filtered,
            projection: request.projection.clone(),
            filters: request.filters.clone(),
            minimum_finality: request.minimum_finality,
            verification_policy: request.verification_policy,
        }
    }
}

/// Ordered source-set identity retained by one historical runtime.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct HistoricalSourcePolicy {
    pub sources: Vec<HistoricalSourcePolicyEntry>,
}

impl HistoricalSourcePolicy {
    #[must_use]
    pub fn from_sources(sources: &[Arc<dyn HistorySource>]) -> Self {
        Self {
            sources: sources
                .iter()
                .map(|source| {
                    let descriptor = source.descriptor();
                    HistoricalSourcePolicyEntry {
                        id: descriptor.id.to_string(),
                        priority: descriptor.priority,
                        schema_version: descriptor.schema_version.clone(),
                        acquisition_identity: source.acquisition_identity(),
                    }
                })
                .collect(),
        }
    }
}

/// Stable source identity needed for exact acquisition compatibility.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct HistoricalSourcePolicyEntry {
    pub id: String,
    pub priority: u16,
    pub schema_version: String,
    pub acquisition_identity: Vec<u8>,
}

/// Exact compatibility identity for one physical historical acquisition.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct AcquisitionShape {
    pub material: MaterialShape,
    pub source_policy: HistoricalSourcePolicy,
    pub source_id: String,
    pub range: BlockRange,
    pub partition: Vec<u8>,
    pub schema_version: String,
    pub expected_parent: Option<BlockHash>,
    pub budget: AcquisitionBudgetShape,
}

#[derive(Serialize)]
struct AcquisitionCompatibilityShape {
    material: MaterialShape,
    source_policy: HistoricalSourcePolicy,
    source_id: String,
    partition: Vec<u8>,
    schema_version: String,
}

/// Resource policy is part of exact single-flight compatibility.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct AcquisitionBudgetShape {
    pub max_input_bytes: u64,
    pub max_frame_bytes: u64,
    pub max_frames: u64,
    pub max_buffered_frames: usize,
    pub max_in_flight_requests: usize,
    pub temporary_disk_bytes: u64,
}

impl From<SourceBudget> for AcquisitionBudgetShape {
    fn from(budget: SourceBudget) -> Self {
        Self {
            max_input_bytes: budget.max_input_bytes,
            max_frame_bytes: budget.max_frame_bytes,
            max_frames: budget.max_frames,
            max_buffered_frames: budget.max_buffered_frames,
            max_in_flight_requests: budget.max_in_flight_requests,
            temporary_disk_bytes: budget.temporary_disk_bytes,
        }
    }
}

/// Hard bounds for one chain-level material coordinator.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct HistoricalMaterialCoordinatorConfig {
    /// Whether compatible requests are only observed or actually shared.
    pub mode: HistoricalMaterialCoordinatorMode,
    /// Aggregate retained frame bytes across active acquisitions.
    pub memory_bytes: u64,
    /// Maximum unacknowledged frames retained by one acquisition.
    pub maximum_buffered_frames_per_acquisition: usize,
    /// Coordinator target before a source-specific efficient span is applied.
    pub minimum_physical_chunk_blocks: u64,
    /// Maximum physical blocks divided by logical blocks for one demand.
    pub maximum_overfetch_ratio: f64,
}

impl Default for HistoricalMaterialCoordinatorConfig {
    fn default() -> Self {
        Self {
            mode: HistoricalMaterialCoordinatorMode::Enabled,
            memory_bytes: 256 * 1_024 * 1_024,
            maximum_buffered_frames_per_acquisition: 128,
            minimum_physical_chunk_blocks: 128,
            maximum_overfetch_ratio: 1.25,
        }
    }
}

/// Coordinator execution policy used for staged rollout.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum HistoricalMaterialCoordinatorMode {
    /// Report compatible active requests while keeping physical reads isolated.
    Observe,
    /// Coalesce compatible exact and overlapping requests.
    #[default]
    Enabled,
}

impl HistoricalMaterialCoordinatorConfig {
    fn validate(self) -> Result<Self, SourceError> {
        if self.memory_bytes == 0
            || self.maximum_buffered_frames_per_acquisition == 0
            || self.minimum_physical_chunk_blocks == 0
            || !self.maximum_overfetch_ratio.is_finite()
            || self.maximum_overfetch_ratio < 1.0
        {
            return Err(SourceError::InvalidBudget);
        }
        Ok(self)
    }
}

/// Low-cardinality process snapshot for acquisition observability.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub struct HistoricalMaterialSnapshot {
    pub acquisitions_started: u64,
    pub requests_coalesced: u64,
    pub requests_coalescible: u64,
    pub physical_frames: u64,
    pub physical_bytes: u64,
    pub overfetched_frames: u64,
    pub overfetched_bytes: u64,
    pub logical_frame_deliveries: u64,
    pub active_acquisitions: u64,
    pub buffered_bytes: u64,
}

/// One participant in a deterministic automatic-demand registration batch.
#[derive(Clone, Debug)]
pub struct HistoricalMaterialStartupPermit {
    batch: Arc<StartupBatchState>,
    planned: Arc<AtomicBool>,
    arrived: Arc<AtomicBool>,
}

impl HistoricalMaterialStartupPermit {
    pub(crate) fn gate(&self) -> HistoricalMaterialStartupGate {
        HistoricalMaterialStartupGate {
            batch: self.batch.clone(),
            phase: StartupGatePhase::Acquisition,
        }
    }

    pub(crate) fn arrive(&self) -> HistoricalMaterialStartupGate {
        if !self.arrived.swap(true, Ordering::AcqRel)
            && self
                .batch
                .acquisition_remaining
                .fetch_sub(1, Ordering::AcqRel)
                == 1
        {
            self.batch.acquisition_completed.notify_waiters();
        }
        self.gate()
    }

    fn planning_gate(&self) -> HistoricalMaterialStartupGate {
        HistoricalMaterialStartupGate {
            batch: self.batch.clone(),
            phase: StartupGatePhase::Planning,
        }
    }

    fn arrive_planning(&self) -> HistoricalMaterialStartupGate {
        if !self.planned.swap(true, Ordering::AcqRel)
            && self.batch.planning_remaining.fetch_sub(1, Ordering::AcqRel) == 1
        {
            self.batch.planning_completed.notify_waiters();
        }
        self.planning_gate()
    }
}

impl Drop for HistoricalMaterialStartupPermit {
    fn drop(&mut self) {
        let _ = self.arrive_planning();
        let _ = self.arrive();
    }
}

#[derive(Clone, Debug)]
pub(crate) struct HistoricalMaterialStartupGate {
    batch: Arc<StartupBatchState>,
    phase: StartupGatePhase,
}

impl HistoricalMaterialStartupGate {
    async fn wait(&self) {
        loop {
            let (remaining, completed) = match self.phase {
                StartupGatePhase::Planning => (
                    &self.batch.planning_remaining,
                    &self.batch.planning_completed,
                ),
                StartupGatePhase::Acquisition => (
                    &self.batch.acquisition_remaining,
                    &self.batch.acquisition_completed,
                ),
            };
            let completed = completed.notified();
            if remaining.load(Ordering::Acquire) == 0 {
                return;
            }
            completed.await;
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum StartupGatePhase {
    Planning,
    Acquisition,
}

#[derive(Debug)]
struct StartupBatchState {
    planning_remaining: AtomicUsize,
    planning_completed: Notify,
    acquisition_remaining: AtomicUsize,
    acquisition_completed: Notify,
    demands: Mutex<HashMap<Vec<u8>, Vec<BlockRange>>>,
}

#[derive(Serialize)]
struct StartupDemandShape {
    material: MaterialShape,
    source_policy: HistoricalSourcePolicy,
    source_id: String,
    source_schema_version: String,
    budget: AcquisitionBudgetShape,
}

/// One immutable frame delivered to one processor runtime.
#[derive(Debug)]
pub struct HistoricalMaterialFrame {
    frame: Arc<BlockFrame>,
    acquisition_id: Option<u64>,
    physical_source: bool,
    coalesced: bool,
    sequence: Option<u64>,
    subscriber_id: Option<u64>,
    acquisition: Option<Weak<Acquisition>>,
    _retention: Option<Arc<MemoryReservation>>,
}

impl HistoricalMaterialFrame {
    fn standalone(frame: BlockFrame) -> Self {
        Self {
            frame: Arc::new(frame),
            acquisition_id: None,
            physical_source: true,
            coalesced: false,
            sequence: None,
            subscriber_id: None,
            acquisition: None,
            _retention: None,
        }
    }

    fn retained(frame: BlockFrame) -> Self {
        Self {
            frame: Arc::new(frame),
            acquisition_id: None,
            physical_source: false,
            coalesced: false,
            sequence: None,
            subscriber_id: None,
            acquisition: None,
            _retention: None,
        }
    }

    /// Borrow the normalized source frame without copying its payload.
    #[must_use]
    pub fn frame(&self) -> &BlockFrame {
        self.frame.as_ref()
    }

    /// Shared acquisition identity, absent for an uncoordinated runtime.
    #[must_use]
    pub const fn acquisition_id(&self) -> Option<u64> {
        self.acquisition_id
    }

    /// Whether this processor delivery owns physical-byte attribution.
    #[must_use]
    pub const fn is_physical_source_delivery(&self) -> bool {
        self.physical_source
    }

    /// Whether another subscriber owns this frame's physical attribution.
    #[must_use]
    pub const fn is_coalesced_delivery(&self) -> bool {
        self.coalesced
    }

    /// Advance this subscriber's durable material watermark.
    ///
    /// A missing acquisition means this frame came from the legacy direct
    /// source path and needs no coordinator acknowledgement.
    pub fn acknowledge(&self) {
        let (Some(sequence), Some(subscriber_id), Some(acquisition)) = (
            self.sequence,
            self.subscriber_id,
            self.acquisition.as_ref().and_then(Weak::upgrade),
        ) else {
            return;
        };
        acquisition.acknowledge(subscriber_id, sequence);
    }
}

/// Chain-level exact single-flight coordinator.
#[derive(Clone)]
pub struct HistoricalMaterialCoordinator {
    inner: Arc<CoordinatorInner>,
}

impl std::fmt::Debug for HistoricalMaterialCoordinator {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HistoricalMaterialCoordinator")
            .field("config", &self.inner.config)
            .field("maximum_active_chunks", &self.inner.maximum_active_chunks)
            .field("snapshot", &self.snapshot())
            .finish()
    }
}

impl HistoricalMaterialCoordinator {
    /// Construct an empty coordinator.
    ///
    /// # Errors
    ///
    /// Returns an error for zero memory or frame bounds.
    pub fn new(config: HistoricalMaterialCoordinatorConfig) -> Result<Self, SourceError> {
        let active_chunks = Arc::new(Semaphore::new(super::HISTORICAL_MAX_ACTIVE_CHUNKS));
        Self::new_with_active_chunks(config, active_chunks, super::HISTORICAL_MAX_ACTIVE_CHUNKS)
    }

    /// Construct a coordinator whose physical acquisitions share the node's
    /// history pipeline chunk budget.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid material coordinator bounds.
    pub fn new_with_pipeline_budget(
        config: HistoricalMaterialCoordinatorConfig,
        pipeline_budget: &super::HistoricalPipelineBudget,
    ) -> Result<Self, SourceError> {
        Self::new_with_active_chunks(
            config,
            pipeline_budget.active_chunks.clone(),
            pipeline_budget.maximum_active_chunks,
        )
    }

    fn new_with_active_chunks(
        config: HistoricalMaterialCoordinatorConfig,
        active_chunks: Arc<Semaphore>,
        maximum_active_chunks: usize,
    ) -> Result<Self, SourceError> {
        let config = config.validate()?;
        Ok(Self {
            inner: Arc::new(CoordinatorInner {
                config,
                active_chunks,
                maximum_active_chunks,
                acquisitions: Mutex::new(HashMap::new()),
                memory: Mutex::new(0),
                memory_changed: Notify::new(),
                next_acquisition_id: AtomicU64::new(1),
                next_subscriber_id: AtomicU64::new(1),
                acquisitions_started: AtomicU64::new(0),
                requests_coalesced: AtomicU64::new(0),
                requests_coalescible: AtomicU64::new(0),
                physical_frames: AtomicU64::new(0),
                physical_bytes: AtomicU64::new(0),
                overfetched_frames: AtomicU64::new(0),
                overfetched_bytes: AtomicU64::new(0),
                logical_frame_deliveries: AtomicU64::new(0),
                active_acquisitions: AtomicU64::new(0),
            }),
        })
    }

    /// Bound one runtime's speculative acquisition window so it cannot queue
    /// later chunks ahead of its own next required chunk. Existing jobs retain
    /// their active slots; a newcomer opens one chunk when all slots are busy.
    pub(crate) fn acquisition_window(&self, requested: usize) -> usize {
        requested.max(1).min(
            self.inner
                .active_chunks
                .available_permits()
                .max(1)
                .min(self.inner.maximum_active_chunks),
        )
    }

    /// Expand one logical demand toward an efficient physical source span.
    ///
    /// Expansion is deterministic, forward-preferring, clipped to a declared
    /// source range, and bounded by both the source frame budget and configured
    /// overfetch ratio. Observe mode retains the legacy exact planning path.
    #[must_use]
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_precision_loss,
        clippy::cast_sign_loss
    )]
    pub fn physical_request(
        &self,
        source: &dyn HistorySource,
        request: &DataRequest,
        budget: SourceBudget,
    ) -> DataRequest {
        if self.inner.config.mode == HistoricalMaterialCoordinatorMode::Observe {
            return request.clone();
        }
        let Some(available) = source.descriptor().range else {
            return request.clone();
        };
        let logical_blocks = request.range.len();
        let ratio_bound = ((logical_blocks as f64) * self.inner.config.maximum_overfetch_ratio)
            .floor()
            .min(u64::MAX as f64) as u64;
        let efficient_blocks = source
            .minimum_efficient_blocks()
            .unwrap_or(1)
            .max(self.inner.config.minimum_physical_chunk_blocks);
        let target_blocks = efficient_blocks
            .min(ratio_bound)
            .min(budget.max_frames.max(logical_blocks));
        if target_blocks <= logical_blocks {
            return request.clone();
        }

        let aligned_start = request
            .range
            .start()
            .0
            .checked_div(target_blocks)
            .unwrap_or(0)
            .saturating_mul(target_blocks);
        let aligned_end = request
            .range
            .end()
            .0
            .checked_div(target_blocks)
            .unwrap_or(0)
            .saturating_add(1)
            .saturating_mul(target_blocks)
            .saturating_sub(1);
        if aligned_start >= available.start().0
            && aligned_end <= available.end().0
            && aligned_end.saturating_sub(aligned_start).saturating_add(1)
                <= ratio_bound.min(budget.max_frames.max(logical_blocks))
            && let Ok(range) = BlockRange::new(aligned_start.into(), aligned_end.into())
        {
            let mut physical = request.clone();
            physical.range = range;
            return physical;
        }

        let extra = target_blocks.saturating_sub(logical_blocks);
        let forward = available
            .end()
            .0
            .saturating_sub(request.range.end().0)
            .min(extra);
        let available_start = available.start().0;
        let backward = request
            .range
            .start()
            .0
            .saturating_sub(available_start)
            .min(extra.saturating_sub(forward));
        let Ok(range) = BlockRange::new(
            request.range.start().0.saturating_sub(backward).into(),
            request.range.end().0.saturating_add(forward).into(),
        ) else {
            return request.clone();
        };
        let mut physical = request.clone();
        physical.range = range;
        physical
    }

    /// Register one automatic demand before source planning and normalize its
    /// physical request against the complete compatible startup demand set.
    pub(crate) async fn physical_request_after_startup_registration(
        &self,
        source_policy: HistoricalSourcePolicy,
        source: &dyn HistorySource,
        request: &DataRequest,
        budget: SourceBudget,
        startup_permit: &HistoricalMaterialStartupPermit,
        cancellation: &CancellationToken,
    ) -> Result<DataRequest, SourceError> {
        let shape = StartupDemandShape {
            material: MaterialShape::from(request),
            source_policy,
            source_id: source.descriptor().id.to_string(),
            source_schema_version: source.descriptor().schema_version.clone(),
            budget: budget.into(),
        };
        let key = serde_json::to_vec(&shape)
            .map_err(|error| SourceError::InvalidPlan(error.to_string()))?;
        {
            let mut demands = startup_permit
                .batch
                .demands
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            demands.entry(key.clone()).or_default().push(request.range);
        }
        let startup_gate = startup_permit.arrive_planning();
        tokio::select! {
            () = startup_gate.wait() => {}
            () = cancellation.cancelled() => {
                let mut demands = startup_permit
                    .batch
                    .demands
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if let Some(ranges) = demands.get_mut(&key)
                    && let Some(index) = ranges.iter().position(|range| *range == request.range)
                {
                    ranges.swap_remove(index);
                }
                return Err(SourceError::Cancelled);
            }
        }
        if self.inner.config.mode == HistoricalMaterialCoordinatorMode::Observe {
            return Ok(request.clone());
        }
        let aggregate_range = {
            let demands = startup_permit
                .batch
                .demands
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            connected_demand_range(demands.get(&key).map_or(&[], Vec::as_slice), request.range)
        };
        let mut aggregate = request.clone();
        aggregate.range = aggregate_range;
        Ok(self.physical_request(source, &aggregate, budget))
    }

    /// Create one permit per automatic job that must register or exit before
    /// startup source reads begin.
    #[must_use]
    pub fn startup_batch(&self, participants: usize) -> Vec<HistoricalMaterialStartupPermit> {
        let batch = Arc::new(StartupBatchState {
            planning_remaining: AtomicUsize::new(participants),
            planning_completed: Notify::new(),
            acquisition_remaining: AtomicUsize::new(participants),
            acquisition_completed: Notify::new(),
            demands: Mutex::new(HashMap::new()),
        });
        (0..participants)
            .map(|_| HistoricalMaterialStartupPermit {
                batch: batch.clone(),
                planned: Arc::new(AtomicBool::new(false)),
                arrived: Arc::new(AtomicBool::new(false)),
            })
            .collect()
    }

    /// Open the missing parts of one physical chunk and join compatible
    /// overlapping acquisitions for the rest.
    ///
    /// The first producer starts after one scheduler yield so startup jobs
    /// spawned in the same batch can register without a wall-clock settling
    /// delay.
    ///
    /// # Errors
    ///
    /// Returns an error when the immutable acquisition identity cannot be
    /// encoded.
    #[allow(clippy::needless_pass_by_value, clippy::too_many_lines)]
    pub fn open(
        &self,
        source_policy: HistoricalSourcePolicy,
        source: Arc<dyn HistorySource>,
        request: &DataRequest,
        chunk: SourceChunk,
        budget: SourceBudget,
        cancellation: CancellationToken,
    ) -> Result<HistoricalMaterialStream, SourceError> {
        self.open_inner(
            source_policy,
            source,
            request,
            chunk,
            budget,
            cancellation,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn open_after_startup_registration(
        &self,
        source_policy: HistoricalSourcePolicy,
        source: Arc<dyn HistorySource>,
        request: &DataRequest,
        chunk: SourceChunk,
        budget: SourceBudget,
        cancellation: CancellationToken,
        startup_gate: HistoricalMaterialStartupGate,
    ) -> Result<HistoricalMaterialStream, SourceError> {
        self.open_inner(
            source_policy,
            source,
            request,
            chunk,
            budget,
            cancellation,
            Some(startup_gate),
        )
    }

    #[allow(
        clippy::needless_pass_by_value,
        clippy::too_many_arguments,
        clippy::too_many_lines
    )]
    fn open_inner(
        &self,
        source_policy: HistoricalSourcePolicy,
        source: Arc<dyn HistorySource>,
        request: &DataRequest,
        chunk: SourceChunk,
        budget: SourceBudget,
        cancellation: CancellationToken,
        startup_gate: Option<HistoricalMaterialStartupGate>,
    ) -> Result<HistoricalMaterialStream, SourceError> {
        let logical_range = intersect_ranges(request.range, chunk.range).ok_or_else(|| {
            SourceError::InvalidPlan(
                "physical source chunk does not intersect its logical demand".to_owned(),
            )
        })?;
        let shape = AcquisitionCompatibilityShape {
            material: MaterialShape::from(request),
            source_policy,
            source_id: source.descriptor().id.to_string(),
            partition: source.coalescing_partition_identity(&chunk),
            schema_version: chunk.schema_version.clone(),
        };
        let key = serde_json::to_vec(&shape)
            .map_err(|error| SourceError::InvalidPlan(error.to_string()))?;
        if self.inner.config.mode == HistoricalMaterialCoordinatorMode::Observe {
            return Ok(self.open_observed(
                key,
                source,
                chunk,
                logical_range,
                budget,
                cancellation,
                startup_gate,
            ));
        }
        let mut subscriptions = Vec::new();
        let mut starts = Vec::new();
        {
            let mut acquisitions = self
                .inner
                .acquisitions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let entries = acquisitions.entry(key).or_default();
            entries.retain(|candidate| candidate.strong_count() != 0);
            let has_joinable_physical_overlap = entries
                .iter()
                .filter_map(Weak::upgrade)
                .any(|candidate| candidate.has_joinable_overlap(chunk.range));
            if !has_joinable_physical_overlap {
                let subscriber_id = self
                    .inner
                    .next_subscriber_id
                    .fetch_add(1, Ordering::Relaxed);
                let id = self
                    .inner
                    .next_acquisition_id
                    .fetch_add(1, Ordering::Relaxed);
                let acquisition = Arc::new(Acquisition::new(
                    id,
                    Arc::downgrade(&self.inner),
                    self.inner.config.maximum_buffered_frames_per_acquisition,
                    chunk.range,
                    logical_range,
                    subscriber_id,
                ));
                entries.push(Arc::downgrade(&acquisition));
                self.inner
                    .acquisitions_started
                    .fetch_add(1, Ordering::Relaxed);
                subscriptions.push(MaterialSubscription {
                    acquisition: acquisition.clone(),
                    subscriber_id,
                    cancellation: cancellation.clone(),
                    terminal: false,
                });
                starts.push((acquisition, chunk.clone()));
            }
            let mut cursor = logical_range.start().0;
            while has_joinable_physical_overlap && cursor <= logical_range.end().0 {
                let subscriber_id = self
                    .inner
                    .next_subscriber_id
                    .fetch_add(1, Ordering::Relaxed);
                let mut joined = None;
                for acquisition in entries.iter().filter_map(Weak::upgrade) {
                    if let Some(end) =
                        acquisition.try_register(subscriber_id, cursor, logical_range.end().0)
                    {
                        if joined
                            .as_ref()
                            .is_none_or(|(_, joined_end)| end > *joined_end)
                        {
                            if let Some((previous, _)) = joined.replace((acquisition, end)) {
                                previous.unregister(subscriber_id);
                            }
                        } else {
                            acquisition.unregister(subscriber_id);
                        }
                    }
                }
                if let Some((acquisition, end)) = joined {
                    self.inner
                        .requests_coalesced
                        .fetch_add(1, Ordering::Relaxed);
                    subscriptions.push(MaterialSubscription {
                        acquisition,
                        subscriber_id,
                        cancellation: cancellation.clone(),
                        terminal: false,
                    });
                    if end == u64::MAX {
                        break;
                    }
                    cursor = end.saturating_add(1);
                    continue;
                }

                let next_joinable_start = entries
                    .iter()
                    .filter_map(Weak::upgrade)
                    .filter_map(|acquisition| {
                        acquisition.joinable_start_after(cursor, logical_range.end().0)
                    })
                    .min();
                let end = next_joinable_start
                    .map_or(logical_range.end().0, |start| start.saturating_sub(1));
                let range = BlockRange::new(cursor.into(), end.into())
                    .map_err(|error| SourceError::InvalidPlan(error.to_string()))?;
                let physical_chunk = source.slice_chunk(&chunk, range)?;
                let id = self
                    .inner
                    .next_acquisition_id
                    .fetch_add(1, Ordering::Relaxed);
                let acquisition = Arc::new(Acquisition::new(
                    id,
                    Arc::downgrade(&self.inner),
                    self.inner.config.maximum_buffered_frames_per_acquisition,
                    range,
                    range,
                    subscriber_id,
                ));
                entries.push(Arc::downgrade(&acquisition));
                self.inner
                    .acquisitions_started
                    .fetch_add(1, Ordering::Relaxed);
                subscriptions.push(MaterialSubscription {
                    acquisition: acquisition.clone(),
                    subscriber_id,
                    cancellation: cancellation.clone(),
                    terminal: false,
                });
                starts.push((acquisition, physical_chunk));
                if end == u64::MAX {
                    break;
                }
                cursor = end.saturating_add(1);
            }
        }
        for (acquisition, physical_chunk) in starts {
            let producer = acquisition.clone();
            let source = source.clone();
            let startup_gate = startup_gate.clone();
            // Reserve immediately when capacity is available. Deferring every
            // acquisition to independently race on the semaphore lets a later
            // chunk fill its bounded queue while the next in-order chunk is
            // still waiting for a permit, which can deadlock the reorder
            // window. Open order is the runtime's consumption order.
            let active_permit = self.inner.active_chunks.clone().try_acquire_owned().ok();
            tokio::spawn(async move {
                tokio::task::yield_now().await;
                if let Some(startup_gate) = startup_gate {
                    tokio::select! {
                        () = startup_gate.wait() => {}
                        () = producer.cancellation.cancelled() => return,
                    }
                }
                producer
                    .produce(source, physical_chunk, budget, active_permit)
                    .await;
            });
        }
        let streams = subscriptions
            .into_iter()
            .map(material_subscription_stream)
            .collect::<Vec<_>>();
        Ok(futures::stream::iter(streams).flatten().boxed())
    }

    #[allow(clippy::too_many_arguments)]
    fn open_observed(
        &self,
        key: Vec<u8>,
        source: Arc<dyn HistorySource>,
        chunk: SourceChunk,
        logical_range: BlockRange,
        budget: SourceBudget,
        cancellation: CancellationToken,
        startup_gate: Option<HistoricalMaterialStartupGate>,
    ) -> HistoricalMaterialStream {
        let subscriber_id = self
            .inner
            .next_subscriber_id
            .fetch_add(1, Ordering::Relaxed);
        let acquisition = {
            let mut acquisitions = self
                .inner
                .acquisitions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let entries = acquisitions.entry(key).or_default();
            entries.retain(|candidate| candidate.strong_count() != 0);
            if entries
                .iter()
                .filter_map(Weak::upgrade)
                .any(|candidate| candidate.has_joinable_overlap(chunk.range))
            {
                self.inner
                    .requests_coalescible
                    .fetch_add(1, Ordering::Relaxed);
            }
            let id = self
                .inner
                .next_acquisition_id
                .fetch_add(1, Ordering::Relaxed);
            let acquisition = Arc::new(Acquisition::new(
                id,
                Arc::downgrade(&self.inner),
                self.inner.config.maximum_buffered_frames_per_acquisition,
                chunk.range,
                logical_range,
                subscriber_id,
            ));
            entries.push(Arc::downgrade(&acquisition));
            self.inner
                .acquisitions_started
                .fetch_add(1, Ordering::Relaxed);
            acquisition
        };
        let producer = acquisition.clone();
        let active_permit = self.inner.active_chunks.clone().try_acquire_owned().ok();
        tokio::spawn(async move {
            tokio::task::yield_now().await;
            if let Some(startup_gate) = startup_gate {
                tokio::select! {
                    () = startup_gate.wait() => {}
                    () = producer.cancellation.cancelled() => return,
                }
            }
            producer.produce(source, chunk, budget, active_permit).await;
        });
        material_subscription_stream(MaterialSubscription {
            acquisition,
            subscriber_id,
            cancellation,
            terminal: false,
        })
    }

    /// Wrap a direct source stream in the same frame type.
    #[must_use]
    pub fn standalone(stream: leani_source_api::BlockFrameStream) -> HistoricalMaterialStream {
        stream
            .map(|item| item.map(HistoricalMaterialFrame::standalone))
            .boxed()
    }

    /// Wrap frames already retained by the node as non-physical reuse.
    pub(crate) fn retained(frames: Vec<BlockFrame>) -> HistoricalMaterialStream {
        futures::stream::iter(
            frames
                .into_iter()
                .map(|frame| Ok(HistoricalMaterialFrame::retained(frame))),
        )
        .boxed()
    }

    #[must_use]
    pub fn snapshot(&self) -> HistoricalMaterialSnapshot {
        HistoricalMaterialSnapshot {
            acquisitions_started: self.inner.acquisitions_started.load(Ordering::Relaxed),
            requests_coalesced: self.inner.requests_coalesced.load(Ordering::Relaxed),
            requests_coalescible: self.inner.requests_coalescible.load(Ordering::Relaxed),
            physical_frames: self.inner.physical_frames.load(Ordering::Relaxed),
            physical_bytes: self.inner.physical_bytes.load(Ordering::Relaxed),
            overfetched_frames: self.inner.overfetched_frames.load(Ordering::Relaxed),
            overfetched_bytes: self.inner.overfetched_bytes.load(Ordering::Relaxed),
            logical_frame_deliveries: self.inner.logical_frame_deliveries.load(Ordering::Relaxed),
            active_acquisitions: self.inner.active_acquisitions.load(Ordering::Relaxed),
            buffered_bytes: *self
                .inner
                .memory
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        }
    }
}

struct CoordinatorInner {
    config: HistoricalMaterialCoordinatorConfig,
    active_chunks: Arc<Semaphore>,
    maximum_active_chunks: usize,
    acquisitions: Mutex<HashMap<Vec<u8>, Vec<Weak<Acquisition>>>>,
    memory: Mutex<u64>,
    memory_changed: Notify,
    next_acquisition_id: AtomicU64,
    next_subscriber_id: AtomicU64,
    acquisitions_started: AtomicU64,
    requests_coalesced: AtomicU64,
    requests_coalescible: AtomicU64,
    physical_frames: AtomicU64,
    physical_bytes: AtomicU64,
    overfetched_frames: AtomicU64,
    overfetched_bytes: AtomicU64,
    logical_frame_deliveries: AtomicU64,
    active_acquisitions: AtomicU64,
}

struct ActiveAcquisitionGuard {
    coordinator: Arc<CoordinatorInner>,
    _permit: OwnedSemaphorePermit,
}

impl ActiveAcquisitionGuard {
    fn new(coordinator: Arc<CoordinatorInner>, permit: OwnedSemaphorePermit) -> Self {
        coordinator
            .active_acquisitions
            .fetch_add(1, Ordering::Relaxed);
        Self {
            coordinator,
            _permit: permit,
        }
    }
}

impl Drop for ActiveAcquisitionGuard {
    fn drop(&mut self) {
        self.coordinator
            .active_acquisitions
            .fetch_sub(1, Ordering::Relaxed);
    }
}

impl CoordinatorInner {
    async fn reserve(
        self: &Arc<Self>,
        bytes: u64,
        cancellation: &CancellationToken,
    ) -> Result<Arc<MemoryReservation>, SourceError> {
        if bytes > self.config.memory_bytes {
            return Err(SourceError::BudgetExceeded {
                resource: "historical_material_memory",
                limit: self.config.memory_bytes,
                observed: bytes,
            });
        }
        loop {
            {
                let mut retained = self
                    .memory
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if retained.saturating_add(bytes) <= self.config.memory_bytes {
                    *retained = retained.saturating_add(bytes);
                    return Ok(Arc::new(MemoryReservation {
                        coordinator: Arc::downgrade(self),
                        bytes,
                    }));
                }
            }
            tokio::select! {
                () = cancellation.cancelled() => return Err(SourceError::Cancelled),
                () = self.memory_changed.notified() => {}
            }
        }
    }
}

struct MemoryReservation {
    coordinator: Weak<CoordinatorInner>,
    bytes: u64,
}

impl std::fmt::Debug for MemoryReservation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MemoryReservation")
            .field("bytes", &self.bytes)
            .finish_non_exhaustive()
    }
}

impl Drop for MemoryReservation {
    fn drop(&mut self) {
        let Some(coordinator) = self.coordinator.upgrade() else {
            return;
        };
        let mut retained = coordinator
            .memory
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *retained = retained.saturating_sub(self.bytes);
        drop(retained);
        coordinator.memory_changed.notify_waiters();
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NextFrameDisposition {
    Retain,
    Overfetch,
    Abandoned,
}

struct Acquisition {
    id: u64,
    coordinator: Weak<CoordinatorInner>,
    maximum_buffered_frames: usize,
    range: BlockRange,
    subscribers_registered: AtomicU64,
    state: Mutex<AcquisitionState>,
    changed: Notify,
    cancellation: CancellationToken,
}

impl Acquisition {
    fn new(
        id: u64,
        coordinator: Weak<CoordinatorInner>,
        maximum_buffered_frames: usize,
        range: BlockRange,
        first_range: BlockRange,
        first_subscriber: u64,
    ) -> Self {
        let mut subscribers = HashMap::new();
        subscribers.insert(
            first_subscriber,
            SubscriberProgress::new(
                first_range.start().0.saturating_sub(range.start().0),
                first_range
                    .end()
                    .0
                    .saturating_sub(range.start().0)
                    .saturating_add(1),
            ),
        );
        Self {
            id,
            coordinator,
            maximum_buffered_frames,
            range,
            subscribers_registered: AtomicU64::new(1),
            state: Mutex::new(AcquisitionState {
                base_sequence: 0,
                produced: 0,
                frames: VecDeque::new(),
                subscribers,
                requested_ranges: vec![(
                    first_range.start().0.saturating_sub(range.start().0),
                    first_range
                        .end()
                        .0
                        .saturating_sub(range.start().0)
                        .saturating_add(1),
                )],
                terminal: None,
            }),
            changed: Notify::new(),
            cancellation: CancellationToken::new(),
        }
    }

    fn try_register(
        &self,
        subscriber_id: u64,
        requested_start: u64,
        requested_end: u64,
    ) -> Option<u64> {
        if requested_start < self.range.start().0 || requested_start > self.range.end().0 {
            return None;
        }
        let end = requested_end.min(self.range.end().0);
        let start_sequence = requested_start.saturating_sub(self.range.start().0);
        let end_sequence = end.saturating_sub(self.range.start().0).saturating_add(1);
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let already_produced_end = end_sequence.min(state.produced);
        let required_retained = already_produced_end.saturating_sub(start_sequence);
        let retained = state
            .frames
            .iter()
            .filter(|frame| {
                start_sequence <= frame.sequence && frame.sequence < already_produced_end
            })
            .count();
        if state.subscribers.is_empty()
            || (start_sequence < state.produced
                && u64::try_from(retained).unwrap_or(u64::MAX) != required_retained)
        {
            return None;
        }
        state.subscribers.insert(
            subscriber_id,
            SubscriberProgress::new(start_sequence, end_sequence),
        );
        state.requested_ranges.push((start_sequence, end_sequence));
        self.subscribers_registered.fetch_add(1, Ordering::Relaxed);
        Some(end)
    }

    fn prepare_next(&self, frame: &BlockFrame) -> Result<NextFrameDisposition, SourceError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.subscribers.is_empty() {
            return Err(SourceError::Cancelled);
        }
        let expected_number = self.range.start().0.saturating_add(state.produced);
        if frame.block.number.0 != expected_number {
            return Err(SourceError::CorruptFrame(format!(
                "historical acquisition {} expected block {}, received {}",
                self.id, expected_number, frame.block.number.0
            )));
        }
        let sequence = state.produced;
        let needed = state
            .subscribers
            .values()
            .any(|progress| progress.delivered <= sequence && sequence < progress.end_exclusive);
        if needed {
            return Ok(NextFrameDisposition::Retain);
        }
        let requested = state
            .requested_ranges
            .iter()
            .any(|(start, end)| *start <= sequence && sequence < *end);
        state.produced = state.produced.saturating_add(1);
        if state.frames.is_empty() {
            state.base_sequence = state.produced;
        }
        Ok(if requested {
            NextFrameDisposition::Abandoned
        } else {
            NextFrameDisposition::Overfetch
        })
    }

    fn joinable_start_after(&self, cursor: u64, requested_end: u64) -> Option<u64> {
        let start = self.range.start().0;
        if start <= cursor || start > requested_end {
            return None;
        }
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (state.base_sequence == 0 && !state.subscribers.is_empty()).then_some(start)
    }

    fn has_joinable_overlap(&self, requested: BlockRange) -> bool {
        let start = self.range.start().0.max(requested.start().0);
        let end = self.range.end().0.min(requested.end().0);
        if start > end {
            return false;
        }
        let start_sequence = start.saturating_sub(self.range.start().0);
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.base_sequence <= start_sequence && !state.subscribers.is_empty()
    }

    #[allow(clippy::too_many_lines)]
    async fn produce(
        self: Arc<Self>,
        source: Arc<dyn HistorySource>,
        chunk: SourceChunk,
        budget: SourceBudget,
        reserved_active_permit: Option<OwnedSemaphorePermit>,
    ) {
        let Some(coordinator) = self.coordinator.upgrade() else {
            return;
        };
        let source_id = source.descriptor().id.to_string();
        let mut physical_frames = 0_u64;
        let mut physical_bytes = 0_u64;
        let mut overfetched_frames = 0_u64;
        let mut overfetched_bytes = 0_u64;
        let active_permit = if self.cancellation.is_cancelled() {
            Err(SourceError::Cancelled)
        } else if let Some(permit) = reserved_active_permit {
            Ok(permit)
        } else {
            tokio::select! {
                () = self.cancellation.cancelled() => Err(SourceError::Cancelled),
                permit = coordinator.active_chunks.clone().acquire_owned() => permit.map_err(|_| {
                    SourceError::Unavailable("historical active-chunk budget closed".to_owned())
                }),
            }
        };
        let outcome = match active_permit {
            Err(error) => Err(error),
            Ok(active_permit) => {
                let _active = ActiveAcquisitionGuard::new(coordinator.clone(), active_permit);
                match source.open(&chunk, budget, self.cancellation.clone()).await {
                    Ok(mut stream) => {
                        let mut outcome = Ok(());
                        while let Some(item) = stream.next().await {
                            let frame = match item {
                                Ok(frame) => frame,
                                Err(error) => {
                                    outcome = Err(error);
                                    break;
                                }
                            };
                            if let Err(error) = frame.validate_shape() {
                                outcome = Err(SourceError::CorruptFrame(error.to_owned()));
                                break;
                            }
                            let estimated_bytes = frame.estimated_heap_bytes();
                            let disposition = match self.prepare_next(&frame) {
                                Ok(disposition) => disposition,
                                Err(error) => {
                                    outcome = Err(error);
                                    break;
                                }
                            };
                            physical_frames = physical_frames.saturating_add(1);
                            physical_bytes = physical_bytes.saturating_add(estimated_bytes);
                            coordinator.physical_frames.fetch_add(1, Ordering::Relaxed);
                            coordinator
                                .physical_bytes
                                .fetch_add(estimated_bytes, Ordering::Relaxed);
                            if disposition != NextFrameDisposition::Retain {
                                if disposition == NextFrameDisposition::Overfetch {
                                    overfetched_frames = overfetched_frames.saturating_add(1);
                                    overfetched_bytes =
                                        overfetched_bytes.saturating_add(estimated_bytes);
                                    coordinator
                                        .overfetched_frames
                                        .fetch_add(1, Ordering::Relaxed);
                                    coordinator
                                        .overfetched_bytes
                                        .fetch_add(estimated_bytes, Ordering::Relaxed);
                                }
                                continue;
                            }
                            let retention = match coordinator
                                .reserve(estimated_bytes.max(1), &self.cancellation)
                                .await
                            {
                                Ok(retention) => retention,
                                Err(error) => {
                                    outcome = Err(error);
                                    break;
                                }
                            };
                            if let Err(error) = self
                                .push(Arc::new(frame), retention, &self.cancellation)
                                .await
                            {
                                outcome = Err(error);
                                break;
                            }
                        }
                        outcome
                    }
                    Err(error) => Err(error),
                }
            }
        };
        let result = if outcome.is_ok() {
            "completed"
        } else {
            "failed"
        };
        let last_error = outcome.as_ref().err().map(ToString::to_string);
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.terminal = Some(outcome);
        }
        tracing::info!(
            acquisition_id = self.id,
            source_id,
            from_block = self.range.start().0,
            to_block = self.range.end().0,
            physical_frames,
            physical_bytes,
            overfetched_frames,
            overfetched_bytes,
            fanout_subscribers = self.subscribers_registered.load(Ordering::Relaxed),
            result,
            last_error = last_error.as_deref().unwrap_or(""),
            "historical material acquisition completed"
        );
        self.changed.notify_waiters();
    }

    async fn push(
        &self,
        frame: Arc<BlockFrame>,
        retention: Arc<MemoryReservation>,
        cancellation: &CancellationToken,
    ) -> Result<(), SourceError> {
        loop {
            {
                let mut state = self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if state.subscribers.is_empty() {
                    return Err(SourceError::Cancelled);
                }
                if state.frames.len() < self.maximum_buffered_frames {
                    let sequence = state.produced;
                    state.produced = state.produced.saturating_add(1);
                    state.frames.push_back(BufferedFrame {
                        sequence,
                        frame,
                        physical_attributed: false,
                        retention,
                    });
                    drop(state);
                    self.changed.notify_waiters();
                    return Ok(());
                }
            }
            tokio::select! {
                () = cancellation.cancelled() => return Err(SourceError::Cancelled),
                () = self.changed.notified() => {}
            }
        }
    }

    fn acknowledge(&self, subscriber_id: u64, sequence: u64) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(progress) = state.subscribers.get_mut(&subscriber_id) else {
            return;
        };
        if progress.acknowledged == sequence {
            progress.acknowledged = progress.acknowledged.saturating_add(1);
            prune_acknowledged(&mut state);
            drop(state);
            self.changed.notify_waiters();
        }
    }

    fn unregister(&self, subscriber_id: u64) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.subscribers.remove(&subscriber_id);
        if state.subscribers.is_empty() {
            state.frames.clear();
            state.base_sequence = state.produced;
            self.cancellation.cancel();
        } else {
            prune_acknowledged(&mut state);
        }
        drop(state);
        self.changed.notify_waiters();
    }
}

impl std::fmt::Debug for Acquisition {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Acquisition")
            .field("id", &self.id)
            .field("range", &self.range)
            .field("maximum_buffered_frames", &self.maximum_buffered_frames)
            .finish_non_exhaustive()
    }
}

struct SubscriberProgress {
    delivered: u64,
    acknowledged: u64,
    end_exclusive: u64,
}

impl SubscriberProgress {
    const fn new(start: u64, end_exclusive: u64) -> Self {
        Self {
            delivered: start,
            acknowledged: start,
            end_exclusive,
        }
    }
}

struct AcquisitionState {
    base_sequence: u64,
    produced: u64,
    frames: VecDeque<BufferedFrame>,
    subscribers: HashMap<u64, SubscriberProgress>,
    requested_ranges: Vec<(u64, u64)>,
    terminal: Option<Result<(), SourceError>>,
}

struct BufferedFrame {
    sequence: u64,
    frame: Arc<BlockFrame>,
    physical_attributed: bool,
    retention: Arc<MemoryReservation>,
}

fn intersect_ranges(first: BlockRange, second: BlockRange) -> Option<BlockRange> {
    BlockRange::new(
        first.start().max(second.start()),
        first.end().min(second.end()),
    )
    .ok()
}

fn connected_demand_range(ranges: &[BlockRange], target: BlockRange) -> BlockRange {
    let mut start = target.start().0;
    let mut end = target.end().0;
    loop {
        let mut changed = false;
        for range in ranges {
            let touches = range.start().0 <= end.saturating_add(1)
                && start <= range.end().0.saturating_add(1);
            if touches {
                let next_start = start.min(range.start().0);
                let next_end = end.max(range.end().0);
                changed |= next_start != start || next_end != end;
                start = next_start;
                end = next_end;
            }
        }
        if !changed {
            return BlockRange::new(start.into(), end.into())
                .expect("merged valid demand ranges remain ordered");
        }
    }
}

fn prune_acknowledged(state: &mut AcquisitionState) {
    let Some(acknowledged) = state
        .subscribers
        .values()
        .map(|progress| progress.acknowledged)
        .min()
    else {
        return;
    };
    while state
        .frames
        .front()
        .is_some_and(|frame| frame.sequence < acknowledged)
    {
        state.frames.pop_front();
    }
    state.base_sequence = state
        .frames
        .front()
        .map_or(state.produced, |frame| frame.sequence);
}

struct MaterialSubscription {
    acquisition: Arc<Acquisition>,
    subscriber_id: u64,
    cancellation: CancellationToken,
    terminal: bool,
}

fn material_subscription_stream(subscription: MaterialSubscription) -> HistoricalMaterialStream {
    futures::stream::unfold(subscription, |mut subscription| async move {
        subscription.next().await.map(|item| (item, subscription))
    })
    .boxed()
}

impl MaterialSubscription {
    async fn next(&mut self) -> Option<Result<HistoricalMaterialFrame, SourceError>> {
        if self.terminal {
            return None;
        }
        loop {
            let notified = self.acquisition.changed.notified();
            {
                let mut state = self
                    .acquisition
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let progress = state.subscribers.get(&self.subscriber_id)?;
                let delivered = progress.delivered;
                if delivered >= progress.end_exclusive {
                    self.terminal = true;
                    return None;
                }
                if delivered < state.produced {
                    let buffered = state
                        .frames
                        .iter_mut()
                        .find(|frame| frame.sequence == delivered)?;
                    let physical_source = !buffered.physical_attributed;
                    buffered.physical_attributed = true;
                    let result = HistoricalMaterialFrame {
                        frame: buffered.frame.clone(),
                        acquisition_id: Some(self.acquisition.id),
                        physical_source,
                        coalesced: !physical_source,
                        sequence: Some(buffered.sequence),
                        subscriber_id: Some(self.subscriber_id),
                        acquisition: Some(Arc::downgrade(&self.acquisition)),
                        _retention: Some(buffered.retention.clone()),
                    };
                    if let Some(progress) = state.subscribers.get_mut(&self.subscriber_id) {
                        progress.delivered = progress.delivered.saturating_add(1);
                    }
                    if let Some(coordinator) = self.acquisition.coordinator.upgrade() {
                        coordinator
                            .logical_frame_deliveries
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    return Some(Ok(result));
                }
                if let Some(outcome) = &state.terminal {
                    self.terminal = true;
                    return match outcome {
                        Ok(()) => None,
                        Err(error) => Some(Err(error.clone())),
                    };
                }
            }
            tokio::select! {
                () = self.cancellation.cancelled() => {
                    self.terminal = true;
                    return Some(Err(SourceError::Cancelled));
                }
                () = notified => {}
            }
        }
    }
}

impl Drop for MaterialSubscription {
    fn drop(&mut self) {
        self.acquisition.unregister(self.subscriber_id);
    }
}
