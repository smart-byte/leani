//! Versioned source requests, plans, budgets, events, and traits.

use std::{pin::Pin, time::Duration};

use async_trait::async_trait;
use futures::Stream;
use leani_primitives::{
    Address, BlockFrame, BlockHash, BlockRange, BlockRef, CapabilitySet, ChainId, FilterScope,
    Finality, LogFieldSet, SourceCursor, SourceId, SourceKind, TransactionHash, TrustModel,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

pub type BlockFrameStream =
    Pin<Box<dyn Stream<Item = Result<BlockFrame, SourceError>> + Send + 'static>>;
pub type ChainEventStream =
    Pin<Box<dyn Stream<Item = Result<ChainEvent, SourceError>> + Send + 'static>>;
pub type FinalityEventStream =
    Pin<Box<dyn Stream<Item = Result<FinalityEvent, SourceError>> + Send + 'static>>;

/// Coarse projection pushed down before source reads.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct FieldProjection {
    pub header_fields: Vec<String>,
    pub transaction_fields: Vec<String>,
    pub receipt_fields: Vec<String>,
    pub log_fields: Vec<String>,
}

/// Normalized predicates that a source may push down.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct FilterSet {
    pub scope: FilterScope,
    pub senders: Vec<Address>,
    pub recipients: Vec<Address>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum VerificationPolicy {
    /// Reject any material that cannot be independently committed-root checked.
    CompleteCryptographic,
    /// Allow dataset-declared predicate completeness with recorded provenance.
    TrustedDataset,
    /// Development-only mode; still rejects an explicit failed check.
    BestEffort,
}

/// Source request compiled from one or more processor descriptors.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DataRequest {
    pub chain_id: ChainId,
    pub range: BlockRange,
    pub required: CapabilitySet,
    pub log_fields: LogFieldSet,
    pub allow_filtered: bool,
    pub projection: FieldProjection,
    pub filters: FilterSet,
    pub minimum_finality: Finality,
    pub verification_policy: VerificationPolicy,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum FinalityModel {
    None,
    Optimistic,
    Safe,
    Finalized,
}

impl FinalityModel {
    #[must_use]
    pub const fn supports(self, required: Finality) -> bool {
        match self {
            Self::None => false,
            Self::Optimistic => matches!(required, Finality::Optimistic),
            Self::Safe => !matches!(required, Finality::Finalized),
            Self::Finalized => true,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum Partitioning {
    None,
    FixedBlockSpan(u64),
    DatasetObjects,
    SourceDefined(String),
}

/// Stable source description used for planning without opening it.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SourceDescriptor {
    pub id: SourceId,
    pub kind: SourceKind,
    pub chain_id: ChainId,
    pub range: Option<BlockRange>,
    /// Material the source can return, including filtered material.
    pub capabilities: CapabilitySet,
    /// Material the source can prove complete without trusting a predicate
    /// projection.
    pub complete_capabilities: CapabilitySet,
    pub trust: TrustModel,
    pub finality: FinalityModel,
    pub partitioning: Partitioning,
    pub expected_lag: Duration,
    pub schema_version: String,
    pub priority: u16,
}

/// One independently retryable, non-overlapping requested range.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SourceChunk {
    pub source_id: SourceId,
    pub range: BlockRange,
    pub partition: Vec<u8>,
    pub schema_version: String,
    pub expected_parent: Option<BlockHash>,
    pub estimated_bytes: Option<u64>,
}

/// Logical reader selected by a source adapter for one physical operation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PhysicalReader {
    Block,
    Transaction,
    Receipt,
    Log,
}

/// Inspectable physical work selected for a source request.
///
/// This is diagnostic rather than executable state: immutable executable
/// projection identity remains in each [`SourceChunk::partition`].
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PhysicalPlanOperation {
    pub reader: PhysicalReader,
    pub table: String,
    pub columns: Vec<String>,
    pub predicates: Vec<String>,
    pub estimated_bytes: Option<u64>,
    pub trust: TrustModel,
    pub completeness: String,
    pub derived: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SourcePlan {
    pub source_id: SourceId,
    pub request: DataRequest,
    pub chunks: Vec<SourceChunk>,
    pub estimated_bytes: Option<u64>,
    pub estimated_lag: Duration,
    pub supplied: CapabilitySet,
    pub complete: CapabilitySet,
    pub trust: TrustModel,
    pub schema_version: String,
    /// Selected tables, columns, predicates, and trust semantics.
    #[serde(default)]
    pub physical_plan: Vec<PhysicalPlanOperation>,
}

/// Cumulative physical-acquisition measurements exposed by a history source.
///
/// Sources populate only fields they can measure honestly. For example, an
/// HTTP range reader can report exact returned bytes, while a columnar object
/// reader may know source-object and projected compressed bytes without
/// observing transport-level framing or cache behavior.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct SourceAcquisitionMetrics {
    pub opened_chunks: u64,
    /// Logical chunk ranges handed to this source, in open order. These are
    /// source-attribution boundaries, not a claim that every frame completed.
    pub opened_ranges: Vec<BlockRange>,
    pub acquired_frames: u64,
    pub normalized_bytes: u64,
    /// Logical byte ranges requested by a columnar/object reader. One logical
    /// range may be coalesced, split, cached, or retried below the adapter.
    pub logical_range_requests: Option<u64>,
    /// Transport operations observed directly by the source adapter.
    pub physical_reads: Option<u64>,
    pub fetched_bytes: Option<u64>,
    pub source_objects: Option<u64>,
    pub source_object_bytes: Option<u64>,
    pub projected_compressed_bytes: Option<u64>,
    pub rows_scanned: Option<u64>,
    pub rows_selected: Option<u64>,
    /// Sum of source-operation elapsed times. Concurrent operations can make
    /// this larger than benchmark wall time.
    pub operation_elapsed_ms: u64,
}

impl SourcePlan {
    /// Validate immutable plan invariants before opening any chunk.
    ///
    /// # Errors
    ///
    /// Returns [`SourceError::InvalidPlan`] for wrong-source, overlapping,
    /// unsorted, out-of-range, or schema-inconsistent chunks.
    pub fn validate(&self) -> Result<(), SourceError> {
        if self.chunks.is_empty() {
            return Err(SourceError::InvalidPlan(
                "plan contains no chunks".to_owned(),
            ));
        }
        let mut next = self.request.range.start().0;
        for chunk in &self.chunks {
            if chunk.source_id != self.source_id {
                return Err(SourceError::InvalidPlan(
                    "chunk belongs to a different source".to_owned(),
                ));
            }
            if chunk.schema_version != self.schema_version {
                return Err(SourceError::SchemaDrift {
                    expected: self.schema_version.clone(),
                    actual: chunk.schema_version.clone(),
                });
            }
            if chunk.range.start().0 != next || chunk.range.end().0 > self.request.range.end().0 {
                return Err(SourceError::InvalidPlan(
                    "chunks are not an exact ordered cover".to_owned(),
                ));
            }
            next = chunk.range.end().0.saturating_add(1);
        }
        if next != self.request.range.end().0.saturating_add(1) {
            return Err(SourceError::InvalidPlan(
                "chunks do not cover the complete request".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Hard per-open resource limits enforced by source adapters.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SourceBudget {
    pub max_input_bytes: u64,
    pub max_frame_bytes: u64,
    pub max_frames: u64,
    pub max_buffered_frames: usize,
    pub max_in_flight_requests: usize,
    pub temporary_disk_bytes: u64,
}

impl SourceBudget {
    /// Validate that no disabled or unbounded zero limit can enter a source.
    ///
    /// # Errors
    ///
    /// Returns [`SourceError::InvalidBudget`] when any hard limit is zero.
    pub const fn validate(self) -> Result<Self, SourceError> {
        if self.max_input_bytes == 0
            || self.max_frame_bytes == 0
            || self.max_frames == 0
            || self.max_buffered_frames == 0
            || self.max_in_flight_requests == 0
        {
            Err(SourceError::InvalidBudget)
        } else {
            Ok(self)
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum LiveStart {
    Head,
    Block(BlockRef),
    /// Start from a consensus-verified anchor while first replaying a bounded
    /// inclusive overlap ending at that anchor.
    ///
    /// The overlap lets the runtime compare live execution material with
    /// independently sourced historical processor coverage before declaring
    /// the hot/cold join complete.
    AnchoredOverlap {
        anchor: BlockRef,
        overlap_blocks: u64,
    },
    /// Resume from a previously verified and durably retained canonical
    /// suffix without downloading that suffix again. The final element is the
    /// current local tip; preceding elements provide bounded reorg context.
    RetainedCanonical {
        canonical: Vec<BlockRef>,
    },
    Cursor(SourceCursor),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ChainEvent {
    Block(Box<BlockFrame>),
    Reorg {
        reverted: Vec<BlockRef>,
        applied: Vec<BlockFrame>,
    },
    Disconnected {
        reason: String,
    },
    Reset {
        last_valid: Option<BlockRef>,
        reason: String,
    },
}

/// Explicit weak-subjectivity root and execution anchor.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ConsensusCheckpoint {
    pub beacon_slot: u64,
    pub beacon_block_root: [u8; 32],
    pub execution_block_hash: BlockHash,
    pub obtained_at_unix_seconds: u64,
    pub source: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum FinalityEvent {
    Optimistic {
        block_hash: BlockHash,
        beacon_slot: u64,
    },
    Safe {
        block_hash: BlockHash,
        beacon_slot: u64,
    },
    Finalized {
        block_hash: BlockHash,
        beacon_slot: u64,
        beacon_block_root: [u8; 32],
    },
    Reset(ConsensusCheckpoint),
    Disagreement {
        first: BlockHash,
        second: BlockHash,
        beacon_slot: u64,
    },
}

/// Optional non-range lookup families implemented by a historical source.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct HistoryLookupCapabilities {
    pub block_hash: bool,
    pub transaction_hash: bool,
}

/// Transaction-hash lookup result with its canonical containing frame.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LocatedTransaction {
    pub frame: BlockFrame,
    pub transaction_index: u32,
}

#[async_trait]
pub trait HistorySource: Send + Sync {
    fn descriptor(&self) -> &SourceDescriptor;

    /// Cumulative physical acquisition measurements, when the adapter exposes
    /// them. Benchmark callers must snapshot immediately before and after a
    /// run and report the delta so reused source instances remain valid.
    fn acquisition_metrics(&self) -> Option<SourceAcquisitionMetrics> {
        None
    }

    /// Optional locator-backed lookup families. Range planning remains the
    /// mandatory common contract for every history source.
    fn lookup_capabilities(&self) -> HistoryLookupCapabilities {
        HistoryLookupCapabilities::default()
    }

    /// Resolve one canonical block hash when this source has a compatible
    /// locator. `None` means the hash is not covered by this source.
    async fn block_by_hash(
        &self,
        _chain_id: ChainId,
        _hash: BlockHash,
        _required: CapabilitySet,
    ) -> Result<Option<BlockFrame>, SourceError> {
        Ok(None)
    }

    /// Resolve one canonical transaction hash when this source has a
    /// compatible locator. `None` means the hash is not covered by this source.
    async fn transaction_by_hash(
        &self,
        _chain_id: ChainId,
        _hash: TransactionHash,
        _required: CapabilitySet,
    ) -> Result<Option<LocatedTransaction>, SourceError> {
        Ok(None)
    }

    /// Smallest block span that is normally efficient for this source.
    ///
    /// The coordinator treats this as a planning hint, not a correctness
    /// requirement. Any expansion remains bounded by the caller's source
    /// budget and configured overfetch ratio.
    fn minimum_efficient_blocks(&self) -> Option<u64> {
        match &self.descriptor().partitioning {
            Partitioning::FixedBlockSpan(blocks) if *blocks != 0 => Some(*blocks),
            Partitioning::None
            | Partitioning::FixedBlockSpan(_)
            | Partitioning::DatasetObjects
            | Partitioning::SourceDefined(_) => None,
        }
    }

    /// Immutable identity of the concrete source instance used for exact
    /// in-flight acquisition sharing.
    ///
    /// Most sources are fully identified by their descriptor. Anchored or
    /// session-bound adapters override this to include immutable state, such
    /// as a consensus anchor, that is not part of the public descriptor.
    fn acquisition_identity(&self) -> Vec<u8> {
        postcard::to_allocvec(self.descriptor())
            .expect("validated source descriptors have a durable identity")
    }

    /// Partition identity with range-local fields removed when the source can
    /// safely share overlapping reads from the same physical partition.
    fn coalescing_partition_identity(&self, chunk: &SourceChunk) -> Vec<u8> {
        chunk.partition.clone()
    }

    /// Derive an executable subrange from one planned source chunk.
    ///
    /// Sources whose encoded partition includes the exact chunk range must
    /// override this method and rebuild that identity.
    ///
    /// # Errors
    ///
    /// Returns an invalid-plan error when the requested slice exceeds the
    /// planned chunk or its source-specific identity cannot be rebuilt.
    fn slice_chunk(
        &self,
        chunk: &SourceChunk,
        range: BlockRange,
    ) -> Result<SourceChunk, SourceError> {
        if range.start() < chunk.range.start() || range.end() > chunk.range.end() {
            return Err(SourceError::InvalidPlan(
                "coalesced subrange exceeds its planned source chunk".to_owned(),
            ));
        }
        let mut sliced = chunk.clone();
        if range.start() != chunk.range.start() {
            sliced.expected_parent = None;
        }
        sliced.range = range;
        sliced.estimated_bytes = None;
        Ok(sliced)
    }

    async fn plan(&self, request: &DataRequest) -> Result<SourcePlan, SourceError>;

    async fn open(
        &self,
        chunk: &SourceChunk,
        budget: SourceBudget,
        cancellation: CancellationToken,
    ) -> Result<BlockFrameStream, SourceError>;
}

#[async_trait]
pub trait LiveSource: Send + Sync {
    fn descriptor(&self) -> &SourceDescriptor;

    async fn subscribe(
        &self,
        request: DataRequest,
        start: LiveStart,
        budget: SourceBudget,
        cancellation: CancellationToken,
    ) -> Result<ChainEventStream, SourceError>;
}

#[async_trait]
pub trait FinalitySource: Send + Sync {
    fn descriptor(&self) -> &SourceDescriptor;

    async fn subscribe(
        &self,
        checkpoint: ConsensusCheckpoint,
        cancellation: CancellationToken,
    ) -> Result<FinalityEventStream, SourceError>;
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum SourceError {
    #[error("source was cancelled")]
    Cancelled,
    #[error("source does not cover requested range {0:?}")]
    MissingRange(BlockRange),
    #[error("source does not completely cover requested range {range:?}: {detail}")]
    IncompleteRange { range: BlockRange, detail: String },
    #[error(
        "source lacks retained material for ranges {gaps:?}: required 0x{required:04x}, available 0x{available:04x}"
    )]
    MissingMaterial {
        gaps: Vec<BlockRange>,
        required: u16,
        available: u16,
    },
    #[error("source schema changed: expected `{expected}`, received `{actual}`")]
    SchemaDrift { expected: String, actual: String },
    #[error("source frame is corrupt: {0}")]
    CorruptFrame(String),
    #[error("source resource budget exceeded: {resource}, limit {limit}, observed {observed}")]
    BudgetExceeded {
        resource: &'static str,
        limit: u64,
        observed: u64,
    },
    #[error("source budget contains a zero hard limit")]
    InvalidBudget,
    #[error("invalid source plan: {0}")]
    InvalidPlan(String),
    #[error("source disconnected: {0}")]
    Disconnected(String),
    #[error("source is unavailable: {0}")]
    Unavailable(String),
    #[error("source protocol error: {0}")]
    Protocol(String),
}
