//! SQLite-backed entities, indexes, undo journal, cursors, and change log.
//!
//! Processor implementations never receive a SQL connection. They operate on
//! [`ReducerOverlay`], which captures the first preimage of every touched key
//! and provides read-your-writes semantics. The store then commits state,
//! inverse mutations, cursor, coverage, public changes, and outbox records in
//! one `SQLite` transaction.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    path::{Component, Path, PathBuf},
    str::FromStr,
    sync::{Arc, Mutex as StdMutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use leani_primitives::{
    BlockFrame, BlockHash, BlockNumber, BlockRange, BlockRef, ChainId, ChangeCursor, CursorKind,
    DurableKind, Finality, Material, OpaqueCursor, ProcessorCursor, TransactionHash,
};
use leani_processor_api::{
    ArtifactPolicyMode, ChangeOperation, CheckpointPolicyMode, DeliveryLimitAction,
    DeliveryOrdering, DeliveryPolicyMode, DomainChange, EncodedDelta, OutputPolicyMode, Processor,
    ProcessorDescriptor, ProcessorError, ReducerTransaction, ReductionMode, StartPoint,
};
use leani_store_artifacts::{
    ArtifactBatchReceipt, ArtifactBatchSink, ArtifactCompression, ArtifactSegmentLimits,
    ArtifactSegmentSink, ArtifactSegmentSinkConfig, ArtifactSegmentSinkStats,
};
use serde::{Deserialize, Serialize};
use sqlx::{
    Row, Sqlite, SqlitePool, Transaction,
    sqlite::{
        SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow, SqliteSynchronous,
    },
};
use thiserror::Error;
use tokio::sync::{Mutex, Notify};

const SCHEMA_V1: &str = include_str!("../migrations/0001_core.sql");
const SCHEMA_V2: &str = include_str!("../migrations/0002_change_context.sql");
const SCHEMA_V3: &str = include_str!("../migrations/0003_recent_transaction_locator.sql");
const SCHEMA_V4: &str = include_str!("../migrations/0004_handoffs.sql");
const SCHEMA_V5: &str = include_str!("../migrations/0005_archive_reconciliations.sql");
const SCHEMA_V6: &str = include_str!("../migrations/0006_durable_consumers.sql");
const SCHEMA_V7: &str = include_str!("../migrations/0007_lifecycle_storage.sql");
const SCHEMA_V8: &str = include_str!("../migrations/0008_delivery_stream_scopes.sql");
const SCHEMA_V9: &str = include_str!("../migrations/0009_backfill_delivery_batch_limits.sql");
const SCHEMA_V10: &str = include_str!("../migrations/0010_coverage_parent_anchors.sql");
const SCHEMA_V11: &str = include_str!("../migrations/0011_finalized_coverage_segments.sql");
const SCHEMA_V12: &str = include_str!("../migrations/0012_delivery_origins.sql");
const SCHEMA_V13: &str = include_str!("../migrations/0013_live_lane_gaps.sql");
const SCHEMA_V14: &str = include_str!("../migrations/0014_processor_artifacts.sql");
const SCHEMA_V15: &str = include_str!("../migrations/0015_processor_artifact_candidates.sql");
const SCHEMA_V16: &str = include_str!("../migrations/0016_tiered_processor_artifacts.sql");
const SCHEMA_V17: &str = include_str!("../migrations/0017_artifact_segment_owners.sql");
const SCHEMA_V18: &str = include_str!("../migrations/0018_processor_artifact_totals.sql");
const SCHEMA_V19: &str = include_str!("../migrations/0019_bulk_artifact_accounting.sql");
/// Current on-disk `SQLite` schema version written by this crate.
pub const CURRENT_SCHEMA_VERSION: u32 = 19;
/// Stable encoding version attached to durable delivery records.
pub const DELIVERY_ENCODING_VERSION: u16 = 1;

/// `SQLite` fsync policy.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Durability {
    /// Fsync each transaction and WAL checkpoint. Safest default.
    #[default]
    Full,
    /// Rely on WAL durability while reducing checkpoint fsyncs.
    Normal,
}

/// Store connection and durability settings.
#[derive(Clone, Debug)]
pub struct StoreConfig {
    pub path: PathBuf,
    pub durability: Durability,
    pub reader_connections: u32,
    pub busy_timeout: Duration,
    pub storage_budget: StoreStorageBudget,
    pub delivery_budget: DeliveryStorageBudget,
    pub artifact_budget: ArtifactStorageBudget,
    pub artifact_segments: Option<ArtifactSegmentStorageConfig>,
}

/// Optional lower tier for immutable finalized processor artifacts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactSegmentStorageConfig {
    pub root: PathBuf,
    pub compression: ArtifactCompression,
    pub target_blocks: u64,
    pub maximum_artifact_logical_bytes: u64,
    pub maximum_segment_logical_bytes: u64,
    pub maximum_segment_physical_bytes: u64,
    pub maximum_retained_physical_bytes: u64,
}

impl ArtifactSegmentStorageConfig {
    fn sink_config(&self) -> ArtifactSegmentSinkConfig {
        ArtifactSegmentSinkConfig {
            compression: self.compression,
            limits: ArtifactSegmentLimits {
                maximum_artifact_logical_bytes: self.maximum_artifact_logical_bytes,
                maximum_segment_logical_bytes: self.maximum_segment_logical_bytes,
                maximum_segment_physical_bytes: self.maximum_segment_physical_bytes,
            },
            maximum_retained_physical_bytes: self.maximum_retained_physical_bytes,
        }
    }
}

/// Node-wide emergency admission limit for the shared `SQLite` database and
/// WAL, independent of any one retained capability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StoreStorageBudget {
    pub maximum_physical_bytes: u64,
}

impl Default for StoreStorageBudget {
    fn default() -> Self {
        Self {
            maximum_physical_bytes: u64::MAX,
        }
    }
}

/// Node-wide delivery-spool and physical-store admission limits.
///
/// Per-stream limits remain part of the processor/subscription contract. These
/// limits prevent several individually healthy streams from exhausting the
/// shared `SQLite` sidecar. The history ceiling is deliberately non-borrowable,
/// preserving delivery headroom for live lanes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeliveryStorageBudget {
    pub maximum_retained_bytes: u64,
    pub maximum_history_retained_bytes: u64,
}

impl Default for DeliveryStorageBudget {
    fn default() -> Self {
        Self {
            maximum_retained_bytes: u64::MAX,
            maximum_history_retained_bytes: u64::MAX,
        }
    }
}

/// Independent logical byte limits for finalized artifacts and optimistic
/// candidates awaiting finality.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ArtifactStorageBudget {
    pub maximum_retained_bytes: u64,
    pub maximum_pending_bytes: u64,
}

impl Default for ArtifactStorageBudget {
    fn default() -> Self {
        Self {
            maximum_retained_bytes: u64::MAX,
            maximum_pending_bytes: u64::MAX,
        }
    }
}

impl StoreConfig {
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            durability: Durability::Full,
            reader_connections: 4,
            busy_timeout: Duration::from_secs(5),
            storage_budget: StoreStorageBudget::default(),
            delivery_budget: DeliveryStorageBudget::default(),
            artifact_budget: ArtifactStorageBudget::default(),
            artifact_segments: None,
        }
    }

    #[must_use]
    pub const fn with_storage_budget(mut self, storage_budget: StoreStorageBudget) -> Self {
        self.storage_budget = storage_budget;
        self
    }

    #[must_use]
    pub const fn with_delivery_budget(mut self, delivery_budget: DeliveryStorageBudget) -> Self {
        self.delivery_budget = delivery_budget;
        self
    }

    #[must_use]
    pub const fn with_artifact_budget(mut self, artifact_budget: ArtifactStorageBudget) -> Self {
        self.artifact_budget = artifact_budget;
        self
    }

    #[must_use]
    pub fn with_artifact_segments(
        mut self,
        artifact_segments: ArtifactSegmentStorageConfig,
    ) -> Self {
        self.artifact_segments = Some(artifact_segments);
        self
    }
}

/// A committed change-log row.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChangeRecord {
    pub delivery_encoding_version: u16,
    pub cursor: ChangeCursor,
    pub origin: DeliveryOrigin,
    pub block: BlockRef,
    pub finality: Finality,
    pub direction: ChangeDirection,
    pub change: DomainChange,
    pub emitted_at_unix_ms: u64,
}

/// Durable provenance for one publication record.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DeliveryOrigin {
    pub kind: DeliveryOriginKind,
    pub id: String,
    pub publication_revision: u64,
}

impl DeliveryOrigin {
    fn live(instance: &str) -> Self {
        Self {
            kind: DeliveryOriginKind::Live,
            id: instance.to_owned(),
            publication_revision: 0,
        }
    }

    fn live_recovery(instance: &str) -> Self {
        Self {
            kind: DeliveryOriginKind::LiveRecovery,
            id: instance.to_owned(),
            publication_revision: 0,
        }
    }
}

/// Why a durable delivery record was published.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryOriginKind {
    Live,
    LiveRecovery,
    HistoricalBackfill,
    Recompute,
}

impl DeliveryOriginKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Live => "live",
            Self::LiveRecovery => "live_recovery",
            Self::HistoricalBackfill => "historical_backfill",
            Self::Recompute => "recompute",
        }
    }

    fn parse(value: &str) -> Result<Self, StoreError> {
        match value {
            "live" => Ok(Self::Live),
            "live_recovery" => Ok(Self::LiveRecovery),
            "historical_backfill" => Ok(Self::HistoricalBackfill),
            "recompute" => Ok(Self::Recompute),
            other => Err(StoreError::Invariant(format!(
                "unknown delivery origin kind {other:?}"
            ))),
        }
    }
}

/// Retained sequence interval for one processor change log.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct ChangeBounds {
    pub earliest: u64,
    pub latest: u64,
}

/// Result of lease-aware change-log pruning.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct ChangePruneOutcome {
    pub requested_before: u64,
    pub effective_before: u64,
    pub deleted: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsumerRole {
    Required,
    BestEffort,
}

impl ConsumerRole {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Required => "required",
            Self::BestEffort => "best_effort",
        }
    }

    fn parse(value: &str) -> Result<Self, StoreError> {
        match value {
            "required" => Ok(Self::Required),
            "best_effort" => Ok(Self::BestEffort),
            other => Err(StoreError::Invariant(format!(
                "unknown durable consumer role {other:?}"
            ))),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsumerState {
    Active,
    ResetRequired,
    Revoked,
}

impl ConsumerState {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::ResetRequired => "reset_required",
            Self::Revoked => "revoked",
        }
    }

    fn parse(value: &str) -> Result<Self, StoreError> {
        match value {
            "active" => Ok(Self::Active),
            "reset_required" => Ok(Self::ResetRequired),
            "revoked" => Ok(Self::Revoked),
            other => Err(StoreError::Invariant(format!(
                "unknown durable consumer state {other:?}"
            ))),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConsumerStartPosition {
    EarliestRetained,
    CurrentHead,
    After(u64),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DurableConsumer {
    pub consumer_id: String,
    pub processor_instance: String,
    pub stream_id: String,
    pub role: ConsumerRole,
    pub state: ConsumerState,
    pub acknowledged_sequence: u64,
    pub delivered_sequence: u64,
    pub lease_generation: u64,
    pub lease_ttl_ms: u64,
    pub lease_expires_at_unix_ms: u64,
    pub lease_active: bool,
    pub created_at_unix_ms: u64,
    pub updated_at_unix_ms: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct ConsumerSessionLease {
    pub generation: u64,
    pub expires_at_unix_ms: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryStreamKind {
    Canonical,
    Live,
    Backfill,
}

impl DeliveryStreamKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Canonical => "canonical",
            Self::Live => "live",
            Self::Backfill => "backfill",
        }
    }

    fn parse(value: &str) -> Result<Self, StoreError> {
        match value {
            "canonical" => Ok(Self::Canonical),
            "live" => Ok(Self::Live),
            "backfill" => Ok(Self::Backfill),
            other => Err(StoreError::Invariant(format!(
                "unknown delivery stream kind {other:?}"
            ))),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DeliveryStream {
    pub stream_id: String,
    pub processor_instance: String,
    pub kind: DeliveryStreamKind,
    pub subscription_id: Option<String>,
    pub created_at_unix_ms: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct ConsumerLag {
    pub changes: u64,
    pub blocks: u64,
    pub bytes: u64,
    pub age_ms: u64,
}

/// Delivery-spool watermarks and physical retention for one processor.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct DeliveryStreamStats {
    pub live_bytes: u64,
    pub pruned_through_sequence: u64,
    pub required_ack_watermark: Option<u64>,
    pub format_version: u16,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessorRunState {
    Running,
    Paused,
    Failed,
}

impl ProcessorRunState {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Paused => "paused",
            Self::Failed => "failed",
        }
    }

    fn parse(value: &str) -> Result<Self, StoreError> {
        match value {
            "running" => Ok(Self::Running),
            "paused" => Ok(Self::Paused),
            "failed" => Ok(Self::Failed),
            other => Err(StoreError::Invariant(format!(
                "unknown processor run state {other:?}"
            ))),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ProcessorRuntimeState {
    pub processor_instance: String,
    pub state: ProcessorRunState,
    pub reason: Option<String>,
    pub updated_at_unix_ms: u64,
}

/// Durable first-unapplied marker for one independently isolated live lane.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct LiveLaneGap {
    pub processor_instance: String,
    pub first_unapplied: BlockRef,
    pub required_delivery_bytes: u64,
    pub reason: String,
    pub created_at_unix_ms: u64,
    pub updated_at_unix_ms: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeDirection {
    Apply,
    Undo,
    Finalized,
    ResetRequired,
}

impl ChangeDirection {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Apply => "apply",
            Self::Undo => "undo",
            Self::Finalized => "finalized",
            Self::ResetRequired => "reset_required",
        }
    }

    fn parse(value: &str) -> Result<Self, StoreError> {
        match value {
            "apply" => Ok(Self::Apply),
            "undo" => Ok(Self::Undo),
            "finalized" => Ok(Self::Finalized),
            "reset_required" => Ok(Self::ResetRequired),
            other => Err(StoreError::Invariant(format!(
                "unknown change direction {other:?}"
            ))),
        }
    }
}

/// Result of an idempotent block application.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApplyOutcome {
    Applied {
        processor_cursor: ProcessorCursor,
        first_change_sequence: Option<u64>,
        last_change_sequence: Option<u64>,
    },
    AlreadyApplied,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HistoricalBatchMode {
    Apply,
    Republish,
    /// Republish after the runtime reacquired and verified every intersecting
    /// compact proof segment. Exact retained rows are still checked directly.
    RepublishVerifiedCompact,
}

/// Owner of compact artifact bytes for one historical materialization commit.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum HistoricalArtifactTarget {
    #[default]
    Sqlite,
    /// A durable external sink committed the exact batch before this
    /// transaction advances coverage and the scheduler checkpoint.
    ExternalCommitted,
}

#[derive(Clone, Debug)]
pub struct HistoricalBatchItem {
    pub delta: EncodedDelta,
    pub finality: Finality,
    pub publish_changes: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HistoricalCommitLimits {
    pub maximum_changes: usize,
    pub maximum_encoded_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoricalBatchOutcome {
    pub processed_blocks: u64,
    pub published_changes: u64,
    /// Measured logical output/delivery bytes committed by this transaction.
    /// Runtime fairness uses this together with source bytes rather than block
    /// counts, so dense processors cannot starve sparse processors.
    pub committed_output_bytes: u64,
    pub first_change_sequence: Option<u64>,
    pub last_change_sequence: Option<u64>,
    /// Time for which this historical operation held the exclusive `SQLite`
    /// writer permit, including reducer reads performed under that permit.
    pub writer_hold_micros: u64,
}

/// One bounded proof segment inside compact finalized processor coverage.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct FinalizedCoverageSegment {
    pub range: BlockRange,
    pub start_parent_hash: BlockHash,
    pub end_hash: BlockHash,
    pub interval_start: BlockNumber,
    pub segment_size: u64,
    pub encoding_version: u16,
}

/// Result of one bounded finalized-coverage compaction transaction.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub struct FinalizedCoverageCompaction {
    pub intervals_created: u64,
    pub segments_created: u64,
    pub exact_coverage_deleted: u64,
    pub applied_blocks_deleted: u64,
    pub finalized_undo_deleted: u64,
    pub compacted_range: Option<BlockRange>,
}

/// Result of republishing a finalized block-local result without mutating
/// processor state or coverage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplayOutcome {
    pub published_changes: usize,
    pub first_change_sequence: Option<u64>,
    pub last_change_sequence: Option<u64>,
}

/// Durable ownership category for one immutable processor artifact.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactOwnerKind {
    ProcessorInstance,
    ProcessorJob,
    OperatorPin,
}

impl ArtifactOwnerKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::ProcessorInstance => "processor_instance",
            Self::ProcessorJob => "processor_job",
            Self::OperatorPin => "operator_pin",
        }
    }

    fn parse(value: &str) -> Result<Self, StoreError> {
        match value {
            "processor_instance" => Ok(Self::ProcessorInstance),
            "processor_job" => Ok(Self::ProcessorJob),
            "operator_pin" => Ok(Self::OperatorPin),
            other => Err(StoreError::Invariant(format!(
                "unknown processor artifact owner kind {other:?}"
            ))),
        }
    }
}

/// One explicit owner protecting an immutable artifact from pruning.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ArtifactOwner {
    pub kind: ArtifactOwnerKind,
    pub id: String,
    pub created_at_unix_ms: u64,
}

/// One checksummed finalized map result read from durable artifact storage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessorArtifact {
    pub delta: EncodedDelta,
    pub retained_at_unix_ms: u64,
    pub encoded_bytes: u64,
}

/// Logical bounds and payload footprint of one processor's artifact store.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub struct ProcessorArtifactStats {
    pub earliest_block: Option<BlockNumber>,
    pub latest_block: Option<BlockNumber>,
    pub artifacts: u64,
    pub logical_bytes: u64,
    pub owners: u64,
}

/// Work completed by one bounded inline-to-segment compaction pass.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub struct ArtifactTieringOutcome {
    pub segments: u64,
    pub artifacts: u64,
    pub logical_bytes: u64,
    pub inline_payload_bytes_reclaimed: u64,
}

/// Result of releasing one artifact owner and reclaiming newly unowned rows.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub struct ArtifactPruneOutcome {
    pub released_owners: u64,
    pub deleted_artifacts: u64,
    pub deleted_logical_bytes: u64,
}

/// Immutable mapping/schema identity carried by every portable artifact
/// export. Lifecycle policy and processor-instance identity are deliberately
/// excluded so the same map result can feed another compatible view.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProcessorArtifactContract {
    pub processor_id: String,
    pub processor_version: String,
    pub code_hash: BlockHash,
    pub config_hash: BlockHash,
    pub delta_schema_version: u16,
}

impl ProcessorArtifactContract {
    fn from_descriptor(descriptor: &ProcessorDescriptor) -> Self {
        Self {
            processor_id: descriptor.id.to_string(),
            processor_version: descriptor.version.to_string(),
            code_hash: descriptor.code_hash,
            config_hash: descriptor.config_hash,
            delta_schema_version: descriptor.schemas.delta_version,
        }
    }

    fn validate(&self, descriptor: &ProcessorDescriptor) -> Result<(), StoreError> {
        if *self != Self::from_descriptor(descriptor) {
            return Err(StoreError::ArtifactContract);
        }
        Ok(())
    }
}

/// One bounded, deterministic, portable artifact export page.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProcessorArtifactExport {
    pub format_version: u16,
    pub contract: ProcessorArtifactContract,
    pub requested_range: BlockRange,
    pub exported_range: Option<BlockRange>,
    pub complete: bool,
    pub artifacts: Vec<EncodedDelta>,
    pub logical_digest: BlockHash,
}

impl ProcessorArtifactExport {
    /// Encode this export in a kind- and version-tagged checksummed envelope.
    ///
    /// # Errors
    ///
    /// Returns an error when the export is internally inconsistent or cannot
    /// be serialized.
    pub fn encode_durable(&self) -> Result<Vec<u8>, StoreError> {
        self.validate_shape()?;
        leani_primitives::durable::encode(DurableKind::ProcessorArtifactExport, 1, self)
            .map_err(|error| StoreError::Encoding(error.to_string()))
    }

    /// Decode and validate an export against an exact map/delta contract.
    ///
    /// # Errors
    ///
    /// Rejects corrupt envelopes, unknown versions, incompatible contracts,
    /// malformed ordering/ranges, and logical digest mismatches.
    pub fn decode_durable(
        descriptor: &ProcessorDescriptor,
        bytes: &[u8],
    ) -> Result<Self, StoreError> {
        let export: Self =
            leani_primitives::durable::decode(DurableKind::ProcessorArtifactExport, 1, bytes)
                .map_err(|error| StoreError::Encoding(error.to_string()))?;
        export.contract.validate(descriptor)?;
        export.validate_shape()?;
        for artifact in &export.artifacts {
            artifact.validate(descriptor)?;
        }
        Ok(export)
    }

    fn validate_shape(&self) -> Result<(), StoreError> {
        if self.format_version != 1 {
            return Err(StoreError::InvalidConfig(format!(
                "unsupported processor artifact export version {}",
                self.format_version
            )));
        }
        let derived_range = self
            .artifacts
            .first()
            .zip(self.artifacts.last())
            .map(|(first, last)| {
                BlockRange::new(first.block.number, last.block.number)
                    .map_err(|error| StoreError::Invariant(error.to_string()))
            })
            .transpose()?;
        if derived_range != self.exported_range
            || self.artifacts.windows(2).any(|pair| {
                pair[1].chain_id != pair[0].chain_id
                    || pair[1].block.number.0 != pair[0].block.number.0.saturating_add(1)
                    || pair[1].block.parent_hash != pair[0].block.hash
            })
            || self.exported_range.is_some_and(|range| {
                range.start() < self.requested_range.start()
                    || range.end() > self.requested_range.end()
            })
            || self.complete
                != self.exported_range.is_some_and(|range| {
                    range.start() == self.requested_range.start()
                        && range.end() == self.requested_range.end()
                })
            || self.logical_digest != artifact_export_digest(self)
        {
            return Err(StoreError::ArtifactExport);
        }
        Ok(())
    }
}

/// Result of replaying one bounded artifact page into a compatible processor.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub struct ArtifactReplayOutcome {
    pub processed_artifacts: u64,
    pub applied_artifacts: u64,
    pub duplicate_artifacts: u64,
    pub last_block: Option<BlockNumber>,
}

/// Result of reversing one unfinalized processor block.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UndoOutcome {
    pub restored_mutations: usize,
    pub first_change_sequence: Option<u64>,
    pub last_change_sequence: Option<u64>,
}

/// Compact operational store metrics.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct StoreStats {
    pub schema_version: u32,
    pub processor_instances: u64,
    pub entities: u64,
    pub index_entries: u64,
    pub exact_coverage_blocks: u64,
    pub coverage_intervals: u64,
    pub coverage_segments: u64,
    pub coverage_owners: u64,
    pub applied_blocks: u64,
    pub undo_records: u64,
    pub changes: u64,
    pub processor_artifacts: u64,
    pub processor_artifact_bytes: u64,
    pub processor_artifact_owners: u64,
    pub pending_processor_artifacts: u64,
    pub pending_processor_artifact_bytes: u64,
    pub delivery_retained_bytes: u64,
    pub history_delivery_retained_bytes: u64,
    pub database_bytes: u64,
    pub freelist_bytes: u64,
    pub wal_bytes: u64,
    pub physical_file_bytes: u64,
    pub artifact_segment_bytes: u64,
    pub total_physical_bytes: u64,
}

/// Fast-changing storage footprint metrics that do not scan logical tables.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct StoreStorageStats {
    pub database_bytes: u64,
    pub freelist_bytes: u64,
    pub wal_bytes: u64,
    pub physical_file_bytes: u64,
    pub artifact_segment_bytes: u64,
    pub total_physical_bytes: u64,
}

/// Fast-changing queue/storage charges used by dashboards and benchmark
/// high-water sampling without scanning retained output or coverage tables.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct StoreBudgetStats {
    pub delivery_retained_bytes: u64,
    pub history_delivery_retained_bytes: u64,
    pub pending_delta_bytes: u64,
    pub processor_artifact_bytes: u64,
    pub pending_processor_artifact_bytes: u64,
    pub maximum_processor_artifact_bytes: u64,
    pub maximum_pending_processor_artifact_bytes: u64,
    pub maximum_delivery_retained_bytes: u64,
    pub maximum_history_delivery_retained_bytes: u64,
    pub maximum_physical_store_bytes: u64,
    pub database_bytes: u64,
    pub freelist_bytes: u64,
    pub wal_bytes: u64,
    pub physical_file_bytes: u64,
    pub artifact_segment_bytes: u64,
    pub total_physical_bytes: u64,
}

/// Logical rows and payload bytes attributable to one immutable processor
/// instance. `SQLite` page overhead remains reported only at store level.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ProcessorStoreStats {
    pub processor_id: String,
    pub processor_version: String,
    pub entities: u64,
    pub entity_bytes: u64,
    pub state_entries: u64,
    pub state_bytes: u64,
    pub index_entries: u64,
    pub index_bytes: u64,
    pub applied_blocks: u64,
    pub undo_records: u64,
    pub undo_bytes: u64,
    pub changes: u64,
    pub change_bytes: u64,
    pub pending_deltas: u64,
    pub pending_delta_bytes: u64,
    pub processor_artifacts: u64,
    pub processor_artifact_bytes: u64,
    pub processor_artifact_owners: u64,
    pub pending_processor_artifacts: u64,
    pub pending_processor_artifact_bytes: u64,
    pub outbox_records: u64,
    pub recovery_checkpoints: u64,
    pub recovery_checkpoint_bytes: u64,
    pub portable_savepoints: u64,
    pub portable_savepoint_bytes: u64,
}

/// Immutable automatic recovery checkpoint metadata.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RecoveryCheckpoint {
    pub checkpoint_id: u64,
    pub processor_instance: String,
    pub block_number: BlockNumber,
    pub block_hash: BlockHash,
    pub state_checksum: BlockHash,
    pub state_bytes: u64,
    pub created_at_unix_ms: u64,
}

/// Operator-owned portable savepoint metadata.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PortableSavepoint {
    pub savepoint_id: String,
    pub processor_instance: String,
    pub block_number: BlockNumber,
    pub block_hash: BlockHash,
    pub state_checksum: BlockHash,
    pub state_bytes: u64,
    pub created_at_unix_ms: u64,
}

/// Optional retained-output constraints applied while creating one stable
/// query snapshot.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OutputQuery {
    pub from_block: Option<BlockNumber>,
    pub to_block: Option<BlockNumber>,
    pub from_timestamp: Option<u64>,
    pub to_timestamp: Option<u64>,
}

impl OutputQuery {
    fn validate(self) -> Result<Self, StoreError> {
        if self
            .from_block
            .zip(self.to_block)
            .is_some_and(|(from, to)| from > to)
            || self
                .from_timestamp
                .zip(self.to_timestamp)
                .is_some_and(|(from, to)| from > to)
        {
            return Err(StoreError::InvalidConfig(
                "output query lower bound exceeds its upper bound".to_owned(),
            ));
        }
        Ok(self)
    }
}

/// Retained block/time bounds for one materialized collection.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct OutputBounds {
    pub earliest_block: BlockNumber,
    pub latest_block: BlockNumber,
    pub earliest_timestamp: u64,
    pub latest_timestamp: u64,
}

/// Metadata for an immutable, short-lived query snapshot.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct QuerySnapshot {
    pub snapshot_id: [u8; 16],
    pub processor_instance: String,
    pub collection: String,
    pub boundary_sequence: u64,
    pub row_count: u64,
    pub value_bytes: u64,
    pub created_at_unix_ms: u64,
    pub expires_at_unix_ms: u64,
}

/// One row read from a stable query snapshot.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct QuerySnapshotEntity {
    pub ordinal: u64,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub block_number: BlockNumber,
    pub block_timestamp: u64,
    pub finality: Finality,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct StateSnapshot {
    format_version: u16,
    processor_instance: String,
    descriptor_hash: BlockHash,
    cursor: ProcessorCursor,
    entries: Vec<StateSnapshotEntry>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct StateSnapshotEntry {
    namespace: String,
    key: Vec<u8>,
    value: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct PortableSavepointArchive {
    format_version: u16,
    snapshot: Vec<u8>,
    checksum: BlockHash,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct RecentStoreStats {
    pub frames: u64,
    pub encoded_bytes: u64,
    pub earliest_block: Option<BlockNumber>,
    pub latest_block: Option<BlockNumber>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct RecentPruneOutcome {
    pub deleted_frames: u64,
    pub deleted_bytes: u64,
    pub retained_frames: u64,
    pub retained_bytes: u64,
    pub hard_limit_exceeded: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct RecentReorgOutcome {
    pub reverted_frames: u64,
    pub applied_frames: u64,
    pub new_tip: BlockRef,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct RecentTransactionLocation {
    pub block_number: BlockNumber,
    pub block_hash: BlockHash,
    pub transaction_index: u32,
}

/// Durable state for one processor's exact historical/live reconciliation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct HotColdHandoffRecord {
    pub id: String,
    pub chain_id: ChainId,
    pub processor_id: String,
    pub processor_version: String,
    pub overlap: BlockRange,
    pub anchor_hash: BlockHash,
    pub state: HotColdHandoffState,
    pub compared_blocks: u64,
    pub failure: Option<String>,
    pub updated_at_unix_ms: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HotColdHandoffState {
    Running,
    Verified,
    Failed,
}

/// Durable verdict comparing archive-remapped processor deltas with the
/// checksums originally produced from the live execution-P2P stream.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ArchiveReconciliationRecord {
    pub id: String,
    pub chain_id: ChainId,
    pub processor_id: String,
    pub processor_version: String,
    pub source_id: String,
    pub overlap: BlockRange,
    pub anchor_hash: BlockHash,
    pub state: ArchiveReconciliationState,
    pub compared_blocks: u64,
    pub failure: Option<String>,
    pub updated_at_unix_ms: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ArchiveReconciliationState {
    Running,
    Verified,
    Failed,
}

impl ArchiveReconciliationState {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Verified => "verified",
            Self::Failed => "failed",
        }
    }

    fn parse(value: &str) -> Result<Self, StoreError> {
        match value {
            "running" => Ok(Self::Running),
            "verified" => Ok(Self::Verified),
            "failed" => Ok(Self::Failed),
            other => Err(StoreError::Invariant(format!(
                "unknown archive reconciliation state {other:?}"
            ))),
        }
    }
}

impl HotColdHandoffState {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Verified => "verified",
            Self::Failed => "failed",
        }
    }

    fn parse(value: &str) -> Result<Self, StoreError> {
        match value {
            "running" => Ok(Self::Running),
            "verified" => Ok(Self::Verified),
            "failed" => Ok(Self::Failed),
            other => Err(StoreError::Invariant(format!(
                "unknown hot/cold handoff state {other:?}"
            ))),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Queued,
    Running,
    StorageBackpressured,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BackfillSubscriptionMode {
    FillMissing,
    Recompute,
}

impl BackfillSubscriptionMode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::FillMissing => "fill_missing",
            Self::Recompute => "recompute",
        }
    }

    fn parse(value: &str) -> Result<Self, StoreError> {
        match value {
            "fill_missing" => Ok(Self::FillMissing),
            "recompute" => Ok(Self::Recompute),
            other => Err(StoreError::Invariant(format!(
                "unknown backfill subscription mode {other:?}"
            ))),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BackfillSubscriptionState {
    WaitingForConsumer,
    Queued,
    Running,
    Backpressured,
    Draining,
    CompleteReclaimable,
    Cancelled,
    Failed,
}

impl BackfillSubscriptionState {
    const fn as_str(self) -> &'static str {
        match self {
            Self::WaitingForConsumer => "waiting_for_consumer",
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Backpressured => "backpressured",
            Self::Draining => "draining",
            Self::CompleteReclaimable => "complete_reclaimable",
            Self::Cancelled => "cancelled",
            Self::Failed => "failed",
        }
    }

    fn parse(value: &str) -> Result<Self, StoreError> {
        match value {
            "waiting_for_consumer" => Ok(Self::WaitingForConsumer),
            "queued" => Ok(Self::Queued),
            "running" => Ok(Self::Running),
            "backpressured" => Ok(Self::Backpressured),
            "draining" => Ok(Self::Draining),
            "complete_reclaimable" => Ok(Self::CompleteReclaimable),
            "cancelled" => Ok(Self::Cancelled),
            "failed" => Ok(Self::Failed),
            other => Err(StoreError::Invariant(format!(
                "unknown backfill subscription state {other:?}"
            ))),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BackfillSubscriptionRecord {
    pub subscription_id: String,
    pub job_id: String,
    pub processor_instance: String,
    pub history_stream_id: String,
    pub mode: BackfillSubscriptionMode,
    pub publication_revision: u64,
    pub state: BackfillSubscriptionState,
    pub consumer_id: String,
    /// Immutable normalized requested ranges. Legacy records may leave this
    /// empty and use `range` as a single range.
    pub ranges: Vec<BlockRange>,
    /// Bounding range retained for schema and API compatibility.
    pub range: BlockRange,
    /// Coverage captured before this subscription became schedulable.
    pub preexisting_coverage: Vec<BlockRange>,
    pub captured_finalized_target: BlockNumber,
    pub idempotency_key: String,
    pub effective_block_limit: u64,
    pub effective_byte_limit: u64,
    pub resume_below_ratio_millionths: u32,
    pub delivery_batch_limits: BackfillDeliveryBatchLimits,
    pub initial_sequence: u64,
    pub completion_sequence: Option<u64>,
    pub processed_work_blocks: u64,
}

/// Durable work progress for one normalized subscription request range.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct BackfillSubscriptionRangeProgress {
    pub range: BlockRange,
    /// Number of blocks from this range's immutable creation-time work set
    /// that have been atomically published to the subscription stream.
    pub committed_work_blocks: u64,
}

/// Effective immutable history delivery-batch limits captured when a
/// subscription is created.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct BackfillDeliveryBatchLimits {
    pub target_encoded_bytes: u64,
    pub maximum_encoded_bytes: u64,
    pub maximum_events: u64,
    pub maximum_processed_blocks: u64,
    pub maximum_delay_ms: u64,
    pub maximum_buffered_batches: u64,
    pub maximum_buffered_bytes: u64,
    pub compression: BackfillDeliveryCompression,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BackfillDeliveryCompression {
    None,
    #[default]
    Gzip,
}

impl BackfillDeliveryCompression {
    const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Gzip => "gzip",
        }
    }

    fn parse(value: &str) -> Result<Self, StoreError> {
        match value {
            "none" => Ok(Self::None),
            "gzip" => Ok(Self::Gzip),
            other => Err(StoreError::Invariant(format!(
                "unknown backfill delivery compression {other:?}"
            ))),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BackfillCompletionDisposition {
    PublishedAll,
    PublishedMissingOnly,
    AlreadyCoveredNoop,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BackfillCompletionMetadata {
    pub mode: BackfillSubscriptionMode,
    pub disposition: BackfillCompletionDisposition,
    pub requested_blocks: u64,
    pub covered_before_request_blocks: u64,
    pub covered_before_request_ranges: Vec<BlockRange>,
    pub newly_processed_blocks: u64,
    pub republished_blocks: u64,
    pub domain_changes: u64,
}

/// Decode versioned terminal metadata carried by a backfill completion
/// delivery record.
///
/// # Errors
///
/// Returns an error when the payload version, enum tags, length, or counts are
/// invalid.
#[allow(clippy::too_many_lines)]
pub fn decode_backfill_completion_metadata(
    payload: &[u8],
) -> Result<BackfillCompletionMetadata, StoreError> {
    const V1_ENCODED_LEN: usize = 44;
    const V2_HEADER_LEN: usize = 48;
    if payload.len() < V1_ENCODED_LEN {
        return Err(StoreError::Invariant(
            "invalid backfill completion payload version or length".to_owned(),
        ));
    }
    let version = u16::from_be_bytes(
        payload
            .get(0..2)
            .ok_or_else(|| {
                StoreError::Invariant("invalid backfill completion version bytes".to_owned())
            })?
            .try_into()
            .map_err(|_| {
                StoreError::Invariant("invalid backfill completion version bytes".to_owned())
            })?,
    );
    if (version == 1 && payload.len() != V1_ENCODED_LEN)
        || (version == 2 && payload.len() < V2_HEADER_LEN)
        || !matches!(version, 1 | 2)
    {
        return Err(StoreError::Invariant(
            "invalid backfill completion payload version or length".to_owned(),
        ));
    }
    let mode = match payload.get(2).copied() {
        Some(0) => BackfillSubscriptionMode::FillMissing,
        Some(1) => BackfillSubscriptionMode::Recompute,
        _ => {
            return Err(StoreError::Invariant(
                "invalid backfill completion mode".to_owned(),
            ));
        }
    };
    let disposition = match payload.get(3).copied() {
        Some(0) => BackfillCompletionDisposition::PublishedAll,
        Some(1) => BackfillCompletionDisposition::PublishedMissingOnly,
        Some(2) => BackfillCompletionDisposition::AlreadyCoveredNoop,
        _ => {
            return Err(StoreError::Invariant(
                "invalid backfill completion disposition".to_owned(),
            ));
        }
    };
    let number = |offset: usize| -> Result<u64, StoreError> {
        let bytes = payload
            .get(offset..offset.saturating_add(8))
            .ok_or_else(|| {
                StoreError::Invariant("invalid backfill completion numeric field".to_owned())
            })?
            .try_into()
            .map_err(|_| {
                StoreError::Invariant("invalid backfill completion numeric bytes".to_owned())
            })?;
        Ok(u64::from_be_bytes(bytes))
    };
    let covered_before_request_ranges = if version == 1 {
        Vec::new()
    } else {
        let range_count = u32::from_be_bytes(
            payload
                .get(44..48)
                .ok_or_else(|| {
                    StoreError::Invariant("invalid backfill completion range count".to_owned())
                })?
                .try_into()
                .map_err(|_| {
                    StoreError::Invariant("invalid backfill completion range count".to_owned())
                })?,
        );
        let range_count = usize::try_from(range_count)
            .map_err(|_| StoreError::Numeric("completion range count"))?;
        let expected_len = V2_HEADER_LEN
            .checked_add(
                range_count
                    .checked_mul(16)
                    .ok_or(StoreError::Numeric("completion range bytes"))?,
            )
            .ok_or(StoreError::Numeric("completion range bytes"))?;
        if payload.len() != expected_len {
            return Err(StoreError::Invariant(
                "invalid backfill completion range payload length".to_owned(),
            ));
        }
        (0..range_count)
            .map(|index| {
                let offset = V2_HEADER_LEN + index * 16;
                BlockRange::new(
                    BlockNumber(number(offset)?),
                    BlockNumber(number(offset + 8)?),
                )
                .map_err(|error| StoreError::Invariant(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?
    };
    let metadata = BackfillCompletionMetadata {
        mode,
        disposition,
        requested_blocks: number(4)?,
        covered_before_request_blocks: number(12)?,
        covered_before_request_ranges,
        newly_processed_blocks: number(20)?,
        republished_blocks: number(28)?,
        domain_changes: number(36)?,
    };
    if metadata.republished_blocks > metadata.requested_blocks
        || metadata.covered_before_request_blocks > metadata.requested_blocks
        || metadata.newly_processed_blocks > metadata.requested_blocks
    {
        return Err(StoreError::Invariant(
            "invalid backfill completion counts".to_owned(),
        ));
    }
    let covered_range_blocks =
        metadata
            .covered_before_request_ranges
            .iter()
            .try_fold(0_u64, |total, range| {
                total
                    .checked_add(range.len())
                    .ok_or(StoreError::Numeric("completion covered range blocks"))
            })?;
    if version == 2 && covered_range_blocks != metadata.covered_before_request_blocks {
        return Err(StoreError::Invariant(
            "backfill completion covered ranges do not match the covered block count".to_owned(),
        ));
    }
    Ok(metadata)
}

impl Default for BackfillDeliveryBatchLimits {
    fn default() -> Self {
        Self {
            target_encoded_bytes: 4 * 1024 * 1024,
            maximum_encoded_bytes: 16 * 1024 * 1024,
            maximum_events: 20_000,
            maximum_processed_blocks: 8_192,
            maximum_delay_ms: 50,
            maximum_buffered_batches: 4,
            maximum_buffered_bytes: 64 * 1024 * 1024,
            compression: BackfillDeliveryCompression::Gzip,
        }
    }
}

impl JobState {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::StorageBackpressured => "storage_backpressured",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    fn parse(value: &str) -> Result<Self, StoreError> {
        match value {
            "queued" => Ok(Self::Queued),
            "running" => Ok(Self::Running),
            "storage_backpressured" => Ok(Self::StorageBackpressured),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            other => Err(StoreError::Invariant(format!(
                "unknown job state {other:?}"
            ))),
        }
    }
}

/// Durable scheduler job and its source-neutral checkpoint.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct JobRecord {
    pub id: String,
    pub kind: String,
    pub state: JobState,
    pub payload: Vec<u8>,
    pub checkpoint: Option<Vec<u8>>,
    pub attempts: u32,
    pub updated_at_unix_ms: u64,
}

/// Rows removed by an explicit terminal historical-work deletion.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct HistoricalWorkDeletion {
    pub jobs: u64,
    pub subscription_ranges: u64,
    pub consumers: u64,
    pub delivery_records: u64,
    pub delivery_streams: u64,
    pub coverage_intervals: u64,
    pub coverage_segments: u64,
    pub exact_coverage: u64,
    pub applied_blocks: u64,
    pub finalized_undo: u64,
}

#[derive(Debug)]
struct WriterArbiter {
    state: StdMutex<WriterArbiterState>,
    changed: Notify,
}

#[derive(Debug, Default)]
struct WriterArbiterState {
    active: bool,
    live_waiters: u64,
    history_waiters: u64,
    next_history_ticket: u64,
    history_queue: VecDeque<u64>,
}

#[derive(Clone, Copy, Debug)]
enum WriterPriority {
    Live,
    History,
}

struct WriterWaitRegistration<'a> {
    arbiter: &'a WriterArbiter,
    priority: WriterPriority,
    history_ticket: Option<u64>,
    waiting: bool,
}

impl Drop for WriterWaitRegistration<'_> {
    fn drop(&mut self) {
        if !self.waiting {
            return;
        }
        let mut state = self
            .arbiter
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match self.priority {
            WriterPriority::Live => {
                state.live_waiters = state.live_waiters.saturating_sub(1);
            }
            WriterPriority::History => {
                state.history_waiters = state.history_waiters.saturating_sub(1);
                if let Some(ticket) = self.history_ticket
                    && let Some(position) = state
                        .history_queue
                        .iter()
                        .position(|queued| *queued == ticket)
                {
                    state.history_queue.remove(position);
                }
            }
        }
        drop(state);
        self.arbiter.changed.notify_waiters();
    }
}

struct WriterPermit<'a> {
    arbiter: &'a WriterArbiter,
}

impl Drop for WriterPermit<'_> {
    fn drop(&mut self) {
        let mut state = self
            .arbiter
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.active = false;
        drop(state);
        self.arbiter.changed.notify_waiters();
    }
}

impl WriterArbiter {
    fn new() -> Self {
        Self {
            state: StdMutex::new(WriterArbiterState::default()),
            changed: Notify::new(),
        }
    }

    async fn lock(&self) -> WriterPermit<'_> {
        self.lock_with_priority(WriterPriority::Live).await
    }

    async fn lock_history(&self) -> WriterPermit<'_> {
        self.lock_with_priority(WriterPriority::History).await
    }

    async fn lock_with_priority(&self, priority: WriterPriority) -> WriterPermit<'_> {
        let history_ticket = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match priority {
                WriterPriority::Live => {
                    state.live_waiters = state.live_waiters.saturating_add(1);
                    None
                }
                WriterPriority::History => {
                    state.history_waiters = state.history_waiters.saturating_add(1);
                    let ticket = state.next_history_ticket;
                    state.next_history_ticket = state.next_history_ticket.saturating_add(1);
                    state.history_queue.push_back(ticket);
                    Some(ticket)
                }
            }
        };
        let mut registration = WriterWaitRegistration {
            arbiter: self,
            priority,
            history_ticket,
            waiting: true,
        };
        loop {
            let changed = self.changed.notified();
            {
                let mut state = self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let eligible = !state.active
                    && match priority {
                        WriterPriority::Live => true,
                        WriterPriority::History => {
                            state.live_waiters == 0
                                && state.history_queue.front().copied() == history_ticket
                        }
                    };
                if eligible {
                    state.active = true;
                    match priority {
                        WriterPriority::Live => {
                            state.live_waiters = state.live_waiters.saturating_sub(1);
                        }
                        WriterPriority::History => {
                            state.history_waiters = state.history_waiters.saturating_sub(1);
                            let admitted = state.history_queue.pop_front();
                            debug_assert_eq!(admitted, history_ticket);
                        }
                    }
                    registration.waiting = false;
                    return WriterPermit { arbiter: self };
                }
            }
            changed.await;
        }
    }
}

#[derive(Debug)]
struct StoreInner {
    pool: SqlitePool,
    writer: WriterArbiter,
    delivery_capacity_changed: Notify,
    delivery_changes_available: Notify,
    path: PathBuf,
    epoch: [u8; 16],
    storage_budget: StoreStorageBudget,
    delivery_budget: DeliveryStorageBudget,
    artifact_budget: ArtifactStorageBudget,
    artifact_segments: Option<ArtifactSegmentStorage>,
    artifact_compaction: Mutex<()>,
}

#[derive(Clone, Debug)]
struct ArtifactSegmentStorage {
    root: PathBuf,
    target_blocks: u64,
    maximum_segment_physical_bytes: u64,
    sink: ArtifactSegmentSink,
}

/// Cloneable durable store handle.
#[derive(Clone, Debug)]
pub struct SqliteStore {
    inner: Arc<StoreInner>,
}

/// Storage-neutral contract for immutable finalized processor artifacts.
///
/// The first implementation is [`SQLite`](SqliteStore). Keeping the runtime
/// behind this boundary lets a measured append-only segment backend replace
/// it without changing processor or replay semantics.
#[async_trait]
pub trait ProcessorArtifactStore: Send + Sync {
    async fn retain_finalized_artifact(
        &self,
        descriptor: &ProcessorDescriptor,
        delta: &EncodedDelta,
        finality: Finality,
    ) -> Result<(), StoreError>;

    async fn processor_artifact(
        &self,
        descriptor: &ProcessorDescriptor,
        block: BlockNumber,
    ) -> Result<Option<ProcessorArtifact>, StoreError>;

    async fn scan_processor_artifacts(
        &self,
        descriptor: &ProcessorDescriptor,
        range: BlockRange,
        limit: usize,
    ) -> Result<Vec<ProcessorArtifact>, StoreError>;

    async fn processor_artifact_stats(
        &self,
        descriptor: &ProcessorDescriptor,
    ) -> Result<ProcessorArtifactStats, StoreError>;

    async fn export_processor_artifacts(
        &self,
        descriptor: &ProcessorDescriptor,
        range: BlockRange,
        limit: usize,
    ) -> Result<ProcessorArtifactExport, StoreError>;
}

async fn migrate(pool: &SqlitePool) -> Result<(), StoreError> {
    let encoded: Vec<u8> =
        sqlx::query_scalar("SELECT value FROM node_meta WHERE key = 'schema_version'")
            .fetch_one(pool)
            .await?;
    let bytes: [u8; 4] = encoded
        .try_into()
        .map_err(|_| StoreError::Invariant("schema_version must contain four bytes".to_owned()))?;
    let version = u32::from_be_bytes(bytes);
    if version == CURRENT_SCHEMA_VERSION {
        return Ok(());
    }
    if version > CURRENT_SCHEMA_VERSION {
        return Err(StoreError::InvalidConfig(format!(
            "database schema {version} is newer than supported schema {CURRENT_SCHEMA_VERSION}"
        )));
    }
    if version == 0 {
        return Err(StoreError::InvalidConfig(
            "database schema 0 cannot be upgraded by this binary".to_owned(),
        ));
    }

    let migrations = [
        (2, SCHEMA_V2),
        (3, SCHEMA_V3),
        (4, SCHEMA_V4),
        (5, SCHEMA_V5),
        (6, SCHEMA_V6),
        (7, SCHEMA_V7),
        (8, SCHEMA_V8),
        (9, SCHEMA_V9),
        (10, SCHEMA_V10),
        (11, SCHEMA_V11),
        (12, SCHEMA_V12),
        (13, SCHEMA_V13),
        (14, SCHEMA_V14),
        (15, SCHEMA_V15),
        (16, SCHEMA_V16),
        (17, SCHEMA_V17),
        (18, SCHEMA_V18),
        (19, SCHEMA_V19),
    ];
    let mut transaction = pool.begin().await?;
    for (target, migration) in migrations {
        if target > version {
            sqlx::raw_sql(migration).execute(&mut *transaction).await?;
        }
    }
    transaction.commit().await?;
    if version <= 2 {
        rebuild_recent_transaction_locators(pool).await?;
    }
    Ok(())
}

impl SqliteStore {
    /// Open a WAL-mode store and apply embedded migrations.
    ///
    /// # Errors
    ///
    /// Fails before returning if the path, `SQLite` settings, or migration is
    /// invalid.
    #[allow(clippy::too_many_lines)]
    pub async fn open(config: StoreConfig) -> Result<Self, StoreError> {
        if config.reader_connections == 0 {
            return Err(StoreError::InvalidConfig(
                "reader_connections must be greater than zero".to_owned(),
            ));
        }
        if config.busy_timeout.is_zero() {
            return Err(StoreError::InvalidConfig(
                "busy_timeout must be non-zero".to_owned(),
            ));
        }
        let storage_budget = config.storage_budget;
        if storage_budget.maximum_physical_bytes == 0 {
            return Err(StoreError::InvalidConfig(
                "store physical budget must be greater than zero".to_owned(),
            ));
        }
        let delivery_budget = config.delivery_budget;
        if delivery_budget.maximum_retained_bytes == 0
            || delivery_budget.maximum_history_retained_bytes == 0
            || delivery_budget.maximum_history_retained_bytes
                > delivery_budget.maximum_retained_bytes
        {
            return Err(StoreError::InvalidConfig(
                "delivery budgets must satisfy 0 < history retained <= total retained".to_owned(),
            ));
        }
        let artifact_budget = config.artifact_budget;
        if artifact_budget.maximum_retained_bytes == 0 || artifact_budget.maximum_pending_bytes == 0
        {
            return Err(StoreError::InvalidConfig(
                "artifact retained and pending budgets must be greater than zero".to_owned(),
            ));
        }
        let artifact_segments = config
            .artifact_segments
            .map(|segments| {
                if segments.target_blocks == 0 {
                    return Err(StoreError::InvalidConfig(
                        "artifact segment target blocks must be greater than zero".to_owned(),
                    ));
                }
                let sink = ArtifactSegmentSink::open(&segments.root, segments.sink_config())
                    .map_err(|error| StoreError::ArtifactSegment(error.to_string()))?;
                Ok(ArtifactSegmentStorage {
                    root: segments.root,
                    target_blocks: segments.target_blocks,
                    maximum_segment_physical_bytes: segments.maximum_segment_physical_bytes,
                    sink,
                })
            })
            .transpose()?;
        let new_store = !config.path.exists();
        if let Some(parent) = config.path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }
        let synchronous = match config.durability {
            Durability::Full => SqliteSynchronous::Full,
            Durability::Normal => SqliteSynchronous::Normal,
        };
        let options =
            SqliteConnectOptions::from_str(&format!("sqlite://{}", config.path.to_string_lossy()))?
                .create_if_missing(true)
                .foreign_keys(true)
                .journal_mode(SqliteJournalMode::Wal)
                .synchronous(synchronous)
                .busy_timeout(config.busy_timeout);
        let pool = SqlitePoolOptions::new()
            .min_connections(1)
            .max_connections(config.reader_connections.saturating_add(1))
            .connect_with(options)
            .await?;
        if new_store {
            sqlx::query("PRAGMA auto_vacuum = INCREMENTAL")
                .execute(&pool)
                .await?;
        }
        sqlx::raw_sql(SCHEMA_V1).execute(&pool).await?;
        migrate(&pool).await?;
        let now = now_i64()?;
        let mut hasher = blake3::Hasher::new();
        hasher.update(config.path.to_string_lossy().as_bytes());
        hasher.update(&now.to_be_bytes());
        let digest = hasher.finalize();
        let generated = &digest.as_bytes()[..16];
        sqlx::query(
            "INSERT INTO node_meta(key, value) VALUES ('store_epoch', ?)
             ON CONFLICT(key) DO NOTHING",
        )
        .bind(generated)
        .execute(&pool)
        .await?;
        let epoch: Vec<u8> =
            sqlx::query_scalar("SELECT value FROM node_meta WHERE key = 'store_epoch'")
                .fetch_one(&pool)
                .await?;
        let epoch: [u8; 16] = epoch
            .try_into()
            .map_err(|_| StoreError::Invariant("store epoch is not 16 bytes".to_owned()))?;
        let store = Self {
            inner: Arc::new(StoreInner {
                pool,
                writer: WriterArbiter::new(),
                delivery_capacity_changed: Notify::new(),
                delivery_changes_available: Notify::new(),
                path: config.path,
                epoch,
                storage_budget,
                delivery_budget,
                artifact_budget,
                artifact_segments,
                artifact_compaction: Mutex::new(()),
            }),
        };
        store.recover_artifact_segment_catalog().await?;
        Ok(store)
    }

    async fn recover_artifact_segment_catalog(&self) -> Result<(), StoreError> {
        let rows = sqlx::query(
            "SELECT segment_id, instance, from_block, to_block, relative_path,
                    artifacts, logical_bytes, physical_bytes, records_checksum, state
             FROM processor_artifact_segments ORDER BY instance, from_block",
        )
        .fetch_all(&self.inner.pool)
        .await?;
        if rows.is_empty() {
            return Ok(());
        }
        let storage = self.inner.artifact_segments.as_ref().ok_or_else(|| {
            StoreError::InvalidConfig(
                "SQLite references processor artifact segments but the segment tier is disabled"
                    .to_owned(),
            )
        })?;
        for row in rows {
            let segment_id: String = row.try_get("segment_id")?;
            let instance: String = row.try_get("instance")?;
            let range = BlockRange::new(
                BlockNumber(i64_u64(
                    row.try_get("from_block")?,
                    "artifact segment start",
                )?),
                BlockNumber(i64_u64(row.try_get("to_block")?, "artifact segment end")?),
            )
            .map_err(|error| StoreError::Invariant(error.to_string()))?;
            let state: String = row.try_get("state")?;
            if state == "deleting" {
                storage
                    .sink
                    .remove_exact(&instance, range)
                    .await
                    .map_err(|error| StoreError::ArtifactSegment(error.to_string()))?;
                sqlx::query(
                    "DELETE FROM processor_artifact_segments
                     WHERE segment_id = ? AND state = 'deleting'",
                )
                .bind(segment_id)
                .execute(&self.inner.pool)
                .await?;
                continue;
            }
            if state != "active" {
                return Err(StoreError::Invariant(format!(
                    "unknown processor artifact segment state {state:?}"
                )));
            }
            let receipt = storage
                .sink
                .retained_batch(&instance, range)
                .await
                .ok_or_else(|| {
                    StoreError::Invariant(format!(
                        "artifact segment catalog entry {segment_id} has no closed file"
                    ))
                })?;
            let relative_path = relative_segment_path(&storage.root, &receipt.path)?;
            let checksum: Vec<u8> = row.try_get("records_checksum")?;
            if row.try_get::<String, _>("relative_path")? != relative_path
                || i64_u64(row.try_get("artifacts")?, "artifact segment count")?
                    != receipt.artifacts
                || i64_u64(
                    row.try_get("logical_bytes")?,
                    "artifact segment logical bytes",
                )? != receipt.logical_bytes
                || i64_u64(
                    row.try_get("physical_bytes")?,
                    "artifact segment physical bytes",
                )? != receipt.physical_bytes
                || checksum.as_slice() != receipt.records_checksum
            {
                return Err(StoreError::Invariant(format!(
                    "artifact segment catalog entry {segment_id} conflicts with its closed file"
                )));
            }
        }
        Ok(())
    }

    /// Register an immutable processor identity and its current operational
    /// lifecycle policy.
    ///
    /// Re-registering the same identity is idempotent. Code, configuration,
    /// requirements, ordering, publication, and start-point drift under the
    /// same instance key is rejected. Lifecycle limits are mutable operator
    /// policy so a failed lane can be repaired by raising a hard limit.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid descriptor, database failure, encoding
    /// failure, or identity collision.
    pub async fn register_processor(
        &self,
        descriptor: &ProcessorDescriptor,
    ) -> Result<String, StoreError> {
        descriptor
            .validate()
            .map_err(|error| StoreError::InvalidConfig(error.to_owned()))?;
        let instance = processor_instance(descriptor);
        let stream_id = default_delivery_stream_id(descriptor);
        let descriptor_json = serde_json::to_string(descriptor)?;
        let _guard = self.inner.writer.lock().await;
        sqlx::query(
            "INSERT INTO processor_instances(
                instance, processor_id, processor_version, code_hash, config_hash,
                descriptor_json, created_at_unix_ms
             ) VALUES (?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(instance) DO NOTHING",
        )
        .bind(&instance)
        .bind(descriptor.id.as_str())
        .bind(descriptor.version.to_string())
        .bind(descriptor.code_hash.0.as_slice())
        .bind(descriptor.config_hash.0.as_slice())
        .bind(&descriptor_json)
        .bind(now_i64()?)
        .execute(&self.inner.pool)
        .await?;
        if descriptor.lifecycle.delivery.mode != DeliveryPolicyMode::None {
            sqlx::query(
                "INSERT INTO delivery_streams(
                    stream_id, instance, stream_kind, created_at_unix_ms
                 ) VALUES (?, ?, ?, ?)
                 ON CONFLICT(stream_id) DO NOTHING",
            )
            .bind(&stream_id)
            .bind(&instance)
            .bind(default_delivery_stream_kind(descriptor).as_str())
            .bind(now_i64()?)
            .execute(&self.inner.pool)
            .await?;
        }
        sqlx::query(
            "INSERT INTO processor_runtime_state(instance, state, updated_at_unix_ms)
             VALUES (?, 'running', ?)
             ON CONFLICT(instance) DO NOTHING",
        )
        .bind(&instance)
        .bind(now_i64()?)
        .execute(&self.inner.pool)
        .await?;
        let stored: String = sqlx::query_scalar(
            "SELECT descriptor_json FROM processor_instances WHERE instance = ?",
        )
        .bind(&instance)
        .fetch_one(&self.inner.pool)
        .await?;
        let stored: ProcessorDescriptor = serde_json::from_str(&stored)?;
        if stored != *descriptor && !same_processor_identity(&stored, descriptor) {
            return Err(StoreError::ProcessorIdentity(instance));
        }
        if stored != *descriptor {
            sqlx::query("UPDATE processor_instances SET descriptor_json = ? WHERE instance = ?")
                .bind(descriptor_json)
                .bind(&instance)
                .execute(&self.inner.pool)
                .await?;
        }
        Ok(instance)
    }

    /// Create or verify the immutable delivery stream for one backfill.
    ///
    /// Reusing the same subscription identity is idempotent. A backfill stream
    /// can only be created for a split, block-local processor.
    ///
    /// # Errors
    ///
    /// Returns an error for an incompatible descriptor or subscription ID, or
    /// when the stream cannot be persisted or read back.
    pub async fn create_backfill_delivery_stream(
        &self,
        descriptor: &ProcessorDescriptor,
        subscription_id: &str,
    ) -> Result<DeliveryStream, StoreError> {
        if descriptor.delivery_ordering != DeliveryOrdering::BlockVersionedIdempotent {
            return Err(StoreError::InvalidConfig(
                "independent backfill streams require block-versioned delivery".to_owned(),
            ));
        }
        if descriptor.lifecycle.delivery.mode == DeliveryPolicyMode::None {
            return Err(StoreError::InvalidConfig(
                "backfill delivery streams require delivery to be enabled".to_owned(),
            ));
        }
        if !valid_subscription_id(subscription_id) {
            return Err(StoreError::InvalidConfig(
                "subscription ID must contain 1-256 portable ASCII characters".to_owned(),
            ));
        }
        let instance = self.register_processor(descriptor).await?;
        let stream_id = backfill_delivery_stream_id(descriptor, subscription_id);
        let now = now_i64()?;
        let _guard = self.inner.writer.lock_history().await;
        sqlx::query(
            "INSERT INTO delivery_streams(
                stream_id, instance, stream_kind, subscription_id,
                created_at_unix_ms
             ) VALUES (?, ?, 'backfill', ?, ?)
             ON CONFLICT(stream_id) DO NOTHING",
        )
        .bind(&stream_id)
        .bind(&instance)
        .bind(subscription_id)
        .bind(now)
        .execute(&self.inner.pool)
        .await?;
        self.delivery_stream(&stream_id)
            .await?
            .ok_or_else(|| StoreError::Invariant("created delivery stream disappeared".to_owned()))
    }

    /// Inspect one immutable delivery stream by its opaque stable ID.
    ///
    /// # Errors
    ///
    /// Returns an error when stored stream metadata is invalid or the query
    /// fails.
    pub async fn delivery_stream(
        &self,
        stream_id: &str,
    ) -> Result<Option<DeliveryStream>, StoreError> {
        let row = sqlx::query(
            "SELECT stream_id, instance, stream_kind, subscription_id,
                    created_at_unix_ms
             FROM delivery_streams WHERE stream_id = ?",
        )
        .bind(stream_id)
        .fetch_optional(&self.inner.pool)
        .await?;
        row.map(|row| {
            Ok(DeliveryStream {
                stream_id: row.try_get("stream_id")?,
                processor_instance: row.try_get("instance")?,
                kind: DeliveryStreamKind::parse(row.try_get("stream_kind")?)?,
                subscription_id: row.try_get("subscription_id")?,
                created_at_unix_ms: i64_u64(
                    row.try_get("created_at_unix_ms")?,
                    "delivery stream creation time",
                )?,
            })
        })
        .transpose()
    }

    /// List every delivery stream owned by one processor instance.
    ///
    /// # Errors
    ///
    /// Returns an error when processor registration, decoding, or the query
    /// fails.
    pub async fn delivery_streams(
        &self,
        descriptor: &ProcessorDescriptor,
    ) -> Result<Vec<DeliveryStream>, StoreError> {
        self.register_processor(descriptor).await?;
        let rows = sqlx::query(
            "SELECT stream_id, instance, stream_kind, subscription_id,
                    created_at_unix_ms
             FROM delivery_streams
             WHERE instance = ?
             ORDER BY created_at_unix_ms, stream_id",
        )
        .bind(processor_instance(descriptor))
        .fetch_all(&self.inner.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(DeliveryStream {
                    stream_id: row.try_get("stream_id")?,
                    processor_instance: row.try_get("instance")?,
                    kind: DeliveryStreamKind::parse(row.try_get("stream_kind")?)?,
                    subscription_id: row.try_get("subscription_id")?,
                    created_at_unix_ms: i64_u64(
                        row.try_get("created_at_unix_ms")?,
                        "delivery stream creation time",
                    )?,
                })
            })
            .collect()
    }

    /// Append the unique terminal boundary for one split history stream.
    ///
    /// The record is idempotent and stream-scoped. A restart after committing
    /// the final block but before publishing completion can safely call this
    /// method again.
    ///
    /// # Errors
    ///
    /// Returns an error for an incompatible stream, exceeded capacity, invalid
    /// stored metadata, or a failed transaction.
    pub async fn append_backfill_completion(
        &self,
        descriptor: &ProcessorDescriptor,
        stream_id: &str,
        chain_id: ChainId,
        through_block: BlockNumber,
    ) -> Result<u64, StoreError> {
        let instance = self.register_processor(descriptor).await?;
        self.validate_delivery_stream(&instance, stream_id).await?;
        let stream = self
            .delivery_stream(stream_id)
            .await?
            .ok_or_else(|| StoreError::Invariant("delivery stream disappeared".to_owned()))?;
        if stream.kind != DeliveryStreamKind::Backfill {
            return Err(StoreError::InvalidConfig(
                "backfill completion requires a backfill delivery stream".to_owned(),
            ));
        }
        let _guard = self.inner.writer.lock_history().await;
        let existing = sqlx::query_scalar::<_, i64>(
            "SELECT stream_sequence FROM change_log
             WHERE stream_id = ? AND kind = 'system.backfill_complete'",
        )
        .bind(stream_id)
        .fetch_optional(&self.inner.pool)
        .await?;
        let subscription_id = stream.subscription_id.ok_or_else(|| {
            StoreError::Invariant("backfill stream has no subscription identity".to_owned())
        })?;
        if existing.is_none() {
            let (preexisting_ranges, processed): (i64, i64) = sqlx::query_as(
                "SELECT (
                    SELECT COUNT(*) FROM backfill_subscription_preexisting_ranges
                    WHERE subscription_id = subscriptions.subscription_id
                 ), processed_work_blocks
                 FROM backfill_subscriptions AS subscriptions
                 WHERE subscription_id = ?",
            )
            .bind(&subscription_id)
            .fetch_one(&self.inner.pool)
            .await?;
            let completion_payload_bytes = 48_u64
                .checked_add(
                    i64_u64(preexisting_ranges, "preexisting coverage range count")?
                        .saturating_mul(16),
                )
                .ok_or(StoreError::Numeric("backfill completion bytes"))?;
            let zero_progress_bytes = if processed == 0 { 32 } else { 0 };
            let incoming_bytes = u64::try_from(subscription_id.len())
                .map_err(|_| StoreError::Numeric("backfill completion bytes"))?
                .checked_add(completion_payload_bytes)
                .and_then(|bytes| bytes.checked_add(zero_progress_bytes))
                .ok_or(StoreError::Numeric("backfill completion bytes"))?;
            enforce_delivery_capacity(
                &self.inner,
                descriptor,
                &instance,
                stream_id,
                incoming_bytes,
                0,
            )
            .await?;
        }
        let mut transaction = self.inner.pool.begin().await?;
        let sequence = append_backfill_completion_transaction(
            &mut transaction,
            &instance,
            stream_id,
            chain_id,
            &subscription_id,
            through_block,
        )
        .await?;
        transaction.commit().await?;
        self.inner.delivery_changes_available.notify_waiters();
        Ok(sequence)
    }

    async fn validate_delivery_stream(
        &self,
        instance: &str,
        stream_id: &str,
    ) -> Result<(), StoreError> {
        let stored_instance: Option<String> =
            sqlx::query_scalar("SELECT instance FROM delivery_streams WHERE stream_id = ?")
                .bind(stream_id)
                .fetch_optional(&self.inner.pool)
                .await?;
        match stored_instance {
            Some(stored) if stored == instance => Ok(()),
            Some(stored) => Err(StoreError::InvalidConfig(format!(
                "delivery stream {stream_id:?} belongs to processor {stored:?}, not {instance:?}"
            ))),
            None => Err(StoreError::InvalidConfig(format!(
                "delivery stream {stream_id:?} does not exist"
            ))),
        }
    }

    /// Reduce and atomically commit one exact block.
    ///
    /// Duplicate application of the same processor/block/checksum is
    /// idempotent. Reuse of the identity with a different checksum is rejected.
    ///
    /// # Errors
    ///
    /// Returns an error when validation or reduction fails, an identity
    /// conflicts, or any atomic store operation fails.
    pub async fn apply<P: Processor + ?Sized>(
        &self,
        processor: &P,
        cursor: ProcessorCursor,
        delta: &EncodedDelta,
        sink_ids: &[String],
    ) -> Result<ApplyOutcome, StoreError> {
        self.apply_with_change_publication(processor, cursor, delta, sink_ids, true)
            .await
    }

    /// Reduce and atomically commit one exact block while publishing into an
    /// explicitly selected delivery stream.
    ///
    /// # Errors
    ///
    /// Returns the same validation, reduction, identity, and storage errors as
    /// [`Self::apply`], plus invalid stream selection.
    pub async fn apply_to_stream<P: Processor + ?Sized>(
        &self,
        processor: &P,
        cursor: ProcessorCursor,
        delta: &EncodedDelta,
        sink_ids: &[String],
        stream_id: &str,
    ) -> Result<ApplyOutcome, StoreError> {
        self.apply_with_change_publication_to_stream(
            processor, cursor, delta, sink_ids, true, stream_id,
        )
        .await
    }

    /// Reduce and commit a block while optionally suppressing its delivery
    /// changes.
    ///
    /// This is used by bounded `terminal_only` jobs: state and coverage commit
    /// for every block, while only the terminal aggregate enters the delivery
    /// spool. Other callers should use [`Self::apply`].
    ///
    /// # Errors
    ///
    /// Returns the same validation, reduction, identity, and storage errors as
    /// [`Self::apply`].
    #[allow(clippy::too_many_lines)]
    pub async fn apply_with_change_publication<P: Processor + ?Sized>(
        &self,
        processor: &P,
        cursor: ProcessorCursor,
        delta: &EncodedDelta,
        sink_ids: &[String],
        publish_changes: bool,
    ) -> Result<ApplyOutcome, StoreError> {
        let stream_id = default_delivery_stream_id(processor.descriptor());
        self.apply_with_change_publication_to_stream(
            processor,
            cursor,
            delta,
            sink_ids,
            publish_changes,
            &stream_id,
        )
        .await
    }

    /// Stream-selecting form of [`Self::apply_with_change_publication`].
    ///
    /// # Errors
    ///
    /// Returns an error when validation or reduction fails, an identity or
    /// stream conflicts, capacity is exhausted, or the transaction fails.
    #[allow(clippy::too_many_lines)]
    pub async fn apply_with_change_publication_to_stream<P: Processor + ?Sized>(
        &self,
        processor: &P,
        cursor: ProcessorCursor,
        delta: &EncodedDelta,
        sink_ids: &[String],
        publish_changes: bool,
        stream_id: &str,
    ) -> Result<ApplyOutcome, StoreError> {
        self.apply_with_origin_to_stream(
            processor,
            cursor,
            delta,
            sink_ids,
            publish_changes,
            stream_id,
            None,
        )
        .await
    }

    /// Commit a finalized block recovered after its original live frame aged
    /// out, publishing it back into the stable live lane with explicit replay
    /// provenance.
    ///
    /// # Errors
    ///
    /// Returns the same validation, reduction, identity, capacity, and
    /// storage errors as [`Self::apply`].
    pub async fn apply_live_recovery<P: Processor + ?Sized>(
        &self,
        processor: &P,
        cursor: ProcessorCursor,
        delta: &EncodedDelta,
        sink_ids: &[String],
    ) -> Result<ApplyOutcome, StoreError> {
        if cursor.finality != Finality::Finalized {
            return Err(StoreError::InvalidConfig(
                "live recovery publication must be finalized".to_owned(),
            ));
        }
        let stream_id = default_delivery_stream_id(processor.descriptor());
        self.apply_with_origin_to_stream(
            processor,
            cursor,
            delta,
            sink_ids,
            true,
            &stream_id,
            Some(DeliveryOriginKind::LiveRecovery),
        )
        .await
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn apply_with_origin_to_stream<P: Processor + ?Sized>(
        &self,
        processor: &P,
        cursor: ProcessorCursor,
        delta: &EncodedDelta,
        sink_ids: &[String],
        publish_changes: bool,
        stream_id: &str,
        origin_override: Option<DeliveryOriginKind>,
    ) -> Result<ApplyOutcome, StoreError> {
        delta.validate(processor.descriptor())?;
        validate_cursor(&cursor, processor.descriptor(), delta)?;
        let descriptor = processor.descriptor();
        let delivery_enabled = descriptor.lifecycle.delivery.mode != DeliveryPolicyMode::None;
        let artifact_encoded = if matches!(
            descriptor.lifecycle.artifacts.mode,
            ArtifactPolicyMode::None
        ) {
            None
        } else {
            Some(delta.encode_durable()?)
        };
        let instance = self.register_processor(descriptor).await?;
        if delivery_enabled {
            self.validate_delivery_stream(&instance, stream_id).await?;
        }
        let _guard = self.inner.writer.lock().await;
        if let Some(checksum) = applied_checksum(
            &self.inner.pool,
            &instance,
            cursor.block_number,
            cursor.block_hash,
        )
        .await?
        {
            if checksum != delta.checksum {
                return Err(StoreError::ConflictingApply {
                    block: cursor.block_number,
                });
            }
            let mut transaction = self.inner.pool.begin().await?;
            if let Some(encoded) = artifact_encoded.as_deref() {
                stage_or_retain_processor_artifact(
                    &mut transaction,
                    descriptor,
                    &instance,
                    delta,
                    encoded,
                    cursor.finality,
                    now_i64()?,
                )
                .await?;
            }
            sqlx::query(
                "DELETE FROM pending_deltas
                 WHERE instance = ? AND block_number = ? AND block_hash = ?",
            )
            .bind(&instance)
            .bind(u64_i64(cursor.block_number.0, "block_number")?)
            .bind(cursor.block_hash.0.as_slice())
            .execute(&mut *transaction)
            .await?;
            if artifact_encoded.is_some() {
                enforce_artifact_storage_capacity(&mut transaction, self.inner.artifact_budget)
                    .await?;
            }
            transaction.commit().await?;
            return Ok(ApplyOutcome::AlreadyApplied);
        }
        let covered_hash: Option<Vec<u8>> = sqlx::query_scalar(
            "SELECT block_hash FROM processor_coverage
             WHERE instance = ? AND block_number = ?",
        )
        .bind(&instance)
        .bind(u64_i64(cursor.block_number.0, "block_number")?)
        .fetch_optional(&self.inner.pool)
        .await?;
        if let Some(covered_hash) = covered_hash {
            let covered_hash = decode_hash(covered_hash)?;
            if covered_hash != cursor.block_hash {
                return Err(StoreError::CanonicalConflict {
                    block: cursor.block_number,
                    stored: covered_hash,
                    incoming: cursor.block_hash,
                });
            }
        }

        let mut overlay = ReducerOverlay::new(self.inner.pool.clone(), instance.clone());
        let returned = processor.reduce(&mut overlay, &cursor, delta).await?;
        if returned.changes != overlay.changes {
            return Err(StoreError::Invariant(
                "processor returned changes that differ from emitted changes".to_owned(),
            ));
        }
        let batch = overlay.into_batch();
        let published_changes = if delivery_enabled && publish_changes {
            batch.changes.as_slice()
        } else {
            &[]
        };
        let publish_progress =
            delivery_enabled && delivery_stream_is_backfill(&self.inner.pool, stream_id).await?;
        let mut incoming_change_bytes =
            published_changes.iter().try_fold(0_u64, |total, change| {
                let bytes = change
                    .key
                    .len()
                    .checked_add(change.payload.len())
                    .and_then(|value| u64::try_from(value).ok())
                    .ok_or(StoreError::Numeric("incoming delivery change bytes"))?;
                total
                    .checked_add(bytes)
                    .ok_or(StoreError::Numeric("incoming delivery change bytes"))
            })?;
        if publish_progress {
            incoming_change_bytes = incoming_change_bytes
                .checked_add(32)
                .ok_or(StoreError::Numeric("incoming progress boundary bytes"))?;
        }
        if delivery_enabled {
            enforce_delivery_capacity(
                &self.inner,
                descriptor,
                &instance,
                stream_id,
                incoming_change_bytes,
                u64::from(publish_progress),
            )
            .await?;
        }
        let inverse_changes = if delivery_enabled && publish_changes {
            build_inverse_changes(&batch)?
        } else {
            Vec::new()
        };
        let prior_cursor = self.processor_cursor_by_instance(&instance).await?;
        let advances_cursor = processor.descriptor().mode == ReductionMode::OrderedState
            || prior_cursor
                .as_ref()
                .is_none_or(|prior| cursor.block_number >= prior.block_number);
        let committed_cursor = if advances_cursor {
            cursor.clone()
        } else {
            prior_cursor.clone().ok_or_else(|| {
                StoreError::Invariant("a non-advancing block must have a prior cursor".to_owned())
            })?
        };
        let undo = UndoRecord {
            mutations: batch.mutations.clone(),
            inverse_changes,
            prior_cursor,
            block: delta.block,
            finality: cursor.finality,
        };
        let undo_bytes = postcard::to_allocvec(&undo)
            .map_err(|error| StoreError::Encoding(error.to_string()))?;
        if retains_processor_output(descriptor) || artifact_encoded.is_some() {
            let output_bytes = if retains_processor_output(descriptor) {
                estimated_retained_mutation_bytes(&batch.mutations, true)?
            } else {
                0
            };
            let artifact_bytes = artifact_encoded.as_ref().map_or(0, |encoded| {
                u64::try_from(encoded.len())
                    .unwrap_or(u64::MAX)
                    .saturating_add(256)
            });
            let incoming_physical_bytes = output_bytes
                .saturating_add(u64::try_from(undo_bytes.len()).unwrap_or(u64::MAX))
                .saturating_add(incoming_change_bytes)
                .saturating_add(artifact_bytes)
                .saturating_add(256);
            enforce_physical_store_capacity(&self.inner, incoming_physical_bytes).await?;
        }
        let encoded_cursor = OpaqueCursor::encode(CursorKind::Processor, &cursor)?;
        let delivery_origin = if publish_progress {
            backfill_delivery_origin(&self.inner.pool, stream_id).await?
        } else if origin_override == Some(DeliveryOriginKind::LiveRecovery) {
            DeliveryOrigin::live_recovery(&instance)
        } else {
            DeliveryOrigin::live(&instance)
        };

        let mut transaction = self.inner.pool.begin().await?;
        if let Some(encoded) = artifact_encoded.as_deref() {
            stage_or_retain_processor_artifact(
                &mut transaction,
                descriptor,
                &instance,
                delta,
                encoded,
                cursor.finality,
                now_i64()?,
            )
            .await?;
        }
        apply_mutations(
            &mut transaction,
            &instance,
            &batch.mutations,
            false,
            retains_processor_output(processor.descriptor()),
            delta.block,
            cursor.finality,
        )
        .await?;
        prune_output_window(
            &mut transaction,
            processor.descriptor(),
            &instance,
            delta.block,
        )
        .await?;
        sqlx::query(
            "INSERT INTO undo_journal(
                instance, block_number, block_hash, encoded_undo, finalized
             ) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(&instance)
        .bind(u64_i64(cursor.block_number.0, "block_number")?)
        .bind(cursor.block_hash.0.as_slice())
        .bind(undo_bytes)
        .bind(i64::from(cursor.finality == Finality::Finalized))
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "INSERT INTO applied_blocks(
                instance, block_number, block_hash, delta_checksum, applied_at_unix_ms
             ) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(&instance)
        .bind(u64_i64(cursor.block_number.0, "block_number")?)
        .bind(cursor.block_hash.0.as_slice())
        .bind(delta.checksum.0.as_slice())
        .bind(now_i64()?)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "INSERT INTO processor_coverage(
                instance, block_number, block_hash, parent_hash, finality
             ) VALUES (?, ?, ?, ?, ?)
             ON CONFLICT(instance, block_number) DO UPDATE SET
               block_hash = excluded.block_hash,
               parent_hash = excluded.parent_hash,
               finality = MAX(processor_coverage.finality, excluded.finality)",
        )
        .bind(&instance)
        .bind(u64_i64(cursor.block_number.0, "block_number")?)
        .bind(cursor.block_hash.0.as_slice())
        .bind(delta.block.parent_hash.0.as_slice())
        .bind(finality_i64(cursor.finality))
        .execute(&mut *transaction)
        .await?;
        if !publish_progress {
            extend_shared_coverage_owner(&mut transaction, &instance, cursor.block_number).await?;
        }
        sqlx::query(
            "DELETE FROM pending_deltas
             WHERE instance = ? AND block_number = ? AND block_hash = ?",
        )
        .bind(&instance)
        .bind(u64_i64(cursor.block_number.0, "block_number")?)
        .bind(cursor.block_hash.0.as_slice())
        .execute(&mut *transaction)
        .await?;
        if advances_cursor {
            upsert_cursor(
                &mut transaction,
                &instance,
                &cursor,
                encoded_cursor.expose(),
            )
            .await?;
        }
        let (mut first, mut last) = if delivery_enabled {
            append_changes(
                &mut transaction,
                &instance,
                stream_id,
                &delivery_origin,
                cursor.chain_id,
                delta.block,
                cursor.finality,
                ChangeDirection::Apply,
                published_changes,
                sink_ids,
            )
            .await?
        } else {
            (None, None)
        };
        if publish_progress {
            let progress = backfill_progress_change(cursor.block_number, cursor.block_number, 1);
            let (progress_first, progress_last) = append_changes(
                &mut transaction,
                &instance,
                stream_id,
                &delivery_origin,
                cursor.chain_id,
                delta.block,
                Finality::Finalized,
                ChangeDirection::Apply,
                &[progress],
                &[],
            )
            .await?;
            record_backfill_progress(
                &mut transaction,
                stream_id,
                delta.block.number,
                1,
                u64::try_from(published_changes.len())
                    .map_err(|_| StoreError::Numeric("published historical changes"))?,
            )
            .await?;
            first = first.or(progress_first);
            last = progress_last.or(last);
        }
        if cursor.finality == Finality::Finalized
            && matches!(
                processor.descriptor().lifecycle.checkpoint.mode,
                CheckpointPolicyMode::Automatic
            )
        {
            create_recovery_checkpoint(
                &mut transaction,
                processor.descriptor(),
                &instance,
                &committed_cursor,
                processor.descriptor().lifecycle.checkpoint.keep,
            )
            .await?;
        }
        if artifact_encoded.is_some() {
            enforce_artifact_storage_capacity(&mut transaction, self.inner.artifact_budget).await?;
        }
        transaction.commit().await?;
        if last.is_some() {
            self.inner.delivery_changes_available.notify_waiters();
        }
        Ok(ApplyOutcome::Applied {
            processor_cursor: committed_cursor,
            first_change_sequence: first,
            last_change_sequence: last,
        })
    }

    /// Commit a bounded finalized history microbatch and its scheduler
    /// checkpoint in one `SQLite` transaction.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty or incompatible batch, invalid ordering,
    /// capacity exhaustion, reducer failure, or a failed transaction.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub async fn commit_historical_microbatch<P: Processor + ?Sized>(
        &self,
        processor: &P,
        mode: HistoricalBatchMode,
        items: &[HistoricalBatchItem],
        sink_ids: &[String],
        stream_id: &str,
        job_id: &str,
        job_checkpoint: &[u8],
        attempts: u32,
        complete_subscription: bool,
        limits: HistoricalCommitLimits,
    ) -> Result<HistoricalBatchOutcome, StoreError> {
        if items.is_empty() {
            return Err(StoreError::InvalidConfig(
                "historical microbatch must contain at least one block".to_owned(),
            ));
        }
        if limits.maximum_changes == 0 || limits.maximum_encoded_bytes == 0 {
            return Err(StoreError::InvalidConfig(
                "historical commit limits must be greater than zero".to_owned(),
            ));
        }
        let descriptor = processor.descriptor();
        if !matches!(descriptor.lifecycle.output.mode, OutputPolicyMode::None)
            || (mode != HistoricalBatchMode::Apply && descriptor.mode != ReductionMode::BlockLocal)
        {
            return Err(StoreError::InvalidConfig(
                "historical microbatching requires output-none; recompute additionally requires a block-local processor"
                    .to_owned(),
            ));
        }
        let instance = self.register_processor(descriptor).await?;
        self.validate_delivery_stream(&instance, stream_id).await?;
        if !delivery_stream_is_backfill(&self.inner.pool, stream_id).await? {
            return Err(StoreError::InvalidConfig(
                "historical microbatching requires a backfill delivery stream".to_owned(),
            ));
        }
        let completion = if complete_subscription {
            let completion: Option<(String, i64)> = sqlx::query_as(
                "SELECT subscriptions.subscription_id, MAX(ranges.to_block)
                 FROM backfill_subscriptions AS subscriptions
                 JOIN backfill_subscription_ranges AS ranges
                   ON ranges.subscription_id = subscriptions.subscription_id
                 WHERE subscriptions.history_stream_id = ?
                 GROUP BY subscriptions.subscription_id",
            )
            .bind(stream_id)
            .fetch_optional(&self.inner.pool)
            .await?;
            let (subscription_id, through_block) = completion.ok_or_else(|| {
                StoreError::Invariant(
                    "final historical microbatch has no durable subscription".to_owned(),
                )
            })?;
            Some((
                subscription_id,
                BlockNumber(i64_u64(through_block, "subscription final block")?),
            ))
        } else {
            None
        };
        let first_block = items[0].delta.block.number;
        let chain_id = items[0].delta.chain_id;
        let mut expected_number = first_block.0;
        let mut expected_parent = None;
        for item in items {
            item.delta.validate(descriptor)?;
            if item.finality != Finality::Finalized
                || item.delta.chain_id != chain_id
                || item.delta.block.number.0 != expected_number
                || expected_parent.is_some_and(|parent| item.delta.block.parent_hash != parent)
            {
                return Err(StoreError::InvalidConfig(
                    "historical microbatch must be contiguous, finalized, and single-chain"
                        .to_owned(),
                ));
            }
            expected_number = expected_number.saturating_add(1);
            expected_parent = Some(item.delta.block.hash);
        }
        let artifact_encodings = if mode == HistoricalBatchMode::Apply
            && !matches!(
                descriptor.lifecycle.artifacts.mode,
                ArtifactPolicyMode::None
            ) {
            items
                .iter()
                .map(|item| item.delta.encode_durable().map_err(StoreError::from))
                .collect::<Result<Vec<_>, _>>()?
        } else {
            Vec::new()
        };
        let artifact_bytes = artifact_encodings
            .iter()
            .try_fold(0_u64, |total, encoded| {
                total
                    .checked_add(
                        u64::try_from(encoded.len())
                            .map_err(|_| StoreError::Numeric("historical artifact bytes"))?
                            .saturating_add(256),
                    )
                    .ok_or(StoreError::Numeric("historical artifact bytes"))
            })?;

        let _guard = self.inner.writer.lock_history().await;
        let writer_started = Instant::now();
        if mode == HistoricalBatchMode::Apply {
            for item in items {
                if let Some(checksum) = applied_checksum(
                    &self.inner.pool,
                    &instance,
                    item.delta.block.number,
                    item.delta.block.hash,
                )
                .await?
                {
                    if checksum != item.delta.checksum {
                        return Err(StoreError::ConflictingApply {
                            block: item.delta.block.number,
                        });
                    }
                    return Err(StoreError::HistoricalBatchRequiresFallback);
                }
                let covered_hash: Option<Vec<u8>> = sqlx::query_scalar(
                    "SELECT block_hash FROM processor_coverage
                     WHERE instance = ? AND block_number = ?",
                )
                .bind(&instance)
                .bind(u64_i64(item.delta.block.number.0, "block_number")?)
                .fetch_optional(&self.inner.pool)
                .await?;
                if let Some(covered_hash) = covered_hash {
                    let covered_hash = decode_hash(covered_hash)?;
                    if covered_hash != item.delta.block.hash {
                        return Err(StoreError::CanonicalConflict {
                            block: item.delta.block.number,
                            stored: covered_hash,
                            incoming: item.delta.block.hash,
                        });
                    }
                    return Err(StoreError::HistoricalBatchRequiresFallback);
                }
            }
        } else {
            for item in items {
                if let Some(covered_hash) = self
                    .coverage_hash(descriptor, item.delta.block.number)
                    .await?
                {
                    if covered_hash != item.delta.block.hash {
                        return Err(StoreError::CanonicalConflict {
                            block: item.delta.block.number,
                            stored: covered_hash,
                            incoming: item.delta.block.hash,
                        });
                    }
                } else if mode == HistoricalBatchMode::RepublishVerifiedCompact
                    && !self
                        .finalized_coverage_segments(
                            descriptor,
                            BlockRange::single(item.delta.block.number),
                        )
                        .await?
                        .is_empty()
                {
                    // The runtime verified this segment's full parent chain
                    // and persisted end anchor immediately before publication.
                } else {
                    return Err(StoreError::InvalidConfig(format!(
                        "cannot republish uncovered block {}",
                        item.delta.block.number.0
                    )));
                }
            }
        }

        let prior_cursor = self.processor_cursor_by_instance(&instance).await?;
        let first_sequence = prior_cursor
            .as_ref()
            .map_or(1, |cursor| cursor.sequence.saturating_add(1));
        let mut block_changes = Vec::with_capacity(items.len());
        let mut cumulative_mutations = Vec::new();
        if mode == HistoricalBatchMode::Apply {
            let mut overlay = ReducerOverlay::new(self.inner.pool.clone(), instance.clone());
            for (index, item) in items.iter().enumerate() {
                let cursor = historical_batch_cursor(
                    descriptor,
                    item,
                    first_sequence.saturating_add(u64::try_from(index).unwrap_or(u64::MAX)),
                );
                let change_start = overlay.changes.len();
                let returned = processor.reduce(&mut overlay, &cursor, &item.delta).await?;
                if returned.changes.as_slice() != &overlay.changes[change_start..] {
                    return Err(StoreError::Invariant(
                        "processor returned changes that differ from emitted changes".to_owned(),
                    ));
                }
                block_changes.push(if item.publish_changes {
                    returned.changes
                } else {
                    Vec::new()
                });
                // Output-none instances deliberately do not make entity/index
                // writes from one isolated block visible to the next block.
                overlay.entities.clear();
                overlay.indexes.clear();
            }
            cumulative_mutations = overlay.into_batch().mutations;
        } else {
            for (index, item) in items.iter().enumerate() {
                let cursor = historical_batch_cursor(
                    descriptor,
                    item,
                    first_sequence.saturating_add(u64::try_from(index).unwrap_or(u64::MAX)),
                );
                let mut overlay = ReducerOverlay::new(self.inner.pool.clone(), instance.clone());
                let returned = processor.reduce(&mut overlay, &cursor, &item.delta).await?;
                if returned.changes != overlay.changes {
                    return Err(StoreError::Invariant(
                        "processor returned changes that differ from emitted changes".to_owned(),
                    ));
                }
                block_changes.push(if item.publish_changes {
                    returned.changes
                } else {
                    Vec::new()
                });
            }
        }

        let mut incoming_change_bytes =
            block_changes
                .iter()
                .flatten()
                .try_fold(32_u64, |total, change| {
                    let bytes = change
                        .key
                        .len()
                        .checked_add(change.payload.len())
                        .and_then(|value| u64::try_from(value).ok())
                        .ok_or(StoreError::Numeric("historical microbatch change bytes"))?;
                    total
                        .checked_add(bytes)
                        .ok_or(StoreError::Numeric("historical microbatch change bytes"))
                })?;
        let published_change_count = block_changes.iter().map(Vec::len).sum::<usize>();
        let encoded_change_bytes = u64::try_from(
            postcard::to_allocvec(&block_changes)
                .map_err(|error| StoreError::Encoding(error.to_string()))?
                .len(),
        )
        .map_err(|_| StoreError::Numeric("historical encoded change bytes"))?;
        if published_change_count > limits.maximum_changes
            || encoded_change_bytes > limits.maximum_encoded_bytes
        {
            return Err(StoreError::HistoricalBatchLimit {
                observed_changes: published_change_count,
                maximum_changes: limits.maximum_changes,
                observed_encoded_bytes: encoded_change_bytes,
                maximum_encoded_bytes: limits.maximum_encoded_bytes,
            });
        }
        if let Some((subscription_id, _)) = &completion {
            let preexisting_ranges: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM backfill_subscription_preexisting_ranges
                 WHERE subscription_id = ?",
            )
            .bind(subscription_id)
            .fetch_one(&self.inner.pool)
            .await?;
            incoming_change_bytes = incoming_change_bytes
                .checked_add(
                    u64::try_from(subscription_id.len())
                        .map_err(|_| StoreError::Numeric("backfill completion bytes"))?
                        .saturating_add(48)
                        .saturating_add(
                            i64_u64(preexisting_ranges, "preexisting coverage range count")?
                                .saturating_mul(16),
                        ),
                )
                .ok_or(StoreError::Numeric("historical microbatch change bytes"))?;
        }
        let processed_blocks = u64::try_from(items.len())
            .map_err(|_| StoreError::Numeric("historical microbatch blocks"))?;
        enforce_delivery_capacity(
            &self.inner,
            descriptor,
            &instance,
            stream_id,
            incoming_change_bytes,
            processed_blocks,
        )
        .await?;
        if artifact_bytes > 0 {
            enforce_physical_store_capacity(
                &self.inner,
                incoming_change_bytes.saturating_add(artifact_bytes),
            )
            .await?;
        }

        let final_item = items.last().ok_or_else(|| {
            StoreError::InvalidConfig("historical microbatch must not be empty".to_owned())
        })?;
        let final_cursor = historical_batch_cursor(
            descriptor,
            final_item,
            first_sequence.saturating_add(processed_blocks.saturating_sub(1)),
        );
        let advances_cursor = prior_cursor
            .as_ref()
            .is_none_or(|prior| final_cursor.block_number >= prior.block_number);
        let delivery_origin = backfill_delivery_origin(&self.inner.pool, stream_id).await?;
        let mut transaction = self.inner.pool.begin().await?;
        let bulk_artifact_accounting = !artifact_encodings.is_empty()
            && descriptor.lifecycle.artifacts.mode == ArtifactPolicyMode::Full;
        let mut artifact_totals_delta = ArtifactTotalsDelta::default();
        if bulk_artifact_accounting {
            begin_bulk_artifact_accounting(&mut transaction, &instance).await?;
        }
        if mode == HistoricalBatchMode::Apply {
            apply_mutations(
                &mut transaction,
                &instance,
                &cumulative_mutations,
                false,
                false,
                final_item.delta.block,
                Finality::Finalized,
            )
            .await?;
        }
        let mut first = None;
        let mut last = None;
        let mut published_changes = 0_u64;
        for (index, (item, changes)) in items.iter().zip(&block_changes).enumerate() {
            if mode == HistoricalBatchMode::Apply {
                if let Some(encoded) = artifact_encodings.get(index) {
                    if insert_processor_artifact(
                        &mut transaction,
                        &instance,
                        descriptor,
                        &item.delta,
                        encoded,
                        now_i64()?,
                    )
                    .await?
                    {
                        artifact_totals_delta.added_artifacts =
                            artifact_totals_delta.added_artifacts.saturating_add(1);
                        artifact_totals_delta.added_bytes = artifact_totals_delta
                            .added_bytes
                            .saturating_add(u64::try_from(encoded.len()).unwrap_or(u64::MAX));
                    }
                    if insert_artifact_owner(
                        &mut transaction,
                        &instance,
                        item.delta.block.number,
                        ArtifactOwnerKind::ProcessorInstance,
                        &instance,
                        now_i64()?,
                    )
                    .await?
                    {
                        artifact_totals_delta.added_owners =
                            artifact_totals_delta.added_owners.saturating_add(1);
                    }
                }
                sqlx::query(
                    "INSERT INTO applied_blocks(
                        instance, block_number, block_hash, delta_checksum, applied_at_unix_ms
                     ) VALUES (?, ?, ?, ?, ?)",
                )
                .bind(&instance)
                .bind(u64_i64(item.delta.block.number.0, "block_number")?)
                .bind(item.delta.block.hash.0.as_slice())
                .bind(item.delta.checksum.0.as_slice())
                .bind(now_i64()?)
                .execute(&mut *transaction)
                .await?;
                sqlx::query(
                    "INSERT INTO processor_coverage(
                        instance, block_number, block_hash, parent_hash, finality
                     ) VALUES (?, ?, ?, ?, ?)",
                )
                .bind(&instance)
                .bind(u64_i64(item.delta.block.number.0, "block_number")?)
                .bind(item.delta.block.hash.0.as_slice())
                .bind(item.delta.block.parent_hash.0.as_slice())
                .bind(finality_i64(Finality::Finalized))
                .execute(&mut *transaction)
                .await?;
                sqlx::query(
                    "DELETE FROM pending_deltas
                     WHERE instance = ? AND block_number = ? AND block_hash = ?",
                )
                .bind(&instance)
                .bind(u64_i64(item.delta.block.number.0, "block_number")?)
                .bind(item.delta.block.hash.0.as_slice())
                .execute(&mut *transaction)
                .await?;
            }
            let (block_first, block_last) = append_changes(
                &mut transaction,
                &instance,
                stream_id,
                &delivery_origin,
                chain_id,
                item.delta.block,
                Finality::Finalized,
                ChangeDirection::Apply,
                changes,
                sink_ids,
            )
            .await?;
            first = first.or(block_first);
            last = block_last.or(last);
            published_changes = published_changes.saturating_add(
                u64::try_from(changes.len())
                    .map_err(|_| StoreError::Numeric("published historical changes"))?,
            );
        }
        if mode == HistoricalBatchMode::Apply && !artifact_encodings.is_empty() {
            prune_processor_artifact_window(
                &mut transaction,
                descriptor,
                &instance,
                final_item.delta.block,
            )
            .await?;
        }
        if mode == HistoricalBatchMode::Apply && advances_cursor {
            let encoded = OpaqueCursor::encode(CursorKind::Processor, &final_cursor)?;
            upsert_cursor(&mut transaction, &instance, &final_cursor, encoded.expose()).await?;
        }
        let progress =
            backfill_progress_change(first_block, final_item.delta.block.number, processed_blocks);
        let (progress_first, progress_last) = append_changes(
            &mut transaction,
            &instance,
            stream_id,
            &delivery_origin,
            chain_id,
            final_item.delta.block,
            Finality::Finalized,
            ChangeDirection::Apply,
            &[progress],
            &[],
        )
        .await?;
        first = first.or(progress_first);
        last = progress_last.or(last);
        record_backfill_progress(
            &mut transaction,
            stream_id,
            final_item.delta.block.number,
            processed_blocks,
            published_changes,
        )
        .await?;
        let updated = sqlx::query(
            "UPDATE jobs
             SET state = 'running', checkpoint = ?, attempts = ?, updated_at_unix_ms = ?
             WHERE job_id = ?",
        )
        .bind(job_checkpoint)
        .bind(i64::from(attempts))
        .bind(now_i64()?)
        .bind(job_id)
        .execute(&mut *transaction)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(StoreError::Invariant(format!(
                "historical microbatch job {job_id:?} disappeared"
            )));
        }
        if let Some((subscription_id, through_block)) = completion {
            append_backfill_completion_transaction(
                &mut transaction,
                &instance,
                stream_id,
                chain_id,
                &subscription_id,
                through_block,
            )
            .await?;
        }
        if bulk_artifact_accounting {
            finish_bulk_artifact_accounting(&mut transaction, &instance, artifact_totals_delta)
                .await?;
        }
        if !artifact_encodings.is_empty() {
            enforce_artifact_storage_capacity(&mut transaction, self.inner.artifact_budget).await?;
        }
        transaction.commit().await?;
        self.inner.delivery_changes_available.notify_waiters();
        Ok(HistoricalBatchOutcome {
            processed_blocks,
            published_changes,
            committed_output_bytes: incoming_change_bytes.saturating_add(artifact_bytes),
            first_change_sequence: first,
            last_change_sequence: last,
            writer_hold_micros: duration_micros(writer_started.elapsed()),
        })
    }

    /// Atomically materialize a bounded finalized batch without a delivery
    /// stream.
    ///
    /// Reducer visibility remains canonical across the batch, while output
    /// metadata, undo records, coverage, the processor cursor, and the job
    /// checkpoint retain their per-block identities inside one transaction.
    ///
    /// # Errors
    ///
    /// Returns an error for incompatible processor policy, invalid ordering,
    /// overlapping coverage, an oversized batch, reducer failure, or a failed
    /// transaction.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub async fn commit_historical_materialization_microbatch<P: Processor + ?Sized>(
        &self,
        processor: &P,
        items: &[HistoricalBatchItem],
        job_id: &str,
        job_checkpoint: &[u8],
        attempts: u32,
        artifact_target: HistoricalArtifactTarget,
        limits: HistoricalCommitLimits,
    ) -> Result<HistoricalBatchOutcome, StoreError> {
        if items.is_empty() {
            return Err(StoreError::InvalidConfig(
                "historical materialization microbatch must contain at least one block".to_owned(),
            ));
        }
        if limits.maximum_changes == 0 || limits.maximum_encoded_bytes == 0 {
            return Err(StoreError::InvalidConfig(
                "historical commit limits must be greater than zero".to_owned(),
            ));
        }
        let descriptor = processor.descriptor();
        if descriptor.lifecycle.delivery.mode != DeliveryPolicyMode::None {
            return Err(StoreError::InvalidConfig(
                "materialization microbatching requires a delivery-none processor".to_owned(),
            ));
        }
        if artifact_target == HistoricalArtifactTarget::ExternalCommitted
            && matches!(
                descriptor.lifecycle.artifacts.mode,
                ArtifactPolicyMode::None
            )
        {
            return Err(StoreError::InvalidConfig(
                "external artifact commit requires artifact retention to be enabled".to_owned(),
            ));
        }
        let instance = self.register_processor(descriptor).await?;
        let first_block = items[0].delta.block.number;
        let chain_id = items[0].delta.chain_id;
        let mut expected_number = first_block.0;
        let mut expected_parent = None;
        for item in items {
            item.delta.validate(descriptor)?;
            if item.finality != Finality::Finalized
                || item.delta.chain_id != chain_id
                || item.delta.block.number.0 != expected_number
                || expected_parent.is_some_and(|parent| item.delta.block.parent_hash != parent)
            {
                return Err(StoreError::InvalidConfig(
                    "historical materialization microbatch must be contiguous, finalized, and single-chain"
                        .to_owned(),
                ));
            }
            expected_number = expected_number.saturating_add(1);
            expected_parent = Some(item.delta.block.hash);
        }
        let artifact_encodings = if artifact_target == HistoricalArtifactTarget::ExternalCommitted
            || matches!(
                descriptor.lifecycle.artifacts.mode,
                ArtifactPolicyMode::None
            ) {
            Vec::new()
        } else {
            items
                .iter()
                .map(|item| item.delta.encode_durable().map_err(StoreError::from))
                .collect::<Result<Vec<_>, _>>()?
        };
        let artifact_bytes = artifact_encodings
            .iter()
            .try_fold(0_u64, |total, encoded| {
                total
                    .checked_add(
                        u64::try_from(encoded.len())
                            .map_err(|_| StoreError::Numeric("materialization artifact bytes"))?
                            .saturating_add(256),
                    )
                    .ok_or(StoreError::Numeric("materialization artifact bytes"))
            })?;

        let _guard = self.inner.writer.lock_history().await;
        let writer_started = Instant::now();
        for item in items {
            if let Some(checksum) = applied_checksum(
                &self.inner.pool,
                &instance,
                item.delta.block.number,
                item.delta.block.hash,
            )
            .await?
            {
                if checksum != item.delta.checksum {
                    return Err(StoreError::ConflictingApply {
                        block: item.delta.block.number,
                    });
                }
                return Err(StoreError::HistoricalBatchRequiresFallback);
            }
            let covered_hash: Option<Vec<u8>> = sqlx::query_scalar(
                "SELECT block_hash FROM processor_coverage
                 WHERE instance = ? AND block_number = ?",
            )
            .bind(&instance)
            .bind(u64_i64(item.delta.block.number.0, "block_number")?)
            .fetch_optional(&self.inner.pool)
            .await?;
            if let Some(covered_hash) = covered_hash {
                let covered_hash = decode_hash(covered_hash)?;
                if covered_hash != item.delta.block.hash {
                    return Err(StoreError::CanonicalConflict {
                        block: item.delta.block.number,
                        stored: covered_hash,
                        incoming: item.delta.block.hash,
                    });
                }
                return Err(StoreError::HistoricalBatchRequiresFallback);
            }
        }

        let prior_cursor = self.processor_cursor_by_instance(&instance).await?;
        let first_sequence = prior_cursor
            .as_ref()
            .map_or(1, |cursor| cursor.sequence.saturating_add(1));
        let mut overlay = ReducerOverlay::new(self.inner.pool.clone(), instance.clone());
        let mut block_batches = Vec::with_capacity(items.len());
        for (index, item) in items.iter().enumerate() {
            let cursor = historical_batch_cursor(
                descriptor,
                item,
                first_sequence.saturating_add(u64::try_from(index).unwrap_or(u64::MAX)),
            );
            let change_start = overlay.changes.len();
            let returned = processor.reduce(&mut overlay, &cursor, &item.delta).await?;
            if returned.changes.as_slice() != &overlay.changes[change_start..] {
                return Err(StoreError::Invariant(
                    "processor returned changes that differ from emitted changes".to_owned(),
                ));
            }
            block_batches.push(overlay.take_batch());
        }
        let observed_changes = block_batches
            .iter()
            .map(|batch| batch.changes.len())
            .sum::<usize>();
        let observed_encoded_bytes = u64::try_from(
            postcard::to_allocvec(
                &block_batches
                    .iter()
                    .map(|batch| &batch.changes)
                    .collect::<Vec<_>>(),
            )
            .map_err(|error| StoreError::Encoding(error.to_string()))?
            .len(),
        )
        .map_err(|_| StoreError::Numeric("historical encoded change bytes"))?;
        if observed_changes > limits.maximum_changes
            || observed_encoded_bytes > limits.maximum_encoded_bytes
        {
            return Err(StoreError::HistoricalBatchLimit {
                observed_changes,
                maximum_changes: limits.maximum_changes,
                observed_encoded_bytes,
                maximum_encoded_bytes: limits.maximum_encoded_bytes,
            });
        }

        let output_bytes = if retains_processor_output(descriptor) {
            block_batches
                .iter()
                .try_fold(0_u64, |total, batch| -> Result<u64, StoreError> {
                    let retained = estimated_retained_mutation_bytes(&batch.mutations, true)?;
                    let undo = u64::try_from(
                        postcard::to_allocvec(&batch.mutations)
                            .map_err(|error| StoreError::Encoding(error.to_string()))?
                            .len(),
                    )
                    .map_err(|_| StoreError::Numeric("materialization undo bytes"))?;
                    total
                        .checked_add(retained.saturating_add(undo).saturating_add(256))
                        .ok_or(StoreError::Numeric(
                            "materialization physical admission bytes",
                        ))
                })?
        } else {
            0
        };
        let committed_output_bytes = output_bytes.saturating_add(artifact_bytes);
        if committed_output_bytes > 0 {
            enforce_physical_store_capacity(&self.inner, committed_output_bytes).await?;
        }

        let mut transaction = self.inner.pool.begin().await?;
        let bulk_artifact_accounting = !artifact_encodings.is_empty()
            && descriptor.lifecycle.artifacts.mode == ArtifactPolicyMode::Full;
        let mut artifact_totals_delta = ArtifactTotalsDelta::default();
        if bulk_artifact_accounting {
            begin_bulk_artifact_accounting(&mut transaction, &instance).await?;
        }
        let mut committed_cursor = prior_cursor.clone();
        for (index, (item, batch)) in items.iter().zip(&block_batches).enumerate() {
            let cursor = historical_batch_cursor(
                descriptor,
                item,
                first_sequence.saturating_add(u64::try_from(index).unwrap_or(u64::MAX)),
            );
            apply_mutations(
                &mut transaction,
                &instance,
                &batch.mutations,
                false,
                retains_processor_output(descriptor),
                item.delta.block,
                Finality::Finalized,
            )
            .await?;
            prune_output_window(&mut transaction, descriptor, &instance, item.delta.block).await?;
            if let Some(encoded) = artifact_encodings.get(index) {
                if insert_processor_artifact(
                    &mut transaction,
                    &instance,
                    descriptor,
                    &item.delta,
                    encoded,
                    now_i64()?,
                )
                .await?
                {
                    artifact_totals_delta.added_artifacts =
                        artifact_totals_delta.added_artifacts.saturating_add(1);
                    artifact_totals_delta.added_bytes = artifact_totals_delta
                        .added_bytes
                        .saturating_add(u64::try_from(encoded.len()).unwrap_or(u64::MAX));
                }
                if insert_artifact_owner(
                    &mut transaction,
                    &instance,
                    item.delta.block.number,
                    ArtifactOwnerKind::ProcessorInstance,
                    &instance,
                    now_i64()?,
                )
                .await?
                {
                    artifact_totals_delta.added_owners =
                        artifact_totals_delta.added_owners.saturating_add(1);
                }
            }
            let undo = UndoRecord {
                mutations: batch.mutations.clone(),
                inverse_changes: Vec::new(),
                prior_cursor: committed_cursor.clone(),
                block: item.delta.block,
                finality: Finality::Finalized,
            };
            let undo_bytes = postcard::to_allocvec(&undo)
                .map_err(|error| StoreError::Encoding(error.to_string()))?;
            sqlx::query(
                "INSERT INTO undo_journal(
                    instance, block_number, block_hash, encoded_undo, finalized
                 ) VALUES (?, ?, ?, ?, 1)",
            )
            .bind(&instance)
            .bind(u64_i64(item.delta.block.number.0, "block_number")?)
            .bind(item.delta.block.hash.0.as_slice())
            .bind(undo_bytes)
            .execute(&mut *transaction)
            .await?;
            sqlx::query(
                "INSERT INTO applied_blocks(
                    instance, block_number, block_hash, delta_checksum, applied_at_unix_ms
                 ) VALUES (?, ?, ?, ?, ?)",
            )
            .bind(&instance)
            .bind(u64_i64(item.delta.block.number.0, "block_number")?)
            .bind(item.delta.block.hash.0.as_slice())
            .bind(item.delta.checksum.0.as_slice())
            .bind(now_i64()?)
            .execute(&mut *transaction)
            .await?;
            sqlx::query(
                "INSERT INTO processor_coverage(
                    instance, block_number, block_hash, parent_hash, finality
                 ) VALUES (?, ?, ?, ?, ?)",
            )
            .bind(&instance)
            .bind(u64_i64(item.delta.block.number.0, "block_number")?)
            .bind(item.delta.block.hash.0.as_slice())
            .bind(item.delta.block.parent_hash.0.as_slice())
            .bind(finality_i64(Finality::Finalized))
            .execute(&mut *transaction)
            .await?;
            sqlx::query(
                "DELETE FROM pending_deltas
                 WHERE instance = ? AND block_number = ? AND block_hash = ?",
            )
            .bind(&instance)
            .bind(u64_i64(item.delta.block.number.0, "block_number")?)
            .bind(item.delta.block.hash.0.as_slice())
            .execute(&mut *transaction)
            .await?;
            if committed_cursor
                .as_ref()
                .is_none_or(|prior| cursor.block_number >= prior.block_number)
            {
                committed_cursor = Some(cursor);
            }
            if !artifact_encodings.is_empty()
                && let Some(final_item) = items.last()
            {
                prune_processor_artifact_window(
                    &mut transaction,
                    descriptor,
                    &instance,
                    final_item.delta.block,
                )
                .await?;
            }
        }
        let last_block = items
            .last()
            .ok_or_else(|| StoreError::Invariant("materialization batch became empty".to_owned()))?
            .delta
            .block
            .number;
        extend_shared_coverage_owner_range(
            &mut transaction,
            &instance,
            BlockRange::new(first_block, last_block)
                .map_err(|error| StoreError::Invariant(error.to_string()))?,
        )
        .await?;
        if let Some(committed_cursor) = &committed_cursor {
            let encoded = OpaqueCursor::encode(CursorKind::Processor, committed_cursor)?;
            upsert_cursor(
                &mut transaction,
                &instance,
                committed_cursor,
                encoded.expose(),
            )
            .await?;
            if matches!(
                descriptor.lifecycle.checkpoint.mode,
                CheckpointPolicyMode::Automatic
            ) {
                create_recovery_checkpoint(
                    &mut transaction,
                    descriptor,
                    &instance,
                    committed_cursor,
                    descriptor.lifecycle.checkpoint.keep,
                )
                .await?;
            }
        }
        let updated = sqlx::query(
            "UPDATE jobs
             SET state = 'running', checkpoint = ?, attempts = ?, updated_at_unix_ms = ?
             WHERE job_id = ?",
        )
        .bind(job_checkpoint)
        .bind(i64::from(attempts))
        .bind(now_i64()?)
        .bind(job_id)
        .execute(&mut *transaction)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(StoreError::Invariant(format!(
                "historical materialization job {job_id:?} disappeared"
            )));
        }
        if bulk_artifact_accounting {
            finish_bulk_artifact_accounting(&mut transaction, &instance, artifact_totals_delta)
                .await?;
        }
        if !artifact_encodings.is_empty() {
            enforce_artifact_storage_capacity(&mut transaction, self.inner.artifact_budget).await?;
        }
        transaction.commit().await?;
        Ok(HistoricalBatchOutcome {
            processed_blocks: u64::try_from(items.len())
                .map_err(|_| StoreError::Numeric("historical microbatch blocks"))?,
            published_changes: 0,
            committed_output_bytes,
            first_change_sequence: None,
            last_change_sequence: None,
            writer_hold_micros: duration_micros(writer_started.elapsed()),
        })
    }

    /// Re-run a finalized block-local reducer and append its domain changes
    /// without changing processor entities, working state, coverage, or its
    /// live cursor.
    ///
    /// This supports application-requested rematerialization after delivery
    /// output has been acknowledged and pruned. The block must already be
    /// present in canonical processor coverage with the same hash. Ordered
    /// reducers and unfinalized blocks are rejected because replaying them
    /// independently could corrupt state or publish changes that still need
    /// rollback semantics.
    ///
    /// # Errors
    ///
    /// Returns an error for an incompatible processor mode/finality, absent or
    /// conflicting canonical coverage, reducer contract failure, delivery
    /// capacity exhaustion, or a database failure.
    #[allow(clippy::too_many_lines)]
    pub async fn republish_block_local<P: Processor + ?Sized>(
        &self,
        processor: &P,
        cursor: ProcessorCursor,
        delta: &EncodedDelta,
        sink_ids: &[String],
    ) -> Result<ReplayOutcome, StoreError> {
        let stream_id = default_delivery_stream_id(processor.descriptor());
        self.republish_block_local_to_stream(processor, cursor, delta, sink_ids, &stream_id)
            .await
    }

    /// Stream-selecting form of [`Self::republish_block_local`].
    ///
    /// # Errors
    ///
    /// Returns the same validation, coverage, reduction, capacity, and storage
    /// errors as [`Self::republish_block_local`], plus invalid stream selection.
    #[allow(clippy::too_many_lines)]
    pub async fn republish_block_local_to_stream<P: Processor + ?Sized>(
        &self,
        processor: &P,
        cursor: ProcessorCursor,
        delta: &EncodedDelta,
        sink_ids: &[String],
        stream_id: &str,
    ) -> Result<ReplayOutcome, StoreError> {
        self.republish_block_local_with_change_publication_to_stream(
            processor, cursor, delta, sink_ids, true, stream_id,
        )
        .await
    }

    /// Stream-selecting replay that can suppress domain changes while still
    /// recording an atomic backfill progress boundary.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::republish_block_local_to_stream`].
    #[allow(clippy::too_many_lines)]
    pub async fn republish_block_local_with_change_publication_to_stream<P: Processor + ?Sized>(
        &self,
        processor: &P,
        cursor: ProcessorCursor,
        delta: &EncodedDelta,
        sink_ids: &[String],
        publish_changes: bool,
        stream_id: &str,
    ) -> Result<ReplayOutcome, StoreError> {
        if processor.descriptor().mode != ReductionMode::BlockLocal {
            return Err(StoreError::InvalidConfig(
                "only block-local processors can republish historical changes".to_owned(),
            ));
        }
        if cursor.finality != Finality::Finalized {
            return Err(StoreError::InvalidConfig(
                "historical change replay requires a finalized block".to_owned(),
            ));
        }
        delta.validate(processor.descriptor())?;
        validate_cursor(&cursor, processor.descriptor(), delta)?;

        let instance = self.register_processor(processor.descriptor()).await?;
        self.validate_delivery_stream(&instance, stream_id).await?;
        let _guard = self.inner.writer.lock_history().await;
        let covered_hash: Option<Vec<u8>> = sqlx::query_scalar(
            "SELECT block_hash FROM processor_coverage
             WHERE instance = ? AND block_number = ?",
        )
        .bind(&instance)
        .bind(u64_i64(cursor.block_number.0, "block_number")?)
        .fetch_optional(&self.inner.pool)
        .await?;
        match covered_hash.map(decode_hash).transpose()? {
            Some(hash) if hash == cursor.block_hash => {}
            Some(hash) => {
                return Err(StoreError::CanonicalConflict {
                    block: cursor.block_number,
                    stored: hash,
                    incoming: cursor.block_hash,
                });
            }
            None => {
                return Err(StoreError::InvalidConfig(format!(
                    "cannot replay uncovered block {}",
                    cursor.block_number.0
                )));
            }
        }

        let mut overlay = ReducerOverlay::new(self.inner.pool.clone(), instance.clone());
        let returned = processor.reduce(&mut overlay, &cursor, delta).await?;
        if returned.changes != overlay.changes {
            return Err(StoreError::Invariant(
                "processor returned changes that differ from emitted changes".to_owned(),
            ));
        }
        let batch = overlay.into_batch();
        let published_changes = if publish_changes {
            batch.changes.as_slice()
        } else {
            &[]
        };
        let publish_progress = delivery_stream_is_backfill(&self.inner.pool, stream_id).await?;
        let mut incoming_change_bytes =
            published_changes.iter().try_fold(0_u64, |total, change| {
                let bytes = change
                    .key
                    .len()
                    .checked_add(change.payload.len())
                    .and_then(|value| u64::try_from(value).ok())
                    .ok_or(StoreError::Numeric("replayed delivery change bytes"))?;
                total
                    .checked_add(bytes)
                    .ok_or(StoreError::Numeric("replayed delivery change bytes"))
            })?;
        if publish_progress {
            incoming_change_bytes = incoming_change_bytes
                .checked_add(32)
                .ok_or(StoreError::Numeric("replayed progress boundary bytes"))?;
        }
        enforce_delivery_capacity(
            &self.inner,
            processor.descriptor(),
            &instance,
            stream_id,
            incoming_change_bytes,
            u64::from(publish_progress),
        )
        .await?;
        let delivery_origin = if publish_progress {
            backfill_delivery_origin(&self.inner.pool, stream_id).await?
        } else {
            DeliveryOrigin {
                kind: DeliveryOriginKind::Recompute,
                id: instance.clone(),
                publication_revision: 0,
            }
        };

        let mut transaction = self.inner.pool.begin().await?;
        let (mut first, mut last) = append_changes(
            &mut transaction,
            &instance,
            stream_id,
            &delivery_origin,
            cursor.chain_id,
            delta.block,
            cursor.finality,
            ChangeDirection::Apply,
            published_changes,
            sink_ids,
        )
        .await?;
        if publish_progress {
            let progress = backfill_progress_change(cursor.block_number, cursor.block_number, 1);
            let (progress_first, progress_last) = append_changes(
                &mut transaction,
                &instance,
                stream_id,
                &delivery_origin,
                cursor.chain_id,
                delta.block,
                Finality::Finalized,
                ChangeDirection::Apply,
                &[progress],
                &[],
            )
            .await?;
            record_backfill_progress(
                &mut transaction,
                stream_id,
                delta.block.number,
                1,
                u64::try_from(published_changes.len())
                    .map_err(|_| StoreError::Numeric("republished historical changes"))?,
            )
            .await?;
            first = first.or(progress_first);
            last = progress_last.or(last);
        }
        transaction.commit().await?;
        if last.is_some() {
            self.inner.delivery_changes_available.notify_waiters();
        }
        Ok(ReplayOutcome {
            published_changes: published_changes.len(),
            first_change_sequence: first,
            last_change_sequence: last,
        })
    }

    /// Reverse one unfinalized processor block using the core-generated
    /// preimages.
    ///
    /// # Errors
    ///
    /// Returns an error when the undo record is absent, corrupt, finalized, or
    /// cannot be committed atomically.
    #[allow(clippy::too_many_lines)]
    pub async fn undo(
        &self,
        descriptor: &ProcessorDescriptor,
        chain_id: ChainId,
        block_number: BlockNumber,
        block_hash: BlockHash,
        sink_ids: &[String],
    ) -> Result<UndoOutcome, StoreError> {
        let instance = processor_instance(descriptor);
        let _guard = self.inner.writer.lock().await;
        let row = sqlx::query(
            "SELECT encoded_undo, finalized FROM undo_journal
             WHERE instance = ? AND block_number = ? AND block_hash = ?",
        )
        .bind(&instance)
        .bind(u64_i64(block_number.0, "block_number")?)
        .bind(block_hash.0.as_slice())
        .fetch_optional(&self.inner.pool)
        .await?
        .ok_or(StoreError::UndoNotFound(block_number))?;
        if row.try_get::<i64, _>("finalized")? != 0 {
            return Err(StoreError::FinalizedUndo(block_number));
        }
        let bytes: Vec<u8> = row.try_get("encoded_undo")?;
        let undo: UndoRecord = postcard::from_bytes(&bytes)
            .map_err(|error| StoreError::Encoding(error.to_string()))?;
        let mut transaction = self.inner.pool.begin().await?;
        apply_mutations(
            &mut transaction,
            &instance,
            &undo.mutations,
            true,
            retains_processor_output(descriptor),
            undo.block,
            undo.finality,
        )
        .await?;
        sqlx::query(
            "DELETE FROM processor_coverage
             WHERE instance = ? AND block_number = ? AND block_hash = ?",
        )
        .bind(&instance)
        .bind(u64_i64(block_number.0, "block_number")?)
        .bind(block_hash.0.as_slice())
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "DELETE FROM applied_blocks
             WHERE instance = ? AND block_number = ? AND block_hash = ?",
        )
        .bind(&instance)
        .bind(u64_i64(block_number.0, "block_number")?)
        .bind(block_hash.0.as_slice())
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "DELETE FROM processor_artifact_candidates
             WHERE instance = ? AND block_number = ? AND block_hash = ?",
        )
        .bind(&instance)
        .bind(u64_i64(block_number.0, "artifact candidate block number")?)
        .bind(block_hash.0.as_slice())
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "DELETE FROM undo_journal
             WHERE instance = ? AND block_number = ? AND block_hash = ?",
        )
        .bind(&instance)
        .bind(u64_i64(block_number.0, "block_number")?)
        .bind(block_hash.0.as_slice())
        .execute(&mut *transaction)
        .await?;
        let restored_cursor = if descriptor.mode == ReductionMode::BlockLocal {
            highest_coverage_cursor(&mut transaction, descriptor, chain_id).await?
        } else {
            undo.prior_cursor.clone()
        };
        if let Some(prior) = restored_cursor {
            let encoded = OpaqueCursor::encode(CursorKind::Processor, &prior)?;
            upsert_cursor(&mut transaction, &instance, &prior, encoded.expose()).await?;
        } else {
            sqlx::query("DELETE FROM processor_cursors WHERE instance = ?")
                .bind(&instance)
                .execute(&mut *transaction)
                .await?;
        }
        let (first, last) = if descriptor.lifecycle.delivery.mode == DeliveryPolicyMode::None {
            (None, None)
        } else {
            append_changes(
                &mut transaction,
                &instance,
                &default_delivery_stream_id(descriptor),
                &DeliveryOrigin::live(&instance),
                chain_id,
                undo.block,
                undo.finality,
                ChangeDirection::Undo,
                &undo.inverse_changes,
                sink_ids,
            )
            .await?
        };
        transaction.commit().await?;
        if last.is_some() {
            self.inner.delivery_changes_available.notify_waiters();
        }
        Ok(UndoOutcome {
            restored_mutations: undo.mutations.len(),
            first_change_sequence: first,
            last_change_sequence: last,
        })
    }

    /// Mark all undo records through a height final. They can no longer be
    /// reversed by the normal runtime path.
    ///
    /// # Errors
    ///
    /// Returns an error when the height exceeds `SQLite`'s numeric range or the
    /// update fails.
    #[allow(clippy::too_many_lines)]
    pub async fn mark_finalized(
        &self,
        descriptor: &ProcessorDescriptor,
        through: BlockNumber,
    ) -> Result<u64, StoreError> {
        let instance = processor_instance(descriptor);
        let _guard = self.inner.writer.lock().await;
        let cursor = self.processor_cursor_by_instance(&instance).await?;
        let finality_context: Option<Vec<u8>> = sqlx::query_scalar(
            "SELECT encoded_undo FROM undo_journal
             WHERE instance = ? AND block_number <= ? AND finalized = 0
             ORDER BY block_number DESC LIMIT 1",
        )
        .bind(&instance)
        .bind(u64_i64(through.0, "block_number")?)
        .fetch_optional(&self.inner.pool)
        .await?;
        let finality_context = finality_context
            .as_deref()
            .map(|bytes| {
                postcard::from_bytes::<UndoRecord>(bytes)
                    .map_err(|error| StoreError::Encoding(error.to_string()))
            })
            .transpose()?;
        let mut transaction = self.inner.pool.begin().await?;
        promote_processor_artifact_candidates(&mut transaction, descriptor, &instance, through)
            .await?;
        let result = sqlx::query(
            "UPDATE undo_journal SET finalized = 1
             WHERE instance = ? AND block_number <= ? AND finalized = 0",
        )
        .bind(&instance)
        .bind(u64_i64(through.0, "block_number")?)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE processor_coverage SET finality = ?
             WHERE instance = ? AND block_number <= ? AND finality < ?",
        )
        .bind(finality_i64(Finality::Finalized))
        .bind(&instance)
        .bind(u64_i64(through.0, "block_number")?)
        .bind(finality_i64(Finality::Finalized))
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE output_entity_meta SET finality = ?
             WHERE instance = ? AND block_number <= ? AND finality < ?",
        )
        .bind(finality_i64(Finality::Finalized))
        .bind(&instance)
        .bind(u64_i64(through.0, "block_number")?)
        .bind(finality_i64(Finality::Finalized))
        .execute(&mut *transaction)
        .await?;
        let mut checkpoint_cursor = cursor.clone();
        if let Some(finalized_cursor) = checkpoint_cursor.as_mut()
            && finalized_cursor.block_number <= through
            && finalized_cursor.finality != Finality::Finalized
        {
            finalized_cursor.finality = Finality::Finalized;
            let encoded = OpaqueCursor::encode(CursorKind::Processor, &finalized_cursor)?;
            upsert_cursor(
                &mut transaction,
                &instance,
                finalized_cursor,
                encoded.expose(),
            )
            .await?;
        }
        if descriptor.lifecycle.delivery.mode != DeliveryPolicyMode::None
            && result.rows_affected() > 0
            && let (Some(cursor), Some(context)) = (&cursor, finality_context)
        {
            append_changes(
                &mut transaction,
                &instance,
                &default_delivery_stream_id(descriptor),
                &DeliveryOrigin::live(&instance),
                cursor.chain_id,
                context.block,
                Finality::Finalized,
                ChangeDirection::Finalized,
                &[DomainChange {
                    kind: "system.finality".to_owned(),
                    key: through.0.to_be_bytes().to_vec(),
                    operation: ChangeOperation::Upsert,
                    payload: through.0.to_be_bytes().to_vec(),
                }],
                &[],
            )
            .await?;
        }
        if result.rows_affected() > 0
            && matches!(
                descriptor.lifecycle.checkpoint.mode,
                CheckpointPolicyMode::Automatic
            )
            && let Some(cursor) = checkpoint_cursor.as_ref()
        {
            create_recovery_checkpoint(
                &mut transaction,
                descriptor,
                &instance,
                cursor,
                descriptor.lifecycle.checkpoint.keep,
            )
            .await?;
        }
        if !matches!(
            descriptor.lifecycle.artifacts.mode,
            ArtifactPolicyMode::None
        ) {
            enforce_artifact_storage_capacity(&mut transaction, self.inner.artifact_budget).await?;
        }
        transaction.commit().await?;
        if result.rows_affected() > 0 {
            self.inner.delivery_changes_available.notify_waiters();
        }
        Ok(result.rows_affected())
    }

    /// List automatic recovery checkpoints newest first.
    ///
    /// # Errors
    ///
    /// Returns an error for corrupt stored metadata or a failed read.
    pub async fn recovery_checkpoints(
        &self,
        descriptor: &ProcessorDescriptor,
    ) -> Result<Vec<RecoveryCheckpoint>, StoreError> {
        let instance = processor_instance(descriptor);
        let rows = sqlx::query(
            "SELECT checkpoint_id, block_number, block_hash, state_checksum,
                    length(state_snapshot) AS state_bytes, created_at_unix_ms
             FROM recovery_checkpoints
             WHERE instance = ?
             ORDER BY block_number DESC, checkpoint_id DESC",
        )
        .bind(&instance)
        .fetch_all(&self.inner.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(RecoveryCheckpoint {
                    checkpoint_id: i64_u64(row.try_get("checkpoint_id")?, "checkpoint ID")?,
                    processor_instance: instance.clone(),
                    block_number: BlockNumber(i64_u64(
                        row.try_get("block_number")?,
                        "checkpoint block number",
                    )?),
                    block_hash: decode_hash(row.try_get("block_hash")?)?,
                    state_checksum: decode_hash(row.try_get("state_checksum")?)?,
                    state_bytes: i64_u64(row.try_get("state_bytes")?, "checkpoint state bytes")?,
                    created_at_unix_ms: i64_u64(
                        row.try_get("created_at_unix_ms")?,
                        "checkpoint creation time",
                    )?,
                })
            })
            .collect()
    }

    /// Restore private processor state from an automatic checkpoint at the
    /// exact current cursor boundary.
    ///
    /// This repairs corrupted working state without changing output,
    /// coverage, delivery, or undo history. Rewinding to an older checkpoint
    /// is rejected because those independent storage classes would otherwise
    /// become inconsistent; an older recovery requires a deterministic
    /// rebuild or a full-store backup.
    ///
    /// # Errors
    ///
    /// Returns an error when the checkpoint is missing/corrupt, belongs to a
    /// different processor contract, is not at the current cursor, or the
    /// atomic state replacement fails.
    pub async fn restore_recovery_checkpoint(
        &self,
        descriptor: &ProcessorDescriptor,
        checkpoint_id: u64,
    ) -> Result<ProcessorCursor, StoreError> {
        let instance = processor_instance(descriptor);
        let _guard = self.inner.writer.lock().await;
        let row = sqlx::query(
            "SELECT state_snapshot, state_checksum
             FROM recovery_checkpoints
             WHERE instance = ? AND checkpoint_id = ?",
        )
        .bind(&instance)
        .bind(u64_i64(checkpoint_id, "checkpoint ID")?)
        .fetch_optional(&self.inner.pool)
        .await?
        .ok_or_else(|| StoreError::RecoveryCheckpointNotFound {
            instance: instance.clone(),
            checkpoint_id,
        })?;
        let encoded: Vec<u8> = row.try_get("state_snapshot")?;
        let checksum = decode_hash(row.try_get("state_checksum")?)?;
        let snapshot = decode_state_snapshot(descriptor, &instance, &encoded, checksum)?;
        let current = self.processor_cursor_by_instance(&instance).await?;
        if current.as_ref() != Some(&snapshot.cursor) {
            return Err(StoreError::CheckpointRestoreBoundary {
                checkpoint: snapshot.cursor.block_number,
                current: current.map(|cursor| cursor.block_number),
            });
        }

        let mut transaction = self.inner.pool.begin().await?;
        sqlx::query("DELETE FROM processor_state WHERE instance = ?")
            .bind(&instance)
            .execute(&mut *transaction)
            .await?;
        for entry in &snapshot.entries {
            sqlx::query(
                "INSERT INTO processor_state(instance, namespace, state_key, value)
                 VALUES (?, ?, ?, ?)",
            )
            .bind(&instance)
            .bind(&entry.namespace)
            .bind(&entry.key)
            .bind(&entry.value)
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        Ok(snapshot.cursor)
    }

    /// Create an operator-owned savepoint from the current atomic state/cursor.
    ///
    /// Savepoints are never removed by automatic checkpoint pruning.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid/duplicate ID, missing cursor, encoding
    /// failure, or failed write.
    pub async fn create_portable_savepoint(
        &self,
        descriptor: &ProcessorDescriptor,
        savepoint_id: &str,
    ) -> Result<PortableSavepoint, StoreError> {
        if !valid_consumer_id(savepoint_id) {
            return Err(StoreError::InvalidConfig(
                "savepoint ID must contain 1-128 portable characters".to_owned(),
            ));
        }
        let instance = self.register_processor(descriptor).await?;
        let _guard = self.inner.writer.lock().await;
        let cursor = self
            .processor_cursor_by_instance(&instance)
            .await?
            .ok_or_else(|| StoreError::NoProcessorCursor(instance.clone()))?;
        let mut transaction = self.inner.pool.begin().await?;
        let (encoded_cursor, encoded_snapshot, checksum) =
            encode_state_snapshot(&mut transaction, descriptor, &instance, &cursor).await?;
        let now = now_i64()?;
        let result = sqlx::query(
            "INSERT INTO portable_savepoints(
                instance, savepoint_id, block_number, block_hash,
                processor_cursor, state_snapshot, state_checksum,
                created_at_unix_ms
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(instance, savepoint_id) DO NOTHING",
        )
        .bind(&instance)
        .bind(savepoint_id)
        .bind(u64_i64(cursor.block_number.0, "savepoint block number")?)
        .bind(cursor.block_hash.0.as_slice())
        .bind(encoded_cursor)
        .bind(&encoded_snapshot)
        .bind(checksum.0.as_slice())
        .bind(now)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 0 {
            return Err(StoreError::SavepointExists {
                instance,
                savepoint_id: savepoint_id.to_owned(),
            });
        }
        transaction.commit().await?;
        Ok(PortableSavepoint {
            savepoint_id: savepoint_id.to_owned(),
            processor_instance: instance,
            block_number: cursor.block_number,
            block_hash: cursor.block_hash,
            state_checksum: checksum,
            state_bytes: u64::try_from(encoded_snapshot.len())
                .map_err(|_| StoreError::Numeric("savepoint state bytes"))?,
            created_at_unix_ms: i64_u64(now, "savepoint creation time")?,
        })
    }

    /// List operator-created portable savepoints newest first.
    ///
    /// # Errors
    ///
    /// Returns an error for corrupt metadata or a failed read.
    pub async fn portable_savepoints(
        &self,
        descriptor: &ProcessorDescriptor,
    ) -> Result<Vec<PortableSavepoint>, StoreError> {
        let instance = processor_instance(descriptor);
        let rows = sqlx::query(
            "SELECT savepoint_id, block_number, block_hash, state_checksum,
                    length(state_snapshot) AS state_bytes, created_at_unix_ms
             FROM portable_savepoints
             WHERE instance = ?
             ORDER BY created_at_unix_ms DESC, savepoint_id",
        )
        .bind(&instance)
        .fetch_all(&self.inner.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(PortableSavepoint {
                    savepoint_id: row.try_get("savepoint_id")?,
                    processor_instance: instance.clone(),
                    block_number: BlockNumber(i64_u64(
                        row.try_get("block_number")?,
                        "savepoint block number",
                    )?),
                    block_hash: decode_hash(row.try_get("block_hash")?)?,
                    state_checksum: decode_hash(row.try_get("state_checksum")?)?,
                    state_bytes: i64_u64(row.try_get("state_bytes")?, "savepoint state bytes")?,
                    created_at_unix_ms: i64_u64(
                        row.try_get("created_at_unix_ms")?,
                        "savepoint creation time",
                    )?,
                })
            })
            .collect()
    }

    /// Export a savepoint as a self-checking, versioned portable archive.
    ///
    /// # Errors
    ///
    /// Returns an error when the savepoint is missing, corrupt, or cannot be
    /// encoded.
    pub async fn export_portable_savepoint(
        &self,
        descriptor: &ProcessorDescriptor,
        savepoint_id: &str,
    ) -> Result<Vec<u8>, StoreError> {
        let instance = processor_instance(descriptor);
        let row = sqlx::query(
            "SELECT state_snapshot, state_checksum
             FROM portable_savepoints
             WHERE instance = ? AND savepoint_id = ?",
        )
        .bind(&instance)
        .bind(savepoint_id)
        .fetch_optional(&self.inner.pool)
        .await?
        .ok_or_else(|| StoreError::SavepointNotFound {
            instance: instance.clone(),
            savepoint_id: savepoint_id.to_owned(),
        })?;
        let snapshot: Vec<u8> = row.try_get("state_snapshot")?;
        let checksum = decode_hash(row.try_get("state_checksum")?)?;
        validate_state_snapshot(descriptor, &instance, &snapshot, checksum)?;
        postcard::to_allocvec(&PortableSavepointArchive {
            format_version: 1,
            snapshot,
            checksum,
        })
        .map_err(|error| StoreError::Encoding(error.to_string()))
    }

    /// Validate a portable archive against an exact immutable processor.
    ///
    /// This does not mutate the active job.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown format, checksum failure, descriptor
    /// mismatch, or corrupt cursor/state encoding.
    pub fn validate_portable_savepoint(
        descriptor: &ProcessorDescriptor,
        archive: &[u8],
    ) -> Result<ProcessorCursor, StoreError> {
        let archive: PortableSavepointArchive = postcard::from_bytes(archive)
            .map_err(|error| StoreError::Encoding(error.to_string()))?;
        if archive.format_version != 1 {
            return Err(StoreError::Encoding(format!(
                "unsupported portable savepoint version {}",
                archive.format_version
            )));
        }
        validate_state_snapshot(
            descriptor,
            descriptor.instance.as_str(),
            &archive.snapshot,
            archive.checksum,
        )
    }

    /// Explicitly delete an operator-owned savepoint.
    ///
    /// # Errors
    ///
    /// Returns an error when the savepoint is absent or the delete fails.
    pub async fn delete_portable_savepoint(
        &self,
        descriptor: &ProcessorDescriptor,
        savepoint_id: &str,
    ) -> Result<(), StoreError> {
        let instance = processor_instance(descriptor);
        let _guard = self.inner.writer.lock().await;
        let result = sqlx::query(
            "DELETE FROM portable_savepoints
             WHERE instance = ? AND savepoint_id = ?",
        )
        .bind(&instance)
        .bind(savepoint_id)
        .execute(&self.inner.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(StoreError::SavepointNotFound {
                instance,
                savepoint_id: savepoint_id.to_owned(),
            });
        }
        Ok(())
    }

    /// Resolve one processor-covered canonical hash to its block number.
    ///
    /// # Errors
    ///
    /// Returns an error for corrupt numeric data or a failed read.
    pub async fn coverage_block_by_hash(
        &self,
        descriptor: &ProcessorDescriptor,
        hash: BlockHash,
    ) -> Result<Option<BlockNumber>, StoreError> {
        let value: Option<i64> = sqlx::query_scalar(
            "SELECT block_number FROM processor_coverage
             WHERE instance = ? AND block_hash = ?
             LIMIT 1",
        )
        .bind(processor_instance(descriptor))
        .bind(hash.0.as_slice())
        .fetch_optional(&self.inner.pool)
        .await?;
        if let Some(value) = value {
            return i64_u64(value, "coverage block number")
                .map(BlockNumber)
                .map(Some);
        }
        let value: Option<i64> = sqlx::query_scalar(
            "SELECT segment_end FROM finalized_coverage_segments
             WHERE instance = ? AND end_hash = ?
             ORDER BY segment_end DESC LIMIT 1",
        )
        .bind(processor_instance(descriptor))
        .bind(hash.0.as_slice())
        .fetch_optional(&self.inner.pool)
        .await?;
        value
            .map(|value| i64_u64(value, "compact coverage block number").map(BlockNumber))
            .transpose()
    }

    /// Return the exact covered hash at one processor block number.
    ///
    /// # Errors
    ///
    /// Returns an error for corrupt durable data, numeric overflow, or a
    /// database failure.
    pub async fn coverage_hash(
        &self,
        descriptor: &ProcessorDescriptor,
        block_number: BlockNumber,
    ) -> Result<Option<BlockHash>, StoreError> {
        let value: Option<Vec<u8>> = sqlx::query_scalar(
            "SELECT block_hash FROM processor_coverage
             WHERE instance = ? AND block_number = ?",
        )
        .bind(processor_instance(descriptor))
        .bind(u64_i64(block_number.0, "block_number")?)
        .fetch_optional(&self.inner.pool)
        .await?;
        if let Some(value) = value {
            return decode_hash(value).map(Some);
        }
        let value: Option<Vec<u8>> = sqlx::query_scalar(
            "SELECT end_hash FROM finalized_coverage_segments
             WHERE instance = ? AND segment_end = ?",
        )
        .bind(processor_instance(descriptor))
        .bind(u64_i64(block_number.0, "block_number")?)
        .fetch_optional(&self.inner.pool)
        .await?;
        value.map(decode_hash).transpose()
    }

    /// Return the stored parent anchor for one exactly covered block.
    ///
    /// Legacy rows created before schema 10 have no parent anchor and return
    /// `None`; newly committed coverage always records it.
    ///
    /// # Errors
    ///
    /// Returns an error for corrupt durable data, numeric overflow, or a
    /// database failure.
    pub async fn coverage_parent_hash(
        &self,
        descriptor: &ProcessorDescriptor,
        block_number: BlockNumber,
    ) -> Result<Option<BlockHash>, StoreError> {
        let value: Option<Vec<u8>> = sqlx::query_scalar(
            "SELECT parent_hash FROM processor_coverage
             WHERE instance = ? AND block_number = ?",
        )
        .bind(processor_instance(descriptor))
        .bind(u64_i64(block_number.0, "block_number")?)
        .fetch_optional(&self.inner.pool)
        .await?
        .flatten();
        if let Some(value) = value {
            return decode_hash(value).map(Some);
        }
        let value: Option<Vec<u8>> = sqlx::query_scalar(
            "SELECT start_parent_hash FROM finalized_coverage_segments
             WHERE instance = ? AND segment_start = ?",
        )
        .bind(processor_instance(descriptor))
        .bind(u64_i64(block_number.0, "block_number")?)
        .fetch_optional(&self.inner.pool)
        .await?;
        value.map(decode_hash).transpose()
    }

    /// Return the highest block whose processor coverage is finalized.
    ///
    /// # Errors
    ///
    /// Returns an error for corrupt numeric data or a failed read.
    pub async fn finalized_through(
        &self,
        descriptor: &ProcessorDescriptor,
    ) -> Result<Option<BlockNumber>, StoreError> {
        let value: Option<i64> = sqlx::query_scalar(
            "SELECT MAX(block_number) FROM (
                SELECT block_number FROM processor_coverage
                 WHERE instance = ? AND finality = ?
                UNION ALL
                SELECT end_block AS block_number FROM finalized_coverage_intervals
                 WHERE instance = ?
             )",
        )
        .bind(processor_instance(descriptor))
        .bind(finality_i64(Finality::Finalized))
        .bind(processor_instance(descriptor))
        .fetch_one(&self.inner.pool)
        .await?;
        value
            .map(|value| i64_u64(value, "finalized block number").map(BlockNumber))
            .transpose()
    }

    /// Return the durable checksum of the exact mapped delta committed for a
    /// processor/block identity.
    ///
    /// This compact record survives recent raw-frame pruning and is used to
    /// revalidate live P2P-derived aggregations when an archive later catches
    /// up.
    ///
    /// # Errors
    ///
    /// Returns an error for corrupt durable data, numeric overflow, or a
    /// database failure.
    pub async fn applied_delta_checksum(
        &self,
        descriptor: &ProcessorDescriptor,
        block_number: BlockNumber,
        block_hash: BlockHash,
    ) -> Result<Option<BlockHash>, StoreError> {
        applied_checksum(
            &self.inner.pool,
            &processor_instance(descriptor),
            block_number,
            block_hash,
        )
        .await
    }

    /// Retain one finalized, checksummed map result under its processor
    /// instance owner. Duplicate writes of identical bytes are idempotent.
    ///
    /// # Errors
    ///
    /// Returns an error when artifact retention is disabled, the delta is
    /// incompatible or corrupt, a finalized block conflicts, or storage fails.
    pub async fn retain_finalized_artifact(
        &self,
        descriptor: &ProcessorDescriptor,
        delta: &EncodedDelta,
        finality: Finality,
    ) -> Result<(), StoreError> {
        if matches!(
            descriptor.lifecycle.artifacts.mode,
            leani_processor_api::ArtifactPolicyMode::None
        ) {
            return Err(StoreError::InvalidConfig(
                "processor artifact retention is disabled".to_owned(),
            ));
        }
        if finality != Finality::Finalized {
            return Err(StoreError::InvalidConfig(
                "processor artifacts may retain only finalized map results".to_owned(),
            ));
        }
        delta.validate(descriptor)?;
        let instance = self.register_processor(descriptor).await?;
        let encoded = delta.encode_durable()?;
        let now = now_i64()?;
        let _guard = self.inner.writer.lock().await;
        let mut transaction = self.inner.pool.begin().await?;
        stage_or_retain_processor_artifact(
            &mut transaction,
            descriptor,
            &instance,
            delta,
            &encoded,
            Finality::Finalized,
            now,
        )
        .await?;
        enforce_artifact_storage_capacity(&mut transaction, self.inner.artifact_budget).await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Read one exact finalized processor artifact by canonical block number.
    ///
    /// # Errors
    ///
    /// Fails closed on corrupt bytes, metadata disagreement, an incompatible
    /// descriptor, numeric overflow, or database I/O failure.
    pub async fn processor_artifact(
        &self,
        descriptor: &ProcessorDescriptor,
        block: BlockNumber,
    ) -> Result<Option<ProcessorArtifact>, StoreError> {
        let row = sqlx::query(
            "SELECT chain_id, block_number, block_hash, parent_hash,
                    block_timestamp, delta_schema_version, delta_checksum,
                    encoded_delta, retained_at_unix_ms
             FROM processor_artifacts WHERE instance = ? AND block_number = ?",
        )
        .bind(processor_instance(descriptor))
        .bind(u64_i64(block.0, "artifact block number")?)
        .fetch_optional(&self.inner.pool)
        .await?;
        if let Some(row) = row {
            return decode_processor_artifact(descriptor, &row).map(Some);
        }
        let retained_at_unix_ms: Option<i64> = sqlx::query_scalar(
            "SELECT created_at_unix_ms FROM processor_artifact_segments
             WHERE instance = ? AND state = 'active' AND from_block <= ? AND to_block >= ?
             ORDER BY from_block LIMIT 1",
        )
        .bind(processor_instance(descriptor))
        .bind(u64_i64(block.0, "artifact block number")?)
        .bind(u64_i64(block.0, "artifact block number")?)
        .fetch_optional(&self.inner.pool)
        .await?;
        let Some(retained_at_unix_ms) = retained_at_unix_ms else {
            return Ok(None);
        };
        let storage = self.artifact_segment_storage()?;
        let mut deltas = storage
            .sink
            .scan(descriptor, BlockRange::single(block), 1)
            .await
            .map_err(|error| StoreError::ArtifactSegment(error.to_string()))?;
        let delta = deltas.pop().ok_or_else(|| {
            StoreError::Invariant(format!(
                "artifact segment catalog has no record for block {}",
                block.0
            ))
        })?;
        processor_artifact_from_segment(delta, retained_at_unix_ms).map(Some)
    }

    /// Scan a bounded canonical artifact range in ascending block order.
    ///
    /// # Errors
    ///
    /// Rejects invalid limits and fails closed on corrupt/incompatible rows.
    pub async fn scan_processor_artifacts(
        &self,
        descriptor: &ProcessorDescriptor,
        range: BlockRange,
        limit: usize,
    ) -> Result<Vec<ProcessorArtifact>, StoreError> {
        if limit == 0 || limit > 10_000 {
            return Err(StoreError::InvalidConfig(
                "processor artifact scan limit must be in 1..=10000".to_owned(),
            ));
        }
        let rows = sqlx::query(
            "SELECT chain_id, block_number, block_hash, parent_hash,
                    block_timestamp, delta_schema_version, delta_checksum,
                    encoded_delta, retained_at_unix_ms
             FROM processor_artifacts
             WHERE instance = ? AND block_number BETWEEN ? AND ?
             ORDER BY block_number LIMIT ?",
        )
        .bind(processor_instance(descriptor))
        .bind(u64_i64(range.start().0, "artifact range start")?)
        .bind(u64_i64(range.end().0, "artifact range end")?)
        .bind(usize_i64(limit, "artifact scan limit")?)
        .fetch_all(&self.inner.pool)
        .await?;
        let mut artifacts = BTreeMap::<BlockNumber, ProcessorArtifact>::new();
        for row in &rows {
            let artifact = decode_processor_artifact(descriptor, row)?;
            artifacts.insert(artifact.delta.block.number, artifact);
        }
        let segment_rows = sqlx::query(
            "SELECT from_block, to_block, created_at_unix_ms
             FROM processor_artifact_segments
             WHERE instance = ? AND state = 'active'
               AND to_block >= ? AND from_block <= ?
             ORDER BY from_block",
        )
        .bind(processor_instance(descriptor))
        .bind(u64_i64(range.start().0, "artifact range start")?)
        .bind(u64_i64(range.end().0, "artifact range end")?)
        .fetch_all(&self.inner.pool)
        .await?;
        if !segment_rows.is_empty() {
            let storage = self.artifact_segment_storage()?;
            for row in segment_rows {
                let segment_start = BlockNumber(i64_u64(
                    row.try_get("from_block")?,
                    "artifact segment start",
                )?);
                if artifacts.len() >= limit
                    && artifacts
                        .last_key_value()
                        .is_some_and(|(last, _)| segment_start > *last)
                {
                    break;
                }
                let segment_end =
                    BlockNumber(i64_u64(row.try_get("to_block")?, "artifact segment end")?);
                let scan_range = BlockRange::new(
                    segment_start.max(range.start()),
                    segment_end.min(range.end()),
                )
                .map_err(|error| StoreError::Invariant(error.to_string()))?;
                let retained_at_unix_ms: i64 = row.try_get("created_at_unix_ms")?;
                let deltas = storage
                    .sink
                    .scan(descriptor, scan_range, limit)
                    .await
                    .map_err(|error| StoreError::ArtifactSegment(error.to_string()))?;
                for delta in deltas {
                    let block = delta.block.number;
                    let artifact = processor_artifact_from_segment(delta, retained_at_unix_ms)?;
                    if artifacts.insert(block, artifact).is_some() {
                        return Err(StoreError::Invariant(format!(
                            "artifact block {} is retained inline and in a segment",
                            block.0
                        )));
                    }
                }
                while artifacts.len() > limit {
                    artifacts.pop_last();
                }
            }
        }
        Ok(artifacts.into_values().take(limit).collect())
    }

    /// Build one deterministic portable export page from retained artifacts.
    /// The page stops before the first canonical gap or chain discontinuity.
    ///
    /// # Errors
    ///
    /// Rejects invalid limits and fails closed on corrupt artifact rows.
    pub async fn export_processor_artifacts(
        &self,
        descriptor: &ProcessorDescriptor,
        range: BlockRange,
        limit: usize,
    ) -> Result<ProcessorArtifactExport, StoreError> {
        let scanned = self
            .scan_processor_artifacts(descriptor, range, limit)
            .await?;
        let mut artifacts = Vec::<EncodedDelta>::with_capacity(scanned.len());
        for artifact in scanned {
            if let Some(previous) = artifacts.last()
                && (artifact.delta.chain_id != previous.chain_id
                    || artifact.delta.block.number.0 != previous.block.number.0.saturating_add(1)
                    || artifact.delta.block.parent_hash != previous.block.hash)
            {
                break;
            }
            artifacts.push(artifact.delta);
        }
        let exported_range = artifacts
            .first()
            .zip(artifacts.last())
            .map(|(first, last)| {
                BlockRange::new(first.block.number, last.block.number)
                    .map_err(|error| StoreError::Invariant(error.to_string()))
            })
            .transpose()?;
        let complete = exported_range.is_some_and(|exported| exported == range);
        let mut export = ProcessorArtifactExport {
            format_version: 1,
            contract: ProcessorArtifactContract::from_descriptor(descriptor),
            requested_range: range,
            exported_range,
            complete,
            artifacts,
            logical_digest: BlockHash::ZERO,
        };
        export.logical_digest = artifact_export_digest(&export);
        export.validate_shape()?;
        Ok(export)
    }

    /// Move bounded contiguous inline artifacts into durable seekable segments.
    ///
    /// Segment publication happens first. One `SQLite` transaction then records
    /// the segment location, coalesces ownership into ranges, and removes the
    /// redundant per-block rows. A crash before that transaction leaves the
    /// inline copy authoritative, while an exact retry adopts the already-
    /// published segment.
    ///
    /// # Errors
    ///
    /// Requires configured artifact segments and rejects corrupt, changed,
    /// non-contiguous, oversized, or conflicting artifacts.
    #[allow(clippy::too_many_lines)]
    pub async fn compact_processor_artifacts_to_segments(
        &self,
        descriptor: &ProcessorDescriptor,
        range: BlockRange,
        maximum_segments: usize,
        flush_partial: bool,
    ) -> Result<ArtifactTieringOutcome, StoreError> {
        if maximum_segments == 0 || maximum_segments > 1_000 {
            return Err(StoreError::InvalidConfig(
                "artifact compaction segment limit must be in 1..=1000".to_owned(),
            ));
        }
        if descriptor.lifecycle.artifacts.mode != ArtifactPolicyMode::Full {
            return Err(StoreError::InvalidConfig(
                "artifact segment compaction currently requires full retention".to_owned(),
            ));
        }
        let storage = self.inner.artifact_segments.clone().ok_or_else(|| {
            StoreError::InvalidConfig("artifact segments are disabled".to_owned())
        })?;
        let _compaction_guard = self.inner.artifact_compaction.lock().await;
        let target = usize::try_from(storage.target_blocks)
            .map_err(|_| StoreError::Numeric("artifact segment target blocks"))?;
        let mut outcome = ArtifactTieringOutcome::default();
        for _ in 0..maximum_segments {
            let artifacts = self
                .inline_artifact_compaction_batch(descriptor, range, target)
                .await?;
            if artifacts.is_empty() || (!flush_partial && artifacts.len() < target) {
                break;
            }
            let deltas = artifacts
                .iter()
                .map(|artifact| artifact.delta.clone())
                .collect::<Vec<_>>();
            let encoded_bytes = artifacts.iter().try_fold(0_u64, |total, artifact| {
                total
                    .checked_add(artifact.encoded_bytes)
                    .ok_or(StoreError::Numeric("artifact compaction logical bytes"))
            })?;
            let batch_range = BlockRange::new(
                deltas
                    .first()
                    .ok_or_else(|| StoreError::Invariant("empty artifact batch".to_owned()))?
                    .block
                    .number,
                deltas
                    .last()
                    .ok_or_else(|| StoreError::Invariant("empty artifact batch".to_owned()))?
                    .block
                    .number,
            )
            .map_err(|error| StoreError::Invariant(error.to_string()))?;
            if storage
                .sink
                .retained_batch(descriptor.instance.as_str(), batch_range)
                .await
                .is_none()
            {
                let directory_overhead = u64::try_from(artifacts.len())
                    .unwrap_or(u64::MAX)
                    .saturating_mul(128)
                    .saturating_add(4_096);
                let conservative_physical_bytes = encoded_bytes
                    .saturating_mul(2)
                    .saturating_add(directory_overhead)
                    .min(storage.maximum_segment_physical_bytes);
                enforce_physical_store_capacity_for_compaction(
                    &self.inner,
                    conservative_physical_bytes,
                )
                .await?;
            }
            let receipt = storage
                .sink
                .retain_finalized_batch(descriptor, &deltas)
                .await
                .map_err(|error| StoreError::ArtifactSegment(error.to_string()))?;
            let relative_path = relative_segment_path(&storage.root, &receipt.path)?;
            let segment_id =
                artifact_segment_id(descriptor, receipt.range, receipt.records_checksum);
            // Compaction is maintenance/history work. Live commits always
            // remain eligible ahead of this short catalog transaction.
            let _guard = self.inner.writer.lock_history().await;
            let mut transaction = self.inner.pool.begin().await?;
            begin_bulk_artifact_accounting(&mut transaction, descriptor.instance.as_str()).await?;
            validate_inline_artifacts_unchanged(
                &mut transaction,
                descriptor,
                receipt.range,
                &artifacts,
            )
            .await?;
            sqlx::query(
                "INSERT INTO processor_artifact_segments(
                    segment_id, instance, chain_id, from_block, to_block,
                    relative_path, artifacts, logical_bytes, physical_bytes,
                    records_checksum, created_at_unix_ms
                 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                 ON CONFLICT(segment_id) DO NOTHING",
            )
            .bind(&segment_id)
            .bind(processor_instance(descriptor))
            .bind(u64_i64(deltas[0].chain_id.0, "artifact segment chain ID")?)
            .bind(u64_i64(receipt.range.start().0, "artifact segment start")?)
            .bind(u64_i64(receipt.range.end().0, "artifact segment end")?)
            .bind(&relative_path)
            .bind(u64_i64(receipt.artifacts, "artifact segment count")?)
            .bind(u64_i64(
                receipt.logical_bytes,
                "artifact segment logical bytes",
            )?)
            .bind(u64_i64(
                receipt.physical_bytes,
                "artifact segment physical bytes",
            )?)
            .bind(receipt.records_checksum.as_slice())
            .bind(now_i64()?)
            .execute(&mut *transaction)
            .await?;
            validate_artifact_segment_catalog_row(
                &mut transaction,
                descriptor,
                &segment_id,
                &relative_path,
                &receipt,
            )
            .await?;
            let moved_owners = move_inline_artifact_owners_to_segment(
                &mut transaction,
                descriptor,
                &segment_id,
                receipt.range,
            )
            .await?;
            let deleted = sqlx::query(
                "DELETE FROM processor_artifacts
                 WHERE instance = ? AND block_number BETWEEN ? AND ?
                   AND payload_tier = 'inline'",
            )
            .bind(processor_instance(descriptor))
            .bind(u64_i64(receipt.range.start().0, "artifact segment start")?)
            .bind(u64_i64(receipt.range.end().0, "artifact segment end")?)
            .execute(&mut *transaction)
            .await?
            .rows_affected();
            if deleted != receipt.artifacts {
                return Err(StoreError::Invariant(
                    "artifact compaction input changed during one writer transaction".to_owned(),
                ));
            }
            finish_bulk_artifact_accounting(
                &mut transaction,
                descriptor.instance.as_str(),
                ArtifactTotalsDelta {
                    removed_artifacts: receipt.artifacts,
                    removed_bytes: encoded_bytes,
                    removed_owners: moved_owners,
                    ..ArtifactTotalsDelta::default()
                },
            )
            .await?;
            transaction.commit().await?;
            outcome.segments = outcome.segments.saturating_add(1);
            outcome.artifacts = outcome.artifacts.saturating_add(receipt.artifacts);
            outcome.logical_bytes = outcome.logical_bytes.saturating_add(receipt.logical_bytes);
            outcome.inline_payload_bytes_reclaimed = outcome
                .inline_payload_bytes_reclaimed
                .saturating_add(encoded_bytes);
        }
        Ok(outcome)
    }

    /// Return the segment tier's physical statistics when configured.
    pub async fn processor_artifact_segment_stats(&self) -> Option<ArtifactSegmentSinkStats> {
        let storage = self.inner.artifact_segments.as_ref()?;
        Some(storage.sink.stats().await)
    }

    /// Compact the earliest available contiguous inline range for one
    /// processor without requiring a caller-owned job range.
    ///
    /// This is the background-maintenance entry point. A non-flushing pass
    /// publishes only full target-sized segments; a flushing pass also closes
    /// the final partial range while leaving disjoint later ranges for the
    /// next bounded iteration.
    ///
    /// # Errors
    ///
    /// Returns the same configuration, corruption, I/O, or capacity errors as
    /// [`Self::compact_processor_artifacts_to_segments`].
    pub async fn compact_available_processor_artifacts_to_segments(
        &self,
        descriptor: &ProcessorDescriptor,
        maximum_segments: usize,
        flush_partial: bool,
    ) -> Result<ArtifactTieringOutcome, StoreError> {
        let bounds: Option<(i64, i64)> = sqlx::query_as(
            "SELECT MIN(block_number), MAX(block_number)
             FROM processor_artifacts
             WHERE instance = ? AND payload_tier = 'inline'
             HAVING COUNT(*) > 0",
        )
        .bind(processor_instance(descriptor))
        .fetch_optional(&self.inner.pool)
        .await?;
        let Some((start, end)) = bounds else {
            return Ok(ArtifactTieringOutcome::default());
        };
        let range = BlockRange::new(
            BlockNumber(i64_u64(start, "artifact compaction start")?),
            BlockNumber(i64_u64(end, "artifact compaction end")?),
        )
        .map_err(|error| StoreError::Invariant(error.to_string()))?;
        self.compact_processor_artifacts_to_segments(
            descriptor,
            range,
            maximum_segments,
            flush_partial,
        )
        .await
    }

    async fn inline_artifact_compaction_batch(
        &self,
        descriptor: &ProcessorDescriptor,
        range: BlockRange,
        limit: usize,
    ) -> Result<Vec<ProcessorArtifact>, StoreError> {
        let rows = sqlx::query(
            "SELECT chain_id, block_number, block_hash, parent_hash,
                    block_timestamp, delta_schema_version, delta_checksum,
                    encoded_delta, retained_at_unix_ms
             FROM processor_artifacts
             WHERE instance = ? AND block_number BETWEEN ? AND ?
               AND payload_tier = 'inline'
             ORDER BY block_number LIMIT ?",
        )
        .bind(processor_instance(descriptor))
        .bind(u64_i64(range.start().0, "artifact compaction start")?)
        .bind(u64_i64(range.end().0, "artifact compaction end")?)
        .bind(usize_i64(limit, "artifact compaction limit")?)
        .fetch_all(&self.inner.pool)
        .await?;
        let mut artifacts = Vec::<ProcessorArtifact>::with_capacity(rows.len());
        for row in &rows {
            let artifact = decode_processor_artifact(descriptor, row)?;
            if let Some(previous) = artifacts.last() {
                if artifact.delta.block.number.0 != previous.delta.block.number.0.saturating_add(1)
                {
                    break;
                }
                if artifact.delta.chain_id != previous.delta.chain_id
                    || artifact.delta.block.parent_hash != previous.delta.block.hash
                {
                    return Err(StoreError::Invariant(format!(
                        "processor artifact chain discontinuity at block {}",
                        artifact.delta.block.number.0
                    )));
                }
            }
            artifacts.push(artifact);
        }
        Ok(artifacts)
    }

    fn artifact_segment_storage(&self) -> Result<&ArtifactSegmentStorage, StoreError> {
        self.inner.artifact_segments.as_ref().ok_or_else(|| {
            StoreError::Invariant(
                "processor artifact row references the disabled segment tier".to_owned(),
            )
        })
    }

    /// Replay one bounded artifact page into a distinct compatible processor
    /// instance. Ordered reducers additionally require a contiguous start from
    /// their configured start/cursor.
    ///
    /// # Errors
    ///
    /// Rejects incompatible map contracts, same-instance replay, gaps, corrupt
    /// artifacts, and normal reducer/storage failures.
    pub async fn replay_processor_artifacts<P: Processor + ?Sized>(
        &self,
        source: &ProcessorDescriptor,
        target: &P,
        range: BlockRange,
        limit: usize,
        sink_ids: &[String],
    ) -> Result<ArtifactReplayOutcome, StoreError> {
        let target_descriptor = target.descriptor();
        ProcessorArtifactContract::from_descriptor(source).validate(target_descriptor)?;
        if source.instance == target_descriptor.instance {
            return Err(StoreError::ArtifactReplaySameInstance);
        }
        let export = self
            .export_processor_artifacts(source, range, limit)
            .await?;
        if export.artifacts.is_empty() {
            return Ok(ArtifactReplayOutcome::default());
        }
        let prior = self.processor_cursor(target_descriptor).await?;
        if target_descriptor.mode == ReductionMode::OrderedState {
            let first = export.artifacts.first().ok_or(StoreError::ArtifactExport)?;
            let expected = prior.as_ref().map_or_else(
                || match &target_descriptor.start {
                    StartPoint::Genesis => Ok((BlockNumber(0), None)),
                    StartPoint::Block(block) => Ok((*block, None)),
                    StartPoint::ProcessorCheckpoint(_) => Err(StoreError::InvalidConfig(
                        "checkpoint-seeded artifact replay requires an existing target cursor"
                            .to_owned(),
                    )),
                },
                |cursor| {
                    Ok((
                        BlockNumber(cursor.block_number.0.saturating_add(1)),
                        Some(cursor.block_hash),
                    ))
                },
            )?;
            if first.block.number != expected.0
                || expected
                    .1
                    .is_some_and(|parent| first.block.parent_hash != parent)
            {
                return Err(StoreError::ArtifactReplayGap {
                    expected: expected.0,
                    received: first.block.number,
                });
            }
        }
        let mut outcome = ArtifactReplayOutcome::default();
        let mut sequence = prior.map_or(1, |cursor| cursor.sequence.saturating_add(1));
        for artifact in export.artifacts {
            artifact.validate(target_descriptor)?;
            let cursor = ProcessorCursor {
                processor_id: target_descriptor.id.to_string(),
                processor_version: target_descriptor.version.to_string(),
                chain_id: artifact.chain_id,
                block_number: artifact.block.number,
                block_hash: artifact.block.hash,
                finality: Finality::Finalized,
                sequence,
            };
            match self.apply(target, cursor, &artifact, sink_ids).await? {
                ApplyOutcome::Applied { .. } => {
                    outcome.applied_artifacts = outcome.applied_artifacts.saturating_add(1);
                }
                ApplyOutcome::AlreadyApplied => {
                    outcome.duplicate_artifacts = outcome.duplicate_artifacts.saturating_add(1);
                }
            }
            outcome.processed_artifacts = outcome.processed_artifacts.saturating_add(1);
            outcome.last_block = Some(artifact.block.number);
            sequence = sequence.saturating_add(1);
        }
        Ok(outcome)
    }

    /// Inspect one processor's immutable artifact bounds and logical bytes.
    ///
    /// # Errors
    ///
    /// Returns an error for numeric overflow or database I/O failure.
    pub async fn processor_artifact_stats(
        &self,
        descriptor: &ProcessorDescriptor,
    ) -> Result<ProcessorArtifactStats, StoreError> {
        let instance = processor_instance(descriptor);
        let (earliest, latest): (Option<i64>, Option<i64>) = sqlx::query_as(
            "SELECT MIN(earliest), MAX(latest)
                 FROM (
                   SELECT MIN(block_number) AS earliest, MAX(block_number) AS latest
                   FROM processor_artifacts WHERE instance = ?
                   UNION ALL
                   SELECT MIN(from_block), MAX(to_block)
                   FROM processor_artifact_segments
                   WHERE instance = ? AND state = 'active'
                 )",
        )
        .bind(&instance)
        .bind(&instance)
        .fetch_one(&self.inner.pool)
        .await?;
        let (artifacts, bytes, owners): (i64, i64, i64) = sqlx::query_as(
            "SELECT COALESCE((SELECT retained_artifacts
                                FROM processor_artifact_totals WHERE instance = ?1), 0),
                    COALESCE((SELECT retained_bytes
                                FROM processor_artifact_totals WHERE instance = ?1), 0),
                    COALESCE((SELECT retained_owners
                                FROM processor_artifact_totals WHERE instance = ?1), 0)",
        )
        .bind(&instance)
        .fetch_one(&self.inner.pool)
        .await?;
        Ok(ProcessorArtifactStats {
            earliest_block: earliest
                .map(|value| i64_u64(value, "earliest artifact block").map(BlockNumber))
                .transpose()?,
            latest_block: latest
                .map(|value| i64_u64(value, "latest artifact block").map(BlockNumber))
                .transpose()?,
            artifacts: i64_u64(artifacts, "processor artifacts")?,
            logical_bytes: i64_u64(bytes, "processor artifact bytes")?,
            owners: i64_u64(owners, "processor artifact owners")?,
        })
    }

    /// Add an independent durable owner to every existing artifact in a
    /// range. Missing artifact blocks do not create phantom ownership.
    ///
    /// # Errors
    ///
    /// Rejects invalid owner IDs and returns database/numeric errors.
    pub async fn add_processor_artifact_owner(
        &self,
        descriptor: &ProcessorDescriptor,
        range: BlockRange,
        kind: ArtifactOwnerKind,
        owner_id: &str,
    ) -> Result<u64, StoreError> {
        validate_artifact_owner_id(owner_id)?;
        let instance = processor_instance(descriptor);
        let _guard = self.inner.writer.lock().await;
        let mut transaction = self.inner.pool.begin().await?;
        let result = sqlx::query(
            "INSERT OR IGNORE INTO processor_artifact_owners(
                instance, block_number, owner_kind, owner_id, created_at_unix_ms
             )
             SELECT instance, block_number, ?, ?, ? FROM processor_artifacts
             WHERE instance = ? AND block_number BETWEEN ? AND ?",
        )
        .bind(kind.as_str())
        .bind(owner_id)
        .bind(now_i64()?)
        .bind(&instance)
        .bind(u64_i64(range.start().0, "artifact owner range start")?)
        .bind(u64_i64(range.end().0, "artifact owner range end")?)
        .execute(&mut *transaction)
        .await?;
        let segments = sqlx::query(
            "SELECT segment_id, from_block, to_block
             FROM processor_artifact_segments
             WHERE instance = ? AND state = 'active'
               AND to_block >= ? AND from_block <= ?",
        )
        .bind(&instance)
        .bind(u64_i64(range.start().0, "artifact owner range start")?)
        .bind(u64_i64(range.end().0, "artifact owner range end")?)
        .fetch_all(&mut *transaction)
        .await?;
        let mut added = result.rows_affected();
        for segment in segments {
            let segment_start = i64_u64(
                segment.try_get("from_block")?,
                "artifact segment owner start",
            )?;
            let segment_end = i64_u64(segment.try_get("to_block")?, "artifact segment owner end")?;
            added = added.saturating_add(
                add_or_merge_segment_owner(
                    &mut transaction,
                    &segment.try_get::<String, _>("segment_id")?,
                    kind,
                    owner_id,
                    range.start().0.max(segment_start),
                    range.end().0.min(segment_end),
                    now_i64()?,
                )
                .await?,
            );
        }
        transaction.commit().await?;
        Ok(added)
    }

    /// List every durable owner protecting one artifact.
    ///
    /// # Errors
    ///
    /// Fails on malformed durable metadata, numeric overflow, or database I/O.
    pub async fn processor_artifact_owners(
        &self,
        descriptor: &ProcessorDescriptor,
        block: BlockNumber,
    ) -> Result<Vec<ArtifactOwner>, StoreError> {
        let rows = sqlx::query(
            "SELECT owner_kind, owner_id, MIN(created_at_unix_ms) AS created_at_unix_ms
             FROM (
               SELECT owner_kind, owner_id, created_at_unix_ms
               FROM processor_artifact_owners
               WHERE instance = ? AND block_number = ?
               UNION ALL
               SELECT owners.owner_kind, owners.owner_id, owners.created_at_unix_ms
               FROM processor_artifact_segment_owners AS owners
               JOIN processor_artifact_segments AS segments
                 ON segments.segment_id = owners.segment_id
               WHERE segments.instance = ? AND segments.state = 'active'
                 AND ? BETWEEN owners.from_block AND owners.to_block
             )
             GROUP BY owner_kind, owner_id
             ORDER BY owner_kind, owner_id",
        )
        .bind(processor_instance(descriptor))
        .bind(u64_i64(block.0, "artifact owner block")?)
        .bind(processor_instance(descriptor))
        .bind(u64_i64(block.0, "artifact owner block")?)
        .fetch_all(&self.inner.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(ArtifactOwner {
                    kind: ArtifactOwnerKind::parse(row.try_get("owner_kind")?)?,
                    id: row.try_get("owner_id")?,
                    created_at_unix_ms: i64_u64(
                        row.try_get("created_at_unix_ms")?,
                        "artifact owner creation time",
                    )?,
                })
            })
            .collect()
    }

    /// Release one owner's claims in a range and atomically delete unowned
    /// inline artifacts or wholly unowned immutable segments.
    ///
    /// A partial owner range pins its enclosing physical segment. Adjacent
    /// artifacts can therefore remain readable until the last owner anywhere
    /// in that segment is released; `deleted_artifacts` reports physical
    /// reclamation rather than the number of claims released.
    ///
    /// # Errors
    ///
    /// Rejects invalid owner IDs and returns database/numeric failures.
    pub async fn release_processor_artifacts(
        &self,
        descriptor: &ProcessorDescriptor,
        range: BlockRange,
        kind: ArtifactOwnerKind,
        owner_id: &str,
    ) -> Result<ArtifactPruneOutcome, StoreError> {
        validate_artifact_owner_id(owner_id)?;
        let instance = processor_instance(descriptor);
        let start = u64_i64(range.start().0, "artifact prune range start")?;
        let end = u64_i64(range.end().0, "artifact prune range end")?;
        let _guard = self.inner.writer.lock().await;
        let mut transaction = self.inner.pool.begin().await?;
        let inline_released = sqlx::query(
            "DELETE FROM processor_artifact_owners
             WHERE instance = ? AND block_number BETWEEN ? AND ?
               AND owner_kind = ? AND owner_id = ?",
        )
        .bind(&instance)
        .bind(start)
        .bind(end)
        .bind(kind.as_str())
        .bind(owner_id)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        let segment_released =
            release_segment_owner_ranges(&mut transaction, &instance, range, kind, owner_id)
                .await?;
        let (deletable, bytes): (i64, i64) = sqlx::query_as(
            "SELECT COUNT(*), COALESCE(SUM(encoded_bytes), 0)
             FROM processor_artifacts AS artifacts
             WHERE artifacts.instance = ? AND artifacts.block_number BETWEEN ? AND ?
               AND NOT EXISTS (
                 SELECT 1 FROM processor_artifact_owners AS owners
                 WHERE owners.instance = artifacts.instance
                   AND owners.block_number = artifacts.block_number
               )",
        )
        .bind(&instance)
        .bind(start)
        .bind(end)
        .fetch_one(&mut *transaction)
        .await?;
        let deleted = sqlx::query(
            "DELETE FROM processor_artifacts
             WHERE instance = ? AND block_number BETWEEN ? AND ?
               AND NOT EXISTS (
                 SELECT 1 FROM processor_artifact_owners AS owners
                 WHERE owners.instance = processor_artifacts.instance
                   AND owners.block_number = processor_artifacts.block_number
               )",
        )
        .bind(&instance)
        .bind(start)
        .bind(end)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        let deleting_segments = sqlx::query(
            "UPDATE processor_artifact_segments AS segments
             SET state = 'deleting'
             WHERE instance = ? AND state = 'active'
               AND to_block >= ? AND from_block <= ?
               AND NOT EXISTS (
                 SELECT 1 FROM processor_artifact_segment_owners AS owners
                 WHERE owners.segment_id = segments.segment_id
               )
             RETURNING segment_id, from_block, to_block, artifacts, logical_bytes",
        )
        .bind(&instance)
        .bind(start)
        .bind(end)
        .fetch_all(&mut *transaction)
        .await?;
        transaction.commit().await?;
        let expected = i64_u64(deletable, "deletable processor artifacts")?;
        if deleted != expected {
            return Err(StoreError::Invariant(
                "artifact prune count changed inside one writer transaction".to_owned(),
            ));
        }
        self.finish_artifact_segment_deletions(&instance, &deleting_segments)
            .await?;
        let segment_deleted_artifacts =
            deleting_segments.iter().try_fold(0_u64, |total, row| {
                Ok::<_, StoreError>(total.saturating_add(i64_u64(
                    row.try_get("artifacts")?,
                    "deleted segment artifacts",
                )?))
            })?;
        let segment_deleted_bytes = deleting_segments.iter().try_fold(0_u64, |total, row| {
            Ok::<_, StoreError>(total.saturating_add(i64_u64(
                row.try_get("logical_bytes")?,
                "deleted segment artifact bytes",
            )?))
        })?;
        Ok(ArtifactPruneOutcome {
            released_owners: inline_released.saturating_add(segment_released),
            deleted_artifacts: deleted.saturating_add(segment_deleted_artifacts),
            deleted_logical_bytes: i64_u64(bytes, "deleted processor artifact bytes")?
                .saturating_add(segment_deleted_bytes),
        })
    }

    async fn finish_artifact_segment_deletions(
        &self,
        instance: &str,
        segments: &[SqliteRow],
    ) -> Result<(), StoreError> {
        if segments.is_empty() {
            return Ok(());
        }
        let storage = self.artifact_segment_storage()?;
        for segment in segments {
            let segment_id: String = segment.try_get("segment_id")?;
            let range = BlockRange::new(
                BlockNumber(i64_u64(
                    segment.try_get("from_block")?,
                    "artifact segment start",
                )?),
                BlockNumber(i64_u64(
                    segment.try_get("to_block")?,
                    "artifact segment end",
                )?),
            )
            .map_err(|error| StoreError::Invariant(error.to_string()))?;
            storage
                .sink
                .remove_exact(instance, range)
                .await
                .map_err(|error| StoreError::ArtifactSegment(error.to_string()))?;
            sqlx::query(
                "DELETE FROM processor_artifact_segments
                 WHERE segment_id = ? AND state = 'deleting'",
            )
            .bind(segment_id)
            .execute(&self.inner.pool)
            .await?;
        }
        Ok(())
    }

    /// Persist a mapped delta before ordered reduction.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid delta, processor identity conflict,
    /// encoding failure, or database failure.
    pub async fn persist_delta(
        &self,
        descriptor: &ProcessorDescriptor,
        delta: &EncodedDelta,
    ) -> Result<(), StoreError> {
        delta.validate(descriptor)?;
        let instance = self.register_processor(descriptor).await?;
        let encoded = delta.encode_durable()?;
        let _guard = self.inner.writer.lock().await;
        sqlx::query(
            "INSERT INTO pending_deltas(
                instance, block_number, block_hash, encoded_delta, inserted_at_unix_ms
             ) VALUES (?, ?, ?, ?, ?)
             ON CONFLICT(instance, block_number, block_hash) DO NOTHING",
        )
        .bind(&instance)
        .bind(u64_i64(delta.block.number.0, "block_number")?)
        .bind(delta.block.hash.0.as_slice())
        .bind(&encoded)
        .bind(now_i64()?)
        .execute(&self.inner.pool)
        .await?;
        let stored: Vec<u8> = sqlx::query_scalar(
            "SELECT encoded_delta FROM pending_deltas
             WHERE instance = ? AND block_number = ? AND block_hash = ?",
        )
        .bind(&instance)
        .bind(u64_i64(delta.block.number.0, "block_number")?)
        .bind(delta.block.hash.0.as_slice())
        .fetch_one(&self.inner.pool)
        .await?;
        if stored != encoded {
            return Err(StoreError::ConflictingPendingDelta(delta.block.number));
        }
        Ok(())
    }

    /// Read durable mapped deltas in block/hash order.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid limits, database failures, or a corrupt or
    /// incompatible durable delta.
    pub async fn pending_deltas(
        &self,
        descriptor: &ProcessorDescriptor,
        from: BlockNumber,
        limit: usize,
    ) -> Result<Vec<EncodedDelta>, StoreError> {
        if limit == 0 || limit > 10_000 {
            return Err(StoreError::InvalidConfig(
                "pending delta limit must be in 1..=10000".to_owned(),
            ));
        }
        let rows: Vec<Vec<u8>> = sqlx::query_scalar(
            "SELECT encoded_delta FROM pending_deltas
             WHERE instance = ? AND block_number >= ?
             ORDER BY block_number, block_hash LIMIT ?",
        )
        .bind(processor_instance(descriptor))
        .bind(u64_i64(from.0, "block_number")?)
        .bind(usize_i64(limit, "limit")?)
        .fetch_all(&self.inner.pool)
        .await?;
        rows.into_iter()
            .map(|encoded| {
                EncodedDelta::decode_durable(descriptor, &encoded).map_err(StoreError::from)
            })
            .collect()
    }

    /// Remove a mapped delta for an abandoned branch.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid numeric input or a database failure.
    pub async fn delete_pending_delta(
        &self,
        descriptor: &ProcessorDescriptor,
        block: BlockRef,
    ) -> Result<bool, StoreError> {
        let _guard = self.inner.writer.lock().await;
        let result = sqlx::query(
            "DELETE FROM pending_deltas
             WHERE instance = ? AND block_number = ? AND block_hash = ?",
        )
        .bind(processor_instance(descriptor))
        .bind(u64_i64(block.number.0, "block_number")?)
        .bind(block.hash.0.as_slice())
        .execute(&self.inner.pool)
        .await?;
        Ok(result.rows_affected() != 0)
    }

    /// Persist one complete recent frame and its canonical block pointer.
    ///
    /// # Errors
    ///
    /// Rejects invalid/corrupt material, canonical conflicts, oversized
    /// numeric fields, durable encoding failures, and database failures.
    pub async fn store_recent_frame(&self, frame: &BlockFrame) -> Result<(), StoreError> {
        frame
            .validate_shape()
            .map_err(|error| StoreError::Invariant(error.to_owned()))?;
        let encoded = leani_primitives::durable::encode(
            DurableKind::BlockFrame,
            leani_primitives::BLOCK_FRAME_SCHEMA_VERSION,
            frame,
        )
        .map_err(|error| StoreError::Encoding(error.to_string()))?;
        let _guard = self.inner.writer.lock().await;
        let existing: Option<Vec<u8>> = sqlx::query_scalar(
            "SELECT block_hash FROM canonical_blocks
             WHERE chain_id = ? AND block_number = ?",
        )
        .bind(u64_i64(frame.chain_id.0, "chain_id")?)
        .bind(u64_i64(frame.block.number.0, "block_number")?)
        .fetch_optional(&self.inner.pool)
        .await?;
        if let Some(existing) = existing {
            let stored = decode_hash(existing)?;
            if stored != frame.block.hash {
                return Err(StoreError::CanonicalConflict {
                    block: frame.block.number,
                    stored,
                    incoming: frame.block.hash,
                });
            }
        }
        let mut transaction = self.inner.pool.begin().await?;
        sqlx::query(
            "INSERT INTO canonical_blocks(
                chain_id, block_number, block_hash, parent_hash, timestamp, finality
             ) VALUES (?, ?, ?, ?, ?, ?)
             ON CONFLICT(chain_id, block_number) DO UPDATE SET
                finality = MAX(canonical_blocks.finality, excluded.finality)",
        )
        .bind(u64_i64(frame.chain_id.0, "chain_id")?)
        .bind(u64_i64(frame.block.number.0, "block_number")?)
        .bind(frame.block.hash.0.as_slice())
        .bind(frame.block.parent_hash.0.as_slice())
        .bind(u64_i64(frame.block.timestamp, "block timestamp")?)
        .bind(finality_i64(frame.finality))
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "INSERT INTO recent_blocks(
                chain_id, block_number, block_hash, encoded_frame, bytes
             ) VALUES (?, ?, ?, ?, ?)
             ON CONFLICT(chain_id, block_number, block_hash) DO NOTHING",
        )
        .bind(u64_i64(frame.chain_id.0, "chain_id")?)
        .bind(u64_i64(frame.block.number.0, "block_number")?)
        .bind(frame.block.hash.0.as_slice())
        .bind(&encoded)
        .bind(usize_i64(encoded.len(), "recent frame bytes")?)
        .execute(&mut *transaction)
        .await?;
        let stored: Vec<u8> = sqlx::query_scalar(
            "SELECT encoded_frame FROM recent_blocks
             WHERE chain_id = ? AND block_number = ? AND block_hash = ?",
        )
        .bind(u64_i64(frame.chain_id.0, "chain_id")?)
        .bind(u64_i64(frame.block.number.0, "block_number")?)
        .bind(frame.block.hash.0.as_slice())
        .fetch_one(&mut *transaction)
        .await?;
        if stored != encoded {
            let stored_frame: BlockFrame = leani_primitives::durable::decode(
                DurableKind::BlockFrame,
                leani_primitives::BLOCK_FRAME_SCHEMA_VERSION,
                &stored,
            )
            .map_err(|error| StoreError::Encoding(error.to_string()))?;
            if !same_recent_material(&stored_frame, frame) {
                return Err(StoreError::Invariant(format!(
                    "recent frame {} conflicts with retained material for the same block hash",
                    frame.block.number.0
                )));
            }
        }
        upsert_recent_transaction_locators(&mut transaction, frame).await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Seed canonical metadata from an independently verified checkpoint.
    ///
    /// This stores no raw frame and is used to anchor the first retained live
    /// descendant.
    ///
    /// # Errors
    ///
    /// Rejects a contradictory block at the same height and database failures.
    pub async fn store_canonical_anchor(
        &self,
        chain_id: ChainId,
        block: BlockRef,
        finality: Finality,
    ) -> Result<(), StoreError> {
        let _guard = self.inner.writer.lock().await;
        sqlx::query(
            "INSERT INTO canonical_blocks(
                chain_id, block_number, block_hash, parent_hash, timestamp, finality
             ) VALUES (?, ?, ?, ?, ?, ?)
             ON CONFLICT(chain_id, block_number) DO UPDATE SET
                finality = MAX(canonical_blocks.finality, excluded.finality)",
        )
        .bind(u64_i64(chain_id.0, "chain_id")?)
        .bind(u64_i64(block.number.0, "block_number")?)
        .bind(block.hash.0.as_slice())
        .bind(block.parent_hash.0.as_slice())
        .bind(u64_i64(block.timestamp, "block timestamp")?)
        .bind(finality_i64(finality))
        .execute(&self.inner.pool)
        .await?;
        let stored: Vec<u8> = sqlx::query_scalar(
            "SELECT block_hash FROM canonical_blocks
             WHERE chain_id = ? AND block_number = ?",
        )
        .bind(u64_i64(chain_id.0, "chain_id")?)
        .bind(u64_i64(block.number.0, "block_number")?)
        .fetch_one(&self.inner.pool)
        .await?;
        let stored = decode_hash(stored)?;
        if stored != block.hash {
            return Err(StoreError::CanonicalConflict {
                block: block.number,
                stored,
                incoming: block.hash,
            });
        }
        Ok(())
    }

    /// Switch the retained canonical recent branch in one transaction.
    ///
    /// Reverted blocks are ordered from old tip toward the ancestor; applied
    /// frames are ordered from the ancestor toward the new tip. Old raw frames
    /// remain as bounded fork candidates, while canonical pointers move to the
    /// replacement branch.
    ///
    /// # Errors
    ///
    /// Rejects non-contiguous branches, canonical mismatches, finalized
    /// reverts, invalid replacement frames, and database failures.
    #[allow(clippy::too_many_lines)]
    pub async fn reorg_recent_frames(
        &self,
        chain_id: ChainId,
        reverted: &[BlockRef],
        applied: &[BlockFrame],
    ) -> Result<RecentReorgOutcome, StoreError> {
        let Some(lowest) = reverted.last().copied() else {
            return Err(StoreError::Invariant(
                "recent reorg must revert at least one block".to_owned(),
            ));
        };
        if lowest.number.0 == 0 {
            return Err(StoreError::Invariant(
                "the genesis block cannot be reverted".to_owned(),
            ));
        }
        for pair in reverted.windows(2) {
            let newer = pair[0];
            let older = pair[1];
            if newer.number.0 != older.number.0.saturating_add(1) || newer.parent_hash != older.hash
            {
                return Err(StoreError::Invariant(
                    "reverted recent branch is not tip-first and contiguous".to_owned(),
                ));
            }
        }
        let ancestor = BlockRef {
            number: BlockNumber(lowest.number.0 - 1),
            hash: lowest.parent_hash,
            parent_hash: BlockHash::ZERO,
            timestamp: 0,
        };
        let mut encoded_applied = Vec::with_capacity(applied.len());
        let mut expected_number = lowest.number;
        let mut expected_parent = ancestor.hash;
        for frame in applied {
            frame
                .validate_shape()
                .map_err(|error| StoreError::Invariant(error.to_owned()))?;
            if frame.chain_id != chain_id
                || frame.block.number != expected_number
                || frame.block.parent_hash != expected_parent
            {
                return Err(StoreError::Invariant(
                    "replacement recent branch is not ancestor-first and contiguous".to_owned(),
                ));
            }
            encoded_applied.push(
                leani_primitives::durable::encode(
                    DurableKind::BlockFrame,
                    leani_primitives::BLOCK_FRAME_SCHEMA_VERSION,
                    frame,
                )
                .map_err(|error| StoreError::Encoding(error.to_string()))?,
            );
            expected_number = BlockNumber(expected_number.0.saturating_add(1));
            expected_parent = frame.block.hash;
        }

        let _guard = self.inner.writer.lock().await;
        let mut transaction = self.inner.pool.begin().await?;
        let ancestor_hash: Option<Vec<u8>> = sqlx::query_scalar(
            "SELECT block_hash FROM canonical_blocks
             WHERE chain_id = ? AND block_number = ?",
        )
        .bind(u64_i64(chain_id.0, "chain_id")?)
        .bind(u64_i64(ancestor.number.0, "ancestor block_number")?)
        .fetch_optional(&mut *transaction)
        .await?;
        if ancestor_hash.as_deref() != Some(ancestor.hash.0.as_slice()) {
            return Err(StoreError::Invariant(
                "recent reorg ancestor is not the retained canonical block".to_owned(),
            ));
        }
        for block in reverted {
            let row: Option<(Vec<u8>, i64)> = sqlx::query_as(
                "SELECT block_hash, finality FROM canonical_blocks
                 WHERE chain_id = ? AND block_number = ?",
            )
            .bind(u64_i64(chain_id.0, "chain_id")?)
            .bind(u64_i64(block.number.0, "reverted block_number")?)
            .fetch_optional(&mut *transaction)
            .await?;
            let Some((stored_hash, finality)) = row else {
                return Err(StoreError::Invariant(format!(
                    "reverted block {} is not retained canonical material",
                    block.number.0
                )));
            };
            if stored_hash.as_slice() != block.hash.0
                || finality == finality_i64(Finality::Finalized)
            {
                return Err(StoreError::Invariant(format!(
                    "reverted block {} conflicts with canonical or finalized material",
                    block.number.0
                )));
            }
            delete_recent_transaction_locators(&mut transaction, chain_id, *block).await?;
            sqlx::query(
                "DELETE FROM canonical_blocks
                 WHERE chain_id = ? AND block_number = ? AND block_hash = ?",
            )
            .bind(u64_i64(chain_id.0, "chain_id")?)
            .bind(u64_i64(block.number.0, "reverted block_number")?)
            .bind(block.hash.0.as_slice())
            .execute(&mut *transaction)
            .await?;
        }
        for (frame, encoded) in applied.iter().zip(&encoded_applied) {
            sqlx::query(
                "INSERT INTO recent_blocks(
                    chain_id, block_number, block_hash, encoded_frame, bytes
                 ) VALUES (?, ?, ?, ?, ?)
                 ON CONFLICT(chain_id, block_number, block_hash) DO NOTHING",
            )
            .bind(u64_i64(chain_id.0, "chain_id")?)
            .bind(u64_i64(frame.block.number.0, "block_number")?)
            .bind(frame.block.hash.0.as_slice())
            .bind(encoded)
            .bind(usize_i64(encoded.len(), "recent frame bytes")?)
            .execute(&mut *transaction)
            .await?;
            let stored: Vec<u8> = sqlx::query_scalar(
                "SELECT encoded_frame FROM recent_blocks
                 WHERE chain_id = ? AND block_number = ? AND block_hash = ?",
            )
            .bind(u64_i64(chain_id.0, "chain_id")?)
            .bind(u64_i64(frame.block.number.0, "block_number")?)
            .bind(frame.block.hash.0.as_slice())
            .fetch_one(&mut *transaction)
            .await?;
            if stored != *encoded {
                let stored_frame: BlockFrame = leani_primitives::durable::decode(
                    DurableKind::BlockFrame,
                    leani_primitives::BLOCK_FRAME_SCHEMA_VERSION,
                    &stored,
                )
                .map_err(|error| StoreError::Encoding(error.to_string()))?;
                if !same_recent_material(&stored_frame, frame) {
                    return Err(StoreError::Invariant(format!(
                        "replacement frame {} conflicts with retained material",
                        frame.block.number.0
                    )));
                }
            }
            sqlx::query(
                "INSERT INTO canonical_blocks(
                    chain_id, block_number, block_hash, parent_hash, timestamp, finality
                 ) VALUES (?, ?, ?, ?, ?, ?)",
            )
            .bind(u64_i64(chain_id.0, "chain_id")?)
            .bind(u64_i64(frame.block.number.0, "block_number")?)
            .bind(frame.block.hash.0.as_slice())
            .bind(frame.block.parent_hash.0.as_slice())
            .bind(u64_i64(frame.block.timestamp, "block timestamp")?)
            .bind(finality_i64(frame.finality))
            .execute(&mut *transaction)
            .await?;
            upsert_recent_transaction_locators(&mut transaction, frame).await?;
        }
        transaction.commit().await?;
        let new_tip = applied.last().map_or(ancestor, |frame| frame.block);
        Ok(RecentReorgOutcome {
            reverted_frames: u64::try_from(reverted.len()).unwrap_or(u64::MAX),
            applied_frames: u64::try_from(applied.len()).unwrap_or(u64::MAX),
            new_tip,
        })
    }

    /// Read one retained canonical frame by block number.
    ///
    /// # Errors
    ///
    /// Returns an error for corrupt durable material or a database failure.
    pub async fn recent_frame(
        &self,
        chain_id: ChainId,
        block_number: BlockNumber,
    ) -> Result<Option<BlockFrame>, StoreError> {
        let encoded: Option<Vec<u8>> = sqlx::query_scalar(
            "SELECT recent.encoded_frame
             FROM canonical_blocks AS canonical
             JOIN recent_blocks AS recent
               ON recent.chain_id = canonical.chain_id
              AND recent.block_number = canonical.block_number
              AND recent.block_hash = canonical.block_hash
             WHERE canonical.chain_id = ? AND canonical.block_number = ?",
        )
        .bind(u64_i64(chain_id.0, "chain_id")?)
        .bind(u64_i64(block_number.0, "block_number")?)
        .fetch_optional(&self.inner.pool)
        .await?;
        encoded
            .map(|encoded| {
                leani_primitives::durable::decode(
                    DurableKind::BlockFrame,
                    leani_primitives::BLOCK_FRAME_SCHEMA_VERSION,
                    &encoded,
                )
                .map_err(|error| StoreError::Encoding(error.to_string()))
            })
            .transpose()
    }

    /// Read one retained frame by its exact block hash, including a bounded
    /// non-canonical fork candidate.
    ///
    /// # Errors
    ///
    /// Returns an error for corrupt durable material or a database failure.
    pub async fn recent_frame_by_hash(
        &self,
        chain_id: ChainId,
        block_hash: BlockHash,
    ) -> Result<Option<BlockFrame>, StoreError> {
        let encoded: Option<Vec<u8>> = sqlx::query_scalar(
            "SELECT encoded_frame FROM recent_blocks
             WHERE chain_id = ? AND block_hash = ? LIMIT 1",
        )
        .bind(u64_i64(chain_id.0, "chain_id")?)
        .bind(block_hash.0.as_slice())
        .fetch_optional(&self.inner.pool)
        .await?;
        encoded
            .map(|encoded| {
                leani_primitives::durable::decode(
                    DurableKind::BlockFrame,
                    leani_primitives::BLOCK_FRAME_SCHEMA_VERSION,
                    &encoded,
                )
                .map_err(|error| StoreError::Encoding(error.to_string()))
            })
            .transpose()
    }

    /// Resolve a transaction hash to its retained canonical recent frame.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid stored numbers or a database failure.
    pub async fn recent_transaction_location(
        &self,
        chain_id: ChainId,
        transaction_hash: TransactionHash,
    ) -> Result<Option<RecentTransactionLocation>, StoreError> {
        let row: Option<(i64, Vec<u8>, i64)> = sqlx::query_as(
            "SELECT block_number, block_hash, transaction_index
             FROM recent_transaction_locator
             WHERE chain_id = ? AND transaction_hash = ?",
        )
        .bind(u64_i64(chain_id.0, "chain_id")?)
        .bind(transaction_hash.0.as_slice())
        .fetch_optional(&self.inner.pool)
        .await?;
        row.map(|(block_number, block_hash, transaction_index)| {
            Ok(RecentTransactionLocation {
                block_number: BlockNumber(i64_u64(block_number, "block_number")?),
                block_hash: decode_hash(block_hash)?,
                transaction_index: u32::try_from(i64_u64(transaction_index, "transaction_index")?)
                    .map_err(|_| {
                        StoreError::Invariant("transaction_index exceeds u32".to_owned())
                    })?,
            })
        })
        .transpose()
    }

    /// Resolve a retained canonical block by hash.
    ///
    /// # Errors
    ///
    /// Returns an error for corrupt stored material or a database failure.
    pub async fn canonical_block_by_hash(
        &self,
        chain_id: ChainId,
        block_hash: BlockHash,
    ) -> Result<Option<BlockRef>, StoreError> {
        let row: Option<(i64, Vec<u8>, i64)> = sqlx::query_as(
            "SELECT block_number, parent_hash, timestamp
             FROM canonical_blocks WHERE chain_id = ? AND block_hash = ?
             LIMIT 1",
        )
        .bind(u64_i64(chain_id.0, "chain_id")?)
        .bind(block_hash.0.as_slice())
        .fetch_optional(&self.inner.pool)
        .await?;
        row.map(|(number, parent, timestamp)| {
            Ok(BlockRef {
                number: BlockNumber(i64_u64(number, "canonical block_number")?),
                hash: block_hash,
                parent_hash: decode_hash(parent)?,
                timestamp: i64_u64(timestamp, "canonical timestamp")?,
            })
        })
        .transpose()
    }

    /// Resolve retained canonical metadata and finality by block number even
    /// when the heavier recent frame has already been pruned.
    ///
    /// # Errors
    ///
    /// Returns an error for corrupt stored material or a database failure.
    pub async fn canonical_block(
        &self,
        chain_id: ChainId,
        block_number: BlockNumber,
    ) -> Result<Option<(BlockRef, Finality)>, StoreError> {
        let row: Option<(Vec<u8>, Vec<u8>, i64, i64)> = sqlx::query_as(
            "SELECT block_hash, parent_hash, timestamp, finality
             FROM canonical_blocks WHERE chain_id = ? AND block_number = ?",
        )
        .bind(u64_i64(chain_id.0, "chain_id")?)
        .bind(u64_i64(block_number.0, "block_number")?)
        .fetch_optional(&self.inner.pool)
        .await?;
        row.map(|(hash, parent_hash, timestamp, finality)| {
            Ok((
                BlockRef {
                    number: block_number,
                    hash: decode_hash(hash)?,
                    parent_hash: decode_hash(parent_hash)?,
                    timestamp: i64_u64(timestamp, "canonical timestamp")?,
                },
                decode_finality(finality)?,
            ))
        })
        .transpose()
    }

    /// Return the highest consensus-finalized canonical block retained for a
    /// chain. This chain-scoped watermark is independent of any processor's
    /// historical coverage and is used to capture immutable backfill targets.
    ///
    /// # Errors
    ///
    /// Returns an error for corrupt canonical metadata or a failed read.
    pub async fn finalized_canonical_head(
        &self,
        chain_id: ChainId,
    ) -> Result<Option<BlockRef>, StoreError> {
        let row: Option<(i64, Vec<u8>, Vec<u8>, i64)> = sqlx::query_as(
            "SELECT block_number, block_hash, parent_hash, timestamp
             FROM canonical_blocks
             WHERE chain_id = ? AND finality = ?
             ORDER BY block_number DESC
             LIMIT 1",
        )
        .bind(u64_i64(chain_id.0, "chain_id")?)
        .bind(finality_i64(Finality::Finalized))
        .fetch_optional(&self.inner.pool)
        .await?;
        row.map(|(number, hash, parent_hash, timestamp)| {
            Ok(BlockRef {
                number: BlockNumber(i64_u64(number, "finalized canonical block number")?),
                hash: decode_hash(hash)?,
                parent_hash: decode_hash(parent_hash)?,
                timestamp: i64_u64(timestamp, "finalized canonical block timestamp")?,
            })
        })
        .transpose()
    }

    /// Mark a hash-bound canonical recent prefix finalized.
    ///
    /// # Errors
    ///
    /// Rejects an unknown or contradictory anchor and database failures.
    pub async fn mark_recent_finalized(
        &self,
        chain_id: ChainId,
        through: BlockNumber,
        expected_hash: BlockHash,
    ) -> Result<u64, StoreError> {
        let _guard = self.inner.writer.lock().await;
        let stored: Option<Vec<u8>> = sqlx::query_scalar(
            "SELECT block_hash FROM canonical_blocks
             WHERE chain_id = ? AND block_number = ?",
        )
        .bind(u64_i64(chain_id.0, "chain_id")?)
        .bind(u64_i64(through.0, "finalized block_number")?)
        .fetch_optional(&self.inner.pool)
        .await?;
        if stored.as_deref() != Some(expected_hash.0.as_slice()) {
            return Err(StoreError::Invariant(format!(
                "finalized block {} is absent or contradicts retained canonical material",
                through.0
            )));
        }
        let result = sqlx::query(
            "UPDATE canonical_blocks SET finality = ?
             WHERE chain_id = ? AND block_number <= ? AND finality < ?",
        )
        .bind(finality_i64(Finality::Finalized))
        .bind(u64_i64(chain_id.0, "chain_id")?)
        .bind(u64_i64(through.0, "finalized block_number")?)
        .bind(finality_i64(Finality::Finalized))
        .execute(&self.inner.pool)
        .await?;
        Ok(result.rows_affected())
    }

    /// Inspect retained recent-frame rows for one chain.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid stored numbers or a database failure.
    pub async fn recent_stats(&self, chain_id: ChainId) -> Result<RecentStoreStats, StoreError> {
        let row = sqlx::query(
            "SELECT COUNT(*) AS frames, COALESCE(SUM(bytes), 0) AS encoded_bytes,
                    MIN(block_number) AS earliest_block, MAX(block_number) AS latest_block
             FROM recent_blocks WHERE chain_id = ?",
        )
        .bind(u64_i64(chain_id.0, "chain_id")?)
        .fetch_one(&self.inner.pool)
        .await?;
        Ok(RecentStoreStats {
            frames: i64_u64(row.try_get("frames")?, "recent frame count")?,
            encoded_bytes: i64_u64(row.try_get("encoded_bytes")?, "recent encoded bytes")?,
            earliest_block: row
                .try_get::<Option<i64>, _>("earliest_block")?
                .map(|value| i64_u64(value, "earliest recent block").map(BlockNumber))
                .transpose()?,
            latest_block: row
                .try_get::<Option<i64>, _>("latest_block")?
                .map(|value| i64_u64(value, "latest recent block").map(BlockNumber))
                .transpose()?,
        })
    }

    /// Return the inclusive block range that has both a canonical pointer and
    /// a retained recent frame.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid stored numbers or a database failure.
    pub async fn recent_canonical_bounds(
        &self,
        chain_id: ChainId,
    ) -> Result<Option<BlockRange>, StoreError> {
        let row = sqlx::query(
            "SELECT MIN(canonical.block_number) AS earliest_block,
                    MAX(canonical.block_number) AS latest_block
             FROM canonical_blocks AS canonical
             JOIN recent_blocks AS recent
               ON recent.chain_id = canonical.chain_id
              AND recent.block_number = canonical.block_number
              AND recent.block_hash = canonical.block_hash
             WHERE canonical.chain_id = ?",
        )
        .bind(u64_i64(chain_id.0, "chain_id")?)
        .fetch_one(&self.inner.pool)
        .await?;
        let earliest = row.try_get::<Option<i64>, _>("earliest_block")?;
        let latest = row.try_get::<Option<i64>, _>("latest_block")?;
        match (earliest, latest) {
            (Some(earliest), Some(latest)) => BlockRange::new(
                BlockNumber(i64_u64(earliest, "earliest canonical recent block")?),
                BlockNumber(i64_u64(latest, "latest canonical recent block")?),
            )
            .map(Some)
            .map_err(|error| StoreError::Invariant(error.to_string())),
            (None, None) => Ok(None),
            _ => Err(StoreError::Invariant(
                "canonical recent bounds are partially null".to_owned(),
            )),
        }
    }

    /// Prune finalized old frames down toward a soft byte limit without
    /// crossing the configured minimum recent-block safety window.
    ///
    /// The outcome reports a hard-limit conflict instead of deleting
    /// unfinalized or safety-window material.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid limits, numeric overflow, or a database
    /// failure.
    pub async fn prune_recent_frames(
        &self,
        chain_id: ChainId,
        finalized_through: BlockNumber,
        minimum_recent_blocks: u64,
        soft_bytes: u64,
        hard_bytes: u64,
    ) -> Result<RecentPruneOutcome, StoreError> {
        if minimum_recent_blocks == 0 || soft_bytes == 0 || hard_bytes < soft_bytes {
            return Err(StoreError::InvalidConfig(
                "recent pruning requires a non-zero window/soft limit and hard >= soft".to_owned(),
            ));
        }
        let _guard = self.inner.writer.lock().await;
        let rows = sqlx::query(
            "SELECT block_number, block_hash, bytes
             FROM recent_blocks WHERE chain_id = ?
             ORDER BY block_number, block_hash",
        )
        .bind(u64_i64(chain_id.0, "chain_id")?)
        .fetch_all(&self.inner.pool)
        .await?;
        let latest = rows
            .last()
            .map(|row| i64_u64(row.get("block_number"), "latest recent block"))
            .transpose()?
            .unwrap_or(0);
        let retain_from = latest.saturating_sub(minimum_recent_blocks.saturating_sub(1));
        let mut retained_bytes = rows.iter().try_fold(0_u64, |total, row| {
            let bytes = i64_u64(row.get("bytes"), "recent frame bytes")?;
            Ok::<_, StoreError>(total.saturating_add(bytes))
        })?;
        let mut deleted_frames = 0_u64;
        let mut deleted_bytes = 0_u64;
        let mut transaction = self.inner.pool.begin().await?;
        for row in &rows {
            if retained_bytes <= soft_bytes {
                break;
            }
            let block = i64_u64(row.get("block_number"), "recent block number")?;
            if block >= retain_from || block > finalized_through.0 {
                continue;
            }
            let hash: Vec<u8> = row.get("block_hash");
            let bytes = i64_u64(row.get("bytes"), "recent frame bytes")?;
            let block_ref = BlockRef {
                number: BlockNumber(block),
                hash: decode_hash(hash.clone())?,
                parent_hash: BlockHash::ZERO,
                timestamp: 0,
            };
            delete_recent_transaction_locators(&mut transaction, chain_id, block_ref).await?;
            sqlx::query(
                "DELETE FROM recent_blocks
                 WHERE chain_id = ? AND block_number = ? AND block_hash = ?",
            )
            .bind(u64_i64(chain_id.0, "chain_id")?)
            .bind(u64_i64(block, "block_number")?)
            .bind(hash)
            .execute(&mut *transaction)
            .await?;
            retained_bytes = retained_bytes.saturating_sub(bytes);
            deleted_bytes = deleted_bytes.saturating_add(bytes);
            deleted_frames = deleted_frames.saturating_add(1);
        }
        transaction.commit().await?;
        Ok(RecentPruneOutcome {
            deleted_frames,
            deleted_bytes,
            retained_frames: u64::try_from(rows.len())
                .unwrap_or(u64::MAX)
                .saturating_sub(deleted_frames),
            retained_bytes,
            hard_limit_exceeded: retained_bytes > hard_bytes,
        })
    }

    /// Merge exact covered blocks into deterministic inclusive ranges.
    ///
    /// # Errors
    ///
    /// Returns an error when range bounds exceed `SQLite`'s numeric range or the
    /// database read fails.
    pub async fn coverage(
        &self,
        descriptor: &ProcessorDescriptor,
        requested: BlockRange,
    ) -> Result<Vec<BlockRange>, StoreError> {
        let rows: Vec<i64> = sqlx::query_scalar(
            "SELECT block_number FROM processor_coverage
             WHERE instance = ? AND block_number BETWEEN ? AND ?
             ORDER BY block_number",
        )
        .bind(processor_instance(descriptor))
        .bind(u64_i64(requested.start().0, "block_number")?)
        .bind(u64_i64(requested.end().0, "block_number")?)
        .fetch_all(&self.inner.pool)
        .await?;
        let mut coverage = merge_block_numbers(
            rows.into_iter()
                .map(|number| i64_u64(number, "block_number"))
                .collect::<Result<Vec<_>, _>>()?,
        )?;
        let compact: Vec<(i64, i64)> = sqlx::query_as(
            "SELECT start_block, end_block FROM finalized_coverage_intervals
             WHERE instance = ? AND end_block >= ? AND start_block <= ?
             ORDER BY start_block",
        )
        .bind(processor_instance(descriptor))
        .bind(u64_i64(requested.start().0, "block_number")?)
        .bind(u64_i64(requested.end().0, "block_number")?)
        .fetch_all(&self.inner.pool)
        .await?;
        for (start, end) in compact {
            coverage.push(
                BlockRange::new(
                    BlockNumber(i64_u64(start, "compact coverage start")?).max(requested.start()),
                    BlockNumber(i64_u64(end, "compact coverage end")?).min(requested.end()),
                )
                .map_err(|error| StoreError::Invariant(error.to_string()))?,
            );
        }
        normalize_ranges(coverage)
    }

    /// Merge exact finalized processor coverage into deterministic ranges.
    ///
    /// # Errors
    ///
    /// Returns an error when range bounds exceed `SQLite`'s numeric range or
    /// the database read fails.
    pub async fn finalized_coverage(
        &self,
        descriptor: &ProcessorDescriptor,
        requested: BlockRange,
    ) -> Result<Vec<BlockRange>, StoreError> {
        let rows: Vec<i64> = sqlx::query_scalar(
            "SELECT block_number FROM processor_coverage
             WHERE instance = ? AND finality = ? AND block_number BETWEEN ? AND ?
             ORDER BY block_number",
        )
        .bind(processor_instance(descriptor))
        .bind(finality_i64(Finality::Finalized))
        .bind(u64_i64(requested.start().0, "block_number")?)
        .bind(u64_i64(requested.end().0, "block_number")?)
        .fetch_all(&self.inner.pool)
        .await?;
        let mut coverage = merge_block_numbers(
            rows.into_iter()
                .map(|number| i64_u64(number, "block_number"))
                .collect::<Result<Vec<_>, _>>()?,
        )?;
        let compact: Vec<(i64, i64)> = sqlx::query_as(
            "SELECT start_block, end_block FROM finalized_coverage_intervals
             WHERE instance = ? AND end_block >= ? AND start_block <= ?
             ORDER BY start_block",
        )
        .bind(processor_instance(descriptor))
        .bind(u64_i64(requested.start().0, "block_number")?)
        .bind(u64_i64(requested.end().0, "block_number")?)
        .fetch_all(&self.inner.pool)
        .await?;
        for (start, end) in compact {
            coverage.push(
                BlockRange::new(
                    BlockNumber(i64_u64(start, "compact coverage start")?).max(requested.start()),
                    BlockNumber(i64_u64(end, "compact coverage end")?).min(requested.end()),
                )
                .map_err(|error| StoreError::Invariant(error.to_string()))?,
            );
        }
        normalize_ranges(coverage)
    }

    /// Return every compact finalized proof segment intersecting a range.
    ///
    /// Returned segments retain their full persisted bounds because verifying
    /// any interior block requires reacquiring and checking that whole bounded
    /// parent chain.
    ///
    /// # Errors
    ///
    /// Returns an error for corrupt hashes, metadata digests, numeric values,
    /// or a failed query.
    pub async fn finalized_coverage_segments(
        &self,
        descriptor: &ProcessorDescriptor,
        requested: BlockRange,
    ) -> Result<Vec<FinalizedCoverageSegment>, StoreError> {
        let rows = sqlx::query(
            "SELECT segment.interval_start, segment.segment_start,
                    segment.segment_end, segment.start_parent_hash,
                    segment.end_hash, segment.metadata_digest,
                    interval.segment_size, interval.encoding_version
             FROM finalized_coverage_segments AS segment
             JOIN finalized_coverage_intervals AS interval
               ON interval.instance = segment.instance
              AND interval.start_block = segment.interval_start
             WHERE segment.instance = ?
               AND segment.segment_end >= ?
               AND segment.segment_start <= ?
             ORDER BY segment.segment_start",
        )
        .bind(processor_instance(descriptor))
        .bind(u64_i64(requested.start().0, "block_number")?)
        .bind(u64_i64(requested.end().0, "block_number")?)
        .fetch_all(&self.inner.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                let interval_start = BlockNumber(i64_u64(
                    row.try_get("interval_start")?,
                    "compact interval start",
                )?);
                let start = BlockNumber(i64_u64(
                    row.try_get("segment_start")?,
                    "compact segment start",
                )?);
                let end = BlockNumber(i64_u64(row.try_get("segment_end")?, "compact segment end")?);
                let start_parent_hash = decode_hash(row.try_get("start_parent_hash")?)?;
                let end_hash = decode_hash(row.try_get("end_hash")?)?;
                let digest = decode_hash(row.try_get("metadata_digest")?)?;
                let expected = compact_segment_digest(start, end, start_parent_hash, end_hash);
                if digest != expected {
                    return Err(StoreError::Invariant(format!(
                        "compact coverage segment {}..={} digest mismatch",
                        start.0, end.0
                    )));
                }
                let segment_size = i64_u64(row.try_get("segment_size")?, "segment size")?;
                let encoding_version = u16::try_from(i64_u64(
                    row.try_get("encoding_version")?,
                    "coverage encoding version",
                )?)
                .map_err(|_| StoreError::Numeric("coverage encoding version"))?;
                Ok(FinalizedCoverageSegment {
                    range: BlockRange::new(start, end)
                        .map_err(|error| StoreError::Invariant(error.to_string()))?,
                    start_parent_hash,
                    end_hash,
                    interval_start,
                    segment_size,
                    encoding_version,
                })
            })
            .collect()
    }

    /// Count exact finalized coverage that is eligible for interval compaction.
    ///
    /// Maintenance workers use this to avoid producing tiny intervals while a
    /// fast historical job is still appending microbatches. A bounded tail may
    /// remain exact until enough adjacent work accumulates or a terminal flush
    /// explicitly compacts it.
    ///
    /// # Errors
    ///
    /// Returns an error when the bound exceeds `SQLite`'s numeric range or the
    /// database read fails.
    pub async fn compactable_finalized_coverage_blocks(
        &self,
        descriptor: &ProcessorDescriptor,
        through: BlockNumber,
    ) -> Result<u64, StoreError> {
        let instance = processor_instance(descriptor);
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM processor_coverage AS coverage
             WHERE coverage.instance = ?
               AND coverage.finality = ?
               AND coverage.block_number <= ?
               AND EXISTS (
                   SELECT 1 FROM finalized_coverage_owners AS owner
                    WHERE owner.instance = coverage.instance
                      AND coverage.block_number BETWEEN owner.from_block AND owner.to_block
               )
               AND NOT EXISTS (
                   SELECT 1 FROM finalized_coverage_intervals AS compact
                    WHERE compact.instance = coverage.instance
                      AND coverage.block_number BETWEEN compact.start_block AND compact.end_block
               )",
        )
        .bind(&instance)
        .bind(finality_i64(Finality::Finalized))
        .bind(u64_i64(through.0, "coverage compaction through")?)
        .fetch_one(&self.inner.pool)
        .await?;
        i64_u64(count, "compactable finalized coverage count")
    }

    /// Compact one contiguous bounded run of finalized block-local coverage.
    ///
    /// Exact coverage, applied-idempotency, and finalized undo rows are
    /// replaced atomically by interval metadata and bounded parent-chain proof
    /// segments. Retained node-owned entities are intentionally untouched; the
    /// compact interval replaces only redundant finalized execution metadata.
    /// Compaction pauses while any subscription still owns creation-time work,
    /// because overlapping jobs may need exact rows for stream-safe replay.
    ///
    /// # Errors
    ///
    /// Returns an error for an incompatible processor, invalid limits, legacy
    /// coverage without parent anchors, corrupt continuity, or a failed
    /// transaction.
    #[allow(clippy::too_many_lines)]
    pub async fn compact_finalized_coverage(
        &self,
        descriptor: &ProcessorDescriptor,
        through: BlockNumber,
        segment_size: u64,
        maximum_blocks: u64,
    ) -> Result<FinalizedCoverageCompaction, StoreError> {
        if descriptor.mode != ReductionMode::BlockLocal {
            return Err(StoreError::InvalidConfig(
                "finalized coverage compaction requires a block-local processor".to_owned(),
            ));
        }
        if segment_size == 0 || maximum_blocks == 0 {
            return Err(StoreError::InvalidConfig(
                "coverage segment and compaction batch sizes must be greater than zero".to_owned(),
            ));
        }
        let limit = usize::try_from(maximum_blocks.min(100_000))
            .map_err(|_| StoreError::Numeric("coverage compaction block limit"))?;
        let instance = processor_instance(descriptor);
        let _guard = self.inner.writer.lock_history().await;
        let active_subscriptions: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM backfill_subscriptions
             WHERE instance = ?
               AND state IN ('waiting_for_consumer', 'queued', 'running', 'backpressured')",
        )
        .bind(&instance)
        .fetch_one(&self.inner.pool)
        .await?;
        if active_subscriptions != 0 {
            return Ok(FinalizedCoverageCompaction::default());
        }
        let rows = sqlx::query(
            "SELECT coverage.block_number, coverage.block_hash, coverage.parent_hash
             FROM processor_coverage AS coverage
             WHERE coverage.instance = ?
               AND coverage.finality = ?
               AND coverage.block_number <= ?
               AND EXISTS (
                   SELECT 1 FROM finalized_coverage_owners AS owner
                    WHERE owner.instance = coverage.instance
                      AND coverage.block_number BETWEEN owner.from_block AND owner.to_block
               )
               AND NOT EXISTS (
                   SELECT 1 FROM finalized_coverage_intervals AS compact
                    WHERE compact.instance = coverage.instance
                      AND coverage.block_number BETWEEN compact.start_block AND compact.end_block
               )
             ORDER BY coverage.block_number
             LIMIT ?",
        )
        .bind(&instance)
        .bind(finality_i64(Finality::Finalized))
        .bind(u64_i64(through.0, "coverage compaction through")?)
        .bind(usize_i64(limit, "coverage compaction block limit")?)
        .fetch_all(&self.inner.pool)
        .await?;
        if rows.is_empty() {
            return Ok(FinalizedCoverageCompaction::default());
        }

        let mut exact: Vec<(BlockNumber, BlockHash, BlockHash)> = Vec::with_capacity(rows.len());
        for row in rows {
            let number = BlockNumber(i64_u64(
                row.try_get("block_number")?,
                "compact exact block number",
            )?);
            let hash = decode_hash(row.try_get("block_hash")?)?;
            let parent = row
                .try_get::<Option<Vec<u8>>, _>("parent_hash")?
                .map(decode_hash)
                .transpose()?
                .ok_or_else(|| {
                    StoreError::InvalidConfig(format!(
                        "coverage block {} predates retained parent anchors",
                        number.0
                    ))
                })?;
            if let Some((prior_number, prior_hash, _)) = exact.last().copied() {
                if number.0 != prior_number.0.saturating_add(1) {
                    break;
                }
                if parent != prior_hash {
                    return Err(StoreError::Invariant(format!(
                        "finalized coverage parent mismatch at block {}",
                        number.0
                    )));
                }
            }
            exact.push((number, hash, parent));
        }
        let first = exact.first().copied().ok_or_else(|| {
            StoreError::Invariant("coverage compaction lost its non-empty range".to_owned())
        })?;
        let last = exact.last().copied().ok_or_else(|| {
            StoreError::Invariant("coverage compaction lost its non-empty range".to_owned())
        })?;
        let compacted_range = BlockRange::new(first.0, last.0)
            .map_err(|error| StoreError::Invariant(error.to_string()))?;
        let chunk_size = usize::try_from(segment_size.min(maximum_blocks))
            .map_err(|_| StoreError::Numeric("coverage segment size"))?
            .max(1);
        let mut segments = Vec::new();
        let mut interval_hasher = blake3::Hasher::new();
        interval_hasher.update(&1_u16.to_be_bytes());
        interval_hasher.update(&first.0.0.to_be_bytes());
        interval_hasher.update(&last.0.0.to_be_bytes());
        for chunk in exact.chunks(chunk_size) {
            let start = chunk.first().copied().ok_or_else(|| {
                StoreError::Invariant("coverage compaction produced an empty segment".to_owned())
            })?;
            let end = chunk.last().copied().ok_or_else(|| {
                StoreError::Invariant("coverage compaction produced an empty segment".to_owned())
            })?;
            let digest = compact_segment_digest(start.0, end.0, start.2, end.1);
            interval_hasher.update(digest.0.as_slice());
            segments.push((start.0, end.0, start.2, end.1, digest));
        }
        let interval_digest = BlockHash::new(*interval_hasher.finalize().as_bytes());

        let mut transaction = self.inner.pool.begin().await?;
        sqlx::query(
            "INSERT INTO finalized_coverage_intervals(
                instance, start_block, end_block, segment_size,
                encoding_version, metadata_digest, created_at_unix_ms
             ) VALUES (?, ?, ?, ?, 1, ?, ?)",
        )
        .bind(&instance)
        .bind(u64_i64(first.0.0, "compact interval start")?)
        .bind(u64_i64(last.0.0, "compact interval end")?)
        .bind(u64_i64(segment_size, "coverage segment size")?)
        .bind(interval_digest.0.as_slice())
        .bind(now_i64()?)
        .execute(&mut *transaction)
        .await?;
        for (start, end, parent, hash, digest) in &segments {
            sqlx::query(
                "INSERT INTO finalized_coverage_segments(
                    instance, interval_start, segment_start, segment_end,
                    start_parent_hash, end_hash, metadata_digest
                 ) VALUES (?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(&instance)
            .bind(u64_i64(first.0.0, "compact interval start")?)
            .bind(u64_i64(start.0, "compact segment start")?)
            .bind(u64_i64(end.0, "compact segment end")?)
            .bind(parent.0.as_slice())
            .bind(hash.0.as_slice())
            .bind(digest.0.as_slice())
            .execute(&mut *transaction)
            .await?;
        }
        let exact_coverage_deleted = sqlx::query(
            "DELETE FROM processor_coverage
             WHERE instance = ? AND block_number BETWEEN ? AND ? AND finality = ?",
        )
        .bind(&instance)
        .bind(u64_i64(first.0.0, "compact interval start")?)
        .bind(u64_i64(last.0.0, "compact interval end")?)
        .bind(finality_i64(Finality::Finalized))
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        let applied_blocks_deleted = sqlx::query(
            "DELETE FROM applied_blocks
             WHERE instance = ? AND block_number BETWEEN ? AND ?",
        )
        .bind(&instance)
        .bind(u64_i64(first.0.0, "compact interval start")?)
        .bind(u64_i64(last.0.0, "compact interval end")?)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        let finalized_undo_deleted = sqlx::query(
            "DELETE FROM undo_journal
             WHERE instance = ? AND block_number BETWEEN ? AND ? AND finalized = 1",
        )
        .bind(&instance)
        .bind(u64_i64(first.0.0, "compact interval start")?)
        .bind(u64_i64(last.0.0, "compact interval end")?)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        transaction.commit().await?;
        Ok(FinalizedCoverageCompaction {
            intervals_created: 1,
            segments_created: u64::try_from(segments.len())
                .map_err(|_| StoreError::Numeric("coverage segment count"))?,
            exact_coverage_deleted,
            applied_blocks_deleted,
            finalized_undo_deleted,
            compacted_range: Some(compacted_range),
        })
    }

    /// Return exact retained canonical coverage for a chain range.
    ///
    /// # Errors
    ///
    /// Returns an error when bounds exceed `SQLite`'s numeric range or the
    /// database read fails.
    pub async fn canonical_coverage(
        &self,
        chain_id: ChainId,
        requested: BlockRange,
    ) -> Result<Vec<BlockRange>, StoreError> {
        let rows: Vec<i64> = sqlx::query_scalar(
            "SELECT block_number FROM canonical_blocks
             WHERE chain_id = ? AND block_number BETWEEN ? AND ?
             ORDER BY block_number",
        )
        .bind(u64_i64(chain_id.0, "chain_id")?)
        .bind(u64_i64(requested.start().0, "block_number")?)
        .bind(u64_i64(requested.end().0, "block_number")?)
        .fetch_all(&self.inner.pool)
        .await?;
        merge_block_numbers(
            rows.into_iter()
                .map(|number| i64_u64(number, "block_number"))
                .collect::<Result<Vec<_>, _>>()?,
        )
    }

    /// Persist the start of one exact historical/live reconciliation.
    ///
    /// Reopening the same identity and bounds is idempotent. Reusing an
    /// identity for another processor, range, chain, or anchor is rejected.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid identity, processor drift, conflicting
    /// durable handoff metadata, numeric overflow, or a database failure.
    pub async fn begin_hot_cold_handoff(
        &self,
        id: &str,
        descriptor: &ProcessorDescriptor,
        chain_id: ChainId,
        overlap: BlockRange,
        anchor_hash: BlockHash,
    ) -> Result<HotColdHandoffRecord, StoreError> {
        if id.is_empty() || overlap.end().0 < overlap.start().0 {
            return Err(StoreError::InvalidConfig(
                "hot/cold handoff identity and overlap must be valid".to_owned(),
            ));
        }
        let instance = self.register_processor(descriptor).await?;
        let _guard = self.inner.writer.lock().await;
        if let Some(existing) = load_handoff(&self.inner.pool, id, descriptor).await? {
            validate_handoff_identity(&existing, chain_id, overlap, anchor_hash)?;
            return Ok(existing);
        }
        let updated_at_unix_ms = now_milliseconds()?;
        sqlx::query(
            "INSERT INTO hot_cold_handoffs(
                handoff_id, chain_id, processor_instance, overlap_from,
                overlap_to, anchor_hash, state, compared_blocks, failure,
                updated_at_unix_ms
             ) VALUES (?, ?, ?, ?, ?, ?, 'running', 0, NULL, ?)",
        )
        .bind(id)
        .bind(u64_i64(chain_id.0, "chain_id")?)
        .bind(&instance)
        .bind(u64_i64(overlap.start().0, "overlap_from")?)
        .bind(u64_i64(overlap.end().0, "overlap_to")?)
        .bind(anchor_hash.0.as_slice())
        .bind(u64_i64(updated_at_unix_ms, "updated_at_unix_ms")?)
        .execute(&self.inner.pool)
        .await?;
        Ok(HotColdHandoffRecord {
            id: id.to_owned(),
            chain_id,
            processor_id: descriptor.id.to_string(),
            processor_version: descriptor.version.to_string(),
            overlap,
            anchor_hash,
            state: HotColdHandoffState::Running,
            compared_blocks: 0,
            failure: None,
            updated_at_unix_ms,
        })
    }

    /// Compare every historical processor-coverage hash with the retained
    /// canonical live hash over the declared overlap and persist the verdict.
    ///
    /// # Errors
    ///
    /// Fails closed when either side is incomplete, any hash differs, the
    /// anchor is not the overlap tip, or durable handoff identity changed.
    #[allow(clippy::too_many_lines)]
    pub async fn verify_hot_cold_handoff(
        &self,
        id: &str,
        descriptor: &ProcessorDescriptor,
        chain_id: ChainId,
        overlap: BlockRange,
        anchor_hash: BlockHash,
    ) -> Result<HotColdHandoffRecord, StoreError> {
        let existing = self
            .begin_hot_cold_handoff(id, descriptor, chain_id, overlap, anchor_hash)
            .await?;
        if existing.state == HotColdHandoffState::Verified {
            return Ok(existing);
        }
        let instance = processor_instance(descriptor);
        let _guard = self.inner.writer.lock().await;
        let rows = sqlx::query(
            "SELECT canonical.block_number AS block_number,
                    canonical.block_hash AS canonical_hash,
                    coverage.block_hash AS coverage_hash
             FROM canonical_blocks AS canonical
             LEFT JOIN processor_coverage AS coverage
               ON coverage.instance = ?
              AND coverage.block_number = canonical.block_number
             WHERE canonical.chain_id = ?
               AND canonical.block_number BETWEEN ? AND ?
             ORDER BY canonical.block_number",
        )
        .bind(&instance)
        .bind(u64_i64(chain_id.0, "chain_id")?)
        .bind(u64_i64(overlap.start().0, "overlap_from")?)
        .bind(u64_i64(overlap.end().0, "overlap_to")?)
        .fetch_all(&self.inner.pool)
        .await?;

        let mut expected = overlap.start().0;
        let mut compared_blocks = 0_u64;
        let mut failure = None;
        for row in rows {
            let number = i64_u64(row.try_get("block_number")?, "block_number")?;
            if number != expected {
                failure = Some(format!(
                    "live canonical overlap is missing block {expected}"
                ));
                break;
            }
            let canonical_hash = decode_hash(row.try_get("canonical_hash")?)?;
            let coverage_hash = row
                .try_get::<Option<Vec<u8>>, _>("coverage_hash")?
                .map(decode_hash)
                .transpose()?;
            match coverage_hash {
                Some(coverage_hash) if coverage_hash == canonical_hash => {
                    compared_blocks = compared_blocks.saturating_add(1);
                    expected = expected.saturating_add(1);
                }
                Some(coverage_hash) => {
                    failure = Some(format!(
                        "block {number} differs: live {canonical_hash:?}, historical {coverage_hash:?}"
                    ));
                    break;
                }
                None => {
                    failure = Some(format!(
                        "historical processor coverage is missing block {number}"
                    ));
                    break;
                }
            }
        }
        if failure.is_none() && compared_blocks != overlap.len() {
            failure = Some(format!(
                "live canonical overlap ended at {}; expected {}",
                expected.saturating_sub(1),
                overlap.end().0
            ));
        }
        if failure.is_none() {
            let canonical_anchor: Option<Vec<u8>> = sqlx::query_scalar(
                "SELECT block_hash FROM canonical_blocks
                 WHERE chain_id = ? AND block_number = ?",
            )
            .bind(u64_i64(chain_id.0, "chain_id")?)
            .bind(u64_i64(overlap.end().0, "overlap_to")?)
            .fetch_optional(&self.inner.pool)
            .await?;
            if canonical_anchor
                .map(decode_hash)
                .transpose()?
                .is_none_or(|hash| hash != anchor_hash)
            {
                failure = Some("overlap tip does not match the verified anchor".to_owned());
            }
        }

        let state = if failure.is_some() {
            HotColdHandoffState::Failed
        } else {
            HotColdHandoffState::Verified
        };
        let updated_at_unix_ms = now_milliseconds()?;
        sqlx::query(
            "UPDATE hot_cold_handoffs
             SET state = ?, compared_blocks = ?, failure = ?, updated_at_unix_ms = ?
             WHERE handoff_id = ?",
        )
        .bind(state.as_str())
        .bind(u64_i64(compared_blocks, "compared_blocks")?)
        .bind(&failure)
        .bind(u64_i64(updated_at_unix_ms, "updated_at_unix_ms")?)
        .bind(id)
        .execute(&self.inner.pool)
        .await?;
        let record = HotColdHandoffRecord {
            id: id.to_owned(),
            chain_id,
            processor_id: descriptor.id.to_string(),
            processor_version: descriptor.version.to_string(),
            overlap,
            anchor_hash,
            state,
            compared_blocks,
            failure: failure.clone(),
            updated_at_unix_ms,
        };
        if let Some(detail) = failure {
            return Err(StoreError::HandoffMismatch {
                id: id.to_owned(),
                detail,
            });
        }
        Ok(record)
    }

    /// Read a durable hot/cold handoff verdict.
    ///
    /// # Errors
    ///
    /// Returns an error for corrupt metadata or a database failure.
    pub async fn hot_cold_handoff(
        &self,
        id: &str,
        descriptor: &ProcessorDescriptor,
    ) -> Result<Option<HotColdHandoffRecord>, StoreError> {
        load_handoff(&self.inner.pool, id, descriptor).await
    }

    /// Read the newest durable handoff for one processor instance.
    ///
    /// # Errors
    ///
    /// Returns an error for corrupt metadata or a database failure.
    pub async fn latest_hot_cold_handoff(
        &self,
        descriptor: &ProcessorDescriptor,
    ) -> Result<Option<HotColdHandoffRecord>, StoreError> {
        let id: Option<String> = sqlx::query_scalar(
            "SELECT handoff_id FROM hot_cold_handoffs
             WHERE processor_instance = ?
             ORDER BY overlap_to DESC, updated_at_unix_ms DESC
             LIMIT 1",
        )
        .bind(processor_instance(descriptor))
        .fetch_optional(&self.inner.pool)
        .await?;
        match id {
            Some(id) => load_handoff(&self.inner.pool, &id, descriptor).await,
            None => Ok(None),
        }
    }

    /// Persist the start of an archive/live delta reconciliation.
    ///
    /// Reusing an identity with different immutable inputs is rejected.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid input, processor drift, identity reuse,
    /// numeric overflow, or a database failure.
    #[allow(clippy::too_many_arguments)]
    pub async fn begin_archive_reconciliation(
        &self,
        id: &str,
        descriptor: &ProcessorDescriptor,
        source_id: &str,
        chain_id: ChainId,
        overlap: BlockRange,
        anchor_hash: BlockHash,
    ) -> Result<ArchiveReconciliationRecord, StoreError> {
        if id.is_empty() || source_id.is_empty() {
            return Err(StoreError::InvalidConfig(
                "archive reconciliation and source identities must not be empty".to_owned(),
            ));
        }
        let instance = self.register_processor(descriptor).await?;
        let _guard = self.inner.writer.lock().await;
        if let Some(existing) =
            load_archive_reconciliation(&self.inner.pool, id, descriptor).await?
        {
            validate_archive_reconciliation_identity(
                &existing,
                source_id,
                chain_id,
                overlap,
                anchor_hash,
            )?;
            return Ok(existing);
        }
        let updated_at_unix_ms = now_milliseconds()?;
        sqlx::query(
            "INSERT INTO archive_reconciliations(
                reconciliation_id, chain_id, processor_instance, source_id,
                overlap_from, overlap_to, anchor_hash, state, compared_blocks,
                failure, updated_at_unix_ms
             ) VALUES (?, ?, ?, ?, ?, ?, ?, 'running', 0, NULL, ?)",
        )
        .bind(id)
        .bind(u64_i64(chain_id.0, "chain_id")?)
        .bind(&instance)
        .bind(source_id)
        .bind(u64_i64(overlap.start().0, "overlap_from")?)
        .bind(u64_i64(overlap.end().0, "overlap_to")?)
        .bind(anchor_hash.0.as_slice())
        .bind(u64_i64(updated_at_unix_ms, "updated_at_unix_ms")?)
        .execute(&self.inner.pool)
        .await?;
        Ok(ArchiveReconciliationRecord {
            id: id.to_owned(),
            chain_id,
            processor_id: descriptor.id.to_string(),
            processor_version: descriptor.version.to_string(),
            source_id: source_id.to_owned(),
            overlap,
            anchor_hash,
            state: ArchiveReconciliationState::Running,
            compared_blocks: 0,
            failure: None,
            updated_at_unix_ms,
        })
    }

    /// Persist a successful or failed archive/live delta comparison.
    ///
    /// A failure is recorded before the method returns a fail-closed error.
    ///
    /// # Errors
    ///
    /// Returns an error for identity drift, a recorded mismatch, numeric
    /// overflow, or a database failure.
    #[allow(clippy::too_many_arguments)]
    pub async fn finish_archive_reconciliation(
        &self,
        id: &str,
        descriptor: &ProcessorDescriptor,
        source_id: &str,
        chain_id: ChainId,
        overlap: BlockRange,
        anchor_hash: BlockHash,
        compared_blocks: u64,
        failure: Option<String>,
    ) -> Result<ArchiveReconciliationRecord, StoreError> {
        let existing = self
            .begin_archive_reconciliation(id, descriptor, source_id, chain_id, overlap, anchor_hash)
            .await?;
        if existing.state == ArchiveReconciliationState::Verified {
            return Ok(existing);
        }
        if existing.state == ArchiveReconciliationState::Failed {
            return Err(StoreError::ArchiveReconciliationMismatch {
                id: id.to_owned(),
                detail: existing
                    .failure
                    .unwrap_or_else(|| "previous comparison failed".to_owned()),
            });
        }
        let state = if failure.is_some() {
            ArchiveReconciliationState::Failed
        } else {
            ArchiveReconciliationState::Verified
        };
        let updated_at_unix_ms = now_milliseconds()?;
        let _guard = self.inner.writer.lock().await;
        sqlx::query(
            "UPDATE archive_reconciliations
             SET state = ?, compared_blocks = ?, failure = ?, updated_at_unix_ms = ?
             WHERE reconciliation_id = ?",
        )
        .bind(state.as_str())
        .bind(u64_i64(compared_blocks, "compared_blocks")?)
        .bind(&failure)
        .bind(u64_i64(updated_at_unix_ms, "updated_at_unix_ms")?)
        .bind(id)
        .execute(&self.inner.pool)
        .await?;
        let record = ArchiveReconciliationRecord {
            id: id.to_owned(),
            chain_id,
            processor_id: descriptor.id.to_string(),
            processor_version: descriptor.version.to_string(),
            source_id: source_id.to_owned(),
            overlap,
            anchor_hash,
            state,
            compared_blocks,
            failure: failure.clone(),
            updated_at_unix_ms,
        };
        if let Some(detail) = failure {
            return Err(StoreError::ArchiveReconciliationMismatch {
                id: id.to_owned(),
                detail,
            });
        }
        Ok(record)
    }

    /// Read the newest verified archive reconciliation for one processor.
    ///
    /// # Errors
    ///
    /// Returns an error for corrupt durable metadata or a database failure.
    pub async fn latest_verified_archive_reconciliation(
        &self,
        descriptor: &ProcessorDescriptor,
    ) -> Result<Option<ArchiveReconciliationRecord>, StoreError> {
        let id: Option<String> = sqlx::query_scalar(
            "SELECT reconciliation_id FROM archive_reconciliations
             WHERE processor_instance = ? AND state = 'verified'
             ORDER BY overlap_to DESC, updated_at_unix_ms DESC
             LIMIT 1",
        )
        .bind(processor_instance(descriptor))
        .fetch_optional(&self.inner.pool)
        .await?;
        match id {
            Some(id) => load_archive_reconciliation(&self.inner.pool, &id, descriptor).await,
            None => Ok(None),
        }
    }

    /// Read one durable archive reconciliation verdict.
    ///
    /// # Errors
    ///
    /// Returns an error for corrupt durable metadata or a database failure.
    pub async fn archive_reconciliation(
        &self,
        id: &str,
        descriptor: &ProcessorDescriptor,
    ) -> Result<Option<ArchiveReconciliationRecord>, StoreError> {
        load_archive_reconciliation(&self.inner.pool, id, descriptor).await
    }

    /// Insert or update one scheduler job atomically.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid job identity, numeric overflow, or a
    /// database failure.
    pub async fn save_job(&self, job: &JobRecord) -> Result<(), StoreError> {
        if job.id.is_empty() || job.kind.is_empty() {
            return Err(StoreError::InvalidConfig(
                "job id and kind must not be empty".to_owned(),
            ));
        }
        let _guard = self.inner.writer.lock().await;
        sqlx::query(
            "INSERT INTO jobs(
                job_id, kind, state, payload, checkpoint, attempts, updated_at_unix_ms
             ) VALUES (?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(job_id) DO UPDATE SET
               kind = excluded.kind,
               state = CASE
                 WHEN jobs.state = 'cancelled'
                 THEN jobs.state
                 WHEN jobs.state IN ('completed', 'failed')
                   AND excluded.state IN ('queued', 'running')
                 THEN jobs.state
                 ELSE excluded.state
               END,
               payload = excluded.payload,
               checkpoint = excluded.checkpoint,
               attempts = excluded.attempts,
               updated_at_unix_ms = excluded.updated_at_unix_ms",
        )
        .bind(&job.id)
        .bind(&job.kind)
        .bind(job.state.as_str())
        .bind(&job.payload)
        .bind(&job.checkpoint)
        .bind(i64::from(job.attempts))
        .bind(u64_i64(job.updated_at_unix_ms, "updated_at_unix_ms")?)
        .execute(&self.inner.pool)
        .await?;
        Ok(())
    }

    /// Read the immutable unresolved request identity for historical work.
    ///
    /// # Errors
    ///
    /// Returns an error when the hash is corrupt or the query fails.
    pub async fn historical_work_identity(
        &self,
        job_id: &str,
    ) -> Result<Option<BlockHash>, StoreError> {
        sqlx::query_scalar::<_, Vec<u8>>(
            "SELECT request_identity_hash FROM historical_work_identities WHERE job_id = ?",
        )
        .bind(job_id)
        .fetch_optional(&self.inner.pool)
        .await?
        .map(decode_hash)
        .transpose()
    }

    /// Atomically create one materialization job and its immutable request identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the identity is reused for different work or the
    /// transaction fails.
    pub async fn create_historical_job(
        &self,
        job: &JobRecord,
        request_identity: BlockHash,
    ) -> Result<JobRecord, StoreError> {
        if job.id.is_empty() || job.kind.is_empty() {
            return Err(StoreError::InvalidConfig(
                "job id and kind must not be empty".to_owned(),
            ));
        }
        let _guard = self.inner.writer.lock().await;
        let mut transaction = self.inner.pool.begin().await?;
        sqlx::query(
            "INSERT INTO jobs(
                job_id, kind, state, payload, checkpoint, attempts, updated_at_unix_ms
             ) VALUES (?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(job_id) DO NOTHING",
        )
        .bind(&job.id)
        .bind(&job.kind)
        .bind(job.state.as_str())
        .bind(&job.payload)
        .bind(&job.checkpoint)
        .bind(i64::from(job.attempts))
        .bind(u64_i64(job.updated_at_unix_ms, "updated_at_unix_ms")?)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "INSERT INTO historical_work_identities(job_id, request_identity_hash)
             VALUES (?, ?) ON CONFLICT(job_id) DO NOTHING",
        )
        .bind(&job.id)
        .bind(request_identity.0.as_slice())
        .execute(&mut *transaction)
        .await?;
        let stored: (String, Vec<u8>) = sqlx::query_as(
            "SELECT jobs.kind, identities.request_identity_hash
             FROM jobs
             JOIN historical_work_identities AS identities
               ON identities.job_id = jobs.job_id
             WHERE jobs.job_id = ?",
        )
        .bind(&job.id)
        .fetch_one(&mut *transaction)
        .await?;
        if stored.0 != job.kind || decode_hash(stored.1)? != request_identity {
            return Err(StoreError::InvalidConfig(
                "historical idempotency key is already used by another request".to_owned(),
            ));
        }
        transaction.commit().await?;
        self.job(&job.id)
            .await?
            .ok_or_else(|| StoreError::Invariant("created historical job disappeared".to_owned()))
    }

    /// Atomically make one prepared split-stream subscription schedulable.
    ///
    /// The stream and consumer must already exist, but neither can trigger
    /// acquisition. The subscription metadata and queued scheduler job become
    /// visible in the same transaction.
    ///
    /// # Errors
    ///
    /// Returns an error when identities or ranges conflict, referenced stream
    /// state is invalid, numeric limits overflow, or the transaction fails.
    #[allow(clippy::too_many_lines)]
    pub async fn create_backfill_subscription_job(
        &self,
        subscription: &BackfillSubscriptionRecord,
        job: &JobRecord,
        request_identity: BlockHash,
    ) -> Result<JobRecord, StoreError> {
        let ranges = normalized_subscription_ranges(subscription)?;
        let preexisting_coverage = normalize_ranges(subscription.preexisting_coverage.clone())?;
        if preexisting_coverage.iter().any(|covered| {
            !ranges.iter().any(|requested| {
                requested.start() <= covered.start() && requested.end() >= covered.end()
            })
        }) {
            return Err(StoreError::InvalidConfig(
                "preexisting subscription coverage must be contained in requested ranges"
                    .to_owned(),
            ));
        }
        if subscription.subscription_id.is_empty()
            || subscription.job_id != job.id
            || ranges
                .last()
                .is_some_and(|range| range.end() > subscription.captured_finalized_target)
            || subscription.publication_revision != 0
            || subscription.effective_block_limit == 0
            || subscription.effective_byte_limit == 0
            || subscription.resume_below_ratio_millionths == 0
            || subscription.resume_below_ratio_millionths >= 1_000_000
            || subscription.delivery_batch_limits.target_encoded_bytes == 0
            || subscription.delivery_batch_limits.maximum_encoded_bytes
                < subscription.delivery_batch_limits.target_encoded_bytes
            || subscription.delivery_batch_limits.maximum_events == 0
            || subscription.delivery_batch_limits.maximum_processed_blocks == 0
            || subscription.delivery_batch_limits.maximum_delay_ms == 0
            || subscription.delivery_batch_limits.maximum_buffered_batches == 0
            || subscription.delivery_batch_limits.maximum_buffered_bytes
                < subscription.delivery_batch_limits.maximum_encoded_bytes
        {
            return Err(StoreError::InvalidConfig(
                "invalid durable backfill subscription identity or limits".to_owned(),
            ));
        }
        if !matches!(
            subscription.state,
            BackfillSubscriptionState::WaitingForConsumer | BackfillSubscriptionState::Queued
        ) || job.state != JobState::Queued
        {
            return Err(StoreError::InvalidConfig(
                "new backfill subscriptions and jobs must start queued or waiting for a consumer"
                    .to_owned(),
            ));
        }
        let _guard = self.inner.writer.lock().await;
        let stream_owner: Option<String> =
            sqlx::query_scalar("SELECT instance FROM delivery_streams WHERE stream_id = ?")
                .bind(&subscription.history_stream_id)
                .fetch_optional(&self.inner.pool)
                .await?;
        if stream_owner.as_deref() != Some(subscription.processor_instance.as_str()) {
            return Err(StoreError::InvalidConfig(
                "backfill history stream does not belong to the subscription processor".to_owned(),
            ));
        }
        let consumer_exists: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM durable_consumers
             WHERE stream_id = ? AND consumer_id = ? AND role = 'required'",
        )
        .bind(&subscription.history_stream_id)
        .bind(&subscription.consumer_id)
        .fetch_one(&self.inner.pool)
        .await?;
        if consumer_exists == 0 {
            return Err(StoreError::InvalidConfig(
                "backfill subscription requires its durable consumer before scheduling".to_owned(),
            ));
        }

        let now = now_i64()?;
        let live_stream_id = format!("{}:live", subscription.processor_instance);
        let live_merge_cut: Option<i64> =
            sqlx::query_scalar("SELECT MAX(stream_sequence) FROM change_log WHERE stream_id = ?")
                .bind(&live_stream_id)
                .fetch_one(&self.inner.pool)
                .await?;
        let mut transaction = self.inner.pool.begin().await?;
        let existing_identity: Option<(String, Vec<u8>)> = sqlx::query_as(
            "SELECT jobs.kind, identities.request_identity_hash
             FROM jobs
             JOIN historical_work_identities AS identities
               ON identities.job_id = jobs.job_id
             WHERE jobs.job_id = ?",
        )
        .bind(&job.id)
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some((kind, identity)) = existing_identity {
            if kind != job.kind || decode_hash(identity)? != request_identity {
                return Err(StoreError::InvalidConfig(
                    "backfill idempotency key is already used by another unresolved request"
                        .to_owned(),
                ));
            }
            transaction.rollback().await?;
            return self.job(&job.id).await?.ok_or_else(|| {
                StoreError::Invariant("idempotent backfill job disappeared".to_owned())
            });
        }
        let publication_revision =
            if matches!(subscription.mode, BackfillSubscriptionMode::Recompute) {
                let highest: i64 = sqlx::query_scalar(
                    "SELECT COALESCE(MAX(publication_revision), 0)
                 FROM backfill_subscriptions
                 WHERE instance = ?",
                )
                .bind(&subscription.processor_instance)
                .fetch_one(&mut *transaction)
                .await?;
                i64_u64(highest, "highest publication revision")?
                    .checked_add(1)
                    .ok_or(StoreError::Numeric("next publication revision"))?
            } else {
                0
            };
        sqlx::query(
            "INSERT INTO backfill_subscriptions(
                subscription_id, job_id, instance, history_stream_id, mode,
                publication_revision, live_stream_id, live_merge_cut_sequence,
                initial_sequence, state, consumer_id, from_block, to_block,
                captured_finalized_target, idempotency_key,
                effective_block_limit, effective_byte_limit,
                resume_below_ratio_millionths,
                delivery_target_encoded_bytes, delivery_maximum_encoded_bytes,
                delivery_maximum_events, delivery_maximum_processed_blocks,
                delivery_maximum_delay_ms, delivery_maximum_buffered_batches,
                delivery_maximum_buffered_bytes, delivery_compression, completion_sequence,
                processed_work_blocks, created_at_unix_ms, updated_at_unix_ms,
                last_error
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
             ON CONFLICT DO NOTHING",
        )
        .bind(&subscription.subscription_id)
        .bind(&subscription.job_id)
        .bind(&subscription.processor_instance)
        .bind(&subscription.history_stream_id)
        .bind(subscription.mode.as_str())
        .bind(u64_i64(
            publication_revision,
            "subscription publication revision",
        )?)
        .bind(&live_stream_id)
        .bind(live_merge_cut)
        .bind(u64_i64(
            subscription.initial_sequence,
            "initial subscription sequence",
        )?)
        .bind(subscription.state.as_str())
        .bind(&subscription.consumer_id)
        .bind(u64_i64(
            subscription.range.start().0,
            "subscription start block",
        )?)
        .bind(u64_i64(
            subscription.range.end().0,
            "subscription end block",
        )?)
        .bind(u64_i64(
            subscription.captured_finalized_target.0,
            "captured finalized target",
        )?)
        .bind(&subscription.idempotency_key)
        .bind(u64_i64(
            subscription.effective_block_limit,
            "subscription block limit",
        )?)
        .bind(u64_i64(
            subscription.effective_byte_limit,
            "subscription byte limit",
        )?)
        .bind(i64::from(subscription.resume_below_ratio_millionths))
        .bind(u64_i64(
            subscription.delivery_batch_limits.target_encoded_bytes,
            "delivery target encoded bytes",
        )?)
        .bind(u64_i64(
            subscription.delivery_batch_limits.maximum_encoded_bytes,
            "delivery maximum encoded bytes",
        )?)
        .bind(u64_i64(
            subscription.delivery_batch_limits.maximum_events,
            "delivery maximum events",
        )?)
        .bind(u64_i64(
            subscription
                .delivery_batch_limits
                .maximum_processed_blocks,
            "delivery maximum processed blocks",
        )?)
        .bind(u64_i64(
            subscription.delivery_batch_limits.maximum_delay_ms,
            "delivery maximum delay",
        )?)
        .bind(u64_i64(
            subscription.delivery_batch_limits.maximum_buffered_batches,
            "delivery maximum buffered batches",
        )?)
        .bind(u64_i64(
            subscription.delivery_batch_limits.maximum_buffered_bytes,
            "delivery maximum buffered bytes",
        )?)
        .bind(subscription.delivery_batch_limits.compression.as_str())
        .bind(
            subscription
                .completion_sequence
                .map(|value| u64_i64(value, "completion sequence"))
                .transpose()?,
        )
        .bind(u64_i64(
            subscription.processed_work_blocks,
            "processed subscription blocks",
        )?)
        .bind(now)
        .bind(now)
        .execute(&mut *transaction)
        .await?;
        let stored_subscription = sqlx::query(
            "SELECT subscription_id, job_id, history_stream_id, mode, consumer_id,
                    from_block, to_block,
                    effective_block_limit, effective_byte_limit,
                    resume_below_ratio_millionths,
                    delivery_target_encoded_bytes, delivery_maximum_encoded_bytes,
                    delivery_maximum_events, delivery_maximum_processed_blocks,
                    delivery_maximum_delay_ms, delivery_maximum_buffered_batches,
                    delivery_maximum_buffered_bytes, delivery_compression
             FROM backfill_subscriptions
             WHERE instance = ? AND idempotency_key = ?",
        )
        .bind(&subscription.processor_instance)
        .bind(&subscription.idempotency_key)
        .fetch_optional(&mut *transaction)
        .await?;
        let expected_range = (
            u64_i64(subscription.range.start().0, "subscription start block")?,
            u64_i64(subscription.range.end().0, "subscription end block")?,
        );
        let expected_block_limit = u64_i64(
            subscription.effective_block_limit,
            "subscription block limit",
        )?;
        let expected_byte_limit =
            u64_i64(subscription.effective_byte_limit, "subscription byte limit")?;
        let same_identity = stored_subscription.is_some_and(|row| {
            row.try_get::<String, _>("subscription_id").ok().as_deref()
                == Some(subscription.subscription_id.as_str())
                && row.try_get::<String, _>("job_id").ok().as_deref()
                    == Some(subscription.job_id.as_str())
                && row
                    .try_get::<String, _>("history_stream_id")
                    .ok()
                    .as_deref()
                    == Some(subscription.history_stream_id.as_str())
                && row.try_get::<String, _>("mode").ok().as_deref()
                    == Some(subscription.mode.as_str())
                && row.try_get::<String, _>("consumer_id").ok().as_deref()
                    == Some(subscription.consumer_id.as_str())
                && row.try_get::<i64, _>("from_block").ok() == Some(expected_range.0)
                && row.try_get::<i64, _>("to_block").ok() == Some(expected_range.1)
                && row.try_get::<i64, _>("effective_block_limit").ok() == Some(expected_block_limit)
                && row.try_get::<i64, _>("effective_byte_limit").ok() == Some(expected_byte_limit)
                && row.try_get::<i64, _>("resume_below_ratio_millionths").ok()
                    == Some(i64::from(subscription.resume_below_ratio_millionths))
                && row.try_get::<i64, _>("delivery_target_encoded_bytes").ok()
                    == u64_i64(
                        subscription.delivery_batch_limits.target_encoded_bytes,
                        "delivery target encoded bytes",
                    )
                    .ok()
                && row.try_get::<i64, _>("delivery_maximum_encoded_bytes").ok()
                    == u64_i64(
                        subscription.delivery_batch_limits.maximum_encoded_bytes,
                        "delivery maximum encoded bytes",
                    )
                    .ok()
                && row.try_get::<i64, _>("delivery_maximum_events").ok()
                    == u64_i64(
                        subscription.delivery_batch_limits.maximum_events,
                        "delivery maximum events",
                    )
                    .ok()
                && row
                    .try_get::<i64, _>("delivery_maximum_processed_blocks")
                    .ok()
                    == u64_i64(
                        subscription.delivery_batch_limits.maximum_processed_blocks,
                        "delivery maximum processed blocks",
                    )
                    .ok()
                && row.try_get::<i64, _>("delivery_maximum_delay_ms").ok()
                    == u64_i64(
                        subscription.delivery_batch_limits.maximum_delay_ms,
                        "delivery maximum delay",
                    )
                    .ok()
                && row
                    .try_get::<i64, _>("delivery_maximum_buffered_batches")
                    .ok()
                    == u64_i64(
                        subscription.delivery_batch_limits.maximum_buffered_batches,
                        "delivery maximum buffered batches",
                    )
                    .ok()
                && row
                    .try_get::<i64, _>("delivery_maximum_buffered_bytes")
                    .ok()
                    == u64_i64(
                        subscription.delivery_batch_limits.maximum_buffered_bytes,
                        "delivery maximum buffered bytes",
                    )
                    .ok()
                && row
                    .try_get::<String, _>("delivery_compression")
                    .ok()
                    .as_deref()
                    == Some(subscription.delivery_batch_limits.compression.as_str())
        });
        if !same_identity {
            return Err(StoreError::InvalidConfig(
                "backfill idempotency key is already used by another subscription".to_owned(),
            ));
        }
        for (ordinal, range) in ranges.iter().enumerate() {
            sqlx::query(
                "INSERT INTO backfill_subscription_ranges(
                    subscription_id, ordinal, from_block, to_block, state, committed_blocks
                 ) VALUES (?, ?, ?, ?, 'queued', 0)
                 ON CONFLICT(subscription_id, ordinal) DO NOTHING",
            )
            .bind(&subscription.subscription_id)
            .bind(u64_i64(
                u64::try_from(ordinal)
                    .map_err(|_| StoreError::Numeric("subscription range ordinal"))?,
                "subscription range ordinal",
            )?)
            .bind(u64_i64(range.start().0, "subscription range start")?)
            .bind(u64_i64(range.end().0, "subscription range end")?)
            .execute(&mut *transaction)
            .await?;
            sqlx::query(
                "INSERT INTO finalized_coverage_owners(
                    instance, owner_kind, owner_id, from_block, to_block
                 ) VALUES (?, 'subscription', ?, ?, ?)
                 ON CONFLICT(instance, owner_kind, owner_id, from_block)
                 DO UPDATE SET to_block = MAX(to_block, excluded.to_block)",
            )
            .bind(&subscription.processor_instance)
            .bind(&subscription.subscription_id)
            .bind(u64_i64(
                range.start().0,
                "subscription coverage owner start",
            )?)
            .bind(u64_i64(range.end().0, "subscription coverage owner end")?)
            .execute(&mut *transaction)
            .await?;
        }
        let stored_ranges: Vec<(i64, i64)> = sqlx::query_as(
            "SELECT from_block, to_block
             FROM backfill_subscription_ranges
             WHERE subscription_id = ?
             ORDER BY ordinal",
        )
        .bind(&subscription.subscription_id)
        .fetch_all(&mut *transaction)
        .await?;
        let expected_ranges = ranges
            .iter()
            .map(|range| {
                Ok((
                    u64_i64(range.start().0, "subscription range start")?,
                    u64_i64(range.end().0, "subscription range end")?,
                ))
            })
            .collect::<Result<Vec<_>, StoreError>>()?;
        if stored_ranges != expected_ranges {
            return Err(StoreError::InvalidConfig(
                "backfill idempotency key is already used by another range set".to_owned(),
            ));
        }
        for (ordinal, range) in preexisting_coverage.iter().enumerate() {
            sqlx::query(
                "INSERT INTO backfill_subscription_preexisting_ranges(
                    subscription_id, ordinal, from_block, to_block
                 ) VALUES (?, ?, ?, ?) ON CONFLICT(subscription_id, ordinal) DO NOTHING",
            )
            .bind(&subscription.subscription_id)
            .bind(u64_i64(
                u64::try_from(ordinal)
                    .map_err(|_| StoreError::Numeric("preexisting range ordinal"))?,
                "preexisting range ordinal",
            )?)
            .bind(u64_i64(range.start().0, "preexisting range start")?)
            .bind(u64_i64(range.end().0, "preexisting range end")?)
            .execute(&mut *transaction)
            .await?;
        }
        let stored_preexisting: Vec<(i64, i64)> = sqlx::query_as(
            "SELECT from_block, to_block
             FROM backfill_subscription_preexisting_ranges
             WHERE subscription_id = ? ORDER BY ordinal",
        )
        .bind(&subscription.subscription_id)
        .fetch_all(&mut *transaction)
        .await?;
        let expected_preexisting = preexisting_coverage
            .iter()
            .map(|range| {
                Ok((
                    u64_i64(range.start().0, "preexisting range start")?,
                    u64_i64(range.end().0, "preexisting range end")?,
                ))
            })
            .collect::<Result<Vec<_>, StoreError>>()?;
        if stored_preexisting != expected_preexisting {
            return Err(StoreError::InvalidConfig(
                "backfill idempotency key is already used by another preexisting coverage snapshot"
                    .to_owned(),
            ));
        }
        sqlx::query(
            "INSERT INTO jobs(
                job_id, kind, state, payload, checkpoint, attempts, updated_at_unix_ms
             ) VALUES (?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(job_id) DO NOTHING",
        )
        .bind(&job.id)
        .bind(&job.kind)
        .bind(job.state.as_str())
        .bind(&job.payload)
        .bind(&job.checkpoint)
        .bind(i64::from(job.attempts))
        .bind(u64_i64(job.updated_at_unix_ms, "updated_at_unix_ms")?)
        .execute(&mut *transaction)
        .await?;
        let stored_job: (String, Vec<u8>) =
            sqlx::query_as("SELECT kind, payload FROM jobs WHERE job_id = ?")
                .bind(&job.id)
                .fetch_one(&mut *transaction)
                .await?;
        if stored_job != (job.kind.clone(), job.payload.clone()) {
            return Err(StoreError::InvalidConfig(
                "backfill job identity conflicts with an existing job".to_owned(),
            ));
        }
        sqlx::query(
            "INSERT INTO historical_work_identities(job_id, request_identity_hash)
             VALUES (?, ?) ON CONFLICT(job_id) DO NOTHING",
        )
        .bind(&job.id)
        .bind(request_identity.0.as_slice())
        .execute(&mut *transaction)
        .await?;
        let stored_identity: Vec<u8> = sqlx::query_scalar(
            "SELECT request_identity_hash FROM historical_work_identities WHERE job_id = ?",
        )
        .bind(&job.id)
        .fetch_one(&mut *transaction)
        .await?;
        if decode_hash(stored_identity)? != request_identity {
            return Err(StoreError::InvalidConfig(
                "backfill idempotency key is already used by another unresolved request".to_owned(),
            ));
        }
        transaction.commit().await?;
        self.job(&job.id)
            .await?
            .ok_or_else(|| StoreError::Invariant("created backfill job disappeared".to_owned()))
    }

    /// Inspect durable subscription lifecycle metadata for one scheduler job.
    ///
    /// # Errors
    ///
    /// Returns an error when stored subscription metadata is invalid or the
    /// query fails.
    pub async fn backfill_subscription_for_job(
        &self,
        job_id: &str,
    ) -> Result<Option<BackfillSubscriptionRecord>, StoreError> {
        let row = sqlx::query(
            "SELECT subscription_id, job_id, instance, history_stream_id,
                    mode, publication_revision, state, consumer_id, from_block, to_block,
                    captured_finalized_target, idempotency_key,
                    effective_block_limit, effective_byte_limit,
                    resume_below_ratio_millionths,
                    delivery_target_encoded_bytes, delivery_maximum_encoded_bytes,
                    delivery_maximum_events, delivery_maximum_processed_blocks,
                    delivery_maximum_delay_ms, delivery_maximum_buffered_batches,
                    delivery_maximum_buffered_bytes, delivery_compression, initial_sequence,
                    completion_sequence, processed_work_blocks
             FROM backfill_subscriptions
             WHERE job_id = ?",
        )
        .bind(job_id)
        .fetch_optional(&self.inner.pool)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let mut subscription = decode_backfill_subscription(&row)?;
        let rows: Vec<(i64, i64)> = sqlx::query_as(
            "SELECT from_block, to_block
             FROM backfill_subscription_ranges
             WHERE subscription_id = ?
             ORDER BY ordinal",
        )
        .bind(&subscription.subscription_id)
        .fetch_all(&self.inner.pool)
        .await?;
        subscription.ranges = rows
            .into_iter()
            .map(|(from, to)| {
                BlockRange::new(
                    BlockNumber(i64_u64(from, "subscription range start")?),
                    BlockNumber(i64_u64(to, "subscription range end")?),
                )
                .map_err(|error| StoreError::Invariant(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        if subscription.ranges.is_empty() {
            subscription.ranges.push(subscription.range);
        }
        let rows: Vec<(i64, i64)> = sqlx::query_as(
            "SELECT from_block, to_block
             FROM backfill_subscription_preexisting_ranges
             WHERE subscription_id = ? ORDER BY ordinal",
        )
        .bind(&subscription.subscription_id)
        .fetch_all(&self.inner.pool)
        .await?;
        subscription.preexisting_coverage = rows
            .into_iter()
            .map(|(from, to)| {
                BlockRange::new(
                    BlockNumber(i64_u64(from, "preexisting coverage start")?),
                    BlockNumber(i64_u64(to, "preexisting coverage end")?),
                )
                .map_err(|error| StoreError::Invariant(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Some(subscription))
    }

    /// Read durable per-range work progress for one backfill subscription.
    ///
    /// The counters apply to the immutable creation-time work set (requested
    /// blocks minus snapshotted preexisting coverage), not to mutable shared
    /// processor coverage. This lets a resumed subscription retain ownership
    /// of its delivery work when another job covers an overlapping block.
    ///
    /// # Errors
    ///
    /// Returns an error when stored ranges or counters are invalid, or the
    /// query fails.
    pub async fn backfill_subscription_range_progress(
        &self,
        job_id: &str,
    ) -> Result<Option<Vec<BackfillSubscriptionRangeProgress>>, StoreError> {
        let subscription_id: Option<String> = sqlx::query_scalar(
            "SELECT subscription_id FROM backfill_subscriptions WHERE job_id = ?",
        )
        .bind(job_id)
        .fetch_optional(&self.inner.pool)
        .await?;
        let Some(subscription_id) = subscription_id else {
            return Ok(None);
        };
        let rows: Vec<(i64, i64, i64)> = sqlx::query_as(
            "SELECT from_block, to_block, committed_blocks
             FROM backfill_subscription_ranges
             WHERE subscription_id = ? ORDER BY ordinal",
        )
        .bind(subscription_id)
        .fetch_all(&self.inner.pool)
        .await?;
        rows.into_iter()
            .map(|(from, to, committed)| {
                Ok(BackfillSubscriptionRangeProgress {
                    range: BlockRange::new(
                        BlockNumber(i64_u64(from, "subscription range start")?),
                        BlockNumber(i64_u64(to, "subscription range end")?),
                    )
                    .map_err(|error| StoreError::Invariant(error.to_string()))?,
                    committed_work_blocks: i64_u64(
                        committed,
                        "committed subscription work blocks",
                    )?,
                })
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Some)
    }

    /// Advance the exact durable lifecycle state for one backfill job.
    ///
    /// # Errors
    ///
    /// Returns an error when the state update cannot be represented or the
    /// database write fails.
    pub async fn set_backfill_subscription_state(
        &self,
        job_id: &str,
        state: BackfillSubscriptionState,
        last_error: Option<&str>,
    ) -> Result<bool, StoreError> {
        let _guard = self.inner.writer.lock().await;
        let result = sqlx::query(
            "UPDATE backfill_subscriptions
             SET state = ?,
                 completion_sequence = CASE
                   WHEN ? = 'draining' THEN COALESCE(
                     (
                       SELECT MAX(stream_sequence) FROM change_log
                       WHERE stream_id = backfill_subscriptions.history_stream_id
                         AND kind = 'system.backfill_complete'
                     ),
                     completion_sequence
                   )
                   ELSE completion_sequence
                 END,
                 last_error = ?,
                 updated_at_unix_ms = ?
             WHERE job_id = ?",
        )
        .bind(state.as_str())
        .bind(state.as_str())
        .bind(last_error)
        .bind(now_i64()?)
        .bind(job_id)
        .execute(&self.inner.pool)
        .await?;
        Ok(result.rows_affected() != 0)
    }

    /// Mark a draining subscription reclaimable after its completion cursor is
    /// durably acknowledged by the required consumer.
    ///
    /// # Errors
    ///
    /// Returns an error when the acknowledgement cannot be represented or the
    /// database write fails.
    pub async fn mark_backfill_subscription_reclaimable(
        &self,
        subscription_id: &str,
        acknowledged_sequence: u64,
    ) -> Result<bool, StoreError> {
        let _guard = self.inner.writer.lock().await;
        let result = sqlx::query(
            "UPDATE backfill_subscriptions
             SET state = 'complete_reclaimable', updated_at_unix_ms = ?
             WHERE subscription_id = ? AND state = 'draining'
               AND completion_sequence IS NOT NULL
               AND completion_sequence <= ?",
        )
        .bind(now_i64()?)
        .bind(subscription_id)
        .bind(u64_i64(
            acknowledged_sequence,
            "acknowledged completion sequence",
        )?)
        .execute(&self.inner.pool)
        .await?;
        Ok(result.rows_affected() != 0)
    }

    /// Read a scheduler job.
    ///
    /// # Errors
    ///
    /// Returns an error for a failed database read or corrupt stored fields.
    pub async fn job(&self, id: &str) -> Result<Option<JobRecord>, StoreError> {
        let row = sqlx::query(
            "SELECT job_id, kind, state, payload, checkpoint, attempts, updated_at_unix_ms
             FROM jobs WHERE job_id = ?",
        )
        .bind(id)
        .fetch_optional(&self.inner.pool)
        .await?;
        row.map(|row| {
            Ok(JobRecord {
                id: row.try_get("job_id")?,
                kind: row.try_get("kind")?,
                state: JobState::parse(row.try_get("state")?)?,
                payload: row.try_get("payload")?,
                checkpoint: row.try_get("checkpoint")?,
                attempts: u32::try_from(row.try_get::<i64, _>("attempts")?)
                    .map_err(|_| StoreError::Numeric("attempts"))?,
                updated_at_unix_ms: i64_u64(
                    row.try_get("updated_at_unix_ms")?,
                    "updated_at_unix_ms",
                )?,
            })
        })
        .transpose()
    }

    /// List durable scheduler jobs in stable identity order.
    ///
    /// # Errors
    ///
    /// Returns an error for a failed database read or corrupt stored fields.
    pub async fn jobs(&self, kind: Option<&str>) -> Result<Vec<JobRecord>, StoreError> {
        let rows = if let Some(kind) = kind {
            sqlx::query(
                "SELECT job_id, kind, state, payload, checkpoint, attempts, updated_at_unix_ms
                 FROM jobs WHERE kind = ? ORDER BY job_id",
            )
            .bind(kind)
            .fetch_all(&self.inner.pool)
            .await?
        } else {
            sqlx::query(
                "SELECT job_id, kind, state, payload, checkpoint, attempts, updated_at_unix_ms
                 FROM jobs ORDER BY job_id",
            )
            .fetch_all(&self.inner.pool)
            .await?
        };
        rows.into_iter()
            .map(|row| {
                Ok(JobRecord {
                    id: row.try_get("job_id")?,
                    kind: row.try_get("kind")?,
                    state: JobState::parse(row.try_get("state")?)?,
                    payload: row.try_get("payload")?,
                    checkpoint: row.try_get("checkpoint")?,
                    attempts: u32::try_from(row.try_get::<i64, _>("attempts")?)
                        .map_err(|_| StoreError::Numeric("attempts"))?,
                    updated_at_unix_ms: i64_u64(
                        row.try_get("updated_at_unix_ms")?,
                        "updated_at_unix_ms",
                    )?,
                })
            })
            .collect()
    }

    /// Delete terminal historical-work control state without deleting processor output.
    ///
    /// A subscription can be deleted after its completion cursor is acknowledged,
    /// or after cancellation/failure when no active required consumer protects an
    /// unacknowledged delivery record. Its isolated history stream is reclaimed in
    /// the same transaction. A materialization deletes only its job and outcome
    /// records; processor entities, state, and coverage remain queryable.
    ///
    /// # Errors
    ///
    /// Returns an error when the job vanished, the requested owner does not match,
    /// subscription delivery is still protected, or the transaction fails.
    #[allow(clippy::too_many_lines)]
    pub async fn delete_terminal_historical_work(
        &self,
        job_id: &str,
        outcome_id: &str,
        subscription: bool,
    ) -> Result<HistoricalWorkDeletion, StoreError> {
        let _guard = self.inner.writer.lock().await;
        let mut transaction = self.inner.pool.begin().await?;
        let job_exists: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM jobs WHERE job_id = ?")
            .bind(job_id)
            .fetch_one(&mut *transaction)
            .await?;
        if job_exists == 0 {
            return Err(StoreError::Invariant(format!(
                "historical job {job_id} disappeared before deletion"
            )));
        }

        let mut deletion = HistoricalWorkDeletion::default();
        if subscription {
            let row: Option<(String, String, String)> = sqlx::query_as(
                "SELECT state, history_stream_id, instance
                 FROM backfill_subscriptions WHERE job_id = ?",
            )
            .bind(job_id)
            .fetch_optional(&mut *transaction)
            .await?;
            let Some((state, stream_id, instance)) = row else {
                return Err(StoreError::Invariant(format!(
                    "historical subscription {job_id} disappeared before deletion"
                )));
            };
            let deletable = if state == "complete_reclaimable" {
                true
            } else if matches!(state.as_str(), "cancelled" | "failed") {
                let protected: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*)
                     FROM durable_consumers AS consumer
                     JOIN delivery_streams AS stream
                       ON stream.stream_id = consumer.stream_id
                     WHERE consumer.stream_id = ?
                       AND consumer.role = 'required'
                       AND consumer.state = 'active'
                       AND consumer.acknowledged_sequence < stream.next_sequence - 1",
                )
                .bind(&stream_id)
                .fetch_one(&mut *transaction)
                .await?;
                protected == 0
            } else {
                false
            };
            if !deletable {
                return Err(StoreError::HistoricalWorkNotDeletable {
                    job_id: job_id.to_owned(),
                    state,
                });
            }

            deletion.subscription_ranges = u64::try_from(
                sqlx::query_scalar::<_, i64>(
                    "SELECT COUNT(*) FROM backfill_subscription_ranges
                     WHERE subscription_id = ?",
                )
                .bind(job_id)
                .fetch_one(&mut *transaction)
                .await?,
            )
            .map_err(|_| StoreError::Numeric("deleted subscription ranges"))?;
            deletion.consumers = u64::try_from(
                sqlx::query_scalar::<_, i64>(
                    "SELECT COUNT(*) FROM durable_consumers WHERE stream_id = ?",
                )
                .bind(&stream_id)
                .fetch_one(&mut *transaction)
                .await?,
            )
            .map_err(|_| StoreError::Numeric("deleted subscription consumers"))?;
            deletion.delivery_records = sqlx::query("DELETE FROM change_log WHERE stream_id = ?")
                .bind(&stream_id)
                .execute(&mut *transaction)
                .await?
                .rows_affected();
            let owned_ranges: Vec<(i64, i64)> = sqlx::query_as(
                "SELECT from_block, to_block FROM finalized_coverage_owners
                 WHERE instance = ? AND owner_kind = 'subscription' AND owner_id = ?
                 ORDER BY from_block",
            )
            .bind(&instance)
            .bind(job_id)
            .fetch_all(&mut *transaction)
            .await?;
            let descriptor_json: String = sqlx::query_scalar(
                "SELECT descriptor_json FROM processor_instances WHERE instance = ?",
            )
            .bind(&instance)
            .fetch_one(&mut *transaction)
            .await?;
            let descriptor: ProcessorDescriptor = serde_json::from_str(&descriptor_json)?;
            let cursor: Option<String> =
                sqlx::query_scalar("SELECT cursor FROM processor_cursors WHERE instance = ?")
                    .bind(&instance)
                    .fetch_optional(&mut *transaction)
                    .await?;
            sqlx::query(
                "DELETE FROM finalized_coverage_owners
                 WHERE instance = ? AND owner_kind = 'subscription' AND owner_id = ?",
            )
            .bind(&instance)
            .bind(job_id)
            .execute(&mut *transaction)
            .await?;
            if descriptor.lifecycle.output.mode == OutputPolicyMode::None {
                let mut candidate_intervals = BTreeSet::new();
                for (from, to) in &owned_ranges {
                    let starts: Vec<i64> = sqlx::query_scalar(
                        "SELECT start_block FROM finalized_coverage_intervals
                         WHERE instance = ? AND end_block >= ? AND start_block <= ?",
                    )
                    .bind(&instance)
                    .bind(*from)
                    .bind(*to)
                    .fetch_all(&mut *transaction)
                    .await?;
                    candidate_intervals.extend(starts);
                }
                for start in candidate_intervals {
                    let segment_count: i64 = sqlx::query_scalar(
                        "SELECT COUNT(*) FROM finalized_coverage_segments
                         WHERE instance = ? AND interval_start = ?
                           AND NOT EXISTS (
                               SELECT 1 FROM finalized_coverage_intervals AS interval
                               JOIN finalized_coverage_owners AS owner
                                 ON owner.instance = interval.instance
                                AND owner.to_block >= interval.start_block
                                AND owner.from_block <= interval.end_block
                              WHERE interval.instance = ? AND interval.start_block = ?
                           )",
                    )
                    .bind(&instance)
                    .bind(start)
                    .bind(&instance)
                    .bind(start)
                    .fetch_one(&mut *transaction)
                    .await?;
                    if segment_count == 0 {
                        continue;
                    }
                    deletion.coverage_segments = deletion.coverage_segments.saturating_add(
                        i64_u64(segment_count, "deleted compact coverage segments")?,
                    );
                    deletion.coverage_intervals = deletion.coverage_intervals.saturating_add(
                        sqlx::query(
                            "DELETE FROM finalized_coverage_intervals
                             WHERE instance = ? AND start_block = ?",
                        )
                        .bind(&instance)
                        .bind(start)
                        .execute(&mut *transaction)
                        .await?
                        .rows_affected(),
                    );
                }
                for (from, to) in &owned_ranges {
                    deletion.exact_coverage = deletion.exact_coverage.saturating_add(
                        sqlx::query(
                            "DELETE FROM processor_coverage
                             WHERE instance = ? AND block_number BETWEEN ? AND ?
                               AND NOT EXISTS (
                                   SELECT 1 FROM finalized_coverage_owners AS owner
                                    WHERE owner.instance = processor_coverage.instance
                                      AND processor_coverage.block_number
                                          BETWEEN owner.from_block AND owner.to_block
                               )",
                        )
                        .bind(&instance)
                        .bind(*from)
                        .bind(*to)
                        .execute(&mut *transaction)
                        .await?
                        .rows_affected(),
                    );
                    deletion.applied_blocks = deletion.applied_blocks.saturating_add(
                        sqlx::query(
                            "DELETE FROM applied_blocks
                             WHERE instance = ? AND block_number BETWEEN ? AND ?
                               AND NOT EXISTS (
                                   SELECT 1 FROM finalized_coverage_owners AS owner
                                    WHERE owner.instance = applied_blocks.instance
                                      AND applied_blocks.block_number
                                          BETWEEN owner.from_block AND owner.to_block
                               )",
                        )
                        .bind(&instance)
                        .bind(*from)
                        .bind(*to)
                        .execute(&mut *transaction)
                        .await?
                        .rows_affected(),
                    );
                    deletion.finalized_undo = deletion.finalized_undo.saturating_add(
                        sqlx::query(
                            "DELETE FROM undo_journal
                             WHERE instance = ? AND block_number BETWEEN ? AND ? AND finalized = 1
                               AND NOT EXISTS (
                                   SELECT 1 FROM finalized_coverage_owners AS owner
                                    WHERE owner.instance = undo_journal.instance
                                      AND undo_journal.block_number
                                          BETWEEN owner.from_block AND owner.to_block
                               )",
                        )
                        .bind(&instance)
                        .bind(*from)
                        .bind(*to)
                        .execute(&mut *transaction)
                        .await?
                        .rows_affected(),
                    );
                }
                if let Some(cursor) = cursor {
                    let cursor: ProcessorCursor =
                        OpaqueCursor::parse(cursor)?.decode(CursorKind::Processor)?;
                    if let Some(retained) =
                        highest_coverage_cursor(&mut transaction, &descriptor, cursor.chain_id)
                            .await?
                    {
                        let encoded = OpaqueCursor::encode(CursorKind::Processor, &retained)?;
                        upsert_cursor(&mut transaction, &instance, &retained, encoded.expose())
                            .await?;
                    } else {
                        sqlx::query("DELETE FROM processor_cursors WHERE instance = ?")
                            .bind(&instance)
                            .execute(&mut *transaction)
                            .await?;
                    }
                }
            }
            sqlx::query("DELETE FROM backfill_subscriptions WHERE job_id = ?")
                .bind(job_id)
                .execute(&mut *transaction)
                .await?;
            deletion.delivery_streams =
                sqlx::query("DELETE FROM delivery_streams WHERE stream_id = ?")
                    .bind(&stream_id)
                    .execute(&mut *transaction)
                    .await?
                    .rows_affected();
        } else {
            let subscription_exists: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM backfill_subscriptions WHERE job_id = ?")
                    .bind(job_id)
                    .fetch_one(&mut *transaction)
                    .await?;
            if subscription_exists != 0 {
                return Err(StoreError::Invariant(format!(
                    "historical job {job_id} is a subscription, not a materialization"
                )));
            }
        }

        deletion.jobs = sqlx::query("DELETE FROM jobs WHERE job_id IN (?, ?)")
            .bind(job_id)
            .bind(outcome_id)
            .execute(&mut *transaction)
            .await?
            .rows_affected();
        transaction.commit().await?;
        Ok(deletion)
    }

    /// List currently retained public output collections.
    ///
    /// # Errors
    ///
    /// Returns an error when the database read fails.
    pub async fn output_collections(
        &self,
        descriptor: &ProcessorDescriptor,
    ) -> Result<Vec<String>, StoreError> {
        Ok(sqlx::query_scalar(
            "SELECT DISTINCT collection FROM entities
             WHERE instance = ? ORDER BY collection",
        )
        .bind(processor_instance(descriptor))
        .fetch_all(&self.inner.pool)
        .await?)
    }

    /// Return retained block/time bounds for one public output collection.
    ///
    /// # Errors
    ///
    /// Returns an error for corrupt metadata or a failed read.
    pub async fn output_bounds(
        &self,
        descriptor: &ProcessorDescriptor,
        collection: &str,
    ) -> Result<Option<OutputBounds>, StoreError> {
        let row = sqlx::query(
            "SELECT MIN(block_number) AS earliest_block,
                    MAX(block_number) AS latest_block,
                    MIN(block_timestamp) AS earliest_timestamp,
                    MAX(block_timestamp) AS latest_timestamp
             FROM output_entity_meta
             WHERE instance = ? AND collection = ?",
        )
        .bind(processor_instance(descriptor))
        .bind(collection)
        .fetch_one(&self.inner.pool)
        .await?;
        let earliest_block: Option<i64> = row.try_get("earliest_block")?;
        let latest_block: Option<i64> = row.try_get("latest_block")?;
        let earliest_timestamp: Option<i64> = row.try_get("earliest_timestamp")?;
        let latest_timestamp: Option<i64> = row.try_get("latest_timestamp")?;
        earliest_block
            .zip(latest_block)
            .zip(earliest_timestamp.zip(latest_timestamp))
            .map(
                |((earliest_block, latest_block), (earliest_timestamp, latest_timestamp))| {
                    Ok(OutputBounds {
                        earliest_block: BlockNumber(i64_u64(
                            earliest_block,
                            "earliest output block",
                        )?),
                        latest_block: BlockNumber(i64_u64(latest_block, "latest output block")?),
                        earliest_timestamp: i64_u64(
                            earliest_timestamp,
                            "earliest output timestamp",
                        )?,
                        latest_timestamp: i64_u64(latest_timestamp, "latest output timestamp")?,
                    })
                },
            )
            .transpose()
    }

    /// Materialize a bounded, immutable output snapshot and its stream boundary
    /// in one transaction.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid filters, a zero TTL/budget, an oversized
    /// result, or a failed transaction.
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::too_many_lines)]
    pub async fn create_query_snapshot(
        &self,
        descriptor: &ProcessorDescriptor,
        collection: &str,
        query: OutputQuery,
        ttl: Duration,
        max_rows: u64,
        max_bytes: u64,
    ) -> Result<QuerySnapshot, StoreError> {
        let query = query.validate()?;
        if collection.is_empty() || ttl.is_zero() || max_rows == 0 || max_bytes == 0 {
            return Err(StoreError::InvalidConfig(
                "query snapshots require a collection and non-zero TTL/row/byte budgets".to_owned(),
            ));
        }
        let instance = self.register_processor(descriptor).await?;
        let stream_id = default_delivery_stream_id(descriptor);
        let from_block = query
            .from_block
            .map(|value| u64_i64(value.0, "query from block"))
            .transpose()?;
        let to_block = query
            .to_block
            .map(|value| u64_i64(value.0, "query to block"))
            .transpose()?;
        let from_timestamp = query
            .from_timestamp
            .map(|value| u64_i64(value, "query from timestamp"))
            .transpose()?;
        let to_timestamp = query
            .to_timestamp
            .map(|value| u64_i64(value, "query to timestamp"))
            .transpose()?;
        let _guard = self.inner.writer.lock().await;
        let mut transaction = self.inner.pool.begin().await?;
        let now = now_i64()?;
        sqlx::query("DELETE FROM query_snapshots WHERE expires_at_unix_ms <= ?")
            .bind(now)
            .execute(&mut *transaction)
            .await?;
        let row = sqlx::query(
            "SELECT COUNT(*) AS row_count,
                    COALESCE(SUM(length(entity.value)), 0) AS value_bytes
             FROM entities AS entity
             JOIN output_entity_meta AS meta
               ON meta.instance = entity.instance
              AND meta.collection = entity.collection
              AND meta.entity_key = entity.entity_key
             WHERE entity.instance = ? AND entity.collection = ?
               AND (? IS NULL OR meta.block_number >= ?)
               AND (? IS NULL OR meta.block_number <= ?)
               AND (? IS NULL OR meta.block_timestamp >= ?)
               AND (? IS NULL OR meta.block_timestamp <= ?)",
        )
        .bind(&instance)
        .bind(collection)
        .bind(from_block)
        .bind(from_block)
        .bind(to_block)
        .bind(to_block)
        .bind(from_timestamp)
        .bind(from_timestamp)
        .bind(to_timestamp)
        .bind(to_timestamp)
        .fetch_one(&mut *transaction)
        .await?;
        let row_count = i64_u64(row.try_get("row_count")?, "query snapshot rows")?;
        let value_bytes = i64_u64(row.try_get("value_bytes")?, "query snapshot bytes")?;
        if row_count > max_rows || value_bytes > max_bytes {
            return Err(StoreError::QueryTooExpensive {
                rows: row_count,
                bytes: value_bytes,
                max_rows,
                max_bytes,
            });
        }
        let pruned: i64 = sqlx::query_scalar(
            "SELECT pruned_through_sequence FROM delivery_streams WHERE stream_id = ?",
        )
        .bind(&stream_id)
        .fetch_one(&mut *transaction)
        .await?;
        let boundary: i64 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(stream_sequence), ?) FROM change_log WHERE stream_id = ?",
        )
        .bind(pruned)
        .bind(&stream_id)
        .fetch_one(&mut *transaction)
        .await?;
        let boundary = i64_u64(boundary, "query snapshot boundary")?;
        let snapshot_id = query_snapshot_id(self.inner.epoch, &instance, collection, boundary);
        let ttl_ms = i64::try_from(ttl.as_millis())
            .map_err(|_| StoreError::Numeric("query snapshot TTL"))?;
        let expires = now
            .checked_add(ttl_ms)
            .ok_or(StoreError::Numeric("query snapshot expiry"))?;
        sqlx::query(
            "INSERT INTO query_snapshots(
                snapshot_id, instance, collection, boundary_sequence,
                row_count, value_bytes, created_at_unix_ms, expires_at_unix_ms
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(snapshot_id.as_slice())
        .bind(&instance)
        .bind(collection)
        .bind(u64_i64(boundary, "query snapshot boundary")?)
        .bind(u64_i64(row_count, "query snapshot rows")?)
        .bind(u64_i64(value_bytes, "query snapshot bytes")?)
        .bind(now)
        .bind(expires)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "INSERT INTO query_snapshot_entities(
                snapshot_id, ordinal, entity_key, value,
                block_number, block_timestamp, finality
             )
             SELECT ?, ROW_NUMBER() OVER (ORDER BY entity.entity_key) - 1,
                    entity.entity_key, entity.value, meta.block_number,
                    meta.block_timestamp, meta.finality
             FROM entities AS entity
             JOIN output_entity_meta AS meta
               ON meta.instance = entity.instance
              AND meta.collection = entity.collection
              AND meta.entity_key = entity.entity_key
             WHERE entity.instance = ? AND entity.collection = ?
               AND (? IS NULL OR meta.block_number >= ?)
               AND (? IS NULL OR meta.block_number <= ?)
               AND (? IS NULL OR meta.block_timestamp >= ?)
               AND (? IS NULL OR meta.block_timestamp <= ?)
             ORDER BY entity.entity_key",
        )
        .bind(snapshot_id.as_slice())
        .bind(&instance)
        .bind(collection)
        .bind(from_block)
        .bind(from_block)
        .bind(to_block)
        .bind(to_block)
        .bind(from_timestamp)
        .bind(from_timestamp)
        .bind(to_timestamp)
        .bind(to_timestamp)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(QuerySnapshot {
            snapshot_id,
            processor_instance: instance,
            collection: collection.to_owned(),
            boundary_sequence: boundary,
            row_count,
            value_bytes,
            created_at_unix_ms: i64_u64(now, "query snapshot creation time")?,
            expires_at_unix_ms: i64_u64(expires, "query snapshot expiry")?,
        })
    }

    /// Read one page from an immutable query snapshot.
    ///
    /// `after_ordinal` is exclusive.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid limit, missing/expired/mismatched
    /// snapshot, corrupt values, or a failed read.
    pub async fn query_snapshot_page(
        &self,
        descriptor: &ProcessorDescriptor,
        collection: &str,
        snapshot_id: [u8; 16],
        after_ordinal: Option<u64>,
        limit: usize,
    ) -> Result<(QuerySnapshot, Vec<QuerySnapshotEntity>), StoreError> {
        if limit == 0 || limit > 10_000 {
            return Err(StoreError::InvalidConfig(
                "snapshot page limit must be in 1..=10000".to_owned(),
            ));
        }
        let instance = processor_instance(descriptor);
        let row = sqlx::query(
            "SELECT instance, collection, boundary_sequence, row_count,
                    value_bytes, created_at_unix_ms, expires_at_unix_ms
             FROM query_snapshots WHERE snapshot_id = ?",
        )
        .bind(snapshot_id.as_slice())
        .fetch_optional(&self.inner.pool)
        .await?
        .ok_or(StoreError::QuerySnapshotExpired)?;
        let stored_instance: String = row.try_get("instance")?;
        let stored_collection: String = row.try_get("collection")?;
        let expires_at_unix_ms =
            i64_u64(row.try_get("expires_at_unix_ms")?, "query snapshot expiry")?;
        if stored_instance != instance || stored_collection != collection {
            return Err(StoreError::QuerySnapshotMismatch);
        }
        if expires_at_unix_ms <= now_milliseconds()? {
            return Err(StoreError::QuerySnapshotExpired);
        }
        let snapshot = QuerySnapshot {
            snapshot_id,
            processor_instance: stored_instance,
            collection: stored_collection,
            boundary_sequence: i64_u64(
                row.try_get("boundary_sequence")?,
                "query snapshot boundary",
            )?,
            row_count: i64_u64(row.try_get("row_count")?, "query snapshot rows")?,
            value_bytes: i64_u64(row.try_get("value_bytes")?, "query snapshot bytes")?,
            created_at_unix_ms: i64_u64(
                row.try_get("created_at_unix_ms")?,
                "query snapshot creation time",
            )?,
            expires_at_unix_ms,
        };
        let after = after_ordinal
            .map(|value| u64_i64(value, "snapshot page ordinal"))
            .transpose()?
            .unwrap_or(-1);
        let rows = sqlx::query(
            "SELECT ordinal, entity_key, value, block_number,
                    block_timestamp, finality
             FROM query_snapshot_entities
             WHERE snapshot_id = ? AND ordinal > ?
             ORDER BY ordinal LIMIT ?",
        )
        .bind(snapshot_id.as_slice())
        .bind(after)
        .bind(usize_i64(limit, "snapshot page limit")?)
        .fetch_all(&self.inner.pool)
        .await?;
        let entities = rows
            .into_iter()
            .map(|row| {
                Ok(QuerySnapshotEntity {
                    ordinal: i64_u64(row.try_get("ordinal")?, "snapshot entity ordinal")?,
                    key: row.try_get("entity_key")?,
                    value: row.try_get("value")?,
                    block_number: BlockNumber(i64_u64(
                        row.try_get("block_number")?,
                        "snapshot entity block",
                    )?),
                    block_timestamp: i64_u64(
                        row.try_get("block_timestamp")?,
                        "snapshot entity timestamp",
                    )?,
                    finality: decode_finality(row.try_get("finality")?)?,
                })
            })
            .collect::<Result<Vec<_>, StoreError>>()?;
        Ok((snapshot, entities))
    }

    /// Release a short-lived query snapshot before its TTL.
    ///
    /// # Errors
    ///
    /// Returns an error when the delete fails.
    pub async fn release_query_snapshot(
        &self,
        descriptor: &ProcessorDescriptor,
        snapshot_id: [u8; 16],
    ) -> Result<bool, StoreError> {
        let _guard = self.inner.writer.lock().await;
        Ok(
            sqlx::query("DELETE FROM query_snapshots WHERE snapshot_id = ? AND instance = ?")
                .bind(snapshot_id.as_slice())
                .bind(processor_instance(descriptor))
                .execute(&self.inner.pool)
                .await?
                .rows_affected()
                > 0,
        )
    }

    /// Read one namespaced entity without exposing SQL.
    ///
    /// # Errors
    ///
    /// Returns an error when the database read fails.
    pub async fn entity(
        &self,
        descriptor: &ProcessorDescriptor,
        collection: &str,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(sqlx::query_scalar(
            "SELECT value FROM entities
             WHERE instance = ? AND collection = ? AND entity_key = ?",
        )
        .bind(processor_instance(descriptor))
        .bind(collection)
        .bind(key)
        .fetch_optional(&self.inner.pool)
        .await?)
    }

    /// Read one public entity together with its retained block metadata.
    ///
    /// # Errors
    ///
    /// Returns an error for corrupt metadata or a failed read.
    pub async fn output_entity(
        &self,
        descriptor: &ProcessorDescriptor,
        collection: &str,
        key: &[u8],
    ) -> Result<Option<QuerySnapshotEntity>, StoreError> {
        let row = sqlx::query(
            "SELECT entity.value, meta.block_number, meta.block_timestamp, meta.finality
             FROM entities AS entity
             JOIN output_entity_meta AS meta
               ON meta.instance = entity.instance
              AND meta.collection = entity.collection
              AND meta.entity_key = entity.entity_key
             WHERE entity.instance = ? AND entity.collection = ? AND entity.entity_key = ?",
        )
        .bind(processor_instance(descriptor))
        .bind(collection)
        .bind(key)
        .fetch_optional(&self.inner.pool)
        .await?;
        row.map(|row| {
            Ok(QuerySnapshotEntity {
                ordinal: 0,
                key: key.to_vec(),
                value: row.try_get("value")?,
                block_number: BlockNumber(i64_u64(
                    row.try_get("block_number")?,
                    "output entity block",
                )?),
                block_timestamp: i64_u64(
                    row.try_get("block_timestamp")?,
                    "output entity timestamp",
                )?,
                finality: decode_finality(row.try_get("finality")?)?,
            })
        })
        .transpose()
    }

    /// Scan entity keys in deterministic byte order.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid limit or failed database read.
    pub async fn scan_entities(
        &self,
        descriptor: &ProcessorDescriptor,
        collection: &str,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StoreError> {
        if limit == 0 || limit > 10_000 {
            return Err(StoreError::InvalidConfig(
                "scan limit must be in 1..=10000".to_owned(),
            ));
        }
        let after = after.unwrap_or_default();
        let rows = sqlx::query(
            "SELECT entity_key, value FROM entities
             WHERE instance = ? AND collection = ? AND entity_key > ?
             ORDER BY entity_key LIMIT ?",
        )
        .bind(processor_instance(descriptor))
        .bind(collection)
        .bind(after)
        .bind(usize_i64(limit, "limit")?)
        .fetch_all(&self.inner.pool)
        .await?;
        rows.into_iter()
            .map(|row| Ok((row.try_get("entity_key")?, row.try_get("value")?)))
            .collect()
    }

    /// Read deterministic entity keys from one processor-maintained index.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid limit or failed database read.
    pub async fn index_keys(
        &self,
        descriptor: &ProcessorDescriptor,
        index: &str,
        index_key: &[u8],
        limit: usize,
    ) -> Result<Vec<Vec<u8>>, StoreError> {
        self.index_keys_after(descriptor, index, index_key, None, limit)
            .await
    }

    /// Read deterministic entity keys from one processor-maintained index
    /// after an optional exclusive key boundary.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid limit or failed database read.
    pub async fn index_keys_after(
        &self,
        descriptor: &ProcessorDescriptor,
        index: &str,
        index_key: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<Vec<u8>>, StoreError> {
        if limit == 0 || limit > 10_000 {
            return Err(StoreError::InvalidConfig(
                "index limit must be in 1..=10000".to_owned(),
            ));
        }
        let limit = usize_i64(limit, "limit")?;
        if let Some(after) = after {
            Ok(sqlx::query_scalar(
                "SELECT entity_key FROM entity_indexes
                 WHERE instance = ? AND index_name = ? AND index_key = ? AND entity_key > ?
                 ORDER BY entity_key LIMIT ?",
            )
            .bind(processor_instance(descriptor))
            .bind(index)
            .bind(index_key)
            .bind(after)
            .bind(limit)
            .fetch_all(&self.inner.pool)
            .await?)
        } else {
            Ok(sqlx::query_scalar(
                "SELECT entity_key FROM entity_indexes
                 WHERE instance = ? AND index_name = ? AND index_key = ?
                 ORDER BY entity_key LIMIT ?",
            )
            .bind(processor_instance(descriptor))
            .bind(index)
            .bind(index_key)
            .bind(limit)
            .fetch_all(&self.inner.pool)
            .await?)
        }
    }

    /// Read committed changes after an opaque monotonic sequence.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid bounds, corrupt stored values, or a failed
    /// database read.
    pub async fn changes(
        &self,
        descriptor: &ProcessorDescriptor,
        chain_id: ChainId,
        after_sequence: u64,
        limit: usize,
    ) -> Result<Vec<ChangeRecord>, StoreError> {
        self.changes_in_stream(
            descriptor,
            &default_delivery_stream_id(descriptor),
            chain_id,
            after_sequence,
            limit,
        )
        .await
    }

    /// Read committed changes from one explicit delivery stream.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid bounds or stream identity, incompatible
    /// stored records, or a failed query.
    pub async fn changes_in_stream(
        &self,
        descriptor: &ProcessorDescriptor,
        stream_id: &str,
        chain_id: ChainId,
        after_sequence: u64,
        limit: usize,
    ) -> Result<Vec<ChangeRecord>, StoreError> {
        if limit == 0 || limit > 10_000 {
            return Err(StoreError::InvalidConfig(
                "change limit must be in 1..=10000".to_owned(),
            ));
        }
        self.validate_delivery_stream(&processor_instance(descriptor), stream_id)
            .await?;
        let rows = sqlx::query(
            "SELECT stream_sequence, encoding_version, chain_id,
                    origin_kind, origin_id, publication_revision,
                    block_number, block_hash, parent_hash,
                    block_timestamp, finality, direction, kind, entity_key,
                    operation, payload, created_at_unix_ms
             FROM change_log
             WHERE stream_id = ? AND stream_sequence > ?
             ORDER BY stream_sequence LIMIT ?",
        )
        .bind(stream_id)
        .bind(u64_i64(after_sequence, "sequence")?)
        .bind(usize_i64(limit, "limit")?)
        .fetch_all(&self.inner.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                let sequence = i64_u64(row.try_get("stream_sequence")?, "sequence")?;
                let encoding_version = i64_u64(
                    row.try_get("encoding_version")?,
                    "delivery encoding version",
                )?;
                if encoding_version != u64::from(DELIVERY_ENCODING_VERSION) {
                    return Err(StoreError::Invariant(format!(
                        "unsupported delivery encoding version {encoding_version}"
                    )));
                }
                let stored_chain_id =
                    ChainId(i64_u64(row.try_get("chain_id")?, "change chain ID")?);
                if stored_chain_id != chain_id {
                    return Err(StoreError::Invariant(format!(
                        "processor change belongs to chain {}, requested {}",
                        stored_chain_id.0, chain_id.0
                    )));
                }
                Ok(ChangeRecord {
                    delivery_encoding_version: DELIVERY_ENCODING_VERSION,
                    cursor: ChangeCursor {
                        chain_id,
                        processor_id: descriptor.id.to_string(),
                        sequence,
                    },
                    origin: DeliveryOrigin {
                        kind: DeliveryOriginKind::parse(row.try_get("origin_kind")?)?,
                        id: row.try_get("origin_id")?,
                        publication_revision: i64_u64(
                            row.try_get("publication_revision")?,
                            "publication revision",
                        )?,
                    },
                    block: BlockRef {
                        number: BlockNumber(i64_u64(row.try_get("block_number")?, "block_number")?),
                        hash: decode_hash(row.try_get("block_hash")?)?,
                        parent_hash: decode_hash(row.try_get("parent_hash")?)?,
                        timestamp: i64_u64(row.try_get("block_timestamp")?, "block_timestamp")?,
                    },
                    finality: decode_finality(row.try_get("finality")?)?,
                    direction: ChangeDirection::parse(row.try_get("direction")?)?,
                    change: DomainChange {
                        kind: row.try_get("kind")?,
                        key: row.try_get("entity_key")?,
                        operation: decode_operation(row.try_get("operation")?)?,
                        payload: row.try_get("payload")?,
                    },
                    emitted_at_unix_ms: i64_u64(
                        row.try_get("created_at_unix_ms")?,
                        "created_at_unix_ms",
                    )?,
                })
            })
            .collect()
    }

    /// Return the first and last retained sequence for a processor.
    ///
    /// # Errors
    ///
    /// Returns an error when stored sequences are invalid or the read fails.
    pub async fn change_bounds(
        &self,
        descriptor: &ProcessorDescriptor,
    ) -> Result<Option<ChangeBounds>, StoreError> {
        self.register_processor(descriptor).await?;
        self.change_bounds_in_stream(descriptor, &default_delivery_stream_id(descriptor))
            .await
    }

    /// Return the first and last retained sequence for one delivery stream.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid stream, corrupt stored bounds, or a
    /// failed query.
    pub async fn change_bounds_in_stream(
        &self,
        descriptor: &ProcessorDescriptor,
        stream_id: &str,
    ) -> Result<Option<ChangeBounds>, StoreError> {
        self.validate_delivery_stream(&processor_instance(descriptor), stream_id)
            .await?;
        let row = sqlx::query(
            "SELECT MIN(stream_sequence) AS earliest, MAX(stream_sequence) AS latest
             FROM change_log WHERE stream_id = ?",
        )
        .bind(stream_id)
        .fetch_one(&self.inner.pool)
        .await?;
        let earliest: Option<i64> = row.try_get("earliest")?;
        let latest: Option<i64> = row.try_get("latest")?;
        earliest
            .zip(latest)
            .map(|(earliest, latest)| {
                Ok(ChangeBounds {
                    earliest: i64_u64(earliest, "earliest change sequence")?,
                    latest: i64_u64(latest, "latest change sequence")?,
                })
            })
            .transpose()
    }

    /// Register a durable consumer at an explicit stream position.
    ///
    /// A consumer identity is scoped to one immutable processor instance.
    /// Registration never substitutes a newer position when the requested
    /// cursor has been pruned.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid input, a duplicate identity, a future or
    /// expired cursor, or a failed write.
    pub async fn create_consumer(
        &self,
        descriptor: &ProcessorDescriptor,
        consumer_id: &str,
        role: ConsumerRole,
        start: ConsumerStartPosition,
        ttl: Duration,
    ) -> Result<DurableConsumer, StoreError> {
        let stream_id = default_delivery_stream_id(descriptor);
        self.create_consumer_inner(descriptor, &stream_id, consumer_id, role, start, ttl, None)
            .await
    }

    /// Register a durable consumer in one explicit delivery stream.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid input, a duplicate identity, a future or
    /// expired cursor, an invalid stream, or a failed write.
    pub async fn create_consumer_in_stream(
        &self,
        descriptor: &ProcessorDescriptor,
        stream_id: &str,
        consumer_id: &str,
        role: ConsumerRole,
        start: ConsumerStartPosition,
        ttl: Duration,
    ) -> Result<DurableConsumer, StoreError> {
        self.create_consumer_inner(descriptor, stream_id, consumer_id, role, start, ttl, None)
            .await
    }

    /// Register a durable consumer with an isolated acknowledgement credential.
    ///
    /// Only a one-way hash is persisted. The caller must retain the original
    /// credential and present it on consumer-scoped delivery operations.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::create_consumer`] plus an invalid
    /// credential error.
    pub async fn create_consumer_with_credential(
        &self,
        descriptor: &ProcessorDescriptor,
        consumer_id: &str,
        role: ConsumerRole,
        start: ConsumerStartPosition,
        ttl: Duration,
        credential: &str,
    ) -> Result<DurableConsumer, StoreError> {
        if credential.len() < 16 || credential.len() > 512 {
            return Err(StoreError::InvalidConfig(
                "consumer credential must contain 16-512 bytes".to_owned(),
            ));
        }
        self.create_consumer_with_credential_in_stream(
            descriptor,
            &default_delivery_stream_id(descriptor),
            consumer_id,
            role,
            start,
            ttl,
            credential,
        )
        .await
    }

    /// Register a credential-protected durable consumer in one delivery stream.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::create_consumer_in_stream`] plus an
    /// invalid credential error.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_consumer_with_credential_in_stream(
        &self,
        descriptor: &ProcessorDescriptor,
        stream_id: &str,
        consumer_id: &str,
        role: ConsumerRole,
        start: ConsumerStartPosition,
        ttl: Duration,
        credential: &str,
    ) -> Result<DurableConsumer, StoreError> {
        if credential.len() < 16 || credential.len() > 512 {
            return Err(StoreError::InvalidConfig(
                "consumer credential must contain 16-512 bytes".to_owned(),
            ));
        }
        self.create_consumer_inner(
            descriptor,
            stream_id,
            consumer_id,
            role,
            start,
            ttl,
            Some(BlockHash::new(
                *blake3::hash(credential.as_bytes()).as_bytes(),
            )),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn create_consumer_inner(
        &self,
        descriptor: &ProcessorDescriptor,
        stream_id: &str,
        consumer_id: &str,
        role: ConsumerRole,
        start: ConsumerStartPosition,
        ttl: Duration,
        credential_hash: Option<BlockHash>,
    ) -> Result<DurableConsumer, StoreError> {
        if !valid_consumer_id(consumer_id) || ttl.is_zero() {
            return Err(StoreError::InvalidConfig(
                "consumer ID must be 1-128 portable characters and lease TTL must be non-zero"
                    .to_owned(),
            ));
        }
        let instance = self.register_processor(descriptor).await?;
        self.validate_delivery_stream(&instance, stream_id).await?;
        let ttl_ms = i64::try_from(ttl.as_millis())
            .map_err(|_| StoreError::Numeric("consumer lease TTL"))?;
        let _guard = self.inner.writer.lock().await;
        let mut transaction = self.inner.pool.begin().await?;
        let exists: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM durable_consumers
             WHERE stream_id = ? AND consumer_id = ?",
        )
        .bind(stream_id)
        .bind(consumer_id)
        .fetch_one(&mut *transaction)
        .await?;
        if exists != 0 {
            return Err(StoreError::ConsumerExists {
                instance,
                consumer_id: consumer_id.to_owned(),
            });
        }
        let earliest: Option<i64> =
            sqlx::query_scalar("SELECT MIN(stream_sequence) FROM change_log WHERE stream_id = ?")
                .bind(stream_id)
                .fetch_one(&mut *transaction)
                .await?;
        let latest: Option<i64> =
            sqlx::query_scalar("SELECT MAX(stream_sequence) FROM change_log WHERE stream_id = ?")
                .bind(stream_id)
                .fetch_one(&mut *transaction)
                .await?;
        let pruned_through: i64 = sqlx::query_scalar(
            "SELECT pruned_through_sequence FROM delivery_streams WHERE stream_id = ?",
        )
        .bind(stream_id)
        .fetch_one(&mut *transaction)
        .await?;
        let earliest = earliest
            .map(|value| i64_u64(value, "earliest change sequence"))
            .transpose()?;
        let latest = latest
            .map(|value| i64_u64(value, "latest change sequence"))
            .transpose()?;
        let pruned_through = i64_u64(pruned_through, "pruned delivery sequence")?;
        let acknowledged = match start {
            ConsumerStartPosition::EarliestRetained => {
                earliest.map_or(pruned_through, |value| value.saturating_sub(1))
            }
            ConsumerStartPosition::CurrentHead => latest.unwrap_or(pruned_through),
            ConsumerStartPosition::After(sequence) => {
                if sequence < pruned_through {
                    return Err(StoreError::ConsumerResetRequired {
                        earliest_available: earliest,
                        latest_available: latest,
                    });
                }
                if sequence > latest.unwrap_or(pruned_through) {
                    return Err(StoreError::AcknowledgementBeyondHead {
                        sequence,
                        head: latest.unwrap_or(pruned_through),
                    });
                }
                sequence
            }
        };
        let now = now_i64()?;
        let expires = now
            .checked_add(ttl_ms)
            .ok_or(StoreError::Numeric("consumer lease expiry"))?;
        sqlx::query(
            "INSERT INTO durable_consumers(
                stream_id, instance, consumer_id, role, state, acknowledged_sequence,
                delivered_sequence, lease_ttl_ms, lease_expires_at_unix_ms,
                created_at_unix_ms, updated_at_unix_ms, credential_hash
             ) VALUES (?, ?, ?, ?, 'active', ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(stream_id)
        .bind(&instance)
        .bind(consumer_id)
        .bind(role.as_str())
        .bind(u64_i64(acknowledged, "acknowledged sequence")?)
        .bind(u64_i64(acknowledged, "delivered sequence")?)
        .bind(ttl_ms)
        .bind(expires)
        .bind(now)
        .bind(now)
        .bind(credential_hash.map(|hash| hash.0.to_vec()))
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        self.consumer_in_stream(descriptor, stream_id, consumer_id)
            .await?
            .ok_or_else(|| StoreError::Invariant("created consumer disappeared".to_owned()))
    }

    /// Verify a consumer-scoped credential without exposing its stored hash.
    ///
    /// Consumers created by trusted local configuration have no credential and
    /// remain manageable only through the administrator-authenticated surface.
    ///
    /// # Errors
    ///
    /// Returns an error when the consumer is absent or the read fails.
    pub async fn consumer_credential_matches(
        &self,
        descriptor: &ProcessorDescriptor,
        consumer_id: &str,
        credential: &str,
    ) -> Result<bool, StoreError> {
        self.consumer_credential_matches_in_stream(
            descriptor,
            &default_delivery_stream_id(descriptor),
            consumer_id,
            credential,
        )
        .await
    }

    /// Verify a consumer credential in one explicit delivery stream.
    ///
    /// # Errors
    ///
    /// Returns an error when the stream or consumer is absent, stored metadata
    /// is invalid, or the query fails.
    pub async fn consumer_credential_matches_in_stream(
        &self,
        descriptor: &ProcessorDescriptor,
        stream_id: &str,
        consumer_id: &str,
        credential: &str,
    ) -> Result<bool, StoreError> {
        self.validate_delivery_stream(&processor_instance(descriptor), stream_id)
            .await?;
        let stored: Option<Option<Vec<u8>>> = sqlx::query_scalar(
            "SELECT credential_hash FROM durable_consumers
             WHERE stream_id = ? AND consumer_id = ?",
        )
        .bind(stream_id)
        .bind(consumer_id)
        .fetch_optional(&self.inner.pool)
        .await?;
        let stored = stored.ok_or_else(|| StoreError::ConsumerNotFound {
            instance: processor_instance(descriptor),
            consumer_id: consumer_id.to_owned(),
        })?;
        let Some(stored) = stored else {
            return Ok(false);
        };
        Ok(stored.as_slice() == blake3::hash(credential.as_bytes()).as_bytes())
    }

    /// Inspect one durable consumer.
    ///
    /// # Errors
    ///
    /// Returns an error for corrupt stored values or a failed read.
    pub async fn consumer(
        &self,
        descriptor: &ProcessorDescriptor,
        consumer_id: &str,
    ) -> Result<Option<DurableConsumer>, StoreError> {
        self.consumer_in_stream(
            descriptor,
            &default_delivery_stream_id(descriptor),
            consumer_id,
        )
        .await
    }

    /// Inspect one durable consumer in an explicit delivery stream.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid stream, corrupt stored values, or a
    /// failed query.
    pub async fn consumer_in_stream(
        &self,
        descriptor: &ProcessorDescriptor,
        stream_id: &str,
        consumer_id: &str,
    ) -> Result<Option<DurableConsumer>, StoreError> {
        self.validate_delivery_stream(&processor_instance(descriptor), stream_id)
            .await?;
        let now = now_i64()?;
        let row = sqlx::query(
            "SELECT stream_id, consumer_id, role, state, acknowledged_sequence,
                    delivered_sequence, lease_generation, lease_ttl_ms,
                    lease_expires_at_unix_ms,
                    created_at_unix_ms, updated_at_unix_ms
             FROM durable_consumers
             WHERE stream_id = ? AND consumer_id = ?",
        )
        .bind(stream_id)
        .bind(consumer_id)
        .fetch_optional(&self.inner.pool)
        .await?;
        row.map(|row| decode_consumer(&row, processor_instance(descriptor), now))
            .transpose()
    }

    /// List durable consumers for one processor instance.
    ///
    /// # Errors
    ///
    /// Returns an error for corrupt stored values or a failed read.
    pub async fn consumers(
        &self,
        descriptor: &ProcessorDescriptor,
    ) -> Result<Vec<DurableConsumer>, StoreError> {
        let instance = processor_instance(descriptor);
        let stream_id = default_delivery_stream_id(descriptor);
        let now = now_i64()?;
        let rows = sqlx::query(
            "SELECT stream_id, consumer_id, role, state, acknowledged_sequence,
                    delivered_sequence, lease_generation, lease_ttl_ms,
                    lease_expires_at_unix_ms,
                    created_at_unix_ms, updated_at_unix_ms
             FROM durable_consumers
             WHERE stream_id = ? ORDER BY consumer_id",
        )
        .bind(&stream_id)
        .fetch_all(&self.inner.pool)
        .await?;
        rows.into_iter()
            .map(|row| decode_consumer(&row, instance.clone(), now))
            .collect()
    }

    /// Measure unacknowledged delivery lag for one durable consumer.
    ///
    /// # Errors
    ///
    /// Returns an error when the consumer is absent, stored values are
    /// corrupt, or the read fails.
    pub async fn consumer_lag(
        &self,
        descriptor: &ProcessorDescriptor,
        consumer_id: &str,
    ) -> Result<ConsumerLag, StoreError> {
        self.consumer_lag_in_stream(
            descriptor,
            &default_delivery_stream_id(descriptor),
            consumer_id,
        )
        .await
    }

    /// Measure one consumer's lag within an explicit delivery stream.
    ///
    /// # Errors
    ///
    /// Returns an error when the consumer is absent, stored values are invalid,
    /// or the query fails.
    pub async fn consumer_lag_in_stream(
        &self,
        descriptor: &ProcessorDescriptor,
        stream_id: &str,
        consumer_id: &str,
    ) -> Result<ConsumerLag, StoreError> {
        let consumer = self
            .consumer_in_stream(descriptor, stream_id, consumer_id)
            .await?
            .ok_or_else(|| StoreError::ConsumerNotFound {
                instance: processor_instance(descriptor),
                consumer_id: consumer_id.to_owned(),
            })?;
        let row = sqlx::query(
            "SELECT COUNT(*) AS changes,
                    COUNT(DISTINCT block_number) AS blocks,
                    COALESCE(SUM(length(entity_key) + length(payload)), 0) AS bytes,
                    MIN(created_at_unix_ms) AS oldest
             FROM change_log WHERE stream_id = ? AND stream_sequence > ?",
        )
        .bind(&consumer.stream_id)
        .bind(u64_i64(
            consumer.acknowledged_sequence,
            "consumer acknowledged sequence",
        )?)
        .fetch_one(&self.inner.pool)
        .await?;
        let oldest: Option<i64> = row.try_get("oldest")?;
        let now = i64_u64(now_i64()?, "current wall clock")?;
        let age_ms = if let Some(oldest) = oldest {
            now.saturating_sub(i64_u64(oldest, "oldest consumer change")?)
        } else {
            0
        };
        Ok(ConsumerLag {
            changes: i64_u64(row.try_get("changes")?, "consumer lag changes")?,
            blocks: i64_u64(row.try_get("blocks")?, "consumer lag blocks")?,
            bytes: i64_u64(row.try_get("bytes")?, "consumer lag bytes")?,
            age_ms,
        })
    }

    /// Inspect the durable delivery spool and the acknowledgement boundary
    /// protected by every registered active required consumer.
    ///
    /// A lapsed lease remains active and therefore remains in the watermark
    /// until an explicit reset or revocation.
    ///
    /// # Errors
    ///
    /// Returns an error when the processor is absent, stored values are
    /// invalid, or the read fails.
    pub async fn delivery_stream_stats(
        &self,
        descriptor: &ProcessorDescriptor,
    ) -> Result<DeliveryStreamStats, StoreError> {
        if descriptor.lifecycle.delivery.mode == DeliveryPolicyMode::None {
            self.register_processor(descriptor).await?;
            return Ok(DeliveryStreamStats {
                live_bytes: 0,
                pruned_through_sequence: 0,
                required_ack_watermark: None,
                format_version: 1,
            });
        }
        self.delivery_stream_stats_in_stream(descriptor, &default_delivery_stream_id(descriptor))
            .await
    }

    /// Inspect one explicit delivery stream's retention watermarks.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid stream, corrupt stored watermarks, or a
    /// failed query.
    pub async fn delivery_stream_stats_in_stream(
        &self,
        descriptor: &ProcessorDescriptor,
        stream_id: &str,
    ) -> Result<DeliveryStreamStats, StoreError> {
        self.register_processor(descriptor).await?;
        self.validate_delivery_stream(&processor_instance(descriptor), stream_id)
            .await?;
        let row = sqlx::query(
            "SELECT live_bytes, pruned_through_sequence, format_version,
                    (
                        SELECT MIN(acknowledged_sequence)
                        FROM durable_consumers
                        WHERE stream_id = delivery_streams.stream_id
                          AND role = 'required'
                          AND state = 'active'
                    ) AS required_ack_watermark
             FROM delivery_streams WHERE stream_id = ?",
        )
        .bind(stream_id)
        .fetch_one(&self.inner.pool)
        .await?;
        let format_version = i64_u64(
            row.try_get("format_version")?,
            "delivery stream format version",
        )?;
        Ok(DeliveryStreamStats {
            live_bytes: i64_u64(row.try_get("live_bytes")?, "delivery live bytes")?,
            pruned_through_sequence: i64_u64(
                row.try_get("pruned_through_sequence")?,
                "pruned delivery sequence",
            )?,
            required_ack_watermark: row
                .try_get::<Option<i64>, _>("required_ack_watermark")?
                .map(|value| i64_u64(value, "required acknowledgement watermark"))
                .transpose()?,
            format_version: u16::try_from(format_version)
                .map_err(|_| StoreError::Numeric("delivery stream format version"))?,
        })
    }

    /// Renew a durable consumer lease without changing its acknowledgement.
    ///
    /// Lease lapse is observable but does not expire or unprotect a required
    /// consumer.
    ///
    /// # Errors
    ///
    /// Returns an error when the consumer is absent, inactive, or the write
    /// fails.
    pub async fn renew_consumer(
        &self,
        descriptor: &ProcessorDescriptor,
        consumer_id: &str,
    ) -> Result<DurableConsumer, StoreError> {
        self.renew_consumer_in_stream(
            descriptor,
            &default_delivery_stream_id(descriptor),
            consumer_id,
        )
        .await
    }

    /// Renew a consumer session lease in one explicit stream.
    ///
    /// # Errors
    ///
    /// Returns an error when the stream or consumer is absent, the consumer is
    /// inactive, or the database write fails.
    pub async fn renew_consumer_in_stream(
        &self,
        descriptor: &ProcessorDescriptor,
        stream_id: &str,
        consumer_id: &str,
    ) -> Result<DurableConsumer, StoreError> {
        let instance = processor_instance(descriptor);
        self.validate_delivery_stream(&instance, stream_id).await?;
        let writer_guard = self.inner.writer.lock().await;
        let now = now_i64()?;
        let result = sqlx::query(
            "UPDATE durable_consumers
             SET lease_expires_at_unix_ms = ? + lease_ttl_ms,
                 updated_at_unix_ms = ?
             WHERE stream_id = ? AND consumer_id = ? AND state = 'active'",
        )
        .bind(now)
        .bind(now)
        .bind(stream_id)
        .bind(consumer_id)
        .execute(&self.inner.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(self
                .consumer_state_error_in_stream(descriptor, stream_id, consumer_id)
                .await?);
        }
        drop(writer_guard);
        self.consumer_in_stream(descriptor, stream_id, consumer_id)
            .await?
            .ok_or_else(|| StoreError::Invariant("renewed consumer disappeared".to_owned()))
    }

    /// Acquire exclusive ownership of one consumer's streaming session.
    ///
    /// # Errors
    ///
    /// Returns an error when the stream or consumer is invalid, another
    /// session remains active, stored values overflow, or the write fails.
    pub async fn acquire_consumer_session_in_stream(
        &self,
        descriptor: &ProcessorDescriptor,
        stream_id: &str,
        consumer_id: &str,
    ) -> Result<ConsumerSessionLease, StoreError> {
        let instance = processor_instance(descriptor);
        self.validate_delivery_stream(&instance, stream_id).await?;
        let _guard = self.inner.writer.lock().await;
        let now = now_i64()?;
        let row = sqlx::query(
            "SELECT state, lease_generation, lease_expires_at_unix_ms, lease_ttl_ms
             FROM durable_consumers
             WHERE stream_id = ? AND consumer_id = ?",
        )
        .bind(stream_id)
        .bind(consumer_id)
        .fetch_optional(&self.inner.pool)
        .await?
        .ok_or_else(|| StoreError::ConsumerNotFound {
            instance,
            consumer_id: consumer_id.to_owned(),
        })?;
        let state = ConsumerState::parse(row.try_get("state")?)?;
        if state != ConsumerState::Active {
            return Err(StoreError::ConsumerInactive {
                consumer_id: consumer_id.to_owned(),
                state,
            });
        }
        let generation = i64_u64(
            row.try_get("lease_generation")?,
            "consumer lease generation",
        )?;
        let expires_at = row.try_get::<i64, _>("lease_expires_at_unix_ms")?;
        if generation > 0 && expires_at > now {
            return Err(StoreError::ConsumerSessionActive {
                consumer_id: consumer_id.to_owned(),
                expires_at_unix_ms: i64_u64(expires_at, "consumer lease expiry")?,
            });
        }
        let next_generation = generation
            .checked_add(1)
            .ok_or(StoreError::Numeric("consumer lease generation"))?;
        let ttl_ms: i64 = row.try_get("lease_ttl_ms")?;
        let next_expiry = now
            .checked_add(ttl_ms)
            .ok_or(StoreError::Numeric("consumer lease expiry"))?;
        sqlx::query(
            "UPDATE durable_consumers
             SET lease_generation = ?, lease_expires_at_unix_ms = ?,
                 updated_at_unix_ms = ?
             WHERE stream_id = ? AND consumer_id = ?",
        )
        .bind(u64_i64(next_generation, "consumer lease generation")?)
        .bind(next_expiry)
        .bind(now)
        .bind(stream_id)
        .bind(consumer_id)
        .execute(&self.inner.pool)
        .await?;
        Ok(ConsumerSessionLease {
            generation: next_generation,
            expires_at_unix_ms: i64_u64(next_expiry, "consumer lease expiry")?,
        })
    }

    /// Renew an exclusively held streaming session when its generation still
    /// matches.
    ///
    /// # Errors
    ///
    /// Returns an error when the session is stale or expired, stored values are
    /// invalid, or the database operation fails.
    pub async fn renew_consumer_session_in_stream(
        &self,
        stream_id: &str,
        consumer_id: &str,
        generation: u64,
    ) -> Result<ConsumerSessionLease, StoreError> {
        let _guard = self.inner.writer.lock().await;
        let now = now_i64()?;
        let result = sqlx::query(
            "UPDATE durable_consumers
             SET lease_expires_at_unix_ms = ? + lease_ttl_ms,
                 updated_at_unix_ms = ?
             WHERE stream_id = ? AND consumer_id = ?
               AND state = 'active' AND lease_generation = ?
               AND lease_expires_at_unix_ms > ?",
        )
        .bind(now)
        .bind(now)
        .bind(stream_id)
        .bind(consumer_id)
        .bind(u64_i64(generation, "consumer lease generation")?)
        .bind(now)
        .execute(&self.inner.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(StoreError::ConsumerSessionLost {
                consumer_id: consumer_id.to_owned(),
                generation,
            });
        }
        let expiry: i64 = sqlx::query_scalar(
            "SELECT lease_expires_at_unix_ms
             FROM durable_consumers
             WHERE stream_id = ? AND consumer_id = ?",
        )
        .bind(stream_id)
        .bind(consumer_id)
        .fetch_one(&self.inner.pool)
        .await?;
        Ok(ConsumerSessionLease {
            generation,
            expires_at_unix_ms: i64_u64(expiry, "consumer lease expiry")?,
        })
    }

    /// Check whether an exclusive streaming session is still current and
    /// unexpired without extending it.
    ///
    /// # Errors
    ///
    /// Returns an error when stored session metadata is invalid or the query
    /// fails.
    pub async fn consumer_session_is_current_in_stream(
        &self,
        stream_id: &str,
        consumer_id: &str,
        generation: u64,
    ) -> Result<bool, StoreError> {
        let row: Option<(String, i64, i64)> = sqlx::query_as(
            "SELECT state, lease_generation, lease_expires_at_unix_ms
             FROM durable_consumers
             WHERE stream_id = ? AND consumer_id = ?",
        )
        .bind(stream_id)
        .bind(consumer_id)
        .fetch_optional(&self.inner.pool)
        .await?;
        let Some((state, stored_generation, expires_at)) = row else {
            return Ok(false);
        };
        Ok(ConsumerState::parse(&state)? == ConsumerState::Active
            && i64_u64(stored_generation, "consumer lease generation")? == generation
            && expires_at > now_i64()?)
    }

    /// Release a streaming session without affecting acknowledgement
    /// retention. A stale generation cannot release its successor.
    ///
    /// # Errors
    ///
    /// Returns an error when the generation cannot be represented or the
    /// database write fails.
    pub async fn release_consumer_session_in_stream(
        &self,
        stream_id: &str,
        consumer_id: &str,
        generation: u64,
    ) -> Result<bool, StoreError> {
        let _guard = self.inner.writer.lock().await;
        let now = now_i64()?;
        let result = sqlx::query(
            "UPDATE durable_consumers
             SET lease_expires_at_unix_ms = ?, updated_at_unix_ms = ?
             WHERE stream_id = ? AND consumer_id = ? AND lease_generation = ?",
        )
        .bind(now)
        .bind(now)
        .bind(stream_id)
        .bind(consumer_id)
        .bind(u64_i64(generation, "consumer lease generation")?)
        .execute(&self.inner.pool)
        .await?;
        Ok(result.rows_affected() != 0)
    }

    /// Read the next replay batch and durably advance only the delivered head.
    ///
    /// The acknowledged cursor is unchanged until [`Self::acknowledge_consumer`]
    /// is called after the destination commit.
    ///
    /// # Errors
    ///
    /// Returns an error when the consumer is absent/inactive, its cursor has
    /// been reset, bounds are invalid, or the read/write fails.
    pub async fn consumer_changes(
        &self,
        descriptor: &ProcessorDescriptor,
        chain_id: ChainId,
        consumer_id: &str,
        limit: usize,
    ) -> Result<Vec<ChangeRecord>, StoreError> {
        self.consumer_changes_in_stream(
            descriptor,
            &default_delivery_stream_id(descriptor),
            chain_id,
            consumer_id,
            limit,
        )
        .await
    }

    /// Read the next replay batch for a consumer in one explicit stream.
    ///
    /// # Errors
    ///
    /// Returns an error when the consumer or stream is invalid, bounds are
    /// invalid, retained data cannot satisfy the cursor, or the query fails.
    pub async fn consumer_changes_in_stream(
        &self,
        descriptor: &ProcessorDescriptor,
        stream_id: &str,
        chain_id: ChainId,
        consumer_id: &str,
        limit: usize,
    ) -> Result<Vec<ChangeRecord>, StoreError> {
        let acknowledged = self
            .consumer_in_stream(descriptor, stream_id, consumer_id)
            .await?
            .ok_or_else(|| StoreError::ConsumerNotFound {
                instance: processor_instance(descriptor),
                consumer_id: consumer_id.to_owned(),
            })?
            .acknowledged_sequence;
        self.consumer_changes_after_in_stream(
            descriptor,
            stream_id,
            chain_id,
            consumer_id,
            acknowledged,
            limit,
        )
        .await
    }

    /// Read after an in-memory session cursor while retaining the durable
    /// acknowledgement as the replay point.
    ///
    /// # Errors
    ///
    /// Returns an error when the consumer or stream is invalid, bounds are
    /// invalid, retained data cannot satisfy the cursor, or the query fails.
    pub async fn consumer_changes_after_in_stream(
        &self,
        descriptor: &ProcessorDescriptor,
        stream_id: &str,
        chain_id: ChainId,
        consumer_id: &str,
        after: u64,
        limit: usize,
    ) -> Result<Vec<ChangeRecord>, StoreError> {
        let instance = processor_instance(descriptor);
        self.validate_delivery_stream(&instance, stream_id).await?;
        let _guard = self.inner.writer.lock().await;
        let row = sqlx::query(
            "SELECT state, acknowledged_sequence FROM durable_consumers
             WHERE stream_id = ? AND consumer_id = ?",
        )
        .bind(stream_id)
        .bind(consumer_id)
        .fetch_optional(&self.inner.pool)
        .await?
        .ok_or_else(|| StoreError::ConsumerNotFound {
            instance: instance.clone(),
            consumer_id: consumer_id.to_owned(),
        })?;
        let state = ConsumerState::parse(row.try_get("state")?)?;
        if state != ConsumerState::Active {
            return Err(StoreError::ConsumerInactive {
                consumer_id: consumer_id.to_owned(),
                state,
            });
        }
        let acknowledged = i64_u64(
            row.try_get("acknowledged_sequence")?,
            "consumer acknowledged sequence",
        )?;
        if after < acknowledged {
            return Err(StoreError::AcknowledgementBackwards {
                sequence: after,
                acknowledged,
            });
        }
        let pruned_through: i64 = sqlx::query_scalar(
            "SELECT pruned_through_sequence FROM delivery_streams WHERE stream_id = ?",
        )
        .bind(stream_id)
        .fetch_one(&self.inner.pool)
        .await?;
        let pruned_through = i64_u64(pruned_through, "pruned delivery sequence")?;
        if acknowledged < pruned_through {
            sqlx::query(
                "UPDATE durable_consumers
                 SET state = 'reset_required', updated_at_unix_ms = ?
                 WHERE stream_id = ? AND consumer_id = ?",
            )
            .bind(now_i64()?)
            .bind(stream_id)
            .bind(consumer_id)
            .execute(&self.inner.pool)
            .await?;
            let bounds = self.change_bounds_in_stream(descriptor, stream_id).await?;
            return Err(StoreError::ConsumerResetRequired {
                earliest_available: bounds.map(|value| value.earliest),
                latest_available: bounds.map(|value| value.latest),
            });
        }
        self.changes_in_stream(descriptor, stream_id, chain_id, after, limit)
            .await
    }

    /// Advance a cumulative acknowledgement after destination commit.
    ///
    /// # Errors
    ///
    /// Rejects backwards acknowledgements and sequences beyond the committed
    /// stream head. Delivery sessions intentionally keep sent progress only in
    /// memory; the durable acknowledgement remains the replay authority.
    pub async fn acknowledge_consumer(
        &self,
        descriptor: &ProcessorDescriptor,
        consumer_id: &str,
        sequence: u64,
    ) -> Result<DurableConsumer, StoreError> {
        self.acknowledge_consumer_in_stream(
            descriptor,
            &default_delivery_stream_id(descriptor),
            consumer_id,
            sequence,
        )
        .await
    }

    /// Advance one stream-scoped cumulative acknowledgement.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid stream or consumer, a backwards,
    /// non-boundary, or future acknowledgement, or a failed transaction.
    pub async fn acknowledge_consumer_in_stream(
        &self,
        descriptor: &ProcessorDescriptor,
        stream_id: &str,
        consumer_id: &str,
        sequence: u64,
    ) -> Result<DurableConsumer, StoreError> {
        self.acknowledge_consumer_in_stream_inner(
            descriptor,
            stream_id,
            consumer_id,
            None,
            sequence,
        )
        .await
    }

    /// Advance one stream-scoped acknowledgement while fencing stale delivery
    /// sessions. Session validation, lease renewal, and acknowledgement commit
    /// atomically so a replaced session can never advance durable progress.
    ///
    /// # Errors
    ///
    /// Returns an error for a stale session or any error described by
    /// [`Self::acknowledge_consumer_in_stream`].
    pub async fn acknowledge_consumer_session_in_stream(
        &self,
        descriptor: &ProcessorDescriptor,
        stream_id: &str,
        consumer_id: &str,
        generation: u64,
        sequence: u64,
    ) -> Result<DurableConsumer, StoreError> {
        self.acknowledge_consumer_in_stream_inner(
            descriptor,
            stream_id,
            consumer_id,
            Some(generation),
            sequence,
        )
        .await
    }

    #[allow(clippy::too_many_lines)]
    async fn acknowledge_consumer_in_stream_inner(
        &self,
        descriptor: &ProcessorDescriptor,
        stream_id: &str,
        consumer_id: &str,
        session_generation: Option<u64>,
        sequence: u64,
    ) -> Result<DurableConsumer, StoreError> {
        let instance = processor_instance(descriptor);
        self.validate_delivery_stream(&instance, stream_id).await?;
        let writer_guard = self.inner.writer.lock().await;
        let mut transaction = self.inner.pool.begin().await?;
        let now = now_i64()?;
        let row = sqlx::query(
            "SELECT state, acknowledged_sequence, acknowledged_work_blocks,
                    lease_generation, lease_expires_at_unix_ms, lease_ttl_ms
             FROM durable_consumers WHERE stream_id = ? AND consumer_id = ?",
        )
        .bind(stream_id)
        .bind(consumer_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or_else(|| StoreError::ConsumerNotFound {
            instance: instance.clone(),
            consumer_id: consumer_id.to_owned(),
        })?;
        let state = ConsumerState::parse(row.try_get("state")?)?;
        if state != ConsumerState::Active {
            return Err(StoreError::ConsumerInactive {
                consumer_id: consumer_id.to_owned(),
                state,
            });
        }
        if let Some(generation) = session_generation {
            let stored_generation = i64_u64(
                row.try_get("lease_generation")?,
                "consumer lease generation",
            )?;
            let expires_at: i64 = row.try_get("lease_expires_at_unix_ms")?;
            if stored_generation != generation || expires_at <= now {
                return Err(StoreError::ConsumerSessionLost {
                    consumer_id: consumer_id.to_owned(),
                    generation,
                });
            }
        }
        let acknowledged = i64_u64(
            row.try_get("acknowledged_sequence")?,
            "consumer acknowledged sequence",
        )?;
        let acknowledged_work_blocks = i64_u64(
            row.try_get("acknowledged_work_blocks")?,
            "consumer acknowledged work blocks",
        )?;
        if sequence < acknowledged {
            return Err(StoreError::AcknowledgementBackwards {
                sequence,
                acknowledged,
            });
        }
        if sequence > acknowledged {
            let head: Option<i64> = sqlx::query_scalar(
                "SELECT MAX(stream_sequence) FROM change_log WHERE stream_id = ?",
            )
            .bind(stream_id)
            .fetch_one(&mut *transaction)
            .await?;
            let head = head
                .map(|value| i64_u64(value, "delivery stream head"))
                .transpose()?
                .unwrap_or(0);
            if sequence > head {
                return Err(StoreError::AcknowledgementBeyondHead { sequence, head });
            }
            let stream_kind: String =
                sqlx::query_scalar("SELECT stream_kind FROM delivery_streams WHERE stream_id = ?")
                    .bind(stream_id)
                    .fetch_one(&mut *transaction)
                    .await?;
            if DeliveryStreamKind::parse(&stream_kind)? == DeliveryStreamKind::Backfill {
                let kind: Option<String> = sqlx::query_scalar(
                    "SELECT kind FROM change_log
                     WHERE stream_id = ? AND stream_sequence = ?",
                )
                .bind(stream_id)
                .bind(u64_i64(sequence, "acknowledged delivery sequence")?)
                .fetch_optional(&mut *transaction)
                .await?;
                if !matches!(
                    kind.as_deref(),
                    Some("system.backfill_progress" | "system.backfill_complete")
                ) {
                    return Err(StoreError::AcknowledgementNotBoundary { sequence });
                }
            }
        }
        let progress_payloads: Vec<Vec<u8>> = sqlx::query_scalar(
            "SELECT payload
             FROM change_log
             WHERE stream_id = ?
               AND stream_sequence > ?
               AND stream_sequence <= ?
               AND kind = 'system.backfill_progress'",
        )
        .bind(stream_id)
        .bind(u64_i64(
            acknowledged,
            "prior acknowledged delivery sequence",
        )?)
        .bind(u64_i64(sequence, "acknowledged delivery sequence")?)
        .fetch_all(&mut *transaction)
        .await?;
        let newly_acknowledged_work =
            progress_payloads.iter().try_fold(0_u64, |total, payload| {
                total
                    .checked_add(backfill_progress_blocks(payload)?)
                    .ok_or(StoreError::Numeric("newly acknowledged work blocks"))
            })?;
        let acknowledged_work_blocks = acknowledged_work_blocks
            .checked_add(newly_acknowledged_work)
            .ok_or(StoreError::Numeric("consumer acknowledged work blocks"))?;
        let acknowledged_sequence = u64_i64(sequence, "acknowledged sequence")?;
        let acknowledged_work_blocks = u64_i64(
            acknowledged_work_blocks,
            "consumer acknowledged work blocks",
        )?;
        let result = if let Some(generation) = session_generation {
            let lease_ttl_ms: i64 = row.try_get("lease_ttl_ms")?;
            let next_expiry = now
                .checked_add(lease_ttl_ms)
                .ok_or(StoreError::Numeric("consumer lease expiry"))?;
            sqlx::query(
                "UPDATE durable_consumers
                 SET acknowledged_sequence = ?,
                     delivered_sequence = MAX(delivered_sequence, ?),
                     acknowledged_work_blocks = ?,
                     lease_expires_at_unix_ms = ?,
                     updated_at_unix_ms = ?
                 WHERE stream_id = ? AND consumer_id = ?
                   AND state = 'active' AND lease_generation = ?
                   AND lease_expires_at_unix_ms > ?",
            )
            .bind(acknowledged_sequence)
            .bind(acknowledged_sequence)
            .bind(acknowledged_work_blocks)
            .bind(next_expiry)
            .bind(now)
            .bind(stream_id)
            .bind(consumer_id)
            .bind(u64_i64(generation, "consumer lease generation")?)
            .bind(now)
            .execute(&mut *transaction)
            .await?
        } else {
            sqlx::query(
                "UPDATE durable_consumers
                 SET acknowledged_sequence = ?,
                     delivered_sequence = MAX(delivered_sequence, ?),
                     acknowledged_work_blocks = ?,
                     updated_at_unix_ms = ?
                 WHERE stream_id = ? AND consumer_id = ?",
            )
            .bind(acknowledged_sequence)
            .bind(acknowledged_sequence)
            .bind(acknowledged_work_blocks)
            .bind(now)
            .bind(stream_id)
            .bind(consumer_id)
            .execute(&mut *transaction)
            .await?
        };
        if result.rows_affected() == 0 {
            if let Some(generation) = session_generation {
                return Err(StoreError::ConsumerSessionLost {
                    consumer_id: consumer_id.to_owned(),
                    generation,
                });
            }
            return Err(StoreError::Invariant(
                "acknowledged consumer disappeared during update".to_owned(),
            ));
        }
        transaction.commit().await?;
        drop(writer_guard);
        let consumer = self
            .consumer_in_stream(descriptor, stream_id, consumer_id)
            .await?
            .ok_or_else(|| StoreError::Invariant("acknowledged consumer disappeared".to_owned()))?;
        self.inner.delivery_capacity_changed.notify_waiters();
        Ok(consumer)
    }

    /// Explicitly revoke a consumer so it no longer protects pruning.
    ///
    /// # Errors
    ///
    /// Returns an error when the consumer does not exist or the write fails.
    pub async fn revoke_consumer(
        &self,
        descriptor: &ProcessorDescriptor,
        consumer_id: &str,
    ) -> Result<DurableConsumer, StoreError> {
        self.transition_consumer(descriptor, consumer_id, ConsumerState::Revoked)
            .await
    }

    /// Explicitly mark an incompatible/expired consumer as requiring reset.
    ///
    /// # Errors
    ///
    /// Returns an error when the consumer does not exist or the write fails.
    pub async fn reset_consumer_required(
        &self,
        descriptor: &ProcessorDescriptor,
        consumer_id: &str,
    ) -> Result<DurableConsumer, StoreError> {
        self.transition_consumer(descriptor, consumer_id, ConsumerState::ResetRequired)
            .await
    }

    async fn transition_consumer(
        &self,
        descriptor: &ProcessorDescriptor,
        consumer_id: &str,
        state: ConsumerState,
    ) -> Result<DurableConsumer, StoreError> {
        let instance = processor_instance(descriptor);
        let stream_id = default_delivery_stream_id(descriptor);
        let writer_guard = self.inner.writer.lock().await;
        let result = sqlx::query(
            "UPDATE durable_consumers SET state = ?, updated_at_unix_ms = ?
             WHERE stream_id = ? AND consumer_id = ?",
        )
        .bind(state.as_str())
        .bind(now_i64()?)
        .bind(&stream_id)
        .bind(consumer_id)
        .execute(&self.inner.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(StoreError::ConsumerNotFound {
                instance,
                consumer_id: consumer_id.to_owned(),
            });
        }
        drop(writer_guard);
        self.consumer(descriptor, consumer_id)
            .await?
            .ok_or_else(|| StoreError::Invariant("transitioned consumer disappeared".to_owned()))
    }

    async fn consumer_state_error_in_stream(
        &self,
        descriptor: &ProcessorDescriptor,
        stream_id: &str,
        consumer_id: &str,
    ) -> Result<StoreError, StoreError> {
        Ok(
            match self
                .consumer_in_stream(descriptor, stream_id, consumer_id)
                .await?
            {
                Some(consumer) => StoreError::ConsumerInactive {
                    consumer_id: consumer.consumer_id,
                    state: consumer.state,
                },
                None => StoreError::ConsumerNotFound {
                    instance: processor_instance(descriptor),
                    consumer_id: consumer_id.to_owned(),
                },
            },
        )
    }

    /// Legacy create/renew helper retained for callers migrating to the
    /// explicit consumer API.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid input or a failed write.
    pub async fn renew_consumer_lease(
        &self,
        descriptor: &ProcessorDescriptor,
        consumer_id: &str,
        acknowledged_sequence: u64,
        ttl: Duration,
    ) -> Result<(), StoreError> {
        if !valid_consumer_id(consumer_id) || ttl.is_zero() {
            return Err(StoreError::InvalidConfig(
                "consumer ID must be 1-128 portable characters and lease TTL must be non-zero"
                    .to_owned(),
            ));
        }
        let instance = self.register_processor(descriptor).await?;
        let stream_id = default_delivery_stream_id(descriptor);
        let _guard = self.inner.writer.lock().await;
        let now = now_i64()?;
        let ttl_ms = i64::try_from(ttl.as_millis())
            .map_err(|_| StoreError::Numeric("consumer lease TTL"))?;
        let expires = now
            .checked_add(ttl_ms)
            .ok_or(StoreError::Numeric("consumer lease expiry"))?;
        sqlx::query(
            "INSERT INTO durable_consumers(
                stream_id, instance, consumer_id, role, state, acknowledged_sequence,
                delivered_sequence, lease_ttl_ms, lease_expires_at_unix_ms,
                created_at_unix_ms, updated_at_unix_ms
             ) VALUES (?, ?, ?, 'required', 'active', ?, ?, ?, ?, ?, ?)
             ON CONFLICT(stream_id, consumer_id) DO UPDATE SET
               acknowledged_sequence = MAX(
                   durable_consumers.acknowledged_sequence,
                   excluded.acknowledged_sequence
               ),
               delivered_sequence = MAX(
                   durable_consumers.delivered_sequence,
                   excluded.delivered_sequence
               ),
               lease_ttl_ms = excluded.lease_ttl_ms,
               lease_expires_at_unix_ms = excluded.lease_expires_at_unix_ms,
               updated_at_unix_ms = excluded.updated_at_unix_ms",
        )
        .bind(&stream_id)
        .bind(&instance)
        .bind(consumer_id)
        .bind(u64_i64(acknowledged_sequence, "acknowledged sequence")?)
        .bind(u64_i64(acknowledged_sequence, "acknowledged sequence")?)
        .bind(ttl_ms)
        .bind(expires)
        .bind(now)
        .bind(now)
        .execute(&self.inner.pool)
        .await?;
        Ok(())
    }

    /// Evaluate finalized-block, wall-clock replay, batch, and durable
    /// acknowledgement boundaries for one processor's delivery policy.
    ///
    /// # Errors
    ///
    /// Returns an error when policy bounds cannot be represented or a
    /// database operation fails.
    #[allow(clippy::too_many_lines)]
    pub async fn prune_delivery_changes(
        &self,
        descriptor: &ProcessorDescriptor,
        finalized_through: BlockNumber,
    ) -> Result<ChangePruneOutcome, StoreError> {
        self.prune_delivery_changes_in_stream(
            descriptor,
            &default_delivery_stream_id(descriptor),
            finalized_through,
        )
        .await
    }

    /// Evaluate delivery pruning for one explicit stream.
    ///
    /// # Errors
    ///
    /// Returns an error when the stream or stored policy values are invalid or
    /// a database operation fails.
    #[allow(clippy::too_many_lines)]
    pub async fn prune_delivery_changes_in_stream(
        &self,
        descriptor: &ProcessorDescriptor,
        stream_id: &str,
        finalized_through: BlockNumber,
    ) -> Result<ChangePruneOutcome, StoreError> {
        self.validate_delivery_stream(&processor_instance(descriptor), stream_id)
            .await?;
        let terminal_backfill: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)
             FROM backfill_subscriptions
             WHERE history_stream_id = ?
               AND state IN ('draining', 'complete_reclaimable', 'cancelled', 'failed')",
        )
        .bind(stream_id)
        .fetch_one(&self.inner.pool)
        .await?;
        let stream_kind: String =
            sqlx::query_scalar("SELECT stream_kind FROM delivery_streams WHERE stream_id = ?")
                .bind(stream_id)
                .fetch_one(&self.inner.pool)
                .await?;
        let stream_kind = DeliveryStreamKind::parse(&stream_kind)?;
        let policy = &descriptor.lifecycle.delivery;
        let pruning = policy.pruning;
        let block_cutoff = finalized_through
            .0
            .saturating_sub(pruning.retain_finalized_blocks);
        let retained_age_seconds = if stream_kind == DeliveryStreamKind::Backfill
            || matches!(policy.mode, DeliveryPolicyMode::UntilAcknowledged)
        {
            pruning.retain_acknowledged_seconds
        } else {
            policy.max_age_seconds
        };
        let age_ms = retained_age_seconds
            .checked_mul(1_000)
            .ok_or(StoreError::Numeric("delivery replay age"))?;
        let created_cutoff = now_i64()?
            .checked_sub(u64_i64(age_ms, "delivery replay age")?)
            .ok_or(StoreError::Numeric("delivery replay cutoff"))?;
        let instance = processor_instance(descriptor);
        let run_state: String =
            sqlx::query_scalar("SELECT state FROM processor_runtime_state WHERE instance = ?")
                .bind(&instance)
                .fetch_one(&self.inner.pool)
                .await?;
        let stream_bytes: i64 =
            sqlx::query_scalar("SELECT live_bytes FROM delivery_streams WHERE stream_id = ?")
                .bind(stream_id)
                .fetch_one(&self.inner.pool)
                .await?;
        let storage_pressure = i64_u64(stream_bytes, "delivery stream bytes")? > policy.max_bytes
            || ProcessorRunState::parse(&run_state)? == ProcessorRunState::Paused;
        let candidate: Option<i64> = if storage_pressure {
            sqlx::query_scalar(
                "SELECT MAX(stream_sequence) FROM change_log
                 WHERE stream_id = ? AND block_number <= ?",
            )
            .bind(stream_id)
            .bind(u64_i64(block_cutoff, "delivery block cutoff")?)
            .fetch_one(&self.inner.pool)
            .await?
        } else {
            sqlx::query_scalar(
                "SELECT MAX(stream_sequence) FROM change_log
                 WHERE stream_id = ? AND block_number <= ? AND created_at_unix_ms <= ?",
            )
            .bind(stream_id)
            .bind(u64_i64(block_cutoff, "delivery block cutoff")?)
            .bind(created_cutoff)
            .fetch_one(&self.inner.pool)
            .await?
        };
        let Some(candidate) = candidate else {
            return Ok(ChangePruneOutcome {
                requested_before: 0,
                effective_before: 0,
                deleted: 0,
            });
        };
        let candidate_before = i64_u64(candidate, "delivery prune candidate")?.saturating_add(1);
        let (eligible_changes, eligible_blocks): (i64, i64) = sqlx::query_as(
            "SELECT COUNT(*), COUNT(DISTINCT block_number) FROM change_log
             WHERE stream_id = ? AND stream_sequence < ?",
        )
        .bind(stream_id)
        .bind(u64_i64(candidate_before, "delivery prune candidate")?)
        .fetch_one(&self.inner.pool)
        .await?;
        let eligible_changes = i64_u64(eligible_changes, "eligible delivery changes")?;
        let eligible_blocks = i64_u64(eligible_blocks, "eligible delivery blocks")?;
        if terminal_backfill == 0
            && !storage_pressure
            && eligible_changes < pruning.minimum_batch_changes
            && eligible_blocks < pruning.minimum_batch_blocks
        {
            return Ok(ChangePruneOutcome {
                requested_before: candidate_before,
                effective_before: candidate_before,
                deleted: 0,
            });
        }
        let requested_before = if eligible_changes > pruning.maximum_delete_changes {
            let offset = pruning.maximum_delete_changes.saturating_sub(1);
            let block_number: i64 = sqlx::query_scalar(
                "SELECT block_number FROM change_log
                 WHERE stream_id = ? AND stream_sequence < ?
                 ORDER BY stream_sequence LIMIT 1 OFFSET ?",
            )
            .bind(stream_id)
            .bind(u64_i64(candidate_before, "delivery prune candidate")?)
            .bind(u64_i64(offset, "delivery prune batch offset")?)
            .fetch_one(&self.inner.pool)
            .await?;
            let block_end: i64 = sqlx::query_scalar(
                "SELECT MAX(stream_sequence) FROM change_log
                 WHERE stream_id = ? AND block_number = ? AND stream_sequence < ?",
            )
            .bind(stream_id)
            .bind(block_number)
            .bind(u64_i64(candidate_before, "delivery prune candidate")?)
            .fetch_one(&self.inner.pool)
            .await?;
            i64_u64(block_end, "delivery block end")?.saturating_add(1)
        } else {
            candidate_before
        };
        self.prune_changes_before_in_stream(descriptor, stream_id, requested_before)
            .await
    }

    /// Delete changes older than an exclusive sequence boundary while
    /// respecting active consumer leases and retaining at least the newest
    /// change as a reset anchor.
    ///
    /// # Errors
    ///
    /// Returns an error when bounds cannot be represented or the transaction
    /// fails.
    #[allow(clippy::too_many_lines)]
    pub async fn prune_changes_before(
        &self,
        descriptor: &ProcessorDescriptor,
        requested_before: u64,
    ) -> Result<ChangePruneOutcome, StoreError> {
        self.prune_changes_before_in_stream(
            descriptor,
            &default_delivery_stream_id(descriptor),
            requested_before,
        )
        .await
    }

    /// Delete changes before a stream-scoped exclusive sequence boundary.
    ///
    /// # Errors
    ///
    /// Returns an error when the stream or boundary is invalid or the pruning
    /// transaction fails.
    #[allow(clippy::too_many_lines)]
    pub async fn prune_changes_before_in_stream(
        &self,
        descriptor: &ProcessorDescriptor,
        stream_id: &str,
        requested_before: u64,
    ) -> Result<ChangePruneOutcome, StoreError> {
        let instance = processor_instance(descriptor);
        self.validate_delivery_stream(&instance, stream_id).await?;
        let _guard = self.inner.writer.lock().await;
        let mut transaction = self.inner.pool.begin().await?;
        let protected: Option<i64> = sqlx::query_scalar(
            "SELECT MIN(
                CASE
                  WHEN acknowledged_sequence = 9223372036854775807
                  THEN acknowledged_sequence
                  ELSE acknowledged_sequence + 1
                END
             ) FROM durable_consumers
             WHERE stream_id = ? AND role = 'required' AND state = 'active'",
        )
        .bind(stream_id)
        .fetch_one(&mut *transaction)
        .await?;
        let latest: Option<i64> =
            sqlx::query_scalar("SELECT MAX(stream_sequence) FROM change_log WHERE stream_id = ?")
                .bind(stream_id)
                .fetch_one(&mut *transaction)
                .await?;
        let mut effective = requested_before;
        if let Some(protected) = protected {
            effective = effective.min(i64_u64(protected, "protected sequence")?);
        }
        if let Some(latest) = latest {
            effective = effective.min(i64_u64(latest, "latest sequence")?);
        }
        if effective > 0 {
            let trailing_block: Option<i64> = sqlx::query_scalar(
                "SELECT block_number FROM change_log
                 WHERE stream_id = ? AND stream_sequence < ?
                 ORDER BY stream_sequence DESC LIMIT 1",
            )
            .bind(stream_id)
            .bind(u64_i64(effective, "effective prune sequence")?)
            .fetch_optional(&mut *transaction)
            .await?;
            if let Some(block_number) = trailing_block {
                let (block_first, block_last): (i64, i64) = sqlx::query_as(
                    "SELECT MIN(stream_sequence), MAX(stream_sequence) FROM change_log
                     WHERE stream_id = ? AND block_number = ?",
                )
                .bind(stream_id)
                .bind(block_number)
                .fetch_one(&mut *transaction)
                .await?;
                if i64_u64(block_last, "logical block end")? >= effective {
                    effective = effective.min(i64_u64(block_first, "logical block start")?);
                }
            }
        }
        let deleted_through: Option<i64> = sqlx::query_scalar(
            "SELECT MAX(stream_sequence) FROM change_log
             WHERE stream_id = ? AND stream_sequence < ?",
        )
        .bind(stream_id)
        .bind(u64_i64(effective, "effective prune sequence")?)
        .fetch_one(&mut *transaction)
        .await?;
        let result =
            sqlx::query("DELETE FROM change_log WHERE stream_id = ? AND stream_sequence < ?")
                .bind(stream_id)
                .bind(u64_i64(effective, "effective prune sequence")?)
                .execute(&mut *transaction)
                .await?;
        if let Some(deleted_through) = deleted_through {
            sqlx::query(
                "UPDATE delivery_streams
                 SET pruned_through_sequence = MAX(pruned_through_sequence, ?)
                 WHERE stream_id = ?",
            )
            .bind(deleted_through)
            .bind(stream_id)
            .execute(&mut *transaction)
            .await?;
        }
        let live_bytes: i64 = sqlx::query_scalar(
            "SELECT COALESCE(SUM(length(entity_key) + length(payload)), 0)
             FROM change_log WHERE stream_id = ?",
        )
        .bind(stream_id)
        .fetch_one(&mut *transaction)
        .await?;
        sqlx::query("UPDATE delivery_streams SET live_bytes = ? WHERE stream_id = ?")
            .bind(live_bytes)
            .bind(stream_id)
            .execute(&mut *transaction)
            .await?;
        let low_water = descriptor.lifecycle.delivery.max_bytes.saturating_mul(9) / 10;
        if i64_u64(live_bytes, "delivery live bytes")? <= low_water {
            sqlx::query(
                "UPDATE processor_runtime_state
                 SET state = 'running', reason = NULL, updated_at_unix_ms = ?
                 WHERE instance = ? AND state = 'paused'",
            )
            .bind(now_i64()?)
            .bind(&instance)
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        self.inner.delivery_capacity_changed.notify_waiters();
        Ok(ChangePruneOutcome {
            requested_before,
            effective_before: effective,
            deleted: result.rows_affected(),
        })
    }

    /// Return the latest committed cursor for an exact processor instance.
    ///
    /// # Errors
    ///
    /// Returns an error when the database read or durable cursor validation
    /// fails.
    pub async fn processor_cursor(
        &self,
        descriptor: &ProcessorDescriptor,
    ) -> Result<Option<ProcessorCursor>, StoreError> {
        self.processor_cursor_by_instance(&processor_instance(descriptor))
            .await
    }

    async fn processor_cursor_by_instance(
        &self,
        instance: &str,
    ) -> Result<Option<ProcessorCursor>, StoreError> {
        let encoded: Option<String> =
            sqlx::query_scalar("SELECT cursor FROM processor_cursors WHERE instance = ?")
                .bind(instance)
                .fetch_optional(&self.inner.pool)
                .await?;
        encoded
            .map(|encoded| {
                OpaqueCursor::parse(encoded)?
                    .decode(CursorKind::Processor)
                    .map_err(StoreError::from)
            })
            .transpose()
    }

    /// Inspect schema and logical storage counts.
    ///
    /// # Errors
    ///
    /// Returns an error for corrupt metadata, numeric overflow, or a failed
    /// database read.
    pub async fn stats(&self) -> Result<StoreStats, StoreError> {
        let schema: Vec<u8> =
            sqlx::query_scalar("SELECT value FROM node_meta WHERE key = 'schema_version'")
                .fetch_one(&self.inner.pool)
                .await?;
        if schema.len() != 4 {
            return Err(StoreError::Invariant(
                "schema_version metadata is not four bytes".to_owned(),
            ));
        }
        let mut version = [0_u8; 4];
        version.copy_from_slice(&schema);
        let storage = self.storage_stats().await?;
        let (delivery_retained_bytes, history_delivery_retained_bytes): (i64, i64) =
            sqlx::query_as(
                "SELECT COALESCE(SUM(live_bytes), 0),
                        COALESCE(SUM(CASE WHEN stream_kind = 'backfill' THEN live_bytes ELSE 0 END), 0)
                 FROM delivery_streams",
            )
            .fetch_one(&self.inner.pool)
            .await?;
        let (
            processor_artifacts,
            processor_artifact_bytes,
            processor_artifact_owners,
            pending_processor_artifacts,
            pending_processor_artifact_bytes,
        ): (i64, i64, i64, i64, i64) = sqlx::query_as(
            "SELECT COALESCE(SUM(retained_artifacts), 0),
                    COALESCE(SUM(retained_bytes), 0),
                    COALESCE(SUM(retained_owners), 0),
                    COALESCE(SUM(pending_artifacts), 0),
                    COALESCE(SUM(pending_bytes), 0)
             FROM processor_artifact_totals",
        )
        .fetch_one(&self.inner.pool)
        .await?;
        Ok(StoreStats {
            schema_version: u32::from_be_bytes(version),
            processor_instances: table_count(&self.inner.pool, "processor_instances").await?,
            entities: table_count(&self.inner.pool, "entities").await?,
            index_entries: table_count(&self.inner.pool, "entity_indexes").await?,
            exact_coverage_blocks: table_count(&self.inner.pool, "processor_coverage").await?,
            coverage_intervals: table_count(&self.inner.pool, "finalized_coverage_intervals")
                .await?,
            coverage_segments: table_count(&self.inner.pool, "finalized_coverage_segments").await?,
            coverage_owners: table_count(&self.inner.pool, "finalized_coverage_owners").await?,
            applied_blocks: table_count(&self.inner.pool, "applied_blocks").await?,
            undo_records: table_count(&self.inner.pool, "undo_journal").await?,
            changes: table_count(&self.inner.pool, "change_log").await?,
            processor_artifacts: i64_u64(processor_artifacts, "processor artifacts")?,
            processor_artifact_bytes: i64_u64(
                processor_artifact_bytes,
                "processor artifact bytes",
            )?,
            processor_artifact_owners: i64_u64(
                processor_artifact_owners,
                "processor artifact owners",
            )?,
            pending_processor_artifacts: i64_u64(
                pending_processor_artifacts,
                "pending processor artifacts",
            )?,
            pending_processor_artifact_bytes: i64_u64(
                pending_processor_artifact_bytes,
                "pending processor artifact bytes",
            )?,
            delivery_retained_bytes: i64_u64(
                delivery_retained_bytes,
                "total retained delivery bytes",
            )?,
            history_delivery_retained_bytes: i64_u64(
                history_delivery_retained_bytes,
                "history retained delivery bytes",
            )?,
            database_bytes: storage.database_bytes,
            freelist_bytes: storage.freelist_bytes,
            wal_bytes: storage.wal_bytes,
            physical_file_bytes: storage.physical_file_bytes,
            artifact_segment_bytes: storage.artifact_segment_bytes,
            total_physical_bytes: storage.total_physical_bytes,
        })
    }

    /// Inspect the `SQLite` database and WAL footprint without scanning logical tables.
    ///
    /// # Errors
    ///
    /// Returns an error for a failed database pragma, numeric overflow, or file
    /// metadata access.
    pub async fn storage_stats(&self) -> Result<StoreStorageStats, StoreError> {
        let physical_file_bytes = file_bytes(&self.inner.path)?;
        let wal_bytes = file_bytes(&PathBuf::from(format!(
            "{}-wal",
            self.inner.path.to_string_lossy()
        )))?;
        let artifact_segment_bytes = if let Some(storage) = &self.inner.artifact_segments {
            storage.sink.stats().await.physical_bytes
        } else {
            0
        };
        Ok(StoreStorageStats {
            database_bytes: database_bytes(&self.inner.pool).await?,
            freelist_bytes: freelist_bytes(&self.inner.pool).await?,
            wal_bytes,
            physical_file_bytes,
            artifact_segment_bytes,
            total_physical_bytes: physical_file_bytes
                .saturating_add(wal_bytes)
                .saturating_add(artifact_segment_bytes),
        })
    }

    /// Inspect fast-changing byte charges without counting retained output or
    /// per-block correctness rows.
    ///
    /// # Errors
    ///
    /// Returns an error for a failed query, numeric overflow, or file metadata
    /// access.
    pub async fn budget_stats(&self) -> Result<StoreBudgetStats, StoreError> {
        let (delivery, history, pending, artifacts, pending_artifacts): (i64, i64, i64, i64, i64) =
            sqlx::query_as(
                "SELECT COALESCE(SUM(live_bytes), 0),
                    COALESCE(SUM(CASE WHEN stream_kind = 'backfill' THEN live_bytes ELSE 0 END), 0),
                    (SELECT COALESCE(SUM(length(encoded_delta)), 0) FROM pending_deltas),
                    (SELECT COALESCE(SUM(retained_bytes), 0)
                       FROM processor_artifact_totals),
                    (SELECT COALESCE(SUM(pending_bytes), 0)
                       FROM processor_artifact_totals)
             FROM delivery_streams",
            )
            .fetch_one(&self.inner.pool)
            .await?;
        let storage = self.storage_stats().await?;
        Ok(StoreBudgetStats {
            delivery_retained_bytes: i64_u64(delivery, "total retained delivery bytes")?,
            history_delivery_retained_bytes: i64_u64(history, "history retained delivery bytes")?,
            pending_delta_bytes: i64_u64(pending, "pending delta bytes")?,
            processor_artifact_bytes: i64_u64(artifacts, "processor artifact bytes")?,
            pending_processor_artifact_bytes: i64_u64(
                pending_artifacts,
                "pending processor artifact bytes",
            )?,
            maximum_processor_artifact_bytes: self.inner.artifact_budget.maximum_retained_bytes,
            maximum_pending_processor_artifact_bytes: self
                .inner
                .artifact_budget
                .maximum_pending_bytes,
            maximum_delivery_retained_bytes: self.inner.delivery_budget.maximum_retained_bytes,
            maximum_history_delivery_retained_bytes: self
                .inner
                .delivery_budget
                .maximum_history_retained_bytes,
            maximum_physical_store_bytes: self.inner.storage_budget.maximum_physical_bytes,
            database_bytes: storage.database_bytes,
            freelist_bytes: storage.freelist_bytes,
            wal_bytes: storage.wal_bytes,
            physical_file_bytes: storage.physical_file_bytes,
            artifact_segment_bytes: storage.artifact_segment_bytes,
            total_physical_bytes: storage.total_physical_bytes,
        })
    }

    /// Inspect logical storage attributable to one processor instance.
    ///
    /// # Errors
    ///
    /// Returns an error for a failed database query or numeric overflow.
    #[allow(clippy::too_many_lines)]
    pub async fn processor_stats(
        &self,
        descriptor: &ProcessorDescriptor,
    ) -> Result<ProcessorStoreStats, StoreError> {
        let instance = processor_instance(descriptor);
        let (entities, entity_bytes) = instance_count_bytes(
            &self.inner.pool,
            "SELECT COUNT(*), COALESCE(SUM(length(entity_key) + length(value)), 0)
             FROM entities WHERE instance = ?",
            &instance,
        )
        .await?;
        let (index_entries, index_bytes) = instance_count_bytes(
            &self.inner.pool,
            "SELECT COUNT(*), COALESCE(SUM(length(index_key) + length(entity_key)), 0)
             FROM entity_indexes WHERE instance = ?",
            &instance,
        )
        .await?;
        let (state_entries, state_bytes) = instance_count_bytes(
            &self.inner.pool,
            "SELECT COUNT(*), COALESCE(SUM(length(state_key) + length(value)), 0)
             FROM processor_state WHERE instance = ?",
            &instance,
        )
        .await?;
        let (undo_records, undo_bytes) = instance_count_bytes(
            &self.inner.pool,
            "SELECT COUNT(*), COALESCE(SUM(length(encoded_undo)), 0)
             FROM undo_journal WHERE instance = ?",
            &instance,
        )
        .await?;
        let (changes, change_bytes) = instance_count_bytes(
            &self.inner.pool,
            "SELECT COUNT(*), COALESCE(SUM(length(entity_key) + length(payload)), 0)
             FROM change_log WHERE instance = ?",
            &instance,
        )
        .await?;
        let (pending_deltas, pending_delta_bytes) = instance_count_bytes(
            &self.inner.pool,
            "SELECT COUNT(*), COALESCE(SUM(length(encoded_delta)), 0)
             FROM pending_deltas WHERE instance = ?",
            &instance,
        )
        .await?;
        let (processor_artifacts, processor_artifact_bytes) = instance_count_bytes(
            &self.inner.pool,
            "SELECT COALESCE((SELECT retained_artifacts
                                FROM processor_artifact_totals WHERE instance = ?1), 0),
                    COALESCE((SELECT retained_bytes
                                FROM processor_artifact_totals WHERE instance = ?1), 0)",
            &instance,
        )
        .await?;
        let (pending_processor_artifacts, pending_processor_artifact_bytes) = instance_count_bytes(
            &self.inner.pool,
            "SELECT COALESCE((SELECT pending_artifacts
                                FROM processor_artifact_totals WHERE instance = ?1), 0),
                    COALESCE((SELECT pending_bytes
                                FROM processor_artifact_totals WHERE instance = ?1), 0)",
            &instance,
        )
        .await?;
        let processor_artifact_owners: i64 = sqlx::query_scalar(
            "SELECT COALESCE((SELECT retained_owners
                                FROM processor_artifact_totals WHERE instance = ?), 0)",
        )
        .bind(&instance)
        .fetch_one(&self.inner.pool)
        .await?;
        let (recovery_checkpoints, recovery_checkpoint_bytes) = instance_count_bytes(
            &self.inner.pool,
            "SELECT COUNT(*), COALESCE(SUM(length(state_snapshot)), 0)
             FROM recovery_checkpoints WHERE instance = ?",
            &instance,
        )
        .await?;
        let (portable_savepoints, portable_savepoint_bytes) = instance_count_bytes(
            &self.inner.pool,
            "SELECT COUNT(*), COALESCE(SUM(length(state_snapshot)), 0)
             FROM portable_savepoints WHERE instance = ?",
            &instance,
        )
        .await?;
        let applied_blocks: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM applied_blocks WHERE instance = ?")
                .bind(&instance)
                .fetch_one(&self.inner.pool)
                .await?;
        let outbox_records: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sink_outbox AS outbox
             JOIN change_log AS changes ON changes.sequence = outbox.change_sequence
             WHERE changes.instance = ? AND outbox.delivered_at_unix_ms IS NULL",
        )
        .bind(&instance)
        .fetch_one(&self.inner.pool)
        .await?;
        Ok(ProcessorStoreStats {
            processor_id: descriptor.id.to_string(),
            processor_version: descriptor.version.to_string(),
            entities,
            entity_bytes,
            state_entries,
            state_bytes,
            index_entries,
            index_bytes,
            applied_blocks: i64_u64(applied_blocks, "processor applied blocks")?,
            undo_records,
            undo_bytes,
            changes,
            change_bytes,
            pending_deltas,
            pending_delta_bytes,
            processor_artifacts,
            processor_artifact_bytes,
            processor_artifact_owners: i64_u64(
                processor_artifact_owners,
                "processor artifact owners",
            )?,
            pending_processor_artifacts,
            pending_processor_artifact_bytes,
            outbox_records: i64_u64(outbox_records, "processor outbox records")?,
            recovery_checkpoints,
            recovery_checkpoint_bytes,
            portable_savepoints,
            portable_savepoint_bytes,
        })
    }

    /// Inspect the persisted running, paused, or failed lifecycle state.
    ///
    /// # Errors
    ///
    /// Returns an error when the processor is absent, stored state is invalid,
    /// or the read fails.
    pub async fn processor_runtime_state(
        &self,
        descriptor: &ProcessorDescriptor,
    ) -> Result<ProcessorRuntimeState, StoreError> {
        let instance = processor_instance(descriptor);
        let row = sqlx::query(
            "SELECT state, reason, updated_at_unix_ms
             FROM processor_runtime_state WHERE instance = ?",
        )
        .bind(&instance)
        .fetch_optional(&self.inner.pool)
        .await?
        .ok_or_else(|| {
            StoreError::Invariant(format!("processor runtime state is missing for {instance}"))
        })?;
        Ok(ProcessorRuntimeState {
            processor_instance: instance,
            state: ProcessorRunState::parse(row.try_get("state")?)?,
            reason: row.try_get("reason")?,
            updated_at_unix_ms: i64_u64(
                row.try_get("updated_at_unix_ms")?,
                "processor runtime state update",
            )?,
        })
    }

    /// Inspect the durable first-unapplied marker for one isolated live lane.
    ///
    /// # Errors
    ///
    /// Returns an error for corrupt stored values or a failed read.
    pub async fn live_lane_gap(
        &self,
        descriptor: &ProcessorDescriptor,
    ) -> Result<Option<LiveLaneGap>, StoreError> {
        let instance = processor_instance(descriptor);
        let row = sqlx::query(
            "SELECT first_unapplied_block, first_unapplied_hash,
                    first_unapplied_parent_hash, first_unapplied_timestamp,
                    required_delivery_bytes, reason,
                    created_at_unix_ms, updated_at_unix_ms
             FROM live_lane_gaps WHERE instance = ?",
        )
        .bind(&instance)
        .fetch_optional(&self.inner.pool)
        .await?;
        row.map(|row| {
            Ok(LiveLaneGap {
                processor_instance: instance,
                first_unapplied: BlockRef {
                    number: BlockNumber(i64_u64(
                        row.try_get("first_unapplied_block")?,
                        "live gap block number",
                    )?),
                    hash: decode_hash(row.try_get("first_unapplied_hash")?)?,
                    parent_hash: decode_hash(row.try_get("first_unapplied_parent_hash")?)?,
                    timestamp: i64_u64(
                        row.try_get("first_unapplied_timestamp")?,
                        "live gap timestamp",
                    )?,
                },
                required_delivery_bytes: i64_u64(
                    row.try_get("required_delivery_bytes")?,
                    "required live delivery bytes",
                )?,
                reason: row.try_get("reason")?,
                created_at_unix_ms: i64_u64(
                    row.try_get("created_at_unix_ms")?,
                    "live gap creation time",
                )?,
                updated_at_unix_ms: i64_u64(
                    row.try_get("updated_at_unix_ms")?,
                    "live gap update time",
                )?,
            })
        })
        .transpose()
    }

    /// Atomically persist the first unapplied delta, its gap marker, and the
    /// processor-local paused/failed state.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid delta/reason, an identity conflict, or
    /// a failed transaction.
    pub async fn park_processor_live_lane(
        &self,
        descriptor: &ProcessorDescriptor,
        delta: &EncodedDelta,
        reason: &str,
        required_delivery_bytes: u64,
        failed: bool,
        persist_delta: bool,
    ) -> Result<(), StoreError> {
        if reason.trim().is_empty() {
            return Err(StoreError::InvalidConfig(
                "processor live-lane park reason must not be empty".to_owned(),
            ));
        }
        delta.validate(descriptor)?;
        let instance = self.register_processor(descriptor).await?;
        let encoded = delta.encode_durable()?;
        let now = now_i64()?;
        let _guard = self.inner.writer.lock().await;
        let mut transaction = self.inner.pool.begin().await?;
        if persist_delta {
            sqlx::query(
                "INSERT INTO pending_deltas(
                    instance, block_number, block_hash, encoded_delta, inserted_at_unix_ms
                 ) VALUES (?, ?, ?, ?, ?)
                 ON CONFLICT(instance, block_number, block_hash) DO NOTHING",
            )
            .bind(&instance)
            .bind(u64_i64(delta.block.number.0, "live gap block number")?)
            .bind(delta.block.hash.0.as_slice())
            .bind(&encoded)
            .bind(now)
            .execute(&mut *transaction)
            .await?;
            let stored: Vec<u8> = sqlx::query_scalar(
                "SELECT encoded_delta FROM pending_deltas
                 WHERE instance = ? AND block_number = ? AND block_hash = ?",
            )
            .bind(&instance)
            .bind(u64_i64(delta.block.number.0, "live gap block number")?)
            .bind(delta.block.hash.0.as_slice())
            .fetch_one(&mut *transaction)
            .await?;
            if stored != encoded {
                return Err(StoreError::ConflictingPendingDelta(delta.block.number));
            }
        }
        sqlx::query(
            "INSERT INTO live_lane_gaps(
                instance, first_unapplied_block, first_unapplied_hash,
                first_unapplied_parent_hash, first_unapplied_timestamp,
                required_delivery_bytes, reason, created_at_unix_ms, updated_at_unix_ms
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(instance) DO NOTHING",
        )
        .bind(&instance)
        .bind(u64_i64(delta.block.number.0, "live gap block number")?)
        .bind(delta.block.hash.0.as_slice())
        .bind(delta.block.parent_hash.0.as_slice())
        .bind(u64_i64(delta.block.timestamp, "live gap timestamp")?)
        .bind(u64_i64(
            required_delivery_bytes,
            "required live delivery bytes",
        )?)
        .bind(reason)
        .bind(now)
        .bind(now)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE processor_runtime_state
             SET state = ?, reason = ?, updated_at_unix_ms = ?
             WHERE instance = ?",
        )
        .bind(if failed { "failed" } else { "paused" })
        .bind(reason)
        .bind(now)
        .bind(&instance)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Atomically persist a first-unapplied marker and failed/paused lane state
    /// when processor mapping failed before a durable delta existed.
    ///
    /// The canonical recent frame remains the replay input. This keeps a
    /// processor-local failure from terminating shared live/RPC ingestion.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty reason or a failed transaction.
    pub async fn park_processor_live_lane_at(
        &self,
        descriptor: &ProcessorDescriptor,
        first_unapplied: BlockRef,
        reason: &str,
        failed: bool,
    ) -> Result<(), StoreError> {
        if reason.trim().is_empty() {
            return Err(StoreError::InvalidConfig(
                "processor live-lane park reason must not be empty".to_owned(),
            ));
        }
        let instance = self.register_processor(descriptor).await?;
        let now = now_i64()?;
        let _guard = self.inner.writer.lock().await;
        let mut transaction = self.inner.pool.begin().await?;
        sqlx::query(
            "INSERT INTO live_lane_gaps(
                instance, first_unapplied_block, first_unapplied_hash,
                first_unapplied_parent_hash, first_unapplied_timestamp,
                required_delivery_bytes, reason, created_at_unix_ms, updated_at_unix_ms
             ) VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?)
             ON CONFLICT(instance) DO NOTHING",
        )
        .bind(&instance)
        .bind(u64_i64(first_unapplied.number.0, "live gap block number")?)
        .bind(first_unapplied.hash.0.as_slice())
        .bind(first_unapplied.parent_hash.0.as_slice())
        .bind(u64_i64(first_unapplied.timestamp, "live gap timestamp")?)
        .bind(reason)
        .bind(now)
        .bind(now)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE processor_runtime_state
             SET state = ?, reason = ?, updated_at_unix_ms = ?
             WHERE instance = ?",
        )
        .bind(if failed { "failed" } else { "paused" })
        .bind(reason)
        .bind(now)
        .bind(&instance)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Advance the durable first-unapplied marker after one recovered block.
    ///
    /// # Errors
    ///
    /// Returns an error when the current marker changed or the write fails.
    pub async fn advance_live_lane_gap(
        &self,
        descriptor: &ProcessorDescriptor,
        applied: BlockRef,
        next: BlockRef,
    ) -> Result<(), StoreError> {
        let instance = processor_instance(descriptor);
        let _guard = self.inner.writer.lock().await;
        let updated = sqlx::query(
            "UPDATE live_lane_gaps
             SET first_unapplied_block = ?, first_unapplied_hash = ?,
                 first_unapplied_parent_hash = ?, first_unapplied_timestamp = ?,
                 updated_at_unix_ms = ?
             WHERE instance = ? AND first_unapplied_block = ? AND first_unapplied_hash = ?",
        )
        .bind(u64_i64(next.number.0, "next live gap block")?)
        .bind(next.hash.0.as_slice())
        .bind(next.parent_hash.0.as_slice())
        .bind(u64_i64(next.timestamp, "next live gap timestamp")?)
        .bind(now_i64()?)
        .bind(&instance)
        .bind(u64_i64(applied.number.0, "applied live gap block")?)
        .bind(applied.hash.0.as_slice())
        .execute(&self.inner.pool)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(StoreError::Invariant(format!(
                "live gap marker changed while advancing {instance}"
            )));
        }
        Ok(())
    }

    /// Rebind a parked first-unapplied block to the canonical replacement
    /// supplied by a shallow live reorg.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid replacement delta, a conflicting
    /// pending encoding, or a failed transaction.
    pub async fn rebase_live_lane_gap(
        &self,
        descriptor: &ProcessorDescriptor,
        reverted: BlockRef,
        replacement: &EncodedDelta,
    ) -> Result<bool, StoreError> {
        replacement.validate(descriptor)?;
        if replacement.block.number != reverted.number {
            return Err(StoreError::InvalidConfig(
                "live gap replacement must retain the reverted block number".to_owned(),
            ));
        }
        let instance = processor_instance(descriptor);
        let encoded = replacement.encode_durable()?;
        let now = now_i64()?;
        let _guard = self.inner.writer.lock().await;
        let mut transaction = self.inner.pool.begin().await?;
        let updated = sqlx::query(
            "UPDATE live_lane_gaps
             SET first_unapplied_hash = ?, first_unapplied_parent_hash = ?,
                 first_unapplied_timestamp = ?, updated_at_unix_ms = ?
             WHERE instance = ? AND first_unapplied_block = ? AND first_unapplied_hash = ?",
        )
        .bind(replacement.block.hash.0.as_slice())
        .bind(replacement.block.parent_hash.0.as_slice())
        .bind(u64_i64(
            replacement.block.timestamp,
            "replacement live gap timestamp",
        )?)
        .bind(now)
        .bind(&instance)
        .bind(u64_i64(reverted.number.0, "reverted live gap block")?)
        .bind(reverted.hash.0.as_slice())
        .execute(&mut *transaction)
        .await?;
        if updated.rows_affected() == 0 {
            transaction.rollback().await?;
            return Ok(false);
        }
        sqlx::query(
            "DELETE FROM pending_deltas
             WHERE instance = ? AND block_number = ? AND block_hash = ?",
        )
        .bind(&instance)
        .bind(u64_i64(reverted.number.0, "reverted live gap block")?)
        .bind(reverted.hash.0.as_slice())
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "INSERT INTO pending_deltas(
                instance, block_number, block_hash, encoded_delta, inserted_at_unix_ms
             ) VALUES (?, ?, ?, ?, ?)
             ON CONFLICT(instance, block_number, block_hash)
             DO UPDATE SET encoded_delta = excluded.encoded_delta,
                           inserted_at_unix_ms = excluded.inserted_at_unix_ms",
        )
        .bind(&instance)
        .bind(u64_i64(
            replacement.block.number.0,
            "replacement live gap block",
        )?)
        .bind(replacement.block.hash.0.as_slice())
        .bind(encoded)
        .bind(now)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(true)
    }

    /// Clear a live gap only after its current first-unapplied marker has been
    /// proven to lie beyond the retained canonical head.
    ///
    /// # Errors
    ///
    /// Returns an error when the marker changed or the transaction fails.
    pub async fn complete_live_lane_gap(
        &self,
        descriptor: &ProcessorDescriptor,
        expected: BlockRef,
    ) -> Result<(), StoreError> {
        let instance = processor_instance(descriptor);
        let _guard = self.inner.writer.lock().await;
        let mut transaction = self.inner.pool.begin().await?;
        let deleted = sqlx::query(
            "DELETE FROM live_lane_gaps
             WHERE instance = ? AND first_unapplied_block = ? AND first_unapplied_hash = ?",
        )
        .bind(&instance)
        .bind(u64_i64(expected.number.0, "completed live gap block")?)
        .bind(expected.hash.0.as_slice())
        .execute(&mut *transaction)
        .await?;
        if deleted.rows_affected() != 1 {
            return Err(StoreError::Invariant(format!(
                "live gap marker changed while completing {instance}"
            )));
        }
        sqlx::query(
            "UPDATE processor_runtime_state
             SET state = 'running', reason = NULL, updated_at_unix_ms = ?
             WHERE instance = ? AND state = 'paused'",
        )
        .bind(now_i64()?)
        .bind(instance)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Explicitly reset a failed live lane after validating that its measured
    /// indivisible item fits the processor's current delivery limit.
    ///
    /// The durable gap is retained and the lane moves to `paused`; the shared
    /// live runtime then replays it before accepting new direct commits.
    ///
    /// # Errors
    ///
    /// Returns an error when the lane is not failed, has no recovery marker,
    /// still cannot fit, or the update fails.
    pub async fn reset_failed_live_lane(
        &self,
        descriptor: &ProcessorDescriptor,
    ) -> Result<LiveLaneGap, StoreError> {
        let gap = self.live_lane_gap(descriptor).await?.ok_or_else(|| {
            StoreError::InvalidConfig(format!(
                "processor {} has no durable live gap to reset",
                descriptor.instance
            ))
        })?;
        if gap.required_delivery_bytes > descriptor.lifecycle.delivery.max_bytes {
            return Err(StoreError::DeliveryItemTooLarge {
                instance: gap.processor_instance.clone(),
                observed_bytes: gap.required_delivery_bytes,
                maximum_bytes: descriptor.lifecycle.delivery.max_bytes,
                observed_work_blocks: 1,
                maximum_work_blocks: None,
            });
        }
        let _guard = self.inner.writer.lock().await;
        let updated = sqlx::query(
            "UPDATE processor_runtime_state
             SET state = 'paused', reason = 'operator_reset_pending_replay',
                 updated_at_unix_ms = ?
             WHERE instance = ? AND state = 'failed'",
        )
        .bind(now_i64()?)
        .bind(&gap.processor_instance)
        .execute(&self.inner.pool)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(StoreError::InvalidConfig(format!(
                "processor {} live lane is not failed",
                descriptor.instance
            )));
        }
        self.inner.delivery_capacity_changed.notify_waiters();
        Ok(gap)
    }

    /// Fail one processor commit lane without affecting shared ingestion or
    /// unrelated processors.
    ///
    /// # Errors
    ///
    /// Returns an error when the processor is absent or the state write fails.
    pub async fn fail_processor_live_lane(
        &self,
        descriptor: &ProcessorDescriptor,
        reason: &str,
    ) -> Result<(), StoreError> {
        if reason.trim().is_empty() {
            return Err(StoreError::InvalidConfig(
                "processor live-lane failure reason must not be empty".to_owned(),
            ));
        }
        let instance = processor_instance(descriptor);
        let _guard = self.inner.writer.lock().await;
        let updated = sqlx::query(
            "UPDATE processor_runtime_state
             SET state = 'failed', reason = ?, updated_at_unix_ms = ?
             WHERE instance = ?",
        )
        .bind(reason)
        .bind(now_i64()?)
        .bind(&instance)
        .execute(&self.inner.pool)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(StoreError::Invariant(format!(
                "processor runtime state is missing for {instance}"
            )));
        }
        Ok(())
    }

    /// Pause one processor commit lane while shared ingestion continues.
    ///
    /// # Errors
    ///
    /// Returns an error when the processor is absent or the state write fails.
    pub async fn pause_processor_live_lane(
        &self,
        descriptor: &ProcessorDescriptor,
        reason: &str,
    ) -> Result<(), StoreError> {
        if reason.trim().is_empty() {
            return Err(StoreError::InvalidConfig(
                "processor live-lane pause reason must not be empty".to_owned(),
            ));
        }
        let instance = processor_instance(descriptor);
        let _guard = self.inner.writer.lock().await;
        let updated = sqlx::query(
            "UPDATE processor_runtime_state
             SET state = 'paused', reason = ?, updated_at_unix_ms = ?
             WHERE instance = ? AND state != 'failed'",
        )
        .bind(reason)
        .bind(now_i64()?)
        .bind(&instance)
        .execute(&self.inner.pool)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(StoreError::ProcessorFailed(instance));
        }
        Ok(())
    }

    /// Mark a successfully replaying processor lane ready again.
    ///
    /// # Errors
    ///
    /// Returns an error when the state write fails.
    pub async fn resume_processor_live_lane(
        &self,
        descriptor: &ProcessorDescriptor,
    ) -> Result<(), StoreError> {
        let instance = processor_instance(descriptor);
        let _guard = self.inner.writer.lock().await;
        sqlx::query(
            "UPDATE processor_runtime_state
             SET state = 'running', reason = NULL, updated_at_unix_ms = ?
             WHERE instance = ? AND state = 'paused'",
        )
        .bind(now_i64()?)
        .bind(instance)
        .execute(&self.inner.pool)
        .await?;
        Ok(())
    }

    /// Verify `SQLite` integrity and decode every durable cursor/undo record.
    ///
    /// # Errors
    ///
    /// Returns an error when integrity checking, reading, or durable decoding
    /// fails.
    pub async fn verify(&self) -> Result<(), StoreError> {
        let result: String = sqlx::query_scalar("PRAGMA integrity_check")
            .fetch_one(&self.inner.pool)
            .await?;
        if result != "ok" {
            return Err(StoreError::Integrity(result));
        }
        let cursors: Vec<String> = sqlx::query_scalar("SELECT cursor FROM processor_cursors")
            .fetch_all(&self.inner.pool)
            .await?;
        for cursor in cursors {
            let _: ProcessorCursor = OpaqueCursor::parse(cursor)?.decode(CursorKind::Processor)?;
        }
        let records: Vec<Vec<u8>> = sqlx::query_scalar("SELECT encoded_undo FROM undo_journal")
            .fetch_all(&self.inner.pool)
            .await?;
        for bytes in records {
            let _: UndoRecord = postcard::from_bytes(&bytes)
                .map_err(|error| StoreError::Encoding(error.to_string()))?;
        }
        let inconsistent_artifact_totals: Option<String> = sqlx::query_scalar(
            "SELECT processors.instance
             FROM processor_instances AS processors
             LEFT JOIN processor_artifact_totals AS totals
               ON totals.instance = processors.instance
             WHERE totals.instance IS NULL
                OR totals.retained_artifacts !=
                   (SELECT COUNT(*) FROM processor_artifacts AS artifacts
                    WHERE artifacts.instance = processors.instance) +
                   (SELECT COALESCE(SUM(segments.artifacts), 0)
                    FROM processor_artifact_segments AS segments
                    WHERE segments.instance = processors.instance AND segments.state = 'active')
                OR totals.retained_bytes !=
                   (SELECT COALESCE(SUM(artifacts.encoded_bytes), 0)
                    FROM processor_artifacts AS artifacts
                    WHERE artifacts.instance = processors.instance) +
                   (SELECT COALESCE(SUM(segments.logical_bytes), 0)
                    FROM processor_artifact_segments AS segments
                    WHERE segments.instance = processors.instance AND segments.state = 'active')
                OR totals.retained_owners !=
                   (SELECT COUNT(*) FROM processor_artifact_owners AS owners
                    WHERE owners.instance = processors.instance) +
                   (SELECT COALESCE(SUM(owners.to_block - owners.from_block + 1), 0)
                    FROM processor_artifact_segment_owners AS owners
                    JOIN processor_artifact_segments AS segments
                      ON segments.segment_id = owners.segment_id
                    WHERE segments.instance = processors.instance AND segments.state = 'active')
                OR totals.pending_artifacts !=
                   (SELECT COUNT(*) FROM processor_artifact_candidates AS candidates
                    WHERE candidates.instance = processors.instance)
                OR totals.pending_bytes !=
                   (SELECT COALESCE(SUM(length(candidates.encoded_delta)), 0)
                    FROM processor_artifact_candidates AS candidates
                    WHERE candidates.instance = processors.instance)
             LIMIT 1",
        )
        .fetch_optional(&self.inner.pool)
        .await?;
        if let Some(instance) = inconsistent_artifact_totals {
            return Err(StoreError::Invariant(format!(
                "processor artifact totals disagree with retained rows for {instance}"
            )));
        }
        let incomplete_bulk_accounting: Option<String> =
            sqlx::query_scalar("SELECT instance FROM processor_artifact_bulk_accounting LIMIT 1")
                .fetch_optional(&self.inner.pool)
                .await?;
        if let Some(instance) = incomplete_bulk_accounting {
            return Err(StoreError::Invariant(format!(
                "processor artifact bulk accounting remained active for {instance}"
            )));
        }
        if let Some(storage) = &self.inner.artifact_segments {
            let descriptors: Vec<String> = sqlx::query_scalar(
                "SELECT descriptor_json FROM processor_instances AS processors
                 WHERE EXISTS (
                   SELECT 1 FROM processor_artifact_segments AS segments
                   WHERE segments.instance = processors.instance
                 )",
            )
            .fetch_all(&self.inner.pool)
            .await?;
            for encoded in descriptors {
                let descriptor: ProcessorDescriptor = serde_json::from_str(&encoded)?;
                storage
                    .sink
                    .verify(&descriptor)
                    .await
                    .map_err(|error| StoreError::ArtifactSegment(error.to_string()))?;
            }
        }
        Ok(())
    }

    /// Checkpoint the WAL and create a consistent `SQLite` backup using
    /// `VACUUM INTO`.
    ///
    /// # Errors
    ///
    /// Returns an error when the destination already exists or the checkpoint
    /// or backup operation fails.
    pub async fn backup(&self, destination: &Path) -> Result<(), StoreError> {
        if destination.exists() {
            return Err(StoreError::DestinationExists(destination.to_path_buf()));
        }
        if let Some(storage) = &self.inner.artifact_segments {
            let segments = storage.sink.stats().await.segments;
            if segments > 0 {
                return Err(StoreError::ArtifactSegmentBackupUnsupported { segments });
            }
        }
        if let Some(parent) = destination.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }
        let _guard = self.inner.writer.lock().await;
        sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
            .execute(&self.inner.pool)
            .await?;
        let escaped = destination.to_string_lossy().replace('\'', "''");
        sqlx::query(&format!("VACUUM INTO '{escaped}'"))
            .execute(&self.inner.pool)
            .await?;
        Ok(())
    }

    /// Run a truncating WAL checkpoint and reclaim free pages.
    ///
    /// # Errors
    ///
    /// Returns an error when checkpointing or compaction fails.
    pub async fn compact(&self) -> Result<(), StoreError> {
        let _guard = self.inner.writer.lock().await;
        sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
            .execute(&self.inner.pool)
            .await?;
        sqlx::query("VACUUM").execute(&self.inner.pool).await?;
        // VACUUM rewrites the database through WAL mode. Truncate that rewrite
        // before returning so an explicit compaction cannot leave a second,
        // database-sized physical copy behind in the WAL.
        sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
            .execute(&self.inner.pool)
            .await?;
        Ok(())
    }

    /// Reclaim a bounded number of trailing freelist pages from an
    /// incremental-auto-vacuum database.
    ///
    /// This is intended for background pruning workers. It never runs on the
    /// acknowledgement request path and yields the writer after one bounded
    /// reclamation pass.
    ///
    /// # Errors
    ///
    /// Returns an error for a zero limit or a failed `SQLite` operation.
    pub async fn reclaim_free_pages(&self, maximum_pages: u32) -> Result<(), StoreError> {
        if maximum_pages == 0 {
            return Err(StoreError::InvalidConfig(
                "incremental vacuum page limit must be greater than zero".to_owned(),
            ));
        }
        let _guard = self.inner.writer.lock_history().await;
        sqlx::query(&format!("PRAGMA incremental_vacuum({maximum_pages})"))
            .execute(&self.inner.pool)
            .await?;
        Ok(())
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    /// Stable random-looking identity generated when this database is created.
    #[must_use]
    pub fn epoch(&self) -> [u8; 16] {
        self.inner.epoch
    }

    /// Wait until acknowledgement or pruning may have released delivery
    /// capacity. Callers must always re-check admission after waking.
    pub async fn wait_for_delivery_capacity_change(&self) {
        self.inner.delivery_capacity_changed.notified().await;
    }

    /// Wait until a committed delivery transaction may have advanced a
    /// stream. Callers always re-read their scoped cursor after waking.
    pub async fn wait_for_delivery_changes(&self) {
        self.inner.delivery_changes_available.notified().await;
    }

    /// Whether the physical store and artifact layers are below their shared
    /// low-water marks and storage-paused work may be retried.
    ///
    /// # Errors
    ///
    /// Returns an error when file metadata cannot be inspected.
    pub async fn storage_below_low_water(&self) -> Result<bool, StoreError> {
        let storage = self.storage_stats().await?;
        if storage.total_physical_bytes
            > self
                .inner
                .storage_budget
                .maximum_physical_bytes
                .saturating_mul(9)
                / 10
        {
            return Ok(false);
        }
        let stats = self.budget_stats().await?;
        Ok(stats.processor_artifact_bytes
            <= self
                .inner
                .artifact_budget
                .maximum_retained_bytes
                .saturating_mul(9)
                / 10
            && stats.pending_processor_artifact_bytes
                <= self
                    .inner
                    .artifact_budget
                    .maximum_pending_bytes
                    .saturating_mul(9)
                    / 10)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
enum Mutation {
    Entity {
        collection: String,
        key: Vec<u8>,
        before: Option<Vec<u8>>,
        after: Option<Vec<u8>>,
    },
    Index {
        index: String,
        index_key: Vec<u8>,
        entity_key: Vec<u8>,
        before: bool,
        after: bool,
    },
    State {
        namespace: String,
        key: Vec<u8>,
        before: Option<Vec<u8>>,
        after: Option<Vec<u8>>,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct UndoRecord {
    mutations: Vec<Mutation>,
    inverse_changes: Vec<DomainChange>,
    prior_cursor: Option<ProcessorCursor>,
    block: BlockRef,
    finality: Finality,
}

#[derive(Debug)]
struct MutationBatch {
    mutations: Vec<Mutation>,
    changes: Vec<DomainChange>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct EntityKey {
    collection: String,
    key: Vec<u8>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct StateKey {
    namespace: String,
    key: Vec<u8>,
}

#[allow(clippy::struct_field_names)]
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct IndexKey {
    index: String,
    index_key: Vec<u8>,
    entity_key: Vec<u8>,
}

#[derive(Clone, Debug)]
struct OverlayValue<T> {
    before: T,
    after: T,
}

/// In-memory reducer view with lazy first-preimage capture.
#[derive(Debug)]
pub struct ReducerOverlay {
    pool: SqlitePool,
    instance: String,
    state: BTreeMap<StateKey, OverlayValue<Option<Vec<u8>>>>,
    entities: BTreeMap<EntityKey, OverlayValue<Option<Vec<u8>>>>,
    indexes: BTreeMap<IndexKey, OverlayValue<bool>>,
    changes: Vec<DomainChange>,
}

impl ReducerOverlay {
    fn new(pool: SqlitePool, instance: String) -> Self {
        Self {
            pool,
            instance,
            state: BTreeMap::new(),
            entities: BTreeMap::new(),
            indexes: BTreeMap::new(),
            changes: Vec::new(),
        }
    }

    async fn load_state(&mut self, namespace: &str, key: &[u8]) -> Result<(), ProcessorError> {
        let state_key = StateKey {
            namespace: namespace.to_owned(),
            key: key.to_vec(),
        };
        if self.state.contains_key(&state_key) {
            return Ok(());
        }
        let value: Option<Vec<u8>> = sqlx::query_scalar(
            "SELECT value FROM processor_state
             WHERE instance = ? AND namespace = ? AND state_key = ?",
        )
        .bind(&self.instance)
        .bind(namespace)
        .bind(key)
        .fetch_optional(&self.pool)
        .await
        .map_err(state_error)?;
        self.state.insert(
            state_key,
            OverlayValue {
                before: value.clone(),
                after: value,
            },
        );
        Ok(())
    }

    async fn load_entity(&mut self, collection: &str, key: &[u8]) -> Result<(), ProcessorError> {
        let entity_key = EntityKey {
            collection: collection.to_owned(),
            key: key.to_vec(),
        };
        if self.entities.contains_key(&entity_key) {
            return Ok(());
        }
        let value: Option<Vec<u8>> = sqlx::query_scalar(
            "SELECT value FROM entities
             WHERE instance = ? AND collection = ? AND entity_key = ?",
        )
        .bind(&self.instance)
        .bind(collection)
        .bind(key)
        .fetch_optional(&self.pool)
        .await
        .map_err(state_error)?;
        self.entities.insert(
            entity_key,
            OverlayValue {
                before: value.clone(),
                after: value,
            },
        );
        Ok(())
    }

    async fn load_index(
        &mut self,
        index: &str,
        index_key: &[u8],
        entity_key: &[u8],
    ) -> Result<(), ProcessorError> {
        let key = IndexKey {
            index: index.to_owned(),
            index_key: index_key.to_vec(),
            entity_key: entity_key.to_vec(),
        };
        if self.indexes.contains_key(&key) {
            return Ok(());
        }
        let exists: i64 = sqlx::query_scalar(
            "SELECT EXISTS(
                SELECT 1 FROM entity_indexes
                WHERE instance = ? AND index_name = ? AND index_key = ? AND entity_key = ?
             )",
        )
        .bind(&self.instance)
        .bind(index)
        .bind(index_key)
        .bind(entity_key)
        .fetch_one(&self.pool)
        .await
        .map_err(state_error)?;
        self.indexes.insert(
            key,
            OverlayValue {
                before: exists != 0,
                after: exists != 0,
            },
        );
        Ok(())
    }

    fn into_batch(self) -> MutationBatch {
        let mut mutations =
            Vec::with_capacity(self.state.len() + self.entities.len() + self.indexes.len());
        for (key, value) in self.entities {
            if value.before != value.after {
                mutations.push(Mutation::Entity {
                    collection: key.collection,
                    key: key.key,
                    before: value.before,
                    after: value.after,
                });
            }
        }
        for (key, value) in self.indexes {
            if value.before != value.after {
                mutations.push(Mutation::Index {
                    index: key.index,
                    index_key: key.index_key,
                    entity_key: key.entity_key,
                    before: value.before,
                    after: value.after,
                });
            }
        }
        for (key, value) in self.state {
            if value.before != value.after {
                mutations.push(Mutation::State {
                    namespace: key.namespace,
                    key: key.key,
                    before: value.before,
                    after: value.after,
                });
            }
        }
        MutationBatch {
            mutations,
            changes: self.changes,
        }
    }

    fn take_batch(&mut self) -> MutationBatch {
        let mut mutations =
            Vec::with_capacity(self.state.len() + self.entities.len() + self.indexes.len());
        for (key, value) in &mut self.entities {
            if value.before != value.after {
                mutations.push(Mutation::Entity {
                    collection: key.collection.clone(),
                    key: key.key.clone(),
                    before: value.before.clone(),
                    after: value.after.clone(),
                });
                value.before.clone_from(&value.after);
            }
        }
        for (key, value) in &mut self.indexes {
            if value.before != value.after {
                mutations.push(Mutation::Index {
                    index: key.index.clone(),
                    index_key: key.index_key.clone(),
                    entity_key: key.entity_key.clone(),
                    before: value.before,
                    after: value.after,
                });
                value.before = value.after;
            }
        }
        for (key, value) in &mut self.state {
            if value.before != value.after {
                mutations.push(Mutation::State {
                    namespace: key.namespace.clone(),
                    key: key.key.clone(),
                    before: value.before.clone(),
                    after: value.after.clone(),
                });
                value.before.clone_from(&value.after);
            }
        }
        MutationBatch {
            mutations,
            changes: std::mem::take(&mut self.changes),
        }
    }
}

#[async_trait]
impl ReducerTransaction for ReducerOverlay {
    async fn state_get(
        &mut self,
        namespace: &str,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, ProcessorError> {
        self.load_state(namespace, key).await?;
        Ok(self
            .state
            .get(&StateKey {
                namespace: namespace.to_owned(),
                key: key.to_vec(),
            })
            .and_then(|value| value.after.clone()))
    }

    async fn state_put(
        &mut self,
        namespace: &str,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), ProcessorError> {
        self.load_state(namespace, &key).await?;
        self.state
            .get_mut(&StateKey {
                namespace: namespace.to_owned(),
                key,
            })
            .expect("loaded processor state exists")
            .after = Some(value);
        Ok(())
    }

    async fn state_delete(&mut self, namespace: &str, key: &[u8]) -> Result<(), ProcessorError> {
        self.load_state(namespace, key).await?;
        self.state
            .get_mut(&StateKey {
                namespace: namespace.to_owned(),
                key: key.to_vec(),
            })
            .expect("loaded processor state exists")
            .after = None;
        Ok(())
    }

    async fn state_scan_prefix(
        &mut self,
        namespace: &str,
        prefix: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ProcessorError> {
        if limit == 0 || limit > 10_000 {
            return Err(ProcessorError::State(
                "scan limit must be in 1..=10000".to_owned(),
            ));
        }
        let upper = prefix_upper_bound(prefix);
        let rows = match upper {
            Some(upper) => sqlx::query(
                "SELECT state_key, value FROM processor_state
                 WHERE instance = ? AND namespace = ?
                   AND state_key >= ? AND state_key < ?
                 ORDER BY state_key LIMIT ?",
            )
            .bind(&self.instance)
            .bind(namespace)
            .bind(prefix)
            .bind(upper)
            .bind(
                usize_i64(limit, "limit")
                    .map_err(|error| ProcessorError::State(error.to_string()))?,
            )
            .fetch_all(&self.pool)
            .await
            .map_err(state_error)?,
            None => sqlx::query(
                "SELECT state_key, value FROM processor_state
                 WHERE instance = ? AND namespace = ? AND state_key >= ?
                 ORDER BY state_key LIMIT ?",
            )
            .bind(&self.instance)
            .bind(namespace)
            .bind(prefix)
            .bind(
                usize_i64(limit, "limit")
                    .map_err(|error| ProcessorError::State(error.to_string()))?,
            )
            .fetch_all(&self.pool)
            .await
            .map_err(state_error)?,
        };
        let mut merged = BTreeMap::new();
        for row in rows {
            let key: Vec<u8> = row.try_get("state_key").map_err(state_error)?;
            let value: Vec<u8> = row.try_get("value").map_err(state_error)?;
            merged.insert(key, value);
        }
        for (key, value) in &self.state {
            if key.namespace == namespace && key.key.starts_with(prefix) {
                match &value.after {
                    Some(value) => {
                        merged.insert(key.key.clone(), value.clone());
                    }
                    None => {
                        merged.remove(&key.key);
                    }
                }
            }
        }
        Ok(merged.into_iter().take(limit).collect())
    }

    async fn get(
        &mut self,
        collection: &str,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, ProcessorError> {
        self.load_entity(collection, key).await?;
        Ok(self
            .entities
            .get(&EntityKey {
                collection: collection.to_owned(),
                key: key.to_vec(),
            })
            .and_then(|value| value.after.clone()))
    }

    async fn put(
        &mut self,
        collection: &str,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), ProcessorError> {
        self.load_entity(collection, &key).await?;
        self.entities
            .get_mut(&EntityKey {
                collection: collection.to_owned(),
                key,
            })
            .expect("loaded entity exists")
            .after = Some(value);
        Ok(())
    }

    async fn delete(&mut self, collection: &str, key: &[u8]) -> Result<(), ProcessorError> {
        self.load_entity(collection, key).await?;
        self.entities
            .get_mut(&EntityKey {
                collection: collection.to_owned(),
                key: key.to_vec(),
            })
            .expect("loaded entity exists")
            .after = None;
        Ok(())
    }

    async fn index_put(
        &mut self,
        index: &str,
        index_key: Vec<u8>,
        entity_key: Vec<u8>,
    ) -> Result<(), ProcessorError> {
        self.load_index(index, &index_key, &entity_key).await?;
        self.indexes
            .get_mut(&IndexKey {
                index: index.to_owned(),
                index_key,
                entity_key,
            })
            .expect("loaded index exists")
            .after = true;
        Ok(())
    }

    async fn index_delete(
        &mut self,
        index: &str,
        index_key: &[u8],
        entity_key: &[u8],
    ) -> Result<(), ProcessorError> {
        self.load_index(index, index_key, entity_key).await?;
        self.indexes
            .get_mut(&IndexKey {
                index: index.to_owned(),
                index_key: index_key.to_vec(),
                entity_key: entity_key.to_vec(),
            })
            .expect("loaded index exists")
            .after = false;
        Ok(())
    }

    async fn scan_prefix(
        &mut self,
        collection: &str,
        prefix: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ProcessorError> {
        if limit == 0 || limit > 10_000 {
            return Err(ProcessorError::State(
                "scan limit must be in 1..=10000".to_owned(),
            ));
        }
        let upper = prefix_upper_bound(prefix);
        let rows = match upper {
            Some(upper) => sqlx::query(
                "SELECT entity_key, value FROM entities
                     WHERE instance = ? AND collection = ?
                       AND entity_key >= ? AND entity_key < ?
                     ORDER BY entity_key LIMIT ?",
            )
            .bind(&self.instance)
            .bind(collection)
            .bind(prefix)
            .bind(upper)
            .bind(
                usize_i64(limit, "limit")
                    .map_err(|error| ProcessorError::State(error.to_string()))?,
            )
            .fetch_all(&self.pool)
            .await
            .map_err(state_error)?,
            None => sqlx::query(
                "SELECT entity_key, value FROM entities
                     WHERE instance = ? AND collection = ? AND entity_key >= ?
                     ORDER BY entity_key LIMIT ?",
            )
            .bind(&self.instance)
            .bind(collection)
            .bind(prefix)
            .bind(
                usize_i64(limit, "limit")
                    .map_err(|error| ProcessorError::State(error.to_string()))?,
            )
            .fetch_all(&self.pool)
            .await
            .map_err(state_error)?,
        };
        let mut merged = BTreeMap::new();
        for row in rows {
            let key: Vec<u8> = row.try_get("entity_key").map_err(state_error)?;
            let value: Vec<u8> = row.try_get("value").map_err(state_error)?;
            merged.insert(key, value);
        }
        for (key, value) in &self.entities {
            if key.collection == collection && key.key.starts_with(prefix) {
                match &value.after {
                    Some(value) => {
                        merged.insert(key.key.clone(), value.clone());
                    }
                    None => {
                        merged.remove(&key.key);
                    }
                }
            }
        }
        Ok(merged.into_iter().take(limit).collect())
    }

    async fn emit(&mut self, change: DomainChange) -> Result<(), ProcessorError> {
        self.changes.push(change);
        Ok(())
    }
}

#[async_trait]
impl ProcessorArtifactStore for SqliteStore {
    async fn retain_finalized_artifact(
        &self,
        descriptor: &ProcessorDescriptor,
        delta: &EncodedDelta,
        finality: Finality,
    ) -> Result<(), StoreError> {
        SqliteStore::retain_finalized_artifact(self, descriptor, delta, finality).await
    }

    async fn processor_artifact(
        &self,
        descriptor: &ProcessorDescriptor,
        block: BlockNumber,
    ) -> Result<Option<ProcessorArtifact>, StoreError> {
        SqliteStore::processor_artifact(self, descriptor, block).await
    }

    async fn scan_processor_artifacts(
        &self,
        descriptor: &ProcessorDescriptor,
        range: BlockRange,
        limit: usize,
    ) -> Result<Vec<ProcessorArtifact>, StoreError> {
        SqliteStore::scan_processor_artifacts(self, descriptor, range, limit).await
    }

    async fn processor_artifact_stats(
        &self,
        descriptor: &ProcessorDescriptor,
    ) -> Result<ProcessorArtifactStats, StoreError> {
        SqliteStore::processor_artifact_stats(self, descriptor).await
    }

    async fn export_processor_artifacts(
        &self,
        descriptor: &ProcessorDescriptor,
        range: BlockRange,
        limit: usize,
    ) -> Result<ProcessorArtifactExport, StoreError> {
        SqliteStore::export_processor_artifacts(self, descriptor, range, limit).await
    }
}

async fn insert_processor_artifact(
    transaction: &mut Transaction<'_, Sqlite>,
    instance: &str,
    descriptor: &ProcessorDescriptor,
    delta: &EncodedDelta,
    encoded: &[u8],
    retained_at_unix_ms: i64,
) -> Result<bool, StoreError> {
    let inserted = sqlx::query(
        "INSERT INTO processor_artifacts(
            instance, chain_id, block_number, block_hash, parent_hash,
            block_timestamp, delta_schema_version, delta_checksum,
            encoded_delta, retained_at_unix_ms, encoded_bytes
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(instance, block_number) DO NOTHING",
    )
    .bind(instance)
    .bind(u64_i64(delta.chain_id.0, "artifact chain ID")?)
    .bind(u64_i64(delta.block.number.0, "artifact block number")?)
    .bind(delta.block.hash.0.as_slice())
    .bind(delta.block.parent_hash.0.as_slice())
    .bind(u64_i64(delta.block.timestamp, "artifact block timestamp")?)
    .bind(i64::from(delta.schema_version))
    .bind(delta.checksum.0.as_slice())
    .bind(encoded)
    .bind(retained_at_unix_ms)
    .bind(usize_i64(encoded.len(), "artifact encoded bytes")?)
    .execute(&mut **transaction)
    .await?
    .rows_affected()
        == 1;
    let row = sqlx::query(
        "SELECT chain_id, block_number, block_hash, parent_hash,
                block_timestamp, delta_schema_version, delta_checksum,
                encoded_delta, retained_at_unix_ms
         FROM processor_artifacts WHERE instance = ? AND block_number = ?",
    )
    .bind(instance)
    .bind(u64_i64(delta.block.number.0, "artifact block number")?)
    .fetch_one(&mut **transaction)
    .await?;
    let stored = decode_processor_artifact(descriptor, &row)?;
    if stored.delta != *delta || stored.delta.encode_durable()? != encoded {
        return Err(StoreError::ConflictingArtifact(delta.block.number));
    }
    Ok(inserted)
}

async fn stage_processor_artifact_candidate(
    transaction: &mut Transaction<'_, Sqlite>,
    instance: &str,
    descriptor: &ProcessorDescriptor,
    delta: &EncodedDelta,
    encoded: &[u8],
    staged_at_unix_ms: i64,
) -> Result<(), StoreError> {
    sqlx::query(
        "INSERT INTO processor_artifact_candidates(
            instance, chain_id, block_number, block_hash, parent_hash,
            block_timestamp, delta_schema_version, delta_checksum,
            encoded_delta, staged_at_unix_ms
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(instance, block_number, block_hash) DO NOTHING",
    )
    .bind(instance)
    .bind(u64_i64(delta.chain_id.0, "artifact candidate chain ID")?)
    .bind(u64_i64(
        delta.block.number.0,
        "artifact candidate block number",
    )?)
    .bind(delta.block.hash.0.as_slice())
    .bind(delta.block.parent_hash.0.as_slice())
    .bind(u64_i64(
        delta.block.timestamp,
        "artifact candidate block timestamp",
    )?)
    .bind(i64::from(delta.schema_version))
    .bind(delta.checksum.0.as_slice())
    .bind(encoded)
    .bind(staged_at_unix_ms)
    .execute(&mut **transaction)
    .await?;
    let stored: Vec<u8> = sqlx::query_scalar(
        "SELECT encoded_delta FROM processor_artifact_candidates
         WHERE instance = ? AND block_number = ? AND block_hash = ?",
    )
    .bind(instance)
    .bind(u64_i64(
        delta.block.number.0,
        "artifact candidate block number",
    )?)
    .bind(delta.block.hash.0.as_slice())
    .fetch_one(&mut **transaction)
    .await?;
    let stored = EncodedDelta::decode_durable(descriptor, &stored)?;
    if stored != *delta {
        return Err(StoreError::ConflictingArtifact(delta.block.number));
    }
    Ok(())
}

async fn stage_or_retain_processor_artifact(
    transaction: &mut Transaction<'_, Sqlite>,
    descriptor: &ProcessorDescriptor,
    instance: &str,
    delta: &EncodedDelta,
    encoded: &[u8],
    finality: Finality,
    now: i64,
) -> Result<(), StoreError> {
    if matches!(
        descriptor.lifecycle.artifacts.mode,
        ArtifactPolicyMode::None
    ) {
        return Ok(());
    }
    if finality == Finality::Finalized {
        insert_processor_artifact(transaction, instance, descriptor, delta, encoded, now).await?;
        insert_artifact_owner(
            transaction,
            instance,
            delta.block.number,
            ArtifactOwnerKind::ProcessorInstance,
            instance,
            now,
        )
        .await?;
        sqlx::query(
            "DELETE FROM processor_artifact_candidates
             WHERE instance = ? AND block_number = ? AND block_hash = ?",
        )
        .bind(instance)
        .bind(u64_i64(
            delta.block.number.0,
            "artifact candidate block number",
        )?)
        .bind(delta.block.hash.0.as_slice())
        .execute(&mut **transaction)
        .await?;
        prune_processor_artifact_window(transaction, descriptor, instance, delta.block).await?;
    } else {
        stage_processor_artifact_candidate(transaction, instance, descriptor, delta, encoded, now)
            .await?;
    }
    Ok(())
}

async fn promote_processor_artifact_candidates(
    transaction: &mut Transaction<'_, Sqlite>,
    descriptor: &ProcessorDescriptor,
    instance: &str,
    through: BlockNumber,
) -> Result<u64, StoreError> {
    if matches!(
        descriptor.lifecycle.artifacts.mode,
        ArtifactPolicyMode::None
    ) {
        return Ok(0);
    }
    let rows = sqlx::query(
        "SELECT chain_id, block_number, block_hash, parent_hash,
                block_timestamp, delta_schema_version, delta_checksum,
                encoded_delta, staged_at_unix_ms AS retained_at_unix_ms
         FROM processor_artifact_candidates
         WHERE instance = ? AND block_number <= ?
         ORDER BY block_number, block_hash",
    )
    .bind(instance)
    .bind(u64_i64(through.0, "artifact finalization height")?)
    .fetch_all(&mut **transaction)
    .await?;
    let mut promoted = 0_u64;
    for row in rows {
        let artifact = decode_processor_artifact(descriptor, &row)?;
        let encoded = artifact.delta.encode_durable()?;
        insert_processor_artifact(
            transaction,
            instance,
            descriptor,
            &artifact.delta,
            &encoded,
            i64::try_from(artifact.retained_at_unix_ms)
                .map_err(|_| StoreError::Numeric("artifact retention time"))?,
        )
        .await?;
        insert_artifact_owner(
            transaction,
            instance,
            artifact.delta.block.number,
            ArtifactOwnerKind::ProcessorInstance,
            instance,
            i64::try_from(artifact.retained_at_unix_ms)
                .map_err(|_| StoreError::Numeric("artifact owner creation time"))?,
        )
        .await?;
        promoted = promoted.saturating_add(1);
    }
    sqlx::query(
        "DELETE FROM processor_artifact_candidates
         WHERE instance = ? AND block_number <= ?",
    )
    .bind(instance)
    .bind(u64_i64(through.0, "artifact finalization height")?)
    .execute(&mut **transaction)
    .await?;
    if let Some(latest) = sqlx::query(
        "SELECT block_number, block_hash, parent_hash, block_timestamp
         FROM processor_artifacts WHERE instance = ? AND block_number <= ?
         ORDER BY block_number DESC LIMIT 1",
    )
    .bind(instance)
    .bind(u64_i64(through.0, "artifact finalization height")?)
    .fetch_optional(&mut **transaction)
    .await?
    {
        prune_processor_artifact_window(
            transaction,
            descriptor,
            instance,
            BlockRef {
                number: BlockNumber(i64_u64(
                    latest.try_get("block_number")?,
                    "latest artifact block",
                )?),
                hash: decode_hash(latest.try_get("block_hash")?)?,
                parent_hash: decode_hash(latest.try_get("parent_hash")?)?,
                timestamp: i64_u64(
                    latest.try_get("block_timestamp")?,
                    "latest artifact timestamp",
                )?,
            },
        )
        .await?;
    }
    Ok(promoted)
}

async fn insert_artifact_owner(
    transaction: &mut Transaction<'_, Sqlite>,
    instance: &str,
    block: BlockNumber,
    kind: ArtifactOwnerKind,
    owner_id: &str,
    created_at_unix_ms: i64,
) -> Result<bool, StoreError> {
    validate_artifact_owner_id(owner_id)?;
    let inserted = sqlx::query(
        "INSERT OR IGNORE INTO processor_artifact_owners(
            instance, block_number, owner_kind, owner_id, created_at_unix_ms
         ) VALUES (?, ?, ?, ?, ?)",
    )
    .bind(instance)
    .bind(u64_i64(block.0, "artifact owner block")?)
    .bind(kind.as_str())
    .bind(owner_id)
    .bind(created_at_unix_ms)
    .execute(&mut **transaction)
    .await?
    .rows_affected()
        == 1;
    Ok(inserted)
}

fn decode_processor_artifact(
    descriptor: &ProcessorDescriptor,
    row: &SqliteRow,
) -> Result<ProcessorArtifact, StoreError> {
    let encoded: Vec<u8> = row.try_get("encoded_delta")?;
    let delta = EncodedDelta::decode_durable(descriptor, &encoded)?;
    validate_processor_artifact_row(row, &delta)?;
    Ok(ProcessorArtifact {
        delta,
        retained_at_unix_ms: i64_u64(
            row.try_get("retained_at_unix_ms")?,
            "artifact retention time",
        )?,
        encoded_bytes: u64::try_from(encoded.len())
            .map_err(|_| StoreError::Numeric("artifact encoded bytes"))?,
    })
}

fn row_block_number(row: &SqliteRow) -> Result<BlockNumber, StoreError> {
    Ok(BlockNumber(i64_u64(
        row.try_get("block_number")?,
        "artifact block number",
    )?))
}

fn processor_artifact_from_segment(
    delta: EncodedDelta,
    retained_at_unix_ms: i64,
) -> Result<ProcessorArtifact, StoreError> {
    let encoded_bytes = u64::try_from(delta.encode_durable()?.len())
        .map_err(|_| StoreError::Numeric("artifact encoded bytes"))?;
    Ok(ProcessorArtifact {
        delta,
        retained_at_unix_ms: i64_u64(retained_at_unix_ms, "artifact retention time")?,
        encoded_bytes,
    })
}

fn validate_processor_artifact_row(
    row: &SqliteRow,
    delta: &EncodedDelta,
) -> Result<(), StoreError> {
    let chain_id = ChainId(i64_u64(row.try_get("chain_id")?, "artifact chain ID")?);
    let block_number = row_block_number(row)?;
    let block_hash = decode_hash(row.try_get("block_hash")?)?;
    let parent_hash = decode_hash(row.try_get("parent_hash")?)?;
    let block_timestamp = i64_u64(row.try_get("block_timestamp")?, "artifact block timestamp")?;
    let schema_version = u16::try_from(i64_u64(
        row.try_get("delta_schema_version")?,
        "artifact delta schema version",
    )?)
    .map_err(|_| StoreError::Numeric("artifact delta schema version"))?;
    let checksum = decode_hash(row.try_get("delta_checksum")?)?;
    if delta.chain_id != chain_id
        || delta.block.number != block_number
        || delta.block.hash != block_hash
        || delta.block.parent_hash != parent_hash
        || delta.block.timestamp != block_timestamp
        || delta.schema_version != schema_version
        || delta.checksum != checksum
    {
        return Err(StoreError::Invariant(format!(
            "processor artifact metadata disagrees with its encoded delta at block {block_number}"
        )));
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn apply_mutations(
    transaction: &mut Transaction<'_, Sqlite>,
    instance: &str,
    mutations: &[Mutation],
    inverse: bool,
    retain_output: bool,
    block: BlockRef,
    finality: Finality,
) -> Result<(), StoreError> {
    let iterable: Box<dyn Iterator<Item = &Mutation> + Send + '_> = if inverse {
        Box::new(mutations.iter().rev())
    } else {
        Box::new(mutations.iter())
    };
    for mutation in iterable {
        match mutation {
            Mutation::Entity {
                collection,
                key,
                before,
                after,
            } => {
                if !retain_output {
                    continue;
                }
                let value = if inverse { before } else { after };
                match value {
                    Some(value) => {
                        sqlx::query(
                            "INSERT INTO entities(instance, collection, entity_key, value)
                             VALUES (?, ?, ?, ?)
                             ON CONFLICT(instance, collection, entity_key)
                             DO UPDATE SET value = excluded.value",
                        )
                        .bind(instance)
                        .bind(collection)
                        .bind(key)
                        .bind(value)
                        .execute(&mut **transaction)
                        .await?;
                        sqlx::query(
                            "INSERT INTO output_entity_meta(
                                instance, collection, entity_key, block_number,
                                block_timestamp, finality, written_at_unix_ms, value_bytes
                             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                             ON CONFLICT(instance, collection, entity_key)
                             DO UPDATE SET
                               block_number = excluded.block_number,
                               block_timestamp = excluded.block_timestamp,
                               finality = excluded.finality,
                               written_at_unix_ms = excluded.written_at_unix_ms,
                               value_bytes = excluded.value_bytes",
                        )
                        .bind(instance)
                        .bind(collection)
                        .bind(key)
                        .bind(u64_i64(block.number.0, "output block number")?)
                        .bind(u64_i64(block.timestamp, "output block timestamp")?)
                        .bind(finality_i64(finality))
                        .bind(now_i64()?)
                        .bind(usize_i64(value.len(), "output value bytes")?)
                        .execute(&mut **transaction)
                        .await?;
                    }
                    None => {
                        sqlx::query(
                            "DELETE FROM entities
                             WHERE instance = ? AND collection = ? AND entity_key = ?",
                        )
                        .bind(instance)
                        .bind(collection)
                        .bind(key)
                        .execute(&mut **transaction)
                        .await?;
                    }
                }
            }
            Mutation::Index {
                index,
                index_key,
                entity_key,
                before,
                after,
            } => {
                if !retain_output {
                    continue;
                }
                let exists = if inverse { before } else { after };
                if *exists {
                    sqlx::query(
                        "INSERT OR IGNORE INTO entity_indexes(
                            instance, index_name, index_key, entity_key
                         ) VALUES (?, ?, ?, ?)",
                    )
                    .bind(instance)
                    .bind(index)
                    .bind(index_key)
                    .bind(entity_key)
                    .execute(&mut **transaction)
                    .await?;
                } else {
                    sqlx::query(
                        "DELETE FROM entity_indexes
                         WHERE instance = ? AND index_name = ?
                           AND index_key = ? AND entity_key = ?",
                    )
                    .bind(instance)
                    .bind(index)
                    .bind(index_key)
                    .bind(entity_key)
                    .execute(&mut **transaction)
                    .await?;
                }
            }
            Mutation::State {
                namespace,
                key,
                before,
                after,
            } => {
                let value = if inverse { before } else { after };
                match value {
                    Some(value) => {
                        sqlx::query(
                            "INSERT INTO processor_state(
                                instance, namespace, state_key, value
                             ) VALUES (?, ?, ?, ?)
                             ON CONFLICT(instance, namespace, state_key)
                             DO UPDATE SET value = excluded.value",
                        )
                        .bind(instance)
                        .bind(namespace)
                        .bind(key)
                        .bind(value)
                        .execute(&mut **transaction)
                        .await?;
                    }
                    None => {
                        sqlx::query(
                            "DELETE FROM processor_state
                             WHERE instance = ? AND namespace = ? AND state_key = ?",
                        )
                        .bind(instance)
                        .bind(namespace)
                        .bind(key)
                        .execute(&mut **transaction)
                        .await?;
                    }
                }
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn append_changes(
    transaction: &mut Transaction<'_, Sqlite>,
    instance: &str,
    stream_id: &str,
    origin: &DeliveryOrigin,
    chain_id: ChainId,
    block: BlockRef,
    finality: Finality,
    direction: ChangeDirection,
    changes: &[DomainChange],
    sink_ids: &[String],
) -> Result<(Option<u64>, Option<u64>), StoreError> {
    let mut first = None;
    let mut last = None;
    let mut appended_bytes = 0_u64;
    let now = now_i64()?;
    let mut next_sequence: i64 =
        sqlx::query_scalar("SELECT next_sequence FROM delivery_streams WHERE stream_id = ?")
            .bind(stream_id)
            .fetch_one(&mut **transaction)
            .await?;
    for change in changes {
        let change_bytes = change
            .key
            .len()
            .checked_add(change.payload.len())
            .and_then(|value| u64::try_from(value).ok())
            .ok_or(StoreError::Numeric("appended delivery change bytes"))?;
        appended_bytes = appended_bytes
            .checked_add(change_bytes)
            .ok_or(StoreError::Numeric("appended delivery change bytes"))?;
        let result = sqlx::query(
            "INSERT INTO change_log(
                chain_id, instance, stream_id, stream_sequence,
                origin_kind, origin_id, publication_revision,
                block_number, block_hash, parent_hash,
                block_timestamp, finality, direction, kind, entity_key,
                operation, payload, created_at_unix_ms
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(u64_i64(chain_id.0, "chain_id")?)
        .bind(instance)
        .bind(stream_id)
        .bind(next_sequence)
        .bind(origin.kind.as_str())
        .bind(&origin.id)
        .bind(u64_i64(
            origin.publication_revision,
            "publication revision",
        )?)
        .bind(u64_i64(block.number.0, "block_number")?)
        .bind(block.hash.0.as_slice())
        .bind(block.parent_hash.0.as_slice())
        .bind(u64_i64(block.timestamp, "block_timestamp")?)
        .bind(finality_i64(finality))
        .bind(direction.as_str())
        .bind(&change.kind)
        .bind(&change.key)
        .bind(operation_i64(change.operation))
        .bind(&change.payload)
        .bind(now)
        .execute(&mut **transaction)
        .await?;
        let physical_sequence = result.last_insert_rowid();
        let sequence = i64_u64(next_sequence, "stream change sequence")?;
        next_sequence = next_sequence
            .checked_add(1)
            .ok_or(StoreError::Numeric("next stream change sequence"))?;
        first.get_or_insert(sequence);
        last = Some(sequence);
        for sink_id in sink_ids {
            sqlx::query("INSERT INTO sink_outbox(sink_id, change_sequence) VALUES (?, ?)")
                .bind(sink_id)
                .bind(physical_sequence)
                .execute(&mut **transaction)
                .await?;
        }
    }
    if !changes.is_empty() {
        sqlx::query(
            "UPDATE delivery_streams
             SET next_sequence = ?, live_bytes = live_bytes + ?
             WHERE stream_id = ?",
        )
        .bind(next_sequence)
        .bind(u64_i64(appended_bytes, "appended delivery change bytes")?)
        .bind(stream_id)
        .execute(&mut **transaction)
        .await?;
    }
    Ok((first, last))
}

async fn backfill_delivery_origin(
    pool: &SqlitePool,
    stream_id: &str,
) -> Result<DeliveryOrigin, StoreError> {
    let row = sqlx::query(
        "SELECT subscription_id, mode, publication_revision
         FROM backfill_subscriptions WHERE history_stream_id = ?",
    )
    .bind(stream_id)
    .fetch_one(pool)
    .await?;
    let mode = BackfillSubscriptionMode::parse(row.try_get("mode")?)?;
    Ok(DeliveryOrigin {
        kind: match mode {
            BackfillSubscriptionMode::FillMissing => DeliveryOriginKind::HistoricalBackfill,
            BackfillSubscriptionMode::Recompute => DeliveryOriginKind::Recompute,
        },
        id: row.try_get("subscription_id")?,
        publication_revision: i64_u64(
            row.try_get("publication_revision")?,
            "publication revision",
        )?,
    })
}

async fn record_backfill_progress(
    transaction: &mut Transaction<'_, Sqlite>,
    stream_id: &str,
    through_block: BlockNumber,
    processed_blocks: u64,
    published_domain_changes: u64,
) -> Result<(), StoreError> {
    let now = now_i64()?;
    sqlx::query(
        "UPDATE backfill_subscriptions
         SET processed_work_blocks = processed_work_blocks + ?,
             published_domain_changes = published_domain_changes + ?,
             updated_at_unix_ms = ?
         WHERE history_stream_id = ?",
    )
    .bind(u64_i64(processed_blocks, "processed backfill blocks")?)
    .bind(u64_i64(
        published_domain_changes,
        "published backfill domain changes",
    )?)
    .bind(now)
    .bind(stream_id)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "UPDATE backfill_subscription_ranges
         SET state = 'running', committed_blocks = committed_blocks + ?
         WHERE subscription_id = (
             SELECT subscription_id FROM backfill_subscriptions
             WHERE history_stream_id = ?
         ) AND from_block <= ? AND to_block >= ?",
    )
    .bind(u64_i64(processed_blocks, "committed backfill blocks")?)
    .bind(stream_id)
    .bind(u64_i64(through_block.0, "progress through block")?)
    .bind(u64_i64(through_block.0, "progress through block")?)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn append_backfill_completion_transaction(
    transaction: &mut Transaction<'_, Sqlite>,
    instance: &str,
    stream_id: &str,
    chain_id: ChainId,
    subscription_id: &str,
    through_block: BlockNumber,
) -> Result<u64, StoreError> {
    let row = sqlx::query(
        "SELECT mode, publication_revision
         FROM backfill_subscriptions
         WHERE subscription_id = ? AND history_stream_id = ?",
    )
    .bind(subscription_id)
    .bind(stream_id)
    .fetch_one(&mut **transaction)
    .await?;
    let delivery_origin = DeliveryOrigin {
        kind: match BackfillSubscriptionMode::parse(row.try_get("mode")?)? {
            BackfillSubscriptionMode::FillMissing => DeliveryOriginKind::HistoricalBackfill,
            BackfillSubscriptionMode::Recompute => DeliveryOriginKind::Recompute,
        },
        id: subscription_id.to_owned(),
        publication_revision: i64_u64(
            row.try_get("publication_revision")?,
            "publication revision",
        )?,
    };
    let sequence = if let Some(sequence) = sqlx::query_scalar::<_, i64>(
        "SELECT stream_sequence FROM change_log
         WHERE stream_id = ? AND kind = 'system.backfill_complete'",
    )
    .bind(stream_id)
    .fetch_optional(&mut **transaction)
    .await?
    {
        i64_u64(sequence, "backfill completion sequence")?
    } else {
        let metadata =
            backfill_completion_metadata(transaction, stream_id, subscription_id).await?;
        let covered_hash: Option<Vec<u8>> = sqlx::query_scalar(
            "SELECT block_hash FROM (
                SELECT block_hash, 0 AS source_order FROM processor_coverage
                 WHERE instance = ? AND block_number = ?
                UNION ALL
                SELECT end_hash AS block_hash, 1 AS source_order
                  FROM finalized_coverage_segments
                 WHERE instance = ? AND segment_end = ?
             ) ORDER BY source_order LIMIT 1",
        )
        .bind(instance)
        .bind(u64_i64(through_block.0, "completion block number")?)
        .bind(instance)
        .bind(u64_i64(through_block.0, "completion block number")?)
        .fetch_optional(&mut **transaction)
        .await?;
        let covered_hash = covered_hash.map(decode_hash).transpose()?.ok_or_else(|| {
            StoreError::Invariant(format!(
                "cannot complete backfill before block {} is covered",
                through_block.0
            ))
        })?;
        let stored_block: Option<(Vec<u8>, i64)> = sqlx::query_as(
            "SELECT parent_hash, block_timestamp
             FROM change_log
             WHERE instance = ? AND block_number = ? AND block_hash = ?
             ORDER BY sequence DESC LIMIT 1",
        )
        .bind(instance)
        .bind(u64_i64(through_block.0, "completion block number")?)
        .bind(covered_hash.0.as_slice())
        .fetch_optional(&mut **transaction)
        .await?;
        let (parent_hash, timestamp) = if let Some((parent_hash, timestamp)) = stored_block {
            (
                decode_hash(parent_hash)?,
                i64_u64(timestamp, "completion block timestamp")?,
            )
        } else {
            (BlockHash::ZERO, 0)
        };
        let boundary_block = BlockRef {
            number: through_block,
            hash: covered_hash,
            parent_hash,
            timestamp,
        };
        if metadata.disposition == BackfillCompletionDisposition::AlreadyCoveredNoop {
            let progress_exists: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM change_log
                 WHERE stream_id = ? AND kind = 'system.backfill_progress'",
            )
            .bind(stream_id)
            .fetch_one(&mut **transaction)
            .await?;
            if progress_exists == 0 {
                let first_block: i64 = sqlx::query_scalar(
                    "SELECT MIN(from_block) FROM backfill_subscription_ranges
                     WHERE subscription_id = ?",
                )
                .bind(subscription_id)
                .fetch_one(&mut **transaction)
                .await?;
                let progress = backfill_progress_change(
                    BlockNumber(i64_u64(first_block, "zero-work progress start")?),
                    through_block,
                    0,
                );
                append_changes(
                    transaction,
                    instance,
                    stream_id,
                    &delivery_origin,
                    chain_id,
                    boundary_block,
                    Finality::Finalized,
                    ChangeDirection::Apply,
                    &[progress],
                    &[],
                )
                .await?;
            }
        }
        let change = DomainChange {
            kind: "system.backfill_complete".to_owned(),
            key: subscription_id.as_bytes().to_vec(),
            operation: ChangeOperation::Upsert,
            payload: encode_backfill_completion_metadata(metadata),
        };
        let (_, last) = append_changes(
            transaction,
            instance,
            stream_id,
            &delivery_origin,
            chain_id,
            boundary_block,
            Finality::Finalized,
            ChangeDirection::Apply,
            &[change],
            &[],
        )
        .await?;
        last.ok_or_else(|| StoreError::Invariant("completion record was not appended".to_owned()))?
    };
    let now = now_i64()?;
    sqlx::query(
        "UPDATE backfill_subscription_ranges
         SET state = 'committed'
         WHERE subscription_id = ?",
    )
    .bind(subscription_id)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "UPDATE backfill_subscriptions
         SET state = 'draining', completion_sequence = ?, last_error = NULL,
             updated_at_unix_ms = ?
         WHERE subscription_id = ?",
    )
    .bind(u64_i64(sequence, "backfill completion sequence")?)
    .bind(now)
    .bind(subscription_id)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "UPDATE jobs
         SET state = 'completed', updated_at_unix_ms = ?
         WHERE job_id = (
           SELECT job_id FROM backfill_subscriptions WHERE subscription_id = ?
         )",
    )
    .bind(now)
    .bind(subscription_id)
    .execute(&mut **transaction)
    .await?;
    Ok(sequence)
}

async fn backfill_completion_metadata(
    transaction: &mut Transaction<'_, Sqlite>,
    stream_id: &str,
    subscription_id: &str,
) -> Result<BackfillCompletionMetadata, StoreError> {
    let row = sqlx::query(
        "SELECT subscriptions.mode, subscriptions.processed_work_blocks,
                subscriptions.published_domain_changes AS domain_changes,
                COALESCE((
                    SELECT SUM(to_block - from_block + 1)
                    FROM backfill_subscription_ranges
                    WHERE subscription_id = subscriptions.subscription_id
                ), subscriptions.to_block - subscriptions.from_block + 1) AS requested_blocks
         FROM backfill_subscriptions AS subscriptions
         WHERE subscriptions.subscription_id = ? AND subscriptions.history_stream_id = ?",
    )
    .bind(subscription_id)
    .bind(stream_id)
    .fetch_one(&mut **transaction)
    .await?;
    let mode = BackfillSubscriptionMode::parse(row.try_get("mode")?)?;
    let processed = i64_u64(
        row.try_get("processed_work_blocks")?,
        "completion processed blocks",
    )?;
    let requested = i64_u64(
        row.try_get("requested_blocks")?,
        "completion requested blocks",
    )?;
    let domain_changes = i64_u64(row.try_get("domain_changes")?, "completion domain changes")?;
    let processed = processed.min(requested);
    let preexisting_rows: Vec<(i64, i64)> = sqlx::query_as(
        "SELECT from_block, to_block
         FROM backfill_subscription_preexisting_ranges
         WHERE subscription_id = ? ORDER BY ordinal",
    )
    .bind(subscription_id)
    .fetch_all(&mut **transaction)
    .await?;
    let covered_before_request_ranges = preexisting_rows
        .into_iter()
        .map(|(from, to)| {
            BlockRange::new(
                BlockNumber(i64_u64(from, "preexisting coverage start")?),
                BlockNumber(i64_u64(to, "preexisting coverage end")?),
            )
            .map_err(|error| StoreError::Invariant(error.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let covered_before_request_blocks =
        covered_before_request_ranges
            .iter()
            .try_fold(0_u64, |total, range| {
                total
                    .checked_add(range.len())
                    .ok_or(StoreError::Numeric("preexisting coverage blocks"))
            })?;
    let complete_counts_are_valid = match mode {
        BackfillSubscriptionMode::FillMissing => covered_before_request_blocks
            .checked_add(processed)
            .is_some_and(|total| total == requested),
        BackfillSubscriptionMode::Recompute => {
            covered_before_request_blocks == requested && processed == requested
        }
    };
    if !complete_counts_are_valid {
        return Err(StoreError::Invariant(
            "backfill cannot complete before its snapshotted coverage and processed work account for every requested block"
                .to_owned(),
        ));
    }
    let (disposition, newly_processed_blocks, republished_blocks) = match mode {
        BackfillSubscriptionMode::Recompute => {
            (BackfillCompletionDisposition::PublishedAll, 0, processed)
        }
        BackfillSubscriptionMode::FillMissing if processed == 0 => {
            (BackfillCompletionDisposition::AlreadyCoveredNoop, 0, 0)
        }
        BackfillSubscriptionMode::FillMissing if processed < requested => (
            BackfillCompletionDisposition::PublishedMissingOnly,
            processed,
            processed,
        ),
        BackfillSubscriptionMode::FillMissing => (
            BackfillCompletionDisposition::PublishedAll,
            processed,
            processed,
        ),
    };
    Ok(BackfillCompletionMetadata {
        mode,
        disposition,
        requested_blocks: requested,
        covered_before_request_blocks,
        covered_before_request_ranges,
        newly_processed_blocks,
        republished_blocks,
        domain_changes,
    })
}

fn encode_backfill_completion_metadata(metadata: BackfillCompletionMetadata) -> Vec<u8> {
    let mut payload = Vec::with_capacity(
        48_usize.saturating_add(metadata.covered_before_request_ranges.len() * 16),
    );
    payload.extend_from_slice(&2_u16.to_be_bytes());
    payload.push(match metadata.mode {
        BackfillSubscriptionMode::FillMissing => 0,
        BackfillSubscriptionMode::Recompute => 1,
    });
    payload.push(match metadata.disposition {
        BackfillCompletionDisposition::PublishedAll => 0,
        BackfillCompletionDisposition::PublishedMissingOnly => 1,
        BackfillCompletionDisposition::AlreadyCoveredNoop => 2,
    });
    for count in [
        metadata.requested_blocks,
        metadata.covered_before_request_blocks,
        metadata.newly_processed_blocks,
        metadata.republished_blocks,
        metadata.domain_changes,
    ] {
        payload.extend_from_slice(&count.to_be_bytes());
    }
    payload.extend_from_slice(
        &u32::try_from(metadata.covered_before_request_ranges.len())
            .expect("subscription range count is bounded at creation")
            .to_be_bytes(),
    );
    for range in metadata.covered_before_request_ranges {
        payload.extend_from_slice(&range.start().0.to_be_bytes());
        payload.extend_from_slice(&range.end().0.to_be_bytes());
    }
    payload
}

async fn create_recovery_checkpoint(
    transaction: &mut Transaction<'_, Sqlite>,
    descriptor: &ProcessorDescriptor,
    instance: &str,
    cursor: &ProcessorCursor,
    keep: u32,
) -> Result<(), StoreError> {
    let (encoded_cursor, encoded_snapshot, checksum) =
        encode_state_snapshot(transaction, descriptor, instance, cursor).await?;
    sqlx::query(
        "INSERT INTO recovery_checkpoints(
            instance, block_number, block_hash, processor_cursor,
            state_snapshot, state_checksum, created_at_unix_ms
         ) VALUES (?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(instance, block_number, block_hash) DO NOTHING",
    )
    .bind(instance)
    .bind(u64_i64(cursor.block_number.0, "checkpoint block number")?)
    .bind(cursor.block_hash.0.as_slice())
    .bind(encoded_cursor)
    .bind(encoded_snapshot)
    .bind(checksum.0.as_slice())
    .bind(now_i64()?)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "DELETE FROM recovery_checkpoints
         WHERE instance = ?
           AND checkpoint_id NOT IN (
             SELECT checkpoint_id
             FROM recovery_checkpoints
             WHERE instance = ?
             ORDER BY block_number DESC, checkpoint_id DESC
             LIMIT ?
           )",
    )
    .bind(instance)
    .bind(instance)
    .bind(i64::from(keep))
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn prune_output_window(
    transaction: &mut Transaction<'_, Sqlite>,
    descriptor: &ProcessorDescriptor,
    instance: &str,
    current_block: BlockRef,
) -> Result<u64, StoreError> {
    if !matches!(descriptor.lifecycle.output.mode, OutputPolicyMode::Window) {
        return Ok(0);
    }
    let window = descriptor.lifecycle.output.window.ok_or_else(|| {
        StoreError::Invariant("window output policy is missing its bound".to_owned())
    })?;
    let rows = sqlx::query(
        "SELECT collection, entity_key, block_number, written_at_unix_ms, value_bytes
         FROM output_entity_meta
         WHERE instance = ? AND finality = ?
         ORDER BY block_number, written_at_unix_ms, collection, entity_key
         LIMIT 10000",
    )
    .bind(instance)
    .bind(finality_i64(Finality::Finalized))
    .fetch_all(&mut **transaction)
    .await?;
    let mut candidates = Vec::new();
    if let Some(max_blocks) = window.max_blocks {
        let retained_from = current_block
            .number
            .0
            .saturating_add(1)
            .saturating_sub(max_blocks);
        for row in rows {
            let block_number = i64_u64(row.try_get("block_number")?, "output block number")?;
            if block_number < retained_from {
                candidates.push((
                    row.try_get::<String, _>("collection")?,
                    row.try_get::<Vec<u8>, _>("entity_key")?,
                ));
            }
        }
    } else if let Some(max_age_seconds) = window.max_age_seconds {
        let cutoff = now_i64()?.saturating_sub(u64_i64(
            max_age_seconds.saturating_mul(1_000),
            "output maximum age",
        )?);
        for row in rows {
            let written_at: i64 = row.try_get("written_at_unix_ms")?;
            if written_at < cutoff {
                candidates.push((
                    row.try_get::<String, _>("collection")?,
                    row.try_get::<Vec<u8>, _>("entity_key")?,
                ));
            }
        }
    } else if let Some(max_rows) = window.max_rows {
        let total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM entities WHERE instance = ?")
            .bind(instance)
            .fetch_one(&mut **transaction)
            .await?;
        let excess = i64_u64(total, "output entity count")?.saturating_sub(max_rows);
        let take = usize::try_from(excess)
            .unwrap_or(usize::MAX)
            .min(rows.len());
        for row in rows.into_iter().take(take) {
            candidates.push((
                row.try_get::<String, _>("collection")?,
                row.try_get::<Vec<u8>, _>("entity_key")?,
            ));
        }
    } else if let Some(max_bytes) = window.max_bytes {
        let total: i64 = sqlx::query_scalar(
            "SELECT COALESCE(SUM(value_bytes), 0)
             FROM output_entity_meta WHERE instance = ?",
        )
        .bind(instance)
        .fetch_one(&mut **transaction)
        .await?;
        let mut excess = i64_u64(total, "output entity bytes")?.saturating_sub(max_bytes);
        for row in rows {
            if excess == 0 {
                break;
            }
            let bytes = i64_u64(row.try_get("value_bytes")?, "output entity bytes")?;
            candidates.push((
                row.try_get::<String, _>("collection")?,
                row.try_get::<Vec<u8>, _>("entity_key")?,
            ));
            excess = excess.saturating_sub(bytes);
        }
    }
    for (collection, key) in &candidates {
        sqlx::query(
            "DELETE FROM entities
             WHERE instance = ? AND collection = ? AND entity_key = ?",
        )
        .bind(instance)
        .bind(collection)
        .bind(key)
        .execute(&mut **transaction)
        .await?;
    }
    if !candidates.is_empty() {
        sqlx::query(
            "DELETE FROM entity_indexes
             WHERE instance = ?
               AND NOT EXISTS (
                 SELECT 1 FROM entities
                 WHERE entities.instance = entity_indexes.instance
                   AND entities.entity_key = entity_indexes.entity_key
               )",
        )
        .bind(instance)
        .execute(&mut **transaction)
        .await?;
    }
    u64::try_from(candidates.len()).map_err(|_| StoreError::Numeric("pruned output entities"))
}

#[allow(clippy::too_many_lines)]
async fn prune_processor_artifact_window(
    transaction: &mut Transaction<'_, Sqlite>,
    descriptor: &ProcessorDescriptor,
    instance: &str,
    current_block: BlockRef,
) -> Result<u64, StoreError> {
    if !matches!(
        descriptor.lifecycle.artifacts.mode,
        ArtifactPolicyMode::Window
    ) {
        return Ok(0);
    }
    let window = descriptor.lifecycle.artifacts.window.ok_or_else(|| {
        StoreError::Invariant("window artifact policy is missing its bound".to_owned())
    })?;
    let mut candidates = Vec::<BlockNumber>::new();
    if let Some(max_blocks) = window.max_blocks {
        let retained_from = current_block
            .number
            .0
            .saturating_add(1)
            .saturating_sub(max_blocks);
        let rows: Vec<i64> = sqlx::query_scalar(
            "SELECT artifacts.block_number
             FROM processor_artifacts AS artifacts
             JOIN processor_artifact_owners AS owners
               ON owners.instance = artifacts.instance
              AND owners.block_number = artifacts.block_number
             WHERE artifacts.instance = ? AND artifacts.block_number < ?
               AND owners.owner_kind = 'processor_instance' AND owners.owner_id = ?
             ORDER BY artifacts.block_number LIMIT 10000",
        )
        .bind(instance)
        .bind(u64_i64(retained_from, "artifact retained-from block")?)
        .bind(instance)
        .fetch_all(&mut **transaction)
        .await?;
        candidates = rows
            .into_iter()
            .map(|value| i64_u64(value, "artifact prune block").map(BlockNumber))
            .collect::<Result<_, _>>()?;
    } else if let Some(max_age_seconds) = window.max_age_seconds {
        let cutoff = now_i64()?.saturating_sub(u64_i64(
            max_age_seconds.saturating_mul(1_000),
            "artifact maximum age",
        )?);
        let rows: Vec<i64> = sqlx::query_scalar(
            "SELECT artifacts.block_number
             FROM processor_artifacts AS artifacts
             JOIN processor_artifact_owners AS owners
               ON owners.instance = artifacts.instance
              AND owners.block_number = artifacts.block_number
             WHERE artifacts.instance = ? AND artifacts.retained_at_unix_ms < ?
               AND owners.owner_kind = 'processor_instance' AND owners.owner_id = ?
             ORDER BY artifacts.retained_at_unix_ms, artifacts.block_number LIMIT 10000",
        )
        .bind(instance)
        .bind(cutoff)
        .bind(instance)
        .fetch_all(&mut **transaction)
        .await?;
        candidates = rows
            .into_iter()
            .map(|value| i64_u64(value, "artifact prune block").map(BlockNumber))
            .collect::<Result<_, _>>()?;
    } else if let Some(max_bytes) = window.max_bytes {
        let total: i64 = sqlx::query_scalar(
            "SELECT COALESCE(SUM(length(artifacts.encoded_delta)), 0)
             FROM processor_artifacts AS artifacts
             JOIN processor_artifact_owners AS owners
               ON owners.instance = artifacts.instance
              AND owners.block_number = artifacts.block_number
             WHERE artifacts.instance = ?
               AND owners.owner_kind = 'processor_instance' AND owners.owner_id = ?",
        )
        .bind(instance)
        .bind(instance)
        .fetch_one(&mut **transaction)
        .await?;
        let mut excess = i64_u64(total, "processor artifact bytes")?.saturating_sub(max_bytes);
        if excess > 0 {
            let rows = sqlx::query(
                "SELECT artifacts.block_number,
                        length(artifacts.encoded_delta) AS encoded_bytes
                 FROM processor_artifacts AS artifacts
                 JOIN processor_artifact_owners AS owners
                   ON owners.instance = artifacts.instance
                  AND owners.block_number = artifacts.block_number
                 WHERE artifacts.instance = ?
                   AND owners.owner_kind = 'processor_instance' AND owners.owner_id = ?
                 ORDER BY artifacts.block_number LIMIT 10000",
            )
            .bind(instance)
            .bind(instance)
            .fetch_all(&mut **transaction)
            .await?;
            for row in rows {
                if excess == 0 {
                    break;
                }
                candidates.push(BlockNumber(i64_u64(
                    row.try_get("block_number")?,
                    "artifact prune block",
                )?));
                excess = excess.saturating_sub(i64_u64(
                    row.try_get("encoded_bytes")?,
                    "artifact encoded bytes",
                )?);
            }
        }
    }
    for block in &candidates {
        sqlx::query(
            "DELETE FROM processor_artifact_owners
             WHERE instance = ? AND block_number = ?
               AND owner_kind = 'processor_instance' AND owner_id = ?",
        )
        .bind(instance)
        .bind(u64_i64(block.0, "artifact prune block")?)
        .bind(instance)
        .execute(&mut **transaction)
        .await?;
    }
    let deleted = sqlx::query(
        "DELETE FROM processor_artifacts
         WHERE instance = ?
           AND NOT EXISTS (
             SELECT 1 FROM processor_artifact_owners AS owners
             WHERE owners.instance = processor_artifacts.instance
               AND owners.block_number = processor_artifacts.block_number
           )",
    )
    .bind(instance)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    Ok(deleted)
}

async fn encode_state_snapshot(
    transaction: &mut Transaction<'_, Sqlite>,
    descriptor: &ProcessorDescriptor,
    instance: &str,
    cursor: &ProcessorCursor,
) -> Result<(Vec<u8>, Vec<u8>, BlockHash), StoreError> {
    let rows = sqlx::query(
        "SELECT namespace, state_key, value
         FROM processor_state
         WHERE instance = ?
         ORDER BY namespace, state_key",
    )
    .bind(instance)
    .fetch_all(&mut **transaction)
    .await?;
    let entries = rows
        .into_iter()
        .map(|row| {
            Ok(StateSnapshotEntry {
                namespace: row.try_get("namespace")?,
                key: row.try_get("state_key")?,
                value: row.try_get("value")?,
            })
        })
        .collect::<Result<Vec<_>, sqlx::Error>>()?;
    let snapshot = StateSnapshot {
        format_version: 1,
        processor_instance: instance.to_owned(),
        descriptor_hash: descriptor_hash(descriptor)?,
        cursor: cursor.clone(),
        entries,
    };
    let encoded_snapshot = postcard::to_allocvec(&snapshot)
        .map_err(|error| StoreError::Encoding(error.to_string()))?;
    let encoded_cursor =
        postcard::to_allocvec(cursor).map_err(|error| StoreError::Encoding(error.to_string()))?;
    let checksum = BlockHash::new(*blake3::hash(&encoded_snapshot).as_bytes());
    Ok((encoded_cursor, encoded_snapshot, checksum))
}

fn validate_state_snapshot(
    descriptor: &ProcessorDescriptor,
    instance: &str,
    encoded: &[u8],
    expected_checksum: BlockHash,
) -> Result<ProcessorCursor, StoreError> {
    Ok(decode_state_snapshot(descriptor, instance, encoded, expected_checksum)?.cursor)
}

fn decode_state_snapshot(
    descriptor: &ProcessorDescriptor,
    instance: &str,
    encoded: &[u8],
    expected_checksum: BlockHash,
) -> Result<StateSnapshot, StoreError> {
    let checksum = BlockHash::new(*blake3::hash(encoded).as_bytes());
    if checksum != expected_checksum {
        return Err(StoreError::SavepointChecksum);
    }
    let snapshot: StateSnapshot =
        postcard::from_bytes(encoded).map_err(|error| StoreError::Encoding(error.to_string()))?;
    if snapshot.format_version != 1
        || snapshot.processor_instance != instance
        || snapshot.descriptor_hash != descriptor_hash(descriptor)?
        || snapshot.cursor.processor_id != descriptor.id.as_str()
        || snapshot.cursor.processor_version != descriptor.version.to_string()
    {
        return Err(StoreError::SavepointContract);
    }
    Ok(snapshot)
}

fn descriptor_hash(descriptor: &ProcessorDescriptor) -> Result<BlockHash, StoreError> {
    let encoded = serde_json::to_vec(descriptor)?;
    Ok(BlockHash::new(*blake3::hash(&encoded).as_bytes()))
}

fn query_snapshot_id(epoch: [u8; 16], instance: &str, collection: &str, boundary: u64) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&epoch);
    hasher.update(instance.as_bytes());
    hasher.update(&[0]);
    hasher.update(collection.as_bytes());
    hasher.update(&boundary.to_be_bytes());
    hasher.update(
        &SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .to_be_bytes(),
    );
    hasher.finalize().as_bytes()[..16]
        .try_into()
        .expect("BLAKE3 digest contains sixteen bytes")
}

async fn highest_coverage_cursor(
    transaction: &mut Transaction<'_, Sqlite>,
    descriptor: &ProcessorDescriptor,
    chain_id: ChainId,
) -> Result<Option<ProcessorCursor>, StoreError> {
    let row = sqlx::query(
        "SELECT block_number, block_hash, finality FROM (
            SELECT block_number, block_hash, finality
              FROM processor_coverage WHERE instance = ?
            UNION ALL
            SELECT segment_end AS block_number, end_hash AS block_hash, ? AS finality
              FROM finalized_coverage_segments WHERE instance = ?
         ) ORDER BY block_number DESC LIMIT 1",
    )
    .bind(processor_instance(descriptor))
    .bind(finality_i64(Finality::Finalized))
    .bind(processor_instance(descriptor))
    .fetch_optional(&mut **transaction)
    .await?;
    row.map(|row| {
        let block_number = BlockNumber(i64_u64(
            row.try_get("block_number")?,
            "coverage block number",
        )?);
        Ok(ProcessorCursor {
            processor_id: descriptor.id.to_string(),
            processor_version: descriptor.version.to_string(),
            chain_id,
            block_number,
            block_hash: decode_hash(row.try_get("block_hash")?)?,
            finality: decode_finality(row.try_get("finality")?)?,
            sequence: block_number.0,
        })
    })
    .transpose()
}

async fn upsert_cursor(
    transaction: &mut Transaction<'_, Sqlite>,
    instance: &str,
    cursor: &ProcessorCursor,
    encoded: &str,
) -> Result<(), StoreError> {
    sqlx::query(
        "INSERT INTO processor_cursors(
            instance, cursor, block_number, block_hash, finality, sequence
         ) VALUES (?, ?, ?, ?, ?, ?)
         ON CONFLICT(instance) DO UPDATE SET
           cursor = excluded.cursor,
           block_number = excluded.block_number,
           block_hash = excluded.block_hash,
           finality = excluded.finality,
           sequence = excluded.sequence",
    )
    .bind(instance)
    .bind(encoded)
    .bind(u64_i64(cursor.block_number.0, "block_number")?)
    .bind(cursor.block_hash.0.as_slice())
    .bind(finality_i64(cursor.finality))
    .bind(u64_i64(cursor.sequence, "processor sequence")?)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn applied_checksum(
    pool: &SqlitePool,
    instance: &str,
    block_number: BlockNumber,
    block_hash: BlockHash,
) -> Result<Option<BlockHash>, StoreError> {
    let value: Option<Vec<u8>> = sqlx::query_scalar(
        "SELECT delta_checksum FROM applied_blocks
         WHERE instance = ? AND block_number = ? AND block_hash = ?",
    )
    .bind(instance)
    .bind(u64_i64(block_number.0, "block_number")?)
    .bind(block_hash.0.as_slice())
    .fetch_optional(pool)
    .await?;
    value.map(decode_hash).transpose()
}

fn same_recent_material(stored: &BlockFrame, incoming: &BlockFrame) -> bool {
    stored.chain_id == incoming.chain_id
        && stored.block == incoming.block
        && stored.header == incoming.header
        && stored.transactions == incoming.transactions
        && stored.receipts == incoming.receipts
        && stored.logs == incoming.logs
        && stored.withdrawals == incoming.withdrawals
        && stored.blob_sidecars == incoming.blob_sidecars
        && stored.traces == incoming.traces
        && stored.state_diffs == incoming.state_diffs
}

fn build_inverse_changes(batch: &MutationBatch) -> Result<Vec<DomainChange>, StoreError> {
    let output_entities = batch
        .mutations
        .iter()
        .filter_map(|mutation| match mutation {
            Mutation::Entity {
                key, before, after, ..
            } => Some((key, before, after)),
            Mutation::Index { .. } | Mutation::State { .. } => None,
        })
        .collect::<Vec<_>>();
    let working_state = batch
        .mutations
        .iter()
        .filter_map(|mutation| match mutation {
            Mutation::State {
                key, before, after, ..
            } => Some((key, before, after)),
            Mutation::Entity { .. } | Mutation::Index { .. } => None,
        })
        .collect::<Vec<_>>();
    let mut output = Vec::with_capacity(batch.changes.len());
    for change in batch.changes.iter().rev() {
        let matching = |(key, _, after): &&(&Vec<u8>, &Option<Vec<u8>>, &Option<Vec<u8>>)| {
            key.as_slice() == change.key
                && match change.operation {
                    ChangeOperation::Upsert => after.as_deref() == Some(&change.payload),
                    ChangeOperation::Delete => after.is_none(),
                }
        };
        let mut matches = output_entities.iter().filter(matching).collect::<Vec<_>>();
        if matches.is_empty() {
            matches = working_state.iter().filter(matching).collect();
        }
        // Delivery-only processors may publish a richer envelope than the
        // lean entity mutation they use for optional materialized output. A
        // newly created, uniquely keyed entity still has an exact inverse:
        // delete that delivery key. This preserves the no-duplicate-output
        // profile without weakening updates, whose prior delivery payload
        // must remain reconstructable from an exact mutation match.
        if matches.is_empty() && change.operation == ChangeOperation::Upsert {
            let created = output_entities
                .iter()
                .chain(working_state.iter())
                .filter(|(key, before, after)| {
                    key.as_slice() == change.key && before.is_none() && after.is_some()
                })
                .collect::<Vec<_>>();
            if created.len() == 1 {
                output.push(DomainChange {
                    kind: change.kind.clone(),
                    key: change.key.clone(),
                    operation: ChangeOperation::Delete,
                    payload: Vec::new(),
                });
                continue;
            }
        }
        if matches.len() != 1 {
            return Err(StoreError::Invariant(format!(
                "change {} has {} matching state mutations; expected one",
                change.kind,
                matches.len()
            )));
        }
        let (_, before, _) = matches[0];
        output.push(DomainChange {
            kind: change.kind.clone(),
            key: change.key.clone(),
            operation: if before.is_some() {
                ChangeOperation::Upsert
            } else {
                ChangeOperation::Delete
            },
            payload: (*before).clone().unwrap_or_default(),
        });
    }
    Ok(output)
}

async fn rebuild_recent_transaction_locators(pool: &SqlitePool) -> Result<(), StoreError> {
    let rows: Vec<Vec<u8>> = sqlx::query_scalar(
        "SELECT recent.encoded_frame
         FROM canonical_blocks AS canonical
         JOIN recent_blocks AS recent
           ON recent.chain_id = canonical.chain_id
          AND recent.block_number = canonical.block_number
          AND recent.block_hash = canonical.block_hash
         ORDER BY canonical.chain_id, canonical.block_number",
    )
    .fetch_all(pool)
    .await?;
    let mut transaction = pool.begin().await?;
    sqlx::query("DELETE FROM recent_transaction_locator")
        .execute(&mut *transaction)
        .await?;
    for encoded in rows {
        let frame: BlockFrame = leani_primitives::durable::decode(
            DurableKind::BlockFrame,
            leani_primitives::BLOCK_FRAME_SCHEMA_VERSION,
            &encoded,
        )
        .map_err(|error| StoreError::Encoding(error.to_string()))?;
        upsert_recent_transaction_locators(&mut transaction, &frame).await?;
    }
    transaction.commit().await?;
    Ok(())
}

async fn upsert_recent_transaction_locators(
    transaction: &mut Transaction<'_, Sqlite>,
    frame: &BlockFrame,
) -> Result<(), StoreError> {
    let Material::Complete(transactions) = &frame.transactions else {
        return Ok(());
    };
    delete_recent_transaction_locators(transaction, frame.chain_id, frame.block).await?;
    for (position, transaction_envelope) in transactions.iter().enumerate() {
        let position = u32::try_from(position)
            .map_err(|_| StoreError::Invariant("transaction count exceeds u32".to_owned()))?;
        if transaction_envelope.index != position {
            return Err(StoreError::Invariant(format!(
                "transaction {} has index {}, expected {position}",
                transaction_envelope.hash, transaction_envelope.index
            )));
        }
        sqlx::query(
            "INSERT INTO recent_transaction_locator(
                chain_id, transaction_hash, block_number, block_hash, transaction_index
             ) VALUES (?, ?, ?, ?, ?)
             ON CONFLICT(chain_id, transaction_hash) DO UPDATE SET
                block_number = excluded.block_number,
                block_hash = excluded.block_hash,
                transaction_index = excluded.transaction_index",
        )
        .bind(u64_i64(frame.chain_id.0, "chain_id")?)
        .bind(transaction_envelope.hash.0.as_slice())
        .bind(u64_i64(frame.block.number.0, "block_number")?)
        .bind(frame.block.hash.0.as_slice())
        .bind(i64::from(position))
        .execute(&mut **transaction)
        .await?;
    }
    Ok(())
}

async fn delete_recent_transaction_locators(
    transaction: &mut Transaction<'_, Sqlite>,
    chain_id: ChainId,
    block: BlockRef,
) -> Result<(), StoreError> {
    sqlx::query(
        "DELETE FROM recent_transaction_locator
         WHERE chain_id = ? AND block_number = ? AND block_hash = ?",
    )
    .bind(u64_i64(chain_id.0, "chain_id")?)
    .bind(u64_i64(block.number.0, "block_number")?)
    .bind(block.hash.0.as_slice())
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn load_handoff(
    pool: &SqlitePool,
    id: &str,
    descriptor: &ProcessorDescriptor,
) -> Result<Option<HotColdHandoffRecord>, StoreError> {
    let row = sqlx::query(
        "SELECT chain_id, processor_instance, overlap_from, overlap_to,
                anchor_hash, state, compared_blocks, failure, updated_at_unix_ms
         FROM hot_cold_handoffs WHERE handoff_id = ?",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;
    row.map(|row| {
        let stored_instance: String = row.try_get("processor_instance")?;
        let expected_instance = processor_instance(descriptor);
        if stored_instance != expected_instance {
            return Err(StoreError::HandoffMismatch {
                id: id.to_owned(),
                detail: "handoff belongs to another processor instance".to_owned(),
            });
        }
        let overlap = BlockRange::new(
            BlockNumber(i64_u64(row.try_get("overlap_from")?, "overlap_from")?),
            BlockNumber(i64_u64(row.try_get("overlap_to")?, "overlap_to")?),
        )
        .map_err(|error| StoreError::Invariant(error.to_string()))?;
        Ok(HotColdHandoffRecord {
            id: id.to_owned(),
            chain_id: ChainId(i64_u64(row.try_get("chain_id")?, "chain_id")?),
            processor_id: descriptor.id.to_string(),
            processor_version: descriptor.version.to_string(),
            overlap,
            anchor_hash: decode_hash(row.try_get("anchor_hash")?)?,
            state: HotColdHandoffState::parse(row.try_get("state")?)?,
            compared_blocks: i64_u64(row.try_get("compared_blocks")?, "compared_blocks")?,
            failure: row.try_get("failure")?,
            updated_at_unix_ms: i64_u64(row.try_get("updated_at_unix_ms")?, "updated_at_unix_ms")?,
        })
    })
    .transpose()
}

async fn load_archive_reconciliation(
    pool: &SqlitePool,
    id: &str,
    descriptor: &ProcessorDescriptor,
) -> Result<Option<ArchiveReconciliationRecord>, StoreError> {
    let row = sqlx::query(
        "SELECT chain_id, processor_instance, source_id, overlap_from,
                overlap_to, anchor_hash, state, compared_blocks, failure,
                updated_at_unix_ms
         FROM archive_reconciliations WHERE reconciliation_id = ?",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;
    row.map(|row| {
        let stored_instance: String = row.try_get("processor_instance")?;
        if stored_instance != processor_instance(descriptor) {
            return Err(StoreError::ArchiveReconciliationMismatch {
                id: id.to_owned(),
                detail: "reconciliation belongs to another processor instance".to_owned(),
            });
        }
        let overlap = BlockRange::new(
            BlockNumber(i64_u64(row.try_get("overlap_from")?, "overlap_from")?),
            BlockNumber(i64_u64(row.try_get("overlap_to")?, "overlap_to")?),
        )
        .map_err(|error| StoreError::Invariant(error.to_string()))?;
        Ok(ArchiveReconciliationRecord {
            id: id.to_owned(),
            chain_id: ChainId(i64_u64(row.try_get("chain_id")?, "chain_id")?),
            processor_id: descriptor.id.to_string(),
            processor_version: descriptor.version.to_string(),
            source_id: row.try_get("source_id")?,
            overlap,
            anchor_hash: decode_hash(row.try_get("anchor_hash")?)?,
            state: ArchiveReconciliationState::parse(row.try_get("state")?)?,
            compared_blocks: i64_u64(row.try_get("compared_blocks")?, "compared_blocks")?,
            failure: row.try_get("failure")?,
            updated_at_unix_ms: i64_u64(row.try_get("updated_at_unix_ms")?, "updated_at_unix_ms")?,
        })
    })
    .transpose()
}

fn validate_archive_reconciliation_identity(
    record: &ArchiveReconciliationRecord,
    source_id: &str,
    chain_id: ChainId,
    overlap: BlockRange,
    anchor_hash: BlockHash,
) -> Result<(), StoreError> {
    if record.source_id != source_id
        || record.chain_id != chain_id
        || record.overlap != overlap
        || record.anchor_hash != anchor_hash
    {
        return Err(StoreError::ArchiveReconciliationMismatch {
            id: record.id.clone(),
            detail: "reconciliation identity was reused with different immutable input".to_owned(),
        });
    }
    Ok(())
}

fn validate_handoff_identity(
    record: &HotColdHandoffRecord,
    chain_id: ChainId,
    overlap: BlockRange,
    anchor_hash: BlockHash,
) -> Result<(), StoreError> {
    if record.chain_id != chain_id || record.overlap != overlap || record.anchor_hash != anchor_hash
    {
        return Err(StoreError::HandoffMismatch {
            id: record.id.clone(),
            detail: "handoff identity was reused with different bounds or anchor".to_owned(),
        });
    }
    Ok(())
}

fn processor_instance(descriptor: &ProcessorDescriptor) -> String {
    descriptor.instance.to_string()
}

fn same_processor_identity(stored: &ProcessorDescriptor, configured: &ProcessorDescriptor) -> bool {
    let mut normalized = stored.clone();
    normalized.lifecycle = configured.lifecycle.clone();
    normalized == *configured
}

#[must_use]
pub fn default_delivery_stream_id(descriptor: &ProcessorDescriptor) -> String {
    let suffix = match descriptor.delivery_ordering {
        DeliveryOrdering::Canonical => "canonical",
        DeliveryOrdering::BlockVersionedIdempotent => "live",
    };
    format!("{}:{suffix}", descriptor.instance)
}

const fn default_delivery_stream_kind(descriptor: &ProcessorDescriptor) -> DeliveryStreamKind {
    match descriptor.delivery_ordering {
        DeliveryOrdering::Canonical => DeliveryStreamKind::Canonical,
        DeliveryOrdering::BlockVersionedIdempotent => DeliveryStreamKind::Live,
    }
}

#[must_use]
pub fn backfill_delivery_stream_id(
    descriptor: &ProcessorDescriptor,
    subscription_id: &str,
) -> String {
    let digest = blake3::hash(subscription_id.as_bytes());
    format!(
        "{}:backfill:{}",
        descriptor.instance,
        &hex::encode(digest.as_bytes())[..24]
    )
}

async fn delivery_stream_is_backfill(
    pool: &SqlitePool,
    stream_id: &str,
) -> Result<bool, StoreError> {
    let kind: String =
        sqlx::query_scalar("SELECT stream_kind FROM delivery_streams WHERE stream_id = ?")
            .bind(stream_id)
            .fetch_one(pool)
            .await?;
    Ok(DeliveryStreamKind::parse(&kind)? == DeliveryStreamKind::Backfill)
}

fn historical_batch_cursor(
    descriptor: &ProcessorDescriptor,
    item: &HistoricalBatchItem,
    sequence: u64,
) -> ProcessorCursor {
    ProcessorCursor {
        processor_id: descriptor.id.to_string(),
        processor_version: descriptor.version.to_string(),
        chain_id: item.delta.chain_id,
        block_number: item.delta.block.number,
        block_hash: item.delta.block.hash,
        finality: item.finality,
        sequence,
    }
}

fn backfill_progress_change(
    from_block: BlockNumber,
    through_block: BlockNumber,
    processed_blocks: u64,
) -> DomainChange {
    let mut payload = Vec::with_capacity(24);
    payload.extend_from_slice(&from_block.0.to_be_bytes());
    payload.extend_from_slice(&through_block.0.to_be_bytes());
    payload.extend_from_slice(&processed_blocks.to_be_bytes());
    DomainChange {
        kind: "system.backfill_progress".to_owned(),
        key: through_block.0.to_be_bytes().to_vec(),
        operation: ChangeOperation::Upsert,
        payload,
    }
}

fn backfill_progress_blocks(payload: &[u8]) -> Result<u64, StoreError> {
    if payload.is_empty() {
        return Ok(1);
    }
    let encoded: [u8; 24] = payload
        .try_into()
        .map_err(|_| StoreError::Invariant("invalid backfill progress payload".to_owned()))?;
    let from_block = u64::from_be_bytes(encoded[0..8].try_into().expect("slice is eight bytes"));
    let through_block =
        u64::from_be_bytes(encoded[8..16].try_into().expect("slice is eight bytes"));
    let processed_blocks =
        u64::from_be_bytes(encoded[16..24].try_into().expect("slice is eight bytes"));
    if from_block > through_block {
        return Err(StoreError::Invariant(
            "invalid backfill progress boundary".to_owned(),
        ));
    }
    Ok(processed_blocks)
}

async fn set_processor_run_state(
    pool: &SqlitePool,
    instance: &str,
    state: ProcessorRunState,
    reason: Option<&str>,
) -> Result<(), StoreError> {
    sqlx::query(
        "UPDATE processor_runtime_state
         SET state = ?, reason = ?, updated_at_unix_ms = ?
         WHERE instance = ?",
    )
    .bind(state.as_str())
    .bind(reason)
    .bind(now_i64()?)
    .bind(instance)
    .execute(pool)
    .await?;
    Ok(())
}

async fn enforce_physical_store_capacity(
    inner: &StoreInner,
    incoming_bytes: u64,
) -> Result<(), StoreError> {
    if inner.storage_budget.maximum_physical_bytes == u64::MAX {
        return Ok(());
    }
    let reserve = inner
        .artifact_segments
        .as_ref()
        .map_or(0, |storage| storage.maximum_segment_physical_bytes);
    enforce_physical_store_capacity_with_limit(
        inner,
        incoming_bytes,
        inner
            .storage_budget
            .maximum_physical_bytes
            .saturating_sub(reserve),
    )
    .await
}

async fn enforce_physical_store_capacity_for_compaction(
    inner: &StoreInner,
    incoming_bytes: u64,
) -> Result<(), StoreError> {
    enforce_physical_store_capacity_with_limit(
        inner,
        incoming_bytes,
        inner.storage_budget.maximum_physical_bytes,
    )
    .await
}

async fn enforce_physical_store_capacity_with_limit(
    inner: &StoreInner,
    incoming_bytes: u64,
    limit: u64,
) -> Result<(), StoreError> {
    if limit == u64::MAX {
        return Ok(());
    }
    let wal_path = PathBuf::from(format!("{}-wal", inner.path.to_string_lossy()));
    let segment_bytes = if let Some(storage) = &inner.artifact_segments {
        storage.sink.stats().await.physical_bytes
    } else {
        0
    };
    let mut current = file_bytes(&inner.path)?
        .saturating_add(file_bytes(&wal_path)?)
        .saturating_add(segment_bytes);
    let mut projected = current
        .checked_add(incoming_bytes)
        .ok_or(StoreError::Numeric("projected physical store bytes"))?;
    if projected <= limit {
        return Ok(());
    }

    // Pressure is rare. Reconcile reusable pages and a completed WAL before
    // rejecting a commit so a bounded window is not paused by stale physical
    // accounting. This runs before the caller opens its write transaction.
    sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
        .execute(&inner.pool)
        .await?;
    sqlx::query("PRAGMA incremental_vacuum(1024)")
        .execute(&inner.pool)
        .await?;
    current = file_bytes(&inner.path)?
        .saturating_add(file_bytes(&wal_path)?)
        .saturating_add(segment_bytes);
    projected = current
        .checked_add(incoming_bytes)
        .ok_or(StoreError::Numeric("projected physical store bytes"))?;
    if projected <= limit {
        return Ok(());
    }
    Err(StoreError::PhysicalStorageLimit {
        limit_bytes: limit,
        projected_bytes: projected,
    })
}

async fn enforce_artifact_storage_capacity(
    transaction: &mut Transaction<'_, Sqlite>,
    budget: ArtifactStorageBudget,
) -> Result<(), StoreError> {
    let (retained, pending): (i64, i64) = sqlx::query_as(
        "SELECT COALESCE(SUM(retained_bytes), 0), COALESCE(SUM(pending_bytes), 0)
         FROM processor_artifact_totals",
    )
    .fetch_one(&mut **transaction)
    .await?;
    let retained = i64_u64(retained, "retained processor artifact bytes")?;
    let pending = i64_u64(pending, "pending processor artifact bytes")?;
    if retained > budget.maximum_retained_bytes {
        return Err(StoreError::ArtifactStorageLimit {
            scope: "retained",
            limit_bytes: budget.maximum_retained_bytes,
            projected_bytes: retained,
        });
    }
    if pending > budget.maximum_pending_bytes {
        return Err(StoreError::ArtifactStorageLimit {
            scope: "pending",
            limit_bytes: budget.maximum_pending_bytes,
            projected_bytes: pending,
        });
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Default)]
struct ArtifactTotalsDelta {
    added_artifacts: u64,
    added_bytes: u64,
    added_owners: u64,
    removed_artifacts: u64,
    removed_bytes: u64,
    removed_owners: u64,
}

async fn begin_bulk_artifact_accounting(
    transaction: &mut Transaction<'_, Sqlite>,
    instance: &str,
) -> Result<(), StoreError> {
    let inserted =
        sqlx::query("INSERT INTO processor_artifact_bulk_accounting(instance) VALUES (?)")
            .bind(instance)
            .execute(&mut **transaction)
            .await?
            .rows_affected();
    if inserted != 1 {
        return Err(StoreError::Invariant(format!(
            "processor artifact bulk accounting is already active for {instance}"
        )));
    }
    Ok(())
}

async fn finish_bulk_artifact_accounting(
    transaction: &mut Transaction<'_, Sqlite>,
    instance: &str,
    delta: ArtifactTotalsDelta,
) -> Result<(), StoreError> {
    let updated = sqlx::query(
        "UPDATE processor_artifact_totals
         SET retained_artifacts = retained_artifacts + ? - ?,
             retained_bytes = retained_bytes + ? - ?,
             retained_owners = retained_owners + ? - ?
         WHERE instance = ?",
    )
    .bind(u64_i64(delta.added_artifacts, "added processor artifacts")?)
    .bind(u64_i64(
        delta.removed_artifacts,
        "removed processor artifacts",
    )?)
    .bind(u64_i64(
        delta.added_bytes,
        "added processor artifact bytes",
    )?)
    .bind(u64_i64(
        delta.removed_bytes,
        "removed processor artifact bytes",
    )?)
    .bind(u64_i64(
        delta.added_owners,
        "added processor artifact owners",
    )?)
    .bind(u64_i64(
        delta.removed_owners,
        "removed processor artifact owners",
    )?)
    .bind(instance)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    let cleared = sqlx::query("DELETE FROM processor_artifact_bulk_accounting WHERE instance = ?")
        .bind(instance)
        .execute(&mut **transaction)
        .await?
        .rows_affected();
    if updated != 1 || cleared != 1 {
        return Err(StoreError::Invariant(format!(
            "processor artifact bulk accounting lost its durable row for {instance}"
        )));
    }
    Ok(())
}

fn estimated_retained_mutation_bytes(
    mutations: &[Mutation],
    retain_output: bool,
) -> Result<u64, StoreError> {
    mutations.iter().try_fold(0_u64, |total, mutation| {
        let bytes = match mutation {
            Mutation::Entity {
                collection,
                key,
                after,
                ..
            } if retain_output => collection
                .len()
                .saturating_add(key.len())
                .saturating_add(after.as_ref().map_or(0, Vec::len)),
            Mutation::Index {
                index,
                index_key,
                entity_key,
                after,
                ..
            } if retain_output && *after => index
                .len()
                .saturating_add(index_key.len())
                .saturating_add(entity_key.len()),
            Mutation::State {
                namespace,
                key,
                after,
                ..
            } => namespace
                .len()
                .saturating_add(key.len())
                .saturating_add(after.as_ref().map_or(0, Vec::len)),
            _ => 0,
        };
        total
            .checked_add(
                u64::try_from(bytes).map_err(|_| {
                    StoreError::Numeric("retained mutation physical admission bytes")
                })?,
            )
            .ok_or(StoreError::Numeric(
                "retained mutation physical admission bytes",
            ))
    })
}

#[allow(clippy::too_many_lines)]
async fn enforce_delivery_capacity(
    inner: &StoreInner,
    descriptor: &ProcessorDescriptor,
    instance: &str,
    stream_id: &str,
    incoming_bytes: u64,
    incoming_work_blocks: u64,
) -> Result<(), StoreError> {
    let pool = &inner.pool;
    let stream_kind: String =
        sqlx::query_scalar("SELECT stream_kind FROM delivery_streams WHERE stream_id = ?")
            .bind(stream_id)
            .fetch_one(pool)
            .await?;
    let stream_kind = DeliveryStreamKind::parse(&stream_kind)?;
    let processor_scoped = stream_kind != DeliveryStreamKind::Backfill;
    let backfill_limits = if stream_kind == DeliveryStreamKind::Backfill {
        let row = sqlx::query(
            "SELECT state, effective_block_limit, effective_byte_limit,
                    resume_below_ratio_millionths, processed_work_blocks,
                    (
                        SELECT MIN(acknowledged_work_blocks)
                        FROM durable_consumers
                        WHERE stream_id = backfill_subscriptions.history_stream_id
                          AND role = 'required'
                          AND state = 'active'
                    ) AS required_acknowledged_work_blocks
             FROM backfill_subscriptions
             WHERE history_stream_id = ?",
        )
        .bind(stream_id)
        .fetch_optional(pool)
        .await?;
        row.map(|row| {
            Ok::<_, StoreError>((
                BackfillSubscriptionState::parse(row.try_get("state")?)?,
                i64_u64(
                    row.try_get("effective_block_limit")?,
                    "subscription block limit",
                )?,
                i64_u64(
                    row.try_get("effective_byte_limit")?,
                    "subscription byte limit",
                )?,
                i64_u64(
                    row.try_get("resume_below_ratio_millionths")?,
                    "subscription resume ratio",
                )?,
                i64_u64(
                    row.try_get("processed_work_blocks")?,
                    "subscription processed work blocks",
                )?,
                row.try_get::<Option<i64>, _>("required_acknowledged_work_blocks")?
                    .map(|value| i64_u64(value, "required acknowledged work blocks"))
                    .transpose()?
                    .unwrap_or(0),
            ))
        })
        .transpose()?
    } else {
        None
    };
    let state = if processor_scoped {
        let state: String =
            sqlx::query_scalar("SELECT state FROM processor_runtime_state WHERE instance = ?")
                .bind(instance)
                .fetch_one(pool)
                .await?;
        ProcessorRunState::parse(&state)?
    } else {
        ProcessorRunState::Running
    };
    if state == ProcessorRunState::Failed {
        return Err(StoreError::ProcessorFailed(instance.to_owned()));
    }
    let current: i64 =
        sqlx::query_scalar("SELECT live_bytes FROM delivery_streams WHERE stream_id = ?")
            .bind(stream_id)
            .fetch_one(pool)
            .await?;
    let current = i64_u64(current, "delivery stream bytes")?;
    let limit = backfill_limits
        .as_ref()
        .map_or(descriptor.lifecycle.delivery.max_bytes, |limits| {
            limits.2.min(descriptor.lifecycle.delivery.max_bytes)
        });
    let indivisible_block_limit = backfill_limits.as_ref().map(|limits| limits.1);
    if incoming_bytes > limit
        || indivisible_block_limit.is_some_and(|block_limit| incoming_work_blocks > block_limit)
    {
        if processor_scoped {
            set_processor_run_state(
                pool,
                instance,
                ProcessorRunState::Failed,
                Some("single_block_exceeds_delivery_limit"),
            )
            .await?;
        }
        return Err(StoreError::DeliveryItemTooLarge {
            instance: instance.to_owned(),
            observed_bytes: incoming_bytes,
            maximum_bytes: limit,
            observed_work_blocks: incoming_work_blocks,
            maximum_work_blocks: indivisible_block_limit,
        });
    }
    let low_water = limit.saturating_mul(9) / 10;
    if state == ProcessorRunState::Paused {
        if current > low_water {
            return Err(StoreError::ProcessorPaused {
                instance: instance.to_owned(),
                current_bytes: current,
                resume_below_bytes: low_water,
            });
        }
        set_processor_run_state(pool, instance, ProcessorRunState::Running, None).await?;
    }
    let projected = current
        .checked_add(incoming_bytes)
        .ok_or(StoreError::Numeric("projected delivery bytes"))?;
    let (total_retained, history_retained): (i64, i64) = sqlx::query_as(
        "SELECT COALESCE(SUM(live_bytes), 0),
                COALESCE(SUM(CASE WHEN stream_kind = 'backfill' THEN live_bytes ELSE 0 END), 0)
         FROM delivery_streams",
    )
    .fetch_one(pool)
    .await?;
    let total_retained = i64_u64(total_retained, "total retained delivery bytes")?;
    let history_retained = i64_u64(history_retained, "history retained delivery bytes")?;
    let projected_total_retained =
        total_retained
            .checked_add(incoming_bytes)
            .ok_or(StoreError::Numeric(
                "projected total retained delivery bytes",
            ))?;
    let projected_history_retained = history_retained
        .checked_add(if stream_kind == DeliveryStreamKind::Backfill {
            incoming_bytes
        } else {
            0
        })
        .ok_or(StoreError::Numeric(
            "projected history retained delivery bytes",
        ))?;
    let wal_path = PathBuf::from(format!("{}-wal", inner.path.to_string_lossy()));
    let physical_bytes = file_bytes(&inner.path)?.saturating_add(file_bytes(&wal_path)?);
    let projected_physical_bytes = physical_bytes
        .checked_add(incoming_bytes)
        .ok_or(StoreError::Numeric("projected physical store bytes"))?;
    let blocks_within_limit =
        backfill_limits
            .as_ref()
            .is_none_or(|(_, block_limit, _, _, processed, acknowledged)| {
                processed
                    .saturating_sub(*acknowledged)
                    .saturating_add(incoming_work_blocks)
                    <= *block_limit
            });
    let below_resume_water = backfill_limits.as_ref().is_none_or(
        |(subscription_state, block_limit, byte_limit, ratio, processed, acknowledged)| {
            if *subscription_state != BackfillSubscriptionState::Backpressured {
                return true;
            }
            let byte_low_water = byte_limit.saturating_mul(*ratio) / 1_000_000;
            let block_low_water = block_limit.saturating_mul(*ratio) / 1_000_000;
            current <= byte_low_water && processed.saturating_sub(*acknowledged) <= block_low_water
        },
    );
    let budget_failure =
        if projected_history_retained > inner.delivery_budget.maximum_history_retained_bytes {
            Some((
                "node_history_retained",
                inner.delivery_budget.maximum_history_retained_bytes,
                projected_history_retained,
            ))
        } else if projected_total_retained > inner.delivery_budget.maximum_retained_bytes {
            Some((
                "node_total_retained",
                inner.delivery_budget.maximum_retained_bytes,
                projected_total_retained,
            ))
        } else if projected_physical_bytes > inner.storage_budget.maximum_physical_bytes {
            Some((
                "node_physical",
                inner.storage_budget.maximum_physical_bytes,
                projected_physical_bytes,
            ))
        } else {
            None
        };
    if projected <= limit && blocks_within_limit && below_resume_water && budget_failure.is_none() {
        return Ok(());
    }
    let action = descriptor.lifecycle.delivery.on_limit;
    if matches!(action, DeliveryLimitAction::ExpireAndReset) {
        sqlx::query(
            "UPDATE durable_consumers
             SET state = 'reset_required', updated_at_unix_ms = ?
             WHERE stream_id = ? AND role = 'required' AND state = 'active'",
        )
        .bind(now_i64()?)
        .bind(stream_id)
        .execute(pool)
        .await?;
    }
    if processor_scoped {
        let state = if matches!(action, DeliveryLimitAction::Fail) {
            ProcessorRunState::Failed
        } else {
            ProcessorRunState::Paused
        };
        set_processor_run_state(pool, instance, state, Some("delivery_spool_hard_limit")).await?;
    }
    let (scope, effective_limit, effective_projected) =
        budget_failure.unwrap_or(("stream", limit, projected));
    Err(StoreError::DeliveryLimit {
        instance: instance.to_owned(),
        action,
        scope,
        limit_bytes: effective_limit,
        projected_bytes: effective_projected,
    })
}

fn valid_consumer_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn valid_subscription_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
}

fn retains_processor_output(descriptor: &ProcessorDescriptor) -> bool {
    !matches!(descriptor.lifecycle.output.mode, OutputPolicyMode::None)
}

fn decode_consumer(
    row: &SqliteRow,
    processor_instance: String,
    now_unix_ms: i64,
) -> Result<DurableConsumer, StoreError> {
    let lease_expires_at_unix_ms = i64_u64(
        row.try_get("lease_expires_at_unix_ms")?,
        "consumer lease expiry",
    )?;
    Ok(DurableConsumer {
        consumer_id: row.try_get("consumer_id")?,
        processor_instance,
        stream_id: row.try_get("stream_id")?,
        role: ConsumerRole::parse(row.try_get("role")?)?,
        state: ConsumerState::parse(row.try_get("state")?)?,
        acknowledged_sequence: i64_u64(
            row.try_get("acknowledged_sequence")?,
            "consumer acknowledged sequence",
        )?,
        delivered_sequence: i64_u64(
            row.try_get("delivered_sequence")?,
            "consumer delivered sequence",
        )?,
        lease_generation: i64_u64(
            row.try_get("lease_generation")?,
            "consumer lease generation",
        )?,
        lease_ttl_ms: i64_u64(row.try_get("lease_ttl_ms")?, "consumer lease TTL")?,
        lease_expires_at_unix_ms,
        lease_active: lease_expires_at_unix_ms > i64_u64(now_unix_ms, "current wall clock")?,
        created_at_unix_ms: i64_u64(row.try_get("created_at_unix_ms")?, "consumer creation time")?,
        updated_at_unix_ms: i64_u64(row.try_get("updated_at_unix_ms")?, "consumer update time")?,
    })
}

fn decode_backfill_subscription(row: &SqliteRow) -> Result<BackfillSubscriptionRecord, StoreError> {
    let from_block = BlockNumber(i64_u64(
        row.try_get("from_block")?,
        "subscription start block",
    )?);
    let to_block = BlockNumber(i64_u64(row.try_get("to_block")?, "subscription end block")?);
    let range = BlockRange::new(from_block, to_block)
        .map_err(|error| StoreError::Invariant(error.to_string()))?;
    Ok(BackfillSubscriptionRecord {
        subscription_id: row.try_get("subscription_id")?,
        job_id: row.try_get("job_id")?,
        processor_instance: row.try_get("instance")?,
        history_stream_id: row.try_get("history_stream_id")?,
        mode: BackfillSubscriptionMode::parse(row.try_get("mode")?)?,
        publication_revision: i64_u64(
            row.try_get("publication_revision")?,
            "subscription publication revision",
        )?,
        state: BackfillSubscriptionState::parse(row.try_get("state")?)?,
        consumer_id: row.try_get("consumer_id")?,
        ranges: Vec::new(),
        range,
        preexisting_coverage: Vec::new(),
        captured_finalized_target: BlockNumber(i64_u64(
            row.try_get("captured_finalized_target")?,
            "captured finalized target",
        )?),
        idempotency_key: row.try_get("idempotency_key")?,
        effective_block_limit: i64_u64(
            row.try_get("effective_block_limit")?,
            "subscription block limit",
        )?,
        effective_byte_limit: i64_u64(
            row.try_get("effective_byte_limit")?,
            "subscription byte limit",
        )?,
        resume_below_ratio_millionths: u32::try_from(i64_u64(
            row.try_get("resume_below_ratio_millionths")?,
            "subscription resume ratio",
        )?)
        .map_err(|_| StoreError::Numeric("subscription resume ratio"))?,
        delivery_batch_limits: BackfillDeliveryBatchLimits {
            target_encoded_bytes: i64_u64(
                row.try_get("delivery_target_encoded_bytes")?,
                "delivery target encoded bytes",
            )?,
            maximum_encoded_bytes: i64_u64(
                row.try_get("delivery_maximum_encoded_bytes")?,
                "delivery maximum encoded bytes",
            )?,
            maximum_events: i64_u64(
                row.try_get("delivery_maximum_events")?,
                "delivery maximum events",
            )?,
            maximum_processed_blocks: i64_u64(
                row.try_get("delivery_maximum_processed_blocks")?,
                "delivery maximum processed blocks",
            )?,
            maximum_delay_ms: i64_u64(
                row.try_get("delivery_maximum_delay_ms")?,
                "delivery maximum delay",
            )?,
            maximum_buffered_batches: i64_u64(
                row.try_get("delivery_maximum_buffered_batches")?,
                "delivery maximum buffered batches",
            )?,
            maximum_buffered_bytes: i64_u64(
                row.try_get("delivery_maximum_buffered_bytes")?,
                "delivery maximum buffered bytes",
            )?,
            compression: BackfillDeliveryCompression::parse(row.try_get("delivery_compression")?)?,
        },
        initial_sequence: i64_u64(
            row.try_get("initial_sequence")?,
            "initial subscription sequence",
        )?,
        completion_sequence: row
            .try_get::<Option<i64>, _>("completion_sequence")?
            .map(|value| i64_u64(value, "subscription completion sequence"))
            .transpose()?,
        processed_work_blocks: i64_u64(
            row.try_get("processed_work_blocks")?,
            "processed subscription blocks",
        )?,
    })
}

fn normalized_subscription_ranges(
    subscription: &BackfillSubscriptionRecord,
) -> Result<Vec<BlockRange>, StoreError> {
    let mut ranges = if subscription.ranges.is_empty() {
        vec![subscription.range]
    } else {
        subscription.ranges.clone()
    };
    ranges.sort_unstable_by_key(|range| (range.start(), range.end()));
    let mut normalized = Vec::<BlockRange>::with_capacity(ranges.len());
    for range in ranges {
        if let Some(previous) = normalized.last_mut()
            && range.start().0 <= previous.end().0.saturating_add(1)
        {
            *previous = BlockRange::new(previous.start(), previous.end().max(range.end()))
                .map_err(|error| StoreError::InvalidConfig(error.to_string()))?;
        } else {
            normalized.push(range);
        }
    }
    let Some(first) = normalized.first() else {
        return Err(StoreError::InvalidConfig(
            "backfill subscription range set must not be empty".to_owned(),
        ));
    };
    let bounding = BlockRange::new(
        first.start(),
        normalized
            .last()
            .expect("normalized range set is non-empty")
            .end(),
    )
    .map_err(|error| StoreError::InvalidConfig(error.to_string()))?;
    if bounding != subscription.range {
        return Err(StoreError::InvalidConfig(
            "backfill subscription bounding range does not match its range set".to_owned(),
        ));
    }
    Ok(normalized)
}

fn normalize_ranges(mut ranges: Vec<BlockRange>) -> Result<Vec<BlockRange>, StoreError> {
    ranges.sort_unstable_by_key(|range| (range.start(), range.end()));
    let mut normalized = Vec::<BlockRange>::with_capacity(ranges.len());
    for range in ranges {
        if let Some(previous) = normalized.last_mut()
            && range.start().0 <= previous.end().0.saturating_add(1)
        {
            *previous = BlockRange::new(previous.start(), previous.end().max(range.end()))
                .map_err(|error| StoreError::InvalidConfig(error.to_string()))?;
        } else {
            normalized.push(range);
        }
    }
    Ok(normalized)
}

fn validate_cursor(
    cursor: &ProcessorCursor,
    descriptor: &ProcessorDescriptor,
    delta: &EncodedDelta,
) -> Result<(), StoreError> {
    if cursor.processor_id != descriptor.id.as_str()
        || cursor.processor_version != descriptor.version.to_string()
        || cursor.chain_id != delta.chain_id
        || cursor.block_number != delta.block.number
        || cursor.block_hash != delta.block.hash
    {
        return Err(StoreError::Processor(ProcessorError::CursorMismatch));
    }
    Ok(())
}

fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut output = prefix.to_vec();
    for index in (0..output.len()).rev() {
        if output[index] != u8::MAX {
            output[index] = output[index].saturating_add(1);
            output.truncate(index + 1);
            return Some(output);
        }
    }
    None
}

fn merge_block_numbers(numbers: Vec<u64>) -> Result<Vec<BlockRange>, StoreError> {
    let mut output = Vec::new();
    let mut iterator = numbers.into_iter();
    let Some(mut start) = iterator.next() else {
        return Ok(output);
    };
    let mut end = start;
    for number in iterator {
        if number == end {
            continue;
        }
        if number == end.saturating_add(1) {
            end = number;
            continue;
        }
        output.push(
            BlockRange::new(BlockNumber(start), BlockNumber(end))
                .map_err(|error| StoreError::Invariant(error.to_string()))?,
        );
        start = number;
        end = number;
    }
    output.push(
        BlockRange::new(BlockNumber(start), BlockNumber(end))
            .map_err(|error| StoreError::Invariant(error.to_string()))?,
    );
    Ok(output)
}

fn compact_segment_digest(
    start: BlockNumber,
    end: BlockNumber,
    start_parent_hash: BlockHash,
    end_hash: BlockHash,
) -> BlockHash {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"leani.finalized-coverage-segment.v1");
    hasher.update(&start.0.to_be_bytes());
    hasher.update(&end.0.to_be_bytes());
    hasher.update(start_parent_hash.0.as_slice());
    hasher.update(end_hash.0.as_slice());
    BlockHash::new(*hasher.finalize().as_bytes())
}

fn artifact_export_digest(export: &ProcessorArtifactExport) -> BlockHash {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"leani.processor-artifact-export.v1");
    hash_artifact_export_bytes(&mut hasher, export.contract.processor_id.as_bytes());
    hash_artifact_export_bytes(&mut hasher, export.contract.processor_version.as_bytes());
    hasher.update(export.contract.code_hash.0.as_slice());
    hasher.update(export.contract.config_hash.0.as_slice());
    hasher.update(&export.contract.delta_schema_version.to_be_bytes());
    hasher.update(&export.requested_range.start().0.to_be_bytes());
    hasher.update(&export.requested_range.end().0.to_be_bytes());
    if let Some(exported) = export.exported_range {
        hasher.update(&[1]);
        hasher.update(&exported.start().0.to_be_bytes());
        hasher.update(&exported.end().0.to_be_bytes());
    } else {
        hasher.update(&[0]);
    }
    hasher.update(&[u8::from(export.complete)]);
    hasher.update(
        &u64::try_from(export.artifacts.len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    for artifact in &export.artifacts {
        hasher.update(&artifact.chain_id.0.to_be_bytes());
        hasher.update(&artifact.block.number.0.to_be_bytes());
        hasher.update(artifact.block.hash.0.as_slice());
        hasher.update(artifact.checksum.0.as_slice());
    }
    BlockHash::new(*hasher.finalize().as_bytes())
}

fn hash_artifact_export_bytes(hasher: &mut blake3::Hasher, bytes: &[u8]) {
    hasher.update(&u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_be_bytes());
    hasher.update(bytes);
}

async fn extend_shared_coverage_owner(
    transaction: &mut Transaction<'_, Sqlite>,
    instance: &str,
    block: BlockNumber,
) -> Result<(), StoreError> {
    extend_shared_coverage_owner_range(transaction, instance, BlockRange::single(block)).await
}

async fn extend_shared_coverage_owner_range(
    transaction: &mut Transaction<'_, Sqlite>,
    instance: &str,
    range: BlockRange,
) -> Result<(), StoreError> {
    let latest: Option<(i64, i64)> = sqlx::query_as(
        "SELECT from_block, to_block FROM finalized_coverage_owners
         WHERE instance = ? AND owner_kind = 'shared' AND owner_id = 'canonical'
         ORDER BY to_block DESC LIMIT 1",
    )
    .bind(instance)
    .fetch_optional(&mut **transaction)
    .await?;
    if let Some((from, to)) = latest {
        let to = i64_u64(to, "shared coverage owner end")?;
        if range.start().0 <= to.saturating_add(1) {
            sqlx::query(
                "UPDATE finalized_coverage_owners
                 SET to_block = MAX(to_block, ?)
                 WHERE instance = ? AND owner_kind = 'shared'
                   AND owner_id = 'canonical' AND from_block = ?",
            )
            .bind(u64_i64(range.end().0, "shared coverage owner end")?)
            .bind(instance)
            .bind(from)
            .execute(&mut **transaction)
            .await?;
            return Ok(());
        }
    }
    sqlx::query(
        "INSERT INTO finalized_coverage_owners(
            instance, owner_kind, owner_id, from_block, to_block
         ) VALUES (?, 'shared', 'canonical', ?, ?)",
    )
    .bind(instance)
    .bind(u64_i64(range.start().0, "shared coverage owner start")?)
    .bind(u64_i64(range.end().0, "shared coverage owner end")?)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn table_count(pool: &SqlitePool, table: &str) -> Result<u64, StoreError> {
    const TABLES: &[&str] = &[
        "processor_instances",
        "entities",
        "entity_indexes",
        "processor_coverage",
        "finalized_coverage_intervals",
        "finalized_coverage_segments",
        "finalized_coverage_owners",
        "applied_blocks",
        "undo_journal",
        "change_log",
        "processor_artifact_owners",
    ];
    if !TABLES.contains(&table) {
        return Err(StoreError::Invariant("invalid count table".to_owned()));
    }
    let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
        .fetch_one(pool)
        .await?;
    i64_u64(count, "table count")
}

async fn instance_count_bytes(
    pool: &SqlitePool,
    statement: &'static str,
    instance: &str,
) -> Result<(u64, u64), StoreError> {
    let row = sqlx::query(statement)
        .bind(instance)
        .fetch_one(pool)
        .await?;
    Ok((
        i64_u64(row.try_get(0)?, "processor row count")?,
        i64_u64(row.try_get(1)?, "processor payload bytes")?,
    ))
}

async fn database_bytes(pool: &SqlitePool) -> Result<u64, StoreError> {
    let page_count: i64 = sqlx::query_scalar("PRAGMA page_count")
        .fetch_one(pool)
        .await?;
    let page_size: i64 = sqlx::query_scalar("PRAGMA page_size")
        .fetch_one(pool)
        .await?;
    i64_u64(
        page_count
            .checked_mul(page_size)
            .ok_or_else(|| StoreError::Invariant("database size overflow".to_owned()))?,
        "database bytes",
    )
}

async fn freelist_bytes(pool: &SqlitePool) -> Result<u64, StoreError> {
    let page_count: i64 = sqlx::query_scalar("PRAGMA freelist_count")
        .fetch_one(pool)
        .await?;
    let page_size: i64 = sqlx::query_scalar("PRAGMA page_size")
        .fetch_one(pool)
        .await?;
    i64_u64(
        page_count
            .checked_mul(page_size)
            .ok_or_else(|| StoreError::Invariant("freelist size overflow".to_owned()))?,
        "freelist bytes",
    )
}

fn file_bytes(path: &Path) -> Result<u64, StoreError> {
    match std::fs::metadata(path) {
        Ok(metadata) => Ok(metadata.len()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(StoreError::Io(error)),
    }
}

fn operation_i64(operation: ChangeOperation) -> i64 {
    match operation {
        ChangeOperation::Upsert => 0,
        ChangeOperation::Delete => 1,
    }
}

fn decode_operation(value: i64) -> Result<ChangeOperation, StoreError> {
    match value {
        0 => Ok(ChangeOperation::Upsert),
        1 => Ok(ChangeOperation::Delete),
        _ => Err(StoreError::Invariant(format!(
            "invalid change operation {value}"
        ))),
    }
}

fn finality_i64(finality: Finality) -> i64 {
    match finality {
        Finality::Optimistic => 0,
        Finality::Safe => 1,
        Finality::Finalized => 2,
    }
}

fn decode_finality(value: i64) -> Result<Finality, StoreError> {
    match value {
        0 => Ok(Finality::Optimistic),
        1 => Ok(Finality::Safe),
        2 => Ok(Finality::Finalized),
        _ => Err(StoreError::Invariant(format!(
            "invalid finality value {value}"
        ))),
    }
}

fn decode_hash(value: Vec<u8>) -> Result<BlockHash, StoreError> {
    let hash: [u8; 32] = value
        .try_into()
        .map_err(|_| StoreError::Invariant("stored hash is not 32 bytes".to_owned()))?;
    Ok(BlockHash::new(hash))
}

fn relative_segment_path(root: &Path, path: &Path) -> Result<String, StoreError> {
    let relative = path.strip_prefix(root).map_err(|_| {
        StoreError::Invariant("artifact segment escaped its configured root".to_owned())
    })?;
    if relative.as_os_str().is_empty()
        || relative
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(StoreError::Invariant(
            "artifact segment has a non-portable relative path".to_owned(),
        ));
    }
    relative
        .to_str()
        .map(str::to_owned)
        .ok_or_else(|| StoreError::Invariant("artifact segment path is not UTF-8".to_owned()))
}

fn artifact_segment_id(
    descriptor: &ProcessorDescriptor,
    range: BlockRange,
    records_checksum: [u8; 32],
) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(descriptor.instance.as_str().as_bytes());
    hasher.update(&range.start().0.to_be_bytes());
    hasher.update(&range.end().0.to_be_bytes());
    hasher.update(&records_checksum);
    hasher.finalize().to_hex().to_string()
}

async fn validate_inline_artifacts_unchanged(
    transaction: &mut Transaction<'_, Sqlite>,
    descriptor: &ProcessorDescriptor,
    range: BlockRange,
    expected: &[ProcessorArtifact],
) -> Result<(), StoreError> {
    let rows = sqlx::query(
        "SELECT chain_id, block_number, block_hash, parent_hash,
                block_timestamp, delta_schema_version, delta_checksum,
                encoded_delta, retained_at_unix_ms
         FROM processor_artifacts
         WHERE instance = ? AND block_number BETWEEN ? AND ?
           AND payload_tier = 'inline'
         ORDER BY block_number",
    )
    .bind(processor_instance(descriptor))
    .bind(u64_i64(range.start().0, "artifact segment start")?)
    .bind(u64_i64(range.end().0, "artifact segment end")?)
    .fetch_all(&mut **transaction)
    .await?;
    if rows.len() != expected.len() {
        return Err(StoreError::Invariant(
            "artifact compaction input changed before publication".to_owned(),
        ));
    }
    for (row, expected) in rows.iter().zip(expected) {
        if decode_processor_artifact(descriptor, row)? != *expected {
            return Err(StoreError::Invariant(
                "artifact compaction input changed before publication".to_owned(),
            ));
        }
    }
    Ok(())
}

async fn validate_artifact_segment_catalog_row(
    transaction: &mut Transaction<'_, Sqlite>,
    descriptor: &ProcessorDescriptor,
    segment_id: &str,
    relative_path: &str,
    receipt: &ArtifactBatchReceipt,
) -> Result<(), StoreError> {
    let row = sqlx::query(
        "SELECT instance, from_block, to_block, relative_path, artifacts,
                logical_bytes, physical_bytes, records_checksum
         FROM processor_artifact_segments WHERE segment_id = ?",
    )
    .bind(segment_id)
    .fetch_one(&mut **transaction)
    .await?;
    let checksum: Vec<u8> = row.try_get("records_checksum")?;
    if row.try_get::<String, _>("instance")? != descriptor.instance.as_str()
        || i64_u64(row.try_get("from_block")?, "artifact segment start")? != receipt.range.start().0
        || i64_u64(row.try_get("to_block")?, "artifact segment end")? != receipt.range.end().0
        || row.try_get::<String, _>("relative_path")? != relative_path
        || i64_u64(row.try_get("artifacts")?, "artifact segment count")? != receipt.artifacts
        || i64_u64(
            row.try_get("logical_bytes")?,
            "artifact segment logical bytes",
        )? != receipt.logical_bytes
        || i64_u64(
            row.try_get("physical_bytes")?,
            "artifact segment physical bytes",
        )? != receipt.physical_bytes
        || checksum.as_slice() != receipt.records_checksum
    {
        return Err(StoreError::Invariant(
            "artifact segment catalog identity conflicts with its file".to_owned(),
        ));
    }
    Ok(())
}

async fn move_inline_artifact_owners_to_segment(
    transaction: &mut Transaction<'_, Sqlite>,
    descriptor: &ProcessorDescriptor,
    segment_id: &str,
    range: BlockRange,
) -> Result<u64, StoreError> {
    let rows = sqlx::query(
        "SELECT block_number, owner_kind, owner_id, created_at_unix_ms
         FROM processor_artifact_owners
         WHERE instance = ? AND block_number BETWEEN ? AND ?
         ORDER BY owner_kind, owner_id, block_number",
    )
    .bind(processor_instance(descriptor))
    .bind(u64_i64(range.start().0, "artifact segment owner start")?)
    .bind(u64_i64(range.end().0, "artifact segment owner end")?)
    .fetch_all(&mut **transaction)
    .await?;
    let moved_owners = u64::try_from(rows.len())
        .map_err(|_| StoreError::Numeric("moved processor artifact owners"))?;
    let mut grouped = BTreeMap::<(String, String), Vec<(u64, i64)>>::new();
    for row in rows {
        grouped
            .entry((row.try_get("owner_kind")?, row.try_get("owner_id")?))
            .or_default()
            .push((
                i64_u64(row.try_get("block_number")?, "artifact owner block")?,
                row.try_get("created_at_unix_ms")?,
            ));
    }
    for ((kind, owner_id), blocks) in grouped {
        let mut start = None::<u64>;
        let mut end = 0_u64;
        let mut created = i64::MAX;
        for (block, created_at) in blocks {
            if start.is_some() && block != end.saturating_add(1) {
                if let Some(range_start) = start {
                    insert_segment_owner_range(
                        transaction,
                        segment_id,
                        &kind,
                        &owner_id,
                        range_start,
                        end,
                        created,
                    )
                    .await?;
                }
                start = None;
                created = i64::MAX;
            }
            start.get_or_insert(block);
            end = block;
            created = created.min(created_at);
        }
        if let Some(start) = start {
            insert_segment_owner_range(
                transaction,
                segment_id,
                &kind,
                &owner_id,
                start,
                end,
                created,
            )
            .await?;
        }
    }
    Ok(moved_owners)
}

async fn insert_segment_owner_range(
    transaction: &mut Transaction<'_, Sqlite>,
    segment_id: &str,
    kind: &str,
    owner_id: &str,
    from_block: u64,
    to_block: u64,
    created_at_unix_ms: i64,
) -> Result<(), StoreError> {
    sqlx::query(
        "INSERT OR IGNORE INTO processor_artifact_segment_owners(
            segment_id, owner_kind, owner_id, from_block, to_block, created_at_unix_ms
         ) VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(segment_id)
    .bind(kind)
    .bind(owner_id)
    .bind(u64_i64(from_block, "artifact segment owner start")?)
    .bind(u64_i64(to_block, "artifact segment owner end")?)
    .bind(created_at_unix_ms)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn add_or_merge_segment_owner(
    transaction: &mut Transaction<'_, Sqlite>,
    segment_id: &str,
    kind: ArtifactOwnerKind,
    owner_id: &str,
    from_block: u64,
    to_block: u64,
    created_at_unix_ms: i64,
) -> Result<u64, StoreError> {
    let rows = sqlx::query(
        "SELECT from_block, to_block, created_at_unix_ms
         FROM processor_artifact_segment_owners
         WHERE segment_id = ? AND owner_kind = ? AND owner_id = ?
         ORDER BY from_block",
    )
    .bind(segment_id)
    .bind(kind.as_str())
    .bind(owner_id)
    .fetch_all(&mut **transaction)
    .await?;
    let mut ranges = Vec::<(u64, u64)>::with_capacity(rows.len().saturating_add(1));
    let mut created = created_at_unix_ms;
    let mut before = 0_u64;
    for row in rows {
        let start = i64_u64(row.try_get("from_block")?, "artifact segment owner start")?;
        let end = i64_u64(row.try_get("to_block")?, "artifact segment owner end")?;
        before = before.saturating_add(end.saturating_sub(start).saturating_add(1));
        created = created.min(row.try_get("created_at_unix_ms")?);
        ranges.push((start, end));
    }
    ranges.push((from_block, to_block));
    ranges.sort_unstable();
    let mut merged = Vec::<(u64, u64)>::new();
    for (start, end) in ranges {
        if let Some((_, previous_end)) = merged.last_mut()
            && start <= previous_end.saturating_add(1)
        {
            *previous_end = (*previous_end).max(end);
        } else {
            merged.push((start, end));
        }
    }
    sqlx::query(
        "DELETE FROM processor_artifact_segment_owners
         WHERE segment_id = ? AND owner_kind = ? AND owner_id = ?",
    )
    .bind(segment_id)
    .bind(kind.as_str())
    .bind(owner_id)
    .execute(&mut **transaction)
    .await?;
    let mut after = 0_u64;
    for (start, end) in merged {
        after = after.saturating_add(end.saturating_sub(start).saturating_add(1));
        insert_segment_owner_range(
            transaction,
            segment_id,
            kind.as_str(),
            owner_id,
            start,
            end,
            created,
        )
        .await?;
    }
    Ok(after.saturating_sub(before))
}

async fn release_segment_owner_ranges(
    transaction: &mut Transaction<'_, Sqlite>,
    instance: &str,
    range: BlockRange,
    kind: ArtifactOwnerKind,
    owner_id: &str,
) -> Result<u64, StoreError> {
    let rows = sqlx::query(
        "SELECT owners.segment_id, owners.from_block, owners.to_block,
                owners.created_at_unix_ms
         FROM processor_artifact_segment_owners AS owners
         JOIN processor_artifact_segments AS segments
           ON segments.segment_id = owners.segment_id
         WHERE segments.instance = ? AND segments.state = 'active'
           AND owners.owner_kind = ? AND owners.owner_id = ?
           AND owners.to_block >= ? AND owners.from_block <= ?",
    )
    .bind(instance)
    .bind(kind.as_str())
    .bind(owner_id)
    .bind(u64_i64(range.start().0, "artifact owner release start")?)
    .bind(u64_i64(range.end().0, "artifact owner release end")?)
    .fetch_all(&mut **transaction)
    .await?;
    let mut released = 0_u64;
    for row in rows {
        let segment_id: String = row.try_get("segment_id")?;
        let start = i64_u64(row.try_get("from_block")?, "artifact segment owner start")?;
        let end = i64_u64(row.try_get("to_block")?, "artifact segment owner end")?;
        let created: i64 = row.try_get("created_at_unix_ms")?;
        sqlx::query(
            "DELETE FROM processor_artifact_segment_owners
             WHERE segment_id = ? AND owner_kind = ? AND owner_id = ?
               AND from_block = ? AND to_block = ?",
        )
        .bind(&segment_id)
        .bind(kind.as_str())
        .bind(owner_id)
        .bind(u64_i64(start, "artifact segment owner start")?)
        .bind(u64_i64(end, "artifact segment owner end")?)
        .execute(&mut **transaction)
        .await?;
        let released_start = start.max(range.start().0);
        let released_end = end.min(range.end().0);
        released = released.saturating_add(
            released_end
                .saturating_sub(released_start)
                .saturating_add(1),
        );
        if start < released_start {
            insert_segment_owner_range(
                transaction,
                &segment_id,
                kind.as_str(),
                owner_id,
                start,
                released_start.saturating_sub(1),
                created,
            )
            .await?;
        }
        if released_end < end {
            insert_segment_owner_range(
                transaction,
                &segment_id,
                kind.as_str(),
                owner_id,
                released_end.saturating_add(1),
                end,
                created,
            )
            .await?;
        }
    }
    Ok(released)
}

fn validate_artifact_owner_id(value: &str) -> Result<(), StoreError> {
    if value.is_empty()
        || value.len() > 192
        || !value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'@')
        })
    {
        return Err(StoreError::InvalidConfig(
            "artifact owner IDs must be 1-192 portable characters".to_owned(),
        ));
    }
    Ok(())
}

fn u64_i64(value: u64, field: &'static str) -> Result<i64, StoreError> {
    i64::try_from(value).map_err(|_| StoreError::Numeric(field))
}

fn usize_i64(value: usize, field: &'static str) -> Result<i64, StoreError> {
    i64::try_from(value).map_err(|_| StoreError::Numeric(field))
}

fn i64_u64(value: i64, field: &'static str) -> Result<u64, StoreError> {
    u64::try_from(value).map_err(|_| StoreError::Numeric(field))
}

fn now_i64() -> Result<i64, StoreError> {
    let milliseconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    i64::try_from(milliseconds).map_err(|_| StoreError::Numeric("wall clock"))
}

fn duration_micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

fn now_milliseconds() -> Result<u64, StoreError> {
    i64_u64(now_i64()?, "wall clock")
}

fn state_error(error: impl std::fmt::Display) -> ProcessorError {
    ProcessorError::State(error.to_string())
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("invalid store configuration: {0}")]
    InvalidConfig(String),
    #[error("SQLite operation failed: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("store I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("processor artifact segment failed: {0}")]
    ArtifactSegment(String),
    #[error("processor failed: {0}")]
    Processor(#[from] ProcessorError),
    #[error("cursor failed validation: {0}")]
    Cursor(#[from] leani_primitives::CursorError),
    #[error("JSON encoding failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("durable encoding failed: {0}")]
    Encoding(String),
    #[error("numeric value for {0} exceeds SQLite's signed range")]
    Numeric(&'static str),
    #[error("processor instance {0} conflicts with its stored descriptor")]
    ProcessorIdentity(String),
    #[error("durable consumer {consumer_id} already exists for processor instance {instance}")]
    ConsumerExists {
        instance: String,
        consumer_id: String,
    },
    #[error("durable consumer {consumer_id} does not exist for processor instance {instance}")]
    ConsumerNotFound {
        instance: String,
        consumer_id: String,
    },
    #[error("durable consumer {consumer_id} is {state:?}")]
    ConsumerInactive {
        consumer_id: String,
        state: ConsumerState,
    },
    #[error(
        "durable consumer {consumer_id} already has an active streaming session until {expires_at_unix_ms}"
    )]
    ConsumerSessionActive {
        consumer_id: String,
        expires_at_unix_ms: u64,
    },
    #[error(
        "durable consumer {consumer_id} streaming session generation {generation} is no longer current"
    )]
    ConsumerSessionLost {
        consumer_id: String,
        generation: u64,
    },
    #[error(
        "durable consumer cursor requires reset; retained bounds are {earliest_available:?}..={latest_available:?}"
    )]
    ConsumerResetRequired {
        earliest_available: Option<u64>,
        latest_available: Option<u64>,
    },
    #[error("acknowledgement {sequence} is behind current acknowledgement {acknowledged}")]
    AcknowledgementBackwards { sequence: u64, acknowledged: u64 },
    #[error("acknowledgement {sequence} exceeds delivered sequence {delivered}")]
    AcknowledgementBeyondDelivered { sequence: u64, delivered: u64 },
    #[error("acknowledgement {sequence} is not a committed progress boundary")]
    AcknowledgementNotBoundary { sequence: u64 },
    #[error("cursor {sequence} exceeds current delivery head {head}")]
    AcknowledgementBeyondHead { sequence: u64, head: u64 },
    #[error("historical job {job_id} in state {state} is not safe to delete")]
    HistoricalWorkNotDeletable { job_id: String, state: String },
    #[error(
        "processor instance {instance} delivery {scope} limit action {action:?}: projected {projected_bytes} bytes exceeds {limit_bytes}"
    )]
    DeliveryLimit {
        instance: String,
        action: DeliveryLimitAction,
        scope: &'static str,
        limit_bytes: u64,
        projected_bytes: u64,
    },
    #[error(
        "physical store limit: projected SQLite, WAL, and artifact-segment footprint {projected_bytes} bytes exceeds {limit_bytes}"
    )]
    PhysicalStorageLimit {
        limit_bytes: u64,
        projected_bytes: u64,
    },
    #[error(
        "SQLite-only backup would omit {segments} processor artifact segments; prune them or use a segment-aware data-directory backup"
    )]
    ArtifactSegmentBackupUnsupported { segments: u64 },
    #[error(
        "processor artifact {scope} limit: projected {projected_bytes} bytes exceeds {limit_bytes}"
    )]
    ArtifactStorageLimit {
        scope: &'static str,
        limit_bytes: u64,
        projected_bytes: u64,
    },
    #[error(
        "processor instance {instance} indivisible delivery item uses {observed_bytes} bytes/{observed_work_blocks} work blocks; limits are {maximum_bytes} bytes/{maximum_work_blocks:?} work blocks"
    )]
    DeliveryItemTooLarge {
        instance: String,
        observed_bytes: u64,
        maximum_bytes: u64,
        observed_work_blocks: u64,
        maximum_work_blocks: Option<u64>,
    },
    #[error(
        "processor instance {instance} is paused at {current_bytes} delivery bytes; resumes below {resume_below_bytes}"
    )]
    ProcessorPaused {
        instance: String,
        current_bytes: u64,
        resume_below_bytes: u64,
    },
    #[error("processor instance {0} has failed and requires operator recovery")]
    ProcessorFailed(String),
    #[error("processor instance {0} has no committed cursor to checkpoint")]
    NoProcessorCursor(String),
    #[error("recovery checkpoint {checkpoint_id} does not exist for processor instance {instance}")]
    RecoveryCheckpointNotFound {
        instance: String,
        checkpoint_id: u64,
    },
    #[error(
        "recovery checkpoint at block {checkpoint} cannot restore current cursor at {current:?}; only an exact-boundary state repair is safe"
    )]
    CheckpointRestoreBoundary {
        checkpoint: BlockNumber,
        current: Option<BlockNumber>,
    },
    #[error("portable savepoint {savepoint_id} already exists for processor instance {instance}")]
    SavepointExists {
        instance: String,
        savepoint_id: String,
    },
    #[error("portable savepoint {savepoint_id} does not exist for processor instance {instance}")]
    SavepointNotFound {
        instance: String,
        savepoint_id: String,
    },
    #[error("portable savepoint checksum mismatch")]
    SavepointChecksum,
    #[error("portable savepoint belongs to another processor contract")]
    SavepointContract,
    #[error(
        "query snapshot requires {rows} rows/{bytes} bytes; limits are {max_rows} rows/{max_bytes} bytes"
    )]
    QueryTooExpensive {
        rows: u64,
        bytes: u64,
        max_rows: u64,
        max_bytes: u64,
    },
    #[error("query snapshot is absent or expired; create a new snapshot")]
    QuerySnapshotExpired,
    #[error("query snapshot belongs to another processor instance or collection")]
    QuerySnapshotMismatch,
    #[error("block {block} was already applied with a different delta")]
    ConflictingApply { block: BlockNumber },
    #[error("historical microbatch overlaps previously committed processor coverage")]
    HistoricalBatchRequiresFallback,
    #[error(
        "historical microbatch uses {observed_changes} changes/{observed_encoded_bytes} encoded bytes; limits are {maximum_changes} changes/{maximum_encoded_bytes} bytes"
    )]
    HistoricalBatchLimit {
        observed_changes: usize,
        maximum_changes: usize,
        observed_encoded_bytes: u64,
        maximum_encoded_bytes: u64,
    },
    #[error(
        "canonical overlap conflict at block {block}: stored {stored:?}, incoming {incoming:?}"
    )]
    CanonicalConflict {
        block: BlockNumber,
        stored: BlockHash,
        incoming: BlockHash,
    },
    #[error("block {0} already has a different pending delta")]
    ConflictingPendingDelta(BlockNumber),
    #[error("block {0} already has a different finalized processor artifact")]
    ConflictingArtifact(BlockNumber),
    #[error("processor artifact belongs to an incompatible map/delta contract")]
    ArtifactContract,
    #[error("processor artifact export metadata, ordering, or digest is invalid")]
    ArtifactExport,
    #[error("processor artifacts must replay into a distinct processor instance")]
    ArtifactReplaySameInstance,
    #[error("ordered artifact replay expected block {expected}, received {received}")]
    ArtifactReplayGap {
        expected: BlockNumber,
        received: BlockNumber,
    },
    #[error("hot/cold handoff {id} failed: {detail}")]
    HandoffMismatch { id: String, detail: String },
    #[error("archive reconciliation {id} failed: {detail}")]
    ArchiveReconciliationMismatch { id: String, detail: String },
    #[error("no undo record exists for block {0}")]
    UndoNotFound(BlockNumber),
    #[error("block {0} is finalized and cannot be undone")]
    FinalizedUndo(BlockNumber),
    #[error("store invariant failed: {0}")]
    Invariant(String),
    #[error("SQLite integrity check failed: {0}")]
    Integrity(String),
    #[error("backup destination already exists: {0}")]
    DestinationExists(PathBuf),
}

#[cfg(test)]
mod tests {
    use leani_primitives::{
        BlockFrame, BlockRef, Capability, CapabilitySet, FilterScope, Provenance, SourceId,
        SourceKind, TransactionEnvelope, TrustModel, VerificationCheck, VerificationReport,
    };
    use leani_processor_api::{
        ArtifactPolicyMode, DataRequirement, DomainChanges, LifecyclePolicies, OutputWindow,
        ProcessorId, ProcessorInstanceId, ProcessorSchemas, PublicationPolicy, ReductionMode,
        RetentionPolicy, StartPoint,
    };
    use semver::Version;

    use super::*;

    #[test]
    fn delivery_only_created_entity_can_emit_a_richer_payload() {
        let key = 42_u64.to_be_bytes().to_vec();
        let batch = MutationBatch {
            mutations: vec![Mutation::Entity {
                collection: "blocks".to_owned(),
                key: key.clone(),
                before: None,
                after: Some(vec![1]),
            }],
            changes: vec![DomainChange {
                kind: "block.bundle".to_owned(),
                key: key.clone(),
                operation: ChangeOperation::Upsert,
                payload: vec![1, 2, 3],
            }],
        };

        assert_eq!(
            build_inverse_changes(&batch).expect("created entity has an exact delete inverse"),
            vec![DomainChange {
                kind: "block.bundle".to_owned(),
                key,
                operation: ChangeOperation::Delete,
                payload: Vec::new(),
            }]
        );
    }

    #[derive(Debug)]
    struct FixtureProcessor {
        descriptor: ProcessorDescriptor,
        output_key_by_block: bool,
    }

    impl FixtureProcessor {
        fn new() -> Self {
            let id = ProcessorId::new("fixture").expect("id");
            let version = Version::new(1, 0, 0);
            let config_hash = BlockHash::new([2; 32]);
            Self {
                descriptor: ProcessorDescriptor {
                    instance: ProcessorInstanceId::legacy(&id, &version, config_hash),
                    id,
                    version,
                    code_hash: BlockHash::new([1; 32]),
                    config_hash,
                    start: StartPoint::Genesis,
                    requirements: vec![DataRequirement {
                        capabilities: CapabilitySet::of(Capability::Header),
                        log_fields: leani_primitives::LogFieldSet::NONE,
                        allow_filtered: false,
                        filter: FilterScope::default(),
                        minimum_finality: Finality::Optimistic,
                    }],
                    mode: ReductionMode::OrderedState,
                    delivery_ordering: DeliveryOrdering::Canonical,
                    publication: PublicationPolicy::OptimisticAndFinalized,
                    lifecycle: LifecyclePolicies::from_legacy(RetentionPolicy::LatestState),
                    schemas: ProcessorSchemas {
                        delta_version: 1,
                        entity_schema: "fixture.entity.v1".to_owned(),
                        change_schema: "fixture.change.v1".to_owned(),
                    },
                },
                output_key_by_block: false,
            }
        }

        fn block_output() -> Self {
            let mut processor = Self::new();
            processor.descriptor.mode = ReductionMode::BlockLocal;
            processor.descriptor.delivery_ordering = DeliveryOrdering::BlockVersionedIdempotent;
            processor.output_key_by_block = true;
            processor
        }

        fn full_artifacts() -> Self {
            let mut processor = Self::new();
            processor.descriptor.lifecycle.artifacts.mode = ArtifactPolicyMode::Full;
            processor
        }
    }

    #[async_trait]
    impl Processor for FixtureProcessor {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn descriptor(&self) -> &ProcessorDescriptor {
            &self.descriptor
        }

        async fn map(&self, block: &BlockFrame) -> Result<EncodedDelta, ProcessorError> {
            Ok(EncodedDelta::new(
                &self.descriptor,
                block.chain_id,
                block.block,
                block.block.number.0.to_be_bytes().to_vec(),
            ))
        }

        async fn reduce(
            &self,
            transaction: &mut dyn ReducerTransaction,
            cursor: &ProcessorCursor,
            delta: &EncodedDelta,
        ) -> Result<DomainChanges, ProcessorError> {
            delta.validate(&self.descriptor)?;
            let state_key = b"counter".to_vec();
            let before = transaction.state_get("state", &state_key).await?;
            let mut count = before
                .as_deref()
                .map(|bytes| {
                    let value: [u8; 8] = bytes
                        .try_into()
                        .map_err(|_| ProcessorError::State("bad counter".to_owned()))?;
                    Ok::<_, ProcessorError>(u64::from_be_bytes(value))
                })
                .transpose()?
                .unwrap_or_default();
            count = count.saturating_add(1);
            let payload = count.to_be_bytes().to_vec();
            transaction
                .state_put("state", state_key, payload.clone())
                .await?;
            let key = if self.output_key_by_block {
                cursor.block_number.0.to_be_bytes().to_vec()
            } else {
                b"counter".to_vec()
            };
            transaction
                .put("state", key.clone(), payload.clone())
                .await?;
            transaction
                .index_put(
                    "by_block",
                    cursor.block_number.0.to_be_bytes().to_vec(),
                    key.clone(),
                )
                .await?;
            let change = DomainChange {
                kind: "fixture.counter".to_owned(),
                key,
                operation: ChangeOperation::Upsert,
                payload,
            };
            transaction.emit(change.clone()).await?;
            Ok(DomainChanges {
                changes: vec![change],
            })
        }
    }

    fn frame(number: u64, parent: BlockHash) -> BlockFrame {
        BlockFrame {
            chain_id: ChainId(1),
            block: BlockRef {
                number: BlockNumber(number),
                hash: BlockHash::new([u8::try_from(number).unwrap_or(0); 32]),
                parent_hash: parent,
                timestamp: number,
            },
            finality: Finality::Optimistic,
            header: leani_primitives::Material::Complete(leani_primitives::HeaderEnvelope {
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
            }),
            transactions: leani_primitives::Material::Missing(
                leani_primitives::MissingReason::NotRequested,
            ),
            receipts: leani_primitives::Material::Missing(
                leani_primitives::MissingReason::NotRequested,
            ),
            logs: leani_primitives::Material::Missing(
                leani_primitives::MissingReason::NotRequested,
            ),
            withdrawals: leani_primitives::Material::Missing(
                leani_primitives::MissingReason::NotRequested,
            ),
            blob_sidecars: leani_primitives::Material::Missing(
                leani_primitives::MissingReason::NotRequested,
            ),
            traces: leani_primitives::Material::Missing(
                leani_primitives::MissingReason::NotRequested,
            ),
            state_diffs: leani_primitives::Material::Missing(
                leani_primitives::MissingReason::NotRequested,
            ),
            provenance: Vec::new(),
            verification: VerificationReport::default(),
        }
    }

    fn with_transaction(mut frame: BlockFrame, hash: TransactionHash) -> BlockFrame {
        frame.transactions = Material::Complete(vec![TransactionEnvelope {
            hash,
            transaction_type: 2,
            index: 0,
            encoded: None,
            from: None,
            to: None,
            nonce: None,
            gas_limit: None,
            value: None,
            input: None,
            max_fee_per_gas: None,
            max_priority_fee_per_gas: None,
            max_fee_per_blob_gas: None,
            blob_versioned_hashes: Vec::new(),
            size_bytes: None,
        }]);
        frame
    }

    fn with_observation(mut frame: BlockFrame, observed_at_unix_ms: u64) -> BlockFrame {
        frame.provenance = vec![Provenance {
            source_id: SourceId::new("fixture-p2p").expect("source ID"),
            source_kind: SourceKind::ExecutionP2p,
            trust: TrustModel::ProtocolVerified,
            range: Some(BlockRange::single(frame.block.number)),
            object: None,
            observed_at_unix_ms,
            projection: vec![
                "header".to_owned(),
                "body".to_owned(),
                "receipts".to_owned(),
            ],
        }];
        frame
    }

    fn cursor(processor: &FixtureProcessor, frame: &BlockFrame, sequence: u64) -> ProcessorCursor {
        ProcessorCursor {
            processor_id: processor.descriptor.id.to_string(),
            processor_version: processor.descriptor.version.to_string(),
            chain_id: frame.chain_id,
            block_number: frame.block.number,
            block_hash: frame.block.hash,
            finality: frame.finality,
            sequence,
        }
    }

    async fn store() -> (tempfile::TempDir, SqliteStore) {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteStore::open(StoreConfig::new(directory.path().join("node.sqlite")))
            .await
            .expect("open");
        (directory, store)
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn finalized_artifacts_are_durable_scannable_and_independently_owned() {
        let (directory, store) = store().await;
        let processor = FixtureProcessor::full_artifacts();
        let first_frame = frame(1, BlockHash::ZERO);
        let second_frame = frame(2, first_frame.block.hash);
        let first = processor.map(&first_frame).await.expect("map first");
        let second = processor.map(&second_frame).await.expect("map second");

        assert!(matches!(
            store
                .retain_finalized_artifact(&processor.descriptor, &first, Finality::Optimistic)
                .await,
            Err(StoreError::InvalidConfig(_))
        ));
        for delta in [&first, &second] {
            store
                .retain_finalized_artifact(&processor.descriptor, delta, Finality::Finalized)
                .await
                .expect("retain artifact");
        }
        store
            .retain_finalized_artifact(&processor.descriptor, &first, Finality::Finalized)
            .await
            .expect("duplicate artifact is idempotent");

        let range = BlockRange::new(BlockNumber(1), BlockNumber(2)).expect("range");
        let scanned = store
            .scan_processor_artifacts(&processor.descriptor, range, 10)
            .await
            .expect("scan artifacts");
        assert_eq!(
            scanned
                .iter()
                .map(|artifact| artifact.delta.clone())
                .collect::<Vec<_>>(),
            vec![first.clone(), second.clone()]
        );
        let stats = store
            .processor_artifact_stats(&processor.descriptor)
            .await
            .expect("artifact stats");
        assert_eq!(stats.artifacts, 2);
        assert_eq!(stats.owners, 2);
        assert!(stats.logical_bytes > 0);

        assert_eq!(
            store
                .add_processor_artifact_owner(
                    &processor.descriptor,
                    BlockRange::single(BlockNumber(1)),
                    ArtifactOwnerKind::OperatorPin,
                    "debug-pin",
                )
                .await
                .expect("pin first artifact"),
            1
        );
        let processor_owner = processor.descriptor.instance.to_string();
        let released = store
            .release_processor_artifacts(
                &processor.descriptor,
                range,
                ArtifactOwnerKind::ProcessorInstance,
                &processor_owner,
            )
            .await
            .expect("release processor owner");
        assert_eq!(released.released_owners, 2);
        assert_eq!(released.deleted_artifacts, 1);
        assert!(
            store
                .processor_artifact(&processor.descriptor, BlockNumber(1))
                .await
                .expect("read pinned artifact")
                .is_some()
        );
        assert!(
            store
                .processor_artifact(&processor.descriptor, BlockNumber(2))
                .await
                .expect("read released artifact")
                .is_none()
        );

        drop(store);
        let reopened = SqliteStore::open(StoreConfig::new(directory.path().join("node.sqlite")))
            .await
            .expect("reopen");
        assert_eq!(
            reopened
                .processor_artifact(&processor.descriptor, BlockNumber(1))
                .await
                .expect("read artifact after restart")
                .expect("pinned artifact")
                .delta,
            first
        );
        let unpinned = reopened
            .release_processor_artifacts(
                &processor.descriptor,
                BlockRange::single(BlockNumber(1)),
                ArtifactOwnerKind::OperatorPin,
                "debug-pin",
            )
            .await
            .expect("release pin");
        assert_eq!(unpinned.deleted_artifacts, 1);
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn tiered_artifacts_compact_query_export_and_reopen_without_inline_payloads() {
        let directory = tempfile::tempdir().expect("tempdir");
        let database = directory.path().join("node.sqlite");
        let segments = directory.path().join("processor-artifacts");
        let segment_config = ArtifactSegmentStorageConfig {
            root: segments,
            compression: ArtifactCompression::Snappy,
            target_blocks: 4,
            maximum_artifact_logical_bytes: 1024 * 1024,
            maximum_segment_logical_bytes: 16 * 1024 * 1024,
            maximum_segment_physical_bytes: 16 * 1024 * 1024,
            maximum_retained_physical_bytes: 32 * 1024 * 1024,
        };
        let store = SqliteStore::open(
            StoreConfig::new(&database).with_artifact_segments(segment_config.clone()),
        )
        .await
        .expect("open tiered store");
        let processor = FixtureProcessor::full_artifacts();
        let mut parent = BlockHash::ZERO;
        let mut expected = Vec::new();
        for number in 1_u64..=8 {
            let current = frame(number, parent);
            let delta = processor.map(&current).await.expect("map artifact");
            store
                .retain_finalized_artifact(&processor.descriptor, &delta, Finality::Finalized)
                .await
                .expect("retain inline artifact");
            parent = current.block.hash;
            expected.push(delta);
        }
        let range = BlockRange::new(BlockNumber(1), BlockNumber(8)).expect("range");

        // Simulate the crash window where the first segment reached disk but
        // SQLite still owns the inline payloads. Compaction must adopt it.
        store
            .inner
            .artifact_segments
            .as_ref()
            .expect("segment storage")
            .sink
            .retain_finalized_batch(&processor.descriptor, &expected[..4])
            .await
            .expect("publish crash-window segment");
        drop(store);
        let store = SqliteStore::open(
            StoreConfig::new(&database).with_artifact_segments(segment_config.clone()),
        )
        .await
        .expect("reopen after close-before-catalog crash window");
        let compacted = store
            .compact_available_processor_artifacts_to_segments(&processor.descriptor, 10, true)
            .await
            .expect("compact artifacts");
        assert_eq!(compacted.segments, 2);
        assert_eq!(compacted.artifacts, 8);
        assert!(compacted.inline_payload_bytes_reclaimed > 0);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COALESCE(SUM(length(encoded_delta)), 0) FROM processor_artifacts",
            )
            .fetch_one(&store.inner.pool)
            .await
            .expect("inline payload bytes"),
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM processor_artifacts")
                .fetch_one(&store.inner.pool)
                .await
                .expect("per-block artifact rows"),
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM processor_artifact_segment_owners",)
                .fetch_one(&store.inner.pool)
                .await
                .expect("segment owner ranges"),
            2
        );
        let compacted_stats = store
            .processor_artifact_stats(&processor.descriptor)
            .await
            .expect("compacted artifact stats");
        assert_eq!(compacted_stats.artifacts, 8);
        assert_eq!(compacted_stats.owners, 8);
        let storage_stats = store.storage_stats().await.expect("tiered storage stats");
        assert!(storage_stats.artifact_segment_bytes > 0);
        assert_eq!(
            storage_stats.total_physical_bytes,
            storage_stats
                .physical_file_bytes
                .saturating_add(storage_stats.wal_bytes)
                .saturating_add(storage_stats.artifact_segment_bytes)
        );
        store.verify().await.expect("verify tiered store");
        assert!(matches!(
            store.backup(&directory.path().join("unsafe.sqlite")).await,
            Err(StoreError::ArtifactSegmentBackupUnsupported { segments: 2 })
        ));
        assert_eq!(
            store
                .processor_artifact_segment_stats()
                .await
                .expect("segment stats")
                .segments,
            2
        );
        let scanned = store
            .scan_processor_artifacts(&processor.descriptor, range, 10)
            .await
            .expect("scan tiered artifacts");
        assert_eq!(
            scanned
                .iter()
                .map(|artifact| artifact.delta.clone())
                .collect::<Vec<_>>(),
            expected
        );
        let export = store
            .export_processor_artifacts(&processor.descriptor, range, 10)
            .await
            .expect("export tiered artifacts");
        assert!(export.complete);
        assert_eq!(export.artifacts, expected);

        drop(store);
        let reopened =
            SqliteStore::open(StoreConfig::new(&database).with_artifact_segments(segment_config))
                .await
                .expect("reopen tiered store");
        assert_eq!(
            reopened
                .processor_artifact(&processor.descriptor, BlockNumber(5))
                .await
                .expect("read restarted tiered artifact")
                .expect("artifact")
                .delta,
            expected[4]
        );
        assert_eq!(
            reopened
                .processor_artifact_stats(&processor.descriptor)
                .await
                .expect("tiered logical stats")
                .logical_bytes,
            compacted.logical_bytes
        );
        assert_eq!(
            reopened
                .add_processor_artifact_owner(
                    &processor.descriptor,
                    BlockRange::new(BlockNumber(2), BlockNumber(3)).expect("pin range"),
                    ArtifactOwnerKind::OperatorPin,
                    "segment-pin",
                )
                .await
                .expect("pin segment subset"),
            2
        );
        let released = reopened
            .release_processor_artifacts(
                &processor.descriptor,
                range,
                ArtifactOwnerKind::ProcessorInstance,
                processor.descriptor.instance.as_str(),
            )
            .await
            .expect("release tiered artifacts");
        assert_eq!(released.released_owners, 8);
        assert_eq!(released.deleted_artifacts, 4);
        assert!(
            reopened
                .processor_artifact(&processor.descriptor, BlockNumber(1))
                .await
                .expect("read artifact in pinned physical segment")
                .is_some()
        );
        assert!(
            reopened
                .processor_artifact_owners(&processor.descriptor, BlockNumber(1))
                .await
                .expect("unowned adjacent artifact")
                .is_empty()
        );
        let pin_released = reopened
            .release_processor_artifacts(
                &processor.descriptor,
                BlockRange::new(BlockNumber(2), BlockNumber(3)).expect("pin range"),
                ArtifactOwnerKind::OperatorPin,
                "segment-pin",
            )
            .await
            .expect("release segment pin");
        assert_eq!(pin_released.released_owners, 2);
        assert_eq!(pin_released.deleted_artifacts, 4);
        assert_eq!(
            reopened
                .processor_artifact_segment_stats()
                .await
                .expect("segment stats after prune")
                .segments,
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM processor_artifact_segments")
                .fetch_one(&reopened.inner.pool)
                .await
                .expect("segment catalog count"),
            0
        );
    }

    #[tokio::test]
    async fn tiered_compaction_disk_pressure_preserves_inline_authority_and_retries() {
        let directory = tempfile::tempdir().expect("tempdir");
        let database = directory.path().join("node.sqlite");
        let segment_config = ArtifactSegmentStorageConfig {
            root: directory.path().join("processor-artifacts"),
            compression: ArtifactCompression::Snappy,
            target_blocks: 4,
            maximum_artifact_logical_bytes: 1024 * 1024,
            maximum_segment_logical_bytes: 16 * 1024 * 1024,
            maximum_segment_physical_bytes: 16 * 1024 * 1024,
            maximum_retained_physical_bytes: 32 * 1024 * 1024,
        };
        let store = SqliteStore::open(
            StoreConfig::new(&database).with_artifact_segments(segment_config.clone()),
        )
        .await
        .expect("open initial tiered store");
        let processor = FixtureProcessor::full_artifacts();
        let mut parent = BlockHash::ZERO;
        let mut expected = Vec::new();
        for number in 1_u64..=4 {
            let current = frame(number, parent);
            let delta = processor.map(&current).await.expect("map artifact");
            store
                .retain_finalized_artifact(&processor.descriptor, &delta, Finality::Finalized)
                .await
                .expect("retain inline artifact");
            parent = current.block.hash;
            expected.push(delta);
        }
        store.compact().await.expect("stabilize physical footprint");
        let tight_limit = store
            .storage_stats()
            .await
            .expect("initial storage stats")
            .total_physical_bytes
            .saturating_add(1);
        drop(store);

        let pressured = SqliteStore::open(
            StoreConfig::new(&database)
                .with_storage_budget(StoreStorageBudget {
                    maximum_physical_bytes: tight_limit,
                })
                .with_artifact_segments(segment_config.clone()),
        )
        .await
        .expect("reopen with tight physical budget");
        assert!(matches!(
            pressured
                .compact_available_processor_artifacts_to_segments(
                    &processor.descriptor,
                    1,
                    false,
                )
                .await,
            Err(StoreError::PhysicalStorageLimit {
                limit_bytes,
                projected_bytes,
            }) if limit_bytes == tight_limit && projected_bytes > limit_bytes
        ));
        assert_eq!(
            pressured
                .processor_artifact_segment_stats()
                .await
                .expect("segment stats after pressure")
                .segments,
            0
        );
        assert_eq!(
            pressured
                .scan_processor_artifacts(
                    &processor.descriptor,
                    BlockRange::new(BlockNumber(1), BlockNumber(4)).expect("range"),
                    10,
                )
                .await
                .expect("inline artifacts remain queryable")
                .into_iter()
                .map(|artifact| artifact.delta)
                .collect::<Vec<_>>(),
            expected
        );
        pressured
            .verify()
            .await
            .expect("pressure rollback keeps exact accounting");
        drop(pressured);

        let retried =
            SqliteStore::open(StoreConfig::new(&database).with_artifact_segments(segment_config))
                .await
                .expect("reopen after capacity is raised");
        let compacted = retried
            .compact_available_processor_artifacts_to_segments(&processor.descriptor, 1, false)
            .await
            .expect("retry compaction");
        assert_eq!(compacted.artifacts, 4);
        assert_eq!(compacted.segments, 1);
        retried.verify().await.expect("verify successful retry");
    }

    #[tokio::test]
    async fn artifact_only_apply_promotes_on_finality_without_output_or_delivery() {
        let (_directory, store) = store().await;
        let mut processor = FixtureProcessor::full_artifacts();
        processor.descriptor.lifecycle.output.mode = OutputPolicyMode::None;
        processor.descriptor.lifecycle.delivery.mode = DeliveryPolicyMode::None;
        processor.descriptor.lifecycle.delivery.consumers.clear();
        let frame = frame(1, BlockHash::ZERO);
        let delta = processor.map(&frame).await.expect("map");

        store
            .apply(&processor, cursor(&processor, &frame, 1), &delta, &[])
            .await
            .expect("apply optimistic artifact candidate");
        let staged = store
            .processor_stats(&processor.descriptor)
            .await
            .expect("staged stats");
        assert_eq!(staged.pending_processor_artifacts, 1);
        assert_eq!(staged.processor_artifacts, 0);
        assert_eq!(staged.entities, 0);
        assert_eq!(staged.changes, 0);

        store
            .mark_finalized(&processor.descriptor, frame.block.number)
            .await
            .expect("finalize candidate");
        let finalized = store
            .processor_stats(&processor.descriptor)
            .await
            .expect("finalized stats");
        assert_eq!(finalized.pending_processor_artifacts, 0);
        assert_eq!(finalized.processor_artifacts, 1);
        assert_eq!(finalized.entities, 0);
        assert_eq!(finalized.changes, 0);
        assert_eq!(
            store
                .processor_artifact(&processor.descriptor, frame.block.number)
                .await
                .expect("read artifact")
                .expect("artifact")
                .delta,
            delta
        );
    }

    #[tokio::test]
    async fn artifact_budgets_roll_back_retained_and_pending_admission() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteStore::open(
            StoreConfig::new(directory.path().join("node.sqlite")).with_artifact_budget(
                ArtifactStorageBudget {
                    maximum_retained_bytes: 1,
                    maximum_pending_bytes: 1,
                },
            ),
        )
        .await
        .expect("open artifact-bounded store");
        let mut processor = FixtureProcessor::full_artifacts();
        processor.descriptor.lifecycle.output.mode = OutputPolicyMode::None;
        processor.descriptor.lifecycle.delivery.mode = DeliveryPolicyMode::None;
        processor.descriptor.lifecycle.delivery.consumers.clear();
        let optimistic = frame(1, BlockHash::ZERO);
        let delta = processor.map(&optimistic).await.expect("map optimistic");
        assert!(matches!(
            store
                .apply(&processor, cursor(&processor, &optimistic, 1), &delta, &[],)
                .await,
            Err(StoreError::ArtifactStorageLimit {
                scope: "pending",
                limit_bytes: 1,
                ..
            })
        ));
        let stats = store
            .processor_stats(&processor.descriptor)
            .await
            .expect("stats after pending rejection");
        assert_eq!(stats.pending_processor_artifacts, 0);
        assert_eq!(stats.applied_blocks, 0);

        assert!(matches!(
            store
                .retain_finalized_artifact(&processor.descriptor, &delta, Finality::Finalized)
                .await,
            Err(StoreError::ArtifactStorageLimit {
                scope: "retained",
                limit_bytes: 1,
                ..
            })
        ));
        assert_eq!(
            store
                .processor_artifact_stats(&processor.descriptor)
                .await
                .expect("stats after retained rejection")
                .artifacts,
            0
        );
    }

    #[tokio::test]
    async fn artifact_finality_promotion_rolls_back_when_retained_budget_is_full() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteStore::open(
            StoreConfig::new(directory.path().join("node.sqlite")).with_artifact_budget(
                ArtifactStorageBudget {
                    maximum_retained_bytes: 1,
                    maximum_pending_bytes: 1_024 * 1_024,
                },
            ),
        )
        .await
        .expect("open artifact-bounded store");
        let mut processor = FixtureProcessor::full_artifacts();
        processor.descriptor.lifecycle.output.mode = OutputPolicyMode::None;
        processor.descriptor.lifecycle.delivery.mode = DeliveryPolicyMode::None;
        processor.descriptor.lifecycle.delivery.consumers.clear();
        let optimistic = frame(1, BlockHash::ZERO);
        let delta = processor.map(&optimistic).await.expect("map optimistic");
        store
            .apply(&processor, cursor(&processor, &optimistic, 1), &delta, &[])
            .await
            .expect("stage candidate");
        assert!(matches!(
            store
                .mark_finalized(&processor.descriptor, optimistic.block.number)
                .await,
            Err(StoreError::ArtifactStorageLimit {
                scope: "retained",
                limit_bytes: 1,
                ..
            })
        ));
        let stats = store
            .processor_stats(&processor.descriptor)
            .await
            .expect("stats after promotion rejection");
        assert_eq!(stats.pending_processor_artifacts, 1);
        assert_eq!(stats.processor_artifacts, 0);
        assert_eq!(stats.undo_records, 1);
    }

    #[tokio::test]
    async fn undo_discards_unfinalized_artifact_candidate() {
        let (_directory, store) = store().await;
        let mut processor = FixtureProcessor::full_artifacts();
        processor.descriptor.lifecycle.delivery.mode = DeliveryPolicyMode::None;
        processor.descriptor.lifecycle.delivery.consumers.clear();
        let frame = frame(1, BlockHash::ZERO);
        let delta = processor.map(&frame).await.expect("map");
        store
            .apply(&processor, cursor(&processor, &frame, 1), &delta, &[])
            .await
            .expect("apply optimistic artifact candidate");

        store
            .undo(
                &processor.descriptor,
                frame.chain_id,
                frame.block.number,
                frame.block.hash,
                &[],
            )
            .await
            .expect("undo candidate");
        let stats = store
            .processor_stats(&processor.descriptor)
            .await
            .expect("stats");
        assert_eq!(stats.pending_processor_artifacts, 0);
        assert_eq!(stats.processor_artifacts, 0);
    }

    #[tokio::test]
    async fn artifact_window_releases_only_its_owner_at_the_safe_boundary() {
        let (_directory, store) = store().await;
        let mut processor = FixtureProcessor::full_artifacts();
        processor.descriptor.lifecycle.artifacts.mode = ArtifactPolicyMode::Window;
        processor.descriptor.lifecycle.artifacts.window =
            Some(leani_processor_api::ArtifactWindow {
                max_blocks: Some(2),
                ..leani_processor_api::ArtifactWindow::default()
            });
        let mut parent = BlockHash::ZERO;
        let mut deltas = Vec::new();
        for number in 1_u64..=3 {
            let current = frame(number, parent);
            let delta = processor.map(&current).await.expect("map");
            store
                .retain_finalized_artifact(&processor.descriptor, &delta, Finality::Finalized)
                .await
                .expect("retain window artifact");
            if number == 1 {
                store
                    .add_processor_artifact_owner(
                        &processor.descriptor,
                        BlockRange::single(BlockNumber(1)),
                        ArtifactOwnerKind::OperatorPin,
                        "window-debug-pin",
                    )
                    .await
                    .expect("pin first artifact");
            }
            parent = current.block.hash;
            deltas.push(delta);
        }

        assert!(
            store
                .processor_artifact(&processor.descriptor, BlockNumber(1))
                .await
                .expect("read pinned artifact")
                .is_some()
        );
        let owners = store
            .processor_artifact_owners(&processor.descriptor, BlockNumber(1))
            .await
            .expect("owners");
        assert_eq!(owners.len(), 1);
        assert_eq!(owners[0].kind, ArtifactOwnerKind::OperatorPin);
        assert_eq!(owners[0].id, "window-debug-pin");
        for (number, expected) in [(2_u64, &deltas[1]), (3, &deltas[2])] {
            assert_eq!(
                store
                    .processor_artifact(&processor.descriptor, BlockNumber(number))
                    .await
                    .expect("read window artifact")
                    .expect("retained")
                    .delta,
                *expected
            );
        }
    }

    #[tokio::test]
    async fn artifact_export_is_portable_checksummed_and_replayable() {
        let (_directory, store) = store().await;
        let source = FixtureProcessor::full_artifacts();
        let mut parent = BlockHash::ZERO;
        let mut deltas = Vec::new();
        for number in 1_u64..=3 {
            let current = frame(number, parent);
            let delta = source.map(&current).await.expect("map source");
            store
                .retain_finalized_artifact(&source.descriptor, &delta, Finality::Finalized)
                .await
                .expect("retain source artifact");
            parent = current.block.hash;
            deltas.push(delta);
        }
        let range = BlockRange::new(BlockNumber(1), BlockNumber(3)).expect("range");
        let first_page = store
            .export_processor_artifacts(&source.descriptor, range, 2)
            .await
            .expect("export first page");
        assert_eq!(
            first_page.exported_range,
            Some(BlockRange::new(BlockNumber(1), BlockNumber(2)).expect("page range"))
        );
        assert!(!first_page.complete);

        let export = store
            .export_processor_artifacts(&source.descriptor, range, 10)
            .await
            .expect("export complete range");
        assert!(export.complete);
        assert_eq!(export.artifacts, deltas);
        let encoded = export.encode_durable().expect("encode export");
        assert_eq!(
            ProcessorArtifactExport::decode_durable(&source.descriptor, &encoded)
                .expect("decode export"),
            export
        );
        let mut corrupt = encoded;
        let midpoint = corrupt.len() / 2;
        corrupt[midpoint] ^= 0xff;
        assert!(ProcessorArtifactExport::decode_durable(&source.descriptor, &corrupt).is_err());

        let mut target = FixtureProcessor::new();
        target.descriptor.instance =
            ProcessorInstanceId::new("fixture-artifact-replay").expect("target instance");
        target.descriptor.start = StartPoint::Block(BlockNumber(1));
        target.descriptor.lifecycle.output.mode = OutputPolicyMode::Full;
        target.descriptor.lifecycle.delivery.mode = DeliveryPolicyMode::None;
        target.descriptor.lifecycle.delivery.consumers.clear();
        let replay = store
            .replay_processor_artifacts(&source.descriptor, &target, range, 10, &[])
            .await
            .expect("replay artifacts");
        assert_eq!(replay.processed_artifacts, 3);
        assert_eq!(replay.applied_artifacts, 3);
        assert_eq!(replay.duplicate_artifacts, 0);
        assert_eq!(replay.last_block, Some(BlockNumber(3)));
        assert_eq!(
            store
                .entity(&target.descriptor, "state", b"counter")
                .await
                .expect("target entity"),
            Some(3_u64.to_be_bytes().to_vec())
        );
        let target_stats = store
            .processor_stats(&target.descriptor)
            .await
            .expect("target stats");
        assert_eq!(target_stats.processor_artifacts, 0);
        assert!(target_stats.entities > 0);
        assert_eq!(target_stats.changes, 0);
    }

    #[tokio::test]
    async fn live_writer_waiters_are_admitted_before_queued_history() {
        let (_directory, store) = store().await;
        let active = store.inner.writer.lock().await;
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();

        let history_store = store.clone();
        let history_sender = sender.clone();
        let history = tokio::spawn(async move {
            let _permit = history_store.inner.writer.lock_history().await;
            history_sender.send("history").expect("history result");
        });
        loop {
            let history_waiting = {
                let state = store
                    .inner
                    .writer
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.history_waiters == 1
            };
            if history_waiting {
                break;
            }
            tokio::task::yield_now().await;
        }

        let live_store = store.clone();
        let live = tokio::spawn(async move {
            let _permit = live_store.inner.writer.lock().await;
            sender.send("live").expect("live result");
        });
        loop {
            let live_waiting = {
                let state = store
                    .inner
                    .writer
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.live_waiters == 1
            };
            if live_waiting {
                break;
            }
            tokio::task::yield_now().await;
        }

        drop(active);
        assert_eq!(receiver.recv().await, Some("live"));
        assert_eq!(receiver.recv().await, Some("history"));
        live.await.expect("live waiter");
        history.await.expect("history waiter");
    }

    #[tokio::test]
    async fn history_writer_waiters_are_admitted_in_fifo_order() {
        let (_directory, store) = store().await;
        let active = store.inner.writer.lock().await;
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let mut waiters = Vec::new();

        for waiter_id in 0_u64..3 {
            let waiter_store = store.clone();
            let waiter_sender = sender.clone();
            waiters.push(tokio::spawn(async move {
                let _permit = waiter_store.inner.writer.lock_history().await;
                waiter_sender
                    .send(waiter_id)
                    .expect("history admission result");
            }));
            loop {
                let queued = {
                    let state = store
                        .inner
                        .writer
                        .state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    state.history_queue.len()
                        == usize::try_from(waiter_id + 1).expect("small queue")
                };
                if queued {
                    break;
                }
                tokio::task::yield_now().await;
            }
        }

        drop(active);
        for waiter_id in 0_u64..3 {
            assert_eq!(receiver.recv().await, Some(waiter_id));
        }
        for waiter in waiters {
            waiter.await.expect("history waiter");
        }
    }

    #[tokio::test]
    async fn upgrades_a_v1_change_log_in_place() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("legacy.sqlite");
        let options =
            SqliteConnectOptions::from_str(&format!("sqlite://{}", path.to_string_lossy()))
                .expect("options")
                .create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .expect("legacy pool");
        sqlx::raw_sql(SCHEMA_V1)
            .execute(&pool)
            .await
            .expect("v1 schema");
        pool.close().await;

        let store = SqliteStore::open(StoreConfig::new(&path))
            .await
            .expect("migrate");
        assert_eq!(
            store.stats().await.expect("stats").schema_version,
            CURRENT_SCHEMA_VERSION
        );
        let columns: Vec<String> =
            sqlx::query_scalar("SELECT name FROM pragma_table_info('change_log') ORDER BY cid")
                .fetch_all(&store.inner.pool)
                .await
                .expect("columns");
        assert!(columns.iter().any(|column| column == "parent_hash"));
        assert!(columns.iter().any(|column| column == "block_timestamp"));
        assert!(columns.iter().any(|column| column == "finality"));
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn unacknowledged_changes_survive_a_v7_to_v8_upgrade() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("v6-delivery.sqlite");
        let store = SqliteStore::open(StoreConfig::new(&path))
            .await
            .expect("open current store");
        let processor = FixtureProcessor::new();
        let first = frame(1, BlockHash::ZERO);
        let second = frame(2, first.block.hash);
        for (sequence, current) in [(1, &first), (2, &second)] {
            let delta = processor.map(current).await.expect("map");
            store
                .apply(
                    &processor,
                    cursor(&processor, current, sequence),
                    &delta,
                    &[],
                )
                .await
                .expect("apply");
        }
        store
            .create_consumer(
                &processor.descriptor,
                "upgrade-replay",
                ConsumerRole::Required,
                ConsumerStartPosition::EarliestRetained,
                Duration::from_mins(1),
            )
            .await
            .expect("consumer");
        let delivered = store
            .consumer_changes(&processor.descriptor, ChainId(1), "upgrade-replay", 100)
            .await
            .expect("initial delivery");
        assert_eq!(delivered.len(), 2);
        assert_eq!(
            store
                .consumer(&processor.descriptor, "upgrade-replay")
                .await
                .expect("consumer")
                .expect("consumer exists")
                .acknowledged_sequence,
            0
        );
        let epoch = store.epoch();
        store.inner.pool.close().await;
        drop(store);

        let options =
            SqliteConnectOptions::from_str(&format!("sqlite://{}", path.to_string_lossy()))
                .expect("options");
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .expect("downgrade fixture pool");
        sqlx::raw_sql(
            "
            DROP TRIGGER processor_artifact_totals_processor_insert;
            DROP TRIGGER processor_artifact_totals_artifact_insert;
            DROP TRIGGER processor_artifact_totals_artifact_delete;
            DROP TRIGGER processor_artifact_totals_candidate_insert;
            DROP TRIGGER processor_artifact_totals_candidate_delete;
            DROP TRIGGER processor_artifact_totals_owner_insert;
            DROP TRIGGER processor_artifact_totals_owner_delete;
            DROP TRIGGER processor_artifact_totals_segment_insert;
            DROP TRIGGER processor_artifact_totals_segment_delete;
            DROP TRIGGER processor_artifact_totals_segment_update;
            DROP TRIGGER processor_artifact_totals_segment_owner_insert;
            DROP TRIGGER processor_artifact_totals_segment_owner_delete;
            DROP TABLE processor_artifact_bulk_accounting;
            DROP TABLE processor_artifact_totals;
            DROP TABLE processor_artifact_segment_owners;
            DROP TABLE processor_artifact_segments;
            DROP TABLE processor_artifact_candidates;
            DROP TABLE processor_artifact_owners;
            DROP TABLE processor_artifacts;
            DROP TABLE historical_work_identities;
            DROP TABLE live_lane_gaps;
            DROP TABLE backfill_subscription_preexisting_ranges;
            DROP TABLE finalized_coverage_owners;
            DROP TABLE finalized_coverage_segments;
            DROP TABLE finalized_coverage_intervals;
            ALTER TABLE processor_coverage DROP COLUMN parent_hash;
            DROP TABLE backfill_subscription_ranges;
            DROP TABLE backfill_subscriptions;
            DROP INDEX change_log_backfill_completion;
            DROP INDEX change_log_stream_origin_sequence;
            DROP INDEX change_log_stream_sequence_unique;
            DROP INDEX change_log_stream_sequence;
            ALTER TABLE change_log DROP COLUMN publication_revision;
            ALTER TABLE change_log DROP COLUMN origin_id;
            ALTER TABLE change_log DROP COLUMN origin_kind;
            ALTER TABLE change_log DROP COLUMN stream_sequence;
            ALTER TABLE change_log DROP COLUMN stream_id;

            ALTER TABLE durable_consumers RENAME TO v8_durable_consumers;
            CREATE TABLE durable_consumers (
                instance TEXT NOT NULL
                    REFERENCES processor_instances(instance) ON DELETE CASCADE,
                consumer_id TEXT NOT NULL,
                role TEXT NOT NULL CHECK (role IN ('required', 'best_effort')),
                state TEXT NOT NULL CHECK (state IN ('active', 'reset_required', 'revoked')),
                acknowledged_sequence INTEGER NOT NULL,
                delivered_sequence INTEGER NOT NULL,
                lease_ttl_ms INTEGER NOT NULL CHECK (lease_ttl_ms > 0),
                lease_expires_at_unix_ms INTEGER NOT NULL,
                created_at_unix_ms INTEGER NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL,
                credential_hash BLOB CHECK (
                    credential_hash IS NULL OR length(credential_hash) = 32
                ),
                PRIMARY KEY (instance, consumer_id),
                CHECK (acknowledged_sequence <= delivered_sequence)
            ) WITHOUT ROWID, STRICT;
            INSERT INTO durable_consumers
            SELECT
                instance, consumer_id, role, state, acknowledged_sequence,
                delivered_sequence, lease_ttl_ms, lease_expires_at_unix_ms,
                created_at_unix_ms, updated_at_unix_ms, credential_hash
            FROM v8_durable_consumers;
            DROP TABLE v8_durable_consumers;

            DROP TABLE delivery_streams;
            CREATE TABLE delivery_streams (
                instance TEXT PRIMARY KEY NOT NULL
                    REFERENCES processor_instances(instance) ON DELETE CASCADE,
                pruned_through_sequence INTEGER NOT NULL DEFAULT 0,
                live_bytes INTEGER NOT NULL DEFAULT 0,
                format_version INTEGER NOT NULL DEFAULT 1 CHECK (format_version = 1)
            ) WITHOUT ROWID, STRICT;
            INSERT INTO delivery_streams(
                instance, pruned_through_sequence, live_bytes, format_version
            )
            SELECT instance, 0, 0, 1 FROM processor_instances;
            UPDATE delivery_streams
            SET live_bytes = (
                SELECT COALESCE(SUM(length(entity_key) + length(payload)), 0)
                FROM change_log
                WHERE change_log.instance = delivery_streams.instance
            );
            UPDATE node_meta SET value = X'00000007' WHERE key = 'schema_version';
            ",
        )
        .execute(&pool)
        .await
        .expect("represent the pre-v8 binary schema");
        pool.close().await;

        let upgraded = SqliteStore::open(StoreConfig::new(&path))
            .await
            .expect("upgrade to v8");
        assert_eq!(upgraded.epoch(), epoch);
        assert_eq!(
            upgraded.stats().await.expect("stats").schema_version,
            CURRENT_SCHEMA_VERSION
        );
        let replayed = upgraded
            .consumer_changes(&processor.descriptor, ChainId(1), "upgrade-replay", 100)
            .await
            .expect("replay unacknowledged changes");
        assert_eq!(replayed, delivered);
        assert!(
            replayed
                .iter()
                .all(|change| change.delivery_encoding_version == 1)
        );
        let acknowledged = upgraded
            .acknowledge_consumer(&processor.descriptor, "upgrade-replay", 2)
            .await
            .expect("acknowledge after upgrade");
        assert_eq!(acknowledged.acknowledged_sequence, 2);
    }

    #[tokio::test]
    async fn hot_cold_handoff_persists_exact_overlap_verdicts() {
        let (_directory, store) = store().await;
        let processor = FixtureProcessor::new();
        let first = frame(1, BlockHash::ZERO);
        let second = frame(2, first.block.hash);
        let third = frame(3, second.block.hash);
        for (sequence, current) in [(1, &first), (2, &second), (3, &third)] {
            store
                .store_recent_frame(current)
                .await
                .expect("store live frame");
            let delta = processor.map(current).await.expect("map historical");
            store
                .apply(
                    &processor,
                    cursor(&processor, current, sequence),
                    &delta,
                    &[],
                )
                .await
                .expect("apply historical");
        }
        let overlap = BlockRange::new(BlockNumber(1), BlockNumber(3)).expect("overlap");
        let running = store
            .begin_hot_cold_handoff(
                "fixture-handoff",
                &processor.descriptor,
                ChainId(1),
                overlap,
                third.block.hash,
            )
            .await
            .expect("begin");
        assert_eq!(running.state, HotColdHandoffState::Running);
        let verified = store
            .verify_hot_cold_handoff(
                "fixture-handoff",
                &processor.descriptor,
                ChainId(1),
                overlap,
                third.block.hash,
            )
            .await
            .expect("verify");
        assert_eq!(verified.state, HotColdHandoffState::Verified);
        assert_eq!(verified.compared_blocks, 3);
        assert_eq!(
            store
                .hot_cold_handoff("fixture-handoff", &processor.descriptor)
                .await
                .expect("read"),
            Some(verified)
        );

        let (_directory, conflicting_store) = self::store().await;
        let live = frame(1, BlockHash::ZERO);
        conflicting_store
            .store_recent_frame(&live)
            .await
            .expect("store canonical live");
        let mut historical = live.clone();
        historical.block.hash = BlockHash::new([0x99; 32]);
        let delta = processor.map(&historical).await.expect("map conflicting");
        conflicting_store
            .apply(&processor, cursor(&processor, &historical, 1), &delta, &[])
            .await
            .expect("apply conflicting history");
        let single = BlockRange::single(BlockNumber(1));
        let error = conflicting_store
            .verify_hot_cold_handoff(
                "conflicting-handoff",
                &processor.descriptor,
                ChainId(1),
                single,
                live.block.hash,
            )
            .await
            .expect_err("mismatch");
        assert!(matches!(error, StoreError::HandoffMismatch { .. }));
        assert_eq!(
            conflicting_store
                .hot_cold_handoff("conflicting-handoff", &processor.descriptor)
                .await
                .expect("failed record")
                .expect("record")
                .state,
            HotColdHandoffState::Failed
        );
    }

    #[tokio::test]
    async fn apply_is_atomic_idempotent_and_queryable() {
        let (_directory, store) = store().await;
        let processor = FixtureProcessor::new();
        let first_frame = frame(1, BlockHash::ZERO);
        let delta = processor.map(&first_frame).await.expect("map");
        let first_cursor = cursor(&processor, &first_frame, 1);
        let outcome = store
            .apply(
                &processor,
                first_cursor.clone(),
                &delta,
                &["fixture-sink".to_owned()],
            )
            .await
            .expect("apply");
        assert!(matches!(outcome, ApplyOutcome::Applied { .. }));
        assert_eq!(
            store
                .entity(&processor.descriptor, "state", b"counter")
                .await
                .expect("entity"),
            Some(1_u64.to_be_bytes().to_vec())
        );
        assert_eq!(
            store
                .apply(&processor, first_cursor.clone(), &delta, &[])
                .await
                .expect("duplicate"),
            ApplyOutcome::AlreadyApplied
        );
        let conflicting = frame(1, BlockHash::new([9; 32]));
        let mut conflicting = conflicting;
        conflicting.block.hash = BlockHash::new([8; 32]);
        let conflicting_delta = processor.map(&conflicting).await.expect("map conflict");
        assert!(matches!(
            store
                .apply(
                    &processor,
                    cursor(&processor, &conflicting, 2),
                    &conflicting_delta,
                    &[]
                )
                .await,
            Err(StoreError::CanonicalConflict {
                block: BlockNumber(1),
                ..
            })
        ));
        assert_eq!(
            store
                .processor_cursor(&processor.descriptor)
                .await
                .expect("cursor"),
            Some(first_cursor)
        );
        assert_eq!(
            store
                .changes(&processor.descriptor, ChainId(1), 0, 100)
                .await
                .expect("changes")
                .len(),
            1
        );
        let processor_stats = store
            .processor_stats(&processor.descriptor)
            .await
            .expect("processor stats");
        assert_eq!(processor_stats.applied_blocks, 1);
        assert_eq!(processor_stats.entities, 1);
        assert_eq!(processor_stats.changes, 1);
        assert_eq!(processor_stats.outbox_records, 1);
        assert!(processor_stats.entity_bytes > 0);
        assert!(processor_stats.undo_bytes > 0);
        store.verify().await.expect("verify");
    }

    #[tokio::test]
    async fn delivery_none_materializes_queryable_sqlite_without_delivery_artifacts() {
        let (_directory, store) = store().await;
        let mut processor = FixtureProcessor::new();
        processor.descriptor.lifecycle.delivery.mode = DeliveryPolicyMode::None;
        processor.descriptor.lifecycle.delivery.consumers.clear();
        let first_frame = frame(1, BlockHash::ZERO);
        let delta = processor.map(&first_frame).await.expect("map");

        let outcome = store
            .apply(&processor, cursor(&processor, &first_frame, 1), &delta, &[])
            .await
            .expect("materialize");
        assert!(matches!(
            outcome,
            ApplyOutcome::Applied {
                first_change_sequence: None,
                last_change_sequence: None,
                ..
            }
        ));
        assert_eq!(
            store
                .entity(&processor.descriptor, "state", b"counter")
                .await
                .expect("entity"),
            Some(1_u64.to_be_bytes().to_vec())
        );
        assert!(
            store
                .delivery_streams(&processor.descriptor)
                .await
                .expect("delivery streams")
                .is_empty()
        );
        let stats = store
            .processor_stats(&processor.descriptor)
            .await
            .expect("processor stats");
        assert_eq!(stats.changes, 0);
        assert_eq!(stats.outbox_records, 0);
        assert_eq!(stats.applied_blocks, 1);
    }

    #[tokio::test]
    async fn block_local_output_none_keeps_delivery_and_undo_without_entities() {
        let (_directory, store) = store().await;
        let mut processor = FixtureProcessor::new();
        processor.descriptor.mode = ReductionMode::BlockLocal;
        processor.descriptor.lifecycle.output.mode = OutputPolicyMode::None;
        let first_frame = frame(1, BlockHash::ZERO);
        let delta = processor.map(&first_frame).await.expect("map");
        store
            .apply(&processor, cursor(&processor, &first_frame, 1), &delta, &[])
            .await
            .expect("apply");

        let stats = store
            .processor_stats(&processor.descriptor)
            .await
            .expect("stats");
        assert_eq!(stats.entities, 0);
        assert_eq!(stats.index_entries, 0);
        assert_eq!(stats.changes, 1);
        assert_eq!(stats.undo_records, 1);
        assert_eq!(
            store
                .entity(&processor.descriptor, "state", b"counter")
                .await
                .expect("entity"),
            None
        );

        let undone = store
            .undo(
                &processor.descriptor,
                first_frame.chain_id,
                first_frame.block.number,
                first_frame.block.hash,
                &[],
            )
            .await
            .expect("undo");
        assert!(undone.restored_mutations > 0);
        assert_eq!(
            store
                .changes(&processor.descriptor, ChainId(1), 0, 100)
                .await
                .expect("changes")
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn finalized_externalized_coverage_compacts_into_bounded_proof_segments() {
        let (_directory, store) = store().await;
        let mut processor = FixtureProcessor::block_output();
        processor.descriptor.lifecycle.output.mode = OutputPolicyMode::None;
        processor.descriptor.lifecycle.delivery.mode = DeliveryPolicyMode::None;
        let mut parent = BlockHash::ZERO;
        let mut frames = Vec::new();
        for number in 1_u64..=6 {
            let mut current = frame(number, parent);
            current.finality = Finality::Finalized;
            let delta = processor.map(&current).await.expect("map");
            store
                .apply(
                    &processor,
                    cursor(&processor, &current, number),
                    &delta,
                    &[],
                )
                .await
                .expect("apply finalized block");
            parent = current.block.hash;
            frames.push(current);
        }

        let requested = BlockRange::new(BlockNumber(1), BlockNumber(6)).expect("range");
        assert_eq!(
            store
                .finalized_coverage(&processor.descriptor, requested)
                .await
                .expect("exact coverage"),
            vec![requested]
        );
        let compacted = store
            .compact_finalized_coverage(&processor.descriptor, BlockNumber(6), 2, 4)
            .await
            .expect("compact first bounded interval");
        assert_eq!(compacted.intervals_created, 1);
        assert_eq!(compacted.segments_created, 2);
        assert_eq!(compacted.exact_coverage_deleted, 4);
        assert_eq!(compacted.applied_blocks_deleted, 4);
        assert_eq!(
            compacted.compacted_range,
            Some(BlockRange::new(BlockNumber(1), BlockNumber(4)).expect("compacted range"))
        );
        assert_eq!(
            store
                .finalized_coverage(&processor.descriptor, requested)
                .await
                .expect("mixed exact and compact coverage"),
            vec![requested]
        );
        let segments = store
            .finalized_coverage_segments(&processor.descriptor, requested)
            .await
            .expect("compact proof segments");
        assert_eq!(segments.len(), 2);
        assert_eq!(
            segments[0].range,
            BlockRange::new(BlockNumber(1), BlockNumber(2)).expect("segment")
        );
        assert_eq!(segments[0].start_parent_hash, BlockHash::ZERO);
        assert_eq!(segments[0].end_hash, frames[1].block.hash);
        assert_eq!(
            store
                .coverage_hash(&processor.descriptor, BlockNumber(2))
                .await
                .expect("segment end anchor"),
            Some(frames[1].block.hash)
        );
        assert_eq!(
            store
                .coverage_parent_hash(&processor.descriptor, BlockNumber(1))
                .await
                .expect("segment start anchor"),
            Some(BlockHash::ZERO)
        );
        assert_eq!(
            store
                .processor_stats(&processor.descriptor)
                .await
                .expect("stats")
                .applied_blocks,
            2
        );
    }

    #[tokio::test]
    async fn finalized_node_owned_compaction_preserves_queryable_output() {
        let (_directory, store) = store().await;
        let mut processor = FixtureProcessor::block_output();
        processor.descriptor.lifecycle.output.mode = OutputPolicyMode::Full;
        processor.descriptor.lifecycle.delivery.mode = DeliveryPolicyMode::None;
        let mut parent = BlockHash::ZERO;
        let mut items = Vec::new();
        for number in 1_u64..=6 {
            let mut current = frame(number, parent);
            current.finality = Finality::Finalized;
            let delta = processor.map(&current).await.expect("map");
            items.push(HistoricalBatchItem {
                delta,
                finality: Finality::Finalized,
                publish_changes: false,
            });
            parent = current.block.hash;
        }
        let job = JobRecord {
            id: "node-owned-compaction".to_owned(),
            kind: "materialization_job".to_owned(),
            state: JobState::Queued,
            payload: b"node-owned-compaction-job".to_vec(),
            checkpoint: None,
            attempts: 0,
            updated_at_unix_ms: 1,
        };
        store
            .save_job(&job)
            .await
            .expect("save materialization job");
        store
            .commit_historical_materialization_microbatch(
                &processor,
                &items,
                &job.id,
                b"checkpoint",
                0,
                HistoricalArtifactTarget::Sqlite,
                HistoricalCommitLimits {
                    maximum_changes: 100,
                    maximum_encoded_bytes: 1024 * 1024,
                },
            )
            .await
            .expect("commit materialization microbatch");
        let before = store
            .processor_stats(&processor.descriptor)
            .await
            .expect("stats before compaction");
        assert!(before.entities > 0);
        assert_eq!(
            store
                .compactable_finalized_coverage_blocks(&processor.descriptor, BlockNumber(6),)
                .await
                .expect("compactable coverage"),
            6
        );

        let compacted = store
            .compact_finalized_coverage(&processor.descriptor, BlockNumber(6), 2, 6)
            .await
            .expect("compact node-owned metadata");
        assert_eq!(compacted.exact_coverage_deleted, 6);
        assert_eq!(compacted.applied_blocks_deleted, 6);
        assert_eq!(compacted.finalized_undo_deleted, 6);

        let after = store
            .processor_stats(&processor.descriptor)
            .await
            .expect("stats after compaction");
        assert_eq!(after.entities, before.entities);
        assert_eq!(after.entity_bytes, before.entity_bytes);
        assert_eq!(after.applied_blocks, 0);
        assert_eq!(after.undo_records, 0);
        store.compact().await.expect("compact physical store");
        assert_eq!(
            store
                .storage_stats()
                .await
                .expect("physical storage after compact")
                .wal_bytes,
            0
        );
        assert_eq!(
            store
                .finalized_coverage(
                    &processor.descriptor,
                    BlockRange::new(BlockNumber(1), BlockNumber(6)).expect("range"),
                )
                .await
                .expect("compact coverage"),
            vec![BlockRange::new(BlockNumber(1), BlockNumber(6)).expect("range")]
        );
    }

    #[tokio::test]
    async fn artifact_only_historical_microbatch_keeps_only_compact_results() {
        let (_directory, store) = store().await;
        let mut processor = FixtureProcessor::block_output();
        processor.descriptor.lifecycle.artifacts.mode = ArtifactPolicyMode::Full;
        processor.descriptor.lifecycle.output.mode = OutputPolicyMode::None;
        processor.descriptor.lifecycle.delivery.mode = DeliveryPolicyMode::None;
        processor.descriptor.lifecycle.delivery.consumers.clear();
        let mut parent = BlockHash::ZERO;
        let mut items = Vec::new();
        for number in 1_u64..=3 {
            let mut current = frame(number, parent);
            current.finality = Finality::Finalized;
            items.push(HistoricalBatchItem {
                delta: processor.map(&current).await.expect("map"),
                finality: Finality::Finalized,
                publish_changes: false,
            });
            parent = current.block.hash;
        }
        let job = JobRecord {
            id: "artifact-only-history".to_owned(),
            kind: "materialization_job".to_owned(),
            state: JobState::Queued,
            payload: b"artifact-only-history-job".to_vec(),
            checkpoint: None,
            attempts: 0,
            updated_at_unix_ms: 1,
        };
        store.save_job(&job).await.expect("save artifact job");

        store
            .commit_historical_materialization_microbatch(
                &processor,
                &items,
                &job.id,
                b"checkpoint",
                0,
                HistoricalArtifactTarget::Sqlite,
                HistoricalCommitLimits {
                    maximum_changes: 100,
                    maximum_encoded_bytes: 1024 * 1024,
                },
            )
            .await
            .expect("commit artifact-only microbatch");

        let stats = store
            .processor_stats(&processor.descriptor)
            .await
            .expect("artifact-only stats");
        assert_eq!(stats.processor_artifacts, 3);
        assert_eq!(stats.processor_artifact_owners, 3);
        assert_eq!(stats.pending_processor_artifacts, 0);
        assert_eq!(stats.entities, 0);
        assert_eq!(stats.index_entries, 0);
        assert_eq!(stats.changes, 0);
        assert_eq!(stats.outbox_records, 0);
        store
            .verify()
            .await
            .expect("bulk artifact totals remain exact");
        assert_eq!(
            store
                .scan_processor_artifacts(
                    &processor.descriptor,
                    BlockRange::new(BlockNumber(1), BlockNumber(3)).expect("range"),
                    10,
                )
                .await
                .expect("scan artifacts")
                .into_iter()
                .map(|artifact| artifact.delta)
                .collect::<Vec<_>>(),
            items.into_iter().map(|item| item.delta).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn externally_committed_artifacts_advance_coverage_without_sqlite_copies() {
        let (_directory, store) = store().await;
        let mut processor = FixtureProcessor::block_output();
        processor.descriptor.lifecycle.artifacts.mode = ArtifactPolicyMode::Full;
        processor.descriptor.lifecycle.output.mode = OutputPolicyMode::None;
        processor.descriptor.lifecycle.delivery.mode = DeliveryPolicyMode::None;
        processor.descriptor.lifecycle.delivery.consumers.clear();
        let mut parent = BlockHash::ZERO;
        let mut items = Vec::new();
        for number in 1_u64..=3 {
            let mut current = frame(number, parent);
            current.finality = Finality::Finalized;
            items.push(HistoricalBatchItem {
                delta: processor.map(&current).await.expect("map"),
                finality: Finality::Finalized,
                publish_changes: false,
            });
            parent = current.block.hash;
        }
        let job = JobRecord {
            id: "external-artifact-history".to_owned(),
            kind: "materialization_job".to_owned(),
            state: JobState::Queued,
            payload: b"external-artifact-history-job".to_vec(),
            checkpoint: None,
            attempts: 0,
            updated_at_unix_ms: 1,
        };
        store.save_job(&job).await.expect("save artifact job");

        store
            .commit_historical_materialization_microbatch(
                &processor,
                &items,
                &job.id,
                b"checkpoint",
                0,
                HistoricalArtifactTarget::ExternalCommitted,
                HistoricalCommitLimits {
                    maximum_changes: 100,
                    maximum_encoded_bytes: 1024 * 1024,
                },
            )
            .await
            .expect("commit external-artifact microbatch");

        let stats = store
            .processor_stats(&processor.descriptor)
            .await
            .expect("external artifact stats");
        assert_eq!(stats.processor_artifacts, 0);
        assert_eq!(stats.processor_artifact_owners, 0);
        assert_eq!(stats.entities, 0);
        assert_eq!(stats.index_entries, 0);
        assert_eq!(stats.applied_blocks, 3);
        assert_eq!(
            store
                .stats()
                .await
                .expect("store stats")
                .exact_coverage_blocks,
            3
        );
        assert_eq!(
            store
                .processor_cursor(&processor.descriptor)
                .await
                .expect("cursor")
                .expect("committed cursor")
                .block_number,
            BlockNumber(3)
        );
        assert_eq!(
            store
                .job(&job.id)
                .await
                .expect("job")
                .expect("saved job")
                .checkpoint,
            Some(b"checkpoint".to_vec())
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn deleting_last_subscription_owner_releases_compact_coverage() {
        let (_directory, store) = store().await;
        let mut processor = FixtureProcessor::block_output();
        processor.descriptor.lifecycle.output.mode = OutputPolicyMode::None;
        store
            .register_processor(&processor.descriptor)
            .await
            .expect("register");
        let id = "owned-coverage";
        let stream = store
            .create_backfill_delivery_stream(&processor.descriptor, id)
            .await
            .expect("stream")
            .stream_id;
        store
            .create_consumer_in_stream(
                &processor.descriptor,
                &stream,
                "destination",
                ConsumerRole::Required,
                ConsumerStartPosition::EarliestRetained,
                Duration::from_secs(30),
            )
            .await
            .expect("consumer");
        let range = BlockRange::new(BlockNumber(1), BlockNumber(2)).expect("range");
        let job = JobRecord {
            id: id.to_owned(),
            kind: "backfill_subscription_job".to_owned(),
            state: JobState::Queued,
            payload: b"owned-coverage-job".to_vec(),
            checkpoint: None,
            attempts: 0,
            updated_at_unix_ms: 1,
        };
        store
            .create_backfill_subscription_job(
                &BackfillSubscriptionRecord {
                    subscription_id: id.to_owned(),
                    job_id: id.to_owned(),
                    processor_instance: processor.descriptor.instance.to_string(),
                    history_stream_id: stream.clone(),
                    mode: BackfillSubscriptionMode::FillMissing,
                    publication_revision: 0,
                    state: BackfillSubscriptionState::Queued,
                    consumer_id: "destination".to_owned(),
                    ranges: vec![range],
                    range,
                    preexisting_coverage: Vec::new(),
                    captured_finalized_target: range.end(),
                    idempotency_key: "owned-coverage-key".to_owned(),
                    effective_block_limit: 16,
                    effective_byte_limit: 1024 * 1024,
                    resume_below_ratio_millionths: 750_000,
                    delivery_batch_limits: BackfillDeliveryBatchLimits::default(),
                    initial_sequence: 0,
                    completion_sequence: None,
                    processed_work_blocks: 0,
                },
                &job,
                BlockHash::new([9; 32]),
            )
            .await
            .expect("subscription");
        let first = frame(1, BlockHash::ZERO);
        let second = frame(2, first.block.hash);
        for (sequence, mut current) in [(1_u64, first), (2, second)] {
            current.finality = Finality::Finalized;
            let delta = processor.map(&current).await.expect("map");
            store
                .apply_with_change_publication_to_stream(
                    &processor,
                    cursor(&processor, &current, sequence),
                    &delta,
                    &[],
                    true,
                    &stream,
                )
                .await
                .expect("apply subscription block");
        }
        let completion = store
            .append_backfill_completion(&processor.descriptor, &stream, ChainId(1), range.end())
            .await
            .expect("completion");
        store
            .set_backfill_subscription_state(id, BackfillSubscriptionState::Draining, None)
            .await
            .expect("draining");
        store
            .acknowledge_consumer_in_stream(
                &processor.descriptor,
                &stream,
                "destination",
                completion,
            )
            .await
            .expect("ack completion");
        assert!(
            store
                .mark_backfill_subscription_reclaimable(id, completion)
                .await
                .expect("reclaimable")
        );
        store
            .compact_finalized_coverage(&processor.descriptor, range.end(), 2, range.len())
            .await
            .expect("compact owned coverage");
        let deleted = store
            .delete_terminal_historical_work(id, "owned-coverage:outcome", true)
            .await
            .expect("delete owned coverage");
        assert_eq!(deleted.coverage_intervals, 1);
        assert_eq!(deleted.coverage_segments, 1);
        assert_eq!(
            store
                .coverage(&processor.descriptor, range)
                .await
                .expect("released coverage"),
            Vec::<BlockRange>::new()
        );
        assert!(
            store
                .processor_cursor(&processor.descriptor)
                .await
                .expect("released cursor")
                .is_none()
        );
    }

    #[tokio::test]
    async fn ordered_output_none_keeps_private_working_state_only() {
        let (_directory, store) = store().await;
        let mut processor = FixtureProcessor::new();
        processor.descriptor.lifecycle.output.mode = OutputPolicyMode::None;
        let block = frame(1, BlockHash::ZERO);
        let delta = processor.map(&block).await.expect("map");
        store
            .apply(&processor, cursor(&processor, &block, 1), &delta, &[])
            .await
            .expect("apply");

        assert_eq!(
            store
                .entity(&processor.descriptor, "state", b"counter")
                .await
                .expect("public entity"),
            None
        );
        let stats = store
            .processor_stats(&processor.descriptor)
            .await
            .expect("stats");
        assert_eq!(stats.entities, 0);
        assert_eq!(stats.state_entries, 1);
        assert!(stats.state_bytes > 0);
    }

    #[tokio::test]
    async fn finalized_block_window_prunes_public_output_not_working_state() {
        let (_directory, store) = store().await;
        let mut processor = FixtureProcessor::block_output();
        processor.descriptor.lifecycle.output.mode = OutputPolicyMode::Window;
        processor.descriptor.lifecycle.output.window = Some(OutputWindow {
            max_blocks: Some(2),
            max_age_seconds: None,
            max_rows: None,
            max_bytes: None,
        });
        let mut parent = BlockHash::ZERO;
        for number in 1..=4 {
            let mut block = frame(number, parent);
            block.finality = Finality::Finalized;
            let delta = processor.map(&block).await.expect("map");
            store
                .apply(&processor, cursor(&processor, &block, number), &delta, &[])
                .await
                .expect("apply");
            parent = block.block.hash;
        }
        let rows = store
            .scan_entities(&processor.descriptor, "state", None, 10)
            .await
            .expect("retained output");
        assert_eq!(
            rows.iter()
                .map(|(key, _)| {
                    u64::from_be_bytes(key.as_slice().try_into().expect("block key"))
                })
                .collect::<Vec<_>>(),
            vec![3, 4]
        );
        let stats = store
            .processor_stats(&processor.descriptor)
            .await
            .expect("stats");
        assert_eq!(stats.entities, 2);
        assert_eq!(stats.state_entries, 1);
    }

    #[tokio::test]
    async fn automatic_checkpoints_are_bounded_and_savepoints_are_explicit() {
        let (_directory, store) = store().await;
        let mut processor = FixtureProcessor::new();
        processor.descriptor.lifecycle.checkpoint.keep = 2;
        let mut parent = BlockHash::ZERO;
        for number in 1..=3 {
            let mut block = frame(number, parent);
            block.finality = Finality::Finalized;
            let delta = processor.map(&block).await.expect("map");
            store
                .apply(&processor, cursor(&processor, &block, number), &delta, &[])
                .await
                .expect("apply");
            parent = block.block.hash;
        }

        let checkpoints = store
            .recovery_checkpoints(&processor.descriptor)
            .await
            .expect("checkpoints");
        assert_eq!(checkpoints.len(), 2);
        assert_eq!(checkpoints[0].block_number, BlockNumber(3));
        assert_eq!(checkpoints[1].block_number, BlockNumber(2));
        assert!(matches!(
            store
                .restore_recovery_checkpoint(&processor.descriptor, checkpoints[1].checkpoint_id)
                .await,
            Err(StoreError::CheckpointRestoreBoundary {
                checkpoint: BlockNumber(2),
                current: Some(BlockNumber(3))
            })
        ));
        let instance = processor_instance(&processor.descriptor);
        let expected_state: Vec<(String, Vec<u8>, Vec<u8>)> = sqlx::query_as(
            "SELECT namespace, state_key, value
             FROM processor_state WHERE instance = ?
             ORDER BY namespace, state_key",
        )
        .bind(&instance)
        .fetch_all(&store.inner.pool)
        .await
        .expect("expected state");
        assert!(!expected_state.is_empty());
        sqlx::query("UPDATE processor_state SET value = X'ff' WHERE instance = ?")
            .bind(&instance)
            .execute(&store.inner.pool)
            .await
            .expect("inject state corruption");
        let restored = store
            .restore_recovery_checkpoint(&processor.descriptor, checkpoints[0].checkpoint_id)
            .await
            .expect("restore latest checkpoint");
        assert_eq!(restored.block_number, BlockNumber(3));
        let repaired_state: Vec<(String, Vec<u8>, Vec<u8>)> = sqlx::query_as(
            "SELECT namespace, state_key, value
             FROM processor_state WHERE instance = ?
             ORDER BY namespace, state_key",
        )
        .bind(&instance)
        .fetch_all(&store.inner.pool)
        .await
        .expect("repaired state");
        assert_eq!(repaired_state, expected_state);

        let created = store
            .create_portable_savepoint(&processor.descriptor, "before-upgrade")
            .await
            .expect("create savepoint");
        assert_eq!(created.block_number, BlockNumber(3));
        let archive = store
            .export_portable_savepoint(&processor.descriptor, "before-upgrade")
            .await
            .expect("export savepoint");
        let restored_cursor =
            SqliteStore::validate_portable_savepoint(&processor.descriptor, &archive)
                .expect("validate archive");
        assert_eq!(restored_cursor.block_number, BlockNumber(3));
        let mut corrupt = archive;
        let last = corrupt.last_mut().expect("archive bytes");
        *last ^= 1;
        assert!(SqliteStore::validate_portable_savepoint(&processor.descriptor, &corrupt).is_err());
        assert_eq!(
            store
                .portable_savepoints(&processor.descriptor)
                .await
                .expect("savepoints")
                .len(),
            1
        );
        store
            .delete_portable_savepoint(&processor.descriptor, "before-upgrade")
            .await
            .expect("explicit delete");
        assert!(
            store
                .portable_savepoints(&processor.descriptor)
                .await
                .expect("savepoints")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn query_snapshot_keeps_values_and_stream_boundary_stable() {
        let (_directory, store) = store().await;
        let processor = FixtureProcessor::new();
        let first = frame(1, BlockHash::ZERO);
        let first_delta = processor.map(&first).await.expect("first map");
        store
            .apply(&processor, cursor(&processor, &first, 1), &first_delta, &[])
            .await
            .expect("first apply");
        let snapshot = store
            .create_query_snapshot(
                &processor.descriptor,
                "state",
                OutputQuery::default(),
                Duration::from_mins(1),
                100,
                1 << 20,
            )
            .await
            .expect("snapshot");
        assert_eq!(snapshot.boundary_sequence, 1);

        let second = frame(2, first.block.hash);
        let second_delta = processor.map(&second).await.expect("second map");
        store
            .apply(
                &processor,
                cursor(&processor, &second, 2),
                &second_delta,
                &[],
            )
            .await
            .expect("second apply");
        let (metadata, rows) = store
            .query_snapshot_page(
                &processor.descriptor,
                "state",
                snapshot.snapshot_id,
                None,
                10,
            )
            .await
            .expect("snapshot page");
        assert_eq!(metadata.boundary_sequence, 1);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].value, 1_u64.to_be_bytes());
        assert_eq!(
            store
                .entity(&processor.descriptor, "state", b"counter")
                .await
                .expect("current entity"),
            Some(2_u64.to_be_bytes().to_vec())
        );
    }

    #[tokio::test]
    async fn durable_consumer_credentials_are_isolated() {
        let (_directory, store) = store().await;
        let processor = FixtureProcessor::new();
        store
            .create_consumer_with_credential(
                &processor.descriptor,
                "consumer-a",
                ConsumerRole::BestEffort,
                ConsumerStartPosition::CurrentHead,
                Duration::from_mins(1),
                "consumer-a-secret",
            )
            .await
            .expect("consumer");
        assert!(
            store
                .consumer_credential_matches(
                    &processor.descriptor,
                    "consumer-a",
                    "consumer-a-secret"
                )
                .await
                .expect("matching credential")
        );
        assert!(
            !store
                .consumer_credential_matches(
                    &processor.descriptor,
                    "consumer-a",
                    "consumer-b-secret"
                )
                .await
                .expect("wrong credential")
        );
    }

    #[tokio::test]
    async fn recent_frame_replay_ignores_evidence_metadata_but_rejects_material_changes() {
        let (_directory, store) = store().await;
        let original = with_observation(frame(1, BlockHash::ZERO), 1);
        store
            .store_recent_frame(&original)
            .await
            .expect("store original frame");

        let mut replay = original.clone();
        replay.finality = Finality::Finalized;
        replay.provenance[0].observed_at_unix_ms = 2;
        replay.verification.dataset_checksum = VerificationCheck::VERIFIED;
        store
            .store_recent_frame(&replay)
            .await
            .expect("same material with refreshed evidence is idempotent");
        assert_eq!(
            store
                .recent_frame(ChainId(1), original.block.number)
                .await
                .expect("read retained frame"),
            Some(original)
        );

        let mut conflicting = replay;
        let Material::Complete(header) = &mut conflicting.header else {
            panic!("fixture header must be complete");
        };
        header.gas_used = Some(1);
        let error = store
            .store_recent_frame(&conflicting)
            .await
            .expect_err("changed execution material must fail closed");
        assert!(matches!(error, StoreError::Invariant(_)));
    }

    #[tokio::test]
    async fn backup_reopens_as_a_complete_verified_restore() {
        let (directory, store) = store().await;
        let processor = FixtureProcessor::new();
        let frame = frame(1, BlockHash::ZERO);
        let delta = processor.map(&frame).await.expect("map");
        store
            .apply(&processor, cursor(&processor, &frame, 1), &delta, &[])
            .await
            .expect("apply");
        store
            .store_recent_frame(&frame)
            .await
            .expect("recent frame");
        let original_epoch = store.epoch();
        let backup = directory.path().join("backup.sqlite");
        store.backup(&backup).await.expect("backup");
        drop(store);

        let restored = SqliteStore::open(StoreConfig::new(backup))
            .await
            .expect("open restored backup");
        restored.verify().await.expect("verify restored backup");
        assert_eq!(restored.epoch(), original_epoch);
        assert_eq!(
            restored
                .entity(&processor.descriptor, "state", b"counter")
                .await
                .expect("entity"),
            Some(1_u64.to_be_bytes().to_vec())
        );
        let restored_cursor = restored
            .processor_cursor(&processor.descriptor)
            .await
            .expect("cursor")
            .expect("restored cursor");
        assert_eq!(restored_cursor.block_number, frame.block.number);
        assert_eq!(restored_cursor.block_hash, frame.block.hash);
        assert_eq!(
            restored
                .coverage(
                    &processor.descriptor,
                    BlockRange::single(frame.block.number)
                )
                .await
                .expect("coverage"),
            vec![BlockRange::single(frame.block.number)]
        );
        assert_eq!(
            restored
                .recent_frame(frame.chain_id, frame.block.number)
                .await
                .expect("recent frame")
                .expect("restored recent frame"),
            frame
        );
    }

    #[tokio::test]
    async fn undo_restores_preimages_and_appends_monotonic_changes() {
        let (_directory, store) = store().await;
        let processor = FixtureProcessor::new();
        let first = frame(1, BlockHash::ZERO);
        let first_delta = processor.map(&first).await.expect("map first");
        store
            .apply(&processor, cursor(&processor, &first, 1), &first_delta, &[])
            .await
            .expect("apply first");
        let second = frame(2, first.block.hash);
        let second_delta = processor.map(&second).await.expect("map second");
        store
            .apply(
                &processor,
                cursor(&processor, &second, 2),
                &second_delta,
                &[],
            )
            .await
            .expect("apply second");
        let outcome = store
            .undo(
                &processor.descriptor,
                ChainId(1),
                second.block.number,
                second.block.hash,
                &[],
            )
            .await
            .expect("undo");
        assert_eq!(outcome.restored_mutations, 3);
        assert_eq!(
            store
                .entity(&processor.descriptor, "state", b"counter")
                .await
                .expect("entity"),
            Some(1_u64.to_be_bytes().to_vec())
        );
        let changes = store
            .changes(&processor.descriptor, ChainId(1), 0, 100)
            .await
            .expect("changes");
        assert_eq!(changes.len(), 3);
        assert_eq!(changes[2].direction, ChangeDirection::Undo);
        assert_eq!(changes[2].change.payload, 1_u64.to_be_bytes());
        assert_eq!(
            store
                .processor_cursor(&processor.descriptor)
                .await
                .expect("cursor")
                .expect("prior cursor")
                .block_number,
            BlockNumber(1)
        );
    }

    #[tokio::test]
    async fn finalized_block_local_result_can_be_republished_without_mutating_coverage() {
        let (_directory, store) = store().await;
        let processor = FixtureProcessor::block_output();
        let mut current = frame(1, BlockHash::ZERO);
        current.finality = Finality::Finalized;
        let delta = processor.map(&current).await.expect("map");
        store
            .apply(&processor, cursor(&processor, &current, 1), &delta, &[])
            .await
            .expect("apply");
        let original_cursor = store
            .processor_cursor(&processor.descriptor)
            .await
            .expect("cursor")
            .expect("processor cursor");

        let replay = store
            .republish_block_local(&processor, cursor(&processor, &current, 2), &delta, &[])
            .await
            .expect("republish");
        assert_eq!(replay.published_changes, 1);
        assert_eq!(
            store
                .changes(&processor.descriptor, current.chain_id, 0, 100)
                .await
                .expect("changes")
                .len(),
            2
        );
        assert_eq!(
            store
                .coverage(
                    &processor.descriptor,
                    BlockRange::single(current.block.number)
                )
                .await
                .expect("coverage"),
            vec![BlockRange::single(current.block.number)]
        );
        assert_eq!(
            store
                .processor_cursor(&processor.descriptor)
                .await
                .expect("cursor"),
            Some(original_cursor)
        );
    }

    #[tokio::test]
    async fn active_consumer_lease_bounds_change_pruning() {
        let (_directory, store) = store().await;
        let processor = FixtureProcessor::new();
        let first = frame(1, BlockHash::ZERO);
        let second = frame(2, first.block.hash);
        for (sequence, current) in [(1, &first), (2, &second)] {
            let delta = processor.map(current).await.expect("map");
            store
                .apply(
                    &processor,
                    cursor(&processor, current, sequence),
                    &delta,
                    &[],
                )
                .await
                .expect("apply");
        }
        store
            .renew_consumer_lease(
                &processor.descriptor,
                "consumer-a",
                1,
                Duration::from_mins(1),
            )
            .await
            .expect("lease");
        let outcome = store
            .prune_changes_before(&processor.descriptor, 999)
            .await
            .expect("prune");
        assert_eq!(outcome.effective_before, 2);
        assert_eq!(outcome.deleted, 1);
        assert_eq!(
            store
                .change_bounds(&processor.descriptor)
                .await
                .expect("bounds"),
            Some(ChangeBounds {
                earliest: 2,
                latest: 2
            })
        );
    }

    #[tokio::test]
    async fn required_consumer_remains_protective_after_lease_lapse() {
        let (_directory, store) = store().await;
        let processor = FixtureProcessor::new();
        let first = frame(1, BlockHash::ZERO);
        let second = frame(2, first.block.hash);
        for (sequence, current) in [(1, &first), (2, &second)] {
            let delta = processor.map(current).await.expect("map");
            store
                .apply(
                    &processor,
                    cursor(&processor, current, sequence),
                    &delta,
                    &[],
                )
                .await
                .expect("apply");
        }
        store
            .create_consumer(
                &processor.descriptor,
                "required-lapsed",
                ConsumerRole::Required,
                ConsumerStartPosition::EarliestRetained,
                Duration::from_millis(1),
            )
            .await
            .expect("consumer");
        tokio::time::sleep(Duration::from_millis(5)).await;
        let consumer = store
            .consumer(&processor.descriptor, "required-lapsed")
            .await
            .expect("inspect")
            .expect("consumer exists");
        assert!(!consumer.lease_active);
        assert_eq!(consumer.state, ConsumerState::Active);

        let outcome = store
            .prune_changes_before(&processor.descriptor, 999)
            .await
            .expect("prune");
        assert_eq!(outcome.effective_before, 1);
        assert_eq!(outcome.deleted, 0);
    }

    #[tokio::test]
    async fn expired_stream_session_can_be_replaced_without_being_renewed_by_reads() {
        let (_directory, store) = store().await;
        let processor = FixtureProcessor::new();
        let first = frame(1, BlockHash::ZERO);
        let delta = processor.map(&first).await.expect("map");
        store
            .apply(&processor, cursor(&processor, &first, 1), &delta, &[])
            .await
            .expect("apply");
        store
            .create_consumer(
                &processor.descriptor,
                "session-owner",
                ConsumerRole::Required,
                ConsumerStartPosition::EarliestRetained,
                Duration::from_millis(100),
            )
            .await
            .expect("consumer");
        let stream_id = default_delivery_stream_id(&processor.descriptor);
        let first_lease = store
            .acquire_consumer_session_in_stream(&processor.descriptor, &stream_id, "session-owner")
            .await
            .expect("first session");
        assert!(
            store
                .consumer_session_is_current_in_stream(
                    &stream_id,
                    "session-owner",
                    first_lease.generation,
                )
                .await
                .expect("current session")
        );

        tokio::time::sleep(Duration::from_millis(120)).await;
        assert!(
            !store
                .consumer_session_is_current_in_stream(
                    &stream_id,
                    "session-owner",
                    first_lease.generation,
                )
                .await
                .expect("expired session")
        );
        assert!(matches!(
            store
                .renew_consumer_session_in_stream(
                    &stream_id,
                    "session-owner",
                    first_lease.generation,
                )
                .await,
            Err(StoreError::ConsumerSessionLost { .. })
        ));
        let replacement = store
            .acquire_consumer_session_in_stream(&processor.descriptor, &stream_id, "session-owner")
            .await
            .expect("replacement session");
        assert_eq!(replacement.generation, first_lease.generation + 1);
        assert!(
            !store
                .consumer_session_is_current_in_stream(
                    &stream_id,
                    "session-owner",
                    first_lease.generation,
                )
                .await
                .expect("superseded session")
        );
        assert!(matches!(
            store
                .acknowledge_consumer_session_in_stream(
                    &processor.descriptor,
                    &stream_id,
                    "session-owner",
                    first_lease.generation,
                    1,
                )
                .await,
            Err(StoreError::ConsumerSessionLost { .. })
        ));
        let acknowledged = store
            .acknowledge_consumer_session_in_stream(
                &processor.descriptor,
                &stream_id,
                "session-owner",
                replacement.generation,
                1,
            )
            .await
            .expect("replacement session acknowledgement");
        assert_eq!(acknowledged.acknowledged_sequence, 1);
        assert!(acknowledged.lease_active);
    }

    #[tokio::test]
    async fn released_stream_session_reacquires_without_stale_generation_interference() {
        let (_directory, store) = store().await;
        let processor = FixtureProcessor::new();
        store
            .register_processor(&processor.descriptor)
            .await
            .expect("register processor");
        store
            .create_consumer(
                &processor.descriptor,
                "session-owner",
                ConsumerRole::Required,
                ConsumerStartPosition::EarliestRetained,
                Duration::from_secs(30),
            )
            .await
            .expect("consumer");
        let stream_id = default_delivery_stream_id(&processor.descriptor);
        let first = store
            .acquire_consumer_session_in_stream(&processor.descriptor, &stream_id, "session-owner")
            .await
            .expect("first session");
        assert!(
            store
                .release_consumer_session_in_stream(&stream_id, "session-owner", first.generation,)
                .await
                .expect("release first session")
        );
        let successor = store
            .acquire_consumer_session_in_stream(&processor.descriptor, &stream_id, "session-owner")
            .await
            .expect("successor session");
        assert_eq!(successor.generation, first.generation + 1);
        assert!(
            !store
                .release_consumer_session_in_stream(&stream_id, "session-owner", first.generation,)
                .await
                .expect("stale release")
        );
        assert!(
            store
                .consumer_session_is_current_in_stream(
                    &stream_id,
                    "session-owner",
                    successor.generation,
                )
                .await
                .expect("successor remains current")
        );
    }

    #[tokio::test]
    async fn acknowledgement_is_bounded_by_committed_stream_head() {
        let (_directory, store) = store().await;
        let processor = FixtureProcessor::block_output();
        let first = frame(1, BlockHash::ZERO);
        let second = frame(2, first.block.hash);
        let third = frame(3, second.block.hash);
        for (sequence, current) in [(1, &first), (2, &second), (3, &third)] {
            let delta = processor.map(current).await.expect("map");
            store
                .apply(
                    &processor,
                    cursor(&processor, current, sequence),
                    &delta,
                    &[],
                )
                .await
                .expect("apply");
        }
        store
            .create_consumer(
                &processor.descriptor,
                "bounded-ack",
                ConsumerRole::Required,
                ConsumerStartPosition::EarliestRetained,
                Duration::from_mins(1),
            )
            .await
            .expect("consumer");
        let first_ack = store
            .acknowledge_consumer(&processor.descriptor, "bounded-ack", 1)
            .await
            .expect("acknowledge committed head without a durable sent watermark");
        assert_eq!(first_ack.acknowledged_sequence, 1);
        assert_eq!(first_ack.delivered_sequence, 1);
        let changes = store
            .consumer_changes(&processor.descriptor, ChainId(1), "bounded-ack", 100)
            .await
            .expect("deliver changes");
        assert_eq!(changes.len(), 2);
        assert!(
            changes
                .iter()
                .all(|change| change.delivery_encoding_version == 1)
        );
        assert_eq!(
            store
                .consumer(&processor.descriptor, "bounded-ack")
                .await
                .expect("consumer")
                .expect("consumer exists")
                .delivered_sequence,
            1,
            "stream reads must not persist a sent watermark"
        );
        assert!(matches!(
            store
                .acknowledge_consumer(&processor.descriptor, "bounded-ack", 4)
                .await,
            Err(StoreError::AcknowledgementBeyondHead {
                sequence: 4,
                head: 3
            })
        ));
        let acknowledged = store
            .acknowledge_consumer(&processor.descriptor, "bounded-ack", 2)
            .await
            .expect("acknowledge delivered head");
        assert_eq!(acknowledged.acknowledged_sequence, 2);
        let pruned = store
            .prune_changes_before(&processor.descriptor, 3)
            .await
            .expect("prune acknowledged head");
        assert_eq!(pruned.deleted, 2);
        let retried = store
            .acknowledge_consumer(&processor.descriptor, "bounded-ack", 2)
            .await
            .expect("retry already-applied acknowledgement after pruning");
        assert_eq!(retried.acknowledged_sequence, 2);
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn split_live_and_backfill_consumers_have_independent_progress() {
        let (_directory, store) = store().await;
        let mut processor = FixtureProcessor::block_output();
        processor.descriptor.delivery_ordering = DeliveryOrdering::BlockVersionedIdempotent;
        let live_stream = default_delivery_stream_id(&processor.descriptor);
        store
            .register_processor(&processor.descriptor)
            .await
            .expect("register split processor");
        let history_stream = store
            .create_backfill_delivery_stream(&processor.descriptor, "subscription-a")
            .await
            .expect("history stream")
            .stream_id;
        for stream_id in [&live_stream, &history_stream] {
            store
                .create_consumer_in_stream(
                    &processor.descriptor,
                    stream_id,
                    "destination",
                    ConsumerRole::Required,
                    ConsumerStartPosition::EarliestRetained,
                    Duration::from_mins(1),
                )
                .await
                .expect("stream consumer");
        }
        let job = JobRecord {
            id: "subscription-a".to_owned(),
            kind: "backfill_subscription_job".to_owned(),
            state: JobState::Queued,
            payload: b"stable-job".to_vec(),
            checkpoint: None,
            attempts: 0,
            updated_at_unix_ms: 1,
        };
        let subscription = BackfillSubscriptionRecord {
            subscription_id: "subscription-a".to_owned(),
            job_id: job.id.clone(),
            processor_instance: processor.descriptor.instance.to_string(),
            history_stream_id: history_stream.clone(),
            mode: BackfillSubscriptionMode::FillMissing,
            publication_revision: 0,
            state: BackfillSubscriptionState::Queued,
            consumer_id: "destination".to_owned(),
            ranges: vec![BlockRange::new(BlockNumber(1), BlockNumber(2)).expect("range")],
            range: BlockRange::new(BlockNumber(1), BlockNumber(2)).expect("range"),
            preexisting_coverage: vec![BlockRange::single(BlockNumber(1))],
            captured_finalized_target: BlockNumber(2),
            idempotency_key: "fixture-idempotency".to_owned(),
            effective_block_limit: 2,
            effective_byte_limit: 1_024,
            resume_below_ratio_millionths: 750_000,
            delivery_batch_limits: BackfillDeliveryBatchLimits {
                target_encoded_bytes: 512,
                maximum_encoded_bytes: 1_024,
                maximum_events: 10,
                maximum_processed_blocks: 2,
                maximum_delay_ms: 25,
                maximum_buffered_batches: 2,
                maximum_buffered_bytes: 2_048,
                compression: BackfillDeliveryCompression::None,
            },
            initial_sequence: 0,
            completion_sequence: None,
            processed_work_blocks: 0,
        };
        assert_eq!(
            store
                .create_backfill_subscription_job(&subscription, &job, BlockHash::new([1; 32]))
                .await
                .expect("create subscription"),
            job
        );
        assert_eq!(
            store
                .create_backfill_subscription_job(&subscription, &job, BlockHash::new([1; 32]))
                .await
                .expect("idempotent subscription"),
            job
        );
        let stored_subscription = store
            .backfill_subscription_for_job(&job.id)
            .await
            .expect("read subscription")
            .expect("subscription exists");
        assert_eq!(stored_subscription.publication_revision, 0);
        assert_eq!(
            stored_subscription.delivery_batch_limits,
            subscription.delivery_batch_limits
        );
        let mut conflicting_subscription = subscription.clone();
        conflicting_subscription.effective_block_limit = 1;
        assert!(matches!(
            store
                .create_backfill_subscription_job(
                    &conflicting_subscription,
                    &job,
                    BlockHash::new([2; 32]),
                )
                .await,
            Err(StoreError::InvalidConfig(message))
                if message.contains("idempotency key")
        ));

        let live_frame = frame(1, BlockHash::ZERO);
        let live_delta = processor.map(&live_frame).await.expect("map live");
        store
            .apply(
                &processor,
                cursor(&processor, &live_frame, 1),
                &live_delta,
                &[],
            )
            .await
            .expect("publish live");
        let history_frame = frame(2, live_frame.block.hash);
        let history_delta = processor.map(&history_frame).await.expect("map history");
        store
            .apply_with_change_publication_to_stream(
                &processor,
                cursor(&processor, &history_frame, 2),
                &history_delta,
                &[],
                true,
                &history_stream,
            )
            .await
            .expect("publish history");

        let live = store
            .consumer_changes_in_stream(
                &processor.descriptor,
                &live_stream,
                ChainId(1),
                "destination",
                100,
            )
            .await
            .expect("live changes");
        let history = store
            .consumer_changes_in_stream(
                &processor.descriptor,
                &history_stream,
                ChainId(1),
                "destination",
                100,
            )
            .await
            .expect("history changes");
        assert_eq!(
            live.iter()
                .map(|change| change.block.number)
                .collect::<Vec<_>>(),
            vec![BlockNumber(1)]
        );
        assert_eq!(
            history
                .iter()
                .map(|change| change.block.number)
                .collect::<Vec<_>>(),
            vec![BlockNumber(2), BlockNumber(2)]
        );
        assert_eq!(
            history
                .iter()
                .map(|change| change.change.kind.as_str())
                .collect::<Vec<_>>(),
            vec!["fixture.counter", "system.backfill_progress"]
        );
        let durable_subscription = store
            .backfill_subscription_for_job(&job.id)
            .await
            .expect("subscription lookup")
            .expect("subscription exists");
        assert_eq!(durable_subscription.processed_work_blocks, 1);
        let committed_range_blocks: i64 = sqlx::query_scalar(
            "SELECT committed_blocks
             FROM backfill_subscription_ranges
             WHERE subscription_id = ? AND ordinal = 0",
        )
        .bind(&subscription.subscription_id)
        .fetch_one(&store.inner.pool)
        .await
        .expect("range progress");
        assert_eq!(committed_range_blocks, 1);

        let history_head = history.last().expect("history head").cursor.sequence;
        assert!(matches!(
            store
                .acknowledge_consumer_in_stream(
                    &processor.descriptor,
                    &history_stream,
                    "destination",
                    history[0].cursor.sequence,
                )
                .await,
            Err(StoreError::AcknowledgementNotBoundary { .. })
        ));
        store
            .acknowledge_consumer_in_stream(
                &processor.descriptor,
                &history_stream,
                "destination",
                history_head,
            )
            .await
            .expect("ack history");
        let acknowledged_work_blocks: i64 = sqlx::query_scalar(
            "SELECT acknowledged_work_blocks
             FROM durable_consumers
             WHERE stream_id = ? AND consumer_id = 'destination'",
        )
        .bind(&history_stream)
        .fetch_one(&store.inner.pool)
        .await
        .expect("acknowledged work");
        assert_eq!(acknowledged_work_blocks, 1);
        assert_eq!(
            store
                .consumer_in_stream(&processor.descriptor, &live_stream, "destination")
                .await
                .expect("live consumer")
                .expect("live consumer exists")
                .acknowledged_sequence,
            0
        );
        assert_eq!(
            store
                .delivery_stream_stats_in_stream(&processor.descriptor, &history_stream)
                .await
                .expect("history stats")
                .required_ack_watermark,
            Some(history_head)
        );
        assert_eq!(
            store
                .delivery_stream_stats_in_stream(&processor.descriptor, &live_stream)
                .await
                .expect("live stats")
                .required_ack_watermark,
            Some(0)
        );

        let completion = store
            .append_backfill_completion(
                &processor.descriptor,
                &history_stream,
                ChainId(1),
                BlockNumber(2),
            )
            .await
            .expect("append completion");
        assert_eq!(
            store
                .append_backfill_completion(
                    &processor.descriptor,
                    &history_stream,
                    ChainId(1),
                    BlockNumber(2),
                )
                .await
                .expect("idempotent completion"),
            completion
        );
        let terminal = store
            .consumer_changes_in_stream(
                &processor.descriptor,
                &history_stream,
                ChainId(1),
                "destination",
                100,
            )
            .await
            .expect("completion delivery");
        assert_eq!(terminal.len(), 1);
        assert_eq!(terminal[0].change.kind, "system.backfill_complete");
        assert_eq!(terminal[0].cursor.sequence, completion);
        assert!(
            store
                .set_backfill_subscription_state(
                    &job.id,
                    BackfillSubscriptionState::Draining,
                    None,
                )
                .await
                .expect("draining state")
        );
        assert!(
            !store
                .mark_backfill_subscription_reclaimable(&subscription.subscription_id, history_head)
                .await
                .expect("completion still protected")
        );
        assert!(matches!(
            store
                .delete_terminal_historical_work(&job.id, &format!("{}:outcome", job.id), true,)
                .await,
            Err(StoreError::HistoricalWorkNotDeletable { .. })
        ));
        store
            .acknowledge_consumer_in_stream(
                &processor.descriptor,
                &history_stream,
                "destination",
                completion,
            )
            .await
            .expect("ack completion");
        assert!(
            store
                .mark_backfill_subscription_reclaimable(&subscription.subscription_id, completion,)
                .await
                .expect("completion acknowledged")
        );
        let state: String = sqlx::query_scalar(
            "SELECT state FROM backfill_subscriptions WHERE subscription_id = ?",
        )
        .bind(&subscription.subscription_id)
        .fetch_one(&store.inner.pool)
        .await
        .expect("subscription state");
        assert_eq!(state, "complete_reclaimable");
        let deleted = store
            .delete_terminal_historical_work(&job.id, &format!("{}:outcome", job.id), true)
            .await
            .expect("delete reclaimable subscription");
        assert_eq!(
            deleted,
            HistoricalWorkDeletion {
                jobs: 1,
                subscription_ranges: 1,
                consumers: 1,
                delivery_records: 3,
                delivery_streams: 1,
                ..HistoricalWorkDeletion::default()
            }
        );
        assert!(store.job(&job.id).await.expect("job lookup").is_none());
        assert!(
            store
                .historical_work_identity(&job.id)
                .await
                .expect("request identity lookup")
                .is_none()
        );
        assert!(
            store
                .delivery_stream(&history_stream)
                .await
                .expect("history stream lookup")
                .is_none()
        );
        assert!(
            store
                .delivery_stream(&live_stream)
                .await
                .expect("live stream lookup")
                .is_some()
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn completion_identifies_preexisting_coverage_noop() {
        let (_directory, store) = store().await;
        let processor = FixtureProcessor::block_output();
        let covered = frame(1, BlockHash::ZERO);
        let delta = processor.map(&covered).await.expect("map covered block");
        store
            .apply(&processor, cursor(&processor, &covered, 1), &delta, &[])
            .await
            .expect("cover block");
        let stream_id = store
            .create_backfill_delivery_stream(&processor.descriptor, "covered-noop")
            .await
            .expect("history stream")
            .stream_id;
        store
            .create_consumer_in_stream(
                &processor.descriptor,
                &stream_id,
                "destination",
                ConsumerRole::Required,
                ConsumerStartPosition::EarliestRetained,
                Duration::from_mins(1),
            )
            .await
            .expect("consumer");
        let job = JobRecord {
            id: "covered-noop".to_owned(),
            kind: "backfill_subscription_job".to_owned(),
            state: JobState::Queued,
            payload: b"covered-noop".to_vec(),
            checkpoint: None,
            attempts: 0,
            updated_at_unix_ms: 1,
        };
        store
            .create_backfill_subscription_job(
                &BackfillSubscriptionRecord {
                    subscription_id: job.id.clone(),
                    job_id: job.id.clone(),
                    processor_instance: processor.descriptor.instance.to_string(),
                    history_stream_id: stream_id.clone(),
                    mode: BackfillSubscriptionMode::FillMissing,
                    publication_revision: 0,
                    state: BackfillSubscriptionState::Queued,
                    consumer_id: "destination".to_owned(),
                    ranges: vec![BlockRange::single(BlockNumber(1))],
                    range: BlockRange::single(BlockNumber(1)),
                    preexisting_coverage: vec![BlockRange::single(BlockNumber(1))],
                    captured_finalized_target: BlockNumber(1),
                    idempotency_key: "covered-noop".to_owned(),
                    effective_block_limit: 1,
                    effective_byte_limit: 1_024,
                    resume_below_ratio_millionths: 750_000,
                    delivery_batch_limits: BackfillDeliveryBatchLimits::default(),
                    initial_sequence: 0,
                    completion_sequence: None,
                    processed_work_blocks: 0,
                },
                &job,
                BlockHash::new([2; 32]),
            )
            .await
            .expect("subscription");
        store
            .append_backfill_completion(
                &processor.descriptor,
                &stream_id,
                ChainId(1),
                BlockNumber(1),
            )
            .await
            .expect("completion");
        let records = store
            .consumer_changes_in_stream(
                &processor.descriptor,
                &stream_id,
                ChainId(1),
                "destination",
                10,
            )
            .await
            .expect("completion records");
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].change.kind, "system.backfill_progress");
        assert_eq!(
            backfill_progress_blocks(&records[0].change.payload).expect("zero-work progress"),
            0
        );
        assert_eq!(records[1].change.kind, "system.backfill_complete");
        let metadata = decode_backfill_completion_metadata(&records[1].change.payload)
            .expect("completion metadata");
        assert_eq!(
            metadata.disposition,
            BackfillCompletionDisposition::AlreadyCoveredNoop
        );
        assert_eq!(metadata.requested_blocks, 1);
        assert_eq!(metadata.covered_before_request_blocks, 1);
        assert_eq!(
            metadata.covered_before_request_ranges,
            vec![BlockRange::single(BlockNumber(1))]
        );
        assert_eq!(metadata.republished_blocks, 0);
    }

    #[tokio::test]
    async fn deleting_materialization_metadata_retains_processor_output() {
        let (_directory, store) = store().await;
        let processor = FixtureProcessor::new();
        let first = frame(1, BlockHash::ZERO);
        let delta = processor.map(&first).await.expect("map");
        store
            .apply(&processor, cursor(&processor, &first, 1), &delta, &[])
            .await
            .expect("apply materialized output");
        let job = JobRecord {
            id: "retained-materialization".to_owned(),
            kind: "materialization_job".to_owned(),
            state: JobState::Completed,
            payload: b"fixture".to_vec(),
            checkpoint: None,
            attempts: 1,
            updated_at_unix_ms: 1,
        };
        let request_identity = BlockHash::new([7; 32]);
        store
            .create_historical_job(&job, request_identity)
            .await
            .expect("create materialization job");
        assert_eq!(
            store
                .create_historical_job(&job, request_identity)
                .await
                .expect("idempotent materialization job"),
            job
        );
        assert!(matches!(
            store
                .create_historical_job(&job, BlockHash::new([8; 32]))
                .await,
            Err(StoreError::InvalidConfig(message))
                if message.contains("idempotency key")
        ));
        store
            .save_job(&JobRecord {
                id: format!("{}:outcome", job.id),
                kind: "historical_backfill_outcome".to_owned(),
                state: JobState::Completed,
                payload: b"fixture".to_vec(),
                checkpoint: None,
                attempts: 1,
                updated_at_unix_ms: 1,
            })
            .await
            .expect("save outcome");

        let deleted = store
            .delete_terminal_historical_work(&job.id, &format!("{}:outcome", job.id), false)
            .await
            .expect("delete materialization metadata");
        assert_eq!(deleted.jobs, 2);
        assert_eq!(deleted.delivery_streams, 0);
        assert!(
            store
                .historical_work_identity(&job.id)
                .await
                .expect("deleted request identity")
                .is_none()
        );
        assert_eq!(
            store
                .entity(&processor.descriptor, "state", b"counter")
                .await
                .expect("retained entity"),
            Some(1_u64.to_be_bytes().to_vec())
        );
    }

    #[tokio::test]
    async fn terminal_job_states_cannot_be_overwritten_by_late_runtime_updates() {
        let (_directory, store) = store().await;
        for terminal in [JobState::Completed, JobState::Failed, JobState::Cancelled] {
            let mut job = JobRecord {
                id: format!("terminal-{terminal:?}"),
                kind: "materialization_job".to_owned(),
                state: terminal,
                payload: b"terminal".to_vec(),
                checkpoint: Some(b"terminal-checkpoint".to_vec()),
                attempts: 1,
                updated_at_unix_ms: 1,
            };
            store.save_job(&job).await.expect("save terminal state");
            job.state = JobState::Running;
            job.checkpoint = Some(b"late-runtime-update".to_vec());
            job.updated_at_unix_ms = 2;
            store.save_job(&job).await.expect("late runtime update");
            assert_eq!(
                store
                    .job(&job.id)
                    .await
                    .expect("job")
                    .expect("durable job")
                    .state,
                terminal
            );
        }
    }

    #[tokio::test]
    async fn explicit_start_rejects_a_pruned_cursor() {
        let (_directory, store) = store().await;
        let processor = FixtureProcessor::new();
        let first = frame(1, BlockHash::ZERO);
        let second = frame(2, first.block.hash);
        for (sequence, current) in [(1, &first), (2, &second)] {
            let delta = processor.map(current).await.expect("map");
            store
                .apply(
                    &processor,
                    cursor(&processor, current, sequence),
                    &delta,
                    &[],
                )
                .await
                .expect("apply");
        }
        let outcome = store
            .prune_changes_before(&processor.descriptor, 999)
            .await
            .expect("prune");
        assert_eq!(outcome.deleted, 1);
        assert!(matches!(
            store
                .create_consumer(
                    &processor.descriptor,
                    "too-late",
                    ConsumerRole::Required,
                    ConsumerStartPosition::After(0),
                    Duration::from_mins(1),
                )
                .await,
            Err(StoreError::ConsumerResetRequired {
                earliest_available: Some(2),
                latest_available: Some(2)
            })
        ));
    }

    #[tokio::test]
    async fn backfill_range_sets_are_normalized_and_part_of_idempotency() {
        let (_directory, store) = store().await;
        let mut processor = FixtureProcessor::block_output();
        processor.descriptor.delivery_ordering = DeliveryOrdering::BlockVersionedIdempotent;
        store
            .register_processor(&processor.descriptor)
            .await
            .expect("register split processor");
        let history_stream = store
            .create_backfill_delivery_stream(&processor.descriptor, "range-set")
            .await
            .expect("history stream")
            .stream_id;
        store
            .create_consumer_in_stream(
                &processor.descriptor,
                &history_stream,
                "destination",
                ConsumerRole::Required,
                ConsumerStartPosition::EarliestRetained,
                Duration::from_mins(1),
            )
            .await
            .expect("consumer");
        let job = JobRecord {
            id: "range-set".to_owned(),
            kind: "backfill_subscription_job".to_owned(),
            state: JobState::Queued,
            payload: b"range-set-job".to_vec(),
            checkpoint: None,
            attempts: 0,
            updated_at_unix_ms: 1,
        };
        let first = BlockRange::new(BlockNumber(1), BlockNumber(2)).expect("first");
        let adjacent = BlockRange::new(BlockNumber(3), BlockNumber(3)).expect("adjacent");
        let last = BlockRange::new(BlockNumber(5), BlockNumber(5)).expect("last");
        let mut subscription = BackfillSubscriptionRecord {
            subscription_id: job.id.clone(),
            job_id: job.id.clone(),
            processor_instance: processor.descriptor.instance.to_string(),
            history_stream_id: history_stream,
            mode: BackfillSubscriptionMode::FillMissing,
            publication_revision: 0,
            state: BackfillSubscriptionState::Queued,
            consumer_id: "destination".to_owned(),
            ranges: vec![last, adjacent, first],
            range: BlockRange::new(first.start(), last.end()).expect("bounding"),
            preexisting_coverage: Vec::new(),
            captured_finalized_target: last.end(),
            idempotency_key: "range-set-idempotency".to_owned(),
            effective_block_limit: 4,
            effective_byte_limit: 1_024,
            resume_below_ratio_millionths: 750_000,
            delivery_batch_limits: BackfillDeliveryBatchLimits::default(),
            initial_sequence: 0,
            completion_sequence: None,
            processed_work_blocks: 0,
        };
        store
            .create_backfill_subscription_job(&subscription, &job, BlockHash::new([3; 32]))
            .await
            .expect("create range set");
        let stored = store
            .backfill_subscription_for_job(&job.id)
            .await
            .expect("read subscription")
            .expect("subscription exists");
        assert_eq!(
            stored.ranges,
            vec![
                BlockRange::new(BlockNumber(1), BlockNumber(3)).expect("coalesced"),
                last
            ]
        );
        store
            .create_backfill_subscription_job(&subscription, &job, BlockHash::new([3; 32]))
            .await
            .expect("idempotent retry");
        subscription.captured_finalized_target = BlockNumber(10);
        store
            .create_backfill_subscription_job(&subscription, &job, BlockHash::new([3; 32]))
            .await
            .expect("idempotent retry keeps its original captured target");
        subscription.ranges = vec![first, last];
        assert!(matches!(
            store
                .create_backfill_subscription_job(&subscription, &job, BlockHash::new([4; 32]))
                .await,
            Err(StoreError::InvalidConfig(message)) if message.contains("idempotency key")
        ));
    }

    #[tokio::test]
    async fn delivery_pruner_applies_finality_age_and_bounded_batch_limits() {
        let (_directory, store) = store().await;
        let mut processor = FixtureProcessor::new();
        processor
            .descriptor
            .lifecycle
            .delivery
            .pruning
            .minimum_batch_blocks = 1;
        processor
            .descriptor
            .lifecycle
            .delivery
            .pruning
            .minimum_batch_changes = 1;
        processor
            .descriptor
            .lifecycle
            .delivery
            .pruning
            .maximum_delete_changes = 1;
        processor
            .descriptor
            .lifecycle
            .delivery
            .pruning
            .retain_finalized_blocks = 1;
        processor.descriptor.lifecycle.delivery.max_age_seconds = 1;
        let mut parent = BlockHash::ZERO;
        for number in 1..=3 {
            let current = frame(number, parent);
            parent = current.block.hash;
            let delta = processor.map(&current).await.expect("map");
            store
                .apply(
                    &processor,
                    cursor(&processor, &current, number),
                    &delta,
                    &[],
                )
                .await
                .expect("apply");
        }
        sqlx::query("UPDATE change_log SET created_at_unix_ms = 0")
            .execute(&store.inner.pool)
            .await
            .expect("age fixture changes");

        let first = store
            .prune_delivery_changes(&processor.descriptor, BlockNumber(3))
            .await
            .expect("prune first bounded batch");
        assert_eq!(first.deleted, 1);
        assert_eq!(
            store
                .change_bounds(&processor.descriptor)
                .await
                .expect("bounds"),
            Some(ChangeBounds {
                earliest: 2,
                latest: 3
            })
        );
        let second = store
            .prune_delivery_changes(&processor.descriptor, BlockNumber(3))
            .await
            .expect("prune second bounded batch");
        assert_eq!(second.deleted, 1);
        assert_eq!(
            store
                .change_bounds(&processor.descriptor)
                .await
                .expect("bounds"),
            Some(ChangeBounds {
                earliest: 3,
                latest: 3
            })
        );
    }

    #[tokio::test]
    async fn delivery_limit_pauses_and_auto_resumes_below_low_water() {
        let (_directory, store) = store().await;
        let mut processor = FixtureProcessor::new();
        processor.descriptor.lifecycle.delivery.max_bytes = 30;
        processor
            .descriptor
            .lifecycle
            .delivery
            .pruning
            .minimum_batch_blocks = 1;
        processor
            .descriptor
            .lifecycle
            .delivery
            .pruning
            .minimum_batch_changes = 1;
        processor
            .descriptor
            .lifecycle
            .delivery
            .pruning
            .retain_finalized_blocks = 1;
        let mut parent = BlockHash::ZERO;
        for number in 1..=2 {
            let current = frame(number, parent);
            parent = current.block.hash;
            let delta = processor.map(&current).await.expect("map");
            store
                .apply(
                    &processor,
                    cursor(&processor, &current, number),
                    &delta,
                    &[],
                )
                .await
                .expect("apply below hard limit");
        }
        let third = frame(3, parent);
        let third_delta = processor.map(&third).await.expect("map third");
        assert!(matches!(
            store
                .apply(&processor, cursor(&processor, &third, 3), &third_delta, &[])
                .await,
            Err(StoreError::DeliveryLimit {
                action: DeliveryLimitAction::Pause,
                ..
            })
        ));
        assert_eq!(
            store
                .processor_runtime_state(&processor.descriptor)
                .await
                .expect("paused state")
                .state,
            ProcessorRunState::Paused
        );

        let pruned = store
            .prune_delivery_changes(&processor.descriptor, BlockNumber(3))
            .await
            .expect("pressure prune");
        assert_eq!(pruned.deleted, 1);
        assert_eq!(
            store
                .processor_runtime_state(&processor.descriptor)
                .await
                .expect("resumed state")
                .state,
            ProcessorRunState::Running
        );
        store
            .apply(&processor, cursor(&processor, &third, 3), &third_delta, &[])
            .await
            .expect("apply after automatic resume");
    }

    #[tokio::test]
    async fn node_wide_delivery_budget_counts_all_streams() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteStore::open(
            StoreConfig::new(directory.path().join("node.sqlite")).with_delivery_budget(
                DeliveryStorageBudget {
                    maximum_retained_bytes: 20,
                    maximum_history_retained_bytes: 20,
                },
            ),
        )
        .await
        .expect("open bounded store");
        let first = FixtureProcessor::new();
        let mut second = FixtureProcessor::new();
        second.descriptor.instance =
            ProcessorInstanceId::new("fixture-second").expect("second instance");
        let first_frame = frame(1, BlockHash::ZERO);
        let first_delta = first.map(&first_frame).await.expect("map first");
        store
            .apply(&first, cursor(&first, &first_frame, 1), &first_delta, &[])
            .await
            .expect("first stream fits global budget");
        let second_frame = frame(1, BlockHash::ZERO);
        let second_delta = second.map(&second_frame).await.expect("map second");
        assert!(matches!(
            store
                .apply(
                    &second,
                    cursor(&second, &second_frame, 1),
                    &second_delta,
                    &[],
                )
                .await,
            Err(StoreError::DeliveryLimit {
                scope: "node_total_retained",
                limit_bytes: 20,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn full_output_stops_before_crossing_the_physical_store_budget() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteStore::open(
            StoreConfig::new(directory.path().join("node.sqlite"))
                .with_storage_budget(StoreStorageBudget {
                    maximum_physical_bytes: 1,
                })
                .with_delivery_budget(DeliveryStorageBudget {
                    maximum_retained_bytes: 1,
                    maximum_history_retained_bytes: 1,
                }),
        )
        .await
        .expect("open physically bounded store");
        let mut processor = FixtureProcessor::block_output();
        processor.descriptor.lifecycle.output.mode = OutputPolicyMode::Full;
        processor.descriptor.lifecycle.output.window = None;
        processor.descriptor.lifecycle.delivery.mode = DeliveryPolicyMode::None;
        processor.descriptor.lifecycle.delivery.consumers.clear();
        let current = frame(1, BlockHash::ZERO);
        let delta = processor.map(&current).await.expect("map");
        assert!(matches!(
            store
                .apply(&processor, cursor(&processor, &current, 1), &delta, &[])
                .await,
            Err(StoreError::PhysicalStorageLimit { limit_bytes: 1, .. })
        ));
        assert!(
            store
                .processor_cursor(&processor.descriptor)
                .await
                .expect("cursor query")
                .is_none()
        );
        assert_eq!(
            store
                .processor_stats(&processor.descriptor)
                .await
                .expect("processor stats")
                .entities,
            0
        );
    }

    #[tokio::test]
    async fn recent_frames_are_checksummed_and_pruned_with_a_safety_window() {
        let (_directory, store) = store().await;
        let mut parent = BlockHash::ZERO;
        for number in 1..=5 {
            let current = with_transaction(
                frame(number, parent),
                TransactionHash::new([u8::try_from(number + 10).expect("fixture"); 32]),
            );
            parent = current.block.hash;
            store
                .store_recent_frame(&current)
                .await
                .expect("store recent");
        }
        let stats = store.recent_stats(ChainId(1)).await.expect("recent stats");
        assert_eq!(stats.frames, 5);
        assert_eq!(stats.earliest_block, Some(BlockNumber(1)));
        assert_eq!(stats.latest_block, Some(BlockNumber(5)));
        assert_eq!(
            store
                .recent_frame(ChainId(1), BlockNumber(3))
                .await
                .expect("read")
                .expect("frame")
                .block
                .number,
            BlockNumber(3)
        );
        let outcome = store
            .prune_recent_frames(ChainId(1), BlockNumber(3), 2, 1, 1)
            .await
            .expect("prune");
        assert_eq!(outcome.deleted_frames, 3);
        assert_eq!(outcome.retained_frames, 2);
        assert!(outcome.hard_limit_exceeded);
        assert!(
            store
                .recent_frame(ChainId(1), BlockNumber(1))
                .await
                .expect("read old")
                .is_none()
        );
        assert!(
            store
                .recent_frame(ChainId(1), BlockNumber(5))
                .await
                .expect("read retained")
                .is_some()
        );
        assert!(
            store
                .recent_transaction_location(ChainId(1), TransactionHash::new([11; 32]))
                .await
                .expect("pruned transaction location")
                .is_none()
        );
        assert!(
            store
                .recent_transaction_location(ChainId(1), TransactionHash::new([15; 32]))
                .await
                .expect("retained transaction location")
                .is_some()
        );
    }

    #[tokio::test]
    async fn recent_reorg_moves_canonical_pointers_and_retains_fork_material() {
        let (_directory, store) = store().await;
        let first = frame(1, BlockHash::ZERO);
        let second = frame(2, first.block.hash);
        let old_transaction = TransactionHash::new([0x13; 32]);
        let replacement_transaction = TransactionHash::new([0x23; 32]);
        let third = with_transaction(frame(3, second.block.hash), old_transaction);
        for current in [&first, &second, &third] {
            store
                .store_recent_frame(current)
                .await
                .expect("store recent");
        }
        assert_eq!(
            store
                .recent_transaction_location(ChainId(1), old_transaction)
                .await
                .expect("transaction location"),
            Some(RecentTransactionLocation {
                block_number: third.block.number,
                block_hash: third.block.hash,
                transaction_index: 0,
            })
        );
        let mut replacement =
            with_transaction(frame(3, second.block.hash), replacement_transaction);
        replacement.block.hash = BlockHash::new([0x33; 32]);
        let outcome = store
            .reorg_recent_frames(
                ChainId(1),
                &[third.block],
                std::slice::from_ref(&replacement),
            )
            .await
            .expect("reorg");
        assert_eq!(outcome.new_tip, replacement.block);
        assert_eq!(
            store
                .recent_frame(ChainId(1), BlockNumber(3))
                .await
                .expect("recent")
                .expect("replacement")
                .block
                .hash,
            replacement.block.hash
        );
        assert_eq!(
            store.recent_stats(ChainId(1)).await.expect("stats").frames,
            4
        );
        assert!(
            store
                .recent_transaction_location(ChainId(1), old_transaction)
                .await
                .expect("reverted transaction location")
                .is_none()
        );
        assert_eq!(
            store
                .recent_transaction_location(ChainId(1), replacement_transaction)
                .await
                .expect("replacement transaction location"),
            Some(RecentTransactionLocation {
                block_number: replacement.block.number,
                block_hash: replacement.block.hash,
                transaction_index: 0,
            })
        );
        assert_eq!(
            store
                .canonical_block_by_hash(ChainId(1), replacement.block.hash)
                .await
                .expect("canonical lookup"),
            Some(replacement.block)
        );
        store
            .mark_recent_finalized(ChainId(1), second.block.number, second.block.hash)
            .await
            .expect("finalize");
        assert!(
            store
                .reorg_recent_frames(ChainId(1), &[replacement.block, second.block], &[])
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn recent_reorg_reuses_retained_fork_with_refreshed_evidence() {
        let (_directory, store) = store().await;
        let first = frame(1, BlockHash::ZERO);
        let second = frame(2, first.block.hash);
        let original = with_observation(frame(3, second.block.hash), 1);
        for current in [&first, &second, &original] {
            store
                .store_recent_frame(current)
                .await
                .expect("store original branch");
        }

        let mut replacement = frame(3, second.block.hash);
        replacement.block.hash = BlockHash::new([0x33; 32]);
        store
            .reorg_recent_frames(
                ChainId(1),
                &[original.block],
                std::slice::from_ref(&replacement),
            )
            .await
            .expect("switch to replacement branch");

        let mut replay = original.clone();
        replay.provenance[0].observed_at_unix_ms = 2;
        let outcome = store
            .reorg_recent_frames(
                ChainId(1),
                &[replacement.block],
                std::slice::from_ref(&replay),
            )
            .await
            .expect("reuse retained original branch");
        assert_eq!(outcome.new_tip, original.block);
        assert_eq!(
            store
                .recent_frame(ChainId(1), original.block.number)
                .await
                .expect("read canonical replay")
                .expect("canonical frame")
                .block,
            original.block
        );
    }

    #[tokio::test]
    async fn canonical_recent_bounds_exclude_retained_longer_forks() {
        let (_directory, store) = store().await;
        let first = frame(1, BlockHash::ZERO);
        let second = frame(2, first.block.hash);
        let third = frame(3, second.block.hash);
        for current in [&first, &second, &third] {
            store
                .store_recent_frame(current)
                .await
                .expect("store recent");
        }
        store
            .reorg_recent_frames(ChainId(1), &[third.block, second.block], &[])
            .await
            .expect("shorter replacement branch");
        assert_eq!(
            store
                .recent_canonical_bounds(ChainId(1))
                .await
                .expect("canonical bounds"),
            Some(BlockRange::single(first.block.number))
        );
        assert_eq!(
            store
                .recent_stats(ChainId(1))
                .await
                .expect("raw stats")
                .latest_block,
            Some(third.block.number)
        );
    }

    #[tokio::test]
    async fn finalized_canonical_head_is_chain_scoped_and_monotonic() {
        let (_directory, store) = store().await;
        let first = frame(1, BlockHash::ZERO);
        let second = frame(2, first.block.hash);
        store
            .store_canonical_anchor(ChainId(1), first.block, Finality::Finalized)
            .await
            .expect("finalized anchor");
        store
            .store_canonical_anchor(ChainId(1), second.block, Finality::Optimistic)
            .await
            .expect("optimistic head");
        assert_eq!(
            store
                .finalized_canonical_head(ChainId(1))
                .await
                .expect("finalized head"),
            Some(first.block)
        );
        assert_eq!(
            store
                .finalized_canonical_head(ChainId(2))
                .await
                .expect("other chain"),
            None
        );
        store
            .mark_recent_finalized(ChainId(1), second.block.number, second.block.hash)
            .await
            .expect("promote head");
        assert_eq!(
            store
                .finalized_canonical_head(ChainId(1))
                .await
                .expect("promoted head"),
            Some(second.block)
        );
    }

    #[tokio::test]
    async fn block_local_cursor_tracks_highest_disjoint_coverage() {
        let (_directory, store) = store().await;
        let mut processor = FixtureProcessor::new();
        processor.descriptor.mode = ReductionMode::BlockLocal;
        let high = frame(10, BlockHash::new([9; 32]));
        let high_delta = processor.map(&high).await.expect("map high");
        store
            .apply(&processor, cursor(&processor, &high, 1), &high_delta, &[])
            .await
            .expect("apply high");
        let low = frame(1, BlockHash::ZERO);
        let low_delta = processor.map(&low).await.expect("map low");
        let outcome = store
            .apply(&processor, cursor(&processor, &low, 2), &low_delta, &[])
            .await
            .expect("apply low");
        assert!(matches!(
            outcome,
            ApplyOutcome::Applied {
                processor_cursor: ProcessorCursor {
                    block_number: BlockNumber(10),
                    ..
                },
                ..
            }
        ));
        store
            .undo(
                &processor.descriptor,
                ChainId(1),
                high.block.number,
                high.block.hash,
                &[],
            )
            .await
            .expect("undo high");
        assert_eq!(
            store
                .processor_cursor(&processor.descriptor)
                .await
                .expect("cursor")
                .expect("low cursor")
                .block_number,
            BlockNumber(1)
        );
    }

    #[tokio::test]
    async fn finalized_undo_is_rejected() {
        let (_directory, store) = store().await;
        let processor = FixtureProcessor::new();
        let frame = frame(1, BlockHash::ZERO);
        let delta = processor.map(&frame).await.expect("map");
        store
            .apply(&processor, cursor(&processor, &frame, 1), &delta, &[])
            .await
            .expect("apply");
        assert_eq!(
            store
                .mark_finalized(&processor.descriptor, BlockNumber(1))
                .await
                .expect("finalize"),
            1
        );
        assert!(matches!(
            store
                .undo(
                    &processor.descriptor,
                    ChainId(1),
                    frame.block.number,
                    frame.block.hash,
                    &[]
                )
                .await,
            Err(StoreError::FinalizedUndo(BlockNumber(1)))
        ));
    }

    #[test]
    fn prefix_bound_handles_carry_and_unbounded_max() {
        assert_eq!(prefix_upper_bound(&[0x01, 0xff]), Some(vec![0x02]));
        assert_eq!(prefix_upper_bound(&[0xff]), None);
        assert_eq!(prefix_upper_bound(&[]), None);
    }
}
