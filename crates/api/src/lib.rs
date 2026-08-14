//! Versioned aggregate HTTP query and resumable SSE API.

mod extension;
mod extensions;

pub use extension::{
    QueryContext, QueryExtension, QueryExtensionRegistration, QueryExtensionSummary,
};
pub use extensions::{BlobsQueryExtension, Erc20QueryExtension, UniswapQueryExtension};

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    convert::Infallible,
    future::Future,
    io::Write as _,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use alloy_primitives::U256;
use async_trait::async_trait;
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{Path, Query, Request, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    middleware::{self, Next},
    response::{
        Html, IntoResponse, Response, Sse,
        sse::{Event, KeepAlive},
    },
    routing::get,
};
use futures::{Stream, StreamExt as _, stream};
use leani_primitives::{
    Address, BlockHash, BlockNumber, BlockRange, ChangeCursor, Finality, Quantity,
};
use leani_processor_api::{ChangeOperation, OutputPolicyMode, Processor};
use leani_processor_api::{ProcessorDescriptor, StartPoint};
use leani_processor_blobs::{
    BlobFork, BlobTransactionEntity, BlobsBlockEntity, BlobsDelta, BlobsProcessor,
};
use leani_processor_erc20::{BALANCE_CHANGE_KIND, TokenBalanceEntity};
use leani_processor_uniswap::PoolPriceEntity;
use leani_source_api::{NetworkTelemetry, NetworkTelemetrySnapshot, coverage_gaps};
use leani_store_history::{
    RawHistoryIndexPolicy, RawHistoryJob, RawHistoryJobDeletion, RawHistoryMaterialProfile,
    RawHistoryRetention, RawHistorySegmentPolicy, VerificationClass,
};
use leani_store_sqlite::{
    ArtifactOwnerKind, ArtifactPruneOutcome, ArtifactReplayOutcome, ChangeBounds, ChangeDirection,
    ChangeRecord, ConsumerRole, ConsumerStartPosition, DeliveryStreamKind, DurableConsumer,
    OutputBounds, OutputQuery, PortableSavepoint, ProcessorArtifactStats, ProcessorRunState,
    QuerySnapshotEntity, RecoveryCheckpoint, SqliteStore, StoreError, default_delivery_stream_id,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;

const API_VERSION: &str = "1";
const MAX_PAGE_SIZE: usize = 1_000;
const CONSUMER_CREDENTIAL_HEADER: &str = "x-leani-consumer-credential";
const CONSUMER_SESSION_HEADER: &str = "x-leani-consumer-session";
static REQUEST_SEQUENCE: AtomicU64 = AtomicU64::new(1);

/// Failure from [`commit_then_ack`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommitThenAckError<CommitError, AcknowledgeError> {
    DestinationCommit(CommitError),
    Acknowledgement(AcknowledgeError),
}

/// Commit destination work before constructing/sending an acknowledgement.
///
/// SDKs can wrap their database transaction future and acknowledgement
/// request with this helper. If the destination commit fails, the
/// acknowledgement closure is never invoked and the node safely replays.
///
/// # Errors
///
/// Returns [`CommitThenAckError::DestinationCommit`] when the destination
/// transaction fails, or [`CommitThenAckError::Acknowledgement`] when the
/// destination committed but the subsequent acknowledgement failed.
pub async fn commit_then_ack<
    CommitFuture,
    Acknowledge,
    AcknowledgeFuture,
    Output,
    CommitError,
    AcknowledgeError,
>(
    destination_commit: CommitFuture,
    acknowledge: Acknowledge,
) -> Result<Output, CommitThenAckError<CommitError, AcknowledgeError>>
where
    CommitFuture: Future<Output = Result<(), CommitError>>,
    Acknowledge: FnOnce() -> AcknowledgeFuture,
    AcknowledgeFuture: Future<Output = Result<Output, AcknowledgeError>>,
{
    destination_commit
        .await
        .map_err(CommitThenAckError::DestinationCommit)?;
    acknowledge()
        .await
        .map_err(CommitThenAckError::Acknowledgement)
}

/// Dynamically updated readiness inputs shared by the service supervisor and
/// HTTP handlers.
#[derive(Clone, Debug)]
pub struct ReadinessHandle {
    inner: Arc<ReadinessInner>,
}

#[derive(Debug)]
struct ReadinessInner {
    live_required: bool,
    finality_required: bool,
    live_ready: AtomicBool,
    finality_ready: AtomicBool,
}

impl ReadinessHandle {
    #[must_use]
    pub fn new(live_required: bool, finality_required: bool) -> Self {
        Self {
            inner: Arc::new(ReadinessInner {
                live_required,
                finality_required,
                live_ready: AtomicBool::new(false),
                finality_ready: AtomicBool::new(false),
            }),
        }
    }

    pub fn set_live_ready(&self, ready: bool) {
        self.inner.live_ready.store(ready, Ordering::Release);
    }

    pub fn set_finality_ready(&self, ready: bool) {
        self.inner.finality_ready.store(ready, Ordering::Release);
    }

    fn snapshot(&self) -> ReadinessSnapshot {
        let live_ready = self.inner.live_ready.load(Ordering::Acquire);
        let finality_ready = self.inner.finality_ready.load(Ordering::Acquire);
        ReadinessSnapshot {
            live_required: self.inner.live_required,
            live_ready,
            finality_required: self.inner.finality_required,
            finality_ready,
            ready: (!self.inner.live_required || live_ready)
                && (!self.inner.finality_required || finality_ready),
        }
    }
}

impl Default for ReadinessHandle {
    fn default() -> Self {
        Self::new(false, false)
    }
}

/// Query and stream limits.
#[derive(Clone)]
pub struct ApiConfig {
    pub chain_id: leani_primitives::ChainId,
    pub default_page_size: usize,
    pub max_page_size: usize,
    pub stream_batch_size: usize,
    pub history_batch_limits: DeliveryBatchLimits,
    pub live_batch_limits: DeliveryBatchLimits,
    pub stream_poll_interval: Duration,
    pub heartbeat_interval: Duration,
    pub query_snapshot_ttl: Duration,
    pub query_snapshot_max_rows: u64,
    pub query_snapshot_max_bytes: u64,
    /// Optional bearer token. Debug output always redacts this value.
    pub bearer_token: Option<Arc<str>>,
    pub readiness: ReadinessHandle,
    pub network_telemetry: NetworkTelemetry,
    /// Processor instances whose historical coverage is application-requested
    /// rather than an automatic start-to-head obligation.
    pub on_demand_processors: BTreeSet<String>,
    /// Processor instances whose historical ranges are owned by application
    /// subscriptions rather than node materialization jobs.
    pub application_subscription_processors: BTreeSet<String>,
    /// Optional generic processor-history control plane supplied by the node
    /// binary. Normal query/RPC service remains available when omitted.
    pub backfill_control: Option<Arc<dyn BackfillControl>>,
    /// Optional independent raw-history control plane supplied by the node.
    pub raw_history_control: Option<Arc<dyn RawHistoryControl>>,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            chain_id: leani_primitives::ChainId(1),
            default_page_size: 100,
            max_page_size: MAX_PAGE_SIZE,
            stream_batch_size: 100,
            history_batch_limits: DeliveryBatchLimits::history_default(),
            live_batch_limits: DeliveryBatchLimits::live_default(),
            stream_poll_interval: Duration::from_millis(500),
            heartbeat_interval: Duration::from_secs(15),
            query_snapshot_ttl: Duration::from_mins(5),
            query_snapshot_max_rows: 100_000,
            query_snapshot_max_bytes: 64 * 1024 * 1024,
            bearer_token: None,
            readiness: ReadinessHandle::default(),
            network_telemetry: NetworkTelemetry::default(),
            on_demand_processors: BTreeSet::new(),
            application_subscription_processors: BTreeSet::new(),
            backfill_control: None,
            raw_history_control: None,
        }
    }
}

impl std::fmt::Debug for ApiConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ApiConfig")
            .field("chain_id", &self.chain_id)
            .field("default_page_size", &self.default_page_size)
            .field("max_page_size", &self.max_page_size)
            .field("stream_batch_size", &self.stream_batch_size)
            .field("history_batch_limits", &self.history_batch_limits)
            .field("live_batch_limits", &self.live_batch_limits)
            .field("stream_poll_interval", &self.stream_poll_interval)
            .field("heartbeat_interval", &self.heartbeat_interval)
            .field("query_snapshot_ttl", &self.query_snapshot_ttl)
            .field("query_snapshot_max_rows", &self.query_snapshot_max_rows)
            .field("query_snapshot_max_bytes", &self.query_snapshot_max_bytes)
            .field(
                "bearer_token",
                &self.bearer_token.as_ref().map(|_| "[REDACTED]"),
            )
            .field("readiness", &self.readiness)
            .field("network_telemetry", &self.network_telemetry)
            .field("on_demand_processors", &self.on_demand_processors)
            .field(
                "application_subscription_processors",
                &self.application_subscription_processors,
            )
            .field(
                "backfill_control",
                &self.backfill_control.as_ref().map(|_| "[AVAILABLE]"),
            )
            .field(
                "raw_history_control",
                &self.raw_history_control.as_ref().map(|_| "[AVAILABLE]"),
            )
            .finish()
    }
}

impl ApiConfig {
    fn validate(&self) -> Result<(), ApiError> {
        if self.chain_id.0 == 0
            || self.default_page_size == 0
            || self.default_page_size > self.max_page_size
            || self.max_page_size > 10_000
            || self.stream_batch_size == 0
            || self.stream_batch_size > 10_000
            || !self.history_batch_limits.is_valid()
            || !self.live_batch_limits.is_valid()
            || self.stream_poll_interval.is_zero()
            || self.heartbeat_interval.is_zero()
            || self.query_snapshot_ttl.is_zero()
            || self.query_snapshot_max_rows == 0
            || self.query_snapshot_max_rows > 1_000_000
            || self.query_snapshot_max_bytes == 0
        {
            return Err(ApiError::invalid(
                "API limits, chain ID, and intervals must be bounded and non-zero",
            ));
        }
        Ok(())
    }
}

/// Independent encoded-size, event, work, and latency limits for one delivery
/// lane. Commit microbatch sizing does not affect these limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeliveryBatchLimits {
    pub target_encoded_bytes: u64,
    pub maximum_encoded_bytes: u64,
    pub maximum_events: u64,
    pub maximum_processed_blocks: u64,
    pub maximum_delay: Duration,
    pub maximum_buffered_batches: usize,
    pub maximum_buffered_bytes: u64,
    pub compression: DeliveryCompression,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryCompression {
    None,
    #[default]
    Gzip,
}

impl DeliveryBatchLimits {
    #[must_use]
    pub const fn history_default() -> Self {
        Self {
            target_encoded_bytes: 4 * 1024 * 1024,
            maximum_encoded_bytes: 16 * 1024 * 1024,
            maximum_events: 20_000,
            maximum_processed_blocks: 8_192,
            maximum_delay: Duration::from_millis(50),
            maximum_buffered_batches: 4,
            maximum_buffered_bytes: 64 * 1024 * 1024,
            compression: DeliveryCompression::Gzip,
        }
    }

    #[must_use]
    pub const fn live_default() -> Self {
        Self {
            target_encoded_bytes: 64 * 1024,
            maximum_encoded_bytes: 1024 * 1024,
            maximum_events: 1_000,
            maximum_processed_blocks: 8,
            maximum_delay: Duration::from_millis(10),
            maximum_buffered_batches: 2,
            maximum_buffered_bytes: 2 * 1024 * 1024,
            compression: DeliveryCompression::Gzip,
        }
    }

    const fn is_valid(self) -> bool {
        self.target_encoded_bytes > 0
            && self.target_encoded_bytes <= self.maximum_encoded_bytes
            && self.maximum_events > 0
            && self.maximum_processed_blocks > 0
            && !self.maximum_delay.is_zero()
            && self.maximum_buffered_batches > 0
            && self.maximum_buffered_bytes >= self.maximum_encoded_bytes
            && self.maximum_buffered_bytes <= 4_294_967_295
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BackfillExecutionMode {
    #[default]
    FillMissing,
    Recompute,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CreateBackfillRequest {
    pub processor: String,
    #[serde(default)]
    pub from_block: Option<u64>,
    #[serde(default)]
    pub to_block: Option<BackfillUpperBound>,
    #[serde(default)]
    pub ranges: Vec<CreateBackfillRangeRequest>,
    #[serde(default)]
    pub mode: BackfillExecutionMode,
    #[serde(default)]
    pub consumer: Option<CreateBackfillConsumerRequest>,
    #[serde(default)]
    pub limits: Option<CreateBackfillLimitsRequest>,
    #[serde(default)]
    pub batching: Option<CreateBackfillBatchingRequest>,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CreateMaterializationRequest {
    pub processor: String,
    #[serde(default)]
    pub from_block: Option<u64>,
    #[serde(default)]
    pub to_block: Option<BackfillUpperBound>,
    #[serde(default)]
    pub ranges: Vec<CreateBackfillRangeRequest>,
    #[serde(default)]
    pub mode: BackfillExecutionMode,
    pub idempotency_key: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CreateBackfillRangeRequest {
    pub from_block: u64,
    pub to_block: BackfillUpperBound,
}

/// Inclusive history upper bound resolved once when work is created.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackfillUpperBound {
    Number(u64),
    Finalized,
}

impl BackfillUpperBound {
    #[must_use]
    pub const fn resolve(self, finalized: u64) -> u64 {
        match self {
            Self::Number(number) => number,
            Self::Finalized => finalized,
        }
    }
}

impl From<u64> for BackfillUpperBound {
    fn from(value: u64) -> Self {
        Self::Number(value)
    }
}

impl<'de> Deserialize<'de> for BackfillUpperBound {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Wire {
            Number(u64),
            Sentinel(String),
        }

        match Wire::deserialize(deserializer)? {
            Wire::Number(number) => Ok(Self::Number(number)),
            Wire::Sentinel(value) if value == "finalized" => Ok(Self::Finalized),
            Wire::Sentinel(value) => Err(serde::de::Error::custom(format!(
                "unknown history upper bound {value:?}; expected a block number or \"finalized\""
            ))),
        }
    }
}

impl Serialize for BackfillUpperBound {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Number(number) => serializer.serialize_u64(*number),
            Self::Finalized => serializer.serialize_str("finalized"),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CreateBackfillConsumerRequest {
    pub id: String,
    pub role: ConsumerRole,
    pub lease_ttl_seconds: u64,
    #[serde(default)]
    pub credential: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CreateBackfillLimitsRequest {
    pub max_unacknowledged_blocks: u64,
    pub max_unacknowledged_bytes: u64,
    pub resume_below_ratio: f64,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CreateBackfillBatchingRequest {
    #[serde(default)]
    pub target_encoded_bytes: Option<u64>,
    #[serde(default)]
    pub maximum_encoded_bytes: Option<u64>,
    #[serde(default)]
    pub maximum_events: Option<u64>,
    #[serde(default)]
    pub maximum_processed_blocks: Option<u64>,
    #[serde(default)]
    pub maximum_delay_ms: Option<u64>,
    #[serde(default)]
    pub compression: Option<DeliveryCompression>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BackfillState {
    WaitingForConsumer,
    Queued,
    Running,
    Backpressured,
    StorageBackpressured,
    Draining,
    CompleteReclaimable,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoricalWorkOwner {
    Materialization,
    Subscription,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BackfillSourceReport {
    pub source_id: String,
    pub source_kind: String,
    pub attempts: u32,
    pub failures: u32,
    pub frames_mapped: u64,
    pub frames_committed: u64,
    pub duplicate_frames: u64,
    pub source_bytes: u64,
    pub physical_source_bytes: u64,
    pub reused_source_bytes: u64,
    pub coalesced_frames: u64,
    pub elapsed_milliseconds: u64,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BackfillReport {
    pub source_ids: Vec<String>,
    pub frames_mapped: u64,
    pub frames_committed: u64,
    pub duplicate_frames: u64,
    pub source_attempts: u32,
    pub source_bytes: u64,
    pub physical_source_bytes: u64,
    pub reused_source_bytes: u64,
    pub coalesced_frames: u64,
    pub acquisition_ids: Vec<u64>,
    pub elapsed_milliseconds: u64,
    pub sources: Vec<BackfillSourceReport>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BackfillStatus {
    pub id: String,
    pub owner: HistoricalWorkOwner,
    pub processor: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delivery_stream_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub publication_revision: Option<String>,
    pub from_block: u64,
    pub to_block: u64,
    pub ranges: Vec<BackfillRange>,
    pub requested_blocks: u64,
    pub processed_blocks: u64,
    pub remaining_blocks: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub captured_finalized_target: Option<u64>,
    pub mode: BackfillExecutionMode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batching: Option<EffectiveBackfillBatching>,
    pub state: BackfillState,
    pub attempts: u32,
    pub updated_at_unix_ms: u64,
    pub report: Option<BackfillReport>,
    pub last_error: Option<String>,
}

/// Exact effect of explicitly deleting terminal historical-work metadata.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoricalWorkDeletion {
    pub id: String,
    pub owner: HistoricalWorkOwner,
    pub removed_jobs: u64,
    pub removed_subscription_ranges: u64,
    pub removed_consumers: u64,
    pub removed_delivery_records: u64,
    pub removed_delivery_streams: u64,
    pub removed_coverage_intervals: u64,
    pub removed_coverage_segments: u64,
    pub removed_exact_coverage: u64,
    pub removed_applied_blocks: u64,
    pub removed_finalized_undo: u64,
    pub retained_processor_output: bool,
    pub retained_live_stream: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EffectiveBackfillBatching {
    pub target_encoded_bytes: u64,
    pub maximum_encoded_bytes: u64,
    pub maximum_events: u64,
    pub maximum_processed_blocks: u64,
    pub maximum_delay_ms: u64,
    pub maximum_buffered_batches: u64,
    pub maximum_buffered_bytes: u64,
    pub compression: DeliveryCompression,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BackfillRange {
    pub from_block: u64,
    pub to_block: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoricalMaterialMetrics {
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

#[derive(Clone, Debug, Error)]
pub enum BackfillControlError {
    #[error("invalid backfill request: {0}")]
    Invalid(String),
    #[error("backfill does not exist: {0}")]
    NotFound(String),
    #[error("backfill conflicts with existing state: {0}")]
    Conflict(String),
    #[error(
        "requested history through block {requested} exceeds the captured finalized head {finalized}"
    )]
    HistoryNotFinalized {
        requested: u64,
        finalized: u64,
        finalized_hash: BlockHash,
    },
    #[error("requested history starts at block {requested}, after finalized head {finalized}")]
    RangeAfterFinalizedHead {
        requested: u64,
        finalized: u64,
        finalized_hash: BlockHash,
    },
    #[error("recompute requires retained finalized coverage for the complete requested range")]
    RecomputeCoverageMissing { gaps: Vec<BackfillRange> },
    #[error("backfill control is temporarily unavailable: {0}")]
    Unavailable(String),
    #[error("backfill control failed: {0}")]
    Internal(String),
}

#[async_trait]
pub trait BackfillControl: std::fmt::Debug + Send + Sync {
    fn historical_material_metrics(&self) -> Option<HistoricalMaterialMetrics> {
        None
    }

    async fn create_historical_work(
        &self,
        request: CreateBackfillRequest,
        owner: HistoricalWorkOwner,
    ) -> Result<BackfillStatus, BackfillControlError>;

    async fn create_subscription(
        &self,
        request: CreateBackfillRequest,
    ) -> Result<BackfillStatus, BackfillControlError> {
        self.create_historical_work(request, HistoricalWorkOwner::Subscription)
            .await
    }

    async fn create_materialization(
        &self,
        request: CreateMaterializationRequest,
    ) -> Result<BackfillStatus, BackfillControlError> {
        self.create_historical_work(
            CreateBackfillRequest {
                processor: request.processor,
                from_block: request.from_block,
                to_block: request.to_block,
                ranges: request.ranges,
                mode: request.mode,
                consumer: None,
                limits: None,
                batching: None,
                idempotency_key: request.idempotency_key,
            },
            HistoricalWorkOwner::Materialization,
        )
        .await
    }

    async fn list(
        &self,
        owner: Option<HistoricalWorkOwner>,
    ) -> Result<Vec<BackfillStatus>, BackfillControlError>;

    async fn inspect(&self, id: &str) -> Result<BackfillStatus, BackfillControlError>;

    async fn cancel(&self, id: &str) -> Result<BackfillStatus, BackfillControlError>;

    async fn delete(&self, id: &str) -> Result<HistoricalWorkDeletion, BackfillControlError>;
}

/// Transport request for an independently retained raw-history job. Chain and
/// source-policy identity are injected by the node and cannot be spoofed by a
/// caller.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RawHistoryProfileName {
    #[default]
    ProcessorReuse,
    PostMergeExecutionRpc,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CreateRawHistoryJobRequest {
    pub idempotency_key: String,
    pub ranges: Vec<BackfillRange>,
    #[serde(default)]
    pub profile: RawHistoryProfileName,
    #[serde(default)]
    pub material: RawHistoryMaterialProfile,
    pub required_capabilities: leani_primitives::CapabilitySet,
    pub verification: VerificationClass,
    pub minimum_trust: leani_primitives::TrustModel,
    pub retention: RawHistoryRetention,
    pub segment: RawHistorySegmentPolicy,
    #[serde(default)]
    pub indexes: RawHistoryIndexPolicy,
}

#[derive(Clone, Debug, Error)]
pub enum RawHistoryControlError {
    #[error("invalid raw-history request: {0}")]
    Invalid(String),
    #[error("raw-history profile is incompatible with the request: {0}")]
    ProfileIncompatible(String),
    #[error("raw-history job does not exist: {0}")]
    NotFound(String),
    #[error("raw-history job conflicts with existing state: {0}")]
    Conflict(String),
    #[error("raw-history control is temporarily unavailable: {0}")]
    Unavailable(String),
    #[error("raw-history control failed: {0}")]
    Internal(String),
}

#[async_trait]
pub trait RawHistoryControl: std::fmt::Debug + Send + Sync {
    async fn create(
        &self,
        request: CreateRawHistoryJobRequest,
    ) -> Result<RawHistoryJob, RawHistoryControlError>;

    async fn list(&self) -> Result<Vec<RawHistoryJob>, RawHistoryControlError>;

    async fn inspect(&self, id: &str) -> Result<RawHistoryJob, RawHistoryControlError>;

    async fn cancel(&self, id: &str) -> Result<RawHistoryJob, RawHistoryControlError>;

    async fn delete(&self, id: &str) -> Result<RawHistoryJobDeletion, RawHistoryControlError>;
}

#[derive(Clone)]
struct ApiState {
    store: SqliteStore,
    primary: Arc<dyn Processor>,
    processors: Arc<BTreeMap<String, Arc<dyn Processor>>>,
    query_extensions: Arc<Vec<MountedQueryExtension>>,
    capabilities: Arc<CapabilitiesResponse>,
    config: ApiConfig,
    started: Instant,
}

impl std::fmt::Debug for ApiState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ApiState")
            .field("store", &self.store)
            .field("primary", &self.primary.descriptor())
            .field("processors", &self.processors.keys().collect::<Vec<_>>())
            .field("query_extensions", &self.query_extensions)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct MountedQueryExtension {
    processor: String,
    #[serde(flatten)]
    summary: QueryExtensionSummary,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CapabilitiesResponse {
    chain_id: u64,
    ethereum_rpc: EthereumRpcCapabilities,
    processors: Vec<ProcessorSummary>,
    query_extensions: Vec<MountedQueryExtension>,
}

#[derive(Clone, Copy, Debug, Serialize)]
struct EthereumRpcCapabilities {
    methods: EthereumRpcMethods,
}

#[derive(Clone, Copy, Debug, Serialize)]
struct EthereumRpcMethods {
    #[serde(rename = "eth_call")]
    eth_call: RpcMethodCapability,
}

#[derive(Clone, Copy, Debug, Serialize)]
struct RpcMethodCapability {
    supported: bool,
    reason: &'static str,
}

struct PreparedQueryExtension {
    processor: Arc<dyn Processor>,
    extension: Arc<dyn QueryExtension>,
    id: String,
    base_path: String,
    alias_path: Option<String>,
}

impl PreparedQueryExtension {
    fn mounted(&self) -> MountedQueryExtension {
        MountedQueryExtension {
            processor: self.processor.descriptor().instance.to_string(),
            summary: QueryExtensionSummary {
                id: self.id.clone(),
                base_path: self.base_path.clone(),
                alias_path: self.alias_path.clone(),
            },
        }
    }
}

fn prepare_query_extensions(
    processors: &BTreeMap<String, Arc<dyn Processor>>,
    registrations: Vec<QueryExtensionRegistration>,
) -> Result<Vec<PreparedQueryExtension>, ApiError> {
    let mut aliases = BTreeMap::<String, usize>::new();
    for registration in &registrations {
        validate_extension_id(registration.extension().id())?;
        if let Some(alias) = registration.extension().alias() {
            validate_extension_alias(
                registration.processor().descriptor().id.as_str(),
                registration.extension().id(),
                alias,
            )?;
            *aliases.entry(alias.to_owned()).or_default() += 1;
        }
        let instance = registration.processor().descriptor().instance.to_string();
        let Some(registered) = processors.get(&instance) else {
            return Err(ApiError::invalid(
                "query extension owner is not a registered processor instance",
            ));
        };
        if !Arc::ptr_eq(registered, registration.processor()) {
            return Err(ApiError::invalid(
                "query extension owner differs from the registered processor allocation",
            ));
        }
    }

    for (alias, count) in &aliases {
        if *count > 1 {
            tracing::warn!(
                alias,
                registrations = count,
                "query extension alias is ambiguous and will not be mounted"
            );
        }
    }

    let mut owners = BTreeSet::new();
    registrations
        .into_iter()
        .map(|registration| {
            let processor = registration.processor().clone();
            let extension = registration.extension().clone();
            let instance = processor.descriptor().instance.to_string();
            let id = extension.id().to_owned();
            if !owners.insert(instance.clone()) {
                return Err(ApiError::invalid(
                    "a processor instance may register only one query extension",
                ));
            }
            let alias_path = extension.alias().and_then(|alias| {
                (aliases.get(alias).copied() == Some(1)).then(|| format!("/v1/q/{alias}"))
            });
            Ok(PreparedQueryExtension {
                processor,
                extension,
                id,
                base_path: format!("/v1/processors/{instance}/query"),
                alias_path,
            })
        })
        .collect()
}

fn validate_extension_id(id: &str) -> Result<(), ApiError> {
    if id.is_empty()
        || id.len() > 64
        || !id.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_' | b'.')
        })
    {
        Err(ApiError::invalid(
            "query extension ID must use 1..=64 lowercase ASCII letters, digits, '.', '_' or '-'",
        ))
    } else {
        Ok(())
    }
}

fn validate_extension_alias(
    processor_id: &str,
    extension_id: &str,
    alias: &str,
) -> Result<(), ApiError> {
    const BUILT_IN_ALIASES: &[(&str, &str, &str)] = &[
        ("blobs", "blobs-money", "blobs-v1"),
        ("erc20", "erc20-balances", "erc20-v1"),
        ("uniswap", "uniswap-latest", "uniswap-v1"),
    ];
    if alias.is_empty()
        || alias.len() > 64
        || !alias
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        Err(ApiError::invalid(
            "query extension alias must be a non-reserved lowercase [a-z0-9-] segment",
        ))
    } else if BUILT_IN_ALIASES.iter().any(|(built_in, owner, extension)| {
        alias == *built_in && (processor_id != *owner || extension_id != *extension)
    }) {
        Err(ApiError::invalid(
            "query extension alias is reserved for a built-in processor extension",
        ))
    } else {
        Ok(())
    }
}

/// Construct the complete native v1 router.
///
/// # Errors
///
/// Returns an error when API limits are invalid.
pub fn router(
    store: SqliteStore,
    blobs: Arc<BlobsProcessor>,
    config: ApiConfig,
) -> Result<Router, ApiError> {
    let processor: Arc<dyn Processor> = blobs;
    let extension: Arc<dyn QueryExtension> = Arc::new(BlobsQueryExtension);
    let registration = QueryExtensionRegistration::new(processor.clone(), extension);
    router_with_processors(store, vec![processor], vec![registration], config)
}

/// Construct the native API with every configured processor registered for
/// status, schema, changes, and streaming.
///
/// # Errors
///
/// Returns an error for invalid limits or duplicate processor IDs. Typed
/// built-in routes are installed only when their processor is present; the
/// generic processor status/change/stream routes work for every processor.
#[allow(clippy::too_many_lines)]
pub fn router_with_processors(
    store: SqliteStore,
    processors: Vec<Arc<dyn Processor>>,
    extensions: Vec<QueryExtensionRegistration>,
    config: ApiConfig,
) -> Result<Router, ApiError> {
    config.validate()?;
    let bearer_token = config.bearer_token.clone();
    let primary = processors
        .first()
        .cloned()
        .ok_or_else(|| ApiError::invalid("at least one processor must be registered"))?;
    let mut registry = BTreeMap::new();
    for processor in processors {
        let instance = processor.descriptor().instance.to_string();
        if registry.insert(instance.clone(), processor).is_some() {
            return Err(ApiError::invalid(&format!(
                "processor instance {instance} is registered more than once"
            )));
        }
    }
    let prepared_extensions = prepare_query_extensions(&registry, extensions)?;
    let mounted_extensions = prepared_extensions
        .iter()
        .map(PreparedQueryExtension::mounted)
        .collect::<Vec<_>>();
    let processors = Arc::new(registry);
    let query_extensions = Arc::new(mounted_extensions);
    let cached_capabilities = Arc::new(capabilities_response(
        config.chain_id.0,
        &processors,
        &query_extensions,
    ));
    let state = ApiState {
        store,
        primary,
        processors,
        query_extensions,
        capabilities: cached_capabilities,
        config,
        started: Instant::now(),
    };
    let protected = Router::new()
        .route("/v1/status", get(status))
        .route("/v1/capabilities", get(capabilities))
        .route("/v1/processors", get(list_processors))
        .route("/v1/processors/{processor}/status", get(processor_status))
        .route("/v1/processors/{processor}/schema", get(processor_schema))
        .route(
            "/v1/processors/{processor}/artifacts/status",
            get(processor_artifact_status),
        )
        .route(
            "/v1/processors/{processor}/artifacts/{block}",
            get(get_processor_artifact),
        )
        .route(
            "/v1/processors/{processor}/artifacts/export",
            get(export_processor_artifacts),
        )
        .route("/v1/processors/{processor}/changes/head", get(change_head))
        .route("/v1/processors/{processor}/changes", get(changes))
        .route("/v1/processors/{processor}/stream", get(change_stream))
        .route(
            "/v1/processors/{processor}/collections",
            get(list_output_collections),
        )
        .route(
            "/v1/processors/{processor}/collections/{collection}/entities",
            get(query_output_entities),
        )
        .route(
            "/v1/processors/{processor}/collections/{collection}/entities/{key}",
            get(get_output_entity),
        )
        .route(
            "/v1/processors/{processor}/collections/{collection}/query-and-follow",
            axum::routing::post(query_and_follow),
        )
        .route(
            "/v1/processors/{processor}/query-snapshots/{snapshot}",
            axum::routing::delete(release_query_snapshot),
        )
        .route(
            "/v1/processors/{processor}/checkpoints",
            get(list_recovery_checkpoints),
        )
        .route(
            "/v1/processors/{processor}/checkpoints/{checkpoint}/restore",
            axum::routing::post(restore_recovery_checkpoint),
        )
        .route(
            "/v1/processors/{processor}/savepoints",
            get(list_portable_savepoints).post(create_portable_savepoint),
        )
        .route(
            "/v1/processors/{processor}/savepoints/{savepoint}",
            get(export_portable_savepoint).delete(delete_portable_savepoint),
        )
        .route(
            "/v1/processors/{processor}/consumers",
            get(list_consumers).post(create_consumer),
        )
        .route(
            "/v1/processors/{processor}/consumers/{consumer}",
            get(inspect_consumer).delete(revoke_consumer),
        )
        .route(
            "/v1/processors/{processor}/consumers/{consumer}/lease",
            axum::routing::post(renew_consumer),
        )
        .route(
            "/v1/processors/{processor}/consumers/{consumer}/changes",
            get(consumer_changes),
        )
        .route(
            "/v1/processors/{processor}/consumers/{consumer}/ack",
            axum::routing::post(acknowledge_consumer),
        )
        .route(
            "/v1/processors/{processor}/streams/live/consumers/{consumer}/stream",
            get(stream_live_consumer),
        )
        .route(
            "/v1/processors/{processor}/streams/live/consumers/{consumer}/ack",
            axum::routing::post(acknowledge_live_consumer),
        )
        .route(
            "/v1/processors/{processor}/streams/live/consumers/{consumer}/lease",
            axum::routing::post(renew_live_consumer).delete(release_live_consumer),
        )
        .route(
            "/admin/v1/processors/{processor}/lanes/live/reset",
            axum::routing::post(reset_live_lane),
        )
        .route(
            "/admin/v1/processors/{processor}/artifacts",
            axum::routing::delete(delete_processor_artifacts),
        )
        .route(
            "/admin/v1/processors/{processor}/artifacts/replay",
            axum::routing::post(replay_processor_artifacts),
        )
        .route(
            "/v1/backfill-subscriptions/{subscription}/consumers/{consumer}/changes",
            get(backfill_consumer_changes),
        )
        .route(
            "/v1/backfill-subscriptions/{subscription}/consumers/{consumer}/stream",
            get(stream_backfill_consumer),
        )
        .route(
            "/v1/backfill-subscriptions/{subscription}/consumers/{consumer}/ack",
            axum::routing::post(acknowledge_backfill_consumer),
        )
        .route(
            "/v1/backfill-subscriptions/{subscription}/consumers/{consumer}/lease",
            axum::routing::post(renew_backfill_consumer).delete(release_backfill_consumer),
        )
        .route(
            "/admin/v1/backfill-subscriptions",
            get(list_backfill_subscriptions).post(create_backfill_subscription),
        )
        .route(
            "/admin/v1/backfill-subscriptions/{id}",
            get(inspect_backfill_subscription).delete(delete_backfill_subscription),
        )
        .route(
            "/admin/v1/backfill-subscriptions/{id}/cancel",
            axum::routing::post(cancel_backfill_subscription),
        )
        .route(
            "/admin/v1/materialization-jobs",
            get(list_materialization_jobs).post(create_materialization_job),
        )
        .route(
            "/admin/v1/materialization-jobs/{id}",
            get(inspect_materialization_job).delete(delete_materialization_job),
        )
        .route(
            "/admin/v1/materialization-jobs/{id}/cancel",
            axum::routing::post(cancel_materialization_job),
        )
        .route(
            "/admin/v1/raw-history-jobs",
            get(list_raw_history_jobs).post(create_raw_history_job),
        )
        .route(
            "/admin/v1/raw-history-jobs/{id}",
            get(inspect_raw_history_job).delete(delete_raw_history_job),
        )
        .route(
            "/admin/v1/raw-history-jobs/{id}/cancel",
            axum::routing::post(cancel_raw_history_job),
        );
    let mut protected = protected.with_state(state.clone());
    for prepared in prepared_extensions {
        let context = QueryContext::new(
            state.clone(),
            prepared.processor.clone(),
            Arc::from(prepared.id.as_str()),
        );
        let routes = prepared.extension.routes();
        protected = protected.nest(
            &prepared.base_path,
            routes.clone().with_state(context.clone()),
        );
        if let Some(alias_path) = prepared.alias_path {
            protected = protected.nest(&alias_path, routes.with_state(context));
        }
    }
    let protected = if let Some(token) = bearer_token {
        protected.layer(middleware::from_fn_with_state(token, authenticate))
    } else {
        protected
    };
    let operational = Router::new()
        .route("/health/live", get(liveness))
        .route("/health/ready", get(readiness))
        .route("/v1/network/status", get(network_status))
        .route("/debug/network", get(network_dashboard))
        .route("/metrics", get(metrics))
        .with_state(state);
    Ok(operational.merge(protected))
}

async fn authenticate(State(expected): State<Arc<str>>, request: Request, next: Next) -> Response {
    let authorized = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|value| value == expected.as_ref());
    if authorized {
        next.run(request).await
    } else {
        ApiError::unauthorized().into_response()
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct StatusResponse {
    api_version: &'static str,
    project: &'static str,
    version: &'static str,
    chain_id: u64,
    uptime_seconds: u64,
    database_bytes: u64,
    processor: ProcessorSummary,
    processors: Vec<ProcessorSummary>,
    readiness: ReadinessSnapshot,
}

async fn status(State(state): State<ApiState>) -> Result<Json<StatusResponse>, ApiError> {
    let store_stats = state.store.stats().await?;
    let processors = processor_summaries(&state);
    Ok(Json(StatusResponse {
        api_version: API_VERSION,
        project: leani_primitives::PROJECT_NAME,
        version: env!("CARGO_PKG_VERSION"),
        chain_id: state.config.chain_id.0,
        uptime_seconds: state.started.elapsed().as_secs(),
        database_bytes: store_stats.database_bytes,
        processor: processor_summary(&state, state.primary.as_ref()),
        processors,
        readiness: state.config.readiness.snapshot(),
    }))
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
#[allow(clippy::struct_excessive_bools)]
struct ReadinessSnapshot {
    live_required: bool,
    live_ready: bool,
    finality_required: bool,
    finality_ready: bool,
    ready: bool,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProcessorSummary {
    id: String,
    instance: String,
    version: String,
    code_hash: String,
    config_hash: String,
    generic_api: &'static str,
    change_schema: String,
    query_extensions: Vec<QueryExtensionSummary>,
    subscriptions: bool,
    artifact_retention: &'static str,
    delivery_ordering: &'static str,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct NetworkProcessorStatus {
    id: String,
    history_control: &'static str,
    history_mode: &'static str,
    coverage: CoverageResponse,
    stored_range_contiguous: bool,
    sync_target_block: Option<u64>,
    blocks_remaining: Option<u64>,
    missing_ranges: Vec<MissingCoverageRange>,
    pending_deltas: u64,
    processor_artifacts: u64,
    processor_artifact_bytes: u64,
    pending_processor_artifacts: u64,
    run_state: &'static str,
    pause_reason: Option<String>,
    live_gap: Option<LiveGapStatus>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct LiveGapStatus {
    first_unapplied_block: u64,
    first_unapplied_hash: String,
    required_delivery_bytes: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct MissingCoverageRange {
    from_block: u64,
    to_block: u64,
    blocks: u64,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct NetworkStorageStatus {
    database_bytes: u64,
    freelist_bytes: u64,
    wal_bytes: u64,
    physical_file_bytes: u64,
    artifact_segment_bytes: u64,
    total_on_disk_bytes: u64,
    applied_blocks: u64,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct NetworkHistoryStatus {
    jobs: Vec<BackfillStatus>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct NetworkStatusResponse {
    observed_at_unix_seconds: u64,
    readiness: ReadinessSnapshot,
    network: NetworkTelemetrySnapshot,
    storage: NetworkStorageStatus,
    processors: Vec<NetworkProcessorStatus>,
    history: NetworkHistoryStatus,
}

#[allow(clippy::too_many_lines)]
async fn network_status(
    State(state): State<ApiState>,
) -> Result<Json<NetworkStatusResponse>, ApiError> {
    let network = state.config.network_telemetry.snapshot();
    let storage_stats = state.store.storage_stats().await?;
    let sync_target_block = live_sync_target(&network);
    let backfills = if let Some(control) = &state.config.backfill_control {
        control.list(None).await.map_err(backfill_error)?
    } else {
        Vec::new()
    };
    let mut processors = Vec::with_capacity(state.processors.len());
    let mut applied_blocks = 0_u64;
    for processor in state.processors.values() {
        let coverage = coverage(&state, processor.as_ref(), None).await?;
        let store_stats = state.store.processor_stats(processor.descriptor()).await?;
        applied_blocks = applied_blocks.saturating_add(store_stats.applied_blocks);
        let runtime = state
            .store
            .processor_runtime_state(processor.descriptor())
            .await
            .ok();
        let live_gap = state
            .store
            .live_lane_gap(processor.descriptor())
            .await?
            .map(|gap| LiveGapStatus {
                first_unapplied_block: gap.first_unapplied.number.0,
                first_unapplied_hash: hash_hex(gap.first_unapplied.hash),
                required_delivery_bytes: gap.required_delivery_bytes.to_string(),
            });
        let instance = processor.descriptor().instance.to_string();
        let on_demand = state.config.on_demand_processors.contains(&instance);
        let requested_ranges = diagnostic_ranges(
            on_demand,
            &instance,
            sync_target_block,
            &coverage,
            &backfills,
        );
        let diagnostic_target = requested_ranges
            .iter()
            .map(|(_, to)| *to)
            .max()
            .or(sync_target_block);
        let missing_ranges = merge_missing_ranges(
            requested_ranges
                .iter()
                .flat_map(|(from, to)| missing_coverage_ranges(*from, *to, &coverage.available))
                .collect(),
        );
        let blocks_remaining = if on_demand {
            diagnostic_target.map(|_| {
                missing_ranges
                    .iter()
                    .fold(0_u64, |total, range| total.saturating_add(range.blocks))
            })
        } else {
            sync_target_block.map(|target| {
                missing_coverage_blocks(
                    coverage.configured_start_block,
                    target,
                    &coverage.available,
                )
            })
        };
        processors.push(NetworkProcessorStatus {
            id: processor.descriptor().id.to_string(),
            history_control: if state
                .config
                .application_subscription_processors
                .contains(&instance)
            {
                "application_subscriptions"
            } else {
                "node_owned"
            },
            history_mode: if on_demand { "on_demand" } else { "automatic" },
            stored_range_contiguous: if on_demand {
                missing_ranges.is_empty()
            } else {
                coverage.complete
            },
            sync_target_block: diagnostic_target,
            blocks_remaining,
            missing_ranges,
            pending_deltas: store_stats.pending_deltas,
            processor_artifacts: store_stats.processor_artifacts,
            processor_artifact_bytes: store_stats.processor_artifact_bytes,
            pending_processor_artifacts: store_stats.pending_processor_artifacts,
            run_state: runtime
                .as_ref()
                .map_or("not_started", |runtime| run_state_name(runtime.state)),
            pause_reason: runtime.and_then(|runtime| runtime.reason),
            live_gap,
            coverage,
        });
    }
    Ok(Json(NetworkStatusResponse {
        observed_at_unix_seconds: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
        readiness: state.config.readiness.snapshot(),
        network,
        storage: NetworkStorageStatus {
            database_bytes: storage_stats.database_bytes,
            freelist_bytes: storage_stats.freelist_bytes,
            wal_bytes: storage_stats.wal_bytes,
            physical_file_bytes: storage_stats.physical_file_bytes,
            artifact_segment_bytes: storage_stats.artifact_segment_bytes,
            total_on_disk_bytes: storage_stats.total_physical_bytes,
            applied_blocks,
        },
        processors,
        history: recent_network_history(backfills),
    }))
}

fn recent_network_history(mut jobs: Vec<BackfillStatus>) -> NetworkHistoryStatus {
    jobs.sort_by_key(|job| std::cmp::Reverse(job.updated_at_unix_ms));
    jobs.truncate(64);
    NetworkHistoryStatus { jobs }
}

fn live_sync_target(network: &NetworkTelemetrySnapshot) -> Option<u64> {
    network
        .sessions
        .iter()
        .filter(|session| session.active && session.lane == leani_source_api::NetworkLane::Live)
        .filter_map(|session| session.observed_head_block)
        .max()
}

fn diagnostic_ranges(
    on_demand: bool,
    processor_instance: &str,
    live_target: Option<u64>,
    coverage: &CoverageResponse,
    backfills: &[BackfillStatus],
) -> Vec<(u64, u64)> {
    if !on_demand {
        return live_target
            .map(|target| vec![(coverage.configured_start_block, target)])
            .unwrap_or_default();
    }
    let mut requested = backfills
        .iter()
        .filter(|job| {
            job.processor == processor_instance
                && matches!(
                    job.state,
                    BackfillState::WaitingForConsumer
                        | BackfillState::Queued
                        | BackfillState::Running
                        | BackfillState::Backpressured
                        | BackfillState::StorageBackpressured
                        | BackfillState::Draining
                )
        })
        .map(|job| (job.from_block, job.to_block))
        .collect::<Vec<_>>();
    if requested.is_empty()
        && let Some(target) = live_target
    {
        let start = coverage
            .available
            .iter()
            .filter(|interval| interval.from_block <= target)
            .max_by_key(|interval| interval.to_block)
            .map_or(target, |interval| interval.from_block);
        requested.push((start, target));
    }
    merge_ranges(requested)
}

fn merge_ranges(mut ranges: Vec<(u64, u64)>) -> Vec<(u64, u64)> {
    ranges.sort_unstable();
    let mut merged: Vec<(u64, u64)> = Vec::with_capacity(ranges.len());
    for (from, to) in ranges {
        if let Some((_, merged_to)) = merged.last_mut()
            && from <= merged_to.saturating_add(1)
        {
            *merged_to = (*merged_to).max(to);
        } else {
            merged.push((from, to));
        }
    }
    merged
}

fn merge_missing_ranges(ranges: Vec<MissingCoverageRange>) -> Vec<MissingCoverageRange> {
    merge_ranges(
        ranges
            .into_iter()
            .map(|range| (range.from_block, range.to_block))
            .collect(),
    )
    .into_iter()
    .map(|(from_block, to_block)| MissingCoverageRange {
        from_block,
        to_block,
        blocks: to_block.saturating_sub(from_block).saturating_add(1),
    })
    .collect()
}

fn missing_coverage_blocks(
    configured_start: u64,
    target: u64,
    available: &[CoverageInterval],
) -> u64 {
    missing_coverage_ranges(configured_start, target, available)
        .iter()
        .fold(0_u64, |total, range| total.saturating_add(range.blocks))
}

fn missing_coverage_ranges(
    configured_start: u64,
    target: u64,
    available: &[CoverageInterval],
) -> Vec<MissingCoverageRange> {
    if target < configured_start {
        return Vec::new();
    }
    let mut next = configured_start;
    let mut missing = Vec::new();
    for interval in available {
        if interval.to_block < next {
            continue;
        }
        if interval.from_block > target {
            break;
        }
        if interval.from_block > next {
            let to_block = interval.from_block.saturating_sub(1).min(target);
            missing.push(MissingCoverageRange {
                from_block: next,
                to_block,
                blocks: to_block.saturating_sub(next).saturating_add(1),
            });
        }
        if interval.to_block >= target {
            return missing;
        }
        next = next.max(interval.to_block.saturating_add(1));
    }
    if next <= target {
        missing.push(MissingCoverageRange {
            from_block: next,
            to_block: target,
            blocks: target.saturating_sub(next).saturating_add(1),
        });
    }
    missing
}

async fn network_dashboard() -> Html<&'static str> {
    Html(include_str!("network-dashboard.html"))
}

fn processor_summary(state: &ApiState, processor: &dyn Processor) -> ProcessorSummary {
    processor_summary_with_extensions(&state.query_extensions, processor)
}

fn processor_summary_with_extensions(
    query_extensions: &[MountedQueryExtension],
    processor: &dyn Processor,
) -> ProcessorSummary {
    let descriptor = processor.descriptor();
    ProcessorSummary {
        id: descriptor.id.to_string(),
        instance: descriptor.instance.to_string(),
        version: descriptor.version.to_string(),
        code_hash: hash_hex(descriptor.code_hash),
        config_hash: hash_hex(descriptor.config_hash),
        generic_api: "processor-v1",
        change_schema: descriptor.schemas.change_schema.clone(),
        query_extensions: query_extensions
            .iter()
            .filter(|extension| extension.processor == descriptor.instance.as_str())
            .map(|extension| extension.summary.clone())
            .collect(),
        subscriptions: descriptor.lifecycle.delivery.mode
            != leani_processor_api::DeliveryPolicyMode::None,
        artifact_retention: match descriptor.lifecycle.artifacts.mode {
            leani_processor_api::ArtifactPolicyMode::None => "none",
            leani_processor_api::ArtifactPolicyMode::Window => "window",
            leani_processor_api::ArtifactPolicyMode::Full => "full",
        },
        delivery_ordering: match descriptor.delivery_ordering {
            leani_processor_api::DeliveryOrdering::Canonical => "canonical",
            leani_processor_api::DeliveryOrdering::BlockVersionedIdempotent => {
                "block_versioned_idempotent"
            }
        },
    }
}

fn processor_summaries(state: &ApiState) -> Vec<ProcessorSummary> {
    processor_summaries_with_extensions(&state.processors, &state.query_extensions)
}

fn processor_summaries_with_extensions(
    processors: &BTreeMap<String, Arc<dyn Processor>>,
    query_extensions: &[MountedQueryExtension],
) -> Vec<ProcessorSummary> {
    processors
        .values()
        .map(|processor| processor_summary_with_extensions(query_extensions, processor.as_ref()))
        .collect()
}

fn capabilities_response(
    chain_id: u64,
    processors: &BTreeMap<String, Arc<dyn Processor>>,
    query_extensions: &[MountedQueryExtension],
) -> CapabilitiesResponse {
    CapabilitiesResponse {
        chain_id,
        ethereum_rpc: EthereumRpcCapabilities {
            methods: EthereumRpcMethods {
                eth_call: RpcMethodCapability {
                    supported: false,
                    reason: "evm_state_unavailable",
                },
            },
        },
        processors: processor_summaries_with_extensions(processors, query_extensions),
        query_extensions: query_extensions.to_vec(),
    }
}

async fn list_processors(State(state): State<ApiState>) -> Json<Value> {
    Json(json!({ "data": processor_summaries(&state) }))
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ArtifactRangeQuery {
    from_block: u64,
    to_block: u64,
    limit: Option<usize>,
}

impl ArtifactRangeQuery {
    fn range(self) -> Result<BlockRange, ApiError> {
        BlockRange::new(BlockNumber(self.from_block), BlockNumber(self.to_block))
            .map_err(|error| ApiError::invalid(&error.to_string()))
    }
}

async fn processor_artifact_status(
    State(state): State<ApiState>,
    Path(processor): Path<String>,
) -> Result<Json<ProcessorArtifactStats>, ApiError> {
    let processor = configured_processor(&state, &processor)?;
    Ok(Json(
        state
            .store
            .processor_artifact_stats(processor.descriptor())
            .await?,
    ))
}

async fn get_processor_artifact(
    State(state): State<ApiState>,
    Path((processor, block)): Path<(String, u64)>,
) -> Result<Json<Value>, ApiError> {
    let processor = configured_processor(&state, &processor)?;
    let artifact = state
        .store
        .processor_artifact(processor.descriptor(), BlockNumber(block))
        .await?
        .ok_or_else(|| ApiError::not_found("processor artifact is not retained"))?;
    Ok(Json(json!({
        "processorInstance": processor.descriptor().instance,
        "chainId": artifact.delta.chain_id.0.to_string(),
        "block": {
            "number": artifact.delta.block.number.0.to_string(),
            "hash": format!("0x{}", hex::encode(artifact.delta.block.hash.0)),
            "parentHash": format!("0x{}", hex::encode(artifact.delta.block.parent_hash.0)),
            "timestamp": artifact.delta.block.timestamp.to_string()
        },
        "deltaSchemaVersion": artifact.delta.schema_version,
        "payload": format!("0x{}", hex::encode(&artifact.delta.payload)),
        "checksum": format!("0x{}", hex::encode(artifact.delta.checksum.0)),
        "encodedBytes": artifact.encoded_bytes.to_string(),
        "retainedAtUnixMs": artifact.retained_at_unix_ms.to_string()
    })))
}

async fn export_processor_artifacts(
    State(state): State<ApiState>,
    Path(processor): Path<String>,
    Query(query): Query<ArtifactRangeQuery>,
) -> Result<Response, ApiError> {
    let processor = configured_processor(&state, &processor)?;
    let limit = page_limit(&state, query.limit)?;
    let range = query.range()?;
    let export = state
        .store
        .export_processor_artifacts(processor.descriptor(), range, limit)
        .await?;
    let digest = format!("0x{}", hex::encode(export.logical_digest.0));
    let complete = export.complete;
    let encoded = export.encode_durable()?;
    let mut response = Response::new(Body::from(encoded));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/vnd.leani.processor-artifacts"),
    );
    response.headers_mut().insert(
        "x-leani-artifact-digest",
        HeaderValue::from_str(&digest)
            .map_err(|error| ApiError::internal(&format!("artifact digest header: {error}")))?,
    );
    response.headers_mut().insert(
        "x-leani-artifact-complete",
        HeaderValue::from_static(if complete { "true" } else { "false" }),
    );
    Ok(response)
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DeleteArtifactQuery {
    from_block: u64,
    to_block: u64,
    #[serde(default = "default_artifact_owner_kind")]
    owner_kind: ArtifactOwnerKind,
    owner_id: Option<String>,
}

const fn default_artifact_owner_kind() -> ArtifactOwnerKind {
    ArtifactOwnerKind::ProcessorInstance
}

async fn delete_processor_artifacts(
    State(state): State<ApiState>,
    Path(processor): Path<String>,
    Query(query): Query<DeleteArtifactQuery>,
) -> Result<Json<ArtifactPruneOutcome>, ApiError> {
    let processor = configured_processor(&state, &processor)?;
    let range = BlockRange::new(BlockNumber(query.from_block), BlockNumber(query.to_block))
        .map_err(|error| ApiError::invalid(&error.to_string()))?;
    let owner_id = query.owner_id.unwrap_or_else(|| {
        if query.owner_kind == ArtifactOwnerKind::ProcessorInstance {
            processor.descriptor().instance.to_string()
        } else {
            String::new()
        }
    });
    if owner_id.is_empty() {
        return Err(ApiError::invalid(
            "ownerId is required for processor_job and operator_pin deletion",
        ));
    }
    Ok(Json(
        state
            .store
            .release_processor_artifacts(processor.descriptor(), range, query.owner_kind, &owner_id)
            .await?,
    ))
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ReplayArtifactsRequest {
    target_processor: String,
    from_block: u64,
    to_block: u64,
    limit: Option<usize>,
}

async fn replay_processor_artifacts(
    State(state): State<ApiState>,
    Path(source): Path<String>,
    Json(request): Json<ReplayArtifactsRequest>,
) -> Result<Json<ArtifactReplayOutcome>, ApiError> {
    let source = configured_processor(&state, &source)?;
    let target = configured_processor(&state, &request.target_processor)?;
    let range = BlockRange::new(
        BlockNumber(request.from_block),
        BlockNumber(request.to_block),
    )
    .map_err(|error| ApiError::invalid(&error.to_string()))?;
    let limit = page_limit(&state, request.limit)?;
    Ok(Json(
        state
            .store
            .replay_processor_artifacts(source.descriptor(), target.as_ref(), range, limit, &[])
            .await?,
    ))
}

async fn capabilities(State(state): State<ApiState>) -> Json<CapabilitiesResponse> {
    Json(state.capabilities.as_ref().clone())
}

fn backfill_control(state: &ApiState) -> Result<&Arc<dyn BackfillControl>, ApiError> {
    state.config.backfill_control.as_ref().ok_or_else(|| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "backfill_unavailable",
            "this node has no historical work control plane",
            true,
        )
    })
}

fn raw_history_control(state: &ApiState) -> Result<&Arc<dyn RawHistoryControl>, ApiError> {
    state.config.raw_history_control.as_ref().ok_or_else(|| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "raw_history_unavailable",
            "this node has no raw-history control plane",
            true,
        )
    })
}

async fn create_raw_history_job(
    State(state): State<ApiState>,
    Json(request): Json<CreateRawHistoryJobRequest>,
) -> Result<(StatusCode, Json<RawHistoryJob>), ApiError> {
    let job = raw_history_control(&state)?
        .create(request)
        .await
        .map_err(raw_history_error)?;
    Ok((StatusCode::ACCEPTED, Json(job)))
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RawHistoryJobListResponse {
    data: Vec<RawHistoryJob>,
}

async fn list_raw_history_jobs(
    State(state): State<ApiState>,
) -> Result<Json<RawHistoryJobListResponse>, ApiError> {
    Ok(Json(RawHistoryJobListResponse {
        data: raw_history_control(&state)?
            .list()
            .await
            .map_err(raw_history_error)?,
    }))
}

async fn inspect_raw_history_job(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<RawHistoryJob>, ApiError> {
    Ok(Json(
        raw_history_control(&state)?
            .inspect(&id)
            .await
            .map_err(raw_history_error)?,
    ))
}

async fn cancel_raw_history_job(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<RawHistoryJob>, ApiError> {
    Ok(Json(
        raw_history_control(&state)?
            .cancel(&id)
            .await
            .map_err(raw_history_error)?,
    ))
}

async fn delete_raw_history_job(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<RawHistoryJobDeletion>, ApiError> {
    Ok(Json(
        raw_history_control(&state)?
            .delete(&id)
            .await
            .map_err(raw_history_error)?,
    ))
}

fn raw_history_error(error: RawHistoryControlError) -> ApiError {
    match error {
        RawHistoryControlError::Invalid(message) => ApiError::invalid(&message),
        RawHistoryControlError::ProfileIncompatible(message) => ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "raw_history_profile_incompatible",
            &message,
            false,
        ),
        RawHistoryControlError::NotFound(message) => ApiError::not_found(&message),
        RawHistoryControlError::Conflict(message) => {
            ApiError::conflict("raw_history_conflict", &message)
        }
        RawHistoryControlError::Unavailable(message) => ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "raw_history_unavailable",
            &message,
            true,
        ),
        RawHistoryControlError::Internal(message) => ApiError::internal(&message),
    }
}

async fn create_backfill_subscription(
    State(state): State<ApiState>,
    Json(request): Json<CreateBackfillRequest>,
) -> Result<(StatusCode, Json<BackfillStatus>), ApiError> {
    let status = backfill_control(&state)?
        .create_subscription(request)
        .await
        .map_err(backfill_error)?;
    Ok((StatusCode::ACCEPTED, Json(status)))
}

async fn create_materialization_job(
    State(state): State<ApiState>,
    Json(request): Json<CreateMaterializationRequest>,
) -> Result<(StatusCode, Json<BackfillStatus>), ApiError> {
    let status = backfill_control(&state)?
        .create_materialization(request)
        .await
        .map_err(backfill_error)?;
    Ok((StatusCode::ACCEPTED, Json(status)))
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BackfillListResponse {
    data: Vec<BackfillStatus>,
}

async fn list_owned_historical_work(
    state: &ApiState,
    owner: HistoricalWorkOwner,
) -> Result<Json<BackfillListResponse>, ApiError> {
    Ok(Json(BackfillListResponse {
        data: backfill_control(state)?
            .list(Some(owner))
            .await
            .map_err(backfill_error)?,
    }))
}

async fn list_backfill_subscriptions(
    State(state): State<ApiState>,
) -> Result<Json<BackfillListResponse>, ApiError> {
    list_owned_historical_work(&state, HistoricalWorkOwner::Subscription).await
}

async fn list_materialization_jobs(
    State(state): State<ApiState>,
) -> Result<Json<BackfillListResponse>, ApiError> {
    list_owned_historical_work(&state, HistoricalWorkOwner::Materialization).await
}

async fn inspect_owned_historical_work(
    state: &ApiState,
    id: &str,
    owner: HistoricalWorkOwner,
) -> Result<Json<BackfillStatus>, ApiError> {
    let status = backfill_control(state)?
        .inspect(id)
        .await
        .map_err(backfill_error)?;
    if status.owner != owner {
        return Err(ApiError::not_found(id));
    }
    Ok(Json(status))
}

async fn inspect_backfill_subscription(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<BackfillStatus>, ApiError> {
    inspect_owned_historical_work(&state, &id, HistoricalWorkOwner::Subscription).await
}

async fn inspect_materialization_job(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<BackfillStatus>, ApiError> {
    inspect_owned_historical_work(&state, &id, HistoricalWorkOwner::Materialization).await
}

async fn cancel_owned_historical_work(
    state: &ApiState,
    id: &str,
    owner: HistoricalWorkOwner,
) -> Result<Json<BackfillStatus>, ApiError> {
    let current = backfill_control(state)?
        .inspect(id)
        .await
        .map_err(backfill_error)?;
    if current.owner != owner {
        return Err(ApiError::not_found(id));
    }
    Ok(Json(
        backfill_control(state)?
            .cancel(id)
            .await
            .map_err(backfill_error)?,
    ))
}

async fn cancel_backfill_subscription(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<BackfillStatus>, ApiError> {
    cancel_owned_historical_work(&state, &id, HistoricalWorkOwner::Subscription).await
}

async fn cancel_materialization_job(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<BackfillStatus>, ApiError> {
    cancel_owned_historical_work(&state, &id, HistoricalWorkOwner::Materialization).await
}

async fn delete_owned_historical_work(
    state: &ApiState,
    id: &str,
    owner: HistoricalWorkOwner,
) -> Result<Json<HistoricalWorkDeletion>, ApiError> {
    let current = backfill_control(state)?
        .inspect(id)
        .await
        .map_err(backfill_error)?;
    if current.owner != owner {
        return Err(ApiError::not_found(id));
    }
    Ok(Json(
        backfill_control(state)?
            .delete(id)
            .await
            .map_err(backfill_error)?,
    ))
}

async fn delete_backfill_subscription(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<HistoricalWorkDeletion>, ApiError> {
    delete_owned_historical_work(&state, &id, HistoricalWorkOwner::Subscription).await
}

async fn delete_materialization_job(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<HistoricalWorkDeletion>, ApiError> {
    delete_owned_historical_work(&state, &id, HistoricalWorkOwner::Materialization).await
}

fn backfill_error(error: BackfillControlError) -> ApiError {
    match error {
        BackfillControlError::Invalid(message) => ApiError::invalid(&message),
        BackfillControlError::NotFound(message) => ApiError::not_found(&message),
        BackfillControlError::Conflict(message) => {
            ApiError::conflict("backfill_conflict", &message)
        }
        BackfillControlError::HistoryNotFinalized {
            requested,
            finalized,
            finalized_hash,
        } => {
            let mut error = ApiError::conflict(
                "history_not_finalized",
                &format!(
                    "requested history through block {requested} exceeds finalized head {finalized}"
                ),
            );
            error.details = Some(json!({
                "requestedToBlock": requested,
                "currentFinalizedHead": {
                    "number": finalized,
                    "hash": finalized_hash,
                },
                "suggestedToBlock": finalized,
                "action": "clamp_and_retry"
            }));
            error
        }
        BackfillControlError::RangeAfterFinalizedHead {
            requested,
            finalized,
            finalized_hash,
        } => {
            let mut error = ApiError::conflict(
                "range_after_finalized_head",
                &format!(
                    "requested history starts at block {requested}, after finalized head {finalized}"
                ),
            );
            error.details = Some(json!({
                "requestedFromBlock": requested,
                "currentFinalizedHead": {
                    "number": finalized,
                    "hash": finalized_hash,
                }
            }));
            error
        }
        BackfillControlError::RecomputeCoverageMissing { gaps } => {
            let mut error = ApiError::conflict(
                "recompute_coverage_missing",
                "recompute requires retained finalized coverage for every requested block",
            );
            error.details = Some(json!({
                "uncoveredRanges": gaps,
                "action": "fill_missing_then_retry"
            }));
            error
        }
        BackfillControlError::Unavailable(message) => ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "backfill_unavailable",
            &message,
            true,
        ),
        BackfillControlError::Internal(message) => ApiError::internal(&message),
    }
}

async fn processor_status(
    State(state): State<ApiState>,
    Path(processor): Path<String>,
) -> Result<Json<CoverageResponse>, ApiError> {
    let processor = configured_processor(&state, &processor)?;
    Ok(Json(coverage(&state, processor.as_ref(), None).await?))
}

async fn processor_schema(
    State(state): State<ApiState>,
    Path(processor): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let processor = configured_processor(&state, &processor)?;
    let descriptor = processor.descriptor();
    Ok(Json(json!({
        "processor": processor_summary(&state, processor.as_ref()),
        "deltaVersion": descriptor.schemas.delta_version,
        "entitySchema": descriptor.schemas.entity_schema,
        "changeSchema": descriptor.schemas.change_schema
    })))
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CoverageInterval {
    from_block: u64,
    to_block: u64,
    finality: &'static str,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestedRange {
    from_block: u64,
    to_block: u64,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CoverageResponse {
    chain_id: u64,
    chain_finalized_head: Option<ChainFinalizedHead>,
    requested: Option<RequestedRange>,
    available: Vec<CoverageInterval>,
    configured_start_block: u64,
    processed_through: Option<u64>,
    finalized_through: Option<u64>,
    complete: bool,
    state: &'static str,
}

impl CoverageResponse {
    #[must_use]
    pub const fn is_complete(&self) -> bool {
        self.complete
    }

    #[must_use]
    pub fn available(&self) -> &[CoverageInterval] {
        &self.available
    }

    #[must_use]
    pub const fn processed_through(&self) -> Option<u64> {
        self.processed_through
    }

    #[must_use]
    pub const fn finalized_through(&self) -> Option<u64> {
        self.finalized_through
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChainFinalizedHead {
    number: u64,
    hash: String,
}

async fn coverage(
    state: &ApiState,
    processor: &dyn Processor,
    requested: Option<BlockRange>,
) -> Result<CoverageResponse, ApiError> {
    let descriptor = processor.descriptor();
    let configured_start = processor_start_block(descriptor);
    let cursor = state.store.processor_cursor(descriptor).await?;
    let query_range = requested.or_else(|| {
        cursor.as_ref().and_then(|cursor| {
            BlockRange::new(BlockNumber(configured_start), cursor.block_number).ok()
        })
    });
    let ranges = if let Some(range) = query_range {
        state.store.coverage(descriptor, range).await?
    } else {
        Vec::new()
    };
    let finalized_through = state.store.finalized_through(descriptor).await?;
    let chain_finalized_head = state
        .store
        .finalized_canonical_head(state.config.chain_id)
        .await?
        .map(|head| ChainFinalizedHead {
            number: head.number.0,
            hash: head.hash.to_string(),
        });
    let run_state = state
        .store
        .processor_runtime_state(descriptor)
        .await
        .ok()
        .map(|runtime| runtime.state);
    let complete = query_range.is_some_and(|range| coverage_gaps(range, &ranges).is_empty());
    let processed_through = ranges.last().map(|range| range.end().0);
    Ok(CoverageResponse {
        chain_id: state.config.chain_id.0,
        chain_finalized_head,
        requested: requested.map(|range| RequestedRange {
            from_block: range.start().0,
            to_block: range.end().0,
        }),
        available: ranges
            .into_iter()
            .map(|range| CoverageInterval {
                from_block: range.start().0,
                to_block: range.end().0,
                finality: if finalized_through.is_some_and(|block| block >= range.end()) {
                    "finalized"
                } else {
                    "optimistic"
                },
            })
            .collect(),
        configured_start_block: configured_start,
        finalized_through: finalized_through.map(|block| block.0),
        processed_through,
        complete,
        state: if run_state == Some(ProcessorRunState::Paused) {
            "paused"
        } else if run_state == Some(ProcessorRunState::Failed) {
            "failed"
        } else if cursor.is_none() {
            "starting"
        } else if complete && state.config.readiness.snapshot().ready {
            "live"
        } else if complete {
            "catching_up"
        } else {
            "backfilling"
        },
    })
}

const fn run_state_name(state: ProcessorRunState) -> &'static str {
    match state {
        ProcessorRunState::Running => "running",
        ProcessorRunState::Paused => "paused",
        ProcessorRunState::Failed => "failed",
    }
}

fn processor_start_block(descriptor: &ProcessorDescriptor) -> u64 {
    match &descriptor.start {
        StartPoint::Genesis | StartPoint::ProcessorCheckpoint(_) => 0,
        StartPoint::Block(block) => block.0,
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct HealthResponse {
    status: &'static str,
    uptime_seconds: u64,
    database: &'static str,
    readiness: ReadinessSnapshot,
    reasons: Vec<&'static str>,
}

async fn liveness(State(state): State<ApiState>) -> Response {
    let database_ok = state.store.stats().await.is_ok();
    let body = HealthResponse {
        status: if database_ok { "live" } else { "failed" },
        uptime_seconds: state.started.elapsed().as_secs(),
        database: if database_ok {
            "available"
        } else {
            "unavailable"
        },
        readiness: state.config.readiness.snapshot(),
        reasons: if database_ok {
            Vec::new()
        } else {
            vec!["store_unavailable"]
        },
    };
    (
        if database_ok {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        Json(body),
    )
        .into_response()
}

async fn readiness(State(state): State<ApiState>) -> Response {
    let database_ok = state.store.stats().await.is_ok();
    let readiness = state.config.readiness.snapshot();
    let mut reasons = Vec::new();
    if !database_ok {
        reasons.push("store_unavailable");
    }
    if readiness.live_required && !readiness.live_ready {
        reasons.push("live_source_not_ready");
    }
    if readiness.finality_required && !readiness.finality_ready {
        reasons.push("finality_source_not_ready");
    }
    let ready = database_ok && readiness.ready;
    (
        if ready {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        Json(HealthResponse {
            status: if ready { "ready" } else { "not_ready" },
            uptime_seconds: state.started.elapsed().as_secs(),
            database: if database_ok {
                "available"
            } else {
                "unavailable"
            },
            readiness,
            reasons,
        }),
    )
        .into_response()
}

#[allow(clippy::too_many_lines)]
async fn metrics(State(state): State<ApiState>) -> Result<Response, ApiError> {
    use std::fmt::Write as _;

    let store_metrics = state.store.stats().await?;
    let store_budgets = state.store.budget_stats().await?;
    let recent_metrics = state.store.recent_stats(state.config.chain_id).await?;
    let readiness = state.config.readiness.snapshot();
    let network = state.config.network_telemetry.snapshot();
    let (backfills, historical_material) = if let Some(control) = &state.config.backfill_control {
        (
            control.list(None).await.map_err(backfill_error)?,
            control.historical_material_metrics(),
        )
    } else {
        (Vec::new(), None)
    };
    let mut output = String::new();
    writeln!(
        output,
        "# HELP leani_up Whether the process store is readable."
    )?;
    writeln!(output, "# TYPE leani_up gauge")?;
    writeln!(output, "leani_up 1")?;
    writeln!(
        output,
        "# HELP leani_ready Whether all required service components are ready."
    )?;
    writeln!(output, "# TYPE leani_ready gauge")?;
    writeln!(output, "leani_ready {}", u8::from(readiness.ready))?;
    for (name, value) in [
        ("live_required", readiness.live_required),
        ("live_ready", readiness.live_ready),
        ("finality_required", readiness.finality_required),
        ("finality_ready", readiness.finality_ready),
    ] {
        writeln!(output, "# TYPE leani_{name} gauge")?;
        writeln!(output, "leani_{name} {}", u8::from(value))?;
    }
    writeln!(output, "# TYPE leani_uptime_seconds counter")?;
    writeln!(
        output,
        "leani_uptime_seconds {}",
        state.started.elapsed().as_secs()
    )?;
    append_network_metrics(&mut output, &network)?;
    for (name, value) in [
        ("database_bytes", store_metrics.database_bytes),
        ("freelist_bytes", store_metrics.freelist_bytes),
        ("wal_bytes", store_metrics.wal_bytes),
        ("physical_file_bytes", store_metrics.physical_file_bytes),
        (
            "artifact_segment_physical_bytes",
            store_metrics.artifact_segment_bytes,
        ),
        (
            "total_physical_store_bytes",
            store_metrics.total_physical_bytes,
        ),
        ("processor_instances", store_metrics.processor_instances),
        ("entities", store_metrics.entities),
        ("index_entries", store_metrics.index_entries),
        ("exact_coverage_blocks", store_metrics.exact_coverage_blocks),
        ("coverage_intervals", store_metrics.coverage_intervals),
        ("coverage_segments", store_metrics.coverage_segments),
        ("coverage_owners", store_metrics.coverage_owners),
        ("applied_blocks", store_metrics.applied_blocks),
        ("undo_records", store_metrics.undo_records),
        ("changes", store_metrics.changes),
        ("processor_artifacts", store_metrics.processor_artifacts),
        (
            "processor_artifact_bytes",
            store_metrics.processor_artifact_bytes,
        ),
        (
            "processor_artifact_owners",
            store_metrics.processor_artifact_owners,
        ),
        (
            "pending_processor_artifacts",
            store_metrics.pending_processor_artifacts,
        ),
        (
            "pending_processor_artifact_bytes",
            store_metrics.pending_processor_artifact_bytes,
        ),
        (
            "delivery_retained_bytes",
            store_metrics.delivery_retained_bytes,
        ),
        (
            "history_delivery_retained_bytes",
            store_metrics.history_delivery_retained_bytes,
        ),
        ("recent_frames", recent_metrics.frames),
        ("recent_frame_bytes", recent_metrics.encoded_bytes),
    ] {
        writeln!(output, "# TYPE leani_store_{name} gauge")?;
        writeln!(output, "leani_store_{name} {value}")?;
    }
    for (name, value) in [
        (
            "processor_artifact_budget_bytes",
            store_budgets.maximum_processor_artifact_bytes,
        ),
        (
            "pending_processor_artifact_budget_bytes",
            store_budgets.maximum_pending_processor_artifact_bytes,
        ),
        (
            "delivery_retained_budget_bytes",
            store_budgets.maximum_delivery_retained_bytes,
        ),
        (
            "history_delivery_retained_budget_bytes",
            store_budgets.maximum_history_delivery_retained_bytes,
        ),
        (
            "physical_budget_bytes",
            store_budgets.maximum_physical_store_bytes,
        ),
    ] {
        writeln!(output, "# TYPE leani_store_{name} gauge")?;
        writeln!(output, "leani_store_{name} {value}")?;
    }
    writeln!(
        output,
        "# HELP leani_delivery_total_retained_bytes Exact retained delivery payload bytes by stream class."
    )?;
    writeln!(output, "# TYPE leani_delivery_total_retained_bytes gauge")?;
    writeln!(
        output,
        "leani_delivery_total_retained_bytes{{stream_class=\"all\"}} {}",
        store_metrics.delivery_retained_bytes
    )?;
    writeln!(
        output,
        "leani_delivery_total_retained_bytes{{stream_class=\"history\"}} {}",
        store_metrics.history_delivery_retained_bytes
    )?;
    append_processor_metrics(&mut output, &state).await?;
    append_history_metrics(&mut output, &backfills)?;
    append_historical_material_metrics(&mut output, historical_material)?;
    Ok((
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        output,
    )
        .into_response())
}

fn append_historical_material_metrics(
    output: &mut String,
    metrics: Option<HistoricalMaterialMetrics>,
) -> Result<(), std::fmt::Error> {
    use std::fmt::Write as _;

    let metrics = metrics.unwrap_or_default();
    for (name, kind, help, value) in [
        (
            "acquisitions_started_total",
            "counter",
            "Physical historical material acquisitions started.",
            metrics.acquisitions_started,
        ),
        (
            "requests_coalesced_total",
            "counter",
            "Historical material requests joined to compatible acquisitions.",
            metrics.requests_coalesced,
        ),
        (
            "requests_coalescible_total",
            "counter",
            "Compatible historical requests observed without changing physical execution.",
            metrics.requests_coalescible,
        ),
        (
            "source_frames_total",
            "counter",
            "Physical normalized frames received by historical acquisitions.",
            metrics.physical_frames,
        ),
        (
            "source_bytes_total",
            "counter",
            "Physical normalized bytes received by historical acquisitions.",
            metrics.physical_bytes,
        ),
        (
            "overfetched_frames_total",
            "counter",
            "Physical historical frames read outside every logical processor range.",
            metrics.overfetched_frames,
        ),
        (
            "overfetched_bytes_total",
            "counter",
            "Physical historical bytes read outside every logical processor range.",
            metrics.overfetched_bytes,
        ),
        (
            "logical_frame_deliveries_total",
            "counter",
            "Historical frame deliveries across independent processor jobs.",
            metrics.logical_frame_deliveries,
        ),
        (
            "active_acquisitions",
            "gauge",
            "Historical material acquisitions currently reading a source.",
            metrics.active_acquisitions,
        ),
        (
            "buffer_bytes",
            "gauge",
            "Normalized historical material bytes currently retained in memory.",
            metrics.buffered_bytes,
        ),
    ] {
        writeln!(output, "# HELP leani_history_material_{name} {help}")?;
        writeln!(output, "# TYPE leani_history_material_{name} {kind}")?;
        writeln!(output, "leani_history_material_{name} {value}")?;
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn append_history_metrics(
    output: &mut String,
    jobs: &[BackfillStatus],
) -> Result<(), std::fmt::Error> {
    use std::fmt::Write as _;

    let mut states = BTreeMap::<(&'static str, &'static str), u64>::new();
    let mut progress = BTreeMap::<&'static str, [u64; 3]>::new();
    let mut sources = BTreeMap::<(String, String, String, String), [u64; 9]>::new();
    for job in jobs {
        let owner = match job.owner {
            HistoricalWorkOwner::Materialization => "materialization",
            HistoricalWorkOwner::Subscription => "subscription",
        };
        let state = match job.state {
            BackfillState::WaitingForConsumer => "waiting_for_consumer",
            BackfillState::Queued => "queued",
            BackfillState::Running => "running",
            BackfillState::Backpressured => "backpressured",
            BackfillState::StorageBackpressured => "storage_backpressured",
            BackfillState::Draining => "draining",
            BackfillState::CompleteReclaimable => "complete_reclaimable",
            BackfillState::Completed => "completed",
            BackfillState::Failed => "failed",
            BackfillState::Cancelled => "cancelled",
        };
        let state_count = states.entry((owner, state)).or_default();
        *state_count = state_count.saturating_add(1);
        let owner_progress = progress.entry(owner).or_default();
        owner_progress[0] = owner_progress[0].saturating_add(job.requested_blocks);
        owner_progress[1] = owner_progress[1].saturating_add(job.processed_blocks);
        owner_progress[2] = owner_progress[2].saturating_add(job.remaining_blocks);
        let Some(report) = &job.report else {
            continue;
        };
        for source in &report.sources {
            let totals = sources
                .entry((
                    owner.to_owned(),
                    job.processor.clone(),
                    source.source_id.clone(),
                    source.source_kind.clone(),
                ))
                .or_default();
            totals[0] = totals[0].saturating_add(source.frames_mapped);
            totals[1] = totals[1].saturating_add(source.frames_committed);
            totals[2] = totals[2].saturating_add(source.source_bytes);
            totals[3] = totals[3].saturating_add(source.elapsed_milliseconds);
            totals[4] = totals[4].saturating_add(u64::from(source.attempts));
            totals[5] = totals[5].saturating_add(u64::from(source.failures));
            totals[6] = totals[6].saturating_add(source.physical_source_bytes);
            totals[7] = totals[7].saturating_add(source.reused_source_bytes);
            totals[8] = totals[8].saturating_add(source.coalesced_frames);
        }
    }
    writeln!(
        output,
        "# HELP leani_history_jobs Historical processor jobs by owner and durable state."
    )?;
    writeln!(output, "# TYPE leani_history_jobs gauge")?;
    for (name, help) in [
        ("requested_blocks", "Requested historical blocks."),
        ("committed_blocks", "Durably processed historical blocks."),
        ("remaining_blocks", "Historical blocks remaining."),
    ] {
        writeln!(output, "# HELP leani_historical_{name} {help}")?;
        writeln!(output, "# TYPE leani_historical_{name} gauge")?;
    }
    for owner in ["materialization", "subscription"] {
        for state in [
            "waiting_for_consumer",
            "queued",
            "running",
            "backpressured",
            "storage_backpressured",
            "draining",
            "complete_reclaimable",
            "completed",
            "failed",
            "cancelled",
        ] {
            writeln!(
                output,
                "leani_history_jobs{{owner=\"{owner}\",state=\"{state}\"}} {}",
                states.get(&(owner, state)).copied().unwrap_or(0)
            )?;
        }
        let owner_progress = progress.get(owner).copied().unwrap_or_default();
        for (name, value) in [
            ("requested_blocks", owner_progress[0]),
            ("committed_blocks", owner_progress[1]),
            ("remaining_blocks", owner_progress[2]),
        ] {
            writeln!(
                output,
                "leani_historical_{name}{{owner_kind=\"{owner}\"}} {value}"
            )?;
        }
    }
    for (name, help) in [
        (
            "frames_mapped_total",
            "Normalized historical frames mapped by source.",
        ),
        (
            "frames_committed_total",
            "Historical frames committed by source.",
        ),
        (
            "input_bytes_total",
            "Normalized execution-material bytes consumed by source.",
        ),
        (
            "elapsed_milliseconds_total",
            "Historical attempt wall time accumulated by source.",
        ),
        ("attempts_total", "Historical source attempts."),
        ("failures_total", "Failed historical source attempts."),
        (
            "physical_input_bytes_total",
            "Physical normalized historical bytes attributed once across shared consumers.",
        ),
        (
            "reused_input_bytes_total",
            "Logical normalized historical bytes reused from shared acquisitions.",
        ),
        (
            "coalesced_frames_total",
            "Historical processor frames served by another consumer's acquisition.",
        ),
    ] {
        writeln!(output, "# HELP leani_history_source_{name} {help}")?;
        writeln!(output, "# TYPE leani_history_source_{name} counter")?;
    }
    for ((owner, processor, source, kind), totals) in sources {
        for (name, value) in [
            ("frames_mapped_total", totals[0]),
            ("frames_committed_total", totals[1]),
            ("input_bytes_total", totals[2]),
            ("elapsed_milliseconds_total", totals[3]),
            ("attempts_total", totals[4]),
            ("failures_total", totals[5]),
            ("physical_input_bytes_total", totals[6]),
            ("reused_input_bytes_total", totals[7]),
            ("coalesced_frames_total", totals[8]),
        ] {
            writeln!(
                output,
                "leani_history_source_{name}{{owner=\"{owner}\",processor=\"{processor}\",source=\"{source}\",kind=\"{kind}\"}} {value}"
            )?;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn append_processor_metrics(output: &mut String, state: &ApiState) -> Result<(), ApiError> {
    use std::fmt::Write as _;

    for processor in state.processors.values() {
        let descriptor = processor.descriptor();
        let cursor = state.store.processor_cursor(descriptor).await?;
        let finalized = state.store.finalized_through(descriptor).await?;
        let processor_store = state.store.processor_stats(descriptor).await?;
        let delivery = state.store.delivery_stream_stats(descriptor).await?;
        if let Ok(runtime) = state.store.processor_runtime_state(descriptor).await {
            writeln!(
                output,
                "leani_processor_run_state{{processor=\"{}\",instance=\"{}\",state=\"{}\"}} 1",
                descriptor.id,
                descriptor.instance,
                run_state_name(runtime.state)
            )?;
        }
        if let Some(gap) = state.store.live_lane_gap(descriptor).await? {
            writeln!(
                output,
                "leani_processor_live_gap_first_unapplied_block{{processor=\"{}\",instance=\"{}\"}} {}",
                descriptor.id, descriptor.instance, gap.first_unapplied.number.0
            )?;
            writeln!(
                output,
                "leani_processor_live_gap_required_delivery_bytes{{processor=\"{}\",instance=\"{}\"}} {}",
                descriptor.id, descriptor.instance, gap.required_delivery_bytes
            )?;
        }
        writeln!(
            output,
            "leani_processor_head_block{{processor=\"{}\",instance=\"{}\"}} {}",
            descriptor.id,
            descriptor.instance,
            cursor.map_or(0, |cursor| cursor.block_number.0)
        )?;
        writeln!(
            output,
            "leani_processor_finalized_block{{processor=\"{}\",instance=\"{}\"}} {}",
            descriptor.id,
            descriptor.instance,
            finalized.map_or(0, |block| block.0)
        )?;
        for (name, value) in [
            ("entity_bytes", processor_store.entity_bytes),
            ("state_bytes", processor_store.state_bytes),
            ("index_bytes", processor_store.index_bytes),
            ("undo_bytes", processor_store.undo_bytes),
            ("change_bytes", processor_store.change_bytes),
            ("pending_deltas", processor_store.pending_deltas),
            ("pending_delta_bytes", processor_store.pending_delta_bytes),
            ("processor_artifacts", processor_store.processor_artifacts),
            (
                "processor_artifact_bytes",
                processor_store.processor_artifact_bytes,
            ),
            (
                "processor_artifact_owners",
                processor_store.processor_artifact_owners,
            ),
            (
                "pending_processor_artifacts",
                processor_store.pending_processor_artifacts,
            ),
            (
                "pending_processor_artifact_bytes",
                processor_store.pending_processor_artifact_bytes,
            ),
            ("outbox_records", processor_store.outbox_records),
            (
                "recovery_checkpoint_bytes",
                processor_store.recovery_checkpoint_bytes,
            ),
            (
                "portable_savepoint_bytes",
                processor_store.portable_savepoint_bytes,
            ),
        ] {
            writeln!(
                output,
                "leani_processor_{name}{{processor=\"{}\",instance=\"{}\"}} {value}",
                descriptor.id, descriptor.instance
            )?;
        }
        for (name, value) in [
            ("delivery_live_bytes", delivery.live_bytes),
            (
                "delivery_pruned_through_sequence",
                delivery.pruned_through_sequence,
            ),
            (
                "delivery_format_version",
                u64::from(delivery.format_version),
            ),
        ] {
            writeln!(
                output,
                "leani_processor_{name}{{processor=\"{}\",instance=\"{}\"}} {value}",
                descriptor.id, descriptor.instance
            )?;
        }
        if let Some(watermark) = delivery.required_ack_watermark {
            writeln!(
                output,
                "leani_processor_required_ack_watermark{{processor=\"{}\",instance=\"{}\"}} {watermark}",
                descriptor.id, descriptor.instance
            )?;
        }
        for consumer in state.store.consumers(descriptor).await? {
            let lag = state
                .store
                .consumer_lag(descriptor, &consumer.consumer_id)
                .await?;
            for (name, value) in [
                ("lag_changes", lag.changes),
                ("lag_blocks", lag.blocks),
                ("lag_bytes", lag.bytes),
                ("lag_age_ms", lag.age_ms),
                ("lease_active", u64::from(u8::from(consumer.lease_active))),
                ("acknowledged_sequence", consumer.acknowledged_sequence),
                ("delivered_sequence", consumer.delivered_sequence),
                (
                    "lease_expires_at_unix_ms",
                    consumer.lease_expires_at_unix_ms,
                ),
            ] {
                writeln!(
                    output,
                    "leani_consumer_{name}{{processor=\"{}\",instance=\"{}\",consumer=\"{}\",role=\"{}\"}} {value}",
                    descriptor.id,
                    descriptor.instance,
                    consumer.consumer_id,
                    match consumer.role {
                        ConsumerRole::Required => "required",
                        ConsumerRole::BestEffort => "best_effort",
                    }
                )?;
            }
        }
    }
    Ok(())
}

fn append_network_metrics(
    output: &mut String,
    network: &NetworkTelemetrySnapshot,
) -> Result<(), std::fmt::Error> {
    append_network_supervisor_metrics(output, network)?;
    append_network_request_metrics(output, network)?;
    append_network_peer_lifecycle_metrics(output, network)?;
    append_network_session_metrics(output, network)
}

fn append_network_supervisor_metrics(
    output: &mut String,
    network: &NetworkTelemetrySnapshot,
) -> Result<(), std::fmt::Error> {
    use std::fmt::Write as _;

    writeln!(
        output,
        "# HELP leani_network_supervisor_info Current required network supervisor state."
    )?;
    writeln!(output, "# TYPE leani_network_supervisor_info gauge")?;
    writeln!(
        output,
        "leani_network_supervisor_info{{state=\"{}\"}} 1",
        network.supervisor.state.as_str()
    )?;
    writeln!(
        output,
        "# TYPE leani_network_supervisor_failures_total counter"
    )?;
    writeln!(
        output,
        "leani_network_supervisor_failures_total {}",
        network.supervisor.failures
    )?;
    writeln!(
        output,
        "# TYPE leani_network_supervisor_retry_seconds gauge"
    )?;
    writeln!(
        output,
        "leani_network_supervisor_retry_seconds {}",
        network.supervisor.retry_in_seconds.unwrap_or(0)
    )?;
    Ok(())
}

fn append_network_request_metrics(
    output: &mut String,
    network: &NetworkTelemetrySnapshot,
) -> Result<(), std::fmt::Error> {
    use std::fmt::Write as _;

    writeln!(
        output,
        "# HELP leani_p2p_active_sessions Active persistent P2P managers."
    )?;
    writeln!(output, "# TYPE leani_p2p_active_sessions gauge")?;
    writeln!(
        output,
        "leani_p2p_active_sessions {}",
        network.active_sessions
    )?;
    writeln!(
        output,
        "# HELP leani_p2p_connected_peer_slots Connected peer slots summed across active sessions."
    )?;
    writeln!(output, "# TYPE leani_p2p_connected_peer_slots gauge")?;
    writeln!(
        output,
        "leani_p2p_connected_peer_slots {}",
        network.connected_peer_slots
    )?;
    writeln!(output, "# TYPE leani_p2p_minimum_peer_slots gauge")?;
    writeln!(
        output,
        "leani_p2p_minimum_peer_slots {}",
        network.peer_targets.minimum
    )?;
    writeln!(output, "# TYPE leani_p2p_preferred_peer_slots gauge")?;
    writeln!(
        output,
        "leani_p2p_preferred_peer_slots {}",
        network.peer_targets.preferred
    )?;
    writeln!(output, "# TYPE leani_p2p_max_outbound_peer_slots gauge")?;
    writeln!(
        output,
        "leani_p2p_max_outbound_peer_slots {}",
        network.peer_targets.max_outbound
    )?;
    writeln!(output, "# TYPE leani_p2p_requests_started_total counter")?;
    writeln!(
        output,
        "leani_p2p_requests_started_total {}",
        network.requests.started
    )?;
    writeln!(output, "# TYPE leani_p2p_requests_succeeded_total counter")?;
    writeln!(
        output,
        "leani_p2p_requests_succeeded_total {}",
        network.requests.succeeded
    )?;
    writeln!(output, "# TYPE leani_p2p_requests_timed_out_total counter")?;
    writeln!(
        output,
        "leani_p2p_requests_timed_out_total {}",
        network.requests.timed_out
    )?;
    writeln!(output, "# TYPE leani_p2p_requests_failed_total counter")?;
    writeln!(
        output,
        "leani_p2p_requests_failed_total {}",
        network.requests.failed
    )?;
    writeln!(
        output,
        "# TYPE leani_p2p_request_queue_wait_milliseconds_total counter"
    )?;
    writeln!(
        output,
        "leani_p2p_request_queue_wait_milliseconds_total {}",
        network.requests.queue_wait_milliseconds
    )?;
    Ok(())
}

fn append_network_peer_lifecycle_metrics(
    output: &mut String,
    network: &NetworkTelemetrySnapshot,
) -> Result<(), std::fmt::Error> {
    use std::fmt::Write as _;

    writeln!(
        output,
        "# TYPE leani_p2p_peer_sessions_established_total counter"
    )?;
    writeln!(
        output,
        "leani_p2p_peer_sessions_established_total {}",
        network.peer_lifecycle.established
    )?;
    writeln!(
        output,
        "# TYPE leani_p2p_peer_sessions_disconnected_total counter"
    )?;
    writeln!(
        output,
        "leani_p2p_peer_sessions_disconnected_total {}",
        network.peer_lifecycle.disconnected
    )?;
    writeln!(output, "# TYPE leani_p2p_peer_disconnects_total counter")?;
    for reason in &network.peer_lifecycle.disconnect_reasons {
        writeln!(
            output,
            "leani_p2p_peer_disconnects_total{{reason=\"{}\"}} {}",
            reason.reason.as_str(),
            reason.count
        )?;
    }
    Ok(())
}

fn append_network_session_metrics(
    output: &mut String,
    network: &NetworkTelemetrySnapshot,
) -> Result<(), std::fmt::Error> {
    use std::fmt::Write as _;

    writeln!(output, "# TYPE leani_p2p_session_connected_peers gauge")?;
    writeln!(output, "# TYPE leani_p2p_session_known_peers gauge")?;
    writeln!(output, "# TYPE leani_p2p_session_attempts counter")?;
    writeln!(output, "# TYPE leani_p2p_session_observed_head_block gauge")?;
    for session in network.sessions.iter().filter(|session| session.active) {
        writeln!(
            output,
            "leani_p2p_session_connected_peers{{session=\"{}\",lane=\"{}\",phase=\"{}\"}} {}",
            session.id,
            session.lane.as_str(),
            session.phase.as_str(),
            session.connected_peers
        )?;
        writeln!(
            output,
            "leani_p2p_session_known_peers{{session=\"{}\",lane=\"{}\",phase=\"{}\"}} {}",
            session.id,
            session.lane.as_str(),
            session.phase.as_str(),
            session.known_peers
        )?;
        writeln!(
            output,
            "leani_p2p_session_attempts{{session=\"{}\",lane=\"{}\"}} {}",
            session.id,
            session.lane.as_str(),
            session.attempts
        )?;
        if let Some(head) = session.observed_head_block {
            writeln!(
                output,
                "leani_p2p_session_observed_head_block{{session=\"{}\",lane=\"{}\"}} {}",
                session.id,
                session.lane.as_str(),
                head
            )?;
        }
    }
    Ok(())
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Page<T> {
    data: Vec<T>,
    next_cursor: Option<String>,
    coverage: CoverageResponse,
}

impl<T> Page<T> {
    #[must_use]
    pub fn new(data: Vec<T>, next_cursor: Option<String>, coverage: CoverageResponse) -> Self {
        Self {
            data,
            next_cursor,
            coverage,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BlobsSnapshotEntry {
    block: BlobsBlock,
    transactions: Vec<BlobTransaction>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BlobScheduleResponse {
    name: String,
    activation_block: u64,
    activation_timestamp: u64,
    fork_id: String,
    target_blobs_per_block: u32,
    max_blobs_per_block: u32,
    base_fee_update_fraction: String,
    eip7918: bool,
    source: &'static str,
}

impl From<&BlobFork> for BlobScheduleResponse {
    fn from(value: &BlobFork) -> Self {
        Self {
            name: value.name.clone(),
            activation_block: value.activation_block,
            activation_timestamp: value.activation_timestamp,
            fork_id: format!("0x{}", value.fork_id),
            target_blobs_per_block: value.target_blobs_per_block,
            max_blobs_per_block: value.max_blobs_per_block,
            base_fee_update_fraction: value.base_fee_update_fraction.to_string(),
            eip7918: value.eip7918,
            source: "chain_spec",
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Erc20Balance {
    token: String,
    address: String,
    balance: String,
    as_of_block: u64,
    block_hash: String,
    finality: &'static str,
    coverage_from: u64,
    complete: bool,
    method: String,
}

impl From<&TokenBalanceEntity> for Erc20Balance {
    fn from(value: &TokenBalanceEntity) -> Self {
        Self {
            token: address_hex(value.token),
            address: address_hex(value.address),
            balance: quantity_decimal(value.balance),
            as_of_block: value.as_of_block.0,
            block_hash: hash_hex(value.block_hash),
            finality: finality_name(value.finality),
            coverage_from: value.coverage_from.0,
            complete: value.complete,
            method: value.method.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UniswapPoolPrice {
    pool: String,
    kind: leani_processor_uniswap::PoolKind,
    reserve0: Option<String>,
    reserve1: Option<String>,
    sqrt_price_x96: Option<String>,
    block_number: u64,
    block_hash: String,
    log_index: u32,
    finality: &'static str,
}

impl From<&PoolPriceEntity> for UniswapPoolPrice {
    fn from(value: &PoolPriceEntity) -> Self {
        Self {
            pool: address_hex(value.pool),
            kind: value.kind,
            reserve0: value.reserve0.map(quantity_decimal),
            reserve1: value.reserve1.map(quantity_decimal),
            sqrt_price_x96: value.sqrt_price_x96.map(quantity_decimal),
            block_number: value.block_number.0,
            block_hash: hash_hex(value.block_hash),
            log_index: value.log_index,
            finality: finality_name(value.finality),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BlobsBlock {
    network: String,
    block_number: u64,
    block_hash: String,
    parent_hash: String,
    timestamp: u64,
    size: String,
    blob_count: u32,
    blob_gas_used: String,
    excess_blob_gas: String,
    blob_base_fee: String,
    execution_base_fee: String,
    gas_used: String,
    gas_limit: String,
    execution_eth_burned_wei: String,
    blob_eth_burned_wei: String,
    reserve_fee_wei: Option<String>,
    transaction_count: u32,
    target_blobs_per_block: u32,
    max_blobs_per_block: u32,
    transform_version: u16,
    finality: &'static str,
}

impl From<&BlobsBlockEntity> for BlobsBlock {
    fn from(value: &BlobsBlockEntity) -> Self {
        Self {
            network: value.network.clone(),
            block_number: value.block_number,
            block_hash: hash_hex(value.block_hash),
            parent_hash: hash_hex(value.parent_hash),
            timestamp: value.timestamp,
            size: value.size_bytes.to_string(),
            blob_count: value.blob_count,
            blob_gas_used: value.blob_gas_used.to_string(),
            excess_blob_gas: value.excess_blob_gas.to_string(),
            blob_base_fee: quantity_decimal(value.blob_base_fee),
            execution_base_fee: quantity_decimal(value.execution_base_fee),
            gas_used: value.gas_used.to_string(),
            gas_limit: value.gas_limit.to_string(),
            execution_eth_burned_wei: quantity_decimal(value.execution_burn),
            blob_eth_burned_wei: value
                .blob_burn
                .map_or_else(|| "0".to_owned(), quantity_decimal),
            reserve_fee_wei: value.reserve_fee.map(quantity_decimal),
            transaction_count: value.transaction_count,
            target_blobs_per_block: value.target_blobs_per_block,
            max_blobs_per_block: value.max_blobs_per_block,
            transform_version: value.transform_version,
            finality: finality_name(value.finality),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BlobTransaction {
    network: String,
    block_number: u64,
    block_hash: String,
    tx_hash: String,
    transaction_index: u32,
    sender_address: String,
    blob_versioned_hashes: Vec<String>,
    blob_count: u32,
    total_burned_wei: String,
    execution_burned_wei: String,
    blob_burned_wei: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BlobsBlockChange {
    block: BlobsBlock,
    transactions: Vec<BlobTransaction>,
}

impl From<&BlobsDelta> for BlobsBlockChange {
    fn from(value: &BlobsDelta) -> Self {
        Self {
            block: BlobsBlock::from(&value.block),
            transactions: value
                .transactions
                .iter()
                .map(BlobTransaction::from)
                .collect(),
        }
    }
}

impl From<&BlobTransactionEntity> for BlobTransaction {
    fn from(value: &BlobTransactionEntity) -> Self {
        Self {
            network: value.network.clone(),
            block_number: value.block_number,
            block_hash: hash_hex(value.block_hash),
            tx_hash: format!("0x{}", hex::encode(value.transaction_hash.0)),
            transaction_index: value.transaction_index,
            sender_address: format!("0x{}", hex::encode(value.sender.0)),
            blob_versioned_hashes: value
                .blob_versioned_hashes
                .iter()
                .copied()
                .map(hash_hex)
                .collect(),
            blob_count: value.blob_count,
            total_burned_wei: quantity_decimal(value.total_burn),
            execution_burned_wei: quantity_decimal(value.execution_burn),
            blob_burned_wei: quantity_decimal(value.blob_burn),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ChangesQuery {
    after: Option<String>,
    limit: Option<usize>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChangeHead {
    earliest_sequence: Option<String>,
    latest_sequence: Option<String>,
    cursor: Option<String>,
}

async fn change_head(
    State(state): State<ApiState>,
    Path(processor): Path<String>,
) -> Result<Json<ChangeHead>, ApiError> {
    let processor = configured_processor(&state, &processor)?;
    let bounds = state.store.change_bounds(processor.descriptor()).await?;
    let cursor = bounds
        .as_ref()
        .map(|bounds| {
            encode_cursor(&ApiCursor {
                version: 2,
                epoch: state.store.epoch(),
                chain_id: state.config.chain_id.0,
                processor_id: processor.descriptor().instance.to_string(),
                processor_version: processor.descriptor().version.to_string(),
                sequence: bounds.latest,
            })
        })
        .transpose()?;
    Ok(Json(ChangeHead {
        earliest_sequence: bounds.as_ref().map(|bounds| bounds.earliest.to_string()),
        latest_sequence: bounds.as_ref().map(|bounds| bounds.latest.to_string()),
        cursor,
    }))
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CreateConsumerRequest {
    id: String,
    role: ConsumerRole,
    start: ConsumerStartRequest,
    lease_ttl_seconds: u64,
    credential: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "position", rename_all = "snake_case", deny_unknown_fields)]
enum ConsumerStartRequest {
    EarliestRetained,
    CurrentHead,
    Cursor { cursor: String },
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ConsumerResponse {
    id: String,
    processor_instance: String,
    stream_id: String,
    role: ConsumerRole,
    state: leani_store_sqlite::ConsumerState,
    acknowledged_sequence: String,
    delivered_sequence: String,
    acknowledged_cursor: String,
    delivered_cursor: String,
    lease_generation: String,
    lease_ttl_ms: String,
    lease_expires_at_unix_ms: String,
    lease_active: bool,
    lag_changes: String,
    lag_blocks: String,
    lag_bytes: String,
    lag_age_ms: String,
    created_at_unix_ms: String,
    updated_at_unix_ms: String,
}

#[derive(Clone, Debug, Serialize)]
struct ConsumerListResponse {
    data: Vec<ConsumerResponse>,
}

async fn create_consumer(
    State(state): State<ApiState>,
    Path(processor): Path<String>,
    Json(request): Json<CreateConsumerRequest>,
) -> Result<(StatusCode, Json<ConsumerResponse>), ApiError> {
    let processor = configured_processor(&state, &processor)?;
    let start = match request.start {
        ConsumerStartRequest::EarliestRetained => ConsumerStartPosition::EarliestRetained,
        ConsumerStartRequest::CurrentHead => ConsumerStartPosition::CurrentHead,
        ConsumerStartRequest::Cursor { cursor } => ConsumerStartPosition::After(
            decode_change_cursor(&state, processor.as_ref(), &cursor)?.sequence,
        ),
    };
    let consumer = state
        .store
        .create_consumer_with_credential(
            processor.descriptor(),
            &request.id,
            request.role,
            start,
            Duration::from_secs(request.lease_ttl_seconds),
            &request.credential,
        )
        .await?;
    Ok((
        StatusCode::CREATED,
        Json(consumer_response(&state, processor.as_ref(), consumer).await?),
    ))
}

async fn list_consumers(
    State(state): State<ApiState>,
    Path(processor): Path<String>,
) -> Result<Json<ConsumerListResponse>, ApiError> {
    let processor = configured_processor(&state, &processor)?;
    let mut data = Vec::new();
    for consumer in state.store.consumers(processor.descriptor()).await? {
        data.push(consumer_response(&state, processor.as_ref(), consumer).await?);
    }
    Ok(Json(ConsumerListResponse { data }))
}

async fn inspect_consumer(
    State(state): State<ApiState>,
    Path((processor, consumer)): Path<(String, String)>,
) -> Result<Json<ConsumerResponse>, ApiError> {
    let processor = configured_processor(&state, &processor)?;
    let consumer = state
        .store
        .consumer(processor.descriptor(), &consumer)
        .await?
        .ok_or_else(|| ApiError::not_found("durable consumer is not registered"))?;
    Ok(Json(
        consumer_response(&state, processor.as_ref(), consumer).await?,
    ))
}

async fn renew_consumer(
    State(state): State<ApiState>,
    Path((processor, consumer)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<Json<ConsumerResponse>, ApiError> {
    let processor = configured_processor(&state, &processor)?;
    authorize_consumer_scope(&state, processor.as_ref(), &consumer, &headers).await?;
    let consumer = state
        .store
        .renew_consumer(processor.descriptor(), &consumer)
        .await?;
    Ok(Json(
        consumer_response(&state, processor.as_ref(), consumer).await?,
    ))
}

async fn revoke_consumer(
    State(state): State<ApiState>,
    Path((processor, consumer)): Path<(String, String)>,
) -> Result<Json<ConsumerResponse>, ApiError> {
    let processor = configured_processor(&state, &processor)?;
    let consumer = state
        .store
        .revoke_consumer(processor.descriptor(), &consumer)
        .await?;
    Ok(Json(
        consumer_response(&state, processor.as_ref(), consumer).await?,
    ))
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ConsumerChangesQuery {
    limit: Option<usize>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ConsumerStreamBatchingQuery {
    target_encoded_bytes: Option<u64>,
    maximum_encoded_bytes: Option<u64>,
    maximum_events: Option<u64>,
    maximum_processed_blocks: Option<u64>,
    maximum_delay_ms: Option<u64>,
}

async fn consumer_changes(
    State(state): State<ApiState>,
    Path((processor, consumer)): Path<(String, String)>,
    Query(query): Query<ConsumerChangesQuery>,
    headers: HeaderMap,
) -> Result<Json<Page<ChangeEnvelope>>, ApiError> {
    let processor = configured_processor(&state, &processor)?;
    authorize_consumer_scope(&state, processor.as_ref(), &consumer, &headers).await?;
    let limit = page_limit(&state, query.limit)?;
    let records = state
        .store
        .consumer_changes(
            processor.descriptor(),
            state.config.chain_id,
            &consumer,
            limit,
        )
        .await?;
    let coverage = coverage(&state, processor.as_ref(), None).await?;
    let data = records
        .into_iter()
        .map(|record| change_envelope(&state, processor.as_ref(), record))
        .collect::<Result<Vec<_>, _>>()?;
    let next_cursor = data.last().map(|event| event.cursor.clone());
    Ok(Json(Page {
        data,
        next_cursor,
        coverage,
    }))
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AcknowledgeConsumerRequest {
    cursor: String,
}

async fn acknowledge_consumer(
    State(state): State<ApiState>,
    Path((processor, consumer)): Path<(String, String)>,
    headers: HeaderMap,
    Json(request): Json<AcknowledgeConsumerRequest>,
) -> Result<Json<ConsumerResponse>, ApiError> {
    let processor = configured_processor(&state, &processor)?;
    authorize_consumer_scope(&state, processor.as_ref(), &consumer, &headers).await?;
    let sequence = decode_change_cursor(&state, processor.as_ref(), &request.cursor)?.sequence;
    let consumer = state
        .store
        .acknowledge_consumer(processor.descriptor(), &consumer, sequence)
        .await?;
    Ok(Json(
        consumer_response(&state, processor.as_ref(), consumer).await?,
    ))
}

async fn backfill_consumer_changes(
    State(state): State<ApiState>,
    Path((subscription, consumer)): Path<(String, String)>,
    Query(query): Query<ConsumerChangesQuery>,
    headers: HeaderMap,
) -> Result<Json<Page<ChangeEnvelope>>, ApiError> {
    let (processor, stream_id, _) = backfill_delivery_scope(&state, &subscription).await?;
    authorize_consumer_scope_in_stream(&state, processor.as_ref(), &stream_id, &consumer, &headers)
        .await?;
    let limit = page_limit(&state, query.limit)?;
    let records = state
        .store
        .consumer_changes_in_stream(
            processor.descriptor(),
            &stream_id,
            state.config.chain_id,
            &consumer,
            limit,
        )
        .await?;
    let coverage = coverage(&state, processor.as_ref(), None).await?;
    let data = records
        .into_iter()
        .map(|record| change_envelope_in_stream(&state, processor.as_ref(), &stream_id, record))
        .collect::<Result<Vec<_>, _>>()?;
    let next_cursor = data.last().map(|event| event.cursor.clone());
    Ok(Json(Page {
        data,
        next_cursor,
        coverage,
    }))
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ConsumerStreamHello {
    #[serde(rename = "type")]
    record_type: &'static str,
    api_version: &'static str,
    chain_id: u64,
    processor: ProcessorSummary,
    subscription_id: String,
    stream_id: String,
    stream_kind: &'static str,
    publication_revision: String,
    ranges: Vec<BackfillRange>,
    acknowledged_cursor: String,
    store_epoch: String,
    heartbeat_interval_ms: String,
    session_token: String,
    session_expires_at_unix_ms: String,
    lease_ttl_ms: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ConsumerStreamBatch {
    #[serde(rename = "type")]
    record_type: &'static str,
    stream_id: String,
    origin_kind: String,
    origin_id: String,
    publication_revision: String,
    from_block: u64,
    through_block: u64,
    processed_block_count: String,
    domain_change_count: String,
    progress_unit_count: String,
    raw_payload_bytes: String,
    uncompressed_encoded_bytes: String,
    transmitted_bytes: String,
    build_delay_ms: String,
    first_cursor: Option<String>,
    last_cursor: Option<String>,
    acknowledgeable_cursor: String,
    changes: Vec<ChangeEnvelope>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ConsumerStreamCompletion {
    #[serde(rename = "type")]
    record_type: &'static str,
    subscription_id: String,
    stream_id: String,
    ranges: Vec<BackfillRange>,
    through_block: u64,
    cursor: String,
    mode: BackfillExecutionMode,
    disposition: leani_store_sqlite::BackfillCompletionDisposition,
    requested_block_count: String,
    covered_before_request_block_count: String,
    covered_before_request_ranges: Vec<BackfillRange>,
    newly_processed_block_count: String,
    republished_block_count: String,
    domain_change_count: String,
    preexisting_coverage_skipped: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    repair_hint: Option<&'static str>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ConsumerStreamHeartbeat {
    #[serde(rename = "type")]
    record_type: &'static str,
    emitted_at: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ConsumerStreamFailure {
    #[serde(rename = "type")]
    record_type: &'static str,
    code: &'static str,
    message: String,
    earliest_available_sequence: Option<String>,
    latest_available_sequence: Option<String>,
}

#[derive(Debug)]
struct ConsumerSessionGuard {
    store: SqliteStore,
    stream_id: String,
    consumer_id: String,
    generation: u64,
}

impl Drop for ConsumerSessionGuard {
    fn drop(&mut self) {
        let store = self.store.clone();
        let stream_id = self.stream_id.clone();
        let consumer_id = self.consumer_id.clone();
        let generation = self.generation;
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = store
                    .release_consumer_session_in_stream(&stream_id, &consumer_id, generation)
                    .await;
            });
        }
    }
}

struct BackfillConsumerStreamState {
    state: ApiState,
    processor: Arc<dyn Processor>,
    subscription_id: String,
    ranges: Vec<BackfillRange>,
    batch_limits: DeliveryBatchLimits,
    gzip: bool,
    stream_id: String,
    consumer_id: String,
    fetch_after: u64,
    fetched: VecDeque<ChangeRecord>,
    pending_progress_unit: Vec<ChangeRecord>,
    ready_progress_unit: Option<HistoryProgressUnit>,
    pending_batch: Option<PendingHistoryBatch>,
    pending_completion: Option<ChangeRecord>,
    completion_sequence: Option<u64>,
    terminal: bool,
    lease: ConsumerSessionGuard,
}

struct HistoryProgressUnit {
    origin: leani_store_sqlite::DeliveryOrigin,
    from_block: u64,
    through_block: u64,
    processed_blocks: u64,
    domain_changes: u64,
    raw_payload_bytes: u64,
    encoded_change_bytes: u64,
    acknowledgeable_cursor: String,
    changes: Vec<ChangeEnvelope>,
}

struct PendingHistoryBatch {
    started: Instant,
    origin: leani_store_sqlite::DeliveryOrigin,
    from_block: u64,
    through_block: u64,
    processed_blocks: u64,
    domain_changes: u64,
    progress_units: u64,
    raw_payload_bytes: u64,
    encoded_change_bytes: u64,
    acknowledgeable_cursor: String,
    changes: Vec<ChangeEnvelope>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct LiveConsumerStreamHello {
    #[serde(rename = "type")]
    record_type: &'static str,
    api_version: &'static str,
    chain_id: u64,
    processor: ProcessorSummary,
    stream_id: String,
    stream_kind: &'static str,
    acknowledged_cursor: String,
    store_epoch: String,
    heartbeat_interval_ms: String,
    session_token: String,
    session_expires_at_unix_ms: String,
    lease_ttl_ms: String,
}

struct LiveConsumerStreamState {
    state: ApiState,
    processor: Arc<dyn Processor>,
    stream_id: String,
    consumer_id: String,
    fetch_after: u64,
    fetched: VecDeque<ChangeRecord>,
    gzip: bool,
    terminal: bool,
    lease: ConsumerSessionGuard,
}

async fn stream_live_consumer(
    State(state): State<ApiState>,
    Path((processor, consumer)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let gzip = accepts_gzip(&headers)
        && state.config.live_batch_limits.compression == DeliveryCompression::Gzip;
    let (processor, stream_id) = live_delivery_scope(&state, &processor).await?;
    authorize_consumer_scope_in_stream(&state, processor.as_ref(), &stream_id, &consumer, &headers)
        .await?;
    let durable_consumer = state
        .store
        .consumer_in_stream(processor.descriptor(), &stream_id, &consumer)
        .await?
        .ok_or_else(|| ApiError::not_found("durable consumer is not registered"))?;
    let lease = state
        .store
        .acquire_consumer_session_in_stream(processor.descriptor(), &stream_id, &consumer)
        .await?;
    let session_token = encode_consumer_session_token(
        &state,
        processor.as_ref(),
        &stream_id,
        &consumer,
        lease.generation,
    )?;
    let hello = ndjson_line(&LiveConsumerStreamHello {
        record_type: "hello",
        api_version: API_VERSION,
        chain_id: state.config.chain_id.0,
        processor: processor_summary(&state, processor.as_ref()),
        stream_id: stream_id.clone(),
        stream_kind: "live",
        acknowledged_cursor: encode_stream_cursor(
            &state,
            processor.as_ref(),
            &stream_id,
            durable_consumer.acknowledged_sequence,
        )?,
        store_epoch: hex::encode(state.store.epoch()),
        heartbeat_interval_ms: state.config.heartbeat_interval.as_millis().to_string(),
        session_token,
        session_expires_at_unix_ms: lease.expires_at_unix_ms.to_string(),
        lease_ttl_ms: durable_consumer.lease_ttl_ms.to_string(),
    })?;
    let stream_state = LiveConsumerStreamState {
        state: state.clone(),
        processor,
        stream_id: stream_id.clone(),
        consumer_id: consumer.clone(),
        fetch_after: durable_consumer.acknowledged_sequence,
        fetched: VecDeque::new(),
        gzip,
        terminal: false,
        lease: ConsumerSessionGuard {
            store: state.store.clone(),
            stream_id,
            consumer_id: consumer,
            generation: lease.generation,
        },
    };
    let body_stream = stream::once(std::future::ready(Ok::<Bytes, Infallible>(hello)))
        .chain(stream::unfold(stream_state, poll_live_consumer));
    let mut response = Response::new(buffered_delivery_body(
        body_stream,
        gzip,
        state.config.live_batch_limits,
    ));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/x-ndjson"),
    );
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    add_compression_headers(&mut response, gzip);
    Ok(response)
}

async fn acknowledge_live_consumer(
    State(state): State<ApiState>,
    Path((processor, consumer)): Path<(String, String)>,
    headers: HeaderMap,
    Json(request): Json<AcknowledgeConsumerRequest>,
) -> Result<Json<ConsumerResponse>, ApiError> {
    let (processor, stream_id) = live_delivery_scope(&state, &processor).await?;
    authorize_consumer_scope_in_stream(&state, processor.as_ref(), &stream_id, &consumer, &headers)
        .await?;
    let sequence =
        decode_stream_change_cursor(&state, processor.as_ref(), &stream_id, &request.cursor)?
            .sequence;
    let generation = consumer_session_generation(
        &state,
        processor.as_ref(),
        &stream_id,
        &consumer,
        &headers,
        true,
    )?
    .expect("required consumer session generation");
    let consumer = state
        .store
        .acknowledge_consumer_session_in_stream(
            processor.descriptor(),
            &stream_id,
            &consumer,
            generation,
            sequence,
        )
        .await?;
    Ok(Json(
        consumer_response_in_stream(&state, processor.as_ref(), consumer).await?,
    ))
}

async fn renew_live_consumer(
    State(state): State<ApiState>,
    Path((processor, consumer)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<Json<ConsumerResponse>, ApiError> {
    let (processor, stream_id) = live_delivery_scope(&state, &processor).await?;
    authorize_consumer_scope_in_stream(&state, processor.as_ref(), &stream_id, &consumer, &headers)
        .await?;
    let generation = consumer_session_generation(
        &state,
        processor.as_ref(),
        &stream_id,
        &consumer,
        &headers,
        true,
    )?
    .expect("required consumer session generation");
    state
        .store
        .renew_consumer_session_in_stream(&stream_id, &consumer, generation)
        .await?;
    let consumer = state
        .store
        .consumer_in_stream(processor.descriptor(), &stream_id, &consumer)
        .await?
        .ok_or_else(|| ApiError::not_found("durable consumer is not registered"))?;
    Ok(Json(
        consumer_response_in_stream(&state, processor.as_ref(), consumer).await?,
    ))
}

async fn release_live_consumer(
    State(state): State<ApiState>,
    Path((processor, consumer)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError> {
    let (processor, stream_id) = live_delivery_scope(&state, &processor).await?;
    authorize_consumer_scope_in_stream(&state, processor.as_ref(), &stream_id, &consumer, &headers)
        .await?;
    let generation = consumer_session_generation(
        &state,
        processor.as_ref(),
        &stream_id,
        &consumer,
        &headers,
        true,
    )?
    .expect("required consumer session generation");
    state
        .store
        .release_consumer_session_in_stream(&stream_id, &consumer, generation)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn live_delivery_scope(
    state: &ApiState,
    processor: &str,
) -> Result<(Arc<dyn Processor>, String), ApiError> {
    let processor = configured_processor(state, processor)?;
    let stream_id = default_delivery_stream_id(processor.descriptor());
    let stream = state
        .store
        .delivery_stream(&stream_id)
        .await?
        .ok_or_else(|| ApiError::not_found("processor delivery stream does not exist"))?;
    if stream.kind != DeliveryStreamKind::Live {
        return Err(ApiError::conflict(
            "processor_has_no_split_live_stream",
            "this processor uses canonical delivery",
        ));
    }
    Ok((processor, stream_id))
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct LiveLaneResetResponse {
    processor: String,
    state: ProcessorRunState,
    reason: Option<String>,
    first_unapplied_block: u64,
    first_unapplied_hash: String,
    required_delivery_bytes: String,
}

async fn reset_live_lane(
    State(state): State<ApiState>,
    Path(processor): Path<String>,
) -> Result<Json<LiveLaneResetResponse>, ApiError> {
    let (processor, _) = live_delivery_scope(&state, &processor).await?;
    let gap = match state
        .store
        .reset_failed_live_lane(processor.descriptor())
        .await
    {
        Ok(gap) => gap,
        Err(
            error @ StoreError::DeliveryItemTooLarge {
                observed_bytes,
                maximum_bytes,
                ..
            },
        ) => {
            let mut response =
                ApiError::conflict("single_block_exceeds_delivery_limit", &error.to_string());
            response.details = Some(json!({
                "requiredBytes": observed_bytes.to_string(),
                "maximumBytes": maximum_bytes.to_string()
            }));
            return Err(response);
        }
        Err(error) => return Err(error.into()),
    };
    let runtime = state
        .store
        .processor_runtime_state(processor.descriptor())
        .await?;
    Ok(Json(LiveLaneResetResponse {
        processor: processor.descriptor().instance.to_string(),
        state: runtime.state,
        reason: runtime.reason,
        first_unapplied_block: gap.first_unapplied.number.0,
        first_unapplied_hash: hash_hex(gap.first_unapplied.hash),
        required_delivery_bytes: gap.required_delivery_bytes.to_string(),
    }))
}

#[allow(clippy::too_many_lines)]
async fn poll_live_consumer(
    mut stream_state: LiveConsumerStreamState,
) -> Option<(Result<Bytes, Infallible>, LiveConsumerStreamState)> {
    if stream_state.terminal {
        return None;
    }
    loop {
        if let Some(first) = stream_state.fetched.front().cloned() {
            let mut records = Vec::new();
            while stream_state
                .fetched
                .front()
                .is_some_and(|record| same_live_commit(&first, record))
            {
                records.push(
                    stream_state
                        .fetched
                        .pop_front()
                        .expect("front record exists"),
                );
            }
            let live_limits = stream_state.state.config.live_batch_limits;
            if u64::try_from(records.len()).unwrap_or(u64::MAX) > live_limits.maximum_events {
                stream_state.terminal = true;
                return Some((
                    Ok(ndjson_failure(
                        "single_block_exceeds_delivery_batch_limit",
                        format!(
                            "one live block contains more than {} delivery records",
                            live_limits.maximum_events
                        ),
                        None,
                        None,
                    )),
                    stream_state,
                ));
            }
            let domain_change_count = records
                .iter()
                .filter(|record| !record.change.kind.starts_with("system."))
                .count();
            let encoded_bytes = records.iter().fold(0_u64, |total, record| {
                total.saturating_add(
                    u64::try_from(record.change.key.len() + record.change.payload.len())
                        .unwrap_or(u64::MAX),
                )
            });
            let changes = records
                .into_iter()
                .map(|record| {
                    change_envelope_in_stream(
                        &stream_state.state,
                        stream_state.processor.as_ref(),
                        &stream_state.stream_id,
                        record,
                    )
                })
                .collect::<Result<Vec<_>, _>>();
            let changes = match changes {
                Ok(changes) => changes,
                Err(error) => {
                    stream_state.terminal = true;
                    return Some((
                        Ok(ndjson_failure(
                            "delivery_encoding_failed",
                            error.to_string(),
                            None,
                            None,
                        )),
                        stream_state,
                    ));
                }
            };
            let encoded_change_bytes = changes.iter().fold(0_u64, |total, change| {
                total.saturating_add(
                    serde_json::to_vec(change)
                        .ok()
                        .and_then(|bytes| u64::try_from(bytes.len()).ok())
                        .unwrap_or(u64::MAX),
                )
            });
            let encoded_change_array_bytes =
                encoded_change_array_bytes(encoded_change_bytes, changes.len());
            if encoded_change_array_bytes > live_limits.maximum_encoded_bytes {
                stream_state.terminal = true;
                return Some((
                    Ok(ndjson_failure(
                        "single_block_exceeds_delivery_batch_limit",
                        format!(
                            "one live block requires at least {encoded_change_array_bytes} encoded bytes; maximum is {}",
                            live_limits.maximum_encoded_bytes
                        ),
                        None,
                        None,
                    )),
                    stream_state,
                ));
            }
            let first_cursor = changes
                .first()
                .map_or_else(String::new, |change| change.cursor.clone());
            let last_cursor = changes
                .last()
                .map_or_else(String::new, |change| change.cursor.clone());
            let encoded_event_payload = match serde_json::to_vec(&changes) {
                Ok(encoded) => encoded,
                Err(error) => {
                    stream_state.terminal = true;
                    return Some((
                        Ok(ndjson_failure(
                            "delivery_encoding_failed",
                            error.to_string(),
                            None,
                            None,
                        )),
                        stream_state,
                    ));
                }
            };
            let transmitted_event_bytes = if stream_state.gzip {
                gzip_member(&encoded_event_payload).map_or(u64::MAX, |bytes| {
                    u64::try_from(bytes.len()).unwrap_or(u64::MAX)
                })
            } else {
                u64::try_from(encoded_event_payload.len()).unwrap_or(u64::MAX)
            };
            let batch = ConsumerStreamBatch {
                record_type: "batch",
                stream_id: stream_state.stream_id.clone(),
                origin_kind: first.origin.kind.as_str().to_owned(),
                origin_id: first.origin.id.clone(),
                publication_revision: first.origin.publication_revision.to_string(),
                from_block: first.block.number.0,
                through_block: first.block.number.0,
                processed_block_count: "1".to_owned(),
                domain_change_count: domain_change_count.to_string(),
                progress_unit_count: "1".to_owned(),
                raw_payload_bytes: encoded_bytes.to_string(),
                uncompressed_encoded_bytes: encoded_event_payload.len().to_string(),
                transmitted_bytes: transmitted_event_bytes.to_string(),
                build_delay_ms: "0".to_owned(),
                first_cursor: Some(first_cursor),
                acknowledgeable_cursor: last_cursor.clone(),
                last_cursor: Some(last_cursor),
                changes,
            };
            if !live_stream_session_is_current(&stream_state).await {
                stream_state.terminal = true;
                return Some((
                    Ok(ndjson_failure(
                        "consumer_session_lost",
                        "consumer streaming lease is no longer current".to_owned(),
                        None,
                        None,
                    )),
                    stream_state,
                ));
            }
            return Some((Ok(ndjson_line_or_error(&batch)), stream_state));
        }

        let fetch_limit = usize::try_from(
            stream_state
                .state
                .config
                .live_batch_limits
                .maximum_events
                .saturating_add(1),
        )
        .unwrap_or(10_000)
        .min(10_000);
        match stream_state
            .state
            .store
            .consumer_changes_after_in_stream(
                stream_state.processor.descriptor(),
                &stream_state.stream_id,
                stream_state.state.config.chain_id,
                &stream_state.consumer_id,
                stream_state.fetch_after,
                fetch_limit,
            )
            .await
        {
            Ok(records) if !records.is_empty() => {
                stream_state.fetch_after = records
                    .last()
                    .map_or(stream_state.fetch_after, |record| record.cursor.sequence);
                stream_state.fetched.extend(records);
            }
            Ok(_) => {
                tokio::select! {
                    () = stream_state.state.store.wait_for_delivery_changes() => {}
                    () = tokio::time::sleep(stream_state.state.config.heartbeat_interval) => {
                        if !live_stream_session_is_current(&stream_state).await {
                            stream_state.terminal = true;
                            return Some((
                                Ok(ndjson_failure(
                                    "consumer_session_lost",
                                    "consumer streaming lease is no longer current".to_owned(),
                                    None,
                                    None,
                                )),
                                stream_state,
                            ));
                        }
                        return Some((
                            Ok(ndjson_line_or_error(&ConsumerStreamHeartbeat {
                                record_type: "heartbeat",
                                emitted_at: unix_ms_rfc3339(current_unix_ms()),
                            })),
                            stream_state,
                        ));
                    }
                }
            }
            Err(StoreError::ConsumerResetRequired {
                earliest_available,
                latest_available,
            }) => {
                stream_state.terminal = true;
                return Some((
                    Ok(ndjson_failure(
                        "reset_required",
                        "consumer cursor predates retained delivery data".to_owned(),
                        earliest_available,
                        latest_available,
                    )),
                    stream_state,
                ));
            }
            Err(error) => {
                stream_state.terminal = true;
                return Some((
                    Ok(ndjson_failure(
                        "delivery_failed",
                        error.to_string(),
                        None,
                        None,
                    )),
                    stream_state,
                ));
            }
        }
    }
}

fn same_live_commit(first: &ChangeRecord, candidate: &ChangeRecord) -> bool {
    first.block == candidate.block
        && first.finality == candidate.finality
        && first.direction == candidate.direction
        && first.origin == candidate.origin
}

async fn live_stream_session_is_current(stream_state: &LiveConsumerStreamState) -> bool {
    stream_state
        .state
        .store
        .consumer_session_is_current_in_stream(
            &stream_state.stream_id,
            &stream_state.consumer_id,
            stream_state.lease.generation,
        )
        .await
        .unwrap_or(false)
}

fn stricter_stream_batch_limits(
    persisted: DeliveryBatchLimits,
    requested: ConsumerStreamBatchingQuery,
) -> Result<DeliveryBatchLimits, ApiError> {
    let persisted_delay_ms = u64::try_from(persisted.maximum_delay.as_millis())
        .map_err(|_| ApiError::invalid("persisted delivery delay exceeds the wire range"))?;
    let effective = DeliveryBatchLimits {
        target_encoded_bytes: requested
            .target_encoded_bytes
            .unwrap_or(persisted.target_encoded_bytes),
        maximum_encoded_bytes: requested
            .maximum_encoded_bytes
            .unwrap_or(persisted.maximum_encoded_bytes),
        maximum_events: requested.maximum_events.unwrap_or(persisted.maximum_events),
        maximum_processed_blocks: requested
            .maximum_processed_blocks
            .unwrap_or(persisted.maximum_processed_blocks),
        maximum_delay: Duration::from_millis(
            requested.maximum_delay_ms.unwrap_or(persisted_delay_ms),
        ),
        ..persisted
    };
    if !effective.is_valid()
        || effective.target_encoded_bytes > persisted.target_encoded_bytes
        || effective.maximum_encoded_bytes > persisted.maximum_encoded_bytes
        || effective.maximum_events > persisted.maximum_events
        || effective.maximum_processed_blocks > persisted.maximum_processed_blocks
        || effective.maximum_delay > persisted.maximum_delay
    {
        return Err(ApiError::invalid(
            "stream batching overrides must be positive, internally consistent, and no looser than the durable subscription limits",
        ));
    }
    Ok(effective)
}

#[allow(clippy::too_many_lines)]
async fn stream_backfill_consumer(
    State(state): State<ApiState>,
    Path((subscription, consumer)): Path<(String, String)>,
    Query(query): Query<ConsumerStreamBatchingQuery>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let accepts_gzip = accepts_gzip(&headers);
    let (processor, stream_id, status) = backfill_delivery_scope(&state, &subscription).await?;
    let persisted_batch_limits =
        status
            .batching
            .map_or(state.config.history_batch_limits, |batching| {
                DeliveryBatchLimits {
                    target_encoded_bytes: batching.target_encoded_bytes,
                    maximum_encoded_bytes: batching.maximum_encoded_bytes,
                    maximum_events: batching.maximum_events,
                    maximum_processed_blocks: batching.maximum_processed_blocks,
                    maximum_delay: Duration::from_millis(batching.maximum_delay_ms),
                    maximum_buffered_batches: usize::try_from(batching.maximum_buffered_batches)
                        .unwrap_or(usize::MAX),
                    maximum_buffered_bytes: batching.maximum_buffered_bytes,
                    compression: batching.compression,
                }
            });
    let batch_limits = stricter_stream_batch_limits(persisted_batch_limits, query)?;
    let gzip = accepts_gzip && batch_limits.compression == DeliveryCompression::Gzip;
    authorize_consumer_scope_in_stream(&state, processor.as_ref(), &stream_id, &consumer, &headers)
        .await?;
    let durable_consumer = state
        .store
        .consumer_in_stream(processor.descriptor(), &stream_id, &consumer)
        .await?
        .ok_or_else(|| ApiError::not_found("durable consumer is not registered"))?;
    let lease = state
        .store
        .acquire_consumer_session_in_stream(processor.descriptor(), &stream_id, &consumer)
        .await?;
    let session_token = encode_consumer_session_token(
        &state,
        processor.as_ref(),
        &stream_id,
        &consumer,
        lease.generation,
    )?;
    let hello = ConsumerStreamHello {
        record_type: "hello",
        api_version: API_VERSION,
        chain_id: state.config.chain_id.0,
        processor: processor_summary(&state, processor.as_ref()),
        subscription_id: subscription.clone(),
        stream_id: stream_id.clone(),
        stream_kind: "backfill",
        publication_revision: status
            .publication_revision
            .clone()
            .unwrap_or_else(|| "0".to_owned()),
        ranges: status.ranges.clone(),
        acknowledged_cursor: encode_stream_cursor(
            &state,
            processor.as_ref(),
            &stream_id,
            durable_consumer.acknowledged_sequence,
        )?,
        store_epoch: hex::encode(state.store.epoch()),
        heartbeat_interval_ms: state.config.heartbeat_interval.as_millis().to_string(),
        session_token,
        session_expires_at_unix_ms: lease.expires_at_unix_ms.to_string(),
        lease_ttl_ms: durable_consumer.lease_ttl_ms.to_string(),
    };
    let hello = ndjson_line(&hello)?;
    let stream_state = BackfillConsumerStreamState {
        state: state.clone(),
        processor,
        subscription_id: subscription,
        ranges: status.ranges,
        batch_limits,
        gzip,
        stream_id: stream_id.clone(),
        consumer_id: consumer.clone(),
        fetch_after: durable_consumer.acknowledged_sequence,
        fetched: VecDeque::new(),
        pending_progress_unit: Vec::new(),
        ready_progress_unit: None,
        pending_batch: None,
        pending_completion: None,
        completion_sequence: None,
        terminal: false,
        lease: ConsumerSessionGuard {
            store: state.store.clone(),
            stream_id,
            consumer_id: consumer,
            generation: lease.generation,
        },
    };
    let batch_limits = stream_state.batch_limits;
    let body_stream = stream::once(std::future::ready(Ok::<Bytes, Infallible>(hello)))
        .chain(stream::unfold(stream_state, poll_backfill_consumer));
    let mut response = Response::new(buffered_delivery_body(body_stream, gzip, batch_limits));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/x-ndjson"),
    );
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    add_compression_headers(&mut response, gzip);
    Ok(response)
}

#[allow(clippy::too_many_lines)]
async fn poll_backfill_consumer(
    mut stream_state: BackfillConsumerStreamState,
) -> Option<(Result<Bytes, Infallible>, BackfillConsumerStreamState)> {
    if stream_state.terminal {
        return None;
    }
    loop {
        if let Some(completion_sequence) = stream_state.completion_sequence {
            let acknowledged = stream_state
                .state
                .store
                .consumer_in_stream(
                    stream_state.processor.descriptor(),
                    &stream_state.stream_id,
                    &stream_state.consumer_id,
                )
                .await
                .ok()
                .flatten()
                .is_some_and(|consumer| consumer.acknowledged_sequence >= completion_sequence);
            if acknowledged {
                stream_state.terminal = true;
                return None;
            }
            tokio::select! {
                () = stream_state.state.store.wait_for_delivery_capacity_change() => {}
                () = tokio::time::sleep(stream_state.state.config.heartbeat_interval) => {
                    if !backfill_stream_session_is_current(&stream_state).await {
                        stream_state.terminal = true;
                        return Some((
                            Ok(ndjson_failure(
                                "consumer_session_lost",
                                "consumer streaming lease is no longer current".to_owned(),
                                None,
                                None,
                            )),
                            stream_state,
                        ));
                    }
                    return Some((
                        Ok(ndjson_line_or_error(&ConsumerStreamHeartbeat {
                            record_type: "heartbeat",
                            emitted_at: unix_ms_rfc3339(current_unix_ms()),
                        })),
                        stream_state,
                    ));
                }
            }
            continue;
        }
        if stream_state
            .pending_batch
            .as_ref()
            .is_some_and(|batch| batch.started.elapsed() >= stream_state.batch_limits.maximum_delay)
        {
            return Some(emit_history_batch(stream_state).await);
        }
        if let Some(record) = stream_state.pending_completion.take() {
            return Some(emit_backfill_completion(stream_state, record).await);
        }
        if let Some(unit) = stream_state.ready_progress_unit.take() {
            let limits = stream_state.batch_limits;
            if history_unit_exceeds_limits(&unit, limits) {
                stream_state.terminal = true;
                return Some((
                    Ok(ndjson_failure(
                        "single_progress_unit_exceeds_delivery_batch_limit",
                        "one committed history progress unit exceeds the configured delivery batch maximum"
                            .to_owned(),
                        None,
                        None,
                    )),
                    stream_state,
                ));
            }
            if stream_state
                .pending_batch
                .as_ref()
                .is_some_and(|batch| history_batch_would_exceed(batch, &unit, limits))
            {
                stream_state.ready_progress_unit = Some(unit);
                return Some(emit_history_batch(stream_state).await);
            }
            append_history_unit(&mut stream_state.pending_batch, unit);
            if stream_state
                .pending_batch
                .as_ref()
                .is_some_and(|batch| history_batch_target_reached(batch, limits))
            {
                return Some(emit_history_batch(stream_state).await);
            }
            continue;
        }
        if let Some(record) = stream_state.fetched.pop_front() {
            let kind = record.change.kind.as_str();
            if kind == "system.backfill_complete" {
                if !stream_state.pending_progress_unit.is_empty() {
                    stream_state.terminal = true;
                    return Some((
                        Ok(ndjson_failure(
                            "delivery_encoding_failed",
                            "backfill completion followed an incomplete progress unit".to_owned(),
                            None,
                            None,
                        )),
                        stream_state,
                    ));
                }
                if stream_state.pending_batch.is_some() {
                    stream_state.pending_completion = Some(record);
                    return Some(emit_history_batch(stream_state).await);
                }
                return Some(emit_backfill_completion(stream_state, record).await);
            }
            let boundary = kind == "system.backfill_progress";
            stream_state.pending_progress_unit.push(record);
            if boundary {
                let records = std::mem::take(&mut stream_state.pending_progress_unit);
                match build_history_progress_unit(&stream_state, records) {
                    Ok(unit) => stream_state.ready_progress_unit = Some(unit),
                    Err(message) => {
                        stream_state.terminal = true;
                        return Some((
                            Ok(ndjson_failure(
                                "delivery_encoding_failed",
                                message,
                                None,
                                None,
                            )),
                            stream_state,
                        ));
                    }
                }
            }
            continue;
        }

        match stream_state
            .state
            .store
            .consumer_changes_after_in_stream(
                stream_state.processor.descriptor(),
                &stream_state.stream_id,
                stream_state.state.config.chain_id,
                &stream_state.consumer_id,
                stream_state.fetch_after,
                MAX_PAGE_SIZE,
            )
            .await
        {
            Ok(records) if !records.is_empty() => {
                stream_state.fetch_after = records
                    .last()
                    .map_or(stream_state.fetch_after, |record| record.cursor.sequence);
                stream_state.fetched.extend(records);
            }
            Ok(_) => {
                if let Some((code, message)) = backfill_terminal_stream_failure(&stream_state).await
                {
                    stream_state.terminal = true;
                    return Some((Ok(ndjson_failure(code, message, None, None)), stream_state));
                }
                let batch_pending = stream_state.pending_batch.is_some();
                let wait = stream_state.pending_batch.as_ref().map_or(
                    stream_state.state.config.heartbeat_interval,
                    |batch| {
                        stream_state
                            .batch_limits
                            .maximum_delay
                            .saturating_sub(batch.started.elapsed())
                    },
                );
                tokio::select! {
                    () = stream_state.state.store.wait_for_delivery_changes() => {}
                    () = tokio::time::sleep(wait) => {
                        if batch_pending {
                            continue;
                        }
                        if !backfill_stream_session_is_current(&stream_state).await {
                            stream_state.terminal = true;
                            return Some((
                                Ok(ndjson_failure(
                                    "consumer_session_lost",
                                    "consumer streaming lease is no longer current".to_owned(),
                                    None,
                                    None,
                                )),
                                stream_state,
                            ));
                        }
                        if let Some((code, message)) =
                            backfill_terminal_stream_failure(&stream_state).await
                        {
                            stream_state.terminal = true;
                            return Some((
                                Ok(ndjson_failure(code, message, None, None)),
                                stream_state,
                            ));
                        }
                        let heartbeat = ConsumerStreamHeartbeat {
                            record_type: "heartbeat",
                            emitted_at: unix_ms_rfc3339(current_unix_ms()),
                        };
                        return Some((Ok(ndjson_line_or_error(&heartbeat)), stream_state));
                    }
                }
            }
            Err(StoreError::ConsumerResetRequired {
                earliest_available,
                latest_available,
            }) => {
                stream_state.terminal = true;
                return Some((
                    Ok(ndjson_failure(
                        "reset_required",
                        "consumer cursor predates retained delivery data".to_owned(),
                        earliest_available,
                        latest_available,
                    )),
                    stream_state,
                ));
            }
            Err(error) => {
                stream_state.terminal = true;
                return Some((
                    Ok(ndjson_failure(
                        "delivery_failed",
                        error.to_string(),
                        None,
                        None,
                    )),
                    stream_state,
                ));
            }
        }
    }
}

async fn backfill_terminal_stream_failure(
    stream_state: &BackfillConsumerStreamState,
) -> Option<(&'static str, String)> {
    let control = stream_state.state.config.backfill_control.as_ref()?;
    match control.inspect(&stream_state.subscription_id).await {
        Ok(status) if status.state == BackfillState::Failed => Some((
            "backfill_failed",
            status
                .last_error
                .unwrap_or_else(|| "the historical subscription failed".to_owned()),
        )),
        Ok(status) if status.state == BackfillState::Cancelled => Some((
            "backfill_cancelled",
            status
                .last_error
                .unwrap_or_else(|| "the historical subscription was cancelled".to_owned()),
        )),
        Ok(_) => None,
        Err(error) => Some(("backfill_status_failed", error.to_string())),
    }
}

fn build_history_progress_unit(
    stream_state: &BackfillConsumerStreamState,
    records: Vec<ChangeRecord>,
) -> Result<HistoryProgressUnit, String> {
    let boundary = records
        .last()
        .ok_or_else(|| "history progress unit is empty".to_owned())?;
    if boundary.change.kind != "system.backfill_progress" {
        return Err("history progress unit has no terminal progress boundary".to_owned());
    }
    let origin = boundary.origin.clone();
    if records.iter().any(|record| record.origin != origin) {
        return Err("history progress unit crosses publication origins".to_owned());
    }
    let (from_block, through_block, processed_blocks) =
        decode_progress_boundary(&boundary.change.payload, boundary.block.number.0)?;
    let acknowledgeable_cursor = change_envelope_in_stream(
        &stream_state.state,
        stream_state.processor.as_ref(),
        &stream_state.stream_id,
        boundary.clone(),
    )
    .map_err(|error| error.to_string())?
    .cursor;
    let domain_changes = u64::try_from(
        records
            .iter()
            .filter(|record| !record.change.kind.starts_with("system."))
            .count(),
    )
    .map_err(|_| "history domain change count is too large".to_owned())?;
    let raw_payload_bytes = records
        .iter()
        .filter(|record| !record.change.kind.starts_with("system."))
        .try_fold(0_u64, |total, record| {
            let bytes = u64::try_from(record.change.key.len() + record.change.payload.len())
                .map_err(|_| "history raw payload is too large".to_owned())?;
            total
                .checked_add(bytes)
                .ok_or_else(|| "history raw payload size overflowed".to_owned())
        })?;
    let changes = records
        .into_iter()
        .filter(|record| !record.change.kind.starts_with("system."))
        .map(|record| {
            change_envelope_in_stream(
                &stream_state.state,
                stream_state.processor.as_ref(),
                &stream_state.stream_id,
                record,
            )
            .map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let encoded_change_bytes = changes.iter().try_fold(0_u64, |total, change| {
        let bytes = serde_json::to_vec(change)
            .map_err(|error| format!("encode history change: {error}"))?;
        total
            .checked_add(
                u64::try_from(bytes.len())
                    .map_err(|_| "encoded history change is too large".to_owned())?,
            )
            .ok_or_else(|| "encoded history batch size overflowed".to_owned())
    })?;
    Ok(HistoryProgressUnit {
        origin,
        from_block,
        through_block,
        processed_blocks,
        domain_changes,
        raw_payload_bytes,
        encoded_change_bytes,
        acknowledgeable_cursor,
        changes,
    })
}

fn encoded_change_array_bytes(encoded_change_bytes: u64, events: usize) -> u64 {
    encoded_change_bytes
        .saturating_add(u64::try_from(events.saturating_sub(1)).unwrap_or(u64::MAX))
        .saturating_add(2)
}

fn history_unit_exceeds_limits(unit: &HistoryProgressUnit, limits: DeliveryBatchLimits) -> bool {
    encoded_change_array_bytes(unit.encoded_change_bytes, unit.changes.len())
        > limits.maximum_encoded_bytes
        || u64::try_from(unit.changes.len()).unwrap_or(u64::MAX) > limits.maximum_events
        || unit.processed_blocks > limits.maximum_processed_blocks
}

fn history_batch_would_exceed(
    batch: &PendingHistoryBatch,
    unit: &HistoryProgressUnit,
    limits: DeliveryBatchLimits,
) -> bool {
    if batch.origin != unit.origin {
        return true;
    }
    let encoded = encoded_change_array_bytes(
        batch
            .encoded_change_bytes
            .saturating_add(unit.encoded_change_bytes),
        batch.changes.len().saturating_add(unit.changes.len()),
    );
    encoded > limits.maximum_encoded_bytes
        || u64::try_from(batch.changes.len().saturating_add(unit.changes.len())).unwrap_or(u64::MAX)
            > limits.maximum_events
        || batch.processed_blocks.saturating_add(unit.processed_blocks)
            > limits.maximum_processed_blocks
}

fn history_batch_target_reached(batch: &PendingHistoryBatch, limits: DeliveryBatchLimits) -> bool {
    encoded_change_array_bytes(batch.encoded_change_bytes, batch.changes.len())
        >= limits.target_encoded_bytes
        || u64::try_from(batch.changes.len()).unwrap_or(u64::MAX) >= limits.maximum_events
        || batch.processed_blocks >= limits.maximum_processed_blocks
}

fn append_history_unit(batch: &mut Option<PendingHistoryBatch>, unit: HistoryProgressUnit) {
    if let Some(batch) = batch {
        batch.through_block = unit.through_block;
        batch.processed_blocks = batch.processed_blocks.saturating_add(unit.processed_blocks);
        batch.domain_changes = batch.domain_changes.saturating_add(unit.domain_changes);
        batch.progress_units = batch.progress_units.saturating_add(1);
        batch.raw_payload_bytes = batch
            .raw_payload_bytes
            .saturating_add(unit.raw_payload_bytes);
        batch.encoded_change_bytes = batch
            .encoded_change_bytes
            .saturating_add(unit.encoded_change_bytes);
        batch.acknowledgeable_cursor = unit.acknowledgeable_cursor;
        batch.changes.extend(unit.changes);
    } else {
        *batch = Some(PendingHistoryBatch {
            started: Instant::now(),
            origin: unit.origin,
            from_block: unit.from_block,
            through_block: unit.through_block,
            processed_blocks: unit.processed_blocks,
            domain_changes: unit.domain_changes,
            progress_units: 1,
            raw_payload_bytes: unit.raw_payload_bytes,
            encoded_change_bytes: unit.encoded_change_bytes,
            acknowledgeable_cursor: unit.acknowledgeable_cursor,
            changes: unit.changes,
        });
    }
}

async fn emit_history_batch(
    mut stream_state: BackfillConsumerStreamState,
) -> (Result<Bytes, Infallible>, BackfillConsumerStreamState) {
    if !backfill_stream_session_is_current(&stream_state).await {
        stream_state.terminal = true;
        return (
            Ok(ndjson_failure(
                "consumer_session_lost",
                "consumer streaming lease is no longer current".to_owned(),
                None,
                None,
            )),
            stream_state,
        );
    }
    let Some(pending) = stream_state.pending_batch.take() else {
        stream_state.terminal = true;
        return (
            Ok(ndjson_failure(
                "delivery_encoding_failed",
                "history delivery batch was unexpectedly empty".to_owned(),
                None,
                None,
            )),
            stream_state,
        );
    };
    let first_cursor = pending.changes.first().map(|change| change.cursor.clone());
    let last_cursor = pending.changes.last().map(|change| change.cursor.clone());
    let encoded_event_payload = match serde_json::to_vec(&pending.changes) {
        Ok(encoded) => encoded,
        Err(error) => {
            stream_state.terminal = true;
            return (
                Ok(ndjson_failure(
                    "delivery_encoding_failed",
                    error.to_string(),
                    None,
                    None,
                )),
                stream_state,
            );
        }
    };
    let transmitted_event_bytes = if stream_state.gzip {
        match gzip_member(&encoded_event_payload) {
            Ok(compressed) => compressed.len(),
            Err(error) => {
                stream_state.terminal = true;
                return (
                    Ok(ndjson_failure(
                        "delivery_encoding_failed",
                        error.to_string(),
                        None,
                        None,
                    )),
                    stream_state,
                );
            }
        }
    } else {
        encoded_event_payload.len()
    };
    let batch = ConsumerStreamBatch {
        record_type: "batch",
        stream_id: stream_state.stream_id.clone(),
        origin_kind: pending.origin.kind.as_str().to_owned(),
        origin_id: pending.origin.id.clone(),
        publication_revision: pending.origin.publication_revision.to_string(),
        from_block: pending.from_block,
        through_block: pending.through_block,
        processed_block_count: pending.processed_blocks.to_string(),
        domain_change_count: pending.domain_changes.to_string(),
        progress_unit_count: pending.progress_units.to_string(),
        raw_payload_bytes: pending.raw_payload_bytes.to_string(),
        uncompressed_encoded_bytes: encoded_event_payload.len().to_string(),
        transmitted_bytes: transmitted_event_bytes.to_string(),
        build_delay_ms: pending.started.elapsed().as_millis().to_string(),
        first_cursor,
        last_cursor,
        acknowledgeable_cursor: pending.acknowledgeable_cursor,
        changes: pending.changes,
    };
    (Ok(ndjson_line_or_error(&batch)), stream_state)
}

async fn emit_backfill_completion(
    mut stream_state: BackfillConsumerStreamState,
    record: ChangeRecord,
) -> (Result<Bytes, Infallible>, BackfillConsumerStreamState) {
    if !backfill_stream_session_is_current(&stream_state).await {
        stream_state.terminal = true;
        return (
            Ok(ndjson_failure(
                "consumer_session_lost",
                "consumer streaming lease is no longer current".to_owned(),
                None,
                None,
            )),
            stream_state,
        );
    }
    let metadata =
        match leani_store_sqlite::decode_backfill_completion_metadata(&record.change.payload) {
            Ok(metadata) => metadata,
            Err(error) => {
                stream_state.terminal = true;
                return (
                    Ok(ndjson_failure(
                        "delivery_encoding_failed",
                        error.to_string(),
                        None,
                        None,
                    )),
                    stream_state,
                );
            }
        };
    let cursor = match encode_stream_cursor(
        &stream_state.state,
        stream_state.processor.as_ref(),
        &stream_state.stream_id,
        record.cursor.sequence,
    ) {
        Ok(cursor) => cursor,
        Err(error) => {
            stream_state.terminal = true;
            return (
                Ok(ndjson_failure(
                    "delivery_encoding_failed",
                    error.to_string(),
                    None,
                    None,
                )),
                stream_state,
            );
        }
    };
    let completion = ConsumerStreamCompletion {
        record_type: "backfill_complete",
        subscription_id: stream_state.subscription_id.clone(),
        stream_id: stream_state.stream_id.clone(),
        ranges: stream_state.ranges.clone(),
        through_block: record.block.number.0,
        cursor,
        mode: match metadata.mode {
            leani_store_sqlite::BackfillSubscriptionMode::FillMissing => {
                BackfillExecutionMode::FillMissing
            }
            leani_store_sqlite::BackfillSubscriptionMode::Recompute => {
                BackfillExecutionMode::Recompute
            }
        },
        disposition: metadata.disposition,
        requested_block_count: metadata.requested_blocks.to_string(),
        covered_before_request_block_count: metadata.covered_before_request_blocks.to_string(),
        covered_before_request_ranges: metadata
            .covered_before_request_ranges
            .into_iter()
            .map(|range| BackfillRange {
                from_block: range.start().0,
                to_block: range.end().0,
            })
            .collect(),
        newly_processed_block_count: metadata.newly_processed_blocks.to_string(),
        republished_block_count: metadata.republished_blocks.to_string(),
        domain_change_count: metadata.domain_changes.to_string(),
        preexisting_coverage_skipped: metadata.republished_blocks < metadata.requested_blocks,
        repair_hint: (metadata.republished_blocks < metadata.requested_blocks)
            .then_some("recompute"),
    };
    stream_state.completion_sequence = Some(record.cursor.sequence);
    (Ok(ndjson_line_or_error(&completion)), stream_state)
}

async fn backfill_stream_session_is_current(stream_state: &BackfillConsumerStreamState) -> bool {
    stream_state
        .state
        .store
        .consumer_session_is_current_in_stream(
            &stream_state.stream_id,
            &stream_state.consumer_id,
            stream_state.lease.generation,
        )
        .await
        .unwrap_or(false)
}

fn ndjson_line<T: Serialize>(value: &T) -> Result<Bytes, ApiError> {
    let mut encoded = serde_json::to_vec(value)
        .map_err(|error| ApiError::internal(&format!("NDJSON encoding failed: {error}")))?;
    encoded.push(b'\n');
    Ok(Bytes::from(encoded))
}

fn accepts_gzip(headers: &HeaderMap) -> bool {
    headers
        .get_all(header::ACCEPT_ENCODING)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .any(|encoding| {
            let mut parts = encoding.split(';');
            parts
                .next()
                .is_some_and(|name| name.eq_ignore_ascii_case("gzip"))
                && !parts.any(|parameter| parameter.trim() == "q=0")
        })
}

fn gzip_member(bytes: &[u8]) -> std::io::Result<Bytes> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder.write_all(bytes)?;
    encoder.flush()?;
    encoder.finish().map(Bytes::from)
}

enum DeliveryCompressor {
    Identity,
    Gzip(Box<flate2::write::GzEncoder<Vec<u8>>>),
}

impl DeliveryCompressor {
    fn new(gzip: bool) -> Self {
        if gzip {
            Self::Gzip(Box::new(flate2::write::GzEncoder::new(
                Vec::new(),
                flate2::Compression::fast(),
            )))
        } else {
            Self::Identity
        }
    }

    fn encode(&mut self, bytes: Bytes) -> std::io::Result<Bytes> {
        match self {
            Self::Identity => Ok(bytes),
            Self::Gzip(encoder) => {
                encoder.write_all(&bytes)?;
                // A sync flush makes every NDJSON record observable without
                // ending the one continuous gzip member. Browser/Bun fetch
                // implementations are not required to stream concatenated
                // gzip members consistently.
                encoder.flush()?;
                Ok(Bytes::from(std::mem::take(encoder.get_mut())))
            }
        }
    }

    fn finish(self) -> std::io::Result<Option<Bytes>> {
        match self {
            Self::Identity => Ok(None),
            Self::Gzip(encoder) => (*encoder).finish().map(|bytes| Some(Bytes::from(bytes))),
        }
    }
}

struct BufferedDeliveryChunk {
    bytes: Bytes,
    _byte_permit: tokio::sync::OwnedSemaphorePermit,
}

fn buffered_delivery_body<S>(source: S, gzip: bool, limits: DeliveryBatchLimits) -> Body
where
    S: Stream<Item = Result<Bytes, Infallible>> + Send + 'static,
{
    let (sender, receiver) = tokio::sync::mpsc::channel(limits.maximum_buffered_batches);
    let byte_capacity = usize::try_from(limits.maximum_buffered_bytes)
        .expect("validated delivery buffer fits usize");
    let byte_budget = Arc::new(tokio::sync::Semaphore::new(byte_capacity));
    tokio::spawn(async move {
        futures::pin_mut!(source);
        let mut compressor = DeliveryCompressor::new(gzip);
        loop {
            let item = tokio::select! {
                () = sender.closed() => return,
                item = source.next() => item,
            };
            let Some(item) = item else {
                match compressor.finish() {
                    Ok(Some(footer)) if !footer.is_empty() => {
                        let _ = send_buffered_delivery_chunk(
                            &sender,
                            &byte_budget,
                            limits.maximum_buffered_bytes,
                            footer,
                        )
                        .await;
                    }
                    Ok(_) => {}
                    Err(error) => {
                        let _ = sender.send(Err(error)).await;
                    }
                }
                return;
            };
            let bytes = match item {
                Ok(bytes) => bytes,
                Err(never) => match never {},
            };
            let bytes = match compressor.encode(bytes) {
                Ok(bytes) => bytes,
                Err(error) => {
                    let _ = sender.send(Err(error)).await;
                    return;
                }
            };
            if let Err(error) = send_buffered_delivery_chunk(
                &sender,
                &byte_budget,
                limits.maximum_buffered_bytes,
                bytes,
            )
            .await
            {
                let _ = sender.send(Err(error)).await;
                return;
            }
        }
    });
    let buffered = stream::unfold(receiver, |mut receiver| async move {
        receiver.recv().await.map(|item| (item, receiver))
    })
    .map(|item| item.map(|chunk| chunk.bytes));
    Body::from_stream(buffered)
}

async fn send_buffered_delivery_chunk(
    sender: &tokio::sync::mpsc::Sender<std::io::Result<BufferedDeliveryChunk>>,
    byte_budget: &Arc<tokio::sync::Semaphore>,
    maximum_buffered_bytes: u64,
    bytes: Bytes,
) -> std::io::Result<()> {
    let charged_bytes = u32::try_from(bytes.len()).map_err(|_| {
        std::io::Error::other("one delivery record exceeds the supported buffer accounting range")
    })?;
    if u64::from(charged_bytes) > maximum_buffered_bytes {
        return Err(std::io::Error::other(format!(
            "one delivery record requires {charged_bytes} buffered bytes; maximum is {maximum_buffered_bytes}"
        )));
    }
    let permit = byte_budget
        .clone()
        .acquire_many_owned(charged_bytes)
        .await
        .map_err(|_| std::io::Error::other("delivery buffer closed"))?;
    sender
        .send(Ok(BufferedDeliveryChunk {
            bytes,
            _byte_permit: permit,
        }))
        .await
        .map_err(|_| std::io::Error::other("delivery receiver closed"))
}

fn add_compression_headers(response: &mut Response, gzip: bool) {
    if gzip {
        response
            .headers_mut()
            .insert(header::CONTENT_ENCODING, HeaderValue::from_static("gzip"));
        response
            .headers_mut()
            .insert(header::VARY, HeaderValue::from_static("accept-encoding"));
    }
}

fn ndjson_line_or_error<T: Serialize>(value: &T) -> Bytes {
    ndjson_line(value).unwrap_or_else(|error| {
        ndjson_failure("delivery_encoding_failed", error.to_string(), None, None)
    })
}

fn ndjson_failure(
    code: &'static str,
    message: String,
    earliest_available: Option<u64>,
    latest_available: Option<u64>,
) -> Bytes {
    ndjson_line(&ConsumerStreamFailure {
        record_type: if code == "reset_required" {
            "reset_required"
        } else {
            "error"
        },
        code,
        message,
        earliest_available_sequence: earliest_available.map(|value| value.to_string()),
        latest_available_sequence: latest_available.map(|value| value.to_string()),
    })
    .unwrap_or_else(|_| Bytes::from_static(b"{\"type\":\"error\",\"code\":\"encoding\"}\n"))
}

fn current_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn decode_progress_boundary(
    payload: &[u8],
    fallback_block: u64,
) -> Result<(u64, u64, u64), String> {
    if payload.is_empty() {
        return Ok((fallback_block, fallback_block, 1));
    }
    let encoded: [u8; 24] = payload
        .try_into()
        .map_err(|_| "backfill progress payload has an invalid length".to_owned())?;
    let from_block = u64::from_be_bytes(
        encoded[0..8]
            .try_into()
            .expect("progress slice is eight bytes"),
    );
    let through_block = u64::from_be_bytes(
        encoded[8..16]
            .try_into()
            .expect("progress slice is eight bytes"),
    );
    let processed_blocks = u64::from_be_bytes(
        encoded[16..24]
            .try_into()
            .expect("progress slice is eight bytes"),
    );
    if from_block > through_block {
        return Err("backfill progress payload is invalid".to_owned());
    }
    Ok((from_block, through_block, processed_blocks))
}

async fn acknowledge_backfill_consumer(
    State(state): State<ApiState>,
    Path((subscription, consumer)): Path<(String, String)>,
    headers: HeaderMap,
    Json(request): Json<AcknowledgeConsumerRequest>,
) -> Result<Json<ConsumerResponse>, ApiError> {
    let (processor, stream_id, _) = backfill_delivery_scope(&state, &subscription).await?;
    authorize_consumer_scope_in_stream(&state, processor.as_ref(), &stream_id, &consumer, &headers)
        .await?;
    let sequence =
        decode_stream_change_cursor(&state, processor.as_ref(), &stream_id, &request.cursor)?
            .sequence;
    let generation = consumer_session_generation(
        &state,
        processor.as_ref(),
        &stream_id,
        &consumer,
        &headers,
        true,
    )?
    .expect("required consumer session generation");
    let consumer = state
        .store
        .acknowledge_consumer_session_in_stream(
            processor.descriptor(),
            &stream_id,
            &consumer,
            generation,
            sequence,
        )
        .await?;
    state
        .store
        .mark_backfill_subscription_reclaimable(&subscription, consumer.acknowledged_sequence)
        .await?;
    Ok(Json(
        consumer_response_in_stream(&state, processor.as_ref(), consumer).await?,
    ))
}

async fn renew_backfill_consumer(
    State(state): State<ApiState>,
    Path((subscription, consumer)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<Json<ConsumerResponse>, ApiError> {
    let (processor, stream_id, _) = backfill_delivery_scope(&state, &subscription).await?;
    authorize_consumer_scope_in_stream(&state, processor.as_ref(), &stream_id, &consumer, &headers)
        .await?;
    let generation = consumer_session_generation(
        &state,
        processor.as_ref(),
        &stream_id,
        &consumer,
        &headers,
        true,
    )?
    .expect("required consumer session generation");
    state
        .store
        .renew_consumer_session_in_stream(&stream_id, &consumer, generation)
        .await?;
    let consumer = state
        .store
        .consumer_in_stream(processor.descriptor(), &stream_id, &consumer)
        .await?
        .ok_or_else(|| ApiError::not_found("durable consumer is not registered"))?;
    Ok(Json(
        consumer_response_in_stream(&state, processor.as_ref(), consumer).await?,
    ))
}

async fn release_backfill_consumer(
    State(state): State<ApiState>,
    Path((subscription, consumer)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError> {
    let (processor, stream_id, _) = backfill_delivery_scope(&state, &subscription).await?;
    authorize_consumer_scope_in_stream(&state, processor.as_ref(), &stream_id, &consumer, &headers)
        .await?;
    let generation = consumer_session_generation(
        &state,
        processor.as_ref(),
        &stream_id,
        &consumer,
        &headers,
        true,
    )?
    .expect("required consumer session generation");
    state
        .store
        .release_consumer_session_in_stream(&stream_id, &consumer, generation)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn backfill_delivery_scope(
    state: &ApiState,
    subscription: &str,
) -> Result<(Arc<dyn Processor>, String, BackfillStatus), ApiError> {
    let control = state.config.backfill_control.as_ref().ok_or_else(|| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "backfill_unavailable",
            "this node has no processor backfill control plane",
            true,
        )
    })?;
    let status = control
        .inspect(subscription)
        .await
        .map_err(backfill_error)?;
    if status.owner != HistoricalWorkOwner::Subscription {
        return Err(ApiError::not_found("backfill subscription does not exist"));
    }
    let processor = configured_processor(state, &status.processor)?;
    let stream_id = status.delivery_stream_id.clone().ok_or_else(|| {
        ApiError::conflict(
            "backfill_has_no_delivery_stream",
            "this backfill subscription has no delivery stream",
        )
    })?;
    state
        .store
        .delivery_stream(&stream_id)
        .await?
        .ok_or_else(|| ApiError::not_found("backfill delivery stream does not exist"))?;
    Ok((processor, stream_id, status))
}

async fn authorize_consumer_scope(
    state: &ApiState,
    processor: &dyn Processor,
    consumer: &str,
    headers: &HeaderMap,
) -> Result<(), ApiError> {
    if let Some(credential) = headers
        .get(CONSUMER_CREDENTIAL_HEADER)
        .and_then(|value| value.to_str().ok())
    {
        if state
            .store
            .consumer_credential_matches(processor.descriptor(), consumer, credential)
            .await?
        {
            return Ok(());
        }
        return Err(ApiError::forbidden(
            "consumer credential does not match this durable consumer",
        ));
    }
    // The optional global bearer setting is the API trust boundary. When it is
    // configured, the router middleware has already authenticated this
    // request. When it is omitted (for example on a loopback-only local
    // sidecar), consumer operations are intentionally unauthenticated too.
    Ok(())
}

async fn authorize_consumer_scope_in_stream(
    state: &ApiState,
    processor: &dyn Processor,
    stream_id: &str,
    consumer: &str,
    headers: &HeaderMap,
) -> Result<(), ApiError> {
    if let Some(credential) = headers
        .get(CONSUMER_CREDENTIAL_HEADER)
        .and_then(|value| value.to_str().ok())
    {
        if state
            .store
            .consumer_credential_matches_in_stream(
                processor.descriptor(),
                stream_id,
                consumer,
                credential,
            )
            .await?
        {
            return Ok(());
        }
        return Err(ApiError::forbidden(
            "consumer credential does not match this durable consumer",
        ));
    }
    Ok(())
}

async fn consumer_response(
    state: &ApiState,
    processor: &dyn Processor,
    consumer: DurableConsumer,
) -> Result<ConsumerResponse, ApiError> {
    let lag = state
        .store
        .consumer_lag(processor.descriptor(), &consumer.consumer_id)
        .await?;
    Ok(ConsumerResponse {
        id: consumer.consumer_id,
        processor_instance: consumer.processor_instance,
        stream_id: consumer.stream_id,
        role: consumer.role,
        state: consumer.state,
        acknowledged_sequence: consumer.acknowledged_sequence.to_string(),
        delivered_sequence: consumer.delivered_sequence.to_string(),
        acknowledged_cursor: encode_consumer_cursor(
            state,
            processor,
            consumer.acknowledged_sequence,
        )?,
        delivered_cursor: encode_consumer_cursor(state, processor, consumer.delivered_sequence)?,
        lease_generation: consumer.lease_generation.to_string(),
        lease_ttl_ms: consumer.lease_ttl_ms.to_string(),
        lease_expires_at_unix_ms: consumer.lease_expires_at_unix_ms.to_string(),
        lease_active: consumer.lease_active,
        lag_changes: lag.changes.to_string(),
        lag_blocks: lag.blocks.to_string(),
        lag_bytes: lag.bytes.to_string(),
        lag_age_ms: lag.age_ms.to_string(),
        created_at_unix_ms: consumer.created_at_unix_ms.to_string(),
        updated_at_unix_ms: consumer.updated_at_unix_ms.to_string(),
    })
}

async fn consumer_response_in_stream(
    state: &ApiState,
    processor: &dyn Processor,
    consumer: DurableConsumer,
) -> Result<ConsumerResponse, ApiError> {
    let lag = state
        .store
        .consumer_lag_in_stream(
            processor.descriptor(),
            &consumer.stream_id,
            &consumer.consumer_id,
        )
        .await?;
    Ok(ConsumerResponse {
        id: consumer.consumer_id,
        processor_instance: consumer.processor_instance,
        stream_id: consumer.stream_id.clone(),
        role: consumer.role,
        state: consumer.state,
        acknowledged_sequence: consumer.acknowledged_sequence.to_string(),
        delivered_sequence: consumer.delivered_sequence.to_string(),
        acknowledged_cursor: encode_stream_cursor(
            state,
            processor,
            &consumer.stream_id,
            consumer.acknowledged_sequence,
        )?,
        delivered_cursor: encode_stream_cursor(
            state,
            processor,
            &consumer.stream_id,
            consumer.delivered_sequence,
        )?,
        lease_generation: consumer.lease_generation.to_string(),
        lease_ttl_ms: consumer.lease_ttl_ms.to_string(),
        lease_expires_at_unix_ms: consumer.lease_expires_at_unix_ms.to_string(),
        lease_active: consumer.lease_active,
        lag_changes: lag.changes.to_string(),
        lag_blocks: lag.blocks.to_string(),
        lag_bytes: lag.bytes.to_string(),
        lag_age_ms: lag.age_ms.to_string(),
        created_at_unix_ms: consumer.created_at_unix_ms.to_string(),
        updated_at_unix_ms: consumer.updated_at_unix_ms.to_string(),
    })
}

fn encode_consumer_cursor(
    state: &ApiState,
    processor: &dyn Processor,
    sequence: u64,
) -> Result<String, ApiError> {
    encode_cursor(&ApiCursor {
        version: 2,
        epoch: state.store.epoch(),
        chain_id: state.config.chain_id.0,
        processor_id: processor.descriptor().instance.to_string(),
        processor_version: processor.descriptor().version.to_string(),
        sequence,
    })
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct OutputCollectionsResponse {
    processor: ProcessorSummary,
    output_mode: OutputPolicyMode,
    retained: bool,
    data: Vec<String>,
}

async fn list_output_collections(
    State(state): State<ApiState>,
    Path(processor): Path<String>,
) -> Result<Json<OutputCollectionsResponse>, ApiError> {
    let processor = configured_processor(&state, &processor)?;
    let output_mode = processor.descriptor().lifecycle.output.mode;
    let retained = !matches!(output_mode, OutputPolicyMode::None);
    let data = if retained {
        state
            .store
            .output_collections(processor.descriptor())
            .await?
    } else {
        Vec::new()
    };
    Ok(Json(OutputCollectionsResponse {
        processor: processor_summary(&state, processor.as_ref()),
        output_mode,
        retained,
        data,
    }))
}

#[derive(Clone, Debug, Serialize)]
struct RecoveryCheckpointList {
    data: Vec<RecoveryCheckpoint>,
}

async fn list_recovery_checkpoints(
    State(state): State<ApiState>,
    Path(processor): Path<String>,
) -> Result<Json<RecoveryCheckpointList>, ApiError> {
    let processor = configured_processor(&state, &processor)?;
    Ok(Json(RecoveryCheckpointList {
        data: state
            .store
            .recovery_checkpoints(processor.descriptor())
            .await?,
    }))
}

async fn restore_recovery_checkpoint(
    State(state): State<ApiState>,
    Path((processor, checkpoint)): Path<(String, u64)>,
) -> Result<Json<RecoveryRestoreResponse>, ApiError> {
    let processor = configured_processor(&state, &processor)?;
    let cursor = state
        .store
        .restore_recovery_checkpoint(processor.descriptor(), checkpoint)
        .await?;
    Ok(Json(RecoveryRestoreResponse {
        checkpoint_id: checkpoint.to_string(),
        processor: processor_summary(&state, processor.as_ref()),
        block_number: cursor.block_number.0,
        block_hash: hash_hex(cursor.block_hash),
        finality: finality_name(cursor.finality),
        sequence: cursor.sequence.to_string(),
    }))
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RecoveryRestoreResponse {
    checkpoint_id: String,
    processor: ProcessorSummary,
    block_number: u64,
    block_hash: String,
    finality: &'static str,
    sequence: String,
}

#[derive(Clone, Debug, Serialize)]
struct PortableSavepointList {
    data: Vec<PortableSavepoint>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateSavepointRequest {
    id: String,
}

async fn list_portable_savepoints(
    State(state): State<ApiState>,
    Path(processor): Path<String>,
) -> Result<Json<PortableSavepointList>, ApiError> {
    let processor = configured_processor(&state, &processor)?;
    Ok(Json(PortableSavepointList {
        data: state
            .store
            .portable_savepoints(processor.descriptor())
            .await?,
    }))
}

async fn create_portable_savepoint(
    State(state): State<ApiState>,
    Path(processor): Path<String>,
    Json(request): Json<CreateSavepointRequest>,
) -> Result<(StatusCode, Json<PortableSavepoint>), ApiError> {
    let processor = configured_processor(&state, &processor)?;
    let savepoint = state
        .store
        .create_portable_savepoint(processor.descriptor(), &request.id)
        .await?;
    Ok((StatusCode::CREATED, Json(savepoint)))
}

async fn export_portable_savepoint(
    State(state): State<ApiState>,
    Path((processor, savepoint)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    let processor = configured_processor(&state, &processor)?;
    let archive = state
        .store
        .export_portable_savepoint(processor.descriptor(), &savepoint)
        .await?;
    Ok((
        [(header::CONTENT_TYPE, "application/vnd.leani.savepoint")],
        archive,
    )
        .into_response())
}

async fn delete_portable_savepoint(
    State(state): State<ApiState>,
    Path((processor, savepoint)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    let processor = configured_processor(&state, &processor)?;
    state
        .store
        .delete_portable_savepoint(processor.descriptor(), &savepoint)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct OutputEntityQuery {
    from_block: Option<u64>,
    to_block: Option<u64>,
    from_timestamp: Option<u64>,
    to_timestamp: Option<u64>,
    limit: Option<usize>,
    #[serde(default)]
    cursor: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct QueryAndFollowRequest {
    from_block: Option<u64>,
    to_block: Option<u64>,
    from_timestamp: Option<u64>,
    to_timestamp: Option<u64>,
    limit: Option<usize>,
}

impl From<&OutputEntityQuery> for OutputQuery {
    fn from(value: &OutputEntityQuery) -> Self {
        Self {
            from_block: value.from_block.map(BlockNumber),
            to_block: value.to_block.map(BlockNumber),
            from_timestamp: value.from_timestamp,
            to_timestamp: value.to_timestamp,
        }
    }
}

impl From<QueryAndFollowRequest> for OutputQuery {
    fn from(value: QueryAndFollowRequest) -> Self {
        Self {
            from_block: value.from_block.map(BlockNumber),
            to_block: value.to_block.map(BlockNumber),
            from_timestamp: value.from_timestamp,
            to_timestamp: value.to_timestamp,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct GenericOutputEntity {
    ordinal: String,
    key: String,
    schema: String,
    data: Value,
    block_number: u64,
    block_timestamp: u64,
    finality: &'static str,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct OutputBoundsResponse {
    earliest_block: u64,
    latest_block: u64,
    earliest_timestamp: u64,
    latest_timestamp: u64,
}

impl From<OutputBounds> for OutputBoundsResponse {
    fn from(value: OutputBounds) -> Self {
        Self {
            earliest_block: value.earliest_block.0,
            latest_block: value.latest_block.0,
            earliest_timestamp: value.earliest_timestamp,
            latest_timestamp: value.latest_timestamp,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct GenericSnapshotPage {
    data: Vec<GenericOutputEntity>,
    next_cursor: Option<String>,
    snapshot_id: String,
    boundary_cursor: String,
    row_count: String,
    value_bytes: String,
    expires_at_unix_ms: String,
    retained_bounds: Option<OutputBoundsResponse>,
    coverage: CoverageResponse,
    recovery: Value,
}

async fn query_output_entities(
    State(state): State<ApiState>,
    Path((processor, collection)): Path<(String, String)>,
    Query(query): Query<OutputEntityQuery>,
) -> Result<Json<GenericSnapshotPage>, ApiError> {
    let processor = configured_processor(&state, &processor)?;
    ensure_output_retained(processor.as_ref())?;
    let limit = page_limit(&state, query.limit)?;
    let page = if let Some(cursor) = query.cursor.as_deref() {
        let cursor = decode_snapshot_cursor(&state, processor.as_ref(), &collection, cursor)?;
        read_generic_snapshot_page(
            &state,
            processor,
            &collection,
            cursor.snapshot_id,
            Some(cursor.after_ordinal),
            limit,
        )
        .await?
    } else {
        create_generic_snapshot_page(
            &state,
            processor,
            &collection,
            OutputQuery::from(&query),
            limit,
        )
        .await?
    };
    Ok(Json(page))
}

async fn query_and_follow(
    State(state): State<ApiState>,
    Path((processor, collection)): Path<(String, String)>,
    Json(query): Json<QueryAndFollowRequest>,
) -> Result<Json<GenericSnapshotPage>, ApiError> {
    let processor = configured_processor(&state, &processor)?;
    ensure_output_retained(processor.as_ref())?;
    let limit = page_limit(&state, query.limit)?;
    Ok(Json(
        create_generic_snapshot_page(&state, processor, &collection, query.into(), limit).await?,
    ))
}

async fn get_output_entity(
    State(state): State<ApiState>,
    Path((processor, collection, key)): Path<(String, String, String)>,
) -> Result<Json<GenericOutputEntity>, ApiError> {
    let processor = configured_processor(&state, &processor)?;
    ensure_output_retained(processor.as_ref())?;
    let key = parse_entity_key(&key)?;
    let entity = state
        .store
        .output_entity(processor.descriptor(), &collection, &key)
        .await?
        .ok_or_else(|| ApiError::not_found("retained entity does not exist"))?;
    Ok(Json(render_output_entity(
        processor.as_ref(),
        &collection,
        entity,
    )?))
}

async fn release_query_snapshot(
    State(state): State<ApiState>,
    Path((processor, snapshot)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    let processor = configured_processor(&state, &processor)?;
    let snapshot_id = parse_snapshot_id(&snapshot)?;
    if state
        .store
        .release_query_snapshot(processor.descriptor(), snapshot_id)
        .await?
    {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::not_found("query snapshot does not exist"))
    }
}

async fn create_generic_snapshot_page(
    state: &ApiState,
    processor: Arc<dyn Processor>,
    collection: &str,
    query: OutputQuery,
    limit: usize,
) -> Result<GenericSnapshotPage, ApiError> {
    let bounds = state
        .store
        .output_bounds(processor.descriptor(), collection)
        .await?;
    reject_unretained_range(processor.as_ref(), query, bounds)?;
    let snapshot = state
        .store
        .create_query_snapshot(
            processor.descriptor(),
            collection,
            query,
            state.config.query_snapshot_ttl,
            state.config.query_snapshot_max_rows,
            state.config.query_snapshot_max_bytes,
        )
        .await?;
    read_generic_snapshot_page(
        state,
        processor,
        collection,
        snapshot.snapshot_id,
        None,
        limit,
    )
    .await
}

async fn read_generic_snapshot_page(
    state: &ApiState,
    processor: Arc<dyn Processor>,
    collection: &str,
    snapshot_id: [u8; 16],
    after_ordinal: Option<u64>,
    limit: usize,
) -> Result<GenericSnapshotPage, ApiError> {
    let (snapshot, mut rows) = state
        .store
        .query_snapshot_page(
            processor.descriptor(),
            collection,
            snapshot_id,
            after_ordinal,
            limit.saturating_add(1),
        )
        .await?;
    let has_more = rows.len() > limit;
    rows.truncate(limit);
    let next_cursor = if has_more {
        rows.last()
            .map(|row| {
                encode_snapshot_cursor(
                    state,
                    processor.as_ref(),
                    collection,
                    snapshot.snapshot_id,
                    row.ordinal,
                )
            })
            .transpose()?
    } else {
        None
    };
    let data = rows
        .into_iter()
        .map(|row| render_output_entity(processor.as_ref(), collection, row))
        .collect::<Result<Vec<_>, _>>()?;
    let retained_bounds = state
        .store
        .output_bounds(processor.descriptor(), collection)
        .await?
        .map(OutputBoundsResponse::from);
    let boundary_cursor =
        encode_consumer_cursor(state, processor.as_ref(), snapshot.boundary_sequence)?;
    Ok(GenericSnapshotPage {
        data,
        next_cursor,
        snapshot_id: hex::encode(snapshot.snapshot_id),
        boundary_cursor: boundary_cursor.clone(),
        row_count: snapshot.row_count.to_string(),
        value_bytes: snapshot.value_bytes.to_string(),
        expires_at_unix_ms: snapshot.expires_at_unix_ms.to_string(),
        retained_bounds,
        coverage: coverage(state, processor.as_ref(), None).await?,
        recovery: json!({
            "follow": format!(
                "/v1/processors/{}/stream?after={boundary_cursor}",
                processor.descriptor().instance
            ),
            "outsideRetention": "create_processor_instance_or_source_scan"
        }),
    })
}

fn render_output_entity(
    processor: &dyn Processor,
    collection: &str,
    entity: QuerySnapshotEntity,
) -> Result<GenericOutputEntity, ApiError> {
    let data = processor
        .entity_json(collection, &entity.key, &entity.value)
        .map_err(|error| {
            ApiError::internal(&format!(
                "processor {} could not render retained entity JSON: {error}",
                processor.descriptor().id
            ))
        })?
        .unwrap_or_else(|| {
            json!({
                "encoding": "hex",
                "value": hex::encode(&entity.value)
            })
        });
    Ok(GenericOutputEntity {
        ordinal: entity.ordinal.to_string(),
        key: format!("0x{}", hex::encode(entity.key)),
        schema: processor.descriptor().schemas.entity_schema.clone(),
        data,
        block_number: entity.block_number.0,
        block_timestamp: entity.block_timestamp,
        finality: finality_name(entity.finality),
    })
}

fn ensure_output_retained(processor: &dyn Processor) -> Result<(), ApiError> {
    if matches!(
        processor.descriptor().lifecycle.output.mode,
        OutputPolicyMode::None
    ) {
        Err(ApiError::output_not_retained(
            "processor coverage exists, but this instance retains no queryable output",
        ))
    } else {
        Ok(())
    }
}

fn reject_unretained_range(
    processor: &dyn Processor,
    query: OutputQuery,
    bounds: Option<OutputBounds>,
) -> Result<(), ApiError> {
    if !matches!(
        processor.descriptor().lifecycle.output.mode,
        OutputPolicyMode::Window
    ) {
        return Ok(());
    }
    if let Some(bounds) = bounds
        && (query
            .from_block
            .is_some_and(|from| from < bounds.earliest_block)
            || query
                .from_timestamp
                .is_some_and(|from| from < bounds.earliest_timestamp))
    {
        return Err(ApiError::output_not_retained(
            "requested range starts before retained output; create a source-scan job or processor instance",
        ));
    }
    Ok(())
}

fn parse_entity_key(value: &str) -> Result<Vec<u8>, ApiError> {
    let encoded = value
        .strip_prefix("0x")
        .ok_or_else(|| ApiError::invalid("entity key must start with 0x"))?;
    hex::decode(encoded).map_err(|_| ApiError::invalid("entity key is not valid hexadecimal"))
}

fn parse_snapshot_id(value: &str) -> Result<[u8; 16], ApiError> {
    let mut snapshot = [0_u8; 16];
    hex::decode_to_slice(value, &mut snapshot)
        .map_err(|_| ApiError::cursor("query snapshot ID must contain exactly 16 bytes"))?;
    Ok(snapshot)
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct SnapshotPageCursor {
    version: u8,
    epoch: [u8; 16],
    chain_id: u64,
    processor_instance: String,
    processor_version: String,
    collection: String,
    snapshot_id: [u8; 16],
    after_ordinal: u64,
}

fn encode_snapshot_cursor(
    state: &ApiState,
    processor: &dyn Processor,
    collection: &str,
    snapshot_id: [u8; 16],
    after_ordinal: u64,
) -> Result<String, ApiError> {
    encode_checksummed(&SnapshotPageCursor {
        version: 1,
        epoch: state.store.epoch(),
        chain_id: state.config.chain_id.0,
        processor_instance: processor.descriptor().instance.to_string(),
        processor_version: processor.descriptor().version.to_string(),
        collection: collection.to_owned(),
        snapshot_id,
        after_ordinal,
    })
}

fn decode_snapshot_cursor(
    state: &ApiState,
    processor: &dyn Processor,
    collection: &str,
    encoded: &str,
) -> Result<SnapshotPageCursor, ApiError> {
    let cursor: SnapshotPageCursor = decode_checksummed(encoded)?;
    if cursor.version != 1
        || cursor.epoch != state.store.epoch()
        || cursor.chain_id != state.config.chain_id.0
        || cursor.processor_instance != processor.descriptor().instance.as_str()
        || cursor.processor_version != processor.descriptor().version.to_string()
        || cursor.collection != collection
    {
        return Err(ApiError::cursor(
            "query cursor belongs to another store, chain, processor, or collection",
        ));
    }
    Ok(cursor)
}

fn encode_checksummed<T: Serialize>(value: &T) -> Result<String, ApiError> {
    let payload = postcard::to_allocvec(value)
        .map_err(|error| ApiError::internal(&format!("cursor encoding failed: {error}")))?;
    let checksum = blake3::hash(&payload);
    let mut bytes = payload;
    bytes.extend_from_slice(&checksum.as_bytes()[..16]);
    Ok(hex::encode(bytes))
}

fn decode_checksummed<T: for<'de> Deserialize<'de>>(encoded: &str) -> Result<T, ApiError> {
    let bytes = hex::decode(encoded).map_err(|_| ApiError::cursor("cursor is not valid hex"))?;
    let payload_length = bytes
        .len()
        .checked_sub(16)
        .ok_or_else(|| ApiError::cursor("cursor is truncated"))?;
    let (payload, checksum) = bytes.split_at(payload_length);
    if blake3::hash(payload).as_bytes()[..16] != *checksum {
        return Err(ApiError::cursor("cursor checksum mismatch"));
    }
    postcard::from_bytes(payload).map_err(|_| ApiError::cursor("cursor payload is invalid"))
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChangeEnvelope {
    api_version: &'static str,
    sequence: String,
    cursor: String,
    operation: &'static str,
    origin_kind: String,
    origin_id: String,
    publication_revision: String,
    chain_id: u64,
    block: Value,
    finality: &'static str,
    kind: String,
    schema: String,
    key: Option<String>,
    data: Option<Value>,
    /// Present only on `reset_required`, as the current coverage hint.
    #[serde(skip_serializing_if = "Option::is_none")]
    coverage: Option<CoverageResponse>,
    emitted_at: String,
}

/// Connection-scoped stream handshake. Synthesized per connection, never
/// stored, carries no SSE id and is never acknowledged.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct StreamHello {
    api_version: &'static str,
    chain_id: u64,
    processor: ProcessorSummary,
    coverage: CoverageResponse,
}

async fn changes(
    State(state): State<ApiState>,
    Path(processor): Path<String>,
    Query(query): Query<ChangesQuery>,
) -> Result<Json<Page<ChangeEnvelope>>, ApiError> {
    let processor = configured_processor(&state, &processor)?;
    let after = query
        .after
        .as_deref()
        .map(|cursor| decode_change_cursor(&state, processor.as_ref(), cursor))
        .transpose()?
        .map_or(0, |cursor| cursor.sequence);
    if let Some(bounds) = expired_resume(&state, processor.as_ref(), after).await? {
        return Err(ApiError::cursor_expired(bounds));
    }
    let limit = page_limit(&state, query.limit)?;
    let records = state
        .store
        .changes(processor.descriptor(), state.config.chain_id, after, limit)
        .await?;
    let coverage = coverage(&state, processor.as_ref(), None).await?;
    let data = records
        .into_iter()
        .map(|record| change_envelope(&state, processor.as_ref(), record))
        .collect::<Result<Vec<_>, _>>()?;
    let next_cursor = data.last().map(|event| event.cursor.clone());
    Ok(Json(Page {
        data,
        next_cursor,
        coverage,
    }))
}

async fn change_stream(
    State(state): State<ApiState>,
    Path(processor): Path<String>,
    Query(query): Query<ChangesQuery>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ApiError> {
    let processor = configured_processor(&state, &processor)?;
    let after = query
        .after
        .as_deref()
        .map(|cursor| decode_change_cursor(&state, processor.as_ref(), cursor))
        .transpose()?
        .map_or(0, |cursor| cursor.sequence);
    let hello = StreamHello {
        api_version: API_VERSION,
        chain_id: state.config.chain_id.0,
        processor: processor_summary(&state, processor.as_ref()),
        coverage: coverage(&state, processor.as_ref(), None).await?,
    };
    let expired = expired_resume(&state, processor.as_ref(), after).await?;
    let mut queued = VecDeque::new();
    let terminal = if let Some(bounds) = expired {
        queued.push_back(reset_envelope(
            &state,
            processor.as_ref(),
            bounds,
            hello.coverage.clone(),
        )?);
        true
    } else {
        false
    };
    let stream_state = ChangeStreamState {
        state: state.clone(),
        processor,
        after,
        queued,
        terminal,
    };
    // Connection handshake: synthesized fresh per connection, deliberately
    // without an SSE id so it can never become a resume cursor.
    let hello_event = Event::default()
        .event("hello")
        .json_data(&hello)
        .unwrap_or_else(|_| Event::default().event("error").data("{}"));
    let poll = stream::once(std::future::ready(Ok(hello_event)))
        .chain(stream::unfold(stream_state, poll_change));
    Ok(Sse::new(poll).keep_alive(
        KeepAlive::new()
            .interval(state.config.heartbeat_interval)
            .text("heartbeat"),
    ))
}

struct ChangeStreamState {
    state: ApiState,
    processor: Arc<dyn Processor>,
    after: u64,
    queued: VecDeque<ChangeEnvelope>,
    terminal: bool,
}

async fn poll_change(
    mut stream_state: ChangeStreamState,
) -> Option<(Result<Event, Infallible>, ChangeStreamState)> {
    loop {
        if let Some(envelope) = stream_state.queued.pop_front() {
            stream_state.after = envelope.sequence.parse().unwrap_or(stream_state.after);
            let event = Event::default()
                .id(envelope.cursor.clone())
                .event(envelope.operation)
                .json_data(&envelope)
                .unwrap_or_else(|_| Event::default().event("error").data("{}"));
            return Some((Ok(event), stream_state));
        }
        if stream_state.terminal {
            return None;
        }
        let records = stream_state
            .state
            .store
            .changes(
                stream_state.processor.descriptor(),
                stream_state.state.config.chain_id,
                stream_state.after,
                stream_state.state.config.stream_batch_size,
            )
            .await;
        match records {
            Ok(records) if records.is_empty() => {
                tokio::time::sleep(stream_state.state.config.stream_poll_interval).await;
            }
            Ok(records) => {
                let converted = records
                    .into_iter()
                    .map(|record| {
                        change_envelope(
                            &stream_state.state,
                            stream_state.processor.as_ref(),
                            record,
                        )
                    })
                    .collect::<Result<VecDeque<_>, _>>();
                match converted {
                    Ok(events) => stream_state.queued = events,
                    Err(error) => {
                        stream_state.terminal = true;
                        return Some((Ok(error.sse_event()), stream_state));
                    }
                }
            }
            Err(error) => {
                stream_state.terminal = true;
                return Some((Ok(ApiError::from(error).sse_event()), stream_state));
            }
        }
    }
}

async fn expired_resume(
    state: &ApiState,
    processor: &dyn Processor,
    after: u64,
) -> Result<Option<ChangeBounds>, ApiError> {
    if after == 0 {
        return Ok(None);
    }
    Ok(state
        .store
        .change_bounds(processor.descriptor())
        .await?
        .filter(|bounds| after.saturating_add(1) < bounds.earliest))
}

fn reset_envelope(
    state: &ApiState,
    processor: &dyn Processor,
    bounds: ChangeBounds,
    coverage: CoverageResponse,
) -> Result<ChangeEnvelope, ApiError> {
    let cursor = encode_cursor(&ApiCursor {
        version: 2,
        epoch: state.store.epoch(),
        chain_id: state.config.chain_id.0,
        processor_id: processor.descriptor().instance.to_string(),
        processor_version: processor.descriptor().version.to_string(),
        sequence: bounds.latest,
    })?;
    Ok(ChangeEnvelope {
        api_version: API_VERSION,
        sequence: bounds.latest.to_string(),
        cursor,
        operation: "reset_required",
        origin_kind: "live".to_owned(),
        origin_id: processor.descriptor().instance.to_string(),
        publication_revision: "0".to_owned(),
        chain_id: state.config.chain_id.0,
        block: Value::Null,
        finality: "finalized",
        kind: "system.reset_required".to_owned(),
        schema: "system.reset-required.v1".to_owned(),
        key: None,
        data: Some(json!({
            "earliestAvailableSequence": bounds.earliest.to_string(),
            "latestAvailableSequence": bounds.latest.to_string(),
            "action": "query_snapshot_then_resume"
        })),
        coverage: Some(coverage),
        emitted_at: unix_ms_rfc3339(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
                .try_into()
                .unwrap_or(u64::MAX),
        ),
    })
}

fn change_envelope(
    state: &ApiState,
    processor: &dyn Processor,
    record: ChangeRecord,
) -> Result<ChangeEnvelope, ApiError> {
    change_envelope_with_stream(state, processor, None, record)
}

fn change_envelope_in_stream(
    state: &ApiState,
    processor: &dyn Processor,
    stream_id: &str,
    record: ChangeRecord,
) -> Result<ChangeEnvelope, ApiError> {
    change_envelope_with_stream(state, processor, Some(stream_id), record)
}

#[allow(clippy::too_many_lines)]
fn change_envelope_with_stream(
    state: &ApiState,
    processor: &dyn Processor,
    stream_id: Option<&str>,
    record: ChangeRecord,
) -> Result<ChangeEnvelope, ApiError> {
    let data = match record.change.operation {
        ChangeOperation::Delete => None,
        ChangeOperation::Upsert if record.change.kind == "system.finality" => {
            let encoded: [u8; 8] = record
                .change
                .payload
                .as_slice()
                .try_into()
                .map_err(|_| ApiError::internal("stored finality change is invalid"))?;
            Some(json!({
                "throughBlock": u64::from_be_bytes(encoded)
            }))
        }
        ChangeOperation::Upsert if record.change.kind == "system.backfill_progress" => {
            let encoded: [u8; 8] = record
                .change
                .key
                .as_slice()
                .try_into()
                .map_err(|_| ApiError::internal("stored backfill progress is invalid"))?;
            Some(json!({
                "throughBlock": u64::from_be_bytes(encoded)
            }))
        }
        ChangeOperation::Upsert if record.change.kind == "system.backfill_complete" => {
            let completion =
                leani_store_sqlite::decode_backfill_completion_metadata(&record.change.payload)?;
            Some(serde_json::to_value(completion)?)
        }
        ChangeOperation::Upsert if record.change.kind == "blobs.block" => {
            let bundle: BlobsDelta =
                postcard::from_bytes(&record.change.payload).map_err(|error| {
                    ApiError::internal(&format!("stored blobs block bundle is invalid: {error}"))
                })?;
            Some(serde_json::to_value(BlobsBlockChange::from(&bundle))?)
        }
        ChangeOperation::Upsert if record.change.kind == BALANCE_CHANGE_KIND => {
            let entity: TokenBalanceEntity =
                postcard::from_bytes(&record.change.payload).map_err(|error| {
                    ApiError::internal(&format!("stored ERC-20 balance change is invalid: {error}"))
                })?;
            Some(serde_json::to_value(Erc20Balance::from(&entity))?)
        }
        ChangeOperation::Upsert
            if matches!(
                record.change.kind.as_str(),
                "uniswap.price.current" | "uniswap.price.observation"
            ) =>
        {
            let entity: PoolPriceEntity =
                postcard::from_bytes(&record.change.payload).map_err(|error| {
                    ApiError::internal(&format!("stored Uniswap price change is invalid: {error}"))
                })?;
            Some(serde_json::to_value(UniswapPoolPrice::from(&entity))?)
        }
        ChangeOperation::Upsert => processor
            .change_json(&record.change)
            .map_err(|error| {
                ApiError::internal(&format!(
                    "processor {} could not render change JSON: {error}",
                    processor.descriptor().id
                ))
            })?
            .or_else(|| {
                Some(json!({
                    "encoding": "hex",
                    "value": hex::encode(&record.change.payload)
                }))
            }),
    };
    let cursor = if let Some(stream_id) = stream_id {
        encode_stream_cursor(state, processor, stream_id, record.cursor.sequence)?
    } else {
        encode_change_cursor(state, processor, &record.cursor)?
    };
    let suffix = match record.change.operation {
        ChangeOperation::Upsert => "put",
        ChangeOperation::Delete => "delete",
    };
    Ok(ChangeEnvelope {
        api_version: API_VERSION,
        sequence: record.cursor.sequence.to_string(),
        cursor,
        operation: match record.direction {
            ChangeDirection::Apply => "apply",
            ChangeDirection::Undo => "undo",
            ChangeDirection::Finalized => "finalized",
            ChangeDirection::ResetRequired => "reset_required",
        },
        origin_kind: record.origin.kind.as_str().to_owned(),
        origin_id: record.origin.id,
        publication_revision: record.origin.publication_revision.to_string(),
        chain_id: record.cursor.chain_id.0,
        block: json!({
            "number": record.block.number.0,
            "hash": hash_hex(record.block.hash),
            "parentHash": hash_hex(record.block.parent_hash),
            "timestamp": record.block.timestamp
        }),
        finality: finality_name(record.finality),
        kind: format!("{}.{}", record.change.kind, suffix),
        schema: processor.descriptor().schemas.change_schema.clone(),
        key: Some(format!("0x{}", hex::encode(record.change.key))),
        data,
        coverage: None,
        emitted_at: unix_ms_rfc3339(record.emitted_at_unix_ms),
    })
}

#[derive(Debug, Deserialize, Serialize)]
struct ApiCursor {
    version: u8,
    epoch: [u8; 16],
    chain_id: u64,
    processor_id: String,
    processor_version: String,
    sequence: u64,
}

#[derive(Debug, Deserialize, Serialize)]
struct StreamApiCursor {
    version: u8,
    epoch: [u8; 16],
    chain_id: u64,
    processor_id: String,
    processor_version: String,
    stream_id: String,
    sequence: u64,
}

#[derive(Debug, Deserialize, Serialize)]
struct ConsumerSessionToken {
    version: u8,
    epoch: [u8; 16],
    chain_id: u64,
    processor_id: String,
    processor_version: String,
    stream_id: String,
    consumer_id: String,
    generation: u64,
}

fn encode_change_cursor(
    state: &ApiState,
    processor: &dyn Processor,
    cursor: &ChangeCursor,
) -> Result<String, ApiError> {
    encode_cursor(&ApiCursor {
        version: 2,
        epoch: state.store.epoch(),
        chain_id: cursor.chain_id.0,
        processor_id: processor.descriptor().instance.to_string(),
        processor_version: processor.descriptor().version.to_string(),
        sequence: cursor.sequence,
    })
}

fn decode_change_cursor(
    state: &ApiState,
    processor: &dyn Processor,
    encoded: &str,
) -> Result<ApiCursor, ApiError> {
    let cursor = decode_cursor(encoded)?;
    validate_api_cursor(state, processor.descriptor(), &cursor)?;
    Ok(cursor)
}

fn encode_stream_cursor(
    state: &ApiState,
    processor: &dyn Processor,
    stream_id: &str,
    sequence: u64,
) -> Result<String, ApiError> {
    encode_cursor(&StreamApiCursor {
        version: 3,
        epoch: state.store.epoch(),
        chain_id: state.config.chain_id.0,
        processor_id: processor.descriptor().instance.to_string(),
        processor_version: processor.descriptor().version.to_string(),
        stream_id: stream_id.to_owned(),
        sequence,
    })
}

fn decode_stream_change_cursor(
    state: &ApiState,
    processor: &dyn Processor,
    stream_id: &str,
    encoded: &str,
) -> Result<StreamApiCursor, ApiError> {
    let cursor: StreamApiCursor = decode_cursor_payload(encoded)?;
    if cursor.version != 3
        || cursor.epoch != state.store.epoch()
        || cursor.chain_id != state.config.chain_id.0
        || cursor.processor_id != processor.descriptor().instance.as_str()
        || cursor.processor_version != processor.descriptor().version.to_string()
        || cursor.stream_id != stream_id
    {
        return Err(ApiError::cursor(
            "cursor belongs to another store, chain, processor, or delivery stream",
        ));
    }
    Ok(cursor)
}

fn encode_consumer_session_token(
    state: &ApiState,
    processor: &dyn Processor,
    stream_id: &str,
    consumer_id: &str,
    generation: u64,
) -> Result<String, ApiError> {
    encode_cursor(&ConsumerSessionToken {
        version: 1,
        epoch: state.store.epoch(),
        chain_id: state.config.chain_id.0,
        processor_id: processor.descriptor().instance.to_string(),
        processor_version: processor.descriptor().version.to_string(),
        stream_id: stream_id.to_owned(),
        consumer_id: consumer_id.to_owned(),
        generation,
    })
}

fn decode_consumer_session_token(
    state: &ApiState,
    processor: &dyn Processor,
    stream_id: &str,
    consumer_id: &str,
    encoded: &str,
) -> Result<u64, ApiError> {
    let token: ConsumerSessionToken = decode_cursor_payload(encoded)?;
    if token.version != 1
        || token.epoch != state.store.epoch()
        || token.chain_id != state.config.chain_id.0
        || token.processor_id != processor.descriptor().instance.as_str()
        || token.processor_version != processor.descriptor().version.to_string()
        || token.stream_id != stream_id
        || token.consumer_id != consumer_id
    {
        return Err(ApiError::conflict(
            "consumer_session_lost",
            "consumer session token belongs to another store, chain, processor, stream, consumer, or generation",
        ));
    }
    Ok(token.generation)
}

fn consumer_session_generation(
    state: &ApiState,
    processor: &dyn Processor,
    stream_id: &str,
    consumer_id: &str,
    headers: &HeaderMap,
    required: bool,
) -> Result<Option<u64>, ApiError> {
    let token = headers
        .get(CONSUMER_SESSION_HEADER)
        .and_then(|value| value.to_str().ok());
    match token {
        Some(token) => {
            decode_consumer_session_token(state, processor, stream_id, consumer_id, token).map(Some)
        }
        None if required => Err(ApiError::invalid(
            "x-leani-consumer-session is required for this mutable session operation",
        )),
        None => Ok(None),
    }
}

fn encode_cursor<T: Serialize>(cursor: &T) -> Result<String, ApiError> {
    let payload = postcard::to_allocvec(cursor)
        .map_err(|error| ApiError::internal(&format!("cursor encoding failed: {error}")))?;
    let checksum = blake3::hash(&payload);
    let mut bytes = payload;
    bytes.extend_from_slice(&checksum.as_bytes()[..16]);
    Ok(hex::encode(bytes))
}

fn decode_cursor(encoded: &str) -> Result<ApiCursor, ApiError> {
    decode_cursor_payload(encoded)
}

fn decode_cursor_payload<T: for<'de> Deserialize<'de>>(encoded: &str) -> Result<T, ApiError> {
    let bytes = hex::decode(encoded).map_err(|_| ApiError::cursor("cursor is not valid hex"))?;
    let payload_length = bytes
        .len()
        .checked_sub(16)
        .ok_or_else(|| ApiError::cursor("cursor is truncated"))?;
    let (payload, checksum) = bytes.split_at(payload_length);
    if blake3::hash(payload).as_bytes()[..16] != *checksum {
        return Err(ApiError::cursor("cursor checksum mismatch"));
    }
    postcard::from_bytes(payload).map_err(|_| ApiError::cursor("cursor payload is invalid"))
}

fn validate_api_cursor(
    state: &ApiState,
    descriptor: &ProcessorDescriptor,
    cursor: &ApiCursor,
) -> Result<(), ApiError> {
    let processor_matches = match cursor.version {
        2 => cursor.processor_id == descriptor.instance.as_str(),
        1 => {
            cursor.processor_id == descriptor.id.as_str()
                && descriptor.instance
                    == leani_processor_api::ProcessorInstanceId::legacy(
                        &descriptor.id,
                        &descriptor.version,
                        descriptor.config_hash,
                    )
        }
        _ => false,
    };
    if !processor_matches
        || cursor.epoch != state.store.epoch()
        || cursor.chain_id != state.config.chain_id.0
        || cursor.processor_version != descriptor.version.to_string()
    {
        return Err(ApiError::cursor(
            "cursor belongs to another store, chain, or processor",
        ));
    }
    Ok(())
}

fn configured_processor(state: &ApiState, processor: &str) -> Result<Arc<dyn Processor>, ApiError> {
    if let Some(configured) = state.processors.get(processor) {
        return Ok(configured.clone());
    }
    let mut by_kind = state
        .processors
        .values()
        .filter(|configured| configured.descriptor().id.as_str() == processor);
    let configured = by_kind.next();
    if by_kind.next().is_some() {
        return Err(ApiError::invalid(
            "processor kind is ambiguous; address the processor by instance",
        ));
    }
    configured
        .cloned()
        .ok_or_else(|| ApiError::not_found("processor instance is not configured"))
}

fn page_limit(state: &ApiState, requested: Option<usize>) -> Result<usize, ApiError> {
    let limit = requested.unwrap_or(state.config.default_page_size);
    if limit == 0 || limit > state.config.max_page_size {
        Err(ApiError::too_expensive(&format!(
            "limit must be in 1..={}",
            state.config.max_page_size
        )))
    } else {
        Ok(limit)
    }
}

fn decode_block(bytes: &[u8]) -> Result<BlobsBlockEntity, ApiError> {
    postcard::from_bytes(bytes)
        .map_err(|error| ApiError::internal(&format!("stored blobs block is invalid: {error}")))
}

fn decode_transaction(bytes: &[u8]) -> Result<BlobTransactionEntity, ApiError> {
    postcard::from_bytes(bytes).map_err(|error| {
        ApiError::internal(&format!("stored blob transaction is invalid: {error}"))
    })
}

fn parse_hash(value: &str) -> Result<BlockHash, ApiError> {
    let value = value
        .strip_prefix("0x")
        .ok_or_else(|| ApiError::invalid("hash must start with 0x"))?;
    let mut hash = [0_u8; 32];
    hex::decode_to_slice(value, &mut hash)
        .map_err(|_| ApiError::invalid("hash must contain exactly 32 bytes"))?;
    Ok(BlockHash::new(hash))
}

fn parse_address(value: &str) -> Result<Address, ApiError> {
    let value = value
        .strip_prefix("0x")
        .ok_or_else(|| ApiError::invalid("address must start with 0x"))?;
    let mut address = [0_u8; 20];
    hex::decode_to_slice(value, &mut address)
        .map_err(|_| ApiError::invalid("address must contain exactly 20 bytes"))?;
    Ok(Address::new(address))
}

fn hash_hex(value: BlockHash) -> String {
    format!("0x{}", hex::encode(value.0))
}

fn address_hex(value: Address) -> String {
    format!("0x{}", hex::encode(value.0))
}

fn quantity_decimal(value: Quantity) -> String {
    U256::from_be_bytes(value.0).to_string()
}

const fn finality_name(finality: Finality) -> &'static str {
    match finality {
        Finality::Optimistic => "optimistic",
        Finality::Safe => "safe",
        Finality::Finalized => "finalized",
    }
}

fn unix_ms_rfc3339(value: u64) -> String {
    // RFC 3339 UTC with fixed millisecond precision (CloudEvents `time`
    // convention). chrono is already in the workspace dependency tree.
    i64::try_from(value)
        .ok()
        .and_then(chrono::DateTime::from_timestamp_millis)
        .map_or_else(
            // Unreachable before year 292278994; keep the function total.
            || format!("unix-ms:{value}"),
            |timestamp| timestamp.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        )
}

#[derive(Clone, Debug, Error)]
#[error("{message}")]
pub struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
    retryable: bool,
    details: Option<Value>,
}

impl ApiError {
    #[must_use]
    pub fn invalid(message: &str) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "invalid_request", message, false)
    }

    #[must_use]
    pub fn cursor(message: &str) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "cursor_invalid", message, false)
    }

    #[must_use]
    pub fn not_found(message: &str) -> Self {
        Self::new(StatusCode::NOT_FOUND, "not_found", message, false)
    }

    fn conflict(code: &'static str, message: &str) -> Self {
        Self::new(StatusCode::CONFLICT, code, message, false)
    }

    fn unauthorized() -> Self {
        Self::new(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "a valid bearer token is required",
            false,
        )
    }

    fn forbidden(message: &str) -> Self {
        Self::new(StatusCode::FORBIDDEN, "forbidden", message, false)
    }

    #[must_use]
    pub fn too_expensive(message: &str) -> Self {
        Self::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "query_too_expensive",
            message,
            false,
        )
    }

    fn output_not_retained(message: &str) -> Self {
        let mut error = Self::new(StatusCode::GONE, "output_not_retained", message, false);
        error.details = Some(json!({
            "action": "create_processor_instance_or_source_scan",
            "complete": false
        }));
        error
    }

    fn range_incomplete(coverage: CoverageResponse) -> Self {
        let mut error = Self::new(
            StatusCode::CONFLICT,
            "range_incomplete",
            "requested block range has unindexed gaps",
            true,
        );
        error.details = serde_json::to_value(coverage).ok();
        error
    }

    fn cursor_expired(bounds: ChangeBounds) -> Self {
        let mut error = Self::new(
            StatusCode::GONE,
            "cursor_expired",
            "cursor predates the retained durable change log",
            false,
        );
        error.details = Some(json!({
            "earliestAvailableSequence": bounds.earliest.to_string(),
            "latestAvailableSequence": bounds.latest.to_string(),
            "action": "query_snapshot_then_resume"
        }));
        error
    }

    #[must_use]
    pub fn internal(message: &str) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "internal", message, true)
    }

    fn new(status: StatusCode, code: &'static str, message: &str, retryable: bool) -> Self {
        Self {
            status,
            code,
            message: message.to_owned(),
            retryable,
            details: None,
        }
    }

    fn body(&self) -> Value {
        let request_id = REQUEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        json!({
            "error": {
                "code": self.code,
                "message": self.message,
                "retryable": self.retryable,
                "details": self.details,
                "requestId": format!("req-{request_id:016x}")
            }
        })
    }

    fn sse_event(&self) -> Event {
        Event::default()
            .event("error")
            .json_data(self.body())
            .unwrap_or_else(|_| Event::default().event("error").data("{}"))
    }
}

impl From<StoreError> for ApiError {
    fn from(error: StoreError) -> Self {
        match error {
            StoreError::InvalidConfig(message) => Self::invalid(&message),
            matched @ StoreError::ConsumerExists { .. } => {
                Self::conflict("consumer_exists", &matched.to_string())
            }
            matched @ (StoreError::ConsumerNotFound { .. }
            | StoreError::SavepointNotFound { .. }
            | StoreError::RecoveryCheckpointNotFound { .. }) => {
                Self::not_found(&matched.to_string())
            }
            matched @ StoreError::ConsumerInactive { .. } => {
                Self::conflict("consumer_inactive", &matched.to_string())
            }
            matched @ StoreError::ConsumerSessionActive { .. } => {
                Self::conflict("consumer_session_active", &matched.to_string())
            }
            matched @ StoreError::ConsumerSessionLost { .. } => {
                Self::conflict("consumer_session_lost", &matched.to_string())
            }
            StoreError::ConsumerResetRequired {
                earliest_available,
                latest_available,
            } => {
                let mut response = Self::new(
                    StatusCode::GONE,
                    "reset_required",
                    "durable consumer cursor predates retained delivery data",
                    false,
                );
                response.details = Some(json!({
                    "earliestAvailableSequence": earliest_available.map(|value| value.to_string()),
                    "latestAvailableSequence": latest_available.map(|value| value.to_string())
                }));
                response
            }
            matched @ (StoreError::AcknowledgementBackwards { .. }
            | StoreError::AcknowledgementBeyondDelivered { .. }
            | StoreError::AcknowledgementNotBoundary { .. }
            | StoreError::AcknowledgementBeyondHead { .. }) => {
                Self::conflict("acknowledgement_invalid", &matched.to_string())
            }
            matched @ StoreError::QueryTooExpensive { .. } => {
                Self::too_expensive(&matched.to_string())
            }
            StoreError::QuerySnapshotExpired => {
                let mut response = Self::new(
                    StatusCode::GONE,
                    "query_snapshot_expired",
                    "query snapshot is absent or expired",
                    false,
                );
                response.details = Some(json!({ "action": "create_query_snapshot" }));
                response
            }
            matched @ StoreError::QuerySnapshotMismatch => {
                Self::conflict("query_snapshot_mismatch", &matched.to_string())
            }
            matched @ StoreError::SavepointExists { .. } => {
                Self::conflict("savepoint_exists", &matched.to_string())
            }
            matched @ (StoreError::SavepointChecksum | StoreError::SavepointContract) => {
                Self::conflict("savepoint_invalid", &matched.to_string())
            }
            matched @ StoreError::CheckpointRestoreBoundary { .. } => {
                Self::conflict("checkpoint_restore_boundary", &matched.to_string())
            }
            matched @ StoreError::ArtifactContract => Self::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "artifact_contract_incompatible",
                &matched.to_string(),
                false,
            ),
            matched @ (StoreError::ArtifactReplaySameInstance
            | StoreError::ArtifactReplayGap { .. }
            | StoreError::ConflictingArtifact(_)) => {
                Self::conflict("artifact_replay_conflict", &matched.to_string())
            }
            other => Self::internal(&other.to_string()),
        }
    }
}

impl From<serde_json::Error> for ApiError {
    fn from(error: serde_json::Error) -> Self {
        Self::internal(&error.to_string())
    }
}

impl From<std::fmt::Error> for ApiError {
    fn from(error: std::fmt::Error) -> Self {
        Self::internal(&error.to_string())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let unauthorized = self.status == StatusCode::UNAUTHORIZED;
        let mut response = (self.status, Json(self.body())).into_response();
        if unauthorized {
            response.headers_mut().insert(
                header::WWW_AUTHENTICATE,
                axum::http::HeaderValue::from_static("Bearer"),
            );
        }
        response
    }
}

#[cfg(test)]
mod tests {
    use axum::{
        body::{Body, to_bytes},
        http::Request,
    };
    use leani_primitives::{Address, ChainId, ProcessorCursor, TransactionHash};
    use tower::ServiceExt;

    use super::*;

    #[derive(Clone, Copy, Debug)]
    struct FixtureQueryExtension {
        id: &'static str,
        alias: Option<&'static str>,
    }

    impl QueryExtension for FixtureQueryExtension {
        fn id(&self) -> &'static str {
            self.id
        }

        fn alias(&self) -> Option<&str> {
            self.alias
        }

        fn routes(&self) -> Router<QueryContext> {
            Router::new().route("/identity", get(fixture_extension_identity))
        }
    }

    async fn fixture_extension_identity(State(context): State<QueryContext>) -> Json<Value> {
        Json(json!({
            "processor": context.descriptor().instance,
            "extension": context.extension_id(),
        }))
    }

    async fn seed_blob_block(
        store: &SqliteStore,
        processor: &BlobsProcessor,
        number: u64,
        transaction_count: u32,
    ) {
        let hash_byte = u8::try_from(number % 251).expect("small hash byte");
        let parent_byte = u8::try_from(number.saturating_sub(1) % 251).expect("small parent byte");
        let block_hash = BlockHash::new([hash_byte; 32]);
        let parent_hash = BlockHash::new([parent_byte; 32]);
        let block = BlobsBlockEntity {
            network: "mainnet".to_owned(),
            block_number: number,
            block_hash,
            parent_hash,
            timestamp: number,
            finality: Finality::Finalized,
            size_bytes: 1,
            blob_count: transaction_count,
            blob_gas_used: u64::from(transaction_count) * 131_072,
            excess_blob_gas: 0,
            blob_base_fee: Quantity::new([1; 32]),
            execution_base_fee: Quantity::new([2; 32]),
            gas_used: 21_000,
            gas_limit: 30_000_000,
            execution_burn: Quantity::new([3; 32]),
            blob_burn: Some(Quantity::new([4; 32])),
            reserve_fee: None,
            transaction_count,
            target_blobs_per_block: 3,
            max_blobs_per_block: 6,
            transform_version: 3,
        };
        let transactions = (0..transaction_count)
            .map(|index| {
                let mut transaction_hash = [0_u8; 32];
                transaction_hash[24..].copy_from_slice(&u64::from(index + 1).to_be_bytes());
                BlobTransactionEntity {
                    network: "mainnet".to_owned(),
                    block_number: number,
                    block_hash,
                    transaction_hash: TransactionHash::new(transaction_hash),
                    transaction_index: index,
                    sender: Address::new([5; 20]),
                    blob_versioned_hashes: vec![BlockHash::new([6; 32])],
                    blob_count: 1,
                    total_burn: Quantity::new([7; 32]),
                    execution_burn: Quantity::new([8; 32]),
                    blob_burn: Quantity::new([9; 32]),
                }
            })
            .collect();
        let payload = postcard::to_allocvec(&BlobsDelta {
            block,
            transactions,
        })
        .expect("blob delta");
        let block_ref = leani_primitives::BlockRef {
            number: BlockNumber(number),
            hash: block_hash,
            parent_hash,
            timestamp: number,
        };
        let delta = leani_processor_api::EncodedDelta::new(
            processor.descriptor(),
            ChainId(1),
            block_ref,
            payload,
        );
        store
            .apply(
                processor,
                ProcessorCursor {
                    processor_id: processor.descriptor().id.to_string(),
                    processor_version: processor.descriptor().version.to_string(),
                    chain_id: ChainId(1),
                    block_number: BlockNumber(number),
                    block_hash,
                    finality: Finality::Finalized,
                    sequence: number,
                },
                &delta,
                &[],
            )
            .await
            .expect("apply blob fixture");
    }

    #[test]
    fn stream_batching_overrides_can_only_tighten_durable_limits() {
        let persisted = DeliveryBatchLimits::history_default();
        let effective = stricter_stream_batch_limits(
            persisted,
            ConsumerStreamBatchingQuery {
                target_encoded_bytes: Some(256 * 1_024),
                maximum_encoded_bytes: Some(1024 * 1_024),
                maximum_events: Some(2_000),
                maximum_processed_blocks: Some(512),
                maximum_delay_ms: Some(10),
            },
        )
        .expect("strict profile");
        assert_eq!(effective.maximum_processed_blocks, 512);
        assert_eq!(effective.maximum_delay, Duration::from_millis(10));

        assert!(
            stricter_stream_batch_limits(
                persisted,
                ConsumerStreamBatchingQuery {
                    maximum_encoded_bytes: Some(persisted.maximum_encoded_bytes.saturating_add(1)),
                    ..ConsumerStreamBatchingQuery::default()
                },
            )
            .is_err()
        );
    }

    #[derive(Debug)]
    struct FixtureBackfillControl;

    #[async_trait]
    impl BackfillControl for FixtureBackfillControl {
        async fn create_historical_work(
            &self,
            request: CreateBackfillRequest,
            owner: HistoricalWorkOwner,
        ) -> Result<BackfillStatus, BackfillControlError> {
            let ranges = if request.ranges.is_empty() {
                vec![BackfillRange {
                    from_block: request.from_block.ok_or_else(|| {
                        BackfillControlError::Invalid("fromBlock is required".to_owned())
                    })?,
                    to_block: request
                        .to_block
                        .ok_or_else(|| {
                            BackfillControlError::Invalid("toBlock is required".to_owned())
                        })?
                        .resolve(100),
                }]
            } else {
                request
                    .ranges
                    .iter()
                    .map(|range| BackfillRange {
                        from_block: range.from_block,
                        to_block: range.to_block.resolve(100),
                    })
                    .collect::<Vec<_>>()
            };
            let from_block = ranges
                .first()
                .expect("fixture range is non-empty")
                .from_block;
            let to_block = ranges.last().expect("fixture range is non-empty").to_block;
            let requested_blocks = ranges
                .iter()
                .map(|range| {
                    range
                        .to_block
                        .saturating_sub(range.from_block)
                        .saturating_add(1)
                })
                .sum();
            Ok(BackfillStatus {
                id: format!("api:{}", request.idempotency_key),
                owner,
                processor: request.processor,
                delivery_stream_id: None,
                publication_revision: None,
                from_block,
                to_block,
                ranges,
                requested_blocks,
                processed_blocks: 0,
                remaining_blocks: requested_blocks,
                captured_finalized_target: Some(to_block),
                mode: request.mode,
                batching: None,
                state: BackfillState::Queued,
                attempts: 0,
                updated_at_unix_ms: 1,
                report: None,
                last_error: None,
            })
        }

        async fn list(
            &self,
            _owner: Option<HistoricalWorkOwner>,
        ) -> Result<Vec<BackfillStatus>, BackfillControlError> {
            Ok(Vec::new())
        }

        async fn inspect(&self, id: &str) -> Result<BackfillStatus, BackfillControlError> {
            Err(BackfillControlError::NotFound(id.to_owned()))
        }

        async fn cancel(&self, id: &str) -> Result<BackfillStatus, BackfillControlError> {
            Err(BackfillControlError::NotFound(id.to_owned()))
        }

        async fn delete(&self, id: &str) -> Result<HistoricalWorkDeletion, BackfillControlError> {
            Err(BackfillControlError::NotFound(id.to_owned()))
        }
    }

    #[derive(Debug)]
    struct FixtureRawHistoryControl;

    #[async_trait]
    impl RawHistoryControl for FixtureRawHistoryControl {
        async fn create(
            &self,
            _request: CreateRawHistoryJobRequest,
        ) -> Result<RawHistoryJob, RawHistoryControlError> {
            Err(RawHistoryControlError::Invalid(
                "fixture rejected create".to_owned(),
            ))
        }

        async fn list(&self) -> Result<Vec<RawHistoryJob>, RawHistoryControlError> {
            Ok(Vec::new())
        }

        async fn inspect(&self, id: &str) -> Result<RawHistoryJob, RawHistoryControlError> {
            Err(RawHistoryControlError::NotFound(id.to_owned()))
        }

        async fn cancel(&self, id: &str) -> Result<RawHistoryJob, RawHistoryControlError> {
            Err(RawHistoryControlError::NotFound(id.to_owned()))
        }

        async fn delete(&self, id: &str) -> Result<RawHistoryJobDeletion, RawHistoryControlError> {
            Err(RawHistoryControlError::NotFound(id.to_owned()))
        }
    }

    #[derive(Debug)]
    struct StaticBackfillControl {
        status: BackfillStatus,
    }

    #[async_trait]
    impl BackfillControl for StaticBackfillControl {
        async fn create_historical_work(
            &self,
            _request: CreateBackfillRequest,
            owner: HistoricalWorkOwner,
        ) -> Result<BackfillStatus, BackfillControlError> {
            let mut status = self.status.clone();
            status.owner = owner;
            Ok(status)
        }

        async fn list(
            &self,
            owner: Option<HistoricalWorkOwner>,
        ) -> Result<Vec<BackfillStatus>, BackfillControlError> {
            Ok((owner.is_none() || owner == Some(self.status.owner))
                .then(|| self.status.clone())
                .into_iter()
                .collect())
        }

        async fn inspect(&self, id: &str) -> Result<BackfillStatus, BackfillControlError> {
            if id == self.status.id {
                Ok(self.status.clone())
            } else {
                Err(BackfillControlError::NotFound(id.to_owned()))
            }
        }

        async fn cancel(&self, id: &str) -> Result<BackfillStatus, BackfillControlError> {
            self.inspect(id).await
        }

        async fn delete(&self, id: &str) -> Result<HistoricalWorkDeletion, BackfillControlError> {
            let status = self.inspect(id).await?;
            Ok(HistoricalWorkDeletion {
                id: id.to_owned(),
                owner: status.owner,
                removed_jobs: 2,
                removed_subscription_ranges: 1,
                removed_consumers: 1,
                removed_delivery_records: 3,
                removed_delivery_streams: 1,
                removed_coverage_intervals: 0,
                removed_coverage_segments: 0,
                removed_exact_coverage: 0,
                removed_applied_blocks: 0,
                removed_finalized_undo: 0,
                retained_processor_output: true,
                retained_live_stream: status.owner == HistoricalWorkOwner::Subscription,
            })
        }
    }

    fn block() -> BlobsBlockEntity {
        BlobsBlockEntity {
            network: "mainnet".to_owned(),
            block_number: 1,
            block_hash: BlockHash::new([1; 32]),
            parent_hash: BlockHash::new([0; 32]),
            timestamp: 2,
            finality: Finality::Finalized,
            size_bytes: 3,
            blob_count: 1,
            blob_gas_used: 131_072,
            excess_blob_gas: 0,
            blob_base_fee: Quantity::new([1; 32]),
            execution_base_fee: Quantity::new([2; 32]),
            gas_used: 4,
            gas_limit: 5,
            execution_burn: Quantity::new([3; 32]),
            blob_burn: Some(Quantity::new([4; 32])),
            reserve_fee: None,
            transaction_count: 1,
            target_blobs_per_block: 3,
            max_blobs_per_block: 6,
            transform_version: 3,
        }
    }

    #[test]
    fn quantities_and_hashes_are_lossless_strings() {
        let wire = BlobsBlock::from(&block());
        let value = serde_json::to_value(wire).expect("JSON");
        assert_eq!(value["blockHash"], format!("0x{}", "01".repeat(32)));
        assert!(
            value["blobBaseFee"]
                .as_str()
                .expect("decimal")
                .parse::<U256>()
                .is_ok()
        );
        assert_eq!(value["finality"], "finalized");
    }

    #[test]
    fn transaction_wire_retains_block_and_blob_identity() {
        let entity = BlobTransactionEntity {
            network: "mainnet".to_owned(),
            block_number: 1,
            block_hash: BlockHash::new([1; 32]),
            transaction_hash: TransactionHash::new([2; 32]),
            transaction_index: 3,
            sender: Address::new([4; 20]),
            blob_versioned_hashes: vec![BlockHash::new([5; 32])],
            blob_count: 1,
            total_burn: Quantity::new([6; 32]),
            execution_burn: Quantity::new([7; 32]),
            blob_burn: Quantity::new([8; 32]),
        };
        let value = serde_json::to_value(BlobTransaction::from(&entity)).expect("JSON");
        assert_eq!(value["transactionIndex"], 3);
        assert_eq!(
            value["blobVersionedHashes"][0],
            format!("0x{}", "05".repeat(32))
        );
    }

    #[test]
    fn blobs_change_wire_contains_one_atomic_block_bundle() {
        let transaction = BlobTransactionEntity {
            network: "mainnet".to_owned(),
            block_number: 1,
            block_hash: BlockHash::new([1; 32]),
            transaction_hash: TransactionHash::new([2; 32]),
            transaction_index: 3,
            sender: Address::new([4; 20]),
            blob_versioned_hashes: vec![BlockHash::new([5; 32])],
            blob_count: 1,
            total_burn: Quantity::new([6; 32]),
            execution_burn: Quantity::new([7; 32]),
            blob_burn: Quantity::new([8; 32]),
        };
        let bundle = BlobsDelta {
            block: block(),
            transactions: vec![transaction],
        };
        let value =
            serde_json::to_value(BlobsBlockChange::from(&bundle)).expect("block bundle JSON");
        assert_eq!(value["block"]["blockNumber"], 1);
        assert_eq!(value["transactions"][0]["blockNumber"], 1);
        assert_eq!(value["transactions"][0]["transactionIndex"], 3);
        assert_eq!(
            value["transactions"][0]["txHash"],
            format!("0x{}", "02".repeat(32))
        );
    }

    #[test]
    fn cursor_detects_corruption() {
        let cursor = ApiCursor {
            version: 1,
            epoch: [1; 16],
            chain_id: 1,
            processor_id: "blobs-money".to_owned(),
            processor_version: "1.2.0".to_owned(),
            sequence: 42,
        };
        let encoded = encode_cursor(&cursor).expect("encode");
        assert_eq!(decode_cursor(&encoded).expect("decode").sequence, 42);
        let mut corrupt = encoded.into_bytes();
        let last = corrupt.last_mut().expect("nonempty");
        *last = if *last == b'0' { b'1' } else { b'0' };
        assert!(decode_cursor(&String::from_utf8(corrupt).expect("ASCII")).is_err());
    }

    #[tokio::test]
    async fn stream_cursor_cannot_cross_delivery_streams() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteStore::open(leani_store_sqlite::StoreConfig::new(
            directory.path().join("node.sqlite"),
        ))
        .await
        .expect("store");
        let processor: Arc<dyn Processor> = Arc::new(BlobsProcessor::default());
        let processors = Arc::new(BTreeMap::from([(
            processor.descriptor().instance.to_string(),
            processor.clone(),
        )]));
        let query_extensions = Arc::new(Vec::new());
        let state = ApiState {
            store,
            primary: processor.clone(),
            capabilities: Arc::new(capabilities_response(1, &processors, &query_extensions)),
            processors,
            query_extensions,
            config: ApiConfig::default(),
            started: Instant::now(),
        };
        let cursor =
            encode_stream_cursor(&state, processor.as_ref(), "history-a", 42).expect("cursor");
        assert_eq!(
            decode_stream_change_cursor(&state, processor.as_ref(), "history-a", &cursor)
                .expect("same stream cursor")
                .sequence,
            42
        );
        assert!(
            decode_stream_change_cursor(&state, processor.as_ref(), "history-b", &cursor).is_err()
        );
        assert!(decode_change_cursor(&state, processor.as_ref(), &cursor).is_err());
    }

    #[tokio::test]
    async fn destination_commit_helper_never_acknowledges_a_failed_commit() {
        let acknowledged = Arc::new(AtomicBool::new(false));
        let observed = acknowledged.clone();
        let result = commit_then_ack(
            async { Err::<(), _>("commit failed") },
            move || async move {
                observed.store(true, Ordering::Release);
                Ok::<_, &str>(())
            },
        )
        .await;
        assert_eq!(
            result,
            Err(CommitThenAckError::DestinationCommit("commit failed"))
        );
        assert!(!acknowledged.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn empty_store_router_is_constructible() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteStore::open(leani_store_sqlite::StoreConfig::new(
            directory.path().join("node.sqlite"),
        ))
        .await
        .expect("store");
        let _router = router(
            store,
            Arc::new(BlobsProcessor::default()),
            ApiConfig::default(),
        )
        .expect("router");
    }

    #[tokio::test]
    async fn query_extension_is_instance_scoped_discoverable_and_authenticated() {
        use leani_testkit::BlockLocalCounter;

        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteStore::open(leani_store_sqlite::StoreConfig::new(
            directory.path().join("node.sqlite"),
        ))
        .await
        .expect("store");
        let processor: Arc<dyn Processor> = Arc::new(BlockLocalCounter::named("orders"));
        let instance = processor.descriptor().instance.to_string();
        let registration = QueryExtensionRegistration::new(
            processor.clone(),
            Arc::new(FixtureQueryExtension {
                id: "orders-v1",
                alias: Some("orders"),
            }),
        );
        let app = router_with_processors(
            store,
            vec![processor],
            vec![registration],
            ApiConfig {
                bearer_token: Some(Arc::from("secret")),
                ..ApiConfig::default()
            },
        )
        .expect("router");

        let unauthorized = app
            .clone()
            .oneshot(
                Request::get(format!("/v1/processors/{instance}/query/identity"))
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

        for path in [
            format!("/v1/processors/{instance}/query/identity"),
            "/v1/q/orders/identity".to_owned(),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::get(&path)
                        .header(header::AUTHORIZATION, "Bearer secret")
                        .body(Body::empty())
                        .expect("request"),
                )
                .await
                .expect("response");
            assert_eq!(response.status(), StatusCode::OK);
            let body = to_bytes(response.into_body(), 1024).await.expect("body");
            let body: Value = serde_json::from_slice(&body).expect("JSON");
            assert_eq!(body["processor"], instance);
            assert_eq!(body["extension"], "orders-v1");
        }

        let response = app
            .oneshot(
                Request::get("/v1/capabilities")
                    .header(header::AUTHORIZATION, "Bearer secret")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        let body = to_bytes(response.into_body(), 4096).await.expect("body");
        let body: Value = serde_json::from_slice(&body).expect("JSON");
        assert_eq!(body["processors"][0]["genericApi"], "processor-v1");
        assert_eq!(body["processors"][0]["changeSchema"], "orders.change.v1");
        assert_eq!(
            body["processors"][0]["queryExtensions"][0]["basePath"],
            format!("/v1/processors/{instance}/query")
        );
        assert_eq!(body["queryExtensions"][0]["aliasPath"], "/v1/q/orders");
    }

    #[tokio::test]
    async fn duplicate_query_aliases_keep_canonical_routes_and_suppress_alias() {
        use leani_testkit::BlockLocalCounter;

        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteStore::open(leani_store_sqlite::StoreConfig::new(
            directory.path().join("node.sqlite"),
        ))
        .await
        .expect("store");
        let first: Arc<dyn Processor> = Arc::new(BlockLocalCounter::named("first"));
        let second: Arc<dyn Processor> = Arc::new(BlockLocalCounter::named("second"));
        let instances = [
            first.descriptor().instance.to_string(),
            second.descriptor().instance.to_string(),
        ];
        let registrations = [&first, &second]
            .into_iter()
            .map(|processor| {
                QueryExtensionRegistration::new(
                    processor.clone(),
                    Arc::new(FixtureQueryExtension {
                        id: "shared-v1",
                        alias: Some("shared"),
                    }),
                )
            })
            .collect();
        let app = router_with_processors(
            store,
            vec![first, second],
            registrations,
            ApiConfig::default(),
        )
        .expect("router");

        for instance in instances {
            let response = app
                .clone()
                .oneshot(
                    Request::get(format!("/v1/processors/{instance}/query/identity"))
                        .body(Body::empty())
                        .expect("request"),
                )
                .await
                .expect("response");
            assert_eq!(response.status(), StatusCode::OK);
        }
        let response = app
            .clone()
            .oneshot(
                Request::get("/v1/q/shared/identity")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let response = app
            .oneshot(
                Request::get("/v1/capabilities")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        let body = to_bytes(response.into_body(), 4096).await.expect("body");
        let body: Value = serde_json::from_slice(&body).expect("JSON");
        assert_eq!(body["queryExtensions"][0]["aliasPath"], Value::Null);
        assert_eq!(body["queryExtensions"][1]["aliasPath"], Value::Null);
    }

    #[tokio::test]
    async fn query_extension_registration_rejects_wrong_owner_and_duplicate_owner() {
        use leani_testkit::BlockLocalCounter;

        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteStore::open(leani_store_sqlite::StoreConfig::new(
            directory.path().join("node.sqlite"),
        ))
        .await
        .expect("store");
        let registered: Arc<dyn Processor> = Arc::new(BlockLocalCounter::named("orders"));
        let lookalike: Arc<dyn Processor> = Arc::new(BlockLocalCounter::named("orders"));
        let extension: Arc<dyn QueryExtension> = Arc::new(FixtureQueryExtension {
            id: "orders-v1",
            alias: None,
        });
        let error = router_with_processors(
            store.clone(),
            vec![registered.clone()],
            vec![QueryExtensionRegistration::new(
                lookalike,
                extension.clone(),
            )],
            ApiConfig::default(),
        )
        .expect_err("lookalike owner must be rejected");
        assert_eq!(error.code, "invalid_request");

        let error = router_with_processors(
            store,
            vec![registered.clone()],
            vec![
                QueryExtensionRegistration::new(registered.clone(), extension.clone()),
                QueryExtensionRegistration::new(registered.clone(), extension),
            ],
            ApiConfig::default(),
        )
        .expect_err("duplicate owner must be rejected");
        assert_eq!(error.code, "invalid_request");

        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteStore::open(leani_store_sqlite::StoreConfig::new(
            directory.path().join("invalid-metadata.sqlite"),
        ))
        .await
        .expect("store");
        let error = router_with_processors(
            store,
            vec![registered.clone()],
            vec![QueryExtensionRegistration::new(
                registered,
                Arc::new(FixtureQueryExtension {
                    id: "Invalid ID",
                    alias: Some("processors"),
                }),
            )],
            ApiConfig::default(),
        )
        .expect_err("invalid extension metadata must be rejected");
        assert_eq!(error.code, "invalid_request");

        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteStore::open(leani_store_sqlite::StoreConfig::new(
            directory.path().join("reserved-alias.sqlite"),
        ))
        .await
        .expect("store");
        let custom: Arc<dyn Processor> = Arc::new(BlockLocalCounter::named("custom"));
        let error = router_with_processors(
            store,
            vec![custom.clone()],
            vec![QueryExtensionRegistration::new(
                custom,
                Arc::new(FixtureQueryExtension {
                    id: "custom-v1",
                    alias: Some("blobs"),
                }),
            )],
            ApiConfig::default(),
        )
        .expect_err("built-in alias squatting must be rejected");
        assert_eq!(error.code, "invalid_request");
    }

    #[tokio::test]
    async fn query_extension_scan_cursors_are_bound_to_extension_namespace() {
        use leani_testkit::BlockLocalCounter;

        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteStore::open(leani_store_sqlite::StoreConfig::new(
            directory.path().join("node.sqlite"),
        ))
        .await
        .expect("store");
        let processor: Arc<dyn Processor> = Arc::new(BlockLocalCounter::named("orders"));
        let instance = processor.descriptor().instance.to_string();
        let other: Arc<dyn Processor> = Arc::new(BlockLocalCounter::named("other"));
        let other_instance = other.descriptor().instance.to_string();
        let processors = Arc::new(BTreeMap::from([
            (instance, processor.clone()),
            (other_instance, other.clone()),
        ]));
        let query_extensions = Arc::new(Vec::new());
        let state = ApiState {
            store,
            primary: processor.clone(),
            capabilities: Arc::new(capabilities_response(1, &processors, &query_extensions)),
            processors,
            query_extensions,
            config: ApiConfig::default(),
            started: Instant::now(),
        };
        let first = QueryContext::new(state.clone(), processor.clone(), Arc::from("orders-v1"));
        let second = QueryContext::new(state, processor, Arc::from("orders-v2"));
        let other = QueryContext::new(second.state.clone(), other, Arc::from("orders-v1"));
        let cursor = first
            .encode_scan_cursor("by-owner", b"last-key")
            .expect("cursor");
        assert_eq!(
            first
                .decode_scan_cursor("by-owner", &cursor)
                .expect("same scope"),
            b"last-key"
        );
        assert!(first.decode_scan_cursor("by-token", &cursor).is_err());
        assert!(second.decode_scan_cursor("by-owner", &cursor).is_err());
        assert!(other.decode_scan_cursor("by-owner", &cursor).is_err());
        let scoped = first
            .encode_scoped_scan_cursor("by-owner", b"blocks:100-200", b"last-key")
            .expect("scoped cursor");
        assert_eq!(
            first
                .decode_scoped_scan_cursor("by-owner", b"blocks:100-200", &scoped)
                .expect("same request scope"),
            b"last-key"
        );
        assert!(
            first
                .decode_scoped_scan_cursor("by-owner", b"blocks:300-400", &scoped)
                .is_err()
        );
        assert!(first.scan("orders", None, 0).await.is_err());
        assert!(first.index_keys("by-owner", b"owner", 0).await.is_err());
    }

    #[tokio::test]
    async fn raw_history_admin_routes_use_the_independent_control_plane() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteStore::open(leani_store_sqlite::StoreConfig::new(
            directory.path().join("node.sqlite"),
        ))
        .await
        .expect("store");
        let app = router(
            store,
            Arc::new(BlobsProcessor::default()),
            ApiConfig {
                raw_history_control: Some(Arc::new(FixtureRawHistoryControl)),
                ..ApiConfig::default()
            },
        )
        .expect("router");
        let list = app
            .clone()
            .oneshot(
                Request::get("/admin/v1/raw-history-jobs")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(list.status(), StatusCode::OK);
        let body = to_bytes(list.into_body(), 1024).await.expect("body");
        assert_eq!(
            serde_json::from_slice::<Value>(&body).expect("JSON")["data"],
            json!([])
        );

        let request = CreateRawHistoryJobRequest {
            idempotency_key: "fixture-raw".to_owned(),
            ranges: vec![BackfillRange {
                from_block: 1,
                to_block: 2,
            }],
            profile: RawHistoryProfileName::ProcessorReuse,
            material: RawHistoryMaterialProfile::default(),
            required_capabilities: leani_primitives::CapabilitySet::of(
                leani_primitives::Capability::Transactions,
            ),
            verification: VerificationClass::TrustedDataset,
            minimum_trust: leani_primitives::TrustModel::TrustedDataset,
            retention: RawHistoryRetention::Full,
            segment: RawHistorySegmentPolicy {
                target_blocks: 2,
                maximum_logical_bytes: 1024,
                maximum_physical_bytes: 1024,
                compression: leani_store_history::Compression::Snappy,
                on_limit: leani_store_history::StorageLimitAction::Pause,
            },
            indexes: RawHistoryIndexPolicy::default(),
        };
        let create = app
            .oneshot(
                Request::post("/admin/v1/raw-history-jobs")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&request).expect("encode request"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(create.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn protected_admin_api_submits_backfill_subscriptions() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteStore::open(leani_store_sqlite::StoreConfig::new(
            directory.path().join("node.sqlite"),
        ))
        .await
        .expect("store");
        let router = router(
            store,
            Arc::new(BlobsProcessor::default()),
            ApiConfig {
                backfill_control: Some(Arc::new(FixtureBackfillControl)),
                ..ApiConfig::default()
            },
        )
        .expect("router");
        let response = router
            .oneshot(
                Request::post("/admin/v1/backfill-subscriptions")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({
                            "processor": "blobs-money",
                            "fromBlock": 20_000_000,
                            "toBlock": 20_000_100,
                            "mode": "recompute",
                            "consumer": {
                                "id": "blobs-api",
                                "role": "required",
                                "leaseTtlSeconds": 300,
                                "credential": "local-consumer-secret"
                            },
                            "limits": {
                                "maxUnacknowledgedBlocks": 16384,
                                "maxUnacknowledgedBytes": 536_870_912,
                                "resumeBelowRatio": 0.75
                            },
                            "idempotencyKey": "startup-gap-1"
                        })
                        .to_string(),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body: Value = serde_json::from_slice(
            &to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("body"),
        )
        .expect("JSON");
        assert_eq!(body["id"], "api:startup-gap-1");
        assert_eq!(body["owner"], "subscription");
        assert_eq!(body["processor"], "blobs-money");
        assert_eq!(body["fromBlock"], 20_000_000);
        assert_eq!(body["toBlock"], 20_000_100);
        assert_eq!(
            body["ranges"],
            json!([{"fromBlock": 20_000_000, "toBlock": 20_000_100}])
        );
        assert_eq!(body["requestedBlocks"], 101);
        assert_eq!(body["mode"], "recompute");
        assert_eq!(body["state"], "queued");
    }

    #[tokio::test]
    async fn protected_admin_api_submits_node_materialization_jobs_without_a_consumer() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteStore::open(leani_store_sqlite::StoreConfig::new(
            directory.path().join("node.sqlite"),
        ))
        .await
        .expect("store");
        let router = router(
            store,
            Arc::new(BlobsProcessor::default()),
            ApiConfig {
                backfill_control: Some(Arc::new(FixtureBackfillControl)),
                ..ApiConfig::default()
            },
        )
        .expect("router");
        let response = router
            .oneshot(
                Request::post("/admin/v1/materialization-jobs")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({
                            "processor": "blobs-money",
                            "fromBlock": 20_000_000,
                            "toBlock": 20_000_100,
                            "idempotencyKey": "local-analysis-1"
                        })
                        .to_string(),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body: Value = serde_json::from_slice(
            &to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("body"),
        )
        .expect("JSON");
        assert_eq!(body["owner"], "materialization");
        assert!(body.get("deliveryStreamId").is_none());
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn backfill_consumer_stream_batches_complete_blocks_and_holds_one_session() {
        use leani_primitives::ProcessorCursor;
        use leani_testkit::{BlockLocalCounter, fixture_frame};

        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteStore::open(leani_store_sqlite::StoreConfig::new(
            directory.path().join("node.sqlite"),
        ))
        .await
        .expect("store");
        let processor = Arc::new(BlockLocalCounter::default().with_split_delivery());
        store
            .register_processor(processor.descriptor())
            .await
            .expect("register processor");
        let subscription_id = "stream-fixture";
        let stream_id = store
            .create_backfill_delivery_stream(processor.descriptor(), subscription_id)
            .await
            .expect("history stream")
            .stream_id;
        store
            .create_consumer_in_stream(
                processor.descriptor(),
                &stream_id,
                "destination",
                ConsumerRole::Required,
                ConsumerStartPosition::EarliestRetained,
                Duration::from_secs(30),
            )
            .await
            .expect("consumer");
        let job = leani_store_sqlite::JobRecord {
            id: subscription_id.to_owned(),
            kind: "backfill_subscription_job".to_owned(),
            state: leani_store_sqlite::JobState::Queued,
            payload: b"api-stream-fixture".to_vec(),
            checkpoint: None,
            attempts: 0,
            updated_at_unix_ms: 1,
        };
        store
            .create_backfill_subscription_job(
                &leani_store_sqlite::BackfillSubscriptionRecord {
                    subscription_id: subscription_id.to_owned(),
                    job_id: job.id.clone(),
                    processor_instance: processor.descriptor().instance.to_string(),
                    history_stream_id: stream_id.clone(),
                    mode: leani_store_sqlite::BackfillSubscriptionMode::FillMissing,
                    publication_revision: 0,
                    state: leani_store_sqlite::BackfillSubscriptionState::Queued,
                    consumer_id: "destination".to_owned(),
                    ranges: vec![
                        leani_primitives::BlockRange::new(BlockNumber(1), BlockNumber(3))
                            .expect("range"),
                    ],
                    range: leani_primitives::BlockRange::new(BlockNumber(1), BlockNumber(3))
                        .expect("range"),
                    preexisting_coverage: Vec::new(),
                    captured_finalized_target: BlockNumber(3),
                    idempotency_key: "api-stream-fixture".to_owned(),
                    effective_block_limit: 3,
                    effective_byte_limit: 1024 * 1024,
                    resume_below_ratio_millionths: 750_000,
                    delivery_batch_limits: leani_store_sqlite::BackfillDeliveryBatchLimits::default(
                    ),
                    initial_sequence: 0,
                    completion_sequence: None,
                    processed_work_blocks: 0,
                },
                &job,
                BlockHash::new([4; 32]),
            )
            .await
            .expect("subscription");
        let mut parent = BlockHash::ZERO;
        for number in 1..=3 {
            let frame = fixture_frame(number, parent);
            let delta = processor.map(&frame).await.expect("map");
            store
                .apply_with_change_publication_to_stream(
                    processor.as_ref(),
                    ProcessorCursor {
                        chain_id: frame.chain_id,
                        processor_id: processor.descriptor().id.to_string(),
                        processor_version: processor.descriptor().version.to_string(),
                        block_number: frame.block.number,
                        block_hash: frame.block.hash,
                        finality: Finality::Finalized,
                        sequence: number,
                    },
                    &delta,
                    &[],
                    true,
                    &stream_id,
                )
                .await
                .expect("history apply");
            parent = frame.block.hash;
        }
        store
            .append_backfill_completion(
                processor.descriptor(),
                &stream_id,
                leani_primitives::ChainId(1),
                BlockNumber(3),
            )
            .await
            .expect("completion");
        let control = Arc::new(StaticBackfillControl {
            status: BackfillStatus {
                id: subscription_id.to_owned(),
                owner: HistoricalWorkOwner::Subscription,
                processor: processor.descriptor().instance.to_string(),
                delivery_stream_id: Some(stream_id.clone()),
                publication_revision: Some("7".to_owned()),
                from_block: 1,
                to_block: 3,
                ranges: vec![BackfillRange {
                    from_block: 1,
                    to_block: 3,
                }],
                requested_blocks: 3,
                processed_blocks: 3,
                remaining_blocks: 0,
                captured_finalized_target: Some(3),
                mode: BackfillExecutionMode::FillMissing,
                batching: None,
                state: BackfillState::Draining,
                attempts: 1,
                updated_at_unix_ms: 1,
                report: None,
                last_error: None,
            },
        });
        let app = router_with_processors(
            store,
            vec![processor],
            Vec::new(),
            ApiConfig {
                backfill_control: Some(control),
                history_batch_limits: DeliveryBatchLimits {
                    target_encoded_bytes: 1024 * 1024,
                    maximum_encoded_bytes: 2 * 1024 * 1024,
                    maximum_events: 100,
                    maximum_processed_blocks: 2,
                    maximum_delay: Duration::from_secs(1),
                    maximum_buffered_batches: 2,
                    maximum_buffered_bytes: 4 * 1024 * 1024,
                    compression: DeliveryCompression::Gzip,
                },
                ..ApiConfig::default()
            },
        )
        .expect("router");
        let path = "/v1/backfill-subscriptions/stream-fixture/consumers/destination/stream";
        let first = app
            .clone()
            .oneshot(
                Request::get(path)
                    .header(header::ACCEPT_ENCODING, "gzip")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(
            first.headers()[header::CONTENT_TYPE],
            "application/x-ndjson"
        );
        assert_eq!(first.headers()[header::CONTENT_ENCODING], "gzip");

        let duplicate = app
            .clone()
            .oneshot(Request::get(path).body(Body::empty()).expect("request"))
            .await
            .expect("duplicate response");
        assert_eq!(duplicate.status(), StatusCode::CONFLICT);
        let duplicate_body: Value = serde_json::from_slice(
            &to_bytes(duplicate.into_body(), usize::MAX)
                .await
                .expect("duplicate body"),
        )
        .expect("duplicate JSON");
        assert_eq!(duplicate_body["error"]["code"], "consumer_session_active");

        let mut chunks = first.into_body().into_data_stream();
        let mut decoder = flate2::write::GzDecoder::new(Vec::new());
        let mut plain_bytes = Vec::new();
        let mut session_token = None;
        let mut completion_cursor = None;
        let mut batches = 0;
        for expected in ["hello", "batch", "batch", "backfill_complete"] {
            while !plain_bytes.contains(&b'\n') {
                let chunk = chunks
                    .next()
                    .await
                    .expect("stream frame")
                    .expect("stream bytes");
                decoder.write_all(&chunk).expect("gzip stream chunk");
                decoder.flush().expect("flush gzip decoder");
                plain_bytes.extend_from_slice(&std::mem::take(decoder.get_mut()));
            }
            let newline = plain_bytes
                .iter()
                .position(|byte| *byte == b'\n')
                .expect("decoded line");
            let line = plain_bytes.drain(..=newline).collect::<Vec<_>>();
            let record: Value = serde_json::from_slice(&line[..newline]).expect("NDJSON record");
            assert_eq!(
                record["type"], expected,
                "unexpected stream record: {record}"
            );
            if expected == "hello" {
                session_token = record["sessionToken"].as_str().map(str::to_owned);
            } else if expected == "batch" {
                batches += 1;
                let event_payload =
                    serde_json::to_vec(&record["changes"]).expect("encoded event payload");
                assert_eq!(
                    record["uncompressedEncodedBytes"],
                    event_payload.len().to_string()
                );
                assert!(
                    record["transmittedBytes"]
                        .as_str()
                        .expect("transmitted bytes")
                        .parse::<usize>()
                        .expect("numeric transmitted bytes")
                        < event_payload.len()
                );
                let expected_events = if batches == 1 { 2 } else { 1 };
                let expected_blocks = expected_events.to_string();
                assert_eq!(record["processedBlockCount"], expected_blocks);
                assert_eq!(record["domainChangeCount"], expected_blocks);
                assert_eq!(record["progressUnitCount"], expected_blocks);
                assert!(record["acknowledgeableCursor"].is_string());
                assert!(record["lastCursor"].is_string());
                assert_ne!(record["acknowledgeableCursor"], record["lastCursor"]);
                assert_eq!(
                    record["changes"].as_array().expect("batch changes").len(),
                    expected_events,
                );
            } else {
                completion_cursor = record["cursor"].as_str().map(str::to_owned);
                assert_eq!(record["disposition"], "published_all");
                assert_eq!(record["requestedBlockCount"], "3");
                assert_eq!(record["coveredBeforeRequestBlockCount"], "0");
                assert_eq!(record["coveredBeforeRequestRanges"], json!([]));
                assert_eq!(record["republishedBlockCount"], "3");
                assert_eq!(record["preexistingCoverageSkipped"], false);
                assert!(record.get("repairHint").is_none());
            }
        }
        let acknowledged = app
            .oneshot(
                Request::post(
                    "/v1/backfill-subscriptions/stream-fixture/consumers/destination/ack",
                )
                .header(header::CONTENT_TYPE, "application/json")
                .header(
                    CONSUMER_SESSION_HEADER,
                    session_token.expect("session token"),
                )
                .body(Body::from(
                    json!({ "cursor": completion_cursor.expect("completion cursor") }).to_string(),
                ))
                .expect("ack request"),
            )
            .await
            .expect("ack response");
        assert_eq!(acknowledged.status(), StatusCode::OK);
        loop {
            let next = tokio::time::timeout(Duration::from_secs(5), chunks.next())
                .await
                .expect("stream closes after completion ACK");
            let Some(chunk) = next else { break };
            decoder
                .write_all(&chunk.expect("gzip footer bytes"))
                .expect("decode gzip footer");
        }
        decoder.try_finish().expect("complete gzip stream");
        plain_bytes.extend_from_slice(decoder.get_ref());
        assert!(plain_bytes.iter().all(u8::is_ascii_whitespace));
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn live_consumer_stream_batches_blocks_and_holds_one_session() {
        use leani_primitives::ProcessorCursor;
        use leani_testkit::{BlockLocalCounter, fixture_frame};

        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteStore::open(leani_store_sqlite::StoreConfig::new(
            directory.path().join("node.sqlite"),
        ))
        .await
        .expect("store");
        let processor = Arc::new(BlockLocalCounter::default().with_split_delivery());
        store
            .register_processor(processor.descriptor())
            .await
            .expect("register processor");
        let stream_id = default_delivery_stream_id(processor.descriptor());
        store
            .create_consumer_in_stream(
                processor.descriptor(),
                &stream_id,
                "destination",
                ConsumerRole::Required,
                ConsumerStartPosition::EarliestRetained,
                Duration::from_secs(30),
            )
            .await
            .expect("consumer");
        let mut parent = BlockHash::ZERO;
        for number in 1..=2 {
            let frame = fixture_frame(number, parent);
            let delta = processor.map(&frame).await.expect("map");
            store
                .apply(
                    processor.as_ref(),
                    ProcessorCursor {
                        chain_id: frame.chain_id,
                        processor_id: processor.descriptor().id.to_string(),
                        processor_version: processor.descriptor().version.to_string(),
                        block_number: frame.block.number,
                        block_hash: frame.block.hash,
                        finality: Finality::Finalized,
                        sequence: number,
                    },
                    &delta,
                    &[],
                )
                .await
                .expect("live apply");
            parent = frame.block.hash;
        }
        let configured: Arc<dyn Processor> = processor;
        let app = router_with_processors(store, vec![configured], Vec::new(), ApiConfig::default())
            .expect("router");
        let path = "/v1/processors/synthetic-counter/streams/live/consumers/destination/stream";
        let first = app
            .clone()
            .oneshot(Request::get(path).body(Body::empty()).expect("request"))
            .await
            .expect("response");
        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(
            first.headers()[header::CONTENT_TYPE],
            "application/x-ndjson"
        );

        let duplicate = app
            .clone()
            .oneshot(Request::get(path).body(Body::empty()).expect("request"))
            .await
            .expect("duplicate response");
        assert_eq!(duplicate.status(), StatusCode::CONFLICT);
        let duplicate_body: Value = serde_json::from_slice(
            &to_bytes(duplicate.into_body(), usize::MAX)
                .await
                .expect("duplicate body"),
        )
        .expect("duplicate JSON");
        assert_eq!(duplicate_body["error"]["code"], "consumer_session_active");

        let mut chunks = first.into_body().into_data_stream();
        let mut last_cursor = None;
        let mut session_token = None;
        for (expected_type, expected_block) in
            [("hello", None), ("batch", Some(1)), ("batch", Some(2))]
        {
            let chunk = tokio::time::timeout(Duration::from_secs(5), chunks.next())
                .await
                .expect("stream record in time")
                .expect("stream record")
                .expect("stream bytes");
            let record: Value = serde_json::from_slice(&chunk).expect("NDJSON record");
            assert_eq!(record["type"], expected_type);
            if let Some(expected_block) = expected_block {
                assert_eq!(record["originKind"], "live");
                assert_eq!(record["fromBlock"], expected_block);
                assert_eq!(record["throughBlock"], expected_block);
                assert_eq!(record["processedBlockCount"], "1");
                assert_eq!(record["domainChangeCount"], "1");
                assert_eq!(record["changes"].as_array().expect("changes").len(), 1);
                last_cursor = record["lastCursor"].as_str().map(str::to_owned);
            } else {
                assert_eq!(record["streamKind"], "live");
                assert_eq!(record["streamId"], stream_id);
                session_token = record["sessionToken"].as_str().map(str::to_owned);
            }
        }
        let missing_session = app
            .clone()
            .oneshot(
                Request::post(
                    "/v1/processors/synthetic-counter/streams/live/consumers/destination/ack",
                )
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({ "cursor": last_cursor.clone().expect("last live cursor") }).to_string(),
                ))
                .expect("sessionless acknowledgement request"),
            )
            .await
            .expect("sessionless acknowledgement response");
        assert_eq!(missing_session.status(), StatusCode::BAD_REQUEST);
        let acknowledge = app
            .clone()
            .oneshot(
                Request::post(
                    "/v1/processors/synthetic-counter/streams/live/consumers/destination/ack",
                )
                .header(header::CONTENT_TYPE, "application/json")
                .header(
                    CONSUMER_SESSION_HEADER,
                    session_token.clone().expect("consumer session token"),
                )
                .body(Body::from(
                    json!({ "cursor": last_cursor.expect("last live cursor") }).to_string(),
                ))
                .expect("acknowledgement request"),
            )
            .await
            .expect("acknowledgement response");
        assert_eq!(acknowledge.status(), StatusCode::OK);
        let acknowledged: Value = serde_json::from_slice(
            &to_bytes(acknowledge.into_body(), usize::MAX)
                .await
                .expect("acknowledgement body"),
        )
        .expect("acknowledgement JSON");
        assert_eq!(acknowledged["acknowledgedSequence"], "2");
        assert_eq!(acknowledged["streamId"], stream_id);

        let released = app
            .clone()
            .oneshot(
                Request::delete(
                    "/v1/processors/synthetic-counter/streams/live/consumers/destination/lease",
                )
                .header(
                    CONSUMER_SESSION_HEADER,
                    session_token.expect("consumer session token"),
                )
                .body(Body::empty())
                .expect("release request"),
            )
            .await
            .expect("release response");
        assert_eq!(released.status(), StatusCode::NO_CONTENT);

        let replacement = app
            .oneshot(Request::get(path).body(Body::empty()).expect("request"))
            .await
            .expect("replacement response");
        assert_eq!(replacement.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn durable_consumer_api_rejects_acknowledgement_beyond_committed_head() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteStore::open(leani_store_sqlite::StoreConfig::new(
            directory.path().join("node.sqlite"),
        ))
        .await
        .expect("store");
        let processor = Arc::new(BlobsProcessor::default());
        let future_cursor = encode_cursor(&ApiCursor {
            version: 2,
            epoch: store.epoch(),
            chain_id: 1,
            processor_id: processor.descriptor().instance.to_string(),
            processor_version: processor.descriptor().version.to_string(),
            sequence: 1,
        })
        .expect("cursor");
        let router = router(store, processor, ApiConfig::default()).expect("router");
        let create = router
            .clone()
            .oneshot(
                Request::post("/v1/processors/blobs-money/consumers")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "id": "blobs-api",
                            "role": "required",
                            "start": { "position": "current_head" },
                            "leaseTtlSeconds": 300,
                            "credential": "blobs-api-test-credential"
                        })
                        .to_string(),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(create.status(), StatusCode::CREATED);

        let unauthenticated = router
            .clone()
            .oneshot(
                Request::post("/v1/processors/blobs-money/consumers/blobs-api/ack")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({ "cursor": future_cursor.clone() }).to_string(),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(unauthenticated.status(), StatusCode::CONFLICT);

        let forbidden = router
            .clone()
            .oneshot(
                Request::post("/v1/processors/blobs-money/consumers/blobs-api/ack")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(CONSUMER_CREDENTIAL_HEADER, "another-consumer-secret")
                    .body(Body::from(
                        serde_json::json!({ "cursor": future_cursor.clone() }).to_string(),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(forbidden.status(), StatusCode::FORBIDDEN);

        let acknowledge = router
            .clone()
            .oneshot(
                Request::post("/v1/processors/blobs-money/consumers/blobs-api/ack")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(CONSUMER_CREDENTIAL_HEADER, "blobs-api-test-credential")
                    .body(Body::from(
                        serde_json::json!({ "cursor": future_cursor }).to_string(),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(acknowledge.status(), StatusCode::CONFLICT);
        let body = to_bytes(acknowledge.into_body(), usize::MAX)
            .await
            .expect("body");
        let body: Value = serde_json::from_slice(&body).expect("JSON");
        assert_eq!(body["error"]["code"], "acknowledgement_invalid");

        let list = router
            .oneshot(
                Request::get("/v1/processors/blobs-money/consumers")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(list.status(), StatusCode::OK);
        let body = to_bytes(list.into_body(), usize::MAX).await.expect("body");
        let body: Value = serde_json::from_slice(&body).expect("JSON");
        assert_eq!(body["data"][0]["id"], "blobs-api");
        assert_eq!(body["data"][0]["acknowledgedSequence"], "0");
        assert_eq!(body["data"][0]["deliveredSequence"], "0");
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn generic_query_and_follow_snapshot_is_stable_across_new_commits() {
        use leani_primitives::ProcessorCursor;
        use leani_testkit::{BlockLocalCounter, fixture_frame};

        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteStore::open(leani_store_sqlite::StoreConfig::new(
            directory.path().join("generic-query.sqlite"),
        ))
        .await
        .expect("store");
        let processor = Arc::new(BlockLocalCounter::default());
        let mut parent = BlockHash::ZERO;
        for number in 1..=2 {
            let frame = fixture_frame(number, parent);
            let delta = processor.map(&frame).await.expect("map");
            let descriptor = processor.descriptor();
            store
                .apply(
                    processor.as_ref(),
                    ProcessorCursor {
                        processor_id: descriptor.id.to_string(),
                        processor_version: descriptor.version.to_string(),
                        chain_id: frame.chain_id,
                        block_number: frame.block.number,
                        block_hash: frame.block.hash,
                        finality: frame.finality,
                        sequence: number,
                    },
                    &delta,
                    &[],
                )
                .await
                .expect("apply");
            parent = frame.block.hash;
        }
        let configured: Arc<dyn Processor> = processor.clone();
        let router = router_with_processors(
            store.clone(),
            vec![configured],
            Vec::new(),
            ApiConfig::default(),
        )
        .expect("router");
        let first = router
            .clone()
            .oneshot(
                Request::post(
                    "/v1/processors/synthetic-counter/collections/counter.blocks/query-and-follow",
                )
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"limit":1}"#))
                .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(first.status(), StatusCode::OK);
        let first: Value =
            serde_json::from_slice(&to_bytes(first.into_body(), usize::MAX).await.expect("body"))
                .expect("JSON");
        assert_eq!(first["rowCount"], "2");
        assert_eq!(first["data"].as_array().expect("data").len(), 1);
        let next = first["nextCursor"]
            .as_str()
            .expect("next cursor")
            .to_owned();
        let boundary = decode_cursor(first["boundaryCursor"].as_str().expect("boundary"))
            .expect("boundary cursor");
        assert_eq!(boundary.sequence, 2);

        let frame = fixture_frame(3, parent);
        let delta = processor.map(&frame).await.expect("third map");
        let descriptor = processor.descriptor();
        store
            .apply(
                processor.as_ref(),
                ProcessorCursor {
                    processor_id: descriptor.id.to_string(),
                    processor_version: descriptor.version.to_string(),
                    chain_id: frame.chain_id,
                    block_number: frame.block.number,
                    block_hash: frame.block.hash,
                    finality: frame.finality,
                    sequence: 3,
                },
                &delta,
                &[],
            )
            .await
            .expect("third apply");

        let second = router
            .oneshot(
                Request::get(format!(
                    "/v1/processors/synthetic-counter/collections/counter.blocks/entities?cursor={next}"
                ))
                .body(Body::empty())
                .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(second.status(), StatusCode::OK);
        let second: Value = serde_json::from_slice(
            &to_bytes(second.into_body(), usize::MAX)
                .await
                .expect("body"),
        )
        .expect("JSON");
        assert_eq!(second["rowCount"], "2");
        assert_eq!(second["data"].as_array().expect("data").len(), 1);
        assert!(second["nextCursor"].is_null());
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn artifact_routes_inspect_export_and_explicitly_delete() {
        use leani_processor_api::{ArtifactPolicyMode, LifecyclePolicies};
        use leani_testkit::{BlockLocalCounter, fixture_frame};

        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteStore::open(leani_store_sqlite::StoreConfig::new(
            directory.path().join("artifact-api.sqlite"),
        ))
        .await
        .expect("store");
        let mut lifecycle = LifecyclePolicies::default();
        lifecycle.artifacts.mode = ArtifactPolicyMode::Full;
        lifecycle.output.mode = OutputPolicyMode::None;
        lifecycle.delivery.mode = leani_processor_api::DeliveryPolicyMode::None;
        lifecycle.delivery.consumers.clear();
        let processor = Arc::new(BlockLocalCounter::default().with_lifecycle(lifecycle));
        let frame = fixture_frame(1, BlockHash::ZERO);
        let delta = processor.map(&frame).await.expect("map");
        store
            .retain_finalized_artifact(processor.descriptor(), &delta, Finality::Finalized)
            .await
            .expect("retain artifact");
        let configured: Arc<dyn Processor> = processor.clone();
        let router =
            router_with_processors(store, vec![configured], Vec::new(), ApiConfig::default())
                .expect("router");

        let status = router
            .clone()
            .oneshot(
                Request::get("/v1/processors/synthetic-counter/artifacts/status")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("status response");
        assert_eq!(status.status(), StatusCode::OK);
        let status: Value = serde_json::from_slice(
            &to_bytes(status.into_body(), usize::MAX)
                .await
                .expect("status body"),
        )
        .expect("status JSON");
        assert_eq!(status["artifacts"], 1);

        let exact = router
            .clone()
            .oneshot(
                Request::get("/v1/processors/synthetic-counter/artifacts/1")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("exact response");
        assert_eq!(exact.status(), StatusCode::OK);
        let exact: Value = serde_json::from_slice(
            &to_bytes(exact.into_body(), usize::MAX)
                .await
                .expect("exact body"),
        )
        .expect("exact JSON");
        assert_eq!(exact["block"]["number"], "1");

        let export = router
            .clone()
            .oneshot(
                Request::get(
                    "/v1/processors/synthetic-counter/artifacts/export?fromBlock=1&toBlock=1",
                )
                .body(Body::empty())
                .expect("request"),
            )
            .await
            .expect("export response");
        assert_eq!(export.status(), StatusCode::OK);
        assert_eq!(
            export.headers()[header::CONTENT_TYPE],
            "application/vnd.leani.processor-artifacts"
        );
        assert_eq!(export.headers()["x-leani-artifact-complete"], "true");
        let bytes = to_bytes(export.into_body(), usize::MAX)
            .await
            .expect("export body");
        let decoded = leani_store_sqlite::ProcessorArtifactExport::decode_durable(
            processor.descriptor(),
            &bytes,
        )
        .expect("decode export");
        assert_eq!(decoded.artifacts, vec![delta]);

        let deleted = router
            .clone()
            .oneshot(
                Request::delete(
                    "/admin/v1/processors/synthetic-counter/artifacts?fromBlock=1&toBlock=1",
                )
                .body(Body::empty())
                .expect("request"),
            )
            .await
            .expect("delete response");
        assert_eq!(deleted.status(), StatusCode::OK);
        let deleted: Value = serde_json::from_slice(
            &to_bytes(deleted.into_body(), usize::MAX)
                .await
                .expect("delete body"),
        )
        .expect("delete JSON");
        assert_eq!(deleted["deleted_artifacts"], 1);

        let missing = router
            .oneshot(
                Request::get("/v1/processors/synthetic-counter/artifacts/1")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("missing response");
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn empty_change_head_and_partial_blob_snapshot_are_explicit() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteStore::open(leani_store_sqlite::StoreConfig::new(
            directory.path().join("node.sqlite"),
        ))
        .await
        .expect("store");
        let router = router(
            store,
            Arc::new(BlobsProcessor::default()),
            ApiConfig::default(),
        )
        .expect("router");

        let head = router
            .clone()
            .oneshot(
                Request::get("/v1/processors/blobs-money/changes/head")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(head.status(), StatusCode::OK);
        let head = to_bytes(head.into_body(), usize::MAX).await.expect("body");
        let head: Value = serde_json::from_slice(&head).expect("JSON");
        assert!(head["earliestSequence"].is_null());
        assert!(head["latestSequence"].is_null());
        assert!(head["cursor"].is_null());

        let snapshot = router
            .oneshot(
                Request::get(
                    "/v1/q/blobs/snapshot?fromBlock=19426589&toBlock=19426589&allowPartial=true",
                )
                .body(Body::empty())
                .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(snapshot.status(), StatusCode::OK);
        let snapshot = to_bytes(snapshot.into_body(), usize::MAX)
            .await
            .expect("body");
        let snapshot: Value = serde_json::from_slice(&snapshot).expect("JSON");
        assert_eq!(snapshot["data"], json!([]));
        assert_eq!(snapshot["coverage"]["complete"], false);
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn blob_pages_bind_cursors_to_requests_and_do_not_truncate_transactions() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteStore::open(leani_store_sqlite::StoreConfig::new(
            directory.path().join("blob-pagination.sqlite"),
        ))
        .await
        .expect("store");
        let processor = Arc::new(BlobsProcessor::default());
        let first_block = processor.schedule().first_block();
        seed_blob_block(&store, &processor, first_block, 2).await;
        seed_blob_block(&store, &processor, first_block + 1, 0).await;
        let router = router(store, processor, ApiConfig::default()).expect("router");

        let first_page = router
            .clone()
            .oneshot(
                Request::get(format!(
                    "/v1/q/blobs/blocks?fromBlock={first_block}&toBlock={}&limit=1",
                    first_block + 1
                ))
                .body(Body::empty())
                .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(first_page.status(), StatusCode::OK);
        let first_page: Value = serde_json::from_slice(
            &to_bytes(first_page.into_body(), usize::MAX)
                .await
                .expect("body"),
        )
        .expect("JSON");
        let range_cursor = first_page["nextCursor"].as_str().expect("range cursor");

        let mismatched = router
            .clone()
            .oneshot(
                Request::get(format!(
                    "/v1/q/blobs/blocks?fromBlock={}&toBlock={}&limit=1&cursor={range_cursor}",
                    first_block + 1,
                    first_block + 1
                ))
                .body(Body::empty())
                .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(mismatched.status(), StatusCode::BAD_REQUEST);
        let mismatched: Value = serde_json::from_slice(
            &to_bytes(mismatched.into_body(), usize::MAX)
                .await
                .expect("body"),
        )
        .expect("JSON");
        assert_eq!(mismatched["error"]["code"], "cursor_invalid");

        let bounded = router
            .clone()
            .oneshot(
                Request::get(format!(
                    "/v1/q/blobs/blocks?fromBlock={first_block}&toBlock={first_block}&limit=1"
                ))
                .body(Body::empty())
                .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(bounded.status(), StatusCode::OK);
        let bounded: Value = serde_json::from_slice(
            &to_bytes(bounded.into_body(), usize::MAX)
                .await
                .expect("body"),
        )
        .expect("JSON");
        assert!(bounded["nextCursor"].is_null());

        let first_transactions = router
            .clone()
            .oneshot(
                Request::get(format!(
                    "/v1/q/blobs/transactions?blockNumber={first_block}&limit=1"
                ))
                .body(Body::empty())
                .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(first_transactions.status(), StatusCode::OK);
        let first_transactions: Value = serde_json::from_slice(
            &to_bytes(first_transactions.into_body(), usize::MAX)
                .await
                .expect("body"),
        )
        .expect("JSON");
        assert_eq!(
            first_transactions["data"].as_array().expect("data").len(),
            1
        );
        let transaction_cursor = first_transactions["nextCursor"]
            .as_str()
            .expect("transaction cursor");

        let second_transactions = router
            .oneshot(
                Request::get(format!(
                    "/v1/q/blobs/transactions?blockNumber={first_block}&limit=1&cursor={transaction_cursor}"
                ))
                .body(Body::empty())
                .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(second_transactions.status(), StatusCode::OK);
        let second_transactions: Value = serde_json::from_slice(
            &to_bytes(second_transactions.into_body(), usize::MAX)
                .await
                .expect("body"),
        )
        .expect("JSON");
        assert_eq!(
            second_transactions["data"].as_array().expect("data").len(),
            1
        );
        assert!(second_transactions["nextCursor"].is_null());
        assert_ne!(
            first_transactions["data"][0]["txHash"],
            second_transactions["data"][0]["txHash"]
        );
    }

    #[tokio::test]
    async fn change_stream_first_frame_is_hello_without_id() {
        use futures::StreamExt as _;

        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteStore::open(leani_store_sqlite::StoreConfig::new(
            directory.path().join("node.sqlite"),
        ))
        .await
        .expect("store");
        let router = router(
            store,
            Arc::new(BlobsProcessor::default()),
            ApiConfig::default(),
        )
        .expect("router");
        let response = router
            .oneshot(
                Request::get("/v1/processors/blobs-money/stream")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let mut body = response.into_body().into_data_stream();
        let first = tokio::time::timeout(std::time::Duration::from_secs(5), body.next())
            .await
            .expect("first frame in time")
            .expect("frame present")
            .expect("frame ok");
        let frame = String::from_utf8(first.to_vec()).expect("utf8");
        assert!(frame.contains("event: hello"), "frame: {frame}");
        assert!(
            !frame.contains("id:"),
            "hello must not carry an id: {frame}"
        );
        let data_line = frame
            .lines()
            .find_map(|line| line.strip_prefix("data: "))
            .expect("data line");
        let hello: Value = serde_json::from_str(data_line).expect("hello JSON");
        assert_eq!(hello["apiVersion"], "1");
        assert_eq!(hello["chainId"], 1);
        assert_eq!(hello["processor"]["id"], "blobs-money");
        assert!(hello["coverage"].is_object());
    }

    #[test]
    fn emitted_at_renders_rfc3339_utc_milliseconds() {
        assert_eq!(unix_ms_rfc3339(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(
            unix_ms_rfc3339(1_709_164_800_000),
            "2024-02-29T00:00:00.000Z"
        );
        assert_eq!(
            unix_ms_rfc3339(1_786_025_151_194),
            "2026-08-06T14:05:51.194Z"
        );
        assert_eq!(unix_ms_rfc3339(951_827_696_789), "2000-02-29T12:34:56.789Z");
    }

    #[test]
    fn apply_envelope_serializes_without_processor_or_coverage() {
        let envelope = ChangeEnvelope {
            api_version: API_VERSION,
            sequence: "1".to_owned(),
            cursor: "00".to_owned(),
            operation: "apply",
            origin_kind: "live".to_owned(),
            origin_id: "blobs-main".to_owned(),
            publication_revision: "0".to_owned(),
            chain_id: 1,
            block: Value::Null,
            finality: "finalized",
            kind: "blobs.block.put".to_owned(),
            schema: "blobs.block-bundle.v1".to_owned(),
            key: None,
            data: None,
            coverage: None,
            emitted_at: unix_ms_rfc3339(0),
        };
        let value = serde_json::to_value(&envelope).expect("JSON");
        let object = value.as_object().expect("object");
        assert!(!object.contains_key("processor"));
        assert!(!object.contains_key("coverage"));
    }

    #[tokio::test]
    async fn generic_router_does_not_require_blobs_processor() {
        use leani_testkit::BlockLocalCounter;

        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteStore::open(leani_store_sqlite::StoreConfig::new(
            directory.path().join("generic.sqlite"),
        ))
        .await
        .expect("store");
        let processor: Arc<dyn Processor> = Arc::new(BlockLocalCounter::default());
        let router =
            router_with_processors(store, vec![processor], Vec::new(), ApiConfig::default())
                .expect("generic-only router");
        let status = router
            .clone()
            .oneshot(
                Request::get("/v1/status")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        let status_code = status.status();
        let body = to_bytes(status.into_body(), usize::MAX)
            .await
            .expect("body");
        assert_eq!(
            status_code,
            StatusCode::OK,
            "unexpected status response: {}",
            String::from_utf8_lossy(&body)
        );
        let body: Value = serde_json::from_slice(&body).expect("JSON");
        assert_eq!(body["processor"]["id"], "synthetic-counter");

        let typed_blobs = router
            .oneshot(
                Request::get("/v1/q/blobs/schedule")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(typed_blobs.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn blob_schedule_exposes_exact_activation_identity() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteStore::open(leani_store_sqlite::StoreConfig::new(
            directory.path().join("node.sqlite"),
        ))
        .await
        .expect("store");
        let router = router(
            store,
            Arc::new(BlobsProcessor::default()),
            ApiConfig::default(),
        )
        .expect("router");
        let response = router
            .oneshot(
                Request::get("/v1/q/blobs/schedule?atBlock=24179383")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let body: Value = serde_json::from_slice(&body).expect("JSON");
        assert_eq!(body["name"], "bpo2");
        assert_eq!(body["activationBlock"], 24_179_383);
        assert_eq!(body["activationTimestamp"], 1_767_747_671_u64);
        assert_eq!(body["forkId"], "0x07c9462e");
    }

    #[tokio::test]
    async fn optional_bearer_auth_is_enforced_and_redacted() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteStore::open(leani_store_sqlite::StoreConfig::new(
            directory.path().join("node.sqlite"),
        ))
        .await
        .expect("store");
        let config = ApiConfig {
            bearer_token: Some(Arc::from("very-secret")),
            ..ApiConfig::default()
        };
        assert!(!format!("{config:?}").contains("very-secret"));
        let router = router(store, Arc::new(BlobsProcessor::default()), config).expect("router");
        let public_health = router
            .clone()
            .oneshot(
                Request::get("/health/live")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(public_health.status(), StatusCode::OK);
        let unauthorized = router
            .clone()
            .oneshot(
                Request::get("/v1/status")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(unauthorized.headers()[header::WWW_AUTHENTICATE], "Bearer");
        let authorized = router
            .oneshot(
                Request::get("/v1/status")
                    .header(header::AUTHORIZATION, "Bearer very-secret")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(authorized.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn readiness_is_component_specific_and_metrics_are_exposed() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteStore::open(leani_store_sqlite::StoreConfig::new(
            directory.path().join("node.sqlite"),
        ))
        .await
        .expect("store");
        let processor = Arc::new(BlobsProcessor::default());
        store
            .create_consumer(
                processor.descriptor(),
                "metrics-required",
                ConsumerRole::Required,
                ConsumerStartPosition::CurrentHead,
                Duration::from_mins(5),
            )
            .await
            .expect("consumer");
        let readiness = ReadinessHandle::new(true, true);
        let config = ApiConfig {
            readiness: readiness.clone(),
            ..ApiConfig::default()
        };
        let router = router(store, processor, config).expect("router");
        let unavailable = router
            .clone()
            .oneshot(
                Request::get("/health/ready")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(unavailable.status(), StatusCode::SERVICE_UNAVAILABLE);
        readiness.set_live_ready(true);
        readiness.set_finality_ready(true);
        let available = router
            .clone()
            .oneshot(
                Request::get("/health/ready")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(available.status(), StatusCode::OK);
        let metrics = router
            .oneshot(
                Request::get("/metrics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(metrics.status(), StatusCode::OK);
        assert_eq!(
            metrics.headers()[header::CONTENT_TYPE],
            "text/plain; version=0.0.4; charset=utf-8"
        );
        let body = to_bytes(metrics.into_body(), usize::MAX)
            .await
            .expect("metrics body");
        let body = String::from_utf8(body.to_vec()).expect("UTF-8");
        assert!(body.contains("leani_live_required 1"));
        assert!(body.contains("leani_live_ready 1"));
        assert!(body.contains("leani_finality_required 1"));
        assert!(body.contains("leani_finality_ready 1"));
        assert!(body.contains("leani_processor_delivery_live_bytes"));
        assert!(body.contains("leani_processor_delivery_pruned_through_sequence"));
        assert!(body.contains("leani_processor_required_ack_watermark"));
        assert!(body.contains("leani_consumer_acknowledged_sequence"));
        assert!(body.contains("leani_consumer_lease_active"));
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn network_status_dashboard_and_metrics_expose_persistent_manager() {
        use leani_source_api::{NetworkDisconnectReason, NetworkLane, NetworkPhase};

        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteStore::open(leani_store_sqlite::StoreConfig::new(
            directory.path().join("network.sqlite"),
        ))
        .await
        .expect("store");
        let telemetry = NetworkTelemetry::default();
        telemetry.set_peer_targets(1, 16, 100);
        let session = telemetry.register(NetworkLane::Live);
        session.set_phase(NetworkPhase::FetchingReceipts);
        session.set_peers(1, 42);
        session.set_range(Some(
            BlockRange::new(BlockNumber(19_426_589), BlockNumber(19_426_620)).expect("range"),
        ));
        session.observe_head(BlockNumber(19_426_620));
        session.record_error("receipt timeout");
        telemetry.request_started(Duration::from_millis(8));
        telemetry.request_succeeded();
        telemetry.request_started(Duration::from_millis(12));
        telemetry.request_timed_out();
        telemetry.peer_session_established();
        telemetry.peer_session_established();
        telemetry.peer_session_closed(NetworkDisconnectReason::TooManyPeers);
        telemetry.supervisor_backoff(
            "hot/cold handoff retained pending deltas",
            Duration::from_secs(30),
        );
        let router = router(
            store,
            Arc::new(BlobsProcessor::default()),
            ApiConfig {
                network_telemetry: telemetry,
                ..ApiConfig::default()
            },
        )
        .expect("router");

        let status = router
            .clone()
            .oneshot(
                Request::get("/v1/network/status")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(status.status(), StatusCode::OK);
        let body = to_bytes(status.into_body(), usize::MAX)
            .await
            .expect("body");
        let body: Value = serde_json::from_slice(&body).expect("JSON");
        assert_eq!(body["network"]["activeSessions"], 1);
        assert_eq!(body["network"]["connectedPeerSlots"], 1);
        assert_eq!(body["network"]["peerTargets"]["minimum"], 1);
        assert_eq!(body["network"]["peerTargets"]["preferred"], 16);
        assert_eq!(body["network"]["peerTargets"]["maxOutbound"], 100);
        assert_eq!(body["network"]["requests"]["started"], 2);
        assert_eq!(body["network"]["requests"]["succeeded"], 1);
        assert_eq!(body["network"]["requests"]["timedOut"], 1);
        assert_eq!(body["network"]["requests"]["queueWaitMilliseconds"], 20);
        assert_eq!(body["network"]["peerLifecycle"]["established"], 2);
        assert_eq!(body["network"]["peerLifecycle"]["disconnected"], 1);
        assert_eq!(
            body["network"]["peerLifecycle"]["disconnectReasons"][0]["reason"],
            "too_many_peers"
        );
        assert_eq!(body["network"]["sessions"][0]["lane"], "live");
        assert_eq!(body["network"]["sessions"][0]["phase"], "fetching_receipts");
        assert_eq!(body["network"]["sessions"][0]["fromBlock"], 19_426_589);
        assert_eq!(
            body["network"]["sessions"][0]["observedHeadBlock"],
            19_426_620
        );
        assert_eq!(body["network"]["supervisor"]["state"], "backing_off");
        assert_eq!(body["network"]["supervisor"]["failures"], 1);
        assert_eq!(
            body["network"]["supervisor"]["lastError"],
            "hot/cold handoff retained pending deltas"
        );
        assert_eq!(
            body["network"]["sessions"][0]["lastError"],
            "receipt timeout"
        );
        assert_eq!(body["processors"][0]["storedRangeContiguous"], false);
        assert_eq!(body["processors"][0]["syncTargetBlock"], 19_426_620);
        assert_eq!(body["processors"][0]["blocksRemaining"], 32);
        assert_eq!(
            body["processors"][0]["missingRanges"],
            json!([{
                "fromBlock": 19_426_589,
                "toBlock": 19_426_620,
                "blocks": 32
            }])
        );
        assert_eq!(body["processors"][0]["pendingDeltas"], 0);
        assert_eq!(body["storage"]["appliedBlocks"], 0);
        assert_eq!(
            body["storage"]["totalOnDiskBytes"].as_u64(),
            Some(
                body["storage"]["physicalFileBytes"]
                    .as_u64()
                    .expect("physical file bytes")
                    + body["storage"]["walBytes"].as_u64().expect("WAL bytes")
                    + body["storage"]["artifactSegmentBytes"]
                        .as_u64()
                        .expect("artifact segment bytes")
            )
        );

        let dashboard = router
            .clone()
            .oneshot(
                Request::get("/debug/network")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(dashboard.status(), StatusCode::OK);
        assert!(
            dashboard.headers()[header::CONTENT_TYPE]
                .to_str()
                .expect("content type")
                .starts_with("text/html")
        );
        let dashboard = to_bytes(dashboard.into_body(), usize::MAX)
            .await
            .expect("dashboard");
        let dashboard = String::from_utf8(dashboard.to_vec()).expect("UTF-8");
        assert!(dashboard.contains("leani network"));
        assert!(dashboard.contains("Stored processor coverage"));
        assert!(dashboard.contains("It does not by itself imply live readiness"));
        assert!(dashboard.contains("active requested ranges"));
        assert!(dashboard.contains("Network lane failed; retrying"));
        assert!(dashboard.contains("Following the live head while backfilling gaps"));
        assert!(dashboard.contains("blocks missing through ${onDemand"));
        assert!(dashboard.contains("requested history"));
        assert!(dashboard.contains("Blocks remaining"));
        assert!(dashboard.contains("Database size"));
        assert!(dashboard.contains("Apply throughput"));
        assert!(dashboard.contains("committed-block rate · live + history"));
        assert!(dashboard.contains("Latest history processing"));
        assert!(dashboard.contains("Latest history input"));
        assert!(dashboard.contains("Historical catch-up log"));
        assert!(dashboard.contains("not an exact wire-byte measurement"));
        assert!(dashboard.contains("Storage growth"));
        assert!(dashboard.contains("Missing ${fmt(range.fromBlock)}"));
        assert!(dashboard.contains("What counts as a material request?"));
        assert!(
            dashboard.contains("Header fetching, peer discovery, and API requests are excluded.")
        );
        assert!(dashboard.contains("request-timeout"));
        assert!(dashboard.contains("request-failed"));
        assert!(dashboard.contains("Average local queue wait"));
        assert!(dashboard.contains("<span class=\"label\">Peers</span>"));
        assert!(dashboard.contains("connected peer slots"));
        assert!(dashboard.contains("known records"));
        assert!(dashboard.contains("preferred"));
        assert!(dashboard.contains("maximum outbound"));
        assert!(dashboard.contains("Known records are persisted discovery candidates"));
        assert!(dashboard.contains("protocol-level reason"));
        assert!(!dashboard.contains("Coverage state"));

        let metrics = router
            .oneshot(
                Request::get("/metrics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        let metrics = to_bytes(metrics.into_body(), usize::MAX)
            .await
            .expect("metrics");
        let metrics = String::from_utf8(metrics.to_vec()).expect("UTF-8");
        assert!(metrics.contains("leani_p2p_active_sessions 1"));
        assert!(metrics.contains("leani_p2p_minimum_peer_slots 1"));
        assert!(metrics.contains("leani_p2p_preferred_peer_slots 16"));
        assert!(metrics.contains("leani_p2p_max_outbound_peer_slots 100"));
        assert!(metrics.contains("leani_network_supervisor_info{state=\"backing_off\"} 1"));
        assert!(metrics.contains("leani_network_supervisor_failures_total 1"));
        assert!(metrics.contains("leani_p2p_requests_started_total 2"));
        assert!(metrics.contains("leani_p2p_requests_timed_out_total 1"));
        assert!(metrics.contains("leani_p2p_peer_sessions_established_total 2"));
        assert!(metrics.contains("leani_p2p_peer_sessions_disconnected_total 1"));
        assert!(metrics.contains("leani_p2p_peer_disconnects_total{reason=\"too_many_peers\"} 1"));
        assert!(metrics.contains(
            "leani_p2p_session_connected_peers{session=\"1\",lane=\"live\",phase=\"fetching_receipts\"} 1"
        ));
        assert!(metrics.contains(
            "leani_p2p_session_observed_head_block{session=\"1\",lane=\"live\"} 19426620"
        ));
        assert!(
            metrics.contains("leani_history_jobs{owner=\"materialization\",state=\"completed\"} 0")
        );
    }

    #[test]
    fn history_metrics_attribute_work_to_processor_source_and_kind() {
        let jobs = vec![BackfillStatus {
            id: "history-1".to_owned(),
            owner: HistoricalWorkOwner::Materialization,
            processor: "blobs-production".to_owned(),
            delivery_stream_id: None,
            publication_revision: None,
            from_block: 100,
            to_block: 109,
            ranges: vec![BackfillRange {
                from_block: 100,
                to_block: 109,
            }],
            requested_blocks: 10,
            processed_blocks: 10,
            remaining_blocks: 0,
            captured_finalized_target: None,
            mode: BackfillExecutionMode::FillMissing,
            batching: None,
            state: BackfillState::Completed,
            attempts: 2,
            updated_at_unix_ms: 1,
            report: Some(BackfillReport {
                source_ids: vec!["era1".to_owned(), "execution-p2p".to_owned()],
                frames_mapped: 10,
                frames_committed: 10,
                duplicate_frames: 0,
                source_attempts: 2,
                source_bytes: 4_096,
                physical_source_bytes: 4_096,
                reused_source_bytes: 0,
                coalesced_frames: 0,
                acquisition_ids: vec![7],
                elapsed_milliseconds: 250,
                sources: vec![
                    BackfillSourceReport {
                        source_id: "era1".to_owned(),
                        source_kind: "history_archive".to_owned(),
                        attempts: 1,
                        failures: 1,
                        frames_mapped: 0,
                        frames_committed: 0,
                        duplicate_frames: 0,
                        source_bytes: 0,
                        physical_source_bytes: 0,
                        reused_source_bytes: 0,
                        coalesced_frames: 0,
                        elapsed_milliseconds: 50,
                        last_error: Some("range unavailable".to_owned()),
                    },
                    BackfillSourceReport {
                        source_id: "execution-p2p".to_owned(),
                        source_kind: "execution_p2p".to_owned(),
                        attempts: 1,
                        failures: 0,
                        frames_mapped: 10,
                        frames_committed: 10,
                        duplicate_frames: 0,
                        source_bytes: 4_096,
                        physical_source_bytes: 4_096,
                        reused_source_bytes: 0,
                        coalesced_frames: 0,
                        elapsed_milliseconds: 200,
                        last_error: None,
                    },
                ],
            }),
            last_error: None,
        }];
        let mut output = String::new();

        append_history_metrics(&mut output, &jobs).expect("metrics");

        assert!(
            output.contains("leani_history_jobs{owner=\"materialization\",state=\"completed\"} 1")
        );
        assert!(output.contains(
            "leani_history_source_input_bytes_total{owner=\"materialization\",processor=\"blobs-production\",source=\"execution-p2p\",kind=\"execution_p2p\"} 4096"
        ));
        assert!(output.contains(
            "leani_history_source_failures_total{owner=\"materialization\",processor=\"blobs-production\",source=\"era1\",kind=\"history_archive\"} 1"
        ));
        assert!(output.contains(
            "leani_history_source_physical_input_bytes_total{owner=\"materialization\",processor=\"blobs-production\",source=\"execution-p2p\",kind=\"execution_p2p\"} 4096"
        ));
        assert!(output.contains(
            "leani_history_source_reused_input_bytes_total{owner=\"materialization\",processor=\"blobs-production\",source=\"execution-p2p\",kind=\"execution_p2p\"} 0"
        ));
    }

    #[test]
    fn history_material_metrics_count_physical_work_once() {
        let mut output = String::new();
        append_historical_material_metrics(
            &mut output,
            Some(HistoricalMaterialMetrics {
                acquisitions_started: 2,
                requests_coalesced: 99,
                requests_coalescible: 3,
                physical_frames: 7,
                physical_bytes: 4_096,
                overfetched_frames: 2,
                overfetched_bytes: 1_024,
                logical_frame_deliveries: 500,
                active_acquisitions: 1,
                buffered_bytes: 512,
            }),
        )
        .expect("material metrics");

        assert!(output.contains("leani_history_material_acquisitions_started_total 2"));
        assert!(output.contains("leani_history_material_requests_coalesced_total 99"));
        assert!(output.contains("leani_history_material_requests_coalescible_total 3"));
        assert!(output.contains("leani_history_material_source_frames_total 7"));
        assert!(output.contains("leani_history_material_source_bytes_total 4096"));
        assert!(output.contains("leani_history_material_overfetched_frames_total 2"));
        assert!(output.contains("leani_history_material_overfetched_bytes_total 1024"));
        assert!(output.contains("leani_history_material_logical_frame_deliveries_total 500"));
        assert!(output.contains("leani_history_material_active_acquisitions 1"));
        assert!(output.contains("leani_history_material_buffer_bytes 512"));
    }

    #[test]
    fn blocks_remaining_counts_internal_gaps_before_the_live_target() {
        let available = vec![
            CoverageInterval {
                from_block: 10,
                to_block: 20,
                finality: "finalized",
            },
            CoverageInterval {
                from_block: 25,
                to_block: 40,
                finality: "optimistic",
            },
        ];
        assert_eq!(missing_coverage_blocks(10, 30, &available), 4);
        assert_eq!(missing_coverage_blocks(10, 8, &available), 0);
        assert_eq!(missing_coverage_blocks(10, 30, &[]), 21);
        assert_eq!(
            missing_coverage_blocks(
                10,
                30,
                &[CoverageInterval {
                    from_block: 10,
                    to_block: 30,
                    finality: "finalized",
                }],
            ),
            0
        );
        assert_eq!(
            missing_coverage_ranges(10, 30, &available),
            vec![MissingCoverageRange {
                from_block: 21,
                to_block: 24,
                blocks: 4,
            }]
        );
        assert_eq!(
            missing_coverage_ranges(
                10,
                30,
                &[CoverageInterval {
                    from_block: 10,
                    to_block: 27,
                    finality: "finalized",
                }],
            ),
            vec![MissingCoverageRange {
                from_block: 28,
                to_block: 30,
                blocks: 3,
            }]
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn on_demand_diagnostics_only_count_requested_history() {
        let coverage = CoverageResponse {
            chain_id: 1,
            chain_finalized_head: Some(ChainFinalizedHead {
                number: 100,
                hash: BlockHash::new([1; 32]).to_string(),
            }),
            requested: None,
            available: vec![
                CoverageInterval {
                    from_block: 10,
                    to_block: 20,
                    finality: "finalized",
                },
                CoverageInterval {
                    from_block: 90,
                    to_block: 100,
                    finality: "optimistic",
                },
            ],
            configured_start_block: 0,
            processed_through: Some(100),
            finalized_through: Some(90),
            complete: false,
            state: "backfilling",
        };
        let active_jobs = vec![
            BackfillStatus {
                id: "first".to_owned(),
                owner: HistoricalWorkOwner::Subscription,
                processor: "blobs-production".to_owned(),
                delivery_stream_id: None,
                publication_revision: None,
                from_block: 15,
                to_block: 25,
                ranges: vec![BackfillRange {
                    from_block: 15,
                    to_block: 25,
                }],
                requested_blocks: 11,
                processed_blocks: 0,
                remaining_blocks: 11,
                captured_finalized_target: None,
                mode: BackfillExecutionMode::FillMissing,
                batching: None,
                state: BackfillState::Running,
                attempts: 1,
                updated_at_unix_ms: 1,
                report: None,
                last_error: None,
            },
            BackfillStatus {
                id: "second".to_owned(),
                owner: HistoricalWorkOwner::Subscription,
                processor: "blobs-production".to_owned(),
                delivery_stream_id: None,
                publication_revision: None,
                from_block: 26,
                to_block: 30,
                ranges: vec![BackfillRange {
                    from_block: 26,
                    to_block: 30,
                }],
                requested_blocks: 5,
                processed_blocks: 0,
                remaining_blocks: 5,
                captured_finalized_target: None,
                mode: BackfillExecutionMode::FillMissing,
                batching: None,
                state: BackfillState::Queued,
                attempts: 0,
                updated_at_unix_ms: 1,
                report: None,
                last_error: None,
            },
        ];

        let requested =
            diagnostic_ranges(true, "blobs-production", Some(100), &coverage, &active_jobs);
        assert_eq!(requested, vec![(15, 30)]);
        assert_eq!(
            merge_missing_ranges(
                requested
                    .iter()
                    .flat_map(|(from, to)| {
                        missing_coverage_ranges(*from, *to, &coverage.available)
                    })
                    .collect()
            ),
            vec![MissingCoverageRange {
                from_block: 21,
                to_block: 30,
                blocks: 10,
            }]
        );

        assert_eq!(
            diagnostic_ranges(true, "blobs-production", Some(100), &coverage, &[]),
            vec![(90, 100)]
        );
        assert_eq!(
            diagnostic_ranges(false, "blobs-production", Some(100), &coverage, &[]),
            vec![(0, 100)]
        );
    }

    #[test]
    fn live_sync_target_ignores_the_next_block_work_probe() {
        use leani_source_api::{NetworkLane, NetworkPhase};

        let telemetry = NetworkTelemetry::default();
        let session = telemetry.register(NetworkLane::Live);
        session.set_phase(NetworkPhase::FollowingHead);
        session.observe_head(BlockNumber(100));
        session.set_range(Some(BlockRange::single(BlockNumber(101))));

        let snapshot = telemetry.snapshot();
        assert_eq!(snapshot.sessions[0].observed_head_block, Some(100));
        assert_eq!(snapshot.sessions[0].to_block, Some(101));
        assert_eq!(live_sync_target(&snapshot), Some(100));
    }

    #[tokio::test]
    async fn contiguous_coverage_does_not_imply_caught_up_or_live_ready() {
        use leani_primitives::ProcessorCursor;
        use leani_source_api::{NetworkLane, NetworkPhase};
        use leani_testkit::{BlockLocalCounter, fixture_frame};

        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteStore::open(leani_store_sqlite::StoreConfig::new(
            directory.path().join("contiguous-but-behind.sqlite"),
        ))
        .await
        .expect("store");
        let processor = Arc::new(BlockLocalCounter::default());
        let mut parent = BlockHash::ZERO;
        for number in 0..=1 {
            let frame = fixture_frame(number, parent);
            let delta = processor.map(&frame).await.expect("map");
            let descriptor = processor.descriptor();
            store
                .apply(
                    processor.as_ref(),
                    ProcessorCursor {
                        processor_id: descriptor.id.to_string(),
                        processor_version: descriptor.version.to_string(),
                        chain_id: frame.chain_id,
                        block_number: frame.block.number,
                        block_hash: frame.block.hash,
                        finality: frame.finality,
                        sequence: number + 1,
                    },
                    &delta,
                    &[],
                )
                .await
                .expect("apply");
            parent = frame.block.hash;
        }

        let telemetry = NetworkTelemetry::default();
        let session = telemetry.register(NetworkLane::Live);
        session.set_phase(NetworkPhase::FetchingBodies);
        session.set_range(Some(
            BlockRange::new(BlockNumber(2), BlockNumber(3)).expect("range"),
        ));
        session.observe_head(BlockNumber(3));
        let configured: Arc<dyn Processor> = processor;
        let router = router_with_processors(
            store,
            vec![configured],
            Vec::new(),
            ApiConfig {
                readiness: ReadinessHandle::new(true, true),
                network_telemetry: telemetry,
                ..ApiConfig::default()
            },
        )
        .expect("router");

        let response = router
            .oneshot(
                Request::get("/v1/network/status")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let body: Value = serde_json::from_slice(&body).expect("JSON");
        assert_eq!(body["readiness"]["ready"], false);
        assert_eq!(body["processors"][0]["coverage"]["complete"], true);
        assert_eq!(body["processors"][0]["coverage"]["state"], "catching_up");
        assert_eq!(body["processors"][0]["storedRangeContiguous"], true);
        assert_eq!(body["processors"][0]["syncTargetBlock"], 3);
        assert_eq!(body["processors"][0]["blocksRemaining"], 2);
    }
}
