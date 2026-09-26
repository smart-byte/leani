//! Capability-aware Ethereum JSON-RPC facade.
//!
//! The facade deliberately refuses methods whose canonical Ethereum response
//! cannot be reconstructed from retained material. This is safer than
//! manufacturing partial blocks or receipts that look like complete RPC data.

mod browser_guard;

use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use alloy_consensus::{
    Header as ConsensusHeader, ReceiptEnvelope as ConsensusReceiptEnvelope, Sealed, TxEnvelope,
    constants::EMPTY_OMMER_ROOT_HASH,
    transaction::{Recovered, SignerRecoverable, TransactionInfo},
};
use alloy_eips::eip2718::Decodable2718;
use alloy_primitives::{B256, U256};
use alloy_rlp::Decodable;
use alloy_rpc_types_eth::{
    Block as RpcBlock, BlockTransactions, Header as RpcHeader, Log as RpcLog,
    Transaction as RpcTransaction, TransactionReceipt as RpcTransactionReceipt, Withdrawal,
    Withdrawals,
};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{
        DefaultBodyLimit, State,
        rejection::BytesRejection,
        ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade, close_code},
    },
    http::{StatusCode, header},
    middleware,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use futures::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use leani_primitives::{
    Address, BlockFrame, BlockHash, BlockNumber, BlockRange, BlockRef, Capability, CapabilitySet,
    ChainId, FilterScope, Finality, Material, TopicFilter, TransactionHash, TrustModel,
};
use leani_processor_api::{Processor, ProcessorDescriptor, StartPoint};
use leani_processor_blobs::{BlobFork, BlobSchedule, BlobsProcessor, get_blob_base_fee};
use leani_source_api::{
    ChainEvent, DataRequest, FieldProjection, FilterSet, HistorySource, SelectionPolicy,
    SourceBudget, SourceError, VerificationPolicy, select_source,
};
use leani_store_sqlite::{SqliteStore, StoreError};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, broadcast, mpsc};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use browser_guard::guard_browser_requests;

const JSONRPC_VERSION: &str = "2.0";
const PARSE_ERROR: i64 = -32_700;
const INVALID_REQUEST: i64 = -32_600;
const METHOD_NOT_FOUND: i64 = -32_601;
const INVALID_PARAMS: i64 = -32_602;
const INTERNAL_ERROR: i64 = -32_603;
const DATA_UNAVAILABLE: i64 = -32_004;
const LIMIT_EXCEEDED: i64 = -32_005;
/// The `-32005` reason of a response past `max_response_bytes`.
const RESPONSE_SIZE_LIMIT_EXCEEDED: &str = "response_size_limit_exceeded";

/// Exact Ethereum JSON-RPC values derivable from one normalized block frame.
///
/// The snapshot is suitable for differential tests against a reference
/// execution client without involving this crate's HTTP transport.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcCompatibilitySnapshot {
    pub block_hashes: Value,
    pub block_full: Value,
    pub receipts: Value,
}

/// Failure to reconstruct a canonical Ethereum RPC compatibility snapshot.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("cannot reconstruct canonical Ethereum RPC response: {reason}")]
pub struct RpcCompatibilityError {
    reason: String,
}

impl RpcCompatibilityError {
    /// Stable machine-oriented reason when available.
    #[must_use]
    pub fn reason(&self) -> &str {
        &self.reason
    }
}

/// Reconstruct the exact block and receipt responses represented by a frame.
/// Blob receipts price blob gas with `schedule`, as the JSON-RPC server does.
///
/// # Errors
///
/// Fails closed when canonical header, transaction, receipt, withdrawal, or
/// signer material is absent, invalid, or internally inconsistent, or when
/// `schedule` has no fork for a block with blob receipts.
pub fn rpc_compatibility_snapshot(
    frame: &BlockFrame,
    schedule: &BlobSchedule,
) -> Result<RpcCompatibilitySnapshot, RpcCompatibilityError> {
    let block_hashes = rpc_block(frame, false).map_err(RpcCompatibilityError::from)?;
    let block_full = rpc_block(frame, true).map_err(RpcCompatibilityError::from)?;
    let receipts = rpc_receipts(frame, schedule)
        .and_then(serialize_rpc)
        .map_err(RpcCompatibilityError::from)?;
    Ok(RpcCompatibilitySnapshot {
        block_hashes,
        block_full,
        receipts,
    })
}

/// Default maximum requests in one JSON-RPC batch.
pub const DEFAULT_MAX_BATCH_REQUESTS: usize = 100;
/// Default maximum encoded bytes of one JSON-RPC response or batch response.
pub const DEFAULT_MAX_RESPONSE_BYTES: usize = 16 * 1_024 * 1_024;
/// Default maximum logs one `eth_getLogs` call returns.
pub const DEFAULT_MAX_LOG_RESULTS: usize = 10_000;
/// Default maximum addresses in one log filter.
pub const DEFAULT_MAX_LOG_ADDRESSES: usize = 1_000;
/// Default maximum alternatives at one topic position of a log filter.
pub const DEFAULT_MAX_LOG_TOPIC_ALTERNATIVES: usize = 1_000;
/// Default maximum live subscriptions on one WebSocket connection.
pub const DEFAULT_MAX_SUBSCRIPTIONS_PER_CONNECTION: usize = 128;
/// Default maximum concurrently open WebSocket connections.
pub const DEFAULT_MAX_WEBSOCKET_CONNECTIONS: usize = 256;
/// Default maximum encoded bytes of the subscription notifications one chain
/// event produces for one WebSocket connection, and of those a connection
/// holds unsent.
pub const DEFAULT_MAX_SUBSCRIPTION_EVENT_BYTES: usize = 16 * 1_024 * 1_024;

/// RPC transport and readiness settings.
#[derive(Clone, Debug)]
pub struct RpcConfig {
    pub chain_id: ChainId,
    pub max_request_bytes: usize,
    pub readiness: RpcReadiness,
    pub max_log_range: u64,
    pub websocket_enabled: bool,
    pub history: Option<HistoricalRpc>,
    /// Browser origins, besides loopback ones, allowed to call RPC, as
    /// lowercase ASCII serializations such as `https://app.example`. Requests
    /// with any other `Origin` header get HTTP 403.
    pub allowed_origins: BTreeSet<String>,
    pub max_batch_requests: usize,
    pub max_response_bytes: usize,
    pub max_log_results: usize,
    pub max_log_addresses: usize,
    pub max_log_topic_alternatives: usize,
    pub max_subscriptions_per_connection: usize,
    pub max_websocket_connections: usize,
    /// Encoded bytes of subscription notifications that one chain event may
    /// produce for one WebSocket connection, and that the connection may
    /// hold unsent. A connection past either is closed.
    pub max_subscription_event_bytes: usize,
    /// Cancelled when the node shuts down: open WebSocket connections then
    /// get a going-away close frame.
    pub shutdown: CancellationToken,
    /// Tracks open WebSocket connections, which outlive their listener's
    /// graceful shutdown, so the node can wait for their close frames.
    pub websocket_sessions: TaskTracker,
}

impl Default for RpcConfig {
    fn default() -> Self {
        Self {
            chain_id: ChainId(1),
            max_request_bytes: 1_048_576,
            readiness: RpcReadiness::default(),
            max_log_range: 1_024,
            websocket_enabled: false,
            history: None,
            allowed_origins: BTreeSet::new(),
            max_batch_requests: DEFAULT_MAX_BATCH_REQUESTS,
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
            max_log_results: DEFAULT_MAX_LOG_RESULTS,
            max_log_addresses: DEFAULT_MAX_LOG_ADDRESSES,
            max_log_topic_alternatives: DEFAULT_MAX_LOG_TOPIC_ALTERNATIVES,
            max_subscriptions_per_connection: DEFAULT_MAX_SUBSCRIPTIONS_PER_CONNECTION,
            max_websocket_connections: DEFAULT_MAX_WEBSOCKET_CONNECTIONS,
            max_subscription_event_bytes: DEFAULT_MAX_SUBSCRIPTION_EVENT_BYTES,
            shutdown: CancellationToken::new(),
            websocket_sessions: TaskTracker::new(),
        }
    }
}

/// Hard limits and trust policy for one ephemeral historical RPC request.
#[derive(Clone, Copy, Debug)]
pub struct HistoricalRpcConfig {
    pub max_range: u64,
    pub max_input_bytes: u64,
    pub max_frame_bytes: u64,
    pub max_buffered_frames: usize,
    pub max_in_flight_requests: usize,
    pub temporary_disk_bytes: u64,
    pub timeout: Duration,
    pub minimum_trust: TrustModel,
    pub verification_policy: VerificationPolicy,
}

impl Default for HistoricalRpcConfig {
    fn default() -> Self {
        Self {
            max_range: 1_024,
            max_input_bytes: 512 * 1_024 * 1_024,
            max_frame_bytes: 32 * 1_024 * 1_024,
            max_buffered_frames: 8,
            max_in_flight_requests: 2,
            temporary_disk_bytes: 512 * 1_024 * 1_024,
            timeout: Duration::from_mins(1),
            minimum_trust: TrustModel::TrustedDataset,
            verification_policy: VerificationPolicy::TrustedDataset,
        }
    }
}

/// Interchangeable, bounded history backends used only for the lifetime of an
/// RPC call.
#[derive(Clone)]
pub struct HistoricalRpc {
    sources: Arc<Vec<Arc<dyn HistorySource>>>,
    config: HistoricalRpcConfig,
    request_slots: Arc<Semaphore>,
}

impl std::fmt::Debug for HistoricalRpc {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HistoricalRpc")
            .field(
                "sources",
                &self
                    .sources
                    .iter()
                    .map(|source| source.descriptor())
                    .collect::<Vec<_>>(),
            )
            .field("config", &self.config)
            .field(
                "available_request_slots",
                &self.request_slots.available_permits(),
            )
            .finish()
    }
}

impl HistoricalRpc {
    /// Construct an on-demand router without opening a source.
    ///
    /// # Errors
    ///
    /// Rejects empty sources and zero resource or timeout limits.
    pub fn new(
        sources: Vec<Arc<dyn HistorySource>>,
        config: HistoricalRpcConfig,
    ) -> Result<Self, &'static str> {
        if sources.is_empty() {
            return Err("on-demand RPC requires at least one history source");
        }
        let source_ids = sources
            .iter()
            .map(|source| source.descriptor().id.clone())
            .collect::<BTreeSet<_>>();
        if source_ids.len() != sources.len() {
            return Err("on-demand RPC history source IDs must be unique");
        }
        if config.max_range == 0
            || config.max_input_bytes == 0
            || config.max_frame_bytes == 0
            || config.max_buffered_frames == 0
            || config.max_in_flight_requests == 0
            || config.timeout.is_zero()
        {
            return Err("on-demand RPC resource limits must be non-zero");
        }
        let request_slots = Arc::new(Semaphore::new(config.max_in_flight_requests));
        Ok(Self {
            sources: Arc::new(sources),
            config,
            request_slots,
        })
    }

    fn supports(&self, chain_id: ChainId, required: CapabilitySet) -> bool {
        let policy_trust = match self.config.verification_policy {
            VerificationPolicy::CompleteCryptographic => TrustModel::ProtocolVerified,
            VerificationPolicy::TrustedDataset => TrustModel::TrustedDataset,
            VerificationPolicy::BestEffort => TrustModel::Untrusted,
        };
        let minimum_trust = self.config.minimum_trust.max(policy_trust);
        self.sources.iter().any(|source| {
            let descriptor = source.descriptor();
            descriptor.chain_id == chain_id
                && descriptor
                    .complete_capabilities
                    .with_derivable()
                    .contains_all(required)
                && descriptor.finality.supports(Finality::Finalized)
                && descriptor.trust >= minimum_trust
        })
    }

    fn supports_block_hash_lookup(&self, chain_id: ChainId, required: CapabilitySet) -> bool {
        self.sources.iter().any(|source| {
            source.lookup_capabilities().block_hash
                && self.source_supports(source.as_ref(), chain_id, required)
        })
    }

    fn supports_transaction_lookup(&self, chain_id: ChainId, required: CapabilitySet) -> bool {
        self.sources.iter().any(|source| {
            source.lookup_capabilities().transaction_hash
                && self.source_supports(source.as_ref(), chain_id, required)
        })
    }

    fn source_supports(
        &self,
        source: &dyn HistorySource,
        chain_id: ChainId,
        required: CapabilitySet,
    ) -> bool {
        let policy_trust = match self.config.verification_policy {
            VerificationPolicy::CompleteCryptographic => TrustModel::ProtocolVerified,
            VerificationPolicy::TrustedDataset => TrustModel::TrustedDataset,
            VerificationPolicy::BestEffort => TrustModel::Untrusted,
        };
        let descriptor = source.descriptor();
        descriptor.chain_id == chain_id
            && descriptor
                .complete_capabilities
                .with_derivable()
                .contains_all(required)
            && descriptor.finality.supports(Finality::Finalized)
            && descriptor.trust >= self.config.minimum_trust.max(policy_trust)
    }

    async fn block_by_hash(
        &self,
        chain_id: ChainId,
        hash: BlockHash,
        required: CapabilitySet,
    ) -> Result<Option<BlockFrame>, HistoricalRpcError> {
        let result = tokio::time::timeout(self.config.timeout, async {
            let _permit = self
                .request_slots
                .acquire()
                .await
                .map_err(|_| HistoricalRpcError::Unavailable)?;
            let mut candidates = self.sources.iter().collect::<Vec<_>>();
            candidates.sort_by_key(|source| source.descriptor().priority);
            for source in candidates {
                if !source.lookup_capabilities().block_hash
                    || !self.source_supports(source.as_ref(), chain_id, required)
                {
                    continue;
                }
                if let Some(frame) = source
                    .block_by_hash(chain_id, hash, required)
                    .await
                    .map_err(|error| historical_source_error(&error))?
                {
                    return Ok(Some(frame));
                }
            }
            Ok(None)
        })
        .await;
        result.unwrap_or(Err(HistoricalRpcError::Timeout))
    }

    async fn transaction_by_hash(
        &self,
        chain_id: ChainId,
        hash: TransactionHash,
        required: CapabilitySet,
    ) -> Result<Option<leani_source_api::LocatedTransaction>, HistoricalRpcError> {
        let result = tokio::time::timeout(self.config.timeout, async {
            let _permit = self
                .request_slots
                .acquire()
                .await
                .map_err(|_| HistoricalRpcError::Unavailable)?;
            let mut candidates = self.sources.iter().collect::<Vec<_>>();
            candidates.sort_by_key(|source| source.descriptor().priority);
            for source in candidates {
                if !source.lookup_capabilities().transaction_hash
                    || !self.source_supports(source.as_ref(), chain_id, required)
                {
                    continue;
                }
                if let Some(transaction) = source
                    .transaction_by_hash(chain_id, hash, required)
                    .await
                    .map_err(|error| historical_source_error(&error))?
                {
                    return Ok(Some(transaction));
                }
            }
            Ok(None)
        })
        .await;
        result.unwrap_or(Err(HistoricalRpcError::Timeout))
    }

    async fn fetch(
        &self,
        chain_id: ChainId,
        range: BlockRange,
        required: CapabilitySet,
        filters: FilterSet,
    ) -> Result<Vec<BlockFrame>, HistoricalRpcError> {
        if range.len() > self.config.max_range {
            return Err(HistoricalRpcError::RangeLimit);
        }
        let cancellation = CancellationToken::new();
        let result = tokio::time::timeout(self.config.timeout, async {
            let _permit = self
                .request_slots
                .acquire()
                .await
                .map_err(|_| HistoricalRpcError::Unavailable)?;
            self.fetch_inner(chain_id, range, required, filters, cancellation.clone())
                .await
        })
        .await;
        if let Ok(result) = result {
            result
        } else {
            cancellation.cancel();
            Err(HistoricalRpcError::Timeout)
        }
    }

    async fn fetch_inner(
        &self,
        chain_id: ChainId,
        range: BlockRange,
        required: CapabilitySet,
        filters: FilterSet,
        cancellation: CancellationToken,
    ) -> Result<Vec<BlockFrame>, HistoricalRpcError> {
        let request = DataRequest {
            chain_id,
            range,
            required,
            allow_filtered: false,
            projection: FieldProjection::default(),
            log_fields: leani_primitives::LogFieldSet::ALL,
            filters,
            minimum_finality: Finality::Finalized,
            verification_policy: self.config.verification_policy,
        };
        let mut candidates = self.sources.iter().cloned().collect::<Vec<_>>();
        let mut last_error = None;
        while !candidates.is_empty() {
            let descriptors = candidates
                .iter()
                .map(|source| source.descriptor().clone())
                .collect::<Vec<_>>();
            let selected = match select_source(
                &descriptors,
                &request,
                SelectionPolicy {
                    minimum_trust: self.config.minimum_trust,
                    prefer_complete: true,
                },
            ) {
                Ok(selected) => selected.id.clone(),
                Err(_) => {
                    return Err(last_error.unwrap_or(HistoricalRpcError::Unavailable));
                }
            };
            let position = candidates
                .iter()
                .position(|source| source.descriptor().id == selected)
                .ok_or(HistoricalRpcError::Invalid)?;
            let source = candidates.remove(position);
            match self
                .fetch_from_source(source.as_ref(), &request, cancellation.clone())
                .await
            {
                Ok(frames) => return Ok(frames),
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or(HistoricalRpcError::Unavailable))
    }

    async fn fetch_from_source(
        &self,
        source: &dyn HistorySource,
        request: &DataRequest,
        cancellation: CancellationToken,
    ) -> Result<Vec<BlockFrame>, HistoricalRpcError> {
        let plan = source
            .plan(request)
            .await
            .map_err(|error| historical_source_error(&error))?;
        plan.validate()
            .map_err(|error| historical_source_error(&error))?;
        if plan
            .estimated_bytes
            .is_some_and(|bytes| bytes > self.config.max_input_bytes)
        {
            return Err(HistoricalRpcError::Budget);
        }
        let frame_limit = request.range.len();
        let budget = SourceBudget {
            max_input_bytes: self.config.max_input_bytes,
            max_frame_bytes: self.config.max_frame_bytes,
            max_frames: frame_limit,
            max_buffered_frames: self.config.max_buffered_frames,
            max_in_flight_requests: self.config.max_in_flight_requests,
            temporary_disk_bytes: self.config.temporary_disk_bytes,
        };
        let mut frames = Vec::with_capacity(
            usize::try_from(frame_limit).map_err(|_| HistoricalRpcError::Invalid)?,
        );
        let mut observed_bytes = 0_u64;
        for chunk in &plan.chunks {
            let mut stream = source
                .open(chunk, budget, cancellation.clone())
                .await
                .map_err(|error| historical_source_error(&error))?;
            while let Some(frame) = stream.next().await {
                let frame = frame.map_err(|error| historical_source_error(&error))?;
                observed_bytes = observed_bytes.saturating_add(frame.estimated_heap_bytes());
                if observed_bytes > self.config.max_input_bytes {
                    return Err(HistoricalRpcError::Budget);
                }
                frames.push(frame);
            }
        }
        validate_historical_frames(&frames, request)?;
        Ok(frames)
    }
}

fn historical_source_error(error: &SourceError) -> HistoricalRpcError {
    match error {
        SourceError::BudgetExceeded { .. } | SourceError::InvalidBudget => {
            HistoricalRpcError::Budget
        }
        SourceError::Cancelled => HistoricalRpcError::Timeout,
        SourceError::MissingRange(_)
        | SourceError::IncompleteRange { .. }
        | SourceError::MissingMaterial { .. }
        | SourceError::InvalidPlan(_) => HistoricalRpcError::Unavailable,
        SourceError::SchemaDrift { .. }
        | SourceError::CorruptFrame(_)
        | SourceError::Protocol(_) => HistoricalRpcError::Invalid,
        SourceError::Disconnected(_) | SourceError::Unavailable(_) => HistoricalRpcError::Source,
    }
}

#[derive(Clone, Copy, Debug)]
enum HistoricalRpcError {
    Unavailable,
    Source,
    Invalid,
    Budget,
    RangeLimit,
    Timeout,
}

fn validate_historical_frames(
    frames: &[BlockFrame],
    request: &DataRequest,
) -> Result<(), HistoricalRpcError> {
    if u64::try_from(frames.len()).unwrap_or(u64::MAX) != request.range.len() {
        return Err(HistoricalRpcError::Invalid);
    }
    for (offset, frame) in frames.iter().enumerate() {
        let expected_number = request
            .range
            .start()
            .0
            .saturating_add(u64::try_from(offset).map_err(|_| HistoricalRpcError::Invalid)?);
        if frame.chain_id != request.chain_id
            || frame.block.number != BlockNumber(expected_number)
            || !frame.finality.satisfies(request.minimum_finality)
            || !frame.capabilities().satisfies(request.required, false)
        {
            return Err(HistoricalRpcError::Invalid);
        }
        if let Material::Complete(header) = &frame.header
            && header.withdrawals_root.is_some()
            && !matches!(frame.withdrawals, Material::Complete(_))
        {
            return Err(HistoricalRpcError::Invalid);
        }
        frame
            .validate_shape()
            .map_err(|_| HistoricalRpcError::Invalid)?;
    }
    for pair in frames.windows(2) {
        if pair[1].block.parent_hash != pair[0].block.hash {
            return Err(HistoricalRpcError::Invalid);
        }
    }
    Ok(())
}

/// Dynamically shared live readiness for JSON-RPC metadata.
#[derive(Clone, Debug, Default)]
pub struct RpcReadiness(Arc<AtomicBool>);

impl RpcReadiness {
    pub fn set_live_ready(&self, ready: bool) {
        self.0.store(ready, Ordering::Release);
    }

    #[must_use]
    pub fn live_ready(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

#[derive(Clone)]
struct RpcState {
    store: SqliteStore,
    progress: Arc<dyn Processor>,
    blob_schedule: Arc<BlobSchedule>,
    config: RpcConfig,
    committed_events: broadcast::Sender<ChainEvent>,
    /// One permit per open WebSocket connection, held until it closes.
    websocket_connections: Arc<Semaphore>,
}

impl RpcState {
    fn new(
        store: SqliteStore,
        progress: Arc<dyn Processor>,
        blob_schedule: Arc<BlobSchedule>,
        mut config: RpcConfig,
        committed_events: broadcast::Sender<ChainEvent>,
    ) -> Self {
        let websocket_connections = Arc::new(Semaphore::new(
            config.max_websocket_connections.min(Semaphore::MAX_PERMITS),
        ));
        // As browsers serialize origins: lowercase, without a trailing slash.
        config.allowed_origins = config
            .allowed_origins
            .iter()
            .map(|origin| origin.trim_end_matches('/').to_ascii_lowercase())
            .collect();
        Self {
            store,
            progress,
            blob_schedule,
            config,
            committed_events,
            websocket_connections,
        }
    }
}

impl std::fmt::Debug for RpcState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RpcState")
            .field("store", &self.store)
            .field("progress", &self.progress.descriptor())
            .field("blob_schedule", &self.blob_schedule)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// Construct the HTTP JSON-RPC router.
///
/// # Panics
///
/// Panics only when a zero chain ID or body limit is supplied. These values
/// are programmer/configuration errors validated by the node before startup.
pub fn router(store: SqliteStore, blobs: Arc<BlobsProcessor>, config: RpcConfig) -> Router {
    let (committed_events, _) = broadcast::channel(1);
    http_router(store, blobs, config, committed_events)
}

/// Construct the HTTP JSON-RPC router with the node's committed live-event
/// channel.
pub fn http_router(
    store: SqliteStore,
    blobs: Arc<BlobsProcessor>,
    config: RpcConfig,
    committed_events: broadcast::Sender<ChainEvent>,
) -> Router {
    let schedule = Arc::new(blobs.schedule().clone());
    let progress: Arc<dyn Processor> = blobs;
    http_router_with_progress(store, progress, schedule, config, committed_events)
}

/// Construct HTTP JSON-RPC using any configured processor as the durable
/// progress cursor. The blob schedule remains an optional chain-configuration
/// helper rather than a required configured processor.
pub fn http_router_with_progress(
    store: SqliteStore,
    progress: Arc<dyn Processor>,
    blob_schedule: Arc<BlobSchedule>,
    config: RpcConfig,
    committed_events: broadcast::Sender<ChainEvent>,
) -> Router {
    rpc_router(
        store,
        progress,
        blob_schedule,
        config,
        committed_events,
        false,
    )
}

/// Construct the WebSocket JSON-RPC router.
pub fn websocket_router(
    store: SqliteStore,
    blobs: Arc<BlobsProcessor>,
    config: RpcConfig,
    committed_events: broadcast::Sender<ChainEvent>,
) -> Router {
    let schedule = Arc::new(blobs.schedule().clone());
    let progress: Arc<dyn Processor> = blobs;
    websocket_router_with_progress(store, progress, schedule, config, committed_events)
}

/// Construct WebSocket JSON-RPC using any configured processor as the durable
/// progress cursor.
pub fn websocket_router_with_progress(
    store: SqliteStore,
    progress: Arc<dyn Processor>,
    blob_schedule: Arc<BlobSchedule>,
    config: RpcConfig,
    committed_events: broadcast::Sender<ChainEvent>,
) -> Router {
    rpc_router(
        store,
        progress,
        blob_schedule,
        config,
        committed_events,
        true,
    )
}

fn rpc_router(
    store: SqliteStore,
    progress: Arc<dyn Processor>,
    blob_schedule: Arc<BlobSchedule>,
    config: RpcConfig,
    committed_events: broadcast::Sender<ChainEvent>,
    websocket: bool,
) -> Router {
    assert!(config.chain_id.0 > 0, "chain ID must be non-zero");
    assert!(config.max_request_bytes > 0, "body limit must be non-zero");
    assert!(config.max_log_range > 0, "log range must be non-zero");
    let max_request_bytes = config.max_request_bytes;
    let state = RpcState::new(store, progress, blob_schedule, config, committed_events);
    let route = if websocket {
        get(websocket_upgrade)
    } else {
        post(handle).get(health)
    };
    Router::new()
        .route("/", route)
        .layer(DefaultBodyLimit::max(max_request_bytes))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            guard_browser_requests,
        ))
        .with_state(state)
}

async fn health() -> Json<Value> {
    Json(json!({
        "service": "leani-json-rpc",
        "jsonrpc": "2.0"
    }))
}

async fn websocket_upgrade(State(state): State<RpcState>, upgrade: WebSocketUpgrade) -> Response {
    let Ok(connection) = state.websocket_connections.clone().try_acquire_owned() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "the WebSocket connection limit is reached; retry later",
        )
            .into_response();
    };
    upgrade
        .max_message_size(state.config.max_request_bytes)
        .on_upgrade(move |socket| {
            let sessions = state.config.websocket_sessions.clone();
            sessions.track_future(websocket_session(socket, state, connection))
        })
}

#[derive(Clone, Debug)]
enum Subscription {
    NewHeads,
    Logs(ParsedLogFilter),
}

/// How long a connection being closed may take to accept its close frame
/// before it is dropped; one that stopped reading never does.
const WEBSOCKET_CLOSE_TIMEOUT: Duration = Duration::from_secs(1);

/// Serve one WebSocket connection. `_connection` holds its connection slot
/// until the connection closes; its subscriptions and unsent messages go
/// with it. The node's shutdown closes it as going away.
async fn websocket_session(socket: WebSocket, state: RpcState, _connection: OwnedSemaphorePermit) {
    let (mut sink, stream) = socket.split();
    let (outbox, mut queue) = Outbox::new(state.config.max_subscription_event_bytes);
    let close = tokio::select! {
        close = serve_websocket(&state, stream, outbox) => close,
        () = send_queued(&mut sink, &mut queue) => None,
        () = state.config.shutdown.cancelled() => Some(CloseFrame {
            code: close_code::AWAY,
            reason: "the node is shutting down".into(),
        }),
    };
    // The close frame goes next: messages still queued are dropped.
    drop(queue);
    if let Some(close) = close {
        let _ = tokio::time::timeout(
            WEBSOCKET_CLOSE_TIMEOUT,
            sink.send(Message::Close(Some(close))),
        )
        .await;
    }
}

/// Answer a connection's calls and queue its subscription notifications
/// until it ends, returning the frame to close it with, if any.
async fn serve_websocket(
    state: &RpcState,
    mut stream: SplitStream<WebSocket>,
    outbox: Outbox,
) -> Option<CloseFrame> {
    let mut events = state.committed_events.subscribe();
    let mut subscriptions = BTreeMap::new();
    let mut next_subscription = 1_u64;
    loop {
        tokio::select! {
            message = outbox.next_message(&mut stream) => {
                let (message, room) = message?;
                let response = match message {
                    Message::Text(text) => websocket_dispatch_text(
                        state,
                        &mut subscriptions,
                        &mut next_subscription,
                        text.as_str(),
                    )
                    .await
                    .map(|response| Message::Text(response.into())),
                    Message::Ping(payload) => Some(Message::Pong(payload)),
                    Message::Pong(_) => None,
                    Message::Close(_) => return None,
                    Message::Binary(_) => {
                        return Some(CloseFrame {
                            code: close_code::UNSUPPORTED,
                            reason: "JSON-RPC messages must be UTF-8 text".into(),
                        });
                    }
                };
                if let Some(response) = response {
                    outbox.respond(response, room);
                }
            }
            event = events.recv() => match event {
                Ok(event) => {
                    if let Err(close) =
                        queue_notifications(state, &subscriptions, &event, &outbox).await
                    {
                        return Some(close);
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    return Some(CloseFrame {
                        code: close_code::AGAIN,
                        reason: "subscription event buffer overflow; reconnect".into(),
                    });
                }
                Err(broadcast::error::RecvError::Closed) => return None,
            },
        }
    }
}

/// Send a connection's queued messages in order, until sending fails.
async fn send_queued(
    sink: &mut SplitSink<WebSocket, Message>,
    queue: &mut mpsc::UnboundedReceiver<Queued>,
) {
    while let Some((message, _room)) = queue.recv().await {
        if sink.send(message.into_message()).await.is_err() {
            return;
        }
    }
}

/// A queued message and the room it takes in the queue until it is sent.
type Queued = (Outgoing, OwnedSemaphorePermit);

/// A WebSocket connection's queue of messages to send. It holds at most one
/// response to a call: the next call is read once the response to the last
/// one is sent, so a client that stops reading stops being answered. Chain
/// events queue subscription notifications whether the client reads them or
/// not, within `limit` encoded bytes: an event whose notifications pass
/// `limit` by themselves, or find no room left in the queue, closes the
/// connection instead.
struct Outbox {
    /// Bounded not by the channel but by the room its messages hold.
    queue: mpsc::UnboundedSender<Queued>,
    /// Room for one response.
    responses: Arc<Semaphore>,
    /// Room for `limit` bytes of notifications.
    notifications: Arc<Semaphore>,
    limit: usize,
}

impl Outbox {
    fn new(limit: usize) -> (Self, mpsc::UnboundedReceiver<Queued>) {
        let (queue, queued) = mpsc::unbounded_channel();
        let outbox = Self {
            queue,
            responses: Arc::new(Semaphore::new(1)),
            notifications: Arc::new(Semaphore::new(limit.min(Semaphore::MAX_PERMITS))),
            limit,
        };
        (outbox, queued)
    }

    /// The connection's next message, read once the queue has room for a
    /// response to it; `None` once the connection ends.
    async fn next_message(
        &self,
        stream: &mut SplitStream<WebSocket>,
    ) -> Option<(Message, OwnedSemaphorePermit)> {
        let room = Arc::clone(&self.responses).acquire_owned().await.ok()?;
        let message = stream.next().await?.ok()?;
        Some((message, room))
    }

    fn respond(&self, response: Message, room: OwnedSemaphorePermit) {
        // Sending fails only once the session ends, dropping its queue.
        let _ = self.queue.send((Outgoing::Message(response), room));
    }

    /// Queue the notification of `result` for `subscription`, adding its
    /// bytes to `event_bytes`, those of its chain event's notifications.
    fn notify(
        &self,
        subscription: &str,
        result: &Arc<str>,
        event_bytes: &mut usize,
    ) -> Result<(), CloseFrame> {
        let bytes = notification_len(subscription, result);
        *event_bytes = event_bytes.saturating_add(bytes);
        if *event_bytes > self.limit {
            return Err(event_budget_close(self.limit));
        }
        let room = u32::try_from(bytes)
            .ok()
            .and_then(|bytes| {
                Arc::clone(&self.notifications)
                    .try_acquire_many_owned(bytes)
                    .ok()
            })
            .ok_or_else(|| slow_client_close(self.limit))?;
        let notification = Outgoing::Notification {
            subscription: subscription.to_owned(),
            result: Arc::clone(result),
        };
        let _ = self.queue.send((notification, room));
        Ok(())
    }
}

/// The frame closing a connection whose subscriptions matched more than
/// `limit` bytes of notifications in one chain event. They would again on a
/// like event, so this is a policy violation rather than a passing state.
fn event_budget_close(limit: usize) -> CloseFrame {
    CloseFrame {
        code: close_code::POLICY,
        reason: format!(
            "notifications of one chain event exceed rpc.max_subscription_event_bytes ({limit})"
        )
        .into(),
    }
}

/// The frame closing a connection that left `limit` bytes of notifications
/// unread. It may keep up after reconnecting, so this asks it to try again.
fn slow_client_close(limit: usize) -> CloseFrame {
    CloseFrame {
        code: close_code::AGAIN,
        reason: format!(
            "client too slow: unsent notifications exceed rpc.max_subscription_event_bytes ({limit})"
        )
        .into(),
    }
}

/// A message waiting in a connection's queue.
enum Outgoing {
    Message(Message),
    /// A subscription notification, encoded when it is sent. Its result is
    /// shared with the connection's other notifications of the same head or
    /// log.
    Notification {
        subscription: String,
        result: Arc<str>,
    },
}

impl Outgoing {
    fn into_message(self) -> Message {
        match self {
            Self::Message(message) => message,
            Self::Notification {
                subscription,
                result,
            } => Message::Text(notification_text(&subscription, &result).into()),
        }
    }
}

/// A subscription notification's text around its subscription ID and its
/// result. Subscription IDs are hexadecimal and need no escaping.
const NOTIFICATION_PARTS: [&str; 3] = [
    r#"{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":""#,
    r#"","result":"#,
    "}}",
];

fn notification_len(subscription: &str, result: &str) -> usize {
    NOTIFICATION_PARTS
        .iter()
        .map(|part| part.len())
        .sum::<usize>()
        + subscription.len()
        + result.len()
}

fn notification_text(subscription: &str, result: &str) -> String {
    let [open, middle, close] = NOTIFICATION_PARTS;
    let mut text = String::with_capacity(notification_len(subscription, result));
    for part in [open, subscription, middle, result, close] {
        text.push_str(part);
    }
    text
}

async fn websocket_dispatch_text(
    state: &RpcState,
    subscriptions: &mut BTreeMap<String, Subscription>,
    next_subscription: &mut u64,
    input: &str,
) -> Option<String> {
    let (mut encoder, calls) = match ResponseEncoder::for_message(input.as_bytes(), &state.config) {
        Ok(message) => message,
        Err(refused) => return Some(refused),
    };
    for call in calls {
        if encoder.skips(&call) {
            encoder.skip(call);
        } else if let Some(response) = websocket_dispatch_value(
            state,
            subscriptions,
            next_subscription,
            call,
            encoder.remaining(),
        )
        .await
        {
            encoder.push(response);
        }
    }
    encoder.finish()
}

async fn websocket_dispatch_value(
    state: &RpcState,
    subscriptions: &mut BTreeMap<String, Subscription>,
    next_subscription: &mut u64,
    value: Value,
    budget: usize,
) -> Option<RpcResponse> {
    let Some(request) = RpcRequest::parse(value) else {
        return Some(RpcResponse::error(
            Value::Null,
            INVALID_REQUEST,
            "Invalid Request",
            None,
        ));
    };
    let subscription_limit = state.config.max_subscriptions_per_connection;
    let result = match request.method.as_str() {
        "eth_subscribe" if subscriptions.len() >= subscription_limit => Err(
            RpcError::limit_exceeded("subscription_limit_exceeded", subscription_limit),
        ),
        "eth_subscribe" => {
            parse_subscription(request.params.as_ref(), &state.config).map(|subscription| {
                let id = format!("0x{:032x}", *next_subscription);
                *next_subscription = next_subscription.saturating_add(1);
                subscriptions.insert(id.clone(), subscription);
                Value::String(id)
            })
        }
        "eth_unsubscribe" => parse_unsubscribe(request.params.as_ref())
            .map(|subscription| Value::Bool(subscriptions.remove(subscription).is_some())),
        method => dispatch(state, method, request.params, budget).await,
    };
    // A notification runs like any call; only its response is left out.
    request.id.map(|id| RpcResponse::answer(id, result))
}

fn parse_subscription(
    params: Option<&Value>,
    config: &RpcConfig,
) -> Result<Subscription, RpcError> {
    let values = params
        .and_then(Value::as_array)
        .ok_or_else(|| RpcError::invalid_params("eth_subscribe expects an array"))?;
    let Some(kind) = values.first().and_then(Value::as_str) else {
        return Err(RpcError::invalid_params(
            "eth_subscribe requires a subscription name",
        ));
    };
    match kind {
        "newHeads" if values.len() == 1 => Ok(Subscription::NewHeads),
        "logs" if values.len() <= 2 => {
            let filter = values.get(1).cloned().unwrap_or_else(|| json!({}));
            let object = filter
                .as_object()
                .ok_or_else(|| RpcError::invalid_params("log filter must be an object"))?;
            if object.contains_key("fromBlock")
                || object.contains_key("toBlock")
                || object.contains_key("blockHash")
            {
                return Err(RpcError::invalid_params(
                    "log subscriptions accept only address and topics filters",
                ));
            }
            parse_log_filter(Some(&Value::Array(vec![filter])), config).map(Subscription::Logs)
        }
        "newHeads" | "logs" => Err(RpcError::invalid_params(
            "subscription has an invalid parameter count",
        )),
        _ => Err(RpcError::data_unavailable_reason(
            "subscription_type_unsupported",
        )),
    }
}

fn parse_unsubscribe(params: Option<&Value>) -> Result<&str, RpcError> {
    let values = params
        .and_then(Value::as_array)
        .ok_or_else(|| RpcError::invalid_params("eth_unsubscribe expects one subscription ID"))?;
    if values.len() != 1 {
        return Err(RpcError::invalid_params(
            "eth_unsubscribe expects one subscription ID",
        ));
    }
    values[0]
        .as_str()
        .ok_or_else(|| RpcError::invalid_params("subscription ID must be a string"))
}

/// Queue the notifications `event` produces for a connection's
/// `subscriptions`, one at a time: a head or log is encoded once, however
/// many subscriptions it matches. A reorg's removed logs come first. Fails
/// with the frame to close the connection with when the event's material is
/// unavailable, or when its notifications pass the outbox's limits.
async fn queue_notifications(
    state: &RpcState,
    subscriptions: &BTreeMap<String, Subscription>,
    event: &ChainEvent,
    outbox: &Outbox,
) -> Result<(), CloseFrame> {
    if subscriptions.is_empty() {
        return Ok(());
    }
    let heads = subscriptions
        .iter()
        .filter(|(_, subscription)| matches!(subscription, Subscription::NewHeads))
        .map(|(id, _)| id.as_str())
        .collect::<Vec<_>>();
    let logs = subscriptions
        .iter()
        .filter_map(|(id, subscription)| match subscription {
            Subscription::Logs(filter) => Some((id.as_str(), filter)),
            Subscription::NewHeads => None,
        })
        .collect::<Vec<_>>();
    let (reverted, applied) = match event {
        ChainEvent::Block(frame) => (Vec::new(), std::slice::from_ref(frame.as_ref())),
        ChainEvent::Reorg { applied, .. } if logs.is_empty() => (Vec::new(), applied.as_slice()),
        ChainEvent::Reorg { reverted, applied } => (
            reverted_frames(state, reverted)
                .await
                .map_err(|error| unavailable_close(&error))?,
            applied.as_slice(),
        ),
        ChainEvent::Disconnected { .. } => {
            return Err(unavailable_close(&RpcError::data_unavailable_reason(
                "live_subscription_disconnected",
            )));
        }
        ChainEvent::Reset { .. } => {
            return Err(unavailable_close(&RpcError::data_unavailable_reason(
                "live_subscription_reset",
            )));
        }
    };
    let mut bytes = 0;
    for frame in &reverted {
        queue_log_notifications(frame, &logs, true, outbox, &mut bytes)?;
    }
    for frame in applied {
        if !heads.is_empty() {
            let head = rpc_header(frame)
                .and_then(serialize_rpc)
                .map_err(|error| unavailable_close(&error))?;
            let head: Arc<str> = Arc::from(head.to_string());
            for subscription in &heads {
                outbox.notify(subscription, &head, &mut bytes)?;
            }
        }
        queue_log_notifications(frame, &logs, false, outbox, &mut bytes)?;
    }
    Ok(())
}

/// The retained frames of a reorg's reverted blocks.
async fn reverted_frames(
    state: &RpcState,
    reverted: &[BlockRef],
) -> Result<Vec<BlockFrame>, RpcError> {
    let mut frames = Vec::with_capacity(reverted.len());
    for block in reverted {
        frames.push(
            state
                .store
                .recent_frame_by_hash(state.config.chain_id, block.hash)
                .await
                .map_err(|error| RpcError::store(&error))?
                .ok_or_else(|| {
                    RpcError::data_unavailable_reason("reverted_subscription_frame_missing")
                })?,
        );
    }
    Ok(frames)
}

/// Queue the notifications of `frame`'s logs for the log `subscriptions`
/// they match, marked `removed` for a reverted block.
fn queue_log_notifications(
    frame: &BlockFrame,
    subscriptions: &[(&str, &ParsedLogFilter)],
    removed: bool,
    outbox: &Outbox,
    event_bytes: &mut usize,
) -> Result<(), CloseFrame> {
    if subscriptions.is_empty() {
        return Ok(());
    }
    let logs = complete(&frame.logs, "complete_logs_not_retained")
        .map_err(|error| unavailable_close(&error))?;
    for log in logs {
        let mut matching = subscriptions
            .iter()
            .filter(|(_, filter)| filter.matches(log))
            .peekable();
        if matching.peek().is_none() {
            continue;
        }
        let result =
            rpc_log_value(frame, log, removed).map_err(|error| unavailable_close(&error))?;
        let result: Arc<str> = Arc::from(result.to_string());
        for (subscription, _) in matching {
            outbox.notify(subscription, &result, event_bytes)?;
        }
    }
    Ok(())
}

/// The frame closing a connection whose subscriptions cannot be served.
fn unavailable_close(error: &RpcError) -> CloseFrame {
    let reason = error
        .data
        .as_ref()
        .and_then(|data| data.get("reason"))
        .and_then(Value::as_str)
        .unwrap_or("subscription material unavailable");
    CloseFrame {
        code: close_code::ERROR,
        reason: reason.to_owned().into(),
    }
}

async fn handle(State(state): State<RpcState>, body: Result<Bytes, BytesRejection>) -> Response {
    let body = match body {
        Ok(body) => body,
        Err(rejection) => return refused_body(&rejection, state.config.max_request_bytes),
    };
    let (mut encoder, calls) = match ResponseEncoder::for_message(&body, &state.config) {
        Ok(message) => message,
        Err(refused) => return json_body(refused),
    };
    for call in calls {
        if encoder.skips(&call) {
            encoder.skip(call);
        } else if let Some(response) = dispatch_value(&state, call, encoder.remaining()).await {
            encoder.push(response);
        }
    }
    encoder
        .finish()
        .map_or_else(|| StatusCode::NO_CONTENT.into_response(), json_body)
}

fn json_body(body: String) -> Response {
    ([(header::CONTENT_TYPE, "application/json")], body).into_response()
}

/// Answer a body that could not be read with a JSON-RPC error instead of
/// axum's plain text: HTTP 413 and `-32005` past `max_request_bytes`, the
/// rejection's status and `-32700` otherwise.
fn refused_body(rejection: &BytesRejection, max_request_bytes: usize) -> Response {
    let status = rejection.status();
    let response = if status == StatusCode::PAYLOAD_TOO_LARGE {
        let error = RpcError::limit_exceeded("request_size_limit_exceeded", max_request_bytes);
        RpcResponse::error(Value::Null, error.code, error.message, error.data)
    } else {
        RpcResponse::error(
            Value::Null,
            PARSE_ERROR,
            "Parse error",
            Some(json!({ "detail": rejection.body_text() })),
        )
    };
    (status, json_body(encode_response(&response))).into_response()
}

/// Encodes the responses to one message within `max_response_bytes`,
/// counting a batch's brackets and commas. The response that would pass the
/// limit, and every later one, becomes a `-32005` error for its request ID;
/// the calls after it do not run, except notifications, which need no room
/// in it. A call that stops early because its result would pass the limit,
/// as `eth_getLogs` does, counts as passing it.
struct ResponseEncoder {
    limit: usize,
    batch: bool,
    bytes: usize,
    responses: Vec<String>,
    full: bool,
}

impl ResponseEncoder {
    /// An encoder for `input` and the calls it holds: one, or a batch of at
    /// most `max_batch_requests`. Input that is not UTF-8 JSON, and an empty
    /// or oversized batch, are refused whole, with one encoded error object.
    fn for_message(input: &[u8], config: &RpcConfig) -> Result<(Self, Vec<Value>), String> {
        let parsed = serde_json::from_slice::<Value>(input).map_err(|error| {
            encode_response(&RpcResponse::error(
                Value::Null,
                PARSE_ERROR,
                "Parse error",
                Some(json!({ "detail": error.to_string() })),
            ))
        })?;
        let (calls, batch) = match parsed {
            Value::Array(calls) => (calls, true),
            call => (vec![call], false),
        };
        if batch && (calls.is_empty() || calls.len() > config.max_batch_requests) {
            return Err(encode_response(&RpcResponse::error(
                Value::Null,
                INVALID_REQUEST,
                "Invalid Request",
                (!calls.is_empty()).then(|| {
                    json!({
                        "detail": "batch exceeds the request limit",
                        "limit": config.max_batch_requests
                    })
                }),
            )));
        }
        let encoder = Self {
            limit: config.max_response_bytes,
            batch,
            bytes: if batch { 2 } else { 0 },
            responses: Vec::new(),
            full: false,
        };
        Ok((encoder, calls))
    }

    /// Whether `call` is left unrun: once the response is full, every call
    /// but a notification is.
    fn skips(&self, call: &Value) -> bool {
        let notification = call
            .as_object()
            .is_some_and(|call| !call.contains_key("id"));
        self.full && !notification
    }

    /// Bytes the next response may still use, after its separating comma.
    fn remaining(&self) -> usize {
        let separator = usize::from(self.batch && !self.responses.is_empty());
        self.limit
            .saturating_sub(self.bytes.saturating_add(separator))
    }

    /// Answer a call left unrun because the response is full: nothing for a
    /// notification, a limit error with a null ID for an invalid request.
    fn skip(&mut self, call: Value) {
        let id = RpcRequest::parse(call).map_or(Some(Value::Null), |request| request.id);
        if let Some(id) = id {
            self.push(response_too_large(id, self.limit));
        }
    }

    fn push(&mut self, response: RpcResponse) {
        let stopped_at_limit = response.over_budget;
        let separator = usize::from(self.batch && !self.responses.is_empty());
        let encoded = (!self.full)
            .then(|| encode_response(&response))
            .filter(|encoded| {
                self.bytes
                    .saturating_add(separator)
                    .saturating_add(encoded.len())
                    <= self.limit
            });
        let encoded = encoded.unwrap_or_else(|| {
            self.full = true;
            encode_response(&response_too_large(response.id, self.limit))
        });
        self.bytes = self
            .bytes
            .saturating_add(separator)
            .saturating_add(encoded.len());
        self.responses.push(encoded);
        self.full |= stopped_at_limit;
    }

    fn finish(self) -> Option<String> {
        if self.responses.is_empty() {
            None
        } else if self.batch {
            Some(format!("[{}]", self.responses.join(",")))
        } else {
            self.responses.into_iter().next()
        }
    }
}

fn encode_response(response: &RpcResponse) -> String {
    // Responses hold only JSON values and strings, which always encode.
    serde_json::to_string(response).unwrap_or_else(|_| {
        r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32603,"message":"Internal error"}}"#
            .to_owned()
    })
}

fn response_too_large(id: Value, limit: usize) -> RpcResponse {
    let error = RpcError::limit_exceeded(RESPONSE_SIZE_LIMIT_EXCEEDED, limit);
    RpcResponse::error(id, error.code, error.message, error.data)
}

/// Answer one call. `budget` is the number of response bytes still free;
/// results that can grow past it stop early.
async fn dispatch_value(state: &RpcState, value: Value, budget: usize) -> Option<RpcResponse> {
    let Some(request) = RpcRequest::parse(value) else {
        return Some(RpcResponse::error(
            Value::Null,
            INVALID_REQUEST,
            "Invalid Request",
            None,
        ));
    };
    let result = dispatch(state, &request.method, request.params, budget).await;
    // A notification runs like any call; only its response is left out.
    request.id.map(|id| RpcResponse::answer(id, result))
}

async fn dispatch(
    state: &RpcState,
    method: &str,
    params: Option<Value>,
    budget: usize,
) -> Result<Value, RpcError> {
    match method {
        "web3_clientVersion" => {
            require_no_params(params)?;
            Ok(Value::String(format!(
                "leani/v{}/rust",
                env!("CARGO_PKG_VERSION")
            )))
        }
        "net_version" => {
            require_no_params(params)?;
            Ok(Value::String(state.config.chain_id.0.to_string()))
        }
        "eth_chainId" => {
            require_no_params(params)?;
            Ok(Value::String(hex_quantity(state.config.chain_id.0)))
        }
        "eth_blockNumber" => {
            require_no_params(params)?;
            let recent = state
                .store
                .recent_canonical_bounds(state.config.chain_id)
                .await
                .map_err(|error| RpcError::store(&error))?;
            let cursor = state
                .store
                .processor_cursor(state.progress.descriptor())
                .await
                .map_err(|error| RpcError::store(&error))?;
            Ok(Value::String(hex_quantity(
                recent.map(BlockRange::end).map_or_else(
                    || cursor.map_or(0, |cursor| cursor.block_number.0),
                    |block| block.0,
                ),
            )))
        }
        "eth_syncing" => {
            require_no_params(params)?;
            if state.config.readiness.live_ready() {
                Ok(Value::Bool(false))
            } else {
                let cursor = state
                    .store
                    .processor_cursor(state.progress.descriptor())
                    .await
                    .map_err(|error| RpcError::store(&error))?;
                let current = cursor.map_or(0, |cursor| cursor.block_number.0);
                Ok(json!({
                    "startingBlock": hex_quantity(processor_start_block(state.progress.descriptor())),
                    "currentBlock": hex_quantity(current),
                    "highestBlock": hex_quantity(current),
                    "leani": {
                        "liveReady": false,
                        "reason": "live_head_not_anchored"
                    }
                }))
            }
        }
        "eth_getBlockByNumber" => eth_get_block_by_number(state, params).await,
        "eth_getBlockByHash" => eth_get_block_by_hash(state, params).await,
        "eth_getBlockTransactionCountByNumber" => {
            eth_get_block_transaction_count(state, params, false).await
        }
        "eth_getBlockTransactionCountByHash" => {
            eth_get_block_transaction_count(state, params, true).await
        }
        "eth_getTransactionByBlockNumberAndIndex" => {
            eth_get_transaction_by_block(state, params, false).await
        }
        "eth_getTransactionByBlockHashAndIndex" => {
            eth_get_transaction_by_block(state, params, true).await
        }
        "eth_getBlockReceipts" => eth_get_block_receipts(state, params).await,
        "eth_getTransactionByHash" => eth_get_transaction_by_hash(state, params).await,
        "eth_getTransactionReceipt" => eth_get_transaction_receipt(state, params).await,
        "eth_config" => eth_config(state, params).await,
        "eth_getLogs" => eth_get_logs(state, params, budget).await,
        "leani_getCapabilities" => {
            require_no_params(params)?;
            Ok(capabilities(state))
        }
        "eth_feeHistory" => Err(RpcError::data_unavailable(method)),
        _ => Err(RpcError::method_not_found()),
    }
}

async fn eth_config(state: &RpcState, params: Option<Value>) -> Result<Value, RpcError> {
    require_no_params(params)?;
    let schedule = state.blob_schedule.as_ref();
    if state.config.chain_id.0 != schedule.chain_id {
        return Err(RpcError::data_unavailable_reason(
            "eth_config_chain_schedule_unavailable",
        ));
    }

    let recent = state
        .store
        .recent_canonical_bounds(state.config.chain_id)
        .await
        .map_err(|error| RpcError::store(&error))?;
    let head_timestamp = if let Some(range) = recent {
        state
            .store
            .recent_frame(state.config.chain_id, range.end())
            .await
            .map_err(|error| RpcError::store(&error))?
            .map(|frame| frame.block.timestamp)
    } else {
        None
    };
    // Forks activate by timestamp, the selection `parameters_at_timestamp`
    // prices blob gas with. Without the head's, a block number or the newest
    // fork would be a guess, wrong across an activation.
    let timestamp = head_timestamp.ok_or_else(|| {
        RpcError::data_unavailable_reason("eth_config_head_timestamp_unavailable")
    })?;
    let current = schedule
        .fork_at_timestamp(timestamp)
        .ok_or_else(|| RpcError::data_unavailable_reason("eth_config_head_predates_schedule"))?;
    let current_index = schedule
        .forks
        .iter()
        .position(|fork| fork.name == current.name)
        .ok_or_else(|| RpcError::data_unavailable_reason("eth_config_schedule_invalid"))?;
    let next = schedule.forks.get(current_index.saturating_add(1));
    Ok(json!({
        "current": eip7910_fork_config(schedule.chain_id, current, current_index),
        "next": next.map(|fork| {
            eip7910_fork_config(schedule.chain_id, fork, current_index.saturating_add(1))
        }),
        // EIP-7910 defines `last` as the final known scheduled configuration,
        // and omits it when there is no future fork.
        "last": next.and_then(|_| {
            schedule.forks.last().map(|fork| {
                eip7910_fork_config(
                    schedule.chain_id,
                    fork,
                    schedule.forks.len().saturating_sub(1),
                )
            })
        })
    }))
}

fn eip7910_fork_config(chain_id: u64, fork: &BlobFork, fork_index: usize) -> Value {
    json!({
        "activationTime": fork.activation_timestamp,
        "blobSchedule": {
            "baseFeeUpdateFraction": fork.base_fee_update_fraction,
            "max": fork.max_blobs_per_block,
            "target": fork.target_blobs_per_block
        },
        "chainId": hex_quantity(chain_id),
        "forkId": format!("0x{}", fork.fork_id),
        "precompiles": eip7910_precompiles(fork_index),
        "systemContracts": eip7910_system_contracts(fork_index)
    })
}

fn eip7910_precompiles(fork_index: usize) -> BTreeMap<&'static str, &'static str> {
    const CANCUN: [(&str, &str); 10] = [
        ("ECREC", "0x0000000000000000000000000000000000000001"),
        ("SHA256", "0x0000000000000000000000000000000000000002"),
        ("RIPEMD160", "0x0000000000000000000000000000000000000003"),
        ("ID", "0x0000000000000000000000000000000000000004"),
        ("MODEXP", "0x0000000000000000000000000000000000000005"),
        ("BN254_ADD", "0x0000000000000000000000000000000000000006"),
        ("BN254_MUL", "0x0000000000000000000000000000000000000007"),
        (
            "BN254_PAIRING",
            "0x0000000000000000000000000000000000000008",
        ),
        ("BLAKE2F", "0x0000000000000000000000000000000000000009"),
        (
            "KZG_POINT_EVALUATION",
            "0x000000000000000000000000000000000000000a",
        ),
    ];
    const PRAGUE: [(&str, &str); 7] = [
        ("BLS12_G1ADD", "0x000000000000000000000000000000000000000b"),
        ("BLS12_G1MSM", "0x000000000000000000000000000000000000000c"),
        ("BLS12_G2ADD", "0x000000000000000000000000000000000000000d"),
        ("BLS12_G2MSM", "0x000000000000000000000000000000000000000e"),
        (
            "BLS12_PAIRING_CHECK",
            "0x000000000000000000000000000000000000000f",
        ),
        (
            "BLS12_MAP_FP_TO_G1",
            "0x0000000000000000000000000000000000000010",
        ),
        (
            "BLS12_MAP_FP2_TO_G2",
            "0x0000000000000000000000000000000000000011",
        ),
    ];
    let mut precompiles = CANCUN.into_iter().collect::<BTreeMap<_, _>>();
    if fork_index >= 1 {
        precompiles.extend(PRAGUE);
    }
    if fork_index >= 2 {
        precompiles.insert("P256VERIFY", "0x0000000000000000000000000000000000000100");
    }
    precompiles
}

fn eip7910_system_contracts(fork_index: usize) -> BTreeMap<&'static str, &'static str> {
    let mut contracts = BTreeMap::from([(
        "BEACON_ROOTS_ADDRESS",
        "0x000f3df6d732807ef1319fb7b8bb8522d0beac02",
    )]);
    if fork_index >= 1 {
        contracts.extend([
            (
                "CONSOLIDATION_REQUEST_PREDEPLOY_ADDRESS",
                "0x0000bbddc7ce488642fb579f8b00f3a590007251",
            ),
            (
                "DEPOSIT_CONTRACT_ADDRESS",
                "0x00000000219ab540356cbb839cbe05303d7705fa",
            ),
            (
                "HISTORY_STORAGE_ADDRESS",
                "0x0000f90827f1c53a10cb7a02335b175320002935",
            ),
            (
                "WITHDRAWAL_REQUEST_PREDEPLOY_ADDRESS",
                "0x00000961ef480eb55e80d19ad83579a64c007002",
            ),
        ]);
    }
    contracts
}

#[allow(clippy::too_many_lines)]
fn capabilities(state: &RpcState) -> Value {
    let mut methods = Map::new();
    for method in [
        "web3_clientVersion",
        "net_version",
        "eth_chainId",
        "eth_blockNumber",
        "eth_syncing",
        "eth_config",
        "leani_getCapabilities",
    ] {
        methods.insert(
            method.to_owned(),
            json!({
                "supported": true,
                "coverage": "node_metadata"
            }),
        );
    }
    for (method, required, on_demand_by_number) in [
        ("eth_getBlockByNumber", rpc_block_capabilities(), true),
        ("eth_getBlockByHash", rpc_block_capabilities(), false),
        ("eth_getBlockReceipts", rpc_receipt_capabilities(), true),
        (
            "eth_getTransactionReceipt",
            rpc_receipt_capabilities(),
            false,
        ),
        (
            "eth_getTransactionByHash",
            CapabilitySet::of(Capability::Header).with(Capability::Transactions),
            false,
        ),
        (
            "eth_getBlockTransactionCountByNumber",
            CapabilitySet::of(Capability::Transactions),
            true,
        ),
        (
            "eth_getBlockTransactionCountByHash",
            CapabilitySet::of(Capability::Transactions),
            false,
        ),
        (
            "eth_getTransactionByBlockNumberAndIndex",
            CapabilitySet::of(Capability::Header).with(Capability::Transactions),
            true,
        ),
        (
            "eth_getTransactionByBlockHashAndIndex",
            CapabilitySet::of(Capability::Header).with(Capability::Transactions),
            false,
        ),
    ] {
        let on_demand = state.config.history.as_ref().is_some_and(|history| {
            if on_demand_by_number {
                history.supports(state.config.chain_id, required)
            } else if matches!(
                method,
                "eth_getTransactionByHash" | "eth_getTransactionReceipt"
            ) {
                history.supports_transaction_lookup(state.config.chain_id, required)
            } else {
                history.supports_block_hash_lookup(state.config.chain_id, required)
            }
        });
        methods.insert(
            method.to_owned(),
            json!({
                "supported": true,
                "coverage": if on_demand {
                    "retained_recent_and_compatible_history_lookup"
                } else {
                    "retained_recent_window"
                }
            }),
        );
    }
    let on_demand_logs = state.config.history.as_ref().is_some_and(|history| {
        history.supports(state.config.chain_id, CapabilitySet::of(Capability::Logs))
    });
    methods.insert(
        "eth_getLogs".to_owned(),
        json!({
            "supported": true,
            "coverage": if on_demand_logs {
                "retained_recent_and_on_demand"
            } else {
                "retained_recent_window"
            },
            "maximumRange": state.config.max_log_range
        }),
    );
    for method in ["eth_subscribe(newHeads)", "eth_subscribe(logs)"] {
        methods.insert(
            method.to_owned(),
            json!({
                "supported": state.config.websocket_enabled,
                "coverage": "post_commit_live_stream",
                "reorgAware": true
            }),
        );
    }
    for method in ["eth_feeHistory", "eth_call", "eth_getBalance"] {
        methods.insert(
            method.to_owned(),
            json!({
                "supported": false,
                "reason": if method == "eth_call" || method == "eth_getBalance" {
                    "evm_state_unavailable"
                } else {
                    "raw_execution_material_not_retained"
                }
            }),
        );
    }
    json!({
        "profile": "leani-evm-free-v1",
        "chainId": state.config.chain_id.0,
        "http": true,
        "webSocket": state.config.websocket_enabled,
        "liveReady": state.config.readiness.live_ready(),
        "onDemandHistory": {
            "configured": state.config.history.is_some(),
            "exactBlocks": state.config.history.as_ref().is_some_and(|history| {
                history.supports(state.config.chain_id, rpc_block_capabilities())
            }),
            "exactReceipts": state.config.history.as_ref().is_some_and(|history| {
                history.supports(state.config.chain_id, rpc_receipt_capabilities())
            }),
            "blockHashLocators": state.config.history.as_ref().is_some_and(|history| {
                history.supports_block_hash_lookup(
                    state.config.chain_id,
                    rpc_block_capabilities()
                )
            }),
            "transactionHashLocators": state.config.history.as_ref().is_some_and(|history| {
                history.supports_transaction_lookup(
                    state.config.chain_id,
                    rpc_block_capabilities()
                )
            }),
            "exactLogs": on_demand_logs
        },
        "methods": methods
    })
}

async fn eth_get_block_by_number(
    state: &RpcState,
    params: Option<Value>,
) -> Result<Value, RpcError> {
    let (selector, full) = parse_block_lookup(params.as_ref(), false)?;
    let frame = resolve_canonical_frame(state, selector, rpc_block_capabilities()).await?;
    frame
        .map(|frame| rpc_block(&frame, full))
        .transpose()
        .map(|value| value.unwrap_or(Value::Null))
}

async fn eth_get_block_by_hash(state: &RpcState, params: Option<Value>) -> Result<Value, RpcError> {
    let (selector, full) = parse_block_lookup(params.as_ref(), true)?;
    let BlockSelector::Hash(hash) = selector else {
        return Err(RpcError::invalid_params("expected a block hash"));
    };
    let frame = resolve_hash_frame(state, hash, rpc_block_capabilities()).await?;
    frame
        .map(|frame| rpc_block(&frame, full))
        .transpose()
        .map(|value| value.unwrap_or(Value::Null))
}

async fn eth_get_block_receipts(
    state: &RpcState,
    params: Option<Value>,
) -> Result<Value, RpcError> {
    let selector = parse_single_block_selector(params.as_ref())?;
    let frame = resolve_any_frame(state, selector, rpc_receipt_capabilities()).await?;
    let Some(frame) = frame else {
        return Ok(Value::Null);
    };
    serialize_rpc(rpc_receipts(&frame, &state.blob_schedule)?)
}

async fn eth_get_block_transaction_count(
    state: &RpcState,
    params: Option<Value>,
    hash: bool,
) -> Result<Value, RpcError> {
    let selector = parse_single_block_selector_for_method(params.as_ref(), hash)?;
    let frame =
        resolve_any_frame(state, selector, CapabilitySet::of(Capability::Transactions)).await?;
    let Some(frame) = frame else {
        return Ok(Value::Null);
    };
    let transactions = complete(&frame.transactions, "complete_transactions_not_retained")?;
    Ok(Value::String(hex_quantity(
        u64::try_from(transactions.len()).unwrap_or(u64::MAX),
    )))
}

async fn eth_get_transaction_by_block(
    state: &RpcState,
    params: Option<Value>,
    hash: bool,
) -> Result<Value, RpcError> {
    let values = params
        .as_ref()
        .and_then(Value::as_array)
        .ok_or_else(|| RpcError::invalid_params("transaction lookup expects block and index"))?;
    if values.len() != 2 {
        return Err(RpcError::invalid_params(
            "transaction lookup expects block and index",
        ));
    }
    let selector = if hash {
        BlockSelector::Hash(parse_block_hash_value(&values[0])?)
    } else {
        parse_block_selector(&values[0])?
    };
    let index = values[1]
        .as_str()
        .ok_or_else(|| RpcError::invalid_params("transaction index must be a quantity"))
        .and_then(parse_hex_quantity)
        .and_then(|index| {
            usize::try_from(index)
                .map_err(|_| RpcError::invalid_params("transaction index exceeds this platform"))
        })?;
    let Some(frame) = resolve_any_frame(
        state,
        selector,
        CapabilitySet::of(Capability::Header).with(Capability::Transactions),
    )
    .await?
    else {
        return Ok(Value::Null);
    };
    let transactions = complete(&frame.transactions, "complete_transactions_not_retained")?;
    if index >= transactions.len() {
        return Ok(Value::Null);
    }
    serialize_rpc(rpc_transaction(&frame, index)?)
}

async fn eth_get_transaction_by_hash(
    state: &RpcState,
    params: Option<Value>,
) -> Result<Value, RpcError> {
    let hash = parse_single_transaction_hash(params.as_ref())?;
    let Some((frame, index)) = find_transaction(state, hash, rpc_block_capabilities()).await?
    else {
        return Ok(Value::Null);
    };
    serialize_rpc(rpc_transaction(&frame, index)?)
}

async fn eth_get_transaction_receipt(
    state: &RpcState,
    params: Option<Value>,
) -> Result<Value, RpcError> {
    let hash = parse_single_transaction_hash(params.as_ref())?;
    let Some((frame, index)) = find_transaction(state, hash, rpc_receipt_capabilities()).await?
    else {
        return Ok(Value::Null);
    };
    let receipts = rpc_receipts(&frame, &state.blob_schedule)?;
    receipts
        .into_iter()
        .nth(index)
        .ok_or_else(|| RpcError::data_unavailable_reason("transaction_receipt_not_retained"))
        .and_then(serialize_rpc)
}

#[derive(Clone, Copy, Debug)]
enum BlockSelector {
    Latest,
    /// The verified finalized head, which `finalized` and `safe` name.
    Finalized,
    Number(BlockNumber),
    Hash(BlockHash),
}

fn parse_block_lookup(
    params: Option<&Value>,
    hash: bool,
) -> Result<(BlockSelector, bool), RpcError> {
    let values = params
        .and_then(Value::as_array)
        .ok_or_else(|| RpcError::invalid_params("block lookup expects two parameters"))?;
    if values.len() != 2 {
        return Err(RpcError::invalid_params(
            "block lookup expects two parameters",
        ));
    }
    let selector = if hash {
        BlockSelector::Hash(parse_block_hash_value(&values[0])?)
    } else {
        parse_block_selector(&values[0])?
    };
    let full = values[1]
        .as_bool()
        .ok_or_else(|| RpcError::invalid_params("full transactions must be a boolean"))?;
    Ok((selector, full))
}

fn parse_single_block_selector(params: Option<&Value>) -> Result<BlockSelector, RpcError> {
    let values = params
        .and_then(Value::as_array)
        .ok_or_else(|| RpcError::invalid_params("block lookup expects one parameter"))?;
    if values.len() != 1 {
        return Err(RpcError::invalid_params(
            "block lookup expects one parameter",
        ));
    }
    let value = &values[0];
    if value.as_str().is_some_and(|value| value.len() == 66) {
        parse_block_hash_value(value).map(BlockSelector::Hash)
    } else {
        parse_block_selector(value)
    }
}

fn parse_single_block_selector_for_method(
    params: Option<&Value>,
    hash: bool,
) -> Result<BlockSelector, RpcError> {
    let values = params
        .and_then(Value::as_array)
        .ok_or_else(|| RpcError::invalid_params("block lookup expects one parameter"))?;
    if values.len() != 1 {
        return Err(RpcError::invalid_params(
            "block lookup expects one parameter",
        ));
    }
    if hash {
        parse_block_hash_value(&values[0]).map(BlockSelector::Hash)
    } else {
        parse_block_selector(&values[0])
    }
}

fn parse_block_selector(value: &Value) -> Result<BlockSelector, RpcError> {
    let value = value
        .as_str()
        .ok_or_else(|| RpcError::invalid_params("block selector must be a string"))?;
    match value {
        "latest" => Ok(BlockSelector::Latest),
        // Leani does not track the justified checkpoint other clients call
        // safe; the finalized head is never newer, so it is safe too.
        "finalized" | "safe" => Ok(BlockSelector::Finalized),
        "earliest" => Ok(BlockSelector::Number(BlockNumber(0))),
        // Leani builds no pending block.
        "pending" => Err(RpcError::data_unavailable_reason(
            "pending_block_unavailable",
        )),
        value => parse_hex_quantity(value)
            .map(BlockNumber)
            .map(BlockSelector::Number),
    }
}

/// Resolve a canonical block by number or tag. `None`, which callers answer
/// with `null`, only for a block above the head: a block at or below it
/// exists, so one the node cannot serve is an error, never `null`.
async fn resolve_canonical_frame(
    state: &RpcState,
    selector: BlockSelector,
    required: CapabilitySet,
) -> Result<Option<BlockFrame>, RpcError> {
    let (number, on_demand) = match selector {
        BlockSelector::Latest => (
            state
                .store
                .recent_canonical_bounds(state.config.chain_id)
                .await
                .map_err(|error| RpcError::store(&error))?
                .map(BlockRange::end),
            false,
        ),
        BlockSelector::Finalized => (Some(finalized_head(state).await?), true),
        BlockSelector::Number(number) => (Some(number), true),
        BlockSelector::Hash(_) => {
            return Err(RpcError::invalid_params(
                "hash selector is invalid for a canonical number lookup",
            ));
        }
    };
    let Some(number) = number else {
        return Err(RpcError::data_unavailable_reason("block_not_retained"));
    };
    let recent = state
        .store
        .recent_frame(state.config.chain_id, number)
        .await
        .map_err(|error| RpcError::store(&error))?;
    if recent.is_some() {
        return Ok(recent);
    }
    let unavailable = match &state.config.history {
        Some(history) if on_demand => match history
            .fetch(
                state.config.chain_id,
                BlockRange::single(number),
                required,
                FilterSet::default(),
            )
            .await
        {
            Ok(mut frames) => return Ok(frames.pop()),
            // No source covers the block, which may not exist yet.
            Err(HistoricalRpcError::Unavailable) => {
                RpcError::history(HistoricalRpcError::Unavailable)
            }
            Err(error) => return Err(RpcError::history(error)),
        },
        _ => RpcError::data_unavailable_reason("block_not_retained"),
    };
    if above_known_head(state, number).await? {
        Ok(None)
    } else {
        Err(unavailable)
    }
}

/// The verified finalized head: the newest canonical block that verified
/// finality has reached.
async fn finalized_head(state: &RpcState) -> Result<BlockNumber, RpcError> {
    state
        .store
        .finalized_canonical_head(state.config.chain_id)
        .await
        .map_err(|error| RpcError::store(&error))?
        .map(|head| head.number)
        .ok_or_else(|| RpcError::data_unavailable_reason("finalized_block_unavailable"))
}

/// Whether `number` is above every block this node knows to exist: its
/// canonical head, and the block its progress processor has reached.
async fn above_known_head(state: &RpcState, number: BlockNumber) -> Result<bool, RpcError> {
    let tip = state
        .store
        .canonical_tip(state.config.chain_id)
        .await
        .map_err(|error| RpcError::store(&error))?
        .map(|tip| tip.number);
    let cursor = state
        .store
        .processor_cursor(state.progress.descriptor())
        .await
        .map_err(|error| RpcError::store(&error))?
        .map(|cursor| cursor.block_number);
    Ok(tip.max(cursor).is_some_and(|head| number > head))
}

async fn resolve_any_frame(
    state: &RpcState,
    selector: BlockSelector,
    required: CapabilitySet,
) -> Result<Option<BlockFrame>, RpcError> {
    match selector {
        BlockSelector::Hash(hash) => resolve_hash_frame(state, hash, required).await,
        selector => resolve_canonical_frame(state, selector, required).await,
    }
}

async fn resolve_hash_frame(
    state: &RpcState,
    hash: BlockHash,
    required: CapabilitySet,
) -> Result<Option<BlockFrame>, RpcError> {
    let recent = state
        .store
        .recent_frame_by_hash(state.config.chain_id, hash)
        .await
        .map_err(|error| RpcError::store(&error))?;
    if recent.is_some() {
        return Ok(recent);
    }
    // Without a hash lookup the hash may name any block, one at or below the
    // head included; only a lookup that does not know it answers `null`.
    let Some(history) = state
        .config
        .history
        .as_ref()
        .filter(|history| history.supports_block_hash_lookup(state.config.chain_id, required))
    else {
        return Err(RpcError::data_unavailable_reason("block_not_retained"));
    };
    history
        .block_by_hash(state.config.chain_id, hash, required)
        .await
        .map_err(RpcError::history)
}

const fn rpc_block_capabilities() -> CapabilitySet {
    CapabilitySet::of(Capability::Header).with(Capability::Transactions)
}

const fn rpc_receipt_capabilities() -> CapabilitySet {
    CapabilitySet::of(Capability::Header)
        .with(Capability::Transactions)
        .with(Capability::Receipts)
}

fn rpc_block(frame: &BlockFrame, full: bool) -> Result<Value, RpcError> {
    let header = decode_header(frame)?;
    if header.ommers_hash != EMPTY_OMMER_ROOT_HASH {
        return Err(RpcError::data_unavailable_reason(
            "ommer_bodies_not_retained",
        ));
    }
    let transactions = complete(&frame.transactions, "complete_transactions_not_retained")?;
    let block_transactions = if full {
        BlockTransactions::Full(
            (0..transactions.len())
                .map(|index| rpc_transaction(frame, index))
                .collect::<Result<Vec<_>, _>>()?,
        )
    } else {
        BlockTransactions::Hashes(
            transactions
                .iter()
                .map(|transaction| B256::from(*transaction.hash.as_array()))
                .collect(),
        )
    };
    let withdrawals = if header.withdrawals_root.is_some() {
        let withdrawals = complete(&frame.withdrawals, "complete_withdrawals_not_retained")?;
        Some(Withdrawals::new(
            withdrawals
                .iter()
                .map(|withdrawal| Withdrawal {
                    index: withdrawal.index,
                    validator_index: withdrawal.validator_index,
                    address: withdrawal.address.into(),
                    amount: withdrawal.amount_gwei,
                })
                .collect(),
        ))
    } else {
        None
    };
    let rpc_header = rpc_header_from_consensus(frame, header)?;
    serialize_rpc(RpcBlock::new(rpc_header, block_transactions).with_withdrawals(withdrawals))
}

fn rpc_header(frame: &BlockFrame) -> Result<RpcHeader, RpcError> {
    let header = decode_header(frame)?;
    rpc_header_from_consensus(frame, header)
}

fn rpc_header_from_consensus(
    frame: &BlockFrame,
    header: ConsensusHeader,
) -> Result<RpcHeader, RpcError> {
    let normalized_header = complete(&frame.header, "complete_header_not_retained")?;
    Ok(RpcHeader::from_consensus(
        Sealed::new_unchecked(header, B256::from(*frame.block.hash.as_array())),
        None,
        normalized_header.size_bytes.map(U256::from),
    ))
}

fn decode_header(frame: &BlockFrame) -> Result<ConsensusHeader, RpcError> {
    let normalized = complete(&frame.header, "complete_header_not_retained")?;
    let encoded = normalized
        .rlp
        .as_deref()
        .ok_or_else(|| RpcError::data_unavailable_reason("canonical_header_rlp_not_retained"))?;
    let mut input = encoded;
    let header = ConsensusHeader::decode(&mut input)
        .map_err(|_| RpcError::data_unavailable_reason("canonical_header_rlp_is_invalid"))?;
    if !input.is_empty()
        || header.hash_slow() != B256::from(*frame.block.hash.as_array())
        || header.number != frame.block.number.0
        || header.parent_hash != B256::from(*frame.block.parent_hash.as_array())
        || header.timestamp != frame.block.timestamp
    {
        return Err(RpcError::data_unavailable_reason(
            "canonical_header_rlp_is_inconsistent",
        ));
    }
    Ok(header)
}

fn rpc_transaction(frame: &BlockFrame, index: usize) -> Result<RpcTransaction, RpcError> {
    let transactions = complete(&frame.transactions, "complete_transactions_not_retained")?;
    let transaction = transactions
        .get(index)
        .ok_or_else(|| RpcError::data_unavailable_reason("transaction_index_not_retained"))?;
    let encoded = transaction
        .encoded
        .as_deref()
        .ok_or_else(|| RpcError::data_unavailable_reason("canonical_transaction_not_retained"))?;
    let decoded = TxEnvelope::decode_2718_exact(encoded)
        .map_err(|_| RpcError::data_unavailable_reason("canonical_transaction_is_invalid"))?;
    if *decoded.tx_hash() != B256::from(*transaction.hash.as_array()) {
        return Err(RpcError::data_unavailable_reason(
            "canonical_transaction_hash_mismatch",
        ));
    }
    let signer = decoded
        .recover_signer()
        .map_err(|_| RpcError::data_unavailable_reason("transaction_signer_recovery_failed"))?;
    let recovered = Recovered::new_unchecked(decoded, signer);
    if transaction
        .from
        .is_some_and(|expected| alloy_primitives::Address::from(expected) != recovered.signer())
    {
        return Err(RpcError::data_unavailable_reason(
            "transaction_signer_mismatch",
        ));
    }
    let header = complete(&frame.header, "complete_header_not_retained")?;
    Ok(RpcTransaction::from_transaction(
        recovered,
        TransactionInfo {
            hash: Some(B256::from(*transaction.hash.as_array())),
            index: Some(u64::try_from(index).unwrap_or(u64::MAX)),
            block_hash: Some(B256::from(*frame.block.hash.as_array())),
            block_number: Some(frame.block.number.0),
            base_fee: header
                .base_fee_per_gas
                .map(quantity_to_u256)
                .map(u256_to_u64)
                .transpose()?,
            block_timestamp: Some(frame.block.timestamp),
        },
    ))
}

fn rpc_receipts(
    frame: &BlockFrame,
    schedule: &BlobSchedule,
) -> Result<Vec<RpcTransactionReceipt>, RpcError> {
    let transactions = complete(&frame.transactions, "complete_transactions_not_retained")?;
    let receipts = complete(&frame.receipts, "complete_receipts_not_retained")?;
    if transactions.len() != receipts.len() {
        return Err(RpcError::data_unavailable_reason(
            "transaction_receipt_count_mismatch",
        ));
    }
    let header = complete(&frame.header, "complete_header_not_retained")?;
    // Only blob receipts carry a price, so a block without a priced fork
    // fails closed only when it has one.
    let blob_gas_price = receipts
        .iter()
        .any(|receipt| receipt.blob_gas_used.is_some())
        .then(|| protocol_blob_gas_price(schedule, frame, header.excess_blob_gas))
        .transpose()?;
    let mut next_log_index = 0_u64;
    transactions
        .iter()
        .zip(receipts)
        .enumerate()
        .map(|(index, (transaction, receipt))| {
            let encoded = receipt.encoded.as_deref().ok_or_else(|| {
                RpcError::data_unavailable_reason("canonical_receipt_not_retained")
            })?;
            let decoded = ConsensusReceiptEnvelope::decode_2718_exact(encoded)
                .map_err(|_| RpcError::data_unavailable_reason("canonical_receipt_is_invalid"))?;
            let tx_index = u64::try_from(index).unwrap_or(u64::MAX);
            let rpc_inner = decoded.map_logs(|log| {
                let rpc_log = RpcLog {
                    inner: log,
                    block_hash: Some(B256::from(*frame.block.hash.as_array())),
                    block_number: Some(frame.block.number.0),
                    block_timestamp: Some(frame.block.timestamp),
                    transaction_hash: Some(B256::from(*transaction.hash.as_array())),
                    transaction_index: Some(tx_index),
                    log_index: Some(next_log_index),
                    removed: false,
                };
                next_log_index = next_log_index.saturating_add(1);
                rpc_log
            });
            let from = transaction
                .from
                .map(alloy_primitives::Address::from)
                .ok_or_else(|| RpcError::data_unavailable_reason("transaction_sender_missing"))?;
            let to = transaction.to.map(alloy_primitives::Address::from);
            let mut rpc = RpcTransactionReceipt {
                inner: rpc_inner,
                transaction_hash: B256::from(*transaction.hash.as_array()),
                transaction_index: Some(tx_index),
                block_hash: Some(B256::from(*frame.block.hash.as_array())),
                block_number: Some(frame.block.number.0),
                gas_used: receipt
                    .gas_used
                    .ok_or_else(|| RpcError::data_unavailable_reason("receipt_gas_used_missing"))?,
                effective_gas_price: receipt
                    .effective_gas_price
                    .map(quantity_to_u256)
                    .map(u256_to_u128)
                    .transpose()?
                    .ok_or_else(|| {
                        RpcError::data_unavailable_reason("receipt_effective_gas_price_missing")
                    })?,
                blob_gas_used: receipt.blob_gas_used,
                blob_gas_price: receipt.blob_gas_used.and(blob_gas_price),
                from,
                to,
                contract_address: None,
            };
            let nonce = transaction
                .nonce
                .ok_or_else(|| RpcError::data_unavailable_reason("transaction_nonce_missing"))?;
            rpc.contract_address = rpc.calculate_create_address(nonce);
            Ok(rpc)
        })
        .collect()
}

/// The protocol blob gas price of `frame`'s block: the blob base fee under the
/// fork active at its header timestamp, from the schedule and fee function the
/// blobs processor uses.
fn protocol_blob_gas_price(
    schedule: &BlobSchedule,
    frame: &BlockFrame,
    excess_blob_gas: Option<u64>,
) -> Result<u128, RpcError> {
    let excess_blob_gas = excess_blob_gas
        .ok_or_else(|| RpcError::data_unavailable_reason("header_excess_blob_gas_missing"))?;
    let parameters = schedule
        .parameters_at_timestamp(frame.block.timestamp)
        .filter(|_| schedule.chain_id == frame.chain_id.0)
        .ok_or_else(|| RpcError::data_unavailable_reason("blob_gas_price_schedule_unavailable"))?;
    // With a validated schedule the fee fails only on a 256-bit overflow.
    get_blob_base_fee(excess_blob_gas, parameters.base_fee_update_fraction)
        .map_err(|_| RpcError::data_unavailable_reason("quantity_exceeds_u128"))
        .and_then(u256_to_u128)
}

async fn find_recent_transaction(
    state: &RpcState,
    hash: TransactionHash,
) -> Result<Option<(BlockFrame, usize)>, RpcError> {
    let Some(location) = state
        .store
        .recent_transaction_location(state.config.chain_id, hash)
        .await
        .map_err(|error| RpcError::store(&error))?
    else {
        return Ok(None);
    };
    let Some(frame) = state
        .store
        .recent_frame_by_hash(state.config.chain_id, location.block_hash)
        .await
        .map_err(|error| RpcError::store(&error))?
    else {
        return Err(RpcError::data_unavailable_reason(
            "transaction_locator_frame_missing",
        ));
    };
    let index = usize::try_from(location.transaction_index)
        .map_err(|_| RpcError::data_unavailable_reason("transaction_index_invalid"))?;
    let transactions = complete(&frame.transactions, "complete_transactions_not_retained")?;
    if transactions
        .get(index)
        .is_none_or(|transaction| transaction.hash != hash)
    {
        return Err(RpcError::data_unavailable_reason(
            "transaction_locator_conflicts_with_frame",
        ));
    }
    Ok(Some((frame, index)))
}

async fn find_transaction(
    state: &RpcState,
    hash: TransactionHash,
    required: CapabilitySet,
) -> Result<Option<(BlockFrame, usize)>, RpcError> {
    if let Some(found) = find_recent_transaction(state, hash).await? {
        return Ok(Some(found));
    }
    let Some(history) = &state.config.history else {
        return Err(RpcError::data_unavailable_reason(
            "transaction_hash_locator_unavailable",
        ));
    };
    if !history.supports_transaction_lookup(state.config.chain_id, required) {
        return Err(RpcError::data_unavailable_reason(
            "transaction_hash_locator_unavailable",
        ));
    }
    let Some(located) = history
        .transaction_by_hash(state.config.chain_id, hash, required)
        .await
        .map_err(RpcError::history)?
    else {
        return Ok(None);
    };
    let index = usize::try_from(located.transaction_index)
        .map_err(|_| RpcError::data_unavailable_reason("transaction_index_invalid"))?;
    Ok(Some((located.frame, index)))
}

fn parse_single_transaction_hash(params: Option<&Value>) -> Result<TransactionHash, RpcError> {
    let values = params
        .and_then(Value::as_array)
        .ok_or_else(|| RpcError::invalid_params("transaction lookup expects one hash"))?;
    if values.len() != 1 {
        return Err(RpcError::invalid_params(
            "transaction lookup expects one hash",
        ));
    }
    values[0]
        .as_str()
        .ok_or_else(|| RpcError::invalid_params("transaction hash must be a hex string"))
        .and_then(parse_fixed_hex::<32>)
        .map(TransactionHash::new)
}

fn complete<'a, T>(material: &'a Material<T>, reason: &'static str) -> Result<&'a T, RpcError> {
    let Material::Complete(value) = material else {
        return Err(RpcError::data_unavailable_reason(reason));
    };
    Ok(value)
}

fn serialize_rpc(value: impl serde::Serialize) -> Result<Value, RpcError> {
    serde_json::to_value(value)
        .map_err(|_| RpcError::data_unavailable_reason("rpc_serialization_failed"))
}

fn u256_to_u64(value: U256) -> Result<u64, RpcError> {
    value
        .try_into()
        .map_err(|_| RpcError::data_unavailable_reason("quantity_exceeds_u64"))
}

fn quantity_to_u256(value: leani_primitives::Quantity) -> U256 {
    U256::from_be_bytes(*value.as_array())
}

fn u256_to_u128(value: U256) -> Result<u128, RpcError> {
    value
        .try_into()
        .map_err(|_| RpcError::data_unavailable_reason("quantity_exceeds_u128"))
}

/// `budget` is the number of response bytes still free: the encoded log
/// array must fit in it, less the response envelope, which the response
/// limit checks afterwards.
async fn eth_get_logs(
    state: &RpcState,
    params: Option<Value>,
    budget: usize,
) -> Result<Value, RpcError> {
    let filter = parse_log_filter(params.as_ref(), &state.config)?;
    let recent_bounds = state
        .store
        .recent_canonical_bounds(state.config.chain_id)
        .await
        .map_err(|error| RpcError::store(&error))?;
    let (from, to) = if let Some(hash) = filter.block_hash {
        let block = state
            .store
            .canonical_block_by_hash(state.config.chain_id, hash)
            .await
            .map_err(|error| RpcError::store(&error))?
            .ok_or_else(|| RpcError::data_unavailable_reason("block_hash_not_retained"))?;
        (block.number, block.number)
    } else {
        let latest = recent_bounds.map(BlockRange::end);
        (
            log_range_bound(state, filter.from, latest).await?,
            log_range_bound(state, filter.to, latest).await?,
        )
    };
    if from > to {
        return Err(RpcError::invalid_params(
            "fromBlock must not exceed toBlock",
        ));
    }
    let length = to.0.saturating_sub(from.0).saturating_add(1);
    if length > state.config.max_log_range {
        return Err(RpcError::data_unavailable_reason(
            "requested_range_exceeds_rpc_limit",
        ));
    }
    let frames = resolve_log_frames(
        state,
        BlockRange::new(from, to)
            .map_err(|_| RpcError::invalid_params("fromBlock must not exceed toBlock"))?,
        &filter,
        recent_bounds.map(BlockRange::start),
        recent_bounds.map(BlockRange::end),
    )
    .await?;
    // The hash was resolved to a number before the block was read; a reorg in
    // between, or history holding another block there, must not answer with
    // another block's logs.
    if filter
        .block_hash
        .is_some_and(|hash| frames.iter().any(|frame| frame.block.hash != hash))
    {
        return Err(RpcError::data_unavailable_reason("block_hash_not_retained"));
    }
    let max_results = state.config.max_log_results;
    let mut output = Vec::new();
    // The encoded array's brackets; each log adds its length and a comma.
    let mut output_bytes = 2_usize;
    for frame in frames {
        let Material::Complete(logs) = &frame.logs else {
            return Err(RpcError::data_unavailable_reason(
                "complete_logs_not_retained",
            ));
        };
        for log in logs {
            if !filter.matches(log) {
                continue;
            }
            if output.len() == max_results {
                return Err(RpcError::too_many_logs(
                    max_results,
                    from,
                    frame.block.number,
                ));
            }
            // Stop building a result the response limit would refuse anyway.
            output_bytes = output_bytes
                .saturating_add(usize::from(!output.is_empty()))
                .saturating_add(rpc_log_json_len(&frame, log, false));
            if output_bytes > budget {
                return Err(RpcError::over_response_budget(
                    state.config.max_response_bytes,
                ));
            }
            output.push(rpc_log_value(&frame, log, false)?);
        }
    }
    Ok(Value::Array(output))
}

/// The block a `fromBlock` or `toBlock` bound names; an absent bound is
/// `latest`.
async fn log_range_bound(
    state: &RpcState,
    bound: Option<BlockSelector>,
    latest: Option<BlockNumber>,
) -> Result<BlockNumber, RpcError> {
    match bound.unwrap_or(BlockSelector::Latest) {
        BlockSelector::Latest => latest.ok_or_else(|| {
            RpcError::data_unavailable_reason("latest_block_unavailable_for_open_log_range")
        }),
        BlockSelector::Finalized => finalized_head(state).await,
        BlockSelector::Number(number) => Ok(number),
        BlockSelector::Hash(_) => Err(RpcError::invalid_params(
            "fromBlock and toBlock must be block numbers or tags",
        )),
    }
}

async fn resolve_log_frames(
    state: &RpcState,
    range: BlockRange,
    filter: &ParsedLogFilter,
    recent_earliest: Option<BlockNumber>,
    recent_latest: Option<BlockNumber>,
) -> Result<Vec<BlockFrame>, RpcError> {
    if let (Some(earliest), Some(latest)) = (recent_earliest, recent_latest)
        && range.start() >= earliest
        && range.end() <= latest
    {
        return read_recent_frames(state, range).await;
    }
    if recent_latest.is_some_and(|latest| range.end() > latest) {
        return Err(RpcError::data_unavailable_reason(
            "requested_log_range_exceeds_known_head",
        ));
    }
    let Some(history) = &state.config.history else {
        return Err(RpcError::data_unavailable_reason(
            "requested_range_outside_recent_window",
        ));
    };
    let history_end = recent_earliest.map_or(range.end(), |earliest| {
        BlockNumber(range.end().0.min(earliest.0.saturating_sub(1)))
    });
    let mut frames = if range.start() <= history_end {
        let historical_range = BlockRange::new(range.start(), history_end)
            .map_err(|_| RpcError::data_unavailable_reason("historical_log_range_invalid"))?;
        history
            .fetch(
                state.config.chain_id,
                historical_range,
                CapabilitySet::of(Capability::Logs),
                historical_log_filters(historical_range, filter),
            )
            .await
            .map_err(RpcError::history)?
    } else {
        Vec::new()
    };
    if let (Some(earliest), Some(latest)) = (recent_earliest, recent_latest) {
        let recent_start = BlockNumber(range.start().0.max(earliest.0));
        let recent_end = BlockNumber(range.end().0.min(latest.0));
        if recent_start <= recent_end {
            let recent = read_recent_frames(
                state,
                BlockRange::new(recent_start, recent_end)
                    .map_err(|_| RpcError::data_unavailable_reason("recent_log_range_invalid"))?,
            )
            .await?;
            if let (Some(prior), Some(next)) = (frames.last(), recent.first())
                && next.block.parent_hash != prior.block.hash
            {
                return Err(RpcError::data_unavailable_reason(
                    "historical_recent_handoff_mismatch",
                ));
            }
            frames.extend(recent);
        }
    }
    if u64::try_from(frames.len()).unwrap_or(u64::MAX) != range.len() {
        return Err(RpcError::data_unavailable_reason(
            "historical_log_range_incomplete",
        ));
    }
    Ok(frames)
}

async fn read_recent_frames(
    state: &RpcState,
    range: BlockRange,
) -> Result<Vec<BlockFrame>, RpcError> {
    let mut frames = Vec::with_capacity(
        usize::try_from(range.len())
            .map_err(|_| RpcError::data_unavailable_reason("log_range_exceeds_platform"))?,
    );
    for number in range.iter() {
        frames.push(
            state
                .store
                .recent_frame(state.config.chain_id, number)
                .await
                .map_err(|error| RpcError::store(&error))?
                .ok_or_else(|| RpcError::data_unavailable_reason("recent_frame_missing"))?,
        );
    }
    Ok(frames)
}

fn historical_log_filters(range: BlockRange, filter: &ParsedLogFilter) -> FilterSet {
    FilterSet {
        scope: FilterScope {
            block_range: Some(range),
            addresses: filter.addresses.clone(),
            topics: filter
                .topics
                .iter()
                .enumerate()
                .filter_map(|(position, alternatives)| {
                    alternatives.as_ref().map(|alternatives| TopicFilter {
                        position: u8::try_from(position).unwrap_or(u8::MAX),
                        alternatives: alternatives.clone(),
                    })
                })
                .collect(),
            transaction_hashes: Vec::new(),
            transaction_types: Vec::new(),
            ..FilterScope::default()
        },
        ..FilterSet::default()
    }
}

fn rpc_log_value(
    frame: &BlockFrame,
    log: &leani_primitives::Log,
    removed: bool,
) -> Result<Value, RpcError> {
    let transaction_hash = log
        .transaction_hash
        .ok_or_else(|| RpcError::data_unavailable_reason("log_transaction_hash_not_retained"))?;
    Ok(json!({
        "address": hex_bytes(log.address.as_array()),
        "topics": log.topics.iter().map(|topic| hex_bytes(topic)).collect::<Vec<_>>(),
        "data": hex_bytes(&log.data),
        "blockNumber": hex_quantity(frame.block.number.0),
        "transactionHash": hex_bytes(transaction_hash.as_array()),
        "transactionIndex": hex_quantity(u64::from(log.transaction_index)),
        "blockHash": hex_bytes(frame.block.hash.as_array()),
        "logIndex": hex_quantity(u64::from(log.log_index)),
        "removed": removed
    }))
}

/// The encoded length of `rpc_log_value(frame, log, removed)`, counted
/// without encoding it: its values are hex strings, a list of them, and a
/// boolean, none of which JSON escapes.
fn rpc_log_json_len(frame: &BlockFrame, log: &leani_primitives::Log, removed: bool) -> usize {
    // `"0x…"`
    let hex_string = |digits: usize| digits.saturating_add(4);
    let quantity = |value: u64| {
        hex_string(value.checked_ilog(16).map_or(1, |exponent| {
            usize::try_from(exponent).map_or(usize::MAX, |exponent| exponent + 1)
        }))
    };
    let topics = log.topics.len();
    let fields = [
        r#""address":"#.len() + hex_string(40),
        r#""topics":[]"#.len() + topics * hex_string(64) + topics.saturating_sub(1),
        r#""data":"#.len() + hex_string(log.data.len().saturating_mul(2)),
        r#""blockNumber":"#.len() + quantity(frame.block.number.0),
        r#""transactionHash":"#.len() + hex_string(64),
        r#""transactionIndex":"#.len() + quantity(u64::from(log.transaction_index)),
        r#""blockHash":"#.len() + hex_string(64),
        r#""logIndex":"#.len() + quantity(u64::from(log.log_index)),
        r#""removed":"#.len() + if removed { "true" } else { "false" }.len(),
    ];
    // Braces, and a comma between fields.
    fields.iter().sum::<usize>() + 2 + fields.len() - 1
}

#[derive(Clone, Debug)]
struct ParsedLogFilter {
    from: Option<BlockSelector>,
    to: Option<BlockSelector>,
    block_hash: Option<BlockHash>,
    addresses: Vec<Address>,
    topics: Vec<Option<Vec<[u8; 32]>>>,
}

impl ParsedLogFilter {
    fn matches(&self, log: &leani_primitives::Log) -> bool {
        if !self.addresses.is_empty() && !self.addresses.contains(&log.address) {
            return false;
        }
        self.topics.iter().enumerate().all(|(position, expected)| {
            expected.as_ref().is_none_or(|alternatives| {
                log.topics
                    .get(position)
                    .is_some_and(|actual| alternatives.contains(actual))
            })
        })
    }
}

fn parse_log_filter(
    params: Option<&Value>,
    config: &RpcConfig,
) -> Result<ParsedLogFilter, RpcError> {
    let values = params
        .and_then(Value::as_array)
        .ok_or_else(|| RpcError::invalid_params("eth_getLogs expects one filter object"))?;
    if values.len() != 1 {
        return Err(RpcError::invalid_params(
            "eth_getLogs expects one filter object",
        ));
    }
    let object = values[0]
        .as_object()
        .ok_or_else(|| RpcError::invalid_params("log filter must be an object"))?;
    let block_hash = object
        .get("blockHash")
        .map(parse_block_hash_value)
        .transpose()?;
    if block_hash.is_some() && (object.contains_key("fromBlock") || object.contains_key("toBlock"))
    {
        return Err(RpcError::invalid_params(
            "blockHash cannot be combined with fromBlock or toBlock",
        ));
    }
    let from = object
        .get("fromBlock")
        .map(parse_block_selector)
        .transpose()?;
    let to = object
        .get("toBlock")
        .map(parse_block_selector)
        .transpose()?;
    let addresses = object
        .get("address")
        .map(|value| parse_addresses(value, config.max_log_addresses))
        .transpose()?
        .unwrap_or_default();
    let topics = object
        .get("topics")
        .map(|value| parse_topics(value, config.max_log_topic_alternatives))
        .transpose()?
        .unwrap_or_default();
    Ok(ParsedLogFilter {
        from,
        to,
        block_hash,
        addresses,
        topics,
    })
}

fn parse_addresses(value: &Value, limit: usize) -> Result<Vec<Address>, RpcError> {
    match value {
        Value::String(value) => parse_fixed_hex::<20>(value)
            .map(Address::new)
            .map(|value| vec![value]),
        Value::Array(values) if values.len() > limit => Err(RpcError::invalid_params_limit(
            "address filter exceeds the address limit",
            limit,
        )),
        Value::Array(values) => values
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .ok_or_else(|| RpcError::invalid_params("address must be a hex string"))
                    .and_then(parse_fixed_hex::<20>)
                    .map(Address::new)
            })
            .collect(),
        _ => Err(RpcError::invalid_params(
            "address must be a hex string or array",
        )),
    }
}

fn parse_topics(value: &Value, limit: usize) -> Result<Vec<Option<Vec<[u8; 32]>>>, RpcError> {
    let values = value
        .as_array()
        .ok_or_else(|| RpcError::invalid_params("topics must be an array"))?;
    if values.len() > 4 {
        return Err(RpcError::invalid_params(
            "topics may contain at most four positions",
        ));
    }
    values
        .iter()
        .map(|value| match value {
            Value::Null => Ok(None),
            // No alternatives is a wildcard too, as in geth and reth.
            Value::Array(alternatives) if alternatives.is_empty() => Ok(None),
            Value::String(topic) => parse_fixed_hex::<32>(topic).map(|topic| Some(vec![topic])),
            Value::Array(alternatives) if alternatives.len() > limit => {
                Err(RpcError::invalid_params_limit(
                    "topic position exceeds the alternatives limit",
                    limit,
                ))
            }
            Value::Array(alternatives) => alternatives
                .iter()
                .map(|topic| {
                    topic
                        .as_str()
                        .ok_or_else(|| RpcError::invalid_params("topic must be a hex string"))
                        .and_then(parse_fixed_hex::<32>)
                })
                .collect::<Result<Vec<_>, _>>()
                .map(Some),
            _ => Err(RpcError::invalid_params(
                "topic position must be null, a hash, or an array",
            )),
        })
        .collect()
}

fn parse_block_hash_value(value: &Value) -> Result<BlockHash, RpcError> {
    value
        .as_str()
        .ok_or_else(|| RpcError::invalid_params("blockHash must be a hex string"))
        .and_then(parse_fixed_hex::<32>)
        .map(BlockHash::new)
}

fn parse_fixed_hex<const N: usize>(value: &str) -> Result<[u8; N], RpcError> {
    let encoded = value
        .strip_prefix("0x")
        .ok_or_else(|| RpcError::invalid_params("hex data must start with 0x"))?;
    let mut output = [0; N];
    hex::decode_to_slice(encoded, &mut output)
        .map_err(|_| RpcError::invalid_params("hex data has the wrong length or encoding"))?;
    Ok(output)
}

fn hex_bytes(value: &[u8]) -> String {
    format!("0x{}", hex::encode(value))
}

fn require_no_params(params: Option<Value>) -> Result<(), RpcError> {
    match params {
        None | Some(Value::Null) => Ok(()),
        Some(Value::Array(values)) if values.is_empty() => Ok(()),
        Some(Value::Object(values)) if values.is_empty() => Ok(()),
        _ => Err(RpcError::invalid_params("this method takes no parameters")),
    }
}

fn parse_hex_quantity(value: &str) -> Result<u64, RpcError> {
    let digits = value
        .strip_prefix("0x")
        .ok_or_else(|| RpcError::invalid_params("quantity must start with 0x"))?;
    // `from_str_radix` alone would also take a leading `+`.
    if digits.is_empty()
        || (digits.len() > 1 && digits.starts_with('0'))
        || !digits.bytes().all(|digit| digit.is_ascii_hexdigit())
    {
        return Err(RpcError::invalid_params(
            "quantity must use canonical hexadecimal encoding",
        ));
    }
    u64::from_str_radix(digits, 16)
        .map_err(|_| RpcError::invalid_params("quantity exceeds 64 bits"))
}

fn hex_quantity(value: u64) -> String {
    format!("0x{value:x}")
}

fn processor_start_block(descriptor: &ProcessorDescriptor) -> u64 {
    match descriptor.start {
        StartPoint::Block(block) => block.0,
        StartPoint::Genesis | StartPoint::ProcessorCheckpoint(_) => 0,
    }
}

#[derive(Debug, Deserialize)]
struct RpcRequest {
    jsonrpc: String,
    /// `None` only when the member is absent, for a notification: a `null`
    /// ID is `Some(Value::Null)` and gets a response.
    #[serde(default, deserialize_with = "present_value")]
    id: Option<Value>,
    method: String,
    #[serde(default)]
    params: Option<Value>,
}

impl RpcRequest {
    /// `call` as a JSON-RPC 2.0 request, or `None` when it is invalid. An ID,
    /// when present, is a string, a number, or `null`.
    fn parse(call: Value) -> Option<Self> {
        // Serde also reads a struct from an array of its fields, such as
        // `["2.0", 1, "eth_chainId"]`, which is not a request object.
        if !call.is_object() {
            return None;
        }
        serde_json::from_value::<Self>(call).ok().filter(|request| {
            request.jsonrpc == JSONRPC_VERSION
                && matches!(
                    request.id,
                    None | Some(Value::Null | Value::String(_) | Value::Number(_))
                )
        })
    }
}

/// Deserialize a member that is present, `null` included, as `Some`.
fn present_value<'de, D>(deserializer: D) -> Result<Option<Value>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Value::deserialize(deserializer).map(Some)
}

#[derive(Debug, serde::Serialize)]
struct RpcResponse {
    jsonrpc: &'static str,
    id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<RpcErrorBody>,
    /// The call stopped because its result would pass the response budget.
    #[serde(skip)]
    over_budget: bool,
}

impl RpcResponse {
    fn success(id: Value, result: Value) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION,
            id,
            result: Some(result),
            error: None,
            over_budget: false,
        }
    }

    fn error(
        id: Value,
        code: i64,
        message: impl Into<Cow<'static, str>>,
        data: Option<Value>,
    ) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION,
            id,
            result: None,
            error: Some(RpcErrorBody {
                code,
                message: message.into(),
                data,
            }),
            over_budget: false,
        }
    }

    /// The response to the call with `id` that ended with `result`.
    fn answer(id: Value, result: Result<Value, RpcError>) -> Self {
        match result {
            Ok(result) => Self::success(id, result),
            Err(error) => Self {
                over_budget: error.over_budget,
                ..Self::error(id, error.code, error.message, error.data)
            },
        }
    }
}

#[derive(Debug, serde::Serialize)]
struct RpcErrorBody {
    code: i64,
    message: Cow<'static, str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<Value>,
}

#[derive(Debug)]
struct RpcError {
    code: i64,
    message: Cow<'static, str>,
    data: Option<Value>,
    /// The call stopped because its result would pass the response budget,
    /// which leaves no room for the calls after it.
    over_budget: bool,
}

impl From<RpcError> for RpcCompatibilityError {
    fn from(error: RpcError) -> Self {
        let reason = error
            .data
            .as_ref()
            .and_then(|data| data.get("reason").or_else(|| data.get("detail")))
            .and_then(Value::as_str)
            .unwrap_or(error.message.as_ref())
            .to_owned();
        Self { reason }
    }
}

impl RpcError {
    fn method_not_found() -> Self {
        Self {
            code: METHOD_NOT_FOUND,
            message: Cow::Borrowed("Method not found"),
            data: None,
            over_budget: false,
        }
    }

    fn invalid_params(detail: &'static str) -> Self {
        Self {
            code: INVALID_PARAMS,
            message: Cow::Borrowed("Invalid params"),
            data: Some(json!({ "detail": detail })),
            over_budget: false,
        }
    }

    fn invalid_params_limit(detail: &'static str, limit: usize) -> Self {
        Self {
            code: INVALID_PARAMS,
            message: Cow::Borrowed("Invalid params"),
            data: Some(json!({ "detail": detail, "limit": limit })),
            over_budget: false,
        }
    }

    fn limit_exceeded(reason: &'static str, limit: usize) -> Self {
        Self {
            code: LIMIT_EXCEEDED,
            message: Cow::Borrowed("Limit exceeded"),
            data: Some(json!({ "reason": reason, "limit": limit })),
            over_budget: false,
        }
    }

    /// The call stopped because its result would pass what is left of the
    /// `limit` of response bytes.
    fn over_response_budget(limit: usize) -> Self {
        Self {
            over_budget: true,
            ..Self::limit_exceeded(RESPONSE_SIZE_LIMIT_EXCEEDED, limit)
        }
    }

    /// More than `limit` logs matched, the one past it in block `at`. The
    /// message names the range before `at`, which stays within the limit,
    /// in the form clients split ranges by.
    fn too_many_logs(limit: usize, from: BlockNumber, at: BlockNumber) -> Self {
        let message = format!("query returned more than {limit} results");
        let (message, data) = if at > from {
            let to = hex_quantity(at.0.saturating_sub(1));
            (
                format!(
                    "{message}. Try with this block range [{}, {to}].",
                    hex_quantity(from.0)
                ),
                json!({ "from": hex_quantity(from.0), "to": to, "limit": limit }),
            )
        } else {
            (message, json!({ "limit": limit }))
        };
        Self {
            code: LIMIT_EXCEEDED,
            message: Cow::Owned(message),
            data: Some(data),
            over_budget: false,
        }
    }

    fn data_unavailable(method: &str) -> Self {
        Self {
            code: DATA_UNAVAILABLE,
            message: Cow::Borrowed("Data unavailable"),
            data: Some(json!({
                "method": method,
                "reason": "raw_execution_material_not_retained",
                "retryable": false
            })),
            over_budget: false,
        }
    }

    fn data_unavailable_reason(reason: &'static str) -> Self {
        Self {
            code: DATA_UNAVAILABLE,
            message: Cow::Borrowed("Data unavailable"),
            data: Some(json!({
                "reason": reason,
                "retryable": false
            })),
            over_budget: false,
        }
    }

    fn store(error: &StoreError) -> Self {
        Self {
            code: INTERNAL_ERROR,
            message: Cow::Borrowed("Internal error"),
            data: Some(json!({ "retryable": true, "detail": error.to_string() })),
            over_budget: false,
        }
    }

    fn history(error: HistoricalRpcError) -> Self {
        let (reason, retryable) = match error {
            HistoricalRpcError::Unavailable => ("no_viable_historical_source", false),
            HistoricalRpcError::Source => ("historical_source_failed", true),
            HistoricalRpcError::Invalid => ("historical_source_returned_invalid_material", false),
            HistoricalRpcError::Budget => ("historical_request_budget_exceeded", false),
            HistoricalRpcError::RangeLimit => ("historical_request_range_exceeded", false),
            HistoricalRpcError::Timeout => ("historical_request_timed_out", true),
        };
        Self {
            code: DATA_UNAVAILABLE,
            message: Cow::Borrowed("Data unavailable"),
            data: Some(json!({
                "reason": reason,
                "retryable": retryable
            })),
            over_budget: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, header},
    };
    use futures::SinkExt;
    use leani_testkit::{
        HistoryStep, ScriptedChunk, ScriptedHistorySource, fixture_frame, fixture_source_descriptor,
    };
    use tokio_tungstenite::{connect_async, tungstenite::Message as ClientMessage};
    use tower::ServiceExt;

    use super::*;

    async fn test_router() -> (Router, tempfile::TempDir) {
        let (router, _store, directory) = test_router_and_store().await;
        (router, directory)
    }

    async fn test_router_and_store() -> (Router, SqliteStore, tempfile::TempDir) {
        let (state, store, directory) = test_state_and_store().await;
        let router = http_router_with_progress(
            state.store,
            state.progress,
            state.blob_schedule,
            state.config,
            state.committed_events,
        );
        (router, store, directory)
    }

    async fn test_state_and_store() -> (RpcState, SqliteStore, tempfile::TempDir) {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteStore::open(leani_store_sqlite::StoreConfig::new(
            directory.path().join("rpc.sqlite"),
        ))
        .await
        .expect("store");
        let (committed_events, _) = broadcast::channel(8);
        let blobs = Arc::new(BlobsProcessor::default());
        let progress: Arc<dyn Processor> = blobs.clone();
        let state = RpcState::new(
            store.clone(),
            progress,
            Arc::new(blobs.schedule().clone()),
            RpcConfig::default(),
            committed_events,
        );
        (state, store, directory)
    }

    /// Build the HTTP or WebSocket router from `state` with `configure`
    /// applied to its RPC settings.
    fn configured_router(
        state: RpcState,
        websocket: bool,
        configure: impl FnOnce(&mut RpcConfig),
    ) -> Router {
        let mut config = state.config;
        configure(&mut config);
        rpc_router(
            state.store,
            state.progress,
            state.blob_schedule,
            config,
            state.committed_events,
            websocket,
        )
    }

    /// An HTTP router whose progress processor committed block `number`
    /// while no frame is retained, as after a backfill.
    async fn router_with_cursor_only(number: u64) -> (Router, tempfile::TempDir) {
        use leani_testkit::BlockLocalCounter;

        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteStore::open(leani_store_sqlite::StoreConfig::new(
            directory.path().join("cursor-only.sqlite"),
        ))
        .await
        .expect("store");
        let counter = BlockLocalCounter::default();
        let frame = fixture_frame(number, BlockHash::ZERO);
        let delta = counter.map(&frame).await.expect("delta");
        store
            .apply(
                &counter,
                leani_primitives::ProcessorCursor {
                    processor_id: counter.descriptor().id.to_string(),
                    processor_version: counter.descriptor().version.to_string(),
                    chain_id: ChainId(1),
                    block_number: frame.block.number,
                    block_hash: frame.block.hash,
                    finality: Finality::Finalized,
                    sequence: number,
                },
                &delta,
                &[],
            )
            .await
            .expect("apply");
        let (committed_events, _) = broadcast::channel(8);
        let router = http_router_with_progress(
            store,
            Arc::new(counter),
            Arc::new(BlobSchedule::mainnet()),
            RpcConfig::default(),
            committed_events,
        );
        (router, directory)
    }

    /// One WebSocket text message dispatched on a connection's subscriptions.
    async fn websocket_call(
        state: &RpcState,
        subscriptions: &mut BTreeMap<String, Subscription>,
        next_subscription: &mut u64,
        input: &str,
    ) -> Value {
        let response = websocket_dispatch_text(state, subscriptions, next_subscription, input)
            .await
            .expect("WebSocket response");
        serde_json::from_str(&response).expect("JSON response")
    }

    /// Serve `router` on an ephemeral loopback port.
    async fn serve(router: Router) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("listener address");
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.expect("RPC server");
        });
        (address, server)
    }

    /// Open a WebSocket connection, sending `origin` when given.
    async fn websocket_connect(
        address: std::net::SocketAddr,
        origin: Option<&str>,
    ) -> Result<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        tokio_tungstenite::tungstenite::Error,
    > {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;

        let mut request = format!("ws://{address}/")
            .into_client_request()
            .expect("WebSocket request");
        if let Some(origin) = origin {
            request
                .headers_mut()
                .insert(header::ORIGIN, origin.parse().expect("origin header value"));
        }
        connect_async(request).await.map(|(socket, _)| socket)
    }

    /// The HTTP status of a refused WebSocket handshake.
    fn refused_handshake_status(error: &tokio_tungstenite::tungstenite::Error) -> u16 {
        match error {
            tokio_tungstenite::tungstenite::Error::Http(response) => response.status().as_u16(),
            other => panic!("expected an HTTP refusal, got {other}"),
        }
    }

    /// Send one text message and parse the JSON reply.
    async fn websocket_round_trip<S>(socket: &mut S, message: String) -> Value
    where
        S: futures::Sink<ClientMessage>
            + futures::Stream<Item = Result<ClientMessage, tokio_tungstenite::tungstenite::Error>>
            + Unpin,
        <S as futures::Sink<ClientMessage>>::Error: std::fmt::Debug,
    {
        socket
            .send(ClientMessage::Text(message.into()))
            .await
            .expect("send");
        let reply = socket.next().await.expect("reply").expect("valid reply");
        serde_json::from_str(reply.to_text().expect("text reply")).expect("JSON reply")
    }

    /// A frame at `number` whose logs come from one address, each with
    /// `data_bytes` of data.
    fn frame_with_logs(
        number: u64,
        parent: BlockHash,
        count: u32,
        data_bytes: usize,
    ) -> BlockFrame {
        let mut frame = fixture_frame(number, parent);
        frame.logs = Material::Complete(
            (0..count)
                .map(|index| leani_primitives::Log {
                    address: Address::new([0x11; 20]),
                    topics: vec![[0x22; 32]],
                    data: vec![0x33; data_bytes],
                    transaction_hash: Some(TransactionHash::new([0x44; 32])),
                    transaction_index: 0,
                    log_index: index,
                })
                .collect(),
        );
        frame
    }

    /// POST `body` to an HTTP RPC router with the given headers.
    async fn post_raw(
        router: Router,
        content_type: Option<&str>,
        origin: Option<&str>,
        body: String,
    ) -> (StatusCode, Vec<u8>) {
        let mut request = Request::post("/");
        if let Some(content_type) = content_type {
            request = request.header(header::CONTENT_TYPE, content_type);
        }
        if let Some(origin) = origin {
            request = request.header(header::ORIGIN, origin);
        }
        let response = router
            .oneshot(request.body(Body::from(body)).expect("request"))
            .await
            .expect("response");
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        (status, bytes.to_vec())
    }

    fn history_source(
        id: &str,
        range: BlockRange,
        capabilities: CapabilitySet,
        priority: u16,
        frames: Vec<BlockFrame>,
    ) -> Arc<dyn HistorySource> {
        let mut descriptor = fixture_source_descriptor(id, range);
        descriptor.capabilities = capabilities;
        descriptor.complete_capabilities = capabilities;
        descriptor.priority = priority;
        Arc::new(ScriptedHistorySource::from_frames(descriptor, frames))
    }

    fn test_history(sources: Vec<Arc<dyn HistorySource>>) -> HistoricalRpc {
        HistoricalRpc::new(
            sources,
            HistoricalRpcConfig {
                max_input_bytes: 1_000_000,
                max_frame_bytes: 500_000,
                temporary_disk_bytes: 1_000_000,
                ..HistoricalRpcConfig::default()
            },
        )
        .expect("historical RPC")
    }

    fn with_log(mut frame: BlockFrame, marker: u8) -> BlockFrame {
        frame.logs = Material::Complete(vec![leani_primitives::Log {
            address: Address::new([0x11; 20]),
            topics: vec![[0x22; 32]],
            data: vec![marker],
            transaction_hash: Some(TransactionHash::new([marker; 32])),
            transaction_index: 0,
            log_index: 0,
        }]);
        frame
    }

    #[tokio::test]
    async fn websocket_subscription_lifecycle_uses_canonical_ids() {
        let (state, _store, _directory) = test_state_and_store().await;
        let mut subscriptions = BTreeMap::new();
        let mut next = 1;
        let subscribed = websocket_call(
            &state,
            &mut subscriptions,
            &mut next,
            r#"{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newHeads"]}"#,
        )
        .await;
        assert_eq!(subscribed["result"], "0x00000000000000000000000000000001");
        assert_eq!(subscriptions.len(), 1);
        let unsubscribed = websocket_call(
            &state,
            &mut subscriptions,
            &mut next,
            r#"{"jsonrpc":"2.0","id":2,"method":"eth_unsubscribe","params":["0x00000000000000000000000000000001"]}"#,
        )
        .await;
        assert_eq!(unsubscribed["result"], true);
        assert!(subscriptions.is_empty());
    }

    /// The notifications `event` queues for a connection's `subscriptions`,
    /// in order, or the frame that closes the connection instead.
    async fn queued_notifications(
        state: &RpcState,
        subscriptions: &BTreeMap<String, Subscription>,
        event: &ChainEvent,
    ) -> Result<Vec<Value>, CloseFrame> {
        let (outbox, mut queue) = Outbox::new(state.config.max_subscription_event_bytes);
        queue_notifications(state, subscriptions, event, &outbox).await?;
        let mut notifications = Vec::new();
        while let Ok((message, _room)) = queue.try_recv() {
            let Message::Text(text) = message.into_message() else {
                panic!("notifications are text messages");
            };
            notifications.push(serde_json::from_str(text.as_str()).expect("JSON notification"));
        }
        Ok(notifications)
    }

    /// The results `event` notifies `subscription` of, when it is its
    /// connection's only one.
    async fn subscription_results(
        state: &RpcState,
        subscription: &Subscription,
        event: &ChainEvent,
    ) -> Result<Vec<Value>, CloseFrame> {
        let subscriptions = BTreeMap::from([("0x1".to_owned(), subscription.clone())]);
        Ok(queued_notifications(state, &subscriptions, event)
            .await?
            .into_iter()
            .map(|notification| notification["params"]["result"].clone())
            .collect())
    }

    #[tokio::test]
    async fn websocket_reorg_marks_old_logs_removed_before_replacements() {
        let (state, store, _directory) = test_state_and_store().await;
        let address = Address::new([0x11; 20]);
        let old_transaction = TransactionHash::new([0x21; 32]);
        let replacement_transaction = TransactionHash::new([0x22; 32]);
        let mut old = fixture_frame(7, BlockHash::ZERO);
        old.logs = Material::Complete(vec![leani_primitives::Log {
            address,
            topics: vec![[0x31; 32]],
            data: vec![1],
            transaction_hash: Some(old_transaction),
            transaction_index: 0,
            log_index: 0,
        }]);
        store
            .store_recent_frame(&old)
            .await
            .expect("old recent frame");
        let mut replacement = fixture_frame(7, BlockHash::ZERO);
        replacement.block.hash = BlockHash::new([0x42; 32]);
        replacement.logs = Material::Complete(vec![leani_primitives::Log {
            address,
            topics: vec![[0x31; 32]],
            data: vec![2],
            transaction_hash: Some(replacement_transaction),
            transaction_index: 0,
            log_index: 0,
        }]);
        let subscription = parse_subscription(
            Some(&json!([
                "logs",
                {
                    "address": hex_bytes(address.as_array()),
                    "topics": [hex_bytes(&[0x31; 32])]
                }
            ])),
            &state.config,
        )
        .expect("log subscription");
        let results = subscription_results(
            &state,
            &subscription,
            &ChainEvent::Reorg {
                reverted: vec![old.block],
                applied: vec![replacement.clone()],
            },
        )
        .await
        .expect("subscription results");
        assert_eq!(results.len(), 2);
        assert_eq!(results[0]["removed"], true);
        assert_eq!(
            results[0]["transactionHash"],
            hex_bytes(old_transaction.as_array())
        );
        assert_eq!(results[1]["removed"], false);
        assert_eq!(
            results[1]["transactionHash"],
            hex_bytes(replacement_transaction.as_array())
        );
    }

    #[tokio::test]
    async fn websocket_new_heads_uses_the_canonical_rpc_header() {
        let (state, _store, _directory) = test_state_and_store().await;
        let frame = rpc_frame(9);
        let results = subscription_results(
            &state,
            &Subscription::NewHeads,
            &ChainEvent::Block(Box::new(frame.clone())),
        )
        .await
        .expect("new head");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["number"], "0x9");
        assert_eq!(results[0]["hash"], hex_bytes(frame.block.hash.as_array()));
    }

    #[test]
    fn log_subscriptions_reject_historical_range_fields() {
        let error = parse_subscription(
            Some(&json!([
                "logs",
                {"fromBlock": "0x1"}
            ])),
            &RpcConfig::default(),
        )
        .expect_err("historical subscription filter is rejected");
        assert_eq!(error.code, INVALID_PARAMS);
    }

    async fn call(router: Router, value: Value) -> (StatusCode, Option<Value>) {
        let response = router
            .oneshot(
                Request::post("/")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(value.to_string()))
                    .expect("request"),
            )
            .await
            .expect("response");
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let body = (!bytes.is_empty()).then(|| serde_json::from_slice(&bytes).expect("JSON"));
        (status, body)
    }

    #[tokio::test]
    async fn metadata_methods_use_canonical_quantities() {
        let (router, _directory) = test_router().await;
        let (_, chain) = call(
            router.clone(),
            json!({"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}),
        )
        .await;
        assert_eq!(chain.expect("body")["result"], "0x1");
        let (_, block) = call(
            router,
            json!({"jsonrpc":"2.0","id":2,"method":"eth_blockNumber"}),
        )
        .await;
        assert_eq!(block.expect("body")["result"], "0x0");
    }

    #[tokio::test]
    async fn eth_config_matches_eip7910_and_tracks_the_retained_head() {
        let (router, store, _directory) = test_router_and_store().await;
        let mut dencun = fixture_frame(19_426_589, BlockHash::ZERO);
        dencun.block.timestamp = 1_710_338_135;
        store
            .store_recent_frame(&dencun)
            .await
            .expect("Dencun frame");
        let (_, body) = call(
            router,
            json!({"jsonrpc":"2.0","id":1,"method":"eth_config","params":[]}),
        )
        .await;
        let result = body.expect("body")["result"].clone();
        assert_eq!(result["current"]["activationTime"], 1_710_338_135_u64);
        assert_eq!(result["current"]["chainId"], "0x1");
        assert_eq!(result["current"]["forkId"], "0x9f3d2254");
        assert_eq!(result["current"]["blobSchedule"]["target"], 3);
        assert_eq!(
            result["current"]["precompiles"]["KZG_POINT_EVALUATION"],
            "0x000000000000000000000000000000000000000a"
        );
        assert!(
            result["current"]["precompiles"]
                .get("BLS12_G1ADD")
                .is_none()
        );
        assert_eq!(result["next"]["forkId"], "0xc376cf8b");
        assert_eq!(result["last"]["forkId"], "0x07c9462e");
        assert_eq!(result["last"]["blobSchedule"]["target"], 14);
    }

    #[tokio::test]
    async fn eth_config_selects_the_fork_by_the_head_timestamp_or_fails_closed() {
        // Carried from Task 9: without a retained head frame, `eth_config`
        // chose the fork by the progress cursor's block number; on an empty
        // node, or before the first scheduled fork, it answered with the last
        // scheduled fork.
        let config = json!({"jsonrpc": "2.0", "id": 1, "method": "eth_config"});
        let (router, _directory) = test_router().await;
        assert_unavailable(
            call(router.clone(), config.clone()).await.1,
            "eth_config_head_timestamp_unavailable",
        );
        let (_, invalid) = call(
            router,
            json!({
                "jsonrpc":"2.0",
                "id":2,
                "method":"eth_config",
                "params":["latest"]
            }),
        )
        .await;
        assert_eq!(invalid.expect("body")["error"]["code"], INVALID_PARAMS);

        // Pectra's first block by number, with no known timestamp.
        let (router, _cursor_directory) = router_with_cursor_only(22_431_084).await;
        assert_unavailable(
            call(router, config.clone()).await.1,
            "eth_config_head_timestamp_unavailable",
        );

        // One second before Dencun no fork is scheduled.
        let (router, store, _directory) = test_router_and_store().await;
        let mut early = fixture_frame(19_426_588, BlockHash::ZERO);
        early.block.timestamp = 1_710_338_134;
        store
            .store_recent_frame(&early)
            .await
            .expect("recent frame");
        assert_unavailable(
            call(router.clone(), config.clone()).await.1,
            "eth_config_head_predates_schedule",
        );
        // A head at Prague's activation time is Prague, whatever its number.
        let mut prague = fixture_frame(19_426_589, early.block.hash);
        prague.block.timestamp = 1_746_612_311;
        store
            .store_recent_frame(&prague)
            .await
            .expect("recent frame");
        let (_, body) = call(router, config).await;
        assert_eq!(
            body.expect("body")["result"]["current"]["forkId"],
            "0xc376cf8b"
        );
    }

    #[tokio::test]
    async fn eth_config_does_not_require_a_blobs_processor_instance() {
        use leani_testkit::BlockLocalCounter;

        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteStore::open(leani_store_sqlite::StoreConfig::new(
            directory.path().join("custom-progress.sqlite"),
        ))
        .await
        .expect("store");
        let mut head = fixture_frame(19_426_589, BlockHash::ZERO);
        head.block.timestamp = 1_710_338_135;
        store.store_recent_frame(&head).await.expect("head frame");
        let progress: Arc<dyn Processor> = Arc::new(BlockLocalCounter::default());
        let (committed_events, _) = broadcast::channel(8);
        let router = http_router_with_progress(
            store,
            progress,
            Arc::new(BlobSchedule::mainnet()),
            RpcConfig::default(),
            committed_events,
        );

        let (_, body) = call(
            router,
            json!({"jsonrpc":"2.0","id":1,"method":"eth_config"}),
        )
        .await;
        assert_eq!(body.expect("body")["result"]["current"]["chainId"], "0x1");
    }

    #[tokio::test]
    async fn unsupported_material_fails_explicitly() {
        let (router, store, _directory) = test_router_and_store().await;
        store
            .store_recent_frame(&fixture_frame(1, BlockHash::ZERO))
            .await
            .expect("recent frame");
        let (_, body) = call(
            router,
            json!({
                "jsonrpc":"2.0",
                "id":"block",
                "method":"eth_getBlockByNumber",
                "params":["latest", true]
            }),
        )
        .await;
        let body = body.expect("body");
        assert_eq!(body["error"]["code"], DATA_UNAVAILABLE);
        assert_eq!(
            body["error"]["data"]["reason"],
            "complete_header_not_retained"
        );
    }

    #[tokio::test]
    async fn websocket_server_streams_post_commit_heads_end_to_end() {
        let (state, _store, _directory) = test_state_and_store().await;
        let events = state.committed_events.clone();
        let mut config = state.config;
        config.websocket_enabled = true;
        let router = websocket_router_with_progress(
            state.store,
            state.progress,
            state.blob_schedule,
            config,
            events.clone(),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("listener address");
        let server = tokio::spawn(async move {
            axum::serve(listener, router)
                .await
                .expect("WebSocket server");
        });
        let (mut socket, _) = connect_async(format!("ws://{address}/"))
            .await
            .expect("WebSocket connection");
        socket
            .send(ClientMessage::Text(
                r#"{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newHeads"]}"#.into(),
            ))
            .await
            .expect("subscribe");
        let response = socket
            .next()
            .await
            .expect("response")
            .expect("valid response");
        let response: Value = serde_json::from_str(response.to_text().expect("text response"))
            .expect("JSON response");
        let subscription = response["result"]
            .as_str()
            .expect("subscription ID")
            .to_owned();
        let frame = rpc_frame(11);
        events
            .send(ChainEvent::Block(Box::new(frame.clone())))
            .expect("publish head");
        let notification = socket
            .next()
            .await
            .expect("notification")
            .expect("valid notification");
        let notification: Value =
            serde_json::from_str(notification.to_text().expect("text notification"))
                .expect("JSON notification");
        assert_eq!(notification["method"], "eth_subscription");
        assert_eq!(notification["params"]["subscription"], subscription);
        assert_eq!(notification["params"]["result"]["number"], "0xb");
        assert_eq!(
            notification["params"]["result"]["hash"],
            hex_bytes(frame.block.hash.as_array())
        );
        server.abort();
    }

    #[tokio::test]
    async fn recent_head_and_logs_are_served_from_the_bounded_frame_store() {
        let (router, store, _directory) = test_router_and_store().await;
        store
            .store_recent_frame(&fixture_frame(7, BlockHash::ZERO))
            .await
            .expect("recent frame");
        let (_, block) = call(
            router.clone(),
            json!({"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"}),
        )
        .await;
        assert_eq!(block.expect("body")["result"], "0x7");
        let (_, logs) = call(
            router,
            json!({
                "jsonrpc":"2.0",
                "id":2,
                "method":"eth_getLogs",
                "params":[{"fromBlock":"0x7","toBlock":"0x7"}]
            }),
        )
        .await;
        assert_eq!(logs.expect("body")["result"], json!([]));
    }

    #[tokio::test]
    async fn exact_empty_block_and_receipts_round_trip_from_canonical_rlp() {
        let (router, store, _directory) = test_router_and_store().await;
        let frame = rpc_frame(9);
        let hash = hex_bytes(frame.block.hash.as_array());
        store
            .store_recent_frame(&frame)
            .await
            .expect("recent frame");
        let (_, block) = call(
            router.clone(),
            json!({
                "jsonrpc":"2.0",
                "id":1,
                "method":"eth_getBlockByNumber",
                "params":["0x9", false]
            }),
        )
        .await;
        let block = block.expect("body")["result"].clone();
        assert_eq!(block["number"], "0x9");
        assert_eq!(block["hash"], hash);
        assert_eq!(block["transactions"], json!([]));
        let (_, receipts) = call(
            router,
            json!({
                "jsonrpc":"2.0",
                "id":2,
                "method":"eth_getBlockReceipts",
                "params":["0x9"]
            }),
        )
        .await;
        assert_eq!(receipts.expect("body")["result"], json!([]));
    }

    #[test]
    fn exact_rpc_snapshot_matches_the_checked_in_compatibility_vector() {
        let actual = rpc_compatibility_snapshot(&rpc_frame(9), &BlobSchedule::mainnet())
            .expect("canonical snapshot");
        let expected: RpcCompatibilitySnapshot = serde_json::from_str(include_str!(
            "../fixtures/ethereum-jsonrpc-empty-block.json"
        ))
        .expect("compatibility fixture");
        assert_eq!(actual, expected);
    }

    /// A block at `timestamp` whose header carries `excess_blob_gas`, with one
    /// type-3 transaction and its canonical receipt.
    fn blob_receipt_frame(timestamp: u64, excess_blob_gas: u64) -> BlockFrame {
        use alloy_consensus::{Receipt, ReceiptWithBloom};
        use alloy_eips::eip2718::Encodable2718;

        let mut frame = rpc_frame_with_header(&ConsensusHeader {
            number: 20,
            timestamp,
            base_fee_per_gas: Some(7),
            withdrawals_root: Some(B256::ZERO),
            blob_gas_used: Some(131_072),
            excess_blob_gas: Some(excess_blob_gas),
            parent_beacon_block_root: Some(B256::ZERO),
            ..Default::default()
        });
        let hash = TransactionHash::new([0x33; 32]);
        let receipt = ConsensusReceiptEnvelope::Eip4844(ReceiptWithBloom {
            receipt: Receipt {
                status: true.into(),
                cumulative_gas_used: 21_000,
                logs: Vec::new(),
            },
            logs_bloom: alloy_primitives::Bloom::ZERO,
        });
        frame.transactions = Material::Complete(vec![leani_primitives::TransactionEnvelope {
            hash,
            transaction_type: 3,
            index: 0,
            encoded: None,
            from: Some(Address::new([0x11; 20])),
            to: Some(Address::new([0x22; 20])),
            nonce: Some(0),
            gas_limit: Some(21_000),
            value: None,
            input: None,
            max_fee_per_gas: None,
            max_priority_fee_per_gas: None,
            max_fee_per_blob_gas: None,
            blob_versioned_hashes: vec![BlockHash::new([0x01; 32])],
            size_bytes: None,
        }]);
        frame.receipts = Material::Complete(vec![leani_primitives::ReceiptEnvelope {
            transaction_hash: hash,
            transaction_type: 3,
            transaction_index: 0,
            encoded: Some(receipt.encoded_2718()),
            success: Some(true),
            gas_used: Some(21_000),
            effective_gas_price: Some(U256::from(7).into()),
            blob_gas_used: Some(131_072),
            blob_gas_price: None,
            logs: Vec::new(),
        }]);
        frame
    }

    async fn block_receipts(router: Router, frame: &BlockFrame) -> Value {
        let (_, body) = call(
            router,
            json!({
                "jsonrpc":"2.0",
                "id":1,
                "method":"eth_getBlockReceipts",
                "params":[hex_bytes(frame.block.hash.as_array())]
            }),
        )
        .await;
        body.expect("body")
    }

    #[tokio::test]
    async fn blob_gas_price_uses_the_fork_at_the_header_timestamp() {
        // Audit probe (H23): `blobGasPrice` used Cancun's update fraction for
        // every fork, so Prague and later receipts were wrong whenever the
        // excess blob gas was non-trivial.
        let cases = [
            // Cancun, unchanged: 99,710,729,314,173 wei.
            (1_710_338_135, "0x5aafb699df7d"),
            // Prague: 2,150,273,305 wei.
            (1_746_612_311, "0x802a9119"),
            // BPO2: 9,991 wei.
            (1_767_747_671, "0x2707"),
        ];
        for (timestamp, price) in cases {
            let (router, store, _directory) = test_router_and_store().await;
            let frame = blob_receipt_frame(timestamp, 107_610_112);
            store
                .store_recent_frame(&frame)
                .await
                .expect("recent frame");
            let receipts = block_receipts(router, &frame).await;
            let receipt = &receipts["result"][0];
            assert_eq!(receipt["type"], "0x3", "timestamp {timestamp}");
            assert_eq!(receipt["blobGasUsed"], "0x20000", "timestamp {timestamp}");
            assert_eq!(receipt["blobGasPrice"], price, "timestamp {timestamp}");
        }
    }

    #[tokio::test]
    async fn blob_gas_price_without_a_scheduled_fork_fails_closed() {
        // One second before Dencun no fork prices blob gas.
        let (router, store, _directory) = test_router_and_store().await;
        let frame = blob_receipt_frame(1_710_338_134, 107_610_112);
        store
            .store_recent_frame(&frame)
            .await
            .expect("recent frame");
        let receipts = block_receipts(router, &frame).await;
        assert_eq!(receipts["error"]["code"], DATA_UNAVAILABLE);
        assert_eq!(
            receipts["error"]["data"]["reason"],
            "blob_gas_price_schedule_unavailable"
        );

        // A schedule for another chain does not price this chain's blocks.
        let (state, store, _directory) = test_state_and_store().await;
        let frame = blob_receipt_frame(1_746_612_311, 107_610_112);
        store
            .store_recent_frame(&frame)
            .await
            .expect("recent frame");
        let mut schedule = BlobSchedule::mainnet();
        schedule.chain_id = 2;
        let router = http_router_with_progress(
            state.store,
            state.progress,
            Arc::new(schedule),
            state.config,
            state.committed_events,
        );
        let receipts = block_receipts(router, &frame).await;
        assert_eq!(
            receipts["error"]["data"]["reason"],
            "blob_gas_price_schedule_unavailable"
        );
    }

    #[test]
    fn protocol_blob_fee_agrees_with_alloy_for_every_scheduled_fraction() {
        // Alloy's independent EIP-4844 `fake_exponential` works in 512 bits
        // and saturates at `u128::MAX`; compare up to that point.
        for fork in &BlobSchedule::mainnet().forks {
            let fraction = fork.base_fee_update_fraction;
            for excess in (0..u64::MAX).step_by(4_999_999) {
                let reference = alloy_eips::eip4844::fake_exponential(
                    1,
                    u128::from(excess),
                    u128::from(fraction),
                );
                if reference == u128::MAX {
                    break;
                }
                assert_eq!(
                    get_blob_base_fee(excess, fraction).expect("fee"),
                    U256::from(reference),
                    "{} at excess {excess}",
                    fork.name
                );
            }
        }
    }

    #[test]
    fn type_3_receipt_snapshot_reports_every_receipt_field() {
        let frame = blob_receipt_frame(1_746_612_311, 107_610_112);
        let receipts = rpc_receipts(&frame, &BlobSchedule::mainnet())
            .and_then(serialize_rpc)
            .expect("receipts");
        assert_eq!(
            receipts,
            json!([{
                "type": "0x3",
                "status": "0x1",
                "cumulativeGasUsed": "0x5208",
                "logs": [],
                "logsBloom": format!("0x{}", "0".repeat(512)),
                "transactionHash": hex_bytes(&[0x33; 32]),
                "transactionIndex": "0x0",
                "blockHash": hex_bytes(frame.block.hash.as_array()),
                "blockNumber": "0x14",
                "gasUsed": "0x5208",
                "effectiveGasPrice": "0x7",
                "blobGasUsed": "0x20000",
                "blobGasPrice": "0x802a9119",
                "from": hex_bytes(&[0x11; 20]),
                "to": hex_bytes(&[0x22; 20]),
                "contractAddress": null
            }])
        );
    }

    #[tokio::test]
    async fn block_number_lookup_falls_back_to_ephemeral_history() {
        let (mut state, store, _directory) = test_state_and_store().await;
        let frame = rpc_frame(9);
        state.config.history = Some(test_history(vec![history_source(
            "historical-block",
            BlockRange::single(frame.block.number),
            CapabilitySet::of(Capability::Header).with(Capability::Transactions),
            0,
            vec![frame.clone()],
        )]));
        let router = http_router_with_progress(
            state.store,
            state.progress,
            state.blob_schedule,
            state.config,
            state.committed_events,
        );
        let (_, response) = call(
            router,
            json!({
                "jsonrpc":"2.0",
                "id":1,
                "method":"eth_getBlockByNumber",
                "params":["0x9", false]
            }),
        )
        .await;
        let block = &response.expect("response")["result"];
        assert_eq!(block["number"], "0x9");
        assert_eq!(block["hash"], hex_bytes(frame.block.hash.as_array()));
        assert_eq!(
            store
                .recent_stats(ChainId(1))
                .await
                .expect("recent statistics")
                .frames,
            0
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn historical_rpc_prefers_seekable_retained_raw_segments() {
        use leani_store_history::{
            Compression, HistoryStore, HistoryStoreConfig, MaterialShapeId, RawHistoryIndexPolicy,
            RetainedHistorySource, RetainedHistorySourceConfig, SegmentDescriptor, SegmentId,
            SegmentOwnerClaim, SegmentOwnerKind, SegmentReservation, StorageBudget,
            VerificationClass,
        };

        let directory = tempfile::tempdir().expect("temporary directory");
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
        .expect("raw store");
        let frame = rpc_frame(9);
        let capabilities = frame.capabilities();
        let mut pending = store
            .begin_segment_profiled(
                SegmentId::new("rpc-retained").expect("segment ID"),
                SegmentDescriptor {
                    chain_id: ChainId(1),
                    range: BlockRange::single(frame.block.number),
                    material_shape: MaterialShapeId::COMPLETE_EXECUTION,
                    present_capabilities: capabilities.present,
                    complete_capabilities: capabilities.complete,
                    verification: VerificationClass::Cryptographic,
                    trust: TrustModel::ProtocolVerified,
                },
                Compression::Snappy,
                SegmentReservation::new(1024 * 1024, 1024 * 1024),
                leani_store_history::RawHistoryProfile::PostMergeExecutionRpc {
                    merge_block: BlockNumber(9),
                },
                RawHistoryIndexPolicy {
                    block_hash: true,
                    transaction_hash: false,
                    logs: false,
                },
            )
            .await
            .expect("begin segment");
        pending.append(&frame).expect("append frame");
        pending
            .commit(&[SegmentOwnerClaim {
                kind: SegmentOwnerKind::OperatorPin,
                owner_id: "rpc-test".to_owned(),
            }])
            .await
            .expect("publish segment");
        let retained = Arc::new(
            RetainedHistorySource::new(
                store,
                RetainedHistorySourceConfig::local(
                    ChainId(1),
                    MaterialShapeId::COMPLETE_EXECUTION,
                    capabilities.complete,
                    VerificationClass::Cryptographic,
                    TrustModel::ProtocolVerified,
                )
                .expect("profile")
                .requiring_profile(
                    leani_store_history::RawHistoryProfile::PostMergeExecutionRpc {
                        merge_block: BlockNumber(9),
                    },
                ),
            )
            .expect("source"),
        );
        let history = test_history(vec![retained.clone()]);
        let output = history
            .fetch(
                ChainId(1),
                BlockRange::single(frame.block.number),
                CapabilitySet::of(Capability::Header).with(Capability::Transactions),
                FilterSet::default(),
            )
            .await
            .expect("retained response");
        assert_eq!(output, vec![frame.clone()]);
        assert_eq!(retained.stats().record_reads, 1);

        let (mut state, _recent_store, _directory) = test_state_and_store().await;
        state.config.history = Some(history);
        let router = http_router_with_progress(
            state.store,
            state.progress,
            state.blob_schedule,
            state.config,
            state.committed_events,
        );
        let (_, response) = call(
            router,
            json!({
                "jsonrpc":"2.0",
                "id":1,
                "method":"eth_getBlockByHash",
                "params":[hex_bytes(frame.block.hash.as_array()), false]
            }),
        )
        .await;
        let block = &response.expect("response")["result"];
        assert_eq!(block["number"], "0x9");
        assert_eq!(block["hash"], hex_bytes(frame.block.hash.as_array()));
        assert_eq!(retained.stats().locator_uses, 1);
    }

    #[tokio::test]
    async fn historical_router_fails_over_at_the_request_boundary() {
        let range = BlockRange::single(BlockNumber(9));
        let capabilities = CapabilitySet::of(Capability::Header).with(Capability::Transactions);
        let mut failed_descriptor = fixture_source_descriptor("failed-history", range);
        failed_descriptor.capabilities = capabilities;
        failed_descriptor.complete_capabilities = capabilities;
        failed_descriptor.priority = 0;
        let failed: Arc<dyn HistorySource> = Arc::new(ScriptedHistorySource::new(
            failed_descriptor,
            vec![ScriptedChunk {
                range,
                schema_version: "fixture-v1".to_owned(),
                estimated_bytes: None,
                steps: vec![HistoryStep::Error(SourceError::Unavailable(
                    "fixture outage".to_owned(),
                ))],
            }],
        ));
        let frame = rpc_frame(9);
        let healthy = history_source(
            "healthy-history",
            range,
            capabilities,
            1,
            vec![frame.clone()],
        );
        let output = test_history(vec![failed, healthy])
            .fetch(ChainId(1), range, capabilities, FilterSet::default())
            .await
            .expect("fallback source");
        assert_eq!(output, vec![frame]);
    }

    #[tokio::test]
    async fn historical_logs_join_to_the_retained_recent_window() {
        let (mut state, store, _directory) = test_state_and_store().await;
        let first = with_log(fixture_frame(1, BlockHash::ZERO), 1);
        let second = with_log(fixture_frame(2, first.block.hash), 2);
        let third = with_log(fixture_frame(3, second.block.hash), 3);
        store
            .store_recent_frame(&third)
            .await
            .expect("recent frame");
        state.config.history = Some(test_history(vec![history_source(
            "historical-logs",
            BlockRange::new(first.block.number, second.block.number).expect("range"),
            CapabilitySet::of(Capability::Logs),
            0,
            vec![first, second],
        )]));
        let router = http_router_with_progress(
            state.store,
            state.progress,
            state.blob_schedule,
            state.config,
            state.committed_events,
        );
        let (_, response) = call(
            router,
            json!({
                "jsonrpc":"2.0",
                "id":1,
                "method":"eth_getLogs",
                "params":[{
                    "fromBlock":"0x1",
                    "toBlock":"0x3",
                    "address": hex_bytes(&[0x11; 20])
                }]
            }),
        )
        .await;
        let logs = response.expect("response")["result"]
            .as_array()
            .expect("logs")
            .clone();
        assert_eq!(logs.len(), 3);
        assert_eq!(logs[0]["blockNumber"], "0x1");
        assert_eq!(logs[2]["blockNumber"], "0x3");
    }

    fn rpc_frame(number: u64) -> BlockFrame {
        rpc_frame_with_header(&ConsensusHeader {
            number,
            timestamp: 1_700_000_000 + number,
            ..Default::default()
        })
    }

    fn rpc_frame_with_header(header: &ConsensusHeader) -> BlockFrame {
        let hash = header.hash_slow();
        let mut frame = fixture_frame(header.number, BlockHash::ZERO);
        frame.block.hash = BlockHash::new(hash.0);
        frame.block.timestamp = header.timestamp;
        frame.header = Material::Complete(leani_primitives::HeaderEnvelope {
            rlp: Some(alloy_rlp::encode(header)),
            transactions_root: Some(BlockHash::new(header.transactions_root.0)),
            receipts_root: Some(BlockHash::new(header.receipts_root.0)),
            withdrawals_root: header.withdrawals_root.map(|root| BlockHash::new(root.0)),
            gas_limit: Some(header.gas_limit),
            gas_used: Some(header.gas_used),
            base_fee_per_gas: header.base_fee_per_gas.map(U256::from).map(Into::into),
            blob_gas_used: header.blob_gas_used,
            excess_blob_gas: header.excess_blob_gas,
            size_bytes: Some(u64::try_from(alloy_rlp::encode(header).len()).unwrap_or(u64::MAX)),
            consensus_size_bytes: None,
            transaction_count: Some(0),
        });
        frame
    }

    #[tokio::test]
    async fn batches_skip_notifications_and_reject_empty_batches() {
        let (router, _directory) = test_router().await;
        let (_, body) = call(
            router.clone(),
            json!([
                {"jsonrpc":"2.0","method":"net_version"},
                {"jsonrpc":"2.0","id":1,"method":"web3_clientVersion"}
            ]),
        )
        .await;
        assert_eq!(body.expect("body").as_array().expect("batch").len(), 1);
        let (_, empty) = call(router, json!([])).await;
        assert_eq!(empty.expect("body")["error"]["code"], INVALID_REQUEST);
    }

    #[tokio::test]
    async fn notification_only_batch_has_no_body() {
        let (router, _directory) = test_router().await;
        let (status, body) = call(router, json!([{"jsonrpc":"2.0","method":"net_version"}])).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert!(body.is_none());
    }

    #[test]
    fn quantities_reject_leading_zeroes() {
        assert_eq!(parse_hex_quantity("0x0").expect("zero"), 0);
        assert!(parse_hex_quantity("0x00").is_err());
        assert!(parse_hex_quantity("1").is_err());
    }

    #[tokio::test]
    async fn cross_origin_and_non_json_requests_are_refused_before_dispatch() {
        // Audit probe (H26), inverted: a 3 KB `text/plain` batch sent with a
        // foreign Origin used to be answered with 4.2 MB of logs.
        let (state, store, _directory) = test_state_and_store().await;
        store
            .store_recent_frame(&frame_with_logs(7, BlockHash::ZERO, 1, 64 * 1_024))
            .await
            .expect("recent frame");
        let request = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_getLogs",
            "params": [{"fromBlock": "0x7", "toBlock": "0x7"}]
        });
        let body = serde_json::to_string(&vec![request; 32]).expect("batch");
        let router = configured_router(state, false, |config| {
            config.allowed_origins = BTreeSet::from(["https://app.example".to_owned()]);
        });
        for (content_type, origin, status) in [
            (
                Some("text/plain"),
                Some("https://untrusted.example"),
                StatusCode::FORBIDDEN,
            ),
            (
                Some("application/json"),
                Some("https://untrusted.example"),
                StatusCode::FORBIDDEN,
            ),
            (
                Some("application/json"),
                Some("null"),
                StatusCode::FORBIDDEN,
            ),
            (
                Some("application/json"),
                Some("http://localhost.untrusted.example"),
                StatusCode::FORBIDDEN,
            ),
            (
                Some("application/json"),
                Some("http://app.example"),
                StatusCode::FORBIDDEN,
            ),
            (Some("text/plain"), None, StatusCode::UNSUPPORTED_MEDIA_TYPE),
            (
                Some("application/x-www-form-urlencoded"),
                None,
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
            ),
            (None, None, StatusCode::UNSUPPORTED_MEDIA_TYPE),
        ] {
            let (actual, _) = post_raw(router.clone(), content_type, origin, body.clone()).await;
            assert_eq!(actual, status, "{content_type:?} from {origin:?}");
        }
        for (content_type, origin) in [
            ("application/json", None),
            ("Application/JSON; charset=utf-8", None),
            ("application/json", Some("http://localhost:5173")),
            ("application/json", Some("http://127.0.0.1")),
            ("application/json", Some("https://[::1]:8443")),
            ("application/json", Some("https://app.example")),
        ] {
            let (status, bytes) =
                post_raw(router.clone(), Some(content_type), origin, body.clone()).await;
            assert_eq!(status, StatusCode::OK, "{content_type} from {origin:?}");
            let results: Value = serde_json::from_slice(&bytes).expect("JSON");
            assert_eq!(results.as_array().expect("batch").len(), 32);
        }
    }

    #[tokio::test]
    async fn cross_origin_websocket_upgrades_are_refused_and_subscriptions_are_bounded() {
        // Audit probe (H26), inverted: a foreign Origin used to open a
        // WebSocket and hold 2,048 subscriptions on it.
        let (state, _store, _directory) = test_state_and_store().await;
        let (address, server) = serve(configured_router(state, true, |_| {})).await;
        for origin in ["https://untrusted.example", "null"] {
            let error = websocket_connect(address, Some(origin))
                .await
                .expect_err("a foreign origin is refused");
            assert_eq!(refused_handshake_status(&error), 403, "{origin}");
        }
        let mut socket = websocket_connect(address, Some("http://localhost:3000"))
            .await
            .expect("a loopback origin connects");
        let subscribe =
            json!({"jsonrpc": "2.0", "id": 1, "method": "eth_subscribe", "params": ["newHeads"]});
        let oversized = websocket_round_trip(
            &mut socket,
            serde_json::to_string(&vec![subscribe.clone(); 2_048]).expect("batch"),
        )
        .await;
        assert_eq!(oversized["error"]["code"], INVALID_REQUEST);
        let (mut accepted, mut refused) = (0, 0);
        for _ in 0..2 {
            let responses = websocket_round_trip(
                &mut socket,
                serde_json::to_string(&vec![subscribe.clone(); DEFAULT_MAX_BATCH_REQUESTS])
                    .expect("batch"),
            )
            .await;
            for response in responses.as_array().expect("batch response") {
                if response["result"].is_string() {
                    accepted += 1;
                } else {
                    // -32005: limit exceeded.
                    assert_eq!(response["error"]["code"], -32_005, "{response}");
                    refused += 1;
                }
            }
        }
        assert_eq!(DEFAULT_MAX_SUBSCRIPTIONS_PER_CONNECTION, 128);
        assert_eq!(
            (accepted, refused),
            (128, 2 * DEFAULT_MAX_BATCH_REQUESTS - 128)
        );
        socket.close(None).await.expect("close");
        server.abort();
    }

    #[tokio::test]
    async fn websocket_connections_are_capped_and_released_on_disconnect() {
        let (state, _store, _directory) = test_state_and_store().await;
        let (address, server) = serve(configured_router(state, true, |config| {
            config.max_websocket_connections = 2;
        }))
        .await;
        let mut first = websocket_connect(address, None)
            .await
            .expect("first connection");
        let _second = websocket_connect(address, None)
            .await
            .expect("second connection");
        let error = websocket_connect(address, None)
            .await
            .expect_err("a third connection is refused");
        assert_eq!(refused_handshake_status(&error), 503);
        first.close(None).await.expect("close");
        while first.next().await.is_some() {}
        let mut reconnected = None;
        for _ in 0..100 {
            match websocket_connect(address, None).await {
                Ok(socket) => {
                    reconnected = Some(socket);
                    break;
                }
                Err(error) => {
                    assert_eq!(refused_handshake_status(&error), 503);
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        }
        assert!(
            reconnected.is_some(),
            "a closed connection releases its slot"
        );
        server.abort();
    }

    #[tokio::test]
    async fn websocket_subscriptions_are_capped_per_connection() {
        let (mut state, _store, _directory) = test_state_and_store().await;
        state.config.max_subscriptions_per_connection = 2;
        let mut subscriptions = BTreeMap::new();
        let mut next = 1;
        let subscribe =
            r#"{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newHeads"]}"#;
        let first = websocket_call(&state, &mut subscriptions, &mut next, subscribe).await;
        let second = websocket_call(&state, &mut subscriptions, &mut next, subscribe).await;
        assert!(first["result"].is_string() && second["result"].is_string());
        let refused = websocket_call(&state, &mut subscriptions, &mut next, subscribe).await;
        assert_eq!(refused["error"]["code"], -32_005, "{refused}");
        assert_eq!(subscriptions.len(), 2);
        let unsubscribe = json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "eth_unsubscribe",
            "params": [first["result"]]
        });
        let unsubscribed = websocket_call(
            &state,
            &mut subscriptions,
            &mut next,
            &unsubscribe.to_string(),
        )
        .await;
        assert_eq!(unsubscribed["result"], true);
        let again = websocket_call(&state, &mut subscriptions, &mut next, subscribe).await;
        assert!(again["result"].is_string(), "{again}");
    }

    #[tokio::test]
    async fn batches_longer_than_the_limit_are_refused_whole() {
        let (router, _directory) = test_router().await;
        let request = json!({"jsonrpc": "2.0", "id": 1, "method": "eth_chainId"});
        assert_eq!(DEFAULT_MAX_BATCH_REQUESTS, 100);
        let (_, full) = call(
            router.clone(),
            Value::Array(vec![request.clone(); DEFAULT_MAX_BATCH_REQUESTS]),
        )
        .await;
        assert_eq!(full.expect("body").as_array().expect("batch").len(), 100);
        let (status, oversized) = call(
            router,
            Value::Array(vec![request; DEFAULT_MAX_BATCH_REQUESTS + 1]),
        )
        .await;
        let oversized = oversized.expect("body");
        assert_eq!(status, StatusCode::OK);
        assert!(oversized["id"].is_null(), "{oversized}");
        assert_eq!(oversized["error"]["code"], INVALID_REQUEST);
        assert_eq!(oversized["error"]["data"]["limit"], 100);
    }

    #[tokio::test]
    async fn responses_past_the_size_limit_become_limit_errors() {
        let (state, _store, _directory) = test_state_and_store().await;
        let batch = json!([
            {"jsonrpc": "2.0", "id": 1, "method": "eth_chainId"},
            {"jsonrpc": "2.0", "id": 2, "method": "eth_chainId"},
            {"jsonrpc": "2.0", "id": 3, "method": "eth_chainId"}
        ]);
        let single = json!({"jsonrpc": "2.0", "id": 4, "method": "eth_chainId"});
        let unlimited = configured_router(state.clone(), false, |_| {});
        let batch_bytes = post_raw(
            unlimited.clone(),
            Some("application/json"),
            None,
            batch.to_string(),
        )
        .await
        .1
        .len();
        let single_bytes = post_raw(
            unlimited,
            Some("application/json"),
            None,
            single.to_string(),
        )
        .await
        .1
        .len();

        let at_limit = configured_router(state.clone(), false, |config| {
            config.max_response_bytes = batch_bytes;
        });
        let (_, body) = call(at_limit, batch.clone()).await;
        let body = body.expect("body");
        let responses = body.as_array().expect("batch");
        assert!(
            responses.iter().all(|response| response["result"] == "0x1"),
            "{body}"
        );
        let below = configured_router(state.clone(), false, |config| {
            config.max_response_bytes = batch_bytes - 1;
        });
        let (_, body) = call(below, batch).await;
        let body = body.expect("body");
        let responses = body.as_array().expect("batch");
        assert_eq!(responses[0]["result"], "0x1");
        assert_eq!(responses[1]["result"], "0x1");
        assert_eq!(responses[2]["id"], 3);
        assert_eq!(responses[2]["error"]["code"], -32_005, "{body}");

        let at_limit = configured_router(state.clone(), false, |config| {
            config.max_response_bytes = single_bytes;
        });
        let (_, body) = call(at_limit, single.clone()).await;
        assert_eq!(body.expect("body")["result"], "0x1");
        let below = configured_router(state, false, |config| {
            config.max_response_bytes = single_bytes - 1;
        });
        let (_, body) = call(below, single).await;
        let body = body.expect("body");
        assert_eq!(body["id"], 4);
        assert_eq!(body["error"]["code"], -32_005, "{body}");
    }

    #[tokio::test]
    async fn get_logs_results_are_capped_with_a_retry_range() {
        let (state, store, _directory) = test_state_and_store().await;
        let seventh = frame_with_logs(7, BlockHash::ZERO, 2, 1);
        let eighth = frame_with_logs(8, seventh.block.hash, 1, 1);
        let ninth = frame_with_logs(9, eighth.block.hash, 3, 1);
        for frame in [&seventh, &eighth, &ninth] {
            store.store_recent_frame(frame).await.expect("recent frame");
        }
        let router = configured_router(state, false, |config| config.max_log_results = 2);
        let logs = |from: &str, to: &str| {
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_getLogs",
                "params": [{"fromBlock": from, "toBlock": to}]
            })
        };
        let (_, at_limit) = call(router.clone(), logs("0x7", "0x7")).await;
        assert_eq!(
            at_limit.expect("body")["result"]
                .as_array()
                .expect("logs")
                .len(),
            2
        );
        let (_, over) = call(router.clone(), logs("0x7", "0x8")).await;
        let over = over.expect("body");
        assert_eq!(over["error"]["code"], -32_005, "{over}");
        assert_eq!(
            over["error"]["message"],
            "query returned more than 2 results. Try with this block range [0x7, 0x7]."
        );
        assert_eq!(
            over["error"]["data"],
            json!({"from": "0x7", "to": "0x7", "limit": 2})
        );
        let (_, single_block) = call(router, logs("0x9", "0x9")).await;
        let single_block = single_block.expect("body");
        assert_eq!(single_block["error"]["code"], -32_005, "{single_block}");
        assert_eq!(
            single_block["error"]["message"],
            "query returned more than 2 results"
        );
    }

    #[tokio::test]
    async fn log_filters_cap_addresses_and_topic_alternatives() {
        let (state, store, _directory) = test_state_and_store().await;
        store
            .store_recent_frame(&fixture_frame(7, BlockHash::ZERO))
            .await
            .expect("recent frame");
        let limits = |config: &mut RpcConfig| {
            config.max_log_addresses = 2;
            config.max_log_topic_alternatives = 2;
        };
        let router = configured_router(state, false, limits);
        let address = |byte: u8| hex_bytes(&[byte; 20]);
        let topic = |byte: u8| hex_bytes(&[byte; 32]);
        for (mut filter, code) in [
            (json!({"address": [address(1), address(2)]}), None),
            (
                json!({"address": [address(1), address(2), address(3)]}),
                Some(INVALID_PARAMS),
            ),
            (json!({"topics": [[topic(1), topic(2)]]}), None),
            (
                json!({"topics": [null, [topic(1), topic(2), topic(3)]]}),
                Some(INVALID_PARAMS),
            ),
        ] {
            filter["fromBlock"] = json!("0x7");
            filter["toBlock"] = json!("0x7");
            let (_, body) = call(
                router.clone(),
                json!({"jsonrpc": "2.0", "id": 1, "method": "eth_getLogs", "params": [filter]}),
            )
            .await;
            let body = body.expect("body");
            match code {
                None => assert_eq!(body["result"], json!([]), "{body}"),
                Some(code) => assert_eq!(body["error"]["code"], code, "{body}"),
            }
        }
        let mut config = RpcConfig::default();
        limits(&mut config);
        let error = parse_subscription(
            Some(&json!(["logs", {"address": [address(1), address(2), address(3)]}])),
            &config,
        )
        .expect_err("an oversized subscription filter is refused");
        assert_eq!(error.code, INVALID_PARAMS);
    }

    #[test]
    fn log_json_length_is_computed_without_encoding() {
        let mut frame = fixture_frame(0, BlockHash::ZERO);
        for (number, data, topics, log_index, transaction_index) in [
            (0_u64, 0_usize, 0_usize, 0_u32, 0_u32),
            (7, 1, 1, 15, 16),
            (19_426_589, 1_000, 4, 255, 4_096),
            (u64::MAX, 3, 2, u32::MAX, u32::MAX),
        ] {
            frame.block.number = BlockNumber(number);
            let log = leani_primitives::Log {
                address: Address::new([0x11; 20]),
                topics: vec![[0x22; 32]; topics],
                data: vec![0x33; data],
                transaction_hash: Some(TransactionHash::new([0x44; 32])),
                transaction_index,
                log_index,
            };
            for removed in [false, true] {
                let encoded = serde_json::to_string(
                    &rpc_log_value(&frame, &log, removed).expect("log value"),
                )
                .expect("encoded log");
                assert_eq!(
                    rpc_log_json_len(&frame, &log, removed),
                    encoded.len(),
                    "{encoded}"
                );
            }
        }
    }

    #[tokio::test]
    async fn get_logs_stops_at_the_remaining_response_budget() {
        // Review 1, minor 2: a batch's earlier responses use part of the
        // response limit, so a later `eth_getLogs` gets only what remains.
        let (state, store, _directory) = test_state_and_store().await;
        store
            .store_recent_frame(&frame_with_logs(7, BlockHash::ZERO, 3, 100))
            .await
            .expect("recent frame");
        let params = || Some(json!([{"fromBlock": "0x7", "toBlock": "0x7"}]));
        let result = eth_get_logs(&state, params(), usize::MAX)
            .await
            .expect("logs");
        let needed = serde_json::to_string(&result).expect("encoded").len();
        eth_get_logs(&state, params(), needed)
            .await
            .expect("exactly enough budget");
        let error = eth_get_logs(&state, params(), needed - 1)
            .await
            .expect_err("one byte short");
        assert_eq!(error.code, -32_005);
        assert_eq!(
            error.data.expect("data")["reason"],
            "response_size_limit_exceeded"
        );
    }

    #[tokio::test]
    async fn configured_origins_match_in_any_case_and_without_a_trailing_slash() {
        let (state, _store, _directory) = test_state_and_store().await;
        let router = configured_router(state, false, |config| {
            config.allowed_origins = BTreeSet::from(["HTTPS://App.Example/".to_owned()]);
        });
        let (status, _) = post_raw(
            router,
            Some("application/json"),
            Some("https://app.example"),
            json!({"jsonrpc": "2.0", "id": 1, "method": "eth_chainId"}).to_string(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    /// A JSON-RPC 2.0 call of `method` with `params` and ID 1.
    fn rpc_call(method: &str, params: Value) -> Value {
        let mut call = json!({"jsonrpc": "2.0", "id": 1, "method": method});
        call["params"] = params;
        call
    }

    /// Assert a `-32004` error with `reason`.
    fn assert_unavailable(body: Option<Value>, reason: &str) {
        let body = body.expect("body");
        assert_eq!(body["error"]["code"], DATA_UNAVAILABLE, "{body}");
        assert_eq!(body["error"]["data"]["reason"], reason, "{body}");
    }

    #[tokio::test]
    async fn null_ids_are_answered_and_other_id_types_are_refused() {
        // Audit JSON-RPC-1 and 2: `"id": null` was taken for a notification
        // and got no response, while object, array, and boolean IDs were
        // echoed. An ID must be a string, a number, or null.
        let (state, _store, _directory) = test_state_and_store().await;
        let router = configured_router(state.clone(), false, |_| {});
        let chain_id = |id: Value| json!({"jsonrpc": "2.0", "id": id, "method": "eth_chainId"});
        let answered = json!({"jsonrpc": "2.0", "id": null, "result": "0x1"});
        let (status, body) = call(router.clone(), chain_id(Value::Null)).await;
        assert_eq!((status, body), (StatusCode::OK, Some(answered.clone())));
        for id in [json!({"id": 1}), json!([1]), json!(true), json!(false)] {
            let (_, body) = call(router.clone(), chain_id(id.clone())).await;
            let body = body.expect("an invalid ID is answered");
            assert!(body["id"].is_null(), "{id}: {body}");
            assert_eq!(body["error"]["code"], INVALID_REQUEST, "{id}: {body}");
        }
        // The same in a batch, where a notification still gets no response.
        let (_, body) = call(
            router,
            json!([
                chain_id(Value::Null),
                {"jsonrpc": "2.0", "method": "eth_chainId"},
                chain_id(json!(true)),
                chain_id(json!("text"))
            ]),
        )
        .await;
        let body = body.expect("body");
        let responses = body.as_array().expect("batch");
        assert_eq!(responses.len(), 3, "{body}");
        assert_eq!(responses[0], answered);
        assert_eq!(responses[1]["error"]["code"], INVALID_REQUEST, "{body}");
        assert_eq!(responses[2]["id"], "text", "{body}");
        // A call left unrun past the response limit is answered too.
        let full = configured_router(state.clone(), false, |config| {
            config.max_response_bytes = 1;
        });
        let (_, body) = call(full, json!([chain_id(json!(1)), chain_id(Value::Null)])).await;
        let body = body.expect("body");
        let responses = body.as_array().expect("batch");
        assert_eq!(responses.len(), 2, "{body}");
        assert!(responses[1]["id"].is_null(), "{body}");
        assert_eq!(responses[1]["error"]["code"], LIMIT_EXCEEDED, "{body}");
        // WebSocket calls follow the same rules.
        let mut subscriptions = BTreeMap::new();
        let mut next = 1;
        let response = websocket_dispatch_text(
            &state,
            &mut subscriptions,
            &mut next,
            &chain_id(Value::Null).to_string(),
        )
        .await
        .map(|response| serde_json::from_str::<Value>(&response).expect("JSON"));
        assert_eq!(response, Some(answered));
        let refused = websocket_call(
            &state,
            &mut subscriptions,
            &mut next,
            &chain_id(json!([1])).to_string(),
        )
        .await;
        assert_eq!(refused["error"]["code"], INVALID_REQUEST, "{refused}");
    }

    #[tokio::test]
    async fn quantities_are_canonical_hexadecimal_digits() {
        // Audit JSON-RPC-3: `u64::from_str_radix` accepts a leading `+`.
        for (quantity, value) in [
            ("0x0", 0),
            ("0x7", 7),
            ("0xa", 10),
            ("0xA", 10),
            ("0xffffffffffffffff", u64::MAX),
        ] {
            assert_eq!(parse_hex_quantity(quantity).expect(quantity), value);
        }
        for quantity in [
            "0x+1",
            "0x+0",
            "0x-1",
            "0x",
            "0x 1",
            "0x1 ",
            "0x1_0",
            "0xg",
            "0X1",
            "+0x1",
            "0x01",
            "0x10000000000000000",
        ] {
            assert!(parse_hex_quantity(quantity).is_err(), "{quantity}");
        }
        let (router, store, _directory) = test_router_and_store().await;
        store
            .store_recent_frame(&fixture_frame(7, BlockHash::ZERO))
            .await
            .expect("recent frame");
        let (_, body) = call(
            router,
            rpc_call(
                "eth_getLogs",
                json!([{"fromBlock": "0x+7", "toBlock": "0x7"}]),
            ),
        )
        .await;
        let body = body.expect("body");
        assert_eq!(body["error"]["code"], INVALID_PARAMS, "{body}");
    }

    #[tokio::test]
    async fn finalized_and_safe_name_the_finalized_head() {
        // Audit JSON-RPC-4: every block tag but `latest` got -32602.
        let (router, store, _directory) = test_router_and_store().await;
        let mut head = with_log(rpc_frame(9), 9);
        head.finality = Finality::Included;
        store.store_recent_frame(&head).await.expect("head frame");
        assert_unavailable(
            call(
                router.clone(),
                rpc_call("eth_getBlockByNumber", json!(["finalized", false])),
            )
            .await
            .1,
            "finalized_block_unavailable",
        );
        let finalized = with_log(rpc_frame(8), 8);
        for frame in [with_log(rpc_frame(7), 7), finalized.clone()] {
            store
                .store_recent_frame(&frame)
                .await
                .expect("recent frame");
        }
        for tag in ["finalized", "safe"] {
            let (_, body) = call(
                router.clone(),
                rpc_call("eth_getBlockByNumber", json!([tag, false])),
            )
            .await;
            let body = body.expect("body");
            assert_eq!(
                body["result"]["hash"],
                hex_bytes(finalized.block.hash.as_array()),
                "{tag}: {body}"
            );
            for method in [
                "eth_getBlockReceipts",
                "eth_getBlockTransactionCountByNumber",
            ] {
                let (_, body) = call(router.clone(), rpc_call(method, json!([tag]))).await;
                let body = body.expect("body");
                assert!(body.get("error").is_none(), "{method}({tag}): {body}");
            }
        }
        let (_, body) = call(
            router.clone(),
            rpc_call("eth_getBlockByNumber", json!(["latest", false])),
        )
        .await;
        assert_eq!(body.expect("body")["result"]["number"], "0x9");
        let log_blocks = |body: Option<Value>| {
            let body = body.expect("body");
            body["result"]
                .as_array()
                .unwrap_or_else(|| panic!("logs: {body}"))
                .iter()
                .map(|log| log["blockNumber"].clone())
                .collect::<Vec<_>>()
        };
        let (_, body) = call(
            router.clone(),
            rpc_call(
                "eth_getLogs",
                json!([{"fromBlock": "0x7", "toBlock": "safe"}]),
            ),
        )
        .await;
        assert_eq!(log_blocks(body), [json!("0x7"), json!("0x8")]);
        let (_, body) = call(
            router.clone(),
            rpc_call("eth_getLogs", json!([{"fromBlock": "finalized"}])),
        )
        .await;
        assert_eq!(log_blocks(body), [json!("0x8"), json!("0x9")]);
        // `earliest` is block 0, which is not retained, and no pending block
        // exists.
        for (tag, reason) in [
            ("earliest", "block_not_retained"),
            ("pending", "pending_block_unavailable"),
        ] {
            assert_unavailable(
                call(
                    router.clone(),
                    rpc_call("eth_getBlockByNumber", json!([tag, false])),
                )
                .await
                .1,
                reason,
            );
        }
        assert_unavailable(
            call(
                router,
                rpc_call(
                    "eth_getLogs",
                    json!([{"fromBlock": "0x7", "toBlock": "pending"}]),
                ),
            )
            .await
            .1,
            "pending_block_unavailable",
        );
    }

    #[tokio::test]
    async fn unreadable_bodies_are_answered_with_json_rpc_errors() {
        // Audit JSON-RPC-5: invalid UTF-8 got HTTP 400 and an oversized body
        // HTTP 413, each with a plain-text body.
        let (state, _store, _directory) = test_state_and_store().await;
        let limit = 256;
        let router = configured_router(state, false, |config| {
            config.max_request_bytes = limit;
        });
        let post = |body: Vec<u8>| {
            let router = router.clone();
            async move {
                let response = router
                    .oneshot(
                        Request::post("/")
                            .header(header::CONTENT_TYPE, "application/json")
                            .body(Body::from(body))
                            .expect("request"),
                    )
                    .await
                    .expect("response");
                let status = response.status();
                let content_type = response.headers().get(header::CONTENT_TYPE).cloned();
                let bytes = to_bytes(response.into_body(), usize::MAX)
                    .await
                    .expect("body");
                (
                    status,
                    content_type,
                    serde_json::from_slice::<Value>(&bytes).ok(),
                )
            }
        };
        let mut invalid = br#"{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[""#.to_vec();
        invalid.extend_from_slice(b"\xff\"]}");
        let (status, _, body) = post(invalid).await;
        assert_eq!(status, StatusCode::OK);
        let body = body.expect("a JSON-RPC error body");
        assert!(body["id"].is_null(), "{body}");
        assert_eq!(body["error"]["code"], PARSE_ERROR, "{body}");

        let request = json!({"jsonrpc": "2.0", "id": 1, "method": "eth_chainId"}).to_string();
        let padded = |length: usize| format!("{request:<length$}").into_bytes();
        let (status, _, body) = post(padded(limit)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.expect("body")["result"], "0x1");
        let (status, content_type, body) = post(padded(limit + 1)).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(
            content_type.as_ref().and_then(|value| value.to_str().ok()),
            Some("application/json")
        );
        assert_eq!(
            body,
            Some(json!({
                "jsonrpc": "2.0",
                "id": null,
                "error": {
                    "code": LIMIT_EXCEEDED,
                    "message": "Limit exceeded",
                    "data": {"reason": "request_size_limit_exceeded", "limit": limit}
                }
            }))
        );
    }

    #[tokio::test]
    async fn block_hash_logs_come_from_the_requested_block() {
        // Audit JSON-RPC-6: `blockHash` was resolved to a number and the logs
        // were read by number, so a reorg in between, or history holding
        // another block at that height, answered with another block's logs.
        let (mut state, store, _directory) = test_state_and_store().await;
        let retained = with_log(fixture_frame(7, BlockHash::ZERO), 7);
        store
            .store_recent_frame(&retained)
            .await
            .expect("recent frame");
        // Block 5 is canonical by its seeded hash; history serves another
        // block 5.
        let seeded = leani_primitives::BlockRef {
            number: BlockNumber(5),
            hash: BlockHash::new([0xab; 32]),
            parent_hash: BlockHash::ZERO,
            timestamp: 1_700_000_005,
        };
        store
            .store_canonical_anchor(ChainId(1), seeded, Finality::Finalized)
            .await
            .expect("canonical anchor");
        state.config.history = Some(test_history(vec![history_source(
            "historical-logs",
            BlockRange::single(BlockNumber(5)),
            CapabilitySet::of(Capability::Logs),
            0,
            vec![with_log(fixture_frame(5, BlockHash::ZERO), 5)],
        )]));
        let router = configured_router(state, false, |_| {});
        let logs = |hash: BlockHash| {
            rpc_call(
                "eth_getLogs",
                json!([{"blockHash": hex_bytes(hash.as_array())}]),
            )
        };
        let (_, body) = call(router.clone(), logs(retained.block.hash)).await;
        let body = body.expect("body");
        assert_eq!(
            body["result"][0]["blockHash"],
            hex_bytes(retained.block.hash.as_array()),
            "{body}"
        );
        assert_unavailable(
            call(router, logs(seeded.hash)).await.1,
            "block_hash_not_retained",
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn only_blocks_above_the_head_read_as_null() {
        // Audit M-A6: a block the node could not serve read as `null`, which
        // clients take for "no such block": a reorg checker concluded that a
        // block below the retained window had been reorged out.
        let unknown_hash = hex_bytes(&[0xee; 32]);
        let null = json!({"jsonrpc": "2.0", "id": 1, "result": null});
        let (state, store, _directory) = test_state_and_store().await;
        let router = configured_router(state.clone(), false, |_| {});
        // With nothing retained, not even `latest` reads as null.
        assert_unavailable(
            call(
                router.clone(),
                rpc_call("eth_getBlockByNumber", json!(["latest", false])),
            )
            .await
            .1,
            "block_not_retained",
        );
        for number in [8, 9] {
            store
                .store_recent_frame(&rpc_frame(number))
                .await
                .expect("recent frame");
        }
        for (method, params) in [
            ("eth_getBlockByNumber", json!(["0x3", false])),
            ("eth_getBlockByHash", json!([unknown_hash, false])),
            ("eth_getBlockReceipts", json!(["0x3"])),
            ("eth_getBlockReceipts", json!([unknown_hash])),
            ("eth_getBlockTransactionCountByNumber", json!(["0x3"])),
            ("eth_getBlockTransactionCountByHash", json!([unknown_hash])),
            (
                "eth_getTransactionByBlockNumberAndIndex",
                json!(["0x3", "0x0"]),
            ),
            (
                "eth_getTransactionByBlockHashAndIndex",
                json!([unknown_hash, "0x0"]),
            ),
        ] {
            assert_unavailable(
                call(router.clone(), rpc_call(method, params)).await.1,
                "block_not_retained",
            );
        }
        for (method, params) in [
            ("eth_getBlockByNumber", json!(["0xa", false])),
            ("eth_getBlockReceipts", json!(["0xa"])),
            ("eth_getBlockTransactionCountByNumber", json!(["0xa"])),
            (
                "eth_getTransactionByBlockNumberAndIndex",
                json!(["0xa", "0x0"]),
            ),
        ] {
            let (_, body) = call(router.clone(), rpc_call(method, params)).await;
            assert_eq!(body, Some(null.clone()), "{method}");
        }

        // History without a hash lookup cannot resolve a hash either. A
        // number history cannot serve reads as null only above the head.
        let mut with_history = state;
        with_history.config.history = Some(test_history(vec![history_source(
            "numbered-history",
            BlockRange::single(BlockNumber(1)),
            rpc_block_capabilities(),
            0,
            Vec::new(),
        )]));
        let router = configured_router(with_history, false, |_| {});
        assert_unavailable(
            call(
                router.clone(),
                rpc_call("eth_getBlockByHash", json!([unknown_hash, false])),
            )
            .await
            .1,
            "block_not_retained",
        );
        let (_, body) = call(
            router.clone(),
            rpc_call("eth_getBlockByNumber", json!(["0xa", false])),
        )
        .await;
        assert_eq!(body, Some(null.clone()));
        assert_unavailable(
            call(
                router,
                rpc_call("eth_getBlockByNumber", json!(["0x3", false])),
            )
            .await
            .1,
            "no_viable_historical_source",
        );

        // A block the progress processor committed exists, although no
        // frame is retained.
        let (router, _cursor_directory) = router_with_cursor_only(20).await;
        assert_unavailable(
            call(
                router.clone(),
                rpc_call("eth_getBlockByNumber", json!([hex_quantity(20), false])),
            )
            .await
            .1,
            "block_not_retained",
        );
        let (_, body) = call(
            router,
            rpc_call("eth_getBlockByNumber", json!([hex_quantity(21), false])),
        )
        .await;
        assert_eq!(body, Some(null));
    }

    #[tokio::test]
    async fn empty_topic_alternatives_match_any_topic() {
        // Audit M-A7: `[]` at a topic position matched nothing; geth and reth
        // read it as a wildcard, like `null`.
        let (router, store, _directory) = test_router_and_store().await;
        store
            .store_recent_frame(&frame_with_logs(7, BlockHash::ZERO, 2, 1))
            .await
            .expect("recent frame");
        for topics in [
            json!([[]]),
            json!([null]),
            json!([[hex_bytes(&[0x22; 32])]]),
        ] {
            let (_, body) = call(
                router.clone(),
                rpc_call(
                    "eth_getLogs",
                    json!([{"fromBlock": "0x7", "toBlock": "0x7", "topics": topics}]),
                ),
            )
            .await;
            let body = body.expect("body");
            assert_eq!(
                body["result"].as_array().map(Vec::len),
                Some(2),
                "{topics}: {body}"
            );
        }
        let Subscription::Logs(filter) = parse_subscription(
            Some(&json!(["logs", {"topics": [[]]}])),
            &RpcConfig::default(),
        )
        .expect("log subscription") else {
            panic!("a log subscription");
        };
        let frame = frame_with_logs(7, BlockHash::ZERO, 1, 1);
        let logs = complete(&frame.logs, "logs").expect("logs");
        assert!(filter.matches(&logs[0]));
    }

    #[tokio::test]
    async fn a_log_query_past_the_response_budget_ends_the_batch() {
        // Task 13 review minor: `eth_getLogs` refused for the remaining
        // response budget answered -32005, but the calls after it still ran.
        let (state, store, _directory) = test_state_and_store().await;
        store
            .store_recent_frame(&frame_with_logs(7, BlockHash::ZERO, 3, 1_000))
            .await
            .expect("recent frame");
        let router = configured_router(state, false, |config| {
            config.max_response_bytes = 1_000;
        });
        let (_, body) = call(
            router,
            json!([
                rpc_call("eth_getLogs", json!([{"fromBlock": "0x7", "toBlock": "0x7"}])),
                {"jsonrpc": "2.0", "id": 2, "method": "eth_chainId"}
            ]),
        )
        .await;
        let body = body.expect("body");
        let responses = body.as_array().expect("batch");
        assert_eq!(
            responses[0]["error"]["data"]["reason"], "response_size_limit_exceeded",
            "{body}"
        );
        assert_eq!(responses[1]["id"], 2, "{body}");
        assert_eq!(responses[1]["error"]["code"], LIMIT_EXCEEDED, "{body}");
    }

    #[tokio::test]
    async fn blob_gas_priced_past_u128_fails_the_block_receipts() {
        // Task 9 review minor: a hostile header's excess blob gas prices blob
        // gas past `u128`; the block's receipts fail closed rather than carry
        // a clipped price.
        let (router, store, _directory) = test_router_and_store().await;
        let frame = blob_receipt_frame(1_710_338_135, 400_000_000);
        store
            .store_recent_frame(&frame)
            .await
            .expect("recent frame");
        assert_unavailable(
            Some(block_receipts(router, &frame).await),
            "quantity_exceeds_u128",
        );
    }

    /// A JSON-RPC 2.0 notification: a call of `method` without an ID.
    fn rpc_notification(method: &str, params: Value) -> Value {
        let mut notification = json!({"jsonrpc": "2.0", "method": method});
        notification["params"] = params;
        notification
    }

    #[tokio::test]
    async fn an_unsubscribe_notification_runs_without_a_response() {
        // Audit F6 probe `audit_probe_unsubscribe_notification_is_not_executed`,
        // inverted: a call without an ID was dropped before it ran, so an
        // `eth_unsubscribe` notification left its subscription active.
        let (state, _store, _directory) = test_state_and_store().await;
        let mut subscriptions = BTreeMap::from([("0x1".to_owned(), Subscription::NewHeads)]);
        let mut next = 2;
        let response = websocket_dispatch_value(
            &state,
            &mut subscriptions,
            &mut next,
            rpc_notification("eth_unsubscribe", json!(["0x1"])),
            state.config.max_response_bytes,
        )
        .await;
        assert!(response.is_none());
        assert!(subscriptions.is_empty(), "{subscriptions:?}");
    }

    #[tokio::test]
    async fn notifications_run_and_only_calls_are_answered() {
        // Audit F6: notifications were never run, in batches too.
        let (mut state, store, _directory) = test_state_and_store().await;
        let mut subscriptions = BTreeMap::from([
            ("0x1".to_owned(), Subscription::NewHeads),
            ("0x2".to_owned(), Subscription::NewHeads),
        ]);
        let mut next = 3;
        let batch = json!([
            rpc_notification("eth_unsubscribe", json!(["0x1"])),
            {"jsonrpc": "2.0", "id": 7, "method": "eth_chainId"},
            rpc_notification("eth_subscribe", json!(["newHeads"])),
        ]);
        let answered =
            websocket_call(&state, &mut subscriptions, &mut next, &batch.to_string()).await;
        assert_eq!(
            answered,
            json!([{"jsonrpc": "2.0", "id": 7, "result": "0x1"}])
        );
        assert_eq!(
            subscriptions.keys().collect::<Vec<_>>(),
            ["0x00000000000000000000000000000003", "0x2"]
        );
        // A message of notifications only runs them and gets no reply.
        for message in [
            rpc_notification("eth_unsubscribe", json!(["0x2"])),
            json!([rpc_notification(
                "eth_unsubscribe",
                json!(["0x00000000000000000000000000000003"])
            )]),
        ] {
            let reply = websocket_dispatch_text(
                &state,
                &mut subscriptions,
                &mut next,
                &message.to_string(),
            )
            .await;
            assert_eq!(reply, None, "{message}");
        }
        assert!(subscriptions.is_empty(), "{subscriptions:?}");
        // Once a call fills the response, later calls get -32005 without
        // running, but notifications, which need no room in it, still run.
        store
            .store_recent_frame(&frame_with_logs(7, BlockHash::ZERO, 3, 1_000))
            .await
            .expect("recent frame");
        state.config.max_response_bytes = 1_000;
        subscriptions.insert("0x4".to_owned(), Subscription::NewHeads);
        let batch = json!([
            rpc_call("eth_getLogs", json!([{"fromBlock": "0x7", "toBlock": "0x7"}])),
            rpc_notification("eth_unsubscribe", json!(["0x4"])),
            {"jsonrpc": "2.0", "id": 2, "method": "eth_chainId"},
        ]);
        let answered =
            websocket_call(&state, &mut subscriptions, &mut next, &batch.to_string()).await;
        let responses = answered.as_array().expect("batch");
        assert_eq!(responses.len(), 2, "{answered}");
        assert_eq!(responses[0]["error"]["code"], LIMIT_EXCEEDED, "{answered}");
        assert_eq!(responses[1]["id"], 2, "{answered}");
        assert_eq!(responses[1]["error"]["code"], LIMIT_EXCEEDED, "{answered}");
        assert!(subscriptions.is_empty(), "{subscriptions:?}");
    }

    #[tokio::test]
    async fn calls_must_be_objects() {
        // Task 15 review minor: serde reads a struct from an array of its
        // fields too, so `[["2.0", 1, "eth_chainId"]]` ran as a call.
        let (state, _store, _directory) = test_state_and_store().await;
        let refused = json!({
            "jsonrpc": "2.0",
            "id": null,
            "error": {"code": INVALID_REQUEST, "message": "Invalid Request"}
        });
        let router = configured_router(state.clone(), false, |_| {});
        let (_, body) = call(router, json!([["2.0", 1, "eth_chainId"]])).await;
        assert_eq!(body, Some(json!([refused.clone()])));
        let answered = websocket_call(
            &state,
            &mut BTreeMap::new(),
            &mut 1,
            r#"[["2.0", 1, "eth_chainId"], ["2.0", "eth_chainId"]]"#,
        )
        .await;
        assert_eq!(answered, json!([refused.clone(), refused]));
    }

    #[test]
    fn only_a_typed_budget_stop_ends_the_batch() {
        // Task 15 review minor: the encoder took any error whose
        // `data.reason` read `response_size_limit_exceeded` for a call that
        // stopped at the response budget, and ended the batch there.
        let (mut encoder, _) = ResponseEncoder::for_message(
            br#"[{"jsonrpc":"2.0","id":1,"method":"eth_chainId"},{"jsonrpc":"2.0","id":2,"method":"eth_chainId"}]"#,
            &RpcConfig::default(),
        )
        .expect("batch");
        encoder.push(RpcResponse::error(
            json!(1),
            LIMIT_EXCEEDED,
            "Limit exceeded",
            Some(json!({"reason": RESPONSE_SIZE_LIMIT_EXCEEDED, "limit": 1})),
        ));
        encoder.push(RpcResponse::success(json!(2), json!("0x1")));
        let body: Value =
            serde_json::from_str(&encoder.finish().expect("body")).expect("JSON body");
        assert_eq!(body[1], json!({"jsonrpc": "2.0", "id": 2, "result": "0x1"}));
    }

    /// Read `socket` until the server closes it, returning the close frame
    /// and the bytes of the text messages before it.
    async fn read_until_closed<S>(
        socket: &mut S,
    ) -> (tokio_tungstenite::tungstenite::protocol::CloseFrame, usize)
    where
        S: futures::Stream<Item = Result<ClientMessage, tokio_tungstenite::tungstenite::Error>>
            + Unpin,
    {
        let mut received = 0;
        loop {
            let message = tokio::time::timeout(Duration::from_secs(5), socket.next())
                .await
                .unwrap_or_else(|_| {
                    panic!("still open after {received} bytes of messages and 5 idle seconds")
                });
            match message {
                Some(Ok(ClientMessage::Text(text))) => received += text.len(),
                Some(Ok(ClientMessage::Close(Some(frame)))) => return (frame, received),
                other => panic!("expected a close frame after {received} bytes: {other:?}"),
            }
        }
    }

    /// Connect a WebSocket client with a small receive buffer, which stops
    /// taking data soon after it stops reading.
    async fn small_buffer_websocket(
        address: std::net::SocketAddr,
    ) -> tokio_tungstenite::WebSocketStream<tokio::net::TcpStream> {
        let socket = tokio::net::TcpSocket::new_v4().expect("socket");
        socket
            .set_recv_buffer_size(4_096)
            .expect("receive buffer size");
        let stream = socket.connect(address).await.expect("TCP connection");
        tokio_tungstenite::client_async(format!("ws://{address}/"), stream)
            .await
            .expect("WebSocket connection")
            .0
    }

    /// A subscription to every log.
    fn subscribe_to_logs() -> String {
        json!({"jsonrpc": "2.0", "id": 1, "method": "eth_subscribe", "params": ["logs", {}]})
            .to_string()
    }

    #[tokio::test]
    async fn subscription_output_per_chain_event_stays_within_its_budget() {
        // Audit F2 probe `audit_probe_subscription_output_bypasses_limits`,
        // inverted: with responses capped at 1 KiB and `eth_getLogs` at one
        // log, 128 subscriptions to every log turned one block with two
        // 1 KiB logs into 256 notifications of 647,648 bytes.
        let (state, _store, _directory) = test_state_and_store().await;
        let events = state.committed_events.clone();
        let budget = 64 * 1_024;
        let (address, server) = serve(configured_router(state, true, |config| {
            config.max_response_bytes = 1_024;
            config.max_log_results = 1;
            config.max_subscription_event_bytes = budget;
        }))
        .await;
        let mut greedy = websocket_connect(address, None).await.expect("connect");
        for _ in 0..DEFAULT_MAX_SUBSCRIPTIONS_PER_CONNECTION {
            let subscribed = websocket_round_trip(&mut greedy, subscribe_to_logs()).await;
            assert!(subscribed["result"].is_string(), "{subscribed}");
        }
        let mut modest = websocket_connect(address, None).await.expect("connect");
        let subscription =
            websocket_round_trip(&mut modest, subscribe_to_logs()).await["result"].clone();
        events
            .send(ChainEvent::Block(Box::new(frame_with_logs(
                1,
                BlockHash::ZERO,
                2,
                1_024,
            ))))
            .expect("publish the block");

        // The greedy connection is closed, as a policy violation naming the
        // limit, with nothing past the budget sent.
        let (close, received) = read_until_closed(&mut greedy).await;
        assert!(received <= budget, "{received} bytes sent");
        assert_eq!(u16::from(close.code), 1008, "{close:?}");
        assert!(
            close.reason.contains("rpc.max_subscription_event_bytes"),
            "{close:?}"
        );
        // The other connection gets its two notifications and stays open.
        for index in 0..2 {
            let notification = modest.next().await.expect("notification").expect("valid");
            let notification: Value =
                serde_json::from_str(notification.to_text().expect("text")).expect("JSON");
            assert_eq!(notification["params"]["subscription"], subscription);
            assert_eq!(
                notification["params"]["result"]["logIndex"],
                hex_quantity(index)
            );
        }
        let chain_id = json!({"jsonrpc": "2.0", "id": 2, "method": "eth_chainId"});
        assert_eq!(
            websocket_round_trip(&mut modest, chain_id.to_string()).await["result"],
            "0x1"
        );
        server.abort();
    }

    #[tokio::test]
    async fn a_client_that_stops_reading_is_closed_before_its_output_grows_past_the_budget() {
        // Audit F2: a connection that stopped reading held its slot, and
        // every notification it could not take, for good.
        let (mut state, _store, _directory) = test_state_and_store().await;
        state.committed_events = broadcast::channel(1_024).0;
        let events = state.committed_events.clone();
        let (address, server) = serve(configured_router(state, true, |config| {
            config.max_websocket_connections = 1;
            config.max_subscription_event_bytes = 256 * 1_024;
        }))
        .await;
        let mut stalled = small_buffer_websocket(address).await;
        let subscribed = websocket_round_trip(&mut stalled, subscribe_to_logs()).await;
        assert!(subscribed["result"].is_string(), "{subscribed}");
        // About 40 KiB of notifications per block, none of them read.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        let mut number = 1;
        let _replacement = loop {
            // Fails once the stalled connection is closed and no longer
            // receives blocks.
            let _ = events.send(ChainEvent::Block(Box::new(frame_with_logs(
                number,
                BlockHash::ZERO,
                16,
                1_024,
            ))));
            number += 1;
            match websocket_connect(address, None).await {
                Ok(socket) => break socket,
                Err(error) => {
                    assert_eq!(refused_handshake_status(&error), 503);
                    assert!(
                        tokio::time::Instant::now() < deadline,
                        "the stalled connection still holds its slot after {number} blocks"
                    );
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        };
        drop(stalled);
        server.abort();
    }

    #[tokio::test]
    async fn a_disconnect_frees_the_connection_while_its_messages_wait() {
        // A client that disconnects while its messages wait to be sent, and
        // while a call of it waits for room for its response, frees its
        // connection slot.
        let (mut state, _store, _directory) = test_state_and_store().await;
        state.committed_events = broadcast::channel(1_024).0;
        let events = state.committed_events.clone();
        let (address, server) = serve(configured_router(state, true, |config| {
            config.max_websocket_connections = 1;
            config.max_subscription_event_bytes = 64 * 1_024 * 1_024;
        }))
        .await;
        let mut stalled = small_buffer_websocket(address).await;
        let subscribed = websocket_round_trip(&mut stalled, subscribe_to_logs()).await;
        assert!(subscribed["result"].is_string(), "{subscribed}");
        // About 16 MiB of notifications, more than the connection can buffer.
        for number in 1..=400 {
            events
                .send(ChainEvent::Block(Box::new(frame_with_logs(
                    number,
                    BlockHash::ZERO,
                    16,
                    1_024,
                ))))
                .expect("publish a block");
            tokio::task::yield_now().await;
        }
        let chain_id = json!({"jsonrpc": "2.0", "id": 2, "method": "eth_chainId"}).to_string();
        for _ in 0..2 {
            stalled
                .send(ClientMessage::Text(chain_id.clone().into()))
                .await
                .expect("send a call");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        drop(stalled);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            match websocket_connect(address, None).await {
                Ok(_) => break,
                Err(error) => {
                    assert_eq!(refused_handshake_status(&error), 503);
                    assert!(
                        tokio::time::Instant::now() < deadline,
                        "the closed connection still holds its slot"
                    );
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        }
        server.abort();
    }

    /// Take the notifications waiting in `queue`, returning their encoded
    /// bytes.
    fn send_all(queue: &mut mpsc::UnboundedReceiver<Queued>) -> usize {
        let mut bytes = 0;
        while let Ok((message, _room)) = queue.try_recv() {
            let Message::Text(text) = message.into_message() else {
                panic!("notifications are text messages");
            };
            bytes += text.len();
        }
        bytes
    }

    /// A connection's only subscription, to every log.
    fn every_log(config: &RpcConfig) -> BTreeMap<String, Subscription> {
        let filter = parse_log_filter(Some(&json!([{}])), config).expect("filter");
        BTreeMap::from([("0x1".to_owned(), Subscription::Logs(filter))])
    }

    #[tokio::test]
    async fn a_log_is_encoded_once_for_every_subscription_it_matches() {
        // Audit F2: 128 subscriptions to every log encoded each log 128
        // times, all at once.
        let (state, _store, _directory) = test_state_and_store().await;
        let Subscription::Logs(filter) = every_log(&state.config)["0x1"].clone() else {
            panic!("a log subscription");
        };
        let mut subscriptions = (0..128_u64)
            .map(|id| (format!("0x{id:032x}"), Subscription::Logs(filter.clone())))
            .collect::<BTreeMap<_, _>>();
        subscriptions.insert("0xff".to_owned(), Subscription::NewHeads);
        subscriptions.insert("0xfe".to_owned(), Subscription::NewHeads);
        let mut block = rpc_frame(9);
        block.logs = frame_with_logs(9, BlockHash::ZERO, 2, 1_024).logs;
        let (outbox, mut queue) = Outbox::new(DEFAULT_MAX_SUBSCRIPTION_EVENT_BYTES);
        queue_notifications(
            &state,
            &subscriptions,
            &ChainEvent::Block(Box::new(block)),
            &outbox,
        )
        .await
        .expect("notifications");
        let mut results = Vec::new();
        while let Ok((message, _room)) = queue.try_recv() {
            let Outgoing::Notification { result, .. } = message else {
                panic!("a notification");
            };
            results.push(result);
        }
        // The head's two notifications come first, then the logs', each
        // notification of a head or log sharing one encoding of it.
        assert_eq!(results.len(), 2 + 2 * 128);
        for shared in [&results[..2], &results[2..130], &results[130..]] {
            assert!(shared.iter().all(|result| Arc::ptr_eq(result, &shared[0])));
        }
        assert!(!Arc::ptr_eq(&results[2], &results[130]));
    }

    #[tokio::test]
    async fn notifications_past_the_room_in_the_queue_close_the_connection() {
        // Audit F2: notifications a connection could not take piled up
        // without a bound.
        let (state, _store, _directory) = test_state_and_store().await;
        let subscriptions = every_log(&state.config);
        let block = |number| {
            ChainEvent::Block(Box::new(frame_with_logs(number, BlockHash::ZERO, 2, 1_024)))
        };
        let (outbox, mut queue) = Outbox::new(usize::MAX);
        queue_notifications(&state, &subscriptions, &block(1), &outbox)
            .await
            .expect("notifications");
        let event = send_all(&mut queue);

        // Room for one and a half blocks: a second block's notifications
        // find too little room while the first's are unsent, and never
        // take more room than there is.
        let limit = event + event / 2;
        let (outbox, mut queue) = Outbox::new(limit);
        queue_notifications(&state, &subscriptions, &block(2), &outbox)
            .await
            .expect("room for one block");
        let close = queue_notifications(&state, &subscriptions, &block(3), &outbox)
            .await
            .expect_err("too little room for another");
        assert_eq!(close.code, close_code::AGAIN);
        assert!(
            close
                .reason
                .as_str()
                .contains("rpc.max_subscription_event_bytes"),
            "{close:?}"
        );
        assert!(send_all(&mut queue) <= limit);
        // Sent notifications free their room.
        queue_notifications(&state, &subscriptions, &block(4), &outbox)
            .await
            .expect("room once sent");
        assert_eq!(send_all(&mut queue), event);

        // One event whose notifications pass the limit by themselves.
        let (outbox, _queue) = Outbox::new(event - 1);
        let close = queue_notifications(&state, &subscriptions, &block(5), &outbox)
            .await
            .expect_err("past the budget");
        assert_eq!(close.code, close_code::POLICY);
        // Both reasons fit a close frame, whose reason takes at most 123
        // bytes, for any limit.
        for close in [
            event_budget_close(usize::MAX),
            slow_client_close(usize::MAX),
        ] {
            assert!(close.reason.as_str().len() <= 123, "{close:?}");
        }
    }
}
