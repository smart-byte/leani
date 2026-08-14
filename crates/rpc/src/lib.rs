//! Capability-aware Ethereum JSON-RPC facade.
//!
//! The facade deliberately refuses methods whose canonical Ethereum response
//! cannot be reconstructed from retained material. This is safer than
//! manufacturing partial blocks or receipts that look like complete RPC data.

use std::{
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
use alloy_eips::{calc_blob_gasprice, eip2718::Decodable2718};
use alloy_primitives::{B256, U256};
use alloy_rlp::Decodable;
use alloy_rpc_types_eth::{
    Block as RpcBlock, BlockTransactions, Header as RpcHeader, Log as RpcLog,
    Transaction as RpcTransaction, TransactionReceipt as RpcTransactionReceipt, Withdrawal,
    Withdrawals,
};
use axum::{
    Json, Router,
    extract::{
        DefaultBodyLimit, State,
        ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade, close_code},
    },
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use futures::StreamExt;
use leani_primitives::{
    Address, BlockFrame, BlockHash, BlockNumber, BlockRange, Capability, CapabilitySet, ChainId,
    FilterScope, Finality, Material, TopicFilter, TransactionHash, TrustModel,
};
use leani_processor_api::{Processor, ProcessorDescriptor, StartPoint};
use leani_processor_blobs::{BlobFork, BlobSchedule, BlobsProcessor};
use leani_source_api::{
    ChainEvent, DataRequest, FieldProjection, FilterSet, HistorySource, SelectionPolicy,
    SourceBudget, SourceError, VerificationPolicy, select_source,
};
use leani_store_sqlite::{SqliteStore, StoreError};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use thiserror::Error;
use tokio::sync::{Semaphore, broadcast};
use tokio_util::sync::CancellationToken;

const JSONRPC_VERSION: &str = "2.0";
const PARSE_ERROR: i64 = -32_700;
const INVALID_REQUEST: i64 = -32_600;
const METHOD_NOT_FOUND: i64 = -32_601;
const INVALID_PARAMS: i64 = -32_602;
const INTERNAL_ERROR: i64 = -32_603;
const DATA_UNAVAILABLE: i64 = -32_004;

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
///
/// # Errors
///
/// Fails closed when canonical header, transaction, receipt, withdrawal, or
/// signer material is absent, invalid, or internally inconsistent.
pub fn rpc_compatibility_snapshot(
    frame: &BlockFrame,
) -> Result<RpcCompatibilitySnapshot, RpcCompatibilityError> {
    let block_hashes = rpc_block(frame, false).map_err(RpcCompatibilityError::from)?;
    let block_full = rpc_block(frame, true).map_err(RpcCompatibilityError::from)?;
    let receipts = rpc_receipts(frame)
        .and_then(serialize_rpc)
        .map_err(RpcCompatibilityError::from)?;
    Ok(RpcCompatibilitySnapshot {
        block_hashes,
        block_full,
        receipts,
    })
}

/// RPC transport and readiness settings.
#[derive(Clone, Debug)]
pub struct RpcConfig {
    pub chain_id: ChainId,
    pub max_request_bytes: usize,
    pub readiness: RpcReadiness,
    pub max_log_range: u64,
    pub websocket_enabled: bool,
    pub history: Option<HistoricalRpc>,
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
    let state = RpcState {
        store,
        progress,
        blob_schedule,
        config,
        committed_events,
    };
    let route = if websocket {
        get(websocket_upgrade)
    } else {
        post(handle).get(health)
    };
    Router::new()
        .route("/", route)
        .layer(DefaultBodyLimit::max(max_request_bytes))
        .with_state(state)
}

async fn health() -> Json<Value> {
    Json(json!({
        "service": "leani-json-rpc",
        "jsonrpc": "2.0"
    }))
}

async fn websocket_upgrade(State(state): State<RpcState>, upgrade: WebSocketUpgrade) -> Response {
    upgrade
        .max_message_size(state.config.max_request_bytes)
        .on_upgrade(move |socket| websocket_session(socket, state))
}

#[derive(Clone, Debug)]
enum Subscription {
    NewHeads,
    Logs(ParsedLogFilter),
}

async fn websocket_session(mut socket: WebSocket, state: RpcState) {
    let mut events = state.committed_events.subscribe();
    let mut subscriptions = BTreeMap::new();
    let mut next_subscription = 1_u64;
    loop {
        tokio::select! {
            message = socket.next() => {
                let Some(message) = message else {
                    break;
                };
                let Ok(message) = message else {
                    break;
                };
                match message {
                    Message::Text(text) => {
                        let response = websocket_dispatch_text(
                            &state,
                            &mut subscriptions,
                            &mut next_subscription,
                            text.as_str(),
                        ).await;
                        if let Some(response) = response
                            && send_websocket_json(&mut socket, &response).await.is_err()
                        {
                            break;
                        }
                    }
                    Message::Ping(payload) => {
                        if socket.send(Message::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    Message::Pong(_) => {}
                    Message::Close(_) => break,
                    Message::Binary(_) => {
                        let _ = socket.send(Message::Close(Some(CloseFrame {
                            code: close_code::UNSUPPORTED,
                            reason: "JSON-RPC messages must be UTF-8 text".into(),
                        }))).await;
                        break;
                    }
                }
            }
            event = events.recv() => {
                match event {
                    Ok(event) => {
                        match websocket_event_messages(&state, &subscriptions, &event).await {
                            Ok(messages) => {
                                let mut failed = false;
                                for message in messages {
                                    if send_websocket_json(&mut socket, &message).await.is_err() {
                                        failed = true;
                                        break;
                                    }
                                }
                                if failed {
                                    break;
                                }
                            }
                            Err(error) => {
                                let reason = error
                                    .data
                                    .as_ref()
                                    .and_then(|data| data.get("reason"))
                                    .and_then(Value::as_str)
                                    .unwrap_or("subscription material unavailable");
                                let _ = socket.send(Message::Close(Some(CloseFrame {
                                    code: close_code::ERROR,
                                    reason: reason.to_owned().into(),
                                }))).await;
                                break;
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        let _ = socket.send(Message::Close(Some(CloseFrame {
                            code: close_code::AGAIN,
                            reason: "subscription event buffer overflow; reconnect".into(),
                        }))).await;
                        break;
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }
}

async fn send_websocket_json(
    socket: &mut WebSocket,
    value: &impl serde::Serialize,
) -> Result<(), axum::Error> {
    let encoded = serde_json::to_string(value).map_err(axum::Error::new)?;
    socket.send(Message::Text(encoded.into())).await
}

async fn websocket_dispatch_text(
    state: &RpcState,
    subscriptions: &mut BTreeMap<String, Subscription>,
    next_subscription: &mut u64,
    input: &str,
) -> Option<Value> {
    let parsed = match serde_json::from_str::<Value>(input) {
        Ok(value) => value,
        Err(error) => {
            return serde_json::to_value(RpcResponse::error(
                Value::Null,
                PARSE_ERROR,
                "Parse error",
                Some(json!({ "detail": error.to_string() })),
            ))
            .ok();
        }
    };
    if let Value::Array(requests) = parsed {
        if requests.is_empty() {
            return serde_json::to_value(RpcResponse::error(
                Value::Null,
                INVALID_REQUEST,
                "Invalid Request",
                None,
            ))
            .ok();
        }
        let mut responses = Vec::with_capacity(requests.len());
        for request in requests {
            if let Some(response) =
                websocket_dispatch_value(state, subscriptions, next_subscription, request).await
            {
                responses.push(response);
            }
        }
        (!responses.is_empty()).then_some(Value::Array(responses))
    } else {
        websocket_dispatch_value(state, subscriptions, next_subscription, parsed).await
    }
}

async fn websocket_dispatch_value(
    state: &RpcState,
    subscriptions: &mut BTreeMap<String, Subscription>,
    next_subscription: &mut u64,
    value: Value,
) -> Option<Value> {
    let request = match serde_json::from_value::<RpcRequest>(value) {
        Ok(request) if request.jsonrpc == JSONRPC_VERSION => request,
        _ => {
            return serde_json::to_value(RpcResponse::error(
                Value::Null,
                INVALID_REQUEST,
                "Invalid Request",
                None,
            ))
            .ok();
        }
    };
    let id = request.id?;
    let result = match request.method.as_str() {
        "eth_subscribe" => parse_subscription(request.params.as_ref()).map(|subscription| {
            let id = format!("0x{:032x}", *next_subscription);
            *next_subscription = next_subscription.saturating_add(1);
            subscriptions.insert(id.clone(), subscription);
            Value::String(id)
        }),
        "eth_unsubscribe" => parse_unsubscribe(request.params.as_ref())
            .map(|subscription| Value::Bool(subscriptions.remove(subscription).is_some())),
        method => dispatch(state, method, request.params).await,
    };
    let response = match result {
        Ok(result) => RpcResponse::success(id, result),
        Err(error) => RpcResponse::error(id, error.code, error.message, error.data),
    };
    serde_json::to_value(response).ok()
}

fn parse_subscription(params: Option<&Value>) -> Result<Subscription, RpcError> {
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
            parse_log_filter(Some(&Value::Array(vec![filter]))).map(Subscription::Logs)
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

async fn websocket_event_messages(
    state: &RpcState,
    subscriptions: &BTreeMap<String, Subscription>,
    event: &ChainEvent,
) -> Result<Vec<Value>, RpcError> {
    let mut messages = Vec::new();
    for (subscription_id, subscription) in subscriptions {
        let results = subscription_results(state, subscription, event).await?;
        messages.extend(results.into_iter().map(|result| {
            json!({
                "jsonrpc": JSONRPC_VERSION,
                "method": "eth_subscription",
                "params": {
                    "subscription": subscription_id,
                    "result": result
                }
            })
        }));
    }
    Ok(messages)
}

async fn subscription_results(
    state: &RpcState,
    subscription: &Subscription,
    event: &ChainEvent,
) -> Result<Vec<Value>, RpcError> {
    match (subscription, event) {
        (Subscription::NewHeads, ChainEvent::Block(frame)) => {
            Ok(vec![serialize_rpc(rpc_header(frame)?)?])
        }
        (Subscription::NewHeads, ChainEvent::Reorg { applied, .. }) => applied
            .iter()
            .map(|frame| rpc_header(frame).and_then(serialize_rpc))
            .collect(),
        (Subscription::Logs(filter), ChainEvent::Block(frame)) => {
            subscription_log_values(frame, filter, false)
        }
        (Subscription::Logs(filter), ChainEvent::Reorg { reverted, applied }) => {
            let mut results = Vec::new();
            for block in reverted {
                let frame = state
                    .store
                    .recent_frame_by_hash(state.config.chain_id, block.hash)
                    .await
                    .map_err(|error| RpcError::store(&error))?
                    .ok_or_else(|| {
                        RpcError::data_unavailable_reason("reverted_subscription_frame_missing")
                    })?;
                results.extend(subscription_log_values(&frame, filter, true)?);
            }
            for frame in applied {
                results.extend(subscription_log_values(frame, filter, false)?);
            }
            Ok(results)
        }
        (_, ChainEvent::Disconnected { .. }) => Err(RpcError::data_unavailable_reason(
            "live_subscription_disconnected",
        )),
        (_, ChainEvent::Reset { .. }) => {
            Err(RpcError::data_unavailable_reason("live_subscription_reset"))
        }
    }
}

async fn handle(State(state): State<RpcState>, body: String) -> Response {
    let parsed = match serde_json::from_str::<Value>(&body) {
        Ok(value) => value,
        Err(error) => {
            return Json(RpcResponse::error(
                Value::Null,
                PARSE_ERROR,
                "Parse error",
                Some(json!({ "detail": error.to_string() })),
            ))
            .into_response();
        }
    };
    if let Value::Array(requests) = parsed {
        if requests.is_empty() {
            return Json(RpcResponse::error(
                Value::Null,
                INVALID_REQUEST,
                "Invalid Request",
                None,
            ))
            .into_response();
        }
        let mut responses = Vec::with_capacity(requests.len());
        for value in requests {
            if let Some(response) = dispatch_value(&state, value).await {
                responses.push(response);
            }
        }
        if responses.is_empty() {
            StatusCode::NO_CONTENT.into_response()
        } else {
            Json(responses).into_response()
        }
    } else {
        match dispatch_value(&state, parsed).await {
            Some(response) => Json(response).into_response(),
            None => StatusCode::NO_CONTENT.into_response(),
        }
    }
}

async fn dispatch_value(state: &RpcState, value: Value) -> Option<RpcResponse> {
    let request = match serde_json::from_value::<RpcRequest>(value) {
        Ok(request) if request.jsonrpc == JSONRPC_VERSION => request,
        _ => {
            return Some(RpcResponse::error(
                Value::Null,
                INVALID_REQUEST,
                "Invalid Request",
                None,
            ));
        }
    };
    let id = request.id?;
    Some(
        match dispatch(state, &request.method, request.params).await {
            Ok(result) => RpcResponse::success(id, result),
            Err(error) => RpcResponse::error(id, error.code, error.message, error.data),
        },
    )
}

async fn dispatch(
    state: &RpcState,
    method: &str,
    params: Option<Value>,
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
        "eth_getLogs" => eth_get_logs(state, params).await,
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
    let cursor = if head_timestamp.is_none() {
        state
            .store
            .processor_cursor(state.progress.descriptor())
            .await
            .map_err(|error| RpcError::store(&error))?
    } else {
        None
    };
    let current = head_timestamp
        .and_then(|timestamp| schedule.fork_at_timestamp(timestamp))
        .or_else(|| cursor.and_then(|cursor| schedule.fork_at_block(cursor.block_number.0)))
        // An empty node still knows its checked, current chain configuration.
        .or_else(|| schedule.forks.last())
        .ok_or_else(|| RpcError::data_unavailable_reason("eth_config_schedule_empty"))?;
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
    serialize_rpc(rpc_receipts(&frame)?)
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
    let receipts = rpc_receipts(&frame)?;
    receipts
        .into_iter()
        .nth(index)
        .ok_or_else(|| RpcError::data_unavailable_reason("transaction_receipt_not_retained"))
        .and_then(serialize_rpc)
}

#[derive(Clone, Copy, Debug)]
enum BlockSelector {
    Latest,
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
        "earliest" | "pending" | "safe" | "finalized" => Err(RpcError::invalid_params(
            "this block tag is unavailable for the retained recent window",
        )),
        value => parse_hex_quantity(value)
            .map(BlockNumber)
            .map(BlockSelector::Number),
    }
}

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
        BlockSelector::Number(number) => (Some(number), true),
        BlockSelector::Hash(_) => {
            return Err(RpcError::invalid_params(
                "hash selector is invalid for a canonical number lookup",
            ));
        }
    };
    let Some(number) = number else {
        return Ok(None);
    };
    let recent = state
        .store
        .recent_frame(state.config.chain_id, number)
        .await
        .map_err(|error| RpcError::store(&error))?;
    if recent.is_some() || !on_demand {
        return Ok(recent);
    }
    let Some(history) = &state.config.history else {
        return Ok(None);
    };
    let mut frames = history
        .fetch(
            state.config.chain_id,
            BlockRange::single(number),
            required,
            FilterSet::default(),
        )
        .await
        .map_err(RpcError::history)?;
    Ok(frames.pop())
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
    let Some(history) = &state.config.history else {
        return Ok(None);
    };
    if !history.supports_block_hash_lookup(state.config.chain_id, required) {
        return Ok(None);
    }
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

fn rpc_receipts(frame: &BlockFrame) -> Result<Vec<RpcTransactionReceipt>, RpcError> {
    let transactions = complete(&frame.transactions, "complete_transactions_not_retained")?;
    let receipts = complete(&frame.receipts, "complete_receipts_not_retained")?;
    if transactions.len() != receipts.len() {
        return Err(RpcError::data_unavailable_reason(
            "transaction_receipt_count_mismatch",
        ));
    }
    let header = complete(&frame.header, "complete_header_not_retained")?;
    let blob_gas_price = header.excess_blob_gas.map(calc_blob_gasprice);
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

async fn eth_get_logs(state: &RpcState, params: Option<Value>) -> Result<Value, RpcError> {
    let filter = parse_log_filter(params.as_ref())?;
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
        let from = filter.from.or(latest).ok_or_else(|| {
            RpcError::data_unavailable_reason("latest_block_unavailable_for_open_log_range")
        })?;
        let to = filter.to.or(latest).ok_or_else(|| {
            RpcError::data_unavailable_reason("latest_block_unavailable_for_open_log_range")
        })?;
        (from, to)
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
    let mut output = Vec::new();
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
            output.push(rpc_log_value(&frame, log, false)?);
        }
    }
    Ok(Value::Array(output))
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

fn subscription_log_values(
    frame: &BlockFrame,
    filter: &ParsedLogFilter,
    removed: bool,
) -> Result<Vec<Value>, RpcError> {
    let logs = complete(&frame.logs, "complete_logs_not_retained")?;
    logs.iter()
        .filter(|log| filter.matches(log))
        .map(|log| rpc_log_value(frame, log, removed))
        .collect()
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

#[derive(Clone, Debug)]
struct ParsedLogFilter {
    from: Option<BlockNumber>,
    to: Option<BlockNumber>,
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

fn parse_log_filter(params: Option<&Value>) -> Result<ParsedLogFilter, RpcError> {
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
        .map(parse_log_block)
        .transpose()?
        .flatten();
    let to = object
        .get("toBlock")
        .map(parse_log_block)
        .transpose()?
        .flatten();
    let addresses = object
        .get("address")
        .map(parse_addresses)
        .transpose()?
        .unwrap_or_default();
    let topics = object
        .get("topics")
        .map(parse_topics)
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

fn parse_log_block(value: &Value) -> Result<Option<BlockNumber>, RpcError> {
    let value = value
        .as_str()
        .ok_or_else(|| RpcError::invalid_params("block selector must be a string"))?;
    match value {
        "latest" => Ok(None),
        "earliest" | "pending" | "safe" | "finalized" => Err(RpcError::invalid_params(
            "this block tag is unavailable for bounded recent logs",
        )),
        value => parse_hex_quantity(value).map(BlockNumber).map(Some),
    }
}

fn parse_addresses(value: &Value) -> Result<Vec<Address>, RpcError> {
    match value {
        Value::String(value) => parse_fixed_hex::<20>(value)
            .map(Address::new)
            .map(|value| vec![value]),
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

fn parse_topics(value: &Value) -> Result<Vec<Option<Vec<[u8; 32]>>>, RpcError> {
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
            Value::String(topic) => parse_fixed_hex::<32>(topic).map(|topic| Some(vec![topic])),
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
    if digits.is_empty() || (digits.len() > 1 && digits.starts_with('0')) {
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
    #[serde(default)]
    id: Option<Value>,
    method: String,
    #[serde(default)]
    params: Option<Value>,
}

#[derive(Debug, serde::Serialize)]
struct RpcResponse {
    jsonrpc: &'static str,
    id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<RpcErrorBody>,
}

impl RpcResponse {
    fn success(id: Value, result: Value) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION,
            id,
            result: Some(result),
            error: None,
        }
    }

    fn error(id: Value, code: i64, message: &'static str, data: Option<Value>) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION,
            id,
            result: None,
            error: Some(RpcErrorBody {
                code,
                message,
                data,
            }),
        }
    }
}

#[derive(Debug, serde::Serialize)]
struct RpcErrorBody {
    code: i64,
    message: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<Value>,
}

#[derive(Debug)]
struct RpcError {
    code: i64,
    message: &'static str,
    data: Option<Value>,
}

impl From<RpcError> for RpcCompatibilityError {
    fn from(error: RpcError) -> Self {
        let reason = error
            .data
            .as_ref()
            .and_then(|data| data.get("reason").or_else(|| data.get("detail")))
            .and_then(Value::as_str)
            .unwrap_or(error.message)
            .to_owned();
        Self { reason }
    }
}

impl RpcError {
    fn method_not_found() -> Self {
        Self {
            code: METHOD_NOT_FOUND,
            message: "Method not found",
            data: None,
        }
    }

    fn invalid_params(detail: &'static str) -> Self {
        Self {
            code: INVALID_PARAMS,
            message: "Invalid params",
            data: Some(json!({ "detail": detail })),
        }
    }

    fn data_unavailable(method: &str) -> Self {
        Self {
            code: DATA_UNAVAILABLE,
            message: "Data unavailable",
            data: Some(json!({
                "method": method,
                "reason": "raw_execution_material_not_retained",
                "retryable": false
            })),
        }
    }

    fn data_unavailable_reason(reason: &'static str) -> Self {
        Self {
            code: DATA_UNAVAILABLE,
            message: "Data unavailable",
            data: Some(json!({
                "reason": reason,
                "retryable": false
            })),
        }
    }

    fn store(error: &StoreError) -> Self {
        Self {
            code: INTERNAL_ERROR,
            message: "Internal error",
            data: Some(json!({ "retryable": true, "detail": error.to_string() })),
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
            message: "Data unavailable",
            data: Some(json!({
                "reason": reason,
                "retryable": retryable
            })),
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
        let state = RpcState {
            store: store.clone(),
            progress,
            blob_schedule: Arc::new(blobs.schedule().clone()),
            config: RpcConfig::default(),
            committed_events,
        };
        (state, store, directory)
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
        let subscribed = websocket_dispatch_text(
            &state,
            &mut subscriptions,
            &mut next,
            r#"{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newHeads"]}"#,
        )
        .await
        .expect("subscription response");
        assert_eq!(subscribed["result"], "0x00000000000000000000000000000001");
        assert_eq!(subscriptions.len(), 1);
        let unsubscribed = websocket_dispatch_text(
            &state,
            &mut subscriptions,
            &mut next,
            r#"{"jsonrpc":"2.0","id":2,"method":"eth_unsubscribe","params":["0x00000000000000000000000000000001"]}"#,
        )
        .await
        .expect("unsubscribe response");
        assert_eq!(unsubscribed["result"], true);
        assert!(subscriptions.is_empty());
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
        let subscription = parse_subscription(Some(&json!([
            "logs",
            {
                "address": hex_bytes(address.as_array()),
                "topics": [hex_bytes(&[0x31; 32])]
            }
        ])))
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
        let error = parse_subscription(Some(&json!([
            "logs",
            {"fromBlock": "0x1"}
        ])))
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
    async fn eth_config_uses_current_checked_schedule_when_the_store_is_empty() {
        let (router, _directory) = test_router().await;
        let (_, body) = call(
            router.clone(),
            json!({"jsonrpc":"2.0","id":1,"method":"eth_config"}),
        )
        .await;
        let result = body.expect("body")["result"].clone();
        assert_eq!(result["current"]["activationTime"], 1_767_747_671_u64);
        assert_eq!(result["current"]["forkId"], "0x07c9462e");
        assert_eq!(result["current"]["blobSchedule"]["target"], 14);
        assert_eq!(
            result["current"]["precompiles"]["P256VERIFY"],
            "0x0000000000000000000000000000000000000100"
        );
        assert!(result["next"].is_null());
        assert!(result["last"].is_null());

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
        let actual = rpc_compatibility_snapshot(&rpc_frame(9)).expect("canonical snapshot");
        let expected: RpcCompatibilitySnapshot = serde_json::from_str(include_str!(
            "../fixtures/ethereum-jsonrpc-empty-block.json"
        ))
        .expect("compatibility fixture");
        assert_eq!(actual, expected);
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
        let header = ConsensusHeader {
            number,
            timestamp: 1_700_000_000 + number,
            ..Default::default()
        };
        let hash = header.hash_slow();
        let mut frame = fixture_frame(number, BlockHash::ZERO);
        frame.block.hash = BlockHash::new(hash.0);
        frame.header = Material::Complete(leani_primitives::HeaderEnvelope {
            rlp: Some(alloy_rlp::encode(&header)),
            transactions_root: Some(BlockHash::new(header.transactions_root.0)),
            receipts_root: Some(BlockHash::new(header.receipts_root.0)),
            withdrawals_root: header.withdrawals_root.map(|root| BlockHash::new(root.0)),
            gas_limit: Some(header.gas_limit),
            gas_used: Some(header.gas_used),
            base_fee_per_gas: header.base_fee_per_gas.map(U256::from).map(Into::into),
            blob_gas_used: header.blob_gas_used,
            excess_blob_gas: header.excess_blob_gas,
            size_bytes: Some(u64::try_from(alloy_rlp::encode(&header).len()).unwrap_or(u64::MAX)),
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
}
