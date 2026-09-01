//! Recent Ethereum body and receipt ingestion through isolated Reth P2P crates.
//!
//! This crate deliberately depends on no Reth database, EVM, RPC, or node
//! builder. It starts only the networking manager with a no-op provider,
//! requests a bounded fixed range, verifies all response commitments, and
//! converts the result into source-neutral [`BlockFrame`] values.

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt as _;
use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    fs::OpenOptions,
    future::Future,
    io::Write as _,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use alloy_consensus::{
    Block, Header, Transaction as _, TxReceipt as _,
    proofs::{calculate_receipt_root, calculate_transaction_root},
    transaction::SignerRecoverable,
};
use alloy_eips::{BlockHashOrNumber, Encodable2718, eip2124::Head};
use alloy_primitives::{Address as AlloyAddress, B256, B512, BloomInput, Sealable, U256};
use alloy_rlp::Encodable as _;
use async_trait::async_trait;
use futures::{StreamExt, stream, stream::FuturesUnordered};
use hickory_resolver::proto::rr::RData;
use leani_primitives::{
    Address, BlockFrame, BlockHash, BlockNumber, BlockRange, BlockRef, Capability, CapabilitySet,
    ChainId, Completeness, ConsensusAnchor, FilterScope, Finality, HeaderEnvelope, Log, Material,
    MissingReason, ObjectIdentity, Provenance, Quantity, ReceiptEnvelope, SourceId, SourceKind,
    TransactionEnvelope, TransactionHash, TrustModel, VerificationCheck, VerificationReport,
    Withdrawal,
};
use leani_source_api::{
    BlockFrameStream, ChainEvent, ChainEventStream, DataRequest, FinalityModel, HistorySource,
    LiveSource, LiveStart, NetworkDisconnectReason, NetworkLane, NetworkPhase,
    NetworkSessionTelemetry, NetworkTelemetry, NetworkTelemetrySnapshot, Partitioning,
    SourceAcquisitionMetrics, SourceBudget, SourceChunk, SourceDescriptor, SourceError, SourcePlan,
};
use reth_chainspec::MAINNET;
use reth_discv4::{DiscoveryUpdate, Discv4, Discv4Config};
use reth_dns_discovery::{
    DnsDiscoveryConfig, DnsDiscoveryService, Resolver as DnsDiscoveryResolver,
    tree::LinkEntry as DnsDiscoveryLink,
};
use reth_ethereum_primitives::{BlockBody, Receipt};
use reth_network::types::{
    EthVersion, GetBlockBodies, GetBlockHeaders, GetReceipts, GetReceipts70, NatResolver, PeerKind,
    ReputationChangeKind,
};
use reth_network::{
    DisconnectReason, EthNetworkPrimitives, FetchClient, NetworkConfigBuilder, NetworkEvent,
    NetworkEventListenerProvider, NetworkHandle, NetworkManager, PeerRequest, PeerRequestSender,
    Peers, PeersConfig, PeersInfo, SessionsConfig, config::rng_secret_key, events::PeerEvent,
};
#[cfg(test)]
use reth_network_p2p::headers::client::HeadersDirection;
use reth_network_p2p::{
    bodies::client::BodiesClient,
    download::DownloadClient,
    error::RequestError,
    headers::client::{HeadersClient, HeadersRequest},
    priority::Priority,
    receipts::client::ReceiptsClient,
};
use reth_network_peers::{NodeRecord, TrustedPeer};
use reth_tasks::Runtime;
use secp256k1::SecretKey;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace, warn};

/// Immutable Reth release used by this adapter.
pub const RETH_VERSION: &str = "2.4.1";
/// Immutable Reth commit used by this adapter.
pub const RETH_REVISION: &str = "8eb210175687c9f0c889a3b6795c16781d830e3a";
const MAX_FIXED_RANGE_BLOCKS: u64 = 64;
// ETH peers may serve at most 1,024 headers per response. Header-only proof
// acquisition is small enough to use that protocol limit; body and receipt
// requests remain independently bounded by their encoded response size.
const MAX_HISTORY_HEADER_REQUEST_BLOCKS: u64 = 1_024;
// A HistorySource budget applies to one `open`, so the bridge must expose
// bounded source chunks instead of one potentially multi-gigabyte gap. Each
// open still uses smaller fixed-range ETH requests internally.
const MAX_HISTORY_OPEN_BLOCKS: u64 = 256;
// Fetch enough blocks per verified material window to keep several peers busy,
// while retaining the ETH response-size-safe body/receipt sub-batches below.
const MAX_HISTORY_MATERIAL_WINDOW_BLOCKS: usize = 32;
const NETWORK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const MAINNET_DNS_DISCOVERY_TREE: &str =
    "enrtree://AKA3AM6LPBYEUDMVNU3BSVQJ5AD45Y7YPOHJLEF6W26QOE4VTUDPE@all.mainnet.ethdisco.net";
const MAINNET_DNS_DISCV4_BOOTSTRAP_PEERS: usize = 32;
const MAINNET_DNS_RETRY_INTERVAL: Duration = Duration::from_secs(15);
const PEER_QUALIFICATION_RETRY_INTERVAL: Duration = Duration::from_secs(5);
const PEER_QUALITY_SCHEMA_VERSION: u32 = 1;
static PEER_CACHE_WRITE_SEQUENCE: AtomicU64 = AtomicU64::new(0);
static PEER_CACHE_WRITE_LOCK: Mutex<()> = Mutex::new(());
// Mainnet ETH responses have a 2 MiB soft limit. Start from the fastest
// evidence-backed width, then shrink body and receipt requests independently
// when a peer truncates or rejects an oversized response.
const DEFAULT_MATERIAL_REQUEST_BLOCKS: usize = 8;
const MATERIAL_BATCH_GROW_SUCCESS_WINDOWS: usize = 8;
// ETH request IDs allow several independent requests on one session. A small
// pipeline hides public-peer latency without letting a single connection
// consume the entire global history budget.
const MAX_MATERIAL_REQUESTS_PER_PEER: usize = 4;
const DEFAULT_MATERIAL_REQUEST_CONCURRENCY: usize = 32;
const DEFAULT_PEER_CACHE_MAX_ENTRIES: usize = 4_096;
// Cryptographically verified execution material is stronger evidence than a
// successful handshake. Persist a small positive signal so later runs try
// proven serving peers before arbitrary discovered records.
const VERIFIED_MATERIAL_RESPONSE_REPUTATION_REWARD: i32 = 1;

/// Parse an exact 32-byte, `0x`-prefixed execution block hash.
///
/// # Errors
///
/// Returns an error when the value is not lowercase or uppercase hexadecimal
/// with exactly 64 digits after the prefix.
pub fn parse_block_hash(value: &str) -> Result<BlockHash, P2pError> {
    let encoded = value.strip_prefix("0x").ok_or_else(|| {
        P2pError::InvalidConfig("expected tip hash must start with 0x".to_owned())
    })?;
    let mut hash = [0_u8; 32];
    hex::decode_to_slice(encoded, &mut hash).map_err(|_| {
        P2pError::InvalidConfig(
            "expected tip hash must contain exactly 32 hexadecimal bytes".to_owned(),
        )
    })?;
    Ok(BlockHash::new(hash))
}

/// Parse an operator-facing Reth NAT resolver.
///
/// `none` disables external address advertisement; all other values use
/// Reth's resolver syntax (`any`, `upnp`, `publicip`, `extip:<ip>`, ...).
///
/// # Errors
///
/// Returns an invalid-configuration error for unsupported resolver syntax.
pub fn parse_nat_resolver(value: &str) -> Result<Option<NatResolver>, P2pError> {
    if value.eq_ignore_ascii_case("none") {
        return Ok(None);
    }
    value.parse().map(Some).map_err(|error| {
        P2pError::InvalidConfig(format!(
            "invalid execution P2P NAT resolver {value:?}: {error}"
        ))
    })
}

/// Parse an `enode://` trusted execution peer.
///
/// # Errors
///
/// Returns an invalid-configuration error for unsupported peer syntax.
pub fn parse_trusted_peer(value: &str) -> Result<TrustedPeer, P2pError> {
    value.parse().map_err(|error| {
        P2pError::InvalidConfig(format!("invalid trusted execution peer {value:?}: {error}"))
    })
}

/// Validate an authenticated EIP-1459 execution-peer tree URL.
///
/// The tree's public key authenticates peer hints only. It does not confer
/// trust on execution headers, bodies, or receipts returned by those peers.
///
/// # Errors
///
/// Returns an invalid-configuration error when `value` is not a valid
/// `enrtree://` link containing a DNS domain and secp256k1 public key.
pub fn validate_bootstrap_dns_tree(value: &str) -> Result<(), P2pError> {
    value.parse::<DnsDiscoveryLink>().map(|_| ()).map_err(|_| {
        P2pError::InvalidConfig(
            "bootstrap DNS tree must be a valid authenticated enrtree:// URL".to_owned(),
        )
    })
}

#[derive(Clone, Debug)]
pub struct RethP2pConfig {
    /// Hard availability floor required before requests may start.
    pub minimum_peers: usize,
    /// Non-blocking operational target for a healthy peer pool. Reth continues
    /// filling outbound slots beyond this target in the background.
    pub preferred_peers: usize,
    /// Outbound slots kept open so material requests have a broad pool of
    /// independently operated peers to choose from.
    pub max_outbound_peers: usize,
    /// Simultaneous discovery dials used while filling unhealthy peer slots.
    pub max_concurrent_dials: usize,
    /// TCP `RLPx` listener port. Zero asks the OS for an ephemeral port.
    pub listener_port: u16,
    /// UDP discovery port used by Discv4. Zero asks the OS for an ephemeral
    /// port.
    pub discovery_port: u16,
    /// UDP discovery port used by Discv5. Zero asks the OS for an independent
    /// ephemeral port.
    pub discv5_port: u16,
    /// Enable Ethereum execution-layer Discv5 alongside Discv4 and DNS.
    pub enable_discv5: bool,
    /// Optional external-address resolver used for inbound advertisement.
    pub nat: Option<NatResolver>,
    /// Optional operator-supplied peers that Reth will keep reconnecting to.
    pub trusted_peers: Vec<TrustedPeer>,
    /// Optional EIP-1459 tree containing execution peers operated for Leani
    /// traffic. The public key embedded in the `enrtree://` URL authenticates
    /// the records; returned chain material remains independently verified.
    pub bootstrap_dns_tree: Option<String>,
    /// How quickly Reth fills newly available outbound slots.
    pub peer_refill_interval: Duration,
    /// Rebuild the complete network and discovery stack after this long
    /// without any connected execution peers.
    pub peer_recovery_timeout: Duration,
    pub peer_wait_timeout: Duration,
    pub request_timeout: Duration,
    pub retries: usize,
    /// Recovery attempts after the persistent peer pool exhausts request
    /// retries.
    pub session_retries: usize,
    pub retry_backoff: Duration,
    /// Upper bound for exponential delays between request-recovery attempts.
    pub retry_backoff_max: Duration,
    /// Keep retrying the persistent pool until cancellation instead of making
    /// temporary public-network availability a terminal node error.
    pub persistent_retries: bool,
    /// Maximum simultaneous adaptive body or receipt requests. The effective
    /// limit is also bounded by connected peers and the source budget.
    pub material_request_concurrency: usize,
    /// Initial and maximum blocks per ETH body/receipt request. Oversized or
    /// partial responses are split and reduce the adaptive working size.
    pub material_request_blocks: usize,
    /// Maximum simultaneous header requests while proving a finalized
    /// historical range. Requests are low priority and yield to the live lane.
    pub history_header_request_concurrency: usize,
    /// Maximum headers requested in one historical proof response.
    pub history_header_request_blocks: u64,
    /// Optional Reth peer metadata cache loaded on startup and refreshed while
    /// the network session is running.
    pub peer_cache_path: Option<PathBuf>,
    /// Stable secp256k1 node identity. When omitted while a peer cache is
    /// configured, a sibling `execution-p2p-secret` file is used.
    pub secret_key_path: Option<PathBuf>,
    /// Maximum retained peer records after every authoritative Reth cache
    /// refresh. This prevents stale discovery results from growing forever.
    pub peer_cache_max_entries: usize,
    pub peer_cache_flush_interval: Duration,
    pub poll_interval: Duration,
    pub max_reorg_depth: usize,
    /// Shared operational status for the persistent P2P manager.
    pub network_telemetry: NetworkTelemetry,
}

impl Default for RethP2pConfig {
    fn default() -> Self {
        Self {
            minimum_peers: 1,
            preferred_peers: 16,
            max_outbound_peers: 100,
            max_concurrent_dials: 30,
            listener_port: 0,
            discovery_port: 0,
            discv5_port: 0,
            enable_discv5: true,
            nat: None,
            trusted_peers: Vec::new(),
            bootstrap_dns_tree: None,
            peer_refill_interval: Duration::from_secs(5),
            peer_recovery_timeout: Duration::from_mins(5),
            peer_wait_timeout: Duration::from_mins(5),
            request_timeout: Duration::from_secs(8),
            retries: 3,
            session_retries: 3,
            retry_backoff: Duration::from_millis(250),
            retry_backoff_max: Duration::from_mins(1),
            persistent_retries: true,
            material_request_concurrency: DEFAULT_MATERIAL_REQUEST_CONCURRENCY,
            material_request_blocks: DEFAULT_MATERIAL_REQUEST_BLOCKS,
            history_header_request_concurrency: 16,
            history_header_request_blocks: MAX_HISTORY_HEADER_REQUEST_BLOCKS,
            peer_cache_path: None,
            secret_key_path: None,
            peer_cache_max_entries: DEFAULT_PEER_CACHE_MAX_ENTRIES,
            peer_cache_flush_interval: Duration::from_mins(1),
            poll_interval: Duration::from_secs(2),
            max_reorg_depth: 64,
            network_telemetry: NetworkTelemetry::default(),
        }
    }
}

impl RethP2pConfig {
    fn validate(&self) -> Result<(), P2pError> {
        if self.minimum_peers == 0 {
            return Err(P2pError::InvalidConfig(
                "minimum peers must be greater than zero".to_owned(),
            ));
        }
        if self.preferred_peers < self.minimum_peers
            || self.preferred_peers > self.max_outbound_peers
        {
            return Err(P2pError::InvalidConfig(
                "preferred peers must be between minimum peers and maximum outbound peers"
                    .to_owned(),
            ));
        }
        if self.max_outbound_peers < self.minimum_peers || self.max_outbound_peers > 400 {
            return Err(P2pError::InvalidConfig(
                "maximum outbound peers must be between minimum peers and 400".to_owned(),
            ));
        }
        if self.max_concurrent_dials == 0 || self.max_concurrent_dials > self.max_outbound_peers {
            return Err(P2pError::InvalidConfig(
                "maximum concurrent dials must be in 1..=maximum outbound peers".to_owned(),
            ));
        }
        if self.enable_discv5 && self.discovery_port != 0 && self.discovery_port == self.discv5_port
        {
            return Err(P2pError::InvalidConfig(
                "fixed Discv4 and Discv5 UDP ports must differ".to_owned(),
            ));
        }
        if let Some(tree) = self.bootstrap_dns_tree.as_deref()
            && validate_bootstrap_dns_tree(tree).is_err()
        {
            return Err(P2pError::InvalidConfig(
                "bootstrap DNS tree must be a valid authenticated enrtree:// URL".to_owned(),
            ));
        }
        if !(1..=64).contains(&self.material_request_concurrency) {
            return Err(P2pError::InvalidConfig(
                "material request concurrency must be in 1..=64".to_owned(),
            ));
        }
        if !(1..=16).contains(&self.material_request_blocks) {
            return Err(P2pError::InvalidConfig(
                "material request blocks must be in 1..=16".to_owned(),
            ));
        }
        if !(1..=64).contains(&self.history_header_request_concurrency) {
            return Err(P2pError::InvalidConfig(
                "history header request concurrency must be in 1..=64".to_owned(),
            ));
        }
        if !(1..=MAX_HISTORY_HEADER_REQUEST_BLOCKS).contains(&self.history_header_request_blocks) {
            return Err(P2pError::InvalidConfig(format!(
                "history header request blocks must be in 1..={MAX_HISTORY_HEADER_REQUEST_BLOCKS}"
            )));
        }
        if !(1..=65_536).contains(&self.peer_cache_max_entries) {
            return Err(P2pError::InvalidConfig(
                "peer cache maximum entries must be in 1..=65536".to_owned(),
            ));
        }
        if self.peer_wait_timeout.is_zero()
            || self.request_timeout.is_zero()
            || self.retry_backoff.is_zero()
            || self.retry_backoff_max.is_zero()
            || self.peer_refill_interval.is_zero()
            || self.peer_recovery_timeout.is_zero()
            || self.peer_cache_flush_interval.is_zero()
            || self.poll_interval.is_zero()
        {
            return Err(P2pError::InvalidConfig(
                "peer, request, and retry durations must be non-zero".to_owned(),
            ));
        }
        if self.retry_backoff_max < self.retry_backoff {
            return Err(P2pError::InvalidConfig(
                "maximum retry backoff must not be shorter than the initial backoff".to_owned(),
            ));
        }
        if self.retries == 0 || self.session_retries == 0 {
            return Err(P2pError::InvalidConfig(
                "request and recovery retries must be greater than zero".to_owned(),
            ));
        }
        if self.max_reorg_depth == 0
            || u64::try_from(self.max_reorg_depth).unwrap_or(u64::MAX) > MAX_FIXED_RANGE_BLOCKS
        {
            return Err(P2pError::InvalidConfig(format!(
                "maximum reorg depth must be in 1..={MAX_FIXED_RANGE_BLOCKS}"
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct P2pProbeMetrics {
    pub reth_version: String,
    pub reth_revision: String,
    pub connected_peers: usize,
    pub response_peers: usize,
    pub blocks: u64,
    pub transactions: u64,
    pub receipts: u64,
    pub encoded_frame_bytes: u64,
    pub elapsed_milliseconds: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct P2pProbeResult {
    pub range: BlockRange,
    pub expected_tip: Option<BlockHash>,
    pub frames: Vec<BlockFrame>,
    pub metrics: P2pProbeMetrics,
}

/// Physical ETH request measurements used by P2P history benchmarks and
/// operational diagnostics. Counts include retries and split fallbacks.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct P2pRequestMetricsSnapshot {
    pub body_request_blocks: usize,
    pub receipt_request_blocks: usize,
    pub headers: P2pRequestKindMetrics,
    /// Subset of `headers` used to link an arbitrary range to the finalized anchor.
    pub history_proof_headers: P2pRequestKindMetrics,
    pub bodies: P2pRequestKindMetrics,
    pub receipts: P2pRequestKindMetrics,
    /// Usefulness accounting for the sparse filtered-log acquisition path.
    pub sparse_logs: P2pSparseLogMetrics,
}

/// Block-level accounting for header-bloom and receipt predicate pushdown.
///
/// These counters describe decoded protocol material, not compressed socket
/// bytes. They make avoided ETH body/receipt work explicit without pretending
/// Reth's fetch API exposes exact Snappy/framing overhead.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct P2pSparseLogMetrics {
    pub eligible_blocks: u64,
    pub bloom_negative_blocks: u64,
    pub bloom_positive_blocks: u64,
    pub receipt_fetched_blocks: u64,
    pub exact_match_blocks: u64,
    pub body_fetched_blocks: u64,
    pub avoided_receipt_blocks: u64,
    pub avoided_body_blocks: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct P2pRequestKindMetrics {
    pub started: u64,
    pub succeeded: u64,
    pub timed_out: u64,
    pub failed: u64,
    pub requested_items: u64,
    pub returned_items: u64,
    /// RLP response payload bytes before request-id framing and Snappy.
    pub response_payload_bytes: u64,
    pub elapsed_milliseconds: u64,
}

#[derive(Clone, Copy, Debug)]
enum P2pRequestKind {
    Headers,
    HistoryProofHeaders,
    Bodies,
    Receipts,
}

#[derive(Clone, Copy, Debug)]
enum P2pRequestOutcome {
    Succeeded,
    TimedOut,
    Failed,
}

#[derive(Clone, Debug, Default)]
struct P2pRequestMetrics {
    inner: Arc<Mutex<P2pRequestMetricsSnapshot>>,
}

impl P2pRequestMetrics {
    fn record_sparse_log_window(
        &self,
        eligible: usize,
        bloom_positive: usize,
        exact_matches: usize,
        bodies_fetched: usize,
    ) {
        let mut snapshot = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let metrics = &mut snapshot.sparse_logs;
        let eligible = u64::try_from(eligible).unwrap_or(u64::MAX);
        let bloom_positive = u64::try_from(bloom_positive).unwrap_or(u64::MAX);
        let exact_matches = u64::try_from(exact_matches).unwrap_or(u64::MAX);
        let bodies_fetched = u64::try_from(bodies_fetched).unwrap_or(u64::MAX);
        metrics.eligible_blocks = metrics.eligible_blocks.saturating_add(eligible);
        metrics.bloom_positive_blocks =
            metrics.bloom_positive_blocks.saturating_add(bloom_positive);
        metrics.bloom_negative_blocks = metrics
            .bloom_negative_blocks
            .saturating_add(eligible.saturating_sub(bloom_positive));
        metrics.receipt_fetched_blocks = metrics
            .receipt_fetched_blocks
            .saturating_add(bloom_positive);
        metrics.exact_match_blocks = metrics.exact_match_blocks.saturating_add(exact_matches);
        metrics.body_fetched_blocks = metrics.body_fetched_blocks.saturating_add(bodies_fetched);
        metrics.avoided_receipt_blocks = metrics
            .avoided_receipt_blocks
            .saturating_add(eligible.saturating_sub(bloom_positive));
        metrics.avoided_body_blocks = metrics
            .avoided_body_blocks
            .saturating_add(eligible.saturating_sub(bodies_fetched));
    }

    fn record_header(
        &self,
        proof: bool,
        requested: usize,
        returned: usize,
        response_payload_bytes: usize,
        elapsed: Duration,
        outcome: P2pRequestOutcome,
    ) {
        self.add_response_payload_bytes(P2pRequestKind::Headers, response_payload_bytes);
        self.record(
            P2pRequestKind::Headers,
            requested,
            returned,
            elapsed,
            outcome,
        );
        if proof {
            self.add_response_payload_bytes(
                P2pRequestKind::HistoryProofHeaders,
                response_payload_bytes,
            );
            self.record(
                P2pRequestKind::HistoryProofHeaders,
                requested,
                returned,
                elapsed,
                outcome,
            );
        }
    }

    fn record(
        &self,
        kind: P2pRequestKind,
        requested: usize,
        returned: usize,
        elapsed: Duration,
        outcome: P2pRequestOutcome,
    ) {
        self.record_batch(kind, 1, requested, returned, elapsed, outcome);
    }

    fn record_batch(
        &self,
        kind: P2pRequestKind,
        started: u64,
        requested: usize,
        returned: usize,
        elapsed: Duration,
        outcome: P2pRequestOutcome,
    ) {
        let mut snapshot = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let metrics = match kind {
            P2pRequestKind::Headers => &mut snapshot.headers,
            P2pRequestKind::HistoryProofHeaders => &mut snapshot.history_proof_headers,
            P2pRequestKind::Bodies => &mut snapshot.bodies,
            P2pRequestKind::Receipts => &mut snapshot.receipts,
        };
        metrics.started = metrics.started.saturating_add(started);
        metrics.requested_items = metrics
            .requested_items
            .saturating_add(u64::try_from(requested).unwrap_or(u64::MAX));
        metrics.returned_items = metrics
            .returned_items
            .saturating_add(u64::try_from(returned).unwrap_or(u64::MAX));
        metrics.elapsed_milliseconds = metrics
            .elapsed_milliseconds
            .saturating_add(u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX));
        match outcome {
            P2pRequestOutcome::Succeeded => {
                metrics.succeeded = metrics.succeeded.saturating_add(started);
            }
            P2pRequestOutcome::TimedOut => {
                metrics.timed_out = metrics.timed_out.saturating_add(started);
            }
            P2pRequestOutcome::Failed => {
                metrics.failed = metrics.failed.saturating_add(started);
            }
        }
    }

    fn add_response_payload_bytes(&self, kind: P2pRequestKind, bytes: usize) {
        let mut snapshot = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let metrics = match kind {
            P2pRequestKind::Headers => &mut snapshot.headers,
            P2pRequestKind::HistoryProofHeaders => &mut snapshot.history_proof_headers,
            P2pRequestKind::Bodies => &mut snapshot.bodies,
            P2pRequestKind::Receipts => &mut snapshot.receipts,
        };
        metrics.response_payload_bytes = metrics
            .response_payload_bytes
            .saturating_add(u64::try_from(bytes).unwrap_or(u64::MAX));
    }

    fn snapshot(&self) -> P2pRequestMetricsSnapshot {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

#[derive(Clone, Debug)]
pub struct RethP2pSource {
    config: RethP2pConfig,
    descriptor: SourceDescriptor,
    network: Arc<PersistentNetwork>,
    material_tuning: Arc<MaterialBatchTuning>,
    request_metrics: P2pRequestMetrics,
}

/// Consensus-verified execution anchor used to prove a historical P2P suffix.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct P2pHistoryAnchor {
    pub block: BlockRef,
    pub consensus: ConsensusAnchor,
}

/// Bounded, finalized history adapter used only to bridge public-dataset lag.
///
/// It first verifies the complete header chain backwards from `anchor` before
/// yielding any requested body or receipt material. This prevents an
/// unanchored prefix from being durably committed if a later request fails.
#[derive(Clone, Debug)]
pub struct RethP2pHistorySource {
    source: RethP2pSource,
    descriptor: SourceDescriptor,
    anchor: P2pHistoryAnchor,
    prefer_shared_live_session: bool,
    anchored_headers: Arc<tokio::sync::Mutex<Option<AnchoredHeaderProof>>>,
    history_session: Arc<tokio::sync::Mutex<Option<P2pSession>>>,
    acquisition_metrics: Arc<Mutex<SourceAcquisitionMetrics>>,
}

#[derive(Clone, Debug)]
struct AnchoredHeaderProof {
    proof: BlockRange,
    retained: BlockRange,
    hashes: Vec<BlockHash>,
}

impl AnchoredHeaderProof {
    fn expected_hash(&self, number: BlockNumber) -> Option<BlockHash> {
        if number < self.retained.start() || number > self.retained.end() {
            return None;
        }
        let offset = usize::try_from(number.0.checked_sub(self.retained.start().0)?).ok()?;
        self.hashes.get(offset).copied()
    }

    fn covers(&self, proof: BlockRange, retained: BlockRange) -> bool {
        if proof.start() < self.proof.start()
            || proof.end() != self.proof.end()
            || retained.start() < self.retained.start()
            || retained.end() > self.retained.end()
        {
            return false;
        }
        self.hashes.len() == usize::try_from(self.retained.len()).unwrap_or(usize::MAX)
    }
}

#[derive(Debug)]
struct HeaderProofSegment {
    range: BlockRange,
    first_parent: BlockHash,
    hashes: Vec<BlockHash>,
}

#[derive(Debug)]
struct AnchoredHeaderProofBuilder {
    proof: BlockRange,
    retained: BlockRange,
    pending: VecDeque<BlockRange>,
    segments: BTreeMap<BlockNumber, HeaderProofSegment>,
    next: Option<BlockNumber>,
    prior_hash: Option<BlockHash>,
    retained_hashes: Vec<BlockHash>,
}

impl AnchoredHeaderProofBuilder {
    fn new(proof: BlockRange, retained: BlockRange, request_blocks: u64) -> Self {
        Self {
            proof,
            retained,
            pending: VecDeque::from(anchored_header_ranges(proof, request_blocks)),
            segments: BTreeMap::new(),
            next: Some(proof.start()),
            prior_hash: None,
            retained_hashes: Vec::with_capacity(
                usize::try_from(retained.len()).unwrap_or_default(),
            ),
        }
    }

    fn take_wave(&mut self, concurrency: usize) -> Vec<BlockRange> {
        self.pending
            .drain(..self.pending.len().min(concurrency.max(1)))
            .collect()
    }

    fn record(
        &mut self,
        range: BlockRange,
        result: Result<HeaderProofSegment, P2pError>,
    ) -> Result<(), P2pError> {
        match result {
            Ok(segment) => {
                self.segments.insert(segment.range.start(), segment);
            }
            Err(_) => self.pending.push_back(range),
        }
        self.advance()
    }

    fn advance(&mut self) -> Result<(), P2pError> {
        while let Some(next) = self.next {
            let Some(segment) = self.segments.remove(&next) else {
                break;
            };
            if segment.range.start() != next || segment.range.end() > self.proof.end() {
                return Err(P2pError::InvalidResponse(
                    "anchored header proof segments are not an exact ordered cover".to_owned(),
                ));
            }
            if let Some(expected_parent) = self.prior_hash
                && segment.first_parent != expected_parent
            {
                return Err(P2pError::InvalidResponse(format!(
                    "anchored header proof boundary failed at block {}",
                    segment.range.start().0
                )));
            }
            for (offset, hash) in segment.hashes.iter().copied().enumerate() {
                let number = segment
                    .range
                    .start()
                    .0
                    .saturating_add(u64::try_from(offset).unwrap_or(u64::MAX));
                if number >= self.retained.start().0 && number <= self.retained.end().0 {
                    self.retained_hashes.push(hash);
                }
            }
            self.prior_hash = segment.hashes.last().copied();
            self.next = (segment.range.end() < self.proof.end())
                .then(|| BlockNumber(segment.range.end().0.saturating_add(1)));
        }
        Ok(())
    }

    fn finish(&mut self, anchor: BlockHash) -> Result<AnchoredHeaderProof, P2pError> {
        self.advance()?;
        if !self.pending.is_empty() || self.next.is_some() || !self.segments.is_empty() {
            return Err(P2pError::InvalidResponse(
                "anchored header proof still has pending ranges".to_owned(),
            ));
        }
        if self.prior_hash != Some(anchor) {
            return Err(P2pError::InvalidResponse(format!(
                "anchored header proof tip {:?}, expected {anchor}",
                self.prior_hash
            )));
        }
        let expected = usize::try_from(self.retained.len()).map_err(|_| {
            P2pError::InvalidConfig("retained history proof range is too large".to_owned())
        })?;
        if self.retained_hashes.len() != expected {
            return Err(P2pError::InvalidResponse(
                "anchored header proof omitted retained material hashes".to_owned(),
            ));
        }
        Ok(AnchoredHeaderProof {
            proof: self.proof,
            retained: self.retained,
            hashes: std::mem::take(&mut self.retained_hashes),
        })
    }
}

#[derive(Debug)]
struct P2pSession {
    network: Arc<PersistentNetwork>,
    generation: u64,
    handle: NetworkHandle<EthNetworkPrimitives>,
    fetch: FetchClient<EthNetworkPrimitives>,
    telemetry: NetworkSessionTelemetry,
}

impl P2pSession {
    fn set_phase(&self, phase: NetworkPhase) {
        self.telemetry.set_phase(phase);
    }

    fn set_range(&self, range: Option<BlockRange>) {
        self.telemetry.set_range(range);
    }

    fn observe_head(&self, head: BlockNumber) {
        self.telemetry.observe_head(head);
    }

    fn record_error(&self, error: impl std::fmt::Display) {
        self.telemetry.record_error(error);
    }

    fn record_attempt(&self) {
        self.telemetry.record_attempt();
    }

    fn clear_error(&self) {
        self.telemetry.clear_error();
    }

    async fn manager_is_current(&self) -> bool {
        let state = self.network.state.lock().await;
        state.as_ref().is_some_and(|running| {
            running.generation == self.generation && !running.network_task.is_finished()
        })
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum PeerMaterialKind {
    Header,
    Body,
    Receipts,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum PeerQualification {
    BodyServing,
    HeadersOnly,
    Lagging,
    Rejected,
    TimedOut,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PeerQualityEvidence {
    fork_compatible: bool,
    highest_served_block: Option<u64>,
    last_header_success_unix_ms: Option<u64>,
    last_body_success_unix_ms: Option<u64>,
    last_receipt_success_unix_ms: Option<u64>,
    response_latency_ms: Option<u64>,
    last_failure_reason: Option<String>,
    last_failure_unix_ms: Option<u64>,
    qualification: Option<PeerQualification>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PeerQualityDocument {
    schema_version: u32,
    peers: BTreeMap<String, PeerQualityEvidence>,
}

impl Default for PeerQualityDocument {
    fn default() -> Self {
        Self {
            schema_version: PEER_QUALITY_SCHEMA_VERSION,
            peers: BTreeMap::new(),
        }
    }
}

#[derive(Debug)]
struct PeerQualityStore {
    path: Option<PathBuf>,
    document: Mutex<PeerQualityDocument>,
    dirty: AtomicBool,
}

impl PeerQualityStore {
    fn load(peer_cache_path: Option<&Path>) -> Self {
        let path = peer_cache_path.map(peer_quality_path);
        let document = path.as_deref().map_or_else(PeerQualityDocument::default, |path| {
            match std::fs::read(path) {
                Ok(encoded) => match serde_json::from_slice::<PeerQualityDocument>(&encoded) {
                    Ok(document)
                        if document.schema_version == PEER_QUALITY_SCHEMA_VERSION => document,
                    Ok(document) => {
                        warn!(
                            path = %path.display(),
                            schema_version = document.schema_version,
                            "ignoring execution peer-quality cache with an unsupported schema"
                        );
                        PeerQualityDocument::default()
                    }
                    Err(error) => {
                        warn!(path = %path.display(), %error, "ignoring unreadable execution peer-quality cache");
                        PeerQualityDocument::default()
                    }
                },
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    PeerQualityDocument::default()
                }
                Err(error) => {
                    warn!(path = %path.display(), %error, "ignoring unreadable execution peer-quality cache");
                    PeerQualityDocument::default()
                }
            }
        });
        Self {
            path,
            document: Mutex::new(document),
            dirty: AtomicBool::new(false),
        }
    }

    fn record_success(&self, peer_id: B512, kind: PeerMaterialKind, block: u64, elapsed: Duration) {
        let now = observed_at_unix_ms();
        let latency = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
        let mut document = self
            .document
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let evidence = document.peers.entry(peer_quality_key(peer_id)).or_default();
        evidence.fork_compatible = true;
        evidence.highest_served_block =
            Some(evidence.highest_served_block.unwrap_or_default().max(block));
        match kind {
            PeerMaterialKind::Header => evidence.last_header_success_unix_ms = Some(now),
            PeerMaterialKind::Body => evidence.last_body_success_unix_ms = Some(now),
            PeerMaterialKind::Receipts => evidence.last_receipt_success_unix_ms = Some(now),
        }
        evidence.response_latency_ms = Some(evidence.response_latency_ms.map_or(latency, |old| {
            old.saturating_mul(3).saturating_add(latency) / 4
        }));
        if matches!(kind, PeerMaterialKind::Body) {
            evidence.qualification = Some(PeerQualification::BodyServing);
        }
        drop(document);
        self.dirty.store(true, Ordering::Release);
    }

    fn record_qualification(
        &self,
        peer_id: B512,
        qualification: PeerQualification,
        detail: Option<&str>,
    ) {
        let now = observed_at_unix_ms();
        let mut document = self
            .document
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let evidence = document.peers.entry(peer_quality_key(peer_id)).or_default();
        evidence.qualification = Some(qualification);
        evidence.fork_compatible |= !matches!(qualification, PeerQualification::Rejected);
        if !matches!(qualification, PeerQualification::BodyServing) {
            evidence.last_failure_reason = detail.map(bounded_quality_detail);
            evidence.last_failure_unix_ms = Some(now);
        }
        drop(document);
        self.dirty.store(true, Ordering::Release);
    }

    fn record_failure(&self, peer_id: B512, detail: &str) {
        let mut document = self
            .document
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let evidence = document.peers.entry(peer_quality_key(peer_id)).or_default();
        evidence.last_failure_reason = Some(bounded_quality_detail(detail));
        evidence.last_failure_unix_ms = Some(observed_at_unix_ms());
        drop(document);
        self.dirty.store(true, Ordering::Release);
    }

    fn rank(&self, peer_id: B512) -> PeerQualityRank {
        self.document
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .peers
            .get(&peer_quality_key(peer_id))
            .map_or_else(PeerQualityRank::default, PeerQualityRank::from)
    }

    fn persist(&self, maximum_entries: usize) -> Result<(), String> {
        let Some(path) = self.path.as_deref() else {
            return Ok(());
        };
        if !self.dirty.swap(false, Ordering::AcqRel) && path.exists() {
            return Ok(());
        }
        let mut document = self
            .document
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if document.peers.len() > maximum_entries {
            let mut entries = document.peers.into_iter().collect::<Vec<_>>();
            entries.sort_by(|(_, left), (_, right)| {
                PeerQualityRank::from(right).cmp(&PeerQualityRank::from(left))
            });
            entries.truncate(maximum_entries);
            document.peers = entries.into_iter().collect();
        }
        let parent = path
            .parent()
            .ok_or_else(|| "execution peer-quality path has no parent directory".to_owned())?;
        std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        let encoded = serde_json::to_vec_pretty(&document).map_err(|error| error.to_string())?;
        let sequence = PEER_CACHE_WRITE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let temporary =
            path.with_extension(format!("json.{}.{}.tmp", std::process::id(), sequence));
        let result = (|| {
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temporary)
                .map_err(|error| error.to_string())?;
            file.write_all(&encoded)
                .map_err(|error| error.to_string())?;
            file.write_all(b"\n").map_err(|error| error.to_string())?;
            file.sync_all().map_err(|error| error.to_string())?;
            std::fs::rename(&temporary, path).map_err(|error| error.to_string())
        })();
        if result.is_err() {
            self.dirty.store(true, Ordering::Release);
            let _ = std::fs::remove_file(&temporary);
        }
        result
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
struct PeerQualityRank {
    body_serving: bool,
    receipt_serving: bool,
    highest_served_block: u64,
    last_material_success_unix_ms: u64,
    inverse_latency_ms: std::cmp::Reverse<u64>,
    no_recent_failure: bool,
}

impl From<&PeerQualityEvidence> for PeerQualityRank {
    fn from(evidence: &PeerQualityEvidence) -> Self {
        let last_material_success_unix_ms = evidence
            .last_header_success_unix_ms
            .into_iter()
            .chain(evidence.last_body_success_unix_ms)
            .chain(evidence.last_receipt_success_unix_ms)
            .max()
            .unwrap_or_default();
        Self {
            body_serving: evidence.last_body_success_unix_ms.is_some(),
            receipt_serving: evidence.last_receipt_success_unix_ms.is_some(),
            highest_served_block: evidence.highest_served_block.unwrap_or_default(),
            last_material_success_unix_ms,
            inverse_latency_ms: std::cmp::Reverse(evidence.response_latency_ms.unwrap_or(u64::MAX)),
            no_recent_failure: evidence
                .last_failure_unix_ms
                .is_none_or(|failure| failure < last_material_success_unix_ms),
        }
    }
}

fn peer_quality_key(peer_id: B512) -> String {
    hex::encode(peer_id.as_slice())
}

fn bounded_quality_detail(detail: &str) -> String {
    detail.chars().take(256).collect()
}

fn peer_quality_path(peer_cache_path: &Path) -> PathBuf {
    peer_cache_path.with_file_name("execution-peer-quality.json")
}

/// Identity-free summary of a peer-quality cache merge.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PeerQualityCacheMerge {
    pub total: usize,
    pub imported: usize,
}

/// Merge sibling execution peer-quality caches into one destination.
///
/// Evidence is monotonic per material kind: the highest served block and most
/// recent verified successes win, while the newest failure classification is
/// retained independently. This lets isolated subscription feeds reuse useful
/// service history without sharing their database or P2P identity.
///
/// # Errors
///
/// Returns an error when an existing quality document cannot be read or the
/// merged document cannot be persisted atomically.
pub fn merge_peer_quality_caches(
    destination: &Path,
    sources: &[PathBuf],
    maximum_entries: usize,
) -> Result<Option<PeerQualityCacheMerge>, String> {
    let destination_document = read_peer_quality_document(destination)?;
    let destination_peers = destination_document
        .peers
        .keys()
        .cloned()
        .collect::<HashSet<_>>();
    let mut merged = destination_document.peers;
    for source in sources
        .iter()
        .filter(|source| source.as_path() != destination)
    {
        let document = match read_peer_quality_document(source) {
            Ok(document) => document,
            Err(error) => {
                warn!(path = %source.display(), %error, "ignoring unreadable sibling execution peer-quality cache");
                continue;
            }
        };
        for (peer_id, evidence) in document.peers {
            match merged.entry(peer_id) {
                std::collections::btree_map::Entry::Vacant(slot) => {
                    slot.insert(evidence);
                }
                std::collections::btree_map::Entry::Occupied(mut slot) => {
                    merge_peer_quality_evidence(slot.get_mut(), evidence);
                }
            }
        }
    }
    if merged.is_empty() {
        return Ok(None);
    }
    let mut entries = merged.into_iter().collect::<Vec<_>>();
    entries.sort_by(|(left_id, left), (right_id, right)| {
        PeerQualityRank::from(right)
            .cmp(&PeerQualityRank::from(left))
            .then_with(|| left_id.cmp(right_id))
    });
    entries.truncate(maximum_entries);
    let imported = entries
        .iter()
        .filter(|(peer_id, _)| !destination_peers.contains(peer_id))
        .count();
    let document = PeerQualityDocument {
        schema_version: PEER_QUALITY_SCHEMA_VERSION,
        peers: entries.into_iter().collect(),
    };
    write_peer_quality_document(destination, &document)?;
    Ok(Some(PeerQualityCacheMerge {
        total: document.peers.len(),
        imported,
    }))
}

fn read_peer_quality_document(path: &Path) -> Result<PeerQualityDocument, String> {
    let encoded = match std::fs::read(path) {
        Ok(encoded) => encoded,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(PeerQualityDocument::default());
        }
        Err(error) => return Err(error.to_string()),
    };
    let document = serde_json::from_slice::<PeerQualityDocument>(&encoded)
        .map_err(|error| error.to_string())?;
    if document.schema_version != PEER_QUALITY_SCHEMA_VERSION {
        return Err(format!(
            "unsupported peer-quality schema {}",
            document.schema_version
        ));
    }
    Ok(document)
}

fn write_peer_quality_document(path: &Path, document: &PeerQualityDocument) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| "execution peer-quality path has no parent directory".to_owned())?;
    std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    let encoded = serde_json::to_vec_pretty(document).map_err(|error| error.to_string())?;
    let sequence = PEER_CACHE_WRITE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let temporary =
        path.with_extension(format!("json.{}.{}.merging", std::process::id(), sequence));
    let result = (|| {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .map_err(|error| error.to_string())?;
        file.write_all(&encoded)
            .map_err(|error| error.to_string())?;
        file.write_all(b"\n").map_err(|error| error.to_string())?;
        file.sync_all().map_err(|error| error.to_string())?;
        std::fs::rename(&temporary, path).map_err(|error| error.to_string())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

fn merge_peer_quality_evidence(retained: &mut PeerQualityEvidence, incoming: PeerQualityEvidence) {
    let retained_success = PeerQualityRank::from(&*retained).last_material_success_unix_ms;
    let incoming_success = PeerQualityRank::from(&incoming).last_material_success_unix_ms;
    retained.fork_compatible |= incoming.fork_compatible;
    retained.highest_served_block = retained
        .highest_served_block
        .into_iter()
        .chain(incoming.highest_served_block)
        .max();
    retained.last_header_success_unix_ms = retained
        .last_header_success_unix_ms
        .into_iter()
        .chain(incoming.last_header_success_unix_ms)
        .max();
    retained.last_body_success_unix_ms = retained
        .last_body_success_unix_ms
        .into_iter()
        .chain(incoming.last_body_success_unix_ms)
        .max();
    retained.last_receipt_success_unix_ms = retained
        .last_receipt_success_unix_ms
        .into_iter()
        .chain(incoming.last_receipt_success_unix_ms)
        .max();
    if incoming_success >= retained_success {
        retained.response_latency_ms = incoming.response_latency_ms;
        retained.qualification = incoming.qualification;
    }
    if incoming.last_failure_unix_ms >= retained.last_failure_unix_ms {
        retained.last_failure_unix_ms = incoming.last_failure_unix_ms;
        retained.last_failure_reason = incoming.last_failure_reason;
    }
}

#[derive(Debug)]
struct PeerQualificationState {
    target: BlockRef,
    outcomes: HashMap<B512, PeerQualification>,
}

#[derive(Debug)]
struct PeerQualificationPool {
    state: Mutex<PeerQualificationState>,
    changed: tokio::sync::Notify,
}

impl PeerQualificationPool {
    fn new(target: BlockRef) -> Self {
        Self {
            state: Mutex::new(PeerQualificationState {
                target,
                outcomes: HashMap::new(),
            }),
            changed: tokio::sync::Notify::new(),
        }
    }

    fn set_target(&self, target: BlockRef) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.target != target {
            state.target = target;
            state.outcomes.clear();
            drop(state);
            self.changed.notify_waiters();
        }
    }

    fn record(&self, target: BlockRef, peer_id: B512, outcome: PeerQualification) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.target == target {
            state.outcomes.insert(peer_id, outcome);
            drop(state);
            self.changed.notify_waiters();
        }
    }

    fn remove(&self, peer_id: B512) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .outcomes
            .remove(&peer_id);
        self.changed.notify_waiters();
    }

    fn ready(&self, target: BlockRef) -> usize {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.target != target {
            return 0;
        }
        state
            .outcomes
            .values()
            .filter(|outcome| matches!(outcome, PeerQualification::BodyServing))
            .count()
    }

    fn peer_is_ready(&self, target: BlockRef, peer_id: B512) -> bool {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.target == target
            && state
                .outcomes
                .get(&peer_id)
                .is_some_and(|outcome| matches!(outcome, PeerQualification::BodyServing))
    }
}

#[derive(Debug)]
struct PersistentNetwork {
    state: tokio::sync::Mutex<Option<PersistentNetworkState>>,
    next_generation: AtomicU64,
    request_gate: Arc<MaterialRequestGate>,
    direct_peers: Arc<DirectPeerPool>,
    peer_quality: Arc<PeerQualityStore>,
    qualifications: Arc<PeerQualificationPool>,
}

#[derive(Debug)]
struct PersistentNetworkState {
    generation: u64,
    handle: NetworkHandle<EthNetworkPrimitives>,
    fetch: FetchClient<EthNetworkPrimitives>,
    cache_flush: tokio::sync::mpsc::Sender<tokio::sync::oneshot::Sender<Result<(), String>>>,
    network_task: tokio::task::JoinHandle<()>,
    shutdown: CancellationToken,
    telemetry: NetworkSessionTelemetry,
    qualification_target: tokio::sync::watch::Sender<BlockRef>,
}

#[derive(Clone, Debug)]
struct DirectPeer {
    peer_id: B512,
    eth_version: EthVersion,
    messages: PeerRequestSender<PeerRequest<EthNetworkPrimitives>>,
    advertised_head: Option<u64>,
}

#[derive(Debug)]
struct DirectPeerState {
    peer: DirectPeer,
    qualified: bool,
    in_flight: usize,
    failures: u32,
    retry_at: Instant,
}

#[derive(Debug)]
struct DirectPeerPool {
    peers: Mutex<Vec<DirectPeerState>>,
    cursor: AtomicUsize,
    changed: tokio::sync::Notify,
    quality: Arc<PeerQualityStore>,
}

impl DirectPeerPool {
    fn new(quality: Arc<PeerQualityStore>) -> Self {
        Self {
            peers: Mutex::new(Vec::new()),
            cursor: AtomicUsize::new(0),
            changed: tokio::sync::Notify::new(),
            quality,
        }
    }

    fn insert(&self, peer: DirectPeer) {
        let mut peers = self
            .peers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(existing) = peers
            .iter_mut()
            .find(|existing| existing.peer.peer_id == peer.peer_id)
        {
            existing.peer = peer;
            existing.qualified = false;
            existing.in_flight = 0;
            existing.retry_at = Instant::now();
        } else {
            peers.push(DirectPeerState {
                peer,
                qualified: false,
                in_flight: 0,
                failures: 0,
                retry_at: Instant::now(),
            });
        }
        drop(peers);
        self.changed.notify_waiters();
    }

    fn remove(&self, peer_id: B512) {
        self.peers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|peer| peer.peer.peer_id != peer_id);
        self.changed.notify_waiters();
    }

    fn clear(&self) {
        self.peers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
        self.changed.notify_waiters();
    }

    fn set_qualified(&self, peer_id: B512, qualified: bool) {
        let mut peers = self
            .peers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(peer) = peers.iter_mut().find(|peer| peer.peer.peer_id == peer_id) {
            peer.qualified = qualified;
        }
        drop(peers);
        self.changed.notify_waiters();
    }

    fn clear_qualifications(&self) {
        let mut peers = self
            .peers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for peer in &mut *peers {
            peer.qualified = false;
        }
        drop(peers);
        self.changed.notify_waiters();
    }

    fn get(&self, peer_id: B512) -> Option<DirectPeer> {
        self.peers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .find(|peer| peer.peer.peer_id == peer_id)
            .map(|peer| peer.peer.clone())
    }

    fn try_acquire_excluding(
        self: &Arc<Self>,
        per_peer_limit: usize,
        excluded: &HashSet<B512>,
        preferred: Option<B512>,
    ) -> Option<DirectPeerLease> {
        let per_peer_limit = per_peer_limit.max(1);
        let mut peers = self
            .peers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = Instant::now();
        let len = peers.len();
        let start = self.cursor.fetch_add(1, Ordering::Relaxed);
        let selected = (0..len)
            .map(|offset| (start.wrapping_add(offset)) % len)
            .filter(|index| {
                !excluded.contains(&peers[*index].peer.peer_id)
                    && peers[*index].qualified
                    && peers[*index].in_flight < per_peer_limit
                    && peers[*index].retry_at <= now
            })
            .min_by_key(|index| {
                (
                    Some(peers[*index].peer.peer_id) != preferred,
                    std::cmp::Reverse(self.quality.rank(peers[*index].peer.peer_id)),
                    peers[*index].failures,
                    peers[*index].in_flight,
                )
            })?;
        peers[selected].in_flight = peers[selected].in_flight.saturating_add(1);
        Some(DirectPeerLease {
            pool: self.clone(),
            peer: peers[selected].peer.clone(),
            outcome: DirectPeerOutcome::Neutral,
        })
    }

    async fn acquire(
        self: &Arc<Self>,
        per_peer_limit: usize,
        unavailable_timeout: Duration,
        cancellation: &CancellationToken,
    ) -> Result<DirectPeerLease, P2pError> {
        self.acquire_excluding(
            per_peer_limit,
            unavailable_timeout,
            &HashSet::new(),
            None,
            cancellation,
        )
        .await?
        .ok_or_else(|| P2pError::Request {
            component: "direct peer availability",
            detail: "all connected peers were unexpectedly excluded".to_owned(),
        })
    }

    async fn acquire_excluding(
        self: &Arc<Self>,
        per_peer_limit: usize,
        unavailable_timeout: Duration,
        excluded: &HashSet<B512>,
        preferred: Option<B512>,
        cancellation: &CancellationToken,
    ) -> Result<Option<DirectPeerLease>, P2pError> {
        let per_peer_limit = per_peer_limit.max(1);
        let mut unavailable_since = None;
        loop {
            let notified = self.changed.notified();
            let (retry_delay, has_peers, has_untried_peers) = {
                let mut peers = self
                    .peers
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let now = Instant::now();
                let len = peers.len();
                let start = self.cursor.fetch_add(1, Ordering::Relaxed);
                let selected = (0..len)
                    .map(|offset| (start.wrapping_add(offset)) % len)
                    .filter(|index| {
                        !excluded.contains(&peers[*index].peer.peer_id)
                            && peers[*index].qualified
                            && peers[*index].in_flight < per_peer_limit
                            && peers[*index].retry_at <= now
                    })
                    .min_by_key(|index| {
                        (
                            Some(peers[*index].peer.peer_id) != preferred,
                            std::cmp::Reverse(self.quality.rank(peers[*index].peer.peer_id)),
                            peers[*index].failures,
                            peers[*index].in_flight,
                        )
                    });
                if let Some(index) = selected {
                    peers[index].in_flight = peers[index].in_flight.saturating_add(1);
                    return Ok(Some(DirectPeerLease {
                        pool: self.clone(),
                        peer: peers[index].peer.clone(),
                        outcome: DirectPeerOutcome::Neutral,
                    }));
                }
                (
                    peers
                        .iter()
                        .filter(|peer| {
                            !excluded.contains(&peer.peer.peer_id)
                                && peer.qualified
                                && peer.in_flight < per_peer_limit
                        })
                        .map(|peer| peer.retry_at.saturating_duration_since(now))
                        .min(),
                    peers.iter().any(|peer| peer.qualified),
                    peers
                        .iter()
                        .any(|peer| peer.qualified && !excluded.contains(&peer.peer.peer_id)),
                )
            };
            if has_peers && !has_untried_peers {
                return Ok(None);
            }
            let now = Instant::now();
            let unavailable_remaining = if has_peers {
                unavailable_since = None;
                None
            } else {
                let since = *unavailable_since.get_or_insert(now);
                let elapsed = now.saturating_duration_since(since);
                if elapsed >= unavailable_timeout {
                    return Err(P2pError::Timeout {
                        component: "direct peer availability",
                    });
                }
                Some(unavailable_timeout.saturating_sub(elapsed))
            };
            let mut delay = retry_delay.unwrap_or(Duration::from_millis(100));
            if let Some(remaining) = unavailable_remaining {
                delay = delay.min(remaining);
            }
            tokio::select! {
                () = cancellation.cancelled() => return Err(P2pError::Cancelled),
                () = notified => {}
                () = tokio::time::sleep(delay.max(Duration::from_millis(10))) => {}
            }
        }
    }

    fn snapshot(&self) -> Vec<DirectPeer> {
        self.peers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|state| state.peer.clone())
            .collect()
    }
}

#[derive(Clone, Copy, Debug)]
enum DirectPeerOutcome {
    Neutral,
    Success,
    Failure,
}

#[derive(Debug)]
struct DirectPeerLease {
    pool: Arc<DirectPeerPool>,
    peer: DirectPeer,
    outcome: DirectPeerOutcome,
}

impl DirectPeerLease {
    fn succeeded(&mut self) {
        self.outcome = DirectPeerOutcome::Success;
    }

    fn failed(&mut self) {
        self.outcome = DirectPeerOutcome::Failure;
    }
}

impl Drop for DirectPeerLease {
    fn drop(&mut self) {
        let mut peers = self
            .pool
            .peers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(peer) = peers
            .iter_mut()
            .find(|peer| peer.peer.peer_id == self.peer.peer_id)
        {
            peer.in_flight = peer.in_flight.saturating_sub(1);
            match self.outcome {
                DirectPeerOutcome::Success => {
                    peer.failures = 0;
                    peer.retry_at = Instant::now();
                }
                DirectPeerOutcome::Failure => {
                    peer.failures = peer.failures.saturating_add(1);
                    let exponent = peer.failures.saturating_sub(1).min(7);
                    let backoff_ms = 250_u64.saturating_mul(1_u64 << exponent).min(30_000);
                    peer.retry_at = Instant::now() + Duration::from_millis(backoff_ms);
                    self.pool
                        .quality
                        .record_failure(self.peer.peer_id, "material request failed");
                }
                DirectPeerOutcome::Neutral => {}
            }
        }
        drop(peers);
        self.pool.changed.notify_waiters();
    }
}

impl Drop for PersistentNetwork {
    fn drop(&mut self) {
        if let Some(state) = self.state.get_mut().take() {
            // Let the detached manager task persist the final peer-quality
            // state before it drops its sockets. Explicit async shutdown still
            // waits for the same task below.
            state.shutdown.cancel();
        }
    }
}

#[derive(Debug, Default)]
struct MaterialRequestGate {
    in_flight: AtomicUsize,
    high_priority_waiters: AtomicUsize,
    changed: tokio::sync::Notify,
}

impl MaterialRequestGate {
    async fn acquire(
        self: &Arc<Self>,
        limit: usize,
        priority: Priority,
        cancellation: &CancellationToken,
    ) -> Result<MaterialRequestPermit, P2pError> {
        let limit = limit.max(1);
        let high_priority_waiter = matches!(priority, Priority::High).then(|| {
            self.high_priority_waiters.fetch_add(1, Ordering::AcqRel);
            HighPriorityWaiter { gate: self.clone() }
        });
        loop {
            let current = self.in_flight.load(Ordering::Acquire);
            let can_dispatch = high_priority_waiter.is_some()
                || self.high_priority_waiters.load(Ordering::Acquire) == 0;
            if can_dispatch
                && current < limit
                && self
                    .in_flight
                    .compare_exchange_weak(
                        current,
                        current.saturating_add(1),
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
            {
                return Ok(MaterialRequestPermit { gate: self.clone() });
            }
            tokio::select! {
                () = cancellation.cancelled() => return Err(P2pError::Cancelled),
                () = self.changed.notified() => {}
            }
        }
    }
}

#[derive(Debug)]
struct MaterialRequestPermit {
    gate: Arc<MaterialRequestGate>,
}

impl Drop for MaterialRequestPermit {
    fn drop(&mut self) {
        self.gate.in_flight.fetch_sub(1, Ordering::AcqRel);
        self.gate.changed.notify_waiters();
    }
}

#[derive(Debug)]
struct HighPriorityWaiter {
    gate: Arc<MaterialRequestGate>,
}

impl Drop for HighPriorityWaiter {
    fn drop(&mut self) {
        self.gate
            .high_priority_waiters
            .fetch_sub(1, Ordering::AcqRel);
        self.gate.changed.notify_waiters();
    }
}

#[derive(Clone, Copy, Debug)]
struct MaterialRequestPolicy {
    concurrency: usize,
    priority: Priority,
}

#[derive(Clone, Copy, Debug)]
struct LiveReceiptMaterial<'a> {
    header: &'a Header,
    body: &'a BlockBody,
    hash: B256,
    preferred_peer: B512,
}

#[derive(Debug)]
struct MaterialBatchTuning {
    maximum_blocks: usize,
    body_blocks: AtomicUsize,
    receipt_blocks: AtomicUsize,
    body_success_windows: AtomicUsize,
    receipt_success_windows: AtomicUsize,
}

impl MaterialBatchTuning {
    fn new(maximum_blocks: usize) -> Self {
        Self {
            maximum_blocks,
            body_blocks: AtomicUsize::new(maximum_blocks),
            receipt_blocks: AtomicUsize::new(maximum_blocks),
            body_success_windows: AtomicUsize::new(0),
            receipt_success_windows: AtomicUsize::new(0),
        }
    }

    fn body_blocks(&self) -> usize {
        self.body_blocks
            .load(Ordering::Acquire)
            .clamp(1, self.maximum_blocks)
    }

    fn receipt_blocks(&self) -> usize {
        self.receipt_blocks
            .load(Ordering::Acquire)
            .clamp(1, self.maximum_blocks)
    }

    fn body_succeeded(&self) {
        grow_material_batch(
            &self.body_blocks,
            &self.body_success_windows,
            self.maximum_blocks,
        );
    }

    fn receipts_succeeded(&self) {
        grow_material_batch(
            &self.receipt_blocks,
            &self.receipt_success_windows,
            self.maximum_blocks,
        );
    }

    fn body_failed(&self) {
        self.body_success_windows.store(0, Ordering::Release);
        shrink_material_batch(&self.body_blocks, self.maximum_blocks);
    }

    fn receipts_failed(&self) {
        self.receipt_success_windows.store(0, Ordering::Release);
        shrink_material_batch(&self.receipt_blocks, self.maximum_blocks);
    }
}

fn grow_material_batch(batch: &AtomicUsize, success_windows: &AtomicUsize, maximum_blocks: usize) {
    let current = batch.load(Ordering::Acquire);
    if current >= maximum_blocks {
        success_windows.store(0, Ordering::Release);
        return;
    }
    let successes = success_windows
        .fetch_add(1, Ordering::AcqRel)
        .saturating_add(1);
    if successes < MATERIAL_BATCH_GROW_SUCCESS_WINDOWS {
        return;
    }
    success_windows.store(0, Ordering::Release);
    batch.store(
        current.saturating_mul(2).clamp(1, maximum_blocks),
        Ordering::Release,
    );
}

fn shrink_material_batch(batch: &AtomicUsize, maximum_blocks: usize) {
    let current = batch.load(Ordering::Acquire);
    batch.store((current / 2).clamp(1, maximum_blocks), Ordering::Release);
}

fn effective_material_concurrency(
    connected_peers: usize,
    configured: usize,
    budgeted: usize,
) -> usize {
    connected_peers
        .max(1)
        .saturating_mul(MAX_MATERIAL_REQUESTS_PER_PEER)
        .min(configured)
        .min(budgeted)
        .max(1)
}

fn direct_peer_request_limit(total_concurrency: usize, connected_peers: usize) -> usize {
    total_concurrency
        .div_ceil(connected_peers.max(1))
        .clamp(1, MAX_MATERIAL_REQUESTS_PER_PEER)
}

fn repeated_incomplete_response(
    responses: &mut HashMap<B512, usize>,
    peer_id: B512,
    retry_limit: usize,
) -> bool {
    let attempts = responses.entry(peer_id).or_default();
    *attempts = attempts.saturating_add(1);
    *attempts >= retry_limit.max(1)
}

fn record_request_error(telemetry: &NetworkTelemetry, error: &P2pError) {
    match error {
        P2pError::Cancelled => {}
        P2pError::Timeout { .. } => telemetry.request_timed_out(),
        _ => telemetry.request_failed(),
    }
}

const fn request_outcome(error: &P2pError) -> P2pRequestOutcome {
    if matches!(error, P2pError::Timeout { .. }) {
        P2pRequestOutcome::TimedOut
    } else {
        P2pRequestOutcome::Failed
    }
}

impl PersistentNetwork {
    async fn current_session(self: &Arc<Self>) -> Option<P2pSession> {
        let state = self.state.lock().await;
        let running = state
            .as_ref()
            .filter(|running| !running.network_task.is_finished())?;
        Some(P2pSession {
            network: self.clone(),
            generation: running.generation,
            handle: running.handle.clone(),
            fetch: running.fetch.clone(),
            telemetry: running.telemetry.clone(),
        })
    }

    async fn shutdown(&self) {
        let Some(mut running) = self.state.lock().await.take() else {
            return;
        };
        let (flush_result, flushed) = tokio::sync::oneshot::channel();
        if running.cache_flush.send(flush_result).await.is_ok() {
            match tokio::time::timeout(NETWORK_SHUTDOWN_TIMEOUT, flushed).await {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(error))) => {
                    warn!(%error, "failed to flush execution peer cache before shutdown");
                }
                Ok(Err(_)) => {
                    warn!("execution peer-cache flush channel closed before shutdown");
                }
                Err(_) => {
                    warn!("timed out flushing execution peer cache before shutdown");
                }
            }
        }
        if tokio::time::timeout(NETWORK_SHUTDOWN_TIMEOUT, running.handle.shutdown())
            .await
            .is_err()
        {
            warn!("timed out while shutting down execution P2P network manager");
            running.shutdown.cancel();
        }
        if tokio::time::timeout(NETWORK_SHUTDOWN_TIMEOUT, &mut running.network_task)
            .await
            .is_err()
        {
            running.network_task.abort();
            let _ = running.network_task.await;
        }
    }
}

fn block_status_head(block: BlockRef) -> Head {
    Head {
        number: block.number.0,
        hash: B256::from(*block.hash.as_array()),
        difficulty: U256::ZERO,
        total_difficulty: U256::from(58_750_000_000_000_000_u128) * U256::from(1_000_000_u64),
        timestamp: block.timestamp,
    }
}

#[derive(Debug)]
struct PeerQualificationResult {
    target: BlockRef,
    peer_id: B512,
    outcome: PeerQualification,
    detail: Option<String>,
    header_elapsed: Option<Duration>,
    body_elapsed: Option<Duration>,
}

#[expect(
    clippy::too_many_lines,
    reason = "one bounded probe classifies the header and body stages together"
)]
async fn qualify_execution_peer(
    peer: DirectPeer,
    target: BlockRef,
    timeout: Duration,
    request_gate: Arc<MaterialRequestGate>,
    request_limit: usize,
    priority: Priority,
    cancellation: CancellationToken,
) -> PeerQualificationResult {
    let result = async {
        if peer
            .advertised_head
            .is_some_and(|head| head < target.number.0)
        {
            return PeerQualificationResult {
                target,
                peer_id: peer.peer_id,
                outcome: PeerQualification::Lagging,
                detail: Some(format!(
                    "advertised head is below verified block {}",
                    target.number.0
                )),
                header_elapsed: None,
                body_elapsed: None,
            };
        }
        let permit = match request_gate
            .acquire(request_limit, priority, &cancellation)
            .await
        {
            Ok(permit) => permit,
            Err(error) => {
                return PeerQualificationResult {
                    target,
                    peer_id: peer.peer_id,
                    outcome: PeerQualification::TimedOut,
                    detail: Some(error.to_string()),
                    header_elapsed: None,
                    body_elapsed: None,
                };
            }
        };
        let header_started = Instant::now();
        let header_response = request_direct_header(
            &peer,
            B256::from(*target.hash.as_array()),
            timeout,
            &cancellation,
        )
        .await;
        drop(permit);
        let header_elapsed = header_started.elapsed();
        let mut headers = match header_response {
            Ok(headers) => headers,
            Err(error) => {
                let outcome = if matches!(error, P2pError::Timeout { .. }) {
                    PeerQualification::TimedOut
                } else {
                    PeerQualification::Lagging
                };
                return PeerQualificationResult {
                    target,
                    peer_id: peer.peer_id,
                    outcome,
                    detail: Some(error.to_string()),
                    header_elapsed: Some(header_elapsed),
                    body_elapsed: None,
                };
            }
        };
        let Some(header) = headers.pop() else {
            return PeerQualificationResult {
                target,
                peer_id: peer.peer_id,
                outcome: PeerQualification::Lagging,
                detail: Some("verified anchor header was not served".to_owned()),
                header_elapsed: Some(header_elapsed),
                body_elapsed: None,
            };
        };
        if !headers.is_empty()
            || header.number != target.number.0
            || header.hash_slow() != B256::from(*target.hash.as_array())
        {
            return PeerQualificationResult {
                target,
                peer_id: peer.peer_id,
                outcome: PeerQualification::Rejected,
                detail: Some("peer returned a mismatched verified anchor header".to_owned()),
                header_elapsed: Some(header_elapsed),
                body_elapsed: None,
            };
        }
        let permit = match request_gate
            .acquire(request_limit, priority, &cancellation)
            .await
        {
            Ok(permit) => permit,
            Err(error) => {
                return PeerQualificationResult {
                    target,
                    peer_id: peer.peer_id,
                    outcome: PeerQualification::TimedOut,
                    detail: Some(error.to_string()),
                    header_elapsed: Some(header_elapsed),
                    body_elapsed: None,
                };
            }
        };
        let body_started = Instant::now();
        let body_response = request_direct_bodies(
            &peer,
            std::slice::from_ref(&B256::from(*target.hash.as_array())),
            timeout,
            &cancellation,
        )
        .await;
        drop(permit);
        let body_elapsed = body_started.elapsed();
        match body_response {
            Ok((bodies, _)) => match validate_bodies(std::slice::from_ref(&header), &bodies) {
                Ok(()) => PeerQualificationResult {
                    target,
                    peer_id: peer.peer_id,
                    outcome: PeerQualification::BodyServing,
                    detail: None,
                    header_elapsed: Some(header_elapsed),
                    body_elapsed: Some(body_elapsed),
                },
                Err(P2pError::IncompleteResponse { .. }) => PeerQualificationResult {
                    target,
                    peer_id: peer.peer_id,
                    outcome: PeerQualification::HeadersOnly,
                    detail: Some("verified anchor body was not served".to_owned()),
                    header_elapsed: Some(header_elapsed),
                    body_elapsed: Some(body_elapsed),
                },
                Err(error) => PeerQualificationResult {
                    target,
                    peer_id: peer.peer_id,
                    outcome: PeerQualification::Rejected,
                    detail: Some(error.to_string()),
                    header_elapsed: Some(header_elapsed),
                    body_elapsed: Some(body_elapsed),
                },
            },
            Err(error) => PeerQualificationResult {
                target,
                peer_id: peer.peer_id,
                outcome: if matches!(error, P2pError::Timeout { .. }) {
                    PeerQualification::TimedOut
                } else {
                    PeerQualification::HeadersOnly
                },
                detail: Some(error.to_string()),
                header_elapsed: Some(header_elapsed),
                body_elapsed: Some(body_elapsed),
            },
        }
    };
    result.await
}

#[expect(
    clippy::too_many_arguments,
    reason = "qualification owns the shared peer, target, quality, request, and shutdown state"
)]
#[expect(
    clippy::too_many_lines,
    reason = "one event loop owns qualification dispatch, target changes, retry state, and results"
)]
fn spawn_peer_qualification_worker(
    direct_peers: Arc<DirectPeerPool>,
    qualifications: Arc<PeerQualificationPool>,
    quality: Arc<PeerQualityStore>,
    handle: NetworkHandle<EthNetworkPrimitives>,
    mut target_updates: tokio::sync::watch::Receiver<BlockRef>,
    request_gate: Arc<MaterialRequestGate>,
    request_timeout: Duration,
    concurrency: usize,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut target = *target_updates.borrow_and_update();
        qualifications.set_target(target);
        let mut pending = HashSet::<B512>::new();
        let mut failures = HashMap::<B512, u32>::new();
        let mut retry_at = HashMap::<B512, Instant>::new();
        let mut tasks = FuturesUnordered::new();
        let mut retry_tick = tokio::time::interval(Duration::from_millis(250));
        retry_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            let now = Instant::now();
            let ready = qualifications.ready(target);
            let qualification_concurrency = if ready == 0 { concurrency } else { 1 };
            let priority = if ready == 0 {
                Priority::High
            } else {
                Priority::Normal
            };
            if tasks.len() < qualification_concurrency {
                let mut candidates = direct_peers.snapshot();
                candidates.sort_by_key(|peer| std::cmp::Reverse(quality.rank(peer.peer_id)));
                for peer in candidates {
                    if tasks.len() >= qualification_concurrency {
                        break;
                    }
                    if pending.contains(&peer.peer_id)
                        || qualifications.peer_is_ready(target, peer.peer_id)
                        || retry_at
                            .get(&peer.peer_id)
                            .is_some_and(|retry_at| *retry_at > now)
                    {
                        continue;
                    }
                    pending.insert(peer.peer_id);
                    tasks.push(tokio::spawn(qualify_execution_peer(
                        peer,
                        target,
                        request_timeout,
                        request_gate.clone(),
                        concurrency,
                        priority,
                        shutdown.clone(),
                    )));
                }
            }
            tokio::select! {
                () = shutdown.cancelled() => break,
                changed = target_updates.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    target = *target_updates.borrow_and_update();
                    qualifications.set_target(target);
                    direct_peers.clear_qualifications();
                    for task in &tasks {
                        task.abort();
                    }
                    tasks.clear();
                    pending.clear();
                    failures.clear();
                    retry_at.clear();
                }
                () = direct_peers.changed.notified() => {}
                _ = retry_tick.tick() => {}
                completed = tasks.next(), if !tasks.is_empty() => {
                    let Ok(result) = completed.expect("qualification task exists") else {
                        continue;
                    };
                    pending.remove(&result.peer_id);
                    if result.target != target {
                        continue;
                    }
                    if let Some(elapsed) = result.header_elapsed {
                        quality.record_success(
                            result.peer_id,
                            PeerMaterialKind::Header,
                            target.number.0,
                            elapsed,
                        );
                    }
                    if let Some(elapsed) = result.body_elapsed
                        && matches!(result.outcome, PeerQualification::BodyServing)
                    {
                        quality.record_success(
                            result.peer_id,
                            PeerMaterialKind::Body,
                            target.number.0,
                            elapsed,
                        );
                    }
                    let first_ready = qualifications.ready(target) == 0
                        && matches!(result.outcome, PeerQualification::BodyServing);
                    quality.record_qualification(
                        result.peer_id,
                        result.outcome,
                        result.detail.as_deref(),
                    );
                    qualifications.record(target, result.peer_id, result.outcome);
                    if matches!(result.outcome, PeerQualification::BodyServing) {
                        direct_peers.set_qualified(result.peer_id, true);
                        failures.remove(&result.peer_id);
                        retry_at.remove(&result.peer_id);
                        handle.reputation_change(
                            result.peer_id,
                            ReputationChangeKind::Other(
                                VERIFIED_MATERIAL_RESPONSE_REPUTATION_REWARD,
                            ),
                        );
                        if first_ready {
                            for task in &tasks {
                                task.abort();
                            }
                            tasks.clear();
                            pending.clear();
                        }
                    } else {
                        direct_peers.set_qualified(result.peer_id, false);
                        let failures = failures.entry(result.peer_id).or_default();
                        *failures = failures.saturating_add(1);
                        let exponent = failures.saturating_sub(1).min(4);
                        let delay = PEER_QUALIFICATION_RETRY_INTERVAL
                            .saturating_mul(1_u32 << exponent);
                        retry_at.insert(result.peer_id, Instant::now() + delay);
                        if matches!(result.outcome, PeerQualification::Rejected) {
                            handle.ban_peer(result.peer_id);
                            direct_peers.remove(result.peer_id);
                            qualifications.remove(result.peer_id);
                        }
                    }
                }
            }
        }
    })
}

async fn wait_for_qualified_peers(
    session: &P2pSession,
    qualifications: &PeerQualificationPool,
    target: BlockRef,
    minimum: usize,
    timeout: Duration,
    cancellation: &CancellationToken,
) -> Result<usize, P2pError> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if !session.manager_is_current().await {
            return Err(P2pError::Network(
                "execution P2P manager restarted during peer qualification".to_owned(),
            ));
        }
        let ready = qualifications.ready(target);
        if ready >= minimum {
            return Ok(ready);
        }
        tokio::select! {
            () = cancellation.cancelled() => return Err(P2pError::Cancelled),
            () = qualifications.changed.notified() => {}
            () = tokio::time::sleep(Duration::from_millis(100)) => {}
            () = tokio::time::sleep_until(deadline) => {
                return Err(P2pError::PeerTimeout { minimum, connected: ready });
            }
        }
    }
}

#[derive(Clone, Debug)]
struct SegmentJoiningDnsResolver(hickory_resolver::TokioResolver);

impl SegmentJoiningDnsResolver {
    fn from_system_conf() -> Result<Self, String> {
        hickory_resolver::TokioResolver::builder_tokio()
            .map_err(|error| error.to_string())?
            .build()
            .map(Self)
            .map_err(|error| error.to_string())
    }
}

impl DnsDiscoveryResolver for SegmentJoiningDnsResolver {
    async fn lookup_txt(&self, query: &str) -> Option<String> {
        let fully_qualified = if query.ends_with('.') {
            query.to_owned()
        } else {
            format!("{query}.")
        };
        let lookup = match self.0.txt_lookup(fully_qualified).await {
            Ok(lookup) => lookup,
            Err(error) => {
                trace!(target: "leani_source_p2p::dns", %error, %query, "DNS peer lookup failed");
                return None;
            }
        };
        let txt = lookup
            .answers()
            .iter()
            .find_map(|record| match &record.data {
                RData::TXT(txt) => Some(txt),
                _ => None,
            })?;
        join_dns_txt_segments(txt.txt_data.iter().map(AsRef::as_ref))
    }
}

fn join_dns_txt_segments<'a>(segments: impl IntoIterator<Item = &'a [u8]>) -> Option<String> {
    let joined = segments
        .into_iter()
        .flat_map(|segment| segment.iter().copied())
        .collect::<Vec<_>>();
    String::from_utf8(joined).ok()
}

#[derive(Clone, Copy, Debug)]
enum DialEvent {
    Established(B512),
    Unavailable(B512),
}

#[derive(Clone, Copy, Debug)]
struct PendingDial {
    record: NodeRecord,
    expires_at: tokio::time::Instant,
}

#[derive(Debug)]
struct EventDrivenDialQueue {
    records: HashMap<B512, NodeRecord>,
    fresh: VecDeque<B512>,
    fresh_ids: HashSet<B512>,
    pending: HashMap<B512, PendingDial>,
    cooldowns: HashMap<B512, tokio::time::Instant>,
    connected: HashSet<B512>,
    attempt_timeout: Duration,
    redial_interval: Duration,
}

impl EventDrivenDialQueue {
    fn new(attempt_timeout: Duration, redial_interval: Duration) -> Self {
        Self {
            records: HashMap::new(),
            fresh: VecDeque::new(),
            fresh_ids: HashSet::new(),
            pending: HashMap::new(),
            cooldowns: HashMap::new(),
            connected: HashSet::new(),
            attempt_timeout,
            redial_interval,
        }
    }

    fn add(&mut self, record: NodeRecord) -> bool {
        let peer_id = record.id;
        let first_seen = self.records.insert(peer_id, record).is_none();
        if first_seen
            && !self.pending.contains_key(&peer_id)
            && !self.connected.contains(&peer_id)
            && self.fresh_ids.insert(peer_id)
        {
            self.fresh.push_back(peer_id);
        }
        first_seen
    }

    fn on_event(&mut self, event: DialEvent, now: tokio::time::Instant) {
        match event {
            DialEvent::Established(peer_id) => {
                self.pending.remove(&peer_id);
                self.cooldowns.remove(&peer_id);
                self.fresh_ids.remove(&peer_id);
                self.connected.insert(peer_id);
            }
            DialEvent::Unavailable(peer_id) => {
                self.connected.remove(&peer_id);
                self.pending.remove(&peer_id);
                if self.records.contains_key(&peer_id) {
                    self.cooldowns.insert(peer_id, now + self.redial_interval);
                }
            }
        }
    }

    fn expire_pending(&mut self, now: tokio::time::Instant) {
        let expired = self
            .pending
            .iter()
            .filter_map(|(peer_id, pending)| (pending.expires_at <= now).then_some(*peer_id))
            .collect::<Vec<_>>();
        for peer_id in expired {
            if let Some(pending) = self.pending.remove(&peer_id) {
                self.records.insert(peer_id, pending.record);
                self.cooldowns.insert(peer_id, now + self.redial_interval);
            }
        }
    }

    fn dispatch(
        &mut self,
        handle: &NetworkHandle<EthNetworkPrimitives>,
        preferred_peers: usize,
        maximum_dials: usize,
    ) -> (usize, usize) {
        let now = tokio::time::Instant::now();
        self.expire_pending(now);
        let connected = handle.num_connected_peers();
        let desired = if connected == 0 {
            maximum_dials
        } else {
            preferred_peers.saturating_sub(connected).min(maximum_dials)
        };
        let capacity = desired.saturating_sub(self.pending.len());
        let mut fresh_attempts = 0_usize;
        let mut retry_attempts = 0_usize;
        for _ in 0..capacity {
            let candidate = self
                .next_fresh()
                .map(|record| (record, true))
                .or_else(|| self.next_cooled(now).map(|record| (record, false)));
            let Some((record, fresh)) = candidate else {
                break;
            };
            connect_node_record(handle, record);
            self.pending.insert(
                record.id,
                PendingDial {
                    record,
                    expires_at: now + self.attempt_timeout,
                },
            );
            if fresh {
                fresh_attempts = fresh_attempts.saturating_add(1);
            } else {
                retry_attempts = retry_attempts.saturating_add(1);
            }
        }
        (fresh_attempts, retry_attempts)
    }

    fn next_fresh(&mut self) -> Option<NodeRecord> {
        while let Some(peer_id) = self.fresh.pop_front() {
            self.fresh_ids.remove(&peer_id);
            if self.connected.contains(&peer_id)
                || self.pending.contains_key(&peer_id)
                || self.cooldowns.contains_key(&peer_id)
            {
                continue;
            }
            if let Some(record) = self.records.get(&peer_id).copied() {
                return Some(record);
            }
        }
        None
    }

    fn next_cooled(&mut self, now: tokio::time::Instant) -> Option<NodeRecord> {
        let peer_id = self
            .cooldowns
            .iter()
            .filter(|(peer_id, retry_at)| {
                **retry_at <= now
                    && !self.connected.contains(*peer_id)
                    && !self.pending.contains_key(*peer_id)
            })
            .min_by_key(|(_, retry_at)| **retry_at)
            .map(|(peer_id, _)| *peer_id)?;
        self.cooldowns.remove(&peer_id);
        self.records.get(&peer_id).copied()
    }

    fn next_wake(&self) -> tokio::time::Instant {
        self.pending
            .values()
            .map(|pending| pending.expires_at)
            .chain(self.cooldowns.values().copied())
            .min()
            .unwrap_or_else(|| tokio::time::Instant::now() + Duration::from_hours(1))
    }
}

#[derive(Debug)]
struct DnsPeerSeederRuntime {
    dns_head: Head,
    preferred_peers: usize,
    max_concurrent_dials: usize,
    dial_attempt_timeout: Duration,
    redial_interval: Duration,
    bootstrap_dns_tree: Option<String>,
    bootstrap_records: Vec<NodeRecord>,
    initial_pending_records: Vec<NodeRecord>,
}

#[expect(
    clippy::too_many_lines,
    reason = "one select loop joins verified DNS records with the bootstrap crawler"
)]
fn spawn_mainnet_dns_peer_seeder(
    handle: NetworkHandle<EthNetworkPrimitives>,
    runtime: DnsPeerSeederRuntime,
    mut dial_events: tokio::sync::mpsc::UnboundedReceiver<DialEvent>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let DnsPeerSeederRuntime {
            dns_head,
            preferred_peers,
            max_concurrent_dials,
            dial_attempt_timeout,
            redial_interval,
            bootstrap_dns_tree,
            bootstrap_records,
            initial_pending_records,
        } = runtime;
        // Reused, quality-ranked peers are actionable without DNS or Discv
        // setup. Submit them first so a warm start does not lose its best
        // candidates behind public discovery initialization.
        let mut dial_queue = EventDrivenDialQueue::new(dial_attempt_timeout, redial_interval);
        for record in &bootstrap_records {
            add_node_record_to_network(&handle, *record);
            dial_queue.add(*record);
        }
        let now = tokio::time::Instant::now();
        for record in initial_pending_records {
            dial_queue.fresh_ids.remove(&record.id);
            dial_queue.fresh.retain(|peer_id| *peer_id != record.id);
            dial_queue.pending.insert(
                record.id,
                PendingDial {
                    record,
                    expires_at: now + dial_attempt_timeout,
                },
            );
        }
        let (fresh_attempts, retry_attempts) =
            dial_queue.dispatch(&handle, preferred_peers, max_concurrent_dials);
        debug!(
            pending_dials = dial_queue.pending.len(),
            fresh_attempts,
            retry_attempts,
            "submitted quality-ranked cached execution peers before public discovery"
        );
        let resolver = match SegmentJoiningDnsResolver::from_system_conf() {
            Ok(resolver) => resolver,
            Err(error) => {
                warn!(%error, "could not start resilient execution peer DNS seeder");
                return;
            }
        };
        let config = mainnet_dns_discovery_config();
        let (service, control) = DnsDiscoveryService::new_pair(Arc::new(resolver), config);
        let mut service_task = AbortTaskOnDrop(service.spawn());
        let mut records = match control.node_record_stream().await {
            Ok(records) => records,
            Err(error) => {
                warn!(%error, "could not subscribe to mainnet execution peer DNS records");
                return;
            }
        };
        let mut dns_trees = vec![MAINNET_DNS_DISCOVERY_TREE.to_owned()];
        if let Some(tree) = bootstrap_dns_tree {
            dns_trees.push(tree);
        }
        for tree in &dns_trees {
            if let Err(error) = control.sync_tree(tree) {
                warn!(%error, %tree, "could not configure authenticated execution peer DNS tree");
                return;
            }
        }
        let mut dns_retry = tokio::time::interval_at(
            tokio::time::Instant::now() + MAINNET_DNS_RETRY_INTERVAL,
            MAINNET_DNS_RETRY_INTERVAL,
        );
        dns_retry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let crawler_secret = rng_secret_key();
        let crawler_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0));
        let crawler_record =
            NodeRecord::from_secret_key(crawler_addr, &crawler_secret).with_tcp_port(0);
        let crawler_config = Discv4Config {
            external_ip_resolver: None,
            resolve_external_ip_interval: None,
            ..Discv4Config::default()
        };
        let (crawler, mut crawler_service) = match Discv4::bind(
            crawler_addr,
            crawler_record,
            crawler_secret,
            crawler_config,
        )
        .await
        {
            Ok(crawler) => crawler,
            Err(error) => {
                warn!(%error, "could not start execution peer Discv4 bootstrap crawler");
                return;
            }
        };
        let mut crawler_updates = crawler_service.update_stream();
        let mut crawler_task = AbortTaskOnDrop(crawler_service.spawn());
        let fork_filter = MAINNET.fork_filter(dns_head);
        let mut seeded = HashSet::new();
        let mut crawler_seed_count = 0_usize;
        let mut crawler_discovered = HashSet::new();
        // Queue every newly verified discovery record for the next bounded
        // refill wave. Reth owns the simultaneous-dial ceiling, and this
        // supplemental queue never revisits a record more frequently than the
        // configured retry ceiling. Fresh records always go first so a
        // long-lived zero-peer process neither exhausts discovery permanently
        // nor hammers one peer.
        for record in bootstrap_records {
            if crawler_seed_count < MAINNET_DNS_DISCV4_BOOTSTRAP_PEERS {
                crawler.add_boot_node(record);
                crawler_seed_count = crawler_seed_count.saturating_add(1);
            }
        }
        loop {
            let connected = handle.num_connected_peers();
            let (fresh_attempts, retry_attempts) =
                dial_queue.dispatch(&handle, preferred_peers, max_concurrent_dials);
            let attempts = fresh_attempts.saturating_add(retry_attempts);
            if attempts > 0 {
                debug!(
                    connected_peers = connected,
                    preferred_peers,
                    pending_dials = dial_queue.pending.len(),
                    attempts,
                    fresh_attempts,
                    retry_attempts,
                    "filled execution dial slots from the event-driven peer queue"
                );
            }
            let next_dial_wake = dial_queue.next_wake();
            tokio::select! {
                _ = &mut service_task.0 => {
                    warn!("mainnet execution peer DNS seeder stopped unexpectedly");
                    return;
                }
                _ = &mut crawler_task.0 => {
                    warn!("execution peer Discv4 bootstrap crawler stopped unexpectedly");
                    return;
                }
                _ = dns_retry.tick() => {
                    // Reth's DNS service refreshes a tree after its root has
                    // resolved, but an initial root lookup failure leaves no
                    // tree to refresh. Resubmit the root independently of the
                    // slower whole-network recovery watchdog.
                    for tree in &dns_trees {
                        if let Err(error) = control.sync_tree(tree) {
                            warn!(%error, %tree, "could not retry authenticated execution peer DNS tree");
                        }
                    }
                }
                () = tokio::time::sleep_until(next_dial_wake) => {}
                event = dial_events.recv() => {
                    let Some(event) = event else {
                        warn!("execution dial event channel closed unexpectedly");
                        return;
                    };
                    dial_queue.on_event(event, tokio::time::Instant::now());
                }
                update = records.next() => {
                    let Some(update) = update else {
                        warn!("mainnet execution peer DNS record stream closed unexpectedly");
                        return;
                    };
                    let Some(fork_id) = update.fork_id else {
                        trace!("skipping DNS execution peer without a fork ID");
                        continue;
                    };
                    if fork_filter.validate(fork_id).is_err() {
                        trace!(?fork_id, "skipping DNS execution peer with an incompatible fork ID");
                        continue;
                    }
                    let record = update.node_record;
                    add_node_record_to_network(&handle, record);
                    dial_queue.add(record);
                    if crawler_seed_count < MAINNET_DNS_DISCV4_BOOTSTRAP_PEERS {
                        crawler.add_boot_node(record);
                        crawler_seed_count = crawler_seed_count.saturating_add(1);
                    }
                    if seeded.insert(record.id) {
                        let seeded_peers = seeded.len();
                        if seeded_peers == 1 || seeded_peers.is_multiple_of(100) {
                            debug!(
                                seeded_peers,
                                "seeded compatible execution peers from the mainnet DNS tree"
                            );
                        }
                    }
                }
                update = crawler_updates.next() => {
                    let Some(update) = update else {
                        warn!("execution peer Discv4 bootstrap crawler stream closed unexpectedly");
                        return;
                    };
                    let mut pending = vec![update];
                    while let Some(update) = pending.pop() {
                        match update {
                            DiscoveryUpdate::EnrForkId(record, fork_id) => {
                                if fork_filter.validate(fork_id).is_err() {
                                    continue;
                                }
                                add_node_record_to_network(&handle, record);
                                dial_queue.add(record);
                                if crawler_discovered.insert(record.id) {
                                    let discovered_peers = crawler_discovered.len();
                                    if discovered_peers == 1 || discovered_peers.is_multiple_of(25) {
                                        debug!(
                                            discovered_peers,
                                            "seeded compatible execution peers from the Discv4 bootstrap crawler"
                                        );
                                    }
                                }
                            }
                            DiscoveryUpdate::Batch(updates) => pending.extend(updates),
                            DiscoveryUpdate::Added(_)
                            | DiscoveryUpdate::DiscoveredAtCapacity(_)
                            | DiscoveryUpdate::Removed(_) => {}
                        }
                    }
                }
            }
        }
    })
}

fn add_node_record_to_network(handle: &NetworkHandle<EthNetworkPrimitives>, record: NodeRecord) {
    let tcp_addr = SocketAddr::new(record.address, record.tcp_port);
    let udp_addr = SocketAddr::new(record.address, record.udp_port);
    handle.add_peer_kind(record.id, None, tcp_addr, Some(udp_addr));
}

fn connect_node_record(handle: &NetworkHandle<EthNetworkPrimitives>, record: NodeRecord) {
    let tcp_addr = SocketAddr::new(record.address, record.tcp_port);
    let udp_addr = SocketAddr::new(record.address, record.udp_port);
    handle.connect_peer_kind(record.id, PeerKind::Basic, tcp_addr, Some(udp_addr));
}

async fn reward_verified_material_peer(
    handle: &NetworkHandle<EthNetworkPrimitives>,
    peer_id: B512,
) {
    handle.reputation_change(
        peer_id,
        ReputationChangeKind::Other(VERIFIED_MATERIAL_RESPONSE_REPUTATION_REWARD),
    );
    // Network-handle commands are queued. Awaiting the following query creates
    // an ordering barrier so a short-lived `subscribe --once` process cannot
    // shut down before the serving-peer reward reaches the persisted cache.
    let _ = handle.reputation_by_id(peer_id).await;
}

struct AbortTaskOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortTaskOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn mainnet_dns_discovery_config() -> DnsDiscoveryConfig {
    DnsDiscoveryConfig {
        // The current mainnet tree is broad enough that Reth's conservative
        // three-query default can spend over a minute expanding branches
        // before it reaches the first ENR leaf. Bootstrap promptly, then rely
        // on the five-minute recheck interval for steady-state refreshes.
        max_requests_per_sec: NonZeroUsize::new(50).expect("non-zero DNS request rate"),
        recheck_interval: Duration::from_mins(5),
        ..DnsDiscoveryConfig::default()
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "one select loop synchronizes manager, cache, telemetry, recovery, and peer events"
)]
fn spawn_network_manager<S>(
    manager: NetworkManager<EthNetworkPrimitives>,
    mut network_events: S,
    runtime: NetworkManagerRuntime,
) -> tokio::task::JoinHandle<()>
where
    S: futures::Stream<Item = NetworkEvent<PeerRequest<EthNetworkPrimitives>>>
        + Unpin
        + Send
        + 'static,
{
    tokio::spawn(async move {
        let NetworkManagerRuntime {
            peer_cache_path,
            peer_cache_max_entries,
            peer_cache_flush_interval,
            telemetry,
            network_telemetry,
            peer_recovery_timeout,
            dns_head,
            direct_peers,
            preferred_peers,
            max_concurrent_dials,
            redial_interval,
            dial_attempt_timeout,
            bootstrap_dns_tree,
            bootstrap_records,
            peer_quality,
            qualifications,
            qualification_target,
            request_gate,
            request_timeout,
            material_request_concurrency,
            mut cache_flush_requests,
            shutdown,
        } = runtime;
        let mut manager = Box::pin(manager);
        let initial_pending_records = bootstrap_records
            .iter()
            .take(max_concurrent_dials)
            .copied()
            .collect::<Vec<_>>();
        for record in &initial_pending_records {
            add_node_record_to_network(manager.as_ref().get_ref().handle(), *record);
            connect_node_record(manager.as_ref().get_ref().handle(), *record);
        }
        let (dial_event_tx, dial_event_rx) = tokio::sync::mpsc::unbounded_channel();
        let dns_peer_seeder = spawn_mainnet_dns_peer_seeder(
            manager.as_ref().get_ref().handle().clone(),
            DnsPeerSeederRuntime {
                dns_head,
                preferred_peers,
                max_concurrent_dials,
                dial_attempt_timeout,
                redial_interval,
                bootstrap_dns_tree,
                bootstrap_records,
                initial_pending_records,
            },
            dial_event_rx,
        );
        let qualification_worker = spawn_peer_qualification_worker(
            direct_peers.clone(),
            qualifications.clone(),
            peer_quality.clone(),
            manager.as_ref().get_ref().handle().clone(),
            qualification_target,
            request_gate,
            request_timeout,
            material_request_concurrency,
            shutdown.clone(),
        );
        let mut network_events_open = true;
        let mut zero_peers_since = Some(tokio::time::Instant::now());
        let mut cache_flush = tokio::time::interval_at(
            tokio::time::Instant::now() + peer_cache_flush_interval,
            peer_cache_flush_interval,
        );
        cache_flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut telemetry_refresh = tokio::time::interval(Duration::from_secs(1));
        telemetry_refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut cache_flush_requests_open = true;
        let mut cache_flushed_before_shutdown = false;
        loop {
            tokio::select! {
                () = shutdown.cancelled() => break,
                () = &mut manager => {
                    break;
                }
                _ = telemetry_refresh.tick() => {
                    let connected_peers = manager.as_ref().get_ref().num_connected_peers();
                    let known_peers = manager.as_ref().get_ref().num_known_peers();
                    telemetry.set_peers(connected_peers, known_peers);
                    let now = tokio::time::Instant::now();
                    if peer_recovery_due(
                        connected_peers,
                        &mut zero_peers_since,
                        now,
                        peer_recovery_timeout,
                    ) {
                        if let Some(path) = peer_cache_path.as_deref()
                            && known_peers > 0
                            && let Err(error) = write_peer_cache_atomically(
                                manager.as_ref().get_ref(),
                                path,
                                peer_cache_max_entries,
                                &peer_quality,
                            )
                        {
                            warn!(path = %path.display(), %error, "failed to persist execution peers before recovery");
                        }
                        warn!(
                            known_peers,
                            ?peer_recovery_timeout,
                            "recycling stalled execution P2P manager to restart discovery"
                        );
                        break;
                    }
                }
                event = network_events.next(), if network_events_open => {
                    match event {
                        Some(NetworkEvent::ActivePeerSession { info, messages }) => {
                            network_telemetry.peer_session_established();
                            let _ = dial_event_tx.send(DialEvent::Established(info.peer_id));
                            direct_peers.insert(DirectPeer {
                                peer_id: info.peer_id,
                                eth_version: info.version,
                                messages,
                                advertised_head: info.status.latest_block,
                            });
                        }
                        Some(NetworkEvent::Peer(PeerEvent::SessionEstablished(info))) => {
                            let _ = dial_event_tx.send(DialEvent::Established(info.peer_id));
                        }
                        Some(NetworkEvent::Peer(PeerEvent::SessionClosed { peer_id, reason })) => {
                            let _ = dial_event_tx.send(DialEvent::Unavailable(peer_id));
                            direct_peers.remove(peer_id);
                            qualifications.remove(peer_id);
                            let classified = classify_disconnect_reason(reason);
                            peer_quality.record_failure(peer_id, classified.as_str());
                            network_telemetry.peer_session_closed(classified);
                            debug!(
                                reason = classified.as_str(),
                                "execution peer session closed"
                            );
                        }
                        Some(NetworkEvent::Peer(PeerEvent::PeerRemoved(peer_id))) => {
                            let _ = dial_event_tx.send(DialEvent::Unavailable(peer_id));
                        }
                        Some(NetworkEvent::Peer(PeerEvent::PeerAdded(_))) => {}
                        None => network_events_open = false,
                    }
                }
                request = cache_flush_requests.recv(), if cache_flush_requests_open => {
                    let Some(response) = request else {
                        cache_flush_requests_open = false;
                        continue;
                    };
                    let result = peer_cache_path.as_deref().map_or(Ok(()), |path| {
                        if manager.as_ref().get_ref().num_known_peers() == 0 {
                            Ok(())
                        } else {
                            write_peer_cache_atomically(
                                manager.as_ref().get_ref(),
                                path,
                                peer_cache_max_entries,
                                &peer_quality,
                            )
                        }
                    });
                    let result = result.and(peer_quality.persist(peer_cache_max_entries));
                    cache_flushed_before_shutdown = result.is_ok();
                    let _ = response.send(result);
                }
                _ = cache_flush.tick(), if peer_cache_path.is_some() => {
                    let path = peer_cache_path.as_deref().expect("guarded by peer cache path");
                    if manager.as_ref().get_ref().num_known_peers() == 0 {
                        continue;
                    }
                    if let Err(error) =
                        write_peer_cache_atomically(
                            manager.as_ref().get_ref(),
                            path,
                            peer_cache_max_entries,
                            &peer_quality,
                        )
                    {
                        warn!(path = %path.display(), %error, "failed to refresh execution peer cache");
                    } else {
                        debug!(path = %path.display(), "refreshed execution peer cache");
                    }
                    if let Err(error) = peer_quality.persist(peer_cache_max_entries) {
                        warn!(%error, "failed to refresh execution peer-quality cache");
                    }
                }
            }
        }
        if !cache_flushed_before_shutdown
            && let Some(path) = peer_cache_path.as_deref()
            && manager.as_ref().get_ref().num_known_peers() > 0
            && let Err(error) = write_peer_cache_atomically(
                manager.as_ref().get_ref(),
                path,
                peer_cache_max_entries,
                &peer_quality,
            )
        {
            warn!(path = %path.display(), %error, "failed to persist final execution peer state");
        }
        if let Err(error) = peer_quality.persist(peer_cache_max_entries) {
            warn!(%error, "failed to persist final execution peer-quality state");
        }
        dns_peer_seeder.abort();
        let _ = dns_peer_seeder.await;
        qualification_worker.abort();
        let _ = qualification_worker.await;
        direct_peers.clear();
    })
}

fn peer_recovery_due(
    connected_peers: usize,
    zero_peers_since: &mut Option<tokio::time::Instant>,
    now: tokio::time::Instant,
    timeout: Duration,
) -> bool {
    if connected_peers > 0 {
        *zero_peers_since = None;
        return false;
    }
    let stalled_since = zero_peers_since.get_or_insert(now);
    now.duration_since(*stalled_since) >= timeout
}

#[derive(Debug)]
struct NetworkManagerRuntime {
    peer_cache_path: Option<PathBuf>,
    peer_cache_max_entries: usize,
    peer_cache_flush_interval: Duration,
    telemetry: NetworkSessionTelemetry,
    network_telemetry: NetworkTelemetry,
    peer_recovery_timeout: Duration,
    dns_head: Head,
    direct_peers: Arc<DirectPeerPool>,
    preferred_peers: usize,
    max_concurrent_dials: usize,
    redial_interval: Duration,
    dial_attempt_timeout: Duration,
    bootstrap_dns_tree: Option<String>,
    bootstrap_records: Vec<NodeRecord>,
    peer_quality: Arc<PeerQualityStore>,
    qualifications: Arc<PeerQualificationPool>,
    qualification_target: tokio::sync::watch::Receiver<BlockRef>,
    request_gate: Arc<MaterialRequestGate>,
    request_timeout: Duration,
    material_request_concurrency: usize,
    cache_flush_requests:
        tokio::sync::mpsc::Receiver<tokio::sync::oneshot::Sender<Result<(), String>>>,
    shutdown: CancellationToken,
}

fn classify_disconnect_reason(reason: Option<DisconnectReason>) -> NetworkDisconnectReason {
    match reason {
        None => NetworkDisconnectReason::ConnectionClosed,
        Some(DisconnectReason::DisconnectRequested) => NetworkDisconnectReason::DisconnectRequested,
        Some(DisconnectReason::TcpSubsystemError) => NetworkDisconnectReason::TcpSubsystemError,
        Some(DisconnectReason::ProtocolBreach) => NetworkDisconnectReason::ProtocolBreach,
        Some(DisconnectReason::UselessPeer) => NetworkDisconnectReason::UselessPeer,
        Some(DisconnectReason::TooManyPeers) => NetworkDisconnectReason::TooManyPeers,
        Some(DisconnectReason::AlreadyConnected) => NetworkDisconnectReason::AlreadyConnected,
        Some(DisconnectReason::IncompatibleP2PProtocolVersion) => {
            NetworkDisconnectReason::IncompatibleP2pProtocolVersion
        }
        Some(DisconnectReason::NullNodeIdentity) => NetworkDisconnectReason::NullNodeIdentity,
        Some(DisconnectReason::ClientQuitting) => NetworkDisconnectReason::ClientQuitting,
        Some(DisconnectReason::UnexpectedHandshakeIdentity) => {
            NetworkDisconnectReason::UnexpectedHandshakeIdentity
        }
        Some(DisconnectReason::ConnectedToSelf) => NetworkDisconnectReason::ConnectedToSelf,
        Some(DisconnectReason::PingTimeout) => NetworkDisconnectReason::PingTimeout,
        Some(DisconnectReason::SubprotocolSpecific) => NetworkDisconnectReason::SubprotocolSpecific,
    }
}

fn write_peer_cache_atomically(
    manager: &NetworkManager<EthNetworkPrimitives>,
    path: &Path,
    maximum_entries: usize,
    quality: &PeerQualityStore,
) -> Result<(), String> {
    let _write_guard = PEER_CACHE_WRITE_LOCK
        .lock()
        .map_err(|_| "execution peer cache write lock is poisoned".to_owned())?;
    let sequence = PEER_CACHE_WRITE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let file_name = path
        .file_name()
        .map_or_else(|| "execution-peers".into(), |name| name.to_string_lossy());
    let temporary = path.with_file_name(format!(
        ".{file_name}.{}.{}.tmp",
        std::process::id(),
        sequence
    ));
    if let Err(error) = manager.write_peers_to_file(&temporary) {
        let _ = std::fs::remove_file(&temporary);
        return Err(error.to_string());
    }
    if let Err(error) = compact_peer_cache_file(&temporary, maximum_entries, quality) {
        let _ = std::fs::remove_file(&temporary);
        return Err(error);
    }
    if let Err(error) = std::fs::rename(&temporary, path) {
        let _ = std::fs::remove_file(&temporary);
        return Err(error.to_string());
    }
    Ok(())
}

fn compact_existing_peer_cache(
    path: &Path,
    maximum_entries: usize,
    quality: &PeerQualityStore,
) -> Result<(), String> {
    let encoded = match std::fs::read(path) {
        Ok(encoded) => encoded,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.to_string()),
    };
    let entries = parse_peer_cache_entries(&encoded)?;
    if entries.len() <= maximum_entries {
        return Ok(());
    }
    let backup = path.with_extension("json.pre-compact");
    if !backup.exists() {
        std::fs::copy(path, &backup).map_err(|error| error.to_string())?;
    }
    let compacted = compact_peer_cache_entries(entries, maximum_entries, quality);
    let rewritten = serde_json::to_vec_pretty(&compacted).map_err(|error| error.to_string())?;
    let temporary = path.with_extension("json.compacting");
    std::fs::write(&temporary, rewritten).map_err(|error| error.to_string())?;
    std::fs::rename(&temporary, path).map_err(|error| error.to_string())
}

fn compact_peer_cache_file(
    path: &Path,
    maximum_entries: usize,
    quality: &PeerQualityStore,
) -> Result<(), String> {
    let encoded = std::fs::read(path).map_err(|error| error.to_string())?;
    let entries = parse_peer_cache_entries(&encoded)?;
    let compacted = compact_peer_cache_entries(entries, maximum_entries, quality);
    let encoded = serde_json::to_vec_pretty(&compacted).map_err(|error| error.to_string())?;
    std::fs::write(path, encoded).map_err(|error| error.to_string())
}

fn compact_peer_cache_entries(
    entries: Vec<(String, serde_json::Value)>,
    maximum_entries: usize,
    quality: &PeerQualityStore,
) -> Vec<serde_json::Value> {
    let mut unique = BTreeMap::new();
    for (record, entry) in entries {
        unique.insert(record, entry);
    }
    let mut entries = unique.into_iter().collect::<Vec<_>>();
    entries.sort_by(|(left_record, left), (right_record, right)| {
        peer_cache_priority(right, quality)
            .cmp(&peer_cache_priority(left, quality))
            .then_with(|| left_record.cmp(right_record))
    });
    entries.truncate(maximum_entries);
    entries.into_iter().map(|(_, entry)| entry).collect()
}

fn peer_cache_priority(
    entry: &serde_json::Value,
    quality: &PeerQualityStore,
) -> (PeerQualityRank, bool, bool, bool, i64) {
    let has_fork = entry
        .get("fork_id")
        .or_else(|| entry.get("forkId"))
        .is_some_and(|value| !value.is_null());
    let reputation = entry
        .get("reputation")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or_default();
    let quality = entry
        .get("record")
        .and_then(serde_json::Value::as_str)
        .and_then(|record| record.parse::<NodeRecord>().ok())
        .map_or_else(PeerQualityRank::default, |record| quality.rank(record.id));
    (
        quality,
        reputation >= 0,
        reputation > 0,
        has_fork,
        reputation,
    )
}

fn prioritized_peer_cache_records(
    path: Option<&Path>,
    quality: &PeerQualityStore,
    maximum_entries: usize,
) -> Vec<NodeRecord> {
    let Some(path) = path else {
        return Vec::new();
    };
    let Ok(encoded) = std::fs::read(path) else {
        return Vec::new();
    };
    let Ok(mut entries) = parse_peer_cache_entries(&encoded) else {
        return Vec::new();
    };
    entries.sort_by(|(left_record, left), (right_record, right)| {
        peer_cache_priority(right, quality)
            .cmp(&peer_cache_priority(left, quality))
            .then_with(|| left_record.cmp(right_record))
    });
    entries
        .into_iter()
        .take(maximum_entries)
        .filter_map(|(record, _)| record.parse::<NodeRecord>().ok())
        .collect()
}

fn parse_peer_cache_entries(encoded: &[u8]) -> Result<Vec<(String, serde_json::Value)>, String> {
    let entries = serde_json::from_slice::<Vec<serde_json::Value>>(encoded)
        .map_err(|error| error.to_string())?;
    entries
        .into_iter()
        .map(|entry| {
            let record = entry
                .get("record")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| "execution peer cache entry has no string record".to_owned())?
                .to_owned();
            Ok((record, entry))
        })
        .collect()
}

fn configured_secret_key_path(config: &RethP2pConfig) -> Option<PathBuf> {
    config.secret_key_path.clone().or_else(|| {
        config
            .peer_cache_path
            .as_ref()
            .map(|path| path.with_file_name("execution-p2p-secret"))
    })
}

fn load_or_create_secret_key(path: Option<&Path>) -> Result<SecretKey, P2pError> {
    let Some(path) = path else {
        return Ok(rng_secret_key());
    };
    match std::fs::read_to_string(path) {
        Ok(encoded) => return parse_secret_key(encoded.trim(), path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(P2pError::InvalidConfig(format!(
                "failed reading execution P2P identity {}: {error}",
                path.display()
            )));
        }
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| {
            P2pError::InvalidConfig(format!(
                "failed creating execution P2P identity directory {}: {error}",
                parent.display()
            ))
        })?;
    }
    let secret = rng_secret_key();
    let encoded = hex::encode(secret.secret_bytes());
    let sequence = PEER_CACHE_WRITE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let temporary = path.with_extension(format!("secret.{}.{}.tmp", std::process::id(), sequence));
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options.open(&temporary).map_err(|error| {
        P2pError::InvalidConfig(format!(
            "failed creating execution P2P identity {}: {error}",
            temporary.display()
        ))
    })?;
    file.write_all(encoded.as_bytes()).map_err(|error| {
        P2pError::InvalidConfig(format!(
            "failed writing execution P2P identity {}: {error}",
            temporary.display()
        ))
    })?;
    file.sync_all().map_err(|error| {
        P2pError::InvalidConfig(format!(
            "failed syncing execution P2P identity {}: {error}",
            temporary.display()
        ))
    })?;
    std::fs::rename(&temporary, path).map_err(|error| {
        let _ = std::fs::remove_file(&temporary);
        P2pError::InvalidConfig(format!(
            "failed installing execution P2P identity {}: {error}",
            path.display()
        ))
    })?;
    Ok(secret)
}

fn parse_secret_key(encoded: &str, path: &Path) -> Result<SecretKey, P2pError> {
    let bytes = hex::decode(encoded).map_err(|error| {
        P2pError::InvalidConfig(format!(
            "execution P2P identity {} is not hexadecimal: {error}",
            path.display()
        ))
    })?;
    SecretKey::from_slice(&bytes).map_err(|error| {
        P2pError::InvalidConfig(format!(
            "execution P2P identity {} is invalid: {error}",
            path.display()
        ))
    })
}

struct P2pLiveState {
    source: RethP2pSource,
    session: P2pSession,
    request: DataRequest,
    budget: SourceBudget,
    cancellation: CancellationToken,
    last: BlockRef,
    queued: VecDeque<BlockFrame>,
    recent: VecDeque<BlockRef>,
    required_peer_head: BlockNumber,
    pending_material_attempts: usize,
    head_unavailable_since: Option<Instant>,
    reconnect_error: Option<String>,
    disconnect_reported: bool,
    terminal: bool,
}

impl std::fmt::Debug for P2pLiveState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("P2pLiveState")
            .field("source", &self.source)
            .field("session", &self.session)
            .field("request", &self.request)
            .field("budget", &self.budget)
            .field("last", &self.last)
            .field("queued", &self.queued.len())
            .field("recent", &self.recent.len())
            .field("required_peer_head", &self.required_peer_head)
            .field("pending_material_attempts", &self.pending_material_attempts)
            .field("head_unavailable_since", &self.head_unavailable_since)
            .field("reconnect_error", &self.reconnect_error)
            .field("disconnect_reported", &self.disconnect_reported)
            .field("terminal", &self.terminal)
            .finish_non_exhaustive()
    }
}

impl RethP2pSource {
    /// Construct the mainnet P2P source without opening sockets.
    ///
    /// # Errors
    ///
    /// Returns an error when peer, retry, or timeout bounds are invalid.
    pub fn mainnet(config: RethP2pConfig) -> Result<Self, P2pError> {
        config.validate()?;
        let material_tuning = Arc::new(MaterialBatchTuning::new(config.material_request_blocks));
        config.network_telemetry.set_peer_targets(
            config.minimum_peers,
            config.preferred_peers,
            config.max_outbound_peers,
        );
        let peer_quality = Arc::new(PeerQualityStore::load(config.peer_cache_path.as_deref()));
        if let Some(path) = config.peer_cache_path.as_deref()
            && let Err(error) =
                compact_existing_peer_cache(path, config.peer_cache_max_entries, &peer_quality)
        {
            warn!(
                path = %path.display(),
                %error,
                "could not compact the existing execution peer cache before startup"
            );
        }
        let initial_target = Self::mainnet_genesis_block();
        let network = Arc::new(PersistentNetwork {
            state: tokio::sync::Mutex::new(None),
            next_generation: AtomicU64::new(1),
            request_gate: Arc::new(MaterialRequestGate::default()),
            direct_peers: Arc::new(DirectPeerPool::new(peer_quality.clone())),
            peer_quality,
            qualifications: Arc::new(PeerQualificationPool::new(initial_target)),
        });
        Ok(Self {
            descriptor: SourceDescriptor {
                id: SourceId::new("reth-p2p-mainnet")
                    .map_err(|error| P2pError::InvalidConfig(error.to_string()))?,
                kind: SourceKind::ExecutionP2p,
                chain_id: ChainId(1),
                range: None,
                capabilities: CapabilitySet::of(Capability::Header)
                    .with(Capability::Body)
                    .with(Capability::Transactions)
                    .with(Capability::Calldata)
                    .with(Capability::Receipts)
                    .with(Capability::Logs)
                    .with(Capability::Withdrawals),
                complete_capabilities: CapabilitySet::of(Capability::Header)
                    .with(Capability::Body)
                    .with(Capability::Transactions)
                    .with(Capability::Calldata)
                    .with(Capability::Receipts)
                    .with(Capability::Logs)
                    .with(Capability::Withdrawals),
                trust: TrustModel::ProtocolVerified,
                finality: FinalityModel::Optimistic,
                partitioning: Partitioning::FixedBlockSpan(MAX_FIXED_RANGE_BLOCKS),
                expected_lag: Duration::from_secs(12),
                schema_version: format!("reth-p2p.v1+{RETH_VERSION}.{RETH_REVISION}"),
                priority: 0,
            },
            config,
            network,
            material_tuning,
            request_metrics: P2pRequestMetrics::default(),
        })
    }

    /// Return the current aggregate execution-network status without exposing
    /// peer identities.
    #[must_use]
    pub fn network_status(&self) -> NetworkTelemetrySnapshot {
        self.config.network_telemetry.snapshot()
    }

    /// Refresh the local ETH status advertised by an already-started peer
    /// manager. This is useful when discovery begins from retained state while
    /// verified finality resolves concurrently.
    pub async fn update_advertised_head(&self, head: BlockRef) {
        let state = self.network.state.lock().await;
        if let Some(running) = state.as_ref() {
            running.handle.update_status(block_status_head(head));
            let _ = running.qualification_target.send(head);
        }
    }

    #[must_use]
    pub const fn descriptor(&self) -> &SourceDescriptor {
        &self.descriptor
    }

    /// Snapshot physical ETH request counts, item fanout, and cumulative
    /// request time without exposing peer identities.
    #[must_use]
    pub fn request_metrics(&self) -> P2pRequestMetricsSnapshot {
        let mut snapshot = self.request_metrics.snapshot();
        snapshot.body_request_blocks = self.material_tuning.body_blocks();
        snapshot.receipt_request_blocks = self.material_tuning.receipt_blocks();
        snapshot
    }

    /// Gracefully stop the shared execution network and flush its peer cache.
    pub async fn shutdown(&self) {
        self.network.shutdown().await;
    }

    /// Return the execution-mainnet genesis block used as a valid status
    /// advertisement before a caller has resolved a newer trusted anchor.
    #[must_use]
    pub fn mainnet_genesis_block() -> BlockRef {
        let header = MAINNET.genesis_header();
        BlockRef {
            number: BlockNumber(header.number),
            hash: BlockHash::new(MAINNET.genesis_hash().0),
            parent_hash: BlockHash::new(header.parent_hash.0),
            timestamp: header.timestamp,
        }
    }

    /// Start the persistent execution network and satisfy the configured peer
    /// availability floor without opening a logical data stream yet.
    ///
    /// A caller can overlap this with independent finality verification. The
    /// later live/history subscription reuses the same manager, connected
    /// peers, discovery state, and request scheduler.
    ///
    /// # Errors
    ///
    /// Returns the same bounded connection, cancellation, and peer-availability
    /// errors as a live subscription.
    pub async fn warm_up(
        &self,
        advertised: BlockRef,
        cancellation: &CancellationToken,
    ) -> Result<usize, P2pError> {
        self.connect(advertised, NetworkLane::Live, cancellation)
            .await
            .map(|(_, connected)| connected)
    }

    /// Fetch the newest self-consistent execution block advertised by the
    /// active peer pool without first replaying every block since the finalized
    /// anchor.
    ///
    /// The returned frame is deliberately optimistic: requested material is
    /// commitment-checked, but its ancestry has not yet been joined to the
    /// independently verified finalized anchor. Callers may use it for a
    /// low-latency preview while the ordinary anchored live lane catches up.
    ///
    /// # Errors
    ///
    /// Returns bounded connection, peer-head, material, verification, budget,
    /// or cancellation errors.
    pub async fn optimistic_head_snapshot(
        &self,
        advertised: BlockRef,
        request: &DataRequest,
        budget: SourceBudget,
        cancellation: &CancellationToken,
    ) -> Result<BlockFrame, P2pError> {
        let budget = budget
            .validate()
            .map_err(|error| P2pError::InvalidConfig(error.to_string()))?;
        let sparse_scope = sparse_log_scope(request);
        if sparse_scope.is_none() && !header_and_body_only_request(request) {
            return Err(P2pError::InvalidConfig(
                "optimistic head preview requires a block-summary or compatible filtered-log request"
                    .to_owned(),
            ));
        }
        if request.chain_id != self.descriptor.chain_id {
            return Err(P2pError::InvalidConfig(
                "optimistic head preview request belongs to another chain".to_owned(),
            ));
        }
        if sparse_scope.is_some()
            && request
                .log_fields
                .contains(leani_primitives::LogField::TransactionHash)
        {
            return Err(P2pError::InvalidConfig(
                "receipt-only optimistic preview cannot supply transaction hashes".to_owned(),
            ));
        }
        let (session, _) = self
            .connect(advertised, NetworkLane::Live, cancellation)
            .await?;
        let (head_number, head_hash) = self
            .wait_for_peer_head(
                &session,
                BlockNumber(advertised.number.0.saturating_add(1)),
                cancellation,
            )
            .await?;
        let range = BlockRange::single(head_number);
        session.set_range(Some(range));
        session.set_phase(NetworkPhase::FetchingHeaders);
        let (header_peer, headers) = self
            .fetch_headers(
                &session.fetch,
                range,
                Some(head_hash),
                false,
                MaterialRequestPolicy {
                    concurrency: budget.max_in_flight_requests,
                    priority: Priority::High,
                },
                cancellation,
            )
            .await?;
        if header_and_body_only_request(request) {
            return self
                .finish_optimistic_body_snapshot(
                    &session,
                    &headers,
                    Some(header_peer),
                    budget,
                    cancellation,
                )
                .await;
        }
        let scope = sparse_scope.ok_or_else(|| {
            P2pError::InvalidConfig("optimistic filtered-log scope disappeared".to_owned())
        })?;
        self.finish_optimistic_sparse_snapshot(
            &session,
            &headers,
            request,
            scope,
            budget,
            cancellation,
        )
        .await
    }

    async fn finish_optimistic_sparse_snapshot(
        &self,
        session: &P2pSession,
        headers: &[Header],
        request: &DataRequest,
        scope: &FilterScope,
        budget: SourceBudget,
        cancellation: &CancellationToken,
    ) -> Result<BlockFrame, P2pError> {
        let header = headers.first().ok_or_else(|| {
            P2pError::InvalidResponse("optimistic head request returned no header".to_owned())
        })?;
        let hashes = [header.hash_slow()];
        let bloom_positive = header_bloom_matches(scope, header);
        let positive_receipts = if bloom_positive {
            session.set_phase(NetworkPhase::FetchingReceipts);
            self.fetch_sparse_receipts(
                session,
                headers,
                &hashes,
                MaterialRequestPolicy {
                    concurrency: budget.max_in_flight_requests,
                    priority: Priority::High,
                },
                cancellation,
            )
            .await?
        } else {
            Vec::new()
        };
        let exact_match_positions = positive_receipts
            .first()
            .is_some_and(|receipts| {
                receipts
                    .iter()
                    .flat_map(|receipt| &receipt.logs)
                    .any(|log| source_log_matches(Some(scope), log))
            })
            .then_some(0)
            .into_iter()
            .collect::<Vec<_>>();
        let bloom_positive_indices = bloom_positive.then_some(0).into_iter().collect::<Vec<_>>();
        let mut frames = normalize_sparse_log_frames(
            headers,
            request,
            budget,
            &bloom_positive_indices,
            &positive_receipts,
            &exact_match_positions,
            &[],
        )?;
        let mut frame = frames.pop().ok_or_else(|| {
            P2pError::InvalidResponse("optimistic head request returned no frame".to_owned())
        })?;
        frame.finality = Finality::Optimistic;
        session.clear_error();
        session.set_range(None);
        session.set_phase(NetworkPhase::FollowingHead);
        Ok(frame)
    }

    async fn finish_optimistic_body_snapshot(
        &self,
        session: &P2pSession,
        headers: &[Header],
        preferred_peer: Option<B512>,
        budget: SourceBudget,
        cancellation: &CancellationToken,
    ) -> Result<BlockFrame, P2pError> {
        let (mut frames, _) = self
            .fetch_live_body_frames(session, headers, preferred_peer, budget, cancellation)
            .await?;
        let mut frame = frames.pop().ok_or_else(|| {
            P2pError::InvalidResponse("optimistic head request returned no frame".to_owned())
        })?;
        frame.finality = Finality::Optimistic;
        session.clear_error();
        session.set_range(None);
        session.set_phase(NetworkPhase::FollowingHead);
        Ok(frame)
    }

    #[allow(clippy::too_many_lines)]
    async fn fetch_sparse_receipts(
        &self,
        session: &P2pSession,
        headers: &[Header],
        hashes: &[B256],
        policy: MaterialRequestPolicy,
        cancellation: &CancellationToken,
    ) -> Result<Vec<Vec<Receipt>>, P2pError> {
        let mut last_error = None;
        let mut incomplete_responses = HashMap::new();
        let attempts = if headers.len() > 1 {
            1
        } else {
            self.config.retries
        };
        for attempt in 0..attempts {
            let queued_at = Instant::now();
            let connected_peers = session.fetch.num_connected_peers();
            let request_limit = effective_material_concurrency(
                connected_peers,
                self.config.material_request_concurrency,
                policy.concurrency,
            );
            let per_peer_limit = direct_peer_request_limit(request_limit, connected_peers);
            let mut lease = self
                .network
                .direct_peers
                .acquire(per_peer_limit, self.config.request_timeout, cancellation)
                .await?;
            let peer = lease.peer.peer_id;
            let permit = self
                .network
                .request_gate
                .acquire(request_limit, policy.priority, cancellation)
                .await?;
            self.config
                .network_telemetry
                .request_started(queued_at.elapsed());
            let request_started_at = Instant::now();
            let response = request_direct_receipts(
                &lease.peer,
                hashes,
                self.config.request_timeout,
                cancellation,
            )
            .await;
            drop(permit);
            match response {
                Ok(response) => {
                    self.request_metrics.add_response_payload_bytes(
                        P2pRequestKind::Receipts,
                        response.response_payload_bytes,
                    );
                    match validate_receipts_against_headers(headers, &response.receipts) {
                        Ok(()) => {
                            lease.succeeded();
                            self.network.peer_quality.record_success(
                                peer,
                                PeerMaterialKind::Receipts,
                                headers.last().map_or(0, |header| header.number),
                                request_started_at.elapsed(),
                            );
                            reward_verified_material_peer(&session.handle, peer).await;
                            self.config.network_telemetry.request_succeeded();
                            self.request_metrics.record_batch(
                                P2pRequestKind::Receipts,
                                response.physical_requests,
                                response.requested_block_hashes,
                                response.receipts.len(),
                                request_started_at.elapsed(),
                                P2pRequestOutcome::Succeeded,
                            );
                            return Ok(response.receipts);
                        }
                        Err(error) => {
                            lease.failed();
                            self.config.network_telemetry.request_failed();
                            self.request_metrics.record_batch(
                                P2pRequestKind::Receipts,
                                response.physical_requests.max(1),
                                response.requested_block_hashes,
                                response.receipts.len(),
                                request_started_at.elapsed(),
                                P2pRequestOutcome::Failed,
                            );
                            if matches!(error, P2pError::InvalidResponse(_)) {
                                session.handle.ban_peer(peer);
                                self.network.direct_peers.remove(peer);
                            } else if matches!(error, P2pError::IncompleteResponse { .. })
                                && repeated_incomplete_response(
                                    &mut incomplete_responses,
                                    peer,
                                    self.config.retries,
                                )
                            {
                                debug!(
                                    attempts = self.config.retries,
                                    "rotating execution peer after repeated incomplete sparse receipt responses"
                                );
                                session.handle.disconnect_peer(peer);
                                self.network.direct_peers.remove(peer);
                                incomplete_responses.remove(&peer);
                            }
                            last_error = Some(error);
                        }
                    }
                }
                Err(P2pError::Cancelled) => return Err(P2pError::Cancelled),
                Err(error) => {
                    lease.failed();
                    record_request_error(&self.config.network_telemetry, &error);
                    self.request_metrics.record(
                        P2pRequestKind::Receipts,
                        hashes.len(),
                        0,
                        request_started_at.elapsed(),
                        request_outcome(&error),
                    );
                    last_error = Some(error);
                }
            }
            if attempt.saturating_add(1) < attempts {
                retry_pause(self.config.retry_backoff, cancellation).await?;
            }
        }
        Err(last_error.unwrap_or_else(|| P2pError::Request {
            component: "sparse receipts",
            detail: "retry budget exhausted".to_owned(),
        }))
    }

    #[allow(clippy::too_many_lines)]
    async fn fetch_sparse_receipts_batched(
        &self,
        session: &P2pSession,
        headers: &[Header],
        hashes: &[B256],
        policy: MaterialRequestPolicy,
        cancellation: &CancellationToken,
    ) -> Result<Vec<Vec<Receipt>>, P2pError> {
        if headers.len() != hashes.len() {
            return Err(P2pError::InvalidResponse(
                "sparse receipt batch header/hash length mismatch".to_owned(),
            ));
        }
        let batch_blocks = self.material_tuning.receipt_blocks();
        let requests = headers
            .chunks(batch_blocks)
            .zip(hashes.chunks(batch_blocks))
            .map(|(header_chunk, hash_chunk)| (header_chunk.to_vec(), hash_chunk.to_vec()))
            .collect::<Vec<_>>();
        let concurrency = effective_material_concurrency(
            session.fetch.num_connected_peers(),
            self.config.material_request_concurrency,
            policy.concurrency,
        );
        let outcomes = stream::iter(requests)
            .map(|(header_chunk, hash_chunk)| async move {
                let result = self
                    .fetch_sparse_receipts(
                        session,
                        &header_chunk,
                        &hash_chunk,
                        policy,
                        cancellation,
                    )
                    .await;
                (header_chunk, hash_chunk, result)
            })
            .buffered(concurrency)
            .collect::<Vec<_>>()
            .await;

        let mut completed = Vec::new();
        let mut fallback = Vec::new();
        for (header_chunk, hash_chunk, result) in outcomes {
            match result {
                Ok(receipts) => completed.push((header_chunk[0].number, receipts)),
                Err(error) if header_chunk.len() > 1 => {
                    debug!(
                        blocks = header_chunk.len(),
                        %error,
                        "splitting an unsuccessful sparse receipt request into single-block requests"
                    );
                    fallback.extend(header_chunk.into_iter().zip(hash_chunk));
                }
                Err(error) => {
                    self.material_tuning.receipts_failed();
                    return Err(error);
                }
            }
        }
        let recovered = stream::iter(fallback)
            .map(|(header, hash)| async move {
                let result = self
                    .fetch_sparse_receipts(
                        session,
                        std::slice::from_ref(&header),
                        std::slice::from_ref(&hash),
                        policy,
                        cancellation,
                    )
                    .await;
                (header.number, result)
            })
            .buffered(concurrency)
            .collect::<Vec<_>>()
            .await;
        let recovered_any = !recovered.is_empty();
        for (number, result) in recovered {
            match result {
                Ok(receipts) => completed.push((number, receipts)),
                Err(error) => {
                    self.material_tuning.receipts_failed();
                    return Err(error);
                }
            }
        }
        if recovered_any {
            self.material_tuning.receipts_failed();
        } else {
            self.material_tuning.receipts_succeeded();
        }
        completed.sort_by_key(|(number, _)| *number);
        let mut receipts = Vec::with_capacity(headers.len());
        for (_, mut batch) in completed {
            receipts.append(&mut batch);
        }
        validate_receipts_against_headers(headers, &receipts)?;
        Ok(receipts)
    }

    async fn borrow_live_session(&self) -> Option<P2pSession> {
        self.network.current_session().await
    }

    fn peers_config(&self) -> PeersConfig {
        if let Some(path) = self.config.peer_cache_path.as_deref()
            && let Err(error) = compact_existing_peer_cache(
                path,
                self.config.peer_cache_max_entries,
                &self.network.peer_quality,
            )
        {
            warn!(
                path = %path.display(),
                %error,
                "ignoring peer-cache compaction failure before network startup"
            );
        }
        let peers = PeersConfig::default()
            .with_max_outbound(self.config.max_outbound_peers)
            .with_max_concurrent_dials(self.config.max_concurrent_dials)
            .with_refill_slots_interval(self.config.peer_refill_interval)
            .with_trusted_nodes(self.config.trusted_peers.clone());
        match peers
            .clone()
            .with_basic_nodes_from_file(self.config.peer_cache_path.as_deref())
        {
            Ok(peers) => peers,
            Err(error) => {
                warn!(
                    path = %self
                        .config
                        .peer_cache_path
                        .as_deref()
                        .map_or_else(|| "<disabled>".to_owned(), |path| path.display().to_string()),
                    %error,
                    "ignoring unreadable execution peer cache"
                );
                peers
            }
        }
    }

    async fn connect(
        &self,
        advertised: BlockRef,
        lane: NetworkLane,
        cancellation: &CancellationToken,
    ) -> Result<(P2pSession, usize), P2pError> {
        let (session, newly_started) = self.network_session(advertised, lane).await?;
        session
            .telemetry
            .set_range(Some(BlockRange::single(advertised.number)));
        let already_qualified = self.network.qualifications.ready(advertised);
        if !newly_started && already_qualified >= self.config.minimum_peers {
            session.set_phase(NetworkPhase::Ready);
            session.clear_error();
            return Ok((session, already_qualified));
        }
        session.set_phase(NetworkPhase::WaitingForPeers);
        let connected = match wait_for_qualified_peers(
            &session,
            &self.network.qualifications,
            advertised,
            self.config.minimum_peers,
            self.config.peer_wait_timeout,
            cancellation,
        )
        .await
        {
            Ok(connected) => connected,
            Err(error) => {
                session.record_error(&error);
                return Err(error);
            }
        };
        if connected < self.config.minimum_peers {
            let error = P2pError::PeerTimeout {
                minimum: self.config.minimum_peers,
                connected,
            };
            session.record_error(&error);
            return Err(error);
        }
        session.clear_error();
        session.set_phase(NetworkPhase::Ready);
        Ok((session, connected))
    }

    #[allow(clippy::too_many_lines)]
    async fn network_session(
        &self,
        advertised: BlockRef,
        lane: NetworkLane,
    ) -> Result<(P2pSession, bool), P2pError> {
        let mut state = self.network.state.lock().await;
        if state
            .as_ref()
            .is_some_and(|running| running.network_task.is_finished())
        {
            state.take();
        }
        if let Some(running) = state.as_ref() {
            running.handle.update_status(block_status_head(advertised));
            let _ = running.qualification_target.send(advertised);
            return Ok((
                P2pSession {
                    network: self.network.clone(),
                    generation: running.generation,
                    handle: running.handle.clone(),
                    fetch: running.fetch.clone(),
                    telemetry: running.telemetry.clone(),
                },
                false,
            ));
        }
        let secret_path = configured_secret_key_path(&self.config);
        let secret = load_or_create_secret_key(secret_path.as_deref())?;
        let telemetry = self.config.network_telemetry.register(lane);
        telemetry.set_range(Some(BlockRange::single(advertised.number)));
        let listener_addr = SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::UNSPECIFIED,
            self.config.listener_port,
        ));
        let discovery_addr = SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::UNSPECIFIED,
            self.config.discovery_port,
        ));
        let advertised_head = block_status_head(advertised);
        let peers = self.peers_config();
        let sessions = SessionsConfig {
            initial_internal_request_timeout: self.config.request_timeout,
            ..SessionsConfig::default().with_upscaled_event_buffer(peers.max_peers())
        };
        let mut builder =
            NetworkConfigBuilder::<EthNetworkPrimitives>::new(secret, Runtime::test())
                .listener_addr(listener_addr)
                .discovery_addr(discovery_addr)
                .disable_tx_gossip(true)
                .mainnet_boot_nodes()
                // Reth 2.4.1 reads only the first byte segment of a DNS TXT
                // record. Mainnet's EIP-1459 tree contains multi-segment
                // records, so use the segment-joining seeder below instead.
                .disable_dns_discovery()
                .peer_config(peers)
                .sessions_config(sessions)
                .set_head(advertised_head);
        if self.config.enable_discv5 {
            let discv5_listen = reth_discv5::discv5::ListenConfig::Ipv4 {
                ip: Ipv4Addr::UNSPECIFIED,
                port: self.config.discv5_port,
            };
            let discv5 = reth_discv5::Config::builder(listener_addr)
                .discv5_config(reth_discv5::discv5::ConfigBuilder::new(discv5_listen).build());
            builder = builder.discovery_v5(discv5);
        }
        builder = match self.config.nat.clone() {
            Some(nat) => builder.external_ip_resolver(nat),
            None => builder.disable_nat(),
        };
        let manager = Box::pin(builder.build_with_noop_provider(MAINNET.clone()).manager())
            .await
            .map_err(|error| {
                let error = P2pError::Network(error.to_string());
                telemetry.record_error(&error);
                error
            })?;
        let handle = manager.handle().clone();
        let fetch = manager.fetch_client();
        let network_events = handle.event_listener();
        // A recycled manager can leave request senders behind when its task was
        // aborted before it emitted every SessionClosed event.
        self.network.direct_peers.clear();
        let generation = self.network.next_generation.fetch_add(1, Ordering::Relaxed);
        let shutdown = CancellationToken::new();
        self.network.qualifications.set_target(advertised);
        let (qualification_target, qualification_updates) = tokio::sync::watch::channel(advertised);
        let (cache_flush, cache_flush_requests) = tokio::sync::mpsc::channel(1);
        let bootstrap_records = prioritized_peer_cache_records(
            self.config.peer_cache_path.as_deref(),
            &self.network.peer_quality,
            self.config.peer_cache_max_entries,
        );
        let network_task = spawn_network_manager(
            manager,
            network_events,
            NetworkManagerRuntime {
                peer_cache_path: self.config.peer_cache_path.clone(),
                peer_cache_max_entries: self.config.peer_cache_max_entries,
                peer_cache_flush_interval: self.config.peer_cache_flush_interval,
                telemetry: telemetry.clone(),
                network_telemetry: self.config.network_telemetry.clone(),
                peer_recovery_timeout: self.config.peer_recovery_timeout,
                dns_head: advertised_head,
                direct_peers: self.network.direct_peers.clone(),
                preferred_peers: self.config.preferred_peers,
                max_concurrent_dials: self.config.max_concurrent_dials,
                redial_interval: self.config.retry_backoff_max,
                dial_attempt_timeout: self.config.request_timeout,
                bootstrap_dns_tree: self.config.bootstrap_dns_tree.clone(),
                bootstrap_records,
                peer_quality: self.network.peer_quality.clone(),
                qualifications: self.network.qualifications.clone(),
                qualification_target: qualification_updates,
                request_gate: self.network.request_gate.clone(),
                request_timeout: self.config.request_timeout,
                material_request_concurrency: self.config.material_request_concurrency,
                cache_flush_requests,
                shutdown: shutdown.clone(),
            },
        );
        let running = PersistentNetworkState {
            generation,
            handle: handle.clone(),
            fetch: fetch.clone(),
            cache_flush,
            network_task,
            shutdown,
            telemetry: telemetry.clone(),
            qualification_target,
        };
        *state = Some(running);
        Ok((
            P2pSession {
                network: self.network.clone(),
                generation,
                handle,
                fetch,
                telemetry,
            },
            true,
        ))
    }

    async fn fetch_verified_range(
        &self,
        session: &P2pSession,
        range: BlockRange,
        expected_tip: Option<BlockHash>,
        budget: SourceBudget,
        cancellation: &CancellationToken,
    ) -> Result<(Vec<BlockFrame>, usize), P2pError> {
        session.set_range(Some(range));
        session.set_phase(NetworkPhase::FetchingHeaders);
        let (header_peer, headers) = match self
            .fetch_live_headers_from_untried_peers(
                session,
                range,
                expected_tip,
                MaterialRequestPolicy {
                    concurrency: budget.max_in_flight_requests,
                    priority: Priority::High,
                },
                cancellation,
            )
            .await
        {
            Ok(headers) => headers,
            Err(error) => {
                session.record_error(&error);
                return Err(error);
            }
        };
        let hashes = headers.iter().map(Sealable::hash_slow).collect::<Vec<_>>();
        session.set_phase(NetworkPhase::FetchingBodies);
        let material_policy = MaterialRequestPolicy {
            concurrency: budget.max_in_flight_requests,
            priority: Priority::Normal,
        };
        let body_result = if let ([header], [hash]) = (headers.as_slice(), hashes.as_slice()) {
            self.fetch_live_body_from_untried_peers(
                session,
                header,
                *hash,
                Some(header_peer),
                material_policy,
                cancellation,
            )
            .await
            .map(|(peer, body)| (HashSet::from([peer]), vec![body]))
        } else {
            self.fetch_bodies_batched(
                &session.fetch,
                &headers,
                &hashes,
                material_policy,
                cancellation,
            )
            .await
        };
        let (body_peers, bodies) = match body_result {
            Ok(bodies) => bodies,
            Err(error) => {
                session.record_error(&error);
                return Err(error);
            }
        };
        let preferred_receipt_peer = body_peers.iter().next().copied();
        session.set_phase(NetworkPhase::FetchingReceipts);
        let receipt_result = if let ([header], [body], [hash]) =
            (headers.as_slice(), bodies.as_slice(), hashes.as_slice())
        {
            self.fetch_live_receipts_from_untried_peers(
                session,
                LiveReceiptMaterial {
                    header,
                    body,
                    hash: *hash,
                    preferred_peer: preferred_receipt_peer.expect("one live body response peer"),
                },
                material_policy,
                cancellation,
            )
            .await
            .map(|(peer, block_receipts)| (HashSet::from([peer]), vec![block_receipts]))
        } else {
            self.fetch_receipts_batched(
                &session.fetch,
                &headers,
                &bodies,
                &hashes,
                material_policy,
                cancellation,
            )
            .await
        };
        let (receipt_peers, receipts) = match receipt_result {
            Ok(receipts) => receipts,
            Err(error) => {
                session.record_error(&error);
                return Err(error);
            }
        };
        let frames = normalize_verified(&headers, &bodies, &receipts, budget)?;
        session.clear_error();
        let mut response_peers = HashSet::from([header_peer]);
        response_peers.extend(body_peers);
        response_peers.extend(receipt_peers);
        Ok((frames, response_peers.len()))
    }

    async fn fetch_requested_live_range(
        &self,
        session: &P2pSession,
        range: BlockRange,
        expected_tip: Option<BlockHash>,
        request: &DataRequest,
        budget: SourceBudget,
        cancellation: &CancellationToken,
    ) -> Result<(Vec<BlockFrame>, usize), P2pError> {
        if !header_only_request(request)
            && !header_and_body_only_request(request)
            && (sparse_log_scope(request).is_none()
                || request
                    .log_fields
                    .contains(leani_primitives::LogField::TransactionHash))
        {
            return self
                .fetch_verified_range(session, range, expected_tip, budget, cancellation)
                .await;
        }
        session.set_range(Some(range));
        session.set_phase(NetworkPhase::FetchingHeaders);
        let (header_peer, headers) = self
            .fetch_live_headers_from_untried_peers(
                session,
                range,
                expected_tip,
                MaterialRequestPolicy {
                    concurrency: budget.max_in_flight_requests,
                    priority: Priority::High,
                },
                cancellation,
            )
            .await
            .inspect_err(|error| session.record_error(error))?;
        let request = DataRequest {
            range,
            ..request.clone()
        };
        let (frames, mut response_peers) = if header_only_request(&request) {
            (
                normalize_verified_headers(&headers, budget)?,
                HashSet::new(),
            )
        } else if header_and_body_only_request(&request) {
            self.fetch_live_body_frames(session, &headers, Some(header_peer), budget, cancellation)
                .await
                .inspect_err(|error| session.record_error(error))?
        } else {
            (
                self.fetch_sparse_live_frames(session, &headers, &request, budget, cancellation)
                    .await
                    .inspect_err(|error| session.record_error(error))?,
                HashSet::new(),
            )
        };
        response_peers.insert(header_peer);
        session.clear_error();
        Ok((frames, response_peers.len()))
    }

    async fn fetch_live_body_frames(
        &self,
        session: &P2pSession,
        headers: &[Header],
        preferred_peer: Option<B512>,
        budget: SourceBudget,
        cancellation: &CancellationToken,
    ) -> Result<(Vec<BlockFrame>, HashSet<B512>), P2pError> {
        let hashes = headers.iter().map(Sealable::hash_slow).collect::<Vec<_>>();
        session.set_phase(NetworkPhase::FetchingBodies);
        let policy = MaterialRequestPolicy {
            concurrency: budget.max_in_flight_requests,
            priority: Priority::High,
        };
        let (body_peers, bodies) = if let ([header], [hash]) = (headers, hashes.as_slice()) {
            self.fetch_live_body_from_untried_peers(
                session,
                header,
                *hash,
                preferred_peer,
                policy,
                cancellation,
            )
            .await
            .map(|(peer, body)| (HashSet::from([peer]), vec![body]))?
        } else {
            self.fetch_bodies_batched(&session.fetch, headers, &hashes, policy, cancellation)
                .await?
        };
        let frames = normalize_verified_bodies(headers, &bodies, budget)?;
        Ok((frames, body_peers))
    }

    async fn fetch_sparse_live_frames(
        &self,
        session: &P2pSession,
        headers: &[Header],
        request: &DataRequest,
        budget: SourceBudget,
        cancellation: &CancellationToken,
    ) -> Result<Vec<BlockFrame>, P2pError> {
        let scope = sparse_log_scope(request).ok_or_else(|| {
            P2pError::InvalidConfig("live request is not eligible for sparse logs".to_owned())
        })?;
        let hashes = headers.iter().map(Sealable::hash_slow).collect::<Vec<_>>();
        let bloom_positive_indices = headers
            .iter()
            .enumerate()
            .filter_map(|(index, header)| header_bloom_matches(scope, header).then_some(index))
            .collect::<Vec<_>>();
        let positive_headers = bloom_positive_indices
            .iter()
            .map(|index| headers[*index].clone())
            .collect::<Vec<_>>();
        let positive_hashes = bloom_positive_indices
            .iter()
            .map(|index| hashes[*index])
            .collect::<Vec<_>>();
        let positive_receipts = if positive_headers.is_empty() {
            Vec::new()
        } else {
            session.set_phase(NetworkPhase::FetchingReceipts);
            self.fetch_sparse_receipts_batched(
                session,
                &positive_headers,
                &positive_hashes,
                MaterialRequestPolicy {
                    concurrency: budget.max_in_flight_requests,
                    priority: Priority::High,
                },
                cancellation,
            )
            .await?
        };
        let exact_match_positions = positive_receipts
            .iter()
            .enumerate()
            .filter_map(|(position, receipts)| {
                receipts
                    .iter()
                    .flat_map(|receipt| &receipt.logs)
                    .any(|log| source_log_matches(Some(scope), log))
                    .then_some(position)
            })
            .collect::<Vec<_>>();
        let frames = normalize_sparse_log_frames(
            headers,
            request,
            budget,
            &bloom_positive_indices,
            &positive_receipts,
            &exact_match_positions,
            &[],
        )?;
        self.request_metrics.record_sparse_log_window(
            headers.len(),
            bloom_positive_indices.len(),
            exact_match_positions.len(),
            0,
        );
        Ok(frames)
    }

    async fn normalize_polled_sparse_live_frame(
        &self,
        session: &P2pSession,
        next: BlockNumber,
        header: Header,
        request: &DataRequest,
        budget: SourceBudget,
        cancellation: &CancellationToken,
    ) -> Result<Option<BlockFrame>, P2pError> {
        let request = DataRequest {
            range: BlockRange::single(next),
            ..request.clone()
        };
        let mut frames = self
            .fetch_sparse_live_frames(session, &[header], &request, budget, cancellation)
            .await
            .inspect_err(|error| session.record_error(error))?;
        session.clear_error();
        session.observe_head(next);
        session.set_phase(NetworkPhase::FollowingHead);
        Ok(frames.pop())
    }

    #[allow(clippy::too_many_lines)]
    async fn poll_next_verified_frame(
        &self,
        session: &P2pSession,
        next: BlockNumber,
        request: &DataRequest,
        budget: SourceBudget,
        cancellation: &CancellationToken,
    ) -> Result<Option<BlockFrame>, P2pError> {
        session.set_range(Some(BlockRange::single(next)));
        session.set_phase(NetworkPhase::FetchingHeaders);
        let range = BlockRange::single(next);
        let (header_peer, mut headers) = self
            .fetch_live_headers_from_untried_peers(
                session,
                range,
                None,
                MaterialRequestPolicy {
                    concurrency: budget.max_in_flight_requests,
                    priority: Priority::High,
                },
                cancellation,
            )
            .await
            .inspect_err(|error| session.record_error(error))?;
        let header = headers
            .pop()
            .expect("one validated live header was returned");
        if header_only_request(request) {
            let mut frames = normalize_verified_headers(&[header], budget)?;
            session.clear_error();
            session.observe_head(next);
            session.set_phase(NetworkPhase::FollowingHead);
            return Ok(frames.pop());
        }
        if header_and_body_only_request(request) {
            let (mut frames, _) = self
                .fetch_live_body_frames(session, &[header], Some(header_peer), budget, cancellation)
                .await
                .inspect_err(|error| session.record_error(error))?;
            session.clear_error();
            session.observe_head(next);
            session.set_phase(NetworkPhase::FollowingHead);
            return Ok(frames.pop());
        }
        let hash = header.hash_slow();
        if sparse_log_scope(request).is_some()
            && !request
                .log_fields
                .contains(leani_primitives::LogField::TransactionHash)
        {
            return self
                .normalize_polled_sparse_live_frame(
                    session,
                    next,
                    header,
                    request,
                    budget,
                    cancellation,
                )
                .await;
        }
        session.set_phase(NetworkPhase::FetchingBodies);
        let material_policy = MaterialRequestPolicy {
            concurrency: budget.max_in_flight_requests,
            priority: Priority::High,
        };
        let (body_peer, body) = match self
            .fetch_live_body_from_untried_peers(
                session,
                &header,
                hash,
                Some(header_peer),
                material_policy,
                cancellation,
            )
            .await
        {
            Ok(body) => body,
            Err(error) => {
                session.record_error(&error);
                return Err(error);
            }
        };
        session.set_phase(NetworkPhase::FetchingReceipts);
        let (_, block_receipts) = match self
            .fetch_live_receipts_from_untried_peers(
                session,
                LiveReceiptMaterial {
                    header: &header,
                    body: &body,
                    hash,
                    preferred_peer: body_peer,
                },
                material_policy,
                cancellation,
            )
            .await
        {
            Ok(receipts) => receipts,
            Err(error) => {
                session.record_error(&error);
                return Err(error);
            }
        };
        let headers = [header];
        let bodies = [body];
        let receipts = [block_receipts];
        let mut frames = normalize_verified(&headers, &bodies, &receipts, budget)?;
        session.clear_error();
        session.observe_head(next);
        session.set_phase(NetworkPhase::FollowingHead);
        Ok(frames.pop())
    }

    async fn connect_and_fetch_requested_live_range(
        &self,
        advertised: BlockRef,
        range: BlockRange,
        expected_tip: Option<BlockHash>,
        request: &DataRequest,
        budget: SourceBudget,
        cancellation: &CancellationToken,
    ) -> Result<(P2pSession, Vec<BlockFrame>, usize), P2pError> {
        let mut attempts = 0_usize;
        loop {
            attempts = attempts.saturating_add(1);
            let session = match self
                .connect(advertised, NetworkLane::Live, cancellation)
                .await
            {
                Ok((session, _)) => session,
                Err(P2pError::Cancelled) => return Err(P2pError::Cancelled),
                Err(error) => {
                    if should_retry_session_error(&self.config, attempts, &error) {
                        let delay = session_retry_delay(&self.config, attempts);
                        debug!(
                            attempt = attempts,
                            ?delay,
                            %error,
                            "retrying execution P2P connection"
                        );
                        retry_pause(delay, cancellation).await?;
                        continue;
                    }
                    return Err(error);
                }
            };
            if let Err(error) = self
                .wait_for_peer_head(&session, advertised.number, cancellation)
                .await
            {
                if matches!(error, P2pError::Cancelled) {
                    shutdown_session(&session);
                    return Err(P2pError::Cancelled);
                }
                if should_retry_session_error(&self.config, attempts, &error) {
                    retry_pause(session_retry_delay(&self.config, attempts), cancellation).await?;
                    continue;
                }
                return Err(error);
            }
            match self
                .fetch_requested_live_range(
                    &session,
                    range,
                    expected_tip,
                    request,
                    budget,
                    cancellation,
                )
                .await
            {
                Ok((frames, response_peers)) => {
                    session.set_range(None);
                    session.set_phase(NetworkPhase::FollowingHead);
                    return Ok((session, frames, response_peers));
                }
                Err(P2pError::Cancelled) => {
                    shutdown_session(&session);
                    return Err(P2pError::Cancelled);
                }
                Err(error) => {
                    session.record_attempt();
                    session.record_error(&error);
                    shutdown_session(&session);
                    if should_retry_session_error(&self.config, attempts, &error) {
                        let delay = session_retry_delay(&self.config, attempts);
                        debug!(
                            attempt = attempts,
                            ?delay,
                            %error,
                            "retrying execution P2P material on the persistent peer pool"
                        );
                        retry_pause(delay, cancellation).await?;
                    } else {
                        return Err(error);
                    }
                }
            }
        }
    }

    async fn discover_peer_head(
        &self,
        session: &P2pSession,
        minimum: BlockNumber,
        cancellation: &CancellationToken,
    ) -> Result<(BlockNumber, BlockHash), P2pError> {
        session.set_range(None);
        session.set_phase(NetworkPhase::FollowingHead);
        let peers = cancellable_timeout(
            session.handle.get_all_peers(),
            self.config.request_timeout,
            cancellation,
            "peer status",
        )
        .await?;
        let mut declared = BTreeMap::<(u64, [u8; 32]), usize>::new();
        let mut unknown_hashes = Vec::new();
        for peer in peers {
            if let Some(number) = peer.status.latest_block {
                if number < minimum.0 {
                    debug!(
                        peer_head = number,
                        required_head = minimum.0,
                        "execution peer is behind the required live head; retaining it for a grace retry"
                    );
                    continue;
                }
                let key = (number, peer.status.blockhash.0);
                *declared.entry(key).or_default() += 1;
            } else if peer.status.blockhash != B256::ZERO {
                unknown_hashes.push((peer.remote_id, peer.status.blockhash));
            }
        }
        if let Some(((number, hash), _)) = declared
            .into_iter()
            .max_by_key(|((number, _), count)| (*count, *number))
        {
            session.observe_head(BlockNumber(number));
            return Ok((BlockNumber(number), BlockHash::new(hash)));
        }
        // Asking for the exact minimum first uses the dynamic direct-peer
        // scheduler: newly established sessions join this request while older
        // peers are still pending. Resolving status-only head hashes remains a
        // fallback for peers that cannot serve the minimum by number.
        if let Some((head, serving_peer)) = self
            .minimum_live_head(session, minimum, cancellation)
            .await?
        {
            if let Some((_, advertised_hash)) = unknown_hashes
                .iter()
                .find(|(peer_id, _)| *peer_id == serving_peer)
                && let Some(advertised_head) = self
                    .resolve_advertised_peer_head(
                        session,
                        vec![(serving_peer, *advertised_hash)],
                        minimum,
                        cancellation,
                    )
                    .await?
            {
                return Ok(advertised_head);
            }
            return Ok(head);
        }

        if let Some(head) = self
            .resolve_advertised_peer_head(session, unknown_hashes, minimum, cancellation)
            .await?
        {
            return Ok(head);
        }

        Err(P2pError::Request {
            component: "peer head",
            detail: "connected peers did not advertise or serve a usable head".to_owned(),
        })
    }

    async fn resolve_advertised_peer_head(
        &self,
        session: &P2pSession,
        candidates: Vec<(B512, B256)>,
        minimum: BlockNumber,
        cancellation: &CancellationToken,
    ) -> Result<Option<(BlockNumber, BlockHash)>, P2pError> {
        let candidates = candidates
            .into_iter()
            .take(self.config.material_request_concurrency)
            .filter_map(|(peer_id, hash)| {
                self.network
                    .direct_peers
                    .get(peer_id)
                    .map(|peer| (peer_id, hash, peer))
            })
            .collect::<Vec<_>>();
        let concurrency = candidates.len();
        if concurrency == 0 {
            return Ok(None);
        }
        let timeout = self.config.request_timeout;
        let mut pending = stream::iter(candidates.into_iter().map(
            |(peer_id, hash, peer)| async move {
                (
                    peer_id,
                    hash,
                    request_direct_header(&peer, hash, timeout, cancellation).await,
                )
            },
        ))
        .buffer_unordered(concurrency);
        while let Some((peer_id, hash, result)) = pending.next().await {
            let headers = match result {
                Ok(headers) => headers,
                Err(P2pError::Cancelled) => return Err(P2pError::Cancelled),
                Err(error) => {
                    debug!(%error, "could not resolve the head advertised by an execution peer");
                    if matches!(error, P2pError::Timeout { .. } | P2pError::Request { .. }) {
                        session.handle.disconnect_peer(peer_id);
                        self.network.direct_peers.remove(peer_id);
                    }
                    continue;
                }
            };
            let Some(header) = headers.into_iter().next() else {
                debug!(
                    required_head = minimum.0,
                    "execution peer does not serve its advertised head yet; retaining it for a grace retry"
                );
                continue;
            };
            if header.hash_slow() != hash {
                session.handle.ban_peer(peer_id);
                self.network.direct_peers.remove(peer_id);
            } else if header.number >= minimum.0 {
                let head = (BlockNumber(header.number), BlockHash::new(hash.0));
                session.observe_head(head.0);
                return Ok(Some(head));
            } else {
                debug!(
                    peer_head = header.number,
                    required_head = minimum.0,
                    "execution peer advertised a lagging head; retaining it for a grace retry"
                );
            }
        }
        Ok(None)
    }

    async fn minimum_live_head(
        &self,
        session: &P2pSession,
        minimum: BlockNumber,
        cancellation: &CancellationToken,
    ) -> Result<Option<((BlockNumber, BlockHash), B512)>, P2pError> {
        // Some ETH peers expose only a head hash, and the direct status event
        // can race the request sender becoming visible locally. A header at
        // the consensus-required minimum is enough to start the anchored live
        // lane; subsequent polling advances it without trusting peer status.
        let range = BlockRange::single(minimum);
        let (serving_peer, mut headers) = match self
            .fetch_live_headers_from_untried_peers(
                session,
                range,
                None,
                MaterialRequestPolicy {
                    concurrency: self.config.material_request_concurrency,
                    priority: Priority::High,
                },
                cancellation,
            )
            .await
        {
            Ok(response) => response,
            Err(P2pError::Cancelled) => return Err(P2pError::Cancelled),
            Err(_) => return Ok(None),
        };
        let header = headers
            .pop()
            .expect("one validated minimum live header was returned");
        let head = (minimum, BlockHash::new(header.hash_slow().0));
        session.observe_head(minimum);
        Ok(Some((head, serving_peer)))
    }

    async fn wait_for_peer_head(
        &self,
        session: &P2pSession,
        minimum: BlockNumber,
        cancellation: &CancellationToken,
    ) -> Result<(BlockNumber, BlockHash), P2pError> {
        let mut attempts = 0_usize;
        loop {
            attempts = attempts.saturating_add(1);
            match self
                .discover_peer_head(session, minimum, cancellation)
                .await
            {
                Ok(head) => return Ok(head),
                Err(P2pError::Cancelled) => return Err(P2pError::Cancelled),
                Err(error) if should_retry_session(&self.config, attempts) => {
                    debug!(
                        required_head = minimum.0,
                        %error,
                        "waiting for an execution peer at or above the verified live anchor"
                    );
                    retry_pause(self.config.poll_interval, cancellation).await?;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn reconstruct_reorg(
        &self,
        state: &P2pLiveState,
        head_number: BlockNumber,
        head_hash: BlockHash,
    ) -> Result<(Vec<BlockRef>, Vec<BlockFrame>, BlockRef), P2pError> {
        let descending = self
            .fetch_descending_headers(
                &state.session.fetch,
                head_number,
                head_hash,
                &state.cancellation,
            )
            .await?;
        let (ancestor, reverted) = plan_reorg(&state.recent, &descending)?;
        let applied = if ancestor.number == head_number {
            Vec::new()
        } else {
            let range = BlockRange::new(
                BlockNumber(ancestor.number.0.saturating_add(1)),
                head_number,
            )
            .map_err(|error| P2pError::InvalidResponse(error.to_string()))?;
            if range.len() > MAX_FIXED_RANGE_BLOCKS {
                return Err(P2pError::ReorgTooDeep {
                    maximum: self.config.max_reorg_depth,
                });
            }
            let (frames, _) = self
                .fetch_requested_live_range(
                    &state.session,
                    range,
                    Some(head_hash),
                    &state.request,
                    state.budget,
                    &state.cancellation,
                )
                .await?;
            if frames
                .first()
                .is_none_or(|frame| frame.block.parent_hash != ancestor.hash)
            {
                return Err(P2pError::InvalidResponse(
                    "replacement branch does not join the discovered common ancestor".to_owned(),
                ));
            }
            frames
        };
        let new_tip = applied.last().map_or(ancestor, |frame| frame.block);
        Ok((reverted, applied, new_tip))
    }

    async fn fetch_descending_headers<C>(
        &self,
        fetch: &C,
        head_number: BlockNumber,
        head_hash: BlockHash,
        cancellation: &CancellationToken,
    ) -> Result<Vec<Header>, P2pError>
    where
        C: HeadersClient<Header = Header> + DownloadClient,
    {
        let limit = u64::try_from(self.config.max_reorg_depth)
            .unwrap_or(u64::MAX)
            .saturating_add(1);
        let mut last_error = None;
        for _ in 0..self.config.retries {
            let response = cancellable_peer_request(
                fetch.get_headers(HeadersRequest::falling(
                    BlockHashOrNumber::Hash(B256::from(*head_hash.as_array())),
                    limit,
                )),
                self.config.request_timeout,
                cancellation,
                "reorg headers",
            )
            .await;
            match response {
                Ok(response) => {
                    let (peer, headers) = response.split();
                    match validate_descending_headers(head_number, head_hash, &headers) {
                        Ok(()) => return Ok(headers),
                        Err(error) => {
                            if !matches!(error, P2pError::IncompleteResponse { .. }) {
                                fetch.report_bad_message(peer);
                            }
                            last_error = Some(error);
                        }
                    }
                }
                Err(error) => last_error = Some(error),
            }
            retry_pause(self.config.retry_backoff, cancellation).await?;
        }
        Err(last_error.unwrap_or_else(|| P2pError::Request {
            component: "reorg headers",
            detail: "retry budget exhausted".to_owned(),
        }))
    }

    /// Fetch and verify a small contiguous mainnet range.
    ///
    /// The optional expected tip should come from a verified consensus
    /// execution anchor. Supplying it binds the P2P material to consensus
    /// rather than merely validating internal execution commitments.
    ///
    /// # Errors
    ///
    /// Returns an error if the requested range or budget is invalid, the
    /// network cannot obtain enough peers, a request times out, a peer
    /// response fails commitment validation, or normalization exceeds its
    /// resource budget.
    #[allow(clippy::too_many_lines)]
    pub async fn probe_fixed_range(
        &self,
        range: BlockRange,
        expected_tip: Option<BlockHash>,
        budget: SourceBudget,
        cancellation: CancellationToken,
    ) -> Result<P2pProbeResult, P2pError> {
        let budget = budget.validate().map_err(P2pError::Source)?;
        if range.len() > MAX_FIXED_RANGE_BLOCKS {
            return Err(P2pError::RangeTooLarge {
                requested: range.len(),
                maximum: MAX_FIXED_RANGE_BLOCKS,
            });
        }
        if range.len() > budget.max_frames {
            return Err(P2pError::Source(SourceError::BudgetExceeded {
                resource: "frames",
                limit: budget.max_frames,
                observed: range.len(),
            }));
        }
        let started = Instant::now();
        let advertised = BlockRef {
            number: range.end(),
            hash: expected_tip.unwrap_or(BlockHash::ZERO),
            parent_hash: BlockHash::ZERO,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        };
        let (session, connected_peers) = self
            .connect(advertised, NetworkLane::Probe, &cancellation)
            .await?;
        let telemetry = session.telemetry.clone();
        telemetry.set_range(Some(range));

        let result = async {
            telemetry.set_phase(NetworkPhase::FetchingHeaders);
            let (header_peer, headers) = self
                .fetch_headers(
                    &session.fetch,
                    range,
                    expected_tip,
                    false,
                    MaterialRequestPolicy {
                        concurrency: budget.max_in_flight_requests,
                        priority: Priority::Normal,
                    },
                    &cancellation,
                )
                .await?;
            let hashes = headers.iter().map(Sealable::hash_slow).collect::<Vec<_>>();
            telemetry.set_phase(NetworkPhase::FetchingBodies);
            let (body_peers, bodies) = self
                .fetch_bodies_batched(
                    &session.fetch,
                    &headers,
                    &hashes,
                    MaterialRequestPolicy {
                        concurrency: budget.max_in_flight_requests,
                        priority: Priority::Normal,
                    },
                    &cancellation,
                )
                .await?;
            telemetry.set_phase(NetworkPhase::FetchingReceipts);
            let (receipt_peers, receipts) = self
                .fetch_receipts_batched(
                    &session.fetch,
                    &headers,
                    &bodies,
                    &hashes,
                    MaterialRequestPolicy {
                        concurrency: budget.max_in_flight_requests,
                        priority: Priority::Normal,
                    },
                    &cancellation,
                )
                .await?;
            telemetry.set_phase(NetworkPhase::Ready);
            telemetry.clear_error();
            let frames = normalize_verified(&headers, &bodies, &receipts, budget)?;
            let transactions = bodies.iter().fold(0_u64, |total, body| {
                total.saturating_add(u64::try_from(body.transactions.len()).unwrap_or(u64::MAX))
            });
            let receipt_count = receipts.iter().fold(0_u64, |total, block| {
                total.saturating_add(u64::try_from(block.len()).unwrap_or(u64::MAX))
            });
            let encoded_frame_bytes = frames.iter().fold(0_u64, |total, frame| {
                total.saturating_add(frame.estimated_heap_bytes())
            });
            let mut response_peers = HashSet::from([header_peer]);
            response_peers.extend(body_peers);
            response_peers.extend(receipt_peers);
            Ok(P2pProbeResult {
                range,
                expected_tip,
                metrics: P2pProbeMetrics {
                    reth_version: RETH_VERSION.to_owned(),
                    reth_revision: RETH_REVISION.to_owned(),
                    connected_peers,
                    response_peers: response_peers.len(),
                    blocks: range.len(),
                    transactions,
                    receipts: receipt_count,
                    encoded_frame_bytes,
                    elapsed_milliseconds: u64::try_from(started.elapsed().as_millis())
                        .unwrap_or(u64::MAX),
                },
                frames,
            })
        }
        .await;

        if let Err(error) = &result {
            telemetry.record_attempt();
            telemetry.record_error(error);
        }
        result
    }

    async fn fetch_headers<C>(
        &self,
        fetch: &C,
        range: BlockRange,
        expected_tip: Option<BlockHash>,
        history_proof: bool,
        policy: MaterialRequestPolicy,
        cancellation: &CancellationToken,
    ) -> Result<(B512, Vec<Header>), P2pError>
    where
        C: HeadersClient<Header = Header> + DownloadClient,
    {
        let mut last_error = None;
        for _ in 0..self.config.retries {
            let request =
                HeadersRequest::rising(BlockHashOrNumber::Number(range.start().0), range.len());
            let request_limit = effective_material_concurrency(
                fetch.num_connected_peers(),
                self.config
                    .material_request_concurrency
                    .max(self.config.history_header_request_concurrency),
                policy.concurrency,
            );
            let queued_at = Instant::now();
            let permit = self
                .network
                .request_gate
                .acquire(request_limit, policy.priority, cancellation)
                .await?;
            self.config
                .network_telemetry
                .request_started(queued_at.elapsed());
            let request_started_at = Instant::now();
            let response = cancellable_peer_request(
                fetch.get_headers_with_priority(request, policy.priority),
                self.config.request_timeout,
                cancellation,
                "headers",
            )
            .await;
            drop(permit);
            match response {
                Ok(response) => {
                    let (peer, headers) = response.split();
                    let response_payload_bytes = headers.length();
                    match validate_headers(range, &headers, expected_tip) {
                        Ok(()) => {
                            self.config.network_telemetry.request_succeeded();
                            self.request_metrics.record_header(
                                history_proof,
                                usize::try_from(range.len()).unwrap_or(usize::MAX),
                                headers.len(),
                                response_payload_bytes,
                                request_started_at.elapsed(),
                                P2pRequestOutcome::Succeeded,
                            );
                            return Ok((peer, headers));
                        }
                        Err(error) => {
                            self.config.network_telemetry.request_failed();
                            self.request_metrics.record_header(
                                history_proof,
                                usize::try_from(range.len()).unwrap_or(usize::MAX),
                                headers.len(),
                                response_payload_bytes,
                                request_started_at.elapsed(),
                                P2pRequestOutcome::Failed,
                            );
                            if !matches!(error, P2pError::IncompleteResponse { .. }) {
                                fetch.report_bad_message(peer);
                            }
                            last_error = Some(error);
                        }
                    }
                }
                Err(error) => {
                    record_request_error(&self.config.network_telemetry, &error);
                    self.request_metrics.record_header(
                        history_proof,
                        usize::try_from(range.len()).unwrap_or(usize::MAX),
                        0,
                        0,
                        request_started_at.elapsed(),
                        request_outcome(&error),
                    );
                    last_error = Some(error);
                }
            }
            retry_pause(self.config.retry_backoff, cancellation).await?;
        }
        Err(last_error.unwrap_or_else(|| P2pError::Request {
            component: "headers",
            detail: "retry budget exhausted".to_owned(),
        }))
    }

    async fn request_live_headers(
        &self,
        lease: DirectPeerLease,
        range: BlockRange,
        request_limit: usize,
        priority: Priority,
        cancellation: &CancellationToken,
    ) -> (DirectPeerLease, Duration, Result<Vec<Header>, P2pError>) {
        let queued_at = Instant::now();
        let permit = self
            .network
            .request_gate
            .acquire(request_limit, priority, cancellation)
            .await;
        self.config
            .network_telemetry
            .request_started(queued_at.elapsed());
        let request_started_at = Instant::now();
        let response = match permit {
            Ok(permit) => {
                let response = request_direct_headers(
                    &lease.peer,
                    HeadersRequest::rising(BlockHashOrNumber::Number(range.start().0), range.len()),
                    self.config.request_timeout,
                    cancellation,
                    "headers",
                )
                .await;
                drop(permit);
                response
            }
            Err(error) => Err(error),
        };
        (lease, request_started_at.elapsed(), response)
    }

    #[allow(clippy::too_many_lines)]
    async fn fetch_live_headers_from_untried_peers(
        &self,
        session: &P2pSession,
        range: BlockRange,
        expected_tip: Option<BlockHash>,
        policy: MaterialRequestPolicy,
        cancellation: &CancellationToken,
    ) -> Result<(B512, Vec<Header>), P2pError> {
        let mut tried = HashSet::new();
        let mut last_error = None;
        let connected_peers = session.fetch.num_connected_peers();
        let request_limit = effective_material_concurrency(
            connected_peers,
            self.config.material_request_concurrency,
            policy.concurrency,
        );
        let per_peer_limit = direct_peer_request_limit(request_limit, connected_peers);
        let Some(first) = self
            .network
            .direct_peers
            .acquire_excluding(
                per_peer_limit,
                self.config.request_timeout,
                &tried,
                None,
                cancellation,
            )
            .await?
        else {
            return Err(P2pError::Request {
                component: "headers",
                detail: "all connected peers were unexpectedly excluded".to_owned(),
            });
        };
        tried.insert(first.peer.peer_id);
        let mut pending = FuturesUnordered::new();
        pending.push(self.request_live_headers(
            first,
            range,
            request_limit,
            policy.priority,
            cancellation,
        ));

        loop {
            while pending.len() < request_limit {
                let Some(lease) =
                    self.network
                        .direct_peers
                        .try_acquire_excluding(per_peer_limit, &tried, None)
                else {
                    break;
                };
                tried.insert(lease.peer.peer_id);
                pending.push(self.request_live_headers(
                    lease,
                    range,
                    request_limit,
                    policy.priority,
                    cancellation,
                ));
            }
            if pending.is_empty() {
                match self
                    .network
                    .direct_peers
                    .acquire_excluding(
                        per_peer_limit,
                        self.config.request_timeout,
                        &tried,
                        None,
                        cancellation,
                    )
                    .await
                {
                    Ok(Some(lease)) => {
                        tried.insert(lease.peer.peer_id);
                        pending.push(self.request_live_headers(
                            lease,
                            range,
                            request_limit,
                            policy.priority,
                            cancellation,
                        ));
                        continue;
                    }
                    Ok(None) => {
                        return Err(last_error.unwrap_or_else(|| P2pError::Request {
                            component: "headers",
                            detail: "all connected peers were tried for the live header range"
                                .to_owned(),
                        }));
                    }
                    Err(P2pError::Cancelled) => return Err(P2pError::Cancelled),
                    Err(error) => return Err(last_error.unwrap_or(error)),
                }
            }
            let next = tokio::select! {
                () = cancellation.cancelled() => return Err(P2pError::Cancelled),
                response = pending.next() => response,
                () = self.network.direct_peers.changed.notified(), if pending.len() < request_limit => {
                    continue;
                }
            };
            let Some((mut lease, elapsed, response)) = next else {
                continue;
            };
            let peer_id = lease.peer.peer_id;
            match response {
                Ok(headers) => {
                    let response_payload_bytes = headers.length();
                    match validate_headers(range, &headers, expected_tip) {
                        Ok(()) => {
                            lease.succeeded();
                            self.network.peer_quality.record_success(
                                peer_id,
                                PeerMaterialKind::Header,
                                range.end().0,
                                elapsed,
                            );
                            reward_verified_material_peer(&session.handle, peer_id).await;
                            self.config.network_telemetry.request_succeeded();
                            self.request_metrics.record_header(
                                false,
                                usize::try_from(range.len()).unwrap_or(usize::MAX),
                                headers.len(),
                                response_payload_bytes,
                                elapsed,
                                P2pRequestOutcome::Succeeded,
                            );
                            return Ok((peer_id, headers));
                        }
                        Err(error) => {
                            lease.failed();
                            self.config.network_telemetry.request_failed();
                            self.request_metrics.record_header(
                                false,
                                usize::try_from(range.len()).unwrap_or(usize::MAX),
                                headers.len(),
                                response_payload_bytes,
                                elapsed,
                                P2pRequestOutcome::Failed,
                            );
                            if !matches!(error, P2pError::IncompleteResponse { .. }) {
                                session.handle.ban_peer(peer_id);
                                self.network.direct_peers.remove(peer_id);
                            }
                            last_error = Some(error);
                        }
                    }
                }
                Err(P2pError::Cancelled) => return Err(P2pError::Cancelled),
                Err(error) => {
                    lease.failed();
                    record_request_error(&self.config.network_telemetry, &error);
                    self.request_metrics.record_header(
                        false,
                        usize::try_from(range.len()).unwrap_or(usize::MAX),
                        0,
                        0,
                        elapsed,
                        request_outcome(&error),
                    );
                    if matches!(error, P2pError::Timeout { .. } | P2pError::Request { .. }) {
                        session.handle.disconnect_peer(peer_id);
                        self.network.direct_peers.remove(peer_id);
                    }
                    last_error = Some(error);
                }
            }
        }
    }

    async fn request_live_body(
        &self,
        lease: DirectPeerLease,
        hash: B256,
        request_limit: usize,
        priority: Priority,
        cancellation: &CancellationToken,
    ) -> (
        DirectPeerLease,
        Duration,
        Result<(Vec<BlockBody>, usize), P2pError>,
    ) {
        let queued_at = Instant::now();
        let permit = self
            .network
            .request_gate
            .acquire(request_limit, priority, cancellation)
            .await;
        self.config
            .network_telemetry
            .request_started(queued_at.elapsed());
        let request_started_at = Instant::now();
        let response = match permit {
            Ok(permit) => {
                let response = request_direct_bodies(
                    &lease.peer,
                    std::slice::from_ref(&hash),
                    self.config.request_timeout,
                    cancellation,
                )
                .await;
                drop(permit);
                response
            }
            Err(error) => Err(error),
        };
        (lease, request_started_at.elapsed(), response)
    }

    #[allow(clippy::too_many_lines)]
    async fn fetch_live_body_from_untried_peers(
        &self,
        session: &P2pSession,
        header: &Header,
        hash: B256,
        preferred_peer: Option<B512>,
        policy: MaterialRequestPolicy,
        cancellation: &CancellationToken,
    ) -> Result<(B512, BlockBody), P2pError> {
        let mut wave_attempts = 0_usize;
        let mut incomplete_responses = HashMap::new();
        loop {
            let mut tried = HashSet::new();
            let mut pending_error = None;
            let mut last_error = None;
            let mut pending = FuturesUnordered::new();
            loop {
                let connected_peers = session.fetch.num_connected_peers();
                let request_limit = effective_material_concurrency(
                    connected_peers,
                    self.config.material_request_concurrency,
                    policy.concurrency,
                );
                let per_peer_limit = direct_peer_request_limit(request_limit, connected_peers);
                while pending.len() < request_limit {
                    let Some(lease) = self.network.direct_peers.try_acquire_excluding(
                        per_peer_limit,
                        &tried,
                        preferred_peer,
                    ) else {
                        break;
                    };
                    tried.insert(lease.peer.peer_id);
                    pending.push(self.request_live_body(
                        lease,
                        hash,
                        request_limit,
                        policy.priority,
                        cancellation,
                    ));
                }
                if pending.is_empty() {
                    match self
                        .network
                        .direct_peers
                        .acquire_excluding(
                            per_peer_limit,
                            self.config.request_timeout,
                            &tried,
                            preferred_peer,
                            cancellation,
                        )
                        .await
                    {
                        Ok(Some(lease)) => {
                            tried.insert(lease.peer.peer_id);
                            pending.push(self.request_live_body(
                                lease,
                                hash,
                                request_limit,
                                policy.priority,
                                cancellation,
                            ));
                            continue;
                        }
                        Ok(None) => {
                            let error =
                                pending_error
                                    .or(last_error)
                                    .unwrap_or_else(|| P2pError::Request {
                                        component: "bodies",
                                        detail:
                                            "all connected peers were tried for the latest block"
                                                .to_owned(),
                                    });
                            let Some(_) = pending_live_material_delay(
                                &self.config,
                                &mut wave_attempts,
                                &error,
                            ) else {
                                return Err(error);
                            };
                            debug!(
                                wave = wave_attempts,
                                peers_tried = tried.len(),
                                %error,
                                "latest execution body is not available; cooling tried peers while discovery continues"
                            );
                            break;
                        }
                        Err(P2pError::Cancelled) => return Err(P2pError::Cancelled),
                        Err(error) => {
                            last_error = Some(error);
                            continue;
                        }
                    }
                }
                let next = tokio::select! {
                    () = cancellation.cancelled() => return Err(P2pError::Cancelled),
                    response = pending.next() => response,
                    () = self.network.direct_peers.changed.notified(), if pending.len() < request_limit => {
                        continue;
                    }
                };
                let Some((mut lease, elapsed, response)) = next else {
                    continue;
                };
                let peer_id = lease.peer.peer_id;
                match response {
                    Ok((mut bodies, response_payload_bytes)) => {
                        self.request_metrics.add_response_payload_bytes(
                            P2pRequestKind::Bodies,
                            response_payload_bytes,
                        );
                        match validate_bodies(std::slice::from_ref(header), &bodies) {
                            Ok(()) => {
                                lease.succeeded();
                                self.network.peer_quality.record_success(
                                    peer_id,
                                    PeerMaterialKind::Body,
                                    header.number,
                                    elapsed,
                                );
                                reward_verified_material_peer(&session.handle, peer_id).await;
                                self.config.network_telemetry.request_succeeded();
                                self.request_metrics.record(
                                    P2pRequestKind::Bodies,
                                    1,
                                    bodies.len(),
                                    elapsed,
                                    P2pRequestOutcome::Succeeded,
                                );
                                return Ok((
                                    peer_id,
                                    bodies.pop().expect("one validated live body"),
                                ));
                            }
                            Err(error) => {
                                self.config.network_telemetry.request_failed();
                                self.request_metrics.record(
                                    P2pRequestKind::Bodies,
                                    1,
                                    bodies.len(),
                                    elapsed,
                                    P2pRequestOutcome::Failed,
                                );
                                if matches!(error, P2pError::IncompleteResponse { .. }) {
                                    // An empty latest-material response is not
                                    // malicious, but immediately selecting the
                                    // same peer again can spam a lagging or
                                    // non-serving session and starve new peers.
                                    // Apply the pool-local exponential
                                    // cooldown without changing Reth
                                    // reputation or banning the peer. Repeated
                                    // misses rotate the session below so newly
                                    // discovered peers get an opportunity.
                                    lease.failed();
                                    if repeated_incomplete_response(
                                        &mut incomplete_responses,
                                        peer_id,
                                        self.config.retries,
                                    ) {
                                        debug!(
                                            attempts = self.config.retries,
                                            "rotating execution peer after repeated incomplete latest body responses"
                                        );
                                        session.handle.disconnect_peer(peer_id);
                                        self.network.direct_peers.remove(peer_id);
                                        incomplete_responses.remove(&peer_id);
                                    }
                                    pending_error = Some(error);
                                } else {
                                    lease.failed();
                                    session.handle.ban_peer(peer_id);
                                    self.network.direct_peers.remove(peer_id);
                                    last_error = Some(error);
                                }
                            }
                        }
                    }
                    Err(P2pError::Cancelled) => return Err(P2pError::Cancelled),
                    Err(error) => {
                        lease.failed();
                        record_request_error(&self.config.network_telemetry, &error);
                        self.request_metrics.record(
                            P2pRequestKind::Bodies,
                            1,
                            0,
                            elapsed,
                            request_outcome(&error),
                        );
                        if matches!(error, P2pError::Timeout { .. }) {
                            session.handle.disconnect_peer(peer_id);
                            self.network.direct_peers.remove(peer_id);
                        }
                        last_error = Some(error);
                    }
                }
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn fetch_live_receipts_from_untried_peers(
        &self,
        session: &P2pSession,
        material: LiveReceiptMaterial<'_>,
        policy: MaterialRequestPolicy,
        cancellation: &CancellationToken,
    ) -> Result<(B512, Vec<Receipt>), P2pError> {
        let mut wave_attempts = 0_usize;
        let mut incomplete_responses = HashMap::new();
        loop {
            let mut tried = HashSet::new();
            let mut pending_error = None;
            let mut last_error = None;
            loop {
                let connected_peers = session.fetch.num_connected_peers();
                let request_limit = effective_material_concurrency(
                    connected_peers,
                    self.config.material_request_concurrency,
                    policy.concurrency,
                );
                let per_peer_limit = direct_peer_request_limit(request_limit, connected_peers);
                let queued_at = Instant::now();
                let Some(mut lease) = self
                    .network
                    .direct_peers
                    .acquire_excluding(
                        per_peer_limit,
                        self.config.request_timeout,
                        &tried,
                        Some(material.preferred_peer),
                        cancellation,
                    )
                    .await?
                else {
                    let error = pending_error
                        .or(last_error)
                        .unwrap_or_else(|| P2pError::Request {
                            component: "receipts",
                            detail: "all connected peers were tried for the latest block"
                                .to_owned(),
                        });
                    let Some(_) =
                        pending_live_material_delay(&self.config, &mut wave_attempts, &error)
                    else {
                        return Err(error);
                    };
                    debug!(
                        wave = wave_attempts,
                        peers_tried = tried.len(),
                        %error,
                        "latest execution receipts are not available; cooling tried peers while discovery continues"
                    );
                    break;
                };
                let peer_id = lease.peer.peer_id;
                tried.insert(peer_id);
                let permit = self
                    .network
                    .request_gate
                    .acquire(request_limit, policy.priority, cancellation)
                    .await?;
                self.config
                    .network_telemetry
                    .request_started(queued_at.elapsed());
                let request_started_at = Instant::now();
                let response = request_direct_receipts(
                    &lease.peer,
                    std::slice::from_ref(&material.hash),
                    self.config.request_timeout,
                    cancellation,
                )
                .await;
                drop(permit);
                match response {
                    Ok(mut response) => {
                        self.request_metrics.add_response_payload_bytes(
                            P2pRequestKind::Receipts,
                            response.response_payload_bytes,
                        );
                        match validate_receipts(
                            std::slice::from_ref(material.header),
                            std::slice::from_ref(material.body),
                            &response.receipts,
                        ) {
                            Ok(()) => {
                                lease.succeeded();
                                self.network.peer_quality.record_success(
                                    peer_id,
                                    PeerMaterialKind::Receipts,
                                    material.header.number,
                                    request_started_at.elapsed(),
                                );
                                reward_verified_material_peer(&session.handle, peer_id).await;
                                self.config.network_telemetry.request_succeeded();
                                self.request_metrics.record_batch(
                                    P2pRequestKind::Receipts,
                                    response.physical_requests,
                                    response.requested_block_hashes,
                                    response.receipts.len(),
                                    request_started_at.elapsed(),
                                    P2pRequestOutcome::Succeeded,
                                );
                                return Ok((
                                    peer_id,
                                    response
                                        .receipts
                                        .pop()
                                        .expect("one validated live receipt block"),
                                ));
                            }
                            Err(error) => {
                                self.config.network_telemetry.request_failed();
                                self.request_metrics.record_batch(
                                    P2pRequestKind::Receipts,
                                    response.physical_requests.max(1),
                                    response.requested_block_hashes,
                                    response.receipts.len(),
                                    request_started_at.elapsed(),
                                    P2pRequestOutcome::Failed,
                                );
                                if matches!(error, P2pError::IncompleteResponse { .. }) {
                                    // See the matching live-body path: rotate
                                    // immediately to fresh sessions and retry
                                    // this peer only after a local cooldown.
                                    lease.failed();
                                    if repeated_incomplete_response(
                                        &mut incomplete_responses,
                                        peer_id,
                                        self.config.retries,
                                    ) {
                                        debug!(
                                            attempts = self.config.retries,
                                            "rotating execution peer after repeated incomplete latest receipt responses"
                                        );
                                        session.handle.disconnect_peer(peer_id);
                                        self.network.direct_peers.remove(peer_id);
                                        incomplete_responses.remove(&peer_id);
                                    }
                                    pending_error = Some(error);
                                } else {
                                    lease.failed();
                                    session.handle.ban_peer(peer_id);
                                    self.network.direct_peers.remove(peer_id);
                                    last_error = Some(error);
                                }
                            }
                        }
                    }
                    Err(P2pError::Cancelled) => return Err(P2pError::Cancelled),
                    Err(error) => {
                        let incomplete = matches!(error, P2pError::IncompleteResponse { .. });
                        record_request_error(&self.config.network_telemetry, &error);
                        self.request_metrics.record(
                            P2pRequestKind::Receipts,
                            1,
                            0,
                            request_started_at.elapsed(),
                            request_outcome(&error),
                        );
                        if matches!(error, P2pError::Timeout { .. }) {
                            session.handle.disconnect_peer(peer_id);
                            self.network.direct_peers.remove(peer_id);
                        }
                        if incomplete {
                            lease.failed();
                            if repeated_incomplete_response(
                                &mut incomplete_responses,
                                peer_id,
                                self.config.retries,
                            ) {
                                debug!(
                                    attempts = self.config.retries,
                                    "rotating execution peer after repeated incomplete latest receipt responses"
                                );
                                session.handle.disconnect_peer(peer_id);
                                self.network.direct_peers.remove(peer_id);
                                incomplete_responses.remove(&peer_id);
                            }
                            pending_error = Some(error);
                        } else {
                            lease.failed();
                            last_error = Some(error);
                        }
                    }
                }
            }
        }
    }

    async fn fetch_bodies<C>(
        &self,
        fetch: &C,
        range: BlockRange,
        headers: &[Header],
        hashes: &[B256],
        policy: MaterialRequestPolicy,
        cancellation: &CancellationToken,
    ) -> Result<(B512, Vec<BlockBody>), P2pError>
    where
        C: BodiesClient<Body = BlockBody> + DownloadClient,
    {
        let mut last_error = None;
        let attempts = if headers.len() > 1 {
            1
        } else {
            self.config.retries
        };
        for attempt in 0..attempts {
            let request_limit = effective_material_concurrency(
                fetch.num_connected_peers(),
                self.config.material_request_concurrency,
                policy.concurrency,
            );
            let queued_at = Instant::now();
            let permit = self
                .network
                .request_gate
                .acquire(request_limit, policy.priority, cancellation)
                .await?;
            self.config
                .network_telemetry
                .request_started(queued_at.elapsed());
            let request_started_at = Instant::now();
            let response = cancellable_peer_request(
                fetch.get_block_bodies_with_priority_and_range_hint(
                    hashes.to_vec(),
                    policy.priority,
                    Some(range.start().0..=range.end().0),
                ),
                self.config.request_timeout,
                cancellation,
                "bodies",
            )
            .await;
            drop(permit);
            match response {
                Ok(response) => {
                    let (peer, bodies) = response.split();
                    self.request_metrics
                        .add_response_payload_bytes(P2pRequestKind::Bodies, bodies.length());
                    match validate_bodies(headers, &bodies) {
                        Ok(()) => {
                            self.config.network_telemetry.request_succeeded();
                            self.request_metrics.record(
                                P2pRequestKind::Bodies,
                                hashes.len(),
                                bodies.len(),
                                request_started_at.elapsed(),
                                P2pRequestOutcome::Succeeded,
                            );
                            return Ok((peer, bodies));
                        }
                        Err(error) => {
                            self.config.network_telemetry.request_failed();
                            self.request_metrics.record(
                                P2pRequestKind::Bodies,
                                hashes.len(),
                                bodies.len(),
                                request_started_at.elapsed(),
                                P2pRequestOutcome::Failed,
                            );
                            if !matches!(error, P2pError::IncompleteResponse { .. }) {
                                fetch.report_bad_message(peer);
                            }
                            last_error = Some(error);
                        }
                    }
                }
                Err(error) => {
                    record_request_error(&self.config.network_telemetry, &error);
                    self.request_metrics.record(
                        P2pRequestKind::Bodies,
                        hashes.len(),
                        0,
                        request_started_at.elapsed(),
                        request_outcome(&error),
                    );
                    last_error = Some(error);
                }
            }
            if attempt.saturating_add(1) < attempts {
                retry_pause(self.config.retry_backoff, cancellation).await?;
            }
        }
        Err(last_error.unwrap_or_else(|| P2pError::Request {
            component: "bodies",
            detail: "retry budget exhausted".to_owned(),
        }))
    }

    async fn fetch_bodies_batched<C>(
        &self,
        fetch: &C,
        headers: &[Header],
        hashes: &[B256],
        policy: MaterialRequestPolicy,
        cancellation: &CancellationToken,
    ) -> Result<(HashSet<B512>, Vec<BlockBody>), P2pError>
    where
        C: BodiesClient<Body = BlockBody> + DownloadClient,
    {
        if headers.len() != hashes.len() {
            return Err(P2pError::InvalidResponse(
                "body batch header/hash length mismatch".to_owned(),
            ));
        }
        let batch_blocks = self.material_tuning.body_blocks();
        let requests = headers
            .chunks(batch_blocks)
            .zip(hashes.chunks(batch_blocks))
            .map(|(header_chunk, hash_chunk)| (header_chunk.to_vec(), hash_chunk.to_vec()))
            .collect::<Vec<_>>();
        let concurrency = effective_material_concurrency(
            fetch.num_connected_peers(),
            self.config.material_request_concurrency,
            policy.concurrency,
        );
        let outcomes = stream::iter(requests)
            .map(|(header_chunk, hash_chunk)| async move {
                let result = history_header_range(&header_chunk, "body")
                    .map(|range| (range, &header_chunk, &hash_chunk));
                let result = match result {
                    Ok((range, headers, hashes)) => {
                        self.fetch_bodies(fetch, range, headers, hashes, policy, cancellation)
                            .await
                    }
                    Err(error) => Err(error),
                };
                (header_chunk, hash_chunk, result)
            })
            .buffered(concurrency)
            .collect::<Vec<_>>()
            .await;

        let mut completed = Vec::new();
        let mut fallback = Vec::new();
        for (header_chunk, hash_chunk, result) in outcomes {
            match result {
                Ok((peer, bodies)) => completed.push((header_chunk[0].number, peer, bodies)),
                Err(error) if header_chunk.len() > 1 => {
                    debug!(
                        blocks = header_chunk.len(),
                        %error,
                        "splitting an unsuccessful P2P body request into single-block requests"
                    );
                    fallback.extend(header_chunk.into_iter().zip(hash_chunk));
                }
                Err(error) => {
                    self.material_tuning.body_failed();
                    return Err(error);
                }
            }
        }
        let recovered = stream::iter(fallback)
            .map(|(header, hash)| async move {
                let range = BlockRange::single(BlockNumber(header.number));
                let result = self
                    .fetch_bodies(
                        fetch,
                        range,
                        std::slice::from_ref(&header),
                        std::slice::from_ref(&hash),
                        policy,
                        cancellation,
                    )
                    .await;
                (header.number, result)
            })
            .buffered(concurrency)
            .collect::<Vec<_>>()
            .await;
        let recovered_any = !recovered.is_empty();
        for (number, result) in recovered {
            match result {
                Ok((peer, bodies)) => completed.push((number, peer, bodies)),
                Err(error) => {
                    self.material_tuning.body_failed();
                    return Err(error);
                }
            }
        }
        if recovered_any {
            self.material_tuning.body_failed();
        } else {
            self.material_tuning.body_succeeded();
        }
        completed.sort_by_key(|(number, _, _)| *number);
        let mut peers = HashSet::new();
        let mut bodies = Vec::with_capacity(headers.len());
        for (_, peer, mut batch) in completed {
            peers.insert(peer);
            bodies.append(&mut batch);
        }
        validate_bodies(headers, &bodies)?;
        Ok((peers, bodies))
    }

    async fn fetch_receipts<C>(
        &self,
        fetch: &C,
        headers: &[Header],
        bodies: &[BlockBody],
        hashes: &[B256],
        policy: MaterialRequestPolicy,
        cancellation: &CancellationToken,
    ) -> Result<(B512, Vec<Vec<Receipt>>), P2pError>
    where
        C: ReceiptsClient<Receipt = Receipt> + DownloadClient,
    {
        let mut last_error = None;
        let attempts = if headers.len() > 1 {
            1
        } else {
            self.config.retries
        };
        for attempt in 0..attempts {
            let request_limit = effective_material_concurrency(
                fetch.num_connected_peers(),
                self.config.material_request_concurrency,
                policy.concurrency,
            );
            let queued_at = Instant::now();
            let permit = self
                .network
                .request_gate
                .acquire(request_limit, policy.priority, cancellation)
                .await?;
            self.config
                .network_telemetry
                .request_started(queued_at.elapsed());
            let request_started_at = Instant::now();
            let response = cancellable_peer_request(
                fetch.get_receipts_with_priority(hashes.to_vec(), policy.priority),
                self.config.request_timeout,
                cancellation,
                "receipts",
            )
            .await;
            drop(permit);
            match response {
                Ok(response) => {
                    let (peer, response) = response.split();
                    self.request_metrics.add_response_payload_bytes(
                        P2pRequestKind::Receipts,
                        response.receipts.length(),
                    );
                    match validate_receipts(headers, bodies, &response.receipts) {
                        Ok(()) => {
                            self.config.network_telemetry.request_succeeded();
                            self.request_metrics.record(
                                P2pRequestKind::Receipts,
                                hashes.len(),
                                response.receipts.len(),
                                request_started_at.elapsed(),
                                P2pRequestOutcome::Succeeded,
                            );
                            return Ok((peer, response.receipts));
                        }
                        Err(error) => {
                            self.config.network_telemetry.request_failed();
                            self.request_metrics.record(
                                P2pRequestKind::Receipts,
                                hashes.len(),
                                response.receipts.len(),
                                request_started_at.elapsed(),
                                P2pRequestOutcome::Failed,
                            );
                            if !matches!(error, P2pError::IncompleteResponse { .. }) {
                                fetch.report_bad_message(peer);
                            }
                            last_error = Some(error);
                        }
                    }
                }
                Err(error) => {
                    record_request_error(&self.config.network_telemetry, &error);
                    self.request_metrics.record(
                        P2pRequestKind::Receipts,
                        hashes.len(),
                        0,
                        request_started_at.elapsed(),
                        request_outcome(&error),
                    );
                    last_error = Some(error);
                }
            }
            if attempt.saturating_add(1) < attempts {
                retry_pause(self.config.retry_backoff, cancellation).await?;
            }
        }
        Err(last_error.unwrap_or_else(|| P2pError::Request {
            component: "receipts",
            detail: "retry budget exhausted".to_owned(),
        }))
    }

    #[allow(clippy::too_many_lines)]
    async fn fetch_receipts_batched<C>(
        &self,
        fetch: &C,
        headers: &[Header],
        bodies: &[BlockBody],
        hashes: &[B256],
        policy: MaterialRequestPolicy,
        cancellation: &CancellationToken,
    ) -> Result<(HashSet<B512>, Vec<Vec<Receipt>>), P2pError>
    where
        C: ReceiptsClient<Receipt = Receipt> + DownloadClient,
    {
        if headers.len() != bodies.len() || headers.len() != hashes.len() {
            return Err(P2pError::InvalidResponse(
                "receipt batch material length mismatch".to_owned(),
            ));
        }
        let batch_blocks = self.material_tuning.receipt_blocks();
        let requests = headers
            .chunks(batch_blocks)
            .zip(bodies.chunks(batch_blocks))
            .zip(hashes.chunks(batch_blocks))
            .map(|((header_chunk, body_chunk), hash_chunk)| {
                (
                    header_chunk.to_vec(),
                    body_chunk.to_vec(),
                    hash_chunk.to_vec(),
                )
            })
            .collect::<Vec<_>>();
        let concurrency = effective_material_concurrency(
            fetch.num_connected_peers(),
            self.config.material_request_concurrency,
            policy.concurrency,
        );
        let outcomes = stream::iter(requests)
            .map(|(header_chunk, body_chunk, hash_chunk)| async move {
                let result = self
                    .fetch_receipts(
                        fetch,
                        &header_chunk,
                        &body_chunk,
                        &hash_chunk,
                        policy,
                        cancellation,
                    )
                    .await;
                (header_chunk, body_chunk, hash_chunk, result)
            })
            .buffered(concurrency)
            .collect::<Vec<_>>()
            .await;

        let mut completed = Vec::new();
        let mut fallback = Vec::new();
        for (header_chunk, body_chunk, hash_chunk, result) in outcomes {
            match result {
                Ok((peer, receipts)) => completed.push((header_chunk[0].number, peer, receipts)),
                Err(error) if header_chunk.len() > 1 => {
                    debug!(
                        blocks = header_chunk.len(),
                        %error,
                        "splitting an unsuccessful P2P receipt request into single-block requests"
                    );
                    fallback.extend(
                        header_chunk
                            .into_iter()
                            .zip(body_chunk)
                            .zip(hash_chunk)
                            .map(|((header, body), hash)| (header, body, hash)),
                    );
                }
                Err(error) => {
                    self.material_tuning.receipts_failed();
                    return Err(error);
                }
            }
        }
        let recovered = stream::iter(fallback)
            .map(|(header, body, hash)| async move {
                let result = self
                    .fetch_receipts(
                        fetch,
                        std::slice::from_ref(&header),
                        std::slice::from_ref(&body),
                        std::slice::from_ref(&hash),
                        policy,
                        cancellation,
                    )
                    .await;
                (header.number, result)
            })
            .buffered(concurrency)
            .collect::<Vec<_>>()
            .await;
        let recovered_any = !recovered.is_empty();
        for (number, result) in recovered {
            match result {
                Ok((peer, receipts)) => completed.push((number, peer, receipts)),
                Err(error) => {
                    self.material_tuning.receipts_failed();
                    return Err(error);
                }
            }
        }
        if recovered_any {
            self.material_tuning.receipts_failed();
        } else {
            self.material_tuning.receipts_succeeded();
        }
        completed.sort_by_key(|(number, _, _)| *number);
        let mut peers = HashSet::new();
        let mut receipts = Vec::with_capacity(headers.len());
        for (_, peer, mut batch) in completed {
            peers.insert(peer);
            receipts.append(&mut batch);
        }
        validate_receipts(headers, bodies, &receipts)?;
        Ok((peers, receipts))
    }
}

impl RethP2pHistorySource {
    /// Construct a bounded Mainnet history bridge without opening sockets.
    ///
    /// `available` is an operator-controlled cost bound, not a claim that
    /// arbitrary peers retain the entire range. The range must end at the
    /// consensus-verified execution anchor.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid P2P configuration, an unfinalized or
    /// inconsistent anchor, or a range that does not end at that anchor.
    pub fn mainnet(
        config: RethP2pConfig,
        available: BlockRange,
        anchor: P2pHistoryAnchor,
    ) -> Result<Self, P2pError> {
        Self::new(RethP2pSource::mainnet(config)?, available, anchor, false)
    }

    /// Construct a bounded Mainnet history bridge that can borrow the active
    /// live source's established persistent execution-peer pool.
    ///
    /// # Errors
    ///
    /// Returns an error when the range and finalized anchor disagree.
    pub fn from_live_source(
        source: RethP2pSource,
        available: BlockRange,
        anchor: P2pHistoryAnchor,
    ) -> Result<Self, P2pError> {
        Self::new(source, available, anchor, true)
    }

    /// Construct a historical source around an existing persistent manager
    /// without waiting for a separately started live lane.
    ///
    /// This is useful for bounded P2P-only diagnostics and benchmarks. Node
    /// operation should normally use [`Self::from_live_source`].
    ///
    /// # Errors
    ///
    /// Returns an error when the range and finalized anchor disagree.
    pub fn from_persistent_source(
        source: RethP2pSource,
        available: BlockRange,
        anchor: P2pHistoryAnchor,
    ) -> Result<Self, P2pError> {
        Self::new(source, available, anchor, false)
    }

    fn new(
        source: RethP2pSource,
        available: BlockRange,
        anchor: P2pHistoryAnchor,
        prefer_shared_live_session: bool,
    ) -> Result<Self, P2pError> {
        if available.end() != anchor.block.number {
            return Err(P2pError::InvalidConfig(
                "P2P history bridge range must end at its execution anchor".to_owned(),
            ));
        }
        if anchor.consensus.finality != Finality::Finalized
            || anchor.consensus.execution_block_hash != anchor.block.hash
        {
            return Err(P2pError::InvalidConfig(
                "P2P history bridge requires a matching finalized consensus anchor".to_owned(),
            ));
        }
        let mut descriptor = source.descriptor.clone();
        descriptor.range = Some(available);
        descriptor.finality = FinalityModel::Finalized;
        descriptor.partitioning =
            Partitioning::SourceDefined("consensus-anchored-suffix".to_owned());
        descriptor.priority = u16::MAX;
        descriptor.schema_version =
            format!("reth-p2p-finalized-history.v4+{RETH_VERSION}.{RETH_REVISION}");
        Ok(Self {
            source,
            descriptor,
            anchor,
            prefer_shared_live_session,
            anchored_headers: Arc::new(tokio::sync::Mutex::new(None)),
            history_session: Arc::new(tokio::sync::Mutex::new(None)),
            acquisition_metrics: Arc::new(Mutex::new(SourceAcquisitionMetrics::default())),
        })
    }

    async fn acquire_history_network(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<P2pSession, P2pError> {
        let minimum = self.source.config.minimum_peers;
        let shared = if self.prefer_shared_live_session {
            self.wait_for_shared_live_session(minimum, cancellation)
                .await?
        } else {
            None
        };
        let session = if let Some(shared) = shared {
            debug!(
                connected_peers = shared.fetch.num_connected_peers(),
                "borrowing the persistent execution peer pool for history"
            );
            shared
        } else {
            self.source
                .connect(self.anchor.block, NetworkLane::History, cancellation)
                .await?
                .0
        };
        Ok(session)
    }

    async fn wait_for_shared_live_session(
        &self,
        minimum: usize,
        cancellation: &CancellationToken,
    ) -> Result<Option<P2pSession>, P2pError> {
        if self.source.network.current_session().await.is_none() {
            return Ok(None);
        }
        let deadline = Instant::now() + self.source.config.peer_wait_timeout;
        loop {
            if let Some(shared) = self.source.borrow_live_session().await
                && shared.fetch.num_connected_peers() >= minimum
            {
                return Ok(Some(shared));
            }
            let now = Instant::now();
            if now >= deadline {
                debug!(
                    minimum_peers = minimum,
                    "persistent execution peer pool did not become available before history fallback"
                );
                return Ok(None);
            }
            let delay = self
                .source
                .config
                .poll_interval
                .min(deadline.saturating_duration_since(now));
            tokio::select! {
                () = cancellation.cancelled() => return Err(P2pError::Cancelled),
                () = tokio::time::sleep(delay) => {}
            }
        }
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn run_bridge(
        &self,
        proof_start: BlockNumber,
        material_end: BlockNumber,
        requested: BlockRange,
        request: DataRequest,
        budget: SourceBudget,
        cancellation: CancellationToken,
        output: tokio::sync::mpsc::Sender<Result<BlockFrame, SourceError>>,
    ) -> Result<(), P2pError> {
        let proof = BlockRange::new(proof_start, self.anchor.block.number)
            .map_err(|error| P2pError::InvalidConfig(error.to_string()))?;
        let retained = BlockRange::new(proof_start, material_end)
            .map_err(|error| P2pError::InvalidConfig(error.to_string()))?;
        if retained.end() > proof.end()
            || requested.start() < retained.start()
            || requested.end() > retained.end()
        {
            return Err(P2pError::InvalidConfig(
                "P2P material range exceeds its retained anchored proof hashes".to_owned(),
            ));
        }
        let proof_bytes = proof.len().saturating_mul(32);
        if proof_bytes > budget.max_input_bytes {
            return Err(P2pError::Source(SourceError::BudgetExceeded {
                resource: "anchored_header_hashes",
                limit: budget.max_input_bytes,
                observed: proof_bytes,
            }));
        }

        // Hold the cache lock across construction. Historical material streams
        // are opened concurrently; without this single-flight section each
        // stream could independently download the same proof suffix.
        let (proof_session, expected_tip) = {
            let mut cached = self.anchored_headers.lock().await;
            if cached
                .as_ref()
                .is_none_or(|cached| !cached.covers(proof, retained))
            {
                let (session, assembled) = self
                    .connect_and_fetch_anchored_headers(
                        proof,
                        retained,
                        budget.max_in_flight_requests,
                        &cancellation,
                    )
                    .await?;
                let expected_tip = assembled.expected_hash(requested.end()).ok_or_else(|| {
                    P2pError::InvalidResponse(
                        "anchored header proof omitted the requested material tip".to_owned(),
                    )
                })?;
                *cached = Some(assembled);
                (Some(session), expected_tip)
            } else {
                let expected_tip = cached
                    .as_ref()
                    .and_then(|cached| cached.expected_hash(requested.end()))
                    .ok_or_else(|| {
                        P2pError::InvalidResponse(
                            "cached anchored header proof omitted the requested material tip"
                                .to_owned(),
                        )
                    })?;
                (None, expected_tip)
            }
        };
        let mut session = if let Some(session) = proof_session {
            session
        } else if let Some(session) = self.history_session.lock().await.take() {
            session
        } else {
            self.connect_history_session(&cancellation).await?
        };
        session.set_range(Some(requested));
        session.set_phase(NetworkPhase::FetchingHeaders);
        let (header_peer, headers) = self
            .source
            .fetch_headers(
                &session.fetch,
                requested,
                Some(expected_tip),
                false,
                MaterialRequestPolicy {
                    concurrency: budget.max_in_flight_requests,
                    priority: Priority::Normal,
                },
                &cancellation,
            )
            .await?;
        reward_verified_material_peer(&session.handle, header_peer).await;
        let result = self
            .stream_anchored_frames(
                &mut session,
                &headers,
                requested,
                &request,
                budget,
                &cancellation,
                &output,
            )
            .await;
        if result.is_ok() {
            session.set_range(None);
            session.set_phase(NetworkPhase::Ready);
            session.clear_error();
        }
        // Every history chunk borrows the same persistent physical manager.
        // Finishing, cancelling, or failing one logical chunk must never tear
        // down connections that sibling chunks and the live lane still use.
        if result.is_ok() && !cancellation.is_cancelled() {
            let mut retained = self.history_session.lock().await;
            if retained.is_none() {
                *retained = Some(session);
            }
        }
        result
    }

    #[allow(clippy::too_many_arguments)]
    async fn stream_anchored_frames(
        &self,
        session: &mut P2pSession,
        headers: &[Header],
        requested: BlockRange,
        request: &DataRequest,
        budget: SourceBudget,
        cancellation: &CancellationToken,
        output: &tokio::sync::mpsc::Sender<Result<BlockFrame, SourceError>>,
    ) -> Result<(), P2pError> {
        let requested_len = usize::try_from(requested.len())
            .map_err(|_| P2pError::InvalidConfig("history range is too large".to_owned()))?;
        if headers.len() != requested_len {
            return Err(P2pError::InvalidResponse(
                "material header response does not cover the requested range".to_owned(),
            ));
        }
        let mut input_bytes = 0_u64;
        for header_chunk in headers.chunks(MAX_HISTORY_MATERIAL_WINDOW_BLOCKS) {
            let hashes = header_chunk
                .iter()
                .map(Sealable::hash_slow)
                .collect::<Vec<_>>();
            let remaining = budget.max_input_bytes.saturating_sub(input_bytes);
            if remaining == 0 {
                return Err(P2pError::Source(SourceError::BudgetExceeded {
                    resource: "input_bytes",
                    limit: budget.max_input_bytes,
                    observed: input_bytes.saturating_add(1),
                }));
            }
            let window_budget = SourceBudget {
                max_input_bytes: remaining,
                ..budget
            };
            let mut frames = if header_only_request(request) {
                normalize_verified_headers(header_chunk, window_budget)?
            } else if header_and_body_only_request(request) {
                self.fetch_history_body_frames(
                    session,
                    header_chunk,
                    &hashes,
                    window_budget,
                    cancellation,
                )
                .await?
            } else if sparse_log_scope(request).is_some() {
                self.fetch_sparse_log_frames(
                    session,
                    header_chunk,
                    &hashes,
                    request,
                    window_budget,
                    cancellation,
                )
                .await?
            } else {
                let (bodies, receipts) = self
                    .fetch_history_material(
                        session,
                        header_chunk,
                        &hashes,
                        budget.max_in_flight_requests,
                        cancellation,
                    )
                    .await?;
                normalize_verified_for_request(
                    header_chunk,
                    &bodies,
                    &receipts,
                    request,
                    window_budget,
                )?
            };
            for frame in &mut frames {
                mark_anchored_history_finalized(frame, &self.anchor.consensus);
                input_bytes = input_bytes.saturating_add(frame.estimated_heap_bytes());
            }
            {
                let mut metrics = self
                    .acquisition_metrics
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                metrics.acquired_frames = metrics
                    .acquired_frames
                    .saturating_add(u64::try_from(frames.len()).unwrap_or(u64::MAX));
                metrics.normalized_bytes = metrics.normalized_bytes.saturating_add(
                    frames.iter().fold(0_u64, |total, frame| {
                        total.saturating_add(frame.estimated_heap_bytes())
                    }),
                );
            }
            for frame in frames {
                if output.send(Ok(frame)).await.is_err() {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    async fn fetch_history_body_frames(
        &self,
        session: &P2pSession,
        headers: &[Header],
        hashes: &[B256],
        budget: SourceBudget,
        cancellation: &CancellationToken,
    ) -> Result<Vec<BlockFrame>, P2pError> {
        session.set_phase(NetworkPhase::FetchingBodies);
        let (peers, bodies) = self
            .source
            .fetch_bodies_batched(
                &session.fetch,
                headers,
                hashes,
                MaterialRequestPolicy {
                    concurrency: budget.max_in_flight_requests,
                    priority: Priority::Normal,
                },
                cancellation,
            )
            .await?;
        for peer in peers {
            reward_verified_material_peer(&session.handle, peer).await;
        }
        normalize_verified_bodies(headers, &bodies, budget)
    }

    async fn fetch_sparse_log_frames(
        &self,
        session: &mut P2pSession,
        headers: &[Header],
        hashes: &[B256],
        request: &DataRequest,
        budget: SourceBudget,
        cancellation: &CancellationToken,
    ) -> Result<Vec<BlockFrame>, P2pError> {
        let range = headers
            .first()
            .zip(headers.last())
            .and_then(|(first, last)| {
                BlockRange::new(BlockNumber(first.number), BlockNumber(last.number)).ok()
            });
        let mut attempts = 0_usize;
        loop {
            attempts = attempts.saturating_add(1);
            session.set_range(range);
            session.record_attempt();
            match self
                .fetch_sparse_log_frames_once(
                    session,
                    headers,
                    hashes,
                    request,
                    budget,
                    cancellation,
                )
                .await
            {
                Ok(frames) => {
                    session.clear_error();
                    return Ok(frames);
                }
                Err(P2pError::Cancelled) => return Err(P2pError::Cancelled),
                Err(error) => {
                    session.record_error(&error);
                    if should_retry_session_error(&self.source.config, attempts, &error) {
                        let delay = session_retry_delay(&self.source.config, attempts);
                        debug!(
                            attempt = attempts,
                            ?delay,
                            %error,
                            "retrying sparse history material on the persistent peer pool"
                        );
                        retry_pause(delay, cancellation).await?;
                    } else {
                        return Err(error);
                    }
                }
            }
        }
    }

    async fn fetch_sparse_log_frames_once(
        &self,
        session: &P2pSession,
        headers: &[Header],
        hashes: &[B256],
        request: &DataRequest,
        budget: SourceBudget,
        cancellation: &CancellationToken,
    ) -> Result<Vec<BlockFrame>, P2pError> {
        let scope = sparse_log_scope(request).ok_or_else(|| {
            P2pError::InvalidConfig("request is not eligible for sparse log acquisition".to_owned())
        })?;
        let bloom_positive_indices = headers
            .iter()
            .enumerate()
            .filter_map(|(index, header)| header_bloom_matches(scope, header).then_some(index))
            .collect::<Vec<_>>();
        let positive_headers = bloom_positive_indices
            .iter()
            .map(|index| headers[*index].clone())
            .collect::<Vec<_>>();
        let positive_hashes = bloom_positive_indices
            .iter()
            .map(|index| hashes[*index])
            .collect::<Vec<_>>();
        let policy = MaterialRequestPolicy {
            concurrency: budget.max_in_flight_requests,
            priority: Priority::Normal,
        };
        session.set_phase(NetworkPhase::FetchingReceipts);
        let positive_receipts = if positive_headers.is_empty() {
            Vec::new()
        } else {
            self.fetch_direct_receipts_batched(
                session,
                &positive_headers,
                &positive_hashes,
                policy,
                cancellation,
            )
            .await?
        };
        let exact_match_positions = positive_receipts
            .iter()
            .enumerate()
            .filter_map(|(position, receipts)| {
                receipts
                    .iter()
                    .flat_map(|receipt| &receipt.logs)
                    .any(|log| source_log_matches(Some(scope), log))
                    .then_some(position)
            })
            .collect::<Vec<_>>();
        let matching_bodies = self
            .fetch_sparse_log_bodies(
                session,
                &positive_headers,
                &positive_hashes,
                &positive_receipts,
                &exact_match_positions,
                request,
                policy,
                cancellation,
            )
            .await?;
        let frames = normalize_sparse_log_frames(
            headers,
            request,
            budget,
            &bloom_positive_indices,
            &positive_receipts,
            &exact_match_positions,
            &matching_bodies,
        )?;
        self.source.request_metrics.record_sparse_log_window(
            headers.len(),
            bloom_positive_indices.len(),
            exact_match_positions.len(),
            matching_bodies.len(),
        );
        Ok(frames)
    }

    #[allow(clippy::too_many_arguments)]
    async fn fetch_sparse_log_bodies(
        &self,
        session: &P2pSession,
        positive_headers: &[Header],
        positive_hashes: &[B256],
        positive_receipts: &[Vec<Receipt>],
        exact_match_positions: &[usize],
        request: &DataRequest,
        policy: MaterialRequestPolicy,
        cancellation: &CancellationToken,
    ) -> Result<Vec<BlockBody>, P2pError> {
        if exact_match_positions.is_empty()
            || !request
                .log_fields
                .contains(leani_primitives::LogField::TransactionHash)
        {
            return Ok(Vec::new());
        }
        let matching_headers = exact_match_positions
            .iter()
            .map(|position| positive_headers[*position].clone())
            .collect::<Vec<_>>();
        let matching_hashes = exact_match_positions
            .iter()
            .map(|position| positive_hashes[*position])
            .collect::<Vec<_>>();
        session.set_phase(NetworkPhase::FetchingBodies);
        let (peers, bodies) = self
            .source
            .fetch_bodies_batched(
                &session.fetch,
                &matching_headers,
                &matching_hashes,
                policy,
                cancellation,
            )
            .await?;
        for peer in peers {
            reward_verified_material_peer(&session.handle, peer).await;
        }
        for (body, positive_position) in bodies.iter().zip(exact_match_positions) {
            validate_body_receipts(
                &positive_headers[*positive_position],
                body,
                &positive_receipts[*positive_position],
            )?;
        }
        Ok(bodies)
    }

    #[allow(clippy::too_many_lines)]
    async fn fetch_direct_receipts(
        &self,
        session: &P2pSession,
        headers: &[Header],
        hashes: &[B256],
        policy: MaterialRequestPolicy,
        cancellation: &CancellationToken,
    ) -> Result<(B512, Vec<Vec<Receipt>>), P2pError> {
        let attempts = if headers.len() > 1 {
            1
        } else {
            self.source.config.retries
        };
        let mut last_error = None;
        for attempt in 0..attempts {
            let queued_at = Instant::now();
            let connected_peers = session.fetch.num_connected_peers();
            let request_limit = effective_material_concurrency(
                connected_peers,
                self.source.config.material_request_concurrency,
                policy.concurrency,
            );
            let per_peer_limit = direct_peer_request_limit(request_limit, connected_peers);
            // Capacity wait is scheduler queueing, not an on-wire request.
            // A busy healthy peer can legitimately take longer than the ETH
            // response timeout to expose another multiplexing slot.
            let mut lease = self
                .source
                .network
                .direct_peers
                .acquire(
                    per_peer_limit,
                    self.source
                        .config
                        .request_timeout
                        .saturating_mul(4)
                        .min(self.source.config.peer_wait_timeout),
                    cancellation,
                )
                .await?;
            let peer_id = lease.peer.peer_id;
            let permit = self
                .source
                .network
                .request_gate
                .acquire(request_limit, policy.priority, cancellation)
                .await?;
            self.source
                .config
                .network_telemetry
                .request_started(queued_at.elapsed());
            let request_started_at = Instant::now();
            let response = request_direct_receipts(
                &lease.peer,
                hashes,
                self.source.config.request_timeout,
                cancellation,
            )
            .await;
            drop(permit);
            match response {
                Ok(response) => {
                    self.source.request_metrics.add_response_payload_bytes(
                        P2pRequestKind::Receipts,
                        response.response_payload_bytes,
                    );
                    match validate_receipts_against_headers(headers, &response.receipts) {
                        Ok(()) => {
                            lease.succeeded();
                            self.source.network.peer_quality.record_success(
                                peer_id,
                                PeerMaterialKind::Receipts,
                                headers.last().map_or(0, |header| header.number),
                                request_started_at.elapsed(),
                            );
                            reward_verified_material_peer(&session.handle, peer_id).await;
                            self.source.config.network_telemetry.request_succeeded();
                            self.source.request_metrics.record_batch(
                                P2pRequestKind::Receipts,
                                response.physical_requests,
                                response.requested_block_hashes,
                                response.receipts.len(),
                                request_started_at.elapsed(),
                                P2pRequestOutcome::Succeeded,
                            );
                            return Ok((peer_id, response.receipts));
                        }
                        Err(error) => {
                            lease.failed();
                            self.source.config.network_telemetry.request_failed();
                            self.source.request_metrics.record_batch(
                                P2pRequestKind::Receipts,
                                response.physical_requests.max(1),
                                response.requested_block_hashes,
                                response.receipts.len(),
                                request_started_at.elapsed(),
                                P2pRequestOutcome::Failed,
                            );
                            match error {
                                P2pError::InvalidResponse(_) => session.handle.ban_peer(peer_id),
                                P2pError::IncompleteResponse { .. } => {
                                    session.handle.disconnect_peer(peer_id);
                                }
                                _ => {}
                            }
                            self.source.network.direct_peers.remove(peer_id);
                            last_error = Some(error);
                        }
                    }
                }
                Err(P2pError::Cancelled) => return Err(P2pError::Cancelled),
                Err(error) => {
                    lease.failed();
                    record_request_error(&self.source.config.network_telemetry, &error);
                    self.source.request_metrics.record(
                        P2pRequestKind::Receipts,
                        hashes.len(),
                        0,
                        request_started_at.elapsed(),
                        request_outcome(&error),
                    );
                    if matches!(error, P2pError::Timeout { .. }) {
                        session.handle.disconnect_peer(peer_id);
                        self.source.network.direct_peers.remove(peer_id);
                    }
                    last_error = Some(error);
                }
            }
            if attempt.saturating_add(1) < attempts {
                retry_pause(self.source.config.retry_backoff, cancellation).await?;
            }
        }
        Err(last_error.unwrap_or_else(|| P2pError::Request {
            component: "receipts",
            detail: "retry budget exhausted".to_owned(),
        }))
    }

    async fn fetch_direct_receipts_batched(
        &self,
        session: &P2pSession,
        headers: &[Header],
        hashes: &[B256],
        policy: MaterialRequestPolicy,
        cancellation: &CancellationToken,
    ) -> Result<Vec<Vec<Receipt>>, P2pError> {
        if headers.len() != hashes.len() {
            return Err(P2pError::InvalidResponse(
                "receipt batch material length mismatch".to_owned(),
            ));
        }
        let batch_blocks = self.source.material_tuning.receipt_blocks();
        let requests = headers
            .chunks(batch_blocks)
            .zip(hashes.chunks(batch_blocks))
            .map(|(header_chunk, hash_chunk)| (header_chunk.to_vec(), hash_chunk.to_vec()))
            .collect::<Vec<_>>();
        let concurrency = effective_material_concurrency(
            session.fetch.num_connected_peers(),
            self.source.config.material_request_concurrency,
            policy.concurrency,
        );
        let outcomes = stream::iter(requests)
            .map(|(header_chunk, hash_chunk)| async move {
                let result = self
                    .fetch_direct_receipts(
                        session,
                        &header_chunk,
                        &hash_chunk,
                        policy,
                        cancellation,
                    )
                    .await;
                (header_chunk, hash_chunk, result)
            })
            .buffered(concurrency)
            .collect::<Vec<_>>()
            .await;

        let mut completed = Vec::new();
        let mut fallback = Vec::new();
        for (header_chunk, hash_chunk, result) in outcomes {
            match result {
                Ok((peer, receipts)) => completed.push((header_chunk[0].number, peer, receipts)),
                Err(error) if header_chunk.len() > 1 => {
                    debug!(
                        blocks = header_chunk.len(),
                        %error,
                        "splitting an unsuccessful direct-peer receipt request into single blocks"
                    );
                    fallback.extend(header_chunk.into_iter().zip(hash_chunk));
                }
                Err(error) => {
                    self.source.material_tuning.receipts_failed();
                    return Err(error);
                }
            }
        }
        let recovered = stream::iter(fallback)
            .map(|(header, hash)| async move {
                let result = self
                    .fetch_direct_receipts(
                        session,
                        std::slice::from_ref(&header),
                        std::slice::from_ref(&hash),
                        policy,
                        cancellation,
                    )
                    .await;
                (header.number, result)
            })
            .buffered(concurrency)
            .collect::<Vec<_>>()
            .await;
        let recovered_any = !recovered.is_empty();
        for (number, result) in recovered {
            match result {
                Ok((peer, receipts)) => completed.push((number, peer, receipts)),
                Err(error) => {
                    self.source.material_tuning.receipts_failed();
                    return Err(error);
                }
            }
        }
        if recovered_any {
            self.source.material_tuning.receipts_failed();
        } else {
            self.source.material_tuning.receipts_succeeded();
        }
        completed.sort_by_key(|(number, _, _)| *number);
        let mut receipts = Vec::with_capacity(headers.len());
        for (_, _, mut batch) in completed {
            receipts.append(&mut batch);
        }
        validate_receipts_against_headers(headers, &receipts)?;
        Ok(receipts)
    }

    #[allow(clippy::too_many_lines)]
    async fn fetch_history_material(
        &self,
        session: &mut P2pSession,
        headers: &[Header],
        hashes: &[B256],
        request_concurrency: usize,
        cancellation: &CancellationToken,
    ) -> Result<(Vec<BlockBody>, Vec<Vec<Receipt>>), P2pError> {
        let range = headers
            .first()
            .zip(headers.last())
            .and_then(|(first, last)| {
                BlockRange::new(BlockNumber(first.number), BlockNumber(last.number)).ok()
            });
        let mut attempts = 0_usize;
        let mut retained_bodies = None;
        let mut retained_receipts = None;
        loop {
            attempts = attempts.saturating_add(1);
            session.set_range(range);
            session.record_attempt();
            session.set_phase(if retained_bodies.is_none() {
                NetworkPhase::FetchingBodies
            } else {
                NetworkPhase::FetchingReceipts
            });
            let policy = MaterialRequestPolicy {
                concurrency: request_concurrency,
                priority: Priority::Normal,
            };
            let bodies_future = async {
                if retained_bodies.is_some() {
                    Ok(None)
                } else {
                    self.source
                        .fetch_bodies_batched(&session.fetch, headers, hashes, policy, cancellation)
                        .await
                        .map(|(peers, bodies)| Some((peers, bodies)))
                }
            };
            let receipts_future = async {
                if retained_receipts.is_some() {
                    Ok(None)
                } else {
                    self.fetch_direct_receipts_batched(
                        session,
                        headers,
                        hashes,
                        policy,
                        cancellation,
                    )
                    .await
                    .map(Some)
                }
            };
            let (bodies_result, receipts_result) = tokio::join!(bodies_future, receipts_future);
            let mut result = Ok(());
            match bodies_result {
                Ok(Some((peers, bodies))) => {
                    for peer in peers {
                        reward_verified_material_peer(&session.handle, peer).await;
                    }
                    retained_bodies = Some(bodies);
                }
                Ok(None) => {}
                Err(error) => result = Err(error),
            }
            match receipts_result {
                Ok(Some(receipts)) => retained_receipts = Some(receipts),
                Err(error) if result.is_ok() => result = Err(error),
                Ok(None) | Err(_) => {}
            }
            match result {
                Ok(()) => {
                    let bodies = retained_bodies
                        .as_ref()
                        .expect("successful material fetch retains bodies");
                    let receipts = retained_receipts
                        .as_ref()
                        .expect("successful material fetch retains receipts");
                    validate_receipts(headers, bodies, receipts)?;
                    session.clear_error();
                    return Ok((
                        retained_bodies
                            .take()
                            .expect("successful material fetch retains bodies"),
                        retained_receipts
                            .take()
                            .expect("successful material fetch retains receipts"),
                    ));
                }
                Err(P2pError::Cancelled) => return Err(P2pError::Cancelled),
                Err(error) => {
                    session.record_error(&error);
                    if should_retry_session_error(&self.source.config, attempts, &error) {
                        let delay = session_retry_delay(&self.source.config, attempts);
                        debug!(
                            attempt = attempts,
                            ?delay,
                            %error,
                            retained_bodies = retained_bodies.is_some(),
                            retained_receipts = retained_receipts.is_some(),
                            "retrying history material on the persistent peer pool"
                        );
                        retry_pause(delay, cancellation).await?;
                    } else {
                        return Err(error);
                    }
                }
            }
        }
    }

    async fn connect_and_fetch_anchored_headers(
        &self,
        proof: BlockRange,
        retained: BlockRange,
        request_concurrency: usize,
        cancellation: &CancellationToken,
    ) -> Result<(P2pSession, AnchoredHeaderProof), P2pError> {
        let mut attempts = 0_usize;
        let mut builder = AnchoredHeaderProofBuilder::new(
            proof,
            retained,
            self.source.config.history_header_request_blocks,
        );
        loop {
            attempts = attempts.saturating_add(1);
            let session = match self.acquire_history_network(cancellation).await {
                Ok(session) => session,
                Err(P2pError::Cancelled) => return Err(P2pError::Cancelled),
                Err(error) => {
                    if should_retry_session_error(&self.source.config, attempts, &error) {
                        let delay = session_retry_delay(&self.source.config, attempts);
                        debug!(
                            attempt = attempts,
                            ?delay,
                            %error,
                            "retrying anchored history P2P connection"
                        );
                        retry_pause(delay, cancellation).await?;
                        continue;
                    }
                    return Err(error);
                }
            };
            session.set_range(Some(proof));
            session.set_phase(NetworkPhase::FetchingHeaders);
            match self
                .fetch_anchored_headers(
                    &session.fetch,
                    &mut builder,
                    request_concurrency,
                    cancellation,
                )
                .await
            {
                Ok(headers) => {
                    session.clear_error();
                    return Ok((session, headers));
                }
                Err(P2pError::Cancelled) => {
                    shutdown_session(&session);
                    return Err(P2pError::Cancelled);
                }
                Err(error) => {
                    session.record_attempt();
                    session.record_error(&error);
                    shutdown_session(&session);
                    if should_retry_session_error(&self.source.config, attempts, &error) {
                        let delay = session_retry_delay(&self.source.config, attempts);
                        debug!(
                            attempt = attempts,
                            ?delay,
                            %error,
                            "retrying anchored history headers on the persistent peer pool"
                        );
                        retry_pause(delay, cancellation).await?;
                    } else {
                        return Err(error);
                    }
                }
            }
        }
    }

    async fn connect_history_session(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<P2pSession, P2pError> {
        let mut attempts = 0_usize;
        loop {
            attempts = attempts.saturating_add(1);
            match self.acquire_history_network(cancellation).await {
                Ok(session) => return Ok(session),
                Err(P2pError::Cancelled) => return Err(P2pError::Cancelled),
                Err(error) if should_retry_session_error(&self.source.config, attempts, &error) => {
                    let delay = session_retry_delay(&self.source.config, attempts);
                    debug!(
                        attempt = attempts,
                        ?delay,
                        %error,
                        "retrying cached anchored history P2P connection"
                    );
                    retry_pause(delay, cancellation).await?;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn fetch_anchored_headers<C>(
        &self,
        fetch: &C,
        builder: &mut AnchoredHeaderProofBuilder,
        request_concurrency: usize,
        cancellation: &CancellationToken,
    ) -> Result<AnchoredHeaderProof, P2pError>
    where
        C: HeadersClient<Header = Header> + DownloadClient,
    {
        while !builder.pending.is_empty() {
            let concurrency = effective_material_concurrency(
                fetch.num_connected_peers(),
                self.source.config.history_header_request_concurrency,
                request_concurrency,
            );
            let wave = builder.take_wave(concurrency);
            let outcomes = stream::iter(wave)
                .map(|range| async move {
                    let result = self
                        .source
                        .fetch_headers(
                            fetch,
                            range,
                            None,
                            true,
                            MaterialRequestPolicy {
                                concurrency: request_concurrency,
                                priority: Priority::Normal,
                            },
                            cancellation,
                        )
                        .await
                        .and_then(|(_, headers)| header_proof_segment(range, &headers));
                    (range, result)
                })
                .buffer_unordered(concurrency)
                .collect::<Vec<_>>()
                .await;
            let mut last_error = None;
            for (range, result) in outcomes {
                if let Err(error) = &result {
                    if matches!(error, P2pError::Cancelled) {
                        return Err(P2pError::Cancelled);
                    }
                    last_error = Some(error.to_string());
                }
                builder.record(range, result)?;
            }
            if let Some(error) = last_error {
                return Err(P2pError::Request {
                    component: "anchored headers",
                    detail: error,
                });
            }
        }
        builder.finish(self.anchor.block.hash)
    }
}

#[async_trait]
impl HistorySource for RethP2pHistorySource {
    fn descriptor(&self) -> &SourceDescriptor {
        &self.descriptor
    }

    fn acquisition_metrics(&self) -> Option<SourceAcquisitionMetrics> {
        Some(
            self.acquisition_metrics
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
        )
    }

    fn acquisition_identity(&self) -> Vec<u8> {
        let mut identity = Vec::with_capacity(128);
        identity.extend_from_slice(self.descriptor.id.as_str().as_bytes());
        identity.extend_from_slice(&self.anchor.block.number.0.to_be_bytes());
        identity.extend_from_slice(&self.anchor.block.hash.0);
        identity.extend_from_slice(&self.anchor.consensus.beacon_slot.to_be_bytes());
        identity.extend_from_slice(&self.anchor.consensus.beacon_block_root);
        identity
    }

    fn coalescing_partition_identity(&self, _chunk: &SourceChunk) -> Vec<u8> {
        Vec::new()
    }

    fn slice_chunk(
        &self,
        chunk: &SourceChunk,
        range: BlockRange,
    ) -> Result<SourceChunk, SourceError> {
        if range.start() < chunk.range.start() || range.end() > chunk.range.end() {
            return Err(SourceError::InvalidPlan(
                "coalesced P2P subrange exceeds its planned chunk".to_owned(),
            ));
        }
        let mut partition = decode_history_partition(&chunk.partition)?;
        partition.range = range;
        partition.request.range = range;
        let mut sliced = chunk.clone();
        sliced.range = range;
        sliced.partition = encode_history_partition(&partition)?;
        sliced.expected_parent = None;
        sliced.estimated_bytes = None;
        Ok(sliced)
    }

    async fn plan(&self, request: &DataRequest) -> Result<SourcePlan, SourceError> {
        if request.chain_id != self.descriptor.chain_id {
            return Err(SourceError::InvalidPlan(
                "P2P history bridge currently supports Ethereum mainnet only".to_owned(),
            ));
        }
        let available = self
            .descriptor
            .range
            .expect("history bridge always has a bounded range");
        if request.range.start().0 < available.start().0
            || request.range.end().0 > available.end().0
        {
            return Err(SourceError::MissingRange(request.range));
        }
        if !self
            .descriptor
            .complete_capabilities
            .with_derivable()
            .contains_all(request.required)
        {
            return Err(SourceError::InvalidPlan(
                "P2P history bridge lacks requested capabilities".to_owned(),
            ));
        }
        let mut chunks = Vec::new();
        let mut start = request.range.start().0;
        while start <= request.range.end().0 {
            let end = start
                .saturating_add(MAX_HISTORY_OPEN_BLOCKS.saturating_sub(1))
                .min(request.range.end().0);
            let range = BlockRange::new(BlockNumber(start), BlockNumber(end))
                .map_err(|error| SourceError::InvalidPlan(error.to_string()))?;
            chunks.push(SourceChunk {
                source_id: self.descriptor.id.clone(),
                range,
                partition: encode_history_partition(&HistoryPartition {
                    proof_start: request.range.start(),
                    material_end: request.range.end(),
                    range,
                    request: DataRequest {
                        range,
                        ..request.clone()
                    },
                })?,
                schema_version: self.descriptor.schema_version.clone(),
                expected_parent: None,
                estimated_bytes: None,
            });
            if end == u64::MAX {
                break;
            }
            start = end + 1;
        }
        Ok(SourcePlan {
            source_id: self.descriptor.id.clone(),
            request: request.clone(),
            chunks,
            estimated_bytes: None,
            estimated_lag: self.descriptor.expected_lag,
            supplied: self.descriptor.capabilities,
            complete: self.descriptor.complete_capabilities,
            trust: self.descriptor.trust,
            schema_version: self.descriptor.schema_version.clone(),
            physical_plan: Vec::new(),
        })
    }

    async fn open(
        &self,
        chunk: &SourceChunk,
        budget: SourceBudget,
        cancellation: CancellationToken,
    ) -> Result<BlockFrameStream, SourceError> {
        let budget = budget.validate()?;
        let partition = decode_history_partition(&chunk.partition)?;
        if chunk.source_id != self.descriptor.id
            || chunk.schema_version != self.descriptor.schema_version
            || partition.range != chunk.range
            || partition.proof_start > chunk.range.start()
            || partition.material_end < chunk.range.end()
            || partition.material_end > self.anchor.block.number
        {
            return Err(SourceError::InvalidPlan(
                "P2P history chunk identity does not match this source".to_owned(),
            ));
        }
        if chunk.range.len() > budget.max_frames {
            return Err(SourceError::BudgetExceeded {
                resource: "frames",
                limit: budget.max_frames,
                observed: chunk.range.len(),
            });
        }
        {
            let mut metrics = self
                .acquisition_metrics
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            metrics.opened_chunks = metrics.opened_chunks.saturating_add(1);
            metrics.opened_ranges.push(chunk.range);
        }
        let (sender, receiver) = tokio::sync::mpsc::channel(budget.max_buffered_frames);
        let source = self.clone();
        let range = chunk.range;
        tokio::spawn(async move {
            let started = Instant::now();
            let outcome = source
                .run_bridge(
                    partition.proof_start,
                    partition.material_end,
                    range,
                    partition.request,
                    budget,
                    cancellation,
                    sender.clone(),
                )
                .await;
            {
                let mut metrics = source
                    .acquisition_metrics
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                metrics.operation_elapsed_ms = metrics.operation_elapsed_ms.saturating_add(
                    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                );
            }
            if let Err(error) = outcome {
                let _ = sender.send(Err(SourceError::from(error))).await;
            }
        });
        Ok(stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|item| (item, receiver))
        })
        .boxed())
    }
}

fn mark_anchored_history_finalized(frame: &mut BlockFrame, anchor: &ConsensusAnchor) {
    frame.finality = Finality::Finalized;
    frame.verification.parent_continuity.detail = Some(format!(
        "verified contiguous ancestry to finalized execution anchor {}",
        anchor.execution_block_hash
    ));
    frame.verification.consensus_anchor =
        (frame.block.hash == anchor.execution_block_hash).then(|| anchor.clone());
}

fn anchored_header_ranges(proof: BlockRange, request_blocks: u64) -> Vec<BlockRange> {
    let request_blocks = request_blocks.clamp(1, MAX_HISTORY_HEADER_REQUEST_BLOCKS);
    let mut ranges = Vec::new();
    let mut start = proof.start().0;
    while start <= proof.end().0 {
        let end = start
            .saturating_add(request_blocks.saturating_sub(1))
            .min(proof.end().0);
        ranges.push(
            BlockRange::new(BlockNumber(start), BlockNumber(end))
                .expect("bounded anchored header range"),
        );
        if end == u64::MAX {
            break;
        }
        start = end + 1;
    }
    ranges
}

fn header_proof_segment(
    range: BlockRange,
    headers: &[Header],
) -> Result<HeaderProofSegment, P2pError> {
    validate_headers(range, headers, None)?;
    let first = headers.first().ok_or_else(|| {
        P2pError::InvalidResponse("anchored header request returned no headers".to_owned())
    })?;
    Ok(HeaderProofSegment {
        range,
        first_parent: block_hash(first.parent_hash),
        hashes: headers
            .iter()
            .map(|header| block_hash(header.hash_slow()))
            .collect(),
    })
}

#[cfg(test)]
fn assemble_anchored_header_proof(
    proof: BlockRange,
    anchor: BlockHash,
    segments: Vec<HeaderProofSegment>,
) -> Result<AnchoredHeaderProof, P2pError> {
    let mut builder = AnchoredHeaderProofBuilder::new(proof, proof, proof.len());
    builder.pending.clear();
    for segment in segments {
        let range = segment.range;
        builder.record(range, Ok(segment))?;
    }
    builder.finish(anchor)
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct HistoryPartition {
    proof_start: BlockNumber,
    material_end: BlockNumber,
    range: BlockRange,
    request: DataRequest,
}

fn encode_history_partition(partition: &HistoryPartition) -> Result<Vec<u8>, SourceError> {
    postcard::to_allocvec(partition).map_err(|error| SourceError::InvalidPlan(error.to_string()))
}

fn decode_history_partition(bytes: &[u8]) -> Result<HistoryPartition, SourceError> {
    postcard::from_bytes(bytes).map_err(|error| SourceError::InvalidPlan(error.to_string()))
}

fn validate_retained_canonical(
    canonical: &[BlockRef],
    max_reorg_depth: usize,
) -> Result<(), SourceError> {
    if canonical.is_empty() || canonical.len() > max_reorg_depth.saturating_add(1) {
        return Err(SourceError::InvalidPlan(format!(
            "retained live suffix must contain 1..={} blocks",
            max_reorg_depth.saturating_add(1)
        )));
    }
    for pair in canonical.windows(2) {
        let [parent, child] = pair else {
            continue;
        };
        if child.number.0 != parent.number.0.saturating_add(1) || child.parent_hash != parent.hash {
            return Err(SourceError::InvalidPlan(
                "retained live suffix is not a contiguous parent-linked chain".to_owned(),
            ));
        }
    }
    Ok(())
}

#[async_trait]
impl LiveSource for RethP2pSource {
    fn descriptor(&self) -> &SourceDescriptor {
        &self.descriptor
    }

    #[allow(clippy::too_many_lines)]
    async fn subscribe(
        &self,
        request: DataRequest,
        start: LiveStart,
        budget: SourceBudget,
        cancellation: CancellationToken,
    ) -> Result<ChainEventStream, SourceError> {
        let budget = budget.validate()?;
        if request.chain_id != self.descriptor.chain_id {
            return Err(SourceError::InvalidPlan(
                "live request belongs to another chain".to_owned(),
            ));
        }
        if !self
            .descriptor
            .complete_capabilities
            .with_derivable()
            .contains_all(request.required)
        {
            return Err(SourceError::InvalidPlan(
                "execution P2P live source lacks requested capabilities".to_owned(),
            ));
        }
        if !self.descriptor.finality.supports(request.minimum_finality) {
            return Err(SourceError::InvalidPlan(
                "execution P2P live source lacks requested finality".to_owned(),
            ));
        }
        let (anchor, overlap_blocks, retained) = match start {
            LiveStart::Block(block) => (block, 0, None),
            LiveStart::AnchoredOverlap {
                anchor,
                overlap_blocks,
            } => {
                if overlap_blocks == 0 || overlap_blocks > MAX_FIXED_RANGE_BLOCKS {
                    return Err(SourceError::InvalidPlan(format!(
                        "anchored live overlap must be in 1..={MAX_FIXED_RANGE_BLOCKS}"
                    )));
                }
                (anchor, overlap_blocks, None)
            }
            LiveStart::RetainedCanonical { canonical } => {
                validate_retained_canonical(&canonical, self.config.max_reorg_depth)?;
                let tip = *canonical
                    .last()
                    .expect("validated retained canonical suffix is non-empty");
                (tip, 0, Some(VecDeque::from(canonical)))
            }
            LiveStart::Cursor(cursor) => {
                if cursor.chain_id != self.descriptor.chain_id
                    || cursor.source_id != self.descriptor.id
                {
                    return Err(SourceError::InvalidPlan(
                        "live cursor belongs to another source or chain".to_owned(),
                    ));
                }
                (
                    BlockRef {
                        number: cursor.block_number,
                        hash: cursor.block_hash,
                        parent_hash: BlockHash::ZERO,
                        timestamp: SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs(),
                    },
                    0,
                    None,
                )
            }
            LiveStart::Head => {
                return Err(SourceError::InvalidPlan(
                    "Reth P2P live ingestion requires an explicit consensus-anchored start block"
                        .to_owned(),
                ));
            }
        };
        let overlap_range = if overlap_blocks == 0 {
            None
        } else {
            let from = anchor
                .number
                .0
                .saturating_sub(overlap_blocks.saturating_sub(1));
            let range = BlockRange::new(BlockNumber(from), anchor.number)
                .map_err(|error| SourceError::InvalidPlan(error.to_string()))?;
            if range.len() > budget.max_frames {
                return Err(SourceError::BudgetExceeded {
                    resource: "frames",
                    limit: budget.max_frames,
                    observed: range.len(),
                });
            }
            Some(range)
        };
        let required_peer_head = if retained.is_some() || overlap_blocks != 0 {
            anchor.number
        } else {
            BlockNumber(anchor.number.0.saturating_add(1))
        };
        let (session, last, queued, recent) = if let Some(recent) = retained {
            let (session, _) = self
                .connect(anchor, NetworkLane::Live, &cancellation)
                .await?;
            self.wait_for_peer_head(&session, required_peer_head, &cancellation)
                .await?;
            (session, anchor, VecDeque::new(), recent)
        } else if overlap_blocks == 0 {
            let (session, _) = self
                .connect(anchor, NetworkLane::Live, &cancellation)
                .await?;
            self.wait_for_peer_head(&session, required_peer_head, &cancellation)
                .await?;
            (session, anchor, VecDeque::new(), VecDeque::from([anchor]))
        } else {
            let range = overlap_range.expect("non-zero overlap has a range");
            let fetched = self
                .connect_and_fetch_requested_live_range(
                    anchor,
                    range,
                    Some(anchor.hash),
                    &request,
                    budget,
                    &cancellation,
                )
                .await;
            let (session, frames, _) = match fetched {
                Ok(result) => result,
                Err(error) => return Err(error.into()),
            };
            let Some(first) = frames.first() else {
                shutdown_session(&session);
                return Err(SourceError::Protocol(
                    "anchored overlap returned no frames".to_owned(),
                ));
            };
            let fetched_anchor = frames.last().expect("non-empty overlap").block;
            if fetched_anchor.number != anchor.number || fetched_anchor.hash != anchor.hash {
                shutdown_session(&session);
                return Err(SourceError::Protocol(
                    "anchored overlap did not end at the verified execution anchor".to_owned(),
                ));
            }
            let predecessor = BlockRef {
                number: BlockNumber(first.block.number.0.saturating_sub(1)),
                hash: first.block.parent_hash,
                parent_hash: BlockHash::ZERO,
                timestamp: first.block.timestamp.saturating_sub(1),
            };
            (
                session,
                predecessor,
                VecDeque::from(frames),
                VecDeque::from([predecessor]),
            )
        };
        session.set_range(None);
        session.set_phase(NetworkPhase::FollowingHead);
        session.clear_error();
        let state = P2pLiveState {
            source: self.clone(),
            session,
            request,
            budget,
            cancellation,
            last,
            queued,
            recent,
            required_peer_head,
            pending_material_attempts: 0,
            head_unavailable_since: None,
            reconnect_error: None,
            disconnect_reported: false,
            terminal: false,
        };
        Ok(stream::unfold(state, next_live_event).boxed())
    }
}

#[allow(clippy::too_many_lines)]
async fn next_live_event(
    mut state: P2pLiveState,
) -> Option<(Result<ChainEvent, SourceError>, P2pLiveState)> {
    loop {
        if state.terminal || state.cancellation.is_cancelled() {
            shutdown_session(&state.session);
            return None;
        }
        if let Some(request_error) = state.reconnect_error.take() {
            match reconnect_live_session(&mut state, &request_error).await {
                Ok(()) => continue,
                Err(P2pError::Cancelled) => return None,
                Err(reconnect) => {
                    state.terminal = true;
                    return Some((Err(reconnect.into()), state));
                }
            }
        }
        if let Some(frame) = state.queued.pop_front() {
            state.last = frame.block;
            state.recent.push_back(frame.block);
            while state.recent.len() > state.source.config.max_reorg_depth.saturating_add(1) {
                state.recent.pop_front();
            }
            return Some((Ok(ChainEvent::Block(Box::new(frame))), state));
        }
        let (head_number, head_hash) = match state
            .source
            .discover_peer_head(
                &state.session,
                state.last.number.max(state.required_peer_head),
                &state.cancellation,
            )
            .await
        {
            Ok(head) => {
                state.head_unavailable_since = None;
                state.disconnect_reported = false;
                head
            }
            Err(P2pError::Cancelled) => {
                shutdown_session(&state.session);
                return None;
            }
            Err(error) => {
                let grace = state
                    .source
                    .descriptor
                    .expected_lag
                    .max(state.source.config.poll_interval);
                let unavailable_for =
                    head_unavailable_for(&mut state.head_unavailable_since, Instant::now());
                debug!(
                    required_head = state.last.number.max(state.required_peer_head).0,
                    ?unavailable_for,
                    ?grace,
                    %error,
                    "live head is temporarily unavailable; retaining the verified tip and retrying peers"
                );
                if retry_pause(state.source.config.poll_interval, &state.cancellation)
                    .await
                    .is_err()
                {
                    shutdown_session(&state.session);
                    return None;
                }
                if head_unavailable_for(&mut state.head_unavailable_since, Instant::now()) < grace {
                    continue;
                }
                if state.disconnect_reported {
                    continue;
                }
                state.disconnect_reported = true;
                return Some((
                    Ok(ChainEvent::Disconnected {
                        reason: error.to_string(),
                    }),
                    state,
                ));
            }
        };
        let next = state.last.number.0.saturating_add(1);
        if head_number.0 < next {
            match state
                .source
                .poll_next_verified_frame(
                    &state.session,
                    BlockNumber(next),
                    &state.request,
                    state.budget,
                    &state.cancellation,
                )
                .await
            {
                Ok(Some(frame)) => {
                    state.pending_material_attempts = 0;
                    if frame.block.parent_hash != state.last.hash {
                        let block = frame.block;
                        let event =
                            reconstruct_reorg_event(&mut state, block.number, block.hash).await;
                        return Some((Ok(event), state));
                    }
                    state.queued.push_back(frame);
                    continue;
                }
                Ok(None) => {
                    state.pending_material_attempts = 0;
                    if retry_pause(state.source.config.poll_interval, &state.cancellation)
                        .await
                        .is_err()
                    {
                        shutdown_session(&state.session);
                        return None;
                    }
                }
                Err(P2pError::Cancelled) => {
                    shutdown_session(&state.session);
                    return None;
                }
                Err(error) => {
                    if let Some(delay) = pending_live_material_delay(
                        &state.source.config,
                        &mut state.pending_material_attempts,
                        &error,
                    ) {
                        debug!(
                            attempt = state.pending_material_attempts,
                            ?delay,
                            %error,
                            "latest execution material is not available yet; retrying on the active peer pool"
                        );
                        if retry_pause(delay, &state.cancellation).await.is_err() {
                            shutdown_session(&state.session);
                            return None;
                        }
                        continue;
                    }
                    let reason = error.to_string();
                    state.reconnect_error = Some(reason.clone());
                    return Some((Ok(ChainEvent::Disconnected { reason }), state));
                }
            }
            continue;
        }
        let end = if state.pending_material_attempts == 0 {
            head_number.0.min(
                next.saturating_add(
                    u64::try_from(state.source.config.material_request_blocks)
                        .expect("material request block bound fits u64")
                        .saturating_sub(1),
                ),
            )
        } else {
            next
        };
        let range = match BlockRange::new(BlockNumber(next), BlockNumber(end)) {
            Ok(range) => range,
            Err(error) => {
                state.terminal = true;
                return Some((Err(SourceError::Protocol(error.to_string())), state));
            }
        };
        let expected_tip = (end == head_number.0).then_some(head_hash);
        let frames = state
            .source
            .fetch_requested_live_range(
                &state.session,
                range,
                expected_tip,
                &state.request,
                state.budget,
                &state.cancellation,
            )
            .await;
        let (frames, _) = match frames {
            Ok(result) => {
                state.pending_material_attempts = 0;
                result
            }
            Err(P2pError::Cancelled) => {
                shutdown_session(&state.session);
                return None;
            }
            Err(error) => {
                if let Some(delay) = pending_live_material_delay(
                    &state.source.config,
                    &mut state.pending_material_attempts,
                    &error,
                ) {
                    debug!(
                        attempt = state.pending_material_attempts,
                        ?delay,
                        %error,
                        "latest execution material is not available yet; retrying on the active peer pool"
                    );
                    if retry_pause(delay, &state.cancellation).await.is_err() {
                        shutdown_session(&state.session);
                        return None;
                    }
                    continue;
                }
                let reason = error.to_string();
                state.reconnect_error = Some(reason.clone());
                return Some((Ok(ChainEvent::Disconnected { reason }), state));
            }
        };
        if frames
            .first()
            .is_none_or(|frame| frame.block.parent_hash != state.last.hash)
        {
            let event = reconstruct_reorg_event(&mut state, head_number, head_hash).await;
            return Some((Ok(event), state));
        }
        state.queued.extend(frames);
    }
}

fn head_unavailable_for(since: &mut Option<Instant>, now: Instant) -> Duration {
    now.saturating_duration_since(*since.get_or_insert(now))
}

fn pending_live_material_delay(
    config: &RethP2pConfig,
    attempts: &mut usize,
    error: &P2pError,
) -> Option<Duration> {
    if !matches!(
        error,
        P2pError::IncompleteResponse {
            component: "headers" | "bodies" | "receipts",
            ..
        }
    ) {
        return None;
    }
    *attempts = (*attempts).saturating_add(1);
    Some(session_retry_delay(config, *attempts).min(config.poll_interval))
}

async fn reconnect_live_session(
    state: &mut P2pLiveState,
    request_error: &str,
) -> Result<(), P2pError> {
    state.session.record_attempt();
    state.session.record_error(request_error);
    shutdown_session(&state.session);
    let mut attempts = 0_usize;
    loop {
        attempts = attempts.saturating_add(1);
        match state
            .source
            .connect(state.last, NetworkLane::Live, &state.cancellation)
            .await
        {
            Ok((session, _)) => {
                state.session = session;
                state.session.set_range(None);
                state.session.set_phase(NetworkPhase::FollowingHead);
                state.head_unavailable_since = None;
                state.disconnect_reported = false;
                return Ok(());
            }
            Err(P2pError::Cancelled) => return Err(P2pError::Cancelled),
            Err(error) => {
                if should_retry_session_error(&state.source.config, attempts, &error) {
                    retry_pause(
                        session_retry_delay(&state.source.config, attempts),
                        &state.cancellation,
                    )
                    .await?;
                } else {
                    return Err(P2pError::Network(format!(
                        "{request_error}; persistent peer-pool recovery failed: {error}"
                    )));
                }
            }
        }
    }
}

async fn reconstruct_reorg_event(
    state: &mut P2pLiveState,
    head_number: BlockNumber,
    head_hash: BlockHash,
) -> ChainEvent {
    let source = state.source.clone();
    match source
        .reconstruct_reorg(state, head_number, head_hash)
        .await
    {
        Ok((reverted, applied, new_tip)) => {
            for _ in 0..reverted.len() {
                state.recent.pop_back();
            }
            state.recent.extend(applied.iter().map(|frame| frame.block));
            state.last = new_tip;
            ChainEvent::Reorg { reverted, applied }
        }
        Err(error) => {
            state.terminal = true;
            ChainEvent::Reset {
                last_valid: state.recent.front().copied(),
                reason: error.to_string(),
            }
        }
    }
}

async fn retry_pause(duration: Duration, cancellation: &CancellationToken) -> Result<(), P2pError> {
    tokio::select! {
        () = cancellation.cancelled() => Err(P2pError::Cancelled),
        () = tokio::time::sleep(duration) => Ok(()),
    }
}

fn shutdown_session(_session: &P2pSession) {
    // Logical live/history/probe sessions share one process-wide network
    // manager. Dropping or retrying one lane must not disconnect healthy peers.
}

fn should_retry_session(config: &RethP2pConfig, attempts: usize) -> bool {
    config.persistent_retries || attempts < config.session_retries
}

fn should_retry_session_error(config: &RethP2pConfig, attempts: usize, error: &P2pError) -> bool {
    matches!(
        error,
        P2pError::Network(_)
            | P2pError::PeerTimeout { .. }
            | P2pError::Timeout { .. }
            | P2pError::Request { .. }
            | P2pError::InvalidResponse(_)
            | P2pError::IncompleteResponse { .. }
    ) && should_retry_session(config, attempts)
}

fn session_retry_delay(config: &RethP2pConfig, attempts: usize) -> Duration {
    let exponent = u32::try_from(attempts.saturating_sub(1).min(16)).unwrap_or(16);
    config
        .retry_backoff
        .saturating_mul(1_u32 << exponent)
        .min(config.retry_backoff_max)
}

async fn cancellable_timeout<F, T, E>(
    future: F,
    timeout: Duration,
    cancellation: &CancellationToken,
    component: &'static str,
) -> Result<T, P2pError>
where
    F: Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    tokio::select! {
        () = cancellation.cancelled() => Err(P2pError::Cancelled),
        result = tokio::time::timeout(timeout, future) => match result {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(error)) => Err(P2pError::Request {
                component,
                detail: error.to_string(),
            }),
            Err(_) => Err(P2pError::Timeout { component }),
        }
    }
}

async fn cancellable_peer_request<F, T>(
    future: F,
    timeout: Duration,
    cancellation: &CancellationToken,
    component: &'static str,
) -> Result<T, P2pError>
where
    F: Future<Output = Result<T, RequestError>>,
{
    tokio::select! {
        () = cancellation.cancelled() => Err(P2pError::Cancelled),
        result = tokio::time::timeout(timeout, future) => match result {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(RequestError::Timeout)) | Err(_) => Err(P2pError::Timeout { component }),
            Ok(Err(error)) => Err(P2pError::Request {
                component,
                detail: error.to_string(),
            }),
        }
    }
}

async fn await_direct_peer_response<T>(
    response: tokio::sync::oneshot::Receiver<Result<T, RequestError>>,
    timeout: Duration,
    cancellation: &CancellationToken,
    component: &'static str,
) -> Result<T, P2pError> {
    tokio::select! {
        () = cancellation.cancelled() => Err(P2pError::Cancelled),
        result = tokio::time::timeout(timeout, response) => match result {
            Ok(Ok(Ok(value))) => Ok(value),
            Ok(Ok(Err(RequestError::Timeout))) | Err(_) => {
                Err(P2pError::Timeout { component })
            }
            Ok(Ok(Err(error))) => Err(P2pError::Request {
                component,
                detail: error.to_string(),
            }),
            Ok(Err(error)) => Err(P2pError::Request {
                component,
                detail: error.to_string(),
            }),
        }
    }
}

async fn request_direct_bodies(
    peer: &DirectPeer,
    hashes: &[B256],
    timeout: Duration,
    cancellation: &CancellationToken,
) -> Result<(Vec<BlockBody>, usize), P2pError> {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    peer.messages
        .try_send(PeerRequest::GetBlockBodies {
            request: GetBlockBodies(hashes.to_vec()),
            response: sender,
        })
        .map_err(|error| P2pError::Request {
            component: "bodies",
            detail: format!("could not queue direct request: {error:?}"),
        })?;
    let response = await_direct_peer_response(receiver, timeout, cancellation, "bodies").await?;
    let response_payload_bytes = response.length();
    Ok((response.0, response_payload_bytes))
}

async fn request_direct_header(
    peer: &DirectPeer,
    hash: B256,
    timeout: Duration,
    cancellation: &CancellationToken,
) -> Result<Vec<Header>, P2pError> {
    request_direct_headers(
        peer,
        HeadersRequest::one(BlockHashOrNumber::Hash(hash)),
        timeout,
        cancellation,
        "peer head header",
    )
    .await
}

async fn request_direct_headers(
    peer: &DirectPeer,
    request: HeadersRequest,
    timeout: Duration,
    cancellation: &CancellationToken,
    component: &'static str,
) -> Result<Vec<Header>, P2pError> {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    peer.messages
        .try_send(PeerRequest::GetBlockHeaders {
            request: GetBlockHeaders {
                start_block: request.start,
                limit: request.limit,
                skip: 0,
                direction: request.direction,
            },
            response: sender,
        })
        .map_err(|error| P2pError::Request {
            component,
            detail: format!("could not queue direct request: {error:?}"),
        })?;
    let response = await_direct_peer_response(receiver, timeout, cancellation, component).await?;
    Ok(response.0)
}

#[derive(Debug)]
struct DirectReceiptsResponse {
    receipts: Vec<Vec<Receipt>>,
    physical_requests: u64,
    requested_block_hashes: usize,
    response_payload_bytes: usize,
}

async fn request_direct_receipts(
    peer: &DirectPeer,
    hashes: &[B256],
    timeout: Duration,
    cancellation: &CancellationToken,
) -> Result<DirectReceiptsResponse, P2pError> {
    if hashes.is_empty() {
        return Ok(DirectReceiptsResponse {
            receipts: Vec::new(),
            physical_requests: 0,
            requested_block_hashes: 0,
            response_payload_bytes: 0,
        });
    }
    match peer.eth_version {
        EthVersion::Eth70 => request_direct_receipts70(peer, hashes, timeout, cancellation).await,
        EthVersion::Eth69 => {
            let (sender, receiver) = tokio::sync::oneshot::channel();
            peer.messages
                .try_send(PeerRequest::GetReceipts69 {
                    request: GetReceipts(hashes.to_vec()),
                    response: sender,
                })
                .map_err(|error| P2pError::Request {
                    component: "receipts",
                    detail: format!("could not queue eth/69 request: {error:?}"),
                })?;
            let response =
                await_direct_peer_response(receiver, timeout, cancellation, "receipts").await?;
            let response_payload_bytes = response.length();
            Ok(DirectReceiptsResponse {
                receipts: response.0,
                physical_requests: 1,
                requested_block_hashes: hashes.len(),
                response_payload_bytes,
            })
        }
        _ => {
            let (sender, receiver) = tokio::sync::oneshot::channel();
            peer.messages
                .try_send(PeerRequest::GetReceipts {
                    request: GetReceipts(hashes.to_vec()),
                    response: sender,
                })
                .map_err(|error| P2pError::Request {
                    component: "receipts",
                    detail: format!("could not queue legacy request: {error:?}"),
                })?;
            let response =
                await_direct_peer_response(receiver, timeout, cancellation, "receipts").await?;
            let response_payload_bytes = response.length();
            Ok(DirectReceiptsResponse {
                receipts: response
                    .0
                    .into_iter()
                    .map(|receipts| {
                        receipts
                            .into_iter()
                            .map(|receipt| receipt.receipt)
                            .collect()
                    })
                    .collect(),
                physical_requests: 1,
                requested_block_hashes: hashes.len(),
                response_payload_bytes,
            })
        }
    }
}

async fn request_direct_receipts70(
    peer: &DirectPeer,
    hashes: &[B256],
    timeout: Duration,
    cancellation: &CancellationToken,
) -> Result<DirectReceiptsResponse, P2pError> {
    const MAX_CONTINUATION_ROUNDS: usize = 64;
    let mut blocks = Vec::<Vec<Receipt>>::new();
    let mut block_index = 0_usize;
    let mut receipt_index = 0_usize;
    let mut requested_block_hashes = 0_usize;
    let mut response_payload_bytes = 0_usize;
    let deadline = Instant::now() + timeout;
    for round in 0..MAX_CONTINUATION_ROUNDS {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(P2pError::Timeout {
                component: "receipts",
            });
        }
        requested_block_hashes =
            requested_block_hashes.saturating_add(hashes.len().saturating_sub(block_index));
        let (sender, receiver) = tokio::sync::oneshot::channel();
        peer.messages
            .try_send(PeerRequest::GetReceipts70 {
                request: GetReceipts70 {
                    first_block_receipt_index: u64::try_from(receipt_index).unwrap_or(u64::MAX),
                    block_hashes: hashes[block_index..].to_vec(),
                },
                response: sender,
            })
            .map_err(|error| P2pError::Request {
                component: "receipts",
                detail: format!("could not queue eth/70 request: {error:?}"),
            })?;
        let response =
            await_direct_peer_response(receiver, remaining, cancellation, "receipts").await?;
        response_payload_bytes = response_payload_bytes.saturating_add(response.length());
        if response.receipts.is_empty()
            || response.receipts.len() > hashes.len().saturating_sub(block_index)
        {
            return Err(P2pError::IncompleteResponse {
                component: "receipts",
                returned: response.receipts.len(),
                expected: hashes.len().saturating_sub(block_index),
            });
        }
        let before = blocks.iter().map(Vec::len).sum::<usize>();
        let response_blocks = response.receipts.len();
        for (offset, mut receipts) in response.receipts.into_iter().enumerate() {
            let index = block_index.saturating_add(offset);
            match index.cmp(&blocks.len()) {
                std::cmp::Ordering::Less => {
                    if offset != 0 || receipt_index == 0 {
                        return Err(P2pError::InvalidResponse(
                            "eth/70 receipt continuation overlapped a completed block".to_owned(),
                        ));
                    }
                    blocks[index].append(&mut receipts);
                }
                std::cmp::Ordering::Equal => blocks.push(receipts),
                std::cmp::Ordering::Greater => {
                    return Err(P2pError::InvalidResponse(
                        "eth/70 receipt continuation skipped a block".to_owned(),
                    ));
                }
            }
        }
        let after = blocks.iter().map(Vec::len).sum::<usize>();
        if after == before {
            return Err(P2pError::InvalidResponse(
                "eth/70 receipt continuation made no progress".to_owned(),
            ));
        }
        if !response.last_block_incomplete {
            return Ok(DirectReceiptsResponse {
                receipts: blocks,
                physical_requests: u64::try_from(round.saturating_add(1)).unwrap_or(u64::MAX),
                requested_block_hashes,
                response_payload_bytes,
            });
        }
        block_index = block_index.saturating_add(response_blocks.saturating_sub(1));
        receipt_index = blocks.get(block_index).map_or(0, Vec::len);
    }
    Err(P2pError::Request {
        component: "receipts",
        detail: "eth/70 continuation round limit exceeded".to_owned(),
    })
}

fn validate_headers(
    range: BlockRange,
    headers: &[Header],
    expected_tip: Option<BlockHash>,
) -> Result<(), P2pError> {
    let expected_len = usize::try_from(range.len()).expect("header range length fits usize");
    if headers.len() != expected_len {
        return Err(P2pError::IncompleteResponse {
            component: "headers",
            returned: headers.len(),
            expected: expected_len,
        });
    }
    for (offset, header) in headers.iter().enumerate() {
        let number = range
            .start()
            .0
            .saturating_add(u64::try_from(offset).expect("range offset fits u64"));
        if header.number != number {
            return Err(P2pError::InvalidResponse(format!(
                "header at offset {offset} has block {}, expected {number}",
                header.number
            )));
        }
        if let Some(previous) = offset.checked_sub(1).and_then(|index| headers.get(index))
            && header.parent_hash != previous.hash_slow()
        {
            return Err(P2pError::InvalidResponse(format!(
                "header parent continuity failed at block {number}"
            )));
        }
    }
    if let (Some(expected), Some(tip)) = (expected_tip, headers.last())
        && tip.hash_slow() != B256::from(*expected.as_array())
    {
        return Err(P2pError::InvalidResponse(format!(
            "tip hash {}, expected {expected}",
            tip.hash_slow()
        )));
    }
    Ok(())
}

#[cfg(test)]
fn classify_polled_header(
    expected: BlockNumber,
    mut headers: Vec<Header>,
) -> Result<Option<Header>, P2pError> {
    match headers.len() {
        0 => Ok(None),
        1 => {
            let header = headers.pop().expect("one header");
            if header.number != expected.0 {
                return Err(P2pError::InvalidResponse(format!(
                    "polled header number {}, expected {}",
                    header.number, expected.0
                )));
            }
            Ok(Some(header))
        }
        length => Err(P2pError::InvalidResponse(format!(
            "polled header response length {length}, expected at most 1"
        ))),
    }
}

fn validate_descending_headers(
    head_number: BlockNumber,
    head_hash: BlockHash,
    headers: &[Header],
) -> Result<(), P2pError> {
    let Some(first) = headers.first() else {
        return Err(P2pError::InvalidResponse(
            "empty descending header response".to_owned(),
        ));
    };
    if first.number != head_number.0 || first.hash_slow() != B256::from(*head_hash.as_array()) {
        return Err(P2pError::InvalidResponse(
            "descending header response does not start at the declared head".to_owned(),
        ));
    }
    for pair in headers.windows(2) {
        let newer = &pair[0];
        let older = &pair[1];
        if newer.number != older.number.saturating_add(1) || newer.parent_hash != older.hash_slow()
        {
            return Err(P2pError::InvalidResponse(format!(
                "descending header continuity failed below block {}",
                newer.number
            )));
        }
    }
    Ok(())
}

fn plan_reorg(
    recent: &VecDeque<BlockRef>,
    descending: &[Header],
) -> Result<(BlockRef, Vec<BlockRef>), P2pError> {
    let ancestor = descending.iter().find_map(|header| {
        let hash = block_hash(header.hash_slow());
        recent
            .iter()
            .rev()
            .find(|known| known.number.0 == header.number && known.hash == hash)
            .copied()
    });
    let Some(ancestor) = ancestor else {
        return Err(P2pError::ReorgTooDeep {
            maximum: recent.len().saturating_sub(1),
        });
    };
    let reverted = recent
        .iter()
        .rev()
        .take_while(|block| block.number > ancestor.number)
        .copied()
        .collect::<Vec<_>>();
    if reverted.is_empty() {
        return Err(P2pError::InvalidResponse(
            "replacement branch did not revert an emitted block".to_owned(),
        ));
    }
    Ok((ancestor, reverted))
}

fn history_header_range(
    headers: &[Header],
    component: &'static str,
) -> Result<BlockRange, P2pError> {
    let first = headers.first().ok_or_else(|| P2pError::Request {
        component,
        detail: "cannot request an empty header batch".to_owned(),
    })?;
    let last = headers
        .last()
        .expect("a first header implies a last header");
    BlockRange::new(BlockNumber(first.number), BlockNumber(last.number))
        .map_err(|error| P2pError::InvalidResponse(error.to_string()))
}

fn validate_bodies(headers: &[Header], bodies: &[BlockBody]) -> Result<(), P2pError> {
    if bodies.len() != headers.len() {
        return Err(P2pError::IncompleteResponse {
            component: "bodies",
            returned: bodies.len(),
            expected: headers.len(),
        });
    }
    for (header, body) in headers.iter().zip(bodies) {
        let transactions_root = calculate_transaction_root(&body.transactions);
        if transactions_root != header.transactions_root {
            return Err(P2pError::InvalidResponse(format!(
                "transaction root mismatch at block {}",
                header.number
            )));
        }
        if body.calculate_ommers_root() != header.ommers_hash {
            return Err(P2pError::InvalidResponse(format!(
                "ommers root mismatch at block {}",
                header.number
            )));
        }
        if body.calculate_withdrawals_root() != header.withdrawals_root {
            return Err(P2pError::InvalidResponse(format!(
                "withdrawals root mismatch at block {}",
                header.number
            )));
        }
    }
    Ok(())
}

fn validate_receipts(
    headers: &[Header],
    bodies: &[BlockBody],
    receipts: &[Vec<Receipt>],
) -> Result<(), P2pError> {
    validate_receipts_against_headers(headers, receipts)?;
    if bodies.len() != headers.len() {
        return Err(P2pError::IncompleteResponse {
            component: "bodies",
            returned: bodies.len(),
            expected: headers.len(),
        });
    }
    for ((header, body), block_receipts) in headers.iter().zip(bodies).zip(receipts) {
        validate_body_receipts(header, body, block_receipts)?;
    }
    Ok(())
}

fn validate_body_receipts(
    header: &Header,
    body: &BlockBody,
    receipts: &[Receipt],
) -> Result<(), P2pError> {
    if receipts.len() != body.transactions.len() {
        return Err(P2pError::InvalidResponse(format!(
            "receipt count {} differs from transaction count {} at block {}",
            receipts.len(),
            body.transactions.len(),
            header.number
        )));
    }
    for (transaction, receipt) in body.transactions.iter().zip(receipts) {
        if transaction.tx_type() != receipt.tx_type {
            return Err(P2pError::InvalidResponse(format!(
                "transaction/receipt type mismatch at block {}",
                header.number
            )));
        }
    }
    Ok(())
}

fn validate_receipts_against_headers(
    headers: &[Header],
    receipts: &[Vec<Receipt>],
) -> Result<(), P2pError> {
    if receipts.len() != headers.len() {
        return Err(P2pError::IncompleteResponse {
            component: "receipts",
            returned: receipts.len(),
            expected: headers.len(),
        });
    }
    for (header, block_receipts) in headers.iter().zip(receipts) {
        let with_bloom = block_receipts
            .iter()
            .map(alloy_consensus::TxReceipt::with_bloom_ref)
            .collect::<Vec<_>>();
        if calculate_receipt_root(&with_bloom) != header.receipts_root {
            return Err(P2pError::InvalidResponse(format!(
                "receipt root mismatch at block {}",
                header.number
            )));
        }
        let bloom = with_bloom
            .iter()
            .fold(alloy_primitives::Bloom::ZERO, |total, receipt| {
                total | receipt.bloom_ref()
            });
        if bloom != header.logs_bloom {
            return Err(P2pError::InvalidResponse(format!(
                "logs bloom mismatch at block {}",
                header.number
            )));
        }
        let mut previous_gas = 0_u64;
        for receipt in block_receipts {
            if receipt.cumulative_gas_used < previous_gas {
                return Err(P2pError::InvalidResponse(format!(
                    "receipt cumulative gas regressed at block {}",
                    header.number
                )));
            }
            previous_gas = receipt.cumulative_gas_used;
        }
        if previous_gas != header.gas_used {
            return Err(P2pError::InvalidResponse(format!(
                "final cumulative gas {previous_gas} differs from header gas {} at block {}",
                header.gas_used, header.number
            )));
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn normalize_sparse_log_frames(
    headers: &[Header],
    request: &DataRequest,
    budget: SourceBudget,
    bloom_positive_indices: &[usize],
    positive_receipts: &[Vec<Receipt>],
    exact_match_positions: &[usize],
    matching_bodies: &[BlockBody],
) -> Result<Vec<BlockFrame>, P2pError> {
    let transaction_hash_required = request
        .log_fields
        .contains(leani_primitives::LogField::TransactionHash);
    let observed_at_unix_ms = observed_at_unix_ms();
    let mut positive_position_by_index = vec![None; headers.len()];
    for (position, index) in bloom_positive_indices.iter().copied().enumerate() {
        positive_position_by_index[index] = Some(position);
    }
    let mut body_position_by_positive = vec![None; positive_receipts.len()];
    for (body_position, positive_position) in exact_match_positions.iter().copied().enumerate() {
        body_position_by_positive[positive_position] = Some(body_position);
    }

    let mut frames = Vec::with_capacity(headers.len());
    let mut total_bytes = 0_u64;
    for (index, header) in headers.iter().enumerate() {
        let positive_position = positive_position_by_index[index];
        let exact_position = positive_position.and_then(|positive_position| {
            body_position_by_positive[positive_position]
                .map(|body_position| (positive_position, body_position))
        });
        let frame = if let Some((positive_position, body_position)) = exact_position {
            let receipts = &positive_receipts[positive_position];
            if transaction_hash_required {
                normalize_block(
                    header,
                    &matching_bodies[body_position],
                    receipts,
                    observed_at_unix_ms,
                    Some(request),
                )?
            } else {
                normalize_sparse_receipt_log_block(header, receipts, request, observed_at_unix_ms)?
            }
        } else {
            normalize_sparse_empty_log_block(
                header,
                request,
                observed_at_unix_ms,
                positive_position.is_some(),
            )?
        };
        push_normalized_frame(&mut frames, frame, &mut total_bytes, budget)?;
    }
    Ok(frames)
}

fn observed_at_unix_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

fn normalize_sparse_empty_log_block(
    header: &Header,
    request: &DataRequest,
    observed_at_unix_ms: u64,
    receipts_verified: bool,
) -> Result<BlockFrame, P2pError> {
    normalize_sparse_log_block(
        header,
        request,
        observed_at_unix_ms,
        receipts_verified,
        Vec::new(),
    )
}

fn normalize_sparse_receipt_log_block(
    header: &Header,
    receipts: &[Receipt],
    request: &DataRequest,
    observed_at_unix_ms: u64,
) -> Result<BlockFrame, P2pError> {
    let scope = sparse_log_scope(request).ok_or_else(|| {
        P2pError::InvalidConfig("request is not eligible for sparse log acquisition".to_owned())
    })?;
    if request
        .log_fields
        .contains(leani_primitives::LogField::TransactionHash)
    {
        return Err(P2pError::InvalidConfig(
            "receipt-only log normalization cannot supply transaction hashes".to_owned(),
        ));
    }
    let mut logs = Vec::new();
    let mut block_log_index = 0_u32;
    for (transaction_index, receipt) in receipts.iter().enumerate() {
        let transaction_index = u32::try_from(transaction_index)
            .map_err(|_| P2pError::InvalidResponse("transaction index overflows u32".to_owned()))?;
        for source_log in &receipt.logs {
            if source_log_matches(Some(scope), source_log) {
                logs.push(Log {
                    address: address(source_log.address),
                    topics: source_log
                        .data
                        .topics()
                        .iter()
                        .map(|topic| topic.0)
                        .collect(),
                    data: source_log.data.data.to_vec(),
                    transaction_hash: None,
                    transaction_index,
                    log_index: block_log_index,
                });
            }
            block_log_index = block_log_index
                .checked_add(1)
                .ok_or_else(|| P2pError::InvalidResponse("log index overflows u32".to_owned()))?;
        }
    }
    normalize_sparse_log_block(header, request, observed_at_unix_ms, true, logs)
}

fn normalize_sparse_log_block(
    header: &Header,
    request: &DataRequest,
    observed_at_unix_ms: u64,
    receipts_verified: bool,
    logs: Vec<Log>,
) -> Result<BlockFrame, P2pError> {
    let scope = sparse_log_scope(request).ok_or_else(|| {
        P2pError::InvalidConfig("request is not eligible for sparse log acquisition".to_owned())
    })?;
    let hash = header.hash_slow();
    let header_requested = requested_material(Some(request), Capability::Header);
    Ok(BlockFrame {
        chain_id: ChainId(1),
        block: BlockRef {
            number: BlockNumber(header.number),
            hash: block_hash(hash),
            parent_hash: block_hash(header.parent_hash),
            timestamp: header.timestamp,
        },
        finality: Finality::Optimistic,
        header: if header_requested {
            Material::Complete(HeaderEnvelope {
                rlp: Some(alloy_rlp::encode(header)),
                transactions_root: Some(block_hash(header.transactions_root)),
                receipts_root: Some(block_hash(header.receipts_root)),
                withdrawals_root: header.withdrawals_root.map(block_hash),
                gas_limit: Some(header.gas_limit),
                gas_used: Some(header.gas_used),
                base_fee_per_gas: header.base_fee_per_gas.map(U256::from).map(quantity),
                blob_gas_used: header.blob_gas_used,
                excess_blob_gas: header.excess_blob_gas,
                size_bytes: None,
                transaction_count: None,
                consensus_size_bytes: None,
            })
        } else {
            Material::Missing(MissingReason::NotRequested)
        },
        transactions: Material::Missing(MissingReason::NotRequested),
        receipts: Material::Missing(MissingReason::NotRequested),
        logs: Material::Filtered {
            value: logs,
            scope: scope.clone(),
            completeness: Completeness::VerifiedPredicate,
        },
        withdrawals: Material::Missing(MissingReason::NotRequested),
        blob_sidecars: Material::Missing(MissingReason::NotRequested),
        traces: Material::Missing(MissingReason::Unsupported),
        state_diffs: Material::Missing(MissingReason::Unsupported),
        provenance: vec![Provenance {
            source_id: SourceId::new("reth-p2p-mainnet")
                .map_err(|error| P2pError::InvalidConfig(error.to_string()))?,
            source_kind: SourceKind::ExecutionP2p,
            trust: TrustModel::ProtocolVerified,
            range: Some(BlockRange::single(BlockNumber(header.number))),
            object: Some(ObjectIdentity {
                locator: format!("devp2p://mainnet/block/{hash:#x}"),
                version: Some(format!("reth/{RETH_VERSION}@{RETH_REVISION}")),
                checksum: Some(hash.0),
                schema: Some(if receipts_verified {
                    "eth/68-70/logs:receipt-filtered".to_owned()
                } else {
                    "eth/68-70/logs:bloom-negative".to_owned()
                }),
            }),
            observed_at_unix_ms,
            projection: vec!["logs".to_owned()],
        }],
        verification: VerificationReport {
            header_hash: VerificationCheck::VERIFIED,
            parent_continuity: VerificationCheck::VERIFIED,
            transactions_root: VerificationCheck::NOT_CHECKED,
            receipts_root: if receipts_verified {
                VerificationCheck::VERIFIED
            } else {
                VerificationCheck::NOT_CHECKED
            },
            withdrawals_root: VerificationCheck::NOT_CHECKED,
            dataset_checksum: VerificationCheck::NOT_CHECKED,
            consensus_anchor: None,
        },
    })
}

fn normalize_verified_headers(
    headers: &[Header],
    budget: SourceBudget,
) -> Result<Vec<BlockFrame>, P2pError> {
    let observed_at_unix_ms = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX);
    let mut total_bytes = 0_u64;
    let mut output = Vec::with_capacity(headers.len());
    for header in headers {
        let hash = header.hash_slow();
        let frame = BlockFrame {
            chain_id: ChainId(1),
            block: BlockRef {
                number: BlockNumber(header.number),
                hash: block_hash(hash),
                parent_hash: block_hash(header.parent_hash),
                timestamp: header.timestamp,
            },
            finality: Finality::Optimistic,
            header: Material::Complete(HeaderEnvelope {
                rlp: Some(alloy_rlp::encode(header)),
                transactions_root: Some(block_hash(header.transactions_root)),
                receipts_root: Some(block_hash(header.receipts_root)),
                withdrawals_root: header.withdrawals_root.map(block_hash),
                gas_limit: Some(header.gas_limit),
                gas_used: Some(header.gas_used),
                base_fee_per_gas: header.base_fee_per_gas.map(U256::from).map(quantity),
                blob_gas_used: header.blob_gas_used,
                excess_blob_gas: header.excess_blob_gas,
                size_bytes: None,
                transaction_count: None,
                consensus_size_bytes: None,
            }),
            transactions: Material::Missing(MissingReason::NotRequested),
            receipts: Material::Missing(MissingReason::NotRequested),
            logs: Material::Missing(MissingReason::NotRequested),
            withdrawals: Material::Missing(MissingReason::NotRequested),
            blob_sidecars: Material::Missing(MissingReason::NotRequested),
            traces: Material::Missing(MissingReason::Unsupported),
            state_diffs: Material::Missing(MissingReason::Unsupported),
            provenance: vec![Provenance {
                source_id: SourceId::new("reth-p2p-mainnet")
                    .map_err(|error| P2pError::InvalidConfig(error.to_string()))?,
                source_kind: SourceKind::ExecutionP2p,
                trust: TrustModel::ProtocolVerified,
                range: Some(BlockRange::single(BlockNumber(header.number))),
                object: Some(ObjectIdentity {
                    locator: format!("devp2p://mainnet/block/{hash:#x}"),
                    version: Some(format!("reth/{RETH_VERSION}@{RETH_REVISION}")),
                    checksum: Some(hash.0),
                    schema: Some("eth/68-70/header".to_owned()),
                }),
                observed_at_unix_ms,
                projection: vec!["header".to_owned()],
            }],
            verification: VerificationReport {
                header_hash: VerificationCheck::VERIFIED,
                parent_continuity: VerificationCheck::VERIFIED,
                transactions_root: VerificationCheck::NOT_CHECKED,
                receipts_root: VerificationCheck::NOT_CHECKED,
                withdrawals_root: VerificationCheck::NOT_CHECKED,
                dataset_checksum: VerificationCheck::NOT_CHECKED,
                consensus_anchor: None,
            },
        };
        push_normalized_frame(&mut output, frame, &mut total_bytes, budget)?;
    }
    Ok(output)
}

fn normalize_verified_bodies(
    headers: &[Header],
    bodies: &[BlockBody],
    budget: SourceBudget,
) -> Result<Vec<BlockFrame>, P2pError> {
    validate_bodies(headers, bodies)?;
    let observed_at_unix_ms = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX);
    let mut total_bytes = 0_u64;
    let mut output = Vec::with_capacity(headers.len());
    for (header, body) in headers.iter().zip(bodies) {
        let hash = header.hash_slow();
        let transactions = normalize_body_transactions(body)?;
        let block_size = alloy_rlp::encode(Block {
            header: header.clone(),
            body: body.clone(),
        })
        .len();
        let frame = BlockFrame {
            chain_id: ChainId(1),
            block: BlockRef {
                number: BlockNumber(header.number),
                hash: block_hash(hash),
                parent_hash: block_hash(header.parent_hash),
                timestamp: header.timestamp,
            },
            finality: Finality::Optimistic,
            header: Material::Complete(HeaderEnvelope {
                rlp: Some(alloy_rlp::encode(header)),
                transactions_root: Some(block_hash(header.transactions_root)),
                receipts_root: Some(block_hash(header.receipts_root)),
                withdrawals_root: header.withdrawals_root.map(block_hash),
                gas_limit: Some(header.gas_limit),
                gas_used: Some(header.gas_used),
                base_fee_per_gas: header.base_fee_per_gas.map(U256::from).map(quantity),
                blob_gas_used: header.blob_gas_used,
                excess_blob_gas: header.excess_blob_gas,
                size_bytes: Some(u64::try_from(block_size).unwrap_or(u64::MAX)),
                transaction_count: Some(u32::try_from(body.transactions.len()).unwrap_or(u32::MAX)),
                consensus_size_bytes: None,
            }),
            transactions: Material::Complete(transactions),
            receipts: Material::Missing(MissingReason::NotRequested),
            logs: Material::Missing(MissingReason::NotRequested),
            withdrawals: Material::Missing(MissingReason::NotRequested),
            blob_sidecars: Material::Missing(MissingReason::NotRequested),
            traces: Material::Missing(MissingReason::Unsupported),
            state_diffs: Material::Missing(MissingReason::Unsupported),
            provenance: vec![Provenance {
                source_id: SourceId::new("reth-p2p-mainnet")
                    .map_err(|error| P2pError::InvalidConfig(error.to_string()))?,
                source_kind: SourceKind::ExecutionP2p,
                trust: TrustModel::ProtocolVerified,
                range: Some(BlockRange::single(BlockNumber(header.number))),
                object: Some(ObjectIdentity {
                    locator: format!("devp2p://mainnet/block/{hash:#x}"),
                    version: Some(format!("reth/{RETH_VERSION}@{RETH_REVISION}")),
                    checksum: Some(hash.0),
                    schema: Some("eth/68-70/header+body".to_owned()),
                }),
                observed_at_unix_ms,
                projection: vec!["header".to_owned(), "body".to_owned()],
            }],
            verification: VerificationReport {
                header_hash: VerificationCheck::VERIFIED,
                parent_continuity: VerificationCheck::VERIFIED,
                transactions_root: VerificationCheck::VERIFIED,
                receipts_root: VerificationCheck::NOT_CHECKED,
                withdrawals_root: VerificationCheck::VERIFIED,
                dataset_checksum: VerificationCheck::NOT_CHECKED,
                consensus_anchor: None,
            },
        };
        push_normalized_frame(&mut output, frame, &mut total_bytes, budget)?;
    }
    Ok(output)
}

fn normalize_body_transactions(body: &BlockBody) -> Result<Vec<TransactionEnvelope>, P2pError> {
    body.transactions
        .iter()
        .enumerate()
        .map(|(index, transaction)| {
            let encoded = transaction.encoded_2718();
            Ok(TransactionEnvelope {
                hash: transaction_hash(*transaction.tx_hash()),
                transaction_type: transaction.tx_type() as u8,
                index: u32::try_from(index).map_err(|_| {
                    P2pError::InvalidResponse(
                        "transaction index overflows u32 in verified body".to_owned(),
                    )
                })?,
                encoded: Some(encoded.clone()),
                from: None,
                to: transaction.to().map(address),
                nonce: Some(transaction.nonce()),
                gas_limit: Some(transaction.gas_limit()),
                value: Some(quantity(transaction.value())),
                input: Some(transaction.input().to_vec()),
                max_fee_per_gas: Some(quantity(U256::from(transaction.max_fee_per_gas()))),
                max_priority_fee_per_gas: transaction
                    .max_priority_fee_per_gas()
                    .map(U256::from)
                    .map(quantity),
                max_fee_per_blob_gas: transaction
                    .max_fee_per_blob_gas()
                    .map(U256::from)
                    .map(quantity),
                blob_versioned_hashes: transaction
                    .blob_versioned_hashes()
                    .unwrap_or_default()
                    .iter()
                    .copied()
                    .map(block_hash)
                    .collect(),
                size_bytes: Some(u32::try_from(encoded.len()).unwrap_or(u32::MAX)),
            })
        })
        .collect()
}

fn normalize_verified(
    headers: &[Header],
    bodies: &[BlockBody],
    receipts: &[Vec<Receipt>],
    budget: SourceBudget,
) -> Result<Vec<BlockFrame>, P2pError> {
    let observed_at_unix_ms = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX);
    let mut total_bytes = 0_u64;
    let mut output = Vec::with_capacity(headers.len());
    for ((header, body), block_receipts) in headers.iter().zip(bodies).zip(receipts) {
        let frame = normalize_block(header, body, block_receipts, observed_at_unix_ms, None)?;
        push_normalized_frame(&mut output, frame, &mut total_bytes, budget)?;
    }
    Ok(output)
}

fn normalize_verified_for_request(
    headers: &[Header],
    bodies: &[BlockBody],
    receipts: &[Vec<Receipt>],
    request: &DataRequest,
    budget: SourceBudget,
) -> Result<Vec<BlockFrame>, P2pError> {
    let observed_at_unix_ms = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX);
    let mut total_bytes = 0_u64;
    let mut output = Vec::with_capacity(headers.len());
    for ((header, body), block_receipts) in headers.iter().zip(bodies).zip(receipts) {
        let frame = normalize_block(
            header,
            body,
            block_receipts,
            observed_at_unix_ms,
            Some(request),
        )?;
        push_normalized_frame(&mut output, frame, &mut total_bytes, budget)?;
    }
    Ok(output)
}

fn push_normalized_frame(
    output: &mut Vec<BlockFrame>,
    frame: BlockFrame,
    total_bytes: &mut u64,
    budget: SourceBudget,
) -> Result<(), P2pError> {
    let frame_bytes = frame.estimated_heap_bytes();
    if frame_bytes > budget.max_frame_bytes {
        return Err(P2pError::Source(SourceError::BudgetExceeded {
            resource: "frame_bytes",
            limit: budget.max_frame_bytes,
            observed: frame_bytes,
        }));
    }
    *total_bytes = total_bytes.saturating_add(frame_bytes);
    if *total_bytes > budget.max_input_bytes {
        return Err(P2pError::Source(SourceError::BudgetExceeded {
            resource: "input_bytes",
            limit: budget.max_input_bytes,
            observed: *total_bytes,
        }));
    }
    output.push(frame);
    Ok(())
}

fn requested_material(request: Option<&DataRequest>, capability: Capability) -> bool {
    request.is_none_or(|request| match capability {
        Capability::Transactions => {
            request.required.contains(Capability::Transactions)
                || request.required.contains(Capability::Body)
                || request.required.contains(Capability::Calldata)
        }
        _ => request.required.contains(capability),
    })
}

fn header_only_request(request: &DataRequest) -> bool {
    request.required == CapabilitySet::of(Capability::Header)
}

fn header_and_body_only_request(request: &DataRequest) -> bool {
    request.required == CapabilitySet::of(Capability::Header).with(Capability::Body)
}

fn effective_filter_scope(request: Option<&DataRequest>) -> Option<FilterScope> {
    let request = request.filter(|request| request.allow_filtered)?;
    let mut scope = request.filters.scope.clone();
    if scope.senders.is_empty() {
        scope.senders.clone_from(&request.filters.senders);
    }
    if scope.recipients.is_empty() {
        scope.recipients.clone_from(&request.filters.recipients);
    }
    (scope != FilterScope::default()).then_some(scope)
}

fn transaction_filter_active(scope: Option<&FilterScope>) -> bool {
    scope.is_some_and(|scope| {
        !scope.transaction_types.is_empty()
            || !scope.transaction_hashes.is_empty()
            || !scope.senders.is_empty()
            || !scope.recipients.is_empty()
    })
}

fn log_filter_active(scope: Option<&FilterScope>) -> bool {
    scope.is_some_and(|scope| !scope.addresses.is_empty() || !scope.topics.is_empty())
}

fn sparse_log_scope(request: &DataRequest) -> Option<&FilterScope> {
    let scope = request.allow_filtered.then_some(&request.filters.scope)?;
    let log_only = request.required.contains(Capability::Logs)
        && ![
            Capability::Body,
            Capability::Transactions,
            Capability::Calldata,
            Capability::Receipts,
            Capability::Withdrawals,
            Capability::BlobSidecars,
            Capability::Traces,
            Capability::StateDiffs,
            Capability::Mempool,
        ]
        .into_iter()
        .any(|capability| request.required.contains(capability));
    let separate_transaction_filters =
        !request.filters.senders.is_empty() || !request.filters.recipients.is_empty();
    (log_only
        && !separate_transaction_filters
        && !transaction_filter_active(Some(scope))
        && log_filter_active(Some(scope)))
    .then_some(scope)
}

fn header_bloom_matches(scope: &FilterScope, header: &Header) -> bool {
    let address_matches = scope.addresses.is_empty()
        || scope.addresses.iter().any(|candidate| {
            header
                .logs_bloom
                .contains_input(BloomInput::Raw(&candidate.0))
        });
    address_matches
        && scope.topics.iter().all(|filter| {
            !filter.alternatives.is_empty()
                && filter
                    .alternatives
                    .iter()
                    .any(|candidate| header.logs_bloom.contains_input(BloomInput::Raw(candidate)))
        })
}

fn source_log_matches(scope: Option<&FilterScope>, source_log: &alloy_primitives::Log) -> bool {
    let Some(scope) = scope else {
        return true;
    };
    if !scope.addresses.is_empty() && !scope.addresses.contains(&address(source_log.address)) {
        return false;
    }
    scope.topics.iter().all(|filter| {
        source_log
            .data
            .topics()
            .get(usize::from(filter.position))
            .is_some_and(|topic| filter.alternatives.contains(&topic.0))
    })
}

fn normalized_material<T>(
    value: T,
    requested: bool,
    filtered: bool,
    scope: Option<&FilterScope>,
) -> Material<T> {
    if !requested {
        Material::Missing(MissingReason::NotRequested)
    } else if filtered {
        Material::Filtered {
            value,
            scope: scope.cloned().unwrap_or_default(),
            completeness: Completeness::VerifiedPredicate,
        }
    } else {
        Material::Complete(value)
    }
}

#[allow(clippy::too_many_lines)]
fn normalize_block(
    header: &Header,
    body: &BlockBody,
    receipts: &[Receipt],
    observed_at_unix_ms: u64,
    request: Option<&DataRequest>,
) -> Result<BlockFrame, P2pError> {
    let hash = header.hash_slow();
    let normalized_block_hash = block_hash(hash);
    let header_requested = requested_material(request, Capability::Header);
    let transactions_requested = requested_material(request, Capability::Transactions);
    let receipts_requested = requested_material(request, Capability::Receipts);
    let logs_requested = requested_material(request, Capability::Logs);
    let withdrawals_requested = requested_material(request, Capability::Withdrawals);
    let filter_scope = effective_filter_scope(request);
    let transaction_filtered = transaction_filter_active(filter_scope.as_ref());
    let logs_filtered = log_filter_active(filter_scope.as_ref()) || transaction_filtered;
    let (header_rlp, block_size) = if header_requested {
        (
            Some(alloy_rlp::encode(header)),
            Some(
                alloy_rlp::encode(Block {
                    header: header.clone(),
                    body: body.clone(),
                })
                .len(),
            ),
        )
    } else {
        (None, None)
    };
    let mut normalized_transactions = Vec::new();
    let mut normalized_receipts = Vec::new();
    let mut normalized_logs = Vec::new();
    let mut previous_gas = 0_u64;
    let mut block_log_index = 0_u32;
    for (index, (transaction, receipt)) in body.transactions.iter().zip(receipts).enumerate() {
        let transaction_index = u32::try_from(index)
            .map_err(|_| P2pError::InvalidResponse("transaction index overflows u32".to_owned()))?;
        let transaction_hash = transaction_hash(*transaction.tx_hash());
        let gas_used = receipt.cumulative_gas_used.saturating_sub(previous_gas);
        previous_gas = receipt.cumulative_gas_used;
        let scope = filter_scope.as_ref();
        let preliminary_transaction_match = scope.is_none_or(|scope| {
            (scope.transaction_types.is_empty()
                || scope
                    .transaction_types
                    .contains(&(transaction.tx_type() as u8)))
                && (scope.transaction_hashes.is_empty()
                    || scope.transaction_hashes.contains(&transaction_hash))
                && (scope.recipients.is_empty()
                    || transaction
                        .to()
                        .map(address)
                        .is_some_and(|recipient| scope.recipients.contains(&recipient)))
        });
        let sender = if preliminary_transaction_match
            && (transactions_requested || scope.is_some_and(|scope| !scope.senders.is_empty()))
        {
            Some(transaction.recover_signer().map_err(|error| {
                P2pError::InvalidResponse(format!(
                    "cannot recover sender for {transaction_hash}: {error}"
                ))
            })?)
        } else {
            None
        };
        let transaction_matches = preliminary_transaction_match
            && scope.is_none_or(|scope| {
                scope.senders.is_empty()
                    || sender
                        .map(address)
                        .is_some_and(|sender| scope.senders.contains(&sender))
            });
        let mut receipt_logs = Vec::new();
        for source_log in &receipt.logs {
            let selected_for_receipt = receipts_requested && transaction_matches;
            let selected_for_logs =
                logs_requested && transaction_matches && source_log_matches(scope, source_log);
            if selected_for_receipt || selected_for_logs {
                let log = Log {
                    address: address(source_log.address),
                    topics: source_log
                        .data
                        .topics()
                        .iter()
                        .map(|topic| topic.0)
                        .collect(),
                    data: source_log.data.data.to_vec(),
                    transaction_hash: request
                        .is_none_or(|request| {
                            request
                                .log_fields
                                .contains(leani_primitives::LogField::TransactionHash)
                        })
                        .then_some(transaction_hash),
                    transaction_index,
                    log_index: block_log_index,
                };
                if selected_for_receipt {
                    receipt_logs.push(log.clone());
                }
                if selected_for_logs {
                    normalized_logs.push(log);
                }
            }
            block_log_index = block_log_index
                .checked_add(1)
                .ok_or_else(|| P2pError::InvalidResponse("log index overflows u32".to_owned()))?;
        }
        if transactions_requested && transaction_matches {
            let encoded = transaction.encoded_2718();
            normalized_transactions.push(TransactionEnvelope {
                hash: transaction_hash,
                transaction_type: transaction.tx_type() as u8,
                index: transaction_index,
                encoded: Some(encoded.clone()),
                from: sender.map(address),
                to: transaction.to().map(address),
                nonce: Some(transaction.nonce()),
                gas_limit: Some(transaction.gas_limit()),
                value: Some(quantity(transaction.value())),
                input: Some(transaction.input().to_vec()),
                max_fee_per_gas: Some(quantity(U256::from(transaction.max_fee_per_gas()))),
                max_priority_fee_per_gas: transaction
                    .max_priority_fee_per_gas()
                    .map(U256::from)
                    .map(quantity),
                max_fee_per_blob_gas: transaction
                    .max_fee_per_blob_gas()
                    .map(U256::from)
                    .map(quantity),
                blob_versioned_hashes: transaction
                    .blob_versioned_hashes()
                    .unwrap_or_default()
                    .iter()
                    .copied()
                    .map(block_hash)
                    .collect(),
                size_bytes: Some(u32::try_from(encoded.len()).unwrap_or(u32::MAX)),
            });
        }
        if receipts_requested && transaction_matches {
            normalized_receipts.push(ReceiptEnvelope {
                transaction_hash,
                transaction_type: receipt.tx_type as u8,
                transaction_index,
                encoded: Some(receipt.with_bloom_ref().encoded_2718()),
                success: Some(receipt.success),
                gas_used: Some(gas_used),
                effective_gas_price: Some(quantity(U256::from(
                    transaction.effective_gas_price(header.base_fee_per_gas),
                ))),
                blob_gas_used: transaction.blob_gas_used(),
                blob_gas_price: None,
                logs: receipt_logs,
            });
        }
    }
    let normalized_withdrawals = if withdrawals_requested {
        body.withdrawals
            .as_ref()
            .map(|withdrawals| {
                withdrawals
                    .iter()
                    .map(|withdrawal| Withdrawal {
                        index: withdrawal.index,
                        validator_index: withdrawal.validator_index,
                        address: address(withdrawal.address),
                        amount_gwei: withdrawal.amount,
                    })
                    .collect()
            })
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    let verification = VerificationReport {
        header_hash: VerificationCheck::VERIFIED,
        parent_continuity: VerificationCheck::VERIFIED,
        transactions_root: VerificationCheck::VERIFIED,
        receipts_root: VerificationCheck::VERIFIED,
        withdrawals_root: VerificationCheck::VERIFIED,
        dataset_checksum: VerificationCheck::NOT_CHECKED,
        consensus_anchor: None,
    };
    Ok(BlockFrame {
        chain_id: ChainId(1),
        block: BlockRef {
            number: BlockNumber(header.number),
            hash: normalized_block_hash,
            parent_hash: block_hash(header.parent_hash),
            timestamp: header.timestamp,
        },
        finality: Finality::Optimistic,
        header: if header_requested {
            Material::Complete(HeaderEnvelope {
                rlp: header_rlp,
                transactions_root: Some(block_hash(header.transactions_root)),
                receipts_root: Some(block_hash(header.receipts_root)),
                withdrawals_root: header.withdrawals_root.map(block_hash),
                gas_limit: Some(header.gas_limit),
                gas_used: Some(header.gas_used),
                base_fee_per_gas: header.base_fee_per_gas.map(U256::from).map(quantity),
                blob_gas_used: header.blob_gas_used,
                excess_blob_gas: header.excess_blob_gas,
                size_bytes: block_size.map(|size| u64::try_from(size).unwrap_or(u64::MAX)),
                consensus_size_bytes: None,
                transaction_count: Some(u32::try_from(body.transactions.len()).unwrap_or(u32::MAX)),
            })
        } else {
            Material::Missing(MissingReason::NotRequested)
        },
        transactions: normalized_material(
            normalized_transactions,
            transactions_requested,
            transaction_filtered,
            filter_scope.as_ref(),
        ),
        receipts: normalized_material(
            normalized_receipts,
            receipts_requested,
            transaction_filtered,
            filter_scope.as_ref(),
        ),
        logs: normalized_material(
            normalized_logs,
            logs_requested,
            logs_filtered,
            filter_scope.as_ref(),
        ),
        withdrawals: normalized_material(
            normalized_withdrawals,
            withdrawals_requested,
            false,
            None,
        ),
        blob_sidecars: Material::Missing(MissingReason::NotRequested),
        traces: Material::Missing(MissingReason::Unsupported),
        state_diffs: Material::Missing(MissingReason::Unsupported),
        provenance: vec![Provenance {
            source_id: SourceId::new("reth-p2p-mainnet")
                .map_err(|error| P2pError::InvalidConfig(error.to_string()))?,
            source_kind: SourceKind::ExecutionP2p,
            trust: TrustModel::ProtocolVerified,
            range: Some(BlockRange::single(BlockNumber(header.number))),
            object: Some(ObjectIdentity {
                locator: format!("devp2p://mainnet/block/{hash:#x}"),
                version: Some(format!("reth/{RETH_VERSION}@{RETH_REVISION}")),
                checksum: Some(hash.0),
                schema: Some("eth/68-70".to_owned()),
            }),
            observed_at_unix_ms,
            projection: request.map_or_else(
                || {
                    vec![
                        "header".to_owned(),
                        "body".to_owned(),
                        "receipts".to_owned(),
                    ]
                },
                |request| {
                    request
                        .required
                        .iter()
                        .map(|capability| format!("{capability:?}").to_lowercase())
                        .collect()
                },
            ),
        }],
        verification,
    })
}

fn block_hash(value: B256) -> BlockHash {
    BlockHash::new(value.0)
}

fn transaction_hash(value: B256) -> TransactionHash {
    TransactionHash::new(value.0)
}

fn address(value: AlloyAddress) -> Address {
    Address::new(value.0.0)
}

fn quantity(value: U256) -> Quantity {
    Quantity::new(value.to_be_bytes())
}

#[derive(Debug, Error)]
pub enum P2pError {
    #[error("invalid P2P configuration: {0}")]
    InvalidConfig(String),
    #[error("fixed P2P range contains {requested} blocks; maximum is {maximum}")]
    RangeTooLarge { requested: u64, maximum: u64 },
    #[error("P2P reorg exceeds the retained depth of {maximum} blocks")]
    ReorgTooDeep { maximum: usize },
    #[error("failed to start Reth networking: {0}")]
    Network(String),
    #[error("P2P source was cancelled")]
    Cancelled,
    #[error("only {connected} peers connected before timeout; required {minimum}")]
    PeerTimeout { minimum: usize, connected: usize },
    #[error("timed out requesting {component}")]
    Timeout { component: &'static str },
    #[error("P2P {component} request failed: {detail}")]
    Request {
        component: &'static str,
        detail: String,
    },
    #[error("invalid peer response: {0}")]
    InvalidResponse(String),
    #[error("incomplete peer {component} response: returned {returned}, expected {expected}")]
    IncompleteResponse {
        component: &'static str,
        returned: usize,
        expected: usize,
    },
    #[error(transparent)]
    Source(SourceError),
}

impl From<P2pError> for SourceError {
    fn from(error: P2pError) -> Self {
        match error {
            P2pError::Cancelled => Self::Cancelled,
            P2pError::Source(error) => error,
            P2pError::InvalidResponse(detail) => Self::CorruptFrame(detail),
            P2pError::PeerTimeout { .. } | P2pError::Network(_) => {
                Self::Unavailable(error.to_string())
            }
            P2pError::InvalidConfig(_)
            | P2pError::RangeTooLarge { .. }
            | P2pError::ReorgTooDeep { .. }
            | P2pError::Timeout { .. }
            | P2pError::IncompleteResponse { .. }
            | P2pError::Request { .. } => Self::Protocol(error.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy_consensus::{
        EthereumReceipt, Header, TxType,
        constants::EMPTY_OMMER_ROOT_HASH,
        proofs::{calculate_receipt_root, calculate_transaction_root},
    };

    use super::*;

    fn direct_peer_pool() -> Arc<DirectPeerPool> {
        Arc::new(DirectPeerPool::new(Arc::new(PeerQualityStore::load(None))))
    }

    fn node_record(marker: u8) -> NodeRecord {
        let secret = SecretKey::from_slice(&[marker; 32]).expect("valid test secret");
        NodeRecord::from_secret_key(
            SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::LOCALHOST,
                30_300 + u16::from(marker),
            )),
            &secret,
        )
    }

    fn empty_fixture() -> (BlockRange, Vec<Header>, Vec<BlockBody>, Vec<Vec<Receipt>>) {
        let range = BlockRange::new(BlockNumber(10), BlockNumber(11)).expect("range");
        let body = BlockBody::default();
        let receipts: Vec<Receipt> = Vec::new();
        let receipt_bloom = receipts
            .iter()
            .map(alloy_consensus::TxReceipt::with_bloom_ref)
            .collect::<Vec<_>>();
        let first = Header {
            number: 10,
            timestamp: 100,
            transactions_root: calculate_transaction_root(&body.transactions),
            receipts_root: calculate_receipt_root(&receipt_bloom),
            ommers_hash: EMPTY_OMMER_ROOT_HASH,
            gas_used: 0,
            ..Default::default()
        };
        let second = Header {
            number: 11,
            parent_hash: first.hash_slow(),
            timestamp: 112,
            transactions_root: first.transactions_root,
            receipts_root: first.receipts_root,
            ommers_hash: EMPTY_OMMER_ROOT_HASH,
            gas_used: 0,
            ..Default::default()
        };
        (
            range,
            vec![first, second],
            vec![body.clone(), body],
            vec![receipts.clone(), receipts],
        )
    }

    #[test]
    fn validates_and_normalizes_complete_responses() {
        let (range, headers, bodies, receipts) = empty_fixture();
        validate_headers(range, &headers, Some(block_hash(headers[1].hash_slow())))
            .expect("headers");
        validate_bodies(&headers, &bodies).expect("bodies");
        validate_receipts_against_headers(&headers, &receipts)
            .expect("receipts are independently committed by headers");
        validate_receipts(&headers, &bodies, &receipts).expect("receipts");
        let frames = normalize_verified(
            &headers,
            &bodies,
            &receipts,
            SourceBudget {
                max_input_bytes: 1_000_000,
                max_frame_bytes: 1_000_000,
                max_frames: 2,
                max_buffered_frames: 2,
                max_in_flight_requests: 1,
                temporary_disk_bytes: 1,
            },
        )
        .expect("normalize");
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[1].block.parent_hash, frames[0].block.hash);
        assert!(frames.iter().all(|frame| frame.validate_shape().is_ok()));
    }

    #[test]
    fn header_only_requests_normalize_without_bodies_or_receipts() {
        let (range, headers, _, _) = empty_fixture();
        let request = DataRequest {
            chain_id: ChainId(1),
            range,
            required: CapabilitySet::of(Capability::Header),
            allow_filtered: false,
            projection: leani_source_api::FieldProjection::default(),
            log_fields: leani_primitives::LogFieldSet::NONE,
            filters: leani_source_api::FilterSet::default(),
            minimum_finality: Finality::Optimistic,
            verification_policy: leani_source_api::VerificationPolicy::CompleteCryptographic,
        };
        assert!(header_only_request(&request));
        let frames = normalize_verified_headers(
            &headers,
            SourceBudget {
                max_input_bytes: 1_000_000,
                max_frame_bytes: 1_000_000,
                max_frames: 2,
                max_buffered_frames: 2,
                max_in_flight_requests: 1,
                temporary_disk_bytes: 0,
            },
        )
        .expect("header frames");

        assert_eq!(frames.len(), 2);
        assert!(frames.iter().all(|frame| {
            matches!(frame.header, Material::Complete(_))
                && matches!(
                    frame.transactions,
                    Material::Missing(MissingReason::NotRequested)
                )
                && matches!(
                    frame.receipts,
                    Material::Missing(MissingReason::NotRequested)
                )
                && frame.capabilities().complete.contains(Capability::Header)
        }));
    }

    #[test]
    fn header_and_body_requests_supply_transaction_counts_without_receipts() {
        let (range, headers, bodies, _) = empty_fixture();
        let request = DataRequest {
            chain_id: ChainId(1),
            range,
            required: CapabilitySet::of(Capability::Header).with(Capability::Body),
            allow_filtered: false,
            projection: leani_source_api::FieldProjection::default(),
            log_fields: leani_primitives::LogFieldSet::NONE,
            filters: leani_source_api::FilterSet::default(),
            minimum_finality: Finality::Optimistic,
            verification_policy: leani_source_api::VerificationPolicy::CompleteCryptographic,
        };
        assert!(header_and_body_only_request(&request));
        let frames = normalize_verified_bodies(
            &headers,
            &bodies,
            SourceBudget {
                max_input_bytes: 1_000_000,
                max_frame_bytes: 1_000_000,
                max_frames: 2,
                max_buffered_frames: 2,
                max_in_flight_requests: 1,
                temporary_disk_bytes: 0,
            },
        )
        .expect("header and body frames");

        assert_eq!(frames.len(), 2);
        assert!(frames.iter().all(|frame| {
            frame
                .header
                .as_complete()
                .is_some_and(|header| header.transaction_count == Some(0))
                && frame.transactions.as_complete().is_some_and(Vec::is_empty)
                && matches!(
                    frame.receipts,
                    Material::Missing(MissingReason::NotRequested)
                )
                && frame
                    .capabilities()
                    .complete
                    .with_derivable()
                    .contains(Capability::Body)
        }));
    }

    #[test]
    fn history_normalization_preserves_verified_processor_predicates() {
        let (range, headers, bodies, receipts) = empty_fixture();
        let request = DataRequest {
            chain_id: ChainId(1),
            range,
            required: CapabilitySet::of(Capability::Header)
                .with(Capability::Transactions)
                .with(Capability::Receipts),
            allow_filtered: true,
            projection: leani_source_api::FieldProjection::default(),
            log_fields: leani_primitives::LogFieldSet::NONE,
            filters: leani_source_api::FilterSet {
                scope: FilterScope {
                    transaction_types: vec![3],
                    ..FilterScope::default()
                },
                senders: Vec::new(),
                recipients: Vec::new(),
            },
            minimum_finality: Finality::Finalized,
            verification_policy: leani_source_api::VerificationPolicy::CompleteCryptographic,
        };
        let frames = normalize_verified_for_request(
            &headers,
            &bodies,
            &receipts,
            &request,
            SourceBudget {
                max_input_bytes: 1_000_000,
                max_frame_bytes: 1_000_000,
                max_frames: 2,
                max_buffered_frames: 2,
                max_in_flight_requests: 1,
                temporary_disk_bytes: 1,
            },
        )
        .expect("filtered normalization");

        assert!(frames.iter().all(|frame| {
            matches!(
                &frame.transactions,
                Material::Filtered {
                    value,
                    scope,
                    completeness: Completeness::VerifiedPredicate,
                } if value.is_empty() && scope.transaction_types == [3]
            ) && matches!(
                &frame.receipts,
                Material::Filtered {
                    value,
                    completeness: Completeness::VerifiedPredicate,
                    ..
                } if value.is_empty()
            ) && matches!(frame.logs, Material::Missing(MissingReason::NotRequested))
                && matches!(
                    frame.withdrawals,
                    Material::Missing(MissingReason::NotRequested)
                )
        }));
    }

    #[test]
    fn sparse_log_requests_require_a_safe_pushdown_shape() {
        let (range, _, _, _) = empty_fixture();
        let address = Address::new([0x11; 20]);
        let topic = [0x22; 32];
        let mut request = DataRequest {
            chain_id: ChainId(1),
            range,
            required: CapabilitySet::of(Capability::Logs),
            allow_filtered: true,
            projection: leani_source_api::FieldProjection::default(),
            log_fields: leani_primitives::LogFieldSet::NONE,
            filters: leani_source_api::FilterSet {
                scope: FilterScope {
                    addresses: vec![address],
                    topics: vec![leani_primitives::TopicFilter {
                        position: 0,
                        alternatives: vec![topic],
                    }],
                    ..FilterScope::default()
                },
                senders: Vec::new(),
                recipients: Vec::new(),
            },
            minimum_finality: Finality::Finalized,
            verification_policy: leani_source_api::VerificationPolicy::CompleteCryptographic,
        };
        assert!(sparse_log_scope(&request).is_some());

        request.required = request.required.with(Capability::Receipts);
        assert!(sparse_log_scope(&request).is_none());
        request.required = CapabilitySet::of(Capability::Logs);
        request.filters.scope.transaction_types = vec![3];
        assert!(sparse_log_scope(&request).is_none());
        request.filters.scope.transaction_types.clear();
        request.filters.senders.push(Address::new([0x33; 20]));
        assert!(sparse_log_scope(&request).is_none());
        request.filters.senders.clear();
        request.filters.recipients.push(Address::new([0x44; 20]));
        assert!(sparse_log_scope(&request).is_none());
        request.filters.recipients.clear();
        request.allow_filtered = false;
        assert!(sparse_log_scope(&request).is_none());
    }

    #[test]
    fn header_bloom_filter_has_no_predicate_false_negatives() {
        let address = Address::new([0x11; 20]);
        let other_address = Address::new([0x12; 20]);
        let topic = [0x22; 32];
        let other_topic = [0x23; 32];
        let mut header = Header::default();
        header.logs_bloom.accrue(BloomInput::Raw(&address.0));
        header.logs_bloom.accrue(BloomInput::Raw(&topic));
        let scope = FilterScope {
            addresses: vec![other_address, address],
            topics: vec![leani_primitives::TopicFilter {
                position: 0,
                alternatives: vec![other_topic, topic],
            }],
            ..FilterScope::default()
        };
        assert!(header_bloom_matches(&scope, &header));

        let wrong_address = FilterScope {
            addresses: vec![other_address],
            ..scope.clone()
        };
        assert!(!header_bloom_matches(&wrong_address, &header));
        let impossible_topic = FilterScope {
            topics: vec![leani_primitives::TopicFilter {
                position: 0,
                alternatives: Vec::new(),
            }],
            ..scope
        };
        assert!(!header_bloom_matches(&impossible_topic, &header));
    }

    #[test]
    fn receipt_only_log_normalization_preserves_position_without_transaction_hash() {
        let (range, headers, _, _) = empty_fixture();
        let address = Address::new([0x11; 20]);
        let topic = [0x22; 32];
        let request = DataRequest {
            chain_id: ChainId(1),
            range,
            required: CapabilitySet::of(Capability::Logs),
            log_fields: leani_primitives::LogFieldSet::NONE,
            allow_filtered: true,
            projection: leani_source_api::FieldProjection::default(),
            filters: leani_source_api::FilterSet {
                scope: FilterScope {
                    addresses: vec![address],
                    topics: vec![leani_primitives::TopicFilter {
                        position: 0,
                        alternatives: vec![topic],
                    }],
                    ..FilterScope::default()
                },
                senders: Vec::new(),
                recipients: Vec::new(),
            },
            minimum_finality: Finality::Finalized,
            verification_policy: leani_source_api::VerificationPolicy::CompleteCryptographic,
        };
        let receipts = vec![EthereumReceipt {
            tx_type: TxType::Legacy,
            success: true,
            cumulative_gas_used: 0,
            logs: vec![alloy_primitives::Log::new_unchecked(
                AlloyAddress::from(address.0),
                vec![B256::from(topic)],
                vec![0xaa, 0xbb].into(),
            )],
        }];
        let frame = normalize_sparse_receipt_log_block(&headers[0], &receipts, &request, 1)
            .expect("receipt-only frame");
        let Material::Filtered {
            value,
            completeness: Completeness::VerifiedPredicate,
            ..
        } = frame.logs
        else {
            panic!("expected predicate-complete logs");
        };
        assert_eq!(value.len(), 1);
        assert_eq!(value[0].transaction_hash, None);
        assert_eq!(value[0].transaction_index, 0);
        assert_eq!(value[0].log_index, 0);
        assert_eq!(value[0].data, [0xaa, 0xbb]);
    }

    #[test]
    fn sparse_log_metrics_account_for_avoided_material() {
        let metrics = P2pRequestMetrics::default();
        metrics.record_sparse_log_window(32, 12, 7, 7);
        assert_eq!(
            metrics.snapshot().sparse_logs,
            P2pSparseLogMetrics {
                eligible_blocks: 32,
                bloom_negative_blocks: 20,
                bloom_positive_blocks: 12,
                receipt_fetched_blocks: 12,
                exact_match_blocks: 7,
                body_fetched_blocks: 7,
                avoided_receipt_blocks: 20,
                avoided_body_blocks: 25,
            }
        );
    }

    #[test]
    fn partial_material_responses_are_retryable_not_corrupt() {
        let (_, headers, bodies, receipts) = empty_fixture();
        assert!(matches!(
            validate_bodies(&headers, &bodies[..1]),
            Err(P2pError::IncompleteResponse {
                component: "bodies",
                returned: 1,
                expected: 2,
            })
        ));
        assert!(matches!(
            validate_receipts(&headers, &bodies, &receipts[..1]),
            Err(P2pError::IncompleteResponse {
                component: "receipts",
                returned: 1,
                expected: 2,
            })
        ));
    }

    #[test]
    fn incomplete_live_material_stays_on_the_active_peer_pool_with_bounded_backoff() {
        let config = RethP2pConfig::default();
        let mut attempts = 0;
        let missing_body = P2pError::IncompleteResponse {
            component: "bodies",
            returned: 0,
            expected: 1,
        };
        let missing_receipts = P2pError::IncompleteResponse {
            component: "receipts",
            returned: 0,
            expected: 1,
        };

        assert_eq!(
            pending_live_material_delay(&config, &mut attempts, &missing_body),
            Some(Duration::from_millis(250))
        );
        assert_eq!(
            pending_live_material_delay(&config, &mut attempts, &missing_receipts),
            Some(Duration::from_millis(500))
        );
        assert_eq!(
            pending_live_material_delay(&config, &mut attempts, &missing_body),
            Some(Duration::from_secs(1))
        );
        assert_eq!(
            pending_live_material_delay(&config, &mut attempts, &missing_receipts),
            Some(Duration::from_secs(2))
        );
        assert_eq!(
            pending_live_material_delay(&config, &mut attempts, &missing_body),
            Some(Duration::from_secs(2))
        );
        assert_eq!(attempts, 5);

        let actual_disconnect = P2pError::Network("peer pool stopped".to_owned());
        assert_eq!(
            pending_live_material_delay(&config, &mut attempts, &actual_disconnect),
            None
        );
        assert_eq!(attempts, 5);
    }

    #[test]
    fn live_head_unavailability_grace_retains_one_expected_block_interval() {
        let started = Instant::now();
        let grace = Duration::from_secs(12);
        let mut unavailable_since = None;

        assert_eq!(
            head_unavailable_for(&mut unavailable_since, started),
            Duration::ZERO
        );
        assert!(
            head_unavailable_for(
                &mut unavailable_since,
                started + grace.saturating_sub(Duration::from_millis(1)),
            ) < grace
        );
        assert_eq!(
            head_unavailable_for(&mut unavailable_since, started + grace),
            grace
        );
    }

    #[test]
    fn material_batch_tuning_starts_wide_and_uses_aimd() {
        let tuning = MaterialBatchTuning::new(DEFAULT_MATERIAL_REQUEST_BLOCKS);
        assert_eq!(tuning.body_blocks(), DEFAULT_MATERIAL_REQUEST_BLOCKS);
        tuning.body_failed();
        assert_eq!(tuning.body_blocks(), DEFAULT_MATERIAL_REQUEST_BLOCKS / 2);
        for _ in 0..MATERIAL_BATCH_GROW_SUCCESS_WINDOWS {
            tuning.body_succeeded();
        }
        assert_eq!(tuning.body_blocks(), DEFAULT_MATERIAL_REQUEST_BLOCKS);

        assert_eq!(tuning.receipt_blocks(), DEFAULT_MATERIAL_REQUEST_BLOCKS);
        tuning.receipts_failed();
        assert_eq!(tuning.receipt_blocks(), DEFAULT_MATERIAL_REQUEST_BLOCKS / 2);

        let wider = MaterialBatchTuning::new(16);
        wider.body_failed();
        assert_eq!(wider.body_blocks(), 8);
        for _ in 0..MATERIAL_BATCH_GROW_SUCCESS_WINDOWS.saturating_sub(1) {
            wider.body_succeeded();
        }
        assert_eq!(wider.body_blocks(), 8);
        wider.body_succeeded();
        assert_eq!(wider.body_blocks(), 16);
    }

    #[test]
    fn finalized_history_uses_exact_consensus_anchor_only_on_anchor_frame() {
        let (_, headers, bodies, receipts) = empty_fixture();
        let mut frames = normalize_verified(
            &headers,
            &bodies,
            &receipts,
            SourceBudget {
                max_input_bytes: 1_000_000,
                max_frame_bytes: 1_000_000,
                max_frames: 2,
                max_buffered_frames: 2,
                max_in_flight_requests: 1,
                temporary_disk_bytes: 1,
            },
        )
        .expect("normalize");
        let anchor = ConsensusAnchor {
            finality: Finality::Finalized,
            execution_block_hash: frames[1].block.hash,
            beacon_slot: 42,
            beacon_block_root: [0x33; 32],
        };

        for frame in &mut frames {
            mark_anchored_history_finalized(frame, &anchor);
        }

        assert_eq!(frames[0].finality, Finality::Finalized);
        assert!(frames[0].verification.consensus_anchor.is_none());
        assert!(
            frames[0]
                .verification
                .parent_continuity
                .detail
                .as_ref()
                .is_some_and(|detail| detail.contains(&anchor.execution_block_hash.to_string()))
        );
        assert_eq!(
            frames[1].verification.consensus_anchor.as_ref(),
            Some(&anchor)
        );
        assert!(frames.iter().all(|frame| frame.validate_shape().is_ok()));
    }

    #[test]
    fn rejects_truncated_and_out_of_order_headers() {
        let (range, mut headers, _, _) = empty_fixture();
        assert!(validate_headers(range, &headers[..1], None).is_err());
        headers.swap(0, 1);
        assert!(validate_headers(range, &headers, None).is_err());
    }

    #[test]
    fn polling_the_next_height_treats_empty_as_not_yet_available() {
        let expected = BlockNumber(42);
        assert!(
            classify_polled_header(expected, Vec::new())
                .expect("empty is not an invalid response")
                .is_none()
        );
        let header = Header {
            number: expected.0,
            ..Default::default()
        };
        assert_eq!(
            classify_polled_header(expected, vec![header.clone()])
                .expect("one matching header")
                .expect("header")
                .number,
            expected.0
        );
        assert!(classify_polled_header(expected, vec![header.clone(), header]).is_err());
        assert!(
            classify_polled_header(
                expected,
                vec![Header {
                    number: expected.0.saturating_add(1),
                    ..Default::default()
                }]
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_wrong_tip_and_parent() {
        let (range, mut headers, _, _) = empty_fixture();
        assert!(validate_headers(range, &headers, Some(BlockHash::ZERO)).is_err());
        headers[1].parent_hash = B256::ZERO;
        assert!(validate_headers(range, &headers, None).is_err());
    }

    #[test]
    fn discovers_a_bounded_common_ancestor_and_tip_ordered_reverts() {
        let ancestor = Header {
            number: 11,
            timestamp: 110,
            ..Default::default()
        };
        let old_twelve = Header {
            number: 12,
            parent_hash: ancestor.hash_slow(),
            timestamp: 120,
            ..Default::default()
        };
        let old_thirteen = Header {
            number: 13,
            parent_hash: old_twelve.hash_slow(),
            timestamp: 130,
            ..Default::default()
        };
        let new_twelve = Header {
            number: 12,
            parent_hash: ancestor.hash_slow(),
            timestamp: 121,
            ..Default::default()
        };
        let new_thirteen = Header {
            number: 13,
            parent_hash: new_twelve.hash_slow(),
            timestamp: 131,
            ..Default::default()
        };
        let known = VecDeque::from([
            block_ref(&ancestor),
            block_ref(&old_twelve),
            block_ref(&old_thirteen),
        ]);
        let replacement = vec![new_thirteen.clone(), new_twelve.clone(), ancestor.clone()];
        validate_descending_headers(
            BlockNumber(13),
            block_hash(new_thirteen.hash_slow()),
            &replacement,
        )
        .expect("descending branch");
        let (common, reverted) = plan_reorg(&known, &replacement).expect("reorg plan");
        assert_eq!(common, block_ref(&ancestor));
        assert_eq!(
            reverted,
            vec![block_ref(&old_thirteen), block_ref(&old_twelve)]
        );
        assert!(plan_reorg(&VecDeque::from([block_ref(&old_thirteen)]), &replacement).is_err());
    }

    #[test]
    fn rejects_body_and_receipt_root_corruption() {
        let (_, mut headers, bodies, _) = empty_fixture();
        headers[0].transactions_root = B256::ZERO;
        assert!(validate_bodies(&headers, &bodies).is_err());

        let (_, mut headers, bodies, mut receipts) = empty_fixture();
        receipts[0].push(EthereumReceipt {
            tx_type: TxType::Legacy,
            success: true,
            cumulative_gas_used: 0,
            logs: Vec::new(),
        });
        assert!(validate_receipts(&headers, &bodies, &receipts).is_err());
        headers[0].receipts_root = B256::ZERO;
        assert!(validate_receipts_against_headers(&headers, &[vec![], vec![]]).is_err());
        assert!(validate_receipts(&headers, &bodies, &[vec![], vec![]]).is_err());
    }

    #[test]
    fn request_metrics_separate_payload_bytes_from_normalized_material() {
        let metrics = P2pRequestMetrics::default();
        metrics.add_response_payload_bytes(P2pRequestKind::Bodies, 1_024);
        metrics.record(
            P2pRequestKind::Bodies,
            4,
            4,
            Duration::from_millis(12),
            P2pRequestOutcome::Succeeded,
        );
        assert_eq!(metrics.snapshot().bodies.response_payload_bytes, 1_024);
    }

    #[test]
    fn parses_exact_execution_hashes() {
        let value = format!("0x{}", "12".repeat(32));
        assert_eq!(
            parse_block_hash(&value).expect("hash"),
            BlockHash::new([0x12; 32])
        );
        assert!(parse_block_hash(value.trim_start_matches("0x")).is_err());
        assert!(parse_block_hash("0x12").is_err());
    }

    #[test]
    fn parses_operator_nat_configuration() {
        assert!(parse_nat_resolver("none").expect("disabled NAT").is_none());
        assert!(parse_nat_resolver("any").expect("automatic NAT").is_some());
        assert!(parse_nat_resolver("not-a-resolver").is_err());
    }

    #[test]
    fn rejects_a_zero_fresh_session_retry_budget() {
        let config = RethP2pConfig {
            session_retries: 0,
            ..RethP2pConfig::default()
        };
        assert!(matches!(
            RethP2pSource::mainnet(config),
            Err(P2pError::InvalidConfig(_))
        ));
        let config = RethP2pConfig {
            material_request_blocks: 17,
            ..RethP2pConfig::default()
        };
        assert!(matches!(
            RethP2pSource::mainnet(config),
            Err(P2pError::InvalidConfig(_))
        ));
    }

    #[test]
    fn peer_targets_separate_the_hard_floor_from_the_soft_preference() {
        let defaults = RethP2pConfig::default();
        assert_eq!(defaults.minimum_peers, 1);
        assert_eq!(defaults.preferred_peers, 16);
        assert_eq!(defaults.max_outbound_peers, 100);
        assert_eq!(defaults.max_concurrent_dials, 30);
        assert_eq!(defaults.peer_refill_interval, Duration::from_secs(5));
        assert_eq!(defaults.peer_recovery_timeout, Duration::from_mins(5));

        for preferred_peers in [0, 101] {
            let config = RethP2pConfig {
                preferred_peers,
                ..RethP2pConfig::default()
            };
            assert!(matches!(
                RethP2pSource::mainnet(config),
                Err(P2pError::InvalidConfig(_))
            ));
        }
    }

    #[test]
    fn dns_txt_segments_are_joined_before_eip_1459_verification() {
        let segments: [&[u8]; 2] = [
            b"enrtree-branch:7KRNP5AGA4KNGPB3UHWPWRWELI,2BMECEA4AEIVMZBPZFTH6U",
            b"J6R4,ULZUPPIRAKFABTKQYTYIPGLLZQ",
        ];

        assert_eq!(
            join_dns_txt_segments(segments),
            Some(
                "enrtree-branch:7KRNP5AGA4KNGPB3UHWPWRWELI,2BMECEA4AEIVMZBPZFTH6UJ6R4,ULZUPPIRAKFABTKQYTYIPGLLZQ"
                    .to_owned()
            )
        );
    }

    #[test]
    fn event_driven_dials_try_fresh_peers_before_cooled_retries() {
        let first = node_record(1);
        let second = node_record(2);
        let mut queue = EventDrivenDialQueue::new(Duration::from_secs(2), Duration::from_secs(10));
        assert!(queue.add(first));
        assert!(queue.add(second));
        assert!(!queue.add(first));
        assert_eq!(queue.next_fresh().map(|record| record.id), Some(first.id));
        let now = tokio::time::Instant::now();
        queue.pending.insert(
            first.id,
            PendingDial {
                record: first,
                expires_at: now + Duration::from_secs(2),
            },
        );
        assert_eq!(queue.next_fresh().map(|record| record.id), Some(second.id));
        queue.expire_pending(now + Duration::from_secs(1));
        assert!(queue.next_cooled(now + Duration::from_secs(20)).is_none());
        queue.expire_pending(now + Duration::from_secs(2));
        assert!(queue.next_cooled(now + Duration::from_secs(11)).is_none());
        assert_eq!(
            queue
                .next_cooled(now + Duration::from_secs(12))
                .map(|record| record.id),
            Some(first.id)
        );
    }

    #[test]
    fn authenticated_bootstrap_tree_configuration_is_strict() {
        assert!(validate_bootstrap_dns_tree(MAINNET_DNS_DISCOVERY_TREE).is_ok());
        assert!(validate_bootstrap_dns_tree("https://example.com/peers.json").is_err());
        assert!(validate_bootstrap_dns_tree("enrtree://missing-key.example.com").is_err());
    }

    #[test]
    fn peer_quality_merge_preserves_verified_material_evidence() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let destination = directory.path().join("execution-peer-quality.json");
        let sibling = directory.path().join("sibling-peer-quality.json");
        let destination_store = PeerQualityStore {
            path: Some(destination.clone()),
            document: Mutex::new(PeerQualityDocument::default()),
            dirty: AtomicBool::new(false),
        };
        let sibling_store = PeerQualityStore {
            path: Some(sibling.clone()),
            document: Mutex::new(PeerQualityDocument::default()),
            dirty: AtomicBool::new(false),
        };
        let body_peer = node_record(3).id;
        let receipt_peer = node_record(4).id;
        destination_store.record_success(
            body_peer,
            PeerMaterialKind::Body,
            25_000_000,
            Duration::from_millis(30),
        );
        sibling_store.record_success(
            receipt_peer,
            PeerMaterialKind::Receipts,
            25_000_100,
            Duration::from_millis(20),
        );
        destination_store.persist(10).expect("persist destination");
        sibling_store.persist(10).expect("persist sibling");

        let merged = merge_peer_quality_caches(&destination, std::slice::from_ref(&sibling), 10)
            .expect("merge quality caches")
            .expect("quality evidence exists");
        assert_eq!(
            merged,
            PeerQualityCacheMerge {
                total: 2,
                imported: 1
            }
        );
        let document = read_peer_quality_document(&destination).expect("merged document");
        assert!(
            document.peers[&peer_quality_key(body_peer)]
                .last_body_success_unix_ms
                .is_some()
        );
        assert!(
            document.peers[&peer_quality_key(receipt_peer)]
                .last_receipt_success_unix_ms
                .is_some()
        );
    }

    #[tokio::test]
    async fn verified_anchor_body_qualification_requires_both_material_responses() {
        let body = BlockBody::default();
        let header = Header {
            number: 25_000_000,
            timestamp: 1_788_000_000,
            transactions_root: calculate_transaction_root(&body.transactions),
            ommers_hash: EMPTY_OMMER_ROOT_HASH,
            ..Header::default()
        };
        let hash = header.hash_slow();
        let target = BlockRef {
            number: BlockNumber(header.number),
            hash: BlockHash::new(hash.0),
            parent_hash: BlockHash::new(header.parent_hash.0),
            timestamp: header.timestamp,
        };
        let peer_id = B512::from([0x42; 64]);
        let (sender, mut receiver) =
            tokio::sync::mpsc::channel::<PeerRequest<EthNetworkPrimitives>>(2);
        let peer = DirectPeer {
            peer_id,
            eth_version: EthVersion::Eth68,
            messages: PeerRequestSender::new(peer_id, sender),
            advertised_head: Some(header.number),
        };
        let responder = tokio::spawn(async move {
            let PeerRequest::GetBlockHeaders { response, .. } =
                receiver.recv().await.expect("header request")
            else {
                panic!("expected header request");
            };
            response
                .send(Ok(vec![header].into()))
                .expect("header response");
            let PeerRequest::GetBlockBodies { response, .. } =
                receiver.recv().await.expect("body request")
            else {
                panic!("expected body request");
            };
            response.send(Ok(vec![body].into())).expect("body response");
        });
        let result = qualify_execution_peer(
            peer,
            target,
            Duration::from_secs(1),
            Arc::new(MaterialRequestGate::default()),
            1,
            Priority::High,
            CancellationToken::new(),
        )
        .await;
        assert_eq!(result.outcome, PeerQualification::BodyServing);
        responder.await.expect("qualification responder");
    }

    #[test]
    fn mainnet_dns_bootstrap_uses_bounded_accelerated_rate() {
        let config = mainnet_dns_discovery_config();
        assert_eq!(config.max_requests_per_sec.get(), 50);
        assert_eq!(config.recheck_interval, Duration::from_mins(5));
        assert_eq!(MAINNET_DNS_RETRY_INTERVAL, Duration::from_secs(15));
    }

    #[tokio::test]
    #[ignore = "requires live mainnet DNS"]
    async fn live_mainnet_dns_discovery_emits_execution_records() {
        let resolver = SegmentJoiningDnsResolver::from_system_conf().expect("system DNS resolver");
        let (service, control) =
            DnsDiscoveryService::new_pair(Arc::new(resolver), mainnet_dns_discovery_config());
        let service_task = service.spawn();
        let mut records = control.node_record_stream().await.expect("record stream");
        control
            .sync_tree(MAINNET_DNS_DISCOVERY_TREE)
            .expect("mainnet DNS tree");

        let record = tokio::time::timeout(Duration::from_secs(20), records.next())
            .await
            .expect("mainnet DNS discovery timed out")
            .expect("mainnet DNS record stream closed");
        assert!(record.node_record.tcp_port > 0);
        assert!(record.fork_id.is_some());
        service_task.abort();
    }

    #[test]
    fn zero_peer_watchdog_recovers_and_resets_after_connectivity() {
        let started = tokio::time::Instant::now();
        let timeout = Duration::from_mins(1);
        let mut zero_peers_since = Some(started);

        assert!(!peer_recovery_due(
            0,
            &mut zero_peers_since,
            started + Duration::from_secs(59),
            timeout
        ));
        assert!(peer_recovery_due(
            0,
            &mut zero_peers_since,
            started + timeout,
            timeout
        ));
        assert!(!peer_recovery_due(
            1,
            &mut zero_peers_since,
            started + timeout,
            timeout
        ));
        assert_eq!(zero_peers_since, None);
        assert!(!peer_recovery_due(
            0,
            &mut zero_peers_since,
            started + timeout + Duration::from_secs(1),
            timeout
        ));
    }

    #[test]
    fn fixed_discv4_and_discv5_ports_must_differ() {
        let colliding = RethP2pConfig {
            discovery_port: 30_303,
            discv5_port: 30_303,
            ..RethP2pConfig::default()
        };
        assert!(matches!(
            RethP2pSource::mainnet(colliding),
            Err(P2pError::InvalidConfig(_))
        ));

        let distinct = RethP2pConfig {
            listener_port: 30_303,
            discovery_port: 30_303,
            discv5_port: 30_304,
            ..RethP2pConfig::default()
        };
        assert!(RethP2pSource::mainnet(distinct).is_ok());
    }

    #[test]
    fn disconnect_reasons_have_stable_identity_free_labels() {
        assert_eq!(
            classify_disconnect_reason(Some(DisconnectReason::TooManyPeers)),
            NetworkDisconnectReason::TooManyPeers
        );
        assert_eq!(
            classify_disconnect_reason(Some(DisconnectReason::PingTimeout)),
            NetworkDisconnectReason::PingTimeout
        );
        assert_eq!(
            classify_disconnect_reason(None),
            NetworkDisconnectReason::ConnectionClosed
        );
    }

    #[tokio::test]
    async fn reth_request_timeouts_are_classified_as_timeouts() {
        let cancellation = CancellationToken::new();
        let timeout = cancellable_peer_request(
            async { Err::<(), _>(RequestError::Timeout) },
            Duration::from_secs(1),
            &cancellation,
            "receipts",
        )
        .await
        .expect_err("timeout");
        assert!(matches!(
            timeout,
            P2pError::Timeout {
                component: "receipts"
            }
        ));

        let failure = cancellable_peer_request(
            async { Err::<(), _>(RequestError::ChannelClosed) },
            Duration::from_secs(1),
            &cancellation,
            "receipts",
        )
        .await
        .expect_err("request failure");
        assert!(matches!(
            failure,
            P2pError::Request {
                component: "receipts",
                ..
            }
        ));
    }

    #[test]
    fn persistent_recovery_backoff_is_exponential_and_capped() {
        let config = RethP2pConfig {
            retry_backoff: Duration::from_secs(2),
            retry_backoff_max: Duration::from_secs(10),
            ..RethP2pConfig::default()
        };
        assert_eq!(session_retry_delay(&config, 1), Duration::from_secs(2));
        assert_eq!(session_retry_delay(&config, 2), Duration::from_secs(4));
        assert_eq!(session_retry_delay(&config, 3), Duration::from_secs(8));
        assert_eq!(session_retry_delay(&config, 4), Duration::from_secs(10));
        assert!(should_retry_session(&config, usize::MAX));

        let bounded = RethP2pConfig {
            persistent_retries: false,
            session_retries: 3,
            ..config.clone()
        };
        assert!(should_retry_session(&bounded, 2));
        assert!(!should_retry_session(&bounded, 3));

        assert!(should_retry_session_error(
            &config,
            usize::MAX,
            &P2pError::Timeout {
                component: "receipts"
            }
        ));
        assert!(!should_retry_session_error(
            &config,
            1,
            &P2pError::InvalidConfig("invalid".to_owned())
        ));
        assert!(!should_retry_session_error(
            &config,
            1,
            &P2pError::Source(SourceError::BudgetExceeded {
                resource: "input_bytes",
                limit: 1,
                observed: 2,
            })
        ));
    }

    #[test]
    fn anchored_history_ranges_cover_ten_thousand_blocks_in_protocol_sized_batches() {
        let proof = BlockRange::new(BlockNumber(10_000), BlockNumber(19_999)).expect("proof range");
        let ranges = anchored_header_ranges(proof, MAX_HISTORY_HEADER_REQUEST_BLOCKS);
        assert_eq!(ranges.len(), 10);
        assert_eq!(ranges.first().expect("first").start(), proof.start());
        assert_eq!(ranges.last().expect("last").end(), proof.end());
        assert!(
            ranges
                .iter()
                .all(|range| range.len() <= MAX_HISTORY_HEADER_REQUEST_BLOCKS)
        );
        assert!(
            ranges
                .windows(2)
                .all(|pair| { pair[0].end().0.saturating_add(1) == pair[1].start().0 })
        );
    }

    #[test]
    fn anchored_header_proof_resolves_hashes_without_retaining_headers() {
        let cached_range =
            BlockRange::new(BlockNumber(100), BlockNumber(110)).expect("cached range");
        let retained = BlockRange::new(BlockNumber(100), BlockNumber(105)).expect("retained range");
        let proof = AnchoredHeaderProof {
            proof: cached_range,
            retained,
            hashes: (100_u8..=105)
                .map(|byte| BlockHash::new([byte; 32]))
                .collect(),
        };

        assert_eq!(
            proof.expected_hash(BlockNumber(104)),
            Some(BlockHash::new([104; 32]))
        );
        assert!(proof.expected_hash(BlockNumber(99)).is_none());
        assert!(proof.expected_hash(BlockNumber(106)).is_none());
        assert!(proof.covers(
            BlockRange::new(BlockNumber(104), BlockNumber(110)).expect("covered proof suffix"),
            BlockRange::new(BlockNumber(104), BlockNumber(105)).expect("covered retained suffix"),
        ));
        assert!(!proof.covers(
            BlockRange::new(BlockNumber(104), BlockNumber(109)).expect("wrong anchor"),
            BlockRange::new(BlockNumber(104), BlockNumber(105)).expect("retained suffix"),
        ));
    }

    #[test]
    fn compact_header_proof_checks_cross_batch_links_and_anchor() {
        let mut headers = Vec::new();
        let mut parent = B256::ZERO;
        for number in 100..=2_199 {
            let header = Header {
                number,
                parent_hash: parent,
                ..Default::default()
            };
            parent = header.hash_slow();
            headers.push(header);
        }
        let proof_range =
            BlockRange::new(BlockNumber(100), BlockNumber(2_199)).expect("proof range");
        let first_range =
            BlockRange::new(BlockNumber(100), BlockNumber(1_123)).expect("first range");
        let second_range =
            BlockRange::new(BlockNumber(1_124), BlockNumber(2_147)).expect("second range");
        let third_range =
            BlockRange::new(BlockNumber(2_148), BlockNumber(2_199)).expect("third range");
        let segments = vec![
            header_proof_segment(third_range, &headers[2_048..]).expect("third segment"),
            header_proof_segment(first_range, &headers[..1_024]).expect("first segment"),
            header_proof_segment(second_range, &headers[1_024..2_048]).expect("second segment"),
        ];
        let anchor = block_hash(headers.last().expect("tip").hash_slow());
        let proof =
            assemble_anchored_header_proof(proof_range, anchor, segments).expect("assembled proof");
        assert_eq!(proof.hashes.len(), 2_100);
        assert_eq!(proof.expected_hash(BlockNumber(2_199)), Some(anchor));

        let mut broken =
            header_proof_segment(second_range, &headers[1_024..2_048]).expect("second segment");
        broken.first_parent = BlockHash::ZERO;
        let invalid = vec![
            header_proof_segment(first_range, &headers[..1_024]).expect("first segment"),
            broken,
            header_proof_segment(third_range, &headers[2_048..]).expect("third segment"),
        ];
        assert!(assemble_anchored_header_proof(proof_range, anchor, invalid).is_err());
    }

    #[test]
    fn header_proof_builder_retains_successful_ranges_across_retry() {
        let proof = BlockRange::new(BlockNumber(100), BlockNumber(2_199)).expect("proof range");
        let retained = BlockRange::new(BlockNumber(100), BlockNumber(199)).expect("retained");
        let mut builder = AnchoredHeaderProofBuilder::new(proof, retained, 1_024);
        let first_wave = builder.take_wave(2);
        assert_eq!(first_wave.len(), 2);
        let first = first_wave[0];
        builder
            .record(
                first,
                Ok(HeaderProofSegment {
                    range: first,
                    first_parent: BlockHash::ZERO,
                    hashes: vec![BlockHash::new([1; 32]); 1_024],
                }),
            )
            .expect("record first proof segment");
        builder
            .record(
                first_wave[1],
                Err(P2pError::Request {
                    component: "headers",
                    detail: "temporary peer failure".to_owned(),
                }),
            )
            .expect("retain failed proof range");

        assert!(builder.segments.is_empty());
        assert_eq!(
            builder.retained_hashes.len(),
            usize::try_from(retained.len()).expect("retained range fits in memory")
        );
        assert_eq!(builder.pending.len(), 2);
        assert_eq!(builder.pending.back().copied(), Some(first_wave[1]));
    }

    #[test]
    fn peer_cache_compaction_prefers_serving_reputation_then_fork_metadata() {
        let entries = parse_peer_cache_entries(
            br#"[{"record":"enode://stale","kind":"basic","reputation":-25600},
                  {"record":"enode://plain","kind":"basic","reputation":50},
                  {"record":"enode://fork","kind":"basic","reputation":0,"fork_id":{"hash":"x"}},
                  {"record":"enode://plain","kind":"basic","reputation":100}]"#,
        )
        .expect("peer entries");

        let quality = PeerQualityStore::load(None);
        let compacted = compact_peer_cache_entries(entries, 2, &quality);
        assert_eq!(compacted.len(), 2);
        assert_eq!(compacted[0]["record"], "enode://plain");
        assert_eq!(compacted[0]["reputation"], 100);
        assert_eq!(compacted[1]["record"], "enode://fork");
        assert!(
            compacted
                .iter()
                .all(|entry| entry["record"] != "enode://stale")
        );
    }

    #[test]
    fn persistent_execution_identity_is_reused() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("execution-p2p-secret");
        let first = load_or_create_secret_key(Some(&path)).expect("create identity");
        let second = load_or_create_secret_key(Some(&path)).expect("reload identity");
        assert_eq!(first.secret_bytes(), second.secret_bytes());
        assert_eq!(
            std::fs::read_to_string(path).expect("identity file").len(),
            64
        );
    }

    #[test]
    fn material_concurrency_uses_a_bounded_per_peer_pipeline() {
        assert_eq!(effective_material_concurrency(1, 8, 8), 4);
        assert_eq!(effective_material_concurrency(2, 8, 8), 8);
        assert_eq!(effective_material_concurrency(3, 8, 4), 4);
        assert_eq!(effective_material_concurrency(8, 8, 4), 4);
        assert_eq!(effective_material_concurrency(0, 8, 8), 4);
        assert_eq!(direct_peer_request_limit(8, 1), 4);
        assert_eq!(direct_peer_request_limit(8, 2), 4);
        assert_eq!(direct_peer_request_limit(8, 4), 2);
        assert_eq!(direct_peer_request_limit(4, 8), 1);
    }

    #[test]
    fn repeated_incomplete_material_rotates_at_the_configured_retry_limit() {
        let peer = B512::from([0x11; 64]);
        let mut responses = HashMap::new();
        assert!(!repeated_incomplete_response(&mut responses, peer, 3));
        assert!(!repeated_incomplete_response(&mut responses, peer, 3));
        assert!(repeated_incomplete_response(&mut responses, peer, 3));
    }

    #[tokio::test]
    async fn empty_direct_peer_pool_has_a_distinct_availability_timeout() {
        let pool = direct_peer_pool();
        let error = pool
            .acquire(4, Duration::from_millis(10), &CancellationToken::new())
            .await
            .expect_err("an empty direct-peer pool must not wait forever");
        assert!(matches!(
            error,
            P2pError::Timeout {
                component: "direct peer availability"
            }
        ));
    }

    #[tokio::test]
    async fn direct_peer_wave_selects_each_connected_peer_at_most_once() {
        let pool = direct_peer_pool();
        let mut receivers = Vec::new();
        for marker in 1_u8..=3 {
            let peer_id = B512::from([marker; 64]);
            let (sender, receiver) = tokio::sync::mpsc::channel(1);
            receivers.push(receiver);
            pool.insert(DirectPeer {
                peer_id,
                eth_version: EthVersion::Eth68,
                messages: PeerRequestSender::new(peer_id, sender),
                advertised_head: None,
            });
            pool.set_qualified(peer_id, true);
        }

        let cancellation = CancellationToken::new();
        let preferred_peer = B512::from([3_u8; 64]);
        let preferred = pool
            .acquire_excluding(
                1,
                Duration::from_secs(1),
                &HashSet::new(),
                Some(preferred_peer),
                &cancellation,
            )
            .await
            .expect("preferred peer acquisition")
            .expect("preferred peer is connected");
        assert_eq!(preferred.peer.peer_id, preferred_peer);
        drop(preferred);

        let mut tried = HashSet::new();
        let first = pool
            .acquire_excluding(1, Duration::from_secs(1), &tried, None, &cancellation)
            .await
            .expect("first peer acquisition")
            .expect("an untried peer remains");
        assert!(tried.insert(first.peer.peer_id));
        let mut wave = vec![first];
        while let Some(lease) = pool.try_acquire_excluding(1, &tried, None) {
            assert!(tried.insert(lease.peer.peer_id));
            wave.push(lease);
        }
        assert_eq!(wave.len(), 3, "all connected peers enter the same wave");
        assert!(
            pool.acquire_excluding(1, Duration::from_secs(1), &tried, None, &cancellation)
                .await
                .expect("exhausted wave is not an error")
                .is_none()
        );
        assert_eq!(tried.len(), 3);
        drop(wave);
        assert!(
            pool.peers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .all(|peer| peer.failures == 0),
            "a neutral lease must not cool or penalize a peer"
        );
        assert!(
            pool.acquire_excluding(
                1,
                Duration::from_secs(1),
                &HashSet::new(),
                None,
                &cancellation,
            )
            .await
            .expect("new wave acquisition")
            .is_some(),
            "a new wave may retry peers after the wave cooldown"
        );
        drop(receivers);
    }

    #[tokio::test]
    async fn local_cooldown_prioritizes_a_new_unasked_peer() {
        let pool = direct_peer_pool();
        let mut receivers = Vec::new();
        for marker in 1_u8..=2 {
            let peer_id = B512::from([marker; 64]);
            let (sender, receiver) = tokio::sync::mpsc::channel(1);
            receivers.push(receiver);
            pool.insert(DirectPeer {
                peer_id,
                eth_version: EthVersion::Eth68,
                messages: PeerRequestSender::new(peer_id, sender),
                advertised_head: None,
            });
            pool.set_qualified(peer_id, true);
        }

        let cancellation = CancellationToken::new();
        for marker in 1_u8..=2 {
            let mut lease = pool
                .acquire_excluding(
                    1,
                    Duration::from_secs(1),
                    &HashSet::new(),
                    Some(B512::from([marker; 64])),
                    &cancellation,
                )
                .await
                .expect("peer acquisition")
                .expect("preferred peer is connected");
            assert_eq!(lease.peer.peer_id, B512::from([marker; 64]));
            lease.failed();
        }

        let fresh_peer = B512::from([3_u8; 64]);
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        receivers.push(receiver);
        pool.insert(DirectPeer {
            peer_id: fresh_peer,
            eth_version: EthVersion::Eth68,
            messages: PeerRequestSender::new(fresh_peer, sender),
            advertised_head: None,
        });
        pool.set_qualified(fresh_peer, true);
        let lease = pool
            .acquire_excluding(
                1,
                Duration::from_secs(1),
                &HashSet::new(),
                None,
                &cancellation,
            )
            .await
            .expect("peer acquisition")
            .expect("fresh peer is connected");
        assert_eq!(lease.peer.peer_id, fresh_peer);
        drop(lease);
        drop(receivers);
    }

    #[tokio::test]
    async fn direct_body_request_uses_the_selected_peer_session() {
        let peer_id = B512::from([0x44; 64]);
        let (sender, mut receiver) =
            tokio::sync::mpsc::channel::<PeerRequest<EthNetworkPrimitives>>(1);
        let peer = DirectPeer {
            peer_id,
            eth_version: EthVersion::Eth68,
            messages: PeerRequestSender::new(peer_id, sender),
            advertised_head: None,
        };
        let hash = B256::from([0x55; 32]);
        let responder = tokio::spawn(async move {
            let request = receiver.recv().await.expect("direct body request");
            let PeerRequest::GetBlockBodies { request, response } = request else {
                panic!("expected a body request");
            };
            assert_eq!(request.0, [hash]);
            response
                .send(Ok(vec![BlockBody::default()].into()))
                .expect("send body response");
        });

        let (bodies, _) = request_direct_bodies(
            &peer,
            &[hash],
            Duration::from_secs(1),
            &CancellationToken::new(),
        )
        .await
        .expect("direct body response");
        assert_eq!(bodies, [BlockBody::default()]);
        responder.await.expect("body responder");
    }

    #[tokio::test]
    async fn direct_head_request_uses_the_advertising_peer_session() {
        let peer_id = B512::from([0x66; 64]);
        let (sender, mut receiver) =
            tokio::sync::mpsc::channel::<PeerRequest<EthNetworkPrimitives>>(1);
        let peer = DirectPeer {
            peer_id,
            eth_version: EthVersion::Eth68,
            messages: PeerRequestSender::new(peer_id, sender),
            advertised_head: None,
        };
        let hash = B256::from([0x77; 32]);
        let responder = tokio::spawn(async move {
            let request = receiver.recv().await.expect("direct header request");
            let PeerRequest::GetBlockHeaders { request, response } = request else {
                panic!("expected a header request");
            };
            assert_eq!(request.start_block, BlockHashOrNumber::Hash(hash));
            assert_eq!(request.limit, 1);
            assert_eq!(request.skip, 0);
            assert_eq!(request.direction, HeadersDirection::Rising);
            response
                .send(Ok(vec![Header::default()].into()))
                .expect("send header response");
        });

        let headers = request_direct_header(
            &peer,
            hash,
            Duration::from_secs(1),
            &CancellationToken::new(),
        )
        .await
        .expect("direct header response");
        assert_eq!(headers, [Header::default()]);
        responder.await.expect("header responder");
    }

    #[tokio::test]
    async fn live_head_requests_take_the_next_available_material_slot() {
        let gate = Arc::new(MaterialRequestGate::default());
        let cancellation = CancellationToken::new();
        let occupied = gate
            .acquire(1, Priority::Normal, &cancellation)
            .await
            .expect("initial slot");
        let (order, mut observed) = tokio::sync::mpsc::unbounded_channel();

        let normal_gate = gate.clone();
        let normal_cancellation = cancellation.clone();
        let normal_order = order.clone();
        let normal = tokio::spawn(async move {
            let _permit = normal_gate
                .acquire(1, Priority::Normal, &normal_cancellation)
                .await
                .expect("normal slot");
            normal_order.send("normal").expect("record normal");
        });
        tokio::task::yield_now().await;

        let high_gate = gate.clone();
        let high_cancellation = cancellation.clone();
        let high = tokio::spawn(async move {
            let _permit = high_gate
                .acquire(1, Priority::High, &high_cancellation)
                .await
                .expect("high-priority slot");
            order.send("high").expect("record high priority");
        });
        while gate.high_priority_waiters.load(Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }

        drop(occupied);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), observed.recv())
                .await
                .expect("request completes")
                .expect("request recorded"),
            "high"
        );
        high.await.expect("high task");
        normal.await.expect("normal task");
    }

    #[test]
    fn retained_live_suffix_requires_parent_continuity() {
        let parent = BlockRef {
            number: BlockNumber(10),
            hash: BlockHash::new([0x10; 32]),
            parent_hash: BlockHash::ZERO,
            timestamp: 10,
        };
        let child = BlockRef {
            number: BlockNumber(11),
            hash: BlockHash::new([0x11; 32]),
            parent_hash: parent.hash,
            timestamp: 11,
        };
        assert!(validate_retained_canonical(&[parent, child], 64).is_ok());
        let invalid = BlockRef {
            parent_hash: BlockHash::ZERO,
            ..child
        };
        assert!(validate_retained_canonical(&[parent, invalid], 64).is_err());
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn finalized_history_bridge_plans_budget_bounded_anchor_streams() {
        use leani_source_api::{FieldProjection, FilterSet, VerificationPolicy};

        let available =
            BlockRange::new(BlockNumber(100), BlockNumber(1_200)).expect("available range");
        let anchor_hash = BlockHash::new([0x22; 32]);
        let live_source = RethP2pSource::mainnet(RethP2pConfig {
            minimum_peers: 1,
            ..RethP2pConfig::default()
        })
        .expect("live source");
        let source = RethP2pHistorySource::from_live_source(
            live_source.clone(),
            available,
            P2pHistoryAnchor {
                block: BlockRef {
                    number: available.end(),
                    hash: anchor_hash,
                    parent_hash: BlockHash::ZERO,
                    timestamp: 1,
                },
                consensus: ConsensusAnchor {
                    finality: Finality::Finalized,
                    execution_block_hash: anchor_hash,
                    beacon_slot: 1,
                    beacon_block_root: [0x33; 32],
                },
            },
        )
        .expect("history source");
        assert_eq!(source.source.config.minimum_peers, 1);
        assert!(source.prefer_shared_live_session);
        assert!(Arc::ptr_eq(&source.source.network, &live_source.network));
        let independent = RethP2pHistorySource::mainnet(
            RethP2pConfig {
                minimum_peers: 1,
                ..RethP2pConfig::default()
            },
            available,
            P2pHistoryAnchor {
                block: BlockRef {
                    number: available.end(),
                    hash: anchor_hash,
                    parent_hash: BlockHash::ZERO,
                    timestamp: 1,
                },
                consensus: ConsensusAnchor {
                    finality: Finality::Finalized,
                    execution_block_hash: anchor_hash,
                    beacon_slot: 1,
                    beacon_block_root: [0x33; 32],
                },
            },
        )
        .expect("independent history source");
        assert!(!independent.prefer_shared_live_session);
        let requested =
            BlockRange::new(BlockNumber(150), BlockNumber(1_180)).expect("requested range");
        let request = DataRequest {
            chain_id: ChainId(1),
            range: requested,
            required: CapabilitySet::of(Capability::Header)
                .with(Capability::Transactions)
                .with(Capability::Receipts),
            allow_filtered: false,
            projection: FieldProjection::default(),
            log_fields: leani_primitives::LogFieldSet::NONE,
            filters: FilterSet::default(),
            minimum_finality: Finality::Finalized,
            verification_policy: VerificationPolicy::CompleteCryptographic,
        };
        let plan = source.plan(&request).await.expect("plan");
        plan.validate().expect("valid plan");
        assert_eq!(plan.chunks.len(), 5);
        assert_eq!(plan.chunks[0].range.start(), requested.start());
        assert_eq!(plan.chunks[0].range.len(), MAX_HISTORY_OPEN_BLOCKS);
        assert_eq!(
            plan.chunks.last().expect("last chunk").range.end(),
            requested.end()
        );
        assert!(
            plan.chunks
                .windows(2)
                .all(|pair| { pair[0].range.end().0.saturating_add(1) == pair[1].range.start().0 })
        );
        assert!(
            plan.chunks
                .iter()
                .all(|chunk| chunk.range.len() <= MAX_HISTORY_OPEN_BLOCKS)
        );
        assert_eq!(plan.trust, TrustModel::ProtocolVerified);
        let first_chunk = &plan.chunks[0];
        let sliced_range = BlockRange::new(
            BlockNumber(first_chunk.range.start().0.saturating_add(1)),
            BlockNumber(first_chunk.range.end().0.saturating_sub(1)),
        )
        .expect("sliced P2P range");
        let sliced = source
            .slice_chunk(first_chunk, sliced_range)
            .expect("slice P2P chunk");
        assert_eq!(sliced.range, sliced_range);
        assert_eq!(
            decode_history_partition(&sliced.partition)
                .expect("sliced partition")
                .range,
            sliced_range
        );
        assert_eq!(
            decode_history_partition(&sliced.partition)
                .expect("sliced partition")
                .proof_start,
            requested.start()
        );
        assert_eq!(
            decode_history_partition(&sliced.partition)
                .expect("sliced partition")
                .material_end,
            requested.end()
        );
        assert!(source.coalescing_partition_identity(first_chunk).is_empty());
        let narrower = RethP2pHistorySource::from_live_source(
            live_source,
            BlockRange::new(BlockNumber(120), available.end()).expect("narrower range"),
            P2pHistoryAnchor {
                block: BlockRef {
                    number: available.end(),
                    hash: anchor_hash,
                    parent_hash: BlockHash::ZERO,
                    timestamp: 1,
                },
                consensus: ConsensusAnchor {
                    finality: Finality::Finalized,
                    execution_block_hash: anchor_hash,
                    beacon_slot: 1,
                    beacon_block_root: [0x33; 32],
                },
            },
        )
        .expect("narrower history source");
        assert_eq!(
            source.acquisition_identity(),
            narrower.acquisition_identity()
        );

        let missing = DataRequest {
            range: BlockRange::new(BlockNumber(99), BlockNumber(1_180)).expect("missing"),
            ..request
        };
        assert!(matches!(
            source.plan(&missing).await,
            Err(SourceError::MissingRange(_))
        ));
    }

    fn block_ref(header: &Header) -> BlockRef {
        BlockRef {
            number: BlockNumber(header.number),
            hash: block_hash(header.hash_slow()),
            parent_hash: block_hash(header.parent_hash),
            timestamp: header.timestamp,
        }
    }

    fn live_request(range: BlockRange) -> DataRequest {
        DataRequest {
            chain_id: ChainId(1),
            range,
            required: CapabilitySet::of(Capability::Logs),
            log_fields: leani_primitives::LogFieldSet::NONE,
            allow_filtered: false,
            projection: leani_source_api::FieldProjection::default(),
            filters: leani_source_api::FilterSet::default(),
            minimum_finality: Finality::Optimistic,
            verification_policy: leani_source_api::VerificationPolicy::CompleteCryptographic,
        }
    }

    #[tokio::test]
    async fn live_source_requires_an_explicit_anchor_before_networking() {
        let source = RethP2pSource::mainnet(RethP2pConfig::default()).expect("source");
        let result = source
            .subscribe(
                live_request(BlockRange::single(BlockNumber(0))),
                LiveStart::Head,
                SourceBudget {
                    max_input_bytes: 1,
                    max_frame_bytes: 1,
                    max_frames: 1,
                    max_buffered_frames: 1,
                    max_in_flight_requests: 1,
                    temporary_disk_bytes: 1,
                },
                CancellationToken::new(),
            )
            .await;
        let Err(error) = result else {
            panic!("unanchored live start unexpectedly succeeded");
        };
        assert!(matches!(error, SourceError::InvalidPlan(_)));

        let anchor = BlockRef {
            number: BlockNumber(10),
            hash: BlockHash::new([0x11; 32]),
            parent_hash: BlockHash::ZERO,
            timestamp: 1,
        };
        let result = source
            .subscribe(
                live_request(BlockRange::single(anchor.number)),
                LiveStart::AnchoredOverlap {
                    anchor,
                    overlap_blocks: MAX_FIXED_RANGE_BLOCKS + 1,
                },
                SourceBudget {
                    max_input_bytes: 1,
                    max_frame_bytes: 1,
                    max_frames: 1,
                    max_buffered_frames: 1,
                    max_in_flight_requests: 1,
                    temporary_disk_bytes: 1,
                },
                CancellationToken::new(),
            )
            .await;
        assert!(matches!(result, Err(SourceError::InvalidPlan(_))));
    }
}
