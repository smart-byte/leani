//! Recent Ethereum body and receipt ingestion through isolated Reth P2P crates.
//!
//! This crate deliberately depends on no Reth database, EVM, RPC, or node
//! builder. It starts only the networking manager with a no-op provider,
//! requests a bounded fixed range, verifies all response commitments, and
//! converts the result into source-neutral [`BlockFrame`] values.

mod peer_store;

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt as _;
use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    fs::OpenOptions,
    future::Future,
    io::Write as _,
    net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4},
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use alloy_consensus::{
    Block, EMPTY_ROOT_HASH, Header, Transaction as _, TxReceipt as _,
    proofs::{calculate_receipt_root, calculate_transaction_root},
    transaction::SignerRecoverable,
};
use alloy_eips::{BlockHashOrNumber, Encodable2718, eip2124::Head};
use alloy_primitives::{Address as AlloyAddress, B256, B512, BloomInput, Sealable, U256};
use alloy_rlp::Encodable as _;
use async_trait::async_trait;
use futures::{FutureExt as _, StreamExt, stream, stream::FuturesUnordered};
use hickory_resolver::proto::rr::RData;
use leani_primitives::{
    Address, BlockFrame, BlockHash, BlockNumber, BlockRange, BlockRef, Capability, CapabilitySet,
    ChainId, Completeness, ConsensusAnchor, FilterScope, Finality, HeaderEnvelope, Log, Material,
    MissingReason, ObjectIdentity, Provenance, Quantity, ReceiptEnvelope, SourceId, SourceKind,
    TransactionEnvelope, TransactionHash, TrustModel, VerificationCheck, VerificationReport,
    Withdrawal,
};
use leani_source_api::{
    AttestedHead, AttestedHeadReceiver, BlockFrameStream, ChainEvent, ChainEventStream,
    DataRequest, FinalityModel, HistorySource, LiveSource, LiveStart, NetworkDisconnectReason,
    NetworkLane, NetworkPeerOrigin, NetworkPeerQualification, NetworkPhase,
    NetworkSessionTelemetry, NetworkTelemetry, NetworkTelemetrySnapshot, Partitioning,
    SourceAcquisitionMetrics, SourceBudget, SourceChunk, SourceDescriptor, SourceError, SourcePlan,
};
use reth_chainspec::MAINNET;
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
    Peers, PeersConfig, SessionsConfig, config::rng_secret_key, events::PeerEvent,
};
#[cfg(test)]
use reth_network_p2p::headers::client::HeadersDirection;
use reth_network_p2p::{
    bodies::client::BodiesClient,
    download::DownloadClient,
    error::RequestError,
    headers::client::{HeadersClient, HeadersRequest},
    priority::Priority,
};
use reth_network_peers::{NodeRecord, TrustedPeer};
use reth_tasks::Runtime;
use secp256k1::SecretKey;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace, warn};

use peer_store::ExecutionPeerStore;
pub use peer_store::{
    ExecutionPeerStoreError, ExecutionPeerStoreMerge, merge_execution_peer_stores,
};

/// Immutable Reth release used by this adapter.
pub const RETH_VERSION: &str = "2.5.2";
/// Immutable Reth commit used by this adapter.
pub const RETH_REVISION: &str = "5a6940e351fed80458fe6c9da8581cbe4b8bd036";
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
const MAINNET_DNS_RETRY_INTERVAL: Duration = Duration::from_secs(15);
const PEER_QUALIFICATION_RETRY_INTERVAL: Duration = Duration::from_secs(5);
static SECRET_KEY_WRITE_SEQUENCE: AtomicU64 = AtomicU64::new(0);
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
// Material request slots only live requests may use: history, probes and
// background qualification leave them free, so they never starve the lane
// that follows the head.
const RESERVED_LIVE_MATERIAL_REQUESTS: usize = 4;
// Peers one poll for a block at the head asks. It usually does not exist yet,
// so racing every eligible peer only multiplies "not yet" replies.
const AT_HEAD_HEADER_PEERS: usize = 2;
// A "not yet" this close before another peer served the block came from the
// poll that served it: polls for one block are at least the first retry pause
// apart, so a peer asked by an earlier poll may have been asked before the
// block existed. A shorter configured pause shortens it.
const HEAD_POLL_NOT_YET_TOLERANCE: Duration = Duration::from_millis(250);
// Peers head discovery races for the minimum live head. The block exists, so
// a few peers answer it; discovery runs on every live-loop iteration.
const MINIMUM_LIVE_HEAD_PEERS: usize = 3;
// How long the live lane waits for a sync-committee-attested head above its
// tip before it reports itself disconnected: four slots, so a missed slot or
// two, when no block is attested, keeps readiness.
const ATTESTED_HEAD_GRACE: Duration = Duration::from_secs(48);
// Headers one request proves of an attested head's ancestry, the ETH limit.
const MAX_ANCESTRY_HEADERS: u64 = 1_024;
// A live body or receipt request asks every eligible peer once per wave. After
// this many waves, or this long, nobody serves the material of that header.
const MAX_LIVE_MATERIAL_WAVES: usize = 8;
const LIVE_MATERIAL_TIMEOUT: Duration = Duration::from_mins(1);
// Continuation rounds, and response bytes across them, one eth/70 receipt
// request may take. Peers answer about 2 MiB per round, so a block's receipts
// fit, while a batch that does not fit is split into single blocks.
const MAX_RECEIPTS70_ROUNDS: usize = 8;
const MAX_RECEIPTS70_BYTES: usize = 64 * 1024 * 1024;
// Session events reach the direct-peer pool over a lossy broadcast, so the
// pool is reconciled with Reth's active sessions this often.
const DIRECT_PEER_RECONCILE_INTERVAL: Duration = Duration::from_secs(10);
const DEFAULT_PEER_STORE_MAX_ENTRIES: usize = 4_096;
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
    /// Hard connected-peer floor required before requests may start.
    ///
    /// Peers do not need to complete capability qualification before they
    /// count toward this floor. Qualification remains a background ranking
    /// signal, while every response is independently commitment-checked.
    pub minimum_peers: usize,
    /// Number of independently verified body-serving peers kept ready before
    /// qualification falls back to low-rate background probing.
    pub body_serving_peer_target: usize,
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
    /// Optional disposable `SQLite` store containing bounded peer candidates and
    /// independently observed service evidence.
    pub peer_store_path: Option<PathBuf>,
    /// Stable secp256k1 node identity. When omitted while a peer store is
    /// configured, a sibling `execution-p2p-secret` file is used.
    pub secret_key_path: Option<PathBuf>,
    /// Maximum retained peer records after merging Reth's current view with
    /// broader discovered candidates. This bounds growth without letting one
    /// short or failed run erase candidates needed by the next startup.
    pub peer_store_max_entries: usize,
    pub peer_store_flush_interval: Duration,
    pub poll_interval: Duration,
    pub max_reorg_depth: usize,
    /// Shared operational status for the persistent P2P manager.
    pub network_telemetry: NetworkTelemetry,
}

impl Default for RethP2pConfig {
    fn default() -> Self {
        Self {
            minimum_peers: 1,
            body_serving_peer_target: 4,
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
            peer_refill_interval: Duration::from_secs(1),
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
            peer_store_path: None,
            secret_key_path: None,
            peer_store_max_entries: DEFAULT_PEER_STORE_MAX_ENTRIES,
            peer_store_flush_interval: Duration::from_mins(1),
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
        if self.body_serving_peer_target == 0
            || self.body_serving_peer_target > self.preferred_peers
        {
            return Err(P2pError::InvalidConfig(
                "body-serving peer target must be between one and preferred peers".to_owned(),
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
        if !(1..=65_536).contains(&self.peer_store_max_entries) {
            return Err(P2pError::InvalidConfig(
                "peer store maximum entries must be in 1..=65536".to_owned(),
            ));
        }
        if self.peer_wait_timeout.is_zero()
            || self.request_timeout.is_zero()
            || self.retry_backoff.is_zero()
            || self.retry_backoff_max.is_zero()
            || self.peer_refill_interval.is_zero()
            || self.peer_recovery_timeout.is_zero()
            || self.peer_store_flush_interval.is_zero()
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
    /// Sync-committee-attested heads a verified finality source publishes.
    /// The live lane includes no block above the newest, and none without
    /// them.
    attested_heads: Option<AttestedHeadReceiver>,
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

/// Assembles the hashes of a finalized header range from segments fetched in
/// any order. Segments are validated top-down from the anchor: each must end
/// at the hash that its validated child names as parent, so every validated
/// segment is proven, and one that does not is fetched again while the
/// validated ones stay.
#[derive(Debug)]
struct AnchoredHeaderProofBuilder {
    proof: BlockRange,
    retained: BlockRange,
    /// Ranges to fetch, the anchor's first.
    pending: VecDeque<BlockRange>,
    /// Fetched segments not validated yet, by last block, with the peer that
    /// served each.
    segments: BTreeMap<BlockNumber, (B512, HeaderProofSegment)>,
    /// The last block of the next segment to validate.
    next_end: Option<BlockNumber>,
    /// The hash that segment must end at: the anchor, then the parent that
    /// the lowest validated segment names.
    expected_hash: BlockHash,
    /// Retained hashes validated so far, newest first.
    retained_hashes: Vec<BlockHash>,
}

impl AnchoredHeaderProofBuilder {
    fn new(
        proof: BlockRange,
        retained: BlockRange,
        request_blocks: u64,
        anchor: BlockHash,
    ) -> Self {
        Self {
            proof,
            retained,
            pending: anchored_header_ranges(proof, request_blocks)
                .into_iter()
                .rev()
                .collect(),
            segments: BTreeMap::new(),
            next_end: Some(proof.end()),
            expected_hash: anchor,
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

    /// Record one fetched range and validate the segments it completes.
    /// Returns each peer whose segment contradicts the anchored chain, with
    /// the error. Such a range, like a failed one, is fetched again first.
    fn record(
        &mut self,
        range: BlockRange,
        result: Result<(B512, HeaderProofSegment), P2pError>,
    ) -> Vec<(B512, P2pError)> {
        match result {
            Ok((peer, segment)) => {
                self.segments.insert(segment.range.end(), (peer, segment));
            }
            Err(_) => self.pending.push_front(range),
        }
        self.advance()
    }

    fn advance(&mut self) -> Vec<(B512, P2pError)> {
        let mut rejected = Vec::new();
        while let Some(next_end) = self.next_end {
            let Some((peer, segment)) = self.segments.remove(&next_end) else {
                break;
            };
            if segment.range.start() < self.proof.start()
                || segment.hashes.last() != Some(&self.expected_hash)
            {
                rejected.push((
                    peer,
                    P2pError::ExpectationMismatch(format!(
                        "anchored header proof segment ending at block {} is not the parent of \
                         the proven chain above it",
                        next_end.0
                    )),
                ));
                self.pending.push_front(segment.range);
                break;
            }
            for (offset, hash) in segment.hashes.iter().copied().enumerate().rev() {
                let number = segment
                    .range
                    .start()
                    .0
                    .saturating_add(u64::try_from(offset).unwrap_or(u64::MAX));
                if number >= self.retained.start().0 && number <= self.retained.end().0 {
                    self.retained_hashes.push(hash);
                }
            }
            self.expected_hash = segment.first_parent;
            self.next_end = (segment.range.start() > self.proof.start())
                .then(|| BlockNumber(segment.range.start().0.saturating_sub(1)));
        }
        rejected
    }

    fn finish(&mut self) -> Result<AnchoredHeaderProof, P2pError> {
        if self.next_end.is_some() {
            return Err(P2pError::InvalidResponse(
                "anchored header proof still has pending ranges".to_owned(),
            ));
        }
        let expected = usize::try_from(self.retained.len()).map_err(|_| {
            P2pError::InvalidConfig("retained history proof range is too large".to_owned())
        })?;
        if self.retained_hashes.len() != expected {
            return Err(P2pError::InvalidResponse(
                "anchored header proof omitted retained material hashes".to_owned(),
            ));
        }
        let mut hashes = std::mem::take(&mut self.retained_hashes);
        hashes.reverse();
        Ok(AnchoredHeaderProof {
            proof: self.proof,
            retained: self.retained,
            hashes,
        })
    }
}

/// Logical lane over the process-wide network manager. Dropping or retrying a
/// lane must not disconnect healthy peers shared by other lanes.
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

    /// Observe a head this session verified: the number of a header that
    /// passed validation at a block this node requested. Peer status claims
    /// never reach it.
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

const fn qualification_served_header(outcome: PeerQualification) -> bool {
    matches!(
        outcome,
        PeerQualification::BodyServing | PeerQualification::HeadersOnly
    )
}

impl From<PeerQualification> for NetworkPeerQualification {
    fn from(value: PeerQualification) -> Self {
        match value {
            PeerQualification::BodyServing => Self::BodyServing,
            PeerQualification::HeadersOnly => Self::HeadersOnly,
            PeerQualification::Lagging => Self::Lagging,
            PeerQualification::Rejected => Self::Rejected,
            PeerQualification::TimedOut => Self::TimedOut,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct PeerCandidateAdmission {
    origin: NetworkPeerOrigin,
    admitted_at: Instant,
}

#[derive(Debug)]
struct PeerCandidateRegistry {
    maximum_entries: usize,
    admissions: Mutex<HashMap<B512, PeerCandidateAdmission>>,
}

impl PeerCandidateRegistry {
    fn new(maximum_entries: usize) -> Self {
        Self {
            maximum_entries,
            admissions: Mutex::new(HashMap::new()),
        }
    }

    fn admit(&self, peer_id: B512, origin: NetworkPeerOrigin) -> bool {
        let mut admissions = self
            .admissions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if admissions.contains_key(&peer_id) {
            return false;
        }
        if admissions.len() >= self.maximum_entries
            && let Some(oldest) = admissions
                .iter()
                .min_by_key(|(_, admission)| admission.admitted_at)
                .map(|(peer_id, _)| *peer_id)
        {
            admissions.remove(&oldest);
        }
        admissions.insert(
            peer_id,
            PeerCandidateAdmission {
                origin,
                admitted_at: Instant::now(),
            },
        );
        true
    }

    fn origin(&self, peer_id: B512) -> NetworkPeerOrigin {
        self.admissions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&peer_id)
            .map_or(NetworkPeerOrigin::Discv4Or5, |admission| admission.origin)
    }

    fn qualification_elapsed(&self, peer_id: B512) -> Option<Duration> {
        self.admissions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&peer_id)
            .map(|admission| admission.admitted_at.elapsed())
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
        if !same_qualification_target(state.target, target) {
            state.target = target;
            state.outcomes.clear();
            drop(state);
            self.changed.notify_waiters();
        }
    }

    fn reset(&self, target: BlockRef) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.target = target;
        state.outcomes.clear();
        drop(state);
        self.changed.notify_waiters();
    }

    fn record(&self, target: BlockRef, peer_id: B512, outcome: PeerQualification) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if same_qualification_target(state.target, target) {
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
        if !same_qualification_target(state.target, target) {
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
        same_qualification_target(state.target, target)
            && state
                .outcomes
                .get(&peer_id)
                .is_some_and(|outcome| matches!(outcome, PeerQualification::BodyServing))
    }
}

/// Qualification targets are blocks: the same block advertised again with a
/// synthetic parent or a fresh timestamp is the same target.
fn same_qualification_target(left: BlockRef, right: BlockRef) -> bool {
    left.number == right.number && left.hash == right.hash
}

#[derive(Debug)]
struct PersistentNetwork {
    state: tokio::sync::Mutex<Option<PersistentNetworkState>>,
    next_generation: AtomicU64,
    request_gate: Arc<MaterialRequestGate>,
    direct_peers: Arc<DirectPeerPool>,
    peer_store: Arc<ExecutionPeerStore>,
    qualifications: Arc<PeerQualificationPool>,
}

impl PersistentNetwork {
    fn drop_peer(&self, peer_id: B512, connection_id: u64) {
        if self.direct_peers.invalidate(peer_id, connection_id) {
            self.qualifications.remove(peer_id);
        }
    }

    fn invalidate_peer(&self, peer_id: B512, connection_id: u64, detail: &str) {
        self.peer_store.record_failure(peer_id, detail);
        self.drop_peer(peer_id, connection_id);
    }

    /// The one penalty rule for a failed response from the session behind
    /// `lease`, checked against an expectation of `expectation`'s trust. A
    /// response `classify_response_failure` finds invalid bans the peer
    /// through `ban_peer` and stores the failure. Any other failure, such as
    /// an empty reply or a timeout, only cools the leased lane.
    fn penalize_response(
        &self,
        lease: &mut DirectPeerLease,
        error: &P2pError,
        expectation: ExpectationTrust,
        ban_peer: impl FnOnce(B512),
    ) -> ResponseFault {
        lease.failed();
        let fault = classify_response_failure(error, expectation);
        if fault == ResponseFault::Invalid {
            ban_peer(lease.peer.peer_id);
            self.invalidate_peer(lease.peer.peer_id, lease.connection_id, &error.to_string());
        }
        fault
    }
}

#[derive(Debug)]
struct PersistentNetworkState {
    generation: u64,
    handle: NetworkHandle<EthNetworkPrimitives>,
    fetch: FetchClient<EthNetworkPrimitives>,
    peer_store_flush: tokio::sync::mpsc::Sender<tokio::sync::oneshot::Sender<Result<(), String>>>,
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
}

#[derive(Debug)]
struct PeerServiceState {
    verified: bool,
    failures: u32,
    retry_at: Instant,
}

impl PeerServiceState {
    fn new() -> Self {
        Self {
            verified: false,
            failures: 0,
            retry_at: Instant::now(),
        }
    }
}

#[derive(Debug)]
struct DirectPeerState {
    peer: DirectPeer,
    connection_id: u64,
    in_flight: usize,
    header: PeerServiceState,
    body: PeerServiceState,
    receipts: PeerServiceState,
    /// Headers the peer served whose body or receipts no peer served, since
    /// a frame on one of its headers last completed.
    withheld_strikes: u32,
    /// When the peer may serve headers again after its last strike.
    withheld_until: Instant,
}

impl DirectPeerState {
    fn new(peer: DirectPeer, connection_id: u64) -> Self {
        Self {
            peer,
            connection_id,
            in_flight: 0,
            header: PeerServiceState::new(),
            body: PeerServiceState::new(),
            receipts: PeerServiceState::new(),
            withheld_strikes: 0,
            withheld_until: Instant::now(),
        }
    }

    fn service(&self, kind: PeerMaterialKind) -> &PeerServiceState {
        match kind {
            PeerMaterialKind::Header => &self.header,
            PeerMaterialKind::Body => &self.body,
            PeerMaterialKind::Receipts => &self.receipts,
        }
    }

    fn service_mut(&mut self, kind: PeerMaterialKind) -> &mut PeerServiceState {
        match kind {
            PeerMaterialKind::Header => &mut self.header,
            PeerMaterialKind::Body => &mut self.body,
            PeerMaterialKind::Receipts => &mut self.receipts,
        }
    }

    /// When the peer's `kind` lane may be asked next. Headers also wait out
    /// the peer's last withheld-header strike.
    fn available_at(&self, kind: PeerMaterialKind) -> Instant {
        match kind {
            PeerMaterialKind::Header => self.header.retry_at.max(self.withheld_until),
            PeerMaterialKind::Body | PeerMaterialKind::Receipts => self.service(kind).retry_at,
        }
    }

    /// Whether the peer ranks after the others for `kind`: headers from a
    /// peer with a withheld-header strike.
    fn struck(&self, kind: PeerMaterialKind) -> bool {
        kind == PeerMaterialKind::Header && self.withheld_strikes > 0
    }
}

fn cool_peer_service(peer: &mut DirectPeerState, kind: PeerMaterialKind) {
    let service = peer.service_mut(kind);
    service.failures = service.failures.saturating_add(1);
    service.retry_at = Instant::now() + peer_cooldown(service.failures);
}

/// The cooldown after `failures` consecutive failures of a lane, or strikes:
/// 250 ms, doubling with each, up to 30 s.
fn peer_cooldown(failures: u32) -> Duration {
    let exponent = failures.saturating_sub(1).min(7);
    Duration::from_millis(250_u64.saturating_mul(1_u64 << exponent).min(30_000))
}

#[derive(Debug)]
struct DirectPeerPool {
    peers: Mutex<Vec<DirectPeerState>>,
    /// Peers dropped for invalid material. Reth keeps a trusted peer's
    /// session despite a ban, so the reconciler must not adopt such a peer
    /// again before that session closes.
    invalidated: Mutex<HashSet<B512>>,
    cursor: AtomicUsize,
    next_connection_id: AtomicU64,
    changed: tokio::sync::Notify,
    quality: Arc<ExecutionPeerStore>,
}

impl DirectPeerPool {
    fn new(quality: Arc<ExecutionPeerStore>) -> Self {
        Self {
            peers: Mutex::new(Vec::new()),
            invalidated: Mutex::new(HashSet::new()),
            cursor: AtomicUsize::new(0),
            next_connection_id: AtomicU64::new(1),
            changed: tokio::sync::Notify::new(),
            quality,
        }
    }

    fn insert(&self, peer: DirectPeer) {
        let connection_id = self.next_connection_id.fetch_add(1, Ordering::Relaxed);
        // A new session starts over, even for a peer whose previous session
        // was dropped for invalid material.
        let mut invalidated = self
            .invalidated
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        invalidated.remove(&peer.peer_id);
        let mut peers = self
            .peers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let state = DirectPeerState::new(peer, connection_id);
        if let Some(existing) = peers
            .iter_mut()
            .find(|existing| existing.peer.peer_id == state.peer.peer_id)
        {
            *existing = state;
        } else {
            peers.push(state);
        }
        drop(peers);
        drop(invalidated);
        self.changed.notify_waiters();
    }

    fn remove(&self, peer_id: B512) {
        self.peers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|peer| peer.peer.peer_id != peer_id);
        self.changed.notify_waiters();
    }

    /// Drop the session `connection_id` for invalid material and keep its peer
    /// out until that session closes. A session the peer opened since keeps
    /// its place: returns whether the session was still pooled.
    fn invalidate(&self, peer_id: B512, connection_id: u64) -> bool {
        // Lock order as in `insert`, so a new session cannot slip between the
        // removal and the mark.
        let mut invalidated = self
            .invalidated
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut peers = self
            .peers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let pooled = peers.len();
        peers.retain(|state| state.peer.peer_id != peer_id || state.connection_id != connection_id);
        let removed = peers.len() != pooled;
        drop(peers);
        if removed {
            invalidated.insert(peer_id);
        }
        drop(invalidated);
        if removed {
            self.changed.notify_waiters();
        }
        removed
    }

    /// Drop a peer whose session closed; a later session may join again.
    fn session_closed(&self, peer_id: B512) {
        self.invalidated
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&peer_id);
        self.remove(peer_id);
    }

    fn invalidated(&self) -> HashSet<B512> {
        self.invalidated
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn clear(&self) {
        self.peers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
        self.invalidated
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
        self.changed.notify_waiters();
    }

    fn len(&self) -> usize {
        self.peers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    fn set_qualification(&self, peer_id: B512, qualification: PeerQualification) {
        // A probe at one target only adds evidence. A lagging, timed-out or
        // header-only answer says nothing against material the session has
        // already served verifiably, so it never clears a verified lane.
        let served_header = qualification_served_header(qualification);
        let served_body = matches!(qualification, PeerQualification::BodyServing);
        let mut peers = self
            .peers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(peer) = peers.iter_mut().find(|peer| peer.peer.peer_id == peer_id) {
            if served_header {
                peer.header.verified = true;
                peer.header.failures = 0;
                peer.header.retry_at = Instant::now();
            }
            if served_body {
                peer.body.verified = true;
                peer.body.failures = 0;
                peer.body.retry_at = Instant::now();
            }
        }
        drop(peers);
        self.changed.notify_waiters();
    }

    fn get(&self, peer_id: B512) -> Option<(DirectPeer, u64)> {
        self.peers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .find(|peer| peer.peer.peer_id == peer_id)
            .map(|peer| (peer.peer.clone(), peer.connection_id))
    }

    /// Lease a header peer for a poll for the next block at the head: the
    /// best eligible peer the rotation has not asked for that block yet, or,
    /// once every eligible peer has been asked, the best one again.
    fn lease_for_head_poll(
        self: &Arc<Self>,
        rotation: &mut HeadPollRotation,
        per_peer_limit: usize,
    ) -> Option<DirectPeerLease> {
        let lease = match self.try_acquire_excluding(
            PeerMaterialKind::Header,
            per_peer_limit,
            rotation.exclusions(),
            None,
        ) {
            Some(lease) => lease,
            None if rotation.start_over() => self.try_acquire_excluding(
                PeerMaterialKind::Header,
                per_peer_limit,
                rotation.exclusions(),
                None,
            )?,
            None => return None,
        };
        rotation.record_asked(lease.peer.peer_id);
        Some(lease)
    }

    fn record_material_failure(&self, peer_id: B512, kind: PeerMaterialKind) {
        let mut peers = self
            .peers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(peer) = peers.iter_mut().find(|peer| peer.peer.peer_id == peer_id) {
            cool_peer_service(peer, kind);
        }
        drop(peers);
        self.changed.notify_waiters();
    }

    /// Strike a peer whose header no peer served the body or receipts of: it
    /// may have made the block up. Each strike keeps the peer out of header
    /// requests for the next step of the lane cooldown, up to 30 s, and ranks
    /// it after other header peers. Unlike a lane cooldown, a header the peer
    /// serves lifts neither; only a completed frame on one of its headers
    /// does. Nothing is persisted and nobody is banned.
    fn strike_withheld_header(&self, peer_id: B512) {
        let mut peers = self
            .peers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(peer) = peers.iter_mut().find(|peer| peer.peer.peer_id == peer_id) {
            peer.withheld_strikes = peer.withheld_strikes.saturating_add(1);
            peer.withheld_until = Instant::now() + peer_cooldown(peer.withheld_strikes);
        }
        drop(peers);
        self.changed.notify_waiters();
    }

    /// A frame on a header the peer served completed: that block exists, so
    /// its withheld-header strikes clear.
    fn clear_withheld_header(&self, peer_id: B512) {
        let mut peers = self
            .peers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let cleared = peers
            .iter_mut()
            .find(|peer| peer.peer.peer_id == peer_id && peer.withheld_strikes > 0)
            .map(|peer| {
                peer.withheld_strikes = 0;
                peer.withheld_until = Instant::now();
            })
            .is_some();
        drop(peers);
        if cleared {
            self.changed.notify_waiters();
        }
    }

    fn try_acquire_excluding(
        self: &Arc<Self>,
        kind: PeerMaterialKind,
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
                    && peers[*index].in_flight < per_peer_limit
                    && peers[*index].available_at(kind) <= now
            })
            .min_by_key(|index| {
                (
                    Some(peers[*index].peer.peer_id) != preferred,
                    peers[*index].struck(kind),
                    !peers[*index].service(kind).verified,
                    std::cmp::Reverse(self.quality.rank(peers[*index].peer.peer_id)),
                    peers[*index].service(kind).failures,
                    peers[*index].in_flight,
                )
            })?;
        peers[selected].in_flight = peers[selected].in_flight.saturating_add(1);
        Some(DirectPeerLease {
            pool: self.clone(),
            peer: peers[selected].peer.clone(),
            connection_id: peers[selected].connection_id,
            kind,
            outcome: DirectPeerOutcome::Neutral,
        })
    }

    async fn acquire(
        self: &Arc<Self>,
        kind: PeerMaterialKind,
        per_peer_limit: usize,
        unavailable_timeout: Duration,
        cancellation: &CancellationToken,
    ) -> Result<DirectPeerLease, P2pError> {
        self.acquire_excluding(
            kind,
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
        kind: PeerMaterialKind,
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
                            && peers[*index].in_flight < per_peer_limit
                            && peers[*index].available_at(kind) <= now
                    })
                    .min_by_key(|index| {
                        (
                            Some(peers[*index].peer.peer_id) != preferred,
                            peers[*index].struck(kind),
                            !peers[*index].service(kind).verified,
                            std::cmp::Reverse(self.quality.rank(peers[*index].peer.peer_id)),
                            peers[*index].service(kind).failures,
                            peers[*index].in_flight,
                        )
                    });
                if let Some(index) = selected {
                    peers[index].in_flight = peers[index].in_flight.saturating_add(1);
                    return Ok(Some(DirectPeerLease {
                        pool: self.clone(),
                        peer: peers[index].peer.clone(),
                        connection_id: peers[index].connection_id,
                        kind,
                        outcome: DirectPeerOutcome::Neutral,
                    }));
                }
                (
                    peers
                        .iter()
                        .filter(|peer| {
                            !excluded.contains(&peer.peer.peer_id)
                                && peer.in_flight < per_peer_limit
                        })
                        .map(|peer| peer.available_at(kind).saturating_duration_since(now))
                        .min(),
                    !peers.is_empty(),
                    peers
                        .iter()
                        .any(|peer| !excluded.contains(&peer.peer.peer_id)),
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

    fn snapshot(&self) -> Vec<(DirectPeer, u64)> {
        self.peers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|state| (state.peer.clone(), state.connection_id))
            .collect()
    }

    fn sessions(&self) -> Vec<PooledPeerSession> {
        self.peers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|state| PooledPeerSession {
                peer_id: state.peer.peer_id,
                connection_id: state.connection_id,
                sender_open: !state.peer.messages.to_session_tx.is_closed(),
            })
            .collect()
    }

    /// Remove an entry only while it still belongs to `connection_id`, so a
    /// session re-established meanwhile stays.
    fn remove_connection(&self, peer_id: B512, connection_id: u64) -> bool {
        let mut peers = self
            .peers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let before = peers.len();
        peers.retain(|state| state.peer.peer_id != peer_id || state.connection_id != connection_id);
        let removed = peers.len() != before;
        drop(peers);
        if removed {
            self.changed.notify_waiters();
        }
        removed
    }

    /// Add a session the pool missed, unless its open event arrived meanwhile
    /// or the peer was dropped for invalid material since.
    fn insert_missing(&self, peer: DirectPeer) -> bool {
        let invalidated = self
            .invalidated
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if invalidated.contains(&peer.peer_id) {
            return false;
        }
        let connection_id = self.next_connection_id.fetch_add(1, Ordering::Relaxed);
        let mut peers = self
            .peers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if peers.iter().any(|state| state.peer.peer_id == peer.peer_id) {
            return false;
        }
        peers.push(DirectPeerState::new(peer, connection_id));
        drop(peers);
        drop(invalidated);
        self.changed.notify_waiters();
        true
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
    connection_id: u64,
    kind: PeerMaterialKind,
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
        if let Some(peer) = peers.iter_mut().find(|peer| {
            peer.peer.peer_id == self.peer.peer_id && peer.connection_id == self.connection_id
        }) {
            peer.in_flight = peer.in_flight.saturating_sub(1);
            match self.outcome {
                DirectPeerOutcome::Success => {
                    let service = peer.service_mut(self.kind);
                    service.verified = true;
                    service.failures = 0;
                    service.retry_at = Instant::now();
                }
                DirectPeerOutcome::Failure => {
                    cool_peer_service(peer, self.kind);
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

/// Process-wide scheduler for material requests: one global limit shared by
/// every lane, with slots reserved for high-priority live requests.
#[derive(Debug)]
struct MaterialRequestGate {
    limit: usize,
    in_flight: AtomicUsize,
    high_priority_waiters: AtomicUsize,
    changed: tokio::sync::Notify,
}

impl MaterialRequestGate {
    fn new(limit: usize) -> Self {
        Self {
            limit: limit.max(1),
            in_flight: AtomicUsize::new(0),
            high_priority_waiters: AtomicUsize::new(0),
            changed: tokio::sync::Notify::new(),
        }
    }

    /// Take a slot if a request of `priority` may use one now. Normal work
    /// leaves the reserved live slots free and yields to a waiting live
    /// request.
    fn try_acquire(self: &Arc<Self>, priority: Priority) -> Option<MaterialRequestPermit> {
        if priority.is_normal() && self.high_priority_waiters.load(Ordering::Acquire) != 0 {
            return None;
        }
        let capacity = material_request_capacity(self.limit, priority);
        self.in_flight
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                (current < capacity).then(|| current.saturating_add(1))
            })
            .ok()
            .map(|_| MaterialRequestPermit { gate: self.clone() })
    }

    async fn acquire(
        self: &Arc<Self>,
        priority: Priority,
        cancellation: &CancellationToken,
    ) -> Result<MaterialRequestPermit, P2pError> {
        let _high_priority_waiter = priority.is_high().then(|| {
            self.high_priority_waiters.fetch_add(1, Ordering::AcqRel);
            HighPriorityWaiter { gate: self.clone() }
        });
        wait_for_release(&self.changed, cancellation, || self.try_acquire(priority)).await
    }
}

/// Slots a request of `priority` may occupy under the gate's global `limit`.
/// Live (high-priority) requests may use every slot. Other work leaves
/// `RESERVED_LIVE_MATERIAL_REQUESTS` slots free, and at most half the limit,
/// so both classes always keep a slot.
fn material_request_capacity(limit: usize, priority: Priority) -> usize {
    let limit = limit.max(1);
    match priority {
        Priority::High => limit,
        Priority::Normal => limit - RESERVED_LIVE_MATERIAL_REQUESTS.min(limit / 2),
    }
}

/// Retry `try_acquire` until it succeeds, waiting for `changed` in between.
/// The wakeup is registered, as an enabled waiter, before every check, so a
/// slot released between a failed check and the wait still wakes the caller,
/// whether the release notifies every waiter or only the first one.
async fn wait_for_release<T>(
    changed: &tokio::sync::Notify,
    cancellation: &CancellationToken,
    mut try_acquire: impl FnMut() -> Option<T>,
) -> Result<T, P2pError> {
    loop {
        let mut released = std::pin::pin!(changed.notified());
        released.as_mut().enable();
        if let Some(acquired) = try_acquire() {
            return Ok(acquired);
        }
        tokio::select! {
            () = cancellation.cancelled() => return Err(P2pError::Cancelled),
            () = released => {}
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

/// A peer's validated reply to a live header request. Its reward waits for
/// the frames built on the headers: a header whose body or receipts no peer
/// serves earns nothing.
#[derive(Clone, Copy, Debug)]
struct HeaderServe {
    peer_id: B512,
    block: u64,
    elapsed: Duration,
    /// Whether the headers were checked against a verified hash, so they are
    /// anchored to a consensus-verified block. Only such a reply earns a
    /// reward or clears a withheld-header strike.
    anchored: bool,
}

#[derive(Clone, Copy, Debug)]
struct LiveReceiptMaterial<'a> {
    header: &'a Header,
    body: &'a BlockBody,
    hash: B256,
    /// The peer that served the header.
    header_peer: B512,
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

/// The policy of a live header request for blocks that exist, such as the
/// minimum live head or an attested head's ancestry: a race of
/// `MINIMUM_LIVE_HEAD_PEERS` peers at a time, at live priority.
const fn minimum_live_head_policy() -> MaterialRequestPolicy {
    MaterialRequestPolicy {
        concurrency: MINIMUM_LIVE_HEAD_PEERS,
        priority: Priority::High,
    }
}

/// Parallel and total peers one live header request asks. Polling at the head
/// asks a cohort of one or two peers per poll, because the next block is
/// usually not produced yet; catching up races every eligible peer.
fn live_header_fanout(at_head: bool, request_limit: usize) -> (usize, usize) {
    if at_head {
        let cohort = request_limit.clamp(1, AT_HEAD_HEADER_PEERS);
        (cohort, cohort)
    } else {
        (request_limit, usize::MAX)
    }
}

/// What a failed live header reply costs its peer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HeaderReplyCost {
    /// Invalid material: ban the session and persist the failure.
    Ban,
    /// A lagging or disagreeing peer: the normal header-lane cooldown.
    Cooldown,
    /// "Not yet" for the next block at the head: nothing now. The lane's
    /// rotation asks other peers, and cools this one if another peer then
    /// serves the block.
    NotYet,
}

/// What a failed live header reply costs its peer. An empty or short reply to
/// a head poll only says the block is not produced yet; for blocks that
/// exist, such as a catch-up range or the minimum live head, it is a lagging
/// peer.
const fn live_header_reply_cost(
    error: &P2pError,
    expectation: ExpectationTrust,
    head_poll: bool,
) -> HeaderReplyCost {
    match classify_response_failure(error, expectation) {
        ResponseFault::Invalid => HeaderReplyCost::Ban,
        ResponseFault::Disagreement
            if head_poll && matches!(error, P2pError::IncompleteResponse { .. }) =>
        {
            HeaderReplyCost::NotYet
        }
        ResponseFault::Disagreement => HeaderReplyCost::Cooldown,
    }
}

/// The live lane's rotation over the peers it polls for the next block.
///
/// Each poll asks peers the rotation has not asked for that block yet, and it
/// starts over once every eligible peer has been asked, so two peers that
/// answer "not yet" cannot hold the lane: the next poll asks others. Once the
/// lane has moved on with a block a poll served, a peer that answered "not
/// yet" for it after that serve, or within the tolerance before it, earns the
/// normal header-lane cooldown. Peers asked before the block existed cost
/// nothing.
#[derive(Debug)]
struct HeadPollRotation {
    block: Option<BlockNumber>,
    /// Peers asked for `block` since the rotation last started over.
    asked: HashSet<B512>,
    /// Peers the current poll asked.
    polling: HashSet<B512>,
    /// Peers that answered "not yet" for `block`, and when they last did.
    not_yet: HashMap<B512, Instant>,
    /// When a poll last served `block`, unless the lane could not go on with
    /// that serve.
    served_at: Option<Instant>,
    /// How long before a serve a "not yet" still counts against its peer.
    not_yet_tolerance: Duration,
}

impl HeadPollRotation {
    /// A rotation for a lane whose first retry pause is `retry_backoff`: a
    /// "not yet" counts within `HEAD_POLL_NOT_YET_TOLERANCE`, or within the
    /// pause if it is shorter, before a serve.
    fn new(retry_backoff: Duration) -> Self {
        Self {
            block: None,
            asked: HashSet::new(),
            polling: HashSet::new(),
            not_yet: HashMap::new(),
            served_at: None,
            not_yet_tolerance: HEAD_POLL_NOT_YET_TOLERANCE.min(retry_backoff),
        }
    }

    /// Begin a poll for `block`. Once the lane has moved on with the block a
    /// poll served before, returns the peers that withheld it, to cool.
    fn begin_poll(&mut self, block: BlockNumber) -> Vec<B512> {
        self.polling.clear();
        if self.block == Some(block) {
            return Vec::new();
        }
        let moved_on = self.block.is_some_and(|polled| block.0 > polled.0);
        self.block = Some(block);
        self.asked.clear();
        let not_yet = std::mem::take(&mut self.not_yet);
        // A block the lane caught up past, which no poll served, blames no
        // peer: they may have been asked before it existed.
        let Some(served_at) = self.served_at.take().filter(|_| moved_on) else {
            return Vec::new();
        };
        let mut withheld = not_yet
            .into_iter()
            .filter(|(_, answered_at)| {
                served_at.saturating_duration_since(*answered_at) <= self.not_yet_tolerance
            })
            .map(|(peer_id, _)| peer_id)
            .collect::<Vec<_>>();
        withheld.sort_unstable();
        withheld
    }

    /// Peers the next lease for this poll skips.
    const fn exclusions(&self) -> &HashSet<B512> {
        &self.asked
    }

    fn record_asked(&mut self, peer_id: B512) {
        self.asked.insert(peer_id);
        self.polling.insert(peer_id);
    }

    /// Every eligible peer has been asked for the block: ask them again,
    /// except those this poll has asked. Returns whether any peer is released.
    fn start_over(&mut self) -> bool {
        if self.asked.len() == self.polling.len() {
            return false;
        }
        self.asked.clone_from(&self.polling);
        true
    }

    fn record_not_yet(&mut self, peer_id: B512, at: Instant) {
        self.not_yet.insert(peer_id, at);
    }

    /// A peer served the block's header at `at`. It withheld nothing, even if
    /// it answered "not yet" before.
    fn record_served(&mut self, peer_id: B512, at: Instant) {
        self.not_yet.remove(&peer_id);
        self.served_at = Some(at);
    }

    /// The poll failed, after its header was served or not: no serve so far
    /// moved the lane on, such as a header whose body no peer served.
    const fn serve_failed(&mut self) {
        self.served_at = None;
    }
}

/// Settle the header serve of frames that completed, including filtered-log
/// frames that needed no body or receipts. Only headers anchored to a
/// verified hash clear their peer's withheld-header strikes; returns whether
/// the peer earns its held-back reward.
fn settle_header_serve(pool: &DirectPeerPool, serve: HeaderServe) -> bool {
    if !serve.anchored {
        return false;
    }
    pool.clear_withheld_header(serve.peer_id);
    true
}

/// The hash of a verified expected tip, which is requested by hash.
fn verified_tip(expected_tip: Option<ExpectedTip>) -> Option<BlockHash> {
    expected_tip
        .filter(|tip| tip.trust == ExpectationTrust::Verified)
        .map(|tip| tip.hash)
}

/// The ETH request for the live headers `range`, checked against
/// `expected_tip`.
///
/// A verified tip, such as an attested head, a hash its header chain proves,
/// or the finalized anchor, is requested by its hash, descending. The reply is
/// then the chain ending at that hash, or nothing when a peer lacks the
/// block, as an honest peer on another branch does. By number, the hash would
/// be compared with whatever block a peer holds at that height: an attested
/// head is not final, so that would ban honest peers after a reorg.
fn live_header_request(range: BlockRange, expected_tip: Option<ExpectedTip>) -> HeadersRequest {
    match verified_tip(expected_tip) {
        Some(hash) => HeadersRequest::falling(
            BlockHashOrNumber::Hash(B256::from(*hash.as_array())),
            range.len(),
        ),
        None => HeadersRequest::rising(BlockHashOrNumber::Number(range.start().0), range.len()),
    }
}

/// Validate a reply to [`live_header_request`] and return its headers,
/// lowest first. For a request by hash, another header breaks the protocol:
/// invalid. No header, or fewer than requested, is incomplete: a
/// disagreement, since the peer may lack the block.
fn validate_live_headers(
    range: BlockRange,
    expected_tip: Option<ExpectedTip>,
    mut headers: Vec<Header>,
) -> Result<Vec<Header>, P2pError> {
    let Some(tip) = verified_tip(expected_tip) else {
        validate_headers(range, &headers, expected_tip.map(|tip| tip.hash))?;
        return Ok(headers);
    };
    let expected = usize::try_from(range.len()).unwrap_or(usize::MAX);
    if headers.len() > expected {
        return Err(P2pError::InvalidResponse(format!(
            "{} headers for {expected} requested",
            headers.len()
        )));
    }
    if headers.is_empty() {
        return Err(P2pError::IncompleteResponse {
            component: "headers",
            returned: 0,
            expected,
        });
    }
    validate_descending_headers(range.end(), tip, &headers)?;
    if headers.len() < expected {
        return Err(P2pError::IncompleteResponse {
            component: "headers",
            returned: headers.len(),
            expected,
        });
    }
    headers.reverse();
    Ok(headers)
}

/// The block reference of a validated header.
fn header_block_ref(header: &Header) -> BlockRef {
    BlockRef {
        number: BlockNumber(header.number),
        hash: block_hash(header.hash_slow()),
        parent_hash: block_hash(header.parent_hash),
        timestamp: header.timestamp,
    }
}

/// Proven headers of consecutive blocks above the live lane's tip, on the
/// chain of the sync-committee-attested head that proved them: each was
/// fetched by hash, down from the head's own. Catch-up takes its batches
/// from them, so they are fetched once. They are invalidated when a batch or
/// a proof fails, and the next catch-up proves them again from the newest
/// attested head.
#[derive(Debug, Default)]
struct AttestedAncestry {
    /// The attested head whose header chain proved `headers`.
    head: Option<AttestedHead>,
    /// The proven headers, lowest first.
    headers: VecDeque<Header>,
    /// The peer that served them.
    header_peer: Option<B512>,
    /// Its serve, settled once frames on these headers first complete.
    serve: Option<HeaderServe>,
}

impl AttestedAncestry {
    /// The ancestry `head` proved: `headers`, a validated chain lowest first,
    /// which `serve` served.
    fn proven(head: AttestedHead, serve: HeaderServe, headers: Vec<Header>) -> Self {
        Self {
            head: Some(head),
            headers: headers.into(),
            header_peer: Some(serve.peer_id),
            serve: Some(serve),
        }
    }

    /// The first proven block, if any.
    fn first(&self) -> Option<u64> {
        self.headers.front().map(|header| header.number)
    }

    /// Take the first `count` proven headers, their peer, and its serve if it
    /// is not settled yet.
    fn take(&mut self, count: usize) -> (Vec<Header>, Option<B512>, Option<HeaderServe>) {
        let count = count.min(self.headers.len());
        (
            self.headers.drain(..count).collect(),
            self.header_peer,
            self.serve.take(),
        )
    }

    /// Forget the proven headers: the next catch-up proves them again from
    /// the newest attested head.
    fn invalidate(&mut self) {
        *self = Self::default();
    }
}

/// What the live lane fetches next.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LiveStep {
    /// No attested head is above the tip: include nothing, and wait for the
    /// next one.
    Wait,
    /// The next block is the attested head: poll it, by its hash.
    Poll {
        block: BlockNumber,
        tip: ExpectedTip,
    },
    /// Catch up on `range` with the proven headers the ancestry holds.
    CatchUp { range: BlockRange },
    /// Prove the attested head's chain down to `from` first.
    Anchor {
        from: BlockNumber,
        head: AttestedHead,
    },
}

/// What the live lane at `last` fetches next, with the newest attested head
/// and the proven ancestry it holds. A block is fetched only up to the
/// attested head, and only by a verified hash: the head's own, or through
/// headers its chain proves. `batch_blocks` bounds a catch-up batch.
fn live_step(
    last: BlockRef,
    attested: Option<AttestedHead>,
    ancestry: &AttestedAncestry,
    batch_blocks: u64,
) -> LiveStep {
    let Some(head) = attested.filter(|head| head.block_number > last.number) else {
        return LiveStep::Wait;
    };
    let next = last.number.0.saturating_add(1);
    if ancestry.first() == Some(next) {
        let proven = u64::try_from(ancestry.headers.len()).unwrap_or(u64::MAX);
        // Never above the newest attested head, even with headers an older
        // one proved.
        let end = next
            .saturating_add(batch_blocks.max(1).min(proven) - 1)
            .min(head.block_number.0);
        if let Ok(range) = BlockRange::new(BlockNumber(next), BlockNumber(end)) {
            return LiveStep::CatchUp { range };
        }
    }
    if head.block_number.0 == next {
        return LiveStep::Poll {
            block: BlockNumber(next),
            tip: ExpectedTip {
                hash: head.block_hash,
                trust: ExpectationTrust::Verified,
            },
        };
    }
    LiveStep::Anchor {
        from: BlockNumber(next),
        head,
    }
}

/// The header windows that prove an attested head's chain down to `from`:
/// each is requested by the hash of its top block, the head's own and then
/// the parent the window above names. Windows hold `MAX_ANCESTRY_HEADERS`,
/// except the top one, which holds the rest, so the lowest window, which the
/// lane keeps, is full.
async fn prove_attested_ancestry<F, Fut>(
    from: BlockNumber,
    head: AttestedHead,
    mut fetch_window: F,
) -> Result<AttestedAncestry, P2pError>
where
    F: FnMut(BlockRange, BlockHash) -> Fut,
    Fut: Future<Output = Result<(HeaderServe, Vec<Header>), P2pError>>,
{
    let total = head
        .block_number
        .0
        .checked_sub(from.0)
        .ok_or_else(|| {
            P2pError::InvalidConfig(format!(
                "attested head {} is below block {}",
                head.block_number.0, from.0
            ))
        })?
        .saturating_add(1);
    let mut size = (total - 1) % MAX_ANCESTRY_HEADERS + 1;
    let mut top = head.block_number.0;
    let mut top_hash = head.block_hash;
    loop {
        let start = top.saturating_sub(size - 1).max(from.0);
        let range = BlockRange::new(BlockNumber(start), BlockNumber(top))
            .map_err(|error| P2pError::InvalidConfig(error.to_string()))?;
        let (serve, headers) = fetch_window(range, top_hash).await?;
        let Some(lowest) = headers.first() else {
            return Err(P2pError::IncompleteResponse {
                component: "headers",
                returned: 0,
                expected: 1,
            });
        };
        if start == from.0 {
            return Ok(AttestedAncestry::proven(head, serve, headers));
        }
        top_hash = block_hash(lowest.parent_hash);
        top = start - 1;
        size = MAX_ANCESTRY_HEADERS;
    }
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
        if running.peer_store_flush.send(flush_result).await.is_ok() {
            match tokio::time::timeout(NETWORK_SHUTDOWN_TIMEOUT, flushed).await {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(error))) => {
                    warn!(%error, "failed to flush execution peer store before shutdown");
                }
                Ok(Err(_)) => {
                    warn!("execution peer-store flush channel closed before shutdown");
                }
                Err(_) => {
                    warn!("timed out flushing execution peer store before shutdown");
                }
            }
        }
        let graceful = tokio::time::timeout(NETWORK_SHUTDOWN_TIMEOUT, running.handle.shutdown())
            .await
            .is_ok();
        stop_network_task(graceful, &running.shutdown, &mut running.network_task).await;
    }
}

/// Stop a network manager task once its network has shut down, gracefully or
/// not, and wait for the task's teardown. The manager future keeps running
/// after a graceful network shutdown, so the task's own token ends its loop;
/// the task then persists the final peer state, unless that overruns the
/// timeout.
async fn stop_network_task(
    graceful: bool,
    shutdown: &CancellationToken,
    task: &mut tokio::task::JoinHandle<()>,
) {
    if !graceful {
        warn!("timed out while shutting down execution P2P network manager");
    }
    shutdown.cancel();
    if tokio::time::timeout(NETWORK_SHUTDOWN_TIMEOUT, &mut *task)
        .await
        .is_err()
    {
        task.abort();
        let _ = task.await;
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
    cancellation: CancellationToken,
) -> PeerQualificationResult {
    // Qualification is background ranking work: it never takes the request
    // slots reserved for the live lane. A peer is qualified by what it serves
    // now, never by its static handshake head.
    let result = async {
        let permit = match request_gate.acquire(Priority::Normal, &cancellation).await {
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
        let headers = match header_response {
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
        let header = match validate_target_header(target, headers) {
            Ok(header) => header,
            Err(error) => {
                // The target is requested by hash, a verified expectation:
                // another block is invalid, while none says the peer lags.
                let (outcome, detail) =
                    match classify_response_failure(&error, ExpectationTrust::Verified) {
                        ResponseFault::Invalid => (
                            PeerQualification::Rejected,
                            "peer returned a mismatched verified anchor header",
                        ),
                        ResponseFault::Disagreement => (
                            PeerQualification::Lagging,
                            "verified anchor header was not served",
                        ),
                    };
                return PeerQualificationResult {
                    target,
                    peer_id: peer.peer_id,
                    outcome,
                    detail: Some(detail.to_owned()),
                    header_elapsed: Some(header_elapsed),
                    body_elapsed: None,
                };
            }
        };
        let permit = match request_gate.acquire(Priority::Normal, &cancellation).await {
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
                // The body answers to the target header's commitments, a
                // verified expectation.
                Err(error) => match classify_response_failure(&error, ExpectationTrust::Verified) {
                    ResponseFault::Disagreement => PeerQualificationResult {
                        target,
                        peer_id: peer.peer_id,
                        outcome: PeerQualification::HeadersOnly,
                        detail: Some("verified anchor body was not served".to_owned()),
                        header_elapsed: Some(header_elapsed),
                        body_elapsed: Some(body_elapsed),
                    },
                    ResponseFault::Invalid => PeerQualificationResult {
                        target,
                        peer_id: peer.peer_id,
                        outcome: PeerQualification::Rejected,
                        detail: Some(error.to_string()),
                        header_elapsed: Some(header_elapsed),
                        body_elapsed: Some(body_elapsed),
                    },
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
    quality: Arc<ExecutionPeerStore>,
    peer_candidates: Arc<PeerCandidateRegistry>,
    handle: NetworkHandle<EthNetworkPrimitives>,
    network_telemetry: NetworkTelemetry,
    mut target_updates: tokio::sync::watch::Receiver<BlockRef>,
    request_gate: Arc<MaterialRequestGate>,
    request_timeout: Duration,
    concurrency: usize,
    body_serving_peer_target: usize,
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
            network_telemetry.set_body_serving_peers(ready);
            let qualification_concurrency = if ready < body_serving_peer_target {
                concurrency
            } else {
                1
            };
            if tasks.len() < qualification_concurrency {
                let mut candidates = direct_peers.snapshot();
                candidates.sort_by_key(|(peer, _)| std::cmp::Reverse(quality.rank(peer.peer_id)));
                for (peer, connection_id) in candidates {
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
                    // A rejected probe drops only the session it ran on, not a
                    // later session of the peer. Its other outcomes, and the
                    // qualification pool, are still kept per peer.
                    tasks.push(tokio::spawn(
                        qualify_execution_peer(
                            peer,
                            target,
                            request_timeout,
                            request_gate.clone(),
                            shutdown.clone(),
                        )
                        .map(move |result| (connection_id, result)),
                    ));
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
                    network_telemetry.set_body_serving_peers(0);
                    // Keep prior service evidence while the new target is
                    // qualified. It remains useful for ranking and avoids a
                    // global material-request outage on every head update.
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
                    let Ok((connection_id, result)) = completed.expect("qualification task exists")
                    else {
                        continue;
                    };
                    pending.remove(&result.peer_id);
                    if !same_qualification_target(result.target, target) {
                        continue;
                    }
                    if let Some(elapsed) = result.header_elapsed
                        && qualification_served_header(result.outcome)
                    {
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
                    let ready_before = qualifications.ready(target);
                    quality.record_qualification(
                        result.peer_id,
                        result.outcome,
                        result.detail.as_deref(),
                    );
                    network_telemetry.peer_qualified(
                        peer_candidates.origin(result.peer_id),
                        result.outcome.into(),
                        peer_candidates.qualification_elapsed(result.peer_id),
                    );
                    qualifications.record(target, result.peer_id, result.outcome);
                    direct_peers.set_qualification(result.peer_id, result.outcome);
                    if matches!(result.outcome, PeerQualification::BodyServing) {
                        failures.remove(&result.peer_id);
                        retry_at.remove(&result.peer_id);
                        handle.reputation_change(
                            result.peer_id,
                            ReputationChangeKind::Other(
                                VERIFIED_MATERIAL_RESPONSE_REPUTATION_REWARD,
                            ),
                        );
                        if ready_before < body_serving_peer_target
                            && qualifications.ready(target) >= body_serving_peer_target
                        {
                            for task in &tasks {
                                task.abort();
                            }
                            tasks.clear();
                            pending.clear();
                        }
                    } else {
                        let failures = failures.entry(result.peer_id).or_default();
                        *failures = failures.saturating_add(1);
                        let exponent = failures.saturating_sub(1).min(4);
                        let delay = PEER_QUALIFICATION_RETRY_INTERVAL
                            .saturating_mul(1_u32 << exponent);
                        retry_at.insert(result.peer_id, Instant::now() + delay);
                        if matches!(result.outcome, PeerQualification::Rejected) {
                            handle.ban_peer(result.peer_id);
                            direct_peers.invalidate(result.peer_id, connection_id);
                            qualifications.remove(result.peer_id);
                        }
                    }
                }
            }
        }
        network_telemetry.set_body_serving_peers(0);
    })
}

async fn wait_for_connected_peers(
    session: &P2pSession,
    direct_peers: &DirectPeerPool,
    minimum: usize,
    timeout: Duration,
    cancellation: &CancellationToken,
) -> Result<usize, P2pError> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if !session.manager_is_current().await {
            return Err(P2pError::Network(
                "execution P2P manager restarted while waiting for a connected peer".to_owned(),
            ));
        }
        let connected = direct_peers.len();
        if connected >= minimum {
            return Ok(connected);
        }
        tokio::select! {
            () = cancellation.cancelled() => return Err(P2pError::Cancelled),
            () = direct_peers.changed.notified() => {}
            () = tokio::time::sleep(Duration::from_millis(100)) => {}
            () = tokio::time::sleep_until(deadline) => {
                return Err(P2pError::PeerTimeout { minimum, connected });
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

#[derive(Debug)]
struct DnsPeerSeederRuntime {
    dns_head: Head,
    bootstrap_dns_tree: Option<String>,
    peer_candidates: Arc<PeerCandidateRegistry>,
    network_telemetry: NetworkTelemetry,
}

fn spawn_cached_peer_admitter(
    handle: NetworkHandle<EthNetworkPrimitives>,
    records: Vec<NodeRecord>,
    batch_size: usize,
    interval: Duration,
    peer_candidates: Arc<PeerCandidateRegistry>,
    network_telemetry: NetworkTelemetry,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut records = records.into_iter();
        let mut admission_tick =
            tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
        admission_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            admission_tick.tick().await;
            let mut admitted = 0_usize;
            for _ in 0..batch_size {
                let Some(record) = records.next() else {
                    debug!("finished staged admission of broad cached execution peers");
                    return;
                };
                admit_node_record_to_network(
                    &handle,
                    record,
                    NetworkPeerOrigin::CachedBroad,
                    &peer_candidates,
                    &network_telemetry,
                );
                admitted = admitted.saturating_add(1);
            }
            trace!(admitted, "admitted a broad cached execution-peer batch");
        }
    })
}

fn spawn_mainnet_dns_peer_seeder(
    handle: NetworkHandle<EthNetworkPrimitives>,
    runtime: DnsPeerSeederRuntime,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let DnsPeerSeederRuntime {
            dns_head,
            bootstrap_dns_tree,
            peer_candidates,
            network_telemetry,
        } = runtime;
        // Reth owns the single bounded dial queue. It receives terminal TCP and
        // handshake failures directly, immediately refills freed slots with
        // fresh candidates, and applies its per-peer backoff policy. Keeping a
        // second Leani-side pending/cooldown queue here delays that feedback
        // and can submit duplicate attempts.
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

        let fork_filter = MAINNET.fork_filter(dns_head);
        let mut seeded_peers = 0_usize;
        loop {
            tokio::select! {
                _ = &mut service_task.0 => {
                    warn!("mainnet execution peer DNS seeder stopped unexpectedly");
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
                    let admitted = admit_node_record_to_network(
                        &handle,
                        record,
                        NetworkPeerOrigin::DnsTree,
                        &peer_candidates,
                        &network_telemetry,
                    );
                    if admitted {
                        seeded_peers = seeded_peers.saturating_add(1);
                        if seeded_peers == 1 || seeded_peers.is_multiple_of(100) {
                            debug!(
                                seeded_peers,
                                "seeded compatible execution peers from the mainnet DNS tree"
                            );
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

fn admit_node_record_to_network(
    handle: &NetworkHandle<EthNetworkPrimitives>,
    record: NodeRecord,
    origin: NetworkPeerOrigin,
    peer_candidates: &PeerCandidateRegistry,
    network_telemetry: &NetworkTelemetry,
) -> bool {
    let admitted = peer_candidates.admit(record.id, origin);
    if admitted {
        network_telemetry.peer_candidate_admitted(origin);
    }
    // Re-submit rediscovered identities as their advertised address may have
    // changed since the first admission.
    add_node_record_to_network(handle, record);
    admitted
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
    // shut down before the serving-peer reward reaches the persistent store.
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

struct PeerPersistenceRequest {
    records: Vec<NodeRecord>,
    response: Option<tokio::sync::oneshot::Sender<Result<(), String>>>,
}

fn spawn_peer_persistence_worker(
    store: Arc<ExecutionPeerStore>,
    mut requests: tokio::sync::mpsc::Receiver<PeerPersistenceRequest>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(request) = requests.recv().await {
            let candidate_count = request.records.len();
            let started = Instant::now();
            let result = store
                .persist(request.records)
                .await
                .map_err(|error| error.to_string());
            if result.is_ok() {
                debug!(
                    candidate_count,
                    elapsed_milliseconds =
                        u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                    "persisted execution peer store"
                );
            }
            if let Some(response) = request.response {
                let _ = response.send(result);
            } else if let Err(error) = result {
                warn!(%error, "failed to refresh execution peer store");
            }
        }
    })
}

#[expect(
    clippy::too_many_lines,
    reason = "one select loop synchronizes manager, persistence, telemetry, recovery, and peer events"
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
            peer_store_flush_interval,
            peer_store_max_entries,
            telemetry,
            network_telemetry,
            peer_recovery_timeout,
            dns_head,
            direct_peers,
            trusted_peer_ids,
            peer_refill_interval,
            bootstrap_dns_tree,
            cached_records,
            peer_store,
            qualifications,
            qualification_target,
            request_gate,
            request_timeout,
            material_request_concurrency,
            body_serving_peer_target,
            mut peer_store_flush_requests,
            shutdown,
        } = runtime;
        let mut manager = Box::pin(manager);
        let peer_candidates = Arc::new(PeerCandidateRegistry::new(peer_store_max_entries));
        for peer_id in trusted_peer_ids {
            if peer_candidates.admit(peer_id, NetworkPeerOrigin::Trusted) {
                network_telemetry.peer_candidate_admitted(NetworkPeerOrigin::Trusted);
            }
        }
        let CachedPeerRecords { hot, broad } = cached_records;
        for record in &hot {
            admit_node_record_to_network(
                manager.as_ref().get_ref().handle(),
                *record,
                NetworkPeerOrigin::CachedHot,
                &peer_candidates,
                &network_telemetry,
            );
            connect_node_record(manager.as_ref().get_ref().handle(), *record);
        }
        debug!(
            hot_candidates = hot.len(),
            broad_candidates = broad.len(),
            "started hedged execution-peer store admission"
        );
        let cached_peer_admitter = spawn_cached_peer_admitter(
            manager.as_ref().get_ref().handle().clone(),
            broad,
            body_serving_peer_target,
            peer_refill_interval,
            peer_candidates.clone(),
            network_telemetry.clone(),
        );
        let dns_peer_seeder = spawn_mainnet_dns_peer_seeder(
            manager.as_ref().get_ref().handle().clone(),
            DnsPeerSeederRuntime {
                dns_head,
                bootstrap_dns_tree,
                peer_candidates: peer_candidates.clone(),
                network_telemetry: network_telemetry.clone(),
            },
        );
        let qualification_worker = spawn_peer_qualification_worker(
            direct_peers.clone(),
            qualifications.clone(),
            peer_store.clone(),
            peer_candidates.clone(),
            manager.as_ref().get_ref().handle().clone(),
            network_telemetry.clone(),
            qualification_target,
            request_gate,
            request_timeout,
            material_request_concurrency,
            body_serving_peer_target,
            shutdown.clone(),
        );
        let direct_peer_reconciler = spawn_direct_peer_reconciler(
            manager.as_ref().get_ref().handle().clone(),
            direct_peers.clone(),
            qualifications.clone(),
            request_timeout,
            shutdown.clone(),
        );
        let mut network_events_open = true;
        let mut zero_peers_since = Some(tokio::time::Instant::now());
        let mut peer_store_flush = tokio::time::interval_at(
            tokio::time::Instant::now() + peer_store_flush_interval,
            peer_store_flush_interval,
        );
        peer_store_flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let (persistence_requests, persistence_receiver) = tokio::sync::mpsc::channel(1);
        let persistence_worker =
            spawn_peer_persistence_worker(peer_store.clone(), persistence_receiver);
        let mut telemetry_refresh = tokio::time::interval(Duration::from_secs(1));
        telemetry_refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut peer_store_flush_requests_open = true;
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
                            network_telemetry.peer_session_established_from(
                                peer_candidates.origin(info.peer_id),
                            );
                            direct_peers.insert(DirectPeer {
                                peer_id: info.peer_id,
                                eth_version: info.version,
                                messages,
                            });
                        }
                        Some(NetworkEvent::Peer(PeerEvent::SessionClosed { peer_id, reason })) => {
                            direct_peers.session_closed(peer_id);
                            qualifications.remove(peer_id);
                            let classified = classify_disconnect_reason(reason);
                            if disconnect_invalidates_service_evidence(classified) {
                                peer_store.record_failure(peer_id, classified.as_str());
                            }
                            network_telemetry.peer_session_closed(classified);
                            debug!(
                                reason = classified.as_str(),
                                "execution peer session closed"
                            );
                        }
                        Some(NetworkEvent::Peer(
                            PeerEvent::SessionEstablished(_)
                            | PeerEvent::PeerRemoved(_)
                            | PeerEvent::PeerAdded(_),
                        )) => {}
                        None => network_events_open = false,
                    }
                }
                request = peer_store_flush_requests.recv(), if peer_store_flush_requests_open => {
                    let Some(response) = request else {
                        peer_store_flush_requests_open = false;
                        continue;
                    };
                    let records = manager.as_ref().get_ref().all_peers().collect();
                    let persistence = PeerPersistenceRequest {
                        records,
                        response: Some(response),
                    };
                    if let Err(error) = persistence_requests.send(persistence).await
                        && let Some(response) = error.0.response
                    {
                        let _ = response.send(Err(
                            "execution peer persistence worker stopped unexpectedly".to_owned(),
                        ));
                    }
                }
                _ = peer_store_flush.tick() => {
                    let _ = persistence_requests.try_send(PeerPersistenceRequest {
                        records: manager.as_ref().get_ref().all_peers().collect(),
                        response: None,
                    });
                }
            }
        }
        dns_peer_seeder.abort();
        let _ = dns_peer_seeder.await;
        cached_peer_admitter.abort();
        let _ = cached_peer_admitter.await;
        qualification_worker.abort();
        let _ = qualification_worker.await;
        direct_peer_reconciler.abort();
        let _ = direct_peer_reconciler.await;
        let (response, persisted) = tokio::sync::oneshot::channel();
        let final_request = PeerPersistenceRequest {
            records: manager.as_ref().get_ref().all_peers().collect(),
            response: Some(response),
        };
        if persistence_requests.send(final_request).await.is_ok() {
            match persisted.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => warn!(%error, "failed to persist final execution peer state"),
                Err(error) => {
                    warn!(%error, "execution peer persistence worker stopped before final flush");
                }
            }
        }
        drop(persistence_requests);
        let _ = persistence_worker.await;
        direct_peers.clear();
    })
}

/// One direct-peer pool entry as the session reconciler sees it.
#[derive(Clone, Copy, Debug)]
struct PooledPeerSession {
    peer_id: B512,
    connection_id: u64,
    sender_open: bool,
}

/// Pool entries to drop and Reth sessions to adopt.
#[derive(Debug, Default, Eq, PartialEq)]
struct DirectPeerReconciliation {
    /// Entries, by peer and connection, whose session is gone.
    stale: Vec<(B512, u64)>,
    /// Active sessions the pool does not hold.
    missing: Vec<B512>,
    /// Missing sessions the previous reconciliation found missing too.
    adopt: Vec<B512>,
}

/// Diff the direct-peer pool against Reth's active sessions. An entry is stale
/// once its request sender is closed, or when its session is missing from a
/// snapshot requested after the entry was added; entries added since the
/// snapshot was requested (`snapshot_connection_id` or later) cannot be judged
/// by it. Active sessions without an open entry are missing, and are adopted
/// once the previous reconciliation found them `missing_before` as well: an
/// event that is merely late, or a session that is closing, such as a peer
/// banned since the snapshot, leaves the list by then. A peer dropped for
/// invalid material is never missing while its session lasts: Reth keeps a
/// trusted peer's session despite the ban.
fn reconcile_direct_peers(
    pooled: &[PooledPeerSession],
    active: &HashSet<B512>,
    snapshot_connection_id: u64,
    missing_before: &HashSet<B512>,
    invalidated: &HashSet<B512>,
) -> DirectPeerReconciliation {
    let stale = pooled
        .iter()
        .filter(|session| {
            !session.sender_open
                || (session.connection_id < snapshot_connection_id
                    && !active.contains(&session.peer_id))
        })
        .map(|session| (session.peer_id, session.connection_id))
        .collect();
    let open = pooled
        .iter()
        .filter(|session| session.sender_open)
        .map(|session| session.peer_id)
        .collect::<HashSet<_>>();
    let mut missing = active
        .iter()
        .filter(|peer_id| !open.contains(*peer_id) && !invalidated.contains(*peer_id))
        .copied()
        .collect::<Vec<_>>();
    missing.sort_unstable();
    let adopt = missing
        .iter()
        .filter(|peer_id| missing_before.contains(*peer_id))
        .copied()
        .collect();
    DirectPeerReconciliation {
        stale,
        missing,
        adopt,
    }
}

/// A request sender for a session whose `ActivePeerSession` event was lost.
/// Its requests reach that session through the network manager, which drops
/// them, and so fails the request, once the session is gone.
fn manager_routed_peer_sender(
    handle: NetworkHandle<EthNetworkPrimitives>,
    peer_id: B512,
) -> PeerRequestSender<PeerRequest<EthNetworkPrimitives>> {
    let (sender, mut requests) = tokio::sync::mpsc::channel(MAX_MATERIAL_REQUESTS_PER_PEER);
    // The forwarder ends once the pool and every lease drop the sender.
    drop(tokio::spawn(async move {
        while let Some(request) = requests.recv().await {
            handle.send_request(peer_id, request);
        }
    }));
    PeerRequestSender::new(peer_id, sender)
}

/// Keep the direct-peer pool in step with Reth's active sessions: drop entries
/// whose session is gone and adopt sessions whose open event was lost.
fn spawn_direct_peer_reconciler(
    handle: NetworkHandle<EthNetworkPrimitives>,
    direct_peers: Arc<DirectPeerPool>,
    qualifications: Arc<PeerQualificationPool>,
    request_timeout: Duration,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut reconcile = tokio::time::interval_at(
            tokio::time::Instant::now() + DIRECT_PEER_RECONCILE_INTERVAL,
            DIRECT_PEER_RECONCILE_INTERVAL,
        );
        reconcile.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut missing_before = HashSet::new();
        loop {
            tokio::select! {
                () = shutdown.cancelled() => break,
                _ = reconcile.tick() => {}
            }
            // Entries added from here on may postdate the session snapshot.
            let snapshot_connection_id = direct_peers.next_connection_id.load(Ordering::Relaxed);
            let sessions = match cancellable_timeout(
                handle.get_all_peers(),
                request_timeout,
                &shutdown,
                "peer sessions",
            )
            .await
            {
                Ok(sessions) => sessions,
                Err(P2pError::Cancelled) => break,
                Err(error) => {
                    debug!(%error, "could not list execution peer sessions for reconciliation");
                    continue;
                }
            };
            let active = sessions
                .iter()
                .map(|session| session.remote_id)
                .collect::<HashSet<_>>();
            let reconciliation = reconcile_direct_peers(
                &direct_peers.sessions(),
                &active,
                snapshot_connection_id,
                &missing_before,
                &direct_peers.invalidated(),
            );
            missing_before = reconciliation.missing.iter().copied().collect();
            let mut dropped = 0_usize;
            for (peer_id, connection_id) in reconciliation.stale {
                if direct_peers.remove_connection(peer_id, connection_id) {
                    qualifications.remove(peer_id);
                    dropped = dropped.saturating_add(1);
                }
            }
            let mut adopted = 0_usize;
            for session in sessions
                .into_iter()
                .filter(|session| reconciliation.adopt.contains(&session.remote_id))
            {
                if direct_peers.insert_missing(DirectPeer {
                    peer_id: session.remote_id,
                    eth_version: session.eth_version,
                    messages: manager_routed_peer_sender(handle.clone(), session.remote_id),
                }) {
                    adopted = adopted.saturating_add(1);
                }
            }
            if dropped > 0 || adopted > 0 {
                debug!(
                    dropped,
                    adopted, "reconciled direct execution peers with active sessions"
                );
            }
        }
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
    peer_store_flush_interval: Duration,
    peer_store_max_entries: usize,
    telemetry: NetworkSessionTelemetry,
    network_telemetry: NetworkTelemetry,
    peer_recovery_timeout: Duration,
    dns_head: Head,
    direct_peers: Arc<DirectPeerPool>,
    trusted_peer_ids: Vec<B512>,
    peer_refill_interval: Duration,
    bootstrap_dns_tree: Option<String>,
    cached_records: CachedPeerRecords,
    peer_store: Arc<ExecutionPeerStore>,
    qualifications: Arc<PeerQualificationPool>,
    qualification_target: tokio::sync::watch::Receiver<BlockRef>,
    request_gate: Arc<MaterialRequestGate>,
    request_timeout: Duration,
    material_request_concurrency: usize,
    body_serving_peer_target: usize,
    peer_store_flush_requests:
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

/// Only a protocol fault the peer is responsible for invalidates its stored
/// service evidence. `UselessPeer` is as often a remote peer dropping this
/// node, which serves no chain data, and TCP subsystem errors are often local
/// or transient, so neither is held against the peer.
const fn disconnect_invalidates_service_evidence(reason: NetworkDisconnectReason) -> bool {
    matches!(
        reason,
        NetworkDisconnectReason::ProtocolBreach
            | NetworkDisconnectReason::UnexpectedHandshakeIdentity
    )
}

#[derive(Debug, Default)]
struct CachedPeerRecords {
    hot: Vec<NodeRecord>,
    broad: Vec<NodeRecord>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum PeerSubnet {
    Ipv4([u8; 2]),
    Ipv6([u8; 4]),
}

fn peer_subnet(address: IpAddr) -> PeerSubnet {
    match address {
        IpAddr::V4(address) => {
            let octets = address.octets();
            PeerSubnet::Ipv4([octets[0], octets[1]])
        }
        IpAddr::V6(address) => {
            let octets = address.octets();
            PeerSubnet::Ipv6([octets[0], octets[1], octets[2], octets[3]])
        }
    }
}

fn prioritized_peer_cache_records(
    store: &ExecutionPeerStore,
    hot_limit: usize,
) -> CachedPeerRecords {
    let records = store.candidates();

    // The immediate-dial tier contains only peers that served a verified body
    // after their most recent recorded failure. Select distinct /16 (IPv4) or
    // /32 (IPv6) networks first so one operator cannot occupy the whole hedge.
    let eligible = records
        .iter()
        .copied()
        .filter(|record| store.is_available_body_server(record.id))
        .collect::<Vec<_>>();
    let mut hot = Vec::with_capacity(hot_limit.min(eligible.len()));
    let mut hot_ids = HashSet::new();
    let mut subnets = HashSet::new();
    for record in &eligible {
        if hot.len() >= hot_limit {
            break;
        }
        if subnets.insert(peer_subnet(record.address)) {
            hot.push(*record);
            hot_ids.insert(record.id);
        }
    }
    for record in eligible {
        if hot.len() >= hot_limit {
            break;
        }
        if hot_ids.insert(record.id) {
            hot.push(record);
        }
    }
    let broad = records
        .into_iter()
        .filter(|record| !hot_ids.contains(&record.id))
        .collect();
    CachedPeerRecords { hot, broad }
}

fn configured_secret_key_path(config: &RethP2pConfig) -> Option<PathBuf> {
    config.secret_key_path.clone().or_else(|| {
        config
            .peer_store_path
            .as_ref()
            .map(|path| path.with_file_name("execution-p2p-secret"))
    })
}

fn load_or_create_secret_key(path: Option<&Path>) -> Result<SecretKey, P2pError> {
    let Some(path) = path else {
        return Ok(rng_secret_key());
    };
    match std::fs::read_to_string(path) {
        Ok(encoded) => {
            restrict_secret_key_permissions(path)?;
            return parse_secret_key(encoded.trim(), path);
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(P2pError::InvalidConfig(format!(
                "failed reading execution P2P identity {}: {error}",
                path.display()
            )));
        }
    }
    let directory = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(directory).map_err(|error| {
        P2pError::InvalidConfig(format!(
            "failed creating execution P2P identity directory {}: {error}",
            directory.display()
        ))
    })?;
    let secret = rng_secret_key();
    let encoded = hex::encode(secret.secret_bytes());
    let sequence = SECRET_KEY_WRITE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
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
    // The temporary file is ours from here on: remove it on every failure.
    let installed = file
        .write_all(encoded.as_bytes())
        .map_err(|error| {
            P2pError::InvalidConfig(format!(
                "failed writing execution P2P identity {}: {error}",
                temporary.display()
            ))
        })
        .and_then(|()| {
            file.sync_all().map_err(|error| {
                P2pError::InvalidConfig(format!(
                    "failed syncing execution P2P identity {}: {error}",
                    temporary.display()
                ))
            })
        })
        .and_then(|()| {
            std::fs::rename(&temporary, path).map_err(|error| {
                P2pError::InvalidConfig(format!(
                    "failed installing execution P2P identity {}: {error}",
                    path.display()
                ))
            })
        });
    drop(file);
    if let Err(error) = installed {
        let _ = std::fs::remove_file(&temporary);
        return Err(error);
    }
    // A crash must not undo the rename after the identity is in use.
    #[cfg(unix)]
    std::fs::File::open(directory)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| {
            P2pError::InvalidConfig(format!(
                "failed syncing execution P2P identity directory {}: {error}",
                directory.display()
            ))
        })?;
    Ok(secret)
}

/// Restrict an existing identity file that other users can access to 0600:
/// the key authenticates this node on the execution network.
#[cfg(unix)]
fn restrict_secret_key_permissions(path: &Path) -> Result<(), P2pError> {
    use std::os::unix::fs::PermissionsExt as _;

    let mode = std::fs::metadata(path)
        .map_err(|error| {
            P2pError::InvalidConfig(format!(
                "failed reading execution P2P identity permissions {}: {error}",
                path.display()
            ))
        })?
        .permissions()
        .mode()
        & 0o777;
    let group_or_other = mode & 0o077;
    if group_or_other == 0 {
        return Ok(());
    }
    warn!(
        path = %path.display(),
        mode = %format!("{mode:03o}"),
        "execution P2P identity is accessible to other users; restricting it to 0600"
    );
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).map_err(|error| {
        P2pError::InvalidConfig(format!(
            "failed restricting execution P2P identity {} to 0600: {error}",
            path.display()
        ))
    })
}

#[cfg(not(unix))]
fn restrict_secret_key_permissions(_path: &Path) -> Result<(), P2pError> {
    Ok(())
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
    /// The newest sync-committee-attested head: the lane includes no block
    /// above it.
    attested: AttestedHeadReceiver,
    /// Proven hashes of the attested head's ancestry above the tip.
    ancestry: AttestedAncestry,
    pending_material_attempts: usize,
    head_poll: HeadPollRotation,
    /// Since when no attested head has been above the tip.
    head_unavailable_since: Option<Instant>,
    /// Since when every head poll's cohort has stayed silent, counted from
    /// the lane's last progress at the earliest.
    head_poll_silent_since: Option<Instant>,
    reconnect_error: Option<String>,
    /// Whether the lane has reported itself disconnected since it last
    /// emitted a block or reorg, or reconnected.
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
            .field("attested", &self.attested.peek())
            .field("pending_material_attempts", &self.pending_material_attempts)
            .field("head_unavailable_since", &self.head_unavailable_since)
            .field("head_poll_silent_since", &self.head_poll_silent_since)
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
            config.body_serving_peer_target,
            config.preferred_peers,
            config.max_outbound_peers,
            config.max_concurrent_dials,
        );
        let peer_store = Arc::new(ExecutionPeerStore::new(
            config.peer_store_path.clone(),
            config.peer_store_max_entries,
        ));
        let initial_target = Self::mainnet_genesis_block();
        let network = Arc::new(PersistentNetwork {
            state: tokio::sync::Mutex::new(None),
            next_generation: AtomicU64::new(1),
            // One global limit for every lane: history header proofs may use
            // their own configured concurrency, bodies and receipts theirs.
            request_gate: Arc::new(MaterialRequestGate::new(
                config
                    .material_request_concurrency
                    .max(config.history_header_request_concurrency),
            )),
            direct_peers: Arc::new(DirectPeerPool::new(peer_store.clone())),
            peer_store,
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
                finality: FinalityModel::Included,
                partitioning: Partitioning::FixedBlockSpan(MAX_FIXED_RANGE_BLOCKS),
                expected_lag: Duration::from_secs(12),
                schema_version: format!("reth-p2p.v1+{RETH_VERSION}.{RETH_REVISION}"),
                priority: 0,
            },
            config,
            network,
            material_tuning,
            request_metrics: P2pRequestMetrics::default(),
            attested_heads: None,
        })
    }

    /// Follow the sync-committee-attested heads a verified finality source
    /// publishes. The live lane includes no block above the newest one, and
    /// a source without them refuses live subscriptions. The source only
    /// reads the heads: it cannot publish one.
    #[must_use]
    pub fn with_attested_heads(mut self, heads: AttestedHeadReceiver) -> Self {
        self.attested_heads = Some(heads);
        self
    }

    /// The attested heads a live subscription follows. Without them nothing
    /// bounds what the lane includes, so the subscription fails closed.
    fn live_attested_heads(&self) -> Result<AttestedHeadReceiver, SourceError> {
        self.attested_heads.clone().ok_or_else(|| {
            SourceError::InvalidPlan(
                "execution P2P live ingestion follows sync-committee-attested heads, but no verified finality source publishes them"
                    .to_owned(),
            )
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
            running.qualification_target.send_if_modified(|current| {
                if same_qualification_target(*current, head) {
                    false
                } else {
                    *current = head;
                    true
                }
            });
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

    /// Gracefully stop the shared execution network and flush its peer store.
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
                "peer preview requires a block-summary or compatible filtered-log request"
                    .to_owned(),
            ));
        }
        if request.chain_id != self.descriptor.chain_id {
            return Err(P2pError::InvalidConfig(
                "peer preview request belongs to another chain".to_owned(),
            ));
        }
        if sparse_scope.is_some()
            && request
                .log_fields
                .contains(leani_primitives::LogField::TransactionHash)
        {
            return Err(P2pError::InvalidConfig(
                "receipt-only peer preview cannot supply transaction hashes".to_owned(),
            ));
        }
        let (session, _) = self
            .connect(advertised, NetworkLane::Live, cancellation)
            .await?;
        // A preview is a peer claim: nothing here is anchored.
        let (head_number, head_hash) = self
            .wait_for_peer_head(
                &session,
                BlockNumber(advertised.number.0.saturating_add(1)),
                None,
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
                // The head was discovered from peer claims.
                Some(ExpectedTip {
                    hash: head_hash,
                    trust: ExpectationTrust::Unverified,
                }),
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
            P2pError::InvalidConfig("peer preview filtered-log scope disappeared".to_owned())
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
            P2pError::InvalidResponse("peer preview request returned no header".to_owned())
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
            P2pError::InvalidResponse("peer preview request returned no frame".to_owned())
        })?;
        frame.finality = Finality::Included;
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
            .fetch_live_body_frames(session, headers, preferred_peer, true, budget, cancellation)
            .await?;
        let mut frame = frames.pop().ok_or_else(|| {
            P2pError::InvalidResponse("peer preview request returned no frame".to_owned())
        })?;
        frame.finality = Finality::Included;
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
                .acquire(
                    PeerMaterialKind::Receipts,
                    per_peer_limit,
                    self.config.request_timeout,
                    cancellation,
                )
                .await?;
            let peer = lease.peer.peer_id;
            let permit = self
                .network
                .request_gate
                .acquire(policy.priority, cancellation)
                .await?;
            self.config
                .network_telemetry
                .request_started(queued_at.elapsed());
            let request_started_at = Instant::now();
            let response = request_direct_receipts(
                &lease.peer,
                headers,
                hashes,
                None,
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
                            self.network.peer_store.record_success(
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
                            self.config.network_telemetry.request_failed();
                            self.request_metrics.record_batch(
                                P2pRequestKind::Receipts,
                                response.physical_requests.max(1),
                                response.requested_block_hashes,
                                response.receipts.len(),
                                request_started_at.elapsed(),
                                P2pRequestOutcome::Failed,
                            );
                            // Receipts requested by hash answer to their
                            // header's commitments, a verified expectation.
                            let fault = self.network.penalize_response(
                                &mut lease,
                                &error,
                                ExpectationTrust::Verified,
                                |peer_id| session.handle.ban_peer(peer_id),
                            );
                            if fault == ResponseFault::Disagreement
                                && matches!(error, P2pError::IncompleteResponse { .. })
                                && repeated_incomplete_response(
                                    &mut incomplete_responses,
                                    peer,
                                    self.config.retries,
                                )
                            {
                                debug!(
                                    attempts = self.config.retries,
                                    "keeping execution peer receipt lane cooled after repeated incomplete sparse responses"
                                );
                                incomplete_responses.remove(&peer);
                            }
                            last_error = Some(error);
                        }
                    }
                }
                Err(P2pError::Cancelled) => return Err(P2pError::Cancelled),
                Err(error) => {
                    // An eth/70 reply is checked against its headers while it is
                    // assembled, so its invalid receipts fail the request itself.
                    self.network.penalize_response(
                        &mut lease,
                        &error,
                        ExpectationTrust::Verified,
                        |peer_id| session.handle.ban_peer(peer_id),
                    );
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
        // Cached peers are admitted through the two-tier startup policy after
        // the manager is built. Loading the whole store through Reth as well would
        // enqueue the entire broad cache immediately and defeat the fresh-peer
        // hedge.
        PeersConfig::default()
            .with_max_outbound(self.config.max_outbound_peers)
            .with_max_concurrent_dials(self.config.max_concurrent_dials)
            .with_refill_slots_interval(self.config.peer_refill_interval)
            .with_trusted_nodes(self.config.trusted_peers.clone())
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
        let already_connected = self.network.direct_peers.len();
        if !newly_started && already_connected >= self.config.minimum_peers {
            session.set_phase(NetworkPhase::Ready);
            session.clear_error();
            return Ok((session, already_connected));
        }
        session.set_phase(NetworkPhase::WaitingForPeers);
        let connected = match wait_for_connected_peers(
            &session,
            &self.network.direct_peers,
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
        self.network
            .peer_store
            .initialize()
            .await
            .map_err(|error| P2pError::InvalidConfig(error.to_string()))?;
        let mut state = self.network.state.lock().await;
        if state
            .as_ref()
            .is_some_and(|running| running.network_task.is_finished())
        {
            state.take();
        }
        if let Some(running) = state.as_ref() {
            running.handle.update_status(block_status_head(advertised));
            running.qualification_target.send_if_modified(|current| {
                if same_qualification_target(*current, advertised) {
                    false
                } else {
                    *current = advertised;
                    true
                }
            });
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
            // Do not leave an unresponsive TCP/RLPx handshake occupying one
            // of the bounded dial slots for Reth's 20-second default. The
            // operator-configured peer request deadline is also the maximum
            // time Leani is willing to spend establishing that peer.
            pending_session_timeout: self.config.request_timeout,
            ..SessionsConfig::default().with_upscaled_event_buffer(peers.max_peers())
        };
        let mut builder =
            NetworkConfigBuilder::<EthNetworkPrimitives>::new(secret, Runtime::test())
                .listener_addr(listener_addr)
                .discovery_addr(discovery_addr)
                .disable_tx_gossip(true)
                .mainnet_boot_nodes()
                // Leani's own EIP-1459 seeder below decodes the DNS tree and
                // admits its records through the peer manager (ADR 0015). It
                // joins multi-segment TXT records, which Reth 2.4.1 read only
                // in part; the pinned Reth resolver joins them too.
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
        self.network.qualifications.reset(advertised);
        let (qualification_target, qualification_updates) = tokio::sync::watch::channel(advertised);
        let (peer_store_flush, peer_store_flush_requests) = tokio::sync::mpsc::channel(1);
        let cached_records = prioritized_peer_cache_records(
            &self.network.peer_store,
            self.config
                .body_serving_peer_target
                .min(self.config.max_concurrent_dials),
        );
        let network_task = spawn_network_manager(
            manager,
            network_events,
            NetworkManagerRuntime {
                peer_store_flush_interval: self.config.peer_store_flush_interval,
                peer_store_max_entries: self.config.peer_store_max_entries,
                telemetry: telemetry.clone(),
                network_telemetry: self.config.network_telemetry.clone(),
                peer_recovery_timeout: self.config.peer_recovery_timeout,
                dns_head: advertised_head,
                direct_peers: self.network.direct_peers.clone(),
                trusted_peer_ids: self
                    .config
                    .trusted_peers
                    .iter()
                    .map(|peer| peer.id)
                    .collect(),
                peer_refill_interval: self.config.peer_refill_interval,
                bootstrap_dns_tree: self.config.bootstrap_dns_tree.clone(),
                cached_records,
                peer_store: self.network.peer_store.clone(),
                qualifications: self.network.qualifications.clone(),
                qualification_target: qualification_updates,
                request_gate: self.network.request_gate.clone(),
                request_timeout: self.config.request_timeout,
                material_request_concurrency: self.config.material_request_concurrency,
                body_serving_peer_target: self.config.body_serving_peer_target,
                peer_store_flush_requests,
                shutdown: shutdown.clone(),
            },
        );
        let running = PersistentNetworkState {
            generation,
            handle: handle.clone(),
            fetch: fetch.clone(),
            peer_store_flush,
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

    /// Bodies and receipts of validated live `headers`, which `header_peer`
    /// served, normalized into frames, with the peers that served them.
    async fn fetch_verified_material(
        &self,
        session: &P2pSession,
        headers: &[Header],
        header_peer: B512,
        budget: SourceBudget,
        cancellation: &CancellationToken,
    ) -> Result<(Vec<BlockFrame>, HashSet<B512>), P2pError> {
        let hashes = headers.iter().map(Sealable::hash_slow).collect::<Vec<_>>();
        session.set_phase(NetworkPhase::FetchingBodies);
        // Live material, like the rest of the live lane, may use the request
        // slots reserved for live requests.
        let material_policy = MaterialRequestPolicy {
            concurrency: budget.max_in_flight_requests,
            priority: Priority::High,
        };
        let (body_peers, bodies) = if let ([header], [hash]) = (headers, hashes.as_slice()) {
            self.fetch_live_body_from_untried_peers(
                session,
                header,
                *hash,
                Some(header_peer),
                false,
                material_policy,
                cancellation,
            )
            .await
            .map(|(peer, body)| (HashSet::from([peer]), vec![body]))?
        } else {
            self.fetch_bodies_batched(
                &session.fetch,
                headers,
                &hashes,
                material_policy,
                cancellation,
            )
            .await?
        };
        let preferred_receipt_peer = body_peers.iter().next().copied();
        session.set_phase(NetworkPhase::FetchingReceipts);
        let (receipt_peers, receipts) = if let ([header], [body], [hash]) =
            (headers, bodies.as_slice(), hashes.as_slice())
        {
            self.fetch_live_receipts_from_untried_peers(
                session,
                LiveReceiptMaterial {
                    header,
                    body,
                    hash: *hash,
                    header_peer,
                    preferred_peer: preferred_receipt_peer.expect("one live body response peer"),
                },
                material_policy,
                cancellation,
            )
            .await
            .map(|(peer, block_receipts)| (peer.into_iter().collect(), vec![block_receipts]))?
        } else {
            self.fetch_receipts_batched(
                session,
                headers,
                &bodies,
                &hashes,
                material_policy,
                cancellation,
            )
            .await?
        };
        let frames = normalize_verified(headers, &bodies, &receipts, budget)?;
        let mut response_peers = body_peers;
        response_peers.extend(receipt_peers);
        Ok((frames, response_peers))
    }

    async fn fetch_requested_live_range(
        &self,
        session: &P2pSession,
        range: BlockRange,
        expected_tip: Option<ExpectedTip>,
        request: &DataRequest,
        budget: SourceBudget,
        cancellation: &CancellationToken,
    ) -> Result<(Vec<BlockFrame>, usize), P2pError> {
        session.set_range(Some(range));
        session.set_phase(NetworkPhase::FetchingHeaders);
        let (header_serve, headers) = self
            .fetch_live_headers_from_untried_peers(
                session,
                range,
                expected_tip,
                None,
                MaterialRequestPolicy {
                    concurrency: budget.max_in_flight_requests,
                    priority: Priority::High,
                },
                cancellation,
            )
            .await
            .inspect_err(|error| session.record_error(error))?;
        self.fetch_live_range_material(
            session,
            range,
            &headers,
            header_serve.peer_id,
            Some(header_serve),
            request,
            budget,
            cancellation,
        )
        .await
    }

    /// The frames of the validated live `headers` of `range`, with the
    /// material `request` needs. `header_peer` served the headers, and
    /// `settle`, if any, is its serve, settled once the frames complete.
    /// Returns the frames and how many peers served them.
    #[allow(clippy::too_many_arguments)]
    async fn fetch_live_range_material(
        &self,
        session: &P2pSession,
        range: BlockRange,
        headers: &[Header],
        header_peer: B512,
        settle: Option<HeaderServe>,
        request: &DataRequest,
        budget: SourceBudget,
        cancellation: &CancellationToken,
    ) -> Result<(Vec<BlockFrame>, usize), P2pError> {
        session.set_range(Some(range));
        let request = DataRequest {
            range,
            ..request.clone()
        };
        let (frames, mut response_peers) = if header_only_request(&request) {
            (normalize_verified_headers(headers, budget)?, HashSet::new())
        } else if header_and_body_only_request(&request) {
            self.fetch_live_body_frames(
                session,
                headers,
                Some(header_peer),
                false,
                budget,
                cancellation,
            )
            .await
            .inspect_err(|error| session.record_error(error))?
        } else if sparse_log_scope(&request).is_some()
            && !request
                .log_fields
                .contains(leani_primitives::LogField::TransactionHash)
        {
            (
                self.fetch_sparse_live_frames(
                    session,
                    headers,
                    header_peer,
                    &request,
                    budget,
                    cancellation,
                )
                .await
                .inspect_err(|error| session.record_error(error))?,
                HashSet::new(),
            )
        } else {
            self.fetch_verified_material(session, headers, header_peer, budget, cancellation)
                .await
                .inspect_err(|error| session.record_error(error))?
        };
        if let Some(serve) = settle {
            self.complete_header_serve(session, serve).await;
        }
        response_peers.insert(header_peer);
        session.clear_error();
        Ok((frames, response_peers.len()))
    }

    async fn fetch_live_body_frames(
        &self,
        session: &P2pSession,
        headers: &[Header],
        preferred_peer: Option<B512>,
        unanchored: bool,
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
                unanchored,
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
        header_peer: B512,
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
        let positive_receipts = if let Some(first) = positive_headers.first() {
            session.set_phase(NetworkPhase::FetchingReceipts);
            // Like the live body and receipt requests, the lane's filtered-log
            // receipt request gives its header up once no peer has served the
            // receipts within the live material bound.
            let mut bound = LiveMaterialBound::new("receipts", first.number, Some(header_peer));
            let deadline = bound.deadline();
            let waves = self.live_sparse_receipt_waves(
                session,
                &positive_headers,
                &positive_hashes,
                &mut bound,
                MaterialRequestPolicy {
                    concurrency: budget.max_in_flight_requests,
                    priority: Priority::High,
                },
                cancellation,
            );
            let outcome = tokio::time::timeout_at(deadline, waves).await;
            outcome.unwrap_or_else(|_| Err(self.live_material_deadline_passed(&bound)))?
        } else {
            Vec::new()
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

    /// Ask for the receipts of the lane's filtered-log blocks in waves, each a
    /// batched request with its own retries, until one serves them.
    async fn live_sparse_receipt_waves(
        &self,
        session: &P2pSession,
        headers: &[Header],
        hashes: &[B256],
        bound: &mut LiveMaterialBound,
        policy: MaterialRequestPolicy,
        cancellation: &CancellationToken,
    ) -> Result<Vec<Vec<Receipt>>, P2pError> {
        loop {
            match self
                .fetch_sparse_receipts_batched(session, headers, hashes, policy, cancellation)
                .await
            {
                Ok(receipts) => return Ok(receipts),
                Err(error) => {
                    self.end_live_material_wave(bound, error, cancellation)
                        .await?;
                }
            }
        }
    }

    async fn normalize_polled_sparse_live_frame(
        &self,
        session: &P2pSession,
        header: Header,
        header_peer: B512,
        request: &DataRequest,
        budget: SourceBudget,
        cancellation: &CancellationToken,
    ) -> Result<Option<BlockFrame>, P2pError> {
        let next = BlockNumber(header.number);
        let request = DataRequest {
            range: BlockRange::single(next),
            ..request.clone()
        };
        let mut frames = self
            .fetch_sparse_live_frames(
                session,
                &[header],
                header_peer,
                &request,
                budget,
                cancellation,
            )
            .await
            .inspect_err(|error| session.record_error(error))?;
        session.clear_error();
        session.observe_head(next);
        session.set_phase(NetworkPhase::FollowingHead);
        Ok(frames.pop())
    }

    /// Poll the block `next`, the attested head, from a small cohort of
    /// peers, and check its header against the head's hash, `expected_tip`.
    #[allow(clippy::too_many_lines, clippy::too_many_arguments)]
    async fn poll_next_verified_frame(
        &self,
        session: &P2pSession,
        next: BlockNumber,
        expected_tip: ExpectedTip,
        request: &DataRequest,
        budget: SourceBudget,
        head_poll: &mut HeadPollRotation,
        cancellation: &CancellationToken,
    ) -> Result<Option<BlockFrame>, P2pError> {
        session.set_range(Some(BlockRange::single(next)));
        session.set_phase(NetworkPhase::FetchingHeaders);
        let range = BlockRange::single(next);
        let (header_serve, mut headers) = self
            .fetch_live_headers_from_untried_peers(
                session,
                range,
                Some(expected_tip),
                Some(head_poll),
                MaterialRequestPolicy {
                    concurrency: budget.max_in_flight_requests,
                    priority: Priority::High,
                },
                cancellation,
            )
            .await
            .inspect_err(|error| session.record_error(error))?;
        let header_peer = header_serve.peer_id;
        let header = headers
            .pop()
            .expect("one validated live header was returned");
        if header_only_request(request) {
            let mut frames = normalize_verified_headers(&[header], budget)?;
            self.complete_header_serve(session, header_serve).await;
            session.clear_error();
            session.observe_head(next);
            session.set_phase(NetworkPhase::FollowingHead);
            return Ok(frames.pop());
        }
        if header_and_body_only_request(request) {
            let (mut frames, _) = self
                .fetch_live_body_frames(
                    session,
                    &[header],
                    Some(header_peer),
                    false,
                    budget,
                    cancellation,
                )
                .await
                .inspect_err(|error| session.record_error(error))?;
            self.complete_header_serve(session, header_serve).await;
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
            let frame = self
                .normalize_polled_sparse_live_frame(
                    session,
                    header,
                    header_peer,
                    request,
                    budget,
                    cancellation,
                )
                .await?;
            self.complete_header_serve(session, header_serve).await;
            return Ok(frame);
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
                false,
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
                    header_peer,
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
        self.complete_header_serve(session, header_serve).await;
        session.clear_error();
        session.observe_head(next);
        session.set_phase(NetworkPhase::FollowingHead);
        Ok(frames.pop())
    }

    async fn connect_and_fetch_requested_live_range(
        &self,
        advertised: BlockRef,
        range: BlockRange,
        expected_tip: Option<ExpectedTip>,
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
            // The range ends at `advertised`: a verified tip is its hash.
            let known = expected_tip
                .filter(|tip| tip.trust == ExpectationTrust::Verified)
                .map(|tip| tip.hash);
            if let Err(error) = self
                .wait_for_peer_head(&session, advertised.number, known, cancellation)
                .await
            {
                if matches!(error, P2pError::Cancelled) {
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
                Err(P2pError::Cancelled) => return Err(P2pError::Cancelled),
                Err(error) => {
                    session.record_attempt();
                    session.record_error(&error);
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

    /// Find a peer head at or above `minimum`, whose hash is `known` when a
    /// consensus-verified block there is known.
    async fn discover_peer_head(
        &self,
        session: &P2pSession,
        minimum: BlockNumber,
        known: Option<BlockHash>,
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
        let declared = declared_peer_heads(
            peers.iter().map(|peer| {
                (
                    peer.remote_id,
                    peer.status.latest_block,
                    peer.status.blockhash,
                )
            }),
            minimum,
        );
        if let Some((number, hash)) = declared.head {
            return Ok(settle_discovered_head(
                &session.telemetry,
                DiscoveredHead::Claimed(number, hash),
            ));
        }
        let unknown_hashes = declared.hash_only;
        // Asking for the exact minimum first uses the dynamic direct-peer
        // scheduler: the minimum exists, so the request races every eligible
        // peer, and newly established sessions join it while older peers are
        // still pending. Resolving status-only head hashes remains a fallback
        // for peers that cannot serve the minimum by number.
        if let Some(((number, hash), serving_peer)) = self
            .minimum_live_head(session, minimum, known, cancellation)
            .await?
        {
            let validated =
                settle_discovered_head(&session.telemetry, DiscoveredHead::Validated(number, hash));
            if let Some((_, advertised_hash)) = unknown_hashes
                .iter()
                .find(|(peer_id, _)| *peer_id == serving_peer)
                && let Some((number, hash)) = self
                    .resolve_advertised_peer_head(
                        session,
                        vec![(serving_peer, *advertised_hash)],
                        minimum,
                        cancellation,
                    )
                    .await?
            {
                return Ok(settle_discovered_head(
                    &session.telemetry,
                    DiscoveredHead::Claimed(number, hash),
                ));
            }
            return Ok(validated);
        }

        if let Some((number, hash)) = self
            .resolve_advertised_peer_head(session, unknown_hashes, minimum, cancellation)
            .await?
        {
            return Ok(settle_discovered_head(
                &session.telemetry,
                DiscoveredHead::Claimed(number, hash),
            ));
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
                    .map(|(peer, connection_id)| (peer_id, connection_id, hash, peer))
            })
            .collect::<Vec<_>>();
        let concurrency = candidates.len();
        if concurrency == 0 {
            return Ok(None);
        }
        let timeout = self.config.request_timeout;
        let mut pending = stream::iter(candidates.into_iter().map(
            |(peer_id, connection_id, hash, peer)| async move {
                (
                    peer_id,
                    connection_id,
                    hash,
                    request_direct_header(&peer, hash, timeout, cancellation).await,
                )
            },
        ))
        .buffer_unordered(concurrency);
        while let Some((peer_id, connection_id, hash, result)) = pending.next().await {
            let headers = match result {
                Ok(headers) => headers,
                Err(P2pError::Cancelled) => return Err(P2pError::Cancelled),
                Err(error) => {
                    debug!(%error, "could not resolve the head advertised by an execution peer");
                    self.network
                        .direct_peers
                        .record_material_failure(peer_id, PeerMaterialKind::Header);
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
                let error = P2pError::InvalidResponse(
                    "peer did not serve its advertised execution head".to_owned(),
                );
                // A header requested by hash answers to that hash, a verified
                // expectation.
                if classify_response_failure(&error, ExpectationTrust::Verified)
                    == ResponseFault::Invalid
                {
                    session.handle.ban_peer(peer_id);
                    self.network
                        .invalidate_peer(peer_id, connection_id, &error.to_string());
                }
            } else if header.number >= minimum.0 {
                // The peer's own header proves only its claim: it steers the
                // next request but is not observed as a verified head.
                return Ok(Some((BlockNumber(header.number), BlockHash::new(hash.0))));
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
        known: Option<BlockHash>,
        cancellation: &CancellationToken,
    ) -> Result<Option<((BlockNumber, BlockHash), B512)>, P2pError> {
        // Some ETH peers expose only a head hash, and the direct status event
        // can race the request sender becoming visible locally. A header at
        // the consensus-required minimum is enough to start the anchored live
        // lane; subsequent polling advances it without trusting peer status.
        let range = BlockRange::single(minimum);
        let (serve, mut headers) = match self
            .fetch_live_headers_from_untried_peers(
                session,
                range,
                // A consensus-verified block at the minimum, such as the
                // finalized anchor or the attested head, is checked: another
                // header there is invalid.
                known.map(|hash| ExpectedTip {
                    hash,
                    trust: ExpectationTrust::Verified,
                }),
                // The minimum is the verified tip or the block after the
                // finalized anchor: it exists, so this is no head poll, and an
                // empty reply is a lagging peer.
                None,
                minimum_live_head_policy(),
                cancellation,
            )
            .await
        {
            Ok(response) => response,
            Err(P2pError::Cancelled) => return Err(P2pError::Cancelled),
            Err(_) => return Ok(None),
        };
        // The header is the whole reply. No frame follows it, so it clears no
        // withheld-header strike, and only an anchored one earns the reward.
        self.reward_header_serve(session, serve).await;
        let header = headers
            .pop()
            .expect("one validated minimum live header was returned");
        Ok(Some((
            (minimum, BlockHash::new(header.hash_slow().0)),
            serve.peer_id,
        )))
    }

    async fn wait_for_peer_head(
        &self,
        session: &P2pSession,
        minimum: BlockNumber,
        known: Option<BlockHash>,
        cancellation: &CancellationToken,
    ) -> Result<(BlockNumber, BlockHash), P2pError> {
        let mut attempts = 0_usize;
        loop {
            attempts = attempts.saturating_add(1);
            match self
                .discover_peer_head(session, minimum, known, cancellation)
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
                    // A session whose manager the zero-peer watchdog stopped
                    // never finds a head: the caller connects again.
                    if !session.manager_is_current().await {
                        return Err(P2pError::Network(
                            "execution P2P manager restarted while waiting for a peer head"
                                .to_owned(),
                        ));
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Prove the chain of the attested `head` down to `from`, keeping the
    /// lowest window of headers for catch-up (see
    /// [`prove_attested_ancestry`]).
    ///
    /// Each window is requested by the hash of its top block, so a peer that
    /// serves another header for it breaks the protocol and is banned, while
    /// one that lacks the block answers with none and is not. The blocks
    /// exist, so a few peers race for each window, as for the minimum live
    /// head.
    async fn anchor_attested_ancestry(
        &self,
        session: &P2pSession,
        from: BlockNumber,
        head: AttestedHead,
        cancellation: &CancellationToken,
    ) -> Result<AttestedAncestry, P2pError> {
        prove_attested_ancestry(from, head, |range, top_hash| async move {
            let (serve, headers) = self
                .fetch_live_headers_from_untried_peers(
                    session,
                    range,
                    Some(ExpectedTip {
                        hash: top_hash,
                        trust: ExpectationTrust::Verified,
                    }),
                    None,
                    minimum_live_head_policy(),
                    cancellation,
                )
                .await
                .inspect_err(|error| session.record_error(error))?;
            if range.start() > from {
                // No frame follows a window above the kept one: it clears no
                // strike, and earns its reward now.
                self.reward_header_serve(session, serve).await;
            }
            Ok((serve, headers))
        })
        .await
    }

    /// Reconstruct the branch that replaces the lane's tip, down from
    /// `head_hash`: the parent a mismatching frame names. `evidence` is how
    /// that frame was checked; anchored to an attested head, the branch is
    /// the verified chain.
    async fn reconstruct_reorg(
        &self,
        state: &P2pLiveState,
        head_number: BlockNumber,
        head_hash: BlockHash,
        evidence: ExpectationTrust,
    ) -> Result<(Vec<BlockRef>, Vec<BlockFrame>, BlockRef), P2pError> {
        let descending = self
            .fetch_descending_headers(
                &state.session.fetch,
                head_number,
                head_hash,
                evidence,
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
                    Some(ExpectedTip {
                        hash: head_hash,
                        trust: evidence,
                    }),
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
        expectation: ExpectationTrust,
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
                            if classify_response_failure(&error, expectation)
                                == ResponseFault::Invalid
                            {
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
        let advertised = Self::probe_advertised_head();
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
                    // Leani has not verified an operator-supplied tip: a
                    // contradiction fails the probe but never the peer.
                    expected_tip.map(|hash| ExpectedTip {
                        hash,
                        trust: ExpectationTrust::Unverified,
                    }),
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
                    &session,
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

    /// Status head a fixed-range probe advertises, which also becomes its
    /// peers' qualification target. A probe has no verified execution head,
    /// so it advertises genesis: never a zero hash or an operator-supplied tip
    /// that peers would be qualified, and penalized, against.
    fn probe_advertised_head() -> BlockRef {
        Self::mainnet_genesis_block()
    }

    async fn fetch_headers<C>(
        &self,
        fetch: &C,
        range: BlockRange,
        expected_tip: Option<ExpectedTip>,
        history_proof: bool,
        policy: MaterialRequestPolicy,
        cancellation: &CancellationToken,
    ) -> Result<(B512, Vec<Header>), P2pError>
    where
        C: HeadersClient<Header = Header> + DownloadClient,
    {
        let mut last_error = None;
        let expectation = expected_tip.map_or(ExpectationTrust::Unverified, |tip| tip.trust);
        for _ in 0..self.config.retries {
            let request =
                HeadersRequest::rising(BlockHashOrNumber::Number(range.start().0), range.len());
            let queued_at = Instant::now();
            let permit = self
                .network
                .request_gate
                .acquire(policy.priority, cancellation)
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
                    match validate_headers(range, &headers, expected_tip.map(|tip| tip.hash)) {
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
                            if classify_response_failure(&error, expectation)
                                == ResponseFault::Invalid
                            {
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
        request: HeadersRequest,
        priority: Priority,
        cancellation: &CancellationToken,
    ) -> (DirectPeerLease, Duration, Result<Vec<Header>, P2pError>) {
        let queued_at = Instant::now();
        let permit = self
            .network
            .request_gate
            .acquire(priority, cancellation)
            .await;
        self.config
            .network_telemetry
            .request_started(queued_at.elapsed());
        let request_started_at = Instant::now();
        let response = match permit {
            Ok(permit) => {
                let response = request_direct_headers(
                    &lease.peer,
                    request,
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

    /// Reward a peer for an anchored live header reply: persisted service
    /// evidence and Reth reputation. Headers checked against no verified hash
    /// earn nothing: they may be a fabricated chain.
    async fn reward_header_serve(&self, session: &P2pSession, serve: HeaderServe) {
        if !serve.anchored {
            return;
        }
        self.network.peer_store.record_success(
            serve.peer_id,
            PeerMaterialKind::Header,
            serve.block,
            serve.elapsed,
        );
        reward_verified_material_peer(&session.handle, serve.peer_id).await;
    }

    /// The frames on `serve`'s headers completed, with their bodies and
    /// receipts: the blocks exist. For anchored headers, the peer's
    /// withheld-header strikes clear, and it earns the reward held back for
    /// its headers.
    async fn complete_header_serve(&self, session: &P2pSession, serve: HeaderServe) {
        if settle_header_serve(&self.network.direct_peers, serve) {
            self.reward_header_serve(session, serve).await;
        }
    }

    /// Fetch a live header range from the first peer that serves it validly.
    /// With the lane's `head_poll` rotation, the range is the next block at
    /// the head, polled from a small cohort of peers not yet asked for it; a
    /// "not yet" reply then costs a peer nothing until another serves the
    /// block. Without it, the blocks exist and eligible peers race. A verified
    /// `expected_tip` is requested by hash (see [`live_header_request`]). The
    /// serving peer is rewarded by the caller, once the frames on these
    /// headers complete.
    #[allow(clippy::too_many_lines)]
    async fn fetch_live_headers_from_untried_peers(
        &self,
        session: &P2pSession,
        range: BlockRange,
        expected_tip: Option<ExpectedTip>,
        mut head_poll: Option<&mut HeadPollRotation>,
        policy: MaterialRequestPolicy,
        cancellation: &CancellationToken,
    ) -> Result<(HeaderServe, Vec<Header>), P2pError> {
        let mut tried = HashSet::new();
        let mut last_error = None;
        let expectation = expected_tip.map_or(ExpectationTrust::Unverified, |tip| tip.trust);
        let connected_peers = session.fetch.num_connected_peers();
        let request_limit = effective_material_concurrency(
            connected_peers,
            self.config.material_request_concurrency,
            policy.concurrency,
        );
        let per_peer_limit = direct_peer_request_limit(request_limit, connected_peers);
        let (fanout, peer_budget) = live_header_fanout(head_poll.is_some(), request_limit);
        let head_poll_lease = match head_poll.as_deref_mut() {
            Some(rotation) => self
                .network
                .direct_peers
                .lease_for_head_poll(rotation, per_peer_limit),
            None => None,
        };
        let first = if let Some(lease) = head_poll_lease {
            lease
        } else {
            // No eligible peer right now: wait for one.
            let Some(lease) = self
                .network
                .direct_peers
                .acquire_excluding(
                    PeerMaterialKind::Header,
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
            if let Some(rotation) = head_poll.as_deref_mut() {
                rotation.record_asked(lease.peer.peer_id);
            }
            lease
        };
        tried.insert(first.peer.peer_id);
        let mut asked = 1_usize;
        let mut pending = FuturesUnordered::new();
        let request = live_header_request(range, expected_tip);
        pending.push(self.request_live_headers(
            first,
            request.clone(),
            policy.priority,
            cancellation,
        ));

        loop {
            while pending.len() < fanout && asked < peer_budget {
                let lease = match head_poll.as_deref_mut() {
                    Some(rotation) => self
                        .network
                        .direct_peers
                        .lease_for_head_poll(rotation, per_peer_limit),
                    None => self.network.direct_peers.try_acquire_excluding(
                        PeerMaterialKind::Header,
                        per_peer_limit,
                        &tried,
                        None,
                    ),
                };
                let Some(lease) = lease else {
                    break;
                };
                tried.insert(lease.peer.peer_id);
                asked = asked.saturating_add(1);
                pending.push(self.request_live_headers(
                    lease,
                    request.clone(),
                    policy.priority,
                    cancellation,
                ));
            }
            if pending.is_empty() {
                if head_poll.is_some() || asked >= peer_budget {
                    // The polling cohort has answered; the lane polls again
                    // later, asking other peers, instead of waiting here for
                    // a block that is usually not produced yet.
                    return Err(last_error.unwrap_or_else(|| P2pError::Request {
                        component: "headers",
                        detail: "the polled peers did not serve the next block".to_owned(),
                    }));
                }
                match self
                    .network
                    .direct_peers
                    .acquire_excluding(
                        PeerMaterialKind::Header,
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
                        asked = asked.saturating_add(1);
                        pending.push(self.request_live_headers(
                            lease,
                            request.clone(),
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
                () = self.network.direct_peers.changed.notified(),
                    if pending.len() < fanout && asked < peer_budget =>
                {
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
                    let returned = headers.len();
                    match validate_live_headers(range, expected_tip, headers) {
                        Ok(headers) => {
                            lease.succeeded();
                            if let Some(rotation) = head_poll.as_deref_mut() {
                                rotation.record_served(peer_id, Instant::now());
                            }
                            self.config.network_telemetry.request_succeeded();
                            self.request_metrics.record_header(
                                false,
                                usize::try_from(range.len()).unwrap_or(usize::MAX),
                                returned,
                                response_payload_bytes,
                                elapsed,
                                P2pRequestOutcome::Succeeded,
                            );
                            let serve = HeaderServe {
                                peer_id,
                                block: range.end().0,
                                elapsed,
                                anchored: expectation == ExpectationTrust::Verified,
                            };
                            return Ok((serve, headers));
                        }
                        Err(error) => {
                            self.config.network_telemetry.request_failed();
                            self.request_metrics.record_header(
                                false,
                                usize::try_from(range.len()).unwrap_or(usize::MAX),
                                returned,
                                response_payload_bytes,
                                elapsed,
                                P2pRequestOutcome::Failed,
                            );
                            match live_header_reply_cost(&error, expectation, head_poll.is_some()) {
                                HeaderReplyCost::Ban | HeaderReplyCost::Cooldown => {
                                    self.network.penalize_response(
                                        &mut lease,
                                        &error,
                                        expectation,
                                        |peer_id| session.handle.ban_peer(peer_id),
                                    );
                                }
                                HeaderReplyCost::NotYet => {
                                    if let Some(rotation) = head_poll.as_deref_mut() {
                                        rotation.record_not_yet(peer_id, Instant::now());
                                    }
                                }
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
                    last_error = Some(error);
                }
            }
        }
    }

    async fn request_live_body(
        &self,
        lease: DirectPeerLease,
        hash: B256,
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
            .acquire(priority, cancellation)
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

    /// End a wave of a live material request: pause before the next wave, or
    /// give the header up. Returns the error that ends the request otherwise.
    async fn end_live_material_wave(
        &self,
        bound: &mut LiveMaterialBound,
        error: P2pError,
        cancellation: &CancellationToken,
    ) -> Result<(), P2pError> {
        match live_material_wave_end(
            &self.config,
            &mut bound.waves,
            bound.started.elapsed(),
            &error,
        ) {
            LiveWaveEnd::NextWave(delay) => {
                debug!(
                    component = bound.component,
                    block = bound.block,
                    wave = bound.waves,
                    %error,
                    "latest execution material is not available; asking the peer pool again"
                );
                retry_pause(delay, cancellation).await
            }
            LiveWaveEnd::Withheld => Err(self.give_up_live_material(bound)),
            LiveWaveEnd::Fail => Err(error),
        }
    }

    /// Give up the header of a live material request that no peer served
    /// within its bound. The peer of an unanchored header may have made the
    /// block up, so it takes a withheld-header strike, without a ban.
    fn give_up_live_material(&self, bound: &LiveMaterialBound) -> P2pError {
        if bound.unanchored
            && let Some(peer_id) = bound.header_peer
        {
            self.network.direct_peers.strike_withheld_header(peer_id);
        }
        withheld_live_material(bound.component, bound.block, bound.waves)
    }

    /// End a live material request whose deadline passed inside a wave: give
    /// the header up once an earlier wave found no peer serving the material,
    /// and time out otherwise.
    fn live_material_deadline_passed(&self, bound: &LiveMaterialBound) -> P2pError {
        if bound.waves > 0 {
            self.give_up_live_material(bound)
        } else {
            P2pError::Timeout {
                component: bound.component,
            }
        }
    }

    /// Fetch the body of a validated live header, asking `header_peer`, the
    /// peer that served the header, first. Waves ask every eligible peer until
    /// one serves the body, within `MAX_LIVE_MATERIAL_WAVES` waves and
    /// `LIVE_MATERIAL_TIMEOUT`, which also ends a wave in progress. Past that
    /// bound the header is given up, and its peer struck.
    #[allow(clippy::too_many_arguments)]
    async fn fetch_live_body_from_untried_peers(
        &self,
        session: &P2pSession,
        header: &Header,
        hash: B256,
        header_peer: Option<B512>,
        unanchored: bool,
        policy: MaterialRequestPolicy,
        cancellation: &CancellationToken,
    ) -> Result<(B512, BlockBody), P2pError> {
        let mut bound = LiveMaterialBound::new("bodies", header.number, header_peer);
        bound.unanchored = unanchored;
        let deadline = bound.deadline();
        let waves = self.live_body_waves(session, header, hash, &mut bound, policy, cancellation);
        let outcome = tokio::time::timeout_at(deadline, waves).await;
        outcome.unwrap_or_else(|_| Err(self.live_material_deadline_passed(&bound)))
    }

    #[allow(clippy::too_many_lines)]
    async fn live_body_waves(
        &self,
        session: &P2pSession,
        header: &Header,
        hash: B256,
        bound: &mut LiveMaterialBound,
        policy: MaterialRequestPolicy,
        cancellation: &CancellationToken,
    ) -> Result<(B512, BlockBody), P2pError> {
        let header_peer = bound.header_peer;
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
                        PeerMaterialKind::Body,
                        per_peer_limit,
                        &tried,
                        header_peer,
                    ) else {
                        break;
                    };
                    tried.insert(lease.peer.peer_id);
                    pending.push(self.request_live_body(
                        lease,
                        hash,
                        policy.priority,
                        cancellation,
                    ));
                }
                if pending.is_empty() {
                    match self
                        .network
                        .direct_peers
                        .acquire_excluding(
                            PeerMaterialKind::Body,
                            per_peer_limit,
                            self.config.request_timeout,
                            &tried,
                            header_peer,
                            cancellation,
                        )
                        .await
                    {
                        Ok(Some(lease)) => {
                            tried.insert(lease.peer.peer_id);
                            pending.push(self.request_live_body(
                                lease,
                                hash,
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
                            self.end_live_material_wave(bound, error, cancellation)
                                .await?;
                            break;
                        }
                        // Waiting for a peer failed, as it does after the
                        // request timeout in a pool without any peer: the
                        // request ends, and the lane reconnects.
                        Err(error) => {
                            self.end_live_material_wave(bound, error, cancellation)
                                .await?;
                            break;
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
                                self.network.peer_store.record_success(
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
                                // Material requested by hash answers to its
                                // header's commitments, a verified
                                // expectation. An empty latest-material
                                // response is not malicious, but immediately
                                // selecting the same peer again can spam a
                                // lagging or non-serving session and starve
                                // new peers: it costs the pool-local
                                // exponential cooldown of the body lane only,
                                // without changing Reth reputation or
                                // disconnecting the session.
                                if self.network.penalize_response(
                                    &mut lease,
                                    &error,
                                    ExpectationTrust::Verified,
                                    |peer_id| session.handle.ban_peer(peer_id),
                                ) == ResponseFault::Disagreement
                                {
                                    if repeated_incomplete_response(
                                        &mut incomplete_responses,
                                        peer_id,
                                        self.config.retries,
                                    ) {
                                        debug!(
                                            attempts = self.config.retries,
                                            "keeping execution peer body lane cooled after repeated incomplete latest responses"
                                        );
                                        incomplete_responses.remove(&peer_id);
                                    }
                                    pending_error = Some(error);
                                } else {
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
                        last_error = Some(error);
                    }
                }
            }
        }
    }

    /// Fetch the receipts of a validated live block, asking the peer that
    /// served its body first, in waves bounded like the live body's. Returns
    /// the peer that served them, or `None` for a block without transactions,
    /// whose receipts no peer is asked for.
    async fn fetch_live_receipts_from_untried_peers(
        &self,
        session: &P2pSession,
        material: LiveReceiptMaterial<'_>,
        policy: MaterialRequestPolicy,
        cancellation: &CancellationToken,
    ) -> Result<(Option<B512>, Vec<Receipt>), P2pError> {
        // A block without transactions commits to the empty receipts root:
        // its receipts are known without asking a peer, once the header and
        // body agree with them.
        if !has_receipts(material.header)
            && validate_receipts(
                std::slice::from_ref(material.header),
                std::slice::from_ref(material.body),
                &[Vec::new()],
            )
            .is_ok()
        {
            return Ok((None, Vec::new()));
        }
        let mut bound = LiveMaterialBound::new(
            "receipts",
            material.header.number,
            Some(material.header_peer),
        );
        let deadline = bound.deadline();
        let waves = self.live_receipt_waves(session, material, &mut bound, policy, cancellation);
        let outcome = tokio::time::timeout_at(deadline, waves).await;
        outcome
            .unwrap_or_else(|_| Err(self.live_material_deadline_passed(&bound)))
            .map(|(peer_id, receipts)| (Some(peer_id), receipts))
    }

    #[allow(clippy::too_many_lines)]
    async fn live_receipt_waves(
        &self,
        session: &P2pSession,
        material: LiveReceiptMaterial<'_>,
        bound: &mut LiveMaterialBound,
        policy: MaterialRequestPolicy,
        cancellation: &CancellationToken,
    ) -> Result<(B512, Vec<Receipt>), P2pError> {
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
                        PeerMaterialKind::Receipts,
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
                    self.end_live_material_wave(bound, error, cancellation)
                        .await?;
                    break;
                };
                let peer_id = lease.peer.peer_id;
                tried.insert(peer_id);
                let permit = self
                    .network
                    .request_gate
                    .acquire(policy.priority, cancellation)
                    .await?;
                self.config
                    .network_telemetry
                    .request_started(queued_at.elapsed());
                let request_started_at = Instant::now();
                let response = request_direct_receipts(
                    &lease.peer,
                    std::slice::from_ref(material.header),
                    std::slice::from_ref(&material.hash),
                    Some(std::slice::from_ref(material.body)),
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
                                self.network.peer_store.record_success(
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
                                // See the matching live-body path: prefer
                                // fresh peers and retry only this material
                                // lane after its local cooldown.
                                if self.network.penalize_response(
                                    &mut lease,
                                    &error,
                                    ExpectationTrust::Verified,
                                    |peer_id| session.handle.ban_peer(peer_id),
                                ) == ResponseFault::Disagreement
                                {
                                    if repeated_incomplete_response(
                                        &mut incomplete_responses,
                                        peer_id,
                                        self.config.retries,
                                    ) {
                                        debug!(
                                            attempts = self.config.retries,
                                            "keeping execution peer receipt lane cooled after repeated incomplete latest responses"
                                        );
                                        incomplete_responses.remove(&peer_id);
                                    }
                                    pending_error = Some(error);
                                } else {
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
                        // An eth/70 reply is checked against its headers while it is
                        // assembled, so its invalid receipts fail the request itself.
                        self.network.penalize_response(
                            &mut lease,
                            &error,
                            ExpectationTrust::Verified,
                            |peer_id| session.handle.ban_peer(peer_id),
                        );
                        if incomplete {
                            if repeated_incomplete_response(
                                &mut incomplete_responses,
                                peer_id,
                                self.config.retries,
                            ) {
                                debug!(
                                    attempts = self.config.retries,
                                    "keeping execution peer receipt lane cooled after repeated incomplete latest responses"
                                );
                                incomplete_responses.remove(&peer_id);
                            }
                            pending_error = Some(error);
                        } else {
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
            let queued_at = Instant::now();
            let permit = self
                .network
                .request_gate
                .acquire(policy.priority, cancellation)
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
                            // Material requested by hash answers to its
                            // header's commitments, a verified expectation.
                            if classify_response_failure(&error, ExpectationTrust::Verified)
                                == ResponseFault::Invalid
                            {
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

    // Reth's generic fetcher chooses receipt encoding from advertised
    // capabilities, which can exceed the negotiated ETH version. All receipt
    // acquisition uses our direct dispatcher and the session's actual version.
    #[allow(clippy::too_many_lines)]
    async fn fetch_receipts(
        &self,
        session: &P2pSession,
        headers: &[Header],
        bodies: Option<&[BlockBody]>,
        hashes: &[B256],
        policy: MaterialRequestPolicy,
        cancellation: &CancellationToken,
    ) -> Result<(B512, Vec<Vec<Receipt>>), P2pError> {
        let attempts = if headers.len() > 1 {
            1
        } else {
            self.config.retries
        };
        let mut last_error = None;
        for attempt in 0..attempts {
            let queued_at = Instant::now();
            let connected_peers = session.fetch.num_connected_peers();
            let request_limit = effective_material_concurrency(
                connected_peers,
                self.config.material_request_concurrency,
                policy.concurrency,
            );
            let per_peer_limit = direct_peer_request_limit(request_limit, connected_peers);
            // Capacity wait is scheduler queueing, not an on-wire request.
            // A busy healthy peer can legitimately take longer than the ETH
            // response timeout to expose another multiplexing slot.
            let mut lease = self
                .network
                .direct_peers
                .acquire(
                    PeerMaterialKind::Receipts,
                    per_peer_limit,
                    self.config
                        .request_timeout
                        .saturating_mul(4)
                        .min(self.config.peer_wait_timeout),
                    cancellation,
                )
                .await?;
            let peer_id = lease.peer.peer_id;
            let permit = self
                .network
                .request_gate
                .acquire(policy.priority, cancellation)
                .await?;
            self.config
                .network_telemetry
                .request_started(queued_at.elapsed());
            let request_started_at = Instant::now();
            let response = request_direct_receipts(
                &lease.peer,
                headers,
                hashes,
                bodies,
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
                    let validation = match bodies {
                        Some(bodies) => validate_receipts(headers, bodies, &response.receipts),
                        None => validate_receipts_against_headers(headers, &response.receipts),
                    };
                    match validation {
                        Ok(()) => {
                            lease.succeeded();
                            self.network.peer_store.record_success(
                                peer_id,
                                PeerMaterialKind::Receipts,
                                headers.last().map_or(0, |header| header.number),
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
                            return Ok((peer_id, response.receipts));
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
                            // Receipts requested by hash answer to their
                            // header's commitments, a verified expectation.
                            self.network.penalize_response(
                                &mut lease,
                                &error,
                                ExpectationTrust::Verified,
                                |peer_id| session.handle.ban_peer(peer_id),
                            );
                            last_error = Some(error);
                        }
                    }
                }
                Err(P2pError::Cancelled) => return Err(P2pError::Cancelled),
                Err(error) => {
                    // An eth/70 reply is checked against its headers while it is
                    // assembled, so its invalid receipts fail the request itself.
                    self.network.penalize_response(
                        &mut lease,
                        &error,
                        ExpectationTrust::Verified,
                        |peer_id| session.handle.ban_peer(peer_id),
                    );
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
    async fn fetch_receipts_batched(
        &self,
        session: &P2pSession,
        headers: &[Header],
        bodies: &[BlockBody],
        hashes: &[B256],
        policy: MaterialRequestPolicy,
        cancellation: &CancellationToken,
    ) -> Result<(HashSet<B512>, Vec<Vec<Receipt>>), P2pError> {
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
            session.fetch.num_connected_peers(),
            self.config.material_request_concurrency,
            policy.concurrency,
        );
        let outcomes = stream::iter(requests)
            .map(|(header_chunk, body_chunk, hash_chunk)| async move {
                let result = self
                    .fetch_receipts(
                        session,
                        &header_chunk,
                        Some(&body_chunk),
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
                        session,
                        std::slice::from_ref(&header),
                        Some(std::slice::from_ref(&body)),
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
        // stream could independently download the same proof suffix. A bad
        // segment no longer wedges that download, and a stream waiting for it
        // stops waiting once cancelled.
        let (proof_session, expected_tip) = {
            let mut cached = tokio::select! {
                () = cancellation.cancelled() => return Err(P2pError::Cancelled),
                cached = self.anchored_headers.lock() => cached,
            };
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
                // Proven by the header chain that links to the finalized
                // consensus anchor.
                Some(ExpectedTip {
                    hash: expected_tip,
                    trust: ExpectationTrust::Verified,
                }),
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
        // A block without transactions commits to the empty receipts root, so
        // no peer is asked for its receipts.
        let (requested_headers, requested_hashes): (Vec<_>, Vec<_>) = headers
            .iter()
            .zip(hashes)
            .filter(|(header, _)| has_receipts(header))
            .map(|(header, hash)| (header.clone(), *hash))
            .unzip();
        let batch_blocks = self.source.material_tuning.receipt_blocks();
        let requests = requested_headers
            .chunks(batch_blocks)
            .zip(requested_hashes.chunks(batch_blocks))
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
                    .source
                    .fetch_receipts(
                        session,
                        &header_chunk,
                        None,
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
                    .source
                    .fetch_receipts(
                        session,
                        std::slice::from_ref(&header),
                        None,
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
        let mut fetched = Vec::with_capacity(requested_headers.len());
        for (_, _, mut batch) in completed {
            fetched.append(&mut batch);
        }
        let receipts = with_known_empty_receipts(headers, fetched);
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
            self.anchor.block.hash,
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
                Err(P2pError::Cancelled) => return Err(P2pError::Cancelled),
                Err(error) => {
                    session.record_attempt();
                    session.record_error(&error);
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
                        .and_then(|(peer, headers)| {
                            header_proof_segment(range, &headers).map(|segment| (peer, segment))
                        });
                    (range, result)
                })
                .buffer_unordered(concurrency)
                .collect::<Vec<_>>()
                .await;
            let mut last_error = None;
            // Every outcome is recorded, so no range of the wave is lost.
            for (range, result) in outcomes {
                if let Err(error) = &result {
                    if matches!(error, P2pError::Cancelled) {
                        return Err(P2pError::Cancelled);
                    }
                    last_error = Some(error.to_string());
                }
                for (peer, error) in builder.record(range, result) {
                    // The segment contradicts the header chain proven to the
                    // finalized anchor, a verified expectation.
                    if classify_response_failure(&error, ExpectationTrust::Verified)
                        == ResponseFault::Invalid
                    {
                        fetch.report_bad_message(peer);
                    }
                    last_error = Some(error.to_string());
                }
            }
            if let Some(error) = last_error {
                return Err(P2pError::Request {
                    component: "anchored headers",
                    detail: error,
                });
            }
        }
        builder.finish()
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
    let mut builder = AnchoredHeaderProofBuilder::new(proof, proof, proof.len(), anchor);
    builder.pending.clear();
    for segment in segments {
        let range = segment.range;
        if let Some((_, error)) = builder
            .record(range, Ok((B512::ZERO, segment)))
            .into_iter()
            .next()
        {
            return Err(error);
        }
    }
    builder.finish()
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
        let attested = self.live_attested_heads()?;
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
        // A retained or cursor tip is no consensus-verified expectation: it
        // may have been reorged away since. Only the attested head is.
        let known = attested
            .peek()
            .filter(|head| head.block_number == required_peer_head)
            .map(|head| head.block_hash);
        let (session, last, queued, recent) = if let Some(recent) = retained {
            let (session, _) = self
                .connect(anchor, NetworkLane::Live, &cancellation)
                .await?;
            self.wait_for_peer_head(&session, required_peer_head, known, &cancellation)
                .await?;
            (session, anchor, VecDeque::new(), recent)
        } else if overlap_blocks == 0 {
            let (session, _) = self
                .connect(anchor, NetworkLane::Live, &cancellation)
                .await?;
            self.wait_for_peer_head(&session, required_peer_head, known, &cancellation)
                .await?;
            (session, anchor, VecDeque::new(), VecDeque::from([anchor]))
        } else {
            let range = overlap_range.expect("non-zero overlap has a range");
            let fetched = self
                .connect_and_fetch_requested_live_range(
                    anchor,
                    range,
                    // The overlap ends at the consensus-verified anchor.
                    Some(ExpectedTip {
                        hash: anchor.hash,
                        trust: ExpectationTrust::Verified,
                    }),
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
                return Err(SourceError::Protocol(
                    "anchored overlap returned no frames".to_owned(),
                ));
            };
            let fetched_anchor = frames.last().expect("non-empty overlap").block;
            if fetched_anchor.number != anchor.number || fetched_anchor.hash != anchor.hash {
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
            attested,
            ancestry: AttestedAncestry::default(),
            pending_material_attempts: 0,
            head_poll: HeadPollRotation::new(self.config.retry_backoff),
            head_unavailable_since: None,
            head_poll_silent_since: None,
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
            note_live_progress(
                &mut state.disconnect_reported,
                &mut state.head_poll_silent_since,
            );
            return Some((Ok(ChainEvent::Block(Box::new(frame))), state));
        }
        let batch_blocks = if state.pending_material_attempts == 0 {
            u64::try_from(state.source.config.material_request_blocks)
                .expect("material request block bound fits u64")
        } else {
            1
        };
        let attested = state.attested.latest();
        match live_step(state.last, attested, &state.ancestry, batch_blocks) {
            LiveStep::Wait => {
                // Nothing above the tip is attested yet: include nothing, and
                // wait for the next attested head, one to two slots.
                tokio::select! {
                    () = state.cancellation.cancelled() => return None,
                    published = state.attested.changed() => {
                        if !published
                            && retry_pause(state.source.config.poll_interval, &state.cancellation)
                                .await
                                .is_err()
                        {
                            return None;
                        }
                    }
                    () = tokio::time::sleep(state.source.config.poll_interval) => {}
                }
                let unavailable_for =
                    head_unavailable_for(&mut state.head_unavailable_since, Instant::now());
                let action = head_unavailable(
                    state.session.manager_is_current().await,
                    unavailable_for,
                    ATTESTED_HEAD_GRACE,
                );
                if action == HeadUnavailable::Retry {
                    continue;
                }
                let reason = format!(
                    "no sync-committee-attested execution head above block {} for {} seconds",
                    state.last.number.0,
                    unavailable_for.as_secs()
                );
                debug!(%reason, "the live lane is waiting for an attested head");
                if action == HeadUnavailable::Reconnect {
                    state.reconnect_error = Some(reason.clone());
                }
                if state.disconnect_reported {
                    continue;
                }
                state.disconnect_reported = true;
                return Some((Ok(ChainEvent::Disconnected { reason }), state));
            }
            LiveStep::Anchor { from, head } => {
                state.head_unavailable_since = None;
                match state
                    .source
                    .anchor_attested_ancestry(&state.session, from, head, &state.cancellation)
                    .await
                {
                    Ok(ancestry) => state.ancestry = ancestry,
                    Err(P2pError::Cancelled) => return None,
                    Err(error) => {
                        // The next attempt proves the chain of the newest
                        // attested head.
                        state.ancestry.invalidate();
                        let manager_current = state.session.manager_is_current().await;
                        if let Some(delay) = pending_live_material_delay(
                            &state.source.config,
                            &mut state.pending_material_attempts,
                            &error,
                            false,
                            manager_current,
                        ) {
                            debug!(
                                attempt = state.pending_material_attempts,
                                ?delay,
                                %error,
                                "the attested head's ancestry is not served yet; retrying on the active peer pool"
                            );
                            if retry_pause(delay, &state.cancellation).await.is_err() {
                                return None;
                            }
                            continue;
                        }
                        let reason = error.to_string();
                        state.reconnect_error = Some(reason.clone());
                        if let Some(event) =
                            report_disconnect(&mut state.disconnect_reported, reason)
                        {
                            return Some((Ok(event), state));
                        }
                    }
                }
            }
            LiveStep::Poll { block, tip } => {
                state.head_unavailable_since = None;
                // Peers that answered "not yet" for a block the lane has since
                // moved past withheld or lagged a block another peer served.
                for peer_id in state.head_poll.begin_poll(block) {
                    state
                        .source
                        .network
                        .direct_peers
                        .record_material_failure(peer_id, PeerMaterialKind::Header);
                }
                match state
                    .source
                    .poll_next_verified_frame(
                        &state.session,
                        block,
                        tip,
                        &state.request,
                        state.budget,
                        &mut state.head_poll,
                        &state.cancellation,
                    )
                    .await
                {
                    Ok(Some(frame)) => {
                        state.pending_material_attempts = 0;
                        state.head_poll_silent_since = None;
                        if frame.block.parent_hash != state.last.hash {
                            // The lane does not go on with this serve: after a
                            // reorg the block is polled again, and the peers that
                            // answered "not yet" are judged against that serve.
                            // The frame is the attested head itself: verified
                            // evidence that the tip is no longer its ancestor.
                            state.head_poll.serve_failed();
                            if let Some(event) =
                                reconstruct_reorg_event(&mut state, frame.block, tip.trust).await
                            {
                                return Some((Ok(event), state));
                            }
                            continue;
                        }
                        state.queued.push_back(frame);
                    }
                    Ok(None) => {
                        state.pending_material_attempts = 0;
                        state.head_poll_silent_since = None;
                        if retry_pause(state.source.config.poll_interval, &state.cancellation)
                            .await
                            .is_err()
                        {
                            return None;
                        }
                    }
                    Err(P2pError::Cancelled) => return None,
                    Err(error) => {
                        state.head_poll.serve_failed();
                        let manager_current = state.session.manager_is_current().await;
                        let grace = state
                            .source
                            .descriptor
                            .expected_lag
                            .max(state.source.config.poll_interval);
                        let (delay, report) = match head_poll_failure(
                            &state.source.config,
                            &mut state.pending_material_attempts,
                            &mut state.head_poll_silent_since,
                            Instant::now(),
                            grace,
                            &error,
                            manager_current,
                        ) {
                            HeadPollFailure::Retry(delay) => (delay, false),
                            HeadPollFailure::Report(delay) => (delay, true),
                            HeadPollFailure::Reconnect => {
                                let reason = error.to_string();
                                state.reconnect_error = Some(reason.clone());
                                if let Some(event) =
                                    report_disconnect(&mut state.disconnect_reported, reason)
                                {
                                    return Some((Ok(event), state));
                                }
                                continue;
                            }
                        };
                        debug!(
                            attempt = state.pending_material_attempts,
                            ?delay,
                            %error,
                            "latest execution material is not available yet; retrying on the active peer pool"
                        );
                        if retry_pause(delay, &state.cancellation).await.is_err() {
                            return None;
                        }
                        if report && !state.disconnect_reported {
                            state.disconnect_reported = true;
                            return Some((
                                Ok(ChainEvent::Disconnected {
                                    reason: error.to_string(),
                                }),
                                state,
                            ));
                        }
                    }
                }
            }
            LiveStep::CatchUp { range } => {
                state.head_unavailable_since = None;
                // The batch's headers were proven by hash, down from an
                // attested head.
                let (headers, header_peer, settle) = state
                    .ancestry
                    .take(usize::try_from(range.len()).unwrap_or(usize::MAX));
                // A proven chain that does not extend the tip shows the tip
                // is no ancestor of the attested head: verified evidence of
                // a reorg, reconstructed from where it forks.
                if let Some(first) = headers.first()
                    && block_hash(first.parent_hash) != state.last.hash
                {
                    debug!(
                        tip = state.last.number.0,
                        attested_head = ?state.ancestry.head.map(|head| head.block_number.0),
                        "the attested head's proven chain does not extend the lane's tip"
                    );
                    state.ancestry.invalidate();
                    let mismatching = header_block_ref(first);
                    if let Some(event) =
                        reconstruct_reorg_event(&mut state, mismatching, ExpectationTrust::Verified)
                            .await
                    {
                        return Some((Ok(event), state));
                    }
                    continue;
                }
                let frames = match header_peer {
                    Some(header_peer) => {
                        state
                            .source
                            .fetch_live_range_material(
                                &state.session,
                                range,
                                &headers,
                                header_peer,
                                settle,
                                &state.request,
                                state.budget,
                                &state.cancellation,
                            )
                            .await
                    }
                    None => Err(P2pError::IncompleteResponse {
                        component: "headers",
                        returned: 0,
                        expected: headers.len(),
                    }),
                };
                let (frames, _) = match frames {
                    Ok(result) => {
                        state.pending_material_attempts = 0;
                        result
                    }
                    Err(P2pError::Cancelled) => return None,
                    Err(error) => {
                        // The next attempt proves the chain of the newest
                        // attested head again: this one may have been
                        // reorged away.
                        debug!(
                            attested_head = ?state.ancestry.head.map(|head| head.block_number.0),
                            %error,
                            "catch-up on the attested head's proven chain failed; proving it again"
                        );
                        state.ancestry.invalidate();
                        let manager_current = state.session.manager_is_current().await;
                        if let Some(delay) = pending_live_material_delay(
                            &state.source.config,
                            &mut state.pending_material_attempts,
                            &error,
                            false,
                            manager_current,
                        ) {
                            debug!(
                                attempt = state.pending_material_attempts,
                                ?delay,
                                %error,
                                "latest execution material is not available yet; retrying on the active peer pool"
                            );
                            if retry_pause(delay, &state.cancellation).await.is_err() {
                                return None;
                            }
                            continue;
                        }
                        let reason = error.to_string();
                        state.reconnect_error = Some(reason.clone());
                        if let Some(event) =
                            report_disconnect(&mut state.disconnect_reported, reason)
                        {
                            return Some((Ok(event), state));
                        }
                        continue;
                    }
                };
                // Validated headers linked to the verified tip are the heads
                // the session observes.
                if let Some(tip) = frames.last() {
                    state.session.observe_head(tip.block.number);
                }
                state.queued.extend(frames);
            }
        }
    }
}

/// Heads that peers declared in their handshake statuses.
#[derive(Debug, Default, Eq, PartialEq)]
struct DeclaredPeerHeads {
    /// The most commonly declared head at or above the required minimum.
    head: Option<(BlockNumber, BlockHash)>,
    /// Peers that declared only a head hash.
    hash_only: Vec<(B512, B256)>,
}

/// Collect the heads peers declared in their handshake statuses. Declared
/// heads are peer claims: they may steer requests as unverified expectations
/// but never become the session's observed head.
fn declared_peer_heads(
    statuses: impl IntoIterator<Item = (B512, Option<u64>, B256)>,
    minimum: BlockNumber,
) -> DeclaredPeerHeads {
    let mut declared = BTreeMap::<(u64, [u8; 32]), usize>::new();
    let mut unknown_hashes = Vec::new();
    for (peer_id, latest_block, blockhash) in statuses {
        if let Some(number) = latest_block {
            if number < minimum.0 {
                debug!(
                    peer_head = number,
                    required_head = minimum.0,
                    "execution peer is behind the required live head; retaining it for a grace retry"
                );
                continue;
            }
            let key = (number, blockhash.0);
            *declared.entry(key).or_default() += 1;
        } else if blockhash != B256::ZERO {
            unknown_hashes.push((peer_id, blockhash));
        }
    }
    DeclaredPeerHeads {
        head: declared
            .into_iter()
            .max_by_key(|((number, _), count)| (*count, *number))
            .map(|((number, hash), _)| (BlockNumber(number), BlockHash::new(hash))),
        hash_only: unknown_hashes,
    }
}

/// How head discovery learned a head.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DiscoveredHead {
    /// A peer's claim: its handshake status, or its own header for the hash
    /// it advertised. It steers the next request only.
    Claimed(BlockNumber, BlockHash),
    /// A header this node validated at a block it requested.
    Validated(BlockNumber, BlockHash),
}

/// Settle a discovered head on the session. Only a validated head advances
/// the session's observed head; a claim is returned to steer the next
/// request, and nothing else.
fn settle_discovered_head(
    telemetry: &NetworkSessionTelemetry,
    head: DiscoveredHead,
) -> (BlockNumber, BlockHash) {
    match head {
        DiscoveredHead::Claimed(number, hash) => (number, hash),
        DiscoveredHead::Validated(number, hash) => {
            telemetry.observe_head(number);
            (number, hash)
        }
    }
}

fn head_unavailable_for(since: &mut Option<Instant>, now: Instant) -> Duration {
    now.saturating_duration_since(*since.get_or_insert(now))
}

/// What the live lane does while head discovery fails.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HeadUnavailable {
    /// Look for the head again on the same session.
    Retry,
    /// Report the lane disconnected, once, and keep looking.
    Report,
    /// Report the lane disconnected and reconnect.
    Reconnect,
}

/// What the live lane does once head discovery has failed for
/// `unavailable_for`. A session whose manager has stopped, as the zero-peer
/// watchdog stops it, never serves the head again: only `connect` builds a
/// new manager, so the lane reconnects. With a running manager, peers may
/// still serve the head, and the lane reports itself disconnected once the
/// grace has passed.
fn head_unavailable(
    manager_current: bool,
    unavailable_for: Duration,
    grace: Duration,
) -> HeadUnavailable {
    if !manager_current {
        HeadUnavailable::Reconnect
    } else if unavailable_for < grace {
        HeadUnavailable::Retry
    } else {
        HeadUnavailable::Report
    }
}

/// The pause before the live lane asks again, on its session, after a head
/// poll or catch-up request failed, or `None` to report the lane disconnected
/// and reconnect. The lane waits while its manager runs and the failure is
/// material not served yet, or, for a head poll, a cohort whose last peer
/// timed out or could not take the request: the next poll asks other peers.
fn pending_live_material_delay(
    config: &RethP2pConfig,
    attempts: &mut usize,
    error: &P2pError,
    head_poll: bool,
    manager_current: bool,
) -> Option<Duration> {
    let waits = matches!(
        error,
        P2pError::IncompleteResponse {
            component: "headers" | "bodies" | "receipts",
            ..
        }
    ) || (head_poll && silent_head_poll(error));
    if !waits || !manager_current {
        return None;
    }
    *attempts = (*attempts).saturating_add(1);
    Some(session_retry_delay(config, *attempts).min(config.poll_interval))
}

/// Whether a failed head poll heard from no peer: its cohort's last peer
/// timed out, or its session could not take the request.
fn silent_head_poll(error: &P2pError) -> bool {
    matches!(
        error,
        P2pError::Timeout {
            component: "headers"
        } | P2pError::Request {
            component: "headers",
            ..
        }
    )
}

/// What the live lane does after its head poll failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HeadPollFailure {
    /// Poll again after the pause.
    Retry(Duration),
    /// Report the lane disconnected, once, and poll again after the pause.
    Report(Duration),
    /// Report the lane disconnected and reconnect.
    Reconnect,
}

/// What the live lane does after its head poll failed with `error`. A silent
/// cohort is waited out like a block not produced yet, until no polled peer
/// has answered for `grace` since `silent_since`: the lane then reports
/// itself disconnected, which clears readiness, and keeps polling. Any
/// answer ends the silence, and a stopped manager reconnects.
fn head_poll_failure(
    config: &RethP2pConfig,
    attempts: &mut usize,
    silent_since: &mut Option<Instant>,
    now: Instant,
    grace: Duration,
    error: &P2pError,
    manager_current: bool,
) -> HeadPollFailure {
    let Some(delay) = pending_live_material_delay(config, attempts, error, true, manager_current)
    else {
        return HeadPollFailure::Reconnect;
    };
    if !silent_head_poll(error) {
        *silent_since = None;
        return HeadPollFailure::Retry(delay);
    }
    if head_unavailable_for(silent_since, now) < grace {
        HeadPollFailure::Retry(delay)
    } else {
        HeadPollFailure::Report(delay)
    }
}

/// The live lane made progress: it emitted a block or reorg. It reports
/// itself disconnected again, once, after its next failure, and a silent head
/// poll starts the grace of [`head_poll_failure`] afresh.
const fn note_live_progress(disconnect_reported: &mut bool, silent_since: &mut Option<Instant>) {
    *disconnect_reported = false;
    *silent_since = None;
}

/// A disconnect to report, once per outage: `None` when it already was.
fn report_disconnect(disconnect_reported: &mut bool, reason: String) -> Option<ChainEvent> {
    (!std::mem::replace(disconnect_reported, true)).then_some(ChainEvent::Disconnected { reason })
}

/// The pause before another wave of a live body or receipt request whose last
/// wave found no peer serving the material, or `None` once the request has
/// run `MAX_LIVE_MATERIAL_WAVES` waves or `LIVE_MATERIAL_TIMEOUT`: no peer
/// serves the material of that header.
fn next_live_material_wave(
    config: &RethP2pConfig,
    waves: &mut usize,
    elapsed: Duration,
) -> Option<Duration> {
    *waves = (*waves).saturating_add(1);
    if *waves >= MAX_LIVE_MATERIAL_WAVES || elapsed >= LIVE_MATERIAL_TIMEOUT {
        return None;
    }
    Some(session_retry_delay(config, *waves).min(config.poll_interval))
}

/// What ends a wave of a live body or receipt request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LiveWaveEnd {
    /// Ask the eligible peers again after the pause.
    NextWave(Duration),
    /// No peer served the material within the bound: give the header up.
    Withheld,
    /// End the request with the error.
    Fail,
}

/// What a live material request does after `error` ended a wave, or the wait
/// for a peer to ask. While peers answer that they lack the material, it asks
/// them again within `next_live_material_wave`'s bound, and gives the header
/// up past it. Any other error ends the request, and the lane decides: a pool
/// without any peer, for one, reconnects.
fn live_material_wave_end(
    config: &RethP2pConfig,
    waves: &mut usize,
    elapsed: Duration,
    error: &P2pError,
) -> LiveWaveEnd {
    if !matches!(error, P2pError::IncompleteResponse { .. }) {
        return LiveWaveEnd::Fail;
    }
    next_live_material_wave(config, waves, elapsed)
        .map_or(LiveWaveEnd::Withheld, LiveWaveEnd::NextWave)
}

/// One live body or receipt request for the block of a validated header, and
/// its bound: `MAX_LIVE_MATERIAL_WAVES` waves and `LIVE_MATERIAL_TIMEOUT`.
#[derive(Debug)]
struct LiveMaterialBound {
    component: &'static str,
    block: u64,
    /// The peer that served the header.
    header_peer: Option<B512>,
    /// Whether that header was only a peer claim. A header checked against a
    /// verified hash proves its block exists, so withheld material is not its
    /// peer's fault.
    unanchored: bool,
    started: Instant,
    waves: usize,
}

impl LiveMaterialBound {
    fn new(component: &'static str, block: u64, header_peer: Option<B512>) -> Self {
        Self {
            component,
            block,
            header_peer,
            unanchored: false,
            started: Instant::now(),
            waves: 0,
        }
    }

    /// The instant the request ends, even inside a wave.
    fn deadline(&self) -> tokio::time::Instant {
        tokio::time::Instant::from_std(self.started + LIVE_MATERIAL_TIMEOUT)
    }
}

/// The error for the material of a validated header that no peer served
/// within the live wave bound. It is no incomplete reply the lane waits out:
/// the lane reports itself disconnected, drops the header, and asks for it
/// again.
fn withheld_live_material(component: &'static str, block: u64, waves: usize) -> P2pError {
    P2pError::Request {
        component,
        detail: format!("no peer served block {block} within {waves} waves"),
    }
}

async fn reconnect_live_session(
    state: &mut P2pLiveState,
    request_error: &str,
) -> Result<(), P2pError> {
    state.session.record_attempt();
    state.session.record_error(request_error);
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
                // Not progress: an outage that keeps failing after reconnects
                // stays reported once, until a block or reorg follows.
                state.head_poll_silent_since = None;
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

/// Reconstruct the reorg that `mismatching` reveals. `evidence` is how the
/// mismatching frame was checked: anchored to an attested head, it proves the
/// tip was reorged away. Returns the event to emit, or `None` when the lane
/// only retries.
async fn reconstruct_reorg_event(
    state: &mut P2pLiveState,
    mismatching: BlockRef,
    evidence: ExpectationTrust,
) -> Option<ChainEvent> {
    let source = state.source.clone();
    let (head_number, head_hash) = reorg_reconstruction_tip(state.last, mismatching);
    let error = match source
        .reconstruct_reorg(state, head_number, head_hash, evidence)
        .await
    {
        Ok((reverted, applied, new_tip)) => {
            for _ in 0..reverted.len() {
                state.recent.pop_back();
            }
            state.recent.extend(applied.iter().map(|frame| frame.block));
            state.last = new_tip;
            state.ancestry = AttestedAncestry::default();
            note_live_progress(
                &mut state.disconnect_reported,
                &mut state.head_poll_silent_since,
            );
            return Some(ChainEvent::Reorg { reverted, applied });
        }
        Err(error) => error,
    };
    match reorg_failure(&error, evidence, &mut state.disconnect_reported) {
        ReorgFailure::Reset => {
            state.terminal = true;
            Some(ChainEvent::Reset {
                last_valid: state.recent.front().copied(),
                reason: error.to_string(),
            })
        }
        // The lane keeps its tip and looks for the branch again, after a
        // pause. Cancellation ends the lane on the next poll.
        ReorgFailure::Report => {
            let _ = retry_pause(source.config.poll_interval, &state.cancellation).await;
            Some(ChainEvent::Disconnected {
                reason: error.to_string(),
            })
        }
        ReorgFailure::Retry => {
            let _ = retry_pause(source.config.poll_interval, &state.cancellation).await;
            None
        }
    }
}

/// The block reorg reconstruction descends from once `mismatching`, fetched
/// past the lane's tip `last`, does not extend it: `last`'s replacement, the
/// parent `mismatching` names. Its descending headers cover the retained
/// window and nothing above the tip, so a shallow reorg is reconstructed
/// however far the discovered head is ahead.
const fn reorg_reconstruction_tip(
    last: BlockRef,
    mismatching: BlockRef,
) -> (BlockNumber, BlockHash) {
    (last.number, mismatching.parent_hash)
}

/// What the live lane does after a reorg reconstruction failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReorgFailure {
    /// The branch is proven to fork below the retained window: reset.
    Reset,
    /// Report the lane disconnected, and look for the branch again.
    Report,
    /// Look for the branch again.
    Retry,
}

/// What the live lane does after a reorg reconstruction failed with `error`:
/// a reset only for a replacement branch proven to fork below the retained
/// window, and a retry otherwise, such as for a peer without the branch or a
/// timeout. The lane reports itself disconnected once, unless it has already
/// since its last progress.
///
/// Only verified `evidence`, a mismatching frame anchored to an attested
/// head, can prove the branch: a fabricated frame and a fabricated
/// descending chain that never joins the window would otherwise reset the
/// lane.
const fn reorg_failure(
    error: &P2pError,
    evidence: ExpectationTrust,
    disconnect_reported: &mut bool,
) -> ReorgFailure {
    if matches!(error, P2pError::ReorgTooDeep { .. })
        && matches!(evidence, ExpectationTrust::Verified)
    {
        ReorgFailure::Reset
    } else if *disconnect_reported {
        ReorgFailure::Retry
    } else {
        *disconnect_reported = true;
        ReorgFailure::Report
    }
}

async fn retry_pause(duration: Duration, cancellation: &CancellationToken) -> Result<(), P2pError> {
    tokio::select! {
        () = cancellation.cancelled() => Err(P2pError::Cancelled),
        () = tokio::time::sleep(duration) => Ok(()),
    }
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
            | P2pError::ExpectationMismatch(_)
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

/// Request the receipts of `headers`, whose block hashes are `hashes`, from
/// one peer. `bodies`, once known, bound each block's receipts by its
/// transaction count.
async fn request_direct_receipts(
    peer: &DirectPeer,
    headers: &[Header],
    hashes: &[B256],
    bodies: Option<&[BlockBody]>,
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
        EthVersion::Eth70 => {
            request_direct_receipts70(peer, headers, hashes, bodies, timeout, cancellation).await
        }
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
    headers: &[Header],
    hashes: &[B256],
    bodies: Option<&[BlockBody]>,
    timeout: Duration,
    cancellation: &CancellationToken,
) -> Result<DirectReceiptsResponse, P2pError> {
    let mut assembled = Receipts70Accumulator::default();
    let mut requested_block_hashes = 0_usize;
    let deadline = Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(P2pError::Timeout {
                component: "receipts",
            });
        }
        let (block_index, receipt_index) = assembled.cursor();
        let block_hashes = hashes.get(block_index..).unwrap_or_default();
        requested_block_hashes = requested_block_hashes.saturating_add(block_hashes.len());
        let (sender, receiver) = tokio::sync::oneshot::channel();
        peer.messages
            .try_send(PeerRequest::GetReceipts70 {
                request: GetReceipts70 {
                    first_block_receipt_index: u64::try_from(receipt_index).unwrap_or(u64::MAX),
                    block_hashes: block_hashes.to_vec(),
                },
                response: sender,
            })
            .map_err(|error| P2pError::Request {
                component: "receipts",
                detail: format!("could not queue eth/70 request: {error:?}"),
            })?;
        let response =
            await_direct_peer_response(receiver, remaining, cancellation, "receipts").await?;
        let response_payload_bytes = response.length();
        if assembled.merge(
            headers,
            bodies,
            response.last_block_incomplete,
            response.receipts,
            response_payload_bytes,
        )? {
            return Ok(DirectReceiptsResponse {
                physical_requests: u64::try_from(assembled.rounds).unwrap_or(u64::MAX),
                requested_block_hashes,
                response_payload_bytes: assembled.response_bytes,
                receipts: assembled.blocks,
            });
        }
    }
}

/// Receipts an eth/70 request assembles across continuation rounds: every
/// completed block, then possibly one partial block the next round continues.
#[derive(Debug, Default)]
struct Receipts70Accumulator {
    blocks: Vec<Vec<Receipt>>,
    /// Whether the last block awaits more receipts.
    partial: bool,
    rounds: usize,
    response_bytes: usize,
}

impl Receipts70Accumulator {
    /// The first block the next round asks for, and its first receipt.
    fn cursor(&self) -> (usize, usize) {
        match self.blocks.last() {
            Some(last) if self.partial => (self.blocks.len().saturating_sub(1), last.len()),
            _ => (self.blocks.len(), 0),
        }
    }

    fn completed(&self) -> usize {
        self.blocks.len().saturating_sub(usize::from(self.partial))
    }

    fn partial_receipts(&self) -> usize {
        self.blocks
            .last()
            .filter(|_| self.partial)
            .map_or(0, Vec::len)
    }

    /// Merge one round's response to a request for the receipts of
    /// `headers`, and return whether the request is complete. The request
    /// takes at most `MAX_RECEIPTS70_ROUNDS` rounds and
    /// `MAX_RECEIPTS70_BYTES` of responses. A block never holds more receipts
    /// than its known transactions, and each block is checked against its
    /// header as it completes. A round must complete a block, even one
    /// without receipts, or add receipts to the partial one.
    fn merge(
        &mut self,
        headers: &[Header],
        bodies: Option<&[BlockBody]>,
        last_block_incomplete: bool,
        receipts: Vec<Vec<Receipt>>,
        response_bytes: usize,
    ) -> Result<bool, P2pError> {
        self.rounds = self.rounds.saturating_add(1);
        self.response_bytes = self.response_bytes.saturating_add(response_bytes);
        if self.response_bytes > MAX_RECEIPTS70_BYTES {
            return Err(P2pError::Request {
                component: "receipts",
                detail: format!(
                    "eth/70 continuation exceeded {MAX_RECEIPTS70_BYTES} response bytes"
                ),
            });
        }
        let (block_index, _) = self.cursor();
        let remaining = headers.len().saturating_sub(block_index);
        if receipts.is_empty() || receipts.len() > remaining {
            return Err(P2pError::IncompleteResponse {
                component: "receipts",
                returned: receipts.len(),
                expected: remaining,
            });
        }
        let completed = self.completed();
        let partial_receipts = self.partial_receipts();
        for (offset, mut block) in receipts.into_iter().enumerate() {
            let index = block_index.saturating_add(offset);
            if offset == 0
                && self.partial
                && let Some(partial) = self.blocks.last_mut()
            {
                partial.append(&mut block);
            } else {
                self.blocks.push(block);
            }
            if let (Some(body), Some(block)) = (
                bodies.and_then(|bodies| bodies.get(index)),
                self.blocks.get(index),
            ) && block.len() > body.transactions.len()
            {
                return Err(P2pError::InvalidResponse(format!(
                    "{} receipts for {} transactions at block {}",
                    block.len(),
                    body.transactions.len(),
                    headers.get(index).map_or(0, |header| header.number)
                )));
            }
        }
        self.partial = last_block_incomplete;
        for index in completed..self.completed() {
            if let (Some(header), Some(block)) = (headers.get(index), self.blocks.get(index)) {
                validate_receipts_against_headers(
                    std::slice::from_ref(header),
                    std::slice::from_ref(block),
                )?;
            }
        }
        if self.completed() == completed && self.partial_receipts() <= partial_receipts {
            // A reply that repeats the partial block serves nothing new; the
            // peer may be unable to fit its next receipt in one response.
            return Err(P2pError::IncompleteResponse {
                component: "receipts",
                returned: 0,
                expected: remaining,
            });
        }
        if !self.partial {
            return Ok(true);
        }
        if self.rounds >= MAX_RECEIPTS70_ROUNDS {
            return Err(P2pError::Request {
                component: "receipts",
                detail: "eth/70 continuation round limit exceeded".to_owned(),
            });
        }
        Ok(false)
    }
}

/// Whether the expectation a peer response is checked against was verified
/// independently of the peers that serve it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExpectationTrust {
    /// A consensus-verified anchor, or a hash proven to link to one.
    Verified,
    /// A peer claim, such as a handshake-status head, that honest peers on
    /// another branch or behind the claim may contradict.
    Unverified,
}

/// An expected tip hash and the trust of its source.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ExpectedTip {
    hash: BlockHash,
    trust: ExpectationTrust,
}

/// Penalty class of a peer response that failed validation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ResponseFault {
    /// Broken commitments, structure or block numbers, or a contradiction of a
    /// verified expectation: ban the session and persist the failure.
    Invalid,
    /// A mismatch with an unverified expectation, or an empty, short or
    /// unknown reply: no ban and no persisted failure, at most a short local
    /// cooldown.
    Disagreement,
}

/// Classify a peer response that failed validation against an expectation of
/// the given trust. Only material that breaks its own commitments, or that
/// contradicts a verified expectation, proves the peer invalid: honest peers
/// on another branch, or behind a claimed head, contradict unverified ones.
const fn classify_response_failure(
    error: &P2pError,
    expectation: ExpectationTrust,
) -> ResponseFault {
    match error {
        P2pError::InvalidResponse(_) => ResponseFault::Invalid,
        P2pError::ExpectationMismatch(_) => match expectation {
            ExpectationTrust::Verified => ResponseFault::Invalid,
            ExpectationTrust::Unverified => ResponseFault::Disagreement,
        },
        _ => ResponseFault::Disagreement,
    }
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
        return Err(P2pError::ExpectationMismatch(format!(
            "tip hash {}, expected {expected}",
            tip.hash_slow()
        )));
    }
    Ok(())
}

/// Validate a reply to a request for the qualification target by hash: the
/// target block, and only it. No block is an incomplete reply.
fn validate_target_header(target: BlockRef, mut headers: Vec<Header>) -> Result<Header, P2pError> {
    let Some(header) = headers.pop() else {
        return Err(P2pError::IncompleteResponse {
            component: "headers",
            returned: 0,
            expected: 1,
        });
    };
    if !headers.is_empty()
        || header.number != target.number.0
        || header.hash_slow() != B256::from(*target.hash.as_array())
    {
        return Err(P2pError::InvalidResponse(
            "peer returned a mismatched verified anchor header".to_owned(),
        ));
    }
    Ok(header)
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
    // A peer that does not have the requested block answers with none.
    let Some(first) = headers.first() else {
        return Err(P2pError::IncompleteResponse {
            component: "reorg headers",
            returned: 0,
            expected: 1,
        });
    };
    // The request is by hash: another block breaks the protocol, while the
    // requested block at another height only contradicts the claimed number.
    if first.hash_slow() != B256::from(*head_hash.as_array()) {
        return Err(P2pError::InvalidResponse(
            "descending header response does not start at the requested head".to_owned(),
        ));
    }
    if first.number != head_number.0 {
        return Err(P2pError::ExpectationMismatch(format!(
            "requested head is block {}, expected {}",
            first.number, head_number.0
        )));
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
        // Only a branch that reaches the oldest retained block without
        // joining the retained chain proves a fork below the window; a
        // shorter reply proves nothing yet.
        let reached = descending.last().map(|header| header.number);
        let oldest = recent.front().map(|block| block.number.0);
        if let (Some(reached), Some(oldest)) = (reached, oldest)
            && reached > oldest
        {
            return Err(P2pError::IncompleteResponse {
                component: "reorg headers",
                returned: descending.len(),
                expected: descending.len().saturating_add(
                    usize::try_from(reached.saturating_sub(oldest)).unwrap_or(usize::MAX),
                ),
            });
        }
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

/// Whether a peer must be asked for a block's receipts. A block without
/// transactions commits to the empty receipts root: its receipts are known.
fn has_receipts(header: &Header) -> bool {
    header.receipts_root != EMPTY_ROOT_HASH
}

/// Receipts for `headers` from those fetched, in order, for the blocks that
/// have receipts, with the empty receipts of the others. Fetched receipts
/// that run out end the list early, and any left over follow it, so
/// validating the list against `headers` still reports the mismatch.
fn with_known_empty_receipts(headers: &[Header], fetched: Vec<Vec<Receipt>>) -> Vec<Vec<Receipt>> {
    let mut fetched = fetched.into_iter();
    let mut receipts = Vec::with_capacity(headers.len());
    for header in headers {
        if !has_receipts(header) {
            receipts.push(Vec::new());
        } else if let Some(block) = fetched.next() {
            receipts.push(block);
        } else {
            break;
        }
    }
    receipts.extend(fetched);
    receipts
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
        finality: Finality::Included,
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
            finality: Finality::Included,
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
            finality: Finality::Included,
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
    // A window's frames, built from its peer responses, are held together.
    if *total_bytes > budget.max_resident_bytes {
        return Err(P2pError::Source(SourceError::BudgetExceeded {
            resource: "resident_bytes",
            limit: budget.max_resident_bytes,
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
        finality: Finality::Included,
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
    /// A self-consistent response that contradicts the block the request
    /// expected. Whether that proves the peer invalid depends on how the
    /// expectation was obtained.
    #[error("peer response contradicts the expected chain: {0}")]
    ExpectationMismatch(String),
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
            P2pError::InvalidResponse(detail) | P2pError::ExpectationMismatch(detail) => {
                Self::CorruptFrame(detail)
            }
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
        Arc::new(DirectPeerPool::new(Arc::new(ExecutionPeerStore::new(
            None,
            DEFAULT_PEER_STORE_MAX_ENTRIES,
        ))))
    }

    fn node_record(marker: u8) -> NodeRecord {
        node_record_at(marker, Ipv4Addr::LOCALHOST)
    }

    #[test]
    fn peer_candidate_registry_evicts_oldest_admissions_at_its_bound() {
        let registry = PeerCandidateRegistry::new(2);
        let first = node_record(1).id;
        let second = node_record(2).id;
        let third = node_record(3).id;
        assert!(registry.admit(first, NetworkPeerOrigin::CachedBroad));
        registry
            .admissions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_mut(&first)
            .expect("first admission")
            .admitted_at = Instant::now()
            .checked_sub(Duration::from_secs(1))
            .expect("one second before now is representable");
        assert!(registry.admit(second, NetworkPeerOrigin::DnsTree));
        assert!(registry.admit(third, NetworkPeerOrigin::Trusted));

        let admissions = registry
            .admissions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(admissions.len(), 2);
        assert!(!admissions.contains_key(&first));
        assert!(admissions.contains_key(&second));
        assert!(admissions.contains_key(&third));
    }

    fn node_record_at(marker: u8, address: Ipv4Addr) -> NodeRecord {
        let secret = SecretKey::from_slice(&[marker; 32]).expect("valid test secret");
        NodeRecord::from_secret_key(
            SocketAddr::V4(SocketAddrV4::new(address, 30_300 + u16::from(marker))),
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
                max_resident_bytes: 1_000_000,
            },
        )
        .expect("normalize");
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[1].block.parent_hash, frames[0].block.hash);
        assert!(frames.iter().all(|frame| frame.validate_shape().is_ok()));
    }

    #[test]
    fn a_normalized_window_must_fit_the_resident_budget() {
        let (_, headers, bodies, receipts) = empty_fixture();
        let budget = SourceBudget {
            max_input_bytes: 1_000_000,
            max_frame_bytes: 1_000_000,
            max_frames: 2,
            max_buffered_frames: 2,
            max_in_flight_requests: 1,
            temporary_disk_bytes: 1,
            max_resident_bytes: 1_000_000,
        };
        let frames = normalize_verified(&headers, &bodies, &receipts, budget).expect("normalize");
        let one_frame = frames[0].estimated_heap_bytes();
        let error = normalize_verified(
            &headers,
            &bodies,
            &receipts,
            SourceBudget {
                max_resident_bytes: one_frame,
                ..budget
            },
        )
        .expect_err("a window of two frames exceeds one frame's worth");
        // Review I1: a window of peer responses was bounded only by what the
        // open may acquire in total.
        assert!(
            matches!(
                error,
                P2pError::Source(SourceError::BudgetExceeded {
                    resource: "resident_bytes",
                    ..
                })
            ),
            "{error:?}"
        );
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
            minimum_finality: Finality::Included,
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
                max_resident_bytes: 1_000_000,
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
            minimum_finality: Finality::Included,
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
                max_resident_bytes: 1_000_000,
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
                max_resident_bytes: 1_000_000,
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
            pending_live_material_delay(&config, &mut attempts, &missing_body, false, true),
            Some(Duration::from_millis(250))
        );
        assert_eq!(
            pending_live_material_delay(&config, &mut attempts, &missing_receipts, false, true),
            Some(Duration::from_millis(500))
        );
        assert_eq!(
            pending_live_material_delay(&config, &mut attempts, &missing_body, false, true),
            Some(Duration::from_secs(1))
        );
        assert_eq!(
            pending_live_material_delay(&config, &mut attempts, &missing_receipts, false, true),
            Some(Duration::from_secs(2))
        );
        assert_eq!(
            pending_live_material_delay(&config, &mut attempts, &missing_body, false, true),
            Some(Duration::from_secs(2))
        );
        assert_eq!(attempts, 5);

        let actual_disconnect = P2pError::Network("peer pool stopped".to_owned());
        assert_eq!(
            pending_live_material_delay(&config, &mut attempts, &actual_disconnect, false, true),
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
                max_resident_bytes: 1_000_000,
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
        assert_eq!(defaults.body_serving_peer_target, 4);
        assert_eq!(defaults.preferred_peers, 16);
        assert_eq!(defaults.max_outbound_peers, 100);
        assert_eq!(defaults.max_concurrent_dials, 30);
        assert_eq!(defaults.peer_refill_interval, Duration::from_secs(1));
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
        for body_serving_peer_target in [0, 17] {
            let config = RethP2pConfig {
                body_serving_peer_target,
                ..RethP2pConfig::default()
            };
            assert!(matches!(
                RethP2pSource::mainnet(config),
                Err(P2pError::InvalidConfig(_))
            ));
        }

        let independent_targets = RethP2pConfig {
            minimum_peers: 8,
            body_serving_peer_target: 4,
            ..RethP2pConfig::default()
        };
        assert!(
            RethP2pSource::mainnet(independent_targets).is_ok(),
            "the connected-peer floor must not become a qualification floor"
        );
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
    fn authenticated_bootstrap_tree_configuration_is_strict() {
        assert!(validate_bootstrap_dns_tree(MAINNET_DNS_DISCOVERY_TREE).is_ok());
        assert!(validate_bootstrap_dns_tree("https://example.com/peers.json").is_err());
        assert!(validate_bootstrap_dns_tree("enrtree://missing-key.example.com").is_err());
    }

    #[tokio::test]
    async fn peer_store_merge_preserves_verified_material_evidence() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let destination = directory.path().join("execution-network.sqlite");
        let sibling = directory.path().join("sibling-network.sqlite");
        let destination_store = ExecutionPeerStore::new(Some(destination.clone()), 10);
        let sibling_store = ExecutionPeerStore::new(Some(sibling.clone()), 10);
        let body_record = node_record(3);
        let receipt_record = node_record(4);
        destination_store.record_success(
            body_record.id,
            PeerMaterialKind::Body,
            25_000_000,
            Duration::from_millis(30),
        );
        sibling_store.record_success(
            receipt_record.id,
            PeerMaterialKind::Receipts,
            25_000_100,
            Duration::from_millis(20),
        );
        destination_store
            .persist(vec![body_record])
            .await
            .expect("persist destination");
        sibling_store
            .persist(vec![receipt_record])
            .await
            .expect("persist sibling");

        let merged = merge_execution_peer_stores(&destination, std::slice::from_ref(&sibling), 10)
            .await
            .expect("merge peer stores")
            .expect("quality evidence exists");
        assert_eq!(
            merged,
            ExecutionPeerStoreMerge {
                total: 2,
                imported: 1
            }
        );
        let reopened = ExecutionPeerStore::new(Some(destination), 10);
        reopened.initialize().await.expect("reopen merged store");
        assert!(reopened.has_material_success(body_record.id, PeerMaterialKind::Body));
        assert!(reopened.has_material_success(receipt_record.id, PeerMaterialKind::Receipts));
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
            Arc::new(MaterialRequestGate::new(1)),
            CancellationToken::new(),
        )
        .await;
        assert_eq!(result.outcome, PeerQualification::BodyServing);
        responder.await.expect("qualification responder");
    }

    #[test]
    fn only_verified_header_outcomes_count_as_header_successes() {
        assert!(qualification_served_header(PeerQualification::BodyServing));
        assert!(qualification_served_header(PeerQualification::HeadersOnly));
        assert!(!qualification_served_header(PeerQualification::Lagging));
        assert!(!qualification_served_header(PeerQualification::Rejected));
        assert!(!qualification_served_header(PeerQualification::TimedOut));
    }

    #[test]
    fn qualification_reset_clears_stale_readiness_even_for_same_target() {
        let target = BlockRef {
            number: BlockNumber(25_000_000),
            hash: BlockHash::new([0x11; 32]),
            parent_hash: BlockHash::new([0x10; 32]),
            timestamp: 1_788_000_000,
        };
        let peer = B512::from([0x42; 64]);
        let qualifications = PeerQualificationPool::new(target);
        qualifications.record(target, peer, PeerQualification::BodyServing);
        assert_eq!(qualifications.ready(target), 1);

        qualifications.reset(target);
        assert_eq!(qualifications.ready(target), 0);
    }

    #[test]
    fn header_only_qualification_does_not_count_as_body_serving() {
        let target = BlockRef {
            number: BlockNumber(25_000_000),
            hash: BlockHash::new([0x11; 32]),
            parent_hash: BlockHash::new([0x10; 32]),
            timestamp: 1_788_000_000,
        };
        let qualifications = PeerQualificationPool::new(target);
        qualifications.record(
            target,
            B512::from([0x42; 64]),
            PeerQualification::HeadersOnly,
        );
        assert_eq!(qualifications.ready(target), 0);
    }

    #[test]
    fn connected_unqualified_peer_is_immediately_available_for_verified_requests() {
        let pool = direct_peer_pool();
        let peer_id = B512::from([0x51; 64]);
        let (sender, _receiver) = tokio::sync::mpsc::channel(1);
        pool.insert(DirectPeer {
            peer_id,
            eth_version: EthVersion::Eth68,
            messages: PeerRequestSender::new(peer_id, sender),
        });

        assert_eq!(pool.len(), 1, "the physical session satisfies the floor");
        assert!(
            pool.try_acquire_excluding(PeerMaterialKind::Body, 1, &HashSet::new(), None,)
                .is_some(),
            "background qualification must rank, not exclude, the session"
        );
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
        let (headers, proof, [first, second, third]) = anchored_header_chain();
        let anchor = block_hash(headers.last().expect("anchor").hash_slow());
        let retained = BlockRange::new(BlockNumber(2_100), BlockNumber(2_199)).expect("retained");
        let mut builder = AnchoredHeaderProofBuilder::new(proof, retained, 1_024, anchor);
        // Ranges are fetched from the anchor down, so each validates on
        // arrival.
        let first_wave = builder.take_wave(2);
        assert_eq!(first_wave, [third, second]);
        assert!(
            builder
                .record(third, Ok((B512::ZERO, proof_segment(&headers, third))))
                .is_empty()
        );
        assert!(
            builder
                .record(
                    second,
                    Err(P2pError::Request {
                        component: "headers",
                        detail: "temporary peer failure".to_owned(),
                    }),
                )
                .is_empty()
        );

        assert!(builder.segments.is_empty());
        // Blocks 2,148..=2,199 are proven: 52 of the 100 retained hashes.
        assert_eq!(builder.retained_hashes.len(), 52);
        // The failed range is fetched again first.
        assert_eq!(builder.pending, VecDeque::from([second, first]));
    }

    #[tokio::test]
    async fn peer_store_pruning_prefers_verified_body_service() {
        let store = ExecutionPeerStore::new(None, 2);
        let proven = node_record(1);
        let recent = node_record(2);
        let excess = node_record(3);
        store.record_success(
            proven.id,
            PeerMaterialKind::Body,
            25_000_000,
            Duration::from_millis(10),
        );
        store
            .persist(vec![proven, recent, excess])
            .await
            .expect("prune in-memory peer store");
        let retained = store.candidate_ids();
        assert_eq!(retained.len(), 2);
        assert!(retained.contains(&proven.id));
    }

    #[tokio::test]
    async fn peer_cache_hot_tier_requires_current_body_service_and_subnet_diversity() {
        let first = node_record_at(1, Ipv4Addr::new(10, 1, 1, 1));
        let same_subnet = node_record_at(2, Ipv4Addr::new(10, 1, 2, 2));
        let diverse = node_record_at(3, Ipv4Addr::new(10, 2, 1, 1));
        let failed = node_record_at(4, Ipv4Addr::new(10, 3, 1, 1));
        let unproven = node_record_at(5, Ipv4Addr::new(10, 4, 1, 1));
        let store = ExecutionPeerStore::new(None, 16);
        for record in [&first, &same_subnet, &diverse, &failed] {
            store.record_success(
                record.id,
                PeerMaterialKind::Body,
                1,
                Duration::from_millis(10),
            );
        }
        store.record_failure(failed.id, "session closed");
        store
            .persist(vec![first, same_subnet, diverse, failed, unproven])
            .await
            .expect("persist candidates");

        let cached = prioritized_peer_cache_records(&store, 2);
        assert_eq!(cached.hot.len(), 2);
        assert_ne!(
            peer_subnet(cached.hot[0].address),
            peer_subnet(cached.hot[1].address)
        );
        assert!(cached.hot.iter().all(|record| record.id != failed.id));
        assert!(cached.broad.iter().any(|record| record.id == failed.id));
        assert!(cached.broad.iter().any(|record| record.id == unproven.id));
        assert_eq!(cached.hot.len() + cached.broad.len(), 5);
    }

    #[tokio::test]
    async fn peer_store_refresh_retains_broad_records_absent_from_current_session() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("execution-network.sqlite");
        let broad = node_record(1);
        let current = node_record(2);
        let store = ExecutionPeerStore::new(Some(path.clone()), 16);
        store
            .persist(vec![broad])
            .await
            .expect("persist broad peer");
        store
            .persist(vec![current])
            .await
            .expect("persist current peer");

        let reopened = ExecutionPeerStore::new(Some(path), 16);
        reopened.initialize().await.expect("reopen peer store");
        assert_eq!(
            reopened.candidate_ids(),
            HashSet::from([broad.id, current.id])
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
            .acquire(
                PeerMaterialKind::Body,
                4,
                Duration::from_millis(10),
                &CancellationToken::new(),
            )
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
            });
            pool.set_qualification(peer_id, PeerQualification::BodyServing);
        }

        let cancellation = CancellationToken::new();
        let preferred_peer = B512::from([3_u8; 64]);
        let preferred = pool
            .acquire_excluding(
                PeerMaterialKind::Body,
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
            .acquire_excluding(
                PeerMaterialKind::Body,
                1,
                Duration::from_secs(1),
                &tried,
                None,
                &cancellation,
            )
            .await
            .expect("first peer acquisition")
            .expect("an untried peer remains");
        assert!(tried.insert(first.peer.peer_id));
        let mut wave = vec![first];
        while let Some(lease) = pool.try_acquire_excluding(PeerMaterialKind::Body, 1, &tried, None)
        {
            assert!(tried.insert(lease.peer.peer_id));
            wave.push(lease);
        }
        assert_eq!(wave.len(), 3, "all connected peers enter the same wave");
        assert!(
            pool.acquire_excluding(
                PeerMaterialKind::Body,
                1,
                Duration::from_secs(1),
                &tried,
                None,
                &cancellation,
            )
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
                .all(|peer| peer.body.failures == 0),
            "a neutral lease must not cool or penalize a peer"
        );
        assert!(
            pool.acquire_excluding(
                PeerMaterialKind::Body,
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
            });
            pool.set_qualification(peer_id, PeerQualification::BodyServing);
        }

        let cancellation = CancellationToken::new();
        for marker in 1_u8..=2 {
            let mut lease = pool
                .acquire_excluding(
                    PeerMaterialKind::Body,
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
        });
        pool.set_qualification(fresh_peer, PeerQualification::BodyServing);
        let lease = pool
            .acquire_excluding(
                PeerMaterialKind::Body,
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

    #[test]
    fn material_failures_are_scoped_to_the_failed_service_lane() {
        let pool = direct_peer_pool();
        let peer_id = B512::from([0x33; 64]);
        let (sender, _receiver) = tokio::sync::mpsc::channel(1);
        pool.insert(DirectPeer {
            peer_id,
            eth_version: EthVersion::Eth68,
            messages: PeerRequestSender::new(peer_id, sender),
        });
        pool.set_qualification(peer_id, PeerQualification::HeadersOnly);

        let excluded = HashSet::new();
        let mut body = pool
            .try_acquire_excluding(PeerMaterialKind::Body, 1, &excluded, None)
            .expect("qualification ranks body work but does not gate verified responses");
        body.failed();
        drop(body);

        assert!(
            pool.try_acquire_excluding(PeerMaterialKind::Body, 1, &excluded, None)
                .is_none(),
            "a failed body lane is cooled before retry"
        );

        let mut receipt = pool
            .try_acquire_excluding(PeerMaterialKind::Receipts, 1, &excluded, None)
            .expect("header-qualified peers may be tried for receipts");
        receipt.failed();
        drop(receipt);

        assert!(
            pool.try_acquire_excluding(PeerMaterialKind::Receipts, 1, &excluded, None)
                .is_none(),
            "a failed receipt lane is cooled before retry"
        );
        assert!(
            pool.try_acquire_excluding(PeerMaterialKind::Header, 1, &excluded, None)
                .is_some(),
            "a receipt failure must not disable header service"
        );
    }

    #[test]
    fn stale_lease_cannot_mutate_a_reconnected_peer_session() {
        let pool = direct_peer_pool();
        let peer_id = B512::from([0x34; 64]);
        let (first_sender, _first_receiver) = tokio::sync::mpsc::channel(1);
        pool.insert(DirectPeer {
            peer_id,
            eth_version: EthVersion::Eth68,
            messages: PeerRequestSender::new(peer_id, first_sender),
        });
        let mut stale_lease = pool
            .try_acquire_excluding(PeerMaterialKind::Body, 1, &HashSet::new(), None)
            .expect("first session lease");
        stale_lease.failed();

        pool.remove(peer_id);
        let (second_sender, _second_receiver) = tokio::sync::mpsc::channel(1);
        pool.insert(DirectPeer {
            peer_id,
            eth_version: EthVersion::Eth68,
            messages: PeerRequestSender::new(peer_id, second_sender),
        });
        let current = pool
            .try_acquire_excluding(PeerMaterialKind::Body, 1, &HashSet::new(), None)
            .expect("reconnected session lease");

        drop(stale_lease);
        let peers = pool
            .peers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let reconnected = peers.first().expect("reconnected peer state");
        assert_eq!(
            reconnected.in_flight, 1,
            "stale lease changed current accounting"
        );
        assert_eq!(
            reconnected.body.failures, 0,
            "stale lease cooled current session"
        );
        drop(peers);
        assert!(
            pool.try_acquire_excluding(PeerMaterialKind::Body, 1, &HashSet::new(), None)
                .is_none(),
            "current session remains at its per-peer limit"
        );
        drop(current);
    }

    #[test]
    fn invalid_peer_removal_also_invalidates_persistent_service_evidence() {
        let peer_store = Arc::new(ExecutionPeerStore::new(
            None,
            DEFAULT_PEER_STORE_MAX_ENTRIES,
        ));
        let direct_peers = Arc::new(DirectPeerPool::new(peer_store.clone()));
        let network = PersistentNetwork {
            state: tokio::sync::Mutex::new(None),
            next_generation: AtomicU64::new(1),
            request_gate: Arc::new(MaterialRequestGate::new(
                DEFAULT_MATERIAL_REQUEST_CONCURRENCY,
            )),
            direct_peers: direct_peers.clone(),
            peer_store: peer_store.clone(),
            qualifications: Arc::new(PeerQualificationPool::new(
                RethP2pSource::mainnet_genesis_block(),
            )),
        };
        let peer_id = B512::from([0x35; 64]);
        let (sender, _receiver) = tokio::sync::mpsc::channel(1);
        direct_peers.insert(DirectPeer {
            peer_id,
            eth_version: EthVersion::Eth68,
            messages: PeerRequestSender::new(peer_id, sender),
        });
        peer_store.record_success(
            peer_id,
            PeerMaterialKind::Body,
            25_000_000,
            Duration::from_millis(1),
        );
        assert!(peer_store.is_available_body_server(peer_id));

        let [session] = direct_peers.sessions()[..] else {
            panic!("one pooled session");
        };
        network.invalidate_peer(peer_id, session.connection_id, "invalid execution material");

        assert!(!peer_store.is_available_body_server(peer_id));
        assert_eq!(direct_peers.len(), 0);
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
        let gate = Arc::new(MaterialRequestGate::new(1));
        let cancellation = CancellationToken::new();
        let occupied = gate
            .acquire(Priority::Normal, &cancellation)
            .await
            .expect("initial slot");
        let (order, mut observed) = tokio::sync::mpsc::unbounded_channel();

        let normal_gate = gate.clone();
        let normal_cancellation = cancellation.clone();
        let normal_order = order.clone();
        let normal = tokio::spawn(async move {
            let _permit = normal_gate
                .acquire(Priority::Normal, &normal_cancellation)
                .await
                .expect("normal slot");
            normal_order.send("normal").expect("record normal");
        });
        tokio::task::yield_now().await;

        let high_gate = gate.clone();
        let high_cancellation = cancellation.clone();
        let high = tokio::spawn(async move {
            let _permit = high_gate
                .acquire(Priority::High, &high_cancellation)
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
            minimum_finality: Finality::Included,
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
                    max_resident_bytes: 1,
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
                    max_resident_bytes: 1,
                },
                CancellationToken::new(),
            )
            .await;
        assert!(matches!(result, Err(SourceError::InvalidPlan(_))));
    }

    #[tokio::test]
    async fn live_subscriptions_follow_only_heads_a_finality_source_publishes() {
        let source = RethP2pSource::mainnet(RethP2pConfig::default()).expect("source");
        // Without a finality source's heads, nothing bounds what the lane
        // includes: live subscriptions fail closed, before networking.
        assert!(
            matches!(
                source.live_attested_heads(),
                Err(SourceError::InvalidPlan(reason)) if reason.contains("attested heads")
            ),
            "a live source without attested heads would follow peers"
        );
        // With them, the source follows what the finality source publishes,
        // through a receiver that cannot publish.
        let heads = leani_source_api::AttestedHeadPublisher::new();
        let source = source.with_attested_heads(heads.subscribe());
        let mut following = source.live_attested_heads().expect("attested heads");
        let chain = header_chain(B256::ZERO, 100..=100, 0);
        let head = attested(&chain[0], 3);
        assert!(heads.publish(head));
        assert_eq!(following.latest(), Some(head));
    }

    #[test]
    fn failed_responses_are_invalid_only_against_verified_expectations() {
        use ExpectationTrust::{Unverified, Verified};
        use ResponseFault::{Disagreement, Invalid};

        let (range, headers, bodies, _) = empty_fixture();
        let other_tip = BlockHash::new([0x99; 32]);
        let tip_mismatch =
            validate_headers(range, &headers, Some(other_tip)).expect_err("another tip");
        let mut discontinuous = headers.clone();
        discontinuous[1].parent_hash = B256::ZERO;
        let broken_continuity =
            validate_headers(range, &discontinuous, None).expect_err("broken continuity");
        let mut renumbered = headers.clone();
        renumbered[0].number = 9;
        let wrong_number = validate_headers(range, &renumbered, None).expect_err("wrong number");
        let short = validate_headers(range, &headers[..1], None).expect_err("short reply");
        let empty = validate_headers(range, &[], None).expect_err("empty reply");
        let mut uncommitted = headers.clone();
        uncommitted[0].transactions_root = B256::ZERO;
        let broken_commitment =
            validate_bodies(&uncommitted, &bodies).expect_err("broken body commitment");
        // A reorg request by hash that the peer answers with the requested
        // block at another height contradicts only the claimed number.
        let tip = headers.last().expect("fixture tip").clone();
        let claimed_number = validate_descending_headers(
            BlockNumber(tip.number + 1),
            block_hash(tip.hash_slow()),
            std::slice::from_ref(&tip),
        )
        .expect_err("claimed number");
        let other_block = validate_descending_headers(
            BlockNumber(tip.number),
            other_tip,
            std::slice::from_ref(&tip),
        )
        .expect_err("another block");
        let timeout = P2pError::Timeout {
            component: "headers",
        };

        let table = [
            (&tip_mismatch, Verified, Invalid),
            (&tip_mismatch, Unverified, Disagreement),
            (&claimed_number, Verified, Invalid),
            (&claimed_number, Unverified, Disagreement),
            (&other_block, Unverified, Invalid),
            (&broken_continuity, Unverified, Invalid),
            (&wrong_number, Unverified, Invalid),
            (&broken_commitment, Unverified, Invalid),
            (&short, Verified, Disagreement),
            (&empty, Unverified, Disagreement),
            (&timeout, Verified, Disagreement),
        ];
        for (error, expectation, fault) in table {
            assert_eq!(
                classify_response_failure(error, expectation),
                fault,
                "{error} against a {expectation:?} expectation"
            );
        }
    }

    #[test]
    fn head_polling_asks_a_small_cohort_and_ignores_not_yet_replies() {
        // Catching up races every eligible peer; polling for the next block
        // asks one or two peers per poll.
        assert_eq!(live_header_fanout(false, 32), (32, usize::MAX));
        assert_eq!(live_header_fanout(true, 32), (2, 2));
        assert_eq!(live_header_fanout(true, 1), (1, 1));

        let not_yet = P2pError::IncompleteResponse {
            component: "headers",
            returned: 0,
            expected: 1,
        };
        let unverified = ExpectationTrust::Unverified;
        assert_eq!(
            live_header_reply_cost(&not_yet, unverified, true),
            HeaderReplyCost::NotYet,
            "an empty reply at the head only says the block is not produced yet"
        );
        assert_eq!(
            live_header_reply_cost(&not_yet, unverified, false),
            HeaderReplyCost::Cooldown,
            "a short catch-up reply still cools the header lane"
        );
        let timeout = P2pError::Timeout {
            component: "headers",
        };
        assert_eq!(
            live_header_reply_cost(&timeout, unverified, true),
            HeaderReplyCost::Cooldown
        );
        let (range, headers, _, _) = empty_fixture();
        let broken = validate_headers(range, &headers[..1], None).expect_err("incomplete");
        assert!(matches!(broken, P2pError::IncompleteResponse { .. }));
        let mut renumbered = headers;
        renumbered[0].number = 9;
        let invalid = validate_headers(range, &renumbered, None).expect_err("wrong number");
        assert_eq!(
            live_header_reply_cost(&invalid, unverified, true),
            HeaderReplyCost::Ban
        );
    }

    #[test]
    fn peer_status_head_claims_never_reach_the_observed_head() {
        let telemetry = NetworkTelemetry::default();
        let session = telemetry.register(NetworkLane::Live);
        let observed = || telemetry.snapshot().sessions[0].observed_head_block;
        let liar = B512::from([0x11; 64]);
        let honest = B512::from([0x12; 64]);
        let hash_only = B512::from([0x13; 64]);
        let declared = declared_peer_heads(
            [
                (liar, Some(u64::MAX), B256::from([0xee; 32])),
                (honest, Some(100), B256::from([0x64; 32])),
                (hash_only, None, B256::from([0x65; 32])),
            ],
            BlockNumber(90),
        );
        assert_eq!(
            declared,
            DeclaredPeerHeads {
                head: Some((BlockNumber(u64::MAX), BlockHash::new([0xee; 32]))),
                hash_only: vec![(hash_only, B256::from([0x65; 32]))],
            }
        );
        let (number, hash) = declared.head.expect("a declared head");

        // Discovery settles every head it returns on the session. A status
        // claim, even one of u64::MAX, and a peer's own header for the hash it
        // advertised only steer the next request...
        assert_eq!(
            settle_discovered_head(&session, DiscoveredHead::Claimed(number, hash)),
            (BlockNumber(u64::MAX), BlockHash::new([0xee; 32]))
        );
        settle_discovered_head(
            &session,
            DiscoveredHead::Claimed(BlockNumber(u64::MAX - 1), BlockHash::new([0x66; 32])),
        );
        assert_eq!(observed(), None, "a claimed head was observed");

        // ...while a header validated at a requested block is observed.
        assert_eq!(
            settle_discovered_head(
                &session,
                DiscoveredHead::Validated(BlockNumber(100), BlockHash::new([0x64; 32]))
            ),
            (BlockNumber(100), BlockHash::new([0x64; 32]))
        );
        assert_eq!(observed(), Some(100));
        settle_discovered_head(&session, DiscoveredHead::Claimed(number, hash));
        assert_eq!(observed(), Some(100), "a claimed head was observed");
    }

    #[test]
    fn qualification_never_clears_verified_material_lanes() {
        let pool = direct_peer_pool();
        let mut receivers = Vec::new();
        for marker in [0x52_u8, 0x53] {
            let peer_id = B512::from([marker; 64]);
            let (sender, receiver) = tokio::sync::mpsc::channel(1);
            receivers.push(receiver);
            pool.insert(DirectPeer {
                peer_id,
                eth_version: EthVersion::Eth68,
                messages: PeerRequestSender::new(peer_id, sender),
            });
        }
        let proven = B512::from([0x52; 64]);
        let lanes = |peer_id: B512| {
            pool.peers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .find(|state| state.peer.peer_id == peer_id)
                .map(|state| (state.header.verified, state.body.verified))
                .expect("pooled peer")
        };
        pool.set_qualification(proven, PeerQualification::BodyServing);
        for outcome in [
            PeerQualification::HeadersOnly,
            PeerQualification::Lagging,
            PeerQualification::TimedOut,
            PeerQualification::Rejected,
        ] {
            pool.set_qualification(proven, outcome);
            assert_eq!(
                lanes(proven),
                (true, true),
                "{outcome:?} cleared verified service evidence"
            );
        }

        let header_only = B512::from([0x53; 64]);
        pool.set_qualification(header_only, PeerQualification::HeadersOnly);
        assert_eq!(lanes(header_only), (true, false));
        drop(receivers);
    }

    #[test]
    fn qualification_targets_compare_by_number_and_hash() {
        let target = BlockRef {
            number: BlockNumber(25_000_000),
            hash: BlockHash::new([0x11; 32]),
            parent_hash: BlockHash::new([0x10; 32]),
            timestamp: 1_788_000_000,
        };
        let peer = B512::from([0x42; 64]);
        let qualifications = PeerQualificationPool::new(target);
        qualifications.record(target, peer, PeerQualification::BodyServing);

        // Every connect() rebuilds the same block with a synthetic parent and
        // a fresh timestamp; that is still the same target.
        let restamped = BlockRef {
            parent_hash: BlockHash::ZERO,
            timestamp: target.timestamp + 12,
            ..target
        };
        qualifications.set_target(restamped);
        assert_eq!(qualifications.ready(restamped), 1);
        assert!(qualifications.peer_is_ready(target, peer));

        let next = BlockRef {
            number: BlockNumber(target.number.0 + 1),
            hash: BlockHash::new([0x12; 32]),
            ..target
        };
        qualifications.set_target(next);
        assert_eq!(qualifications.ready(next), 0);
        assert!(!qualifications.peer_is_ready(target, peer));
    }

    #[tokio::test]
    async fn a_release_between_a_full_check_and_the_wait_is_not_lost() {
        use std::sync::atomic::AtomicBool;

        let changed = tokio::sync::Notify::new();
        let released = AtomicBool::new(false);
        let mut checks = 0_usize;
        let acquired = tokio::time::timeout(
            Duration::from_secs(1),
            wait_for_release(&changed, &CancellationToken::new(), || {
                checks += 1;
                if released.load(Ordering::Acquire) {
                    return Some(());
                }
                // The only slot is released right after this check saw it
                // taken and before the caller waits for a release.
                released.store(true, Ordering::Release);
                changed.notify_waiters();
                None
            }),
        )
        .await;
        assert!(
            acquired.is_ok(),
            "a release racing the capacity check was lost"
        );
        assert_eq!(checks, 2);
    }

    #[tokio::test]
    async fn live_requests_proceed_while_qualification_saturates_the_gate() {
        // One global limit of 32 keeps 4 slots for live requests; small limits
        // reserve at most half, so background work always keeps a slot.
        assert_eq!(material_request_capacity(32, Priority::High), 32);
        assert_eq!(material_request_capacity(32, Priority::Normal), 28);
        assert_eq!(material_request_capacity(4, Priority::Normal), 2);
        assert_eq!(material_request_capacity(1, Priority::Normal), 1);

        let gate = Arc::new(MaterialRequestGate::new(32));
        let cancellation = CancellationToken::new();
        // Background qualification takes every slot it may...
        let mut qualification = Vec::new();
        while let Some(permit) = gate.try_acquire(Priority::Normal) {
            qualification.push(permit);
        }
        assert_eq!(qualification.len(), 28);
        // ...and live requests still proceed on the reserved slots.
        let mut live = Vec::new();
        for _ in 0..4 {
            live.push(
                tokio::time::timeout(
                    Duration::from_millis(250),
                    gate.acquire(Priority::High, &cancellation),
                )
                .await
                .expect("the live request starved behind background qualification")
                .expect("live slot"),
            );
        }
        assert!(
            gate.try_acquire(Priority::High).is_none(),
            "the global limit still holds"
        );
        // While a live request waits, background work may not take a slot,
        // even below its own capacity...
        let waiting = tokio::spawn({
            let gate = gate.clone();
            let cancellation = cancellation.clone();
            async move { gate.acquire(Priority::High, &cancellation).await }
        });
        while gate.high_priority_waiters.load(Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }
        qualification.truncate(qualification.len() - 5);
        assert!(
            gate.try_acquire(Priority::Normal).is_none(),
            "background work took a slot a live request was waiting for"
        );
        // ...the live request gets a freed slot, and background work resumes
        // once no live request waits.
        let freed = tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .expect("the waiting live request is woken")
            .expect("live task")
            .expect("freed slot");
        drop(freed);
        assert!(gate.try_acquire(Priority::Normal).is_some());
    }

    #[test]
    fn local_or_transient_session_errors_keep_peer_evidence() {
        for reason in [
            NetworkDisconnectReason::UselessPeer,
            NetworkDisconnectReason::TcpSubsystemError,
            NetworkDisconnectReason::ConnectionClosed,
            NetworkDisconnectReason::TooManyPeers,
            NetworkDisconnectReason::PingTimeout,
        ] {
            assert!(
                !disconnect_invalidates_service_evidence(reason),
                "{reason:?}"
            );
        }
        for reason in [
            NetworkDisconnectReason::ProtocolBreach,
            NetworkDisconnectReason::UnexpectedHandshakeIdentity,
        ] {
            assert!(
                disconnect_invalidates_service_evidence(reason),
                "{reason:?}"
            );
        }
    }

    #[test]
    fn reconciliation_drops_dead_sessions_and_adopts_missed_ones() {
        let peer = |marker: u8| B512::from([marker; 64]);
        let session = |marker: u8, connection_id: u64, sender_open: bool| PooledPeerSession {
            peer_id: peer(marker),
            connection_id,
            sender_open,
        };
        let pooled = [
            // Active and open: kept.
            session(1, 1, true),
            // Its session ended but the close event was lost: stale.
            session(2, 2, true),
            // Its sender closed, though the peer reconnected: stale.
            session(3, 3, false),
            // Inserted after the snapshot was requested: kept while open...
            session(4, 9, true),
            // ...but dropped once its sender is closed.
            session(6, 10, false),
        ];
        let active = HashSet::from([peer(1), peer(3), peer(5)]);
        let first = reconcile_direct_peers(&pooled, &active, 9, &HashSet::new(), &HashSet::new());
        assert_eq!(
            first,
            DirectPeerReconciliation {
                stale: vec![(peer(2), 2), (peer(3), 3), (peer(6), 10)],
                // The reconnected session behind the closed sender, and the
                // session whose open event was lost, are missing...
                missing: vec![peer(3), peer(5)],
                // ...but its event may only be late, or the session closing.
                adopt: Vec::new(),
            }
        );

        // Still missing one reconciliation later, both are adopted; a session
        // missing for the first time waits for the next one.
        let pooled = [session(1, 1, true), session(4, 9, true)];
        let active = HashSet::from([peer(1), peer(3), peer(4), peer(5), peer(7)]);
        let missing_before = first.missing.iter().copied().collect::<HashSet<_>>();
        assert_eq!(
            reconcile_direct_peers(&pooled, &active, 11, &missing_before, &HashSet::new()),
            DirectPeerReconciliation {
                stale: Vec::new(),
                missing: vec![peer(3), peer(5), peer(7)],
                adopt: vec![peer(3), peer(5)],
            }
        );
        // A session that has left the list since, such as a peer banned after
        // the first snapshot, is not adopted.
        let left = reconcile_direct_peers(
            &pooled,
            &HashSet::from([peer(1), peer(4)]),
            11,
            &missing_before,
            &HashSet::new(),
        );
        assert!(left.missing.is_empty() && left.adopt.is_empty());
    }

    #[test]
    fn reconciliation_only_touches_the_session_it_judged() {
        let pool = direct_peer_pool();
        let peer_id = B512::from([0x61; 64]);
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        pool.insert(DirectPeer {
            peer_id,
            eth_version: EthVersion::Eth68,
            messages: PeerRequestSender::new(peer_id, sender),
        });
        // The session ended without a close event.
        drop(receiver);
        let [stale] = pool.sessions()[..] else {
            panic!("one pooled session");
        };
        assert!(!stale.sender_open);

        // The peer reconnects before the stale entry is dropped: the new
        // session stays, and adopting the peer again does not replace it.
        let (sender, _receiver) = tokio::sync::mpsc::channel(1);
        pool.insert(DirectPeer {
            peer_id,
            eth_version: EthVersion::Eth68,
            messages: PeerRequestSender::new(peer_id, sender),
        });
        assert!(!pool.remove_connection(peer_id, stale.connection_id));
        let (routed, _routed_receiver) = tokio::sync::mpsc::channel(1);
        assert!(!pool.insert_missing(DirectPeer {
            peer_id,
            eth_version: EthVersion::Eth68,
            messages: PeerRequestSender::new(peer_id, routed),
        }));
        let [current] = pool.sessions()[..] else {
            panic!("one pooled session");
        };
        assert!(current.sender_open);
        assert!(pool.remove_connection(peer_id, current.connection_id));
        assert_eq!(pool.len(), 0);
    }

    #[test]
    fn probes_advertise_genesis_rather_than_a_zero_or_unverified_head() {
        let advertised = RethP2pSource::probe_advertised_head();
        assert_ne!(advertised.hash, BlockHash::ZERO);
        assert_eq!(advertised, RethP2pSource::mainnet_genesis_block());
    }

    #[cfg(unix)]
    #[test]
    fn existing_identity_files_are_restricted_to_the_owner() {
        use std::os::unix::fs::PermissionsExt as _;

        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("execution-p2p-secret");
        let mode = |path: &Path| {
            std::fs::metadata(path)
                .expect("identity metadata")
                .permissions()
                .mode()
                & 0o777
        };
        let created = load_or_create_secret_key(Some(&path)).expect("create identity");
        assert_eq!(mode(&path), 0o600);
        assert_eq!(
            std::fs::read_dir(directory.path())
                .expect("identity directory")
                .count(),
            1,
            "only the identity itself is left behind"
        );

        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644))
            .expect("loosen identity");
        let reloaded = load_or_create_secret_key(Some(&path)).expect("reload identity");
        assert_eq!(reloaded.secret_bytes(), created.secret_bytes());
        assert_eq!(
            mode(&path),
            0o600,
            "an identity other users can read is tightened"
        );
    }

    /// Pool header peers whose lanes are qualified, in insertion order.
    fn header_peer_pool(markers: &[u8]) -> (Arc<DirectPeerPool>, Vec<B512>, Vec<PeerReceiver>) {
        let pool = direct_peer_pool();
        let mut peers = Vec::new();
        let mut receivers = Vec::new();
        for marker in markers {
            let peer_id = B512::from([*marker; 64]);
            let (sender, receiver) = tokio::sync::mpsc::channel(1);
            receivers.push(receiver);
            pool.insert(DirectPeer {
                peer_id,
                eth_version: EthVersion::Eth68,
                messages: PeerRequestSender::new(peer_id, sender),
            });
            pool.set_qualification(peer_id, PeerQualification::BodyServing);
            peers.push(peer_id);
        }
        (pool, peers, receivers)
    }

    type PeerReceiver = tokio::sync::mpsc::Receiver<PeerRequest<EthNetworkPrimitives>>;

    /// A header lane's failure count and the end of its cooldown.
    fn header_lane(pool: &DirectPeerPool, peer_id: B512) -> (u32, Instant) {
        pool.peers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .find(|state| state.peer.peer_id == peer_id)
            .map(|state| (state.header.failures, state.header.retry_at))
            .expect("pooled peer")
    }

    /// Whether a cooldown applied between `before` and `after` is the first
    /// step, 250 ms from when it was applied: a check of bounds the test
    /// captured, however long the test itself runs.
    fn first_cooldown_step(retry_at: Instant, before: Instant, after: Instant) -> bool {
        cooldown_step(Duration::from_millis(250), retry_at, before, after)
    }

    /// Whether a cooldown applied between `before` and `after` lasts `step`.
    fn cooldown_step(step: Duration, retry_at: Instant, before: Instant, after: Instant) -> bool {
        retry_at >= before + step && retry_at <= after + step
    }

    /// Move every header-lane cooldown, and every withheld-header strike,
    /// `by` into the past, as if that much time had passed: the pool reads
    /// the real clock.
    fn age_header_lanes(pool: &DirectPeerPool, by: Duration) {
        for state in pool
            .peers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter_mut()
        {
            if let Some(earlier) = state.header.retry_at.checked_sub(by) {
                state.header.retry_at = earlier;
            }
            if let Some(earlier) = state.withheld_until.checked_sub(by) {
                state.withheld_until = earlier;
            }
        }
    }

    /// A peer's withheld-header strikes and the end of its last strike.
    fn withheld_strike(pool: &DirectPeerPool, peer_id: B512) -> (u32, Instant) {
        pool.peers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .find(|state| state.peer.peer_id == peer_id)
            .map(|state| (state.withheld_strikes, state.withheld_until))
            .expect("pooled peer")
    }

    #[test]
    fn head_polls_rotate_to_peers_not_yet_asked_for_the_block() {
        let config = RethP2pConfig::default();
        let (pool, peers, receivers) = header_peer_pool(&[0x81, 0x82, 0x83, 0x84]);
        // The first two peers rank highest, so a choice by rank alone asks
        // them on every poll.
        for peer_id in &peers[..2] {
            pool.quality.record_success(
                *peer_id,
                PeerMaterialKind::Body,
                25_000_000,
                Duration::from_millis(10),
            );
        }
        let request_limit =
            effective_material_concurrency(peers.len(), config.material_request_concurrency, 4);
        let (cohort, _) = live_header_fanout(true, request_limit);
        assert_eq!(cohort, AT_HEAD_HEADER_PEERS);
        let not_yet = P2pError::IncompleteResponse {
            component: "headers",
            returned: 0,
            expected: 1,
        };
        let block = BlockNumber(25_000_001);
        let mut rotation = HeadPollRotation::new(RethP2pConfig::default().retry_backoff);
        let mut cohorts = Vec::new();
        for _ in 0..3 {
            assert!(rotation.begin_poll(block).is_empty());
            let leases = (0..cohort)
                .map(|_| {
                    pool.lease_for_head_poll(&mut rotation, 1)
                        .expect("an eligible peer")
                })
                .collect::<Vec<_>>();
            cohorts.push(
                leases
                    .iter()
                    .map(|lease| lease.peer.peer_id)
                    .collect::<HashSet<_>>(),
            );
            for lease in leases {
                assert_eq!(
                    live_header_reply_cost(&not_yet, ExpectationTrust::Unverified, true),
                    HeaderReplyCost::NotYet
                );
                rotation.record_not_yet(lease.peer.peer_id, Instant::now());
            }
            // The next poll starts at least one production poll interval,
            // plus the first retry pause, later.
            age_header_lanes(&pool, config.poll_interval + Duration::from_millis(250));
        }
        assert_eq!(cohorts[0], HashSet::from([peers[0], peers[1]]));
        assert!(
            cohorts[0].is_disjoint(&cohorts[1]),
            "two consecutive polls asked the same peers: {cohorts:?}"
        );
        // Once every eligible peer has been asked, the rotation starts over.
        assert_eq!(cohorts[2], cohorts[0]);
        drop(receivers);
    }

    #[test]
    fn head_polls_cool_peers_that_withheld_a_served_block() {
        let (pool, _, receivers) = header_peer_pool(&[0x91, 0x92, 0x93]);
        let block = BlockNumber(25_000_001);
        let mut rotation = HeadPollRotation::new(RethP2pConfig::default().retry_backoff);
        assert!(rotation.begin_poll(block).is_empty());
        let withholder = pool
            .lease_for_head_poll(&mut rotation, 1)
            .expect("a peer to poll");
        let server = pool
            .lease_for_head_poll(&mut rotation, 1)
            .expect("a second peer to poll");
        let (withholder_id, server_id) = (withholder.peer.peer_id, server.peer.peer_id);
        // One peer answers "not yet", which costs it nothing yet, while the
        // other, asked with it, serves the block.
        let answered_at = Instant::now();
        rotation.record_not_yet(withholder_id, answered_at);
        drop(withholder);
        rotation.record_served(server_id, answered_at);
        drop(server);
        assert_eq!(header_lane(&pool, withholder_id).0, 0);

        // The lane moves on: the peer withheld, or lagged, a block another
        // peer served, and gets the normal header-lane cooldown, 250 ms at
        // first. Nothing is persisted, and it stays pooled.
        let withheld = rotation.begin_poll(BlockNumber(block.0 + 1));
        assert_eq!(withheld, vec![withholder_id]);
        let before = Instant::now();
        for peer_id in withheld {
            pool.record_material_failure(peer_id, PeerMaterialKind::Header);
        }
        let after = Instant::now();
        let (failures, retry_at) = header_lane(&pool, withholder_id);
        assert_eq!(failures, 1);
        assert!(first_cooldown_step(retry_at, before, after));
        assert_eq!(pool.len(), 3);
        // The next poll asks the other peers while it cools.
        let polled = (0..2)
            .map(|_| {
                pool.lease_for_head_poll(&mut rotation, 1)
                    .expect("an eligible peer")
            })
            .collect::<Vec<_>>();
        assert!(
            polled
                .iter()
                .all(|lease| lease.peer.peer_id != withholder_id)
        );
        drop(polled);
        drop(receivers);
    }

    #[test]
    fn minimum_live_head_empties_earn_the_normal_cooldown() {
        // The minimum live head is the verified tip or the block after the
        // finalized anchor: it exists, so `minimum_live_head` is no head poll
        // (it passes no rotation, which only the lane's head poll holds), and
        // an empty reply is a lagging peer.
        let empty = P2pError::IncompleteResponse {
            component: "headers",
            returned: 0,
            expected: 1,
        };
        assert_eq!(
            live_header_reply_cost(&empty, ExpectationTrust::Unverified, false),
            HeaderReplyCost::Cooldown
        );
        // That is the normal header-lane cooldown, 250 ms at first.
        let (pool, peers, receivers) = header_peer_pool(&[0xa1]);
        let mut lease = pool
            .try_acquire_excluding(PeerMaterialKind::Header, 1, &HashSet::new(), None)
            .expect("the peer");
        lease.failed();
        let before = Instant::now();
        drop(lease);
        let after = Instant::now();
        let (failures, retry_at) = header_lane(&pool, peers[0]);
        assert_eq!(failures, 1);
        assert!(first_cooldown_step(retry_at, before, after));
        drop(receivers);
    }

    #[test]
    fn reconciliation_never_readopts_a_peer_dropped_for_invalid_material() {
        let peer = |marker: u8| B512::from([marker; 64]);
        // Reth keeps a trusted peer's session despite a ban, so a peer dropped
        // for invalid material can stay active.
        let invalidated = HashSet::from([peer(8)]);
        let active = HashSet::from([peer(1), peer(8)]);
        let pooled = [PooledPeerSession {
            peer_id: peer(1),
            connection_id: 1,
            sender_open: true,
        }];
        let first = reconcile_direct_peers(&pooled, &active, 2, &HashSet::new(), &invalidated);
        let missing_before = first.missing.iter().copied().collect::<HashSet<_>>();
        let second = reconcile_direct_peers(&pooled, &active, 3, &missing_before, &invalidated);
        assert!(
            second.adopt.is_empty(),
            "a peer dropped for invalid material was adopted again: {second:?}"
        );
        assert!(second.missing.is_empty());
    }

    #[test]
    fn invalidated_peers_stay_out_of_the_pool_until_their_session_closes() {
        let (pool, peers, receivers) = header_peer_pool(&[0xb1]);
        let peer_id = peers[0];
        let session = || {
            let (sender, receiver) = tokio::sync::mpsc::channel(1);
            (
                DirectPeer {
                    peer_id,
                    eth_version: EthVersion::Eth68,
                    messages: PeerRequestSender::new(peer_id, sender),
                },
                receiver,
            )
        };
        let connection_id = |pool: &DirectPeerPool| {
            let [session] = pool.sessions()[..] else {
                panic!("one pooled session");
            };
            session.connection_id
        };
        assert!(pool.invalidate(peer_id, connection_id(&pool)));
        assert_eq!(pool.len(), 0);
        assert_eq!(pool.invalidated(), HashSet::from([peer_id]));
        // The reconciler cannot adopt the peer while its session lasts...
        let (adopted, _adopted_receiver) = session();
        assert!(!pool.insert_missing(adopted));
        // ...but once that session closes, a later one joins again.
        pool.session_closed(peer_id);
        assert!(pool.invalidated().is_empty());
        let (adopted, _adopted_receiver) = session();
        assert!(pool.insert_missing(adopted));
        // A new session announced by Reth starts over as well.
        assert!(pool.invalidate(peer_id, connection_id(&pool)));
        let (announced, _announced_receiver) = session();
        pool.insert(announced);
        assert!(pool.invalidated().is_empty());
        assert_eq!(pool.len(), 1);
        drop(receivers);
    }

    #[tokio::test]
    async fn a_release_reaches_the_waiter_that_registered_first() {
        use std::sync::atomic::AtomicBool;

        // A single release (`notify_one`) wakes the waiter that registered
        // first. A waiter must register before its capacity check, or one
        // that registers later takes the release meant for it.
        let changed = tokio::sync::Notify::new();
        let mut later = std::pin::pin!(changed.notified());
        let released = AtomicBool::new(false);
        let acquired = tokio::time::timeout(
            Duration::from_secs(1),
            wait_for_release(&changed, &CancellationToken::new(), || {
                if released.load(Ordering::Acquire) {
                    return Some(());
                }
                // Another waiter registers after this check, and the only
                // slot is released to one waiter.
                later.as_mut().enable();
                released.store(true, Ordering::Release);
                changed.notify_one();
                None
            }),
        )
        .await;
        assert!(
            acquired.is_ok(),
            "the release went to a waiter that registered later"
        );
    }

    #[test]
    fn live_lane_reconnects_once_the_watchdog_recycles_its_manager() {
        let config = RethP2pConfig::default();
        let source = RethP2pSource::mainnet(config.clone()).expect("source");
        let grace = source.descriptor.expected_lag.max(config.poll_interval);
        // Five minutes without peers end the manager task, and only
        // `connect` builds another: the lane's session is then dead.
        let started = tokio::time::Instant::now();
        let mut zero_peers_since = Some(started);
        assert_eq!(config.peer_recovery_timeout, Duration::from_mins(5));
        assert!(peer_recovery_due(
            0,
            &mut zero_peers_since,
            started + config.peer_recovery_timeout,
            config.peer_recovery_timeout,
        ));
        assert_eq!(
            head_unavailable(false, config.poll_interval, grace),
            HeadUnavailable::Reconnect,
            "the lane kept the session of a recycled manager"
        );
        assert_eq!(
            head_unavailable(false, config.peer_recovery_timeout, grace),
            HeadUnavailable::Reconnect
        );
        // While the manager runs, its peers may still serve the head: the
        // lane keeps the session and reports itself disconnected once the
        // grace has passed.
        assert_eq!(
            head_unavailable(true, config.poll_interval, grace),
            HeadUnavailable::Retry
        );
        assert_eq!(
            head_unavailable(true, grace, grace),
            HeadUnavailable::Report
        );
    }

    #[test]
    fn a_silent_head_poll_peer_does_not_disconnect_the_lane() {
        let config = RethP2pConfig::default();
        let mut attempts = 0;
        // The last peer of a head poll's cohort timed out, or its session
        // could not take the request: the next poll asks other peers.
        let silent = P2pError::Timeout {
            component: "headers",
        };
        assert_eq!(
            pending_live_material_delay(&config, &mut attempts, &silent, true, true),
            Some(Duration::from_millis(250)),
            "a silent head-poll peer disconnected the lane"
        );
        let unqueued = P2pError::Request {
            component: "headers",
            detail: "could not queue direct request: Full(..)".to_owned(),
        };
        assert_eq!(
            pending_live_material_delay(&config, &mut attempts, &unqueued, true, true),
            Some(Duration::from_millis(500))
        );
        // Once the manager has stopped, every failure reconnects.
        let not_yet = P2pError::IncompleteResponse {
            component: "headers",
            returned: 0,
            expected: 1,
        };
        assert_eq!(
            pending_live_material_delay(&config, &mut attempts, &not_yet, true, false),
            None
        );
        assert_eq!(
            pending_live_material_delay(&config, &mut attempts, &silent, true, false),
            None
        );
        // So do the same failures while catching up, and a pool without any
        // peer, whose reconnection waits for one.
        assert_eq!(
            pending_live_material_delay(&config, &mut attempts, &silent, false, true),
            None
        );
        let no_peer = P2pError::Timeout {
            component: "direct peer availability",
        };
        assert_eq!(
            pending_live_material_delay(&config, &mut attempts, &no_peer, true, true),
            None
        );
    }

    #[test]
    fn live_material_waves_end_after_eight_waves_or_a_minute() {
        let config = RethP2pConfig::default();
        // A wave in which no peer serves the material pauses, as in
        // production, before the next one...
        let mut waves = 0;
        let mut elapsed = Duration::ZERO;
        let mut pauses = Vec::new();
        while let Some(pause) = next_live_material_wave(&config, &mut waves, elapsed) {
            assert!(waves < 64, "live material waves never end");
            pauses.push(pause);
            elapsed += pause;
        }
        // ...until eight waves have asked every eligible peer.
        assert_eq!(waves, 8);
        assert_eq!(
            pauses,
            [
                Duration::from_millis(250),
                Duration::from_millis(500),
                Duration::from_secs(1),
                Duration::from_secs(2),
                Duration::from_secs(2),
                Duration::from_secs(2),
                Duration::from_secs(2),
            ]
        );
        // Slow waves end once a minute has passed.
        let mut waves = 0;
        assert_eq!(
            next_live_material_wave(&config, &mut waves, Duration::from_mins(1)),
            None,
            "live material waves have no time bound"
        );
        // The lane then reports itself disconnected, which clears readiness,
        // and asks for the header again.
        let withheld = withheld_live_material("bodies", 25_000_001, 8);
        let mut attempts = 0;
        assert_eq!(
            pending_live_material_delay(&config, &mut attempts, &withheld, true, true),
            None
        );
    }

    /// Headers for blocks `numbers`, the first child of `parent`. The
    /// timestamp tells branches apart.
    fn header_chain(
        parent: B256,
        numbers: std::ops::RangeInclusive<u64>,
        timestamp: u64,
    ) -> Vec<Header> {
        let mut parent = parent;
        numbers
            .map(|number| {
                let header = Header {
                    number,
                    parent_hash: parent,
                    timestamp,
                    ..Default::default()
                };
                parent = header.hash_slow();
                header
            })
            .collect()
    }

    /// A header chain for blocks 100..=2199, whose tip is the anchor, and the
    /// three ranges that prove it.
    fn anchored_header_chain() -> (Vec<Header>, BlockRange, [BlockRange; 3]) {
        let range = |start, end| {
            BlockRange::new(BlockNumber(start), BlockNumber(end)).expect("proof range")
        };
        (
            header_chain(B256::ZERO, 100..=2_199, 0),
            range(100, 2_199),
            [range(100, 1_123), range(1_124, 2_147), range(2_148, 2_199)],
        )
    }

    fn proof_segment(headers: &[Header], range: BlockRange) -> HeaderProofSegment {
        let offset = usize::try_from(range.start().0 - 100).expect("offset");
        let len = usize::try_from(range.len()).expect("length");
        header_proof_segment(range, &headers[offset..offset + len]).expect("proof segment")
    }

    fn rejected_peers(rejected: &[(B512, P2pError)]) -> Vec<B512> {
        assert!(rejected.iter().all(|(_, error)| {
            classify_response_failure(error, ExpectationTrust::Verified) == ResponseFault::Invalid
        }));
        rejected.iter().map(|(peer, _)| *peer).collect()
    }

    #[test]
    fn anchored_header_proof_recovers_from_a_bad_segment() {
        let (headers, proof, [first, second, third]) = anchored_header_chain();
        let anchor = block_hash(headers.last().expect("anchor").hash_slow());
        let (honest, liar) = (B512::from([0x71; 64]), B512::from([0x72; 64]));
        // A peer serves a chain of its own for the middle range: it joins the
        // segment below, but not the one above.
        let forged = header_proof_segment(
            second,
            &header_chain(headers[1_023].hash_slow(), 1_124..=2_147, 1),
        )
        .expect("forged segment");
        let mut builder = AnchoredHeaderProofBuilder::new(proof, proof, 1_024, anchor);
        assert_eq!(builder.take_wave(3).len(), 3);
        let segment = |range| Ok((honest, proof_segment(&headers, range)));
        assert!(builder.record(third, segment(third)).is_empty());
        // Checked against the proven segment above it, the forged one fails,
        // and its peer is reported...
        assert_eq!(
            rejected_peers(&builder.record(second, Ok((liar, forged)))),
            [liar]
        );
        assert!(builder.record(first, segment(first)).is_empty());
        // ...while its range is fetched again...
        assert_eq!(
            builder.pending,
            VecDeque::from([second]),
            "a bad segment wedged the proof"
        );
        // ...and the proof completes once another peer serves it.
        assert_eq!(builder.take_wave(3), [second]);
        assert!(builder.record(second, segment(second)).is_empty());
        let assembled = builder
            .finish()
            .expect("the proof recovers from a bad segment");
        assert_eq!(assembled.hashes.len(), 2_100);
        assert_eq!(
            assembled.expected_hash(BlockNumber(1_500)),
            Some(block_hash(headers[1_400].hash_slow()))
        );
    }

    #[test]
    fn anchored_header_proof_recovers_from_a_wrong_tip() {
        let (headers, proof, [first, second, third]) = anchored_header_chain();
        let anchor = block_hash(headers.last().expect("anchor").hash_slow());
        // The top segment joins the chain below it but ends at another block
        // than the finalized anchor.
        let forged = header_proof_segment(
            third,
            &header_chain(headers[2_047].hash_slow(), 2_148..=2_199, 1),
        )
        .expect("forged segment");
        let (honest, liar) = (B512::from([0x73; 64]), B512::from([0x74; 64]));
        let mut builder = AnchoredHeaderProofBuilder::new(proof, proof, 1_024, anchor);
        assert_eq!(builder.take_wave(3).len(), 3);
        let segment = |range| Ok((honest, proof_segment(&headers, range)));
        assert!(builder.record(first, segment(first)).is_empty());
        assert!(builder.record(second, segment(second)).is_empty());
        assert_eq!(
            rejected_peers(&builder.record(third, Ok((liar, forged)))),
            [liar]
        );
        assert!(builder.finish().is_err());
        assert_eq!(
            builder.pending,
            VecDeque::from([third]),
            "a wrong tip wedged the proof"
        );
        assert_eq!(builder.take_wave(3), [third]);
        assert!(builder.record(third, segment(third)).is_empty());
        let assembled = builder
            .finish()
            .expect("the proof recovers from a wrong tip");
        assert_eq!(assembled.expected_hash(BlockNumber(2_199)), Some(anchor));
    }

    /// The lane's retained window, blocks 1,036..=1,100, and a branch that
    /// replaces its last three blocks and leads a hundred blocks further.
    fn reorged_live_window() -> (VecDeque<BlockRef>, Vec<Header>) {
        let canonical = header_chain(B256::ZERO, 1_000..=1_100, 0);
        let recent = canonical[36..].iter().map(block_ref).collect();
        let replacement = header_chain(canonical[97].hash_slow(), 1_098..=1_200, 1);
        (recent, [&canonical[..=97], &replacement[..]].concat())
    }

    #[test]
    fn shallow_reorgs_while_behind_are_reconstructed_from_the_replaced_tip() {
        let max_reorg_depth = RethP2pConfig::default().max_reorg_depth;
        let (recent, branch) = reorged_live_window();
        assert_eq!(recent.len(), max_reorg_depth + 1);
        let last = *recent.back().expect("the lane's tip");
        let at = |number: u64| &branch[usize::try_from(number - 1_000).expect("offset")];
        // Catching up to the head discovered at block 1,200, the lane fetched
        // block 1,101 of the branch, which does not extend its tip.
        let head = branch.last().expect("the branch's head");
        assert_eq!(head.number, 1_200);
        let mismatching = block_ref(at(1_101));
        // The full reply a peer gives from a block: the headers below it,
        // as many as the window holds.
        let descending_from = |number: u64| {
            branch
                .iter()
                .rev()
                .skip_while(|header| header.number > number)
                .take(max_reorg_depth + 1)
                .cloned()
                .collect::<Vec<_>>()
        };
        // From the far head, even a full reply stays above the retained
        // window, so it can never find the fork.
        assert!(
            plan_reorg(&recent, &descending_from(head.number)).is_err(),
            "a reply from the far head reached the retained window"
        );
        // From the block that replaced the tip, it covers the window, so the
        // three replaced blocks are reverted.
        let (number, hash) = reorg_reconstruction_tip(last, mismatching);
        let descending = descending_from(number.0);
        validate_descending_headers(number, hash, &descending).expect("descending branch");
        let (ancestor, reverted) = plan_reorg(&recent, &descending).expect("a shallow reorg");
        assert_eq!(ancestor.number, BlockNumber(1_097));
        assert_eq!(reverted.len(), 3);
    }

    #[test]
    fn empty_or_short_reorg_replies_are_retryable_not_invalid() {
        let (recent, branch) = reorged_live_window();
        let tip = &branch[100];
        let (number, hash) = (BlockNumber(tip.number), block_hash(tip.hash_slow()));
        // A peer that does not have the block answers with no headers.
        let empty = validate_descending_headers(number, hash, &[]).expect_err("no headers");
        assert!(
            matches!(empty, P2pError::IncompleteResponse { .. }),
            "an empty reply is {empty}"
        );
        assert_eq!(
            classify_response_failure(&empty, ExpectationTrust::Unverified),
            ResponseFault::Disagreement
        );
        // A consistent reply that ends above the fork proves no deep reorg.
        let short = branch[99..=100].iter().rev().cloned().collect::<Vec<_>>();
        validate_descending_headers(number, hash, &short).expect("a short branch");
        let incomplete = plan_reorg(&recent, &short).expect_err("no ancestor yet");
        assert!(
            matches!(incomplete, P2pError::IncompleteResponse { .. }),
            "a short reply is {incomplete}"
        );
        for retryable in [
            incomplete,
            P2pError::Timeout {
                component: "reorg headers",
            },
        ] {
            assert_ne!(
                reorg_failure(&retryable, ExpectationTrust::Verified, &mut false),
                ReorgFailure::Reset,
                "{retryable}"
            );
        }
        // Only a branch that passes the whole window without joining it
        // proves a reorg too deep to follow.
        let unrelated = header_chain(B256::repeat_byte(0x42), 1_000..=1_100, 2);
        let descending = unrelated.iter().rev().take(65).cloned().collect::<Vec<_>>();
        let too_deep = plan_reorg(&recent, &descending).expect_err("no common block");
        assert!(matches!(too_deep, P2pError::ReorgTooDeep { .. }));
        assert_eq!(
            reorg_failure(&too_deep, ExpectationTrust::Verified, &mut false),
            ReorgFailure::Reset
        );
    }

    fn receipt(cumulative_gas_used: u64) -> Receipt {
        EthereumReceipt {
            tx_type: TxType::Legacy,
            success: true,
            cumulative_gas_used,
            logs: Vec::new(),
        }
    }

    /// A header whose commitments hold exactly `receipts`.
    fn header_committing(number: u64, receipts: &[Receipt]) -> Header {
        let with_bloom = receipts
            .iter()
            .map(alloy_consensus::TxReceipt::with_bloom_ref)
            .collect::<Vec<_>>();
        Header {
            number,
            receipts_root: calculate_receipt_root(&with_bloom),
            gas_used: receipts
                .last()
                .map_or(0, |receipt| receipt.cumulative_gas_used),
            ..Default::default()
        }
    }

    #[test]
    fn eth70_receipt_continuation_is_bounded_and_validated_as_blocks_complete() {
        let large = header_committing(10, &vec![receipt(21_000); 16]);
        // A peer that keeps a block incomplete gets eight rounds...
        let mut assembled = Receipts70Accumulator::default();
        for round in 1..=8 {
            let merged = assembled.merge(
                std::slice::from_ref(&large),
                None,
                true,
                vec![vec![receipt(21_000)]],
                1_024,
            );
            if round < 8 {
                assert!(matches!(merged, Ok(false)), "round {round}: {merged:?}");
            } else {
                assert!(
                    matches!(merged, Err(P2pError::Request { .. })),
                    "eth/70 continuation took a ninth round: {merged:?}"
                );
            }
        }
        // ...and 64 MiB of responses across them.
        let mut assembled = Receipts70Accumulator::default();
        let ten_mib = 10 * 1024 * 1024;
        for round in 1..=7 {
            let merged = assembled.merge(
                std::slice::from_ref(&large),
                None,
                true,
                vec![vec![receipt(21_000)]],
                ten_mib,
            );
            if round < 7 {
                assert!(matches!(merged, Ok(false)), "round {round}: {merged:?}");
            } else {
                assert!(
                    matches!(merged, Err(P2pError::Request { .. })),
                    "eth/70 continuation kept 70 MiB: {merged:?}"
                );
            }
        }
        // A block never holds more receipts than its transactions...
        let transaction =
            alloy_consensus::EthereumTxEnvelope::Legacy(alloy_consensus::Signed::new_unhashed(
                alloy_consensus::TxLegacy::default(),
                alloy_primitives::Signature::test_signature(),
            ));
        let body = BlockBody {
            transactions: vec![transaction],
            ..BlockBody::default()
        };
        let single = header_committing(11, &[receipt(21_000)]);
        let mut assembled = Receipts70Accumulator::default();
        let merged = assembled.merge(
            std::slice::from_ref(&single),
            Some(std::slice::from_ref(&body)),
            true,
            vec![vec![receipt(21_000), receipt(42_000)]],
            1_024,
        );
        assert!(
            matches!(merged, Err(P2pError::InvalidResponse(_))),
            "a block kept more receipts than transactions: {merged:?}"
        );
        // ...and each completed block is checked against its header before
        // the next round.
        let headers = [single.clone(), header_committing(12, &[receipt(21_000)])];
        let mut assembled = Receipts70Accumulator::default();
        let merged = assembled.merge(
            &headers,
            None,
            true,
            vec![vec![receipt(5)], vec![receipt(21_000)]],
            1_024,
        );
        assert!(
            matches!(merged, Err(P2pError::InvalidResponse(_))),
            "a completed block was not validated: {merged:?}"
        );
    }

    #[test]
    fn eth70_accepts_responses_of_empty_blocks() {
        let (_, headers, _, _) = empty_fixture();
        let mut assembled = Receipts70Accumulator::default();
        let merged = assembled.merge(&headers, None, false, vec![Vec::new(), Vec::new()], 3);
        assert!(
            matches!(merged, Ok(true)),
            "a reply of empty blocks was rejected: {merged:?}"
        );
        assert_eq!(assembled.blocks, [Vec::<Receipt>::new(), Vec::new()]);
    }

    #[test]
    fn blocks_without_transactions_are_not_asked_for_receipts() {
        let (_, empty, _, _) = empty_fixture();
        let full = header_committing(12, &[receipt(21_000)]);
        assert!(
            !has_receipts(&empty[0]),
            "an empty block was asked for receipts"
        );
        assert!(has_receipts(&full));
        let headers = [empty[0].clone(), full, empty[1].clone()];
        let receipts = with_known_empty_receipts(&headers, vec![vec![receipt(21_000)]]);
        assert_eq!(receipts, [Vec::new(), vec![receipt(21_000)], Vec::new()]);
        validate_receipts_against_headers(&headers, &receipts).expect("committed receipts");
    }

    #[tokio::test]
    async fn shutdown_stops_the_manager_task_after_a_graceful_network_shutdown() {
        use std::sync::atomic::AtomicBool;

        // The manager future keeps running after Reth's graceful network
        // shutdown: the task ends, and persists the final peer state, only
        // once its token is cancelled.
        let shutdown = CancellationToken::new();
        let torn_down = Arc::new(AtomicBool::new(false));
        let mut task = tokio::spawn({
            let shutdown = shutdown.clone();
            let torn_down = torn_down.clone();
            async move {
                shutdown.cancelled().await;
                tokio::task::yield_now().await;
                torn_down.store(true, Ordering::Release);
            }
        });
        stop_network_task(true, &shutdown, &mut task).await;
        assert!(
            torn_down.load(Ordering::Acquire),
            "shutdown waited out its timeout and aborted the manager's teardown"
        );
    }

    #[test]
    fn head_polls_cool_only_peers_that_withheld_a_block_already_served() {
        let config = RethP2pConfig::default();
        let peer = |marker: u8| B512::from([marker; 64]);
        let block = BlockNumber(25_000_001);
        let slot = Duration::from_secs(12);
        let previous = Instant::now();
        let served_at = previous + slot;
        let mut rotation = HeadPollRotation::new(RethP2pConfig::default().retry_backoff);
        assert!(rotation.begin_poll(block).is_empty());
        // Polls start after the previous block and run about nine times per
        // slot: peers asked before the block exists answer "not yet"...
        rotation.record_not_yet(peer(1), previous + config.retry_backoff);
        rotation.record_not_yet(
            peer(2),
            previous + slot.saturating_sub(config.poll_interval),
        );
        // ...as does a peer asked together with the one that serves it.
        rotation.record_not_yet(
            peer(3),
            previous + slot.saturating_sub(Duration::from_millis(100)),
        );
        rotation.record_served(peer(4), served_at);
        assert_eq!(
            rotation.begin_poll(BlockNumber(block.0 + 1)),
            vec![peer(3)],
            "peers asked before the block existed were cooled"
        );

        // A header the lane did not move on with, such as one whose body no
        // peer served, is no serve: the lane later caught up past the block.
        let mut rotation = HeadPollRotation::new(RethP2pConfig::default().retry_backoff);
        assert!(rotation.begin_poll(block).is_empty());
        rotation.record_served(peer(5), previous + Duration::from_secs(1));
        rotation.serve_failed();
        rotation.record_not_yet(peer(1), previous + Duration::from_secs(3));
        assert!(
            rotation.begin_poll(BlockNumber(block.0 + 2)).is_empty(),
            "a failed serve cooled the peers asked after it"
        );
    }

    #[test]
    fn minimum_live_head_races_a_small_cohort() {
        let config = RethP2pConfig::default();
        // Discovery asks for a block that exists on every live-loop
        // iteration, with the policy `minimum_live_head` passes: a few peers
        // answer it at a time, however many are connected, at live priority.
        let policy = minimum_live_head_policy();
        assert!(policy.priority.is_high());
        for connected in [2, 40, 400] {
            let request_limit = effective_material_concurrency(
                connected,
                config.material_request_concurrency,
                policy.concurrency,
            );
            let (width, _) = live_header_fanout(false, request_limit);
            assert!(
                (2..=4).contains(&width),
                "head discovery races {width} of {connected} peers"
            );
        }
        // It is no head poll: an empty reply cools the peer.
        let empty = P2pError::IncompleteResponse {
            component: "headers",
            returned: 0,
            expected: 1,
        };
        assert_eq!(
            live_header_reply_cost(&empty, ExpectationTrust::Unverified, false),
            HeaderReplyCost::Cooldown
        );
    }

    #[test]
    fn invalidation_only_drops_the_session_that_served_invalid_material() {
        let (pool, peers, receivers) = header_peer_pool(&[0xc1]);
        let peer_id = peers[0];
        let [old] = pool.sessions()[..] else {
            panic!("one pooled session");
        };
        // The peer reconnects before a request on its old session fails with
        // invalid material.
        let (sender, _receiver) = tokio::sync::mpsc::channel(1);
        pool.insert(DirectPeer {
            peer_id,
            eth_version: EthVersion::Eth68,
            messages: PeerRequestSender::new(peer_id, sender),
        });
        let [current] = pool.sessions()[..] else {
            panic!("one pooled session");
        };
        assert!(
            !pool.invalidate(peer_id, old.connection_id),
            "an old session's invalid material dropped the new session"
        );
        assert_eq!(pool.len(), 1);
        assert!(pool.invalidated().is_empty());
        // The session that served it is dropped, and kept out.
        assert!(pool.invalidate(peer_id, current.connection_id));
        assert_eq!(pool.len(), 0);
        assert_eq!(pool.invalidated(), HashSet::from([peer_id]));
        drop(receivers);
    }

    #[test]
    fn failed_material_bans_invalid_peers_and_cools_the_rest() {
        let peer_store = Arc::new(ExecutionPeerStore::new(
            None,
            DEFAULT_PEER_STORE_MAX_ENTRIES,
        ));
        let direct_peers = Arc::new(DirectPeerPool::new(peer_store.clone()));
        let network = PersistentNetwork {
            state: tokio::sync::Mutex::new(None),
            next_generation: AtomicU64::new(1),
            request_gate: Arc::new(MaterialRequestGate::new(
                DEFAULT_MATERIAL_REQUEST_CONCURRENCY,
            )),
            direct_peers: direct_peers.clone(),
            peer_store: peer_store.clone(),
            qualifications: Arc::new(PeerQualificationPool::new(
                RethP2pSource::mainnet_genesis_block(),
            )),
        };
        let (liar, silent) = (B512::from([0xe1; 64]), B512::from([0xe2; 64]));
        let mut receivers = Vec::new();
        for peer_id in [liar, silent] {
            let (sender, receiver) = tokio::sync::mpsc::channel(1);
            receivers.push(receiver);
            direct_peers.insert(DirectPeer {
                peer_id,
                eth_version: EthVersion::Eth70,
                messages: PeerRequestSender::new(peer_id, sender),
            });
            peer_store.record_success(
                peer_id,
                PeerMaterialKind::Body,
                25_000_000,
                Duration::from_millis(1),
            );
        }
        let receipts_lease = |peer_id: B512| {
            let others = [liar, silent]
                .into_iter()
                .filter(|other| *other != peer_id)
                .collect::<HashSet<_>>();
            direct_peers
                .try_acquire_excluding(PeerMaterialKind::Receipts, 1, &others, None)
                .expect("the peer's receipt lane")
        };

        // An eth/70 peer completes a block with receipts its header does not
        // commit to: the reply fails while it is assembled, before any caller
        // validates it.
        let header = header_committing(12, &[receipt(21_000)]);
        let invalid = Receipts70Accumulator::default()
            .merge(
                std::slice::from_ref(&header),
                None,
                false,
                vec![vec![receipt(5)]],
                1,
            )
            .expect_err("receipts the header does not commit to");
        let mut banned = Vec::new();
        let mut lease = receipts_lease(liar);
        let fault = network.penalize_response(
            &mut lease,
            &invalid,
            ExpectationTrust::Verified,
            |peer_id| banned.push(peer_id),
        );
        drop(lease);
        assert_eq!(
            banned,
            [liar],
            "an eth/70 peer that served invalid receipts was not banned"
        );
        assert_eq!(fault, ResponseFault::Invalid);
        assert!(direct_peers.get(liar).is_none());
        assert!(!peer_store.is_available_body_server(liar));

        // A transport failure costs the peer a cooldown on that lane only.
        let timeout = P2pError::Timeout {
            component: "receipts",
        };
        let mut lease = receipts_lease(silent);
        let fault = network.penalize_response(
            &mut lease,
            &timeout,
            ExpectationTrust::Verified,
            |peer_id| banned.push(peer_id),
        );
        drop(lease);
        assert_eq!(fault, ResponseFault::Disagreement);
        assert_eq!(banned, [liar]);
        assert!(peer_store.is_available_body_server(silent));
        let receipt_failures = direct_peers
            .peers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .find(|state| state.peer.peer_id == silent)
            .map(|state| state.receipts.failures);
        assert_eq!(receipt_failures, Some(1));
        drop(receivers);
    }

    #[test]
    fn an_empty_pool_ends_a_live_material_request() {
        // Without a pooled peer, waiting for one fails after the request
        // timeout. The request ends, so the lane reports itself disconnected
        // and reconnects: that waits for peers, and rebuilds a manager the
        // zero-peer watchdog stopped.
        let no_peer = P2pError::Timeout {
            component: "direct peer availability",
        };
        let config = RethP2pConfig::default();
        assert_eq!(
            live_material_wave_end(&config, &mut 0, Duration::ZERO, &no_peer),
            LiveWaveEnd::Fail,
            "an empty pool kept the live body request waiting"
        );
        assert_eq!(
            pending_live_material_delay(&config, &mut 0, &no_peer, true, true),
            None
        );
    }

    #[test]
    fn filtered_log_receipts_are_bounded_like_other_live_material() {
        let config = RethP2pConfig::default();
        // No peer serves the receipts of a validated header's block that the
        // lane's filtered-log request needs.
        let missing = P2pError::IncompleteResponse {
            component: "receipts",
            returned: 0,
            expected: 1,
        };
        let mut waves = 0;
        let mut elapsed = Duration::ZERO;
        let mut ends = Vec::new();
        while ends.len() < 64 {
            let end = live_material_wave_end(&config, &mut waves, elapsed, &missing);
            ends.push(end);
            let LiveWaveEnd::NextWave(pause) = end else {
                break;
            };
            elapsed += pause;
        }
        assert_eq!(
            ends.last(),
            Some(&LiveWaveEnd::Withheld),
            "filtered-log receipts are not bounded like other live material: {ends:?}"
        );
        assert_eq!(waves, 8);
        assert_eq!(
            live_material_wave_end(&config, &mut 0, Duration::from_mins(1), &missing),
            LiveWaveEnd::Withheld
        );
        // The header is then given up, which the lane reports.
        assert_eq!(
            pending_live_material_delay(
                &config,
                &mut 0,
                &withheld_live_material("receipts", 25_000_001, 8),
                true,
                true
            ),
            None
        );
    }

    #[test]
    fn withheld_header_strikes_outlast_header_successes_until_a_frame_completes() {
        let (pool, peers, receivers) = header_peer_pool(&[0xd1, 0xd2]);
        let (fabricator, honest) = (peers[0], peers[1]);
        // The fabricator has served verified bodies before, so it ranks
        // first for headers.
        pool.quality.record_success(
            fabricator,
            PeerMaterialKind::Body,
            25_000_000,
            Duration::from_millis(10),
        );
        let header_race = |excluded: &[B512]| {
            pool.try_acquire_excluding(
                PeerMaterialKind::Header,
                1,
                &excluded.iter().copied().collect(),
                None,
            )
            .map(|lease| lease.peer.peer_id)
        };
        let serve_header = |excluded: &[B512]| {
            let mut lease = pool
                .try_acquire_excluding(
                    PeerMaterialKind::Header,
                    1,
                    &excluded.iter().copied().collect(),
                    None,
                )
                .expect("a header peer");
            lease.succeeded();
            lease.peer.peer_id
        };
        assert_eq!(serve_header(&[]), fabricator);

        // No peer serves the body of the header it served within the bound:
        // the lane gives the header up and strikes its peer.
        let before = Instant::now();
        pool.strike_withheld_header(fabricator);
        let after = Instant::now();
        let first_strike = withheld_strike(&pool, fabricator);
        // While the strike lasts, header races ask other peers.
        assert_eq!(header_race(&[]), Some(honest));
        assert_eq!(header_race(&[honest]), None, "a struck peer was asked");
        // Once it has cooled, the peer serves a header again...
        age_header_lanes(&pool, Duration::from_millis(250));
        assert_eq!(serve_header(&[honest]), fabricator);
        // ...which lifts a lane cooldown, but not the strike: the next header
        // race still asks the honest peer first.
        assert_eq!(
            header_race(&[]),
            Some(honest),
            "a header success cleared the withheld-material strike"
        );
        assert_eq!(first_strike.0, 1);
        assert!(first_cooldown_step(first_strike.1, before, after));

        // The strike escalates like a lane cooldown, up to 30 s.
        let before = Instant::now();
        pool.strike_withheld_header(fabricator);
        let after = Instant::now();
        let (strikes, until) = withheld_strike(&pool, fabricator);
        assert_eq!(strikes, 2);
        assert!(cooldown_step(
            Duration::from_millis(500),
            until,
            before,
            after
        ));
        for _ in 0..8 {
            pool.strike_withheld_header(fabricator);
        }
        let before = Instant::now();
        pool.strike_withheld_header(fabricator);
        let after = Instant::now();
        let (strikes, until) = withheld_strike(&pool, fabricator);
        assert_eq!(strikes, 11);
        assert!(cooldown_step(Duration::from_secs(30), until, before, after));

        // A frame on one of its headers completes: its header was real, the
        // strike clears, and it ranks first again.
        pool.clear_withheld_header(fabricator);
        assert_eq!(withheld_strike(&pool, fabricator).0, 0);
        assert_eq!(header_race(&[]), Some(fabricator));
        drop(receivers);
    }

    #[test]
    fn a_silent_head_poll_cohort_is_reported_once_its_silence_outlasts_the_grace() {
        let config = RethP2pConfig::default();
        let source = RethP2pSource::mainnet(config.clone()).expect("source");
        let grace = source.descriptor.expected_lag.max(config.poll_interval);
        assert_eq!(grace, Duration::from_secs(12));
        let silent = P2pError::Timeout {
            component: "headers",
        };
        let not_yet = P2pError::IncompleteResponse {
            component: "headers",
            returned: 0,
            expected: 1,
        };
        let started = Instant::now();
        let mut attempts = 0;
        let mut silent_since = None;
        let mut poll_failed = |at: Instant, error: &P2pError, manager_current: bool| {
            head_poll_failure(
                &config,
                &mut attempts,
                &mut silent_since,
                at,
                grace,
                error,
                manager_current,
            )
        };
        // The cohort's peers time out: the lane polls others, within the
        // grace...
        assert!(matches!(
            poll_failed(started, &silent, true),
            HeadPollFailure::Retry(_)
        ));
        assert!(matches!(
            poll_failed(
                started + grace.saturating_sub(Duration::from_millis(1)),
                &silent,
                true
            ),
            HeadPollFailure::Retry(_)
        ));
        // ...until no polled peer has answered for the grace: the lane then
        // reports itself disconnected, and keeps polling.
        assert!(
            matches!(
                poll_failed(started + grace, &silent, true),
                HeadPollFailure::Report(_)
            ),
            "a silent head-poll cohort never cleared readiness"
        );
        // A peer that answers, even "not yet", ends the silence.
        assert!(matches!(
            poll_failed(started + grace + config.poll_interval, &not_yet, true),
            HeadPollFailure::Retry(_)
        ));
        assert!(matches!(
            poll_failed(started + grace + config.poll_interval * 2, &silent, true),
            HeadPollFailure::Retry(_)
        ));
        // A stopped manager reconnects.
        assert_eq!(
            poll_failed(started + grace * 3, &silent, false),
            HeadPollFailure::Reconnect
        );
    }

    #[test]
    fn live_progress_restarts_the_head_poll_silence_clock() {
        // Final review B1: only an answered head poll ended a silence, so
        // after a catch-up that followed a silent poll, the next silent poll
        // reported the lane disconnected at once and cleared readiness.
        let config = RethP2pConfig::default();
        let source = RethP2pSource::mainnet(config.clone()).expect("source");
        let grace = source.descriptor.expected_lag.max(config.poll_interval);
        let silent = P2pError::Timeout {
            component: "headers",
        };
        let started = Instant::now();
        let mut attempts = 0;
        let mut silent_since = None;
        let mut disconnect_reported = true;
        assert!(matches!(
            head_poll_failure(
                &config,
                &mut attempts,
                &mut silent_since,
                started,
                grace,
                &silent,
                true
            ),
            HeadPollFailure::Retry(_)
        ));
        // The lane emits the blocks of a catch-up.
        note_live_progress(&mut disconnect_reported, &mut silent_since);
        assert!(!disconnect_reported);
        // One report per outage, however often the lane reconnects meanwhile.
        assert!(report_disconnect(&mut disconnect_reported, "down".to_owned()).is_some());
        assert!(report_disconnect(&mut disconnect_reported, "still down".to_owned()).is_none());
        assert!(
            matches!(
                head_poll_failure(
                    &config,
                    &mut attempts,
                    &mut silent_since,
                    started + grace + Duration::from_secs(1),
                    grace,
                    &silent,
                    true
                ),
                HeadPollFailure::Retry(_)
            ),
            "the first silent poll after progress reported the lane disconnected"
        );
    }

    #[test]
    fn head_poll_tolerance_never_exceeds_the_first_retry_pause() {
        let peer = |marker: u8| B512::from([marker; 64]);
        let block = BlockNumber(25_000_001);
        let slot = Duration::from_secs(12);
        let previous = Instant::now();
        // With a first retry pause of 100 ms, polls for one block can be
        // 150 ms apart: a peer asked by the poll before the serving one may
        // have been asked before the block existed.
        let mut rotation = HeadPollRotation::new(Duration::from_millis(100));
        assert!(rotation.begin_poll(block).is_empty());
        rotation.record_not_yet(
            peer(1),
            previous + slot.saturating_sub(Duration::from_millis(150)),
        );
        rotation.record_not_yet(
            peer(2),
            previous + slot.saturating_sub(Duration::from_millis(50)),
        );
        rotation.record_served(peer(3), previous + slot);
        assert_eq!(
            rotation.begin_poll(BlockNumber(block.0 + 1)),
            vec![peer(2)],
            "a peer asked by an earlier poll was cooled"
        );
    }

    #[test]
    fn reorg_retries_report_the_lane_disconnected_once() {
        let timeout = P2pError::Timeout {
            component: "reorg headers",
        };
        let mut reported = false;
        assert_eq!(
            reorg_failure(&timeout, ExpectationTrust::Verified, &mut reported),
            ReorgFailure::Report
        );
        assert_eq!(
            reorg_failure(&timeout, ExpectationTrust::Verified, &mut reported),
            ReorgFailure::Retry,
            "every reorg retry reported the lane disconnected again"
        );
        assert!(reported);
        // A reorg proven deeper than the window still resets.
        assert_eq!(
            reorg_failure(
                &P2pError::ReorgTooDeep { maximum: 64 },
                ExpectationTrust::Verified,
                &mut reported
            ),
            ReorgFailure::Reset
        );
    }

    #[test]
    fn a_live_material_request_ends_at_its_deadline() {
        let source = RethP2pSource::mainnet(RethP2pConfig::default()).expect("source");
        let header_peer = B512::from([0xf1; 64]);
        let (sender, _receiver) = tokio::sync::mpsc::channel(1);
        source.network.direct_peers.insert(DirectPeer {
            peer_id: header_peer,
            eth_version: EthVersion::Eth68,
            messages: PeerRequestSender::new(header_peer, sender),
        });
        let strikes = || withheld_strike(&source.network.direct_peers, header_peer).0;
        let mut bound = LiveMaterialBound::new("bodies", 25_000_001, Some(header_peer));
        // A minute after the request started, its deadline ends even a wave
        // in progress. In the first wave, no peer has answered that it lacks
        // the body yet: the request times out, and nobody is struck.
        assert!(matches!(
            source.live_material_deadline_passed(&bound),
            P2pError::Timeout {
                component: "bodies"
            }
        ));
        assert_eq!(strikes(), 0);
        // After a wave in which no peer served the body, the header is given
        // up. A header checked against a verified hash proves its block
        // exists, so its peer is not struck.
        bound.waves = 1;
        let withheld = source.live_material_deadline_passed(&bound);
        assert!(
            matches!(
                withheld,
                P2pError::Request {
                    component: "bodies",
                    ..
                }
            ),
            "{withheld}"
        );
        assert_eq!(strikes(), 0);
        // A peer claim may have made the block up: its peer is struck.
        bound.unanchored = true;
        source.live_material_deadline_passed(&bound);
        assert_eq!(strikes(), 1);
        // The lane does not wait that out: it reports itself disconnected.
        assert_eq!(
            pending_live_material_delay(&source.config, &mut 0, &withheld, true, true),
            None
        );
    }

    /// `header` as a head the sync committee attested at `beacon_slot`.
    fn attested(header: &Header, beacon_slot: u64) -> AttestedHead {
        AttestedHead {
            beacon_slot,
            beacon_block_root: [0xa7; 32],
            block_number: BlockNumber(header.number),
            block_hash: block_hash(header.hash_slow()),
        }
    }

    /// A header served by an honest peer on `chain`: `validate_live_headers`
    /// input for a request by `tip`'s hash, falling, of `range`.
    fn falling_reply(chain: &[Header], range: BlockRange) -> Vec<Header> {
        chain
            .iter()
            .filter(|header| range.contains(BlockNumber(header.number)))
            .rev()
            .cloned()
            .collect()
    }

    fn verified(header: &Header) -> ExpectedTip {
        ExpectedTip {
            hash: block_hash(header.hash_slow()),
            trust: ExpectationTrust::Verified,
        }
    }

    #[test]
    fn blocks_above_the_attested_head_are_never_fetched_as_included() {
        let chain = header_chain(B256::ZERO, 100..=102, 0);
        let last = block_ref(&chain[1]);
        let ancestry = AttestedAncestry::default();
        // With no attested head, or none above the tip, the lane fetches
        // nothing: a peer's child of the tip, however self-consistent, is
        // never included.
        for attested_head in [
            None,
            Some(attested(&chain[0], 9)),
            Some(attested(&chain[1], 10)),
        ] {
            assert_eq!(
                live_step(last, attested_head, &ancestry, 8),
                LiveStep::Wait,
                "the lane fetched above {attested_head:?}"
            );
        }
        // Once block 102 is attested, it is polled by the attested hash.
        let head = attested(&chain[2], 11);
        let LiveStep::Poll { block, tip } = live_step(last, Some(head), &ancestry, 8) else {
            panic!("the attested head is not polled");
        };
        assert_eq!(block, BlockNumber(102));
        assert_eq!(
            tip,
            ExpectedTip {
                hash: head.block_hash,
                trust: ExpectationTrust::Verified
            }
        );
        // A fabricated child served for that hash breaks the protocol: its
        // peer is invalid, and banned even at the head.
        let range = BlockRange::single(block);
        let fabricated = header_chain(chain[1].hash_slow(), 102..=102, 7);
        let error = validate_live_headers(range, Some(tip), fabricated)
            .expect_err("another header for the attested hash");
        assert_eq!(
            classify_response_failure(&error, tip.trust),
            ResponseFault::Invalid
        );
        assert_eq!(
            live_header_reply_cost(&error, tip.trust, true),
            HeaderReplyCost::Ban
        );
        assert_eq!(
            validate_live_headers(range, Some(tip), chain[2..].to_vec()).expect("attested header"),
            chain[2..]
        );
        // Waiting a missed slot or two keeps readiness; four slots without an
        // attested head report the lane disconnected.
        assert_eq!(
            head_unavailable(true, Duration::from_secs(36), ATTESTED_HEAD_GRACE),
            HeadUnavailable::Retry
        );
        assert_eq!(
            head_unavailable(true, ATTESTED_HEAD_GRACE, ATTESTED_HEAD_GRACE),
            HeadUnavailable::Report
        );
    }

    #[test]
    fn verified_tips_are_requested_by_hash_so_honest_peers_on_another_branch_are_not_banned() {
        let chain = header_chain(B256::ZERO, 100..=102, 0);
        // A sibling of the attested block 102, on a branch an honest peer
        // follows because the attested head, which is not final, was reorged.
        let sibling = header_chain(chain[1].hash_slow(), 102..=102, 9);
        let tip = Some(verified(&chain[2]));
        // The attested block is requested by its hash, descending, never by
        // its number: the peer's own block at that number is never compared.
        for range in [
            BlockRange::single(BlockNumber(102)),
            BlockRange::new(BlockNumber(100), BlockNumber(102)).expect("range"),
        ] {
            let request = live_header_request(range, tip);
            assert_eq!(
                request.start,
                BlockHashOrNumber::Hash(chain[2].hash_slow()),
                "{range:?} was requested by number"
            );
            assert_eq!(request.direction, HeadersDirection::Falling);
            assert_eq!(request.limit, range.len());
        }
        // The honest peer lacks the attested block and answers with none: a
        // disagreement, never a ban.
        let range = BlockRange::single(BlockNumber(102));
        let empty = validate_live_headers(range, tip, Vec::new()).expect_err("no header");
        assert_eq!(
            classify_response_failure(&empty, ExpectationTrust::Verified),
            ResponseFault::Disagreement
        );
        assert_eq!(
            live_header_reply_cost(&empty, ExpectationTrust::Verified, true),
            HeaderReplyCost::NotYet
        );
        assert_eq!(
            live_header_reply_cost(&empty, ExpectationTrust::Verified, false),
            HeaderReplyCost::Cooldown
        );
        // Serving its sibling for the requested hash breaks the protocol.
        let other = validate_live_headers(range, tip, sibling.clone()).expect_err("sibling");
        assert_eq!(
            live_header_reply_cost(&other, ExpectationTrust::Verified, false),
            HeaderReplyCost::Ban
        );
        // Unverified tips stay by number, and a mismatch is a disagreement.
        let claimed = Some(ExpectedTip {
            hash: block_hash(chain[2].hash_slow()),
            trust: ExpectationTrust::Unverified,
        });
        assert_eq!(
            live_header_request(range, claimed).start,
            BlockHashOrNumber::Number(102)
        );
        let mismatch = validate_live_headers(range, claimed, sibling).expect_err("mismatch");
        assert_eq!(
            classify_response_failure(&mismatch, ExpectationTrust::Unverified),
            ResponseFault::Disagreement
        );
        // A ranged reply by hash comes back lowest first; a short one is
        // incomplete, and one with more headers than requested is invalid.
        let whole = BlockRange::new(BlockNumber(100), BlockNumber(102)).expect("range");
        assert_eq!(
            validate_live_headers(whole, tip, falling_reply(&chain, whole)).expect("chain"),
            chain
        );
        let short = validate_live_headers(whole, tip, falling_reply(&chain, range))
            .expect_err("short reply");
        assert_eq!(
            classify_response_failure(&short, ExpectationTrust::Verified),
            ResponseFault::Disagreement
        );
        let mut long = falling_reply(&chain, whole);
        long.push(header_chain(B256::ZERO, 99..=99, 0).remove(0));
        let long = validate_live_headers(range, tip, long).expect_err("long reply");
        assert_eq!(
            classify_response_failure(&long, ExpectationTrust::Verified),
            ResponseFault::Invalid
        );
    }

    fn header_serve(peer: u8, block: u64) -> HeaderServe {
        HeaderServe {
            peer_id: B512::from([peer; 64]),
            block,
            elapsed: Duration::from_millis(5),
            anchored: true,
        }
    }

    #[test]
    fn catch_up_batches_come_from_headers_the_attested_head_proved() {
        let chain = header_chain(B256::ZERO, 100..=140, 0);
        let last = block_ref(&chain[0]);
        let head = attested(&chain[40], 20);
        // Without proven headers, the lane first proves the chain down from
        // the attested head.
        let mut ancestry = AttestedAncestry::default();
        assert_eq!(
            live_step(last, Some(head), &ancestry, 8),
            LiveStep::Anchor {
                from: BlockNumber(101),
                head
            }
        );
        let serve = header_serve(0x51, 140);
        ancestry = AttestedAncestry::proven(head, serve, chain[1..].to_vec());
        assert_eq!(ancestry.head, Some(head));
        // Batches then come from those headers, which are fetched once. The
        // serve that proved them is settled with the first frames.
        assert_eq!(
            live_step(last, Some(head), &ancestry, 8),
            LiveStep::CatchUp {
                range: BlockRange::new(BlockNumber(101), BlockNumber(108)).expect("range")
            }
        );
        let (headers, peer, settle) = ancestry.take(8);
        assert_eq!(headers, chain[1..=8]);
        assert_eq!(peer, Some(serve.peer_id));
        assert!(settle.is_some_and(|settled| settled.peer_id == serve.peer_id));
        let last = block_ref(&chain[8]);
        let (_, peer, settle) = ancestry.take(1);
        assert_eq!(peer, Some(serve.peer_id));
        assert!(settle.is_none(), "one serve was settled twice");
        // After a failure, one block at a time.
        let last = BlockRef {
            number: BlockNumber(109),
            ..last
        };
        assert_eq!(
            live_step(last, Some(head), &ancestry, 1),
            LiveStep::CatchUp {
                range: BlockRange::single(BlockNumber(110))
            }
        );
        // Never above the newest attested head, even with headers an older
        // one proved.
        let lower = attested(&chain[15], 21);
        assert_eq!(
            live_step(last, Some(lower), &ancestry, 8),
            LiveStep::CatchUp {
                range: BlockRange::new(BlockNumber(110), BlockNumber(115)).expect("range")
            }
        );
        // Invalidated, they are proven again from the newest attested head.
        ancestry.invalidate();
        assert_eq!(ancestry.head, None);
        assert_eq!(
            live_step(last, Some(head), &ancestry, 8),
            LiveStep::Anchor {
                from: BlockNumber(110),
                head
            }
        );
    }

    #[tokio::test]
    async fn attested_ancestry_windows_walk_down_by_verified_hashes() {
        let chain = header_chain(B256::ZERO, 1..=3_000, 0);
        let prove = |from: u64, head: u64| {
            let chain = chain.clone();
            async move {
                let requests = std::sync::Mutex::new(Vec::new());
                let head = attested(&chain[usize::try_from(head - 1).expect("index")], 30);
                let proven = prove_attested_ancestry(BlockNumber(from), head, |range, top| {
                    requests
                        .lock()
                        .expect("requests")
                        .push((range.start().0, range.end().0));
                    let window = chain
                        .iter()
                        .filter(|header| range.contains(BlockNumber(header.number)))
                        .cloned()
                        .collect::<Vec<_>>();
                    // Each window is requested by the hash of its top block.
                    let proven = window
                        .last()
                        .is_some_and(|tip| block_hash(tip.hash_slow()) == top);
                    async move {
                        if proven {
                            Ok((header_serve(0x61, range.end().0), window))
                        } else {
                            Err(P2pError::InvalidResponse("unproven window".to_owned()))
                        }
                    }
                })
                .await;
                (proven, requests.into_inner().expect("requests"))
            }
        };
        // One block, and exactly one full window.
        let (proven, requests) = prove(2_000, 2_000).await;
        assert_eq!(requests, [(2_000, 2_000)]);
        assert_eq!(proven.expect("proven").first(), Some(2_000));
        let (proven, requests) = prove(1_000, 2_023).await;
        assert_eq!(requests, [(1_000, 2_023)]);
        assert_eq!(proven.expect("proven").headers.len(), 1_024);
        // One more: the top window holds the rest, so the kept lowest one is
        // full.
        let (proven, requests) = prove(1_000, 2_024).await;
        assert_eq!(requests, [(2_024, 2_024), (1_000, 2_023)]);
        let proven = proven.expect("proven");
        assert_eq!((proven.first(), proven.headers.len()), (Some(1_000), 1_024));
        assert_eq!(
            proven.head.map(|head| head.block_number),
            Some(BlockNumber(2_024))
        );
        let (proven, requests) = prove(500, 2_548).await;
        assert_eq!(requests, [(2_548, 2_548), (1_524, 2_547), (500, 1_523)]);
        assert_eq!(proven.expect("proven").first(), Some(500));
        // A head below the lane's next block proves nothing.
        let (proven, requests) = prove(2_001, 2_000).await;
        assert!(matches!(proven, Err(P2pError::InvalidConfig(_))));
        assert!(requests.is_empty());
    }

    #[test]
    fn a_reorg_of_the_attested_head_during_catch_up_reanchors_without_banning_honest_peers() {
        // The lane catches up on branch A, proven by attested head A140.
        let branch_a = header_chain(B256::ZERO, 100..=140, 0);
        let at_a = |number: u64| &branch_a[usize::try_from(number - 100).expect("index")];
        let head_a = attested(at_a(140), 40);
        let mut ancestry =
            AttestedAncestry::proven(head_a, header_serve(0x71, 140), branch_a[1..].to_vec());
        let mut recent = branch_a[..=8]
            .iter()
            .map(block_ref)
            .collect::<VecDeque<_>>();
        let _ = ancestry.take(8);
        let last = block_ref(at_a(108));
        // Meanwhile the network reorged below block 105: the newest attested
        // head is B150, and honest peers follow branch B.
        let branch_b = header_chain(at_a(104).hash_slow(), 105..=150, 1);
        let at_b = |number: u64| &branch_b[usize::try_from(number - 105).expect("index")];
        let head_b = attested(at_b(150), 41);
        // The next batch still comes from branch A's proven headers. An
        // honest peer asked for them by hash lacks the reorged blocks and
        // answers with none: a disagreement, never a ban.
        let LiveStep::CatchUp { range } = live_step(last, Some(head_b), &ancestry, 8) else {
            panic!("no catch-up from the proven headers");
        };
        assert_eq!(
            range,
            BlockRange::new(BlockNumber(109), BlockNumber(116)).expect("range")
        );
        let stale = Some(verified(at_a(116)));
        let empty = validate_live_headers(range, stale, Vec::new()).expect_err("reorged away");
        assert_ne!(
            live_header_reply_cost(&empty, ExpectationTrust::Verified, false),
            HeaderReplyCost::Ban,
            "an honest peer was banned for a reorged attested block"
        );
        // The failed batch invalidates the proof, and the lane proves the
        // chain of the newest attested head instead.
        ancestry.invalidate();
        let LiveStep::Anchor { from, head } = live_step(last, Some(head_b), &ancestry, 8) else {
            panic!("the lane kept following the reorged branch");
        };
        assert_eq!((from, head), (BlockNumber(109), head_b));
        // Honest peers serve that chain by hash.
        let window = BlockRange::new(from, head_b.block_number).expect("range");
        let proven = validate_live_headers(
            window,
            Some(verified(at_b(150))),
            falling_reply(&branch_b, window),
        )
        .expect("the attested chain");
        ancestry = AttestedAncestry::proven(head_b, header_serve(0x72, 150), proven);
        // Its first block does not extend the tip: verified evidence of the
        // reorg, reconstructed from the parent it names.
        let (headers, _, _) = ancestry.take(8);
        let first = header_block_ref(&headers[0]);
        assert_ne!(first.parent_hash, last.hash);
        let (number, hash) = reorg_reconstruction_tip(last, first);
        assert_eq!(
            (number, hash),
            (BlockNumber(108), block_hash(at_b(108).hash_slow()))
        );
        let descending = branch_a[..=4]
            .iter()
            .chain(&branch_b[..=3])
            .rev()
            .cloned()
            .collect::<Vec<_>>();
        validate_descending_headers(number, hash, &descending).expect("branch B down to A100");
        recent.truncate(9);
        let (ancestor, reverted) = plan_reorg(&recent, &descending).expect("the fork");
        assert_eq!(ancestor.number, BlockNumber(104));
        assert_eq!(
            reverted
                .iter()
                .map(|block| block.number.0)
                .collect::<Vec<_>>(),
            [108, 107, 106, 105]
        );
    }

    #[test]
    fn only_verified_evidence_resets_the_lane() {
        let (recent, _) = reorged_live_window();
        let last = *recent.back().expect("the lane's tip");
        // A peer fabricates a frame past the tip that does not extend it, and
        // a descending chain below the parent it names that never joins the
        // retained window.
        let fabricated = header_chain(B256::repeat_byte(0x42), 1_000..=1_101, 3);
        let mismatching = block_ref(&fabricated[101]);
        let (number, hash) = reorg_reconstruction_tip(last, mismatching);
        let descending = fabricated[..=100]
            .iter()
            .rev()
            .take(65)
            .cloned()
            .collect::<Vec<_>>();
        validate_descending_headers(number, hash, &descending)
            .expect("a self-consistent fabrication");
        let too_deep = plan_reorg(&recent, &descending).expect_err("no common block");
        assert!(matches!(too_deep, P2pError::ReorgTooDeep { .. }));
        // Unanchored, that proves nothing: the lane reports once and looks
        // for the branch again, and never resets.
        let mut reported = false;
        assert_eq!(
            reorg_failure(&too_deep, ExpectationTrust::Unverified, &mut reported),
            ReorgFailure::Report,
            "unverified peer material reset the lane"
        );
        assert_eq!(
            reorg_failure(&too_deep, ExpectationTrust::Unverified, &mut reported),
            ReorgFailure::Retry
        );
        // Anchored to an attested head, the same proof resets.
        assert_eq!(
            reorg_failure(&too_deep, ExpectationTrust::Verified, &mut reported),
            ReorgFailure::Reset
        );
    }

    #[test]
    fn only_anchored_headers_clear_withheld_strikes_or_earn_rewards() {
        let (pool, peers, _receivers) = header_peer_pool(&[0xe7]);
        let peer_id = peers[0];
        pool.strike_withheld_header(peer_id);
        let serve = |anchored| HeaderServe {
            peer_id,
            block: 101,
            elapsed: Duration::from_millis(5),
            anchored,
        };
        // A bloom-negative filtered-log frame completes without a body or
        // receipts. Its header, checked against no verified hash, may be
        // fabricated: it clears nothing and earns nothing.
        assert!(
            !settle_header_serve(&pool, serve(false)),
            "an unanchored header earned its reward"
        );
        assert_eq!(
            withheld_strike(&pool, peer_id).0,
            1,
            "an unanchored header cleared the strike"
        );
        // An anchored one does both.
        assert!(settle_header_serve(&pool, serve(true)));
        assert_eq!(withheld_strike(&pool, peer_id).0, 0);
    }
}
