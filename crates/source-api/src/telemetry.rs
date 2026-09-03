use std::{
    collections::{BTreeMap, VecDeque},
    fmt,
    sync::{
        Arc, RwLock, Weak,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use leani_primitives::{BlockNumber, BlockRange};
use serde::Serialize;

const RETAINED_INACTIVE_SESSIONS: usize = 32;
const MAX_ERROR_LENGTH: usize = 512;

/// Purpose that first opened a persistent network manager.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NetworkLane {
    Live,
    History,
    Probe,
}

impl NetworkLane {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Live => "live",
            Self::History => "history",
            Self::Probe => "probe",
        }
    }
}

/// Current work performed by a network session.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NetworkPhase {
    Starting,
    WaitingForPeers,
    SettlingPeers,
    Ready,
    FetchingHeaders,
    FetchingBodies,
    FetchingReceipts,
    FollowingHead,
    Stopped,
}

impl NetworkPhase {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::WaitingForPeers => "waiting_for_peers",
            Self::SettlingPeers => "settling_peers",
            Self::Ready => "ready",
            Self::FetchingHeaders => "fetching_headers",
            Self::FetchingBodies => "fetching_bodies",
            Self::FetchingReceipts => "fetching_receipts",
            Self::FollowingHead => "following_head",
            Self::Stopped => "stopped",
        }
    }
}

/// Stable, identity-free classification of an execution peer disconnect.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NetworkDisconnectReason {
    ConnectionClosed,
    DisconnectRequested,
    TcpSubsystemError,
    ProtocolBreach,
    UselessPeer,
    TooManyPeers,
    AlreadyConnected,
    IncompatibleP2pProtocolVersion,
    NullNodeIdentity,
    ClientQuitting,
    UnexpectedHandshakeIdentity,
    ConnectedToSelf,
    PingTimeout,
    SubprotocolSpecific,
}

impl NetworkDisconnectReason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ConnectionClosed => "connection_closed",
            Self::DisconnectRequested => "disconnect_requested",
            Self::TcpSubsystemError => "tcp_subsystem_error",
            Self::ProtocolBreach => "protocol_breach",
            Self::UselessPeer => "useless_peer",
            Self::TooManyPeers => "too_many_peers",
            Self::AlreadyConnected => "already_connected",
            Self::IncompatibleP2pProtocolVersion => "incompatible_p2p_protocol_version",
            Self::NullNodeIdentity => "null_node_identity",
            Self::ClientQuitting => "client_quitting",
            Self::UnexpectedHandshakeIdentity => "unexpected_handshake_identity",
            Self::ConnectedToSelf => "connected_to_self",
            Self::PingTimeout => "ping_timeout",
            Self::SubprotocolSpecific => "subprotocol_specific",
        }
    }
}

/// Lifecycle state of the required live/finality lane supervisor.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NetworkSupervisorState {
    Idle,
    Running,
    BackingOff,
    Stopped,
}

impl NetworkSupervisorState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Running => "running",
            Self::BackingOff => "backing_off",
            Self::Stopped => "stopped",
        }
    }
}

#[derive(Clone, Debug)]
struct SessionRecord {
    lane: NetworkLane,
    phase: NetworkPhase,
    active: bool,
    connected_peers: usize,
    known_peers: usize,
    attempts: u64,
    observed_head: Option<BlockNumber>,
    range: Option<BlockRange>,
    last_error: Option<String>,
    started: Instant,
    updated: Instant,
}

#[derive(Clone, Debug)]
struct SupervisorRecord {
    state: NetworkSupervisorState,
    failures: u64,
    last_error: Option<String>,
    retry_at: Option<Instant>,
    updated: Instant,
}

#[derive(Clone, Copy, Debug, Default)]
struct PeerOriginRecord {
    candidates_admitted: u64,
    sessions_established: u64,
    body_serving: u64,
    headers_only: u64,
    lagging: u64,
    rejected: u64,
    timed_out: u64,
    qualification_elapsed_milliseconds: u64,
}

#[derive(Debug)]
struct NetworkTelemetryInner {
    next_id: AtomicU64,
    requests_started: AtomicU64,
    requests_succeeded: AtomicU64,
    requests_timed_out: AtomicU64,
    requests_failed: AtomicU64,
    request_queue_wait_milliseconds: AtomicU64,
    peer_sessions_established: AtomicU64,
    peer_sessions_closed: AtomicU64,
    body_serving_peer_slots: AtomicU64,
    minimum_peer_slots: AtomicU64,
    body_serving_peer_target: AtomicU64,
    preferred_peer_slots: AtomicU64,
    max_outbound_peer_slots: AtomicU64,
    max_concurrent_dials: AtomicU64,
    disconnect_reasons: RwLock<BTreeMap<NetworkDisconnectReason, u64>>,
    peer_origins: RwLock<BTreeMap<NetworkPeerOrigin, PeerOriginRecord>>,
    sessions: RwLock<BTreeMap<u64, SessionRecord>>,
    inactive_order: RwLock<VecDeque<u64>>,
    supervisor: RwLock<SupervisorRecord>,
}

/// Shared operational view of active and recently closed network sessions.
#[derive(Clone, Debug)]
pub struct NetworkTelemetry {
    inner: Arc<NetworkTelemetryInner>,
}

impl Default for NetworkTelemetry {
    fn default() -> Self {
        Self {
            inner: Arc::new(NetworkTelemetryInner {
                next_id: AtomicU64::new(1),
                requests_started: AtomicU64::new(0),
                requests_succeeded: AtomicU64::new(0),
                requests_timed_out: AtomicU64::new(0),
                requests_failed: AtomicU64::new(0),
                request_queue_wait_milliseconds: AtomicU64::new(0),
                peer_sessions_established: AtomicU64::new(0),
                peer_sessions_closed: AtomicU64::new(0),
                body_serving_peer_slots: AtomicU64::new(0),
                minimum_peer_slots: AtomicU64::new(0),
                body_serving_peer_target: AtomicU64::new(0),
                preferred_peer_slots: AtomicU64::new(0),
                max_outbound_peer_slots: AtomicU64::new(0),
                max_concurrent_dials: AtomicU64::new(0),
                disconnect_reasons: RwLock::new(BTreeMap::new()),
                peer_origins: RwLock::new(BTreeMap::new()),
                sessions: RwLock::new(BTreeMap::new()),
                inactive_order: RwLock::new(VecDeque::new()),
                supervisor: RwLock::new(SupervisorRecord {
                    state: NetworkSupervisorState::Idle,
                    failures: 0,
                    last_error: None,
                    retry_at: None,
                    updated: Instant::now(),
                }),
            }),
        }
    }
}

impl NetworkTelemetry {
    /// Publish execution peer availability and pool targets.
    pub fn set_peer_targets(
        &self,
        minimum: usize,
        body_serving: usize,
        preferred: usize,
        max_outbound: usize,
        max_concurrent_dials: usize,
    ) {
        self.inner.minimum_peer_slots.store(
            u64::try_from(minimum).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.inner.body_serving_peer_target.store(
            u64::try_from(body_serving).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.inner.preferred_peer_slots.store(
            u64::try_from(preferred).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.inner.max_outbound_peer_slots.store(
            u64::try_from(max_outbound).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.inner.max_concurrent_dials.store(
            u64::try_from(max_concurrent_dials).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }

    /// Publish the number of peers proven to serve commitment-valid bodies at
    /// the current verified execution target.
    pub fn set_body_serving_peers(&self, peers: usize) {
        self.inner
            .body_serving_peer_slots
            .store(u64::try_from(peers).unwrap_or(u64::MAX), Ordering::Relaxed);
    }

    /// Register a persistent network manager.
    #[must_use]
    pub fn register(&self, lane: NetworkLane) -> NetworkSessionTelemetry {
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let now = Instant::now();
        write_lock(&self.inner.sessions).insert(
            id,
            SessionRecord {
                lane,
                phase: NetworkPhase::Starting,
                active: true,
                connected_peers: 0,
                known_peers: 0,
                attempts: 0,
                observed_head: None,
                range: None,
                last_error: None,
                started: now,
                updated: now,
            },
        );
        NetworkSessionTelemetry {
            registration: Arc::new(NetworkSessionRegistration {
                id,
                telemetry: Arc::downgrade(&self.inner),
            }),
        }
    }

    /// Record that the required network lane is actively running.
    pub fn supervisor_running(&self) {
        let mut supervisor = write_lock(&self.inner.supervisor);
        supervisor.state = NetworkSupervisorState::Running;
        supervisor.last_error = None;
        supervisor.retry_at = None;
        supervisor.updated = Instant::now();
    }

    /// Record a fail-closed network-lane exit and its bounded retry delay.
    pub fn supervisor_backoff(&self, error: impl fmt::Display, retry: Duration) {
        let mut error = error.to_string();
        if error.len() > MAX_ERROR_LENGTH {
            error.truncate(MAX_ERROR_LENGTH);
        }
        let now = Instant::now();
        let mut supervisor = write_lock(&self.inner.supervisor);
        supervisor.state = NetworkSupervisorState::BackingOff;
        supervisor.failures = supervisor.failures.saturating_add(1);
        supervisor.last_error = Some(error);
        supervisor.retry_at = now.checked_add(retry);
        supervisor.updated = now;
    }

    /// Record that the network supervisor has stopped.
    pub fn supervisor_stopped(&self) {
        let mut supervisor = write_lock(&self.inner.supervisor);
        supervisor.state = NetworkSupervisorState::Stopped;
        supervisor.retry_at = None;
        supervisor.updated = Instant::now();
    }

    /// Record one request after it has obtained local scheduler capacity.
    pub fn request_started(&self, queue_wait: Duration) {
        self.inner.requests_started.fetch_add(1, Ordering::Relaxed);
        self.inner.request_queue_wait_milliseconds.fetch_add(
            u64::try_from(queue_wait.as_millis()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }

    pub fn request_succeeded(&self) {
        self.inner
            .requests_succeeded
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn request_timed_out(&self) {
        self.inner
            .requests_timed_out
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn request_failed(&self) {
        self.inner.requests_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an active execution peer session without retaining its identity.
    pub fn peer_session_established(&self) {
        self.inner
            .peer_sessions_established
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record one unique peer candidate admitted to the network manager.
    pub fn peer_candidate_admitted(&self, origin: NetworkPeerOrigin) {
        let mut origins = write_lock(&self.inner.peer_origins);
        let record = origins.entry(origin).or_default();
        record.candidates_admitted = record.candidates_admitted.saturating_add(1);
    }

    /// Attribute an active peer session to its first known candidate origin.
    pub fn peer_session_established_from(&self, origin: NetworkPeerOrigin) {
        self.peer_session_established();
        let mut origins = write_lock(&self.inner.peer_origins);
        let record = origins.entry(origin).or_default();
        record.sessions_established = record.sessions_established.saturating_add(1);
    }

    /// Record the material-serving qualification reached by one peer.
    pub fn peer_qualified(
        &self,
        origin: NetworkPeerOrigin,
        outcome: NetworkPeerQualification,
        elapsed: Option<Duration>,
    ) {
        let mut origins = write_lock(&self.inner.peer_origins);
        let record = origins.entry(origin).or_default();
        match outcome {
            NetworkPeerQualification::BodyServing => {
                record.body_serving = record.body_serving.saturating_add(1);
            }
            NetworkPeerQualification::HeadersOnly => {
                record.headers_only = record.headers_only.saturating_add(1);
            }
            NetworkPeerQualification::Lagging => {
                record.lagging = record.lagging.saturating_add(1);
            }
            NetworkPeerQualification::Rejected => {
                record.rejected = record.rejected.saturating_add(1);
            }
            NetworkPeerQualification::TimedOut => {
                record.timed_out = record.timed_out.saturating_add(1);
            }
        }
        if let Some(elapsed) = elapsed {
            record.qualification_elapsed_milliseconds = record
                .qualification_elapsed_milliseconds
                .saturating_add(u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX));
        }
    }

    /// Record a closed execution peer session by its protocol-level reason.
    pub fn peer_session_closed(&self, reason: NetworkDisconnectReason) {
        self.inner
            .peer_sessions_closed
            .fetch_add(1, Ordering::Relaxed);
        let mut reasons = write_lock(&self.inner.disconnect_reasons);
        let count = reasons.entry(reason).or_default();
        *count = count.saturating_add(1);
    }

    /// Return a serialization-safe snapshot without exposing peer identities.
    #[must_use]
    pub fn snapshot(&self) -> NetworkTelemetrySnapshot {
        let sessions = read_lock(&self.inner.sessions);
        let mut active_sessions = 0_usize;
        let mut connected_peer_slots = 0_usize;
        let mut known_peer_records = 0_usize;
        let sessions = sessions
            .iter()
            .rev()
            .map(|(id, record)| {
                if record.active {
                    active_sessions = active_sessions.saturating_add(1);
                    connected_peer_slots =
                        connected_peer_slots.saturating_add(record.connected_peers);
                    known_peer_records = known_peer_records.saturating_add(record.known_peers);
                }
                NetworkSessionSnapshot {
                    id: *id,
                    lane: record.lane,
                    phase: record.phase,
                    active: record.active,
                    connected_peers: record.connected_peers,
                    known_peers: record.known_peers,
                    attempts: record.attempts,
                    observed_head_block: record.observed_head.map(|block| block.0),
                    from_block: record.range.map(|range| range.start().0),
                    to_block: record.range.map(|range| range.end().0),
                    last_error: record.last_error.clone(),
                    age_seconds: record.started.elapsed().as_secs(),
                    update_age_seconds: record.updated.elapsed().as_secs(),
                }
            })
            .collect();
        let supervisor = read_lock(&self.inner.supervisor);
        let disconnect_reasons = read_lock(&self.inner.disconnect_reasons)
            .iter()
            .map(|(reason, count)| NetworkDisconnectSnapshot {
                reason: *reason,
                count: *count,
            })
            .collect();
        let peer_origins = read_lock(&self.inner.peer_origins)
            .iter()
            .map(|(origin, record)| NetworkPeerOriginSnapshot {
                origin: *origin,
                candidates_admitted: record.candidates_admitted,
                sessions_established: record.sessions_established,
                qualifications: NetworkPeerQualificationSnapshot {
                    body_serving: record.body_serving,
                    headers_only: record.headers_only,
                    lagging: record.lagging,
                    rejected: record.rejected,
                    timed_out: record.timed_out,
                    elapsed_milliseconds: record.qualification_elapsed_milliseconds,
                },
            })
            .collect();
        let retry_in_seconds = supervisor.retry_at.map(|retry_at| {
            let remaining = retry_at.saturating_duration_since(Instant::now());
            remaining
                .as_secs()
                .saturating_add(u64::from(remaining.subsec_nanos() > 0))
        });
        NetworkTelemetrySnapshot {
            active_sessions,
            connected_peer_slots,
            known_peer_records,
            body_serving_peer_slots: self.inner.body_serving_peer_slots.load(Ordering::Relaxed),
            peer_targets: NetworkPeerTargetsSnapshot {
                minimum: self.inner.minimum_peer_slots.load(Ordering::Relaxed),
                body_serving: self.inner.body_serving_peer_target.load(Ordering::Relaxed),
                preferred: self.inner.preferred_peer_slots.load(Ordering::Relaxed),
                max_outbound: self.inner.max_outbound_peer_slots.load(Ordering::Relaxed),
                max_concurrent_dials: self.inner.max_concurrent_dials.load(Ordering::Relaxed),
            },
            requests: NetworkRequestSnapshot {
                started: self.inner.requests_started.load(Ordering::Relaxed),
                succeeded: self.inner.requests_succeeded.load(Ordering::Relaxed),
                timed_out: self.inner.requests_timed_out.load(Ordering::Relaxed),
                failed: self.inner.requests_failed.load(Ordering::Relaxed),
                queue_wait_milliseconds: self
                    .inner
                    .request_queue_wait_milliseconds
                    .load(Ordering::Relaxed),
            },
            peer_lifecycle: NetworkPeerLifecycleSnapshot {
                established: self.inner.peer_sessions_established.load(Ordering::Relaxed),
                disconnected: self.inner.peer_sessions_closed.load(Ordering::Relaxed),
                disconnect_reasons,
            },
            peer_origins,
            supervisor: NetworkSupervisorSnapshot {
                state: supervisor.state,
                failures: supervisor.failures,
                last_error: supervisor.last_error.clone(),
                retry_in_seconds,
                update_age_seconds: supervisor.updated.elapsed().as_secs(),
            },
            sessions,
        }
    }
}

/// Mutable handle for one registered network session.
#[derive(Clone)]
pub struct NetworkSessionTelemetry {
    registration: Arc<NetworkSessionRegistration>,
}

impl fmt::Debug for NetworkSessionTelemetry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NetworkSessionTelemetry")
            .field("id", &self.registration.id)
            .finish_non_exhaustive()
    }
}

impl NetworkSessionTelemetry {
    #[must_use]
    pub fn id(&self) -> u64 {
        self.registration.id
    }

    pub fn set_phase(&self, phase: NetworkPhase) {
        self.update(|record| record.phase = phase);
    }

    pub fn set_peers(&self, connected: usize, known: usize) {
        self.update(|record| {
            record.connected_peers = connected;
            record.known_peers = known;
        });
    }

    pub fn set_range(&self, range: Option<BlockRange>) {
        self.update(|record| record.range = range);
    }

    /// Advance the highest execution head observed from the connected peer set.
    ///
    /// This is intentionally independent from [`Self::set_range`]: a request
    /// may probe the block after the observed head without making that future
    /// block a valid synchronization target.
    pub fn observe_head(&self, head: BlockNumber) {
        self.update(|record| {
            record.observed_head = Some(
                record
                    .observed_head
                    .map_or(head, |current| current.max(head)),
            );
        });
    }

    pub fn record_attempt(&self) {
        self.update(|record| record.attempts = record.attempts.saturating_add(1));
    }

    pub fn record_error(&self, error: impl fmt::Display) {
        let mut error = error.to_string();
        if error.len() > MAX_ERROR_LENGTH {
            error.truncate(MAX_ERROR_LENGTH);
        }
        self.update(|record| record.last_error = Some(error));
    }

    pub fn clear_error(&self) {
        self.update(|record| record.last_error = None);
    }

    fn update(&self, update: impl FnOnce(&mut SessionRecord)) {
        let Some(telemetry) = self.registration.telemetry.upgrade() else {
            return;
        };
        let mut sessions = write_lock(&telemetry.sessions);
        if let Some(record) = sessions.get_mut(&self.registration.id) {
            update(record);
            record.updated = Instant::now();
        }
    }
}

#[derive(Debug)]
struct NetworkSessionRegistration {
    id: u64,
    telemetry: Weak<NetworkTelemetryInner>,
}

impl Drop for NetworkSessionRegistration {
    fn drop(&mut self) {
        let Some(telemetry) = self.telemetry.upgrade() else {
            return;
        };
        if let Some(record) = write_lock(&telemetry.sessions).get_mut(&self.id) {
            record.active = false;
            record.phase = NetworkPhase::Stopped;
            record.connected_peers = 0;
            record.updated = Instant::now();
        }
        let expired = {
            let mut order = write_lock(&telemetry.inactive_order);
            order.push_back(self.id);
            (order.len() > RETAINED_INACTIVE_SESSIONS)
                .then(|| order.pop_front())
                .flatten()
        };
        if let Some(expired) = expired {
            write_lock(&telemetry.sessions).remove(&expired);
        }
    }
}

/// Point-in-time operational network status.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkTelemetrySnapshot {
    pub active_sessions: usize,
    /// Connected peer slots across active physical network managers.
    pub connected_peer_slots: usize,
    /// Known-peer records across active physical network managers.
    pub known_peer_records: usize,
    /// Peers proven to serve commitment-valid bodies at the current target.
    pub body_serving_peer_slots: u64,
    pub peer_targets: NetworkPeerTargetsSnapshot,
    pub requests: NetworkRequestSnapshot,
    pub peer_lifecycle: NetworkPeerLifecycleSnapshot,
    /// Candidate, session, and capability outcomes grouped by startup origin.
    pub peer_origins: Vec<NetworkPeerOriginSnapshot>,
    pub supervisor: NetworkSupervisorSnapshot,
    pub sessions: Vec<NetworkSessionSnapshot>,
}

/// How a peer candidate first entered this process's network manager.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NetworkPeerOrigin {
    CachedHot,
    CachedBroad,
    DnsTree,
    Trusted,
    /// Reth's built-in Discv4 or Discv5 service discovered the peer without it
    /// first passing through Leani's explicit candidate admission path.
    Discv4Or5,
}

impl NetworkPeerOrigin {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CachedHot => "cached_hot",
            Self::CachedBroad => "cached_broad",
            Self::DnsTree => "dns_tree",
            Self::Trusted => "trusted",
            Self::Discv4Or5 => "discv4_or_5",
        }
    }
}

/// Result of testing a connected execution peer against verified material.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NetworkPeerQualification {
    BodyServing,
    HeadersOnly,
    Lagging,
    Rejected,
    TimedOut,
}

impl NetworkPeerQualification {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BodyServing => "body_serving",
            Self::HeadersOnly => "headers_only",
            Self::Lagging => "lagging",
            Self::Rejected => "rejected",
            Self::TimedOut => "timed_out",
        }
    }
}

/// Identity-free startup and qualification counters for one peer origin.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkPeerOriginSnapshot {
    pub origin: NetworkPeerOrigin,
    pub candidates_admitted: u64,
    pub sessions_established: u64,
    pub qualifications: NetworkPeerQualificationSnapshot,
}

/// Cumulative qualification outcomes and their total admission-to-result time.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkPeerQualificationSnapshot {
    pub body_serving: u64,
    pub headers_only: u64,
    pub lagging: u64,
    pub rejected: u64,
    pub timed_out: u64,
    pub elapsed_milliseconds: u64,
}

/// Configured execution peer-pool thresholds.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkPeerTargetsSnapshot {
    /// Hard connected-peer floor required before requests may start.
    pub minimum: u64,
    /// Desired number of independently verified body-serving peers.
    pub body_serving: u64,
    /// Non-blocking healthy-pool target; background dialing continues beyond it.
    pub preferred: u64,
    /// Maximum outbound connections maintained by the peer manager.
    pub max_outbound: u64,
    /// Hard ceiling on simultaneous outbound connection attempts.
    pub max_concurrent_dials: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkRequestSnapshot {
    pub started: u64,
    pub succeeded: u64,
    pub timed_out: u64,
    pub failed: u64,
    pub queue_wait_milliseconds: u64,
}

/// Cumulative physical peer-session lifecycle since process start.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkPeerLifecycleSnapshot {
    pub established: u64,
    pub disconnected: u64,
    pub disconnect_reasons: Vec<NetworkDisconnectSnapshot>,
}

/// One bounded disconnect-reason counter.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkDisconnectSnapshot {
    pub reason: NetworkDisconnectReason,
    pub count: u64,
}

/// Current lifecycle and retry state of the required network lane.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkSupervisorSnapshot {
    pub state: NetworkSupervisorState,
    pub failures: u64,
    pub last_error: Option<String>,
    pub retry_in_seconds: Option<u64>,
    pub update_age_seconds: u64,
}

/// Point-in-time status for one active or recently stopped network session.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkSessionSnapshot {
    pub id: u64,
    pub lane: NetworkLane,
    pub phase: NetworkPhase,
    pub active: bool,
    pub connected_peers: usize,
    pub known_peers: usize,
    pub attempts: u64,
    /// Highest execution block currently advertised or directly served by a peer.
    pub observed_head_block: Option<u64>,
    /// First block in the current material request, if a request is active.
    pub from_block: Option<u64>,
    /// Last block in the current material request, if a request is active.
    pub to_block: Option<u64>,
    pub last_error: Option<String>,
    pub age_seconds: u64,
    pub update_age_seconds: u64,
}

fn read_lock<T>(lock: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    lock.read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn write_lock<T>(lock: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    lock.write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use leani_primitives::{BlockNumber, BlockRange};

    use super::*;

    #[test]
    fn sessions_remain_visible_after_close_without_peer_identities() {
        let telemetry = NetworkTelemetry::default();
        telemetry.set_peer_targets(1, 4, 16, 100, 30);
        telemetry.set_body_serving_peers(3);
        telemetry.request_started(Duration::from_millis(7));
        telemetry.request_succeeded();
        telemetry.request_started(Duration::from_millis(3));
        telemetry.request_timed_out();
        telemetry.peer_session_established();
        telemetry.peer_session_established();
        telemetry.peer_candidate_admitted(NetworkPeerOrigin::CachedHot);
        telemetry.peer_session_established_from(NetworkPeerOrigin::CachedHot);
        telemetry.peer_qualified(
            NetworkPeerOrigin::CachedHot,
            NetworkPeerQualification::BodyServing,
            Some(Duration::from_millis(12)),
        );
        telemetry.peer_session_closed(NetworkDisconnectReason::TooManyPeers);
        telemetry.peer_session_closed(NetworkDisconnectReason::ConnectionClosed);
        let session = telemetry.register(NetworkLane::History);
        let id = session.id();
        session.set_phase(NetworkPhase::FetchingReceipts);
        session.set_peers(2, 120);
        session.set_range(Some(
            BlockRange::new(BlockNumber(10), BlockNumber(20)).expect("range"),
        ));
        session.observe_head(BlockNumber(19));
        session.observe_head(BlockNumber(18));
        session.record_attempt();
        session.record_error("receipt timeout");

        let active = telemetry.snapshot();
        assert_eq!(
            active.requests,
            NetworkRequestSnapshot {
                started: 2,
                succeeded: 1,
                timed_out: 1,
                failed: 0,
                queue_wait_milliseconds: 10,
            }
        );
        assert_eq!(active.active_sessions, 1);
        assert_eq!(active.connected_peer_slots, 2);
        assert_eq!(active.body_serving_peer_slots, 3);
        assert_eq!(
            active.peer_targets,
            NetworkPeerTargetsSnapshot {
                minimum: 1,
                body_serving: 4,
                preferred: 16,
                max_outbound: 100,
                max_concurrent_dials: 30,
            }
        );
        assert_eq!(active.peer_lifecycle.established, 3);
        assert_eq!(active.peer_lifecycle.disconnected, 2);
        assert_eq!(
            active.peer_lifecycle.disconnect_reasons,
            vec![
                NetworkDisconnectSnapshot {
                    reason: NetworkDisconnectReason::ConnectionClosed,
                    count: 1,
                },
                NetworkDisconnectSnapshot {
                    reason: NetworkDisconnectReason::TooManyPeers,
                    count: 1,
                },
            ]
        );
        assert_eq!(active.sessions[0].phase, NetworkPhase::FetchingReceipts);
        assert_eq!(
            active.peer_origins,
            vec![NetworkPeerOriginSnapshot {
                origin: NetworkPeerOrigin::CachedHot,
                candidates_admitted: 1,
                sessions_established: 1,
                qualifications: NetworkPeerQualificationSnapshot {
                    body_serving: 1,
                    headers_only: 0,
                    lagging: 0,
                    rejected: 0,
                    timed_out: 0,
                    elapsed_milliseconds: 12,
                },
            }]
        );
        assert_eq!(active.sessions[0].observed_head_block, Some(19));
        assert_eq!(active.sessions[0].from_block, Some(10));

        drop(session);
        let stopped = telemetry.snapshot();
        assert_eq!(stopped.active_sessions, 0);
        assert_eq!(stopped.sessions[0].id, id);
        assert_eq!(stopped.sessions[0].phase, NetworkPhase::Stopped);
        assert_eq!(
            stopped.sessions[0].last_error.as_deref(),
            Some("receipt timeout")
        );
    }

    #[test]
    fn supervisor_failure_exposes_bounded_error_and_retry_state() {
        let telemetry = NetworkTelemetry::default();
        telemetry.supervisor_running();
        assert_eq!(
            telemetry.snapshot().supervisor.state,
            NetworkSupervisorState::Running
        );

        telemetry.supervisor_backoff("handoff retained pending deltas", Duration::from_secs(30));
        let failed = telemetry.snapshot().supervisor;
        assert_eq!(failed.state, NetworkSupervisorState::BackingOff);
        assert_eq!(failed.failures, 1);
        assert_eq!(
            failed.last_error.as_deref(),
            Some("handoff retained pending deltas")
        );
        assert!(failed.retry_in_seconds.is_some_and(|seconds| seconds <= 30));

        telemetry.supervisor_stopped();
        let stopped = telemetry.snapshot().supervisor;
        assert_eq!(stopped.state, NetworkSupervisorState::Stopped);
        assert_eq!(stopped.failures, 1);
        assert!(stopped.retry_in_seconds.is_none());
    }
}
