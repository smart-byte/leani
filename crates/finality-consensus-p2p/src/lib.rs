//! Locally verified Ethereum consensus finality over native consensus P2P.
//!
//! The transport implements the standard discv5 + libp2p Noise/Yamux
//! light-client req/resp path. Peers are untrusted: checkpoint bootstraps,
//! sync-committee transitions, BLS signatures, finality branches, and
//! execution payload branches are processed by the same verifier used by the
//! Beacon API adapter.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fmt,
    hash::{BuildHasher as _, RandomState},
    io::{Read, Write},
    net::{IpAddr, Ipv4Addr},
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use async_trait::async_trait;
use discv5::{
    ConfigBuilder, Discv5, Enr, ListenConfig,
    enr::{CombinedKey, CombinedPublicKey, EnrPublicKey, NodeId},
};
use futures::{AsyncReadExt, AsyncWriteExt, StreamExt, stream};
use helios_consensus_core::{
    consensus_spec::MainnetConsensusSpec,
    types::{Bootstrap, FinalityUpdate, OptimisticUpdate, Update},
};
use leani_finality_beacon_api::{
    AnchorFile, AnchorWriter, BeaconApiError, CheckpointOrigin, Clock, DEFAULT_MAX_CHECKPOINT_AGE,
    HELIOS_REVISION, MainnetLightClientVerifier, StartAnchor, TrustedCheckpoint,
    VerifiedFinalityAnchor, current_slot, known_mainnet_fork_digests, mainnet_fork_digest,
    resolve_start_anchor, signed_implausibly_ahead, verification_slot,
};
use leani_primitives::{
    BlockNumber, Capability, CapabilitySet, ChainId, SourceId, SourceKind, TrustModel,
};
use leani_source_api::{
    AttestedHead, AttestedHeadPublisher, ConsensusCheckpoint, FinalityEvent, FinalityEventStream,
    FinalityModel, FinalitySource, Partitioning, SourceDescriptor, SourceError,
};
use libp2p::{
    Multiaddr, PeerId, StreamProtocol, SwarmBuilder, identity,
    multiaddr::Protocol,
    noise,
    swarm::{SwarmEvent, dial_opts::DialOpts},
    tcp, yamux,
};
use serde::{Deserialize, Serialize};
use snap::{read::FrameDecoder, write::FrameEncoder};
use ssz::Decode;
use thiserror::Error;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

const BOOTSTRAP_PROTOCOL: &str = "/eth2/beacon_chain/req/light_client_bootstrap/1/ssz_snappy";
const UPDATE_PROTOCOL: &str = "/eth2/beacon_chain/req/light_client_updates_by_range/1/ssz_snappy";
const FINALITY_PROTOCOL: &str = "/eth2/beacon_chain/req/light_client_finality_update/1/ssz_snappy";
const OPTIMISTIC_PROTOCOL: &str =
    "/eth2/beacon_chain/req/light_client_optimistic_update/1/ssz_snappy";
const STATUS_PROTOCOL: &str = "/eth2/beacon_chain/req/status/1/ssz_snappy";
const PING_PROTOCOL: &str = "/eth2/beacon_chain/req/ping/1/ssz_snappy";
const GOODBYE_PROTOCOL: &str = "/eth2/beacon_chain/req/goodbye/1/ssz_snappy";
const MAX_LIGHT_CLIENT_SSZ_BYTES: usize = 2 * 1_024 * 1_024;
const MAX_WIRE_BYTES: usize = 3 * 1_024 * 1_024;
const MAX_CONTROL_WIRE_BYTES: usize = 1_024;
const STATUS_BYTES: usize = 84;
const DISCOVERY_QUERIES: usize = 3;
const PEER_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(4);
const MAX_PEER_REDIALS: usize = 2;
/// Default and largest consensus peer set dialed at once.
pub const DEFAULT_MAXIMUM_PEERS: usize = 24;
/// How often an exhausted peer set is reported at `warn` level.
const EXHAUSTED_PEERS_WARNING_INTERVAL: Duration = Duration::from_mins(5);
const EMBEDDED_MAINNET_BOOTNODES: &str = include_str!("../assets/mainnet-bootnodes.txt");

/// Native consensus-network settings.
#[derive(Clone, Debug)]
pub struct ConsensusP2pConfig {
    pub bootnodes: Vec<String>,
    pub discovery_ip: IpAddr,
    pub discovery_port: u16,
    pub minimum_peers: usize,
    pub maximum_peers: usize,
    pub discovery_timeout: Duration,
    pub connection_timeout: Duration,
    pub request_timeout: Duration,
    pub poll_interval: Duration,
    pub max_checkpoint_age: Duration,
    /// Whether the newest verified anchor is read from, and persisted to, a
    /// file for restarts.
    pub anchor: AnchorFile,
}

impl Default for ConsensusP2pConfig {
    fn default() -> Self {
        Self {
            bootnodes: embedded_mainnet_bootnodes(),
            discovery_ip: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            discovery_port: 0,
            minimum_peers: 2,
            maximum_peers: DEFAULT_MAXIMUM_PEERS,
            discovery_timeout: Duration::from_secs(15),
            connection_timeout: Duration::from_secs(20),
            request_timeout: Duration::from_secs(15),
            poll_interval: Duration::from_secs(12),
            max_checkpoint_age: DEFAULT_MAX_CHECKPOINT_AGE,
            anchor: AnchorFile::Disabled,
        }
    }
}

impl ConsensusP2pConfig {
    fn validate(&self) -> Result<(), ConsensusP2pError> {
        if self.bootnodes.is_empty() {
            return Err(ConsensusP2pError::InvalidConfig(
                "at least one consensus bootnode ENR is required".to_owned(),
            ));
        }
        if self.minimum_peers == 0
            || self.maximum_peers < self.minimum_peers
            || self.maximum_peers > 128
        {
            return Err(ConsensusP2pError::InvalidConfig(
                "peer bounds must satisfy 1 <= minimum <= maximum <= 128".to_owned(),
            ));
        }
        if self.discovery_timeout.is_zero()
            || self.connection_timeout.is_zero()
            || self.request_timeout.is_zero()
            || self.poll_interval.is_zero()
            || self.max_checkpoint_age.is_zero()
        {
            return Err(ConsensusP2pError::InvalidConfig(
                "network timeouts and checkpoint age must be non-zero".to_owned(),
            ));
        }
        for bootnode in &self.bootnodes {
            bootnode.parse::<Enr>().map_err(|error| {
                ConsensusP2pError::InvalidConfig(format!("invalid bootnode ENR: {error}"))
            })?;
        }
        Ok(())
    }
}

/// Bounded live-network evidence returned by `source probe finality`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ConsensusP2pProbeReport {
    pub verifier: String,
    pub checkpoint_root: [u8; 32],
    pub checkpoint_slot: u64,
    pub accepted: bool,
    pub selected: Option<VerifiedFinalityAnchor>,
    pub checkpoint_anchor: Option<VerifiedFinalityAnchor>,
    pub updates_verified: u64,
    pub discovered_peers: usize,
    pub connected_peers: usize,
    pub attempted_peers: usize,
    pub errors: Vec<String>,
}

/// Ethereum mainnet finality directly from consensus peers.
#[derive(Clone, Debug)]
pub struct VerifiedConsensusP2p {
    config: ConsensusP2pConfig,
    descriptor: SourceDescriptor,
    connector: Arc<dyn PeerConnector>,
    clock: Clock,
    /// Peers that served invalid material: never asked or dialed again by
    /// this source, across reconnects.
    banned: Arc<Mutex<HashSet<PeerId>>>,
    anchor_writer: AnchorWriter,
    /// The checkpoint the last probe started for and the anchor it chose, so
    /// a subscription from the handed-over anchor keeps the operator's trust
    /// root.
    trust: Arc<Mutex<Option<(TrustedCheckpoint, StartAnchor)>>>,
    /// Where every finality refresh publishes the newest verified attested
    /// head.
    attested_heads: Option<AttestedHeadPublisher>,
}

impl VerifiedConsensusP2p {
    /// Construct a mainnet source. No sockets are opened until probe/subscribe.
    ///
    /// # Errors
    ///
    /// Rejects invalid peer bounds, timeouts, or bootnode ENRs.
    pub fn mainnet(config: ConsensusP2pConfig) -> Result<Self, ConsensusP2pError> {
        let clock = Clock::default();
        Self::with_connector(
            config,
            Arc::new(Libp2pConnector {
                clock: clock.clone(),
            }),
            clock,
        )
    }

    fn with_connector(
        config: ConsensusP2pConfig,
        connector: Arc<dyn PeerConnector>,
        clock: Clock,
    ) -> Result<Self, ConsensusP2pError> {
        config.validate()?;
        Ok(Self {
            connector,
            clock,
            banned: Arc::default(),
            anchor_writer: AnchorWriter::new(config.anchor.clone()),
            trust: Arc::default(),
            attested_heads: None,
            config,
            descriptor: SourceDescriptor {
                id: SourceId::new("consensus-p2p-light-client")
                    .map_err(|error| ConsensusP2pError::InvalidConfig(error.to_string()))?,
                kind: SourceKind::ConsensusP2p,
                chain_id: ChainId(1),
                range: None,
                capabilities: CapabilitySet::of(Capability::ConsensusFinality),
                complete_capabilities: CapabilitySet::of(Capability::ConsensusFinality),
                trust: TrustModel::ProtocolVerified,
                finality: FinalityModel::Finalized,
                partitioning: Partitioning::None,
                expected_lag: Duration::from_secs(24),
                schema_version: format!("consensus-p2p-light-client.v1+helios.{HELIOS_REVISION}"),
                priority: 0,
            },
        })
    }

    /// Publish the attested head of every finality refresh, each slot, to
    /// `heads`: the execution live lane includes no block above it.
    #[must_use]
    pub fn with_attested_heads(mut self, heads: AttestedHeadPublisher) -> Self {
        self.attested_heads = Some(heads);
        self
    }

    /// Discover peers, perform the required status handshake, and verify a
    /// complete checkpoint-to-finality sync.
    pub async fn probe_checkpoint(&self, checkpoint: TrustedCheckpoint) -> ConsensusP2pProbeReport {
        let checkpoint_root = checkpoint.root;
        let checkpoint_slot = checkpoint.slot.unwrap_or_default();
        match P2pLightClient::connect_and_sync(self, checkpoint, CancellationToken::new()).await {
            Ok(client) => {
                *self.trust.lock().unwrap_or_else(PoisonError::into_inner) =
                    Some((checkpoint, client.start));
                ConsensusP2pProbeReport {
                    verifier: format!("helios-consensus-core@{HELIOS_REVISION}"),
                    checkpoint_root,
                    checkpoint_slot,
                    accepted: true,
                    selected: client.verifier.finalized_anchor().ok(),
                    checkpoint_anchor: Some(client.verifier.checkpoint_anchor()),
                    updates_verified: client.verifier.updates_verified(),
                    discovered_peers: client.network.discovered_peers,
                    connected_peers: client.network.connected_count(),
                    attempted_peers: client.network.attempted_peers,
                    errors: client.network.peer_errors.clone(),
                }
            }
            Err(failure) => ConsensusP2pProbeReport {
                verifier: format!("helios-consensus-core@{HELIOS_REVISION}"),
                checkpoint_root,
                checkpoint_slot,
                accepted: false,
                selected: None,
                checkpoint_anchor: None,
                updates_verified: 0,
                discovered_peers: failure.discovered_peers,
                connected_peers: failure.connected_peers,
                attempted_peers: failure.attempted_peers,
                errors: failure.errors,
            },
        }
    }
}

#[async_trait]
impl FinalitySource for VerifiedConsensusP2p {
    fn descriptor(&self) -> &SourceDescriptor {
        &self.descriptor
    }

    async fn subscribe(
        &self,
        checkpoint: ConsensusCheckpoint,
        cancellation: CancellationToken,
    ) -> Result<FinalityEventStream, SourceError> {
        if checkpoint.beacon_block_root == [0; 32] {
            return Err(SourceError::Protocol(
                "weak-subjectivity checkpoint root must not be zero".to_owned(),
            ));
        }
        // The anchor a probe handed over belongs to the probe's trust root.
        // Without a probe, the checkpoint is the operator's trust root.
        let remembered = *self.trust.lock().unwrap_or_else(PoisonError::into_inner);
        let trusted = remembered
            .filter(|(configured, start)| {
                start.root == checkpoint.beacon_block_root
                    || configured.root == checkpoint.beacon_block_root
            })
            .map_or(
                TrustedCheckpoint {
                    root: checkpoint.beacon_block_root,
                    slot: Some(checkpoint.beacon_slot),
                    origin: CheckpointOrigin::Operator,
                },
                |(configured, _)| configured,
            );
        let client = P2pLightClient::connect_and_sync(self, trusted, cancellation.clone())
            .await
            .map_err(|failure| SourceError::Unavailable(failure.errors.join("; ")))?;
        let bootstrap = client.verifier.checkpoint_anchor();
        // The checkpoint must match its own verified bootstrap exactly. The
        // start may instead be a persisted anchor the rules accepted, or the
        // trust root a probe started from.
        let expected = bootstrap.beacon_block_root == checkpoint.beacon_block_root;
        if (expected
            && (bootstrap.beacon_slot != checkpoint.beacon_slot
                || bootstrap.execution_block_hash != checkpoint.execution_block_hash))
            || (!expected && !client.start.persisted && client.start.root != trusted.root)
        {
            return Err(SourceError::Protocol(
                "configured checkpoint differs from the P2P-verified bootstrap".to_owned(),
            ));
        }
        let pending = client
            .verifier
            .finalized_anchor()
            .map_err(|error| beacon_source_error(&error))?;
        let state = SubscriptionState {
            client,
            cancellation,
            pending: Some(pending),
            last_emitted: None,
            last_exhausted_warning: None,
            terminal: false,
        };
        Ok(stream::unfold(state, next_finality).boxed())
    }
}

#[derive(Debug)]
struct SubscriptionState {
    client: P2pLightClient,
    cancellation: CancellationToken,
    pending: Option<VerifiedFinalityAnchor>,
    last_emitted: Option<VerifiedFinalityAnchor>,
    last_exhausted_warning: Option<tokio::time::Instant>,
    terminal: bool,
}

async fn next_finality(
    mut state: SubscriptionState,
) -> Option<(Result<FinalityEvent, SourceError>, SubscriptionState)> {
    if state.terminal {
        return None;
    }
    loop {
        if state.cancellation.is_cancelled() {
            state.terminal = true;
            return Some((Err(SourceError::Cancelled), state));
        }
        if let Some(anchor) = state.pending.take()
            && state.last_emitted != Some(anchor)
        {
            state.last_emitted = Some(anchor);
            return Some((
                Ok(FinalityEvent::Finalized {
                    block_number: BlockNumber(anchor.execution_block_number),
                    block_hash: anchor.execution_block_hash,
                    beacon_slot: anchor.beacon_slot,
                    beacon_block_root: anchor.beacon_block_root,
                }),
                state,
            ));
        }
        tokio::select! {
            () = tokio::time::sleep(state.client.network.poll_interval) => {}
            () = state.cancellation.cancelled() => {
                state.terminal = true;
                return Some((Err(SourceError::Cancelled), state));
            }
        }
        match state.client.refresh().await {
            Ok(anchor) => state.pending = Some(anchor),
            Err(error) if retryable_refresh_error(&error) => {
                let now = tokio::time::Instant::now();
                if state.last_exhausted_warning.is_none_or(|warned| {
                    now.duration_since(warned) >= EXHAUSTED_PEERS_WARNING_INTERVAL
                }) {
                    state.last_exhausted_warning = Some(now);
                    warn!(
                        %error,
                        "consensus finality refresh exhausted its peer set; retrying"
                    );
                } else {
                    debug!(
                        %error,
                        "consensus finality refresh exhausted its peer set; retrying"
                    );
                }
            }
            Err(error) => {
                state.terminal = true;
                return Some((Err(error), state));
            }
        }
    }
}

fn retryable_refresh_error(error: &SourceError) -> bool {
    matches!(
        error,
        SourceError::Disconnected(_) | SourceError::Unavailable(_)
    )
}

#[derive(Debug)]
struct P2pLightClient {
    source: VerifiedConsensusP2p,
    cancellation: CancellationToken,
    network: P2pNetwork,
    verifier: MainnetLightClientVerifier,
    start: StartAnchor,
}

impl P2pLightClient {
    /// Bootstrap once, from the checkpoint or a newer persisted anchor, then
    /// verify the latest finality update. When no peer serves a bootstrap for
    /// the persisted anchor, the checkpoint itself is used.
    async fn connect_and_sync(
        source: &VerifiedConsensusP2p,
        checkpoint: TrustedCheckpoint,
        cancellation: CancellationToken,
    ) -> Result<Self, ProbeFailure> {
        let mut start = resolve_start_anchor(
            source.config.anchor.path(),
            checkpoint,
            source.config.max_checkpoint_age,
            source.clock.now(),
        );
        if start.persisted {
            info!(
                beacon_slot = start.slot,
                "bootstrapping consensus P2P finality from the persisted verified anchor"
            );
        }
        let checkpoint_slot = checkpoint.slot.unwrap_or_default();
        let start_slot = start.slot.unwrap_or(checkpoint_slot);
        let mut network = connect_network(source, start.root, start_slot, &cancellation).await?;
        let verifier = match bootstrap(source, &mut network, start.root, start_slot, &cancellation)
            .await
        {
            Ok(verifier) => verifier,
            Err(failure) if start.persisted => {
                warn!(
                    beacon_slot = start.slot,
                    "no consensus peer served a bootstrap for the persisted finality anchor; bootstrapping from the configured checkpoint"
                );
                start = StartAnchor::configured(checkpoint);
                bootstrap(
                    source,
                    &mut network,
                    start.root,
                    checkpoint_slot,
                    &cancellation,
                )
                .await
                .map_err(|mut fallback| {
                    fallback.errors.splice(0..0, failure.errors);
                    fallback
                })?
            }
            Err(failure) => return Err(failure),
        };
        let mut client = Self {
            source: source.clone(),
            cancellation,
            network,
            verifier,
            start,
        };
        if let Err(error) = client.refresh().await {
            return Err(client.network.failure(error.to_string()));
        }
        Ok(client)
    }

    /// Verify the latest finality update from any peer. Every failure here is
    /// retryable: one stale or invalid peer never ends the stream. With
    /// finality verified, the latest optimistic update's attested head is
    /// published.
    async fn refresh(&mut self) -> Result<VerifiedFinalityAnchor, SourceError> {
        let anchor = match self.refresh_once().await {
            Ok(anchor) => anchor,
            Err(error) => {
                self.reconnect(error).await?;
                self.refresh_once()
                    .await
                    .map_err(SourceError::Unavailable)?
            }
        };
        if let Some(heads) = self.source.attested_heads.clone()
            && let Some(head) = self.attested_head(heads.latest()).await
            && heads.publish(head)
        {
            debug!(
                beacon_slot = head.beacon_slot,
                block = head.block_number.0,
                "published a sync-committee-attested execution head"
            );
        }
        Ok(anchor)
    }

    /// Verify the latest optimistic update from the first peer that serves
    /// a usable head newer than `published`. A peer whose update is stale,
    /// no newer than the published head, or signed by too few of the sync
    /// committee is skipped; one that fails verification is banned. No head
    /// leaves the previous one in place: the live lane waits for it.
    ///
    /// Only the first peer is waited for. Once every connected peer was
    /// tried, the refresh ends instead of waiting out the request deadline
    /// for another: the next refresh asks again.
    async fn attested_head(&mut self, published: Option<AttestedHead>) -> Option<AttestedHead> {
        let deadline = tokio::time::Instant::now() + self.network.request_timeout;
        let mut tried = HashSet::new();
        loop {
            let wait_until = if tried.is_empty() {
                deadline
            } else {
                tokio::time::Instant::now()
            };
            let Ok(Some(peer)) = self.network.next_peer(&mut tried, wait_until).await else {
                break;
            };
            let update = match self
                .network
                .request(peer, LightClientRequest::Optimistic, deadline)
                .await
            {
                Ok(LightClientResponse::Optimistic(update)) => update,
                Ok(_) => {
                    self.network.ban(peer, "answered with other material");
                    continue;
                }
                Err(PeerFailure::Unavailable(error)) => {
                    debug!(%peer, %error, "consensus peer served no optimistic update");
                    continue;
                }
                Err(PeerFailure::Invalid(error)) => {
                    self.network.ban(peer, error);
                    continue;
                }
            };
            match self
                .verifier
                .verify_attested_head(&update, self.source.clock.now())
            {
                Ok(head)
                    if published
                        .is_none_or(|published| head.beacon_slot > published.beacon_slot) =>
                {
                    return Some(head);
                }
                Ok(head) => {
                    debug!(
                        %peer,
                        beacon_slot = head.beacon_slot,
                        "consensus peer's attested head is no newer than the published one"
                    );
                }
                Err(error) if error.is_unusable_head() => {
                    debug!(%peer, %error, "consensus peer's optimistic update anchors no head");
                }
                Err(error) => {
                    self.network.ban(peer, error);
                }
            }
        }
        None
    }

    async fn refresh_once(&mut self) -> Result<VerifiedFinalityAnchor, String> {
        let deadline = tokio::time::Instant::now() + self.network.request_timeout;
        let mut tried = HashSet::new();
        let mut errors = Vec::new();
        while let Some(peer) = self
            .network
            .next_peer(&mut tried, deadline)
            .await
            .map_err(RequestFailure::into_message)?
        {
            let update = match self
                .network
                .request(peer, LightClientRequest::Finality, deadline)
                .await
            {
                Ok(LightClientResponse::Finality(update)) => update,
                Ok(_) => {
                    errors.push(self.network.ban(peer, "answered with other material"));
                    continue;
                }
                Err(PeerFailure::Unavailable(error)) => {
                    errors.push(format!("{peer}: {error}"));
                    continue;
                }
                Err(PeerFailure::Invalid(error)) => {
                    errors.push(self.network.ban(peer, error));
                    continue;
                }
            };
            let signature_slot = *update.signature_slot();
            let now = self.source.clock.now();
            if signature_slot > verification_slot(now) {
                let error = format!(
                    "finality update signed at slot {signature_slot} is ahead of the local clock"
                );
                // A little early is a skewed clock; an epoch beyond it is a
                // claim no honest peer makes, so the peer is not asked again.
                errors.push(if signed_implausibly_ahead(signature_slot, now) {
                    self.network.ban(peer, error)
                } else {
                    format!("{peer}: {error}")
                });
                continue;
            }
            // The update's signature may need the next sync committees.
            for period in self.verifier.update_periods_before(signature_slot) {
                self.sync_period(period).await?;
            }
            match self
                .verifier
                .apply_finality_update(&update, self.source.clock.now())
            {
                Ok(anchor) => {
                    self.source
                        .anchor_writer
                        .persist(anchor, self.start.checkpoint_root)
                        .await;
                    return Ok(anchor);
                }
                Err(error) if error.is_stale_update() => errors.push(format!("{peer}: {error}")),
                Err(error) => errors.push(self.network.ban(peer, error)),
            }
        }
        if errors.is_empty() {
            errors
                .push("no connected peer became available before the request deadline".to_owned());
        }
        self.network.peer_errors.extend(errors.iter().cloned());
        Err(format!(
            "no connected consensus peer served a verifiable finality update: {}",
            errors.join("; ")
        ))
    }

    async fn sync_period(&mut self, period: u64) -> Result<(), String> {
        let verifier = &mut self.verifier;
        let clock = &self.source.clock;
        self.network
            .request_verified(
                LightClientRequest::Update { period },
                |response| match response {
                    LightClientResponse::Update(update) => {
                        verifier.apply_update(&update, clock.now())
                    }
                    _ => Err(BeaconApiError::Protocol(
                        "answered an update request with other material".to_owned(),
                    )),
                },
            )
            .await
            .map_err(RequestFailure::into_message)
    }

    async fn reconnect(&mut self, previous_error: String) -> Result<(), SourceError> {
        let anchor = self
            .verifier
            .finalized_anchor()
            .unwrap_or_else(|_| self.verifier.checkpoint_anchor());
        self.network = reconnect_network(
            &self.source,
            anchor.beacon_block_root,
            anchor.beacon_slot,
            &self.cancellation,
            previous_error,
        )
        .await
        .map_err(|failure| SourceError::Unavailable(failure.errors.join("; ")))?;
        Ok(())
    }
}

/// Bootstrap from `root`, reconnecting once when no connected peer serves a
/// verifiable bootstrap.
async fn bootstrap(
    source: &VerifiedConsensusP2p,
    network: &mut P2pNetwork,
    root: [u8; 32],
    status_slot: u64,
    cancellation: &CancellationToken,
) -> Result<MainnetLightClientVerifier, ProbeFailure> {
    match bootstrap_verified(network, &source.config, &source.clock, root).await {
        Ok(verifier) => Ok(verifier),
        Err(RequestFailure::Fatal(error)) => Err(network.failure(error)),
        Err(RequestFailure::Retry(error)) => {
            *network = reconnect_network(source, root, status_slot, cancellation, error).await?;
            bootstrap_verified(network, &source.config, &source.clock, root)
                .await
                .map_err(|failure| network.failure(failure.into_message()))
        }
    }
}

/// Bootstrap from `root` with the first peer whose bootstrap verifies.
async fn bootstrap_verified(
    network: &mut P2pNetwork,
    config: &ConsensusP2pConfig,
    clock: &Clock,
    root: [u8; 32],
) -> Result<MainnetLightClientVerifier, RequestFailure> {
    network
        .request_verified(
            LightClientRequest::Bootstrap(root),
            |response| match response {
                LightClientResponse::Bootstrap(bootstrap) => MainnetLightClientVerifier::bootstrap(
                    root,
                    &bootstrap,
                    config.max_checkpoint_age,
                    clock.now(),
                ),
                _ => Err(BeaconApiError::Protocol(
                    "answered a bootstrap request with other material".to_owned(),
                )),
            },
        )
        .await
}

/// Why no connected peer served acceptable light-client material.
#[derive(Debug)]
enum RequestFailure {
    /// Other peers, a reconnect, or a later poll may succeed.
    Retry(String),
    /// The local trust root is unusable, whichever peer answers.
    Fatal(String),
}

impl RequestFailure {
    fn into_message(self) -> String {
        match self {
            Self::Retry(error) | Self::Fatal(error) => error,
        }
    }
}

/// Open a peer set that skips, and keeps sharing, the source's banned peers.
async fn connect_network(
    source: &VerifiedConsensusP2p,
    status_root: [u8; 32],
    status_slot: u64,
    cancellation: &CancellationToken,
) -> Result<P2pNetwork, ProbeFailure> {
    let banned = source
        .banned
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    let mut network = source
        .connector
        .connect(
            &source.config,
            status_root,
            status_slot,
            &banned,
            cancellation.child_token(),
        )
        .await?;
    network.banned = source.banned.clone();
    Ok(network)
}

async fn reconnect_network(
    source: &VerifiedConsensusP2p,
    status_root: [u8; 32],
    status_slot: u64,
    cancellation: &CancellationToken,
    previous_error: String,
) -> Result<P2pNetwork, ProbeFailure> {
    let mut network = connect_network(source, status_root, status_slot, cancellation)
        .await
        .map_err(|mut failure| {
            failure.errors.insert(
                0,
                format!("reconnect after request failure: {previous_error}"),
            );
            failure
        })?;
    network.peer_errors.push(format!(
        "reconnected after request failure: {previous_error}"
    ));
    Ok(network)
}

fn beacon_source_error(error: &BeaconApiError) -> SourceError {
    SourceError::Protocol(error.to_string())
}

#[derive(Debug)]
struct ProbeFailure {
    discovered_peers: usize,
    connected_peers: usize,
    attempted_peers: usize,
    errors: Vec<String>,
}

/// Light-client material requested from one consensus peer.
#[derive(Clone, Copy, Debug)]
enum LightClientRequest {
    Bootstrap([u8; 32]),
    Update { period: u64 },
    Finality,
    Optimistic,
}

/// Decoded, fork-context-checked, but still unverified peer material.
enum LightClientResponse {
    Bootstrap(Box<Bootstrap<MainnetConsensusSpec>>),
    Update(Box<Update<MainnetConsensusSpec>>),
    Finality(Box<FinalityUpdate<MainnetConsensusSpec>>),
    Optimistic(Box<OptimisticUpdate<MainnetConsensusSpec>>),
}

/// Why one peer did not serve usable light-client material.
#[derive(Debug)]
enum PeerFailure {
    /// Transport failure or an error response; the peer may serve later.
    Unavailable(String),
    /// Undecodable or wrong-fork material.
    Invalid(String),
}

impl fmt::Display for PeerFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable(error) | Self::Invalid(error) => formatter.write_str(error),
        }
    }
}

/// Consensus peers that completed the Status handshake. The libp2p swarm
/// implements it; tests script it.
#[async_trait]
trait ConsensusPeers: Send {
    fn connected(&self) -> Vec<PeerId>;

    /// Wait until the connected set changes; `false` once it never will.
    async fn changed(&mut self) -> bool;

    async fn request(
        &mut self,
        peer: PeerId,
        request: LightClientRequest,
        timeout: Duration,
    ) -> Result<LightClientResponse, PeerFailure>;
}

/// Opens a consensus peer set that advertises `status_root`/`status_slot`.
#[async_trait]
trait PeerConnector: Send + Sync + fmt::Debug {
    async fn connect(
        &self,
        config: &ConsensusP2pConfig,
        status_root: [u8; 32],
        status_slot: u64,
        banned: &HashSet<PeerId>,
        cancellation: CancellationToken,
    ) -> Result<P2pNetwork, ProbeFailure>;
}

/// Discovers mainnet peers over discv5 and dials them over libp2p.
#[derive(Debug)]
struct Libp2pConnector {
    clock: Clock,
}

#[async_trait]
impl PeerConnector for Libp2pConnector {
    async fn connect(
        &self,
        config: &ConsensusP2pConfig,
        status_root: [u8; 32],
        status_slot: u64,
        banned: &HashSet<PeerId>,
        cancellation: CancellationToken,
    ) -> Result<P2pNetwork, ProbeFailure> {
        let current_slot = current_slot(self.clock.now());
        let mut discovered = discover_mainnet_peers(config, current_slot, cancellation.clone())
            .await
            .map_err(|error| ProbeFailure {
                discovered_peers: 0,
                connected_peers: 0,
                attempted_peers: 0,
                errors: vec![error.to_string()],
            })?;
        discovered.retain(|peer| !banned.contains(&peer.peer_id));
        let discovered_peers = discovered.len();
        if discovered_peers < config.minimum_peers {
            return Err(ProbeFailure {
                discovered_peers,
                connected_peers: 0,
                attempted_peers: 0,
                errors: vec![format!(
                    "discovery found {discovered_peers} dialable peers, need {}",
                    config.minimum_peers
                )],
            });
        }
        let attempted_peers = discovered.len().min(config.maximum_peers);
        let status = encode_status(status_root, status_slot, current_slot);
        let peers = discovered
            .into_iter()
            .take(config.maximum_peers)
            .collect::<Vec<_>>();
        let (control, connected) = spawn_swarm(
            &peers,
            status.clone(),
            self.clock.clone(),
            cancellation.clone(),
        )
        .map_err(|error| ProbeFailure {
            discovered_peers,
            connected_peers: 0,
            attempted_peers,
            errors: vec![error.to_string()],
        })?;
        let mut network = P2pNetwork::new(
            Box::new(Libp2pPeers { control, connected }),
            config,
            cancellation,
            discovered_peers,
            attempted_peers,
        );
        network
            .wait_for_connections(config.minimum_peers, config.connection_timeout)
            .await
            .map_err(|error| network.failure(error))?;
        Ok(network)
    }
}

struct Libp2pPeers {
    control: libp2p_stream::Control,
    connected: watch::Receiver<Vec<PeerId>>,
}

#[async_trait]
impl ConsensusPeers for Libp2pPeers {
    fn connected(&self) -> Vec<PeerId> {
        self.connected.borrow().clone()
    }

    async fn changed(&mut self) -> bool {
        self.connected.changed().await.is_ok()
    }

    async fn request(
        &mut self,
        peer: PeerId,
        request: LightClientRequest,
        timeout: Duration,
    ) -> Result<LightClientResponse, PeerFailure> {
        let (protocol, payload) = match request {
            LightClientRequest::Bootstrap(root) => (BOOTSTRAP_PROTOCOL, root.to_vec()),
            LightClientRequest::Update { period } => {
                let mut payload = Vec::with_capacity(16);
                payload.extend_from_slice(&period.to_le_bytes());
                payload.extend_from_slice(&1_u64.to_le_bytes());
                (UPDATE_PROTOCOL, payload)
            }
            LightClientRequest::Finality => (FINALITY_PROTOCOL, Vec::new()),
            LightClientRequest::Optimistic => (OPTIMISTIC_PROTOCOL, Vec::new()),
        };
        let response = request_peer(
            &mut self.control,
            peer,
            StreamProtocol::new(protocol),
            &payload,
            true,
            timeout,
        )
        .await
        .map_err(PeerFailure::Unavailable)?;
        decode_light_client_response(request, &response.payload, response.context)
    }
}

/// Decode a peer's SSZ reply to `request` and check its fork-digest
/// `context` against the slot of the header it carries. Undecodable or
/// wrong-fork material is invalid: its peer is banned. A digest of no fork
/// this release knows is checked first: after a network fork this release
/// does not support, every honest peer answers so, and none is banned.
fn decode_light_client_response(
    request: LightClientRequest,
    payload: &[u8],
    context: Option<[u8; 4]>,
) -> Result<LightClientResponse, PeerFailure> {
    if let Some(received) = context
        && !known_mainnet_fork_digests().contains(&received)
    {
        static WARNED: std::sync::Once = std::sync::Once::new();
        WARNED.call_once(|| {
            warn!(
                digest = ?received,
                "consensus peers serve a fork this release cannot verify; the network may have forked, so upgrade Leani"
            );
        });
        return Err(PeerFailure::Unavailable(format!(
            "peer serves fork digest {received:02x?}, unknown to this release; upgrade Leani if the network forked"
        )));
    }
    let (material, slot) = match request {
        LightClientRequest::Bootstrap(_) => {
            let bootstrap =
                Bootstrap::<MainnetConsensusSpec>::from_ssz_bytes(payload).map_err(|error| {
                    PeerFailure::Invalid(format!("invalid bootstrap SSZ: {error:?}"))
                })?;
            let slot = bootstrap.header().beacon().slot;
            (LightClientResponse::Bootstrap(Box::new(bootstrap)), slot)
        }
        LightClientRequest::Update { .. } => {
            let update =
                Update::<MainnetConsensusSpec>::from_ssz_bytes(payload).map_err(|error| {
                    PeerFailure::Invalid(format!("invalid light-client update SSZ: {error:?}"))
                })?;
            let slot = update.attested_header().beacon().slot;
            (LightClientResponse::Update(Box::new(update)), slot)
        }
        LightClientRequest::Finality => {
            let update = FinalityUpdate::<MainnetConsensusSpec>::from_ssz_bytes(payload).map_err(
                |error| PeerFailure::Invalid(format!("invalid finality update SSZ: {error:?}")),
            )?;
            let slot = update.attested_header().beacon().slot;
            (LightClientResponse::Finality(Box::new(update)), slot)
        }
        LightClientRequest::Optimistic => {
            let update = OptimisticUpdate::<MainnetConsensusSpec>::from_ssz_bytes(payload)
                .map_err(|error| {
                    PeerFailure::Invalid(format!("invalid optimistic update SSZ: {error:?}"))
                })?;
            let slot = update.attested_header.beacon().slot;
            (LightClientResponse::Optimistic(Box::new(update)), slot)
        }
    };
    validate_context(context, slot).map_err(PeerFailure::Invalid)?;
    Ok(material)
}

struct P2pNetwork {
    peers: Box<dyn ConsensusPeers>,
    cancellation: CancellationToken,
    request_timeout: Duration,
    poll_interval: Duration,
    discovered_peers: usize,
    attempted_peers: usize,
    peer_errors: Vec<String>,
    /// Peers that served invalid material, shared with the source.
    banned: Arc<Mutex<HashSet<PeerId>>>,
}

impl std::fmt::Debug for P2pNetwork {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("P2pNetwork")
            .field("connected_peers", &self.connected_count())
            .field("request_timeout", &self.request_timeout)
            .field("poll_interval", &self.poll_interval)
            .field("discovered_peers", &self.discovered_peers)
            .field("attempted_peers", &self.attempted_peers)
            .field("peer_errors", &self.peer_errors)
            .finish_non_exhaustive()
    }
}

impl Drop for P2pNetwork {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

impl P2pNetwork {
    fn new(
        peers: Box<dyn ConsensusPeers>,
        config: &ConsensusP2pConfig,
        cancellation: CancellationToken,
        discovered_peers: usize,
        attempted_peers: usize,
    ) -> Self {
        Self {
            peers,
            cancellation,
            request_timeout: config.request_timeout,
            poll_interval: config.poll_interval,
            discovered_peers,
            attempted_peers,
            peer_errors: Vec::new(),
            banned: Arc::default(),
        }
    }

    fn connected_count(&self) -> usize {
        self.peers.connected().len()
    }

    fn failure(&self, error: String) -> ProbeFailure {
        let mut errors = self.peer_errors.clone();
        errors.push(error);
        ProbeFailure {
            discovered_peers: self.discovered_peers,
            connected_peers: self.connected_count(),
            attempted_peers: self.attempted_peers,
            errors,
        }
    }

    async fn wait_for_connections(
        &mut self,
        minimum: usize,
        timeout: Duration,
    ) -> Result<(), String> {
        tokio::time::timeout(timeout, async {
            loop {
                if self.connected_count() >= minimum {
                    return Ok(());
                }
                tokio::select! {
                    changed = self.peers.changed() => {
                        if !changed {
                            return Err("consensus swarm stopped".to_owned());
                        }
                    }
                    () = self.cancellation.cancelled() => {
                        return Err("consensus P2P connection was cancelled".to_owned());
                    }
                }
            }
        })
        .await
        .map_err(|_| {
            format!(
                "timed out with {} connected consensus peers, need {minimum}",
                self.connected_count()
            )
        })?
    }

    /// The next connected peer not yet tried or banned, in random order so
    /// no single peer answers first every time. Waits for new connections
    /// until `deadline`.
    async fn next_peer(
        &mut self,
        tried: &mut HashSet<PeerId>,
        deadline: tokio::time::Instant,
    ) -> Result<Option<PeerId>, RequestFailure> {
        let order = RandomState::new();
        loop {
            let banned = self
                .banned
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone();
            let candidate = self
                .peers
                .connected()
                .into_iter()
                .filter(|peer| !tried.contains(peer) && !banned.contains(peer))
                .min_by_key(|peer| order.hash_one(peer));
            if let Some(peer) = candidate {
                tried.insert(peer);
                return Ok(Some(peer));
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Ok(None);
            }
            // Dials continue in the swarm task after the minimum connection
            // threshold is reached. Give newly connected peers a chance
            // instead of freezing failover to the first transient snapshot.
            tokio::select! {
                changed = self.peers.changed() => {
                    if !changed {
                        return Ok(None);
                    }
                }
                () = tokio::time::sleep(remaining.min(Duration::from_millis(100))) => {}
                () = self.cancellation.cancelled() => {
                    return Err(RequestFailure::Retry(
                        "consensus P2P request was cancelled".to_owned(),
                    ));
                }
            }
        }
    }

    async fn request(
        &mut self,
        peer: PeerId,
        request: LightClientRequest,
        deadline: tokio::time::Instant,
    ) -> Result<LightClientResponse, PeerFailure> {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        self.peers
            .request(peer, request, remaining.min(PEER_ATTEMPT_TIMEOUT))
            .await
    }

    /// Ban `peer` for the source's lifetime after it served invalid
    /// material.
    fn ban(&mut self, peer: PeerId, error: impl fmt::Display) -> String {
        debug!(%peer, %error, "banning consensus peer for invalid light-client material");
        self.banned
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(peer);
        format!("{peer}: {error} (banned)")
    }

    /// Request `request` from peers until `accept` verifies a response.
    ///
    /// A peer whose material fails verification is banned for this session;
    /// one whose material is only stale is skipped.
    async fn request_verified<T>(
        &mut self,
        request: LightClientRequest,
        mut accept: impl FnMut(LightClientResponse) -> Result<T, BeaconApiError>,
    ) -> Result<T, RequestFailure> {
        let deadline = tokio::time::Instant::now() + self.request_timeout;
        let mut tried = HashSet::new();
        let mut errors = Vec::new();
        while let Some(peer) = self.next_peer(&mut tried, deadline).await? {
            let error = match self.request(peer, request, deadline).await {
                Ok(response) => match accept(response) {
                    Ok(accepted) => return Ok(accepted),
                    Err(error) if error.is_stale_update() => format!("{peer}: {error}"),
                    Err(
                        error @ (BeaconApiError::CheckpointTooOld { .. }
                        | BeaconApiError::InvalidCheckpointTime),
                    ) => return Err(RequestFailure::Fatal(error.to_string())),
                    Err(error) => self.ban(peer, error),
                },
                Err(PeerFailure::Unavailable(error)) => format!("{peer}: {error}"),
                Err(PeerFailure::Invalid(error)) => self.ban(peer, error),
            };
            errors.push(error);
        }
        if errors.is_empty() {
            errors
                .push("no connected peer became available before the request deadline".to_owned());
        }
        self.peer_errors.extend(errors.iter().cloned());
        Err(RequestFailure::Retry(format!(
            "no connected consensus peer served a verifiable {request:?}: {}",
            errors.join("; ")
        )))
    }
}

#[derive(Debug)]
struct PeerResponse {
    context: Option<[u8; 4]>,
    payload: Vec<u8>,
}

async fn request_peer(
    control: &mut libp2p_stream::Control,
    peer: PeerId,
    protocol: StreamProtocol,
    request: &[u8],
    response_has_context: bool,
    timeout: Duration,
) -> Result<PeerResponse, String> {
    tokio::time::timeout(timeout, async {
        let mut stream = control
            .open_stream(peer, protocol.clone())
            .await
            .map_err(|error| error.to_string())?;
        if !request.is_empty() {
            let encoded = encode_snappy_payload(request)?;
            stream
                .write_all(&encoded)
                .await
                .map_err(|error| error.to_string())?;
        }
        stream.close().await.map_err(|error| error.to_string())?;
        let mut wire = Vec::new();
        let mut limited = stream.take(u64::try_from(MAX_WIRE_BYTES + 1).expect("bounded"));
        limited
            .read_to_end(&mut wire)
            .await
            .map_err(|error| error.to_string())?;
        if wire.len() > MAX_WIRE_BYTES {
            return Err(format!("peer response exceeds {MAX_WIRE_BYTES} wire bytes"));
        }
        decode_peer_response(&wire, response_has_context)
    })
    .await
    .map_err(|_| format!("request timed out for {}", protocol.as_ref()))?
}

fn decode_peer_response(wire: &[u8], response_has_context: bool) -> Result<PeerResponse, String> {
    let (&code, remainder) = wire
        .split_first()
        .ok_or_else(|| "peer returned no response chunk".to_owned())?;
    if code != 0 {
        let detail = decode_snappy_payload(remainder, 256).map_or_else(
            |error| format!("undecodable error payload: {error}"),
            |bytes| String::from_utf8_lossy(&bytes).into_owned(),
        );
        return Err(format!("peer returned response code {code}: {detail}"));
    }
    let (context, encoded) = if response_has_context {
        if remainder.len() < 4 {
            return Err("successful response omitted fork-digest context".to_owned());
        }
        (
            Some(remainder[..4].try_into().expect("four-byte context slice")),
            &remainder[4..],
        )
    } else {
        (None, remainder)
    };
    let payload = decode_snappy_payload(encoded, MAX_LIGHT_CLIENT_SSZ_BYTES)?;
    Ok(PeerResponse { context, payload })
}

fn encode_snappy_payload(payload: &[u8]) -> Result<Vec<u8>, String> {
    let mut framed = FrameEncoder::new(Vec::new());
    framed
        .write_all(payload)
        .map_err(|error| error.to_string())?;
    let compressed = framed.into_inner().map_err(|error| error.to_string())?;
    let mut encoded = encode_varint(
        u64::try_from(payload.len()).map_err(|_| "payload length exceeds u64".to_owned())?,
    );
    encoded.extend_from_slice(&compressed);
    Ok(encoded)
}

fn decode_snappy_payload(encoded: &[u8], maximum: usize) -> Result<Vec<u8>, String> {
    let (declared, prefix) = decode_varint(encoded)?;
    let declared =
        usize::try_from(declared).map_err(|_| "declared SSZ length exceeds usize".to_owned())?;
    if declared > maximum {
        return Err(format!(
            "declared SSZ length {declared} exceeds maximum {maximum}"
        ));
    }
    let decoder = FrameDecoder::new(&encoded[prefix..]);
    let mut payload = Vec::with_capacity(declared);
    decoder
        .take(u64::try_from(maximum + 1).expect("bounded"))
        .read_to_end(&mut payload)
        .map_err(|error| error.to_string())?;
    if payload.len() != declared {
        return Err(format!(
            "declared SSZ length {declared} differs from decoded {}",
            payload.len()
        ));
    }
    Ok(payload)
}

fn encode_varint(mut value: u64) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(10);
    loop {
        let mut byte = u8::try_from(value & 0x7f).expect("seven bits fit u8");
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        encoded.push(byte);
        if value == 0 {
            return encoded;
        }
    }
}

fn decode_varint(bytes: &[u8]) -> Result<(u64, usize), String> {
    let mut value = 0_u64;
    for (index, byte) in bytes.iter().copied().take(10).enumerate() {
        if index == 9 && byte > 1 {
            return Err("protobuf varint overflows u64".to_owned());
        }
        value |= u64::from(byte & 0x7f) << (index * 7);
        if byte & 0x80 == 0 {
            let consumed = index + 1;
            if encode_varint(value).len() != consumed {
                return Err("protobuf varint is not minimally encoded".to_owned());
            }
            return Ok((value, consumed));
        }
    }
    Err("protobuf varint is absent or exceeds ten bytes".to_owned())
}

fn validate_context(context: Option<[u8; 4]>, slot: u64) -> Result<(), String> {
    let received = context.ok_or_else(|| "response omitted fork context".to_owned())?;
    let expected = mainnet_fork_digest(slot);
    if received != expected {
        return Err(format!(
            "response fork digest {received:02x?} does not match slot {slot} digest {expected:02x?}"
        ));
    }
    Ok(())
}

fn encode_status(checkpoint_root: [u8; 32], checkpoint_slot: u64, current_slot: u64) -> Vec<u8> {
    // A checkpoint block before its epoch's first slot means that slot was
    // skipped, so the block is the checkpoint of the epoch that follows it.
    let finalized_epoch = checkpoint_slot.div_ceil(32);
    let head_slot = checkpoint_slot.max(finalized_epoch.saturating_mul(32));
    let mut status = Vec::with_capacity(STATUS_BYTES);
    status.extend_from_slice(&mainnet_fork_digest(current_slot));
    // The weak-subjectivity checkpoint is locally trusted before bootstrap
    // verification and is therefore the only internally consistent chain
    // position this outbound light client can advertise. Advertising a zero
    // genesis status makes current full nodes classify the client as
    // irrelevant and disconnect it before a bootstrap can complete.
    status.extend_from_slice(&checkpoint_root);
    status.extend_from_slice(&finalized_epoch.to_le_bytes());
    status.extend_from_slice(&checkpoint_root);
    status.extend_from_slice(&head_slot.to_le_bytes());
    status
}

fn validate_peer_status(status: &[u8], current_slot: u64) -> Result<(), String> {
    if status.len() != STATUS_BYTES {
        return Err(format!(
            "status response is {} bytes, expected {STATUS_BYTES}",
            status.len()
        ));
    }
    let expected = mainnet_fork_digest(current_slot);
    if status[..4] != expected {
        return Err(format!(
            "peer status fork digest {:02x?} differs from expected {expected:02x?}",
            &status[..4]
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct PeerAddress {
    peer_id: PeerId,
    address: Multiaddr,
}

async fn discover_mainnet_peers(
    config: &ConsensusP2pConfig,
    current_slot: u64,
    cancellation: CancellationToken,
) -> Result<Vec<PeerAddress>, ConsensusP2pError> {
    let key = CombinedKey::generate_secp256k1();
    let local_enr = discv5::Enr::empty(&key)
        .map_err(|error| ConsensusP2pError::Discovery(error.to_string()))?;
    let listen = ListenConfig::from_ip(config.discovery_ip, config.discovery_port);
    let discovery_config = ConfigBuilder::new(listen).build();
    let mut discovery = Discv5::new(local_enr, key, discovery_config)
        .map_err(|error| ConsensusP2pError::Discovery(error.to_string()))?;
    let mut enrs = Vec::new();
    let mut configured_nodes = HashSet::new();
    for encoded in &config.bootnodes {
        let enr = encoded
            .parse::<Enr>()
            .map_err(|error| ConsensusP2pError::Discovery(error.clone()))?;
        configured_nodes.insert(enr.node_id());
        discovery
            .add_enr(enr.clone())
            .map_err(|error| ConsensusP2pError::Discovery((*error).to_owned()))?;
        enrs.push(enr);
    }
    discovery
        .start()
        .await
        .map_err(|error| ConsensusP2pError::Discovery(error.to_string()))?;
    let query = async {
        for _ in 0..DISCOVERY_QUERIES {
            match discovery.find_node(NodeId::random()).await {
                Ok(found) => enrs.extend(found),
                Err(error) => debug!(%error, "consensus discv5 query failed"),
            }
            if enrs.len() >= config.maximum_peers.saturating_mul(2) {
                break;
            }
        }
    };
    tokio::select! {
        () = query => {}
        () = tokio::time::sleep(config.discovery_timeout) => {
            debug!("consensus discovery reached its time bound");
        }
        () = cancellation.cancelled() => {
            return Err(ConsensusP2pError::Cancelled);
        }
    }
    enrs.extend(discovery.table_entries_enr());
    let expected_digest = mainnet_fork_digest(current_slot);
    let mut digest_counts = BTreeMap::<[u8; 4], usize>::new();
    let mut configured_preferred = BTreeSet::new();
    let mut preferred = BTreeSet::new();
    let mut configured_fallback = BTreeSet::new();
    let mut fallback = BTreeSet::new();
    for enr in enrs {
        // Requiring `eth2` distinguishes consensus ENRs from execution-only
        // discovery records. The Status exchange below remains authoritative:
        // official bootstrap ENRs can retain an older fork digest because
        // their purpose is discovery, while the peer they lead us to speaks
        // the current protocol.
        let Some(digest) = enr_fork_digest(&enr) else {
            continue;
        };
        *digest_counts.entry(digest).or_default() += 1;
        let Some(peer) = peer_address(&enr) else {
            continue;
        };
        match (
            digest == expected_digest,
            configured_nodes.contains(&enr.node_id()),
        ) {
            (true, true) => configured_preferred.insert(peer),
            (true, false) => preferred.insert(peer),
            (false, true) => configured_fallback.insert(peer),
            (false, false) => fallback.insert(peer),
        };
    }
    debug!(
        expected_digest = ?expected_digest,
        ?digest_counts,
        preferred = configured_preferred.len() + preferred.len(),
        fallback = fallback.len(),
        "classified consensus discovery records"
    );
    let mut peers = configured_preferred.into_iter().collect::<Vec<_>>();
    peers.extend(preferred);
    peers.extend(configured_fallback);
    peers.extend(fallback);
    Ok(peers)
}

#[allow(deprecated)]
fn enr_fork_digest(enr: &Enr) -> Option<[u8; 4]> {
    let value = enr.get("eth2")?;
    value.get(..4)?.try_into().ok()
}

fn peer_address(enr: &Enr) -> Option<PeerAddress> {
    let peer_id = enr_peer_id(enr)?;
    let mut address = Multiaddr::empty();
    if let (Some(ip), Some(port)) = (enr.ip4(), enr.tcp4()) {
        address.push(Protocol::Ip4(ip));
        address.push(Protocol::Tcp(port));
    } else if let (Some(ip), Some(port)) = (enr.ip6(), enr.tcp6()) {
        address.push(Protocol::Ip6(ip));
        address.push(Protocol::Tcp(port));
    } else {
        return None;
    }
    Some(PeerAddress { peer_id, address })
}

fn enr_peer_id(enr: &Enr) -> Option<PeerId> {
    let public = match enr.public_key() {
        CombinedPublicKey::Secp256k1(key) => {
            let key = identity::secp256k1::PublicKey::try_from_bytes(&key.encode()).ok()?;
            identity::PublicKey::from(key)
        }
        CombinedPublicKey::Ed25519(key) => {
            let key = identity::ed25519::PublicKey::try_from_bytes(&key.encode()).ok()?;
            identity::PublicKey::from(key)
        }
    };
    Some(PeerId::from_public_key(&public))
}

#[allow(clippy::too_many_lines)]
fn spawn_swarm(
    peers: &[PeerAddress],
    status: Vec<u8>,
    clock: Clock,
    cancellation: CancellationToken,
) -> Result<(libp2p_stream::Control, watch::Receiver<Vec<PeerId>>), ConsensusP2pError> {
    let key = identity::Keypair::generate_secp256k1();
    let mut swarm = SwarmBuilder::with_existing_identity(key)
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )
        .map_err(|error| ConsensusP2pError::Network(error.to_string()))?
        .with_behaviour(|_| libp2p_stream::Behaviour::new())
        .map_err(|error| ConsensusP2pError::Network(error.to_string()))?
        .with_swarm_config(|config| config.with_idle_connection_timeout(Duration::from_secs(90)))
        .build();
    let mut control = swarm.behaviour().new_control();
    let status_incoming = control
        .accept(StreamProtocol::new(STATUS_PROTOCOL))
        .map_err(|error| ConsensusP2pError::Network(error.to_string()))?;
    let mut ping_control = swarm.behaviour().new_control();
    let ping_incoming = ping_control
        .accept(StreamProtocol::new(PING_PROTOCOL))
        .map_err(|error| ConsensusP2pError::Network(error.to_string()))?;
    let mut goodbye_control = swarm.behaviour().new_control();
    let goodbye_incoming = goodbye_control
        .accept(StreamProtocol::new(GOODBYE_PROTOCOL))
        .map_err(|error| ConsensusP2pError::Network(error.to_string()))?;
    let peer_addresses = peers
        .iter()
        .map(|peer| (peer.peer_id, peer.address.clone()))
        .collect::<HashMap<_, _>>();
    for peer in peers {
        let options = DialOpts::peer_id(peer.peer_id)
            .addresses(vec![peer.address.clone()])
            .build();
        if let Err(error) = swarm.dial(options) {
            debug!(peer = %peer.peer_id, %error, "could not schedule consensus peer dial");
        }
    }
    let (connected_tx, connected_rx) = watch::channel(Vec::new());
    let (status_tx, mut status_rx) = mpsc::unbounded_channel();
    let status_control = control.clone();
    let handler_cancellation = cancellation.clone();
    tokio::spawn(handle_status_requests(
        status_incoming,
        status.clone(),
        handler_cancellation,
    ));
    let handler_cancellation = cancellation.clone();
    tokio::spawn(handle_u64_requests(ping_incoming, 0, handler_cancellation));
    let handler_cancellation = cancellation.clone();
    tokio::spawn(handle_u64_requests(
        goodbye_incoming,
        1,
        handler_cancellation,
    ));
    tokio::spawn(async move {
        let mut validated = BTreeSet::new();
        let mut redial_attempts = HashMap::<PeerId, usize>::new();
        loop {
            tokio::select! {
                () = cancellation.cancelled() => break,
                Some((peer, result)) = status_rx.recv() => {
                    match result {
                        Ok(()) => {
                            validated.insert(peer);
                            connected_tx.send_replace(validated.iter().copied().collect());
                        }
                        Err(error) => {
                            debug!(%peer, %error, "consensus peer status handshake failed");
                            let _ = swarm.disconnect_peer_id(peer);
                        }
                    }
                }
                event = swarm.select_next_some() => {
                    match event {
                        SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                            let mut peer_control = status_control.clone();
                            let peer_status = status.clone();
                            let peer_status_tx = status_tx.clone();
                            let peer_clock = clock.clone();
                            tokio::spawn(async move {
                                let result = async {
                                    let response = request_peer(
                                        &mut peer_control,
                                        peer_id,
                                        StreamProtocol::new(STATUS_PROTOCOL),
                                        &peer_status,
                                        false,
                                        PEER_ATTEMPT_TIMEOUT,
                                    )
                                    .await?;
                                    validate_peer_status(
                                        &response.payload,
                                        current_slot(peer_clock.now()),
                                    )
                                }
                                .await;
                                let _ = peer_status_tx.send((peer_id, result));
                            });
                        }
                        SwarmEvent::ConnectionClosed {
                            peer_id,
                            num_established: 0,
                            ..
                        } => {
                            validated.remove(&peer_id);
                            connected_tx.send_replace(validated.iter().copied().collect());
                            if let Some(address) = peer_addresses.get(&peer_id)
                                && *redial_attempts.entry(peer_id).or_default()
                                    < MAX_PEER_REDIALS
                            {
                                *redial_attempts.entry(peer_id).or_default() += 1;
                                let options = DialOpts::peer_id(peer_id)
                                    .addresses(vec![address.clone()])
                                    .build();
                                if let Err(error) = swarm.dial(options) {
                                    debug!(%peer_id, %error, "could not schedule consensus peer redial");
                                }
                            }
                        }
                        SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                            debug!(?peer_id, %error, "consensus peer dial failed");
                            if let Some(peer_id) = peer_id
                                && let Some(address) = peer_addresses.get(&peer_id)
                                && *redial_attempts.entry(peer_id).or_default()
                                    < MAX_PEER_REDIALS
                            {
                                *redial_attempts.entry(peer_id).or_default() += 1;
                                let options = DialOpts::peer_id(peer_id)
                                    .addresses(vec![address.clone()])
                                    .build();
                                if let Err(error) = swarm.dial(options) {
                                    debug!(%peer_id, %error, "could not schedule consensus peer redial");
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    });
    Ok((control, connected_rx))
}

async fn handle_status_requests(
    mut incoming: libp2p_stream::IncomingStreams,
    status: Vec<u8>,
    cancellation: CancellationToken,
) {
    loop {
        let next = tokio::select! {
            () = cancellation.cancelled() => break,
            next = incoming.next() => next,
        };
        let Some((peer, stream)) = next else {
            break;
        };
        let response = status.clone();
        tokio::spawn(async move {
            if let Err(error) = respond_to_request(stream, STATUS_BYTES, &response).await {
                debug!(%peer, %error, "failed inbound consensus status response");
            }
        });
    }
}

async fn handle_u64_requests(
    mut incoming: libp2p_stream::IncomingStreams,
    response: u64,
    cancellation: CancellationToken,
) {
    loop {
        let next = tokio::select! {
            () = cancellation.cancelled() => break,
            next = incoming.next() => next,
        };
        let Some((peer, stream)) = next else {
            break;
        };
        tokio::spawn(async move {
            if let Err(error) = respond_to_request(stream, 8, &response.to_le_bytes()).await {
                debug!(%peer, %error, "failed inbound consensus u64 response");
            }
        });
    }
}

async fn respond_to_request(
    mut stream: libp2p::swarm::Stream,
    expected_request_bytes: usize,
    response: &[u8],
) -> Result<(), String> {
    let request = tokio::time::timeout(
        PEER_ATTEMPT_TIMEOUT,
        read_control_request(&mut stream, expected_request_bytes),
    )
    .await
    .map_err(|_| "inbound control request timed out".to_owned())??;
    if request.len() != expected_request_bytes {
        return Err(format!(
            "inbound request is {} bytes, expected {expected_request_bytes}",
            request.len()
        ));
    }
    let mut encoded = vec![0];
    encoded.extend_from_slice(&encode_snappy_payload(response)?);
    stream
        .write_all(&encoded)
        .await
        .map_err(|error| error.to_string())?;
    stream.close().await.map_err(|error| error.to_string())
}

async fn read_control_request(
    stream: &mut libp2p::swarm::Stream,
    expected_request_bytes: usize,
) -> Result<Vec<u8>, String> {
    let mut wire = Vec::new();
    let mut chunk = [0_u8; 256];
    loop {
        let read = stream
            .read(&mut chunk)
            .await
            .map_err(|error| error.to_string())?;
        if read == 0 {
            return decode_snappy_payload(&wire, expected_request_bytes);
        }
        wire.extend_from_slice(&chunk[..read]);
        if wire.len() > MAX_CONTROL_WIRE_BYTES {
            return Err(format!(
                "inbound control request exceeds {MAX_CONTROL_WIRE_BYTES} wire bytes"
            ));
        }
        if let Ok(request) = decode_snappy_payload(&wire, expected_request_bytes)
            && request.len() == expected_request_bytes
        {
            return Ok(request);
        }
    }
}

fn embedded_mainnet_bootnodes() -> Vec<String> {
    EMBEDDED_MAINNET_BOOTNODES
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

#[derive(Debug, Error)]
pub enum ConsensusP2pError {
    #[error("invalid consensus P2P configuration: {0}")]
    InvalidConfig(String),
    #[error("consensus discovery failed: {0}")]
    Discovery(String),
    #[error("consensus libp2p failed: {0}")]
    Network(String),
    #[error("consensus P2P operation was cancelled")]
    Cancelled,
}

#[cfg(test)]
mod tests {
    use std::{
        path::Path,
        sync::atomic::{AtomicUsize, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use alloy_primitives::b256;
    use leani_finality_beacon_api::{
        CheckpointOrigin, FINALITY_ANCHOR_FILE, PersistedFinalityAnchor, persist_finality_anchor,
        read_finality_anchor,
    };
    use leani_primitives::BlockHash;

    use super::*;

    const BOOTSTRAP_JSON: &str =
        include_str!("../../finality-beacon-api/tests/fixtures/helios/bootstrap.json");
    const UPDATES_JSON: &str =
        include_str!("../../finality-beacon-api/tests/fixtures/helios/updates.json");
    const FINALITY_JSON: &str =
        include_str!("../../finality-beacon-api/tests/fixtures/helios/finality.json");
    const OPTIMISTIC_JSON: &str =
        include_str!("../../finality-beacon-api/tests/fixtures/helios/optimistic.json");
    const BOOTSTRAP_SLOT: u64 = 7_069_376;
    const FINALIZED_SLOT: u64 = 7_109_344;
    const FINALIZED_BLOCK: u64 = 17_923_026;
    const ATTESTED_SLOT: u64 = 7_109_431;
    const ATTESTED_BLOCK: u64 = 17_923_113;
    const SIGNATURE_SLOT: u64 = 7_109_431;
    const SLOTS_PER_PERIOD: u64 = 8_192;
    const SLOT_SECONDS: u64 = 12;
    const DAY: u64 = 86_400;
    /// An earlier checkpoint root that no scripted peer serves.
    const UNSERVED_ROOT: [u8; 32] = [0x11; 32];

    fn bootstrap_root() -> [u8; 32] {
        b256!("5afc212a7924789b2bc86acad3ab3a6ffb1f6e97253ea50bee7f4f51422c9275").into()
    }

    fn bootstrap_anchor() -> VerifiedFinalityAnchor {
        VerifiedFinalityAnchor {
            beacon_slot: BOOTSTRAP_SLOT,
            beacon_block_root: bootstrap_root(),
            execution_block_number: 17_883_333,
            execution_block_hash: BlockHash::new(
                b256!("d131b92cb98455882c2c7b4ebf55dc6d02cc47e0e55a4d9570dea498affd6e74").into(),
            ),
        }
    }

    fn checkpoint(anchor: VerifiedFinalityAnchor) -> ConsensusCheckpoint {
        ConsensusCheckpoint {
            beacon_slot: anchor.beacon_slot,
            beacon_block_root: anchor.beacon_block_root,
            execution_block_hash: anchor.execution_block_hash,
            obtained_at_unix_seconds: 0,
            source: "test checkpoint".to_owned(),
        }
    }

    fn fixture_data(encoded: &str) -> serde_json::Value {
        let mut response: serde_json::Value =
            serde_json::from_str(encoded).expect("fixture response");
        response["data"].take()
    }

    fn fixture_updates() -> Vec<serde_json::Value> {
        serde_json::from_str::<Vec<serde_json::Value>>(UPDATES_JSON)
            .expect("fixture updates")
            .into_iter()
            .map(|mut update| update["data"].take())
            .collect()
    }

    fn attested_slot(update: &serde_json::Value) -> u64 {
        update["attested_header"]["beacon"]["slot"]
            .as_str()
            .and_then(|slot| slot.parse().ok())
            .expect("attested slot")
    }

    fn fixture_bootstrap() -> Bootstrap<MainnetConsensusSpec> {
        serde_json::from_value(fixture_data(BOOTSTRAP_JSON)).expect("fixture bootstrap")
    }

    fn fixture_update(period: u64) -> Option<Update<MainnetConsensusSpec>> {
        fixture_updates()
            .into_iter()
            .find(|update| attested_slot(update) / SLOTS_PER_PERIOD == period)
            .map(|update| serde_json::from_value(update).expect("fixture update"))
    }

    fn fixture_finality(behaviour: Behaviour) -> FinalityUpdate<MainnetConsensusSpec> {
        let data = match behaviour {
            Behaviour::Honest | Behaviour::ForgedOptimistic | Behaviour::LowParticipation => {
                fixture_data(FINALITY_JSON)
            }
            // The signed period-867 update served again as an older
            // finality update.
            Behaviour::Stale => {
                let mut update = fixture_updates().swap_remove(5);
                let fields = update.as_object_mut().expect("update fields");
                fields.remove("next_sync_committee");
                fields.remove("next_sync_committee_branch");
                update
            }
            // A different attested header than the sync committee signed.
            Behaviour::Forged => {
                let mut update = fixture_data(FINALITY_JSON);
                update["attested_header"]["beacon"]["proposer_index"] =
                    serde_json::Value::from("1");
                update
            }
        };
        serde_json::from_value(data).expect("fixture finality update")
    }

    fn fixture_optimistic(behaviour: Behaviour) -> OptimisticUpdate<MainnetConsensusSpec> {
        let mut data = fixture_data(OPTIMISTIC_JSON);
        match behaviour {
            Behaviour::Honest | Behaviour::Stale => {}
            // A different attested header than the sync committee signed.
            Behaviour::Forged | Behaviour::ForgedOptimistic => {
                data["attested_header"]["beacon"]["proposer_index"] = serde_json::Value::from("1");
            }
            // 341 of the 512 members: one short of two thirds.
            Behaviour::LowParticipation => {
                let mut bits = [0_u8; 64];
                bits[..42].fill(0xff);
                bits[42] = 0x1f;
                data["sync_aggregate"]["sync_committee_bits"] =
                    serde_json::Value::from(format!("0x{}", hex::encode(bits)));
            }
        }
        serde_json::from_value(data).expect("fixture optimistic update")
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum Behaviour {
        Honest,
        Stale,
        Forged,
        /// Honest finality, and a forged optimistic update.
        ForgedOptimistic,
        /// Honest finality, and an optimistic update too few signed.
        LowParticipation,
    }

    /// A scripted consensus peer set serving the vendored Helios fixtures.
    #[derive(Debug)]
    struct Script {
        behaviours: Mutex<BTreeMap<PeerId, Behaviour>>,
        /// Peers that stay disconnected until a peer serves a forged update.
        late: Mutex<BTreeSet<PeerId>>,
        revealed: watch::Sender<bool>,
        requests: Mutex<Vec<String>>,
        connects: AtomicUsize,
        /// The banned peers each connection was asked to skip.
        dial_bans: Mutex<Vec<HashSet<PeerId>>>,
    }

    impl Default for Script {
        fn default() -> Self {
            Self {
                behaviours: Mutex::default(),
                late: Mutex::default(),
                revealed: watch::channel(false).0,
                requests: Mutex::default(),
                connects: AtomicUsize::default(),
                dial_bans: Mutex::default(),
            }
        }
    }

    impl Script {
        /// Peers in ascending `PeerId` order with the given behaviours.
        fn new(behaviours: &[Behaviour]) -> (Arc<Self>, Vec<PeerId>) {
            let mut peers = behaviours
                .iter()
                .map(|_| PeerId::random())
                .collect::<Vec<_>>();
            peers.sort();
            let script = Self::default();
            script
                .behaviours
                .lock()
                .expect("behaviours")
                .extend(peers.iter().copied().zip(behaviours.iter().copied()));
            (Arc::new(script), peers)
        }

        /// Add an honest peer that connects only after a forged update.
        fn late_honest_peer(&self) -> PeerId {
            let peer = PeerId::random();
            self.set(peer, Behaviour::Honest);
            self.late.lock().expect("late peers").insert(peer);
            peer
        }

        fn set(&self, peer: PeerId, behaviour: Behaviour) {
            self.behaviours
                .lock()
                .expect("behaviours")
                .insert(peer, behaviour);
        }

        fn requests(&self) -> Vec<String> {
            self.requests.lock().expect("requests").clone()
        }

        fn count(&self, fragment: &str) -> usize {
            self.requests()
                .iter()
                .filter(|request| request.contains(fragment))
                .count()
        }
    }

    struct ScriptedPeers {
        script: Arc<Script>,
        revealed: watch::Receiver<bool>,
    }

    #[async_trait]
    impl ConsensusPeers for ScriptedPeers {
        fn connected(&self) -> Vec<PeerId> {
            let revealed = *self.revealed.borrow();
            let late = self.script.late.lock().expect("late peers").clone();
            self.script
                .behaviours
                .lock()
                .expect("behaviours")
                .keys()
                .copied()
                .filter(|peer| revealed || !late.contains(peer))
                .collect()
        }

        async fn changed(&mut self) -> bool {
            self.revealed.changed().await.is_ok()
        }

        async fn request(
            &mut self,
            peer: PeerId,
            request: LightClientRequest,
            _timeout: Duration,
        ) -> Result<LightClientResponse, PeerFailure> {
            let label = match request {
                LightClientRequest::Bootstrap(root) => format!("bootstrap {}", hex::encode(root)),
                LightClientRequest::Update { period } => format!("update {period}"),
                LightClientRequest::Finality => "finality".to_owned(),
                LightClientRequest::Optimistic => "optimistic".to_owned(),
            };
            self.script
                .requests
                .lock()
                .expect("requests")
                .push(format!("{peer} {label}"));
            let behaviour = self
                .script
                .behaviours
                .lock()
                .expect("behaviours")
                .get(&peer)
                .copied()
                .ok_or_else(|| PeerFailure::Unavailable("peer disconnected".to_owned()))?;
            let unavailable = || PeerFailure::Unavailable("resource unavailable".to_owned());
            match request {
                LightClientRequest::Bootstrap(root) if root == bootstrap_root() => Ok(
                    LightClientResponse::Bootstrap(Box::new(fixture_bootstrap())),
                ),
                LightClientRequest::Bootstrap(_) => Err(unavailable()),
                LightClientRequest::Update { period } => fixture_update(period)
                    .map(|update| LightClientResponse::Update(Box::new(update)))
                    .ok_or_else(unavailable),
                LightClientRequest::Finality => {
                    if behaviour == Behaviour::Forged {
                        self.script.revealed.send_replace(true);
                    }
                    Ok(LightClientResponse::Finality(Box::new(fixture_finality(
                        behaviour,
                    ))))
                }
                LightClientRequest::Optimistic => {
                    if behaviour == Behaviour::ForgedOptimistic {
                        self.script.revealed.send_replace(true);
                    }
                    Ok(LightClientResponse::Optimistic(Box::new(
                        fixture_optimistic(behaviour),
                    )))
                }
            }
        }
    }

    #[derive(Debug)]
    struct ScriptedConnector(Arc<Script>);

    #[async_trait]
    impl PeerConnector for ScriptedConnector {
        async fn connect(
            &self,
            config: &ConsensusP2pConfig,
            _status_root: [u8; 32],
            _status_slot: u64,
            banned: &HashSet<PeerId>,
            cancellation: CancellationToken,
        ) -> Result<P2pNetwork, ProbeFailure> {
            self.0.connects.fetch_add(1, Ordering::SeqCst);
            self.0
                .dial_bans
                .lock()
                .expect("dial bans")
                .push(banned.clone());
            let peers = self.0.behaviours.lock().expect("behaviours").len();
            Ok(P2pNetwork::new(
                Box::new(ScriptedPeers {
                    script: self.0.clone(),
                    revealed: self.0.revealed.subscribe(),
                }),
                config,
                cancellation,
                peers,
                peers,
            ))
        }
    }

    fn slot_time(slot: u64) -> u64 {
        leani_finality_beacon_api::MAINNET_GENESIS_TIME + slot * SLOT_SECONDS
    }

    /// Wall clock that advances with tokio time, so paused-time tests
    /// simulate days of polling instantly.
    fn simulated_clock(start: u64) -> Clock {
        let started = tokio::time::Instant::now();
        Clock::new(move || UNIX_EPOCH + Duration::from_secs(start) + started.elapsed())
    }

    fn scripted_source(
        script: &Arc<Script>,
        clock: Clock,
        anchor: AnchorFile,
        poll_interval: Duration,
    ) -> VerifiedConsensusP2p {
        VerifiedConsensusP2p::with_connector(
            ConsensusP2pConfig {
                minimum_peers: 1,
                poll_interval,
                anchor,
                ..ConsensusP2pConfig::default()
            },
            Arc::new(ScriptedConnector(script.clone())),
            clock,
        )
        .expect("scripted source")
    }

    fn read_write(path: &Path) -> AnchorFile {
        AnchorFile::ReadWrite {
            path: path.to_path_buf(),
            write_failures: Arc::default(),
        }
    }

    /// The operator's configured checkpoint.
    fn operator(root: [u8; 32], slot: u64) -> TrustedCheckpoint {
        TrustedCheckpoint {
            root,
            slot: Some(slot),
            origin: CheckpointOrigin::Operator,
        }
    }

    fn finalized_slot(event: Option<Result<FinalityEvent, SourceError>>) -> u64 {
        match event {
            Some(Ok(FinalityEvent::Finalized { beacon_slot, .. })) => beacon_slot,
            other => panic!("expected a finalized event, got {other:?}"),
        }
    }

    #[test]
    fn status_advertises_the_epoch_of_a_skipped_boundary_slot() {
        let epoch = 400_000_u64;
        let status = encode_status([0x42; 32], epoch * 32 - 3, epoch * 32);
        assert_eq!(&status[36..44], &epoch.to_le_bytes());
        let head_slot = u64::from_le_bytes(status[76..84].try_into().expect("head slot"));
        assert!(head_slot >= epoch * 32, "head slot {head_slot}");

        let aligned = encode_status([0x42; 32], epoch * 32, epoch * 32);
        assert_eq!(&aligned[36..44], &epoch.to_le_bytes());
        assert_eq!(&aligned[76..84], &(epoch * 32).to_le_bytes());
    }

    #[tokio::test(start_paused = true)]
    async fn a_bad_first_peer_does_not_fail_startup() {
        // Only the forging peer is connected at first, so it answers first.
        let (script, peers) = Script::new(&[Behaviour::Forged]);
        let honest = script.late_honest_peer();
        let source = scripted_source(
            &script,
            simulated_clock(slot_time(SIGNATURE_SLOT) + 60),
            AnchorFile::Disabled,
            Duration::from_secs(SLOT_SECONDS),
        );
        let mut events = source
            .subscribe(checkpoint(bootstrap_anchor()), CancellationToken::new())
            .await
            .expect("startup survives a forged finality update");
        assert_eq!(finalized_slot(events.next().await), FINALIZED_SLOT);
        let requests = script.requests();
        let forged = requests
            .iter()
            .position(|request| request == &format!("{} finality", peers[0]))
            .expect("the forging peer is asked first");
        assert!(
            requests[forged..]
                .iter()
                .any(|request| request == &format!("{honest} finality")),
            "{requests:?}"
        );

        let report = source
            .probe_checkpoint(operator(bootstrap_root(), BOOTSTRAP_SLOT))
            .await;
        assert!(report.accepted, "{:?}", report.errors);
        assert_eq!(
            report.selected.map(|anchor| anchor.beacon_slot),
            Some(FINALIZED_SLOT)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn refreshes_publish_the_verified_attested_head() {
        // Only the peer that forges its optimistic update is connected at
        // first, so it is asked first; an honest peer connects afterwards.
        let (script, peers) = Script::new(&[Behaviour::ForgedOptimistic]);
        let honest = script.late_honest_peer();
        let heads = AttestedHeadPublisher::new();
        let source = scripted_source(
            &script,
            simulated_clock(slot_time(SIGNATURE_SLOT) + 60),
            AnchorFile::Disabled,
            Duration::from_secs(SLOT_SECONDS),
        )
        .with_attested_heads(heads.clone());
        let mut events = source
            .subscribe(checkpoint(bootstrap_anchor()), CancellationToken::new())
            .await
            .expect("subscribe");
        match events.next().await {
            Some(Ok(FinalityEvent::Finalized {
                block_number,
                beacon_slot,
                ..
            })) => {
                assert_eq!(block_number, BlockNumber(FINALIZED_BLOCK));
                assert_eq!(beacon_slot, FINALIZED_SLOT);
            }
            other => panic!("expected a finalized event, got {other:?}"),
        }
        let head = heads.latest().expect("a verified attested head");
        assert_eq!(
            (head.beacon_slot, head.block_number),
            (ATTESTED_SLOT, BlockNumber(ATTESTED_BLOCK))
        );
        let requests = script.requests();
        assert!(
            requests.contains(&format!("{} optimistic", peers[0]))
                && requests.contains(&format!("{honest} optimistic")),
            "{requests:?}"
        );
        // The forging peer is banned: later refreshes never ask it again.
        let before = script.requests().len();
        let quiet =
            tokio::time::timeout(Duration::from_secs(3 * SLOT_SECONDS), events.next()).await;
        assert!(quiet.is_err(), "the stream yielded {quiet:?}");
        let later = script.requests()[before..].to_vec();
        assert!(
            !later.is_empty()
                && later
                    .iter()
                    .all(|request| !request.starts_with(&peers[0].to_string())),
            "{later:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn low_participation_heads_are_skipped_without_a_ban() {
        let (script, peers) = Script::new(&[Behaviour::LowParticipation]);
        let heads = AttestedHeadPublisher::new();
        let source = scripted_source(
            &script,
            simulated_clock(slot_time(SIGNATURE_SLOT) + 60),
            AnchorFile::Disabled,
            Duration::from_secs(SLOT_SECONDS),
        )
        .with_attested_heads(heads.clone());
        let mut events = source
            .subscribe(checkpoint(bootstrap_anchor()), CancellationToken::new())
            .await
            .expect("subscribe");
        assert_eq!(finalized_slot(events.next().await), FINALIZED_SLOT);
        // Finality advances, but no head does: too few members signed it.
        assert_eq!(heads.latest(), None);
        // Low participation is no fault of the peer, so it is asked again.
        let quiet =
            tokio::time::timeout(Duration::from_secs(3 * SLOT_SECONDS), events.next()).await;
        assert!(quiet.is_err(), "the stream yielded {quiet:?}");
        assert!(script.count(&format!("{} optimistic", peers[0])) >= 2);
        assert_eq!(heads.latest(), None);
    }

    #[tokio::test(start_paused = true)]
    async fn refreshes_skip_heads_no_newer_than_the_published_one() {
        let (script, _) = Script::new(&[Behaviour::Honest, Behaviour::Honest]);
        let heads = AttestedHeadPublisher::new();
        // A head from a later slot is published already, so the fixture's
        // head is stale for both peers.
        let newer = AttestedHead {
            beacon_slot: ATTESTED_SLOT + 1,
            beacon_block_root: [0x77; 32],
            block_number: BlockNumber(ATTESTED_BLOCK + 1),
            block_hash: BlockHash::new([0x77; 32]),
        };
        assert!(heads.publish(newer));
        let source = scripted_source(
            &script,
            simulated_clock(slot_time(SIGNATURE_SLOT) + 60),
            AnchorFile::Disabled,
            Duration::from_secs(SLOT_SECONDS),
        )
        .with_attested_heads(heads.clone());
        let started = tokio::time::Instant::now();
        let mut events = source
            .subscribe(checkpoint(bootstrap_anchor()), CancellationToken::new())
            .await
            .expect("subscribe");
        assert_eq!(finalized_slot(events.next().await), FINALIZED_SLOT);
        // Final review B4: once every peer was tried, the refresh waited out
        // the request timeout for another to connect; two such refreshes
        // outlast the attested-head grace.
        assert!(
            started.elapsed() < ConsensusP2pConfig::default().request_timeout,
            "the refresh waited {:?} after trying every peer",
            started.elapsed()
        );
        // The refresh went on to the next peer instead of settling for the
        // first stale head, and banned neither.
        assert_eq!(
            script.count("optimistic"),
            2,
            "the refresh stopped at the first, stale head"
        );
        assert_eq!(heads.latest(), Some(newer));
        let quiet =
            tokio::time::timeout(Duration::from_secs(2 * SLOT_SECONDS), events.next()).await;
        assert!(quiet.is_err(), "the stream yielded {quiet:?}");
        assert!(script.count("optimistic") >= 4, "a stale peer was banned");
    }

    /// Encode `update` as the SSZ `LightClientOptimisticUpdate` a peer sends:
    /// the offset of the attested header, the sync aggregate, and the
    /// signature slot, then the header: its beacon header, the offset of the
    /// execution payload header, and the execution branch, then the payload
    /// header.
    fn optimistic_ssz(update: &OptimisticUpdate<MainnetConsensusSpec>) -> Vec<u8> {
        use ssz::Encode as _;

        let header = &update.attested_header;
        let beacon = header.beacon().as_ssz_bytes();
        let branch = header
            .execution_branch()
            .expect("a Capella header has an execution branch")
            .as_ssz_bytes();
        let execution = header
            .execution()
            .expect("a Capella header has an execution payload")
            .as_ssz_bytes();
        let header_offset = beacon.len() + 4 + branch.len();
        assert_eq!(header_offset, 244);
        let mut encoded_header = beacon;
        encoded_header
            .extend_from_slice(&u32::try_from(header_offset).expect("offset").to_le_bytes());
        encoded_header.extend_from_slice(&branch);
        encoded_header.extend_from_slice(&execution);
        let aggregate = update.sync_aggregate.as_ssz_bytes();
        let offset = 4 + aggregate.len() + 8;
        assert_eq!(offset, 172);
        let mut encoded = u32::try_from(offset)
            .expect("offset")
            .to_le_bytes()
            .to_vec();
        encoded.extend_from_slice(&aggregate);
        encoded.extend_from_slice(&update.signature_slot.to_le_bytes());
        encoded.extend_from_slice(&encoded_header);
        encoded
    }

    /// A verifier synced from the fixtures, as a finality refresh leaves it.
    fn fixture_verifier(now: SystemTime) -> MainnetLightClientVerifier {
        let mut verifier = MainnetLightClientVerifier::bootstrap(
            bootstrap_root(),
            &fixture_bootstrap(),
            DEFAULT_MAX_CHECKPOINT_AGE,
            now,
        )
        .expect("verified bootstrap");
        for period in 862..867 {
            verifier
                .apply_update(&fixture_update(period).expect("fixture update"), now)
                .expect("verified update");
        }
        verifier
            .apply_finality_update(&fixture_finality(Behaviour::Honest), now)
            .expect("verified finality");
        verifier
    }

    #[test]
    fn optimistic_updates_decode_from_peer_ssz_with_their_fork_context() {
        let encoded = optimistic_ssz(&fixture_optimistic(Behaviour::Honest));
        let context = Some(mainnet_fork_digest(ATTESTED_SLOT));
        let decoded =
            decode_light_client_response(LightClientRequest::Optimistic, &encoded, context)
                .map_err(|error| error.to_string())
                .expect("decoded optimistic update");
        let LightClientResponse::Optimistic(update) = decoded else {
            panic!("decoded other material");
        };
        // It verifies to the head the Beacon API's JSON of the same update
        // gives.
        let now = UNIX_EPOCH + Duration::from_secs(slot_time(SIGNATURE_SLOT) + 60);
        let verifier = fixture_verifier(now);
        let head = verifier
            .verify_attested_head(&update, now)
            .expect("verified attested head");
        assert_eq!(
            head,
            verifier
                .verify_attested_head(&fixture_optimistic(Behaviour::Honest), now)
                .expect("verified JSON update")
        );
        assert_eq!(
            (head.beacon_slot, head.block_number, head.block_hash),
            (
                ATTESTED_SLOT,
                BlockNumber(ATTESTED_BLOCK),
                BlockHash::new(
                    b256!("3c015340e234ff7f8e75ecebb11d45154a394cd896ddcfcfffc941a07b314960")
                        .into()
                )
            )
        );
        // The wrong fork digest, or none, is invalid material, as is a
        // payload shorter than the container's fixed part.
        for context in [Some(mainnet_fork_digest(0)), None] {
            assert!(matches!(
                decode_light_client_response(LightClientRequest::Optimistic, &encoded, context),
                Err(PeerFailure::Invalid(_))
            ));
        }
        assert!(matches!(
            decode_light_client_response(LightClientRequest::Optimistic, &encoded[..171], context),
            Err(PeerFailure::Invalid(_))
        ));
        // A digest of no known fork is what every honest peer sends after a
        // fork this release does not support: unavailable, never banned,
        // even when its material does not decode.
        for payload in [&encoded[..], &encoded[..171]] {
            assert!(matches!(
                decode_light_client_response(
                    LightClientRequest::Optimistic,
                    payload,
                    Some([0xde, 0xad, 0xbe, 0xef])
                ),
                Err(PeerFailure::Unavailable(_))
            ));
        }
        // Cutting the payload header's extra data short still decodes, as
        // another header the committee did not sign: verification refuses
        // it, and its peer is banned.
        let Ok(LightClientResponse::Optimistic(cut)) = decode_light_client_response(
            LightClientRequest::Optimistic,
            &encoded[..encoded.len() - 1],
            context,
        ) else {
            panic!("a shortened extra data field is valid SSZ");
        };
        let error = verifier
            .verify_attested_head(&cut, now)
            .expect_err("an unsigned header");
        assert!(!error.is_unusable_head(), "{error}");
    }

    #[tokio::test(start_paused = true)]
    async fn stale_or_forged_peers_do_not_end_the_finality_stream() {
        let (script, peers) = Script::new(&[Behaviour::Honest, Behaviour::Honest]);
        let source = scripted_source(
            &script,
            simulated_clock(slot_time(SIGNATURE_SLOT) + 60),
            AnchorFile::Disabled,
            Duration::from_secs(SLOT_SECONDS),
        );
        let mut events = source
            .subscribe(checkpoint(bootstrap_anchor()), CancellationToken::new())
            .await
            .expect("subscribe");
        assert_eq!(finalized_slot(events.next().await), FINALIZED_SLOT);

        // The first peer falls behind the verified store.
        script.set(peers[0], Behaviour::Stale);
        let quiet =
            tokio::time::timeout(Duration::from_secs(10 * SLOT_SECONDS), events.next()).await;
        assert!(quiet.is_err(), "a stale peer ended the stream: {quiet:?}");

        // Then it forges an update and is not asked again this session.
        script.set(peers[0], Behaviour::Forged);
        let before = script.requests().len();
        let quiet =
            tokio::time::timeout(Duration::from_secs(10 * SLOT_SECONDS), events.next()).await;
        assert!(
            quiet.is_err(),
            "a forged update ended the stream: {quiet:?}"
        );
        let forged_requests = script.requests()[before..]
            .iter()
            .filter(|request| request.starts_with(&format!("{} finality", peers[0])))
            .count();
        assert!(
            forged_requests <= 1,
            "{forged_requests} requests to a banned peer"
        );
        assert_eq!(script.connects.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn restart_uses_the_persisted_anchor_without_the_configured_checkpoint_age() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join(FINALITY_ANCHOR_FILE);
        persist_finality_anchor(
            &path,
            &PersistedFinalityAnchor {
                anchor: bootstrap_anchor(),
                checkpoint_root: UNSERVED_ROOT,
            },
        )
        .expect("persist anchor");
        let (script, _) = Script::new(&[Behaviour::Honest, Behaviour::Honest]);
        // The configured checkpoint is more than 14 days old; the persisted
        // anchor verified from it is not.
        let source = scripted_source(
            &script,
            simulated_clock(slot_time(BOOTSTRAP_SLOT) + 13 * DAY),
            read_write(&path),
            Duration::from_secs(SLOT_SECONDS),
        );
        let report = source
            .probe_checkpoint(operator(UNSERVED_ROOT, BOOTSTRAP_SLOT - SLOTS_PER_PERIOD))
            .await;
        assert!(report.accepted, "{:?}", report.errors);
        assert_eq!(report.checkpoint_anchor, Some(bootstrap_anchor()));
        assert_eq!(
            report.selected.map(|anchor| anchor.beacon_slot),
            Some(FINALIZED_SLOT)
        );
        assert_eq!(
            script.count(&format!("bootstrap {}", hex::encode(UNSERVED_ROOT))),
            0
        );
        let persisted = read_finality_anchor(&path)
            .expect("read anchor")
            .expect("anchor present");
        assert_eq!(persisted.anchor.beacon_slot, FINALIZED_SLOT);
        assert_eq!(persisted.checkpoint_root, UNSERVED_ROOT);

        // The node then subscribes with the verified bootstrap anchor the
        // probe handed over, and keeps the operator's lineage. The fixtures
        // hold one bootstrap, so persist that one again.
        std::fs::remove_file(&path).expect("reset anchor");
        persist_finality_anchor(
            &path,
            &PersistedFinalityAnchor {
                anchor: bootstrap_anchor(),
                checkpoint_root: UNSERVED_ROOT,
            },
        )
        .expect("persist anchor");
        let mut events = source
            .subscribe(checkpoint(bootstrap_anchor()), CancellationToken::new())
            .await
            .expect("subscribe from the handed-over anchor");
        assert_eq!(finalized_slot(events.next().await), FINALIZED_SLOT);
        assert_eq!(
            read_finality_anchor(&path)
                .expect("read anchor")
                .expect("anchor present")
                .checkpoint_root,
            UNSERVED_ROOT
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_persisted_anchor_from_another_trust_root_is_ignored() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join(FINALITY_ANCHOR_FILE);
        persist_finality_anchor(
            &path,
            &PersistedFinalityAnchor {
                anchor: VerifiedFinalityAnchor {
                    beacon_slot: FINALIZED_SLOT,
                    beacon_block_root: [0x55; 32],
                    ..bootstrap_anchor()
                },
                checkpoint_root: [0x44; 32],
            },
        )
        .expect("persist anchor");
        let (script, _) = Script::new(&[Behaviour::Honest, Behaviour::Honest]);
        let source = scripted_source(
            &script,
            simulated_clock(slot_time(SIGNATURE_SLOT) + 60),
            read_write(&path),
            Duration::from_secs(SLOT_SECONDS),
        );
        let report = source
            .probe_checkpoint(operator(bootstrap_root(), BOOTSTRAP_SLOT))
            .await;
        assert!(report.accepted, "{:?}", report.errors);
        assert_eq!(report.checkpoint_anchor, Some(bootstrap_anchor()));
        assert_eq!(
            script.count(&format!("bootstrap {}", hex::encode([0x55; 32]))),
            0
        );
    }

    #[tokio::test(start_paused = true)]
    async fn an_unserved_persisted_anchor_falls_back_to_the_configured_checkpoint() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join(FINALITY_ANCHOR_FILE);
        persist_finality_anchor(
            &path,
            &PersistedFinalityAnchor {
                anchor: VerifiedFinalityAnchor {
                    beacon_slot: FINALIZED_SLOT,
                    beacon_block_root: [0x66; 32],
                    ..bootstrap_anchor()
                },
                checkpoint_root: bootstrap_root(),
            },
        )
        .expect("persist anchor");
        let (script, _) = Script::new(&[Behaviour::Honest, Behaviour::Honest]);
        let source = scripted_source(
            &script,
            simulated_clock(slot_time(SIGNATURE_SLOT) + 60),
            read_write(&path),
            Duration::from_secs(SLOT_SECONDS),
        );
        let report = source
            .probe_checkpoint(operator(bootstrap_root(), BOOTSTRAP_SLOT))
            .await;
        assert!(report.accepted, "{:?}", report.errors);
        assert_eq!(report.checkpoint_anchor, Some(bootstrap_anchor()));
        assert!(script.count(&format!("bootstrap {}", hex::encode([0x66; 32]))) > 0);
    }

    #[tokio::test(start_paused = true)]
    async fn banned_peers_stay_banned_across_reconnects() {
        let (script, peers) = Script::new(&[Behaviour::Honest, Behaviour::Honest]);
        let source = scripted_source(
            &script,
            simulated_clock(slot_time(SIGNATURE_SLOT) + 60),
            AnchorFile::Disabled,
            Duration::from_secs(SLOT_SECONDS),
        );
        let mut events = source
            .subscribe(checkpoint(bootstrap_anchor()), CancellationToken::new())
            .await
            .expect("subscribe");
        assert_eq!(finalized_slot(events.next().await), FINALIZED_SLOT);

        // One peer forges and the other falls behind, so each refresh
        // exhausts the peer set and reconnects.
        script.set(peers[0], Behaviour::Forged);
        script.set(peers[1], Behaviour::Stale);
        let before = script.requests().len();
        let quiet =
            tokio::time::timeout(Duration::from_secs(5 * SLOT_SECONDS), events.next()).await;
        assert!(quiet.is_err(), "the stream ended: {quiet:?}");
        assert!(
            script.connects.load(Ordering::SeqCst) >= 2,
            "the exhausted peer set reconnects"
        );
        let forged_requests = script.requests()[before..]
            .iter()
            .filter(|request| request.starts_with(&format!("{} finality", peers[0])))
            .count();
        assert_eq!(forged_requests, 1, "a banned peer was asked again");
        let dial_bans = script.dial_bans.lock().expect("dial bans").clone();
        assert!(
            dial_bans
                .last()
                .is_some_and(|banned| banned.contains(&peers[0])),
            "{dial_bans:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn one_bootstrap_serves_polls_past_the_checkpoint_age_limit() {
        let (script, _) = Script::new(&[Behaviour::Honest]);
        let source = scripted_source(
            &script,
            simulated_clock(slot_time(SIGNATURE_SLOT) + 60),
            AnchorFile::Disabled,
            Duration::from_secs(DAY),
        );
        let mut events = source
            .subscribe(checkpoint(bootstrap_anchor()), CancellationToken::new())
            .await
            .expect("subscribe");
        assert_eq!(finalized_slot(events.next().await), FINALIZED_SLOT);
        let quiet = tokio::time::timeout(Duration::from_secs(20 * DAY), events.next()).await;
        assert!(quiet.is_err(), "finality stream yielded {quiet:?}");
        assert!(script.count("finality") >= 20);
        assert_eq!(script.count("bootstrap"), 1);
        assert_eq!(script.count("update"), 5);
        assert_eq!(script.connects.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn protobuf_varints_are_strict() {
        for value in [0, 1, 127, 128, 16_384, u64::MAX] {
            let encoded = encode_varint(value);
            assert_eq!(decode_varint(&encoded), Ok((value, encoded.len())));
        }
        assert!(decode_varint(&[0x80, 0]).is_err());
        assert!(decode_varint(&[0xff; 10]).is_err());
    }

    #[test]
    fn snappy_frames_enforce_declared_length_and_limit() {
        let encoded = encode_snappy_payload(b"consensus").expect("encode");
        assert_eq!(
            decode_snappy_payload(&encoded, 64).expect("decode"),
            b"consensus"
        );
        assert!(decode_snappy_payload(&encoded, 4).is_err());
        let mut wrong = encoded;
        wrong[0] = 8;
        assert!(decode_snappy_payload(&wrong, 64).is_err());
    }

    #[test]
    fn embedded_bootnodes_are_signed_and_dialable() {
        let bootnodes = embedded_mainnet_bootnodes();
        assert!(bootnodes.len() >= 8);
        let mut dialable = 0;
        for encoded in bootnodes {
            let enr = encoded.parse::<Enr>().expect("ENR");
            assert!(enr.verify());
            dialable += usize::from(peer_address(&enr).is_some());
        }
        assert!(dialable >= 3, "only {dialable} bootnodes were dialable");
    }

    #[test]
    fn status_is_exact_ssz_container() {
        let now = current_slot(SystemTime::now());
        let status = encode_status([0x42; 32], 12_352, now);
        assert_eq!(status.len(), STATUS_BYTES);
        assert_eq!(&status[4..36], &[0x42; 32]);
        assert_eq!(&status[36..44], &(12_352_u64 / 32).to_le_bytes());
        assert_eq!(&status[44..76], &[0x42; 32]);
        assert_eq!(&status[76..84], &12_352_u64.to_le_bytes());
        assert!(validate_peer_status(&status, now).is_ok());
    }

    #[test]
    fn response_parser_requires_code_and_context() {
        assert!(decode_peer_response(&[], true).is_err());
        let payload = encode_snappy_payload(b"ok").expect("frame");
        let mut wire = vec![0];
        wire.extend_from_slice(&mainnet_fork_digest(current_slot(SystemTime::now())));
        wire.extend_from_slice(&payload);
        let decoded = decode_peer_response(&wire, true).expect("response");
        assert_eq!(decoded.payload, b"ok");
        assert!(decoded.context.is_some());
    }

    #[test]
    fn only_transient_refresh_failures_are_retried() {
        assert!(retryable_refresh_error(&SourceError::Unavailable(
            "peer cohort exhausted".to_owned()
        )));
        assert!(retryable_refresh_error(&SourceError::Disconnected(
            "peer disconnected".to_owned()
        )));
        assert!(!retryable_refresh_error(&SourceError::Protocol(
            "invalid verified update".to_owned()
        )));
        assert!(!retryable_refresh_error(&SourceError::Cancelled));
    }
}
