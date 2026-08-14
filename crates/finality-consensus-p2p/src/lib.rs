//! Locally verified Ethereum consensus finality over native consensus P2P.
//!
//! The transport implements the standard discv5 + libp2p Noise/Yamux
//! light-client req/resp path. Peers are untrusted: checkpoint bootstraps,
//! sync-committee transitions, BLS signatures, finality branches, and
//! execution payload branches are processed by the same verifier used by the
//! Beacon API adapter.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    io::{Read, Write},
    net::{IpAddr, Ipv4Addr},
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
    types::{Bootstrap, FinalityUpdate, Update},
};
use leani_finality_beacon_api::{
    BeaconApiError, DEFAULT_MAX_CHECKPOINT_AGE, HELIOS_REVISION, MainnetLightClientVerifier,
    VerifiedFinalityAnchor, mainnet_fork_digest,
};
use leani_primitives::{Capability, CapabilitySet, ChainId, SourceId, SourceKind, TrustModel};
use leani_source_api::{
    ConsensusCheckpoint, FinalityEvent, FinalityEventStream, FinalityModel, FinalitySource,
    Partitioning, SourceDescriptor, SourceError,
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
use tracing::debug;

const BOOTSTRAP_PROTOCOL: &str = "/eth2/beacon_chain/req/light_client_bootstrap/1/ssz_snappy";
const UPDATE_PROTOCOL: &str = "/eth2/beacon_chain/req/light_client_updates_by_range/1/ssz_snappy";
const FINALITY_PROTOCOL: &str = "/eth2/beacon_chain/req/light_client_finality_update/1/ssz_snappy";
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
}

impl Default for ConsensusP2pConfig {
    fn default() -> Self {
        Self {
            bootnodes: embedded_mainnet_bootnodes(),
            discovery_ip: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            discovery_port: 0,
            minimum_peers: 2,
            maximum_peers: 24,
            discovery_timeout: Duration::from_secs(15),
            connection_timeout: Duration::from_secs(20),
            request_timeout: Duration::from_secs(15),
            poll_interval: Duration::from_secs(12),
            max_checkpoint_age: DEFAULT_MAX_CHECKPOINT_AGE,
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
}

impl VerifiedConsensusP2p {
    /// Construct a mainnet source. No sockets are opened until probe/subscribe.
    ///
    /// # Errors
    ///
    /// Rejects invalid peer bounds, timeouts, or bootnode ENRs.
    pub fn mainnet(config: ConsensusP2pConfig) -> Result<Self, ConsensusP2pError> {
        config.validate()?;
        Ok(Self {
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

    /// Discover peers, perform the required status handshake, and verify a
    /// complete checkpoint-to-finality sync.
    pub async fn probe_checkpoint(
        &self,
        checkpoint_root: [u8; 32],
        checkpoint_slot: u64,
    ) -> ConsensusP2pProbeReport {
        match P2pLightClient::connect_and_sync(
            self.config.clone(),
            checkpoint_root,
            checkpoint_slot,
            CancellationToken::new(),
        )
        .await
        {
            Ok(client) => ConsensusP2pProbeReport {
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
            },
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
        let client = P2pLightClient::connect_and_sync(
            self.config.clone(),
            checkpoint.beacon_block_root,
            checkpoint.beacon_slot,
            cancellation.clone(),
        )
        .await
        .map_err(|failure| SourceError::Unavailable(failure.errors.join("; ")))?;
        let bootstrap = client.verifier.checkpoint_anchor();
        if bootstrap.beacon_slot != checkpoint.beacon_slot
            || bootstrap.beacon_block_root != checkpoint.beacon_block_root
            || bootstrap.execution_block_hash != checkpoint.execution_block_hash
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
                debug!(
                    %error,
                    "consensus finality refresh exhausted its current peer cohort; retrying"
                );
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
    config: ConsensusP2pConfig,
    cancellation: CancellationToken,
    network: P2pNetwork,
    verifier: MainnetLightClientVerifier,
    next_period: u64,
}

impl P2pLightClient {
    async fn connect_and_sync(
        config: ConsensusP2pConfig,
        checkpoint_root: [u8; 32],
        checkpoint_slot: u64,
        cancellation: CancellationToken,
    ) -> Result<Self, ProbeFailure> {
        let mut network = P2pNetwork::connect(
            &config,
            checkpoint_root,
            checkpoint_slot,
            cancellation.child_token(),
        )
        .await?;
        let bootstrap = match network.fetch_bootstrap(checkpoint_root).await {
            Ok(bootstrap) => bootstrap,
            Err(error) => {
                network = reconnect_network(
                    &config,
                    checkpoint_root,
                    checkpoint_slot,
                    &cancellation,
                    error,
                )
                .await?;
                network
                    .fetch_bootstrap(checkpoint_root)
                    .await
                    .map_err(|error| network.failure(error))?
            }
        };
        let mut verifier = MainnetLightClientVerifier::bootstrap(
            checkpoint_root,
            &bootstrap,
            config.max_checkpoint_age,
        )
        .map_err(|error| network.failure(error.to_string()))?;
        let current_period = MainnetLightClientVerifier::current_period();
        let mut period = verifier.first_required_period();
        // A bootstrap already supplies the current sync committee. Period P's
        // update is needed to cross into P+1, not to verify finality within P.
        // Deferring the current-period update avoids requiring peers to serve
        // optional next-committee material during initial startup.
        while period < current_period {
            let update = match network.fetch_update(period).await {
                Ok(update) => update,
                Err(error) => {
                    let anchor = verifier
                        .finalized_anchor()
                        .unwrap_or_else(|_| verifier.checkpoint_anchor());
                    network = reconnect_network(
                        &config,
                        anchor.beacon_block_root,
                        anchor.beacon_slot,
                        &cancellation,
                        error,
                    )
                    .await?;
                    network
                        .fetch_update(period)
                        .await
                        .map_err(|error| network.failure(error))?
                }
            };
            verifier
                .apply_update(&update)
                .map_err(|error| network.failure(error.to_string()))?;
            period = period.saturating_add(1);
        }
        let update = match network.fetch_finality().await {
            Ok(update) => update,
            Err(error) => {
                let anchor = verifier
                    .finalized_anchor()
                    .unwrap_or_else(|_| verifier.checkpoint_anchor());
                network = reconnect_network(
                    &config,
                    anchor.beacon_block_root,
                    anchor.beacon_slot,
                    &cancellation,
                    error,
                )
                .await?;
                network
                    .fetch_finality()
                    .await
                    .map_err(|error| network.failure(error))?
            }
        };
        verifier
            .apply_finality_update(&update)
            .map_err(|error| network.failure(error.to_string()))?;
        Ok(Self {
            config,
            cancellation,
            network,
            verifier,
            next_period: current_period,
        })
    }

    async fn refresh(&mut self) -> Result<VerifiedFinalityAnchor, SourceError> {
        let current_period = MainnetLightClientVerifier::current_period();
        while self.next_period < current_period {
            let update = self.fetch_update_resilient(self.next_period).await?;
            self.verifier
                .apply_update(&update)
                .map_err(|error| beacon_source_error(&error))?;
            self.next_period = self.next_period.saturating_add(1);
        }
        let update = self.fetch_finality_resilient().await?;
        self.verifier
            .apply_finality_update(&update)
            .map_err(|error| beacon_source_error(&error))
    }

    async fn fetch_update_resilient(
        &mut self,
        period: u64,
    ) -> Result<Update<MainnetConsensusSpec>, SourceError> {
        match self.network.fetch_update(period).await {
            Ok(update) => Ok(update),
            Err(error) => {
                self.reconnect(error).await?;
                self.network
                    .fetch_update(period)
                    .await
                    .map_err(SourceError::Unavailable)
            }
        }
    }

    async fn fetch_finality_resilient(
        &mut self,
    ) -> Result<FinalityUpdate<MainnetConsensusSpec>, SourceError> {
        match self.network.fetch_finality().await {
            Ok(update) => Ok(update),
            Err(error) => {
                self.reconnect(error).await?;
                self.network
                    .fetch_finality()
                    .await
                    .map_err(SourceError::Unavailable)
            }
        }
    }

    async fn reconnect(&mut self, previous_error: String) -> Result<(), SourceError> {
        let anchor = self
            .verifier
            .finalized_anchor()
            .unwrap_or_else(|_| self.verifier.checkpoint_anchor());
        self.network = reconnect_network(
            &self.config,
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

async fn reconnect_network(
    config: &ConsensusP2pConfig,
    status_root: [u8; 32],
    status_slot: u64,
    cancellation: &CancellationToken,
    previous_error: String,
) -> Result<P2pNetwork, ProbeFailure> {
    let mut network =
        P2pNetwork::connect(config, status_root, status_slot, cancellation.child_token())
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

struct P2pNetwork {
    control: libp2p_stream::Control,
    connected: watch::Receiver<Vec<PeerId>>,
    cancellation: CancellationToken,
    request_timeout: Duration,
    poll_interval: Duration,
    discovered_peers: usize,
    attempted_peers: usize,
    peer_errors: Vec<String>,
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
    async fn connect(
        config: &ConsensusP2pConfig,
        checkpoint_root: [u8; 32],
        checkpoint_slot: u64,
        cancellation: CancellationToken,
    ) -> Result<Self, ProbeFailure> {
        let discovered = discover_mainnet_peers(config, cancellation.clone())
            .await
            .map_err(|error| ProbeFailure {
                discovered_peers: 0,
                connected_peers: 0,
                attempted_peers: 0,
                errors: vec![error.to_string()],
            })?;
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
        let status = encode_status(checkpoint_root, checkpoint_slot);
        let peers = discovered
            .into_iter()
            .take(config.maximum_peers)
            .collect::<Vec<_>>();
        let (control, connected) = spawn_swarm(&peers, status.clone(), cancellation.clone())
            .map_err(|error| ProbeFailure {
                discovered_peers,
                connected_peers: 0,
                attempted_peers,
                errors: vec![error.to_string()],
            })?;
        let mut network = Self {
            control,
            connected,
            cancellation,
            request_timeout: config.request_timeout,
            poll_interval: config.poll_interval,
            discovered_peers,
            attempted_peers,
            peer_errors: Vec::new(),
        };
        network
            .wait_for_connections(config.minimum_peers, config.connection_timeout)
            .await
            .map_err(|error| network.failure(error))?;
        Ok(network)
    }

    fn connected_count(&self) -> usize {
        self.connected.borrow().len()
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
                    changed = self.connected.changed() => {
                        changed.map_err(|_| "consensus swarm stopped".to_owned())?;
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

    async fn fetch_bootstrap(
        &mut self,
        checkpoint_root: [u8; 32],
    ) -> Result<Bootstrap<MainnetConsensusSpec>, String> {
        let response = self
            .request_failover(
                StreamProtocol::new(BOOTSTRAP_PROTOCOL),
                checkpoint_root.to_vec(),
                true,
            )
            .await?;
        let bootstrap = Bootstrap::<MainnetConsensusSpec>::from_ssz_bytes(&response.payload)
            .map_err(|error| format!("invalid bootstrap SSZ: {error:?}"))?;
        validate_context(response.context, bootstrap.header().beacon().slot)?;
        Ok(bootstrap)
    }

    async fn fetch_update(&mut self, period: u64) -> Result<Update<MainnetConsensusSpec>, String> {
        let mut request = Vec::with_capacity(16);
        request.extend_from_slice(&period.to_le_bytes());
        request.extend_from_slice(&1_u64.to_le_bytes());
        let response = self
            .request_failover(StreamProtocol::new(UPDATE_PROTOCOL), request, true)
            .await?;
        let update = Update::<MainnetConsensusSpec>::from_ssz_bytes(&response.payload)
            .map_err(|error| format!("invalid light-client update SSZ: {error:?}"))?;
        validate_context(response.context, update.attested_header().beacon().slot)?;
        Ok(update)
    }

    async fn fetch_finality(&mut self) -> Result<FinalityUpdate<MainnetConsensusSpec>, String> {
        let response = self
            .request_failover(StreamProtocol::new(FINALITY_PROTOCOL), Vec::new(), true)
            .await?;
        let update = FinalityUpdate::<MainnetConsensusSpec>::from_ssz_bytes(&response.payload)
            .map_err(|error| format!("invalid finality update SSZ: {error:?}"))?;
        validate_context(response.context, update.attested_header().beacon().slot)?;
        Ok(update)
    }

    async fn request_failover(
        &mut self,
        protocol: StreamProtocol,
        request: Vec<u8>,
        response_has_context: bool,
    ) -> Result<PeerResponse, String> {
        let deadline = tokio::time::Instant::now() + self.request_timeout;
        let mut attempted = HashSet::new();
        let mut errors = Vec::new();
        loop {
            let peers = self.connected.borrow().clone();
            for peer in peers {
                if !attempted.insert(peer) {
                    continue;
                }
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    break;
                }
                match request_peer(
                    &mut self.control,
                    peer,
                    protocol.clone(),
                    &request,
                    response_has_context,
                    remaining.min(PEER_ATTEMPT_TIMEOUT),
                )
                .await
                {
                    Ok(response) => return Ok(response),
                    Err(error) => errors.push(format!("{peer}: {error}")),
                }
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            // Dials continue in the swarm task after the minimum connection
            // threshold is reached. Give newly connected peers a chance
            // instead of freezing failover to the first transient snapshot.
            let pause = remaining.min(Duration::from_millis(100));
            tokio::select! {
                changed = self.connected.changed() => {
                    if changed.is_err() {
                        break;
                    }
                }
                () = tokio::time::sleep(pause) => {}
                () = self.cancellation.cancelled() => {
                    return Err("consensus P2P request was cancelled".to_owned());
                }
            }
        }
        if errors.is_empty() {
            errors
                .push("no connected peer became available before the request deadline".to_owned());
        }
        self.peer_errors.extend(errors.iter().cloned());
        Err(format!(
            "all connected peers failed {}: {}",
            protocol.as_ref(),
            errors.join("; ")
        ))
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

fn encode_status(checkpoint_root: [u8; 32], checkpoint_slot: u64) -> Vec<u8> {
    let current_slot = current_slot();
    let mut status = Vec::with_capacity(STATUS_BYTES);
    status.extend_from_slice(&mainnet_fork_digest(current_slot));
    // The weak-subjectivity checkpoint is locally trusted before bootstrap
    // verification and is therefore the only internally consistent chain
    // position this outbound light client can advertise. Advertising a zero
    // genesis status makes current full nodes classify the client as
    // irrelevant and disconnect it before a bootstrap can complete.
    status.extend_from_slice(&checkpoint_root);
    status.extend_from_slice(&(checkpoint_slot / 32).to_le_bytes());
    status.extend_from_slice(&checkpoint_root);
    status.extend_from_slice(&checkpoint_slot.to_le_bytes());
    status
}

fn validate_peer_status(status: &[u8]) -> Result<(), String> {
    if status.len() != STATUS_BYTES {
        return Err(format!(
            "status response is {} bytes, expected {STATUS_BYTES}",
            status.len()
        ));
    }
    let expected = mainnet_fork_digest(current_slot());
    if status[..4] != expected {
        return Err(format!(
            "peer status fork digest {:02x?} differs from expected {expected:02x?}",
            &status[..4]
        ));
    }
    Ok(())
}

fn current_slot() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .saturating_sub(leani_finality_beacon_api::MAINNET_GENESIS_TIME)
        / 12
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct PeerAddress {
    peer_id: PeerId,
    address: Multiaddr,
}

async fn discover_mainnet_peers(
    config: &ConsensusP2pConfig,
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
    let expected_digest = mainnet_fork_digest(current_slot());
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
                                    validate_peer_status(&response.payload)
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
    use super::*;

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
        let status = encode_status([0x42; 32], 12_345);
        assert_eq!(status.len(), STATUS_BYTES);
        assert_eq!(&status[4..36], &[0x42; 32]);
        assert_eq!(&status[36..44], &(12_345_u64 / 32).to_le_bytes());
        assert_eq!(&status[44..76], &[0x42; 32]);
        assert_eq!(&status[76..84], &12_345_u64.to_le_bytes());
        assert!(validate_peer_status(&status).is_ok());
    }

    #[test]
    fn response_parser_requires_code_and_context() {
        assert!(decode_peer_response(&[], true).is_err());
        let payload = encode_snappy_payload(b"ok").expect("frame");
        let mut wire = vec![0];
        wire.extend_from_slice(&mainnet_fork_digest(current_slot()));
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
