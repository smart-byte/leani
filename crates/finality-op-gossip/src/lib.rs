//! Sequencer-signed unsafe blocks from an op-stack chain's consensus gossip.
//! A block the chain's sequencer key signed is `attested` there, with no L1
//! access. Peers are found over Discv5 by their `opstack` record entry;
//! blocks arrive on the `/optimism/<chain id>/<version>/blocks` topics.

use std::{
    collections::{HashMap, HashSet},
    net::Ipv4Addr,
    path::PathBuf,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use alloy_primitives::{Address, B256};
use discv5::{
    ConfigBuilder, Discv5, Enr, ListenConfig,
    enr::{CombinedKey, CombinedPublicKey, EnrPublicKey, NodeId},
};
use futures::StreamExt;
use leani_primitives::{
    BlockHash, BlockNumber, Capability, CapabilitySet, ChainId, SourceId, SourceKind, TrustModel,
};
use leani_source_api::{
    AttestedHead, AttestedHeadPublisher, ConsensusCheckpoint, FinalityEvent, FinalityEventStream,
    FinalityModel, FinalitySource, Partitioning, SourceDescriptor, SourceError,
};
use libp2p::{
    Multiaddr, PeerId, SwarmBuilder,
    gossipsub::{self, IdentTopic, MessageAcceptance, MessageAuthenticity, MessageId},
    identify, identity,
    multiaddr::Protocol,
    noise, ping,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, yamux,
};
use op_alloy_rpc_types_engine::OpNetworkPayloadEnvelope;
use sha2::{Digest, Sha256};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// Gossip topic versions: v1 (Bedrock) through v4 (Isthmus).
const TOPIC_VERSIONS: [u8; 4] = [0, 1, 2, 3];
const MAX_GOSSIP_BYTES: usize = 10 * 1_024 * 1_024;
/// Concurrent random lookups per discovery round: few records in the shared
/// table carry the chain's `opstack` entry.
const LOOKUPS: usize = 4;
/// Peers at their connection limit close at once; ask again later.
const REDIAL_AFTER: Duration = Duration::from_secs(60);
/// Most consensus peers are full and only a few carry blocks to a new
/// node: the ones that did are kept for redials and the next run.
const KEPT_CARRIERS: usize = 32;
/// Without a signed block this long, redial the peers that carried them.
const STALL_AFTER: Duration = Duration::from_secs(10);
/// Message-id domains for payloads that do and do not decompress.
const VALID_SNAPPY_DOMAIN: [u8; 4] = [1, 0, 0, 0];
const INVALID_SNAPPY_DOMAIN: [u8; 4] = [0, 0, 0, 0];

#[derive(Debug, thiserror::Error)]
pub enum OpGossipError {
    #[error("undecodable gossip block: {0}")]
    Decode(String),
    #[error("block {number} was signed by {recovered}, not the sequencer {expected}")]
    WrongSigner {
        number: u64,
        expected: Address,
        recovered: Address,
    },
    #[error("discovery: {0}")]
    Discovery(String),
    #[error("transport: {0}")]
    Transport(String),
}

/// A block the sequencer signed, as gossip announced it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SignedBlock {
    pub number: u64,
    pub hash: B256,
    pub parent_hash: B256,
    pub timestamp: u64,
    pub state_root: B256,
    /// The message passer's storage root, in payloads since Isthmus.
    pub withdrawals_root: Option<B256>,
    pub received_at_unix_ms: u64,
}

#[must_use]
pub fn topic(chain_id: u64, version: u8) -> String {
    format!("/optimism/{chain_id}/{version}/blocks")
}

/// Decode one message of topic `version` and check that `signer` signed it:
/// the signature covers `keccak(0^32 ‖ chain id ‖ keccak(payload))`.
///
/// # Errors
///
/// Returns an error for an undecodable message or another signer.
pub fn verify_block(
    chain_id: u64,
    signer: Address,
    version: u8,
    data: &[u8],
) -> Result<SignedBlock, OpGossipError> {
    let decoded = match version {
        0 => OpNetworkPayloadEnvelope::decode_v1(data),
        1 => OpNetworkPayloadEnvelope::decode_v2(data),
        2 => OpNetworkPayloadEnvelope::decode_v3(data),
        3 => OpNetworkPayloadEnvelope::decode_v4(data),
        _ => {
            return Err(OpGossipError::Decode(format!(
                "unknown topic version {version}"
            )));
        }
    };
    let envelope = decoded.map_err(|error| OpGossipError::Decode(error.to_string()))?;
    let payload = envelope.payload.as_v1();
    let message = envelope.payload_hash.signature_message(chain_id);
    let recovered = envelope
        .signature
        .recover_address_from_prehash(&message)
        .map_err(|error| OpGossipError::Decode(error.to_string()))?;
    if recovered != signer {
        return Err(OpGossipError::WrongSigner {
            number: payload.block_number,
            expected: signer,
            recovered,
        });
    }
    let withdrawals_root = match &envelope.payload {
        op_alloy_rpc_types_engine::OpExecutionPayload::V4(payload) => {
            Some(payload.withdrawals_root)
        }
        _ => None,
    };
    Ok(SignedBlock {
        number: payload.block_number,
        hash: payload.block_hash,
        parent_hash: payload.parent_hash,
        timestamp: payload.timestamp,
        state_root: payload.state_root,
        withdrawals_root,
        received_at_unix_ms: unix_now_ms(),
    })
}

fn message_id(message: &gossipsub::Message) -> MessageId {
    let decompressed = snap::raw::decompress_len(&message.data)
        .ok()
        .filter(|length| *length <= MAX_GOSSIP_BYTES)
        .and_then(|_| snap::raw::Decoder::new().decompress_vec(&message.data).ok());
    let mut hasher = Sha256::new();
    if let Some(decompressed) = decompressed {
        hasher.update(VALID_SNAPPY_DOMAIN);
        hasher.update(decompressed);
    } else {
        hasher.update(INVALID_SNAPPY_DOMAIN);
        hasher.update(&message.data);
    }
    MessageId::from(hasher.finalize()[..20].to_vec())
}

/// The chain id an `opstack` record entry names: an RLP string holding the
/// chain id and version as unsigned varints.
#[allow(deprecated)]
fn enr_op_stack_chain(enr: &Enr) -> Option<u64> {
    let raw = enr.get("opstack")?;
    // Record values come back RLP-encoded from some ENR versions and as the
    // bare string from others.
    let bytes = alloy_rlp::Header::decode_bytes(&mut &raw[..], false).unwrap_or(&raw[..]);
    let mut chain_id = 0_u64;
    for (index, byte) in bytes.iter().enumerate().take(10) {
        chain_id |= u64::from(byte & 0x7f) << (7 * index);
        if byte & 0x80 == 0 {
            return Some(chain_id);
        }
    }
    None
}

/// The discovery key of the libp2p identity: peers derive the peer id they
/// dial from the record's key.
fn discovery_key(keypair: &identity::Keypair) -> Result<CombinedKey, OpGossipError> {
    let mut secret = keypair
        .clone()
        .try_into_secp256k1()
        .map_err(|error| OpGossipError::Discovery(error.to_string()))?
        .secret()
        .to_bytes();
    CombinedKey::secp256k1_from_bytes(&mut secret)
        .map_err(|error| OpGossipError::Discovery(error.to_string()))
}

/// The record peers find us by: the chain's `opstack` entry and the ports.
/// Discovery fills in the external address once peers agree on it.
fn local_record(key: &CombinedKey, config: &OpGossipConfig) -> Result<Enr, OpGossipError> {
    let mut chain = Vec::new();
    for mut value in [config.chain_id, 0] {
        loop {
            let byte = u8::try_from(value & 0x7f).unwrap_or_default();
            value >>= 7;
            if value == 0 {
                chain.push(byte);
                break;
            }
            chain.push(byte | 0x80);
        }
    }
    let mut builder = Enr::builder();
    builder.add_value("opstack", &alloy_primitives::Bytes::from(chain));
    if config.listen_port != 0 {
        builder.tcp4(config.listen_port);
    }
    if config.discovery_port != 0 {
        builder.udp4(config.discovery_port);
    }
    builder
        .build(key)
        .map_err(|error| OpGossipError::Discovery(error.to_string()))
}

fn peer_address(enr: &Enr) -> Option<(PeerId, Multiaddr)> {
    let CombinedPublicKey::Secp256k1(key) = enr.public_key() else {
        return None;
    };
    let key = identity::secp256k1::PublicKey::try_from_bytes(&key.encode()).ok()?;
    let peer_id = PeerId::from_public_key(&identity::PublicKey::from(key));
    let mut address = Multiaddr::empty();
    address.push(Protocol::Ip4(enr.ip4()?));
    address.push(Protocol::Tcp(enr.tcp4()?));
    Some((peer_id, address))
}

/// Peers drop connections that answer neither identify nor ping.
#[derive(NetworkBehaviour)]
struct Behaviour {
    gossipsub: gossipsub::Behaviour,
    identify: identify::Behaviour,
    ping: ping::Behaviour,
}

#[derive(Clone, Debug)]
pub struct OpGossipConfig {
    pub chain_id: u64,
    pub signer: Address,
    /// Discv5 records of consensus peers to start discovery from.
    pub bootnodes: Vec<String>,
    pub discovery_port: u16,
    pub listen_port: u16,
    pub maximum_peers: usize,
    pub rediscovery_interval: Duration,
    /// Where the peers that carried signed blocks are kept across runs.
    pub peers_path: Option<PathBuf>,
}

impl OpGossipConfig {
    /// The gossip of an op-stack chain entry.
    ///
    /// # Errors
    ///
    /// Returns an error for a chain without an op-stack schedule.
    pub fn for_chain(chain: &leani_chain::Chain) -> Result<Self, OpGossipError> {
        let op_stack = chain.op_stack.as_ref().ok_or_else(|| {
            OpGossipError::Discovery(format!("{} is not an op-stack chain", chain.name))
        })?;
        Ok(Self {
            chain_id: chain.chain_id,
            signer: op_stack.sequencer_signer,
            bootnodes: op_stack.consensus_bootnodes.clone(),
            discovery_port: 0,
            listen_port: 0,
            maximum_peers: 16,
            rediscovery_interval: Duration::from_secs(5),
            peers_path: None,
        })
    }
}

/// Peers whose forwarded blocks the sequencer signed, newest first, as
/// `/p2p/`-terminated listen addresses: one per line on disk.
#[derive(Debug, Default)]
struct Carriers(Vec<(PeerId, Multiaddr)>);

impl Carriers {
    fn parse(text: &str) -> Self {
        let peers = text
            .lines()
            .filter_map(|line| {
                let address = line.trim().parse::<Multiaddr>().ok()?;
                let peer = address.iter().find_map(|part| match part {
                    Protocol::P2p(peer) => Some(peer),
                    _ => None,
                })?;
                Some((peer, address))
            })
            .take(KEPT_CARRIERS)
            .collect();
        Self(peers)
    }

    fn render(&self) -> String {
        self.0
            .iter()
            .map(|(_, address)| address.to_string() + "\n")
            .collect()
    }

    /// Make `peer` the newest carrier; whether the kept set changed.
    fn record(&mut self, peer: PeerId, address: Multiaddr) -> bool {
        let known = self.0.iter().position(|(kept, _)| *kept == peer);
        let changed = known.is_none_or(|index| self.0[index].1 != address);
        if let Some(index) = known {
            self.0.remove(index);
        }
        self.0.insert(0, (peer, address));
        self.0.truncate(KEPT_CARRIERS);
        changed
    }
}

/// A public IPv4 TCP address among the ones a peer listens on, ending in
/// its peer id: what another run can dial.
fn dialable(peer: PeerId, listen: &[Multiaddr]) -> Option<Multiaddr> {
    listen
        .iter()
        .find(|address| {
            let mut parts = address.iter();
            matches!(
                (parts.next(), parts.next()),
                (Some(Protocol::Ip4(ip)), Some(Protocol::Tcp(_)))
                    if !(ip.is_private() || ip.is_loopback() || ip.is_link_local() || ip.is_unspecified())
            )
        })
        .map(|address| address.clone().with(Protocol::P2p(peer)))
}

/// Follow the chain's block gossip until `cancellation`. Every block the
/// sequencer signed arrives on the returned channel, in gossip order.
///
/// # Errors
///
/// Returns an error when discovery or the transport cannot start.
#[allow(clippy::too_many_lines)]
pub async fn follow(
    config: OpGossipConfig,
    cancellation: CancellationToken,
) -> Result<mpsc::Receiver<SignedBlock>, OpGossipError> {
    let keypair = identity::Keypair::generate_secp256k1();
    let key = discovery_key(&keypair)?;
    let local_enr = local_record(&key, &config)?;
    let listen = ListenConfig::from_ip(Ipv4Addr::UNSPECIFIED.into(), config.discovery_port);
    let mut discovery = Discv5::new(local_enr, key, ConfigBuilder::new(listen).build())
        .map_err(|error| OpGossipError::Discovery(error.to_string()))?;
    for encoded in &config.bootnodes {
        let enr = encoded
            .parse::<Enr>()
            .map_err(|error| OpGossipError::Discovery(error.clone()))?;
        discovery
            .add_enr(enr)
            .map_err(|error| OpGossipError::Discovery((*error).to_owned()))?;
    }
    discovery
        .start()
        .await
        .map_err(|error| OpGossipError::Discovery(error.to_string()))?;

    let gossip_config = gossipsub::ConfigBuilder::default()
        .validation_mode(gossipsub::ValidationMode::None)
        .validate_messages()
        .max_transmit_size(MAX_GOSSIP_BYTES)
        .message_id_fn(message_id)
        .build()
        .map_err(|error| OpGossipError::Transport(error.to_string()))?;
    let gossipsub: gossipsub::Behaviour =
        gossipsub::Behaviour::new(MessageAuthenticity::Anonymous, gossip_config)
            .map_err(|error| OpGossipError::Transport(error.to_owned()))?;
    let behaviour = Behaviour {
        gossipsub,
        identify: identify::Behaviour::new(identify::Config::new(String::new(), keypair.public())),
        ping: ping::Behaviour::default(),
    };
    let mut swarm = SwarmBuilder::with_existing_identity(keypair)
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )
        .map_err(|error| OpGossipError::Transport(error.to_string()))?
        .with_behaviour(|_| behaviour)
        .map_err(|error| OpGossipError::Transport(error.to_string()))?
        .with_swarm_config(|swarm| swarm.with_idle_connection_timeout(Duration::from_secs(120)))
        .build();
    let mut versions = Vec::new();
    for version in TOPIC_VERSIONS {
        let topic = IdentTopic::new(topic(config.chain_id, version));
        swarm
            .behaviour_mut()
            .gossipsub
            .subscribe(&topic)
            .map_err(|error| OpGossipError::Transport(error.to_string()))?;
        versions.push((topic.hash(), version));
    }
    let listen: Multiaddr = format!("/ip4/0.0.0.0/tcp/{}", config.listen_port)
        .parse()
        .map_err(|error: libp2p::multiaddr::Error| OpGossipError::Transport(error.to_string()))?;
    swarm
        .listen_on(listen)
        .map_err(|error| OpGossipError::Transport(error.to_string()))?;

    let (blocks, received) = mpsc::channel(256);
    let (found, mut candidates) = mpsc::channel(64);
    let discovery_cancellation = cancellation.clone();
    let rediscovery_interval = config.rediscovery_interval;
    let chain_id = config.chain_id;
    // A lookup takes seconds; the swarm must keep running meanwhile.
    tokio::spawn(async move {
        let mut rediscover = tokio::time::interval(rediscovery_interval);
        let round = async {
            loop {
                rediscover.tick().await;
                let lookups = (0..LOOKUPS).map(|_| discovery.find_node(NodeId::random()));
                let records = futures::future::join_all(lookups)
                    .await
                    .into_iter()
                    .flat_map(Result::unwrap_or_default)
                    .chain(discovery.table_entries_enr())
                    .filter(|enr| enr_op_stack_chain(enr) == Some(chain_id))
                    .filter_map(|enr| peer_address(&enr))
                    .collect::<Vec<_>>();
                debug!(candidates = records.len(), "op-stack gossip discovery");
                for record in records {
                    if found.send(record).await.is_err() {
                        return;
                    }
                }
            }
        };
        tokio::select! {
            () = discovery_cancellation.cancelled() => {}
            () = round => {}
        }
        // Frees the UDP port for a restarted lane.
        discovery.shutdown();
    });
    tokio::spawn(async move {
        let mut dialed = HashMap::<PeerId, Instant>::new();
        let mut connected = HashSet::new();
        let mut joined = false;
        let mut carriers = config
            .peers_path
            .as_deref()
            .and_then(|path| std::fs::read_to_string(path).ok())
            .map_or_else(Carriers::default, |text| Carriers::parse(&text));
        if !carriers.0.is_empty() {
            info!(
                peers = carriers.0.len(),
                "dialing the gossip peers that carried signed blocks before"
            );
            for (_, address) in &carriers.0 {
                if let Err(error) = swarm.dial(address.clone()) {
                    debug!(%error, "could not dial op-stack gossip peer");
                }
            }
        }
        let mut listening = HashMap::<PeerId, Multiaddr>::new();
        let mut last_block = Instant::now();
        let mut stalled = false;
        let mut stall_check = tokio::time::interval(STALL_AFTER);
        loop {
            tokio::select! {
                () = cancellation.cancelled() => return,
                _ = stall_check.tick() => {
                    if last_block.elapsed() < STALL_AFTER {
                        continue;
                    }
                    let idle: Vec<_> = carriers
                        .0
                        .iter()
                        .filter(|(peer, _)| !connected.contains(peer))
                        .map(|(_, address)| address.clone())
                        .collect();
                    if !idle.is_empty() && !std::mem::replace(&mut stalled, true) {
                        info!(
                            peers = idle.len(),
                            "no signed block for {STALL_AFTER:?}; redialing the gossip peers that carried them"
                        );
                    }
                    for address in idle {
                        if let Err(error) = swarm.dial(address) {
                            debug!(%error, "could not dial op-stack gossip peer");
                        }
                    }
                }
                Some((peer_id, address)) = candidates.recv() => {
                    let due = dialed
                        .get(&peer_id)
                        .is_none_or(|at| at.elapsed() >= REDIAL_AFTER);
                    if connected.len() < config.maximum_peers && !connected.contains(&peer_id) && due {
                        dialed.insert(peer_id, Instant::now());
                        let address = address.with(Protocol::P2p(peer_id));
                        if let Err(error) = swarm.dial(address) {
                            debug!(%error, "could not dial op-stack gossip peer");
                        }
                    }
                }
                event = swarm.select_next_some() => match event {
                    SwarmEvent::ConnectionEstablished { peer_id, endpoint, .. } => {
                        debug!(%peer_id, address = %endpoint.get_remote_address(), "op-stack gossip connection established");
                        if connected.insert(peer_id) && !joined {
                            joined = true;
                            info!(chain_id = config.chain_id, "joined op-stack block gossip");
                        }
                    }
                    SwarmEvent::ConnectionClosed { peer_id, num_established, cause, endpoint, .. } => {
                        debug!(%peer_id, ?cause, dialer = endpoint.is_dialer(), "op-stack gossip connection closed");
                        if num_established == 0 {
                            connected.remove(&peer_id);
                            listening.remove(&peer_id);
                        }
                    }
                    SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                        debug!(?peer_id, %error, "op-stack gossip dial failed");
                    }
                    SwarmEvent::Behaviour(BehaviourEvent::Identify(identify::Event::Received { peer_id, info, .. })) => {
                        debug!(%peer_id, agent = info.agent_version, protocol = info.protocol_version, protocols = ?info.protocols, "op-stack gossip peer identified");
                        if let Some(address) = dialable(peer_id, &info.listen_addrs) {
                            listening.insert(peer_id, address);
                        }
                    }
                    SwarmEvent::Behaviour(BehaviourEvent::Gossipsub(gossipsub::Event::Subscribed { peer_id, topic })) => {
                        debug!(%peer_id, %topic, "op-stack gossip peer subscribed");
                    }
                    SwarmEvent::Behaviour(BehaviourEvent::Gossipsub(gossipsub::Event::Message {
                        propagation_source,
                        message_id,
                        message,
                    })) => {
                        let Some(version) = versions
                            .iter()
                            .find(|(hash, _)| *hash == message.topic)
                            .map(|(_, version)| *version)
                        else {
                            continue;
                        };
                        let acceptance = match verify_block(
                            config.chain_id,
                            config.signer,
                            version,
                            &message.data,
                        ) {
                            Ok(block) => {
                                last_block = Instant::now();
                                if std::mem::replace(&mut stalled, false) {
                                    info!("signed blocks arrive again");
                                }
                                if let Some(address) = listening.get(&propagation_source)
                                    && carriers.record(propagation_source, address.clone())
                                    && let Some(path) = &config.peers_path
                                    && let Err(error) = std::fs::write(path, carriers.render())
                                {
                                    warn!(%error, "could not keep the gossip peers that carry blocks");
                                }
                                if blocks.send(block).await.is_err() {
                                    return;
                                }
                                MessageAcceptance::Accept
                            }
                            Err(error @ OpGossipError::WrongSigner { .. }) => {
                                warn!(%error, "op-stack gossip block is not the sequencer's");
                                MessageAcceptance::Reject
                            }
                            Err(error) => {
                                debug!(%error, "dropping op-stack gossip message");
                                MessageAcceptance::Reject
                            }
                        };
                        swarm.behaviour_mut().gossipsub.report_message_validation_result(
                            &message_id,
                            &propagation_source,
                            acceptance,
                        );
                    }
                    _ => {}
                },
            }
        }
    });
    Ok(received)
}

/// Publish every signed block as the live lane's attested head, and keep the
/// newest one for whoever needs a start. The head's slot is its number.
/// `every_block` also receives each one.
///
/// # Errors
///
/// Returns an error when the gossip cannot start.
pub async fn publish_heads(
    config: OpGossipConfig,
    heads: AttestedHeadPublisher,
    every_block: Option<mpsc::Sender<SignedBlock>>,
    cancellation: CancellationToken,
) -> Result<watch::Receiver<Option<SignedBlock>>, OpGossipError> {
    let mut blocks = follow(config, cancellation.clone()).await?;
    let (latest, receiver) = watch::channel(None);
    tokio::spawn(async move {
        loop {
            let block = tokio::select! {
                () = cancellation.cancelled() => return,
                block = blocks.recv() => block,
            };
            let Some(block) = block else { return };
            if let Some(every_block) = &every_block
                && every_block.try_send(block).is_err()
            {
                warn!(
                    block = block.number,
                    "a signed block skipped the L1 anchor's ledger"
                );
            }
            heads.publish(AttestedHead {
                beacon_slot: block.number,
                beacon_block_root: [0; 32],
                block_number: BlockNumber(block.number),
                block_hash: BlockHash::new(block.hash.0),
            });
            latest.send_if_modified(|current: &mut Option<SignedBlock>| {
                let newer = current.is_none_or(|current| current.number < block.number);
                if newer {
                    *current = Some(block);
                }
                newer
            });
        }
    });
    Ok(receiver)
}

/// Sequencer-signed heads as the finality lane of an op-stack chain. Heads
/// are optimistic; only the L1 anchor's blocks, when it runs, are finalized.
#[derive(Debug)]
pub struct OpGossipFinality {
    descriptor: SourceDescriptor,
    heads: watch::Receiver<Option<SignedBlock>>,
    finalized: Option<watch::Receiver<Option<(BlockNumber, BlockHash)>>>,
}

impl OpGossipFinality {
    /// # Errors
    ///
    /// Returns an error for an invalid source id.
    pub fn new(
        chain_id: ChainId,
        heads: watch::Receiver<Option<SignedBlock>>,
        finalized: Option<watch::Receiver<Option<(BlockNumber, BlockHash)>>>,
    ) -> Result<Self, SourceError> {
        Ok(Self {
            descriptor: SourceDescriptor {
                id: SourceId::new("op-gossip")
                    .map_err(|error| SourceError::InvalidPlan(error.to_string()))?,
                kind: SourceKind::SequencerGossip,
                chain_id,
                range: None,
                capabilities: CapabilitySet::of(Capability::ConsensusFinality),
                complete_capabilities: CapabilitySet::of(Capability::ConsensusFinality),
                trust: TrustModel::ProtocolVerified,
                finality: FinalityModel::Attested,
                partitioning: Partitioning::None,
                expected_lag: Duration::from_secs(2),
                schema_version: "op-gossip.v1".to_owned(),
                priority: 0,
            },
            heads,
            finalized,
        })
    }
}

#[async_trait::async_trait]
impl FinalitySource for OpGossipFinality {
    fn descriptor(&self) -> &SourceDescriptor {
        &self.descriptor
    }

    async fn subscribe(
        &self,
        _checkpoint: ConsensusCheckpoint,
        cancellation: CancellationToken,
    ) -> Result<FinalityEventStream, SourceError> {
        let heads = self.heads.clone();
        let finalized = self.finalized.clone().map(|mut finalized| {
            finalized.mark_changed();
            finalized
        });
        Ok(futures::stream::unfold(
            (heads, finalized, cancellation),
            |(mut heads, mut finalized, cancellation)| async move {
                loop {
                    let anchored = async {
                        match finalized.as_mut() {
                            Some(finalized) => finalized.changed().await.is_ok(),
                            None => std::future::pending().await,
                        }
                    };
                    tokio::select! {
                        () = cancellation.cancelled() => return None,
                        changed = heads.changed() => {
                            changed.ok()?;
                            let Some(block) = *heads.borrow_and_update() else { continue };
                            let event = FinalityEvent::Optimistic {
                                block_hash: BlockHash::new(block.hash.0),
                                beacon_slot: block.number,
                            };
                            return Some((Ok(event), (heads, finalized, cancellation)));
                        }
                        open = anchored => {
                            if !open {
                                return Some((
                                    Err(SourceError::Disconnected("the L1 anchor ended".to_owned())),
                                    (heads, None, cancellation),
                                ));
                            }
                            let latest = finalized.as_mut().and_then(|finalized| *finalized.borrow_and_update());
                            if let Some((number, hash)) = latest {
                                let event = FinalityEvent::Finalized {
                                    block_number: number,
                                    block_hash: hash,
                                    beacon_slot: number.0,
                                    beacon_block_root: [0; 32],
                                };
                                return Some((Ok(event), (heads, finalized, cancellation)));
                            }
                        }
                    }
                }
            },
        )
        .boxed())
    }
}

fn unix_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn op_stack_records_name_their_chain() {
        // Kona's Base mainnet consensus bootnode: `opstack` = varint 8453, version 0.
        let base: Enr = "enr:-J24QNz9lbrKbN4iSmmjtnr7SjUMk4zB7f1krHZcTZx-JRKZd0kA2gjufUROD6T3sOWDVDnFJRvqBBo62zuF-hYCohOGAYiOoEyEgmlkgnY0gmlwhAPniryHb3BzdGFja4OFQgCJc2VjcDI1NmsxoQKNVFlCxh_B-716tTs-h1vMzZkSs1FTu_OYTNjgufplG4N0Y3CCJAaDdWRwgiQG"
            .parse()
            .expect("valid record");
        assert_eq!(enr_op_stack_chain(&base), Some(8453));
        assert!(peer_address(&base).is_some());
    }

    #[test]
    fn our_record_names_the_chain_and_the_dialed_identity() {
        let keypair = identity::Keypair::generate_secp256k1();
        let key = discovery_key(&keypair).expect("secp256k1 key");
        let base = leani_chain::ChainRegistry::standard()
            .get("base-mainnet")
            .expect("base entry");
        let mut config = OpGossipConfig::for_chain(&base).expect("op-stack entry");
        config.listen_port = 9222;
        let mut record = local_record(&key, &config).expect("record");
        record
            .set_ip("1.2.3.4".parse().expect("ip"), &key)
            .expect("ip");
        assert_eq!(enr_op_stack_chain(&record), Some(8453));
        let (peer_id, address) = peer_address(&record).expect("dialable");
        assert_eq!(peer_id, keypair.public().to_peer_id());
        assert_eq!(address.to_string(), "/ip4/1.2.3.4/tcp/9222");
    }

    #[test]
    fn topics_follow_the_versioned_layout() {
        assert_eq!(topic(8453, 3), "/optimism/8453/3/blocks");
    }

    #[test]
    fn carriers_keep_public_listen_addresses_newest_first() {
        let peer = || {
            identity::Keypair::generate_secp256k1()
                .public()
                .to_peer_id()
        };
        let (first, second) = (peer(), peer());
        let listen = |text: &str| text.parse::<Multiaddr>().expect("address");
        assert_eq!(
            dialable(first, &[listen("/ip4/10.0.0.7/tcp/9222")]),
            None,
            "private"
        );
        let address = dialable(
            first,
            &[
                listen("/ip4/127.0.0.1/tcp/9222"),
                listen("/ip4/5.6.7.8/tcp/9222"),
            ],
        )
        .expect("public");
        assert_eq!(
            address.to_string(),
            format!("/ip4/5.6.7.8/tcp/9222/p2p/{first}")
        );

        let mut carriers = Carriers::default();
        assert!(carriers.record(first, address.clone()));
        let other = dialable(second, &[listen("/ip4/9.9.9.9/tcp/9003")]).expect("public");
        assert!(carriers.record(second, other));
        assert!(
            !carriers.record(first, address),
            "known at the same address"
        );
        let kept = Carriers::parse(&carriers.render());
        let peers: Vec<_> = kept.0.iter().map(|(peer, _)| *peer).collect();
        assert_eq!(peers, [first, second]);

        for _ in 0..KEPT_CARRIERS {
            let newer = peer();
            carriers.record(
                newer,
                dialable(newer, &[listen("/ip4/1.1.1.1/tcp/9222")]).expect("public"),
            );
        }
        assert_eq!(carriers.0.len(), KEPT_CARRIERS);
        assert!(
            carriers.0.iter().all(|(kept, _)| *kept != second),
            "the oldest leave first"
        );
    }
}
