//! Sequencer-signed unsafe blocks from an op-stack chain's consensus gossip.
//! A block the chain's sequencer key signed is `attested` there, with no L1
//! access. Peers are found over Discv5 by their `opstack` record entry;
//! blocks arrive on the `/optimism/<chain id>/<version>/blocks` topics.

use std::{
    collections::HashSet,
    net::Ipv4Addr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use alloy_primitives::{Address, B256};
use discv5::{
    ConfigBuilder, Discv5, Enr, ListenConfig,
    enr::{CombinedKey, CombinedPublicKey, EnrPublicKey, NodeId},
};
use futures::StreamExt;
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
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// Gossip topic versions: v1 (Bedrock) through v4 (Isthmus).
const TOPIC_VERSIONS: [u8; 4] = [0, 1, 2, 3];
const MAX_GOSSIP_BYTES: usize = 10 * 1_024 * 1_024;
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
    Ok(SignedBlock {
        number: payload.block_number,
        hash: payload.block_hash,
        parent_hash: payload.parent_hash,
        timestamp: payload.timestamp,
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
        })
    }
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
    let key = CombinedKey::generate_secp256k1();
    let local_enr =
        Enr::empty(&key).map_err(|error| OpGossipError::Discovery(error.to_string()))?;
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
    let keypair = identity::Keypair::generate_secp256k1();
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
    tokio::spawn(async move {
        let mut dialed = HashSet::new();
        let mut connected = HashSet::new();
        let mut rediscover = tokio::time::interval(config.rediscovery_interval);
        loop {
            tokio::select! {
                () = cancellation.cancelled() => return,
                _ = rediscover.tick(), if connected.len() < config.maximum_peers => {
                    let found = discovery.find_node(NodeId::random()).await.unwrap_or_default();
                    let records = found
                        .into_iter()
                        .chain(discovery.table_entries_enr())
                        .collect::<Vec<_>>();
                    let mut chains = std::collections::BTreeMap::<Option<u64>, usize>::new();
                    for enr in &records {
                        *chains.entry(enr_op_stack_chain(enr)).or_default() += 1;
                    }
                    debug!(records = records.len(), ?chains, "op-stack gossip discovery records");
                    let candidates = records
                        .into_iter()
                        .filter(|enr| enr_op_stack_chain(enr) == Some(config.chain_id))
                        .filter_map(|enr| peer_address(&enr))
                        .collect::<Vec<_>>();
                    debug!(candidates = candidates.len(), connected = connected.len(), "op-stack gossip discovery");
                    for (peer_id, address) in candidates {
                        if dialed.insert(peer_id) {
                            let address = address.with(Protocol::P2p(peer_id));
                            if let Err(error) = swarm.dial(address) {
                                debug!(%error, "could not dial op-stack gossip peer");
                            }
                        }
                    }
                }
                event = swarm.select_next_some() => match event {
                    SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                        if connected.insert(peer_id) && connected.len() == 1 {
                            info!(chain_id = config.chain_id, "joined op-stack block gossip");
                        }
                    }
                    SwarmEvent::ConnectionClosed { peer_id, num_established: 0, .. } => {
                        connected.remove(&peer_id);
                        dialed.remove(&peer_id);
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
    fn topics_follow_the_versioned_layout() {
        assert_eq!(topic(8453, 3), "/optimism/8453/3/blocks");
    }
}
