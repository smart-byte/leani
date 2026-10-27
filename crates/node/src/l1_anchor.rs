//! The opt-in L1 anchor of a rollup node: Ethereum's verified finality and
//! execution P2P read the chain entry's settlement contracts.

use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use futures::StreamExt;
use leani_chain::L1Anchor;
use leani_l1_anchor::{
    AnchorLane, FinalizedBlock, FinalizedBlocks, FinalizedTip, GameLedger, Observation,
};
use leani_primitives::{
    Address, BlockFrame, BlockHash, BlockNumber, BlockRange, BlockRef, Capability, CapabilitySet,
    ChainId, ConsensusAnchor, Finality, LogFieldSet,
};
use leani_source_api::{
    ConsensusCheckpoint, DataRequest, FieldProjection, FilterSet, FinalitySource, HistorySource,
    SourceBudget, SourceError, VerificationPolicy,
};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::config::{Config, FinalitySourceKind, L1AnchorConfig};

/// Most bytes one finalized L1 block may cost: its receipts and, for
/// dispute games, its body.
const BYTES_PER_L1_BLOCK: u64 = 4 * 1_024 * 1_024;

/// Finalized Ethereum blocks over execution P2P, hash-linked down from a
/// verified finalized tip and checked against their roots.
struct P2pFinalizedBlocks {
    source: leani_source_p2p::RethP2pSource,
    request: DataRequest,
}

#[async_trait::async_trait]
impl FinalizedBlocks for P2pFinalizedBlocks {
    async fn frames(
        &self,
        range: BlockRange,
        tip: FinalizedTip,
        cancellation: &CancellationToken,
    ) -> Result<Vec<BlockFrame>, SourceError> {
        let anchor = leani_source_p2p::P2pHistoryAnchor {
            block: BlockRef {
                number: BlockNumber(tip.number),
                hash: tip.hash,
                parent_hash: BlockHash::ZERO,
                timestamp: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
            },
            consensus: ConsensusAnchor {
                finality: Finality::Finalized,
                execution_block_hash: tip.hash,
                beacon_slot: tip.beacon_slot,
                beacon_block_root: tip.beacon_block_root,
            },
        };
        let history = leani_source_p2p::RethP2pHistorySource::from_persistent_source(
            self.source.clone(),
            range,
            anchor,
        )?;
        let mut request = self.request.clone();
        request.range = range;
        let plan = history.plan(&request).await?;
        let input_bytes = range.len().saturating_mul(BYTES_PER_L1_BLOCK);
        let mut frames = Vec::new();
        for chunk in &plan.chunks {
            let budget = SourceBudget {
                max_input_bytes: input_bytes,
                max_frame_bytes: BYTES_PER_L1_BLOCK,
                max_frames: chunk.range.len(),
                max_buffered_frames: 64,
                max_in_flight_requests: 4,
                temporary_disk_bytes: 1,
                max_resident_bytes: input_bytes,
            };
            let mut stream = history.open(chunk, budget, cancellation.clone()).await?;
            while let Some(frame) = stream.next().await {
                frames.push(frame?);
            }
        }
        frames.sort_by_key(|frame| frame.block.number);
        Ok(frames)
    }
}

/// How long a failed L1 lane waits before it starts again.
const L1_RESTART_DELAY: Duration = Duration::from_secs(30);

/// Start the L1 lane when the node configures one. Its observations arrive
/// in L1 order across its own restarts, which resume where it stopped.
pub(crate) fn spawn(
    config: &Config,
    cancellation: &CancellationToken,
) -> Result<Option<(L1Anchor, mpsc::Receiver<Observation>)>> {
    if config.l1_anchor.is_none() {
        return Ok(None);
    }
    let anchor = config
        .chain_entry()?
        .l1_anchor
        .context("the chain entry names no settlement contracts")?;
    let (sender, observations) = mpsc::channel(256);
    let config = config.clone();
    let cancellation = cancellation.child_token();
    tokio::spawn(async move {
        while !cancellation.is_cancelled() {
            let outcome = match lane(&config, anchor).await {
                Ok(lane) => lane
                    .run(sender.clone(), cancellation.clone())
                    .await
                    .map_err(Into::into),
                Err(error) => Err(error),
            };
            if let Err(error) = outcome {
                warn!(error = %format!("{error:#}"), delay = ?L1_RESTART_DELAY, "the L1 anchor lane failed; restarting");
            }
            tokio::select! {
                () = cancellation.cancelled() => return,
                () = tokio::time::sleep(L1_RESTART_DELAY) => {}
            }
        }
    });
    Ok(Some((anchor, observations)))
}

async fn lane(config: &Config, anchor: L1Anchor) -> Result<AnchorLane<P2pFinalizedBlocks>> {
    let l1 = config
        .l1_anchor
        .as_ref()
        .context("the node configures no L1 anchor")?;
    let ethereum = leani_chain::ChainRegistry::standard().get("ethereum-mainnet")?;
    let source = leani_source_p2p::RethP2pSource::new(
        ethereum.clone(),
        leani_source_p2p::RethP2pConfig {
            listener_port: l1.listener_port,
            discovery_port: l1.discovery_port,
            discv5_port: l1.discv5_port,
            nat: leani_source_p2p::parse_nat_resolver(&l1.nat)?,
            peer_store_path: Some(config.data_dir.join("l1-execution-network.sqlite")),
            secret_key_path: Some(config.data_dir.join("l1-execution-p2p-secret")),
            ..leani_source_p2p::RethP2pConfig::default()
        },
    )?;
    let (finality, checkpoint) = l1_finality(config, l1, ethereum, &source).await?;
    let mut filters = FilterSet::default();
    filters.scope.addresses = leani_l1_anchor::log_addresses(&anchor)
        .into_iter()
        .map(Address::from)
        .collect();
    let mut required = CapabilitySet::of(Capability::Logs);
    if let Some(recipient) = leani_l1_anchor::calldata_recipient(&anchor) {
        filters.recipients = vec![Address::from(recipient)];
        required = required.with(Capability::Calldata);
    }
    let request = DataRequest {
        chain_id: ChainId(1),
        range: BlockRange::new(BlockNumber(0), BlockNumber(0))?,
        required,
        log_fields: LogFieldSet::NONE,
        allow_filtered: true,
        projection: FieldProjection::default(),
        filters,
        minimum_finality: Finality::Finalized,
        verification_policy: VerificationPolicy::CompleteCryptographic,
    };
    Ok(AnchorLane {
        anchor,
        finality,
        checkpoint,
        blocks: P2pFinalizedBlocks { source, request },
        cursor_path: config.data_dir.join("l1-anchor-cursor.json"),
        lookback: l1.lookback_blocks,
    })
}

/// An op-stack chain's signed heads, and the blocks its L1 anchor
/// finalizes. They outlive restarts of the lanes that follow them.
#[derive(Clone, Debug)]
pub(crate) struct OpHeads {
    pub(crate) heads: watch::Receiver<Option<leani_finality_op_gossip::SignedBlock>>,
    pub(crate) finalized: Option<watch::Receiver<FinalizedBlock>>,
}

/// Join the chain's sequencer gossip, publishing each signed block as the
/// attested head, and start the L1 anchor when the node configures one.
pub(crate) async fn start_op_heads(
    config: &Config,
    attested_heads: leani_source_api::AttestedHeadPublisher,
    cancellation: &CancellationToken,
) -> Result<OpHeads> {
    let chain = config.chain_entry()?;
    // The slot length once every scheduled fork is active.
    let fastest_block_ms = chain
        .op_stack
        .as_ref()
        .context("op_gossip finality needs an op-stack chain")?
        .cadence()
        .slot_ms(u64::MAX);
    let mut gossip = leani_finality_op_gossip::OpGossipConfig::for_chain(&chain)?;
    gossip.listen_port = config.finality.listen_port;
    gossip.discovery_port = config.finality.discovery_port;
    gossip.peers_path = Some(config.data_dir.join("op-gossip-peers.txt"));
    if !config.finality.bootnodes.is_empty() {
        gossip.bootnodes.clone_from(&config.finality.bootnodes);
    }
    let anchor = spawn(config, cancellation)?;
    let (every_block, signed) = match anchor {
        Some((L1Anchor::DisputeGames { .. }, _)) => {
            let (sender, receiver) = mpsc::channel(4_096);
            (Some(sender), Some(receiver))
        }
        _ => (None, None),
    };
    let heads = leani_finality_op_gossip::publish_heads(
        gossip,
        attested_heads,
        every_block,
        cancellation.child_token(),
    )
    .await?;
    let finalized = match (anchor, signed) {
        (Some((_, observations)), Some(signed)) => Some(follow_dispute_games(
            config,
            fastest_block_ms,
            signed,
            observations,
            cancellation.child_token(),
        )?),
        _ => None,
    };
    Ok(OpHeads { heads, finalized })
}

/// The L1's verified finality. `headers` numbers the blocks Gloas headers
/// name by hash.
async fn l1_finality(
    config: &Config,
    l1: &L1AnchorConfig,
    ethereum: Arc<leani_chain::Chain>,
    headers: &leani_source_p2p::RethP2pSource,
) -> Result<(Arc<dyn FinalitySource>, ConsensusCheckpoint)> {
    use leani_finality_beacon_api::{
        AnchorFile, BeaconApiConfig, CheckpointOrigin, TrustedCheckpoint, VerifiedBeaconApi,
        parse_checkpoint_root,
    };

    let root = parse_checkpoint_root(&l1.finality.checkpoint)?;
    let trusted = TrustedCheckpoint {
        root,
        slot: (l1.finality.checkpoint_slot > 0).then_some(l1.finality.checkpoint_slot),
        origin: CheckpointOrigin::Operator,
    };
    // Its own file: the rollup's finality never writes beacon anchors.
    let anchor = AnchorFile::ReadWrite {
        path: config.data_dir.join("l1-anchor-finality.json"),
        write_failures: Arc::default(),
    };
    let (source, bootstrap): (Arc<dyn FinalitySource>, _) = match l1.finality.kind {
        FinalitySourceKind::BeaconApi => {
            let mut beacon = BeaconApiConfig::new(ethereum, l1.finality.endpoints.clone());
            beacon.minimum_agreement = l1.finality.minimum_agreement;
            beacon.anchor = anchor;
            let source =
                VerifiedBeaconApi::new(beacon)?.with_execution_headers(Arc::new(headers.clone()));
            let probe = source.probe_root(trusted).await;
            if !probe.accepted {
                bail!(
                    "the L1 anchor's Beacon API finality was not accepted: {}",
                    probe.disagreements.join("; ")
                );
            }
            (
                Arc::new(source),
                probe
                    .checkpoint_anchor
                    .context("accepted finality report omitted its checkpoint anchor")?,
            )
        }
        FinalitySourceKind::ConsensusP2p => {
            let mut p2p = crate::process::consensus_p2p_config(ethereum, &l1.finality);
            p2p.anchor = anchor;
            let source = leani_finality_consensus_p2p::VerifiedConsensusP2p::new(p2p)?
                .with_execution_headers(Arc::new(headers.clone()));
            let probe = source.probe_checkpoint(trusted).await;
            if !probe.accepted {
                bail!(
                    "the L1 anchor's consensus P2P finality was not accepted: {}",
                    probe.errors.join("; ")
                );
            }
            (
                Arc::new(source),
                probe
                    .checkpoint_anchor
                    .context("accepted P2P report omitted its checkpoint anchor")?,
            )
        }
        FinalitySourceKind::JsonRpc
        | FinalitySourceKind::OpGossip
        | FinalitySourceKind::SequencerFeed
        | FinalitySourceKind::Disabled => {
            bail!("the L1 anchor needs beacon_api or consensus_p2p finality")
        }
    };
    let checkpoint = ConsensusCheckpoint {
        beacon_slot: bootstrap.beacon_slot,
        beacon_block_root: bootstrap.beacon_block_root,
        execution_block_hash: bootstrap.execution_block_hash,
        obtained_at_unix_seconds: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
        source: "L1 anchor checkpoint".to_owned(),
    };
    info!(
        slot = checkpoint.beacon_slot,
        "the L1 anchor's Ethereum finality starts"
    );
    Ok((source, checkpoint))
}

/// Finalize the op-stack blocks whose dispute game the registry anchors:
/// each game's claim is checked against the output root of the block the
/// sequencer signed, which `signed` delivers.
fn follow_dispute_games(
    config: &Config,
    fastest_block_ms: u64,
    mut signed: mpsc::Receiver<leani_finality_op_gossip::SignedBlock>,
    mut observations: mpsc::Receiver<Observation>,
    cancellation: CancellationToken,
) -> Result<watch::Receiver<FinalizedBlock>> {
    // Games are created within about an hour of their block; keep four
    // hours at the fastest scheduled cadence, so a fork like Denim fits too.
    let capacity = usize::try_from(4 * 3_600_000 / fastest_block_ms.max(1)).unwrap_or(usize::MAX);
    let mut ledger =
        GameLedger::load(Some(config.data_dir.join("l1-anchor-games.json")), capacity)?;
    let (finalized, receiver) = watch::channel(None);
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = cancellation.cancelled() => return,
                block = signed.recv() => {
                    let Some(block) = block else { return };
                    if let Some(withdrawals_root) = block.withdrawals_root {
                        let root = leani_l1_anchor::output_root(block.state_root, withdrawals_root, block.hash);
                        ledger.record_block(block.number, block.hash, root);
                    }
                }
                observation = observations.recv() => {
                    // A closed lane drops the sender: the finality stream
                    // then ends and the lanes restart with it.
                    let Some(observation) = observation else { return };
                    match ledger.observe(observation) {
                        Ok(Some((number, hash))) => {
                            info!(block = number, "the L1 anchor finalized a block");
                            finalized.send_replace(Some((BlockNumber(number), BlockHash::new(hash.0))));
                        }
                        Ok(None) => {}
                        Err(error) => warn!(%error, "could not record a dispute game"),
                    }
                }
            }
        }
    });
    Ok(receiver)
}

/// The blocks a Nitro chain's L1 anchor confirms: `confirmed` names the
/// newest at once and across restarts, `finalized` once this node holds it.
#[derive(Clone, Debug)]
pub(crate) struct Assertions {
    pub(crate) finalized: watch::Receiver<FinalizedBlock>,
    pub(crate) confirmed: watch::Receiver<Option<BlockHash>>,
}

/// Finalize the Nitro block each confirmed assertion names, once this node
/// holds it: a block it never followed cannot be finalized. The newest
/// confirmed hash is kept at `confirmed_path`, since the L1 lane resumes
/// past the assertions it already read.
pub(crate) fn follow_assertions(
    store: leani_store_sqlite::SqliteStore,
    chain_id: ChainId,
    mut observations: mpsc::Receiver<Observation>,
    confirmed_path: std::path::PathBuf,
    cancellation: CancellationToken,
) -> Assertions {
    let kept = std::fs::read_to_string(&confirmed_path)
        .ok()
        .and_then(|text| text.trim().parse::<alloy_primitives::B256>().ok())
        .map(|hash| BlockHash::new(hash.0));
    let (finalized, finalized_receiver) = watch::channel(None);
    let (confirmed, confirmed_receiver) = watch::channel(kept);
    tokio::spawn(async move {
        let mut pending = kept;
        let mut retry = tokio::time::interval(Duration::from_secs(30));
        loop {
            tokio::select! {
                () = cancellation.cancelled() => return,
                observation = observations.recv() => match observation {
                    Some(Observation::AssertionConfirmed { block_hash }) => {
                        if let Err(error) = std::fs::write(&confirmed_path, format!("{block_hash:#x}\n")) {
                            warn!(%error, "could not keep the confirmed block");
                        }
                        let hash = BlockHash::new(block_hash.0);
                        confirmed.send_replace(Some(hash));
                        pending = Some(hash);
                    }
                    Some(_) => continue,
                    None => return,
                },
                _ = retry.tick(), if pending.is_some() => {}
            }
            let Some(hash) = pending else { continue };
            match store.canonical_block_by_hash(chain_id, hash).await {
                Ok(Some(block)) => {
                    info!(block = block.number.0, "the L1 anchor finalized a block");
                    finalized.send_replace(Some((block.number, hash)));
                    pending = None;
                }
                Ok(None) => {
                    debug!(%hash, "the L1 anchor confirmed a block this node does not hold");
                }
                Err(error) => warn!(%error, "could not look up a confirmed block"),
            }
        }
    });
    Assertions {
        finalized: finalized_receiver,
        confirmed: confirmed_receiver,
    }
}
