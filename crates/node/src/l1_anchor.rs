//! The opt-in L1 anchor of a rollup node: Ethereum's verified finality and
//! execution P2P read the chain entry's settlement contracts.

use std::{
    sync::{Arc, atomic::AtomicBool},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use futures::StreamExt;
use leani_chain::L1Anchor;
use leani_l1_anchor::{
    AnchorLane, BatchInbox, BatchReader, Blobs, FinalizedBlocks, FinalizedTip, GameLedger,
    HeldBlocks, Observation,
};
use leani_primitives::{
    Address, BlockFrame, BlockHash, BlockNumber, BlockRange, BlockRef, Capability, CapabilitySet,
    ChainId, ConsensusAnchor, Finality, LogFieldSet,
};
use leani_source_api::{
    ConsensusCheckpoint, DataRequest, FieldProjection, FilterSet, FinalitySource, HistorySource,
    RollupBlock, RollupFinality, SourceBudget, SourceError, VerificationPolicy,
};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::config::{Config, FinalitySourceKind, FinalizedBy, L1AnchorConfig, LiveSourceKind};

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
    store: &leani_store_sqlite::SqliteStore,
    cancellation: &CancellationToken,
) -> Result<Option<(L1Anchor, mpsc::Receiver<Observation>)>> {
    if config.l1_anchor.is_none() {
        return Ok(None);
    }
    let anchor = config
        .l1_anchor_contracts()
        .map_err(anyhow::Error::msg)?
        .context("the chain entry names no settlement contracts")?;
    let (sender, observations) = mpsc::channel(256);
    let config = config.clone();
    let store = store.clone();
    let cancellation = cancellation.child_token();
    tokio::spawn(async move {
        while !cancellation.is_cancelled() {
            let outcome = match lane(&config, anchor, store.clone()).await {
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

async fn lane(
    config: &Config,
    anchor: L1Anchor,
    store: leani_store_sqlite::SqliteStore,
) -> Result<AnchorLane<P2pFinalizedBlocks>> {
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
    let system_config = config
        .chain_entry()?
        .op_stack
        .as_ref()
        .map(|op_stack| op_stack.system_config);
    let inbox = config.batch_inbox().map_err(anyhow::Error::msg)?;
    let (filters, required) = anchor_request(&anchor, system_config, inbox.as_ref());
    let chain_id = ChainId(config.chain_entry()?.chain_id);
    let batches = inbox.map(|inbox| {
        let blobs = matches!(inbox, BatchInbox::OpStack { .. }).then(|| {
            Arc::new(BeaconBlobs {
                client: reqwest::Client::new(),
                endpoints: l1.finality.endpoints.clone(),
            }) as Arc<dyn Blobs>
        });
        BatchReader::new(inbox, Arc::new(StoreHeldBlocks { store, chain_id }), blobs)
    });
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
        cursor_path: anchor_cursor_path(config),
        lookback: l1.lookback_blocks,
        chain_id: config.chain_entry()?.chain_id,
        system_config,
        follow_game_type: l1.game_type.is_none(),
        batches,
    })
}

/// What the lane reads from each finalized Ethereum block: the anchor's
/// logs and, where game creations or batches are read from calls, every
/// transaction's calldata. A recipient filter would keep only the logs of
/// transactions to those recipients, and registries, games and the
/// `SystemConfig` log in calls to themselves.
fn anchor_request(
    anchor: &L1Anchor,
    system_config: Option<alloy_primitives::Address>,
    inbox: Option<&BatchInbox>,
) -> (FilterSet, CapabilitySet) {
    let mut addresses = leani_l1_anchor::log_addresses(anchor, system_config);
    let mut required = CapabilitySet::of(Capability::Logs);
    if matches!(anchor, L1Anchor::DisputeGames { .. }) {
        required = required.with(Capability::Calldata);
    }
    match inbox {
        // Derivation reads a batcher transaction only when it succeeded,
        // from the batcher, by its blobs or calldata.
        Some(BatchInbox::OpStack { .. }) => {
            required = required
                .with(Capability::Calldata)
                .with(Capability::Senders)
                .with(Capability::BlobFields)
                .with(Capability::ReceiptOutcome);
        }
        // The inbox's log names each post, whose call carries its count.
        Some(BatchInbox::Nitro {
            sequencer_inbox, ..
        }) => {
            addresses.push(*sequencer_inbox);
            required = required.with(Capability::Calldata);
        }
        None => {}
    }
    let mut filters = FilterSet::default();
    filters.scope.addresses = addresses.into_iter().map(Address::from).collect();
    (filters, required)
}

struct StoreHeldBlocks {
    store: leani_store_sqlite::SqliteStore,
    chain_id: ChainId,
}

#[async_trait::async_trait]
impl HeldBlocks for StoreHeldBlocks {
    async fn hash_at(&self, number: u64) -> Result<Option<alloy_primitives::B256>, SourceError> {
        Ok(self
            .store
            .canonical_block(self.chain_id, BlockNumber(number))
            .await
            .map_err(store_error)?
            .map(|(block, _)| alloy_primitives::B256::from(block.hash.0)))
    }

    async fn number_of(&self, hash: alloy_primitives::B256) -> Result<Option<u64>, SourceError> {
        Ok(self
            .store
            .canonical_block_by_hash(self.chain_id, BlockHash::new(hash.0))
            .await
            .map_err(store_error)?
            .map(|block| block.number.0))
    }

    async fn head(&self) -> Result<Option<(u64, u64)>, SourceError> {
        Ok(self
            .store
            .canonical_tip(self.chain_id)
            .await
            .map_err(store_error)?
            .map(|block| (block.number.0, block.timestamp)))
    }
}

#[allow(clippy::needless_pass_by_value)] // Called as `map_err(store_error)`.
fn store_error(error: leani_store_sqlite::StoreError) -> SourceError {
    SourceError::Unavailable(format!("the node's store: {error}"))
}

/// Blobs from Beacon APIs, tried in turn. The batch reader checks each blob
/// against the versioned hash that names it.
struct BeaconBlobs {
    client: reqwest::Client,
    endpoints: Vec<url::Url>,
}

/// A blob is 131,072 bytes, hex-encoded in JSON.
const BLOB_JSON_BYTES: usize = 2 * 131_072 + 64;

#[derive(serde::Deserialize)]
struct BlobsResponse {
    data: Vec<String>,
}

#[async_trait::async_trait]
impl Blobs for BeaconBlobs {
    async fn blobs(
        &self,
        slot: u64,
        hashes: &[alloy_primitives::B256],
    ) -> Result<Vec<Vec<u8>>, SourceError> {
        let query: Vec<String> = hashes
            .iter()
            .map(|hash| format!("versioned_hashes={hash}"))
            .collect();
        let path = format!("eth/v1/beacon/blobs/{slot}?{}", query.join("&"));
        let mut failures = Vec::new();
        for endpoint in &self.endpoints {
            let endpoint = leani_finality_beacon_api::normalized_endpoint(endpoint);
            let label = leani_finality_beacon_api::request_label(&endpoint, &path);
            let fetched = async {
                let url = leani_finality_beacon_api::request_url(&endpoint, &path)
                    .map_err(|error| error.to_string())?;
                let response = self
                    .client
                    .get(url)
                    .header(reqwest::header::ACCEPT, "application/json")
                    .timeout(Duration::from_secs(30))
                    .send()
                    .await
                    .map_err(|error| error.without_url().to_string())?;
                let body = leani_finality_beacon_api::read_response(
                    response,
                    &label,
                    BLOB_JSON_BYTES * hashes.len().max(1) + 1_024,
                )
                .await
                .map_err(|error| error.to_string())?;
                let response: BlobsResponse =
                    serde_json::from_slice(&body).map_err(|error| error.to_string())?;
                response
                    .data
                    .iter()
                    .map(|blob| {
                        alloy_primitives::hex::decode(blob).map_err(|error| error.to_string())
                    })
                    .collect::<Result<Vec<_>, _>>()
            }
            .await;
            match fetched {
                Ok(blobs) if blobs.len() >= hashes.len() => return Ok(blobs),
                Ok(blobs) => failures.push(format!(
                    "{label}: {} of {} blobs",
                    blobs.len(),
                    hashes.len()
                )),
                Err(error) => failures.push(format!("{label}: {error}")),
            }
        }
        Err(SourceError::Unavailable(format!(
            "no Beacon API served the blobs of slot {slot}: {}",
            failures.join("; ")
        )))
    }
}

/// Where the L1 anchor lane keeps how far it read and what it learned.
fn anchor_cursor_path(config: &Config) -> std::path::PathBuf {
    config.data_dir.join("l1-anchor-cursor.json")
}

/// An op-stack chain's signed heads, and the blocks its finality from
/// Ethereum names. They outlive restarts of the lanes that follow them.
#[derive(Clone, Debug)]
pub(crate) struct OpHeads {
    pub(crate) heads: watch::Receiver<Option<leani_finality_op_gossip::SignedBlock>>,
    pub(crate) rollup: RollupFinality,
    /// The signed blocks' hashes, which check the blocks JSON-RPC endpoints
    /// serve when the live lane falls back to them.
    pub(crate) signed: Option<leani_source_rpc::feed::SignedHashes>,
    /// Set by a P2P run that no execution peer served: the next run follows
    /// over JSON-RPC.
    pub(crate) over_rpc: Arc<AtomicBool>,
    pub(crate) returns: Arc<std::sync::Mutex<crate::process::P2pReturns>>,
}

/// Join the chain's sequencer gossip, publishing each signed block as the
/// attested head, and start the L1 anchor when the node configures one.
pub(crate) async fn start_op_heads(
    config: &Config,
    store: &leani_store_sqlite::SqliteStore,
    attested_heads: leani_source_api::AttestedHeadPublisher,
    cancellation: &CancellationToken,
) -> Result<OpHeads> {
    let chain = config.chain_entry()?;
    let op_stack = chain
        .op_stack
        .as_ref()
        .context("op_gossip finality needs an op-stack chain")?;
    // The slot length once every scheduled fork is active.
    let fastest_block_ms = op_stack.cadence().slot_ms(u64::MAX);
    let mut gossip = leani_finality_op_gossip::OpGossipConfig::for_chain(&chain)?;
    // A configured signer stays; otherwise the newest the L1 anchor read.
    let learned = leani_l1_anchor::learned(&anchor_cursor_path(config))?;
    let configured = config
        .finality
        .sequencer_signer()
        .map_err(anyhow::Error::msg)?;
    let (rotations, signer_watch) = watch::channel(
        configured
            .or(learned.signer)
            .unwrap_or(op_stack.sequencer_signer),
    );
    gossip.signer = signer_watch;
    gossip.listen_port = config.finality.listen_port;
    gossip.discovery_port = config.finality.discovery_port;
    gossip.peers_path = Some(config.data_dir.join("op-gossip-peers.txt"));
    if !config.finality.bootnodes.is_empty() {
        gossip.bootnodes.clone_from(&config.finality.bootnodes);
    }
    let anchor = spawn(config, store, cancellation)?;
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
    let hashes = config
        .sources
        .live
        .kind
        .uses(LiveSourceKind::JsonRpc)
        .then(|| keep_signed_hashes(heads.clone(), cancellation.child_token()));
    let mut rollup = match (anchor, signed) {
        (Some((_, observations)), Some(signed)) => follow_dispute_games(
            config,
            fastest_block_ms,
            signed,
            observations,
            rotations,
            cancellation.child_token(),
        )?,
        _ => RollupFinality::default(),
    };
    if config.finalized_by() == Some(FinalizedBy::JsonRpc) {
        rollup.finalized = Some(endpoint_finality(config, cancellation).await?);
    }
    Ok(OpHeads {
        heads,
        rollup,
        signed: hashes,
        over_rpc: Arc::default(),
        returns: Arc::default(),
    })
}

/// Record every signed head's hash. A head the watch coalesced away is
/// still attested through the next one it links to.
fn keep_signed_hashes(
    mut heads: watch::Receiver<Option<leani_finality_op_gossip::SignedBlock>>,
    cancellation: CancellationToken,
) -> leani_source_rpc::feed::SignedHashes {
    let hashes = leani_source_rpc::feed::SignedHashes::default();
    let kept = hashes.clone();
    tokio::spawn(async move {
        loop {
            if let Some(block) = *heads.borrow_and_update() {
                kept.insert(block.number, block.hash);
            }
            tokio::select! {
                () = cancellation.cancelled() => return,
                changed = heads.changed() => if changed.is_err() { return },
            }
        }
    });
    hashes
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

/// Settle the op-stack blocks whose dispute game the registry anchors:
/// each game's claim is checked against the output root of the block the
/// sequencer signed, which `signed` delivers. The batches the lane reads
/// finalize blocks before that. A new signer the `SystemConfig` names goes
/// to `rotations`, unless the configuration fixes one.
fn follow_dispute_games(
    config: &Config,
    fastest_block_ms: u64,
    mut signed: mpsc::Receiver<leani_finality_op_gossip::SignedBlock>,
    mut observations: mpsc::Receiver<Observation>,
    rotations: watch::Sender<alloy_primitives::Address>,
    cancellation: CancellationToken,
) -> Result<RollupFinality> {
    let configured_signer = config
        .finality
        .sequencer_signer()
        .map_err(anyhow::Error::msg)?;
    // Games are created within about an hour of their block; keep four
    // hours at the fastest scheduled cadence, so a fork like Denim fits too.
    let capacity = usize::try_from(4 * 3_600_000 / fastest_block_ms.max(1)).unwrap_or(usize::MAX);
    let mut ledger =
        GameLedger::load(Some(config.data_dir.join("l1-anchor-games.json")), capacity)?;
    let (settled, settled_receiver) = watch::channel(None);
    let (data_final, data_final_receiver) = watch::channel(None);
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = cancellation.cancelled() => return,
                block = signed.recv() => {
                    let Some(block) = block else { return };
                    if let Some(withdrawals_root) = block.withdrawals_root {
                        let root = leani_l1_anchor::output_root(block.state_root, withdrawals_root, block.hash);
                        ledger.record_block(block.number, block.hash, root, block.timestamp);
                    }
                }
                observation = observations.recv() => {
                    // A closed lane drops the sender: the finality stream
                    // then ends and the lanes restart with it.
                    let Some(observation) = observation else { return };
                    if let Observation::SignerChanged { signer } = observation {
                        match configured_signer {
                            Some(kept) if kept != signer => warn!(%signer, %kept, "the SystemConfig names another sequencer signer; finality.sequencer_signer stays"),
                            Some(_) => {}
                            None => {
                                if rotations.send_replace(signer) != signer {
                                    info!(%signer, "the SystemConfig names a new sequencer signer; gossip blocks are checked against it");
                                }
                            }
                        }
                    }
                    if let Observation::DataFinal { number, hash } = observation {
                        debug!(block = number, "a finalized batch finalized a block");
                        data_final.send_replace(Some((BlockNumber(number), BlockHash::new(hash.0))));
                        continue;
                    }
                    match ledger.observe(observation) {
                        Ok(Some((number, hash))) => {
                            info!(block = number, "the L1 anchor settled a block");
                            settled.send_replace(Some((BlockNumber(number), BlockHash::new(hash.0))));
                        }
                        Ok(None) => {}
                        Err(error) => warn!(%error, "could not record a dispute game"),
                    }
                }
            }
        }
    });
    Ok(rollup_finality(
        config,
        data_final_receiver,
        settled_receiver,
    ))
}

/// The blocks a node's L1 lane names: its batches' `finalized` blocks when
/// it reads them, and the anchor's `settled` blocks.
fn rollup_finality(
    config: &Config,
    data_final: watch::Receiver<RollupBlock>,
    settled: watch::Receiver<RollupBlock>,
) -> RollupFinality {
    let batches = config.finalized_by() == Some(FinalizedBy::Batches);
    RollupFinality {
        finalized: batches.then_some(data_final),
        settled: Some(settled),
    }
}

/// The `finalized` block the chain's JSON-RPC endpoints agree on, as it
/// moves. The runtime finalizes it only where this node holds that hash.
pub(crate) async fn endpoint_finality(
    config: &Config,
    cancellation: &CancellationToken,
) -> Result<watch::Receiver<RollupBlock>> {
    // Gossip and the feed check the blocks the endpoints serve, not their
    // `finalized` tag: unless the quorum is configured, two of the configured
    // endpoints must agree on it, so one that is down pauses finality instead
    // of leaving it to the other.
    let mut quorum = config
        .json_rpc_pool(Vec::new())
        .map_err(anyhow::Error::msg)?;
    if config.sources.json_rpc.quorum.is_none() {
        quorum = quorum.at_least(2);
    }
    let source = leani_source_rpc::RpcFinalitySource::new(
        "json-rpc-finality",
        ChainId(config.chain.chain_id),
        quorum,
        Duration::from_millis(config.sources.json_rpc.finality_poll_interval_ms),
    )?;
    let checkpoint = ConsensusCheckpoint {
        beacon_slot: 0,
        beacon_block_root: [0; 32],
        execution_block_hash: BlockHash::ZERO,
        obtained_at_unix_seconds: 0,
        source: "the endpoints' finalized tag".to_owned(),
    };
    let mut events = source
        .subscribe(checkpoint, cancellation.child_token())
        .await?;
    let (sender, receiver) = watch::channel(None);
    tokio::spawn(async move {
        while let Some(event) = events.next().await {
            match event {
                Ok(leani_source_api::FinalityEvent::Finalized {
                    block_number,
                    block_hash,
                    ..
                }) => {
                    sender.send_replace(Some((block_number, block_hash)));
                }
                Ok(_) => {}
                // The closed watch ends the finality stream, and the lanes
                // restart with a new poller.
                Err(error) => {
                    warn!(%error, "the endpoints' finalized tag is unavailable");
                    return;
                }
            }
        }
    });
    Ok(receiver)
}

/// The blocks a Nitro chain's L1 lane names: `confirmed` names the newest
/// assertion's block at once and across restarts; `rollup` names it
/// settled once this node holds it, and the blocks its batches finalize.
#[derive(Clone, Debug)]
pub(crate) struct Assertions {
    pub(crate) rollup: RollupFinality,
    pub(crate) confirmed: watch::Receiver<Option<BlockHash>>,
}

/// Settle the Nitro block each confirmed assertion names, once this node
/// holds it: a block it never followed cannot be settled. The newest
/// confirmed hash is kept at `confirmed_path`, since the L1 lane resumes
/// past the assertions it already read. The batches the lane reads finalize
/// blocks before that.
pub(crate) fn follow_assertions(
    config: &Config,
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
    let (settled, settled_receiver) = watch::channel(None);
    let (data_final, data_final_receiver) = watch::channel(None);
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
                    Some(Observation::DataFinal { number, hash }) => {
                        debug!(block = number, "a finalized batch finalized a block");
                        data_final.send_replace(Some((BlockNumber(number), BlockHash::new(hash.0))));
                        continue;
                    }
                    Some(_) => continue,
                    None => return,
                },
                _ = retry.tick(), if pending.is_some() => {}
            }
            let Some(hash) = pending else { continue };
            match store.canonical_block_by_hash(chain_id, hash).await {
                Ok(Some(block)) => {
                    info!(block = block.number.0, "the L1 anchor settled a block");
                    settled.send_replace(Some((block.number, hash)));
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
        rollup: rollup_finality(config, data_final_receiver, settled_receiver),
        confirmed: confirmed_receiver,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_lane_keeps_the_logs_of_transactions_to_any_contract() {
        let base = leani_chain::ChainRegistry::standard()
            .get("base")
            .expect("entry");
        let op_stack = base.op_stack.as_ref().expect("op-stack");
        let registry = alloy_primitives::Address::repeat_byte(2);
        let anchor = L1Anchor::DisputeGames {
            factory: alloy_primitives::Address::repeat_byte(1),
            registry,
            game_type: 0,
            claims: leani_chain::GameClaims::OutputRoots,
        };
        let batches = BatchInbox::OpStack {
            inbox: alloy_primitives::Address::repeat_byte(4),
            batcher: alloy_primitives::Address::repeat_byte(5),
            cadence: op_stack.cadence(),
        };
        // A game's own call, closing it, is where the registry logs the
        // anchor's move; the SystemConfig logs in calls to itself.
        let game = Address::new([6; 20]);
        for inbox in [None, Some(&batches)] {
            let (filters, required) = anchor_request(&anchor, Some(op_stack.system_config), inbox);
            let scope = filters.effective_scope();
            assert!(scope.matches_transaction(2, Some(&game), || unreachable!()));
            assert!(scope.matches_log(&Address::from(registry), &[[0_u8; 32]]));
            assert!(required.contains(Capability::Calldata));
        }
    }
}
