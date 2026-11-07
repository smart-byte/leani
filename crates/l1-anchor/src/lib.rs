//! Rollup finality from Ethereum. A rollup's settlement contracts emit
//! events in L1 blocks; once those blocks are finalized, the events say
//! which L2 block the chain's own rules treat as final: a confirmed Nitro
//! assertion names its block hash, and an op-stack anchor state registry
//! moves to a dispute game whose claim, an output root or a super root
//! holding one, is checked against a block the sequencer signed. The same
//! blocks carry an op-stack chain's new sequencer signer and respected game
//! type.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use alloy_primitives::{Address, B256, b256, keccak256};
use futures::StreamExt;
use leani_chain::{GameClaims, L1Anchor};
use leani_primitives::{
    BlockFrame, BlockHash, BlockNumber, BlockRange, Capability, CapabilitySet, ChainId, Material,
    SourceId, SourceKind, TrustModel,
};
use leani_source_api::{
    ConsensusCheckpoint, FinalityEvent, FinalityEventStream, FinalityModel, FinalitySource,
    Partitioning, RollupFinality, SourceDescriptor, SourceError,
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// `DisputeGameCreated(address indexed, uint32 indexed, bytes32 indexed)`.
const DISPUTE_GAME_CREATED: B256 =
    b256!("0x5b565efe82411da98814f356d0e7bcb8f0219b8d970307c5afb4a6903a8b2e35");
/// `AnchorUpdated(address indexed)`.
const ANCHOR_UPDATED: B256 =
    b256!("0x474f180d74ea8751955ee261c93ff8270411b180408d1014c49f552c92a4d11e");
/// `AssertionConfirmed(bytes32 indexed, bytes32, bytes32)`.
const ASSERTION_CONFIRMED: B256 =
    b256!("0xfc42829b29c259a7370ab56c8f69fce23b5f351a9ce151da453281993ec0090c");
/// `ConfigUpdate(uint256 indexed, uint8 indexed, bytes)`, as a `SystemConfig`
/// emits it.
const CONFIG_UPDATE: B256 =
    b256!("0x1d2b0bda21d56b8bd12d4f94ebacffdfb35f5e226f84b461103bb8beab6353be");
/// `SystemConfig`'s `UpdateType.BATCHER`.
const BATCHER: u64 = 0;
/// `SystemConfig`'s `UpdateType.UNSAFE_BLOCK_SIGNER`.
const UNSAFE_BLOCK_SIGNER: u64 = 3;
/// `RespectedGameTypeSet(uint32)`, as an anchor state registry emits it.
const RESPECTED_GAME_TYPE_SET: B256 =
    b256!("0xcee0703b5e4bad4efededab85c9fd1aec17dee7c5f6c584330e0509b677745a2");
/// `create(uint32,bytes32,bytes)` and
/// `createWithInitData(uint32,bytes32,bytes,bytes)`: the extra data, at the
/// same place in both, starts with the claimed L2 block number, or is the
/// preimage of the claimed super root.
const CREATE_SELECTORS: [[u8; 4]; 2] = [[0x82, 0xec, 0xf2, 0xf6], [0x10, 0x11, 0xf3, 0x77]];

/// The L2 block a dispute game claims an output root for.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClaimedBlock {
    Number(u64),
    /// The block made at this Unix time, as a super root names its chains'
    /// blocks.
    Time(u64),
}

/// What a finalized L1 block says about the rollup.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Observation {
    /// A game of the anchor's type claims `root` as the output root of
    /// `l2_block`.
    GameCreated {
        game: Address,
        l2_block: ClaimedBlock,
        root: B256,
    },
    /// The registry's anchor moved to `game`.
    AnchorUpdated { game: Address },
    /// The rollup confirmed the assertion ending at `block_hash`.
    AssertionConfirmed { block_hash: B256 },
    /// The `SystemConfig` names a new key to sign the sequencer's blocks.
    SignerChanged { signer: Address },
    /// The `SystemConfig` names a new batcher, whose transactions to the
    /// batch inbox derivation reads from this L1 block on.
    BatcherChanged { batcher: Address },
    /// The batches read so far carry the rollup block `number`, which the
    /// node holds as `hash`: its data is final on Ethereum.
    DataFinal { number: u64, hash: B256 },
    /// The registry respects games of `game_type` from this block on.
    GameTypeChanged { game_type: u32 },
}

/// The contracts whose logs the anchor reads, `system_config` among them for
/// an op-stack chain's signer updates.
#[must_use]
pub fn log_addresses(anchor: &L1Anchor, system_config: Option<Address>) -> Vec<Address> {
    match *anchor {
        L1Anchor::DisputeGames {
            factory, registry, ..
        } => [factory, registry]
            .into_iter()
            .chain(system_config)
            .collect(),
        L1Anchor::NitroRollup { rollup, .. } => vec![rollup],
    }
}

/// The contract whose calldata the anchor reads, if any: a game's L2 block
/// is only in its creation call.
#[must_use]
pub const fn calldata_recipient(anchor: &L1Anchor) -> Option<Address> {
    match *anchor {
        L1Anchor::DisputeGames { factory, .. } => Some(factory),
        L1Anchor::NitroRollup { .. } => None,
    }
}

/// The anchor's observations in one L1 block of rollup `chain_id`, in log
/// order, with the signer updates of the rollup's `system_config`. A game
/// created through another contract has no readable creation call and is
/// skipped: a later game moves finality on.
#[must_use]
pub fn observe(
    anchor: &L1Anchor,
    chain_id: u64,
    system_config: Option<Address>,
    frame: &BlockFrame,
) -> Vec<Observation> {
    let logs = match &frame.logs {
        Material::Complete(logs) | Material::Filtered { value: logs, .. } => logs.as_slice(),
        Material::Missing(_) => return Vec::new(),
    };
    let calldata = |index: u32| match &frame.transactions {
        Material::Complete(transactions)
        | Material::Filtered {
            value: transactions,
            ..
        } => transactions
            .iter()
            .find(|transaction| transaction.index == index)
            .and_then(|transaction| transaction.input.as_deref()),
        Material::Missing(_) => None,
    };
    let mut observations = Vec::new();
    for log in logs {
        let address = Address::from(log.address.0);
        let topic = |index: usize| log.topics.get(index).map(|topic| B256::from(*topic));
        let observation = match *anchor {
            L1Anchor::DisputeGames {
                factory,
                registry,
                game_type,
                claims,
            } => {
                if address == factory && topic(0) == Some(DISPUTE_GAME_CREATED) {
                    let (Some(game), Some(kind), Some(claim)) = (topic(1), topic(2), topic(3))
                    else {
                        continue;
                    };
                    if kind != B256::from(alloy_primitives::U256::from(game_type)) {
                        continue;
                    }
                    let Some((l2_block, root)) = calldata(log.transaction_index)
                        .and_then(extra_data)
                        .and_then(|extra| claimed(extra, claims, claim, chain_id))
                    else {
                        debug!(game = %Address::from_word(game), "no readable claim in a dispute game's creation call");
                        continue;
                    };
                    Observation::GameCreated {
                        game: Address::from_word(game),
                        l2_block,
                        root,
                    }
                } else if address == registry && topic(0) == Some(ANCHOR_UPDATED) {
                    let Some(game) = topic(1) else { continue };
                    Observation::AnchorUpdated {
                        game: Address::from_word(game),
                    }
                } else if address == registry && topic(0) == Some(RESPECTED_GAME_TYPE_SET) {
                    let Some(game_type) = log
                        .data
                        .get(..32)
                        .map(alloy_primitives::U256::from_be_slice)
                        .and_then(|game_type| u32::try_from(game_type).ok())
                    else {
                        continue;
                    };
                    Observation::GameTypeChanged { game_type }
                } else if Some(address) == system_config && topic(0) == Some(CONFIG_UPDATE) {
                    // `abi.encode(bytes)` of one address: offset, length, word.
                    let Some(updated) = log
                        .data
                        .get(64..96)
                        .map(|word| Address::from_word(B256::from_slice(word)))
                    else {
                        continue;
                    };
                    let update = |kind: u64| {
                        topic(2) == Some(B256::from(alloy_primitives::U256::from(kind)))
                    };
                    if update(UNSAFE_BLOCK_SIGNER) {
                        Observation::SignerChanged { signer: updated }
                    } else if update(BATCHER) {
                        Observation::BatcherChanged { batcher: updated }
                    } else {
                        continue;
                    }
                } else {
                    continue;
                }
            }
            L1Anchor::NitroRollup { rollup, .. } => {
                if address != rollup || topic(0) != Some(ASSERTION_CONFIRMED) || log.data.len() < 32
                {
                    continue;
                }
                Observation::AssertionConfirmed {
                    block_hash: B256::from_slice(&log.data[..32]),
                }
            }
        };
        observations.push(observation);
    }
    observations
}

/// The extra data of a game creation call: the third argument of both
/// creation functions.
fn extra_data(input: &[u8]) -> Option<&[u8]> {
    let (selector, arguments) = input.split_first_chunk::<4>()?;
    if !CREATE_SELECTORS.contains(selector) {
        return None;
    }
    let word = |offset: usize| arguments.get(offset..offset.checked_add(32)?);
    let start = usize::try_from(alloy_primitives::U256::from_be_slice(word(64)?)).ok()?;
    let length = usize::try_from(alloy_primitives::U256::from_be_slice(word(start)?)).ok()?;
    let from = start.checked_add(32)?;
    arguments.get(from..from.checked_add(length)?)
}

/// The block a game's `extra` data names and the output root `claim` holds
/// for it: the claim itself, or the rollup's entry in the super root it is.
fn claimed(
    extra: &[u8],
    claims: GameClaims,
    claim: B256,
    chain_id: u64,
) -> Option<(ClaimedBlock, B256)> {
    match claims {
        GameClaims::OutputRoots => {
            let number = alloy_primitives::U256::from_be_slice(extra.get(..32)?);
            Some((ClaimedBlock::Number(u64::try_from(number).ok()?), claim))
        }
        GameClaims::SuperRoots => {
            // Version 1: the time, then each chain's id and output root.
            if keccak256(extra) != claim {
                return None;
            }
            let (&version, rest) = extra.split_first()?;
            let (time, outputs) = rest.split_first_chunk::<8>()?;
            let (outputs, partial) = outputs.as_chunks::<64>();
            if version != 1 || !partial.is_empty() {
                return None;
            }
            let output = outputs.iter().find(|output| {
                alloy_primitives::U256::from_be_slice(&output[..32])
                    == alloy_primitives::U256::from(chain_id)
            })?;
            Some((
                ClaimedBlock::Time(u64::from_be_bytes(*time)),
                B256::from_slice(&output[32..]),
            ))
        }
    }
}

/// An op-stack output root (version 0), as dispute games claim it.
/// `withdrawals_root` is the header's: the message passer's storage root
/// since Isthmus.
#[must_use]
pub fn output_root(state_root: B256, withdrawals_root: B256, block_hash: B256) -> B256 {
    let mut preimage = [0_u8; 128];
    preimage[32..64].copy_from_slice(state_root.as_slice());
    preimage[64..96].copy_from_slice(withdrawals_root.as_slice());
    preimage[96..].copy_from_slice(block_hash.as_slice());
    keccak256(preimage)
}

/// Games whose claim matched a block the sequencer signed, until the
/// registry names one: that block is then final. Signed blocks are kept
/// only for `capacity` blocks, long enough for a game to be created over
/// them; verified games survive restarts in a file.
#[derive(Debug)]
pub struct GameLedger {
    capacity: usize,
    /// Each signed block's hash, output root, and time.
    signed: BTreeMap<u64, (B256, B256, u64)>,
    games: BTreeMap<Address, (u64, B256)>,
    path: Option<PathBuf>,
}

#[derive(Default, Deserialize, Serialize)]
struct StoredGames {
    games: Vec<(Address, u64, B256)>,
}

impl GameLedger {
    /// # Errors
    ///
    /// Returns an error for an unreadable or malformed ledger file.
    pub fn load(path: Option<PathBuf>, capacity: usize) -> std::io::Result<Self> {
        let games = match &path {
            Some(path) if path.exists() => {
                let stored: StoredGames = serde_json::from_slice(&std::fs::read(path)?)?;
                stored
                    .games
                    .into_iter()
                    .map(|(game, number, hash)| (game, (number, hash)))
                    .collect()
            }
            _ => BTreeMap::new(),
        };
        Ok(Self {
            capacity,
            signed: BTreeMap::new(),
            games,
            path,
        })
    }

    /// Remember a signed block, its output root, and the Unix time it was
    /// made.
    pub fn record_block(&mut self, number: u64, hash: B256, output_root: B256, time: u64) {
        self.signed.insert(number, (hash, output_root, time));
        while self.signed.len() > self.capacity {
            self.signed.pop_first();
        }
    }

    /// Apply one observation; returns the block it finalizes.
    ///
    /// # Errors
    ///
    /// Returns an error when the ledger file cannot be written.
    pub fn observe(&mut self, observation: Observation) -> std::io::Result<Option<(u64, B256)>> {
        match observation {
            Observation::GameCreated {
                game,
                l2_block,
                root,
            } => {
                let signed = match l2_block {
                    ClaimedBlock::Number(number) => self.signed.get_key_value(&number),
                    ClaimedBlock::Time(time) => {
                        self.signed.iter().find(|(_, (_, _, made))| *made == time)
                    }
                };
                match signed {
                    Some((&number, &(hash, signed_root, _))) if signed_root == root => {
                        info!(%game, l2_block = number, "a dispute game claims the signed block's output root");
                        self.games.insert(game, (number, hash));
                        self.save()?;
                    }
                    Some((&number, _)) => {
                        warn!(%game, l2_block = number, "a dispute game claims another output root than the signed block's");
                    }
                    None => {
                        debug!(%game, ?l2_block, "a dispute game claims a block this node did not see signed");
                    }
                }
                Ok(None)
            }
            Observation::AnchorUpdated { game } => {
                let Some(final_block) = self.games.remove(&game) else {
                    return Ok(None);
                };
                // Games anchored earlier than the new anchor can no longer
                // become it.
                self.games.retain(|_, (number, _)| *number > final_block.0);
                self.save()?;
                Ok(Some(final_block))
            }
            Observation::AssertionConfirmed { .. }
            | Observation::SignerChanged { .. }
            | Observation::BatcherChanged { .. }
            | Observation::GameTypeChanged { .. }
            | Observation::DataFinal { .. } => Ok(None),
        }
    }

    fn save(&self) -> std::io::Result<()> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        let stored = StoredGames {
            games: self
                .games
                .iter()
                .map(|(game, (number, hash))| (*game, *number, *hash))
                .collect(),
        };
        write_atomically(path, &serde_json::to_vec(&stored)?)
    }
}

fn write_atomically(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let staged = path.with_extension("tmp");
    std::fs::write(&staged, bytes)?;
    std::fs::rename(staged, path)
}

/// A finalized L1 block, as verified L1 finality names it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FinalizedTip {
    pub number: u64,
    pub hash: BlockHash,
    pub beacon_slot: u64,
    pub beacon_block_root: [u8; 32],
}

/// Finalized L1 blocks: the anchor's logs and creation calls of `range`,
/// hash-linked down from `tip`, which ends it.
#[async_trait::async_trait]
pub trait FinalizedBlocks: Send + Sync {
    async fn frames(
        &self,
        range: BlockRange,
        tip: FinalizedTip,
        cancellation: &CancellationToken,
    ) -> Result<Vec<BlockFrame>, SourceError>;
}

#[derive(Deserialize, Serialize)]
struct Cursor {
    number: u64,
    hash: B256,
    #[serde(default, flatten)]
    learned: Learned,
    #[serde(default)]
    batches: BatchCursor,
}

/// What the lane learned from the rollup's contracts up to where it stopped.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct Learned {
    /// The newest signer the `SystemConfig` named; `None` before any update.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signer: Option<Address>,
    /// The newest game type the registry respects; `None` before any change.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub game_type: Option<u32>,
    /// The newest batcher the `SystemConfig` named; `None` before any update.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batcher: Option<Address>,
}

/// What the lane whose cursor is at `cursor_path` learned, read before it
/// runs; nothing for a lane that never ran.
///
/// # Errors
///
/// Returns an error for a cursor that cannot be read.
pub fn learned(cursor_path: &Path) -> Result<Learned, SourceError> {
    Ok(read_cursor(cursor_path)?.map_or_else(Learned::default, |cursor| cursor.learned))
}

/// Follows verified L1 finality and reads each newly finalized range for
/// the anchor's events. Where it stopped survives restarts; a cold start
/// reads `lookback` blocks below the first finalized tip.
#[allow(missing_debug_implementations)]
pub struct AnchorLane<B> {
    pub anchor: L1Anchor,
    pub finality: Arc<dyn FinalitySource>,
    pub checkpoint: ConsensusCheckpoint,
    pub blocks: B,
    pub cursor_path: PathBuf,
    pub lookback: u64,
    /// The rollup's chain id, by which a super root names its output root.
    pub chain_id: u64,
    /// The rollup's `SystemConfig`, whose signer updates the lane reports.
    pub system_config: Option<Address>,
    /// Whether a game type the registry comes to respect replaces the
    /// anchor's; `false` keeps a configured one.
    pub follow_game_type: bool,
    /// Reads the rollup's batches for data finality; `None` leaves finality
    /// to settlement.
    pub batches: Option<BatchReader>,
}

impl<B: FinalizedBlocks> AnchorLane<B> {
    /// Send each observation, in L1 order, until `cancellation`.
    ///
    /// # Errors
    ///
    /// Returns an error when L1 finality ends or fails, a range cannot be
    /// read, or finalized L1 blocks fail to link to the last range read.
    #[allow(clippy::too_many_lines)]
    pub async fn run(
        mut self,
        observations: mpsc::Sender<Observation>,
        cancellation: CancellationToken,
    ) -> Result<(), SourceError> {
        let mut cursor = read_cursor(&self.cursor_path)?;
        let mut learned = cursor
            .as_ref()
            .map_or_else(Learned::default, |cursor| cursor.learned);
        if let Some(game_type) = learned.game_type {
            self.respect(game_type);
        }
        if let (Some(reader), Some(cursor)) = (&mut self.batches, &cursor) {
            reader.resume(cursor.batches);
        }
        let mut data_final = 0;
        let mut events = self
            .finality
            .subscribe(self.checkpoint.clone(), cancellation.clone())
            .await?;
        loop {
            let event = tokio::select! {
                () = cancellation.cancelled() => return Ok(()),
                event = events.next() => event,
            };
            let Some(event) = event else {
                return Err(SourceError::Disconnected("L1 finality ended".to_owned()));
            };
            let FinalityEvent::Finalized {
                block_number,
                block_hash,
                beacon_slot,
                beacon_block_root,
            } = event?
            else {
                continue;
            };
            // Blocks up to `observed` were read before; reading again from
            // an open channel's first frame only rebuilds that channel.
            let observed = cursor.as_ref().map(|cursor| cursor.number);
            let start = cursor.as_ref().map_or_else(
                || block_number.0.saturating_sub(self.lookback),
                |cursor| {
                    let next = cursor.number.saturating_add(1);
                    cursor
                        .batches
                        .resume_from
                        .map_or(next, |from| from.min(next))
                },
            );
            if observed.is_some_and(|observed| observed >= block_number.0) {
                continue;
            }
            let range = BlockRange::new(BlockNumber(start), block_number)
                .map_err(|error| SourceError::InvalidPlan(error.to_string()))?;
            let tip = FinalizedTip {
                number: block_number.0,
                hash: block_hash,
                beacon_slot,
                beacon_block_root,
            };
            let frames = self.blocks.frames(range, tip, &cancellation).await?;
            if let Some(cursor) = &cursor
                && let Some(next) = frames
                    .iter()
                    .find(|frame| frame.block.number.0 == cursor.number.saturating_add(1))
                && next.block.parent_hash.0 != cursor.hash.0
            {
                return Err(SourceError::Unavailable(format!(
                    "finalized L1 block {} does not link to block {} read before",
                    next.block.number.0, cursor.number
                )));
            }
            let mut found = 0_usize;
            for frame in &frames {
                let mut seen = Vec::new();
                if observed.is_none_or(|observed| frame.block.number.0 > observed) {
                    for observation in
                        observe(&self.anchor, self.chain_id, self.system_config, frame)
                    {
                        match observation {
                            Observation::SignerChanged { signer } => learned.signer = Some(signer),
                            Observation::BatcherChanged { batcher } => {
                                learned.batcher = Some(batcher);
                            }
                            Observation::GameTypeChanged { game_type } => {
                                learned.game_type = Some(game_type);
                                self.respect(game_type);
                            }
                            _ => {}
                        }
                        seen.push(observation);
                    }
                }
                if let Some(reader) = &mut self.batches {
                    let batcher = match reader.inbox {
                        BatchInbox::OpStack { batcher, .. } => learned.batcher.unwrap_or(batcher),
                        BatchInbox::Nitro { .. } => Address::ZERO,
                    };
                    if let Some((number, hash)) = reader.read(frame, batcher).await?
                        && number > data_final
                    {
                        data_final = number;
                        seen.push(Observation::DataFinal { number, hash });
                    }
                }
                found += seen.len();
                for observation in seen {
                    if observations.send(observation).await.is_err() {
                        return Ok(());
                    }
                }
            }
            info!(
                from = range.start().0,
                to = range.end().0,
                observations = found,
                "read finalized L1 blocks for the rollup anchor"
            );
            let next = Cursor {
                number: block_number.0,
                hash: B256::from(block_hash.0),
                learned,
                batches: self
                    .batches
                    .as_ref()
                    .map(BatchReader::cursor)
                    .unwrap_or_default(),
            };
            write_atomically(
                &self.cursor_path,
                &serde_json::to_vec(&next)
                    .map_err(|error| SourceError::Unavailable(error.to_string()))?,
            )
            .map_err(|error| SourceError::Unavailable(error.to_string()))?;
            cursor = Some(next);
        }
    }
}

impl<B> AnchorLane<B> {
    /// Read games of `game_type` from here on, unless the anchor's is fixed.
    fn respect(&mut self, game_type: u32) {
        if let (
            true,
            L1Anchor::DisputeGames {
                game_type: kept, ..
            },
        ) = (self.follow_game_type, &mut self.anchor)
            && *kept != game_type
        {
            info!(
                from = *kept,
                to = game_type,
                "the anchor state registry respects another game type"
            );
            *kept = game_type;
        }
    }
}

fn read_cursor(path: &Path) -> Result<Option<Cursor>, SourceError> {
    if !path.exists() {
        return Ok(None);
    }
    let bytes = std::fs::read(path).map_err(|error| SourceError::Unavailable(error.to_string()))?;
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|error| SourceError::Unavailable(format!("{}: {error}", path.display())))
}

/// Finality from Ethereum alone: the blocks the rollup's batches finalize
/// and its anchor settles, newest first when several arrive between reads.
#[derive(Debug)]
pub struct AnchoredFinality {
    descriptor: SourceDescriptor,
    rollup: RollupFinality,
}

impl AnchoredFinality {
    /// # Errors
    ///
    /// Returns an error for an invalid source id.
    pub fn new(chain_id: ChainId, rollup: RollupFinality) -> Result<Self, SourceError> {
        Ok(Self {
            descriptor: SourceDescriptor {
                id: SourceId::new("l1-anchor")
                    .map_err(|error| SourceError::InvalidPlan(error.to_string()))?,
                kind: SourceKind::L1Anchor,
                chain_id,
                range: None,
                capabilities: CapabilitySet::of(Capability::ConsensusFinality),
                complete_capabilities: CapabilitySet::of(Capability::ConsensusFinality),
                trust: TrustModel::ProtocolVerified,
                finality: FinalityModel::Finalized,
                partitioning: Partitioning::None,
                expected_lag: std::time::Duration::from_mins(13),
                schema_version: "l1-anchor.v1".to_owned(),
                priority: 0,
            },
            rollup,
        })
    }
}

#[async_trait::async_trait]
impl FinalitySource for AnchoredFinality {
    fn descriptor(&self) -> &SourceDescriptor {
        &self.descriptor
    }

    async fn subscribe(
        &self,
        _checkpoint: ConsensusCheckpoint,
        cancellation: CancellationToken,
    ) -> Result<FinalityEventStream, SourceError> {
        Ok(futures::stream::unfold(
            (self.rollup.subscribe(), cancellation),
            |(mut rollup, cancellation)| async move {
                tokio::select! {
                    () = cancellation.cancelled() => None,
                    // A lane that ended ends the stream; the lanes restart.
                    event = rollup.next() => event.ok().map(|event| (Ok(event), (rollup, cancellation))),
                }
            },
        )
        .boxed())
    }
}

mod batches;
#[cfg(test)]
mod tests;

pub use batches::{
    BatchCursor, BatchInbox, BatchReader, Blobs, HeldBlocks, SEQUENCER_BATCH_DELIVERED,
};
