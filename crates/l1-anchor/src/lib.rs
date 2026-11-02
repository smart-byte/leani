//! Rollup finality from Ethereum. A rollup's settlement contracts emit
//! events in L1 blocks; once those blocks are finalized, the events say
//! which L2 block the chain's own rules treat as final: a confirmed Nitro
//! assertion names its block hash, and an op-stack anchor state registry
//! moves to a dispute game whose root claim is checked against a block the
//! sequencer signed.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use alloy_primitives::{Address, B256, b256, keccak256};
use futures::StreamExt;
use leani_chain::L1Anchor;
use leani_primitives::{
    BlockFrame, BlockHash, BlockNumber, BlockRange, Capability, CapabilitySet, ChainId, Material,
    SourceId, SourceKind, TrustModel,
};
use leani_source_api::{
    ConsensusCheckpoint, FinalityEvent, FinalityEventStream, FinalityModel, FinalitySource,
    Partitioning, SourceDescriptor, SourceError,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch};
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
/// `create(uint32,bytes32,bytes)` and
/// `createWithInitData(uint32,bytes32,bytes,bytes)`: the extra data, at the
/// same place in both, starts with the claimed L2 block number.
const CREATE_SELECTORS: [[u8; 4]; 2] = [[0x82, 0xec, 0xf2, 0xf6], [0x10, 0x11, 0xf3, 0x77]];

/// What a finalized L1 block says about the rollup.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Observation {
    /// A game of the anchor's type claims `root` as the output root of L2
    /// block `l2_block`.
    GameCreated {
        game: Address,
        l2_block: u64,
        root: B256,
    },
    /// The registry's anchor moved to `game`.
    AnchorUpdated { game: Address },
    /// The rollup confirmed the assertion ending at `block_hash`.
    AssertionConfirmed { block_hash: B256 },
}

/// The contracts whose logs the anchor reads.
#[must_use]
pub fn log_addresses(anchor: &L1Anchor) -> Vec<Address> {
    match *anchor {
        L1Anchor::DisputeGames {
            factory, registry, ..
        } => vec![factory, registry],
        L1Anchor::NitroRollup { rollup } => vec![rollup],
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

/// The anchor's observations in one L1 block, in log order. A game created
/// through another contract has no readable creation call and is skipped:
/// a later game moves finality on.
#[must_use]
pub fn observe(anchor: &L1Anchor, frame: &BlockFrame) -> Vec<Observation> {
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
            } => {
                if address == factory && topic(0) == Some(DISPUTE_GAME_CREATED) {
                    let (Some(game), Some(kind), Some(root)) = (topic(1), topic(2), topic(3))
                    else {
                        continue;
                    };
                    if kind != B256::from(alloy_primitives::U256::from(game_type)) {
                        continue;
                    }
                    let Some(l2_block) = calldata(log.transaction_index).and_then(claimed_block)
                    else {
                        debug!(game = %Address::from_word(game), "no readable creation call for a dispute game");
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
                } else {
                    continue;
                }
            }
            L1Anchor::NitroRollup { rollup } => {
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

/// The L2 block a game creation call claims: the first word of its extra
/// data, the third argument of both creation functions.
fn claimed_block(input: &[u8]) -> Option<u64> {
    let (selector, arguments) = input.split_first_chunk::<4>()?;
    if !CREATE_SELECTORS.contains(selector) {
        return None;
    }
    let word = |offset: usize| arguments.get(offset..offset.checked_add(32)?);
    let extra = usize::try_from(alloy_primitives::U256::from_be_slice(word(64)?)).ok()?;
    let length = alloy_primitives::U256::from_be_slice(word(extra)?);
    if length < alloy_primitives::U256::from(32) {
        return None;
    }
    u64::try_from(alloy_primitives::U256::from_be_slice(word(extra + 32)?)).ok()
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
    signed: BTreeMap<u64, (B256, B256)>,
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

    /// Remember a signed block and its output root.
    pub fn record_block(&mut self, number: u64, hash: B256, output_root: B256) {
        self.signed.insert(number, (hash, output_root));
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
                match self.signed.get(&l2_block) {
                    Some((hash, signed_root)) if *signed_root == root => {
                        info!(%game, l2_block, "a dispute game claims the signed block's output root");
                        self.games.insert(game, (l2_block, *hash));
                        self.save()?;
                    }
                    Some(_) => {
                        warn!(%game, l2_block, "a dispute game claims another output root than the signed block's");
                    }
                    None => {
                        debug!(%game, l2_block, "a dispute game claims a block this node did not see signed");
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
            Observation::AssertionConfirmed { .. } => Ok(None),
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
}

impl<B: FinalizedBlocks> AnchorLane<B> {
    /// Send each observation, in L1 order, until `cancellation`.
    ///
    /// # Errors
    ///
    /// Returns an error when L1 finality ends or fails, a range cannot be
    /// read, or finalized L1 blocks fail to link to the last range read.
    pub async fn run(
        self,
        observations: mpsc::Sender<Observation>,
        cancellation: CancellationToken,
    ) -> Result<(), SourceError> {
        let mut cursor = read_cursor(&self.cursor_path)?;
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
            let start = cursor.as_ref().map_or_else(
                || block_number.0.saturating_sub(self.lookback),
                |cursor| cursor.number.saturating_add(1),
            );
            if start > block_number.0 {
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
            if let (Some(cursor), Some(first)) = (&cursor, frames.first())
                && first.block.number.0 == cursor.number.saturating_add(1)
                && first.block.parent_hash.0 != cursor.hash.0
            {
                return Err(SourceError::Unavailable(format!(
                    "finalized L1 block {} does not link to block {} read before",
                    first.block.number.0, cursor.number
                )));
            }
            let mut found = 0_usize;
            for frame in &frames {
                for observation in observe(&self.anchor, frame) {
                    found += 1;
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

fn read_cursor(path: &Path) -> Result<Option<Cursor>, SourceError> {
    if !path.exists() {
        return Ok(None);
    }
    let bytes = std::fs::read(path).map_err(|error| SourceError::Unavailable(error.to_string()))?;
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|error| SourceError::Unavailable(format!("{}: {error}", path.display())))
}

/// The rollup block the L1 anchor finalized last.
pub type FinalizedBlock = Option<(BlockNumber, BlockHash)>;

/// Finality from the L1 anchor alone: each block it finalizes, newest first
/// when several arrive between reads.
#[derive(Debug)]
pub struct AnchoredFinality {
    descriptor: SourceDescriptor,
    finalized: watch::Receiver<FinalizedBlock>,
}

impl AnchoredFinality {
    /// # Errors
    ///
    /// Returns an error for an invalid source id.
    pub fn new(
        chain_id: ChainId,
        finalized: watch::Receiver<FinalizedBlock>,
    ) -> Result<Self, SourceError> {
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
            finalized,
        })
    }
}

/// The finality event of an anchored block; the beacon fields carry its
/// number and a zero root.
#[must_use]
pub const fn finalized_event(number: BlockNumber, hash: BlockHash) -> FinalityEvent {
    FinalityEvent::Finalized {
        block_number: number,
        block_hash: hash,
        beacon_slot: number.0,
        beacon_block_root: [0; 32],
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
        let mut finalized = self.finalized.clone();
        finalized.mark_changed();
        Ok(futures::stream::unfold(
            (finalized, cancellation),
            |(mut finalized, cancellation)| async move {
                loop {
                    tokio::select! {
                        () = cancellation.cancelled() => return None,
                        changed = finalized.changed() => {
                            changed.ok()?;
                            let latest = *finalized.borrow_and_update();
                            if let Some((number, hash)) = latest {
                                return Some((Ok(finalized_event(number, hash)), (finalized, cancellation)));
                            }
                        }
                    }
                }
            },
        )
        .boxed())
    }
}

#[cfg(test)]
mod tests;
