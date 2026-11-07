//! Data finality: a rollup block is final once the batch that carries it is
//! in a finalized Ethereum block, which fixes it for good.
//!
//! An op-stack batch names the block it builds on, in full or by the first 20
//! bytes of its hash. When a batch in finalized Ethereum data names the
//! node's block, that block and, by their parent hashes, its ancestors are
//! the chain derivation produces. Batches must follow each other, as
//! Holocene requires: a batch that skips blocks finalizes nothing, and one
//! naming another block than the node's moves no finality. A Nitro batch
//! post names how many messages the chain has, which numbers its last block.

use std::sync::Arc;

use alloy_primitives::{Address, B256};
use leani_chain::Cadence;
use leani_op_batches::{Batch, Channels};
use leani_primitives::{BlockFrame, Material, ReceiptEnvelope, TransactionEnvelope};
use leani_source_api::SourceError;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

/// `SequencerBatchDelivered(uint256 indexed, bytes32 indexed, bytes32
/// indexed, bytes32, uint256, (uint64,uint64,uint64,uint64), uint8)`, which
/// a `SequencerInbox` emits for every batch it accepts.
pub const SEQUENCER_BATCH_DELIVERED: B256 =
    alloy_primitives::b256!("0x7394f4a19a13c7b92b5bb71033245305946ef78452f7b4986ac1390b5df4ebd7");

/// `SequencerInbox`'s batch posts and the word of their calldata that holds
/// `newMessageCount`: the blob posts and the calldata posts, each plain and
/// with a delay proof.
const NITRO_BATCH_POSTS: [([u8; 4], usize); 6] = [
    ([0x3e, 0x5a, 0xa0, 0x82], 4), // addSequencerL2BatchFromBlobs
    ([0x91, 0x7c, 0xf8, 0xac], 4), // addSequencerL2BatchFromBlobsDelayProof
    ([0x8f, 0x11, 0x1f, 0x3c], 5), // addSequencerL2BatchFromOrigin
    ([0x69, 0xca, 0xcd, 0xed], 5), // addSequencerL2BatchFromOriginDelayProof
    ([0xe0, 0xbc, 0x97, 0x29], 5), // addSequencerL2Batch
    ([0x6e, 0x62, 0x00, 0x55], 5), // addSequencerL2BatchDelayProof
];

/// Where a rollup posts its batches on Ethereum.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BatchInbox {
    /// An op-stack batch inbox: frames in the blobs or calldata of
    /// transactions from the batcher, whom the `SystemConfig` can replace.
    OpStack {
        inbox: Address,
        batcher: Address,
        cadence: Cadence,
    },
    /// A Nitro `SequencerInbox`, whose message `n` is block `genesis + n`.
    Nitro {
        sequencer_inbox: Address,
        genesis: u64,
    },
}

/// The rollup chain this node holds.
#[async_trait::async_trait]
pub trait HeldBlocks: Send + Sync {
    /// The hash of the node's canonical block `number`.
    async fn hash_at(&self, number: u64) -> Result<Option<B256>, SourceError>;
    /// The number of the node's canonical block `hash`.
    async fn number_of(&self, hash: B256) -> Result<Option<u64>, SourceError>;
    /// The number and timestamp of the node's newest canonical block.
    async fn head(&self) -> Result<Option<(u64, u64)>, SourceError>;
}

/// Blobs by the versioned hashes that name them in the Ethereum block at a
/// beacon slot. What it returns is checked against those hashes.
#[async_trait::async_trait]
pub trait Blobs: Send + Sync {
    async fn blobs(&self, slot: u64, hashes: &[B256]) -> Result<Vec<Vec<u8>>, SourceError>;
}

/// Where batch reading stands, kept with the lane's cursor.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct BatchCursor {
    /// The block the next batch builds on; `None` until a batch matched.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_parent: Option<u64>,
    /// The L1 block holding the first frame of a channel still open: reading
    /// resumes there.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume_from: Option<u64>,
}

/// Reads the batches of finalized L1 blocks in order and names the newest
/// rollup block they make final.
#[allow(missing_debug_implementations)]
pub struct BatchReader {
    pub inbox: BatchInbox,
    pub held: Arc<dyn HeldBlocks>,
    /// Blobs for op-stack batches; `None` reads calldata batches only.
    pub blobs: Option<Arc<dyn Blobs>>,
    channels: Channels,
    next_parent: Option<u64>,
}

/// What a batch builds on.
enum Parent {
    Hash(B256),
    Prefix([u8; 20]),
}

impl Parent {
    fn matches(&self, hash: B256) -> bool {
        match self {
            Self::Hash(parent) => *parent == hash,
            Self::Prefix(prefix) => hash[..20] == prefix[..],
        }
    }
}

impl BatchReader {
    #[must_use]
    pub fn new(
        inbox: BatchInbox,
        held: Arc<dyn HeldBlocks>,
        blobs: Option<Arc<dyn Blobs>>,
    ) -> Self {
        Self {
            inbox,
            held,
            blobs,
            channels: Channels::default(),
            next_parent: None,
        }
    }

    /// Continue from `cursor`, kept by the run that stopped.
    pub const fn resume(&mut self, cursor: BatchCursor) {
        self.next_parent = cursor.next_parent;
    }

    /// Where reading stands after the blocks read so far.
    #[must_use]
    pub fn cursor(&self) -> BatchCursor {
        BatchCursor {
            next_parent: self.next_parent,
            resume_from: self.channels.open_since(),
        }
    }

    /// The newest rollup block that `frame`, a finalized L1 block, makes
    /// final, if any. `batcher` is the op-stack batcher at this block.
    ///
    /// # Errors
    ///
    /// Returns an error when blobs or held blocks cannot be read; reading
    /// this block again then gives the same result.
    pub async fn read(
        &mut self,
        frame: &BlockFrame,
        batcher: Address,
    ) -> Result<Option<(u64, B256)>, SourceError> {
        match self.inbox {
            BatchInbox::OpStack { inbox, cadence, .. } => {
                self.read_op_stack(frame, inbox, batcher, cadence).await
            }
            BatchInbox::Nitro {
                sequencer_inbox,
                genesis,
            } => self.read_nitro(frame, sequencer_inbox, genesis).await,
        }
    }

    async fn read_op_stack(
        &mut self,
        frame: &BlockFrame,
        inbox: Address,
        batcher: Address,
        cadence: Cadence,
    ) -> Result<Option<(u64, B256)>, SourceError> {
        let l1 = frame.block.number.0;
        let mut newest = None;
        for transaction in batcher_transactions(frame, inbox, batcher) {
            let data = if transaction.transaction_type == 3 {
                self.blob_data(frame, transaction).await?
            } else {
                vec![transaction.input.clone().unwrap_or_default()]
            };
            for data in data {
                let decoded = match leani_op_batches::frames(&data) {
                    Ok(decoded) => decoded,
                    Err(error) => {
                        warn!(l1, %error, "skipping a batcher transaction's data, as derivation does");
                        continue;
                    }
                };
                let completed: Vec<Vec<u8>> = decoded
                    .into_iter()
                    .filter_map(|piece| self.channels.add(piece, l1))
                    .collect();
                for channel in completed {
                    let readable = match leani_op_batches::batches(&channel) {
                        Ok(readable) => readable,
                        Err(error) => {
                            warn!(l1, %error, "skipping a channel derivation cannot read");
                            continue;
                        }
                    };
                    for batch in readable {
                        match batch {
                            Ok(batch) => {
                                if let Some(final_block) = self.match_batch(&batch, cadence).await?
                                {
                                    newest = Some(final_block);
                                }
                            }
                            Err(error) => {
                                warn!(l1, %error, "the rest of a channel is unreadable; derivation drops it too");
                                break;
                            }
                        }
                    }
                }
            }
        }
        Ok(newest)
    }

    /// The data of a blob transaction's blobs, each checked against the
    /// versioned hash the transaction names; a blob whose encoding is
    /// invalid is skipped, as derivation skips it.
    async fn blob_data(
        &self,
        frame: &BlockFrame,
        transaction: &TransactionEnvelope,
    ) -> Result<Vec<Vec<u8>>, SourceError> {
        let Some(blobs) = &self.blobs else {
            return Err(SourceError::Unavailable(
                "op-stack blob batches need a Beacon API for blobs".to_owned(),
            ));
        };
        let hashes: Vec<B256> = transaction
            .blob_versioned_hashes
            .iter()
            .map(|hash| B256::from(hash.0))
            .collect();
        let slot = leani_primitives::slot_at(frame.block.timestamp);
        let fetched = blobs.blobs(slot, &hashes).await?;
        let mut by_hash = std::collections::HashMap::new();
        for blob in fetched {
            let hash = tokio::task::spawn_blocking({
                let blob = blob.clone();
                move || leani_op_batches::versioned_hash(&blob)
            })
            .await
            .map_err(|error| SourceError::Unavailable(error.to_string()))?
            .map_err(|error| SourceError::Unavailable(format!("a blob of slot {slot}: {error}")))?;
            by_hash.insert(hash, blob);
        }
        let mut data = Vec::with_capacity(hashes.len());
        for hash in &hashes {
            let blob = by_hash.remove(hash).ok_or_else(|| {
                SourceError::Unavailable(format!("no blob {hash} at slot {slot}"))
            })?;
            match leani_op_batches::blob_data(&blob) {
                Ok(bytes) => data.push(bytes),
                Err(error) => warn!(%hash, %error, "skipping a blob derivation cannot read"),
            }
        }
        Ok(data)
    }

    /// The block `batch` makes final, if it follows the batches before it
    /// and names a block the node holds.
    async fn match_batch(
        &mut self,
        batch: &Batch,
        cadence: Cadence,
    ) -> Result<Option<(u64, B256)>, SourceError> {
        let blocks = batch.blocks();
        let (parent, number) = match *batch {
            Batch::Singular {
                parent_hash,
                timestamp,
                ..
            } => {
                // Once the node holds a block newer than the batch's, its
                // parent is either held or on another branch.
                if !self.holds(|head| head.1 > timestamp).await? {
                    return Ok(None);
                }
                let number = self.held.number_of(parent_hash).await?;
                (Parent::Hash(parent_hash), number)
            }
            Batch::Span {
                relative_timestamp,
                parent_check,
                ..
            } => {
                let Some(first) = cadence
                    .slot_starting_at(cadence.genesis_time.saturating_add(relative_timestamp))
                else {
                    warn!(relative_timestamp, "a span batch's time starts no block");
                    return Ok(None);
                };
                let last = first.saturating_add(blocks - 1);
                if !self.holds(|head| head.0 >= last).await? {
                    return Ok(None);
                }
                (Parent::Prefix(parent_check), first.checked_sub(1))
            }
        };
        // A singular batch whose parent the node does not hold builds, if it
        // follows the batches before it, on a block the node holds another
        // version of.
        let Some(number) = number.or(self.next_parent) else {
            debug!("a batch builds on a block this node does not hold");
            return Ok(None);
        };
        if let Some(next) = self.next_parent {
            if number < next {
                debug!(parent = number, next, "a batch for blocks already read");
                return Ok(None);
            }
            if number > next {
                warn!(
                    parent = number,
                    next, "a batch skips blocks, which derivation drops; finality waits"
                );
                return Ok(None);
            }
        }
        let held = self.held.hash_at(number).await?;
        match held {
            Some(hash) if parent.matches(hash) => {
                self.next_parent = Some(number + blocks);
                Ok(Some((number, hash)))
            }
            held => {
                if self.next_parent.is_some() {
                    warn!(
                        block = number,
                        held = ?held,
                        "a batch builds on another block than this node's; its blocks stay unfinalized"
                    );
                    self.next_parent = Some(number + blocks);
                }
                Ok(None)
            }
        }
    }

    /// Whether the node's head satisfies `ready`. While it does not, the
    /// node does not hold a finalized batch's blocks yet: matching starts
    /// over at a later batch whose parent it holds, and the lane goes on.
    async fn holds(&mut self, ready: impl Fn((u64, u64)) -> bool) -> Result<bool, SourceError> {
        if self.held.head().await?.is_some_and(ready) {
            return Ok(true);
        }
        if self.next_parent.take().is_some() {
            info!(
                "this node does not hold a finalized batch's blocks yet; matching starts over at a later batch"
            );
        }
        Ok(false)
    }

    async fn read_nitro(
        &mut self,
        frame: &BlockFrame,
        sequencer_inbox: Address,
        genesis: u64,
    ) -> Result<Option<(u64, B256)>, SourceError> {
        let mut last = None;
        for count in nitro_message_counts(frame, sequencer_inbox) {
            let Some(block) = genesis
                .checked_add(count)
                .and_then(|end| end.checked_sub(1))
            else {
                continue;
            };
            last = Some(last.map_or(block, |newest: u64| newest.max(block)));
        }
        let Some(last) = last else { return Ok(None) };
        if !self.holds(|head| head.0 >= last).await? {
            return Ok(None);
        }
        Ok(self.held.hash_at(last).await?.map(|hash| (last, hash)))
    }
}

/// The transactions of `frame` derivation reads: sent by `batcher` to
/// `inbox` and successful.
fn batcher_transactions(
    frame: &BlockFrame,
    inbox: Address,
    batcher: Address,
) -> impl Iterator<Item = &TransactionEnvelope> {
    let transactions = present(&frame.transactions);
    let receipts = present(&frame.receipts);
    transactions.iter().filter(move |transaction| {
        transaction.to.map(Address::from) == Some(inbox)
            && transaction.from.map(Address::from) == Some(batcher)
            && receipts.iter().any(|receipt: &ReceiptEnvelope| {
                receipt.transaction_index == transaction.index && receipt.success == Some(true)
            })
    })
}

/// The `newMessageCount` of each batch a `SequencerInbox` accepted in
/// `frame`: the calldata of every transaction that emitted
/// `SequencerBatchDelivered`.
fn nitro_message_counts(frame: &BlockFrame, sequencer_inbox: Address) -> Vec<u64> {
    let transactions = present(&frame.transactions);
    present(&frame.logs)
        .iter()
        .filter(|log| {
            Address::from(log.address.0) == sequencer_inbox
                && log.topics.first().map(|topic| B256::from(*topic))
                    == Some(SEQUENCER_BATCH_DELIVERED)
        })
        .filter_map(|log| {
            let input = transactions
                .iter()
                .find(|transaction| transaction.index == log.transaction_index)?
                .input
                .as_deref()?;
            new_message_count(input)
        })
        .collect()
}

/// `newMessageCount` of a `SequencerInbox` batch post's calldata.
fn new_message_count(input: &[u8]) -> Option<u64> {
    let (selector, arguments) = input.split_first_chunk::<4>()?;
    let (_, word) = NITRO_BATCH_POSTS
        .iter()
        .find(|(known, _)| known == selector)?;
    let word = arguments.get(word * 32..(word + 1) * 32)?;
    let (high, low) = word.split_at(24);
    high.iter()
        .all(|byte| *byte == 0)
        .then(|| u64::from_be_bytes(low.try_into().unwrap_or_default()))
}

fn present<T>(material: &Material<Vec<T>>) -> &[T] {
    match material {
        Material::Complete(value) | Material::Filtered { value, .. } => value,
        Material::Missing(_) => &[],
    }
}

#[cfg(test)]
mod tests;
