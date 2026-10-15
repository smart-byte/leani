//! Decisions of the live lane at the head: when it may accept a block before
//! an attested head covers it, and what an attested head, or its absence,
//! later means for the blocks it accepted.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    time::Duration,
};

use alloy_consensus::Header;
use alloy_primitives::{B256, B512};
use leani_primitives::{BlockFrame, BlockHash, BlockNumber, BlockRef, Material, SLOT_SECONDS};
use leani_source_api::AttestedHead;

/// Unattested blocks run at most this many slots past the newest attested
/// beacon slot, before Gloas, after it and across the boundary.
pub(crate) const MAX_UNATTESTED_SLOTS: u64 = 2;
/// Distinct peers that must serve one header before the lane accepts it
/// unattested.
// ponytail: constant; make it config if someone asks.
pub(crate) const HEAD_PEER_QUORUM: usize = 2;
/// A block first seen this long after its slot started waits for its child
/// or its attestation. Late payloads are the ones honestly orphaned: both
/// devnet-8 EL reorgs were served after 7 s.
// ponytail: threshold from glamsterdam-devnet-8; re-measure on Sepolia (G2).
pub(crate) const LATE_REVEAL_AFTER: Duration = Duration::from_secs(7);
/// Undo unattested blocks no attested head covered this long after the
/// oldest one's slot started. Gloas attests at about 25 s, plus 12 s per
/// missed slot.
// ponytail: re-measure the attested p99 on Sepolia (G2) before changing.
pub(crate) const HEAD_UNDO_GRACE: Duration = Duration::from_secs(60);
/// First poll for the next block, after its slot starts.
pub(crate) const HEAD_FIRST_POLL_OFFSET: Duration = Duration::from_secs(1);
/// Pause between polls while the next block is not served yet.
pub(crate) const HEAD_POLL_RETRY: Duration = Duration::from_millis(500);

/// Whether the lane may poll the next block before an attested head covers it.
#[derive(Clone, Copy, Debug)]
pub(crate) struct HeadGate {
    /// Every processor on the lane accepts unattested frames.
    pub(crate) enabled: bool,
    /// The beacon slot in progress.
    pub(crate) wall_slot: u64,
}

impl HeadGate {
    pub(crate) fn allows(self, attested: Option<AttestedHead>) -> bool {
        self.enabled
            && attested.is_some_and(|attested| {
                self.wall_slot.saturating_sub(attested.beacon_slot) <= MAX_UNATTESTED_SLOTS
            })
    }
}

/// Distinct peers that served each header for one block number.
#[derive(Debug, Default)]
pub(crate) struct HeadVotes {
    block: Option<BlockNumber>,
    by_hash: HashMap<B256, (Header, HashSet<B512>)>,
}

impl HeadVotes {
    /// Record `peer`'s header for `block`. Returns it once `quorum` distinct
    /// peers served the same one.
    pub(crate) fn record(
        &mut self,
        block: BlockNumber,
        peer: B512,
        header: Header,
        quorum: usize,
    ) -> Option<Header> {
        if self.block != Some(block) {
            self.block = Some(block);
            self.by_hash.clear();
        }
        let (agreed, peers) = self
            .by_hash
            .entry(header.hash_slow())
            .or_insert_with(|| (header, HashSet::new()));
        peers.insert(peer);
        (peers.len() >= quorum).then(|| agreed.clone())
    }
}

/// What an attested head means for the unattested tail.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Attestation {
    /// No tail, or the head is outside it.
    None,
    /// The head is a delivered block: it and everything below are attested.
    Confirm(BlockRef),
    /// The attested chain holds another block at a tail height.
    Conflict {
        number: BlockNumber,
        hash: BlockHash,
    },
}

/// Blocks the lane delivered unattested, oldest first. They are always a
/// contiguous suffix of the delivered chain.
#[derive(Debug, Default)]
pub(crate) struct UnattestedTail(VecDeque<BlockRef>);

impl UnattestedTail {
    pub(crate) fn push(&mut self, block: BlockRef) {
        self.0.push_back(block);
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn newest(&self) -> Option<BlockRef> {
        self.0.back().copied()
    }

    pub(crate) fn clear(&mut self) {
        self.0.clear();
    }

    pub(crate) fn attestation(&self, attested: AttestedHead) -> Attestation {
        match self
            .0
            .iter()
            .find(|block| block.number == attested.block_number)
        {
            Some(block) if block.hash == attested.block_hash => Attestation::Confirm(*block),
            Some(_) => Attestation::Conflict {
                number: attested.block_number,
                hash: attested.block_hash,
            },
            None => Attestation::None,
        }
    }

    pub(crate) fn confirm_through(&mut self, number: BlockNumber) {
        while self.0.front().is_some_and(|block| block.number <= number) {
            self.0.pop_front();
        }
    }

    /// The whole tail, tip first, once its oldest block's slot started more
    /// than `HEAD_UNDO_GRACE` before `now`.
    pub(crate) fn expired(&self, now: u64) -> Option<Vec<BlockRef>> {
        let oldest = self.0.front()?;
        (now.saturating_sub(oldest.timestamp) > HEAD_UNDO_GRACE.as_secs())
            .then(|| self.0.iter().rev().copied().collect())
    }

    /// Whether every block in `reverted` is in the tail: only then may a reorg
    /// that rests on unverified evidence undo them.
    pub(crate) fn contains_all(&self, reverted: &[BlockRef]) -> bool {
        reverted.iter().all(|block| self.0.contains(block))
    }

    pub(crate) fn remove(&mut self, reverted: &[BlockRef]) {
        self.0.retain(|block| !reverted.contains(block));
    }
}

/// Whether a block first seen at `seen` (UNIX seconds) came later than
/// `LATE_REVEAL_AFTER` into its slot.
pub(crate) const fn late_reveal(slot_start: u64, seen: u64) -> bool {
    seen.saturating_sub(slot_start) > LATE_REVEAL_AFTER.as_secs()
}

/// How long to wait before polling the block after one at `last_timestamp`,
/// at `now_ms` UNIX milliseconds.
pub(crate) fn head_poll_delay(last_timestamp: u64, now_ms: u64) -> Duration {
    let first_poll_ms = (last_timestamp + SLOT_SECONDS) * 1_000
        + u64::try_from(HEAD_FIRST_POLL_OFFSET.as_millis()).unwrap_or(u64::MAX);
    Duration::from_millis(first_poll_ms.saturating_sub(now_ms))
}

/// The header a P2P frame carries, decoded from its RLP.
pub(crate) fn frame_header(frame: &BlockFrame) -> Option<Header> {
    let Material::Complete(envelope) = &frame.header else {
        return None;
    };
    let rlp = envelope.rlp.as_deref()?;
    alloy_rlp::Decodable::decode(&mut &rlp[..]).ok()
}

pub(crate) fn unix_now_ms() -> u64 {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

pub(crate) fn unix_now() -> u64 {
    unix_now_ms() / 1_000
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block(number: u64, hash: u8, timestamp: u64) -> BlockRef {
        BlockRef {
            number: BlockNumber(number),
            hash: BlockHash::new([hash; 32]),
            parent_hash: BlockHash::new([hash.wrapping_sub(1); 32]),
            timestamp,
        }
    }

    fn attested(number: u64, hash: u8, beacon_slot: u64) -> AttestedHead {
        AttestedHead {
            beacon_slot,
            beacon_block_root: [0; 32],
            block_number: BlockNumber(number),
            block_hash: BlockHash::new([hash; 32]),
        }
    }

    #[test]
    fn the_gate_counts_slots_ahead_of_the_attested_head() {
        let gate = |enabled, wall_slot| HeadGate { enabled, wall_slot };
        let head = Some(attested(100, 1, 1_000));
        assert!(gate(true, 1_002).allows(head));
        assert!(
            !gate(true, 1_003).allows(head),
            "three slots past the attested head"
        );
        assert!(
            !gate(false, 1_001).allows(head),
            "a processor needs attested frames"
        );
        assert!(!gate(true, 1_001).allows(None), "no attested anchor yet");
    }

    #[test]
    fn votes_need_distinct_peers_on_one_header() {
        let mut votes = HeadVotes::default();
        let header = Header {
            number: 7,
            ..Header::default()
        };
        let other = Header {
            number: 7,
            gas_limit: 1,
            ..Header::default()
        };
        let (first, second) = (B512::repeat_byte(1), B512::repeat_byte(2));
        assert_eq!(votes.record(BlockNumber(7), first, header.clone(), 2), None);
        assert_eq!(
            votes.record(BlockNumber(7), first, header.clone(), 2),
            None,
            "same peer twice"
        );
        assert_eq!(
            votes.record(BlockNumber(7), second, other, 2),
            None,
            "different header"
        );
        assert_eq!(
            votes.record(BlockNumber(7), second, header.clone(), 2),
            Some(header)
        );
        assert_eq!(
            votes.record(
                BlockNumber(8),
                first,
                Header {
                    number: 8,
                    ..Header::default()
                },
                2
            ),
            None,
            "a new block starts over"
        );
    }

    #[test]
    fn an_attested_head_confirms_or_contradicts_the_tail() {
        let mut tail = UnattestedTail::default();
        tail.push(block(101, 1, 0));
        tail.push(block(102, 2, 12));
        assert_eq!(
            tail.attestation(attested(100, 9, 5)),
            Attestation::None,
            "below the tail"
        );
        assert_eq!(
            tail.attestation(attested(101, 1, 5)),
            Attestation::Confirm(block(101, 1, 0))
        );
        assert_eq!(
            tail.attestation(attested(102, 7, 6)),
            Attestation::Conflict {
                number: BlockNumber(102),
                hash: BlockHash::new([7; 32])
            }
        );
        assert_eq!(
            tail.attestation(attested(103, 3, 7)),
            Attestation::None,
            "above: the attested path proves it"
        );
        // The fork-boundary repeat: the same hash attested twice is a no-op.
        tail.confirm_through(BlockNumber(101));
        assert_eq!(tail.attestation(attested(101, 1, 6)), Attestation::None);
    }

    #[test]
    fn the_tail_expires_whole_after_the_grace() {
        let mut tail = UnattestedTail::default();
        tail.push(block(101, 1, 1_000));
        tail.push(block(102, 2, 1_012));
        assert_eq!(tail.expired(1_000 + HEAD_UNDO_GRACE.as_secs()), None);
        assert_eq!(
            tail.expired(1_001 + HEAD_UNDO_GRACE.as_secs()),
            Some(vec![block(102, 2, 1_012), block(101, 1, 1_000)]),
            "tip first"
        );
    }

    #[test]
    fn unverified_evidence_undoes_only_the_tail() {
        let mut tail = UnattestedTail::default();
        tail.push(block(101, 1, 0));
        assert!(tail.contains_all(&[block(101, 1, 0)]));
        assert!(!tail.contains_all(&[block(101, 1, 0), block(100, 0, 0)]));
    }

    #[test]
    fn late_reveals_wait_and_polls_follow_the_slot() {
        assert!(!late_reveal(1_000, 1_007));
        assert!(late_reveal(1_000, 1_008));
        // The next block's slot starts at 1_012; polls start a second in.
        assert_eq!(
            head_poll_delay(1_000, 1_005_000),
            Duration::from_millis(8_000)
        );
        assert_eq!(head_poll_delay(1_000, 1_014_000), Duration::ZERO);
    }
}
