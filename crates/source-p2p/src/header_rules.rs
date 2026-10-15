//! Rules a peer-served execution header must pass before the live lane
//! accepts it unattested. Any peer can serve a self-consistent block above
//! the chain tip. These rules, the parent the lane holds, and peer agreement
//! are what the `unattested` level rests on.

use std::sync::Arc;

use alloy_consensus::Header;
use alloy_eips::eip4844::DATA_GAS_PER_BLOB;
use leani_primitives::slot_starting_at;
use leani_source_api::AttestedHead;
use reth_chainspec::ChainSpec;
use reth_consensus::{ConsensusError, HeaderValidator};
use reth_ethereum_consensus::EthBeaconConsensus;
use reth_primitives_traits::SealedHeader;

use crate::live_head::{MAX_UNATTESTED_SLOTS, late_reveal};

/// How far ahead of the local clock a header's slot may start.
// ponytail: fixed clock-skew allowance; make it config if operators run skewed clocks.
const MAX_CLOCK_SKEW_SECONDS: u64 = 2;

/// The verdict on one header. No verdict bans a peer: the local rule set
/// may be the stale side, as a pre-fork rule is after a fork.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum HeaderVerdict {
    Valid,
    /// The header carries fields of a fork the chain spec does not schedule,
    /// such as Amsterdam's before its mainnet timestamp is configured. The
    /// block waits for an attested head instead.
    UnknownFork(String),
    Invalid(String),
}

#[derive(Debug)]
pub(crate) struct HeaderRules {
    consensus: EthBeaconConsensus<ChainSpec>,
}

impl HeaderRules {
    pub(crate) fn new(chain_spec: Arc<ChainSpec>) -> Self {
        Self {
            consensus: EthBeaconConsensus::new(chain_spec),
        }
    }

    /// Judge `header` as the child of `parent` at `now` UNIX seconds.
    /// `attested_parent_root` is the attested beacon root of the slot before
    /// the header's, when the newest attested head is exactly that slot.
    pub(crate) fn check(
        &self,
        header: &Header,
        parent: &Header,
        now: u64,
        attested_parent_root: Option<[u8; 32]>,
    ) -> HeaderVerdict {
        let Some(slot) = slot_starting_at(header.timestamp) else {
            return HeaderVerdict::Invalid(format!(
                "timestamp {} is not a slot start",
                header.timestamp
            ));
        };
        if header.timestamp > now.saturating_add(MAX_CLOCK_SKEW_SECONDS) {
            return HeaderVerdict::Invalid(format!("slot {slot} has not started"));
        }
        // EIP-7843, from Amsterdam: the slot number restates the timestamp
        // and rises with every block.
        if let Some(slot_number) = header.slot_number {
            if slot_number != slot {
                return HeaderVerdict::Invalid(format!(
                    "slot number {slot_number} is not the timestamp's slot {slot}"
                ));
            }
            if parent
                .slot_number
                .is_some_and(|parent| slot_number <= parent)
            {
                return HeaderVerdict::Invalid(format!(
                    "slot number {slot_number} does not follow its parent's"
                ));
            }
        }
        if let (Some(expected), Some(root)) =
            (attested_parent_root, header.parent_beacon_block_root)
            && root.0 != expected
        {
            return HeaderVerdict::Invalid(
                "parent beacon block root is not the attested one".to_owned(),
            );
        }
        // ponytail: block_access_list_hash needs the BAL (eth/71) and
        // gas_limit == bid.gas_limit an authenticated bid; neither is checked.
        let header = SealedHeader::seal_slow(header.clone());
        let parent = SealedHeader::seal_slow(parent.clone());
        match self.consensus.validate_header(&header).and_then(|()| {
            self.consensus
                .validate_header_against_parent(&header, &parent)
        }) {
            Ok(()) => HeaderVerdict::Valid,
            Err(
                error @ (ConsensusError::SlotNumberUnexpected
                | ConsensusError::BlockAccessListHashUnexpected),
            ) => HeaderVerdict::UnknownFork(error.to_string()),
            Err(error) => HeaderVerdict::Invalid(error.to_string()),
        }
    }
}

impl HeaderRules {
    /// Whether a branch that peers serve without verified evidence may join
    /// the unattested tail, judged before any body is fetched. `headers` runs
    /// from the common ancestor up, the ancestor first. Every later header
    /// must pass the rules against its parent and start a slot at most
    /// `MAX_UNATTESTED_SLOTS` past the newest attested one, and the tip must
    /// not be a late reveal: the head path's checks, for the whole branch.
    pub(crate) fn accepts_branch(
        &self,
        headers: &[Header],
        attested: Option<AttestedHead>,
        now: u64,
    ) -> bool {
        let (Some(attested), Some(tip)) = (attested, headers.last()) else {
            return false;
        };
        headers.len() >= 2
            && !late_reveal(tip.timestamp, now)
            && headers.windows(2).all(|pair| {
                slot_starting_at(pair[1].timestamp).is_some_and(|slot| {
                    slot.saturating_sub(attested.beacon_slot) <= MAX_UNATTESTED_SLOTS
                }) && self.check(&pair[1], &pair[0], now, None) == HeaderVerdict::Valid
            })
    }
}

/// Whether a header's blob gas pays for exactly the blobs its body carries.
pub(crate) fn blob_gas_matches(header: &Header, blobs: u64) -> bool {
    header.blob_gas_used.unwrap_or(0) == blobs.saturating_mul(DATA_GAS_PER_BLOB)
}

#[cfg(test)]
mod tests {
    use alloy_consensus::{EMPTY_OMMER_ROOT_HASH, constants::EMPTY_ROOT_HASH};
    use alloy_eips::eip7685::EMPTY_REQUESTS_HASH;
    use alloy_primitives::{B64, B256, U256};
    use leani_primitives::{MAINNET_GENESIS_TIME, SLOT_SECONDS};
    use reth_chainspec::{ChainSpecBuilder, EthChainSpec, MAINNET};

    use super::*;

    /// A post-Osaka mainnet slot (2026-05-28), so every scheduled blob
    /// parameter set applies.
    const SLOT: u64 = 14_431_332;

    fn parent() -> Header {
        Header {
            number: 24_000_000,
            timestamp: MAINNET_GENESIS_TIME + SLOT * SLOT_SECONDS,
            gas_limit: 45_000_000,
            gas_used: 22_500_000,
            base_fee_per_gas: Some(1_000_000_000),
            withdrawals_root: Some(EMPTY_ROOT_HASH),
            blob_gas_used: Some(0),
            excess_blob_gas: Some(0),
            parent_beacon_block_root: Some(B256::repeat_byte(0x11)),
            requests_hash: Some(EMPTY_REQUESTS_HASH),
            ommers_hash: EMPTY_OMMER_ROOT_HASH,
            difficulty: U256::ZERO,
            nonce: B64::ZERO,
            ..Header::default()
        }
    }

    /// The protocol-valid child of `parent` on `spec`, one slot later.
    fn child_of(parent: &Header, spec: &ChainSpec) -> Header {
        let timestamp = parent.timestamp + SLOT_SECONDS;
        let blob_params = spec
            .blob_params_at_timestamp(timestamp)
            .expect("blob parameters");
        Header {
            parent_hash: parent.hash_slow(),
            number: parent.number + 1,
            timestamp,
            base_fee_per_gas: spec.next_block_base_fee(parent, timestamp),
            excess_blob_gas: Some(blob_params.next_block_excess_blob_gas_osaka(
                parent.excess_blob_gas.unwrap_or(0),
                parent.blob_gas_used.unwrap_or(0),
                parent.base_fee_per_gas.unwrap_or(0),
            )),
            slot_number: parent.slot_number.map(|slot| slot + 1),
            ..parent.clone()
        }
    }

    fn rules() -> HeaderRules {
        HeaderRules::new(MAINNET.clone())
    }

    #[test]
    fn a_protocol_valid_child_passes() {
        let parent = parent();
        let child = child_of(&parent, &MAINNET);
        assert_eq!(
            rules().check(&child, &parent, child.timestamp + 1, None),
            HeaderVerdict::Valid
        );
    }

    #[test]
    fn slot_alignment_and_the_clock_bound_the_timestamp() {
        let parent = parent();
        let mut off_slot = child_of(&parent, &MAINNET);
        off_slot.timestamp += 1;
        assert!(matches!(
            rules().check(&off_slot, &parent, off_slot.timestamp + 1, None),
            HeaderVerdict::Invalid(_)
        ));
        let child = child_of(&parent, &MAINNET);
        assert!(matches!(
            rules().check(&child, &parent, child.timestamp - 5, None),
            HeaderVerdict::Invalid(_)
        ));
    }

    #[test]
    fn a_forged_base_fee_is_invalid() {
        let parent = parent();
        let mut child = child_of(&parent, &MAINNET);
        child.base_fee_per_gas = child.base_fee_per_gas.map(|fee| fee + 1);
        assert!(matches!(
            rules().check(&child, &parent, child.timestamp + 1, None),
            HeaderVerdict::Invalid(_)
        ));
    }

    #[test]
    fn the_parent_beacon_root_must_match_an_attested_one() {
        let parent = parent();
        let child = child_of(&parent, &MAINNET);
        assert_eq!(
            rules().check(&child, &parent, child.timestamp + 1, Some([0x11; 32])),
            HeaderVerdict::Valid
        );
        assert!(matches!(
            rules().check(&child, &parent, child.timestamp + 1, Some([0x22; 32])),
            HeaderVerdict::Invalid(_)
        ));
    }

    #[test]
    fn amsterdam_header_on_a_pre_amsterdam_spec_is_unknown_fork() {
        let parent = parent();
        let mut child = child_of(&parent, &MAINNET);
        child.block_access_list_hash = Some(B256::ZERO);
        child.slot_number = Some(SLOT + 1);
        assert!(matches!(
            rules().check(&child, &parent, child.timestamp + 1, None),
            HeaderVerdict::UnknownFork(_)
        ));
    }

    #[test]
    fn amsterdam_slot_numbers_follow_the_timestamp_and_the_parent() {
        let spec = Arc::new(ChainSpecBuilder::mainnet().amsterdam_activated().build());
        let rules = HeaderRules::new(spec.clone());
        let mut parent = parent();
        parent.block_access_list_hash = Some(B256::ZERO);
        parent.slot_number = Some(SLOT);
        let child = child_of(&parent, &spec);
        assert_eq!(
            rules.check(&child, &parent, child.timestamp + 1, None),
            HeaderVerdict::Valid
        );
        let mut stale = child.clone();
        stale.slot_number = Some(SLOT);
        assert!(matches!(
            rules.check(&stale, &parent, child.timestamp + 1, None),
            HeaderVerdict::Invalid(_)
        ));
    }

    fn attested_at(beacon_slot: u64) -> leani_source_api::AttestedHead {
        leani_source_api::AttestedHead {
            beacon_slot,
            beacon_block_root: [0; 32],
            block_number: leani_primitives::BlockNumber(0),
            block_hash: leani_primitives::BlockHash::ZERO,
        }
    }

    #[test]
    fn an_unverified_branch_passes_the_rules_the_head_path_does() {
        let ancestor = parent();
        let child = child_of(&ancestor, &MAINNET);
        let grandchild = child_of(&child, &MAINNET);
        let branch = [ancestor.clone(), child.clone(), grandchild.clone()];
        let now = grandchild.timestamp + 1;
        assert!(rules().accepts_branch(&branch, Some(attested_at(SLOT)), now));
        // No attested anchor, or one too far behind the branch's tip.
        assert!(!rules().accepts_branch(&branch, None, now));
        assert!(
            !rules().accepts_branch(&branch, Some(attested_at(SLOT - 1)), now),
            "the tip is three slots past the attested head"
        );
        // A late tip waits for the attested path.
        assert!(!rules().accepts_branch(
            &branch,
            Some(attested_at(SLOT)),
            grandchild.timestamp + 8
        ));
        // Any applied header that breaks a rule rejects the branch.
        let mut forged = child;
        forged.base_fee_per_gas = forged.base_fee_per_gas.map(|fee| fee + 1);
        let after_forged = child_of(&forged, &MAINNET);
        assert!(!rules().accepts_branch(
            &[ancestor, forged, after_forged],
            Some(attested_at(SLOT)),
            now
        ));
    }

    #[test]
    fn blob_gas_pays_for_the_body_blobs() {
        let mut header = parent();
        header.blob_gas_used = Some(2 * 131_072);
        assert!(blob_gas_matches(&header, 2));
        assert!(!blob_gas_matches(&header, 1));
    }
}
