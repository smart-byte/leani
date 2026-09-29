//! Small deterministic normalized fixtures.

use std::time::Duration;

use leani_primitives::{
    BlockFrame, BlockHash, BlockNumber, BlockRange, BlockRef, Capability, CapabilitySet, ChainId,
    Finality, Material, MissingReason, SourceId, SourceKind, TrustModel, VerificationReport,
};
use leani_source_api::{FinalityModel, Partitioning, SourceBudget, SourceDescriptor};

/// Build a finalized frame for block `number`.
///
/// Each block number has its own hash: `[number; 32]` below 255, and
/// `0xff` bytes followed by the big-endian number from 255 on.
#[must_use]
pub fn fixture_frame(number: u64, parent_hash: BlockHash) -> BlockFrame {
    let hash = match u8::try_from(number) {
        Ok(byte) if byte < u8::MAX => [byte; 32],
        _ => {
            let mut hash = [u8::MAX; 32];
            hash[24..].copy_from_slice(&number.to_be_bytes());
            hash
        }
    };
    BlockFrame {
        chain_id: ChainId(1),
        block: BlockRef {
            number: BlockNumber(number),
            hash: BlockHash::new(hash),
            parent_hash,
            timestamp: 1_700_000_000_u64.saturating_add(number),
        },
        finality: Finality::Finalized,
        header: Material::Missing(MissingReason::NotRequested),
        transactions: Material::Complete(Vec::new()),
        receipts: Material::Complete(Vec::new()),
        logs: Material::Complete(Vec::new()),
        withdrawals: Material::Missing(MissingReason::Unsupported),
        blob_sidecars: Material::Missing(MissingReason::NotRequested),
        traces: Material::Missing(MissingReason::Unsupported),
        state_diffs: Material::Missing(MissingReason::Unsupported),
        provenance: Vec::new(),
        verification: VerificationReport::default(),
    }
}

#[must_use]
/// Build a protocol-verified synthetic descriptor.
///
/// # Panics
///
/// Panics when `id` is not a valid portable [`SourceId`].
pub fn fixture_source_descriptor(id: &str, range: BlockRange) -> SourceDescriptor {
    SourceDescriptor {
        id: SourceId::new(id).expect("fixture source ID is valid"),
        kind: SourceKind::Synthetic,
        chain_id: ChainId(1),
        range: Some(range),
        capabilities: CapabilitySet::from_iter([
            Capability::Transactions,
            Capability::Receipts,
            Capability::Logs,
        ]),
        complete_capabilities: CapabilitySet::from_iter([
            Capability::Transactions,
            Capability::Receipts,
            Capability::Logs,
        ]),
        trust: TrustModel::ProtocolVerified,
        finality: FinalityModel::Finalized,
        partitioning: Partitioning::FixedBlockSpan(100),
        expected_lag: Duration::ZERO,
        schema_version: "fixture-v1".to_owned(),
        priority: 0,
    }
}

#[must_use]
pub const fn default_source_budget() -> SourceBudget {
    SourceBudget {
        max_input_bytes: 1024 * 1024,
        max_frame_bytes: 512 * 1024,
        max_frames: 1_000,
        max_buffered_frames: 2,
        max_in_flight_requests: 1,
        temporary_disk_bytes: 0,
        max_resident_bytes: 1024 * 1024,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn fixture_frames_have_one_hash_per_block_number() {
        // Audit probe (Processor-10): every block from 255 on shared the
        // hash `[0xff; 32]`.
        assert_ne!(
            fixture_frame(255, BlockHash::ZERO).block.hash,
            fixture_frame(256, BlockHash::ZERO).block.hash
        );
        let numbers = [0, 1, 254, 255, 256, 1_000, 1 << 32, u64::MAX];
        let hashes = numbers
            .iter()
            .map(|number| fixture_frame(*number, BlockHash::ZERO).block.hash)
            .collect::<HashSet<_>>();
        assert_eq!(hashes.len(), numbers.len());
        // Hashes below 255 keep their established values.
        assert_eq!(
            fixture_frame(7, BlockHash::ZERO).block.hash,
            BlockHash::new([7; 32])
        );
    }
}
