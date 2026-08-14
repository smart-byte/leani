//! Small deterministic normalized fixtures.

use std::time::Duration;

use leani_primitives::{
    BlockFrame, BlockHash, BlockNumber, BlockRange, BlockRef, Capability, CapabilitySet, ChainId,
    Finality, Material, MissingReason, SourceId, SourceKind, TrustModel, VerificationReport,
};
use leani_source_api::{FinalityModel, Partitioning, SourceBudget, SourceDescriptor};

#[must_use]
pub fn fixture_frame(number: u64, parent_hash: BlockHash) -> BlockFrame {
    BlockFrame {
        chain_id: ChainId(1),
        block: BlockRef {
            number: BlockNumber(number),
            hash: BlockHash::new([u8::try_from(number).unwrap_or(u8::MAX); 32]),
            parent_hash,
            timestamp: 1_700_000_000 + number,
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
    }
}
