//! Normalized, field-oriented block material.

use serde::{Deserialize, Serialize};

use crate::{
    Address, BlockHash, BlockRef, Capability, CapabilitySet, ChainId, Finality,
    FrameCapabilityReport, Material, Provenance, Quantity, TransactionHash, VerificationReport,
};

/// Optional log identity fields a processor or RPC surface may require.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[repr(u8)]
pub enum LogField {
    TransactionHash = 0,
}

impl LogField {
    const fn bit(self) -> u8 {
        1 << (self as u8)
    }
}

/// Stable bit representation of optional log identity requirements.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct LogFieldSet(u8);

impl LogFieldSet {
    pub const NONE: Self = Self(0);
    pub const ALL: Self = Self(LogField::TransactionHash.bit());

    #[must_use]
    pub const fn of(field: LogField) -> Self {
        Self(field.bit())
    }

    #[must_use]
    pub const fn with(self, field: LogField) -> Self {
        Self(self.0 | field.bit())
    }

    #[must_use]
    pub const fn contains(self, field: LogField) -> bool {
        self.0 & field.bit() != 0
    }

    #[must_use]
    pub const fn contains_all(self, required: Self) -> bool {
        self.0 & required.0 == required.0
    }

    #[must_use]
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

/// Source-neutral execution-header fields plus optional canonical bytes.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct HeaderEnvelope {
    /// Canonical RLP bytes when a raw source supplied the complete header.
    pub rlp: Option<Vec<u8>>,
    pub transactions_root: Option<BlockHash>,
    pub receipts_root: Option<BlockHash>,
    pub withdrawals_root: Option<BlockHash>,
    pub gas_limit: Option<u64>,
    pub gas_used: Option<u64>,
    pub base_fee_per_gas: Option<Quantity>,
    pub blob_gas_used: Option<u64>,
    pub excess_blob_gas: Option<u64>,
    /// Canonical execution-block RLP bytes, matching Ethereum JSON-RPC `size`.
    pub size_bytes: Option<u64>,
    pub transaction_count: Option<u32>,
    /// Serialized consensus beacon-block bytes when a consensus dataset exposes it.
    ///
    /// This is deliberately distinct from [`Self::size_bytes`]: neither value
    /// may be substituted for the other.
    #[serde(default)]
    pub consensus_size_bytes: Option<u64>,
}

/// Decoded transaction fields plus optional canonical EIP-2718 bytes.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TransactionEnvelope {
    pub hash: TransactionHash,
    pub transaction_type: u8,
    pub index: u32,
    pub encoded: Option<Vec<u8>>,
    pub from: Option<Address>,
    pub to: Option<Address>,
    pub nonce: Option<u64>,
    pub gas_limit: Option<u64>,
    pub value: Option<Quantity>,
    pub input: Option<Vec<u8>>,
    pub max_fee_per_gas: Option<Quantity>,
    pub max_priority_fee_per_gas: Option<Quantity>,
    pub max_fee_per_blob_gas: Option<Quantity>,
    pub blob_versioned_hashes: Vec<BlockHash>,
    pub size_bytes: Option<u32>,
}

/// Decoded receipt fields plus optional canonical typed-receipt bytes.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReceiptEnvelope {
    pub transaction_hash: TransactionHash,
    pub transaction_type: u8,
    pub transaction_index: u32,
    pub encoded: Option<Vec<u8>>,
    pub success: Option<bool>,
    pub gas_used: Option<u64>,
    pub effective_gas_price: Option<Quantity>,
    pub blob_gas_used: Option<u64>,
    pub blob_gas_price: Option<Quantity>,
    pub logs: Vec<Log>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Log {
    pub address: Address,
    pub topics: Vec<[u8; 32]>,
    pub data: Vec<u8>,
    /// Present only when the compiled request requires transaction identity.
    /// Receipt ordering still supplies `transaction_index` and `log_index`
    /// without downloading the corresponding block body.
    pub transaction_hash: Option<TransactionHash>,
    pub transaction_index: u32,
    pub log_index: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Withdrawal {
    pub index: u64,
    pub validator_index: u64,
    pub address: Address,
    pub amount_gwei: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BlobSidecar {
    pub transaction_hash: TransactionHash,
    pub index: u32,
    pub versioned_hash: BlockHash,
    pub blob: Vec<u8>,
    pub commitment: Vec<u8>,
    pub proof: Vec<u8>,
}

/// Opaque trace entry until a processor declares a trace schema.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Trace {
    pub transaction_hash: TransactionHash,
    pub schema: String,
    pub encoded: Vec<u8>,
}

/// Opaque state-diff entry with explicit account and schema.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StateDiff {
    pub address: Address,
    pub schema: String,
    pub encoded: Vec<u8>,
}

/// Source-neutral material for exactly one execution block.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BlockFrame {
    pub chain_id: ChainId,
    pub block: BlockRef,
    pub finality: Finality,
    pub header: Material<HeaderEnvelope>,
    pub transactions: Material<Vec<TransactionEnvelope>>,
    pub receipts: Material<Vec<ReceiptEnvelope>>,
    pub logs: Material<Vec<Log>>,
    pub withdrawals: Material<Vec<Withdrawal>>,
    pub blob_sidecars: Material<Vec<BlobSidecar>>,
    pub traces: Material<Vec<Trace>>,
    pub state_diffs: Material<Vec<StateDiff>>,
    pub provenance: Vec<Provenance>,
    pub verification: VerificationReport,
}

impl BlockFrame {
    /// Report both material presence and independent completeness.
    #[must_use]
    pub fn capabilities(&self) -> FrameCapabilityReport {
        let mut present = CapabilitySet::NONE;
        let mut complete = CapabilitySet::NONE;
        add_material(
            &self.header,
            Capability::Header,
            &mut present,
            &mut complete,
        );
        add_material(
            &self.transactions,
            Capability::Transactions,
            &mut present,
            &mut complete,
        );
        add_material(
            &self.receipts,
            Capability::Receipts,
            &mut present,
            &mut complete,
        );
        add_material(&self.logs, Capability::Logs, &mut present, &mut complete);
        add_material(
            &self.withdrawals,
            Capability::Withdrawals,
            &mut present,
            &mut complete,
        );
        add_material(
            &self.blob_sidecars,
            Capability::BlobSidecars,
            &mut present,
            &mut complete,
        );
        add_material(
            &self.traces,
            Capability::Traces,
            &mut present,
            &mut complete,
        );
        add_material(
            &self.state_diffs,
            Capability::StateDiffs,
            &mut present,
            &mut complete,
        );
        if self.verification.consensus_anchor.is_some() {
            present = present.with(Capability::ConsensusFinality);
            complete = complete.with(Capability::ConsensusFinality);
        }
        FrameCapabilityReport { present, complete }
    }

    /// Reject structurally contradictory frames before verification.
    ///
    /// # Errors
    ///
    /// Returns a static description of the violated invariant.
    pub fn validate_shape(&self) -> Result<(), &'static str> {
        if self.chain_id.0 == 0 {
            return Err("chain ID must be greater than zero");
        }
        if let (Some(transactions), Some(receipts)) =
            (self.transactions.as_complete(), self.receipts.as_complete())
            && transactions.len() != receipts.len()
        {
            return Err("complete transaction and receipt counts differ");
        }
        if self.verification.has_failures() {
            return Err("verification report contains a failed check");
        }
        if let Some(anchor) = &self.verification.consensus_anchor
            && anchor.execution_block_hash != self.block.hash
        {
            return Err("consensus anchor refers to another execution block");
        }
        Ok(())
    }

    #[must_use]
    pub fn estimated_heap_bytes(&self) -> u64 {
        let header = self
            .header
            .as_present()
            .and_then(|header| header.rlp.as_ref())
            .map_or(0_u64, |rlp| dynamic_bytes(rlp.len()));
        let transactions = self.transactions.as_present().map_or(0_u64, |values| {
            vector_elements::<TransactionEnvelope>(values.len()).saturating_add(values.iter().fold(
                0_u64,
                |total, transaction| {
                    total
                        .saturating_add(
                            transaction
                                .encoded
                                .as_ref()
                                .map_or(0_u64, |value| dynamic_bytes(value.len())),
                        )
                        .saturating_add(
                            transaction
                                .input
                                .as_ref()
                                .map_or(0_u64, |value| dynamic_bytes(value.len())),
                        )
                        .saturating_add(vector_elements::<BlockHash>(
                            transaction.blob_versioned_hashes.len(),
                        ))
                },
            ))
        });
        let receipts = self.receipts.as_present().map_or(0_u64, |values| {
            vector_elements::<ReceiptEnvelope>(values.len()).saturating_add(values.iter().fold(
                0_u64,
                |total, receipt| {
                    total
                        .saturating_add(
                            receipt
                                .encoded
                                .as_ref()
                                .map_or(0_u64, |value| dynamic_bytes(value.len())),
                        )
                        .saturating_add(logs_heap_bytes(&receipt.logs))
                },
            ))
        });
        let logs = self
            .logs
            .as_present()
            .map_or(0_u64, |values| logs_heap_bytes(values));
        let withdrawals = self
            .withdrawals
            .as_present()
            .map_or(0_u64, |values| vector_elements::<Withdrawal>(values.len()));
        let sidecars = self.blob_sidecars.as_present().map_or(0_u64, |values| {
            vector_elements::<BlobSidecar>(values.len()).saturating_add(values.iter().fold(
                0_u64,
                |total, sidecar| {
                    total
                        .saturating_add(dynamic_bytes(sidecar.blob.len()))
                        .saturating_add(dynamic_bytes(sidecar.commitment.len()))
                        .saturating_add(dynamic_bytes(sidecar.proof.len()))
                },
            ))
        });
        let traces = self.traces.as_present().map_or(0_u64, |values| {
            vector_elements::<Trace>(values.len()).saturating_add(values.iter().fold(
                0_u64,
                |total, trace| {
                    total
                        .saturating_add(dynamic_bytes(trace.schema.len()))
                        .saturating_add(dynamic_bytes(trace.encoded.len()))
                },
            ))
        });
        let state_diffs = self.state_diffs.as_present().map_or(0_u64, |values| {
            vector_elements::<StateDiff>(values.len()).saturating_add(values.iter().fold(
                0_u64,
                |total, diff| {
                    total
                        .saturating_add(dynamic_bytes(diff.schema.len()))
                        .saturating_add(dynamic_bytes(diff.encoded.len()))
                },
            ))
        });
        header
            .saturating_add(transactions)
            .saturating_add(receipts)
            .saturating_add(logs)
            .saturating_add(withdrawals)
            .saturating_add(sidecars)
            .saturating_add(traces)
            .saturating_add(state_diffs)
    }
}

fn dynamic_bytes(length: usize) -> u64 {
    u64::try_from(length).unwrap_or(u64::MAX)
}

fn vector_elements<T>(length: usize) -> u64 {
    dynamic_bytes(length)
        .saturating_mul(u64::try_from(std::mem::size_of::<T>()).unwrap_or(u64::MAX))
}

fn logs_heap_bytes(logs: &[Log]) -> u64 {
    vector_elements::<Log>(logs.len()).saturating_add(logs.iter().fold(0_u64, |total, log| {
        total
            .saturating_add(vector_elements::<[u8; 32]>(log.topics.len()))
            .saturating_add(dynamic_bytes(log.data.len()))
    }))
}

fn add_material<T>(
    material: &Material<T>,
    capability: Capability,
    present: &mut CapabilitySet,
    complete: &mut CapabilitySet,
) {
    if material.is_present() {
        *present = present.with(capability);
    }
    if material.is_complete() {
        *complete = complete.with(capability);
    }
}

#[cfg(test)]
mod tests {
    use crate::{Completeness, FilterScope, MissingReason};

    use super::*;

    fn block() -> BlockRef {
        BlockRef {
            number: 1.into(),
            hash: BlockHash::new([1; 32]),
            parent_hash: BlockHash::ZERO,
            timestamp: 10,
        }
    }

    fn frame() -> BlockFrame {
        BlockFrame {
            chain_id: 1.into(),
            block: block(),
            finality: Finality::Optimistic,
            header: Material::Missing(MissingReason::NotRequested),
            transactions: Material::Complete(Vec::new()),
            receipts: Material::Complete(Vec::new()),
            logs: Material::Filtered {
                value: Vec::new(),
                scope: FilterScope::default(),
                completeness: Completeness::DatasetDeclared,
            },
            withdrawals: Material::Missing(MissingReason::Unsupported),
            blob_sidecars: Material::Missing(MissingReason::NotRequested),
            traces: Material::Missing(MissingReason::Unsupported),
            state_diffs: Material::Missing(MissingReason::Unsupported),
            provenance: Vec::new(),
            verification: VerificationReport::default(),
        }
    }

    #[test]
    fn capability_report_keeps_filtered_separate() {
        let report = frame().capabilities();
        assert!(report.present.contains(Capability::Logs));
        assert!(!report.complete.contains(Capability::Logs));
        assert!(report.complete.contains(Capability::Transactions));
    }

    #[test]
    fn heap_estimate_includes_filtered_log_storage() {
        let mut frame = frame();
        frame.logs = Material::Filtered {
            value: vec![Log {
                address: Address::new([1; 20]),
                topics: vec![[2; 32], [3; 32]],
                data: vec![4; 128],
                transaction_hash: None,
                transaction_index: 1,
                log_index: 2,
            }],
            scope: FilterScope::default(),
            completeness: Completeness::VerifiedPredicate,
        };
        assert!(frame.estimated_heap_bytes() >= 128 + 64);
    }

    #[test]
    fn contradictory_complete_counts_fail() {
        let mut frame = frame();
        frame.transactions = Material::Complete(vec![TransactionEnvelope {
            hash: TransactionHash::new([2; 32]),
            transaction_type: 2,
            index: 0,
            encoded: Some(vec![2]),
            from: Some(Address::new([3; 20])),
            to: None,
            nonce: Some(1),
            gas_limit: Some(21_000),
            value: Some(Quantity::new([0; 32])),
            input: Some(Vec::new()),
            max_fee_per_gas: None,
            max_priority_fee_per_gas: None,
            max_fee_per_blob_gas: None,
            blob_versioned_hashes: Vec::new(),
            size_bytes: Some(1),
        }]);
        assert_eq!(
            frame.validate_shape(),
            Err("complete transaction and receipt counts differ")
        );
    }

    #[test]
    fn complete_frame_round_trips_durably() {
        let frame = frame();
        let encoded = crate::durable::encode(
            crate::DurableKind::BlockFrame,
            crate::BLOCK_FRAME_SCHEMA_VERSION,
            &frame,
        )
        .expect("encode frame");
        let decoded: BlockFrame = crate::durable::decode(
            crate::DurableKind::BlockFrame,
            crate::BLOCK_FRAME_SCHEMA_VERSION,
            &encoded,
        )
        .expect("decode frame");
        assert_eq!(decoded, frame);
    }
}
