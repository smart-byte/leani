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
///
/// Decoding rejects bits that name no [`LogField`].
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct LogFieldSet(u8);

impl<'de> Deserialize<'de> for LogFieldSet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let bits = u8::deserialize(deserializer)?;
        if bits & !Self::ALL.0 == 0 {
            Ok(Self(bits))
        } else {
            Err(serde::de::Error::custom(format!(
                "unknown log field bits 0x{:02x}",
                bits & !Self::ALL.0
            )))
        }
    }
}

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
    ///
    /// The serde default only lets self-describing input written before this
    /// field existed, such as JSON-lines history archives, omit it. Durable
    /// postcard frames always encode every field, so it adds no compatibility
    /// for them.
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
    /// Sender address, which Body/Calldata projections may omit. Request
    /// [`Capability::Transactions`] when this field is needed.
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

    /// The first field a requested group promises that this frame lacks.
    ///
    /// Groups guarantee fields only on components whose material is present:
    /// whether material is present at all is [`Self::capabilities`]'s job.
    /// Pass `groups` with derivable groups already added
    /// ([`CapabilitySet::with_derivable`]). The checks are chain-agnostic:
    /// fork-dependent header fields, such as the base fee, are not checked.
    #[must_use]
    pub fn group_violation(&self, groups: CapabilitySet) -> Option<&'static str> {
        let header = self.header.as_present();
        let transactions = self
            .transactions
            .as_present()
            .map_or(&[][..], Vec::as_slice);
        let receipts = self.receipts.as_present();
        if groups.contains(Capability::Header)
            && header.is_some_and(|header| header.gas_limit.is_none() || header.gas_used.is_none())
        {
            return Some("header lacks gas limit or gas used");
        }
        if groups.contains(Capability::Body) {
            if transactions.iter().any(|transaction| {
                transaction.nonce.is_none()
                    || transaction.gas_limit.is_none()
                    || transaction.value.is_none()
            }) {
                return Some("transactions lack body fields");
            }
            if header.is_some_and(|header| header.transaction_count.is_none()) {
                return Some("header lacks the transaction count");
            }
        }
        if groups.contains(Capability::Calldata)
            && transactions
                .iter()
                .any(|transaction| transaction.input.is_none())
        {
            return Some("transactions lack calldata");
        }
        if groups.contains(Capability::Senders)
            && transactions
                .iter()
                .any(|transaction| transaction.from.is_none())
        {
            return Some("transactions lack senders");
        }
        if groups.contains(Capability::BlobFields) {
            if transactions.iter().any(|transaction| {
                transaction.transaction_type == 3
                    && (transaction.blob_versioned_hashes.is_empty()
                        || transaction.max_fee_per_blob_gas.is_none())
            }) {
                return Some("blob transactions lack blob fields");
            }
            if receipts.is_some_and(|receipts| {
                receipts
                    .iter()
                    .any(|receipt| receipt.transaction_type == 3 && receipt.blob_gas_used.is_none())
            }) {
                return Some("blob receipts lack blob gas used");
            }
        }
        if groups.contains(Capability::ReceiptOutcome)
            && let Some(receipts) = receipts
        {
            if receipts.iter().any(|receipt| {
                receipt.success.is_none()
                    || receipt.gas_used.is_none()
                    || receipt.effective_gas_price.is_none()
            }) {
                return Some("receipts lack outcome fields");
            }
            if self.transactions.is_present() && !receipts_match(transactions, receipts) {
                return Some("receipts do not match the transactions one to one");
            }
        }
        if groups.contains(Capability::BlockSize)
            && let Some(header) = header
        {
            if header.size_bytes.is_none() {
                return Some("header lacks the block size");
            }
            if header.transaction_count.is_none() {
                return Some("header lacks the transaction count");
            }
        }
        None
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

/// Whether `receipts` hold exactly one receipt per transaction, in order.
fn receipts_match(transactions: &[TransactionEnvelope], receipts: &[ReceiptEnvelope]) -> bool {
    transactions.len() == receipts.len()
        && transactions
            .iter()
            .zip(receipts)
            .all(|(transaction, receipt)| {
                receipt.transaction_hash == transaction.hash
                    && receipt.transaction_index == transaction.index
                    && receipt.transaction_type == transaction.transaction_type
            })
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
            finality: Finality::Included,
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

    fn grouped_transaction(index: u8, transaction_type: u8) -> TransactionEnvelope {
        let blob = transaction_type == 3;
        TransactionEnvelope {
            hash: TransactionHash::new([index + 1; 32]),
            transaction_type,
            index: u32::from(index),
            encoded: None,
            from: Some(Address::new([3; 20])),
            // a contract creation: `to` is None as a value, never as "not supplied"
            to: None,
            nonce: Some(1),
            gas_limit: Some(21_000),
            value: Some(Quantity::new([0; 32])),
            input: Some(Vec::new()),
            max_fee_per_gas: None,
            max_priority_fee_per_gas: None,
            max_fee_per_blob_gas: blob.then(|| Quantity::new([0; 32])),
            blob_versioned_hashes: if blob {
                vec![BlockHash::new([9; 32])]
            } else {
                Vec::new()
            },
            size_bytes: None,
        }
    }

    fn grouped_receipt(transaction: &TransactionEnvelope) -> ReceiptEnvelope {
        ReceiptEnvelope {
            transaction_hash: transaction.hash,
            transaction_type: transaction.transaction_type,
            transaction_index: transaction.index,
            encoded: None,
            // a reverted transaction still has an outcome
            success: Some(false),
            gas_used: Some(21_000),
            effective_gas_price: Some(Quantity::new([0; 32])),
            blob_gas_used: (transaction.transaction_type == 3).then_some(131_072),
            blob_gas_price: None,
            logs: Vec::new(),
        }
    }

    /// A frame filling every field group: one type-2 and one type-3 transaction.
    fn grouped_frame() -> BlockFrame {
        let transactions = vec![grouped_transaction(0, 2), grouped_transaction(1, 3)];
        let receipts = transactions.iter().map(grouped_receipt).collect();
        BlockFrame {
            header: Material::Complete(HeaderEnvelope {
                rlp: None,
                transactions_root: None,
                receipts_root: None,
                withdrawals_root: None,
                gas_limit: Some(30_000_000),
                gas_used: Some(42_000),
                base_fee_per_gas: None,
                blob_gas_used: None,
                excess_blob_gas: None,
                size_bytes: Some(1_000),
                transaction_count: Some(2),
                consensus_size_bytes: None,
            }),
            transactions: Material::Complete(transactions),
            receipts: Material::Complete(receipts),
            ..frame()
        }
    }

    const FIELD_GROUPS: CapabilitySet = CapabilitySet::of(Capability::Header)
        .with(Capability::Body)
        .with(Capability::Calldata)
        .with(Capability::Senders)
        .with(Capability::BlobFields)
        .with(Capability::ReceiptOutcome)
        .with(Capability::BlockSize);

    fn header(frame: &mut BlockFrame) -> &mut HeaderEnvelope {
        match &mut frame.header {
            Material::Complete(header) => header,
            _ => unreachable!("grouped_frame() has a complete header"),
        }
    }

    fn transactions(frame: &mut BlockFrame) -> &mut Vec<TransactionEnvelope> {
        match &mut frame.transactions {
            Material::Complete(transactions) => transactions,
            _ => unreachable!("grouped_frame() has complete transactions"),
        }
    }

    fn receipts(frame: &mut BlockFrame) -> &mut Vec<ReceiptEnvelope> {
        match &mut frame.receipts {
            Material::Complete(receipts) => receipts,
            _ => unreachable!("grouped_frame() has complete receipts"),
        }
    }

    #[test]
    fn a_frame_filling_every_group_passes() {
        assert_eq!(grouped_frame().group_violation(FIELD_GROUPS), None);
    }

    /// A group, a way to break its promise, and the violation it reports.
    type BrokenPromise = (Capability, fn(&mut BlockFrame), &'static str);

    #[test]
    fn each_group_names_the_field_it_misses() {
        let cases: [BrokenPromise; 11] = [
            (
                Capability::Header,
                |frame| header(frame).gas_used = None,
                "header lacks gas limit or gas used",
            ),
            (
                Capability::Body,
                |frame| transactions(frame)[0].nonce = None,
                "transactions lack body fields",
            ),
            (
                Capability::Body,
                |frame| header(frame).transaction_count = None,
                "header lacks the transaction count",
            ),
            (
                Capability::Calldata,
                |frame| transactions(frame)[0].input = None,
                "transactions lack calldata",
            ),
            (
                Capability::Senders,
                |frame| transactions(frame)[1].from = None,
                "transactions lack senders",
            ),
            (
                Capability::BlobFields,
                |frame| transactions(frame)[1].blob_versioned_hashes.clear(),
                "blob transactions lack blob fields",
            ),
            (
                Capability::BlobFields,
                |frame| receipts(frame)[1].blob_gas_used = None,
                "blob receipts lack blob gas used",
            ),
            (
                Capability::ReceiptOutcome,
                |frame| receipts(frame)[0].success = None,
                "receipts lack outcome fields",
            ),
            (
                Capability::ReceiptOutcome,
                |frame| receipts(frame).swap(0, 1),
                "receipts do not match the transactions one to one",
            ),
            (
                Capability::BlockSize,
                |frame| header(frame).size_bytes = None,
                "header lacks the block size",
            ),
            (
                Capability::BlockSize,
                |frame| header(frame).transaction_count = None,
                "header lacks the transaction count",
            ),
        ];
        for (group, break_promise, reason) in cases {
            let mut frame = grouped_frame();
            break_promise(&mut frame);
            assert_eq!(
                frame.group_violation(CapabilitySet::of(group)),
                Some(reason),
                "{group:?}"
            );
            // a group nobody requested is never checked
            assert_eq!(frame.group_violation(CapabilitySet::NONE), None);
        }
    }

    #[test]
    fn groups_promise_nothing_about_absent_material() {
        let mut frame = grouped_frame();
        frame.receipts = Material::Missing(MissingReason::NotRequested);
        frame.header = Material::Missing(MissingReason::NotRequested);
        assert_eq!(frame.group_violation(FIELD_GROUPS), None);
    }

    #[test]
    fn decoding_rejects_unknown_log_field_bits() {
        let encoded = postcard::to_allocvec(&0b10_u8).expect("encode");
        assert!(postcard::from_bytes::<LogFieldSet>(&encoded).is_err());

        let encoded = postcard::to_allocvec(&LogFieldSet::ALL).expect("encode");
        assert_eq!(
            postcard::from_bytes::<LogFieldSet>(&encoded).expect("decode"),
            LogFieldSet::ALL
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
