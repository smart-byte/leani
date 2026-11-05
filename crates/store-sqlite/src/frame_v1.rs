//! Block frames as frame schema 1 encoded them, before headers carried their
//! fee recipient, for the store upgrade that re-encodes recent frames.

use leani_primitives::{
    Address, BlobSidecar, BlockFrame, BlockHash, BlockRef, ChainId, Finality, HeaderEnvelope, Log,
    Material, Provenance, Quantity, ReceiptEnvelope, StateDiff, Trace, TransactionEnvelope,
    VerificationReport, Withdrawal,
};
use serde::Deserialize;

#[derive(Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
pub(crate) struct BlockFrameV1 {
    chain_id: ChainId,
    block: BlockRef,
    finality: Finality,
    header: Material<HeaderEnvelopeV1>,
    transactions: Material<Vec<TransactionEnvelope>>,
    receipts: Material<Vec<ReceiptEnvelope>>,
    logs: Material<Vec<Log>>,
    withdrawals: Material<Vec<Withdrawal>>,
    blob_sidecars: Material<Vec<BlobSidecar>>,
    traces: Material<Vec<Trace>>,
    state_diffs: Material<Vec<StateDiff>>,
    provenance: Vec<Provenance>,
    verification: VerificationReport,
}

#[derive(Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
struct HeaderEnvelopeV1 {
    rlp: Option<Vec<u8>>,
    transactions_root: Option<BlockHash>,
    receipts_root: Option<BlockHash>,
    withdrawals_root: Option<BlockHash>,
    gas_limit: Option<u64>,
    gas_used: Option<u64>,
    base_fee_per_gas: Option<Quantity>,
    blob_gas_used: Option<u64>,
    excess_blob_gas: Option<u64>,
    size_bytes: Option<u64>,
    transaction_count: Option<u32>,
    consensus_size_bytes: Option<u64>,
}

impl BlockFrameV1 {
    /// The frame with its header's fee recipient read from the header's RLP,
    /// or `None` for a header without RLP to read it from.
    pub(crate) fn upgrade(self) -> Option<BlockFrame> {
        let mut readable = true;
        let header = self.header.map(|header| {
            let fee_recipient = header.rlp.as_deref().and_then(beneficiary);
            readable &= fee_recipient.is_some();
            HeaderEnvelope {
                rlp: header.rlp,
                transactions_root: header.transactions_root,
                receipts_root: header.receipts_root,
                withdrawals_root: header.withdrawals_root,
                fee_recipient,
                gas_limit: header.gas_limit,
                gas_used: header.gas_used,
                base_fee_per_gas: header.base_fee_per_gas,
                blob_gas_used: header.blob_gas_used,
                excess_blob_gas: header.excess_blob_gas,
                size_bytes: header.size_bytes,
                transaction_count: header.transaction_count,
                consensus_size_bytes: header.consensus_size_bytes,
            }
        });
        readable.then_some(BlockFrame {
            chain_id: self.chain_id,
            block: self.block,
            finality: self.finality,
            header,
            transactions: self.transactions,
            receipts: self.receipts,
            logs: self.logs,
            withdrawals: self.withdrawals,
            blob_sidecars: self.blob_sidecars,
            traces: self.traces,
            state_diffs: self.state_diffs,
            provenance: self.provenance,
            verification: self.verification,
        })
    }
}

/// The beneficiary of an RLP-encoded execution header: its third field,
/// after the parent and ommers hashes.
fn beneficiary(rlp: &[u8]) -> Option<Address> {
    let (&prefix, rest) = rlp.split_first()?;
    // A header is a list longer than 55 bytes, so its prefix gives the width
    // of the length that follows it.
    let width = prefix.checked_sub(0xf7).filter(|width| *width > 0)?;
    let fields = rest.get(usize::from(width)..)?;
    // 0xa0 starts each 32-byte hash, 0x94 the 20-byte address.
    if fields.first() != Some(&0xa0)
        || fields.get(33) != Some(&0xa0)
        || fields.get(66) != Some(&0x94)
    {
        return None;
    }
    fields
        .get(67..87)
        .and_then(|bytes| <[u8; 20]>::try_from(bytes).ok())
        .map(Address::new)
}
