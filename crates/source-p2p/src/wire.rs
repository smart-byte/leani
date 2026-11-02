//! Execution wire types of every family leani follows over devp2p: Ethereum
//! transactions and receipts, plus the op-stack deposit (`0x7E`) and
//! post-exec (`0x7D`) types. Decoding dispatches on the EIP-2718 type byte.
//! Roots still commit to the exact bytes; which types a chain allows is a
//! family rule checked after decoding.

use alloy_consensus::{
    Eip658Value, Eip2718EncodableReceipt, Header, ReceiptWithBloom, RlpDecodableReceipt,
    RlpEncodableReceipt, Transaction, TxReceipt, Typed2718,
    transaction::{SignerRecoverable, TxHashRef},
};
use alloy_eips::{
    eip2718::{Decodable2718, Eip2718Result, Encodable2718, IsTyped2718},
    eip2930::AccessList,
    eip7702::SignedAuthorization,
};
use alloy_primitives::{Address, B256, Bloom, Bytes, ChainId, Log, TxHash, TxKind, U256};
use alloy_rlp::{BufMut, Decodable, Encodable};
use op_alloy_consensus::{DEPOSIT_TX_TYPE_ID, OpReceipt, OpTxEnvelope, POST_EXEC_TX_TYPE_ID};
use reth_ethereum_primitives::{Receipt as EthereumReceipt, TransactionSigned};
use reth_network::types::NewBlock;
use reth_primitives_traits::InMemorySize;
use serde::{Deserialize, Serialize};

pub(crate) type BlockBody = alloy_consensus::BlockBody<WireTx>;
pub(crate) type Block = alloy_consensus::Block<WireTx>;
pub(crate) type Receipt = WireReceipt;

const fn is_op_stack_type(ty: u8) -> bool {
    ty == DEPOSIT_TX_TYPE_ID || ty == POST_EXEC_TX_TYPE_ID
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub(crate) struct WireNetworkPrimitives;

impl reth_network::NetworkPrimitives for WireNetworkPrimitives {
    type BlockHeader = Header;
    type BlockBody = BlockBody;
    type Block = Block;
    type BroadcastedTransaction = WireTx;
    type PooledTransaction = WireTx;
    type Receipt = WireReceipt;
    type NewBlockPayload = NewBlock<Block>;
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(untagged)]
pub(crate) enum WireTx {
    Ethereum(TransactionSigned),
    OpStack(OpTxEnvelope),
}

macro_rules! each_tx {
    ($value:expr, $tx:ident => $body:expr) => {
        match $value {
            WireTx::Ethereum($tx) => $body,
            WireTx::OpStack($tx) => $body,
        }
    };
}

impl WireTx {
    /// The EIP-2718 type byte.
    #[must_use]
    pub(crate) fn tx_type(&self) -> u8 {
        self.ty()
    }

    /// Whether this is an op-stack system transaction type.
    #[must_use]
    pub(crate) const fn is_op_stack(&self) -> bool {
        matches!(self, Self::OpStack(_))
    }
}

impl Typed2718 for WireTx {
    fn ty(&self) -> u8 {
        each_tx!(self, tx => tx.ty())
    }
}

impl IsTyped2718 for WireTx {
    fn is_type(ty: u8) -> bool {
        <TransactionSigned as IsTyped2718>::is_type(ty) || is_op_stack_type(ty)
    }
}

impl Encodable2718 for WireTx {
    fn type_flag(&self) -> Option<u8> {
        each_tx!(self, tx => tx.type_flag())
    }

    fn encode_2718_len(&self) -> usize {
        each_tx!(self, tx => tx.encode_2718_len())
    }

    fn encode_2718(&self, out: &mut dyn BufMut) {
        each_tx!(self, tx => tx.encode_2718(out));
    }

    fn trie_hash(&self) -> B256 {
        each_tx!(self, tx => tx.trie_hash())
    }
}

impl Decodable2718 for WireTx {
    fn typed_decode(ty: u8, buf: &mut &[u8]) -> Eip2718Result<Self> {
        if is_op_stack_type(ty) {
            OpTxEnvelope::typed_decode(ty, buf).map(Self::OpStack)
        } else {
            TransactionSigned::typed_decode(ty, buf).map(Self::Ethereum)
        }
    }

    fn fallback_decode(buf: &mut &[u8]) -> Eip2718Result<Self> {
        TransactionSigned::fallback_decode(buf).map(Self::Ethereum)
    }
}

impl Encodable for WireTx {
    fn encode(&self, out: &mut dyn BufMut) {
        self.network_encode(out);
    }

    fn length(&self) -> usize {
        self.network_len()
    }
}

impl Decodable for WireTx {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        Ok(Self::network_decode(buf)?)
    }
}

impl Transaction for WireTx {
    fn chain_id(&self) -> Option<ChainId> {
        each_tx!(self, tx => tx.chain_id())
    }
    fn nonce(&self) -> u64 {
        each_tx!(self, tx => tx.nonce())
    }
    fn gas_limit(&self) -> u64 {
        each_tx!(self, tx => tx.gas_limit())
    }
    fn gas_price(&self) -> Option<u128> {
        each_tx!(self, tx => tx.gas_price())
    }
    fn max_fee_per_gas(&self) -> u128 {
        each_tx!(self, tx => tx.max_fee_per_gas())
    }
    fn max_priority_fee_per_gas(&self) -> Option<u128> {
        each_tx!(self, tx => tx.max_priority_fee_per_gas())
    }
    fn max_fee_per_blob_gas(&self) -> Option<u128> {
        each_tx!(self, tx => tx.max_fee_per_blob_gas())
    }
    fn priority_fee_or_price(&self) -> u128 {
        each_tx!(self, tx => tx.priority_fee_or_price())
    }
    fn effective_gas_price(&self, base_fee: Option<u64>) -> u128 {
        each_tx!(self, tx => tx.effective_gas_price(base_fee))
    }
    fn is_dynamic_fee(&self) -> bool {
        each_tx!(self, tx => tx.is_dynamic_fee())
    }
    fn kind(&self) -> TxKind {
        each_tx!(self, tx => tx.kind())
    }
    fn is_create(&self) -> bool {
        each_tx!(self, tx => tx.is_create())
    }
    fn value(&self) -> U256 {
        each_tx!(self, tx => tx.value())
    }
    fn input(&self) -> &Bytes {
        each_tx!(self, tx => tx.input())
    }
    fn access_list(&self) -> Option<&AccessList> {
        each_tx!(self, tx => tx.access_list())
    }
    fn blob_versioned_hashes(&self) -> Option<&[B256]> {
        each_tx!(self, tx => tx.blob_versioned_hashes())
    }
    fn authorization_list(&self) -> Option<&[SignedAuthorization]> {
        each_tx!(self, tx => tx.authorization_list())
    }
}

impl SignerRecoverable for WireTx {
    fn recover_signer(&self) -> Result<Address, alloy_consensus::crypto::RecoveryError> {
        each_tx!(self, tx => tx.recover_signer())
    }

    fn recover_signer_unchecked(&self) -> Result<Address, alloy_consensus::crypto::RecoveryError> {
        each_tx!(self, tx => tx.recover_signer_unchecked())
    }
}

impl TxHashRef for WireTx {
    fn tx_hash(&self) -> &TxHash {
        each_tx!(self, tx => TxHashRef::tx_hash(tx))
    }
}

impl InMemorySize for WireTx {
    fn size(&self) -> usize {
        each_tx!(self, tx => tx.size())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(untagged)]
pub(crate) enum WireReceipt {
    Ethereum(EthereumReceipt),
    OpStack(OpReceipt),
}

macro_rules! each_receipt {
    ($value:expr, $receipt:ident => $body:expr) => {
        match $value {
            WireReceipt::Ethereum($receipt) => $body,
            WireReceipt::OpStack($receipt) => $body,
        }
    };
}

/// The EIP-2718 type of the eth/69 receipt at the front of `buf`: the first
/// field of its RLP list.
fn eth69_receipt_type(buf: &[u8]) -> alloy_rlp::Result<u8> {
    let mut buf = buf;
    let header = alloy_rlp::Header::decode(&mut buf)?;
    if !header.list {
        return Err(alloy_rlp::Error::UnexpectedString);
    }
    u8::decode(&mut buf)
}

impl WireReceipt {
    /// The EIP-2718 type of the receipt's transaction.
    #[must_use]
    pub(crate) fn tx_type(&self) -> u8 {
        each_receipt!(self, receipt => receipt.ty())
    }

    #[must_use]
    pub(crate) fn logs(&self) -> &[Log] {
        each_receipt!(self, receipt => receipt.logs())
    }

    #[must_use]
    pub(crate) fn cumulative_gas_used(&self) -> u64 {
        each_receipt!(self, receipt => receipt.cumulative_gas_used())
    }

    #[must_use]
    pub(crate) fn success(&self) -> bool {
        each_receipt!(self, receipt => receipt.status())
    }

    /// The deposit nonce and receipt version of an op-stack deposit receipt.
    #[must_use]
    pub(crate) fn deposit_fields(&self) -> Option<(Option<u64>, Option<u64>)> {
        match self {
            Self::OpStack(OpReceipt::Deposit(deposit)) => {
                Some((deposit.deposit_nonce, deposit.deposit_receipt_version))
            }
            _ => None,
        }
    }

    /// This receipt as Regolith hashed it into the receipts root: without the
    /// deposit nonce, which Canyon added back.
    #[must_use]
    pub(crate) fn without_deposit_nonce(&self) -> Self {
        let mut receipt = self.clone();
        if let Self::OpStack(OpReceipt::Deposit(deposit)) = &mut receipt {
            deposit.deposit_nonce = None;
        }
        receipt
    }
}

impl From<TransactionSigned> for WireTx {
    fn from(transaction: TransactionSigned) -> Self {
        Self::Ethereum(transaction)
    }
}

impl From<EthereumReceipt> for WireReceipt {
    fn from(receipt: EthereumReceipt) -> Self {
        Self::Ethereum(receipt)
    }
}

impl Typed2718 for WireReceipt {
    fn ty(&self) -> u8 {
        self.tx_type()
    }
}

impl TxReceipt for WireReceipt {
    type Log = Log;

    fn status_or_post_state(&self) -> Eip658Value {
        each_receipt!(self, receipt => receipt.status_or_post_state())
    }
    fn status(&self) -> bool {
        each_receipt!(self, receipt => receipt.status())
    }
    fn bloom(&self) -> Bloom {
        each_receipt!(self, receipt => receipt.bloom())
    }
    fn cumulative_gas_used(&self) -> u64 {
        each_receipt!(self, receipt => receipt.cumulative_gas_used())
    }
    fn logs(&self) -> &[Log] {
        each_receipt!(self, receipt => receipt.logs())
    }
    fn into_logs(self) -> Vec<Log> {
        each_receipt!(self, receipt => receipt.into_logs())
    }
}

impl Encodable for WireReceipt {
    fn encode(&self, out: &mut dyn BufMut) {
        each_receipt!(self, receipt => receipt.encode(out));
    }

    fn length(&self) -> usize {
        each_receipt!(self, receipt => receipt.length())
    }
}

impl Decodable for WireReceipt {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        if is_op_stack_type(eth69_receipt_type(buf)?) {
            OpReceipt::decode(buf).map(Self::OpStack)
        } else {
            EthereumReceipt::decode(buf).map(Self::Ethereum)
        }
    }
}

impl RlpEncodableReceipt for WireReceipt {
    fn rlp_encoded_length_with_bloom(&self, bloom: &Bloom) -> usize {
        each_receipt!(self, receipt => receipt.rlp_encoded_length_with_bloom(bloom))
    }

    fn rlp_encode_with_bloom(&self, bloom: &Bloom, out: &mut dyn BufMut) {
        each_receipt!(self, receipt => receipt.rlp_encode_with_bloom(bloom, out));
    }
}

impl RlpDecodableReceipt for WireReceipt {
    fn rlp_decode_with_bloom(buf: &mut &[u8]) -> alloy_rlp::Result<ReceiptWithBloom<Self>> {
        // A typed receipt is a string whose payload starts with its type.
        let mut peek = *buf;
        let header = alloy_rlp::Header::decode(&mut peek)?;
        if !header.list && peek.first().copied().is_some_and(is_op_stack_type) {
            return OpReceipt::rlp_decode_with_bloom(buf)
                .map(|receipt| receipt.map_receipt(Self::OpStack));
        }
        EthereumReceipt::rlp_decode_with_bloom(buf)
            .map(|receipt| receipt.map_receipt(Self::Ethereum))
    }
}

impl Eip2718EncodableReceipt for WireReceipt {
    fn eip2718_encoded_length_with_bloom(&self, bloom: &Bloom) -> usize {
        each_receipt!(self, receipt => receipt.eip2718_encoded_length_with_bloom(bloom))
    }

    fn eip2718_encode_with_bloom(&self, bloom: &Bloom, out: &mut dyn BufMut) {
        each_receipt!(self, receipt => receipt.eip2718_encode_with_bloom(bloom, out));
    }
}

impl InMemorySize for WireReceipt {
    fn size(&self) -> usize {
        each_receipt!(self, receipt => receipt.size())
    }
}

#[cfg(test)]
mod tests {
    use alloy_consensus::{Receipt as ConsensusReceipt, TxType};
    use op_alloy_consensus::{OpDepositReceipt, TxDeposit};

    use super::*;

    #[test]
    fn deposit_transactions_and_receipts_round_trip_beside_ethereum_ones() {
        let deposit = OpTxEnvelope::Deposit(alloy_consensus::Sealed::new(TxDeposit {
            source_hash: B256::repeat_byte(1),
            from: Address::repeat_byte(2),
            to: TxKind::Call(Address::repeat_byte(3)),
            mint: 5,
            value: U256::from(7),
            gas_limit: 21_000,
            is_system_transaction: false,
            input: Bytes::from_static(b"deposit"),
        }));
        let mut encoded = Vec::new();
        WireTx::OpStack(deposit.clone()).encode(&mut encoded);
        let decoded = WireTx::decode(&mut encoded.as_slice()).expect("decode deposit");
        assert_eq!(decoded, WireTx::OpStack(deposit));
        assert_eq!(decoded.ty(), DEPOSIT_TX_TYPE_ID);

        let inner = ConsensusReceipt {
            status: Eip658Value::Eip658(true),
            cumulative_gas_used: 21_000,
            logs: Vec::new(),
        };
        let deposit_receipt = WireReceipt::OpStack(OpReceipt::Deposit(OpDepositReceipt {
            inner: inner.clone(),
            deposit_nonce: Some(9),
            deposit_receipt_version: Some(1),
        }));
        let ethereum_receipt = WireReceipt::Ethereum(EthereumReceipt {
            tx_type: TxType::Eip4844,
            success: true,
            cumulative_gas_used: 42_000,
            logs: Vec::new(),
        });
        for receipt in [deposit_receipt, ethereum_receipt] {
            let mut encoded = Vec::new();
            receipt.encode(&mut encoded);
            assert_eq!(
                WireReceipt::decode(&mut encoded.as_slice()).expect("decode receipt"),
                receipt
            );
        }
    }
}
