//! Blocks the Arbitrum sequencer built, made from its signed feed messages
//! alone: `ArbOS`'s `StartBlock` transaction, then the message's signed
//! transactions in order (Nitro's `ProduceBlock` and `ParseL2Transactions`).
//! Execution results, the header among them, are not part of a message.

use alloy_consensus::{Transaction as _, TxEnvelope, transaction::SignerRecoverable as _};
use alloy_eips::eip2718::{Decodable2718 as _, Encodable2718 as _};
use alloy_primitives::{Address, B256, Bytes, U256, address, keccak256};
use leani_primitives::{
    BlockFrame, BlockNumber, BlockRange, BlockRef, Capability, CapabilitySet, ChainId, CheckStatus,
    Completeness, FilterScope, Finality, Material, MissingReason, ObjectIdentity, Provenance,
    SourceId, SourceKind, TransactionEnvelope, TransactionHash, TrustModel, VerificationCheck,
    VerificationReport,
};

use crate::{
    decode::{address as primitive, amount, block_hash, typed_rlp},
    feed::SignedBlock,
};

/// The sender of the messages the sequencer builds from signed transactions
/// (`l1pricing.BatchPosterAddress`).
pub(crate) const SEQUENCER: Address = address!("a4b000000000000000000073657175656e636572");
/// `ArbOS`, which sends its internal transactions to itself.
const ARBOS: Address = address!("00000000000000000000000000000000000a4b05");
/// `ArbRetryableTx`: a `redeem` call makes `ArbOS` add a retry transaction that
/// no message carries.
const ARB_RETRYABLE_TX: Address = address!("000000000000000000000000000000000000006e");
/// The L1 message kind of an L2 message.
const L2_MESSAGE: u8 = 3;
/// The L2 message kinds of a sequencer message.
const BATCH: u8 = 3;
const SIGNED_TX: u8 = 4;
/// `startBlock(uint256,uint64,uint64,uint64)`.
const START_BLOCK: [u8; 4] = [0x6b, 0xf6, 0xa4, 0x2d];

/// What a block made from its message gives: its transactions' bodies,
/// calldata, and senders.
pub(crate) const SEQUENCED: CapabilitySet = CapabilitySet::of(Capability::Body)
    .with(Capability::Calldata)
    .with(Capability::Senders);

/// The signed transactions of a message the sequencer built, or `None` for
/// any other: a message from L1 may hold transactions that do not execute,
/// and `ArbOS` adds transactions to some.
fn signed_transactions(block: &SignedBlock) -> Option<Vec<&[u8]>> {
    let message = &block.message;
    if message.kind != L2_MESSAGE || message.sender != SEQUENCER {
        return None;
    }
    match message.l2.split_first()? {
        (&SIGNED_TX, transaction) => Some(vec![transaction]),
        (&BATCH, mut segments) => {
            let mut transactions = Vec::new();
            while !segments.is_empty() {
                let (length, rest) = segments.split_first_chunk::<8>()?;
                let length = usize::try_from(u64::from_be_bytes(*length)).ok()?;
                let (segment, rest) = rest.split_at_checked(length)?;
                let (&SIGNED_TX, transaction) = segment.split_first()? else {
                    return None;
                };
                transactions.push(transaction);
                segments = rest;
            }
            Some(transactions)
        }
        _ => None,
    }
}

/// `ArbOS`'s `StartBlock` transaction (`InternalTxStartBlock`), an
/// `ArbitrumInternalTx`: `rlp([chain id, data])`.
fn start_block(
    chain_id: u64,
    l1_base_fee: u128,
    l1_block: u64,
    number: u64,
    time_passed: u64,
) -> TransactionEnvelope {
    let mut data = START_BLOCK.to_vec();
    for word in [
        U256::from(l1_base_fee),
        U256::from(l1_block),
        U256::from(number),
        U256::from(time_passed),
    ] {
        data.extend(word.to_be_bytes::<32>());
    }
    let encoded = typed_rlp(0x6a, &[&U256::from(chain_id), &Bytes::from(data.clone())]);
    TransactionEnvelope {
        hash: TransactionHash::new(keccak256(&encoded).0),
        transaction_type: 0x6a,
        index: 0,
        size_bytes: Some(u32::try_from(encoded.len()).unwrap_or(u32::MAX)),
        encoded: Some(encoded),
        from: Some(primitive(ARBOS)),
        to: Some(primitive(ARBOS)),
        nonce: Some(0),
        gas_limit: Some(0),
        value: Some(amount(U256::ZERO)),
        input: Some(data),
        max_fee_per_gas: Some(amount(U256::ZERO)),
        max_priority_fee_per_gas: None,
        max_fee_per_blob_gas: None,
        blob_versioned_hashes: Vec::new(),
    }
}

/// A signed transaction at `index`, or `None` when it does not decode or
/// recover its sender, or calls `ArbRetryableTx`. Nitro takes no blob
/// transactions.
fn signed_transaction(index: u32, mut bytes: &[u8]) -> Option<TransactionEnvelope> {
    let transaction = TxEnvelope::decode_2718(&mut bytes)
        .ok()
        .filter(|_| bytes.is_empty())?;
    if transaction.is_eip4844() || transaction.to() == Some(ARB_RETRYABLE_TX) {
        return None;
    }
    let from = transaction.recover_signer().ok()?;
    let encoded = transaction.encoded_2718();
    Some(TransactionEnvelope {
        hash: TransactionHash::new(transaction.tx_hash().0),
        transaction_type: transaction.tx_type() as u8,
        index,
        size_bytes: Some(u32::try_from(encoded.len()).unwrap_or(u32::MAX)),
        encoded: Some(encoded),
        from: Some(primitive(from)),
        to: transaction.to().map(primitive),
        nonce: Some(transaction.nonce()),
        gas_limit: Some(transaction.gas_limit()),
        value: Some(amount(transaction.value())),
        input: Some(transaction.input().to_vec()),
        max_fee_per_gas: Some(amount(U256::from(transaction.max_fee_per_gas()))),
        max_priority_fee_per_gas: transaction
            .max_priority_fee_per_gas()
            .map(|fee| amount(U256::from(fee))),
        max_fee_per_blob_gas: None,
        blob_versioned_hashes: Vec::new(),
    })
}

/// The frame of `block`, built on `parent`, with the transactions and
/// senders its message gives. `None` when the message does not give them
/// all: it is not one the sequencer built from signed transactions, or one
/// of them does not decode, recover its sender, or calls `ArbRetryableTx`.
///
/// The transactions are declared complete, not checked: a contract that
/// calls `ArbRetryableTx` makes `ArbOS` add a retry transaction that no
/// message shows.
pub(crate) fn sequenced_frame(
    block: &SignedBlock,
    parent: &BlockRef,
    chain_id: ChainId,
    source_id: &SourceId,
    observed_at_unix_ms: u64,
) -> Option<BlockFrame> {
    let signed = signed_transactions(block)?;
    let message = &block.message;
    // ArbOS raises a timestamp below its parent's to it.
    let timestamp = message.timestamp.max(parent.timestamp);
    let mut transactions = vec![start_block(
        chain_id.0,
        message.l1_base_fee,
        message.l1_block,
        block.number,
        timestamp - parent.timestamp,
    )];
    for (index, bytes) in (1..).zip(signed) {
        transactions.push(signed_transaction(index, bytes)?);
    }
    let not_in_feed = |what: &str| VerificationCheck {
        status: CheckStatus::NotChecked,
        detail: Some(format!("the sequencer feed carries no {what}")),
    };
    let number = BlockNumber(block.number);
    Some(BlockFrame {
        chain_id,
        block: BlockRef {
            number,
            hash: block_hash(block.hash),
            parent_hash: parent.hash,
            timestamp,
        },
        finality: Finality::Unattested,
        header: Material::Missing(MissingReason::NotAvailable),
        transactions: Material::Filtered {
            value: transactions,
            scope: FilterScope::default(),
            completeness: Completeness::DatasetDeclared,
        },
        receipts: Material::Missing(MissingReason::NotAvailable),
        logs: Material::Missing(MissingReason::NotAvailable),
        withdrawals: Material::Missing(MissingReason::NotAvailable),
        blob_sidecars: Material::Missing(MissingReason::NotRequested),
        traces: Material::Missing(MissingReason::Unsupported),
        state_diffs: Material::Missing(MissingReason::Unsupported),
        provenance: vec![Provenance {
            source_id: source_id.clone(),
            source_kind: SourceKind::SequencerGossip,
            trust: TrustModel::TrustedDataset,
            range: Some(BlockRange::single(number)),
            object: Some(ObjectIdentity {
                locator: format!("sequencer-feed://{}/block/{:#x}", chain_id.0, block.hash),
                version: None,
                checksum: Some(block.hash.0),
                schema: Some("nitro-feed-message".to_owned()),
            }),
            observed_at_unix_ms,
            projection: vec!["body".to_owned()],
        }],
        verification: VerificationReport {
            header_hash: not_in_feed("header"),
            // The parent is the block signed below it.
            parent_continuity: not_in_feed("parent hash"),
            transactions_root: not_in_feed("transactions root"),
            ..VerificationReport::default()
        },
    })
}

/// The root of a frame's transactions, as its block's header names it.
pub(crate) fn transactions_root(frame: &BlockFrame) -> B256 {
    let transactions = frame
        .transactions
        .as_present()
        .map_or(&[][..], Vec::as_slice);
    alloy_trie::root::ordered_trie_root_with_encoder(transactions, |transaction, out| {
        out.extend_from_slice(transaction.encoded.as_deref().unwrap_or_default());
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use alloy_consensus::{Signed, TxEip1559};
    use serde_json::Value;

    use super::*;
    use crate::{
        decode::{decode_block, frame},
        feed::{FeedConfig, Message, signed_blocks},
    };

    const CHAIN: ChainId = ChainId(42_161);

    fn source() -> SourceId {
        SourceId::new("feed").unwrap()
    }

    /// Arbitrum One blocks 524,760,445 to 524,760,447 as the feed signed
    /// them and an endpoint served them, and the block before. The last is a
    /// second later than its parent.
    fn fixture() -> (BlockRef, Vec<SignedBlock>, Vec<Value>) {
        let fixture: Value =
            serde_json::from_str(include_str!("fixtures/arbitrum-one-sequenced-blocks.json"))
                .unwrap();
        let config = FeedConfig {
            url: "wss://feed.invalid".parse().unwrap(),
            chain_id: CHAIN.0,
            signer: address!("C1b634853Cb333D3aD8663715b08f41A3Aec47cc"),
            first_block: 22_207_817,
        };
        let signed = signed_blocks(&config, &fixture["feed"].to_string()).unwrap();
        let parent = &fixture["parent"];
        let parent = BlockRef {
            number: BlockNumber(parent["number"].as_u64().unwrap()),
            hash: block_hash(parent["hash"].as_str().unwrap().parse().unwrap()),
            parent_hash: block_hash(parent["parentHash"].as_str().unwrap().parse().unwrap()),
            timestamp: parent["timestamp"].as_u64().unwrap(),
        };
        (
            parent,
            signed,
            fixture["blocks"].as_array().unwrap().clone(),
        )
    }

    #[test]
    fn a_sequencer_message_makes_the_transactions_its_block_holds() {
        let (mut parent, signed, served) = fixture();
        assert_eq!(signed.len(), served.len());
        for (block, served) in signed.iter().zip(&served) {
            let made = sequenced_frame(block, &parent, CHAIN, &source(), 0).unwrap();
            let decoded = decode_block(served, &Value::Null).unwrap();
            let served = frame(&decoded, CHAIN, &source(), Finality::Unattested, 0, None).unwrap();
            assert_eq!(made.block, served.block);
            assert_eq!(
                made.transactions.as_present(),
                served.transactions.as_present()
            );
            assert_eq!(transactions_root(&made), decoded.header.transactions_root);
            // Declared, so only for requests that take filtered material.
            assert!(made.capabilities().satisfies(SEQUENCED, true));
            assert!(!made.capabilities().satisfies(SEQUENCED, false));
            assert_eq!(made.group_violation(SEQUENCED), None);
            parent = made.block;
        }
    }

    fn message(kind: u8, sender: Address, l2: Vec<u8>) -> SignedBlock {
        SignedBlock {
            number: 2,
            hash: B256::repeat_byte(2),
            message: Arc::new(Message {
                kind,
                sender,
                l1_block: 1,
                timestamp: 10,
                l1_base_fee: 0,
                l2,
            }),
        }
    }

    fn batch(transactions: &[&[u8]]) -> Vec<u8> {
        let mut l2 = vec![BATCH];
        for transaction in transactions {
            l2.extend((transaction.len() as u64 + 1).to_be_bytes());
            l2.push(SIGNED_TX);
            l2.extend(*transaction);
        }
        l2
    }

    #[test]
    fn only_messages_the_sequencer_built_from_signed_transactions_make_blocks() {
        let (parent, signed, _) = fixture();
        let made = |block: &SignedBlock| sequenced_frame(block, &parent, CHAIN, &source(), 0);
        let empty = message(L2_MESSAGE, SEQUENCER, batch(&[]));
        assert_eq!(
            made(&empty)
                .unwrap()
                .transactions
                .as_present()
                .map(Vec::len),
            Some(1),
            "a block holds ArbOS's start transaction"
        );
        // L1's messages, such as a deposit, a retryable, or one an L1
        // account sent.
        assert!(made(&message(12, Address::repeat_byte(1), Vec::new())).is_none());
        assert!(made(&message(L2_MESSAGE, Address::repeat_byte(1), batch(&[]))).is_none());
        // Bytes past the last transaction, or a segment that is no signed
        // transaction.
        let mut trailing = batch(&[]);
        trailing.push(0);
        assert!(made(&message(L2_MESSAGE, SEQUENCER, trailing)).is_none());
        let mut unsigned = batch(&[]);
        unsigned.extend(2_u64.to_be_bytes());
        unsigned.extend([0, 0]);
        assert!(made(&message(L2_MESSAGE, SEQUENCER, unsigned)).is_none());

        // A call to ArbRetryableTx, whose redeem ArbOS follows with a retry.
        let signed_1559 = signed
            .iter()
            .flat_map(|block| signed_transactions(block).unwrap())
            .find_map(|bytes| match TxEnvelope::decode_2718(&mut &bytes[..]) {
                Ok(TxEnvelope::Eip1559(transaction)) => Some(transaction),
                _ => None,
            })
            .unwrap();
        let mut redeem = signed_1559.tx().clone();
        redeem.to = ARB_RETRYABLE_TX.into();
        let redeem = TxEnvelope::Eip1559(Signed::<TxEip1559>::new_unhashed(
            redeem,
            *signed_1559.signature(),
        ))
        .encoded_2718();
        let original = TxEnvelope::Eip1559(signed_1559).encoded_2718();
        assert!(made(&message(L2_MESSAGE, SEQUENCER, batch(&[&original]))).is_some());
        assert!(
            made(&message(
                L2_MESSAGE,
                SEQUENCER,
                batch(&[&original, &redeem])
            ))
            .is_none()
        );
    }
}
