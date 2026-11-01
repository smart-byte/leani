//! A block served over JSON-RPC, re-encoded and checked against its own
//! header hash, transactions root, and receipts root.

use alloy_consensus::{Header, ReceiptWithBloom};
use alloy_consensus_any::{AnyHeader, AnyReceiptEnvelope};
use alloy_eips::eip2718::Encodable2718;
use alloy_primitives::{Address, B256, Bytes, U256};
use alloy_rlp::Encodable;
use alloy_rpc_types_eth::{Log as RpcLog, TransactionReceipt};
use leani_primitives::{
    BlockFrame, BlockHash, BlockNumber, BlockRange, BlockRef, Capability, ChainId, Finality,
    HeaderEnvelope, Log, Material, MissingReason, ObjectIdentity, Provenance, Quantity,
    ReceiptEnvelope, SourceId, SourceKind, TransactionEnvelope, TransactionHash, TrustModel,
    VerificationCheck, VerificationReport,
};
use leani_source_api::DataRequest;
use serde_json::Value;

use crate::RpcError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Check {
    Verified,
    Mismatch,
    /// The endpoint served something this release cannot re-encode.
    Unchecked(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlockCheck {
    pub number: u64,
    pub hash: B256,
    pub header: Check,
    pub transactions_root: Check,
    pub receipts_root: Check,
}

impl BlockCheck {
    #[must_use]
    pub fn verified(&self) -> bool {
        [&self.header, &self.transactions_root, &self.receipts_root]
            .iter()
            .all(|check| **check == Check::Verified)
    }
}

/// A served block with everything a frame needs.
#[derive(Clone, Debug)]
pub struct DecodedBlock {
    pub header: Header,
    pub hash: B256,
    pub size_bytes: Option<u64>,
    pub check: BlockCheck,
    /// `None` when only transaction hashes were fetched.
    transactions: Option<Vec<(Value, Option<Vec<u8>>)>>,
    /// `None` when receipts were not fetched.
    receipts: Option<Vec<(AnyReceiptEnvelope, Value)>>,
}

impl DecodedBlock {
    #[must_use]
    pub fn block_ref(&self) -> BlockRef {
        BlockRef {
            number: BlockNumber(self.header.number),
            hash: BlockHash::new(self.hash.0),
            parent_hash: BlockHash::new(self.header.parent_hash.0),
            timestamp: self.header.timestamp,
        }
    }
}

const fn check(matches: bool) -> Check {
    if matches {
        Check::Verified
    } else {
        Check::Mismatch
    }
}

fn quantity(value: &Value, field: &str) -> Result<U256, RpcError> {
    optional_quantity(value, field)?
        .ok_or_else(|| RpcError::Malformed(format!("{field} is not a quantity")))
}

fn optional_quantity(value: &Value, field: &str) -> Result<Option<U256>, RpcError> {
    match &value[field] {
        Value::Null => Ok(None),
        Value::String(text) => U256::from_str_radix(text.trim_start_matches("0x"), 16)
            .map(Some)
            .map_err(|_| RpcError::Malformed(format!("{field} is not a quantity"))),
        _ => Err(RpcError::Malformed(format!("{field} is not a quantity"))),
    }
}

fn parsed<T: std::str::FromStr>(value: &Value, field: &str) -> Result<T, RpcError> {
    value[field]
        .as_str()
        .and_then(|text| text.parse().ok())
        .ok_or_else(|| RpcError::Malformed(format!("{field} is malformed")))
}

fn typed_rlp(ty: u8, fields: &[&dyn Encodable]) -> Vec<u8> {
    let payload_length = fields.iter().map(|field| Encodable::length(*field)).sum();
    let mut out = vec![ty];
    alloy_rlp::Header {
        list: true,
        payload_length,
    }
    .encode(&mut out);
    for field in fields {
        field.encode(&mut out);
    }
    out
}

/// The EIP-2718 bytes of an Arbitrum system transaction, which no Ethereum
/// type covers. `None` for a type this release does not re-encode.
fn arbitrum_transaction(ty: u8, tx: &Value) -> Result<Option<Vec<u8>>, RpcError> {
    Ok(match ty {
        // ArbitrumInternalTx: rlp([chain id, data]).
        0x6a => {
            let chain_id = quantity(tx, "chainId")?;
            let input: Bytes = parsed(tx, "input")?;
            Some(typed_rlp(ty, &[&chain_id, &input]))
        }
        // ArbitrumDepositTx: rlp([chain id, L1 request id, from, to, value]).
        0x64 => {
            let chain_id = quantity(tx, "chainId")?;
            let request_id: B256 = parsed(tx, "requestId")?;
            let from: Address = parsed(tx, "from")?;
            let to: Address = parsed(tx, "to")?;
            let value = quantity(tx, "value")?;
            Some(typed_rlp(ty, &[&chain_id, &request_id, &from, &to, &value]))
        }
        _ => None,
    })
}

fn transaction_type(tx: &Value) -> Result<u8, RpcError> {
    let ty = optional_quantity(tx, "type")?.unwrap_or_default();
    u8::try_from(ty).map_err(|_| RpcError::Malformed("transaction type exceeds a byte".to_owned()))
}

/// Decode a block served by `eth_getBlockByNumber(number, true)` and the
/// receipts `eth_getBlockReceipts` served for it, and check both.
///
/// # Errors
///
/// Returns an error for a response that is not a block or receipt list.
#[allow(clippy::too_many_lines)]
pub fn decode_block(block: &Value, receipts: &Value) -> Result<DecodedBlock, RpcError> {
    let rpc_header: alloy_rpc_types_eth::Header<AnyHeader> = serde_json::from_value(block.clone())
        .map_err(|error| RpcError::Malformed(error.to_string()))?;
    let header: Header = rpc_header.inner.clone().into_header_with_defaults();
    let hash: B256 = parsed(block, "hash")?;

    let served = block["transactions"]
        .as_array()
        .ok_or_else(|| RpcError::Malformed("block has no transaction list".to_owned()))?;
    let (transactions, transactions_root) = if served.iter().all(Value::is_object) {
        let mut transactions = Vec::with_capacity(served.len());
        let mut unchecked = None;
        for tx in served {
            let ty = transaction_type(tx)?;
            let encoded = match arbitrum_transaction(ty, tx)? {
                Some(bytes) => Some(bytes),
                None => serde_json::from_value::<alloy_rpc_types_eth::Transaction>(tx.clone())
                    .ok()
                    .map(|transaction| transaction.inner.into_inner().encoded_2718()),
            };
            if encoded.is_none() && unchecked.is_none() {
                unchecked = Some(format!("transaction type {ty:#04x}"));
            }
            transactions.push((tx.clone(), encoded));
        }
        let root = match unchecked {
            Some(reason) => Check::Unchecked(reason),
            None => check(
                alloy_trie::root::ordered_trie_root_with_encoder(
                    &transactions,
                    |(_, bytes): &(Value, Option<Vec<u8>>), out| {
                        out.extend_from_slice(bytes.as_deref().unwrap_or_default());
                    },
                ) == header.transactions_root,
            ),
        };
        (Some(transactions), root)
    } else {
        (
            None,
            Check::Unchecked("transactions not fetched".to_owned()),
        )
    };
    let served_transactions = served.len();

    if receipts.is_null() {
        return Ok(DecodedBlock {
            check: BlockCheck {
                number: header.number,
                hash,
                header: check(header.hash_slow() == hash),
                transactions_root,
                receipts_root: Check::Unchecked("receipts not fetched".to_owned()),
            },
            size_bytes: optional_quantity(block, "size")?.and_then(|size| u64::try_from(size).ok()),
            header,
            hash,
            transactions,
            receipts: None,
        });
    }
    let receipts = receipts
        .as_array()
        .ok_or_else(|| RpcError::Malformed("receipts are not a list".to_owned()))?
        .iter()
        .map(|json| {
            let receipt: TransactionReceipt<AnyReceiptEnvelope<RpcLog>> =
                serde_json::from_value(json.clone())
                    .map_err(|error| RpcError::Malformed(error.to_string()))?;
            let envelope = receipt.inner;
            let receipt = AnyReceiptEnvelope {
                inner: ReceiptWithBloom {
                    receipt: alloy_consensus::Receipt {
                        status: envelope.inner.receipt.status,
                        cumulative_gas_used: envelope.inner.receipt.cumulative_gas_used,
                        logs: envelope
                            .inner
                            .receipt
                            .logs
                            .into_iter()
                            .map(|log| log.inner)
                            .collect(),
                    },
                    logs_bloom: envelope.inner.logs_bloom,
                },
                r#type: envelope.r#type,
            };
            Ok((receipt, json.clone()))
        })
        .collect::<Result<Vec<_>, RpcError>>()?;
    // Blooms are recomputed, never taken from the endpoint.
    let blooms_hold = receipts
        .iter()
        .all(|(receipt, _)| receipt.inner.logs_bloom == receipt.inner.receipt.bloom_slow());
    let receipts_root =
        alloy_trie::root::ordered_trie_root_with_encoder(&receipts, |(receipt, _), out| {
            receipt.encode_2718(out);
        });
    let counts_match = receipts.len() == served_transactions;
    Ok(DecodedBlock {
        check: BlockCheck {
            number: header.number,
            hash,
            header: check(header.hash_slow() == hash),
            transactions_root,
            receipts_root: check(
                counts_match && blooms_hold && receipts_root == header.receipts_root,
            ),
        },
        size_bytes: optional_quantity(block, "size")?.and_then(|size| u64::try_from(size).ok()),
        header,
        hash,
        transactions,
        receipts: Some(receipts),
    })
}

fn block_hash(value: B256) -> BlockHash {
    BlockHash::new(value.0)
}

fn address(value: Address) -> leani_primitives::Address {
    leani_primitives::Address::new(value.0.0)
}

fn amount(value: U256) -> Quantity {
    Quantity::new(value.to_be_bytes())
}

fn requested(request: Option<&DataRequest>, capability: Capability) -> bool {
    request.is_none_or(|request| request.required.with_derivable().contains(capability))
}

fn material<T>(value: T, requested: bool) -> Material<T> {
    if requested {
        Material::Complete(value)
    } else {
        Material::Missing(MissingReason::NotRequested)
    }
}

/// The frame of a checked block. Transaction senders are the endpoint's;
/// the transactions root covers them only through each signature.
///
/// # Errors
///
/// Returns an error for a block that failed a check or a malformed field.
#[allow(clippy::too_many_lines)]
pub fn frame(
    decoded: &DecodedBlock,
    chain_id: ChainId,
    source_id: &SourceId,
    finality: Finality,
    observed_at_unix_ms: u64,
    request: Option<&DataRequest>,
) -> Result<BlockFrame, RpcError> {
    if decoded.check.header != Check::Verified
        || decoded.check.receipts_root == Check::Mismatch
        || decoded.check.transactions_root == Check::Mismatch
    {
        return Err(RpcError::Unverified(format!("{:?}", decoded.check)));
    }
    let header = &decoded.header;
    let mut transactions = Vec::new();
    for (index, (tx, encoded)) in decoded.transactions.iter().flatten().enumerate() {
        let index = u32::try_from(index)
            .map_err(|_| RpcError::Malformed("transaction index overflows u32".to_owned()))?;
        let hash: B256 = parsed(tx, "hash")?;
        let max_fee = match optional_quantity(tx, "maxFeePerGas")? {
            Some(fee) => Some(fee),
            None => optional_quantity(tx, "gasPrice")?,
        };
        transactions.push(TransactionEnvelope {
            hash: TransactionHash::new(hash.0),
            transaction_type: transaction_type(tx)?,
            index,
            encoded: encoded.clone(),
            from: tx["from"]
                .as_str()
                .and_then(|from| from.parse().ok())
                .map(address),
            to: tx["to"]
                .as_str()
                .and_then(|to| to.parse().ok())
                .map(address),
            nonce: optional_quantity(tx, "nonce")?.and_then(|nonce| u64::try_from(nonce).ok()),
            gas_limit: optional_quantity(tx, "gas")?.and_then(|gas| u64::try_from(gas).ok()),
            value: optional_quantity(tx, "value")?.map(amount),
            input: tx["input"]
                .as_str()
                .and_then(|input| input.parse::<Bytes>().ok())
                .map(|input| input.to_vec()),
            max_fee_per_gas: max_fee.map(amount),
            max_priority_fee_per_gas: optional_quantity(tx, "maxPriorityFeePerGas")?.map(amount),
            max_fee_per_blob_gas: optional_quantity(tx, "maxFeePerBlobGas")?.map(amount),
            blob_versioned_hashes: tx["blobVersionedHashes"]
                .as_array()
                .map(|hashes| {
                    hashes
                        .iter()
                        .filter_map(|hash| hash.as_str()?.parse::<B256>().ok())
                        .map(block_hash)
                        .collect()
                })
                .unwrap_or_default(),
            size_bytes: encoded
                .as_ref()
                .map(|bytes| u32::try_from(bytes.len()).unwrap_or(u32::MAX)),
        });
    }
    let mut receipts = Vec::new();
    let mut logs = Vec::new();
    let mut previous_gas = 0_u64;
    let mut block_log_index = 0_u32;
    for (index, (receipt, receipt_json)) in decoded.receipts.iter().flatten().enumerate() {
        let index = u32::try_from(index)
            .map_err(|_| RpcError::Malformed("receipt index overflows u32".to_owned()))?;
        let hash: B256 = parsed(receipt_json, "transactionHash")?;
        let hash = TransactionHash::new(hash.0);
        let cumulative = receipt.inner.receipt.cumulative_gas_used;
        let mut receipt_logs = Vec::with_capacity(receipt.inner.receipt.logs.len());
        for source in &receipt.inner.receipt.logs {
            receipt_logs.push(Log {
                address: address(source.address),
                topics: source.data.topics().iter().map(|topic| topic.0).collect(),
                data: source.data.data.to_vec(),
                transaction_hash: Some(hash),
                transaction_index: index,
                log_index: block_log_index,
            });
            block_log_index = block_log_index
                .checked_add(1)
                .ok_or_else(|| RpcError::Malformed("log index overflows u32".to_owned()))?;
        }
        logs.extend(receipt_logs.iter().cloned());
        receipts.push(ReceiptEnvelope {
            transaction_hash: hash,
            transaction_type: receipt.r#type,
            transaction_index: index,
            encoded: Some(receipt.encoded_2718()),
            success: Some(receipt.inner.receipt.status.coerce_status()),
            gas_used: Some(cumulative.saturating_sub(previous_gas)),
            effective_gas_price: optional_quantity(receipt_json, "effectiveGasPrice")?.map(amount),
            blob_gas_used: optional_quantity(receipt_json, "blobGasUsed")?
                .and_then(|gas| u64::try_from(gas).ok()),
            blob_gas_price: optional_quantity(receipt_json, "blobGasPrice")?.map(amount),
            logs: receipt_logs,
        });
        previous_gas = cumulative;
    }
    let transactions_root = match &decoded.check.transactions_root {
        Check::Unchecked(reason) => VerificationCheck {
            status: leani_primitives::CheckStatus::NotChecked,
            detail: Some(reason.clone()),
        },
        Check::Verified | Check::Mismatch => VerificationCheck::VERIFIED,
    };
    let receipts_root = match &decoded.check.receipts_root {
        Check::Unchecked(reason) => VerificationCheck {
            status: leani_primitives::CheckStatus::NotChecked,
            detail: Some(reason.clone()),
        },
        Check::Verified | Check::Mismatch => VerificationCheck::VERIFIED,
    };
    let fetched = |present: bool, capability: Capability| requested(request, capability) && present;
    let has_transactions = decoded.transactions.is_some();
    let has_receipts = decoded.receipts.is_some();
    Ok(BlockFrame {
        chain_id,
        block: decoded.block_ref(),
        finality,
        header: material(
            HeaderEnvelope {
                rlp: Some(alloy_rlp::encode(header)),
                transactions_root: Some(block_hash(header.transactions_root)),
                receipts_root: Some(block_hash(header.receipts_root)),
                withdrawals_root: header.withdrawals_root.map(block_hash),
                gas_limit: Some(header.gas_limit),
                gas_used: Some(header.gas_used),
                base_fee_per_gas: header.base_fee_per_gas.map(U256::from).map(amount),
                blob_gas_used: header.blob_gas_used,
                excess_blob_gas: header.excess_blob_gas,
                size_bytes: decoded.size_bytes,
                consensus_size_bytes: None,
                transaction_count: Some(
                    u32::try_from(
                        decoded
                            .transactions
                            .as_ref()
                            .map_or(transactions.len(), Vec::len),
                    )
                    .unwrap_or(u32::MAX),
                ),
            },
            requested(request, Capability::Header),
        ),
        transactions: material(
            transactions,
            fetched(has_transactions, Capability::Transactions),
        ),
        receipts: material(receipts, fetched(has_receipts, Capability::Receipts)),
        logs: material(logs, fetched(has_receipts, Capability::Logs)),
        withdrawals: material(Vec::new(), requested(request, Capability::Withdrawals)),
        blob_sidecars: Material::Missing(MissingReason::NotRequested),
        traces: Material::Missing(MissingReason::Unsupported),
        state_diffs: Material::Missing(MissingReason::Unsupported),
        provenance: vec![Provenance {
            source_id: source_id.clone(),
            source_kind: SourceKind::JsonRpc,
            trust: TrustModel::TrustedDataset,
            range: Some(BlockRange::single(BlockNumber(header.number))),
            object: Some(ObjectIdentity {
                locator: format!("json-rpc://{}/block/{:#x}", chain_id.0, decoded.hash),
                version: None,
                checksum: Some(decoded.hash.0),
                schema: Some("eth_getBlockByNumber+eth_getBlockReceipts".to_owned()),
            }),
            observed_at_unix_ms,
            projection: vec![
                "header".to_owned(),
                "body".to_owned(),
                "receipts".to_owned(),
            ],
        }],
        verification: VerificationReport {
            header_hash: VerificationCheck::VERIFIED,
            parent_continuity: VerificationCheck::VERIFIED,
            transactions_root,
            receipts_root,
            withdrawals_root: VerificationCheck::NOT_CHECKED,
            dataset_checksum: VerificationCheck::NOT_CHECKED,
            consensus_anchor: None,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Value {
        serde_json::from_str(include_str!("fixtures/robinhood-block.json")).unwrap()
    }

    #[test]
    fn served_nitro_blocks_check_against_their_roots() {
        let sample = sample();
        let decoded = decode_block(&sample["block"], &sample["receipts"]).unwrap();
        assert!(decoded.check.verified(), "{:?}", decoded.check);

        let mut tampered = sample.clone();
        tampered["receipts"][0]["cumulativeGasUsed"] = Value::from("0x1");
        let decoded = decode_block(&tampered["block"], &tampered["receipts"]).unwrap();
        assert_eq!(decoded.check.receipts_root, Check::Mismatch);
        let source = SourceId::new("json-rpc").unwrap();
        assert!(
            frame(
                &decoded,
                ChainId(4663),
                &source,
                Finality::Unattested,
                0,
                None
            )
            .is_err()
        );
    }

    #[test]
    fn checked_blocks_become_complete_frames() {
        let sample = sample();
        let decoded = decode_block(&sample["block"], &sample["receipts"]).unwrap();
        let source = SourceId::new("json-rpc").unwrap();
        let frame = frame(
            &decoded,
            ChainId(4663),
            &source,
            Finality::Unattested,
            7,
            None,
        )
        .unwrap();
        frame.validate_shape().unwrap();
        let Material::Complete(transactions) = &frame.transactions else {
            panic!("complete transactions");
        };
        let Material::Complete(receipts) = &frame.receipts else {
            panic!("complete receipts");
        };
        assert_eq!(transactions.len(), receipts.len());
        assert_eq!(transactions[0].transaction_type, 0x6a);
        assert!(transactions.iter().all(|tx| tx.encoded.is_some()));
        assert_eq!(frame.block.number, BlockNumber(95_188_614));
    }
}
