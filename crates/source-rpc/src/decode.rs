//! A block served over JSON-RPC, re-encoded and checked against its own
//! header hash, transactions root, and receipts root.

use alloy_consensus::{Header, ReceiptWithBloom};
use alloy_consensus_any::{AnyHeader, AnyReceiptEnvelope};
use alloy_eips::eip2718::Encodable2718;
use alloy_primitives::{Address, B256, Bytes, TxKind, U256};
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
    /// `None` when receipts were not fetched. Each receipt comes with the
    /// bytes its block's receipts root hashed.
    receipts: Option<Vec<(AnyReceiptEnvelope, Value, Vec<u8>)>>,
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

/// The EIP-2718 bytes of a rollup's system transaction, which no Ethereum
/// type covers. `None` for a type this release does not re-encode.
#[allow(clippy::too_many_lines)]
fn rollup_transaction(ty: u8, tx: &Value) -> Result<Option<Vec<u8>>, RpcError> {
    Ok(match ty {
        // ArbitrumInternalTx: rlp([chain id, data]).
        0x6a => {
            let chain_id = quantity(tx, "chainId")?;
            let input: Bytes = parsed(tx, "input")?;
            Some(typed_rlp(ty, &[&chain_id, &input]))
        }
        // ArbitrumUnsignedTx: rlp([chain id, from, nonce, max fee, gas, to,
        // value, data]), `to` empty for a creation.
        0x65 => {
            let fields = (
                quantity(tx, "chainId")?,
                parsed::<Address>(tx, "from")?,
                quantity(tx, "nonce")?,
                quantity(tx, "maxFeePerGas")?,
                quantity(tx, "gas")?,
                call_target(tx, "to")?,
                quantity(tx, "value")?,
                parsed::<Bytes>(tx, "input")?,
            );
            Some(typed_rlp(
                ty,
                &[
                    &fields.0, &fields.1, &fields.2, &fields.3, &fields.4, &fields.5, &fields.6,
                    &fields.7,
                ],
            ))
        }
        // ArbitrumContractTx: rlp([chain id, request id, from, max fee, gas,
        // to, value, data]).
        0x66 => {
            let fields = (
                quantity(tx, "chainId")?,
                parsed::<B256>(tx, "requestId")?,
                parsed::<Address>(tx, "from")?,
                quantity(tx, "maxFeePerGas")?,
                quantity(tx, "gas")?,
                call_target(tx, "to")?,
                quantity(tx, "value")?,
                parsed::<Bytes>(tx, "input")?,
            );
            Some(typed_rlp(
                ty,
                &[
                    &fields.0, &fields.1, &fields.2, &fields.3, &fields.4, &fields.5, &fields.6,
                    &fields.7,
                ],
            ))
        }
        // ArbitrumRetryTx: rlp([chain id, nonce, from, max fee, gas, to,
        // value, data, ticket id, refund to, max refund, submission fee
        // refund]).
        0x68 => {
            let fields = (
                quantity(tx, "chainId")?,
                quantity(tx, "nonce")?,
                parsed::<Address>(tx, "from")?,
                quantity(tx, "maxFeePerGas")?,
                quantity(tx, "gas")?,
                call_target(tx, "to")?,
                quantity(tx, "value")?,
                parsed::<Bytes>(tx, "input")?,
                parsed::<B256>(tx, "ticketId")?,
                parsed::<Address>(tx, "refundTo")?,
                quantity(tx, "maxRefund")?,
                quantity(tx, "submissionFeeRefund")?,
            );
            Some(typed_rlp(
                ty,
                &[
                    &fields.0, &fields.1, &fields.2, &fields.3, &fields.4, &fields.5, &fields.6,
                    &fields.7, &fields.8, &fields.9, &fields.10, &fields.11,
                ],
            ))
        }
        // ArbitrumSubmitRetryableTx: rlp([chain id, request id, from, L1
        // base fee, deposit value, max fee, gas, retry to, retry value,
        // beneficiary, max submission fee, fee refund address, retry data]).
        // The JSON's own `to`, `value`, and `input` are the call the ticket
        // makes to the ArbRetryableTx precompile.
        0x69 => {
            let fields = (
                quantity(tx, "chainId")?,
                parsed::<B256>(tx, "requestId")?,
                parsed::<Address>(tx, "from")?,
                quantity(tx, "l1BaseFee")?,
                quantity(tx, "depositValue")?,
                quantity(tx, "maxFeePerGas")?,
                quantity(tx, "gas")?,
                call_target(tx, "retryTo")?,
                quantity(tx, "retryValue")?,
                parsed::<Address>(tx, "beneficiary")?,
                quantity(tx, "maxSubmissionFee")?,
                parsed::<Address>(tx, "refundTo")?,
                parsed::<Bytes>(tx, "retryData")?,
            );
            Some(typed_rlp(
                ty,
                &[
                    &fields.0, &fields.1, &fields.2, &fields.3, &fields.4, &fields.5, &fields.6,
                    &fields.7, &fields.8, &fields.9, &fields.10, &fields.11, &fields.12,
                ],
            ))
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
        // Op-stack deposit: rlp([source hash, from, to, mint, value, gas,
        // is system tx, data]), `to` empty for a creation. Endpoints leave
        // `isSystemTx` out while it is false, as it is since Regolith.
        0x7e => {
            let source_hash: B256 = parsed(tx, "sourceHash")?;
            let from: Address = parsed(tx, "from")?;
            let to = call_target(tx, "to")?;
            let mint = optional_quantity(tx, "mint")?.unwrap_or_default();
            let value = quantity(tx, "value")?;
            let gas = quantity(tx, "gas")?;
            let system = tx["isSystemTx"].as_bool().unwrap_or_default();
            let input: Bytes = parsed(tx, "input")?;
            Some(typed_rlp(
                ty,
                &[
                    &source_hash,
                    &from,
                    &to,
                    &mint,
                    &value,
                    &gas,
                    &system,
                    &input,
                ],
            ))
        }
        _ => None,
    })
}

/// The address `field` names, or a creation where it is null or absent.
fn call_target(tx: &Value, field: &str) -> Result<TxKind, RpcError> {
    Ok(if tx[field].is_null() {
        TxKind::Create
    } else {
        TxKind::Call(parsed(tx, field)?)
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
        for tx in served {
            let ty = transaction_type(tx)?;
            let encoded = match rollup_transaction(ty, tx)? {
                Some(bytes) => Some(bytes),
                None => serde_json::from_value::<alloy_rpc_types_eth::Transaction>(tx.clone())
                    .ok()
                    .map(|transaction| transaction.inner.into_inner().encoded_2718()),
            };
            // A transaction that cannot be encoded cannot be checked, and
            // then nothing the block serves is.
            if encoded.is_none() {
                return Err(RpcError::Unverified(format!(
                    "transaction type {ty:#04x} cannot be checked against the transactions root"
                )));
            }
            transactions.push((tx.clone(), encoded));
        }
        let root = check(
            alloy_trie::root::ordered_trie_root_with_encoder(
                &transactions,
                |(_, bytes): &(Value, Option<Vec<u8>>), out| {
                    out.extend_from_slice(bytes.as_deref().unwrap_or_default());
                },
            ) == header.transactions_root,
        );
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
    let encode = |with_nonces: bool| {
        receipts
            .iter()
            .map(|(receipt, json)| receipt_bytes(receipt, json, with_nonces))
            .collect::<Result<Vec<_>, RpcError>>()
    };
    let root = |encoded: &[Vec<u8>]| {
        alloy_trie::root::ordered_trie_root_with_encoder(encoded, |bytes, out| {
            out.extend_from_slice(bytes);
        })
    };
    let mut encoded = encode(true)?;
    let mut receipts_root = root(&encoded);
    // Between Regolith and Canyon an op-stack receipts root left deposit
    // nonces out.
    if receipts_root != header.receipts_root
        && receipts
            .iter()
            .any(|(receipt, json)| receipt.r#type == 0x7e && !json["depositNonce"].is_null())
    {
        let stripped = encode(false)?;
        if root(&stripped) == header.receipts_root {
            receipts_root = header.receipts_root;
            encoded = stripped;
        }
    }
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
        receipts: Some(
            receipts
                .into_iter()
                .zip(encoded)
                .map(|((receipt, json), bytes)| (receipt, json, bytes))
                .collect(),
        ),
    })
}

/// A receipt's EIP-2718 bytes. An op-stack deposit receipt's also hold its
/// nonce, unless `with_nonce` leaves it out, and its version.
fn receipt_bytes(
    receipt: &AnyReceiptEnvelope,
    json: &Value,
    with_nonce: bool,
) -> Result<Vec<u8>, RpcError> {
    if receipt.r#type != 0x7e {
        return Ok(receipt.encoded_2718());
    }
    let nonce = optional_quantity(json, "depositNonce")?.filter(|_| with_nonce);
    let version = optional_quantity(json, "depositReceiptVersion")?;
    let inner = &receipt.inner;
    let mut fields: Vec<&dyn Encodable> = vec![
        &inner.receipt.status,
        &inner.receipt.cumulative_gas_used,
        &inner.logs_bloom,
        &inner.receipt.logs,
    ];
    fields.extend(nonce.as_ref().map(|nonce| nonce as &dyn Encodable));
    fields.extend(version.as_ref().map(|version| version as &dyn Encodable));
    Ok(typed_rlp(0x7e, &fields))
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
    // A receipt names its transaction only in the endpoint's JSON: it binds
    // to the checked transaction at its index.
    if decoded.receipts.is_some() && decoded.transactions.is_none() {
        return Err(RpcError::Unverified(
            "receipts came without the transactions they belong to".to_owned(),
        ));
    }
    let header = &decoded.header;
    let mut transactions = Vec::new();
    let mut hashes = Vec::new();
    for (index, (tx, encoded)) in decoded.transactions.iter().flatten().enumerate() {
        let index = u32::try_from(index)
            .map_err(|_| RpcError::Malformed("transaction index overflows u32".to_owned()))?;
        let hash = encoded
            .as_deref()
            .map(alloy_primitives::keccak256)
            .ok_or_else(|| {
                RpcError::Unverified(format!("transaction {index} has no checked bytes"))
            })?;
        if parsed::<B256>(tx, "hash")? != hash {
            return Err(RpcError::Unverified(format!(
                "transaction {index} is served with another hash than its bytes have"
            )));
        }
        hashes.push(hash);
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
    for (index, (receipt, receipt_json, encoded)) in decoded.receipts.iter().flatten().enumerate() {
        let index = u32::try_from(index)
            .map_err(|_| RpcError::Malformed("receipt index overflows u32".to_owned()))?;
        let hash = hashes
            .get(usize::try_from(index).unwrap_or(usize::MAX))
            .copied()
            .ok_or_else(|| RpcError::Unverified(format!("receipt {index} has no transaction")))?;
        if parsed::<B256>(receipt_json, "transactionHash")? != hash {
            return Err(RpcError::Unverified(format!(
                "receipt {index} names another transaction than the one at its index"
            )));
        }
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
            encoded: Some(encoded.clone()),
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
    fn retryable_tickets_and_their_redeems_check_against_the_root() {
        // Arbitrum One block 522,204,106: an L1 submission of a retryable
        // ticket and its redeem, besides the internal transaction.
        let sample: Value =
            serde_json::from_str(include_str!("fixtures/arbitrum-one-retryable-block.json"))
                .unwrap();
        let decoded = decode_block(&sample["block"], &sample["receipts"]).unwrap();
        assert!(decoded.check.verified(), "{:?}", decoded.check);
        let mut changed = sample.clone();
        changed["block"]["transactions"][1]["maxSubmissionFee"] = Value::from("0x1");
        let decoded = decode_block(&changed["block"], &changed["receipts"]).unwrap();
        assert_eq!(decoded.check.transactions_root, Check::Mismatch);
    }

    #[test]
    fn a_transaction_that_cannot_be_checked_refuses_the_block() {
        let mut sample = sample();
        sample["block"]["transactions"][0]["type"] = Value::from("0x6f");
        assert!(decode_block(&sample["block"], &sample["receipts"]).is_err());
    }

    #[test]
    fn transaction_hashes_come_from_the_checked_bytes() {
        let source = SourceId::new("json-rpc").unwrap();
        let framed = |sample: &Value| {
            let decoded = decode_block(&sample["block"], &sample["receipts"]).unwrap();
            frame(
                &decoded,
                ChainId(4663),
                &source,
                Finality::Unattested,
                0,
                None,
            )
        };
        let mut hash = sample();
        hash["block"]["transactions"][0]["hash"] =
            Value::from(format!("{:#x}", B256::repeat_byte(7)));
        assert!(framed(&hash).is_err());
        let mut receipt = sample();
        receipt["receipts"][0]["transactionHash"] =
            Value::from(format!("{:#x}", B256::repeat_byte(7)));
        assert!(framed(&receipt).is_err());
        // Receipts bind to the transactions at their index.
        let mut bare = sample();
        let hashes = bare["block"]["transactions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|tx| tx["hash"].clone())
            .collect::<Vec<_>>();
        bare["block"]["transactions"] = Value::from(hashes);
        assert!(framed(&bare).is_err());
    }

    #[test]
    fn served_op_stack_blocks_check_against_their_roots() {
        let sample: Value =
            serde_json::from_str(include_str!("fixtures/unichain-block.json")).unwrap();
        let decoded = decode_block(&sample["block"], &sample["receipts"]).unwrap();
        assert!(decoded.check.verified(), "{:?}", decoded.check);

        let mut minted = sample.clone();
        minted["block"]["transactions"][0]["mint"] = Value::from("0x1");
        let decoded = decode_block(&minted["block"], &minted["receipts"]).unwrap();
        assert_eq!(decoded.check.transactions_root, Check::Mismatch);
        let mut renumbered = sample;
        renumbered["receipts"][0]["depositNonce"] = Value::from("0x1");
        let decoded = decode_block(&renumbered["block"], &renumbered["receipts"]).unwrap();
        assert_eq!(decoded.check.receipts_root, Check::Mismatch);
    }

    #[test]
    fn regolith_receipts_roots_leave_deposit_nonces_out() {
        let sample: Value =
            serde_json::from_str(include_str!("fixtures/op-mainnet-regolith-block.json")).unwrap();
        let decoded = decode_block(&sample["block"], &sample["receipts"]).unwrap();
        assert!(decoded.check.verified(), "{:?}", decoded.check);
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
