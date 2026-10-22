//! Blocks from a JSON-RPC endpoint, checked locally against their own
//! commitments: the header hash, the transactions root, and the receipts
//! root. The endpoint is trusted only for which block is canonical; nothing
//! it serves for that block can change without failing a check.

use alloy_consensus::{Header, ReceiptWithBloom};
use alloy_consensus_any::{AnyHeader, AnyReceiptEnvelope};
use alloy_eips::eip2718::Encodable2718;
use alloy_primitives::{Address, B256, Bytes, U256};
use alloy_rlp::Encodable;
use alloy_rpc_types_eth::{Log, TransactionReceipt};
use serde_json::Value;
use url::Url;

#[derive(Debug, thiserror::Error)]
pub enum RpcError {
    #[error("rpc request {method} failed: {detail}")]
    Request {
        method: &'static str,
        detail: String,
    },
    #[error("malformed rpc response: {0}")]
    Malformed(String),
}

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

const fn check(matches: bool) -> Check {
    if matches {
        Check::Verified
    } else {
        Check::Mismatch
    }
}

fn quantity(value: &Value, field: &str) -> Result<U256, RpcError> {
    value[field]
        .as_str()
        .and_then(|text| U256::from_str_radix(text.trim_start_matches("0x"), 16).ok())
        .ok_or_else(|| RpcError::Malformed(format!("{field} is not a quantity")))
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

/// Check a block served by `eth_getBlockByNumber(number, true)` against the
/// receipts `eth_getBlockReceipts` served for it.
///
/// # Errors
///
/// Returns an error for a response that is not a block or receipt list.
pub fn check_block(block: &Value, receipts: &Value) -> Result<BlockCheck, RpcError> {
    let rpc_header: alloy_rpc_types_eth::Header<AnyHeader> = serde_json::from_value(block.clone())
        .map_err(|error| RpcError::Malformed(error.to_string()))?;
    let header: Header = rpc_header.inner.clone().into_header_with_defaults();
    let hash: B256 = parsed(block, "hash")?;

    let mut encoded = Vec::new();
    let mut unchecked = None;
    for tx in block["transactions"]
        .as_array()
        .ok_or_else(|| RpcError::Malformed("block has no full transactions".to_owned()))?
    {
        let ty = u8::try_from(quantity(tx, "type")?)
            .map_err(|_| RpcError::Malformed("transaction type exceeds a byte".to_owned()))?;
        if let Some(bytes) = arbitrum_transaction(ty, tx)? {
            encoded.push(bytes);
            continue;
        }
        let Ok(transaction) =
            serde_json::from_value::<alloy_rpc_types_eth::Transaction>(tx.clone())
        else {
            unchecked = Some(format!("transaction type {ty:#04x}"));
            break;
        };
        encoded.push(transaction.inner.into_inner().encoded_2718());
    }
    let transactions_root = match unchecked {
        Some(reason) => Check::Unchecked(reason),
        None => check(
            alloy_trie::root::ordered_trie_root_with_encoder(&encoded, |bytes, out| {
                out.extend_from_slice(bytes);
            }) == header.transactions_root,
        ),
    };

    let receipts = receipts
        .as_array()
        .ok_or_else(|| RpcError::Malformed("receipts are not a list".to_owned()))?
        .iter()
        .map(|receipt| {
            let receipt: TransactionReceipt<AnyReceiptEnvelope<Log>> =
                serde_json::from_value(receipt.clone())
                    .map_err(|error| RpcError::Malformed(error.to_string()))?;
            let envelope = receipt.inner;
            Ok(AnyReceiptEnvelope {
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
            })
        })
        .collect::<Result<Vec<_>, RpcError>>()?;
    // Blooms are recomputed, never taken from the endpoint.
    let blooms_hold = receipts
        .iter()
        .all(|receipt| receipt.inner.logs_bloom == receipt.inner.receipt.bloom_slow());
    let receipts_root =
        alloy_trie::root::ordered_trie_root_with_encoder(&receipts, |receipt, out| {
            receipt.encode_2718(out);
        });
    Ok(BlockCheck {
        number: header.number,
        hash,
        header: check(header.hash_slow() == hash),
        transactions_root,
        receipts_root: check(blooms_hold && receipts_root == header.receipts_root),
    })
}

/// A JSON-RPC endpoint. HTTP/1.1 only: some public Nitro endpoints stall
/// HTTP/2 requests.
#[derive(Clone, Debug)]
pub struct RpcClient {
    endpoint: Url,
    client: reqwest::Client,
}

impl RpcClient {
    /// # Errors
    ///
    /// Returns an error when the HTTP client cannot be built.
    pub fn new(endpoint: Url) -> Result<Self, RpcError> {
        let client = reqwest::Client::builder()
            .http1_only()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|error| RpcError::Request {
                method: "client",
                detail: error.to_string(),
            })?;
        Ok(Self { endpoint, client })
    }

    async fn call(&self, method: &'static str, params: Value) -> Result<Value, RpcError> {
        let response: Value = self
            .client
            .post(self.endpoint.clone())
            .json(
                &serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}),
            )
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(|error| RpcError::Request {
                method,
                detail: error.to_string(),
            })?
            .json()
            .await
            .map_err(|error| RpcError::Request {
                method,
                detail: error.to_string(),
            })?;
        match response.get("result") {
            Some(result) if !result.is_null() => Ok(result.clone()),
            _ => Err(RpcError::Request {
                method,
                detail: response["error"].to_string(),
            }),
        }
    }

    /// # Errors
    ///
    /// Returns an error when the endpoint does not answer.
    pub async fn head(&self) -> Result<u64, RpcError> {
        let head = self.call("eth_blockNumber", serde_json::json!([])).await?;
        u64::from_str_radix(
            head.as_str().unwrap_or_default().trim_start_matches("0x"),
            16,
        )
        .map_err(|error| RpcError::Malformed(error.to_string()))
    }

    /// The block with full transactions and its receipts.
    ///
    /// # Errors
    ///
    /// Returns an error when the endpoint serves either incompletely.
    pub async fn block(&self, number: u64) -> Result<(Value, Value), RpcError> {
        let tag = format!("{number:#x}");
        let block = self
            .call("eth_getBlockByNumber", serde_json::json!([tag, true]))
            .await?;
        let receipts = self
            .call("eth_getBlockReceipts", serde_json::json!([tag]))
            .await?;
        Ok((block, receipts))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn served_nitro_blocks_check_against_their_roots() {
        let sample: Value =
            serde_json::from_str(include_str!("fixtures/robinhood-block.json")).unwrap();
        let checked = check_block(&sample["block"], &sample["receipts"]).unwrap();
        assert!(checked.verified(), "{checked:?}");

        let mut tampered = sample.clone();
        tampered["receipts"][0]["gasUsed"] = Value::from("0x1");
        tampered["receipts"][0]["cumulativeGasUsed"] = Value::from("0x1");
        let checked = check_block(&tampered["block"], &tampered["receipts"]).unwrap();
        assert_eq!(checked.receipts_root, Check::Mismatch);
    }
}
