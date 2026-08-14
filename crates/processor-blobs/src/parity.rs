//! Canonical read-only comparison with blobs.money row exports.

use std::collections::{BTreeMap, BTreeSet};

use alloy_primitives::U256;
use leani_primitives::Quantity;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::BlobsDelta;

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BlobsCompatibilityExport {
    pub blocks: Vec<CompatibilityBlock>,
    #[serde(alias = "blob_transactions")]
    pub blob_transactions: Vec<CompatibilityBlobTransaction>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CompatibilityBlock {
    pub network: String,
    pub block_number: u64,
    pub block_hash: Option<String>,
    pub timestamp: u64,
    pub size: Option<String>,
    pub blob_count: u32,
    pub blob_gas_used: String,
    pub excess_blob_gas: String,
    pub blob_base_fee: String,
    pub execution_base_fee: Option<String>,
    pub gas_used: Option<String>,
    pub gas_limit: Option<String>,
    pub eth_burned_execution: Option<String>,
    pub blob_eth_burned: Option<String>,
    pub reserve_fee: Option<String>,
    pub transaction_count: Option<u32>,
    pub target_blobs_per_block: u32,
    pub max_blobs_per_block: u32,
    pub transform_version: Option<u16>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CompatibilityBlobTransaction {
    pub network: String,
    pub block_number: u64,
    pub tx_hash: String,
    pub sender_address: String,
    pub blob_count: u32,
    pub eth_burned: String,
    pub execution_eth_burned: String,
    pub blob_eth_burned: String,
}

impl BlobsCompatibilityExport {
    #[must_use]
    pub fn from_deltas(deltas: &[BlobsDelta]) -> Self {
        let mut blocks = Vec::with_capacity(deltas.len());
        let mut blob_transactions = Vec::new();
        for delta in deltas {
            let block = &delta.block;
            blocks.push(CompatibilityBlock {
                network: block.network.clone(),
                block_number: block.block_number,
                block_hash: Some(block.block_hash.to_string()),
                timestamp: block.timestamp,
                size: Some(block.size_bytes.to_string()),
                blob_count: block.blob_count,
                blob_gas_used: block.blob_gas_used.to_string(),
                excess_blob_gas: block.excess_blob_gas.to_string(),
                blob_base_fee: decimal(block.blob_base_fee),
                execution_base_fee: Some(decimal(block.execution_base_fee)),
                gas_used: Some(block.gas_used.to_string()),
                gas_limit: Some(block.gas_limit.to_string()),
                eth_burned_execution: Some(decimal(block.execution_burn)),
                blob_eth_burned: block.blob_burn.map(decimal),
                reserve_fee: block.reserve_fee.map(decimal),
                transaction_count: Some(block.transaction_count),
                target_blobs_per_block: block.target_blobs_per_block,
                max_blobs_per_block: block.max_blobs_per_block,
                transform_version: Some(block.transform_version),
            });
            blob_transactions.extend(delta.transactions.iter().map(|transaction| {
                CompatibilityBlobTransaction {
                    network: transaction.network.clone(),
                    block_number: transaction.block_number,
                    tx_hash: transaction.transaction_hash.to_string(),
                    sender_address: transaction.sender.to_string(),
                    blob_count: transaction.blob_count,
                    eth_burned: decimal(transaction.total_burn),
                    execution_eth_burned: decimal(transaction.execution_burn),
                    blob_eth_burned: decimal(transaction.blob_burn),
                }
            }));
        }
        blocks.sort_by_key(|block| (block.network.clone(), block.block_number));
        blob_transactions
            .sort_by_key(|transaction| (transaction.network.clone(), transaction.tx_hash.clone()));
        Self {
            blocks,
            blob_transactions,
        }
    }

    #[must_use]
    /// Compare every compatibility row and field.
    ///
    /// # Panics
    ///
    /// Panics only if an in-memory compatibility row unexpectedly cannot be
    /// represented as a JSON value; these row types contain no fallible values.
    pub fn compare(&self, actual: &Self) -> BlobsParityReport {
        let expected_blocks = self
            .blocks
            .iter()
            .map(|row| {
                (
                    format!("{}:{}", row.network, row.block_number),
                    serde_json::to_value(row).expect("compatibility block serializes"),
                )
            })
            .collect();
        let actual_blocks = actual
            .blocks
            .iter()
            .map(|row| {
                (
                    format!("{}:{}", row.network, row.block_number),
                    serde_json::to_value(row).expect("compatibility block serializes"),
                )
            })
            .collect();
        let expected_transactions = self
            .blob_transactions
            .iter()
            .map(|row| {
                (
                    format!("{}:{}", row.network, row.tx_hash.to_ascii_lowercase()),
                    serde_json::to_value(row).expect("compatibility transaction serializes"),
                )
            })
            .collect();
        let actual_transactions = actual
            .blob_transactions
            .iter()
            .map(|row| {
                (
                    format!("{}:{}", row.network, row.tx_hash.to_ascii_lowercase()),
                    serde_json::to_value(row).expect("compatibility transaction serializes"),
                )
            })
            .collect();
        let mut mismatches = Vec::new();
        compare_values("block", &expected_blocks, &actual_blocks, &mut mismatches);
        compare_values(
            "blob_transaction",
            &expected_transactions,
            &actual_transactions,
            &mut mismatches,
        );
        BlobsParityReport {
            expected_blocks: u64::try_from(self.blocks.len()).unwrap_or(u64::MAX),
            actual_blocks: u64::try_from(actual.blocks.len()).unwrap_or(u64::MAX),
            expected_transactions: u64::try_from(self.blob_transactions.len()).unwrap_or(u64::MAX),
            actual_transactions: u64::try_from(actual.blob_transactions.len()).unwrap_or(u64::MAX),
            mismatches,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ParityClassification {
    MissingExpectedRow,
    MissingActualRow,
    UnexplainedValue,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ParityMismatch {
    pub entity: String,
    pub key: String,
    pub field: String,
    pub expected: Option<Value>,
    pub actual: Option<Value>,
    pub classification: ParityClassification,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct BlobsParityReport {
    pub expected_blocks: u64,
    pub actual_blocks: u64,
    pub expected_transactions: u64,
    pub actual_transactions: u64,
    pub mismatches: Vec<ParityMismatch>,
}

impl BlobsParityReport {
    #[must_use]
    pub fn is_clean(&self) -> bool {
        self.mismatches.is_empty()
    }
}

fn compare_values(
    entity: &str,
    expected: &BTreeMap<String, Value>,
    actual: &BTreeMap<String, Value>,
    mismatches: &mut Vec<ParityMismatch>,
) {
    let keys = expected
        .keys()
        .chain(actual.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    for key in keys {
        match (expected.get(&key), actual.get(&key)) {
            (Some(expected), Some(actual)) => {
                compare_row(entity, &key, expected, actual, mismatches);
            }
            (Some(expected), None) => mismatches.push(ParityMismatch {
                entity: entity.to_owned(),
                key,
                field: "*".to_owned(),
                expected: Some(expected.clone()),
                actual: None,
                classification: ParityClassification::MissingActualRow,
            }),
            (None, Some(actual)) => mismatches.push(ParityMismatch {
                entity: entity.to_owned(),
                key,
                field: "*".to_owned(),
                expected: None,
                actual: Some(actual.clone()),
                classification: ParityClassification::MissingExpectedRow,
            }),
            (None, None) => {}
        }
    }
}

fn compare_row(
    entity: &str,
    key: &str,
    expected: &Value,
    actual: &Value,
    mismatches: &mut Vec<ParityMismatch>,
) {
    let Some(expected) = expected.as_object() else {
        return;
    };
    let Some(actual) = actual.as_object() else {
        return;
    };
    let fields = expected
        .keys()
        .chain(actual.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    for field in fields {
        if expected.get(&field) != actual.get(&field) {
            mismatches.push(ParityMismatch {
                entity: entity.to_owned(),
                key: key.to_owned(),
                field: field.clone(),
                expected: expected.get(&field).cloned(),
                actual: actual.get(&field).cloned(),
                classification: ParityClassification::UnexplainedValue,
            });
        }
    }
}

fn decimal(value: Quantity) -> String {
    <U256 as From<Quantity>>::from(value).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block(number: u64) -> CompatibilityBlock {
        CompatibilityBlock {
            network: "mainnet".to_owned(),
            block_number: number,
            block_hash: Some(format!("0x{number:064x}")),
            timestamp: number,
            size: Some("1".to_owned()),
            blob_count: 0,
            blob_gas_used: "0".to_owned(),
            excess_blob_gas: "0".to_owned(),
            blob_base_fee: "1".to_owned(),
            execution_base_fee: Some("2".to_owned()),
            gas_used: Some("3".to_owned()),
            gas_limit: Some("4".to_owned()),
            eth_burned_execution: Some("6".to_owned()),
            blob_eth_burned: None,
            reserve_fee: None,
            transaction_count: Some(0),
            target_blobs_per_block: 3,
            max_blobs_per_block: 6,
            transform_version: Some(3),
        }
    }

    #[test]
    fn exact_export_is_clean() {
        let expected = BlobsCompatibilityExport {
            blocks: vec![block(1)],
            blob_transactions: Vec::new(),
        };
        assert!(expected.compare(&expected).is_clean());
    }

    #[test]
    fn field_and_presence_mismatches_are_classified() {
        let expected = BlobsCompatibilityExport {
            blocks: vec![block(1), block(2)],
            blob_transactions: Vec::new(),
        };
        let mut changed = block(1);
        changed.blob_base_fee = "2".to_owned();
        let actual = BlobsCompatibilityExport {
            blocks: vec![changed, block(3)],
            blob_transactions: Vec::new(),
        };
        let report = expected.compare(&actual);
        assert_eq!(report.mismatches.len(), 3);
        assert!(
            report
                .mismatches
                .iter()
                .any(|mismatch| mismatch.field == "blobBaseFee")
        );
    }
}
