//! Durable blobs entities and block-local delta.

use leani_primitives::{Address, BlockHash, Finality, Quantity, TransactionHash};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BlobsBlockEntity {
    pub network: String,
    pub block_number: u64,
    pub block_hash: BlockHash,
    pub parent_hash: BlockHash,
    pub timestamp: u64,
    pub finality: Finality,
    pub size_bytes: u64,
    pub blob_count: u32,
    pub blob_gas_used: u64,
    pub excess_blob_gas: u64,
    pub blob_base_fee: Quantity,
    pub execution_base_fee: Quantity,
    pub gas_used: u64,
    pub gas_limit: u64,
    pub execution_burn: Quantity,
    pub blob_burn: Option<Quantity>,
    pub reserve_fee: Option<Quantity>,
    pub transaction_count: u32,
    pub target_blobs_per_block: u32,
    pub max_blobs_per_block: u32,
    pub transform_version: u16,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BlobTransactionEntity {
    pub network: String,
    pub block_number: u64,
    pub block_hash: BlockHash,
    pub transaction_hash: TransactionHash,
    pub transaction_index: u32,
    pub sender: Address,
    pub blob_versioned_hashes: Vec<BlockHash>,
    pub blob_count: u32,
    pub total_burn: Quantity,
    pub execution_burn: Quantity,
    pub blob_burn: Quantity,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BlobsDelta {
    pub block: BlobsBlockEntity,
    pub transactions: Vec<BlobTransactionEntity>,
}
