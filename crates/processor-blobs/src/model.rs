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
    /// Protocol blob base fee in wei per blob gas: the EIP-4844 fee for
    /// `excess_blob_gas` under the fork active at `timestamp`.
    pub blob_base_fee: Quantity,
    pub execution_base_fee: Quantity,
    pub gas_used: u64,
    pub gas_limit: u64,
    pub execution_burn: Quantity,
    /// `blob_base_fee` times `blob_gas_used`; `None` for a block without blobs.
    pub blob_burn: Option<Quantity>,
    /// EIP-7918 reserve price in wei per blob gas from Fusaka on, `None`
    /// before. It is not applied to `blob_base_fee`.
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
    /// The block's `blob_base_fee` times this transaction's blob gas.
    pub blob_burn: Quantity,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BlobsDelta {
    pub block: BlobsBlockEntity,
    pub transactions: Vec<BlobTransactionEntity>,
}
