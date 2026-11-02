//! History from public, daily-partitioned Parquet exports on AWS Open Data.
//!
//! Rows are unsorted within a day, so a chunk is one UTC day: every table
//! of that day is read once with only the needed columns. Each block's
//! receipts are rebuilt from its receipt and log rows and checked against
//! its receipts root; where the export carries every header field, the
//! block hash is recomputed too.

mod catalog;
mod history;
mod layout;
mod read;
mod verify;

pub use catalog::{Listing, S3Listing, StoreListing};
pub use history::{AwsPublicConfig, AwsPublicSource};

use leani_primitives::BlockRange;

/// AWS Open Data's public blockchain bucket.
pub const AWS_PUBLIC_BLOCKCHAIN: &str = "https://aws-public-blockchain.s3.amazonaws.com/";

#[derive(Debug, thiserror::Error)]
pub enum AwsError {
    #[error("object store: {0}")]
    Store(String),
    #[error("listing {prefix}: {detail}")]
    Listing { prefix: String, detail: String },
    #[error("parquet: {0}")]
    Parquet(String),
    #[error("{table} has no column {column}")]
    MissingColumn { table: &'static str, column: String },
    #[error("malformed {table} row: {detail}")]
    Malformed { table: &'static str, detail: String },
    #[error("blocks {0:?} are not published in every table yet")]
    NotPublished(BlockRange),
    #[error("block {block}: {detail}")]
    Mismatch { block: u64, detail: String },
    #[error("{resource} would reach {observed} bytes, over the {limit}-byte budget")]
    Budget {
        resource: &'static str,
        observed: u64,
        limit: u64,
    },
    #[error("cancelled")]
    Cancelled,
}
