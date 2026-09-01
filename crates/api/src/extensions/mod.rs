//! Built-in implementations of the public processor query-extension boundary.

mod blobs;
mod blocks;
mod erc20;
mod uniswap;

pub use blobs::BlobsQueryExtension;
pub use blocks::BlockSummaryQueryExtension;
pub use erc20::Erc20QueryExtension;
pub use uniswap::{UniswapObservationsQueryExtension, UniswapQueryExtension};
