//! Built-in implementations of the public processor query-extension boundary.

mod blobs;
mod erc20;
mod uniswap;

pub use blobs::BlobsQueryExtension;
pub use erc20::Erc20QueryExtension;
pub use uniswap::UniswapQueryExtension;
