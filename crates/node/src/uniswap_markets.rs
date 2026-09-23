//! Built-in Ethereum Mainnet Uniswap markets and their processor contract.

use std::collections::HashSet;

use anyhow::{Context as _, Result, bail};
use leani_processor_api::ProcessorInstanceId;
use leani_processor_uniswap::UNISWAP_OBSERVATIONS_VERSION;
use leani_store_sqlite::StoreError;

use crate::{builtin_processors::processor_contract, config::ProcessorConfig};

#[derive(Clone, Copy, Debug)]
pub(crate) struct Token {
    pub symbol: &'static str,
    pub address: &'static str,
    pub decimals: u8,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Market {
    pub symbol: &'static str,
    pub pool: &'static str,
    pub fee_tier: u32,
    pub token0: Token,
    pub token1: Token,
    pub base_is_token0: bool,
    pub start_block: u64,
}

const USDC: Token = Token {
    symbol: "USDC",
    address: "0xA0b86991c6218b36c1d19d4a2e9eb0cE3606eB48",
    decimals: 6,
};
const USDT: Token = Token {
    symbol: "USDT",
    address: "0xdAC17F958D2ee523a2206206994597C13D831ec7",
    decimals: 6,
};
const WETH: Token = Token {
    symbol: "ETH",
    address: "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
    decimals: 18,
};
const WBTC: Token = Token {
    symbol: "WBTC",
    address: "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
    decimals: 8,
};

/// Exact creation block of the USDC/WETH 0.05% pool.
const USDC_WETH_500_CREATION_BLOCK: u64 = 12_376_729;

/// Uniswap V3 factory deployment block: a lower bound for a pool whose exact
/// creation block is not pinned here, since no V3 pool predates the factory.
pub(crate) const UNISWAP_V3_FACTORY_BLOCK: u64 = 12_369_621;

/// Processor instance of a compact `[uniswap]` node configuration.
pub(crate) const COMPACT_INSTANCE: &str = "uniswap-observations";

pub(crate) const MARKET_CATALOG: &[Market] = &[
    Market {
        symbol: "ETH/USDC",
        pool: "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
        fee_tier: 500,
        token0: USDC,
        token1: WETH,
        base_is_token0: false,
        start_block: USDC_WETH_500_CREATION_BLOCK,
    },
    Market {
        symbol: "ETH/USDT",
        pool: "0x4e68Ccd3E89f51C3074ca5072bbAC773960dFa36",
        fee_tier: 3_000,
        token0: WETH,
        token1: USDT,
        base_is_token0: true,
        start_block: UNISWAP_V3_FACTORY_BLOCK,
    },
    Market {
        symbol: "WBTC/ETH",
        pool: "0xCBCdF9626bC03E24f779434178A73a0B4bad62eD",
        fee_tier: 3_000,
        token0: WBTC,
        token1: WETH,
        base_is_token0: true,
        start_block: UNISWAP_V3_FACTORY_BLOCK,
    },
];

pub(crate) fn resolve_markets(requested: &[String]) -> Result<Vec<Market>> {
    let mut seen = HashSet::new();
    requested
        .iter()
        .map(|requested| {
            let normalized = requested.trim().to_ascii_uppercase().replace("WETH", "ETH");
            let market = MARKET_CATALOG
                .iter()
                .find(|market| market.symbol == normalized)
                .copied()
                .with_context(|| {
                    format!(
                        "unknown Uniswap V3 market {requested:?}; available markets: {}",
                        MARKET_CATALOG
                            .iter()
                            .map(|market| market.symbol)
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                })?;
            if !seen.insert(market.pool.to_ascii_lowercase()) {
                bail!("market {} was requested more than once", market.symbol);
            }
            Ok(market)
        })
        .collect()
}

pub(crate) fn processor_config(
    markets: &[Market],
    instance: &str,
    finalized_only: bool,
) -> Result<ProcessorConfig> {
    let start_block = markets
        .iter()
        .map(|market| market.start_block)
        .min()
        .context("at least one market is required")?;
    let mut processor = processor_contract(
        "uniswap-observations",
        instance,
        UNISWAP_OBSERVATIONS_VERSION,
        start_block,
        finalized_only,
    );
    let pools = markets
        .iter()
        .map(|market| {
            toml::Value::Table(toml::Table::from_iter([
                (
                    "address".to_owned(),
                    toml::Value::String(market.pool.to_owned()),
                ),
                ("kind".to_owned(), toml::Value::String("v3".to_owned())),
            ]))
        })
        .collect();
    processor
        .settings
        .insert("pools".to_owned(), toml::Value::Array(pools));
    Ok(processor)
}

/// Name the operator's routes when the store refuses the compact
/// `[uniswap]` processor it holds: that processor's identity includes its
/// market set and start block, as when ETH/USDT or WBTC/ETH moved to the
/// factory block. Other processors and errors pass through.
pub(crate) fn explain_compact_refusal(
    error: StoreError,
    instance: &ProcessorInstanceId,
) -> anyhow::Error {
    if instance.as_str() != COMPACT_INSTANCE || !matches!(error, StoreError::ProcessorIdentity(_)) {
        return error.into();
    }
    anyhow::Error::new(error).context(format!(
        "this store's `{COMPACT_INSTANCE}` processor was created for another market set or start \
         block (ETH/USDT and WBTC/ETH now start at the Uniswap V3 factory block \
         {UNISWAP_V3_FACTORY_BLOCK}); keep that data and start with a new `data_dir`, or move to \
         an advanced configuration whose `[[processors]]` entry names a new `instance`"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn market_catalog_is_unique_and_matches_the_compact_schema() {
        let symbols = MARKET_CATALOG
            .iter()
            .map(|market| market.symbol)
            .collect::<Vec<_>>();
        let pools = MARKET_CATALOG
            .iter()
            .map(|market| market.pool.to_ascii_lowercase())
            .collect::<HashSet<_>>();
        assert_eq!(
            symbols.iter().copied().collect::<HashSet<_>>().len(),
            symbols.len()
        );
        assert_eq!(pools.len(), symbols.len());

        let schema: serde_json::Value =
            serde_json::from_str(include_str!("../../../config/schema-v1.json"))
                .expect("configuration schema");
        let schema_symbols =
            schema["$defs"]["starterUniswap"]["properties"]["markets"]["items"]["enum"]
                .as_array()
                .expect("starter market enum")
                .iter()
                .map(|value| value.as_str().expect("market symbol"))
                .collect::<Vec<_>>();
        assert_eq!(schema_symbols, symbols);
    }
}
