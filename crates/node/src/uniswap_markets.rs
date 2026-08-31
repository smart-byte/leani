//! Built-in Ethereum Mainnet Uniswap markets and their processor contract.

use std::collections::HashSet;

use anyhow::{Context as _, Result, bail};
use leani_processor_api::UndoPolicyMode;

use crate::config::{ProcessorConfig, ProcessorHistoryMode, PublishMode};

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

pub(crate) const MARKET_CATALOG: &[Market] = &[
    Market {
        symbol: "ETH/USDC",
        pool: "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
        fee_tier: 500,
        token0: USDC,
        token1: WETH,
        base_is_token0: false,
        start_block: 12_376_729,
    },
    Market {
        symbol: "ETH/USDT",
        pool: "0x4e68Ccd3E89f51C3074ca5072bbAC773960dFa36",
        fee_tier: 3_000,
        token0: WETH,
        token1: USDT,
        base_is_token0: true,
        start_block: 12_376_729,
    },
    Market {
        symbol: "WBTC/ETH",
        pool: "0xCBCdF9626bC03E24f779434178A73a0B4bad62eD",
        fee_tier: 3_000,
        token0: WBTC,
        token1: WETH,
        base_is_token0: true,
        start_block: 12_376_729,
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
    let mut processor: ProcessorConfig = toml::from_str(
        r#"
id = "uniswap-observations"
instance = "uniswap-observations"
version = "2.0.0"
history_control = "node_owned"
history_mode = "on_demand"
start_block = 12376729
publish = "optimistic_and_finalized"

[state]
mode = "checkpointed"

[artifacts]
mode = "none"

[output]
mode = "full"

[delivery]
mode = "window"
max_bytes = "64MiB"
max_age = "24h"
on_limit = "pause"

[checkpoint]
mode = "automatic"
keep = 3

[undo]
mode = "unfinalized"
safety_blocks = 256

[settings]
"#,
    )
    .context("parse built-in Uniswap processor contract")?;
    instance.clone_into(&mut processor.instance);
    processor.history_mode = ProcessorHistoryMode::OnDemand;
    processor.start_block = markets
        .iter()
        .map(|market| market.start_block)
        .min()
        .context("at least one market is required")?;
    if finalized_only {
        processor.publish = PublishMode::FinalizedOnly;
        processor.output.finalized_only = true;
        processor.undo.mode = UndoPolicyMode::None;
        processor.undo.safety_blocks = 0;
    }
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
