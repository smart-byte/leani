use std::collections::BTreeMap;

use alloy_primitives::{Address, B256};
use serde::Deserialize;

use crate::{Cadence, ChainError};

/// Execution forks the op-stack family implements, by registry name. Each
/// changes the EIP-2124 fork id when it activates after genesis.
const EXECUTION_FORKS: [&str; 13] = [
    "regolith", "canyon", "ecotone", "fjord", "granite", "holocene", "isthmus", "jovian", "karst",
    "azul", "beryl", "cobalt", "denim",
];
/// Forks that change only the rollup node, not execution peers.
const CONSENSUS_ONLY_FORKS: [&str; 1] = ["delta"];
/// Milliseconds between blocks once Base's Denim is active.
const DENIM_BLOCK_MS: u64 = 200;

/// An OP-stack chain's schedule and sequencer, read from its upstream
/// superchain-registry chain config plus the entry's `[op_stack]` section.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpStackNetwork {
    pub block_time: u64,
    /// The first block the rollup derives, and its timestamp: Bedrock on OP
    /// Mainnet, genesis elsewhere.
    pub first_block: u64,
    pub first_block_time: u64,
    /// Execution forks by activation timestamp, in activation order.
    pub forks: Vec<(&'static str, u64)>,
    /// Pre-Bedrock block-numbered forks of a migrated chain, by name.
    pub block_forks: Vec<(&'static str, u64)>,
    /// The key that signs unsafe-block gossip: the `attested` anchor.
    pub sequencer_signer: Address,
    /// Discv5 records of consensus peers that carry block gossip.
    pub consensus_bootnodes: Vec<String>,
}

#[derive(Deserialize)]
pub(crate) struct OpStackEntry {
    sequencer_signer: Address,
    #[serde(default)]
    consensus_bootnodes: Vec<String>,
    #[serde(default)]
    block_forks: BTreeMap<String, u64>,
}

#[derive(Deserialize)]
struct RegistryChain {
    chain_id: u64,
    block_time: u64,
    hardforks: BTreeMap<String, toml::Value>,
    genesis: RegistryGenesis,
}

#[derive(Deserialize)]
struct RegistryGenesis {
    l2_time: u64,
    l2: RegistryBlock,
}

#[derive(Deserialize)]
struct RegistryBlock {
    hash: B256,
    number: u64,
}

impl OpStackNetwork {
    pub(crate) fn from_registry(
        chain_id: u64,
        entry: OpStackEntry,
        registry: &str,
    ) -> Result<Self, ChainError> {
        let invalid = |message: String| ChainError::InvalidEntry(message);
        let registry: RegistryChain = toml::from_str(registry)
            .map_err(|error| invalid(format!("superchain registry config: {error}")))?;
        if registry.chain_id != chain_id {
            return Err(invalid(format!(
                "registry config is for chain {}, not {chain_id}",
                registry.chain_id
            )));
        }
        if registry.block_time == 0 {
            return Err(invalid("block_time must be positive".to_owned()));
        }
        let mut forks = Vec::new();
        for (key, value) in registry.hardforks {
            let Some(name) = key.strip_suffix("_time") else {
                continue;
            };
            if CONSENSUS_ONLY_FORKS.contains(&name) {
                continue;
            }
            // A fork this release does not implement: the entry needs code.
            let name = EXECUTION_FORKS
                .iter()
                .find(|known| **known == name)
                .ok_or_else(|| invalid(format!("unknown op-stack fork {name}")))?;
            let timestamp = value
                .as_integer()
                .and_then(|value| u64::try_from(value).ok())
                .ok_or_else(|| invalid(format!("{key} is not a timestamp")))?;
            forks.push((*name, timestamp));
        }
        forks.sort_by_key(|(_, timestamp)| *timestamp);
        let block_forks = entry
            .block_forks
            .into_iter()
            .map(|(name, block)| {
                ["berlin", "bedrock"]
                    .into_iter()
                    .find(|known| *known == name)
                    .map(|known| (known, block))
                    .ok_or_else(|| invalid(format!("unknown block fork {name}")))
            })
            .collect::<Result<Vec<_>, _>>()?;
        if registry.genesis.l2.hash.is_zero() {
            return Err(invalid("genesis l2 hash is zero".to_owned()));
        }
        Ok(Self {
            block_time: registry.block_time,
            first_block: registry.genesis.l2.number,
            first_block_time: registry.genesis.l2_time,
            forks,
            block_forks,
            sequencer_signer: entry.sequencer_signer,
            consensus_bootnodes: entry.consensus_bootnodes,
        })
    }

    /// One block per slot from the first derived block.
    // ponytail: whole-second slots only. Past Base's Denim, peer-served heads
    // fail these rules and the lane follows sequencer-signed heads alone;
    // model the 200 ms slots if unattested heads matter there.
    #[must_use]
    pub const fn cadence(&self) -> Cadence {
        Cadence {
            genesis_time: self.first_block_time,
            seconds: self.block_time,
        }
    }

    #[must_use]
    pub fn activation(&self, fork: &str) -> Option<u64> {
        self.forks
            .iter()
            .find(|(name, _)| *name == fork)
            .map(|(_, timestamp)| *timestamp)
    }

    /// The first block of Base's Denim: the first legacy slot at or after
    /// its activation time. Blocks after it come every 200 ms.
    #[must_use]
    pub fn denim_block(&self) -> Option<u64> {
        let denim = self.activation("denim")?;
        Some(
            self.first_block
                + denim
                    .saturating_sub(self.first_block_time)
                    .div_ceil(self.block_time),
        )
    }

    /// Milliseconds between block `number` and its parent; `u64::MAX` gives
    /// the cadence once every scheduled fork is active.
    #[must_use]
    pub fn block_interval_ms(&self, number: u64) -> u64 {
        match self.denim_block() {
            Some(denim) if number > denim => DENIM_BLOCK_MS,
            _ => self.block_time * 1_000,
        }
    }
}
