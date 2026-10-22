//! Chains Leani can follow. A chain entry is data: identity, schedule,
//! network parameters, and defaults. Its family is code: the protocol rules
//! every chain of that lineage shares.

use std::sync::{Arc, LazyLock};

use alloy_primitives::B256;
use serde::{Deserialize, Serialize};
use url::Url;

mod beacon;

pub use beacon::{BeaconFork, BeaconNetwork, SLOTS_PER_EPOCH};

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
pub enum ChainError {
    #[error("invalid chain entry: {0}")]
    InvalidEntry(String),
    #[error("unknown chain {name:?}; known chains: {known}")]
    Unknown { name: String, known: String },
    #[error("{0}")]
    Unsupported(String),
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Family {
    Ethereum,
}

impl Family {
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Ethereum => "ethereum",
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionNetwork {
    /// EIP-1459 tree of execution peers. Records are authenticated by the
    /// key in the URL; what peers serve is verified independently.
    #[serde(default)]
    pub dns_tree: Option<String>,
    /// `enode://` URLs. Empty means the execution chain spec's built-ins.
    #[serde(default)]
    pub bootnodes: Vec<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ChainDefaults {
    /// Untrusted Beacon API transport pool; responses are verified locally.
    #[serde(default)]
    pub beacon_endpoints: Vec<Url>,
    /// Weak-subjectivity checkpoint providers `leani init` asks.
    #[serde(default)]
    pub checkpoint_providers: Vec<Url>,
    /// History source kinds the compact config enables, in priority order.
    #[serde(default)]
    pub history_sources: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Chain {
    pub name: String,
    pub aliases: Vec<String>,
    pub family: Family,
    pub chain_id: u64,
    pub genesis_hash: B256,
    /// First post-Merge execution block, for families that have one.
    pub merge_block: Option<u64>,
    pub execution: ExecutionNetwork,
    pub beacon: Option<BeaconNetwork>,
    pub defaults: ChainDefaults,
}

impl Chain {
    /// Parse an entry file. `beacon_config` is the network's upstream
    /// consensus `config.yaml`, required when the entry has a `[beacon]`
    /// section.
    ///
    /// # Errors
    ///
    /// Returns an error when the entry or its beacon config is malformed.
    pub fn from_entry(entry: &str, beacon_config: Option<&str>) -> Result<Self, ChainError> {
        let entry: EntryFile =
            toml::from_str(entry).map_err(|error| ChainError::InvalidEntry(error.to_string()))?;
        let beacon = match (entry.beacon, beacon_config) {
            (Some(beacon), Some(yaml)) => Some(BeaconNetwork::from_config_yaml(
                beacon.genesis_time,
                beacon.genesis_validators_root,
                yaml,
            )?),
            (None, None) => None,
            (Some(_), None) => {
                return Err(ChainError::InvalidEntry(format!(
                    "{} has a [beacon] section but no beacon config",
                    entry.name
                )));
            }
            (None, Some(_)) => {
                return Err(ChainError::InvalidEntry(format!(
                    "{} has a beacon config but no [beacon] section",
                    entry.name
                )));
            }
        };
        Ok(Self {
            name: entry.name,
            aliases: entry.aliases,
            family: entry.family,
            chain_id: entry.chain_id,
            genesis_hash: entry.genesis_hash,
            merge_block: entry.merge_block,
            execution: entry.execution,
            beacon,
            defaults: entry.defaults,
        })
    }

    fn answers_to(&self, name: &str) -> bool {
        self.name.eq_ignore_ascii_case(name)
            || self
                .aliases
                .iter()
                .any(|alias| alias.eq_ignore_ascii_case(name))
    }

    /// The beacon network of an Ethereum-family chain.
    ///
    /// # Errors
    ///
    /// Returns an error when the chain has no beacon chain.
    pub fn beacon(&self) -> Result<&BeaconNetwork, ChainError> {
        self.beacon
            .as_ref()
            .ok_or_else(|| ChainError::Unsupported(format!("{} has no beacon chain", self.name)))
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EntryFile {
    name: String,
    #[serde(default)]
    aliases: Vec<String>,
    family: Family,
    chain_id: u64,
    genesis_hash: B256,
    #[serde(default)]
    merge_block: Option<u64>,
    #[serde(default)]
    execution: ExecutionNetwork,
    #[serde(default)]
    beacon: Option<BeaconEntry>,
    #[serde(default)]
    defaults: ChainDefaults,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BeaconEntry {
    genesis_time: u64,
    genesis_validators_root: B256,
}

/// Entries built into this binary: `(entry, beacon config)`.
const BUILT_IN: &[(&str, Option<&str>)] = &[
    (
        include_str!("../../../config/chains/ethereum-mainnet.toml"),
        Some(include_str!("../../../config/chains/beacon/mainnet.yaml")),
    ),
    (
        include_str!("../../../config/chains/ethereum-sepolia.toml"),
        Some(include_str!("../../../config/chains/beacon/sepolia.yaml")),
    ),
    (
        include_str!("../../../config/chains/ethereum-hoodi.toml"),
        Some(include_str!("../../../config/chains/beacon/hoodi.yaml")),
    ),
];

#[derive(Clone, Debug)]
pub struct ChainRegistry {
    chains: Vec<Arc<Chain>>,
}

static STANDARD: LazyLock<ChainRegistry> = LazyLock::new(|| ChainRegistry {
    chains: BUILT_IN
        .iter()
        .map(|(entry, beacon)| {
            Arc::new(Chain::from_entry(entry, *beacon).expect("built-in chain entries are valid"))
        })
        .collect(),
});

impl ChainRegistry {
    /// The chains built into this binary.
    #[must_use]
    pub fn standard() -> &'static Self {
        &STANDARD
    }

    /// Look a chain up by name or alias, ignoring case.
    ///
    /// # Errors
    ///
    /// Returns an error naming the known chains.
    pub fn get(&self, name: &str) -> Result<Arc<Chain>, ChainError> {
        self.chains
            .iter()
            .find(|chain| chain.answers_to(name))
            .cloned()
            .ok_or_else(|| ChainError::Unknown {
                name: name.to_owned(),
                known: self.names().join(", "),
            })
    }

    #[must_use]
    pub fn names(&self) -> Vec<&str> {
        self.chains
            .iter()
            .map(|chain| chain.name.as_str())
            .collect()
    }

    pub fn iter(&self) -> impl Iterator<Item = &Arc<Chain>> {
        self.chains.iter()
    }
}

/// Ethereum mainnet's built-in entry.
///
/// # Panics
///
/// Never: the entry is built in.
#[must_use]
pub fn ethereum_mainnet() -> Arc<Chain> {
    ChainRegistry::standard()
        .get("ethereum-mainnet")
        .expect("ethereum-mainnet is built in")
}

/// The execution chain spec an Ethereum-family chain's peers and header
/// rules follow.
///
/// # Errors
///
/// Returns an error for a chain without a built-in execution spec.
pub fn ethereum_chain_spec(chain: &Chain) -> Result<Arc<reth_chainspec::ChainSpec>, ChainError> {
    let spec = match (chain.family, chain.chain_id) {
        (Family::Ethereum, 1) => reth_chainspec::MAINNET.clone(),
        (Family::Ethereum, 11_155_111) => reth_chainspec::SEPOLIA.clone(),
        (Family::Ethereum, 560_048) => reth_chainspec::HOODI.clone(),
        _ => {
            return Err(ChainError::Unsupported(format!(
                "no execution chain spec for {}",
                chain.name
            )));
        }
    };
    if reth_chainspec::EthChainSpec::genesis_hash(spec.as_ref()) != chain.genesis_hash {
        return Err(ChainError::InvalidEntry(format!(
            "{} genesis hash disagrees with its execution chain spec",
            chain.name
        )));
    }
    Ok(spec)
}

#[cfg(test)]
mod tests;
