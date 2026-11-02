//! Chains Leani can follow. A chain entry is data: identity, schedule,
//! network parameters, and defaults. Its family is code: the protocol rules
//! every chain of that lineage shares.

use std::sync::{Arc, LazyLock};

use alloy_primitives::B256;
use serde::{Deserialize, Serialize};
use url::Url;

mod beacon;
mod op_stack;

pub use beacon::{BeaconFork, BeaconNetwork, SLOTS_PER_EPOCH};
pub use op_stack::OpStackNetwork;

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
    OpStack,
    Arbitrum,
}

impl Family {
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Ethereum => "ethereum",
            Self::OpStack => "op-stack",
            Self::Arbitrum => "arbitrum",
        }
    }
}

/// When blocks are due: one per `seconds` from `genesis_time`. Beacon slots
/// on Ethereum, L2 blocks on OP-stack chains.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Cadence {
    pub genesis_time: u64,
    pub seconds: u64,
}

impl Cadence {
    #[must_use]
    pub const fn slot_start(&self, slot: u64) -> u64 {
        self.genesis_time
            .saturating_add(slot.saturating_mul(self.seconds))
    }

    /// The slot that starts at `timestamp`, or `None` before genesis or off a
    /// slot boundary.
    #[must_use]
    pub const fn slot_starting_at(&self, timestamp: u64) -> Option<u64> {
        match timestamp.checked_sub(self.genesis_time) {
            Some(since) if since % self.seconds == 0 => Some(since / self.seconds),
            _ => None,
        }
    }

    /// The slot in progress at `timestamp`.
    #[must_use]
    pub const fn slot_at(&self, timestamp: u64) -> u64 {
        timestamp.saturating_sub(self.genesis_time) / self.seconds
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
    #[serde(default = "enabled")]
    pub discv4: bool,
    /// A Discv5 packet protocol id other than `discv5`, which partitions
    /// the chain's discovery network (Base uses `basev0`).
    #[serde(default)]
    pub discv5_protocol_id: Option<String>,
}

const fn enabled() -> bool {
    true
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
    /// JSON-RPC endpoints for chains without public execution P2P.
    #[serde(default)]
    pub rpc_endpoints: Vec<Url>,
}

/// Column layout of a public Parquet copy of a chain's history.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum DatasetLayout {
    /// AWS's own Ethereum export: `blocks`, `transactions` with receipt
    /// columns, and `logs` with a topics list.
    AwsEthereumV1,
    /// `SonarX`'s EVM export: upper-case columns, decimal numbers, and
    /// separate `receipts`.
    SonarxEvmV1,
}

/// Where a chain's history sits in a public, daily-partitioned Parquet set.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PublicDataset {
    pub layout: DatasetLayout,
    /// Object prefix holding one `<table>/date=YYYY-MM-DD/` tree per table.
    pub prefix: String,
    /// First block the rows can be checked from: earlier blocks follow
    /// rules the dataset cannot reproduce (Arbitrum Classic, pre-Byzantium
    /// receipts).
    #[serde(default)]
    pub first_block: u64,
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
    pub op_stack: Option<OpStackNetwork>,
    pub public_dataset: Option<PublicDataset>,
    pub defaults: ChainDefaults,
}

impl Chain {
    /// Parse an entry file. `upstream` is the chain's upstream config: the
    /// consensus `config.yaml` for an entry with a `[beacon]` section, the
    /// superchain-registry chain config for an op-stack entry.
    ///
    /// # Errors
    ///
    /// Returns an error when the entry or its upstream config is malformed.
    pub fn from_entry(entry: &str, upstream: Option<&str>) -> Result<Self, ChainError> {
        let entry: EntryFile =
            toml::from_str(entry).map_err(|error| ChainError::InvalidEntry(error.to_string()))?;
        let (beacon_config, registry) = match entry.family {
            Family::Ethereum => (upstream, None),
            Family::OpStack => (None, upstream),
            Family::Arbitrum => (None, None),
        };
        let op_stack = match (entry.op_stack, registry) {
            (Some(op_stack), Some(registry)) => Some(OpStackNetwork::from_registry(
                entry.chain_id,
                op_stack,
                registry,
            )?),
            (None, None) if entry.family != Family::OpStack => None,
            _ => {
                return Err(ChainError::InvalidEntry(format!(
                    "{} needs an [op_stack] section and its registry config",
                    entry.name
                )));
            }
        };
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
            op_stack,
            public_dataset: entry.public_dataset,
            defaults: entry.defaults,
        })
    }

    /// The chain's block cadence: whole seconds per block from genesis.
    ///
    /// # Errors
    ///
    /// Returns an error for a chain whose blocks do not follow a whole-second
    /// cadence, such as a Nitro chain's.
    pub fn cadence(&self) -> Result<Cadence, ChainError> {
        match (&self.beacon, &self.op_stack) {
            (Some(beacon), _) => Ok(beacon.cadence()),
            (None, Some(op_stack)) => Ok(op_stack.cadence()),
            (None, None) => Err(ChainError::Unsupported(format!(
                "{} has no whole-second block cadence",
                self.name
            ))),
        }
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
    op_stack: Option<op_stack::OpStackEntry>,
    #[serde(default)]
    public_dataset: Option<PublicDataset>,
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
    (
        include_str!("../../../config/chains/op-mainnet.toml"),
        Some(include_str!("../../../config/chains/op/op.toml")),
    ),
    (
        include_str!("../../../config/chains/base-mainnet.toml"),
        Some(include_str!("../../../config/chains/op/base.toml")),
    ),
    (
        include_str!("../../../config/chains/unichain-mainnet.toml"),
        Some(include_str!("../../../config/chains/op/unichain.toml")),
    ),
    (
        include_str!("../../../config/chains/worldchain-mainnet.toml"),
        Some(include_str!("../../../config/chains/op/worldchain.toml")),
    ),
    (
        include_str!("../../../config/chains/ink-mainnet.toml"),
        Some(include_str!("../../../config/chains/op/ink.toml")),
    ),
    (
        include_str!("../../../config/chains/soneium-mainnet.toml"),
        Some(include_str!("../../../config/chains/op/soneium.toml")),
    ),
    (
        include_str!("../../../config/chains/zora-mainnet.toml"),
        Some(include_str!("../../../config/chains/op/zora.toml")),
    ),
    (
        include_str!("../../../config/chains/mode-mainnet.toml"),
        Some(include_str!("../../../config/chains/op/mode.toml")),
    ),
    (
        include_str!("../../../config/chains/arbitrum-one.toml"),
        None,
    ),
    (
        include_str!("../../../config/chains/arbitrum-nova.toml"),
        None,
    ),
    (
        include_str!("../../../config/chains/robinhood-mainnet.toml"),
        None,
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

/// The execution chain spec a chain's peers handshake with: genesis, fork
/// id, and (on Ethereum) the header rules.
///
/// # Errors
///
/// Returns an error for an Ethereum-family chain without a built-in spec.
pub fn execution_chain_spec(chain: &Chain) -> Result<Arc<reth_chainspec::ChainSpec>, ChainError> {
    match (chain.family, &chain.op_stack) {
        (Family::Ethereum, _) => ethereum_chain_spec(chain),
        (Family::OpStack, Some(op_stack)) => Ok(Arc::new(op_stack_chain_spec(chain, op_stack))),
        (Family::OpStack, None) => Err(ChainError::Unsupported(format!(
            "{} has no op-stack schedule",
            chain.name
        ))),
        (Family::Arbitrum, _) => Err(ChainError::Unsupported(format!(
            "{} runs no public execution P2P; its blocks come from JSON-RPC",
            chain.name
        ))),
    }
}

/// A spec that only carries what the handshake needs: the genesis hash and
/// every fork in activation order, which is all EIP-2124 hashes.
fn op_stack_chain_spec(chain: &Chain, op_stack: &OpStackNetwork) -> reth_chainspec::ChainSpec {
    use reth_chainspec::{ChainHardforks, ForkCondition, Hardfork};

    #[derive(Clone, Copy, Debug)]
    struct OpFork(&'static str);
    impl Hardfork for OpFork {
        fn name(&self) -> &'static str {
            self.0
        }
    }

    let mut forks: Vec<(Box<dyn Hardfork>, ForkCondition)> = Vec::new();
    let mut blocks = op_stack.block_forks.clone();
    blocks.sort_by_key(|(_, block)| *block);
    for (name, block) in blocks {
        forks.push((Box::new(OpFork(name)), ForkCondition::Block(block)));
    }
    for (name, timestamp) in &op_stack.forks {
        forks.push((Box::new(OpFork(name)), ForkCondition::Timestamp(*timestamp)));
    }
    let genesis_header = alloy_consensus::Header {
        timestamp: op_stack.first_block_time,
        ..alloy_consensus::Header::default()
    };
    reth_chainspec::ChainSpec {
        chain: reth_chainspec::Chain::from_id(chain.chain_id),
        genesis_header: reth_primitives_traits::SealedHeader::new(
            genesis_header,
            chain.genesis_hash,
        ),
        hardforks: ChainHardforks::new(forks),
        ..reth_chainspec::ChainSpec::default()
    }
}

#[cfg(test)]
mod tests;
