use alloy_primitives::B256;
use sha2::{Digest, Sha256};

use crate::ChainError;

pub const SLOTS_PER_EPOCH: u64 = 32;
/// `FAR_FUTURE_EPOCH`: a fork the config names but does not schedule.
const FAR_FUTURE_EPOCH: u64 = u64::MAX;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeaconFork {
    /// Lowercase consensus fork name, as Beacon API `version` fields spell it.
    pub name: String,
    pub version: [u8; 4],
    pub epoch: u64,
}

/// A beacon chain's identity and schedule, read from the network's upstream
/// consensus `config.yaml`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeaconNetwork {
    pub config_name: String,
    pub genesis_time: u64,
    pub seconds_per_slot: u64,
    pub genesis_validators_root: B256,
    /// Scheduled forks in activation order, `phase0` first.
    pub forks: Vec<BeaconFork>,
    pub max_blobs_per_block_electra: u64,
    /// EIP-7892 blob-parameter-only forks as `(epoch, max blobs per block)`.
    pub blob_schedule: Vec<(u64, u64)>,
}

impl BeaconNetwork {
    /// Parse the keys Leani uses from an upstream `config.yaml`.
    ///
    /// # Errors
    ///
    /// Returns an error when a used key is missing or malformed.
    pub fn from_config_yaml(
        genesis_time: u64,
        genesis_validators_root: B256,
        yaml: &str,
    ) -> Result<Self, ChainError> {
        let invalid = |message: String| ChainError::InvalidEntry(message);
        let mut config_name = None;
        let mut seconds_per_slot = None;
        let mut max_blobs_per_block_electra = None;
        let mut versions = Vec::new();
        let mut epochs = Vec::new();
        let mut blob_schedule = Vec::new();
        let mut in_blob_schedule = false;
        for line in yaml.lines() {
            let line = line.split('#').next().unwrap_or_default().trim_end();
            if line.trim().is_empty() {
                continue;
            }
            let indented = line.starts_with(' ') || line.starts_with('-');
            let Some((key, value)) = line.trim().trim_start_matches("- ").split_once(':') else {
                return Err(invalid(format!("unreadable beacon config line {line:?}")));
            };
            let (key, value) = (key.trim(), value.trim().trim_matches('\''));
            if in_blob_schedule && indented {
                let number = parse_u64(value)?;
                match key {
                    "EPOCH" => blob_schedule.push((number, 0)),
                    "MAX_BLOBS_PER_BLOCK" => {
                        let last = blob_schedule.last_mut().ok_or_else(|| {
                            invalid("blob schedule entry has no epoch".to_owned())
                        })?;
                        last.1 = number;
                    }
                    _ => return Err(invalid(format!("unknown blob schedule key {key}"))),
                }
                continue;
            }
            in_blob_schedule = key == "BLOB_SCHEDULE";
            if let Some(fork) = key.strip_suffix("_FORK_VERSION") {
                versions.push((fork.to_ascii_lowercase(), parse_version(value)?));
            } else if let Some(fork) = key.strip_suffix("_FORK_EPOCH") {
                epochs.push((fork.to_ascii_lowercase(), parse_u64(value)?));
            } else if key == "CONFIG_NAME" {
                config_name = Some(value.to_owned());
            } else if key == "SECONDS_PER_SLOT" {
                seconds_per_slot = Some(parse_u64(value)?);
            } else if key == "MAX_BLOBS_PER_BLOCK_ELECTRA" {
                max_blobs_per_block_electra = Some(parse_u64(value)?);
            }
        }
        let mut forks = Vec::with_capacity(versions.len());
        for (name, version) in versions {
            let epoch = if name == "genesis" {
                0
            } else {
                epochs
                    .iter()
                    .find(|(fork, _)| *fork == name)
                    .map(|(_, epoch)| *epoch)
                    .ok_or_else(|| invalid(format!("fork {name} has a version but no epoch")))?
            };
            if epoch == FAR_FUTURE_EPOCH {
                continue;
            }
            let name = if name == "genesis" {
                "phase0".to_owned()
            } else {
                name
            };
            forks.push(BeaconFork {
                name,
                version,
                epoch,
            });
        }
        forks.sort_by_key(|fork| fork.epoch);
        if forks.first().is_none_or(|fork| fork.name != "phase0") {
            return Err(invalid(
                "beacon config has no genesis fork version".to_owned(),
            ));
        }
        let network = Self {
            config_name: config_name.ok_or_else(|| invalid("missing CONFIG_NAME".to_owned()))?,
            genesis_time,
            seconds_per_slot: seconds_per_slot
                .filter(|seconds| *seconds > 0)
                .ok_or_else(|| invalid("missing SECONDS_PER_SLOT".to_owned()))?,
            genesis_validators_root,
            forks,
            max_blobs_per_block_electra: max_blobs_per_block_electra.unwrap_or_default(),
            blob_schedule,
        };
        Ok(network)
    }

    #[must_use]
    pub const fn cadence(&self) -> crate::Cadence {
        crate::Cadence {
            genesis_time: self.genesis_time,
            seconds: self.seconds_per_slot,
        }
    }

    #[must_use]
    pub const fn slot_start(&self, slot: u64) -> u64 {
        self.genesis_time
            .saturating_add(slot.saturating_mul(self.seconds_per_slot))
    }

    /// The slot that starts at `timestamp`, or `None` before genesis or off a
    /// slot boundary.
    #[must_use]
    pub const fn slot_starting_at(&self, timestamp: u64) -> Option<u64> {
        match timestamp.checked_sub(self.genesis_time) {
            Some(since) if since % self.seconds_per_slot == 0 => {
                Some(since / self.seconds_per_slot)
            }
            _ => None,
        }
    }

    /// The slot in progress at `timestamp`.
    #[must_use]
    pub const fn slot_at(&self, timestamp: u64) -> u64 {
        timestamp.saturating_sub(self.genesis_time) / self.seconds_per_slot
    }

    #[must_use]
    pub fn fork(&self, name: &str) -> Option<&BeaconFork> {
        self.forks.iter().find(|fork| fork.name == name)
    }

    /// The fork active at `epoch`.
    #[must_use]
    pub fn fork_at_epoch(&self, epoch: u64) -> &BeaconFork {
        self.forks
            .iter()
            .rev()
            .find(|fork| fork.epoch <= epoch)
            .unwrap_or(&self.forks[0])
    }

    /// The four-byte P2P fork digest at `slot`.
    #[must_use]
    pub fn fork_digest(&self, slot: u64) -> [u8; 4] {
        let epoch = slot / SLOTS_PER_EPOCH;
        let mut fork_data = [0_u8; 64];
        fork_data[..4].copy_from_slice(&self.fork_at_epoch(epoch).version);
        fork_data[32..].copy_from_slice(self.genesis_validators_root.as_slice());
        let mut digest = Sha256::digest(fork_data);
        if let Some(fulu) = self.fork("fulu")
            && epoch >= fulu.epoch
        {
            // EIP-7892 folds the active blob parameters into the digest, so
            // blob-parameter-only forks separate P2P traffic without a new
            // fork version.
            let electra = self.fork("electra").map_or(0, |fork| fork.epoch);
            let (parameter_epoch, maximum_blobs) = self
                .blob_schedule
                .iter()
                .rev()
                .copied()
                .find(|(activation, _)| epoch >= *activation)
                .unwrap_or((electra, self.max_blobs_per_block_electra));
            let mut parameters = [0_u8; 16];
            parameters[..8].copy_from_slice(&parameter_epoch.to_le_bytes());
            parameters[8..].copy_from_slice(&maximum_blobs.to_le_bytes());
            let mask = Sha256::digest(parameters);
            for (byte, mask) in digest.iter_mut().zip(mask) {
                *byte ^= mask;
            }
        }
        [digest[0], digest[1], digest[2], digest[3]]
    }
}

fn parse_u64(value: &str) -> Result<u64, ChainError> {
    value
        .parse()
        .map_err(|_| ChainError::InvalidEntry(format!("{value:?} is not an unsigned integer")))
}

fn parse_version(value: &str) -> Result<[u8; 4], ChainError> {
    let invalid = || ChainError::InvalidEntry(format!("{value:?} is not a 4-byte fork version"));
    let hex = value.strip_prefix("0x").ok_or_else(invalid)?;
    let number = u32::from_str_radix(hex, 16).map_err(|_| invalid())?;
    if hex.len() != 8 {
        return Err(invalid());
    }
    Ok(number.to_be_bytes())
}
