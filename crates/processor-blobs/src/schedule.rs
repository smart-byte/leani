//! Versioned Ethereum blob-parameter schedules.

use serde::{Deserialize, Serialize};

use crate::BLOB_GAS_PER_BLOB;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BlobFork {
    pub name: String,
    pub activation_block: u64,
    pub activation_timestamp: u64,
    /// Four-byte EIP-2124/EIP-6122 fork hash, encoded as eight lower-case hex digits.
    pub fork_id: String,
    pub target_blobs_per_block: u32,
    pub max_blobs_per_block: u32,
    pub base_fee_update_fraction: u64,
    pub eip7918: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlobParameters {
    pub target_blobs_per_block: u32,
    pub max_blobs_per_block: u32,
    pub target_blob_gas: u64,
    pub max_blob_gas: u64,
    pub base_fee_update_fraction: u64,
    pub eip7918: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BlobSchedule {
    pub chain_id: u64,
    pub network: String,
    pub forks: Vec<BlobFork>,
}

impl BlobSchedule {
    #[must_use]
    pub fn mainnet() -> Self {
        Self {
            chain_id: 1,
            network: "mainnet".to_owned(),
            forks: vec![
                BlobFork {
                    name: "dencun".to_owned(),
                    activation_block: 19_426_589,
                    activation_timestamp: 1_710_338_135,
                    fork_id: "9f3d2254".to_owned(),
                    target_blobs_per_block: 3,
                    max_blobs_per_block: 6,
                    base_fee_update_fraction: 3_338_477,
                    eip7918: false,
                },
                BlobFork {
                    name: "pectra".to_owned(),
                    activation_block: 22_431_084,
                    activation_timestamp: 1_746_612_311,
                    fork_id: "c376cf8b".to_owned(),
                    target_blobs_per_block: 6,
                    max_blobs_per_block: 9,
                    base_fee_update_fraction: 5_007_716,
                    eip7918: false,
                },
                BlobFork {
                    name: "fusaka".to_owned(),
                    activation_block: 23_935_694,
                    activation_timestamp: 1_764_798_551,
                    fork_id: "5167e2a6".to_owned(),
                    target_blobs_per_block: 6,
                    max_blobs_per_block: 9,
                    base_fee_update_fraction: 5_007_716,
                    eip7918: true,
                },
                BlobFork {
                    name: "bpo1".to_owned(),
                    activation_block: 23_975_778,
                    activation_timestamp: 1_765_290_071,
                    fork_id: "cba2a1c0".to_owned(),
                    target_blobs_per_block: 10,
                    max_blobs_per_block: 15,
                    base_fee_update_fraction: 8_346_193,
                    eip7918: true,
                },
                BlobFork {
                    name: "bpo2".to_owned(),
                    activation_block: 24_179_383,
                    activation_timestamp: 1_767_747_671,
                    fork_id: "07c9462e".to_owned(),
                    target_blobs_per_block: 14,
                    max_blobs_per_block: 21,
                    base_fee_update_fraction: 11_684_671,
                    eip7918: true,
                },
            ],
        }
    }

    /// Validate ordering and schedule values.
    ///
    /// # Errors
    ///
    /// Returns a static reason for malformed schedule configuration.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.chain_id == 0 || self.network.is_empty() || self.forks.is_empty() {
            return Err("schedule identity and forks are required");
        }
        let mut previous_block = None;
        let mut previous_timestamp = None;
        for fork in &self.forks {
            if fork.name.is_empty()
                || fork.activation_block == 0
                || fork.activation_timestamp == 0
                || fork.fork_id.len() != 8
                || !fork
                    .fork_id
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                || fork.target_blobs_per_block == 0
                || fork.max_blobs_per_block < fork.target_blobs_per_block
                || fork.base_fee_update_fraction == 0
                || previous_block.is_some_and(|block| fork.activation_block <= block)
                || previous_timestamp
                    .is_some_and(|timestamp| fork.activation_timestamp <= timestamp)
            {
                return Err("blob forks must be valid and strictly ordered");
            }
            previous_block = Some(fork.activation_block);
            previous_timestamp = Some(fork.activation_timestamp);
        }
        Ok(())
    }

    #[must_use]
    pub fn first_block(&self) -> u64 {
        self.forks.first().map_or(0, |fork| fork.activation_block)
    }

    #[must_use]
    pub fn parameters(&self, block_number: u64) -> Option<BlobParameters> {
        self.fork_at_block(block_number).map(|fork| BlobParameters {
            target_blobs_per_block: fork.target_blobs_per_block,
            max_blobs_per_block: fork.max_blobs_per_block,
            target_blob_gas: u64::from(fork.target_blobs_per_block)
                .saturating_mul(BLOB_GAS_PER_BLOB),
            max_blob_gas: u64::from(fork.max_blobs_per_block).saturating_mul(BLOB_GAS_PER_BLOB),
            base_fee_update_fraction: fork.base_fee_update_fraction,
            eip7918: fork.eip7918,
        })
    }

    #[must_use]
    pub fn fork_at_block(&self, block_number: u64) -> Option<&BlobFork> {
        self.forks
            .iter()
            .rev()
            .find(|fork| block_number >= fork.activation_block)
    }

    #[must_use]
    pub fn fork_at_timestamp(&self, timestamp: u64) -> Option<&BlobFork> {
        self.forks
            .iter()
            .rev()
            .find(|fork| timestamp >= fork.activation_timestamp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mainnet_schedule_selects_historical_parameters() {
        let schedule = BlobSchedule::mainnet();
        schedule.validate().expect("valid");
        assert!(schedule.parameters(schedule.first_block() - 1).is_none());
        let dencun = schedule.parameters(19_426_589).expect("Dencun");
        assert_eq!(dencun.target_blobs_per_block, 3);
        assert_eq!(dencun.max_blobs_per_block, 6);
        assert!(!dencun.eip7918);
        assert_eq!(
            schedule
                .parameters(23_975_777)
                .expect("pre-BPO1")
                .target_blobs_per_block,
            6
        );
        assert_eq!(
            schedule
                .parameters(23_975_778)
                .expect("BPO1")
                .target_blobs_per_block,
            10
        );
        assert_eq!(
            schedule
                .parameters(24_179_382)
                .expect("pre-BPO2")
                .target_blobs_per_block,
            10
        );
        let bpo2 = schedule.parameters(24_179_383).expect("BPO2");
        assert_eq!(bpo2.target_blobs_per_block, 14);
        assert!(bpo2.eip7918);
        assert_eq!(
            schedule
                .fork_at_timestamp(1_767_747_670)
                .expect("pre-BPO2")
                .name,
            "bpo1"
        );
        assert_eq!(
            schedule
                .fork_at_timestamp(1_767_747_671)
                .expect("BPO2")
                .fork_id,
            "07c9462e"
        );
    }
}
