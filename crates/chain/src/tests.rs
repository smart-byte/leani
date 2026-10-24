use alloy_primitives::b256;

use super::*;

#[test]
fn registry_resolves_names_and_aliases() {
    let registry = ChainRegistry::standard();
    assert_eq!(registry.get("ethereum-hoodi").unwrap().chain_id, 560_048);
    assert_eq!(registry.get("hoodi").unwrap().chain_id, 560_048);
    assert_eq!(registry.get("Mainnet").unwrap().chain_id, 1);
    assert_eq!(registry.get("sepolia").unwrap().chain_id, 11_155_111);
    let unknown = registry.get("dogecoin").unwrap_err().to_string();
    assert!(unknown.contains("dogecoin"), "{unknown}");
    assert!(
        unknown.contains("ethereum-mainnet, ethereum-sepolia, ethereum-hoodi"),
        "{unknown}"
    );
}

#[test]
fn mainnet_entry_matches_its_beacon_chain() {
    let chain = ethereum_mainnet();
    let beacon = chain.beacon().unwrap();
    assert_eq!(beacon.genesis_time, 1_606_824_023);
    assert_eq!(beacon.seconds_per_slot, 12);
    assert_eq!(
        beacon.genesis_validators_root,
        b256!("4b363db94e286120d76eb905340fdd4e54bfe9f06bf33ff6cf5ad27f511bfe95")
    );
    let epochs: Vec<_> = beacon
        .forks
        .iter()
        .map(|fork| (fork.name.as_str(), fork.epoch, fork.version[0]))
        .collect();
    assert_eq!(
        epochs,
        [
            ("phase0", 0, 0),
            ("altair", 74_240, 1),
            ("bellatrix", 144_896, 2),
            ("capella", 194_048, 3),
            ("deneb", 269_568, 4),
            ("electra", 364_032, 5),
            ("fulu", 411_392, 6),
        ]
    );
    assert_eq!(beacon.blob_schedule, [(412_672, 15), (419_072, 21)]);
    assert_eq!(beacon.fork_digest(0), [0xb5, 0x30, 0x3f, 0x2a]);
    assert_eq!(
        beacon.fork_digest(269_568 * SLOTS_PER_EPOCH),
        [0x6a, 0x95, 0xa1, 0xa9]
    );
    assert_eq!(
        beacon.fork_digest(411_392 * SLOTS_PER_EPOCH),
        [0xcc, 0x2c, 0x5c, 0xdb]
    );
    assert_eq!(
        beacon.fork_digest(412_672 * SLOTS_PER_EPOCH),
        [0xcb, 0x0d, 0x1a, 0xcc]
    );
    assert_eq!(
        beacon.fork_digest(419_072 * SLOTS_PER_EPOCH),
        [0x8c, 0x9f, 0x62, 0xfe]
    );
    assert_eq!(beacon.slot_starting_at(1_606_824_023 + 24), Some(2));
    assert_eq!(beacon.slot_starting_at(1_606_824_023 + 25), None);
}

#[test]
fn hoodi_digest_matches_a_live_node_record() {
    let hoodi = ChainRegistry::standard().get("hoodi").unwrap();
    let beacon = hoodi.beacon().unwrap();
    // The `eth2` field of a Lodestar Hoodi node record at epoch 131409: Fulu
    // with the second blob-parameter fork.
    assert_eq!(
        beacon.fork_digest(131_409 * SLOTS_PER_EPOCH),
        [0xc6, 0xec, 0xb7, 0x6c]
    );
}

#[test]
fn execution_and_consensus_glamsterdam_activate_together() {
    use reth_chainspec::{EthereumHardfork, ForkCondition, Hardforks as _};

    for chain in ChainRegistry::standard()
        .iter()
        .filter(|chain| chain.beacon.is_some())
    {
        let beacon = chain.beacon().unwrap();
        let gloas = beacon.fork("gloas").map(|fork| {
            beacon.genesis_time + fork.epoch * SLOTS_PER_EPOCH * beacon.seconds_per_slot
        });
        let amsterdam = match execution_chain_spec(chain)
            .unwrap()
            .fork(EthereumHardfork::Amsterdam)
        {
            ForkCondition::Timestamp(timestamp) => Some(timestamp),
            _ => None,
        };
        assert_eq!(amsterdam, gloas, "{}", chain.name);
    }
    // Sepolia forked at epoch 353024; Hoodi's fork is not scheduled.
    let sepolia = ChainRegistry::standard().get("sepolia").unwrap();
    assert_eq!(
        sepolia.beacon().unwrap().fork("gloas").unwrap().epoch,
        353_024
    );
    let hoodi = ChainRegistry::standard().get("hoodi").unwrap();
    assert!(hoodi.beacon().unwrap().fork("gloas").is_none());
}

#[test]
fn testnet_schedules_match_their_beacon_apis() {
    // `/eth/v1/config/fork_schedule` of Lodestar's public nodes.
    let sepolia = ChainRegistry::standard().get("sepolia").unwrap();
    let forks: Vec<_> = sepolia
        .beacon()
        .unwrap()
        .forks
        .iter()
        .map(|fork| (u32::from_be_bytes(fork.version), fork.epoch))
        .collect();
    assert_eq!(
        forks,
        [
            (0x9000_0069, 0),
            (0x9000_0070, 50),
            (0x9000_0071, 100),
            (0x9000_0072, 56_832),
            (0x9000_0073, 132_608),
            (0x9000_0074, 222_464),
            (0x9000_0075, 272_640),
            (0x9000_0076, 353_024),
        ]
    );
    let hoodi = ChainRegistry::standard().get("hoodi").unwrap();
    assert_eq!(hoodi.beacon().unwrap().genesis_time, 1_742_213_400);
    assert_eq!(
        hoodi.beacon().unwrap().fork("electra").unwrap().epoch,
        2_048
    );
}

#[test]
fn execution_specs_match_entry_genesis() {
    for chain in ChainRegistry::standard().iter() {
        if chain.family == Family::Arbitrum {
            continue;
        }
        let spec = execution_chain_spec(chain).unwrap();
        assert_eq!(
            reth_chainspec::EthChainSpec::chain_id(spec.as_ref()),
            chain.chain_id,
            "{}",
            chain.name
        );
    }
}

#[test]
fn entry_with_beacon_section_needs_its_config() {
    let entry = "name = \"x\"\nfamily = \"ethereum\"\nchain_id = 7\ngenesis_hash = \"0x0000000000000000000000000000000000000000000000000000000000000000\"\n[beacon]\ngenesis_time = 1\ngenesis_validators_root = \"0x0000000000000000000000000000000000000000000000000000000000000000\"\n";
    assert!(matches!(
        Chain::from_entry(entry, None),
        Err(ChainError::InvalidEntry(_))
    ));
}

#[test]
fn op_stack_fork_ids_match_live_peers() {
    // Fork ids once every scheduled fork is active (EIP-2124); OP Mainnet
    // and Unichain were verified live against their peers.
    for (name, hash) in [
        ("op-mainnet", [0xc2, 0x92, 0x39, 0xaf]),
        ("base", [0x16, 0x9d, 0x85, 0xa3]),
        ("unichain", [0x1f, 0xaa, 0x45, 0x6e]),
        ("worldchain", [0xf1, 0x13, 0x24, 0x8b]),
    ] {
        let chain = ChainRegistry::standard().get(name).unwrap();
        let spec = execution_chain_spec(&chain).unwrap();
        let fork_id = spec.latest_fork_id();
        assert_eq!((fork_id.hash.0, fork_id.next), (hash, 0), "{name}");
        assert_eq!(
            reth_chainspec::EthChainSpec::genesis_hash(spec.as_ref()),
            chain.genesis_hash
        );
    }
}

#[test]
fn base_announces_cobalt_until_denim() {
    // Synced Base nodes serve a node announcing this id; the schedule
    // without Cobalt (`ea0159eb`) reads as stale software to them.
    let base = ChainRegistry::standard().get("base").unwrap();
    let spec = execution_chain_spec(&base).unwrap();
    let head = reth_chainspec::Head {
        timestamp: 1_792_962_127,
        ..Default::default()
    };
    let fork_id = spec.fork_id(&head);
    assert_eq!(
        (fork_id.hash.0, fork_id.next),
        ([0x68, 0x64, 0x7e, 0x86], 1_793_901_600)
    );
}

#[test]
fn op_stack_entries_carry_their_schedule() {
    let op = ChainRegistry::standard().get("optimism").unwrap();
    let schedule = op.op_stack.as_ref().unwrap();
    assert_eq!(schedule.first_block, 105_235_063);
    assert_eq!(schedule.block_time, 2);
    assert_eq!(schedule.activation("isthmus"), Some(1_746_806_401));
    assert_eq!(
        schedule.activation("delta"),
        None,
        "consensus-only forks are skipped"
    );
    // Block n of the rollup is due at its slot of the cadence.
    let cadence = op.cadence().unwrap();
    assert_eq!(cadence.slot_start(10), 1_686_068_903 + 20);
    let base = ChainRegistry::standard().get("base").unwrap();
    assert!(!base.execution.discv4);
    assert_eq!(base.execution.discv5_protocol_id.as_deref(), Some("basev0"));
    assert_eq!(base.cadence().unwrap().slot_at(1_686_789_347 + 7), 3);
}

#[test]
fn an_unknown_op_stack_fork_needs_a_release() {
    let entry = include_str!("../../../config/chains/unichain-mainnet.toml");
    let registry = include_str!("../../../config/chains/op/unichain.toml")
        .replace("karst_time", "lagoon_time");
    let error = Chain::from_entry(entry, Some(&registry)).unwrap_err();
    assert!(
        error.to_string().contains("unknown op-stack fork lagoon"),
        "{error}"
    );
}

#[test]
fn nitro_chains_name_their_rpc_endpoints() {
    let robinhood = ChainRegistry::standard().get("robinhood").unwrap();
    assert_eq!(
        (robinhood.family, robinhood.chain_id),
        (Family::Arbitrum, 4663)
    );
    assert_eq!(
        robinhood.defaults.rpc_endpoints[0].as_str(),
        "https://rpc.mainnet.chain.robinhood.com/"
    );
    assert!(execution_chain_spec(&robinhood).is_err());
    assert_eq!(
        ChainRegistry::standard().get("arb1").unwrap().chain_id,
        42_161
    );
}

#[test]
fn public_datasets_name_their_layout() {
    let registry = ChainRegistry::standard();
    let datasets = [
        "ethereum-mainnet",
        "base-mainnet",
        "arbitrum-one",
        "robinhood",
    ]
    .map(|name| registry.get(name).unwrap().public_dataset.clone());
    let [Some(ethereum), Some(base), Some(arbitrum), None] = datasets else {
        panic!("{datasets:?}");
    };
    assert_eq!(
        (
            ethereum.layout,
            ethereum.prefix.as_str(),
            ethereum.first_block
        ),
        (DatasetLayout::AwsEthereumV1, "v1.0/eth", 4_370_000)
    );
    assert_eq!(base.layout, DatasetLayout::SonarxEvmV1);
    assert_eq!(arbitrum.first_block, 22_207_817);
}

#[test]
fn rollups_name_their_l1_anchor() {
    let registry = ChainRegistry::standard();
    assert_eq!(
        registry.get("base").unwrap().l1_anchor,
        Some(L1Anchor::DisputeGames {
            factory: "0x43edB88C4B80fDD2AdFF2412A7BebF9dF42cB40e"
                .parse()
                .unwrap(),
            registry: "0x909f6cf47ed12f010a796527f562bfc26c7f4e72"
                .parse()
                .unwrap(),
            game_type: 621,
        })
    );
    assert!(matches!(
        registry.get("robinhood").unwrap().l1_anchor,
        Some(L1Anchor::NitroRollup { .. })
    ));
    // OP Mainnet's respected games claim super roots, which the anchor does
    // not read.
    assert_eq!(registry.get("op-mainnet").unwrap().l1_anchor, None);
    let misplaced = include_str!("../../../config/chains/arbitrum-one.toml")
        .replace("family = \"arbitrum\"", "family = \"ethereum\"");
    assert!(Chain::from_entry(&misplaced, None).is_err());
}
