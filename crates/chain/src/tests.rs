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
    assert_eq!(beacon.slot_starting_at(1_606_824_023 + 24), Some(2));
    assert_eq!(beacon.slot_starting_at(1_606_824_023 + 25), None);
}

#[test]
fn hoodi_digest_matches_a_live_node_record() {
    let hoodi = ChainRegistry::standard().get("hoodi").unwrap();
    let beacon = hoodi.beacon().unwrap();
    // The `eth2` field of a Lodestar Hoodi node record at epoch 131409: Fulu
    // with the second blob-parameter fork, Gloas next at 132352.
    assert_eq!(
        beacon.fork_digest(131_409 * SLOTS_PER_EPOCH),
        [0xc6, 0xec, 0xb7, 0x6c]
    );
    assert_eq!(beacon.fork("gloas").unwrap().epoch, 132_352);
    assert_eq!(
        beacon.fork("gloas").unwrap().version,
        [0x80, 0x00, 0x09, 0x10]
    );
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
        let spec = ethereum_chain_spec(chain).unwrap();
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
