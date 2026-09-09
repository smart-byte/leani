//! Regenerate illustrated wire output using the configured production processors.
use std::{fs, path::Path};

use alloy_primitives::{U256, keccak256};
use leani::{Config, ProcessorRegistry};
use leani_primitives::{
    Address, BlockNumber, ChangeCursor, Log, Material, ProcessorCursor, TransactionHash,
};
use leani_processor_api::DeliveryPolicyMode;
use leani_store_sqlite::{
    ChangeDirection, ChangeRecord, DELIVERY_ENCODING_VERSION, DeliveryOrigin, DeliveryOriginKind,
};
use leani_testkit::{GeneratedHistorySource, MemoryReducer, SyntheticCorpusKind};
use serde_json::{Value, json};

fn check_or_update(path: &Path, expected: &Value) {
    if std::env::var_os("LEANI_UPDATE_DOC_FIXTURES").is_some() {
        fs::write(
            path,
            format!("{}\n", serde_json::to_string_pretty(expected).unwrap()),
        )
        .unwrap();
    } else {
        let actual: Value = serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap();
        assert_eq!(
            actual,
            *expected,
            "{}: run LEANI_UPDATE_DOC_FIXTURES=1 cargo test -p leani --test showcase_contracts",
            path.display()
        );
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn showcase_configs_produce_the_illustrated_change_contracts() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let base = fs::read_to_string(root.join("config/modes/windowed.toml")).unwrap();
    let prefix = base.split("[[processors]]").next().unwrap();
    let suffix = &base[base.find("[rpc]").unwrap()..];
    for id in [
        "blobs-money",
        "uniswap-latest",
        "erc20-balances",
        "evm-events",
        "transaction-stats",
    ] {
        let path = root.join(format!("site/src/data/processors/{id}.json"));
        let mut demo: Value = serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let config_path = directory.path().join("node.toml");
        fs::write(
            &config_path,
            format!("{prefix}{}\n{suffix}", demo["config"].as_str().unwrap()),
        )
        .unwrap();
        let config = Config::load(&config_path)
            .unwrap()
            .validate()
            .unwrap()
            .into_inner();
        let processor = ProcessorRegistry::standard()
            .instantiate(&config.processors[0], 1)
            .unwrap();
        let descriptor = processor.descriptor();
        assert_ne!(
            descriptor.lifecycle.delivery.mode,
            DeliveryPolicyMode::None,
            "{id} advertises SSE"
        );
        let kind = match id {
            "blobs-money" => SyntheticCorpusKind::BlobsLike,
            "uniswap-latest" => SyntheticCorpusKind::UniswapLike,
            _ => SyntheticCorpusKind::Dense,
        };
        let (source, _) = GeneratedHistorySource::new(kind, 1, 42, 1).unwrap();
        let mut frame = source.frame(BlockNumber(19_430_000));
        frame.block.timestamp = 1_710_500_000;
        if id == "blobs-money" {
            let Material::Complete(transactions) = &mut frame.transactions else {
                panic!("transactions");
            };
            let Material::Complete(receipts) = &mut frame.receipts else {
                panic!("receipts");
            };
            let Material::Complete(header) = &mut frame.header else {
                panic!("header");
            };
            transactions.truncate(1);
            receipts.truncate(1);
            header.blob_gas_used =
                Some(u64::try_from(transactions[0].blob_versioned_hashes.len()).unwrap() * 131_072);
            header.transaction_count = Some(1);
            header.gas_used = receipts[0].gas_used;
        }
        if id == "transaction-stats" {
            let Material::Complete(transactions) = &mut frame.transactions else {
                panic!("fixture transactions");
            };
            transactions[0].from = Some(Address::new([0x11; 20]));
            transactions[0].to = Some(Address::new([0x22; 20]));
            transactions[0].value = Some(U256::from(42).into());
        }
        if id == "erc20-balances" || id == "evm-events" {
            let token = if id == "erc20-balances" {
                Address::new([0x22; 20])
            } else {
                Address::from(
                    "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
                        .parse::<alloy_primitives::Address>()
                        .unwrap(),
                )
            };
            let mut recipient = [0_u8; 32];
            recipient[12..].fill(0x11);
            frame.logs = Material::Complete(vec![Log {
                address: token,
                topics: vec![
                    keccak256(b"Transfer(address,address,uint256)").0,
                    [0; 32],
                    recipient,
                ],
                data: U256::from(42).to_be_bytes::<32>().to_vec(),
                transaction_hash: Some(TransactionHash::new([0x33; 32])),
                transaction_index: 0,
                log_index: 0,
            }]);
        }
        let cursor = ProcessorCursor {
            processor_id: descriptor.id.to_string(),
            processor_version: descriptor.version.to_string(),
            chain_id: frame.chain_id,
            block_number: frame.block.number,
            block_hash: frame.block.hash,
            finality: frame.finality,
            sequence: 1,
        };
        let delta = processor.map(&frame).await.unwrap();
        let changes = processor
            .reduce(&mut MemoryReducer::default(), &cursor, &delta)
            .await
            .unwrap();
        let change = changes
            .changes
            .into_iter()
            .next()
            .unwrap_or_else(|| panic!("{id}: fixture must produce a change"));
        let record = ChangeRecord {
            delivery_encoding_version: DELIVERY_ENCODING_VERSION,
            cursor: ChangeCursor {
                chain_id: frame.chain_id,
                processor_id: descriptor.id.to_string(),
                sequence: 1,
            },
            origin: DeliveryOrigin {
                kind: DeliveryOriginKind::HistoricalBackfill,
                id: "illustrative-fixture".to_owned(),
                publication_revision: 0,
            },
            block: frame.block,
            finality: frame.finality,
            direction: ChangeDirection::Apply,
            change,
            emitted_at_unix_ms: frame.block.timestamp * 1_000,
        };
        let wire = leani_api::change_envelope_json([0; 16], processor.as_ref(), record).unwrap();
        demo["sseId"] = wire["cursor"].clone();
        demo["outputJson"] = wire;
        let summary = &mut demo["helloJson"]["processor"];
        summary["codeHash"] = json!(descriptor.code_hash.to_string());
        summary["configHash"] = json!(descriptor.config_hash.to_string());
        summary["changeSchema"] = json!(descriptor.schemas.change_schema);
        assert_eq!(summary["instance"], descriptor.instance.as_str());
        assert_eq!(summary["version"], descriptor.version.to_string());
        assert_eq!(summary["subscriptions"], true);
        demo["helloJson"]["coverage"]["available"] = json!([{ "fromBlock": frame.block.number.0, "toBlock": frame.block.number.0, "finality": "finalized" }]);
        demo["helloJson"]["coverage"]["processedThrough"] = json!(frame.block.number.0);
        demo["helloJson"]["coverage"]["finalizedThrough"] = json!(frame.block.number.0);
        demo["helloJson"]["coverage"]["complete"] = json!(false);
        check_or_update(&path, &demo);
        if id == "blobs-money" {
            check_or_update(
                &root.join("packages/sdk/test/fixtures/blobs-change.json"),
                &demo["outputJson"],
            );
        }
    }
}
