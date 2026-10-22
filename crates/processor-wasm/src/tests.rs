use leani_primitives::{
    Address, BlockFrame, BlockHash, BlockNumber, ChainId, Completeness, Finality, HeaderEnvelope,
    Log, Material, MissingReason, ProcessorCursor, ReceiptEnvelope, TransactionHash,
};
use leani_processor_api::{
    ChangeOperation, DeliveryOrdering, DomainChange, LifecyclePolicies, Processor, ProcessorError,
    ProcessorInstanceId, PublicationPolicy, ReductionMode,
};
use leani_store_sqlite::{ChangeDirection, SqliteStore, StoreConfig};
use leani_testkit::fixture_frame;
use semver::Version;
use serde_json::{Value, json};

use crate::{WasmContract, WasmLimits, WasmModule, WasmProcessor};

const PROBE: &[u8] = include_bytes!("../modules/abi_probe.wasm");
const BLOCK_GAS: &[u8] = crate::TEMPLATES[0].module;
const TRANSFERS: &[u8] = crate::TEMPLATES[1].module;
const VOLUME: &[u8] = crate::TEMPLATES[2].module;
const EVENT_DECODER: &[u8] = crate::TEMPLATES[3].module;
const TOKEN: [u8; 20] = [0xaa; 20];
const OTHER: [u8; 20] = [0xbb; 20];

fn contract() -> WasmContract {
    WasmContract {
        instance: ProcessorInstanceId::new("probe").expect("instance"),
        version: Version::new(1, 0, 0),
        start_block: BlockNumber(0),
        publication: PublicationPolicy::Attested,
        lifecycle: LifecyclePolicies::default(),
    }
}

fn processor_with(
    bytes: &[u8],
    params: &Value,
    limits: WasmLimits,
) -> Result<WasmProcessor, ProcessorError> {
    WasmProcessor::new(WasmModule::new(bytes, &limits)?, params, contract(), limits)
}

fn probe(mode: &str) -> WasmProcessor {
    probe_with(mode, WasmLimits::default()).expect("probe processor")
}

fn probe_with(mode: &str, limits: WasmLimits) -> Result<WasmProcessor, ProcessorError> {
    processor_with(PROBE, &json!({ "mode": mode }), limits)
}

fn transfer_topic() -> [u8; 32] {
    *alloy_primitives::keccak256(b"Transfer(address,address,uint256)")
}

fn word(value: u64) -> [u8; 32] {
    let mut word = [0; 32];
    word[24..].copy_from_slice(&value.to_be_bytes());
    word
}

fn topic(address: [u8; 20]) -> [u8; 32] {
    let mut topic = [0; 32];
    topic[12..].copy_from_slice(&address);
    topic
}

fn transfer(token: [u8; 20], value: u64, log_index: u32) -> Log {
    Log {
        address: Address::new(token),
        topics: vec![transfer_topic(), topic([1; 20]), topic([2; 20])],
        data: word(value).to_vec(),
        transaction_hash: Some(TransactionHash::new([9; 32])),
        transaction_index: 0,
        log_index,
    }
}

/// An unfinalized block, which keeps its undo record, with a header, so every
/// requirement here is satisfied.
fn frame(number: u64, parent: BlockHash, logs: Vec<Log>) -> BlockFrame {
    let mut frame = fixture_frame(number, parent);
    frame.finality = Finality::Attested;
    frame.header = Material::Complete(HeaderEnvelope {
        rlp: None,
        transactions_root: None,
        receipts_root: None,
        withdrawals_root: None,
        gas_limit: Some(30_000_000),
        gas_used: Some(21_000),
        base_fee_per_gas: None,
        blob_gas_used: None,
        excess_blob_gas: None,
        size_bytes: None,
        transaction_count: Some(0),
        consensus_size_bytes: None,
    });
    frame.logs = Material::Complete(logs);
    frame
}

fn cursor(processor: &WasmProcessor, frame: &BlockFrame) -> ProcessorCursor {
    ProcessorCursor {
        processor_id: "wasm".to_owned(),
        processor_version: processor.descriptor().version.to_string(),
        chain_id: frame.chain_id,
        block_number: frame.block.number,
        block_hash: frame.block.hash,
        finality: frame.finality,
        sequence: frame.block.number.0,
    }
}

async fn open_store(directory: &tempfile::TempDir) -> SqliteStore {
    SqliteStore::open(StoreConfig::new(directory.path().join("wasm.sqlite")))
        .await
        .expect("store")
}

async fn apply(
    store: &SqliteStore,
    processor: &WasmProcessor,
    frame: &BlockFrame,
) -> Result<(), String> {
    let delta = processor
        .map(frame)
        .await
        .map_err(|error| error.to_string())?;
    store
        .apply(processor, cursor(processor, frame), &delta, &[])
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
}

/// Every applied change as `(block, kind, rendered JSON)`.
async fn changes(store: &SqliteStore, processor: &WasmProcessor) -> Vec<(u64, String, Value)> {
    store
        .changes(processor.descriptor(), ChainId(1), 0, 1_000)
        .await
        .expect("changes")
        .into_iter()
        .filter(|record| record.direction == ChangeDirection::Apply)
        .map(|record| {
            let json = processor
                .change_json(&record.change)
                .expect("render")
                .expect("JSON");
            (record.block.number.0, record.change.kind, json)
        })
        .collect()
}

async fn entity(
    store: &SqliteStore,
    processor: &WasmProcessor,
    collection: &str,
    key: &[u8],
) -> Option<Value> {
    store
        .entity(processor.descriptor(), collection, key)
        .await
        .expect("entity")
        .map(|bytes| {
            processor
                .entity_json(collection, key, &bytes)
                .expect("render")
                .expect("JSON")
        })
}

fn message<T: std::fmt::Debug, E: std::fmt::Display>(result: Result<T, E>) -> String {
    result.expect_err("call fails").to_string()
}

const BLOCK_LOCAL: &str =
    r#"{"abi":1,"name":"host","reduction":"block_local","requires":{"capabilities":["header"]}}"#;
const ORDERED: &str =
    r#"{"abi":1,"name":"host","reduction":"ordered","requires":{"capabilities":["header"]}}"#;
const OUTPUT_NOTHING: &str = "(call $output (i32.const 0) (i32.const 0)) (i32.const 0)";

/// A hand-written module whose `describe` returns `manifest` and whose `map`
/// and `reduce` run the given instructions. Its 2 MiB of memory hold the
/// manifest at 0, the JSON `0` at 128 KiB, and the call input at 1 MiB.
fn host_module(manifest: &str, map: &str, reduce: &str) -> Vec<u8> {
    wat::parse_str(format!(
        r#"(module
  (import "leani" "output" (func $output (param i32 i32)))
  (import "leani" "keccak256" (func $keccak256 (param i32 i32 i32)))
  (import "leani" "read" (func $read (param i32 i32 i32 i32 i32) (result i32)))
  (import "leani" "scan" (func $scan (param i32 i32 i32 i32 i32 i32) (result i32)))
  (import "leani" "write" (func $write (param i32 i32 i32 i32 i32 i32 i32) (result i32)))
  (import "leani" "emit" (func $emit (param i32 i32 i32 i32 i32 i32 i32)))
  (memory (export "memory") 32)
  (data (i32.const 0) "{manifest}")
  (data (i32.const 131072) "0")
  (func (export "leani_alloc") (param i32) (result i32) (i32.const 1048576))
  (func (export "leani_describe") (param i32 i32) (result i32)
    (call $output (i32.const 0) (i32.const {length})) (i32.const 0))
  (func (export "leani_map") (param i32 i32) (result i32) (local $i i32) {map})
  (func (export "leani_reduce") (param i32 i32) (result i32) (local $i i32) {reduce}))"#,
        manifest = manifest.replace('"', "\\\""),
        length = manifest.len(),
    ))
    .expect("wat")
}

fn host_processor(manifest: &str, map: &str, reduce: &str, limits: WasmLimits) -> WasmProcessor {
    WasmProcessor::new(
        WasmModule::new(&host_module(manifest, map, reduce), &limits).expect("module"),
        &json!({}),
        contract(),
        limits,
    )
    .expect("processor")
}

#[test]
fn invalid_oversized_or_foreign_modules_are_refused_before_they_run() {
    let limits = WasmLimits::default();
    let wasi = wat::parse_str(
        r#"(module (import "wasi_snapshot_preview1" "fd_write" (func (param i32 i32 i32 i32) (result i32))))"#,
    )
    .expect("wat");
    assert!(message(WasmModule::new(&wasi, &limits)).contains("not part of the Leani ABI"));
    let bare = wat::parse_str("(module)").expect("wat");
    assert!(message(WasmModule::new(&bare, &limits)).contains("does not export `memory`"));
    assert!(message(WasmModule::new(b"\0asm", &limits)).contains("invalid Wasm module"));
    let tiny = WasmLimits {
        module_bytes: 64,
        ..limits
    };
    assert!(message(WasmModule::new(PROBE, &tiny)).contains("-byte limit"));
    let mistyped = wat::parse_str(
        r#"(module (memory (export "memory") 1)
            (func (export "leani_alloc") (param i32) (result i32) (i32.const 0))
            (func (export "leani_describe") (param i32 i32) (result i32) (i32.const 0))
            (func (export "leani_map") (param i32) (result i32) (i32.const 0))
            (func (export "leani_reduce") (param i32 i32) (result i32) (i32.const 0)))"#,
    )
    .expect("wat");
    assert!(
        message(WasmModule::new(&mistyped, &limits))
            .contains("does not export `leani_map` as the Leani ABI defines it")
    );
}

#[test]
fn every_template_describes_itself_with_its_example_parameters() {
    for template in crate::TEMPLATES {
        let params = serde_json::from_str(template.params).expect("template parameters");
        let processor =
            processor_with(template.module, &params, WasmLimits::default()).expect("template");
        assert_eq!(processor.manifest().name, template.name);
    }
}

#[test]
fn the_descriptor_names_the_module_and_canonical_parameters() {
    let limits = WasmLimits::default();
    let token = Address::new(TOKEN).to_string();
    let first = processor_with(
        TRANSFERS,
        &json!({ "tokens": [token], "minValue": "5" }),
        limits,
    )
    .expect("processor");
    let reordered: Value =
        serde_json::from_str(&format!(r#"{{"minValue":"5","tokens":["{token}"]}}"#))
            .expect("params");
    let same = processor_with(TRANSFERS, &reordered, limits).expect("processor");
    let other = processor_with(TRANSFERS, &json!({ "minValue": "6" }), limits).expect("processor");
    let descriptor = first.descriptor();
    assert_eq!(descriptor.id.as_str(), "wasm");
    assert_eq!(
        descriptor.code_hash,
        BlockHash::new(*blake3::hash(TRANSFERS).as_bytes())
    );
    assert_eq!(descriptor.config_hash, same.descriptor().config_hash);
    assert_ne!(descriptor.config_hash, other.descriptor().config_hash);
    assert_eq!(descriptor.mode, ReductionMode::BlockLocal);
    assert_eq!(
        descriptor.delivery_ordering,
        DeliveryOrdering::BlockVersionedIdempotent
    );
    assert_eq!(
        descriptor.schemas.change_schema,
        "wasm.erc20-transfers.change.v1"
    );
    let requirement = &descriptor.requirements[0];
    assert_eq!(requirement.filter.addresses, vec![Address::new(TOKEN)]);
    assert_eq!(
        requirement.filter.topics[0].alternatives,
        vec![transfer_topic()]
    );

    let ordered = probe("state");
    assert_eq!(ordered.descriptor().mode, ReductionMode::OrderedState);
    assert_eq!(
        ordered.descriptor().delivery_ordering,
        DeliveryOrdering::Canonical
    );
}

#[tokio::test]
async fn a_block_maps_and_reduces_through_a_real_store() {
    let processor = probe("echo");
    let directory = tempfile::tempdir().expect("directory");
    let store = open_store(&directory).await;
    let block = frame(7, BlockHash::ZERO, Vec::new());
    apply(&store, &processor, &block).await.expect("apply");
    let expected = json!({ "number": 7, "delta": [0, 0, 0, 0, 0, 0, 0, 7] });
    assert_eq!(
        changes(&store, &processor).await,
        vec![(7, "probe.block".to_owned(), expected.clone())]
    );
    assert_eq!(
        entity(&store, &processor, "blocks", &7_u64.to_be_bytes()).await,
        Some(expected)
    );
}

#[tokio::test]
async fn limits_and_guest_failures_fail_the_call_with_a_reason() {
    let block = frame(1, BlockHash::ZERO, Vec::new());
    let spin = probe_with(
        "spin",
        WasmLimits {
            fuel: 5_000_000,
            ..WasmLimits::default()
        },
    )
    .expect("probe");
    assert!(message(spin.map(&block).await).contains("exhausted its fuel limit of 5000000"));
    assert!(message(probe("panic").map(&block).await).contains("probe map panic at block 1"));
    assert!(message(probe("fail").map(&block).await).contains("probe refused the block"));
    assert!(
        message(probe_with("describe-panic", WasmLimits::default()))
            .contains("probe describe panic")
    );
    assert!(message(probe_with("bad-manifest", WasmLimits::default())).contains("manifest name"));

    assert!(
        apply_once(&probe("read-block-local"), &block)
            .await
            .contains("cannot read state")
    );
    assert!(
        apply_once(&probe("bad-entity"), &block)
            .await
            .contains("an entity is not JSON")
    );
    let no_changes = probe_with(
        "echo",
        WasmLimits {
            changes: 0,
            ..WasmLimits::default()
        },
    )
    .expect("probe");
    assert!(
        apply_once(&no_changes, &block)
            .await
            .contains("at most 0 changes")
    );
}

#[tokio::test]
async fn memory_past_its_limit_fails_the_call() {
    let block = frame(1, BlockHash::ZERO, Vec::new());
    // The probe allocates 128 MiB, past the default 64 MiB.
    assert!(message(probe("grow").map(&block).await).contains("wasm map"));
    let roomy = probe_with(
        "grow",
        WasmLimits {
            memory_bytes: 256 << 20,
            ..WasmLimits::default()
        },
    )
    .expect("probe");
    roomy.map(&block).await.expect("fits in 256 MiB");
}

#[tokio::test]
async fn map_host_calls_charge_fuel_by_the_byte_and_check_their_arguments() {
    let block = frame(1, BlockHash::ZERO, Vec::new());
    let defaults = WasmLimits::default();
    // Hashing 1 MiB costs a fuel unit per byte, not one per call.
    let hashing = host_processor(
        BLOCK_LOCAL,
        "(loop $again
           (call $keccak256 (i32.const 0) (i32.const 1048576) (i32.const 0))
           (local.set $i (i32.add (local.get $i) (i32.const 1)))
           (br_if $again (i32.lt_u (local.get $i) (i32.const 1000))))
         (call $output (i32.const 0) (i32.const 0)) (i32.const 0)",
        "(i32.const 0)",
        WasmLimits {
            fuel: 50_000_000,
            ..defaults
        },
    );
    assert!(message(hashing.map(&block).await).contains("exhausted its fuel limit of 50000000"));
    let large = host_processor(
        BLOCK_LOCAL,
        "(call $output (i32.const 0) (i32.const 4096)) (i32.const 0)",
        "(i32.const 0)",
        WasmLimits {
            value_bytes: 1024,
            ..defaults
        },
    );
    assert!(
        message(large.map(&block).await)
            .contains("the output is 4096 bytes, more than the 1024-byte limit")
    );
    let outside = host_processor(
        BLOCK_LOCAL,
        "(call $output (i32.const 2097000) (i32.const 1000)) (i32.const 0)",
        "(i32.const 0)",
        defaults,
    );
    assert!(
        message(outside.map(&block).await).contains("the output lies outside the module's memory")
    );
    let reading_in_map = host_processor(
        ORDERED,
        "(drop (call $read (i32.const 0) (i32.const 0) (i32.const 1) (i32.const 0) (i32.const 1)))
         (i32.const 0)",
        "(i32.const 0)",
        defaults,
    );
    assert!(
        message(reading_in_map.map(&block).await).contains("state is available only in reduce")
    );
}

#[tokio::test]
async fn reduce_host_calls_check_writes_scans_changes_and_fuel() {
    let block = frame(1, BlockHash::ZERO, Vec::new());
    let defaults = WasmLimits::default();
    let unknown_write = host_processor(
        BLOCK_LOCAL,
        OUTPUT_NOTHING,
        "(drop (call $write (i32.const 9) (i32.const 0) (i32.const 1) (i32.const 0) (i32.const 1)
           (i32.const 0) (i32.const 0)))
         (i32.const 0)",
        defaults,
    );
    assert!(
        apply_once(&unknown_write, &block)
            .await
            .contains("unknown write operation 9")
    );
    let wide_scan = host_processor(
        ORDERED,
        OUTPUT_NOTHING,
        "(drop (call $scan (i32.const 0) (i32.const 0) (i32.const 1) (i32.const 0) (i32.const 0)
           (i32.const 10001)))
         (i32.const 0)",
        defaults,
    );
    assert!(
        apply_once(&wide_scan, &block)
            .await
            .contains("a scan returns at most 10000 entries, not 10001")
    );
    // 64 KiB keys: the 1,025th change passes the bytes a call may hold.
    let many_changes = host_processor(
        BLOCK_LOCAL,
        OUTPUT_NOTHING,
        "(loop $again
           (call $emit (i32.const 0) (i32.const 1) (i32.const 65536) (i32.const 65536)
             (i32.const 0) (i32.const 131072) (i32.const 1))
           (local.set $i (i32.add (local.get $i) (i32.const 1)))
           (br_if $again (i32.lt_u (local.get $i) (i32.const 1100))))
         (i32.const 0)",
        defaults,
    );
    assert!(
        apply_once(&many_changes, &block)
            .await
            .contains("changes may hold at most 67108864 bytes")
    );
    let spinning_reduce = host_processor(
        BLOCK_LOCAL,
        OUTPUT_NOTHING,
        "(loop $forever (br $forever)) (i32.const 0)",
        WasmLimits {
            fuel: 1_000_000,
            ..defaults
        },
    );
    assert!(
        apply_once(&spinning_reduce, &block)
            .await
            .contains("exhausted its fuel limit of 1000000")
    );
}

#[test]
fn a_table_past_its_limit_fails_to_instantiate() {
    let limits = WasmLimits::default();
    let module = wat::parse_str(
        r#"(module (memory (export "memory") 1) (table 20000 funcref)
            (func (export "leani_alloc") (param i32) (result i32) (i32.const 0))
            (func (export "leani_describe") (param i32 i32) (result i32) (i32.const 0))
            (func (export "leani_map") (param i32 i32) (result i32) (i32.const 0))
            (func (export "leani_reduce") (param i32 i32) (result i32) (i32.const 0)))"#,
    )
    .expect("wat");
    let module = WasmModule::new(&module, &limits).expect("a valid module");
    assert!(
        message(WasmProcessor::new(module, &json!({}), contract(), limits))
            .contains("denied to allocate or grow the table")
    );
}

/// Apply `frame` in a fresh store, where the instance is new, and return the
/// failure.
async fn apply_once(processor: &WasmProcessor, frame: &BlockFrame) -> String {
    let directory = tempfile::tempdir().expect("directory");
    apply(&open_store(&directory).await, processor, frame)
        .await
        .expect_err("apply fails")
}

#[tokio::test]
async fn ordered_state_reads_its_writes_publishes_once_per_key_and_undoes() {
    let processor = probe("state");
    let directory = tempfile::tempdir().expect("directory");
    let store = open_store(&directory).await;
    let first = frame(1, BlockHash::ZERO, Vec::new());
    let second = frame(2, first.block.hash, Vec::new());
    apply(&store, &processor, &first).await.expect("first");
    apply(&store, &processor, &second).await.expect("second");
    let counts = changes(&store, &processor)
        .await
        .into_iter()
        .map(|(block, kind, json)| (block, kind, json["count"].clone()))
        .collect::<Vec<_>>();
    // Two emits per block, one published change per key: the last.
    assert_eq!(
        counts,
        vec![
            (1, "probe.total".to_owned(), json!(1)),
            (2, "probe.total".to_owned(), json!(2)),
        ]
    );
    store
        .undo(
            processor.descriptor(),
            second.chain_id,
            second.block.number,
            second.block.hash,
            &[],
        )
        .await
        .expect("undo");
    assert_eq!(
        entity(&store, &processor, "totals", b"total")
            .await
            .map(|total| total["count"].clone()),
        Some(json!(1))
    );
    let starved = probe_with(
        "state",
        WasmLimits {
            host_calls: 1,
            ..WasmLimits::default()
        },
    )
    .expect("probe");
    assert!(
        apply_once(&starved, &first)
            .await
            .contains("at most 1 state accesses")
    );
}

#[tokio::test]
async fn nan_results_are_canonical_and_maps_repeat_exactly() {
    let processor = probe("floats");
    let block = frame(3, BlockHash::ZERO, Vec::new());
    let first = processor.map(&block).await.expect("map");
    let second = processor.map(&block).await.expect("map");
    assert_eq!(first.checksum, second.checksum);
    // After postcard's length prefix: the canonical NaN, whatever the CPU.
    assert_eq!(first.payload[1..9], 0x7ff8_0000_0000_0000_u64.to_be_bytes());
}

#[tokio::test]
async fn every_source_shape_and_finality_maps_a_block_alike() {
    let processor = processor_with(
        TRANSFERS,
        &json!({ "tokens": [Address::new(TOKEN).to_string()] }),
        WasmLimits::default(),
    )
    .expect("processor");
    let matching = transfer(TOKEN, 10, 1);
    // A complete source delivers every log of the block.
    let complete = frame(
        5,
        BlockHash::ZERO,
        vec![
            transfer(OTHER, 99, 0),
            matching.clone(),
            transfer(OTHER, 7, 2),
        ],
    );
    // A filtered source delivers only the logs matching the requirement.
    let mut filtered = complete.clone();
    filtered.logs = Material::Filtered {
        value: vec![matching],
        scope: processor.descriptor().requirements[0].filter.clone(),
        completeness: Completeness::VerifiedPredicate,
    };
    let mut finalized = filtered.clone();
    finalized.finality = Finality::Finalized;
    // A source may deliver logs in another order.
    let mut reversed = complete.clone();
    if let Material::Complete(logs) = &mut reversed.logs {
        logs.push(transfer(TOKEN, 3, 4));
        logs.reverse();
    }
    let mut ordered = complete.clone();
    if let Material::Complete(logs) = &mut ordered.logs {
        logs.push(transfer(TOKEN, 3, 4));
    }
    let complete = processor.map(&complete).await.expect("complete");
    assert_eq!(
        processor.map(&filtered).await.expect("filtered").checksum,
        complete.checksum
    );
    assert_eq!(
        processor.map(&finalized).await.expect("finalized").checksum,
        complete.checksum
    );
    assert_eq!(
        processor.map(&reversed).await.expect("reversed").checksum,
        processor.map(&ordered).await.expect("ordered").checksum
    );
}

#[tokio::test]
async fn a_module_sees_the_same_logs_from_receipts_or_a_log_list() {
    let processor = probe("logs");
    let logs = vec![transfer(TOKEN, 1, 0), transfer(OTHER, 2, 1)];
    let listed = frame(4, BlockHash::ZERO, logs.clone());
    // P2P delivers the block's receipts, which carry its logs.
    let mut in_receipts = listed.clone();
    in_receipts.logs = Material::Missing(MissingReason::NotRequested);
    in_receipts.receipts = Material::Complete(vec![ReceiptEnvelope {
        transaction_hash: TransactionHash::new([9; 32]),
        transaction_type: 2,
        transaction_index: 0,
        encoded: None,
        success: Some(true),
        gas_used: Some(21_000),
        effective_gas_price: None,
        blob_gas_used: None,
        blob_gas_price: None,
        logs,
    }]);
    for frame in [listed, in_receipts] {
        let delta = processor.map(&frame).await.expect("map");
        let seen = postcard::from_bytes::<Vec<u8>>(&delta.payload).expect("delta");
        assert_eq!(
            serde_json::from_slice::<Value>(&seen).expect("JSON"),
            json!({ "logs": [0, 1], "transactions": false, "header": 21_000 })
        );
    }
}

#[tokio::test]
async fn ordered_reduce_reads_the_entities_earlier_blocks_wrote() {
    let processor = probe("replay");
    let directory = tempfile::tempdir().expect("directory");
    let store = open_store(&directory).await;
    let first = frame(1, BlockHash::ZERO, Vec::new());
    let second = frame(2, first.block.hash, Vec::new());
    apply(&store, &processor, &first).await.expect("first");
    apply(&store, &processor, &second).await.expect("second");
    assert_eq!(
        entity(&store, &processor, "totals", b"total").await,
        Some(json!({ "block": 2, "previous": { "block": 1, "previous": null } }))
    );
}

#[tokio::test]
async fn erc20_transfers_keep_matching_transfers_as_json() {
    let processor = processor_with(
        TRANSFERS,
        &json!({ "minValue": "10" }),
        WasmLimits::default(),
    )
    .expect("processor");
    let mut erc721 = transfer(TOKEN, 1, 3);
    erc721.topics.push(word(77));
    erc721.data.clear();
    let block = frame(
        9,
        BlockHash::ZERO,
        vec![transfer(TOKEN, 9, 0), transfer(TOKEN, 10, 1), erc721],
    );
    let directory = tempfile::tempdir().expect("directory");
    let store = open_store(&directory).await;
    apply(&store, &processor, &block).await.expect("apply");
    let applied = changes(&store, &processor).await;
    assert_eq!(applied.len(), 1, "{applied:?}");
    let (number, kind, transfer) = &applied[0];
    assert_eq!((*number, kind.as_str()), (9, "erc20.transfer"));
    assert_eq!(transfer["token"], Address::new(TOKEN).to_string());
    assert_eq!(transfer["value"], "10");
    assert_eq!(transfer["logIndex"], 1);
    assert_eq!(
        transfer["transactionHash"],
        TransactionHash::new([9; 32]).to_string()
    );
}

#[tokio::test]
async fn token_volume_accumulates_in_chain_order_and_undoes() {
    let processor = processor_with(VOLUME, &json!({}), WasmLimits::default()).expect("processor");
    let directory = tempfile::tempdir().expect("directory");
    let store = open_store(&directory).await;
    let first = frame(
        1,
        BlockHash::ZERO,
        vec![transfer(TOKEN, 5, 0), transfer(TOKEN, 6, 1)],
    );
    let second = frame(
        2,
        first.block.hash,
        vec![transfer(TOKEN, 100, 0), transfer(OTHER, 1, 1)],
    );
    apply(&store, &processor, &first).await.expect("first");
    apply(&store, &processor, &second).await.expect("second");
    assert_eq!(
        entity(&store, &processor, "volumes", &TOKEN).await,
        Some(json!({
            "token": Address::new(TOKEN).to_string(),
            "transfers": 3,
            "volume": "111",
            "lastBlock": 2,
        }))
    );
    store
        .undo(
            processor.descriptor(),
            second.chain_id,
            second.block.number,
            second.block.hash,
            &[],
        )
        .await
        .expect("undo");
    let restored = entity(&store, &processor, "volumes", &TOKEN)
        .await
        .expect("volume");
    assert_eq!(
        (restored["transfers"].clone(), restored["volume"].clone()),
        (json!(2), json!("11"))
    );
    assert_eq!(entity(&store, &processor, "volumes", &OTHER).await, None);
    let deleted = DomainChange {
        kind: "erc20.volume".to_owned(),
        key: OTHER.to_vec(),
        operation: ChangeOperation::Delete,
        payload: Vec::new(),
    };
    assert_eq!(
        processor.change_json(&deleted).expect("render"),
        Some(Value::Null)
    );
}

/// Native `evm-events` and the Wasm `erc20-transfers` doing the same work:
/// decode ERC-20 transfers, write one entity and one change per transfer.
/// Run in release:
/// `cargo test -p leani-processor-wasm --release -- --ignored --nocapture wasm_versus_native`.
#[tokio::test]
#[ignore = "measurement, not a check"]
#[allow(clippy::too_many_lines)]
async fn wasm_versus_native() {
    use leani_processor_evm_events::{
        EventDefinition, EventOutput, EvmEventsConfig, EvmEventsProcessor,
    };
    use std::time::{Duration, Instant};

    let native = EvmEventsProcessor::new(EvmEventsConfig {
        start_block: BlockNumber(0),
        addresses: Vec::new(),
        events: vec![EventDefinition {
            abi: "event Transfer(address indexed from, address indexed to, uint256 value)"
                .to_owned(),
            output: EventOutput {
                collection: "transfers".to_owned(),
                kind: "erc20.transfer".to_owned(),
                key_fields: Vec::new(),
                bucket_seconds: None,
            },
        }],
    })
    .expect("native processor");
    let wasm = processor_with(TRANSFERS, &json!({}), WasmLimits::default()).expect("wasm");
    let per_block = |total: Duration, blocks: u32| total / blocks;
    println!(
        "| logs/block | native map | wasm map | native reduce | wasm reduce | native map+apply | wasm map+apply |"
    );
    println!("|---:|---:|---:|---:|---:|---:|---:|");
    for logs in [0_u32, 10, 100, 1_000] {
        let blocks = if logs >= 1_000 { 20 } else { 200 };
        let frames = (1..=u64::from(blocks))
            .map(|number| {
                frame(
                    number,
                    BlockHash::ZERO,
                    (0..logs)
                        .map(|index| transfer(TOKEN, u64::from(index) + 1, index))
                        .collect(),
                )
            })
            .collect::<Vec<_>>();
        let mut timings = Vec::new();
        for processor in [&native as &dyn Processor, &wasm as &dyn Processor] {
            let started = Instant::now();
            let mut deltas = Vec::with_capacity(frames.len());
            for frame in &frames {
                deltas.push(processor.map(frame).await.expect("map"));
            }
            let map = started.elapsed();
            let started = Instant::now();
            for (frame, delta) in frames.iter().zip(&deltas) {
                let mut reducer = leani_testkit::MemoryReducer::default();
                let cursor = ProcessorCursor {
                    processor_id: processor.descriptor().id.to_string(),
                    processor_version: processor.descriptor().version.to_string(),
                    chain_id: frame.chain_id,
                    block_number: frame.block.number,
                    block_hash: frame.block.hash,
                    finality: frame.finality,
                    sequence: frame.block.number.0,
                };
                processor
                    .reduce(&mut reducer, &cursor, delta)
                    .await
                    .expect("reduce");
                assert_eq!(reducer.emitted().len(), logs as usize);
            }
            let reduce = started.elapsed();
            let directory = tempfile::tempdir().expect("directory");
            let store = open_store(&directory).await;
            let started = Instant::now();
            for frame in &frames {
                let delta = processor.map(frame).await.expect("map");
                let cursor = ProcessorCursor {
                    processor_id: processor.descriptor().id.to_string(),
                    processor_version: processor.descriptor().version.to_string(),
                    chain_id: frame.chain_id,
                    block_number: frame.block.number,
                    block_hash: frame.block.hash,
                    finality: frame.finality,
                    sequence: frame.block.number.0,
                };
                store
                    .apply(processor, cursor, &delta, &[])
                    .await
                    .expect("apply");
            }
            timings.push((map, reduce, started.elapsed()));
        }
        let [
            (native_map, native_reduce, native_apply),
            (wasm_map, wasm_reduce, wasm_apply),
        ] = timings[..]
        else {
            unreachable!("two processors");
        };
        println!(
            "| {logs} | {:?} | {:?} ({:.1}x) | {:?} | {:?} ({:.1}x) | {:?} | {:?} ({:.1}x) |",
            per_block(native_map, blocks),
            per_block(wasm_map, blocks),
            wasm_map.as_secs_f64() / native_map.as_secs_f64(),
            per_block(native_reduce, blocks),
            per_block(wasm_reduce, blocks),
            wasm_reduce.as_secs_f64() / native_reduce.as_secs_f64(),
            per_block(native_apply, blocks),
            per_block(wasm_apply, blocks),
            wasm_apply.as_secs_f64() / native_apply.as_secs_f64(),
        );
    }
}

#[tokio::test]
async fn block_gas_carries_a_256_bit_fee_through_its_delta() {
    let processor =
        processor_with(BLOCK_GAS, &json!({}), WasmLimits::default()).expect("processor");
    let mut block = frame(11, BlockHash::ZERO, Vec::new());
    if let Material::Complete(header) = &mut block.header {
        let mut fee = [0; 32];
        fee[0] = 1;
        header.base_fee_per_gas = Some(leani_primitives::Quantity::new(fee));
    }
    let directory = tempfile::tempdir().expect("directory");
    let store = open_store(&directory).await;
    apply(&store, &processor, &block).await.expect("apply");
    assert_eq!(
        entity(&store, &processor, "blocks", &11_u64.to_be_bytes()).await,
        Some(json!({
            "number": 11,
            "timestamp": 1_700_000_011,
            "gasUsed": 21_000,
            "gasLimit": 30_000_000,
            "utilizationBasisPoints": 7,
            "baseFeePerGas": "452312848583266388373324160190187140051835877600158453279131187530910662656",
        }))
    );
}

#[tokio::test]
async fn the_event_decoder_decodes_signed_static_and_dynamic_arguments() {
    let swap = "Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)";
    let named = "event Named(string label, bytes data, bool flag);";
    let processor = processor_with(
        EVENT_DECODER,
        &json!({ "events": [swap, named] }),
        WasmLimits::default(),
    )
    .expect("processor");
    let signed = |value: i64| {
        let mut word = [if value < 0 { 0xff } else { 0 }; 32];
        word[24..].copy_from_slice(&value.to_be_bytes());
        word
    };
    let swap_log = Log {
        address: Address::new(TOKEN),
        topics: vec![
            *alloy_primitives::keccak256(
                b"Swap(address,address,int256,int256,uint160,uint128,int24)",
            ),
            topic([1; 20]),
            topic([2; 20]),
        ],
        data: [signed(-5), signed(7), word(1), word(2), signed(-3)].concat(),
        transaction_hash: Some(TransactionHash::new([9; 32])),
        transaction_index: 0,
        log_index: 0,
    };
    let padded = |bytes: &[u8]| {
        let mut chunk = word(u64::try_from(bytes.len()).expect("len")).to_vec();
        let mut value = bytes.to_vec();
        value.resize(32, 0);
        chunk.extend(value);
        chunk
    };
    let named_log = Log {
        address: Address::new(OTHER),
        topics: vec![*alloy_primitives::keccak256(b"Named(string,bytes,bool)")],
        data: [
            word(0x60).to_vec(),
            word(0xa0).to_vec(),
            word(1).to_vec(),
            padded(b"hello"),
            padded(&[0xbe, 0xef]),
        ]
        .concat(),
        transaction_hash: Some(TransactionHash::new([9; 32])),
        transaction_index: 0,
        log_index: 1,
    };
    // A log with the Swap topic but a truncated body is another layout.
    let mut truncated = swap_log.clone();
    truncated.data.truncate(64);
    truncated.log_index = 2;
    let block = frame(12, BlockHash::ZERO, vec![swap_log, named_log, truncated]);
    let directory = tempfile::tempdir().expect("directory");
    let store = open_store(&directory).await;
    apply(&store, &processor, &block).await.expect("apply");
    let applied = changes(&store, &processor).await;
    let args = applied
        .iter()
        .map(|(_, kind, entity)| (kind.as_str(), entity["args"].clone()))
        .collect::<Vec<_>>();
    assert_eq!(
        args,
        [
            (
                "event.Swap",
                json!({
                    "sender": Address::new([1; 20]).to_string(),
                    "recipient": Address::new([2; 20]).to_string(),
                    "amount0": "-5",
                    "amount1": "7",
                    "sqrtPriceX96": "1",
                    "liquidity": "2",
                    "tick": "-3",
                })
            ),
            (
                "event.Named",
                json!({ "label": "hello", "data": "0xbeef", "flag": true })
            ),
        ]
    );
    assert_eq!(
        applied[0].2["signature"],
        "Swap(address,address,int256,int256,uint160,uint128,int24)"
    );
    assert!(
        message(processor_with(
            EVENT_DECODER,
            &json!({ "events": ["Bad(uint7 x)"] }),
            WasmLimits::default(),
        ))
        .contains("invalid width")
    );
}
