use alloy_primitives::{U256, address};
use leani_primitives::{
    BlockRef, ChainId, Finality, Log, MissingReason, TransactionEnvelope, TransactionHash,
};

use super::*;

const BASE: L1Anchor = L1Anchor::DisputeGames {
    factory: address!("0x43edB88C4B80fDD2AdFF2412A7BebF9dF42cB40e"),
    registry: address!("0x909f6cf47ed12f010a796527f562bfc26c7f4e72"),
    game_type: 621,
};
const ROBINHOOD: L1Anchor = L1Anchor::NitroRollup {
    rollup: address!("0x23A19d23e89166adedbDcB432518AB01e4272D94"),
};
/// A Base game created in L1 tx 0x1fb4c09b…, claiming L2 block 53,042,760.
const GAME: Address = address!("0xd286d94c1950ab8fe746b21d0c8e4b79e08461b4");
const ROOT: B256 = b256!("0xcdefaf4edf04754c074d0bf29dc2c69c54003f4fb2c533ea33582de8d050911d");

fn byte(number: u64) -> u8 {
    u8::try_from(number % 256).expect("one byte")
}

fn word(value: U256) -> [u8; 32] {
    value.to_be_bytes()
}

/// `createWithInitData(621, ROOT, extra, init)` with the extra data's
/// first word the claimed block, as Base's proposer calls it.
fn creation_call(l2_block: u64) -> Vec<u8> {
    let mut input = vec![0x10, 0x11, 0xf3, 0x77];
    input.extend(word(U256::from(621)));
    input.extend(ROOT.0);
    input.extend(word(U256::from(0x80)));
    input.extend(word(U256::from(0xe0)));
    input.extend(word(U256::from(64)));
    input.extend(word(U256::from(l2_block)));
    input.extend([0x90; 32]);
    input.extend(word(U256::ZERO));
    input
}

fn log(address: Address, topics: Vec<B256>, data: Vec<u8>, transaction_index: u32) -> Log {
    Log {
        address: address.into(),
        topics: topics.into_iter().map(|topic| topic.0).collect(),
        data,
        transaction_hash: None,
        transaction_index,
        log_index: 0,
    }
}

fn frame(number: u64, logs: Vec<Log>, inputs: Vec<(u32, Vec<u8>)>) -> BlockFrame {
    BlockFrame {
        chain_id: ChainId(1),
        block: BlockRef {
            number: BlockNumber(number),
            hash: BlockHash::new([byte(number); 32]),
            parent_hash: BlockHash::new([byte(number.wrapping_sub(1)); 32]),
            timestamp: 0,
        },
        finality: Finality::Finalized,
        header: Material::Missing(MissingReason::NotRequested),
        receipts: Material::Missing(MissingReason::NotRequested),
        logs: Material::Complete(logs),
        withdrawals: Material::Missing(MissingReason::NotRequested),
        blob_sidecars: Material::Missing(MissingReason::NotRequested),
        traces: Material::Missing(MissingReason::NotRequested),
        state_diffs: Material::Missing(MissingReason::NotRequested),
        provenance: Vec::new(),
        verification: leani_primitives::VerificationReport::default(),
        transactions: Material::Complete(
            inputs
                .into_iter()
                .map(|(index, input)| TransactionEnvelope {
                    hash: TransactionHash::ZERO,
                    transaction_type: 2,
                    index,
                    encoded: None,
                    from: None,
                    to: None,
                    nonce: None,
                    gas_limit: None,
                    value: None,
                    input: Some(input),
                    max_fee_per_gas: None,
                    max_priority_fee_per_gas: None,
                    max_fee_per_blob_gas: None,
                    blob_versioned_hashes: Vec::new(),
                    size_bytes: None,
                })
                .collect(),
        ),
    }
}

#[test]
fn event_ids_are_the_signatures_hashes() {
    for (id, signature) in [
        (
            DISPUTE_GAME_CREATED,
            "DisputeGameCreated(address,uint32,bytes32)",
        ),
        (ANCHOR_UPDATED, "AnchorUpdated(address)"),
        (
            ASSERTION_CONFIRMED,
            "AssertionConfirmed(bytes32,bytes32,bytes32)",
        ),
    ] {
        assert_eq!(id, keccak256(signature), "{signature}");
    }
    for (selector, signature) in CREATE_SELECTORS.iter().zip([
        "create(uint32,bytes32,bytes)",
        "createWithInitData(uint32,bytes32,bytes,bytes)",
    ]) {
        assert_eq!(selector, &keccak256(signature)[..4], "{signature}");
    }
}

#[test]
fn games_of_the_respected_type_name_their_block_and_claim() {
    let created = |game_type: u64| {
        log(
            address!("0x43edB88C4B80fDD2AdFF2412A7BebF9dF42cB40e"),
            vec![
                DISPUTE_GAME_CREATED,
                GAME.into_word(),
                B256::from(U256::from(game_type)),
                ROOT,
            ],
            Vec::new(),
            7,
        )
    };
    let updated = log(
        address!("0x909f6cf47ed12f010a796527f562bfc26c7f4e72"),
        vec![ANCHOR_UPDATED, GAME.into_word()],
        Vec::new(),
        9,
    );
    let block = frame(
        1,
        vec![created(621), created(1), updated],
        vec![(7, creation_call(53_042_760))],
    );
    assert_eq!(
        observe(&BASE, &block),
        vec![
            Observation::GameCreated {
                game: GAME,
                l2_block: 53_042_760,
                root: ROOT,
            },
            Observation::AnchorUpdated { game: GAME },
        ]
    );
    // Without its creation call the game cannot be placed.
    let unreadable = frame(1, vec![created(621)], Vec::new());
    assert!(observe(&BASE, &unreadable).is_empty());
}

#[test]
fn confirmed_assertions_name_their_block_hash() {
    // Robinhood's rollup in L1 block 26,265,153: block hash, then send root.
    let data = [
        b256!("0xbc3fe60eac62be6babeebe3978e8732713fc9a815dcb4cadebdbad0700de7a85").0,
        b256!("0x24b7884da1687f1d33e10cbf68b8391de13b9e4518a7d2b1236b134488f6a75c").0,
    ]
    .concat();
    let confirmed = log(
        address!("0x23A19d23e89166adedbDcB432518AB01e4272D94"),
        vec![ASSERTION_CONFIRMED, B256::repeat_byte(1)],
        data.clone(),
        0,
    );
    let elsewhere = log(Address::repeat_byte(2), vec![ASSERTION_CONFIRMED], data, 1);
    assert_eq!(
        observe(
            &ROBINHOOD,
            &frame(1, vec![confirmed, elsewhere], Vec::new())
        ),
        vec![Observation::AssertionConfirmed {
            block_hash: b256!("0xbc3fe60eac62be6babeebe3978e8732713fc9a815dcb4cadebdbad0700de7a85"),
        }]
    );
}

#[test]
fn output_roots_match_a_resolved_base_game() {
    // Base block 52,826,760 and the root claim of a game the registry
    // anchored.
    assert_eq!(
        output_root(
            b256!("0x86ef2970fa49acc06920541e477b9f5473e90f5c5c3463c56985165d22e5443f"),
            b256!("0xd81c90e19d27336378a302428bcf24a20cf9f55058121350600a4bf3c23e3087"),
            b256!("0x5be84d38fe30eae41522d45813de4e27a9f9c4d16a8f668d28a5255e9dc733fe"),
        ),
        b256!("0x00ec5fba12b44ec944a362ce7df4519dd986829173ce13e5a4dc0b7bf3ca31dd")
    );
}

#[test]
fn a_game_finalizes_its_block_only_when_its_claim_matched_a_signed_block() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("games.json");
    let hash = B256::repeat_byte(5);
    let mut ledger = GameLedger::load(Some(path.clone()), 4).expect("ledger");
    ledger.record_block(100, hash, ROOT);
    let other = Address::repeat_byte(3);
    for observation in [
        Observation::GameCreated {
            game: GAME,
            l2_block: 100,
            root: ROOT,
        },
        Observation::GameCreated {
            game: other,
            l2_block: 100,
            root: B256::repeat_byte(9),
        },
    ] {
        assert_eq!(ledger.observe(observation).expect("observe"), None);
    }
    // The ledger outlives a restart; signed blocks do not need to.
    let mut ledger = GameLedger::load(Some(path), 4).expect("reload");
    assert_eq!(
        ledger
            .observe(Observation::AnchorUpdated { game: other })
            .expect("observe"),
        None
    );
    assert_eq!(
        ledger
            .observe(Observation::AnchorUpdated { game: GAME })
            .expect("observe"),
        Some((100, hash))
    );
    // Signed blocks beyond the capacity are forgotten, oldest first.
    for number in 0..6 {
        ledger.record_block(number, hash, ROOT);
    }
    assert_eq!(
        ledger.signed.keys().copied().collect::<Vec<_>>(),
        [2, 3, 4, 5]
    );
}

struct Scripted {
    events: Vec<FinalityEvent>,
}

#[async_trait::async_trait]
impl FinalitySource for Scripted {
    fn descriptor(&self) -> &leani_source_api::SourceDescriptor {
        unimplemented!("unused")
    }

    async fn subscribe(
        &self,
        _checkpoint: ConsensusCheckpoint,
        _cancellation: CancellationToken,
    ) -> Result<leani_source_api::FinalityEventStream, SourceError> {
        Ok(futures::stream::iter(self.events.clone().into_iter().map(Ok)).boxed())
    }
}

struct Chain {
    requested: std::sync::Mutex<Vec<(u64, u64)>>,
}

#[async_trait::async_trait]
impl FinalizedBlocks for Chain {
    async fn frames(
        &self,
        range: BlockRange,
        _tip: FinalizedTip,
        _cancellation: &CancellationToken,
    ) -> Result<Vec<BlockFrame>, SourceError> {
        self.requested
            .lock()
            .unwrap()
            .push((range.start().0, range.end().0));
        let confirmed = |number: u64| {
            log(
                address!("0x23A19d23e89166adedbDcB432518AB01e4272D94"),
                vec![ASSERTION_CONFIRMED, B256::ZERO],
                [B256::repeat_byte(byte(number)).0, [0; 32]].concat(),
                0,
            )
        };
        Ok((range.start().0..=range.end().0)
            .map(|number| frame(number, vec![confirmed(number)], Vec::new()))
            .collect())
    }
}

fn finalized(number: u64) -> FinalityEvent {
    FinalityEvent::Finalized {
        block_number: BlockNumber(number),
        block_hash: BlockHash::new([byte(number); 32]),
        beacon_slot: number,
        beacon_block_root: [0; 32],
    }
}

#[tokio::test]
async fn the_lane_reads_each_finalized_range_once_and_resumes_after_it() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let cursor_path = directory.path().join("cursor.json");
    let checkpoint = ConsensusCheckpoint {
        beacon_slot: 0,
        beacon_block_root: [0; 32],
        execution_block_hash: BlockHash::ZERO,
        obtained_at_unix_seconds: 0,
        source: "fixture".to_owned(),
    };
    let run = |events: Vec<FinalityEvent>| {
        let chain = Arc::new(Chain {
            requested: std::sync::Mutex::new(Vec::new()),
        });
        let lane = AnchorLane {
            anchor: ROBINHOOD,
            finality: Arc::new(Scripted { events }),
            checkpoint: checkpoint.clone(),
            blocks: ChainRef(chain.clone()),
            cursor_path: cursor_path.clone(),
            lookback: 2,
        };
        async move {
            let (sender, mut received) = mpsc::channel(64);
            let outcome = lane.run(sender, CancellationToken::new()).await;
            let mut hashes = Vec::new();
            while let Ok(Observation::AssertionConfirmed { block_hash }) = received.try_recv() {
                hashes.push(block_hash.0[0]);
            }
            (outcome, chain.requested.lock().unwrap().clone(), hashes)
        }
    };
    let (outcome, requested, hashes) = run(vec![finalized(10), finalized(10), finalized(13)]).await;
    assert!(matches!(outcome, Err(SourceError::Disconnected(_))));
    assert_eq!(requested, [(8, 10), (11, 13)]);
    assert_eq!(hashes, [8, 9, 10, 11, 12, 13]);
    let (_, requested, _) = run(vec![finalized(15)]).await;
    assert_eq!(requested, [(14, 15)]);
}

struct ChainRef(Arc<Chain>);

#[async_trait::async_trait]
impl FinalizedBlocks for ChainRef {
    async fn frames(
        &self,
        range: BlockRange,
        tip: FinalizedTip,
        cancellation: &CancellationToken,
    ) -> Result<Vec<BlockFrame>, SourceError> {
        self.0.frames(range, tip, cancellation).await
    }
}
