use alloy_primitives::{U256, address};
use leani_primitives::{
    BlockRef, ChainId, Finality, Log, MissingReason, TransactionEnvelope, TransactionHash,
};

use super::*;

const BASE: L1Anchor = L1Anchor::DisputeGames {
    factory: address!("0x43edB88C4B80fDD2AdFF2412A7BebF9dF42cB40e"),
    registry: address!("0x909f6cf47ed12f010a796527f562bfc26c7f4e72"),
    game_type: 621,
    claims: GameClaims::OutputRoots,
};
const OP_MAINNET: L1Anchor = L1Anchor::DisputeGames {
    factory: address!("0xe5965Ab5962eDc7477C8520243A95517CD252fA9"),
    registry: address!("0x23b2c62946350f4246f9f9d027e071f0264fd113"),
    game_type: 9,
    claims: GameClaims::SuperRoots,
};
const ROBINHOOD: L1Anchor = L1Anchor::NitroRollup {
    rollup: address!("0x23A19d23e89166adedbDcB432518AB01e4272D94"),
    sequencer_inbox: None,
};
/// A Base game created in L1 tx 0x1fb4c09b…, claiming L2 block 53,042,760.
const GAME: Address = address!("0xd286d94c1950ab8fe746b21d0c8e4b79e08461b4");
const ROOT: B256 = b256!("0xcdefaf4edf04754c074d0bf29dc2c69c54003f4fb2c533ea33582de8d050911d");
/// OP Mainnet's game created in L1 tx 0xec0aaf58…: a super root holding
/// OP Mainnet's output root at block 159,073,087, made at 1,793,744,951.
const SUPER_GAME: Address = address!("0xa8d35d50443471783eedeeeeefb1d751d0be76a0");
const SUPER_ROOT: B256 =
    b256!("0xb31a67f36e5f000742b986f81a174aae2c5cc99720cdb5dd0dc9548f16c15dc3");
const OP_OUTPUT_ROOT: B256 =
    b256!("0x5940406c69fa8dc5c2609b6d45bcdd424db059bceb732993db4146400b5e51b0");
/// OP Mainnet's `SystemConfig`, as its registry config names it.
const SYSTEM_CONFIG: Address = address!("0x229047fed2591dbec1eF1118d64F7aF3dB9EB290");
const OP_REGISTRY: Address = address!("0x23b2c62946350f4246f9f9d027e071f0264fd113");

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

/// The preimage of `SUPER_ROOT`: version 1, its time, then OP Mainnet's
/// chain id and output root.
fn super_root_preimage() -> Vec<u8> {
    let mut preimage = vec![1];
    preimage.extend(1_793_744_951_u64.to_be_bytes());
    preimage.extend(word(U256::from(10)));
    preimage.extend(OP_OUTPUT_ROOT.0);
    preimage
}

/// `create(9, SUPER_ROOT, preimage)`, as OP Mainnet's proposer calls it.
fn super_root_call(preimage: &[u8]) -> Vec<u8> {
    let mut input = vec![0x82, 0xec, 0xf2, 0xf6];
    input.extend(word(U256::from(9)));
    input.extend(SUPER_ROOT.0);
    input.extend(word(U256::from(0x60)));
    input.extend(word(U256::from(preimage.len())));
    input.extend(preimage);
    input.resize(4 + 128 + preimage.len().next_multiple_of(32), 0);
    input
}

/// `ConfigUpdate(version, update_type, data)`, as a `SystemConfig` emits it.
fn config_update(address: Address, update_type: u64, data: Vec<u8>) -> Log {
    log(
        address,
        vec![
            CONFIG_UPDATE,
            B256::ZERO,
            B256::from(U256::from(update_type)),
        ],
        data,
        0,
    )
}

/// The data of a signer update: `abi.encode(bytes(abi.encode(signer)))`.
fn signer_update(signer: Address) -> Vec<u8> {
    [
        word(U256::from(32)),
        word(U256::from(32)),
        signer.into_word().0,
    ]
    .concat()
}

fn respected_game_type(game_type: u64) -> Log {
    log(
        OP_REGISTRY,
        vec![RESPECTED_GAME_TYPE_SET],
        word(U256::from(game_type)).to_vec(),
        0,
    )
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
        (CONFIG_UPDATE, "ConfigUpdate(uint256,uint8,bytes)"),
        (RESPECTED_GAME_TYPE_SET, "RespectedGameTypeSet(uint32)"),
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
        observe(&BASE, 8453, None, &block),
        vec![
            Observation::GameCreated {
                game: GAME,
                l2_block: ClaimedBlock::Number(53_042_760),
                root: ROOT,
            },
            Observation::AnchorUpdated { game: GAME },
        ]
    );
    // Without its creation call the game cannot be placed.
    let unreadable = frame(1, vec![created(621)], Vec::new());
    assert!(observe(&BASE, 8453, None, &unreadable).is_empty());
}

#[test]
fn super_root_games_name_their_block_by_time_and_hold_the_chains_output_root() {
    let created = log(
        address!("0xe5965Ab5962eDc7477C8520243A95517CD252fA9"),
        vec![
            DISPUTE_GAME_CREATED,
            SUPER_GAME.into_word(),
            B256::from(U256::from(9)),
            SUPER_ROOT,
        ],
        Vec::new(),
        6,
    );
    let block = |preimage: &[u8]| {
        frame(
            26_337_378,
            vec![created.clone()],
            vec![(6, super_root_call(preimage))],
        )
    };
    assert_eq!(
        observe(&OP_MAINNET, 10, None, &block(&super_root_preimage())),
        vec![Observation::GameCreated {
            game: SUPER_GAME,
            l2_block: ClaimedBlock::Time(1_793_744_951),
            root: OP_OUTPUT_ROOT,
        }]
    );
    // A preimage the claim does not hash to says nothing, and neither does
    // one without the chain.
    let mut forged = super_root_preimage();
    forged[72] ^= 1;
    assert!(observe(&OP_MAINNET, 10, None, &block(&forged)).is_empty());
    assert!(observe(&OP_MAINNET, 8453, None, &block(&super_root_preimage())).is_empty());
}

#[test]
fn signer_rotations_and_respected_game_types_come_from_their_contracts() {
    let signer = address!("0xfaaca0a10e4a23c5e01485de07767547d4d8056d");
    let block = frame(
        26_340_000,
        vec![
            config_update(SYSTEM_CONFIG, 3, signer_update(signer)),
            // A gas limit update, and a signer update another contract emits.
            config_update(SYSTEM_CONFIG, 2, word(U256::from(60_000_000)).to_vec()),
            config_update(Address::repeat_byte(7), 3, signer_update(signer)),
            respected_game_type(1),
        ],
        Vec::new(),
    );
    assert_eq!(
        observe(&OP_MAINNET, 10, Some(SYSTEM_CONFIG), &block),
        vec![
            Observation::SignerChanged { signer },
            Observation::GameTypeChanged { game_type: 1 },
        ]
    );
    // Without its SystemConfig, the anchor reads no signer.
    assert_eq!(
        observe(&OP_MAINNET, 10, None, &block),
        vec![Observation::GameTypeChanged { game_type: 1 }]
    );
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
            4663,
            None,
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
    ledger.record_block(100, hash, ROOT, 1_000);
    let other = Address::repeat_byte(3);
    for observation in [
        Observation::GameCreated {
            game: GAME,
            l2_block: ClaimedBlock::Number(100),
            root: ROOT,
        },
        Observation::GameCreated {
            game: other,
            l2_block: ClaimedBlock::Number(100),
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
        ledger.record_block(number, hash, ROOT, number);
    }
    assert_eq!(
        ledger.signed.keys().copied().collect::<Vec<_>>(),
        [2, 3, 4, 5]
    );
}

#[test]
fn a_super_root_game_finalizes_the_signed_block_made_at_its_time() {
    // OP Mainnet block 159,073,087, whose header gives the output root the
    // super root holds.
    let output = output_root(
        b256!("0xac77b135f179847082b8f199623c5c28bcf51e674f6fda4a5f3a76ff951e1b83"),
        b256!("0xdb83ef9f8451e27b1a4bb15ca3140d7414f816b4f489ab8bb4ed5244ff3c5465"),
        b256!("0x79e40e07815b96e2b5f0862030a135a94b4f3a91fe67d1105e9440b97824ce93"),
    );
    assert_eq!(output, OP_OUTPUT_ROOT);
    assert_eq!(keccak256(super_root_preimage()), SUPER_ROOT);
    let hash = b256!("0x79e40e07815b96e2b5f0862030a135a94b4f3a91fe67d1105e9440b97824ce93");
    let mut ledger = GameLedger::load(None, 4).expect("ledger");
    ledger.record_block(
        159_073_086,
        B256::repeat_byte(4),
        B256::repeat_byte(8),
        1_793_744_949,
    );
    ledger.record_block(159_073_087, hash, output, 1_793_744_951);
    let created = Observation::GameCreated {
        game: SUPER_GAME,
        l2_block: ClaimedBlock::Time(1_793_744_951),
        root: OP_OUTPUT_ROOT,
    };
    assert_eq!(ledger.observe(created).expect("observe"), None);
    assert_eq!(
        ledger
            .observe(Observation::AnchorUpdated { game: SUPER_GAME })
            .expect("observe"),
        Some((159_073_087, hash))
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
    ) -> Result<FrameChunks, SourceError> {
        self.requested
            .lock()
            .unwrap()
            .push((range.start().0, range.end().0));
        Ok(one_chunk(
            (range.start().0..=range.end().0)
                .map(|number| frame(number, vec![confirmed(number)], Vec::new()))
                .collect(),
        ))
    }
}

/// Robinhood Chain's rollup confirming an assertion whose hash names the
/// block it was read in.
fn confirmed(number: u64) -> Log {
    log(
        address!("0x23A19d23e89166adedbDcB432518AB01e4272D94"),
        vec![ASSERTION_CONFIRMED, B256::ZERO],
        [B256::repeat_byte(byte(number)).0, [0; 32]].concat(),
        0,
    )
}

fn one_chunk(frames: Vec<BlockFrame>) -> FrameChunks {
    futures::stream::iter([Ok(frames)]).boxed()
}

/// Blocks in chunks of two, each holding a confirmation; the chunk that
/// starts at `failing` cannot be read.
struct Chunked {
    failing: u64,
}

#[async_trait::async_trait]
impl FinalizedBlocks for Chunked {
    async fn frames(
        &self,
        range: BlockRange,
        _tip: FinalizedTip,
        _cancellation: &CancellationToken,
    ) -> Result<FrameChunks, SourceError> {
        let end = range.end().0;
        let chunks = (range.start().0..=end)
            .step_by(2)
            .map(|start| {
                if start == self.failing {
                    return Err(SourceError::Unavailable("no peer serves it".to_owned()));
                }
                Ok((start..=(start + 1).min(end))
                    .map(|number| frame(number, vec![confirmed(number)], Vec::new()))
                    .collect())
            })
            .collect::<Vec<_>>();
        Ok(futures::stream::iter(chunks).boxed())
    }
}

#[tokio::test]
async fn a_range_is_handed_on_chunk_by_chunk_and_its_last_chunk_again_after_a_restart() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let cursor_path = directory.path().join("cursor.json");
    let run = |failing: u64| {
        let lane = AnchorLane {
            anchor: ROBINHOOD,
            finality: Arc::new(Scripted {
                events: vec![finalized(13)],
            }),
            checkpoint: ConsensusCheckpoint {
                beacon_slot: 0,
                beacon_block_root: [0; 32],
                execution_block_hash: BlockHash::ZERO,
                obtained_at_unix_seconds: 0,
                source: "fixture".to_owned(),
            },
            blocks: Chunked { failing },
            cursor_path: cursor_path.clone(),
            lookback: 4,
            chain_id: 4663,
            system_config: None,
            follow_game_type: true,
            batches: None,
        };
        async move {
            let (sender, mut received) = mpsc::channel(64);
            let outcome = lane.run(sender, CancellationToken::new()).await;
            let mut hashes = Vec::new();
            while let Ok(Observation::AssertionConfirmed { block_hash }) = received.try_recv() {
                hashes.push(block_hash.0[0]);
            }
            (outcome, hashes)
        }
    };
    // Blocks 9 to 13 in chunks starting at 9, 11 and 13; the second fails.
    let (outcome, hashes) = run(11).await;
    assert!(matches!(outcome, Err(SourceError::Unavailable(_))));
    assert_eq!(hashes, [9, 10]);
    assert_eq!(
        read_cursor(&cursor_path)
            .expect("cursor")
            .map(|cursor| cursor.number),
        Some(10)
    );
    // The restart hands on the chunk read last again: a crash may have lost
    // what its observers had not kept yet.
    let (_, hashes) = run(u64::MAX).await;
    assert_eq!(hashes, [9, 10, 11, 12, 13]);
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
async fn the_lane_reads_each_finalized_range_and_resumes_at_the_last_one() {
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
            chain_id: 4663,
            system_config: None,
            follow_game_type: true,
            batches: None,
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
    let (_, requested, hashes) = run(vec![finalized(15)]).await;
    assert_eq!(requested, [(11, 15)]);
    assert_eq!(hashes, [11, 12, 13, 14, 15]);
}

struct ChainRef(Arc<Chain>);

#[async_trait::async_trait]
impl FinalizedBlocks for ChainRef {
    async fn frames(
        &self,
        range: BlockRange,
        tip: FinalizedTip,
        cancellation: &CancellationToken,
    ) -> Result<FrameChunks, SourceError> {
        self.0.frames(range, tip, cancellation).await
    }
}

/// A block's logs and its calls' inputs by transaction index.
type Contents = (Vec<Log>, Vec<(u32, Vec<u8>)>);

/// Blocks holding the contents given for their number, empty otherwise.
struct Frames(std::collections::BTreeMap<u64, Contents>);

#[async_trait::async_trait]
impl FinalizedBlocks for Frames {
    async fn frames(
        &self,
        range: BlockRange,
        _tip: FinalizedTip,
        _cancellation: &CancellationToken,
    ) -> Result<FrameChunks, SourceError> {
        Ok(one_chunk(
            (range.start().0..=range.end().0)
                .map(|number| {
                    let (logs, inputs) = self.0.get(&number).cloned().unwrap_or_default();
                    frame(number, logs, inputs)
                })
                .collect(),
        ))
    }
}

/// OP Mainnet's factory creating `game` of `game_type` in transaction 6.
fn super_game(game: Address, game_type: u64) -> (Log, (u32, Vec<u8>)) {
    (
        log(
            address!("0xe5965Ab5962eDc7477C8520243A95517CD252fA9"),
            vec![
                DISPUTE_GAME_CREATED,
                game.into_word(),
                B256::from(U256::from(game_type)),
                SUPER_ROOT,
            ],
            Vec::new(),
            6,
        ),
        (6, super_root_call(&super_root_preimage())),
    )
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn the_lane_follows_a_new_respected_game_type_and_keeps_what_it_learned() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let cursor_path = directory.path().join("cursor.json");
    let checkpoint = ConsensusCheckpoint {
        beacon_slot: 0,
        beacon_block_root: [0; 32],
        execution_block_hash: BlockHash::ZERO,
        obtained_at_unix_seconds: 0,
        source: "fixture".to_owned(),
    };
    let signer = Address::repeat_byte(0x51);
    let game = Address::repeat_byte;
    let (old_type, old_call) = super_game(game(0xa1), 9);
    let (unwatched, unwatched_call) = super_game(game(0xa2), 9);
    let (new_type, new_call) = super_game(game(0xa3), 1);
    let (after_restart, after_restart_call) = super_game(game(0xa4), 1);
    let blocks = std::collections::BTreeMap::from([
        (8, (vec![old_type], vec![old_call])),
        (
            9,
            (
                vec![
                    respected_game_type(1),
                    config_update(SYSTEM_CONFIG, 3, signer_update(signer)),
                ],
                Vec::new(),
            ),
        ),
        (
            10,
            (
                vec![unwatched.clone(), new_type],
                vec![unwatched_call.clone(), new_call],
            ),
        ),
        (
            11,
            (
                vec![unwatched, after_restart],
                vec![unwatched_call, after_restart_call],
            ),
        ),
    ]);
    let blocks = Arc::new(Frames(blocks));
    let run = |tip: u64, follow_game_type: bool| {
        let lane = AnchorLane {
            anchor: OP_MAINNET,
            finality: Arc::new(Scripted {
                events: vec![finalized(tip)],
            }),
            checkpoint: checkpoint.clone(),
            blocks: FramesRef(blocks.clone()),
            cursor_path: cursor_path.clone(),
            lookback: 2,
            chain_id: 10,
            system_config: Some(SYSTEM_CONFIG),
            follow_game_type,
            batches: None,
        };
        async move {
            let (sender, mut received) = mpsc::channel(64);
            let _ = lane.run(sender, CancellationToken::new()).await;
            let mut observed = Vec::new();
            while let Ok(observation) = received.try_recv() {
                observed.push(match observation {
                    Observation::GameCreated { game, .. } => format!("game {:x}", game.0[0]),
                    other => format!("{other:?}"),
                });
            }
            observed
        }
    };
    assert_eq!(
        run(10, true).await,
        [
            "game a1".to_owned(),
            "GameTypeChanged { game_type: 1 }".to_owned(),
            format!("SignerChanged {{ signer: {signer} }}"),
            "game a3".to_owned(),
        ]
    );
    assert_eq!(
        learned(&cursor_path).expect("learned"),
        Learned {
            signer: Some(signer),
            game_type: Some(1),
            batcher: None,
        }
    );
    // A restart keeps the learned type, and hands on the range it read last
    // again under it: the old type's game a1 stays out. A configured type
    // stays fixed.
    std::fs::write(
        directory.path().join("kept.json"),
        std::fs::read(&cursor_path).unwrap(),
    )
    .unwrap();
    assert_eq!(
        run(11, true).await,
        [
            "GameTypeChanged { game_type: 1 }".to_owned(),
            format!("SignerChanged {{ signer: {signer} }}"),
            "game a3".to_owned(),
            "game a4".to_owned(),
        ]
    );
    std::fs::copy(directory.path().join("kept.json"), &cursor_path).unwrap();
    assert_eq!(
        run(11, false).await,
        [
            "game a1".to_owned(),
            "GameTypeChanged { game_type: 1 }".to_owned(),
            format!("SignerChanged {{ signer: {signer} }}"),
            "game a2".to_owned(),
            "game a2".to_owned(),
        ]
    );
}

struct FramesRef(Arc<Frames>);

#[async_trait::async_trait]
impl FinalizedBlocks for FramesRef {
    async fn frames(
        &self,
        range: BlockRange,
        tip: FinalizedTip,
        cancellation: &CancellationToken,
    ) -> Result<FrameChunks, SourceError> {
        self.0.frames(range, tip, cancellation).await
    }
}
