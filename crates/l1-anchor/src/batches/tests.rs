use std::sync::Mutex;

use alloy_primitives::{address, b256, keccak256};
use leani_primitives::{
    BlockHash, BlockNumber, BlockRef, ChainId, Finality, Log, MissingReason, TransactionHash,
};

use tokio_util::sync::CancellationToken;

use super::*;

const INBOX: Address = address!("0xff00000000000000000000000000000000008453");
const BATCHER: Address = address!("0x5050f69a9786f081509234f1a7f4684b5e5b76c9");
/// Base's blocks 53,963,991 to 53,963,994, with the batches Base's batcher
/// posted for the last three.
const BASE_BLOCKS: [(u64, B256); 4] = [
    (
        53_963_991,
        b256!("0x92ae788fd8f15866bee1ec2d168248663e47635211661489d8f41392452df99f"),
    ),
    (
        53_963_992,
        b256!("0x0e705f8b5b729a0f669327a8e43f517d873439e8f1a3c0992354d1a874376731"),
    ),
    (
        53_963_993,
        b256!("0xe812f41c6a76ac83c1bd91b09b4795b9ea2c39464c86c189922ea2cd48a64ff1"),
    ),
    (
        53_963_994,
        b256!("0x0cefcadd58b66184c5beb570b93627dfd33b62aac3bb18621d8d4054470d7151"),
    ),
];
const BASE_TIME: u64 = 1_793_983_174;

struct Held {
    blocks: Mutex<Vec<(u64, B256)>>,
    head: Mutex<(u64, u64)>,
}

impl Held {
    fn new(blocks: &[(u64, B256)], head: (u64, u64)) -> Arc<Self> {
        Arc::new(Self {
            blocks: Mutex::new(blocks.to_vec()),
            head: Mutex::new(head),
        })
    }
}

#[async_trait::async_trait]
impl HeldBlocks for Held {
    async fn hash_at(&self, number: u64) -> Result<Option<B256>, SourceError> {
        Ok(self
            .blocks
            .lock()
            .unwrap()
            .iter()
            .find(|(held, _)| *held == number)
            .map(|(_, hash)| *hash))
    }
    async fn number_of(&self, hash: B256) -> Result<Option<u64>, SourceError> {
        Ok(self
            .blocks
            .lock()
            .unwrap()
            .iter()
            .find(|(_, held)| *held == hash)
            .map(|(number, _)| *number))
    }
    async fn head(&self) -> Result<Option<(u64, u64)>, SourceError> {
        Ok(Some(*self.head.lock().unwrap()))
    }
}

fn base_reader(held: Arc<Held>) -> BatchReader {
    BatchReader::new(
        BatchInbox::OpStack {
            inbox: INBOX,
            batcher: BATCHER,
            cadence: Cadence {
                genesis_time: 1_686_789_347,
                seconds: 2,
                first_block: Some(0),
                fast_from: None,
            },
        },
        held,
        None,
    )
}

fn frame_data(id: u8, number: u16, data: &[u8], last: bool) -> Vec<u8> {
    let mut bytes = vec![0];
    bytes.extend([id; 16]);
    bytes.extend(number.to_be_bytes());
    bytes.extend(
        u32::try_from(data.len())
            .expect("frame length")
            .to_be_bytes(),
    );
    bytes.extend(data);
    bytes.push(u8::from(last));
    bytes
}

struct Posted {
    from: Address,
    to: Address,
    input: Vec<u8>,
    success: bool,
}

fn posted(input: Vec<u8>) -> Posted {
    Posted {
        from: BATCHER,
        to: INBOX,
        input,
        success: true,
    }
}

fn l1_hash(number: u64) -> BlockHash {
    BlockHash::new([u8::try_from(number % 256).expect("one byte"); 32])
}

fn l1_block(number: u64, transactions: Vec<Posted>, logs: Vec<Log>) -> BlockFrame {
    let receipts = transactions
        .iter()
        .enumerate()
        .map(|(index, posted)| leani_primitives::ReceiptEnvelope {
            transaction_hash: TransactionHash::ZERO,
            transaction_type: 2,
            transaction_index: u32::try_from(index).expect("index"),
            encoded: None,
            success: Some(posted.success),
            gas_used: None,
            effective_gas_price: None,
            blob_gas_used: None,
            blob_gas_price: None,
            logs: Vec::new(),
        })
        .collect();
    BlockFrame {
        chain_id: ChainId(1),
        block: BlockRef {
            number: BlockNumber(number),
            hash: l1_hash(number),
            parent_hash: l1_hash(number - 1),
            timestamp: 1_793_983_200,
        },
        finality: Finality::Finalized,
        header: Material::Missing(MissingReason::NotRequested),
        receipts: Material::Complete(receipts),
        logs: Material::Complete(logs),
        withdrawals: Material::Missing(MissingReason::NotRequested),
        blob_sidecars: Material::Missing(MissingReason::NotRequested),
        traces: Material::Missing(MissingReason::NotRequested),
        state_diffs: Material::Missing(MissingReason::NotRequested),
        provenance: Vec::new(),
        verification: leani_primitives::VerificationReport::default(),
        transactions: Material::Complete(
            transactions
                .into_iter()
                .enumerate()
                .map(|(index, posted)| TransactionEnvelope {
                    hash: TransactionHash::ZERO,
                    transaction_type: 2,
                    index: u32::try_from(index).expect("index"),
                    encoded: None,
                    from: Some(posted.from.into()),
                    to: Some(posted.to.into()),
                    nonce: None,
                    gas_limit: None,
                    value: None,
                    input: Some(posted.input),
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

const BASE_CHANNEL: &[u8] =
    include_bytes!("../../../op-batches/tests/fixtures/base-singular-batches.bin");

fn base_post(l1: u64) -> BlockFrame {
    l1_block(
        l1,
        vec![posted(frame_data(7, 0, BASE_CHANNEL, true))],
        Vec::new(),
    )
}

async fn read(reader: &mut BatchReader, frame: &BlockFrame) -> Option<(u64, B256)> {
    reader.read(frame, BATCHER).await.expect("batches read")
}

#[tokio::test]
async fn singular_batches_finalize_the_blocks_they_build_on() {
    let mut reader = base_reader(Held::new(&BASE_BLOCKS, (53_963_995, BASE_TIME + 1)));
    assert_eq!(
        read(&mut reader, &base_post(100)).await,
        Some(BASE_BLOCKS[2])
    );
    assert_eq!(reader.cursor().next_parent, Some(53_963_994));
    // A batcher may post a channel again: its blocks are read already.
    assert_eq!(read(&mut reader, &base_post(101)).await, None);
    assert_eq!(reader.cursor().next_parent, Some(53_963_994));
}

#[tokio::test]
async fn a_batch_that_skips_blocks_finalizes_nothing() {
    let mut reader = base_reader(Held::new(&BASE_BLOCKS, (53_963_995, BASE_TIME + 1)));
    reader.resume(BatchCursor {
        next_parent: Some(53_963_990),
        resume_from: None,
    });
    assert_eq!(read(&mut reader, &base_post(100)).await, None);
    assert_eq!(reader.cursor().next_parent, Some(53_963_990));
}

#[tokio::test]
async fn matching_starts_at_the_first_batch_whose_parent_the_node_holds() {
    let mut held = BASE_BLOCKS;
    held[0].1 = B256::repeat_byte(9);
    let mut reader = base_reader(Held::new(&held, (53_963_995, BASE_TIME + 1)));
    assert_eq!(
        read(&mut reader, &base_post(100)).await,
        Some(BASE_BLOCKS[2])
    );
}

#[tokio::test]
async fn a_node_on_another_branch_finalizes_nothing_past_where_the_batches_leave_it() {
    let mut held = BASE_BLOCKS;
    held[1].1 = B256::repeat_byte(9);
    held[2].1 = B256::repeat_byte(8);
    let mut reader = base_reader(Held::new(&held, (53_963_995, BASE_TIME + 1)));
    reader.resume(BatchCursor {
        next_parent: Some(53_963_991),
        resume_from: None,
    });
    assert_eq!(
        read(&mut reader, &base_post(100)).await,
        Some(BASE_BLOCKS[0])
    );
    // The batches go on along their own chain.
    assert_eq!(reader.cursor().next_parent, Some(53_963_994));
}

#[tokio::test]
async fn failed_transactions_and_other_senders_are_not_batches() {
    let mut reader = base_reader(Held::new(&BASE_BLOCKS, (53_963_995, BASE_TIME + 1)));
    let failed = Posted {
        success: false,
        ..posted(frame_data(7, 0, BASE_CHANNEL, true))
    };
    let foreign = Posted {
        from: Address::repeat_byte(3),
        ..posted(frame_data(7, 0, BASE_CHANNEL, true))
    };
    assert_eq!(
        read(
            &mut reader,
            &l1_block(100, vec![failed, foreign], Vec::new())
        )
        .await,
        None
    );
}

#[tokio::test]
async fn matching_starts_over_while_the_node_lacks_a_batchs_blocks() {
    let mut reader = base_reader(Held::new(&BASE_BLOCKS, (53_963_990, BASE_TIME - 1)));
    reader.resume(BatchCursor {
        next_parent: Some(53_963_991),
        resume_from: None,
    });
    assert_eq!(read(&mut reader, &base_post(100)).await, None);
    assert_eq!(reader.cursor().next_parent, None);
}

#[tokio::test]
async fn a_channel_split_across_l1_blocks_is_read_again_from_its_first_frame() {
    let (first, second) = BASE_CHANNEL.split_at(4_000);
    let first_post = l1_block(
        100,
        vec![posted(frame_data(7, 0, first, false))],
        Vec::new(),
    );
    let second_post = l1_block(
        101,
        vec![posted(frame_data(7, 1, second, true))],
        Vec::new(),
    );
    let held = Held::new(&BASE_BLOCKS, (53_963_995, BASE_TIME + 1));
    let mut reader = base_reader(held.clone());
    assert_eq!(read(&mut reader, &first_post).await, None);
    let cursor = reader.cursor();
    assert_eq!(cursor.resume_from, Some(100));
    // A restart rebuilds the channel from block 100.
    let mut restarted = base_reader(held);
    restarted.resume(cursor);
    assert_eq!(read(&mut restarted, &first_post).await, None);
    assert_eq!(
        read(&mut restarted, &second_post).await,
        Some(BASE_BLOCKS[2])
    );
    assert_eq!(restarted.cursor().resume_from, None);
}

#[tokio::test]
async fn a_span_batch_finalizes_the_block_its_parent_check_names() {
    // Mode's batcher posted this channel in Ethereum block 26,356,926: one
    // span batch of 867 blocks after block 46,906,110.
    let channel = include_bytes!("../../../op-batches/tests/fixtures/mode-span-channel.bin");
    let parent = b256!("0x56562d6dd27c555278f14d51301de67a4bcb374eb58277ef3db13053d1ff552b");
    let held = Held::new(&[(46_906_110, parent)], (46_907_000, 0));
    let mut reader = BatchReader::new(
        BatchInbox::OpStack {
            inbox: INBOX,
            batcher: BATCHER,
            cadence: Cadence {
                genesis_time: 1_700_167_583,
                seconds: 2,
                first_block: Some(0),
                fast_from: None,
            },
        },
        held,
        None,
    );
    let post = l1_block(
        26_356_926,
        vec![posted(frame_data(1, 0, channel, true))],
        Vec::new(),
    );
    assert_eq!(read(&mut reader, &post).await, Some((46_906_110, parent)));
    assert_eq!(reader.cursor().next_parent, Some(46_906_110 + 867));
}

#[test]
fn sequencer_inbox_ids_are_their_signatures_hashes() {
    let posts = [
        "addSequencerL2BatchFromBlobs(uint256,uint256,address,uint256,uint256)",
        "addSequencerL2BatchFromBlobsDelayProof(uint256,uint256,address,uint256,uint256,(bytes32,(uint8,address,uint64,uint64,uint256,uint256,bytes32)))",
        "addSequencerL2BatchFromOrigin(uint256,bytes,uint256,address,uint256,uint256)",
        "addSequencerL2BatchFromOriginDelayProof(uint256,bytes,uint256,address,uint256,uint256,(bytes32,(uint8,address,uint64,uint64,uint256,uint256,bytes32)))",
        "addSequencerL2Batch(uint256,bytes,uint256,address,uint256,uint256)",
        "addSequencerL2BatchDelayProof(uint256,bytes,uint256,address,uint256,uint256,(bytes32,(uint8,address,uint64,uint64,uint256,uint256,bytes32)))",
    ];
    for ((selector, _), signature) in NITRO_BATCH_POSTS.iter().zip(posts) {
        assert_eq!(selector[..], keccak256(signature)[..4], "{signature}");
    }
    assert_eq!(
        SEQUENCER_BATCH_DELIVERED,
        keccak256(
            "SequencerBatchDelivered(uint256,bytes32,bytes32,bytes32,uint256,(uint64,uint64,uint64,uint64),uint8)"
        )
    );
}

#[tokio::test]
async fn a_nitro_batch_post_finalizes_the_block_of_its_last_message() {
    const SEQUENCER_INBOX: Address = address!("0x1c479675ad559dc151f6ec7ed3fbf8cee79582b6");
    let word = |value: u64| alloy_primitives::U256::from(value).to_be_bytes::<32>();
    // addSequencerL2BatchFromBlobs(7, 10, refunder, 100, 150).
    let mut input = vec![0x3e, 0x5a, 0xa0, 0x82];
    for value in [7, 10, 0, 100, 150] {
        input.extend(word(value));
    }
    let delivered = Log {
        address: SEQUENCER_INBOX.into(),
        topics: vec![SEQUENCER_BATCH_DELIVERED.0, [0; 32], [0; 32], [0; 32]],
        data: Vec::new(),
        transaction_hash: None,
        transaction_index: 1,
        log_index: 0,
    };
    let other = Posted {
        from: Address::repeat_byte(1),
        to: SEQUENCER_INBOX,
        input: input.clone(),
        success: true,
    };
    let post = Posted {
        from: Address::repeat_byte(1),
        to: SEQUENCER_INBOX,
        input,
        success: true,
    };
    let last = 22_207_817 + 149;
    let held = Held::new(&[(last, B256::repeat_byte(5))], (last + 10, 0));
    let mut reader = BatchReader::new(
        BatchInbox::Nitro {
            sequencer_inbox: SEQUENCER_INBOX,
            genesis: 22_207_817,
        },
        held,
        None,
    );
    // Only the transaction that emitted `SequencerBatchDelivered` counts.
    let frame = l1_block(100, vec![other, post], vec![delivered]);
    assert_eq!(
        read(&mut reader, &frame).await,
        Some((last, B256::repeat_byte(5)))
    );
    let unconfirmed = l1_block(
        101,
        vec![Posted {
            from: Address::repeat_byte(1),
            to: SEQUENCER_INBOX,
            input: Vec::new(),
            success: true,
        }],
        Vec::new(),
    );
    assert_eq!(read(&mut reader, &unconfirmed).await, None);
}

struct Scripted(Vec<leani_source_api::FinalityEvent>);

#[async_trait::async_trait]
impl leani_source_api::FinalitySource for Scripted {
    fn descriptor(&self) -> &leani_source_api::SourceDescriptor {
        unimplemented!("unused")
    }

    async fn subscribe(
        &self,
        _checkpoint: leani_source_api::ConsensusCheckpoint,
        _cancellation: CancellationToken,
    ) -> Result<leani_source_api::FinalityEventStream, SourceError> {
        use futures::StreamExt;
        Ok(futures::stream::iter(self.0.clone().into_iter().map(Ok)).boxed())
    }
}

struct Posts(std::collections::BTreeMap<u64, BlockFrame>);

#[async_trait::async_trait]
impl crate::FinalizedBlocks for Posts {
    async fn frames(
        &self,
        range: leani_primitives::BlockRange,
        _tip: crate::FinalizedTip,
        _cancellation: &CancellationToken,
    ) -> Result<crate::FrameChunks, SourceError> {
        use futures::StreamExt;
        let frames = (range.start().0..=range.end().0)
            .map(|number| {
                self.0
                    .get(&number)
                    .cloned()
                    .unwrap_or_else(|| l1_block(number, Vec::new(), Vec::new()))
            })
            .collect::<Vec<_>>();
        Ok(futures::stream::iter([Ok(frames)]).boxed())
    }
}

#[tokio::test]
async fn the_lane_reports_final_blocks_and_rebuilds_an_open_channel_after_a_restart() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let cursor_path = directory.path().join("cursor.json");
    let (first, second) = BASE_CHANNEL.split_at(4_000);
    let posts = Arc::new(Posts(
        [
            (
                100,
                l1_block(
                    100,
                    vec![posted(frame_data(7, 0, first, false))],
                    Vec::new(),
                ),
            ),
            (
                101,
                l1_block(
                    101,
                    vec![posted(frame_data(7, 1, second, true))],
                    Vec::new(),
                ),
            ),
        ]
        .into(),
    ));
    let held = Held::new(&BASE_BLOCKS, (53_963_995, BASE_TIME + 1));
    let run = |tip: u64| {
        let lane = crate::AnchorLane {
            anchor: leani_chain::L1Anchor::NitroRollup {
                rollup: Address::ZERO,
                sequencer_inbox: None,
            },
            finality: Arc::new(Scripted(vec![leani_source_api::FinalityEvent::Finalized {
                block_number: BlockNumber(tip),
                block_hash: l1_hash(tip),
                beacon_slot: tip,
                beacon_block_root: [0; 32],
            }])),
            checkpoint: leani_source_api::ConsensusCheckpoint {
                beacon_slot: 0,
                beacon_block_root: [0; 32],
                execution_block_hash: BlockHash::ZERO,
                obtained_at_unix_seconds: 0,
                source: "fixture".to_owned(),
            },
            blocks: PostsRef(posts.clone()),
            cursor_path: cursor_path.clone(),
            lookback: 2,
            chain_id: 8453,
            system_config: None,
            follow_game_type: false,
            batches: Some(base_reader(held.clone())),
        };
        async move {
            let (sender, mut received) = tokio::sync::mpsc::channel(64);
            let _ = lane.run(sender, CancellationToken::new()).await;
            let mut final_blocks = Vec::new();
            while let Ok(observation) = received.try_recv() {
                if let crate::Observation::DataFinal { number, hash } = observation {
                    final_blocks.push((number, hash));
                }
            }
            final_blocks
        }
    };
    // The channel's first frame is in block 100; its last is not final yet.
    assert_eq!(run(100).await, []);
    // After a restart the lane reads block 100 again to rebuild the channel.
    assert_eq!(run(101).await, [BASE_BLOCKS[2]]);
}

struct PostsRef(Arc<Posts>);

#[async_trait::async_trait]
impl crate::FinalizedBlocks for PostsRef {
    async fn frames(
        &self,
        range: leani_primitives::BlockRange,
        tip: crate::FinalizedTip,
        cancellation: &CancellationToken,
    ) -> Result<crate::FrameChunks, SourceError> {
        self.0.frames(range, tip, cancellation).await
    }
}
