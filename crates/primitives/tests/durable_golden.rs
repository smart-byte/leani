use leani_primitives::{
    BlockHash, BlockNumber, BlockRef, ChainId, ChangeCursor,
    durable::{DurableKind, encode},
};

#[test]
fn block_ref_v1_encoding_is_stable() {
    let block = BlockRef {
        number: BlockNumber(42),
        hash: BlockHash::new([0x11; 32]),
        parent_hash: BlockHash::new([0x22; 32]),
        timestamp: 1_700_000_000,
    };
    let encoded = encode(DurableKind::BlockRef, 1, &block).expect("encode fixture");
    let expected = include_str!("fixtures/block_ref_v1.hex").trim();
    assert_eq!(hex::encode(encoded), expected);
}

#[test]
fn change_cursor_v1_encoding_is_stable() {
    let cursor = ChangeCursor {
        chain_id: ChainId(1),
        processor_id: "fixture".to_owned(),
        sequence: 77,
    };
    let encoded = encode(DurableKind::ChangeCursor, 1, &cursor).expect("encode fixture");
    let expected = include_str!("fixtures/change_cursor_v1.hex").trim();
    assert_eq!(hex::encode(encoded), expected);
}
