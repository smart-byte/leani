use leani_primitives::{
    Address, BLOCK_FRAME_SCHEMA_VERSION, BlobSidecar, BlockFrame, BlockHash, BlockNumber,
    BlockRange, BlockRef, ChainId, ChangeCursor, CheckStatus, Completeness, ConsensusAnchor,
    FilterScope, Finality, HeaderEnvelope, Log, Material, MissingReason, ObjectIdentity,
    ProcessorCursor, Provenance, Quantity, ReceiptEnvelope, SourceId, SourceKind, StateDiff,
    TopicFilter, Trace, TransactionEnvelope, TransactionHash, TrustModel, VerificationCheck,
    VerificationReport, Withdrawal,
    durable::{DurableKind, decode, encode},
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

#[test]
fn processor_cursor_v1_encoding_is_stable() {
    let cursor = ProcessorCursor {
        processor_id: "fixture".to_owned(),
        processor_version: "1.2.3".to_owned(),
        chain_id: ChainId(1),
        block_number: BlockNumber(42),
        block_hash: BlockHash::new([0x11; 32]),
        finality: Finality::Included,
        sequence: 77,
    };
    let encoded = encode(DurableKind::ProcessorCursor, 1, &cursor).expect("encode fixture");
    let expected = include_str!("fixtures/processor_cursor_v1.hex").trim();
    assert_eq!(hex::encode(&encoded), expected);
    let decoded: ProcessorCursor = decode(
        DurableKind::ProcessorCursor,
        1,
        &hex::decode(expected).expect("fixture hex"),
    )
    .expect("decode fixture");
    assert_eq!(decoded, cursor);
}

#[test]
fn block_frame_v1_encoding_is_stable() {
    let frame = fixture_frame();
    let encoded = encode(DurableKind::BlockFrame, BLOCK_FRAME_SCHEMA_VERSION, &frame)
        .expect("encode fixture");
    let expected = include_str!("fixtures/block_frame_v1.hex").trim();
    assert_eq!(hex::encode(&encoded), expected);
    let decoded: BlockFrame = decode(
        DurableKind::BlockFrame,
        BLOCK_FRAME_SCHEMA_VERSION,
        &hex::decode(expected).expect("fixture hex"),
    )
    .expect("decode fixture");
    assert_eq!(decoded, frame);
}

#[test]
fn block_frame_missing_material_v1_encoding_is_stable() {
    let mut frame = fixture_frame();
    frame.header = Material::Missing(MissingReason::NotRequested);
    frame.transactions = Material::Missing(MissingReason::Unsupported);
    frame.receipts = Material::Missing(MissingReason::NotAvailable);
    frame.logs = Material::Missing(MissingReason::TemporarilyUnavailable);
    frame.withdrawals = Material::Missing(MissingReason::Pruned);
    let encoded = encode(DurableKind::BlockFrame, BLOCK_FRAME_SCHEMA_VERSION, &frame)
        .expect("encode fixture");
    let expected = include_str!("fixtures/block_frame_missing_v1.hex").trim();
    assert_eq!(hex::encode(&encoded), expected);
    let decoded: BlockFrame = decode(
        DurableKind::BlockFrame,
        BLOCK_FRAME_SCHEMA_VERSION,
        &hex::decode(expected).expect("fixture hex"),
    )
    .expect("decode fixture");
    assert_eq!(decoded, frame);
}

const TRANSACTION_HASH: TransactionHash = TransactionHash::new([0x55; 32]);

/// A frame that exercises every material state, completeness claim, and
/// filter-scope field of the durable `BlockFrame` payload.
fn fixture_frame() -> BlockFrame {
    let scope = fixture_scope();
    let log = fixture_log();
    BlockFrame {
        chain_id: ChainId(1),
        block: BlockRef {
            number: BlockNumber(42),
            hash: BlockHash::new([0x11; 32]),
            parent_hash: BlockHash::new([0x22; 32]),
            timestamp: 1_700_000_000,
        },
        finality: Finality::Finalized,
        header: Material::Complete(fixture_header()),
        transactions: Material::Filtered {
            value: vec![fixture_transaction()],
            scope: scope.clone(),
            completeness: Completeness::VerifiedPredicate,
        },
        receipts: Material::Filtered {
            value: vec![fixture_receipt(log.clone())],
            scope: scope.clone(),
            completeness: Completeness::DatasetDeclared,
        },
        logs: Material::Filtered {
            value: vec![log],
            scope,
            completeness: Completeness::Partial,
        },
        withdrawals: Material::Complete(vec![Withdrawal {
            index: 1,
            validator_index: 2,
            address: Address::new([0x88; 20]),
            amount_gwei: 3,
        }]),
        blob_sidecars: Material::Complete(vec![BlobSidecar {
            transaction_hash: TRANSACTION_HASH,
            index: 0,
            versioned_hash: BlockHash::new([0x99; 32]),
            blob: vec![0xb1],
            commitment: vec![0xc1],
            proof: vec![0xd1],
        }]),
        traces: Material::Complete(vec![Trace {
            transaction_hash: TRANSACTION_HASH,
            schema: "fixture.trace.v1".to_owned(),
            encoded: vec![0x71],
        }]),
        state_diffs: Material::Complete(vec![StateDiff {
            address: Address::new([0x33; 20]),
            schema: "fixture.state-diff.v1".to_owned(),
            encoded: vec![0x51],
        }]),
        provenance: vec![fixture_provenance()],
        verification: fixture_verification(),
    }
}

fn fixture_scope() -> FilterScope {
    FilterScope {
        block_range: Some(BlockRange::new(BlockNumber(40), BlockNumber(44)).expect("range")),
        addresses: vec![Address::new([0x33; 20])],
        topics: vec![TopicFilter {
            position: 0,
            alternatives: vec![[0x44; 32]],
        }],
        transaction_hashes: vec![TRANSACTION_HASH],
        transaction_types: vec![2, 3],
        senders: vec![Address::new([0x66; 20])],
        recipients: vec![Address::new([0x77; 20])],
    }
}

fn fixture_log() -> Log {
    Log {
        address: Address::new([0x33; 20]),
        topics: vec![[0x44; 32], [0x45; 32]],
        data: vec![1, 2, 3],
        transaction_hash: Some(TRANSACTION_HASH),
        transaction_index: 0,
        log_index: 7,
    }
}

fn fixture_header() -> HeaderEnvelope {
    HeaderEnvelope {
        rlp: Some(vec![0xf9, 0x02, 0x10]),
        transactions_root: Some(BlockHash::new([0x81; 32])),
        receipts_root: Some(BlockHash::new([0x82; 32])),
        withdrawals_root: None,
        gas_limit: Some(30_000_000),
        gas_used: Some(21_000),
        base_fee_per_gas: Some(Quantity::new([0x07; 32])),
        blob_gas_used: Some(131_072),
        excess_blob_gas: Some(0),
        size_bytes: Some(1_234),
        transaction_count: Some(1),
        consensus_size_bytes: Some(5_678),
    }
}

fn fixture_transaction() -> TransactionEnvelope {
    TransactionEnvelope {
        hash: TRANSACTION_HASH,
        transaction_type: 3,
        index: 0,
        encoded: Some(vec![0x03, 0xf8]),
        from: Some(Address::new([0x66; 20])),
        to: Some(Address::new([0x77; 20])),
        nonce: Some(9),
        gas_limit: Some(21_000),
        value: Some(Quantity::new([0x01; 32])),
        input: Some(vec![0xde, 0xad]),
        max_fee_per_gas: Some(Quantity::new([0x02; 32])),
        max_priority_fee_per_gas: None,
        max_fee_per_blob_gas: Some(Quantity::new([0x03; 32])),
        blob_versioned_hashes: vec![BlockHash::new([0x99; 32])],
        size_bytes: Some(150),
    }
}

fn fixture_receipt(log: Log) -> ReceiptEnvelope {
    ReceiptEnvelope {
        transaction_hash: TRANSACTION_HASH,
        transaction_type: 3,
        transaction_index: 0,
        encoded: None,
        success: Some(true),
        gas_used: Some(21_000),
        effective_gas_price: Some(Quantity::new([0x04; 32])),
        blob_gas_used: Some(131_072),
        blob_gas_price: None,
        logs: vec![log],
    }
}

fn fixture_provenance() -> Provenance {
    Provenance {
        source_id: SourceId::new("fixture-source").expect("source ID"),
        source_kind: SourceKind::ExecutionP2p,
        trust: TrustModel::ProtocolVerified,
        range: Some(BlockRange::single(BlockNumber(42))),
        object: Some(ObjectIdentity {
            locator: "fixture://block/42".to_owned(),
            version: Some("1".to_owned()),
            checksum: Some([0xab; 32]),
            schema: None,
        }),
        observed_at_unix_ms: 1_700_000_000_000,
        projection: vec!["header".to_owned(), "logs".to_owned()],
    }
}

fn fixture_verification() -> VerificationReport {
    VerificationReport {
        header_hash: VerificationCheck::VERIFIED,
        parent_continuity: VerificationCheck::VERIFIED,
        transactions_root: VerificationCheck::NOT_CHECKED,
        receipts_root: VerificationCheck {
            status: CheckStatus::Unavailable,
            detail: Some("receipts were projected".to_owned()),
        },
        withdrawals_root: VerificationCheck::UNAVAILABLE,
        dataset_checksum: VerificationCheck::VERIFIED,
        consensus_anchor: Some(ConsensusAnchor {
            finality: Finality::Finalized,
            execution_block_hash: BlockHash::new([0x11; 32]),
            beacon_slot: 8_000_000,
            beacon_block_root: [0xcd; 32],
        }),
    }
}
