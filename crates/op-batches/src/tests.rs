use std::io::{Read, Write};

use alloy_primitives::{B256, b256};

use super::*;

/// A blob Base's batcher posted in Ethereum block 26,356,996: the last frame
/// of channel `438721c4…`.
fn base_blob() -> Vec<u8> {
    let mut blob = Vec::new();
    flate2::read::GzDecoder::new(
        &include_bytes!("../tests/fixtures/base-last-frame-blob.bin.gz")[..],
    )
    .read_to_end(&mut blob)
    .expect("fixture decompresses");
    blob
}

const BASE_BLOB_HASH: B256 =
    b256!("01292fd550222609e98aff3e7f1ef800cc28bbe4370675ae07839fe8d6cc1705");

#[test]
fn a_blob_decodes_to_the_frames_the_batcher_posted() {
    let frames = frames(&blob_data(&base_blob()).expect("blob decodes")).expect("frames decode");
    assert_eq!(frames.len(), 1);
    let frame = &frames[0];
    assert_eq!(frame.channel[..4], [0x43, 0x87, 0x21, 0xc4]);
    assert_eq!(frame.number, 6);
    assert!(frame.last);
    assert_eq!(frame.data.len(), 366);
}

#[test]
fn a_blob_matches_only_the_versioned_hash_of_its_commitment() {
    let mut blob = base_blob();
    assert_eq!(versioned_hash(&blob).expect("commitment"), BASE_BLOB_HASH);
    blob[100] ^= 1;
    assert_ne!(versioned_hash(&blob).expect("commitment"), BASE_BLOB_HASH);
}

#[test]
fn a_blob_with_data_past_its_length_is_refused() {
    let mut blob = base_blob();
    blob[BLOB_BYTES - 1] = 1;
    assert!(matches!(blob_data(&blob), Err(Error::Blob(_))));
    let mut blob = base_blob();
    blob[1] = 1;
    assert!(matches!(blob_data(&blob), Err(Error::Blob(_))));
}

#[test]
fn a_span_batch_names_its_parent_and_how_many_blocks_follow() {
    // The whole channel Mode's batcher posted in Ethereum block 26,356,926:
    // one span batch of 867 blocks after block 46,906,110, whose hash is
    // 0x56562d6dd27c555278f14d51301de67a4bcb374eb58277ef3db13053d1ff552b.
    let channel = include_bytes!("../tests/fixtures/mode-span-channel.bin");
    let read: Vec<_> = batches(channel)
        .expect("brotli channel")
        .collect::<Result<_, _>>()
        .expect("batches decode");
    let parent = b256!("56562d6dd27c555278f14d51301de67a4bcb374eb58277ef3db13053d1ff552b");
    let mut parent_check = [0; 20];
    parent_check.copy_from_slice(&parent[..20]);
    assert_eq!(read.len(), 1);
    assert!(matches!(
        read[0],
        Batch::Span {
            relative_timestamp: 93_812_222,
            l1_origin: 26_356_920,
            parent_check: check,
            blocks: 867,
            ..
        } if check == parent_check
    ));
}

#[test]
fn consecutive_singular_batches_name_the_block_before_them() {
    // The first three batches of a Base channel, recompressed whole: blocks
    // 53,963,992 to 53,963,994, all in the same second.
    let channel = include_bytes!("../tests/fixtures/base-singular-batches.bin");
    let read: Vec<_> = batches(channel)
        .expect("brotli channel")
        .collect::<Result<_, _>>()
        .expect("batches decode");
    let parents: Vec<_> = read
        .iter()
        .map(|batch| match batch {
            Batch::Singular {
                parent_hash,
                timestamp,
                ..
            } => (*parent_hash, *timestamp),
            Batch::Span { .. } => panic!("Base posts singular batches"),
        })
        .collect();
    assert_eq!(
        parents,
        [
            (
                b256!("92ae788fd8f15866bee1ec2d168248663e47635211661489d8f41392452df99f"),
                1_793_983_174
            ),
            (
                b256!("0e705f8b5b729a0f669327a8e43f517d873439e8f1a3c0992354d1a874376731"),
                1_793_983_174
            ),
            (
                b256!("e812f41c6a76ac83c1bd91b09b4795b9ea2c39464c86c189922ea2cd48a64ff1"),
                1_793_983_174
            ),
        ]
    );
}

fn frame(channel: u8, number: u16, data: &[u8], last: bool) -> Frame {
    Frame {
        channel: [channel; 16],
        number,
        data: data.to_vec(),
        last,
    }
}

#[test]
fn a_channel_completes_when_its_frames_arrive_in_order() {
    let mut channels = Channels::default();
    assert_eq!(channels.add(frame(1, 0, b"ab", false), 10), None);
    assert_eq!(channels.open_since(), Some(10));
    assert_eq!(channels.add(frame(1, 1, b"cd", false), 11), None);
    assert_eq!(
        channels.add(frame(1, 2, b"e", true), 12),
        Some(b"abcde".to_vec())
    );
    assert_eq!(channels.open_since(), None);
}

#[test]
fn a_frame_out_of_order_is_dropped_and_the_channel_stays_open() {
    let mut channels = Channels::default();
    channels.add(frame(1, 0, b"a", false), 10);
    assert_eq!(channels.add(frame(1, 2, b"x", true), 10), None);
    assert_eq!(channels.add(frame(2, 1, b"x", true), 10), None);
    assert_eq!(
        channels.add(frame(1, 1, b"b", true), 11),
        Some(b"ab".to_vec())
    );
    // A frame of a channel that already completed is out of order too.
    assert_eq!(channels.add(frame(1, 2, b"c", true), 11), None);
}

#[test]
fn a_new_channels_first_frame_drops_an_unfinished_channel() {
    let mut channels = Channels::default();
    channels.add(frame(1, 0, b"a", false), 10);
    channels.add(frame(2, 0, b"x", false), 11);
    assert_eq!(channels.add(frame(1, 1, b"b", true), 11), None);
    assert_eq!(
        channels.add(frame(2, 1, b"y", true), 12),
        Some(b"xy".to_vec())
    );
}

#[test]
fn a_channel_open_past_the_timeout_is_dropped() {
    let mut channels = Channels::default();
    channels.add(frame(1, 0, b"a", false), 10);
    assert_eq!(
        channels.add(frame(1, 1, b"b", true), 10 + CHANNEL_TIMEOUT + 1),
        None
    );
    let mut channels = Channels::default();
    channels.add(frame(1, 0, b"a", false), 10);
    assert_eq!(
        channels.add(frame(1, 1, b"b", true), 10 + CHANNEL_TIMEOUT),
        Some(b"ab".to_vec())
    );
}

fn rlp_bytes(bytes: &[u8]) -> Vec<u8> {
    rlp_with(0x80, bytes)
}

fn rlp_list(items: &[Vec<u8>]) -> Vec<u8> {
    rlp_with(0xc0, &items.concat())
}

fn rlp_with(offset: u8, payload: &[u8]) -> Vec<u8> {
    if offset == 0x80 && payload.len() == 1 && payload[0] < 0x80 {
        return payload.to_vec();
    }
    let mut out = if payload.len() <= 55 {
        vec![offset + u8::try_from(payload.len()).expect("short payload")]
    } else {
        let length = (payload.len() as u64).to_be_bytes();
        let length: Vec<u8> = length.into_iter().skip_while(|byte| *byte == 0).collect();
        let mut out = vec![offset + 55 + u8::try_from(length.len()).expect("length of a length")];
        out.extend(length);
        out
    };
    out.extend_from_slice(payload);
    out
}

fn rlp_uint(value: u64) -> Vec<u8> {
    let bytes: Vec<u8> = value
        .to_be_bytes()
        .into_iter()
        .skip_while(|byte| *byte == 0)
        .collect();
    rlp_bytes(&bytes)
}

fn zlib_channel(batches: &[Vec<u8>]) -> Vec<u8> {
    let stream: Vec<u8> = batches.iter().flat_map(|batch| rlp_bytes(batch)).collect();
    let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(&stream).expect("compress");
    encoder.finish().expect("compress")
}

fn singular_batch(parent: B256, epoch: u64, timestamp: u64) -> Vec<u8> {
    let mut batch = vec![0];
    batch.extend(rlp_list(&[
        rlp_bytes(parent.as_slice()),
        rlp_uint(epoch),
        rlp_bytes(B256::repeat_byte(7).as_slice()),
        rlp_uint(timestamp),
        rlp_list(&[rlp_bytes(&[2; 300])]),
    ]));
    batch
}

#[test]
fn a_zlib_channel_reads_like_a_brotli_one() {
    let span = {
        let mut batch = vec![1];
        // relative timestamp 300, L1 origin 20,000,000, two checks, 3 blocks.
        batch.extend([0xac, 0x02, 0x80, 0xda, 0xc4, 0x09]);
        batch.extend([0x11; 20]);
        batch.extend([0x22; 20]);
        batch.push(3);
        batch.extend([0; 40]);
        batch
    };
    let channel = zlib_channel(&[singular_batch(B256::repeat_byte(9), 5, 1_000), span]);
    let read: Vec<_> = batches(&channel)
        .expect("zlib channel")
        .collect::<Result<_, _>>()
        .expect("batches decode");
    assert_eq!(
        read,
        vec![
            Batch::Singular {
                parent_hash: B256::repeat_byte(9),
                epoch: 5,
                epoch_hash: B256::repeat_byte(7),
                timestamp: 1_000,
            },
            Batch::Span {
                relative_timestamp: 300,
                l1_origin: 20_000_000,
                parent_check: [0x11; 20],
                l1_origin_check: [0x22; 20],
                blocks: 3,
            },
        ]
    );
}

#[test]
fn reading_stops_at_the_decompressed_size_limit() {
    let channel = zlib_channel(&[
        singular_batch(B256::repeat_byte(1), 1, 1),
        singular_batch(B256::repeat_byte(2), 1, 2),
    ]);
    let first_batch_bytes = 3 + singular_batch(B256::ZERO, 1, 1).len() as u64;
    let read: Vec<_> = batches_within(&channel, first_batch_bytes + 10)
        .expect("zlib channel")
        .collect();
    assert_eq!(read.len(), 2);
    assert!(read[0].is_ok());
    assert!(matches!(read[1], Err(Error::Truncated)));
}

#[test]
fn a_channel_of_another_compression_is_refused() {
    assert!(matches!(batches(&[2, 0, 0]), Err(Error::Batch(_))));
}
