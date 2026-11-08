//! What an OP Stack batcher posts to Ethereum, read the way derivation reads
//! it since Holocene: blobs (or calldata) carry frames, frames make up one
//! channel at a time, and a channel's compressed data holds batches. Only a
//! batch's header is decoded: the block it builds on and how many blocks it
//! carries. Every input is untrusted and bounded.

use std::io::{self, Read};

use alloy_primitives::B256;
use sha2::{Digest, Sha256};

#[cfg(test)]
mod tests;

/// Bytes in a blob.
pub const BLOB_BYTES: usize = 131_072;
/// Data a version-0 blob can hold: 4 field elements of 31 bytes and 6 bits
/// each per round, less the 4-byte header.
const MAX_BLOB_DATA: usize = (4 * 31 + 3) * 1_024 - 4;
/// Rounds of 4 field elements in a blob.
const ROUNDS: usize = 1_024;
/// `MaxFrameLen` of the derivation spec.
const MAX_FRAME_BYTES: usize = 1_000_000;
/// `MAX_RLP_BYTES_PER_CHANNEL` since Fjord: a channel decompresses to at
/// most this many bytes, and batches past it are not read. It bounds the
/// compressed channel too.
pub const MAX_CHANNEL_BYTES: u64 = 100_000_000;
/// L1 blocks a channel may stay open after its first frame since Granite.
pub const CHANNEL_TIMEOUT: u64 = 50;
/// `MaxSpanBatchElementCount` of the derivation spec.
const MAX_SPAN_BLOCKS: u64 = 10_000_000;
/// A batch's header fits in this many bytes of its content: an RLP list
/// header and four fields for a singular batch, three varints and two checks
/// for a span batch.
const HEADER_BYTES: usize = 128;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid blob: {0}")]
    Blob(String),
    #[error("invalid frame: {0}")]
    Frame(String),
    #[error("invalid batch: {0}")]
    Batch(String),
    #[error("the channel ends inside a batch")]
    Truncated,
    #[error("KZG: {0}")]
    Kzg(String),
    #[error("decompression: {0}")]
    Io(#[from] io::Error),
}

/// The bytes a version-0 blob encodes.
///
/// # Errors
///
/// Returns an error for a blob of another size or version, a field element
/// that is not canonical, or bytes past the encoded length.
pub fn blob_data(blob: &[u8]) -> Result<Vec<u8>, Error> {
    if blob.len() != BLOB_BYTES {
        return Err(Error::Blob(format!(
            "{} bytes, not {BLOB_BYTES}",
            blob.len()
        )));
    }
    if blob[1] != 0 {
        return Err(Error::Blob(format!("encoding version {}", blob[1])));
    }
    let length = usize::from(blob[2]) << 16 | usize::from(blob[3]) << 8 | usize::from(blob[4]);
    if length > MAX_BLOB_DATA {
        return Err(Error::Blob(format!(
            "{length} bytes of data exceed {MAX_BLOB_DATA}"
        )));
    }
    let mut decoder = BlobDecoder {
        blob,
        output: vec![0; MAX_BLOB_DATA],
        at: 28,
        element: 32,
    };
    // The first field element's first 5 bytes hold the version and length.
    decoder.output[..27].copy_from_slice(&blob[5..32]);
    let mut high = [blob[0], 0, 0, 0];
    for slot in &mut high[1..] {
        *slot = decoder.element()?;
    }
    decoder.reassemble(high);
    for _ in 1..ROUNDS {
        if decoder.at >= length {
            break;
        }
        for slot in &mut high {
            *slot = decoder.element()?;
        }
        decoder.reassemble(high);
    }
    let BlobDecoder {
        mut output,
        element,
        ..
    } = decoder;
    if output[length..].iter().any(|byte| *byte != 0)
        || blob[element..].iter().any(|byte| *byte != 0)
    {
        return Err(Error::Blob("data past its length".to_owned()));
    }
    output.truncate(length);
    Ok(output)
}

/// Each field element gives 31 bytes and, from its first byte, 6 bits;
/// every 4 elements' 24 bits make 3 more bytes, written into the gaps left
/// after the first three elements' bytes.
struct BlobDecoder<'a> {
    blob: &'a [u8],
    output: Vec<u8>,
    at: usize,
    element: usize,
}

impl BlobDecoder<'_> {
    fn element(&mut self) -> Result<u8, Error> {
        let high = self.blob[self.element];
        if high & 0b1100_0000 != 0 {
            return Err(Error::Blob(format!(
                "field element {} is not canonical",
                self.element / 32
            )));
        }
        let end = (self.at + 31).min(MAX_BLOB_DATA);
        if self.at < end {
            let take = end - self.at;
            self.output[self.at..end]
                .copy_from_slice(&self.blob[self.element + 1..self.element + 1 + take]);
        }
        self.at += 32;
        self.element += 32;
        Ok(high)
    }

    fn reassemble(&mut self, high: [u8; 4]) {
        // The 128th byte of a round does not exist.
        self.at -= 1;
        let mut put = |offset: usize, byte: u8| {
            if let Some(slot) = self
                .at
                .checked_sub(offset)
                .and_then(|index| self.output.get_mut(index))
            {
                *slot = byte;
            }
        };
        put(96, (high[0] & 0x3f) | ((high[1] & 0x30) << 2));
        put(64, (high[1] & 0x0f) | ((high[3] & 0x0f) << 4));
        put(32, (high[2] & 0x3f) | ((high[3] & 0x30) << 2));
    }
}

/// The versioned hash that names `blob` in a transaction: the SHA-256 of its
/// KZG commitment, version byte `0x01`. A blob matches a transaction only if
/// this equals the hash the transaction carries.
///
/// # Errors
///
/// Returns an error for a blob that is not 131,072 bytes or not a vector of
/// canonical field elements.
pub fn versioned_hash(blob: &[u8]) -> Result<B256, Error> {
    let blob = c_kzg::Blob::from_bytes(blob).map_err(|error| Error::Kzg(format!("{error:?}")))?;
    let commitment = c_kzg::ethereum_kzg_settings(0)
        .blob_to_kzg_commitment(&blob)
        .map_err(|error| Error::Kzg(format!("{error:?}")))?;
    let mut hash: [u8; 32] = Sha256::digest(commitment.as_slice()).into();
    hash[0] = 0x01;
    Ok(B256::from(hash))
}

/// One frame of a channel.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Frame {
    pub channel: [u8; 16],
    pub number: u16,
    pub data: Vec<u8>,
    pub last: bool,
}

/// The frames of one blob's or one calldata transaction's data, derivation
/// version 0. Derivation drops every frame of data that has an invalid one.
///
/// # Errors
///
/// Returns an error for another derivation version, a truncated or oversized
/// frame, or an end marker other than 0 or 1.
pub fn frames(data: &[u8]) -> Result<Vec<Frame>, Error> {
    match data.first() {
        Some(0) => {}
        Some(version) => return Err(Error::Frame(format!("derivation version {version}"))),
        None => return Err(Error::Frame("no data".to_owned())),
    }
    let mut found = Vec::new();
    let mut at = 1;
    while at < data.len() {
        let header = data
            .get(at..at + 22)
            .ok_or_else(|| Error::Frame("truncated header".to_owned()))?;
        let length = u32::from_be_bytes([header[18], header[19], header[20], header[21]]);
        let length = usize::try_from(length).unwrap_or(usize::MAX);
        if length > MAX_FRAME_BYTES {
            return Err(Error::Frame(format!("{length} bytes")));
        }
        let body = data
            .get(at + 22..at + 22 + length)
            .ok_or_else(|| Error::Frame("truncated data".to_owned()))?;
        let last = match data.get(at + 22 + length) {
            Some(0) => false,
            Some(1) => true,
            Some(marker) => return Err(Error::Frame(format!("end marker {marker}"))),
            None => return Err(Error::Frame("no end marker".to_owned())),
        };
        let mut channel = [0; 16];
        channel.copy_from_slice(&header[..16]);
        found.push(Frame {
            channel,
            number: u16::from_be_bytes([header[16], header[17]]),
            data: body.to_vec(),
            last,
        });
        at += 23 + length;
    }
    Ok(found)
}

/// The one channel being assembled, under Holocene's rules: frames arrive
/// in order, a channel's first frame replaces an unfinished channel, and a
/// channel open for more than [`CHANNEL_TIMEOUT`] L1 blocks is dropped.
#[derive(Debug, Default)]
pub struct Channels {
    open: Option<OpenChannel>,
}

#[derive(Debug)]
struct OpenChannel {
    id: [u8; 16],
    since: u64,
    next: u16,
    data: Vec<u8>,
}

impl Channels {
    /// Add `frame`, read from L1 block `l1`. Returns the channel's
    /// compressed data once its last frame is in; drops a frame out of order.
    pub fn add(&mut self, frame: Frame, l1: u64) -> Option<Vec<u8>> {
        if self
            .open
            .as_ref()
            .is_some_and(|open| l1 > open.since.saturating_add(CHANNEL_TIMEOUT))
        {
            self.open = None;
        }
        if frame.number == 0 {
            self.open = Some(OpenChannel {
                id: frame.channel,
                since: l1,
                next: 0,
                data: Vec::new(),
            });
        }
        let open = self.open.as_mut()?;
        if open.id != frame.channel || open.next != frame.number {
            return None;
        }
        if open.data.len().saturating_add(frame.data.len()) as u64 > MAX_CHANNEL_BYTES {
            self.open = None;
            return None;
        }
        open.data.extend(frame.data);
        if frame.last {
            return self.open.take().map(|open| open.data);
        }
        match open.next.checked_add(1) {
            Some(next) => open.next = next,
            None => self.open = None,
        }
        None
    }

    /// The L1 block holding the open channel's first frame: reading again
    /// from there rebuilds the channel.
    #[must_use]
    pub fn open_since(&self) -> Option<u64> {
        self.open.as_ref().map(|open| open.since)
    }
}

/// What a batch says about the blocks it carries.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Batch {
    /// One block, built on the block hashed `parent_hash`.
    Singular {
        parent_hash: B256,
        epoch: u64,
        epoch_hash: B256,
        timestamp: u64,
    },
    /// `blocks` blocks in a row, the first built on the block whose hash
    /// begins with `parent_check`; `relative_timestamp` is the first block's
    /// time after the chain's genesis.
    Span {
        relative_timestamp: u64,
        l1_origin: u64,
        parent_check: [u8; 20],
        l1_origin_check: [u8; 20],
        blocks: u64,
    },
}

impl Batch {
    /// Blocks the batch carries.
    #[must_use]
    pub const fn blocks(&self) -> u64 {
        match self {
            Self::Singular { .. } => 1,
            Self::Span { blocks, .. } => *blocks,
        }
    }
}

/// The batches of a channel's compressed data, in order: brotli (version
/// byte 1) or zlib. Reading stops at the first error; the batches before it
/// stand.
///
/// # Errors
///
/// Returns an error for data that is neither brotli nor zlib.
pub fn batches(channel: &[u8]) -> Result<Batches<'_>, Error> {
    batches_within(channel, MAX_CHANNEL_BYTES)
}

fn batches_within(channel: &[u8], limit: u64) -> Result<Batches<'_>, Error> {
    let reader: Box<dyn Read + Send + '_> = match channel.first() {
        Some(1) => Box::new(brotli_decompressor::Decompressor::new(&channel[1..], 4_096)),
        // A zlib header: deflate (8) or the reserved method 15.
        Some(byte) if matches!(byte & 0x0f, 8 | 15) => {
            Box::new(flate2::read::ZlibDecoder::new(channel))
        }
        Some(byte) => return Err(Error::Batch(format!("channel compression {byte}"))),
        None => return Err(Error::Batch("empty channel".to_owned())),
    };
    Ok(Batches {
        reader: reader.take(limit),
        done: false,
    })
}

/// The batches of one channel, read lazily: each RLP byte string of the
/// decompressed stream is one batch, and only its header is kept.
pub struct Batches<'a> {
    reader: io::Take<Box<dyn Read + Send + 'a>>,
    done: bool,
}

impl std::fmt::Debug for Batches<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Batches")
            .field("done", &self.done)
            .finish_non_exhaustive()
    }
}

impl Iterator for Batches<'_> {
    type Item = Result<Batch, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let item = self.read();
        if !matches!(item, Some(Ok(_))) {
            self.done = true;
        }
        item
    }
}

impl Batches<'_> {
    fn read(&mut self) -> Option<Result<Batch, Error>> {
        let mut prefix = [0_u8; 1];
        match self.reader.read(&mut prefix) {
            Ok(0) => return None,
            Ok(_) => {}
            Err(error) => return Some(Err(error.into())),
        }
        Some(self.batch(prefix[0]))
    }

    fn batch(&mut self, prefix: u8) -> Result<Batch, Error> {
        let length = match prefix {
            0x80..=0xb7 => u64::from(prefix - 0x80),
            0xb8..=0xbf => {
                let mut bytes = [0_u8; 8];
                let size = usize::from(prefix - 0xb7);
                self.exact(&mut bytes[8 - size..])?;
                u64::from_be_bytes(bytes)
            }
            _ => {
                return Err(Error::Batch(format!(
                    "RLP prefix {prefix:#x}, not a byte string"
                )));
            }
        };
        let kept = usize::try_from(length.min(HEADER_BYTES as u64)).unwrap_or(HEADER_BYTES);
        let mut header = [0_u8; HEADER_BYTES];
        self.exact(&mut header[..kept])?;
        let skipped = io::copy(
            &mut (&mut self.reader).take(length - kept as u64),
            &mut io::sink(),
        )?;
        if skipped != length - kept as u64 {
            return Err(Error::Truncated);
        }
        let header = &header[..kept];
        match header.split_first() {
            Some((0, content)) => singular(content),
            Some((1, content)) => span(content),
            Some((version, _)) => Err(Error::Batch(format!("batch version {version}"))),
            None => Err(Error::Batch("empty batch".to_owned())),
        }
    }

    fn exact(&mut self, buffer: &mut [u8]) -> Result<(), Error> {
        self.reader.read_exact(buffer).map_err(|error| {
            if error.kind() == io::ErrorKind::UnexpectedEof {
                Error::Truncated
            } else {
                error.into()
            }
        })
    }
}

/// A singular batch's first four fields: `[parent_hash, epoch_number,
/// epoch_hash, timestamp, transactions]`.
fn singular(content: &[u8]) -> Result<Batch, Error> {
    let mut rlp = Rlp {
        bytes: content,
        at: 0,
    };
    rlp.list_header()?;
    let parent_hash = rlp.hash()?;
    let epoch = rlp.uint()?;
    let epoch_hash = rlp.hash()?;
    let timestamp = rlp.uint()?;
    Ok(Batch::Singular {
        parent_hash,
        epoch,
        epoch_hash,
        timestamp,
    })
}

fn span(content: &[u8]) -> Result<Batch, Error> {
    let mut at = 0;
    let relative_timestamp = varint(content, &mut at)?;
    let l1_origin = varint(content, &mut at)?;
    let checks = content
        .get(at..at + 40)
        .ok_or_else(|| Error::Batch("truncated span batch prefix".to_owned()))?;
    at += 40;
    let blocks = varint(content, &mut at)?;
    if blocks == 0 || blocks > MAX_SPAN_BLOCKS {
        return Err(Error::Batch(format!("span batch of {blocks} blocks")));
    }
    let mut parent_check = [0; 20];
    parent_check.copy_from_slice(&checks[..20]);
    let mut l1_origin_check = [0; 20];
    l1_origin_check.copy_from_slice(&checks[20..]);
    Ok(Batch::Span {
        relative_timestamp,
        l1_origin,
        parent_check,
        l1_origin_check,
        blocks,
    })
}

/// An unsigned LEB128 varint, as span batches encode integers.
fn varint(bytes: &[u8], at: &mut usize) -> Result<u64, Error> {
    let mut value = 0_u64;
    for shift in (0..64).step_by(7) {
        let byte = *bytes
            .get(*at)
            .ok_or_else(|| Error::Batch("truncated varint".to_owned()))?;
        *at += 1;
        let bits = u64::from(byte & 0x7f);
        if shift == 63 && bits > 1 {
            return Err(Error::Batch("varint overflows".to_owned()));
        }
        value |= bits << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(Error::Batch("varint overflows".to_owned()))
}

/// Just enough RLP to read the head of a singular batch.
struct Rlp<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl Rlp<'_> {
    fn byte(&mut self) -> Result<u8, Error> {
        let byte = *self
            .bytes
            .get(self.at)
            .ok_or_else(|| Error::Batch("truncated singular batch".to_owned()))?;
        self.at += 1;
        Ok(byte)
    }

    fn list_header(&mut self) -> Result<(), Error> {
        match self.byte()? {
            0xc0..=0xf7 => Ok(()),
            prefix @ 0xf8..=0xff => {
                self.at += usize::from(prefix - 0xf7);
                Ok(())
            }
            prefix => Err(Error::Batch(format!(
                "a singular batch is an RLP list, not prefix {prefix:#x}"
            ))),
        }
    }

    /// A byte string of at most 55 bytes, as every field read here is.
    fn string(&mut self) -> Result<&[u8], Error> {
        let (start, length) = match self.byte()? {
            0x00..=0x7f => (self.at - 1, 1),
            prefix @ 0x80..=0xb7 => (self.at, usize::from(prefix - 0x80)),
            prefix => {
                return Err(Error::Batch(format!(
                    "a singular batch field has prefix {prefix:#x}"
                )));
            }
        };
        let bytes = self
            .bytes
            .get(start..start + length)
            .ok_or_else(|| Error::Batch("truncated singular batch".to_owned()))?;
        self.at = start + length;
        Ok(bytes)
    }

    fn hash(&mut self) -> Result<B256, Error> {
        let bytes = self.string()?;
        B256::try_from(bytes).map_err(|_| Error::Batch(format!("a hash of {} bytes", bytes.len())))
    }

    fn uint(&mut self) -> Result<u64, Error> {
        let bytes = self.string()?;
        if bytes.len() > 8 || bytes.first() == Some(&0) && bytes.len() > 1 {
            return Err(Error::Batch("an integer that is not canonical".to_owned()));
        }
        Ok(bytes
            .iter()
            .fold(0_u64, |value, byte| value << 8 | u64::from(*byte)))
    }
}
