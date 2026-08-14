use std::{
    fs::{File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use flate2::{Compression as DeflateLevel, read::ZlibDecoder, write::ZlibEncoder};
use leani_primitives::{
    BLOCK_FRAME_SCHEMA_VERSION, BlockFrame, BlockHash, BlockNumber, BlockRange, CapabilitySet,
    ChainId, DurableKind, Finality, TrustModel,
};
use serde::{Deserialize, Serialize};
use snap::raw::{Decoder as SnappyDecoder, Encoder as SnappyEncoder};
use thiserror::Error;

const HEADER_MAGIC: &[u8; 8] = b"IDXRAW01";
const DIRECTORY_MAGIC: &[u8; 8] = b"IDXDIR01";
const TRAILER_MAGIC: &[u8; 8] = b"IDXEND01";
pub(crate) const FORMAT_VERSION: u16 = 1;
pub(crate) const FRAME_ENCODING_VERSION: u16 = BLOCK_FRAME_SCHEMA_VERSION;
const HEADER_LEN: usize = 80;
const HEADER_BYTES: u64 = 80;
const DIRECTORY_PREFIX_LEN: usize = 16;
const DIRECTORY_PREFIX_BYTES: u64 = 16;
const DIRECTORY_ENTRY_LEN: usize = 120;
const DIRECTORY_ENTRY_BYTES: u64 = 120;
const FOOTER_SUMMARY_LEN: usize = 128;
const FOOTER_SUMMARY_BYTES: u64 = 128;
const TRAILER_LEN: usize = 56;
const TRAILER_BYTES: u64 = 56;
const TRAILER_SEEK: i64 = 56;

/// Portable identifier used for a segment and its temporary files.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct SegmentId(String);

impl SegmentId {
    /// Validate a caller-provided stable identifier.
    ///
    /// # Errors
    ///
    /// Rejects empty, overlong, or path-like identifiers.
    pub fn new(value: impl Into<String>) -> Result<Self, SegmentError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 96
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        {
            return Err(SegmentError::InvalidId(value));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for SegmentId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

/// Opaque canonical identity for projection and filter semantics.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct MaterialShapeId(pub [u8; 32]);

impl MaterialShapeId {
    pub const COMPLETE_EXECUTION: Self = Self([0; 32]);
}

/// Verification guarantee under which a segment was admitted.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum VerificationClass {
    BestEffort = 0,
    TrustedDataset = 1,
    Cryptographic = 2,
}

impl VerificationClass {
    pub(crate) fn from_byte(value: u8) -> Result<Self, SegmentError> {
        match value {
            0 => Ok(Self::BestEffort),
            1 => Ok(Self::TrustedDataset),
            2 => Ok(Self::Cryptographic),
            other => Err(SegmentError::UnknownVerification(other)),
        }
    }
}

/// Per-record compression. Whole-file compression is intentionally absent so
/// one block can be read without decoding neighboring blocks.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum Compression {
    #[default]
    None = 0,
    Snappy = 1,
    Deflate = 2,
}

impl Compression {
    pub(crate) fn from_byte(value: u8) -> Result<Self, SegmentError> {
        match value {
            0 => Ok(Self::None),
            1 => Ok(Self::Snappy),
            2 => Ok(Self::Deflate),
            other => Err(SegmentError::UnknownCompression(other)),
        }
    }

    fn encode(self, input: &[u8]) -> Result<Vec<u8>, SegmentError> {
        match self {
            Self::None => Ok(input.to_vec()),
            Self::Snappy => SnappyEncoder::new()
                .compress_vec(input)
                .map_err(SegmentError::Snappy),
            Self::Deflate => {
                let mut encoder = ZlibEncoder::new(Vec::new(), DeflateLevel::fast());
                encoder.write_all(input)?;
                encoder.finish().map_err(SegmentError::Io)
            }
        }
    }

    fn decode(self, input: &[u8], expected_len: usize) -> Result<Vec<u8>, SegmentError> {
        let decoded = match self {
            Self::None => input.to_vec(),
            Self::Snappy => SnappyDecoder::new()
                .decompress_vec(input)
                .map_err(SegmentError::Snappy)?,
            Self::Deflate => {
                let mut decoder = ZlibDecoder::new(input);
                let mut output = Vec::with_capacity(expected_len);
                decoder.read_to_end(&mut output)?;
                output
            }
        };
        if decoded.len() != expected_len {
            return Err(SegmentError::DecodedLength {
                expected: expected_len,
                actual: decoded.len(),
            });
        }
        Ok(decoded)
    }
}

/// Immutable identity and guarantees shared by every frame in a segment.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SegmentDescriptor {
    pub chain_id: ChainId,
    pub range: BlockRange,
    pub material_shape: MaterialShapeId,
    pub present_capabilities: CapabilitySet,
    pub complete_capabilities: CapabilitySet,
    pub verification: VerificationClass,
    pub trust: TrustModel,
}

/// Hard limits enforced before any record is appended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentLimits {
    pub maximum_frame_logical_bytes: u64,
    pub maximum_segment_logical_bytes: u64,
    pub maximum_segment_physical_bytes: u64,
}

impl SegmentLimits {
    /// Validate one internally consistent non-zero limit set.
    ///
    /// # Errors
    ///
    /// Returns an error if any limit is zero or a frame may exceed the
    /// segment logical ceiling.
    pub const fn validate(self) -> Result<Self, SegmentError> {
        if self.maximum_frame_logical_bytes == 0
            || self.maximum_segment_logical_bytes == 0
            || self.maximum_segment_physical_bytes == 0
            || self.maximum_frame_logical_bytes > self.maximum_segment_logical_bytes
        {
            return Err(SegmentError::InvalidLimits);
        }
        Ok(self)
    }
}

/// Validated metadata for one closed immutable segment.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SegmentMetadata {
    pub id: SegmentId,
    pub descriptor: SegmentDescriptor,
    pub compression: Compression,
    pub logical_bytes: u64,
    pub physical_bytes: u64,
    pub first_parent_hash: BlockHash,
    pub last_hash: BlockHash,
    pub ordered_hash_digest: [u8; 32],
    pub records_checksum: [u8; 32],
    pub content_checksum: [u8; 32],
}

/// Result of one seekable block read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SegmentRead {
    pub frame: BlockFrame,
    pub stored_bytes_read: u64,
    pub logical_bytes_read: u64,
    pub records_decoded: u64,
}

#[derive(Clone, Copy, Debug)]
struct DirectoryEntry {
    block_number: BlockNumber,
    payload_offset: u64,
    stored_len: u32,
    logical_len: u32,
    block_hash: BlockHash,
    parent_hash: BlockHash,
    stored_checksum: [u8; 32],
}

impl DirectoryEntry {
    fn encode(self) -> [u8; DIRECTORY_ENTRY_LEN] {
        let mut output = [0; DIRECTORY_ENTRY_LEN];
        output[0..8].copy_from_slice(&self.block_number.0.to_be_bytes());
        output[8..16].copy_from_slice(&self.payload_offset.to_be_bytes());
        output[16..20].copy_from_slice(&self.stored_len.to_be_bytes());
        output[20..24].copy_from_slice(&self.logical_len.to_be_bytes());
        output[24..56].copy_from_slice(self.block_hash.as_array());
        output[56..88].copy_from_slice(self.parent_hash.as_array());
        output[88..120].copy_from_slice(&self.stored_checksum);
        output
    }

    fn decode(bytes: &[u8; DIRECTORY_ENTRY_LEN]) -> Self {
        let mut block_hash = [0; 32];
        block_hash.copy_from_slice(&bytes[24..56]);
        let mut parent_hash = [0; 32];
        parent_hash.copy_from_slice(&bytes[56..88]);
        let mut stored_checksum = [0; 32];
        stored_checksum.copy_from_slice(&bytes[88..120]);
        Self {
            block_number: BlockNumber(u64::from_be_bytes(bytes[0..8].try_into().unwrap())),
            payload_offset: u64::from_be_bytes(bytes[8..16].try_into().unwrap()),
            stored_len: u32::from_be_bytes(bytes[16..20].try_into().unwrap()),
            logical_len: u32::from_be_bytes(bytes[20..24].try_into().unwrap()),
            block_hash: BlockHash::new(block_hash),
            parent_hash: BlockHash::new(parent_hash),
            stored_checksum,
        }
    }
}

/// Streaming writer for one exact contiguous finalized range.
#[derive(Debug)]
pub struct SegmentWriter {
    id: SegmentId,
    descriptor: SegmentDescriptor,
    compression: Compression,
    limits: SegmentLimits,
    partial_path: PathBuf,
    index_path: PathBuf,
    output: BufWriter<File>,
    index: BufWriter<File>,
    content_hasher: blake3::Hasher,
    records_hasher: blake3::Hasher,
    ordered_hash_hasher: blake3::Hasher,
    count: u64,
    logical_bytes: u64,
    stored_bytes: u64,
    first_parent_hash: Option<BlockHash>,
    last_hash: Option<BlockHash>,
    failed: bool,
}

impl SegmentWriter {
    /// Create unique partial data and seek-index files.
    ///
    /// # Errors
    ///
    /// Rejects invalid limits/descriptors and existing paths.
    pub fn create(
        partial_path: impl Into<PathBuf>,
        index_path: impl Into<PathBuf>,
        id: SegmentId,
        descriptor: SegmentDescriptor,
        compression: Compression,
        limits: SegmentLimits,
    ) -> Result<Self, SegmentError> {
        limits.validate()?;
        validate_descriptor(&descriptor)?;
        let partial_path = partial_path.into();
        let index_path = index_path.into();
        let output_file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&partial_path)?;
        let index_file = match OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&index_path)
        {
            Ok(file) => file,
            Err(error) => {
                let _ = std::fs::remove_file(&partial_path);
                return Err(error.into());
            }
        };
        let header = encode_header(&descriptor, compression);
        let mut output = BufWriter::new(output_file);
        if let Err(error) = output.write_all(&header) {
            let _ = std::fs::remove_file(&partial_path);
            let _ = std::fs::remove_file(&index_path);
            return Err(error.into());
        }
        let mut content_hasher = blake3::Hasher::new();
        content_hasher.update(&header);
        Ok(Self {
            id,
            descriptor,
            compression,
            limits,
            partial_path,
            index_path,
            output,
            index: BufWriter::new(index_file),
            content_hasher,
            records_hasher: blake3::Hasher::new(),
            ordered_hash_hasher: blake3::Hasher::new(),
            count: 0,
            logical_bytes: 0,
            stored_bytes: 0,
            first_parent_hash: None,
            last_hash: None,
            failed: false,
        })
    }

    /// Append exactly one frame while retaining only that frame and its
    /// compressed representation in memory.
    ///
    /// # Errors
    ///
    /// Rejects wrong-chain, non-finalized, out-of-order, discontinuous,
    /// wrong-shape, oversized, or invalid frames.
    pub fn append(&mut self, frame: &BlockFrame) -> Result<(), SegmentError> {
        if self.failed {
            return Err(SegmentError::WriterPoisoned);
        }
        if let Err(error) = self.validate_frame(frame) {
            self.failed = true;
            return Err(error);
        }
        let logical = leani_primitives::durable::encode(
            DurableKind::BlockFrame,
            FRAME_ENCODING_VERSION,
            frame,
        )?;
        let logical_len =
            u32::try_from(logical.len()).map_err(|_| SegmentError::FrameOversized {
                limit: self.limits.maximum_frame_logical_bytes,
                observed: u64::MAX,
            })?;
        if u64::from(logical_len) > self.limits.maximum_frame_logical_bytes {
            self.failed = true;
            return Err(SegmentError::FrameOversized {
                limit: self.limits.maximum_frame_logical_bytes,
                observed: u64::from(logical_len),
            });
        }
        let projected_logical = self
            .logical_bytes
            .checked_add(u64::from(logical_len))
            .ok_or(SegmentError::ArithmeticOverflow)?;
        if projected_logical > self.limits.maximum_segment_logical_bytes {
            self.failed = true;
            return Err(SegmentError::SegmentLogicalBudget {
                limit: self.limits.maximum_segment_logical_bytes,
                observed: projected_logical,
            });
        }
        let stored = self.compression.encode(&logical)?;
        let stored_len =
            u32::try_from(stored.len()).map_err(|_| SegmentError::SegmentPhysicalBudget {
                limit: self.limits.maximum_segment_physical_bytes,
                observed: u64::MAX,
            })?;
        let projected_stored = self
            .stored_bytes
            .checked_add(u64::from(stored_len))
            .ok_or(SegmentError::ArithmeticOverflow)?;
        let projected_count = self
            .count
            .checked_add(1)
            .ok_or(SegmentError::ArithmeticOverflow)?;
        let projected_physical = closed_file_len(projected_count, projected_stored)?;
        if projected_physical > self.limits.maximum_segment_physical_bytes {
            self.failed = true;
            return Err(SegmentError::SegmentPhysicalBudget {
                limit: self.limits.maximum_segment_physical_bytes,
                observed: projected_physical,
            });
        }

        let payload_offset = HEADER_BYTES
            .checked_add(self.stored_bytes)
            .ok_or(SegmentError::ArithmeticOverflow)?;
        let stored_checksum = *blake3::hash(&stored).as_bytes();
        let entry = DirectoryEntry {
            block_number: frame.block.number,
            payload_offset,
            stored_len,
            logical_len,
            block_hash: frame.block.hash,
            parent_hash: frame.block.parent_hash,
            stored_checksum,
        };
        if let Err(error) = self.output.write_all(&stored) {
            self.failed = true;
            return Err(error.into());
        }
        if let Err(error) = self.index.write_all(&entry.encode()) {
            self.failed = true;
            return Err(error.into());
        }
        self.content_hasher.update(&stored);
        self.records_hasher.update(&stored);
        self.ordered_hash_hasher.update(frame.block.hash.as_array());
        self.first_parent_hash
            .get_or_insert(frame.block.parent_hash);
        self.last_hash = Some(frame.block.hash);
        self.count = projected_count;
        self.logical_bytes = projected_logical;
        self.stored_bytes = projected_stored;
        Ok(())
    }

    #[must_use]
    pub const fn descriptor(&self) -> &SegmentDescriptor {
        &self.descriptor
    }

    /// Close, fsync, and atomically publish the segment.
    ///
    /// # Errors
    ///
    /// Fails when the exact range was not written or publication cannot be
    /// made durable. No catalog state is changed by this operation.
    pub fn finish(mut self, final_path: impl AsRef<Path>) -> Result<SegmentMetadata, SegmentError> {
        if self.failed {
            return Err(SegmentError::WriterPoisoned);
        }
        let expected_count = self.descriptor.range.len();
        if self.count != expected_count {
            return Err(SegmentError::IncompleteRange {
                expected: expected_count,
                actual: self.count,
            });
        }
        self.output.flush()?;
        self.index.flush()?;
        self.index.get_ref().sync_all()?;

        let directory_offset = HEADER_BYTES
            .checked_add(self.stored_bytes)
            .ok_or(SegmentError::ArithmeticOverflow)?;
        let mut directory_prefix = [0; DIRECTORY_PREFIX_LEN];
        directory_prefix[..8].copy_from_slice(DIRECTORY_MAGIC);
        directory_prefix[8..].copy_from_slice(&self.count.to_be_bytes());
        write_hashed(
            &mut self.output,
            &mut self.content_hasher,
            &directory_prefix,
        )?;
        let mut index_reader = BufReader::new(File::open(&self.index_path)?);
        let copied = copy_hashed(
            &mut index_reader,
            &mut self.output,
            &mut self.content_hasher,
        )?;
        let expected_index_len = self
            .count
            .checked_mul(DIRECTORY_ENTRY_BYTES)
            .ok_or(SegmentError::ArithmeticOverflow)?;
        if copied != expected_index_len {
            return Err(SegmentError::IndexLength {
                expected: expected_index_len,
                actual: copied,
            });
        }
        let first_parent_hash = self
            .first_parent_hash
            .ok_or(SegmentError::IncompleteRange {
                expected: expected_count,
                actual: 0,
            })?;
        let last_hash = self.last_hash.ok_or(SegmentError::IncompleteRange {
            expected: expected_count,
            actual: 0,
        })?;
        let ordered_hash_digest = *self.ordered_hash_hasher.finalize().as_bytes();
        let records_checksum = *self.records_hasher.finalize().as_bytes();
        let mut summary = [0; FOOTER_SUMMARY_LEN];
        summary[0..32].copy_from_slice(first_parent_hash.as_array());
        summary[32..64].copy_from_slice(last_hash.as_array());
        summary[64..96].copy_from_slice(&ordered_hash_digest);
        summary[96..128].copy_from_slice(&records_checksum);
        write_hashed(&mut self.output, &mut self.content_hasher, &summary)?;

        let directory_len = DIRECTORY_PREFIX_BYTES
            .checked_add(expected_index_len)
            .and_then(|value| value.checked_add(FOOTER_SUMMARY_BYTES))
            .ok_or(SegmentError::ArithmeticOverflow)?;
        let content_checksum = *self.content_hasher.finalize().as_bytes();
        let mut trailer = [0; TRAILER_LEN];
        trailer[0..8].copy_from_slice(&directory_offset.to_be_bytes());
        trailer[8..16].copy_from_slice(&directory_len.to_be_bytes());
        trailer[16..48].copy_from_slice(&content_checksum);
        trailer[48..56].copy_from_slice(TRAILER_MAGIC);
        self.output.write_all(&trailer)?;
        self.output.flush()?;
        self.output.get_ref().sync_all()?;

        let physical_bytes = closed_file_len(self.count, self.stored_bytes)?;
        let final_path = final_path.as_ref();
        if final_path.exists() {
            return Err(SegmentError::FinalPathExists(final_path.to_path_buf()));
        }
        std::fs::rename(&self.partial_path, final_path)?;
        sync_parent(final_path)?;
        std::fs::remove_file(&self.index_path)?;
        sync_parent(&self.index_path)?;

        Ok(SegmentMetadata {
            id: self.id,
            descriptor: self.descriptor,
            compression: self.compression,
            logical_bytes: self.logical_bytes,
            physical_bytes,
            first_parent_hash,
            last_hash,
            ordered_hash_digest,
            records_checksum,
            content_checksum,
        })
    }

    fn validate_frame(&self, frame: &BlockFrame) -> Result<(), SegmentError> {
        frame.validate_shape().map_err(SegmentError::InvalidFrame)?;
        if frame.chain_id != self.descriptor.chain_id {
            return Err(SegmentError::ChainMismatch {
                expected: self.descriptor.chain_id,
                actual: frame.chain_id,
            });
        }
        if frame.finality != Finality::Finalized {
            return Err(SegmentError::NonFinalized(frame.block.number));
        }
        let expected_number = BlockNumber(
            self.descriptor
                .range
                .start()
                .0
                .checked_add(self.count)
                .ok_or(SegmentError::ArithmeticOverflow)?,
        );
        if frame.block.number != expected_number {
            return Err(SegmentError::Ordering {
                expected: expected_number,
                actual: frame.block.number,
            });
        }
        if frame.block.number.0 > self.descriptor.range.end().0 {
            return Err(SegmentError::Ordering {
                expected: self.descriptor.range.end(),
                actual: frame.block.number,
            });
        }
        if let Some(last_hash) = self.last_hash
            && frame.block.parent_hash != last_hash
        {
            return Err(SegmentError::ParentMismatch {
                block: frame.block.number,
                expected: last_hash,
                actual: frame.block.parent_hash,
            });
        }
        let capabilities = frame.capabilities();
        if capabilities.present != self.descriptor.present_capabilities
            || capabilities.complete != self.descriptor.complete_capabilities
        {
            return Err(SegmentError::CapabilityMismatch);
        }
        Ok(())
    }
}

/// Validated seekable reader. Opening verifies the complete file checksum and
/// every directory/anchor invariant without decoding frame payloads.
#[derive(Debug)]
pub struct SegmentReader {
    file: File,
    metadata: SegmentMetadata,
    directory_offset: u64,
}

impl SegmentReader {
    /// Open and fully validate a closed segment.
    ///
    /// # Errors
    ///
    /// Any malformed, truncated, corrupt, discontinuous, or unsupported file
    /// is rejected before it can satisfy coverage.
    #[allow(clippy::too_many_lines)]
    pub fn open(path: impl AsRef<Path>, id: SegmentId) -> Result<Self, SegmentError> {
        let mut file = OpenOptions::new().read(true).open(path.as_ref())?;
        let file_len = file.metadata()?.len();
        if file_len < HEADER_BYTES + DIRECTORY_PREFIX_BYTES + FOOTER_SUMMARY_BYTES + TRAILER_BYTES {
            return Err(SegmentError::Truncated);
        }
        let mut header = [0; HEADER_LEN];
        file.read_exact(&mut header)?;
        let (descriptor, compression) = decode_header(&header)?;

        file.seek(SeekFrom::End(-TRAILER_SEEK))?;
        let mut trailer = [0; TRAILER_LEN];
        file.read_exact(&mut trailer)?;
        if &trailer[48..56] != TRAILER_MAGIC {
            return Err(SegmentError::TrailerMagic);
        }
        let directory_offset = decode_u64(&trailer[0..8])?;
        let directory_len = decode_u64(&trailer[8..16])?;
        let mut expected_content_checksum = [0; 32];
        expected_content_checksum.copy_from_slice(&trailer[16..48]);
        let expected_trailer_offset = directory_offset
            .checked_add(directory_len)
            .ok_or(SegmentError::ArithmeticOverflow)?;
        if expected_trailer_offset
            .checked_add(TRAILER_BYTES)
            .ok_or(SegmentError::ArithmeticOverflow)?
            != file_len
        {
            return Err(SegmentError::DirectoryBounds);
        }
        let (actual_content_checksum, actual_records_checksum) =
            hash_content_and_records(&mut file, directory_offset, expected_trailer_offset)?;
        if actual_content_checksum != expected_content_checksum {
            return Err(SegmentError::ContentChecksum);
        }

        file.seek(SeekFrom::Start(directory_offset))?;
        let mut directory_prefix = [0; DIRECTORY_PREFIX_LEN];
        file.read_exact(&mut directory_prefix)?;
        if &directory_prefix[..8] != DIRECTORY_MAGIC {
            return Err(SegmentError::DirectoryMagic);
        }
        let count = decode_u64(&directory_prefix[8..16])?;
        if count != descriptor.range.len() {
            return Err(SegmentError::IncompleteRange {
                expected: descriptor.range.len(),
                actual: count,
            });
        }
        let expected_directory_len = DIRECTORY_PREFIX_BYTES
            .checked_add(
                count
                    .checked_mul(DIRECTORY_ENTRY_BYTES)
                    .ok_or(SegmentError::ArithmeticOverflow)?,
            )
            .and_then(|value| value.checked_add(FOOTER_SUMMARY_BYTES))
            .ok_or(SegmentError::ArithmeticOverflow)?;
        if directory_len != expected_directory_len {
            return Err(SegmentError::DirectoryLength {
                expected: expected_directory_len,
                actual: directory_len,
            });
        }

        let mut ordered_hash_hasher = blake3::Hasher::new();
        let mut previous_hash = None;
        let mut logical_bytes = 0_u64;
        let mut stored_bytes = 0_u64;
        let mut first_parent_hash = None;
        let mut last_hash = None;
        for index in 0..count {
            let entry = read_entry_at(&mut file, directory_offset, index)?;
            let expected_number = BlockNumber(
                descriptor
                    .range
                    .start()
                    .0
                    .checked_add(index)
                    .ok_or(SegmentError::ArithmeticOverflow)?,
            );
            if entry.block_number != expected_number {
                return Err(SegmentError::Ordering {
                    expected: expected_number,
                    actual: entry.block_number,
                });
            }
            let expected_offset = HEADER_BYTES
                .checked_add(stored_bytes)
                .ok_or(SegmentError::ArithmeticOverflow)?;
            if entry.payload_offset != expected_offset
                || entry
                    .payload_offset
                    .checked_add(u64::from(entry.stored_len))
                    .ok_or(SegmentError::ArithmeticOverflow)?
                    > directory_offset
            {
                return Err(SegmentError::RecordBounds(entry.block_number));
            }
            if let Some(previous) = previous_hash
                && entry.parent_hash != previous
            {
                return Err(SegmentError::ParentMismatch {
                    block: entry.block_number,
                    expected: previous,
                    actual: entry.parent_hash,
                });
            }
            first_parent_hash.get_or_insert(entry.parent_hash);
            previous_hash = Some(entry.block_hash);
            last_hash = Some(entry.block_hash);
            ordered_hash_hasher.update(entry.block_hash.as_array());
            logical_bytes = logical_bytes
                .checked_add(u64::from(entry.logical_len))
                .ok_or(SegmentError::ArithmeticOverflow)?;
            stored_bytes = stored_bytes
                .checked_add(u64::from(entry.stored_len))
                .ok_or(SegmentError::ArithmeticOverflow)?;
        }
        if HEADER_BYTES
            .checked_add(stored_bytes)
            .ok_or(SegmentError::ArithmeticOverflow)?
            != directory_offset
        {
            return Err(SegmentError::DirectoryBounds);
        }

        let summary_offset = directory_offset
            .checked_add(DIRECTORY_PREFIX_BYTES)
            .and_then(|value| value.checked_add(count.checked_mul(DIRECTORY_ENTRY_BYTES)?))
            .ok_or(SegmentError::ArithmeticOverflow)?;
        file.seek(SeekFrom::Start(summary_offset))?;
        let mut summary = [0; FOOTER_SUMMARY_LEN];
        file.read_exact(&mut summary)?;
        let first_parent_hash = first_parent_hash.ok_or(SegmentError::Truncated)?;
        let last_hash = last_hash.ok_or(SegmentError::Truncated)?;
        if summary[0..32] != first_parent_hash.as_array()[..]
            || summary[32..64] != last_hash.as_array()[..]
        {
            return Err(SegmentError::AnchorMismatch);
        }
        let ordered_hash_digest = *ordered_hash_hasher.finalize().as_bytes();
        if summary[64..96] != ordered_hash_digest {
            return Err(SegmentError::OrderedHashDigest);
        }
        let mut records_checksum = [0; 32];
        records_checksum.copy_from_slice(&summary[96..128]);
        if actual_records_checksum != records_checksum {
            return Err(SegmentError::RecordsChecksum);
        }

        Ok(Self {
            file,
            metadata: SegmentMetadata {
                id,
                descriptor,
                compression,
                logical_bytes,
                physical_bytes: file_len,
                first_parent_hash,
                last_hash,
                ordered_hash_digest,
                records_checksum,
                content_checksum: expected_content_checksum,
            },
            directory_offset,
        })
    }

    #[must_use]
    pub const fn metadata(&self) -> &SegmentMetadata {
        &self.metadata
    }

    /// Decode one block using its fixed-width seek entry only.
    ///
    /// # Errors
    ///
    /// Rejects out-of-range blocks and any record-level corruption.
    pub fn read_block(&mut self, block: BlockNumber) -> Result<SegmentRead, SegmentError> {
        if !self.metadata.descriptor.range.contains(block) {
            return Err(SegmentError::BlockOutOfRange {
                block,
                range: self.metadata.descriptor.range,
            });
        }
        let index = block.0 - self.metadata.descriptor.range.start().0;
        let entry = read_entry_at(&mut self.file, self.directory_offset, index)?;
        self.file.seek(SeekFrom::Start(entry.payload_offset))?;
        let mut stored = vec![0; entry.stored_len as usize];
        self.file.read_exact(&mut stored)?;
        if blake3::hash(&stored).as_bytes() != &entry.stored_checksum {
            return Err(SegmentError::RecordChecksum(block));
        }
        let decoded = self
            .metadata
            .compression
            .decode(&stored, entry.logical_len as usize)?;
        let frame: BlockFrame = leani_primitives::durable::decode(
            DurableKind::BlockFrame,
            FRAME_ENCODING_VERSION,
            &decoded,
        )?;
        if frame.chain_id != self.metadata.descriptor.chain_id
            || frame.block.number != block
            || frame.block.hash != entry.block_hash
            || frame.block.parent_hash != entry.parent_hash
            || frame.finality != Finality::Finalized
        {
            return Err(SegmentError::DecodedIdentity(block));
        }
        let capabilities = frame.capabilities();
        if capabilities.present != self.metadata.descriptor.present_capabilities
            || capabilities.complete != self.metadata.descriptor.complete_capabilities
        {
            return Err(SegmentError::CapabilityMismatch);
        }
        frame.validate_shape().map_err(SegmentError::InvalidFrame)?;
        Ok(SegmentRead {
            frame,
            stored_bytes_read: u64::from(entry.stored_len),
            logical_bytes_read: u64::from(entry.logical_len),
            records_decoded: 1,
        })
    }
}

fn validate_descriptor(descriptor: &SegmentDescriptor) -> Result<(), SegmentError> {
    if descriptor.chain_id.0 == 0 {
        return Err(SegmentError::InvalidDescriptor("chain ID must be non-zero"));
    }
    if !descriptor
        .present_capabilities
        .contains_all(descriptor.complete_capabilities)
    {
        return Err(SegmentError::InvalidDescriptor(
            "complete capabilities must also be present",
        ));
    }
    Ok(())
}

fn decode_u64(bytes: &[u8]) -> Result<u64, SegmentError> {
    let encoded: [u8; 8] = bytes.try_into().map_err(|_| SegmentError::Truncated)?;
    Ok(u64::from_be_bytes(encoded))
}

fn decode_trust(value: u8) -> Result<TrustModel, SegmentError> {
    match value {
        0 => Ok(TrustModel::Untrusted),
        1 => Ok(TrustModel::TrustedManifest),
        2 => Ok(TrustModel::TrustedDataset),
        3 => Ok(TrustModel::ProtocolVerified),
        other => Err(SegmentError::UnknownTrust(other)),
    }
}

fn encode_header(descriptor: &SegmentDescriptor, compression: Compression) -> [u8; 80] {
    let mut output = [0; 80];
    output[0..8].copy_from_slice(HEADER_MAGIC);
    output[8..10].copy_from_slice(&FORMAT_VERSION.to_be_bytes());
    output[10..12].copy_from_slice(&FRAME_ENCODING_VERSION.to_be_bytes());
    output[12] = compression as u8;
    output[13] = Finality::Finalized as u8;
    output[14] = descriptor.verification as u8;
    output[15] = descriptor.trust as u8;
    output[16..24].copy_from_slice(&descriptor.chain_id.0.to_be_bytes());
    output[24..32].copy_from_slice(&descriptor.range.start().0.to_be_bytes());
    output[32..40].copy_from_slice(&descriptor.range.end().0.to_be_bytes());
    output[40..72].copy_from_slice(&descriptor.material_shape.0);
    output[72..74].copy_from_slice(&descriptor.present_capabilities.bits().to_be_bytes());
    output[74..76].copy_from_slice(&descriptor.complete_capabilities.bits().to_be_bytes());
    output
}

fn decode_header(bytes: &[u8; 80]) -> Result<(SegmentDescriptor, Compression), SegmentError> {
    if &bytes[0..8] != HEADER_MAGIC {
        return Err(SegmentError::HeaderMagic);
    }
    let format = u16::from_be_bytes(bytes[8..10].try_into().unwrap());
    if format != FORMAT_VERSION {
        return Err(SegmentError::FormatVersion(format));
    }
    let frame_encoding = u16::from_be_bytes(bytes[10..12].try_into().unwrap());
    if frame_encoding != FRAME_ENCODING_VERSION {
        return Err(SegmentError::FrameEncodingVersion(frame_encoding));
    }
    let compression = Compression::from_byte(bytes[12])?;
    if bytes[13] != Finality::Finalized as u8 {
        return Err(SegmentError::NonFinalizedEncoding(bytes[13]));
    }
    let verification = VerificationClass::from_byte(bytes[14])?;
    let trust = decode_trust(bytes[15])?;
    if bytes[76..80] != [0; 4] {
        return Err(SegmentError::Reserved);
    }
    let chain_id = ChainId(u64::from_be_bytes(bytes[16..24].try_into().unwrap()));
    let range = BlockRange::new(
        BlockNumber(u64::from_be_bytes(bytes[24..32].try_into().unwrap())),
        BlockNumber(u64::from_be_bytes(bytes[32..40].try_into().unwrap())),
    )
    .map_err(|_| SegmentError::InvalidDescriptor("invalid block range"))?;
    let mut material_shape = [0; 32];
    material_shape.copy_from_slice(&bytes[40..72]);
    let present = CapabilitySet::from_bits(u16::from_be_bytes(bytes[72..74].try_into().unwrap()))
        .ok_or(SegmentError::UnknownCapabilities)?;
    let complete = CapabilitySet::from_bits(u16::from_be_bytes(bytes[74..76].try_into().unwrap()))
        .ok_or(SegmentError::UnknownCapabilities)?;
    let descriptor = SegmentDescriptor {
        chain_id,
        range,
        material_shape: MaterialShapeId(material_shape),
        present_capabilities: present,
        complete_capabilities: complete,
        verification,
        trust,
    };
    validate_descriptor(&descriptor)?;
    Ok((descriptor, compression))
}

fn read_entry_at(
    file: &mut File,
    directory_offset: u64,
    index: u64,
) -> Result<DirectoryEntry, SegmentError> {
    let offset = directory_offset
        .checked_add(DIRECTORY_PREFIX_BYTES)
        .and_then(|value| value.checked_add(index.checked_mul(DIRECTORY_ENTRY_BYTES)?))
        .ok_or(SegmentError::ArithmeticOverflow)?;
    file.seek(SeekFrom::Start(offset))?;
    let mut bytes = [0; DIRECTORY_ENTRY_LEN];
    file.read_exact(&mut bytes)?;
    Ok(DirectoryEntry::decode(&bytes))
}

fn closed_file_len(count: u64, stored_bytes: u64) -> Result<u64, SegmentError> {
    HEADER_BYTES
        .checked_add(stored_bytes)
        .and_then(|value| value.checked_add(DIRECTORY_PREFIX_BYTES))
        .and_then(|value| value.checked_add(count.checked_mul(DIRECTORY_ENTRY_BYTES)?))
        .and_then(|value| value.checked_add(FOOTER_SUMMARY_BYTES))
        .and_then(|value| value.checked_add(TRAILER_BYTES))
        .ok_or(SegmentError::ArithmeticOverflow)
}

fn write_hashed(
    output: &mut impl Write,
    hasher: &mut blake3::Hasher,
    bytes: &[u8],
) -> Result<(), SegmentError> {
    output.write_all(bytes)?;
    hasher.update(bytes);
    Ok(())
}

fn copy_hashed(
    input: &mut impl Read,
    output: &mut impl Write,
    hasher: &mut blake3::Hasher,
) -> Result<u64, SegmentError> {
    let mut buffer = vec![0; 64 * 1024];
    let mut copied = 0_u64;
    loop {
        let read = input.read(&mut buffer)?;
        if read == 0 {
            return Ok(copied);
        }
        output.write_all(&buffer[..read])?;
        hasher.update(&buffer[..read]);
        copied = copied
            .checked_add(u64::try_from(read).map_err(|_| SegmentError::ArithmeticOverflow)?)
            .ok_or(SegmentError::ArithmeticOverflow)?;
    }
}

fn hash_content_and_records(
    file: &mut File,
    directory_offset: u64,
    content_length: u64,
) -> Result<([u8; 32], [u8; 32]), SegmentError> {
    file.seek(SeekFrom::Start(0))?;
    let mut position = 0_u64;
    let mut buffer = vec![0; 64 * 1024];
    let mut content_hasher = blake3::Hasher::new();
    let mut records_hasher = blake3::Hasher::new();
    while position < content_length {
        let requested = usize::try_from((content_length - position).min(buffer.len() as u64))
            .map_err(|_| SegmentError::ArithmeticOverflow)?;
        let read = file.read(&mut buffer[..requested])?;
        if read == 0 {
            return Err(SegmentError::Truncated);
        }
        content_hasher.update(&buffer[..read]);
        let read_u64 = u64::try_from(read).map_err(|_| SegmentError::ArithmeticOverflow)?;
        let chunk_end = position
            .checked_add(read_u64)
            .ok_or(SegmentError::ArithmeticOverflow)?;
        let records_start = position.max(HEADER_BYTES);
        let records_end = chunk_end.min(directory_offset);
        if records_start < records_end {
            let local_start = usize::try_from(records_start - position)
                .map_err(|_| SegmentError::ArithmeticOverflow)?;
            let local_end = usize::try_from(records_end - position)
                .map_err(|_| SegmentError::ArithmeticOverflow)?;
            records_hasher.update(&buffer[local_start..local_end]);
        }
        position = chunk_end;
    }
    Ok((
        *content_hasher.finalize().as_bytes(),
        *records_hasher.finalize().as_bytes(),
    ))
}

fn sync_parent(path: &Path) -> Result<(), SegmentError> {
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

#[derive(Debug, Error)]
pub enum SegmentError {
    #[error("invalid segment ID `{0}`")]
    InvalidId(String),
    #[error("invalid segment descriptor: {0}")]
    InvalidDescriptor(&'static str),
    #[error("segment limits must be non-zero and frame <= segment logical limit")]
    InvalidLimits,
    #[error("segment writer is poisoned after an earlier append failure")]
    WriterPoisoned,
    #[error("invalid frame: {0}")]
    InvalidFrame(&'static str),
    #[error("segment chain mismatch: expected {expected}, received {actual}")]
    ChainMismatch { expected: ChainId, actual: ChainId },
    #[error("block {0} is not finalized")]
    NonFinalized(BlockNumber),
    #[error("block order mismatch: expected {expected}, received {actual}")]
    Ordering {
        expected: BlockNumber,
        actual: BlockNumber,
    },
    #[error("parent mismatch at block {block}: expected {expected}, received {actual}")]
    ParentMismatch {
        block: BlockNumber,
        expected: BlockHash,
        actual: BlockHash,
    },
    #[error("frame capabilities do not match the segment material shape")]
    CapabilityMismatch,
    #[error("single frame exceeds logical limit {limit} bytes (observed {observed})")]
    FrameOversized { limit: u64, observed: u64 },
    #[error("segment exceeds logical limit {limit} bytes (observed {observed})")]
    SegmentLogicalBudget { limit: u64, observed: u64 },
    #[error("segment exceeds physical limit {limit} bytes (observed {observed})")]
    SegmentPhysicalBudget { limit: u64, observed: u64 },
    #[error("segment is incomplete: expected {expected} frames, received {actual}")]
    IncompleteRange { expected: u64, actual: u64 },
    #[error("temporary seek index length mismatch: expected {expected}, observed {actual}")]
    IndexLength { expected: u64, actual: u64 },
    #[error("closed segment path already exists: {0}")]
    FinalPathExists(PathBuf),
    #[error("segment is truncated")]
    Truncated,
    #[error("invalid segment header magic")]
    HeaderMagic,
    #[error("unsupported segment format version {0}")]
    FormatVersion(u16),
    #[error("unsupported frame encoding version {0}")]
    FrameEncodingVersion(u16),
    #[error("unknown compression codec {0}")]
    UnknownCompression(u8),
    #[error("unknown verification class {0}")]
    UnknownVerification(u8),
    #[error("unknown trust model {0}")]
    UnknownTrust(u8),
    #[error("raw segments must encode finalized data, received finality tag {0}")]
    NonFinalizedEncoding(u8),
    #[error("segment reserved bytes are non-zero")]
    Reserved,
    #[error("segment contains unknown capability bits")]
    UnknownCapabilities,
    #[error("invalid segment trailer magic")]
    TrailerMagic,
    #[error("invalid segment directory magic")]
    DirectoryMagic,
    #[error("segment directory lies outside file bounds")]
    DirectoryBounds,
    #[error("segment directory length mismatch: expected {expected}, observed {actual}")]
    DirectoryLength { expected: u64, actual: u64 },
    #[error("record for block {0} lies outside the payload area")]
    RecordBounds(BlockNumber),
    #[error("segment first/last anchors do not match its seek directory")]
    AnchorMismatch,
    #[error("segment ordered canonical hash digest mismatch")]
    OrderedHashDigest,
    #[error("segment records checksum mismatch")]
    RecordsChecksum,
    #[error("segment content checksum mismatch")]
    ContentChecksum,
    #[error("record checksum mismatch for block {0}")]
    RecordChecksum(BlockNumber),
    #[error("decoded record length mismatch: expected {expected}, observed {actual}")]
    DecodedLength { expected: usize, actual: usize },
    #[error("decoded record identity mismatch for block {0}")]
    DecodedIdentity(BlockNumber),
    #[error("block {block} is outside segment range {range:?}")]
    BlockOutOfRange {
        block: BlockNumber,
        range: BlockRange,
    },
    #[error("segment size arithmetic overflow")]
    ArithmeticOverflow,
    #[error("segment I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("durable frame encoding failed: {0}")]
    Durable(#[from] leani_primitives::DurableError),
    #[error("snappy codec failed: {0}")]
    Snappy(snap::Error),
}

#[cfg(test)]
mod tests {
    use std::io::{Seek, SeekFrom, Write};

    use leani_primitives::{BlockHash, BlockNumber, BlockRange, Finality};
    use leani_testkit::{GeneratedHistorySource, SyntheticCorpusKind, fixture_frame};
    use tempfile::tempdir;

    use super::*;

    fn frames(start: u64, end: u64) -> Vec<BlockFrame> {
        let mut parent = BlockHash::new([0x44; 32]);
        (start..=end)
            .map(|number| {
                let frame = fixture_frame(number, parent);
                parent = frame.block.hash;
                frame
            })
            .collect()
    }

    fn descriptor(frames: &[BlockFrame]) -> SegmentDescriptor {
        let capabilities = frames[0].capabilities();
        SegmentDescriptor {
            chain_id: frames[0].chain_id,
            range: BlockRange::new(
                frames[0].block.number,
                frames.last().expect("non-empty fixture").block.number,
            )
            .expect("ordered fixture"),
            material_shape: MaterialShapeId([0x33; 32]),
            present_capabilities: capabilities.present,
            complete_capabilities: capabilities.complete,
            verification: VerificationClass::TrustedDataset,
            trust: TrustModel::TrustedDataset,
        }
    }

    fn limits() -> SegmentLimits {
        SegmentLimits {
            maximum_frame_logical_bytes: 1024 * 1024,
            maximum_segment_logical_bytes: 16 * 1024 * 1024,
            maximum_segment_physical_bytes: 16 * 1024 * 1024,
        }
    }

    fn write_segment(
        directory: &Path,
        id: &str,
        frames: &[BlockFrame],
        compression: Compression,
    ) -> (PathBuf, SegmentMetadata) {
        let id = SegmentId::new(id).expect("valid ID");
        let partial = directory.join(format!("{}.partial", id.as_str()));
        let index = directory.join(format!("{}.idxpartial", id.as_str()));
        let final_path = directory.join(format!("{}.idxraw", id.as_str()));
        let mut writer = SegmentWriter::create(
            partial,
            index,
            id,
            descriptor(frames),
            compression,
            limits(),
        )
        .expect("create writer");
        for frame in frames {
            writer.append(frame).expect("append frame");
        }
        let metadata = writer.finish(&final_path).expect("close segment");
        (final_path, metadata)
    }

    #[test]
    fn every_record_codec_round_trips_and_reads_one_record() {
        for compression in [Compression::None, Compression::Snappy, Compression::Deflate] {
            for count in [1_u64, 2, 17, 129] {
                let directory = tempdir().expect("temporary directory");
                let expected = frames(100, 99 + count);
                let (path, written) = write_segment(
                    directory.path(),
                    &format!("roundtrip-{}-{count}", compression as u8),
                    &expected,
                    compression,
                );
                let mut reader =
                    SegmentReader::open(&path, written.id.clone()).expect("validated segment");
                assert_eq!(reader.metadata(), &written);
                for index in [0, count / 2, count - 1] {
                    let read = reader
                        .read_block(BlockNumber(100 + index))
                        .expect("seek one frame");
                    assert_eq!(
                        read.frame,
                        expected[usize::try_from(index).expect("fixture index fits usize")]
                    );
                    assert_eq!(read.records_decoded, 1);
                    assert!(read.stored_bytes_read < written.physical_bytes);
                }
            }
        }
    }

    #[test]
    fn writer_rejects_non_finalized_discontinuous_and_incomplete_ranges() {
        let directory = tempdir().expect("temporary directory");
        let expected = frames(10, 12);
        let mut optimistic = expected[0].clone();
        optimistic.finality = Finality::Optimistic;
        let mut writer = SegmentWriter::create(
            directory.path().join("finality.partial"),
            directory.path().join("finality.idxpartial"),
            SegmentId::new("finality").expect("ID"),
            descriptor(&expected),
            Compression::None,
            limits(),
        )
        .expect("writer");
        assert!(matches!(
            writer.append(&optimistic),
            Err(SegmentError::NonFinalized(BlockNumber(10)))
        ));

        let mut discontinuous = expected.clone();
        discontinuous[1].block.parent_hash = BlockHash::new([0x99; 32]);
        let mut writer = SegmentWriter::create(
            directory.path().join("parents.partial"),
            directory.path().join("parents.idxpartial"),
            SegmentId::new("parents").expect("ID"),
            descriptor(&expected),
            Compression::None,
            limits(),
        )
        .expect("writer");
        writer.append(&discontinuous[0]).expect("first frame");
        assert!(matches!(
            writer.append(&discontinuous[1]),
            Err(SegmentError::ParentMismatch { .. })
        ));

        let mut writer = SegmentWriter::create(
            directory.path().join("short.partial"),
            directory.path().join("short.idxpartial"),
            SegmentId::new("short").expect("ID"),
            descriptor(&expected),
            Compression::None,
            limits(),
        )
        .expect("writer");
        writer.append(&expected[0]).expect("first frame");
        assert!(matches!(
            writer.finish(directory.path().join("short.idxraw")),
            Err(SegmentError::IncompleteRange {
                expected: 3,
                actual: 1
            })
        ));
    }

    #[test]
    fn corrupt_and_truncated_files_fail_closed() {
        let directory = tempdir().expect("temporary directory");
        let expected = frames(20, 24);
        let (path, metadata) =
            write_segment(directory.path(), "corrupt", &expected, Compression::Snappy);
        let mut file = OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("open segment for corruption");
        file.seek(SeekFrom::Start(HEADER_BYTES + 1))
            .expect("seek payload");
        file.write_all(&[0xff]).expect("corrupt payload");
        file.sync_all().expect("persist corruption");
        assert!(matches!(
            SegmentReader::open(&path, metadata.id.clone()),
            Err(SegmentError::ContentChecksum)
        ));

        let (path, metadata) =
            write_segment(directory.path(), "truncated", &expected, Compression::None);
        let file = OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("open segment for truncation");
        file.set_len(metadata.physical_bytes - 1)
            .expect("truncate segment");
        assert!(matches!(
            SegmentReader::open(path, metadata.id),
            Err(SegmentError::TrailerMagic | SegmentError::DirectoryBounds)
        ));
    }

    #[test]
    fn single_item_and_segment_limits_fail_before_publication() {
        let directory = tempdir().expect("temporary directory");
        let expected = frames(30, 30);
        let mut writer = SegmentWriter::create(
            directory.path().join("bounded.partial"),
            directory.path().join("bounded.idxpartial"),
            SegmentId::new("bounded").expect("ID"),
            descriptor(&expected),
            Compression::None,
            SegmentLimits {
                maximum_frame_logical_bytes: 1,
                maximum_segment_logical_bytes: 1,
                maximum_segment_physical_bytes: 1024,
            },
        )
        .expect("writer");
        assert!(matches!(
            writer.append(&expected[0]),
            Err(SegmentError::FrameOversized { .. })
        ));
        assert!(!directory.path().join("bounded.idxraw").exists());
    }

    #[test]
    #[ignore = "explicit small codec throughput/size fixture"]
    fn codec_candidate_performance_fixture() {
        use std::time::Instant;

        const BLOCKS: u64 = 4_096;
        let (source, _) =
            GeneratedHistorySource::new(SyntheticCorpusKind::BlobsLike, BLOCKS, 0x51de, 512)
                .expect("synthetic source");
        let first = source.frame(BlockNumber(1));
        let capabilities = first.capabilities();
        let descriptor = SegmentDescriptor {
            chain_id: first.chain_id,
            range: BlockRange::new(BlockNumber(1), BlockNumber(BLOCKS)).expect("range"),
            material_shape: MaterialShapeId([0x77; 32]),
            present_capabilities: capabilities.present,
            complete_capabilities: capabilities.complete,
            verification: VerificationClass::Cryptographic,
            trust: TrustModel::ProtocolVerified,
        };
        for compression in [Compression::None, Compression::Snappy, Compression::Deflate] {
            let directory = tempdir().expect("temporary directory");
            let id = SegmentId::new(format!("fixture-{}", compression as u8)).expect("ID");
            let partial = directory.path().join(format!("{}.partial", id.as_str()));
            let index = directory.path().join(format!("{}.idxpartial", id.as_str()));
            let final_path = directory.path().join(format!("{}.idxraw", id.as_str()));
            let mut writer = SegmentWriter::create(
                partial,
                index,
                id.clone(),
                descriptor.clone(),
                compression,
                SegmentLimits {
                    maximum_frame_logical_bytes: 16 * 1024 * 1024,
                    maximum_segment_logical_bytes: 2 * 1024 * 1024 * 1024,
                    maximum_segment_physical_bytes: 2 * 1024 * 1024 * 1024,
                },
            )
            .expect("writer");
            let write_started = Instant::now();
            writer.append(&first).expect("first frame");
            for number in 2..=BLOCKS {
                writer
                    .append(&source.frame(BlockNumber(number)))
                    .expect("generated frame");
            }
            let metadata = writer.finish(&final_path).expect("finish fixture");
            let write_elapsed = write_started.elapsed();
            let mut reader = SegmentReader::open(&final_path, id).expect("open fixture");
            let read_started = Instant::now();
            for number in (1..=BLOCKS).step_by(64) {
                let read = reader
                    .read_block(BlockNumber(number))
                    .expect("random-access fixture read");
                assert_eq!(read.records_decoded, 1);
            }
            eprintln!(
                "codec={compression:?} blocks={BLOCKS} logical_bytes={} physical_bytes={} write_ms={} sampled_read_ms={}",
                metadata.logical_bytes,
                metadata.physical_bytes,
                write_elapsed.as_millis(),
                read_started.elapsed().as_millis()
            );
        }
    }
}
