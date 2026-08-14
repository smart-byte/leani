use std::{
    fs::{File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use flate2::{Compression as DeflateLevel, read::ZlibDecoder, write::ZlibEncoder};
use leani_primitives::{BlockHash, BlockNumber, BlockRange, ChainId};
use leani_processor_api::{EncodedDelta, ProcessorDescriptor};
use serde::{Deserialize, Serialize};
use snap::raw::{Decoder as SnappyDecoder, Encoder as SnappyEncoder};
use thiserror::Error;

const HEADER_MAGIC: &[u8; 8] = b"IDXART01";
const DIRECTORY_MAGIC: &[u8; 8] = b"IDXADI01";
const TRAILER_MAGIC: &[u8; 8] = b"IDXATE01";
const FORMAT_VERSION: u16 = 2;
const HEADER_PREFIX_LEN: usize = 16;
const DIRECTORY_PREFIX_LEN: usize = 16;
const DIRECTORY_PREFIX_BYTES: u64 = 16;
const DIRECTORY_ENTRY_LEN: usize = 96;
const DIRECTORY_ENTRY_BYTES: u64 = 96;
const TRAILER_PREFIX_LEN: usize = 72;
const TRAILER_LEN: usize = 104;
const TRAILER_BYTES: u64 = 104;

/// Per-artifact compression. Each record remains independently seekable.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum ArtifactCompression {
    #[default]
    None = 0,
    Snappy = 1,
    Deflate = 2,
}

impl ArtifactCompression {
    fn from_byte(value: u8) -> Result<Self, ArtifactSegmentError> {
        match value {
            0 => Ok(Self::None),
            1 => Ok(Self::Snappy),
            2 => Ok(Self::Deflate),
            other => Err(ArtifactSegmentError::UnknownCompression(other)),
        }
    }

    fn encode(self, input: &[u8]) -> Result<Vec<u8>, ArtifactSegmentError> {
        match self {
            Self::None => Ok(input.to_vec()),
            Self::Snappy => SnappyEncoder::new()
                .compress_vec(input)
                .map_err(ArtifactSegmentError::Snappy),
            Self::Deflate => {
                let mut encoder = ZlibEncoder::new(Vec::new(), DeflateLevel::fast());
                encoder.write_all(input)?;
                encoder.finish().map_err(ArtifactSegmentError::Io)
            }
        }
    }

    fn decode(self, input: &[u8], expected: usize) -> Result<Vec<u8>, ArtifactSegmentError> {
        let decoded = match self {
            Self::None => input.to_vec(),
            Self::Snappy => SnappyDecoder::new()
                .decompress_vec(input)
                .map_err(ArtifactSegmentError::Snappy)?,
            Self::Deflate => {
                let mut decoder = ZlibDecoder::new(input);
                let mut output = Vec::with_capacity(expected);
                decoder.read_to_end(&mut output)?;
                output
            }
        };
        if decoded.len() != expected {
            return Err(ArtifactSegmentError::DecodedLength {
                expected,
                actual: decoded.len(),
            });
        }
        Ok(decoded)
    }
}

/// Immutable map/delta and canonical-range identity of one segment.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ArtifactSegmentDescriptor {
    pub processor_id: String,
    pub processor_instance: String,
    pub processor_version: String,
    pub code_hash: BlockHash,
    pub config_hash: BlockHash,
    pub delta_schema_version: u16,
    pub chain_id: ChainId,
    pub range: BlockRange,
}

impl ArtifactSegmentDescriptor {
    #[must_use]
    pub fn new(processor: &ProcessorDescriptor, chain_id: ChainId, range: BlockRange) -> Self {
        Self {
            processor_id: processor.id.to_string(),
            processor_instance: processor.instance.to_string(),
            processor_version: processor.version.to_string(),
            code_hash: processor.code_hash,
            config_hash: processor.config_hash,
            delta_schema_version: processor.schemas.delta_version,
            chain_id,
            range,
        }
    }

    fn validate_processor(
        &self,
        processor: &ProcessorDescriptor,
    ) -> Result<(), ArtifactSegmentError> {
        if self.processor_id != processor.id.as_str()
            || self.processor_instance != processor.instance.as_str()
            || self.processor_version != processor.version.to_string()
            || self.code_hash != processor.code_hash
            || self.config_hash != processor.config_hash
            || self.delta_schema_version != processor.schemas.delta_version
        {
            return Err(ArtifactSegmentError::Contract);
        }
        Ok(())
    }
}

/// Hard limits checked before each append.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ArtifactSegmentLimits {
    pub maximum_artifact_logical_bytes: u64,
    pub maximum_segment_logical_bytes: u64,
    pub maximum_segment_physical_bytes: u64,
}

impl ArtifactSegmentLimits {
    /// Validate internally consistent non-zero limits.
    ///
    /// # Errors
    ///
    /// Rejects zero limits and a per-artifact limit larger than the segment
    /// logical ceiling.
    pub const fn validate(self) -> Result<Self, ArtifactSegmentError> {
        if self.maximum_artifact_logical_bytes == 0
            || self.maximum_segment_logical_bytes == 0
            || self.maximum_segment_physical_bytes == 0
            || self.maximum_artifact_logical_bytes > self.maximum_segment_logical_bytes
        {
            return Err(ArtifactSegmentError::InvalidLimits);
        }
        Ok(self)
    }
}

/// Validated immutable metadata for a closed segment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactSegmentMetadata {
    pub descriptor: ArtifactSegmentDescriptor,
    pub compression: ArtifactCompression,
    pub artifacts: u64,
    pub logical_bytes: u64,
    pub physical_bytes: u64,
    pub records_checksum: [u8; 32],
    pub content_checksum: [u8; 32],
}

/// Descriptor and boundary identity recovered without loading processor code.
///
/// This is sufficient for a node to rebuild its durable segment catalog at
/// startup. Decoding records still requires [`ArtifactSegmentReader::open`]
/// with the exact processor contract.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactSegmentInspection {
    pub metadata: ArtifactSegmentMetadata,
    pub first_block_hash: BlockHash,
    pub last_block_hash: BlockHash,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DirectoryEntry {
    block_number: BlockNumber,
    offset: u64,
    stored_bytes: u64,
    logical_bytes: u64,
    block_hash: BlockHash,
    delta_checksum: BlockHash,
}

impl DirectoryEntry {
    fn encode(self) -> [u8; DIRECTORY_ENTRY_LEN] {
        let mut encoded = [0_u8; DIRECTORY_ENTRY_LEN];
        encoded[0..8].copy_from_slice(&self.block_number.0.to_be_bytes());
        encoded[8..16].copy_from_slice(&self.offset.to_be_bytes());
        encoded[16..24].copy_from_slice(&self.stored_bytes.to_be_bytes());
        encoded[24..32].copy_from_slice(&self.logical_bytes.to_be_bytes());
        encoded[32..64].copy_from_slice(&self.block_hash.0);
        encoded[64..96].copy_from_slice(&self.delta_checksum.0);
        encoded
    }

    fn decode(encoded: [u8; DIRECTORY_ENTRY_LEN]) -> Self {
        Self {
            block_number: BlockNumber(read_u64(&encoded, 0)),
            offset: read_u64(&encoded, 8),
            stored_bytes: read_u64(&encoded, 16),
            logical_bytes: read_u64(&encoded, 24),
            block_hash: BlockHash::new(read_hash(&encoded, 32)),
            delta_checksum: BlockHash::new(read_hash(&encoded, 64)),
        }
    }
}

/// Bounded writer for one contiguous immutable artifact segment.
#[derive(Debug)]
pub struct ArtifactSegmentWriter {
    final_path: PathBuf,
    partial_path: PathBuf,
    directory_path: PathBuf,
    output: BufWriter<File>,
    directory: BufWriter<File>,
    descriptor: ArtifactSegmentDescriptor,
    processor: ProcessorDescriptor,
    compression: ArtifactCompression,
    limits: ArtifactSegmentLimits,
    position: u64,
    next_block: BlockNumber,
    previous_hash: Option<BlockHash>,
    artifacts: u64,
    logical_bytes: u64,
    content_hasher: blake3::Hasher,
    records_hasher: blake3::Hasher,
}

impl ArtifactSegmentWriter {
    /// Create a new uniquely named partial segment and spill its seek directory
    /// into a bounded sidecar.
    ///
    /// # Errors
    ///
    /// Rejects invalid limits, an existing destination/partial file, or an
    /// unencodable descriptor.
    pub fn create(
        final_path: impl Into<PathBuf>,
        processor: &ProcessorDescriptor,
        chain_id: ChainId,
        range: BlockRange,
        compression: ArtifactCompression,
        limits: ArtifactSegmentLimits,
    ) -> Result<Self, ArtifactSegmentError> {
        let limits = limits.validate()?;
        let final_path = final_path.into();
        if final_path.exists() {
            return Err(ArtifactSegmentError::DestinationExists(final_path));
        }
        if let Some(parent) = final_path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }
        let partial_path = sibling_path(&final_path, "partial");
        let directory_path = sibling_path(&final_path, "directory.partial");
        let output = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&partial_path)?;
        let directory = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&directory_path)?;
        let descriptor = ArtifactSegmentDescriptor::new(processor, chain_id, range);
        let descriptor_bytes = postcard::to_allocvec(&descriptor)
            .map_err(|error| ArtifactSegmentError::Encoding(error.to_string()))?;
        let descriptor_len = u32::try_from(descriptor_bytes.len())
            .map_err(|_| ArtifactSegmentError::Numeric("descriptor bytes"))?;
        let mut header = Vec::with_capacity(HEADER_PREFIX_LEN + descriptor_bytes.len());
        header.extend_from_slice(HEADER_MAGIC);
        header.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
        header.push(compression as u8);
        header.push(0);
        header.extend_from_slice(&descriptor_len.to_be_bytes());
        header.extend_from_slice(&descriptor_bytes);
        let mut output = BufWriter::new(output);
        output.write_all(&header)?;
        let mut content_hasher = blake3::Hasher::new();
        content_hasher.update(&header);
        Ok(Self {
            final_path,
            partial_path,
            directory_path,
            output,
            directory: BufWriter::new(directory),
            descriptor,
            processor: processor.clone(),
            compression,
            limits,
            position: u64::try_from(header.len())
                .map_err(|_| ArtifactSegmentError::Numeric("header bytes"))?,
            next_block: range.start(),
            previous_hash: None,
            artifacts: 0,
            logical_bytes: 0,
            content_hasher,
            records_hasher: blake3::Hasher::new(),
        })
    }

    /// Append the exact next finalized map artifact.
    ///
    /// # Errors
    ///
    /// Rejects incompatible/corrupt, out-of-order, parent-broken, or oversized
    /// artifacts before publishing any segment metadata.
    pub fn append(&mut self, delta: &EncodedDelta) -> Result<(), ArtifactSegmentError> {
        delta
            .validate(&self.processor)
            .map_err(|error| ArtifactSegmentError::Delta(error.to_string()))?;
        if delta.chain_id != self.descriptor.chain_id
            || delta.block.number != self.next_block
            || !self.descriptor.range.contains(delta.block.number)
        {
            return Err(ArtifactSegmentError::Ordering {
                expected: self.next_block,
                received: delta.block.number,
            });
        }
        if self
            .previous_hash
            .is_some_and(|previous| delta.block.parent_hash != previous)
        {
            return Err(ArtifactSegmentError::ParentLink(delta.block.number));
        }
        let logical = delta
            .encode_durable()
            .map_err(|error| ArtifactSegmentError::Delta(error.to_string()))?;
        let logical_bytes = u64::try_from(logical.len())
            .map_err(|_| ArtifactSegmentError::Numeric("artifact logical bytes"))?;
        if logical_bytes > self.limits.maximum_artifact_logical_bytes {
            return Err(ArtifactSegmentError::ArtifactTooLarge {
                observed: logical_bytes,
                limit: self.limits.maximum_artifact_logical_bytes,
            });
        }
        let projected_logical = self
            .logical_bytes
            .checked_add(logical_bytes)
            .ok_or(ArtifactSegmentError::Numeric("segment logical bytes"))?;
        if projected_logical > self.limits.maximum_segment_logical_bytes {
            return Err(ArtifactSegmentError::SegmentLogicalLimit {
                projected: projected_logical,
                limit: self.limits.maximum_segment_logical_bytes,
            });
        }
        let stored = self.compression.encode(&logical)?;
        let stored_bytes = u64::try_from(stored.len())
            .map_err(|_| ArtifactSegmentError::Numeric("artifact stored bytes"))?;
        let projected_count = self.artifacts.saturating_add(1);
        let projected_physical = self
            .position
            .saturating_add(stored_bytes)
            .saturating_add(DIRECTORY_PREFIX_BYTES)
            .saturating_add(projected_count.saturating_mul(DIRECTORY_ENTRY_BYTES))
            .saturating_add(TRAILER_BYTES);
        if projected_physical > self.limits.maximum_segment_physical_bytes {
            return Err(ArtifactSegmentError::SegmentPhysicalLimit {
                projected: projected_physical,
                limit: self.limits.maximum_segment_physical_bytes,
            });
        }
        let entry = DirectoryEntry {
            block_number: delta.block.number,
            offset: self.position,
            stored_bytes,
            logical_bytes,
            block_hash: delta.block.hash,
            delta_checksum: delta.checksum,
        };
        self.output.write_all(&stored)?;
        self.directory.write_all(&entry.encode())?;
        self.content_hasher.update(&stored);
        hash_record(&mut self.records_hasher, &logical);
        self.position = self.position.saturating_add(stored_bytes);
        self.logical_bytes = projected_logical;
        self.artifacts = projected_count;
        self.previous_hash = Some(delta.block.hash);
        self.next_block = BlockNumber(delta.block.number.0.saturating_add(1));
        Ok(())
    }

    /// Finalize, fsync, atomically publish, and return immutable metadata.
    ///
    /// # Errors
    ///
    /// Rejects incomplete ranges and I/O or atomic-publication failures.
    pub fn finish(mut self) -> Result<ArtifactSegmentMetadata, ArtifactSegmentError> {
        if self.artifacts != self.descriptor.range.len()
            || self.previous_hash.is_none()
            || self.next_block.0 != self.descriptor.range.end().0.saturating_add(1)
        {
            return Err(ArtifactSegmentError::Incomplete {
                expected: self.descriptor.range.len(),
                observed: self.artifacts,
            });
        }
        self.directory.flush()?;
        let directory_offset = self.position;
        let mut directory_prefix = Vec::with_capacity(DIRECTORY_PREFIX_LEN);
        directory_prefix.extend_from_slice(DIRECTORY_MAGIC);
        directory_prefix.extend_from_slice(&self.artifacts.to_be_bytes());
        self.output.write_all(&directory_prefix)?;
        self.content_hasher.update(&directory_prefix);
        self.position = self.position.saturating_add(DIRECTORY_PREFIX_BYTES);
        let mut directory = BufReader::new(File::open(&self.directory_path)?);
        let mut buffer = vec![0_u8; 64 * 1_024];
        loop {
            let read = directory.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            self.output.write_all(&buffer[..read])?;
            self.content_hasher.update(&buffer[..read]);
            self.position = self
                .position
                .saturating_add(u64::try_from(read).unwrap_or(u64::MAX));
        }
        let records_checksum = *self.records_hasher.finalize().as_bytes();
        let mut trailer = Vec::with_capacity(TRAILER_PREFIX_LEN);
        trailer.extend_from_slice(TRAILER_MAGIC);
        trailer.extend_from_slice(&directory_offset.to_be_bytes());
        trailer.extend_from_slice(&self.artifacts.to_be_bytes());
        trailer.extend_from_slice(&self.logical_bytes.to_be_bytes());
        trailer.extend_from_slice(&[0_u8; 8]);
        trailer.extend_from_slice(&records_checksum);
        debug_assert_eq!(trailer.len(), TRAILER_PREFIX_LEN);
        self.output.write_all(&trailer)?;
        self.content_hasher.update(&trailer);
        let content_checksum = *self.content_hasher.finalize().as_bytes();
        self.output.write_all(&content_checksum)?;
        self.position = self.position.saturating_add(TRAILER_BYTES);
        self.output.flush()?;
        self.output.get_ref().sync_all()?;
        std::fs::rename(&self.partial_path, &self.final_path)?;
        sync_parent(&self.final_path)?;
        std::fs::remove_file(&self.directory_path)?;
        Ok(ArtifactSegmentMetadata {
            descriptor: self.descriptor,
            compression: self.compression,
            artifacts: self.artifacts,
            logical_bytes: self.logical_bytes,
            physical_bytes: self.position,
            records_checksum,
            content_checksum,
        })
    }
}

/// Fail-closed reader with O(1) block-number seeks.
#[derive(Debug)]
pub struct ArtifactSegmentReader {
    path: PathBuf,
    processor: ProcessorDescriptor,
    metadata: ArtifactSegmentMetadata,
    directory_offset: u64,
}

impl ArtifactSegmentReader {
    /// Inspect and checksum a closed segment without loading processor code.
    ///
    /// # Errors
    ///
    /// Rejects truncated, corrupt, unsupported, or structurally malformed
    /// segments. Record payloads are decoded only after the exact processor
    /// descriptor is supplied to [`Self::open`].
    pub fn inspect(
        path: impl Into<PathBuf>,
    ) -> Result<ArtifactSegmentInspection, ArtifactSegmentError> {
        let path = path.into();
        let parsed = parse_segment(&path)?;
        Ok(ArtifactSegmentInspection {
            metadata: parsed.metadata,
            first_block_hash: parsed.first.block_hash,
            last_block_hash: parsed.last.block_hash,
        })
    }

    /// Open and fully validate a segment's envelope and content checksum.
    ///
    /// # Errors
    ///
    /// Rejects truncated/corrupt files, unsupported versions/compression,
    /// incompatible contracts, malformed directories, and numeric overflow.
    pub fn open(
        path: impl Into<PathBuf>,
        processor: &ProcessorDescriptor,
    ) -> Result<Self, ArtifactSegmentError> {
        let path = path.into();
        let parsed = parse_segment(&path)?;
        parsed.metadata.descriptor.validate_processor(processor)?;
        let reader = Self {
            path,
            processor: processor.clone(),
            metadata: parsed.metadata,
            directory_offset: parsed.directory_offset,
        };
        Ok(reader)
    }

    #[must_use]
    pub const fn metadata(&self) -> &ArtifactSegmentMetadata {
        &self.metadata
    }

    /// Seek and decode one exact canonical block artifact.
    ///
    /// # Errors
    ///
    /// Rejects out-of-range blocks and any corrupt directory, compression,
    /// durable envelope, checksum, or block identity.
    pub fn read(&self, block: BlockNumber) -> Result<EncodedDelta, ArtifactSegmentError> {
        let mut file = File::open(&self.path)?;
        self.read_with_file(&mut file, block)
    }

    fn read_with_file(
        &self,
        file: &mut File,
        block: BlockNumber,
    ) -> Result<EncodedDelta, ArtifactSegmentError> {
        if !self.metadata.descriptor.range.contains(block) {
            return Err(ArtifactSegmentError::OutsideRange(block));
        }
        let ordinal = block
            .0
            .saturating_sub(self.metadata.descriptor.range.start().0);
        let entry = self.read_entry_with_file(file, ordinal)?;
        if entry.block_number != block {
            return Err(ArtifactSegmentError::Directory);
        }
        let stored_len = usize::try_from(entry.stored_bytes)
            .map_err(|_| ArtifactSegmentError::Numeric("stored record bytes"))?;
        let logical_len = usize::try_from(entry.logical_bytes)
            .map_err(|_| ArtifactSegmentError::Numeric("logical record bytes"))?;
        let mut stored = vec![0_u8; stored_len];
        file.seek(SeekFrom::Start(entry.offset))?;
        file.read_exact(&mut stored)?;
        let logical = self.metadata.compression.decode(&stored, logical_len)?;
        let delta = EncodedDelta::decode_durable(&self.processor, &logical)
            .map_err(|error| ArtifactSegmentError::Delta(error.to_string()))?;
        if delta.block.number != block
            || delta.block.hash != entry.block_hash
            || delta.checksum != entry.delta_checksum
            || delta.chain_id != self.metadata.descriptor.chain_id
        {
            return Err(ArtifactSegmentError::RecordIdentity(block));
        }
        Ok(delta)
    }

    /// Read a bounded ascending range without loading the whole segment.
    ///
    /// # Errors
    ///
    /// Rejects an invalid limit/range or any corrupt record.
    pub fn scan(
        &self,
        range: BlockRange,
        limit: usize,
    ) -> Result<Vec<EncodedDelta>, ArtifactSegmentError> {
        if limit == 0 || limit > 10_000 {
            return Err(ArtifactSegmentError::InvalidLimit(limit));
        }
        let start = range.start().max(self.metadata.descriptor.range.start());
        let end = range.end().min(self.metadata.descriptor.range.end());
        if start > end {
            return Ok(Vec::new());
        }
        let mut file = File::open(&self.path)?;
        BlockRange::new(start, end)
            .map_err(|_| ArtifactSegmentError::Directory)?
            .iter()
            .take(limit)
            .map(|block| self.read_with_file(&mut file, block))
            .collect()
    }

    /// Decode every record and verify the ordered logical-record checksum and
    /// internal parent links.
    ///
    /// # Errors
    ///
    /// Fails on the first corrupt record/link or a checksum mismatch.
    pub fn verify(&self) -> Result<(), ArtifactSegmentError> {
        let mut hasher = blake3::Hasher::new();
        let mut previous = None;
        let mut file = File::open(&self.path)?;
        for block in self.metadata.descriptor.range.iter() {
            let delta = self.read_with_file(&mut file, block)?;
            if previous.is_some_and(|hash| delta.block.parent_hash != hash) {
                return Err(ArtifactSegmentError::ParentLink(block));
            }
            let encoded = delta
                .encode_durable()
                .map_err(|error| ArtifactSegmentError::Delta(error.to_string()))?;
            hash_record(&mut hasher, &encoded);
            previous = Some(delta.block.hash);
        }
        if hasher.finalize().as_bytes() != &self.metadata.records_checksum {
            return Err(ArtifactSegmentError::RecordsChecksum);
        }
        Ok(())
    }

    fn read_entry_with_file(
        &self,
        file: &mut File,
        ordinal: u64,
    ) -> Result<DirectoryEntry, ArtifactSegmentError> {
        if ordinal >= self.metadata.artifacts {
            return Err(ArtifactSegmentError::Directory);
        }
        let offset = self
            .directory_offset
            .saturating_add(DIRECTORY_PREFIX_BYTES)
            .saturating_add(ordinal.saturating_mul(DIRECTORY_ENTRY_BYTES));
        let mut encoded = [0_u8; DIRECTORY_ENTRY_LEN];
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(&mut encoded)?;
        Ok(DirectoryEntry::decode(encoded))
    }
}

struct ParsedSegment {
    metadata: ArtifactSegmentMetadata,
    directory_offset: u64,
    first: DirectoryEntry,
    last: DirectoryEntry,
}

fn parse_segment(path: &Path) -> Result<ParsedSegment, ArtifactSegmentError> {
    let physical_bytes = std::fs::metadata(path)?.len();
    if physical_bytes < u64::try_from(HEADER_PREFIX_LEN).unwrap_or(u64::MAX) + TRAILER_BYTES {
        return Err(ArtifactSegmentError::Truncated);
    }
    verify_content_checksum(path, physical_bytes)?;
    let mut file = BufReader::new(File::open(path)?);
    let mut header = [0_u8; HEADER_PREFIX_LEN];
    file.read_exact(&mut header)?;
    if &header[0..8] != HEADER_MAGIC {
        return Err(ArtifactSegmentError::Magic);
    }
    let version = read_u16(&header, 8);
    if version != FORMAT_VERSION {
        return Err(ArtifactSegmentError::Version(version));
    }
    let compression = ArtifactCompression::from_byte(header[10])?;
    let descriptor_len = usize::try_from(read_u32(&header, 12))
        .map_err(|_| ArtifactSegmentError::Numeric("descriptor length"))?;
    let mut descriptor_bytes = vec![0_u8; descriptor_len];
    file.read_exact(&mut descriptor_bytes)?;
    let descriptor: ArtifactSegmentDescriptor = postcard::from_bytes(&descriptor_bytes)
        .map_err(|error| ArtifactSegmentError::Encoding(error.to_string()))?;

    file.seek(SeekFrom::End(-104))?;
    let mut trailer = [0_u8; TRAILER_LEN];
    file.read_exact(&mut trailer)?;
    if &trailer[0..8] != TRAILER_MAGIC {
        return Err(ArtifactSegmentError::Magic);
    }
    let directory_offset = read_u64(&trailer, 8);
    let artifacts = read_u64(&trailer, 16);
    let logical_bytes = read_u64(&trailer, 24);
    let records_checksum = read_hash(&trailer, 40);
    let content_checksum = read_hash(&trailer, 72);
    let expected_end = directory_offset
        .checked_add(DIRECTORY_PREFIX_BYTES)
        .and_then(|value| value.checked_add(artifacts.checked_mul(DIRECTORY_ENTRY_BYTES)?))
        .and_then(|value| value.checked_add(TRAILER_BYTES))
        .ok_or(ArtifactSegmentError::Numeric("segment layout"))?;
    if expected_end != physical_bytes || artifacts != descriptor.range.len() || artifacts == 0 {
        return Err(ArtifactSegmentError::Directory);
    }
    file.seek(SeekFrom::Start(directory_offset))?;
    let mut prefix = [0_u8; DIRECTORY_PREFIX_LEN];
    file.read_exact(&mut prefix)?;
    if &prefix[0..8] != DIRECTORY_MAGIC || read_u64(&prefix, 8) != artifacts {
        return Err(ArtifactSegmentError::Directory);
    }
    let first = read_directory_entry(&mut file, directory_offset, artifacts, 0)?;
    let last = read_directory_entry(
        &mut file,
        directory_offset,
        artifacts,
        artifacts.saturating_sub(1),
    )?;
    let minimum_record_offset = u64::try_from(HEADER_PREFIX_LEN)
        .unwrap_or(u64::MAX)
        .saturating_add(u64::try_from(descriptor_bytes.len()).unwrap_or(u64::MAX));
    if first.block_number != descriptor.range.start()
        || last.block_number != descriptor.range.end()
        || first.offset < minimum_record_offset
        || last.offset.saturating_add(last.stored_bytes) > directory_offset
    {
        return Err(ArtifactSegmentError::Directory);
    }
    Ok(ParsedSegment {
        metadata: ArtifactSegmentMetadata {
            descriptor,
            compression,
            artifacts,
            logical_bytes,
            physical_bytes,
            records_checksum,
            content_checksum,
        },
        directory_offset,
        first,
        last,
    })
}

fn read_directory_entry(
    file: &mut (impl Read + Seek),
    directory_offset: u64,
    artifacts: u64,
    ordinal: u64,
) -> Result<DirectoryEntry, ArtifactSegmentError> {
    if ordinal >= artifacts {
        return Err(ArtifactSegmentError::Directory);
    }
    let offset = directory_offset
        .saturating_add(DIRECTORY_PREFIX_BYTES)
        .saturating_add(ordinal.saturating_mul(DIRECTORY_ENTRY_BYTES));
    let mut encoded = [0_u8; DIRECTORY_ENTRY_LEN];
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(&mut encoded)?;
    Ok(DirectoryEntry::decode(encoded))
}

fn verify_content_checksum(path: &Path, physical_bytes: u64) -> Result<(), ArtifactSegmentError> {
    let content_len = physical_bytes.saturating_sub(32);
    let mut file = BufReader::new(File::open(path)?);
    let mut hasher = blake3::Hasher::new();
    let mut remaining = content_len;
    let mut buffer = vec![0_u8; 64 * 1_024];
    while remaining > 0 {
        let buffer_bytes = u64::try_from(buffer.len()).unwrap_or(u64::MAX);
        let maximum = usize::try_from(remaining.min(buffer_bytes))
            .map_err(|_| ArtifactSegmentError::Numeric("content checksum bytes"))?;
        file.read_exact(&mut buffer[..maximum])?;
        hasher.update(&buffer[..maximum]);
        remaining = remaining.saturating_sub(u64::try_from(maximum).unwrap_or(u64::MAX));
    }
    let mut stored = [0_u8; 32];
    file.read_exact(&mut stored)?;
    if hasher.finalize().as_bytes() != &stored {
        return Err(ArtifactSegmentError::ContentChecksum);
    }
    Ok(())
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    let mut encoded = [0_u8; 2];
    encoded.copy_from_slice(&bytes[offset..offset + 2]);
    u16::from_be_bytes(encoded)
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    let mut encoded = [0_u8; 4];
    encoded.copy_from_slice(&bytes[offset..offset + 4]);
    u32::from_be_bytes(encoded)
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    let mut encoded = [0_u8; 8];
    encoded.copy_from_slice(&bytes[offset..offset + 8]);
    u64::from_be_bytes(encoded)
}

fn read_hash(bytes: &[u8], offset: usize) -> [u8; 32] {
    let mut encoded = [0_u8; 32];
    encoded.copy_from_slice(&bytes[offset..offset + 32]);
    encoded
}

fn sibling_path(path: &Path, suffix: &str) -> PathBuf {
    let name = path
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .unwrap_or("artifacts");
    path.with_file_name(format!("{name}.{suffix}"))
}

fn sync_parent(path: &Path) -> Result<(), ArtifactSegmentError> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

fn hash_record(hasher: &mut blake3::Hasher, bytes: &[u8]) {
    hasher.update(&u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_be_bytes());
    hasher.update(bytes);
}

/// Artifact segment codec and storage failure.
#[derive(Debug, Error)]
pub enum ArtifactSegmentError {
    #[error("artifact segment I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("artifact segment encoding failed: {0}")]
    Encoding(String),
    #[error("artifact delta failed validation: {0}")]
    Delta(String),
    #[error("artifact segment limits are invalid")]
    InvalidLimits,
    #[error("artifact segment destination already exists: {0}")]
    DestinationExists(PathBuf),
    #[error("artifact segment numeric value for {0} overflowed")]
    Numeric(&'static str),
    #[error("artifact segment has invalid magic")]
    Magic,
    #[error("artifact segment version {0} is unsupported")]
    Version(u16),
    #[error("artifact segment compression {0} is unsupported")]
    UnknownCompression(u8),
    #[error("artifact segment Snappy codec failed: {0}")]
    Snappy(#[from] snap::Error),
    #[error("artifact segment is truncated")]
    Truncated,
    #[error("artifact segment contract does not match the processor")]
    Contract,
    #[error("artifact segment directory is malformed")]
    Directory,
    #[error("artifact segment content checksum does not match")]
    ContentChecksum,
    #[error("artifact segment ordered-record checksum does not match")]
    RecordsChecksum,
    #[error("artifact segment record decoded to {actual} bytes, expected {expected}")]
    DecodedLength { expected: usize, actual: usize },
    #[error("artifact segment expected block {expected}, received {received}")]
    Ordering {
        expected: BlockNumber,
        received: BlockNumber,
    },
    #[error("artifact segment parent link failed at block {0}")]
    ParentLink(BlockNumber),
    #[error("artifact block {0} is outside the segment range")]
    OutsideRange(BlockNumber),
    #[error("artifact record identity does not match block {0}")]
    RecordIdentity(BlockNumber),
    #[error("artifact scan limit {0} is outside 1..=10000")]
    InvalidLimit(usize),
    #[error("artifact uses {observed} logical bytes, exceeding {limit}")]
    ArtifactTooLarge { observed: u64, limit: u64 },
    #[error("artifact segment would use {projected} logical bytes, exceeding {limit}")]
    SegmentLogicalLimit { projected: u64, limit: u64 },
    #[error("artifact segment would use {projected} physical bytes, exceeding {limit}")]
    SegmentPhysicalLimit { projected: u64, limit: u64 },
    #[error("artifact segment is incomplete: expected {expected} artifacts, observed {observed}")]
    Incomplete { expected: u64, observed: u64 },
}

#[cfg(test)]
mod tests {
    use leani_primitives::BlockRef;
    use leani_processor_api::Processor;
    use leani_testkit::BlockLocalCounter;

    use super::*;

    fn deltas(processor: &dyn Processor, range: BlockRange) -> Vec<EncodedDelta> {
        let mut parent = BlockHash::ZERO;
        range
            .iter()
            .map(|number| {
                let mut hash = [0_u8; 32];
                hash[24..32].copy_from_slice(&number.0.to_be_bytes());
                let block = BlockRef {
                    number,
                    hash: BlockHash::new(hash),
                    parent_hash: parent,
                    timestamp: 1_700_000_000 + number.0 * 12,
                };
                parent = block.hash;
                EncodedDelta::new(
                    processor.descriptor(),
                    ChainId(1),
                    block,
                    number.0.to_be_bytes().to_vec(),
                )
            })
            .collect()
    }

    fn limits() -> ArtifactSegmentLimits {
        ArtifactSegmentLimits {
            maximum_artifact_logical_bytes: 1024 * 1024,
            maximum_segment_logical_bytes: 16 * 1024 * 1024,
            maximum_segment_physical_bytes: 16 * 1024 * 1024,
        }
    }

    #[test]
    fn every_codec_roundtrips_seeks_and_verifies() {
        let processor = BlockLocalCounter::default();
        let range = BlockRange::new(BlockNumber(1), BlockNumber(257)).expect("range");
        let deltas = deltas(&processor, range);
        for compression in [
            ArtifactCompression::None,
            ArtifactCompression::Snappy,
            ArtifactCompression::Deflate,
        ] {
            let directory = tempfile::tempdir().expect("tempdir");
            let path = directory.path().join(format!("{compression:?}.artifacts"));
            let mut writer = ArtifactSegmentWriter::create(
                &path,
                processor.descriptor(),
                ChainId(1),
                range,
                compression,
                limits(),
            )
            .expect("writer");
            for delta in &deltas {
                writer.append(delta).expect("append");
            }
            let written = writer.finish().expect("finish");
            assert_eq!(written.artifacts, range.len());
            assert_eq!(
                written.physical_bytes,
                std::fs::metadata(&path).expect("metadata").len()
            );
            let reader = ArtifactSegmentReader::open(&path, processor.descriptor()).expect("open");
            assert_eq!(reader.metadata(), &written);
            assert_eq!(reader.read(BlockNumber(129)).expect("seek"), deltas[128]);
            assert_eq!(
                reader
                    .scan(
                        BlockRange::new(BlockNumber(250), BlockNumber(257)).expect("scan range"),
                        4,
                    )
                    .expect("scan"),
                deltas[249..253]
            );
            reader.verify().expect("full verification");
        }
    }

    #[test]
    fn corruption_and_incomplete_publication_fail_closed() {
        let processor = BlockLocalCounter::default();
        let range = BlockRange::new(BlockNumber(1), BlockNumber(3)).expect("range");
        let deltas = deltas(&processor, range);
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("corrupt.artifacts");
        let mut writer = ArtifactSegmentWriter::create(
            &path,
            processor.descriptor(),
            ChainId(1),
            range,
            ArtifactCompression::Snappy,
            limits(),
        )
        .expect("writer");
        writer.append(&deltas[0]).expect("append first");
        assert!(matches!(
            writer.finish(),
            Err(ArtifactSegmentError::Incomplete {
                expected: 3,
                observed: 1
            })
        ));

        let path = directory.path().join("complete.artifacts");
        let mut writer = ArtifactSegmentWriter::create(
            &path,
            processor.descriptor(),
            ChainId(1),
            range,
            ArtifactCompression::Snappy,
            limits(),
        )
        .expect("writer");
        for delta in &deltas {
            writer.append(delta).expect("append");
        }
        writer.finish().expect("finish");
        let mut file = OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("open corrupt");
        file.seek(SeekFrom::Start(32)).expect("seek corrupt");
        file.write_all(&[0xff]).expect("corrupt byte");
        file.sync_all().expect("sync corruption");
        assert!(matches!(
            ArtifactSegmentReader::open(&path, processor.descriptor()),
            Err(ArtifactSegmentError::ContentChecksum)
        ));
    }

    #[test]
    fn ordering_parent_and_hard_limits_are_enforced_before_close() {
        let processor = BlockLocalCounter::default();
        let range = BlockRange::new(BlockNumber(1), BlockNumber(2)).expect("range");
        let deltas = deltas(&processor, range);
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("limits.artifacts");
        let mut writer = ArtifactSegmentWriter::create(
            &path,
            processor.descriptor(),
            ChainId(1),
            range,
            ArtifactCompression::None,
            ArtifactSegmentLimits {
                maximum_artifact_logical_bytes: 1,
                maximum_segment_logical_bytes: 2,
                maximum_segment_physical_bytes: 1024,
            },
        )
        .expect("writer");
        assert!(matches!(
            writer.append(&deltas[0]),
            Err(ArtifactSegmentError::ArtifactTooLarge { limit: 1, .. })
        ));

        let mut ordering = ArtifactSegmentWriter::create(
            directory.path().join("ordering.artifacts"),
            processor.descriptor(),
            ChainId(1),
            range,
            ArtifactCompression::None,
            limits(),
        )
        .expect("ordering writer");
        assert!(matches!(
            ordering.append(&deltas[1]),
            Err(ArtifactSegmentError::Ordering {
                expected: BlockNumber(1),
                received: BlockNumber(2)
            })
        ));

        let mut parent = ArtifactSegmentWriter::create(
            directory.path().join("parent.artifacts"),
            processor.descriptor(),
            ChainId(1),
            range,
            ArtifactCompression::None,
            limits(),
        )
        .expect("parent writer");
        parent.append(&deltas[0]).expect("append first");
        let mut bad_block = deltas[1].block;
        bad_block.parent_hash = BlockHash::new([9; 32]);
        let bad_parent = EncodedDelta::new(
            processor.descriptor(),
            ChainId(1),
            bad_block,
            deltas[1].payload.clone(),
        );
        assert!(matches!(
            parent.append(&bad_parent),
            Err(ArtifactSegmentError::ParentLink(BlockNumber(2)))
        ));

        let mut physical = ArtifactSegmentWriter::create(
            directory.path().join("physical.artifacts"),
            processor.descriptor(),
            ChainId(1),
            range,
            ArtifactCompression::None,
            ArtifactSegmentLimits {
                maximum_artifact_logical_bytes: 1024,
                maximum_segment_logical_bytes: 2048,
                maximum_segment_physical_bytes: 1,
            },
        )
        .expect("physical writer");
        assert!(matches!(
            physical.append(&deltas[0]),
            Err(ArtifactSegmentError::SegmentPhysicalLimit { limit: 1, .. })
        ));
    }
}
