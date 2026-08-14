use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use leani_primitives::{BlockHash, BlockNumber, BlockRange, ChainId};
use leani_processor_api::{EncodedDelta, ProcessorDescriptor};
use serde::Serialize;
use thiserror::Error;
use tokio::sync::Mutex;

use crate::{
    ArtifactCompression, ArtifactSegmentError, ArtifactSegmentLimits, ArtifactSegmentReader,
    ArtifactSegmentWriter,
};

/// Result of durably retaining one contiguous finalized artifact batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactBatchReceipt {
    pub range: BlockRange,
    pub path: PathBuf,
    pub artifacts: u64,
    pub logical_bytes: u64,
    pub physical_bytes: u64,
    pub records_checksum: [u8; 32],
    pub newly_retained: bool,
}

/// Runtime-facing durable boundary for finalized map artifacts.
#[async_trait]
pub trait ArtifactBatchSink: Send + Sync {
    /// Durably retain one contiguous finalized batch before processor coverage
    /// advances.
    async fn retain_finalized_batch(
        &self,
        descriptor: &ProcessorDescriptor,
        deltas: &[EncodedDelta],
    ) -> Result<ArtifactBatchReceipt, ArtifactSinkError>;
}

/// Immutable-segment sink policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ArtifactSegmentSinkConfig {
    pub compression: ArtifactCompression,
    pub limits: ArtifactSegmentLimits,
    pub maximum_retained_physical_bytes: u64,
}

impl ArtifactSegmentSinkConfig {
    /// Validate all hard bounds.
    ///
    /// # Errors
    ///
    /// Rejects invalid segment limits or an aggregate limit smaller than one
    /// permitted segment.
    pub fn validate(self) -> Result<Self, ArtifactSinkError> {
        if self.limits.validate().is_err()
            || self.maximum_retained_physical_bytes == 0
            || self.limits.maximum_segment_physical_bytes > self.maximum_retained_physical_bytes
        {
            return Err(ArtifactSinkError::InvalidConfig);
        }
        Ok(self)
    }
}

/// Current immutable-segment footprint.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactSegmentSinkStats {
    pub segments: u64,
    pub artifacts: u64,
    pub logical_bytes: u64,
    pub physical_bytes: u64,
}

#[derive(Clone, Debug)]
struct SegmentEntry {
    range: BlockRange,
    path: PathBuf,
    artifacts: u64,
    logical_bytes: u64,
    physical_bytes: u64,
    first_hash: BlockHash,
    last_hash: BlockHash,
    records_checksum: [u8; 32],
}

type SegmentKey = (String, u64, u64);
type SegmentCatalog = BTreeMap<SegmentKey, SegmentEntry>;

#[derive(Clone, Copy, Debug)]
struct ValidatedBatch {
    range: BlockRange,
    chain_id: ChainId,
    first_hash: BlockHash,
    last_hash: BlockHash,
    logical_bytes: u64,
    records_checksum: [u8; 32],
}

#[derive(Debug, Default)]
struct SinkState {
    entries: SegmentCatalog,
    stats: ArtifactSegmentSinkStats,
    directory_physical_bytes: u64,
}

/// Direct immutable-segment implementation of [`ArtifactBatchSink`].
#[derive(Clone, Debug)]
pub struct ArtifactSegmentSink {
    root: PathBuf,
    config: ArtifactSegmentSinkConfig,
    state: Arc<Mutex<SinkState>>,
}

impl ArtifactSegmentSink {
    /// Open a segment sink rooted at one node-owned directory.
    ///
    /// Orphan partial publications are removed and their parent directories
    /// fsynced before aggregate admission is evaluated. Existing closed files
    /// count against aggregate admission.
    /// Every closed segment is checksummed and catalogued before the sink is
    /// returned, so retained artifacts remain queryable immediately after a
    /// process restart.
    ///
    /// # Errors
    ///
    /// Rejects invalid limits or an unreadable/uncreatable directory.
    pub fn open(
        root: impl Into<PathBuf>,
        config: ArtifactSegmentSinkConfig,
    ) -> Result<Self, ArtifactSinkError> {
        let config = config.validate()?;
        let root = root.into();
        std::fs::create_dir_all(&root)?;
        remove_orphan_partials(&root)?;
        let directory_physical_bytes = directory_physical_bytes(&root)?;
        if directory_physical_bytes > config.maximum_retained_physical_bytes {
            return Err(ArtifactSinkError::AggregatePhysicalLimit {
                projected: directory_physical_bytes,
                limit: config.maximum_retained_physical_bytes,
            });
        }
        let (entries, stats) = discover_segments(&root)?;
        Ok(Self {
            root,
            config,
            state: Arc::new(Mutex::new(SinkState {
                entries,
                stats,
                directory_physical_bytes,
            })),
        })
    }

    /// Return in-process adopted/written segment statistics.
    pub async fn stats(&self) -> ArtifactSegmentSinkStats {
        self.state.lock().await.stats
    }

    /// Return one exact retained batch from the rebuilt catalog.
    pub async fn retained_batch(
        &self,
        instance: &str,
        range: BlockRange,
    ) -> Option<ArtifactBatchReceipt> {
        self.state
            .lock()
            .await
            .entries
            .get(&(instance.to_owned(), range.start().0, range.end().0))
            .map(|entry| receipt(entry, false))
    }

    /// Delete one exact closed segment after its external ownership catalog
    /// has durably marked it for deletion.
    ///
    /// Missing segments are an idempotent success for crash recovery.
    ///
    /// # Errors
    ///
    /// Returns an I/O failure without removing the in-memory entry.
    pub async fn remove_exact(
        &self,
        instance: &str,
        range: BlockRange,
    ) -> Result<bool, ArtifactSinkError> {
        let key = (instance.to_owned(), range.start().0, range.end().0);
        let mut state = self.state.lock().await;
        let Some(entry) = state.entries.get(&key).cloned() else {
            return Ok(false);
        };
        match std::fs::remove_file(&entry.path) {
            Ok(()) => {
                if let Some(parent) = entry.path.parent() {
                    std::fs::File::open(parent)?.sync_all()?;
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        state.entries.remove(&key);
        state.stats.segments = state.stats.segments.saturating_sub(1);
        state.stats.artifacts = state.stats.artifacts.saturating_sub(entry.artifacts);
        state.stats.logical_bytes = state
            .stats
            .logical_bytes
            .saturating_sub(entry.logical_bytes);
        state.stats.physical_bytes = state
            .stats
            .physical_bytes
            .saturating_sub(entry.physical_bytes);
        state.directory_physical_bytes = state
            .directory_physical_bytes
            .saturating_sub(entry.physical_bytes);
        Ok(true)
    }

    /// Read a bounded ascending artifact range from adopted segments.
    ///
    /// # Errors
    ///
    /// Rejects invalid limits, gaps, incompatible contracts, and corrupt
    /// segments.
    pub async fn scan(
        &self,
        descriptor: &ProcessorDescriptor,
        range: BlockRange,
        limit: usize,
    ) -> Result<Vec<EncodedDelta>, ArtifactSinkError> {
        if limit == 0 || limit > 10_000 {
            return Err(ArtifactSinkError::InvalidScanLimit(limit));
        }
        let key_prefix = descriptor.instance.to_string();
        let entries = self
            .state
            .lock()
            .await
            .entries
            .iter()
            .filter(|((instance, _, _), entry)| {
                instance == &key_prefix
                    && entry.range.end() >= range.start()
                    && entry.range.start() <= range.end()
            })
            .map(|(_, entry)| entry.clone())
            .collect::<Vec<_>>();
        let descriptor = descriptor.clone();
        tokio::task::spawn_blocking(move || scan_entries(&descriptor, range, limit, &entries))
            .await
            .map_err(|error| ArtifactSinkError::Task(error.to_string()))?
    }

    /// Fully verify adopted segments and their cross-segment parent links.
    ///
    /// # Errors
    ///
    /// Fails on corrupt records, internal gaps, overlaps, or parent mismatch.
    pub async fn verify(&self, descriptor: &ProcessorDescriptor) -> Result<(), ArtifactSinkError> {
        let instance = descriptor.instance.to_string();
        let entries = self
            .state
            .lock()
            .await
            .entries
            .iter()
            .filter(|((candidate, _, _), _)| candidate == &instance)
            .map(|(_, entry)| entry.clone())
            .collect::<Vec<_>>();
        let descriptor = descriptor.clone();
        tokio::task::spawn_blocking(move || verify_entries(&descriptor, &entries))
            .await
            .map_err(|error| ArtifactSinkError::Task(error.to_string()))?
    }
}

#[async_trait]
impl ArtifactBatchSink for ArtifactSegmentSink {
    #[allow(clippy::too_many_lines)]
    async fn retain_finalized_batch(
        &self,
        descriptor: &ProcessorDescriptor,
        deltas: &[EncodedDelta],
    ) -> Result<ArtifactBatchReceipt, ArtifactSinkError> {
        let ValidatedBatch {
            range,
            chain_id,
            first_hash,
            last_hash,
            logical_bytes,
            records_checksum,
        } = validate_batch(descriptor, deltas)?;
        let instance = descriptor.instance.to_string();
        let key = (instance.clone(), range.start().0, range.end().0);
        let mut state = self.state.lock().await;
        if let Some(entry) = state.entries.get(&key) {
            validate_entry_identity(
                entry,
                first_hash,
                last_hash,
                logical_bytes,
                records_checksum,
            )?;
            return Ok(receipt(entry, false));
        }
        if let Some((_, entry)) = state.entries.iter().find(|((candidate, _, _), entry)| {
            candidate == &instance
                && entry.range.start() <= range.end()
                && entry.range.end() >= range.start()
        }) {
            return Err(ArtifactSinkError::Conflict(entry.range));
        }

        let processor_directory = self.root.join(contract_directory_name(descriptor));
        let path = processor_directory.join(format!(
            "{:020}-{:020}.artifacts",
            range.start().0,
            range.end().0
        ));
        let existed = path.exists();
        let remaining = if existed {
            self.config.limits.maximum_segment_physical_bytes
        } else {
            self.config
                .maximum_retained_physical_bytes
                .saturating_sub(state.directory_physical_bytes)
        };
        if !existed && remaining == 0 {
            return Err(ArtifactSinkError::AggregatePhysicalLimit {
                projected: state.directory_physical_bytes.saturating_add(1),
                limit: self.config.maximum_retained_physical_bytes,
            });
        }
        let mut limits = self.config.limits;
        limits.maximum_segment_physical_bytes =
            limits.maximum_segment_physical_bytes.min(remaining);
        let descriptor_owned = descriptor.clone();
        let deltas_owned = deltas.to_vec();
        let compression = self.config.compression;
        let path_owned = path.clone();
        let task = tokio::task::spawn_blocking(move || {
            if path_owned.exists() {
                let reader = ArtifactSegmentReader::open(&path_owned, &descriptor_owned)?;
                reader.verify()?;
                if reader.metadata().records_checksum != records_checksum {
                    return Err(ArtifactSegmentError::RecordsChecksum);
                }
                return Ok(reader.metadata().clone());
            }
            let mut writer = ArtifactSegmentWriter::create(
                &path_owned,
                &descriptor_owned,
                chain_id,
                range,
                compression,
                limits,
            )?;
            for delta in &deltas_owned {
                writer.append(delta)?;
            }
            writer.finish()
        })
        .await
        .map_err(|error| ArtifactSinkError::Task(error.to_string()))?;
        let metadata = match task {
            Ok(metadata) => metadata,
            Err(error) => {
                state.directory_physical_bytes = directory_physical_bytes(&self.root)?;
                return Err(error.into());
            }
        };
        let projected = if existed {
            state.directory_physical_bytes
        } else {
            state
                .directory_physical_bytes
                .saturating_add(metadata.physical_bytes)
        };
        if projected > self.config.maximum_retained_physical_bytes {
            return Err(ArtifactSinkError::AggregatePhysicalLimit {
                projected,
                limit: self.config.maximum_retained_physical_bytes,
            });
        }
        let entry = SegmentEntry {
            range,
            path,
            artifacts: metadata.artifacts,
            logical_bytes: metadata.logical_bytes,
            physical_bytes: metadata.physical_bytes,
            first_hash,
            last_hash,
            records_checksum,
        };
        state.directory_physical_bytes = projected;
        state.stats.segments = state.stats.segments.saturating_add(1);
        state.stats.artifacts = state.stats.artifacts.saturating_add(entry.artifacts);
        state.stats.logical_bytes = state
            .stats
            .logical_bytes
            .saturating_add(entry.logical_bytes);
        state.stats.physical_bytes = state
            .stats
            .physical_bytes
            .saturating_add(entry.physical_bytes);
        let result = receipt(&entry, !existed);
        state.entries.insert(key, entry);
        Ok(result)
    }
}

fn validate_batch(
    descriptor: &ProcessorDescriptor,
    deltas: &[EncodedDelta],
) -> Result<ValidatedBatch, ArtifactSinkError> {
    let first = deltas.first().ok_or(ArtifactSinkError::EmptyBatch)?;
    let last = deltas.last().ok_or(ArtifactSinkError::EmptyBatch)?;
    let range = BlockRange::new(first.block.number, last.block.number)
        .map_err(|error| ArtifactSinkError::Batch(error.to_string()))?;
    if range.len() != u64::try_from(deltas.len()).unwrap_or(u64::MAX) {
        return Err(ArtifactSinkError::Batch(
            "artifact batch must have exactly one delta per block".to_owned(),
        ));
    }
    let mut next = range.start();
    let mut parent = None;
    let mut logical_bytes = 0_u64;
    let mut records = blake3::Hasher::new();
    for delta in deltas {
        delta
            .validate(descriptor)
            .map_err(|error| ArtifactSinkError::Batch(error.to_string()))?;
        if delta.chain_id != first.chain_id
            || delta.block.number != next
            || parent.is_some_and(|hash| delta.block.parent_hash != hash)
        {
            return Err(ArtifactSinkError::Batch(
                "artifact batch must be contiguous, parent-linked, and single-chain".to_owned(),
            ));
        }
        let encoded = delta
            .encode_durable()
            .map_err(|error| ArtifactSinkError::Batch(error.to_string()))?;
        logical_bytes = logical_bytes
            .checked_add(u64::try_from(encoded.len()).unwrap_or(u64::MAX))
            .ok_or_else(|| ArtifactSinkError::Batch("artifact bytes overflowed".to_owned()))?;
        hash_record(&mut records, &encoded);
        next = BlockNumber(next.0.saturating_add(1));
        parent = Some(delta.block.hash);
    }
    Ok(ValidatedBatch {
        range,
        chain_id: first.chain_id,
        first_hash: first.block.hash,
        last_hash: last.block.hash,
        logical_bytes,
        records_checksum: *records.finalize().as_bytes(),
    })
}

fn scan_entries(
    descriptor: &ProcessorDescriptor,
    range: BlockRange,
    limit: usize,
    entries: &[SegmentEntry],
) -> Result<Vec<EncodedDelta>, ArtifactSinkError> {
    let mut result = Vec::with_capacity(limit);
    let mut expected = range.start();
    for entry in entries {
        if result.len() == limit || expected > range.end() {
            break;
        }
        let start = expected.max(entry.range.start());
        let end = range.end().min(entry.range.end());
        if start > end {
            continue;
        }
        if start != expected {
            return Err(ArtifactSinkError::CoverageGap(expected));
        }
        let reader = ArtifactSegmentReader::open(&entry.path, descriptor)?;
        let remaining = limit.saturating_sub(result.len());
        let decoded = reader.scan(
            BlockRange::new(start, end)
                .map_err(|error| ArtifactSinkError::Batch(error.to_string()))?,
            remaining,
        )?;
        if decoded.is_empty() {
            return Err(ArtifactSinkError::CoverageGap(expected));
        }
        expected = BlockNumber(
            decoded
                .last()
                .ok_or(ArtifactSinkError::CoverageGap(expected))?
                .block
                .number
                .0
                .saturating_add(1),
        );
        result.extend(decoded);
    }
    Ok(result)
}

fn verify_entries(
    descriptor: &ProcessorDescriptor,
    entries: &[SegmentEntry],
) -> Result<(), ArtifactSinkError> {
    let mut previous_range = None;
    let mut previous_hash = None;
    for entry in entries {
        let reader = ArtifactSegmentReader::open(&entry.path, descriptor)?;
        let first = reader.read(entry.range.start())?;
        let last = reader.read(entry.range.end())?;
        if first.block.hash != entry.first_hash
            || last.block.hash != entry.last_hash
            || previous_range.is_some_and(|range: BlockRange| {
                range.end().0.saturating_add(1) != entry.range.start().0
            })
            || previous_hash.is_some_and(|hash| first.block.parent_hash != hash)
        {
            return Err(ArtifactSinkError::CoverageGap(entry.range.start()));
        }
        reader.verify()?;
        previous_range = Some(entry.range);
        previous_hash = Some(entry.last_hash);
    }
    Ok(())
}

fn validate_entry_identity(
    entry: &SegmentEntry,
    first_hash: BlockHash,
    last_hash: BlockHash,
    logical_bytes: u64,
    records_checksum: [u8; 32],
) -> Result<(), ArtifactSinkError> {
    if entry.first_hash != first_hash
        || entry.last_hash != last_hash
        || entry.logical_bytes != logical_bytes
        || entry.records_checksum != records_checksum
    {
        return Err(ArtifactSinkError::Conflict(entry.range));
    }
    Ok(())
}

fn receipt(entry: &SegmentEntry, newly_retained: bool) -> ArtifactBatchReceipt {
    ArtifactBatchReceipt {
        range: entry.range,
        path: entry.path.clone(),
        artifacts: entry.artifacts,
        logical_bytes: entry.logical_bytes,
        physical_bytes: entry.physical_bytes,
        records_checksum: entry.records_checksum,
        newly_retained,
    }
}

fn directory_physical_bytes(path: &Path) -> Result<u64, ArtifactSinkError> {
    let mut total = 0_u64;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            total = total.saturating_add(directory_physical_bytes(&entry.path())?);
        } else {
            total = total.saturating_add(metadata.len());
        }
    }
    Ok(total)
}

fn remove_orphan_partials(root: &Path) -> Result<(), ArtifactSinkError> {
    let mut paths = Vec::new();
    collect_partial_paths(root, &mut paths)?;
    let mut parents = BTreeSet::new();
    for path in paths {
        match std::fs::remove_file(&path) {
            Ok(()) => {
                if let Some(parent) = path.parent() {
                    parents.insert(parent.to_path_buf());
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    for parent in parents {
        std::fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}

fn collect_partial_paths(root: &Path, paths: &mut Vec<PathBuf>) -> Result<(), std::io::Error> {
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            collect_partial_paths(&path, paths)?;
        } else if path.file_name().is_some_and(|name| {
            let name = name.to_string_lossy();
            name.ends_with(".artifacts.partial") || name.ends_with(".artifacts.directory.partial")
        }) {
            paths.push(path);
        }
    }
    Ok(())
}

fn discover_segments(
    root: &Path,
) -> Result<(SegmentCatalog, ArtifactSegmentSinkStats), ArtifactSinkError> {
    let mut paths = Vec::new();
    collect_segment_paths(root, &mut paths)?;
    paths.sort();
    let mut entries = BTreeMap::new();
    let mut stats = ArtifactSegmentSinkStats::default();
    for path in paths {
        let inspected = ArtifactSegmentReader::inspect(&path)?;
        let descriptor = &inspected.metadata.descriptor;
        let range = descriptor.range;
        let instance = descriptor.processor_instance.clone();
        if entries.iter().any(
            |((candidate, _, _), entry): (&(String, u64, u64), &SegmentEntry)| {
                candidate == &instance
                    && entry.range.start() <= range.end()
                    && entry.range.end() >= range.start()
            },
        ) {
            return Err(ArtifactSinkError::Conflict(range));
        }
        let entry = SegmentEntry {
            range,
            path,
            artifacts: inspected.metadata.artifacts,
            logical_bytes: inspected.metadata.logical_bytes,
            physical_bytes: inspected.metadata.physical_bytes,
            first_hash: inspected.first_block_hash,
            last_hash: inspected.last_block_hash,
            records_checksum: inspected.metadata.records_checksum,
        };
        stats.segments = stats.segments.saturating_add(1);
        stats.artifacts = stats.artifacts.saturating_add(entry.artifacts);
        stats.logical_bytes = stats.logical_bytes.saturating_add(entry.logical_bytes);
        stats.physical_bytes = stats.physical_bytes.saturating_add(entry.physical_bytes);
        entries.insert((instance, range.start().0, range.end().0), entry);
    }
    Ok((entries, stats))
}

fn collect_segment_paths(root: &Path, paths: &mut Vec<PathBuf>) -> Result<(), std::io::Error> {
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            collect_segment_paths(&path, paths)?;
        } else if path
            .extension()
            .is_some_and(|extension| extension == "artifacts")
        {
            paths.push(path);
        }
    }
    Ok(())
}

fn contract_directory_name(descriptor: &ProcessorDescriptor) -> String {
    let mut hasher = blake3::Hasher::new();
    hash_contract_field(&mut hasher, descriptor.instance.as_str().as_bytes());
    hash_contract_field(&mut hasher, descriptor.id.as_str().as_bytes());
    hash_contract_field(&mut hasher, descriptor.version.to_string().as_bytes());
    hash_contract_field(&mut hasher, &descriptor.code_hash.0);
    hash_contract_field(&mut hasher, &descriptor.config_hash.0);
    hash_contract_field(&mut hasher, &descriptor.schemas.delta_version.to_be_bytes());
    hasher.finalize().to_hex().to_string()
}

fn hash_contract_field(hasher: &mut blake3::Hasher, bytes: &[u8]) {
    hasher.update(&u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_be_bytes());
    hasher.update(bytes);
}

fn hash_record(hasher: &mut blake3::Hasher, bytes: &[u8]) {
    hasher.update(&u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_be_bytes());
    hasher.update(bytes);
}

/// Artifact sink failure.
#[derive(Debug, Error)]
pub enum ArtifactSinkError {
    #[error("artifact sink configuration is invalid")]
    InvalidConfig,
    #[error("artifact sink received an empty batch")]
    EmptyBatch,
    #[error("artifact sink batch is invalid: {0}")]
    Batch(String),
    #[error("artifact sink conflicts with retained range {0:?}")]
    Conflict(BlockRange),
    #[error("artifact sink coverage has a gap at block {0}")]
    CoverageGap(BlockNumber),
    #[error("artifact sink scan limit {0} is outside 1..=10000")]
    InvalidScanLimit(usize),
    #[error("artifact sink would use {projected} physical bytes, exceeding {limit}")]
    AggregatePhysicalLimit { projected: u64, limit: u64 },
    #[error("artifact segment failed: {0}")]
    Segment(#[from] ArtifactSegmentError),
    #[error("artifact sink I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("artifact sink blocking task failed: {0}")]
    Task(String),
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

    fn config() -> ArtifactSegmentSinkConfig {
        ArtifactSegmentSinkConfig {
            compression: ArtifactCompression::Snappy,
            limits: ArtifactSegmentLimits {
                maximum_artifact_logical_bytes: 1024 * 1024,
                maximum_segment_logical_bytes: 16 * 1024 * 1024,
                maximum_segment_physical_bytes: 16 * 1024 * 1024,
            },
            maximum_retained_physical_bytes: 32 * 1024 * 1024,
        }
    }

    #[tokio::test]
    async fn direct_sink_is_idempotent_scannable_and_restart_adoptable() {
        let processor = BlockLocalCounter::default();
        let all = deltas(
            &processor,
            BlockRange::new(BlockNumber(1), BlockNumber(8)).expect("range"),
        );
        let directory = tempfile::tempdir().expect("tempdir");
        let sink = ArtifactSegmentSink::open(directory.path(), config()).expect("sink");
        let first = sink
            .retain_finalized_batch(processor.descriptor(), &all[..4])
            .await
            .expect("retain first");
        assert!(first.newly_retained);
        let second = sink
            .retain_finalized_batch(processor.descriptor(), &all[4..])
            .await
            .expect("retain second");
        assert!(second.newly_retained);
        let retry = sink
            .retain_finalized_batch(processor.descriptor(), &all[..4])
            .await
            .expect("idempotent retry");
        assert!(!retry.newly_retained);
        assert_eq!(retry.physical_bytes, first.physical_bytes);
        assert_eq!(sink.stats().await.segments, 2);
        assert_eq!(sink.stats().await.artifacts, 8);
        let scanned = sink
            .scan(
                processor.descriptor(),
                BlockRange::new(BlockNumber(2), BlockNumber(7)).expect("scan range"),
                4,
            )
            .await
            .expect("scan");
        assert_eq!(scanned, all[1..5]);
        sink.verify(processor.descriptor()).await.expect("verify");

        let reopened = ArtifactSegmentSink::open(directory.path(), config()).expect("reopen");
        assert_eq!(reopened.stats().await.segments, 2);
        assert_eq!(reopened.stats().await.artifacts, 8);
        assert_eq!(
            reopened
                .scan(
                    processor.descriptor(),
                    BlockRange::new(BlockNumber(2), BlockNumber(7)).expect("scan range"),
                    10,
                )
                .await
                .expect("restart scan"),
            all[1..7]
        );
        let adopted = reopened
            .retain_finalized_batch(processor.descriptor(), &all[..4])
            .await
            .expect("adopt existing");
        assert!(!adopted.newly_retained);
        assert_eq!(reopened.stats().await.segments, 2);
        assert_eq!(reopened.stats().await.artifacts, 8);
        assert!(
            reopened
                .remove_exact(
                    processor.descriptor().instance.as_str(),
                    BlockRange::new(BlockNumber(1), BlockNumber(4)).expect("remove range"),
                )
                .await
                .expect("remove first")
        );
        assert_eq!(reopened.stats().await.segments, 1);
        assert!(
            !reopened
                .remove_exact(
                    processor.descriptor().instance.as_str(),
                    BlockRange::new(BlockNumber(1), BlockNumber(4)).expect("remove range"),
                )
                .await
                .expect("idempotent remove")
        );
    }

    #[tokio::test]
    async fn direct_sink_rejects_overlap_conflict_and_hard_limit() {
        let processor = BlockLocalCounter::default();
        let all = deltas(
            &processor,
            BlockRange::new(BlockNumber(1), BlockNumber(8)).expect("range"),
        );
        let directory = tempfile::tempdir().expect("tempdir");
        let sink = ArtifactSegmentSink::open(directory.path(), config()).expect("sink");
        sink.retain_finalized_batch(processor.descriptor(), &all[..4])
            .await
            .expect("retain first");
        assert!(matches!(
            sink.retain_finalized_batch(processor.descriptor(), &all[2..6])
                .await,
            Err(ArtifactSinkError::Conflict(_))
        ));
        let mut conflicting = all[..4].to_vec();
        let mut payload = conflicting[0].payload.clone();
        payload[0] ^= 0xff;
        conflicting[0] = EncodedDelta::new(
            processor.descriptor(),
            ChainId(1),
            conflicting[0].block,
            payload,
        );
        assert!(matches!(
            sink.retain_finalized_batch(processor.descriptor(), &conflicting)
                .await,
            Err(ArtifactSinkError::Conflict(_))
        ));

        let limited_directory = tempfile::tempdir().expect("limited tempdir");
        let limited = ArtifactSegmentSink::open(
            limited_directory.path(),
            ArtifactSegmentSinkConfig {
                compression: ArtifactCompression::None,
                limits: ArtifactSegmentLimits {
                    maximum_artifact_logical_bytes: 1024,
                    maximum_segment_logical_bytes: 1024,
                    maximum_segment_physical_bytes: 1024,
                },
                maximum_retained_physical_bytes: 1024,
            },
        )
        .expect("limited sink");
        assert!(
            limited
                .retain_finalized_batch(processor.descriptor(), &all[..4])
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn restart_removes_orphan_partial_publications_before_budget_admission() {
        let processor = BlockLocalCounter::default();
        let range = BlockRange::new(BlockNumber(1), BlockNumber(4)).expect("range");
        let all = deltas(&processor, range);
        let directory = tempfile::tempdir().expect("tempdir");
        let processor_directory = directory
            .path()
            .join(contract_directory_name(processor.descriptor()));
        let path = processor_directory.join("00000000000000000001-00000000000000000004.artifacts");
        let mut writer = ArtifactSegmentWriter::create(
            &path,
            processor.descriptor(),
            ChainId(1),
            range,
            ArtifactCompression::None,
            config().limits,
        )
        .expect("writer");
        writer.append(&all[0]).expect("append first artifact");
        drop(writer);

        let partial = path.with_file_name(format!(
            "{}.partial",
            path.file_name().expect("file name").to_string_lossy()
        ));
        let directory_partial = path.with_file_name(format!(
            "{}.directory.partial",
            path.file_name().expect("file name").to_string_lossy()
        ));
        assert!(partial.exists());
        assert!(directory_partial.exists());

        let mut constrained = config();
        constrained.maximum_retained_physical_bytes =
            constrained.limits.maximum_segment_physical_bytes;
        let reopened = ArtifactSegmentSink::open(directory.path(), constrained)
            .expect("orphan cleanup precedes budget admission");
        assert!(!partial.exists());
        assert!(!directory_partial.exists());
        assert_eq!(reopened.stats().await, ArtifactSegmentSinkStats::default());

        let retained = reopened
            .retain_finalized_batch(processor.descriptor(), &all)
            .await
            .expect("retry after restart");
        assert!(retained.newly_retained);
    }
}
