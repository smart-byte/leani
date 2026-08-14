//! Checksummed local normalized-frame archive history source.
//!
//! Public dataset adapters can project into newline-delimited [`BlockFrame`]
//! objects once, after which the bounded scheduler and every processor run
//! without source-specific code or network access.

use std::{
    fs,
    path::{Component, Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use futures::stream;
use leani_primitives::{
    BlockFrame, BlockNumber, BlockRange, Capability, CapabilitySet, ChainId, ObjectIdentity,
    Provenance, SourceId, SourceKind, TrustModel,
};
use leani_source_api::{
    BlockFrameStream, DataRequest, FinalityModel, HistorySource, Partitioning, SourceBudget,
    SourceChunk, SourceDescriptor, SourceError, SourcePlan,
};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

/// Portable archive catalog.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveManifest {
    pub format_version: u32,
    pub id: String,
    pub chain_id: u64,
    pub schema_version: String,
    pub capabilities: Vec<Capability>,
    pub complete_capabilities: Vec<Capability>,
    pub finality: FinalityModel,
    pub objects: Vec<ArchiveObject>,
}

/// One immutable JSON-lines object.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveObject {
    pub from_block: u64,
    pub to_block: u64,
    pub path: String,
    /// Lowercase BLAKE3 digest without a prefix.
    pub blake3: String,
    pub bytes: u64,
}

#[derive(Clone, Debug)]
struct ValidatedObject {
    range: BlockRange,
    path: PathBuf,
    checksum: [u8; 32],
    bytes: u64,
}

/// Immutable local archive source loaded from one manifest.
#[derive(Clone, Debug)]
pub struct LocalArchiveSource {
    root: PathBuf,
    descriptor: SourceDescriptor,
    objects: Arc<Vec<ValidatedObject>>,
}

impl LocalArchiveSource {
    /// Load and validate a manifest without reading any data object.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed JSON, unsafe paths, invalid checksums,
    /// overlapping objects, or invalid descriptor fields.
    pub fn open_manifest(path: impl AsRef<Path>) -> Result<Self, SourceError> {
        let path = path.as_ref();
        let bytes = fs::read(path)
            .map_err(|error| SourceError::Unavailable(format!("read manifest: {error}")))?;
        let manifest: ArchiveManifest =
            serde_json::from_slice(&bytes).map_err(|error| SourceError::SchemaDrift {
                expected: "normalized-frame-archive-manifest.v1".to_owned(),
                actual: error.to_string(),
            })?;
        if manifest.format_version != 1
            || manifest.chain_id == 0
            || manifest.schema_version.trim().is_empty()
            || manifest.objects.is_empty()
        {
            return Err(SourceError::InvalidPlan(
                "archive manifest identity, chain, schema, and objects are required".to_owned(),
            ));
        }
        let root = path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();
        let mut objects = manifest
            .objects
            .iter()
            .map(validate_object)
            .collect::<Result<Vec<_>, _>>()?;
        objects.sort_by_key(|object| object.range.start().0);
        for pair in objects.windows(2) {
            if pair[0].range.end().0 >= pair[1].range.start().0 {
                return Err(SourceError::InvalidPlan(
                    "archive objects overlap".to_owned(),
                ));
            }
        }
        let (Some(first), Some(last)) = (objects.first(), objects.last()) else {
            return Err(SourceError::InvalidPlan(
                "archive manifest has no objects".to_owned(),
            ));
        };
        let range = BlockRange::new(first.range.start(), last.range.end())
            .map_err(|error| SourceError::InvalidPlan(error.to_string()))?;
        let capabilities = CapabilitySet::from_iter(manifest.capabilities);
        let complete_capabilities = CapabilitySet::from_iter(manifest.complete_capabilities);
        if !capabilities.contains_all(complete_capabilities) {
            return Err(SourceError::InvalidPlan(
                "complete capabilities must be a subset of capabilities".to_owned(),
            ));
        }
        Ok(Self {
            root,
            descriptor: SourceDescriptor {
                id: SourceId::new(&manifest.id)
                    .map_err(|error| SourceError::InvalidPlan(error.to_string()))?,
                kind: SourceKind::HistoryArchive,
                chain_id: ChainId(manifest.chain_id),
                range: Some(range),
                capabilities,
                complete_capabilities,
                trust: TrustModel::TrustedDataset,
                finality: manifest.finality,
                partitioning: Partitioning::DatasetObjects,
                expected_lag: Duration::ZERO,
                schema_version: manifest.schema_version,
                priority: 20,
            },
            objects: Arc::new(objects),
        })
    }
}

fn validate_object(object: &ArchiveObject) -> Result<ValidatedObject, SourceError> {
    let path = Path::new(&object.path);
    if path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(SourceError::InvalidPlan(
            "archive object paths must be simple relative paths".to_owned(),
        ));
    }
    if object.bytes == 0 {
        return Err(SourceError::InvalidPlan(
            "archive object byte lengths must be non-zero".to_owned(),
        ));
    }
    let checksum = hex::decode(&object.blake3)
        .ok()
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| {
            SourceError::InvalidPlan(
                "archive object checksum must be 32 lowercase hexadecimal bytes".to_owned(),
            )
        })?;
    if object.blake3.bytes().any(|byte| byte.is_ascii_uppercase()) {
        return Err(SourceError::InvalidPlan(
            "archive object checksum must be lowercase".to_owned(),
        ));
    }
    Ok(ValidatedObject {
        range: BlockRange::new(BlockNumber(object.from_block), BlockNumber(object.to_block))
            .map_err(|error| SourceError::InvalidPlan(error.to_string()))?,
        path: path.to_path_buf(),
        checksum,
        bytes: object.bytes,
    })
}

#[async_trait]
impl HistorySource for LocalArchiveSource {
    fn descriptor(&self) -> &SourceDescriptor {
        &self.descriptor
    }

    async fn plan(&self, request: &DataRequest) -> Result<SourcePlan, SourceError> {
        if request.chain_id != self.descriptor.chain_id
            || !self.descriptor.capabilities.contains_all(request.required)
            || (!request.allow_filtered
                && !self
                    .descriptor
                    .complete_capabilities
                    .contains_all(request.required))
            || !self.descriptor.finality.supports(request.minimum_finality)
        {
            return Err(SourceError::InvalidPlan(
                "archive cannot satisfy the requested chain, capabilities, or finality".to_owned(),
            ));
        }
        let mut chunks = Vec::new();
        let mut next = request.range.start().0;
        let mut estimated_bytes = 0_u64;
        for (index, object) in self.objects.iter().enumerate() {
            if object.range.end().0 < next || object.range.start().0 > request.range.end().0 {
                continue;
            }
            if object.range.start().0 > next {
                return Err(SourceError::MissingRange(
                    BlockRange::new(
                        BlockNumber(next),
                        BlockNumber(object.range.start().0.saturating_sub(1)),
                    )
                    .map_err(|error| SourceError::InvalidPlan(error.to_string()))?,
                ));
            }
            let end = object.range.end().0.min(request.range.end().0);
            chunks.push(SourceChunk {
                source_id: self.descriptor.id.clone(),
                range: BlockRange::new(BlockNumber(next), BlockNumber(end))
                    .map_err(|error| SourceError::InvalidPlan(error.to_string()))?,
                partition: u64::try_from(index)
                    .map_err(|_| SourceError::InvalidPlan("too many archive objects".to_owned()))?
                    .to_be_bytes()
                    .to_vec(),
                schema_version: self.descriptor.schema_version.clone(),
                expected_parent: None,
                estimated_bytes: Some(object.bytes),
            });
            estimated_bytes = estimated_bytes.saturating_add(object.bytes);
            next = end.saturating_add(1);
            if next > request.range.end().0 {
                break;
            }
        }
        if next <= request.range.end().0 {
            return Err(SourceError::MissingRange(
                BlockRange::new(BlockNumber(next), request.range.end())
                    .map_err(|error| SourceError::InvalidPlan(error.to_string()))?,
            ));
        }
        let plan = SourcePlan {
            source_id: self.descriptor.id.clone(),
            request: request.clone(),
            chunks,
            estimated_bytes: Some(estimated_bytes),
            estimated_lag: Duration::ZERO,
            supplied: self.descriptor.capabilities,
            complete: self.descriptor.complete_capabilities,
            trust: self.descriptor.trust,
            schema_version: self.descriptor.schema_version.clone(),
            physical_plan: Vec::new(),
        };
        plan.validate()?;
        Ok(plan)
    }

    async fn open(
        &self,
        chunk: &SourceChunk,
        budget: SourceBudget,
        cancellation: CancellationToken,
    ) -> Result<BlockFrameStream, SourceError> {
        let budget = budget.validate()?;
        if chunk.source_id != self.descriptor.id
            || chunk.schema_version != self.descriptor.schema_version
        {
            return Err(SourceError::InvalidPlan(
                "archive chunk identity or schema changed".to_owned(),
            ));
        }
        let partition: [u8; 8] = chunk.partition.as_slice().try_into().map_err(|_| {
            SourceError::InvalidPlan("archive partition must be eight bytes".to_owned())
        })?;
        let index = usize::try_from(u64::from_be_bytes(partition))
            .map_err(|_| SourceError::InvalidPlan("archive partition is too large".to_owned()))?;
        let object = self
            .objects
            .get(index)
            .ok_or_else(|| SourceError::InvalidPlan("unknown archive partition".to_owned()))?
            .clone();
        if object.bytes > budget.max_input_bytes {
            return Err(SourceError::BudgetExceeded {
                resource: "input_bytes",
                limit: budget.max_input_bytes,
                observed: object.bytes,
            });
        }
        if cancellation.is_cancelled() {
            return Err(SourceError::Cancelled);
        }
        let path = self.root.join(&object.path);
        let source_id = self.descriptor.id.clone();
        let source_schema = self.descriptor.schema_version.clone();
        let source_trust = self.descriptor.trust;
        let chunk_range = chunk.range;
        let frames = tokio::task::spawn_blocking(move || {
            read_object(
                &path,
                &object,
                chunk_range,
                budget,
                &source_id,
                &source_schema,
                source_trust,
            )
        })
        .await
        .map_err(|error| {
            SourceError::Unavailable(format!("archive reader task failed: {error}"))
        })??;
        if cancellation.is_cancelled() {
            return Err(SourceError::Cancelled);
        }
        Ok(Box::pin(stream::iter(frames.into_iter().map(Ok))))
    }
}

#[allow(clippy::too_many_arguments)]
fn read_object(
    path: &Path,
    object: &ValidatedObject,
    range: BlockRange,
    budget: SourceBudget,
    source_id: &SourceId,
    schema: &str,
    trust: TrustModel,
) -> Result<Vec<BlockFrame>, SourceError> {
    let bytes = fs::read(path)
        .map_err(|error| SourceError::Unavailable(format!("read {}: {error}", path.display())))?;
    let observed_bytes = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if observed_bytes != object.bytes {
        return Err(SourceError::CorruptFrame(format!(
            "archive object size mismatch: expected {}, observed {observed_bytes}",
            object.bytes
        )));
    }
    if *blake3::hash(&bytes).as_bytes() != object.checksum {
        return Err(SourceError::CorruptFrame(
            "archive object checksum mismatch".to_owned(),
        ));
    }
    let mut frames = Vec::new();
    let mut frame_bytes = 0_u64;
    for (line_number, line) in bytes.split(|byte| *byte == b'\n').enumerate() {
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let mut frame: BlockFrame = serde_json::from_slice(line).map_err(|error| {
            SourceError::CorruptFrame(format!(
                "{} line {}: {error}",
                path.display(),
                line_number.saturating_add(1)
            ))
        })?;
        if !range.contains(frame.block.number) {
            continue;
        }
        frame
            .validate_shape()
            .map_err(|error| SourceError::CorruptFrame(error.to_owned()))?;
        let estimated = frame.estimated_heap_bytes();
        if estimated > budget.max_frame_bytes {
            return Err(SourceError::BudgetExceeded {
                resource: "frame_bytes",
                limit: budget.max_frame_bytes,
                observed: estimated,
            });
        }
        frame_bytes = frame_bytes.saturating_add(estimated);
        frame.provenance.push(Provenance {
            source_id: source_id.clone(),
            source_kind: SourceKind::HistoryArchive,
            trust,
            range: Some(range),
            object: Some(ObjectIdentity {
                locator: path.display().to_string(),
                version: None,
                checksum: Some(object.checksum),
                schema: Some(schema.to_owned()),
            }),
            observed_at_unix_ms: 0,
            projection: Vec::new(),
        });
        frames.push(frame);
    }
    let observed_frames = u64::try_from(frames.len()).unwrap_or(u64::MAX);
    if observed_frames > budget.max_frames {
        return Err(SourceError::BudgetExceeded {
            resource: "frames",
            limit: budget.max_frames,
            observed: observed_frames,
        });
    }
    if frame_bytes > budget.max_input_bytes {
        return Err(SourceError::BudgetExceeded {
            resource: "normalized_frame_bytes",
            limit: budget.max_input_bytes,
            observed: frame_bytes,
        });
    }
    if observed_frames != range.len() {
        return Err(SourceError::MissingRange(range));
    }
    for pair in frames.windows(2) {
        if pair[1].block.number.0 != pair[0].block.number.0.saturating_add(1)
            || pair[1].block.parent_hash != pair[0].block.hash
        {
            return Err(SourceError::CorruptFrame(
                "archive frames are not a contiguous parent-linked sequence".to_owned(),
            ));
        }
    }
    Ok(frames)
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use futures::StreamExt;
    use leani_primitives::{BlockHash, Finality};
    use leani_source_api::{FieldProjection, FilterSet, VerificationPolicy};

    use super::*;

    fn frame(number: u64, parent: BlockHash) -> BlockFrame {
        BlockFrame {
            chain_id: ChainId(1),
            block: leani_primitives::BlockRef {
                number: BlockNumber(number),
                hash: BlockHash::new([u8::try_from(number).unwrap_or(0); 32]),
                parent_hash: parent,
                timestamp: number,
            },
            finality: Finality::Finalized,
            header: leani_primitives::Material::Missing(
                leani_primitives::MissingReason::NotRequested,
            ),
            transactions: leani_primitives::Material::Complete(Vec::new()),
            receipts: leani_primitives::Material::Complete(Vec::new()),
            logs: leani_primitives::Material::Complete(Vec::new()),
            withdrawals: leani_primitives::Material::Missing(
                leani_primitives::MissingReason::NotRequested,
            ),
            blob_sidecars: leani_primitives::Material::Missing(
                leani_primitives::MissingReason::NotRequested,
            ),
            traces: leani_primitives::Material::Missing(
                leani_primitives::MissingReason::Unsupported,
            ),
            state_diffs: leani_primitives::Material::Missing(
                leani_primitives::MissingReason::Unsupported,
            ),
            provenance: Vec::new(),
            verification: leani_primitives::VerificationReport::default(),
        }
    }

    #[tokio::test]
    async fn manifest_source_verifies_and_streams_exact_ranges() {
        let directory = tempfile::tempdir().expect("tempdir");
        let object_path = directory.path().join("frames.jsonl");
        let first = frame(1, BlockHash::ZERO);
        let second = frame(2, first.block.hash);
        let mut object = fs::File::create(&object_path).expect("object");
        writeln!(object, "{}", serde_json::to_string(&first).expect("JSON")).expect("write");
        writeln!(object, "{}", serde_json::to_string(&second).expect("JSON")).expect("write");
        drop(object);
        let bytes = fs::read(&object_path).expect("read object");
        let manifest = ArchiveManifest {
            format_version: 1,
            id: "local-fixture".to_owned(),
            chain_id: 1,
            schema_version: "normalized-frame-jsonl.v1".to_owned(),
            capabilities: vec![
                Capability::Transactions,
                Capability::Receipts,
                Capability::Logs,
            ],
            complete_capabilities: vec![
                Capability::Transactions,
                Capability::Receipts,
                Capability::Logs,
            ],
            finality: FinalityModel::Finalized,
            objects: vec![ArchiveObject {
                from_block: 1,
                to_block: 2,
                path: "frames.jsonl".to_owned(),
                blake3: blake3::hash(&bytes).to_hex().to_string(),
                bytes: u64::try_from(bytes.len()).expect("size"),
            }],
        };
        let manifest_path = directory.path().join("manifest.json");
        fs::write(
            &manifest_path,
            serde_json::to_vec_pretty(&manifest).expect("manifest"),
        )
        .expect("write manifest");
        let source = LocalArchiveSource::open_manifest(&manifest_path).expect("source");
        let range = BlockRange::new(BlockNumber(1), BlockNumber(2)).expect("range");
        let plan = source
            .plan(&DataRequest {
                chain_id: ChainId(1),
                range,
                required: CapabilitySet::from_iter([
                    Capability::Transactions,
                    Capability::Receipts,
                ]),
                allow_filtered: false,
                projection: FieldProjection::default(),
                log_fields: leani_primitives::LogFieldSet::NONE,
                filters: FilterSet::default(),
                minimum_finality: Finality::Finalized,
                verification_policy: VerificationPolicy::CompleteCryptographic,
            })
            .await
            .expect("plan");
        let output = source
            .open(
                &plan.chunks[0],
                SourceBudget {
                    max_input_bytes: 1_000_000,
                    max_frame_bytes: 100_000,
                    max_frames: 10,
                    max_buffered_frames: 2,
                    max_in_flight_requests: 1,
                    temporary_disk_bytes: 0,
                },
                CancellationToken::new(),
            )
            .await
            .expect("open")
            .collect::<Vec<_>>()
            .await;
        assert_eq!(output.len(), 2);
        assert!(output.into_iter().all(|frame| frame.is_ok()));
    }

    #[test]
    fn rejects_parent_traversal_before_reading_objects() {
        let directory = tempfile::tempdir().expect("tempdir");
        let manifest = ArchiveManifest {
            format_version: 1,
            id: "unsafe".to_owned(),
            chain_id: 1,
            schema_version: "v1".to_owned(),
            capabilities: Vec::new(),
            complete_capabilities: Vec::new(),
            finality: FinalityModel::Finalized,
            objects: vec![ArchiveObject {
                from_block: 1,
                to_block: 1,
                path: "../secret".to_owned(),
                blake3: "00".repeat(32),
                bytes: 1,
            }],
        };
        let path = directory.path().join("manifest.json");
        fs::write(&path, serde_json::to_vec(&manifest).expect("JSON")).expect("write");
        assert!(LocalArchiveSource::open_manifest(path).is_err());
    }
}
