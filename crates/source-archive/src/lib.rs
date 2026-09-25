//! Checksummed local normalized-frame archive history source.
//!
//! Public dataset adapters can project into newline-delimited [`BlockFrame`]
//! objects once, after which the bounded scheduler and every processor run
//! without source-specific code or network access.

use std::{
    fs,
    io::Read,
    path::{Component, Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use futures::stream;
use leani_primitives::{
    BlockFrame, BlockNumber, BlockRange, Capability, CapabilitySet, ChainId, CheckStatus, Finality,
    ObjectIdentity, Provenance, SourceId, SourceKind, TrustModel, VerificationCheck,
    VerificationReport,
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
        let root = self.root.clone();
        let descriptor = self.descriptor.clone();
        let chunk_range = chunk.range;
        let frames = tokio::task::spawn_blocking(move || {
            read_object(&root, &object, chunk_range, budget, &descriptor)
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

/// Open an object below `root`, refusing a path that crosses a symbolic
/// link below it, so a manifest cannot reach files outside the archive.
fn open_object(root: &Path, relative: &Path) -> Result<(PathBuf, fs::File), SourceError> {
    let mut path = root.to_path_buf();
    for component in relative.components() {
        path.push(component);
        let metadata = fs::symlink_metadata(&path).map_err(|error| {
            SourceError::Unavailable(format!("read {}: {error}", path.display()))
        })?;
        if metadata.file_type().is_symlink() {
            return Err(SourceError::InvalidPlan(format!(
                "archive object path {} crosses a symbolic link",
                relative.display()
            )));
        }
    }
    let file = fs::File::open(&path)
        .map_err(|error| SourceError::Unavailable(format!("read {}: {error}", path.display())))?;
    Ok((path, file))
}

#[allow(clippy::too_many_lines)]
fn read_object(
    root: &Path,
    object: &ValidatedObject,
    range: BlockRange,
    budget: SourceBudget,
    descriptor: &SourceDescriptor,
) -> Result<Vec<BlockFrame>, SourceError> {
    // The manifest declares the chain and finality of everything in the
    // archive; a frame that claims otherwise is corrupt.
    let finality = match descriptor.finality {
        FinalityModel::Finalized => Finality::Finalized,
        FinalityModel::Included => Finality::Included,
        FinalityModel::None => {
            return Err(SourceError::InvalidPlan(
                "the archive manifest declares no finality".to_owned(),
            ));
        }
    };
    let (path, file) = open_object(root, &object.path)?;
    let unavailable = |error: std::io::Error| {
        SourceError::Unavailable(format!("read {}: {error}", path.display()))
    };
    let size_mismatch = |observed: u64| {
        SourceError::CorruptFrame(format!(
            "archive object size mismatch: expected {}, observed {observed}",
            object.bytes
        ))
    };
    let size = file.metadata().map_err(unavailable)?.len();
    if size != object.bytes {
        return Err(size_mismatch(size));
    }
    let mut bytes = Vec::with_capacity(usize::try_from(object.bytes).unwrap_or(0));
    file.take(object.bytes)
        .read_to_end(&mut bytes)
        .map_err(unavailable)?;
    let observed_bytes = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if observed_bytes != object.bytes {
        return Err(size_mismatch(observed_bytes));
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
        if frame.chain_id != descriptor.chain_id {
            return Err(SourceError::CorruptFrame(format!(
                "archive frame {} names chain {}, but the manifest chain is {}",
                frame.block.number.0, frame.chain_id.0, descriptor.chain_id.0
            )));
        }
        if frame.finality != finality {
            return Err(SourceError::CorruptFrame(format!(
                "archive frame {} is {}, but the manifest declares {} blocks",
                frame.block.number.0,
                frame.finality.name(),
                finality.name()
            )));
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
        // The archive checks its object's digest and, below, the parent
        // links between its frames. A frame's own verification claims and
        // consensus anchor came from whoever wrote it, and so does the trust
        // of its earlier provenance.
        frame.verification = VerificationReport {
            dataset_checksum: VerificationCheck {
                status: CheckStatus::Verified,
                detail: Some("BLAKE3 of the archive object matches its manifest".to_owned()),
            },
            ..VerificationReport::default()
        };
        for provenance in &mut frame.provenance {
            provenance.trust = provenance.trust.min(descriptor.trust);
        }
        frame.provenance.push(Provenance {
            source_id: descriptor.id.clone(),
            source_kind: SourceKind::HistoryArchive,
            trust: descriptor.trust,
            range: Some(range),
            object: Some(ObjectIdentity {
                locator: path.display().to_string(),
                version: None,
                checksum: Some(object.checksum),
                schema: Some(descriptor.schema_version.clone()),
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
    for index in 1..frames.len() {
        let (previous, current) = (&frames[index - 1], &frames[index]);
        if current.block.number.0 != previous.block.number.0.saturating_add(1)
            || current.block.parent_hash != previous.block.hash
        {
            return Err(SourceError::CorruptFrame(
                "archive frames are not a contiguous parent-linked sequence".to_owned(),
            ));
        }
        frames[index].verification.parent_continuity = VerificationCheck::VERIFIED;
    }
    Ok(frames)
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use futures::StreamExt;
    use leani_primitives::{BlockHash, CheckStatus, Finality, VerificationCheck};
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

    /// Write `frames` as one object and a manifest for it under `directory`,
    /// with the object at `object_path`.
    fn write_archive(
        directory: &Path,
        object_path: &str,
        frames: &[BlockFrame],
        finality: FinalityModel,
    ) -> PathBuf {
        let mut object = Vec::new();
        for frame in frames {
            object.extend_from_slice(&serde_json::to_vec(frame).expect("JSON"));
            object.push(b'\n');
        }
        let path = directory.join(object_path);
        fs::create_dir_all(path.parent().expect("object directory")).expect("directory");
        fs::write(&path, &object).expect("write object");
        let manifest = ArchiveManifest {
            format_version: 1,
            id: "local-fixture".to_owned(),
            chain_id: 1,
            schema_version: "normalized-frame-jsonl.v1".to_owned(),
            capabilities: vec![Capability::Transactions],
            complete_capabilities: vec![Capability::Transactions],
            finality,
            objects: vec![ArchiveObject {
                from_block: frames[0].block.number.0,
                to_block: frames[frames.len() - 1].block.number.0,
                path: object_path.to_owned(),
                blake3: blake3::hash(&object).to_hex().to_string(),
                bytes: u64::try_from(object.len()).expect("size"),
            }],
        };
        let manifest_path = directory.join("manifest.json");
        fs::write(
            &manifest_path,
            serde_json::to_vec(&manifest).expect("manifest"),
        )
        .expect("write manifest");
        manifest_path
    }

    async fn read_archive(
        manifest: &Path,
        range: BlockRange,
        minimum_finality: Finality,
    ) -> Result<Vec<BlockFrame>, SourceError> {
        let source = LocalArchiveSource::open_manifest(manifest)?;
        let plan = source
            .plan(&DataRequest {
                chain_id: ChainId(1),
                range,
                required: CapabilitySet::of(Capability::Transactions),
                allow_filtered: false,
                projection: FieldProjection::default(),
                log_fields: leani_primitives::LogFieldSet::NONE,
                filters: FilterSet::default(),
                minimum_finality,
                verification_policy: VerificationPolicy::TrustedDataset,
            })
            .await?;
        let mut frames = Vec::new();
        for chunk in &plan.chunks {
            let mut stream = source
                .open(
                    chunk,
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
                .await?;
            while let Some(frame) = stream.next().await {
                frames.push(frame?);
            }
        }
        Ok(frames)
    }

    fn blocks_one_and_two() -> (BlockFrame, BlockFrame) {
        let first = frame(1, BlockHash::ZERO);
        let second = frame(2, first.block.hash);
        (first, second)
    }

    #[tokio::test]
    async fn frames_must_carry_the_manifest_chain_and_finality() {
        let range = BlockRange::new(BlockNumber(1), BlockNumber(2)).expect("range");
        // Audit History-5: each frame's own chain and finality were trusted.
        let (first, mut foreign) = blocks_one_and_two();
        foreign.chain_id = ChainId(5);
        let directory = tempfile::tempdir().expect("tempdir");
        let manifest = write_archive(
            directory.path(),
            "frames.jsonl",
            &[first, foreign],
            FinalityModel::Finalized,
        );
        assert!(matches!(
            read_archive(&manifest, range, Finality::Finalized).await,
            Err(SourceError::CorruptFrame(_))
        ));

        let (first, mut included) = blocks_one_and_two();
        included.finality = Finality::Included;
        let directory = tempfile::tempdir().expect("tempdir");
        let manifest = write_archive(
            directory.path(),
            "frames.jsonl",
            &[first, included],
            FinalityModel::Finalized,
        );
        assert!(matches!(
            read_archive(&manifest, range, Finality::Finalized).await,
            Err(SourceError::CorruptFrame(_))
        ));

        // An archive of included blocks cannot vouch for finality.
        let (first, second) = blocks_one_and_two();
        let directory = tempfile::tempdir().expect("tempdir");
        let manifest = write_archive(
            directory.path(),
            "frames.jsonl",
            &[first, second],
            FinalityModel::Included,
        );
        assert!(matches!(
            read_archive(&manifest, range, Finality::Included).await,
            Err(SourceError::CorruptFrame(_))
        ));
    }

    #[tokio::test]
    async fn frames_report_only_the_checks_the_archive_made() {
        let (first, mut second) = blocks_one_and_two();
        second.verification = leani_primitives::VerificationReport {
            header_hash: VerificationCheck::VERIFIED,
            parent_continuity: VerificationCheck::VERIFIED,
            transactions_root: VerificationCheck::VERIFIED,
            receipts_root: VerificationCheck::VERIFIED,
            withdrawals_root: VerificationCheck::VERIFIED,
            dataset_checksum: VerificationCheck::NOT_CHECKED,
            consensus_anchor: Some(leani_primitives::ConsensusAnchor {
                finality: Finality::Finalized,
                execution_block_hash: second.block.hash,
                beacon_slot: 7,
                beacon_block_root: [7; 32],
            }),
        };
        second.provenance.push(Provenance {
            source_id: SourceId::new("upstream").expect("source ID"),
            source_kind: SourceKind::ExecutionP2p,
            trust: TrustModel::ProtocolVerified,
            range: None,
            object: None,
            observed_at_unix_ms: 0,
            projection: Vec::new(),
        });
        let directory = tempfile::tempdir().expect("tempdir");
        let manifest = write_archive(
            directory.path(),
            "frames.jsonl",
            &[first, second],
            FinalityModel::Finalized,
        );
        let frames = read_archive(
            &manifest,
            BlockRange::new(BlockNumber(1), BlockNumber(2)).expect("range"),
            Finality::Finalized,
        )
        .await
        .expect("frames");
        // Audit History-5: the object's own verification claims were
        // passed through.
        let checks = &frames[1].verification;
        for (name, check) in [
            ("header hash", &checks.header_hash),
            ("transactions root", &checks.transactions_root),
            ("receipts root", &checks.receipts_root),
            ("withdrawals root", &checks.withdrawals_root),
        ] {
            assert_eq!(check.status, CheckStatus::NotChecked, "{name}");
        }
        assert_eq!(checks.consensus_anchor, None);
        assert_eq!(checks.dataset_checksum.status, CheckStatus::Verified);
        assert_eq!(checks.parent_continuity.status, CheckStatus::Verified);
        assert_eq!(
            frames[0].verification.parent_continuity.status,
            CheckStatus::NotChecked,
            "the first frame's parent is outside the object"
        );
        assert!(
            frames[1]
                .provenance
                .iter()
                .all(|entry| entry.trust <= TrustModel::TrustedDataset),
            "{:?}",
            frames[1].provenance
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn objects_behind_symbolic_links_are_refused() {
        let range = BlockRange::new(BlockNumber(1), BlockNumber(2)).expect("range");
        let (first, second) = blocks_one_and_two();
        let outside = tempfile::tempdir().expect("outside");
        write_archive(
            outside.path(),
            "frames.jsonl",
            &[first, second],
            FinalityModel::Finalized,
        );
        let directory = tempfile::tempdir().expect("tempdir");
        let (first, second) = blocks_one_and_two();
        let manifest = write_archive(
            directory.path(),
            "frames.jsonl",
            &[first, second],
            FinalityModel::Finalized,
        );
        read_archive(&manifest, range, Finality::Finalized)
            .await
            .expect("a regular object");

        // Audit History-5: the object path followed a symbolic link out of
        // the archive directory.
        fs::remove_file(directory.path().join("frames.jsonl")).expect("remove object");
        std::os::unix::fs::symlink(
            outside.path().join("frames.jsonl"),
            directory.path().join("frames.jsonl"),
        )
        .expect("symlinked object");
        assert!(matches!(
            read_archive(&manifest, range, Finality::Finalized).await,
            Err(SourceError::InvalidPlan(detail)) if detail.contains("symbolic link")
        ));

        // A linked directory on the way to the object is refused as well.
        let directory = tempfile::tempdir().expect("tempdir");
        let (first, second) = blocks_one_and_two();
        let manifest = write_archive(
            directory.path(),
            "nested/frames.jsonl",
            &[first, second],
            FinalityModel::Finalized,
        );
        fs::remove_dir_all(directory.path().join("nested")).expect("remove directory");
        std::os::unix::fs::symlink(outside.path(), directory.path().join("nested"))
            .expect("symlinked directory");
        assert!(matches!(
            read_archive(&manifest, range, Finality::Finalized).await,
            Err(SourceError::InvalidPlan(detail)) if detail.contains("symbolic link")
        ));
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

    /// The source budget's counters, each tripped at `open`: bytes acquired
    /// from the object, bytes of each frame, and frames emitted.
    #[tokio::test]
    async fn budget_contract_names_each_exceeded_counter() {
        let directory = tempfile::tempdir().expect("tempdir");
        let (mut first, second) = blocks_one_and_two();
        first.withdrawals =
            leani_primitives::Material::Complete(vec![leani_primitives::Withdrawal {
                index: 0,
                validator_index: 1,
                address: leani_primitives::Address::new([0; 20]),
                amount_gwei: 1,
            }]);
        let manifest = write_archive(
            directory.path(),
            "frames.jsonl",
            &[first.clone(), second],
            FinalityModel::Finalized,
        );
        let source = LocalArchiveSource::open_manifest(&manifest).expect("source");
        let plan = source
            .plan(&DataRequest {
                chain_id: ChainId(1),
                range: BlockRange::new(BlockNumber(1), BlockNumber(2)).expect("range"),
                required: CapabilitySet::of(Capability::Transactions),
                allow_filtered: false,
                projection: FieldProjection::default(),
                log_fields: leani_primitives::LogFieldSet::NONE,
                filters: FilterSet::default(),
                minimum_finality: Finality::Finalized,
                verification_policy: VerificationPolicy::TrustedDataset,
            })
            .await
            .expect("plan");
        let object_bytes = plan.chunks[0].estimated_bytes.expect("object size");
        let budget = SourceBudget {
            max_input_bytes: 1_000_000,
            max_frame_bytes: 100_000,
            max_frames: 10,
            max_buffered_frames: 2,
            max_in_flight_requests: 1,
            temporary_disk_bytes: 0,
        };
        for (counter, budget, expected) in [
            ("none", budget, None),
            (
                "acquired",
                SourceBudget {
                    max_input_bytes: object_bytes - 1,
                    ..budget
                },
                Some("input_bytes"),
            ),
            (
                "frame",
                SourceBudget {
                    max_frame_bytes: first.estimated_heap_bytes() - 1,
                    ..budget
                },
                Some("frame_bytes"),
            ),
            (
                "emitted",
                SourceBudget {
                    max_frames: 1,
                    ..budget
                },
                Some("frames"),
            ),
        ] {
            let outcome: Result<Vec<BlockFrame>, SourceError> = match source
                .open(&plan.chunks[0], budget, CancellationToken::new())
                .await
            {
                Ok(stream) => stream.collect::<Vec<_>>().await.into_iter().collect(),
                Err(error) => Err(error),
            };
            match (outcome, expected) {
                (Ok(frames), None) => assert_eq!(frames.len(), 2, "{counter}"),
                (
                    Err(SourceError::BudgetExceeded {
                        resource,
                        limit,
                        observed,
                    }),
                    Some(expected),
                ) => {
                    assert_eq!(resource, expected, "{counter}");
                    assert!(observed > limit, "{counter}: {observed} <= {limit}");
                }
                (outcome, _) => panic!("{counter}: {:?}", outcome.map(|frames| frames.len())),
            }
        }
    }
}
