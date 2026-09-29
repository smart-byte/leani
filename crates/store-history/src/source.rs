use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use futures::stream;
use leani_primitives::{
    BlockHash, BlockNumber, BlockRange, Capability, CapabilitySet, ChainId, LogFieldSet, SourceId,
    SourceKind, TransactionHash, TrustModel,
};
use leani_source_api::{
    BlockFrameStream, DataRequest, FinalityModel, HistoryLookupCapabilities, HistorySource,
    LocatedTransaction, Partitioning, PhysicalPlanOperation, PhysicalReader, SourceBudget,
    SourceChunk, SourceDescriptor, SourceError, SourcePlan, VerificationPolicy, coverage_gaps,
};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::{
    HistoryStore, HistoryStoreError, MaterialShapeId, RawHistoryMaterialProfile, RawHistoryProfile,
    SegmentError, SegmentId, SegmentRead, SegmentRecord, VerificationClass,
};

const RETAINED_SCHEMA_VERSION: &str = "retained-raw-segment.v1";

/// One profile-specific view over the shared raw segment catalog.
#[derive(Clone, Debug)]
pub struct RetainedHistorySourceConfig {
    pub id: SourceId,
    pub chain_id: ChainId,
    pub material_shape: MaterialShapeId,
    pub capabilities: CapabilitySet,
    pub log_fields: LogFieldSet,
    pub verification: VerificationClass,
    pub trust: TrustModel,
    pub priority: u16,
    pub required_profile: RawHistoryProfile,
    pub material_profile: Option<RawHistoryMaterialProfile>,
}

impl RetainedHistorySourceConfig {
    /// Build the conventional highest-priority local source identity.
    ///
    /// # Errors
    ///
    /// Rejects a zero chain, empty/historical-invalid capabilities, or trust
    /// weaker than the advertised verification class.
    pub fn local(
        chain_id: ChainId,
        material_shape: MaterialShapeId,
        capabilities: CapabilitySet,
        verification: VerificationClass,
        trust: TrustModel,
    ) -> Result<Self, SourceError> {
        if chain_id.0 == 0
            || capabilities == CapabilitySet::NONE
            || capabilities.contains(Capability::Mempool)
            || trust < minimum_trust_for_verification(verification)
        {
            return Err(SourceError::InvalidPlan(
                "invalid retained-history source profile".to_owned(),
            ));
        }
        let shape_prefix = hex::encode(&material_shape.0[..6]);
        Ok(Self {
            id: SourceId::new(format!("retained-{chain_id}-{shape_prefix}"))
                .map_err(|error| SourceError::InvalidPlan(error.to_string()))?,
            chain_id,
            material_shape,
            capabilities,
            log_fields: LogFieldSet::ALL,
            verification,
            trust,
            priority: 0,
            required_profile: RawHistoryProfile::ProcessorReuse,
            material_profile: None,
        })
    }

    /// Describe the shape so a narrower log predicate can reuse its frames.
    /// Construction verifies that this profile hashes to `material_shape`.
    #[must_use]
    pub fn with_material_profile(mut self, profile: RawHistoryMaterialProfile) -> Self {
        self.material_profile = Some(profile);
        self
    }

    #[must_use]
    pub const fn requiring_profile(mut self, profile: RawHistoryProfile) -> Self {
        self.required_profile = profile;
        self
    }

    #[must_use]
    pub const fn with_log_fields(mut self, log_fields: LogFieldSet) -> Self {
        self.log_fields = log_fields;
        self
    }
}

/// Monotonic physical work performed by the local retained source.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RetainedHistorySourceStats {
    pub segment_opens: u64,
    pub record_reads: u64,
    pub stored_record_bytes: u64,
    pub decompressed_bytes: u64,
    pub cache_hits: u64,
    pub locator_uses: u64,
}

#[derive(Debug, Default)]
struct SourceStats {
    segment_opens: AtomicU64,
    record_reads: AtomicU64,
    stored_record_bytes: AtomicU64,
    decompressed_bytes: AtomicU64,
    cache_hits: AtomicU64,
    locator_uses: AtomicU64,
}

impl SourceStats {
    fn snapshot(&self) -> RetainedHistorySourceStats {
        RetainedHistorySourceStats {
            segment_opens: self.segment_opens.load(Ordering::Relaxed),
            record_reads: self.record_reads.load(Ordering::Relaxed),
            stored_record_bytes: self.stored_record_bytes.load(Ordering::Relaxed),
            decompressed_bytes: self.decompressed_bytes.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            locator_uses: self.locator_uses.load(Ordering::Relaxed),
        }
    }
}

/// Dynamic catalog-backed finalized history source.
#[derive(Clone, Debug)]
pub struct RetainedHistorySource {
    store: HistoryStore,
    config: RetainedHistorySourceConfig,
    descriptor: SourceDescriptor,
    stats: Arc<SourceStats>,
}

impl RetainedHistorySource {
    /// Construct a source without requiring existing coverage. Newly closed
    /// compatible segments become visible to later plans immediately.
    ///
    /// # Errors
    ///
    /// Rejects invalid profile identity or advertised guarantees.
    pub fn new(
        store: HistoryStore,
        config: RetainedHistorySourceConfig,
    ) -> Result<Self, SourceError> {
        if config.chain_id.0 == 0
            || config
                .material_profile
                .as_ref()
                .is_some_and(|profile| profile.shape_id() != config.material_shape)
            || config.capabilities == CapabilitySet::NONE
            || config.capabilities.contains(Capability::Mempool)
            || (config.log_fields != LogFieldSet::NONE
                && !config
                    .capabilities
                    .with_derivable()
                    .contains(Capability::Logs))
            || config.trust < minimum_trust_for_verification(config.verification)
        {
            return Err(SourceError::InvalidPlan(
                "invalid retained-history source profile".to_owned(),
            ));
        }
        let descriptor = SourceDescriptor {
            id: config.id.clone(),
            kind: SourceKind::RetainedHistory,
            chain_id: config.chain_id,
            // Coverage is dynamic and can contain gaps. The plan method is the
            // authoritative exact cover check.
            range: None,
            capabilities: config.capabilities,
            complete_capabilities: if config
                .material_profile
                .as_ref()
                .is_some_and(|profile| profile.allow_filtered)
            {
                CapabilitySet::NONE
            } else {
                config.capabilities
            },
            trust: config.trust,
            finality: FinalityModel::Finalized,
            partitioning: Partitioning::SourceDefined("raw-segment".to_owned()),
            expected_lag: Duration::ZERO,
            schema_version: RETAINED_SCHEMA_VERSION.to_owned(),
            priority: config.priority,
        };
        Ok(Self {
            store,
            config,
            descriptor,
            stats: Arc::new(SourceStats::default()),
        })
    }

    #[must_use]
    pub fn stats(&self) -> RetainedHistorySourceStats {
        self.stats.snapshot()
    }

    async fn compatible_segments(
        &self,
        request: &DataRequest,
    ) -> Result<(Vec<SegmentRecord>, CapabilitySet), SourceError> {
        let required_verification =
            verification_for_policy(request.verification_policy).max(self.config.verification);
        let required_trust = minimum_trust_for_policy(request.verification_policy);
        let mut available = CapabilitySet::NONE;
        let mut compatible = Vec::new();
        if !self.config.log_fields.contains_all(request.log_fields) {
            return Ok((compatible, available));
        }
        for record in self
            .store
            .segments()
            .await
            .map_err(|error| source_store_error(&error))?
        {
            let descriptor = &record.metadata.descriptor;
            if descriptor.chain_id != request.chain_id
                || descriptor.material_shape != self.config.material_shape
                || !record.profile.satisfies(self.config.required_profile)
                || descriptor.range.end().0 < request.range.start().0
                || descriptor.range.start().0 > request.range.end().0
            {
                continue;
            }
            let supplied = if request.allow_filtered
                && request.verification_policy != VerificationPolicy::CompleteCryptographic
            {
                descriptor.present_capabilities.with_derivable()
            } else {
                descriptor.complete_capabilities.with_derivable()
            };
            available = available.union(supplied);
            if supplied.contains_all(request.required)
                && descriptor.verification >= required_verification
                && descriptor.trust >= required_trust
                && descriptor.trust >= self.config.trust
            {
                compatible.push(record);
            }
        }
        compatible.sort_by_key(|record| {
            (
                record.metadata.descriptor.range.start().0,
                std::cmp::Reverse(record.metadata.descriptor.range.end().0),
                record.metadata.id.clone(),
            )
        });
        Ok((compatible, available))
    }

    fn material_covers(&self, request: &DataRequest) -> bool {
        if self.config.material_shape == MaterialShapeId::COMPLETE_EXECUTION
            || RawHistoryMaterialProfile::from_request(request).shape_id()
                == self.config.material_shape
        {
            return true;
        }
        // Scope inclusion is sufficient for logs when every requested field
        // is retained. Other material keeps its exact shape requirement.
        let Some(profile) = &self.config.material_profile else {
            return false;
        };
        request.required == CapabilitySet::of(Capability::Logs)
            && request.allow_filtered
            && profile.log_fields.contains_all(request.log_fields)
            && profile.projection.log_fields.is_empty()
            && profile.filters.senders.is_empty()
            && profile.filters.recipients.is_empty()
            && profile.filters.scope.covers(&request.filters.scope)
    }

    fn compatible_lookup_segment(&self, record: &SegmentRecord, required: CapabilitySet) -> bool {
        let descriptor = &record.metadata.descriptor;
        descriptor.chain_id == self.config.chain_id
            && descriptor.material_shape == self.config.material_shape
            && record.profile.satisfies(self.config.required_profile)
            && descriptor
                .complete_capabilities
                .with_derivable()
                .contains_all(required)
            && descriptor.verification >= self.config.verification
            && descriptor.trust >= self.config.trust
    }

    async fn read_lookup(
        &self,
        segment_id: &SegmentId,
        block_number: BlockNumber,
        required: CapabilitySet,
    ) -> Result<Option<SegmentRead>, SourceError> {
        let Some(record) = self
            .store
            .segment(segment_id)
            .await
            .map_err(|error| source_store_error(&error))?
        else {
            return Ok(None);
        };
        if !self.compatible_lookup_segment(&record, required) {
            return Ok(None);
        }
        let read = match self.store.read_block(segment_id, block_number).await {
            Ok(read) => read,
            // A segment that failed validation no longer covers the block,
            // and one whose file is gone cannot serve it now.
            Err(HistoryStoreError::Quarantined { .. }) => return Ok(None),
            Err(HistoryStoreError::Segment(error)) if error.is_missing_file() => return Ok(None),
            Err(error) => return Err(source_store_error(&error)),
        };
        self.stats.locator_uses.fetch_add(1, Ordering::Relaxed);
        self.stats.segment_opens.fetch_add(1, Ordering::Relaxed);
        self.stats.record_reads.fetch_add(1, Ordering::Relaxed);
        self.stats
            .stored_record_bytes
            .fetch_add(read.stored_bytes_read, Ordering::Relaxed);
        self.stats
            .decompressed_bytes
            .fetch_add(read.logical_bytes_read, Ordering::Relaxed);
        Ok(Some(read))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct RetainedPartition {
    segment_id: SegmentId,
    range: BlockRange,
}

#[async_trait]
impl HistorySource for RetainedHistorySource {
    fn descriptor(&self) -> &SourceDescriptor {
        &self.descriptor
    }

    fn lookup_capabilities(&self) -> HistoryLookupCapabilities {
        HistoryLookupCapabilities {
            block_hash: true,
            transaction_hash: true,
        }
    }

    async fn block_by_hash(
        &self,
        chain_id: ChainId,
        hash: BlockHash,
        required: CapabilitySet,
    ) -> Result<Option<leani_primitives::BlockFrame>, SourceError> {
        if chain_id != self.config.chain_id {
            return Ok(None);
        }
        for locator in self
            .store
            .block_hash_locators(chain_id, hash)
            .await
            .map_err(|error| source_store_error(&error))?
        {
            let Some(read) = self
                .read_lookup(&locator.segment_id, locator.block_number, required)
                .await?
            else {
                continue;
            };
            if read.frame.block.hash != hash {
                return Err(SourceError::CorruptFrame(
                    "block-hash locator conflicts with retained frame".to_owned(),
                ));
            }
            return Ok(Some(read.frame));
        }
        Ok(None)
    }

    async fn transaction_by_hash(
        &self,
        chain_id: ChainId,
        hash: TransactionHash,
        required: CapabilitySet,
    ) -> Result<Option<LocatedTransaction>, SourceError> {
        if chain_id != self.config.chain_id {
            return Ok(None);
        }
        for locator in self
            .store
            .transaction_locators(chain_id, hash)
            .await
            .map_err(|error| source_store_error(&error))?
        {
            let Some(read) = self
                .read_lookup(&locator.segment_id, locator.block_number, required)
                .await?
            else {
                continue;
            };
            let index = usize::try_from(locator.transaction_index).map_err(|_| {
                SourceError::CorruptFrame("transaction locator index exceeds platform".to_owned())
            })?;
            if read
                .frame
                .transactions
                .as_complete()
                .and_then(|transactions| transactions.get(index))
                .is_none_or(|transaction| transaction.hash != hash)
            {
                return Err(SourceError::CorruptFrame(
                    "transaction locator conflicts with retained frame".to_owned(),
                ));
            }
            return Ok(Some(LocatedTransaction {
                frame: read.frame,
                transaction_index: locator.transaction_index,
            }));
        }
        Ok(None)
    }

    fn coalescing_partition_identity(&self, chunk: &SourceChunk) -> Vec<u8> {
        postcard::from_bytes::<RetainedPartition>(&chunk.partition).map_or_else(
            |_| chunk.partition.clone(),
            |partition| postcard::to_allocvec(&partition.segment_id).unwrap_or_default(),
        )
    }

    fn slice_chunk(
        &self,
        chunk: &SourceChunk,
        range: BlockRange,
    ) -> Result<SourceChunk, SourceError> {
        if range.start() < chunk.range.start() || range.end() > chunk.range.end() {
            return Err(SourceError::InvalidPlan(
                "retained segment slice exceeds its planned chunk".to_owned(),
            ));
        }
        let mut partition: RetainedPartition = postcard::from_bytes(&chunk.partition)
            .map_err(|error| SourceError::InvalidPlan(error.to_string()))?;
        partition.range = range;
        let mut sliced = chunk.clone();
        sliced.range = range;
        sliced.partition = postcard::to_allocvec(&partition)
            .map_err(|error| SourceError::InvalidPlan(error.to_string()))?;
        sliced.expected_parent = None;
        sliced.estimated_bytes = None;
        Ok(sliced)
    }

    #[allow(clippy::too_many_lines)]
    async fn plan(&self, request: &DataRequest) -> Result<SourcePlan, SourceError> {
        if request.chain_id != self.config.chain_id {
            return Err(SourceError::MissingRange(request.range));
        }
        if !self
            .config
            .capabilities
            .with_derivable()
            .contains_all(request.required)
        {
            return Err(SourceError::MissingMaterial {
                gaps: vec![request.range],
                required: request.required.bits(),
                available: self.config.capabilities.with_derivable().bits(),
            });
        }
        if !self.material_covers(request) {
            return Err(SourceError::MissingMaterial {
                gaps: vec![request.range],
                required: request.required.bits(),
                available: 0,
            });
        }
        let (segments, available) = self.compatible_segments(request).await?;
        let covered = segments
            .iter()
            .map(|record| record.metadata.descriptor.range)
            .collect::<Vec<_>>();
        let gaps = coverage_gaps(request.range, &covered);
        if !gaps.is_empty() {
            return Err(SourceError::MissingMaterial {
                gaps,
                required: request.required.bits(),
                available: available.bits(),
            });
        }

        let selected = select_cover(request.range, &segments)?;
        let mut chunks = Vec::with_capacity(selected.len());
        let mut physical_plan = Vec::with_capacity(selected.len());
        let mut estimated_bytes = Some(0_u64);
        let mut supplied = CapabilitySet::ALL;
        let mut complete = CapabilitySet::ALL;
        let mut trust = self.config.trust;
        for (record, range) in selected {
            let descriptor = &record.metadata.descriptor;
            supplied = supplied.intersection(descriptor.present_capabilities.with_derivable());
            complete = complete.intersection(descriptor.complete_capabilities.with_derivable());
            trust = trust.min(descriptor.trust);
            let estimate = record
                .metadata
                .physical_bytes
                .checked_mul(range.len())
                .map(|bytes| bytes.div_ceil(descriptor.range.len()));
            estimated_bytes = estimated_bytes
                .zip(estimate)
                .and_then(|(total, value)| total.checked_add(value));
            physical_plan.push(PhysicalPlanOperation {
                reader: PhysicalReader::Block,
                table: record.metadata.id.as_str().to_owned(),
                columns: vec!["durable_block_frame".to_owned()],
                predicates: vec![format!(
                    "block_number BETWEEN {} AND {}",
                    range.start().0,
                    range.end().0
                )],
                estimated_bytes: estimate,
                trust: descriptor.trust,
                completeness: if descriptor
                    .complete_capabilities
                    .with_derivable()
                    .contains_all(request.required)
                {
                    "complete retained frame"
                } else {
                    "filtered retained frame; consumer validates predicate completeness"
                }
                .to_owned(),
                derived: false,
            });
            chunks.push(SourceChunk {
                source_id: self.descriptor.id.clone(),
                range,
                partition: postcard::to_allocvec(&RetainedPartition {
                    segment_id: record.metadata.id.clone(),
                    range,
                })
                .map_err(|error| SourceError::InvalidPlan(error.to_string()))?,
                schema_version: RETAINED_SCHEMA_VERSION.to_owned(),
                expected_parent: (range.start() == descriptor.range.start())
                    .then_some(record.metadata.first_parent_hash),
                estimated_bytes: estimate,
            });
        }
        let plan = SourcePlan {
            source_id: self.descriptor.id.clone(),
            request: request.clone(),
            chunks,
            estimated_bytes,
            estimated_lag: Duration::ZERO,
            supplied,
            complete,
            trust,
            schema_version: RETAINED_SCHEMA_VERSION.to_owned(),
            physical_plan,
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
        if chunk.source_id != self.descriptor.id || chunk.schema_version != RETAINED_SCHEMA_VERSION
        {
            return Err(SourceError::InvalidPlan(
                "retained chunk identity does not match this source".to_owned(),
            ));
        }
        let partition: RetainedPartition = postcard::from_bytes(&chunk.partition)
            .map_err(|error| SourceError::InvalidPlan(error.to_string()))?;
        if partition.range != chunk.range {
            return Err(SourceError::InvalidPlan(
                "retained partition range differs from its chunk".to_owned(),
            ));
        }
        if chunk.range.len() > budget.max_frames {
            return Err(SourceError::BudgetExceeded {
                resource: "frames",
                limit: budget.max_frames,
                observed: chunk.range.len(),
            });
        }
        let mut reader = self
            .store
            .reader(&partition.segment_id)
            .await
            .map_err(|error| retained_read_error(&error, chunk.range))?;
        let descriptor = &reader.metadata().descriptor;
        if descriptor.chain_id != self.config.chain_id
            || descriptor.material_shape != self.config.material_shape
            || descriptor.range.start() > chunk.range.start()
            || descriptor.range.end() < chunk.range.end()
        {
            return Err(SourceError::InvalidPlan(
                "retained segment no longer satisfies its planned chunk".to_owned(),
            ));
        }
        self.stats.segment_opens.fetch_add(1, Ordering::Relaxed);
        let stats = self.stats.clone();
        let store = self.store.clone();
        let runtime = tokio::runtime::Handle::current();
        let range = chunk.range;
        let (sender, receiver) = tokio::sync::mpsc::channel(budget.max_buffered_frames);
        tokio::task::spawn_blocking(move || {
            let mut input_bytes = 0_u64;
            for number in range.iter() {
                if cancellation.is_cancelled() {
                    let _ = sender.blocking_send(Err(SourceError::Cancelled));
                    return;
                }
                let read = match reader.read_block(number) {
                    Ok(read) => read,
                    Err(error) => {
                        let metadata = reader.metadata().clone();
                        let error = runtime.block_on(store.segment_read_failed(&metadata, error));
                        let _ = sender.blocking_send(Err(retained_read_error(&error, range)));
                        return;
                    }
                };
                input_bytes = match charge_record(&read, budget, input_bytes) {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        let _ = sender.blocking_send(Err(error));
                        return;
                    }
                };
                stats.record_reads.fetch_add(1, Ordering::Relaxed);
                stats
                    .stored_record_bytes
                    .fetch_add(read.stored_bytes_read, Ordering::Relaxed);
                stats
                    .decompressed_bytes
                    .fetch_add(read.logical_bytes_read, Ordering::Relaxed);
                if sender.blocking_send(Ok(read.frame)).is_err() {
                    return;
                }
            }
        });
        Ok(Box::pin(stream::unfold(receiver, |mut receiver| async {
            receiver.recv().await.map(|item| (item, receiver))
        })))
    }
}

/// Check one record read against `budget`, returning the input the open
/// has acquired with it.
fn charge_record(
    read: &SegmentRead,
    budget: SourceBudget,
    input_bytes: u64,
) -> Result<u64, SourceError> {
    // One stored record is held at a time.
    if read.stored_bytes_read > budget.max_resident_bytes {
        return Err(SourceError::BudgetExceeded {
            resource: "resident_bytes",
            limit: budget.max_resident_bytes,
            observed: read.stored_bytes_read,
        });
    }
    if read.logical_bytes_read > budget.max_frame_bytes {
        return Err(SourceError::BudgetExceeded {
            resource: "frame_bytes",
            limit: budget.max_frame_bytes,
            observed: read.logical_bytes_read,
        });
    }
    let acquired = input_bytes.saturating_add(read.logical_bytes_read);
    if acquired > budget.max_input_bytes {
        return Err(SourceError::BudgetExceeded {
            resource: "input_bytes",
            limit: budget.max_input_bytes,
            observed: acquired,
        });
    }
    Ok(acquired)
}

fn select_cover(
    requested: BlockRange,
    segments: &[SegmentRecord],
) -> Result<Vec<(&SegmentRecord, BlockRange)>, SourceError> {
    let mut selected = Vec::new();
    let mut next = requested.start();
    loop {
        let record = segments
            .iter()
            .filter(|record| record.metadata.descriptor.range.contains(next))
            .max_by_key(|record| record.metadata.descriptor.range.end().0)
            .ok_or_else(|| {
                BlockRange::new(next, requested.end()).map_or_else(
                    |error| SourceError::InvalidPlan(error.to_string()),
                    SourceError::MissingRange,
                )
            })?;
        let end = BlockNumber(
            record
                .metadata
                .descriptor
                .range
                .end()
                .0
                .min(requested.end().0),
        );
        let range = BlockRange::new(next, end)
            .map_err(|error| SourceError::InvalidPlan(error.to_string()))?;
        selected.push((record, range));
        if end == requested.end() {
            return Ok(selected);
        }
        next = BlockNumber(
            end.0
                .checked_add(1)
                .ok_or_else(|| SourceError::InvalidPlan("block range overflow".to_owned()))?,
        );
    }
}

const fn minimum_trust_for_verification(verification: VerificationClass) -> TrustModel {
    match verification {
        VerificationClass::BestEffort => TrustModel::Untrusted,
        VerificationClass::TrustedDataset => TrustModel::TrustedDataset,
        VerificationClass::Cryptographic => TrustModel::ProtocolVerified,
    }
}

const fn verification_for_policy(policy: VerificationPolicy) -> VerificationClass {
    match policy {
        VerificationPolicy::CompleteCryptographic => VerificationClass::Cryptographic,
        VerificationPolicy::TrustedDataset => VerificationClass::TrustedDataset,
        VerificationPolicy::BestEffort => VerificationClass::BestEffort,
    }
}

const fn minimum_trust_for_policy(policy: VerificationPolicy) -> TrustModel {
    match policy {
        VerificationPolicy::CompleteCryptographic => TrustModel::ProtocolVerified,
        VerificationPolicy::TrustedDataset => TrustModel::TrustedDataset,
        VerificationPolicy::BestEffort => TrustModel::Untrusted,
    }
}

/// A retained segment that failed validation no longer covers `range`, and
/// one whose file is gone, as when its mount went away, cannot serve it now;
/// another source may still serve it.
fn retained_read_error(error: &HistoryStoreError, range: BlockRange) -> SourceError {
    match error {
        HistoryStoreError::Quarantined { .. } => SourceError::MissingRange(range),
        HistoryStoreError::Segment(segment) if segment.is_missing_file() => {
            SourceError::MissingRange(range)
        }
        other => source_store_error(other),
    }
}

/// A failure that can clear, such as transient I/O or a busy catalog, is
/// unavailable, so callers retry or fail over; anything else is corrupt.
fn source_store_error(error: &HistoryStoreError) -> SourceError {
    let transient = match error {
        HistoryStoreError::UnknownSegment(_)
        | HistoryStoreError::Io(_)
        | HistoryStoreError::Task(_)
        | HistoryStoreError::Sql(
            sqlx::Error::Io(_)
            | sqlx::Error::PoolTimedOut
            | sqlx::Error::PoolClosed
            | sqlx::Error::WorkerCrashed,
        ) => true,
        // A read that ends early is a truncated file.
        HistoryStoreError::Segment(SegmentError::Io(io)) => {
            io.kind() != std::io::ErrorKind::UnexpectedEof
        }
        // SQLITE_BUSY, SQLITE_LOCKED, SQLITE_IOERR, and SQLITE_FULL,
        // including their extended codes: the catalog is contended, or its
        // disk failed or filled up.
        HistoryStoreError::Sql(sqlx::Error::Database(database)) => database
            .code()
            .and_then(|code| code.parse::<i32>().ok())
            .is_some_and(|code| matches!(code & 0xff, 5 | 6 | 10 | 13)),
        _ => false,
    };
    if transient {
        SourceError::Unavailable(error.to_string())
    } else {
        SourceError::CorruptFrame(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use leani_primitives::{
        BlockFrame, BlockHash, Material, MissingReason, TransactionEnvelope, TransactionHash,
    };
    use leani_testkit::fixture_frame;
    use tempfile::tempdir;

    use super::*;
    use crate::{
        Compression, HistoryStoreConfig, RawHistoryIndexPolicy, SegmentDescriptor, SegmentId,
        SegmentReservation, StorageBudget,
    };

    fn config(root: &std::path::Path) -> HistoryStoreConfig {
        HistoryStoreConfig::new(root).with_budget(StorageBudget {
            maximum_logical_bytes: 32 * 1024 * 1024,
            maximum_physical_bytes: 32 * 1024 * 1024,
            maximum_frame_logical_bytes: 1024 * 1024,
            maximum_segment_logical_bytes: 4 * 1024 * 1024,
            maximum_segment_physical_bytes: 4 * 1024 * 1024,
        })
    }

    fn frames(start: u64, end: u64) -> Vec<BlockFrame> {
        let mut parent = BlockHash::new([0x31; 32]);
        (start..=end)
            .map(|number| {
                let frame = fixture_frame(number, parent);
                parent = frame.block.hash;
                frame
            })
            .collect()
    }

    async fn publish(
        store: &HistoryStore,
        id: &str,
        frames: &[BlockFrame],
        shape: MaterialShapeId,
        verification: VerificationClass,
        trust: TrustModel,
    ) {
        let capabilities = frames[0].capabilities();
        let mut pending = store
            .begin_segment(
                SegmentId::new(id).expect("segment ID"),
                SegmentDescriptor {
                    chain_id: ChainId(1),
                    range: BlockRange::new(
                        frames[0].block.number,
                        frames.last().expect("frames").block.number,
                    )
                    .expect("range"),
                    material_shape: shape,
                    present_capabilities: capabilities.present,
                    complete_capabilities: capabilities.complete,
                    verification,
                    trust,
                },
                Compression::Snappy,
                SegmentReservation::new(1024 * 1024, 1024 * 1024),
            )
            .await
            .expect("begin");
        for frame in frames {
            pending.append(frame).expect("append");
        }
        pending.commit(&[]).await.expect("commit");
    }

    fn request(range: BlockRange, required: CapabilitySet) -> DataRequest {
        let log_fields = if required.contains(Capability::Logs) {
            leani_primitives::LogFieldSet::ALL
        } else {
            leani_primitives::LogFieldSet::NONE
        };
        DataRequest {
            chain_id: ChainId(1),
            range,
            required,
            allow_filtered: false,
            projection: leani_source_api::FieldProjection::default(),
            log_fields,
            filters: leani_source_api::FilterSet::default(),
            minimum_finality: leani_primitives::Finality::Finalized,
            verification_policy: VerificationPolicy::TrustedDataset,
        }
    }

    fn budget() -> SourceBudget {
        SourceBudget {
            max_input_bytes: 8 * 1024 * 1024,
            max_frame_bytes: 1024 * 1024,
            max_frames: 100,
            max_buffered_frames: 2,
            max_in_flight_requests: 1,
            temporary_disk_bytes: 0,
            max_resident_bytes: 8 * 1024 * 1024,
        }
    }

    #[tokio::test]
    async fn locator_lookups_return_validated_retained_frames() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(config(directory.path()))
            .await
            .expect("store");
        let transaction_hash = TransactionHash::new([0xc1; 32]);
        let mut frame = fixture_frame(90, BlockHash::new([0x31; 32]));
        frame.transactions = Material::Complete(vec![TransactionEnvelope {
            hash: transaction_hash,
            transaction_type: 2,
            index: 0,
            encoded: Some(vec![0x02, 0xc0]),
            from: None,
            to: None,
            nonce: Some(0),
            gas_limit: Some(21_000),
            value: None,
            input: Some(Vec::new()),
            max_fee_per_gas: None,
            max_priority_fee_per_gas: None,
            max_fee_per_blob_gas: None,
            blob_versioned_hashes: Vec::new(),
            size_bytes: Some(2),
        }]);
        frame.receipts = Material::Missing(MissingReason::NotRequested);
        let capabilities = frame.capabilities();
        let mut pending = store
            .begin_segment_indexed(
                SegmentId::new("source-locators").expect("segment ID"),
                SegmentDescriptor {
                    chain_id: ChainId(1),
                    range: BlockRange::single(frame.block.number),
                    material_shape: MaterialShapeId::COMPLETE_EXECUTION,
                    present_capabilities: capabilities.present,
                    complete_capabilities: capabilities.complete,
                    verification: VerificationClass::TrustedDataset,
                    trust: TrustModel::TrustedDataset,
                },
                Compression::Snappy,
                SegmentReservation::new(1024 * 1024, 1024 * 1024),
                RawHistoryIndexPolicy {
                    block_hash: true,
                    transaction_hash: true,
                    logs: false,
                },
            )
            .await
            .expect("begin indexed segment");
        pending.append(&frame).expect("append frame");
        pending.commit(&[]).await.expect("commit segment");
        let source = RetainedHistorySource::new(
            store,
            RetainedHistorySourceConfig::local(
                ChainId(1),
                MaterialShapeId::COMPLETE_EXECUTION,
                capabilities.complete,
                VerificationClass::TrustedDataset,
                TrustModel::TrustedDataset,
            )
            .expect("source config"),
        )
        .expect("source");

        let block = source
            .block_by_hash(
                ChainId(1),
                frame.block.hash,
                CapabilitySet::of(Capability::Transactions),
            )
            .await
            .expect("block lookup")
            .expect("located block");
        assert_eq!(block, frame);
        let transaction = source
            .transaction_by_hash(
                ChainId(1),
                transaction_hash,
                CapabilitySet::of(Capability::Transactions),
            )
            .await
            .expect("transaction lookup")
            .expect("located transaction");
        assert_eq!(transaction.frame, frame);
        assert_eq!(transaction.transaction_index, 0);
        assert_eq!(source.stats().locator_uses, 2);
    }

    #[tokio::test]
    async fn plans_adjacent_segments_and_streams_without_whole_segment_decode() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(config(directory.path()))
            .await
            .expect("store");
        let shape = MaterialShapeId::COMPLETE_EXECUTION;
        let all = frames(1, 6);
        Box::pin(publish(
            &store,
            "retained-a",
            &all[..3],
            shape,
            VerificationClass::TrustedDataset,
            TrustModel::TrustedDataset,
        ))
        .await;
        Box::pin(publish(
            &store,
            "retained-b",
            &all[3..],
            shape,
            VerificationClass::TrustedDataset,
            TrustModel::TrustedDataset,
        ))
        .await;
        let capabilities = all[0].capabilities().complete;
        let source = RetainedHistorySource::new(
            store,
            RetainedHistorySourceConfig::local(
                ChainId(1),
                shape,
                capabilities,
                VerificationClass::TrustedDataset,
                TrustModel::TrustedDataset,
            )
            .expect("profile"),
        )
        .expect("source");
        let request = request(
            BlockRange::new(BlockNumber(2), BlockNumber(5)).expect("range"),
            CapabilitySet::of(Capability::Transactions),
        );
        let plan = source.plan(&request).await.expect("plan");
        assert_eq!(plan.chunks.len(), 2);
        assert_eq!(plan.chunks[0].range.start(), BlockNumber(2));
        assert_eq!(plan.chunks[1].range.end(), BlockNumber(5));
        let mut narrower = request.clone();
        narrower.allow_filtered = true;
        narrower.filters.scope.transaction_types = vec![3];
        assert_eq!(
            source
                .plan(&narrower)
                .await
                .expect("complete material contains narrower predicate")
                .chunks
                .len(),
            2
        );
        let mut observed = Vec::new();
        for chunk in &plan.chunks {
            let mut stream = source
                .open(chunk, budget(), CancellationToken::new())
                .await
                .expect("open");
            while let Some(frame) = stream.next().await {
                observed.push(frame.expect("frame"));
            }
        }
        assert_eq!(observed, all[1..5]);
        let stats = source.stats();
        assert_eq!(stats.segment_opens, 2);
        assert_eq!(stats.record_reads, 4);
        assert!(stats.stored_record_bytes > 0);
        assert!(stats.decompressed_bytes >= stats.stored_record_bytes);
        assert_eq!(stats.cache_hits, 0);
        assert_eq!(stats.locator_uses, 0);
    }

    #[tokio::test]
    async fn gaps_capabilities_and_trust_fail_before_open() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(config(directory.path()))
            .await
            .expect("store");
        let shape = MaterialShapeId::COMPLETE_EXECUTION;
        let all = frames(10, 14);
        Box::pin(publish(
            &store,
            "gap-a",
            &all[..2],
            shape,
            VerificationClass::TrustedDataset,
            TrustModel::TrustedDataset,
        ))
        .await;
        Box::pin(publish(
            &store,
            "gap-b",
            &all[3..],
            shape,
            VerificationClass::TrustedDataset,
            TrustModel::TrustedDataset,
        ))
        .await;
        let capabilities = all[0].capabilities().complete;
        let source = RetainedHistorySource::new(
            store.clone(),
            RetainedHistorySourceConfig::local(
                ChainId(1),
                shape,
                capabilities,
                VerificationClass::TrustedDataset,
                TrustModel::TrustedDataset,
            )
            .expect("profile"),
        )
        .expect("source");
        let range = BlockRange::new(BlockNumber(10), BlockNumber(14)).expect("range");
        assert!(matches!(
            source
                .plan(&request(
                    range,
                    CapabilitySet::of(Capability::Transactions)
                ))
                .await,
            Err(SourceError::MissingMaterial { ref gaps, .. })
                if gaps == &vec![BlockRange::single(BlockNumber(12))]
        ));
        assert!(matches!(
            source
                .plan(&request(range, CapabilitySet::of(Capability::BlobSidecars)))
                .await,
            Err(SourceError::MissingMaterial { .. })
        ));

        let stronger = RetainedHistorySource::new(
            store,
            RetainedHistorySourceConfig::local(
                ChainId(1),
                shape,
                capabilities,
                VerificationClass::Cryptographic,
                TrustModel::ProtocolVerified,
            )
            .expect("strong profile"),
        )
        .expect("source");
        let mut cryptographic = request(
            BlockRange::new(BlockNumber(10), BlockNumber(11)).expect("range"),
            CapabilitySet::of(Capability::Transactions),
        );
        cryptographic.verification_policy = VerificationPolicy::CompleteCryptographic;
        assert!(matches!(
            stronger.plan(&cryptographic).await,
            Err(SourceError::MissingMaterial { .. })
        ));
        assert_eq!(stronger.stats().segment_opens, 0);
    }

    #[test]
    fn transient_store_failures_stay_retryable() {
        // Audit History-6: every store failure but an unknown segment read as
        // corrupt material, which no retry or failover recovers.
        for error in [
            HistoryStoreError::Io(std::io::Error::other("disk busy")),
            HistoryStoreError::Sql(sqlx::Error::PoolTimedOut),
            HistoryStoreError::Segment(crate::SegmentError::Io(std::io::Error::other(
                "read interrupted",
            ))),
        ] {
            let mapped = source_store_error(&error);
            assert!(matches!(mapped, SourceError::Unavailable(_)), "{mapped}");
        }
        for error in [
            HistoryStoreError::Segment(crate::SegmentError::ContentChecksum),
            HistoryStoreError::CatalogIntegrity("malformed row".to_owned()),
        ] {
            let mapped = source_store_error(&error);
            assert!(matches!(mapped, SourceError::CorruptFrame(_)), "{mapped}");
        }
    }

    /// A catalog failure the driver reports with `code`.
    #[derive(Debug)]
    struct SqliteFailure(i32);

    impl std::fmt::Display for SqliteFailure {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(formatter, "SQLite error {}", self.0)
        }
    }

    impl std::error::Error for SqliteFailure {}

    impl sqlx::error::DatabaseError for SqliteFailure {
        fn message(&self) -> &'static str {
            "SQLite error"
        }

        fn code(&self) -> Option<std::borrow::Cow<'_, str>> {
            Some(self.0.to_string().into())
        }

        fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
            self
        }

        fn as_error_mut(&mut self) -> &mut (dyn std::error::Error + Send + Sync + 'static) {
            self
        }

        fn into_error(self: Box<Self>) -> Box<dyn std::error::Error + Send + Sync + 'static> {
            self
        }

        fn kind(&self) -> sqlx::error::ErrorKind {
            sqlx::error::ErrorKind::Other
        }
    }

    #[test]
    fn catalog_failures_that_can_clear_stay_retryable() {
        let classify = |code| {
            source_store_error(&HistoryStoreError::Sql(sqlx::Error::Database(Box::new(
                SqliteFailure(code),
            ))))
        };
        // BUSY, BUSY_SNAPSHOT, LOCKED, IOERR, IOERR_READ, and FULL. Review M7:
        // a failing or full disk read as corrupt material.
        for code in [5, 517, 6, 10, 266, 13] {
            let mapped = classify(code);
            assert!(
                matches!(mapped, SourceError::Unavailable(_)),
                "{code}: {mapped}"
            );
        }
        // CORRUPT and CONSTRAINT.
        for code in [11, 19] {
            let mapped = classify(code);
            assert!(
                matches!(mapped, SourceError::CorruptFrame(_)),
                "{code}: {mapped}"
            );
        }
    }

    #[tokio::test]
    async fn open_holds_one_stored_record_within_the_resident_budget() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(config(directory.path()))
            .await
            .expect("store");
        let shape = MaterialShapeId::COMPLETE_EXECUTION;
        let all = frames(24, 25);
        Box::pin(publish(
            &store,
            "resident",
            &all,
            shape,
            VerificationClass::TrustedDataset,
            TrustModel::TrustedDataset,
        ))
        .await;
        let source = RetainedHistorySource::new(
            store,
            RetainedHistorySourceConfig::local(
                ChainId(1),
                shape,
                all[0].capabilities().complete,
                VerificationClass::TrustedDataset,
                TrustModel::TrustedDataset,
            )
            .expect("profile"),
        )
        .expect("source");
        let plan = source
            .plan(&request(
                BlockRange::new(BlockNumber(24), BlockNumber(25)).expect("range"),
                CapabilitySet::of(Capability::Transactions),
            ))
            .await
            .expect("plan");
        let mut constrained = budget();
        constrained.max_resident_bytes = 1;
        let mut stream = source
            .open(&plan.chunks[0], constrained, CancellationToken::new())
            .await
            .expect("open");
        // Review I1: a stored record was bounded only by what the open may
        // acquire in total.
        let first = stream.next().await;
        assert!(
            matches!(
                first,
                Some(Err(SourceError::BudgetExceeded {
                    resource: "resident_bytes",
                    ..
                }))
            ),
            "{first:?}"
        );
    }

    #[tokio::test]
    async fn a_segment_file_gone_while_open_reads_as_missing() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(config(directory.path()))
            .await
            .expect("store");
        let shape = MaterialShapeId::COMPLETE_EXECUTION;
        let all = frames(30, 31);
        let capabilities = all[0].capabilities();
        let mut pending = store
            .begin_segment_indexed(
                SegmentId::new("gone-while-open").expect("segment ID"),
                SegmentDescriptor {
                    chain_id: ChainId(1),
                    range: BlockRange::new(BlockNumber(30), BlockNumber(31)).expect("range"),
                    material_shape: shape,
                    present_capabilities: capabilities.present,
                    complete_capabilities: capabilities.complete,
                    verification: VerificationClass::TrustedDataset,
                    trust: TrustModel::TrustedDataset,
                },
                Compression::Snappy,
                SegmentReservation::new(1024 * 1024, 1024 * 1024),
                RawHistoryIndexPolicy {
                    block_hash: true,
                    transaction_hash: false,
                    logs: false,
                },
            )
            .await
            .expect("begin");
        for frame in &all {
            pending.append(frame).expect("append");
        }
        let record = pending.commit(&[]).await.expect("commit");
        let source = RetainedHistorySource::new(
            store.clone(),
            RetainedHistorySourceConfig::local(
                ChainId(1),
                shape,
                capabilities.complete,
                VerificationClass::TrustedDataset,
                TrustModel::TrustedDataset,
            )
            .expect("profile"),
        )
        .expect("source");
        let required = CapabilitySet::of(Capability::Transactions);
        let plan = source
            .plan(&request(
                BlockRange::new(BlockNumber(30), BlockNumber(31)).expect("range"),
                required,
            ))
            .await
            .expect("plan");
        std::fs::remove_file(directory.path().join(&record.relative_path))
            .expect("take the file away");
        // Another source may serve the blocks while the file is gone, and the
        // segment serves them again once it returns.
        assert!(matches!(
            source
                .open(&plan.chunks[0], budget(), CancellationToken::new())
                .await,
            Err(SourceError::MissingRange(_))
        ));
        assert_eq!(
            source
                .block_by_hash(ChainId(1), all[0].block.hash, required)
                .await
                .expect("lookup"),
            None
        );
        assert!(
            store
                .segment(&record.metadata.id)
                .await
                .expect("lookup")
                .is_some()
        );
    }

    #[tokio::test]
    async fn open_enforces_frame_and_input_budgets() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(config(directory.path()))
            .await
            .expect("store");
        let shape = MaterialShapeId::COMPLETE_EXECUTION;
        let all = frames(20, 21);
        Box::pin(publish(
            &store,
            "budget",
            &all,
            shape,
            VerificationClass::TrustedDataset,
            TrustModel::TrustedDataset,
        ))
        .await;
        let source = RetainedHistorySource::new(
            store,
            RetainedHistorySourceConfig::local(
                ChainId(1),
                shape,
                all[0].capabilities().complete,
                VerificationClass::TrustedDataset,
                TrustModel::TrustedDataset,
            )
            .expect("profile"),
        )
        .expect("source");
        let plan = source
            .plan(&request(
                BlockRange::new(BlockNumber(20), BlockNumber(21)).expect("range"),
                CapabilitySet::of(Capability::Transactions),
            ))
            .await
            .expect("plan");
        let mut constrained = budget();
        constrained.max_frame_bytes = 1;
        let mut stream = source
            .open(&plan.chunks[0], constrained, CancellationToken::new())
            .await
            .expect("open starts within frame-count budget");
        assert!(matches!(
            stream.next().await,
            Some(Err(SourceError::BudgetExceeded {
                resource: "frame_bytes",
                ..
            }))
        ));
    }
}
