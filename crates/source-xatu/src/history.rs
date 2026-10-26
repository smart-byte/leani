//! Capability-driven historical-source adapter for public Xatu projections.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use futures::{StreamExt, stream};
use leani_primitives::{
    BlockNumber, BlockRange, Capability, CapabilitySet, ChainId, SourceId, SourceKind, TrustModel,
};
use leani_source_api::{
    BlockFrameStream, DataRequest, FinalityModel, HistorySource, Partitioning,
    PhysicalPlanOperation, PhysicalReader, SourceAcquisitionMetrics, SourceBudget, SourceChunk,
    SourceDescriptor, SourceError, SourcePlan, VerificationPolicy,
};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::{
    XatuBlobsProjector, XatuCatalog, XatuCatalogConfig, XatuError,
    projection::{GenericProjectionKind, TransactionColumns, generic_log_columns},
};

const SCHEMA_VERSION: &str = "xatu.history.v4";
const PHYSICAL_PARTITION_BLOCKS: u64 = 1_000;
// Execution parent hashes are supplied by Beacon execution payloads.
const MAINNET_MERGE_BLOCK: u64 = 15_537_394;

/// Planner and chunking settings for public Xatu history.
#[derive(Clone, Debug)]
pub struct XatuHistoryConfig {
    pub catalog: XatuCatalogConfig,
    pub range: Option<BlockRange>,
    pub chunk_blocks: u64,
    pub blobs_chunk_blocks: u64,
    pub batch_rows: usize,
    pub priority: u16,
}

impl XatuHistoryConfig {
    /// Public mainnet defaults with 1,000-block independently retryable chunks.
    ///
    /// Xatu history is Ethereum mainnet only: its projections stamp every
    /// frame as chain 1.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid Xatu network name, or for any network
    /// but `mainnet`.
    pub fn public(network: impl Into<String>) -> Result<Self, XatuError> {
        Ok(Self {
            catalog: XatuCatalogConfig::public(network)?,
            range: None,
            chunk_blocks: PHYSICAL_PARTITION_BLOCKS,
            blobs_chunk_blocks: 8_000,
            batch_rows: 8_192,
            priority: 10,
        })
    }
}

/// Trusted-dataset Xatu history source producing source-neutral frames.
#[derive(Clone, Debug)]
pub struct XatuHistorySource {
    descriptor: SourceDescriptor,
    projector: Arc<XatuBlobsProjector>,
    chunk_blocks: u64,
    blobs_chunk_blocks: u64,
    batch_rows: usize,
    acquisition_metrics: Arc<Mutex<SourceAcquisitionMetrics>>,
}

impl XatuHistorySource {
    /// Configure an HTTP-backed, read-only public history source.
    ///
    /// # Errors
    ///
    /// Returns an error for zero chunk size, a catalog for any network but
    /// `mainnet`, or an invalid object-store configuration.
    pub fn new(config: XatuHistoryConfig) -> Result<Self, XatuError> {
        if config.chunk_blocks == 0
            || !config
                .chunk_blocks
                .is_multiple_of(PHYSICAL_PARTITION_BLOCKS)
            || config.blobs_chunk_blocks == 0
            || !config
                .blobs_chunk_blocks
                .is_multiple_of(PHYSICAL_PARTITION_BLOCKS)
            || config.batch_rows == 0
        {
            return Err(XatuError::BlockRange);
        }
        let supported = BlockRange::new(BlockNumber(MAINNET_MERGE_BLOCK), BlockNumber(u64::MAX))
            .map_err(|_| XatuError::BlockRange)?;
        let range = if let Some(requested) = config.range {
            if requested.end() < supported.start() {
                return Err(XatuError::BlockRange);
            }
            BlockRange::new(requested.start().max(supported.start()), requested.end())
                .map_err(|_| XatuError::BlockRange)?
        } else {
            supported
        };
        let id = SourceId::new(format!("xatu-{}", config.catalog.network))
            .map_err(|error| XatuError::Data(error.to_string()))?;
        let catalog = XatuCatalog::new(config.catalog)?;
        let capabilities = CapabilitySet::from_iter([
            Capability::Header,
            Capability::Transactions,
            Capability::Body,
            Capability::Calldata,
            Capability::Receipts,
            Capability::Logs,
        ]);
        Ok(Self {
            descriptor: SourceDescriptor {
                id,
                kind: SourceKind::PublicDataset,
                chain_id: ChainId(1),
                range: Some(range),
                capabilities,
                complete_capabilities: CapabilitySet::NONE,
                trust: TrustModel::TrustedDataset,
                finality: FinalityModel::Finalized,
                partitioning: Partitioning::SourceDefined(
                    "xatu-projection-aware-block-span".to_owned(),
                ),
                // Nominal daily export cadence; actual availability is table-specific.
                expected_lag: Duration::from_hours(24),
                schema_version: SCHEMA_VERSION.to_owned(),
                priority: config.priority,
            },
            projector: Arc::new(XatuBlobsProjector::new(catalog)),
            chunk_blocks: config.chunk_blocks,
            blobs_chunk_blocks: config.blobs_chunk_blocks,
            batch_rows: config.batch_rows,
            acquisition_metrics: Arc::new(Mutex::new(SourceAcquisitionMetrics::default())),
        })
    }
}

/// Compatibility alias for configurations written before the generic planner.
pub type XatuBlobsHistorySource = XatuHistorySource;

#[async_trait]
impl HistorySource for XatuHistorySource {
    fn descriptor(&self) -> &SourceDescriptor {
        &self.descriptor
    }

    fn acquisition_metrics(&self) -> Option<SourceAcquisitionMetrics> {
        Some(
            self.acquisition_metrics
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
        )
    }

    fn coalescing_partition_identity(&self, chunk: &SourceChunk) -> Vec<u8> {
        let Ok(mut identity) = decode_partition(&chunk.partition) else {
            return chunk.partition.clone();
        };
        identity.range =
            BlockRange::new(BlockNumber(0), BlockNumber(0)).expect("zero range is valid");
        postcard::to_allocvec(&identity).unwrap_or_else(|_| chunk.partition.clone())
    }

    fn slice_chunk(
        &self,
        chunk: &SourceChunk,
        range: BlockRange,
    ) -> Result<SourceChunk, SourceError> {
        if range.start() < chunk.range.start() || range.end() > chunk.range.end() {
            return Err(SourceError::InvalidPlan(
                "coalesced Xatu subrange exceeds its planned chunk".to_owned(),
            ));
        }
        let mut identity = decode_partition(&chunk.partition)?;
        identity.range = range;
        let mut sliced = chunk.clone();
        sliced.range = range;
        sliced.partition = postcard::to_allocvec(&identity)
            .map_err(|error| SourceError::InvalidPlan(error.to_string()))?;
        sliced.expected_parent = None;
        sliced.estimated_bytes = None;
        Ok(sliced)
    }

    async fn plan(&self, request: &DataRequest) -> Result<SourcePlan, SourceError> {
        if request.chain_id != self.descriptor.chain_id {
            return Err(SourceError::InvalidPlan(
                "Xatu history currently supports Ethereum mainnet only".to_owned(),
            ));
        }
        if request.range.start().0 < MAINNET_MERGE_BLOCK {
            return Err(SourceError::InvalidPlan(format!(
                "Xatu execution projections require Beacon parent hashes; request blocks from {MAINNET_MERGE_BLOCK} (the Merge) onward, or select a source supporting pre-Merge history"
            )));
        }
        if let Some(available) = self.descriptor.range
            && (available.start().0 > request.range.start().0
                || available.end().0 < request.range.end().0)
        {
            return Err(SourceError::MissingRange(request.range));
        }
        if !request.allow_filtered {
            return Err(SourceError::InvalidPlan(
                "Xatu history is a dataset-declared predicate projection".to_owned(),
            ));
        }
        if matches!(
            request.verification_policy,
            VerificationPolicy::CompleteCryptographic
        ) {
            return Err(SourceError::InvalidPlan(
                "Xatu filtered rows require trusted-dataset verification policy".to_owned(),
            ));
        }
        let mode = projection_mode(request)?;
        let chunk_blocks = if mode == ProjectionMode::Blobs {
            self.blobs_chunk_blocks
        } else {
            self.chunk_blocks
        };
        let mut chunks = Vec::new();
        let mut start = request.range.start().0;
        while start <= request.range.end().0 {
            // Keep logical retry boundaries aligned with Xatu's physical
            // block partitions. Advancing a fixed span from an arbitrary
            // request start makes every chunk straddle two adjacent Parquet
            // objects, so all interior objects are downloaded twice.
            let partition_start = (start / chunk_blocks) * chunk_blocks;
            let end = partition_start
                .saturating_add(chunk_blocks.saturating_sub(1))
                .min(request.range.end().0);
            let range = BlockRange::new(BlockNumber(start), BlockNumber(end))
                .map_err(|error| SourceError::InvalidPlan(error.to_string()))?;
            chunks.push(SourceChunk {
                source_id: self.descriptor.id.clone(),
                range,
                partition: encode_partition(mode, range, request)?,
                schema_version: self.descriptor.schema_version.clone(),
                expected_parent: None,
                estimated_bytes: None,
            });
            if end == u64::MAX {
                break;
            }
            start = end + 1;
        }
        Ok(SourcePlan {
            source_id: self.descriptor.id.clone(),
            request: request.clone(),
            chunks,
            estimated_bytes: None,
            estimated_lag: self.descriptor.expected_lag,
            supplied: supplied_groups(mode, request),
            complete: self.descriptor.complete_capabilities,
            trust: self.descriptor.trust,
            schema_version: self.descriptor.schema_version.clone(),
            physical_plan: physical_plan(mode, request),
        })
    }

    async fn open(
        &self,
        chunk: &SourceChunk,
        budget: SourceBudget,
        cancellation: CancellationToken,
    ) -> Result<BlockFrameStream, SourceError> {
        let identity = decode_partition(&chunk.partition)?;
        if chunk.source_id != self.descriptor.id
            || chunk.schema_version != self.descriptor.schema_version
            || identity.range != chunk.range
        {
            return Err(SourceError::InvalidPlan(
                "Xatu chunk identity does not match this source".to_owned(),
            ));
        }
        {
            let mut metrics = self
                .acquisition_metrics
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            metrics.opened_chunks = metrics.opened_chunks.saturating_add(1);
            metrics.opened_ranges.push(chunk.range);
        }
        let projection = match identity.mode {
            ProjectionMode::Blobs => {
                self.projector
                    .project_auto_dates(chunk.range, budget, self.batch_rows, cancellation)
                    .await
            }
            ProjectionMode::Headers | ProjectionMode::Transactions | ProjectionMode::Logs => {
                self.projector
                    .project_generic(
                        chunk.range,
                        identity.mode.generic_kind(),
                        &identity.filters,
                        identity.log_fields,
                        identity.groups,
                        budget,
                        self.batch_rows,
                        cancellation,
                    )
                    .await
            }
        }
        .map_err(|error| source_error(error, chunk.range))?;
        let normalized_bytes = projection.frames.iter().fold(0_u64, |total, frame| {
            total.saturating_add(frame.estimated_heap_bytes())
        });
        let mut metrics = self
            .acquisition_metrics
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        metrics.acquired_frames = metrics
            .acquired_frames
            .saturating_add(projection.metrics.frames);
        metrics.normalized_bytes = metrics.normalized_bytes.saturating_add(normalized_bytes);
        add_optional(
            &mut metrics.source_objects,
            u64::try_from(projection.metrics.objects.len()).unwrap_or(u64::MAX),
        );
        add_optional(
            &mut metrics.source_object_bytes,
            projection.metrics.object_bytes,
        );
        add_optional(
            &mut metrics.projected_compressed_bytes,
            projection.metrics.projected_compressed_bytes,
        );
        add_optional(
            &mut metrics.logical_range_requests,
            projection.metrics.logical_range_requests,
        );
        add_optional(&mut metrics.fetched_bytes, projection.metrics.fetched_bytes);
        add_optional(&mut metrics.rows_scanned, projection.metrics.rows_scanned);
        add_optional(&mut metrics.rows_selected, projection.metrics.rows_selected);
        metrics.operation_elapsed_ms = metrics
            .operation_elapsed_ms
            .saturating_add(projection.metrics.elapsed_ms);
        drop(metrics);
        Ok(stream::iter(projection.frames.into_iter().map(Ok)).boxed())
    }
}

fn add_optional(target: &mut Option<u64>, value: u64) {
    *target = Some(target.unwrap_or(0).saturating_add(value));
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
enum ProjectionMode {
    Blobs,
    Headers,
    Transactions,
    Logs,
}

impl ProjectionMode {
    const fn generic_kind(self) -> GenericProjectionKind {
        match self {
            Self::Headers => GenericProjectionKind::Headers,
            Self::Transactions => GenericProjectionKind::Transactions,
            Self::Logs => GenericProjectionKind::Logs,
            Self::Blobs => unreachable!(),
        }
    }
}

/// Version 3 records the requested field groups.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct PartitionIdentity {
    version: u8,
    mode: ProjectionMode,
    range: BlockRange,
    filters: leani_source_api::FilterSet,
    log_fields: leani_primitives::LogFieldSet,
    groups: CapabilitySet,
}

/// Chunks planned before field groups, which persisted jobs may still hold.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct PartitionIdentityV2 {
    version: u8,
    mode: ProjectionMode,
    range: BlockRange,
    filters: leani_source_api::FilterSet,
    log_fields: leani_primitives::LogFieldSet,
    include_header: bool,
}

impl From<PartitionIdentityV2> for PartitionIdentity {
    fn from(identity: PartitionIdentityV2) -> Self {
        let header = if identity.include_header {
            CapabilitySet::of(Capability::Header)
        } else {
            CapabilitySet::NONE
        };
        // The groups each version 2 projection filled.
        let groups = match identity.mode {
            // Body joined the blobs projection after version 3.
            ProjectionMode::Blobs => BLOBS_GROUPS.without(Capability::Body),
            ProjectionMode::Headers => CapabilitySet::of(Capability::Header),
            ProjectionMode::Transactions => header
                .with(Capability::Body)
                .with(Capability::Senders)
                .with(Capability::Calldata),
            ProjectionMode::Logs => header.with(Capability::Logs),
        };
        Self {
            version: 3,
            mode: identity.mode,
            range: identity.range,
            filters: identity.filters,
            log_fields: identity.log_fields,
            groups,
        }
    }
}

/// What the type-3 blobs projection fills.
const BLOBS_GROUPS: CapabilitySet = CapabilitySet::of(Capability::Header)
    .with(Capability::Body)
    .with(Capability::Senders)
    .with(Capability::BlobFields)
    .with(Capability::BlockSize)
    .with(Capability::ReceiptOutcome);

/// What the generic transaction projection can fill.
const TRANSACTION_GROUPS: CapabilitySet = CapabilitySet::of(Capability::Header)
    .with(Capability::Body)
    .with(Capability::Senders)
    .with(Capability::Calldata)
    .with(Capability::ReceiptOutcome);

fn projection_mode(request: &DataRequest) -> Result<ProjectionMode, SourceError> {
    let required = request.required.with_derivable();
    if required.contains(Capability::BlobFields) || required.contains(Capability::BlockSize) {
        if !BLOBS_GROUPS.contains_all(required)
            || request.filters.scope.transaction_types.as_slice() != [3]
            || !request.filters.scope.addresses.is_empty()
            || !request.filters.scope.topics.is_empty()
            || !request.filters.senders.is_empty()
            || !request.filters.recipients.is_empty()
        {
            return Err(SourceError::InvalidPlan(
                "Xatu blob fields and block sizes come only from the type-3 blobs projection, without calldata or logs"
                    .to_owned(),
            ));
        }
        return Ok(ProjectionMode::Blobs);
    }
    if required.contains(Capability::Logs) {
        let allowed = CapabilitySet::of(Capability::Header).with(Capability::Logs);
        if !allowed.contains_all(required) {
            return Err(SourceError::InvalidPlan(
                "Xatu log projection cannot be combined with transaction or receipt material"
                    .to_owned(),
            ));
        }
        // The logs table names each log's transaction only by hash, so no
        // other transaction predicate could be applied to the logs claimed.
        let scope = request.filters.effective_scope();
        if !scope.transaction_types.is_empty()
            || !scope.senders.is_empty()
            || !scope.recipients.is_empty()
        {
            return Err(SourceError::InvalidPlan(
                "Xatu log projection can select a log's transaction only by hash".to_owned(),
            ));
        }
        return Ok(ProjectionMode::Logs);
    }
    if required.intersection(TRANSACTION_GROUPS.without(Capability::Header)) != CapabilitySet::NONE
    {
        if !TRANSACTION_GROUPS.contains_all(required) {
            return Err(SourceError::InvalidPlan(
                "Xatu transaction projection lacks requested capabilities".to_owned(),
            ));
        }
        return Ok(ProjectionMode::Transactions);
    }
    if required == CapabilitySet::of(Capability::Header) {
        return Ok(ProjectionMode::Headers);
    }
    Err(SourceError::InvalidPlan(
        "Xatu has no physical projection for the requested capability combination".to_owned(),
    ))
}

fn encode_partition(
    mode: ProjectionMode,
    range: BlockRange,
    request: &DataRequest,
) -> Result<Vec<u8>, SourceError> {
    postcard::to_allocvec(&PartitionIdentity {
        version: 3,
        mode,
        range,
        filters: request.filters.clone(),
        log_fields: request.log_fields,
        groups: request.required,
    })
    .map_err(|error| SourceError::InvalidPlan(error.to_string()))
}

fn decode_partition(bytes: &[u8]) -> Result<PartitionIdentity, SourceError> {
    let invalid = |error: postcard::Error| {
        SourceError::InvalidPlan(format!("invalid Xatu chunk identity: {error}"))
    };
    // postcard writes the leading `u8` version as one byte
    match bytes.first() {
        Some(3) => postcard::from_bytes::<PartitionIdentity>(bytes).map_err(invalid),
        Some(2) => postcard::from_bytes::<PartitionIdentityV2>(bytes)
            .map(PartitionIdentity::from)
            .map_err(invalid),
        version => Err(SourceError::InvalidPlan(format!(
            "unsupported Xatu chunk identity version {version:?}"
        ))),
    }
}

/// The field groups `mode` fills for `request`.
fn supplied_groups(mode: ProjectionMode, request: &DataRequest) -> CapabilitySet {
    let header = if request.required.contains(Capability::Header) {
        CapabilitySet::of(Capability::Header)
    } else {
        CapabilitySet::NONE
    };
    match mode {
        ProjectionMode::Blobs => BLOBS_GROUPS,
        ProjectionMode::Headers => CapabilitySet::of(Capability::Header),
        ProjectionMode::Logs => header.with(Capability::Logs),
        ProjectionMode::Transactions => {
            let columns = TransactionColumns::for_groups(request.required, &request.filters);
            [
                (columns.senders, Capability::Senders),
                (columns.calldata, Capability::Calldata),
                (columns.outcome, Capability::ReceiptOutcome),
            ]
            .into_iter()
            .filter(|(read, _)| *read)
            .fold(header.with(Capability::Body), |supplied, (_, group)| {
                supplied.with(group)
            })
        }
    }
}

#[allow(clippy::too_many_lines)]
fn physical_plan(mode: ProjectionMode, request: &DataRequest) -> Vec<PhysicalPlanOperation> {
    let range = format!(
        "block_number BETWEEN {} AND {}",
        request.range.start().0,
        request.range.end().0
    );
    let operation =
        |reader, table: &str, columns: &[&str], predicates: Vec<String>| PhysicalPlanOperation {
            reader,
            table: table.to_owned(),
            columns: columns.iter().map(|column| (*column).to_owned()).collect(),
            predicates,
            estimated_bytes: None,
            trust: TrustModel::TrustedDataset,
            completeness: "dataset_declared".to_owned(),
            derived: true,
        };
    if mode == ProjectionMode::Blobs {
        return vec![
            operation(
                PhysicalReader::Block,
                "canonical_execution_block",
                &[
                    "block_date_time",
                    "block_number",
                    "block_hash",
                    "gas_used",
                    "extra_data",
                    "base_fee_per_gas",
                ],
                vec![range.clone()],
            ),
            operation(
                PhysicalReader::Block,
                "canonical_beacon_block",
                &[
                    "slot",
                    "slot_start_date_time",
                    "block_version",
                    "block_total_bytes",
                    "execution_payload_block_hash",
                    "execution_payload_block_number",
                    "execution_payload_base_fee_per_gas",
                    "execution_payload_blob_gas_used",
                    "execution_payload_excess_blob_gas",
                    "execution_payload_gas_limit",
                    "execution_payload_gas_used",
                    "execution_payload_parent_hash",
                    "execution_payload_transactions_count",
                    "execution_payload_transactions_total_bytes",
                ],
                vec!["UTC date partitions derived from selected execution blocks".to_owned()],
            ),
            operation(
                PhysicalReader::Transaction,
                "canonical_beacon_block_execution_transaction",
                &[
                    "slot",
                    "position",
                    "hash",
                    "from",
                    "to",
                    "gas",
                    "type",
                    "size",
                    "blob_gas",
                    "blob_gas_fee_cap",
                    "blob_hashes",
                ],
                vec!["type = 3".to_owned()],
            ),
            operation(
                PhysicalReader::Receipt,
                "canonical_execution_transaction",
                &[
                    "block_number",
                    "transaction_index",
                    "transaction_hash",
                    "nonce",
                    "value",
                    "gas_used",
                    "gas_price",
                    "transaction_type",
                    "success",
                ],
                vec![range, "transaction_type = 3".to_owned()],
            ),
            operation(
                PhysicalReader::Block,
                "canonical_beacon_block_withdrawal",
                &[
                    "slot",
                    "withdrawal_index",
                    "withdrawal_validator_index",
                    "withdrawal_address",
                    "withdrawal_amount",
                ],
                vec!["slots selected by canonical beacon blocks".to_owned()],
            ),
        ];
    }
    let mut plan = vec![
        operation(
            PhysicalReader::Block,
            "canonical_execution_block",
            &["block_date_time", "block_number", "block_hash"],
            vec![range.clone()],
        ),
        operation(
            PhysicalReader::Block,
            "canonical_beacon_block",
            &[
                "slot_start_date_time",
                "execution_payload_block_hash",
                "execution_payload_block_number",
                "execution_payload_parent_hash",
                "execution_payload_transactions_count",
                "execution_payload_gas_limit",
                "execution_payload_gas_used",
                "execution_payload_base_fee_per_gas",
                "execution_payload_blob_gas_used",
                "execution_payload_excess_blob_gas",
            ],
            vec!["UTC date partitions derived from selected execution blocks".to_owned()],
        ),
    ];
    match mode {
        ProjectionMode::Headers => {}
        ProjectionMode::Transactions => plan.push(operation(
            PhysicalReader::Transaction,
            "canonical_execution_transaction",
            &TransactionColumns::for_groups(request.required, &request.filters).names(),
            transaction_predicates(request, range),
        )),
        ProjectionMode::Logs => {
            let columns = generic_log_columns(request.log_fields, &request.filters);
            plan.push(operation(
                PhysicalReader::Log,
                "canonical_execution_logs",
                &columns,
                log_predicates(request, range),
            ));
        }
        ProjectionMode::Blobs => unreachable!(),
    }
    plan
}

fn transaction_predicates(request: &DataRequest, range: String) -> Vec<String> {
    let scope = request.filters.effective_scope();
    let mut predicates = vec![range];
    if !scope.senders.is_empty() {
        predicates.push(format!("from_address IN ({} values)", scope.senders.len()));
    }
    if !scope.recipients.is_empty() {
        predicates.push(format!("to_address IN ({} values)", scope.recipients.len()));
    }
    if !scope.transaction_types.is_empty() {
        predicates.push(format!("transaction_type IN {:?}", scope.transaction_types));
    }
    predicates
}

fn log_predicates(request: &DataRequest, range: String) -> Vec<String> {
    let mut predicates = vec![range];
    if !request.filters.scope.addresses.is_empty() {
        predicates.push(format!(
            "address IN ({} values)",
            request.filters.scope.addresses.len()
        ));
    }
    for topic in &request.filters.scope.topics {
        predicates.push(format!(
            "topic{} IN ({} values)",
            topic.position,
            topic.alternatives.len()
        ));
    }
    predicates
}

fn source_error(error: XatuError, chunk_range: BlockRange) -> SourceError {
    match error {
        XatuError::Cancelled => SourceError::Cancelled,
        XatuError::Budget {
            resource,
            actual,
            limit,
        } => SourceError::BudgetExceeded {
            resource,
            limit,
            observed: actual,
        },
        XatuError::ObjectStore(detail) => SourceError::Unavailable(detail),
        // Another source may already hold what Xatu has not published.
        XatuError::NotPublished { location } => {
            debug!(location, "xatu has not published this object yet");
            SourceError::MissingRange(chunk_range)
        }
        error @ (XatuError::IncompleteRange { range, .. }
        | XatuError::IncompleteWithdrawals { range, .. }
        | XatuError::IncompleteLogs { range, .. }) => SourceError::IncompleteRange {
            range,
            detail: error.to_string(),
        },
        XatuError::Data(detail) => SourceError::CorruptFrame(detail),
        // A schema drift, or an object that cannot be pinned, does not clear
        // on retry.
        XatuError::Schema { table, missing } => SourceError::SchemaDrift {
            expected: format!("Xatu {table} columns {missing:?}"),
            actual: "an object without them".to_owned(),
        },
        XatuError::ColumnType {
            table,
            column,
            actual,
        } => SourceError::SchemaDrift {
            expected: format!("the declared encoding of Xatu {table} column {column}"),
            actual: format!("Arrow {actual}"),
        },
        error @ XatuError::Unpinned { .. } => SourceError::SchemaDrift {
            expected: "an object with a strong ETag".to_owned(),
            actual: error.to_string(),
        },
        other => SourceError::Protocol(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use crate::XatuTable;
    use leani_primitives::{FilterScope, Finality};
    use leani_source_api::{FieldProjection, FilterSet};

    use super::*;

    #[test]
    fn incomplete_table_coverage_is_a_failover_eligible_missing_range() {
        let range = BlockRange::new(BlockNumber(10), BlockNumber(20)).expect("range");
        assert_eq!(
            source_error(
                XatuError::IncompleteRange {
                    table: XatuTable::CanonicalBeaconBlock,
                    range,
                    expected: 11,
                    actual: 9,
                },
                range,
            ),
            SourceError::IncompleteRange {
                range,
                detail: "Xatu table canonical_beacon_block does not completely cover BlockRange { start: BlockNumber(10), end: BlockNumber(20) }: expected 11 rows, received 9".to_owned(),
            },
        );
    }

    #[test]
    fn not_published_maps_to_missing_range() {
        let range =
            BlockRange::new(BlockNumber(23_000_000), BlockNumber(23_000_999)).expect("range");
        // An object Xatu had not published yet read as an unavailable source,
        // which looked like a transient failure using up the job's attempts.
        assert_eq!(
            source_error(
                XatuError::NotPublished {
                    location: "canonical_execution_block/1000/23000000.parquet".to_owned(),
                },
                range,
            ),
            SourceError::MissingRange(range),
        );
    }

    /// The blobs-money shape: type-3 rows with their blob and outcome fields.
    fn request(range: BlockRange) -> DataRequest {
        DataRequest {
            chain_id: ChainId(1),
            range,
            required: groups(&[
                Capability::Header,
                Capability::Body,
                Capability::Senders,
                Capability::BlobFields,
                Capability::BlockSize,
                Capability::ReceiptOutcome,
            ]),
            allow_filtered: true,
            projection: FieldProjection::default(),
            log_fields: leani_primitives::LogFieldSet::NONE,
            filters: FilterSet {
                scope: FilterScope {
                    transaction_types: vec![3],
                    ..FilterScope::default()
                },
                ..FilterSet::default()
            },
            minimum_finality: Finality::Finalized,
            verification_policy: VerificationPolicy::TrustedDataset,
        }
    }

    #[tokio::test]
    async fn planner_rejects_pre_merge_history_before_acquisition() {
        let source = XatuHistorySource::new(XatuHistoryConfig::public("mainnet").expect("config"))
            .expect("source");
        let range =
            BlockRange::new(BlockNumber(10_000_835), BlockNumber(10_001_834)).expect("range");
        let error = source
            .plan(&request(range))
            .await
            .expect_err("unsupported range");
        assert!(error.to_string().contains("Beacon parent hashes"));
        assert_eq!(
            source
                .descriptor()
                .range
                .expect("advertised range")
                .start()
                .0,
            MAINNET_MERGE_BLOCK
        );
    }

    #[tokio::test]
    async fn planner_builds_an_exact_bounded_cover_without_network() {
        let mut config = XatuHistoryConfig::public("mainnet").expect("config");
        config.chunk_blocks = 1_000;
        config.blobs_chunk_blocks = 1_000;
        let source = XatuBlobsHistorySource::new(config).expect("source");
        let range =
            BlockRange::new(BlockNumber(19_426_589), BlockNumber(19_428_600)).expect("range");
        let plan = source.plan(&request(range)).await.expect("plan");
        plan.validate().expect("valid");
        assert_eq!(plan.chunks.len(), 3);
        assert_eq!(
            plan.chunks
                .iter()
                .map(|chunk| (chunk.range.start().0, chunk.range.end().0))
                .collect::<Vec<_>>(),
            vec![
                (19_426_589, 19_426_999),
                (19_427_000, 19_427_999),
                (19_428_000, 19_428_600),
            ]
        );
        let first = &plan.chunks[0];
        let sliced_range = BlockRange::new(
            BlockNumber(first.range.start().0.saturating_add(10)),
            BlockNumber(first.range.end().0.saturating_sub(10)),
        )
        .expect("sliced Xatu range");
        let sliced = source
            .slice_chunk(first, sliced_range)
            .expect("sliced Xatu chunk");
        assert_eq!(sliced.range, sliced_range);
        assert_eq!(
            decode_partition(&sliced.partition)
                .expect("sliced identity")
                .range,
            sliced_range
        );
        assert_eq!(
            source.coalescing_partition_identity(first),
            source.coalescing_partition_identity(&sliced)
        );
        assert!(
            source
                .plan(&DataRequest {
                    verification_policy: VerificationPolicy::CompleteCryptographic,
                    ..request(range)
                })
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn planner_uses_projection_aware_chunk_spans() {
        let source = XatuHistorySource::new(XatuHistoryConfig::public("mainnet").expect("config"))
            .expect("source");
        let range =
            BlockRange::new(BlockNumber(19_430_589), BlockNumber(19_450_588)).expect("range");

        let blobs = source.plan(&request(range)).await.expect("blobs plan");
        blobs.validate().expect("valid blobs plan");
        assert_eq!(blobs.chunks.len(), 4);
        assert_eq!(blobs.chunks[0].range.end().0, 19_431_999);
        assert_eq!(blobs.chunks[1].range.start().0, 19_432_000);
        assert_eq!(blobs.chunks[1].range.end().0, 19_439_999);

        let logs = source
            .plan(&DataRequest {
                required: CapabilitySet::of(Capability::Logs),
                filters: FilterSet {
                    scope: FilterScope {
                        addresses: vec![leani_primitives::Address::new([7; 20])],
                        ..FilterScope::default()
                    },
                    ..FilterSet::default()
                },
                ..request(range)
            })
            .await
            .expect("logs plan");
        logs.validate().expect("valid logs plan");
        assert_eq!(logs.chunks.len(), 21);
        assert_eq!(logs.chunks[0].range.end().0, 19_430_999);
        assert_eq!(logs.chunks[1].range.start().0, 19_431_000);
        assert_eq!(logs.chunks[1].range.end().0, 19_431_999);
    }

    #[test]
    fn partition_identity_round_trips_and_rejects_corruption() {
        let range = BlockRange::new(BlockNumber(1), BlockNumber(2)).expect("range");
        let request = request(range);
        let encoded =
            encode_partition(ProjectionMode::Blobs, range, &request).expect("encode identity");
        let decoded = decode_partition(&encoded).expect("decode");
        assert_eq!(decoded.range, range);
        assert_eq!(decoded.mode, ProjectionMode::Blobs);
        assert_eq!(decoded.filters, request.filters);
        assert_eq!(decoded.groups, request.required);
        assert!(decode_partition(&[0; 15]).is_err());
    }

    #[tokio::test]
    async fn planner_selects_only_log_or_transaction_material() {
        let source = XatuHistorySource::new(XatuHistoryConfig::public("mainnet").expect("config"))
            .expect("source");
        let range =
            BlockRange::new(BlockNumber(20_000_000), BlockNumber(20_000_010)).expect("range");
        let logs = source
            .plan(&DataRequest {
                required: CapabilitySet::of(Capability::Logs),
                filters: FilterSet {
                    scope: FilterScope {
                        addresses: vec![leani_primitives::Address::new([7; 20])],
                        ..FilterScope::default()
                    },
                    ..FilterSet::default()
                },
                ..request(range)
            })
            .await
            .expect("log plan");
        assert!(logs.physical_plan.iter().any(|operation| {
            operation.reader == PhysicalReader::Log && operation.table == "canonical_execution_logs"
        }));
        assert!(!logs.physical_plan.iter().any(|operation| {
            operation.table == "canonical_beacon_block_execution_transaction"
                || operation.table == "canonical_beacon_block_withdrawal"
        }));

        let transactions = source
            .plan(&DataRequest {
                required: groups(&[Capability::Body, Capability::Senders, Capability::Calldata]),
                filters: FilterSet {
                    senders: vec![leani_primitives::Address::new([8; 20])],
                    recipients: vec![leani_primitives::Address::new([9; 20])],
                    scope: FilterScope {
                        senders: vec![leani_primitives::Address::new([8; 20])],
                        recipients: vec![leani_primitives::Address::new([9; 20])],
                        ..FilterScope::default()
                    },
                },
                ..request(range)
            })
            .await
            .expect("transaction plan");
        let transaction_reader = transactions
            .physical_plan
            .iter()
            .find(|operation| operation.reader == PhysicalReader::Transaction)
            .expect("transaction reader");
        assert_eq!(transaction_reader.table, "canonical_execution_transaction");
        assert!(transaction_reader.columns.contains(&"value".to_owned()));
        assert!(
            !transactions
                .physical_plan
                .iter()
                .any(|operation| operation.reader == PhysicalReader::Receipt)
        );
    }

    fn groups(capabilities: &[Capability]) -> CapabilitySet {
        capabilities.iter().copied().collect()
    }

    fn transaction_columns(plan: &SourcePlan) -> Vec<String> {
        plan.physical_plan
            .iter()
            .find(|operation| operation.reader == PhysicalReader::Transaction)
            .expect("transaction reader")
            .columns
            .clone()
    }

    #[tokio::test]
    async fn planner_chooses_the_projection_from_field_groups() {
        use Capability::{
            BlobFields, BlockSize, Body, Calldata, Header, Logs, ReceiptOutcome, Receipts, Senders,
            Transactions,
        };
        let source = XatuHistorySource::new(XatuHistoryConfig::public("mainnet").expect("config"))
            .expect("source");
        let range =
            BlockRange::new(BlockNumber(20_000_000), BlockNumber(20_000_010)).expect("range");
        let recipients = FilterSet {
            scope: FilterScope {
                recipients: vec![leani_primitives::Address::new([9; 20])],
                ..FilterScope::default()
            },
            ..FilterSet::default()
        };
        let plan = |required: &[Capability], filters: FilterSet| {
            let request = DataRequest {
                required: groups(required),
                filters,
                ..request(range)
            };
            let source = &source;
            async move { source.plan(&request).await }
        };
        let mode = |plan: &SourcePlan| {
            decode_partition(&plan.chunks[0].partition)
                .expect("identity")
                .mode
        };

        // rollup-txs: receipt outcome comes from the transaction table
        let rollup = [Header, Body, Senders, Calldata, ReceiptOutcome];
        let rollup_plan = plan(&rollup, recipients.clone()).await.expect("rollup-txs");
        assert_eq!(mode(&rollup_plan), ProjectionMode::Transactions);
        assert_eq!(rollup_plan.supplied, groups(&rollup));
        let columns = transaction_columns(&rollup_plan);
        for column in ["from_address", "input", "gas_used", "gas_price", "success"] {
            assert!(columns.contains(&column.to_owned()), "{column}");
        }

        // transaction-stats reads neither calldata nor receipts
        let stats_plan = plan(&[Body, Senders], recipients.clone())
            .await
            .expect("transaction-stats");
        assert_eq!(stats_plan.supplied, groups(&[Body, Senders]));
        let columns = transaction_columns(&stats_plan);
        assert!(columns.contains(&"from_address".to_owned()));
        for column in ["input", "gas_used", "gas_price", "success"] {
            assert!(!columns.contains(&column.to_owned()), "{column}");
        }
        // a sender predicate reads senders even when no group needs them
        let mut senders = FilterSet::default();
        senders.scope.senders = vec![leani_primitives::Address::new([8; 20])];
        let body_plan = plan(&[Body], senders).await.expect("body");
        assert!(transaction_columns(&body_plan).contains(&"from_address".to_owned()));
        assert!(
            !transaction_columns(&plan(&[Body], recipients.clone()).await.expect("body"))
                .contains(&"from_address".to_owned())
        );

        // blobs-money keeps the type-3 blobs projection
        let blobs = [Header, Body, Senders, BlobFields, BlockSize, ReceiptOutcome];
        let blobs_plan = plan(&blobs, request(range).filters).await.expect("blobs");
        assert_eq!(mode(&blobs_plan), ProjectionMode::Blobs);
        assert_eq!(blobs_plan.supplied, groups(&blobs));

        let headers_plan = plan(&[Header], FilterSet::default())
            .await
            .expect("headers");
        assert_eq!(mode(&headers_plan), ProjectionMode::Headers);
        assert_eq!(headers_plan.supplied, groups(&[Header]));

        // Calldata never joins the blobs projection, and bundles promise
        // fields no projection fills: transactions derive blob fields,
        // receipts derive logs.
        for required in [
            &[Calldata, BlobFields][..],
            &[Header, Transactions, Receipts],
            &[Header, Transactions],
            &[Header, Receipts],
            &[Logs, ReceiptOutcome],
            &[BlockSize, Calldata],
        ] {
            let result = plan(required, request(range).filters).await;
            assert!(
                matches!(result, Err(SourceError::InvalidPlan(_))),
                "{required:?}: {result:?}"
            );
        }
    }

    #[test]
    fn version_two_partitions_open_as_their_field_groups() {
        use Capability::{
            BlobFields, BlockSize, Body, Calldata, Header, Logs, ReceiptOutcome, Senders,
        };
        let range =
            BlockRange::new(BlockNumber(20_000_000), BlockNumber(20_000_999)).expect("range");
        for (mode, include_header, expected) in [
            (ProjectionMode::Headers, true, groups(&[Header])),
            (
                ProjectionMode::Transactions,
                true,
                groups(&[Header, Body, Senders, Calldata]),
            ),
            (
                ProjectionMode::Transactions,
                false,
                groups(&[Body, Senders, Calldata]),
            ),
            (ProjectionMode::Logs, false, groups(&[Logs])),
            (
                ProjectionMode::Blobs,
                true,
                groups(&[Header, Senders, BlobFields, BlockSize, ReceiptOutcome]),
            ),
        ] {
            let bytes = postcard::to_allocvec(&PartitionIdentityV2 {
                version: 2,
                mode,
                range,
                filters: FilterSet::default(),
                log_fields: leani_primitives::LogFieldSet::NONE,
                include_header,
            })
            .expect("v2 identity");
            let identity = decode_partition(&bytes).expect("v2 decodes");
            assert_eq!(identity.mode, mode);
            assert_eq!(identity.range, range);
            assert_eq!(identity.groups, expected, "{mode:?}");
        }
    }

    #[test]
    fn schema_drift_is_not_retried() {
        // Review M1: a permanent column-type or missing-column drift mapped to
        // a retryable protocol error.
        let drifts = [
            XatuError::ColumnType {
                table: XatuTable::CanonicalExecutionBlock,
                column: "extra_data".to_owned(),
                actual: "Int64".to_owned(),
            },
            XatuError::Schema {
                table: XatuTable::CanonicalExecutionBlock,
                missing: vec!["block_hash".to_owned()],
            },
        ];
        for drift in drifts {
            let error = source_error(drift, BlockRange::single(BlockNumber(20_000_000)));
            assert!(
                matches!(error, SourceError::SchemaDrift { .. }),
                "{error:?}"
            );
        }
    }

    #[test]
    fn xatu_history_is_mainnet_only() {
        // Audit History-3: every frame is stamped as chain 1, whatever the
        // catalog network.
        assert!(matches!(
            XatuHistoryConfig::public("sepolia"),
            Err(XatuError::UnsupportedNetwork(network)) if network == "sepolia"
        ));
        assert!(matches!(
            crate::XatuCatalogConfig::public("holesky"),
            Err(XatuError::UnsupportedNetwork(_))
        ));
        let mut config = XatuHistoryConfig::public("mainnet").expect("mainnet");
        config.catalog.network = "hoodi".to_owned();
        assert!(matches!(
            XatuHistorySource::new(config),
            Err(XatuError::UnsupportedNetwork(_))
        ));
    }

    #[tokio::test]
    async fn unsupported_projection_fails_before_open() {
        let source = XatuHistorySource::new(XatuHistoryConfig::public("mainnet").expect("config"))
            .expect("source");
        let range = BlockRange::single(BlockNumber(20_000_000));
        let result = source
            .plan(&DataRequest {
                required: CapabilitySet::of(Capability::Logs).with(Capability::Transactions),
                ..request(range)
            })
            .await;
        assert!(matches!(result, Err(SourceError::InvalidPlan(_))));

        // Logs name their transaction only by hash, so a log projection never
        // claims another transaction predicate it could not apply.
        let address = leani_primitives::Address::new([8; 20]);
        let logs = |filters| DataRequest {
            required: CapabilitySet::of(Capability::Logs),
            filters,
            ..request(range)
        };
        for filters in [
            request(range).filters,
            FilterSet {
                senders: vec![address],
                ..FilterSet::default()
            },
            FilterSet {
                scope: FilterScope {
                    recipients: vec![address],
                    ..FilterScope::default()
                },
                ..FilterSet::default()
            },
        ] {
            let result = source.plan(&logs(filters)).await;
            assert!(
                matches!(&result, Err(SourceError::InvalidPlan(detail)) if detail.contains("only by hash")),
                "{result:?}"
            );
        }
        source
            .plan(&logs(FilterSet {
                scope: FilterScope {
                    transaction_hashes: vec![leani_primitives::TransactionHash::new([9; 32])],
                    ..FilterScope::default()
                },
                ..FilterSet::default()
            }))
            .await
            .expect("a log's transaction can be selected by hash");
    }
}
