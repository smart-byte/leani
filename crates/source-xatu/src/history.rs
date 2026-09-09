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

use crate::{
    XatuBlobsProjector, XatuCatalog, XatuCatalogConfig, XatuError,
    projection::{GenericProjectionKind, generic_log_columns},
};

const SCHEMA_VERSION: &str = "xatu.history.v3";
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
    /// # Errors
    ///
    /// Returns an error for an invalid portable Xatu network name.
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
    /// Returns an error for zero chunk size or an invalid object-store
    /// configuration.
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
                expected_lag: Duration::from_mins(15),
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
            supplied: request.required,
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
                        identity.include_header,
                        budget,
                        self.batch_rows,
                        cancellation,
                    )
                    .await
            }
        }
        .map_err(source_error)?;
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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct PartitionIdentity {
    version: u8,
    mode: ProjectionMode,
    range: BlockRange,
    filters: leani_source_api::FilterSet,
    log_fields: leani_primitives::LogFieldSet,
    include_header: bool,
}

fn projection_mode(request: &DataRequest) -> Result<ProjectionMode, SourceError> {
    let required = request.required;
    if required.contains(Capability::Receipts) {
        let allowed = CapabilitySet::of(Capability::Header)
            .with(Capability::Transactions)
            .with(Capability::Receipts);
        if !allowed.contains_all(required)
            || request.filters.scope.transaction_types.as_slice() != [3]
            || !request.filters.scope.addresses.is_empty()
            || !request.filters.scope.topics.is_empty()
            || !request.filters.senders.is_empty()
            || !request.filters.recipients.is_empty()
        {
            return Err(SourceError::InvalidPlan(
                "Xatu receipt projection supports only blobs/type-3 transaction material"
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
        return Ok(ProjectionMode::Logs);
    }
    if required.contains(Capability::Transactions)
        || required.contains(Capability::Body)
        || required.contains(Capability::Calldata)
    {
        let allowed = CapabilitySet::of(Capability::Header)
            .with(Capability::Body)
            .with(Capability::Transactions)
            .with(Capability::Calldata);
        if !allowed.contains_all(required) {
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
        version: 2,
        mode,
        range,
        filters: request.filters.clone(),
        log_fields: request.log_fields,
        include_header: request.required.contains(Capability::Header),
    })
    .map_err(|error| SourceError::InvalidPlan(error.to_string()))
}

fn decode_partition(bytes: &[u8]) -> Result<PartitionIdentity, SourceError> {
    let identity: PartitionIdentity = postcard::from_bytes(bytes).map_err(|error| {
        SourceError::InvalidPlan(format!("invalid Xatu chunk identity: {error}"))
    })?;
    if identity.version != 2 {
        return Err(SourceError::InvalidPlan(format!(
            "unsupported Xatu chunk identity version {}",
            identity.version
        )));
    }
    Ok(identity)
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
            ],
            vec!["UTC date partitions derived from selected execution blocks".to_owned()],
        ),
    ];
    match mode {
        ProjectionMode::Headers => {}
        ProjectionMode::Transactions => plan.push(operation(
            PhysicalReader::Transaction,
            "canonical_execution_transaction",
            &[
                "block_number",
                "transaction_index",
                "transaction_hash",
                "nonce",
                "from_address",
                "to_address",
                "value",
                "input",
                "gas_limit",
                "transaction_type",
            ],
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
    let mut predicates = vec![range];
    if !request.filters.senders.is_empty() {
        predicates.push(format!(
            "from_address IN ({} values)",
            request.filters.senders.len()
        ));
    }
    if !request.filters.recipients.is_empty() {
        predicates.push(format!(
            "to_address IN ({} values)",
            request.filters.recipients.len()
        ));
    }
    if !request.filters.scope.transaction_types.is_empty() {
        predicates.push(format!(
            "transaction_type IN {:?}",
            request.filters.scope.transaction_types
        ));
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

fn source_error(error: XatuError) -> SourceError {
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
        error @ XatuError::IncompleteRange { range, .. } => SourceError::IncompleteRange {
            range,
            detail: error.to_string(),
        },
        XatuError::Data(detail) => SourceError::CorruptFrame(detail),
        XatuError::Schema { table, missing } => {
            SourceError::Protocol(format!("Xatu {table} schema is missing {missing:?}"))
        }
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
            source_error(XatuError::IncompleteRange {
                table: XatuTable::CanonicalBeaconBlock,
                range,
                expected: 11,
                actual: 9,
            }),
            SourceError::IncompleteRange {
                range,
                detail: "Xatu table canonical_beacon_block does not completely cover BlockRange { start: BlockNumber(10), end: BlockNumber(20) }: expected 11 rows, received 9".to_owned(),
            },
        );
    }

    fn request(range: BlockRange) -> DataRequest {
        DataRequest {
            chain_id: ChainId(1),
            range,
            required: CapabilitySet::of(Capability::Header)
                .with(Capability::Transactions)
                .with(Capability::Receipts),
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
                required: CapabilitySet::of(Capability::Transactions),
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
    }
}
