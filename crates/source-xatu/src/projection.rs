//! Bounded Arrow projection and normalization for the blobs reference workload.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use arrow_array::{
    Array, BinaryArray, BooleanArray, FixedSizeBinaryArray, LargeBinaryArray, LargeListArray,
    LargeStringArray, ListArray, RecordBatch, StringArray, TimestampMillisecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use futures::StreamExt;
use leani_primitives::{
    Address, BlockFrame, BlockHash, BlockNumber, BlockRange, BlockRef, ChainId, Completeness,
    FilterScope, Finality, HeaderEnvelope, Log, LogField, LogFieldSet, Material, MissingReason,
    ObjectIdentity, Provenance, Quantity, ReceiptEnvelope, SourceId, SourceKind,
    TransactionEnvelope, TransactionHash, TrustModel, VerificationReport,
};
use leani_source_api::{FilterSet, SourceBudget};
use object_store::{ObjectStore, ObjectStoreExt, path::Path as ObjectPath};
use parquet::{
    arrow::{ProjectionMask, async_reader::ParquetRecordBatchStreamBuilder},
    file::metadata::ParquetMetaData,
};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::{
    CatalogObject, XatuCatalog, XatuDate, XatuError, XatuTable, reader::ObjectStoreReader,
};

const EXECUTION_BLOCK_COLUMNS: &[&str] = &[
    "block_date_time",
    "block_number",
    "block_hash",
    "gas_used",
    "extra_data",
    "base_fee_per_gas",
];
const EXECUTION_TRANSACTION_COLUMNS: &[&str] = &[
    "block_number",
    "transaction_index",
    "transaction_hash",
    "gas_used",
    "gas_price",
    "transaction_type",
    "success",
];
const BEACON_BLOCK_COLUMNS: &[&str] = &[
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
];
const BEACON_TRANSACTION_COLUMNS: &[&str] = &[
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
];
const WITHDRAWAL_COLUMNS: &[&str] = &[
    "slot",
    "withdrawal_index",
    "withdrawal_validator_index",
    "withdrawal_address",
    "withdrawal_amount",
];
const GENERIC_EXECUTION_BLOCK_COLUMNS: &[&str] = &["block_date_time", "block_number", "block_hash"];
const GENERIC_BEACON_BLOCK_COLUMNS: &[&str] = &[
    "slot_start_date_time",
    "execution_payload_block_hash",
    "execution_payload_block_number",
    "execution_payload_parent_hash",
    "execution_payload_transactions_count",
];
const GENERIC_TRANSACTION_COLUMNS: &[&str] = &[
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
];
const GENERIC_LOG_COLUMNS: &[&str] = &[
    "block_number",
    "transaction_index",
    "transaction_hash",
    "log_index",
    "address",
    "topic0",
    "topic1",
    "topic2",
    "topic3",
    "data",
];

pub(crate) fn generic_log_columns(
    log_fields: LogFieldSet,
    filters: &FilterSet,
) -> Vec<&'static str> {
    GENERIC_LOG_COLUMNS
        .iter()
        .copied()
        .filter(|column| {
            *column != "transaction_hash"
                || log_fields.contains(LogField::TransactionHash)
                || !filters.scope.transaction_hashes.is_empty()
        })
        .collect()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GenericProjectionKind {
    Headers,
    Transactions,
    Logs,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProjectionObjectMetrics {
    pub table: XatuTable,
    pub partition: String,
    pub locator: String,
    pub e_tag: Option<String>,
    pub object_bytes: u64,
    pub projected_compressed_bytes: u64,
    #[serde(default)]
    pub logical_range_requests: u64,
    #[serde(default)]
    pub fetched_bytes: u64,
    pub selected_columns: Vec<String>,
    pub rows_scanned: u64,
    pub rows_selected: u64,
    pub batches: u64,
    pub peak_batch_memory_bytes: u64,
    pub elapsed_ms: u64,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProjectionMetrics {
    pub objects: Vec<ProjectionObjectMetrics>,
    pub object_bytes: u64,
    pub projected_compressed_bytes: u64,
    pub logical_range_requests: u64,
    pub fetched_bytes: u64,
    pub rows_scanned: u64,
    pub rows_selected: u64,
    pub frames: u64,
    pub blob_transactions: u64,
    pub elapsed_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BlobsProjection {
    pub range: BlockRange,
    pub frames: Vec<BlockFrame>,
    pub metrics: ProjectionMetrics,
}

#[derive(Clone, Debug)]
pub struct XatuBlobsProjector {
    catalog: XatuCatalog,
}

impl XatuBlobsProjector {
    #[must_use]
    pub const fn new(catalog: XatuCatalog) -> Self {
        Self { catalog }
    }

    /// Project the five Xatu tables needed by the blobs reference processor.
    ///
    /// Rows are streamed in bounded Arrow batches, filtered immediately, and
    /// joined into source-neutral frames. No Parquet object is materialized on
    /// local disk.
    ///
    /// # Errors
    ///
    /// Fails closed on missing columns, malformed values, cross-table
    /// disagreement, missing joins, cancellation, or a hard budget violation.
    #[allow(clippy::too_many_lines)]
    pub async fn project(
        &self,
        range: BlockRange,
        start_date: XatuDate,
        end_date: XatuDate,
        budget: SourceBudget,
        batch_rows: usize,
        cancellation: CancellationToken,
    ) -> Result<BlobsProjection, XatuError> {
        self.project_inner(
            range,
            Some((start_date, end_date)),
            budget,
            batch_rows,
            cancellation,
        )
        .await
    }

    /// Project a block range and derive its UTC beacon partitions from the
    /// execution-block timestamps.
    ///
    /// This is the scheduler-facing form: it avoids requiring a source-specific
    /// date in the generic [`leani_source_api::SourceChunk`] contract.
    ///
    /// # Errors
    ///
    /// Fails under the same strict schema, join, cancellation, and budget
    /// conditions as [`Self::project`], or if no execution rows are available
    /// from which to derive dates.
    pub async fn project_auto_dates(
        &self,
        range: BlockRange,
        budget: SourceBudget,
        batch_rows: usize,
        cancellation: CancellationToken,
    ) -> Result<BlobsProjection, XatuError> {
        self.project_inner(range, None, budget, batch_rows, cancellation)
            .await
    }

    #[allow(clippy::too_many_lines)]
    async fn project_inner(
        &self,
        range: BlockRange,
        dates: Option<(XatuDate, XatuDate)>,
        budget: SourceBudget,
        batch_rows: usize,
        cancellation: CancellationToken,
    ) -> Result<BlobsProjection, XatuError> {
        budget
            .validate()
            .map_err(|error| XatuError::Data(error.to_string()))?;
        if batch_rows == 0 {
            return Err(XatuError::Data(
                "Parquet batch rows must be greater than zero".to_owned(),
            ));
        }
        let started = Instant::now();
        let mut projected_bytes = 0_u64;
        let mut metrics = Vec::new();
        let mut execution_blocks = BTreeMap::new();
        let mut beacon_blocks = BTreeMap::new();
        let mut execution_transactions = BTreeMap::new();
        let mut beacon_transactions = Vec::new();
        let mut transaction_sizes = BTreeMap::new();
        let mut withdrawals = BTreeMap::new();

        for object in self
            .catalog
            .execution_objects(XatuTable::CanonicalExecutionBlock, range)?
        {
            metrics.push(
                read_projected(
                    self.catalog.store(),
                    &object,
                    EXECUTION_BLOCK_COLUMNS,
                    budget,
                    batch_rows,
                    &mut projected_bytes,
                    &cancellation,
                    |batch| parse_execution_blocks(batch, range, &mut execution_blocks),
                )
                .await?,
            );
        }
        let (start_date, end_date) =
            if let Some(dates) = dates {
                dates
            } else {
                let first = execution_blocks.values().next().ok_or_else(|| {
                    XatuError::Data("execution range returned no blocks".to_owned())
                })?;
                let last = execution_blocks.values().next_back().ok_or_else(|| {
                    XatuError::Data("execution range returned no blocks".to_owned())
                })?;
                (
                    XatuDate::from_unix_seconds(first.timestamp)?,
                    XatuDate::from_unix_seconds(last.timestamp)?,
                )
            };
        for object in
            self.catalog
                .daily_objects(XatuTable::CanonicalBeaconBlock, start_date, end_date)?
        {
            metrics.push(
                read_projected(
                    self.catalog.store(),
                    &object,
                    BEACON_BLOCK_COLUMNS,
                    budget,
                    batch_rows,
                    &mut projected_bytes,
                    &cancellation,
                    |batch| parse_beacon_blocks(batch, range, &mut beacon_blocks),
                )
                .await?,
            );
        }
        let slots = beacon_blocks
            .values()
            .map(|block| block.slot)
            .collect::<BTreeSet<_>>();
        for object in self.catalog.daily_objects(
            XatuTable::CanonicalBeaconBlockExecutionTransaction,
            start_date,
            end_date,
        )? {
            metrics.push(
                read_projected(
                    self.catalog.store(),
                    &object,
                    BEACON_TRANSACTION_COLUMNS,
                    budget,
                    batch_rows,
                    &mut projected_bytes,
                    &cancellation,
                    |batch| {
                        parse_beacon_transactions(
                            batch,
                            &slots,
                            &mut beacon_transactions,
                            &mut transaction_sizes,
                        )
                    },
                )
                .await?,
            );
        }
        for object in self.catalog.daily_objects(
            XatuTable::CanonicalBeaconBlockWithdrawal,
            start_date,
            end_date,
        )? {
            metrics.push(
                read_projected(
                    self.catalog.store(),
                    &object,
                    WITHDRAWAL_COLUMNS,
                    budget,
                    batch_rows,
                    &mut projected_bytes,
                    &cancellation,
                    |batch| parse_withdrawals(batch, &slots, &mut withdrawals),
                )
                .await?,
            );
        }
        for object in self
            .catalog
            .execution_objects(XatuTable::CanonicalExecutionTransaction, range)?
        {
            metrics.push(
                read_projected(
                    self.catalog.store(),
                    &object,
                    EXECUTION_TRANSACTION_COLUMNS,
                    budget,
                    batch_rows,
                    &mut projected_bytes,
                    &cancellation,
                    |batch| parse_execution_transactions(batch, range, &mut execution_transactions),
                )
                .await?,
            );
        }

        let provenance = metrics
            .iter()
            .map(|metric| metric.provenance(range))
            .collect::<Result<Vec<_>, _>>()?;
        let frames = normalize_frames(NormalizationInputs {
            range,
            execution_blocks: &execution_blocks,
            beacon_blocks: &beacon_blocks,
            beacon_transactions,
            transaction_sizes: &transaction_sizes,
            withdrawals: &withdrawals,
            execution_transactions: &execution_transactions,
            provenance: &provenance,
        })?;
        let frame_count = u64::try_from(frames.len()).unwrap_or(u64::MAX);
        if frame_count > budget.max_frames {
            return Err(XatuError::Budget {
                resource: "frames",
                actual: frame_count,
                limit: budget.max_frames,
            });
        }
        for frame in &frames {
            let bytes = frame.estimated_heap_bytes();
            if bytes > budget.max_frame_bytes {
                return Err(XatuError::Budget {
                    resource: "frame_bytes",
                    actual: bytes,
                    limit: budget.max_frame_bytes,
                });
            }
        }

        let blob_transactions = frames
            .iter()
            .map(|frame| {
                frame
                    .transactions
                    .as_present()
                    .map_or(0_u64, |transactions| {
                        u64::try_from(transactions.len()).unwrap_or(u64::MAX)
                    })
            })
            .sum();
        let summary = ProjectionMetrics {
            object_bytes: metrics.iter().map(|metric| metric.object_bytes).sum(),
            projected_compressed_bytes: metrics
                .iter()
                .map(|metric| metric.projected_compressed_bytes)
                .sum(),
            logical_range_requests: metrics
                .iter()
                .map(|metric| metric.logical_range_requests)
                .sum(),
            fetched_bytes: metrics.iter().map(|metric| metric.fetched_bytes).sum(),
            rows_scanned: metrics.iter().map(|metric| metric.rows_scanned).sum(),
            rows_selected: metrics.iter().map(|metric| metric.rows_selected).sum(),
            frames: frame_count,
            blob_transactions,
            elapsed_ms: elapsed_ms(started),
            objects: metrics,
        };
        Ok(BlobsProjection {
            range,
            frames,
            metrics: summary,
        })
    }

    /// Project a capability-specific header, transaction, or log view.
    ///
    /// The execution block table supplies exact block timestamps used to
    /// resolve daily beacon partitions. The beacon table supplies canonical
    /// execution parent hashes, which the execution-only public tables omit.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub(crate) async fn project_generic(
        &self,
        range: BlockRange,
        kind: GenericProjectionKind,
        filters: &FilterSet,
        log_fields: LogFieldSet,
        include_header: bool,
        budget: SourceBudget,
        batch_rows: usize,
        cancellation: CancellationToken,
    ) -> Result<BlobsProjection, XatuError> {
        budget
            .validate()
            .map_err(|error| XatuError::Data(error.to_string()))?;
        if batch_rows == 0 {
            return Err(XatuError::Data(
                "Parquet batch rows must be greater than zero".to_owned(),
            ));
        }
        let started = Instant::now();
        let mut projected_bytes = 0_u64;
        let mut metrics = Vec::new();
        let mut execution_blocks = BTreeMap::new();
        for object in self
            .catalog
            .execution_objects(XatuTable::CanonicalExecutionBlock, range)?
        {
            metrics.push(
                read_projected(
                    self.catalog.store(),
                    &object,
                    GENERIC_EXECUTION_BLOCK_COLUMNS,
                    budget,
                    batch_rows,
                    &mut projected_bytes,
                    &cancellation,
                    |batch| parse_generic_execution_blocks(batch, range, &mut execution_blocks),
                )
                .await?,
            );
        }
        let first = execution_blocks
            .values()
            .next()
            .ok_or_else(|| XatuError::Data("execution range returned no blocks".to_owned()))?;
        let last = execution_blocks
            .values()
            .next_back()
            .ok_or_else(|| XatuError::Data("execution range returned no blocks".to_owned()))?;
        let start_date = XatuDate::from_unix_seconds(first.timestamp)?;
        let end_date = XatuDate::from_unix_seconds(last.timestamp)?;

        let mut beacon_blocks = BTreeMap::new();
        for object in
            self.catalog
                .daily_objects(XatuTable::CanonicalBeaconBlock, start_date, end_date)?
        {
            metrics.push(
                read_projected(
                    self.catalog.store(),
                    &object,
                    GENERIC_BEACON_BLOCK_COLUMNS,
                    budget,
                    batch_rows,
                    &mut projected_bytes,
                    &cancellation,
                    |batch| parse_generic_beacon_blocks(batch, range, &mut beacon_blocks),
                )
                .await?,
            );
        }

        let mut transactions = BTreeMap::<u64, Vec<TransactionEnvelope>>::new();
        let mut logs = BTreeMap::<u64, Vec<Log>>::new();
        match kind {
            GenericProjectionKind::Headers => {}
            GenericProjectionKind::Transactions => {
                for object in self
                    .catalog
                    .execution_objects(XatuTable::CanonicalExecutionTransaction, range)?
                {
                    metrics.push(
                        read_projected(
                            self.catalog.store(),
                            &object,
                            GENERIC_TRANSACTION_COLUMNS,
                            budget,
                            batch_rows,
                            &mut projected_bytes,
                            &cancellation,
                            |batch| {
                                parse_generic_transactions(batch, range, filters, &mut transactions)
                            },
                        )
                        .await?,
                    );
                }
            }
            GenericProjectionKind::Logs => {
                let columns = generic_log_columns(log_fields, filters);
                for object in self
                    .catalog
                    .execution_objects(XatuTable::CanonicalExecutionLogs, range)?
                {
                    metrics.push(
                        read_projected(
                            self.catalog.store(),
                            &object,
                            &columns,
                            budget,
                            batch_rows,
                            &mut projected_bytes,
                            &cancellation,
                            |batch| {
                                parse_generic_logs(batch, range, filters, log_fields, &mut logs)
                            },
                        )
                        .await?,
                    );
                }
            }
        }

        let provenance = metrics
            .iter()
            .map(|metric| metric.provenance(range))
            .collect::<Result<Vec<_>, _>>()?;
        let frames = normalize_generic_frames(GenericNormalizationInputs {
            range,
            kind,
            include_header,
            filters,
            execution_blocks: &execution_blocks,
            beacon_blocks: &beacon_blocks,
            transactions,
            logs,
            provenance: &provenance,
        })?;
        validate_frame_budget(&frames, budget)?;
        let frame_count = u64::try_from(frames.len()).unwrap_or(u64::MAX);
        Ok(BlobsProjection {
            range,
            metrics: ProjectionMetrics {
                object_bytes: metrics.iter().map(|metric| metric.object_bytes).sum(),
                projected_compressed_bytes: metrics
                    .iter()
                    .map(|metric| metric.projected_compressed_bytes)
                    .sum(),
                logical_range_requests: metrics
                    .iter()
                    .map(|metric| metric.logical_range_requests)
                    .sum(),
                fetched_bytes: metrics.iter().map(|metric| metric.fetched_bytes).sum(),
                rows_scanned: metrics.iter().map(|metric| metric.rows_scanned).sum(),
                rows_selected: metrics.iter().map(|metric| metric.rows_selected).sum(),
                frames: frame_count,
                blob_transactions: 0,
                elapsed_ms: elapsed_ms(started),
                objects: metrics,
            },
            frames,
        })
    }
}

impl ProjectionObjectMetrics {
    fn provenance(&self, range: BlockRange) -> Result<Provenance, XatuError> {
        Ok(Provenance {
            source_id: SourceId::new("xatu-public")
                .map_err(|error| XatuError::Data(error.to_string()))?,
            source_kind: SourceKind::PublicDataset,
            trust: TrustModel::TrustedDataset,
            range: Some(range),
            object: Some(ObjectIdentity {
                locator: self.locator.clone(),
                version: self.e_tag.clone(),
                checksum: None,
                schema: Some(format!("xatu.{}.v1", self.table.name())),
            }),
            observed_at_unix_ms: now_unix_ms(),
            projection: self.selected_columns.clone(),
        })
    }
}

#[allow(clippy::too_many_arguments)]
async fn read_projected(
    store: Arc<dyn ObjectStore>,
    object: &CatalogObject,
    columns: &[&str],
    budget: SourceBudget,
    batch_rows: usize,
    projected_bytes_used: &mut u64,
    cancellation: &CancellationToken,
    mut consume: impl FnMut(&RecordBatch) -> Result<u64, XatuError>,
) -> Result<ProjectionObjectMetrics, XatuError> {
    if cancellation.is_cancelled() {
        return Err(XatuError::Cancelled);
    }
    let started = Instant::now();
    let location = ObjectPath::parse(&object.location)
        .map_err(|error| XatuError::ObjectStore(error.to_string()))?;
    let head = store
        .head(&location)
        .await
        .map_err(|error| XatuError::ObjectStore(error.to_string()))?;
    let (reader, reader_metrics) = ObjectStoreReader::new(Arc::clone(&store), location, head.size);
    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .map_err(|error| XatuError::Parquet(error.to_string()))?;
    let schema = builder.metadata().file_metadata().schema_descr();
    let root_indices = selected_root_indices(schema, columns)?;
    let projected_compressed_bytes = projected_compressed_bytes(builder.metadata(), &root_indices);
    let next_bytes = projected_bytes_used.saturating_add(projected_compressed_bytes);
    if next_bytes > budget.max_input_bytes {
        return Err(XatuError::Budget {
            resource: "projected_input_bytes",
            actual: next_bytes,
            limit: budget.max_input_bytes,
        });
    }
    *projected_bytes_used = next_bytes;
    let projection = ProjectionMask::roots(schema, root_indices);
    let mut stream = builder
        .with_batch_size(batch_rows)
        .with_projection(projection)
        .build()
        .map_err(|error| XatuError::Parquet(error.to_string()))?;
    let mut rows_scanned = 0_u64;
    let mut rows_selected = 0_u64;
    let mut batches = 0_u64;
    let mut peak_batch_memory_bytes = 0_u64;
    loop {
        let next = tokio::select! {
            () = cancellation.cancelled() => return Err(XatuError::Cancelled),
            next = stream.next() => next,
        };
        let Some(batch) = next else {
            break;
        };
        let batch = batch.map_err(|error| XatuError::Parquet(error.to_string()))?;
        rows_scanned =
            rows_scanned.saturating_add(u64::try_from(batch.num_rows()).unwrap_or(u64::MAX));
        rows_selected = rows_selected.saturating_add(consume(&batch)?);
        batches = batches.saturating_add(1);
        peak_batch_memory_bytes = peak_batch_memory_bytes
            .max(u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX));
    }

    let reader_metrics = reader_metrics.snapshot();
    Ok(ProjectionObjectMetrics {
        table: object.table,
        partition: object.partition.clone(),
        locator: object.url.to_string(),
        e_tag: head.e_tag,
        object_bytes: head.size,
        projected_compressed_bytes,
        logical_range_requests: reader_metrics.logical_range_requests,
        fetched_bytes: reader_metrics.returned_bytes,
        selected_columns: columns.iter().map(|column| (*column).to_owned()).collect(),
        rows_scanned,
        rows_selected,
        batches,
        peak_batch_memory_bytes,
        elapsed_ms: elapsed_ms(started),
    })
}

fn selected_root_indices(
    schema: &parquet::schema::types::SchemaDescriptor,
    columns: &[&str],
) -> Result<Vec<usize>, XatuError> {
    let fields = schema.root_schema().get_fields();
    columns
        .iter()
        .map(|name| {
            fields
                .iter()
                .position(|field| field.name() == *name)
                .ok_or_else(|| XatuError::Data(format!("projected column {name:?} is missing")))
        })
        .collect()
}

fn projected_compressed_bytes(metadata: &ParquetMetaData, root_indices: &[usize]) -> u64 {
    let roots = root_indices.iter().copied().collect::<BTreeSet<_>>();
    let schema = metadata.file_metadata().schema_descr();
    metadata
        .row_groups()
        .iter()
        .flat_map(|group| group.columns().iter().enumerate())
        .filter(|(leaf, _)| roots.contains(&schema.get_column_root_idx(*leaf)))
        .map(|(_, column)| u64::try_from(column.compressed_size()).unwrap_or(0))
        .sum()
}

#[derive(Clone, Debug)]
struct ExecutionBlockRow {
    number: u64,
    hash: BlockHash,
    timestamp: u64,
    gas_used: u64,
    extra_data: Vec<u8>,
    base_fee_per_gas: u64,
}

#[derive(Clone, Debug)]
struct BeaconBlockRow {
    slot: u64,
    number: u64,
    hash: BlockHash,
    parent_hash: BlockHash,
    timestamp: u64,
    consensus_size_bytes: u64,
    fork: ExecutionBlockFork,
    base_fee_per_gas: Quantity,
    blob_gas_used: u64,
    excess_blob_gas: u64,
    gas_limit: u64,
    gas_used: u64,
    transaction_count: u32,
    transactions_total_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExecutionBlockFork {
    Cancun,
    Prague,
}

#[derive(Clone, Debug)]
struct BeaconTransactionRow {
    slot: u64,
    index: u32,
    hash: TransactionHash,
    from: Address,
    to: Option<Address>,
    gas_limit: u64,
    size_bytes: u32,
    blob_gas: u64,
    max_fee_per_blob_gas: Quantity,
    blob_hashes: Vec<BlockHash>,
}

#[derive(Clone, Debug)]
struct ExecutionTransactionRow {
    block_number: u64,
    index: u32,
    hash: TransactionHash,
    gas_used: u64,
    gas_price: Quantity,
    success: bool,
}

#[derive(Clone, Copy, Debug)]
struct TransactionSizeRow {
    transaction_type: u8,
    size_bytes: u32,
}

#[derive(Clone, Copy, Debug)]
struct WithdrawalRow {
    index: u64,
    validator_index: u64,
    address: Address,
    amount_gwei: u64,
}

#[derive(Clone, Debug)]
struct GenericExecutionBlockRow {
    hash: BlockHash,
    timestamp: u64,
}

#[derive(Clone, Debug)]
struct GenericBeaconBlockRow {
    hash: BlockHash,
    parent_hash: BlockHash,
    timestamp: u64,
    transaction_count: u32,
}

fn parse_generic_execution_blocks(
    batch: &RecordBatch,
    range: BlockRange,
    output: &mut BTreeMap<u64, GenericExecutionBlockRow>,
) -> Result<u64, XatuError> {
    let mut selected = 0_u64;
    for row in 0..batch.num_rows() {
        let number = required_u64(batch, "block_number", row)?;
        if !range.contains(BlockNumber(number)) {
            continue;
        }
        let timestamp_ms = required_i64(batch, "block_date_time", row)?;
        let timestamp = u64::try_from(timestamp_ms)
            .map_err(|_| XatuError::Data("negative block timestamp".to_owned()))?
            / 1_000;
        insert_unique(
            output,
            number,
            GenericExecutionBlockRow {
                hash: required_hash(batch, "block_hash", row)?,
                timestamp,
            },
            "generic execution block",
        )?;
        selected = selected.saturating_add(1);
    }
    Ok(selected)
}

fn parse_generic_beacon_blocks(
    batch: &RecordBatch,
    range: BlockRange,
    output: &mut BTreeMap<u64, GenericBeaconBlockRow>,
) -> Result<u64, XatuError> {
    let mut selected = 0_u64;
    for row in 0..batch.num_rows() {
        let Some(number) = optional_u64(batch, "execution_payload_block_number", row)? else {
            continue;
        };
        if !range.contains(BlockNumber(number)) {
            continue;
        }
        insert_unique(
            output,
            number,
            GenericBeaconBlockRow {
                hash: required_hash(batch, "execution_payload_block_hash", row)?,
                parent_hash: required_hash(batch, "execution_payload_parent_hash", row)?,
                timestamp: required_u64(batch, "slot_start_date_time", row)?,
                transaction_count: required_u32(
                    batch,
                    "execution_payload_transactions_count",
                    row,
                )?,
            },
            "generic beacon block",
        )?;
        selected = selected.saturating_add(1);
    }
    Ok(selected)
}

fn parse_generic_transactions(
    batch: &RecordBatch,
    range: BlockRange,
    filters: &FilterSet,
    output: &mut BTreeMap<u64, Vec<TransactionEnvelope>>,
) -> Result<u64, XatuError> {
    let mut selected = 0_u64;
    for row in 0..batch.num_rows() {
        let block_number = required_u64(batch, "block_number", row)?;
        if !range.contains(BlockNumber(block_number)) {
            continue;
        }
        let transaction_type = required_u8(batch, "transaction_type", row)?;
        if !filters.scope.transaction_types.is_empty()
            && !filters.scope.transaction_types.contains(&transaction_type)
        {
            continue;
        }
        let hash = required_transaction_hash(batch, "transaction_hash", row)?;
        if !filters.scope.transaction_hashes.is_empty()
            && !filters.scope.transaction_hashes.contains(&hash)
        {
            continue;
        }
        let from = required_address(batch, "from_address", row)?;
        let to = optional_address(batch, "to_address", row)?;
        let senders = if filters.senders.is_empty() {
            &filters.scope.senders
        } else {
            &filters.senders
        };
        let recipients = if filters.recipients.is_empty() {
            &filters.scope.recipients
        } else {
            &filters.recipients
        };
        if !senders.is_empty() && !senders.contains(&from) {
            continue;
        }
        if !recipients.is_empty() && to.is_none_or(|address| !recipients.contains(&address)) {
            continue;
        }
        output
            .entry(block_number)
            .or_default()
            .push(TransactionEnvelope {
                hash,
                transaction_type,
                index: required_u32(batch, "transaction_index", row)?,
                encoded: None,
                from: Some(from),
                to,
                nonce: Some(required_u64(batch, "nonce", row)?),
                gas_limit: Some(required_u64(batch, "gas_limit", row)?),
                value: Some(required_quantity_compatible(batch, "value", row)?),
                input: optional_variable_bytes(batch, "input", row)?,
                max_fee_per_gas: None,
                max_priority_fee_per_gas: None,
                max_fee_per_blob_gas: None,
                blob_versioned_hashes: Vec::new(),
                size_bytes: None,
            });
        selected = selected.saturating_add(1);
    }
    Ok(selected)
}

fn parse_generic_logs(
    batch: &RecordBatch,
    range: BlockRange,
    filters: &FilterSet,
    log_fields: LogFieldSet,
    output: &mut BTreeMap<u64, Vec<Log>>,
) -> Result<u64, XatuError> {
    let mut selected = 0_u64;
    for row in 0..batch.num_rows() {
        let block_number = required_u64(batch, "block_number", row)?;
        if !range.contains(BlockNumber(block_number)) {
            continue;
        }
        let address = required_address(batch, "address", row)?;
        if !filters.scope.addresses.is_empty() && !filters.scope.addresses.contains(&address) {
            continue;
        }
        let transaction_hash = if log_fields.contains(LogField::TransactionHash)
            || !filters.scope.transaction_hashes.is_empty()
        {
            Some(required_transaction_hash(batch, "transaction_hash", row)?)
        } else {
            None
        };
        if !filters.scope.transaction_hashes.is_empty()
            && transaction_hash.is_none_or(|hash| !filters.scope.transaction_hashes.contains(&hash))
        {
            continue;
        }
        let mut topics = vec![fixed_bytes::<32>(batch, "topic0", row)?];
        for name in ["topic1", "topic2", "topic3"] {
            match optional_fixed_bytes::<32>(batch, name, row)? {
                Some(topic) => topics.push(topic),
                None => break,
            }
        }
        if filters.scope.topics.iter().any(|filter| {
            topics
                .get(usize::from(filter.position))
                .is_none_or(|topic| !filter.alternatives.contains(topic))
        }) {
            continue;
        }
        output.entry(block_number).or_default().push(Log {
            address,
            topics,
            data: optional_variable_bytes(batch, "data", row)?.unwrap_or_default(),
            transaction_hash,
            transaction_index: required_u32(batch, "transaction_index", row)?,
            log_index: required_u32(batch, "log_index", row)?,
        });
        selected = selected.saturating_add(1);
    }
    Ok(selected)
}

struct GenericNormalizationInputs<'a> {
    range: BlockRange,
    kind: GenericProjectionKind,
    include_header: bool,
    filters: &'a FilterSet,
    execution_blocks: &'a BTreeMap<u64, GenericExecutionBlockRow>,
    beacon_blocks: &'a BTreeMap<u64, GenericBeaconBlockRow>,
    transactions: BTreeMap<u64, Vec<TransactionEnvelope>>,
    logs: BTreeMap<u64, Vec<Log>>,
    provenance: &'a [Provenance],
}

#[allow(clippy::too_many_lines)]
fn normalize_generic_frames(
    mut inputs: GenericNormalizationInputs<'_>,
) -> Result<Vec<BlockFrame>, XatuError> {
    let expected = usize::try_from(inputs.range.len())
        .map_err(|_| XatuError::Data("range is too large".to_owned()))?;
    if inputs.execution_blocks.len() != expected {
        return Err(XatuError::IncompleteRange {
            table: XatuTable::CanonicalExecutionBlock,
            range: inputs.range,
            expected,
            actual: inputs.execution_blocks.len(),
        });
    }
    if inputs.beacon_blocks.len() != expected {
        return Err(XatuError::IncompleteRange {
            table: XatuTable::CanonicalBeaconBlock,
            range: inputs.range,
            expected,
            actual: inputs.beacon_blocks.len(),
        });
    }
    let mut frames = Vec::with_capacity(expected);
    for number in inputs.range.iter() {
        let execution = inputs
            .execution_blocks
            .get(&number.0)
            .ok_or_else(|| XatuError::Data(format!("missing execution block {number}")))?;
        let beacon = inputs
            .beacon_blocks
            .get(&number.0)
            .ok_or_else(|| XatuError::Data(format!("missing beacon block {number}")))?;
        if execution.hash != beacon.hash || execution.timestamp != beacon.timestamp {
            return Err(XatuError::Data(format!(
                "execution/beacon block disagreement at {number}"
            )));
        }
        let mut scope = inputs.filters.scope.clone();
        scope.block_range = Some(BlockRange::single(number));
        if scope.senders.is_empty() {
            scope.senders.clone_from(&inputs.filters.senders);
        }
        if scope.recipients.is_empty() {
            scope.recipients.clone_from(&inputs.filters.recipients);
        }
        let header = if inputs.include_header {
            Material::Filtered {
                value: HeaderEnvelope {
                    rlp: None,
                    transactions_root: None,
                    receipts_root: None,
                    withdrawals_root: None,
                    gas_limit: None,
                    gas_used: None,
                    base_fee_per_gas: None,
                    blob_gas_used: None,
                    excess_blob_gas: None,
                    size_bytes: None,
                    transaction_count: Some(beacon.transaction_count),
                    consensus_size_bytes: None,
                },
                scope: FilterScope {
                    block_range: Some(BlockRange::single(number)),
                    ..FilterScope::default()
                },
                completeness: Completeness::DatasetDeclared,
            }
        } else {
            Material::Missing(MissingReason::NotRequested)
        };
        let (transactions, logs) = match inputs.kind {
            GenericProjectionKind::Headers => (
                Material::Missing(MissingReason::NotRequested),
                Material::Missing(MissingReason::NotRequested),
            ),
            GenericProjectionKind::Transactions => {
                let mut values = inputs.transactions.remove(&number.0).unwrap_or_default();
                values.sort_by_key(|transaction| transaction.index);
                if values.windows(2).any(|pair| pair[0].index == pair[1].index) {
                    return Err(XatuError::Data(format!(
                        "duplicate transaction index in block {number}"
                    )));
                }
                (
                    Material::Filtered {
                        value: values,
                        scope,
                        completeness: Completeness::DatasetDeclared,
                    },
                    Material::Missing(MissingReason::NotRequested),
                )
            }
            GenericProjectionKind::Logs => {
                let mut values = inputs.logs.remove(&number.0).unwrap_or_default();
                values.sort_by_key(|log| log.log_index);
                if values
                    .windows(2)
                    .any(|pair| pair[0].log_index == pair[1].log_index)
                {
                    return Err(XatuError::Data(format!(
                        "duplicate log index in block {number}"
                    )));
                }
                (
                    Material::Missing(MissingReason::NotRequested),
                    Material::Filtered {
                        value: values,
                        scope,
                        completeness: Completeness::DatasetDeclared,
                    },
                )
            }
        };
        frames.push(BlockFrame {
            chain_id: ChainId(1),
            block: BlockRef {
                number,
                hash: beacon.hash,
                parent_hash: beacon.parent_hash,
                timestamp: beacon.timestamp,
            },
            finality: Finality::Finalized,
            header,
            transactions,
            receipts: Material::Missing(MissingReason::NotRequested),
            logs,
            withdrawals: Material::Missing(MissingReason::NotRequested),
            blob_sidecars: Material::Missing(MissingReason::NotRequested),
            traces: Material::Missing(MissingReason::Unsupported),
            state_diffs: Material::Missing(MissingReason::Unsupported),
            provenance: inputs.provenance.to_vec(),
            verification: VerificationReport::default(),
        });
    }
    Ok(frames)
}

fn validate_frame_budget(frames: &[BlockFrame], budget: SourceBudget) -> Result<(), XatuError> {
    let frame_count = u64::try_from(frames.len()).unwrap_or(u64::MAX);
    if frame_count > budget.max_frames {
        return Err(XatuError::Budget {
            resource: "frames",
            actual: frame_count,
            limit: budget.max_frames,
        });
    }
    for frame in frames {
        let bytes = frame.estimated_heap_bytes();
        if bytes > budget.max_frame_bytes {
            return Err(XatuError::Budget {
                resource: "frame_bytes",
                actual: bytes,
                limit: budget.max_frame_bytes,
            });
        }
    }
    Ok(())
}

fn parse_execution_blocks(
    batch: &RecordBatch,
    range: BlockRange,
    output: &mut BTreeMap<u64, ExecutionBlockRow>,
) -> Result<u64, XatuError> {
    let mut selected = 0_u64;
    for row in 0..batch.num_rows() {
        let number = required_u64(batch, "block_number", row)?;
        if !range.contains(BlockNumber(number)) {
            continue;
        }
        let timestamp_ms = required_i64(batch, "block_date_time", row)?;
        let timestamp = u64::try_from(timestamp_ms)
            .map_err(|_| XatuError::Data("negative block timestamp".to_owned()))?
            / 1_000;
        let value = ExecutionBlockRow {
            number,
            hash: required_hash(batch, "block_hash", row)?,
            timestamp,
            gas_used: required_u64(batch, "gas_used", row)?,
            extra_data: decode_variable_bytes(
                required_bytes(batch, "extra_data", row)?,
                &format!("extra_data at row {row}"),
            )?,
            base_fee_per_gas: required_u64(batch, "base_fee_per_gas", row)?,
        };
        insert_unique(output, number, value, "execution block")?;
        selected = selected.saturating_add(1);
    }
    Ok(selected)
}

fn parse_beacon_blocks(
    batch: &RecordBatch,
    range: BlockRange,
    output: &mut BTreeMap<u64, BeaconBlockRow>,
) -> Result<u64, XatuError> {
    let mut selected = 0_u64;
    for row in 0..batch.num_rows() {
        let Some(number) = optional_u64(batch, "execution_payload_block_number", row)? else {
            continue;
        };
        if !range.contains(BlockNumber(number)) {
            continue;
        }
        let value = BeaconBlockRow {
            slot: required_u64(batch, "slot", row)?,
            number,
            hash: required_hash(batch, "execution_payload_block_hash", row)?,
            parent_hash: required_hash(batch, "execution_payload_parent_hash", row)?,
            timestamp: required_u64(batch, "slot_start_date_time", row)?,
            consensus_size_bytes: required_u64(batch, "block_total_bytes", row)?,
            fork: parse_execution_block_fork(required_bytes(batch, "block_version", row)?)?,
            base_fee_per_gas: required_quantity_le(
                batch,
                "execution_payload_base_fee_per_gas",
                row,
            )?,
            blob_gas_used: required_u64(batch, "execution_payload_blob_gas_used", row)?,
            excess_blob_gas: required_u64(batch, "execution_payload_excess_blob_gas", row)?,
            gas_limit: required_u64(batch, "execution_payload_gas_limit", row)?,
            gas_used: required_u64(batch, "execution_payload_gas_used", row)?,
            transaction_count: required_u32(batch, "execution_payload_transactions_count", row)?,
            transactions_total_bytes: required_u64(
                batch,
                "execution_payload_transactions_total_bytes",
                row,
            )?,
        };
        insert_unique(output, number, value, "beacon execution block")?;
        selected = selected.saturating_add(1);
    }
    Ok(selected)
}

fn parse_beacon_transactions(
    batch: &RecordBatch,
    slots: &BTreeSet<u64>,
    output: &mut Vec<BeaconTransactionRow>,
    sizes: &mut BTreeMap<u64, Vec<TransactionSizeRow>>,
) -> Result<u64, XatuError> {
    let mut selected = 0_u64;
    for row in 0..batch.num_rows() {
        let slot = required_u64(batch, "slot", row)?;
        if !slots.contains(&slot) {
            continue;
        }
        let transaction_type = required_u8(batch, "type", row)?;
        let size_bytes = required_u32(batch, "size", row)?;
        sizes.entry(slot).or_default().push(TransactionSizeRow {
            transaction_type,
            size_bytes,
        });
        selected = selected.saturating_add(1);
        if transaction_type != 3 {
            continue;
        }
        let blob_hashes = required_hash_list(batch, "blob_hashes", row)?;
        if blob_hashes.is_empty() {
            return Err(XatuError::Data(format!(
                "type-3 transaction at slot {slot} has no blob hashes"
            )));
        }
        output.push(BeaconTransactionRow {
            slot,
            index: required_u32(batch, "position", row)?,
            hash: required_transaction_hash(batch, "hash", row)?,
            from: required_address(batch, "from", row)?,
            to: optional_address(batch, "to", row)?,
            gas_limit: required_u64(batch, "gas", row)?,
            size_bytes,
            blob_gas: required_u64(batch, "blob_gas", row)?,
            max_fee_per_blob_gas: required_quantity_le(batch, "blob_gas_fee_cap", row)?,
            blob_hashes,
        });
    }
    Ok(selected)
}

fn parse_withdrawals(
    batch: &RecordBatch,
    slots: &BTreeSet<u64>,
    output: &mut BTreeMap<u64, Vec<WithdrawalRow>>,
) -> Result<u64, XatuError> {
    let mut selected = 0_u64;
    for row in 0..batch.num_rows() {
        let slot = required_u64(batch, "slot", row)?;
        if !slots.contains(&slot) {
            continue;
        }
        output.entry(slot).or_default().push(WithdrawalRow {
            index: required_u64(batch, "withdrawal_index", row)?,
            validator_index: required_u64(batch, "withdrawal_validator_index", row)?,
            address: required_address(batch, "withdrawal_address", row)?,
            amount_gwei: required_quantity_u64_le(batch, "withdrawal_amount", row)?,
        });
        selected = selected.saturating_add(1);
    }
    Ok(selected)
}

fn parse_execution_transactions(
    batch: &RecordBatch,
    range: BlockRange,
    output: &mut BTreeMap<TransactionHash, ExecutionTransactionRow>,
) -> Result<u64, XatuError> {
    let mut selected = 0_u64;
    for row in 0..batch.num_rows() {
        let block_number = required_u64(batch, "block_number", row)?;
        if !range.contains(BlockNumber(block_number))
            || required_u64(batch, "transaction_type", row)? != 3
        {
            continue;
        }
        let hash = required_transaction_hash(batch, "transaction_hash", row)?;
        let value = ExecutionTransactionRow {
            block_number,
            index: required_u32(batch, "transaction_index", row)?,
            hash,
            gas_used: required_u64(batch, "gas_used", row)?,
            gas_price: required_quantity_compatible(batch, "gas_price", row)?,
            success: required_bool(batch, "success", row)?,
        };
        insert_unique(output, hash, value, "execution transaction")?;
        selected = selected.saturating_add(1);
    }
    Ok(selected)
}

struct NormalizationInputs<'a> {
    range: BlockRange,
    execution_blocks: &'a BTreeMap<u64, ExecutionBlockRow>,
    beacon_blocks: &'a BTreeMap<u64, BeaconBlockRow>,
    beacon_transactions: Vec<BeaconTransactionRow>,
    transaction_sizes: &'a BTreeMap<u64, Vec<TransactionSizeRow>>,
    withdrawals: &'a BTreeMap<u64, Vec<WithdrawalRow>>,
    execution_transactions: &'a BTreeMap<TransactionHash, ExecutionTransactionRow>,
    provenance: &'a [Provenance],
}

#[allow(clippy::too_many_lines)]
fn normalize_frames(inputs: NormalizationInputs<'_>) -> Result<Vec<BlockFrame>, XatuError> {
    let NormalizationInputs {
        range,
        execution_blocks,
        beacon_blocks,
        mut beacon_transactions,
        transaction_sizes,
        withdrawals,
        execution_transactions,
        provenance,
    } = inputs;
    let expected = usize::try_from(range.len())
        .map_err(|_| XatuError::Data("range is too large".to_owned()))?;
    if execution_blocks.len() != expected {
        return Err(XatuError::IncompleteRange {
            table: XatuTable::CanonicalExecutionBlock,
            range,
            expected,
            actual: execution_blocks.len(),
        });
    }
    if beacon_blocks.len() != expected {
        return Err(XatuError::IncompleteRange {
            table: XatuTable::CanonicalBeaconBlock,
            range,
            expected,
            actual: beacon_blocks.len(),
        });
    }
    beacon_transactions.sort_by_key(|transaction| (transaction.slot, transaction.index));
    let mut transactions_by_slot = BTreeMap::<u64, Vec<BeaconTransactionRow>>::new();
    for transaction in beacon_transactions {
        transactions_by_slot
            .entry(transaction.slot)
            .or_default()
            .push(transaction);
    }

    let scope = FilterScope {
        block_range: Some(range),
        transaction_types: vec![3],
        ..FilterScope::default()
    };
    let mut frames = Vec::with_capacity(expected);
    for number in range.iter() {
        let execution = execution_blocks
            .get(&number.0)
            .ok_or_else(|| XatuError::Data(format!("missing execution block {}", number.0)))?;
        let beacon = beacon_blocks
            .get(&number.0)
            .ok_or_else(|| XatuError::Data(format!("missing beacon block {}", number.0)))?;
        validate_block_join(execution, beacon)?;
        let block_transaction_sizes = transaction_sizes
            .get(&beacon.slot)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let block_withdrawals = withdrawals
            .get(&beacon.slot)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let execution_size_bytes = execution_block_rlp_len(
            execution,
            beacon,
            block_transaction_sizes,
            block_withdrawals,
        )?;
        let block_transactions = transactions_by_slot
            .remove(&beacon.slot)
            .unwrap_or_default();
        let mut transactions = Vec::with_capacity(block_transactions.len());
        let mut receipts = Vec::with_capacity(block_transactions.len());
        for transaction in block_transactions {
            let receipt = execution_transactions
                .get(&transaction.hash)
                .ok_or_else(|| {
                    XatuError::Data(format!(
                        "missing execution transaction {} for block {}",
                        transaction.hash, number.0
                    ))
                })?;
            if receipt.block_number != number.0
                || receipt.index != transaction.index
                || receipt.hash != transaction.hash
            {
                return Err(XatuError::Data(format!(
                    "cross-table transaction disagreement for {}",
                    transaction.hash
                )));
            }
            let expected_blob_gas = u64::try_from(transaction.blob_hashes.len())
                .unwrap_or(u64::MAX)
                .saturating_mul(131_072);
            if transaction.blob_gas != expected_blob_gas {
                return Err(XatuError::Data(format!(
                    "blob gas/hash-count disagreement for {}",
                    transaction.hash
                )));
            }
            transactions.push(TransactionEnvelope {
                hash: transaction.hash,
                transaction_type: 3,
                index: transaction.index,
                encoded: None,
                from: Some(transaction.from),
                to: transaction.to,
                nonce: None,
                gas_limit: Some(transaction.gas_limit),
                value: None,
                input: None,
                max_fee_per_gas: None,
                max_priority_fee_per_gas: None,
                max_fee_per_blob_gas: Some(transaction.max_fee_per_blob_gas),
                blob_versioned_hashes: transaction.blob_hashes,
                size_bytes: Some(transaction.size_bytes),
            });
            receipts.push(ReceiptEnvelope {
                transaction_hash: receipt.hash,
                transaction_type: 3,
                transaction_index: receipt.index,
                encoded: None,
                success: Some(receipt.success),
                gas_used: Some(receipt.gas_used),
                effective_gas_price: Some(receipt.gas_price),
                blob_gas_used: Some(expected_blob_gas),
                blob_gas_price: None,
                logs: Vec::new(),
            });
        }
        let mut receipt_scope = scope.clone();
        receipt_scope.transaction_hashes = transactions
            .iter()
            .map(|transaction| transaction.hash)
            .collect();
        frames.push(BlockFrame {
            chain_id: ChainId(1),
            block: BlockRef {
                number,
                hash: beacon.hash,
                parent_hash: beacon.parent_hash,
                timestamp: beacon.timestamp,
            },
            finality: Finality::Finalized,
            header: Material::Filtered {
                value: HeaderEnvelope {
                    rlp: None,
                    transactions_root: None,
                    receipts_root: None,
                    withdrawals_root: None,
                    gas_limit: Some(beacon.gas_limit),
                    gas_used: Some(beacon.gas_used),
                    base_fee_per_gas: Some(beacon.base_fee_per_gas),
                    blob_gas_used: Some(beacon.blob_gas_used),
                    excess_blob_gas: Some(beacon.excess_blob_gas),
                    size_bytes: Some(execution_size_bytes),
                    consensus_size_bytes: Some(beacon.consensus_size_bytes),
                    transaction_count: Some(beacon.transaction_count),
                },
                scope: FilterScope {
                    block_range: Some(BlockRange::single(number)),
                    ..FilterScope::default()
                },
                completeness: Completeness::DatasetDeclared,
            },
            transactions: Material::Filtered {
                value: transactions,
                scope: scope.clone(),
                completeness: Completeness::DatasetDeclared,
            },
            receipts: Material::Filtered {
                value: receipts,
                scope: receipt_scope,
                completeness: Completeness::DatasetDeclared,
            },
            logs: Material::Missing(MissingReason::NotRequested),
            withdrawals: Material::Missing(MissingReason::NotRequested),
            blob_sidecars: Material::Missing(MissingReason::NotRequested),
            traces: Material::Missing(MissingReason::Unsupported),
            state_diffs: Material::Missing(MissingReason::Unsupported),
            provenance: provenance.to_vec(),
            verification: VerificationReport::default(),
        });
    }
    if !transactions_by_slot.is_empty() {
        return Err(XatuError::Data(
            "blob transactions remained after block join".to_owned(),
        ));
    }
    Ok(frames)
}

fn validate_block_join(
    execution: &ExecutionBlockRow,
    beacon: &BeaconBlockRow,
) -> Result<(), XatuError> {
    if execution.number != beacon.number
        || execution.hash != beacon.hash
        || execution.timestamp != beacon.timestamp
        || execution.gas_used != beacon.gas_used
        || quantity_to_u64(beacon.base_fee_per_gas) != Some(execution.base_fee_per_gas)
    {
        return Err(XatuError::Data(format!(
            "execution/beacon block disagreement at {}",
            execution.number
        )));
    }
    Ok(())
}

fn parse_execution_block_fork(value: &[u8]) -> Result<ExecutionBlockFork, XatuError> {
    match value {
        b"deneb" => Ok(ExecutionBlockFork::Cancun),
        b"electra" | b"fulu" => Ok(ExecutionBlockFork::Prague),
        _ => Err(XatuError::Data(format!(
            "unsupported Xatu beacon block version {:?}; execution-size rules must be updated explicitly",
            String::from_utf8_lossy(value)
        ))),
    }
}

fn execution_block_rlp_len(
    execution: &ExecutionBlockRow,
    beacon: &BeaconBlockRow,
    transactions: &[TransactionSizeRow],
    withdrawals: &[WithdrawalRow],
) -> Result<u64, XatuError> {
    if transactions.len() != usize::try_from(beacon.transaction_count).unwrap_or(usize::MAX) {
        return Err(XatuError::Data(format!(
            "execution transaction-size coverage mismatch at {}: rows={}, expected={}",
            execution.number,
            transactions.len(),
            beacon.transaction_count
        )));
    }
    let raw_transaction_bytes = transactions
        .iter()
        .map(|transaction| u64::from(transaction.size_bytes))
        .sum::<u64>();
    if raw_transaction_bytes != beacon.transactions_total_bytes {
        return Err(XatuError::Data(format!(
            "execution transaction byte disagreement at {}: rows={raw_transaction_bytes}, beacon={}",
            execution.number, beacon.transactions_total_bytes
        )));
    }

    // Cancun execution header fields, in canonical RLP order. Hashes and roots
    // have fixed encoded lengths, so their values are unnecessary for a size
    // calculation. Prague adds the fixed-size requests hash.
    let mut header_payload = 0_u64;
    for fixed_bytes in [32_u64, 32, 20, 32, 32, 32, 256] {
        header_payload = header_payload.saturating_add(rlp_bytes_len(fixed_bytes));
    }
    header_payload = header_payload
        .saturating_add(rlp_integer_len(0)) // difficulty
        .saturating_add(rlp_integer_len(execution.number))
        .saturating_add(rlp_integer_len(beacon.gas_limit))
        .saturating_add(rlp_integer_len(beacon.gas_used))
        .saturating_add(rlp_integer_len(beacon.timestamp))
        .saturating_add(rlp_byte_slice_len(&execution.extra_data))
        .saturating_add(rlp_bytes_len(32)) // prev_randao / mix_hash
        .saturating_add(rlp_bytes_len(8)) // nonce
        .saturating_add(rlp_quantity_len(beacon.base_fee_per_gas))
        .saturating_add(rlp_bytes_len(32)) // withdrawals root
        .saturating_add(rlp_integer_len(beacon.blob_gas_used))
        .saturating_add(rlp_integer_len(beacon.excess_blob_gas))
        .saturating_add(rlp_bytes_len(32)); // parent beacon block root
    if beacon.fork == ExecutionBlockFork::Prague {
        header_payload = header_payload.saturating_add(rlp_bytes_len(32)); // requests hash
    }
    let header = rlp_list_len(header_payload);

    let transaction_payload = transactions
        .iter()
        .map(|transaction| {
            let size = u64::from(transaction.size_bytes);
            if transaction.transaction_type == 0 {
                // A legacy transaction is already an RLP list.
                size
            } else {
                // An EIP-2718 transaction is a byte string inside the body list.
                rlp_bytes_len(size)
            }
        })
        .sum::<u64>();
    let transaction_list = rlp_list_len(transaction_payload);

    let withdrawal_payload = withdrawals
        .iter()
        .map(|withdrawal| {
            let payload = rlp_integer_len(withdrawal.index)
                .saturating_add(rlp_integer_len(withdrawal.validator_index))
                .saturating_add(rlp_byte_slice_len(withdrawal.address.as_array()))
                .saturating_add(rlp_integer_len(withdrawal.amount_gwei));
            rlp_list_len(payload)
        })
        .sum::<u64>();
    let withdrawal_list = rlp_list_len(withdrawal_payload);

    // Post-Shanghai block body: [header, transactions, ommers, withdrawals].
    Ok(rlp_list_len(
        header
            .saturating_add(transaction_list)
            .saturating_add(1) // empty ommers list
            .saturating_add(withdrawal_list),
    ))
}

fn rlp_integer_len(value: u64) -> u64 {
    if value < 0x80 {
        1
    } else {
        1 + u64::from((u64::BITS - value.leading_zeros()).div_ceil(8))
    }
}

fn rlp_quantity_len(value: Quantity) -> u64 {
    let first = value.0.iter().position(|byte| *byte != 0);
    first.map_or(1, |index| rlp_byte_slice_len(&value.0[index..]))
}

fn rlp_byte_slice_len(value: &[u8]) -> u64 {
    if value.len() == 1 && value[0] < 0x80 {
        1
    } else {
        rlp_bytes_len(u64::try_from(value.len()).unwrap_or(u64::MAX))
    }
}

fn rlp_bytes_len(payload: u64) -> u64 {
    payload.saturating_add(rlp_header_len(payload))
}

fn rlp_list_len(payload: u64) -> u64 {
    payload.saturating_add(rlp_header_len(payload))
}

fn rlp_header_len(payload: u64) -> u64 {
    if payload <= 55 {
        1
    } else {
        1 + u64::from((u64::BITS - payload.leading_zeros()).div_ceil(8))
    }
}

fn insert_unique<K: Ord, V>(
    output: &mut BTreeMap<K, V>,
    key: K,
    value: V,
    label: &str,
) -> Result<(), XatuError> {
    if output.insert(key, value).is_some() {
        Err(XatuError::Data(format!("duplicate {label} row")))
    } else {
        Ok(())
    }
}

fn required_u64(batch: &RecordBatch, name: &str, row: usize) -> Result<u64, XatuError> {
    optional_u64(batch, name, row)?
        .ok_or_else(|| XatuError::Data(format!("{name} is null at row {row}")))
}

fn required_u32(batch: &RecordBatch, name: &str, row: usize) -> Result<u32, XatuError> {
    let value = required_u64(batch, name, row)?;
    u32::try_from(value).map_err(|_| XatuError::Data(format!("{name} overflows u32 at row {row}")))
}

fn required_u8(batch: &RecordBatch, name: &str, row: usize) -> Result<u8, XatuError> {
    let value = required_u64(batch, name, row)?;
    u8::try_from(value).map_err(|_| XatuError::Data(format!("{name} overflows u8 at row {row}")))
}

fn optional_u64(batch: &RecordBatch, name: &str, row: usize) -> Result<Option<u64>, XatuError> {
    let array = column(batch, name)?;
    if array.is_null(row) {
        return Ok(None);
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt64Array>() {
        return Ok(Some(array.value(row)));
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt32Array>() {
        return Ok(Some(u64::from(array.value(row))));
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt16Array>() {
        return Ok(Some(u64::from(array.value(row))));
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt8Array>() {
        return Ok(Some(u64::from(array.value(row))));
    }
    Err(type_error(name, array))
}

fn required_i64(batch: &RecordBatch, name: &str, row: usize) -> Result<i64, XatuError> {
    let array = column(batch, name)?;
    if array.is_null(row) {
        return Err(XatuError::Data(format!("{name} is null at row {row}")));
    }
    if let Some(array) = array.as_any().downcast_ref::<TimestampMillisecondArray>() {
        return Ok(array.value(row));
    }
    Err(type_error(name, array))
}

fn required_bool(batch: &RecordBatch, name: &str, row: usize) -> Result<bool, XatuError> {
    let array = column(batch, name)?;
    if array.is_null(row) {
        return Err(XatuError::Data(format!("{name} is null at row {row}")));
    }
    array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .map(|array| array.value(row))
        .ok_or_else(|| type_error(name, array))
}

fn required_hash(batch: &RecordBatch, name: &str, row: usize) -> Result<BlockHash, XatuError> {
    fixed_bytes::<32>(batch, name, row).map(BlockHash::new)
}

fn required_transaction_hash(
    batch: &RecordBatch,
    name: &str,
    row: usize,
) -> Result<TransactionHash, XatuError> {
    fixed_bytes::<32>(batch, name, row).map(TransactionHash::new)
}

fn required_address(batch: &RecordBatch, name: &str, row: usize) -> Result<Address, XatuError> {
    fixed_bytes::<20>(batch, name, row).map(Address::new)
}

fn optional_address(
    batch: &RecordBatch,
    name: &str,
    row: usize,
) -> Result<Option<Address>, XatuError> {
    let array = column(batch, name)?;
    if array.is_null(row) {
        Ok(None)
    } else {
        fixed_bytes::<20>(batch, name, row)
            .map(Address::new)
            .map(Some)
    }
}

fn required_quantity_le(
    batch: &RecordBatch,
    name: &str,
    row: usize,
) -> Result<Quantity, XatuError> {
    let value = binary_at(column(batch, name)?, row)?;
    if value.is_empty() || value.len() > 32 {
        return Err(XatuError::Data(format!(
            "{name} has {} raw bytes at row {row}; expected 1-32",
            value.len()
        )));
    }
    let mut bytes = [0; 32];
    for (target, source) in bytes[32 - value.len()..].iter_mut().zip(value.iter().rev()) {
        *target = *source;
    }
    Ok(Quantity::new(bytes))
}

/// Decode `ClickHouse` unsigned integer columns across the public Xatu Parquet
/// representations observed in the wild.
///
/// Older canonical execution exports exposed `gas_price` as an Arrow unsigned
/// integer. Current exports encode the underlying `ClickHouse` `UInt128` as a
/// little-endian `FixedSizeBinary(16)`. Both are exact integer
/// representations, so normalize either form into the source-neutral
/// 256-bit quantity without narrowing.
fn required_quantity_compatible(
    batch: &RecordBatch,
    name: &str,
    row: usize,
) -> Result<Quantity, XatuError> {
    let array = column(batch, name)?;
    if array.is_null(row) {
        return Err(XatuError::Data(format!("{name} is null at row {row}")));
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt64Array>() {
        return Ok(quantity_from_u64(array.value(row)));
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt32Array>() {
        return Ok(quantity_from_u64(u64::from(array.value(row))));
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt16Array>() {
        return Ok(quantity_from_u64(u64::from(array.value(row))));
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt8Array>() {
        return Ok(quantity_from_u64(u64::from(array.value(row))));
    }
    required_quantity_le(batch, name, row)
}

fn required_quantity_u64_le(batch: &RecordBatch, name: &str, row: usize) -> Result<u64, XatuError> {
    quantity_to_u64(required_quantity_le(batch, name, row)?)
        .ok_or_else(|| XatuError::Data(format!("{name} overflows u64 at row {row}")))
}

fn required_hash_list(
    batch: &RecordBatch,
    name: &str,
    row: usize,
) -> Result<Vec<BlockHash>, XatuError> {
    let array = column(batch, name)?;
    if array.is_null(row) {
        return Ok(Vec::new());
    }
    let values = if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
        list.value(row)
    } else if let Some(list) = array.as_any().downcast_ref::<LargeListArray>() {
        list.value(row)
    } else {
        return Err(type_error(name, array));
    };
    (0..values.len())
        .map(|index| {
            binary_at(values.as_ref(), index).and_then(|value| {
                decode_fixed_bytes::<32>(value, &format!("{name} element {index} at row {row}"))
                    .map(BlockHash::new)
            })
        })
        .collect()
}

fn fixed_bytes<const N: usize>(
    batch: &RecordBatch,
    name: &str,
    row: usize,
) -> Result<[u8; N], XatuError> {
    let value = binary_at(column(batch, name)?, row)?;
    decode_fixed_bytes(value, &format!("{name} at row {row}"))
}

fn optional_fixed_bytes<const N: usize>(
    batch: &RecordBatch,
    name: &str,
    row: usize,
) -> Result<Option<[u8; N]>, XatuError> {
    let array = column(batch, name)?;
    if array.is_null(row) {
        return Ok(None);
    }
    binary_at(array, row)
        .and_then(|value| decode_fixed_bytes(value, &format!("{name} at row {row}")))
        .map(Some)
}

fn required_bytes<'a>(
    batch: &'a RecordBatch,
    name: &str,
    row: usize,
) -> Result<&'a [u8], XatuError> {
    binary_at(column(batch, name)?, row)
}

fn optional_variable_bytes(
    batch: &RecordBatch,
    name: &str,
    row: usize,
) -> Result<Option<Vec<u8>>, XatuError> {
    let array = column(batch, name)?;
    if array.is_null(row) {
        return Ok(None);
    }
    decode_variable_bytes(binary_at(array, row)?, &format!("{name} at row {row}")).map(Some)
}

fn decode_fixed_bytes<const N: usize>(value: &[u8], context: &str) -> Result<[u8; N], XatuError> {
    if let Ok(bytes) = value.try_into() {
        return Ok(bytes);
    }
    if value.len() == N * 2 + 2 && value.starts_with(b"0x") {
        let mut bytes = [0; N];
        hex::decode_to_slice(&value[2..], &mut bytes)
            .map_err(|error| XatuError::Data(format!("{context} has invalid hex: {error}")))?;
        return Ok(bytes);
    }
    Err(XatuError::Data(format!(
        "{context} has {} bytes; expected {N} raw bytes or 0x-prefixed hex",
        value.len()
    )))
}

fn decode_variable_bytes(value: &[u8], context: &str) -> Result<Vec<u8>, XatuError> {
    if let Some(hexadecimal) = value.strip_prefix(b"0x") {
        if !hexadecimal.len().is_multiple_of(2) {
            return Err(XatuError::Data(format!(
                "{context} has an odd hexadecimal length"
            )));
        }
        return hex::decode(hexadecimal)
            .map_err(|error| XatuError::Data(format!("{context} has invalid hex: {error}")));
    }
    Ok(value.to_vec())
}

fn binary_at(array: &dyn Array, row: usize) -> Result<&[u8], XatuError> {
    if array.is_null(row) {
        return Err(XatuError::Data(format!(
            "binary value is null at row {row}"
        )));
    }
    if let Some(array) = array.as_any().downcast_ref::<FixedSizeBinaryArray>() {
        return Ok(array.value(row));
    }
    if let Some(array) = array.as_any().downcast_ref::<BinaryArray>() {
        return Ok(array.value(row));
    }
    if let Some(array) = array.as_any().downcast_ref::<LargeBinaryArray>() {
        return Ok(array.value(row));
    }
    if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(array.value(row).as_bytes());
    }
    if let Some(array) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(array.value(row).as_bytes());
    }
    Err(XatuError::Data(format!(
        "expected binary Arrow array, got {:?}",
        array.data_type()
    )))
}

fn column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a dyn Array, XatuError> {
    let index = batch
        .schema()
        .index_of(name)
        .map_err(|error| XatuError::Data(error.to_string()))?;
    Ok(batch.column(index).as_ref())
}

fn type_error(name: &str, array: &dyn Array) -> XatuError {
    XatuError::Data(format!(
        "{name} has unexpected Arrow type {:?}",
        array.data_type()
    ))
}

fn quantity_from_u64(value: u64) -> Quantity {
    let mut bytes = [0; 32];
    bytes[24..].copy_from_slice(&value.to_be_bytes());
    Quantity::new(bytes)
}

fn quantity_to_u64(value: Quantity) -> Option<u64> {
    if value.0[..24].iter().any(|byte| *byte != 0) {
        return None;
    }
    Some(u64::from_be_bytes(
        value.0[24..].try_into().expect("eight-byte suffix"),
    ))
}

fn elapsed_ms(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::ArrayRef;
    use arrow_schema::{DataType, Field, Schema};
    use leani_primitives::TopicFilter;

    #[test]
    fn little_endian_xatu_quantities_become_canonical_big_endian() {
        let value = 1_234_567_u64;
        let mut little = [0; 32];
        little[..8].copy_from_slice(&value.to_le_bytes());
        little.reverse();
        assert_eq!(quantity_to_u64(Quantity::new(little)), Some(value));
    }

    #[test]
    fn quantity_u64_round_trip_is_exact() {
        assert_eq!(quantity_to_u64(quantity_from_u64(u64::MAX)), Some(u64::MAX));
    }

    #[test]
    fn generic_transaction_projection_reads_and_filters_material_fields() {
        let hash = format!("0x{}", "11".repeat(32));
        let from = format!("0x{}", "22".repeat(20));
        let to = format!("0x{}", "33".repeat(20));
        let batch = RecordBatch::try_from_iter(vec![
            (
                "block_number",
                Arc::new(UInt64Array::from(vec![42])) as ArrayRef,
            ),
            (
                "transaction_index",
                Arc::new(UInt64Array::from(vec![3])) as ArrayRef,
            ),
            (
                "transaction_hash",
                Arc::new(StringArray::from(vec![hash.as_str()])) as ArrayRef,
            ),
            ("nonce", Arc::new(UInt64Array::from(vec![7])) as ArrayRef),
            (
                "from_address",
                Arc::new(StringArray::from(vec![from.as_str()])) as ArrayRef,
            ),
            (
                "to_address",
                Arc::new(StringArray::from(vec![Some(to.as_str())])) as ArrayRef,
            ),
            ("value", Arc::new(UInt64Array::from(vec![99])) as ArrayRef),
            (
                "input",
                Arc::new(StringArray::from(vec![Some("0x1234")])) as ArrayRef,
            ),
            (
                "gas_limit",
                Arc::new(UInt64Array::from(vec![21_000])) as ArrayRef,
            ),
            (
                "transaction_type",
                Arc::new(UInt32Array::from(vec![2])) as ArrayRef,
            ),
        ])
        .expect("batch");
        let filters = FilterSet {
            scope: FilterScope {
                senders: vec![Address::new([0x22; 20])],
                recipients: vec![Address::new([0x33; 20])],
                ..FilterScope::default()
            },
            ..FilterSet::default()
        };
        let mut output = BTreeMap::new();
        assert_eq!(
            parse_generic_transactions(
                &batch,
                BlockRange::single(BlockNumber(42)),
                &filters,
                &mut output,
            )
            .expect("parse"),
            1
        );
        let transaction = &output[&42][0];
        assert_eq!(transaction.index, 3);
        assert_eq!(transaction.from, Some(Address::new([0x22; 20])));
        assert_eq!(transaction.to, Some(Address::new([0x33; 20])));
        assert_eq!(transaction.value, Some(quantity_from_u64(99)));
        assert_eq!(transaction.input.as_deref(), Some(&[0x12, 0x34][..]));
    }

    #[test]
    fn generic_log_projection_reads_all_topics_and_applies_predicates() {
        let hash = format!("0x{}", "44".repeat(32));
        let address = format!("0x{}", "55".repeat(20));
        let topic0 = format!("0x{}", "66".repeat(32));
        let topic1 = format!("0x{}", "77".repeat(32));
        let batch = RecordBatch::try_from_iter(vec![
            (
                "block_number",
                Arc::new(UInt32Array::from(vec![42])) as ArrayRef,
            ),
            (
                "transaction_index",
                Arc::new(UInt32Array::from(vec![1])) as ArrayRef,
            ),
            (
                "transaction_hash",
                Arc::new(StringArray::from(vec![hash.as_str()])) as ArrayRef,
            ),
            (
                "log_index",
                Arc::new(UInt32Array::from(vec![2])) as ArrayRef,
            ),
            (
                "address",
                Arc::new(StringArray::from(vec![address.as_str()])) as ArrayRef,
            ),
            (
                "topic0",
                Arc::new(StringArray::from(vec![topic0.as_str()])) as ArrayRef,
            ),
            (
                "topic1",
                Arc::new(StringArray::from(vec![Some(topic1.as_str())])) as ArrayRef,
            ),
            (
                "topic2",
                Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            ),
            (
                "topic3",
                Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            ),
            (
                "data",
                Arc::new(StringArray::from(vec![Some("0xdeadbeef")])) as ArrayRef,
            ),
        ])
        .expect("batch");
        let filters = FilterSet {
            scope: FilterScope {
                addresses: vec![Address::new([0x55; 20])],
                topics: vec![TopicFilter {
                    position: 1,
                    alternatives: vec![[0x77; 32]],
                }],
                ..FilterScope::default()
            },
            ..FilterSet::default()
        };
        let mut output = BTreeMap::new();
        assert_eq!(
            parse_generic_logs(
                &batch,
                BlockRange::single(BlockNumber(42)),
                &filters,
                LogFieldSet::ALL,
                &mut output,
            )
            .expect("parse"),
            1
        );
        let log = &output[&42][0];
        assert_eq!(log.topics, vec![[0x66; 32], [0x77; 32]]);
        assert_eq!(log.data, vec![0xde, 0xad, 0xbe, 0xef]);
    }

    #[test]
    fn slim_log_projection_omits_transaction_hash_unless_the_filter_needs_it() {
        let filters = FilterSet::default();
        assert!(!generic_log_columns(LogFieldSet::NONE, &filters).contains(&"transaction_hash"));
        assert!(generic_log_columns(LogFieldSet::ALL, &filters).contains(&"transaction_hash"));

        let mut hash_filter = FilterSet::default();
        hash_filter.scope.transaction_hashes = vec![TransactionHash::new([0x11; 32])];
        assert!(generic_log_columns(LogFieldSet::NONE, &hash_filter).contains(&"transaction_hash"));
    }

    #[test]
    fn clickhouse_uint128_gas_price_is_decoded_without_narrowing() {
        let value = (u128::from(u64::MAX) << 32) | 0x0102_0304;
        let little_endian = value.to_le_bytes();
        let gas_price = FixedSizeBinaryArray::try_from_iter([little_endian.as_slice()].into_iter())
            .expect("array");
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "gas_price",
                DataType::FixedSizeBinary(16),
                false,
            )])),
            vec![Arc::new(gas_price)],
        )
        .expect("batch");

        let decoded = required_quantity_compatible(&batch, "gas_price", 0).expect("decode UInt128");
        let mut expected = [0_u8; 32];
        expected[16..].copy_from_slice(&value.to_be_bytes());
        assert_eq!(decoded, Quantity::new(expected));
    }

    #[test]
    fn legacy_uint64_gas_price_remains_compatible() {
        let value = 12_345_678_901_u64;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "gas_price",
                DataType::UInt64,
                false,
            )])),
            vec![Arc::new(UInt64Array::from(vec![value]))],
        )
        .expect("batch");

        assert_eq!(
            required_quantity_compatible(&batch, "gas_price", 0).expect("decode UInt64"),
            quantity_from_u64(value)
        );
    }

    fn dencun_transaction_sizes() -> Vec<TransactionSizeRow> {
        [
            (2, 352),
            (2, 193),
            (0, 368),
            (2, 1_190),
            (0, 172),
            (2, 987),
            (0, 171),
            (2, 280),
            (2, 181),
            (0, 205),
            (0, 977),
            (2, 3_097),
            (0, 433),
            (0, 111),
            (2, 441),
            (0, 175),
            (2, 946),
            (2, 960),
            (2, 852),
            (0, 112),
            (0, 175),
            (2, 313),
            (2, 505),
            (2, 505),
            (0, 109),
            (2, 119),
            (2, 566),
            (2, 117),
            (2, 1_718),
            (2, 181),
            (2, 348),
            (2, 376),
            (2, 121),
            (2, 120),
            (2, 117),
            (2, 179),
            (2, 733),
            (2, 180),
            (2, 214),
            (2, 214),
            (2, 182),
            (2, 179),
            (2, 179),
            (2, 179),
            (2, 214),
            (2, 214),
            (2, 179),
            (2, 440),
            (2, 179),
            (2, 214),
            (2, 214),
            (2, 119),
            (2, 180),
            (2, 117),
            (2, 696),
            (2, 118),
            (2, 2_584),
            (2, 180),
            (2, 121),
            (2, 117),
            (2, 117),
            (2, 117),
            (2, 117),
            (2, 117),
            (2, 119),
            (2, 117),
            (2, 117),
            (2, 117),
            (2, 117),
            (2, 122),
            (3, 150),
            (2, 768),
            (2, 179),
            (2, 759),
            (2, 116),
            (2, 1_175),
            (2, 1_173),
            (2, 2_314),
            (2, 115),
        ]
        .into_iter()
        .map(|(transaction_type, size_bytes)| TransactionSizeRow {
            transaction_type,
            size_bytes,
        })
        .collect()
    }

    #[test]
    fn dencun_execution_block_size_matches_verified_raw_block() {
        let transaction_sizes = dencun_transaction_sizes();
        assert_eq!(
            transaction_sizes
                .iter()
                .map(|transaction| u64::from(transaction.size_bytes))
                .sum::<u64>(),
            33_644
        );
        let withdrawals = (0..16)
            .map(|offset| WithdrawalRow {
                index: 38_266_054 + offset,
                validator_index: 1_268_201 + offset,
                address: Address::new([0; 20]),
                amount_gwei: if offset == 15 { 60_026_761 } else { 16_025_579 },
            })
            .collect::<Vec<_>>();
        let execution = ExecutionBlockRow {
            number: 19_426_589,
            hash: BlockHash::ZERO,
            timestamp: 1_710_338_159,
            gas_used: 7_155_950,
            extra_data: vec![0; 11],
            base_fee_per_gas: 55_745_530_424,
        };
        let beacon = BeaconBlockRow {
            slot: 8_626_181,
            number: execution.number,
            hash: execution.hash,
            parent_hash: BlockHash::ZERO,
            timestamp: execution.timestamp,
            consensus_size_bytes: 148_616,
            fork: ExecutionBlockFork::Cancun,
            base_fee_per_gas: quantity_from_u64(execution.base_fee_per_gas),
            blob_gas_used: 131_072,
            excess_blob_gas: 0,
            gas_limit: 30_000_000,
            gas_used: execution.gas_used,
            transaction_count: 79,
            transactions_total_bytes: 33_644,
        };

        assert_eq!(
            execution_block_rlp_len(&execution, &beacon, &transaction_sizes, &withdrawals)
                .expect("exact size"),
            34_975
        );
    }

    #[test]
    fn execution_size_fork_rules_fail_closed() {
        assert_eq!(
            parse_execution_block_fork(b"deneb").expect("Deneb"),
            ExecutionBlockFork::Cancun
        );
        assert_eq!(
            parse_execution_block_fork(b"fulu").expect("Fulu"),
            ExecutionBlockFork::Prague
        );
        assert!(parse_execution_block_fork(b"unknown-future-fork").is_err());
    }
}
