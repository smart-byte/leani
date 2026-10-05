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
use arrow_schema::{DataType, Schema, TimeUnit};
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
    errors::ParquetError,
    file::metadata::ParquetMetaData,
};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::{
    CatalogObject, XatuCatalog, XatuDate, XatuError, XatuTable,
    catalog::pinning_problem,
    reader::{InputBudget, ObjectStoreReader},
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
    "execution_payload_gas_limit",
    "execution_payload_gas_used",
    "execution_payload_base_fee_per_gas",
    "execution_payload_blob_gas_used",
    "execution_payload_excess_blob_gas",
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

/// Most withdrawals one execution payload holds (`MAX_WITHDRAWALS_PER_PAYLOAD`).
const MAX_WITHDRAWALS_PER_PAYLOAD: usize = 16;

/// How a projected Xatu column is encoded, which fixes the Arrow types it
/// may arrive as. Decoders follow the declared encoding and a batch's Arrow
/// type, never the content of a value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ColumnEncoding {
    /// A `ClickHouse` unsigned integer of any width.
    Unsigned,
    /// A `ClickHouse` `DateTime64(3)`.
    TimestampMillis,
    Boolean,
    /// A `UInt128` or `UInt256` as little-endian fixed-width bytes, which
    /// older exports narrowed to an unsigned integer.
    UnsignedOrLittleEndian,
    /// A `UInt128` or `UInt256` as little-endian fixed-width bytes.
    LittleEndian,
    /// Exactly this many bytes, as `0x`-prefixed hexadecimal text such as a
    /// `FixedString`, or as a raw binary column of exactly that width.
    FixedHex(i32),
    /// `0x`-prefixed hexadecimal text of any length.
    Hex,
    /// A list of 32-byte [`Self::FixedHex`] values.
    HashList,
    /// Plain text, such as a fork name.
    Text,
}

impl ColumnEncoding {
    fn accepts(self, data_type: &DataType) -> bool {
        let unsigned = matches!(
            data_type,
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
        );
        let little_endian =
            matches!(data_type, DataType::FixedSizeBinary(width) if (1..=32).contains(width));
        let text = matches!(
            data_type,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary
        );
        match self {
            Self::Unsigned => unsigned,
            Self::TimestampMillis => {
                matches!(data_type, DataType::Timestamp(TimeUnit::Millisecond, _))
            }
            Self::Boolean => matches!(data_type, DataType::Boolean),
            Self::UnsignedOrLittleEndian => unsigned || little_endian,
            Self::LittleEndian => little_endian,
            Self::FixedHex(bytes) => {
                text || matches!(
                    data_type,
                    DataType::FixedSizeBinary(width)
                        if *width == bytes || *width == bytes.saturating_mul(2).saturating_add(2)
                )
            }
            Self::Hex | Self::Text => text,
            Self::HashList => matches!(
                data_type,
                DataType::List(field) | DataType::LargeList(field)
                    if Self::FixedHex(32).accepts(field.data_type())
            ),
        }
    }
}

/// The declared encoding of every column a projection reads, by name. A
/// column shared by several tables is encoded alike in each.
const COLUMN_ENCODINGS: &[(&str, ColumnEncoding)] = &[
    ("address", ColumnEncoding::FixedHex(20)),
    ("base_fee_per_gas", ColumnEncoding::Unsigned),
    ("blob_gas", ColumnEncoding::Unsigned),
    ("blob_gas_fee_cap", ColumnEncoding::LittleEndian),
    ("blob_hashes", ColumnEncoding::HashList),
    ("block_date_time", ColumnEncoding::TimestampMillis),
    ("block_hash", ColumnEncoding::FixedHex(32)),
    ("block_number", ColumnEncoding::Unsigned),
    ("block_total_bytes", ColumnEncoding::Unsigned),
    ("block_version", ColumnEncoding::Text),
    ("data", ColumnEncoding::Hex),
    (
        "execution_payload_base_fee_per_gas",
        ColumnEncoding::LittleEndian,
    ),
    ("execution_payload_blob_gas_used", ColumnEncoding::Unsigned),
    ("execution_payload_block_hash", ColumnEncoding::FixedHex(32)),
    ("execution_payload_block_number", ColumnEncoding::Unsigned),
    (
        "execution_payload_excess_blob_gas",
        ColumnEncoding::Unsigned,
    ),
    ("execution_payload_gas_limit", ColumnEncoding::Unsigned),
    ("execution_payload_gas_used", ColumnEncoding::Unsigned),
    (
        "execution_payload_parent_hash",
        ColumnEncoding::FixedHex(32),
    ),
    (
        "execution_payload_transactions_count",
        ColumnEncoding::Unsigned,
    ),
    (
        "execution_payload_transactions_total_bytes",
        ColumnEncoding::Unsigned,
    ),
    ("extra_data", ColumnEncoding::Hex),
    ("from", ColumnEncoding::FixedHex(20)),
    ("from_address", ColumnEncoding::FixedHex(20)),
    ("gas", ColumnEncoding::Unsigned),
    ("gas_limit", ColumnEncoding::Unsigned),
    ("gas_price", ColumnEncoding::UnsignedOrLittleEndian),
    ("gas_used", ColumnEncoding::Unsigned),
    ("hash", ColumnEncoding::FixedHex(32)),
    ("input", ColumnEncoding::Hex),
    ("log_index", ColumnEncoding::Unsigned),
    ("nonce", ColumnEncoding::Unsigned),
    ("position", ColumnEncoding::Unsigned),
    ("size", ColumnEncoding::Unsigned),
    ("slot", ColumnEncoding::Unsigned),
    ("slot_start_date_time", ColumnEncoding::Unsigned),
    ("success", ColumnEncoding::Boolean),
    ("to", ColumnEncoding::FixedHex(20)),
    ("to_address", ColumnEncoding::FixedHex(20)),
    ("topic0", ColumnEncoding::FixedHex(32)),
    ("topic1", ColumnEncoding::FixedHex(32)),
    ("topic2", ColumnEncoding::FixedHex(32)),
    ("topic3", ColumnEncoding::FixedHex(32)),
    ("transaction_hash", ColumnEncoding::FixedHex(32)),
    ("transaction_index", ColumnEncoding::Unsigned),
    ("transaction_type", ColumnEncoding::Unsigned),
    ("type", ColumnEncoding::Unsigned),
    ("value", ColumnEncoding::UnsignedOrLittleEndian),
    ("withdrawal_address", ColumnEncoding::FixedHex(20)),
    ("withdrawal_amount", ColumnEncoding::LittleEndian),
    ("withdrawal_index", ColumnEncoding::Unsigned),
    ("withdrawal_validator_index", ColumnEncoding::Unsigned),
];

fn column_encoding(name: &str) -> Option<ColumnEncoding> {
    COLUMN_ENCODINGS
        .iter()
        .find(|(column, _)| *column == name)
        .map(|(_, encoding)| *encoding)
}

/// Refuse an object whose projected columns arrive as Arrow types their
/// declared encodings do not allow, before any row is decoded.
fn validate_column_types(
    table: XatuTable,
    schema: &Schema,
    columns: &[&str],
) -> Result<(), XatuError> {
    for name in columns {
        let encoding = column_encoding(name).ok_or_else(|| {
            XatuError::Data(format!(
                "projected column {name:?} has no declared encoding"
            ))
        })?;
        let field = schema
            .field_with_name(name)
            .map_err(|error| XatuError::Data(error.to_string()))?;
        if !encoding.accepts(field.data_type()) {
            return Err(XatuError::ColumnType {
                table,
                column: (*name).to_owned(),
                actual: format!("{:?}", field.data_type()),
            });
        }
    }
    Ok(())
}

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

/// Stream `columns` of `object` into `consume`. Every byte requested from
/// the store, footer and merged gaps included, is charged before it is
/// requested to the open's input budget, of which earlier objects used
/// `input_bytes_used`.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn read_projected(
    store: Arc<dyn ObjectStore>,
    object: &CatalogObject,
    columns: &[&str],
    budget: SourceBudget,
    batch_rows: usize,
    input_bytes_used: &mut u64,
    cancellation: &CancellationToken,
    mut consume: impl FnMut(&RecordBatch) -> Result<u64, XatuError>,
) -> Result<ProjectionObjectMetrics, XatuError> {
    if cancellation.is_cancelled() {
        return Err(XatuError::Cancelled);
    }
    let started = Instant::now();
    let location = ObjectPath::parse(&object.location)
        .map_err(|error| XatuError::ObjectStore(error.to_string()))?;
    // Xatu answers 404 Not Found for an object it has not published yet.
    let not_published = || XatuError::NotPublished {
        location: object.location.clone(),
    };
    let head = store.head(&location).await.map_err(|error| match error {
        object_store::Error::NotFound { .. } => not_published(),
        error => XatuError::ObjectStore(error.to_string()),
    })?;
    // Every read is conditioned on this version, so an object rewritten
    // mid-read fails instead of mixing two versions.
    if let Some(detail) = pinning_problem(head.e_tag.as_deref()) {
        return Err(XatuError::Unpinned {
            location: object.location.clone(),
            detail,
        });
    }
    let input = InputBudget::new(budget.max_input_bytes, *input_bytes_used);
    let parquet_error = |error: ParquetError| match input.refused() {
        Some(actual) => XatuError::Budget {
            resource: "input_bytes",
            actual,
            limit: budget.max_input_bytes,
        },
        // The object was removed after its HEAD.
        None if is_not_found(&error) => not_published(),
        None => XatuError::Parquet(error.to_string()),
    };
    let (reader, reader_metrics) = ObjectStoreReader::new(
        Arc::clone(&store),
        &head,
        budget.max_in_flight_requests,
        Arc::clone(&input),
    );
    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .map_err(parquet_error)?;
    let schema = builder.metadata().file_metadata().schema_descr();
    let root_indices = selected_root_indices(schema, columns)?;
    validate_column_types(object.table, builder.schema(), columns)?;
    let (projected_compressed_bytes, largest_row_group) =
        projected_compressed_bytes(builder.metadata(), &root_indices);
    // Refuse the object before any column is requested when the selected
    // column chunks alone exceed what the budget has left.
    let needed = input.used().saturating_add(projected_compressed_bytes);
    if needed > budget.max_input_bytes {
        return Err(XatuError::Budget {
            resource: "input_bytes",
            actual: needed,
            limit: budget.max_input_bytes,
        });
    }
    // The reader fetches, and holds, one row group's selected columns at a
    // time.
    if largest_row_group > budget.max_resident_bytes {
        return Err(XatuError::Budget {
            resource: "resident_bytes",
            actual: largest_row_group,
            limit: budget.max_resident_bytes,
        });
    }
    let projection = ProjectionMask::roots(schema, root_indices);
    let mut stream = builder
        .with_batch_size(batch_rows)
        .with_projection(projection)
        .build()
        .map_err(parquet_error)?;
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
        let batch = batch.map_err(parquet_error)?;
        rows_scanned =
            rows_scanned.saturating_add(u64::try_from(batch.num_rows()).unwrap_or(u64::MAX));
        rows_selected = rows_selected.saturating_add(consume(&batch)?);
        batches = batches.saturating_add(1);
        peak_batch_memory_bytes = peak_batch_memory_bytes
            .max(u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX));
    }

    let reader_metrics = reader_metrics.snapshot();
    *input_bytes_used = input.used();
    Ok(ProjectionObjectMetrics {
        table: object.table,
        partition: object.partition.clone(),
        locator: object.url.to_string(),
        e_tag: head.e_tag,
        object_bytes: head.size,
        projected_compressed_bytes,
        logical_range_requests: reader_metrics.logical_range_requests,
        fetched_bytes: reader_metrics.requested_bytes,
        selected_columns: columns.iter().map(|column| (*column).to_owned()).collect(),
        rows_scanned,
        rows_selected,
        batches,
        peak_batch_memory_bytes,
        elapsed_ms: elapsed_ms(started),
    })
}

/// Whether a Parquet read failed on the object store's 404 Not Found.
fn is_not_found(error: &ParquetError) -> bool {
    matches!(
        error,
        ParquetError::External(source)
            if matches!(
                source.downcast_ref::<object_store::Error>(),
                Some(object_store::Error::NotFound { .. })
            )
    )
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

/// The selected columns' compressed bytes across all row groups, and in the
/// largest row group.
fn projected_compressed_bytes(metadata: &ParquetMetaData, root_indices: &[usize]) -> (u64, u64) {
    let roots = root_indices.iter().copied().collect::<BTreeSet<_>>();
    let schema = metadata.file_metadata().schema_descr();
    metadata
        .row_groups()
        .iter()
        .map(|group| {
            group
                .columns()
                .iter()
                .enumerate()
                .filter(|(leaf, _)| roots.contains(&schema.get_column_root_idx(*leaf)))
                .map(|(_, column)| u64::try_from(column.compressed_size()).unwrap_or(0))
                .fold(0_u64, u64::saturating_add)
        })
        .fold((0, 0), |(total, largest), group| {
            (total.saturating_add(group), largest.max(group))
        })
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
    gas_limit: u64,
    gas_used: u64,
    base_fee_per_gas: Quantity,
    blob_gas_used: Option<u64>,
    excess_blob_gas: Option<u64>,
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
                gas_limit: required_u64(batch, "execution_payload_gas_limit", row)?,
                gas_used: required_u64(batch, "execution_payload_gas_used", row)?,
                base_fee_per_gas: required_quantity_le(
                    batch,
                    "execution_payload_base_fee_per_gas",
                    row,
                )?,
                // Before Mainnet Dencun these columns use zero defaults,
                // but the execution header has no blob fields.
                blob_gas_used: (required_u64(batch, "slot_start_date_time", row)? >= 1_710_338_135)
                    .then(|| optional_u64(batch, "execution_payload_blob_gas_used", row))
                    .transpose()?
                    .flatten(),
                excess_blob_gas: (required_u64(batch, "slot_start_date_time", row)?
                    >= 1_710_338_135)
                    .then(|| optional_u64(batch, "execution_payload_excess_blob_gas", row))
                    .transpose()?
                    .flatten(),
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
        // A log carries zero to four topics; `LOG0` has none.
        let mut topics = Vec::with_capacity(4);
        let mut absent = None;
        for name in ["topic0", "topic1", "topic2", "topic3"] {
            match (optional_topic(batch, name, row)?, absent) {
                (Some(topic), None) => topics.push(topic),
                (Some(_), Some(missing)) => {
                    return Err(XatuError::Data(format!(
                        "{name} follows the absent {missing} at row {row}"
                    )));
                }
                (None, _) => absent = absent.or(Some(name)),
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
                    gas_limit: Some(beacon.gas_limit),
                    gas_used: Some(beacon.gas_used),
                    base_fee_per_gas: Some(beacon.base_fee_per_gas),
                    blob_gas_used: beacon.blob_gas_used,
                    excess_blob_gas: beacon.excess_blob_gas,
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
                validate_transaction_count(
                    number,
                    &values,
                    beacon.transaction_count,
                    selects_transactions(inputs.filters),
                )?;
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

/// Whether `filters` select transactions, so that a block's projected list
/// may be shorter than its payload.
fn selects_transactions(filters: &FilterSet) -> bool {
    !filters.scope.transaction_types.is_empty()
        || !filters.scope.transaction_hashes.is_empty()
        || !filters.scope.senders.is_empty()
        || !filters.scope.recipients.is_empty()
        || !filters.senders.is_empty()
        || !filters.recipients.is_empty()
}

/// Check a block's projected transactions, sorted by index without
/// duplicates, against the count its beacon payload declares. A list the
/// filters did not narrow holds every transaction, so its indices are
/// exactly `0..count`.
fn validate_transaction_count(
    number: BlockNumber,
    transactions: &[TransactionEnvelope],
    count: u32,
    filtered: bool,
) -> Result<(), XatuError> {
    let expected = usize::try_from(count).unwrap_or(usize::MAX);
    if transactions.len() > expected
        || transactions
            .last()
            .is_some_and(|transaction| transaction.index >= count)
    {
        return Err(XatuError::Data(format!(
            "block {number} projects transactions beyond its {count} payload transactions"
        )));
    }
    if !filtered && transactions.len() != expected {
        return Err(XatuError::IncompleteRange {
            table: XatuTable::CanonicalExecutionTransaction,
            range: BlockRange::single(number),
            expected,
            actual: transactions.len(),
        });
    }
    Ok(())
}

/// Prove a chunk's withdrawal rows complete. Withdrawal indices are global
/// and consecutive, and blocks take them in execution order, so between two
/// slots with withdrawals the indices continue exactly, and a slot between
/// them without rows had none. The nearest slots with rows outside the chunk,
/// which `withdrawals` also holds, prove its edges the same way, and a slot
/// with the most withdrawals a payload holds lacks none. Any other edge, and
/// any jump in the indices, leaves the rows unproven: they have not all been
/// exported yet.
fn validate_withdrawal_coverage(
    range: BlockRange,
    beacon_blocks: &BTreeMap<u64, BeaconBlockRow>,
    withdrawals: &BTreeMap<u64, Vec<WithdrawalRow>>,
) -> Result<(), XatuError> {
    fn indices_of(rows: &[WithdrawalRow]) -> impl Iterator<Item = u64> + '_ {
        rows.iter().map(|row| row.index)
    }
    let unproven = |detail: String| XatuError::IncompleteWithdrawals { range, detail };
    let (Some(first), Some(last)) = (
        beacon_blocks.values().next(),
        beacon_blocks.values().next_back(),
    ) else {
        return Ok(());
    };
    // The last index before the chunk, and the first after it.
    let mut previous = withdrawals
        .range(..first.slot)
        .next_back()
        .and_then(|(_, rows)| indices_of(rows).max());
    let next = withdrawals
        .range(last.slot.saturating_add(1)..)
        .next()
        .and_then(|(_, rows)| indices_of(rows).min());
    let mut unproven_empty = None;
    let mut last_full = false;
    for (position, block) in beacon_blocks.values().enumerate() {
        let mut indices = withdrawals
            .get(&block.slot)
            .map_or_else(Vec::new, |rows| indices_of(rows).collect::<Vec<_>>());
        if indices.len() > MAX_WITHDRAWALS_PER_PAYLOAD {
            return Err(XatuError::Data(format!(
                "slot {} has {} withdrawal rows; a payload holds at most {MAX_WITHDRAWALS_PER_PAYLOAD}",
                block.slot,
                indices.len()
            )));
        }
        last_full = indices.len() == MAX_WITHDRAWALS_PER_PAYLOAD;
        indices.sort_unstable();
        let Some(&lowest) = indices.first() else {
            unproven_empty.get_or_insert(block.slot);
            continue;
        };
        match previous {
            Some(before) if lowest <= before => {
                return Err(XatuError::Data(format!(
                    "slot {} repeats or reorders withdrawal index {lowest}",
                    block.slot
                )));
            }
            Some(before) if lowest == before.saturating_add(1) => {}
            // A full payload at the chunk's first slot lacks no rows.
            _ if position == 0 && last_full => {}
            Some(before) => {
                return Err(unproven(format!(
                    "withdrawal indices jump from {before} to {lowest} at slot {}",
                    block.slot
                )));
            }
            None => {
                return Err(unproven(match unproven_empty {
                    Some(slot) => format!(
                        "slot {slot} has no withdrawal rows, and no earlier slot shows it had none"
                    ),
                    None => format!(
                        "no earlier slot shows that slot {} lacks no withdrawal index below {lowest}",
                        block.slot
                    ),
                }));
            }
        }
        for pair in indices.windows(2) {
            if pair[1] == pair[0] {
                return Err(XatuError::Data(format!(
                    "slot {} repeats withdrawal index {}",
                    block.slot, pair[1]
                )));
            }
            if pair[1] != pair[0].saturating_add(1) {
                return Err(unproven(format!(
                    "withdrawal indices jump from {} to {} at slot {}",
                    pair[0], pair[1], block.slot
                )));
            }
        }
        previous = indices.last().copied();
        unproven_empty = None;
    }
    match (previous, next) {
        (Some(before), Some(after)) if after <= before => Err(XatuError::Data(format!(
            "slot after the chunk repeats or reorders withdrawal index {after}"
        ))),
        (Some(before), Some(after)) if after == before.saturating_add(1) => Ok(()),
        // A full payload at the chunk's last slot lacks no rows.
        _ if last_full => Ok(()),
        _ => Err(unproven(match unproven_empty {
            Some(slot) => {
                format!("slot {slot} has no withdrawal rows, and no later slot shows it had none")
            }
            None => format!(
                "no later slot shows that slot {} lacks no withdrawal index above {}",
                last.slot,
                previous.unwrap_or_default()
            ),
        })),
    }
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
                binary_at(column(batch, "extra_data")?, row)?,
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

/// Collect the withdrawal rows of `slots`, and of the nearest slot with rows
/// on either side of them, whose indices bound theirs.
fn parse_withdrawals(
    batch: &RecordBatch,
    slots: &BTreeSet<u64>,
    output: &mut BTreeMap<u64, Vec<WithdrawalRow>>,
) -> Result<u64, XatuError> {
    let (Some(&first), Some(&last)) = (slots.first(), slots.last()) else {
        return Ok(0);
    };
    let mut selected = 0_u64;
    for row in 0..batch.num_rows() {
        let slot = required_u64(batch, "slot", row)?;
        if slot < first {
            // Keep only the nearest earlier slot.
            if output.range(slot.saturating_add(1)..first).next().is_some() {
                continue;
            }
            if let Some(farther) = output.range(..slot).next().map(|(slot, _)| *slot) {
                output.remove(&farther);
            }
        } else if slot > last {
            // Keep only the nearest later slot.
            if output.range(last.saturating_add(1)..slot).next().is_some() {
                continue;
            }
            if let Some(farther) = output
                .range(slot.saturating_add(1)..)
                .next()
                .map(|(slot, _)| *slot)
            {
                output.remove(&farther);
            }
        } else if slots.contains(&slot) {
            selected = selected.saturating_add(1);
        } else {
            continue;
        }
        output.entry(slot).or_default().push(WithdrawalRow {
            index: required_u64(batch, "withdrawal_index", row)?,
            validator_index: required_u64(batch, "withdrawal_validator_index", row)?,
            address: required_address(batch, "withdrawal_address", row)?,
            amount_gwei: required_quantity_u64_le(batch, "withdrawal_amount", row)?,
        });
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
    // Each block's execution size depends on its withdrawals.
    validate_withdrawal_coverage(range, beacon_blocks, withdrawals)?;
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
            // One receipt per projected transaction, so the receipts are
            // complete for the same type-3 predicate. Listing their hashes
            // would narrow the claim below the consumer's filter.
            receipts: Material::Filtered {
                value: receipts,
                scope: scope.clone(),
                completeness: Completeness::DatasetDeclared,
            },
            // Receipts imply logs, but this projection reads no log rows, so
            // its receipts carry none. Partial logs keep the receipts from
            // satisfying a log requirement.
            logs: Material::Filtered {
                value: Vec::new(),
                scope: scope.clone(),
                completeness: Completeness::Partial,
            },
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

/// Decode a `ClickHouse` `UInt128` or `UInt256`, exported as little-endian
/// fixed-width bytes.
fn required_quantity_le(
    batch: &RecordBatch,
    name: &str,
    row: usize,
) -> Result<Quantity, XatuError> {
    let array = column(batch, name)?;
    if array.is_null(row) {
        return Err(XatuError::Data(format!("{name} is null at row {row}")));
    }
    let value = array
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .map(|array| array.value(row))
        .ok_or_else(|| type_error(name, array))?;
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
            fixed_bytes_at::<32>(
                values.as_ref(),
                index,
                &format!("{name} element {index} at row {row}"),
            )
            .map(BlockHash::new)
        })
        .collect()
}

fn fixed_bytes<const N: usize>(
    batch: &RecordBatch,
    name: &str,
    row: usize,
) -> Result<[u8; N], XatuError> {
    fixed_bytes_at(column(batch, name)?, row, &format!("{name} at row {row}"))
}

/// Decode an `N`-byte value by its Arrow type: a binary column exactly `N`
/// wide holds raw bytes, and any other holds `0x`-prefixed hexadecimal text.
fn fixed_bytes_at<const N: usize>(
    array: &dyn Array,
    row: usize,
    context: &str,
) -> Result<[u8; N], XatuError> {
    let value = binary_at(array, row)?;
    if matches!(array.data_type(), DataType::FixedSizeBinary(width) if usize::try_from(*width) == Ok(N))
    {
        return value
            .try_into()
            .map_err(|_| XatuError::Data(format!("{context} is not {N} raw bytes")));
    }
    decode_fixed_bytes(value, context)
}

/// A topic, or `None` for an absent topic: null, empty text, `0x`, or
/// either empty representation with `ClickHouse` `FixedString` NUL padding.
fn optional_topic(
    batch: &RecordBatch,
    name: &str,
    row: usize,
) -> Result<Option<[u8; 32]>, XatuError> {
    let array = column(batch, name)?;
    if array.is_null(row) {
        return Ok(None);
    }
    let raw = matches!(array.data_type(), DataType::FixedSizeBinary(32));
    let value = binary_at(array, row)?;
    if !raw
        && (value.iter().all(|byte| *byte == 0)
            || value
                .strip_prefix(b"0x")
                .is_some_and(|rest| rest.iter().all(|byte| *byte == 0)))
    {
        return Ok(None);
    }
    fixed_bytes_at(array, row, &format!("{name} at row {row}")).map(Some)
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

/// Decode `0x`-prefixed hexadecimal text of exactly `N` bytes.
fn decode_fixed_bytes<const N: usize>(value: &[u8], context: &str) -> Result<[u8; N], XatuError> {
    let hexadecimal = value
        .strip_prefix(b"0x")
        .filter(|hexadecimal| hexadecimal.len() == N * 2)
        .ok_or_else(|| {
            XatuError::Data(format!(
                "{context} has {} bytes; expected 0x-prefixed hex of {N} bytes",
                value.len()
            ))
        })?;
    let mut bytes = [0; N];
    hex::decode_to_slice(hexadecimal, &mut bytes)
        .map_err(|error| XatuError::Data(format!("{context} has invalid hex: {error}")))?;
    Ok(bytes)
}

/// Decode `0x`-prefixed hexadecimal text; an empty value holds no bytes.
fn decode_variable_bytes(value: &[u8], context: &str) -> Result<Vec<u8>, XatuError> {
    if value.is_empty() {
        return Ok(Vec::new());
    }
    let hexadecimal = value
        .strip_prefix(b"0x")
        .ok_or_else(|| XatuError::Data(format!("{context} is not 0x-prefixed hexadecimal")))?;
    if !hexadecimal.len().is_multiple_of(2) {
        return Err(XatuError::Data(format!(
            "{context} has an odd hexadecimal length"
        )));
    }
    hex::decode(hexadecimal)
        .map_err(|error| XatuError::Data(format!("{context} has invalid hex: {error}")))
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
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use super::*;
    use arrow_array::{ArrayRef, Int64Array};
    use arrow_schema::{DataType, Field, Schema};
    use bytes::Bytes;
    use futures::stream::BoxStream;
    use leani_primitives::{Capability, CapabilitySet, TopicFilter};
    use object_store::{
        CopyOptions, GetOptions, GetRange, GetResult, ListResult, MultipartUpload, ObjectMeta,
        PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory,
    };
    use parquet::{
        arrow::ArrowWriter,
        file::properties::{EnabledStatistics, WriterProperties},
    };

    /// An in-memory store that records how many ranged reads are in flight
    /// and can replace or remove an object just before its first read below
    /// the footer.
    #[derive(Debug, Default)]
    struct RecordingStore {
        inner: InMemory,
        in_flight: AtomicUsize,
        peak_in_flight: AtomicUsize,
        replacement: std::sync::Mutex<Option<(ObjectPath, Bytes)>>,
        removal: std::sync::Mutex<Option<ObjectPath>>,
        /// Report every HEAD's `ETag` as weak, as a compressing CDN may.
        weak_e_tags: bool,
        /// Bytes of every ranged read requested.
        requested_bytes: AtomicUsize,
    }

    impl std::fmt::Display for RecordingStore {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("RecordingStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for RecordingStore {
        async fn put_opts(
            &self,
            location: &ObjectPath,
            payload: PutPayload,
            options: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &ObjectPath,
            options: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            let Some(GetRange::Bounded(range)) = options.range.clone() else {
                let mut result = self.inner.get_opts(location, options).await?;
                if self.weak_e_tags {
                    result.meta.e_tag = result.meta.e_tag.map(|tag| format!("W/{tag}"));
                }
                return Ok(result);
            };
            self.requested_bytes.fetch_add(
                usize::try_from(range.end - range.start).expect("range length"),
                Ordering::SeqCst,
            );
            if range.end < self.inner.head(location).await?.size {
                let replacement = self.replacement.lock().expect("replacement lock").take();
                if let Some((path, bytes)) = replacement {
                    self.inner.put(&path, bytes.into()).await?;
                }
                let removal = self.removal.lock().expect("removal lock").take();
                if let Some(path) = removal {
                    self.inner.delete(&path).await?;
                }
            }
            let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak_in_flight.fetch_max(now, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(20)).await;
            let result = self.inner.get_opts(location, options).await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            result
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<ObjectPath>>,
        ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// One uncompressed Parquet row group without dictionaries or statistics,
    /// so objects with equally wide values share one byte layout.
    fn parquet_object(columns: Vec<(&str, ArrayRef)>) -> Bytes {
        parquet_object_in_row_groups(columns, 1024 * 1024)
    }

    /// An object whose row groups hold at most `rows` rows each.
    fn parquet_object_in_row_groups(columns: Vec<(&str, ArrayRef)>, rows: usize) -> Bytes {
        let batch = RecordBatch::try_from_iter(columns).expect("batch");
        let properties = WriterProperties::builder()
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::None)
            .set_max_row_group_row_count(Some(rows))
            .build();
        let mut bytes = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut bytes, batch.schema(), Some(properties)).expect("writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");
        Bytes::from(bytes)
    }

    const EXECUTION_BLOCK_OBJECT: &str = "canonical_execution_block/1000/0.parquet";

    fn execution_block_object() -> CatalogObject {
        CatalogObject {
            table: XatuTable::CanonicalExecutionBlock,
            partition: "0".to_owned(),
            location: EXECUTION_BLOCK_OBJECT.to_owned(),
            url: url::Url::parse("https://xatu.invalid/canonical_execution_block/1000/0.parquet")
                .expect("URL"),
        }
    }

    async fn stored_object(store: &RecordingStore, bytes: Bytes) -> CatalogObject {
        let location = ObjectPath::from(EXECUTION_BLOCK_OBJECT);
        store.put(&location, bytes.into()).await.expect("put");
        execution_block_object()
    }

    fn test_budget(max_in_flight_requests: usize) -> SourceBudget {
        SourceBudget {
            max_input_bytes: 64 << 20,
            max_frame_bytes: 1 << 20,
            max_frames: 1_000,
            max_buffered_frames: 16,
            max_in_flight_requests,
            temporary_disk_bytes: 1,
            max_resident_bytes: 64 << 20,
        }
    }

    #[allow(clippy::unnecessary_wraps)]
    fn rows(batch: &RecordBatch) -> Result<u64, XatuError> {
        Ok(u64::try_from(batch.num_rows()).unwrap_or(u64::MAX))
    }

    #[tokio::test]
    async fn range_reads_keep_to_the_budgeted_requests_in_flight() {
        let store = Arc::new(RecordingStore::default());
        let numbers =
            |first: u64| Arc::new(UInt64Array::from_iter_values(first..first + 4)) as ArrayRef;
        // Projected columns more than a coalescing gap apart.
        let filler = |byte: u8| {
            Arc::new(BinaryArray::from_iter_values(
                (0..4).map(|_| vec![byte; 400_000]),
            )) as ArrayRef
        };
        let object = stored_object(
            &store,
            parquet_object(vec![
                ("block_number", numbers(1)),
                ("filler_a", filler(0xaa)),
                ("gas_used", numbers(5)),
                ("filler_b", filler(0xbb)),
                ("base_fee_per_gas", numbers(9)),
            ]),
        )
        .await;
        let mut projected = 0;
        let metrics = read_projected(
            Arc::clone(&store) as Arc<dyn ObjectStore>,
            &object,
            &["block_number", "gas_used", "base_fee_per_gas"],
            test_budget(1),
            8_192,
            &mut projected,
            &CancellationToken::new(),
            rows,
        )
        .await
        .expect("projection");
        assert_eq!(metrics.rows_selected, 4);
        // Audit History-1: object_store fetched up to ten ranges at once.
        assert_eq!(
            store.peak_in_flight.load(Ordering::SeqCst),
            1,
            "the source budget allows one request in flight"
        );
    }

    #[tokio::test]
    async fn range_reads_are_pinned_to_the_object_version_seen_at_head() {
        let store = Arc::new(RecordingStore::default());
        let version = |first: u64| {
            parquet_object(vec![(
                "block_number",
                Arc::new(UInt64Array::from_iter_values(first..first + 1_000)) as ArrayRef,
            )])
        };
        let object = stored_object(&store, version(0)).await;
        // The publisher rewrites the object after its footer was read.
        *store.replacement.lock().expect("replacement lock") =
            Some((ObjectPath::from(EXECUTION_BLOCK_OBJECT), version(1_000_000)));
        let mut projected = 0;
        let mut first_seen = None;
        let result = read_projected(
            Arc::clone(&store) as Arc<dyn ObjectStore>,
            &object,
            &["block_number"],
            test_budget(4),
            8_192,
            &mut projected,
            &CancellationToken::new(),
            |batch| {
                first_seen = first_seen.or_else(|| {
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .map(|numbers| numbers.value(0))
                });
                rows(batch)
            },
        )
        .await;
        // Audit History-4: the footer of one version decoded the pages of
        // another.
        let Err(error) = result else {
            panic!(
                "a read spanning two object versions succeeded, starting at block {first_seen:?}"
            );
        };
        assert!(
            error.to_string().contains("precondition"),
            "the ETag condition refuses the rewritten object: {error}"
        );
    }

    #[tokio::test]
    async fn projected_columns_must_arrive_as_their_declared_arrow_types() {
        let store = Arc::new(RecordingStore::default());
        let object = stored_object(
            &store,
            parquet_object(vec![
                (
                    "block_number",
                    Arc::new(UInt64Array::from(vec![7])) as ArrayRef,
                ),
                // `extra_data` is hexadecimal text, not a signed integer.
                (
                    "extra_data",
                    Arc::new(Int64Array::from(vec![1])) as ArrayRef,
                ),
            ]),
        )
        .await;
        let mut projected = 0;
        let mut blocks = BTreeMap::new();
        // Block 7 is outside the range, so no row reaches a decoder.
        let range = BlockRange::single(BlockNumber(42));
        let error = read_projected(
            Arc::clone(&store) as Arc<dyn ObjectStore>,
            &object,
            &["block_number", "extra_data"],
            test_budget(4),
            8_192,
            &mut projected,
            &CancellationToken::new(),
            |batch| parse_execution_blocks(batch, range, &mut blocks),
        )
        .await
        .expect_err("a column of an undeclared Arrow type");
        assert!(error.to_string().contains("extra_data"), "{error}");
    }

    #[test]
    fn every_projected_column_declares_its_arrow_encoding() {
        for column in EXECUTION_BLOCK_COLUMNS
            .iter()
            .chain(EXECUTION_TRANSACTION_COLUMNS)
            .chain(BEACON_BLOCK_COLUMNS)
            .chain(BEACON_TRANSACTION_COLUMNS)
            .chain(WITHDRAWAL_COLUMNS)
            .chain(GENERIC_EXECUTION_BLOCK_COLUMNS)
            .chain(GENERIC_BEACON_BLOCK_COLUMNS)
            .chain(GENERIC_TRANSACTION_COLUMNS)
            .chain(GENERIC_LOG_COLUMNS)
        {
            assert!(
                column_encoding(column).is_some(),
                "{column} has no declared Arrow encoding"
            );
        }
        let accepts = |column: &str, data_type: DataType| {
            column_encoding(column)
                .expect("declared column")
                .accepts(&data_type)
        };
        assert!(accepts("block_number", DataType::UInt64));
        assert!(!accepts("block_number", DataType::Int64));
        assert!(accepts("transaction_hash", DataType::FixedSizeBinary(66)));
        assert!(accepts("transaction_hash", DataType::FixedSizeBinary(32)));
        assert!(!accepts("transaction_hash", DataType::FixedSizeBinary(20)));
        assert!(accepts("to_address", DataType::Utf8));
        assert!(!accepts("input", DataType::FixedSizeBinary(32)));
        assert!(accepts("gas_price", DataType::FixedSizeBinary(16)));
        assert!(!accepts("withdrawal_amount", DataType::Utf8));
        assert!(!accepts("success", DataType::UInt8));
    }

    #[test]
    fn hex_columns_are_decoded_by_their_arrow_type_not_their_content() {
        let text = |value: &str| {
            RecordBatch::try_from_iter(vec![(
                "value",
                Arc::new(StringArray::from(vec![value])) as ArrayRef,
            )])
            .expect("batch")
        };
        // Audit History-2: 32 characters of text were taken for a raw hash.
        assert!(required_hash(&text("0123456789abcdef0123456789abcdef"), "value", 0).is_err());
        assert_eq!(
            required_hash(&text(&format!("0x{}", "ab".repeat(32))), "value", 0).expect("hex hash"),
            BlockHash::new([0xab; 32])
        );
        let binary = |value: &[u8]| {
            RecordBatch::try_from_iter(vec![(
                "value",
                Arc::new(BinaryArray::from_vec(vec![value])) as ArrayRef,
            )])
            .expect("batch")
        };
        // Raw bytes in a hexadecimal-text column are not guessed at.
        assert!(optional_variable_bytes(&binary(&[0x12, 0x34]), "value", 0).is_err());
        assert_eq!(
            optional_variable_bytes(&binary(b"0x1234"), "value", 0).expect("hex bytes"),
            Some(vec![0x12, 0x34])
        );
        // A fixed-width binary column of the value's width holds raw bytes.
        let raw = FixedSizeBinaryArray::try_from_iter([[0xcd_u8; 32]].into_iter()).expect("array");
        let batch =
            RecordBatch::try_from_iter(vec![("value", Arc::new(raw) as ArrayRef)]).expect("batch");
        assert_eq!(
            required_hash(&batch, "value", 0).expect("raw hash"),
            BlockHash::new([0xcd; 32])
        );
    }

    fn generic_transaction(index: u32) -> TransactionEnvelope {
        TransactionEnvelope {
            hash: TransactionHash::new([u8::try_from(index).unwrap_or(u8::MAX); 32]),
            transaction_type: 2,
            index,
            encoded: None,
            from: Some(Address::new([0x22; 20])),
            to: None,
            nonce: Some(0),
            gas_limit: Some(21_000),
            value: Some(quantity_from_u64(0)),
            input: None,
            max_fee_per_gas: None,
            max_priority_fee_per_gas: None,
            max_fee_per_blob_gas: None,
            blob_versioned_hashes: Vec::new(),
            size_bytes: None,
        }
    }

    /// Normalize block 42, whose payload holds `transaction_count`
    /// transactions, from projected rows at `indices`.
    fn generic_transaction_frames(
        filters: &FilterSet,
        transaction_count: u32,
        indices: &[u32],
    ) -> Result<Vec<BlockFrame>, XatuError> {
        let number = 42;
        let hash = BlockHash::new([0x42; 32]);
        normalize_generic_frames(GenericNormalizationInputs {
            range: BlockRange::single(BlockNumber(number)),
            kind: GenericProjectionKind::Transactions,
            include_header: true,
            filters,
            execution_blocks: &BTreeMap::from([(
                number,
                GenericExecutionBlockRow {
                    hash,
                    timestamp: 1_700_000_000,
                },
            )]),
            beacon_blocks: &BTreeMap::from([(
                number,
                GenericBeaconBlockRow {
                    hash,
                    parent_hash: BlockHash::ZERO,
                    timestamp: 1_700_000_000,
                    transaction_count,
                    gas_limit: 30_000_000,
                    gas_used: 21_000,
                    base_fee_per_gas: quantity_from_u64(7),
                    blob_gas_used: None,
                    excess_blob_gas: None,
                },
            )]),
            transactions: BTreeMap::from([(
                number,
                indices.iter().copied().map(generic_transaction).collect(),
            )]),
            logs: BTreeMap::new(),
            provenance: &[],
        })
    }

    #[test]
    fn unfiltered_transactions_account_for_every_payload_transaction() {
        let unfiltered = FilterSet::default();
        // Audit M-H8: a block with three transactions was projected as two.
        assert!(
            matches!(
                generic_transaction_frames(&unfiltered, 3, &[0, 2]),
                Err(XatuError::IncompleteRange {
                    table: XatuTable::CanonicalExecutionTransaction,
                    expected: 3,
                    actual: 2,
                    ..
                })
            ),
            "an unfiltered list shorter than the payload"
        );
        generic_transaction_frames(&unfiltered, 3, &[0, 1, 2]).expect("complete transactions");
        generic_transaction_frames(&unfiltered, 0, &[]).expect("an empty block");
        assert!(generic_transaction_frames(&unfiltered, 1, &[1]).is_err());

        // A filtered list may be shorter, but never longer or outside the
        // payload.
        let senders = FilterSet {
            senders: vec![Address::new([0x22; 20])],
            ..FilterSet::default()
        };
        generic_transaction_frames(&senders, 3, &[2]).expect("a filtered subset");
        assert!(
            generic_transaction_frames(&senders, 1, &[0, 1]).is_err(),
            "more transactions than the payload"
        );
        assert!(
            generic_transaction_frames(&senders, 2, &[2]).is_err(),
            "an index outside the payload"
        );
    }

    fn log_batch(topic_columns: [ArrayRef; 4]) -> RecordBatch {
        let hash = format!("0x{}", "44".repeat(32));
        let address = format!("0x{}", "55".repeat(20));
        let [first, second, third, fourth] = topic_columns;
        RecordBatch::try_from_iter(vec![
            (
                "block_number",
                Arc::new(UInt64Array::from(vec![42])) as ArrayRef,
            ),
            (
                "transaction_index",
                Arc::new(UInt64Array::from(vec![1])) as ArrayRef,
            ),
            (
                "transaction_hash",
                Arc::new(StringArray::from(vec![hash.as_str()])) as ArrayRef,
            ),
            (
                "log_index",
                Arc::new(UInt64Array::from(vec![2])) as ArrayRef,
            ),
            (
                "address",
                Arc::new(StringArray::from(vec![address.as_str()])) as ArrayRef,
            ),
            ("topic0", first),
            ("topic1", second),
            ("topic2", third),
            ("topic3", fourth),
            (
                "data",
                Arc::new(StringArray::from(vec![Some("0x")])) as ArrayRef,
            ),
        ])
        .expect("log batch")
    }

    fn text_topic(value: Option<&str>) -> ArrayRef {
        Arc::new(StringArray::from(vec![value]))
    }

    fn parse_block_42_logs(
        batch: &RecordBatch,
        filters: &FilterSet,
    ) -> Result<BTreeMap<u64, Vec<Log>>, XatuError> {
        let mut output = BTreeMap::new();
        parse_generic_logs(
            batch,
            BlockRange::single(BlockNumber(42)),
            filters,
            LogFieldSet::ALL,
            &mut output,
        )?;
        Ok(output)
    }

    #[test]
    fn logs_without_topics_are_projected() {
        let topic = format!("0x{}", "66".repeat(32));
        let nul_padded: ArrayRef = Arc::new(
            FixedSizeBinaryArray::try_from_iter([[0_u8; 66]].into_iter()).expect("NUL topic"),
        );
        let mut padded_hex = [0_u8; 66];
        padded_hex[..2].copy_from_slice(b"0x");
        let padded_hex: ArrayRef =
            Arc::new(FixedSizeBinaryArray::try_from_iter([padded_hex].into_iter()).unwrap());
        for (name, topic0) in [
            ("null topic0", text_topic(None)),
            (
                "empty hexadecimal topic0 from public Xatu",
                text_topic(Some("0x")),
            ),
            ("NUL-padded empty hexadecimal topic0", padded_hex),
            (
                "binary empty hexadecimal topic0",
                Arc::new(BinaryArray::from_vec(vec![b"0x"])) as ArrayRef,
            ),
            ("empty topic0", text_topic(Some(""))),
            ("NUL-padded FixedString topic0", nul_padded),
        ] {
            // Audit M-H9: one LOG0 row aborted the whole chunk.
            let output = parse_block_42_logs(
                &log_batch([topic0, text_topic(None), text_topic(None), text_topic(None)]),
                &FilterSet::default(),
            )
            .unwrap_or_else(|error| panic!("{name}: {error}"));
            assert!(output[&42][0].topics.is_empty(), "{name}");
        }
        assert!(
            parse_block_42_logs(
                &log_batch([
                    text_topic(Some(&topic)),
                    text_topic(None),
                    text_topic(Some(&topic)),
                    text_topic(None),
                ]),
                &FilterSet::default(),
            )
            .is_err(),
            "a topic after an absent topic"
        );
        let topic_zero = FilterSet {
            scope: FilterScope {
                topics: vec![TopicFilter {
                    position: 0,
                    alternatives: vec![[0x66; 32]],
                }],
                ..FilterScope::default()
            },
            ..FilterSet::default()
        };
        let output = parse_block_42_logs(
            &log_batch([
                text_topic(None),
                text_topic(None),
                text_topic(None),
                text_topic(None),
            ]),
            &topic_zero,
        )
        .expect("a topic filter skips LOG0 rows");
        assert!(output.is_empty());
    }

    #[test]
    fn a_zero_topic_is_present_but_malformed_short_hex_is_rejected() {
        let raw: ArrayRef =
            Arc::new(FixedSizeBinaryArray::try_from_iter([[0_u8; 32]].into_iter()).unwrap());
        let output = parse_block_42_logs(
            &log_batch([raw, text_topic(None), text_topic(None), text_topic(None)]),
            &FilterSet::default(),
        )
        .unwrap();
        assert_eq!(output[&42][0].topics, vec![[0; 32]]);
        let zero_text = format!("0x{}", "0".repeat(64));
        let output = parse_block_42_logs(
            &log_batch([
                text_topic(Some(&zero_text)),
                text_topic(None),
                text_topic(None),
                text_topic(None),
            ]),
            &FilterSet::default(),
        )
        .unwrap();
        assert_eq!(output[&42][0].topics, vec![[0; 32]]);
        assert!(
            parse_block_42_logs(
                &log_batch([
                    text_topic(Some("0x00")),
                    text_topic(None),
                    text_topic(None),
                    text_topic(None)
                ]),
                &FilterSet::default()
            )
            .is_err()
        );
    }

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

    const DENCUN_BLOCK: u64 = 19_426_589;
    const DENCUN_SLOT: u64 = 8_626_181;

    /// The withdrawal rows of Dencun block 19,426,589, truncated or extended
    /// to `count`.
    fn dencun_withdrawals(count: u64) -> Vec<WithdrawalRow> {
        (0..count)
            .map(|offset| WithdrawalRow {
                index: 38_266_054 + offset,
                validator_index: 1_268_201 + offset,
                address: Address::new([0; 20]),
                amount_gwei: if offset == 15 { 60_026_761 } else { 16_025_579 },
            })
            .collect()
    }

    fn dencun_blocks(hash: BlockHash) -> (ExecutionBlockRow, BeaconBlockRow) {
        let execution = ExecutionBlockRow {
            number: DENCUN_BLOCK,
            hash,
            timestamp: 1_710_338_159,
            gas_used: 7_155_950,
            extra_data: vec![0; 11],
            base_fee_per_gas: 55_745_530_424,
        };
        let beacon = BeaconBlockRow {
            slot: DENCUN_SLOT,
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
        (execution, beacon)
    }

    /// Normalize Dencun block 19,426,589 with one blob transaction and the
    /// given withdrawal rows.
    fn dencun_blob_frames(
        withdrawals: &BTreeMap<u64, Vec<WithdrawalRow>>,
    ) -> Result<Vec<BlockFrame>, XatuError> {
        let transaction_hash = TransactionHash::new([0x33; 32]);
        let (execution, beacon) = dencun_blocks(BlockHash::new([0x11; 32]));
        normalize_frames(NormalizationInputs {
            range: BlockRange::single(BlockNumber(DENCUN_BLOCK)),
            execution_blocks: &BTreeMap::from([(DENCUN_BLOCK, execution)]),
            beacon_blocks: &BTreeMap::from([(DENCUN_BLOCK, beacon)]),
            beacon_transactions: vec![BeaconTransactionRow {
                slot: DENCUN_SLOT,
                index: 60,
                hash: transaction_hash,
                from: Address::new([0x44; 20]),
                to: Some(Address::new([0x55; 20])),
                gas_limit: 21_000,
                size_bytes: 150,
                blob_gas: 131_072,
                max_fee_per_blob_gas: quantity_from_u64(1),
                blob_hashes: vec![BlockHash::new([0x66; 32])],
            }],
            transaction_sizes: &BTreeMap::from([(DENCUN_SLOT, dencun_transaction_sizes())]),
            withdrawals,
            execution_transactions: &BTreeMap::from([(
                transaction_hash,
                ExecutionTransactionRow {
                    block_number: DENCUN_BLOCK,
                    index: 60,
                    hash: transaction_hash,
                    gas_used: 21_000,
                    gas_price: quantity_from_u64(1),
                    success: true,
                },
            )]),
            provenance: &[],
        })
    }

    fn dencun_slot_withdrawals(rows: Vec<WithdrawalRow>) -> BTreeMap<u64, Vec<WithdrawalRow>> {
        BTreeMap::from([(DENCUN_SLOT, rows)])
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
        let (execution, beacon) = dencun_blocks(BlockHash::ZERO);

        assert_eq!(
            execution_block_rlp_len(
                &execution,
                &beacon,
                &transaction_sizes,
                &dencun_withdrawals(16)
            )
            .expect("exact size"),
            34_975
        );
    }

    #[test]
    fn blob_block_sizes_need_every_withdrawal_row_and_at_most_sixteen() {
        dencun_blob_frames(&dencun_slot_withdrawals(dencun_withdrawals(16)))
            .expect("the verified block");
        // Audit M-H8: missing rows were read as a block without withdrawals,
        // which understated its size.
        assert!(
            matches!(
                dencun_blob_frames(&BTreeMap::new()),
                Err(XatuError::IncompleteWithdrawals { .. })
            ),
            "a slot without withdrawal rows"
        );
        assert!(
            matches!(
                dencun_blob_frames(&dencun_slot_withdrawals(dencun_withdrawals(17))),
                Err(XatuError::Data(_))
            ),
            "more withdrawals than a payload holds"
        );
        let mut gap = dencun_withdrawals(16);
        gap[3].index += 100;
        assert!(
            matches!(
                dencun_blob_frames(&dencun_slot_withdrawals(gap)),
                Err(XatuError::IncompleteWithdrawals { .. })
            ),
            "non-consecutive withdrawal indices"
        );
        let mut duplicate = dencun_withdrawals(16);
        duplicate[3].index = duplicate[2].index;
        assert!(
            matches!(
                dencun_blob_frames(&dencun_slot_withdrawals(duplicate)),
                Err(XatuError::Data(_))
            ),
            "a duplicated withdrawal row"
        );
    }

    #[test]
    fn blob_receipts_declare_the_predicate_of_their_transactions() {
        let frames = dencun_blob_frames(&dencun_slot_withdrawals(dencun_withdrawals(16)))
            .expect("normalized blob frame");

        let frame = &frames[0];
        let Material::Filtered {
            scope: transaction_scope,
            ..
        } = &frame.transactions
        else {
            panic!("blob transactions are a filtered projection");
        };
        let Material::Filtered {
            value: receipts,
            scope: receipt_scope,
            ..
        } = &frame.receipts
        else {
            panic!("blob receipts are a filtered projection");
        };
        assert_eq!(receipts.len(), 1);
        assert_eq!(receipt_scope, transaction_scope);
        // The blobs processor's requirement filter.
        let blob_transactions = FilterScope {
            transaction_types: vec![3],
            ..FilterScope::default()
        };
        assert!(receipt_scope.covers_at(&blob_transactions, frame.block.number));
    }

    #[tokio::test]
    async fn weak_etags_are_refused_before_any_range_read() {
        let store = Arc::new(RecordingStore {
            weak_e_tags: true,
            ..RecordingStore::default()
        });
        let object = stored_object(
            &store,
            parquet_object(vec![(
                "block_number",
                Arc::new(UInt64Array::from(vec![7])) as ArrayRef,
            )]),
        )
        .await;
        let mut projected = 0;
        let error = read_projected(
            Arc::clone(&store) as Arc<dyn ObjectStore>,
            &object,
            &["block_number"],
            test_budget(4),
            8_192,
            &mut projected,
            &CancellationToken::new(),
            rows,
        )
        .await
        .expect_err("a weak ETag cannot pin a read");
        // Review M4: If-Match compares strongly, so a weak ETag failed every
        // read with a precondition error that looked transient.
        assert!(error.to_string().contains("weak ETag"), "{error}");
        assert_eq!(store.peak_in_flight.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn an_unpublished_object_is_reported_as_not_published() {
        let store = Arc::new(RecordingStore::default());
        let mut projected = 0;
        let error = read_projected(
            Arc::clone(&store) as Arc<dyn ObjectStore>,
            &execution_block_object(),
            &["block_number"],
            test_budget(4),
            8_192,
            &mut projected,
            &CancellationToken::new(),
            rows,
        )
        .await
        .expect_err("an object the store does not hold");
        // Xatu answers 404 for an object it has not published yet, which
        // read as an unavailable source.
        assert_eq!(
            error,
            XatuError::NotPublished {
                location: EXECUTION_BLOCK_OBJECT.to_owned(),
            }
        );
        assert_eq!(store.requested_bytes.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn an_object_removed_mid_read_is_reported_as_not_published() {
        let store = Arc::new(RecordingStore::default());
        let object = stored_object(
            &store,
            parquet_object(vec![(
                "block_number",
                Arc::new(UInt64Array::from_iter_values(0..1_000)) as ArrayRef,
            )]),
        )
        .await;
        *store.removal.lock().expect("removal lock") =
            Some(ObjectPath::from(EXECUTION_BLOCK_OBJECT));
        let mut projected = 0;
        let error = read_projected(
            Arc::clone(&store) as Arc<dyn ObjectStore>,
            &object,
            &["block_number"],
            test_budget(4),
            8_192,
            &mut projected,
            &CancellationToken::new(),
            rows,
        )
        .await
        .expect_err("a range read of a removed object");
        assert_eq!(
            error,
            XatuError::NotPublished {
                location: EXECUTION_BLOCK_OBJECT.to_owned(),
            }
        );
    }

    #[tokio::test]
    async fn probes_flag_objects_whose_reads_cannot_be_pinned() {
        assert_eq!(crate::catalog::pinning_problem(Some("\"8e7c\"")), None);
        assert!(
            crate::catalog::pinning_problem(None)
                .is_some_and(|problem| problem.contains("no ETag"))
        );
        let store = Arc::new(RecordingStore {
            weak_e_tags: true,
            ..RecordingStore::default()
        });
        let object = stored_object(
            &store,
            parquet_object(vec![(
                "block_number",
                Arc::new(UInt64Array::from(vec![7])) as ArrayRef,
            )]),
        )
        .await;
        let catalog = XatuCatalog::with_store(
            crate::XatuCatalogConfig::default(),
            Arc::clone(&store) as Arc<dyn ObjectStore>,
        );
        let report = catalog.inspect(vec![object], 1).await.expect("inspection");
        // Review M4: the probe neither required nor judged the ETag.
        assert_eq!(report.objects[0].e_tag.as_deref(), Some("W/\"0\""));
        assert!(!report.is_valid());
        assert!(
            report
                .failures
                .iter()
                .any(|failure| failure.error.contains("weak ETag")),
            "{:?}",
            report.failures
        );
    }

    /// Normalize three consecutive blocks without transactions whose slots
    /// hold withdrawal rows with the given indices.
    fn withdrawal_frames(slots: [&[u64]; 3]) -> Result<Vec<BlockFrame>, XatuError> {
        withdrawal_frames_between(None, slots, None)
    }

    fn withdrawal_row(index: u64) -> WithdrawalRow {
        WithdrawalRow {
            index,
            validator_index: index,
            address: Address::new([0; 20]),
            amount_gwei: 1,
        }
    }

    /// [`withdrawal_frames`], with the chunk's nearest outside slots holding
    /// the withdrawal index `before` its first slot and `after` its last.
    fn withdrawal_frames_between(
        before: Option<u64>,
        slots: [&[u64]; 3],
        after: Option<u64>,
    ) -> Result<Vec<BlockFrame>, XatuError> {
        let first_block = 20_000_000;
        let first_slot = 9_000_000;
        let mut execution_blocks = BTreeMap::new();
        let mut beacon_blocks = BTreeMap::new();
        let mut withdrawals = BTreeMap::new();
        if let Some(index) = before {
            withdrawals.insert(first_slot - 2, vec![withdrawal_row(index)]);
        }
        if let Some(index) = after {
            withdrawals.insert(first_slot + 4, vec![withdrawal_row(index)]);
        }
        for (offset, indices) in (0_u64..).zip(slots) {
            let (mut execution, mut beacon) =
                dencun_blocks(BlockHash::new([u8::try_from(offset).expect("offset"); 32]));
            execution.number = first_block + offset;
            beacon.number = execution.number;
            beacon.slot = first_slot + offset;
            beacon.transaction_count = 0;
            beacon.transactions_total_bytes = 0;
            if !indices.is_empty() {
                withdrawals.insert(
                    beacon.slot,
                    indices.iter().copied().map(withdrawal_row).collect(),
                );
            }
            execution_blocks.insert(execution.number, execution);
            beacon_blocks.insert(beacon.number, beacon);
        }
        normalize_frames(NormalizationInputs {
            range: BlockRange::new(BlockNumber(first_block), BlockNumber(first_block + 2))
                .expect("range"),
            execution_blocks: &execution_blocks,
            beacon_blocks: &beacon_blocks,
            beacon_transactions: Vec::new(),
            transaction_sizes: &BTreeMap::new(),
            withdrawals: &withdrawals,
            execution_transactions: &BTreeMap::new(),
            provenance: &[],
        })
    }

    fn indices(range: std::ops::Range<u64>) -> Vec<u64> {
        range.collect()
    }

    #[test]
    fn a_slot_between_consecutive_withdrawal_indices_had_none() {
        // Review I2: a block without withdrawals failed its chunk forever.
        let frames = withdrawal_frames([&indices(0..16), &[], &indices(16..32)])
            .expect("withdrawal indices continue across the empty slot");
        assert_eq!(frames.len(), 3);
    }

    #[test]
    fn a_truncated_withdrawal_slot_leaves_its_chunk_incomplete() {
        // Review I2: ten of sixteen rows passed, understating the block size.
        let error = withdrawal_frames([&indices(0..10), &indices(16..32), &indices(32..48)])
            .expect_err("indices 10 to 15 are missing");
        assert!(
            matches!(error, XatuError::IncompleteWithdrawals { .. }),
            "{error}"
        );
    }

    #[test]
    fn withdrawal_rows_that_cannot_be_proven_complete_leave_their_chunk_incomplete() {
        for (name, slots) in [
            (
                "an index gap across an empty slot",
                [indices(0..16), Vec::new(), indices(20..36)],
            ),
            (
                "an empty first slot",
                [Vec::new(), indices(0..16), indices(16..32)],
            ),
            (
                "an empty last slot",
                [indices(0..16), indices(16..32), Vec::new()],
            ),
            ("no rows at all", [Vec::new(), Vec::new(), Vec::new()]),
        ] {
            let error = withdrawal_frames([&slots[0], &slots[1], &slots[2]]).expect_err(name);
            assert!(
                matches!(error, XatuError::IncompleteWithdrawals { .. }),
                "{name}: {error}"
            );
        }
    }

    #[test]
    fn blob_receipts_do_not_stand_in_for_logs() {
        let frames = dencun_blob_frames(&dencun_slot_withdrawals(dencun_withdrawals(16)))
            .expect("normalized blob frame");
        let requirement = |capabilities| leani_processor_api::DataRequirement {
            capabilities,
            log_fields: LogFieldSet::NONE,
            allow_filtered: true,
            filter: FilterScope {
                transaction_types: vec![3],
                ..FilterScope::default()
            },
            minimum_finality: Finality::Finalized,
        };
        let blobs = CapabilitySet::of(Capability::Header)
            .with(Capability::Transactions)
            .with(Capability::Receipts);
        assert_eq!(requirement(blobs).validate_frame(&frames[0]), Ok(()));
        // Task 4 carry-forward: receipts derive logs, but Xatu projects no
        // logs into its blob receipts.
        assert!(
            requirement(CapabilitySet::of(Capability::Logs))
                .validate_frame(&frames[0])
                .is_err(),
            "logs of blob transactions derived from receipts without logs"
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

    #[test]
    fn truncated_withdrawals_at_the_edges_of_a_one_block_chunk_stay_incomplete() {
        let size = |frames: Vec<BlockFrame>| {
            frames[0]
                .header
                .as_present()
                .expect("header")
                .size_bytes
                .expect("size")
        };
        assert_eq!(
            size(
                dencun_blob_frames(&dencun_slot_withdrawals(dencun_withdrawals(16)))
                    .expect("all sixteen rows")
            ),
            34_975
        );
        // External review F3: without its last row the block measured 34,939
        // bytes, and without its first 34,940; both passed.
        for (name, rows) in [
            ("the last row missing", dencun_withdrawals(15)),
            (
                "the first row missing",
                dencun_withdrawals(16).into_iter().skip(1).collect(),
            ),
        ] {
            let result = dencun_blob_frames(&dencun_slot_withdrawals(rows));
            assert!(
                matches!(result, Err(XatuError::IncompleteWithdrawals { .. })),
                "{name}: {:?}",
                result.map(size)
            );
        }
    }

    #[test]
    fn neighbouring_slots_prove_the_withdrawals_at_the_chunk_edges() {
        // External review F3: a block without withdrawals at either edge of a
        // chunk failed it, whatever the slots around the chunk showed.
        for (name, before, slots, after) in [
            (
                "an empty first slot",
                Some(99),
                [Vec::new(), indices(100..116), indices(116..132)],
                None,
            ),
            (
                "an empty last slot",
                None,
                [indices(100..116), indices(116..132), Vec::new()],
                Some(132),
            ),
            (
                "no withdrawals at all",
                Some(99),
                [Vec::new(), Vec::new(), Vec::new()],
                Some(100),
            ),
            (
                "short payloads at both edges",
                Some(99),
                [indices(100..110), indices(110..126), indices(126..130)],
                Some(130),
            ),
        ] {
            let frames =
                withdrawal_frames_between(before, [&slots[0], &slots[1], &slots[2]], after)
                    .unwrap_or_else(|error| panic!("{name}: {error}"));
            assert_eq!(frames.len(), 3, "{name}");
        }
    }

    #[test]
    fn chunk_edges_without_neighbouring_evidence_stay_incomplete() {
        for (name, before, slots, after) in [
            (
                "an empty first slot after an index gap",
                Some(98),
                [Vec::new(), indices(100..116), indices(116..132)],
                None,
            ),
            (
                "an empty last slot before an index gap",
                None,
                [indices(100..116), indices(116..132), Vec::new()],
                Some(133),
            ),
            (
                "a short first slot without an earlier slot",
                None,
                [indices(101..116), indices(116..132), indices(132..148)],
                None,
            ),
            (
                "a short last slot without a later slot",
                None,
                [indices(100..116), indices(116..132), indices(132..147)],
                None,
            ),
            (
                "a short last slot before an index gap",
                None,
                [indices(100..116), indices(116..132), indices(132..147)],
                Some(148),
            ),
        ] {
            let error = withdrawal_frames_between(before, [&slots[0], &slots[1], &slots[2]], after)
                .expect_err(name);
            assert!(
                matches!(error, XatuError::IncompleteWithdrawals { .. }),
                "{name}: {error}"
            );
        }
    }

    #[test]
    fn withdrawal_parsing_keeps_the_nearest_slot_on_each_side_of_the_chunk() {
        let rows: [(u64, u64); 9] = [
            (5, 1),
            (8, 2),
            (9, 3),
            (9, 4),
            (10, 5),
            (11, 6),
            (12, 7),
            (14, 8),
            (3, 0),
        ];
        let numbers = |values: Vec<u64>| Arc::new(UInt64Array::from(values)) as ArrayRef;
        let fixed = |width: usize| {
            Arc::new(
                FixedSizeBinaryArray::try_from_iter(rows.iter().map(|_| {
                    let mut value = vec![0_u8; width];
                    value[0] = 1;
                    value
                }))
                .expect("fixed-width column"),
            ) as ArrayRef
        };
        let batch = RecordBatch::try_from_iter([
            (
                "slot",
                numbers(rows.iter().map(|(slot, _)| *slot).collect()),
            ),
            (
                "withdrawal_index",
                numbers(rows.iter().map(|(_, index)| *index).collect()),
            ),
            (
                "withdrawal_validator_index",
                numbers(rows.iter().map(|(_, index)| *index).collect()),
            ),
            ("withdrawal_address", fixed(20)),
            ("withdrawal_amount", fixed(8)),
        ])
        .expect("batch");
        let mut output = BTreeMap::new();
        parse_withdrawals(&batch, &BTreeSet::from([10, 11]), &mut output).expect("rows");
        let kept = output
            .iter()
            .map(|(slot, rows)| (*slot, rows.iter().map(|row| row.index).collect()))
            .collect::<Vec<(u64, Vec<u64>)>>();
        // Review 2: rows outside the chunk were dropped, leaving its edges
        // without evidence.
        assert_eq!(
            kept,
            [(9, vec![3, 4]), (10, vec![5]), (11, vec![6]), (12, vec![7])]
        );
    }

    #[tokio::test]
    async fn range_reads_charge_every_requested_byte_to_the_input_budget() {
        let store = Arc::new(RecordingStore::default());
        let padding = vec![0xab; 128 * 1024];
        let object = stored_object(
            &store,
            parquet_object(vec![
                (
                    "block_number",
                    Arc::new(UInt64Array::from(vec![7])) as ArrayRef,
                ),
                (
                    "padding",
                    Arc::new(BinaryArray::from(vec![padding.as_slice()])) as ArrayRef,
                ),
                ("gas_used", Arc::new(UInt64Array::from(vec![8])) as ArrayRef),
            ]),
        )
        .await;
        let mut budget = test_budget(4);
        budget.max_input_bytes = 1_024;
        let mut used = 0;
        let metrics = read_projected(
            Arc::clone(&store) as Arc<dyn ObjectStore>,
            &object,
            &["block_number", "gas_used"],
            budget,
            8_192,
            &mut used,
            &CancellationToken::new(),
            rows,
        )
        .await
        .expect("the footer and the two columns fit the budget");
        let requested =
            u64::try_from(store.requested_bytes.load(Ordering::SeqCst)).expect("requested bytes");
        // External review F4: the 128 KiB between the columns were merged into
        // one request of 131,808 bytes under a 1,024-byte budget, and 711
        // bytes were reported fetched.
        assert!(
            requested <= 1_024,
            "{requested} bytes requested under a 1,024-byte budget"
        );
        assert_eq!(metrics.fetched_bytes, requested);
        assert_eq!(used, requested);
        assert!(metrics.projected_compressed_bytes < requested);
    }

    #[tokio::test]
    async fn a_row_group_must_fit_the_resident_budget() {
        let store = Arc::new(RecordingStore::default());
        let numbers = (0..2_048_u64).collect::<Vec<_>>();
        let object = stored_object(
            &store,
            parquet_object(vec![
                (
                    "block_number",
                    Arc::new(UInt64Array::from(numbers.clone())) as ArrayRef,
                ),
                ("gas_used", Arc::new(UInt64Array::from(numbers)) as ArrayRef),
            ]),
        )
        .await;
        let mut budget = test_budget(4);
        budget.max_resident_bytes = 1_024;
        let error = read_projected(
            Arc::clone(&store) as Arc<dyn ObjectStore>,
            &object,
            &["block_number", "gas_used"],
            budget,
            8_192,
            &mut 0,
            &CancellationToken::new(),
            rows,
        )
        .await
        .expect_err("the row group's columns exceed what one read may hold");
        // Review I1: a row group was bounded only by what the open may
        // acquire in total.
        assert!(
            matches!(
                error,
                XatuError::Budget {
                    resource: "resident_bytes",
                    limit: 1_024,
                    ..
                }
            ),
            "{error}"
        );
    }

    #[tokio::test]
    async fn row_groups_are_held_one_at_a_time() {
        let store = Arc::new(RecordingStore::default());
        let numbers = (0..2_048_u64).collect::<Vec<_>>();
        let object = stored_object(
            &store,
            parquet_object_in_row_groups(
                vec![
                    (
                        "block_number",
                        Arc::new(UInt64Array::from(numbers.clone())) as ArrayRef,
                    ),
                    ("gas_used", Arc::new(UInt64Array::from(numbers)) as ArrayRef),
                ],
                1_024,
            ),
        )
        .await;
        let read = |budget| {
            let store = Arc::clone(&store) as Arc<dyn ObjectStore>;
            let object = object.clone();
            async move {
                read_projected(
                    store,
                    &object,
                    &["block_number", "gas_used"],
                    budget,
                    8_192,
                    &mut 0,
                    &CancellationToken::new(),
                    rows,
                )
                .await
            }
        };
        let whole = read(test_budget(4)).await.expect("read");
        // A batch never spans row groups.
        assert_eq!(whole.batches, 2);
        // Review 2 A: room for the larger row group, less than both.
        let mut budget = test_budget(4);
        budget.max_resident_bytes = whole.projected_compressed_bytes - 1;
        let held = read(budget)
            .await
            .expect("each row group fits what one read may hold");
        assert_eq!(held.rows_scanned, 2_048);
    }

    #[tokio::test]
    async fn footer_reads_count_toward_the_input_budget() {
        let store = Arc::new(RecordingStore::default());
        let object = stored_object(
            &store,
            parquet_object(vec![(
                "block_number",
                Arc::new(UInt64Array::from(vec![7])) as ArrayRef,
            )]),
        )
        .await;
        let mut budget = test_budget(4);
        budget.max_input_bytes = 64;
        let mut used = 0;
        let error = read_projected(
            Arc::clone(&store) as Arc<dyn ObjectStore>,
            &object,
            &["block_number"],
            budget,
            8_192,
            &mut used,
            &CancellationToken::new(),
            rows,
        )
        .await
        .expect_err("the footer alone exceeds the budget");
        // External review F4: footers were read before, and outside, the
        // budget check.
        assert!(
            matches!(
                error,
                XatuError::Budget {
                    resource: "input_bytes",
                    limit: 64,
                    ..
                }
            ),
            "{error}"
        );
        assert!(store.requested_bytes.load(Ordering::SeqCst) <= 64);
    }

    /// The source budget's counters, each tripped where the projection
    /// enforces it: bytes acquired from the object store, bytes of each
    /// normalized frame, and frames emitted.
    #[tokio::test]
    async fn budget_contract_names_each_exceeded_counter() {
        let store = Arc::new(RecordingStore::default());
        let object = stored_object(
            &store,
            parquet_object(vec![(
                "block_number",
                Arc::new(UInt64Array::from(vec![7])) as ArrayRef,
            )]),
        )
        .await;
        let mut acquisition = test_budget(4);
        acquisition.max_input_bytes = 64;
        let acquired = read_projected(
            Arc::clone(&store) as Arc<dyn ObjectStore>,
            &object,
            &["block_number"],
            acquisition,
            8_192,
            &mut 0,
            &CancellationToken::new(),
            rows,
        )
        .await
        .map(|_| ());
        let blob_frames = dencun_blob_frames(&dencun_slot_withdrawals(dencun_withdrawals(16)))
            .expect("blob frame");
        let mut frame_limited = test_budget(1);
        frame_limited.max_frame_bytes = blob_frames[0].estimated_heap_bytes() - 1;
        let frames = withdrawal_frames([&indices(0..16), &indices(16..32), &indices(32..48)])
            .expect("frames");
        let mut emitted = test_budget(1);
        emitted.max_frames = 2;
        assert_eq!(validate_frame_budget(&blob_frames, test_budget(1)), Ok(()));
        assert_eq!(validate_frame_budget(&frames, test_budget(1)), Ok(()));
        for (counter, outcome, expected) in [
            ("acquired", acquired, "input_bytes"),
            (
                "frame",
                validate_frame_budget(&blob_frames, frame_limited),
                "frame_bytes",
            ),
            ("emitted", validate_frame_budget(&frames, emitted), "frames"),
        ] {
            match outcome {
                Err(XatuError::Budget {
                    resource,
                    actual,
                    limit,
                }) => {
                    assert_eq!(resource, expected, "{counter}");
                    assert!(actual > limit, "{counter}: {actual} <= {limit}");
                }
                other => panic!("{counter}: {other:?}"),
            }
        }
    }
}
