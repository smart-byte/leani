//! Projected, block-filtered reads of one day's table files, decoded into
//! rows.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use alloy_primitives::{Address, B256, Bloom, Bytes};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Decimal128Array, Int32Array, Int64Array, LargeListArray,
    LargeStringArray, ListArray, RecordBatch, StringArray, StringViewArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt64Array,
};
use arrow_schema::{ArrowError, DataType, TimeUnit};
use bytes::Bytes as Chunk;
use futures::{FutureExt, StreamExt, TryStreamExt, future::BoxFuture, stream};
use leani_chain::DatasetLayout;
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use parquet::{
    arrow::{
        ProjectionMask,
        arrow_reader::{ArrowPredicateFn, ArrowReaderOptions, RowFilter},
        async_reader::{AsyncFileReader, ParquetRecordBatchStreamBuilder},
    },
    errors::ParquetError,
    file::metadata::{ParquetMetaData, ParquetMetaDataReader},
};
use tokio_util::sync::CancellationToken;

use crate::{
    AwsError,
    layout::{Column, Table},
};

/// Bytes one chunk may download, or hold decoded: each object is charged
/// from its metadata before any of its columns is requested.
#[derive(Debug)]
pub(crate) struct InputBudget {
    resource: &'static str,
    limit: u64,
    used: AtomicU64,
}

impl InputBudget {
    pub(crate) fn new(resource: &'static str, limit: u64) -> Self {
        Self {
            resource,
            limit,
            used: AtomicU64::new(0),
        }
    }

    fn charge(&self, bytes: u64) -> Result<(), AwsError> {
        self.used
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                used.checked_add(bytes).filter(|total| *total <= self.limit)
            })
            .map(|_| ())
            .map_err(|used| AwsError::Budget {
                resource: self.resource,
                observed: used.saturating_add(bytes),
                limit: self.limit,
            })
    }
}

/// What one table read selects.
pub(crate) struct TableRead<'a> {
    pub store: &'a Arc<dyn ObjectStore>,
    pub layout: DatasetLayout,
    pub table: Table,
    pub objects: &'a [(String, u64)],
    pub keep: Arc<dyn Fn(u64) -> bool + Send + Sync>,
    pub batch_rows: usize,
    pub concurrency: usize,
    pub budget: &'a InputBudget,
    pub resident: &'a InputBudget,
    /// Blocks `keep` accepts and blocks in the day: rows are unsorted, so a
    /// read keeps about that share of each object's decoded bytes.
    pub share: (u64, u64),
    pub cancellation: &'a CancellationToken,
}

/// The rows of `read.table` whose block `read.keep` accepts, with only the
/// columns the layout names.
pub(crate) async fn read_table(read: TableRead<'_>) -> Result<Vec<RecordBatch>, AwsError> {
    // Futures built up front: a stream closure borrowing `read` is not
    // general enough for the `Send` bound `async_trait` puts on `open`.
    let reads = read
        .objects
        .iter()
        .map(|(key, size)| read_object(&read, key, *size))
        .collect::<Vec<_>>();
    let batches = stream::iter(reads)
        .buffer_unordered(read.concurrency.max(1))
        .try_collect::<Vec<_>>()
        .await?;
    Ok(batches.into_iter().flatten().collect())
}

async fn read_object(
    read: &TableRead<'_>,
    key: &str,
    size: u64,
) -> Result<Vec<RecordBatch>, AwsError> {
    let parquet =
        |error: parquet::errors::ParquetError| AwsError::Parquet(format!("{key}: {error}"));
    let reader = StoreReader::new(Arc::clone(read.store), key, size);
    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .map_err(parquet)?;
    let schema = builder.parquet_schema();
    let roots = schema.root_schema().get_fields();
    let root_of = |name: &str| roots.iter().position(|field| field.name() == name);
    let selected = Column::of(read.table)
        .iter()
        .filter_map(|column| column.name(read.layout, read.table))
        .filter_map(root_of)
        .collect::<Vec<_>>();
    let number = Column::Number
        .name(read.layout, read.table)
        .and_then(root_of)
        .ok_or_else(|| AwsError::MissingColumn {
            table: read.table.label(),
            column: "block number".to_owned(),
        })?;
    let (compressed, uncompressed) = builder
        .metadata()
        .row_groups()
        .iter()
        .flat_map(|row_group| row_group.columns().iter().enumerate())
        .filter(|(leaf, _)| selected.contains(&schema.get_column_root_idx(*leaf)))
        .fold((0_u64, 0_u64), |(compressed, uncompressed), (_, column)| {
            (
                compressed.saturating_add(u64::try_from(column.compressed_size()).unwrap_or(0)),
                uncompressed.saturating_add(u64::try_from(column.uncompressed_size()).unwrap_or(0)),
            )
        });
    let (kept, day) = read.share;
    let decoded = u128::from(uncompressed) * u128::from(kept) / u128::from(day.max(1));
    read.resident
        .charge(u64::try_from(decoded).unwrap_or(u64::MAX))?;
    read.budget.charge(compressed)?;
    let keep = Arc::clone(&read.keep);
    let predicate = ArrowPredicateFn::new(
        ProjectionMask::roots(schema, [number]),
        move |batch: RecordBatch| -> Result<BooleanArray, ArrowError> {
            let numbers = batch.column(0);
            Ok((0..numbers.len())
                .map(|row| {
                    Some(
                        uint(numbers, row).is_some_and(|n| u64::try_from(n).is_ok_and(|n| keep(n))),
                    )
                })
                .collect())
        },
    );
    let mask = ProjectionMask::roots(schema, selected);
    let mut rows = builder
        .with_projection(mask)
        .with_row_filter(RowFilter::new(vec![Box::new(predicate)]))
        .with_batch_size(read.batch_rows)
        .build()
        .map_err(parquet)?;
    let mut batches = Vec::new();
    loop {
        let next = tokio::select! {
            () = read.cancellation.cancelled() => return Err(AwsError::Cancelled),
            next = rows.next() => next,
        };
        match next {
            Some(batch) => batches.push(batch.map_err(parquet)?),
            None => return Ok(batches),
        }
    }
}

/// Range reads of one object, sized from its listing.
#[derive(Clone, Debug)]
pub(crate) struct StoreReader {
    store: Arc<dyn ObjectStore>,
    path: Path,
    size: u64,
}

impl StoreReader {
    pub(crate) fn new(store: Arc<dyn ObjectStore>, key: &str, size: u64) -> Self {
        Self {
            store,
            path: Path::from(key),
            size,
        }
    }
}

fn store_error(error: object_store::Error) -> ParquetError {
    ParquetError::External(Box::new(error))
}

impl AsyncFileReader for StoreReader {
    fn get_bytes(
        &mut self,
        range: std::ops::Range<u64>,
    ) -> BoxFuture<'_, parquet::errors::Result<Chunk>> {
        async move {
            self.store
                .get_range(&self.path, range)
                .await
                .map_err(store_error)
        }
        .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<std::ops::Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Chunk>>> {
        async move {
            self.store
                .get_ranges(&self.path, &ranges)
                .await
                .map_err(store_error)
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        async move {
            let size = self.size;
            let metadata = ParquetMetaDataReader::new()
                .with_arrow_reader_options(options)
                .load_and_finish(self, size)
                .await?;
            Ok(Arc::new(metadata))
        }
        .boxed()
    }
}

/// A block row. `header` holds every consensus field when the layout
/// carries them all.
#[derive(Clone, Debug)]
pub(crate) struct BlockRow {
    pub number: u64,
    pub hash: B256,
    pub parent_hash: B256,
    pub timestamp: u64,
    pub receipts_root: B256,
    pub transactions_root: B256,
    pub bloom: Bloom,
    pub gas_limit: u64,
    pub gas_used: u64,
    pub base_fee: Option<u64>,
    pub transaction_count: Option<u32>,
    pub size: Option<u64>,
    pub blob_gas_used: Option<u64>,
    pub excess_blob_gas: Option<u64>,
    pub withdrawals_root: Option<B256>,
    pub header: Option<alloy_consensus::Header>,
}

#[derive(Clone, Debug)]
pub(crate) struct ReceiptRow {
    pub number: u64,
    pub index: u32,
    pub hash: B256,
    pub transaction_type: Option<u8>,
    /// `None` before Byzantium, whose receipts commit to a state root.
    pub success: Option<bool>,
    pub cumulative_gas: u64,
    pub gas_used: Option<u64>,
    pub effective_gas_price: Option<u128>,
    pub deposit_nonce: Option<u64>,
    pub deposit_version: Option<u64>,
}

#[derive(Clone, Debug)]
pub(crate) struct LogRow {
    pub number: u64,
    pub transaction_index: u32,
    pub log_index: u32,
    pub address: Address,
    pub topics: Vec<B256>,
    pub data: Bytes,
}

/// Column access by layout name for one table's batches.
struct Rows<'a> {
    layout: DatasetLayout,
    table: Table,
    batch: &'a RecordBatch,
}

impl<'a> Rows<'a> {
    fn column(&self, column: Column) -> Option<&'a ArrayRef> {
        self.batch
            .column_by_name(column.name(self.layout, self.table)?)
    }

    fn malformed(&self, row: usize, what: &str) -> AwsError {
        AwsError::Malformed {
            table: self.table.label(),
            detail: format!("row {row}: {what}"),
        }
    }

    fn uint(&self, column: Column, row: usize) -> Option<u128> {
        self.column(column).and_then(|array| uint(array, row))
    }

    fn u64(&self, column: Column, row: usize) -> Option<u64> {
        self.uint(column, row)
            .and_then(|value| u64::try_from(value).ok())
    }

    fn required_u64(&self, column: Column, row: usize) -> Result<u64, AwsError> {
        self.u64(column, row)
            .ok_or_else(|| self.malformed(row, &format!("no {column:?}")))
    }

    fn text(&self, column: Column, row: usize) -> Option<&'a str> {
        self.column(column).and_then(|array| text(array, row))
    }

    fn parsed<T: std::str::FromStr>(
        &self,
        column: Column,
        row: usize,
    ) -> Result<Option<T>, AwsError> {
        self.text(column, row)
            .map(|text| {
                text.parse()
                    .map_err(|_| self.malformed(row, &format!("{column:?} {text:?}")))
            })
            .transpose()
    }

    fn required<T: std::str::FromStr>(&self, column: Column, row: usize) -> Result<T, AwsError> {
        self.parsed(column, row)?
            .ok_or_else(|| self.malformed(row, &format!("no {column:?}")))
    }
}

pub(crate) fn blocks(
    layout: DatasetLayout,
    batches: &[RecordBatch],
) -> Result<Vec<BlockRow>, AwsError> {
    let mut blocks = Vec::new();
    for batch in batches {
        let rows = Rows {
            layout,
            table: Table::Blocks,
            batch,
        };
        for row in 0..batch.num_rows() {
            let number = rows.required_u64(Column::Number, row)?;
            let timestamp = rows
                .column(Column::Timestamp)
                .and_then(|array| seconds(array, row))
                .ok_or_else(|| rows.malformed(row, "no timestamp"))?;
            let mut block = BlockRow {
                number,
                hash: rows.required(Column::Hash, row)?,
                parent_hash: rows.required(Column::ParentHash, row)?,
                timestamp,
                receipts_root: rows.required(Column::ReceiptsRoot, row)?,
                transactions_root: rows.required(Column::TransactionsRoot, row)?,
                bloom: rows.required(Column::Bloom, row)?,
                gas_limit: rows.required_u64(Column::GasLimit, row)?,
                gas_used: rows.required_u64(Column::GasUsed, row)?,
                base_fee: rows.u64(Column::BaseFee, row),
                transaction_count: rows
                    .u64(Column::TransactionCount, row)
                    .and_then(|count| u32::try_from(count).ok()),
                size: rows.u64(Column::Size, row),
                blob_gas_used: rows.u64(Column::BlobGasUsed, row),
                excess_blob_gas: rows.u64(Column::ExcessBlobGas, row),
                withdrawals_root: rows.parsed(Column::WithdrawalsRoot, row)?,
                header: None,
            };
            // Without a mix hash the layout cannot rebuild the header.
            if let Some(mix_hash) = rows.parsed::<B256>(Column::MixHash, row)? {
                block.header = Some(alloy_consensus::Header {
                    parent_hash: block.parent_hash,
                    ommers_hash: rows.required(Column::Ommers, row)?,
                    beneficiary: rows.required(Column::Miner, row)?,
                    state_root: rows.required(Column::StateRoot, row)?,
                    transactions_root: block.transactions_root,
                    receipts_root: block.receipts_root,
                    logs_bloom: block.bloom,
                    difficulty: alloy_primitives::U256::from(
                        rows.uint(Column::Difficulty, row).unwrap_or_default(),
                    ),
                    number,
                    gas_limit: block.gas_limit,
                    gas_used: block.gas_used,
                    timestamp,
                    extra_data: rows.required(Column::ExtraData, row)?,
                    mix_hash,
                    nonce: alloy_primitives::B64::from(
                        rows.u64(Column::Nonce, row).unwrap_or_default(),
                    ),
                    base_fee_per_gas: block.base_fee,
                    withdrawals_root: block.withdrawals_root,
                    blob_gas_used: block.blob_gas_used,
                    excess_blob_gas: block.excess_blob_gas,
                    parent_beacon_block_root: rows.parsed(Column::BeaconRoot, row)?,
                    ..Default::default()
                });
            }
            blocks.push(block);
        }
    }
    Ok(blocks)
}

pub(crate) fn receipts(
    layout: DatasetLayout,
    batches: &[RecordBatch],
) -> Result<Vec<ReceiptRow>, AwsError> {
    let mut receipts = Vec::new();
    for batch in batches {
        let rows = Rows {
            layout,
            table: Table::Receipts,
            batch,
        };
        for row in 0..batch.num_rows() {
            receipts.push(ReceiptRow {
                number: rows.required_u64(Column::Number, row)?,
                index: index(&rows, Column::TransactionIndex, row)?,
                hash: rows.required(Column::TransactionHash, row)?,
                transaction_type: rows
                    .u64(Column::TransactionType, row)
                    .and_then(|kind| u8::try_from(kind).ok()),
                success: rows.u64(Column::Status, row).map(|status| status == 1),
                cumulative_gas: rows.required_u64(Column::CumulativeGas, row)?,
                gas_used: rows.u64(Column::GasUsed, row),
                effective_gas_price: rows.uint(Column::EffectiveGasPrice, row),
                deposit_nonce: rows.u64(Column::DepositNonce, row),
                deposit_version: rows.u64(Column::DepositVersion, row),
            });
        }
    }
    Ok(receipts)
}

/// `(block, transaction index, type)` of every transaction row.
pub(crate) fn transaction_types(
    layout: DatasetLayout,
    batches: &[RecordBatch],
) -> Result<Vec<(u64, u32, u8)>, AwsError> {
    let mut types = Vec::new();
    for batch in batches {
        // Both transaction tables share these columns.
        let rows = Rows {
            layout,
            table: Table::TransactionTypes,
            batch,
        };
        for row in 0..batch.num_rows() {
            let kind = rows.required_u64(Column::TransactionType, row)?;
            types.push((
                rows.required_u64(Column::Number, row)?,
                index(&rows, Column::TransactionIndex, row)?,
                u8::try_from(kind).map_err(|_| rows.malformed(row, "transaction type over 255"))?,
            ));
        }
    }
    Ok(types)
}

pub(crate) fn logs(
    layout: DatasetLayout,
    batches: &[RecordBatch],
) -> Result<Vec<LogRow>, AwsError> {
    let mut logs = Vec::new();
    for batch in batches {
        let rows = Rows {
            layout,
            table: Table::Logs,
            batch,
        };
        for row in 0..batch.num_rows() {
            let topics = match rows.column(Column::Topics) {
                Some(list) => list_texts(list, row),
                None => (0..4)
                    .map_while(|position| rows.text(Column::Topic(position), row))
                    .filter(|topic| !topic.is_empty())
                    .map(str::to_owned)
                    .collect(),
            }
            .into_iter()
            .map(|topic| {
                topic
                    .parse::<B256>()
                    .map_err(|_| rows.malformed(row, &format!("topic {topic:?}")))
            })
            .collect::<Result<Vec<_>, _>>()?;
            logs.push(LogRow {
                number: rows.required_u64(Column::Number, row)?,
                transaction_index: index(&rows, Column::TransactionIndex, row)?,
                log_index: index(&rows, Column::LogIndex, row)?,
                address: rows.required(Column::Address, row)?,
                topics,
                data: rows.parsed(Column::Data, row)?.unwrap_or_default(),
            });
        }
    }
    Ok(logs)
}

fn index(rows: &Rows<'_>, column: Column, row: usize) -> Result<u32, AwsError> {
    u32::try_from(rows.required_u64(column, row)?)
        .map_err(|_| rows.malformed(row, &format!("{column:?} over u32")))
}

/// An unsigned integer cell: `INT64`, or the `DECIMAL(38, 0)` `SonarX` uses.
fn uint(array: &ArrayRef, row: usize) -> Option<u128> {
    if array.is_null(row) {
        return None;
    }
    let any = array.as_any();
    match array.data_type() {
        DataType::Int64 => u128::try_from(any.downcast_ref::<Int64Array>()?.value(row)).ok(),
        DataType::Int32 => u128::try_from(any.downcast_ref::<Int32Array>()?.value(row)).ok(),
        DataType::UInt64 => Some(u128::from(any.downcast_ref::<UInt64Array>()?.value(row))),
        DataType::Decimal128(_, 0) => {
            u128::try_from(any.downcast_ref::<Decimal128Array>()?.value(row)).ok()
        }
        _ => None,
    }
}

fn seconds(array: &ArrayRef, row: usize) -> Option<u64> {
    if array.is_null(row) {
        return None;
    }
    let any = array.as_any();
    let (value, per_second) = match array.data_type() {
        DataType::Timestamp(TimeUnit::Second, _) => {
            (any.downcast_ref::<TimestampSecondArray>()?.value(row), 1)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => (
            any.downcast_ref::<TimestampMillisecondArray>()?.value(row),
            1_000,
        ),
        DataType::Timestamp(TimeUnit::Microsecond, _) => (
            any.downcast_ref::<TimestampMicrosecondArray>()?.value(row),
            1_000_000,
        ),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => (
            any.downcast_ref::<TimestampNanosecondArray>()?.value(row),
            1_000_000_000,
        ),
        _ => return uint(array, row).and_then(|value| u64::try_from(value).ok()),
    };
    u64::try_from(value / per_second).ok()
}

fn text(array: &ArrayRef, row: usize) -> Option<&str> {
    if array.is_null(row) {
        return None;
    }
    let any = array.as_any();
    match array.data_type() {
        DataType::Utf8 => Some(any.downcast_ref::<StringArray>()?.value(row)),
        DataType::LargeUtf8 => Some(any.downcast_ref::<LargeStringArray>()?.value(row)),
        DataType::Utf8View => Some(any.downcast_ref::<StringViewArray>()?.value(row)),
        _ => None,
    }
}

fn list_texts(array: &ArrayRef, row: usize) -> Vec<String> {
    if array.is_null(row) {
        return Vec::new();
    }
    let any = array.as_any();
    let values = match array.data_type() {
        DataType::List(_) => any.downcast_ref::<ListArray>().map(|list| list.value(row)),
        DataType::LargeList(_) => any
            .downcast_ref::<LargeListArray>()
            .map(|list| list.value(row)),
        _ => None,
    };
    let Some(values) = values else {
        return Vec::new();
    };
    (0..values.len())
        .filter_map(|item| text(&values, item).map(str::to_owned))
        .collect()
}
