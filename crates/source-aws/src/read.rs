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
use leani_primitives::BlockRange;
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use parquet::{
    arrow::{
        ProjectionMask,
        arrow_reader::{
            ArrowPredicateFn, ArrowReaderOptions, RowFilter, RowSelection, RowSelector,
        },
        async_reader::{AsyncFileReader, ParquetRecordBatchStreamBuilder},
    },
    errors::ParquetError,
    file::{
        metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader},
        page_index::column_index::ColumnIndexMetaData,
        statistics::Statistics,
    },
};
use tokio_util::sync::CancellationToken;

use crate::{
    AwsError,
    cache::RangeCache,
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
    pub requests: &'a Arc<RangeRequests>,
    /// Where the ranges read are kept for the chunks that read them again.
    pub cache: Option<&'a Arc<RangeCache>>,
    pub budget: &'a InputBudget,
    pub resident: &'a InputBudget,
    /// Blocks `keep` accepts and blocks in the day: rows are unsorted, so a
    /// read keeps about that share of each object's decoded bytes.
    pub share: (u64, u64),
    /// The chunk's blocks: row groups and pages whose block-number
    /// statistics hold none of them are not fetched.
    pub blocks: BlockRange,
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
    let parquet = |error: parquet::errors::ParquetError| {
        read.requests
            .over_budget()
            .unwrap_or_else(|| AwsError::Parquet(format!("{key}: {error}")))
    };
    let reader = StoreReader::new(
        Arc::clone(read.store),
        key,
        size,
        Arc::clone(read.requests),
        read.cache.cloned(),
    );
    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional);
    let builder = ParquetRecordBatchStreamBuilder::new_with_options(reader, options)
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
    let number_leaf = (0..schema.num_columns())
        .find(|leaf| schema.get_column_root_idx(*leaf) == number)
        .unwrap_or(number);
    let metadata = builder.metadata();
    let candidates = candidate_rows(metadata, number_leaf, read.blocks);
    let projected = (0..schema.num_columns())
        .filter(|leaf| selected.contains(&schema.get_column_root_idx(*leaf)))
        .collect::<Vec<_>>();
    let (certain, uncompressed) = projected_bytes(metadata, &projected, &candidates);
    let (kept, day) = read.share;
    let decoded = u128::from(uncompressed) * u128::from(kept) / u128::from(day.max(1));
    read.resident
        .charge(u64::try_from(decoded).unwrap_or(u64::MAX))?;
    read.budget.charge(certain)?;
    let Some((row_groups, selection)) = row_selection(metadata, &candidates) else {
        return Ok(Vec::new());
    };
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
        .with_row_groups(row_groups)
        .with_row_selection(selection)
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

/// Rows of each row group that may hold a block of `blocks`, by the
/// block-number column's statistics: `None` for a row group that holds
/// none, and every row where its pages have no index.
fn candidate_rows(
    metadata: &ParquetMetaData,
    number: usize,
    blocks: BlockRange,
) -> Vec<Option<Vec<std::ops::Range<usize>>>> {
    let overlaps = |(first, last): (u64, u64)| first <= blocks.end().0 && last >= blocks.start().0;
    metadata
        .row_groups()
        .iter()
        .enumerate()
        .map(|(index, row_group)| {
            let rows = usize::try_from(row_group.num_rows()).unwrap_or(0);
            if row_group
                .column(number)
                .statistics()
                .and_then(block_bounds)
                .is_some_and(|bounds| !overlaps(bounds))
            {
                return None;
            }
            let pages = metadata.page_index_for_row_group(index);
            let (Some(column_index), Some(locations)) =
                (pages.column_index(number), pages.page_locations(number))
            else {
                return Some(std::iter::once(0..rows).collect());
            };
            let mut ranges: Vec<std::ops::Range<usize>> = Vec::new();
            for (page, location) in locations.iter().enumerate() {
                if column_index.is_null_page(page)
                    || page_bounds(column_index, page).is_some_and(|bounds| !overlaps(bounds))
                {
                    continue;
                }
                let start = usize::try_from(location.first_row_index).unwrap_or(0);
                let end = locations.get(page + 1).map_or(rows, |next| {
                    usize::try_from(next.first_row_index).unwrap_or(rows)
                });
                match ranges.last_mut() {
                    Some(last) if last.end == start => last.end = end,
                    _ => ranges.push(start..end),
                }
            }
            (!ranges.is_empty()).then_some(ranges)
        })
        .collect()
}

/// Bytes the `projected` leaf columns fetch for `candidates` whatever the
/// row filter keeps: whole column chunks without an offset index, which the
/// reader cannot read page by page. Their decoded size in every row group
/// comes second.
fn projected_bytes(
    metadata: &ParquetMetaData,
    projected: &[usize],
    candidates: &[Option<Vec<std::ops::Range<usize>>>],
) -> (u64, u64) {
    let mut certain = 0_u64;
    let mut uncompressed = 0_u64;
    for (index, row_group) in metadata.row_groups().iter().enumerate() {
        let pages = metadata.page_index_for_row_group(index);
        for &leaf in projected {
            let column = row_group.column(leaf);
            uncompressed =
                uncompressed.saturating_add(u64::try_from(column.uncompressed_size()).unwrap_or(0));
            if candidates[index].is_some() && pages.page_locations(leaf).is_none() {
                certain =
                    certain.saturating_add(u64::try_from(column.compressed_size()).unwrap_or(0));
            }
        }
    }
    (certain, uncompressed)
}

/// The row groups `candidates` reads and their rows as one selection, or
/// `None` when it reads none.
fn row_selection(
    metadata: &ParquetMetaData,
    candidates: &[Option<Vec<std::ops::Range<usize>>>],
) -> Option<(Vec<usize>, RowSelection)> {
    let mut row_groups = Vec::new();
    let mut selectors = Vec::<RowSelector>::new();
    for (index, (rows, row_group)) in candidates.iter().zip(metadata.row_groups()).enumerate() {
        if let Some(rows) = rows {
            row_groups.push(index);
            let total = usize::try_from(row_group.num_rows()).unwrap_or(0);
            selectors
                .extend(RowSelection::from_consecutive_ranges(rows.iter().cloned(), total).iter());
        }
    }
    (!row_groups.is_empty()).then(|| (row_groups, RowSelection::from(selectors)))
}

/// Block-number bounds of a column chunk: `INT64` statistics, or the
/// big-endian `DECIMAL(38, 0)` `SonarX` stores numbers as.
pub(crate) fn block_bounds(statistics: &Statistics) -> Option<(u64, u64)> {
    match statistics {
        Statistics::Int64(values) => Some((
            u64::try_from(*values.min_opt()?).ok()?,
            u64::try_from(*values.max_opt()?).ok()?,
        )),
        Statistics::FixedLenByteArray(values) => Some((
            decimal(values.min_bytes_opt()?)?,
            decimal(values.max_bytes_opt()?)?,
        )),
        _ => None,
    }
}

/// The same bounds of one page, from its column index.
fn page_bounds(index: &ColumnIndexMetaData, page: usize) -> Option<(u64, u64)> {
    match index {
        ColumnIndexMetaData::INT64(values) => Some((
            u64::try_from(*values.min_value(page)?).ok()?,
            u64::try_from(*values.max_value(page)?).ok()?,
        )),
        ColumnIndexMetaData::FIXED_LEN_BYTE_ARRAY(values) => Some((
            decimal(values.min_value(page)?)?,
            decimal(values.max_value(page)?)?,
        )),
        _ => None,
    }
}

fn decimal(bytes: &[u8]) -> Option<u64> {
    u64::try_from(i128::from_be_bytes(<[u8; 16]>::try_from(bytes).ok()?)).ok()
}

/// Longest range one GET reads. One connection to a distant bucket is slow,
/// so a longer read goes out as parts in parallel.
pub(crate) const PART_BYTES: u64 = 8 * 1_024 * 1_024;

/// The ranged GETs one chunk read shares: at most `limit` in flight, and
/// at most `input_bytes` downloaded.
#[derive(Debug)]
pub(crate) struct RangeRequests {
    permits: tokio::sync::Semaphore,
    reads: AtomicU64,
    bytes: AtomicU64,
    /// What the downloads would have reached with a range refused for
    /// passing `input_bytes`.
    refused: AtomicU64,
    input_bytes: u64,
}

impl RangeRequests {
    pub(crate) fn new(limit: usize, input_bytes: u64) -> Self {
        Self {
            permits: tokio::sync::Semaphore::new(limit.max(1)),
            reads: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            refused: AtomicU64::new(0),
            input_bytes,
        }
    }

    /// GETs answered so far, and the bytes they returned.
    pub(crate) fn transferred(&self) -> (u64, u64) {
        (
            self.reads.load(Ordering::Relaxed),
            self.bytes.load(Ordering::Relaxed),
        )
    }

    /// The budget error once the downloads passed `input_bytes`, or a range
    /// was refused for passing it.
    fn over_budget(&self) -> Option<AwsError> {
        let observed = self
            .bytes
            .load(Ordering::Relaxed)
            .max(self.refused.load(Ordering::Relaxed));
        (observed > self.input_bytes).then_some(AwsError::Budget {
            resource: "input_bytes",
            observed,
            limit: self.input_bytes,
        })
    }
}

/// Range reads of one object, sized from its listing.
#[derive(Clone, Debug)]
pub(crate) struct StoreReader {
    store: Arc<dyn ObjectStore>,
    path: Path,
    size: u64,
    requests: Arc<RangeRequests>,
    cache: Option<Arc<RangeCache>>,
}

impl StoreReader {
    pub(crate) fn new(
        store: Arc<dyn ObjectStore>,
        key: &str,
        size: u64,
        requests: Arc<RangeRequests>,
        cache: Option<Arc<RangeCache>>,
    ) -> Self {
        Self {
            store,
            path: Path::from(key),
            size,
            requests,
            cache,
        }
    }

    /// `range`, in parts of at most [`PART_BYTES`] fetched together. A range
    /// the budget has no room for is refused before any part is fetched:
    /// the parts are requested at once, and a file names its own sizes.
    async fn fetch(&self, range: std::ops::Range<u64>) -> object_store::Result<Chunk> {
        let reached = self
            .requests
            .bytes
            .load(Ordering::Relaxed)
            .saturating_add(range.end.saturating_sub(range.start));
        if reached > self.requests.input_bytes {
            self.requests.refused.fetch_max(reached, Ordering::Relaxed);
            return Err(object_store::Error::Generic {
                store: "aws-public",
                source: "the chunk's input budget has no room for the range".into(),
            });
        }
        let step = usize::try_from(PART_BYTES).unwrap_or(usize::MAX);
        let parts = (range.start..range.end)
            .step_by(step)
            .map(|start| start..start.saturating_add(PART_BYTES).min(range.end))
            .collect::<Vec<_>>();
        let count = parts.len().max(1);
        let fetched = stream::iter(parts)
            .map(|part| async move {
                let _permit = self.requests.permits.acquire().await.ok();
                let over = || object_store::Error::Generic {
                    store: "aws-public",
                    source: "the chunk's input budget is spent".into(),
                };
                if self.requests.over_budget().is_some() {
                    return Err(over());
                }
                let bytes = self.store.get_range(&self.path, part).await?;
                self.requests.reads.fetch_add(1, Ordering::Relaxed);
                self.requests
                    .bytes
                    .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                if self.requests.over_budget().is_some() {
                    return Err(over());
                }
                Ok(bytes)
            })
            .buffered(count)
            .try_collect::<Vec<_>>()
            .await?;
        if let [whole] = fetched.as_slice() {
            return Ok(whole.clone());
        }
        let mut joined =
            bytes::BytesMut::with_capacity(fetched.iter().map(bytes::Bytes::len).sum());
        for part in &fetched {
            joined.extend_from_slice(part);
        }
        Ok(joined.freeze())
    }

    async fn fetch_all(&self, ranges: &[std::ops::Range<u64>]) -> object_store::Result<Vec<Chunk>> {
        object_store::coalesce_ranges(
            ranges,
            |range| self.fetch(range),
            object_store::OBJECT_STORE_COALESCE_DEFAULT,
        )
        .await
    }

    /// `ranges`, through the cache when there is one.
    async fn read(&self, ranges: Vec<std::ops::Range<u64>>) -> object_store::Result<Vec<Chunk>> {
        match &self.cache {
            Some(cache) => {
                cache
                    .read(self.path.as_ref(), self.size, ranges, |ranges| async move {
                        self.fetch_all(&ranges).await
                    })
                    .await
            }
            None => self.fetch_all(&ranges).await,
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
            let mut bytes = self.read(vec![range]).await.map_err(store_error)?;
            Ok(bytes.pop().unwrap_or_default())
        }
        .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<std::ops::Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Chunk>>> {
        async move { self.read(ranges).await.map_err(store_error) }.boxed()
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
    pub fee_recipient: alloy_primitives::Address,
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
                fee_recipient: rows.required(Column::Miner, row)?,
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
                    beneficiary: block.fee_recipient,
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

#[cfg(test)]
mod tests {
    use std::{
        sync::{Mutex, atomic::AtomicUsize},
        time::Duration,
    };

    use futures::stream::BoxStream;
    use leani_primitives::BlockNumber;
    use object_store::{
        CopyOptions, GetOptions, GetRange, GetResult, ListResult, MultipartUpload, ObjectMeta,
        PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory,
    };
    use parquet::{
        arrow::ArrowWriter,
        file::properties::{EnabledStatistics, WriterProperties},
    };

    use super::*;

    /// An in-memory store that records each ranged read and how many are in
    /// flight at once.
    #[derive(Debug, Default)]
    struct RecordingStore {
        inner: InMemory,
        in_flight: AtomicUsize,
        peak_in_flight: AtomicUsize,
        ranges: Mutex<Vec<std::ops::Range<u64>>>,
    }

    impl RecordingStore {
        fn requested_bytes(&self) -> u64 {
            self.ranges
                .lock()
                .unwrap()
                .iter()
                .map(|range| range.end - range.start)
                .sum()
        }
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
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            if let Some(GetRange::Bounded(range)) = &options.range {
                self.ranges.lock().unwrap().push(range.clone());
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
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[tokio::test]
    async fn large_reads_go_out_as_parallel_parts_within_the_request_limit() {
        let store = Arc::new(RecordingStore::default());
        let size = 3 * PART_BYTES + 5;
        let data = (0..size).map(|byte| (byte % 251) as u8).collect::<Vec<_>>();
        store
            .inner
            .put(&Path::from("object"), data.clone().into())
            .await
            .unwrap();
        let mut reader = StoreReader::new(
            store.clone(),
            "object",
            size,
            Arc::new(RangeRequests::new(2, u64::MAX)),
            None,
        );
        let ranges = vec![0..2 * PART_BYTES + 3, 2 * PART_BYTES + 3..size];
        let chunks = reader.get_byte_ranges(ranges.clone()).await.unwrap();
        for (range, chunk) in ranges.iter().zip(&chunks) {
            assert_eq!(
                chunk.as_ref(),
                &data[usize::try_from(range.start).unwrap()..usize::try_from(range.end).unwrap()]
            );
        }
        let requested = store.ranges.lock().unwrap().clone();
        assert_eq!(requested.len(), 4, "{requested:?}");
        assert!(
            requested
                .iter()
                .all(|range| range.end - range.start <= PART_BYTES)
        );
        assert_eq!(store.peak_in_flight.load(Ordering::SeqCst), 2);
    }

    /// A file of `blocks` blocks with `rows` rows of 1,000 bytes each, in
    /// block order, in row groups of 2,000 rows and plain pages of 200, with
    /// `statistics` for each. Only page statistics come with a page index,
    /// as AWS's Ethereum export has none.
    fn clustered_file(blocks: u64, rows: usize, statistics: EnabledStatistics) -> Vec<u8> {
        let numbers = arrow_array::Int64Array::from_iter_values(
            (0..blocks).flat_map(|block| std::iter::repeat_n(i64::try_from(block).unwrap(), rows)),
        );
        let data =
            StringArray::from_iter_values((0..numbers.len()).map(|row| format!("{row:0>1000}")));
        let batch = RecordBatch::try_from_iter([
            ("block_number", Arc::new(numbers) as ArrayRef),
            ("data", Arc::new(data) as ArrayRef),
        ])
        .unwrap();
        let properties = WriterProperties::builder()
            .set_dictionary_enabled(false)
            .set_statistics_enabled(statistics)
            .set_offset_index_disabled(true)
            .set_max_row_group_row_count(Some(2_000))
            .set_data_page_row_count_limit(200)
            .set_write_batch_size(200)
            .build();
        let mut bytes = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut bytes, batch.schema(), Some(properties)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        bytes
    }

    async fn read_blocks(
        store: Arc<dyn ObjectStore>,
        size: u64,
        blocks: BlockRange,
    ) -> Vec<RecordBatch> {
        read_kept(store, size, blocks, blocks, u64::MAX, None)
            .await
            .unwrap()
    }

    /// The rows of `kept` blocks of a chunk of `blocks`, downloading at most
    /// `input` bytes.
    async fn read_kept(
        store: Arc<dyn ObjectStore>,
        size: u64,
        blocks: BlockRange,
        kept: BlockRange,
        input: u64,
        cache: Option<&Arc<RangeCache>>,
    ) -> Result<Vec<RecordBatch>, AwsError> {
        let budget = InputBudget::new("input_bytes", input);
        let resident = InputBudget::new("resident_bytes", u64::MAX);
        let requests = Arc::new(RangeRequests::new(4, input));
        let objects = [("file".to_owned(), size)];
        read_table(TableRead {
            store: &store,
            layout: DatasetLayout::AwsEthereumV1,
            table: Table::Logs,
            objects: &objects,
            keep: Arc::new(move |number| kept.contains(BlockNumber(number))),
            batch_rows: 1_024,
            concurrency: 1,
            requests: &requests,
            cache,
            budget: &budget,
            resident: &resident,
            share: (kept.len(), 1_000),
            blocks,
            cancellation: &CancellationToken::new(),
        })
        .await
    }

    async fn stored(file: Vec<u8>) -> (Arc<RecordingStore>, u64) {
        let size = u64::try_from(file.len()).unwrap();
        let store = Arc::new(RecordingStore::default());
        store
            .inner
            .put(&Path::from("file"), file.into())
            .await
            .unwrap();
        (store, size)
    }

    /// Blocks 150–249 span the first two of five row groups.
    fn chunk() -> BlockRange {
        BlockRange::new(BlockNumber(150), BlockNumber(249)).unwrap()
    }

    fn numbers(batches: &[RecordBatch]) -> Vec<u64> {
        batches
            .iter()
            .flat_map(|batch| {
                let numbers = batch.column_by_name("block_number").unwrap().clone();
                (0..batch.num_rows())
                    .map(move |row| u64::try_from(uint(&numbers, row).unwrap()).unwrap())
            })
            .collect()
    }

    #[tokio::test]
    async fn a_chunk_fetches_only_the_pages_holding_its_blocks() {
        let (store, size) = stored(clustered_file(1_000, 10, EnabledStatistics::Page)).await;
        let batches = read_blocks(store.clone(), size, chunk()).await;
        let numbers = numbers(&batches);
        assert_eq!(numbers.len(), 1_000);
        assert!(
            numbers
                .iter()
                .all(|number| chunk().contains(BlockNumber(*number)))
        );
        let fetched = store.requested_bytes();
        assert!(fetched < size / 4, "fetched {fetched} of {size} bytes");
    }

    #[tokio::test]
    async fn files_without_a_page_index_are_read_by_row_group() {
        let (store, size) = stored(clustered_file(1_000, 10, EnabledStatistics::Chunk)).await;
        let batches = read_blocks(store.clone(), size, chunk()).await;
        let numbers = numbers(&batches);
        assert_eq!(numbers.len(), 1_000);
        assert!(
            numbers
                .iter()
                .all(|number| chunk().contains(BlockNumber(*number)))
        );
        let fetched = store.requested_bytes();
        assert!(fetched < size / 2, "fetched {fetched} of {size} bytes");
    }

    #[tokio::test]
    async fn a_range_past_the_budget_is_refused_before_it_is_fetched() {
        let (store, size) = stored(clustered_file(1_000, 10, EnabledStatistics::Page)).await;
        let requests = Arc::new(RangeRequests::new(4, size / 4));
        let mut reader = StoreReader::new(store.clone(), "file", size, Arc::clone(&requests), None);
        assert!(reader.get_bytes(0..size / 2).await.is_err());
        assert_eq!(store.requested_bytes(), 0);
        assert!(matches!(
            requests.over_budget(),
            Some(AwsError::Budget {
                resource: "input_bytes",
                ..
            })
        ));
    }

    #[tokio::test]
    async fn the_input_budget_counts_what_a_read_downloads() {
        let (store, size) = stored(clustered_file(1_000, 10, EnabledStatistics::Page)).await;
        // The bloom keeps a tenth of the chunk: the reader fetches the pages
        // of those blocks only, far less than every page of the chunk.
        let kept = BlockRange::new(BlockNumber(150), BlockNumber(159)).unwrap();
        let unlimited = read_kept(store.clone(), size, chunk(), kept, u64::MAX, None)
            .await
            .unwrap();
        assert_eq!(numbers(&unlimited).len(), 100);
        let downloaded = store.requested_bytes();
        let batches = read_kept(store.clone(), size, chunk(), kept, downloaded + 1_024, None)
            .await
            .unwrap();
        assert_eq!(numbers(&batches).len(), 100);
        let error = read_kept(store, size, chunk(), kept, downloaded / 2, None)
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                AwsError::Budget {
                    resource: "input_bytes",
                    ..
                }
            ),
            "{error}"
        );
    }

    #[tokio::test]
    async fn a_file_without_a_page_index_over_the_budget_is_refused_before_its_columns() {
        let (store, size) = stored(clustered_file(1_000, 10, EnabledStatistics::Chunk)).await;
        let error = read_kept(store.clone(), size, chunk(), chunk(), size / 4, None)
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                AwsError::Budget {
                    resource: "input_bytes",
                    ..
                }
            ),
            "{error}"
        );
        let footer = store.requested_bytes();
        assert!(footer < size / 100, "read {footer} of {size} bytes");
    }

    #[tokio::test]
    async fn ranges_read_again_come_from_the_cache_outside_the_input_budget() {
        let (store, size) = stored(clustered_file(1_000, 10, EnabledStatistics::Page)).await;
        let cache = Arc::new(RangeCache::new(std::env::temp_dir(), 1 << 30));
        let first = read_kept(
            store.clone(),
            size,
            chunk(),
            chunk(),
            u64::MAX,
            Some(&cache),
        )
        .await
        .unwrap();
        let downloaded = store.requested_bytes();
        let again = read_kept(store.clone(), size, chunk(), chunk(), 1, Some(&cache))
            .await
            .unwrap();
        assert_eq!(numbers(&again), numbers(&first));
        assert_eq!(store.requested_bytes(), downloaded);
    }

    #[tokio::test]
    async fn chunks_reading_a_range_at_once_download_it_once() {
        let file = clustered_file(1_000, 10, EnabledStatistics::Page);
        let (alone, size) = stored(file.clone()).await;
        let cache = Arc::new(RangeCache::new(std::env::temp_dir(), 1 << 30));
        read_kept(
            alone.clone(),
            size,
            chunk(),
            chunk(),
            u64::MAX,
            Some(&cache),
        )
        .await
        .unwrap();
        let (store, _) = stored(file).await;
        let cache = Arc::new(RangeCache::new(std::env::temp_dir(), 1 << 30));
        let (first, second) = tokio::join!(
            read_kept(
                store.clone(),
                size,
                chunk(),
                chunk(),
                u64::MAX,
                Some(&cache)
            ),
            read_kept(
                store.clone(),
                size,
                chunk(),
                chunk(),
                u64::MAX,
                Some(&cache)
            ),
        );
        assert_eq!(numbers(&first.unwrap()), numbers(&second.unwrap()));
        assert_eq!(store.requested_bytes(), alone.requested_bytes());
    }
}
