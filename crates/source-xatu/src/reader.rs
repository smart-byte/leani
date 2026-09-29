//! Parquet range reader over the read-only Xatu HTTP object store.

use std::{
    ops::Range,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use bytes::{Bytes, BytesMut};
use futures::{FutureExt, StreamExt, TryStreamExt, future::BoxFuture, stream};
use object_store::{GetOptions, ObjectMeta, ObjectStore, path::Path};
use parquet::{
    arrow::{
        arrow_reader::ArrowReaderOptions,
        async_reader::{AsyncFileReader, MetadataFetch},
    },
    errors::{ParquetError, Result},
    file::metadata::{ParquetMetaData, ParquetMetaDataReader},
};

/// Ranges this close together are fetched as one request, as `object_store`
/// coalesces them, while the input budget covers the bytes between them.
const RANGE_COALESCE_BYTES: u64 = 1_024 * 1_024;
/// Largest range read while loading a Parquet footer. The footer's own
/// length field names the metadata range, and Xatu footers are a few
/// megabytes at most.
const MAX_METADATA_BYTES: u64 = 64 * 1_024 * 1_024;

/// Bytes one source open may request from the object store. A reader starts
/// from what the open's earlier objects used, and charges each request before
/// it is sent: footers, column chunks, and the gaps merged between them.
#[derive(Debug)]
pub(crate) struct InputBudget {
    limit: u64,
    used: AtomicU64,
    /// What a refused request would have brought the total to, or zero.
    refused: AtomicU64,
}

impl InputBudget {
    /// A budget of `limit` bytes, `used` of them by earlier objects.
    pub(crate) fn new(limit: u64, used: u64) -> Arc<Self> {
        Arc::new(Self {
            limit,
            used: AtomicU64::new(used),
            refused: AtomicU64::new(0),
        })
    }

    pub(crate) fn used(&self) -> u64 {
        self.used.load(Ordering::Relaxed)
    }

    fn remaining(&self) -> u64 {
        self.limit.saturating_sub(self.used())
    }

    /// What a refused request would have brought the total to.
    pub(crate) fn refused(&self) -> Option<u64> {
        Some(self.refused.load(Ordering::Relaxed)).filter(|total| *total != 0)
    }

    /// Charge `bytes` before they are requested.
    fn charge(&self, bytes: u64) -> Result<()> {
        self.used
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                used.checked_add(bytes).filter(|total| *total <= self.limit)
            })
            .map(|_| ())
            .map_err(|used| {
                let total = used.saturating_add(bytes);
                self.refused.store(total, Ordering::Relaxed);
                ParquetError::General(format!(
                    "Xatu reads would reach {total} bytes, over the {}-byte input budget",
                    self.limit
                ))
            })
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ObjectStoreReader {
    store: Arc<dyn ObjectStore>,
    path: Path,
    file_size: u64,
    /// The object version seen at HEAD. Every read is conditioned on it, so
    /// an object rewritten mid-read fails the read instead of mixing two
    /// versions.
    e_tag: Option<String>,
    /// Most range requests in flight at once.
    concurrency: usize,
    budget: Arc<InputBudget>,
    metrics: Arc<ReaderMetrics>,
}

#[derive(Debug, Default)]
struct ReaderMetrics {
    logical_range_requests: AtomicU64,
    requested_bytes: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ReaderMetricsSnapshot {
    pub logical_range_requests: u64,
    /// Bytes of every range requested, merged gaps and footers included.
    pub requested_bytes: u64,
}

impl ObjectStoreReader {
    /// Read the object `head` describes, pinned to its `ETag`, with at most
    /// `concurrency` range requests in flight, charging each to `budget`.
    pub(crate) fn new(
        store: Arc<dyn ObjectStore>,
        head: &ObjectMeta,
        concurrency: usize,
        budget: Arc<InputBudget>,
    ) -> (Self, ReaderMetricsHandle) {
        let metrics = Arc::new(ReaderMetrics::default());
        (
            Self {
                store,
                path: head.location.clone(),
                file_size: head.size,
                e_tag: head.e_tag.clone(),
                concurrency: concurrency.max(1),
                budget,
                metrics: Arc::clone(&metrics),
            },
            ReaderMetricsHandle(metrics),
        )
    }

    /// Read `range` of the pinned object version, refusing a response of
    /// another length.
    async fn read_pinned(&self, range: Range<u64>) -> Result<Bytes> {
        let expected = range
            .end
            .checked_sub(range.start)
            .ok_or_else(|| ParquetError::General(format!("invalid Xatu byte range {range:?}")))?;
        self.metrics
            .requested_bytes
            .fetch_add(expected, Ordering::Relaxed);
        let options = GetOptions::new()
            .with_range(Some(range.clone()))
            .with_if_match(self.e_tag.clone());
        let mut body = self
            .store
            .get_opts(&self.path, options)
            .await
            .map_err(parquet_error)?
            .into_stream();
        let mut bytes = BytesMut::new();
        while let Some(chunk) = body.next().await {
            let chunk = chunk.map_err(parquet_error)?;
            if u64::try_from(bytes.len().saturating_add(chunk.len())).unwrap_or(u64::MAX) > expected
            {
                return Err(ParquetError::General(format!(
                    "Xatu object returned more than the {expected} bytes of range {range:?}"
                )));
            }
            bytes.extend_from_slice(&chunk);
        }
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX) != expected {
            return Err(ParquetError::General(format!(
                "Xatu object returned {} of the {expected} bytes of range {range:?}",
                bytes.len()
            )));
        }
        Ok(bytes.freeze())
    }
}

/// Metadata reads of an [`ObjectStoreReader`], each refused before it is
/// requested when it exceeds [`MAX_METADATA_BYTES`].
struct MetadataReads<'a>(&'a mut ObjectStoreReader);

impl MetadataFetch for MetadataReads<'_> {
    fn fetch(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
        let length = range.end.saturating_sub(range.start);
        if length > MAX_METADATA_BYTES {
            return futures::future::ready(Err(ParquetError::General(format!(
                "Xatu Parquet metadata range of {length} bytes exceeds {MAX_METADATA_BYTES}"
            ))))
            .boxed();
        }
        self.0.get_bytes(range)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ReaderMetricsHandle(Arc<ReaderMetrics>);

impl ReaderMetricsHandle {
    pub(crate) fn snapshot(&self) -> ReaderMetricsSnapshot {
        ReaderMetricsSnapshot {
            logical_range_requests: self.0.logical_range_requests.load(Ordering::Relaxed),
            requested_bytes: self.0.requested_bytes.load(Ordering::Relaxed),
        }
    }
}

fn parquet_error(error: object_store::Error) -> ParquetError {
    ParquetError::External(Box::new(error))
}

/// Sort `ranges` and merge those that overlap or touch, and those at most
/// [`RANGE_COALESCE_BYTES`] apart while `gap_allowance` still covers the
/// bytes between them, which the merged request downloads too.
fn merge_ranges(ranges: &[Range<u64>], mut gap_allowance: u64) -> Vec<Range<u64>> {
    let mut sorted = ranges.to_vec();
    sorted.sort_unstable_by_key(|range| range.start);
    let mut merged: Vec<Range<u64>> = Vec::with_capacity(sorted.len());
    for range in sorted {
        match merged.last_mut() {
            Some(last) if range.start <= last.end => last.end = last.end.max(range.end),
            Some(last) if range.start - last.end <= RANGE_COALESCE_BYTES.min(gap_allowance) => {
                gap_allowance -= range.start - last.end;
                last.end = range.end;
            }
            _ => merged.push(range),
        }
    }
    merged
}

fn span_bytes(ranges: &[Range<u64>]) -> u64 {
    ranges.iter().fold(0, |total, range| {
        total.saturating_add(range.end - range.start)
    })
}

impl AsyncFileReader for ObjectStoreReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
        async move {
            self.metrics
                .logical_range_requests
                .fetch_add(1, Ordering::Relaxed);
            self.budget.charge(range.end.saturating_sub(range.start))?;
            self.read_pinned(range).await
        }
        .boxed()
    }

    fn get_byte_ranges(&mut self, ranges: Vec<Range<u64>>) -> BoxFuture<'_, Result<Vec<Bytes>>> {
        async move {
            self.metrics.logical_range_requests.fetch_add(
                u64::try_from(ranges.len()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
            if let Some(range) = ranges.iter().find(|range| range.end < range.start) {
                return Err(ParquetError::General(format!(
                    "invalid Xatu byte range {range:?}"
                )));
            }
            // Merge across a gap only while the budget also covers it, and
            // charge every merged byte before any is requested.
            let needed = span_bytes(&merge_ranges(&ranges, 0));
            let merged = merge_ranges(&ranges, self.budget.remaining().saturating_sub(needed));
            self.budget.charge(span_bytes(&merged))?;
            let reader = &*self;
            let fetched = stream::iter(merged.iter().cloned())
                .map(|range| reader.read_pinned(range))
                .buffered(reader.concurrency)
                .try_collect::<Vec<_>>()
                .await?;
            let pages = ranges
                .iter()
                .map(|range| {
                    // The last merged range starting at or before this one
                    // contains it, and its bytes have exactly its length.
                    let index = merged
                        .partition_point(|fetched| fetched.start <= range.start)
                        .checked_sub(1)?;
                    let start = usize::try_from(range.start - merged[index].start).ok()?;
                    let end = usize::try_from(range.end - merged[index].start).ok()?;
                    let bytes = &fetched[index];
                    (end <= bytes.len()).then(|| bytes.slice(start..end))
                })
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| ParquetError::General("Xatu range was not fetched".to_owned()))?;
            Ok(pages)
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, Result<Arc<ParquetMetaData>>> {
        async move {
            let file_size = self.file_size;
            let metadata = ParquetMetaDataReader::new()
                .with_arrow_reader_options(options)
                .load_and_finish(MetadataReads(self), file_size)
                .await?;
            Ok(Arc::new(metadata))
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::{ObjectStoreExt, memory::InMemory};

    // Retain the former split strategy only for manual transport comparisons.
    const MAX_HTTP_RANGE_BYTES: u64 = 256 * 1024;
    const MAX_HTTP_RANGE_CONCURRENCY: usize = 4;

    async fn read_bounded_range(
        store: &dyn ObjectStore,
        path: &Path,
        range: Range<u64>,
    ) -> object_store::Result<Bytes> {
        if range.end.saturating_sub(range.start) <= MAX_HTTP_RANGE_BYTES {
            return store.get_range(path, range).await;
        }
        let mut result = BytesMut::new();
        let mut start = range.start;
        while start < range.end {
            let end = start.saturating_add(MAX_HTTP_RANGE_BYTES).min(range.end);
            let bytes = store.get_range(path, start..end).await?;
            result.extend_from_slice(&bytes);
            start = end;
        }
        Ok(result.freeze())
    }

    const TRANSPORT_CASES: &[(&str, &[&str])] = &[
        (
            "canonical_beacon_block/2024/3/14.parquet",
            &[
                "slot_start_date_time",
                "execution_payload_block_hash",
                "execution_payload_block_number",
                "execution_payload_parent_hash",
                "execution_payload_transactions_count",
            ],
        ),
        (
            "canonical_execution_logs/1000/17000000.parquet",
            &[
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
            ],
        ),
    ];

    /// Compare the former split strategy with the production coalescing reader on
    /// identical public objects. Repeated passes are not a cold-CDN guarantee.
    #[tokio::test]
    #[ignore = "manual public Xatu HTTP comparison; run with --ignored --nocapture"]
    async fn compare_public_xatu_range_strategies() {
        use std::time::{Duration, Instant};

        use object_store::http::HttpBuilder;

        let store: Arc<dyn ObjectStore> = Arc::new(
            HttpBuilder::new()
                .with_url(crate::DEFAULT_XATU_BASE_URL.trim_end_matches('/'))
                .build()
                .expect("HTTP store"),
        );
        let deadline = Duration::from_secs(20);
        for (name, columns) in TRANSPORT_CASES {
            let path = Path::from(*name);
            let head = tokio::time::timeout(deadline, store.head(&path))
                .await
                .expect("HEAD deadline")
                .expect("HEAD");
            let (mut reader, _) = ObjectStoreReader::new(
                Arc::clone(&store),
                &head,
                MAX_HTTP_RANGE_CONCURRENCY,
                InputBudget::new(u64::MAX, 0),
            );
            let metadata = tokio::time::timeout(deadline, reader.get_metadata(None))
                .await
                .expect("metadata deadline")
                .expect("metadata");
            let ranges: Vec<_> = metadata
                .row_groups()
                .iter()
                .flat_map(parquet::file::metadata::RowGroupMetaData::columns)
                .filter(|column| columns.contains(&column.column_path().string().as_str()))
                .map(|column| {
                    let (start, length) = column.byte_range();
                    start..start + length
                })
                .collect();
            assert!(!ranges.is_empty());
            let expected_bytes: u64 = ranges.iter().map(|range| range.end - range.start).sum();
            let mut reference = None;
            // Reverse the order for the second pair to reduce ordering bias.
            for (pass, bounded) in [false, true, true, false].into_iter().enumerate() {
                let label = if bounded { "bounded" } else { "coalesced" };
                let began = Instant::now();
                let result = tokio::time::timeout(deadline, async {
                    if bounded {
                        stream::iter(ranges.clone())
                            .map(|range| read_bounded_range(store.as_ref(), &path, range))
                            .buffered(MAX_HTTP_RANGE_CONCURRENCY)
                            .try_collect::<Vec<Bytes>>()
                            .await
                            .map_err(parquet_error)
                    } else {
                        reader.get_byte_ranges(ranges.clone()).await
                    }
                })
                .await;
                let elapsed = began.elapsed().as_millis();
                match result {
                    Ok(Ok(bytes)) => {
                        assert_eq!(
                            bytes.iter().map(|part| part.len() as u64).sum::<u64>(),
                            expected_bytes
                        );
                        if let Some(reference) = &reference {
                            assert!(
                                &bytes == reference,
                                "strategies returned different bytes for {name}"
                            );
                        } else {
                            reference = Some(bytes);
                        }
                        println!(
                            "path={name} strategy={label} pass={pass} elapsed_ms={elapsed} bytes={expected_bytes} status=ok"
                        );
                    }
                    Ok(Err(error)) => println!(
                        "path={name} strategy={label} pass={pass} elapsed_ms={elapsed} status=error error={error}"
                    ),
                    Err(_) => println!(
                        "path={name} strategy={label} pass={pass} elapsed_ms={elapsed} status=timeout"
                    ),
                }
            }
            assert!(reference.is_some(), "no complete read of {name}");
        }
    }

    #[tokio::test]
    async fn metadata_reads_are_capped_before_they_are_requested() {
        use parquet::arrow::async_reader::MetadataFetch as _;

        let store = Arc::new(InMemory::new());
        let path = Path::from("footer.parquet");
        store.put(&path, vec![0_u8; 64].into()).await.expect("put");
        let head = store.head(&path).await.expect("head");
        let (mut reader, metrics) =
            ObjectStoreReader::new(store, &head, 4, InputBudget::new(u64::MAX, 0));
        // A footer's length field is untrusted, and names the metadata range.
        assert!(
            MetadataReads(&mut reader)
                .fetch(0..MAX_METADATA_BYTES + 1)
                .await
                .is_err()
        );
        assert_eq!(metrics.snapshot().logical_range_requests, 0);
        assert_eq!(
            MetadataReads(&mut reader)
                .fetch(8..16)
                .await
                .expect("a small metadata range")
                .len(),
            8
        );
    }

    #[tokio::test]
    async fn reads_preserve_large_unaligned_and_overlapping_ranges() {
        let store = Arc::new(InMemory::new());
        let path = Path::from("columns.parquet");
        let payload: Vec<u8> = (0..900_001_u32)
            .map(|value| value.to_le_bytes()[0])
            .collect();
        store.put(&path, payload.clone().into()).await.expect("put");
        let head = store.head(&path).await.expect("head");
        let (mut reader, metrics) =
            ObjectStoreReader::new(store, &head, 4, InputBudget::new(u64::MAX, 0));
        assert_eq!(
            reader.get_bytes(13..800_009).await.expect("large read"),
            payload[13..800_009]
        );
        let ranges = vec![700_003..900_001, 5..600_001, 99..103];
        let pages = reader
            .get_byte_ranges(ranges.clone())
            .await
            .expect("ranges");
        for (page, range) in pages.iter().zip(ranges) {
            assert_eq!(
                page.as_ref(),
                &payload
                    [usize::try_from(range.start).unwrap()..usize::try_from(range.end).unwrap()]
            );
        }
        assert_eq!(metrics.snapshot().logical_range_requests, 4);
    }
}
