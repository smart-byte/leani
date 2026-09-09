//! Parquet range reader over the read-only Xatu HTTP object store.

use std::{
    ops::Range,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use bytes::Bytes;
use futures::{FutureExt, future::BoxFuture};
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use parquet::{
    arrow::{arrow_reader::ArrowReaderOptions, async_reader::AsyncFileReader},
    errors::{ParquetError, Result},
    file::metadata::{ParquetMetaData, ParquetMetaDataReader},
};

#[derive(Clone, Debug)]
pub(crate) struct ObjectStoreReader {
    store: Arc<dyn ObjectStore>,
    path: Path,
    file_size: u64,
    metrics: Arc<ReaderMetrics>,
}

#[derive(Debug, Default)]
struct ReaderMetrics {
    logical_range_requests: AtomicU64,
    returned_bytes: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ReaderMetricsSnapshot {
    pub logical_range_requests: u64,
    pub returned_bytes: u64,
}

impl ObjectStoreReader {
    pub(crate) fn new(
        store: Arc<dyn ObjectStore>,
        path: Path,
        file_size: u64,
    ) -> (Self, ReaderMetricsHandle) {
        let metrics = Arc::new(ReaderMetrics::default());
        (
            Self {
                store,
                path,
                file_size,
                metrics: Arc::clone(&metrics),
            },
            ReaderMetricsHandle(metrics),
        )
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ReaderMetricsHandle(Arc<ReaderMetrics>);

impl ReaderMetricsHandle {
    pub(crate) fn snapshot(&self) -> ReaderMetricsSnapshot {
        ReaderMetricsSnapshot {
            logical_range_requests: self.0.logical_range_requests.load(Ordering::Relaxed),
            returned_bytes: self.0.returned_bytes.load(Ordering::Relaxed),
        }
    }
}

fn parquet_error(error: object_store::Error) -> ParquetError {
    ParquetError::External(Box::new(error))
}

impl AsyncFileReader for ObjectStoreReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
        async move {
            self.metrics
                .logical_range_requests
                .fetch_add(1, Ordering::Relaxed);
            match self.store.get_range(&self.path, range).await {
                Ok(bytes) => {
                    self.metrics.returned_bytes.fetch_add(
                        u64::try_from(bytes.len()).unwrap_or(u64::MAX),
                        Ordering::Relaxed,
                    );
                    Ok(bytes)
                }
                Err(error) => Err(parquet_error(error)),
            }
        }
        .boxed()
    }

    fn get_byte_ranges(&mut self, ranges: Vec<Range<u64>>) -> BoxFuture<'_, Result<Vec<Bytes>>> {
        async move {
            self.metrics.logical_range_requests.fetch_add(
                u64::try_from(ranges.len()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
            match self.store.get_ranges(&self.path, &ranges).await {
                Ok(bytes) => {
                    self.metrics.returned_bytes.fetch_add(
                        bytes.iter().fold(0_u64, |total, value| {
                            total.saturating_add(u64::try_from(value.len()).unwrap_or(u64::MAX))
                        }),
                        Ordering::Relaxed,
                    );
                    Ok(bytes)
                }
                Err(error) => Err(parquet_error(error)),
            }
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
                .load_and_finish(self, file_size)
                .await?;
            Ok(Arc::new(metadata))
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use futures::{StreamExt, TryStreamExt, stream};
    use object_store::memory::InMemory;

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
            let (mut reader, _) =
                ObjectStoreReader::new(Arc::clone(&store), path.clone(), head.size);
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
    async fn reads_preserve_large_unaligned_and_overlapping_ranges() {
        let store = Arc::new(InMemory::new());
        let path = Path::from("columns.parquet");
        let payload: Vec<u8> = (0..900_001_u32)
            .map(|value| value.to_le_bytes()[0])
            .collect();
        store.put(&path, payload.clone().into()).await.expect("put");
        let (mut reader, metrics) = ObjectStoreReader::new(store, path, payload.len() as u64);
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
