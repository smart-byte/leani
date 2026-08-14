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
