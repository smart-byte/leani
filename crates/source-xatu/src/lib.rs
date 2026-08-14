//! Xatu catalog, Parquet reader, and source-neutral normalizer.

mod catalog;
mod history;
mod projection;
mod reader;

pub use catalog::{
    CatalogObject, CatalogProbeReport, DEFAULT_XATU_BASE_URL, ObjectInspection, ObjectProbeFailure,
    ParquetColumn, PartitionKind, XATU_DATA_ORIGIN, XatuCatalog, XatuCatalogConfig, XatuDate,
    XatuError, XatuTable,
};
pub use history::{XatuBlobsHistorySource, XatuHistoryConfig, XatuHistorySource};
pub use projection::{
    BlobsProjection, ProjectionMetrics, ProjectionObjectMetrics, XatuBlobsProjector,
};
