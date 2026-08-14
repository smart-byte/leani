//! Immutable, seekable append-only processor-artifact segments.

mod segment;
mod sink;

pub use segment::{
    ArtifactCompression, ArtifactSegmentDescriptor, ArtifactSegmentError,
    ArtifactSegmentInspection, ArtifactSegmentLimits, ArtifactSegmentMetadata,
    ArtifactSegmentReader, ArtifactSegmentWriter,
};
pub use sink::{
    ArtifactBatchReceipt, ArtifactBatchSink, ArtifactSegmentSink, ArtifactSegmentSinkConfig,
    ArtifactSegmentSinkStats, ArtifactSinkError,
};
