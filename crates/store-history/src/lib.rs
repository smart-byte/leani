//! Durable raw execution history backed by immutable segment files.
//!
//! This store deliberately owns a catalog separate from processor state and
//! delivery storage. Raw history can therefore outlive processor artifacts,
//! or be omitted entirely, without coupling its retention to consumer acks.

mod catalog;
mod job;
mod runner;
mod segment;
mod source;

pub use catalog::{
    BlockHashLocator, HistoryStore, HistoryStoreConfig, HistoryStoreError, HistoryStoreStats,
    PendingSegment, RecoveryReport, SegmentOwner, SegmentOwnerClaim, SegmentOwnerKind,
    SegmentRecord, SegmentReservation, StorageBudget, TransactionLocator,
};
pub use job::{
    RawHistoryIndexPolicy, RawHistoryJob, RawHistoryJobDeletion, RawHistoryJobId,
    RawHistoryJobSpec, RawHistoryJobState, RawHistoryMaterialProfile, RawHistoryProfile,
    RawHistoryRetention, RawHistorySegmentPolicy, StorageLimitAction,
};
pub use runner::{RawHistoryRunError, RawHistoryRunOutcome, RawHistoryRunner, RawHistorySourceSet};
pub use segment::{
    Compression, MaterialShapeId, SegmentDescriptor, SegmentError, SegmentId, SegmentLimits,
    SegmentMetadata, SegmentRead, SegmentReader, SegmentWriter, VerificationClass,
};
pub use source::{RetainedHistorySource, RetainedHistorySourceConfig, RetainedHistorySourceStats};
