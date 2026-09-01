//! Contracts for historical, live, and finality sources.

mod conformance;
mod contract;
mod planner;
mod telemetry;

pub use conformance::{
    ConformanceError, CrossSourceReport, FrameDifference, FrameFingerprint,
    compare_frame_sequences, frame_fingerprint,
};
pub use contract::{
    BlockFrameStream, ChainEvent, ChainEventStream, ConsensusCheckpoint, DataRequest,
    FieldProjection, FilterSet, FinalityEvent, FinalityEventStream, FinalityModel, FinalitySource,
    HistoryLookupCapabilities, HistorySource, LiveSource, LiveStart, LocatedTransaction,
    Partitioning, PhysicalPlanOperation, PhysicalReader, SourceAcquisitionMetrics, SourceBudget,
    SourceChunk, SourceDescriptor, SourceError, SourcePlan, VerificationPolicy,
};
pub use planner::{PlanError, SelectionPolicy, coverage_gaps, select_source};
pub use telemetry::{
    NetworkDisconnectReason, NetworkDisconnectSnapshot, NetworkLane, NetworkPeerLifecycleSnapshot,
    NetworkPeerOrigin, NetworkPeerOriginSnapshot, NetworkPeerQualification,
    NetworkPeerQualificationSnapshot, NetworkPeerTargetsSnapshot, NetworkPhase,
    NetworkSessionSnapshot, NetworkSessionTelemetry, NetworkSupervisorSnapshot,
    NetworkSupervisorState, NetworkTelemetry, NetworkTelemetrySnapshot,
};
