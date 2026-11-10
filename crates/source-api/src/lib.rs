//! Contracts for historical, live, and finality sources.

mod conformance;
mod contract;
mod planner;
mod rollup;
mod stored;
mod telemetry;

pub use conformance::{
    ConformanceError, CrossSourceReport, FrameDifference, FrameFingerprint,
    compare_frame_sequences, frame_fingerprint,
};
pub use contract::{
    AttestedHead, AttestedHeadPublisher, AttestedHeadReceiver, BlockFrameStream, ChainEvent,
    ChainEventStream, ConfirmationVotes, ConsensusCheckpoint, DataRequest, ExecutionHeaders,
    FieldProjection, FilterSet, FinalityEvent, FinalityEventStream, FinalityModel, FinalitySource,
    HistoryLookupCapabilities, HistorySource, LiveSource, LiveStart, LocatedTransaction,
    Partitioning, PhysicalPlanOperation, PhysicalReader, SourceAcquisitionMetrics, SourceBudget,
    SourceChunk, SourceDescriptor, SourceError, SourcePlan, VerificationPolicy,
};
pub use planner::{PlanError, SelectionPolicy, coverage_gaps, select_source};
pub use rollup::{RollupBlock, RollupFinality};
pub use stored::stop_at_unfilled_groups;
pub use telemetry::{
    LIVE_DELIVERY_LEVELS, NetworkConfirmationEndpointSnapshot, NetworkConfirmationEndpointState,
    NetworkConfirmationHeadSnapshot, NetworkConfirmationSnapshot, NetworkDisconnectReason,
    NetworkDisconnectSnapshot, NetworkLane, NetworkLiveLevelsSnapshot,
    NetworkPeerLifecycleSnapshot, NetworkPeerOrigin, NetworkPeerOriginSnapshot,
    NetworkPeerQualification, NetworkPeerQualificationSnapshot, NetworkPeerTargetsSnapshot,
    NetworkPhase, NetworkSessionSnapshot, NetworkSessionTelemetry, NetworkSupervisorSnapshot,
    NetworkSupervisorState, NetworkTelemetry, NetworkTelemetrySnapshot, UnattestedUndo,
};
