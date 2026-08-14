//! Processor descriptors, deterministic deltas, reducer transactions, and
//! domain changes.

mod contract;
mod descriptor;

pub use contract::{
    ChangeOperation, DomainChange, DomainChanges, EncodedDelta, Processor, ProcessorError,
    ReducerTransaction,
};
pub use descriptor::{
    ArtifactPolicy, ArtifactPolicyMode, ArtifactWindow, CheckpointPolicy, CheckpointPolicyMode,
    DataRequirement, DeliveryLimitAction, DeliveryOrdering, DeliveryPolicy, DeliveryPolicyMode,
    DeliveryPruningPolicy, DurableConsumerPolicy, LifecyclePolicies, OutputPolicy,
    OutputPolicyMode, OutputWindow, ProcessorDescriptor, ProcessorId, ProcessorIdError,
    ProcessorInstanceId, ProcessorInstanceIdError, ProcessorSchemas, PublicationPolicy,
    ReductionMode, RetentionPolicy, StartPoint, StatePolicy, StatePolicyMode, UndoPolicy,
    UndoPolicyMode,
};
