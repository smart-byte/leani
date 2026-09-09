//! Shared lifecycle contract for built-in demo processors.

use leani_processor_api::{
    ArtifactPolicyMode, CheckpointPolicyMode, DeliveryLimitAction, DeliveryPolicyMode,
    OutputPolicyMode, StatePolicyMode, UndoPolicyMode,
};

use crate::config::{
    ArtifactPolicyConfig, CheckpointPolicyConfig, DeliveryPolicyConfig, DeliveryPruningConfig,
    HumanBytes, HumanDuration, OutputPolicyConfig, ProcessorConfig, ProcessorCoverageConfig,
    ProcessorHistoryControl, ProcessorHistoryMode, PublishMode, StatePolicyConfig,
    UndoPolicyConfig,
};

pub(crate) fn processor_contract(
    id: &str,
    instance: &str,
    version: &str,
    start_block: u64,
    finalized_only: bool,
) -> ProcessorConfig {
    ProcessorConfig {
        id: id.to_owned(),
        instance: instance.to_owned(),
        version: version.to_owned(),
        history_control: ProcessorHistoryControl::NodeOwned,
        history_mode: ProcessorHistoryMode::OnDemand,
        require_retained_input: false,
        start_block,
        publish: if finalized_only {
            PublishMode::FinalizedOnly
        } else {
            PublishMode::IncludedAndFinalized
        },
        state: StatePolicyConfig {
            mode: StatePolicyMode::Checkpointed,
        },
        artifacts: ArtifactPolicyConfig {
            mode: ArtifactPolicyMode::None,
            window: None,
        },
        output: OutputPolicyConfig {
            mode: OutputPolicyMode::Full,
            window: None,
            finalized_only,
        },
        delivery: DeliveryPolicyConfig {
            mode: DeliveryPolicyMode::Window,
            max_bytes: HumanBytes::from_bytes(64 * 1_024 * 1_024),
            max_age: HumanDuration::from_seconds(24 * 60 * 60),
            on_limit: DeliveryLimitAction::Pause,
            pruning: DeliveryPruningConfig::default(),
            consumers: Vec::new(),
        },
        checkpoint: CheckpointPolicyConfig {
            mode: CheckpointPolicyMode::Automatic,
            keep: 3,
        },
        undo: UndoPolicyConfig {
            mode: if finalized_only {
                UndoPolicyMode::None
            } else {
                UndoPolicyMode::Unfinalized
            },
            safety_blocks: if finalized_only { 0 } else { 256 },
        },
        coverage: ProcessorCoverageConfig::default(),
        settings: toml::Table::new(),
    }
}
