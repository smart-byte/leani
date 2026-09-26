//! Static processor identity, input contract, and output policies.

use std::{fmt, str::FromStr};

use leani_primitives::{
    BlockFrame, BlockHash, BlockNumber, Capability, CapabilitySet, Completeness, FilterScope,
    Finality, LogField, LogFieldSet, Material,
};
use semver::Version;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Stable processor namespace. Decoding validates it like [`Self::new`].
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct ProcessorId(String);

impl ProcessorId {
    /// Validate a stable processor namespace.
    ///
    /// # Errors
    ///
    /// Returns [`ProcessorIdError`] for an empty, too-long, or non-portable ID.
    pub fn new(value: impl Into<String>) -> Result<Self, ProcessorIdError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 64
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        {
            Err(ProcessorIdError(value))
        } else {
            Ok(Self(value))
        }
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ProcessorId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for ProcessorId {
    type Err = ProcessorIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl<'de> Deserialize<'de> for ProcessorId {
    fn deserialize<Deserializer>(deserializer: Deserializer) -> Result<Self, Deserializer::Error>
    where
        Deserializer: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("invalid processor ID `{0}`; use 1-64 ASCII letters, digits, '.', '_' or '-'")]
pub struct ProcessorIdError(String);

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct ProcessorInstanceId(String);

impl ProcessorInstanceId {
    /// Validate a stable processor-instance identity.
    ///
    /// `@` and `:` remain accepted for backwards-compatible derived instance
    /// IDs. New operator-selected IDs should use the same portable alphabet as
    /// [`ProcessorId`].
    ///
    /// # Errors
    ///
    /// Returns [`ProcessorInstanceIdError`] for an empty, too-long, or
    /// non-portable ID.
    pub fn new(value: impl Into<String>) -> Result<Self, ProcessorInstanceIdError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 192
            || value.starts_with(':')
            || matches!(value.as_str(), "." | "..")
            || !value.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'@' | b':')
            })
        {
            Err(ProcessorInstanceIdError(value))
        } else {
            Ok(Self(value))
        }
    }

    /// Reproduce the processor-instance key used before explicit instance IDs.
    ///
    /// # Errors
    ///
    /// Returns [`ProcessorInstanceIdError`] when the derived key is not a
    /// valid instance ID: the version carries `+` build metadata, or the ID
    /// and version together exceed the 192-byte limit.
    pub fn legacy(
        id: &ProcessorId,
        version: &Version,
        config_hash: BlockHash,
    ) -> Result<Self, ProcessorInstanceIdError> {
        Self::new(format!("{id}@{version}:{}", hex::encode(config_hash.0)))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ProcessorInstanceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for ProcessorInstanceId {
    type Err = ProcessorInstanceIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl<'de> Deserialize<'de> for ProcessorInstanceId {
    fn deserialize<Deserializer>(deserializer: Deserializer) -> Result<Self, Deserializer::Error>
    where
        Deserializer: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error(
    "invalid processor instance ID `{0}`; use 1-192 ASCII letters, digits, '.', '_', '-', '@' or ':', without a leading ':' or a '.'/'..' path segment"
)]
pub struct ProcessorInstanceIdError(String);

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum StartPoint {
    Genesis,
    Block(BlockNumber),
    ProcessorCheckpoint(String),
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ReductionMode {
    BlockLocal,
    OrderedState,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicationPolicy {
    IncludedAndFinalized,
    FinalizedOnly,
    TerminalOnly,
}

/// Legacy output-retention setting accepted while version-1 descriptors and
/// configurations migrate to [`LifecyclePolicies`].
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RetentionPolicy {
    FullOutputHistory,
    LatestState,
    FinalizedHistory,
}

/// Persistence of processor working state.
///
/// The store persists state in every mode; [`CheckpointPolicy`] alone takes
/// recovery checkpoints. [`LifecyclePolicies::validate`] therefore rejects
/// `Ephemeral`, and `Checkpointed` without automatic checkpoints.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StatePolicyMode {
    /// Not implemented: rejected by validation.
    Ephemeral,
    #[default]
    Durable,
    /// Durable state with automatic recovery checkpoints.
    Checkpointed,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StatePolicy {
    #[serde(default)]
    pub mode: StatePolicyMode,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputPolicyMode {
    None,
    Latest,
    Window,
    #[default]
    Full,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OutputWindow {
    #[serde(default)]
    pub max_blocks: Option<u64>,
    #[serde(default)]
    pub max_age_seconds: Option<u64>,
    #[serde(default)]
    pub max_rows: Option<u64>,
    #[serde(default)]
    pub max_bytes: Option<u64>,
}

impl OutputWindow {
    fn validate(self) -> Result<(), &'static str> {
        let values = [
            self.max_blocks,
            self.max_age_seconds,
            self.max_rows,
            self.max_bytes,
        ];
        if values.into_iter().flatten().any(|value| value == 0) {
            return Err("output window limits must be greater than zero");
        }
        if values.into_iter().flatten().count() != 1 {
            return Err("output window must declare exactly one block, age, row, or byte limit");
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OutputPolicy {
    pub mode: OutputPolicyMode,
    #[serde(default)]
    pub window: Option<OutputWindow>,
    /// Ignored; kept for descriptor compatibility. To publish only finalized
    /// blocks, use [`PublicationPolicy::FinalizedOnly`]
    /// (`publish = "finalized_only"`).
    #[serde(default)]
    pub finalized_only: bool,
}

impl Default for OutputPolicy {
    fn default() -> Self {
        Self {
            mode: OutputPolicyMode::Full,
            window: None,
            finalized_only: false,
        }
    }
}

/// Retention mode for immutable finalized map results.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactPolicyMode {
    /// Do not retain processor artifacts after reduction/publication.
    #[default]
    None,
    /// Retain a bounded rolling artifact window.
    Window,
    /// Retain every finalized artifact until explicit deletion.
    Full,
}

/// Exactly one bound for a rolling processor-artifact window.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactWindow {
    #[serde(default)]
    pub max_blocks: Option<u64>,
    #[serde(default)]
    pub max_age_seconds: Option<u64>,
    #[serde(default)]
    pub max_bytes: Option<u64>,
}

impl ArtifactWindow {
    fn validate(self) -> Result<(), &'static str> {
        let values = [self.max_blocks, self.max_age_seconds, self.max_bytes];
        if values.into_iter().flatten().any(|value| value == 0) {
            return Err("artifact window limits must be greater than zero");
        }
        if values.into_iter().flatten().count() != 1 {
            return Err("artifact window must declare exactly one block, age, or byte limit");
        }
        Ok(())
    }
}

/// Lifecycle contract for compact, versioned processor artifacts.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactPolicy {
    #[serde(default)]
    pub mode: ArtifactPolicyMode,
    #[serde(default)]
    pub window: Option<ArtifactWindow>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryPolicyMode {
    None,
    BestEffort,
    Window,
    #[default]
    UntilAcknowledged,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryLimitAction {
    #[default]
    Pause,
    Fail,
    ExpireAndReset,
}

/// Ordering contract for one processor's public changes.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryOrdering {
    /// Publications require one canonical catch-up-then-live order.
    #[default]
    Canonical,
    /// Changes carry stable block/version identity and are safe to upsert
    /// idempotently when independently delivered live and history overlap.
    BlockVersionedIdempotent,
}

/// Longest lease TTL a durable consumer may hold: 100 years. Its
/// milliseconds, and the expiry of a lease taken now, stay far inside an
/// `i64`.
pub const MAXIMUM_CONSUMER_LEASE_TTL: std::time::Duration =
    std::time::Duration::from_hours(100 * 365 * 24);

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DurableConsumerPolicy {
    pub id: String,
    pub required: bool,
    /// Between one second and [`MAXIMUM_CONSUMER_LEASE_TTL`].
    pub lease_ttl_seconds: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryPruningPolicy {
    pub interval_seconds: u64,
    pub minimum_batch_blocks: u64,
    pub minimum_batch_changes: u64,
    pub maximum_delete_changes: u64,
    pub retain_finalized_blocks: u64,
    pub retain_acknowledged_seconds: u64,
}

impl Default for DeliveryPruningPolicy {
    fn default() -> Self {
        Self {
            interval_seconds: 30,
            minimum_batch_blocks: 64,
            minimum_batch_changes: 10_000,
            maximum_delete_changes: 10_000,
            retain_finalized_blocks: 256,
            retain_acknowledged_seconds: 60 * 60,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryPolicy {
    pub mode: DeliveryPolicyMode,
    pub max_bytes: u64,
    pub max_age_seconds: u64,
    #[serde(default)]
    pub on_limit: DeliveryLimitAction,
    #[serde(default)]
    pub pruning: DeliveryPruningPolicy,
    #[serde(default)]
    pub consumers: Vec<DurableConsumerPolicy>,
}

impl Default for DeliveryPolicy {
    fn default() -> Self {
        Self {
            mode: DeliveryPolicyMode::Window,
            max_bytes: 1 << 30,
            max_age_seconds: 24 * 60 * 60,
            on_limit: DeliveryLimitAction::Pause,
            pruning: DeliveryPruningPolicy::default(),
            consumers: Vec::new(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointPolicyMode {
    None,
    #[default]
    Automatic,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointPolicy {
    #[serde(default)]
    pub mode: CheckpointPolicyMode,
    pub keep: u32,
}

impl Default for CheckpointPolicy {
    fn default() -> Self {
        Self {
            mode: CheckpointPolicyMode::Automatic,
            keep: 3,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UndoPolicyMode {
    None,
    #[default]
    Unfinalized,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UndoPolicy {
    #[serde(default)]
    pub mode: UndoPolicyMode,
    pub safety_blocks: u64,
}

impl Default for UndoPolicy {
    fn default() -> Self {
        Self {
            mode: UndoPolicyMode::Unfinalized,
            safety_blocks: 256,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LifecyclePolicies {
    #[serde(default)]
    pub state: StatePolicy,
    #[serde(default)]
    pub artifacts: ArtifactPolicy,
    #[serde(default)]
    pub output: OutputPolicy,
    #[serde(default)]
    pub delivery: DeliveryPolicy,
    #[serde(default)]
    pub checkpoint: CheckpointPolicy,
    #[serde(default)]
    pub undo: UndoPolicy,
}

impl LifecyclePolicies {
    #[must_use]
    pub fn from_legacy(retention: RetentionPolicy) -> Self {
        let output = match retention {
            RetentionPolicy::FullOutputHistory => OutputPolicy::default(),
            RetentionPolicy::LatestState => OutputPolicy {
                mode: OutputPolicyMode::Latest,
                window: None,
                finalized_only: false,
            },
            RetentionPolicy::FinalizedHistory => OutputPolicy {
                finalized_only: true,
                ..OutputPolicy::default()
            },
        };
        Self {
            output,
            ..Self::default()
        }
    }

    /// Validate cross-policy lifecycle invariants.
    ///
    /// # Errors
    ///
    /// Returns a static explanation for an ambiguous or lossy policy set.
    pub fn validate(&self, publication: PublicationPolicy) -> Result<(), &'static str> {
        match (self.artifacts.mode, self.artifacts.window) {
            (ArtifactPolicyMode::Window, Some(window)) => window.validate()?,
            (ArtifactPolicyMode::Window, None) => {
                return Err("windowed artifacts require an artifact window");
            }
            (_, Some(_)) => return Err("an artifact window is valid only in window mode"),
            _ => {}
        }
        match (self.output.mode, self.output.window) {
            (OutputPolicyMode::Window, Some(window)) => window.validate()?,
            (OutputPolicyMode::Window, None) => {
                return Err("windowed output requires an output window");
            }
            (_, Some(_)) => return Err("an output window is valid only in window mode"),
            _ => {}
        }
        let delivery_enabled = !matches!(self.delivery.mode, DeliveryPolicyMode::None);
        if delivery_enabled && (self.delivery.max_bytes == 0 || self.delivery.max_age_seconds == 0)
        {
            return Err("delivery byte and age limits must be greater than zero");
        }
        let pruning = self.delivery.pruning;
        if delivery_enabled
            && (pruning.interval_seconds == 0
                || pruning.minimum_batch_blocks == 0
                || pruning.minimum_batch_changes == 0
                || pruning.maximum_delete_changes == 0)
        {
            return Err("delivery pruning batch limits and interval must be greater than zero");
        }
        if !delivery_enabled && !self.delivery.consumers.is_empty() {
            return Err("delivery consumers require delivery to be enabled");
        }
        let mut consumer_ids = std::collections::BTreeSet::new();
        for consumer in &self.delivery.consumers {
            if consumer.id.is_empty()
                || consumer.id.len() > 128
                || !consumer
                    .id
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
                || !consumer_ids.insert(&consumer.id)
            {
                return Err("delivery consumers need unique portable IDs");
            }
            if consumer.lease_ttl_seconds == 0
                || std::time::Duration::from_secs(consumer.lease_ttl_seconds)
                    > MAXIMUM_CONSUMER_LEASE_TTL
            {
                return Err("delivery consumer lease TTLs must be non-zero and at most 100 years");
            }
            if consumer.required
                && !matches!(self.delivery.mode, DeliveryPolicyMode::UntilAcknowledged)
            {
                return Err("required consumers need until_acknowledged delivery");
            }
        }
        if matches!(self.delivery.mode, DeliveryPolicyMode::UntilAcknowledged)
            && !self
                .delivery
                .consumers
                .iter()
                .any(|consumer| consumer.required)
        {
            return Err("until_acknowledged delivery requires a required durable consumer");
        }
        if matches!(publication, PublicationPolicy::IncludedAndFinalized)
            && matches!(self.undo.mode, UndoPolicyMode::None)
        {
            return Err("included publication requires an unfinalized undo policy");
        }
        if matches!(self.checkpoint.mode, CheckpointPolicyMode::Automatic)
            && self.checkpoint.keep == 0
        {
            return Err("automatic checkpoints must retain at least one checkpoint");
        }
        // The store always persists processor state, and only the checkpoint
        // policy takes checkpoints. Reject the modes that promise otherwise.
        match self.state.mode {
            StatePolicyMode::Ephemeral => {
                return Err(
                    "ephemeral processor state is not implemented; use durable or checkpointed state",
                );
            }
            StatePolicyMode::Checkpointed
                if !matches!(self.checkpoint.mode, CheckpointPolicyMode::Automatic) =>
            {
                return Err("checkpointed processor state requires automatic checkpoints");
            }
            StatePolicyMode::Durable | StatePolicyMode::Checkpointed => {}
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DataRequirement {
    pub capabilities: CapabilitySet,
    pub log_fields: LogFieldSet,
    pub allow_filtered: bool,
    pub filter: FilterScope,
    pub minimum_finality: Finality,
}

impl DataRequirement {
    /// Check frame-level material and finality requirements.
    ///
    /// When the requirement accepts filtered material, every filtered
    /// component that can supply one of its capabilities, directly or by
    /// derivation, must claim predicate completeness rather than
    /// [`Completeness::Partial`], and its scope must cover [`Self::filter`] at
    /// the frame's block ([`FilterScope::covers_at`]). Filtered components for
    /// other capabilities are not inspected.
    ///
    /// # Errors
    ///
    /// Returns a static reason when the frame cannot be passed to the mapper.
    pub fn validate_frame(&self, frame: &BlockFrame) -> Result<(), &'static str> {
        if !frame.finality.satisfies(self.minimum_finality) {
            return Err("frame does not meet minimum finality");
        }
        if !frame
            .capabilities()
            .satisfies(self.capabilities, self.allow_filtered)
        {
            return Err("frame does not meet capability completeness");
        }
        if self.allow_filtered {
            self.validate_filtered_material(frame)?;
        }
        if frame.verification.has_failures() {
            return Err("frame contains failed verification");
        }
        if self.log_fields.contains(LogField::TransactionHash)
            && (frame
                .logs
                .as_present()
                .is_some_and(|logs| logs.iter().any(|log| log.transaction_hash.is_none()))
                || frame.receipts.as_present().is_some_and(|receipts| {
                    receipts.iter().any(|receipt| {
                        receipt
                            .logs
                            .iter()
                            .any(|log| log.transaction_hash.is_none())
                    })
                }))
        {
            return Err("frame logs omit a required transaction hash");
        }
        Ok(())
    }

    fn validate_filtered_material(&self, frame: &BlockFrame) -> Result<(), &'static str> {
        let components = [
            (Capability::Header, filter_claim(&frame.header)),
            (Capability::Transactions, filter_claim(&frame.transactions)),
            (Capability::Receipts, filter_claim(&frame.receipts)),
            (Capability::Logs, filter_claim(&frame.logs)),
            (Capability::Withdrawals, filter_claim(&frame.withdrawals)),
            (Capability::BlobSidecars, filter_claim(&frame.blob_sidecars)),
            (Capability::Traces, filter_claim(&frame.traces)),
            (Capability::StateDiffs, filter_claim(&frame.state_diffs)),
        ];
        for (capability, claim) in components {
            let Some((scope, completeness)) = claim else {
                continue;
            };
            let supplies = CapabilitySet::of(capability).with_derivable();
            if supplies.intersection(self.capabilities) == CapabilitySet::NONE {
                continue;
            }
            if completeness == Completeness::Partial {
                return Err("filtered frame material is only a partial projection");
            }
            if !scope.covers_at(&self.filter, frame.block.number) {
                return Err("filtered frame material does not cover the requirement filter");
            }
        }
        Ok(())
    }
}

/// The predicate and completeness claim of filtered material.
fn filter_claim<T>(material: &Material<T>) -> Option<(&FilterScope, Completeness)> {
    match material {
        Material::Filtered {
            scope,
            completeness,
            ..
        } => Some((scope, *completeness)),
        Material::Complete(_) | Material::Missing(_) => None,
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProcessorSchemas {
    pub delta_version: u16,
    pub entity_schema: String,
    pub change_schema: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ProcessorDescriptor {
    pub id: ProcessorId,
    pub instance: ProcessorInstanceId,
    pub version: Version,
    pub code_hash: BlockHash,
    pub config_hash: BlockHash,
    pub start: StartPoint,
    pub requirements: Vec<DataRequirement>,
    pub mode: ReductionMode,
    pub delivery_ordering: DeliveryOrdering,
    pub publication: PublicationPolicy,
    pub lifecycle: LifecyclePolicies,
    pub schemas: ProcessorSchemas,
}

impl ProcessorDescriptor {
    /// Validate static descriptor invariants.
    ///
    /// # Errors
    ///
    /// Returns a static reason for empty requirements or schema identifiers.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.requirements.is_empty() {
            return Err("processor must declare at least one data requirement");
        }
        if self.schemas.delta_version == 0 {
            return Err("delta schema version must be greater than zero");
        }
        if self.schemas.entity_schema.is_empty() || self.schemas.change_schema.is_empty() {
            return Err("processor entity and change schemas must not be empty");
        }
        if self.requirements.iter().any(|requirement| {
            requirement.log_fields != LogFieldSet::NONE
                && !requirement
                    .capabilities
                    .with_derivable()
                    .contains(leani_primitives::Capability::Logs)
        }) {
            return Err("optional log fields require logs or receipt material");
        }
        if self.delivery_ordering == DeliveryOrdering::BlockVersionedIdempotent
            && self.mode != ReductionMode::BlockLocal
        {
            return Err("block-versioned delivery requires a block-local processor");
        }
        self.lifecycle.validate(self.publication)?;
        Ok(())
    }
}

impl<'de> Deserialize<'de> for ProcessorDescriptor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct DescriptorWire {
            id: ProcessorId,
            #[serde(default)]
            instance: Option<ProcessorInstanceId>,
            version: Version,
            code_hash: BlockHash,
            config_hash: BlockHash,
            start: StartPoint,
            requirements: Vec<DataRequirement>,
            mode: ReductionMode,
            delivery_ordering: DeliveryOrdering,
            publication: PublicationPolicy,
            #[serde(default)]
            lifecycle: Option<LifecyclePolicies>,
            #[serde(default)]
            retention: Option<RetentionPolicy>,
            schemas: ProcessorSchemas,
        }

        let wire = DescriptorWire::deserialize(deserializer)?;
        let instance = match wire.instance {
            Some(instance) => instance,
            None => ProcessorInstanceId::legacy(&wire.id, &wire.version, wire.config_hash)
                .map_err(serde::de::Error::custom)?,
        };
        let lifecycle = wire.lifecycle.unwrap_or_else(|| {
            LifecyclePolicies::from_legacy(
                wire.retention.unwrap_or(RetentionPolicy::FullOutputHistory),
            )
        });
        Ok(Self {
            id: wire.id,
            instance,
            version: wire.version,
            code_hash: wire.code_hash,
            config_hash: wire.config_hash,
            start: wire.start,
            requirements: wire.requirements,
            mode: wire.mode,
            delivery_ordering: wire.delivery_ordering,
            publication: wire.publication,
            lifecycle,
            schemas: wire.schemas,
        })
    }
}

#[cfg(test)]
mod tests {
    use leani_primitives::{
        Address, BlockHash, BlockRange, BlockRef, Capability, ChainId, Completeness,
        HeaderEnvelope, Log, Material, MissingReason, ReceiptEnvelope, TransactionHash,
        VerificationReport,
    };

    use super::*;

    #[test]
    fn processor_instance_rejects_axum_capture_syntax_at_every_decode_boundary() {
        assert!(ProcessorInstanceId::new(":main").is_err());
        assert!(ProcessorInstanceId::new(".").is_err());
        assert!(ProcessorInstanceId::new("..").is_err());
        assert!(serde_json::from_str::<ProcessorInstanceId>(r#"":main""#).is_err());
        assert!(ProcessorInstanceId::new("main@1.0.0:abcd").is_ok());
    }

    #[test]
    fn complete_requirement_rejects_filtered_material() {
        let frame = BlockFrame {
            chain_id: ChainId(1),
            block: BlockRef {
                number: BlockNumber(1),
                hash: BlockHash::new([1; 32]),
                parent_hash: BlockHash::ZERO,
                timestamp: 1,
            },
            finality: Finality::Finalized,
            header: Material::Missing(MissingReason::NotRequested),
            transactions: Material::Missing(MissingReason::NotRequested),
            receipts: Material::Missing(MissingReason::NotRequested),
            logs: Material::Filtered {
                value: Vec::new(),
                scope: FilterScope::default(),
                completeness: leani_primitives::Completeness::DatasetDeclared,
            },
            withdrawals: Material::Missing(MissingReason::Unsupported),
            blob_sidecars: Material::Missing(MissingReason::Unsupported),
            traces: Material::Missing(MissingReason::Unsupported),
            state_diffs: Material::Missing(MissingReason::Unsupported),
            provenance: Vec::new(),
            verification: VerificationReport::default(),
        };
        let requirement = DataRequirement {
            capabilities: CapabilitySet::of(Capability::Logs),
            log_fields: LogFieldSet::NONE,
            allow_filtered: false,
            filter: FilterScope::default(),
            minimum_finality: Finality::Included,
        };
        assert_eq!(
            requirement.validate_frame(&frame),
            Err("frame does not meet capability completeness")
        );
    }

    #[test]
    fn receipt_requirement_rejects_missing_log_transaction_hashes() {
        let frame = BlockFrame {
            chain_id: ChainId(1),
            block: BlockRef {
                number: BlockNumber(1),
                hash: BlockHash::new([1; 32]),
                parent_hash: BlockHash::ZERO,
                timestamp: 1,
            },
            finality: Finality::Finalized,
            header: Material::Missing(MissingReason::NotRequested),
            transactions: Material::Missing(MissingReason::NotRequested),
            receipts: Material::Complete(vec![ReceiptEnvelope {
                transaction_hash: TransactionHash::new([2; 32]),
                transaction_type: 2,
                transaction_index: 0,
                encoded: None,
                success: Some(true),
                gas_used: Some(21_000),
                effective_gas_price: None,
                blob_gas_used: None,
                blob_gas_price: None,
                logs: vec![Log {
                    address: Address::new([3; 20]),
                    topics: Vec::new(),
                    data: Vec::new(),
                    transaction_hash: None,
                    transaction_index: 0,
                    log_index: 0,
                }],
            }]),
            logs: Material::Missing(MissingReason::NotRequested),
            withdrawals: Material::Missing(MissingReason::NotRequested),
            blob_sidecars: Material::Missing(MissingReason::NotRequested),
            traces: Material::Missing(MissingReason::NotRequested),
            state_diffs: Material::Missing(MissingReason::NotRequested),
            provenance: Vec::new(),
            verification: VerificationReport::default(),
        };
        let requirement = DataRequirement {
            capabilities: CapabilitySet::of(Capability::Receipts),
            log_fields: LogFieldSet::of(LogField::TransactionHash),
            allow_filtered: false,
            filter: FilterScope::default(),
            minimum_finality: Finality::Finalized,
        };

        assert_eq!(
            requirement.validate_frame(&frame),
            Err("frame logs omit a required transaction hash")
        );
    }

    fn empty_frame() -> BlockFrame {
        BlockFrame {
            chain_id: ChainId(1),
            block: BlockRef {
                number: BlockNumber(1),
                hash: BlockHash::new([1; 32]),
                parent_hash: BlockHash::ZERO,
                timestamp: 1,
            },
            finality: Finality::Finalized,
            header: Material::Missing(MissingReason::NotRequested),
            transactions: Material::Missing(MissingReason::NotRequested),
            receipts: Material::Missing(MissingReason::NotRequested),
            logs: Material::Missing(MissingReason::NotRequested),
            withdrawals: Material::Missing(MissingReason::NotRequested),
            blob_sidecars: Material::Missing(MissingReason::NotRequested),
            traces: Material::Missing(MissingReason::NotRequested),
            state_diffs: Material::Missing(MissingReason::NotRequested),
            provenance: Vec::new(),
            verification: VerificationReport::default(),
        }
    }

    fn address_scope(addresses: &[u8]) -> FilterScope {
        FilterScope {
            addresses: addresses
                .iter()
                .map(|byte| Address::new([*byte; 20]))
                .collect(),
            ..FilterScope::default()
        }
    }

    fn filtered<T>(value: T, scope: FilterScope, completeness: Completeness) -> Material<T> {
        Material::Filtered {
            value,
            scope,
            completeness,
        }
    }

    fn filtered_log_requirement(addresses: &[u8]) -> DataRequirement {
        DataRequirement {
            capabilities: CapabilitySet::of(Capability::Logs),
            log_fields: LogFieldSet::NONE,
            allow_filtered: true,
            filter: address_scope(addresses),
            minimum_finality: Finality::Included,
        }
    }

    #[test]
    fn filtered_material_must_be_predicate_complete_and_cover_the_requirement_filter() {
        let requirement = filtered_log_requirement(&[0xaa]);
        let mut frame = empty_frame();

        // Audit probe: another consumer's partial projection was accepted.
        frame.logs = filtered(Vec::new(), address_scope(&[0xbb]), Completeness::Partial);
        assert!(requirement.validate_frame(&frame).is_err());

        frame.logs = filtered(
            Vec::new(),
            address_scope(&[0xbb]),
            Completeness::VerifiedPredicate,
        );
        assert_eq!(
            requirement.validate_frame(&frame),
            Err("filtered frame material does not cover the requirement filter")
        );

        frame.logs = filtered(
            Vec::new(),
            address_scope(&[0xaa, 0xbb]),
            Completeness::Partial,
        );
        assert_eq!(
            requirement.validate_frame(&frame),
            Err("filtered frame material is only a partial projection")
        );

        frame.logs = filtered(
            Vec::new(),
            address_scope(&[0xbb, 0xaa]),
            Completeness::DatasetDeclared,
        );
        assert_eq!(requirement.validate_frame(&frame), Ok(()));

        frame.logs = filtered(
            Vec::new(),
            FilterScope::default(),
            Completeness::VerifiedPredicate,
        );
        assert_eq!(requirement.validate_frame(&frame), Ok(()));

        frame.logs = Material::Complete(Vec::new());
        assert_eq!(requirement.validate_frame(&frame), Ok(()));

        let wildcard = filtered_log_requirement(&[]);
        frame.logs = filtered(
            Vec::new(),
            address_scope(&[0xaa]),
            Completeness::VerifiedPredicate,
        );
        assert_eq!(
            wildcard.validate_frame(&frame),
            Err("filtered frame material does not cover the requirement filter")
        );
    }

    #[test]
    fn filtered_receipts_that_derive_logs_must_cover_the_requirement_filter() {
        let requirement = filtered_log_requirement(&[0xaa]);
        let mut frame = empty_frame();
        frame.receipts = filtered(
            Vec::new(),
            address_scope(&[0xbb]),
            Completeness::VerifiedPredicate,
        );
        assert_eq!(
            requirement.validate_frame(&frame),
            Err("filtered frame material does not cover the requirement filter")
        );

        // Covering logs do not excuse other non-covering material a mapper
        // could read for the same capability.
        frame.logs = filtered(
            Vec::new(),
            address_scope(&[0xaa]),
            Completeness::VerifiedPredicate,
        );
        assert_eq!(
            requirement.validate_frame(&frame),
            Err("filtered frame material does not cover the requirement filter")
        );

        frame.receipts = Material::Missing(MissingReason::NotRequested);
        assert_eq!(requirement.validate_frame(&frame), Ok(()));
    }

    #[test]
    fn filtered_material_the_requirement_does_not_read_is_ignored() {
        let requirement = filtered_log_requirement(&[0xaa]);
        let mut frame = empty_frame();
        frame.logs = Material::Complete(Vec::new());
        frame.transactions = filtered(Vec::new(), address_scope(&[0xbb]), Completeness::Partial);
        frame.header = filtered(
            HeaderEnvelope {
                rlp: None,
                transactions_root: None,
                receipts_root: None,
                withdrawals_root: None,
                gas_limit: None,
                gas_used: None,
                base_fee_per_gas: None,
                blob_gas_used: None,
                excess_blob_gas: None,
                size_bytes: None,
                transaction_count: None,
                consensus_size_bytes: None,
            },
            address_scope(&[0xbb]),
            Completeness::Partial,
        );
        assert_eq!(requirement.validate_frame(&frame), Ok(()));
    }

    #[test]
    fn filtered_material_is_compared_at_the_frame_block() {
        let requirement = filtered_log_requirement(&[0xaa]);
        let mut frame = empty_frame();
        let mut scope = address_scope(&[0xaa]);
        scope.block_range = Some(BlockRange::single(frame.block.number));
        frame.logs = filtered(Vec::new(), scope.clone(), Completeness::DatasetDeclared);
        assert_eq!(requirement.validate_frame(&frame), Ok(()));

        scope.block_range = Some(BlockRange::single(BlockNumber(2)));
        frame.logs = filtered(Vec::new(), scope, Completeness::DatasetDeclared);
        assert_eq!(
            requirement.validate_frame(&frame),
            Err("filtered frame material does not cover the requirement filter")
        );
    }

    #[test]
    fn invalid_ranges_ids_and_capability_bits_do_not_deserialize() {
        // Audit probe: both decoded without validation.
        assert!(serde_json::from_str::<BlockRange>(r#"{"start":9,"end":1}"#).is_err());
        assert!(serde_json::from_str::<ProcessorId>(r#""../bad""#).is_err());
        assert!(serde_json::from_str::<CapabilitySet>("4096").is_err());
        assert!(serde_json::from_str::<LogFieldSet>("2").is_err());

        assert_eq!(
            serde_json::from_str::<BlockRange>(r#"{"start":1,"end":9}"#).expect("range"),
            BlockRange::new(BlockNumber(1), BlockNumber(9)).expect("range")
        );
        assert_eq!(
            serde_json::from_str::<ProcessorId>(r#""fixture""#).expect("ID"),
            ProcessorId::new("fixture").expect("ID")
        );
        assert_eq!(
            serde_json::from_str::<CapabilitySet>("4095").expect("capabilities"),
            CapabilitySet::ALL
        );
        assert_eq!(
            serde_json::from_str::<LogFieldSet>("1").expect("log fields"),
            LogFieldSet::ALL
        );
    }

    #[test]
    fn legacy_instance_derivation_failure_is_a_deserialization_error() {
        let descriptor = |id: &str, version: &str| {
            serde_json::json!({
                "id": id,
                "version": version,
                "code_hash": BlockHash::new([1; 32]),
                "config_hash": BlockHash::new([2; 32]),
                "start": "Genesis",
                "requirements": [{
                    "capabilities": CapabilitySet::of(Capability::Header),
                    "log_fields": LogFieldSet::NONE,
                    "allow_filtered": false,
                    "filter": FilterScope::default(),
                    "minimum_finality": "Included"
                }],
                "mode": "BlockLocal",
                "delivery_ordering": "block_versioned_idempotent",
                "publication": "finalized_only",
                "schemas": {
                    "delta_version": 1,
                    "entity_schema": "fixture.entity.v1",
                    "change_schema": "fixture.change.v1"
                }
            })
        };

        // Audit probe: semver build metadata panicked while deriving the
        // legacy instance.
        let error =
            serde_json::from_value::<ProcessorDescriptor>(descriptor("fixture", "1.0.0+build"))
                .expect_err("build metadata cannot form a legacy instance ID");
        assert!(error.to_string().contains("invalid processor instance ID"));

        let long_version = format!("1.0.0-{}", "a".repeat(64));
        let error = serde_json::from_value::<ProcessorDescriptor>(descriptor(
            &"p".repeat(64),
            &long_version,
        ))
        .expect_err("an over-long legacy instance ID is invalid");
        assert!(error.to_string().contains("invalid processor instance ID"));
    }

    #[test]
    fn artifact_policy_is_independent_and_window_is_bounded() {
        let mut lifecycle = LifecyclePolicies::default();
        assert_eq!(lifecycle.artifacts.mode, ArtifactPolicyMode::None);

        lifecycle.artifacts.mode = ArtifactPolicyMode::Window;
        assert_eq!(
            lifecycle.validate(PublicationPolicy::FinalizedOnly),
            Err("windowed artifacts require an artifact window")
        );

        lifecycle.artifacts.window = Some(ArtifactWindow {
            max_blocks: Some(1_024),
            ..ArtifactWindow::default()
        });
        lifecycle
            .validate(PublicationPolicy::FinalizedOnly)
            .expect("one artifact-window bound is valid");

        lifecycle.artifacts.window = Some(ArtifactWindow {
            max_blocks: Some(1_024),
            max_bytes: Some(1 << 20),
            ..ArtifactWindow::default()
        });
        assert_eq!(
            lifecycle.validate(PublicationPolicy::FinalizedOnly),
            Err("artifact window must declare exactly one block, age, or byte limit")
        );
    }

    #[test]
    fn consumer_lease_ttls_are_bounded_like_the_store_bounds_them() {
        // Task 16b: validation refused only a zero TTL, and the store then
        // refused one over 100 years.
        assert_eq!(
            MAXIMUM_CONSUMER_LEASE_TTL.as_secs(),
            100 * 365 * 24 * 60 * 60
        );
        let mut lifecycle = LifecyclePolicies::default();
        lifecycle.delivery.mode = DeliveryPolicyMode::UntilAcknowledged;
        lifecycle.delivery.consumers = vec![DurableConsumerPolicy {
            id: "destination".to_owned(),
            required: true,
            lease_ttl_seconds: 100 * 365 * 24 * 60 * 60,
        }];
        assert_eq!(lifecycle.validate(PublicationPolicy::FinalizedOnly), Ok(()));
        for lease_ttl_seconds in [100 * 365 * 24 * 60 * 60 + 1, u64::MAX, 0] {
            lifecycle.delivery.consumers[0].lease_ttl_seconds = lease_ttl_seconds;
            assert_eq!(
                lifecycle.validate(PublicationPolicy::FinalizedOnly),
                Err("delivery consumer lease TTLs must be non-zero and at most 100 years"),
                "{lease_ttl_seconds}"
            );
        }
    }

    #[test]
    fn state_modes_without_distinct_persistence_are_rejected() {
        // Audit probe (Processor-6): `ephemeral` validated whenever
        // publication was not included, although the store always persists
        // processor state, and `checkpointed` state validated without
        // checkpoints.
        let mut lifecycle = LifecyclePolicies::default();
        lifecycle.state.mode = StatePolicyMode::Ephemeral;
        assert_eq!(
            lifecycle.validate(PublicationPolicy::FinalizedOnly),
            Err("ephemeral processor state is not implemented; use durable or checkpointed state")
        );
        lifecycle.state.mode = StatePolicyMode::Checkpointed;
        lifecycle.checkpoint.mode = CheckpointPolicyMode::None;
        assert_eq!(
            lifecycle.validate(PublicationPolicy::FinalizedOnly),
            Err("checkpointed processor state requires automatic checkpoints")
        );
        lifecycle.checkpoint.mode = CheckpointPolicyMode::Automatic;
        assert_eq!(lifecycle.validate(PublicationPolicy::FinalizedOnly), Ok(()));
        lifecycle.state.mode = StatePolicyMode::Durable;
        lifecycle.checkpoint.mode = CheckpointPolicyMode::None;
        assert_eq!(lifecycle.validate(PublicationPolicy::FinalizedOnly), Ok(()));
    }

    #[test]
    fn legacy_retention_descriptor_normalizes_to_lifecycle_policies() {
        let input = serde_json::json!({
            "id": "fixture",
            "version": "1.0.0",
            "code_hash": BlockHash::new([1; 32]),
            "config_hash": BlockHash::new([2; 32]),
            "start": "Genesis",
            "requirements": [{
                "capabilities": CapabilitySet::of(Capability::Header),
                "log_fields": LogFieldSet::NONE,
                "allow_filtered": false,
                "filter": FilterScope::default(),
                "minimum_finality": "Included"
            }],
            "mode": "BlockLocal",
            "delivery_ordering": "block_versioned_idempotent",
            "publication": "finalized_only",
            "retention": "LatestState",
            "schemas": {
                "delta_version": 1,
                "entity_schema": "fixture.entity.v1",
                "change_schema": "fixture.change.v1"
            }
        });
        let descriptor: ProcessorDescriptor =
            serde_json::from_value(input).expect("legacy descriptor");
        assert_eq!(descriptor.lifecycle.output.mode, OutputPolicyMode::Latest);
        assert_eq!(
            descriptor.instance,
            ProcessorInstanceId::legacy(
                &descriptor.id,
                &descriptor.version,
                descriptor.config_hash
            )
            .expect("legacy instance")
        );
    }
}
