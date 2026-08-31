//! Strict, versioned node configuration.

use std::{
    collections::HashSet,
    fmt, fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    str::FromStr,
};

use leani_processor_api::{
    ArtifactPolicy, ArtifactPolicyMode, ArtifactWindow, CheckpointPolicy, CheckpointPolicyMode,
    DeliveryLimitAction, DeliveryPolicy, DeliveryPolicyMode, DeliveryPruningPolicy,
    DurableConsumerPolicy, LifecyclePolicies, OutputPolicy, OutputPolicyMode, OutputWindow,
    PublicationPolicy, StatePolicy, StatePolicyMode, UndoPolicy, UndoPolicyMode,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;
use url::Url;

/// Configuration schema understood by this binary.
pub const CONFIG_VERSION: u32 = 1;

/// Configuration loaded from TOML.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub config_version: u32,
    pub data_dir: PathBuf,
    pub chain: ChainConfig,
    #[serde(default)]
    pub raw_history: RawHistoryConfig,
    #[serde(default)]
    pub artifact_storage: ArtifactStorageConfig,
    pub budgets: BudgetConfig,
    pub sources: SourcesConfig,
    pub finality: FinalityConfig,
    pub processors: Vec<ProcessorConfig>,
    pub rpc: RpcConfig,
    pub api: ApiConfig,
}

/// Compact product-level configuration expanded into [`Config`] before
/// validation. The `network` field deliberately discriminates this document
/// from the fully explicit advanced schema.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StarterConfig {
    pub config_version: u32,
    pub network: StarterNetwork,
    #[serde(default = "default_starter_data_dir")]
    pub data_dir: PathBuf,
    pub finality: StarterFinalityConfig,
    pub uniswap: StarterUniswapConfig,
    #[serde(default)]
    pub api: StarterApiConfig,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum StarterNetwork {
    EthereumMainnet,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StarterFinalityConfig {
    pub checkpoint: String,
    pub checkpoint_slot: u64,
    pub endpoints: Vec<Url>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StarterUniswapConfig {
    pub markets: Vec<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StarterApiConfig {
    #[serde(default = "default_starter_api_bind")]
    pub bind: SocketAddr,
}

impl Default for StarterApiConfig {
    fn default() -> Self {
        Self {
            bind: default_starter_api_bind(),
        }
    }
}

fn default_starter_api_bind() -> SocketAddr {
    "127.0.0.1:18080"
        .parse()
        .expect("built-in API bind is valid")
}

fn default_starter_data_dir() -> PathBuf {
    PathBuf::from("./data")
}

impl StarterConfig {
    pub(crate) fn uniswap(
        data_dir: PathBuf,
        markets: Vec<String>,
        checkpoint: String,
        checkpoint_slot: u64,
        endpoints: Vec<Url>,
    ) -> Self {
        Self {
            config_version: CONFIG_VERSION,
            network: StarterNetwork::EthereumMainnet,
            data_dir,
            finality: StarterFinalityConfig {
                checkpoint,
                checkpoint_slot,
                endpoints,
            },
            uniswap: StarterUniswapConfig { markets },
            api: StarterApiConfig::default(),
        }
    }

    fn expand(self) -> Result<Config, String> {
        let mut config: Config = toml::from_str(include_str!(
            "../../../config/defaults/ethereum-mainnet-uniswap.toml"
        ))
        .map_err(|error| format!("built-in Ethereum Mainnet defaults are invalid: {error}"))?;
        let markets = crate::uniswap_markets::resolve_markets(&self.uniswap.markets)
            .map_err(|error| error.to_string())?;
        let processor =
            crate::uniswap_markets::processor_config(&markets, "uniswap-observations", false)
                .map_err(|error| error.to_string())?;

        config.config_version = self.config_version;
        config.data_dir = self.data_dir;
        config.finality.checkpoint = self.finality.checkpoint;
        config.finality.checkpoint_slot = self.finality.checkpoint_slot;
        config.finality.endpoints = self.finality.endpoints;
        config.processors = vec![processor];
        config.api.bind = self.api.bind;
        Ok(config)
    }
}

/// Physical backend for immutable finalized processor artifacts. Lifecycle
/// ownership remains processor-local and independent of this representation.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactStorageBackend {
    /// Keep each immutable artifact as one `SQLite` KV row.
    #[default]
    Sqlite,
    /// Use `SQLite` as a durable write buffer and compact closed ranges into
    /// checksummed seekable segment files.
    TieredSegments,
}

/// Node-wide immutable artifact storage policy.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactStorageConfig {
    #[serde(default)]
    pub backend: ArtifactStorageBackend,
    #[serde(default = "default_artifact_segment_blocks")]
    pub segment_target_blocks: u64,
    #[serde(default = "default_artifact_segment_compression")]
    pub compression: leani_store_artifacts::ArtifactCompression,
    #[serde(default = "default_artifact_maximum_item_bytes")]
    pub maximum_artifact_logical_bytes: HumanBytes,
    #[serde(default = "default_artifact_maximum_segment_logical_bytes")]
    pub maximum_segment_logical_bytes: HumanBytes,
    #[serde(default = "default_artifact_maximum_segment_physical_bytes")]
    pub maximum_segment_physical_bytes: HumanBytes,
    #[serde(default = "default_artifact_compaction_interval")]
    pub compaction_interval: HumanMilliseconds,
    #[serde(default = "default_artifact_compaction_segments_per_cycle")]
    pub maximum_segments_per_cycle: usize,
}

impl Default for ArtifactStorageConfig {
    fn default() -> Self {
        Self {
            backend: ArtifactStorageBackend::default(),
            segment_target_blocks: default_artifact_segment_blocks(),
            compression: default_artifact_segment_compression(),
            maximum_artifact_logical_bytes: default_artifact_maximum_item_bytes(),
            maximum_segment_logical_bytes: default_artifact_maximum_segment_logical_bytes(),
            maximum_segment_physical_bytes: default_artifact_maximum_segment_physical_bytes(),
            compaction_interval: default_artifact_compaction_interval(),
            maximum_segments_per_cycle: default_artifact_compaction_segments_per_cycle(),
        }
    }
}

/// Independent durable raw-history store. Disabled configurations retain the
/// existing processor/RPC behavior and create no segment catalog.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RawHistoryConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_raw_history_logical_bytes")]
    pub maximum_logical_bytes: HumanBytes,
    #[serde(default = "default_raw_history_physical_bytes")]
    pub maximum_physical_bytes: HumanBytes,
    #[serde(default = "default_raw_history_frame_bytes")]
    pub maximum_frame_logical_bytes: HumanBytes,
    #[serde(default = "default_raw_history_segment_logical_bytes")]
    pub maximum_segment_logical_bytes: HumanBytes,
    #[serde(default = "default_raw_history_segment_physical_bytes")]
    pub maximum_segment_physical_bytes: HumanBytes,
    #[serde(default = "default_raw_history_reader_connections")]
    pub reader_connections: u32,
    #[serde(default = "default_raw_history_source_frames")]
    pub maximum_source_frames: u64,
    #[serde(default = "default_raw_history_buffered_frames")]
    pub maximum_buffered_frames: usize,
}

impl Default for RawHistoryConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            maximum_logical_bytes: default_raw_history_logical_bytes(),
            maximum_physical_bytes: default_raw_history_physical_bytes(),
            maximum_frame_logical_bytes: default_raw_history_frame_bytes(),
            maximum_segment_logical_bytes: default_raw_history_segment_logical_bytes(),
            maximum_segment_physical_bytes: default_raw_history_segment_physical_bytes(),
            reader_connections: default_raw_history_reader_connections(),
            maximum_source_frames: default_raw_history_source_frames(),
            maximum_buffered_frames: default_raw_history_buffered_frames(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ChainConfig {
    pub name: String,
    pub chain_id: u64,
    /// Canonical execution-layer Merge block. Required before the node can
    /// accept the exact post-Merge execution-RPC raw-history profile.
    #[serde(default)]
    pub merge_block: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BudgetConfig {
    pub memory_bytes: u64,
    pub temporary_disk_bytes: u64,
    pub pending_delta_bytes: u64,
    pub recent_raw_soft_bytes: u64,
    pub recent_raw_hard_bytes: u64,
    pub source_concurrency: usize,
    pub mapper_concurrency: usize,
    #[serde(default)]
    pub history_material: HistoryMaterialBudgetConfig,
    #[serde(default)]
    pub history_pipeline: HistoryPipelineBudgetConfig,
    #[serde(default)]
    pub store: StoreBudgetConfig,
    #[serde(default)]
    pub artifacts: ArtifactBudgetConfig,
    #[serde(default)]
    pub delivery: DeliveryBudgetConfig,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StoreBudgetConfig {
    #[serde(default = "default_store_physical_bytes")]
    pub maximum_physical_bytes: HumanBytes,
}

impl Default for StoreBudgetConfig {
    fn default() -> Self {
        Self {
            maximum_physical_bytes: default_store_physical_bytes(),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactBudgetConfig {
    #[serde(default = "default_artifact_retained_bytes")]
    pub maximum_retained_bytes: HumanBytes,
    #[serde(default = "default_artifact_pending_bytes")]
    pub maximum_pending_bytes: HumanBytes,
}

impl Default for ArtifactBudgetConfig {
    fn default() -> Self {
        Self {
            maximum_retained_bytes: default_artifact_retained_bytes(),
            maximum_pending_bytes: default_artifact_pending_bytes(),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryMaterialBudgetConfig {
    #[serde(default)]
    pub mode: HistoryMaterialCoordinatorMode,
    #[serde(default = "default_history_material_memory")]
    pub memory_bytes: HumanBytes,
    #[serde(default = "default_history_material_buffered_frames")]
    pub maximum_buffered_frames_per_acquisition: usize,
    #[serde(default = "default_history_material_minimum_chunk_blocks")]
    pub minimum_physical_chunk_blocks: u64,
    #[serde(default = "default_history_material_overfetch_ratio")]
    pub maximum_overfetch_ratio: f64,
}

impl Default for HistoryMaterialBudgetConfig {
    fn default() -> Self {
        Self {
            mode: HistoryMaterialCoordinatorMode::default(),
            memory_bytes: default_history_material_memory(),
            maximum_buffered_frames_per_acquisition: default_history_material_buffered_frames(),
            minimum_physical_chunk_blocks: default_history_material_minimum_chunk_blocks(),
            maximum_overfetch_ratio: default_history_material_overfetch_ratio(),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryPipelineBudgetConfig {
    #[serde(default = "default_history_pipeline_active_chunks")]
    pub maximum_active_chunks: usize,
    #[serde(default = "default_history_pipeline_mapped_bytes")]
    pub maximum_mapped_bytes: HumanBytes,
    #[serde(default)]
    pub commit: HistoryCommitBudgetConfig,
}

impl Default for HistoryPipelineBudgetConfig {
    fn default() -> Self {
        Self {
            maximum_active_chunks: default_history_pipeline_active_chunks(),
            maximum_mapped_bytes: default_history_pipeline_mapped_bytes(),
            commit: HistoryCommitBudgetConfig::default(),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryBudgetConfig {
    #[serde(default = "default_delivery_retained_bytes")]
    pub maximum_retained_bytes: HumanBytes,
    #[serde(default = "default_delivery_history_retained_bytes")]
    pub maximum_history_retained_bytes: HumanBytes,
}

impl Default for DeliveryBudgetConfig {
    fn default() -> Self {
        Self {
            maximum_retained_bytes: default_delivery_retained_bytes(),
            maximum_history_retained_bytes: default_delivery_history_retained_bytes(),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryCommitBudgetConfig {
    #[serde(default = "default_history_commit_blocks")]
    pub maximum_blocks: usize,
    #[serde(default = "default_history_commit_changes")]
    pub maximum_changes: usize,
    #[serde(default = "default_history_commit_encoded_bytes")]
    pub maximum_encoded_bytes: HumanBytes,
    #[serde(default = "default_history_commit_delay")]
    pub maximum_delay: HumanMilliseconds,
    #[serde(default = "default_history_commit_target_writer_hold")]
    pub target_writer_hold: HumanMilliseconds,
}

impl Default for HistoryCommitBudgetConfig {
    fn default() -> Self {
        Self {
            maximum_blocks: default_history_commit_blocks(),
            maximum_changes: default_history_commit_changes(),
            maximum_encoded_bytes: default_history_commit_encoded_bytes(),
            maximum_delay: default_history_commit_delay(),
            target_writer_hold: default_history_commit_target_writer_hold(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryMaterialCoordinatorMode {
    Disabled,
    Observe,
    #[default]
    Enabled,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SourcesConfig {
    pub history: Vec<HistorySourceConfig>,
    pub live: LiveSourceConfig,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HistorySourceConfig {
    pub id: String,
    pub kind: HistorySourceKind,
    pub priority: u16,
    pub trust: HistoryTrust,
    /// Logical acquisition span. Xatu values must be multiples of its
    /// physical 1,000-block Parquet partition size.
    #[serde(default)]
    pub chunk_blocks: Option<u64>,
    /// Wider Xatu acquisition span for the multi-table blobs projection.
    #[serde(default)]
    pub blobs_chunk_blocks: Option<u64>,
    /// Parquet rows decoded per bounded Arrow batch by Xatu.
    #[serde(default)]
    pub batch_rows: Option<usize>,
    #[serde(default)]
    pub manifest: Option<PathBuf>,
    #[serde(default)]
    pub endpoint: Option<Url>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HistorySourceKind {
    Xatu,
    EraE,
    Parquet,
    Archive,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryTrust {
    TrustedDataset,
    VerifiedMaterial,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LiveSourceConfig {
    pub kind: LiveSourceKind,
    /// Hard availability floor required before execution requests may start.
    pub minimum_peers: usize,
    /// Non-blocking operational target for a healthy peer pool. The manager
    /// continues dialing beyond this value up to `max_outbound_peers`.
    #[serde(default = "default_execution_preferred_peers")]
    pub preferred_peers: usize,
    /// Outbound execution-peer pool maintained for request diversity and
    /// rapid replacement of peers that do not serve required material.
    #[serde(default = "default_execution_max_outbound_peers")]
    pub max_outbound_peers: usize,
    #[serde(default = "default_execution_max_concurrent_dials")]
    pub max_concurrent_dials: usize,
    /// Stable inbound `RLPx` TCP port. Zero uses an ephemeral port.
    #[serde(default)]
    pub listener_port: u16,
    /// UDP port used by execution Discv4. Zero uses an ephemeral port.
    #[serde(default)]
    pub discovery_port: u16,
    /// UDP port used by execution Discv5. Zero uses an independent ephemeral
    /// port.
    #[serde(default)]
    pub discv5_port: u16,
    #[serde(default = "default_true")]
    pub enable_discv5: bool,
    /// Reth NAT resolver (`none`, `any`, `upnp`, `publicip`, `extip:<ip>`, ...).
    #[serde(default = "default_execution_nat")]
    pub nat: String,
    /// Optional stable `enode://` seeds. They supplement public discovery and
    /// are not an allowlist.
    #[serde(default)]
    pub trusted_peers: Vec<String>,
    #[serde(default = "default_execution_peer_refill_interval_ms")]
    pub peer_refill_interval_ms: u64,
    /// Recreate the execution network manager after this long with zero
    /// connected peers so discovery recovers from stale sockets or bootstrap
    /// failures.
    #[serde(default = "default_execution_peer_recovery_timeout_seconds")]
    pub peer_recovery_timeout_seconds: u64,
    /// Concurrent adaptive body/receipt requests. The runtime further bounds
    /// this by connected peers and source budgets.
    #[serde(default = "default_execution_material_request_concurrency")]
    pub material_request_concurrency: usize,
    /// Adaptive maximum blocks carried by one execution body/receipt request.
    #[serde(default = "default_execution_material_request_blocks")]
    pub material_request_blocks: usize,
    /// Initial deadline for one execution-peer request. Reth adapts this per
    /// peer after successful responses.
    #[serde(default = "default_execution_request_timeout_seconds")]
    pub request_timeout_seconds: u64,
    /// Maximum attempts for one header, body, or receipt request before the
    /// containing operation rotates or fails over.
    #[serde(default = "default_execution_request_retries")]
    pub request_retries: usize,
    /// Delay between request attempts while another peer becomes available.
    #[serde(default = "default_execution_request_retry_backoff_ms")]
    pub request_retry_backoff_ms: u64,
    /// Retry the persistent peer pool until node cancellation. Disable only for
    /// bounded diagnostics.
    #[serde(default = "default_execution_persistent_retries")]
    pub persistent_retries: bool,
    #[serde(default = "default_execution_retry_backoff_max_seconds")]
    pub retry_backoff_max_seconds: u64,
    #[serde(default = "default_execution_peer_cache_flush_seconds")]
    pub peer_cache_flush_seconds: u64,
    #[serde(default = "default_execution_peer_cache_max_entries")]
    pub peer_cache_max_entries: usize,
    /// Finalized processor-delta checksums compared per archive catch-up
    /// batch. Raw live frames need not be retained for this audit.
    #[serde(default = "default_archive_reconciliation_blocks")]
    pub archive_reconciliation_blocks: u64,
    #[serde(default = "default_archive_reconciliation_interval_seconds")]
    pub archive_reconciliation_interval_seconds: u64,
    /// Optional maximum finalized suffix that may be filled directly from
    /// execution P2P after configured durable historical sources fail. When
    /// omitted, every gap at or after the processor start block is eligible;
    /// P2P material availability remains probabilistic.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub history_fallback_blocks: Option<u64>,
    /// Concurrent low-priority header requests used to prove a historical
    /// fallback range against the finalized execution anchor.
    #[serde(default = "default_history_header_request_concurrency")]
    pub history_header_request_concurrency: usize,
    /// Headers requested per proof response, bounded by the ETH protocol.
    #[serde(default = "default_history_header_request_blocks")]
    pub history_header_request_blocks: u64,
    /// Inclusive execution blocks ending at the verified finality anchor that
    /// are fetched through P2P and reconciled with historical coverage.
    #[serde(default = "default_handoff_overlap_blocks")]
    pub handoff_overlap_blocks: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LiveSourceKind {
    P2p,
    Disabled,
}

impl LiveSourceConfig {
    /// Earliest block eligible for finalized execution-P2P fallback.
    ///
    /// An omitted bound covers the processor's complete configured range. An
    /// explicit bound limits eligibility to that many blocks ending at the
    /// finalized anchor.
    #[must_use]
    pub fn history_fallback_start(&self, finalized_anchor: u64, processor_start: u64) -> u64 {
        self.history_fallback_blocks
            .map_or(processor_start, |blocks| {
                finalized_anchor
                    .saturating_sub(blocks.saturating_sub(1))
                    .max(processor_start)
            })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FinalityConfig {
    pub kind: FinalitySourceKind,
    pub checkpoint: String,
    #[serde(default)]
    pub checkpoint_slot: u64,
    #[serde(default)]
    pub endpoints: Vec<Url>,
    #[serde(default = "default_minimum_agreement")]
    pub minimum_agreement: usize,
    #[serde(default)]
    pub bootnodes: Vec<String>,
    #[serde(default = "default_finality_minimum_peers")]
    pub minimum_peers: usize,
    #[serde(default)]
    pub discovery_port: u16,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FinalitySourceKind {
    BeaconApi,
    ConsensusP2p,
    Disabled,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessorConfig {
    /// Processor kind registered by the native/package factory.
    pub id: String,
    /// Stable operator-selected identity.
    pub instance: String,
    pub version: String,
    /// Historical scheduling policy for this processor. Live canonical
    /// following remains independent and is always configured by the live
    /// source lane.
    #[serde(default)]
    pub history_control: ProcessorHistoryControl,
    #[serde(default)]
    pub history_mode: ProcessorHistoryMode,
    /// For on-demand/recompute work, fail before mapping unless the complete
    /// request can be satisfied from the durable local raw catalog.
    #[serde(default)]
    pub require_retained_input: bool,
    pub start_block: u64,
    pub publish: PublishMode,
    pub state: StatePolicyConfig,
    #[serde(default)]
    pub artifacts: ArtifactPolicyConfig,
    pub output: OutputPolicyConfig,
    pub delivery: DeliveryPolicyConfig,
    pub checkpoint: CheckpointPolicyConfig,
    pub undo: UndoPolicyConfig,
    #[serde(default)]
    pub coverage: ProcessorCoverageConfig,
    #[serde(default)]
    pub settings: toml::Table,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessorCoverageConfig {
    #[serde(default = "default_coverage_verification_segment_blocks")]
    pub verification_segment_blocks: u64,
}

impl Default for ProcessorCoverageConfig {
    fn default() -> Self {
        Self {
            verification_segment_blocks: default_coverage_verification_segment_blocks(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessorHistoryMode {
    #[default]
    Automatic,
    OnDemand,
}

/// Single durable authority allowed to create historical work for a processor
/// instance.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessorHistoryControl {
    #[default]
    NodeOwned,
    ApplicationSubscriptions,
}

impl ProcessorConfig {
    /// Decode the processor-owned settings table using its strict native or
    /// packaged configuration type.
    ///
    /// # Errors
    ///
    /// Returns the TOML decoding error without performing I/O.
    pub fn decode_settings<T: DeserializeOwned>(&self) -> Result<T, toml::de::Error> {
        toml::Value::Table(self.settings.clone()).try_into()
    }

    /// Build the processor's orthogonal lifecycle contract.
    ///
    /// # Errors
    ///
    pub fn lifecycle_policies(&self) -> Result<LifecyclePolicies, &'static str> {
        Ok(LifecyclePolicies {
            state: self.state.into(),
            artifacts: self.artifacts.into(),
            output: self.output.into(),
            delivery: (&self.delivery).into(),
            checkpoint: self.checkpoint.into(),
            undo: self.undo.into(),
        })
    }

    #[must_use]
    pub fn publication_policy(&self) -> PublicationPolicy {
        self.publish.into()
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactWindowConfig {
    #[serde(default)]
    pub max_blocks: Option<u64>,
    #[serde(default)]
    pub max_age: Option<HumanDuration>,
    #[serde(default)]
    pub max_bytes: Option<HumanBytes>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactPolicyConfig {
    #[serde(default)]
    pub mode: ArtifactPolicyMode,
    #[serde(default)]
    pub window: Option<ArtifactWindowConfig>,
}

impl From<ArtifactPolicyConfig> for ArtifactPolicy {
    fn from(value: ArtifactPolicyConfig) -> Self {
        Self {
            mode: value.mode,
            window: value.window.map(|window| ArtifactWindow {
                max_blocks: window.max_blocks,
                max_age_seconds: window.max_age.map(HumanDuration::seconds),
                max_bytes: window.max_bytes.map(HumanBytes::bytes),
            }),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PublishMode {
    OptimisticAndFinalized,
    FinalizedOnly,
    TerminalOnly,
}

impl From<PublishMode> for PublicationPolicy {
    fn from(value: PublishMode) -> Self {
        match value {
            PublishMode::OptimisticAndFinalized => Self::OptimisticAndFinalized,
            PublishMode::FinalizedOnly => Self::FinalizedOnly,
            PublishMode::TerminalOnly => Self::TerminalOnly,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StatePolicyConfig {
    pub mode: StatePolicyMode,
}

impl From<StatePolicyConfig> for StatePolicy {
    fn from(value: StatePolicyConfig) -> Self {
        Self { mode: value.mode }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OutputWindowConfig {
    #[serde(default)]
    pub max_blocks: Option<u64>,
    #[serde(default)]
    pub max_age: Option<HumanDuration>,
    #[serde(default)]
    pub max_rows: Option<u64>,
    #[serde(default)]
    pub max_bytes: Option<HumanBytes>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OutputPolicyConfig {
    pub mode: OutputPolicyMode,
    #[serde(default)]
    pub window: Option<OutputWindowConfig>,
    #[serde(default)]
    pub finalized_only: bool,
}

impl From<OutputPolicyConfig> for OutputPolicy {
    fn from(value: OutputPolicyConfig) -> Self {
        Self {
            mode: value.mode,
            window: value.window.map(|window| OutputWindow {
                max_blocks: window.max_blocks,
                max_age_seconds: window.max_age.map(HumanDuration::seconds),
                max_rows: window.max_rows,
                max_bytes: window.max_bytes.map(HumanBytes::bytes),
            }),
            finalized_only: value.finalized_only,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryConsumerConfig {
    pub id: String,
    pub required: bool,
    pub lease_ttl: HumanDuration,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryPruningConfig {
    pub interval: HumanDuration,
    pub minimum_batch_blocks: u64,
    pub minimum_batch_changes: u64,
    pub maximum_delete_changes: u64,
    pub retain_finalized_blocks: u64,
    pub retain_acknowledged_age: HumanDuration,
}

impl Default for DeliveryPruningConfig {
    fn default() -> Self {
        Self {
            interval: HumanDuration(30),
            minimum_batch_blocks: 64,
            minimum_batch_changes: 10_000,
            maximum_delete_changes: 10_000,
            retain_finalized_blocks: 256,
            retain_acknowledged_age: HumanDuration(60 * 60),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryPolicyConfig {
    pub mode: DeliveryPolicyMode,
    #[serde(default = "default_delivery_max_bytes")]
    pub max_bytes: HumanBytes,
    #[serde(default = "default_delivery_max_age")]
    pub max_age: HumanDuration,
    #[serde(default)]
    pub on_limit: DeliveryLimitAction,
    #[serde(default)]
    pub pruning: DeliveryPruningConfig,
    #[serde(default)]
    pub consumers: Vec<DeliveryConsumerConfig>,
}

impl From<&DeliveryPolicyConfig> for DeliveryPolicy {
    fn from(value: &DeliveryPolicyConfig) -> Self {
        Self {
            mode: value.mode,
            max_bytes: value.max_bytes.bytes(),
            max_age_seconds: value.max_age.seconds(),
            on_limit: value.on_limit,
            pruning: DeliveryPruningPolicy {
                interval_seconds: value.pruning.interval.seconds(),
                minimum_batch_blocks: value.pruning.minimum_batch_blocks,
                minimum_batch_changes: value.pruning.minimum_batch_changes,
                maximum_delete_changes: value.pruning.maximum_delete_changes,
                retain_finalized_blocks: value.pruning.retain_finalized_blocks,
                retain_acknowledged_seconds: value.pruning.retain_acknowledged_age.seconds(),
            },
            consumers: value
                .consumers
                .iter()
                .map(|consumer| DurableConsumerPolicy {
                    id: consumer.id.clone(),
                    required: consumer.required,
                    lease_ttl_seconds: consumer.lease_ttl.seconds(),
                })
                .collect(),
        }
    }
}

const fn default_delivery_max_bytes() -> HumanBytes {
    HumanBytes(1 << 30)
}

const fn default_delivery_max_age() -> HumanDuration {
    HumanDuration(24 * 60 * 60)
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointPolicyConfig {
    pub mode: CheckpointPolicyMode,
    pub keep: u32,
}

impl From<CheckpointPolicyConfig> for CheckpointPolicy {
    fn from(value: CheckpointPolicyConfig) -> Self {
        Self {
            mode: value.mode,
            keep: value.keep,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UndoPolicyConfig {
    pub mode: UndoPolicyMode,
    pub safety_blocks: u64,
}

impl From<UndoPolicyConfig> for UndoPolicy {
    fn from(value: UndoPolicyConfig) -> Self {
        Self {
            mode: value.mode,
            safety_blocks: value.safety_blocks,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HumanBytes(u64);

impl HumanBytes {
    #[must_use]
    pub const fn from_bytes(bytes: u64) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub const fn bytes(self) -> u64 {
        self.0
    }
}

impl Serialize for HumanBytes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(self.0)
    }
}

impl<'de> Deserialize<'de> for HumanBytes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Wire {
            Bytes(u64),
            Text(String),
        }
        match Wire::deserialize(deserializer)? {
            Wire::Bytes(bytes) => Ok(Self(bytes)),
            Wire::Text(text) => parse_bytes(&text)
                .map(Self)
                .map_err(serde::de::Error::custom),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HumanDuration(u64);

impl HumanDuration {
    #[must_use]
    pub const fn from_seconds(seconds: u64) -> Self {
        Self(seconds)
    }

    #[must_use]
    pub const fn seconds(self) -> u64 {
        self.0
    }
}

impl Serialize for HumanDuration {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(self.0)
    }
}

impl<'de> Deserialize<'de> for HumanDuration {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Wire {
            Seconds(u64),
            Text(String),
        }
        match Wire::deserialize(deserializer)? {
            Wire::Seconds(seconds) => Ok(Self(seconds)),
            Wire::Text(text) => parse_duration_seconds(&text)
                .map(Self)
                .map_err(serde::de::Error::custom),
        }
    }
}

fn parse_bytes(value: &str) -> Result<u64, &'static str> {
    parse_human_u64(
        value,
        &[
            ("GiB", 1_u64 << 30),
            ("MiB", 1_u64 << 20),
            ("KiB", 1_u64 << 10),
            ("GB", 1_000_000_000),
            ("MB", 1_000_000),
            ("KB", 1_000),
            ("B", 1),
        ],
        "byte size must be an integer with B, KiB, MiB, GiB, KB, MB, or GB",
    )
}

fn parse_duration_seconds(value: &str) -> Result<u64, &'static str> {
    parse_human_u64(
        value,
        &[("d", 86_400), ("h", 3_600), ("m", 60), ("s", 1)],
        "duration must be an integer followed by s, m, h, or d",
    )
}

fn parse_duration_milliseconds(value: &str) -> Result<u64, &'static str> {
    parse_human_u64(
        value,
        &[
            ("ms", 1),
            ("d", 86_400_000),
            ("h", 3_600_000),
            ("m", 60_000),
            ("s", 1_000),
        ],
        "duration must be an integer followed by ms, s, m, h, or d",
    )
}

fn parse_human_u64(
    value: &str,
    units: &[(&str, u64)],
    error: &'static str,
) -> Result<u64, &'static str> {
    for (suffix, multiplier) in units {
        if let Some(number) = value.strip_suffix(suffix) {
            let number = u64::from_str(number).map_err(|_| error)?;
            return number.checked_mul(*multiplier).ok_or(error);
        }
    }
    Err(error)
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RpcConfig {
    pub http_bind: SocketAddr,
    pub ws_bind: SocketAddr,
    pub historical_mode: HistoricalMode,
    pub transaction_locator: bool,
    pub minimum_recent_blocks: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoricalMode {
    OnDemand,
    Disabled,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ApiConfig {
    pub bind: SocketAddr,
    #[serde(default)]
    pub bearer_token_env: Option<String>,
    #[serde(default)]
    pub delivery: ApiDeliveryConfig,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ApiDeliveryConfig {
    #[serde(default = "DeliveryBatchConfig::history_default")]
    pub history_batches: DeliveryBatchConfig,
    #[serde(default = "DeliveryBatchConfig::live_default")]
    pub live_batches: DeliveryBatchConfig,
}

impl Default for ApiDeliveryConfig {
    fn default() -> Self {
        Self {
            history_batches: DeliveryBatchConfig::history_default(),
            live_batches: DeliveryBatchConfig::live_default(),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryBatchConfig {
    pub target_encoded_bytes: HumanBytes,
    pub maximum_encoded_bytes: HumanBytes,
    pub maximum_events: u64,
    pub maximum_processed_blocks: u64,
    pub maximum_delay: HumanMilliseconds,
    pub maximum_buffered_batches: u64,
    pub maximum_buffered_bytes: HumanBytes,
    #[serde(default)]
    pub compression: DeliveryCompressionConfig,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryCompressionConfig {
    None,
    #[default]
    Gzip,
}

impl DeliveryBatchConfig {
    const fn history_default() -> Self {
        Self {
            target_encoded_bytes: HumanBytes(4 * 1024 * 1024),
            maximum_encoded_bytes: HumanBytes(16 * 1024 * 1024),
            maximum_events: 20_000,
            maximum_processed_blocks: 8_192,
            maximum_delay: HumanMilliseconds(50),
            maximum_buffered_batches: 4,
            maximum_buffered_bytes: HumanBytes(64 * 1024 * 1024),
            compression: DeliveryCompressionConfig::Gzip,
        }
    }

    const fn live_default() -> Self {
        Self {
            target_encoded_bytes: HumanBytes(64 * 1024),
            maximum_encoded_bytes: HumanBytes(1024 * 1024),
            maximum_events: 1_000,
            maximum_processed_blocks: 8,
            maximum_delay: HumanMilliseconds(10),
            maximum_buffered_batches: 2,
            maximum_buffered_bytes: HumanBytes(2 * 1024 * 1024),
            compression: DeliveryCompressionConfig::Gzip,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HumanMilliseconds(u64);

impl HumanMilliseconds {
    #[must_use]
    pub const fn from_milliseconds(milliseconds: u64) -> Self {
        Self(milliseconds)
    }

    #[must_use]
    pub const fn milliseconds(self) -> u64 {
        self.0
    }
}

impl Serialize for HumanMilliseconds {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(self.0)
    }
}

impl<'de> Deserialize<'de> for HumanMilliseconds {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Wire {
            Milliseconds(u64),
            Text(String),
        }
        match Wire::deserialize(deserializer)? {
            Wire::Milliseconds(milliseconds) => Ok(Self(milliseconds)),
            Wire::Text(text) => parse_duration_milliseconds(&text)
                .map(Self)
                .map_err(serde::de::Error::custom),
        }
    }
}

/// A configuration that passed all cross-field checks.
#[derive(Clone, Debug)]
pub struct ValidatedConfig(Config);

impl ValidatedConfig {
    #[must_use]
    pub fn get(&self) -> &Config {
        &self.0
    }

    #[must_use]
    pub fn into_inner(self) -> Config {
        self.0
    }
}

impl Config {
    /// Read TOML without opening the configured data directory or any network
    /// connection.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] when the file cannot be read or its TOML does
    /// not match the strict schema.
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let input = fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        let document =
            toml::from_str::<toml::Value>(&input).map_err(|source| ConfigError::Parse {
                path: path.to_path_buf(),
                source,
            })?;
        let is_starter = document
            .as_table()
            .is_some_and(|table| table.contains_key("network"));
        if is_starter {
            let starter =
                toml::from_str::<StarterConfig>(&input).map_err(|source| ConfigError::Parse {
                    path: path.to_path_buf(),
                    source,
                })?;
            starter.expand().map_err(|detail| ConfigError::Expand {
                path: path.to_path_buf(),
                detail,
            })
        } else {
            toml::from_str(&input).map_err(|source| ConfigError::Parse {
                path: path.to_path_buf(),
                source,
            })
        }
    }

    /// Perform validation that requires no I/O.
    ///
    /// # Errors
    ///
    /// Returns every cross-field validation problem in [`ValidationErrors`].
    pub fn validate(self) -> Result<ValidatedConfig, ValidationErrors> {
        let errors = self.validation_errors();
        if errors.is_empty() {
            Ok(ValidatedConfig(self))
        } else {
            Err(ValidationErrors(errors))
        }
    }

    /// Return every validation issue so `doctor` can report them together.
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn validation_errors(&self) -> Vec<ValidationError> {
        let mut errors = Vec::new();

        if self.config_version != CONFIG_VERSION {
            errors.push(ValidationError::new(
                "config_version",
                format!(
                    "unsupported schema {}; expected {CONFIG_VERSION}",
                    self.config_version
                ),
            ));
        }
        if self.data_dir.as_os_str().is_empty() {
            errors.push(ValidationError::new("data_dir", "must not be empty"));
        }
        if self.chain.name.trim().is_empty() {
            errors.push(ValidationError::new("chain.name", "must not be empty"));
        }
        if self.chain.chain_id == 0 {
            errors.push(ValidationError::new(
                "chain.chain_id",
                "must be greater than zero",
            ));
        }
        validate_positive_budgets(&self.budgets, &mut errors);
        validate_raw_history(self.raw_history, &mut errors);
        validate_artifact_storage(self.artifact_storage, &self.budgets, &mut errors);
        validate_delivery_batch_config(
            "api.delivery.history_batches",
            self.api.delivery.history_batches,
            &mut errors,
        );
        validate_delivery_batch_config(
            "api.delivery.live_batches",
            self.api.delivery.live_batches,
            &mut errors,
        );
        if self.budgets.recent_raw_soft_bytes > self.budgets.recent_raw_hard_bytes {
            errors.push(ValidationError::new(
                "budgets.recent_raw_soft_bytes",
                "must not exceed recent_raw_hard_bytes",
            ));
        }
        if self.sources.history.is_empty() {
            errors.push(ValidationError::new(
                "sources.history",
                "at least one historical source is required",
            ));
        }
        unique_non_empty_ids(
            self.sources.history.iter().map(|source| source.id.as_str()),
            "sources.history",
            &mut errors,
        );
        validate_history_sources(&self.sources.history, &mut errors);
        if matches!(self.sources.live.kind, LiveSourceKind::P2p)
            && self.sources.live.minimum_peers == 0
        {
            errors.push(ValidationError::new(
                "sources.live.minimum_peers",
                "must be greater than zero for p2p",
            ));
        }
        if matches!(self.sources.live.kind, LiveSourceKind::P2p)
            && (self.sources.live.max_outbound_peers < self.sources.live.minimum_peers
                || self.sources.live.max_outbound_peers > 400)
        {
            errors.push(ValidationError::new(
                "sources.live.max_outbound_peers",
                "must be between minimum_peers and 400 for p2p",
            ));
        }
        if matches!(self.sources.live.kind, LiveSourceKind::P2p)
            && (self.sources.live.preferred_peers < self.sources.live.minimum_peers
                || self.sources.live.preferred_peers > self.sources.live.max_outbound_peers)
        {
            errors.push(ValidationError::new(
                "sources.live.preferred_peers",
                "must be between minimum_peers and max_outbound_peers for p2p",
            ));
        }
        if matches!(self.sources.live.kind, LiveSourceKind::P2p)
            && (self.sources.live.max_concurrent_dials == 0
                || self.sources.live.max_concurrent_dials > self.sources.live.max_outbound_peers)
        {
            errors.push(ValidationError::new(
                "sources.live.max_concurrent_dials",
                "must be in 1..=max_outbound_peers for p2p",
            ));
        }
        if matches!(self.sources.live.kind, LiveSourceKind::P2p)
            && self.sources.live.enable_discv5
            && self.sources.live.discovery_port != 0
            && self.sources.live.discovery_port == self.sources.live.discv5_port
        {
            errors.push(ValidationError::new(
                "sources.live.discv5_port",
                "must differ from the fixed Discv4 discovery_port",
            ));
        }
        if matches!(self.sources.live.kind, LiveSourceKind::P2p)
            && (self.sources.live.peer_refill_interval_ms == 0
                || self.sources.live.peer_recovery_timeout_seconds == 0)
        {
            errors.push(ValidationError::new(
                "sources.live peer refill/recovery intervals",
                "must be greater than zero for p2p",
            ));
        }
        if matches!(self.sources.live.kind, LiveSourceKind::P2p)
            && !(1..=64).contains(&self.sources.live.material_request_concurrency)
        {
            errors.push(ValidationError::new(
                "sources.live.material_request_concurrency",
                "must be in 1..=64 for p2p",
            ));
        }
        if matches!(self.sources.live.kind, LiveSourceKind::P2p)
            && !(1..=16).contains(&self.sources.live.material_request_blocks)
        {
            errors.push(ValidationError::new(
                "sources.live.material_request_blocks",
                "must be in 1..=16 for p2p",
            ));
        }
        if matches!(self.sources.live.kind, LiveSourceKind::P2p)
            && !(2..=120).contains(&self.sources.live.request_timeout_seconds)
        {
            errors.push(ValidationError::new(
                "sources.live.request_timeout_seconds",
                "must be in 2..=120 for p2p",
            ));
        }
        if matches!(self.sources.live.kind, LiveSourceKind::P2p)
            && !(1..=10).contains(&self.sources.live.request_retries)
        {
            errors.push(ValidationError::new(
                "sources.live.request_retries",
                "must be in 1..=10 for p2p",
            ));
        }
        if matches!(self.sources.live.kind, LiveSourceKind::P2p)
            && !(1..=10_000).contains(&self.sources.live.request_retry_backoff_ms)
        {
            errors.push(ValidationError::new(
                "sources.live.request_retry_backoff_ms",
                "must be in 1..=10000 for p2p",
            ));
        }
        if matches!(self.sources.live.kind, LiveSourceKind::P2p)
            && (self.sources.live.retry_backoff_max_seconds == 0
                || self.sources.live.peer_cache_flush_seconds == 0
                || self.sources.live.archive_reconciliation_interval_seconds == 0)
        {
            errors.push(ValidationError::new(
                "sources.live retry/cache/archive intervals",
                "must be greater than zero for p2p",
            ));
        }
        if matches!(self.sources.live.kind, LiveSourceKind::P2p)
            && !(1..=65_536).contains(&self.sources.live.peer_cache_max_entries)
        {
            errors.push(ValidationError::new(
                "sources.live.peer_cache_max_entries",
                "must be in 1..=65536 for p2p",
            ));
        }
        if matches!(self.sources.live.kind, LiveSourceKind::P2p)
            && leani_source_p2p::parse_nat_resolver(&self.sources.live.nat).is_err()
        {
            errors.push(ValidationError::new(
                "sources.live.nat",
                "must be a supported Reth NAT resolver or none",
            ));
        }
        if matches!(self.sources.live.kind, LiveSourceKind::P2p) {
            for (index, peer) in self.sources.live.trusted_peers.iter().enumerate() {
                if leani_source_p2p::parse_trusted_peer(peer).is_err() {
                    errors.push(ValidationError::new(
                        format!("sources.live.trusted_peers[{index}]"),
                        "must be a valid enode trusted peer",
                    ));
                }
            }
        }
        if matches!(self.sources.live.kind, LiveSourceKind::P2p)
            && !(1..=4_096).contains(&self.sources.live.archive_reconciliation_blocks)
        {
            errors.push(ValidationError::new(
                "sources.live.archive_reconciliation_blocks",
                "must be in 1..=4096 for p2p",
            ));
        }
        if matches!(self.sources.live.kind, LiveSourceKind::P2p)
            && self
                .sources
                .live
                .history_fallback_blocks
                .is_some_and(|blocks| !(1..=100_000_000).contains(&blocks))
        {
            errors.push(ValidationError::new(
                "sources.live.history_fallback_blocks",
                "must be in 1..=100000000 for p2p",
            ));
        }
        if matches!(self.sources.live.kind, LiveSourceKind::P2p)
            && !(1..=64).contains(&self.sources.live.history_header_request_concurrency)
        {
            errors.push(ValidationError::new(
                "sources.live.history_header_request_concurrency",
                "must be in 1..=64 for p2p",
            ));
        }
        if matches!(self.sources.live.kind, LiveSourceKind::P2p)
            && !(1..=1_024).contains(&self.sources.live.history_header_request_blocks)
        {
            errors.push(ValidationError::new(
                "sources.live.history_header_request_blocks",
                "must be in 1..=1024 for p2p",
            ));
        }
        if matches!(self.sources.live.kind, LiveSourceKind::P2p)
            && !(1..=64).contains(&self.sources.live.handoff_overlap_blocks)
        {
            errors.push(ValidationError::new(
                "sources.live.handoff_overlap_blocks",
                "must be in 1..=64 for p2p",
            ));
        }
        validate_finality(&self.finality, &mut errors);
        if matches!(self.sources.live.kind, LiveSourceKind::P2p)
            && !matches!(
                self.finality.kind,
                FinalitySourceKind::BeaconApi | FinalitySourceKind::ConsensusP2p
            )
        {
            errors.push(ValidationError::new(
                "sources.live/finality",
                "the P2P live source requires verified beacon_api or consensus_p2p finality",
            ));
        }
        if self.processors.is_empty() {
            errors.push(ValidationError::new(
                "processors",
                "at least one processor is required",
            ));
        }
        unique_non_empty_ids(
            self.processors
                .iter()
                .map(|processor| processor.instance.as_str()),
            "processors",
            &mut errors,
        );
        for (index, processor) in self.processors.iter().enumerate() {
            if processor.require_retained_input && !self.raw_history.enabled {
                errors.push(ValidationError::new(
                    format!("processors[{index}].require_retained_input"),
                    "requires raw_history.enabled = true",
                ));
            }
            if processor.require_retained_input
                && processor.history_mode != ProcessorHistoryMode::OnDemand
            {
                errors.push(ValidationError::new(
                    format!("processors[{index}].history_mode"),
                    "require_retained_input requires on_demand history",
                ));
            }
            if processor.coverage.verification_segment_blocks == 0 {
                errors.push(ValidationError::new(
                    format!("processors[{index}].coverage.verification_segment_blocks"),
                    "must be greater than zero",
                ));
            }
            if leani_processor_api::ProcessorInstanceId::new(&processor.instance).is_err() {
                errors.push(ValidationError::new(
                    format!("processors[{index}].instance"),
                    "must be a portable processor instance ID",
                ));
            }
            if processor.version.trim().is_empty() {
                errors.push(ValidationError::new(
                    format!("processors[{index}].version"),
                    "must not be empty",
                ));
            }
            match processor.lifecycle_policies() {
                Ok(lifecycle) => {
                    if let Err(message) = lifecycle.validate(processor.publication_policy()) {
                        errors.push(ValidationError::new(
                            format!("processors[{index}]"),
                            message,
                        ));
                    }
                    if processor.history_control
                        == ProcessorHistoryControl::ApplicationSubscriptions
                        && processor.history_mode != ProcessorHistoryMode::OnDemand
                    {
                        errors.push(ValidationError::new(
                            format!("processors[{index}].history_mode"),
                            "application_subscriptions requires on_demand history",
                        ));
                    }
                    if processor.history_control
                        == ProcessorHistoryControl::ApplicationSubscriptions
                        && lifecycle.delivery.mode != DeliveryPolicyMode::UntilAcknowledged
                    {
                        errors.push(ValidationError::new(
                            format!("processors[{index}].delivery.mode"),
                            "application_subscriptions requires until_acknowledged delivery",
                        ));
                    }
                }
                Err(message) => errors.push(ValidationError::new(
                    format!("processors[{index}]"),
                    message,
                )),
            }
        }
        if self
            .api
            .bearer_token_env
            .as_ref()
            .is_some_and(|name| name.trim().is_empty())
        {
            errors.push(ValidationError::new(
                "api.bearer_token_env",
                "must be a non-empty environment variable name",
            ));
        }
        if self.rpc.minimum_recent_blocks == 0 {
            errors.push(ValidationError::new(
                "rpc.minimum_recent_blocks",
                "must be greater than zero",
            ));
        }
        if self.rpc.http_bind == self.rpc.ws_bind
            || self.rpc.http_bind == self.api.bind
            || self.rpc.ws_bind == self.api.bind
        {
            errors.push(ValidationError::new(
                "rpc/api bind",
                "HTTP RPC, WebSocket RPC, and native API addresses must be distinct",
            ));
        }

        errors
    }
}

fn validate_history_sources(sources: &[HistorySourceConfig], errors: &mut Vec<ValidationError>) {
    for (index, source) in sources.iter().enumerate() {
        match source.kind {
            HistorySourceKind::Archive if source.manifest.is_none() => {
                errors.push(ValidationError::new(
                    format!("sources.history[{index}].manifest"),
                    "is required for an archive source",
                ));
            }
            HistorySourceKind::Archive => {}
            _ if source.manifest.is_some() => {
                errors.push(ValidationError::new(
                    format!("sources.history[{index}].manifest"),
                    "is only valid for an archive source",
                ));
            }
            _ => {}
        }
        match source.kind {
            HistorySourceKind::EraE => {
                if let Some(endpoint) = &source.endpoint {
                    if !matches!(endpoint.scheme(), "http" | "https" | "file") {
                        errors.push(ValidationError::new(
                            format!("sources.history[{index}].endpoint"),
                            "eraE endpoints must use http, https, or file",
                        ));
                    }
                    if !endpoint.path().ends_with('/') {
                        errors.push(ValidationError::new(
                            format!("sources.history[{index}].endpoint"),
                            "eraE endpoints must end with `/`",
                        ));
                    }
                }
            }
            _ if source.endpoint.is_some() => {
                errors.push(ValidationError::new(
                    format!("sources.history[{index}].endpoint"),
                    "is currently only valid for an eraE source",
                ));
            }
            _ => {}
        }
        if matches!(source.kind, HistorySourceKind::Xatu) {
            for (field, blocks) in [
                ("chunk_blocks", source.chunk_blocks),
                ("blobs_chunk_blocks", source.blobs_chunk_blocks),
            ] {
                if matches!(blocks, Some(blocks) if blocks == 0 || !blocks.is_multiple_of(1_000)) {
                    errors.push(ValidationError::new(
                        format!("sources.history[{index}].{field}"),
                        "Xatu chunk blocks must be a non-zero multiple of 1,000",
                    ));
                }
            }
            if source.batch_rows == Some(0) {
                errors.push(ValidationError::new(
                    format!("sources.history[{index}].batch_rows"),
                    "Xatu batch rows must be greater than zero",
                ));
            }
        } else {
            for (field, configured) in [
                ("chunk_blocks", source.chunk_blocks.is_some()),
                ("blobs_chunk_blocks", source.blobs_chunk_blocks.is_some()),
                ("batch_rows", source.batch_rows.is_some()),
            ] {
                if configured {
                    errors.push(ValidationError::new(
                        format!("sources.history[{index}].{field}"),
                        "is currently only valid for a Xatu source",
                    ));
                }
            }
        }
        if matches!(source.kind, HistorySourceKind::Archive)
            && matches!(source.trust, HistoryTrust::VerifiedMaterial)
        {
            errors.push(ValidationError::new(
                format!("sources.history[{index}].trust"),
                "normalized archives are trusted datasets; verified raw material needs a source-specific adapter",
            ));
        }
        if matches!(source.kind, HistorySourceKind::EraE)
            && matches!(source.trust, HistoryTrust::VerifiedMaterial)
        {
            errors.push(ValidationError::new(
                format!("sources.history[{index}].trust"),
                "sparse eraE reads verify execution commitments but still trust the canonical catalog; use trusted_dataset until consensus proof verification is enabled",
            ));
        }
    }
}

fn validate_delivery_batch_config(
    path: &str,
    config: DeliveryBatchConfig,
    errors: &mut Vec<ValidationError>,
) {
    if config.target_encoded_bytes.bytes() == 0
        || config.maximum_encoded_bytes.bytes() == 0
        || config.maximum_events == 0
        || config.maximum_processed_blocks == 0
        || config.maximum_delay.milliseconds() == 0
        || config.maximum_buffered_batches == 0
        || config.maximum_buffered_bytes.bytes() == 0
    {
        errors.push(ValidationError::new(
            path,
            "delivery batch sizes, counts, and delay must be greater than zero",
        ));
    }
    if config.target_encoded_bytes.bytes() > config.maximum_encoded_bytes.bytes() {
        errors.push(ValidationError::new(
            format!("{path}.target_encoded_bytes"),
            "must not exceed maximum_encoded_bytes",
        ));
    }
    if config.maximum_buffered_bytes.bytes() < config.maximum_encoded_bytes.bytes() {
        errors.push(ValidationError::new(
            format!("{path}.maximum_buffered_bytes"),
            "must be at least maximum_encoded_bytes",
        ));
    }
}

#[allow(clippy::too_many_lines)]
fn validate_positive_budgets(budgets: &BudgetConfig, errors: &mut Vec<ValidationError>) {
    let values = [
        ("memory_bytes", budgets.memory_bytes),
        ("temporary_disk_bytes", budgets.temporary_disk_bytes),
        ("pending_delta_bytes", budgets.pending_delta_bytes),
        ("recent_raw_soft_bytes", budgets.recent_raw_soft_bytes),
        ("recent_raw_hard_bytes", budgets.recent_raw_hard_bytes),
    ];
    for (name, value) in values {
        if value == 0 {
            errors.push(ValidationError::new(
                format!("budgets.{name}"),
                "must be greater than zero",
            ));
        }
    }
    if budgets.source_concurrency == 0 {
        errors.push(ValidationError::new(
            "budgets.source_concurrency",
            "must be greater than zero",
        ));
    }
    if budgets.mapper_concurrency == 0 {
        errors.push(ValidationError::new(
            "budgets.mapper_concurrency",
            "must be greater than zero",
        ));
    }
    if budgets.history_material.memory_bytes.bytes() == 0 {
        errors.push(ValidationError::new(
            "budgets.history_material.memory_bytes",
            "must be greater than zero",
        ));
    }
    if budgets
        .history_material
        .maximum_buffered_frames_per_acquisition
        == 0
    {
        errors.push(ValidationError::new(
            "budgets.history_material.maximum_buffered_frames_per_acquisition",
            "must be greater than zero",
        ));
    }
    if budgets.history_material.minimum_physical_chunk_blocks == 0 {
        errors.push(ValidationError::new(
            "budgets.history_material.minimum_physical_chunk_blocks",
            "must be greater than zero",
        ));
    }
    if !budgets.history_material.maximum_overfetch_ratio.is_finite()
        || budgets.history_material.maximum_overfetch_ratio < 1.0
    {
        errors.push(ValidationError::new(
            "budgets.history_material.maximum_overfetch_ratio",
            "must be finite and at least 1.0",
        ));
    }
    if budgets.history_pipeline.maximum_active_chunks == 0
        || budgets.history_pipeline.maximum_active_chunks > u32::MAX as usize
    {
        errors.push(ValidationError::new(
            "budgets.history_pipeline.maximum_active_chunks",
            "must be in 1..=4294967295",
        ));
    }
    if budgets.history_pipeline.maximum_mapped_bytes.bytes() == 0
        || budgets.history_pipeline.maximum_mapped_bytes.bytes() > u64::from(u32::MAX)
    {
        errors.push(ValidationError::new(
            "budgets.history_pipeline.maximum_mapped_bytes",
            "must be in 1B..=4294967295B",
        ));
    }
    let commit = budgets.history_pipeline.commit;
    if commit.maximum_blocks == 0 {
        errors.push(ValidationError::new(
            "budgets.history_pipeline.commit.maximum_blocks",
            "must be greater than zero",
        ));
    }
    if commit.maximum_changes == 0 {
        errors.push(ValidationError::new(
            "budgets.history_pipeline.commit.maximum_changes",
            "must be greater than zero",
        ));
    }
    if commit.maximum_encoded_bytes.bytes() == 0 {
        errors.push(ValidationError::new(
            "budgets.history_pipeline.commit.maximum_encoded_bytes",
            "must be greater than zero",
        ));
    }
    if commit.maximum_delay.milliseconds() == 0 {
        errors.push(ValidationError::new(
            "budgets.history_pipeline.commit.maximum_delay",
            "must be greater than zero",
        ));
    }
    if commit.target_writer_hold.milliseconds() == 0 {
        errors.push(ValidationError::new(
            "budgets.history_pipeline.commit.target_writer_hold",
            "must be greater than zero",
        ));
    }
    if budgets.store.maximum_physical_bytes.bytes() == 0 {
        errors.push(ValidationError::new(
            "budgets.store.maximum_physical_bytes",
            "must be greater than zero",
        ));
    }
    if budgets.artifacts.maximum_retained_bytes.bytes() == 0
        || budgets.artifacts.maximum_pending_bytes.bytes() == 0
    {
        errors.push(ValidationError::new(
            "budgets.artifacts",
            "maximum_retained_bytes and maximum_pending_bytes must be greater than zero",
        ));
    }
    let delivery = budgets.delivery;
    if delivery.maximum_history_retained_bytes.bytes() == 0
        || delivery.maximum_retained_bytes.bytes() == 0
        || delivery.maximum_history_retained_bytes.bytes() > delivery.maximum_retained_bytes.bytes()
    {
        errors.push(ValidationError::new(
            "budgets.delivery",
            "must satisfy 0 < maximum_history_retained_bytes <= maximum_retained_bytes",
        ));
    }
}

fn validate_raw_history(config: RawHistoryConfig, errors: &mut Vec<ValidationError>) {
    if !config.enabled {
        return;
    }
    let logical = config.maximum_logical_bytes.bytes();
    let physical = config.maximum_physical_bytes.bytes();
    let frame = config.maximum_frame_logical_bytes.bytes();
    let segment_logical = config.maximum_segment_logical_bytes.bytes();
    let segment_physical = config.maximum_segment_physical_bytes.bytes();
    if logical == 0
        || physical == 0
        || frame == 0
        || segment_logical == 0
        || segment_physical == 0
        || config.reader_connections == 0
        || config.maximum_source_frames == 0
        || config.maximum_buffered_frames == 0
        || frame > segment_logical
        || segment_logical > logical
        || segment_physical > physical
    {
        errors.push(ValidationError::new(
            "raw_history",
            "enabled raw history requires non-zero limits with frame <= segment <= store and at least one reader",
        ));
    }
}

fn validate_artifact_storage(
    config: ArtifactStorageConfig,
    budgets: &BudgetConfig,
    errors: &mut Vec<ValidationError>,
) {
    if config.backend == ArtifactStorageBackend::Sqlite {
        return;
    }
    let artifact = config.maximum_artifact_logical_bytes.bytes();
    let segment_logical = config.maximum_segment_logical_bytes.bytes();
    let segment_physical = config.maximum_segment_physical_bytes.bytes();
    if config.segment_target_blocks == 0
        || artifact == 0
        || segment_logical == 0
        || segment_physical == 0
        || config.compaction_interval.milliseconds() == 0
        || config.maximum_segments_per_cycle == 0
        || config.maximum_segments_per_cycle > 1_000
    {
        errors.push(ValidationError::new(
            "artifact_storage",
            "tiered segment sizes, cadence, and per-cycle work must be positive; maximum_segments_per_cycle must not exceed 1000",
        ));
    }
    if artifact > segment_logical {
        errors.push(ValidationError::new(
            "artifact_storage.maximum_artifact_logical_bytes",
            "must not exceed maximum_segment_logical_bytes",
        ));
    }
    if segment_physical > budgets.store.maximum_physical_bytes.bytes() {
        errors.push(ValidationError::new(
            "artifact_storage.maximum_segment_physical_bytes",
            "must not exceed budgets.store.maximum_physical_bytes",
        ));
    }
}

fn validate_finality(finality: &FinalityConfig, errors: &mut Vec<ValidationError>) {
    if matches!(finality.kind, FinalitySourceKind::Disabled) {
        return;
    }
    if finality.checkpoint.trim().is_empty()
        || finality.checkpoint.contains("REPLACE_WITH")
        || !is_hex_hash(&finality.checkpoint)
    {
        errors.push(ValidationError::new(
            "finality.checkpoint",
            "must be a 0x-prefixed 32-byte weak-subjectivity checkpoint root",
        ));
    }
    match finality.kind {
        FinalitySourceKind::BeaconApi => {
            if finality.endpoints.is_empty() {
                errors.push(ValidationError::new(
                    "finality.endpoints",
                    "at least one endpoint is required",
                ));
            }
            if finality.minimum_agreement == 0
                || finality.minimum_agreement > finality.endpoints.len()
            {
                errors.push(ValidationError::new(
                    "finality.minimum_agreement",
                    "must be within 1..=the number of endpoints",
                ));
            }
            for (index, endpoint) in finality.endpoints.iter().enumerate() {
                if !matches!(endpoint.scheme(), "http" | "https") {
                    errors.push(ValidationError::new(
                        format!("finality.endpoints[{index}]"),
                        "only http and https URLs are supported",
                    ));
                }
            }
        }
        FinalitySourceKind::ConsensusP2p => {
            if finality.checkpoint_slot == 0 {
                errors.push(ValidationError::new(
                    "finality.checkpoint_slot",
                    "must identify the non-zero beacon slot of the checkpoint for consensus_p2p finality",
                ));
            }
            if !finality.endpoints.is_empty() {
                errors.push(ValidationError::new(
                    "finality.endpoints",
                    "must be empty for consensus_p2p finality",
                ));
            }
            if finality.minimum_peers == 0 || finality.minimum_peers > 128 {
                errors.push(ValidationError::new(
                    "finality.minimum_peers",
                    "must be between 1 and 128",
                ));
            }
            for (index, bootnode) in finality.bootnodes.iter().enumerate() {
                if !bootnode.starts_with("enr:") || bootnode.len() > 2_048 {
                    errors.push(ValidationError::new(
                        format!("finality.bootnodes[{index}]"),
                        "must be an `enr:` record no longer than 2,048 bytes",
                    ));
                }
            }
        }
        FinalitySourceKind::Disabled => {}
    }
}

const fn default_history_material_memory() -> HumanBytes {
    HumanBytes(256 * 1_024 * 1_024)
}

const fn default_raw_history_logical_bytes() -> HumanBytes {
    HumanBytes(512 * 1_024 * 1_024 * 1_024)
}

const fn default_raw_history_physical_bytes() -> HumanBytes {
    HumanBytes(512 * 1_024 * 1_024 * 1_024)
}

const fn default_raw_history_frame_bytes() -> HumanBytes {
    HumanBytes(64 * 1_024 * 1_024)
}

const fn default_raw_history_segment_logical_bytes() -> HumanBytes {
    HumanBytes(512 * 1_024 * 1_024)
}

const fn default_raw_history_segment_physical_bytes() -> HumanBytes {
    HumanBytes(512 * 1_024 * 1_024)
}

const fn default_raw_history_reader_connections() -> u32 {
    4
}

const fn default_raw_history_source_frames() -> u64 {
    8_192
}

const fn default_raw_history_buffered_frames() -> usize {
    8
}

const fn default_history_material_buffered_frames() -> usize {
    128
}

const fn default_history_material_minimum_chunk_blocks() -> u64 {
    128
}

const fn default_history_material_overfetch_ratio() -> f64 {
    1.25
}

const fn default_history_pipeline_active_chunks() -> usize {
    4
}

const fn default_history_pipeline_mapped_bytes() -> HumanBytes {
    HumanBytes(128 * 1024 * 1024)
}

const fn default_delivery_retained_bytes() -> HumanBytes {
    HumanBytes(1_536 * 1_024 * 1_024)
}

const fn default_delivery_history_retained_bytes() -> HumanBytes {
    HumanBytes(1_024 * 1_024 * 1_024)
}

const fn default_store_physical_bytes() -> HumanBytes {
    HumanBytes(2 * 1_024 * 1_024 * 1_024)
}

const fn default_artifact_retained_bytes() -> HumanBytes {
    HumanBytes(1_024 * 1_024 * 1_024)
}

const fn default_artifact_pending_bytes() -> HumanBytes {
    HumanBytes(256 * 1_024 * 1_024)
}

const fn default_artifact_segment_blocks() -> u64 {
    2_048
}

const fn default_artifact_segment_compression() -> leani_store_artifacts::ArtifactCompression {
    leani_store_artifacts::ArtifactCompression::Snappy
}

const fn default_artifact_maximum_item_bytes() -> HumanBytes {
    HumanBytes(16 * 1_024 * 1_024)
}

const fn default_artifact_maximum_segment_logical_bytes() -> HumanBytes {
    HumanBytes(64 * 1_024 * 1_024)
}

const fn default_artifact_maximum_segment_physical_bytes() -> HumanBytes {
    HumanBytes(64 * 1_024 * 1_024)
}

const fn default_artifact_compaction_interval() -> HumanMilliseconds {
    HumanMilliseconds(100)
}

const fn default_artifact_compaction_segments_per_cycle() -> usize {
    4
}

const fn default_history_commit_blocks() -> usize {
    128
}

const fn default_history_commit_changes() -> usize {
    10_000
}

const fn default_history_commit_encoded_bytes() -> HumanBytes {
    HumanBytes(16 * 1024 * 1024)
}

const fn default_history_commit_delay() -> HumanMilliseconds {
    HumanMilliseconds(50)
}

const fn default_history_commit_target_writer_hold() -> HumanMilliseconds {
    HumanMilliseconds(leani_runtime::HISTORICAL_COMMIT_TARGET_WRITER_HOLD_MS)
}

const fn default_coverage_verification_segment_blocks() -> u64 {
    8_192
}

const fn default_minimum_agreement() -> usize {
    1
}

const fn default_handoff_overlap_blocks() -> u64 {
    32
}

const fn default_history_header_request_concurrency() -> usize {
    16
}

const fn default_history_header_request_blocks() -> u64 {
    1_024
}

const fn default_execution_max_outbound_peers() -> usize {
    100
}

const fn default_execution_preferred_peers() -> usize {
    16
}

const fn default_execution_max_concurrent_dials() -> usize {
    30
}

const fn default_execution_peer_refill_interval_ms() -> u64 {
    5_000
}

const fn default_execution_peer_recovery_timeout_seconds() -> u64 {
    300
}

const fn default_execution_material_request_concurrency() -> usize {
    32
}

const fn default_execution_material_request_blocks() -> usize {
    8
}

const fn default_execution_request_timeout_seconds() -> u64 {
    8
}

const fn default_execution_request_retries() -> usize {
    3
}

const fn default_execution_request_retry_backoff_ms() -> u64 {
    250
}

const fn default_true() -> bool {
    true
}

fn default_execution_nat() -> String {
    "none".to_owned()
}

const fn default_execution_persistent_retries() -> bool {
    true
}

const fn default_execution_retry_backoff_max_seconds() -> u64 {
    60
}

const fn default_execution_peer_cache_flush_seconds() -> u64 {
    60
}

const fn default_execution_peer_cache_max_entries() -> usize {
    4_096
}

const fn default_archive_reconciliation_blocks() -> u64 {
    64
}

const fn default_archive_reconciliation_interval_seconds() -> u64 {
    300
}

const fn default_finality_minimum_peers() -> usize {
    2
}

fn is_hex_hash(value: &str) -> bool {
    value.len() == 66
        && value.starts_with("0x")
        && value[2..].bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn unique_non_empty_ids<'a>(
    ids: impl IntoIterator<Item = &'a str>,
    field: &str,
    errors: &mut Vec<ValidationError>,
) {
    let mut seen = HashSet::new();
    for id in ids {
        if id.trim().is_empty() {
            errors.push(ValidationError::new(field, "IDs must not be empty"));
        } else if !seen.insert(id) {
            errors.push(ValidationError::new(field, format!("duplicate ID `{id}`")));
        }
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read configuration at {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse configuration at {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
    #[error("failed to expand compact configuration at {path}: {detail}")]
    Expand { path: PathBuf, detail: String },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ValidationError {
    pub field: String,
    pub message: String,
}

impl ValidationError {
    pub(crate) fn new(field: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            message: message.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidationErrors(pub Vec<ValidationError>);

impl fmt::Display for ValidationErrors {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "configuration validation failed")?;
        for error in &self.0 {
            write!(formatter, "\n- {}: {}", error.field, error.message)?;
        }
        Ok(())
    }
}

impl std::error::Error for ValidationErrors {}

#[cfg(test)]
pub(crate) const VALID_CONFIG_TOML: &str = r#"
config_version = 1
data_dir = "./data"

[chain]
name = "ethereum-mainnet"
chain_id = 1

[budgets]
memory_bytes = 1024
temporary_disk_bytes = 2048
pending_delta_bytes = 1024
recent_raw_soft_bytes = 1024
recent_raw_hard_bytes = 2048
source_concurrency = 2
mapper_concurrency = 2

[[sources.history]]
id = "xatu"
kind = "xatu"
priority = 10
trust = "trusted_dataset"

[sources.live]
kind = "p2p"
minimum_peers = 1

[finality]
kind = "beacon_api"
checkpoint = "0x0000000000000000000000000000000000000000000000000000000000000000"
checkpoint_slot = 0
endpoints = ["http://127.0.0.1:5052"]

[[processors]]
id = "fixture"
instance = "fixture-main"
version = "0.1.0"
start_block = 1
publish = "optimistic_and_finalized"

[processors.state]
mode = "durable"

[processors.output]
mode = "full"

[processors.delivery]
mode = "window"

[processors.checkpoint]
mode = "automatic"
keep = 3

[processors.undo]
mode = "unfinalized"
safety_blocks = 256

[processors.coverage]
verification_segment_blocks = 8192

[rpc]
http_bind = "127.0.0.1:8545"
ws_bind = "127.0.0.1:8546"
historical_mode = "on_demand"
transaction_locator = false
minimum_recent_blocks = 128

[api]
bind = "127.0.0.1:8080"
"#;

#[cfg(test)]
mod tests {
    use super::*;

    const BASE_PROCESSOR_CONTRACT: &str = r#"instance = "fixture-main"
version = "0.1.0"
start_block = 1
publish = "optimistic_and_finalized"

[processors.state]
mode = "durable"

[processors.output]
mode = "full"

[processors.delivery]
mode = "window"

[processors.checkpoint]
mode = "automatic"
keep = 3

[processors.undo]
mode = "unfinalized"
safety_blocks = 256

[processors.coverage]
verification_segment_blocks = 8192"#;

    fn config() -> Config {
        toml::from_str(VALID_CONFIG_TOML).expect("valid fixture")
    }

    #[test]
    fn accepts_valid_configuration() {
        let config = config();
        config.clone().validate().expect("configuration validates");
        assert_eq!(config.sources.live.history_fallback_blocks, None);
        assert_eq!(config.sources.live.history_fallback_start(9_999, 1), 1);
        assert_eq!(
            config.processors[0].history_mode,
            ProcessorHistoryMode::Automatic
        );
        assert!(!config.raw_history.enabled);
        assert!(!config.processors[0].require_retained_input);
        assert_eq!(
            config.budgets.history_material.mode,
            HistoryMaterialCoordinatorMode::Enabled
        );
        assert_eq!(
            config.budgets.history_material.memory_bytes.bytes(),
            256 * 1_024 * 1_024
        );
        assert_eq!(
            config
                .budgets
                .history_material
                .maximum_buffered_frames_per_acquisition,
            128
        );
        assert_eq!(
            config
                .budgets
                .history_material
                .minimum_physical_chunk_blocks,
            128
        );
        assert!(
            (config.budgets.history_material.maximum_overfetch_ratio - 1.25).abs() < f64::EPSILON
        );
        assert_eq!(config.budgets.history_pipeline.maximum_active_chunks, 4);
        assert_eq!(
            config.budgets.history_pipeline.maximum_mapped_bytes.bytes(),
            128 * 1_024 * 1_024
        );
        assert_eq!(config.budgets.history_pipeline.commit.maximum_blocks, 128);
        assert_eq!(
            config.budgets.history_pipeline.commit.maximum_changes,
            10_000
        );
        assert_eq!(
            config
                .budgets
                .history_pipeline
                .commit
                .maximum_encoded_bytes
                .bytes(),
            16 * 1_024 * 1_024
        );
        assert_eq!(
            config
                .budgets
                .history_pipeline
                .commit
                .maximum_delay
                .milliseconds(),
            50
        );
        assert_eq!(
            config
                .budgets
                .history_pipeline
                .commit
                .target_writer_hold
                .milliseconds(),
            20
        );
        assert_eq!(
            config
                .budgets
                .delivery
                .maximum_history_retained_bytes
                .bytes(),
            1_024 * 1_024 * 1_024
        );
        assert_eq!(
            config.budgets.delivery.maximum_retained_bytes.bytes(),
            1_536 * 1_024 * 1_024
        );
        assert_eq!(
            config.budgets.store.maximum_physical_bytes.bytes(),
            2 * 1_024 * 1_024 * 1_024
        );
        assert_eq!(
            config.budgets.artifacts.maximum_retained_bytes.bytes(),
            1_024 * 1_024 * 1_024
        );
        assert_eq!(
            config.budgets.artifacts.maximum_pending_bytes.bytes(),
            256 * 1_024 * 1_024
        );
    }

    #[test]
    fn explicit_p2p_history_fallback_bound_limits_the_finalized_suffix() {
        let configured = VALID_CONFIG_TOML.replace(
            "minimum_peers = 1",
            "minimum_peers = 1\nhistory_fallback_blocks = 64",
        );
        let config: Config = toml::from_str(&configured).expect("configuration parses");
        config.clone().validate().expect("configuration validates");
        assert_eq!(config.sources.live.history_fallback_blocks, Some(64));
        assert_eq!(config.sources.live.history_fallback_start(1_000, 1), 937);
        assert_eq!(config.sources.live.history_fallback_start(1_000, 980), 980);
    }

    #[test]
    fn artifact_storage_defaults_to_sqlite_with_tier_tuning_ready() {
        let config = config();
        assert_eq!(
            config.artifact_storage.backend,
            ArtifactStorageBackend::Sqlite
        );
        assert_eq!(config.artifact_storage.segment_target_blocks, 2_048);
        assert_eq!(
            config.artifact_storage.compression,
            leani_store_artifacts::ArtifactCompression::Snappy
        );
    }

    #[test]
    fn accepts_and_bounds_tiered_artifact_storage() {
        let configured = VALID_CONFIG_TOML.replace(
            "[budgets]",
            r#"[artifact_storage]
backend = "tiered_segments"
segment_target_blocks = 4096
compression = "deflate"
maximum_artifact_logical_bytes = "8MiB"
maximum_segment_logical_bytes = "32MiB"
maximum_segment_physical_bytes = "32MiB"
compaction_interval = "250ms"
maximum_segments_per_cycle = 2

[budgets]"#,
        );
        let config: Config = toml::from_str(&configured).expect("configuration parses");
        config.clone().validate().expect("configuration validates");
        assert_eq!(
            config.artifact_storage.backend,
            ArtifactStorageBackend::TieredSegments
        );
        assert_eq!(config.artifact_storage.segment_target_blocks, 4_096);
        assert_eq!(
            config.artifact_storage.compaction_interval.milliseconds(),
            250
        );

        let mut invalid = config;
        invalid.artifact_storage.maximum_segment_physical_bytes =
            HumanBytes(3 * 1_024 * 1_024 * 1_024);
        assert!(
            invalid
                .validation_errors()
                .iter()
                .any(|error| { error.field == "artifact_storage.maximum_segment_physical_bytes" })
        );
    }

    #[test]
    fn accepts_history_material_rollout_mode_and_human_readable_budget() {
        let configured = VALID_CONFIG_TOML.replace(
            "mapper_concurrency = 2",
            r#"mapper_concurrency = 2

[budgets.history_material]
mode = "observe"
memory_bytes = "64MiB"
maximum_buffered_frames_per_acquisition = 32
minimum_physical_chunk_blocks = 64
maximum_overfetch_ratio = 1.5"#,
        );
        let config: Config = toml::from_str(&configured).expect("configuration parses");
        config.clone().validate().expect("configuration validates");
        assert_eq!(
            config.budgets.history_material.mode,
            HistoryMaterialCoordinatorMode::Observe
        );
        assert_eq!(
            config.budgets.history_material.memory_bytes.bytes(),
            64 * 1_024 * 1_024
        );
        assert_eq!(
            config
                .budgets
                .history_material
                .maximum_buffered_frames_per_acquisition,
            32
        );
        assert_eq!(
            config
                .budgets
                .history_material
                .minimum_physical_chunk_blocks,
            64
        );
        assert!(
            (config.budgets.history_material.maximum_overfetch_ratio - 1.5).abs() < f64::EPSILON
        );
    }

    #[test]
    fn accepts_on_demand_processor_history_without_changing_rpc_history() {
        let configured = VALID_CONFIG_TOML.replace(
            "start_block = 1",
            "history_mode = \"on_demand\"\nstart_block = 1",
        );
        let config: Config = toml::from_str(&configured).expect("configuration parses");
        assert_eq!(
            config.processors[0].history_mode,
            ProcessorHistoryMode::OnDemand
        );
        assert!(matches!(
            config.rpc.historical_mode,
            HistoricalMode::OnDemand
        ));
        config.validate().expect("configuration validates");
    }

    #[test]
    fn accepts_orthogonal_policies_and_human_readable_limits() {
        let modern = VALID_CONFIG_TOML.replace(
            BASE_PROCESSOR_CONTRACT,
            r#"instance = "fixture-production"
version = "0.1.0"
start_block = 1
publish = "optimistic_and_finalized"

[processors.state]
mode = "checkpointed"

[processors.artifacts]
mode = "window"

[processors.artifacts.window]
max_bytes = "64MiB"

[processors.output]
mode = "none"

[processors.delivery]
mode = "until_acknowledged"
max_bytes = "1GiB"
max_age = "24h"
on_limit = "pause"

[processors.delivery.pruning]
interval = "30s"
minimum_batch_blocks = 64
minimum_batch_changes = 10000
maximum_delete_changes = 10000
retain_finalized_blocks = 256
retain_acknowledged_age = "1h"

[[processors.delivery.consumers]]
id = "fixture-api"
required = true
lease_ttl = "5m"

[processors.checkpoint]
mode = "automatic"
keep = 3

[processors.undo]
mode = "unfinalized"
safety_blocks = 256

[processors.coverage]
verification_segment_blocks = 8192"#,
        );
        let config: Config = toml::from_str(&modern).expect("modern config parses");
        config.clone().validate().expect("modern config validates");
        let lifecycle = config.processors[0]
            .lifecycle_policies()
            .expect("lifecycle");
        assert_eq!(lifecycle.artifacts.mode, ArtifactPolicyMode::Window);
        assert_eq!(
            lifecycle
                .artifacts
                .window
                .expect("artifact window")
                .max_bytes,
            Some(64 * 1_024 * 1_024)
        );
        assert_eq!(lifecycle.delivery.max_bytes, 1_u64 << 30);
        assert_eq!(lifecycle.delivery.max_age_seconds, 24 * 60 * 60);
        assert_eq!(lifecycle.delivery.consumers[0].lease_ttl_seconds, 5 * 60);
    }

    #[test]
    fn accepts_node_owned_on_demand_sqlite_materialization_without_delivery() {
        let modern = VALID_CONFIG_TOML.replace(
            BASE_PROCESSOR_CONTRACT,
            r#"instance = "fixture-analysis"
version = "0.1.0"
history_control = "node_owned"
history_mode = "on_demand"
start_block = 1
publish = "optimistic_and_finalized"

[processors.state]
mode = "durable"

[processors.output]
mode = "full"

[processors.delivery]
mode = "none"

[processors.checkpoint]
mode = "automatic"
keep = 3

[processors.undo]
mode = "unfinalized"
safety_blocks = 256

[processors.coverage]
verification_segment_blocks = 8192"#,
        );
        let config: Config = toml::from_str(&modern).expect("materialization config parses");
        config.clone().validate().expect("configuration validates");
        let processor = &config.processors[0];
        assert_eq!(
            processor.history_control,
            ProcessorHistoryControl::NodeOwned
        );
        assert_eq!(processor.history_mode, ProcessorHistoryMode::OnDemand);
        assert_eq!(
            processor
                .lifecycle_policies()
                .expect("lifecycle")
                .delivery
                .mode,
            DeliveryPolicyMode::None
        );
    }

    #[test]
    fn rejects_automatic_history_owned_by_application_subscriptions() {
        let modern = VALID_CONFIG_TOML.replace(
            BASE_PROCESSOR_CONTRACT,
            r#"instance = "fixture-externalized"
version = "0.1.0"
history_control = "application_subscriptions"
start_block = 1
publish = "optimistic_and_finalized"

[processors.state]
mode = "durable"

[processors.output]
mode = "none"

[processors.delivery]
mode = "until_acknowledged"

[[processors.delivery.consumers]]
id = "fixture-api"
required = true
lease_ttl = "5m"

[processors.checkpoint]
mode = "automatic"
keep = 3

[processors.undo]
mode = "none"
safety_blocks = 0

[processors.coverage]
verification_segment_blocks = 8192"#,
        );
        let config: Config = toml::from_str(&modern).expect("subscription config parses");
        assert!(config.validation_errors().iter().any(|error| {
            error.field.ends_with("history_mode") && error.message.contains("requires on_demand")
        }));
    }

    #[test]
    fn rejects_removed_operator_delivery_topology_knobs() {
        let modern = VALID_CONFIG_TOML.replace(
            BASE_PROCESSOR_CONTRACT,
            r#"instance = "fixture-analysis"
version = "0.1.0"
start_block = 1
publish = "optimistic_and_finalized"

[processors.state]
mode = "durable"

[processors.output]
mode = "full"

[processors.delivery]
mode = "none"
topology = "split_live_history"

[processors.checkpoint]
mode = "automatic"
keep = 3

[processors.undo]
mode = "none"
safety_blocks = 0

[processors.coverage]
verification_segment_blocks = 8192"#,
        );
        let error = toml::from_str::<Config>(&modern).expect_err("stale topology is rejected");
        assert!(error.to_string().contains("topology"));
    }

    #[test]
    fn rejects_removed_legacy_retention() {
        let stale = VALID_CONFIG_TOML.replace(
            "publish = \"optimistic_and_finalized\"",
            "publish = \"optimistic_and_finalized\"\nretention = \"full_output_history\"",
        );
        let error = toml::from_str::<Config>(&stale).expect_err("legacy retention is rejected");
        assert!(error.to_string().contains("retention"));
    }

    #[test]
    fn rejects_unknown_fields() {
        let invalid = VALID_CONFIG_TOML.replace("chain_id = 1", "chain_id = 1\nmystery = true");
        let error = toml::from_str::<Config>(&invalid).expect_err("unknown field rejected");
        assert!(error.to_string().contains("unknown field"));
    }

    #[test]
    fn reports_cross_field_errors_together() {
        let mut config = config();
        config.config_version = 99;
        config.budgets.recent_raw_soft_bytes = 3_000;
        config.processors.push(config.processors[0].clone());
        let errors = config.validate().expect_err("invalid configuration").0;
        assert!(errors.iter().any(|error| error.field == "config_version"));
        assert!(
            errors
                .iter()
                .any(|error| error.field == "budgets.recent_raw_soft_bytes")
        );
        assert!(
            errors
                .iter()
                .any(|error| error.message.contains("duplicate ID"))
        );
    }

    #[test]
    fn validates_xatu_acquisition_tuning() {
        let mut config = config();
        config.sources.history[0].chunk_blocks = Some(8_000);
        config.sources.history[0].blobs_chunk_blocks = Some(16_000);
        config.sources.history[0].batch_rows = Some(4_096);
        config
            .clone()
            .validate()
            .expect("aligned Xatu acquisition tuning is valid");

        config.sources.history[0].chunk_blocks = Some(1_001);
        config.sources.history[0].blobs_chunk_blocks = Some(0);
        config.sources.history[0].batch_rows = Some(0);
        let errors = config
            .validate()
            .expect_err("misaligned or zero Xatu tuning is rejected")
            .0;
        for field in ["chunk_blocks", "blobs_chunk_blocks", "batch_rows"] {
            assert!(errors.iter().any(|error| error.field.ends_with(field)));
        }
    }

    #[test]
    fn retained_only_processors_require_enabled_raw_history_and_on_demand_scheduling() {
        let mut config = config();
        config.processors[0].require_retained_input = true;
        let errors = config
            .clone()
            .validate()
            .expect_err("raw store and on-demand mode are required")
            .0;
        assert!(
            errors
                .iter()
                .any(|error| error.field.ends_with("require_retained_input"))
        );
        assert!(errors.iter().any(|error| {
            error.field.ends_with("history_mode")
                && error.message.contains("require_retained_input")
        }));

        config.raw_history.enabled = true;
        config.processors[0].history_mode = ProcessorHistoryMode::OnDemand;
        config
            .validate()
            .expect("retained-only on-demand processor configuration");
    }

    #[test]
    fn placeholder_checkpoint_is_not_operational() {
        let mut config = config();
        config.finality.checkpoint =
            "REPLACE_WITH_A_RECENT_WEAK_SUBJECTIVITY_CHECKPOINT".to_owned();
        let errors = config.validate().expect_err("placeholder rejected").0;
        assert!(
            errors
                .iter()
                .any(|error| error.field == "finality.checkpoint")
        );
    }

    #[test]
    fn consensus_p2p_requires_checkpoint_slot() {
        let mut config = config();
        config.finality.kind = FinalitySourceKind::ConsensusP2p;
        config.finality.endpoints.clear();
        let errors = config.clone().validate().expect_err("slot is required").0;
        assert!(
            errors
                .iter()
                .any(|error| error.field == "finality.checkpoint_slot")
        );
        config.finality.checkpoint_slot = 12_345;
        config
            .validate()
            .expect("checkpoint slot makes P2P finality operational");
    }

    #[test]
    fn aggressive_p2p_bounds_are_validated_together() {
        let mut config = config();
        config.sources.live.max_outbound_peers = 2;
        config.sources.live.preferred_peers = 3;
        config.sources.live.max_concurrent_dials = 3;
        config.sources.live.material_request_concurrency = 0;
        config.sources.live.material_request_blocks = 17;
        config.sources.live.request_timeout_seconds = 1;
        config.sources.live.request_retries = 0;
        config.sources.live.request_retry_backoff_ms = 0;
        config.sources.live.history_fallback_blocks = Some(100_000_001);
        config.sources.live.history_header_request_concurrency = 65;
        config.sources.live.history_header_request_blocks = 1_025;
        config.sources.live.archive_reconciliation_blocks = 4_097;
        config.sources.live.discovery_port = 30_303;
        config.sources.live.discv5_port = 30_303;
        let errors = config.validate().expect_err("p2p bounds rejected").0;
        for field in [
            "sources.live.preferred_peers",
            "sources.live.max_concurrent_dials",
            "sources.live.material_request_concurrency",
            "sources.live.material_request_blocks",
            "sources.live.request_timeout_seconds",
            "sources.live.request_retries",
            "sources.live.request_retry_backoff_ms",
            "sources.live.history_fallback_blocks",
            "sources.live.history_header_request_concurrency",
            "sources.live.history_header_request_blocks",
            "sources.live.archive_reconciliation_blocks",
            "sources.live.discv5_port",
        ] {
            assert!(
                errors.iter().any(|error| error.field == field),
                "missing validation error for {field}"
            );
        }
    }

    #[test]
    fn doctor_style_loading_does_not_create_data_directory() {
        let temp = tempfile::tempdir().expect("temporary directory");
        let config_path = temp.path().join("node.toml");
        fs::write(&config_path, VALID_CONFIG_TOML).expect("write fixture");
        let data_path = temp.path().join("data");
        let mut loaded = Config::load(&config_path).expect("load fixture");
        loaded.data_dir = data_path.clone();
        let _ = loaded.validation_errors();
        assert!(!data_path.exists());
    }

    #[test]
    fn compact_uniswap_configuration_expands_to_sane_node_defaults() {
        let temp = tempfile::tempdir().expect("temporary directory");
        let config_path = temp.path().join("leani.toml");
        fs::write(
            &config_path,
            r#"config_version = 1
network = "ethereum-mainnet"
data_dir = "./data"

[finality]
checkpoint = "0x1111111111111111111111111111111111111111111111111111111111111111"
checkpoint_slot = 15000000
endpoints = ["https://ethereum-beacon-api.publicnode.com/"]

[uniswap]
markets = ["ETH/USDC", "WBTC/ETH"]

[api]
bind = "127.0.0.1:18080"
"#,
        )
        .expect("write compact config");

        let config = Config::load(&config_path).expect("load compact config");
        config
            .clone()
            .validate()
            .expect("expanded configuration validates");
        assert_eq!(config.chain.chain_id, 1);
        assert_eq!(config.sources.live.minimum_peers, 1);
        assert_eq!(config.sources.live.preferred_peers, 16);
        assert_eq!(config.rpc.http_bind.port(), 18_545);
        assert_eq!(config.api.bind.port(), 18_080);
        assert_eq!(config.processors.len(), 1);
        let pools = config.processors[0].settings["pools"]
            .as_array()
            .expect("pool array");
        assert_eq!(pools.len(), 2);
    }

    #[test]
    fn compact_configuration_rejects_unknown_markets_during_expansion() {
        let temp = tempfile::tempdir().expect("temporary directory");
        let config_path = temp.path().join("leani.toml");
        fs::write(
            &config_path,
            r#"config_version = 1
network = "ethereum-mainnet"

[finality]
checkpoint = "0x1111111111111111111111111111111111111111111111111111111111111111"
checkpoint_slot = 15000000
endpoints = ["https://ethereum-beacon-api.publicnode.com/"]

[uniswap]
markets = ["LINK/ETH"]
"#,
        )
        .expect("write compact config");

        let error = Config::load(&config_path).expect_err("unknown market rejected");
        assert!(error.to_string().contains("unknown Uniswap V3 market"));
    }

    #[test]
    fn generated_starter_document_only_contains_product_choices() {
        let starter = StarterConfig::uniswap(
            PathBuf::from("./data"),
            vec!["ETH/USDC".to_owned()],
            format!("0x{}", "11".repeat(32)),
            15_000_000,
            vec![Url::parse("https://beacon.example/").expect("URL")],
        );
        let encoded = toml::to_string_pretty(&starter).expect("serialize starter config");
        assert!(encoded.contains("network = \"ethereum-mainnet\""));
        assert!(encoded.contains("markets = [\"ETH/USDC\"]"));
        assert!(!encoded.contains("[budgets]"));
        assert!(!encoded.contains("[sources.live]"));
        assert!(!encoded.contains("[[processors.settings.pools]]"));
    }

    #[test]
    fn checked_in_mode_profiles_and_json_schema_track_config_v1() {
        let repository = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        for relative in [
            "config/modes/externalized.toml",
            "config/modes/aggregate-only.toml",
            "config/modes/terminal.toml",
            "config/modes/head-only.toml",
            "config/modes/windowed.toml",
            "config/modes/full.toml",
            "config/benchmarks/real-source.toml",
        ] {
            let path = repository.join(relative);
            Config::load(&path)
                .unwrap_or_else(|error| panic!("{}: {error}", path.display()))
                .validate()
                .unwrap_or_else(|error| panic!("{}: {error}", path.display()));
        }
        let schema_path = repository.join("config/schema-v1.json");
        let schema: serde_json::Value = serde_json::from_slice(
            &std::fs::read(&schema_path)
                .unwrap_or_else(|error| panic!("{}: {error}", schema_path.display())),
        )
        .expect("configuration schema is JSON");
        assert_eq!(schema["oneOf"].as_array().map(Vec::len), Some(2));
        assert_eq!(
            schema["$defs"]["starterConfig"]["properties"]["config_version"]["const"],
            1
        );
        assert_eq!(
            schema["$defs"]["advancedConfig"]["additionalProperties"],
            serde_json::Value::Bool(false)
        );
    }
}
