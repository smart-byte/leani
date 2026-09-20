//! Reproducible pre-tuning benchmark harness.

use std::{
    fs::{self, OpenOptions},
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use alloy_primitives::U256;
use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use futures::StreamExt;
use leani_api::{
    ApiConfig, BackfillControl, BackfillControlError, BackfillExecutionMode,
    BackfillRange as ApiBackfillRange, BackfillState as ApiBackfillState,
    BackfillStatus as ApiBackfillStatus, CreateBackfillRequest, DeliveryBatchLimits,
    DeliveryCompression, EffectiveBackfillBatching, HistoricalWorkOwner,
};
use leani_primitives::{
    BlockNumber, BlockRange, CapabilitySet, ChainId, Finality, ProcessorCursor, SourceKind,
    TrustModel,
};
use leani_processor_api::{
    ArtifactPolicyMode, DeliveryPolicyMode, DurableConsumerPolicy, EncodedDelta, LifecyclePolicies,
    OutputPolicyMode, Processor, ProcessorDescriptor, PublicationPolicy, ReductionMode,
};
use leani_processor_blobs::{
    BLOCK_COLLECTION, BlobFork, BlobSchedule, BlobTransactionEntity, BlobsBlockEntity, BlobsDelta,
    BlobsProcessor, TRANSACTION_BLOCK_INDEX, TRANSACTION_COLLECTION,
};
use leani_processor_uniswap::{
    HISTORY_COLLECTION as UNISWAP_HISTORY_COLLECTION, PoolConfig, PoolKind, PoolPriceEntity,
    UniswapConfig, UniswapObservationsProcessor, UniswapPriceDelta,
};
use leani_runtime::{
    BackfillJob, BackfillReport, HistoricalJobOwner, HistoricalMaterialCoordinator,
    HistoricalMaterialCoordinatorConfig, HistoricalPipelineBudget, HistoricalRuntime,
    HistoricalRuntimeConfig,
};
use leani_source_api::{
    HistorySource, NetworkTelemetry, SourceAcquisitionMetrics, SourceBudget, VerificationPolicy,
};
use leani_store_artifacts::{
    ArtifactCompression, ArtifactSegmentLimits, ArtifactSegmentReader, ArtifactSegmentSink,
    ArtifactSegmentSinkConfig, ArtifactSegmentSinkStats, ArtifactSegmentWriter,
};
use leani_store_history::{
    Compression as RawHistoryCompression, HistoryStore, HistoryStoreConfig, RawHistoryIndexPolicy,
    RawHistoryJobId, RawHistoryJobSpec, RawHistoryMaterialProfile, RawHistoryProfile,
    RawHistoryRetention, RawHistoryRunOutcome, RawHistoryRunner, RawHistorySegmentPolicy,
    RawHistorySourceSet, RetainedHistorySource, RetainedHistorySourceConfig,
    StorageBudget as RawHistoryStorageBudget, StorageLimitAction, VerificationClass,
};
use leani_store_sqlite::{
    ArtifactSegmentStorageConfig, BackfillDeliveryBatchLimits, BackfillDeliveryCompression,
    BackfillSubscriptionMode, BackfillSubscriptionRecord, BackfillSubscriptionState,
    CURRENT_SCHEMA_VERSION, ConsumerRole, ConsumerStartPosition, DELIVERY_ENCODING_VERSION,
    JobRecord, JobState, ProcessorStoreStats, SqliteStore, StoreConfig, StoreStats,
    default_delivery_stream_id,
};
use leani_testkit::{
    BlockLocalCounter, GeneratedHistorySource, GeneratedSourceStats, SyntheticCorpusKind,
    SyntheticCorpusManifest, synthetic_corpus_manifest, uniswap_weth_usdc_pool,
    update_frame_digest,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::{
    cli::{
        BenchmarkArtifactCompression, BenchmarkCompression, BenchmarkCorpus, BenchmarkDestination,
        BenchmarkMode, BenchmarkPostgresSchema, BenchmarkProductProfile, BenchmarkRealSourcePolicy,
    },
    config::{
        ArtifactStorageBackend, ArtifactStorageConfig, Config, HumanBytes, HumanMilliseconds,
    },
    process::{
        Exit, configured_history_sources, configured_store_config, execution_p2p_source,
        select_processor_config, supervise_artifact_compaction, verified_p2p_history_anchor,
    },
    processors::ProcessorRegistry,
};

const REPORT_VERSION: u32 = 19;
const REPORT_SCHEMA: &str = "leani.benchmark.v19";
const DELIVERY_READ_LIMIT: usize = 1_000;
const GENERIC_EVENT_LOG_SCHEMA: &str = "leani.benchmark.generic-event-log.v1";
const ONE_DOMAIN_EVENT_PER_ROW: &str = "one_domain_event_per_row";
const BLOBS_APPLICATION_SCHEMA: &str = "blobs-money.benchmark.application.v1";
const BLOBS_APPLICATION_ROW_SEMANTICS: &str = "one_block_row_plus_one_blob_transaction_row";

#[derive(Clone, Debug)]
pub(crate) struct BenchmarkOptions {
    pub mode: BenchmarkMode,
    pub profile: BenchmarkProductProfile,
    pub destination: BenchmarkDestination,
    pub postgres_schema: BenchmarkPostgresSchema,
    pub corpus: BenchmarkCorpus,
    pub blocks: u64,
    pub seed: u64,
    pub chunk_blocks: u64,
    pub artifact_segment_blocks: u64,
    pub artifact_segment_compression: BenchmarkArtifactCompression,
    pub artifact_compaction_interval_ms: u64,
    pub artifact_compaction_maximum_segments_per_cycle: usize,
    pub warmups: u32,
    pub runs: u32,
    pub sample_interval_ms: u64,
    pub consumer_delay_ms: u64,
    pub consumer_reconnect_every_batches: u64,
    pub consumer_drop_ack_response_once: bool,
    pub concurrent_live_blocks: u64,
    pub live_block_interval_ms: u64,
    pub mapper_concurrency: usize,
    pub maximum_active_chunks: usize,
    pub maximum_mapped_bytes: u64,
    pub commit_maximum_blocks: usize,
    pub commit_maximum_changes: usize,
    pub commit_maximum_encoded_bytes: u64,
    pub commit_maximum_delay_ms: u64,
    pub commit_target_writer_hold_ms: u64,
    pub delivery_target_encoded_bytes: u64,
    pub delivery_maximum_encoded_bytes: u64,
    pub delivery_maximum_events: u64,
    pub delivery_maximum_processed_blocks: u64,
    pub delivery_maximum_delay_ms: u64,
    pub delivery_maximum_buffered_batches: usize,
    pub delivery_maximum_buffered_bytes: u64,
    pub delivery_compression: BenchmarkCompression,
    pub report: Option<PathBuf>,
    pub samples_report: Option<PathBuf>,
}

#[derive(Clone, Debug)]
pub(crate) struct RealSourceBenchmarkOptions {
    pub processor: String,
    pub source_policy: BenchmarkRealSourcePolicy,
    pub from_block: u64,
    pub to_block: u64,
    pub data_dir: PathBuf,
    pub consumer_delay_ms: u64,
    pub timeout_seconds: u64,
    pub sample_interval_ms: u64,
    pub source_concurrency: Option<usize>,
    pub mapper_concurrency: Option<usize>,
    pub maximum_active_chunks: Option<usize>,
    pub expected_output_digest: Option<String>,
    pub delivery_compression: BenchmarkCompression,
    pub report: PathBuf,
}

const REAL_SOURCE_REPORT_VERSION: u32 = 7;
const REAL_SOURCE_REPORT_SCHEMA: &str = "leani.real-source-benchmark.v7";

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RealSourceBenchmarkReport {
    report_version: u32,
    report_schema: &'static str,
    status: &'static str,
    generated_at_unix_ms: u64,
    node_version: &'static str,
    git_revision: String,
    git_dirty: bool,
    build_profile: &'static str,
    host: BenchmarkHost,
    processor: BenchmarkProcessorIdentity,
    product_profile: &'static str,
    destination: &'static str,
    source_policy: &'static str,
    range: BlockRange,
    requested_blocks: u64,
    config_path: String,
    data_dir: String,
    timeout_seconds: u64,
    sample_interval_ms: u64,
    consumer_delay_ms: u64,
    delivery_compression: &'static str,
    tuning: RealSourceRuntimeTuning,
    source_budget: SourceBudget,
    sources: Vec<RealSourceMeasurement>,
    p2p_network: Option<leani_source_api::NetworkTelemetrySnapshot>,
    p2p_requests: Option<leani_source_p2p::P2pRequestMetricsSnapshot>,
    runtime: Option<BackfillReport>,
    delivery: Option<DeliveryMeasurement>,
    elapsed_milliseconds: u64,
    time_to_first_source_frame_ms: Option<u64>,
    time_to_source_complete_ms: Option<u64>,
    time_to_first_processor_commit_ms: Option<u64>,
    producer_complete_milliseconds: Option<u64>,
    consumer_complete_milliseconds: Option<u64>,
    blocks_per_second_milli: Option<u64>,
    peak_rss_bytes: Option<u64>,
    peak_physical_store_bytes: Option<u64>,
    peak_delivery_retained_bytes: Option<u64>,
    peak_material_buffered_bytes: Option<u64>,
    store: Option<StoreStats>,
    processor_store: Option<ProcessorStoreStats>,
    output_digest: Option<String>,
    expected_output_digest: Option<String>,
    output_digest_matches_expected: Option<bool>,
    pipeline_correctness_passed: bool,
    correctness_passed: bool,
    error: Option<String>,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RealSourceRuntimeTuning {
    source_concurrency: usize,
    mapper_concurrency: usize,
    maximum_active_chunks: usize,
    maximum_mapped_bytes: u64,
    p2p_material_request_concurrency: usize,
    p2p_material_request_blocks: usize,
    p2p_header_request_concurrency: usize,
    p2p_header_request_blocks: u64,
    p2p_request_timeout_seconds: u64,
    p2p_request_retries: usize,
    p2p_request_retry_backoff_ms: u64,
    p2p_persistent_retries: bool,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RealSourceMeasurement {
    id: String,
    kind: SourceKind,
    priority: u16,
    metrics: Option<SourceAcquisitionMetrics>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BenchmarkSuiteReport {
    report_version: u32,
    report_schema: &'static str,
    generated_at_unix_ms: u64,
    node_version: &'static str,
    git_revision: String,
    git_dirty: bool,
    build_profile: &'static str,
    host: BenchmarkHost,
    configuration: BenchmarkConfiguration,
    product_profile: BenchmarkProductProfileManifest,
    processor: BenchmarkProcessorIdentity,
    corpus: SyntheticCorpusManifest,
    baseline: BaselineContract,
    sample_report: Option<BenchmarkSampleReportIdentity>,
    runs: Vec<BenchmarkRunReport>,
    summary: BenchmarkSummary,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BenchmarkHost {
    os: &'static str,
    architecture: &'static str,
    logical_cpus: usize,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BenchmarkConfiguration {
    mode: &'static str,
    profile: &'static str,
    destination: &'static str,
    postgres_schema: &'static str,
    blocks: u64,
    seed: u64,
    chunk_blocks: u64,
    artifact_segment_blocks: u64,
    artifact_segment_compression: &'static str,
    artifact_compaction_interval_ms: u64,
    artifact_compaction_maximum_segments_per_cycle: usize,
    warmups: u32,
    measured_runs: u32,
    sample_interval_ms: u64,
    consumer_delay_ms: u64,
    consumer_reconnect_every_batches: u64,
    consumer_drop_ack_response_once: bool,
    concurrent_live_blocks: u64,
    live_block_interval_ms: u64,
    mapper_concurrency: usize,
    maximum_active_chunks: usize,
    maximum_mapped_bytes: u64,
    history_material_memory_bytes: u64,
    maximum_buffered_frames_per_acquisition: usize,
    minimum_physical_chunk_blocks: u64,
    maximum_overfetch_ratio: f64,
    commit_maximum_blocks: usize,
    commit_maximum_changes: usize,
    commit_maximum_encoded_bytes: u64,
    commit_maximum_delay_ms: u64,
    commit_target_writer_hold_ms: u64,
    delivery_target_encoded_bytes: u64,
    delivery_maximum_encoded_bytes: u64,
    delivery_maximum_events: u64,
    delivery_maximum_processed_blocks: u64,
    delivery_maximum_delay_ms: u64,
    delivery_maximum_buffered_batches: usize,
    delivery_maximum_buffered_bytes: u64,
    delivery_compression: &'static str,
    delivery_read_limit: usize,
    sqlite_schema_version: u32,
    delivery_encoding_version: u16,
    sqlite_durability: &'static str,
    sqlite_reader_connections: u32,
    sqlite_busy_timeout_ms: u64,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BenchmarkProcessorIdentity {
    instance: String,
    descriptor_hash: String,
    schema_hash: String,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BenchmarkProductProfileManifest {
    name: &'static str,
    raw_history: &'static str,
    processor_artifacts: &'static str,
    materialized_output: &'static str,
    delivery: &'static str,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BenchmarkSampleReportIdentity {
    path: String,
    records: u64,
    blake3: String,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BaselineContract {
    commit_max_blocks: usize,
    commit_max_delay_ms: u64,
    delivery_aggregation: &'static str,
    compression: &'static str,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RowCountCheck {
    schema: String,
    semantics: String,
    expected_rows: u64,
    observed_rows: u64,
    correctness_passed: bool,
}

impl RowCountCheck {
    fn exact(
        schema: impl Into<String>,
        semantics: impl Into<String>,
        expected_rows: u64,
        observed_rows: u64,
    ) -> Self {
        Self {
            schema: schema.into(),
            semantics: semantics.into(),
            expected_rows,
            observed_rows,
            correctness_passed: observed_rows == expected_rows,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct LogicalLayerMeasurement {
    rows: u64,
    bytes: Option<u64>,
}

#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct LogicalStorageMeasurement {
    raw_history: LogicalLayerMeasurement,
    processor_artifacts: LogicalLayerMeasurement,
    materialized_entities: LogicalLayerMeasurement,
    materialized_indexes: LogicalLayerMeasurement,
    processor_state: LogicalLayerMeasurement,
    delivery: LogicalLayerMeasurement,
    undo: LogicalLayerMeasurement,
    checkpoints: LogicalLayerMeasurement,
    recent_reorg: LogicalLayerMeasurement,
    correctness_metadata: LogicalLayerMeasurement,
    destination: LogicalLayerMeasurement,
}

#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
#[allow(clippy::struct_field_names)]
struct PhysicalStorageMeasurement {
    node_sqlite_database_bytes: u64,
    node_sqlite_wal_bytes: u64,
    node_sqlite_freelist_bytes: u64,
    node_non_sqlite_segment_bytes: u64,
    raw_history_segment_bytes: u64,
    raw_history_catalog_bytes: u64,
    destination_table_bytes: u64,
    destination_index_bytes: u64,
    destination_total_bytes: u64,
    destination_wal_written_bytes: u64,
    total_retained_bytes: u64,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BenchmarkStorageMeasurement {
    canonical_logical_output_definition: &'static str,
    canonical_logical_output_bytes: u64,
    physical_measurement: &'static str,
    logical: LogicalStorageMeasurement,
    physical: PhysicalStorageMeasurement,
    storage_amplification_milli: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BenchmarkRunReport {
    iteration: u32,
    mode: &'static str,
    profile: &'static str,
    elapsed_milliseconds: u64,
    blocks_per_second_milli: u64,
    time_to_first_source_frame_ms: Option<u64>,
    sampled_time_to_first_commit_ms: Option<u64>,
    source: GeneratedSourceStats,
    runtime: Option<BackfillReport>,
    prefill_runtime: Option<BackfillReport>,
    delivery: Option<DeliveryMeasurement>,
    query: Option<QueryMeasurement>,
    store: Option<StoreStats>,
    processor_store: Option<ProcessorStoreStats>,
    artifact_segment_store: Option<ArtifactSegmentSinkStats>,
    artifact_segment_candidate: Option<ArtifactSegmentCandidateMeasurement>,
    raw_history: Option<RawHistoryMeasurement>,
    storage: BenchmarkStorageMeasurement,
    observed_digest: String,
    expected_digest: String,
    correctness_passed: bool,
    peak_rss_bytes: Option<u64>,
    peak_physical_store_bytes: Option<u64>,
    peak_delivery_retained_bytes: Option<u64>,
    peak_history_delivery_retained_bytes: Option<u64>,
    peak_pending_delta_bytes: Option<u64>,
    peak_processor_artifact_bytes: Option<u64>,
    peak_pending_processor_artifact_bytes: Option<u64>,
    peak_active_material_acquisitions: Option<u64>,
    peak_material_buffered_bytes: Option<u64>,
    #[serde(skip_serializing)]
    samples: Vec<BenchmarkSample>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RawHistoryMeasurement {
    execution: &'static str,
    profile: &'static str,
    acquisition_milliseconds: u64,
    retained_read_milliseconds: u64,
    retained_read_purpose: &'static str,
    external_frames_after_acquisition: u64,
    external_frames_after_replay: u64,
    external_estimated_bytes: u64,
    committed_blocks: u64,
    closed_segments: u64,
    owners: u64,
    retained_logical_bytes: u64,
    retained_segment_physical_bytes: u64,
    catalog_physical_bytes: u64,
    total_physical_bytes: u64,
    retained_segment_opens: u64,
    retained_record_reads: u64,
    retained_stored_record_bytes: u64,
    retained_decompressed_bytes: u64,
}

#[derive(Debug)]
struct RawReplayContext {
    source: RetainedHistorySource,
    committed_blocks: u64,
    acquisition_milliseconds: u64,
    external_after_acquisition: GeneratedSourceStats,
}

#[derive(Debug, Default)]
struct BenchmarkRawInput {
    store: Option<HistoryStore>,
    replay: Option<RawReplayContext>,
    measurement_started: Option<Instant>,
}

/// Supplemental codec/footprint measurement. This deliberately re-encodes
/// artifacts read from `SQLite` and is not a segment-backed process-throughput
/// result.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ArtifactSegmentCandidateMeasurement {
    input: &'static str,
    compression: &'static str,
    target_blocks_per_segment: u64,
    segments: u64,
    artifacts: u64,
    logical_bytes: u64,
    physical_bytes: u64,
    storage_amplification_milli: Option<u64>,
    sqlite_scan_milliseconds: u64,
    segment_write_milliseconds: u64,
    segment_verify_milliseconds: u64,
    exact_lookup_microseconds: u64,
    input_digest: String,
    verified_digest: String,
    correctness_passed: bool,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BenchmarkSample {
    elapsed_milliseconds: u64,
    rss_bytes: Option<u64>,
    source_frames: u64,
    source_estimated_bytes: u64,
    committed_through: Option<u64>,
    database_bytes: Option<u64>,
    freelist_bytes: Option<u64>,
    wal_bytes: Option<u64>,
    physical_store_bytes: Option<u64>,
    non_sqlite_segment_bytes: Option<u64>,
    raw_history_segment_bytes: Option<u64>,
    raw_history_catalog_bytes: Option<u64>,
    delivery_retained_bytes: Option<u64>,
    history_delivery_retained_bytes: Option<u64>,
    pending_delta_bytes: Option<u64>,
    processor_artifact_bytes: Option<u64>,
    pending_processor_artifact_bytes: Option<u64>,
    active_material_acquisitions: Option<u64>,
    material_buffered_bytes: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default)]
struct BenchmarkSamplePeaks {
    rss_bytes: Option<u64>,
    physical_store_bytes: Option<u64>,
    delivery_retained_bytes: Option<u64>,
    history_delivery_retained_bytes: Option<u64>,
    pending_delta_bytes: Option<u64>,
    processor_artifact_bytes: Option<u64>,
    pending_processor_artifact_bytes: Option<u64>,
    active_material_acquisitions: Option<u64>,
    material_buffered_bytes: Option<u64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BenchmarkSampleRecord<'a> {
    report_version: u32,
    report_schema: &'static str,
    iteration: u32,
    mode: &'static str,
    profile: &'static str,
    sample: &'a BenchmarkSample,
}

#[derive(Clone, Copy)]
struct SampleStores<'a> {
    processor: Option<&'a SqliteStore>,
    raw_history: Option<&'a HistoryStore>,
}

#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct DeliveryMeasurement {
    batches: u64,
    processed_blocks: u64,
    domain_events: u64,
    progress_boundaries: u64,
    completion_records: u64,
    raw_payload_bytes: u64,
    uncompressed_encoded_bytes: u64,
    transmitted_bytes: u64,
    encoded_json_bytes: u64,
    observed_response_body_bytes: Option<u64>,
    acknowledgements: u64,
    consumer_reconnects: u64,
    simulated_ack_response_losses: u64,
    pruned_records: u64,
    completion_sequence: Option<u64>,
    destination_digest: String,
    destination_digest_algorithm: String,
    destination_transactions: u64,
    destination_transaction_ms: u64,
    time_to_first_batch_ms: Option<u64>,
    time_to_first_destination_commit_ms: Option<u64>,
    time_to_first_acknowledgement_ms: Option<u64>,
    producer_complete_milliseconds: Option<u64>,
    consumer_complete_milliseconds: Option<u64>,
    post_producer_drain_milliseconds: Option<u64>,
    producer_blocks_per_second_milli: Option<u64>,
    consumer_blocks_per_second_milli: Option<u64>,
    domain_events_per_second_milli: Option<u64>,
    raw_payload_bytes_per_second: Option<u64>,
    transmitted_bytes_per_second: Option<u64>,
    destination_row_count: Option<RowCountCheck>,
    destination_through_block: Option<u64>,
    destination_table_bytes: Option<u64>,
    destination_index_bytes: Option<u64>,
    destination_total_bytes: Option<u64>,
    destination_wal_written_bytes: Option<u64>,
    batch_samples: Vec<DeliveryBatchSample>,
    concurrent_live: Option<ConcurrentLiveMeasurement>,
    #[serde(skip)]
    live_destination: Option<LiveDestinationMeasurement>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ConcurrentLiveMeasurement {
    requested_blocks: u64,
    applied_blocks: u64,
    expected_domain_events: u64,
    delivered_blocks: u64,
    delivered_domain_events: u64,
    expected_canonical_output_bytes: u64,
    canonical_output_bytes: u64,
    batches: u64,
    acknowledgements: u64,
    acknowledged_sequence: u64,
    expected_destination_digest: String,
    destination_digest: String,
    destination_digest_algorithm: &'static str,
    time_to_first_commit_ms: Option<u64>,
    time_to_first_destination_batch_ms: Option<u64>,
    median_commit_latency_us: u64,
    p95_commit_latency_us: u64,
    p99_commit_latency_us: u64,
    maximum_commit_latency_us: u64,
    correctness_passed: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct DeliveryBatchSample {
    processed_blocks: u64,
    domain_events: u64,
    raw_payload_bytes: u64,
    uncompressed_encoded_bytes: u64,
    transmitted_bytes: u64,
    encoded_json_bytes: u64,
    destination_transaction_ms: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SdkDestinationMeasurement {
    batches: u64,
    processed_blocks: u64,
    domain_events: u64,
    progress_boundaries: u64,
    completion_records: u64,
    raw_payload_bytes: u64,
    uncompressed_encoded_bytes: u64,
    transmitted_bytes: u64,
    encoded_json_bytes: u64,
    acknowledgements: u64,
    consumer_reconnects: u64,
    simulated_ack_response_losses: u64,
    pruned_records: u64,
    completion_sequence: Option<String>,
    destination_digest: String,
    destination_transactions: u64,
    destination_transaction_ms: u64,
    time_to_first_batch_ms: Option<u64>,
    time_to_first_destination_commit_ms: Option<u64>,
    time_to_first_acknowledgement_ms: Option<u64>,
    destination_schema: String,
    destination_row_semantics: String,
    destination_rows: u64,
    destination_through_block: u64,
    destination_table_bytes: u64,
    destination_index_bytes: u64,
    destination_total_bytes: u64,
    destination_wal_written_bytes: u64,
    batch_samples: Vec<DeliveryBatchSample>,
    live_batches: u64,
    live_processed_blocks: u64,
    live_domain_events: u64,
    live_raw_payload_bytes: u64,
    live_acknowledgements: u64,
    live_acknowledged_sequence: Option<String>,
    live_destination_digest: String,
    time_to_first_live_batch_ms: Option<u64>,
}

#[derive(Debug)]
struct LiveProducerMeasurement {
    applied_blocks: u64,
    time_to_first_commit_ms: Option<u64>,
    commit_latencies_us: Vec<u64>,
}

#[derive(Clone, Debug)]
struct LiveDestinationMeasurement {
    delivered_blocks: u64,
    delivered_domain_events: u64,
    canonical_output_bytes: u64,
    batches: u64,
    acknowledgements: u64,
    acknowledged_sequence: u64,
    destination_digest: String,
    time_to_first_destination_batch_ms: Option<u64>,
}

const SWEEP_SCHEMA: &str = "leani.benchmark-sweep.v2";

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BenchmarkSweepManifest {
    schema: String,
    order_seed: u64,
    base_arguments: Vec<String>,
    candidates: Vec<BenchmarkSweepCandidate>,
    #[serde(default)]
    gates: BenchmarkSweepGates,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BenchmarkSweepCandidate {
    id: String,
    arguments: Vec<String>,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BenchmarkSweepGates {
    #[serde(default = "default_sweep_minimum_measured_runs")]
    minimum_measured_runs: usize,
    #[serde(
        rename = "maximumCoefficientOfVariation",
        default = "default_sweep_maximum_coefficient_of_variation"
    )]
    coefficient_of_variation: f64,
    #[serde(rename = "maximumPeakRssBytes")]
    peak_rss_bytes: Option<u64>,
    #[serde(rename = "maximumPeakPhysicalStoreBytes")]
    peak_physical_store_bytes: Option<u64>,
    #[serde(rename = "maximumPeakDeliveryRetainedBytes")]
    peak_delivery_retained_bytes: Option<u64>,
    maximum_live_p95_commit_latency_us: Option<u64>,
}

impl Default for BenchmarkSweepGates {
    fn default() -> Self {
        Self {
            minimum_measured_runs: default_sweep_minimum_measured_runs(),
            coefficient_of_variation: default_sweep_maximum_coefficient_of_variation(),
            peak_rss_bytes: None,
            peak_physical_store_bytes: None,
            peak_delivery_retained_bytes: None,
            maximum_live_p95_commit_latency_us: None,
        }
    }
}

const fn default_sweep_minimum_measured_runs() -> usize {
    3
}

const fn default_sweep_maximum_coefficient_of_variation() -> f64 {
    0.05
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BenchmarkSweepSummary {
    schema: &'static str,
    generated_at_unix_ms: u64,
    manifest_blake3: String,
    executable_blake3: String,
    candidate_order: Vec<String>,
    pareto_frontier: Vec<String>,
    candidates: Vec<BenchmarkSweepEvaluation>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BenchmarkSweepEvaluation {
    id: String,
    candidate_identity: String,
    report: String,
    report_blake3: String,
    source: &'static str,
    correctness_passed: bool,
    measured_runs: usize,
    coefficient_of_variation: f64,
    median_blocks_per_second_milli: u64,
    peak_rss_bytes: Option<u64>,
    peak_physical_store_bytes: Option<u64>,
    peak_delivery_retained_bytes: Option<u64>,
    peak_live_p95_commit_latency_us: Option<u64>,
    eligible: bool,
    rejection_reasons: Vec<String>,
    pareto: bool,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct QueryMeasurement {
    elapsed_milliseconds: u64,
    row_count: RowCountCheck,
    payload_bytes: u64,
    canonical_output_bytes: u64,
    digest: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BenchmarkSummary {
    runs: usize,
    median_elapsed_milliseconds: u64,
    p95_elapsed_milliseconds: u64,
    median_blocks_per_second_milli: u64,
    p95_blocks_per_second_milli: u64,
    coefficient_of_variation: f64,
    peak_rss_bytes: Option<u64>,
    peak_physical_store_bytes: Option<u64>,
    peak_delivery_retained_bytes: Option<u64>,
    peak_history_delivery_retained_bytes: Option<u64>,
    peak_pending_delta_bytes: Option<u64>,
    peak_processor_artifact_bytes: Option<u64>,
    peak_pending_processor_artifact_bytes: Option<u64>,
    peak_active_material_acquisitions: Option<u64>,
    peak_material_buffered_bytes: Option<u64>,
}

#[derive(Debug)]
struct Sampler {
    cancellation: CancellationToken,
    samples: Arc<Mutex<Vec<BenchmarkSample>>>,
    task: tokio::task::JoinHandle<()>,
}

struct SubscriptionFixture {
    store: SqliteStore,
    source: Arc<GeneratedHistorySource>,
    raw_history_store: Option<HistoryStore>,
    raw_replay: Option<RawReplayContext>,
    measurement_started: Option<Instant>,
    processor: Arc<dyn Processor>,
    runtime: HistoricalRuntime,
    material_coordinator: HistoricalMaterialCoordinator,
    job: BackfillJob,
    source_budget: SourceBudget,
    stream_id: String,
    consumer_id: &'static str,
    _directory: tempfile::TempDir,
}

#[derive(Clone, Debug)]
struct BenchmarkBackfillControl {
    status: ApiBackfillStatus,
}

#[derive(Debug)]
struct BenchmarkApiServer {
    base_url: String,
    cancellation: CancellationToken,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
}

#[async_trait]
impl BackfillControl for BenchmarkBackfillControl {
    async fn create_historical_work(
        &self,
        _request: CreateBackfillRequest,
        _owner: HistoricalWorkOwner,
    ) -> Result<ApiBackfillStatus, BackfillControlError> {
        Err(BackfillControlError::Unavailable(
            "the benchmark fixture is read-only".to_owned(),
        ))
    }

    async fn list(
        &self,
        owner: Option<HistoricalWorkOwner>,
    ) -> Result<Vec<ApiBackfillStatus>, BackfillControlError> {
        Ok((owner.is_none() || owner == Some(self.status.owner))
            .then(|| self.status.clone())
            .into_iter()
            .collect())
    }

    async fn inspect(&self, id: &str) -> Result<ApiBackfillStatus, BackfillControlError> {
        if id == self.status.id {
            Ok(self.status.clone())
        } else {
            Err(BackfillControlError::NotFound(id.to_owned()))
        }
    }

    async fn cancel(&self, id: &str) -> Result<ApiBackfillStatus, BackfillControlError> {
        Err(BackfillControlError::Conflict(format!(
            "benchmark subscription {id} cannot be cancelled"
        )))
    }

    async fn delete(
        &self,
        id: &str,
    ) -> Result<leani_api::HistoricalWorkDeletion, BackfillControlError> {
        Err(BackfillControlError::Conflict(format!(
            "benchmark subscription {id} cannot be deleted"
        )))
    }
}

/// Measure real historical acquisition through the production processor and
/// acknowledged HTTP delivery path.
#[allow(clippy::too_many_lines)]
pub(crate) async fn run_real_source(
    config_path: &Path,
    options: RealSourceBenchmarkOptions,
    registry: &ProcessorRegistry,
) -> Result<Exit> {
    validate_real_source_options(&options)?;
    let mut config = Config::load(config_path)
        .with_context(|| format!("load configuration {}", config_path.display()))?;
    config.data_dir.clone_from(&options.data_dir);
    if let Some(source_concurrency) = options.source_concurrency {
        config.budgets.source_concurrency = source_concurrency;
    }
    if let Some(mapper_concurrency) = options.mapper_concurrency {
        config.budgets.mapper_concurrency = mapper_concurrency;
    }
    if let Some(maximum_active_chunks) = options.maximum_active_chunks {
        config.budgets.history_pipeline.maximum_active_chunks = maximum_active_chunks;
    }
    let config = config
        .validate()
        .map_err(|errors| anyhow!(errors))?
        .into_inner();
    if config.chain.chain_id != 1 {
        bail!("real-source benchmarks currently support Ethereum mainnet only");
    }
    let database_path = options.data_dir.join("leani.sqlite");
    if database_path.exists() {
        bail!(
            "{} already exists; real-source benchmarks require a fresh data directory",
            database_path.display()
        );
    }
    fs::create_dir_all(&options.data_dir).with_context(|| {
        format!(
            "create real-source benchmark directory {}",
            options.data_dir.display()
        )
    })?;

    let configured = select_processor_config(&config, &options.processor)?;
    let configured_processor = registry.instantiate(configured, config.chain.chain_id)?;
    let processor = externalized_real_source_processor(configured_processor.as_ref())?;
    let range = BlockRange::new(
        BlockNumber(options.from_block),
        BlockNumber(options.to_block),
    )?;
    let p2p_network_telemetry = NetworkTelemetry::default();
    let mut p2p_request_metrics_source = None;
    let (sources, verification_policy) =
        if matches!(options.source_policy, BenchmarkRealSourcePolicy::P2pOnly) {
            let anchor = verified_p2p_history_anchor(&config).await?;
            let available_start = config
                .sources
                .live
                .history_fallback_start(anchor.block.number.0, configured.start_block);
            if range.start().0 < available_start || range.end() > anchor.block.number {
                bail!(
                    "P2P benchmark range {}..={} is outside configured finalized fallback {}..={}",
                    range.start().0,
                    range.end().0,
                    available_start,
                    anchor.block.number.0
                );
            }
            let available = BlockRange::new(BlockNumber(available_start), anchor.block.number)?;
            let persistent = execution_p2p_source(&config, p2p_network_telemetry.clone())?;
            p2p_request_metrics_source = Some(persistent.clone());
            let source = leani_source_p2p::RethP2pHistorySource::from_persistent_source(
                persistent.as_ref().clone(),
                available,
                anchor,
            )?;
            (
                vec![Arc::new(source) as Arc<dyn HistorySource>],
                VerificationPolicy::CompleteCryptographic,
            )
        } else {
            let (available_sources, verification_policy) =
                configured_history_sources(&config, processor.as_ref(), None)?;
            (
                select_real_sources(&config, available_sources, options.source_policy)?,
                verification_policy,
            )
        };
    let source_before = sources
        .iter()
        .map(|source| source.acquisition_metrics())
        .collect::<Vec<_>>();
    let source_budget = configured_real_source_budget(&config, range);
    let store = SqliteStore::open(configured_store_config(&config, &database_path)).await?;
    store.register_processor(processor.descriptor()).await?;
    let limits = configured_real_delivery_limits(&config, options.delivery_compression);
    let consumer_id = "real-source-benchmark";
    let (job, stream_id) = create_real_source_subscription(
        &store,
        processor.as_ref(),
        range,
        verification_policy,
        &limits,
        consumer_id,
    )
    .await?;
    let pipeline = configured_real_pipeline(&config)?;
    let coordinator = configured_real_coordinator(&config, &pipeline)?;
    let mut runtime = HistoricalRuntime::new_with_sources(
        store.clone(),
        sources.clone(),
        processor.clone(),
        configured_real_runtime(&config),
    )?
    .with_pipeline_budget(pipeline);
    if let Some(coordinator) = coordinator.as_ref() {
        runtime = runtime.with_material_coordinator(coordinator.clone());
    }
    let control = BenchmarkBackfillControl {
        status: real_source_backfill_status(&job, processor.descriptor(), range, &limits),
    };
    let server =
        BenchmarkApiServer::start_with(store.clone(), processor.clone(), control, limits.api)
            .await?;
    let pruner =
        BenchmarkDeliveryPruner::start(store.clone(), processor.descriptor().clone(), stream_id);
    let sampler = Sampler::start_real(
        options.sample_interval_ms,
        sources.clone(),
        store.clone(),
        processor.descriptor().clone(),
        coordinator,
    );
    let started = Instant::now();
    let run_cancellation = CancellationToken::new();
    let measured = Box::pin(tokio::time::timeout(
        Duration::from_secs(options.timeout_seconds),
        async {
            let producer = async {
                let report = runtime
                    .run(job.clone(), source_budget, run_cancellation.clone())
                    .await?;
                Ok::<_, anyhow::Error>((report, elapsed_milliseconds(started)))
            };
            let consumer = async {
                let delivery = consume_subscription_http(
                    &store,
                    &server.base_url,
                    &job.id,
                    consumer_id,
                    options.consumer_delay_ms,
                    options.delivery_compression,
                    started,
                )
                .await?;
                Ok::<_, anyhow::Error>((delivery, elapsed_milliseconds(started)))
            };
            tokio::try_join!(producer, consumer)
        },
    ))
    .await;
    run_cancellation.cancel();

    let mut error = None;
    let (runtime_report, mut delivery, producer_complete, consumer_complete) = match measured {
        Ok(Ok(((runtime, producer_ms), (delivery, consumer_ms)))) => (
            Some(runtime),
            Some(delivery),
            Some(producer_ms),
            Some(consumer_ms),
        ),
        Ok(Err(run_error)) => {
            error = Some(format!("real-source pipeline: {run_error:#}"));
            (None, None, None, None)
        }
        Err(_) => {
            error = Some(format!(
                "real-source pipeline timed out after {} seconds",
                options.timeout_seconds
            ));
            (None, None, None, None)
        }
    };
    if let Some(delivery) = delivery.as_mut()
        && let Some(consumer_ms) = consumer_complete
    {
        finalize_delivery_throughput(delivery, range.len(), consumer_ms, producer_complete);
    }
    let pruned_record_count = pruner.stop().await;
    match (delivery.as_mut(), pruned_record_count) {
        (Some(delivery), Ok(record_count)) => {
            delivery.pruned_records = delivery.pruned_records.saturating_add(record_count);
        }
        (_, Err(prune_error)) if error.is_none() => {
            error = Some(format!("delivery pruning: {prune_error:#}"));
        }
        _ => {}
    }
    if let Err(server_error) = server.stop().await
        && error.is_none()
    {
        error = Some(format!("benchmark HTTP server: {server_error:#}"));
    }
    let sample_records = sampler.finish().await;
    let peaks = benchmark_sample_peaks(&sample_records);
    let time_to_first_source_frame_ms = sample_records
        .iter()
        .find(|sample| sample.source_frames > 0)
        .map(|sample| sample.elapsed_milliseconds)
        .map(|milestone| producer_complete.map_or(milestone, |upper| milestone.min(upper)));
    // Summed source counters can double-count reacquired frames after a
    // partial failed attempt, so only publish this milestone for the direct
    // single-attempt case.
    let time_to_source_complete_ms = (runtime_report
        .as_ref()
        .is_some_and(|report| report.source_attempts == 1))
    .then(|| {
        sample_records
            .iter()
            .find(|sample| sample.source_frames >= range.len())
            .map(|sample| sample.elapsed_milliseconds)
            .map(|milestone| producer_complete.map_or(milestone, |upper| milestone.min(upper)))
    })
    .flatten();
    let time_to_first_processor_commit_ms = sample_records
        .iter()
        .find(|sample| sample.committed_through.is_some())
        .map(|sample| sample.elapsed_milliseconds)
        .map(|milestone| producer_complete.map_or(milestone, |upper| milestone.min(upper)));
    if let Err(compaction_error) =
        compact_coverage_through(&store, processor.descriptor(), range.end()).await
        && error.is_none()
    {
        error = Some(format!("coverage compaction: {compaction_error:#}"));
    }
    if let Err(verification_error) = store.verify().await
        && error.is_none()
    {
        error = Some(format!("database verification: {verification_error:#}"));
    }
    let store_stats = store.stats().await.ok();
    let processor_store = store.processor_stats(processor.descriptor()).await.ok();
    let source_measurements = real_source_measurements(&sources, &source_before);
    let elapsed = elapsed_milliseconds(started);
    let pipeline_correctness_passed = error.is_none()
        && runtime_report.as_ref().is_some_and(|runtime| {
            runtime.requested == range
                && runtime.frames_mapped == range.len()
                && runtime.frames_committed == range.len()
                && runtime.final_coverage == vec![range]
        })
        && delivery.as_ref().is_some_and(|delivery| {
            delivery.processed_blocks == range.len()
                && delivery.completion_records == 1
                && delivery.acknowledgements > 0
        })
        && processor_store
            .as_ref()
            .is_some_and(|stats| stats.pending_deltas == 0);
    let output_digest = delivery
        .as_ref()
        .map(|delivery| delivery.destination_digest.clone());
    let output_digest_matches_expected = options
        .expected_output_digest
        .as_ref()
        .map(|expected| output_digest.as_deref() == Some(expected.as_str()));
    let correctness_passed =
        pipeline_correctness_passed && output_digest_matches_expected.unwrap_or(true);
    if !correctness_passed && error.is_none() {
        error = Some(if pipeline_correctness_passed {
            format!(
                "real-source output digest mismatch: expected {}, observed {}",
                options
                    .expected_output_digest
                    .as_deref()
                    .unwrap_or("missing"),
                output_digest.as_deref().unwrap_or("missing"),
            )
        } else {
            "real-source pipeline correctness contract failed".to_owned()
        });
    }
    let report = RealSourceBenchmarkReport {
        report_version: REAL_SOURCE_REPORT_VERSION,
        report_schema: REAL_SOURCE_REPORT_SCHEMA,
        status: if correctness_passed {
            "passed"
        } else {
            "failed"
        },
        generated_at_unix_ms: now_milliseconds(),
        node_version: env!("CARGO_PKG_VERSION"),
        git_revision: git_output(&["rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".to_owned()),
        git_dirty: git_output(&["status", "--porcelain"]).is_some_and(|value| !value.is_empty()),
        build_profile: build_profile(),
        host: BenchmarkHost {
            os: std::env::consts::OS,
            architecture: std::env::consts::ARCH,
            logical_cpus: std::thread::available_parallelism().map_or(1, usize::from),
        },
        processor: processor_identity(processor.descriptor())?,
        product_profile: "externalized",
        destination: "rust_http_hash_and_ack",
        source_policy: real_source_policy_name(options.source_policy),
        range,
        requested_blocks: range.len(),
        config_path: config_path.display().to_string(),
        data_dir: options.data_dir.display().to_string(),
        timeout_seconds: options.timeout_seconds,
        sample_interval_ms: options.sample_interval_ms,
        consumer_delay_ms: options.consumer_delay_ms,
        delivery_compression: benchmark_compression_name(options.delivery_compression),
        tuning: RealSourceRuntimeTuning {
            source_concurrency: config.budgets.source_concurrency,
            mapper_concurrency: config.budgets.mapper_concurrency,
            maximum_active_chunks: config.budgets.history_pipeline.maximum_active_chunks,
            maximum_mapped_bytes: config.budgets.history_pipeline.maximum_mapped_bytes.bytes(),
            p2p_material_request_concurrency: config.sources.live.material_request_concurrency,
            p2p_material_request_blocks: config.sources.live.material_request_blocks,
            p2p_header_request_concurrency: config.sources.live.history_header_request_concurrency,
            p2p_header_request_blocks: config.sources.live.history_header_request_blocks,
            p2p_request_timeout_seconds: config.sources.live.request_timeout_seconds,
            p2p_request_retries: config.sources.live.request_retries,
            p2p_request_retry_backoff_ms: config.sources.live.request_retry_backoff_ms,
            p2p_persistent_retries: config.sources.live.persistent_retries,
        },
        source_budget,
        sources: source_measurements,
        p2p_network: matches!(options.source_policy, BenchmarkRealSourcePolicy::P2pOnly)
            .then(|| p2p_network_telemetry.snapshot()),
        p2p_requests: p2p_request_metrics_source.map(|source| source.request_metrics()),
        runtime: runtime_report,
        delivery: delivery.clone(),
        elapsed_milliseconds: elapsed,
        time_to_first_source_frame_ms,
        time_to_source_complete_ms,
        time_to_first_processor_commit_ms,
        producer_complete_milliseconds: producer_complete,
        consumer_complete_milliseconds: consumer_complete,
        blocks_per_second_milli: consumer_complete
            .map(|consumer_ms| throughput_per_second_milli(range.len(), consumer_ms)),
        peak_rss_bytes: peaks.rss_bytes,
        peak_physical_store_bytes: peaks.physical_store_bytes,
        peak_delivery_retained_bytes: peaks.delivery_retained_bytes,
        peak_material_buffered_bytes: peaks.material_buffered_bytes,
        store: store_stats,
        processor_store,
        output_digest,
        expected_output_digest: options.expected_output_digest.clone(),
        output_digest_matches_expected,
        pipeline_correctness_passed,
        correctness_passed,
        error: error.clone(),
    };
    write_immutable(&options.report, &serde_json::to_vec_pretty(&report)?)?;
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "reportVersion": report.report_version,
            "report": options.report.display().to_string(),
            "status": report.status,
            "elapsedMilliseconds": report.elapsed_milliseconds,
            "blocksPerSecondMilli": report.blocks_per_second_milli,
        }))?
    );
    if let Some(error) = error {
        bail!(error);
    }
    Ok(Exit::Success)
}

#[derive(Clone, Copy)]
struct RealDeliveryLimits {
    api: DeliveryBatchLimits,
    store: BackfillDeliveryBatchLimits,
}

fn validate_real_source_options(options: &RealSourceBenchmarkOptions) -> Result<()> {
    if options.from_block > options.to_block {
        bail!("real-source benchmark range is inverted");
    }
    if options.timeout_seconds == 0 {
        bail!("real-source benchmark timeout must be greater than zero");
    }
    if options.sample_interval_ms == 0 {
        bail!("real-source benchmark sample interval must be greater than zero");
    }
    if let Some(digest) = options.expected_output_digest.as_deref()
        && (digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()))
    {
        bail!("expected output digest must be a 64-character hexadecimal BLAKE3 digest");
    }
    if options.report.exists() {
        bail!(
            "{} already exists; benchmark reports are immutable",
            options.report.display()
        );
    }
    Ok(())
}

fn externalized_real_source_processor(processor: &dyn Processor) -> Result<Arc<dyn Processor>> {
    let descriptor = processor.descriptor();
    let lifecycle =
        benchmark_profile_lifecycle(&descriptor.lifecycle, BenchmarkProductProfile::Externalized);
    let instance = descriptor.instance.clone();
    if let Some(blobs) = processor.as_any().downcast_ref::<BlobsProcessor>() {
        return Ok(Arc::new(blobs.clone().with_contract(
            instance,
            PublicationPolicy::FinalizedOnly,
            lifecycle,
        )));
    }
    if let Some(uniswap) = processor
        .as_any()
        .downcast_ref::<UniswapObservationsProcessor>()
    {
        return Ok(Arc::new(uniswap.clone().with_contract(
            instance,
            PublicationPolicy::FinalizedOnly,
            lifecycle,
        )));
    }
    bail!(
        "real-source externalized benchmark currently supports blobs-money and uniswap-observations processors"
    )
}

fn select_real_sources(
    config: &Config,
    sources: Vec<Arc<dyn HistorySource>>,
    policy: BenchmarkRealSourcePolicy,
) -> Result<Vec<Arc<dyn HistorySource>>> {
    let erae_ids = config
        .sources
        .history
        .iter()
        .filter(|source| matches!(source.kind, crate::config::HistorySourceKind::EraE))
        .map(|source| source.id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let selected = sources
        .into_iter()
        .filter(|source| match policy {
            BenchmarkRealSourcePolicy::XatuOnly => {
                source.descriptor().kind == SourceKind::PublicDataset
            }
            BenchmarkRealSourcePolicy::EraeOnly => {
                erae_ids.contains(source.descriptor().id.as_str())
            }
            BenchmarkRealSourcePolicy::P2pOnly => false,
            BenchmarkRealSourcePolicy::HistoryPortfolio => true,
        })
        .collect::<Vec<_>>();
    if selected.is_empty() {
        bail!(
            "source policy {} has no compatible configured source",
            real_source_policy_name(policy)
        );
    }
    Ok(selected)
}

const fn real_source_policy_name(policy: BenchmarkRealSourcePolicy) -> &'static str {
    match policy {
        BenchmarkRealSourcePolicy::XatuOnly => "xatu_only",
        BenchmarkRealSourcePolicy::EraeOnly => "erae_only",
        BenchmarkRealSourcePolicy::P2pOnly => "p2p_only",
        BenchmarkRealSourcePolicy::HistoryPortfolio => "history_portfolio",
    }
}

const fn build_profile() -> &'static str {
    if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    }
}

fn configured_real_source_budget(config: &Config, range: BlockRange) -> SourceBudget {
    SourceBudget {
        max_input_bytes: config.budgets.temporary_disk_bytes,
        max_frame_bytes: config.budgets.memory_bytes.min(32 * 1_024 * 1_024),
        max_frames: range.len(),
        max_buffered_frames: config
            .budgets
            .mapper_concurrency
            .max(config.budgets.source_concurrency),
        max_in_flight_requests: config.budgets.source_concurrency,
        temporary_disk_bytes: config.budgets.temporary_disk_bytes,
    }
}

fn configured_real_runtime(config: &Config) -> HistoricalRuntimeConfig {
    let history = config.budgets.history_pipeline;
    HistoricalRuntimeConfig {
        mapper_concurrency: config.budgets.mapper_concurrency,
        maximum_active_chunks: history.maximum_active_chunks,
        maximum_mapped_bytes: history.maximum_mapped_bytes.bytes(),
        commit_maximum_blocks: history.commit.maximum_blocks,
        commit_maximum_changes: history.commit.maximum_changes,
        commit_maximum_encoded_bytes: history.commit.maximum_encoded_bytes.bytes(),
        commit_maximum_delay: Duration::from_millis(history.commit.maximum_delay.milliseconds()),
        commit_target_writer_hold: Duration::from_millis(
            history.commit.target_writer_hold.milliseconds(),
        ),
        ..HistoricalRuntimeConfig::default()
    }
}

fn configured_real_pipeline(config: &Config) -> Result<HistoricalPipelineBudget> {
    let history = config.budgets.history_pipeline;
    Ok(HistoricalPipelineBudget::new(
        history.maximum_active_chunks,
        config.budgets.mapper_concurrency,
        history.maximum_mapped_bytes.bytes(),
    )?)
}

fn configured_real_coordinator(
    config: &Config,
    pipeline: &HistoricalPipelineBudget,
) -> Result<Option<HistoricalMaterialCoordinator>> {
    let history = config.budgets.history_material;
    if matches!(
        history.mode,
        crate::config::HistoryMaterialCoordinatorMode::Disabled
    ) {
        return Ok(None);
    }
    let mode = match history.mode {
        crate::config::HistoryMaterialCoordinatorMode::Observe => {
            leani_runtime::HistoricalMaterialCoordinatorMode::Observe
        }
        crate::config::HistoryMaterialCoordinatorMode::Enabled => {
            leani_runtime::HistoricalMaterialCoordinatorMode::Enabled
        }
        crate::config::HistoryMaterialCoordinatorMode::Disabled => unreachable!(),
    };
    Ok(Some(
        HistoricalMaterialCoordinator::new_with_pipeline_budget(
            HistoricalMaterialCoordinatorConfig {
                mode,
                memory_bytes: history.memory_bytes.bytes(),
                maximum_buffered_frames_per_acquisition: history
                    .maximum_buffered_frames_per_acquisition,
                minimum_physical_chunk_blocks: history.minimum_physical_chunk_blocks,
                maximum_overfetch_ratio: history.maximum_overfetch_ratio,
            },
            pipeline,
        )?,
    ))
}

fn configured_real_delivery_limits(
    config: &Config,
    compression: BenchmarkCompression,
) -> RealDeliveryLimits {
    let configured = config.api.delivery.history_batches;
    RealDeliveryLimits {
        api: DeliveryBatchLimits {
            target_encoded_bytes: configured.target_encoded_bytes.bytes(),
            maximum_encoded_bytes: configured.maximum_encoded_bytes.bytes(),
            maximum_events: configured.maximum_events,
            maximum_processed_blocks: configured.maximum_processed_blocks,
            maximum_delay: Duration::from_millis(configured.maximum_delay.milliseconds()),
            maximum_buffered_batches: usize::try_from(configured.maximum_buffered_batches)
                .unwrap_or(usize::MAX),
            maximum_buffered_bytes: configured.maximum_buffered_bytes.bytes(),
            compression: api_compression(compression),
        },
        store: BackfillDeliveryBatchLimits {
            target_encoded_bytes: configured.target_encoded_bytes.bytes(),
            maximum_encoded_bytes: configured.maximum_encoded_bytes.bytes(),
            maximum_events: configured.maximum_events,
            maximum_processed_blocks: configured.maximum_processed_blocks,
            maximum_delay_ms: configured.maximum_delay.milliseconds(),
            maximum_buffered_batches: configured.maximum_buffered_batches,
            maximum_buffered_bytes: configured.maximum_buffered_bytes.bytes(),
            compression: store_compression(compression),
        },
    }
}

async fn create_real_source_subscription(
    store: &SqliteStore,
    processor: &dyn Processor,
    range: BlockRange,
    verification_policy: VerificationPolicy,
    limits: &RealDeliveryLimits,
    consumer_id: &str,
) -> Result<(BackfillJob, String)> {
    let subscription_id = format!(
        "real-source-{}-{}-{}",
        processor.descriptor().instance,
        range.start().0,
        range.end().0
    );
    let stream_id = store
        .create_backfill_delivery_stream(processor.descriptor(), &subscription_id)
        .await?
        .stream_id;
    create_required_benchmark_consumer(store, processor.descriptor(), &stream_id, consumer_id)
        .await?;
    let mut job = BackfillJob::for_processor(
        subscription_id.clone(),
        processor,
        ChainId(1),
        range,
        verification_policy,
    )?;
    job.owner = HistoricalJobOwner::Subscription;
    job.delivery_stream_id = Some(stream_id.clone());
    let record = JobRecord {
        id: job.id.clone(),
        kind: job.owner.job_kind().to_owned(),
        state: JobState::Queued,
        payload: serde_json::to_vec(&job)?,
        checkpoint: None,
        attempts: 0,
        updated_at_unix_ms: now_milliseconds(),
    };
    store
        .create_backfill_subscription_job(
            &BackfillSubscriptionRecord {
                subscription_id: subscription_id.clone(),
                job_id: job.id.clone(),
                processor_instance: processor.descriptor().instance.to_string(),
                history_stream_id: stream_id.clone(),
                mode: BackfillSubscriptionMode::FillMissing,
                publication_revision: 0,
                state: BackfillSubscriptionState::Queued,
                consumer_id: consumer_id.to_owned(),
                ranges: vec![range],
                range,
                preexisting_coverage: Vec::new(),
                captured_finalized_target: range.end(),
                idempotency_key: subscription_id,
                effective_block_limit: range.len().clamp(1, 16_384),
                effective_byte_limit: processor.descriptor().lifecycle.delivery.max_bytes,
                resume_below_ratio_millionths: 750_000,
                delivery_batch_limits: limits.store,
                initial_sequence: 0,
                completion_sequence: None,
                processed_work_blocks: 0,
            },
            &record,
            leani_primitives::BlockHash::new([7; 32]),
        )
        .await?;
    Ok((job, stream_id))
}

fn real_source_backfill_status(
    job: &BackfillJob,
    descriptor: &ProcessorDescriptor,
    range: BlockRange,
    limits: &RealDeliveryLimits,
) -> ApiBackfillStatus {
    ApiBackfillStatus {
        id: job.id.clone(),
        owner: HistoricalWorkOwner::Subscription,
        processor: descriptor.instance.to_string(),
        delivery_stream_id: job.delivery_stream_id.clone(),
        publication_revision: Some("0".to_owned()),
        from_block: range.start().0,
        to_block: range.end().0,
        ranges: vec![ApiBackfillRange {
            from_block: range.start().0,
            to_block: range.end().0,
        }],
        requested_blocks: range.len(),
        processed_blocks: 0,
        remaining_blocks: range.len(),
        captured_finalized_target: Some(range.end().0),
        mode: BackfillExecutionMode::FillMissing,
        batching: Some(EffectiveBackfillBatching {
            target_encoded_bytes: limits.store.target_encoded_bytes,
            maximum_encoded_bytes: limits.store.maximum_encoded_bytes,
            maximum_events: limits.store.maximum_events,
            maximum_processed_blocks: limits.store.maximum_processed_blocks,
            maximum_delay_ms: limits.store.maximum_delay_ms,
            maximum_buffered_batches: limits.store.maximum_buffered_batches,
            maximum_buffered_bytes: limits.store.maximum_buffered_bytes,
            compression: match limits.store.compression {
                BackfillDeliveryCompression::None => DeliveryCompression::None,
                BackfillDeliveryCompression::Gzip => DeliveryCompression::Gzip,
            },
        }),
        state: ApiBackfillState::Running,
        attempts: 0,
        updated_at_unix_ms: now_milliseconds(),
        report: None,
        last_error: None,
    }
}

fn real_source_measurements(
    sources: &[Arc<dyn HistorySource>],
    before: &[Option<SourceAcquisitionMetrics>],
) -> Vec<RealSourceMeasurement> {
    sources
        .iter()
        .zip(before)
        .map(|(source, before)| RealSourceMeasurement {
            id: source.descriptor().id.to_string(),
            kind: source.descriptor().kind,
            priority: source.descriptor().priority,
            metrics: source
                .acquisition_metrics()
                .map(|after| acquisition_metrics_delta(after, before.as_ref())),
        })
        .collect()
}

fn acquisition_metrics_delta(
    after: SourceAcquisitionMetrics,
    before: Option<&SourceAcquisitionMetrics>,
) -> SourceAcquisitionMetrics {
    let before = before.cloned().unwrap_or_default();
    SourceAcquisitionMetrics {
        opened_chunks: after.opened_chunks.saturating_sub(before.opened_chunks),
        opened_ranges: after
            .opened_ranges
            .into_iter()
            .skip(before.opened_ranges.len())
            .collect(),
        acquired_frames: after.acquired_frames.saturating_sub(before.acquired_frames),
        normalized_bytes: after
            .normalized_bytes
            .saturating_sub(before.normalized_bytes),
        logical_range_requests: optional_counter_delta(
            after.logical_range_requests,
            before.logical_range_requests,
        ),
        physical_reads: optional_counter_delta(after.physical_reads, before.physical_reads),
        fetched_bytes: optional_counter_delta(after.fetched_bytes, before.fetched_bytes),
        source_objects: optional_counter_delta(after.source_objects, before.source_objects),
        source_object_bytes: optional_counter_delta(
            after.source_object_bytes,
            before.source_object_bytes,
        ),
        projected_compressed_bytes: optional_counter_delta(
            after.projected_compressed_bytes,
            before.projected_compressed_bytes,
        ),
        rows_scanned: optional_counter_delta(after.rows_scanned, before.rows_scanned),
        rows_selected: optional_counter_delta(after.rows_selected, before.rows_selected),
        operation_elapsed_ms: after
            .operation_elapsed_ms
            .saturating_sub(before.operation_elapsed_ms),
    }
}

fn optional_counter_delta(after: Option<u64>, before: Option<u64>) -> Option<u64> {
    after.map(|after| after.saturating_sub(before.unwrap_or(0)))
}

fn processor_identity(descriptor: &ProcessorDescriptor) -> Result<BenchmarkProcessorIdentity> {
    Ok(BenchmarkProcessorIdentity {
        instance: descriptor.instance.to_string(),
        descriptor_hash: blake3::hash(&serde_json::to_vec(descriptor)?).to_string(),
        schema_hash: blake3::hash(&serde_json::to_vec(&descriptor.schemas)?).to_string(),
    })
}

/// Run or resume a deterministic manifest-defined benchmark sweep.
///
/// Each candidate writes one immutable benchmark report and samples file. A
/// rerun reuses only complete, valid reports and reconstructs the frontier, so
/// interruption never requires repeating successful candidates.
pub(crate) async fn run_sweep(manifest_path: &Path, output_directory: &Path) -> Result<Exit> {
    let manifest_bytes = fs::read(manifest_path)
        .with_context(|| format!("read benchmark sweep manifest {}", manifest_path.display()))?;
    let manifest: BenchmarkSweepManifest =
        serde_json::from_slice(&manifest_bytes).with_context(|| {
            format!(
                "decode benchmark sweep manifest {}",
                manifest_path.display()
            )
        })?;
    validate_sweep_manifest(&manifest)?;
    fs::create_dir_all(output_directory).with_context(|| {
        format!(
            "create benchmark sweep output directory {}",
            output_directory.display()
        )
    })?;

    let mut candidates = manifest.candidates.clone();
    candidates.sort_by_key(|candidate| sweep_order_key(manifest.order_seed, &candidate.id));
    let candidate_order = candidates
        .iter()
        .map(|candidate| candidate.id.clone())
        .collect::<Vec<_>>();
    let executable = std::env::current_exe().context("resolve current benchmark executable")?;
    let executable_blake3 = file_blake3(&executable)?;
    let mut evaluations = Vec::with_capacity(candidates.len());

    for candidate in candidates {
        evaluations.push(
            run_or_resume_sweep_candidate(
                &executable,
                &executable_blake3,
                &manifest.base_arguments,
                &candidate,
                output_directory,
                manifest.gates,
            )
            .await?,
        );
    }

    mark_pareto_frontier(&mut evaluations);
    let pareto_frontier = evaluations
        .iter()
        .filter(|candidate| candidate.pareto)
        .map(|candidate| candidate.id.clone())
        .collect::<Vec<_>>();
    let summary = BenchmarkSweepSummary {
        schema: SWEEP_SCHEMA,
        generated_at_unix_ms: now_milliseconds(),
        manifest_blake3: blake3::hash(&manifest_bytes).to_string(),
        executable_blake3,
        candidate_order,
        pareto_frontier,
        candidates: evaluations,
    };
    let summary_path = output_directory.join("sweep-summary.json");
    let temporary_path = output_directory.join(".sweep-summary.json.tmp");
    let encoded = serde_json::to_vec_pretty(&summary)?;
    fs::write(&temporary_path, &encoded)
        .with_context(|| format!("write temporary sweep summary {}", temporary_path.display()))?;
    fs::rename(&temporary_path, &summary_path)
        .with_context(|| format!("publish sweep summary {}", summary_path.display()))?;
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "schema": SWEEP_SCHEMA,
            "summary": summary_path.display().to_string(),
            "candidates": summary.candidates.len(),
            "paretoFrontier": summary.pareto_frontier,
        }))?
    );
    Ok(Exit::Success)
}

async fn run_or_resume_sweep_candidate(
    executable: &Path,
    executable_blake3: &str,
    base_arguments: &[String],
    candidate: &BenchmarkSweepCandidate,
    output_directory: &Path,
    gates: BenchmarkSweepGates,
) -> Result<BenchmarkSweepEvaluation> {
    let candidate_identity = sweep_candidate_identity(executable_blake3, base_arguments, candidate);
    let basename = format!("{}-{candidate_identity}", candidate.id);
    let report_path = output_directory.join(format!("{basename}.json"));
    let samples_path = output_directory.join(format!("{basename}-samples.ndjson"));
    let resumed = report_path.is_file();
    if resumed {
        if !samples_path.is_file() {
            bail!(
                "candidate {} has a report but no samples file at {}; refusing an incomplete resume",
                candidate.id,
                samples_path.display()
            );
        }
    } else {
        if samples_path.exists() {
            bail!(
                "candidate {} has an orphaned samples file at {}; move it aside before resuming",
                candidate.id,
                samples_path.display()
            );
        }
        let mut arguments =
            Vec::with_capacity(5 + base_arguments.len() + candidate.arguments.len());
        arguments.push("benchmark".to_owned());
        arguments.extend(base_arguments.iter().cloned());
        arguments.extend(candidate.arguments.iter().cloned());
        arguments.push("--samples-report".to_owned());
        arguments.push(samples_path.display().to_string());
        arguments.push("--report".to_owned());
        arguments.push(report_path.display().to_string());
        eprintln!("running benchmark sweep candidate {}", candidate.id);
        let command_executable = executable.to_owned();
        let status = tokio::task::spawn_blocking(move || {
            Command::new(command_executable).args(arguments).status()
        })
        .await
        .context("benchmark sweep candidate task panicked")?
        .context("start benchmark sweep candidate")?;
        if !status.success() {
            bail!(
                "benchmark sweep candidate {} failed with {status}",
                candidate.id
            );
        }
    }
    evaluate_sweep_candidate(
        &candidate.id,
        &candidate_identity,
        &report_path,
        resumed,
        gates,
    )
}

fn validate_sweep_manifest(manifest: &BenchmarkSweepManifest) -> Result<()> {
    if manifest.schema != SWEEP_SCHEMA {
        bail!(
            "unsupported benchmark sweep schema {:?}; expected {SWEEP_SCHEMA}",
            manifest.schema
        );
    }
    if manifest.candidates.is_empty() {
        bail!("benchmark sweep requires at least one candidate");
    }
    if !manifest.gates.coefficient_of_variation.is_finite()
        || manifest.gates.coefficient_of_variation < 0.0
    {
        bail!("benchmark sweep maximum coefficient of variation is invalid");
    }
    if manifest.gates.minimum_measured_runs == 0 {
        bail!("benchmark sweep minimum measured runs must be greater than zero");
    }
    validate_sweep_arguments(&manifest.base_arguments, "baseArguments")?;
    let mut ids = std::collections::BTreeSet::new();
    for candidate in &manifest.candidates {
        if candidate.id.is_empty()
            || candidate.id.len() > 80
            || !candidate
                .id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            bail!(
                "benchmark sweep candidate ID {:?} must use 1-80 ASCII letters, digits, '-' or '_'",
                candidate.id
            );
        }
        if !ids.insert(candidate.id.as_str()) {
            bail!("duplicate benchmark sweep candidate ID {:?}", candidate.id);
        }
        validate_sweep_arguments(
            &candidate.arguments,
            &format!("candidate {:?} arguments", candidate.id),
        )?;
    }
    Ok(())
}

fn validate_sweep_arguments(arguments: &[String], field: &str) -> Result<()> {
    for argument in arguments {
        if argument == "benchmark"
            || argument == "sweep"
            || argument == "--report"
            || argument.starts_with("--report=")
            || argument == "--samples-report"
            || argument.starts_with("--samples-report=")
        {
            bail!("benchmark sweep {field} cannot contain orchestration argument {argument:?}");
        }
    }
    Ok(())
}

fn sweep_order_key(seed: u64, id: &str) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"leani.benchmark-sweep.order.v1");
    hasher.update(&seed.to_be_bytes());
    hasher.update(id.as_bytes());
    *hasher.finalize().as_bytes()
}

fn sweep_candidate_identity(
    executable_blake3: &str,
    base_arguments: &[String],
    candidate: &BenchmarkSweepCandidate,
) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"leani.benchmark-sweep.candidate.v1");
    hasher.update(executable_blake3.as_bytes());
    for argument in base_arguments
        .iter()
        .chain(std::iter::once(&candidate.id))
        .chain(candidate.arguments.iter())
    {
        hasher.update(
            &u64::try_from(argument.len())
                .unwrap_or(u64::MAX)
                .to_be_bytes(),
        );
        hasher.update(argument.as_bytes());
    }
    hasher.finalize().to_string()
}

fn file_blake3(path: &Path) -> Result<String> {
    let file = fs::File::open(path)
        .with_context(|| format!("open benchmark executable {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut buffer = vec![0_u8; 64 * 1_024];
    let mut hasher = blake3::Hasher::new();
    loop {
        let read = reader
            .read(&mut buffer)
            .with_context(|| format!("hash benchmark executable {}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize().to_string())
}

fn evaluate_sweep_candidate(
    id: &str,
    candidate_identity: &str,
    report_path: &Path,
    resumed: bool,
    gates: BenchmarkSweepGates,
) -> Result<BenchmarkSweepEvaluation> {
    let bytes = fs::read(report_path)
        .with_context(|| format!("read sweep candidate report {}", report_path.display()))?;
    let report: serde_json::Value = serde_json::from_slice(&bytes)
        .with_context(|| format!("decode sweep candidate report {}", report_path.display()))?;
    if report
        .get("reportSchema")
        .and_then(serde_json::Value::as_str)
        != Some(REPORT_SCHEMA)
    {
        bail!(
            "candidate {id} report {} has the wrong report schema",
            report_path.display()
        );
    }
    let summary = report
        .get("summary")
        .context("sweep candidate report has no summary")?;
    let coefficient_of_variation = summary_f64(summary, "coefficientOfVariation")?;
    let median_blocks_per_second_milli = summary_u64(summary, "medianBlocksPerSecondMilli")?;
    let peak_rss_bytes = summary_optional_u64(summary, "peakRssBytes")?;
    let peak_physical_store_bytes = summary_optional_u64(summary, "peakPhysicalStoreBytes")?;
    let peak_delivery_retained_bytes = summary_optional_u64(summary, "peakDeliveryRetainedBytes")?;
    let runs = report
        .get("runs")
        .and_then(serde_json::Value::as_array)
        .context("sweep candidate report has no measured runs")?;
    let correctness_passed = !runs.is_empty()
        && runs.iter().all(|run| {
            run.get("correctnessPassed")
                .and_then(serde_json::Value::as_bool)
                == Some(true)
        });
    let peak_live_p95_commit_latency_us = peak_live_p95_commit_latency(runs)?;
    let mut rejection_reasons = Vec::new();
    if !correctness_passed {
        rejection_reasons.push("correctness_failed".to_owned());
    }
    if runs.len() < gates.minimum_measured_runs {
        rejection_reasons.push("measured_runs".to_owned());
    }
    if coefficient_of_variation > gates.coefficient_of_variation {
        rejection_reasons.push("coefficient_of_variation".to_owned());
    }
    if peak_rss_bytes.is_none() {
        rejection_reasons.push("peak_rss_bytes_missing".to_owned());
    }
    if peak_physical_store_bytes.is_none() {
        rejection_reasons.push("peak_physical_store_bytes_missing".to_owned());
    }
    if peak_delivery_retained_bytes.is_none() {
        rejection_reasons.push("peak_delivery_retained_bytes_missing".to_owned());
    }
    if gates
        .peak_rss_bytes
        .is_some_and(|maximum| peak_rss_bytes.is_some_and(|value| value > maximum))
    {
        rejection_reasons.push("peak_rss_bytes".to_owned());
    }
    if gates
        .peak_physical_store_bytes
        .is_some_and(|maximum| peak_physical_store_bytes.is_some_and(|value| value > maximum))
    {
        rejection_reasons.push("peak_physical_store_bytes".to_owned());
    }
    if gates
        .peak_delivery_retained_bytes
        .is_some_and(|maximum| peak_delivery_retained_bytes.is_some_and(|value| value > maximum))
    {
        rejection_reasons.push("peak_delivery_retained_bytes".to_owned());
    }
    apply_live_latency_gate(
        &mut rejection_reasons,
        peak_live_p95_commit_latency_us,
        gates.maximum_live_p95_commit_latency_us,
    );
    Ok(BenchmarkSweepEvaluation {
        id: id.to_owned(),
        candidate_identity: candidate_identity.to_owned(),
        report: report_path.display().to_string(),
        report_blake3: blake3::hash(&bytes).to_string(),
        source: if resumed { "resumed" } else { "executed" },
        correctness_passed,
        measured_runs: runs.len(),
        coefficient_of_variation,
        median_blocks_per_second_milli,
        peak_rss_bytes,
        peak_physical_store_bytes,
        peak_delivery_retained_bytes,
        peak_live_p95_commit_latency_us,
        eligible: rejection_reasons.is_empty(),
        rejection_reasons,
        pareto: false,
    })
}

fn peak_live_p95_commit_latency(runs: &[serde_json::Value]) -> Result<Option<u64>> {
    runs.iter()
        .filter_map(|run| {
            run.get("delivery")?
                .get("concurrentLive")?
                .as_object()
                .and_then(|live| live.get("p95CommitLatencyUs"))
        })
        .map(|value| {
            value
                .as_u64()
                .context("sweep candidate run has invalid live p95 commit latency")
        })
        .collect::<Result<Vec<_>>>()
        .map(|latencies| latencies.into_iter().max())
}

fn apply_live_latency_gate(
    rejection_reasons: &mut Vec<String>,
    observed: Option<u64>,
    maximum: Option<u64>,
) {
    if maximum.is_some() && observed.is_none() {
        rejection_reasons.push("live_p95_commit_latency_missing".to_owned());
    } else if maximum.is_some_and(|maximum| observed.is_some_and(|value| value > maximum)) {
        rejection_reasons.push("live_p95_commit_latency".to_owned());
    }
}

fn summary_u64(summary: &serde_json::Value, field: &'static str) -> Result<u64> {
    summary
        .get(field)
        .and_then(serde_json::Value::as_u64)
        .with_context(|| format!("sweep candidate summary has no valid {field}"))
}

fn summary_optional_u64(summary: &serde_json::Value, field: &'static str) -> Result<Option<u64>> {
    match summary.get(field) {
        Some(serde_json::Value::Null) => Ok(None),
        Some(value) => value
            .as_u64()
            .map(Some)
            .with_context(|| format!("sweep candidate summary has no valid {field}")),
        None => bail!("sweep candidate summary has no {field}"),
    }
}

fn summary_f64(summary: &serde_json::Value, field: &'static str) -> Result<f64> {
    summary
        .get(field)
        .and_then(serde_json::Value::as_f64)
        .filter(|value| value.is_finite())
        .with_context(|| format!("sweep candidate summary has no valid {field}"))
}

fn mark_pareto_frontier(candidates: &mut [BenchmarkSweepEvaluation]) {
    let frontier = candidates
        .iter()
        .enumerate()
        .filter(|(_, candidate)| candidate.eligible)
        .filter(|(index, candidate)| {
            !candidates.iter().enumerate().any(|(other_index, other)| {
                other_index != *index && other.eligible && sweep_dominates(other, candidate)
            })
        })
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    for index in frontier {
        candidates[index].pareto = true;
    }
}

fn sweep_dominates(candidate: &BenchmarkSweepEvaluation, other: &BenchmarkSweepEvaluation) -> bool {
    let no_worse = candidate.median_blocks_per_second_milli >= other.median_blocks_per_second_milli
        && candidate.peak_rss_bytes.expect("eligible candidate RSS")
            <= other.peak_rss_bytes.expect("eligible candidate RSS")
        && candidate
            .peak_physical_store_bytes
            .expect("eligible candidate physical store")
            <= other
                .peak_physical_store_bytes
                .expect("eligible candidate physical store")
        && candidate
            .peak_delivery_retained_bytes
            .expect("eligible candidate delivery bytes")
            <= other
                .peak_delivery_retained_bytes
                .expect("eligible candidate delivery bytes")
        && optional_latency_no_worse(
            candidate.peak_live_p95_commit_latency_us,
            other.peak_live_p95_commit_latency_us,
        );
    let strictly_better = candidate.median_blocks_per_second_milli
        > other.median_blocks_per_second_milli
        || candidate.peak_rss_bytes.expect("eligible candidate RSS")
            < other.peak_rss_bytes.expect("eligible candidate RSS")
        || candidate
            .peak_physical_store_bytes
            .expect("eligible candidate physical store")
            < other
                .peak_physical_store_bytes
                .expect("eligible candidate physical store")
        || candidate
            .peak_delivery_retained_bytes
            .expect("eligible candidate delivery bytes")
            < other
                .peak_delivery_retained_bytes
                .expect("eligible candidate delivery bytes")
        || optional_latency_better(
            candidate.peak_live_p95_commit_latency_us,
            other.peak_live_p95_commit_latency_us,
        );
    no_worse && strictly_better
}

const fn optional_latency_no_worse(candidate: Option<u64>, other: Option<u64>) -> bool {
    match (candidate, other) {
        (Some(candidate), Some(other)) => candidate <= other,
        (None, None) => true,
        _ => false,
    }
}

const fn optional_latency_better(candidate: Option<u64>, other: Option<u64>) -> bool {
    matches!((candidate, other), (Some(candidate), Some(other)) if candidate < other)
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn run(options: BenchmarkOptions) -> Result<Exit> {
    validate_options(&options)?;
    let kind = corpus_kind(options.corpus);
    let (_, manifest) =
        GeneratedHistorySource::new(kind, options.blocks, options.seed, options.chunk_blocks)?;
    let processor = benchmark_processor_identity(kind, options.profile)?;
    let mut measured = Vec::with_capacity(usize::try_from(options.runs).unwrap_or(0));
    let total = options.warmups.saturating_add(options.runs);
    for ordinal in 0..total {
        let report = Box::pin(run_once(&options, kind, ordinal.saturating_add(1))).await?;
        if !report.correctness_passed {
            bail!(
                "benchmark correctness check failed in {} iteration {}: expected {}, observed {}",
                report.mode,
                report.iteration,
                report.expected_digest,
                report.observed_digest
            );
        }
        if ordinal >= options.warmups {
            measured.push(report);
        }
    }
    let sample_report = options
        .samples_report
        .as_deref()
        .map(|path| write_sample_report(path, &measured))
        .transpose()?;
    let report = BenchmarkSuiteReport {
        report_version: REPORT_VERSION,
        report_schema: REPORT_SCHEMA,
        generated_at_unix_ms: now_milliseconds(),
        node_version: env!("CARGO_PKG_VERSION"),
        git_revision: git_output(&["rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".to_owned()),
        git_dirty: git_output(&["status", "--porcelain"]).is_some_and(|output| !output.is_empty()),
        build_profile: build_profile(),
        host: BenchmarkHost {
            os: std::env::consts::OS,
            architecture: std::env::consts::ARCH,
            logical_cpus: std::thread::available_parallelism().map_or(1, usize::from),
        },
        configuration: BenchmarkConfiguration {
            mode: mode_name(options.mode),
            profile: profile_name(options.profile),
            destination: destination_name(options.destination),
            postgres_schema: postgres_schema_name(options.postgres_schema),
            blocks: options.blocks,
            seed: options.seed,
            chunk_blocks: options.chunk_blocks,
            artifact_segment_blocks: options.artifact_segment_blocks,
            artifact_segment_compression: artifact_compression_name(
                options.artifact_segment_compression,
            ),
            artifact_compaction_interval_ms: options.artifact_compaction_interval_ms,
            artifact_compaction_maximum_segments_per_cycle: options
                .artifact_compaction_maximum_segments_per_cycle,
            warmups: options.warmups,
            measured_runs: options.runs,
            sample_interval_ms: options.sample_interval_ms,
            consumer_delay_ms: options.consumer_delay_ms,
            consumer_reconnect_every_batches: options.consumer_reconnect_every_batches,
            consumer_drop_ack_response_once: options.consumer_drop_ack_response_once,
            concurrent_live_blocks: options.concurrent_live_blocks,
            live_block_interval_ms: options.live_block_interval_ms,
            mapper_concurrency: options.mapper_concurrency,
            maximum_active_chunks: options.maximum_active_chunks,
            maximum_mapped_bytes: options.maximum_mapped_bytes,
            history_material_memory_bytes: HistoricalMaterialCoordinatorConfig::default()
                .memory_bytes,
            maximum_buffered_frames_per_acquisition: HistoricalMaterialCoordinatorConfig::default()
                .maximum_buffered_frames_per_acquisition,
            minimum_physical_chunk_blocks: HistoricalMaterialCoordinatorConfig::default()
                .minimum_physical_chunk_blocks,
            maximum_overfetch_ratio: HistoricalMaterialCoordinatorConfig::default()
                .maximum_overfetch_ratio,
            commit_maximum_blocks: options.commit_maximum_blocks,
            commit_maximum_changes: options.commit_maximum_changes,
            commit_maximum_encoded_bytes: options.commit_maximum_encoded_bytes,
            commit_maximum_delay_ms: options.commit_maximum_delay_ms,
            commit_target_writer_hold_ms: options.commit_target_writer_hold_ms,
            delivery_target_encoded_bytes: options.delivery_target_encoded_bytes,
            delivery_maximum_encoded_bytes: options.delivery_maximum_encoded_bytes,
            delivery_maximum_events: options.delivery_maximum_events,
            delivery_maximum_processed_blocks: options.delivery_maximum_processed_blocks,
            delivery_maximum_delay_ms: options.delivery_maximum_delay_ms,
            delivery_maximum_buffered_batches: options.delivery_maximum_buffered_batches,
            delivery_maximum_buffered_bytes: options.delivery_maximum_buffered_bytes,
            delivery_compression: benchmark_compression_name(options.delivery_compression),
            delivery_read_limit: DELIVERY_READ_LIMIT,
            sqlite_schema_version: CURRENT_SCHEMA_VERSION,
            delivery_encoding_version: DELIVERY_ENCODING_VERSION,
            sqlite_durability: "full",
            sqlite_reader_connections: 4,
            sqlite_busy_timeout_ms: 5_000,
        },
        product_profile: product_profile_manifest(options.profile),
        processor,
        corpus: manifest,
        baseline: BaselineContract {
            commit_max_blocks: options.commit_maximum_blocks,
            commit_max_delay_ms: options.commit_maximum_delay_ms,
            delivery_aggregation: "independent_progress_unit_aggregation",
            compression: benchmark_baseline_compression(options.delivery_compression),
        },
        sample_report,
        summary: summarize(&measured),
        runs: measured,
    };
    let encoded = serde_json::to_vec_pretty(&report)?;
    if let Some(path) = options.report.as_deref() {
        write_immutable(path, &encoded)?;
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "reportVersion": report.report_version,
                "report": path.display().to_string(),
                "summary": &report.summary,
            }))?
        );
    } else {
        println!(
            "{}",
            String::from_utf8(encoded).context("benchmark JSON is UTF-8")?
        );
    }
    Ok(Exit::Success)
}

async fn run_once(
    options: &BenchmarkOptions,
    kind: SyntheticCorpusKind,
    iteration: u32,
) -> Result<BenchmarkRunReport> {
    match options.mode {
        BenchmarkMode::Acquire => run_acquire(options, kind, iteration).await,
        BenchmarkMode::Process => run_process(options, kind, iteration).await,
        BenchmarkMode::Deliver => run_deliver(options, kind, iteration).await,
        BenchmarkMode::EndToEnd => Box::pin(run_end_to_end(options, kind, iteration)).await,
    }
}

async fn run_acquire(
    options: &BenchmarkOptions,
    kind: SyntheticCorpusKind,
    iteration: u32,
) -> Result<BenchmarkRunReport> {
    if options.profile == BenchmarkProductProfile::RawOnly {
        return run_raw_acquire(options, kind, iteration).await;
    }
    let (source, manifest) =
        GeneratedHistorySource::new(kind, options.blocks, options.seed, options.chunk_blocks)?;
    let source = Arc::new(source);
    let processor = BlockLocalCounter::default().with_delivery_none();
    let job = BackfillJob::for_processor(
        format!("benchmark-acquire-{iteration}"),
        &processor,
        ChainId(1),
        manifest.range,
        VerificationPolicy::CompleteCryptographic,
    )?;
    let plan = source.plan(&job.request).await?;
    let started = source.start_measurement();
    let sampler = Sampler::start(
        options.sample_interval_ms,
        source.clone(),
        None,
        None,
        None,
        None,
        None,
    );
    let mut digest = blake3::Hasher::new();
    let mut expected_parent = None;
    let cancellation = CancellationToken::new();
    for chunk in &plan.chunks {
        let mut frames = source
            .open(chunk, source_budget(options.blocks), cancellation.clone())
            .await?;
        while let Some(frame) = frames.next().await {
            let frame = frame?;
            frame
                .validate_shape()
                .map_err(|error| anyhow::anyhow!(error))?;
            if let Some(parent) = expected_parent
                && frame.block.parent_hash != parent
            {
                bail!(
                    "synthetic acquisition parent mismatch at {}",
                    frame.block.number.0
                );
            }
            expected_parent = Some(frame.block.hash);
            update_frame_digest(&mut digest, &frame);
        }
    }
    let elapsed = elapsed_milliseconds(started);
    let resource_samples = sampler.finish().await;
    let observed_digest = digest.finalize().to_string();
    Ok(finalize_run(
        iteration,
        BenchmarkMode::Acquire,
        options.profile,
        options.blocks,
        manifest.expected_frames,
        manifest.expected_canonical_processor_output_bytes,
        manifest.expected_canonical_processor_output_bytes,
        elapsed,
        source.stats(),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        observed_digest,
        manifest.expected_frame_digest,
        resource_samples,
    ))
}

async fn run_raw_acquire(
    options: &BenchmarkOptions,
    kind: SyntheticCorpusKind,
    iteration: u32,
) -> Result<BenchmarkRunReport> {
    let (source, manifest) =
        GeneratedHistorySource::new(kind, options.blocks, options.seed, options.chunk_blocks)?;
    let source = Arc::new(source);
    let directory = tempfile::Builder::new()
        .prefix("leani-raw-acquire-benchmark-")
        .tempdir()?;
    let raw_store = open_benchmark_raw_history_store(directory.path().join("raw-history")).await?;
    let started = source.start_measurement();
    let sampler = Sampler::start(
        options.sample_interval_ms,
        source.clone(),
        None,
        Some(raw_store.clone()),
        None,
        None,
        None,
    );
    let required_capabilities = source.descriptor().complete_capabilities;
    let retained = retain_benchmark_history(
        &raw_store,
        source.clone(),
        required_capabilities,
        &manifest,
        options,
        iteration,
    )
    .await?;
    let (observed_digest, retained_read_milliseconds) =
        verify_retained_frame_digest(&retained.source, manifest.range, options.blocks, iteration)
            .await?;
    let elapsed = elapsed_milliseconds(started);
    let resource_samples = sampler.finish().await;
    let history_stats = raw_store.stats().await?;
    let retained_stats = retained.source.stats();
    if source.stats() != retained.external_after_acquisition {
        bail!("raw-only retained verification performed an external source read");
    }
    if retained_stats.record_reads != options.blocks {
        bail!(
            "raw-only verification read {} retained records for {} requested blocks",
            retained_stats.record_reads,
            options.blocks
        );
    }
    let mut report = finalize_run(
        iteration,
        BenchmarkMode::Acquire,
        options.profile,
        options.blocks,
        manifest.expected_frames,
        history_stats.retained_logical_bytes,
        history_stats.retained_logical_bytes,
        elapsed,
        source.stats(),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        observed_digest,
        manifest.expected_frame_digest,
        resource_samples,
    );
    attach_raw_history_measurement(
        &mut report,
        raw_history_measurement(
            "external_acquisition_then_retained_local_verification",
            "frame_digest_verification",
            &retained,
            retained_read_milliseconds,
            history_stats,
        ),
    );
    Ok(report)
}

async fn verify_retained_frame_digest(
    source: &RetainedHistorySource,
    range: BlockRange,
    blocks: u64,
    iteration: u32,
) -> Result<(String, u64)> {
    let processor = BlockLocalCounter::default().with_delivery_none();
    let job = BackfillJob::for_processor(
        format!("benchmark-raw-verify-{iteration}"),
        &processor,
        ChainId(1),
        range,
        VerificationPolicy::CompleteCryptographic,
    )?;
    let plan = source.plan(&job.request).await?;
    let started = Instant::now();
    let mut digest = blake3::Hasher::new();
    let mut expected_parent = None;
    for chunk in &plan.chunks {
        let mut frames = source
            .open(chunk, source_budget(blocks), CancellationToken::new())
            .await?;
        while let Some(frame) = frames.next().await {
            let frame = frame?;
            frame
                .validate_shape()
                .map_err(|error| anyhow::anyhow!(error))?;
            if let Some(parent) = expected_parent
                && frame.block.parent_hash != parent
            {
                bail!(
                    "retained raw acquisition parent mismatch at {}",
                    frame.block.number.0
                );
            }
            expected_parent = Some(frame.block.hash);
            update_frame_digest(&mut digest, &frame);
        }
    }
    Ok((digest.finalize().to_string(), elapsed_milliseconds(started)))
}

#[allow(clippy::too_many_lines)]
async fn run_process(
    options: &BenchmarkOptions,
    kind: SyntheticCorpusKind,
    iteration: u32,
) -> Result<BenchmarkRunReport> {
    let (source, manifest) =
        GeneratedHistorySource::new(kind, options.blocks, options.seed, options.chunk_blocks)?;
    let source = Arc::new(source);
    let processor = benchmark_processor(kind, options.profile)?;
    let directory = tempfile::Builder::new()
        .prefix("leani-process-benchmark-")
        .tempdir()?;
    let artifact_segment_root = directory.path().join("processor-artifacts");
    let mut store_config = StoreConfig::new(directory.path().join("benchmark.sqlite"));
    if options.profile == BenchmarkProductProfile::CompactArtifactTiered {
        store_config = store_config.with_artifact_segments(ArtifactSegmentStorageConfig {
            root: artifact_segment_root.clone(),
            compression: artifact_compression(options.artifact_segment_compression),
            target_blocks: options.artifact_segment_blocks,
            maximum_artifact_logical_bytes: 64 * 1024 * 1024,
            maximum_segment_logical_bytes: 1024 * 1024 * 1024,
            maximum_segment_physical_bytes: 1024 * 1024 * 1024,
            maximum_retained_physical_bytes: 2 * 1024 * 1024 * 1024,
        });
    }
    let store = SqliteStore::open(store_config).await?;
    let artifact_segment_sink =
        if options.profile == BenchmarkProductProfile::CompactArtifactSegment {
            Some(Arc::new(ArtifactSegmentSink::open(
                artifact_segment_root,
                ArtifactSegmentSinkConfig {
                    compression: artifact_compression(options.artifact_segment_compression),
                    limits: ArtifactSegmentLimits {
                        maximum_artifact_logical_bytes: 64 * 1024 * 1024,
                        maximum_segment_logical_bytes: 1024 * 1024 * 1024,
                        maximum_segment_physical_bytes: 1024 * 1024 * 1024,
                    },
                    maximum_retained_physical_bytes: 2 * 1024 * 1024 * 1024,
                },
            )?))
        } else {
            None
        };
    let raw_history_store = if profile_retains_raw(options.profile) {
        Some(open_benchmark_raw_history_store(directory.path().join("raw-history")).await?)
    } else {
        None
    };
    let (pipeline_budget, material_coordinator) = benchmark_pipeline(options)?;
    let started = source.start_measurement();
    let sampler = Sampler::start(
        options.sample_interval_ms,
        source.clone(),
        Some(store.clone()),
        raw_history_store.clone(),
        Some(processor.descriptor().clone()),
        Some(material_coordinator.clone()),
        artifact_segment_sink.as_deref().cloned(),
    );
    let raw_replay = if let Some(raw_store) = raw_history_store.as_ref() {
        Some(
            prepare_retained_replay(
                raw_store,
                source.clone(),
                processor.as_ref(),
                &manifest,
                options,
                iteration,
            )
            .await?,
        )
    } else {
        None
    };
    let runtime_source: Arc<dyn HistorySource> = raw_replay.as_ref().map_or_else(
        || source.clone() as Arc<dyn HistorySource>,
        |replay| Arc::new(replay.source.clone()) as Arc<dyn HistorySource>,
    );
    let mut runtime = HistoricalRuntime::new(
        store.clone(),
        runtime_source,
        processor.clone(),
        benchmark_runtime_config(options),
    )?
    .with_pipeline_budget(pipeline_budget)
    .with_material_coordinator(material_coordinator.clone());
    if let Some(sink) = artifact_segment_sink.as_ref() {
        runtime = runtime.with_artifact_sink(sink.clone())?;
    }
    let job = BackfillJob::for_processor(
        format!("benchmark-process-{iteration}"),
        processor.as_ref(),
        ChainId(1),
        manifest.range,
        VerificationPolicy::CompleteCryptographic,
    )?;
    let compactor =
        BenchmarkCoverageCompactor::start(store.clone(), processor.descriptor().clone());
    let artifact_compactor = if options.profile == BenchmarkProductProfile::CompactArtifactTiered {
        let cancellation = CancellationToken::new();
        let task = tokio::spawn(supervise_artifact_compaction(
            store.clone(),
            vec![processor.clone()],
            ArtifactStorageConfig {
                backend: ArtifactStorageBackend::TieredSegments,
                segment_target_blocks: options.artifact_segment_blocks,
                compression: artifact_compression(options.artifact_segment_compression),
                maximum_artifact_logical_bytes: HumanBytes::from_bytes(64 * 1024 * 1024),
                maximum_segment_logical_bytes: HumanBytes::from_bytes(1024 * 1024 * 1024),
                maximum_segment_physical_bytes: HumanBytes::from_bytes(1024 * 1024 * 1024),
                compaction_interval: HumanMilliseconds::from_milliseconds(
                    options.artifact_compaction_interval_ms,
                ),
                maximum_segments_per_cycle: options.artifact_compaction_maximum_segments_per_cycle,
            },
            cancellation.clone(),
        ));
        Some((cancellation, task))
    } else {
        None
    };
    let processor_started = Instant::now();
    let runtime_result = runtime
        .run(job, source_budget(options.blocks), CancellationToken::new())
        .await;
    let retained_read_milliseconds = elapsed_milliseconds(processor_started);
    if let Some((cancellation, task)) = artifact_compactor {
        cancellation.cancel();
        task.await.context("join artifact compaction supervisor")?;
    }
    let runtime_report = runtime_result?;
    compactor.stop().await?;
    compact_benchmark_coverage(&store, processor.descriptor(), options.blocks).await?;
    if options.profile == BenchmarkProductProfile::CompactArtifactTiered {
        compact_benchmark_artifacts(
            &store,
            processor.descriptor(),
            manifest.range,
            options.artifact_segment_blocks,
        )
        .await?;
    }
    store.compact().await?;
    let elapsed = elapsed_milliseconds(started);
    let (query, expected_digest) = query_process_profile(
        options.profile,
        &store,
        artifact_segment_sink.as_deref(),
        processor.as_ref(),
        kind,
        &manifest,
    )
    .await?;
    if let Some(sink) = artifact_segment_sink.as_ref() {
        sink.verify(processor.descriptor()).await?;
    }
    store.verify().await?;
    let store_stats = store.stats().await?;
    let processor_store = store.processor_stats(processor.descriptor()).await?;
    let artifact_segment_store = if let Some(sink) = artifact_segment_sink.as_ref() {
        Some(sink.stats().await)
    } else {
        store.processor_artifact_segment_stats().await
    };
    if store_stats.changes != 0
        || processor_store.changes != 0
        || processor_store.outbox_records != 0
    {
        bail!("no-delivery materialization created delivery artifacts");
    }
    validate_process_profile(
        options.profile,
        options.blocks,
        &processor_store,
        artifact_segment_store.as_ref(),
    )?;
    let resource_samples = sampler.finish().await;
    let artifact_segment_candidate = if options.profile == BenchmarkProductProfile::CompactArtifact
    {
        Some(
            measure_artifact_segment_candidate(
                &store,
                processor.as_ref(),
                manifest.range,
                directory.path().join("artifact-segment-candidate"),
                options.artifact_segment_blocks,
                artifact_compression(options.artifact_segment_compression),
            )
            .await?,
        )
    } else {
        None
    };
    let observed_digest = query.digest.clone();
    let mut report = finalize_run(
        iteration,
        BenchmarkMode::Process,
        options.profile,
        options.blocks,
        manifest.expected_processor_events,
        manifest.expected_canonical_processor_output_bytes,
        manifest.expected_canonical_processor_output_bytes,
        elapsed,
        source.stats(),
        Some(runtime_report),
        None,
        None,
        Some(query),
        Some(store_stats),
        Some(processor_store),
        artifact_segment_store,
        artifact_segment_candidate,
        observed_digest,
        expected_digest,
        resource_samples,
    );
    if let Some(replay) = raw_replay {
        let history_stats = raw_history_store
            .as_ref()
            .context("raw-history store missing after retained replay")?
            .stats()
            .await?;
        let replay_stats = replay.source.stats();
        if replay.committed_blocks != options.blocks {
            bail!(
                "raw-history acquisition committed {} of {} requested blocks",
                replay.committed_blocks,
                options.blocks
            );
        }
        if replay_stats.record_reads != options.blocks {
            bail!(
                "retained-history replay read {} records for {} requested blocks",
                replay_stats.record_reads,
                options.blocks
            );
        }
        if replay.external_after_acquisition.frames != options.blocks {
            bail!(
                "raw-history acquisition read {} external frames for {} requested blocks",
                replay.external_after_acquisition.frames,
                options.blocks
            );
        }
        if report.source != replay.external_after_acquisition {
            bail!(
                "raw-artifact processor replay performed unexpected external reads: frame count changed from {} to {}",
                replay.external_after_acquisition.frames,
                report.source.frames,
            );
        }
        attach_raw_history_measurement(
            &mut report,
            raw_history_measurement(
                "external_acquisition_then_retained_local_processor_replay",
                "processor_replay",
                &replay,
                retained_read_milliseconds,
                history_stats,
            ),
        );
    }
    Ok(report)
}

async fn prepare_retained_replay(
    store: &HistoryStore,
    external_source: Arc<GeneratedHistorySource>,
    processor: &dyn Processor,
    manifest: &SyntheticCorpusManifest,
    options: &BenchmarkOptions,
    iteration: u32,
) -> Result<RawReplayContext> {
    let required_capabilities = processor
        .descriptor()
        .requirements
        .iter()
        .fold(CapabilitySet::NONE, |capabilities, requirement| {
            capabilities.union(requirement.capabilities)
        });
    retain_benchmark_history(
        store,
        external_source,
        required_capabilities,
        manifest,
        options,
        iteration,
    )
    .await
}

async fn retain_benchmark_history(
    store: &HistoryStore,
    external_source: Arc<GeneratedHistorySource>,
    required_capabilities: CapabilitySet,
    manifest: &SyntheticCorpusManifest,
    options: &BenchmarkOptions,
    iteration: u32,
) -> Result<RawReplayContext> {
    let material = RawHistoryMaterialProfile::default();
    let measured_source = external_source.clone();
    let external_source: Arc<dyn HistorySource> = external_source;
    let source_set = RawHistorySourceSet::new(vec![external_source])?;
    let runner = RawHistoryRunner::new(store.clone(), source_set, source_budget(options.blocks))?;
    let id = RawHistoryJobId::new(format!("benchmark-raw-{iteration}"))?;
    store
        .create_raw_history_job(
            id.clone(),
            RawHistoryJobSpec {
                chain_id: ChainId(1),
                ranges: vec![manifest.range],
                profile: RawHistoryProfile::ProcessorReuse,
                material: material.clone(),
                required_capabilities,
                verification: VerificationClass::Cryptographic,
                minimum_trust: TrustModel::ProtocolVerified,
                source_policy_digest: runner.source_policy_digest(),
                retention: RawHistoryRetention::Full,
                segment: RawHistorySegmentPolicy {
                    target_blocks: options.chunk_blocks,
                    maximum_logical_bytes: 1024 * 1024 * 1024,
                    maximum_physical_bytes: 1024 * 1024 * 1024,
                    compression: RawHistoryCompression::Snappy,
                    on_limit: StorageLimitAction::Fail,
                },
                indexes: RawHistoryIndexPolicy::default(),
            },
        )
        .await?;
    let acquisition_started = Instant::now();
    let outcome = runner.run(&id, CancellationToken::new()).await?;
    let acquisition_milliseconds = elapsed_milliseconds(acquisition_started);
    let external_after_acquisition = measured_source.stats();
    let RawHistoryRunOutcome::Complete(job) = outcome else {
        bail!("raw-history benchmark acquisition stopped before completion")
    };
    if !job.remaining_ranges.is_empty() {
        bail!("completed raw-history benchmark retained unfinished ranges");
    }
    let source = RetainedHistorySource::new(
        store.clone(),
        RetainedHistorySourceConfig::local(
            ChainId(1),
            material.shape_id(),
            required_capabilities,
            VerificationClass::Cryptographic,
            TrustModel::ProtocolVerified,
        )?,
    )?;
    Ok(RawReplayContext {
        source,
        committed_blocks: options.blocks,
        acquisition_milliseconds,
        external_after_acquisition,
    })
}

async fn open_benchmark_raw_history_store(root: PathBuf) -> Result<HistoryStore> {
    Ok(HistoryStore::open(
        HistoryStoreConfig::new(root).with_budget(RawHistoryStorageBudget {
            maximum_logical_bytes: 16 * 1024 * 1024 * 1024,
            maximum_physical_bytes: 16 * 1024 * 1024 * 1024,
            maximum_frame_logical_bytes: 64 * 1024 * 1024,
            maximum_segment_logical_bytes: 1024 * 1024 * 1024,
            maximum_segment_physical_bytes: 1024 * 1024 * 1024,
        }),
    )
    .await?)
}

async fn prepare_benchmark_raw_input(
    root: PathBuf,
    source: Arc<GeneratedHistorySource>,
    processor: &dyn Processor,
    manifest: &SyntheticCorpusManifest,
    options: &BenchmarkOptions,
    iteration: u32,
) -> Result<BenchmarkRawInput> {
    if !profile_retains_raw(options.profile) {
        return Ok(BenchmarkRawInput::default());
    }
    let store = open_benchmark_raw_history_store(root).await?;
    let measurement_started = source.start_measurement();
    let replay =
        prepare_retained_replay(&store, source, processor, manifest, options, iteration).await?;
    Ok(BenchmarkRawInput {
        store: Some(store),
        replay: Some(replay),
        measurement_started: Some(measurement_started),
    })
}

fn raw_history_measurement(
    execution: &'static str,
    retained_read_purpose: &'static str,
    replay: &RawReplayContext,
    retained_read_milliseconds: u64,
    history_stats: leani_store_history::HistoryStoreStats,
) -> RawHistoryMeasurement {
    let replay_stats = replay.source.stats();
    RawHistoryMeasurement {
        execution,
        profile: "processor_reuse",
        acquisition_milliseconds: replay.acquisition_milliseconds,
        retained_read_milliseconds,
        retained_read_purpose,
        external_frames_after_acquisition: replay.external_after_acquisition.frames,
        external_frames_after_replay: replay.external_after_acquisition.frames,
        external_estimated_bytes: replay.external_after_acquisition.estimated_bytes,
        committed_blocks: replay.committed_blocks,
        closed_segments: history_stats.closed_segments,
        owners: history_stats.owners,
        retained_logical_bytes: history_stats.retained_logical_bytes,
        retained_segment_physical_bytes: history_stats.retained_segment_physical_bytes,
        catalog_physical_bytes: history_stats.catalog_physical_bytes,
        total_physical_bytes: history_stats.total_physical_bytes,
        retained_segment_opens: replay_stats.segment_opens,
        retained_record_reads: replay_stats.record_reads,
        retained_stored_record_bytes: replay_stats.stored_record_bytes,
        retained_decompressed_bytes: replay_stats.decompressed_bytes,
    }
}

async fn attach_subscription_raw_history(
    report: &mut BenchmarkRunReport,
    fixture: &SubscriptionFixture,
    retained_read_milliseconds: u64,
    blocks: u64,
) -> Result<()> {
    let Some(replay) = fixture.raw_replay.as_ref() else {
        return Ok(());
    };
    let raw_store = fixture
        .raw_history_store
        .as_ref()
        .context("raw subscription replay has no raw-history store")?;
    let retained_stats = replay.source.stats();
    if replay.committed_blocks != blocks
        || retained_stats.record_reads != blocks
        || replay.external_after_acquisition.frames != blocks
    {
        bail!("raw-externalized acquisition/replay did not cover exactly {blocks} blocks");
    }
    if report.source != replay.external_after_acquisition {
        bail!("raw-externalized processor execution performed an external source read");
    }
    attach_raw_history_measurement(
        report,
        raw_history_measurement(
            "external_acquisition_then_retained_local_processor_delivery",
            "processor_delivery",
            replay,
            retained_read_milliseconds,
            raw_store.stats().await?,
        ),
    );
    Ok(())
}

async fn query_process_profile(
    profile: BenchmarkProductProfile,
    store: &SqliteStore,
    artifact_segment_sink: Option<&ArtifactSegmentSink>,
    processor: &dyn Processor,
    kind: SyntheticCorpusKind,
    manifest: &SyntheticCorpusManifest,
) -> Result<(QueryMeasurement, String)> {
    match profile {
        BenchmarkProductProfile::Materialized | BenchmarkProductProfile::RawMaterialized => Ok((
            query_processor_output(store, processor, kind, manifest.expected_processor_events)
                .await?,
            manifest.expected_query_output_digest.clone(),
        )),
        BenchmarkProductProfile::RawArtifactMaterialized => {
            let materialized =
                query_processor_output(store, processor, kind, manifest.expected_processor_events)
                    .await?;
            let artifacts = query_processor_artifacts(
                store,
                processor,
                kind,
                manifest.range,
                manifest.expected_processor_events,
            )
            .await?;
            if !artifacts.row_count.correctness_passed
                || artifacts.canonical_output_bytes
                    != manifest.expected_canonical_processor_output_bytes
                || artifacts.digest != manifest.expected_processor_output_digest
            {
                bail!("raw-artifact-materialized artifact view failed independent validation");
            }
            Ok((materialized, manifest.expected_query_output_digest.clone()))
        }
        BenchmarkProductProfile::CompactArtifact
        | BenchmarkProductProfile::CompactArtifactTiered
        | BenchmarkProductProfile::RawArtifact => Ok((
            query_processor_artifacts(
                store,
                processor,
                kind,
                manifest.range,
                manifest.expected_processor_events,
            )
            .await?,
            manifest.expected_processor_output_digest.clone(),
        )),
        BenchmarkProductProfile::CompactArtifactSegment => Ok((
            query_segment_processor_artifacts(
                artifact_segment_sink.context("artifact segment sink missing")?,
                processor,
                kind,
                manifest.range,
                manifest.expected_processor_events,
            )
            .await?,
            manifest.expected_processor_output_digest.clone(),
        )),
        BenchmarkProductProfile::AcquireDiscard
        | BenchmarkProductProfile::RawOnly
        | BenchmarkProductProfile::Externalized
        | BenchmarkProductProfile::RawExternalized => {
            bail!("process benchmark received a non-process product profile")
        }
    }
}

fn validate_process_profile(
    profile: BenchmarkProductProfile,
    blocks: u64,
    processor_store: &ProcessorStoreStats,
    artifact_segment_store: Option<&ArtifactSegmentSinkStats>,
) -> Result<()> {
    match profile {
        BenchmarkProductProfile::Materialized | BenchmarkProductProfile::RawMaterialized
            if processor_store.processor_artifacts != 0 =>
        {
            bail!("materialized benchmark retained compact processor artifacts")
        }
        BenchmarkProductProfile::CompactArtifact | BenchmarkProductProfile::RawArtifact
            if processor_store.processor_artifacts != blocks
                || processor_store.entities != 0
                || processor_store.index_entries != 0 =>
        {
            bail!(
                "compact-artifact benchmark did not retain exactly one artifact per block without materialized output"
            )
        }
        BenchmarkProductProfile::RawArtifactMaterialized
            if processor_store.processor_artifacts != blocks || processor_store.entities == 0 =>
        {
            bail!(
                "raw-artifact-materialized benchmark did not retain both one artifact per block and materialized output"
            )
        }
        BenchmarkProductProfile::CompactArtifactSegment
            if processor_store.processor_artifacts != 0
                || processor_store.entities != 0
                || processor_store.index_entries != 0
                || artifact_segment_store.is_none_or(|stats| stats.artifacts != blocks) =>
        {
            bail!(
                "segment-artifact benchmark did not retain exactly one external artifact per block without SQLite artifact/output copies"
            )
        }
        BenchmarkProductProfile::CompactArtifactTiered
            if processor_store.processor_artifacts != blocks
                || processor_store.entities != 0
                || processor_store.index_entries != 0
                || artifact_segment_store.is_none_or(|stats| stats.artifacts != blocks) =>
        {
            bail!(
                "tiered-artifact benchmark did not retain one metadata row and one segmented payload per block without materialized output"
            )
        }
        BenchmarkProductProfile::Materialized
        | BenchmarkProductProfile::RawMaterialized
        | BenchmarkProductProfile::RawArtifactMaterialized
        | BenchmarkProductProfile::CompactArtifact
        | BenchmarkProductProfile::CompactArtifactSegment
        | BenchmarkProductProfile::CompactArtifactTiered
        | BenchmarkProductProfile::RawArtifact => Ok(()),
        BenchmarkProductProfile::AcquireDiscard
        | BenchmarkProductProfile::RawOnly
        | BenchmarkProductProfile::Externalized
        | BenchmarkProductProfile::RawExternalized => {
            unreachable!("process profile was validated before execution")
        }
    }
}

#[allow(clippy::too_many_lines)]
async fn run_deliver(
    options: &BenchmarkOptions,
    kind: SyntheticCorpusKind,
    iteration: u32,
) -> Result<BenchmarkRunReport> {
    let fixture = subscription_fixture(options, kind, iteration).await?;
    let retained_read_started = Instant::now();
    let prefill_runtime = fixture
        .runtime
        .run(
            fixture.job.clone(),
            fixture.source_budget,
            CancellationToken::new(),
        )
        .await?;
    let retained_read_milliseconds = elapsed_milliseconds(retained_read_started);
    let (_, manifest) =
        GeneratedHistorySource::new(kind, options.blocks, options.seed, options.chunk_blocks)?;
    let server = match options.destination {
        BenchmarkDestination::RustDirect => None,
        BenchmarkDestination::RustHttp | BenchmarkDestination::SdkPostgres => {
            Some(BenchmarkApiServer::start(&fixture, options).await?)
        }
    };
    let subscription_id = fixture.job.id.clone();
    let pruner = BenchmarkDeliveryPruner::start(
        fixture.store.clone(),
        fixture.processor.descriptor().clone(),
        fixture.stream_id.clone(),
    );
    let sampler = Sampler::start(
        options.sample_interval_ms,
        fixture.source.clone(),
        Some(fixture.store.clone()),
        fixture.raw_history_store.clone(),
        Some(fixture.processor.descriptor().clone()),
        Some(fixture.material_coordinator.clone()),
        None,
    );
    let started_unix_ms = now_milliseconds();
    let started = Instant::now();
    let mut delivery = match options.destination {
        BenchmarkDestination::RustDirect => {
            consume_subscription(
                &fixture.store,
                fixture.processor.descriptor(),
                &fixture.stream_id,
                fixture.consumer_id,
                options.consumer_delay_ms,
                started,
            )
            .await?
        }
        BenchmarkDestination::RustHttp => {
            let server = server.as_ref().context("benchmark HTTP server missing")?;
            consume_subscription_http(
                &fixture.store,
                &server.base_url,
                &subscription_id,
                fixture.consumer_id,
                options.consumer_delay_ms,
                options.delivery_compression,
                started,
            )
            .await?
        }
        BenchmarkDestination::SdkPostgres => {
            let server = server.as_ref().context("benchmark HTTP server missing")?;
            consume_subscription_sdk_postgres(
                &fixture.store,
                fixture.processor.descriptor(),
                &server.base_url,
                &subscription_id,
                &fixture.stream_id,
                fixture.consumer_id,
                options.consumer_delay_ms,
                options.consumer_reconnect_every_batches,
                options.consumer_drop_ack_response_once,
                options.concurrent_live_blocks,
                0,
                options.postgres_schema,
                expected_postgres_rows(options.postgres_schema, &manifest),
                iteration,
                started_unix_ms,
            )
            .await?
        }
    };
    let consumer_complete_milliseconds = elapsed_milliseconds(started);
    finalize_delivery_throughput(
        &mut delivery,
        options.blocks,
        consumer_complete_milliseconds,
        None,
    );
    delivery.pruned_records = delivery.pruned_records.saturating_add(pruner.stop().await?);
    compact_benchmark_coverage(
        &fixture.store,
        fixture.processor.descriptor(),
        options.blocks,
    )
    .await?;
    let elapsed = elapsed_milliseconds(started);
    if let Some(server) = server {
        server.stop().await?;
    }
    let resource_samples = sampler.finish().await;
    fixture.store.compact().await?;
    fixture.store.verify().await?;
    let store_stats = fixture.store.stats().await?;
    let processor_store = fixture
        .store
        .processor_stats(fixture.processor.descriptor())
        .await?;
    validate_externalized_store_pruned(&delivery, &store_stats, &processor_store)?;
    let observed_digest = delivery.destination_digest.clone();
    let mut report = finalize_run(
        iteration,
        BenchmarkMode::Deliver,
        options.profile,
        options.blocks,
        manifest.expected_processor_events,
        manifest.expected_canonical_processor_output_bytes,
        manifest.expected_canonical_processor_output_bytes,
        elapsed,
        fixture.source.stats(),
        None,
        Some(prefill_runtime),
        Some(delivery),
        None,
        Some(store_stats),
        Some(processor_store),
        None,
        None,
        observed_digest,
        expected_delivery_digest(&manifest, options.destination),
        resource_samples,
    );
    attach_subscription_raw_history(
        &mut report,
        &fixture,
        retained_read_milliseconds,
        options.blocks,
    )
    .await?;
    Ok(report)
}

#[allow(clippy::too_many_lines)]
async fn run_end_to_end(
    options: &BenchmarkOptions,
    kind: SyntheticCorpusKind,
    iteration: u32,
) -> Result<BenchmarkRunReport> {
    let fixture = subscription_fixture(options, kind, iteration).await?;
    let (_, manifest) =
        GeneratedHistorySource::new(kind, options.blocks, options.seed, options.chunk_blocks)?;
    let live_manifest = concurrent_live_manifest(options, kind)?;
    let server = match options.destination {
        BenchmarkDestination::RustDirect => None,
        BenchmarkDestination::RustHttp | BenchmarkDestination::SdkPostgres => {
            Some(BenchmarkApiServer::start(&fixture, options).await?)
        }
    };
    let subscription_id = fixture.job.id.clone();
    let pruner = BenchmarkDeliveryPruner::start(
        fixture.store.clone(),
        fixture.processor.descriptor().clone(),
        fixture.stream_id.clone(),
    );
    let live_pruner = (options.concurrent_live_blocks > 0).then(|| {
        BenchmarkDeliveryPruner::start(
            fixture.store.clone(),
            fixture.processor.descriptor().clone(),
            default_delivery_stream_id(fixture.processor.descriptor()),
        )
    });
    let started_unix_ms = now_milliseconds();
    let started = fixture
        .measurement_started
        .unwrap_or_else(|| fixture.source.start_measurement());
    let sampler = Sampler::start(
        options.sample_interval_ms,
        fixture.source.clone(),
        Some(fixture.store.clone()),
        fixture.raw_history_store.clone(),
        Some(fixture.processor.descriptor().clone()),
        Some(fixture.material_coordinator.clone()),
        None,
    );
    let producer = async {
        let retained_read_started = Instant::now();
        let report = fixture
            .runtime
            .run(
                fixture.job.clone(),
                fixture.source_budget,
                CancellationToken::new(),
            )
            .await?;
        Ok::<_, anyhow::Error>((
            report,
            elapsed_milliseconds(retained_read_started),
            elapsed_milliseconds(started),
        ))
    };
    let live_producer = async {
        if options.concurrent_live_blocks == 0 {
            Ok(None)
        } else {
            produce_concurrent_live(&fixture, options, started)
                .await
                .map(Some)
        }
    };
    let consumer = async {
        let delivery = match options.destination {
            BenchmarkDestination::RustDirect => {
                consume_subscription(
                    &fixture.store,
                    fixture.processor.descriptor(),
                    &fixture.stream_id,
                    fixture.consumer_id,
                    options.consumer_delay_ms,
                    started,
                )
                .await
            }
            BenchmarkDestination::RustHttp => {
                let server = server.as_ref().context("benchmark HTTP server missing")?;
                consume_subscription_http(
                    &fixture.store,
                    &server.base_url,
                    &subscription_id,
                    fixture.consumer_id,
                    options.consumer_delay_ms,
                    options.delivery_compression,
                    started,
                )
                .await
            }
            BenchmarkDestination::SdkPostgres => {
                let server = server.as_ref().context("benchmark HTTP server missing")?;
                consume_subscription_sdk_postgres(
                    &fixture.store,
                    fixture.processor.descriptor(),
                    &server.base_url,
                    &subscription_id,
                    &fixture.stream_id,
                    fixture.consumer_id,
                    options.consumer_delay_ms,
                    options.consumer_reconnect_every_batches,
                    options.consumer_drop_ack_response_once,
                    options.concurrent_live_blocks,
                    live_manifest
                        .as_ref()
                        .map_or(0, |manifest| manifest.expected_processor_events),
                    options.postgres_schema,
                    expected_postgres_rows(options.postgres_schema, &manifest),
                    iteration,
                    started_unix_ms,
                )
                .await
            }
        }?;
        Ok::<_, anyhow::Error>((delivery, elapsed_milliseconds(started)))
    };
    let (
        (runtime_report, retained_read_milliseconds, producer_complete_milliseconds),
        live_producer,
        (mut delivery, consumer_complete_milliseconds),
    ) = tokio::try_join!(producer, live_producer, consumer)?;
    if let (Some(producer), Some(live_manifest)) = (live_producer, live_manifest.as_ref()) {
        attach_concurrent_live_measurement(
            &mut delivery,
            producer,
            live_manifest,
            options.concurrent_live_blocks,
        )?;
    }
    finalize_delivery_throughput(
        &mut delivery,
        options.blocks,
        consumer_complete_milliseconds,
        Some(producer_complete_milliseconds),
    );
    delivery.pruned_records = delivery.pruned_records.saturating_add(pruner.stop().await?);
    if let Some(live_pruner) = live_pruner {
        delivery.pruned_records = delivery
            .pruned_records
            .saturating_add(live_pruner.stop().await?);
    }
    compact_benchmark_coverage(
        &fixture.store,
        fixture.processor.descriptor(),
        options.blocks,
    )
    .await?;
    let elapsed = elapsed_milliseconds(started);
    if let Some(server) = server {
        server.stop().await?;
    }
    let resource_samples = sampler.finish().await;
    fixture.store.compact().await?;
    fixture.store.verify().await?;
    let store_stats = fixture.store.stats().await?;
    let processor_store = fixture
        .store
        .processor_stats(fixture.processor.descriptor())
        .await?;
    validate_externalized_store_pruned(&delivery, &store_stats, &processor_store)?;
    let observed_digest = delivery.destination_digest.clone();
    let retained_canonical_output_bytes = manifest
        .expected_canonical_processor_output_bytes
        .saturating_add(live_manifest.as_ref().map_or(0, |manifest| {
            manifest.expected_canonical_processor_output_bytes
        }));
    let mut report = finalize_run(
        iteration,
        BenchmarkMode::EndToEnd,
        options.profile,
        options.blocks,
        manifest.expected_processor_events,
        manifest.expected_canonical_processor_output_bytes,
        retained_canonical_output_bytes,
        elapsed,
        fixture.source.stats(),
        Some(runtime_report),
        None,
        Some(delivery),
        None,
        Some(store_stats),
        Some(processor_store),
        None,
        None,
        observed_digest,
        expected_delivery_digest(&manifest, options.destination),
        resource_samples,
    );
    attach_subscription_raw_history(
        &mut report,
        &fixture,
        retained_read_milliseconds,
        options.blocks,
    )
    .await?;
    Ok(report)
}

fn finalize_delivery_throughput(
    delivery: &mut DeliveryMeasurement,
    blocks: u64,
    consumer_complete_milliseconds: u64,
    producer_complete_milliseconds: Option<u64>,
) {
    delivery.producer_complete_milliseconds = producer_complete_milliseconds;
    delivery.consumer_complete_milliseconds = Some(consumer_complete_milliseconds);
    delivery.post_producer_drain_milliseconds = producer_complete_milliseconds
        .map(|producer| consumer_complete_milliseconds.saturating_sub(producer));
    delivery.producer_blocks_per_second_milli =
        producer_complete_milliseconds.map(|elapsed| throughput_per_second_milli(blocks, elapsed));
    delivery.consumer_blocks_per_second_milli = Some(throughput_per_second_milli(
        blocks,
        consumer_complete_milliseconds,
    ));
    delivery.domain_events_per_second_milli = Some(throughput_per_second_milli(
        delivery.domain_events,
        consumer_complete_milliseconds,
    ));
    delivery.raw_payload_bytes_per_second = Some(throughput_per_second(
        delivery.raw_payload_bytes,
        consumer_complete_milliseconds,
    ));
    delivery.transmitted_bytes_per_second = Some(throughput_per_second(
        delivery.transmitted_bytes,
        consumer_complete_milliseconds,
    ));
}

fn throughput_per_second_milli(units: u64, elapsed_milliseconds: u64) -> u64 {
    units.saturating_mul(1_000_000) / elapsed_milliseconds.max(1)
}

fn throughput_per_second(units: u64, elapsed_milliseconds: u64) -> u64 {
    units.saturating_mul(1_000) / elapsed_milliseconds.max(1)
}

fn validate_externalized_store_pruned(
    delivery: &DeliveryMeasurement,
    store: &StoreStats,
    processor: &ProcessorStoreStats,
) -> Result<()> {
    let maximum_anchor_records =
        3_u64.saturating_add(u64::from(delivery.concurrent_live.is_some()));
    if delivery.pruned_records == 0
        || processor.changes == 0
        || processor.changes > maximum_anchor_records
        || store.changes != processor.changes
        || store.delivery_retained_bytes != processor.change_bytes
        || processor.outbox_records != 0
    {
        bail!(
            "acknowledged externalized benchmark did not converge to bounded reset anchors: maximum_anchor_records={}, pruned={}, store_changes={}, store_bytes={}, processor_changes={}, processor_bytes={}, outbox={}",
            maximum_anchor_records,
            delivery.pruned_records,
            store.changes,
            store.delivery_retained_bytes,
            processor.changes,
            processor.change_bytes,
            processor.outbox_records,
        );
    }
    Ok(())
}

fn concurrent_live_manifest(
    options: &BenchmarkOptions,
    kind: SyntheticCorpusKind,
) -> Result<Option<SyntheticCorpusManifest>> {
    if options.concurrent_live_blocks == 0 {
        return Ok(None);
    }
    let start = options
        .blocks
        .checked_add(1)
        .context("concurrent live range start overflowed")?;
    let end = options
        .blocks
        .checked_add(options.concurrent_live_blocks)
        .context("concurrent live range end overflowed")?;
    let range = BlockRange::new(BlockNumber(start), BlockNumber(end))?;
    Ok(Some(synthetic_corpus_manifest(
        kind,
        options.seed,
        range,
        options.chunk_blocks,
    )))
}

async fn produce_concurrent_live(
    fixture: &SubscriptionFixture,
    options: &BenchmarkOptions,
    benchmark_started: Instant,
) -> Result<LiveProducerMeasurement> {
    let mut commit_latencies_us =
        Vec::with_capacity(usize::try_from(options.concurrent_live_blocks).unwrap_or(usize::MAX));
    let mut time_to_first_commit_ms = None;
    for offset in 0..options.concurrent_live_blocks {
        if offset > 0 && options.live_block_interval_ms > 0 {
            tokio::time::sleep(Duration::from_millis(options.live_block_interval_ms)).await;
        }
        let number = options
            .blocks
            .checked_add(offset)
            .and_then(|value| value.checked_add(1))
            .context("concurrent live block number overflowed")?;
        let frame = fixture.source.frame(BlockNumber(number));
        let delta = fixture.processor.map(&frame).await?;
        let commit_started = Instant::now();
        fixture
            .store
            .apply(
                fixture.processor.as_ref(),
                ProcessorCursor {
                    processor_id: fixture.processor.descriptor().id.to_string(),
                    processor_version: fixture.processor.descriptor().version.to_string(),
                    chain_id: frame.chain_id,
                    block_number: frame.block.number,
                    block_hash: frame.block.hash,
                    finality: Finality::Finalized,
                    sequence: number,
                },
                &delta,
                &[],
            )
            .await?;
        commit_latencies_us
            .push(u64::try_from(commit_started.elapsed().as_micros()).unwrap_or(u64::MAX));
        time_to_first_commit_ms.get_or_insert_with(|| elapsed_milliseconds(benchmark_started));
    }
    Ok(LiveProducerMeasurement {
        applied_blocks: options.concurrent_live_blocks,
        time_to_first_commit_ms,
        commit_latencies_us,
    })
}

fn attach_concurrent_live_measurement(
    delivery: &mut DeliveryMeasurement,
    producer: LiveProducerMeasurement,
    manifest: &SyntheticCorpusManifest,
    requested_blocks: u64,
) -> Result<()> {
    let sdk = delivery
        .live_destination
        .take()
        .context("SDK destination omitted concurrent live measurements")?;
    let mut commit_latencies_us = producer.commit_latencies_us;
    commit_latencies_us.sort_unstable();
    let correctness_passed = producer.applied_blocks == requested_blocks
        && sdk.delivered_domain_events == manifest.expected_processor_events
        && sdk.canonical_output_bytes == manifest.expected_canonical_processor_output_bytes
        && (manifest.expected_processor_events == 0 || sdk.delivered_blocks > 0)
        && sdk.delivered_blocks <= requested_blocks
        && sdk.destination_digest == manifest.expected_processor_output_sha256
        && (manifest.expected_processor_events == 0
            || (sdk.acknowledgements > 0 && sdk.acknowledged_sequence > 0));
    delivery.concurrent_live = Some(ConcurrentLiveMeasurement {
        requested_blocks,
        applied_blocks: producer.applied_blocks,
        expected_domain_events: manifest.expected_processor_events,
        delivered_blocks: sdk.delivered_blocks,
        delivered_domain_events: sdk.delivered_domain_events,
        expected_canonical_output_bytes: manifest.expected_canonical_processor_output_bytes,
        canonical_output_bytes: sdk.canonical_output_bytes,
        batches: sdk.batches,
        acknowledgements: sdk.acknowledgements,
        acknowledged_sequence: sdk.acknowledged_sequence,
        expected_destination_digest: manifest.expected_processor_output_sha256.clone(),
        destination_digest: sdk.destination_digest,
        destination_digest_algorithm: "sha256",
        time_to_first_commit_ms: producer.time_to_first_commit_ms,
        time_to_first_destination_batch_ms: sdk.time_to_first_destination_batch_ms,
        median_commit_latency_us: percentile(&commit_latencies_us, 50),
        p95_commit_latency_us: percentile(&commit_latencies_us, 95),
        p99_commit_latency_us: percentile(&commit_latencies_us, 99),
        maximum_commit_latency_us: commit_latencies_us.last().copied().unwrap_or(0),
        correctness_passed,
    });
    Ok(())
}

async fn compact_benchmark_coverage(
    store: &SqliteStore,
    descriptor: &ProcessorDescriptor,
    blocks: u64,
) -> Result<()> {
    compact_coverage_through(store, descriptor, BlockNumber(blocks)).await
}

async fn compact_coverage_through(
    store: &SqliteStore,
    descriptor: &ProcessorDescriptor,
    through: BlockNumber,
) -> Result<()> {
    loop {
        let outcome = store
            .compact_finalized_coverage(descriptor, through, 8_192, 10_000)
            .await?;
        if outcome.exact_coverage_deleted == 0 {
            break;
        }
    }
    store.reclaim_free_pages(16_384).await?;
    Ok(())
}

async fn compact_benchmark_artifacts(
    store: &SqliteStore,
    descriptor: &ProcessorDescriptor,
    range: BlockRange,
    target_blocks: u64,
) -> Result<()> {
    let mut compacted = 0_u64;
    while compacted < range.len() {
        let outcome = store
            .compact_processor_artifacts_to_segments(descriptor, range, 1_000, true)
            .await?;
        if outcome.artifacts == 0 {
            break;
        }
        compacted = compacted.saturating_add(outcome.artifacts);
    }
    let retained = store
        .processor_artifact_segment_stats()
        .await
        .map_or(0, |stats| stats.artifacts);
    if retained != range.len() {
        bail!(
            "tiered artifact benchmark retained {retained}/{} segmented blocks after compacting {compacted} tail blocks at target span {target_blocks}",
            range.len()
        );
    }
    Ok(())
}

struct BenchmarkDeliveryPruner {
    cancellation: CancellationToken,
    task: tokio::task::JoinHandle<Result<u64>>,
    store: SqliteStore,
    descriptor: ProcessorDescriptor,
    stream_id: String,
}

struct BenchmarkCoverageCompactor {
    cancellation: CancellationToken,
    task: tokio::task::JoinHandle<Result<u64>>,
}

impl BenchmarkCoverageCompactor {
    fn start(store: SqliteStore, descriptor: ProcessorDescriptor) -> Self {
        let cancellation = CancellationToken::new();
        let shutdown = cancellation.clone();
        let task = tokio::spawn(async move {
            let mut deleted = 0_u64;
            loop {
                let mut compacted = false;
                if let Some(finalized) = store.finalized_through(&descriptor).await? {
                    let compactable = store
                        .compactable_finalized_coverage_blocks(&descriptor, finalized)
                        .await?;
                    if compactable >= 8_192 {
                        let outcome = store
                            .compact_finalized_coverage(&descriptor, finalized, 8_192, 10_000)
                            .await?;
                        if outcome.exact_coverage_deleted > 0 {
                            deleted = deleted.saturating_add(outcome.exact_coverage_deleted);
                            store.reclaim_free_pages(256).await?;
                            compacted = true;
                        }
                    }
                }
                if compacted {
                    tokio::task::yield_now().await;
                    continue;
                }
                if shutdown.is_cancelled() {
                    break;
                }
                tokio::select! {
                    () = shutdown.cancelled() => {}
                    () = tokio::time::sleep(Duration::from_millis(10)) => {}
                }
            }
            Ok(deleted)
        });
        Self { cancellation, task }
    }

    async fn stop(self) -> Result<u64> {
        self.cancellation.cancel();
        self.task
            .await
            .context("benchmark coverage compactor task panicked")?
    }
}

impl BenchmarkDeliveryPruner {
    fn start(store: SqliteStore, descriptor: ProcessorDescriptor, stream_id: String) -> Self {
        let final_store = store.clone();
        let final_descriptor = descriptor.clone();
        let final_stream_id = stream_id.clone();
        let cancellation = CancellationToken::new();
        let shutdown = cancellation.clone();
        let task = tokio::spawn(async move {
            let mut deleted = 0_u64;
            let mut last_coverage_check = Instant::now()
                .checked_sub(Duration::from_millis(100))
                .unwrap_or_else(Instant::now);
            loop {
                if let Some(finalized) = store.finalized_through(&descriptor).await? {
                    let outcome = store
                        .prune_delivery_changes_in_stream(&descriptor, &stream_id, finalized)
                        .await?;
                    deleted = deleted.saturating_add(outcome.deleted);
                    if descriptor.mode == ReductionMode::BlockLocal
                        && descriptor.lifecycle.output.mode == OutputPolicyMode::None
                        && last_coverage_check.elapsed() >= Duration::from_millis(100)
                    {
                        let stats = store.stats().await?;
                        if stats.exact_coverage_blocks >= 8_192 {
                            let compacted = store
                                .compact_finalized_coverage(&descriptor, finalized, 8_192, 10_000)
                                .await?;
                            if compacted.exact_coverage_deleted > 0 {
                                store.reclaim_free_pages(256).await?;
                            }
                        }
                        last_coverage_check = Instant::now();
                    }
                    if outcome.deleted > 0 {
                        store.reclaim_free_pages(256).await?;
                        tokio::task::yield_now().await;
                        continue;
                    }
                }
                if shutdown.is_cancelled() {
                    break;
                }
                tokio::select! {
                    () = shutdown.cancelled() => {}
                    () = store.wait_for_delivery_capacity_change() => {}
                    () = tokio::time::sleep(Duration::from_millis(10)) => {}
                }
            }
            Ok(deleted)
        });
        Self {
            cancellation,
            task,
            store: final_store,
            descriptor: final_descriptor,
            stream_id: final_stream_id,
        }
    }

    async fn stop(self) -> Result<u64> {
        self.cancellation.cancel();
        let mut deleted = self
            .task
            .await
            .context("benchmark delivery pruner task panicked")??;
        if let Some(finalized) = self.store.finalized_through(&self.descriptor).await? {
            loop {
                let outcome = self
                    .store
                    .prune_delivery_changes_in_stream(&self.descriptor, &self.stream_id, finalized)
                    .await?;
                if outcome.deleted == 0 {
                    break;
                }
                deleted = deleted.saturating_add(outcome.deleted);
            }
            self.store.reclaim_free_pages(256).await?;
        }
        Ok(deleted)
    }
}

#[allow(clippy::too_many_arguments)]
fn finalize_run(
    iteration: u32,
    mode: BenchmarkMode,
    profile: BenchmarkProductProfile,
    blocks: u64,
    expected_events: u64,
    expected_canonical_output_bytes: u64,
    retained_canonical_output_bytes: u64,
    elapsed_milliseconds: u64,
    source: GeneratedSourceStats,
    runtime: Option<BackfillReport>,
    prefill_runtime: Option<BackfillReport>,
    delivery: Option<DeliveryMeasurement>,
    query: Option<QueryMeasurement>,
    store: Option<StoreStats>,
    processor_store: Option<ProcessorStoreStats>,
    artifact_segment_store: Option<ArtifactSegmentSinkStats>,
    artifact_segment_candidate: Option<ArtifactSegmentCandidateMeasurement>,
    observed_digest: String,
    expected_digest: String,
    samples: Vec<BenchmarkSample>,
) -> BenchmarkRunReport {
    let delivery_correct = delivery.as_ref().is_none_or(|delivery| {
        delivery.processed_blocks == blocks
            && delivery.domain_events == expected_events
            && delivery.raw_payload_bytes == expected_canonical_output_bytes
            && delivery.completion_records == 1
            && delivery.completion_sequence.is_some()
            && delivery.acknowledgements > 0
            && delivery
                .destination_row_count
                .as_ref()
                .is_none_or(|row_count| row_count.correctness_passed)
            && delivery
                .destination_through_block
                .is_none_or(|through| through == blocks)
            && delivery
                .concurrent_live
                .as_ref()
                .is_none_or(|live| live.correctness_passed)
    });
    let query_correct = query.as_ref().is_none_or(|query| {
        query.row_count.correctness_passed
            && query.canonical_output_bytes == expected_canonical_output_bytes
    });
    let correctness_passed = observed_digest == expected_digest
        && source.frames == blocks
        && delivery_correct
        && query_correct
        && artifact_segment_candidate
            .as_ref()
            .is_none_or(|candidate| candidate.correctness_passed);
    let time_to_first_source_frame_ms = (mode != BenchmarkMode::Deliver)
        .then_some(source.first_frame_elapsed_milliseconds)
        .flatten();
    let sampled_time_to_first_commit_ms = samples
        .iter()
        .find(|sample| sample.committed_through.is_some())
        .map(|sample| sample.elapsed_milliseconds);
    let peaks = benchmark_sample_peaks(&samples);
    let storage = benchmark_storage_measurement(
        profile,
        retained_canonical_output_bytes,
        store.as_ref(),
        processor_store.as_ref(),
        artifact_segment_store.as_ref(),
        delivery.as_ref(),
    );
    BenchmarkRunReport {
        iteration,
        mode: mode_name(mode),
        profile: profile_name(profile),
        elapsed_milliseconds,
        blocks_per_second_milli: throughput_per_second_milli(blocks, elapsed_milliseconds),
        time_to_first_source_frame_ms,
        sampled_time_to_first_commit_ms,
        source,
        runtime,
        prefill_runtime,
        delivery,
        query,
        store,
        processor_store,
        artifact_segment_store,
        artifact_segment_candidate,
        raw_history: None,
        storage,
        correctness_passed,
        observed_digest,
        expected_digest,
        peak_rss_bytes: peaks.rss_bytes,
        peak_physical_store_bytes: peaks.physical_store_bytes,
        peak_delivery_retained_bytes: peaks.delivery_retained_bytes,
        peak_history_delivery_retained_bytes: peaks.history_delivery_retained_bytes,
        peak_pending_delta_bytes: peaks.pending_delta_bytes,
        peak_processor_artifact_bytes: peaks.processor_artifact_bytes,
        peak_pending_processor_artifact_bytes: peaks.pending_processor_artifact_bytes,
        peak_active_material_acquisitions: peaks.active_material_acquisitions,
        peak_material_buffered_bytes: peaks.material_buffered_bytes,
        samples,
    }
}

fn benchmark_sample_peaks(samples: &[BenchmarkSample]) -> BenchmarkSamplePeaks {
    let maximum =
        |value: fn(&BenchmarkSample) -> Option<u64>| samples.iter().filter_map(value).max();
    BenchmarkSamplePeaks {
        rss_bytes: maximum(|sample| sample.rss_bytes),
        physical_store_bytes: maximum(|sample| sample.physical_store_bytes),
        delivery_retained_bytes: maximum(|sample| sample.delivery_retained_bytes),
        history_delivery_retained_bytes: maximum(|sample| sample.history_delivery_retained_bytes),
        pending_delta_bytes: maximum(|sample| sample.pending_delta_bytes),
        processor_artifact_bytes: maximum(|sample| sample.processor_artifact_bytes),
        pending_processor_artifact_bytes: maximum(|sample| sample.pending_processor_artifact_bytes),
        active_material_acquisitions: maximum(|sample| sample.active_material_acquisitions),
        material_buffered_bytes: maximum(|sample| sample.material_buffered_bytes),
    }
}

fn benchmark_storage_measurement(
    profile: BenchmarkProductProfile,
    canonical_logical_output_bytes: u64,
    store: Option<&StoreStats>,
    processor: Option<&ProcessorStoreStats>,
    artifact_segment_store: Option<&ArtifactSegmentSinkStats>,
    delivery: Option<&DeliveryMeasurement>,
) -> BenchmarkStorageMeasurement {
    let logical = benchmark_logical_storage(store, processor, artifact_segment_store, delivery);
    let physical = benchmark_physical_storage(store, artifact_segment_store, delivery);
    let storage_amplification_milli = storage_amplification_milli(
        physical.total_retained_bytes,
        canonical_logical_output_bytes,
    );
    BenchmarkStorageMeasurement {
        canonical_logical_output_definition: if profile == BenchmarkProductProfile::RawOnly {
            "sum_uncompressed_retained_raw_frame_bytes_v1"
        } else {
            "sum_canonical_processor_event_bytes_for_retained_run_outputs_v1"
        },
        canonical_logical_output_bytes,
        physical_measurement: "post_run_after_eligible_pruning_and_sqlite_compaction_v1",
        logical,
        physical,
        storage_amplification_milli,
    }
}

fn attach_raw_history_measurement(
    report: &mut BenchmarkRunReport,
    measurement: RawHistoryMeasurement,
) {
    report.storage.logical.raw_history = LogicalLayerMeasurement {
        rows: measurement.committed_blocks,
        bytes: Some(measurement.retained_logical_bytes),
    };
    report.storage.physical.raw_history_segment_bytes = measurement.retained_segment_physical_bytes;
    report.storage.physical.raw_history_catalog_bytes = measurement.catalog_physical_bytes;
    report.storage.physical.total_retained_bytes = report
        .storage
        .physical
        .total_retained_bytes
        .saturating_add(measurement.total_physical_bytes);
    report.storage.storage_amplification_milli = storage_amplification_milli(
        report.storage.physical.total_retained_bytes,
        report.storage.canonical_logical_output_bytes,
    );
    report.raw_history = Some(measurement);
}

fn storage_amplification_milli(
    physical_bytes: u64,
    canonical_logical_output_bytes: u64,
) -> Option<u64> {
    (canonical_logical_output_bytes > 0).then(|| {
        let scaled = u128::from(physical_bytes).saturating_mul(1_000);
        u64::try_from(scaled / u128::from(canonical_logical_output_bytes)).unwrap_or(u64::MAX)
    })
}

fn benchmark_logical_storage(
    store: Option<&StoreStats>,
    processor: Option<&ProcessorStoreStats>,
    artifact_segment_store: Option<&ArtifactSegmentSinkStats>,
    delivery: Option<&DeliveryMeasurement>,
) -> LogicalStorageMeasurement {
    let measured = |rows, bytes| LogicalLayerMeasurement {
        rows,
        bytes: Some(bytes),
    };
    LogicalStorageMeasurement {
        raw_history: measured(0, 0),
        processor_artifacts: artifact_segment_store.map_or_else(
            || {
                processor.map_or_else(
                    || measured(0, 0),
                    |stats| measured(stats.processor_artifacts, stats.processor_artifact_bytes),
                )
            },
            |stats| measured(stats.artifacts, stats.logical_bytes),
        ),
        materialized_entities: processor.map_or_else(
            || measured(0, 0),
            |stats| measured(stats.entities, stats.entity_bytes),
        ),
        materialized_indexes: processor.map_or_else(
            || measured(0, 0),
            |stats| measured(stats.index_entries, stats.index_bytes),
        ),
        processor_state: processor.map_or_else(
            || measured(0, 0),
            |stats| measured(stats.state_entries, stats.state_bytes),
        ),
        delivery: processor.map_or_else(
            || measured(0, 0),
            |stats| measured(stats.changes, stats.change_bytes),
        ),
        undo: processor.map_or_else(
            || measured(0, 0),
            |stats| measured(stats.undo_records, stats.undo_bytes),
        ),
        checkpoints: processor.map_or_else(
            || measured(0, 0),
            |stats| {
                measured(
                    stats
                        .recovery_checkpoints
                        .saturating_add(stats.portable_savepoints),
                    stats
                        .recovery_checkpoint_bytes
                        .saturating_add(stats.portable_savepoint_bytes),
                )
            },
        ),
        recent_reorg: measured(0, 0),
        correctness_metadata: LogicalLayerMeasurement {
            rows: store.map_or(0, |stats| {
                stats
                    .exact_coverage_blocks
                    .saturating_add(stats.coverage_intervals)
                    .saturating_add(stats.coverage_segments)
                    .saturating_add(stats.coverage_owners)
                    .saturating_add(stats.applied_blocks)
            }),
            // These compact control rows do not yet expose a stable logical
            // byte counter. Their physical pages remain fully charged below.
            bytes: None,
        },
        destination: delivery
            .and_then(|measurement| {
                measurement.destination_row_count.as_ref().map(|rows| {
                    let live_rows = measurement
                        .concurrent_live
                        .as_ref()
                        .map_or(0, |live| live.delivered_domain_events);
                    let live_bytes = measurement
                        .concurrent_live
                        .as_ref()
                        .map_or(0, |live| live.canonical_output_bytes);
                    measured(
                        rows.observed_rows.saturating_add(live_rows),
                        measurement.raw_payload_bytes.saturating_add(live_bytes),
                    )
                })
            })
            .unwrap_or_else(|| measured(0, 0)),
    }
}

fn benchmark_physical_storage(
    store: Option<&StoreStats>,
    artifact_segment_store: Option<&ArtifactSegmentSinkStats>,
    delivery: Option<&DeliveryMeasurement>,
) -> PhysicalStorageMeasurement {
    let node_sqlite_database_bytes = store.map_or(0, |stats| stats.physical_file_bytes);
    let node_sqlite_wal_bytes = store.map_or(0, |stats| stats.wal_bytes);
    let node_non_sqlite_segment_bytes =
        artifact_segment_store.map_or(0, |stats| stats.physical_bytes);
    let destination_total_bytes = delivery
        .and_then(|measurement| measurement.destination_total_bytes)
        .unwrap_or(0);
    let total_retained_bytes = node_sqlite_database_bytes
        .saturating_add(node_sqlite_wal_bytes)
        .saturating_add(node_non_sqlite_segment_bytes)
        .saturating_add(destination_total_bytes);
    PhysicalStorageMeasurement {
        node_sqlite_database_bytes,
        node_sqlite_wal_bytes,
        node_sqlite_freelist_bytes: store.map_or(0, |stats| stats.freelist_bytes),
        node_non_sqlite_segment_bytes,
        raw_history_segment_bytes: 0,
        raw_history_catalog_bytes: 0,
        destination_table_bytes: delivery
            .and_then(|measurement| measurement.destination_table_bytes)
            .unwrap_or(0),
        destination_index_bytes: delivery
            .and_then(|measurement| measurement.destination_index_bytes)
            .unwrap_or(0),
        destination_total_bytes,
        destination_wal_written_bytes: delivery
            .and_then(|measurement| measurement.destination_wal_written_bytes)
            .unwrap_or(0),
        total_retained_bytes,
    }
}

async fn subscription_fixture(
    options: &BenchmarkOptions,
    kind: SyntheticCorpusKind,
    iteration: u32,
) -> Result<SubscriptionFixture> {
    let (source, manifest) =
        GeneratedHistorySource::new(kind, options.blocks, options.seed, options.chunk_blocks)?;
    let source = Arc::new(source);
    let processor = benchmark_processor(kind, options.profile)?;
    let directory = tempfile::Builder::new()
        .prefix("leani-delivery-benchmark-")
        .tempdir()?;
    let store =
        SqliteStore::open(StoreConfig::new(directory.path().join("benchmark.sqlite"))).await?;
    let raw_input = prepare_benchmark_raw_input(
        directory.path().join("raw-history"),
        source.clone(),
        processor.as_ref(),
        &manifest,
        options,
        iteration,
    )
    .await?;
    store.register_processor(processor.descriptor()).await?;
    let consumer_id = "benchmark-destination";
    let (job, stream_id) = create_benchmark_subscription(
        &store,
        processor.as_ref(),
        &manifest,
        options,
        iteration,
        consumer_id,
    )
    .await?;
    let (pipeline_budget, material_coordinator) = benchmark_pipeline(options)?;
    let runtime_source: Arc<dyn HistorySource> = raw_input.replay.as_ref().map_or_else(
        || source.clone() as Arc<dyn HistorySource>,
        |replay| Arc::new(replay.source.clone()) as Arc<dyn HistorySource>,
    );
    let runtime = HistoricalRuntime::new(
        store.clone(),
        runtime_source,
        processor.clone(),
        benchmark_runtime_config(options),
    )?
    .with_pipeline_budget(pipeline_budget)
    .with_material_coordinator(material_coordinator.clone());
    Ok(SubscriptionFixture {
        store,
        source,
        raw_history_store: raw_input.store,
        raw_replay: raw_input.replay,
        measurement_started: raw_input.measurement_started,
        processor,
        runtime,
        material_coordinator,
        job,
        source_budget: source_budget(options.blocks),
        stream_id,
        consumer_id,
        _directory: directory,
    })
}

async fn create_benchmark_subscription(
    store: &SqliteStore,
    processor: &dyn Processor,
    manifest: &SyntheticCorpusManifest,
    options: &BenchmarkOptions,
    iteration: u32,
    consumer_id: &str,
) -> Result<(BackfillJob, String)> {
    let subscription_id = format!("benchmark-subscription-{iteration}");
    let stream_id = store
        .create_backfill_delivery_stream(processor.descriptor(), &subscription_id)
        .await?
        .stream_id;
    create_required_benchmark_consumer(store, processor.descriptor(), &stream_id, consumer_id)
        .await?;
    if options.concurrent_live_blocks > 0 {
        let live_stream_id = default_delivery_stream_id(processor.descriptor());
        create_required_benchmark_consumer(
            store,
            processor.descriptor(),
            &live_stream_id,
            consumer_id,
        )
        .await?;
    }
    let mut job = BackfillJob::for_processor(
        subscription_id.clone(),
        processor,
        ChainId(1),
        manifest.range,
        VerificationPolicy::CompleteCryptographic,
    )?;
    job.owner = HistoricalJobOwner::Subscription;
    job.delivery_stream_id = Some(stream_id.clone());
    let payload = serde_json::to_vec(&job)?;
    let record = JobRecord {
        id: job.id.clone(),
        kind: job.owner.job_kind().to_owned(),
        state: JobState::Queued,
        payload,
        checkpoint: None,
        attempts: 0,
        updated_at_unix_ms: now_milliseconds(),
    };
    store
        .create_backfill_subscription_job(
            &BackfillSubscriptionRecord {
                subscription_id: subscription_id.clone(),
                job_id: job.id.clone(),
                processor_instance: processor.descriptor().instance.to_string(),
                history_stream_id: stream_id.clone(),
                mode: BackfillSubscriptionMode::FillMissing,
                publication_revision: 0,
                state: BackfillSubscriptionState::Queued,
                consumer_id: consumer_id.to_owned(),
                ranges: vec![manifest.range],
                range: manifest.range,
                preexisting_coverage: Vec::new(),
                captured_finalized_target: manifest.range.end(),
                idempotency_key: subscription_id,
                // Isolated delivery pre-fills the complete range before it starts the
                // consumer, so the fixture budget must hold that range. The
                // end-to-end mode still consumes concurrently; controlled small
                // high-water marks belong to the later backpressure benchmark.
                effective_block_limit: options.blocks.max(1),
                effective_byte_limit: 1_024 * 1_024 * 1_024,
                resume_below_ratio_millionths: 750_000,
                delivery_batch_limits: store_delivery_batch_limits(options),
                initial_sequence: 0,
                completion_sequence: None,
                processed_work_blocks: 0,
            },
            &record,
            leani_primitives::BlockHash::new([3; 32]),
        )
        .await?;
    Ok((job, stream_id))
}

async fn create_required_benchmark_consumer(
    store: &SqliteStore,
    descriptor: &ProcessorDescriptor,
    stream_id: &str,
    consumer_id: &str,
) -> Result<()> {
    store
        .create_consumer_in_stream(
            descriptor,
            stream_id,
            consumer_id,
            ConsumerRole::Required,
            ConsumerStartPosition::EarliestRetained,
            Duration::from_mins(5),
        )
        .await?;
    Ok(())
}

impl BenchmarkApiServer {
    async fn start(fixture: &SubscriptionFixture, options: &BenchmarkOptions) -> Result<Self> {
        let processor: Arc<dyn Processor> = fixture.processor.clone();
        let control = BenchmarkBackfillControl {
            status: ApiBackfillStatus {
                id: fixture.job.id.clone(),
                owner: HistoricalWorkOwner::Subscription,
                processor: fixture.processor.descriptor().instance.to_string(),
                delivery_stream_id: Some(fixture.stream_id.clone()),
                publication_revision: Some("0".to_owned()),
                from_block: 1,
                to_block: options.blocks,
                ranges: vec![ApiBackfillRange {
                    from_block: 1,
                    to_block: options.blocks,
                }],
                requested_blocks: options.blocks,
                processed_blocks: 0,
                remaining_blocks: options.blocks,
                captured_finalized_target: Some(options.blocks),
                mode: BackfillExecutionMode::FillMissing,
                batching: Some(api_effective_batching(options)),
                state: ApiBackfillState::Running,
                attempts: 0,
                updated_at_unix_ms: now_milliseconds(),
                report: None,
                last_error: None,
            },
        };
        Self::start_with(
            fixture.store.clone(),
            processor,
            control,
            api_delivery_batch_limits(options),
        )
        .await
    }

    async fn start_with(
        store: SqliteStore,
        processor: Arc<dyn Processor>,
        control: BenchmarkBackfillControl,
        history_batch_limits: DeliveryBatchLimits,
    ) -> Result<Self> {
        let router = leani_api::router_with_processors(
            store,
            vec![processor],
            Vec::new(),
            ApiConfig {
                stream_batch_size: 10_000,
                stream_poll_interval: Duration::from_millis(1),
                heartbeat_interval: Duration::from_secs(1),
                history_batch_limits,
                backfill_control: Some(Arc::new(control)),
                ..ApiConfig::default()
            },
        )?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .context("bind benchmark HTTP API")?;
        let address = listener.local_addr()?;
        let cancellation = CancellationToken::new();
        let shutdown = cancellation.clone();
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(shutdown.cancelled_owned())
                .await
        });
        Ok(Self {
            base_url: format!("http://{address}/"),
            cancellation,
            task,
        })
    }

    async fn stop(self) -> Result<()> {
        self.cancellation.cancel();
        self.task
            .await
            .context("benchmark HTTP API task panicked")?
            .context("benchmark HTTP API failed")
    }
}

#[allow(clippy::too_many_lines)]
async fn consume_subscription(
    store: &SqliteStore,
    descriptor: &ProcessorDescriptor,
    stream_id: &str,
    consumer_id: &str,
    consumer_delay_ms: u64,
    started: Instant,
) -> Result<DeliveryMeasurement> {
    let mut measurement = DeliveryMeasurement::default();
    let mut hasher = blake3::Hasher::new();
    let mut after = 0_u64;
    let mut complete = false;
    while !complete {
        let records = store
            .consumer_changes_after_in_stream(
                descriptor,
                stream_id,
                ChainId(1),
                consumer_id,
                after,
                DELIVERY_READ_LIMIT,
            )
            .await?;
        if records.is_empty() {
            tokio::select! {
                () = store.wait_for_delivery_changes() => {}
                () = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
            continue;
        }
        measurement
            .time_to_first_batch_ms
            .get_or_insert_with(|| elapsed_milliseconds(started));
        measurement.batches = measurement.batches.saturating_add(1);
        let mut batch_sample = DeliveryBatchSample {
            processed_blocks: 0,
            domain_events: 0,
            raw_payload_bytes: 0,
            uncompressed_encoded_bytes: 0,
            transmitted_bytes: 0,
            encoded_json_bytes: 0,
            destination_transaction_ms: 0,
        };
        let mut boundary = None;
        for record in records {
            after = record.cursor.sequence;
            let encoded_bytes =
                u64::try_from(serde_json::to_vec(&record)?.len()).unwrap_or(u64::MAX);
            measurement.encoded_json_bytes =
                measurement.encoded_json_bytes.saturating_add(encoded_bytes);
            batch_sample.encoded_json_bytes = batch_sample
                .encoded_json_bytes
                .saturating_add(encoded_bytes);
            match record.change.kind.as_str() {
                kind @ ("synthetic.counter" | "blobs.block" | "uniswap.price.observation") => {
                    measurement.domain_events = measurement.domain_events.saturating_add(1);
                    batch_sample.domain_events = batch_sample.domain_events.saturating_add(1);
                    let event =
                        canonical_change_event(kind, &record.change.key, &record.change.payload)?;
                    let raw_bytes = u64::try_from(event.len()).unwrap_or(u64::MAX);
                    measurement.raw_payload_bytes =
                        measurement.raw_payload_bytes.saturating_add(raw_bytes);
                    batch_sample.raw_payload_bytes =
                        batch_sample.raw_payload_bytes.saturating_add(raw_bytes);
                    update_length_prefixed(&mut hasher, &event);
                }
                "system.backfill_progress" => {
                    measurement.progress_boundaries =
                        measurement.progress_boundaries.saturating_add(1);
                    let processed_blocks = decode_progress_blocks(&record.change.payload)?;
                    measurement.processed_blocks = measurement
                        .processed_blocks
                        .saturating_add(processed_blocks);
                    batch_sample.processed_blocks = batch_sample
                        .processed_blocks
                        .saturating_add(processed_blocks);
                    boundary = Some(record.cursor.sequence);
                }
                "system.backfill_complete" => {
                    measurement.completion_records =
                        measurement.completion_records.saturating_add(1);
                    measurement.completion_sequence = Some(record.cursor.sequence);
                    boundary = Some(record.cursor.sequence);
                    complete = true;
                }
                _ => {}
            }
        }
        // The direct sink has no transport encoder. Its serialized record
        // bytes are therefore both its uncompressed and transmitted size.
        batch_sample.uncompressed_encoded_bytes = batch_sample.encoded_json_bytes;
        batch_sample.transmitted_bytes = batch_sample.encoded_json_bytes;
        measurement.uncompressed_encoded_bytes = measurement
            .uncompressed_encoded_bytes
            .saturating_add(batch_sample.uncompressed_encoded_bytes);
        measurement.transmitted_bytes = measurement
            .transmitted_bytes
            .saturating_add(batch_sample.transmitted_bytes);
        measurement.batch_samples.push(batch_sample);
        if let Some(sequence) = boundary {
            store
                .acknowledge_consumer_in_stream(descriptor, stream_id, consumer_id, sequence)
                .await?;
            measurement.acknowledgements = measurement.acknowledgements.saturating_add(1);
        }
        if consumer_delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(consumer_delay_ms)).await;
        }
    }
    if let Some(sequence) = measurement.completion_sequence
        && let Some(subscription_id) = store
            .delivery_stream(stream_id)
            .await?
            .and_then(|stream| stream.subscription_id)
    {
        store
            .mark_backfill_subscription_reclaimable(&subscription_id, sequence)
            .await?;
    }
    store.reclaim_free_pages(4_096).await?;
    measurement.destination_digest = hasher.finalize().to_string();
    "blake3".clone_into(&mut measurement.destination_digest_algorithm);
    Ok(measurement)
}

#[allow(clippy::too_many_lines)]
async fn consume_subscription_http(
    store: &SqliteStore,
    base_url: &str,
    subscription_id: &str,
    consumer_id: &str,
    consumer_delay_ms: u64,
    compression: BenchmarkCompression,
    started: Instant,
) -> Result<DeliveryMeasurement> {
    let client = reqwest::Client::builder()
        .build()
        .context("build benchmark HTTP client")?;
    let stream_url = format!(
        "{base_url}v1/backfill-subscriptions/{subscription_id}/consumers/{consumer_id}/stream"
    );
    let mut request = client
        .get(&stream_url)
        .header(reqwest::header::ACCEPT, "application/x-ndjson");
    if matches!(compression, BenchmarkCompression::Gzip) {
        request = request.header(reqwest::header::ACCEPT_ENCODING, "gzip");
    }
    let response = request
        .send()
        .await
        .context("open benchmark delivery stream")?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        bail!("benchmark delivery stream returned {status}: {body}");
    }

    let gzip_response = response
        .headers()
        .get(reqwest::header::CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("gzip"));
    if matches!(compression, BenchmarkCompression::Gzip) && !gzip_response {
        bail!("benchmark requested gzip but the delivery stream was not compressed");
    }
    if matches!(compression, BenchmarkCompression::None) && gzip_response {
        bail!("benchmark received gzip without requesting compression");
    }

    let mut measurement = DeliveryMeasurement {
        observed_response_body_bytes: Some(0),
        ..DeliveryMeasurement::default()
    };
    let mut hasher = blake3::Hasher::new();
    let mut session_token = None;
    let mut buffer = Vec::new();
    let mut gzip_decoder = gzip_response.then(|| flate2::write::GzDecoder::new(Vec::new()));
    let mut chunks = response.bytes_stream();
    let mut complete = false;
    loop {
        let stream_ended = if let Some(chunk) = chunks.next().await {
            let chunk = chunk.context("read benchmark delivery stream")?;
            measurement.observed_response_body_bytes = measurement
                .observed_response_body_bytes
                .map(|bytes| bytes.saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX)));
            if let Some(decoder) = gzip_decoder.as_mut() {
                decoder
                    .write_all(&chunk)
                    .context("decompress benchmark delivery stream")?;
                decoder
                    .flush()
                    .context("flush benchmark delivery decompressor")?;
                drain_gzip_output(decoder, &mut buffer);
            } else {
                buffer.extend_from_slice(&chunk);
            }
            false
        } else {
            if let Some(decoder) = gzip_decoder.as_mut() {
                decoder
                    .try_finish()
                    .context("finish benchmark delivery decompressor")?;
                drain_gzip_output(decoder, &mut buffer);
            }
            true
        };
        while let Some(newline) = buffer.iter().position(|byte| *byte == b'\n') {
            let line = buffer.drain(..=newline).collect::<Vec<_>>();
            let line = &line[..line.len().saturating_sub(1)];
            if line.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            measurement.encoded_json_bytes = measurement
                .encoded_json_bytes
                .saturating_add(u64::try_from(line.len() + 1).unwrap_or(u64::MAX));
            let record: serde_json::Value =
                serde_json::from_slice(line).context("decode benchmark delivery record")?;
            let record_type = record
                .get("type")
                .and_then(serde_json::Value::as_str)
                .context("benchmark delivery record has no type")?;
            match record_type {
                "hello" => {
                    session_token = Some(
                        record
                            .get("sessionToken")
                            .and_then(serde_json::Value::as_str)
                            .context("benchmark hello has no session token")?
                            .to_owned(),
                    );
                }
                "batch" => {
                    measurement
                        .time_to_first_batch_ms
                        .get_or_insert_with(|| elapsed_milliseconds(started));
                    measurement.batches = measurement.batches.saturating_add(1);
                    measurement.progress_boundaries =
                        measurement.progress_boundaries.saturating_add(1);
                    let processed_blocks = record
                        .get("processedBlockCount")
                        .and_then(serde_json::Value::as_str)
                        .context("benchmark batch has no processed block count")?
                        .parse::<u64>()
                        .context("benchmark batch processed block count is invalid")?;
                    measurement.processed_blocks = measurement
                        .processed_blocks
                        .saturating_add(processed_blocks);
                    let domain_before = measurement.domain_events;
                    let raw_before = measurement.raw_payload_bytes;
                    let uncompressed_encoded_bytes = record
                        .get("uncompressedEncodedBytes")
                        .and_then(serde_json::Value::as_str)
                        .context("benchmark batch has no uncompressed encoded byte count")?
                        .parse::<u64>()
                        .context("benchmark batch uncompressed encoded byte count is invalid")?;
                    let transmitted_bytes = record
                        .get("transmittedBytes")
                        .and_then(serde_json::Value::as_str)
                        .context("benchmark batch has no transmitted byte count")?
                        .parse::<u64>()
                        .context("benchmark batch transmitted byte count is invalid")?;
                    measurement.uncompressed_encoded_bytes = measurement
                        .uncompressed_encoded_bytes
                        .saturating_add(uncompressed_encoded_bytes);
                    measurement.transmitted_bytes = measurement
                        .transmitted_bytes
                        .saturating_add(transmitted_bytes);
                    for change in record
                        .get("changes")
                        .and_then(serde_json::Value::as_array)
                        .context("benchmark batch has no changes")?
                    {
                        let Some(event) = canonical_http_change(change)? else {
                            continue;
                        };
                        measurement.domain_events = measurement.domain_events.saturating_add(1);
                        measurement.raw_payload_bytes = measurement
                            .raw_payload_bytes
                            .saturating_add(u64::try_from(event.len()).unwrap_or(u64::MAX));
                        update_length_prefixed(&mut hasher, &event);
                    }
                    if consumer_delay_ms > 0 {
                        tokio::time::sleep(Duration::from_millis(consumer_delay_ms)).await;
                    }
                    acknowledge_http_cursor(
                        &client,
                        base_url,
                        subscription_id,
                        consumer_id,
                        session_token
                            .as_deref()
                            .context("batch arrived before hello")?,
                        record
                            .get("acknowledgeableCursor")
                            .and_then(serde_json::Value::as_str)
                            .context("benchmark batch has no last cursor")?,
                        &mut measurement,
                    )
                    .await?;
                    measurement
                        .time_to_first_acknowledgement_ms
                        .get_or_insert_with(|| elapsed_milliseconds(started));
                    measurement.batch_samples.push(DeliveryBatchSample {
                        processed_blocks,
                        domain_events: measurement.domain_events.saturating_sub(domain_before),
                        raw_payload_bytes: measurement.raw_payload_bytes.saturating_sub(raw_before),
                        uncompressed_encoded_bytes,
                        transmitted_bytes,
                        encoded_json_bytes: u64::try_from(line.len() + 1).unwrap_or(u64::MAX),
                        destination_transaction_ms: 0,
                    });
                }
                "backfill_complete" => {
                    measurement.completion_records =
                        measurement.completion_records.saturating_add(1);
                    let cursor = record
                        .get("cursor")
                        .and_then(serde_json::Value::as_str)
                        .context("benchmark completion has no cursor")?;
                    acknowledge_http_cursor(
                        &client,
                        base_url,
                        subscription_id,
                        consumer_id,
                        session_token
                            .as_deref()
                            .context("completion arrived before hello")?,
                        cursor,
                        &mut measurement,
                    )
                    .await?;
                    measurement
                        .time_to_first_acknowledgement_ms
                        .get_or_insert_with(|| elapsed_milliseconds(started));
                    complete = true;
                }
                "heartbeat" => {}
                "error" | "reset_required" => {
                    bail!("benchmark delivery failed: {record}");
                }
                other => bail!("unexpected benchmark delivery record {other}"),
            }
        }
        if stream_ended {
            break;
        }
    }
    if !buffer.is_empty() {
        bail!("benchmark delivery stream ended with an incomplete record");
    }
    if !complete {
        bail!("benchmark delivery stream ended without completion");
    }
    store.reclaim_free_pages(4_096).await?;
    measurement.destination_digest = hasher.finalize().to_string();
    "blake3".clone_into(&mut measurement.destination_digest_algorithm);
    Ok(measurement)
}

fn drain_gzip_output(decoder: &mut flate2::write::GzDecoder<Vec<u8>>, destination: &mut Vec<u8>) {
    let output_bytes = decoder.get_mut();
    destination.extend_from_slice(output_bytes);
    output_bytes.clear();
}

async fn acknowledge_http_cursor(
    client: &reqwest::Client,
    base_url: &str,
    subscription_id: &str,
    consumer_id: &str,
    session_token: &str,
    cursor: &str,
    measurement: &mut DeliveryMeasurement,
) -> Result<()> {
    let url = format!(
        "{base_url}v1/backfill-subscriptions/{subscription_id}/consumers/{consumer_id}/ack"
    );
    let response = client
        .post(url)
        .header(reqwest::header::ACCEPT, "application/json")
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .header("x-leani-consumer-session", session_token)
        .json(&serde_json::json!({ "cursor": cursor }))
        .send()
        .await
        .context("acknowledge benchmark delivery cursor")?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        bail!("benchmark acknowledgement returned {status}: {body}");
    }
    let response: serde_json::Value = response
        .json()
        .await
        .context("decode benchmark acknowledgement")?;
    let sequence = response
        .get("acknowledgedSequence")
        .and_then(serde_json::Value::as_str)
        .context("benchmark acknowledgement has no sequence")?
        .parse::<u64>()
        .context("benchmark acknowledgement sequence is invalid")?;
    measurement.acknowledgements = measurement.acknowledgements.saturating_add(1);
    measurement.completion_sequence = Some(sequence);
    Ok(())
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn consume_subscription_sdk_postgres(
    store: &SqliteStore,
    descriptor: &ProcessorDescriptor,
    base_url: &str,
    subscription_id: &str,
    stream_id: &str,
    consumer_id: &str,
    consumer_delay_ms: u64,
    consumer_reconnect_every_batches: u64,
    consumer_drop_ack_response_once: bool,
    concurrent_live_blocks: u64,
    expected_live_events: u64,
    postgres_schema: BenchmarkPostgresSchema,
    expected_destination_rows: u64,
    iteration: u32,
    benchmark_started_unix_ms: u64,
) -> Result<DeliveryMeasurement> {
    if std::env::var_os("LEANI_BENCHMARK_POSTGRES_URL").is_none() {
        bail!(
            "sdk-postgres requires LEANI_BENCHMARK_POSTGRES_URL pointing to a dedicated benchmark database"
        );
    }
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .context("resolve benchmark repository root")?;
    let script = repository.join("packages/sdk/benchmark/postgres-destination.ts");
    if !script.is_file() {
        bail!(
            "SDK PostgreSQL benchmark fixture is missing: {}",
            script.display()
        );
    }
    let base_url = base_url.to_owned();
    let subscription_id = subscription_id.to_owned();
    let processor_instance = descriptor.instance.to_string();
    let command_consumer_id = consumer_id.to_owned();
    let run_id = format!(
        "leani-{}-{iteration}-{}",
        std::process::id(),
        now_milliseconds()
    );
    let output = tokio::task::spawn_blocking(move || {
        Command::new("bun")
            .arg("run")
            .arg(script)
            .arg("--base-url")
            .arg(base_url)
            .arg("--subscription")
            .arg(subscription_id)
            .arg("--processor")
            .arg(processor_instance)
            .arg("--consumer")
            .arg(command_consumer_id)
            .arg("--run-id")
            .arg(run_id)
            .arg("--consumer-delay-ms")
            .arg(consumer_delay_ms.to_string())
            .arg("--consumer-reconnect-every-batches")
            .arg(consumer_reconnect_every_batches.to_string())
            .arg("--consumer-drop-ack-response-once")
            .arg(consumer_drop_ack_response_once.to_string())
            .arg("--concurrent-live-blocks")
            .arg(concurrent_live_blocks.to_string())
            .arg("--expected-live-events")
            .arg(expected_live_events.to_string())
            .arg("--destination-schema")
            .arg(postgres_schema_name(postgres_schema))
            .arg("--expected-destination-rows")
            .arg(expected_destination_rows.to_string())
            .arg("--benchmark-started-unix-ms")
            .arg(benchmark_started_unix_ms.to_string())
            .current_dir(repository)
            .output()
    })
    .await
    .context("SDK PostgreSQL benchmark task panicked")?
    .context("start Bun SDK PostgreSQL destination")?;
    if !output.status.success() {
        bail!(
            "SDK PostgreSQL destination failed with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let output = String::from_utf8(output.stdout).context("SDK destination output is UTF-8")?;
    let sdk: SdkDestinationMeasurement =
        serde_json::from_str(output.trim()).context("decode SDK destination report")?;
    let (expected_schema, expected_row_semantics) = postgres_contract(postgres_schema);
    if sdk.destination_schema != expected_schema
        || sdk.destination_row_semantics != expected_row_semantics
    {
        bail!(
            "SDK destination reported unsupported row contract {}/{}",
            sdk.destination_schema,
            sdk.destination_row_semantics
        );
    }
    let completion_sequence = sdk
        .completion_sequence
        .as_deref()
        .context("SDK destination did not acknowledge completion")?
        .parse::<u64>()
        .context("SDK completion sequence is invalid")?;
    let consumer = store
        .consumer_in_stream(descriptor, stream_id, consumer_id)
        .await?
        .context("SDK destination consumer disappeared")?;
    if consumer.acknowledged_sequence != completion_sequence {
        bail!(
            "SDK destination reported acknowledgement {}, node retained {}",
            completion_sequence,
            consumer.acknowledged_sequence
        );
    }
    let live_destination = if concurrent_live_blocks == 0 {
        None
    } else if expected_live_events == 0 {
        Some(LiveDestinationMeasurement {
            delivered_blocks: sdk.live_processed_blocks,
            delivered_domain_events: sdk.live_domain_events,
            canonical_output_bytes: sdk.live_raw_payload_bytes,
            batches: sdk.live_batches,
            acknowledgements: sdk.live_acknowledgements,
            acknowledged_sequence: 0,
            destination_digest: sdk.live_destination_digest,
            time_to_first_destination_batch_ms: sdk.time_to_first_live_batch_ms,
        })
    } else {
        let acknowledged_sequence = sdk
            .live_acknowledged_sequence
            .as_deref()
            .context("SDK destination did not acknowledge concurrent live traffic")?
            .parse::<u64>()
            .context("SDK live acknowledgement sequence is invalid")?;
        let live_stream_id = default_delivery_stream_id(descriptor);
        let consumer = store
            .consumer_in_stream(descriptor, &live_stream_id, consumer_id)
            .await?
            .context("SDK live destination consumer disappeared")?;
        if consumer.acknowledged_sequence != acknowledged_sequence {
            bail!(
                "SDK live destination reported acknowledgement {}, node retained {}",
                acknowledged_sequence,
                consumer.acknowledged_sequence
            );
        }
        Some(LiveDestinationMeasurement {
            delivered_blocks: sdk.live_processed_blocks,
            delivered_domain_events: sdk.live_domain_events,
            canonical_output_bytes: sdk.live_raw_payload_bytes,
            batches: sdk.live_batches,
            acknowledgements: sdk.live_acknowledgements,
            acknowledged_sequence,
            destination_digest: sdk.live_destination_digest,
            time_to_first_destination_batch_ms: sdk.time_to_first_live_batch_ms,
        })
    };
    store.reclaim_free_pages(4_096).await?;
    Ok(DeliveryMeasurement {
        batches: sdk.batches,
        processed_blocks: sdk.processed_blocks,
        domain_events: sdk.domain_events,
        progress_boundaries: sdk.progress_boundaries,
        completion_records: sdk.completion_records,
        raw_payload_bytes: sdk.raw_payload_bytes,
        uncompressed_encoded_bytes: sdk.uncompressed_encoded_bytes,
        transmitted_bytes: sdk.transmitted_bytes,
        encoded_json_bytes: sdk.encoded_json_bytes,
        observed_response_body_bytes: None,
        acknowledgements: sdk.acknowledgements,
        consumer_reconnects: sdk.consumer_reconnects,
        simulated_ack_response_losses: sdk.simulated_ack_response_losses,
        pruned_records: sdk.pruned_records,
        completion_sequence: Some(completion_sequence),
        destination_digest: sdk.destination_digest,
        destination_digest_algorithm: "sha256".to_owned(),
        destination_transactions: sdk.destination_transactions,
        destination_transaction_ms: sdk.destination_transaction_ms,
        time_to_first_batch_ms: sdk.time_to_first_batch_ms,
        time_to_first_destination_commit_ms: sdk.time_to_first_destination_commit_ms,
        time_to_first_acknowledgement_ms: sdk.time_to_first_acknowledgement_ms,
        producer_complete_milliseconds: None,
        consumer_complete_milliseconds: None,
        post_producer_drain_milliseconds: None,
        producer_blocks_per_second_milli: None,
        consumer_blocks_per_second_milli: None,
        domain_events_per_second_milli: None,
        raw_payload_bytes_per_second: None,
        transmitted_bytes_per_second: None,
        destination_row_count: Some(RowCountCheck::exact(
            expected_schema,
            expected_row_semantics,
            expected_destination_rows,
            sdk.destination_rows,
        )),
        destination_through_block: Some(sdk.destination_through_block),
        destination_table_bytes: Some(sdk.destination_table_bytes),
        destination_index_bytes: Some(sdk.destination_index_bytes),
        destination_total_bytes: Some(sdk.destination_total_bytes),
        destination_wal_written_bytes: Some(sdk.destination_wal_written_bytes),
        batch_samples: sdk.batch_samples,
        concurrent_live: None,
        live_destination,
    })
}

fn decode_hex_json(value: &serde_json::Value, field: &str) -> Result<Vec<u8>> {
    let encoded = value
        .get(field)
        .and_then(serde_json::Value::as_str)
        .with_context(|| format!("benchmark change has no {field}"))?;
    decode_hex(encoded)
}

fn canonical_http_change(change: &serde_json::Value) -> Result<Option<Vec<u8>>> {
    let Some(kind) = change.get("kind").and_then(serde_json::Value::as_str) else {
        bail!("benchmark change has no kind");
    };
    let key = match kind {
        "synthetic.counter.put" | "blobs.block.put" | "uniswap.price.observation.put" => {
            decode_hex_json(change, "key")?
        }
        _ => return Ok(None),
    };
    let data = change
        .get("data")
        .context("benchmark domain change has no data")?;
    match kind {
        "synthetic.counter.put" => {
            let payload = data
                .get("value")
                .and_then(serde_json::Value::as_str)
                .context("counter change has no hex payload")?;
            canonical_change_event("synthetic.counter", &key, &decode_hex(payload)?).map(Some)
        }
        "blobs.block.put" => {
            let block_hash = decode_json_hex(data, &["block", "blockHash"])?;
            let transactions = data
                .get("transactions")
                .and_then(serde_json::Value::as_array)
                .context("blobs change has no transactions")?;
            let mut event = Vec::new();
            event.push(1);
            event.extend_from_slice(&key);
            event.extend_from_slice(&block_hash);
            event.extend_from_slice(
                &u32::try_from(transactions.len())
                    .unwrap_or(u32::MAX)
                    .to_be_bytes(),
            );
            for transaction in transactions {
                event.extend_from_slice(&decode_json_hex(transaction, &["txHash"])?);
                let blob_count = transaction
                    .get("blobCount")
                    .and_then(serde_json::Value::as_u64)
                    .context("blobs transaction has no blob count")?;
                event.extend_from_slice(
                    &u32::try_from(blob_count)
                        .context("blobs transaction count exceeds u32")?
                        .to_be_bytes(),
                );
                for hash in transaction
                    .get("blobVersionedHashes")
                    .and_then(serde_json::Value::as_array)
                    .context("blobs transaction has no versioned hashes")?
                {
                    event.extend_from_slice(&decode_hex(
                        hash.as_str().context("blob hash is not a string")?,
                    )?);
                }
            }
            Ok(Some(event))
        }
        "uniswap.price.observation.put" => {
            let decimal = data
                .get("sqrtPriceX96")
                .and_then(serde_json::Value::as_str)
                .context("Uniswap observation has no square-root price")?;
            let sqrt_price = decimal
                .parse::<U256>()
                .context("Uniswap square-root price is not a uint256")?;
            let mut event = Vec::with_capacity(1 + key.len() + 32);
            event.push(2);
            event.extend_from_slice(&key);
            event.extend_from_slice(&sqrt_price.to_be_bytes::<32>());
            Ok(Some(event))
        }
        _ => Ok(None),
    }
}

fn decode_json_hex(value: &serde_json::Value, path: &[&str]) -> Result<Vec<u8>> {
    let mut current = value;
    for field in path {
        current = current
            .get(*field)
            .with_context(|| format!("benchmark change has no {}", path.join(".")))?;
    }
    decode_hex(
        current
            .as_str()
            .with_context(|| format!("benchmark {} is not a string", path.join(".")))?,
    )
}

fn decode_hex(encoded: &str) -> Result<Vec<u8>> {
    hex::decode(encoded.strip_prefix("0x").unwrap_or(encoded))
        .map_err(|error| anyhow!("benchmark change contains invalid hex: {error}"))
}

fn decode_progress_blocks(payload: &[u8]) -> Result<u64> {
    if payload.is_empty() {
        return Ok(1);
    }
    let processed = payload
        .get(16..24)
        .context("benchmark progress payload has an invalid length")?;
    let processed: [u8; 8] = processed
        .try_into()
        .map_err(|_| anyhow!("benchmark progress block count is invalid"))?;
    Ok(u64::from_be_bytes(processed))
}

async fn query_processor_output(
    store: &SqliteStore,
    processor: &dyn Processor,
    kind: SyntheticCorpusKind,
    expected_rows: u64,
) -> Result<QueryMeasurement> {
    if kind == SyntheticCorpusKind::BlobsLike {
        return query_blobs_materialized_output(store, processor, expected_rows).await;
    }
    let started = Instant::now();
    let mut after = None::<Vec<u8>>;
    let mut rows = 0_u64;
    let mut bytes = 0_u64;
    let mut canonical_output_bytes = 0_u64;
    let mut hasher = blake3::Hasher::new();
    let collection = benchmark_output_collection(kind);
    loop {
        let page = store
            .scan_entities(processor.descriptor(), collection, after.as_deref(), 10_000)
            .await?;
        if page.is_empty() {
            break;
        }
        for (key, value) in &page {
            rows = rows.saturating_add(1);
            bytes = bytes
                .saturating_add(u64::try_from(key.len()).unwrap_or(u64::MAX))
                .saturating_add(u64::try_from(value.len()).unwrap_or(u64::MAX));
            let event = canonical_change_event(benchmark_domain_kind(kind), key, value)?;
            canonical_output_bytes = canonical_output_bytes
                .saturating_add(u64::try_from(event.len()).unwrap_or(u64::MAX));
            update_length_prefixed(&mut hasher, &event);
        }
        after = page.last().map(|(key, _)| key.clone());
        if page.len() < 10_000 {
            break;
        }
    }
    Ok(QueryMeasurement {
        elapsed_milliseconds: elapsed_milliseconds(started),
        row_count: RowCountCheck::exact(
            format!(
                "sqlite.{}.{collection}",
                processor.descriptor().schemas.entity_schema
            ),
            benchmark_query_row_semantics(kind),
            expected_rows,
            rows,
        ),
        payload_bytes: bytes,
        canonical_output_bytes,
        digest: hasher.finalize().to_string(),
    })
}

async fn query_blobs_materialized_output(
    store: &SqliteStore,
    processor: &dyn Processor,
    expected_rows: u64,
) -> Result<QueryMeasurement> {
    let started = Instant::now();
    let mut after = None::<Vec<u8>>;
    let mut rows = 0_u64;
    let mut bytes = 0_u64;
    let mut canonical_output_bytes = 0_u64;
    let mut hasher = blake3::Hasher::new();
    loop {
        let page = store
            .scan_entities(
                processor.descriptor(),
                BLOCK_COLLECTION,
                after.as_deref(),
                10_000,
            )
            .await?;
        if page.is_empty() {
            break;
        }
        for (key, value) in &page {
            let block: BlobsBlockEntity =
                postcard::from_bytes(value).context("decode benchmark materialized blobs block")?;
            let transaction_keys = store
                .index_keys(processor.descriptor(), TRANSACTION_BLOCK_INDEX, key, 10_000)
                .await?;
            let mut transactions = Vec::with_capacity(transaction_keys.len());
            bytes = bytes
                .saturating_add(u64::try_from(key.len()).unwrap_or(u64::MAX))
                .saturating_add(u64::try_from(value.len()).unwrap_or(u64::MAX));
            for transaction_key in transaction_keys {
                let transaction_value = store
                    .entity(
                        processor.descriptor(),
                        TRANSACTION_COLLECTION,
                        &transaction_key,
                    )
                    .await?
                    .context("blobs transaction index points to a missing entity")?;
                bytes = bytes
                    .saturating_add(u64::try_from(transaction_key.len()).unwrap_or(u64::MAX))
                    .saturating_add(u64::try_from(transaction_value.len()).unwrap_or(u64::MAX));
                transactions.push(
                    postcard::from_bytes::<BlobTransactionEntity>(&transaction_value)
                        .context("decode benchmark materialized blobs transaction")?,
                );
            }
            transactions.sort_by_key(|transaction| transaction.transaction_hash);
            let event = canonical_blobs_event(key, &block, &transactions);
            rows = rows.saturating_add(1);
            canonical_output_bytes = canonical_output_bytes
                .saturating_add(u64::try_from(event.len()).unwrap_or(u64::MAX));
            update_length_prefixed(&mut hasher, &event);
        }
        after = page.last().map(|(key, _)| key.clone());
        if page.len() < 10_000 {
            break;
        }
    }
    Ok(QueryMeasurement {
        elapsed_milliseconds: elapsed_milliseconds(started),
        row_count: RowCountCheck::exact(
            format!(
                "sqlite.{}.reconstructed_blobs_snapshot",
                processor.descriptor().schemas.entity_schema
            ),
            benchmark_query_row_semantics(SyntheticCorpusKind::BlobsLike),
            expected_rows,
            rows,
        ),
        payload_bytes: bytes,
        canonical_output_bytes,
        digest: hasher.finalize().to_string(),
    })
}

async fn query_processor_artifacts(
    store: &SqliteStore,
    processor: &dyn Processor,
    kind: SyntheticCorpusKind,
    range: BlockRange,
    expected_events: u64,
) -> Result<QueryMeasurement> {
    let started = Instant::now();
    let mut next = range.start();
    let mut rows = 0_u64;
    let mut events = 0_u64;
    let mut payload_bytes = 0_u64;
    let mut canonical_output_bytes = 0_u64;
    let mut hasher = blake3::Hasher::new();
    loop {
        let page_range = BlockRange::new(next, range.end())?;
        let page = store
            .scan_processor_artifacts(processor.descriptor(), page_range, 10_000)
            .await?;
        if page.is_empty() {
            break;
        }
        for artifact in &page {
            rows = rows.saturating_add(1);
            payload_bytes = payload_bytes
                .saturating_add(u64::try_from(artifact.delta.payload.len()).unwrap_or(u64::MAX));
            for event in canonical_artifact_events(kind, &artifact.delta)? {
                events = events.saturating_add(1);
                canonical_output_bytes = canonical_output_bytes
                    .saturating_add(u64::try_from(event.len()).unwrap_or(u64::MAX));
                update_length_prefixed(&mut hasher, &event);
            }
        }
        let last = page
            .last()
            .context("processor artifact page unexpectedly empty")?
            .delta
            .block
            .number;
        if last == range.end() {
            break;
        }
        next = BlockNumber(last.0.saturating_add(1));
    }
    if events != expected_events {
        bail!(
            "processor artifact event count mismatch: expected {expected_events}, observed {events}"
        );
    }
    Ok(QueryMeasurement {
        elapsed_milliseconds: elapsed_milliseconds(started),
        row_count: RowCountCheck::exact(
            format!(
                "{}.delta.v{}",
                processor.descriptor().id,
                processor.descriptor().schemas.delta_version
            ),
            "one_finalized_processor_artifact_per_mapped_block",
            range.len(),
            rows,
        ),
        payload_bytes,
        canonical_output_bytes,
        digest: hasher.finalize().to_string(),
    })
}

async fn query_segment_processor_artifacts(
    sink: &ArtifactSegmentSink,
    processor: &dyn Processor,
    kind: SyntheticCorpusKind,
    range: BlockRange,
    expected_events: u64,
) -> Result<QueryMeasurement> {
    let started = Instant::now();
    let mut next = range.start();
    let mut rows = 0_u64;
    let mut events = 0_u64;
    let mut payload_bytes = 0_u64;
    let mut canonical_output_bytes = 0_u64;
    let mut hasher = blake3::Hasher::new();
    loop {
        let page_range = BlockRange::new(next, range.end())?;
        let page = sink
            .scan(processor.descriptor(), page_range, 10_000)
            .await?;
        if page.is_empty() {
            break;
        }
        for delta in &page {
            rows = rows.saturating_add(1);
            payload_bytes = payload_bytes
                .saturating_add(u64::try_from(delta.payload.len()).unwrap_or(u64::MAX));
            for event in canonical_artifact_events(kind, delta)? {
                events = events.saturating_add(1);
                canonical_output_bytes = canonical_output_bytes
                    .saturating_add(u64::try_from(event.len()).unwrap_or(u64::MAX));
                update_length_prefixed(&mut hasher, &event);
            }
        }
        let last = page
            .last()
            .context("segment artifact page unexpectedly empty")?
            .block
            .number;
        if last == range.end() {
            break;
        }
        next = BlockNumber(last.0.saturating_add(1));
    }
    if events != expected_events {
        bail!(
            "segment artifact event count mismatch: expected {expected_events}, observed {events}"
        );
    }
    Ok(QueryMeasurement {
        elapsed_milliseconds: elapsed_milliseconds(started),
        row_count: RowCountCheck::exact(
            format!(
                "{}.delta.v{}",
                processor.descriptor().id,
                processor.descriptor().schemas.delta_version
            ),
            "one_finalized_processor_artifact_per_mapped_block_in_immutable_segments",
            range.len(),
            rows,
        ),
        payload_bytes,
        canonical_output_bytes,
        digest: hasher.finalize().to_string(),
    })
}

#[allow(clippy::too_many_lines)]
async fn measure_artifact_segment_candidate(
    store: &SqliteStore,
    processor: &dyn Processor,
    range: BlockRange,
    directory: PathBuf,
    target_blocks: u64,
    compression: ArtifactCompression,
) -> Result<ArtifactSegmentCandidateMeasurement> {
    let mut next = range.start();
    let mut segment_paths = Vec::new();
    let mut input_hasher = blake3::Hasher::new();
    let mut sqlite_scan = Duration::ZERO;
    let mut segment_write = Duration::ZERO;
    let mut artifacts = 0_u64;
    let mut logical_bytes = 0_u64;
    let mut physical_bytes = 0_u64;
    let limits = ArtifactSegmentLimits {
        maximum_artifact_logical_bytes: 64 * 1024 * 1024,
        maximum_segment_logical_bytes: 1024 * 1024 * 1024,
        maximum_segment_physical_bytes: 1024 * 1024 * 1024,
    };

    loop {
        let segment_end = BlockNumber(
            next.0
                .saturating_add(target_blocks.saturating_sub(1))
                .min(range.end().0),
        );
        let segment_range = BlockRange::new(next, segment_end)?;
        let scan_started = Instant::now();
        let page = store
            .scan_processor_artifacts(
                processor.descriptor(),
                segment_range,
                usize::try_from(segment_range.len()).context("artifact segment scan limit")?,
            )
            .await?;
        sqlite_scan = sqlite_scan.saturating_add(scan_started.elapsed());
        if u64::try_from(page.len()).unwrap_or(u64::MAX) != segment_range.len() {
            bail!(
                "artifact segment candidate expected {} SQLite artifacts for {}..={}, observed {}",
                segment_range.len(),
                segment_range.start().0,
                segment_range.end().0,
                page.len()
            );
        }
        for artifact in &page {
            let encoded = artifact.delta.encode_durable()?;
            logical_bytes =
                logical_bytes.saturating_add(u64::try_from(encoded.len()).unwrap_or(u64::MAX));
            update_length_prefixed(&mut input_hasher, &encoded);
        }

        let path = directory.join(format!(
            "{:020}-{:020}.artifacts",
            segment_range.start().0,
            segment_range.end().0
        ));
        let write_started = Instant::now();
        let mut writer = ArtifactSegmentWriter::create(
            &path,
            processor.descriptor(),
            ChainId(1),
            segment_range,
            compression,
            limits,
        )?;
        for artifact in &page {
            writer.append(&artifact.delta)?;
        }
        let metadata = writer.finish()?;
        segment_write = segment_write.saturating_add(write_started.elapsed());
        artifacts = artifacts.saturating_add(metadata.artifacts);
        physical_bytes = physical_bytes.saturating_add(metadata.physical_bytes);
        segment_paths.push((segment_range, path));
        if segment_end == range.end() {
            break;
        }
        next = BlockNumber(segment_end.0.saturating_add(1));
    }

    let midpoint = BlockNumber(range.start().0.saturating_add(range.len() / 2));
    let mut verified_hasher = blake3::Hasher::new();
    let mut verified_artifacts = 0_u64;
    let mut verified_logical_bytes = 0_u64;
    let mut expected_parent = None;
    let mut exact_lookup_microseconds = None;
    let verify_started = Instant::now();
    for (segment_range, path) in &segment_paths {
        let reader = ArtifactSegmentReader::open(path, processor.descriptor())?;
        if segment_range.contains(midpoint) {
            let lookup_started = Instant::now();
            let exact = reader.read(midpoint)?;
            exact_lookup_microseconds =
                Some(u64::try_from(lookup_started.elapsed().as_micros()).unwrap_or(u64::MAX));
            if exact.block.number != midpoint {
                bail!("artifact segment exact lookup returned the wrong block");
            }
        }
        let decoded = reader.scan(
            *segment_range,
            usize::try_from(segment_range.len()).context("artifact segment verify limit")?,
        )?;
        for delta in decoded {
            let block = delta.block.number;
            if expected_parent.is_some_and(|parent| delta.block.parent_hash != parent) {
                bail!(
                    "artifact segment candidate parent mismatch at block {}",
                    block.0
                );
            }
            expected_parent = Some(delta.block.hash);
            let encoded = delta.encode_durable()?;
            verified_artifacts = verified_artifacts.saturating_add(1);
            verified_logical_bytes = verified_logical_bytes
                .saturating_add(u64::try_from(encoded.len()).unwrap_or(u64::MAX));
            update_length_prefixed(&mut verified_hasher, &encoded);
        }
    }
    let segment_verify_milliseconds = duration_milliseconds(verify_started.elapsed());
    let input_digest = input_hasher.finalize().to_string();
    let verified_digest = verified_hasher.finalize().to_string();
    let correctness_passed = artifacts == range.len()
        && verified_artifacts == artifacts
        && verified_logical_bytes == logical_bytes
        && verified_digest == input_digest
        && exact_lookup_microseconds.is_some();
    let storage_amplification_milli = (logical_bytes > 0).then(|| {
        let scaled = u128::from(physical_bytes).saturating_mul(1_000);
        u64::try_from(scaled / u128::from(logical_bytes)).unwrap_or(u64::MAX)
    });
    Ok(ArtifactSegmentCandidateMeasurement {
        input: "post_process_sqlite_artifact_scan_v1_not_segment_backend_throughput",
        compression: artifact_compression_name_from_store(compression),
        target_blocks_per_segment: target_blocks,
        segments: u64::try_from(segment_paths.len()).unwrap_or(u64::MAX),
        artifacts,
        logical_bytes,
        physical_bytes,
        storage_amplification_milli,
        sqlite_scan_milliseconds: duration_milliseconds(sqlite_scan),
        segment_write_milliseconds: duration_milliseconds(segment_write),
        segment_verify_milliseconds,
        exact_lookup_microseconds: exact_lookup_microseconds.unwrap_or(u64::MAX),
        input_digest,
        verified_digest,
        correctness_passed,
    })
}

fn canonical_artifact_events(
    kind: SyntheticCorpusKind,
    delta: &EncodedDelta,
) -> Result<Vec<Vec<u8>>> {
    match kind {
        SyntheticCorpusKind::BlobsLike => Ok(vec![canonical_change_event(
            "blobs.block",
            &delta.block.number.0.to_be_bytes(),
            &delta.payload,
        )?]),
        SyntheticCorpusKind::UniswapLike => {
            let decoded: UniswapPriceDelta = postcard::from_bytes(&delta.payload)
                .context("decode benchmark Uniswap processor artifact")?;
            decoded
                .observations
                .into_iter()
                .map(|observation| {
                    let mut key = Vec::with_capacity(56);
                    key.extend_from_slice(&observation.pool.0);
                    key.extend_from_slice(&observation.block_hash.0);
                    key.extend_from_slice(&observation.log_index.to_be_bytes());
                    let payload = postcard::to_allocvec(&observation)
                        .context("encode benchmark Uniswap artifact observation")?;
                    canonical_change_event("uniswap.price.observation", &key, &payload)
                })
                .collect()
        }
        SyntheticCorpusKind::Zero | SyntheticCorpusKind::Sparse | SyntheticCorpusKind::Dense => {
            Ok(vec![canonical_change_event(
                "synthetic.counter",
                &delta.block.canonical_key(delta.chain_id).encode_ordered(),
                &delta.payload,
            )?])
        }
    }
}

const fn benchmark_query_row_semantics(kind: SyntheticCorpusKind) -> &'static str {
    match kind {
        SyntheticCorpusKind::BlobsLike => "one_reconstructed_block_snapshot_per_blobs_delta",
        SyntheticCorpusKind::UniswapLike => "one_observation_entity_per_price_delta",
        SyntheticCorpusKind::Zero | SyntheticCorpusKind::Sparse | SyntheticCorpusKind::Dense => {
            "one_counter_entity_per_processor_delta"
        }
    }
}

impl Sampler {
    fn start(
        interval_ms: u64,
        source: Arc<GeneratedHistorySource>,
        store: Option<SqliteStore>,
        raw_history_store: Option<HistoryStore>,
        descriptor: Option<ProcessorDescriptor>,
        material_coordinator: Option<HistoricalMaterialCoordinator>,
        artifact_segment_sink: Option<ArtifactSegmentSink>,
    ) -> Self {
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let samples = Arc::new(Mutex::new(Vec::new()));
        let task_samples = samples.clone();
        let started = Instant::now();
        let task = tokio::spawn(async move {
            loop {
                sample_once(
                    &task_samples,
                    started,
                    &source,
                    SampleStores {
                        processor: store.as_ref(),
                        raw_history: raw_history_store.as_ref(),
                    },
                    descriptor.as_ref(),
                    material_coordinator.as_ref(),
                    artifact_segment_sink.as_ref(),
                )
                .await;
                tokio::select! {
                    () = task_cancellation.cancelled() => {
                        sample_once(
                            &task_samples,
                            started,
                            &source,
                            SampleStores {
                                processor: store.as_ref(),
                                raw_history: raw_history_store.as_ref(),
                            },
                            descriptor.as_ref(),
                            material_coordinator.as_ref(),
                            artifact_segment_sink.as_ref(),
                        ).await;
                        break;
                    }
                    () = tokio::time::sleep(Duration::from_millis(interval_ms)) => {}
                }
            }
        });
        Self {
            cancellation,
            samples,
            task,
        }
    }

    fn start_real(
        interval_ms: u64,
        sources: Vec<Arc<dyn HistorySource>>,
        store: SqliteStore,
        descriptor: ProcessorDescriptor,
        material_coordinator: Option<HistoricalMaterialCoordinator>,
    ) -> Self {
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let samples = Arc::new(Mutex::new(Vec::new()));
        let task_samples = samples.clone();
        let started = Instant::now();
        let task = tokio::spawn(async move {
            loop {
                sample_real_once(
                    &task_samples,
                    started,
                    &sources,
                    &store,
                    &descriptor,
                    material_coordinator.as_ref(),
                )
                .await;
                tokio::select! {
                    () = task_cancellation.cancelled() => {
                        sample_real_once(
                            &task_samples,
                            started,
                            &sources,
                            &store,
                            &descriptor,
                            material_coordinator.as_ref(),
                        ).await;
                        break;
                    }
                    () = tokio::time::sleep(Duration::from_millis(interval_ms)) => {}
                }
            }
        });
        Self {
            cancellation,
            samples,
            task,
        }
    }

    async fn finish(self) -> Vec<BenchmarkSample> {
        self.cancellation.cancel();
        let _ = self.task.await;
        self.samples.lock().await.clone()
    }
}

async fn sample_real_once(
    samples: &Mutex<Vec<BenchmarkSample>>,
    started: Instant,
    sources: &[Arc<dyn HistorySource>],
    store: &SqliteStore,
    descriptor: &ProcessorDescriptor,
    material_coordinator: Option<&HistoricalMaterialCoordinator>,
) {
    let (source_frames, source_bytes) = sources.iter().fold((0_u64, 0_u64), |total, source| {
        source.acquisition_metrics().map_or(total, |metrics| {
            (
                total.0.saturating_add(metrics.acquired_frames),
                total.1.saturating_add(metrics.normalized_bytes),
            )
        })
    });
    let storage = store.budget_stats().await.ok();
    let material = material_coordinator.map(HistoricalMaterialCoordinator::snapshot);
    let committed_through = store
        .processor_cursor(descriptor)
        .await
        .ok()
        .flatten()
        .map(|cursor| cursor.block_number.0);
    samples.lock().await.push(BenchmarkSample {
        elapsed_milliseconds: elapsed_milliseconds(started),
        rss_bytes: process_rss_bytes(),
        source_frames,
        source_estimated_bytes: source_bytes,
        committed_through,
        database_bytes: storage.as_ref().map(|stats| stats.database_bytes),
        freelist_bytes: storage.as_ref().map(|stats| stats.freelist_bytes),
        wal_bytes: storage.as_ref().map(|stats| stats.wal_bytes),
        physical_store_bytes: storage
            .as_ref()
            .map(|stats| stats.physical_file_bytes.saturating_add(stats.wal_bytes)),
        non_sqlite_segment_bytes: None,
        raw_history_segment_bytes: None,
        raw_history_catalog_bytes: None,
        delivery_retained_bytes: storage.as_ref().map(|stats| stats.delivery_retained_bytes),
        history_delivery_retained_bytes: storage
            .as_ref()
            .map(|stats| stats.history_delivery_retained_bytes),
        pending_delta_bytes: storage.as_ref().map(|stats| stats.pending_delta_bytes),
        processor_artifact_bytes: storage.as_ref().map(|stats| stats.processor_artifact_bytes),
        pending_processor_artifact_bytes: storage
            .as_ref()
            .map(|stats| stats.pending_processor_artifact_bytes),
        active_material_acquisitions: material
            .as_ref()
            .map(|snapshot| snapshot.active_acquisitions),
        material_buffered_bytes: material.as_ref().map(|snapshot| snapshot.buffered_bytes),
    });
}

async fn sample_once(
    samples: &Mutex<Vec<BenchmarkSample>>,
    started: Instant,
    source: &GeneratedHistorySource,
    stores: SampleStores<'_>,
    descriptor: Option<&ProcessorDescriptor>,
    material_coordinator: Option<&HistoricalMaterialCoordinator>,
    artifact_segment_sink: Option<&ArtifactSegmentSink>,
) {
    let source = source.stats();
    let storage = if let Some(store) = stores.processor {
        store.budget_stats().await.ok()
    } else {
        None
    };
    let material = material_coordinator.map(HistoricalMaterialCoordinator::snapshot);
    let raw_history = if let Some(store) = stores.raw_history {
        store.stats().await.ok()
    } else {
        None
    };
    let artifact_segments = if let Some(sink) = artifact_segment_sink {
        Some(sink.stats().await)
    } else if let Some(store) = stores.processor {
        store.processor_artifact_segment_stats().await
    } else {
        None
    };
    let committed_through = if let (Some(store), Some(descriptor)) = (stores.processor, descriptor)
    {
        store
            .processor_cursor(descriptor)
            .await
            .ok()
            .flatten()
            .map(|cursor| cursor.block_number.0)
    } else {
        None
    };
    samples.lock().await.push(BenchmarkSample {
        elapsed_milliseconds: elapsed_milliseconds(started),
        rss_bytes: process_rss_bytes(),
        source_frames: source.frames,
        source_estimated_bytes: source.estimated_bytes,
        committed_through,
        database_bytes: storage.as_ref().map(|stats| stats.database_bytes),
        freelist_bytes: storage.as_ref().map(|stats| stats.freelist_bytes),
        wal_bytes: storage.as_ref().map(|stats| stats.wal_bytes),
        physical_store_bytes: (storage.is_some() || raw_history.is_some()).then(|| {
            storage
                .as_ref()
                .map_or(0, |stats| {
                    stats
                        .physical_file_bytes
                        .saturating_add(stats.wal_bytes)
                        .saturating_add(
                            artifact_segments.map_or(0, |segments| segments.physical_bytes),
                        )
                })
                .saturating_add(raw_history.map_or(0, |stats| stats.total_physical_bytes))
        }),
        non_sqlite_segment_bytes: artifact_segments.map(|segments| segments.physical_bytes),
        raw_history_segment_bytes: raw_history.map(|stats| stats.retained_segment_physical_bytes),
        raw_history_catalog_bytes: raw_history.map(|stats| stats.catalog_physical_bytes),
        delivery_retained_bytes: storage.as_ref().map(|stats| stats.delivery_retained_bytes),
        history_delivery_retained_bytes: storage
            .as_ref()
            .map(|stats| stats.history_delivery_retained_bytes),
        pending_delta_bytes: storage.as_ref().map(|stats| stats.pending_delta_bytes),
        processor_artifact_bytes: storage.as_ref().map(|stats| stats.processor_artifact_bytes),
        pending_processor_artifact_bytes: storage
            .as_ref()
            .map(|stats| stats.pending_processor_artifact_bytes),
        active_material_acquisitions: material
            .as_ref()
            .map(|snapshot| snapshot.active_acquisitions),
        material_buffered_bytes: material.as_ref().map(|snapshot| snapshot.buffered_bytes),
    });
}

#[allow(clippy::cast_precision_loss)]
fn summarize(runs: &[BenchmarkRunReport]) -> BenchmarkSummary {
    let mut elapsed = runs
        .iter()
        .map(|run| run.elapsed_milliseconds)
        .collect::<Vec<_>>();
    let mut throughput = runs
        .iter()
        .map(|run| run.blocks_per_second_milli)
        .collect::<Vec<_>>();
    elapsed.sort_unstable();
    throughput.sort_unstable();
    let mean = if elapsed.is_empty() {
        0.0
    } else {
        elapsed.iter().map(|value| *value as f64).sum::<f64>() / elapsed.len() as f64
    };
    let variance = if elapsed.len() < 2 || mean == 0.0 {
        0.0
    } else {
        elapsed
            .iter()
            .map(|value| {
                let delta = *value as f64 - mean;
                delta * delta
            })
            .sum::<f64>()
            / elapsed.len() as f64
    };
    BenchmarkSummary {
        runs: runs.len(),
        median_elapsed_milliseconds: percentile(&elapsed, 50),
        p95_elapsed_milliseconds: percentile(&elapsed, 95),
        median_blocks_per_second_milli: percentile(&throughput, 50),
        p95_blocks_per_second_milli: percentile(&throughput, 95),
        coefficient_of_variation: if mean == 0.0 {
            0.0
        } else {
            variance.sqrt() / mean
        },
        peak_rss_bytes: runs.iter().filter_map(|run| run.peak_rss_bytes).max(),
        peak_physical_store_bytes: runs
            .iter()
            .filter_map(|run| run.peak_physical_store_bytes)
            .max(),
        peak_delivery_retained_bytes: runs
            .iter()
            .filter_map(|run| run.peak_delivery_retained_bytes)
            .max(),
        peak_history_delivery_retained_bytes: runs
            .iter()
            .filter_map(|run| run.peak_history_delivery_retained_bytes)
            .max(),
        peak_pending_delta_bytes: runs
            .iter()
            .filter_map(|run| run.peak_pending_delta_bytes)
            .max(),
        peak_processor_artifact_bytes: runs
            .iter()
            .filter_map(|run| run.peak_processor_artifact_bytes)
            .max(),
        peak_pending_processor_artifact_bytes: runs
            .iter()
            .filter_map(|run| run.peak_pending_processor_artifact_bytes)
            .max(),
        peak_active_material_acquisitions: runs
            .iter()
            .filter_map(|run| run.peak_active_material_acquisitions)
            .max(),
        peak_material_buffered_bytes: runs
            .iter()
            .filter_map(|run| run.peak_material_buffered_bytes)
            .max(),
    }
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let index = sorted.len().saturating_mul(percentile).saturating_add(99) / 100;
    sorted[index.saturating_sub(1).min(sorted.len().saturating_sub(1))]
}

fn source_budget(blocks: u64) -> SourceBudget {
    SourceBudget {
        max_input_bytes: 64 * 1_024 * 1_024 * 1_024,
        max_frame_bytes: 16 * 1_024 * 1_024,
        max_frames: blocks,
        max_buffered_frames: 64,
        max_in_flight_requests: 8,
        temporary_disk_bytes: 0,
    }
}

fn benchmark_runtime_config(options: &BenchmarkOptions) -> HistoricalRuntimeConfig {
    HistoricalRuntimeConfig {
        mapper_concurrency: options.mapper_concurrency,
        maximum_active_chunks: options.maximum_active_chunks,
        maximum_mapped_bytes: options.maximum_mapped_bytes,
        commit_maximum_blocks: options.commit_maximum_blocks,
        commit_maximum_changes: options.commit_maximum_changes,
        commit_maximum_encoded_bytes: options.commit_maximum_encoded_bytes,
        commit_maximum_delay: Duration::from_millis(options.commit_maximum_delay_ms),
        commit_target_writer_hold: Duration::from_millis(options.commit_target_writer_hold_ms),
        ..HistoricalRuntimeConfig::default()
    }
}

fn benchmark_pipeline(
    options: &BenchmarkOptions,
) -> Result<(HistoricalPipelineBudget, HistoricalMaterialCoordinator)> {
    let pipeline = HistoricalPipelineBudget::new(
        options.maximum_active_chunks,
        options.mapper_concurrency,
        options.maximum_mapped_bytes,
    )?;
    let coordinator = HistoricalMaterialCoordinator::new_with_pipeline_budget(
        HistoricalMaterialCoordinatorConfig::default(),
        &pipeline,
    )?;
    Ok((pipeline, coordinator))
}

const fn benchmark_compression_name(compression: BenchmarkCompression) -> &'static str {
    match compression {
        BenchmarkCompression::None => "none",
        BenchmarkCompression::Gzip => "gzip",
    }
}

const fn artifact_compression(compression: BenchmarkArtifactCompression) -> ArtifactCompression {
    match compression {
        BenchmarkArtifactCompression::None => ArtifactCompression::None,
        BenchmarkArtifactCompression::Snappy => ArtifactCompression::Snappy,
        BenchmarkArtifactCompression::Deflate => ArtifactCompression::Deflate,
    }
}

const fn artifact_compression_name(compression: BenchmarkArtifactCompression) -> &'static str {
    artifact_compression_name_from_store(artifact_compression(compression))
}

const fn artifact_compression_name_from_store(compression: ArtifactCompression) -> &'static str {
    match compression {
        ArtifactCompression::None => "none",
        ArtifactCompression::Snappy => "snappy",
        ArtifactCompression::Deflate => "deflate",
    }
}

const fn benchmark_baseline_compression(compression: BenchmarkCompression) -> &'static str {
    match compression {
        BenchmarkCompression::None => "none",
        BenchmarkCompression::Gzip => "gzip_content_negotiated_with_record_flush",
    }
}

const fn api_compression(compression: BenchmarkCompression) -> DeliveryCompression {
    match compression {
        BenchmarkCompression::None => DeliveryCompression::None,
        BenchmarkCompression::Gzip => DeliveryCompression::Gzip,
    }
}

const fn store_compression(compression: BenchmarkCompression) -> BackfillDeliveryCompression {
    match compression {
        BenchmarkCompression::None => BackfillDeliveryCompression::None,
        BenchmarkCompression::Gzip => BackfillDeliveryCompression::Gzip,
    }
}

fn api_delivery_batch_limits(options: &BenchmarkOptions) -> DeliveryBatchLimits {
    DeliveryBatchLimits {
        target_encoded_bytes: options.delivery_target_encoded_bytes,
        maximum_encoded_bytes: options.delivery_maximum_encoded_bytes,
        maximum_events: options.delivery_maximum_events,
        maximum_processed_blocks: options.delivery_maximum_processed_blocks,
        maximum_delay: Duration::from_millis(options.delivery_maximum_delay_ms),
        maximum_buffered_batches: options.delivery_maximum_buffered_batches,
        maximum_buffered_bytes: options.delivery_maximum_buffered_bytes,
        compression: api_compression(options.delivery_compression),
    }
}

fn api_effective_batching(options: &BenchmarkOptions) -> EffectiveBackfillBatching {
    EffectiveBackfillBatching {
        target_encoded_bytes: options.delivery_target_encoded_bytes,
        maximum_encoded_bytes: options.delivery_maximum_encoded_bytes,
        maximum_events: options.delivery_maximum_events,
        maximum_processed_blocks: options.delivery_maximum_processed_blocks,
        maximum_delay_ms: options.delivery_maximum_delay_ms,
        maximum_buffered_batches: u64::try_from(options.delivery_maximum_buffered_batches)
            .unwrap_or(u64::MAX),
        maximum_buffered_bytes: options.delivery_maximum_buffered_bytes,
        compression: api_compression(options.delivery_compression),
    }
}

fn store_delivery_batch_limits(options: &BenchmarkOptions) -> BackfillDeliveryBatchLimits {
    BackfillDeliveryBatchLimits {
        target_encoded_bytes: options.delivery_target_encoded_bytes,
        maximum_encoded_bytes: options.delivery_maximum_encoded_bytes,
        maximum_events: options.delivery_maximum_events,
        maximum_processed_blocks: options.delivery_maximum_processed_blocks,
        maximum_delay_ms: options.delivery_maximum_delay_ms,
        maximum_buffered_batches: u64::try_from(options.delivery_maximum_buffered_batches)
            .unwrap_or(u64::MAX),
        maximum_buffered_bytes: options.delivery_maximum_buffered_bytes,
        compression: store_compression(options.delivery_compression),
    }
}

fn validate_options(options: &BenchmarkOptions) -> Result<()> {
    validate_profile_stage(options.mode, options.profile)?;
    if options.postgres_schema == BenchmarkPostgresSchema::BlobsApplication
        && (options.destination != BenchmarkDestination::SdkPostgres
            || options.corpus != BenchmarkCorpus::BlobsLike)
    {
        bail!(
            "the blobs-application PostgreSQL schema requires --destination sdk-postgres and --corpus blobs-like"
        );
    }
    if options.destination != BenchmarkDestination::SdkPostgres
        && options.postgres_schema != BenchmarkPostgresSchema::GenericEventLog
    {
        bail!("a PostgreSQL schema can only be selected with --destination sdk-postgres");
    }
    if options.blocks == 0 || options.blocks > 10_000_000 {
        bail!("benchmark blocks must be within 1..=10000000");
    }
    if options.chunk_blocks == 0 || options.chunk_blocks > options.blocks {
        bail!("benchmark chunk blocks must be within 1..=blocks");
    }
    if !(1..=10_000).contains(&options.artifact_segment_blocks) {
        bail!("benchmark artifact segment blocks must be within 1..=10000");
    }
    if options.artifact_compaction_interval_ms == 0
        || options.artifact_compaction_interval_ms > 60_000
    {
        bail!("benchmark artifact compaction interval must be within 1..=60000 milliseconds");
    }
    if !(1..=1_000).contains(&options.artifact_compaction_maximum_segments_per_cycle) {
        bail!("benchmark artifact compaction segments per cycle must be within 1..=1000");
    }
    if options.runs == 0 || options.runs > 20 || options.warmups > 5 {
        bail!("benchmark requires 1..=20 measured runs and at most 5 warm-ups");
    }
    if !(50..=1_000).contains(&options.sample_interval_ms) {
        bail!("benchmark sample interval must be within 50..=1000 milliseconds");
    }
    if options.consumer_delay_ms > 60_000 {
        bail!("benchmark consumer delay must not exceed 60000 milliseconds");
    }
    if options.consumer_reconnect_every_batches > 0
        && (options.destination != BenchmarkDestination::SdkPostgres
            || !matches!(
                options.mode,
                BenchmarkMode::Deliver | BenchmarkMode::EndToEnd
            ))
    {
        bail!("benchmark consumer reconnect cadence requires deliver/end-to-end with sdk-postgres");
    }
    if options.consumer_drop_ack_response_once
        && (options.destination != BenchmarkDestination::SdkPostgres
            || !matches!(
                options.mode,
                BenchmarkMode::Deliver | BenchmarkMode::EndToEnd
            ))
    {
        bail!(
            "benchmark acknowledgement response loss requires deliver/end-to-end with sdk-postgres"
        );
    }
    if options.concurrent_live_blocks > 0
        && (options.destination != BenchmarkDestination::SdkPostgres
            || options.mode != BenchmarkMode::EndToEnd)
    {
        bail!("concurrent live traffic requires end-to-end mode with the sdk-postgres destination");
    }
    if options.concurrent_live_blocks > 100_000 {
        bail!("benchmark concurrent live blocks must not exceed 100000");
    }
    if options.live_block_interval_ms > 60_000 {
        bail!("benchmark live block interval must not exceed 60000 milliseconds");
    }
    if options.mapper_concurrency == 0 || options.mapper_concurrency > 64 {
        bail!("benchmark mapper concurrency must be within 1..=64");
    }
    if options.maximum_active_chunks == 0
        || options.maximum_active_chunks > u32::MAX as usize
        || options.maximum_mapped_bytes == 0
        || options.maximum_mapped_bytes > u64::from(u32::MAX)
    {
        bail!("benchmark history pipeline limits are invalid");
    }
    validate_batch_options(options)?;
    if options.report.is_some()
        && options.samples_report.is_some()
        && options.report == options.samples_report
    {
        bail!("benchmark report and samples report must use different paths");
    }
    Ok(())
}

fn validate_profile_stage(mode: BenchmarkMode, profile: BenchmarkProductProfile) -> Result<()> {
    let profile_matches_stage = matches!(
        (mode, profile),
        (
            BenchmarkMode::Acquire,
            BenchmarkProductProfile::AcquireDiscard | BenchmarkProductProfile::RawOnly
        ) | (
            BenchmarkMode::Process,
            BenchmarkProductProfile::Materialized
                | BenchmarkProductProfile::CompactArtifact
                | BenchmarkProductProfile::CompactArtifactSegment
                | BenchmarkProductProfile::CompactArtifactTiered
                | BenchmarkProductProfile::RawArtifact
                | BenchmarkProductProfile::RawMaterialized
                | BenchmarkProductProfile::RawArtifactMaterialized
        ) | (
            BenchmarkMode::Deliver | BenchmarkMode::EndToEnd,
            BenchmarkProductProfile::Externalized | BenchmarkProductProfile::RawExternalized
        )
    );
    if !profile_matches_stage {
        bail!(
            "benchmark stage {} is incompatible with product profile {}; use acquire/acquire-discard|raw-only, process/materialized|compact-artifact|compact-artifact-segment|compact-artifact-tiered|raw-artifact|raw-materialized|raw-artifact-materialized, or deliver|end-to-end/externalized|raw-externalized",
            mode_name(mode),
            profile_name(profile)
        );
    }
    Ok(())
}

fn validate_batch_options(options: &BenchmarkOptions) -> Result<()> {
    if options.commit_maximum_blocks == 0
        || options.commit_maximum_changes == 0
        || options.commit_maximum_encoded_bytes == 0
        || options.commit_maximum_delay_ms == 0
        || options.commit_target_writer_hold_ms == 0
    {
        bail!("benchmark commit limits must be greater than zero");
    }
    if options.delivery_target_encoded_bytes == 0
        || options.delivery_target_encoded_bytes > options.delivery_maximum_encoded_bytes
        || options.delivery_maximum_events == 0
        || options.delivery_maximum_processed_blocks == 0
        || options.delivery_maximum_delay_ms == 0
        || options.delivery_maximum_buffered_batches == 0
        || options.delivery_maximum_buffered_bytes < options.delivery_maximum_encoded_bytes
    {
        bail!("benchmark delivery limits are invalid");
    }
    Ok(())
}

const fn corpus_kind(corpus: BenchmarkCorpus) -> SyntheticCorpusKind {
    match corpus {
        BenchmarkCorpus::Zero => SyntheticCorpusKind::Zero,
        BenchmarkCorpus::Sparse => SyntheticCorpusKind::Sparse,
        BenchmarkCorpus::BlobsLike => SyntheticCorpusKind::BlobsLike,
        BenchmarkCorpus::UniswapLike => SyntheticCorpusKind::UniswapLike,
        BenchmarkCorpus::Dense => SyntheticCorpusKind::Dense,
    }
}

const fn mode_name(mode: BenchmarkMode) -> &'static str {
    match mode {
        BenchmarkMode::Acquire => "acquire",
        BenchmarkMode::Process => "process",
        BenchmarkMode::Deliver => "deliver",
        BenchmarkMode::EndToEnd => "end_to_end",
    }
}

const fn profile_name(profile: BenchmarkProductProfile) -> &'static str {
    match profile {
        BenchmarkProductProfile::AcquireDiscard => "acquire_discard",
        BenchmarkProductProfile::RawOnly => "raw_only",
        BenchmarkProductProfile::Materialized => "materialized",
        BenchmarkProductProfile::CompactArtifact => "compact_artifact",
        BenchmarkProductProfile::CompactArtifactSegment => "compact_artifact_segment",
        BenchmarkProductProfile::CompactArtifactTiered => "compact_artifact_tiered",
        BenchmarkProductProfile::RawArtifact => "raw_artifact",
        BenchmarkProductProfile::RawMaterialized => "raw_materialized",
        BenchmarkProductProfile::RawArtifactMaterialized => "raw_artifact_materialized",
        BenchmarkProductProfile::Externalized => "externalized",
        BenchmarkProductProfile::RawExternalized => "raw_externalized",
    }
}

const fn product_profile_manifest(
    profile: BenchmarkProductProfile,
) -> BenchmarkProductProfileManifest {
    match profile {
        BenchmarkProductProfile::AcquireDiscard => BenchmarkProductProfileManifest {
            name: "acquire_discard",
            raw_history: "none",
            processor_artifacts: "none",
            materialized_output: "none",
            delivery: "none",
        },
        BenchmarkProductProfile::RawOnly => BenchmarkProductProfileManifest {
            name: "raw_only",
            raw_history: "full_processor_reuse",
            processor_artifacts: "none",
            materialized_output: "none",
            delivery: "none",
        },
        BenchmarkProductProfile::Materialized => BenchmarkProductProfileManifest {
            name: "materialized",
            raw_history: "none",
            processor_artifacts: "none",
            materialized_output: "full",
            delivery: "none",
        },
        BenchmarkProductProfile::CompactArtifact => BenchmarkProductProfileManifest {
            name: "compact_artifact",
            raw_history: "none",
            processor_artifacts: "full_sqlite_kv",
            materialized_output: "none",
            delivery: "none",
        },
        BenchmarkProductProfile::CompactArtifactSegment => BenchmarkProductProfileManifest {
            name: "compact_artifact_segment",
            raw_history: "none",
            processor_artifacts: "full_immutable_segments_per_commit",
            materialized_output: "none",
            delivery: "none",
        },
        BenchmarkProductProfile::CompactArtifactTiered => BenchmarkProductProfileManifest {
            name: "compact_artifact_tiered",
            raw_history: "none",
            processor_artifacts: "sqlite_write_buffer_concurrently_compacted_to_immutable_segments",
            materialized_output: "none",
            delivery: "none",
        },
        BenchmarkProductProfile::RawArtifact => BenchmarkProductProfileManifest {
            name: "raw_artifact",
            raw_history: "full_processor_reuse_then_local_replay",
            processor_artifacts: "full_sqlite_kv",
            materialized_output: "none",
            delivery: "none",
        },
        BenchmarkProductProfile::RawMaterialized => BenchmarkProductProfileManifest {
            name: "raw_materialized",
            raw_history: "full_processor_reuse_then_local_replay",
            processor_artifacts: "none",
            materialized_output: "full",
            delivery: "none",
        },
        BenchmarkProductProfile::RawArtifactMaterialized => BenchmarkProductProfileManifest {
            name: "raw_artifact_materialized",
            raw_history: "full_processor_reuse_then_local_replay",
            processor_artifacts: "full_sqlite_kv",
            materialized_output: "full",
            delivery: "none",
        },
        BenchmarkProductProfile::Externalized => BenchmarkProductProfileManifest {
            name: "externalized",
            raw_history: "none",
            processor_artifacts: "none",
            materialized_output: "none",
            delivery: "until_acknowledged",
        },
        BenchmarkProductProfile::RawExternalized => BenchmarkProductProfileManifest {
            name: "raw_externalized",
            raw_history: "full_processor_reuse_then_local_replay",
            processor_artifacts: "none",
            materialized_output: "none",
            delivery: "until_acknowledged",
        },
    }
}

const fn destination_name(destination: BenchmarkDestination) -> &'static str {
    match destination {
        BenchmarkDestination::RustHttp => "rust_http",
        BenchmarkDestination::RustDirect => "rust_direct",
        BenchmarkDestination::SdkPostgres => "sdk_postgres",
    }
}

const fn postgres_schema_name(schema: BenchmarkPostgresSchema) -> &'static str {
    match schema {
        BenchmarkPostgresSchema::GenericEventLog => "generic_event_log",
        BenchmarkPostgresSchema::BlobsApplication => "blobs_application",
    }
}

const fn postgres_contract(schema: BenchmarkPostgresSchema) -> (&'static str, &'static str) {
    match schema {
        BenchmarkPostgresSchema::GenericEventLog => {
            (GENERIC_EVENT_LOG_SCHEMA, ONE_DOMAIN_EVENT_PER_ROW)
        }
        BenchmarkPostgresSchema::BlobsApplication => {
            (BLOBS_APPLICATION_SCHEMA, BLOBS_APPLICATION_ROW_SEMANTICS)
        }
    }
}

const fn expected_postgres_rows(
    schema: BenchmarkPostgresSchema,
    manifest: &SyntheticCorpusManifest,
) -> u64 {
    match schema {
        BenchmarkPostgresSchema::GenericEventLog => manifest.expected_processor_events,
        BenchmarkPostgresSchema::BlobsApplication => manifest
            .expected_frames
            .saturating_add(manifest.expected_blob_transactions),
    }
}

fn expected_delivery_digest(
    manifest: &SyntheticCorpusManifest,
    destination: BenchmarkDestination,
) -> String {
    match destination {
        BenchmarkDestination::RustHttp | BenchmarkDestination::RustDirect => {
            manifest.expected_processor_output_digest.clone()
        }
        BenchmarkDestination::SdkPostgres => manifest.expected_processor_output_sha256.clone(),
    }
}

fn benchmark_processor_identity(
    kind: SyntheticCorpusKind,
    profile: BenchmarkProductProfile,
) -> Result<BenchmarkProcessorIdentity> {
    let processor = benchmark_processor(kind, profile)?;
    let descriptor = processor.descriptor();
    let descriptor_hash = blake3::hash(&serde_json::to_vec(descriptor)?).to_string();
    let schema_hash = blake3::hash(&serde_json::to_vec(&descriptor.schemas)?).to_string();
    Ok(BenchmarkProcessorIdentity {
        instance: descriptor.instance.to_string(),
        descriptor_hash,
        schema_hash,
    })
}

fn benchmark_processor(
    kind: SyntheticCorpusKind,
    profile: BenchmarkProductProfile,
) -> Result<Arc<dyn Processor>> {
    match kind {
        SyntheticCorpusKind::BlobsLike => {
            let processor = BlobsProcessor::new(BlobSchedule {
                chain_id: 1,
                network: "synthetic-mainnet".to_owned(),
                forks: vec![BlobFork {
                    name: "synthetic-dencun".to_owned(),
                    activation_block: 1,
                    activation_timestamp: 1,
                    fork_id: "00000001".to_owned(),
                    target_blobs_per_block: 6,
                    max_blobs_per_block: 32,
                    base_fee_update_fraction: 3_338_477,
                    eip7918: false,
                }],
            })?;
            let descriptor = processor.descriptor().clone();
            Ok(Arc::new(processor.with_contract(
                descriptor.instance.clone(),
                PublicationPolicy::FinalizedOnly,
                benchmark_profile_lifecycle(&descriptor.lifecycle, profile),
            )))
        }
        SyntheticCorpusKind::UniswapLike => {
            let processor = UniswapObservationsProcessor::new(UniswapConfig {
                start_block: leani_primitives::BlockNumber(1),
                pools: vec![PoolConfig {
                    address: uniswap_weth_usdc_pool(),
                    kind: PoolKind::V3,
                }],
            })?;
            let descriptor = processor.descriptor().clone();
            Ok(Arc::new(processor.with_contract(
                descriptor.instance.clone(),
                PublicationPolicy::FinalizedOnly,
                benchmark_profile_lifecycle(&descriptor.lifecycle, profile),
            )))
        }
        SyntheticCorpusKind::Zero | SyntheticCorpusKind::Sparse | SyntheticCorpusKind::Dense => {
            let processor = BlockLocalCounter::default();
            let lifecycle = benchmark_profile_lifecycle(&processor.descriptor().lifecycle, profile);
            let processor = processor.with_lifecycle(lifecycle);
            let processor = if matches!(
                profile,
                BenchmarkProductProfile::Externalized | BenchmarkProductProfile::RawExternalized
            ) {
                processor.with_split_delivery()
            } else {
                processor
            };
            Ok(Arc::new(processor))
        }
    }
}

fn benchmark_profile_lifecycle(
    base: &LifecyclePolicies,
    profile: BenchmarkProductProfile,
) -> LifecyclePolicies {
    let mut lifecycle = base.clone();
    match profile {
        BenchmarkProductProfile::AcquireDiscard | BenchmarkProductProfile::RawOnly => {
            lifecycle.artifacts.mode = ArtifactPolicyMode::None;
            lifecycle.artifacts.window = None;
            lifecycle.output.mode = OutputPolicyMode::None;
            lifecycle.delivery.mode = DeliveryPolicyMode::None;
            lifecycle.delivery.consumers.clear();
        }
        BenchmarkProductProfile::Materialized | BenchmarkProductProfile::RawMaterialized => {
            lifecycle.artifacts.mode = ArtifactPolicyMode::None;
            lifecycle.artifacts.window = None;
            lifecycle.output.mode = OutputPolicyMode::Full;
            lifecycle.delivery.mode = DeliveryPolicyMode::None;
            lifecycle.delivery.consumers.clear();
        }
        BenchmarkProductProfile::CompactArtifact
        | BenchmarkProductProfile::CompactArtifactSegment
        | BenchmarkProductProfile::CompactArtifactTiered
        | BenchmarkProductProfile::RawArtifact => {
            lifecycle.artifacts.mode = ArtifactPolicyMode::Full;
            lifecycle.artifacts.window = None;
            lifecycle.output.mode = OutputPolicyMode::None;
            lifecycle.delivery.mode = DeliveryPolicyMode::None;
            lifecycle.delivery.consumers.clear();
        }
        BenchmarkProductProfile::RawArtifactMaterialized => {
            lifecycle.artifacts.mode = ArtifactPolicyMode::Full;
            lifecycle.artifacts.window = None;
            lifecycle.output.mode = OutputPolicyMode::Full;
            lifecycle.delivery.mode = DeliveryPolicyMode::None;
            lifecycle.delivery.consumers.clear();
        }
        BenchmarkProductProfile::Externalized | BenchmarkProductProfile::RawExternalized => {
            lifecycle.artifacts.mode = ArtifactPolicyMode::None;
            lifecycle.artifacts.window = None;
            lifecycle.output.mode = OutputPolicyMode::None;
            lifecycle.delivery.mode = DeliveryPolicyMode::UntilAcknowledged;
            lifecycle.delivery.consumers = vec![DurableConsumerPolicy {
                id: "benchmark-destination".to_owned(),
                required: true,
                lease_ttl_seconds: 300,
            }];
            lifecycle.delivery.pruning.retain_finalized_blocks = 0;
            lifecycle.delivery.pruning.retain_acknowledged_seconds = 0;
            lifecycle.delivery.pruning.minimum_batch_blocks = 1;
            lifecycle.delivery.pruning.minimum_batch_changes = 1;
        }
    }
    lifecycle
}

const fn profile_retains_raw(profile: BenchmarkProductProfile) -> bool {
    matches!(
        profile,
        BenchmarkProductProfile::RawArtifact
            | BenchmarkProductProfile::RawOnly
            | BenchmarkProductProfile::RawMaterialized
            | BenchmarkProductProfile::RawArtifactMaterialized
            | BenchmarkProductProfile::RawExternalized
    )
}

fn benchmark_output_collection(kind: SyntheticCorpusKind) -> &'static str {
    match kind {
        SyntheticCorpusKind::BlobsLike => {
            unreachable!("blobs materialized output uses reconstructed snapshots")
        }
        SyntheticCorpusKind::UniswapLike => UNISWAP_HISTORY_COLLECTION,
        SyntheticCorpusKind::Zero | SyntheticCorpusKind::Sparse | SyntheticCorpusKind::Dense => {
            "counter.blocks"
        }
    }
}

const fn benchmark_domain_kind(kind: SyntheticCorpusKind) -> &'static str {
    match kind {
        SyntheticCorpusKind::BlobsLike => "blobs.block",
        SyntheticCorpusKind::UniswapLike => "uniswap.price.observation",
        SyntheticCorpusKind::Zero | SyntheticCorpusKind::Sparse | SyntheticCorpusKind::Dense => {
            "synthetic.counter"
        }
    }
}

fn canonical_change_event(kind: &str, key: &[u8], payload: &[u8]) -> Result<Vec<u8>> {
    match kind {
        "synthetic.counter" => {
            let mut event = Vec::with_capacity(1 + key.len() + payload.len());
            event.push(0);
            event.extend_from_slice(key);
            event.extend_from_slice(payload);
            Ok(event)
        }
        "blobs.block" => {
            let delta: BlobsDelta =
                postcard::from_bytes(payload).context("decode benchmark blobs delivery payload")?;
            Ok(canonical_blobs_event(
                key,
                &delta.block,
                &delta.transactions,
            ))
        }
        "uniswap.price.observation" => {
            let entity: PoolPriceEntity = postcard::from_bytes(payload)
                .context("decode benchmark Uniswap delivery payload")?;
            let mut event = Vec::with_capacity(1 + key.len() + 32);
            event.push(2);
            event.extend_from_slice(key);
            event.extend_from_slice(
                &entity
                    .sqrt_price_x96
                    .context("benchmark V3 observation has no square-root price")?
                    .0,
            );
            Ok(event)
        }
        other => bail!("unexpected benchmark domain change {other}"),
    }
}

fn canonical_blobs_event(
    key: &[u8],
    block: &BlobsBlockEntity,
    transactions: &[BlobTransactionEntity],
) -> Vec<u8> {
    let mut event = Vec::new();
    event.push(1);
    event.extend_from_slice(key);
    event.extend_from_slice(&block.block_hash.0);
    event.extend_from_slice(
        &u32::try_from(transactions.len())
            .unwrap_or(u32::MAX)
            .to_be_bytes(),
    );
    for transaction in transactions {
        event.extend_from_slice(&transaction.transaction_hash.0);
        event.extend_from_slice(&transaction.blob_count.to_be_bytes());
        for hash in &transaction.blob_versioned_hashes {
            event.extend_from_slice(&hash.0);
        }
    }
    event
}

fn update_length_prefixed(hasher: &mut blake3::Hasher, bytes: &[u8]) {
    hasher.update(&u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_be_bytes());
    hasher.update(bytes);
}

fn elapsed_milliseconds(started: Instant) -> u64 {
    duration_milliseconds(started.elapsed())
}

fn duration_milliseconds(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn now_milliseconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn git_output(arguments: &[&str]) -> Option<String> {
    let output = Command::new("git").args(arguments).output().ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn process_rss_bytes() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let status = std::fs::read_to_string("/proc/self/status").ok()?;
        let kib = status
            .lines()
            .find_map(|line| line.strip_prefix("VmRSS:"))?
            .split_whitespace()
            .next()?
            .parse::<u64>()
            .ok()?;
        return kib.checked_mul(1_024);
    }
    #[cfg(target_os = "macos")]
    {
        let output = Command::new("ps")
            .args(["-o", "rss=", "-p", &std::process::id().to_string()])
            .output()
            .ok()?;
        let kib = String::from_utf8_lossy(&output.stdout)
            .trim()
            .parse::<u64>()
            .ok()?;
        return kib.checked_mul(1_024);
    }
    #[allow(unreachable_code)]
    None
}

fn write_immutable(path: &Path, encoded: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .with_context(|| format!("create immutable benchmark report {}", path.display()))?;
    file.write_all(encoded)
        .with_context(|| format!("write benchmark report {}", path.display()))?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    Ok(())
}

fn write_sample_report(
    path: &Path,
    runs: &[BenchmarkRunReport],
) -> Result<BenchmarkSampleReportIdentity> {
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .with_context(|| {
            format!(
                "create immutable benchmark samples report {}",
                path.display()
            )
        })?;
    let mut writer = BufWriter::new(file);
    let mut hasher = blake3::Hasher::new();
    let mut records = 0_u64;
    for run in runs {
        for sample in &run.samples {
            let encoded = serde_json::to_vec(&BenchmarkSampleRecord {
                report_version: REPORT_VERSION,
                report_schema: REPORT_SCHEMA,
                iteration: run.iteration,
                mode: run.mode,
                profile: run.profile,
                sample,
            })?;
            writer.write_all(&encoded)?;
            writer.write_all(b"\n")?;
            hasher.update(&encoded);
            hasher.update(b"\n");
            records = records.saturating_add(1);
        }
    }
    writer.flush()?;
    writer.get_ref().sync_all()?;
    Ok(BenchmarkSampleReportIdentity {
        path: path.display().to_string(),
        records,
        blake3: hasher.finalize().to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn real_source_metrics_are_reported_as_per_run_deltas() {
        let before = SourceAcquisitionMetrics {
            opened_chunks: 2,
            opened_ranges: vec![BlockRange::single(BlockNumber(1))],
            acquired_frames: 20,
            normalized_bytes: 200,
            logical_range_requests: Some(3),
            physical_reads: Some(4),
            fetched_bytes: Some(400),
            source_objects: None,
            source_object_bytes: None,
            projected_compressed_bytes: Some(300),
            rows_scanned: Some(1_000),
            rows_selected: Some(50),
            operation_elapsed_ms: 25,
        };
        let after = SourceAcquisitionMetrics {
            opened_chunks: 5,
            opened_ranges: vec![
                BlockRange::single(BlockNumber(1)),
                BlockRange::single(BlockNumber(2)),
                BlockRange::single(BlockNumber(3)),
            ],
            acquired_frames: 50,
            normalized_bytes: 800,
            logical_range_requests: Some(8),
            physical_reads: Some(10),
            fetched_bytes: Some(1_000),
            source_objects: Some(3),
            source_object_bytes: Some(2_000),
            projected_compressed_bytes: Some(750),
            rows_scanned: Some(2_500),
            rows_selected: Some(125),
            operation_elapsed_ms: 80,
        };
        let delta = acquisition_metrics_delta(after, Some(&before));
        assert_eq!(delta.opened_chunks, 3);
        assert_eq!(
            delta.opened_ranges,
            vec![
                BlockRange::single(BlockNumber(2)),
                BlockRange::single(BlockNumber(3)),
            ]
        );
        assert_eq!(delta.acquired_frames, 30);
        assert_eq!(delta.normalized_bytes, 600);
        assert_eq!(delta.logical_range_requests, Some(5));
        assert_eq!(delta.physical_reads, Some(6));
        assert_eq!(delta.fetched_bytes, Some(600));
        assert_eq!(delta.source_objects, Some(3));
        assert_eq!(delta.projected_compressed_bytes, Some(450));
        assert_eq!(delta.rows_scanned, Some(1_500));
        assert_eq!(delta.rows_selected, Some(75));
        assert_eq!(delta.operation_elapsed_ms, 55);
    }

    #[test]
    fn real_source_processor_is_externalized_without_changing_identity() {
        let configured: Arc<dyn Processor> = Arc::new(BlobsProcessor::default());
        let expected_instance = configured.descriptor().instance.clone();
        let processor =
            externalized_real_source_processor(configured.as_ref()).expect("externalized");
        assert_eq!(processor.descriptor().instance, expected_instance);
        assert_eq!(
            processor.descriptor().lifecycle.output.mode,
            OutputPolicyMode::None
        );
        assert_eq!(
            processor.descriptor().lifecycle.delivery.mode,
            DeliveryPolicyMode::UntilAcknowledged
        );
    }

    #[test]
    fn gzip_http_consumer_releases_decoded_output_incrementally() {
        let payload = (0..4 * 1_024 * 1_024)
            .map(|offset| u8::try_from(offset % 251).expect("bounded byte"))
            .collect::<Vec<_>>();
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&payload).expect("compress fixture");
        let compressed = encoder.finish().expect("finish fixture compression");

        let mut decoder = flate2::write::GzDecoder::new(Vec::new());
        let mut output = Vec::new();
        let mut decoded_bytes = 0_usize;
        for chunk in compressed.chunks(37) {
            decoder.write_all(chunk).expect("decompress fixture chunk");
            decoder.flush().expect("flush fixture decoder");
            drain_gzip_output(&mut decoder, &mut output);
            decoded_bytes = decoded_bytes.saturating_add(output.len());
            output.clear();
            assert!(decoder.get_ref().is_empty());
        }
        decoder.try_finish().expect("finish fixture decoder");
        drain_gzip_output(&mut decoder, &mut output);
        decoded_bytes = decoded_bytes.saturating_add(output.len());

        assert_eq!(decoded_bytes, payload.len());
        assert!(decoder.get_ref().is_empty());
    }

    fn options(mode: BenchmarkMode, corpus: BenchmarkCorpus) -> BenchmarkOptions {
        BenchmarkOptions {
            mode,
            profile: match mode {
                BenchmarkMode::Acquire => BenchmarkProductProfile::AcquireDiscard,
                BenchmarkMode::Process => BenchmarkProductProfile::Materialized,
                BenchmarkMode::Deliver | BenchmarkMode::EndToEnd => {
                    BenchmarkProductProfile::Externalized
                }
            },
            destination: BenchmarkDestination::RustDirect,
            postgres_schema: BenchmarkPostgresSchema::GenericEventLog,
            corpus,
            blocks: 256,
            seed: 7,
            chunk_blocks: 64,
            artifact_segment_blocks: 64,
            artifact_segment_compression: BenchmarkArtifactCompression::Snappy,
            artifact_compaction_interval_ms: 10,
            artifact_compaction_maximum_segments_per_cycle: 2,
            warmups: 0,
            runs: 1,
            sample_interval_ms: 50,
            consumer_delay_ms: 0,
            consumer_reconnect_every_batches: 0,
            consumer_drop_ack_response_once: false,
            concurrent_live_blocks: 0,
            live_block_interval_ms: 10,
            mapper_concurrency: 4,
            maximum_active_chunks: 4,
            maximum_mapped_bytes: 128 * 1024 * 1024,
            commit_maximum_blocks: 128,
            commit_maximum_changes: 10_000,
            commit_maximum_encoded_bytes: 16 * 1024 * 1024,
            commit_maximum_delay_ms: 50,
            commit_target_writer_hold_ms: 20,
            delivery_target_encoded_bytes: 4 * 1024 * 1024,
            delivery_maximum_encoded_bytes: 16 * 1024 * 1024,
            delivery_maximum_events: 20_000,
            delivery_maximum_processed_blocks: 8_192,
            delivery_maximum_delay_ms: 50,
            delivery_maximum_buffered_batches: 4,
            delivery_maximum_buffered_bytes: 64 * 1024 * 1024,
            delivery_compression: BenchmarkCompression::Gzip,
            report: None,
            samples_report: None,
        }
    }

    fn sweep_evaluation(
        id: &str,
        throughput: u64,
        rss: u64,
        physical: u64,
        delivery: u64,
        eligible: bool,
    ) -> BenchmarkSweepEvaluation {
        BenchmarkSweepEvaluation {
            id: id.to_owned(),
            candidate_identity: "00".repeat(32),
            report: format!("{id}.json"),
            report_blake3: "00".repeat(32),
            source: "executed",
            correctness_passed: eligible,
            measured_runs: 3,
            coefficient_of_variation: 0.01,
            median_blocks_per_second_milli: throughput,
            peak_rss_bytes: Some(rss),
            peak_physical_store_bytes: Some(physical),
            peak_delivery_retained_bytes: Some(delivery),
            peak_live_p95_commit_latency_us: None,
            eligible,
            rejection_reasons: Vec::new(),
            pareto: false,
        }
    }

    async fn materialized_digest(
        kind: SyntheticCorpusKind,
        blocks: u64,
        commit_blocks: usize,
    ) -> (QueryMeasurement, SyntheticCorpusManifest) {
        let (source, manifest) = GeneratedHistorySource::new(kind, blocks, 7, 37).expect("source");
        let source = Arc::new(source);
        let processor =
            benchmark_processor(kind, BenchmarkProductProfile::Materialized).expect("processor");
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteStore::open(StoreConfig::new(directory.path().join("parity.sqlite")))
            .await
            .expect("store");
        let runtime = HistoricalRuntime::new(
            store.clone(),
            source,
            processor.clone(),
            HistoricalRuntimeConfig {
                commit_maximum_blocks: commit_blocks,
                commit_maximum_delay: Duration::from_mins(1),
                ..HistoricalRuntimeConfig::default()
            },
        )
        .expect("runtime");
        let job = BackfillJob::for_processor(
            format!("parity-{commit_blocks}"),
            processor.as_ref(),
            ChainId(1),
            manifest.range,
            VerificationPolicy::CompleteCryptographic,
        )
        .expect("job");
        runtime
            .run(job, source_budget(blocks), CancellationToken::new())
            .await
            .expect("materialize");
        let query = query_processor_output(
            &store,
            processor.as_ref(),
            kind,
            manifest.expected_processor_events,
        )
        .await
        .expect("query");
        store.verify().await.expect("verify");
        (query, manifest)
    }

    async fn assert_raw_product_profile(
        corpus: BenchmarkCorpus,
        kind: SyntheticCorpusKind,
        profile: BenchmarkProductProfile,
        retains_artifacts: bool,
        retains_materialized: bool,
    ) {
        let mut raw_options = options(BenchmarkMode::Process, corpus);
        raw_options.profile = profile;
        let run = Box::pin(run_once(&raw_options, kind, 1))
            .await
            .expect("production-shape retained-history replay benchmark");
        assert!(run.correctness_passed, "{corpus:?} {profile:?}");
        assert_eq!(run.source.frames, raw_options.blocks);
        assert_eq!(run.storage.logical.raw_history.rows, raw_options.blocks);
        assert_eq!(
            run.storage.logical.processor_artifacts.rows,
            if retains_artifacts {
                raw_options.blocks
            } else {
                0
            }
        );
        assert_eq!(
            run.storage.logical.materialized_entities.rows > 0,
            retains_materialized
        );
        assert!(run.storage.physical.raw_history_segment_bytes > 0);
        assert!(run.storage.physical.raw_history_catalog_bytes > 0);
        let replay = run.raw_history.expect("raw-history measurement");
        assert_eq!(replay.committed_blocks, raw_options.blocks);
        assert_eq!(replay.external_frames_after_acquisition, raw_options.blocks);
        assert_eq!(replay.external_frames_after_replay, raw_options.blocks);
        assert_eq!(replay.retained_record_reads, raw_options.blocks);
    }

    async fn assert_raw_only_profile() {
        let mut raw_options = options(BenchmarkMode::Acquire, BenchmarkCorpus::Sparse);
        raw_options.profile = BenchmarkProductProfile::RawOnly;
        let raw = Box::pin(run_once(&raw_options, SyntheticCorpusKind::Sparse, 1))
            .await
            .expect("raw-only acquisition");
        assert!(raw.correctness_passed);
        assert_eq!(raw.source.frames, raw_options.blocks);
        assert_eq!(
            raw.storage.canonical_logical_output_definition,
            "sum_uncompressed_retained_raw_frame_bytes_v1"
        );
        assert_eq!(raw.storage.logical.raw_history.rows, raw_options.blocks);
        assert_eq!(raw.storage.logical.processor_artifacts.rows, 0);
        assert_eq!(raw.storage.logical.materialized_entities.rows, 0);
        assert!(raw.storage.physical.raw_history_segment_bytes > 0);
        let retained = raw.raw_history.expect("raw-only measurement");
        assert_eq!(retained.retained_read_purpose, "frame_digest_verification");
        assert_eq!(retained.retained_record_reads, raw_options.blocks);
    }

    fn assert_direct_segment_count_is_bounded(segments: u64, benchmark_options: &BenchmarkOptions) {
        // The direct sink publishes one immutable segment per adaptive commit
        // microbatch. Its boundaries intentionally follow runtime backpressure,
        // not the offline compactor's `artifact_segment_blocks` target. A run
        // therefore has at least one segment per source chunk and, if adaptive
        // commits shrink to one block, at most one segment per block.
        let maximum_blocks_per_segment = benchmark_options
            .chunk_blocks
            .min(u64::try_from(benchmark_options.commit_maximum_blocks).unwrap_or(u64::MAX))
            .max(1);
        let minimum = benchmark_options
            .blocks
            .div_ceil(maximum_blocks_per_segment);
        assert!(
            segments >= minimum,
            "direct artifact sink wrote {segments} segments, below the {minimum} source/commit boundary minimum"
        );
        assert!(
            segments <= benchmark_options.blocks,
            "direct artifact sink wrote {segments} segments for only {} blocks",
            benchmark_options.blocks
        );
    }

    #[tokio::test]
    async fn acquire_and_process_modes_match_the_same_manifest() {
        let acquire = Box::pin(run_once(
            &options(BenchmarkMode::Acquire, BenchmarkCorpus::Sparse),
            SyntheticCorpusKind::Sparse,
            1,
        ))
        .await
        .expect("acquire");
        let process = Box::pin(run_once(
            &options(BenchmarkMode::Process, BenchmarkCorpus::Sparse),
            SyntheticCorpusKind::Sparse,
            1,
        ))
        .await
        .expect("process");
        assert!(acquire.correctness_passed);
        assert!(acquire.time_to_first_source_frame_ms.is_some());
        assert_eq!(acquire.storage.physical.total_retained_bytes, 0);
        assert_eq!(acquire.storage.storage_amplification_milli, Some(0));

        assert_raw_only_profile().await;

        assert!(process.correctness_passed);
        assert!(process.time_to_first_source_frame_ms.is_some());
        assert!(process.storage.canonical_logical_output_bytes > 0);
        assert!(process.storage.logical.materialized_entities.rows > 0);
        assert!(process.storage.physical.total_retained_bytes > 0);
        assert!(process.storage.storage_amplification_milli.is_some());
        assert_eq!(process.store.expect("store").changes, 0);

        let mut artifact_options = options(BenchmarkMode::Process, BenchmarkCorpus::Sparse);
        artifact_options.profile = BenchmarkProductProfile::CompactArtifact;
        let artifacts = Box::pin(run_once(&artifact_options, SyntheticCorpusKind::Sparse, 1))
            .await
            .expect("compact artifacts");
        assert!(artifacts.correctness_passed);
        assert_eq!(
            artifacts.storage.logical.processor_artifacts.rows,
            artifact_options.blocks
        );
        assert_eq!(artifacts.storage.logical.materialized_entities.rows, 0);
        assert_eq!(artifacts.storage.logical.materialized_indexes.rows, 0);
        assert!(artifacts.storage.physical.total_retained_bytes > 0);
        let segment = artifacts
            .artifact_segment_candidate
            .expect("artifact segment candidate");
        assert!(segment.correctness_passed);
        assert_eq!(segment.artifacts, artifact_options.blocks);
        assert_eq!(segment.segments, 4);
        assert_eq!(segment.input_digest, segment.verified_digest);
        assert!(segment.logical_bytes > 0);
        assert!(segment.physical_bytes > 0);

        artifact_options.profile = BenchmarkProductProfile::CompactArtifactSegment;
        let segment_artifacts =
            Box::pin(run_once(&artifact_options, SyntheticCorpusKind::Sparse, 1))
                .await
                .expect("direct segment artifacts");
        assert!(segment_artifacts.correctness_passed);
        let direct = segment_artifacts
            .artifact_segment_store
            .expect("direct artifact segment store");
        assert_eq!(direct.artifacts, artifact_options.blocks);
        assert_direct_segment_count_is_bounded(direct.segments, &artifact_options);
        assert_eq!(
            segment_artifacts.storage.logical.processor_artifacts.rows,
            artifact_options.blocks
        );
        assert!(
            segment_artifacts
                .storage
                .physical
                .node_non_sqlite_segment_bytes
                > 0
        );

        artifact_options.profile = BenchmarkProductProfile::CompactArtifactTiered;
        let tiered_artifacts =
            Box::pin(run_once(&artifact_options, SyntheticCorpusKind::Sparse, 1))
                .await
                .expect("tiered segment artifacts");
        assert!(tiered_artifacts.correctness_passed);
        let tiered = tiered_artifacts
            .artifact_segment_store
            .expect("tiered artifact segment store");
        assert_eq!(tiered.artifacts, artifact_options.blocks);
        assert_eq!(tiered.segments, 4);
        assert_eq!(
            tiered_artifacts.storage.logical.processor_artifacts.rows,
            artifact_options.blocks
        );
        assert_eq!(
            segment_artifacts
                .processor_store
                .expect("SQLite processor stats")
                .processor_artifacts,
            0
        );
        assert!(segment_artifacts.artifact_segment_candidate.is_none());
    }

    #[test]
    fn product_profile_owns_lifecycle_and_is_validated_against_stage() {
        let materialized = benchmark_processor(
            SyntheticCorpusKind::BlobsLike,
            BenchmarkProductProfile::Materialized,
        )
        .expect("materialized processor");
        assert_eq!(
            materialized.descriptor().lifecycle.output.mode,
            OutputPolicyMode::Full
        );
        assert_eq!(
            materialized.descriptor().lifecycle.delivery.mode,
            DeliveryPolicyMode::None
        );
        assert_eq!(
            materialized.descriptor().lifecycle.artifacts.mode,
            ArtifactPolicyMode::None
        );

        let compact = benchmark_processor(
            SyntheticCorpusKind::BlobsLike,
            BenchmarkProductProfile::CompactArtifact,
        )
        .expect("compact artifact processor");
        assert_eq!(
            compact.descriptor().lifecycle.output.mode,
            OutputPolicyMode::None
        );
        assert_eq!(
            compact.descriptor().lifecycle.delivery.mode,
            DeliveryPolicyMode::None
        );
        assert_eq!(
            compact.descriptor().lifecycle.artifacts.mode,
            ArtifactPolicyMode::Full
        );
        validate_profile_stage(
            BenchmarkMode::Process,
            BenchmarkProductProfile::CompactArtifact,
        )
        .expect("compact artifacts are a process-stage profile");

        let segment = benchmark_processor(
            SyntheticCorpusKind::BlobsLike,
            BenchmarkProductProfile::CompactArtifactSegment,
        )
        .expect("segment artifact processor");
        assert_eq!(
            segment.descriptor().lifecycle,
            compact.descriptor().lifecycle
        );
        validate_profile_stage(
            BenchmarkMode::Process,
            BenchmarkProductProfile::CompactArtifactSegment,
        )
        .expect("segment artifacts are a process-stage profile");

        let externalized = benchmark_processor(
            SyntheticCorpusKind::BlobsLike,
            BenchmarkProductProfile::Externalized,
        )
        .expect("externalized processor");
        assert_eq!(
            externalized.descriptor().lifecycle.output.mode,
            OutputPolicyMode::None
        );
        assert_eq!(
            externalized.descriptor().lifecycle.delivery.mode,
            DeliveryPolicyMode::UntilAcknowledged
        );
        assert_eq!(
            externalized.descriptor().lifecycle.delivery.consumers,
            vec![DurableConsumerPolicy {
                id: "benchmark-destination".to_owned(),
                required: true,
                lease_ttl_seconds: 300,
            }]
        );

        let mut mismatched = options(BenchmarkMode::Process, BenchmarkCorpus::BlobsLike);
        mismatched.profile = BenchmarkProductProfile::Externalized;
        assert!(
            validate_options(&mismatched)
                .expect_err("stage/profile mismatch must fail")
                .to_string()
                .contains("incompatible with product profile")
        );
    }

    #[test]
    fn sweep_manifest_validation_and_pareto_frontier_are_deterministic() {
        let manifest: BenchmarkSweepManifest = serde_json::from_value(serde_json::json!({
            "schema": SWEEP_SCHEMA,
            "orderSeed": 7,
            "baseArguments": [
                "--mode", "process", "--profile", "materialized", "--blocks", "64"
            ],
            "candidates": [
                { "id": "chunks-1", "arguments": ["--maximum-active-chunks", "1"] },
                { "id": "chunks-4", "arguments": ["--maximum-active-chunks", "4"] }
            ],
            "gates": {
                "maximumCoefficientOfVariation": 0.05,
                "maximumLiveP95CommitLatencyUs": 50000
            }
        }))
        .expect("manifest");
        validate_sweep_manifest(&manifest).expect("valid sweep manifest");
        assert_eq!(
            manifest.gates.maximum_live_p95_commit_latency_us,
            Some(50_000)
        );
        assert_eq!(
            sweep_order_key(7, "chunks-1"),
            sweep_order_key(7, "chunks-1")
        );
        assert_ne!(
            sweep_order_key(7, "chunks-1"),
            sweep_order_key(8, "chunks-1")
        );
        let first_identity = sweep_candidate_identity(
            "binary-a",
            &manifest.base_arguments,
            &manifest.candidates[0],
        );
        let mut changed_candidate = manifest.candidates[0].clone();
        changed_candidate.arguments[1] = "2".to_owned();
        assert_ne!(
            first_identity,
            sweep_candidate_identity("binary-a", &manifest.base_arguments, &changed_candidate)
        );
        assert_ne!(
            first_identity,
            sweep_candidate_identity(
                "binary-b",
                &manifest.base_arguments,
                &manifest.candidates[0],
            )
        );

        let mut candidates = vec![
            sweep_evaluation("fast", 120, 120, 100, 80, true),
            sweep_evaluation("lean", 100, 80, 80, 60, true),
            sweep_evaluation("dominated", 90, 100, 100, 80, true),
            sweep_evaluation("failed-gate", 200, 10, 10, 10, false),
        ];
        mark_pareto_frontier(&mut candidates);
        assert!(candidates[0].pareto);
        assert!(candidates[1].pareto);
        assert!(!candidates[2].pareto);
        assert!(!candidates[3].pareto);

        let mut invalid = manifest;
        invalid.candidates[0].arguments.push("--report".to_owned());
        assert!(validate_sweep_manifest(&invalid).is_err());

        assert!(optional_latency_no_worse(Some(10), Some(20)));
        assert!(optional_latency_better(Some(10), Some(20)));
        assert!(!optional_latency_no_worse(Some(10), None));
        assert!(!optional_latency_better(None, None));
    }

    #[test]
    fn consumer_fault_injection_is_scoped_to_sdk_delivery_modes() {
        let mut invalid = options(BenchmarkMode::Process, BenchmarkCorpus::Sparse);
        invalid.consumer_drop_ack_response_once = true;
        assert!(
            validate_options(&invalid)
                .expect_err("process mode cannot inject acknowledgement loss")
                .to_string()
                .contains("acknowledgement response loss")
        );

        let mut valid = options(BenchmarkMode::EndToEnd, BenchmarkCorpus::Sparse);
        valid.destination = BenchmarkDestination::SdkPostgres;
        valid.consumer_drop_ack_response_once = true;
        valid.consumer_reconnect_every_batches = 2;
        validate_options(&valid).expect("SDK destination accepts delivery fault injection");

        let mut invalid_live = options(BenchmarkMode::Deliver, BenchmarkCorpus::Sparse);
        invalid_live.destination = BenchmarkDestination::SdkPostgres;
        invalid_live.concurrent_live_blocks = 10;
        assert!(
            validate_options(&invalid_live)
                .expect_err("isolated delivery cannot produce concurrent live blocks")
                .to_string()
                .contains("concurrent live traffic")
        );

        valid.concurrent_live_blocks = 10;
        valid.live_block_interval_ms = 1;
        validate_options(&valid).expect("SDK end-to-end accepts concurrent live traffic");
    }

    #[test]
    fn blobs_application_destination_is_explicit_and_has_independent_rows() {
        let (_, manifest) = GeneratedHistorySource::new(SyntheticCorpusKind::BlobsLike, 256, 7, 64)
            .expect("blobs manifest");
        assert_eq!(
            expected_postgres_rows(BenchmarkPostgresSchema::GenericEventLog, &manifest),
            manifest.expected_processor_events
        );
        assert_eq!(
            expected_postgres_rows(BenchmarkPostgresSchema::BlobsApplication, &manifest),
            manifest
                .expected_frames
                .saturating_add(manifest.expected_blob_transactions)
        );

        let mut valid = options(BenchmarkMode::EndToEnd, BenchmarkCorpus::BlobsLike);
        valid.destination = BenchmarkDestination::SdkPostgres;
        valid.postgres_schema = BenchmarkPostgresSchema::BlobsApplication;
        validate_options(&valid).expect("blobs application destination");

        valid.corpus = BenchmarkCorpus::UniswapLike;
        assert!(
            validate_options(&valid)
                .expect_err("Uniswap cannot use the blobs application schema")
                .to_string()
                .contains("blobs-application PostgreSQL schema")
        );
    }

    #[test]
    fn concurrent_live_manifest_starts_after_the_history_range() {
        let mut options = options(BenchmarkMode::EndToEnd, BenchmarkCorpus::UniswapLike);
        options.concurrent_live_blocks = 20;
        let manifest = concurrent_live_manifest(&options, SyntheticCorpusKind::UniswapLike)
            .expect("live manifest")
            .expect("configured live manifest");
        assert_eq!(manifest.range.start(), BlockNumber(257));
        assert_eq!(manifest.range.end(), BlockNumber(276));
        assert_eq!(manifest.expected_frames, 20);
        assert!(manifest.expected_processor_events > 0);
    }

    #[test]
    fn sweep_rejects_candidates_that_exceed_the_live_latency_gate() {
        let directory = tempfile::tempdir().expect("sweep report directory");
        let report = directory.path().join("candidate.json");
        std::fs::write(
            &report,
            serde_json::to_vec(&serde_json::json!({
                "reportSchema": REPORT_SCHEMA,
                "summary": {
                    "coefficientOfVariation": 0.01,
                    "medianBlocksPerSecondMilli": 1000,
                    "peakRssBytes": 100,
                    "peakPhysicalStoreBytes": 100,
                    "peakDeliveryRetainedBytes": 100
                },
                "runs": [{
                    "correctnessPassed": true,
                    "delivery": { "concurrentLive": { "p95CommitLatencyUs": 200 } }
                }]
            }))
            .expect("encode candidate"),
        )
        .expect("write candidate");
        let evaluation = evaluate_sweep_candidate(
            "slow-live",
            "candidate",
            &report,
            false,
            BenchmarkSweepGates {
                minimum_measured_runs: 1,
                maximum_live_p95_commit_latency_us: Some(100),
                ..BenchmarkSweepGates::default()
            },
        )
        .expect("evaluate candidate");
        assert_eq!(evaluation.peak_live_p95_commit_latency_us, Some(200));
        assert!(!evaluation.eligible);
        assert!(
            evaluation
                .rejection_reasons
                .contains(&"live_p95_commit_latency".to_owned())
        );
    }

    #[tokio::test]
    async fn summary_omits_samples_and_optional_ndjson_is_content_addressed() {
        let run = Box::pin(run_once(
            &options(BenchmarkMode::Acquire, BenchmarkCorpus::Sparse),
            SyntheticCorpusKind::Sparse,
            1,
        ))
        .await
        .expect("acquire");
        assert!(!run.samples.is_empty());
        let summary = serde_json::to_value(&run).expect("serialize summary run");
        assert!(summary.get("samples").is_none());

        let directory = tempfile::tempdir().expect("temporary samples directory");
        let path = directory.path().join("samples.ndjson");
        let identity =
            write_sample_report(&path, std::slice::from_ref(&run)).expect("write sample report");
        let bytes = std::fs::read(&path).expect("read sample report");
        assert_eq!(identity.records, run.samples.len() as u64);
        assert_eq!(identity.blake3, blake3::hash(&bytes).to_string());
        for line in bytes
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
        {
            let record: serde_json::Value =
                serde_json::from_slice(line).expect("valid sample record");
            assert_eq!(record["reportVersion"], REPORT_VERSION);
            assert_eq!(record["reportSchema"], REPORT_SCHEMA);
            assert_eq!(record["iteration"], 1);
            assert_eq!(record["mode"], "acquire");
            assert_eq!(record["profile"], "acquire_discard");
            assert!(record.get("sample").is_some());
        }
    }

    #[tokio::test]
    async fn delivery_and_end_to_end_acknowledge_complete_output() {
        for mode in [BenchmarkMode::Deliver, BenchmarkMode::EndToEnd] {
            let run = Box::pin(run_once(
                &options(mode, BenchmarkCorpus::BlobsLike),
                SyntheticCorpusKind::BlobsLike,
                1,
            ))
            .await
            .expect("delivery benchmark");
            assert!(run.correctness_passed);
            let delivery = run.delivery.expect("delivery measurement");
            assert_eq!(delivery.domain_events, 256);
            assert_eq!(
                delivery.raw_payload_bytes,
                run.storage.canonical_logical_output_bytes
            );
            assert_eq!(delivery.completion_records, 1);
            assert!(delivery.acknowledgements > 0);
            let consumer_complete = delivery
                .consumer_complete_milliseconds
                .expect("consumer completion measurement");
            assert!(consumer_complete <= run.elapsed_milliseconds);
            assert!(
                delivery
                    .consumer_blocks_per_second_milli
                    .is_some_and(|rate| rate > 0)
            );
            assert!(
                delivery
                    .domain_events_per_second_milli
                    .is_some_and(|rate| rate > 0)
            );
            assert!(
                delivery
                    .raw_payload_bytes_per_second
                    .is_some_and(|rate| rate > 0)
            );
            assert!(
                delivery
                    .transmitted_bytes_per_second
                    .is_some_and(|rate| rate > 0)
            );
            match mode {
                BenchmarkMode::Deliver => {
                    assert!(delivery.producer_complete_milliseconds.is_none());
                    assert!(delivery.producer_blocks_per_second_milli.is_none());
                    assert!(delivery.post_producer_drain_milliseconds.is_none());
                }
                BenchmarkMode::EndToEnd => {
                    let producer_complete = delivery
                        .producer_complete_milliseconds
                        .expect("producer completion measurement");
                    assert!(producer_complete <= run.elapsed_milliseconds);
                    assert!(
                        delivery
                            .producer_blocks_per_second_milli
                            .is_some_and(|rate| rate > 0)
                    );
                    // Completion is visible to the consumer before the producer
                    // finishes saving its checkpoint, so either future can
                    // finish first. Drain is zero if the consumer finishes first.
                    assert_eq!(
                        delivery.post_producer_drain_milliseconds,
                        Some(consumer_complete.saturating_sub(producer_complete))
                    );
                }
                BenchmarkMode::Acquire | BenchmarkMode::Process => unreachable!(),
            }
        }

        let mut raw_options = options(BenchmarkMode::EndToEnd, BenchmarkCorpus::BlobsLike);
        raw_options.profile = BenchmarkProductProfile::RawExternalized;
        let raw = Box::pin(run_once(&raw_options, SyntheticCorpusKind::BlobsLike, 1))
            .await
            .expect("raw-externalized benchmark");
        assert!(raw.correctness_passed);
        assert_eq!(raw.source.frames, raw_options.blocks);
        assert_eq!(raw.storage.logical.raw_history.rows, raw_options.blocks);
        assert_eq!(raw.storage.logical.processor_artifacts.rows, 0);
        assert_eq!(raw.storage.logical.materialized_entities.rows, 0);
        assert!(raw.storage.physical.raw_history_segment_bytes > 0);
        let retained = raw.raw_history.expect("raw-externalized measurement");
        assert_eq!(retained.retained_read_purpose, "processor_delivery");
        assert_eq!(
            retained.external_frames_after_acquisition,
            raw_options.blocks
        );
        assert_eq!(retained.external_frames_after_replay, raw_options.blocks);
        assert_eq!(retained.retained_record_reads, raw_options.blocks);
    }

    #[tokio::test]
    async fn production_processor_shapes_match_independent_manifests() {
        for corpus in [BenchmarkCorpus::BlobsLike, BenchmarkCorpus::UniswapLike] {
            let kind = corpus_kind(corpus);
            for mode in [BenchmarkMode::Process, BenchmarkMode::EndToEnd] {
                let run = Box::pin(run_once(&options(mode, corpus), kind, 1))
                    .await
                    .expect("production-shape benchmark");
                assert!(run.correctness_passed, "{corpus:?} {mode:?}");
            }
            let mut artifact_options = options(BenchmarkMode::Process, corpus);
            artifact_options.profile = BenchmarkProductProfile::CompactArtifact;
            let artifacts = Box::pin(run_once(&artifact_options, kind, 1))
                .await
                .expect("production-shape compact-artifact benchmark");
            assert!(artifacts.correctness_passed, "{corpus:?} compact artifact");
            assert_eq!(
                artifacts.storage.logical.processor_artifacts.rows,
                artifact_options.blocks
            );
            artifact_options.profile = BenchmarkProductProfile::CompactArtifactSegment;
            let segments = Box::pin(run_once(&artifact_options, kind, 1))
                .await
                .expect("production-shape direct-segment benchmark");
            assert!(segments.correctness_passed, "{corpus:?} direct segment");
            assert_eq!(
                segments.storage.logical.processor_artifacts.rows,
                artifact_options.blocks
            );
            assert!(segments.artifact_segment_store.is_some());
            artifact_options.profile = BenchmarkProductProfile::CompactArtifactTiered;
            let tiered = Box::pin(run_once(&artifact_options, kind, 1))
                .await
                .expect("production-shape tiered-segment benchmark");
            assert!(tiered.correctness_passed, "{corpus:?} tiered segment");
            assert_eq!(
                tiered.storage.logical.processor_artifacts.rows,
                artifact_options.blocks
            );
            assert!(tiered.artifact_segment_store.is_some());

            assert_raw_product_profile(
                corpus,
                kind,
                BenchmarkProductProfile::RawArtifact,
                true,
                false,
            )
            .await;
            assert_raw_product_profile(
                corpus,
                kind,
                BenchmarkProductProfile::RawMaterialized,
                false,
                true,
            )
            .await;
            assert_raw_product_profile(
                corpus,
                kind,
                BenchmarkProductProfile::RawArtifactMaterialized,
                true,
                true,
            )
            .await;
        }
    }

    #[tokio::test]
    async fn real_http_delivery_matches_both_production_processor_shapes() {
        for corpus in [BenchmarkCorpus::BlobsLike, BenchmarkCorpus::UniswapLike] {
            let mut options = options(BenchmarkMode::EndToEnd, corpus);
            options.destination = BenchmarkDestination::RustHttp;
            options.blocks = 64;
            options.chunk_blocks = 16;
            let run = Box::pin(run_once(&options, corpus_kind(corpus), 1))
                .await
                .expect("HTTP production-shape benchmark");
            assert!(run.correctness_passed, "{corpus:?}");
            let delivery = run.delivery.as_ref().expect("delivery");
            assert!(delivery.producer_complete_milliseconds.is_some());
            assert!(delivery.consumer_complete_milliseconds.is_some());
            assert!(
                delivery
                    .consumer_blocks_per_second_milli
                    .is_some_and(|rate| rate > 0)
            );
            assert!(
                delivery
                    .transmitted_bytes_per_second
                    .is_some_and(|rate| rate > 0)
            );
            assert!(
                run.time_to_first_source_frame_ms <= delivery.time_to_first_batch_ms,
                "source material must exist before the first delivered batch"
            );
        }
    }

    #[tokio::test]
    async fn one_block_and_microbatch_materialization_are_byte_identical() {
        for kind in [
            SyntheticCorpusKind::BlobsLike,
            SyntheticCorpusKind::UniswapLike,
        ] {
            let (isolated, manifest) = materialized_digest(kind, 257, 1).await;
            let (microbatched, _) = materialized_digest(kind, 257, 73).await;
            assert_eq!(
                isolated.row_count.observed_rows,
                manifest.expected_processor_events
            );
            assert!(isolated.row_count.correctness_passed);
            assert_eq!(
                isolated.canonical_output_bytes,
                manifest.expected_canonical_processor_output_bytes
            );
            assert_eq!(isolated.digest, manifest.expected_query_output_digest);
            assert_eq!(microbatched.digest, isolated.digest);
            assert_eq!(microbatched.payload_bytes, isolated.payload_bytes);
        }
    }
}
