//! Command-line contract.

use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};

/// Low-storage, source-interchangeable Ethereum indexing node.
#[derive(Clone, Debug, Parser)]
#[command(name = "leani", version, propagate_version = true)]
pub struct Cli {
    /// Path to the versioned TOML configuration.
    ///
    /// When omitted, Leani uses `./leani.toml`. `LEANI_CONFIG` provides a
    /// persistent override without repeating this flag. For `init`, this
    /// selects the new file instead.
    #[arg(long, global = true, env = "LEANI_CONFIG")]
    pub config: Option<PathBuf>,

    /// Log rendering format.
    #[arg(long, global = true, value_enum, default_value_t = LogFormat::Pretty)]
    pub log_format: LogFormat,

    /// Tracing filter, e.g. `info,leani=debug`.
    #[arg(long, global = true, env = "LEANI_LOG")]
    pub log_filter: Option<String>,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Clone, Debug, Subcommand)]
pub enum Command {
    /// Validate and describe configuration without mutating state.
    Doctor {
        /// Emit machine-readable JSON.
        #[arg(long)]
        json: bool,
    },
    /// Start configured sources, processors, stores, and serving endpoints.
    Serve,
    /// Create a compact configuration for a built-in processor.
    Init {
        /// Processor preset to configure.
        #[arg(value_enum)]
        protocol: SubscribeProtocol,
        /// Optional protocol targets; Uniswap requires one or more markets such as `ETH/USDC`.
        #[arg(num_args = 0.., value_name = "TARGET")]
        targets: Vec<String>,
        /// Data directory written to the generated configuration.
        #[arg(long, default_value = "./data")]
        data_dir: PathBuf,
        /// Checkpoint provider used for weak-subjectivity quorum (repeatable or comma-delimited).
        #[arg(
            long,
            value_delimiter = ',',
            default_values = [
                "https://ethereum-beacon-api.publicnode.com/",
                "https://mainnet.checkpoint.sigp.io/",
                "https://beaconstate-mainnet.chainsafe.io/"
            ],
            env = "LEANI_CHECKPOINT_URLS"
        )]
        checkpoint_url: Vec<url::Url>,
        /// Number of independent checkpoint providers that must agree.
        #[arg(long, default_value_t = 2)]
        checkpoint_quorum: usize,
        /// Accept a new checkpoint provider quorum without interactive confirmation.
        #[arg(long)]
        yes: bool,
    },
    /// Stream live processor output from an embedded runtime or a running node.
    Subscribe {
        /// Built-in subscription feed to activate.
        #[arg(value_enum)]
        protocol: SubscribeProtocol,
        /// Optional feed targets; Uniswap requires one or more markets such as `ETH/USDC`.
        #[arg(num_args = 0.., value_name = "TARGET")]
        targets: Vec<String>,
        /// Rendering contract for stdout.
        #[arg(long, value_enum, default_value_t = SubscribeFormat::Pretty)]
        format: SubscribeFormat,
        /// Where the subscription processor should run.
        #[arg(long, value_enum, default_value_t = SubscribeMode::Auto)]
        mode: SubscribeMode,
        /// Base URL of an already running Leani node.
        #[arg(long)]
        endpoint: Option<url::Url>,
        /// Processor instance or unambiguous processor kind on a running node.
        #[arg(long)]
        processor: Option<String>,
        /// Optional bearer token for a running node.
        #[arg(long, env = "LEANI_API_TOKEN")]
        token: Option<String>,
        /// Publication finality.
        #[arg(long, value_enum, default_value_t = SubscribeFinality::Optimistic)]
        finality: SubscribeFinality,
        /// Finality transport for the embedded runtime.
        #[arg(long, value_enum, default_value_t = SubscribeFinalitySource::Auto)]
        finality_source: SubscribeFinalitySource,
        /// Checkpoint provider used for weak-subjectivity quorum (repeatable or comma-delimited).
        #[arg(
            long,
            value_delimiter = ',',
            default_values = [
                "https://ethereum-beacon-api.publicnode.com/",
                "https://mainnet.checkpoint.sigp.io/",
                "https://beaconstate-mainnet.chainsafe.io/"
            ],
            env = "LEANI_CHECKPOINT_URLS"
        )]
        checkpoint_url: Vec<url::Url>,
        /// Number of independent checkpoint providers that must agree.
        #[arg(long, default_value_t = 2)]
        checkpoint_quorum: usize,
        /// Accept a new checkpoint provider quorum without interactive confirmation.
        #[arg(long)]
        yes: bool,
        /// Persistent directory for the lightweight embedded runtime.
        #[arg(long)]
        data_dir: Option<PathBuf>,
        /// Exit after the first matching update.
        #[arg(long)]
        once: bool,
    },
    /// Reset reconstructible local runtime state for cold-start testing.
    Reset {
        #[command(subcommand)]
        command: ResetCommand,
    },
    /// Process a historical block range and then exit.
    Backfill {
        /// Configured processor ID.
        #[arg(long, default_value = "blobs-money")]
        processor: String,
        #[arg(long)]
        from: u64,
        #[arg(long)]
        to: u64,
    },
    /// Inspect or probe configured data sources.
    Source {
        #[command(subcommand)]
        command: SourceCommand,
    },
    /// Compare normalized source exports or render exact RPC response vectors.
    Conformance {
        #[command(subcommand)]
        command: ConformanceCommand,
    },
    /// Run a reproducible historical throughput and storage measurement.
    Benchmark {
        /// Optional benchmark orchestration command.
        #[command(subcommand)]
        command: Option<Box<BenchmarkCommand>>,
        /// Isolated stage or full pipeline to measure.
        #[arg(long, value_enum, default_value_t = BenchmarkMode::Process)]
        mode: BenchmarkMode,
        /// Product storage/delivery policy measured by the selected stage.
        #[arg(long, value_enum)]
        profile: Option<BenchmarkProductProfile>,
        /// Destination path used by delivery modes.
        #[arg(long, value_enum, default_value_t = BenchmarkDestination::RustHttp)]
        destination: BenchmarkDestination,
        /// `PostgreSQL` schema exercised by the SDK destination.
        #[arg(long, value_enum, default_value_t = BenchmarkPostgresSchema::GenericEventLog)]
        postgres_schema: BenchmarkPostgresSchema,
        /// Deterministic material shape.
        #[arg(long, value_enum, default_value_t = BenchmarkCorpus::BlobsLike)]
        corpus: BenchmarkCorpus,
        /// Number of generated finalized blocks.
        #[arg(long, default_value_t = 10_000)]
        blocks: u64,
        /// Stable corpus seed recorded in the report.
        #[arg(long, default_value_t = 1)]
        seed: u64,
        /// Physical source chunk size in blocks.
        #[arg(long, default_value_t = 2_048)]
        chunk_blocks: u64,
        /// Blocks per immutable artifact segment.
        #[arg(long, default_value_t = 8_192)]
        artifact_segment_blocks: u64,
        /// Compression used by immutable artifact segments.
        #[arg(long, value_enum, default_value_t = BenchmarkArtifactCompression::Snappy)]
        artifact_segment_compression: BenchmarkArtifactCompression,
        /// Background artifact compaction cadence in milliseconds.
        #[arg(long, default_value_t = 100)]
        artifact_compaction_interval_ms: u64,
        /// Maximum artifact segments published per background cycle.
        #[arg(long, default_value_t = 4)]
        artifact_compaction_maximum_segments_per_cycle: usize,
        /// Unreported warm-up runs.
        #[arg(long, default_value_t = 1)]
        warmups: u32,
        /// Measured runs retained in the report.
        #[arg(long, default_value_t = 3)]
        runs: u32,
        /// Resource sample interval in milliseconds.
        #[arg(long, default_value_t = 200)]
        sample_interval_ms: u64,
        /// Optional fixed delay after each destination batch.
        #[arg(long, default_value_t = 0)]
        consumer_delay_ms: u64,
        /// Reopen the SDK/PostgreSQL stream after every N acknowledged batches (zero disables).
        #[arg(long, default_value_t = 0)]
        consumer_reconnect_every_batches: u64,
        /// Simulate losing the first successful SDK/PostgreSQL acknowledgement response.
        #[arg(long, default_value_t = false)]
        consumer_drop_ack_response_once: bool,
        /// Finalized live blocks committed while history is acquired and delivered.
        #[arg(long, default_value_t = 0)]
        concurrent_live_blocks: u64,
        /// Delay between synthetic live commits in milliseconds.
        #[arg(long, default_value_t = 10)]
        live_block_interval_ms: u64,
        /// Parallel processor map tasks.
        #[arg(long, default_value_t = 4)]
        mapper_concurrency: usize,
        /// Node-wide concurrently active physical source chunks.
        #[arg(long, default_value_t = 4)]
        maximum_active_chunks: usize,
        /// Node-wide mapped-delta byte budget.
        #[arg(long, default_value_t = 134_217_728)]
        maximum_mapped_bytes: u64,
        /// Maximum blocks per atomic `SQLite` history commit.
        #[arg(long, default_value_t = 128)]
        commit_maximum_blocks: usize,
        /// Maximum domain changes per atomic `SQLite` history commit.
        #[arg(long, default_value_t = 10_000)]
        commit_maximum_changes: usize,
        /// Maximum stable-encoded change bytes per history commit.
        #[arg(long, default_value_t = 16_777_216)]
        commit_maximum_encoded_bytes: u64,
        /// Flush delay for a partially filled history commit.
        #[arg(long, default_value_t = 50)]
        commit_maximum_delay_ms: u64,
        /// Target p95 `SQLite` writer hold time for adaptive history commits.
        #[arg(long, default_value_t = 20)]
        commit_target_writer_hold_ms: u64,
        /// Target uncompressed encoded bytes per delivered history batch.
        #[arg(long, default_value_t = 4_194_304)]
        delivery_target_encoded_bytes: u64,
        /// Hard maximum uncompressed encoded bytes per delivery batch.
        #[arg(long, default_value_t = 16_777_216)]
        delivery_maximum_encoded_bytes: u64,
        /// Hard maximum domain events per delivery batch.
        #[arg(long, default_value_t = 20_000)]
        delivery_maximum_events: u64,
        /// Hard maximum processed blocks represented by one delivery batch.
        #[arg(long, default_value_t = 8_192)]
        delivery_maximum_processed_blocks: u64,
        /// Flush delay for a partially filled delivery batch.
        #[arg(long, default_value_t = 50)]
        delivery_maximum_delay_ms: u64,
        /// Maximum encoded batches queued per connection.
        #[arg(long, default_value_t = 4)]
        delivery_maximum_buffered_batches: usize,
        /// Maximum encoded bytes queued per connection.
        #[arg(long, default_value_t = 67_108_864)]
        delivery_maximum_buffered_bytes: u64,
        /// Delivery transport compression.
        #[arg(long, value_enum, default_value_t = BenchmarkCompression::Gzip)]
        delivery_compression: BenchmarkCompression,
        /// Optional JSON report destination.
        #[arg(long)]
        report: Option<PathBuf>,
        /// Optional immutable NDJSON destination for high-frequency samples.
        #[arg(long)]
        samples_report: Option<PathBuf>,
    },
    /// Run explicit end-to-end verification gates.
    E2e {
        #[command(subcommand)]
        command: E2eCommand,
    },
    /// Inspect and maintain the durable store.
    Db {
        #[command(subcommand)]
        command: DbCommand,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum SubscribeProtocol {
    Blocks,
    UniswapV3,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum SubscribeFormat {
    Pretty,
    Json,
    Raw,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum SubscribeMode {
    /// Attach to a reachable node from the endpoint or local config, otherwise run embedded.
    Auto,
    /// Require an existing node and never fall back to an embedded runtime.
    Client,
    /// Always start the lightweight embedded runtime.
    Embedded,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum SubscribeFinality {
    Optimistic,
    Finalized,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum SubscribeFinalitySource {
    /// Reuse an explicit node profile, otherwise use verified Beacon API proofs.
    Auto,
    /// Fetch proof-carrying light-client updates over HTTP.
    BeaconApi,
    /// Fetch proof-carrying light-client updates from consensus P2P peers.
    P2p,
}

#[derive(Clone, Debug, Subcommand)]
pub enum ResetCommand {
    /// Delete all node and embedded-subscription state in the resolved data directory.
    All {
        /// Runtime data directory to reset instead of the configured/default directory.
        #[arg(long)]
        data_dir: Option<PathBuf>,
        /// Reset without interactive confirmation.
        #[arg(long)]
        yes: bool,
    },
    /// Delete reconstructible state for one embedded market subscription.
    Subscription {
        /// Built-in subscription feed whose embedded state should be reset.
        #[arg(value_enum)]
        protocol: SubscribeProtocol,
        /// The exact targets used by `leani subscribe`; empty for `blocks`.
        #[arg(num_args = 0.., value_name = "TARGET")]
        targets: Vec<String>,
        /// Price finality used by the subscription state being reset.
        #[arg(long, value_enum, default_value_t = SubscribeFinality::Optimistic)]
        finality: SubscribeFinality,
        /// Exact embedded subscription directory to reset.
        #[arg(long)]
        data_dir: Option<PathBuf>,
        /// Reset without interactive confirmation.
        #[arg(long)]
        yes: bool,
    },
}

/// Benchmark orchestration commands layered over individual benchmark runs.
#[derive(Clone, Debug, Subcommand)]
pub enum BenchmarkCommand {
    /// Run or resume a manifest-defined candidate sweep.
    Sweep {
        /// Versioned JSON sweep manifest.
        #[arg(long)]
        manifest: PathBuf,
        /// Directory containing immutable candidate reports and the sweep summary.
        #[arg(long)]
        output_directory: PathBuf,
    },
    /// Measure a pinned Mainnet range through real history sources, processor,
    /// HTTP delivery, acknowledgement, and pruning.
    RealSource {
        /// Configured processor kind or instance.
        #[arg(long, default_value = "blobs-money")]
        processor: String,
        /// Historical source selection policy.
        #[arg(long, value_enum, default_value_t = BenchmarkRealSourcePolicy::HistoryPortfolio)]
        source_policy: BenchmarkRealSourcePolicy,
        /// First finalized execution block in the inclusive range.
        #[arg(long)]
        from_block: u64,
        /// Last finalized execution block in the inclusive range.
        #[arg(long)]
        to_block: u64,
        /// Fresh benchmark directory. Existing node state is rejected.
        #[arg(long)]
        data_dir: PathBuf,
        /// Optional delay before acknowledging every delivered batch.
        #[arg(long, default_value_t = 0)]
        consumer_delay_ms: u64,
        /// Overall wall-clock limit for acquisition, processing, and delivery.
        #[arg(long, default_value_t = 7_200)]
        timeout_seconds: u64,
        /// Resource/progress sampling interval.
        #[arg(long, default_value_t = 250)]
        sample_interval_ms: u64,
        /// Override configured historical source acquisition concurrency.
        #[arg(long)]
        source_concurrency: Option<usize>,
        /// Override configured processor mapper concurrency.
        #[arg(long)]
        mapper_concurrency: Option<usize>,
        /// Override the shared historical coordinator active-chunk ceiling.
        #[arg(long)]
        maximum_active_chunks: Option<usize>,
        /// Optional expected BLAKE3 digest of canonical delivered domain events.
        #[arg(long)]
        expected_output_digest: Option<String>,
        /// Delivery transport compression. Batch sizing comes from node config.
        #[arg(long, value_enum, default_value_t = BenchmarkCompression::Gzip)]
        delivery_compression: BenchmarkCompression,
        /// Versioned JSON evidence report.
        #[arg(long)]
        report: PathBuf,
    },
}

/// Real historical-source policy used by a benchmark run.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum BenchmarkRealSourcePolicy {
    /// Use only configured Xatu history sources.
    XatuOnly,
    /// Use only configured eraE history sources.
    EraeOnly,
    /// Use only the consensus-anchored execution P2P historical fallback.
    P2pOnly,
    /// Use all compatible archive/dataset sources in production failover order.
    /// This fixed-range policy intentionally excludes the live/P2P lane.
    HistoryPortfolio,
}

/// Independently measurable benchmark stage.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum BenchmarkMode {
    Acquire,
    Process,
    Deliver,
    EndToEnd,
}

/// Explicit product policy exercised by a benchmark stage.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum BenchmarkProductProfile {
    /// Acquire and verify source material without retaining processor output.
    AcquireDiscard,
    /// Acquire and retain complete processor-reuse history without processing it.
    RawOnly,
    /// Retain full processor output in the node-owned query store.
    Materialized,
    /// Retain one compact, replayable map artifact per finalized block.
    CompactArtifact,
    /// Retain compact map artifacts directly in immutable segment files.
    CompactArtifactSegment,
    /// Buffer artifacts durably in `SQLite` and compact them into larger segments.
    CompactArtifactTiered,
    /// Retain raw processor-reuse history, then replay it into compact artifacts.
    RawArtifact,
    /// Retain raw processor-reuse history, then replay it into materialized output.
    RawMaterialized,
    /// Retain raw history plus both compact artifacts and materialized output.
    RawArtifactMaterialized,
    /// Deliver processor output to a consumer and retain none after acknowledgement.
    Externalized,
    /// Retain raw history while processor output is delivered and pruned after acknowledgement.
    RawExternalized,
}

/// Delivery destination used by the benchmark harness.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum BenchmarkDestination {
    /// Exercise the real HTTP protocol with a Rust discard sink.
    RustHttp,
    /// Bypass HTTP to establish the durable-store protocol ceiling.
    RustDirect,
    /// Exercise the TypeScript SDK and a `PostgreSQL` bulk destination.
    SdkPostgres,
}

/// Destination representation used by SDK/PostgreSQL benchmark runs.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum BenchmarkPostgresSchema {
    /// Stable protocol fixture with one opaque row per domain event.
    GenericEventLog,
    /// Application-shaped blobs block and transaction tables.
    BlobsApplication,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum BenchmarkCompression {
    None,
    Gzip,
}

/// Independently seekable artifact-record compression under comparison.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum BenchmarkArtifactCompression {
    None,
    Snappy,
    Deflate,
}

/// Deterministic synthetic corpus shape.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum BenchmarkCorpus {
    Zero,
    Sparse,
    BlobsLike,
    UniswapLike,
    Dense,
}

#[derive(Clone, Debug, Subcommand)]
pub enum E2eCommand {
    /// Run a deterministic bounded source/runtime/SQLite fixture with no network.
    Fixture {
        /// Fresh directory for the fixture database.
        #[arg(long)]
        data_dir: PathBuf,
        /// Number of deterministic finalized blocks.
        #[arg(long, default_value_t = 128)]
        blocks: u64,
        /// Optional JSON evidence report.
        #[arg(long)]
        report: Option<PathBuf>,
    },
    /// Backfill and follow Ethereum Mainnet using production source adapters.
    Mainnet {
        /// Configured processor to verify.
        #[arg(long, default_value = "blobs-money")]
        processor: String,
        /// First historical block in the required continuous coverage range.
        #[arg(long)]
        from_block: u64,
        /// Fresh data directory. Existing state requires `--resume`.
        #[arg(long)]
        data_dir: PathBuf,
        /// Reuse an existing E2E data directory after an interrupted run.
        #[arg(long)]
        resume: bool,
        /// Blocks that must be followed beyond the initial finalized anchor.
        #[arg(long, default_value_t = 16)]
        minimum_follow_blocks: u64,
        /// Seconds all convergence and head-freshness assertions must remain true.
        #[arg(long, default_value_t = 300)]
        stable_seconds: u64,
        /// Maximum permitted age of the latest retained execution block.
        #[arg(long, default_value_t = 60)]
        max_head_age_seconds: u64,
        /// Overall wall-clock limit for the staging run.
        #[arg(long, default_value_t = 7_200)]
        timeout_seconds: u64,
        /// JSON evidence report written on both success and failure.
        #[arg(long, default_value = "mainnet-e2e-report.json")]
        report: PathBuf,
    },
}

#[derive(Clone, Debug, Subcommand)]
pub enum ConformanceCommand {
    /// Compare canonical material in two normalized frame exports.
    Frames {
        #[arg(long)]
        left_source: String,
        #[arg(long)]
        left: PathBuf,
        #[arg(long)]
        right_source: String,
        #[arg(long)]
        right: PathBuf,
        /// Canonical material to compare (repeatable or comma-delimited).
        #[arg(long, value_enum, value_delimiter = ',', required = true)]
        capability: Vec<CompareCapability>,
        /// Optional JSON report destination.
        #[arg(long)]
        report: Option<PathBuf>,
    },
    /// Compare deterministic processor deltas from two source exports.
    Processor {
        /// Configured built-in processor ID.
        #[arg(long, default_value = "blobs-money")]
        processor: String,
        #[arg(long)]
        left_source: String,
        #[arg(long)]
        left: PathBuf,
        #[arg(long)]
        right_source: String,
        #[arg(long)]
        right: PathBuf,
        /// Optional JSON report destination.
        #[arg(long)]
        report: Option<PathBuf>,
    },
    /// Render exact Ethereum block/receipt responses from normalized frames.
    Rpc {
        /// Normalized frame export produced by a source probe.
        #[arg(long)]
        frames: PathBuf,
        /// Optional reference snapshot array from an execution client.
        #[arg(long)]
        expected: Option<PathBuf>,
        /// Optional destination for the reconstructed snapshots.
        #[arg(long)]
        output: Option<PathBuf>,
        /// Optional differential JSON report destination.
        #[arg(long)]
        report: Option<PathBuf>,
    },
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum CompareCapability {
    Header,
    Body,
    Transactions,
    Calldata,
    Receipts,
    Logs,
    Withdrawals,
    BlobSidecars,
    Traces,
    StateDiffs,
    ConsensusFinality,
}

impl CompareCapability {
    pub const fn into_primitive(self) -> leani_primitives::Capability {
        match self {
            Self::Header => leani_primitives::Capability::Header,
            Self::Body => leani_primitives::Capability::Body,
            Self::Transactions => leani_primitives::Capability::Transactions,
            Self::Calldata => leani_primitives::Capability::Calldata,
            Self::Receipts => leani_primitives::Capability::Receipts,
            Self::Logs => leani_primitives::Capability::Logs,
            Self::Withdrawals => leani_primitives::Capability::Withdrawals,
            Self::BlobSidecars => leani_primitives::Capability::BlobSidecars,
            Self::Traces => leani_primitives::Capability::Traces,
            Self::StateDiffs => leani_primitives::Capability::StateDiffs,
            Self::ConsensusFinality => leani_primitives::Capability::ConsensusFinality,
        }
    }
}

#[derive(Clone, Debug, Subcommand)]
pub enum SourceCommand {
    /// Probe a source without changing processor state.
    Probe {
        #[command(subcommand)]
        source: ProbeSource,
    },
}

#[derive(Clone, Debug, Subcommand)]
pub enum ProbeSource {
    /// Inspect the configured Xatu catalog.
    Xatu {
        /// Xatu network catalog.
        #[arg(long, default_value = "mainnet")]
        network: String,
        /// First execution block in the inclusive probe range.
        #[arg(long)]
        from_block: u64,
        /// Last execution block in the inclusive probe range.
        #[arg(long)]
        to_block: u64,
        /// Processor projection whose required tables should be inspected.
        #[arg(long, default_value = "blobs-money")]
        processor: String,
        /// First beacon date, required to inspect daily beacon tables.
        #[arg(long, requires = "to_date")]
        from_date: Option<leani_source_xatu::XatuDate>,
        /// Last beacon date, required to inspect daily beacon tables.
        #[arg(long, requires = "from_date")]
        to_date: Option<leani_source_xatu::XatuDate>,
        /// Maximum simultaneous footer requests.
        #[arg(long, default_value_t = 4)]
        concurrency: usize,
        /// Maximum projected compressed Parquet bytes.
        #[arg(long, default_value_t = 268_435_456)]
        max_input_bytes: u64,
        /// Arrow rows decoded in one bounded batch.
        #[arg(long, default_value_t = 8_192)]
        batch_rows: usize,
        /// Optional JSON report destination.
        #[arg(long)]
        report: Option<PathBuf>,
        /// Optional destination for normalized projected frames.
        #[arg(long, requires_all = ["from_date", "to_date"])]
        projection_output: Option<PathBuf>,
        /// Read-only blobs.money compatibility export to compare field by field.
        #[arg(long, requires_all = ["from_date", "to_date"])]
        expected_export: Option<PathBuf>,
    },
    /// Exercise the recent execution P2P boundary.
    P2p {
        /// First execution block in the inclusive probe range.
        #[arg(long)]
        from_block: u64,
        /// Last execution block in the inclusive probe range (at most 64 blocks).
        #[arg(long)]
        to_block: u64,
        /// Consensus-verified hash expected for `to-block`.
        #[arg(long)]
        expected_tip: Option<String>,
        /// Minimum connected peers before issuing requests.
        #[arg(long, default_value_t = 3)]
        minimum_peers: usize,
        /// Maximum time to wait for peers.
        #[arg(long, default_value_t = 300)]
        peer_wait_seconds: u64,
        /// Timeout applied independently to each request.
        #[arg(long, default_value_t = 30)]
        request_timeout_seconds: u64,
        /// Maximum attempts per header, body, or receipt request.
        #[arg(long, default_value_t = 5)]
        retries: usize,
        /// Pause between request attempts while the peer pool changes.
        #[arg(long, default_value_t = 1)]
        retry_backoff_seconds: u64,
        /// Maximum normalized bytes accepted for the complete range.
        #[arg(long, default_value_t = 268_435_456)]
        max_input_bytes: u64,
        /// Optional destination for a JSON summary.
        #[arg(long)]
        report: Option<PathBuf>,
        /// Optional destination for complete normalized frames.
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// Exercise sparse execution-history archive reads.
    Erae {
        /// First execution block in the inclusive probe range.
        #[arg(long)]
        from_block: u64,
        /// Last execution block in the inclusive probe range.
        #[arg(long)]
        to_block: u64,
        /// Override the public mainnet archive base URL.
        #[arg(long)]
        endpoint: Option<url::Url>,
        /// Maximum sparse archive and normalized bytes.
        #[arg(long, default_value_t = 268_435_456)]
        max_input_bytes: u64,
        /// Optional destination for a JSON summary.
        #[arg(long)]
        report: Option<PathBuf>,
        /// Optional destination for complete normalized frames.
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// Exercise the configured finality boundary.
    Finality {
        /// Override the configured weak-subjectivity checkpoint root.
        #[arg(long)]
        checkpoint: Option<String>,
        /// Override the finalized beacon slot associated with the checkpoint.
        #[arg(long)]
        checkpoint_slot: Option<u64>,
        /// Override configured Beacon API endpoints (repeatable).
        #[arg(long = "endpoint")]
        endpoints: Vec<url::Url>,
        /// Override the number of endpoints that must return the same verified anchor.
        #[arg(long)]
        minimum_agreement: Option<usize>,
        /// Optional JSON report destination.
        #[arg(long)]
        report: Option<PathBuf>,
    },
}

#[derive(Clone, Debug, Subcommand)]
pub enum DbCommand {
    Inspect,
    Verify,
    Backup {
        destination: PathBuf,
    },
    Compact,
    /// Prune durable changes older than a sequence while respecting active
    /// consumer leases.
    PruneChanges {
        #[arg(long)]
        processor: String,
        #[arg(long)]
        before: u64,
    },
}

#[derive(Clone, Copy, Debug, Default, ValueEnum)]
pub enum LogFormat {
    #[default]
    Pretty,
    Json,
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[test]
    fn accepts_global_config_after_subcommand() {
        let cli = Cli::try_parse_from(["leani", "doctor", "--json", "--config", "fixture.toml"])
            .expect("CLI parses");
        assert_eq!(cli.config, Some(PathBuf::from("fixture.toml")));
        assert!(matches!(cli.command, Command::Doctor { json: true }));
    }

    #[test]
    fn subscription_parses_checkpoint_quorum_and_p2p_finality() {
        let cli = Cli::try_parse_from([
            "leani",
            "subscribe",
            "uniswap-v3",
            "ETH/USDC",
            "--finality-source",
            "p2p",
            "--checkpoint-url",
            "https://a.example/",
            "--checkpoint-url",
            "https://b.example/",
            "--checkpoint-quorum",
            "2",
        ])
        .expect("CLI parses");
        let Command::Subscribe {
            finality_source,
            checkpoint_url,
            checkpoint_quorum,
            ..
        } = cli.command
        else {
            panic!("expected subscribe command");
        };
        assert_eq!(finality_source, SubscribeFinalitySource::P2p);
        assert_eq!(checkpoint_url.len(), 2);
        assert_eq!(checkpoint_quorum, 2);
    }

    #[test]
    fn processor_init_parses_compact_uniswap_setup() {
        let cli = Cli::try_parse_from([
            "leani",
            "init",
            "uniswap-v3",
            "ETH/USDC",
            "ETH/USDT",
            "--yes",
        ])
        .expect("CLI parses");
        let Command::Init {
            protocol,
            targets,
            yes,
            ..
        } = cli.command
        else {
            panic!("expected init command");
        };
        assert_eq!(protocol, SubscribeProtocol::UniswapV3);
        assert_eq!(targets, ["ETH/USDC", "ETH/USDT"]);
        assert!(yes);
    }

    #[test]
    fn blocks_feed_and_init_need_no_positional_targets() {
        let subscribe = Cli::try_parse_from(["leani", "subscribe", "blocks", "--once"])
            .expect("blocks subscription parses");
        let Command::Subscribe {
            protocol,
            targets,
            processor,
            once,
            ..
        } = subscribe.command
        else {
            panic!("expected subscribe command");
        };
        assert_eq!(protocol, SubscribeProtocol::Blocks);
        assert!(targets.is_empty());
        assert!(processor.is_none());
        assert!(once);

        let init =
            Cli::try_parse_from(["leani", "init", "blocks", "--yes"]).expect("blocks init parses");
        let Command::Init {
            protocol, targets, ..
        } = init.command
        else {
            panic!("expected init command");
        };
        assert_eq!(protocol, SubscribeProtocol::Blocks);
        assert!(targets.is_empty());
    }

    #[test]
    fn reset_commands_are_explicitly_scoped() {
        let cli = Cli::try_parse_from([
            "leani",
            "reset",
            "subscription",
            "uniswap-v3",
            "ETH/USDC",
            "--finality",
            "finalized",
            "--yes",
        ])
        .expect("CLI parses");
        let Command::Reset {
            command:
                ResetCommand::Subscription {
                    protocol,
                    targets,
                    finality,
                    yes,
                    ..
                },
        } = cli.command
        else {
            panic!("expected subscription reset command");
        };
        assert_eq!(protocol, SubscribeProtocol::UniswapV3);
        assert_eq!(targets, ["ETH/USDC"]);
        assert_eq!(finality, SubscribeFinality::Finalized);
        assert!(yes);

        let cli = Cli::try_parse_from(["leani", "reset", "all", "--yes"]).expect("CLI parses");
        let Command::Reset {
            command: ResetCommand::All { yes, .. },
        } = cli.command
        else {
            panic!("expected full reset command");
        };
        assert!(yes);
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "the table intentionally covers every top-level command"
    )]
    fn parses_all_top_level_command_skeletons() {
        for args in [
            vec!["leani", "serve"],
            vec!["leani", "init", "uniswap-v3", "ETH/USDC", "--yes"],
            vec![
                "leani",
                "subscribe",
                "uniswap-v3",
                "ETH/USDC",
                "ETH/USDT",
                "--format",
                "json",
            ],
            vec![
                "leani",
                "reset",
                "subscription",
                "uniswap-v3",
                "ETH/USDC",
                "--yes",
            ],
            vec!["leani", "reset", "all", "--yes"],
            vec!["leani", "backfill", "--from", "1", "--to", "2"],
            vec![
                "leani",
                "source",
                "probe",
                "xatu",
                "--from-block",
                "1",
                "--to-block",
                "2",
            ],
            vec![
                "leani",
                "conformance",
                "frames",
                "--left-source",
                "xatu",
                "--left",
                "xatu.json",
                "--right-source",
                "erae",
                "--right",
                "erae.json",
                "--capability",
                "header,transactions",
            ],
            vec![
                "leani",
                "benchmark",
                "--profile",
                "materialized",
                "--blocks",
                "2",
                "--runs",
                "1",
                "--warmups",
                "0",
            ],
            vec![
                "leani",
                "benchmark",
                "sweep",
                "--manifest",
                "sweep.json",
                "--output-directory",
                "sweep-results",
            ],
            vec![
                "leani",
                "benchmark",
                "real-source",
                "--processor",
                "blobs-money",
                "--source-policy",
                "xatu-only",
                "--from-block",
                "19426589",
                "--to-block",
                "19426590",
                "--data-dir",
                "real-source-data",
                "--report",
                "real-source.json",
                "--source-concurrency",
                "8",
                "--mapper-concurrency",
                "4",
                "--maximum-active-chunks",
                "8",
            ],
            vec![
                "leani",
                "e2e",
                "mainnet",
                "--from-block",
                "1",
                "--data-dir",
                "e2e-data",
            ],
            vec!["leani", "db", "inspect"],
        ] {
            Cli::try_parse_from(args).expect("command parses");
        }
    }
}
