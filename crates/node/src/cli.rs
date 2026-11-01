//! Command-line contract.

use std::{path::PathBuf, time::Duration};

use clap::{Parser, Subcommand, ValueEnum};
use serde::{Deserialize, Serialize};

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
    /// Print the guide for agents and scripts that matches this binary.
    #[command(after_help = "Examples:
  leani guide
  leani guide processors
  leani guide config

Written for agents and scripts. For tutorials and guides, see https://leani.dev/docs/.")]
    Guide {
        /// Section to print [default: the overview].
        #[arg(value_enum)]
        topic: Option<GuideTopic>,
    },
    /// Create a configuration.
    ///
    /// A compact one for a built-in processor; with any requirement flag,
    /// --requirements, or --json, a full one built for what you need, each
    /// derived setting commented with why.
    #[command(after_help = "Examples:
  leani init blocks
  leani init uniswap-v3 ETH/USDC --data-dir data
  leani init uniswap-v3 ETH/USDC --keep window --window-days 30 --memory 1GiB --bandwidth 100
  leani init --requirements requirements.json --json
  leani init blocks --keep result --follow none --history-from-block 20000000

`leani guide config` documents the requirements. Without a terminal, pass --yes only
after the user has approved the checkpoint providers.")]
    Init {
        /// Processor preset to configure; --requirements names its own.
        #[arg(value_enum, required_unless_present = "requirements")]
        protocol: Option<SubscribeProtocol>,
        /// Optional protocol targets; Uniswap requires one or more markets such as `ETH/USDC`.
        #[arg(num_args = 0.., value_name = "TARGET")]
        targets: Vec<String>,
        /// Data directory for the generated configuration [default: `data` beside it].
        #[arg(long)]
        data_dir: Option<PathBuf>,
        /// Checkpoint provider used for weak-subjectivity quorum (repeatable or comma-delimited).
        #[arg(
            long,
            value_delimiter = ',',
            default_values = [
                "https://ethereum-beacon-api.publicnode.com/",
                "https://mainnet.checkpoint.sigp.io/",
                "https://beaconstate-mainnet.chainsafe.io/"
            ],
            env = "LEANI_CHECKPOINT_URLS",
            hide_env_values = true
        )]
        checkpoint_url: Vec<url::Url>,
        /// Number of independent checkpoint providers that must agree.
        #[arg(long, default_value_t = 2)]
        checkpoint_quorum: usize,
        /// Accept a new checkpoint provider quorum without interactive confirmation.
        #[arg(long)]
        yes: bool,
        /// How much to keep for queries or your app.
        #[arg(long, value_enum)]
        keep: Option<Keep>,
        /// Days `--keep window` keeps [default: 30].
        #[arg(long, value_name = "DAYS")]
        window_days: Option<u64>,
        /// Which blocks to follow as they arrive.
        #[arg(long, value_enum)]
        follow: Option<Follow>,
        /// First block to backfill.
        #[arg(long, value_name = "BLOCK")]
        history_from_block: Option<u64>,
        /// Memory the node may use, such as `1GiB`.
        #[arg(long, value_name = "BYTES")]
        memory: Option<crate::config::HumanBytes>,
        /// Disk the node may use, such as `20GiB`.
        #[arg(long, value_name = "BYTES")]
        disk: Option<crate::config::HumanBytes>,
        /// The host's link, in megabits per second.
        #[arg(long, value_name = "MBIT")]
        bandwidth: Option<u64>,
        /// Tuning recipes to apply instead of the ones the limits call for
        /// (comma-delimited).
        #[arg(long, value_enum, value_delimiter = ',')]
        tuning: Vec<Recipe>,
        /// Requirements as JSON, as `leani guide config` describes, from a
        /// file or `-` for stdin. They replace the preset and the requirement
        /// flags.
        #[arg(long, value_name = "FILE", conflicts_with_all = [
            "protocol", "targets", "keep", "window_days", "follow",
            "history_from_block", "memory", "disk", "bandwidth", "tuning",
        ])]
        requirements: Option<PathBuf>,
        /// Print what was built as JSON: the configuration, the settings it
        /// derived and why, warnings, and next steps.
        #[arg(long)]
        json: bool,
    },
    /// Stream live processor output from an embedded runtime or a running node.
    #[command(after_help = "Examples:
  leani subscribe blocks
  leani subscribe blocks --finality finalized
  leani subscribe uniswap-v3 ETH/USDC --json
  leani subscribe blocks --json --once --timeout 5m
  leani subscribe blocks --once --finality unattested

Exit codes: 0 success or stopped, 124 --timeout elapsed before --once was
satisfied, 130 interrupted before --once was satisfied.")]
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
        /// Shorthand for `--format json`.
        #[arg(long, conflicts_with = "format")]
        json: bool,
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
        #[arg(long, env = "LEANI_API_TOKEN", hide_env_values = true)]
        token: Option<String>,
        /// Lowest level that counts as a result: `unattested` (default),
        /// `attested`, or `finalized`.
        #[arg(long, value_enum, default_value_t = SubscribeFinality::Unattested)]
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
            env = "LEANI_CHECKPOINT_URLS",
            hide_env_values = true
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
        /// Stop after this long, e.g. `90s`, `5m`, or `300` (seconds); with
        /// `--once`, exit 124 if nothing matched.
        #[arg(long, value_name = "DURATION", value_parser = parse_duration)]
        timeout: Option<Duration>,
    },
    /// Reset reconstructible local runtime state for cold-start testing.
    Reset {
        #[command(subcommand)]
        command: ResetCommand,
    },
    /// Process a historical block range and then exit.
    #[command(after_help = "Examples:
  leani backfill --from-block 17000000 --to-block 17000999
  leani backfill --processor weth-transfers --from-block 17000000 --to-block 17000009
  leani backfill --endpoint http://127.0.0.1:18080 --from-block 17000000 --to-block 17000999")]
    Backfill {
        /// Configured processor instance or kind [default: the only configured processor].
        #[arg(long)]
        processor: Option<String>,
        /// First block of the range, inclusive.
        #[arg(long, visible_alias = "from")]
        from_block: u64,
        /// Last block of the range, inclusive.
        #[arg(long, visible_alias = "to")]
        to_block: u64,
        /// Submit to an already running node, preserving an optional URL prefix.
        #[arg(long)]
        endpoint: Option<url::Url>,
        /// Bearer token for a running node.
        #[arg(long, env = "LEANI_API_TOKEN", hide_env_values = true)]
        token: Option<String>,
    },
    /// Create Wasm processors and run them over finalized history.
    Processor {
        #[command(subcommand)]
        command: ProcessorCommand,
    },
    /// Inspect or probe configured data sources.
    Source {
        #[command(subcommand)]
        command: SourceCommand,
    },
    /// Compare normalized source exports or render exact RPC response vectors.
    #[command(hide = true)]
    Conformance {
        #[command(subcommand)]
        command: ConformanceCommand,
    },
    /// Run a reproducible historical throughput and storage measurement.
    #[command(hide = true)]
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
    #[command(hide = true)]
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

/// A duration such as `30s`, `5m`, or `1h30m`; a bare number counts
/// seconds, as `timeout(1)` does.
fn parse_duration(value: &str) -> Result<Duration, humantime::DurationError> {
    value.parse::<u64>().map_or_else(
        |_| humantime::parse_duration(value),
        |seconds| Ok(Duration::from_secs(seconds)),
    )
}

#[derive(Clone, Debug, Subcommand)]
pub enum ProcessorCommand {
    /// Create a Wasm processor as a Rust crate or a TypeScript package.
    ///
    /// The processor is generated from a spec of the onchain data to keep,
    /// copied from a template's source, or the starter.
    #[command(after_help = "Examples:
  leani processor create usdc-transfers
  leani processor create usdc-transfers --language typescript
  leani processor create usdc-volume --spec spec.json
  leani processor create my-decoder --template event-decoder --language typescript
  echo '{\"data\":{\"kind\":\"blocks\"}}' | leani processor create gas --spec - --json

A spec names the data and what to keep of it:

  {\"name\": \"usdc-volume\",
   \"data\": {\"kind\": \"events\", \"contracts\": [\"0xa0b8...\"],
            \"events\": [\"Transfer(address indexed from, address indexed to, uint256 value)\"]},
   \"keep\": {\"kind\": \"totals\", \"by\": \"to\", \"sum\": [\"value\"]}}

data.kind is events, transactions (to, from, functions), or blocks; each takes
fields to keep, all by default. keep.kind is rows or totals. The project
carries the guest SDK of this binary: Rust builds with cargo, TypeScript (as
AssemblyScript compiles it) with npm. `leani guide processors` describes specs
and the processor contract.")]
    Create {
        /// Directory to create; its last component names the project.
        directory: PathBuf,
        /// The spec as JSON, or `-` for standard input.
        #[arg(long, conflicts_with = "template")]
        spec: Option<PathBuf>,
        /// Start from a bundled template's source instead.
        #[arg(long)]
        template: Option<String>,
        /// The language to write it in; a spec may name one.
        #[arg(long, value_enum)]
        language: Option<ProcessorLanguage>,
        /// Print the result as JSON.
        #[arg(long)]
        json: bool,
    },
    /// Run a Wasm processor over finalized history and print its changes.
    #[command(after_help = "Examples:
  leani processor run transfers.wasm --from-block 21000000 --to-block 21000099
  leani processor run transfers.wasm --from-block 21000000 --to-block 21000099 --param minValue=1000000000
  leani processor run --template event-decoder --from 21000000 --to 21000009 \\
    --params '{\"events\":[\"Transfer(address indexed from, address indexed to, uint256 value)\"]}'

Each change prints as one JSON line on stdout; progress and the report go to
stderr. Sources, chain, and finality come from the configuration file; its
processors are ignored. Build modules with the leani-wasm-guest crate, or run
a bundled template: block-gas, erc20-transfers, token-volume, event-decoder.")]
    Run {
        /// Wasm module built with leani-wasm-guest.
        #[arg(required_unless_present = "template")]
        module: Option<PathBuf>,
        /// A bundled template instead of a module file; without parameters
        /// it runs with its example parameters.
        #[arg(long, conflicts_with = "module")]
        template: Option<String>,
        /// First block of the range, inclusive.
        #[arg(long, visible_alias = "from")]
        from_block: u64,
        /// Last block of the range, inclusive.
        #[arg(long, visible_alias = "to")]
        to_block: u64,
        /// One parameter, as KEY=VALUE (repeatable); VALUE is read as JSON when it parses, else as a string, as are integers past 2^63.
        #[arg(long = "param", value_name = "KEY=VALUE")]
        params: Vec<String>,
        /// Every parameter as one JSON object; --param entries override its keys.
        #[arg(long = "params", value_name = "JSON")]
        params_json: Option<String>,
        /// Keep the run's state here instead of in a temporary directory; a
        /// later run there must use the same module, parameters, and
        /// --from-block.
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
}

/// The language `leani processor create` writes a processor in.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum ProcessorLanguage {
    /// A crate that cargo builds.
    Rust,
    /// A package that npm builds with the `assemblyscript` compiler.
    Typescript,
}

/// Sections of `leani guide` beyond the overview.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum GuideTopic {
    /// Built-in processors, event decoding, backfills, and custom processors.
    Processors,
    /// Building a node configuration from requirements with `leani init`.
    Config,
}

/// How much a built configuration keeps.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub enum Keep {
    /// Everything the processor writes, for queries.
    #[default]
    Everything,
    /// The last `--window-days` days, for queries.
    Window,
    /// The current value of each key.
    Latest,
    /// One result for a finite range, handed to your app.
    Result,
    /// Nothing to query: every change waits until your app's database
    /// confirms it.
    Stream,
    /// Nothing to query: a short best-effort stream of the live head.
    Live,
}

/// Which blocks a built configuration follows.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub enum Follow {
    /// The live head, undoing blocks a reorg replaces.
    #[default]
    Head,
    /// Finalized blocks only.
    Finalized,
    /// No live blocks: history only.
    None,
}

/// A measured tuning recipe from the configuration guide; requirements
/// spell it in camel case, such as `lessMemory`.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize, ValueEnum)]
#[serde(rename_all = "camelCase")]
pub enum Recipe {
    LimitedMemory,
    FasterErae,
    LessMemory,
    TransactionsFromXatu,
    SlowDisk,
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

/// Lowest chain-confidence level that counts as a subscription result.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum SubscribeFinality {
    /// Print blocks as soon as the live lane accepts them: peers agree on the
    /// header, it passes Leani's header rules and extends the delivered chain.
    /// The sync committee has not attested them yet, so they can be undone.
    Unattested,
    /// Print blocks once the sync committee attested them; a reorg can still
    /// remove them.
    Attested,
    /// Print blocks only after Ethereum consensus has finalized them.
    Finalized,
}

impl SubscribeFinality {
    /// The lowest level a printed row has.
    #[must_use]
    pub const fn level(self) -> leani_primitives::Finality {
        match self {
            Self::Unattested => leani_primitives::Finality::Unattested,
            Self::Attested => leani_primitives::Finality::Attested,
            Self::Finalized => leani_primitives::Finality::Finalized,
        }
    }

    /// The publish mode of the embedded processor for this level.
    #[must_use]
    pub const fn publish_mode(self) -> crate::config::PublishMode {
        match self {
            Self::Unattested => crate::config::PublishMode::Unattested,
            Self::Attested => crate::config::PublishMode::Attested,
            Self::Finalized => crate::config::PublishMode::Finalized,
        }
    }
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
        /// Finality of the subscription state being reset (`unattested`,
        /// `attested` or `finalized`).
        #[arg(long, value_enum, default_value_t = SubscribeFinality::Unattested)]
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
        /// Xatu network catalog; only mainnet is supported.
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
        /// Maximum bytes the projection fetches in all, Parquet footers and
        /// merged gaps included, and the most it holds at once.
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
    /// Fetch blocks over JSON-RPC and check them against their own roots.
    Rpc {
        /// First block; omitted, 31 blocks before the last.
        #[arg(long)]
        from_block: Option<u64>,
        /// Last block; omitted, the endpoint's head minus ten.
        #[arg(long)]
        to_block: Option<u64>,
        /// Endpoint to ask instead of the chain entry's.
        #[arg(long)]
        endpoint: Option<url::Url>,
    },
    /// Follow an op-stack chain's sequencer-signed block gossip.
    OpGossip {
        /// How long to listen.
        #[arg(long, default_value_t = 120)]
        seconds: u64,
        /// Optional JSON lines destination for the verified blocks.
        #[arg(long)]
        output: Option<PathBuf>,
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
        /// Override the public mainnet archive base URL with an https, file,
        /// or loopback http URL.
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
    fn timeouts_take_units_or_bare_seconds() {
        for (value, seconds) in [("300", 300), ("90s", 90), ("5m", 300), ("1h30m", 5_400)] {
            assert_eq!(
                parse_duration(value).unwrap(),
                Duration::from_secs(seconds),
                "{value}"
            );
        }
        assert!(parse_duration("5 minutes please").is_err());
        assert!(parse_duration("-1").is_err());
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
        assert_eq!(protocol, Some(SubscribeProtocol::UniswapV3));
        assert_eq!(targets, ["ETH/USDC", "ETH/USDT"]);
        assert!(yes);
    }

    #[test]
    fn subscribe_defaults_to_unattested() {
        let cli = Cli::try_parse_from(["leani", "subscribe", "blocks"]).expect("parse");
        let Command::Subscribe { finality, .. } = cli.command else {
            panic!("subscribe command");
        };
        assert_eq!(finality, SubscribeFinality::Unattested);
    }

    #[test]
    fn processor_create_takes_a_spec_or_a_template() {
        let cli = Cli::try_parse_from([
            "leani",
            "processor",
            "create",
            "volume",
            "--spec",
            "-",
            "--language",
            "typescript",
            "--json",
        ])
        .expect("CLI parses");
        let Command::Processor {
            command:
                ProcessorCommand::Create {
                    directory,
                    spec,
                    template,
                    language,
                    json,
                },
        } = cli.command
        else {
            panic!("expected processor create command");
        };
        assert_eq!(directory, PathBuf::from("volume"));
        assert_eq!(spec, Some(PathBuf::from("-")));
        assert_eq!(template, None);
        assert_eq!(language, Some(ProcessorLanguage::Typescript));
        assert!(json);
        assert!(
            Cli::try_parse_from([
                "leani",
                "processor",
                "create",
                "x",
                "--spec",
                "s.json",
                "--template",
                "block-gas"
            ])
            .is_err(),
            "a spec or a template"
        );
    }

    #[test]
    fn processor_run_takes_a_module_or_a_template() {
        let cli = Cli::try_parse_from([
            "leani",
            "processor",
            "run",
            "--template",
            "block-gas",
            "--from",
            "1",
            "--to",
            "2",
        ])
        .expect("CLI parses");
        let Command::Processor {
            command:
                ProcessorCommand::Run {
                    module,
                    template,
                    from_block,
                    to_block,
                    ..
                },
        } = cli.command
        else {
            panic!("expected processor run command");
        };
        assert_eq!(module, None);
        assert_eq!(template.as_deref(), Some("block-gas"));
        assert_eq!((from_block, to_block), (1, 2));
        assert!(
            Cli::try_parse_from(["leani", "processor", "run", "--from", "1", "--to", "2"]).is_err(),
            "a module or a template"
        );
    }

    #[test]
    fn processor_run_help_names_every_bundled_template() {
        let mut command = <Cli as clap::CommandFactory>::command();
        let help = command
            .find_subcommand_mut("processor")
            .and_then(|processor| processor.find_subcommand_mut("run"))
            .and_then(|run| run.get_after_help().map(ToString::to_string))
            .expect("run help");
        let names = leani_processor_wasm::TEMPLATES
            .map(|template| template.name)
            .join(", ");
        assert!(
            help.contains(&format!("a bundled template: {names}.")),
            "{help}"
        );
    }

    #[test]
    fn init_takes_requirement_flags_or_a_requirements_file() {
        let cli = Cli::try_parse_from([
            "leani",
            "init",
            "uniswap-v3",
            "ETH/USDC",
            "--keep",
            "window",
            "--window-days",
            "30",
            "--follow",
            "finalized",
            "--memory",
            "1GiB",
            "--bandwidth",
            "100",
            "--tuning",
            "less-memory,slow-disk",
            "--json",
        ])
        .expect("CLI parses");
        let Command::Init {
            keep,
            window_days,
            follow,
            memory,
            bandwidth,
            tuning,
            json,
            ..
        } = cli.command
        else {
            panic!("expected init command");
        };
        assert_eq!(keep, Some(Keep::Window));
        assert_eq!(window_days, Some(30));
        assert_eq!(follow, Some(Follow::Finalized));
        assert_eq!(memory.map(crate::config::HumanBytes::bytes), Some(1 << 30));
        assert_eq!(bandwidth, Some(100));
        assert_eq!(tuning, [Recipe::LessMemory, Recipe::SlowDisk]);
        assert!(json);

        let file = Cli::try_parse_from(["leani", "init", "--requirements", "-", "--json"])
            .expect("a requirements file needs no preset");
        assert!(matches!(file.command, Command::Init { protocol: None, .. }));
        assert!(
            Cli::try_parse_from([
                "leani",
                "init",
                "--requirements",
                "r.json",
                "--keep",
                "live"
            ])
            .is_err(),
            "the file replaces the flags"
        );
        assert!(
            Cli::try_parse_from(["leani", "init"]).is_err(),
            "a preset or a file"
        );
    }

    #[test]
    fn init_sizes_take_a_byte_count_or_a_unit() {
        for (given, bytes) in [("1073741824", 1 << 30), ("512MiB", 512 << 20)] {
            let cli =
                Cli::try_parse_from(["leani", "init", "blocks", "--memory", given]).expect(given);
            let Command::Init { memory, .. } = cli.command else {
                panic!("expected init command");
            };
            assert_eq!(memory.map(crate::config::HumanBytes::bytes), Some(bytes));
        }
        assert!(Cli::try_parse_from(["leani", "init", "blocks", "--disk", "lots"]).is_err());
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
        assert_eq!(protocol, Some(SubscribeProtocol::Blocks));
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
