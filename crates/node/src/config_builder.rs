//! A complete node configuration from what an application needs: what to
//! index, how much of it to keep, how fresh, how much history, and the host's
//! limits.
//!
//! The builder writes every setting, and comments each one it derived with
//! the reason. Its values come from the shipped profiles in `config/modes`
//! and the configuration guide's measured sizing table, never from guesses.

use leani_processor_api::{
    DeliveryLimitAction, DeliveryPolicyMode, OutputPolicyMode, UndoPolicyMode,
};
use serde::{Deserialize, Serialize};

use crate::{
    builtin_processors::processor_contract,
    cli::{Follow, Keep, Recipe},
    config::{
        Config, DeliveryConsumerConfig, DeliveryEmit, DeliveryPolicyConfig, DeliveryPruningConfig,
        FinalityConfig, FinalitySourceKind, HistorySourceKind, HumanBytes, HumanDuration,
        HumanMilliseconds, LiveSourceKind, OutputPolicyConfig, OutputWindowConfig, ProcessorConfig,
        ProcessorHistoryControl, ProcessorHistoryMode, PublishMode, UndoPolicyConfig,
        ValidationError,
    },
    processors::ProcessorRegistry,
};

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct Requirements {
    pub processor: ProcessorChoice,
    #[serde(default)]
    pub keep: Keep,
    /// How many days `keep = "window"` keeps.
    #[serde(default)]
    pub window_days: Option<u64>,
    #[serde(default)]
    pub follow: Follow,
    #[serde(default)]
    pub history: Option<History>,
    #[serde(default)]
    pub limits: Limits,
    /// Tuning recipes to apply instead of the ones the limits call for.
    #[serde(default)]
    pub tuning: Option<Vec<Recipe>>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "preset", rename_all = "kebab-case", deny_unknown_fields)]
pub(crate) enum ProcessorChoice {
    /// Every block's summary.
    Blocks,
    /// Uniswap pool observations for markets such as `ETH/USDC`.
    Uniswap { markets: Vec<String> },
    /// One contract's events, decoded from Solidity signatures such as
    /// `Transfer(address indexed from, address indexed to, uint256 value)`.
    Events {
        address: String,
        events: Vec<String>,
    },
    /// Any processor this node runs: a Wasm module (`wasm`), another built-in,
    /// or one compiled into a custom node.
    Custom {
        id: String,
        #[serde(default)]
        instance: Option<String>,
        #[serde(default)]
        version: Option<String>,
        #[serde(default)]
        settings: toml::Table,
    },
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct History {
    #[serde(default)]
    pub from_block: Option<u64>,
    #[serde(default)]
    pub last_days: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct Limits {
    #[serde(default)]
    pub memory: Option<HumanBytes>,
    #[serde(default)]
    pub disk: Option<HumanBytes>,
    /// Megabits per second.
    #[serde(default)]
    pub bandwidth_mbit: Option<u64>,
}

pub(crate) struct BuildContext<'a> {
    pub registry: &'a ProcessorRegistry,
    /// The finality section of a configuration that follows the chain:
    /// `leani init` fetches a checkpoint, a running node passes its own.
    pub finality: Option<FinalityConfig>,
    /// The chain head, when known, for history given in days.
    pub head: Option<u64>,
    /// Where the node keeps its data; `./data` beside the file when absent.
    pub data_dir: Option<std::path::PathBuf>,
}

/// The finality section for a checkpoint `leani init` fetched: the built-in
/// Mainnet defaults with that checkpoint and those Beacon transports.
pub(crate) fn finality(
    checkpoint: String,
    checkpoint_slot: u64,
    endpoints: Vec<url::Url>,
) -> Result<FinalityConfig, String> {
    Ok(FinalityConfig {
        checkpoint,
        checkpoint_slot,
        endpoints,
        ..Config::ethereum_mainnet_defaults()?.finality
    })
}

/// A node passes its own finality section, so a configuration that follows
/// the chain starts from the checkpoint this node verified.
pub(crate) fn build_json(
    requirements: &serde_json::Value,
    head: Option<u64>,
    registry: &ProcessorRegistry,
    finality: &FinalityConfig,
) -> Result<serde_json::Value, String> {
    let requirements: Requirements = serde_json::from_value(requirements.clone())
        .map_err(|error| format!("requirements: {error}"))?;
    let finality = matches!(finality.kind, FinalitySourceKind::BeaconApi).then(|| finality.clone());
    let build = build(
        &requirements,
        &BuildContext {
            registry,
            finality,
            head,
            data_dir: None,
        },
    )?;
    serde_json::to_value(build).map_err(|error| error.to_string())
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Build {
    pub toml: String,
    pub decisions: Vec<Decision>,
    pub tuning: Vec<Recipe>,
    pub warnings: Vec<String>,
    pub errors: Vec<ValidationError>,
    pub next: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct Decision {
    pub setting: String,
    pub value: String,
    pub because: String,
}

const GIB: u64 = 1 << 30;
const MIB: u64 = 1 << 20;
/// Mainnet slots a day; a few are missed, so a day holds slightly fewer
/// blocks.
const BLOCKS_PER_DAY: u64 = 7_200;
const DEFAULT_WINDOW_DAYS: u64 = 30;
/// Below this link speed, a recipe may put Xatu before eraE.
const SLOW_LINK_MBIT: u64 = 200;
const APP_CONSUMER: &str = "app";

/// Refuse requirements that cannot be met before anything is fetched for
/// them, such as unknown markets or history in days without a head.
pub(crate) fn check(requirements: &Requirements, head: Option<u64>) -> Result<(), String> {
    processor_entry(&requirements.processor, PublishMode::Finalized)?;
    history_start(requirements, head).map(drop)
}

/// Build a configuration for `requirements`.
///
/// # Errors
///
/// Returns why the requirements cannot be met, such as history in days
/// without a known head or a live configuration without a checkpoint.
pub(crate) fn build(
    requirements: &Requirements,
    context: &BuildContext<'_>,
) -> Result<Build, String> {
    let mut config = Config::ethereum_mainnet_defaults()?;
    if let Some(data_dir) = &context.data_dir {
        config.data_dir.clone_from(data_dir);
    }
    let mut decisions = Decisions::default();
    let mut warnings = Vec::new();
    let publish = if requirements.follow == Follow::Head {
        PublishMode::Unattested
    } else {
        PublishMode::Finalized
    };
    let mut processor = processor_entry(&requirements.processor, publish)?;
    let wasm = processor.id == leani_processor_wasm::PROCESSOR_ID;

    let start = history_start(requirements, context.head)?;
    keep(requirements, &mut processor, &mut decisions);
    follow(
        requirements,
        &mut config,
        &mut processor,
        context,
        &mut decisions,
    )?;
    history(requirements, start, &mut processor, &mut decisions);

    // Whether the processor reads logs decides which history source may go
    // first: only eraE proves a block's logs complete.
    let reads_logs = wasm
        || context
            .registry
            .instantiate(&processor, config.chain.chain_id)
            .map_or(true, |instance| {
                instance
                    .descriptor()
                    .requirements
                    .iter()
                    .any(|requirement| {
                        requirement.capabilities.intersection(
                            leani_primitives::CapabilitySet::of(leani_primitives::Capability::Logs)
                                .union(leani_primitives::CapabilitySet::of(
                                    leani_primitives::Capability::Receipts,
                                )),
                        ) != leani_primitives::CapabilitySet::NONE
                    })
            });
    let tuning = requirements
        .tuning
        .clone()
        .unwrap_or_else(|| recipes_for(&requirements.limits, reads_logs));
    for recipe in &tuning {
        apply(*recipe, &mut config, &mut decisions);
    }
    if reads_logs && tuning.contains(&Recipe::TransactionsFromXatu) {
        warnings.push(
            "transactionsFromXatu puts Xatu first, but this processor reads logs, which only eraE proves complete".to_owned(),
        );
    } else if reads_logs
        && requirements
            .limits
            .bandwidth_mbit
            .is_some_and(|mbit| mbit < SLOW_LINK_MBIT)
    {
        decisions.add(
            "sources.history.erae.priority",
            "10",
            "eraE stays first on a slow link: this processor reads logs, which only eraE proves complete",
        );
    }
    disk(requirements, &mut config, &mut decisions, &mut warnings);

    let instance = processor.instance.clone();
    config.processors = vec![processor];
    let mut errors = config.validation_errors();
    if wasm {
        warnings.push(crate::processors::WASM_CHECKED_WHERE_INSTALLED.to_owned());
    } else {
        errors.extend(context.registry.validation_errors(&config));
        warnings.extend(context.registry.validation_warnings(&config));
    }
    let toml = toml::to_string_pretty(&config)
        .map_err(|error| format!("render the configuration: {error}"))?;
    Ok(Build {
        toml: format!("{}{}", heading(requirements), annotate(&toml, &decisions.0)),
        decisions: decisions.0,
        tuning,
        warnings,
        errors,
        next: next_steps(requirements, &instance, start),
    })
}

#[derive(Default)]
struct Decisions(Vec<Decision>);

impl Decisions {
    #[expect(
        clippy::needless_pass_by_value,
        reason = "callers pass literals and numbers"
    )]
    fn add(&mut self, setting: &str, value: impl ToString, because: impl Into<String>) {
        self.0.retain(|decision| decision.setting != setting);
        self.0.push(Decision {
            setting: setting.to_owned(),
            value: value.to_string(),
            because: because.into(),
        });
    }
}

fn processor_entry(
    choice: &ProcessorChoice,
    publish: PublishMode,
) -> Result<ProcessorConfig, String> {
    match choice {
        ProcessorChoice::Blocks => Ok(crate::block_summaries::processor_config(
            crate::block_summaries::COMPACT_INSTANCE,
            publish,
        )),
        ProcessorChoice::Uniswap { markets } => {
            let markets = crate::uniswap_markets::resolve_markets(markets)
                .map_err(|error| error.to_string())?;
            crate::uniswap_markets::processor_config(
                &markets,
                crate::uniswap_markets::COMPACT_INSTANCE,
                publish,
            )
            .map_err(|error| error.to_string())
        }
        ProcessorChoice::Events { address, events } => {
            let mut processor = processor_contract(
                "evm-events",
                "contract-events",
                leani_processor_evm_events::EVM_EVENTS_VERSION,
                0,
                publish,
            );
            processor.settings = events_settings(address, events)?;
            Ok(processor)
        }
        ProcessorChoice::Custom {
            id,
            instance,
            version,
            settings,
        } => {
            let mut processor = processor_contract(
                id,
                instance.as_deref().unwrap_or(id),
                version.as_deref().unwrap_or("1.0.0"),
                0,
                publish,
            );
            processor.settings = settings.clone();
            Ok(processor)
        }
    }
}

/// `evm-events` settings decoding `events` of the contract at `address`, one
/// entity per event.
fn events_settings(address: &str, events: &[String]) -> Result<toml::Table, String> {
    if events.is_empty() {
        return Err("events needs at least one event signature".to_owned());
    }
    let definitions = events
        .iter()
        .map(|signature| {
            let signature = signature.trim().trim_start_matches("event ").trim();
            let name = signature
                .split_once('(')
                .map(|(name, _)| name.trim())
                .filter(|name| !name.is_empty())
                .ok_or_else(|| format!("`{signature}` is not an event signature such as `Transfer(address indexed from, address indexed to, uint256 value)`"))?;
            let collection = format!("events.{}", name.to_ascii_lowercase());
            let mut output = toml::Table::new();
            output.insert("collection".into(), collection.clone().into());
            output.insert("kind".into(), collection.into());
            let mut definition = toml::Table::new();
            definition.insert("abi".into(), format!("event {signature}").into());
            definition.insert("output".into(), output.into());
            Ok(toml::Value::Table(definition))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let mut settings = toml::Table::new();
    settings.insert(
        "addresses".into(),
        toml::Value::Array(vec![address.trim().to_owned().into()]),
    );
    settings.insert("events".into(), toml::Value::Array(definitions));
    Ok(settings)
}

fn history_start(requirements: &Requirements, head: Option<u64>) -> Result<Option<u64>, String> {
    match requirements.history {
        None => Ok(None),
        Some(History {
            from_block: Some(block),
            last_days: None,
        }) => Ok(Some(block)),
        Some(History {
            from_block: None,
            last_days: Some(days),
        }) => head
            .map(|head| Some(head.saturating_sub(days.saturating_mul(BLOCKS_PER_DAY))))
            .ok_or_else(|| {
                "history in days needs the chain head, which a running node knows once it retains blocks; give history.fromBlock instead".to_owned()
            }),
        Some(_) => Err("history takes exactly one of fromBlock and lastDays".to_owned()),
    }
}

fn delivery(
    mode: DeliveryPolicyMode,
    max_bytes: u64,
    max_age_seconds: u64,
    on_limit: DeliveryLimitAction,
    consumer: bool,
) -> DeliveryPolicyConfig {
    DeliveryPolicyConfig {
        mode,
        emit: DeliveryEmit::Changes,
        max_bytes: HumanBytes::from_bytes(max_bytes),
        max_age: HumanDuration::from_seconds(max_age_seconds),
        on_limit,
        pruning: DeliveryPruningConfig::default(),
        consumers: if consumer {
            vec![DeliveryConsumerConfig {
                id: APP_CONSUMER.to_owned(),
                required: true,
                lease_ttl: HumanDuration::from_seconds(300),
            }]
        } else {
            Vec::new()
        },
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "one arm per keep option, each with its settings and reasons"
)]
fn keep(requirements: &Requirements, processor: &mut ProcessorConfig, decisions: &mut Decisions) {
    let output = |mode| OutputPolicyConfig {
        mode,
        window: None,
        finalized_only: None,
    };
    match requirements.keep {
        Keep::Everything => {
            processor.output = output(OutputPolicyMode::Full);
            decisions.add(
                "processors.output.mode",
                "full",
                "keep everything the processor writes, for queries",
            );
        }
        Keep::Window => {
            let days = window_days(requirements);
            let blocks = days.saturating_mul(BLOCKS_PER_DAY);
            processor.output = OutputPolicyConfig {
                window: Some(OutputWindowConfig {
                    max_blocks: Some(blocks),
                    max_age: None,
                    max_rows: None,
                    max_bytes: None,
                }),
                ..output(OutputPolicyMode::Window)
            };
            decisions.add(
                "processors.output.mode",
                "window",
                format!("keep only the last {days} days for queries"),
            );
            decisions.add(
                "processors.output.window.max_blocks",
                blocks,
                format!("about {days} days at 7,200 blocks a day"),
            );
        }
        Keep::Latest => {
            processor.output = output(OutputPolicyMode::Latest);
            decisions.add(
                "processors.output.mode",
                "latest",
                "keep the current value of each key",
            );
        }
        Keep::Result => {
            processor.output = output(OutputPolicyMode::Latest);
            // A job result covers finalized history and takes no `publish`.
            processor.publish = None;
            processor.delivery = DeliveryPolicyConfig {
                emit: DeliveryEmit::JobResult,
                ..delivery(
                    DeliveryPolicyMode::UntilAcknowledged,
                    64 * MIB,
                    24 * 3_600,
                    DeliveryLimitAction::Pause,
                    true,
                )
            };
            decisions.add(
                "processors.output.mode",
                "latest",
                "keep the running result while the range is processed",
            );
            decisions.add(
                "processors.delivery.emit",
                "job_result",
                "deliver one result, when the range is done",
            );
            decisions.add(
                "processors.delivery.mode",
                "until_acknowledged",
                "hold the result until your app confirms it",
            );
        }
        Keep::Stream => {
            processor.output = output(OutputPolicyMode::None);
            processor.history_control = ProcessorHistoryControl::ApplicationSubscriptions;
            processor.delivery = delivery(
                DeliveryPolicyMode::UntilAcknowledged,
                GIB,
                24 * 3_600,
                DeliveryLimitAction::Pause,
                true,
            );
            decisions.add(
                "processors.output.mode",
                "none",
                "keep nothing to query: your app's database keeps the data",
            );
            decisions.add(
                "processors.history_control",
                "application_subscriptions",
                "your app asks for the ranges it needs",
            );
            decisions.add(
                "processors.delivery.mode",
                "until_acknowledged",
                "hold every change until your app confirms it",
            );
            decisions.add(
                "processors.delivery.on_limit",
                "pause",
                "stop processing when 1 GiB or 24 h waits, never drop",
            );
        }
        Keep::Live => {
            processor.output = output(OutputPolicyMode::None);
            processor.delivery = delivery(
                DeliveryPolicyMode::BestEffort,
                64 * MIB,
                3_600,
                DeliveryLimitAction::ExpireAndReset,
                false,
            );
            decisions.add("processors.output.mode", "none", "keep nothing to query");
            decisions.add(
                "processors.delivery.mode",
                "best_effort",
                "stream changes to readers for up to 1 h or 64 MiB",
            );
            decisions.add(
                "processors.delivery.on_limit",
                "expire_and_reset",
                "drop the oldest changes and reset slow readers",
            );
        }
    }
    if matches!(
        requirements.keep,
        Keep::Everything | Keep::Window | Keep::Latest
    ) {
        decisions.add(
            "processors.delivery.mode",
            "window",
            "your app can also stream changes, held for 24 h or 64 MiB",
        );
    }
}

fn follow(
    requirements: &Requirements,
    config: &mut Config,
    processor: &mut ProcessorConfig,
    context: &BuildContext<'_>,
    decisions: &mut Decisions,
) -> Result<(), String> {
    let terminal = requirements.keep == Keep::Result;
    match requirements.follow {
        Follow::None => {
            config.sources.live.kind = LiveSourceKind::Disabled;
            config.sources.live.minimum_peers = 0;
            config.finality.kind = FinalitySourceKind::Disabled;
            config.finality.checkpoint = String::new();
            config.finality.checkpoint_slot = 0;
            decisions.add(
                "sources.live.kind",
                "disabled",
                "history only: no live blocks to follow",
            );
            decisions.add(
                "finality.kind",
                "disabled",
                "history only: finalized history needs no live finality",
            );
        }
        Follow::Head | Follow::Finalized => {
            config.finality = context.finality.clone().ok_or_else(|| {
                "following the chain needs a finality checkpoint: `leani init` fetches one, and a running node passes its own".to_owned()
            })?;
            decisions.add(
                "finality.checkpoint",
                &config.finality.checkpoint,
                "the verified checkpoint finality starts from",
            );
        }
    }
    if terminal {
        processor.undo = UndoPolicyConfig {
            mode: UndoPolicyMode::None,
            safety_blocks: 0,
        };
        decisions.add(
            "processors.undo.mode",
            "none",
            "one result from finalized blocks: nothing to undo",
        );
        return Ok(());
    }
    // `processor_contract` set `publish` and `undo` for the follow mode.
    if requirements.follow == Follow::Head {
        decisions.add(
            "processors.publish",
            "unattested",
            "publish each block seconds into its slot, then mark it attested and finalized",
        );
        decisions.add(
            "processors.undo.mode",
            "unfinalized",
            "undo blocks a reorg replaces",
        );
    } else {
        decisions.add(
            "processors.publish",
            "finalized",
            "publish finalized blocks only",
        );
        decisions.add(
            "processors.undo.mode",
            "none",
            "finalized blocks only: nothing to undo",
        );
    }
    Ok(())
}

fn history(
    requirements: &Requirements,
    start: Option<u64>,
    processor: &mut ProcessorConfig,
    decisions: &mut Decisions,
) {
    if let Some(start) = start {
        processor.start_block = start;
        decisions.add(
            "processors.start_block",
            start,
            "the first block of the history you asked for",
        );
    }
    // A bounded result and a node without a live lane backfill explicit
    // ranges with `leani backfill`, and an app that subscribes asks for its
    // own.
    if start.is_some()
        && requirements.keep != Keep::Result
        && requirements.follow != Follow::None
        && !matches!(
            processor.history_control,
            ProcessorHistoryControl::ApplicationSubscriptions
        )
    {
        processor.history_mode = ProcessorHistoryMode::Automatic;
        decisions.add(
            "processors.history_mode",
            "automatic",
            "backfill from start_block on its own",
        );
    } else {
        processor.history_mode = ProcessorHistoryMode::OnDemand;
        decisions.add(
            "processors.history_mode",
            "on_demand",
            "backfill only the ranges asked for",
        );
    }
}

/// The recipes the configuration guide's sizing table calls for.
fn recipes_for(limits: &Limits, reads_logs: bool) -> Vec<Recipe> {
    let mut recipes = Vec::new();
    let bandwidth = limits.bandwidth_mbit;
    match limits.memory.map(HumanBytes::bytes) {
        Some(memory) if memory < GIB => recipes.push(Recipe::LimitedMemory),
        Some(memory) => {
            recipes.push(Recipe::FasterErae);
            // Xatu backfills hold about 450 MiB per active chunk, and the
            // defaults keep 8 chunks.
            if memory < 4 * GIB {
                recipes.push(Recipe::LessMemory);
            }
        }
        None => {}
    }
    if !reads_logs && bandwidth.is_some_and(|mbit| mbit < SLOW_LINK_MBIT) {
        recipes.push(Recipe::TransactionsFromXatu);
    }
    recipes.sort();
    recipes
}

fn apply(recipe: Recipe, config: &mut Config, decisions: &mut Decisions) {
    let budgets = &mut config.budgets;
    match recipe {
        Recipe::LimitedMemory => {
            budgets.source_concurrency = 4;
            budgets.history_pipeline.maximum_active_chunks = 2;
            decisions.add(
                "budgets.source_concurrency",
                4,
                "limited memory: 4 reads, measured at 160-560 MiB peak",
            );
            decisions.add(
                "budgets.history_pipeline.maximum_active_chunks",
                2,
                "limited memory: 2 chunks in flight",
            );
        }
        Recipe::FasterErae => {
            budgets.history_material.memory_bytes = HumanBytes::from_bytes(512 * MIB);
            budgets
                .history_material
                .maximum_buffered_frames_per_acquisition = 512;
            decisions.add(
                "budgets.history_material.memory_bytes",
                "512MiB",
                "512 MiB: eraE backfills a quarter to a half faster",
            );
            decisions.add(
                "budgets.history_material.maximum_buffered_frames_per_acquisition",
                512,
                "at the same peak memory as the default",
            );
        }
        Recipe::LessMemory => {
            budgets.history_pipeline.maximum_active_chunks = 4;
            decisions.add(
                "budgets.history_pipeline.maximum_active_chunks",
                4,
                "less memory: 4 chunks in flight, measured at 250-730 MiB peak",
            );
        }
        Recipe::TransactionsFromXatu => {
            for source in &mut config.sources.history {
                let (priority, because) = match source.kind {
                    HistorySourceKind::Xatu => (
                        10,
                        "slow link: Xatu downloads less per block, and this processor reads no logs",
                    ),
                    _ => (source.priority.max(20), "slow link: after Xatu"),
                };
                if matches!(source.kind, HistorySourceKind::Xatu) || priority != source.priority {
                    source.priority = priority;
                    decisions.add(
                        &format!("sources.history.{}.priority", source.id),
                        priority,
                        because,
                    );
                }
            }
        }
        Recipe::SlowDisk => {
            budgets.history_pipeline.commit.target_writer_hold =
                HumanMilliseconds::from_milliseconds(250);
            decisions.add(
                "budgets.history_pipeline.commit.target_writer_hold",
                "250ms",
                "slow disk: keep batches large; a lower target shrinks them",
            );
        }
    }
}

fn disk(
    requirements: &Requirements,
    config: &mut Config,
    decisions: &mut Decisions,
    warnings: &mut Vec<String>,
) {
    let Some(disk) = requirements.limits.disk.map(HumanBytes::bytes) else {
        return;
    };
    let store = disk / 4 * 3;
    config.budgets.store.maximum_physical_bytes = HumanBytes::from_bytes(store);
    decisions.add(
        "budgets.store.maximum_physical_bytes",
        store,
        format!("{}: three quarters of your disk for the store", size(store)),
    );
    if config.budgets.temporary_disk_bytes > disk / 4 {
        config.budgets.temporary_disk_bytes = disk / 4;
        decisions.add(
            "budgets.temporary_disk_bytes",
            disk / 4,
            format!("{}: the last quarter for temporary files", size(disk / 4)),
        );
    }
    if requirements.keep == Keep::Everything {
        warnings.push(format!(
            "keeping everything grows with the chain, and the store may hold at most {} GiB on this disk; keep a window if it fills",
            store / GIB
        ));
    }
}

fn size(bytes: u64) -> String {
    if bytes >= GIB {
        let tenths = bytes * 10 / GIB;
        if tenths.is_multiple_of(10) {
            format!("{} GiB", tenths / 10)
        } else {
            format!("{}.{} GiB", tenths / 10, tenths % 10)
        }
    } else {
        format!("{} MiB", bytes / MIB)
    }
}

fn window_days(requirements: &Requirements) -> u64 {
    requirements
        .window_days
        .unwrap_or(DEFAULT_WINDOW_DAYS)
        .max(1)
}

fn heading(requirements: &Requirements) -> String {
    let what = match &requirements.processor {
        ProcessorChoice::Blocks => "block summaries".to_owned(),
        ProcessorChoice::Uniswap { markets } => format!("Uniswap {}", markets.join(", ")),
        ProcessorChoice::Events { address, .. } => format!("events of {address}"),
        ProcessorChoice::Custom { id, .. } => format!("processor {id}"),
    };
    let keep = match requirements.keep {
        Keep::Everything => "everything".to_owned(),
        Keep::Window => format!("the last {} days", window_days(requirements)),
        Keep::Latest => "the latest value per key".to_owned(),
        Keep::Result => "one result".to_owned(),
        Keep::Stream => "nothing; a stream to your database".to_owned(),
        Keep::Live => "nothing; a live stream".to_owned(),
    };
    let follow = match requirements.follow {
        Follow::Head => "the live head",
        Follow::Finalized => "finalized blocks",
        Follow::None => "no live blocks",
    };
    let limits = &requirements.limits;
    let mut bounds = Vec::new();
    if let Some(memory) = limits.memory {
        bounds.push(format!("memory {}", size(memory.bytes())));
    }
    if let Some(disk) = limits.disk {
        bounds.push(format!("disk {}", size(disk.bytes())));
    }
    if let Some(mbit) = limits.bandwidth_mbit {
        bounds.push(format!("link {mbit} Mbit/s"));
    }
    format!(
        "# Built by leani for: {what}; keep {keep}; follow {follow}{}.\n# A commented setting was derived from these needs; the rest are leani's defaults.\n\n",
        if bounds.is_empty() {
            String::new()
        } else {
            format!("; {}", bounds.join(", "))
        }
    )
}

fn annotate(toml: &str, decisions: &[Decision]) -> String {
    let mut section = String::new();
    let mut table_id: Option<String> = None;
    let mut annotated = std::collections::BTreeSet::new();
    let mut out = String::with_capacity(toml.len() * 2);
    for line in toml.lines() {
        let trimmed = line.trim();
        let header = trimmed
            .strip_prefix("[[")
            .and_then(|header| header.strip_suffix("]]"))
            .or_else(|| {
                trimmed
                    .strip_prefix('[')
                    .and_then(|header| header.strip_suffix(']'))
            });
        if let Some(header) = header {
            header.trim().clone_into(&mut section);
            table_id = None;
        } else if let Some((key, value)) = trimmed.split_once(" = ") {
            if key == "id" {
                table_id = Some(value.trim_matches('"').to_owned());
            }
            let path = match (section.as_str(), &table_id) {
                ("", _) => key.to_owned(),
                ("sources.history", Some(id)) => format!("sources.history.{id}.{key}"),
                (section, _) => format!("{section}.{key}"),
            };
            if let Some(decision) = decisions.iter().find(|decision| decision.setting == path)
                && annotated.insert(path)
            {
                out.push_str(line);
                out.push_str("  # ");
                out.push_str(&decision.because);
                out.push('\n');
                continue;
            }
        }
        out.push_str(line);
        out.push('\n');
    }
    out
}

fn next_steps(requirements: &Requirements, instance: &str, start: Option<u64>) -> Vec<String> {
    let mut next = vec!["leani doctor".to_owned()];
    if requirements.keep == Keep::Result || requirements.follow == Follow::None {
        next.push(format!(
            "leani backfill --processor {instance} --from-block {} --to-block <TO>",
            start.map_or_else(|| "<FROM>".to_owned(), |start| start.to_string())
        ));
    } else {
        next.push("leani serve".to_owned());
    }
    next
}

#[cfg(test)]
mod tests {
    use super::*;

    fn finality() -> FinalityConfig {
        let mut finality = Config::ethereum_mainnet_defaults()
            .expect("defaults")
            .finality;
        finality.checkpoint =
            "0x009ad7e68ebe7c01d494f6a30af39722fa3a7ff770ec490a04c6a8c10429126a".to_owned();
        finality.checkpoint_slot = 15_429_088;
        finality
    }

    fn requirements(json: serde_json::Value) -> Requirements {
        serde_json::from_value(json).expect("requirements")
    }

    fn built(json: serde_json::Value, head: Option<u64>) -> (Build, Config) {
        let registry = ProcessorRegistry::standard();
        let build = build(
            &requirements(json),
            &BuildContext {
                registry: &registry,
                finality: Some(finality()),
                head,
                data_dir: None,
            },
        )
        .expect("build");
        let config: Config =
            toml::from_str(&build.toml).unwrap_or_else(|error| panic!("{error}\n{}", build.toml));
        assert!(
            build.errors.is_empty(),
            "{:?}\n{}",
            build.errors,
            build.toml
        );
        (build, config)
    }

    fn decided<'a>(build: &'a Build, setting: &str) -> &'a Decision {
        build
            .decisions
            .iter()
            .find(|decision| decision.setting == setting)
            .unwrap_or_else(|| panic!("no decision for {setting}: {:?}", build.decisions))
    }

    #[test]
    fn a_month_of_events_on_a_small_host_on_a_slow_link() {
        let (build, config) = built(
            serde_json::json!({
                "processor": {
                    "preset": "events",
                    "address": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
                    "events": ["Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)"],
                },
                "keep": "window",
                "windowDays": 30,
                "follow": "finalized",
                "limits": { "memory": "512MiB", "bandwidthMbit": 100 },
            }),
            None,
        );
        let processor = &config.processors[0];
        assert_eq!(processor.id, "evm-events");
        assert_eq!(
            processor.output.window.and_then(|window| window.max_blocks),
            Some(216_000)
        );
        assert!(matches!(
            processor.publish,
            Some(crate::config::PublishMode::Finalized)
        ));
        assert_eq!(config.budgets.source_concurrency, 4);
        assert_eq!(config.budgets.history_pipeline.maximum_active_chunks, 2);
        assert_eq!(build.tuning, [Recipe::LimitedMemory]);
        // A log processor keeps eraE first even on a slow link.
        let first = config
            .sources
            .history
            .iter()
            .min_by_key(|source| source.priority);
        assert_eq!(first.map(|source| source.id.as_str()), Some("erae"));
        assert!(
            decided(&build, "sources.history.erae.priority")
                .because
                .contains("logs"),
            "{:?}",
            build.decisions
        );
        assert!(
            build.toml.contains("max_blocks = 216000  # "),
            "{}",
            build.toml
        );
    }

    #[test]
    fn a_transactions_processor_on_a_slow_link_reads_xatu_first() {
        let (build, config) = built(
            serde_json::json!({
                "processor": {
                    "preset": "custom",
                    "id": "transaction-stats",
                    "version": "1.0.0",
                    "settings": {
                        "from": "0x1111111111111111111111111111111111111111",
                        "to": "0x2222222222222222222222222222222222222222",
                        "complete_from_start": true,
                    },
                },
                "keep": "latest",
                "follow": "finalized",
                "history": { "fromBlock": 15_537_394 },
                "limits": { "bandwidthMbit": 50 },
            }),
            None,
        );
        let first = config
            .sources
            .history
            .iter()
            .min_by_key(|source| source.priority);
        assert_eq!(first.map(|source| source.id.as_str()), Some("xatu"));
        assert!(build.tuning.contains(&Recipe::TransactionsFromXatu));
        assert!(
            build.toml.contains("priority = 10  # slow link: Xatu")
                && build
                    .toml
                    .contains("priority = 20  # slow link: after Xatu"),
            "{}",
            build.toml
        );
        assert_eq!(config.processors[0].start_block, 15_537_394);
        // transaction-stats is ordered: `latest` keeps its one total.
        assert!(build.warnings.is_empty(), "{:?}", build.warnings);
    }

    #[test]
    fn a_log_processor_that_asks_for_xatu_first_is_warned() {
        let (build, _) = built(
            serde_json::json!({
                "processor": {
                    "preset": "events",
                    "address": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                    "events": ["Transfer(address indexed from, address indexed to, uint256 value)"],
                },
                "follow": "finalized",
                "limits": { "bandwidthMbit": 50 },
                "tuning": ["transactionsFromXatu"],
            }),
            None,
        );
        assert!(
            build
                .warnings
                .iter()
                .any(|warning| warning.contains("only eraE proves complete")),
            "{:?}",
            build.warnings
        );
        assert!(!build.toml.contains("eraE stays first"), "{}", build.toml);
    }

    #[test]
    fn a_stream_to_the_apps_database_keeps_nothing_until_it_confirms() {
        let (build, config) = built(
            serde_json::json!({ "processor": { "preset": "blocks" }, "keep": "stream" }),
            None,
        );
        let processor = &config.processors[0];
        assert_eq!(
            processor.output.mode,
            leani_processor_api::OutputPolicyMode::None
        );
        assert_eq!(
            processor.delivery.mode,
            leani_processor_api::DeliveryPolicyMode::UntilAcknowledged
        );
        assert!(
            processor
                .delivery
                .consumers
                .iter()
                .any(|consumer| consumer.required)
        );
        assert!(matches!(
            processor.history_control,
            crate::config::ProcessorHistoryControl::ApplicationSubscriptions
        ));
        assert!(
            decided(&build, "processors.delivery.mode")
                .because
                .contains("confirm")
        );
    }

    #[test]
    fn history_only_runs_without_a_live_lane_or_finality() {
        let registry = ProcessorRegistry::standard();
        let build = build(
            &requirements(serde_json::json!({
                "processor": { "preset": "blocks" },
                "keep": "result",
                "follow": "none",
                "history": { "fromBlock": 20_000_000 },
            })),
            &BuildContext {
                registry: &registry,
                finality: None,
                head: None,
                data_dir: None,
            },
        )
        .expect("build");
        let config: Config = toml::from_str(&build.toml).expect("configuration");
        assert!(build.errors.is_empty(), "{:?}", build.errors);
        assert!(matches!(
            config.sources.live.kind,
            crate::config::LiveSourceKind::Disabled
        ));
        assert!(matches!(
            config.finality.kind,
            crate::config::FinalitySourceKind::Disabled
        ));
        assert_eq!(config.processors[0].publish, None);
        assert_eq!(
            config.processors[0].delivery.emit,
            crate::config::DeliveryEmit::JobResult
        );
        assert!(
            build
                .next
                .iter()
                .any(|step| step.starts_with("leani backfill")),
            "{:?}",
            build.next
        );
    }

    #[test]
    fn history_in_days_needs_the_head() {
        let registry = ProcessorRegistry::standard();
        let days = requirements(serde_json::json!({
            "processor": { "preset": "blocks" },
            "history": { "lastDays": 7 },
        }));
        let refused = build(
            &days,
            &BuildContext {
                registry: &registry,
                finality: Some(finality()),
                head: None,
                data_dir: None,
            },
        )
        .expect_err("no head");
        assert!(refused.contains("fromBlock"), "{refused}");
        let (_, config) = built(
            serde_json::json!({ "processor": { "preset": "blocks" }, "history": { "lastDays": 7 } }),
            Some(26_000_000),
        );
        assert_eq!(config.processors[0].start_block, 26_000_000 - 7 * 7_200);
        assert!(matches!(
            config.processors[0].history_mode,
            crate::config::ProcessorHistoryMode::Automatic
        ));
    }

    #[test]
    fn chosen_recipes_replace_the_ones_the_limits_call_for() {
        let (build, config) = built(
            serde_json::json!({
                "processor": { "preset": "uniswap", "markets": ["ETH/USDC"] },
                "limits": { "memory": "512MiB" },
                "tuning": ["lessMemory"],
            }),
            None,
        );
        assert_eq!(build.tuning, [Recipe::LessMemory]);
        assert_eq!(config.budgets.source_concurrency, 8);
        assert_eq!(config.budgets.history_pipeline.maximum_active_chunks, 4);
    }

    #[test]
    fn the_api_hook_refuses_malformed_requirements() {
        let registry = ProcessorRegistry::standard();
        let refused = build_json(
            &serde_json::json!({ "processor": { "preset": "blocks" }, "keep": "forever" }),
            None,
            &registry,
            &finality(),
        )
        .expect_err("unknown keep");
        assert!(refused.starts_with("requirements:"), "{refused}");
        let built = build_json(
            &serde_json::json!({ "processor": { "preset": "blocks" } }),
            Some(1),
            &registry,
            &finality(),
        )
        .expect("build");
        assert!(
            built["toml"]
                .as_str()
                .is_some_and(|toml| toml.contains("[[processors]]"))
        );
    }

    #[test]
    fn every_keep_follow_and_history_builds_a_valid_configuration() {
        for keep in ["everything", "window", "latest", "result", "stream", "live"] {
            for follow in ["head", "finalized", "none"] {
                for history in [None, Some(serde_json::json!({ "fromBlock": 20_000_000 }))] {
                    let (build, _) = built(
                        serde_json::json!({
                            "processor": { "preset": "blocks" },
                            "keep": keep,
                            "windowDays": 3,
                            "follow": follow,
                            "history": history,
                            "limits": { "memory": "4GiB", "disk": "40GiB", "bandwidthMbit": 1000 },
                        }),
                        None,
                    );
                    assert!(
                        build
                            .toml
                            .lines()
                            .any(|line| !line.starts_with('#') && line.contains("  # ")),
                        "{keep}/{follow}/{history:?}: no setting carries its reason\n{}",
                        build.toml
                    );
                }
            }
        }
    }
}
