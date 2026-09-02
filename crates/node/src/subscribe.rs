//! Lightweight live market subscriptions backed by native processors.

use std::{
    collections::{BTreeMap, HashSet},
    fs,
    io::{self, IsTerminal, Write},
    net::{Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    str::FromStr,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use alloy_primitives::{Address as AlloyAddress, I256, U256, U512};
use anyhow::{Context, Result, bail};
use futures::{StreamExt as _, future::join_all};
use leani_primitives::{
    Address, BlockFrame, BlockHash, BlockNumber, BlockRange, BlockRef, ChainId, Finality,
};
use leani_processor_block_summary::{
    BLOCK_SUMMARY_KIND, BLOCK_SUMMARY_VERSION, BlockSummaryEntity,
};
use leani_processor_uniswap::{PoolPriceEntity, UNISWAP_OBSERVATIONS_VERSION, UniswapPriceDelta};
use leani_source_api::{
    ChainEvent, ChainEventStream, DataRequest, FieldProjection, FilterSet, LiveSource as _,
    LiveStart, NetworkTelemetrySnapshot, SourceBudget, SourceError, VerificationPolicy,
};
use leani_store_sqlite::{ChangeDirection, ChangeRecord, SqliteStore};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use url::Url;

use crate::{
    cli::{
        SubscribeFinality, SubscribeFinalitySource, SubscribeFormat, SubscribeMode,
        SubscribeProtocol,
    },
    config::{Config, FinalitySourceKind},
    local_state,
    process::{Exit, configured_store_config, spawn_embedded_network_runtime},
    processors::ProcessorRegistry,
    uniswap_markets::{MARKET_CATALOG, Market, Token, processor_config, resolve_markets},
};

const BLOCK_PROCESSOR_INSTANCE: &str = "cli-ethereum-blocks";
const UNISWAP_PROCESSOR_INSTANCE: &str = "cli-uniswap-v3-prices";
const CHECKPOINT_CACHE_MAX_AGE: Duration = Duration::from_hours(12);
const LOCALLY_VERIFIED_CHECKPOINT_MAX_AGE: Duration = Duration::from_hours(13 * 24);
const MAINNET_SLOT_SECONDS: u64 = 12;
const OPTIMISTIC_UPDATE_MAX_AGE: Duration = Duration::from_secs(90);
const FINALIZED_UPDATE_MAX_AGE: Duration = Duration::from_mins(30);
const PRICE_PRECISION: usize = 8;
const STARTUP_UPDATE_SCAN_LIMIT: usize = 10_000;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum ObservationKey {
    Block(BlockHash),
    Uniswap(Address, BlockHash, u32),
}

#[derive(Clone, Debug)]
enum SubscriptionItem {
    Block(BlockSummaryEntity),
    Uniswap {
        market: Market,
        entity: PoolPriceEntity,
    },
}

impl SubscriptionItem {
    fn scope_key(&self) -> String {
        match self {
            Self::Block(_) => "blocks".to_owned(),
            Self::Uniswap { entity, .. } => entity.pool.to_string(),
        }
    }

    fn same_scope(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Block(_), Self::Block(_)) => true,
            (Self::Uniswap { entity: left, .. }, Self::Uniswap { entity: right, .. }) => {
                left.pool == right.pool
            }
            _ => false,
        }
    }

    const fn block_number(&self) -> BlockNumber {
        match self {
            Self::Block(entity) => entity.block_number,
            Self::Uniswap { entity, .. } => entity.block_number,
        }
    }

    const fn block_hash(&self) -> BlockHash {
        match self {
            Self::Block(entity) => entity.block_hash,
            Self::Uniswap { entity, .. } => entity.block_hash,
        }
    }

    const fn item_index(&self) -> u32 {
        match self {
            Self::Block(_) => 0,
            Self::Uniswap { entity, .. } => entity.log_index,
        }
    }
}

pub(crate) struct SubscribeOptions {
    pub protocol: SubscribeProtocol,
    pub targets: Vec<String>,
    pub format: SubscribeFormat,
    pub mode: SubscribeMode,
    pub endpoint: Option<Url>,
    pub processor: String,
    pub token: Option<String>,
    pub finality: SubscribeFinality,
    pub finality_source: SubscribeFinalitySource,
    pub checkpoint_urls: Vec<Url>,
    pub checkpoint_quorum: usize,
    pub accept_checkpoint: bool,
    pub data_dir: Option<PathBuf>,
    pub once: bool,
    pub requested_config: Option<PathBuf>,
    pub working_directory: PathBuf,
}

pub(crate) struct ResetSubscriptionOptions {
    pub protocol: SubscribeProtocol,
    pub targets: Vec<String>,
    pub finality: SubscribeFinality,
    pub data_dir: Option<PathBuf>,
    pub confirmed: bool,
    pub requested_config: Option<PathBuf>,
    pub working_directory: PathBuf,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum CheckpointTrust {
    ProviderQuorum,
    LocallyVerified,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct CachedCheckpoint {
    schema: String,
    root: String,
    slot: u64,
    execution_block_hash: Option<String>,
    trust: CheckpointTrust,
    accepted_providers: Vec<String>,
    beacon_api_endpoints: Vec<String>,
    updated_at_unix_seconds: u64,
}

pub(crate) struct InitializedCheckpoint {
    pub root: String,
    pub slot: u64,
    pub beacon_api_endpoints: Vec<Url>,
}

#[derive(Clone, Debug)]
struct ProviderCheckpoint {
    provider: Url,
    root: String,
    slot: u64,
    beacon_api: bool,
}

#[derive(Clone, Debug)]
struct CheckpointQuorum {
    root: String,
    slot: u64,
    agreeing_providers: Vec<Url>,
    beacon_api_endpoints: Vec<Url>,
    attempted: usize,
}

#[derive(Debug, Deserialize)]
struct BeaconHeaderResponse {
    data: BeaconHeaderData,
}

#[derive(Debug, Deserialize)]
struct BeaconHeaderData {
    root: String,
    header: BeaconHeader,
}

#[derive(Debug, Deserialize)]
struct BeaconHeader {
    message: BeaconHeaderMessage,
}

#[derive(Debug, Deserialize)]
struct BeaconHeaderMessage {
    slot: String,
}

#[derive(Debug, Deserialize)]
struct CheckpointzResponse {
    data: CheckpointzData,
}

#[derive(Debug, Deserialize)]
struct CheckpointzData {
    slots: Vec<CheckpointzSlot>,
}

#[derive(Debug, Deserialize)]
struct CheckpointzSlot {
    slot: String,
    block_root: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct AttachedPoolPrice {
    pool: String,
    kind: String,
    reserve0: Option<String>,
    reserve1: Option<String>,
    amount0: Option<String>,
    amount1: Option<String>,
    sqrt_price_x96: Option<String>,
    block_number: u64,
    block_hash: String,
    log_index: u32,
    finality: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AttachedBlockSummary {
    chain_id: u64,
    block_number: u64,
    block_hash: String,
    parent_hash: String,
    timestamp: u64,
    gas_limit: Option<u64>,
    gas_used: Option<u64>,
    base_fee_per_gas: Option<String>,
    blob_gas_used: Option<u64>,
    excess_blob_gas: Option<u64>,
    transaction_count: Option<u32>,
    size_bytes: Option<u64>,
    finality: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AttachedLatestObservation {
    data: AttachedPoolPrice,
    timestamp: u64,
}

#[derive(Clone, Debug, Deserialize)]
struct AttachedLatestBlock {
    data: AttachedBlockSummary,
}

#[derive(Debug)]
struct RenderedPrice<'a> {
    market: &'a Market,
    price: String,
    base_volume: Option<String>,
    block_number: u64,
    block_hash: String,
    timestamp: u64,
    log_index: u32,
    finality: String,
    operation: String,
    sqrt_price_x96: String,
    amount0: Option<String>,
    amount1: Option<String>,
    sequence: Option<String>,
}

#[derive(Debug)]
struct RenderedBlockSummary {
    chain_id: u64,
    block_number: u64,
    block_hash: String,
    parent_hash: String,
    timestamp: u64,
    gas_limit: Option<u64>,
    gas_used: Option<u64>,
    base_fee_wei: Option<String>,
    blob_gas_used: Option<u64>,
    excess_blob_gas: Option<u64>,
    transaction_count: Option<u32>,
    size_bytes: Option<u64>,
    finality: String,
    operation: String,
    sequence: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct AttachedEnvelope {
    sequence: String,
    cursor: String,
    operation: String,
    block: Value,
    finality: String,
    kind: String,
    data: Option<Value>,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

#[derive(Debug)]
struct ParsedSseEvent {
    event: Option<String>,
    data: Option<String>,
}

#[derive(Clone, Copy, Debug, Default)]
struct SseRenderOutcome {
    stop: bool,
    observed_change: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ChangeHead {
    cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ConfiguredPoolsResponse {
    data: Vec<ConfiguredPool>,
}

#[derive(Debug, Deserialize)]
struct ConfiguredPool {
    address: String,
}

pub(crate) async fn subscribe(
    options: SubscribeOptions,
    registry: &ProcessorRegistry,
) -> Result<Exit> {
    let markets = subscription_markets(options.protocol, &options.targets)?;
    let configured_path = local_state::configured_path(
        options.requested_config.as_deref(),
        &options.working_directory,
    );
    let inferred_endpoint = infer_endpoint(configured_path.as_deref())?;
    let endpoint = options.endpoint.clone().or(inferred_endpoint);

    match options.mode {
        SubscribeMode::Client => {
            let endpoint = endpoint
                .context("client mode needs --endpoint or a local leani.toml with api.bind")?;
            subscribe_attached(&options, &markets, endpoint).await
        }
        SubscribeMode::Auto if options.endpoint.is_some() => {
            subscribe_attached(
                &options,
                &markets,
                endpoint.expect("explicit endpoint is present"),
            )
            .await
        }
        SubscribeMode::Auto if endpoint_is_reachable(&options, endpoint.as_ref()).await => {
            subscribe_attached(
                &options,
                &markets,
                endpoint.expect("reachable endpoint is present"),
            )
            .await
        }
        SubscribeMode::Auto | SubscribeMode::Embedded => {
            Box::pin(subscribe_embedded(
                &options,
                &markets,
                configured_path.as_deref(),
                registry,
            ))
            .await
        }
    }
}

pub(crate) fn reset_subscription(options: &ResetSubscriptionOptions) -> Result<Exit> {
    let markets = subscription_markets(options.protocol, &options.targets)?;
    let configured_path = local_state::configured_path(
        options.requested_config.as_deref(),
        &options.working_directory,
    );
    let data_dir = subscription_data_dir_for(
        options.data_dir.as_deref(),
        options.protocol,
        &markets,
        configured_path.as_deref(),
        options.finality,
        &options.working_directory,
    )?;
    reset_subscription_directory(&data_dir, options.confirmed)?;
    Ok(Exit::Success)
}

fn subscription_markets(protocol: SubscribeProtocol, targets: &[String]) -> Result<Vec<Market>> {
    match protocol {
        SubscribeProtocol::Blocks => {
            if !targets.is_empty() {
                bail!("the blocks feed does not take market arguments");
            }
            Ok(Vec::new())
        }
        SubscribeProtocol::UniswapV3 => {
            if targets.is_empty() {
                bail!("the uniswap-v3 feed requires at least one market");
            }
            resolve_markets(targets)
        }
    }
}

pub(crate) async fn initialize_checkpoint(
    checkpoint_urls: Vec<Url>,
    checkpoint_quorum: usize,
    accept_checkpoint: bool,
    data_dir: &Path,
) -> Result<InitializedCheckpoint> {
    let checkpoint = trusted_checkpoint(
        &SubscribeOptions {
            protocol: SubscribeProtocol::UniswapV3,
            targets: Vec::new(),
            format: SubscribeFormat::Pretty,
            mode: SubscribeMode::Embedded,
            endpoint: None,
            processor: "uniswap-observations".to_owned(),
            token: None,
            finality: SubscribeFinality::Optimistic,
            finality_source: SubscribeFinalitySource::BeaconApi,
            checkpoint_urls,
            checkpoint_quorum,
            accept_checkpoint,
            data_dir: Some(data_dir.to_path_buf()),
            once: false,
            requested_config: None,
            working_directory: data_dir.to_path_buf(),
        },
        data_dir,
        true,
        false,
    )
    .await?;
    let beacon_api_endpoints = checkpoint
        .beacon_api_endpoints
        .iter()
        .map(|endpoint| Url::parse(endpoint).context("parse Beacon API endpoint"))
        .collect::<Result<Vec<_>>>()?;
    Ok(InitializedCheckpoint {
        root: checkpoint.root,
        slot: checkpoint.slot,
        beacon_api_endpoints,
    })
}

fn infer_endpoint(path: Option<&Path>) -> Result<Option<Url>> {
    let Some(path) = path else {
        return Ok(None);
    };
    let config = Config::load(path)?;
    let bind = config.api.bind;
    let ip = if bind.ip().is_unspecified() {
        match bind {
            SocketAddr::V4(_) => std::net::IpAddr::V4(Ipv4Addr::LOCALHOST),
            SocketAddr::V6(_) => std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST),
        }
    } else {
        bind.ip()
    };
    Url::parse(&format!("http://{}/", SocketAddr::new(ip, bind.port())))
        .map(Some)
        .context("construct local API endpoint")
}

async fn endpoint_is_reachable(options: &SubscribeOptions, endpoint: Option<&Url>) -> bool {
    let Some(endpoint) = endpoint else {
        return false;
    };
    let Ok(url) = endpoint.join("health/live") else {
        return false;
    };
    let request = authorized(
        reqwest::Client::new()
            .get(url)
            .timeout(Duration::from_millis(800)),
        options.token.as_deref(),
    );
    request
        .send()
        .await
        .is_ok_and(|response| response.status().is_success())
}

#[allow(clippy::too_many_lines)]
async fn subscribe_embedded(
    options: &SubscribeOptions,
    markets: &[Market],
    config_path: Option<&Path>,
    registry: &ProcessorRegistry,
) -> Result<Exit> {
    let data_dir = subscription_data_dir(options, markets, config_path)?;
    fs::create_dir_all(&data_dir)
        .with_context(|| format!("create subscription data directory {}", data_dir.display()))?;
    let data_dir_lock = local_state::lock_runtime_directory(&data_dir)?;
    let config = embedded_config(options, markets, config_path, &data_dir).await?;
    if let Some(peer_cache) = merge_local_execution_peer_caches(
        config_path,
        &data_dir,
        config.sources.live.peer_cache_max_entries,
    )? {
        if peer_cache.imported == 0 {
            eprintln!(
                "leani: loaded {} cached execution peer candidates",
                peer_cache.total,
            );
        } else {
            eprintln!(
                "leani: loaded {} cached execution peer candidates ({} imported from other local Mainnet contexts)",
                peer_cache.total, peer_cache.imported,
            );
        }
    }
    let processors = registry.instantiate_all(&config)?;
    let processor = processors
        .first()
        .cloned()
        .context("subscription processor was not instantiated")?;
    let store = SqliteStore::open(configured_store_config(
        &config,
        data_dir.join("leani.sqlite"),
    ))
    .await?;
    store.register_processor(processor.descriptor()).await?;
    let mut after = store
        .change_bounds(processor.descriptor())
        .await?
        .map_or(0, |bounds| bounds.latest);
    let mut runtime =
        spawn_embedded_network_runtime(config.clone(), store.clone(), processors, data_dir_lock)?;
    let preview_requirement = processor
        .descriptor()
        .requirements
        .first()
        .context("subscription processor omitted its data requirement")?;
    let preview_request =
        processor_data_request(preview_requirement, BlockRange::single(BlockNumber(0)));
    let optimistic_snapshot = optimistic_head_snapshot(
        runtime.verified_anchor.clone(),
        runtime.execution_source.clone(),
        preview_request.clone(),
        embedded_snapshot_budget(&config),
        runtime.cancellation_token(),
    );
    tokio::pin!(optimistic_snapshot);

    match options.protocol {
        SubscribeProtocol::Blocks => eprintln!(
            "leani: embedded block-summary processor active; bootstrapping verified Ethereum blocks..."
        ),
        SubscribeProtocol::UniswapV3 => eprintln!(
            "leani: embedded Uniswap V3 processor active for {}; bootstrapping verified Ethereum data...",
            markets
                .iter()
                .map(|market| market.symbol)
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
    eprintln!(
        "leani: a cold start verifies consensus, then discovers execution peers; reusable state is cached for later runs"
    );
    let startup_started = Instant::now();
    let mut next_startup_status = Duration::from_secs(15);
    let mut announced_ready = false;
    let mut snapshot_pending =
        options.finality == SubscribeFinality::Optimistic && options.format != SubscribeFormat::Raw;
    // An optimistic preview can outrun the durable runtime's anchored overlap.
    // Retain both its identities and ordered block context through readiness so
    // later overlap commits confirm the preview instead of regressing stdout.
    let mut previewed = HashSet::<ObservationKey>::new();
    let mut preview_blocks = BTreeMap::<u64, (BlockRef, Vec<SubscriptionItem>)>::new();
    let mut preview_stream = None::<ChainEventStream>;
    let outcome = async {
    loop {
        if runtime.is_finished() {
            bail!("embedded network runtime stopped before the subscription completed");
        }
        if !announced_ready && startup_started.elapsed() >= next_startup_status {
            let status = runtime.execution_source.network_status();
            if runtime.verified_anchor.borrow().is_none() {
                eprintln!(
                    "leani: still verifying a consensus anchor before execution peer discovery; Ctrl-C stops immediately"
                );
            } else if status.connected_peer_slots == 0 {
                eprintln!(
                    "leani: still searching for a usable Ethereum peer ({}); Ctrl-C stops immediately",
                    format_peer_search_status(&status),
                );
            } else {
                eprintln!(
                    "leani: {}; validating a current serving head...",
                    format_peer_search_status(&status),
                );
            }
            next_startup_status = next_startup_status.saturating_add(Duration::from_secs(30));
        }
        if !announced_ready && runtime.readiness.is_ready() {
            // The live runtime validates and catches up a finalized overlap
            // before readiness. Preserve only the newest fresh observation
            // per requested market from that work: replaying the whole overlap
            // is noisy, while discarding it can leave an otherwise successful
            // first run blank until the next swap.
            let ready_after = store
                .change_bounds(processor.descriptor())
                .await?
                .map_or(after, |bounds| bounds.latest);
            let startup_records = store
                .changes(
                    processor.descriptor(),
                    ChainId(config.chain.chain_id),
                    ready_after.saturating_sub(
                        u64::try_from(STARTUP_UPDATE_SCAN_LIMIT)
                            .expect("startup scan limit fits u64"),
                    ),
                    STARTUP_UPDATE_SCAN_LIMIT,
                )
                .await?;
            let startup_items = latest_fresh_items(
                options.protocol,
                markets,
                startup_records,
                options.finality,
                unix_seconds(),
            )?;
            // Readiness means the anchored lane is operational, not that it
            // has already committed a displayable update. Keep racing the
            // current-head snapshot until either it resolves or anchored work
            // produces the first fresh item. Cancelling it here made a ready
            // but still-empty store wait through the finalized overlap.
            if !startup_items.is_empty() || !snapshot_pending {
                snapshot_pending = false;
                preview_stream = None;
                after = ready_after;
                announced_ready = true;
                if startup_items.is_empty() {
                    eprintln!(
                        "leani: execution peers and verified finality are connected; waiting for the next matching update..."
                    );
                } else {
                    eprintln!("leani: live execution and verified finality are ready");
                }
                for (item, record) in startup_items {
                    match reconcile_preview_handoff(
                        &mut preview_blocks,
                        &mut previewed,
                        &item,
                        record.direction,
                    ) {
                        PreviewHandoffAction::Suppress => continue,
                        PreviewHandoffAction::Render { reverted } => {
                            render_preview_reverts(options.format, &reverted)?;
                        }
                    }
                    render_local_item(
                        options.format,
                        &item,
                        &record,
                        processor.as_ref(),
                        store.epoch(),
                    )?;
                    if options.once {
                        return Ok(Exit::Success);
                    }
                }
            }
        }
        if !announced_ready {
            tokio::select! {
                result = tokio::signal::ctrl_c() => {
                    result.context("install Ctrl-C handler")?;
                    return Ok(Exit::Success);
                }
                changed = runtime.verified_anchor.changed() => {
                    changed.context("verified anchor channel closed")?;
                    persist_current_verified_anchor(&runtime, &data_dir, &config)?;
                }
                snapshot = &mut optimistic_snapshot, if snapshot_pending => {
                    snapshot_pending = false;
                    match snapshot {
                        Ok(frame) => {
                            let items = optimistic_items(
                                options.protocol,
                                markets,
                                processor.as_ref(),
                                &frame,
                            )
                            .await?;
                            let rendered = render_optimistic_items(
                                options.format,
                                &items,
                                frame.block,
                                "apply",
                            )?;
                            previewed.extend(rendered.iter().copied());
                            retain_preview_block(
                                &mut preview_blocks,
                                &mut previewed,
                                frame.block,
                                items,
                            );
                            if options.once && !rendered.is_empty() {
                                return Ok(Exit::Success);
                            }
                            match runtime.execution_source.subscribe(
                                DataRequest {
                                    range: BlockRange::single(frame.block.number),
                                    ..preview_request.clone()
                                },
                                LiveStart::RetainedCanonical {
                                    canonical: vec![frame.block],
                                },
                                embedded_snapshot_budget(&config),
                                runtime.cancellation_token(),
                            ).await {
                                Ok(stream) => preview_stream = Some(stream),
                                Err(error) => tracing::debug!(
                                    %error,
                                    "optimistic head preview could not follow the live tail; anchored startup continues"
                                ),
                            }
                        }
                        Err(error) => tracing::debug!(
                            %error,
                            "optimistic head preview was unavailable; anchored startup continues"
                        ),
                    }
                }
                event = next_preview_event(&mut preview_stream) => {
                    let Some(event) = event else {
                        preview_stream = None;
                        continue;
                    };
                    let mut rendered_any = false;
                    match event {
                        Ok(ChainEvent::Block(frame)) => {
                            let items = optimistic_items(
                                options.protocol,
                                markets,
                                processor.as_ref(),
                                &frame,
                            ).await?;
                            let rendered = render_optimistic_items(
                                options.format,
                                &items,
                                frame.block,
                                "apply",
                            )?;
                            rendered_any = !rendered.is_empty();
                            previewed.extend(rendered);
                            retain_preview_block(
                                &mut preview_blocks,
                                &mut previewed,
                                frame.block,
                                items,
                            );
                        }
                        Ok(ChainEvent::Reorg { reverted, applied }) => {
                            for block in reverted {
                                if let Some(items) = revert_preview_block(
                                    &mut preview_blocks,
                                    &mut previewed,
                                    block,
                                ) {
                                    let reverted = render_optimistic_items(
                                        options.format,
                                        &items,
                                        block,
                                        "revert",
                                    )?;
                                    debug_assert!(reverted
                                        .iter()
                                        .all(|key| !previewed.contains(key)));
                                }
                            }
                            for frame in applied {
                                let items = optimistic_items(
                                    options.protocol,
                                    markets,
                                    processor.as_ref(),
                                    &frame,
                                ).await?;
                                let rendered = render_optimistic_items(
                                    options.format,
                                    &items,
                                    frame.block,
                                    "apply",
                                )?;
                                rendered_any |= !rendered.is_empty();
                                previewed.extend(rendered);
                                retain_preview_block(
                                    &mut preview_blocks,
                                    &mut previewed,
                                    frame.block,
                                    items,
                                );
                            }
                        }
                        Ok(ChainEvent::Disconnected { reason }) => tracing::debug!(
                            %reason,
                            "optimistic preview tail transiently disconnected"
                        ),
                        Ok(ChainEvent::Reset { reason, .. }) => {
                            tracing::debug!(
                                %reason,
                                "optimistic preview tail reset; anchored startup continues"
                            );
                            preview_stream = None;
                        }
                        Err(error) => {
                            tracing::debug!(
                                %error,
                                "optimistic preview tail stopped; anchored startup continues"
                            );
                            preview_stream = None;
                        }
                    }
                    if options.once && rendered_any {
                        return Ok(Exit::Success);
                    }
                }
                () = store.wait_for_delivery_changes() => {}
                () = tokio::time::sleep(Duration::from_secs(1)) => {}
            }
            continue;
        }
        let records = store
            .changes(
                processor.descriptor(),
                ChainId(config.chain.chain_id),
                after,
                256,
            )
            .await?;
        let full_batch = records.len() == 256;
        for record in records {
            after = record.cursor.sequence;
            if !update_is_fresh(options.finality, record.block.timestamp, unix_seconds()) {
                continue;
            }
            if let Some(item) = local_item(options.protocol, markets, &record)? {
                match reconcile_preview_handoff(
                    &mut preview_blocks,
                    &mut previewed,
                    &item,
                    record.direction,
                ) {
                    PreviewHandoffAction::Suppress => continue,
                    PreviewHandoffAction::Render { reverted } => {
                        render_preview_reverts(options.format, &reverted)?;
                    }
                }
                render_local_item(
                    options.format,
                    &item,
                    &record,
                    processor.as_ref(),
                    store.epoch(),
                )?;
                if options.once {
                    return Ok(Exit::Success);
                }
            }
        }
        if full_batch {
            continue;
        }
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                result.context("install Ctrl-C handler")?;
                return Ok(Exit::Success);
            }
            changed = runtime.verified_anchor.changed() => {
                changed.context("verified anchor channel closed")?;
                persist_current_verified_anchor(&runtime, &data_dir, &config)?;
            }
            () = store.wait_for_delivery_changes() => {}
            () = tokio::time::sleep(Duration::from_secs(1)) => {}
        }
    }
    }
    .await;
    let anchor_result = persist_current_verified_anchor(&runtime, &data_dir, &config);
    runtime.shutdown().await;
    anchor_result?;
    outcome
}

fn format_peer_search_status(status: &NetworkTelemetrySnapshot) -> String {
    let mut reasons = status
        .peer_lifecycle
        .disconnect_reasons
        .iter()
        .collect::<Vec<_>>();
    reasons.sort_by(|left, right| {
        right
            .count
            .cmp(&left.count)
            .then_with(|| left.reason.cmp(&right.reason))
    });
    let reasons = reasons
        .into_iter()
        .take(2)
        .map(|reason| format!("{}={}", reason.reason.as_str(), reason.count))
        .collect::<Vec<_>>();
    let reasons = if reasons.is_empty() {
        String::new()
    } else {
        format!("; {}", reasons.join(", "))
    };
    format!(
        "{} connected, {}/{} body-serving, {} candidates known; {} active sessions established, {} closed; up to {} concurrent dials{}",
        status.connected_peer_slots,
        status.body_serving_peer_slots,
        status.peer_targets.body_serving,
        status.known_peer_records,
        status.peer_lifecycle.established,
        status.peer_lifecycle.disconnected,
        status.peer_targets.max_concurrent_dials,
        reasons,
    )
}

async fn embedded_config(
    options: &SubscribeOptions,
    markets: &[Market],
    config_path: Option<&Path>,
    data_dir: &Path,
) -> Result<Config> {
    let mut config = if let Some(path) = config_path {
        Config::load(path)?
    } else {
        toml::from_str(include_str!("../../../config/example.toml"))
            .context("parse built-in node configuration")?
    };
    if config.chain.chain_id != 1 {
        bail!("built-in subscriptions currently support Ethereum mainnet only");
    }

    config.data_dir = data_dir.to_path_buf();
    config.raw_history.enabled = false;
    config.sources.live.listener_port = 0;
    config.sources.live.discovery_port = 0;
    config.sources.live.discv5_port = 0;
    config.finality.discovery_port = 0;
    let finality_kind = match options.finality_source {
        SubscribeFinalitySource::Auto
            if config_path.is_some()
                && !matches!(config.finality.kind, FinalitySourceKind::Disabled) =>
        {
            config.finality.kind
        }
        SubscribeFinalitySource::Auto | SubscribeFinalitySource::BeaconApi => {
            FinalitySourceKind::BeaconApi
        }
        SubscribeFinalitySource::P2p => FinalitySourceKind::ConsensusP2p,
    };
    let invalid_checkpoint = config.finality.checkpoint.trim().is_empty()
        || config.finality.checkpoint.starts_with("REPLACE_")
        || config.finality.checkpoint_slot == 0;
    let missing_beacon_transport = matches!(finality_kind, FinalitySourceKind::BeaconApi)
        && config.finality.endpoints.is_empty();
    config.finality.kind = finality_kind;
    if invalid_checkpoint || missing_beacon_transport {
        let checkpoint = trusted_checkpoint(
            options,
            data_dir,
            matches!(finality_kind, FinalitySourceKind::BeaconApi),
            true,
        )
        .await?;
        config.finality.checkpoint = checkpoint.root;
        config.finality.checkpoint_slot = checkpoint.slot;
        config.finality.endpoints = checkpoint
            .beacon_api_endpoints
            .iter()
            .map(|endpoint| Url::parse(endpoint).context("parse cached Beacon API endpoint"))
            .collect::<Result<Vec<_>>>()?;
    }
    match finality_kind {
        FinalitySourceKind::BeaconApi => {
            config.finality.minimum_agreement = config
                .finality
                .minimum_agreement
                .clamp(1, config.finality.endpoints.len());
        }
        FinalitySourceKind::ConsensusP2p => {
            config.finality.endpoints.clear();
            config.finality.minimum_peers = config.finality.minimum_peers.max(1);
        }
        FinalitySourceKind::Disabled => unreachable!("subscription finality is always enabled"),
    }
    config.processors = vec![subscription_processor(
        options.protocol,
        markets,
        options.finality,
    )?];
    Ok(config.validate()?.into_inner())
}

fn subscription_processor(
    protocol: SubscribeProtocol,
    markets: &[Market],
    finality: SubscribeFinality,
) -> Result<crate::config::ProcessorConfig> {
    match protocol {
        SubscribeProtocol::Blocks => Ok(crate::block_summaries::processor_config(
            BLOCK_PROCESSOR_INSTANCE,
            finality == SubscribeFinality::Finalized,
        )),
        SubscribeProtocol::UniswapV3 => processor_config(
            markets,
            UNISWAP_PROCESSOR_INSTANCE,
            finality == SubscribeFinality::Finalized,
        ),
    }
}

fn subscription_data_dir(
    options: &SubscribeOptions,
    markets: &[Market],
    config_path: Option<&Path>,
) -> Result<PathBuf> {
    subscription_data_dir_for(
        options.data_dir.as_deref(),
        options.protocol,
        markets,
        config_path,
        options.finality,
        &options.working_directory,
    )
}

fn subscription_data_dir_for(
    explicit_data_dir: Option<&Path>,
    protocol: SubscribeProtocol,
    markets: &[Market],
    config_path: Option<&Path>,
    finality: SubscribeFinality,
    working_directory: &Path,
) -> Result<PathBuf> {
    if let Some(path) = explicit_data_dir {
        return Ok(path.to_path_buf());
    }
    let root = local_state::runtime_data_dir(config_path, working_directory)?.join("subscriptions");
    let mut hasher = blake3::Hasher::new();
    match protocol {
        SubscribeProtocol::Blocks => {
            hasher.update(b"block-summary/");
            hasher.update(BLOCK_SUMMARY_VERSION.as_bytes());
        }
        SubscribeProtocol::UniswapV3 => {
            hasher.update(b"uniswap-observations/");
            hasher.update(UNISWAP_OBSERVATIONS_VERSION.as_bytes());
        }
    }
    let mut pools = markets.iter().map(|market| market.pool).collect::<Vec<_>>();
    pools.sort_unstable();
    for pool in pools {
        hasher.update(pool.as_bytes());
    }
    hasher.update(match finality {
        SubscribeFinality::Optimistic => b"optimistic",
        SubscribeFinality::Finalized => b"finalized",
    });
    Ok(root.join(&hasher.finalize().to_hex()[..16]))
}

fn reset_subscription_directory(data_dir: &Path, confirmed: bool) -> Result<bool> {
    let name = data_dir
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .context("subscription state directory has no UTF-8 identity")?;
    let parent = data_dir
        .parent()
        .and_then(Path::file_name)
        .and_then(std::ffi::OsStr::to_str);
    let derived = parent == Some("subscriptions")
        && name.len() == 16
        && name.bytes().all(|byte| byte.is_ascii_hexdigit());
    if !derived && !local_state::is_runtime_directory(data_dir) {
        bail!(
            "refusing to reset directory without Leani runtime identity {}",
            data_dir.display()
        );
    }
    if !data_dir.exists() {
        eprintln!(
            "leani: no embedded subscription state exists at {}",
            data_dir.display()
        );
        return Ok(false);
    }
    if !data_dir.is_dir() {
        bail!(
            "subscription state path is not a directory: {}",
            data_dir.display()
        );
    }

    eprintln!("leani: embedded subscription cold-start reset");
    eprintln!("  directory: {}", data_dir.display());
    eprintln!(
        "  removes:   checkpoint, feed-local peer/quality caches, P2P identity, and SQLite state"
    );
    eprintln!(
        "  note:      other local Mainnet contexts remain reusable; `leani reset all` removes every peer cache"
    );
    eprintln!("Stop any embedded subscriber using this feed before continuing.");
    if !confirmed {
        if !io::stdin().is_terminal() {
            bail!("subscription reset requires an interactive terminal or --yes");
        }
        eprint!("Reset this reconstructible subscription state? [y/N] ");
        io::stderr().flush()?;
        let mut answer = String::new();
        io::stdin().read_line(&mut answer)?;
        if !matches!(answer.trim().to_ascii_lowercase().as_str(), "y" | "yes") {
            bail!("subscription reset was not confirmed");
        }
    }
    let _lock = local_state::lock_runtime_directory(data_dir)?;
    let unknown = local_state::remove_known_runtime_state(data_dir)?;
    for path in unknown {
        eprintln!("leani: preserved unknown entry {}", path.display());
    }
    eprintln!("leani: subscription state reset; the next embedded run is cold");
    Ok(true)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PeerCacheMerge {
    total: usize,
    imported: usize,
}

fn merge_local_execution_peer_caches(
    config_path: Option<&Path>,
    data_dir: &Path,
    maximum_entries: usize,
) -> Result<Option<PeerCacheMerge>> {
    let destination = data_dir.join("execution-peers.json");
    let mut sources = Vec::new();
    if destination.is_file() {
        sources.push(destination.clone());
    }
    if let Some(config_path) = config_path {
        sources.push(
            Config::load(config_path)?
                .data_dir
                .join("execution-peers.json"),
        );
    }
    if data_dir
        .parent()
        .and_then(Path::file_name)
        .is_some_and(|name| name == "subscriptions")
        && let Some(subscriptions) = data_dir.parent()
    {
        for entry in fs::read_dir(subscriptions)
            .with_context(|| format!("read subscription state root {}", subscriptions.display()))?
        {
            let path = entry
                .with_context(|| format!("read entry in {}", subscriptions.display()))?
                .path()
                .join("execution-peers.json");
            sources.push(path);
        }
    }
    sources.sort_unstable();
    sources.dedup();

    let destination_entries = read_peer_cache_entries(&destination)?;
    let destination_records = destination_entries
        .iter()
        .filter_map(peer_cache_record)
        .collect::<HashSet<_>>();
    let mut merged = BTreeMap::<String, Value>::new();
    for entry in destination_entries {
        if let Some(record) = peer_cache_record(&entry) {
            merged.insert(record, entry);
        }
    }
    for source in sources.iter().filter(|source| **source != destination) {
        let entries = match read_peer_cache_entries(source) {
            Ok(entries) => entries,
            Err(error) => {
                eprintln!(
                    "leani: ignoring unreadable peer cache {}: {error:#}",
                    source.display()
                );
                continue;
            }
        };
        for entry in entries {
            let Some(record) = peer_cache_record(&entry) else {
                continue;
            };
            match merged.entry(record) {
                std::collections::btree_map::Entry::Vacant(slot) => {
                    slot.insert(entry);
                }
                std::collections::btree_map::Entry::Occupied(mut slot)
                    if peer_cache_priority(&entry) > peer_cache_priority(slot.get()) =>
                {
                    slot.insert(entry);
                }
                std::collections::btree_map::Entry::Occupied(_) => {}
            }
        }
    }
    let mut entries = merged.into_iter().collect::<Vec<_>>();
    entries.sort_by(|(left_record, left), (right_record, right)| {
        peer_cache_priority(right)
            .cmp(&peer_cache_priority(left))
            .then_with(|| left_record.cmp(right_record))
    });
    entries.truncate(maximum_entries);
    let imported = entries
        .iter()
        .filter(|(record, _)| !destination_records.contains(record))
        .count();
    let entries = entries
        .into_iter()
        .map(|(_, entry)| entry)
        .collect::<Vec<_>>();
    if entries.is_empty() {
        return Ok(None);
    }
    write_peer_cache(&destination, &entries)?;
    merge_local_execution_peer_quality(&sources, data_dir, maximum_entries)?;
    Ok(Some(PeerCacheMerge {
        total: entries.len(),
        imported,
    }))
}

fn merge_local_execution_peer_quality(
    peer_cache_sources: &[PathBuf],
    data_dir: &Path,
    maximum_entries: usize,
) -> Result<()> {
    let quality_destination = data_dir.join("execution-peer-quality.json");
    let quality_sources = peer_cache_sources
        .iter()
        .map(|source| source.with_file_name("execution-peer-quality.json"))
        .collect::<Vec<_>>();
    leani_source_p2p::merge_peer_quality_caches(
        &quality_destination,
        &quality_sources,
        maximum_entries,
    )
    .map_err(anyhow::Error::msg)
    .with_context(|| {
        format!(
            "merge execution peer-quality caches into {}",
            quality_destination.display()
        )
    })?;
    Ok(())
}

fn read_peer_cache_entries(path: &Path) -> Result<Vec<Value>> {
    let encoded = match fs::read(path) {
        Ok(encoded) => encoded,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(error).with_context(|| format!("read peer cache {}", path.display()));
        }
    };
    serde_json::from_slice(&encoded).with_context(|| format!("parse peer cache {}", path.display()))
}

fn peer_cache_record(entry: &Value) -> Option<String> {
    entry
        .get("record")
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn peer_cache_priority(entry: &Value) -> (bool, bool, bool, i64) {
    let reputation = entry
        .get("reputation")
        .and_then(Value::as_i64)
        .unwrap_or_default();
    let has_fork = entry
        .get("fork_id")
        .or_else(|| entry.get("forkId"))
        .is_some_and(|fork| !fork.is_null());
    (reputation >= 0, reputation > 0, has_fork, reputation)
}

fn write_peer_cache(path: &Path, entries: &[Value]) -> Result<()> {
    let parent = path
        .parent()
        .context("execution peer cache path has no parent directory")?;
    fs::create_dir_all(parent)
        .with_context(|| format!("create peer cache directory {}", parent.display()))?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)
        .with_context(|| format!("create peer cache in {}", parent.display()))?;
    serde_json::to_writer_pretty(temporary.as_file_mut(), entries)?;
    temporary.as_file_mut().write_all(b"\n")?;
    temporary.as_file_mut().sync_all()?;
    temporary
        .persist(path)
        .map_err(|error| error.error)
        .with_context(|| format!("write merged peer cache at {}", path.display()))?;
    Ok(())
}

async fn trusted_checkpoint(
    options: &SubscribeOptions,
    data_dir: &Path,
    require_beacon_api: bool,
    persist: bool,
) -> Result<CachedCheckpoint> {
    let cache_path = data_dir.join("checkpoint.json");
    let now = unix_seconds();
    let cached = read_checkpoint_cache(&cache_path);
    if let Some(cached) = cached.clone()
        && cached_checkpoint_is_reusable(&cached, options, require_beacon_api, now)
    {
        if cached.trust == CheckpointTrust::LocallyVerified {
            eprintln!(
                "leani: reusing locally verified checkpoint at slot {}",
                cached.slot
            );
        }
        return Ok(cached);
    }

    let quorum = fetch_checkpoint_quorum(options).await?;
    if require_beacon_api && quorum.beacon_api_endpoints.is_empty() {
        bail!(
            "checkpoint quorum succeeded, but none of the configured providers exposes the Beacon light-client API; use --finality-source p2p or include a full Beacon API provider"
        );
    }
    let previously_accepted = cached.as_ref().is_some_and(|cached| {
        quorum.agreeing_providers.iter().all(|provider| {
            cached
                .accepted_providers
                .iter()
                .any(|accepted| accepted == provider.as_str())
        })
    });
    if !previously_accepted {
        confirm_checkpoint(options, &quorum)?;
    }
    let checkpoint = CachedCheckpoint {
        schema: "leani.verified-checkpoint.v1".to_owned(),
        root: quorum.root,
        slot: quorum.slot,
        execution_block_hash: None,
        trust: CheckpointTrust::ProviderQuorum,
        accepted_providers: quorum
            .agreeing_providers
            .into_iter()
            .map(|provider| provider.to_string())
            .collect(),
        beacon_api_endpoints: quorum
            .beacon_api_endpoints
            .into_iter()
            .map(|provider| provider.to_string())
            .collect(),
        updated_at_unix_seconds: now,
    };
    if persist {
        write_checkpoint_cache(&cache_path, &checkpoint)?;
    }
    Ok(checkpoint)
}

fn read_checkpoint_cache(cache_path: &Path) -> Option<CachedCheckpoint> {
    fs::read(cache_path)
        .ok()
        .and_then(|encoded| serde_json::from_slice::<CachedCheckpoint>(&encoded).ok())
        .filter(|cached| cached.schema == "leani.verified-checkpoint.v1")
}

fn cached_checkpoint_is_reusable(
    cached: &CachedCheckpoint,
    options: &SubscribeOptions,
    require_beacon_api: bool,
    now: u64,
) -> bool {
    if require_beacon_api && cached.beacon_api_endpoints.is_empty() {
        return false;
    }
    match cached.trust {
        CheckpointTrust::LocallyVerified => checkpoint_age_seconds(cached.slot, now)
            .is_some_and(|age| age <= LOCALLY_VERIFIED_CHECKPOINT_MAX_AGE.as_secs()),
        CheckpointTrust::ProviderQuorum => {
            let configured = options
                .checkpoint_urls
                .iter()
                .map(Url::as_str)
                .collect::<HashSet<_>>();
            cached.accepted_providers.len() >= options.checkpoint_quorum
                && cached
                    .accepted_providers
                    .iter()
                    .all(|provider| configured.contains(provider.as_str()))
                && now.saturating_sub(cached.updated_at_unix_seconds)
                    <= CHECKPOINT_CACHE_MAX_AGE.as_secs()
        }
    }
}

fn checkpoint_age_seconds(slot: u64, now: u64) -> Option<u64> {
    let checkpoint_time = leani_finality_beacon_api::MAINNET_GENESIS_TIME
        .checked_add(slot.checked_mul(MAINNET_SLOT_SECONDS)?)?;
    (checkpoint_time <= now.saturating_add(MAINNET_SLOT_SECONDS))
        .then(|| now.saturating_sub(checkpoint_time))
}

async fn fetch_checkpoint_quorum(options: &SubscribeOptions) -> Result<CheckpointQuorum> {
    if options.checkpoint_quorum < 2 {
        bail!("checkpoint quorum must require at least two providers");
    }
    if options.checkpoint_quorum > options.checkpoint_urls.len() {
        bail!(
            "checkpoint quorum {} exceeds {} configured providers",
            options.checkpoint_quorum,
            options.checkpoint_urls.len()
        );
    }
    let mut unique = HashSet::new();
    for provider in &options.checkpoint_urls {
        if !unique.insert(provider.as_str()) {
            bail!("checkpoint provider {provider} is configured more than once");
        }
    }
    let attempted = options.checkpoint_urls.len();
    let responses = futures::future::join_all(
        options
            .checkpoint_urls
            .iter()
            .cloned()
            .map(fetch_checkpoint),
    )
    .await;
    let mut successful = Vec::new();
    let mut failures = Vec::new();
    for response in responses {
        match response {
            Ok(response) => successful.push(response),
            Err(error) => failures.push(format!("{error:#}")),
        }
    }
    select_checkpoint_quorum(&successful, failures, attempted, options.checkpoint_quorum)
}

fn select_checkpoint_quorum(
    successful: &[ProviderCheckpoint],
    failures: Vec<String>,
    attempted: usize,
    minimum_agreement: usize,
) -> Result<CheckpointQuorum> {
    let beacon_api_endpoints = successful
        .iter()
        .filter(|response| response.beacon_api)
        .map(|response| response.provider.clone())
        .collect::<Vec<_>>();
    let mut grouped = BTreeMap::<(u64, String), Vec<Url>>::new();
    for response in successful {
        grouped
            .entry((response.slot, response.root.clone()))
            .or_default()
            .push(response.provider.clone());
    }
    let selected = grouped
        .iter()
        .filter(|(_, providers)| providers.len() >= minimum_agreement)
        .max_by_key(|((slot, _), providers)| (*slot, providers.len()));
    let Some(((slot, root), providers)) = selected else {
        let observations = grouped
            .iter()
            .map(|((slot, root), providers)| {
                format!("slot {slot} root {root} from {}", providers.len())
            })
            .chain(failures)
            .collect::<Vec<_>>()
            .join("; ");
        bail!("checkpoint quorum {minimum_agreement}/{attempted} was not reached: {observations}");
    };
    let mut agreeing_providers = providers.clone();
    agreeing_providers.sort_by(|left, right| left.as_str().cmp(right.as_str()));
    Ok(CheckpointQuorum {
        root: root.clone(),
        slot: *slot,
        agreeing_providers,
        beacon_api_endpoints,
        attempted,
    })
}

async fn fetch_checkpoint(provider: Url) -> Result<ProviderCheckpoint> {
    let client = reqwest::Client::new();
    let beacon_endpoint = provider_url(&provider, "eth/v1/beacon/headers/finalized")?;
    let beacon_response = client
        .get(beacon_endpoint)
        .timeout(Duration::from_secs(15))
        .send()
        .await?;
    if beacon_response.status().is_success() {
        let response = beacon_response.json::<BeaconHeaderResponse>().await?;
        let root = normalized_checkpoint_root(&response.data.root)?;
        return Ok(ProviderCheckpoint {
            provider,
            root,
            slot: response
                .data
                .header
                .message
                .slot
                .parse()
                .context("checkpoint response contains an invalid slot")?,
            beacon_api: true,
        });
    }

    let checkpointz_endpoint = provider_url(&provider, "checkpointz/v1/beacon/slots")?;
    let response = client
        .get(checkpointz_endpoint)
        .timeout(Duration::from_secs(15))
        .send()
        .await?
        .error_for_status()?
        .json::<CheckpointzResponse>()
        .await?;
    let slot = response
        .data
        .slots
        .into_iter()
        .find(|slot| slot.block_root.is_some())
        .context("checkpoint service returned no usable finalized checkpoint")?;
    let root =
        normalized_checkpoint_root(&slot.block_root.expect("usable checkpoint has a block root"))?;
    Ok(ProviderCheckpoint {
        provider,
        root,
        slot: slot
            .slot
            .parse()
            .context("checkpoint response contains an invalid slot")?,
        beacon_api: false,
    })
}

fn normalized_checkpoint_root(root: &str) -> Result<String> {
    let root = leani_finality_beacon_api::parse_checkpoint_root(root)
        .context("checkpoint response contains an invalid block root")?;
    Ok(format!("0x{}", hex::encode(root)))
}

fn provider_url(provider: &Url, suffix: &str) -> Result<Url> {
    let mut provider = provider.clone();
    if !provider.path().ends_with('/') {
        provider.set_path(&format!("{}/", provider.path()));
    }
    provider.join(suffix).context("construct checkpoint URL")
}

fn confirm_checkpoint(options: &SubscribeOptions, checkpoint: &CheckpointQuorum) -> Result<()> {
    eprintln!("leani: weak-subjectivity checkpoint bootstrap");
    eprintln!(
        "  quorum:   {}/{} providers",
        checkpoint.agreeing_providers.len(),
        checkpoint.attempted
    );
    for provider in &checkpoint.agreeing_providers {
        eprintln!("  provider: {provider}");
    }
    eprintln!("  root:     {}", checkpoint.root);
    eprintln!("  slot:     {}", checkpoint.slot);
    eprintln!("Subsequent light-client updates are verified from this agreed root.");
    if options.accept_checkpoint {
        return Ok(());
    }
    if !io::stdin().is_terminal() {
        bail!("checkpoint trust requires an interactive terminal or --yes");
    }
    eprint!("Trust this checkpoint quorum and continue? [y/N] ");
    io::stderr().flush()?;
    let mut answer = String::new();
    io::stdin().read_line(&mut answer)?;
    if !matches!(answer.trim().to_ascii_lowercase().as_str(), "y" | "yes") {
        bail!("checkpoint provider quorum was not accepted");
    }
    Ok(())
}

fn write_checkpoint_cache(cache_path: &Path, checkpoint: &CachedCheckpoint) -> Result<()> {
    let parent = cache_path
        .parent()
        .context("checkpoint cache path has no parent directory")?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)
        .with_context(|| format!("create checkpoint cache in {}", parent.display()))?;
    serde_json::to_writer_pretty(temporary.as_file_mut(), checkpoint)?;
    temporary.as_file_mut().write_all(b"\n")?;
    temporary.as_file_mut().sync_all()?;
    temporary
        .persist(cache_path)
        .map_err(|error| error.error)
        .with_context(|| format!("persist checkpoint cache at {}", cache_path.display()))?;
    Ok(())
}

fn persist_current_verified_anchor(
    runtime: &crate::process::EmbeddedNetworkRuntime,
    data_dir: &Path,
    config: &Config,
) -> Result<()> {
    let Some(anchor) = *runtime.verified_anchor.borrow() else {
        return Ok(());
    };
    let cache_path = data_dir.join("checkpoint.json");
    let previous = read_checkpoint_cache(&cache_path);
    let root = format!("0x{}", hex::encode(anchor.beacon_block_root));
    let execution_block_hash = anchor.block.hash.to_string();
    if previous.as_ref().is_some_and(|checkpoint| {
        checkpoint.trust == CheckpointTrust::LocallyVerified
            && checkpoint.slot == anchor.beacon_slot
            && checkpoint.root == root
            && checkpoint.execution_block_hash.as_deref() == Some(execution_block_hash.as_str())
    }) {
        return Ok(());
    }
    let accepted_providers = previous
        .as_ref()
        .map(|checkpoint| checkpoint.accepted_providers.clone())
        .unwrap_or_default();
    let beacon_api_endpoints = if matches!(config.finality.kind, FinalitySourceKind::BeaconApi) {
        config
            .finality
            .endpoints
            .iter()
            .map(Url::to_string)
            .collect()
    } else {
        previous
            .as_ref()
            .map(|checkpoint| checkpoint.beacon_api_endpoints.clone())
            .unwrap_or_default()
    };
    write_checkpoint_cache(
        &cache_path,
        &CachedCheckpoint {
            schema: "leani.verified-checkpoint.v1".to_owned(),
            root,
            slot: anchor.beacon_slot,
            execution_block_hash: Some(execution_block_hash),
            trust: CheckpointTrust::LocallyVerified,
            accepted_providers,
            beacon_api_endpoints,
            updated_at_unix_seconds: unix_seconds(),
        },
    )?;
    eprintln!(
        "leani: persisted locally verified checkpoint at slot {}",
        anchor.beacon_slot
    );
    Ok(())
}

fn local_item(
    protocol: SubscribeProtocol,
    markets: &[Market],
    record: &ChangeRecord,
) -> Result<Option<SubscriptionItem>> {
    match protocol {
        SubscribeProtocol::Blocks => {
            if record.change.kind != BLOCK_SUMMARY_KIND {
                return Ok(None);
            }
            let entity = postcard::from_bytes(&record.change.payload)
                .context("decode embedded Ethereum block summary")?;
            Ok(Some(SubscriptionItem::Block(entity)))
        }
        SubscribeProtocol::UniswapV3 => {
            if record.change.kind != "uniswap.price.observation" {
                return Ok(None);
            }
            let entity: PoolPriceEntity = postcard::from_bytes(&record.change.payload)
                .context("decode embedded Uniswap observation")?;
            let market = markets
                .iter()
                .find(|market| address_matches(entity.pool, market.pool));
            Ok(market.map(|market| SubscriptionItem::Uniswap {
                market: *market,
                entity,
            }))
        }
    }
}

fn latest_fresh_items(
    protocol: SubscribeProtocol,
    markets: &[Market],
    records: Vec<ChangeRecord>,
    finality: SubscribeFinality,
    now: u64,
) -> Result<Vec<(SubscriptionItem, ChangeRecord)>> {
    let mut latest = BTreeMap::new();
    for record in records {
        if !update_is_fresh(finality, record.block.timestamp, now) {
            continue;
        }
        if let Some(item) = local_item(protocol, markets, &record)? {
            let scope = item.scope_key();
            match record.direction {
                ChangeDirection::Apply | ChangeDirection::Finalized => {
                    latest.insert(scope, (item, record));
                }
                ChangeDirection::Undo => {
                    latest.remove(&scope);
                }
                ChangeDirection::ResetRequired => {
                    bail!("embedded subscription history requires a fresh snapshot");
                }
            }
        }
    }
    Ok(latest.into_values().collect())
}

fn address_matches(address: Address, expected: &str) -> bool {
    expected
        .parse::<AlloyAddress>()
        .is_ok_and(|expected| Address::from(expected) == address)
}

fn embedded_snapshot_budget(config: &Config) -> SourceBudget {
    SourceBudget {
        max_input_bytes: config.budgets.memory_bytes,
        max_frame_bytes: config.budgets.memory_bytes.min(32 * 1_024 * 1_024),
        max_frames: 64,
        max_buffered_frames: 64,
        max_in_flight_requests: config.budgets.source_concurrency,
        temporary_disk_bytes: config.budgets.temporary_disk_bytes,
    }
}

async fn optimistic_head_snapshot(
    mut anchors: tokio::sync::watch::Receiver<Option<leani_runtime::AppliedFinalityAnchor>>,
    source: std::sync::Arc<leani_source_p2p::RethP2pSource>,
    mut request: DataRequest,
    budget: SourceBudget,
    cancellation: tokio_util::sync::CancellationToken,
) -> Result<BlockFrame> {
    const MAX_SNAPSHOT_ATTEMPTS: usize = 3;

    let advertised = loop {
        if let Some(anchor) = *anchors.borrow() {
            break anchor.block;
        }
        tokio::select! {
            changed = anchors.changed() => {
                changed.context("verified anchor channel closed before optimistic preview")?;
            }
            () = cancellation.cancelled() => {
                bail!("embedded runtime stopped before optimistic preview");
            }
        }
    };
    request.range = BlockRange::single(advertised.number);
    for attempt in 1..=MAX_SNAPSHOT_ATTEMPTS {
        match source
            .optimistic_head_snapshot(advertised, &request, budget, &cancellation)
            .await
        {
            Ok(frame) => return Ok(frame),
            Err(error)
                if attempt < MAX_SNAPSHOT_ATTEMPTS
                    && !matches!(
                        &error,
                        leani_source_p2p::P2pError::InvalidConfig(_)
                            | leani_source_p2p::P2pError::RangeTooLarge { .. }
                            | leani_source_p2p::P2pError::ReorgTooDeep { .. }
                            | leani_source_p2p::P2pError::Cancelled
                            | leani_source_p2p::P2pError::Source(_)
                    ) =>
            {
                tracing::debug!(
                    attempt,
                    maximum_attempts = MAX_SNAPSHOT_ATTEMPTS,
                    %error,
                    "optimistic execution-head preview is temporarily unavailable; retrying the active peer pool"
                );
                tokio::select! {
                    () = cancellation.cancelled() => {
                        bail!("embedded runtime stopped before optimistic preview");
                    }
                    () = tokio::time::sleep(Duration::from_secs(1)) => {}
                }
            }
            Err(error) => return Err(error).context("fetch optimistic execution-head preview"),
        }
    }
    unreachable!("bounded optimistic snapshot attempts always return")
}

fn processor_data_request(
    requirement: &leani_processor_api::DataRequirement,
    range: BlockRange,
) -> DataRequest {
    DataRequest {
        chain_id: ChainId(1),
        range,
        required: requirement.capabilities,
        log_fields: requirement.log_fields,
        allow_filtered: requirement.allow_filtered,
        projection: FieldProjection::default(),
        filters: FilterSet {
            scope: requirement.filter.clone(),
            senders: requirement.filter.senders.clone(),
            recipients: requirement.filter.recipients.clone(),
        },
        minimum_finality: requirement.minimum_finality,
        verification_policy: VerificationPolicy::CompleteCryptographic,
    }
}

async fn optimistic_items(
    protocol: SubscribeProtocol,
    markets: &[Market],
    processor: &dyn leani_processor_api::Processor,
    frame: &BlockFrame,
) -> Result<Vec<SubscriptionItem>> {
    if !update_is_fresh(
        SubscribeFinality::Optimistic,
        frame.block.timestamp,
        unix_seconds(),
    ) {
        return Ok(Vec::new());
    }
    let delta = processor
        .map(frame)
        .await
        .context("map optimistic subscription head preview")?;
    match protocol {
        SubscribeProtocol::Blocks => {
            let entity = postcard::from_bytes(&delta.payload)
                .context("decode optimistic Ethereum block summary")?;
            Ok(vec![SubscriptionItem::Block(entity)])
        }
        SubscribeProtocol::UniswapV3 => {
            let delta: UniswapPriceDelta = postcard::from_bytes(&delta.payload)
                .context("decode optimistic Uniswap head preview")?;
            Ok(delta
                .observations
                .into_iter()
                .filter_map(|entity| {
                    markets
                        .iter()
                        .find(|market| address_matches(entity.pool, market.pool))
                        .map(|market| SubscriptionItem::Uniswap {
                            market: *market,
                            entity,
                        })
                })
                .collect())
        }
    }
}

fn render_optimistic_items(
    format: SubscribeFormat,
    items: &[SubscriptionItem],
    block: BlockRef,
    operation: &str,
) -> Result<Vec<ObservationKey>> {
    let mut rendered = Vec::new();
    for item in items {
        render_item(format, item, block, Finality::Optimistic, operation, None)?;
        rendered.push(observation_key(item));
    }
    Ok(rendered)
}

async fn next_preview_event(
    stream: &mut Option<ChainEventStream>,
) -> Option<Result<ChainEvent, SourceError>> {
    match stream {
        Some(stream) => stream.next().await,
        None => std::future::pending().await,
    }
}

fn retain_preview_block(
    blocks: &mut BTreeMap<u64, (BlockRef, Vec<SubscriptionItem>)>,
    previewed: &mut HashSet<ObservationKey>,
    block: BlockRef,
    items: Vec<SubscriptionItem>,
) {
    if let Some((_, replaced)) = blocks.insert(block.number.0, (block, items)) {
        for item in replaced {
            previewed.remove(&observation_key(&item));
        }
    }
    while blocks.len() > 64 {
        let Some((_, (_, expired))) = blocks.pop_first() else {
            break;
        };
        for item in expired {
            previewed.remove(&observation_key(&item));
        }
    }
}

fn revert_preview_block(
    blocks: &mut BTreeMap<u64, (BlockRef, Vec<SubscriptionItem>)>,
    previewed: &mut HashSet<ObservationKey>,
    block: BlockRef,
) -> Option<Vec<SubscriptionItem>> {
    let (retained, items) = blocks.remove(&block.number.0)?;
    if retained.hash != block.hash {
        blocks.insert(block.number.0, (retained, items));
        return None;
    }
    for item in &items {
        previewed.remove(&observation_key(item));
    }
    Some(items)
}

const fn observation_key(item: &SubscriptionItem) -> ObservationKey {
    match item {
        SubscriptionItem::Block(entity) => ObservationKey::Block(entity.block_hash),
        SubscriptionItem::Uniswap { entity, .. } => {
            ObservationKey::Uniswap(entity.pool, entity.block_hash, entity.log_index)
        }
    }
}

fn preview_is_at_or_after(
    blocks: &BTreeMap<u64, (BlockRef, Vec<SubscriptionItem>)>,
    candidate: &SubscriptionItem,
) -> bool {
    blocks.values().any(|(block, items)| {
        items.iter().any(|preview| {
            preview.same_scope(candidate)
                && (preview.block_number() > candidate.block_number()
                    || (preview.block_number() == candidate.block_number()
                        && block.hash == candidate.block_hash()
                        && preview.item_index() >= candidate.item_index()))
        })
    })
}

#[derive(Debug)]
enum PreviewHandoffAction {
    Suppress,
    Render {
        reverted: Vec<(BlockRef, SubscriptionItem)>,
    },
}

fn reconcile_preview_handoff(
    blocks: &mut BTreeMap<u64, (BlockRef, Vec<SubscriptionItem>)>,
    previewed: &mut HashSet<ObservationKey>,
    candidate: &SubscriptionItem,
    direction: ChangeDirection,
) -> PreviewHandoffAction {
    // An exact durable apply confirms an already-rendered preview. An undo of
    // that identity must remain visible because the user saw the apply.
    let key = observation_key(candidate);
    if previewed.remove(&key) {
        remove_preview_item(blocks, key);
        return if direction == ChangeDirection::Undo {
            PreviewHandoffAction::Render {
                reverted: Vec::new(),
            }
        } else {
            PreviewHandoffAction::Suppress
        };
    }
    if preview_is_at_or_after(blocks, candidate) {
        return PreviewHandoffAction::Suppress;
    }

    // Crossing a still-unconfirmed preview means the durable chain selected a
    // replacement. Revert those speculative observations newest-first before
    // releasing the durable candidate.
    let candidate_number = candidate.block_number();
    let candidate_hash = candidate.block_hash();
    let candidate_index = candidate.item_index();
    let mut reverted = Vec::new();
    for (block, items) in blocks.values_mut() {
        items.retain(|preview| {
            if !preview.same_scope(candidate) {
                return true;
            }
            let crossed = block.number < candidate_number
                || (block.number == candidate_number
                    && (block.hash != candidate_hash || preview.item_index() < candidate_index));
            if !crossed {
                return true;
            }
            let key = observation_key(preview);
            if previewed.remove(&key) {
                reverted.push((*block, preview.clone()));
            }
            false
        });
    }
    blocks.retain(|_, (_, items)| !items.is_empty());
    reverted.sort_by_key(|(block, item)| {
        (
            std::cmp::Reverse(block.number),
            std::cmp::Reverse(item.item_index()),
        )
    });
    PreviewHandoffAction::Render { reverted }
}

fn remove_preview_item(
    blocks: &mut BTreeMap<u64, (BlockRef, Vec<SubscriptionItem>)>,
    key: ObservationKey,
) {
    for (_, items) in blocks.values_mut() {
        items.retain(|item| observation_key(item) != key);
    }
    blocks.retain(|_, (_, items)| !items.is_empty());
}

fn render_preview_reverts(
    format: SubscribeFormat,
    reverted: &[(BlockRef, SubscriptionItem)],
) -> Result<()> {
    for (block, item) in reverted {
        render_item(format, item, *block, Finality::Optimistic, "revert", None)?;
    }
    Ok(())
}

fn render_local_item(
    format: SubscribeFormat,
    item: &SubscriptionItem,
    record: &ChangeRecord,
    processor: &dyn leani_processor_api::Processor,
    store_epoch: [u8; 16],
) -> Result<()> {
    if format == SubscribeFormat::Raw {
        let envelope = leani_api::change_envelope_json(store_epoch, processor, record.clone())?;
        println!("{}", serde_json::to_string(&envelope)?);
        io::stdout().flush()?;
        return Ok(());
    }
    render_item(
        format,
        item,
        record.block,
        record.finality,
        direction_name(record.direction),
        Some(record.cursor.sequence.to_string()),
    )
}

fn render_item(
    format: SubscribeFormat,
    item: &SubscriptionItem,
    block: BlockRef,
    finality: Finality,
    operation: &str,
    sequence: Option<String>,
) -> Result<()> {
    match item {
        SubscriptionItem::Block(entity) => render_block_summary(
            format,
            &RenderedBlockSummary {
                chain_id: entity.chain_id.0,
                block_number: entity.block_number.0,
                block_hash: entity.block_hash.to_string(),
                parent_hash: entity.parent_hash.to_string(),
                timestamp: entity.timestamp,
                gas_limit: entity.gas_limit,
                gas_used: entity.gas_used,
                base_fee_wei: entity
                    .base_fee_per_gas
                    .map(|value| U256::from_be_bytes(value.0).to_string()),
                blob_gas_used: entity.blob_gas_used,
                excess_blob_gas: entity.excess_blob_gas,
                transaction_count: entity.transaction_count,
                size_bytes: entity.size_bytes,
                finality: finality_name(finality).to_owned(),
                operation: operation.to_owned(),
                sequence,
            },
        ),
        SubscriptionItem::Uniswap { market, entity } => {
            render_entity(format, market, entity, block, finality, operation, sequence)
        }
    }
}

fn render_entity(
    format: SubscribeFormat,
    market: &Market,
    entity: &PoolPriceEntity,
    block: BlockRef,
    finality: Finality,
    operation: &str,
    sequence: Option<String>,
) -> Result<()> {
    let sqrt = entity
        .sqrt_price_x96
        .context("Uniswap V3 observation omitted sqrtPriceX96")?;
    let amount0 = entity.amount0.map(signed_amount);
    let amount1 = entity.amount1.map(signed_amount);
    let price = v3_price(market, U256::from_be_bytes(sqrt.0), PRICE_PRECISION)?;
    let output = RenderedPrice {
        market,
        price,
        base_volume: amount0
            .zip(amount1)
            .map(|(amount0, amount1)| base_volume(market, amount0, amount1)),
        block_number: block.number.0,
        block_hash: format!("0x{}", hex::encode(block.hash.0)),
        timestamp: block.timestamp,
        log_index: entity.log_index,
        finality: finality_name(finality).to_owned(),
        operation: operation.to_owned(),
        sqrt_price_x96: U256::from_be_bytes(sqrt.0).to_string(),
        amount0: amount0.map(|amount| amount.to_string()),
        amount1: amount1.map(|amount| amount.to_string()),
        sequence,
    };
    render_price(format, &output)
}

fn render_price(format: SubscribeFormat, output: &RenderedPrice<'_>) -> Result<()> {
    match format {
        SubscribeFormat::Pretty => {
            let timestamp = readable_timestamp(output.timestamp)?;
            let volume = output.base_volume.as_ref().map_or_else(
                || "volume=n/a".to_owned(),
                |volume| format!("volume={} {}", volume, base_token(output.market).symbol),
            );
            println!(
                "{}  {} {}  {}  block={}  {}{}",
                timestamp,
                output.market.symbol,
                output.price,
                volume,
                output.block_number,
                output.finality,
                if output.operation == "apply" {
                    String::new()
                } else {
                    format!("  {}", output.operation)
                }
            );
        }
        SubscribeFormat::Json => println!(
            "{}",
            serde_json::to_string(&json!({
                "schema": "leani.market-price.v2",
                "market": output.market.symbol,
                "protocol": "uniswap-v3",
                "pool": output.market.pool,
                "feeTier": output.market.fee_tier,
                "price": output.price,
                "baseVolume": output.base_volume,
                "baseToken": {
                    "symbol": base_token(output.market).symbol,
                    "address": base_token(output.market).address,
                    "decimals": base_token(output.market).decimals,
                },
                "quoteToken": {
                    "symbol": quote_token(output.market).symbol,
                    "address": quote_token(output.market).address,
                    "decimals": quote_token(output.market).decimals,
                },
                "blockNumber": output.block_number,
                "blockHash": output.block_hash,
                "timestamp": output.timestamp,
                "logIndex": output.log_index,
                "finality": output.finality,
                "operation": output.operation,
                "sequence": output.sequence,
                "raw": {
                    "sqrtPriceX96": output.sqrt_price_x96,
                    "amount0": output.amount0,
                    "amount1": output.amount1,
                },
            }))?
        ),
        SubscribeFormat::Raw => unreachable!("raw records are rendered before price conversion"),
    }
    io::stdout().flush()?;
    Ok(())
}

fn render_block_summary(format: SubscribeFormat, output: &RenderedBlockSummary) -> Result<()> {
    match format {
        SubscribeFormat::Pretty => {
            let timestamp = readable_timestamp(output.timestamp)?;
            let gas = match (output.gas_used, output.gas_limit) {
                (Some(used), Some(limit)) if limit > 0 => format!(
                    "{} / {} ({})",
                    compact_count(used),
                    compact_count(limit),
                    tenths_percent(used, limit),
                ),
                (Some(used), _) => compact_count(used),
                _ => "unknown".to_owned(),
            };
            let base_fee = output.base_fee_wei.as_deref().map_or_else(
                || "unknown".to_owned(),
                |value| {
                    value.parse::<U256>().map_or_else(
                        |_| "unknown".to_owned(),
                        |value| format!("{} gwei", decimal_token_amount(value, 9)),
                    )
                },
            );
            let blobs = output.blob_gas_used.map_or_else(
                || "unknown".to_owned(),
                |gas| {
                    if gas % 131_072 == 0 {
                        (gas / 131_072).to_string()
                    } else {
                        format!("gas:{gas}")
                    }
                },
            );
            let transactions = output
                .transaction_count
                .map_or_else(|| "unknown".to_owned(), |count| count.to_string());
            println!(
                "{}  block={}  txs={}  gas={}  base_fee={}  blobs={}  {}{}",
                timestamp,
                output.block_number,
                transactions,
                gas,
                base_fee,
                blobs,
                output.finality,
                if output.operation == "apply" {
                    String::new()
                } else {
                    format!("  {}", output.operation)
                }
            );
        }
        SubscribeFormat::Json => println!(
            "{}",
            serde_json::to_string(&json!({
                "schema": "leani.block-summary.v1",
                "chainId": output.chain_id,
                "blockNumber": output.block_number,
                "blockHash": output.block_hash,
                "parentHash": output.parent_hash,
                "timestamp": output.timestamp,
                "gasLimit": output.gas_limit,
                "gasUsed": output.gas_used,
                "baseFeePerGas": output.base_fee_wei,
                "blobGasUsed": output.blob_gas_used,
                "excessBlobGas": output.excess_blob_gas,
                "transactionCount": output.transaction_count,
                "sizeBytes": output.size_bytes,
                "finality": output.finality,
                "operation": output.operation,
                "sequence": output.sequence,
            }))?
        ),
        SubscribeFormat::Raw => {
            unreachable!("raw records are rendered before block-summary conversion")
        }
    }
    io::stdout().flush()?;
    Ok(())
}

fn compact_count(value: u64) -> String {
    if value >= 1_000_000 {
        let hundredths = (u128::from(value) * 100 + 500_000) / 1_000_000;
        format!("{}.{:02}M", hundredths / 100, hundredths % 100)
    } else if value >= 1_000 {
        let tenths = (u128::from(value) * 10 + 500) / 1_000;
        format!("{}.{:01}k", tenths / 10, tenths % 10)
    } else {
        value.to_string()
    }
}

fn tenths_percent(value: u64, total: u64) -> String {
    let tenths = (u128::from(value) * 1_000 + u128::from(total) / 2) / u128::from(total);
    format!("{}.{:01}%", tenths / 10, tenths % 10)
}

fn parse_quantity(value: &str) -> Result<U256> {
    if let Some(hex) = value.strip_prefix("0x") {
        U256::from_str_radix(hex, 16).context("invalid hexadecimal quantity from node API")
    } else {
        U256::from_str(value).context("invalid decimal quantity from node API")
    }
}

fn readable_timestamp(timestamp: u64) -> Result<String> {
    let timestamp = i64::try_from(timestamp).context("block timestamp exceeds the Unix range")?;
    let timestamp = chrono::DateTime::from_timestamp(timestamp, 0)
        .context("block timestamp is outside the supported calendar range")?;
    Ok(timestamp.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
}

fn update_is_fresh(finality: SubscribeFinality, timestamp: u64, now: u64) -> bool {
    let maximum_age = match finality {
        SubscribeFinality::Optimistic => OPTIMISTIC_UPDATE_MAX_AGE,
        SubscribeFinality::Finalized => FINALIZED_UPDATE_MAX_AGE,
    };
    now.saturating_sub(timestamp) <= maximum_age.as_secs()
}

fn signed_amount(amount: leani_primitives::Quantity) -> I256 {
    I256::from_raw(U256::from_be_bytes(amount.0))
}

fn base_volume(market: &Market, amount0: I256, amount1: I256) -> String {
    let amount = if market.base_is_token0 {
        amount0
    } else {
        amount1
    };
    decimal_token_amount(amount.unsigned_abs(), base_token(market).decimals)
}

fn decimal_token_amount(amount: U256, decimals: u8) -> String {
    let precision = usize::from(decimals);
    if precision == 0 {
        return amount.to_string();
    }
    let digits = amount.to_string();
    let padded = if digits.len() <= precision {
        format!("{}{}", "0".repeat(precision + 1 - digits.len()), digits)
    } else {
        digits
    };
    let split = padded.len() - precision;
    let fraction = padded[split..].trim_end_matches('0');
    if fraction.is_empty() {
        padded[..split].to_owned()
    } else {
        format!("{}.{}", &padded[..split], fraction)
    }
}

fn v3_price(market: &Market, sqrt_price_x96: U256, precision: usize) -> Result<String> {
    if sqrt_price_x96.is_zero() {
        bail!("sqrtPriceX96 must be non-zero");
    }
    let sqrt = U512::from(sqrt_price_x96);
    let square = sqrt * sqrt;
    let q192 = U512::from(1_u8) << 192;
    let (numerator, denominator) = if market.base_is_token0 {
        (
            square * pow10(market.token0.decimals),
            q192 * pow10(market.token1.decimals),
        )
    } else {
        (
            q192 * pow10(market.token1.decimals),
            square * pow10(market.token0.decimals),
        )
    };
    decimal_ratio(numerator, denominator, precision)
}

fn decimal_ratio(numerator: U512, denominator: U512, precision: usize) -> Result<String> {
    if denominator.is_zero() {
        bail!("price denominator must be non-zero");
    }
    let scale = pow10(u8::try_from(precision).context("price precision is too large")?);
    let rounded = (numerator * scale + denominator / U512::from(2_u8)) / denominator;
    let digits = rounded.to_string();
    if precision == 0 {
        return Ok(digits);
    }
    let padded = if digits.len() <= precision {
        format!("{}{}", "0".repeat(precision + 1 - digits.len()), digits)
    } else {
        digits
    };
    let split = padded.len() - precision;
    Ok(format!("{}.{}", &padded[..split], &padded[split..]))
}

fn pow10(exponent: u8) -> U512 {
    (0..exponent).fold(U512::from(1_u8), |value, _| value * U512::from(10_u8))
}

fn base_token(market: &Market) -> Token {
    if market.base_is_token0 {
        market.token0
    } else {
        market.token1
    }
}

fn quote_token(market: &Market) -> Token {
    if market.base_is_token0 {
        market.token1
    } else {
        market.token0
    }
}

const fn finality_name(finality: Finality) -> &'static str {
    match finality {
        Finality::Optimistic => "optimistic",
        Finality::Safe => "safe",
        Finality::Finalized => "finalized",
    }
}

const fn direction_name(direction: ChangeDirection) -> &'static str {
    match direction {
        ChangeDirection::Apply => "apply",
        ChangeDirection::Undo => "undo",
        ChangeDirection::Finalized => "finalized",
        ChangeDirection::ResetRequired => "reset_required",
    }
}

enum AttachedStreamConnection {
    Connected(reqwest::Response),
    Retry,
    Interrupted,
}

async fn connect_attached_stream(
    client: &reqwest::Client,
    options: &SubscribeOptions,
    stream_url: Url,
    backoff: Duration,
) -> Result<AttachedStreamConnection> {
    let response = authorized(client.get(stream_url), options.token.as_deref())
        .send()
        .await;
    let error = match response {
        Ok(response)
            if response.status().is_client_error()
                && response.status() != reqwest::StatusCode::REQUEST_TIMEOUT
                && response.status() != reqwest::StatusCode::TOO_MANY_REQUESTS =>
        {
            bail!(
                "node rejected the subscription request with {}; check the endpoint, processor, and token",
                response.status()
            );
        }
        Ok(response) => match response.error_for_status() {
            Ok(response) => return Ok(AttachedStreamConnection::Connected(response)),
            Err(error) => error,
        },
        Err(error) => error,
    };
    eprintln!(
        "leani: stream connection failed ({error}); reconnecting in {}s",
        backoff.as_secs()
    );
    if wait_for_reconnect(backoff).await? {
        return Ok(AttachedStreamConnection::Interrupted);
    }
    Ok(AttachedStreamConnection::Retry)
}

async fn subscribe_attached(
    options: &SubscribeOptions,
    markets: &[Market],
    endpoint: Url,
) -> Result<Exit> {
    let client = reqwest::Client::new();
    if options.protocol == SubscribeProtocol::UniswapV3 {
        validate_attached_markets(&client, options, markets, &endpoint).await?;
    }
    let (mut cursor, rendered_snapshot) =
        attached_start_cursor(&client, options, markets, &endpoint).await?;
    if rendered_snapshot && options.once {
        return Ok(Exit::Success);
    }
    announce_attached(options.protocol, &endpoint, markets);
    let mut backoff = Duration::from_secs(1);
    let mut last_sequence = None;
    loop {
        let mut stream_url = processor_url(&endpoint, &options.processor, "stream")?;
        if let Some(cursor) = &cursor {
            stream_url.query_pairs_mut().append_pair("after", cursor);
        }
        let response = match connect_attached_stream(&client, options, stream_url, backoff).await? {
            AttachedStreamConnection::Connected(response) => response,
            AttachedStreamConnection::Retry => {
                backoff = backoff.saturating_mul(2).min(Duration::from_secs(30));
                continue;
            }
            AttachedStreamConnection::Interrupted => return Ok(Exit::Success),
        };
        let mut bytes = response.bytes_stream();
        let mut buffer = Vec::new();
        loop {
            let chunk = tokio::select! {
                result = tokio::signal::ctrl_c() => {
                    result.context("install Ctrl-C handler")?;
                    return Ok(Exit::Success);
                }
                chunk = bytes.next() => chunk,
            };
            let Some(chunk) = chunk else {
                break;
            };
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(error) => {
                    eprintln!("leani: stream read failed: {error}");
                    break;
                }
            };
            buffer.extend_from_slice(&chunk);
            let outcome = render_attached_sse_events(
                &mut buffer,
                &mut cursor,
                &mut last_sequence,
                options,
                markets,
            )?;
            if outcome.observed_change {
                backoff = Duration::from_secs(1);
            }
            if outcome.stop {
                return Ok(Exit::Success);
            }
        }
        eprintln!(
            "leani: stream ended; reconnecting in {}s",
            backoff.as_secs()
        );
        if wait_for_reconnect(backoff).await? {
            return Ok(Exit::Success);
        }
        backoff = backoff.saturating_mul(2).min(Duration::from_secs(30));
    }
}

async fn attached_start_cursor(
    client: &reqwest::Client,
    options: &SubscribeOptions,
    markets: &[Market],
    endpoint: &Url,
) -> Result<(Option<String>, bool)> {
    let use_latest =
        options.finality == SubscribeFinality::Optimistic && options.format != SubscribeFormat::Raw;
    if use_latest {
        match options.protocol {
            SubscribeProtocol::Blocks => {
                let (cursor, latest) =
                    attached_latest_block_snapshot(client, options, endpoint).await?;
                let mut rendered = false;
                if let Some(latest) = latest
                    && update_is_fresh(options.finality, latest.timestamp, unix_seconds())
                {
                    render_attached_block_summary(options.format, latest, "apply", None)?;
                    rendered = true;
                }
                Ok((cursor, rendered))
            }
            SubscribeProtocol::UniswapV3 => {
                let (cursor, latest) =
                    attached_latest_snapshot(client, options, markets, endpoint).await?;
                let mut rendered = false;
                for (market, latest) in latest {
                    if !update_is_fresh(options.finality, latest.timestamp, unix_seconds()) {
                        continue;
                    }
                    render_attached_price(
                        options.format,
                        &market,
                        &latest.data,
                        latest.timestamp,
                        "apply",
                        None,
                    )?;
                    rendered = true;
                    if options.once {
                        break;
                    }
                }
                Ok((cursor, rendered))
            }
        }
    } else {
        Ok((
            attached_change_head(client, options, endpoint)
                .await?
                .cursor,
            false,
        ))
    }
}

fn announce_attached(protocol: SubscribeProtocol, endpoint: &Url, markets: &[Market]) {
    match protocol {
        SubscribeProtocol::Blocks => {
            eprintln!("leani: attached to {endpoint}; waiting for the next Ethereum block...");
        }
        SubscribeProtocol::UniswapV3 => eprintln!(
            "leani: attached to {} for {}; waiting for fresh prices...",
            endpoint,
            markets
                .iter()
                .map(|market| market.symbol)
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

fn render_attached_sse_events(
    buffer: &mut Vec<u8>,
    cursor: &mut Option<String>,
    last_sequence: &mut Option<u64>,
    options: &SubscribeOptions,
    markets: &[Market],
) -> Result<SseRenderOutcome> {
    let mut outcome = SseRenderOutcome::default();
    while let Some(end) = sse_event_end(buffer) {
        let event = buffer.drain(..end).collect::<Vec<_>>();
        let delimiter = if event.ends_with(b"\r\n\r\n") { 4 } else { 2 };
        let event = &event[..event.len().saturating_sub(delimiter)];
        let parsed = match parse_sse_event(event) {
            Ok(parsed) => parsed,
            Err(error) => {
                eprintln!("leani: ignored malformed SSE frame: {error}");
                continue;
            }
        };
        if parsed.event.as_deref() == Some("hello") {
            continue;
        }
        let Some(data) = parsed.data else {
            continue;
        };
        if parsed.event.as_deref() == Some("error") {
            bail!("node subscription stream reported an error: {data}");
        }
        let value: Value = match serde_json::from_str(&data) {
            Ok(value) => value,
            Err(error) => {
                eprintln!("leani: ignored malformed SSE JSON: {error}");
                continue;
            }
        };
        let envelope = match serde_json::from_value::<AttachedEnvelope>(value.clone()) {
            Ok(envelope) => envelope,
            Err(error) => {
                eprintln!("leani: ignored unrecognized SSE event: {error}");
                continue;
            }
        };
        outcome.observed_change = true;
        if envelope.operation == "reset_required" {
            bail!(
                "subscription cursor is no longer retained; query a fresh snapshot and restart without the old cursor"
            );
        }
        let sequence = envelope
            .sequence
            .parse::<u64>()
            .context("node SSE event sequence is not an unsigned integer")?;
        if last_sequence.is_some_and(|last| sequence <= last) {
            continue;
        }
        *last_sequence = Some(sequence);
        *cursor = Some(envelope.cursor.clone());
        if render_attached(
            options.protocol,
            options.format,
            options.finality,
            markets,
            envelope,
            &value,
        )? && options.once
        {
            outcome.stop = true;
            return Ok(outcome);
        }
    }
    Ok(outcome)
}

async fn attached_change_head(
    client: &reqwest::Client,
    options: &SubscribeOptions,
    endpoint: &Url,
) -> Result<ChangeHead> {
    let head_url = processor_url(endpoint, &options.processor, "changes/head")?;
    Ok(authorized(client.get(head_url), options.token.as_deref())
        .send()
        .await?
        .error_for_status()?
        .json::<ChangeHead>()
        .await?)
}

async fn attached_latest_snapshot(
    client: &reqwest::Client,
    options: &SubscribeOptions,
    markets: &[Market],
    endpoint: &Url,
) -> Result<(Option<String>, Vec<(Market, AttachedLatestObservation)>)> {
    const MAX_STABILITY_ATTEMPTS: usize = 8;

    for attempt in 1..=MAX_STABILITY_ATTEMPTS {
        let before = attached_change_head(client, options, endpoint).await?;
        let latest = attached_latest_observations(client, options, markets, endpoint).await?;
        let after = attached_change_head(client, options, endpoint).await?;
        if before.cursor == after.cursor {
            return Ok((after.cursor, latest));
        }
        tracing::debug!(
            attempt,
            "Uniswap changes advanced during latest-price bootstrap; retrying a stable snapshot"
        );
    }

    let head = attached_change_head(client, options, endpoint).await?;
    tracing::debug!(
        "Uniswap changes remained busy during latest-price bootstrap; continuing directly from the live cursor"
    );
    Ok((head.cursor, Vec::new()))
}

async fn attached_latest_block_snapshot(
    client: &reqwest::Client,
    options: &SubscribeOptions,
    endpoint: &Url,
) -> Result<(Option<String>, Option<AttachedBlockSummary>)> {
    const MAX_STABILITY_ATTEMPTS: usize = 8;

    for attempt in 1..=MAX_STABILITY_ATTEMPTS {
        let before = attached_change_head(client, options, endpoint).await?;
        let latest = attached_latest_block(client, options, endpoint).await?;
        let after = attached_change_head(client, options, endpoint).await?;
        if before.cursor == after.cursor {
            return Ok((after.cursor, latest));
        }
        tracing::debug!(
            attempt,
            "block summaries advanced during latest-block bootstrap; retrying a stable snapshot"
        );
    }

    let head = attached_change_head(client, options, endpoint).await?;
    tracing::debug!(
        "block summaries remained busy during latest-block bootstrap; continuing directly from the live cursor"
    );
    Ok((head.cursor, None))
}

async fn attached_latest_block(
    client: &reqwest::Client,
    options: &SubscribeOptions,
    endpoint: &Url,
) -> Result<Option<AttachedBlockSummary>> {
    let url = processor_url(endpoint, &options.processor, "query/latest")?;
    let response = authorized(client.get(url), options.token.as_deref())
        .send()
        .await?;
    if response.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    Ok(Some(
        response
            .error_for_status()?
            .json::<AttachedLatestBlock>()
            .await?
            .data,
    ))
}

async fn attached_latest_observations(
    client: &reqwest::Client,
    options: &SubscribeOptions,
    markets: &[Market],
    endpoint: &Url,
) -> Result<Vec<(Market, AttachedLatestObservation)>> {
    let responses = join_all(markets.iter().copied().map(|market| async move {
        let url = processor_url(
            endpoint,
            &options.processor,
            &format!("query/pools/{}/latest", market.pool),
        )?;
        let response = authorized(client.get(url), options.token.as_deref())
            .send()
            .await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let latest = response
            .error_for_status()?
            .json::<AttachedLatestObservation>()
            .await?;
        Ok::<_, anyhow::Error>(Some((market, latest)))
    }))
    .await;
    responses
        .into_iter()
        .filter_map(Result::transpose)
        .collect()
}

async fn validate_attached_markets(
    client: &reqwest::Client,
    options: &SubscribeOptions,
    markets: &[Market],
    endpoint: &Url,
) -> Result<()> {
    let pools_url = processor_url(endpoint, &options.processor, "query/pools")?;
    let configured = authorized(client.get(pools_url), options.token.as_deref())
        .send()
        .await?
        .error_for_status()?
        .json::<ConfiguredPoolsResponse>()
        .await?;
    validate_market_scope(markets, &configured.data)
}

fn validate_market_scope(markets: &[Market], configured: &[ConfiguredPool]) -> Result<()> {
    let configured_addresses = configured
        .iter()
        .map(|pool| pool.address.to_ascii_lowercase())
        .collect::<HashSet<_>>();
    let missing = markets
        .iter()
        .filter(|market| !configured_addresses.contains(&market.pool.to_ascii_lowercase()))
        .map(|market| market.symbol)
        .collect::<Vec<_>>();
    if missing.is_empty() {
        return Ok(());
    }
    let configured_markets = configured
        .iter()
        .map(|pool| {
            MARKET_CATALOG
                .iter()
                .find(|market| market.pool.eq_ignore_ascii_case(&pool.address))
                .map_or(pool.address.as_str(), |market| market.symbol)
        })
        .collect::<Vec<_>>();
    let subject = missing.join(", ");
    let verb = if missing.len() == 1 { "is" } else { "are" };
    let available = if configured_markets.is_empty() {
        "none".to_owned()
    } else {
        configured_markets.join(", ")
    };
    bail!("{subject} {verb} not processed by this node.\nConfigured markets: {available}")
}

async fn wait_for_reconnect(delay: Duration) -> Result<bool> {
    tokio::select! {
        result = tokio::signal::ctrl_c() => {
            result.context("install Ctrl-C handler")?;
            Ok(true)
        }
        () = tokio::time::sleep(delay) => Ok(false),
    }
}

fn render_attached(
    protocol: SubscribeProtocol,
    format: SubscribeFormat,
    finality: SubscribeFinality,
    markets: &[Market],
    envelope: AttachedEnvelope,
    raw: &Value,
) -> Result<bool> {
    if protocol == SubscribeProtocol::Blocks {
        return render_attached_block(format, finality, envelope, raw);
    }
    if !envelope.kind.starts_with("uniswap.price.observation.") {
        return Ok(false);
    }
    let Some(entity) = envelope.data else {
        return Ok(false);
    };
    let mut entity: AttachedPoolPrice =
        serde_json::from_value(entity).context("decode attached Uniswap price observation")?;
    entity.finality.clone_from(&envelope.finality);
    if finality == SubscribeFinality::Finalized && entity.finality != "finalized" {
        return Ok(false);
    }
    let Some(market) = markets
        .iter()
        .find(|market| market.pool.eq_ignore_ascii_case(&entity.pool))
    else {
        return Ok(false);
    };
    let timestamp = envelope
        .block
        .get("timestamp")
        .and_then(Value::as_u64)
        .context("attached Uniswap event omitted block.timestamp")?;
    if !update_is_fresh(finality, timestamp, unix_seconds()) {
        return Ok(false);
    }
    if format == SubscribeFormat::Raw {
        println!("{}", serde_json::to_string(&raw)?);
        io::stdout().flush()?;
        return Ok(true);
    }
    render_attached_price(
        format,
        market,
        &entity,
        timestamp,
        &envelope.operation,
        Some(envelope.sequence),
    )?;
    Ok(true)
}

fn render_attached_block(
    format: SubscribeFormat,
    finality: SubscribeFinality,
    envelope: AttachedEnvelope,
    raw: &Value,
) -> Result<bool> {
    if !envelope.kind.starts_with(BLOCK_SUMMARY_KIND) {
        return Ok(false);
    }
    let Some(entity) = envelope.data else {
        return Ok(false);
    };
    let mut entity: AttachedBlockSummary =
        serde_json::from_value(entity).context("decode attached block summary")?;
    entity.finality.clone_from(&envelope.finality);
    if finality == SubscribeFinality::Finalized && entity.finality != "finalized" {
        return Ok(false);
    }
    if !update_is_fresh(finality, entity.timestamp, unix_seconds()) {
        return Ok(false);
    }
    if format == SubscribeFormat::Raw {
        println!("{}", serde_json::to_string(raw)?);
        io::stdout().flush()?;
        return Ok(true);
    }
    render_attached_block_summary(format, entity, &envelope.operation, Some(envelope.sequence))?;
    Ok(true)
}

fn render_attached_block_summary(
    format: SubscribeFormat,
    entity: AttachedBlockSummary,
    operation: &str,
    sequence: Option<String>,
) -> Result<()> {
    let base_fee_wei = entity
        .base_fee_per_gas
        .as_deref()
        .map(parse_quantity)
        .transpose()?
        .map(|value| value.to_string());
    render_block_summary(
        format,
        &RenderedBlockSummary {
            chain_id: entity.chain_id,
            block_number: entity.block_number,
            block_hash: entity.block_hash,
            parent_hash: entity.parent_hash,
            timestamp: entity.timestamp,
            gas_limit: entity.gas_limit,
            gas_used: entity.gas_used,
            base_fee_wei,
            blob_gas_used: entity.blob_gas_used,
            excess_blob_gas: entity.excess_blob_gas,
            transaction_count: entity.transaction_count,
            size_bytes: entity.size_bytes,
            finality: entity.finality,
            operation: operation.to_owned(),
            sequence,
        },
    )
}

fn render_attached_price(
    format: SubscribeFormat,
    market: &Market,
    entity: &AttachedPoolPrice,
    timestamp: u64,
    operation: &str,
    sequence: Option<String>,
) -> Result<()> {
    let sqrt = entity
        .sqrt_price_x96
        .as_deref()
        .context("Uniswap V3 observation omitted sqrtPriceX96")?;
    let amount0 = entity
        .amount0
        .as_deref()
        .map(|amount| {
            amount
                .parse::<I256>()
                .context("invalid signed amount0 from node API")
        })
        .transpose()?;
    let amount1 = entity
        .amount1
        .as_deref()
        .map(|amount| {
            amount
                .parse::<I256>()
                .context("invalid signed amount1 from node API")
        })
        .transpose()?;
    let sqrt_value = U256::from_str(sqrt).context("invalid sqrtPriceX96 from node API")?;
    let price = v3_price(market, sqrt_value, PRICE_PRECISION)?;
    let output = RenderedPrice {
        market,
        price,
        base_volume: amount0
            .zip(amount1)
            .map(|(amount0, amount1)| base_volume(market, amount0, amount1)),
        block_number: entity.block_number,
        block_hash: entity.block_hash.clone(),
        timestamp,
        log_index: entity.log_index,
        finality: entity.finality.clone(),
        operation: operation.to_owned(),
        sqrt_price_x96: sqrt.to_owned(),
        amount0: amount0.map(|amount| amount.to_string()),
        amount1: amount1.map(|amount| amount.to_string()),
        sequence,
    };
    render_price(format, &output)?;
    Ok(())
}

fn processor_url(endpoint: &Url, processor: &str, suffix: &str) -> Result<Url> {
    let mut endpoint = endpoint.clone();
    if !endpoint.path().ends_with('/') {
        endpoint.set_path(&format!("{}/", endpoint.path()));
    }
    endpoint
        .join(&format!("v1/processors/{processor}/{suffix}"))
        .context("construct node subscription URL")
}

fn authorized(request: reqwest::RequestBuilder, token: Option<&str>) -> reqwest::RequestBuilder {
    match token {
        Some(token) => request.bearer_auth(token),
        None => request,
    }
}

fn sse_event_end(buffer: &[u8]) -> Option<usize> {
    let crlf = buffer
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|position| (position, position + 4));
    let lf = buffer
        .windows(2)
        .position(|window| window == b"\n\n")
        .map(|position| (position, position + 2));
    match (crlf, lf) {
        (Some(left), Some(right)) => Some(if left.0 <= right.0 { left.1 } else { right.1 }),
        (Some((_, end)), None) | (None, Some((_, end))) => Some(end),
        (None, None) => None,
    }
}

fn parse_sse_event(event: &[u8]) -> Result<ParsedSseEvent> {
    let event = std::str::from_utf8(event).context("node SSE stream is not UTF-8")?;
    let event_name = event
        .lines()
        .find_map(|line| line.strip_prefix("event:"))
        .map(|value| value.trim_start().to_owned());
    let data = event
        .lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(str::trim_start)
        .collect::<Vec<_>>();
    Ok(ParsedSseEvent {
        event: event_name,
        data: (!data.is_empty()).then(|| data.join("\n")),
    })
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ProcessorHistoryMode, PublishMode};
    use leani_primitives::{BlockHash, BlockNumber, ChangeCursor};
    use leani_processor_api::{ChangeOperation, DomainChange};
    use leani_processor_uniswap::PoolKind;
    use leani_store_sqlite::{DeliveryOrigin, DeliveryOriginKind};

    fn checkpoint_provider(provider: &str, root_byte: u8, slot: u64) -> ProviderCheckpoint {
        ProviderCheckpoint {
            provider: Url::parse(provider).expect("provider URL"),
            root: format!("0x{}", hex::encode([root_byte; 32])),
            slot,
            beacon_api: provider.contains("publicnode"),
        }
    }

    fn subscribe_options(checkpoint_urls: Vec<Url>) -> SubscribeOptions {
        SubscribeOptions {
            protocol: SubscribeProtocol::UniswapV3,
            targets: vec!["ETH/USDC".to_owned()],
            format: SubscribeFormat::Pretty,
            mode: SubscribeMode::Embedded,
            endpoint: None,
            processor: "uniswap-observations".to_owned(),
            token: None,
            finality: SubscribeFinality::Optimistic,
            finality_source: SubscribeFinalitySource::Auto,
            checkpoint_urls,
            checkpoint_quorum: 2,
            accept_checkpoint: true,
            data_dir: None,
            once: true,
            requested_config: None,
            working_directory: PathBuf::from("."),
        }
    }

    fn price_record(market: Market, sequence: u64, timestamp: u64) -> ChangeRecord {
        let block_hash = BlockHash::new([u8::try_from(sequence).unwrap_or(u8::MAX); 32]);
        let entity = PoolPriceEntity {
            pool: Address::from(
                market
                    .pool
                    .parse::<AlloyAddress>()
                    .expect("catalog address"),
            ),
            kind: PoolKind::V3,
            reserve0: None,
            reserve1: None,
            amount0: None,
            amount1: None,
            sqrt_price_x96: None,
            block_number: BlockNumber(sequence),
            block_hash,
            log_index: 0,
            finality: Finality::Optimistic,
        };
        ChangeRecord {
            delivery_encoding_version: 1,
            cursor: ChangeCursor {
                chain_id: ChainId(1),
                processor_id: "uniswap-observations".to_owned(),
                sequence,
            },
            origin: DeliveryOrigin {
                kind: DeliveryOriginKind::Live,
                id: "test".to_owned(),
                publication_revision: 0,
            },
            block: leani_primitives::BlockRef {
                number: BlockNumber(sequence),
                hash: block_hash,
                parent_hash: BlockHash::new([0; 32]),
                timestamp,
            },
            finality: Finality::Optimistic,
            direction: ChangeDirection::Apply,
            change: DomainChange {
                kind: "uniswap.price.observation".to_owned(),
                key: Vec::new(),
                operation: ChangeOperation::Upsert,
                payload: postcard::to_allocvec(&entity).expect("encode observation"),
            },
            emitted_at_unix_ms: timestamp.saturating_mul(1_000),
        }
    }

    fn block_record(sequence: u64, timestamp: u64) -> ChangeRecord {
        let block_hash = BlockHash::new([u8::try_from(sequence).unwrap_or(u8::MAX); 32]);
        let entity = BlockSummaryEntity {
            chain_id: ChainId(1),
            block_number: BlockNumber(sequence),
            block_hash,
            parent_hash: BlockHash::new([0; 32]),
            timestamp,
            gas_limit: Some(60_000_000),
            gas_used: Some(30_000_000),
            base_fee_per_gas: None,
            blob_gas_used: Some(262_144),
            excess_blob_gas: Some(393_216),
            transaction_count: Some(123),
            size_bytes: None,
            finality: Finality::Optimistic,
        };
        ChangeRecord {
            delivery_encoding_version: 1,
            cursor: ChangeCursor {
                chain_id: ChainId(1),
                processor_id: "block-summary".to_owned(),
                sequence,
            },
            origin: DeliveryOrigin {
                kind: DeliveryOriginKind::Live,
                id: "test".to_owned(),
                publication_revision: 0,
            },
            block: BlockRef {
                number: BlockNumber(sequence),
                hash: block_hash,
                parent_hash: BlockHash::new([0; 32]),
                timestamp,
            },
            finality: Finality::Optimistic,
            direction: ChangeDirection::Apply,
            change: DomainChange {
                kind: BLOCK_SUMMARY_KIND.to_owned(),
                key: block_hash.0.to_vec(),
                operation: ChangeOperation::Upsert,
                payload: postcard::to_allocvec(&entity).expect("encode block summary"),
            },
            emitted_at_unix_ms: timestamp.saturating_mul(1_000),
        }
    }

    #[test]
    fn resolves_multiple_markets_and_weth_spelling() {
        let markets =
            resolve_markets(&["weth/usdc".to_owned(), "WBTC/ETH".to_owned()]).expect("markets");
        assert_eq!(markets.len(), 2);
        assert_eq!(markets[0].symbol, "ETH/USDC");
    }

    #[test]
    fn rejects_duplicate_markets() {
        let error = resolve_markets(&["ETH/USDC".to_owned(), "WETH/USDC".to_owned()])
            .expect_err("duplicate");
        assert!(error.to_string().contains("more than once"));
    }

    #[test]
    fn subscription_targets_are_protocol_specific() {
        assert!(subscription_markets(SubscribeProtocol::Blocks, &[]).is_ok());
        assert!(subscription_markets(SubscribeProtocol::Blocks, &["ETH/USDC".to_owned()]).is_err());
        assert!(subscription_markets(SubscribeProtocol::UniswapV3, &[]).is_err());
    }

    #[test]
    fn attached_subscription_rejects_markets_outside_the_node_scope() {
        let requested =
            resolve_markets(&["ETH/USDC".to_owned(), "ETH/USDT".to_owned()]).expect("markets");
        let configured = vec![ConfiguredPool {
            address: requested[0].pool.to_owned(),
        }];

        let error = validate_market_scope(&requested, &configured)
            .expect_err("ETH/USDT is not configured on the node");
        assert_eq!(
            error.to_string(),
            "ETH/USDT is not processed by this node.\nConfigured markets: ETH/USDC"
        );
        validate_market_scope(&requested[..1], &configured).expect("ETH/USDC is configured");
    }

    #[test]
    fn exact_v3_price_handles_token_orientation_and_decimals() {
        let market = resolve_markets(&["ETH/USDC".to_owned()]).expect("market")[0];
        // sqrt(10^18 WETH units / (1_500 * 10^6 USDC units)) * 2^96.
        let sqrt = U256::from_str("2045662359789070170858018546451766").expect("sqrt");
        let price = v3_price(&market, sqrt, 8).expect("price");
        assert_eq!(price, "1500.00000000");
    }

    #[test]
    fn pretty_timestamp_is_utc_and_human_readable() {
        assert_eq!(
            readable_timestamp(1_787_924_399).expect("timestamp"),
            "2026-08-28T13:39:59Z"
        );
    }

    #[test]
    fn pretty_block_counts_and_gas_percentage_use_exact_integer_rounding() {
        assert_eq!(compact_count(52_481_000), "52.48M");
        assert_eq!(compact_count(12_349), "12.3k");
        assert_eq!(compact_count(999), "999");
        assert_eq!(tenths_percent(52_481_000, 60_000_000), "87.5%");
    }

    #[test]
    fn catch_up_updates_stay_silent_until_the_stream_is_current() {
        let now = 10_000;
        assert!(update_is_fresh(
            SubscribeFinality::Optimistic,
            now - OPTIMISTIC_UPDATE_MAX_AGE.as_secs(),
            now
        ));
        assert!(!update_is_fresh(
            SubscribeFinality::Optimistic,
            now - OPTIMISTIC_UPDATE_MAX_AGE.as_secs() - 1,
            now
        ));
        assert!(update_is_fresh(
            SubscribeFinality::Finalized,
            now - 15 * 60,
            now
        ));
    }

    #[test]
    fn readiness_keeps_only_the_newest_fresh_price_per_market() {
        let eth = resolve_markets(&["ETH/USDC".to_owned()]).expect("ETH market")[0];
        let wbtc = resolve_markets(&["WBTC/ETH".to_owned()]).expect("WBTC market")[0];
        let prices = latest_fresh_items(
            SubscribeProtocol::UniswapV3,
            &[eth, wbtc],
            vec![
                price_record(eth, 1, 9_000),
                price_record(eth, 2, 9_950),
                price_record(wbtc, 3, 9_960),
                price_record(eth, 4, 9_990),
            ],
            SubscribeFinality::Optimistic,
            10_000,
        )
        .expect("select startup prices");

        assert_eq!(prices.len(), 2);
        assert!(matches!(
            &prices[0].0,
            SubscriptionItem::Uniswap { market, .. } if market.symbol == "ETH/USDC"
        ));
        assert_eq!(prices[0].1.cursor.sequence, 4);
        assert!(matches!(
            &prices[1].0,
            SubscriptionItem::Uniswap { market, .. } if market.symbol == "WBTC/ETH"
        ));
        assert_eq!(prices[1].1.cursor.sequence, 3);
    }

    #[test]
    fn readiness_keeps_only_the_newest_fresh_block() {
        let items = latest_fresh_items(
            SubscribeProtocol::Blocks,
            &[],
            vec![
                block_record(40, 9_900),
                block_record(41, 9_912),
                block_record(42, 9_924),
            ],
            SubscribeFinality::Optimistic,
            10_000,
        )
        .expect("select startup block");

        assert_eq!(items.len(), 1);
        assert_eq!(items[0].0.block_number(), BlockNumber(42));
        assert_eq!(items[0].1.cursor.sequence, 42);
    }

    #[test]
    fn readiness_snapshot_cannot_follow_a_newer_optimistic_preview() {
        let market = resolve_markets(&["ETH/USDC".to_owned()]).expect("market")[0];
        let older_record = price_record(market, 86, 10_000);
        let preview_record = price_record(market, 88, 10_024);
        let older = local_item(SubscribeProtocol::UniswapV3, &[market], &older_record)
            .expect("decode older price")
            .expect("older market");
        let preview = local_item(SubscribeProtocol::UniswapV3, &[market], &preview_record)
            .expect("decode preview price")
            .expect("preview market");
        let mut preview_blocks = BTreeMap::from([(
            preview.block_number().0,
            (preview_record.block, vec![preview.clone()]),
        )]);
        let mut previewed = HashSet::from([observation_key(&preview)]);

        assert!(preview_is_at_or_after(&preview_blocks, &older));
        assert!(preview_is_at_or_after(&preview_blocks, &preview));
        assert!(matches!(
            reconcile_preview_handoff(
                &mut preview_blocks,
                &mut previewed,
                &older,
                ChangeDirection::Apply,
            ),
            PreviewHandoffAction::Suppress
        ));

        let newer_record = price_record(market, 89, 10_036);
        let newer = local_item(SubscribeProtocol::UniswapV3, &[market], &newer_record)
            .expect("decode newer price")
            .expect("newer market");
        assert!(!preview_is_at_or_after(&preview_blocks, &newer));

        assert!(matches!(
            reconcile_preview_handoff(
                &mut preview_blocks,
                &mut previewed,
                &preview,
                ChangeDirection::Apply,
            ),
            PreviewHandoffAction::Suppress
        ));
        assert!(previewed.is_empty());
        assert!(preview_blocks.is_empty());
        assert!(matches!(
            reconcile_preview_handoff(
                &mut preview_blocks,
                &mut previewed,
                &newer,
                ChangeDirection::Apply,
            ),
            PreviewHandoffAction::Render { reverted } if reverted.is_empty()
        ));
    }

    #[test]
    fn delayed_durable_block_overlap_stays_behind_the_preview_barrier() {
        let preview_record = block_record(65, 10_024);
        let preview = local_item(SubscribeProtocol::Blocks, &[], &preview_record)
            .expect("decode preview block")
            .expect("preview block");
        let mut preview_blocks = BTreeMap::from([(
            preview.block_number().0,
            (preview_record.block, vec![preview.clone()]),
        )]);
        let mut previewed = HashSet::from([observation_key(&preview)]);

        for number in 58..65 {
            let record = block_record(number, 9_900 + number);
            let delayed = local_item(SubscribeProtocol::Blocks, &[], &record)
                .expect("decode delayed block")
                .expect("delayed block");
            assert!(matches!(
                reconcile_preview_handoff(
                    &mut preview_blocks,
                    &mut previewed,
                    &delayed,
                    record.direction,
                ),
                PreviewHandoffAction::Suppress
            ));
        }
        assert!(previewed.contains(&observation_key(&preview)));

        assert!(matches!(
            reconcile_preview_handoff(
                &mut preview_blocks,
                &mut previewed,
                &preview,
                preview_record.direction,
            ),
            PreviewHandoffAction::Suppress
        ));
        assert!(previewed.is_empty());
        assert!(preview_blocks.is_empty());

        let next_record = block_record(66, 10_036);
        let next = local_item(SubscribeProtocol::Blocks, &[], &next_record)
            .expect("decode next block")
            .expect("next block");
        assert!(matches!(
            reconcile_preview_handoff(
                &mut preview_blocks,
                &mut previewed,
                &next,
                next_record.direction,
            ),
            PreviewHandoffAction::Render { reverted } if reverted.is_empty()
        ));
    }

    #[test]
    fn replacement_at_preview_height_reverts_the_preview_first() {
        let preview_record = block_record(65, 10_024);
        let preview = local_item(SubscribeProtocol::Blocks, &[], &preview_record)
            .expect("decode preview block")
            .expect("preview block");
        let mut replacement = preview.clone();
        let SubscriptionItem::Block(entity) = &mut replacement else {
            panic!("expected block summary");
        };
        entity.block_hash = BlockHash::new([99; 32]);
        let mut preview_blocks = BTreeMap::from([(
            preview.block_number().0,
            (preview_record.block, vec![preview.clone()]),
        )]);
        let mut previewed = HashSet::from([observation_key(&preview)]);

        let action = reconcile_preview_handoff(
            &mut preview_blocks,
            &mut previewed,
            &replacement,
            ChangeDirection::Apply,
        );
        let PreviewHandoffAction::Render { reverted } = action else {
            panic!("replacement must be rendered");
        };
        assert_eq!(reverted.len(), 1);
        assert_eq!(reverted[0].0, preview_record.block);
        assert_eq!(observation_key(&reverted[0].1), observation_key(&preview));
        assert!(previewed.is_empty());
        assert!(preview_blocks.is_empty());
    }

    #[test]
    fn mismatched_reorg_revert_preserves_the_preview_barrier() {
        let preview_record = block_record(65, 10_024);
        let preview = local_item(SubscribeProtocol::Blocks, &[], &preview_record)
            .expect("decode preview block")
            .expect("preview block");
        let preview_key = observation_key(&preview);
        let mut preview_blocks = BTreeMap::from([(
            preview.block_number().0,
            (preview_record.block, vec![preview]),
        )]);
        let mut previewed = HashSet::from([preview_key]);
        let unrelated_revert = BlockRef {
            hash: BlockHash::new([99; 32]),
            ..preview_record.block
        };

        assert!(
            revert_preview_block(&mut preview_blocks, &mut previewed, unrelated_revert).is_none()
        );
        assert!(preview_blocks.contains_key(&65));
        assert!(previewed.contains(&preview_key));

        let reverted =
            revert_preview_block(&mut preview_blocks, &mut previewed, preview_record.block)
                .expect("matching preview reverts");
        assert_eq!(reverted.len(), 1);
        assert!(preview_blocks.is_empty());
        assert!(previewed.is_empty());
    }

    #[test]
    fn readiness_does_not_emit_a_lone_undo() {
        let market = resolve_markets(&["ETH/USDC".to_owned()]).expect("market")[0];
        let apply = price_record(market, 1, 9_990);
        let mut undo = apply.clone();
        undo.cursor.sequence = 2;
        undo.direction = ChangeDirection::Undo;

        let items = latest_fresh_items(
            SubscribeProtocol::UniswapV3,
            &[market],
            vec![apply, undo],
            SubscribeFinality::Optimistic,
            10_000,
        )
        .expect("startup state");
        assert!(items.is_empty());
    }

    #[test]
    fn attached_subscription_hides_stale_node_catch_up_observations() {
        let market = resolve_markets(&["ETH/USDC".to_owned()]).expect("market")[0];
        let envelope = AttachedEnvelope {
            sequence: "1".to_owned(),
            cursor: "cursor".to_owned(),
            operation: "apply".to_owned(),
            block: json!({
                "timestamp": unix_seconds()
                    .saturating_sub(OPTIMISTIC_UPDATE_MAX_AGE.as_secs() + 1),
            }),
            finality: "optimistic".to_owned(),
            kind: "uniswap.price.observation.apply".to_owned(),
            data: Some(
                serde_json::to_value(AttachedPoolPrice {
                    pool: market.pool.to_owned(),
                    kind: "v3".to_owned(),
                    reserve0: None,
                    reserve1: None,
                    amount0: None,
                    amount1: None,
                    sqrt_price_x96: None,
                    block_number: 1,
                    block_hash: format!("0x{}", hex::encode([1; 32])),
                    log_index: 0,
                    finality: "optimistic".to_owned(),
                })
                .expect("price JSON"),
            ),
            extra: BTreeMap::new(),
        };

        assert!(
            !render_attached(
                SubscribeProtocol::UniswapV3,
                SubscribeFormat::Raw,
                SubscribeFinality::Optimistic,
                &[market],
                envelope,
                &Value::Null,
            )
            .expect("filter catch-up observation")
        );
    }

    #[test]
    fn volume_uses_the_first_market_token_and_its_decimals() {
        let eth_usdc = resolve_markets(&["ETH/USDC".to_owned()]).expect("market")[0];
        assert_eq!(
            base_volume(
                &eth_usdc,
                I256::unchecked_from(2_500_000_000_i64),
                I256::unchecked_from(-1_250_000_000_000_000_000_i128),
            ),
            "1.25"
        );

        let wbtc_eth = resolve_markets(&["WBTC/ETH".to_owned()]).expect("market")[0];
        assert_eq!(
            base_volume(
                &wbtc_eth,
                I256::unchecked_from(12_345_678_i64),
                I256::unchecked_from(-10_000_000_000_000_000_i64),
            ),
            "0.12345678"
        );
    }

    #[test]
    fn checkpoint_selection_requires_exact_root_and_slot_quorum() {
        let selected = select_checkpoint_quorum(
            &[
                checkpoint_provider("https://ethereum-beacon-api.publicnode.com/", 0xaa, 100),
                checkpoint_provider("https://mainnet.checkpoint.sigp.io/", 0xaa, 100),
                checkpoint_provider("https://beaconstate-mainnet.chainsafe.io/", 0xbb, 100),
            ],
            Vec::new(),
            3,
            2,
        )
        .expect("quorum");
        assert_eq!(selected.slot, 100);
        assert_eq!(selected.root, format!("0x{}", hex::encode([0xaa; 32])));
        assert_eq!(selected.agreeing_providers.len(), 2);
        assert_eq!(selected.beacon_api_endpoints.len(), 1);

        let error = select_checkpoint_quorum(
            &[
                checkpoint_provider("https://a.example/", 0xaa, 100),
                checkpoint_provider("https://b.example/", 0xbb, 100),
            ],
            vec!["https://c.example/: unavailable".to_owned()],
            3,
            2,
        )
        .expect_err("no quorum");
        assert!(error.to_string().contains("quorum 2/3 was not reached"));
    }

    #[test]
    fn locally_verified_checkpoint_skips_provider_bootstrap_while_recent() {
        let slot = 15_000_000;
        let now =
            leani_finality_beacon_api::MAINNET_GENESIS_TIME + slot * MAINNET_SLOT_SECONDS + 60;
        let mut checkpoint = CachedCheckpoint {
            schema: "leani.verified-checkpoint.v1".to_owned(),
            root: format!("0x{}", hex::encode([0xaa; 32])),
            slot,
            execution_block_hash: Some(format!("0x{}", hex::encode([0xbb; 32]))),
            trust: CheckpointTrust::LocallyVerified,
            accepted_providers: Vec::new(),
            beacon_api_endpoints: Vec::new(),
            updated_at_unix_seconds: now,
        };
        let options = subscribe_options(Vec::new());
        assert!(cached_checkpoint_is_reusable(
            &checkpoint,
            &options,
            false,
            now
        ));
        assert!(!cached_checkpoint_is_reusable(
            &checkpoint,
            &options,
            true,
            now
        ));
        checkpoint
            .beacon_api_endpoints
            .push("https://ethereum-beacon-api.publicnode.com/".to_owned());
        assert!(cached_checkpoint_is_reusable(
            &checkpoint,
            &options,
            true,
            now
        ));
        assert!(!cached_checkpoint_is_reusable(
            &checkpoint,
            &options,
            false,
            now + LOCALLY_VERIFIED_CHECKPOINT_MAX_AGE.as_secs() + 1
        ));
    }

    #[test]
    fn checkpoint_cache_round_trips_and_replaces_atomically() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("checkpoint.json");
        let mut checkpoint = CachedCheckpoint {
            schema: "leani.verified-checkpoint.v1".to_owned(),
            root: format!("0x{}", hex::encode([0xaa; 32])),
            slot: 15_000_000,
            execution_block_hash: None,
            trust: CheckpointTrust::ProviderQuorum,
            accepted_providers: vec!["https://a.example/".to_owned()],
            beacon_api_endpoints: Vec::new(),
            updated_at_unix_seconds: 1,
        };
        write_checkpoint_cache(&path, &checkpoint).expect("write cache");
        assert_eq!(
            read_checkpoint_cache(&path).expect("read cache").slot,
            15_000_000
        );

        checkpoint.slot += 1;
        checkpoint.trust = CheckpointTrust::LocallyVerified;
        write_checkpoint_cache(&path, &checkpoint).expect("replace cache");
        let persisted = read_checkpoint_cache(&path).expect("read replacement");
        assert_eq!(persisted.slot, 15_000_001);
        assert_eq!(persisted.trust, CheckpointTrust::LocallyVerified);
    }

    #[test]
    fn peer_cache_merge_reuses_sibling_candidates_and_keeps_best_metadata() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let subscriptions = directory.path().join("subscriptions");
        let target = subscriptions.join("0123456789abcdef");
        let sibling = subscriptions.join("fedcba9876543210");
        fs::create_dir_all(&target).expect("target state");
        fs::create_dir_all(&sibling).expect("sibling state");
        write_peer_cache(
            &target.join("execution-peers.json"),
            &[
                json!({"record": "enode://shared", "reputation": 0}),
                json!({"record": "enode://local", "reputation": 1}),
            ],
        )
        .expect("target cache");
        write_peer_cache(
            &sibling.join("execution-peers.json"),
            &[
                json!({"record": "enode://shared", "reputation": 12}),
                json!({"record": "enode://imported", "reputation": 0, "fork_id": {}}),
            ],
        )
        .expect("sibling cache");
        fs::write(
            target.join("execution-peer-quality.json"),
            serde_json::to_vec_pretty(&json!({
                "schema_version": 1,
                "peers": {
                    "local-peer": {
                        "fork_compatible": true,
                        "highest_served_block": 10,
                        "last_header_success_unix_ms": 1,
                        "last_body_success_unix_ms": 1,
                        "last_receipt_success_unix_ms": null,
                        "response_latency_ms": 30,
                        "last_failure_reason": null,
                        "last_failure_unix_ms": null,
                        "qualification": "body_serving"
                    }
                }
            }))
            .expect("target quality JSON"),
        )
        .expect("target quality cache");
        fs::write(
            sibling.join("execution-peer-quality.json"),
            serde_json::to_vec_pretty(&json!({
                "schema_version": 1,
                "peers": {
                    "imported-peer": {
                        "fork_compatible": true,
                        "highest_served_block": 12,
                        "last_header_success_unix_ms": 2,
                        "last_body_success_unix_ms": null,
                        "last_receipt_success_unix_ms": 2,
                        "response_latency_ms": 20,
                        "last_failure_reason": null,
                        "last_failure_unix_ms": null,
                        "qualification": null
                    }
                }
            }))
            .expect("sibling quality JSON"),
        )
        .expect("sibling quality cache");

        let merged = merge_local_execution_peer_caches(None, &target, 3)
            .expect("merge caches")
            .expect("peer cache exists");
        assert_eq!(
            merged,
            PeerCacheMerge {
                total: 3,
                imported: 1,
            }
        );
        let entries =
            read_peer_cache_entries(&target.join("execution-peers.json")).expect("merged entries");
        assert_eq!(entries.len(), 3);
        assert!(entries.iter().any(|entry| {
            peer_cache_record(entry).as_deref() == Some("enode://shared")
                && entry["reputation"] == 12
        }));
        assert!(
            entries
                .iter()
                .any(|entry| { peer_cache_record(entry).as_deref() == Some("enode://imported") })
        );
        let quality: Value = serde_json::from_slice(
            &fs::read(target.join("execution-peer-quality.json")).expect("merged quality cache"),
        )
        .expect("parse merged quality cache");
        assert!(quality["peers"]["local-peer"].is_object());
        assert!(quality["peers"]["imported-peer"].is_object());
    }

    #[test]
    fn peer_search_status_reports_current_connections_and_churn() {
        let telemetry = leani_source_api::NetworkTelemetry::default();
        telemetry.peer_session_established();
        telemetry.peer_session_established();
        telemetry.peer_session_closed(leani_source_api::NetworkDisconnectReason::TooManyPeers);
        telemetry.peer_session_closed(leani_source_api::NetworkDisconnectReason::ConnectionClosed);

        let status = format_peer_search_status(&telemetry.snapshot());
        assert_eq!(
            status,
            "0 connected, 0/0 body-serving, 0 candidates known; 2 active sessions established, 2 closed; up to 0 concurrent dials; connection_closed=1, too_many_peers=1"
        );
    }

    #[test]
    fn subscription_state_identity_is_independent_of_market_order() {
        let left =
            resolve_markets(&["ETH/USDC".to_owned(), "ETH/USDT".to_owned()]).expect("left markets");
        let right = resolve_markets(&["ETH/USDT".to_owned(), "ETH/USDC".to_owned()])
            .expect("right markets");
        let left = subscription_data_dir_for(
            None,
            SubscribeProtocol::UniswapV3,
            &left,
            None,
            SubscribeFinality::Optimistic,
            Path::new("."),
        )
        .expect("left identity");
        let right = subscription_data_dir_for(
            None,
            SubscribeProtocol::UniswapV3,
            &right,
            None,
            SubscribeFinality::Optimistic,
            Path::new("."),
        )
        .expect("right identity");
        assert_eq!(left.file_name(), right.file_name());
    }

    #[test]
    fn block_and_uniswap_subscriptions_have_distinct_state() {
        let market = resolve_markets(&["ETH/USDC".to_owned()]).expect("market");
        let blocks = subscription_data_dir_for(
            None,
            SubscribeProtocol::Blocks,
            &[],
            None,
            SubscribeFinality::Optimistic,
            Path::new("."),
        )
        .expect("blocks identity");
        let uniswap = subscription_data_dir_for(
            None,
            SubscribeProtocol::UniswapV3,
            &market,
            None,
            SubscribeFinality::Optimistic,
            Path::new("."),
        )
        .expect("Uniswap identity");
        assert_ne!(blocks.file_name(), uniswap.file_name());
    }

    #[test]
    fn reset_removes_only_the_exact_derived_subscription_directory() {
        let root = tempfile::tempdir().expect("temporary directory");
        let subscriptions = root.path().join("subscriptions");
        let target = subscriptions.join("0123456789abcdef");
        let sibling = subscriptions.join("fedcba9876543210");
        fs::create_dir_all(&target).expect("target directory");
        fs::create_dir_all(&sibling).expect("sibling directory");
        fs::write(target.join("checkpoint.json"), b"fixture").expect("target state");
        fs::write(sibling.join("checkpoint.json"), b"sibling").expect("sibling state");

        assert!(reset_subscription_directory(&target, true).expect("reset target"));
        assert!(local_state::is_runtime_directory(&target));
        assert!(!target.join("checkpoint.json").exists());
        assert!(sibling.join("checkpoint.json").is_file());
        assert!(reset_subscription_directory(&target, true).is_ok());
        assert!(reset_subscription_directory(root.path(), true).is_err());
    }

    #[test]
    fn parses_sse_across_standard_line_endings() {
        let unix = b"event: apply\ndata: {\"sequence\":\"1\"}\n\nrest";
        assert_eq!(sse_event_end(unix), Some(37));
        let parsed = parse_sse_event(&unix[..35]).expect("data");
        assert_eq!(parsed.event.as_deref(), Some("apply"));
        assert_eq!(parsed.data.as_deref(), Some("{\"sequence\":\"1\"}"));
        let windows = b"data: {}\r\n\r\n";
        assert_eq!(sse_event_end(windows), Some(windows.len()));
    }

    #[test]
    fn embedded_processor_contains_every_requested_pool() {
        let markets =
            resolve_markets(&["ETH/USDC".to_owned(), "ETH/USDT".to_owned()]).expect("markets");
        let processor = subscription_processor(
            SubscribeProtocol::UniswapV3,
            &markets,
            SubscribeFinality::Optimistic,
        )
        .expect("processor");
        let pools = processor.settings["pools"].as_array().expect("pools");
        assert_eq!(pools.len(), 2);
        assert_eq!(processor.history_mode, ProcessorHistoryMode::OnDemand);
        assert!(matches!(
            processor.publish,
            PublishMode::OptimisticAndFinalized
        ));
    }

    #[test]
    fn embedded_block_processor_requires_bodies_but_not_receipts() {
        let processor = subscription_processor(
            SubscribeProtocol::Blocks,
            &[],
            SubscribeFinality::Optimistic,
        )
        .expect("processor");
        let registry = ProcessorRegistry::standard();
        let processor = registry.instantiate(&processor, 1).expect("instantiate");
        assert_eq!(
            processor.descriptor().requirements[0].capabilities,
            leani_primitives::CapabilitySet::of(leani_primitives::Capability::Header)
                .with(leani_primitives::Capability::Body)
        );
    }
}
