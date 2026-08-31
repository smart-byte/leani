//! Lightweight live market subscriptions backed by native processors.

use std::{
    collections::{BTreeMap, HashSet},
    fs,
    io::{self, IsTerminal, Write},
    net::{Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use alloy_primitives::{Address as AlloyAddress, I256, U256, U512};
use anyhow::{Context, Result, bail};
use futures::StreamExt as _;
use leani_primitives::{Address, ChainId, Finality};
use leani_processor_uniswap::PoolPriceEntity;
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
    process::{Exit, configured_store_config, spawn_embedded_network_runtime},
    processors::ProcessorRegistry,
    uniswap_markets::{MARKET_CATALOG, Market, Token, processor_config, resolve_markets},
};

const PROCESSOR_INSTANCE: &str = "cli-uniswap-v3-prices";
const CHECKPOINT_CACHE_MAX_AGE: Duration = Duration::from_hours(12);
const LOCALLY_VERIFIED_CHECKPOINT_MAX_AGE: Duration = Duration::from_hours(13 * 24);
const MAINNET_SLOT_SECONDS: u64 = 12;
const OPTIMISTIC_PRICE_MAX_AGE: Duration = Duration::from_secs(90);
const FINALIZED_PRICE_MAX_AGE: Duration = Duration::from_mins(30);
const PRICE_PRECISION: usize = 8;
const STARTUP_PRICE_SCAN_LIMIT: usize = 10_000;

pub(crate) struct SubscribeOptions {
    pub protocol: SubscribeProtocol,
    pub markets: Vec<String>,
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

#[derive(Debug)]
struct RenderedPrice<'a> {
    market: &'a Market,
    price: String,
    base_volume: String,
    block_number: u64,
    block_hash: String,
    timestamp: u64,
    log_index: u32,
    finality: String,
    operation: String,
    sqrt_price_x96: String,
    amount0: String,
    amount1: String,
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
    data: Option<AttachedPoolPrice>,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
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
    if options.protocol != SubscribeProtocol::UniswapV3 {
        bail!("unsupported subscription protocol");
    }
    let markets = resolve_markets(&options.markets)?;
    let configured_path = configured_path(&options);
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
            subscribe_embedded(&options, &markets, configured_path.as_deref(), registry).await
        }
    }
}

pub(crate) async fn initialize_checkpoint(
    checkpoint_urls: Vec<Url>,
    checkpoint_quorum: usize,
    accept_checkpoint: bool,
    data_dir: &Path,
) -> Result<InitializedCheckpoint> {
    fs::create_dir_all(data_dir)
        .with_context(|| format!("create data directory {}", data_dir.display()))?;
    let checkpoint = trusted_checkpoint(
        &SubscribeOptions {
            protocol: SubscribeProtocol::UniswapV3,
            markets: Vec::new(),
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

fn configured_path(options: &SubscribeOptions) -> Option<PathBuf> {
    options.requested_config.clone().or_else(|| {
        let local = options.working_directory.join("leani.toml");
        local.is_file().then_some(local)
    })
}

fn infer_endpoint(path: Option<&Path>) -> Result<Option<Url>> {
    let Some(path) = path else {
        return Ok(None);
    };
    let config = Config::load(path)?;
    let SocketAddr::V4(bind) = config.api.bind else {
        return Ok(None);
    };
    let ip = if bind.ip().is_unspecified() {
        Ipv4Addr::LOCALHOST
    } else {
        *bind.ip()
    };
    Url::parse(&format!("http://{ip}:{}/", bind.port()))
        .map(Some)
        .context("construct local API endpoint")
}

async fn endpoint_is_reachable(options: &SubscribeOptions, endpoint: Option<&Url>) -> bool {
    let Some(endpoint) = endpoint else {
        return false;
    };
    let Ok(url) = processor_url(endpoint, &options.processor, "changes/head") else {
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
    if seed_configured_peer_cache(config_path, &data_dir)? {
        eprintln!("leani: reusing cached execution peers from the configured node context");
    }
    let config = embedded_config(options, markets, config_path, &data_dir).await?;
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
    let mut runtime = spawn_embedded_network_runtime(config.clone(), store.clone(), processors);

    eprintln!(
        "leani: embedded Uniswap V3 processor active for {}; connecting to Ethereum peers...",
        markets
            .iter()
            .map(|market| market.symbol)
            .collect::<Vec<_>>()
            .join(", ")
    );
    eprintln!(
        "leani: first-run peer discovery can take about a minute; peer state is cached for later runs"
    );
    let mut announced_ready = false;
    loop {
        if runtime.is_finished() {
            bail!("embedded network runtime stopped before the subscription completed");
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
                        u64::try_from(STARTUP_PRICE_SCAN_LIMIT)
                            .expect("startup scan limit fits u64"),
                    ),
                    STARTUP_PRICE_SCAN_LIMIT,
                )
                .await?;
            let startup_prices =
                latest_fresh_prices(markets, startup_records, options.finality, unix_seconds())?;
            after = ready_after;
            announced_ready = true;
            if startup_prices.is_empty() {
                eprintln!(
                    "leani: execution peers and verified finality are connected; waiting for a fresh matching swap..."
                );
            } else {
                eprintln!("leani: live execution and verified finality are ready");
            }
            for (market, entity, record) in startup_prices {
                render_local(options.format, &market, &entity, &record)?;
                if options.once {
                    persist_current_verified_anchor(&runtime, &data_dir, &config)?;
                    runtime.shutdown().await;
                    return Ok(Exit::Success);
                }
            }
        }
        if !announced_ready {
            tokio::select! {
                result = tokio::signal::ctrl_c() => {
                    result.context("install Ctrl-C handler")?;
                    persist_current_verified_anchor(&runtime, &data_dir, &config)?;
                    runtime.shutdown().await;
                    return Ok(Exit::Success);
                }
                changed = runtime.verified_anchor.changed() => {
                    changed.context("verified anchor channel closed")?;
                    persist_current_verified_anchor(&runtime, &data_dir, &config)?;
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
        for record in records {
            after = record.cursor.sequence;
            if !price_is_fresh(options.finality, record.block.timestamp, unix_seconds()) {
                continue;
            }
            if let Some((market, entity)) = local_price(markets, &record)? {
                render_local(options.format, market, &entity, &record)?;
                if options.once {
                    persist_current_verified_anchor(&runtime, &data_dir, &config)?;
                    runtime.shutdown().await;
                    return Ok(Exit::Success);
                }
            }
        }
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                result.context("install Ctrl-C handler")?;
                persist_current_verified_anchor(&runtime, &data_dir, &config)?;
                runtime.shutdown().await;
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
        bail!("the built-in Uniswap V3 market catalog currently supports Ethereum mainnet only");
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
    config.processors = vec![subscription_processor(markets, options.finality)?];
    Ok(config.validate()?.into_inner())
}

fn subscription_processor(
    markets: &[Market],
    finality: SubscribeFinality,
) -> Result<crate::config::ProcessorConfig> {
    processor_config(
        markets,
        PROCESSOR_INSTANCE,
        finality == SubscribeFinality::Finalized,
    )
}

fn subscription_data_dir(
    options: &SubscribeOptions,
    markets: &[Market],
    config_path: Option<&Path>,
) -> Result<PathBuf> {
    if let Some(path) = &options.data_dir {
        return Ok(path.clone());
    }
    let root = if let Some(path) = config_path {
        Config::load(path)?.data_dir.join("subscriptions")
    } else if let Some(path) = std::env::var_os("XDG_DATA_HOME") {
        PathBuf::from(path).join("leani/subscriptions")
    } else if let Some(path) = std::env::var_os("HOME") {
        PathBuf::from(path).join(".local/share/leani/subscriptions")
    } else {
        options.working_directory.join(".leani/subscriptions")
    };
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"uniswap-observations/2.0.0");
    for market in markets {
        hasher.update(market.pool.as_bytes());
    }
    hasher.update(match options.finality {
        SubscribeFinality::Optimistic => b"optimistic",
        SubscribeFinality::Finalized => b"finalized",
    });
    Ok(root.join(&hasher.finalize().to_hex()[..16]))
}

fn seed_configured_peer_cache(config_path: Option<&Path>, data_dir: &Path) -> Result<bool> {
    let Some(config_path) = config_path else {
        return Ok(false);
    };
    let source = Config::load(config_path)?
        .data_dir
        .join("execution-peers.json");
    seed_peer_cache(&source, &data_dir.join("execution-peers.json"))
}

fn seed_peer_cache(source: &Path, destination: &Path) -> Result<bool> {
    if source == destination || !source.is_file() || destination.exists() {
        return Ok(false);
    }
    let parent = destination
        .parent()
        .context("execution peer cache path has no parent directory")?;
    fs::create_dir_all(parent)
        .with_context(|| format!("create peer cache directory {}", parent.display()))?;
    let mut input = fs::File::open(source)
        .with_context(|| format!("open configured peer cache {}", source.display()))?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)
        .with_context(|| format!("create peer cache in {}", parent.display()))?;
    io::copy(&mut input, temporary.as_file_mut())?;
    temporary.as_file_mut().sync_all()?;
    match temporary.persist_noclobber(destination) {
        Ok(_) => Ok(true),
        Err(error) if error.error.kind() == io::ErrorKind::AlreadyExists => Ok(false),
        Err(error) => Err(error.error)
            .with_context(|| format!("seed peer cache at {}", destination.display())),
    }
}

async fn trusted_checkpoint(
    options: &SubscribeOptions,
    data_dir: &Path,
    require_beacon_api: bool,
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
    write_checkpoint_cache(&cache_path, &checkpoint)?;
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

fn local_price<'a>(
    markets: &'a [Market],
    record: &ChangeRecord,
) -> Result<Option<(&'a Market, PoolPriceEntity)>> {
    if record.change.kind != "uniswap.price.observation" {
        return Ok(None);
    }
    let entity: PoolPriceEntity = postcard::from_bytes(&record.change.payload)
        .context("decode embedded Uniswap observation")?;
    let market = markets
        .iter()
        .find(|market| address_matches(entity.pool, market.pool));
    Ok(market.map(|market| (market, entity)))
}

fn latest_fresh_prices(
    markets: &[Market],
    records: Vec<ChangeRecord>,
    finality: SubscribeFinality,
    now: u64,
) -> Result<Vec<(Market, PoolPriceEntity, ChangeRecord)>> {
    let mut latest = BTreeMap::new();
    for record in records {
        if !price_is_fresh(finality, record.block.timestamp, now) {
            continue;
        }
        if let Some((market, entity)) = local_price(markets, &record)? {
            latest.insert(market.symbol, (*market, entity, record));
        }
    }
    Ok(latest.into_values().collect())
}

fn address_matches(address: Address, expected: &str) -> bool {
    expected
        .parse::<AlloyAddress>()
        .is_ok_and(|expected| Address::from(expected) == address)
}

fn render_local(
    format: SubscribeFormat,
    market: &Market,
    entity: &PoolPriceEntity,
    record: &ChangeRecord,
) -> Result<()> {
    if format == SubscribeFormat::Raw {
        println!("{}", serde_json::to_string(record)?);
        io::stdout().flush()?;
        return Ok(());
    }
    let sqrt = entity
        .sqrt_price_x96
        .context("Uniswap V3 observation omitted sqrtPriceX96")?;
    let amount0 = signed_amount(
        entity
            .amount0
            .context("Uniswap V3 observation omitted Swap amount0")?,
    );
    let amount1 = signed_amount(
        entity
            .amount1
            .context("Uniswap V3 observation omitted Swap amount1")?,
    );
    let price = v3_price(market, U256::from_be_bytes(sqrt.0), PRICE_PRECISION)?;
    let output = RenderedPrice {
        market,
        price,
        base_volume: base_volume(market, amount0, amount1),
        block_number: record.block.number.0,
        block_hash: format!("0x{}", hex::encode(record.block.hash.0)),
        timestamp: record.block.timestamp,
        log_index: entity.log_index,
        finality: finality_name(record.finality).to_owned(),
        operation: direction_name(record.direction).to_owned(),
        sqrt_price_x96: U256::from_be_bytes(sqrt.0).to_string(),
        amount0: amount0.to_string(),
        amount1: amount1.to_string(),
        sequence: Some(record.cursor.sequence.to_string()),
    };
    render_price(format, &output)
}

fn render_price(format: SubscribeFormat, output: &RenderedPrice<'_>) -> Result<()> {
    match format {
        SubscribeFormat::Pretty => {
            let timestamp = readable_timestamp(output.timestamp)?;
            println!(
                "{}  {} {}  volume={} {}  block={}  {}{}",
                timestamp,
                output.market.symbol,
                output.price,
                output.base_volume,
                base_token(output.market).symbol,
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

fn readable_timestamp(timestamp: u64) -> Result<String> {
    let timestamp = i64::try_from(timestamp).context("block timestamp exceeds the Unix range")?;
    let timestamp = chrono::DateTime::from_timestamp(timestamp, 0)
        .context("block timestamp is outside the supported calendar range")?;
    Ok(timestamp.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
}

fn price_is_fresh(finality: SubscribeFinality, timestamp: u64, now: u64) -> bool {
    let maximum_age = match finality {
        SubscribeFinality::Optimistic => OPTIMISTIC_PRICE_MAX_AGE,
        SubscribeFinality::Finalized => FINALIZED_PRICE_MAX_AGE,
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

async fn subscribe_attached(
    options: &SubscribeOptions,
    markets: &[Market],
    endpoint: Url,
) -> Result<Exit> {
    let client = reqwest::Client::new();
    validate_attached_markets(&client, options, markets, &endpoint).await?;
    let head_url = processor_url(&endpoint, &options.processor, "changes/head")?;
    let head = authorized(client.get(head_url), options.token.as_deref())
        .send()
        .await?
        .error_for_status()?
        .json::<ChangeHead>()
        .await?;
    let mut cursor = head.cursor;
    eprintln!(
        "leani: attached to {} for {}; waiting for fresh prices...",
        endpoint,
        markets
            .iter()
            .map(|market| market.symbol)
            .collect::<Vec<_>>()
            .join(", ")
    );
    let mut backoff = Duration::from_secs(1);
    loop {
        let mut stream_url = processor_url(&endpoint, &options.processor, "stream")?;
        if let Some(cursor) = &cursor {
            stream_url.query_pairs_mut().append_pair("after", cursor);
        }
        let response = authorized(client.get(stream_url), options.token.as_deref())
            .send()
            .await
            .and_then(reqwest::Response::error_for_status);
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                eprintln!(
                    "leani: stream connection failed ({error}); reconnecting in {}s",
                    backoff.as_secs()
                );
                if wait_for_reconnect(backoff).await? {
                    return Ok(Exit::Success);
                }
                backoff = backoff.saturating_mul(2).min(Duration::from_secs(30));
                continue;
            }
        };
        backoff = Duration::from_secs(1);
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
            while let Some(end) = sse_event_end(&buffer) {
                let event = buffer.drain(..end).collect::<Vec<_>>();
                let delimiter = if event.ends_with(b"\r\n\r\n") { 4 } else { 2 };
                let event = &event[..event.len().saturating_sub(delimiter)];
                let Some(data) = sse_data(event)? else {
                    continue;
                };
                let value: Value = serde_json::from_str(&data)?;
                let Ok(envelope) = serde_json::from_value::<AttachedEnvelope>(value.clone()) else {
                    continue;
                };
                cursor = Some(envelope.cursor.clone());
                if render_attached(options.format, options.finality, markets, envelope, &value)?
                    && options.once
                {
                    return Ok(Exit::Success);
                }
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
    format: SubscribeFormat,
    finality: SubscribeFinality,
    markets: &[Market],
    envelope: AttachedEnvelope,
    raw: &Value,
) -> Result<bool> {
    if !envelope.kind.starts_with("uniswap.price.observation.") {
        return Ok(false);
    }
    let Some(entity) = envelope.data else {
        return Ok(false);
    };
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
        .unwrap_or_default();
    if !price_is_fresh(finality, timestamp, unix_seconds()) {
        return Ok(false);
    }
    if format == SubscribeFormat::Raw {
        println!("{}", serde_json::to_string(&raw)?);
        io::stdout().flush()?;
        return Ok(true);
    }
    let sqrt = entity
        .sqrt_price_x96
        .as_deref()
        .context("Uniswap V3 observation omitted sqrtPriceX96")?;
    let amount0 = entity
        .amount0
        .as_deref()
        .context("Uniswap V3 observation omitted Swap amount0")?
        .parse::<I256>()
        .context("invalid signed amount0 from node API")?;
    let amount1 = entity
        .amount1
        .as_deref()
        .context("Uniswap V3 observation omitted Swap amount1")?
        .parse::<I256>()
        .context("invalid signed amount1 from node API")?;
    let sqrt_value = U256::from_str(sqrt).context("invalid sqrtPriceX96 from node API")?;
    let price = v3_price(market, sqrt_value, PRICE_PRECISION)?;
    let output = RenderedPrice {
        market,
        price,
        base_volume: base_volume(market, amount0, amount1),
        block_number: entity.block_number,
        block_hash: entity.block_hash,
        timestamp,
        log_index: entity.log_index,
        finality: entity.finality,
        operation: envelope.operation,
        sqrt_price_x96: sqrt.to_owned(),
        amount0: amount0.to_string(),
        amount1: amount1.to_string(),
        sequence: Some(envelope.sequence),
    };
    render_price(format, &output)?;
    Ok(true)
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
    buffer
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|position| position + 4)
        .or_else(|| {
            buffer
                .windows(2)
                .position(|window| window == b"\n\n")
                .map(|position| position + 2)
        })
}

fn sse_data(event: &[u8]) -> Result<Option<String>> {
    let event = std::str::from_utf8(event).context("node SSE stream is not UTF-8")?;
    let data = event
        .lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(str::trim_start)
        .collect::<Vec<_>>();
    Ok((!data.is_empty()).then(|| data.join("\n")))
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
            markets: vec!["ETH/USDC".to_owned()],
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
    fn catch_up_prices_stay_silent_until_the_stream_is_current() {
        let now = 10_000;
        assert!(price_is_fresh(
            SubscribeFinality::Optimistic,
            now - OPTIMISTIC_PRICE_MAX_AGE.as_secs(),
            now
        ));
        assert!(!price_is_fresh(
            SubscribeFinality::Optimistic,
            now - OPTIMISTIC_PRICE_MAX_AGE.as_secs() - 1,
            now
        ));
        assert!(price_is_fresh(
            SubscribeFinality::Finalized,
            now - 15 * 60,
            now
        ));
    }

    #[test]
    fn readiness_keeps_only_the_newest_fresh_price_per_market() {
        let eth = resolve_markets(&["ETH/USDC".to_owned()]).expect("ETH market")[0];
        let wbtc = resolve_markets(&["WBTC/ETH".to_owned()]).expect("WBTC market")[0];
        let prices = latest_fresh_prices(
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
        assert_eq!(prices[0].0.symbol, "ETH/USDC");
        assert_eq!(prices[0].2.cursor.sequence, 4);
        assert_eq!(prices[1].0.symbol, "WBTC/ETH");
        assert_eq!(prices[1].2.cursor.sequence, 3);
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
                    .saturating_sub(OPTIMISTIC_PRICE_MAX_AGE.as_secs() + 1),
            }),
            finality: "optimistic".to_owned(),
            kind: "uniswap.price.observation.apply".to_owned(),
            data: Some(AttachedPoolPrice {
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
            }),
            extra: BTreeMap::new(),
        };

        assert!(
            !render_attached(
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
    fn peer_cache_seed_is_atomic_and_never_replaces_local_state() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let source = directory.path().join("configured-peers.json");
        let destination = directory.path().join("embedded/execution-peers.json");
        fs::write(&source, b"configured").expect("write configured cache");

        assert!(seed_peer_cache(&source, &destination).expect("seed cache"));
        assert_eq!(
            fs::read(&destination).expect("read seeded cache"),
            b"configured"
        );

        fs::write(&source, b"new configured state").expect("update configured cache");
        assert!(!seed_peer_cache(&source, &destination).expect("preserve cache"));
        assert_eq!(
            fs::read(&destination).expect("read local cache"),
            b"configured"
        );
    }

    #[test]
    fn parses_sse_across_standard_line_endings() {
        let unix = b"event: apply\ndata: {\"sequence\":\"1\"}\n\nrest";
        assert_eq!(sse_event_end(unix), Some(37));
        assert_eq!(
            sse_data(&unix[..35]).expect("data").as_deref(),
            Some("{\"sequence\":\"1\"}")
        );
        let windows = b"data: {}\r\n\r\n";
        assert_eq!(sse_event_end(windows), Some(windows.len()));
    }

    #[test]
    fn embedded_processor_contains_every_requested_pool() {
        let markets =
            resolve_markets(&["ETH/USDC".to_owned(), "ETH/USDT".to_owned()]).expect("markets");
        let processor =
            subscription_processor(&markets, SubscribeFinality::Optimistic).expect("processor");
        let pools = processor.settings["pools"].as_array().expect("pools");
        assert_eq!(pools.len(), 2);
        assert_eq!(processor.history_mode, ProcessorHistoryMode::OnDemand);
        assert!(matches!(
            processor.publish,
            PublishMode::OptimisticAndFinalized
        ));
    }
}
