//! Ad-hoc Wasm processor runs over the node's retained recent blocks and its
//! live chain.
//!
//! A run is never persisted and never touches the node's processors. It
//! reduces into a throwaway store, which gives it the real apply, undo, and
//! change semantics, streams every block's changes as server-sent events,
//! and ends with its client, its time limit, or the node. A running run takes
//! another module or other parameters: it reruns the blocks it holds with
//! them and follows on with them.

use std::{
    collections::{HashMap, VecDeque},
    convert::Infallible,
    sync::{
        Arc, Mutex, MutexGuard, PoisonError,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use axum::{
    Extension, Json,
    extract::{ConnectInfo, Path, State},
    http::{HeaderMap, StatusCode},
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
};
use futures::{StreamExt, stream};
use leani_primitives::{BlockFrame, BlockHash, BlockNumber, BlockRange, BlockRef, ProcessorCursor};
use leani_processor_api::{
    ChangeOperation, LifecyclePolicies, Processor, ProcessorError, ProcessorInstanceId,
    PublicationPolicy,
};
use leani_processor_wasm::{TEMPLATES, WasmContract, WasmLimits, WasmModule, WasmProcessor};
use leani_source_api::{ChainEvent, LiveStart};
use leani_store_sqlite::{ChangeDirection, Durability, SqliteStore, StoreConfig};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, broadcast, mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::{ApiError, ApiState, LiveFeed};

/// Concurrent runs per node, and concurrent swaps into them: each one
/// interprets guest code on the node's CPUs.
const MAX_RUNS: usize = 2;
const DEFAULT_BLOCKS: u64 = 16;
const MAX_BLOCKS: u64 = 1_024;
/// Recent blocks a run's own feed fetches again: the execution P2P source
/// replays at most 64 blocks before the block it starts from.
const MAX_FEED_BLOCKS: u64 = 64;
const DEFAULT_FOLLOW_SECONDS: u64 = 600;
const MAX_FOLLOW_SECONDS: u64 = 3_600;
const MAX_MODULE_BYTES: u64 = 4 << 20;
/// A hex-encoded maximum module plus parameters.
pub(crate) const MAX_REQUEST_BYTES: usize = 9 << 20;
const READ_BYTES: u64 = 64 << 20;
/// Changes per store read; a block's changes may fill several.
const CHANGE_PAGE: usize = 1_000;
/// A run's throwaway store, which keeps every block it applies.
const RUN_STORE_BYTES: u64 = 1 << 30;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct RunRequest {
    /// The module, as hex; or name a `template` instead.
    #[serde(default)]
    module: Option<String>,
    #[serde(default)]
    template: Option<String>,
    #[serde(default)]
    params: Option<Value>,
    /// Recent blocks to run over before following: 1 to 1024, 16 by default.
    #[serde(default)]
    blocks: Option<u64>,
    #[serde(default)]
    follow: bool,
    /// How long to follow: 1 to 3600 seconds, 600 by default.
    #[serde(default)]
    max_seconds: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct SwapRequest {
    /// The module, as hex; or name a `template` instead.
    #[serde(default)]
    module: Option<String>,
    #[serde(default)]
    template: Option<String>,
    #[serde(default)]
    params: Option<Value>,
}

/// A processor a run has yet to take, with the version it makes the run.
type Pending = Option<(u64, Arc<WasmProcessor>)>;

struct Swaps {
    version: u64,
    sender: watch::Sender<Pending>,
}

#[derive(Clone)]
pub(crate) struct Runs {
    slots: Arc<Semaphore>,
    compiles: Arc<Semaphore>,
    swaps: Arc<Mutex<HashMap<String, Swaps>>>,
    next: Arc<AtomicU64>,
}

impl Default for Runs {
    fn default() -> Self {
        Self {
            slots: Arc::new(Semaphore::new(MAX_RUNS)),
            compiles: Arc::new(Semaphore::new(MAX_RUNS)),
            swaps: Arc::default(),
            next: Arc::default(),
        }
    }
}

impl Runs {
    fn swaps(&self) -> MutexGuard<'_, HashMap<String, Swaps>> {
        self.swaps.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A run's entry among the swaps, which goes when the run ends.
struct Registered {
    runs: Runs,
    id: String,
}

impl Drop for Registered {
    fn drop(&mut self) {
        self.runs.swaps().remove(&self.id);
    }
}

fn capacity(code: &'static str) -> ApiError {
    ApiError::new(
        StatusCode::SERVICE_UNAVAILABLE,
        code,
        &format!("{MAX_RUNS} Wasm runs, or swaps into them, are active; retry when one ends"),
        true,
    )
}

pub(crate) async fn templates() -> Result<Json<Value>, ApiError> {
    let data = TEMPLATES
        .iter()
        .map(|template| {
            Ok(json!({
                "name": template.name,
                "description": template.description,
                "params": example_params(template)?,
                "moduleHash": format!("0x{}", blake3::hash(template.module).to_hex()),
                "moduleBytes": template.module.len(),
                "sources": { "rust": template.source, "typescript": template.typescript },
            }))
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
    Ok(Json(json!({ "data": data })))
}

fn example_params(template: &leani_processor_wasm::Template) -> Result<Value, ApiError> {
    serde_json::from_str(template.params).map_err(|error| ApiError::internal(&error.to_string()))
}

pub(crate) async fn compose(
    Json(spec): Json<leani_processor_wasm::scaffold::Spec>,
) -> Result<Json<Value>, ApiError> {
    let generated = leani_processor_wasm::scaffold::generate(&spec)
        .map_err(|message| ApiError::invalid(&message))?;
    Ok(Json(json!({
        "name": generated.name,
        "language": generated.language,
        "path": generated.language.source_path(),
        "source": generated.source(),
        "files": generated.files.iter().map(|(path, _)| path).collect::<Vec<_>>(),
        "fields": generated.available,
    })))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BuildRequest {
    source: String,
    #[serde(default)]
    language: leani_processor_wasm::scaffold::Language,
}

/// A source that does not compile is a result, with its diagnostics, not an
/// error.
///
/// Rust builds only for requests from the node's own machine: rustc reads the
/// host's files when the source asks it to, and the source guard is a
/// denylist, not an isolation boundary. TypeScript, whose compiler is served
/// nothing but the processor and the SDK, builds for any client the API
/// admits.
pub(crate) async fn build_module(
    State(state): State<ApiState>,
    peer: Option<Extension<ConnectInfo<std::net::SocketAddr>>>,
    headers: HeaderMap,
    Json(request): Json<BuildRequest>,
) -> Result<Json<Value>, ApiError> {
    let build = state.config.wasm_build.clone().ok_or_else(|| {
        ApiError::conflict(
            "wasm_builds_disabled",
            "this node does not build modules: build the processor where you write it (`leani processor create`) and load its .wasm, or, for development, set `api.allow_wasm_builds = true` in the node's configuration",
        )
    })?;
    let peer = peer.map(|Extension(ConnectInfo(address))| address.ip());
    if request.language == leani_processor_wasm::scaffold::Language::Rust
        && !crate::browser_guard::from_this_machine(peer, &headers)
    {
        return Err(ApiError::forbidden(
            "this node builds Rust only for requests from its own machine, as rustc can read the host's files while it compiles: build TypeScript here, reach the node through an SSH tunnel, or build the crate where you are (`leani processor create`) and run its module",
        ));
    }
    build(request.source, request.language)
        .await
        .map(Json)
        .map_err(|message| ApiError::invalid(&message))
}

pub(crate) async fn start_run(
    State(state): State<ApiState>,
    Json(request): Json<RunRequest>,
) -> Result<Response, ApiError> {
    if request.follow && state.config.live_events.is_none() {
        return Err(ApiError::invalid("this node has no live lane to follow"));
    }
    let plan = Plan {
        blocks: within(
            request.blocks.unwrap_or(DEFAULT_BLOCKS),
            MAX_BLOCKS,
            "blocks",
        )?,
        follow: request
            .follow
            .then(|| {
                within(
                    request.max_seconds.unwrap_or(DEFAULT_FOLLOW_SECONDS),
                    MAX_FOLLOW_SECONDS,
                    "maxSeconds",
                )
            })
            .transpose()?
            .map(Duration::from_secs),
    };
    let (bytes, params) = module_and_params(
        request.module.as_deref(),
        request.template.as_deref(),
        request.params.as_ref(),
    )?;
    // The slot first: compiling runs the module's code.
    let permit = Arc::clone(&state.wasm_runs.slots)
        .try_acquire_owned()
        .map_err(|_| capacity("wasm_run_capacity"))?;
    let processor = compile(bytes, params).await?;
    // Unguessable without the store's secret, so that only the client that
    // started a run steers it.
    let counter = state.wasm_runs.next.fetch_add(1, Ordering::Relaxed);
    let id = hex::encode(
        &state
            .store
            .node_mac("leani wasm run", &counter.to_be_bytes())
            .as_bytes()[..16],
    );
    let (sender, receiver) = watch::channel(None);
    state
        .wasm_runs
        .swaps()
        .insert(id.clone(), Swaps { version: 1, sender });
    let registered = Registered {
        runs: state.wasm_runs.clone(),
        id,
    };
    let keep_alive = KeepAlive::new()
        .interval(state.config.heartbeat_interval)
        .text("heartbeat");
    let (events, stream) = mpsc::channel(64);
    tokio::spawn(run(
        state,
        Arc::new(processor),
        plan,
        registered,
        receiver,
        events,
        permit,
    ));
    let stream = stream::unfold(stream, |mut stream| async move {
        stream
            .recv()
            .await
            .map(|event| (Ok::<_, Infallible>(event), stream))
    });
    Ok(Sse::new(stream).keep_alive(keep_alive).into_response())
}

fn within(value: u64, maximum: u64, name: &str) -> Result<u64, ApiError> {
    if (1..=maximum).contains(&value) {
        Ok(value)
    } else {
        Err(ApiError::invalid(&format!(
            "{name} must be in 1..={maximum}"
        )))
    }
}

/// The run takes the newest processor it was given, announced by a `reset`
/// event.
pub(crate) async fn swap_processor(
    State(state): State<ApiState>,
    Path(run): Path<String>,
    Json(request): Json<SwapRequest>,
) -> Result<Response, ApiError> {
    // Its own code: a client starts a new run for this one, not for a
    // missing template.
    let unknown = || {
        ApiError::new(
            StatusCode::NOT_FOUND,
            "run_not_found",
            &format!("no running run `{run}`"),
            false,
        )
    };
    let (bytes, params) = module_and_params(
        request.module.as_deref(),
        request.template.as_deref(),
        request.params.as_ref(),
    )?;
    if !state.wasm_runs.swaps().contains_key(&run) {
        return Err(unknown());
    }
    let compiling = state
        .wasm_runs
        .compiles
        .try_acquire()
        .map_err(|_| capacity("wasm_swap_capacity"))?;
    let processor = Arc::new(compile(bytes, params).await?);
    drop(compiling);
    let version = {
        let mut runs = state.wasm_runs.swaps();
        let swaps = runs.get_mut(&run).ok_or_else(unknown)?;
        swaps.version += 1;
        swaps.sender.send_replace(Some((swaps.version, processor)));
        swaps.version
    };
    Ok((StatusCode::ACCEPTED, Json(json!({ "version": version }))).into_response())
}

fn module_and_params(
    module: Option<&str>,
    template: Option<&str>,
    params: Option<&Value>,
) -> Result<(Vec<u8>, Value), ApiError> {
    let (bytes, example) = match (module, template) {
        (Some(module), None) => (
            hex::decode(module.trim_start_matches("0x"))
                .map_err(|_| ApiError::invalid("module must be the module's bytes as hex"))?,
            None,
        ),
        (None, Some(name)) => {
            let template = TEMPLATES
                .iter()
                .find(|template| template.name == name)
                .ok_or_else(|| {
                    let names = TEMPLATES.map(|template| template.name).join(", ");
                    ApiError::invalid(&format!("no template `{name}`; the templates are {names}"))
                })?;
            (template.module.to_vec(), Some(template))
        }
        _ => return Err(ApiError::invalid("send exactly one of module or template")),
    };
    let params = match (params, example) {
        (Some(params), _) => params.clone(),
        (None, Some(template)) => example_params(template)?,
        (None, None) => json!({}),
    };
    Ok((bytes, params))
}

async fn compile(bytes: Vec<u8>, params: Value) -> Result<WasmProcessor, ApiError> {
    let limits = WasmLimits {
        module_bytes: MAX_MODULE_BYTES,
        // Below a configured instance's default: a run is a trial, and it
        // shares the node's CPUs.
        fuel: 250_000_000,
        ..WasmLimits::default()
    };
    let instance = ProcessorInstanceId::new("playground")
        .map_err(|error| ApiError::internal(&error.to_string()))?;
    // Validation and `leani_describe` run guest code: keep them off the
    // async workers, as `map` is.
    tokio::task::spawn_blocking(move || {
        WasmProcessor::new(
            WasmModule::new(&bytes, &limits)?,
            &params,
            WasmContract {
                instance,
                version: semver::Version::new(0, 0, 0),
                start_block: BlockNumber(0),
                publication: PublicationPolicy::Unattested,
                lifecycle: LifecyclePolicies::default(),
            },
            limits,
        )
    })
    .await
    .map_err(|error| ApiError::internal(&error.to_string()))?
    .map_err(|error| ApiError::invalid(&error.to_string()))
}

#[derive(Clone, Copy, Debug)]
struct Plan {
    blocks: u64,
    follow: Option<Duration>,
}

async fn run(
    state: ApiState,
    processor: Arc<WasmProcessor>,
    plan: Plan,
    registered: Registered,
    swaps: watch::Receiver<Pending>,
    events: mpsc::Sender<Event>,
    _permit: OwnedSemaphorePermit,
) {
    let started = tokio::time::Instant::now();
    let directory = match tempfile::tempdir() {
        Ok(directory) => directory,
        Err(error) => {
            let _ = events
                .send(ApiError::internal(&error.to_string()).sse_event())
                .await;
            return;
        }
    };
    let mut runner = Runner {
        store: None,
        state,
        processor: processor.clone(),
        events,
        run: registered.id.clone(),
        version: 0,
        swaps,
        kept: VecDeque::new(),
        keep: 0,
        deadline: None,
        sequence: 0,
        first: None,
        last: None,
        blocks: 0,
        changes: 0,
    };
    let outcome = Box::pin(runner.execute(&directory, plan, processor)).await;
    drop(registered);
    let summary = json!({
        "blocks": runner.blocks,
        "changes": runner.changes,
        "elapsedMs": u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
    });
    let ending = match outcome {
        Ok(()) => event("done", &summary),
        Err(Stop::Disconnected) => return,
        Err(Stop::Failed(message)) => {
            let mut error = ApiError::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "wasm_run_failed",
                &message,
                false,
            );
            error.details = Some(summary);
            error.sse_event()
        }
        Err(Stop::Replan(..)) => unreachable!("execute plans every swapped-in processor"),
    };
    let _ = runner.events.send(ending).await;
}

enum Stop {
    Disconnected,
    Failed(String),
    /// A swapped-in processor, with its version, that requires material the
    /// blocks the run holds lack: the run plans its feed again.
    Replan(u64, Arc<WasmProcessor>),
}

impl From<String> for Stop {
    fn from(message: String) -> Self {
        Self::Failed(message)
    }
}

struct Runner {
    store: Option<SqliteStore>,
    state: ApiState,
    processor: Arc<WasmProcessor>,
    events: mpsc::Sender<Event>,
    run: String,
    /// 1 for the processor the run started with, then one more per swap.
    version: u64,
    swaps: watch::Receiver<Pending>,
    /// The latest blocks of the run's own feed, at most `keep`, which a
    /// swapped-in processor reruns.
    kept: VecDeque<BlockFrame>,
    keep: usize,
    deadline: Option<tokio::time::Instant>,
    sequence: u64,
    first: Option<BlockNumber>,
    last: Option<BlockRef>,
    blocks: u64,
    changes: u64,
}

impl Runner {
    async fn execute(
        &mut self,
        directory: &tempfile::TempDir,
        plan: Plan,
        processor: Arc<WasmProcessor>,
    ) -> Result<(), Stop> {
        let (mut version, mut processor) = (1, processor);
        loop {
            self.reset(directory, version, processor).await?;
            match Box::pin(self.serve(directory, plan)).await {
                Err(Stop::Replan(next, swapped)) => (version, processor) = (next, swapped),
                outcome => return outcome,
            }
        }
    }

    async fn serve(&mut self, directory: &tempfile::TempDir, plan: Plan) -> Result<(), Stop> {
        // Subscribe before reading the retained blocks so no block falls
        // between them.
        let live = plan
            .follow
            .and(self.state.config.live_events.as_ref())
            .map(broadcast::Sender::subscribe);
        let (retained, newest) = self.newest_retained().await?;
        if !covers(&self.processor, &newest)
            && let Some(feed) = self.state.config.live_feed.clone()
        {
            return Box::pin(self.run_own_feed(directory, &feed, newest.block, plan)).await;
        }
        let from = retained
            .end()
            .0
            .saturating_sub(plan.blocks - 1)
            .max(retained.start().0);
        self.ready(from, retained.end().0, plan.follow.is_some(), "lane")
            .await?;
        self.catch_up(from, retained.end().0).await?;
        let Some(mut live) = live else {
            return Ok(());
        };
        let deadline = self.deadline(plan.follow.unwrap_or_default());
        let shutdown = self.state.config.shutdown.clone();
        loop {
            let received = tokio::select! {
                received = live.recv() => received,
                Ok(()) = self.swaps.changed() => {
                    self.swap_on_lane(directory, plan).await?;
                    continue;
                }
                () = tokio::time::sleep_until(deadline) => return Ok(()),
                () = shutdown.cancelled() => return Ok(()),
                () = self.events.closed() => return Err(Stop::Disconnected),
            };
            match received {
                Ok(ChainEvent::Block(frame)) => self.follow(*frame).await?,
                Ok(ChainEvent::Reorg { reverted, applied }) => {
                    self.revert(reverted).await?;
                    for frame in applied {
                        self.follow(frame).await?;
                    }
                }
                Ok(
                    event @ (ChainEvent::Attested { .. }
                    | ChainEvent::Disconnected { .. }
                    | ChainEvent::Reset { .. }),
                ) => self.notify(&event).await?,
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    // The retained chain still holds what the stream dropped.
                    let next = self.last.map_or(0, |last| last.number.0 + 1);
                    if let Some(bounds) = self
                        .state
                        .store
                        .recent_canonical_bounds(self.state.config.chain_id)
                        .await
                        .map_err(|error| error.to_string())?
                    {
                        self.catch_up(next.max(bounds.start().0), bounds.end().0)
                            .await?;
                    }
                }
                Err(broadcast::error::RecvError::Closed) => return Ok(()),
            }
        }
    }

    async fn swap_on_lane(
        &mut self,
        directory: &tempfile::TempDir,
        plan: Plan,
    ) -> Result<(), Stop> {
        let Some((version, processor)) = self.swaps.borrow_and_update().clone() else {
            return Ok(());
        };
        let (retained, newest) = self.newest_retained().await?;
        if !covers(&processor, &newest) {
            return Err(Stop::Replan(version, processor));
        }
        let to = self.last.map_or(retained.end().0, |last| last.number.0);
        let from = to.saturating_sub(plan.blocks - 1).max(retained.start().0);
        self.reset(directory, version, processor).await?;
        self.ready(from, to, true, "lane").await?;
        self.catch_up(from, to).await
    }

    async fn reset(
        &mut self,
        directory: &tempfile::TempDir,
        version: u64,
        processor: Arc<WasmProcessor>,
    ) -> Result<(), Stop> {
        self.store = None;
        std::fs::remove_dir_all(directory.path().join(self.version.to_string())).ok();
        let path = directory.path().join(version.to_string());
        std::fs::create_dir_all(&path).map_err(|error| error.to_string())?;
        // Throwaway: no fsync per block, and a bounded size.
        let mut config = StoreConfig::new(path.join("run.sqlite"));
        config.durability = Durability::Normal;
        config.storage_budget.maximum_physical_bytes = RUN_STORE_BYTES;
        self.store = Some(
            SqliteStore::open(config)
                .await
                .map_err(|error| error.to_string())?,
        );
        self.processor = processor;
        self.version = version;
        self.kept.clear();
        self.sequence = 0;
        self.first = None;
        self.last = None;
        self.blocks = 0;
        self.changes = 0;
        Ok(())
    }

    async fn newest_retained(&self) -> Result<(BlockRange, BlockFrame), Stop> {
        let chain = self.state.config.chain_id;
        let retained = self
            .state
            .store
            .recent_canonical_bounds(chain)
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| {
                "this node retains no recent blocks yet; start its live lane and wait for blocks"
                    .to_owned()
            })?;
        let newest = self
            .state
            .store
            .recent_frames(chain, BlockRange::single(retained.end()), READ_BYTES)
            .await
            .map_err(|error| error.to_string())?
            .pop()
            .ok_or_else(|| format!("the node no longer retains block {}", retained.end()))?;
        Ok((retained, newest))
    }

    /// When the run ends: `limit` after it first waits for blocks, however
    /// often it swaps.
    fn deadline(&mut self, limit: Duration) -> tokio::time::Instant {
        *self
            .deadline
            .get_or_insert_with(|| tokio::time::Instant::now() + limit)
    }

    /// Forward the chain events that only inform the client.
    async fn notify(&self, event: &ChainEvent) -> Result<(), Stop> {
        match event {
            ChainEvent::Attested { through } => {
                self.send(
                    "attested",
                    &json!({ "throughBlock": through.number.0, "hash": through.hash.to_string() }),
                )
                .await
            }
            ChainEvent::Disconnected { reason } | ChainEvent::Reset { reason, .. } => {
                self.send("notice", &json!({ "message": reason })).await
            }
            ChainEvent::Block(_) | ChainEvent::Reorg { .. } => Ok(()),
        }
    }

    /// Announce the run's processor: `ready` for the first, `reset` for each
    /// swapped in, before the blocks it runs over.
    async fn ready(&self, from: u64, to: u64, follow: bool, feed: &str) -> Result<(), Stop> {
        self.send(
            if self.version == 1 { "ready" } else { "reset" },
            &json!({
                "run": self.run,
                "version": self.version,
                "manifest": self.processor.manifest(),
                "moduleHash": self.processor.descriptor().code_hash.to_string(),
                "window": { "fromBlock": from, "toBlock": to },
                "follow": follow,
                "feed": feed,
            }),
        )
        .await
    }

    /// Run on a feed of the module's own material, which the live lane
    /// lacks: up to [`MAX_FEED_BLOCKS`] blocks ending at the lane's newest,
    /// `anchor`, and then, while the run follows, every new block.
    async fn run_own_feed(
        &mut self,
        directory: &tempfile::TempDir,
        feed: &LiveFeed,
        anchor: BlockRef,
        plan: Plan,
    ) -> Result<(), Stop> {
        let blocks = plan.blocks.min(MAX_FEED_BLOCKS);
        self.keep = usize::try_from(blocks).unwrap_or(usize::MAX);
        let follow = plan.follow.is_some();
        self.ready(
            anchor.number.0.saturating_sub(blocks - 1),
            anchor.number.0,
            follow,
            "own",
        )
        .await?;
        self.send(
            "notice",
            &json!({ "message": format!(
                "This node's live lane does not fetch what this module needs, so the run fetches its own: the last {blocks} blocks{}.",
                if follow { ", then new ones" } else { "" }
            ) }),
        )
        .await?;
        let cancellation = CancellationToken::new();
        let _close = cancellation.clone().drop_guard();
        let processor: Arc<dyn Processor> = self.processor.clone();
        let mut feed = feed(
            processor,
            LiveStart::AnchoredOverlap {
                anchor,
                overlap_blocks: blocks,
            },
            cancellation,
        )
        .await
        .map_err(|error| format!("open a feed of this module's material: {error}"))?;
        // A run that does not follow still ends if its feed stalls.
        let deadline = self.deadline(
            plan.follow
                .unwrap_or(Duration::from_secs(MAX_FOLLOW_SECONDS)),
        );
        let shutdown = self.state.config.shutdown.clone();
        loop {
            if !follow && self.last.is_some_and(|last| last.number >= anchor.number) {
                return Ok(());
            }
            let received = tokio::select! {
                received = feed.next() => received,
                Ok(()) = self.swaps.changed() => {
                    self.swap_on_feed(directory, follow).await?;
                    continue;
                }
                () = tokio::time::sleep_until(deadline) => {
                    return if follow {
                        Ok(())
                    } else {
                        Err(format!(
                            "the run's feed did not deliver its blocks within {MAX_FOLLOW_SECONDS} seconds"
                        )
                        .into())
                    };
                }
                () = shutdown.cancelled() => return Ok(()),
                () = self.events.closed() => return Err(Stop::Disconnected),
            };
            match received {
                None => return Ok(()),
                Some(Err(error)) => return Err(format!("the run's feed failed: {error}").into()),
                Some(Ok(ChainEvent::Block(frame))) => self.apply_fed(*frame).await?,
                Some(Ok(ChainEvent::Reorg { reverted, applied })) => {
                    self.revert(reverted).await?;
                    for frame in applied {
                        self.apply_fed(frame).await?;
                    }
                }
                Some(Ok(
                    event @ (ChainEvent::Attested { .. }
                    | ChainEvent::Disconnected { .. }
                    | ChainEvent::Reset { .. }),
                )) => self.notify(&event).await?,
            }
        }
    }

    async fn swap_on_feed(
        &mut self,
        directory: &tempfile::TempDir,
        follow: bool,
    ) -> Result<(), Stop> {
        let Some((version, processor)) = self.swaps.borrow_and_update().clone() else {
            return Ok(());
        };
        let (Some(first), Some(newest)) = (self.kept.front(), self.kept.back()) else {
            return Err(Stop::Replan(version, processor));
        };
        if !covers(&processor, newest) {
            return Err(Stop::Replan(version, processor));
        }
        let window = (first.block.number.0, newest.block.number.0);
        let kept = std::mem::take(&mut self.kept);
        self.reset(directory, version, processor).await?;
        self.ready(window.0, window.1, follow, "own").await?;
        for frame in kept {
            self.apply_fed(frame).await?;
        }
        Ok(())
    }

    async fn apply_fed(&mut self, frame: BlockFrame) -> Result<(), Stop> {
        if let Some(last) = self.last {
            if frame.block.number <= last.number {
                return Ok(());
            }
            if frame.block.parent_hash != last.hash {
                return Err(format!(
                    "the feed's block {} does not extend block {}",
                    frame.block.number, last.number
                )
                .into());
            }
        }
        self.apply(&frame).await?;
        self.kept.push_back(frame);
        while self.kept.len() > self.keep {
            self.kept.pop_front();
        }
        Ok(())
    }

    async fn catch_up(&mut self, from: u64, to: u64) -> Result<(), Stop> {
        let mut next = from;
        while next <= to {
            let range = BlockRange::new(BlockNumber(next), BlockNumber(to))
                .map_err(|error| error.to_string())?;
            let frames = self
                .state
                .store
                .recent_frames(self.state.config.chain_id, range, READ_BYTES)
                .await
                .map_err(|error| error.to_string())?;
            if frames.is_empty() {
                self.send(
                    "notice",
                    &json!({ "message": format!("the node no longer retains block {next}; the run skips the rest of its window") }),
                )
                .await?;
                return Ok(());
            }
            for frame in frames {
                next = frame.block.number.0 + 1;
                self.apply(&frame).await?;
            }
        }
        Ok(())
    }

    /// Apply a live block, first filling any blocks the stream skipped.
    async fn follow(&mut self, frame: BlockFrame) -> Result<(), Stop> {
        if let Some(last) = self.last {
            if frame.block.number <= last.number {
                return Ok(());
            }
            if frame.block.number.0 > last.number.0 + 1 {
                self.catch_up(last.number.0 + 1, frame.block.number.0 - 1)
                    .await?;
            }
        }
        self.apply(&frame).await
    }

    async fn apply(&mut self, frame: &BlockFrame) -> Result<(), Stop> {
        let processor = self.processor.as_ref();
        let delta = processor
            .map(frame)
            .await
            .map_err(|error| explain(frame.block.number, &error))?;
        let cursor = ProcessorCursor {
            processor_id: processor.descriptor().id.to_string(),
            processor_version: processor.descriptor().version.to_string(),
            chain_id: frame.chain_id,
            block_number: frame.block.number,
            block_hash: frame.block.hash,
            finality: frame.finality,
            sequence: frame.block.number.0,
        };
        self.store()?
            .apply(processor, cursor, &delta, &[])
            .await
            .map_err(|error| format!("block {}: {error}", frame.block.number))?;
        self.first.get_or_insert(frame.block.number);
        self.last = Some(frame.block);
        self.blocks += 1;
        let changes = self.read_changes().await?;
        self.send(
            "block",
            &json!({
                "number": frame.block.number.0,
                "hash": frame.block.hash.to_string(),
                "timestamp": frame.block.timestamp,
                "finality": frame.finality.name(),
                "changes": changes,
            }),
        )
        .await
    }

    /// Undo the reverted blocks this run applied, newest first.
    async fn revert(&mut self, mut reverted: Vec<BlockRef>) -> Result<(), Stop> {
        if let Some(lowest) = reverted.iter().map(|block| block.number).min() {
            self.kept.retain(|frame| frame.block.number < lowest);
        }
        reverted.sort_by_key(|block| std::cmp::Reverse(block.number));
        for block in reverted {
            // Only blocks this run applied have an undo record here.
            if self.last.is_none_or(|last| block.number > last.number)
                || self.first.is_none_or(|first| block.number < first)
            {
                continue;
            }
            let descriptor = self.processor.descriptor().clone();
            self.store()?
                .undo(
                    &descriptor,
                    self.state.config.chain_id,
                    block.number,
                    block.hash,
                    &[],
                )
                .await
                .map_err(|error| format!("undo block {}: {error}", block.number))?;
            self.last = Some(BlockRef {
                number: BlockNumber(block.number.0.saturating_sub(1)),
                hash: block.parent_hash,
                parent_hash: BlockHash::ZERO,
                timestamp: 0,
            });
            let changes = self.read_changes().await?;
            self.send(
                "undo",
                &json!({ "number": block.number.0, "hash": block.hash.to_string(), "changes": changes }),
            )
            .await?;
        }
        Ok(())
    }

    async fn read_changes(&mut self) -> Result<Vec<Value>, Stop> {
        let mut records = Vec::new();
        loop {
            let page = self
                .store()?
                .changes(
                    self.processor.descriptor(),
                    self.state.config.chain_id,
                    self.sequence,
                    CHANGE_PAGE,
                )
                .await
                .map_err(|error| error.to_string())?;
            let full = page.len() == CHANGE_PAGE;
            if let Some(last) = page.last() {
                self.sequence = last.cursor.sequence;
            }
            records.extend(page);
            if !full {
                break;
            }
        }
        let mut changes = Vec::with_capacity(records.len());
        for record in records {
            if !matches!(
                record.direction,
                ChangeDirection::Apply | ChangeDirection::Undo
            ) {
                continue;
            }
            self.changes += 1;
            changes.push(json!({
                "kind": record.change.kind,
                "key": format!("0x{}", hex::encode(&record.change.key)),
                "operation": match record.change.operation {
                    ChangeOperation::Upsert => "upsert",
                    ChangeOperation::Delete => "delete",
                },
                "undo": record.direction == ChangeDirection::Undo,
                "value": self
                    .processor
                    .change_json(&record.change)
                    .map_err(|error| error.to_string())?,
            }));
        }
        Ok(changes)
    }

    fn store(&self) -> Result<&SqliteStore, Stop> {
        self.store
            .as_ref()
            .ok_or_else(|| Stop::Failed("the run's store is not open".to_owned()))
    }

    async fn send(&self, name: &str, data: &Value) -> Result<(), Stop> {
        self.events
            .send(event(name, data))
            .await
            .map_err(|_| Stop::Disconnected)
    }
}

fn covers(processor: &WasmProcessor, frame: &BlockFrame) -> bool {
    processor
        .descriptor()
        .requirements
        .iter()
        .all(|requirement| requirement.validate_frame(frame).is_ok())
}

fn event(name: &str, data: &Value) -> Event {
    Event::default()
        .event(name)
        .json_data(data)
        .unwrap_or_else(|_| Event::default().event("error").data("{}"))
}

/// Name the usual reason a recent block cannot run: the live lane fetches
/// only what the node's configured processors need.
fn explain(block: BlockNumber, error: &ProcessorError) -> String {
    match error {
        ProcessorError::Input(detail) => format!(
            "block {block} lacks material this processor requires ({detail}); this node's live lane fetches only what its configured processors need"
        ),
        other => format!("block {block}: {other}"),
    }
}

#[cfg(test)]
mod tests;
