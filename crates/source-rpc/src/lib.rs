//! Blocks from JSON-RPC endpoints. Each block's contents are checked against
//! its own header hash, transactions root, and receipts root; which block is
//! canonical is what a quorum of independent endpoints agrees on, or what a
//! sequencer signed.

use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex, PoisonError, Weak},
    time::Duration,
};

use alloy_primitives::B256;
use futures::{StreamExt, TryStreamExt, stream};
use leani_primitives::{Capability, CapabilitySet};
use serde_json::Value;
use tracing::{debug, warn};
use url::Url;

/// Attempts per request while an endpoint rate-limits or fails.
const RETRIES: u32 = 5;
/// Calls in flight per endpoint when it takes no batches.
const CONCURRENT_CALLS: usize = 16;
/// Response bodies past this are refused: an endpoint must not exhaust memory.
const MAX_RESPONSE_BYTES: usize = 256 * 1_024 * 1_024;
/// Blocks per part of a range spread over the endpoints.
const PART_BLOCKS: u64 = 8;
/// Calls per second of an endpoint's pace for each part it keeps in flight:
/// at that pace a part's calls take longer than a round trip of up to 200 ms.
const CALLS_PER_SECOND_PER_PART: u32 = 40;
/// Parts an endpoint keeps in flight at most, however fast its pace.
const MAX_PARTS_IN_FLIGHT: u32 = 16;
/// How long a part is assumed to take an endpoint that has served none yet.
const UNKNOWN_PART_TIME: Duration = Duration::from_secs(1);
/// Headers proven per window walking down a chain.
const PROOF_WINDOW: u64 = 4_096;
/// The slowest pace a rate-limited endpoint backs off to.
const SLOWEST_CALL_INTERVAL: Duration = Duration::from_secs(2);
/// The longest `Retry-After` an endpoint's calls wait for.
const LONGEST_RETRY_AFTER: Duration = Duration::from_secs(30);

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The interval between calls that rate-limit headers allow: the tightest
/// `quota;w=seconds` policy they name, or else the limit over the longest
/// reset seen in `window`, as a reset is either seconds or a Unix time.
fn announced_interval(
    headers: &reqwest::header::HeaderMap,
    window: &mut Duration,
    now_unix: u64,
) -> Option<Duration> {
    let value = |name: &str| headers.get(name)?.to_str().ok();
    let limits = value("ratelimit-policy")
        .or_else(|| value("x-ratelimit-limit"))
        .or_else(|| value("ratelimit-limit"))?;
    let mut tightest = None;
    let mut limit = None;
    for policy in limits.split(',') {
        let mut fields = policy.split(';').map(str::trim);
        let mut quota = fields.next().and_then(|lead| lead.parse::<u32>().ok());
        let mut seconds = None;
        for field in fields {
            if let Some(value) = field.strip_prefix("q=") {
                quota = value.parse().ok();
            } else if let Some(value) = field.strip_prefix("w=") {
                seconds = value.parse::<u64>().ok();
            }
        }
        match (quota.filter(|quota| *quota > 0), seconds) {
            (Some(quota), Some(seconds)) => {
                tightest = tightest.max(Some(Duration::from_secs(seconds) / quota));
            }
            (Some(quota), None) => limit = limit.or(Some(quota)),
            _ => {}
        }
    }
    if tightest.is_some() {
        return tightest;
    }
    let limit = limit?;
    let reset = value("x-ratelimit-reset")
        .or_else(|| value("ratelimit-reset"))?
        .trim()
        .parse::<u64>()
        .ok()?;
    let reset = if reset > 1_000_000_000 {
        reset.saturating_sub(now_unix)
    } else {
        reset
    };
    *window = (*window).max(Duration::from_secs(reset));
    (!window.is_zero()).then(|| *window / limit)
}

/// How long a `Retry-After` header in seconds asks to wait.
fn retry_after(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    let seconds = headers.get(reqwest::header::RETRY_AFTER)?.to_str().ok()?;
    seconds.trim().parse().ok().map(Duration::from_secs)
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.as_secs())
}

/// What a block fetch reads, from what the request needs: public endpoints
/// limit calls, and full transactions are the heaviest.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FetchMode {
    /// The header, checked against its hash.
    Header,
    /// The header and receipts, checked against the receipts root.
    Receipts,
    /// Header, transactions, and receipts, checked against both roots.
    Full,
}

impl FetchMode {
    #[must_use]
    pub fn for_capabilities(required: CapabilitySet) -> Self {
        let required = required.with_derivable();
        if [
            Capability::Body,
            Capability::Transactions,
            Capability::Calldata,
            Capability::Senders,
        ]
        .into_iter()
        .any(|capability| required.contains(capability))
        {
            Self::Full
        } else if required.contains(Capability::Receipts) || required.contains(Capability::Logs) {
            Self::Receipts
        } else {
            Self::Header
        }
    }

    pub(crate) const fn byte(self) -> u8 {
        match self {
            Self::Header => 0,
            Self::Receipts => 1,
            Self::Full => 2,
        }
    }

    pub(crate) const fn from_byte(byte: u8) -> Self {
        match byte {
            0 => Self::Header,
            1 => Self::Receipts,
            _ => Self::Full,
        }
    }
}

pub mod chainlist;
mod decode;
pub mod feed;
mod finality;
mod history;
mod live;
mod websocket;

pub use decode::{BlockCheck, Check, DecodedBlock, decode_block, frame};
pub use finality::RpcFinalitySource;
pub use history::RpcHistorySource;
pub use live::RpcLiveSource;

#[derive(Debug, thiserror::Error)]
pub enum RpcError {
    #[error("rpc request {method} failed: {detail}")]
    Request {
        method: &'static str,
        detail: String,
    },
    #[error("malformed rpc response: {0}")]
    Malformed(String),
    #[error("served block failed its checks: {0}")]
    Unverified(String),
    #[error("endpoints disagree: {0}")]
    Disagreement(String),
}

impl From<RpcError> for leani_source_api::SourceError {
    fn from(error: RpcError) -> Self {
        match error {
            RpcError::Unverified(detail) | RpcError::Disagreement(detail) => {
                Self::CorruptFrame(detail)
            }
            error => Self::Unavailable(error.to_string()),
        }
    }
}

/// The interval between an endpoint's calls, which widens on a rate limit and
/// narrows back to the configured one or the one it announces, and when it
/// last widened.
#[derive(Debug)]
struct Pace {
    interval: Duration,
    slowed: tokio::time::Instant,
    /// The interval its rate-limit headers allow, once they say.
    announced: Option<Duration>,
    /// The longest reset its headers named: a fixed window's reset shrinks
    /// as the window runs out.
    window: Duration,
    /// No call goes out before then: a rate limit asked to retry after it.
    resume: tokio::time::Instant,
}

/// What every client of one endpoint in the process shares, so that the
/// pools of all lanes keep to the endpoint's one budget.
#[derive(Debug)]
struct Shared {
    /// When the next call may go out. Calls wait for it in turn, and each
    /// takes its slot at the interval of that moment: none is booked ahead,
    /// so calls queued during a rate limit speed up once it lifts.
    next: tokio::sync::Mutex<tokio::time::Instant>,
    pace: Mutex<Pace>,
    /// How long its parts usually take, once it served one.
    part_time: Mutex<Option<Duration>>,
    /// Cleared once the endpoint refuses a batch for any reason but a rate
    /// limit; it then gets one request per call.
    batches: std::sync::atomic::AtomicBool,
}

/// The endpoints the process's clients use.
static ENDPOINTS: std::sync::LazyLock<Mutex<HashMap<Url, Weak<Shared>>>> =
    std::sync::LazyLock::new(Mutex::default);

/// A part of a range in flight that no second endpoint fetches yet.
struct InFlight {
    index: usize,
    part: (u64, u64),
    /// The pool's index of the endpoint that fetches it.
    fetcher: usize,
    started: tokio::time::Instant,
}

impl InFlight {
    /// The part the pool's endpoint at `at` takes over, or else when it next
    /// may; `None` when there is none. It takes an unserved part another
    /// endpoint fetches once it would likely serve the part first, judged by
    /// how long each one's parts usually take, or once that endpoint has
    /// taken twice as long as usual.
    fn take_over(
        unhedged: &Mutex<Vec<Self>>,
        served: &[Option<Vec<(Value, Value)>>],
        clients: &[RpcClient],
        at: usize,
    ) -> Option<Result<Self, tokio::time::Instant>> {
        let now = tokio::time::Instant::now();
        let own = clients[at].usual();
        let mut unhedged = lock(unhedged);
        unhedged.retain(|in_flight| served[in_flight.index].is_none());
        let (position, due) = unhedged
            .iter()
            .enumerate()
            .filter(|(_, in_flight)| in_flight.fetcher != at)
            .map(|(position, in_flight)| {
                let usual = clients[in_flight.fetcher].usual();
                if now + own < in_flight.started + usual {
                    (position, now)
                } else {
                    (position, in_flight.started + usual * 2)
                }
            })
            .min_by_key(|(_, due)| *due)?;
        if due > now {
            return Some(Err(due));
        }
        Some(Ok(unhedged.swap_remove(position)))
    }
}

/// Where a pool's endpoint comes from. A pool puts them in this order: its
/// first endpoints take work first and failed parts go to them first.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Tier {
    /// The node operator's own.
    Own,
    /// Vetted public endpoints the chain entry names.
    Bundled,
    /// Public endpoints a list such as chainlist.org names.
    Listed,
}

/// Calls per second each public endpoint gets: they are shared and offered
/// for light use.
pub const PUBLIC_CALLS_PER_SECOND: u32 = 40;
/// Public endpoints a pool keeps at most, so that few operators see its
/// traffic.
const MAX_PUBLIC_ENDPOINTS: usize = 8;
/// How long a public endpoint may take to qualify.
const PUBLIC_QUALIFY_WITHIN: Duration = Duration::from_secs(5);

/// A JSON-RPC endpoint. HTTP/1.1 only: some public Nitro endpoints stall
/// HTTP/2 requests.
#[derive(Clone, Debug)]
pub struct RpcClient {
    endpoint: Url,
    tier: Tier,
    client: reqwest::Client,
    shared: Arc<Shared>,
    /// The interval `calls_per_second` allows.
    fastest: Duration,
    /// Parts of a range it keeps in flight, so that its pace and not round
    /// trips bound it.
    parts: u32,
}

impl RpcClient {
    /// # Errors
    ///
    /// Returns an error when the HTTP client cannot be built.
    pub fn new(endpoint: Url, calls_per_second: u32) -> Result<Self, RpcError> {
        let client = reqwest::Client::builder()
            .http1_only()
            .user_agent(concat!("leani/", env!("CARGO_PKG_VERSION")))
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|error| RpcError::Request {
                method: "client",
                detail: error.to_string(),
            })?;
        let fastest = Duration::from_secs(1) / calls_per_second.max(1);
        let shared = {
            let mut endpoints = lock(&ENDPOINTS);
            endpoints.retain(|_, shared| shared.strong_count() > 0);
            if let Some(shared) = endpoints.get(&endpoint).and_then(Weak::upgrade) {
                shared
            } else {
                let now = tokio::time::Instant::now();
                let shared = Arc::new(Shared {
                    next: tokio::sync::Mutex::new(now),
                    pace: Mutex::new(Pace {
                        interval: fastest,
                        slowed: now,
                        announced: None,
                        window: Duration::ZERO,
                        resume: now,
                    }),
                    part_time: Mutex::default(),
                    batches: std::sync::atomic::AtomicBool::new(true),
                });
                endpoints.insert(endpoint.clone(), Arc::downgrade(&shared));
                shared
            }
        };
        Ok(Self {
            endpoint,
            tier: Tier::Own,
            client,
            shared,
            fastest,
            parts: calls_per_second
                .div_ceil(CALLS_PER_SECOND_PER_PART)
                .clamp(1, MAX_PARTS_IN_FLIGHT),
        })
    }

    /// Wait for the budget of `calls` calls.
    async fn pace(&self, calls: u32) {
        let queued = tokio::time::Instant::now();
        let mut next = self.shared.next.lock().await;
        // A rate limit can push the resume past the slot while it waits.
        loop {
            let until = (*next).max(lock(&self.shared.pace).resume);
            if until <= tokio::time::Instant::now() {
                break;
            }
            tokio::time::sleep_until(until).await;
        }
        *next = tokio::time::Instant::now() + lock(&self.shared.pace).interval * calls;
        let waited = queued.elapsed();
        if waited > Duration::from_secs(30) {
            debug!(
                endpoint = self.host(),
                ?waited,
                calls,
                "calls waited behind others"
            );
        }
    }

    /// Halve the pace after a rate limit on a request `sent` then: a shared
    /// public endpoint stays usable when its clients slow down. Requests
    /// sent before the last slowdown met the same limit and count once.
    fn throttled(&self, sent: tokio::time::Instant) {
        let mut pace = lock(&self.shared.pace);
        if sent < pace.slowed {
            return;
        }
        pace.interval = (pace.interval * 2)
            .min(SLOWEST_CALL_INTERVAL)
            .max(self.floor(&pace));
        pace.slowed = tokio::time::Instant::now();
        debug!(endpoint = self.host(), interval = ?pace.interval, "rate-limited; slowing down");
    }

    /// Creep back towards the configured or announced pace for each of
    /// `calls` served.
    fn served(&self, calls: u32) {
        let mut pace = lock(&self.shared.pace);
        let floor = self.floor(&pace);
        for _ in 0..calls {
            pace.interval = pace.interval.saturating_sub(pace.interval / 16).max(floor);
        }
    }

    /// The shortest interval it gets: the configured one, or the one it
    /// announces when that is longer.
    fn floor(&self, pace: &Pace) -> Duration {
        pace.announced
            .map_or(self.fastest, |announced| announced.max(self.fastest))
    }

    /// Keep to the limits a response's headers announce, and, after a rate
    /// limit, to how long it asks to wait. An announcement counts at most
    /// [`SLOWEST_CALL_INTERVAL`] apart and a wait at most
    /// [`LONGEST_RETRY_AFTER`], so no endpoint holds calls up for long.
    fn heard(&self, headers: &reqwest::header::HeaderMap, limited: bool) {
        let mut pace = lock(&self.shared.pace);
        let mut window = pace.window;
        if let Some(announced) = announced_interval(headers, &mut window, unix_now()) {
            let announced = announced.min(SLOWEST_CALL_INTERVAL);
            pace.announced = Some(announced);
            pace.interval = pace.interval.max(announced);
        }
        pace.window = window;
        if limited && let Some(wait) = retry_after(headers) {
            let resume = tokio::time::Instant::now() + wait.min(LONGEST_RETRY_AFTER);
            pace.resume = pace.resume.max(resume);
        }
    }

    /// How long its parts usually take.
    fn usual(&self) -> Duration {
        lock(&self.shared.part_time).unwrap_or(UNKNOWN_PART_TIME)
    }

    /// Count a part it served in `took` into its usual time.
    fn took(&self, took: Duration) {
        let mut usual = lock(&self.shared.part_time);
        *usual = Some(usual.map_or(took, |usual| (usual * 7 + took) / 8));
    }

    #[must_use]
    pub const fn endpoint(&self) -> &Url {
        &self.endpoint
    }

    /// The endpoint's host, for logs: a configured URL may carry an API key.
    #[must_use]
    pub fn host(&self) -> &str {
        self.endpoint.host_str().unwrap_or("endpoint")
    }

    /// Check that the endpoint serves `chain_id` and, when `receipts`, whole
    /// blocks' receipts, which the fetches rely on.
    ///
    /// # Errors
    ///
    /// Returns an error naming what the endpoint lacks.
    pub async fn qualify(&self, chain_id: u64, receipts: bool) -> Result<(), RpcError> {
        let served = self.call("eth_chainId", serde_json::json!([])).await?;
        let served = served
            .as_str()
            .and_then(|served| u64::from_str_radix(served.trim_start_matches("0x"), 16).ok())
            .ok_or_else(|| RpcError::Malformed("chain id".to_owned()))?;
        if served != chain_id {
            return Err(RpcError::Request {
                method: "eth_chainId",
                detail: format!("serves chain {served}, not {chain_id}"),
            });
        }
        if receipts {
            self.call("eth_getBlockReceipts", serde_json::json!(["latest"]))
                .await?;
        }
        Ok(())
    }

    /// POST `body`, backing off while the endpoint rate-limits or fails:
    /// public endpoints answer bursts with 429 for a few seconds.
    async fn post<T: serde::de::DeserializeOwned>(
        &self,
        method: &'static str,
        body: &Value,
    ) -> Result<T, RpcError> {
        // An endpoint's URL may carry an API key: errors leave it out.
        let failed = |error: reqwest::Error| RpcError::Request {
            method,
            detail: error.without_url().to_string(),
        };
        let calls = body
            .as_array()
            .map_or(1, |calls| u32::try_from(calls.len()).unwrap_or(u32::MAX));
        let mut backoff = Duration::from_millis(500);
        for attempt in 0..RETRIES {
            self.pace(calls).await;
            let sent = tokio::time::Instant::now();
            let response = self
                .client
                .post(self.endpoint.clone())
                .json(body)
                .send()
                .await
                .map_err(failed)?;
            let status = response.status();
            let limited = status == reqwest::StatusCode::TOO_MANY_REQUESTS;
            self.heard(response.headers(), limited);
            if limited {
                self.throttled(sent);
            } else if status.is_success() {
                self.served(calls);
            }
            if (limited || status.is_server_error()) && attempt + 1 < RETRIES {
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(8));
                continue;
            }
            let response = response.error_for_status().map_err(failed)?;
            return read_json(response, MAX_RESPONSE_BYTES)
                .await
                .map_err(|detail| RpcError::Request { method, detail });
        }
        Err(RpcError::Request {
            method,
            detail: "retries exhausted".to_owned(),
        })
    }

    async fn call(&self, method: &'static str, params: Value) -> Result<Value, RpcError> {
        let failed = |detail: String| RpcError::Request { method, detail };
        let response: Value = self
            .post(
                method,
                &serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}),
            )
            .await?;
        match response.get("result") {
            Some(result) if !result.is_null() => Ok(result.clone()),
            _ => Err(failed(response["error"].to_string())),
        }
    }

    /// # Errors
    ///
    /// Returns an error when the endpoint does not answer.
    pub async fn head(&self) -> Result<u64, RpcError> {
        let head = self.call("eth_blockNumber", serde_json::json!([])).await?;
        u64::from_str_radix(
            head.as_str().unwrap_or_default().trim_start_matches("0x"),
            16,
        )
        .map_err(|error| RpcError::Malformed(error.to_string()))
    }

    /// The number and hash of the block at `tag`: a number or `latest`,
    /// `safe`, `finalized`.
    ///
    /// # Errors
    ///
    /// Returns an error when the endpoint does not serve the block.
    pub async fn block_id(&self, tag: &str) -> Result<(u64, B256), RpcError> {
        let block = self
            .call("eth_getBlockByNumber", serde_json::json!([tag, false]))
            .await?;
        let number = block["number"]
            .as_str()
            .and_then(|number| u64::from_str_radix(number.trim_start_matches("0x"), 16).ok())
            .ok_or_else(|| RpcError::Malformed("block number".to_owned()))?;
        let hash = block["hash"]
            .as_str()
            .and_then(|hash| hash.parse().ok())
            .ok_or_else(|| RpcError::Malformed("block hash".to_owned()))?;
        Ok((number, hash))
    }

    /// Blocks and, unless `mode` is `Header`, their receipts, in one batched
    /// request; one request per call where the endpoint refuses batches.
    /// Receipts are `Value::Null` in `Header` mode.
    ///
    /// # Errors
    ///
    /// Returns an error when the endpoint serves any of them incompletely.
    pub async fn blocks(
        &self,
        first: u64,
        last: u64,
        mode: FetchMode,
    ) -> Result<Vec<(Value, Value)>, RpcError> {
        let numbers = (first..=last).collect::<Vec<_>>();
        let full = mode == FetchMode::Full;
        let calls = numbers
            .iter()
            .enumerate()
            .flat_map(|(index, number)| {
                let tag = format!("{number:#x}");
                let block = serde_json::json!({"jsonrpc": "2.0", "id": 2 * index, "method": "eth_getBlockByNumber", "params": [tag, full]});
                let receipts = (mode != FetchMode::Header).then(|| serde_json::json!({"jsonrpc": "2.0", "id": 2 * index + 1, "method": "eth_getBlockReceipts", "params": [tag]}));
                std::iter::once(block).chain(receipts)
            })
            .collect::<Vec<_>>();
        let batched = if self
            .shared
            .batches
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            self.batch(&calls).await
        } else {
            None
        };
        if let Some(responses) = batched.filter(|responses| responses.len() == calls.len()) {
            let mut results = vec![Value::Null; 2 * numbers.len()];
            for response in responses {
                let id = response["id"]
                    .as_u64()
                    .and_then(|id| usize::try_from(id).ok())
                    .filter(|id| *id < results.len())
                    .ok_or_else(|| RpcError::Malformed("batch response id".to_owned()))?;
                if response["result"].is_null() {
                    return Err(RpcError::Request {
                        method: "batch",
                        detail: response["error"].to_string(),
                    });
                }
                results[id] = response["result"].clone();
            }
            let mut results = results.into_iter();
            return Ok(std::iter::from_fn(|| Some((results.next()?, results.next()?))).collect());
        }
        // Pacing bounds the calls; concurrency only hides their latency.
        stream::iter(numbers)
            .map(|number| async move {
                let tag = format!("{number:#x}");
                let block = self
                    .call("eth_getBlockByNumber", serde_json::json!([tag, full]))
                    .await?;
                let receipts = if mode == FetchMode::Header {
                    Value::Null
                } else {
                    self.call("eth_getBlockReceipts", serde_json::json!([tag]))
                        .await?
                };
                Ok::<_, RpcError>((block, receipts))
            })
            .buffered(CONCURRENT_CALLS)
            .try_collect()
            .await
    }

    /// One batched request, or `None` when the endpoint will not take it.
    async fn batch(&self, calls: &[Value]) -> Option<Vec<Value>> {
        let count = u32::try_from(calls.len()).unwrap_or(u32::MAX);
        self.pace(count).await;
        let sent = tokio::time::Instant::now();
        let response = self
            .client
            .post(self.endpoint.clone())
            .json(calls)
            .send()
            .await
            .ok()?;
        let limited = response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS;
        self.heard(response.headers(), limited);
        if !response.status().is_success() {
            if limited {
                self.throttled(sent);
            } else {
                self.shared
                    .batches
                    .store(false, std::sync::atomic::Ordering::Relaxed);
            }
            return None;
        }
        self.served(count);
        let responses = read_json::<Vec<Value>>(response, MAX_RESPONSE_BYTES).await;
        if responses.is_err() {
            self.shared
                .batches
                .store(false, std::sync::atomic::Ordering::Relaxed);
        }
        responses.ok()
    }

    /// The block reference at `tag`.
    ///
    /// # Errors
    ///
    /// Returns an error when the endpoint does not serve the block.
    pub async fn block_ref(&self, tag: &str) -> Result<leani_primitives::BlockRef, RpcError> {
        let block = self
            .call("eth_getBlockByNumber", serde_json::json!([tag, false]))
            .await?;
        let field = |name: &str| {
            block[name]
                .as_str()
                .ok_or_else(|| RpcError::Malformed(format!("block {name}")))
        };
        let number = |name: &str| {
            field(name).and_then(|value| {
                u64::from_str_radix(value.trim_start_matches("0x"), 16)
                    .map_err(|_| RpcError::Malformed(format!("block {name}")))
            })
        };
        let hash = |name: &str| {
            field(name).and_then(|value| {
                value
                    .parse::<B256>()
                    .map(|hash| leani_primitives::BlockHash::new(hash.0))
                    .map_err(|_| RpcError::Malformed(format!("block {name}")))
            })
        };
        Ok(leani_primitives::BlockRef {
            number: leani_primitives::BlockNumber(number("number")?),
            hash: hash("hash")?,
            parent_hash: hash("parentHash")?,
            timestamp: number("timestamp")?,
        })
    }

    /// The block with full transactions and its receipts.
    ///
    /// # Errors
    ///
    /// Returns an error when the endpoint serves either incompletely.
    pub async fn block(&self, number: u64) -> Result<(Value, Value), RpcError> {
        let tag = format!("{number:#x}");
        let block = self
            .call("eth_getBlockByNumber", serde_json::json!([tag, true]))
            .await?;
        let receipts = self
            .call("eth_getBlockReceipts", serde_json::json!([tag]))
            .await?;
        Ok((block, receipts))
    }
}

/// Independent endpoints of one chain, and how many of them must agree on a
/// block hash before it counts as canonical. Blocks are spread over all of
/// them.
#[derive(Clone, Debug)]
pub struct RpcQuorum {
    clients: Vec<RpcClient>,
    quorum: usize,
}

impl RpcQuorum {
    /// # Errors
    ///
    /// Returns an error for a quorum outside `1..=endpoints`.
    pub fn new(
        endpoints: Vec<Url>,
        quorum: usize,
        calls_per_second: u32,
    ) -> Result<Self, RpcError> {
        Self::pooled(endpoints, calls_per_second, Vec::new(), Vec::new(), quorum)
    }

    /// The operator's `own` endpoints at `calls_per_second`, then the public
    /// `bundled` and `listed` ones at [`PUBLIC_CALLS_PER_SECOND`]. An
    /// endpoint named twice is kept once, where it comes first.
    ///
    /// # Errors
    ///
    /// Returns an error for a quorum outside `1..=endpoints`.
    pub fn pooled(
        own: Vec<Url>,
        calls_per_second: u32,
        bundled: Vec<Url>,
        listed: Vec<Url>,
        quorum: usize,
    ) -> Result<Self, RpcError> {
        let mut clients = Vec::<RpcClient>::new();
        for (tier, endpoints, calls_per_second) in [
            (Tier::Own, own, calls_per_second),
            (Tier::Bundled, bundled, PUBLIC_CALLS_PER_SECOND),
            (Tier::Listed, listed, PUBLIC_CALLS_PER_SECOND),
        ] {
            for endpoint in endpoints {
                if clients.iter().all(|client| client.endpoint != endpoint) {
                    let mut client = RpcClient::new(endpoint, calls_per_second)?;
                    client.tier = tier;
                    clients.push(client);
                }
            }
        }
        if quorum == 0 || quorum > clients.len() {
            return Err(RpcError::Malformed(format!(
                "quorum {quorum} must be within 1..={}",
                clients.len()
            )));
        }
        Ok(Self { clients, quorum })
    }

    /// Keep the endpoints that serve `chain_id` and, when `receipts`, whole
    /// blocks' receipts: public endpoint lists include ones that lack
    /// `eth_getBlockReceipts`, and ones that hang, so a public endpoint gets
    /// [`PUBLIC_QUALIFY_WITHIN`]. Of the public ones, the bundled come first
    /// and then the listed that answered fastest, at most
    /// [`MAX_PUBLIC_ENDPOINTS`] together.
    ///
    /// # Errors
    ///
    /// Returns an error when fewer endpoints than the quorum qualify.
    pub async fn qualified(mut self, chain_id: u64, receipts: bool) -> Result<Self, RpcError> {
        let checks = futures::future::join_all(self.clients.iter().map(|client| async move {
            let started = tokio::time::Instant::now();
            let check = client.qualify(chain_id, receipts);
            let checked = if client.tier == Tier::Own {
                check.await
            } else {
                tokio::time::timeout(PUBLIC_QUALIFY_WITHIN, check)
                    .await
                    .unwrap_or_else(|_| {
                        Err(RpcError::Request {
                            method: "qualify",
                            detail: format!("no answer within {PUBLIC_QUALIFY_WITHIN:?}"),
                        })
                    })
            };
            checked.map(|()| started.elapsed())
        }))
        .await;
        let mut kept = Vec::new();
        for (client, check) in self.clients.into_iter().zip(checks) {
            match check {
                Ok(elapsed) => kept.push((client, elapsed)),
                Err(error) => {
                    warn!(endpoint = client.host(), %error, "dropping a JSON-RPC endpoint");
                }
            }
        }
        kept.sort_by_key(|(client, elapsed)| {
            (
                client.tier,
                (client.tier == Tier::Listed).then_some(*elapsed),
            )
        });
        let mut public = 0;
        let kept = kept
            .into_iter()
            .map(|(client, _)| client)
            .filter(|client| {
                public += usize::from(client.tier != Tier::Own);
                client.tier == Tier::Own || public <= MAX_PUBLIC_ENDPOINTS
            })
            .collect::<Vec<_>>();
        if kept.len() < self.quorum {
            return Err(RpcError::Request {
                method: "qualify",
                detail: format!(
                    "{} endpoints qualify, the quorum needs {}",
                    kept.len(),
                    self.quorum
                ),
            });
        }
        self.clients = kept;
        Ok(self)
    }

    #[must_use]
    pub const fn quorum(&self) -> usize {
        self.quorum
    }

    #[must_use]
    pub fn endpoints(&self) -> Vec<&Url> {
        self.clients.iter().map(RpcClient::endpoint).collect()
    }

    /// Each endpoint's host and where it comes from, in pool order.
    #[must_use]
    pub fn hosts(&self) -> Vec<(Tier, &str)> {
        self.clients
            .iter()
            .map(|client| (client.tier, client.host()))
            .collect()
    }

    /// Full blocks, split into parts that the endpoints take as they finish
    /// the last, so every endpoint serves some and a fast one serves more:
    /// each takes one before any takes a second, and keeps one in flight per
    /// [`CALLS_PER_SECOND_PER_PART`] calls per second of its pace, which
    /// spaces a part's calls further apart than they take, so the parts in
    /// flight hide their latency. Once none is left, an idle endpoint also
    /// takes a part another still fetches when it would likely serve it
    /// first, or once the other has taken twice as long as usual, and the
    /// first answer counts, so a slow or throttled endpoint does not hold up
    /// the range. A part an endpoint fails goes to the others, and that
    /// endpoint takes no more parts of this range.
    ///
    /// # Errors
    ///
    /// Returns the last error when no endpoint serves a part.
    pub async fn blocks(
        &self,
        first: u64,
        last: u64,
        mode: FetchMode,
    ) -> Result<Vec<(Value, Value)>, RpcError> {
        let parts = std::iter::successors(Some(first), |start| {
            start.checked_add(PART_BLOCKS).filter(|next| *next <= last)
        })
        .map(|start| (start, start.saturating_add(PART_BLOCKS - 1).min(last)))
        .collect::<Vec<_>>();
        let queue = &Mutex::new(parts.iter().copied().enumerate().collect::<VecDeque<_>>());
        let unhedged = &Mutex::new(Vec::<InFlight>::new());
        let served = &Mutex::new(vec![None::<Vec<(Value, Value)>>; parts.len()]);
        let failed = &Mutex::new(vec![false; self.clients.len()]);
        let all_served = &tokio::sync::Notify::new();
        let failure = &Mutex::new(None);
        let rounds = self
            .clients
            .iter()
            .map(|client| client.parts)
            .max()
            .unwrap_or(0);
        let workers = (0..rounds)
            .flat_map(|round| {
                self.clients
                    .iter()
                    .enumerate()
                    .filter(move |(_, client)| client.parts > round)
            })
            .map(|(at, client)| async move {
                loop {
                    if lock(failed)[at] {
                        return;
                    }
                    let next = lock(queue).pop_front();
                    let (index, (start, end)) = if let Some((index, part)) = next {
                        lock(unhedged).push(InFlight {
                            index,
                            part,
                            fetcher: at,
                            started: tokio::time::Instant::now(),
                        });
                        (index, part)
                    } else {
                        let taken = InFlight::take_over(unhedged, &lock(served), &self.clients, at);
                        match taken {
                            None => return,
                            Some(Err(due)) => {
                                tokio::time::sleep_until(due).await;
                                continue;
                            }
                            Some(Ok(taken)) => (taken.index, taken.part),
                        }
                    };
                    let started = tokio::time::Instant::now();
                    let mut fetched = client.blocks(start, end, mode).await;
                    let failed_here = fetched.is_err();
                    match &fetched {
                        Ok(_) => client.took(started.elapsed()),
                        Err(error) => {
                            debug!(endpoint = client.host(), start, end, %error, "a part failed; trying the other endpoints");
                        }
                    }
                    for other in &self.clients {
                        if fetched.is_ok() {
                            break;
                        }
                        if !std::ptr::eq(other, client) {
                            fetched = other.blocks(start, end, mode).await;
                        }
                    }
                    match fetched {
                        Ok(blocks) => {
                            let mut served = lock(served);
                            if served[index].is_none() {
                                served[index] = Some(blocks);
                                if served.iter().all(Option::is_some) {
                                    all_served.notify_one();
                                }
                            }
                        }
                        Err(error) => *lock(failure) = Some(error),
                    }
                    if failed_here {
                        lock(failed)[at] = true;
                        return;
                    }
                }
            });
        // A slower fetch of a part already served is dropped.
        tokio::select! {
            _ = futures::future::join_all(workers) => {}
            () = all_served.notified() => {}
        }
        let served = std::mem::take(&mut *lock(served));
        if served.iter().any(Option::is_none) {
            return Err(lock(failure)
                .take()
                .unwrap_or_else(|| RpcError::Malformed("no endpoints".to_owned())));
        }
        Ok(served.into_iter().flatten().flatten().collect())
    }

    /// The hashes of block `from` and of each block up to `head` that ends a
    /// `grid`-aligned span, proven by parent links down from `head`'s trusted
    /// hash. Headers come over the pool a window at a time and are hashed
    /// locally, so no endpoint is trusted.
    ///
    /// # Errors
    ///
    /// Returns an error when a header does not hash to what it claims or
    /// does not link to the chain above it.
    pub async fn prove_down(
        &self,
        head: u64,
        head_hash: B256,
        from: u64,
        grid: u64,
    ) -> Result<std::collections::BTreeMap<u64, B256>, RpcError> {
        let mut proven = std::collections::BTreeMap::new();
        let mut expected = head_hash;
        let mut top = head;
        loop {
            let low = top.saturating_sub(PROOF_WINDOW - 1).max(from);
            let headers = self.blocks(low, top, FetchMode::Header).await?;
            for (block, _) in headers.iter().rev() {
                let decoded = decode_block(block, &Value::Null)?;
                let number = decoded.header.number;
                if decoded.check.header != Check::Verified || decoded.hash != expected {
                    return Err(RpcError::Unverified(format!(
                        "block {number} is not on the chain of block {head}"
                    )));
                }
                if number == from || (number + 1) % grid.max(1) == 0 {
                    proven.insert(number, decoded.hash);
                }
                expected = decoded.header.parent_hash;
            }
            if low == from {
                return Ok(proven);
            }
            top = low - 1;
        }
    }

    /// The hash at `number` when at least `quorum` endpoints serve the same
    /// one; `None` while too few endpoints serve the block yet.
    ///
    /// # Errors
    ///
    /// Returns an error when endpoints that serve the block disagree such
    /// that no quorum can form.
    pub async fn agreed_hash(&self, number: u64) -> Result<Option<B256>, RpcError> {
        let tag = format!("{number:#x}");
        let served =
            futures::future::join_all(self.clients.iter().map(|client| client.block_id(&tag)))
                .await;
        let hashes = served
            .into_iter()
            .filter_map(Result::ok)
            .map(|(_, hash)| hash)
            .collect::<Vec<_>>();
        if let Some(agreed) = hashes
            .iter()
            .find(|hash| hashes.iter().filter(|other| other == hash).count() >= self.quorum)
        {
            return Ok(Some(*agreed));
        }
        let distinct = hashes
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len();
        if hashes.len() >= self.quorum && distinct > 1 {
            return Err(RpcError::Disagreement(format!(
                "block {number}: {} endpoints serve {distinct} hashes",
                hashes.len()
            )));
        }
        Ok(None)
    }

    /// The quorum's block at `tag`, with its parent and timestamp.
    ///
    /// # Errors
    ///
    /// Returns an error when no quorum forms.
    pub async fn agreed_ref(&self, tag: &str) -> Result<leani_primitives::BlockRef, RpcError> {
        let (number, hash) = self.agreed_tag(tag).await?;
        let block = self.clients[0].block_ref(&format!("{number:#x}")).await?;
        if block.hash.0 != hash.0 {
            return Err(RpcError::Disagreement(format!(
                "block {number} changed while it was read"
            )));
        }
        Ok(block)
    }

    /// The block whose hash is `hash`, from the first endpoint that serves a
    /// header hashing to it, so no endpoint is trusted.
    ///
    /// # Errors
    ///
    /// Returns an error when no endpoint serves that header.
    pub async fn proven_ref(&self, hash: B256) -> Result<leani_primitives::BlockRef, RpcError> {
        let mut refused = Vec::new();
        for client in &self.clients {
            let served = client
                .call(
                    "eth_getBlockByHash",
                    serde_json::json!([format!("{hash:#x}"), false]),
                )
                .await
                .and_then(|block| decode_block(&block, &Value::Null));
            match served {
                Ok(decoded) if decoded.check.header == Check::Verified && decoded.hash == hash => {
                    return Ok(leani_primitives::BlockRef {
                        number: leani_primitives::BlockNumber(decoded.header.number),
                        hash: leani_primitives::BlockHash::new(hash.0),
                        parent_hash: leani_primitives::BlockHash::new(decoded.header.parent_hash.0),
                        timestamp: decoded.header.timestamp,
                    });
                }
                Ok(_) => refused.push(format!("{}: another header", client.host())),
                Err(error) => refused.push(format!("{}: {error}", client.host())),
            }
        }
        Err(RpcError::Unverified(format!(
            "no endpoint serves block {hash:#x}: {}",
            refused.join("; ")
        )))
    }

    /// The highest block at `tag` that at least `quorum` endpoints have
    /// reached and agree on.
    ///
    /// # Errors
    ///
    /// Returns an error when fewer than `quorum` endpoints answer or they
    /// disagree.
    pub async fn agreed_tag(&self, tag: &str) -> Result<(u64, B256), RpcError> {
        let served =
            futures::future::join_all(self.clients.iter().map(|client| client.block_id(tag))).await;
        let mut numbers = served
            .iter()
            .filter_map(|served| served.as_ref().ok().map(|(number, _)| *number))
            .collect::<Vec<_>>();
        numbers.sort_unstable_by(|left, right| right.cmp(left));
        let reported = served
            .iter()
            .filter_map(|served| served.as_ref().ok())
            .collect::<Vec<_>>();
        let Some(candidate) = numbers.get(self.quorum - 1).copied() else {
            let errors = served
                .into_iter()
                .filter_map(Result::err)
                .map(|error| error.to_string())
                .collect::<Vec<_>>();
            return Err(RpcError::Request {
                method: "eth_getBlockByNumber",
                detail: format!(
                    "{} of {} endpoints answered `{tag}`: {}",
                    numbers.len(),
                    self.quorum,
                    errors.join("; ")
                ),
            });
        };
        // When enough endpoints already reported the candidate itself, their
        // answers are the hash check.
        if let Some((_, hash)) = reported.iter().find(|(number, hash)| {
            *number == candidate
                && reported
                    .iter()
                    .filter(|other| other.0 == candidate && other.1 == *hash)
                    .count()
                    >= self.quorum
        }) {
            return Ok((candidate, *hash));
        }
        match self.agreed_hash(candidate).await? {
            Some(hash) => Ok((candidate, hash)),
            None => Err(RpcError::Disagreement(format!(
                "no {} endpoints agree on `{tag}` block {candidate}",
                self.quorum
            ))),
        }
    }
}

fn unix_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
        })
}

/// `response`'s JSON body, refused past `max` bytes.
async fn read_json<T: serde::de::DeserializeOwned>(
    mut response: reqwest::Response,
    max: usize,
) -> Result<T, String> {
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| error.without_url().to_string())?
    {
        if body.len() + chunk.len() > max {
            return Err(format!("response body over {max} bytes"));
        }
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body).map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    /// An endpoint that serves fixed `(number, hash)` blocks by number and as
    /// `latest`.
    async fn endpoint(blocks: Vec<(u64, B256)>) -> Url {
        delayed_endpoint(blocks, Duration::ZERO).await
    }

    /// An [`endpoint`] that answers each request after `delay`.
    async fn delayed_endpoint(blocks: Vec<(u64, B256)>, delay: Duration) -> Url {
        serving(
            blocks
                .into_iter()
                .map(|(number, hash)| {
                    let served = serde_json::json!({"number": format!("{number:#x}"), "hash": format!("{hash:#x}")});
                    (number, served)
                })
                .collect(),
            delay,
        )
        .await
    }

    /// An endpoint that answers any by-number or by-hash request with the
    /// block it holds, after `delay`.
    async fn serving(blocks: Vec<(u64, Value)>, delay: Duration) -> Url {
        counted(blocks, delay, Arc::default()).await
    }

    /// A [`serving`] endpoint that counts the requests it answers.
    async fn counted(
        blocks: Vec<(u64, Value)>,
        delay: Duration,
        requests: Arc<std::sync::atomic::AtomicUsize>,
    ) -> Url {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let latest = blocks.iter().map(|(number, _)| *number).max().unwrap();
        let by_hash = blocks
            .iter()
            .filter_map(|(_, block)| Some((block["hash"].as_str()?.to_owned(), block.clone())))
            .collect::<HashMap<_, _>>();
        let by_number = blocks.into_iter().collect::<HashMap<_, _>>();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let by_number = by_number.clone();
                let by_hash = by_hash.clone();
                let requests = requests.clone();
                tokio::spawn(async move {
                    let mut request = vec![0; 16_384];
                    let read = socket.read(&mut request).await.unwrap_or(0);
                    requests.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let request = String::from_utf8_lossy(&request[..read]);
                    let body: Value =
                        serde_json::from_str(request.split("\r\n\r\n").nth(1).unwrap_or("{}"))
                            .unwrap_or_default();
                    let tag = body["params"][0].as_str().unwrap_or_default();
                    let number = if tag == "latest" {
                        latest
                    } else {
                        u64::from_str_radix(tag.trim_start_matches("0x"), 16).unwrap_or(u64::MAX)
                    };
                    let result = if body["method"] == "eth_chainId" {
                        Value::from("0x1")
                    } else {
                        by_hash
                            .get(tag)
                            .or_else(|| by_number.get(&number))
                            .cloned()
                            .unwrap_or(Value::Null)
                    };
                    let response = serde_json::json!({"jsonrpc": "2.0", "id": 1, "result": result})
                        .to_string();
                    let reply = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{response}",
                        response.len()
                    );
                    tokio::time::sleep(delay).await;
                    let _ = socket.write_all(reply.as_bytes()).await;
                });
            }
        });
        format!("http://{address}/").parse().unwrap()
    }

    #[tokio::test]
    async fn response_bodies_past_the_limit_are_refused() {
        let url = endpoint(vec![(1, B256::ZERO)]).await;
        let read = async |max| {
            let response = reqwest::Client::new()
                .post(url.clone())
                .body("{}")
                .send()
                .await
                .unwrap();
            read_json::<Value>(response, max).await
        };
        assert!(read(1_024).await.is_ok());
        let error = read(16).await.unwrap_err();
        assert!(error.contains("over 16 bytes"), "{error}");
    }

    #[tokio::test]
    async fn the_quorum_head_is_the_highest_block_enough_endpoints_agree_on() {
        let (a, b) = (B256::repeat_byte(0xa), B256::repeat_byte(0xb));
        let ahead = endpoint(vec![(10, a), (11, b)]).await;
        let behind = endpoint(vec![(10, a)]).await;
        let quorum = RpcQuorum::new(vec![ahead.clone(), behind], 2, 1_000).unwrap();
        // One endpoint lags: the head both have is the quorum's.
        assert_eq!(quorum.agreed_tag("latest").await.unwrap(), (10, a));
        assert_eq!(quorum.agreed_hash(11).await.unwrap(), None);

        let forked = endpoint(vec![(10, b)]).await;
        let quorum = RpcQuorum::new(vec![ahead, forked], 2, 1_000).unwrap();
        assert!(matches!(
            quorum.agreed_hash(10).await,
            Err(RpcError::Disagreement(_))
        ));
        assert!(quorum.agreed_tag("latest").await.is_err());
    }

    #[tokio::test]
    async fn a_range_spreads_over_the_endpoints_and_survives_one_that_fails() {
        let blocks = (1..=40)
            .map(|number| (number, B256::with_last_byte(u8::try_from(number).unwrap())))
            .collect::<Vec<_>>();
        let serving = endpoint(blocks.clone()).await;
        let empty = endpoint(vec![(0, B256::ZERO)]).await;
        let pool = RpcQuorum::new(vec![empty, serving], 1, 1_000).unwrap();
        let served = pool.blocks(1, 40, FetchMode::Header).await.unwrap();
        let numbers = served
            .iter()
            .map(|(block, _)| {
                u64::from_str_radix(
                    block["number"].as_str().unwrap().trim_start_matches("0x"),
                    16,
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(numbers, (1..=40).collect::<Vec<_>>());
    }

    /// Empty blocks `1..=count` at `timestamp`, chained by parent hash, as an
    /// endpoint serves their headers, and their hashes.
    fn header_chain(count: u64, timestamp: u64) -> (Vec<(u64, Value)>, Vec<B256>) {
        let mut parent = B256::ZERO;
        let mut hashes = Vec::new();
        let served = (1..=count)
            .map(|number| {
                let header = alloy_consensus::Header {
                    number,
                    parent_hash: parent,
                    timestamp,
                    transactions_root: alloy_trie::EMPTY_ROOT_HASH,
                    ..Default::default()
                };
                parent = header.hash_slow();
                hashes.push(parent);
                let mut served =
                    serde_json::to_value(alloy_rpc_types_eth::Header::new(header)).unwrap();
                served["transactions"] = serde_json::json!([]);
                (number, served)
            })
            .collect();
        (served, hashes)
    }

    #[tokio::test]
    async fn a_proof_walks_down_from_the_trusted_block_and_keeps_its_grid() {
        let (served, hashes) = header_chain(600, 0);
        let pool = RpcQuorum::new(
            vec![serving(served.clone(), Duration::ZERO).await],
            1,
            1_000,
        )
        .unwrap();
        let proven = pool.prove_down(600, hashes[599], 100, 256).await.unwrap();
        assert_eq!(
            proven.into_iter().collect::<Vec<_>>(),
            [(100, hashes[99]), (255, hashes[254]), (511, hashes[510])]
        );
        // Another block 300 breaks the chain down from 600.
        let mut forged = served;
        forged[299].1["extraData"] = serde_json::json!("0x01");
        let pool = RpcQuorum::new(vec![serving(forged, Duration::ZERO).await], 1, 1_000).unwrap();
        assert!(matches!(
            pool.prove_down(600, hashes[599], 100, 256).await,
            Err(RpcError::Unverified(_))
        ));
    }

    /// Headers `range` from an anchored source over `endpoint`, chunk by
    /// chunk.
    async fn anchored_headers(
        endpoint: Url,
        anchor: (u64, B256),
        range: std::ops::RangeInclusive<u64>,
    ) -> Result<Vec<leani_primitives::BlockFrame>, leani_source_api::SourceError> {
        use leani_source_api::HistorySource as _;

        let pool = RpcQuorum::new(vec![endpoint], 1, 1_000).unwrap();
        let source =
            RpcHistorySource::anchored("rpc", leani_primitives::ChainId(1), pool, 1, anchor)
                .unwrap();
        let request = leani_source_api::DataRequest {
            chain_id: leani_primitives::ChainId(1),
            range: leani_primitives::BlockRange::new(
                leani_primitives::BlockNumber(*range.start()),
                leani_primitives::BlockNumber(*range.end()),
            )
            .unwrap(),
            required: CapabilitySet::of(Capability::Header),
            log_fields: leani_primitives::LogFieldSet::NONE,
            allow_filtered: false,
            projection: leani_source_api::FieldProjection::default(),
            filters: leani_source_api::FilterSet::default(),
            minimum_finality: leani_primitives::Finality::Finalized,
            verification_policy: leani_source_api::VerificationPolicy::CompleteCryptographic,
        };
        let budget = leani_source_api::SourceBudget {
            max_input_bytes: 1 << 30,
            max_frame_bytes: 1 << 20,
            max_frames: 1_000,
            max_buffered_frames: 64,
            max_in_flight_requests: 4,
            temporary_disk_bytes: 1,
            max_resident_bytes: 1 << 30,
        };
        let mut frames = Vec::new();
        for chunk in source.plan(&request).await?.chunks {
            let mut stream = source
                .open(&chunk, budget, tokio_util::sync::CancellationToken::new())
                .await?;
            while let Some(frame) = stream.next().await {
                frames.push(frame?);
            }
        }
        Ok(frames)
    }

    #[tokio::test]
    async fn anchored_history_proves_each_chunk_down_from_its_anchor() {
        let (served, hashes) = header_chain(600, 0);
        let endpoint = serving(served, Duration::ZERO).await;
        let frames = anchored_headers(endpoint, (600, hashes[599]), 100..=300)
            .await
            .unwrap();
        assert_eq!(
            frames
                .iter()
                .map(|frame| (frame.block.number.0, B256::from(frame.block.hash.0)))
                .collect::<Vec<_>>(),
            (100..=300)
                .map(|number| (number, hashes[usize::try_from(number).unwrap() - 1]))
                .collect::<Vec<_>>()
        );
        assert!(
            frames
                .iter()
                .all(|frame| frame.finality == leani_primitives::Finality::Finalized)
        );

        // A chain that links up by itself but is not the anchor's.
        let (forked, _) = header_chain(600, 1);
        let endpoint = serving(forked, Duration::ZERO).await;
        assert!(
            anchored_headers(endpoint, (600, hashes[599]), 100..=300)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn a_hash_resolves_to_the_block_whose_header_hashes_to_it() {
        let (served, hashes) = header_chain(64, 0);
        let pool = RpcQuorum::new(vec![serving(served, Duration::ZERO).await], 1, 1_000).unwrap();
        assert_eq!(pool.proven_ref(hashes[41]).await.unwrap().number.0, 42);
        assert!(pool.proven_ref(B256::repeat_byte(1)).await.is_err());
    }

    #[tokio::test]
    async fn a_range_spreads_over_every_endpoint() {
        let blocks = (1..=64)
            .map(|number| {
                let hash = B256::with_last_byte(u8::try_from(number).unwrap());
                let served = serde_json::json!({"number": format!("{number:#x}"), "hash": format!("{hash:#x}")});
                (number, served)
            })
            .collect::<Vec<_>>();
        let mut endpoints = Vec::new();
        let mut requests = Vec::new();
        for _ in 0..7 {
            let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            endpoints
                .push(counted(blocks.clone(), Duration::from_millis(50), counter.clone()).await);
            requests.push(counter);
        }
        let pool = RpcQuorum::new(endpoints, 1, 1_000).unwrap();
        pool.blocks(1, 64, FetchMode::Header).await.unwrap();
        // Eight parts: each endpoint takes one before any takes a second.
        assert!(
            requests
                .iter()
                .all(|served| served.load(std::sync::atomic::Ordering::Relaxed) > 0),
            "{requests:?}"
        );
    }

    #[tokio::test]
    async fn a_slow_endpoint_does_not_hold_up_a_range() {
        let blocks = (1..=40)
            .map(|number| (number, B256::with_last_byte(u8::try_from(number).unwrap())))
            .collect::<Vec<_>>();
        let slow = delayed_endpoint(blocks.clone(), Duration::from_secs(20)).await;
        let fast = endpoint(blocks).await;
        let pool = RpcQuorum::new(vec![slow, fast], 1, 1_000).unwrap();
        // The fast endpoint takes over the parts the slow one still fetches,
        // also a range of one part.
        for last in [40, 8] {
            let served = tokio::time::timeout(
                Duration::from_secs(10),
                pool.blocks(1, last, FetchMode::Header),
            )
            .await
            .expect("the fast endpoint took over")
            .unwrap();
            assert_eq!(served.len(), usize::try_from(last).unwrap());
        }
    }

    #[tokio::test]
    async fn a_part_is_fetched_once_while_its_endpoint_takes_its_usual_time() {
        let blocks = (1..=8)
            .map(|number| {
                let hash = B256::with_last_byte(u8::try_from(number).unwrap());
                let served = serde_json::json!({"number": format!("{number:#x}"), "hash": format!("{hash:#x}")});
                (number, served)
            })
            .collect::<Vec<_>>();
        let requests: [Arc<std::sync::atomic::AtomicUsize>; 2] = Default::default();
        let mut endpoints = Vec::new();
        for counter in &requests {
            endpoints
                .push(counted(blocks.clone(), Duration::from_millis(50), counter.clone()).await);
        }
        let pool = RpcQuorum::new(endpoints, 1, 1_000).unwrap();
        // The first endpoint takes the one part each time, and the second
        // leaves it to it, also before it knows its usual time.
        for _ in 0..2 {
            pool.blocks(1, 8, FetchMode::Header).await.unwrap();
        }
        assert_eq!(requests[1].load(std::sync::atomic::Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn a_part_at_an_endpoint_known_to_be_slow_is_taken_over_at_once() {
        let blocks = (1..=16)
            .map(|number| (number, B256::with_last_byte(u8::try_from(number).unwrap())))
            .collect::<Vec<_>>();
        let slow = RpcClient::new(
            delayed_endpoint(blocks.clone(), Duration::from_millis(1_500)).await,
            1_000,
        )
        .unwrap();
        let fast = RpcClient::new(endpoint(blocks).await, 1_000).unwrap();
        let alone = RpcQuorum {
            clients: vec![slow.clone()],
            quorum: 1,
        };
        alone.blocks(1, 8, FetchMode::Header).await.unwrap();
        let pool = RpcQuorum {
            clients: vec![slow, fast],
            quorum: 1,
        };
        let started = tokio::time::Instant::now();
        pool.blocks(1, 16, FetchMode::Header).await.unwrap();
        // The slow endpoint's parts take seconds; the fast one does not wait
        // for twice that before it serves the part the slow one took.
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "{:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn an_endpoint_with_a_fast_pace_keeps_several_parts_in_flight() {
        let blocks = (1..=64)
            .map(|number| (number, B256::with_last_byte(u8::try_from(number).unwrap())))
            .collect::<Vec<_>>();
        let pool = RpcQuorum::new(
            vec![delayed_endpoint(blocks, Duration::from_millis(300)).await],
            1,
            1_000,
        )
        .unwrap();
        let started = tokio::time::Instant::now();
        pool.blocks(1, 64, FetchMode::Header).await.unwrap();
        // Its eight parts one after another would take 2.4 s of round trips.
        assert!(
            started.elapsed() < Duration::from_millis(1_500),
            "{:?}",
            started.elapsed()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn calls_queued_during_a_rate_limit_speed_up_once_it_lifts() {
        let client = RpcClient::new("http://127.0.0.1:1/queued".parse().unwrap(), 1_000).unwrap();
        lock(&client.shared.pace).interval = SLOWEST_CALL_INTERVAL;
        let started = tokio::time::Instant::now();
        futures::join!(
            futures::future::join_all((0..20).map(|_| client.pace(1))),
            async { lock(&client.shared.pace).interval = client.fastest },
        );
        // Only the first call goes out at the slow interval.
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "{:?}",
            started.elapsed()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_burst_of_rate_limits_slows_an_endpoint_once() {
        let client = RpcClient::new("http://127.0.0.1:1/burst".parse().unwrap(), 1_000).unwrap();
        let sent = tokio::time::Instant::now();
        tokio::time::advance(Duration::from_millis(1)).await;
        // Requests sent together meet the same limit.
        for _ in 0..5 {
            client.throttled(sent);
        }
        assert_eq!(lock(&client.shared.pace).interval, Duration::from_millis(2));
        tokio::time::advance(Duration::from_millis(1)).await;
        client.throttled(tokio::time::Instant::now());
        assert_eq!(lock(&client.shared.pace).interval, Duration::from_millis(4));
    }

    #[test]
    fn an_endpoint_recovers_its_pace_per_call_served() {
        let slowed = |path: &str| {
            let client =
                RpcClient::new(format!("http://127.0.0.1:1/{path}").parse().unwrap(), 1).unwrap();
            lock(&client.shared.pace).interval = SLOWEST_CALL_INTERVAL * 4;
            client
        };
        let (batched, single) = (slowed("batched"), slowed("single"));
        batched.served(16);
        for _ in 0..16 {
            single.served(1);
        }
        assert_eq!(
            lock(&batched.shared.pace).interval,
            lock(&single.shared.pace).interval
        );
        assert!(lock(&batched.shared.pace).interval < SLOWEST_CALL_INTERVAL * 2);
    }

    fn headers(pairs: &[(&'static str, &'static str)]) -> reqwest::header::HeaderMap {
        pairs
            .iter()
            .map(|(name, value)| {
                (
                    reqwest::header::HeaderName::from_static(name),
                    reqwest::header::HeaderValue::from_static(value),
                )
            })
            .collect()
    }

    #[test]
    fn rate_limit_headers_name_the_pace_they_allow() {
        let once = |pairs: &[(&'static str, &'static str)]| {
            announced_interval(&headers(pairs), &mut Duration::default(), 0)
        };
        // Policies with windows, as fastnode sends them: the tightest counts.
        assert_eq!(
            once(&[("x-ratelimit-limit", "10, 10;w=1, 50;w=1")]),
            Some(Duration::from_millis(100))
        );
        assert_eq!(
            once(&[("ratelimit-policy", "\"default\";q=100;w=60")]),
            Some(Duration::from_millis(600))
        );
        assert_eq!(once(&[("x-ratelimit-limit", "1200")]), None);
        assert_eq!(once(&[]), None);
        // A limit and a Unix-time reset, as Pocket sends them; later in a
        // fixed window the reset is nearer, but the window stays.
        let mut window = Duration::ZERO;
        let pocket = headers(&[
            ("x-ratelimit-limit", "1200"),
            ("x-ratelimit-reset", "1793188972"),
        ]);
        assert_eq!(
            announced_interval(&pocket, &mut window, 1_793_188_912),
            Some(Duration::from_millis(50))
        );
        assert_eq!(
            announced_interval(&pocket, &mut window, 1_793_188_962),
            Some(Duration::from_millis(50))
        );
    }

    #[test]
    fn an_endpoint_keeps_to_the_pace_its_headers_announce() {
        let client =
            RpcClient::new("http://127.0.0.1:1/announcing".parse().unwrap(), 1_000).unwrap();
        client.heard(&headers(&[("x-ratelimit-limit", "10;w=1")]), false);
        assert_eq!(
            lock(&client.shared.pace).interval,
            Duration::from_millis(100)
        );
        client.throttled(tokio::time::Instant::now());
        client.served(1_000);
        // It recovers to the announced pace, not to the configured one.
        assert_eq!(
            lock(&client.shared.pace).interval,
            Duration::from_millis(100)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_rate_limited_endpoint_waits_as_long_as_it_asks() {
        let client =
            RpcClient::new("http://127.0.0.1:1/retry-after".parse().unwrap(), 1_000).unwrap();
        client.heard(&headers(&[("retry-after", "9")]), true);
        let started = tokio::time::Instant::now();
        client.pace(1).await;
        assert!(started.elapsed() >= Duration::from_secs(9));
        // A long one is cut short, so no endpoint holds calls up for long.
        client.heard(&headers(&[("retry-after", "3600")]), true);
        let started = tokio::time::Instant::now();
        client.pace(1).await;
        assert!(started.elapsed() <= LONGEST_RETRY_AFTER);
    }

    #[test]
    fn clients_of_one_endpoint_share_its_budget() {
        let url: Url = "http://127.0.0.1:1/shared".parse().unwrap();
        let (live, history) = (
            RpcClient::new(url.clone(), 1_000).unwrap(),
            RpcClient::new(url, 1_000).unwrap(),
        );
        live.throttled(tokio::time::Instant::now());
        assert_eq!(
            lock(&history.shared.pace).interval,
            Duration::from_millis(2)
        );
    }

    #[tokio::test]
    async fn a_slow_endpoint_is_not_booked_for_parts_others_served() {
        let blocks = (1..=40)
            .map(|number| (number, B256::with_last_byte(u8::try_from(number).unwrap())))
            .collect::<Vec<_>>();
        let pool = RpcQuorum {
            clients: vec![
                RpcClient::new(endpoint(blocks.clone()).await, 1).unwrap(),
                RpcClient::new(endpoint(blocks).await, 1_000).unwrap(),
            ],
            quorum: 1,
        };
        // The fast endpoint serves every range first; the slow one's next
        // call waits for no part it never fetched, at most for the one part
        // it started.
        for _ in 0..5 {
            pool.blocks(1, 40, FetchMode::Header).await.unwrap();
        }
        let next = *pool.clients[0]
            .shared
            .next
            .try_lock()
            .expect("no call waits");
        let waits = next.saturating_duration_since(tokio::time::Instant::now());
        assert!(waits <= Duration::from_secs(8), "{waits:?}");
    }

    #[tokio::test]
    async fn a_pool_puts_own_endpoints_first_and_keeps_few_public_ones() {
        let serving = || endpoint(vec![(1, B256::repeat_byte(1))]);
        let own = serving().await;
        let bundled = serving().await;
        let mut listed = vec![bundled.clone()];
        for _ in 0..9 {
            listed.push(serving().await);
        }
        let silent: Url = "http://127.0.0.1:1/".parse().unwrap();
        listed.push(silent);
        listed
            .push(delayed_endpoint(vec![(1, B256::repeat_byte(1))], Duration::from_secs(60)).await);
        let pool = RpcQuorum::pooled(vec![own.clone()], 1_000, vec![bundled.clone()], listed, 1)
            .unwrap()
            .qualified(1, true);
        let pool = tokio::time::timeout(PUBLIC_QUALIFY_WITHIN * 2, pool)
            .await
            .expect("an endpoint that hangs is given up")
            .unwrap();
        let tiers = pool
            .hosts()
            .into_iter()
            .map(|(tier, _)| tier)
            .collect::<Vec<_>>();
        // The bundled endpoint is listed too but kept once; eight public
        // endpoints at most, and the ones that do not answer are dropped.
        assert_eq!(
            tiers,
            [Tier::Own, Tier::Bundled]
                .into_iter()
                .chain(std::iter::repeat_n(Tier::Listed, 7))
                .collect::<Vec<_>>()
        );
        assert_eq!(pool.endpoints()[..2], [&own, &bundled]);
    }

    #[tokio::test]
    async fn errors_leave_out_an_endpoints_key() {
        let client =
            RpcClient::new("http://127.0.0.1:1/v2/secret-key".parse().unwrap(), 1_000).unwrap();
        let error = client.head().await.unwrap_err().to_string();
        assert!(!error.contains("secret-key"), "{error}");
    }

    #[test]
    fn a_quorum_needs_as_many_endpoints() {
        let endpoints = vec!["http://127.0.0.1:1/".parse().unwrap()];
        assert!(RpcQuorum::new(endpoints.clone(), 2, 10).is_err());
        assert!(RpcQuorum::new(endpoints, 0, 10).is_err());
    }

    #[test]
    fn fetches_read_only_what_the_request_needs() {
        let mode = |capabilities: &[Capability]| {
            FetchMode::for_capabilities(
                capabilities
                    .iter()
                    .fold(CapabilitySet::of(Capability::Header), |set, capability| {
                        set.with(*capability)
                    }),
            )
        };
        assert_eq!(mode(&[]), FetchMode::Header);
        assert_eq!(mode(&[Capability::Logs]), FetchMode::Receipts);
        assert_eq!(
            mode(&[Capability::Logs, Capability::Transactions]),
            FetchMode::Full
        );
        assert_eq!(
            FetchMode::from_byte(FetchMode::Receipts.byte()),
            FetchMode::Receipts
        );
    }
}
