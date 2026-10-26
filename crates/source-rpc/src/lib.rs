//! Blocks from JSON-RPC endpoints. Each block's contents are checked against
//! its own header hash, transactions root, and receipts root; which block is
//! canonical is what a quorum of independent endpoints agrees on.

use std::{sync::Arc, time::Duration};

use alloy_primitives::B256;
use futures::{StreamExt, TryStreamExt, stream};
use leani_primitives::{Capability, CapabilitySet};
use serde_json::Value;
use url::Url;

/// Attempts per request while an endpoint rate-limits or fails.
const RETRIES: u32 = 5;
/// Calls in flight per endpoint when it takes no batches.
const CONCURRENT_CALLS: usize = 16;
/// Response bodies past this are refused: an endpoint must not exhaust memory.
const MAX_RESPONSE_BYTES: usize = 256 * 1_024 * 1_024;

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

/// A JSON-RPC endpoint. HTTP/1.1 only: some public Nitro endpoints stall
/// HTTP/2 requests.
#[derive(Clone, Debug)]
pub struct RpcClient {
    endpoint: Url,
    client: reqwest::Client,
    /// When the next call may go out, to keep within `calls_per_second`.
    pace: Arc<tokio::sync::Mutex<tokio::time::Instant>>,
    call_interval: Duration,
    /// Cleared once the endpoint refuses a batch for any reason but a rate
    /// limit; it then gets one request per call.
    batches: Arc<std::sync::atomic::AtomicBool>,
}

impl RpcClient {
    /// # Errors
    ///
    /// Returns an error when the HTTP client cannot be built.
    pub fn new(endpoint: Url, calls_per_second: u32) -> Result<Self, RpcError> {
        let client = reqwest::Client::builder()
            .http1_only()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|error| RpcError::Request {
                method: "client",
                detail: error.to_string(),
            })?;
        Ok(Self {
            endpoint,
            client,
            pace: Arc::new(tokio::sync::Mutex::new(tokio::time::Instant::now())),
            call_interval: Duration::from_secs(1) / calls_per_second.max(1),
            batches: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        })
    }

    /// Wait for the budget of `calls` calls.
    async fn pace(&self, calls: u32) {
        let start = {
            let mut next = self.pace.lock().await;
            let start = (*next).max(tokio::time::Instant::now());
            *next = start + self.call_interval * calls;
            start
        };
        tokio::time::sleep_until(start).await;
    }

    #[must_use]
    pub const fn endpoint(&self) -> &Url {
        &self.endpoint
    }

    /// POST `body`, backing off while the endpoint rate-limits or fails:
    /// public endpoints answer bursts with 429 for a few seconds.
    async fn post<T: serde::de::DeserializeOwned>(
        &self,
        method: &'static str,
        body: &Value,
    ) -> Result<T, RpcError> {
        let failed = |detail: String| RpcError::Request { method, detail };
        let calls = body
            .as_array()
            .map_or(1, |calls| u32::try_from(calls.len()).unwrap_or(u32::MAX));
        let mut backoff = Duration::from_millis(500);
        for attempt in 0..RETRIES {
            self.pace(calls).await;
            let response = self
                .client
                .post(self.endpoint.clone())
                .json(body)
                .send()
                .await
                .map_err(|error| failed(error.to_string()))?;
            let status = response.status();
            if (status == reqwest::StatusCode::TOO_MANY_REQUESTS || status.is_server_error())
                && attempt + 1 < RETRIES
            {
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(8));
                continue;
            }
            let response = response
                .error_for_status()
                .map_err(|error| failed(error.to_string()))?;
            return read_json(response, MAX_RESPONSE_BYTES)
                .await
                .map_err(failed);
        }
        Err(failed("retries exhausted".to_owned()))
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
        let batched = if self.batches.load(std::sync::atomic::Ordering::Relaxed) {
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
        self.pace(u32::try_from(calls.len()).unwrap_or(u32::MAX))
            .await;
        let response = self
            .client
            .post(self.endpoint.clone())
            .json(calls)
            .send()
            .await
            .ok()?;
        if !response.status().is_success() {
            if response.status() != reqwest::StatusCode::TOO_MANY_REQUESTS {
                self.batches
                    .store(false, std::sync::atomic::Ordering::Relaxed);
            }
            return None;
        }
        let responses = read_json::<Vec<Value>>(response, MAX_RESPONSE_BYTES).await;
        if responses.is_err() {
            self.batches
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
/// block hash before it counts as canonical.
#[derive(Clone, Debug)]
pub struct RpcQuorum {
    clients: Vec<RpcClient>,
    quorum: usize,
    /// The endpoint full blocks came from last; fetches stay there while it
    /// serves them.
    fetcher: Arc<std::sync::atomic::AtomicUsize>,
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
        if quorum == 0 || quorum > endpoints.len() {
            return Err(RpcError::Malformed(format!(
                "quorum {quorum} must be within 1..={}",
                endpoints.len()
            )));
        }
        let clients = endpoints
            .into_iter()
            .map(|endpoint| RpcClient::new(endpoint, calls_per_second))
            .collect::<Result<_, _>>()?;
        Ok(Self {
            clients,
            quorum,
            fetcher: Arc::default(),
        })
    }

    #[must_use]
    pub const fn quorum(&self) -> usize {
        self.quorum
    }

    #[must_use]
    pub fn endpoints(&self) -> Vec<&Url> {
        self.clients.iter().map(RpcClient::endpoint).collect()
    }

    /// Fetch full blocks from the first endpoint that serves them; the others
    /// only answer hash checks.
    ///
    /// # Errors
    ///
    /// Returns the last error when no endpoint serves the range.
    pub async fn blocks(
        &self,
        first: u64,
        last: u64,
        mode: FetchMode,
    ) -> Result<Vec<(Value, Value)>, RpcError> {
        use std::sync::atomic::Ordering;

        let mut failure = None;
        let preferred = self.fetcher.load(Ordering::Relaxed);
        for offset in 0..self.clients.len() {
            let index = (preferred + offset) % self.clients.len();
            match self.clients[index].blocks(first, last, mode).await {
                Ok(blocks) => {
                    self.fetcher.store(index, Ordering::Relaxed);
                    return Ok(blocks);
                }
                Err(error) => failure = Some(error),
            }
        }
        Err(failure.unwrap_or_else(|| RpcError::Malformed("no endpoints".to_owned())))
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
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let by_number = blocks.iter().copied().collect::<HashMap<_, _>>();
        let latest = blocks.iter().map(|(number, _)| *number).max().unwrap();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let by_number = by_number.clone();
                tokio::spawn(async move {
                    let mut request = vec![0; 16_384];
                    let read = socket.read(&mut request).await.unwrap_or(0);
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
                    let result = by_number.get(&number).map_or(Value::Null, |hash| {
                        serde_json::json!({"number": format!("{number:#x}"), "hash": format!("{hash:#x}")})
                    });
                    let response = serde_json::json!({"jsonrpc": "2.0", "id": 1, "result": result})
                        .to_string();
                    let reply = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{response}",
                        response.len()
                    );
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
