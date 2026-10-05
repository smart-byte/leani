//! CLI transport for the same historical source and execution pipeline as node jobs.
use std::{
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::{
    Exit, NativeBackfillControl, OnDemandP2pBridge, ShutdownSignals, backfill_processor_config,
    configured_store_config, execution_p2p_source, historical_runtime_config, historical_services,
    historical_source_budget, history_sources_with_bridge, p2p_history_anchor,
    require_ordered_backfill_start,
};
use crate::{
    config::{ArtifactStorageBackend, Config},
    processors::ProcessorRegistry,
};

#[allow(clippy::too_many_arguments)]
pub(super) async fn run(
    config_path: &Path,
    processor: Option<&str>,
    from: u64,
    to: u64,
    endpoint: Option<&url::Url>,
    token: Option<&str>,
    registry: &ProcessorRegistry,
) -> Result<Exit> {
    leani_primitives::BlockRange::new(
        leani_primitives::BlockNumber(from),
        leani_primitives::BlockNumber(to),
    )?;
    let processor = match processor {
        Some(processor) => processor.to_owned(),
        None => only_configured_processor(&Config::load(config_path).with_context(|| {
            format!(
                "--processor is required without a readable {}",
                config_path.display()
            )
        })?)?,
    };
    if let Some(endpoint) = endpoint {
        return remote(endpoint, token, &processor, from, to).await;
    }
    Box::pin(standalone(config_path, &processor, from, to, registry)).await
}

/// The default `--processor`: the instance of a configuration's only processor.
fn only_configured_processor(config: &Config) -> Result<String> {
    match config.processors.as_slice() {
        [only] => Ok(only.instance.clone()),
        processors => bail!(
            "the configuration enables {} processors; pass --processor with one of [{}]",
            processors.len(),
            processors
                .iter()
                .map(|processor| processor.instance.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

fn api_url(endpoint: &url::Url, id: Option<&str>) -> Result<url::Url> {
    if !matches!(endpoint.scheme(), "http" | "https")
        || endpoint.host_str().is_none()
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
    {
        bail!(
            "backfill endpoint must be an HTTP(S) base URL without credentials, query or fragment"
        );
    }
    let mut url = endpoint.clone();
    let mut path = url
        .path_segments_mut()
        .map_err(|()| anyhow::anyhow!("endpoint cannot be a base URL"))?;
    path.pop_if_empty()
        .extend(["admin", "v1", "materialization-jobs"]);
    if let Some(id) = id {
        path.push(id);
    }
    drop(path);
    Ok(url)
}

/// Attempts per API call; the create request's idempotency key makes a
/// repeated create return the job the node already accepted.
const API_ATTEMPTS: u32 = 5;

/// Pause between status polls of a submitted job.
const POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Pause between progress lines of a standalone backfill.
const PROGRESS_INTERVAL: Duration = Duration::from_secs(30);

/// The blocks a standalone job has committed, across its runs. Best effort:
/// a failed read must not end the backfill.
async fn committed_blocks(store: &leani_store_sqlite::SqliteStore, job_id: &str) -> Option<u64> {
    store
        .job(job_id)
        .await
        .ok()
        .flatten()
        .and_then(|record| record.checkpoint)
        .and_then(|checkpoint| {
            leani_runtime::historical_checkpoint_committed_blocks(&checkpoint).ok()
        })
}

/// Why one request/response exchange failed.
enum Failure {
    /// The connection failed, before or after the response headers: a retry
    /// may succeed.
    Transport(anyhow::Error),
    Final(anyhow::Error),
}

/// Send `request` and read its whole body.
async fn exchange(
    request: reqwest::RequestBuilder,
) -> Result<(reqwest::StatusCode, Vec<u8>), Failure> {
    let transport = |error: reqwest::Error| Failure::Transport(error.without_url().into());
    let mut response = request.send().await.map_err(transport)?;
    let status = response.status();
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(transport)? {
        if bytes.len().saturating_add(chunk.len()) > 1_048_576 {
            return Err(Failure::Final(anyhow::anyhow!(
                "backfill API response exceeds 1 MiB"
            )));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok((status, bytes))
}

fn decode(status: reqwest::StatusCode, bytes: &[u8]) -> Result<serde_json::Value> {
    if !status.is_success() {
        // Error bodies need not be JSON: a wrong prefix gets an empty 404.
        let body = String::from_utf8_lossy(bytes);
        let body = body.chars().take(512).collect::<String>();
        bail!("backfill API returned {status}: {body}");
    }
    serde_json::from_slice(bytes).context("decode backfill API response")
}

/// Send `request`, retrying transport failures, including a body cut short,
/// and 5xx answers with backoff.
async fn call(request: impl Fn() -> reqwest::RequestBuilder) -> Result<serde_json::Value> {
    let mut delay = Duration::from_millis(500);
    let mut attempt = 1;
    loop {
        let last = attempt == API_ATTEMPTS;
        match exchange(request()).await {
            Ok((status, _)) if status.is_server_error() && !last => {
                warn!(attempt, %status, "backfill API call failed; retrying");
            }
            Ok((status, bytes)) => return decode(status, &bytes),
            Err(Failure::Transport(error)) if !last => {
                warn!(attempt, error = %error, "backfill API call failed; retrying");
            }
            Err(Failure::Transport(error) | Failure::Final(error)) => return Err(error),
        }
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(Duration::from_secs(8));
        attempt += 1;
    }
}

/// A plain-HTTP endpoint that is not a loopback name or address.
fn cleartext_remote(endpoint: &url::Url) -> bool {
    endpoint.scheme() == "http"
        && match endpoint.host() {
            Some(url::Host::Domain(name)) => !name.eq_ignore_ascii_case("localhost"),
            Some(url::Host::Ipv4(address)) => !address.is_loopback(),
            Some(url::Host::Ipv6(address)) => !address.is_loopback(),
            None => true,
        }
}

async fn remote(
    endpoint: &url::Url,
    token: Option<&str>,
    processor: &str,
    from: u64,
    to: u64,
) -> Result<Exit> {
    // Listen before submitting: Ctrl-C during the request must still cancel
    // the job the node creates, rather than kill the process.
    let mut signals = ShutdownSignals::new()?;
    let (interrupt, interrupts) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(
        async move { while signals.recv().await.is_ok() && interrupt.send(()).is_ok() {} },
    );
    remote_with_interrupts(endpoint, token, processor, from, to, interrupts).await
}

/// [`remote`], cancelling the job on the first of `interrupts`. Every wait,
/// for a request or between polls, also waits for them.
async fn remote_with_interrupts(
    endpoint: &url::Url,
    token: Option<&str>,
    processor: &str,
    from: u64,
    to: u64,
    mut interrupts: tokio::sync::mpsc::UnboundedReceiver<()>,
) -> Result<Exit> {
    let collection = api_url(endpoint, None)?;
    if token.is_some() && cleartext_remote(endpoint) {
        warn!("sending the API bearer token over plain HTTP to a non-loopback endpoint");
    }
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        "x-leani-request",
        reqwest::header::HeaderValue::from_static("1"),
    );
    if let Some(token) = token {
        let mut value = reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|_| anyhow::anyhow!("invalid API bearer token"))?;
        value.set_sensitive(true);
        headers.insert(reqwest::header::AUTHORIZATION, value);
    }
    let client = reqwest::Client::builder()
        .default_headers(headers)
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(30))
        .build()?;
    // Each invocation can retry a failed/cancelled job. FillMissing reuses
    // committed coverage; a permanent key for the range would return the old
    // terminal failure forever. Keep this key stable within this submission.
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let identity =
        blake3::hash(format!("{processor}:{from}:{to}:{nonce}:{}", std::process::id()).as_bytes());
    let request = leani_api::CreateMaterializationRequest {
        processor: processor.to_owned(),
        from_block: Some(from),
        to_block: Some(to.into()),
        ranges: Vec::new(),
        mode: leani_api::BackfillExecutionMode::FillMissing,
        idempotency_key: format!("cli-{identity}"),
    };
    let mut interrupted = false;
    let create = call(|| client.post(collection.clone()).json(&request));
    tokio::pin!(create);
    // The create keeps running on a first interrupt: its job ID is what the
    // cancellation names.
    let mut status = loop {
        tokio::select! {
            status = &mut create => break status?,
            Some(()) = interrupts.recv() => {
                if interrupted {
                    bail!("backfill interrupted before the node confirmed the job");
                }
                interrupted = true;
            }
        }
    };
    let id = status["id"]
        .as_str()
        .context("backfill API response omitted job ID")?
        .to_owned();
    let job = api_url(endpoint, Some(&id))?;
    loop {
        match status["state"].as_str() {
            Some("completed") => {
                println!("{}", serde_json::to_string_pretty(&status)?);
                return Ok(Exit::Success);
            }
            Some("failed" | "cancelled") => bail!("backfill did not complete: {status}"),
            Some(_) => {}
            None => bail!("backfill API response omitted job state"),
        }
        if interrupted {
            let mut cancel = job.clone();
            cancel
                .path_segments_mut()
                .map_err(|()| anyhow::anyhow!("invalid job URL"))?
                .push("cancel");
            status = tokio::select! {
                status = call(|| client.post(cancel.clone())) => status?,
                Some(()) = interrupts.recv() => {
                    bail!("backfill interrupted again before the node confirmed its cancellation");
                }
            };
            // The job may have completed before the cancel arrived.
            if status["state"].as_str() == Some("completed") {
                println!("{}", serde_json::to_string_pretty(&status)?);
                return Ok(Exit::Success);
            }
            bail!("backfill cancelled: {status}");
        }
        tokio::select! {
            () = tokio::time::sleep(POLL_INTERVAL) => {}
            Some(()) = interrupts.recv() => {
                interrupted = true;
                continue;
            }
        }
        // An interrupt drops a status request still in flight and cancels.
        tokio::select! {
            polled = call(|| client.get(job.clone())) => status = polled?,
            Some(()) = interrupts.recv() => interrupted = true,
        }
    }
}

#[allow(clippy::too_many_lines)]
async fn standalone(
    config_path: &Path,
    processor_id: &str,
    from: u64,
    to: u64,
    registry: &ProcessorRegistry,
) -> Result<Exit> {
    use leani_primitives::{BlockNumber, BlockRange, ChainId};
    use leani_runtime::{BackfillJob, HistoricalRuntime};
    use leani_store_sqlite::SqliteStore;

    let config = Config::load(config_path)
        .with_context(|| format!("load configuration {}", config_path.display()))?
        .validate()
        .map_err(|errors| anyhow::anyhow!(errors))?
        .into_inner();
    let configured = backfill_processor_config(&config, processor_id)?;
    let processor = registry.instantiate(configured, config.chain.chain_id)?;
    let range = BlockRange::new(BlockNumber(from), BlockNumber(to))?;
    let _data_dir_lock = crate::local_state::lock_runtime_directory(&config.data_dir).context(
        "stop the node before standalone backfill, or pass --endpoint to submit to its API",
    )?;
    let store = SqliteStore::open(
        configured_store_config(&config, config.data_dir.join("leani.sqlite"))
            .with_processors(vec![processor.descriptor().clone()]),
    )
    .await
    .map_err(crate::uniswap_markets::explain_compact_refusal)?;
    require_ordered_backfill_start(&store, processor.as_ref(), configured, from).await?;
    let cancellation = CancellationToken::new();
    let mut signals = ShutdownSignals::new()?;
    let telemetry = leani_source_api::NetworkTelemetry::default();
    let bridge = if matches!(config.sources.live.kind, crate::config::LiveSourceKind::P2p)
        && !configured.require_retained_input
    {
        // Like a running node, retain locally verified finality so repeated
        // standalone runs can advance beyond the original checkpoint's age.
        let anchor_file = leani_finality_beacon_api::AnchorFile::ReadWrite {
            path: config
                .data_dir
                .join(leani_finality_beacon_api::FINALITY_ANCHOR_FILE),
            write_failures: std::sync::Arc::default(),
        };
        let anchor = tokio::select! {
            anchor = p2p_history_anchor(&config, anchor_file) => anchor.context("initialize verified P2P backfill fallback")?,
            signal = signals.recv() => { signal?; bail!("backfill cancelled during finality initialization"); }
        };
        if range.end() > anchor.block.number {
            bail!(
                "requested range ends at {}, after verified finalized head {}",
                range.end(),
                anchor.block.number
            );
        }
        let source = execution_p2p_source(&config, telemetry.clone(), None)?;
        Some(OnDemandP2pBridge {
            source: source.as_ref().clone(),
            anchor,
        })
    } else {
        None
    };
    let raw_history = if config.raw_history.enabled {
        let raw = config.raw_history;
        let mut settings =
            leani_store_history::HistoryStoreConfig::new(config.data_dir.join("raw-history"))
                .with_budget(leani_store_history::StorageBudget {
                    maximum_logical_bytes: raw.maximum_logical_bytes.bytes(),
                    maximum_physical_bytes: raw.maximum_physical_bytes.bytes(),
                    maximum_frame_logical_bytes: raw.maximum_frame_logical_bytes.bytes(),
                    maximum_segment_logical_bytes: raw.maximum_segment_logical_bytes.bytes(),
                    maximum_segment_physical_bytes: raw.maximum_segment_physical_bytes.bytes(),
                });
        settings.reader_connections = raw.reader_connections;
        Some(leani_store_history::HistoryStore::open(settings).await?)
    } else {
        None
    };
    let (sources, verification_policy) = history_sources_with_bridge(
        &config,
        processor.as_ref(),
        raw_history.as_ref(),
        bridge.as_ref(),
        range,
    )?;
    let source_ids = sources
        .iter()
        .map(|source| source.descriptor().id.to_string())
        .collect::<Vec<_>>();
    info!(
        processor = %processor.descriptor().instance,
        requested_from = from,
        requested_to = to,
        requested_blocks = range.len(),
        source_ids = ?source_ids,
        "starting processor historical backfill"
    );
    if let Some(bridge) = &bridge {
        // As the node seeds it: retained blocks the anchor does not prove are
        // reverted first, and the next serve's startup reconciliation undoes
        // any processor coverage of them.
        let chain_id = ChainId(config.chain.chain_id);
        store
            .revert_unproven_recent_blocks(chain_id, bridge.anchor.block, &[])
            .await?;
        store
            .store_canonical_anchor(
                chain_id,
                bridge.anchor.block,
                leani_primitives::Finality::Finalized,
            )
            .await?;
    }
    // The configured pipeline, as the node's on-demand backfills use it.
    let runtime_config = historical_runtime_config(&config, sources.len());
    let (pipeline_budget, material_coordinator) = historical_services(&config)?;
    let runtime = HistoricalRuntime::new_with_sources(
        store.clone(),
        sources,
        processor.clone(),
        runtime_config,
    )?
    .with_pipeline_budget(pipeline_budget);
    let runtime = match material_coordinator {
        Some(coordinator) => runtime.with_material_coordinator(coordinator),
        None => runtime,
    };
    let job_id = format!("{processor_id}-{}-{from}-{to}", config.chain.chain_id);
    let job = BackfillJob::for_processor(
        job_id.clone(),
        processor.as_ref(),
        ChainId(config.chain.chain_id),
        range,
        verification_policy,
    )?;
    let operation = runtime.run(
        job,
        historical_source_budget(&config, range),
        cancellation.clone(),
    );
    tokio::pin!(operation);
    let mut interrupted = false;
    let mut progress = tokio::time::interval_at(
        tokio::time::Instant::now() + PROGRESS_INTERVAL,
        PROGRESS_INTERVAL,
    );
    // A rerun of the same range resumes its job, and its count.
    let mut committed_before = committed_blocks(&store, &job_id).await.unwrap_or(0);
    let result = loop {
        tokio::select! {
            result = &mut operation => break result,
            _ = progress.tick() => {
                let committed = committed_blocks(&store, &job_id)
                    .await
                    .unwrap_or(committed_before);
                info!(
                    committed_blocks = committed,
                    requested_blocks = range.len(),
                    blocks_per_minute = committed.saturating_sub(committed_before) * 60
                        / PROGRESS_INTERVAL.as_secs(),
                    connected_peers = ?bridge
                        .as_ref()
                        .map(|_| telemetry.snapshot().connected_peer_slots),
                    "processor historical backfill progress"
                );
                committed_before = committed;
            }
            signal = signals.recv() => break match signal {
                Ok(()) => {
                    interrupted = true;
                    cancellation.cancel();
                    tokio::select! {
                        result = &mut operation => result,
                        _ = signals.recv() => std::process::exit(130),
                    }
                }
                Err(error) => {
                    warn!(%error, "the shutdown signal handler stopped; the backfill continues");
                    operation.await
                }
            },
        }
    };
    cancellation.cancel();
    if let Some(bridge) = &bridge {
        // Bounded inside; it flushes the peer store before closing the pool.
        bridge.source.shutdown().await;
    }
    if interrupted && let Some(mut record) = store.job(&job_id).await? {
        // A suspended record would make the next serve resume a backfill the
        // operator stopped.
        if matches!(
            record.state,
            leani_store_sqlite::JobState::Queued | leani_store_sqlite::JobState::Running
        ) {
            record.state = leani_store_sqlite::JobState::Cancelled;
            store.save_job(&record).await?;
        }
    }
    let report = result?;
    if config.artifact_storage.backend == ArtifactStorageBackend::TieredSegments
        && processor.descriptor().lifecycle.artifacts.mode
            == leani_processor_api::ArtifactPolicyMode::Full
    {
        let compacted = NativeBackfillControl::flush_tiered_artifacts(
            &store,
            processor.descriptor(),
            &[range],
            config.artifact_storage.maximum_segments_per_cycle,
        )
        .await?;
        info!(
            processor_instance = %processor.descriptor().instance,
            segments = compacted.segments,
            artifacts = compacted.artifacts,
            logical_bytes = compacted.logical_bytes,
            reclaimed_inline_bytes = compacted.inline_payload_bytes_reclaimed,
            "flushed CLI backfill artifacts to segments"
        );
    }
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(Exit::Success)
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    #[test]
    fn backfill_defaults_to_the_only_configured_processor() {
        let repository = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let mut config =
            crate::config::Config::load(&repository.join("config/modes/windowed.toml")).unwrap();
        let only = config.processors[0].instance.clone();
        assert_eq!(super::only_configured_processor(&config).unwrap(), only);

        let mut second = config.processors[0].clone();
        second.instance = "second".to_owned();
        config.processors.push(second);
        let error = super::only_configured_processor(&config)
            .unwrap_err()
            .to_string();
        assert!(error.contains(&format!("[{only}, second]")), "{error}");
    }

    #[test]
    fn only_non_loopback_plain_http_is_cleartext() {
        for (url, cleartext) in [
            ("http://127.0.0.1:1/", false),
            ("http://[::1]:1/", false),
            ("http://LocalHost/", false),
            ("http://node.internal/", true),
            ("http://10.0.0.1/", true),
            ("https://node.internal/", false),
        ] {
            let url = url::Url::parse(url).unwrap();
            assert_eq!(super::cleartext_remote(&url), cleartext, "{url}");
        }
    }

    #[tokio::test]
    async fn api_calls_retry_a_body_cut_short() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            for body in [&br#"{"sta"#[..], &br#"{"state":"running"}"#[..]] {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = [0_u8; 1_024];
                let _ = stream.read(&mut request).await.unwrap();
                // The first answer promises more body than it sends.
                let head = b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 19\r\nconnection: close\r\n\r\n";
                stream.write_all(head).await.unwrap();
                stream.write_all(body).await.unwrap();
            }
        });
        let client = reqwest::Client::new();
        let value = super::call(|| client.get(&url)).await.unwrap();
        server.await.unwrap();
        assert_eq!(value["state"], "running");
    }

    #[tokio::test]
    async fn an_interrupt_during_a_stalled_status_request_cancels_the_job() {
        let polled = Arc::new(tokio::sync::Notify::new());
        let cancelled = Arc::new(AtomicUsize::new(0));
        let (polled_route, cancelled_route) = (polled.clone(), cancelled.clone());
        let router = axum::Router::new()
            .route(
                "/admin/v1/materialization-jobs",
                axum::routing::post(|| async {
                    axum::Json(serde_json::json!({"id": "job-1", "state": "running"}))
                }),
            )
            .route(
                "/admin/v1/materialization-jobs/job-1",
                axum::routing::get(move || {
                    let polled = polled_route.clone();
                    async move {
                        polled.notify_one();
                        // A node that never answers its status.
                        std::future::pending::<()>().await;
                    }
                }),
            )
            .route(
                "/admin/v1/materialization-jobs/job-1/cancel",
                axum::routing::post(move || {
                    let cancelled = cancelled_route.clone();
                    async move {
                        cancelled.fetch_add(1, Ordering::SeqCst);
                        axum::Json(serde_json::json!({"id": "job-1", "state": "cancelled"}))
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint =
            url::Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let (interrupt, interrupts) = tokio::sync::mpsc::unbounded_channel();
        let run = tokio::spawn(async move {
            super::remote_with_interrupts(&endpoint, None, "blocks", 1, 2, interrupts).await
        });
        polled.notified().await;
        interrupt.send(()).unwrap();
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), run)
            .await
            .expect("the interrupt ends the stalled poll")
            .unwrap();
        server.abort();
        assert!(format!("{:#}", result.unwrap_err()).contains("cancelled"));
        assert_eq!(cancelled.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn api_calls_retry_server_errors() {
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = calls.clone();
        let router = axum::Router::new().route(
            "/",
            axum::routing::get(move || {
                let calls = counted.clone();
                async move {
                    if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                        (
                            axum::http::StatusCode::SERVICE_UNAVAILABLE,
                            "busy".to_owned(),
                        )
                    } else {
                        (
                            axum::http::StatusCode::OK,
                            r#"{"state":"running"}"#.to_owned(),
                        )
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let client = reqwest::Client::new();
        let value = super::call(|| client.get(&url)).await.unwrap();
        server.abort();
        assert_eq!(value["state"], "running");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}
