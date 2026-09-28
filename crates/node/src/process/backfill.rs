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
    processor: &str,
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
    if let Some(endpoint) = endpoint {
        return remote(endpoint, token, processor, from, to).await;
    }
    Box::pin(standalone(config_path, processor, from, to, registry)).await
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

async fn response_json(mut response: reqwest::Response) -> Result<serde_json::Value> {
    let status = response.status();
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(reqwest::Error::without_url)?
    {
        if bytes.len().saturating_add(chunk.len()) > 1_048_576 {
            bail!("backfill API response exceeds 1 MiB");
        }
        bytes.extend_from_slice(&chunk);
    }
    if !status.is_success() {
        // Error bodies need not be JSON: a wrong prefix gets an empty 404.
        let body = String::from_utf8_lossy(&bytes);
        let body = body.chars().take(512).collect::<String>();
        bail!("backfill API returned {status}: {body}");
    }
    serde_json::from_slice(&bytes).context("decode backfill API response")
}

/// Send `request`, retrying transport errors and 5xx answers with backoff.
async fn call(request: impl Fn() -> reqwest::RequestBuilder) -> Result<serde_json::Value> {
    let mut delay = Duration::from_millis(500);
    for attempt in 1..API_ATTEMPTS {
        match request().send().await {
            Ok(response) if !response.status().is_server_error() => {
                return response_json(response).await;
            }
            Ok(response) => {
                warn!(attempt, status = %response.status(), "backfill API call failed; retrying");
            }
            Err(error) => {
                warn!(attempt, error = %error.without_url(), "backfill API call failed; retrying");
            }
        }
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(Duration::from_secs(8));
    }
    response_json(
        request()
            .send()
            .await
            .map_err(reqwest::Error::without_url)?,
    )
    .await
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
    // Listen before submitting: Ctrl-C during the request must still cancel
    // the job the node creates, rather than kill the process.
    let mut signals = ShutdownSignals::new()?;
    let mut interrupted = false;
    let create = call(|| client.post(collection.clone()).json(&request));
    tokio::pin!(create);
    let mut status = loop {
        tokio::select! {
            status = &mut create => break status?,
            signal = signals.recv() => {
                signal?;
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
            status = call(|| client.post(cancel.clone())).await?;
            // The job may have completed before the cancel arrived.
            if status["state"].as_str() == Some("completed") {
                println!("{}", serde_json::to_string_pretty(&status)?);
                return Ok(Exit::Success);
            }
            bail!("backfill cancelled: {status}");
        }
        tokio::select! {
            () = tokio::time::sleep(Duration::from_millis(500)) => {
                status = call(|| client.get(job.clone())).await?;
            },
            signal = signals.recv() => {
                signal?;
                interrupted = true;
            }
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
    let cancellation = CancellationToken::new();
    let mut signals = ShutdownSignals::new()?;
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
        let source =
            execution_p2p_source(&config, leani_source_api::NetworkTelemetry::default(), None)?;
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
    let store = SqliteStore::open(
        configured_store_config(&config, config.data_dir.join("leani.sqlite"))
            .with_processors(vec![processor.descriptor().clone()]),
    )
    .await
    .map_err(crate::uniswap_markets::explain_compact_refusal)?;
    if let Some(bridge) = &bridge {
        store
            .store_canonical_anchor(
                ChainId(config.chain.chain_id),
                bridge.anchor.block,
                leani_primitives::Finality::Finalized,
            )
            .await?;
    }
    require_ordered_backfill_start(&store, processor.as_ref(), configured, from).await?;
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
    let job = BackfillJob::for_processor(
        format!("{processor_id}-{}-{from}-{to}", config.chain.chain_id),
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
    let result = tokio::select! {
        result = &mut operation => result,
        signal = signals.recv() => {
            cancellation.cancel();
            signal?;
            operation.await
        }
    };
    cancellation.cancel();
    if let Some(bridge) = &bridge {
        let _ = tokio::time::timeout(Duration::from_secs(5), bridge.source.shutdown()).await;
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
