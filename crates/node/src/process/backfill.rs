//! CLI transport for the same historical source and execution pipeline as node jobs.
use std::{
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use tokio_util::sync::CancellationToken;
use tracing::info;

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
    let value: serde_json::Value =
        serde_json::from_slice(&bytes).context("decode backfill API response")?;
    if !status.is_success() {
        bail!("backfill API returned {status}: {value}");
    }
    Ok(value)
}

async fn remote(
    endpoint: &url::Url,
    token: Option<&str>,
    processor: &str,
    from: u64,
    to: u64,
) -> Result<Exit> {
    let collection = api_url(endpoint, None)?;
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
    let mut status = response_json(
        client
            .post(collection)
            .json(&request)
            .send()
            .await
            .map_err(reqwest::Error::without_url)?,
    )
    .await?;
    let id = status["id"]
        .as_str()
        .context("backfill API response omitted job ID")?
        .to_owned();
    let job = api_url(endpoint, Some(&id))?;
    let mut signals = ShutdownSignals::new()?;
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
        tokio::select! {
            () = tokio::time::sleep(Duration::from_millis(500)) => {},
            signal = signals.recv() => {
                signal?;
                let mut cancel = job.clone();
                cancel.path_segments_mut().map_err(|()| anyhow::anyhow!("invalid job URL"))?.push("cancel");
                response_json(client.post(cancel).send().await.map_err(reqwest::Error::without_url)?).await?;
                bail!("backfill cancelled");
            }
        }
        status = response_json(
            client
                .get(job.clone())
                .send()
                .await
                .map_err(reqwest::Error::without_url)?,
        )
        .await?;
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
