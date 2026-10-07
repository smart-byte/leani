//! Repeatable acquisition benchmark over a public, local, or loopback mirror.
//!
//! `cargo run --release -p leani-source-erae --example throughput -- \
//!   https://data.ethpandaops.io/erae/mainnet/ 19426589 19427612 full 4 4`
//! The final arguments are the material, active chunks, and buffered frames.

use std::{error::Error, sync::Arc, time::Instant};

use futures::{StreamExt, TryStreamExt, stream};
use leani_primitives::{
    BlockNumber, BlockRange, Capability, CapabilitySet, ChainId, Finality, LogFieldSet,
};
use leani_source_api::{
    DataRequest, FieldProjection, FilterSet, HistorySource, SourceBudget, VerificationPolicy,
    frame_fingerprint,
};
use leani_source_erae::{EraeConfig, EraeSource};
use tokio_util::sync::CancellationToken;

#[tokio::main]
#[allow(clippy::too_many_lines, clippy::cast_precision_loss)]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 6 {
        return Err("usage: throughput BASE_URL FROM_BLOCK TO_BLOCK headers|bodies|logs|full CHUNKS BUFFERED_FRAMES".into());
    }
    let range = BlockRange::new(BlockNumber(args[1].parse()?), BlockNumber(args[2].parse()?))?;
    let required = match args[3].as_str() {
        "headers" => CapabilitySet::of(Capability::Header),
        "bodies" => CapabilitySet::of(Capability::Header).with(Capability::Body),
        "logs" => CapabilitySet::of(Capability::Header).with(Capability::Logs),
        "full" => CapabilitySet::of(Capability::Header)
            .with(Capability::Transactions)
            .with(Capability::Receipts)
            .with(Capability::Logs)
            .with(Capability::Withdrawals),
        _ => return Err("material must be headers, bodies, logs, or full".into()),
    };
    let chunks = args[4].parse::<usize>()?;
    let buffered_frames = args[5].parse::<usize>()?;
    if chunks == 0 || buffered_frames == 0 {
        return Err("chunk and frame concurrency must be positive".into());
    }
    let mut config = EraeConfig::public_mainnet()?;
    config.base_url = args[0].parse()?;
    let source = Arc::new(EraeSource::new(config)?);
    let budget = SourceBudget {
        max_input_bytes: 1 << 30,
        max_frame_bytes: 32 << 20,
        max_frames: range.len(),
        max_buffered_frames: buffered_frames,
        max_in_flight_requests: chunks,
        temporary_disk_bytes: 1,
        max_resident_bytes: 128 << 20,
    };
    let cancellation = CancellationToken::new();
    let started = Instant::now();
    let plan = source
        .plan(&DataRequest {
            chain_id: ChainId(1),
            range,
            required,
            log_fields: LogFieldSet::NONE,
            allow_filtered: false,
            projection: FieldProjection::default(),
            filters: FilterSet::default(),
            minimum_finality: Finality::Finalized,
            verification_policy: VerificationPolicy::TrustedDataset,
        })
        .await?;
    let mut results = stream::iter(plan.chunks)
        .map(|chunk| {
            let source = Arc::clone(&source);
            let cancellation = cancellation.clone();
            async move {
                let mut frames = source.open(&chunk, budget, cancellation).await?;
                let mut fingerprint = blake3::Hasher::new();
                let mut count = 0_u64;
                let mut first_frame_seconds = None;
                let mut last = None;
                while let Some(frame) = frames.try_next().await? {
                    first_frame_seconds.get_or_insert_with(|| started.elapsed().as_secs_f64());
                    if let Some(previous) = last
                        && frame.block.parent_hash != previous
                    {
                        return Err("block parent continuity failed".into());
                    }
                    last = Some(frame.block.hash);
                    fingerprint.update(&frame_fingerprint(&frame, required)?.digest);
                    count += 1;
                }
                if count != chunk.range.len() {
                    return Err("source did not emit the complete chunk".into());
                }
                Ok::<_, Box<dyn Error>>((
                    chunk.range.start(),
                    count,
                    fingerprint.finalize(),
                    first_frame_seconds,
                ))
            }
        })
        .buffer_unordered(chunks)
        .try_collect::<Vec<_>>()
        .await?;
    let elapsed_seconds = started.elapsed().as_secs_f64();
    results.sort_unstable_by_key(|result| result.0);
    let mut fingerprint = blake3::Hasher::new();
    let mut frames = 0_u64;
    let mut first_frame_seconds = f64::INFINITY;
    for (_, count, digest, first) in results {
        fingerprint.update(digest.as_bytes());
        frames += count;
        first_frame_seconds = first_frame_seconds.min(first.unwrap_or(f64::INFINITY));
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "range": range,
            "material": args[3],
            "active_chunks": chunks,
            "buffered_frames": buffered_frames,
            "frames": frames,
            "elapsed_seconds": elapsed_seconds,
            "first_frame_seconds": first_frame_seconds,
            "blocks_per_second": frames as f64 / elapsed_seconds,
            "frame_digest": fingerprint.finalize().to_hex().to_string(),
            "source": source.acquisition_metrics(),
        }))?
    );
    Ok(())
}
