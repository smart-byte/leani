//! Streaming deterministic corpora for correctness and performance evidence.

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use alloy_primitives::{U256, keccak256};
use async_trait::async_trait;
use futures::{StreamExt, stream};
use leani_primitives::{
    Address, BlockFrame, BlockHash, BlockNumber, BlockRange, BlockRef, Capability, CapabilitySet,
    ChainId, Finality, HeaderEnvelope, Log, Material, MissingReason, Quantity, ReceiptEnvelope,
    SourceId, SourceKind, TransactionEnvelope, TransactionHash, TrustModel, VerificationReport,
};
use leani_source_api::{
    DataRequest, FinalityModel, HistorySource, Partitioning, SelectionPolicy, SourceBudget,
    SourceChunk, SourceDescriptor, SourceError, SourcePlan, VerificationPolicy, select_source,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

/// Stable synthetic material shape used by benchmark reports and fixtures.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SyntheticCorpusKind {
    Zero,
    Sparse,
    BlobsLike,
    UniswapLike,
    Dense,
}

impl SyntheticCorpusKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Zero => "zero",
            Self::Sparse => "sparse",
            Self::BlobsLike => "blobs_like",
            Self::UniswapLike => "uniswap_like",
            Self::Dense => "dense",
        }
    }
}

/// Immutable identity and expected result for one generated corpus.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SyntheticCorpusManifest {
    pub schema_version: u32,
    pub generator: String,
    pub kind: SyntheticCorpusKind,
    pub seed: u64,
    pub range: BlockRange,
    pub chunk_blocks: u64,
    pub expected_frames: u64,
    pub expected_transactions: u64,
    pub expected_blob_transactions: u64,
    pub expected_logs: u64,
    pub expected_processor_events: u64,
    pub expected_canonical_processor_output_bytes: u64,
    pub expected_frame_digest: String,
    pub expected_processor_output_digest: String,
    pub expected_processor_output_sha256: String,
    pub expected_query_output_digest: String,
}

/// Monotonic physical generation counters sampled by benchmark reports.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GeneratedSourceStats {
    pub frames: u64,
    pub estimated_bytes: u64,
    pub first_frame_elapsed_milliseconds: Option<u64>,
}

/// Constant-memory deterministic history source.
#[derive(Clone, Debug)]
pub struct GeneratedHistorySource {
    descriptor: SourceDescriptor,
    kind: SyntheticCorpusKind,
    seed: u64,
    chunk_blocks: u64,
    frames: Arc<AtomicU64>,
    estimated_bytes: Arc<AtomicU64>,
    measurement_started: Arc<Mutex<Option<Instant>>>,
    first_frame_elapsed_milliseconds: Arc<AtomicU64>,
}

impl GeneratedHistorySource {
    /// Construct a generated source plus its immutable expected manifest.
    ///
    /// # Errors
    ///
    /// Returns an error when the block or chunk count is zero or cannot form a
    /// valid range.
    pub fn new(
        kind: SyntheticCorpusKind,
        blocks: u64,
        seed: u64,
        chunk_blocks: u64,
    ) -> Result<(Self, SyntheticCorpusManifest), SourceError> {
        if blocks == 0 || chunk_blocks == 0 {
            return Err(SourceError::InvalidPlan(
                "synthetic corpus blocks and chunk size must be non-zero".to_owned(),
            ));
        }
        let range = BlockRange::new(BlockNumber(1), BlockNumber(blocks))
            .map_err(|error| SourceError::InvalidPlan(error.to_string()))?;
        let descriptor = SourceDescriptor {
            id: SourceId::new(format!("benchmark-{}", kind.as_str()))
                .map_err(|error| SourceError::InvalidPlan(error.to_string()))?,
            kind: SourceKind::Synthetic,
            chain_id: ChainId(1),
            range: Some(range),
            capabilities: CapabilitySet::from_iter([
                Capability::Header,
                Capability::Transactions,
                Capability::Receipts,
                Capability::Logs,
            ]),
            complete_capabilities: CapabilitySet::from_iter([
                Capability::Header,
                Capability::Transactions,
                Capability::Receipts,
                Capability::Logs,
            ]),
            trust: TrustModel::ProtocolVerified,
            finality: FinalityModel::Finalized,
            partitioning: Partitioning::FixedBlockSpan(chunk_blocks),
            expected_lag: Duration::ZERO,
            schema_version: "synthetic-benchmark-v1".to_owned(),
            priority: 0,
        };
        let manifest = build_manifest(kind, seed, range, chunk_blocks);
        Ok((
            Self {
                descriptor,
                kind,
                seed,
                chunk_blocks,
                frames: Arc::new(AtomicU64::new(0)),
                estimated_bytes: Arc::new(AtomicU64::new(0)),
                measurement_started: Arc::new(Mutex::new(None)),
                first_frame_elapsed_milliseconds: Arc::new(AtomicU64::new(u64::MAX)),
            },
            manifest,
        ))
    }

    /// Start the exact source-latency clock used by benchmark reports.
    ///
    /// The generated source is one-shot in benchmark runs. Calling this before
    /// opening any chunk keeps first-frame latency independent of the periodic
    /// resource sampler.
    #[must_use]
    pub fn start_measurement(&self) -> Instant {
        let started = Instant::now();
        if let Ok(mut measurement_started) = self.measurement_started.lock() {
            *measurement_started = Some(started);
        }
        self.first_frame_elapsed_milliseconds
            .store(u64::MAX, Ordering::Relaxed);
        started
    }

    #[must_use]
    pub fn stats(&self) -> GeneratedSourceStats {
        let first_frame_elapsed_milliseconds = self
            .first_frame_elapsed_milliseconds
            .load(Ordering::Relaxed);
        GeneratedSourceStats {
            frames: self.frames.load(Ordering::Relaxed),
            estimated_bytes: self.estimated_bytes.load(Ordering::Relaxed),
            first_frame_elapsed_milliseconds: (first_frame_elapsed_milliseconds != u64::MAX)
                .then_some(first_frame_elapsed_milliseconds),
        }
    }

    #[must_use]
    pub fn frame(&self, number: BlockNumber) -> BlockFrame {
        generated_frame(self.kind, self.seed, number)
    }
}

/// Build the immutable expected manifest for an explicit synthetic range.
///
/// This is useful when a benchmark drives material outside the history
/// source's advertised range, such as a concurrent live tail.
#[must_use]
pub fn synthetic_corpus_manifest(
    kind: SyntheticCorpusKind,
    seed: u64,
    range: BlockRange,
    chunk_blocks: u64,
) -> SyntheticCorpusManifest {
    build_manifest(kind, seed, range, chunk_blocks)
}

#[async_trait]
impl HistorySource for GeneratedHistorySource {
    fn descriptor(&self) -> &SourceDescriptor {
        &self.descriptor
    }

    async fn plan(&self, request: &DataRequest) -> Result<SourcePlan, SourceError> {
        let minimum_trust = match request.verification_policy {
            VerificationPolicy::CompleteCryptographic => TrustModel::ProtocolVerified,
            VerificationPolicy::TrustedDataset => TrustModel::TrustedDataset,
            VerificationPolicy::BestEffort => TrustModel::Untrusted,
        };
        select_source(
            std::slice::from_ref(&self.descriptor),
            request,
            SelectionPolicy {
                minimum_trust,
                prefer_complete: !request.allow_filtered,
            },
        )
        .map_err(|error| SourceError::InvalidPlan(error.to_string()))?;

        let available = self
            .descriptor
            .range
            .ok_or_else(|| SourceError::InvalidPlan("generated source has no range".to_owned()))?;
        if request.range.start() < available.start() || request.range.end() > available.end() {
            return Err(SourceError::MissingRange(request.range));
        }
        let mut chunks = Vec::new();
        let mut start = request.range.start().0;
        let mut ordinal = 0_u64;
        while start <= request.range.end().0 {
            let end = start
                .saturating_add(self.chunk_blocks.saturating_sub(1))
                .min(request.range.end().0);
            let range = BlockRange::new(BlockNumber(start), BlockNumber(end))
                .map_err(|error| SourceError::InvalidPlan(error.to_string()))?;
            chunks.push(SourceChunk {
                source_id: self.descriptor.id.clone(),
                range,
                partition: ordinal.to_be_bytes().to_vec(),
                schema_version: self.descriptor.schema_version.clone(),
                expected_parent: Some(parent_hash(self.seed, self.kind, BlockNumber(start))),
                estimated_bytes: None,
            });
            ordinal = ordinal.saturating_add(1);
            start = end.saturating_add(1);
        }
        let plan = SourcePlan {
            source_id: self.descriptor.id.clone(),
            request: request.clone(),
            chunks,
            estimated_bytes: None,
            estimated_lag: Duration::ZERO,
            supplied: self.descriptor.capabilities,
            complete: self.descriptor.complete_capabilities,
            trust: self.descriptor.trust,
            schema_version: self.descriptor.schema_version.clone(),
            physical_plan: Vec::new(),
        };
        plan.validate()?;
        Ok(plan)
    }

    async fn open(
        &self,
        chunk: &SourceChunk,
        budget: SourceBudget,
        cancellation: CancellationToken,
    ) -> Result<leani_source_api::BlockFrameStream, SourceError> {
        let budget = budget.validate()?;
        if chunk.source_id != self.descriptor.id
            || chunk.schema_version != self.descriptor.schema_version
        {
            return Err(SourceError::InvalidPlan(
                "generated chunk identity does not match its source".to_owned(),
            ));
        }
        let available = self
            .descriptor
            .range
            .ok_or_else(|| SourceError::InvalidPlan("generated source has no range".to_owned()))?;
        if chunk.range.start() < available.start() || chunk.range.end() > available.end() {
            return Err(SourceError::MissingRange(chunk.range));
        }
        let state = GeneratedStreamState {
            source: self.clone(),
            next: chunk.range.start().0,
            end: chunk.range.end().0,
            budget,
            frames: 0,
            bytes: 0,
            cancellation,
            terminal: false,
        };
        Ok(stream::unfold(state, next_generated_frame).boxed())
    }
}

#[derive(Debug)]
struct GeneratedStreamState {
    source: GeneratedHistorySource,
    next: u64,
    end: u64,
    budget: SourceBudget,
    frames: u64,
    bytes: u64,
    cancellation: CancellationToken,
    terminal: bool,
}

async fn next_generated_frame(
    mut state: GeneratedStreamState,
) -> Option<(Result<BlockFrame, SourceError>, GeneratedStreamState)> {
    if state.terminal || state.next > state.end {
        return None;
    }
    if state.cancellation.is_cancelled() {
        state.terminal = true;
        return Some((Err(SourceError::Cancelled), state));
    }
    let frame = state.source.frame(BlockNumber(state.next));
    let frame_bytes = frame.estimated_heap_bytes();
    state.frames = state.frames.saturating_add(1);
    state.bytes = state.bytes.saturating_add(frame_bytes);
    if frame_bytes > state.budget.max_frame_bytes {
        state.terminal = true;
        return Some((
            Err(SourceError::BudgetExceeded {
                resource: "frame_bytes",
                limit: state.budget.max_frame_bytes,
                observed: frame_bytes,
            }),
            state,
        ));
    }
    if state.frames > state.budget.max_frames {
        state.terminal = true;
        return Some((
            Err(SourceError::BudgetExceeded {
                resource: "frames",
                limit: state.budget.max_frames,
                observed: state.frames,
            }),
            state,
        ));
    }
    if state.bytes > state.budget.max_input_bytes {
        state.terminal = true;
        return Some((
            Err(SourceError::BudgetExceeded {
                resource: "input_bytes",
                limit: state.budget.max_input_bytes,
                observed: state.bytes,
            }),
            state,
        ));
    }
    let previous_frames = state.source.frames.fetch_add(1, Ordering::Relaxed);
    if previous_frames == 0
        && let Ok(measurement_started) = state.source.measurement_started.lock()
        && let Some(started) = *measurement_started
    {
        state.source.first_frame_elapsed_milliseconds.store(
            u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX - 1),
            Ordering::Relaxed,
        );
    }
    state
        .source
        .estimated_bytes
        .fetch_add(frame_bytes, Ordering::Relaxed);
    state.next = state.next.saturating_add(1);
    Some((Ok(frame), state))
}

fn build_manifest(
    kind: SyntheticCorpusKind,
    seed: u64,
    range: BlockRange,
    chunk_blocks: u64,
) -> SyntheticCorpusManifest {
    let mut frame_hasher = blake3::Hasher::new();
    let mut output_hasher = blake3::Hasher::new();
    let mut output_sha256 = Sha256::new();
    let mut query_events = Vec::new();
    let mut transactions = 0_u64;
    let mut blob_transactions = 0_u64;
    let mut logs = 0_u64;
    let mut processor_events = 0_u64;
    let mut canonical_processor_output_bytes = 0_u64;
    for number in range.iter() {
        let count_usize = transaction_count(kind, number.0);
        let count = u64::try_from(count_usize).unwrap_or(u64::MAX);
        let block_logs =
            update_expected_frame_digest(&mut frame_hasher, kind, seed, number, count_usize);
        transactions = transactions.saturating_add(count);
        if is_blob_corpus(kind) {
            blob_transactions = blob_transactions.saturating_add(count);
        }
        logs = logs.saturating_add(block_logs);
        let block = BlockRef {
            number,
            hash: deterministic_hash(b"block", seed, kind, number.0, 0),
            parent_hash: parent_hash(seed, kind, number),
            timestamp: 1_700_000_000_u64.saturating_add(number.0.saturating_mul(12)),
        };
        let events = expected_processor_events(kind, seed, block, count_usize);
        processor_events =
            processor_events.saturating_add(u64::try_from(events.len()).unwrap_or(u64::MAX));
        for event in events {
            canonical_processor_output_bytes = canonical_processor_output_bytes
                .saturating_add(u64::try_from(event.len()).unwrap_or(u64::MAX));
            if kind == SyntheticCorpusKind::UniswapLike {
                query_events.push(event.clone());
            }
            update_length_prefixed(&mut output_hasher, &event);
            update_sha256_length_prefixed(&mut output_sha256, &event);
        }
    }
    let expected_query_output_digest = if query_events.is_empty() {
        output_hasher.clone().finalize().to_string()
    } else {
        query_events.sort_unstable();
        let mut hasher = blake3::Hasher::new();
        for event in query_events {
            update_length_prefixed(&mut hasher, &event);
        }
        hasher.finalize().to_string()
    };
    SyntheticCorpusManifest {
        schema_version: 4,
        generator: "leani-testkit.synthetic-benchmark.v4".to_owned(),
        kind,
        seed,
        range,
        chunk_blocks,
        expected_frames: range.len(),
        expected_transactions: transactions,
        expected_blob_transactions: blob_transactions,
        expected_logs: logs,
        expected_processor_events: processor_events,
        expected_canonical_processor_output_bytes: canonical_processor_output_bytes,
        expected_frame_digest: frame_hasher.finalize().to_string(),
        expected_processor_output_digest: output_hasher.finalize().to_string(),
        expected_processor_output_sha256: hex::encode(output_sha256.finalize()),
        expected_query_output_digest,
    }
}

fn expected_processor_events(
    kind: SyntheticCorpusKind,
    seed: u64,
    block: BlockRef,
    transaction_count: usize,
) -> Vec<Vec<u8>> {
    match kind {
        SyntheticCorpusKind::BlobsLike => {
            vec![canonical_blobs_event(kind, seed, block, transaction_count)]
        }
        SyntheticCorpusKind::UniswapLike => (0..transaction_count)
            .map(|index| {
                canonical_uniswap_event(seed, block, u32::try_from(index).unwrap_or(u32::MAX))
            })
            .collect(),
        SyntheticCorpusKind::Zero | SyntheticCorpusKind::Sparse | SyntheticCorpusKind::Dense => {
            let mut event = Vec::with_capacity(1 + 80 + 8);
            event.push(0);
            event.extend_from_slice(&block.canonical_key(ChainId(1)).encode_ordered());
            event.extend_from_slice(
                &u64::try_from(transaction_count)
                    .unwrap_or(u64::MAX)
                    .to_be_bytes(),
            );
            vec![event]
        }
    }
}

fn canonical_blobs_event(
    kind: SyntheticCorpusKind,
    seed: u64,
    block: BlockRef,
    transaction_count: usize,
) -> Vec<u8> {
    let mut transactions = (0..transaction_count)
        .map(|index| {
            let index = u32::try_from(index).unwrap_or(u32::MAX);
            let hash = deterministic_hash(b"transaction", seed, kind, block.number.0, index);
            let blobs = (0..=usize::try_from((block.number.0 + u64::from(index)) % 3).unwrap_or(0))
                .map(|blob_index| {
                    deterministic_hash(
                        b"blob",
                        seed,
                        kind,
                        block.number.0,
                        index
                            .saturating_mul(4)
                            .saturating_add(u32::try_from(blob_index).unwrap_or(u32::MAX)),
                    )
                })
                .collect::<Vec<_>>();
            (hash, blobs)
        })
        .collect::<Vec<_>>();
    transactions.sort_unstable_by_key(|(hash, _)| *hash);
    let mut event = Vec::new();
    event.push(1);
    event.extend_from_slice(&block.number.0.to_be_bytes());
    event.extend_from_slice(&block.hash.0);
    event.extend_from_slice(
        &u32::try_from(transactions.len())
            .unwrap_or(u32::MAX)
            .to_be_bytes(),
    );
    for (hash, blobs) in transactions {
        event.extend_from_slice(&hash.0);
        event.extend_from_slice(&u32::try_from(blobs.len()).unwrap_or(u32::MAX).to_be_bytes());
        for blob in blobs {
            event.extend_from_slice(&blob.0);
        }
    }
    event
}

fn canonical_uniswap_event(seed: u64, block: BlockRef, log_index: u32) -> Vec<u8> {
    let pool = uniswap_weth_usdc_pool();
    let mut event = Vec::with_capacity(1 + 56 + 32);
    event.push(2);
    event.extend_from_slice(&pool.0);
    event.extend_from_slice(&block.hash.0);
    event.extend_from_slice(&log_index.to_be_bytes());
    let sqrt_price = U256::from(1_u64 << 32)
        .saturating_mul(U256::from(block.number.0.saturating_add(seed).max(1)));
    event.extend_from_slice(&sqrt_price.to_be_bytes::<32>());
    event
}

fn update_expected_frame_digest(
    hasher: &mut blake3::Hasher,
    kind: SyntheticCorpusKind,
    seed: u64,
    number: BlockNumber,
    transaction_count: usize,
) -> u64 {
    let hash = deterministic_hash(b"block", seed, kind, number.0, 0);
    let parent = parent_hash(seed, kind, number);
    hasher.update(&number.0.to_be_bytes());
    hasher.update(&hash.0);
    hasher.update(&parent.0);
    hasher.update(
        &1_700_000_000_u64
            .saturating_add(number.0.saturating_mul(12))
            .to_be_bytes(),
    );
    hasher.update(
        &u64::try_from(transaction_count)
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    let blob = is_blob_corpus(kind);
    for index in 0..transaction_count {
        let index_u32 = u32::try_from(index).unwrap_or(u32::MAX);
        let transaction_hash = deterministic_hash(b"transaction", seed, kind, number.0, index_u32);
        hasher.update(&transaction_hash.0);
        hasher.update(&[if blob { 3 } else { 2 }]);
        hasher.update(&index_u32.to_be_bytes());
        if blob {
            let blob_hash_count =
                1 + usize::try_from((number.0 + u64::from(index_u32)) % 3).unwrap_or(0);
            for blob_index in 0..blob_hash_count {
                let blob_hash = deterministic_hash(
                    b"blob",
                    seed,
                    kind,
                    number.0,
                    index_u32
                        .saturating_mul(4)
                        .saturating_add(u32::try_from(blob_index).unwrap_or(u32::MAX)),
                );
                hasher.update(&blob_hash.0);
            }
        }
    }
    let log_count = match kind {
        SyntheticCorpusKind::Sparse | SyntheticCorpusKind::UniswapLike => {
            u64::try_from(transaction_count).unwrap_or(u64::MAX)
        }
        SyntheticCorpusKind::Dense => {
            u64::try_from(transaction_count.div_ceil(2)).unwrap_or(u64::MAX)
        }
        SyntheticCorpusKind::Zero | SyntheticCorpusKind::BlobsLike => 0,
    };
    hasher.update(&log_count.to_be_bytes());
    let mut log_index = 0_u32;
    for index in 0..transaction_count {
        let include_log = match kind {
            SyntheticCorpusKind::Sparse | SyntheticCorpusKind::UniswapLike => true,
            SyntheticCorpusKind::Dense => index % 2 == 0,
            SyntheticCorpusKind::Zero | SyntheticCorpusKind::BlobsLike => false,
        };
        if !include_log {
            continue;
        }
        let index_u32 = u32::try_from(index).unwrap_or(u32::MAX);
        let transaction_hash = deterministic_hash(b"transaction", seed, kind, number.0, index_u32);
        hasher.update(&transaction_hash.0);
        hasher.update(&log_index.to_be_bytes());
        let (address, topics, data) = generated_log(kind, seed, number, index_u32);
        hasher.update(&address.0);
        hasher.update(
            &u64::try_from(topics.len())
                .unwrap_or(u64::MAX)
                .to_be_bytes(),
        );
        for topic in topics {
            hasher.update(&topic);
        }
        update_length_prefixed(hasher, &data);
        log_index = log_index.saturating_add(1);
    }
    log_count
}

/// Add one normalized frame to the stable synthetic-corpus digest.
pub fn update_frame_digest(hasher: &mut blake3::Hasher, frame: &BlockFrame) {
    hasher.update(&frame.block.number.0.to_be_bytes());
    hasher.update(&frame.block.hash.0);
    hasher.update(&frame.block.parent_hash.0);
    hasher.update(&frame.block.timestamp.to_be_bytes());
    if let Some(transactions) = frame.transactions.as_complete() {
        hasher.update(
            &u64::try_from(transactions.len())
                .unwrap_or(u64::MAX)
                .to_be_bytes(),
        );
        for transaction in transactions {
            hasher.update(&transaction.hash.0);
            hasher.update(&[transaction.transaction_type]);
            hasher.update(&transaction.index.to_be_bytes());
            for hash in &transaction.blob_versioned_hashes {
                hasher.update(&hash.0);
            }
        }
    }
    if let Some(logs) = frame.logs.as_complete() {
        hasher.update(&u64::try_from(logs.len()).unwrap_or(u64::MAX).to_be_bytes());
        for log in logs {
            if let Some(transaction_hash) = log.transaction_hash {
                hasher.update(&transaction_hash.0);
            }
            hasher.update(&log.log_index.to_be_bytes());
            hasher.update(&log.address.0);
            hasher.update(
                &u64::try_from(log.topics.len())
                    .unwrap_or(u64::MAX)
                    .to_be_bytes(),
            );
            for topic in &log.topics {
                hasher.update(topic);
            }
            update_length_prefixed(hasher, &log.data);
        }
    }
}

#[allow(clippy::too_many_lines)]
fn generated_frame(kind: SyntheticCorpusKind, seed: u64, number: BlockNumber) -> BlockFrame {
    let block_hash = deterministic_hash(b"block", seed, kind, number.0, 0);
    let block = BlockRef {
        number,
        hash: block_hash,
        parent_hash: parent_hash(seed, kind, number),
        timestamp: 1_700_000_000_u64.saturating_add(number.0.saturating_mul(12)),
    };
    let transaction_count = transaction_count(kind, number.0);
    let mut transactions = Vec::with_capacity(transaction_count);
    let mut receipts = Vec::with_capacity(transaction_count);
    let mut block_logs = Vec::new();
    for index in 0..transaction_count {
        let index_u32 = u32::try_from(index).unwrap_or(u32::MAX);
        let hash = TransactionHash::new(
            deterministic_hash(b"transaction", seed, kind, number.0, index_u32).0,
        );
        let blob = is_blob_corpus(kind);
        let payload_bytes = match kind {
            SyntheticCorpusKind::Zero => 0,
            SyntheticCorpusKind::Sparse => 128,
            SyntheticCorpusKind::BlobsLike => 256 + index.saturating_mul(16),
            SyntheticCorpusKind::UniswapLike => 256,
            SyntheticCorpusKind::Dense => 4_096,
        };
        let blob_hash_count = if blob {
            1 + usize::try_from((number.0 + u64::from(index_u32)) % 3).unwrap_or(0)
        } else {
            0
        };
        let blob_versioned_hashes = (0..blob_hash_count)
            .map(|blob_index| {
                deterministic_hash(
                    b"blob",
                    seed,
                    kind,
                    number.0,
                    index_u32
                        .saturating_mul(4)
                        .saturating_add(u32::try_from(blob_index).unwrap_or(u32::MAX)),
                )
            })
            .collect::<Vec<_>>();
        transactions.push(TransactionEnvelope {
            hash,
            transaction_type: if blob { 3 } else { 2 },
            index: index_u32,
            encoded: Some(deterministic_bytes(
                seed,
                number.0,
                index_u32,
                payload_bytes,
            )),
            from: Some(Address::new([0x11; 20])),
            to: Some(Address::new([0x22; 20])),
            nonce: Some(number.0.saturating_add(u64::from(index_u32))),
            gas_limit: Some(21_000),
            value: Some(quantity(1)),
            input: Some(deterministic_bytes(
                seed ^ 0xa5a5_a5a5_a5a5_a5a5,
                number.0,
                index_u32,
                payload_bytes / 2,
            )),
            max_fee_per_gas: Some(quantity(20_000_000_000)),
            max_priority_fee_per_gas: Some(quantity(1_000_000_000)),
            max_fee_per_blob_gas: blob.then(|| quantity(1)),
            blob_versioned_hashes,
            size_bytes: u32::try_from(payload_bytes).ok(),
        });
        let include_log = match kind {
            SyntheticCorpusKind::Sparse | SyntheticCorpusKind::UniswapLike => true,
            SyntheticCorpusKind::Dense => index % 2 == 0,
            SyntheticCorpusKind::Zero | SyntheticCorpusKind::BlobsLike => false,
        };
        let logs = if include_log {
            let (address, topics, data) = generated_log(kind, seed, number, index_u32);
            vec![Log {
                address,
                topics,
                data,
                transaction_hash: Some(hash),
                transaction_index: index_u32,
                log_index: u32::try_from(block_logs.len()).unwrap_or(u32::MAX),
            }]
        } else {
            Vec::new()
        };
        block_logs.extend(logs.iter().cloned());
        receipts.push(ReceiptEnvelope {
            transaction_hash: hash,
            transaction_type: if blob { 3 } else { 2 },
            transaction_index: index_u32,
            encoded: Some(deterministic_bytes(seed ^ 0x33, number.0, index_u32, 128)),
            success: Some(true),
            gas_used: Some(21_000),
            effective_gas_price: Some(quantity(2_000_000_000)),
            blob_gas_used: blob.then_some(
                131_072_u64.saturating_mul(u64::try_from(blob_hash_count).unwrap_or(u64::MAX)),
            ),
            blob_gas_price: blob.then(|| quantity(1)),
            logs,
        });
    }
    BlockFrame {
        chain_id: ChainId(1),
        block,
        finality: Finality::Finalized,
        header: Material::Complete(HeaderEnvelope {
            rlp: Some(deterministic_bytes(seed ^ 0x77, number.0, 0, 512)),
            transactions_root: Some(deterministic_hash(b"tx-root", seed, kind, number.0, 0)),
            receipts_root: Some(deterministic_hash(b"receipt-root", seed, kind, number.0, 0)),
            withdrawals_root: None,
            gas_limit: Some(30_000_000),
            gas_used: Some(
                21_000_u64.saturating_mul(u64::try_from(transaction_count).unwrap_or(u64::MAX)),
            ),
            base_fee_per_gas: Some(quantity(10_000_000_000)),
            blob_gas_used: is_blob_corpus(kind).then_some(
                transactions
                    .iter()
                    .map(|transaction| {
                        131_072_u64.saturating_mul(
                            u64::try_from(transaction.blob_versioned_hashes.len())
                                .unwrap_or(u64::MAX),
                        )
                    })
                    .fold(0_u64, u64::saturating_add),
            ),
            excess_blob_gas: Some(0),
            size_bytes: Some(1_024),
            transaction_count: u32::try_from(transaction_count).ok(),
            consensus_size_bytes: None,
        }),
        transactions: Material::Complete(transactions),
        receipts: Material::Complete(receipts),
        logs: Material::Complete(block_logs),
        withdrawals: Material::Missing(MissingReason::NotRequested),
        blob_sidecars: Material::Missing(MissingReason::NotRequested),
        traces: Material::Missing(MissingReason::Unsupported),
        state_diffs: Material::Missing(MissingReason::Unsupported),
        provenance: Vec::new(),
        verification: VerificationReport::default(),
    }
}

const fn transaction_count(kind: SyntheticCorpusKind, number: u64) -> usize {
    match kind {
        SyntheticCorpusKind::Zero => 0,
        SyntheticCorpusKind::Sparse => {
            if number.is_multiple_of(997) {
                1
            } else {
                0
            }
        }
        SyntheticCorpusKind::BlobsLike => match number % 20 {
            0 => 4,
            1 | 5 => 2,
            7 | 11 | 13 => 1,
            _ => 0,
        },
        SyntheticCorpusKind::UniswapLike => {
            if number.is_multiple_of(4) || number % 97 == 1 {
                1
            } else {
                0
            }
        }
        SyntheticCorpusKind::Dense => 32,
    }
}

const fn is_blob_corpus(kind: SyntheticCorpusKind) -> bool {
    matches!(
        kind,
        SyntheticCorpusKind::BlobsLike | SyntheticCorpusKind::Dense
    )
}

/// Uniswap V3 USDC/WETH 0.05% pool used by the reference benchmark.
#[must_use]
pub const fn uniswap_weth_usdc_pool() -> Address {
    Address::new([
        0x88, 0xe6, 0xa0, 0xc2, 0xdd, 0xd2, 0x6f, 0xee, 0xb6, 0x4f, 0x03, 0x9a, 0x2c, 0x41, 0x29,
        0x6f, 0xcb, 0x3f, 0x56, 0x40,
    ])
}

fn generated_log(
    kind: SyntheticCorpusKind,
    seed: u64,
    number: BlockNumber,
    transaction_index: u32,
) -> (Address, Vec<[u8; 32]>, Vec<u8>) {
    if kind == SyntheticCorpusKind::UniswapLike {
        let sqrt_price = U256::from(1_u64 << 32)
            .saturating_mul(U256::from(number.0.saturating_add(seed).max(1)));
        let mut data = vec![0_u8; 160];
        data[64..96].copy_from_slice(&sqrt_price.to_be_bytes::<32>());
        return (
            uniswap_weth_usdc_pool(),
            vec![keccak256("Swap(address,address,int256,int256,uint160,uint128,int24)").0],
            data,
        );
    }
    (
        Address::new([0x88; 20]),
        vec![[0x55; 32]],
        deterministic_bytes(seed ^ 0x55, number.0, transaction_index, 96),
    )
}

fn parent_hash(seed: u64, kind: SyntheticCorpusKind, number: BlockNumber) -> BlockHash {
    if number.0 <= 1 {
        BlockHash::ZERO
    } else {
        deterministic_hash(b"block", seed, kind, number.0 - 1, 0)
    }
}

fn deterministic_hash(
    domain: &[u8],
    seed: u64,
    kind: SyntheticCorpusKind,
    number: u64,
    index: u32,
) -> BlockHash {
    let mut hasher = blake3::Hasher::new();
    hasher.update(domain);
    hasher.update(&seed.to_be_bytes());
    hasher.update(kind.as_str().as_bytes());
    hasher.update(&number.to_be_bytes());
    hasher.update(&index.to_be_bytes());
    BlockHash::new(*hasher.finalize().as_bytes())
}

fn deterministic_bytes(seed: u64, number: u64, index: u32, len: usize) -> Vec<u8> {
    let mut output = Vec::with_capacity(len);
    let mut counter = 0_u32;
    while output.len() < len {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&seed.to_be_bytes());
        hasher.update(&number.to_be_bytes());
        hasher.update(&index.to_be_bytes());
        hasher.update(&counter.to_be_bytes());
        output.extend_from_slice(hasher.finalize().as_bytes());
        counter = counter.saturating_add(1);
    }
    output.truncate(len);
    output
}

fn quantity(value: u64) -> Quantity {
    let mut bytes = [0_u8; 32];
    bytes[24..].copy_from_slice(&value.to_be_bytes());
    Quantity::new(bytes)
}

fn update_length_prefixed(hasher: &mut blake3::Hasher, bytes: &[u8]) {
    hasher.update(&u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_be_bytes());
    hasher.update(bytes);
}

fn update_sha256_length_prefixed(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update(u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_be_bytes());
    hasher.update(bytes);
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use leani_source_api::{DataRequest, FieldProjection, FilterSet};

    use super::*;

    fn request(range: BlockRange) -> DataRequest {
        DataRequest {
            chain_id: ChainId(1),
            range,
            required: CapabilitySet::from_iter([
                Capability::Header,
                Capability::Transactions,
                Capability::Receipts,
                Capability::Logs,
            ]),
            allow_filtered: false,
            projection: FieldProjection::default(),
            log_fields: leani_primitives::LogFieldSet::ALL,
            filters: FilterSet::default(),
            minimum_finality: Finality::Finalized,
            verification_policy: VerificationPolicy::CompleteCryptographic,
        }
    }

    async fn assert_corpus_matches_manifest(kind: SyntheticCorpusKind) {
        let (source, manifest) = GeneratedHistorySource::new(kind, 10_000, 7, 512).expect("corpus");
        let plan = source.plan(&request(manifest.range)).await.expect("plan");
        assert_eq!(plan.chunks.len(), 20);
        let mut digest = blake3::Hasher::new();
        let mut frames = 0_u64;
        for chunk in &plan.chunks {
            let mut stream = source
                .open(
                    chunk,
                    SourceBudget {
                        max_input_bytes: 64 * 1024 * 1024,
                        max_frame_bytes: 1024 * 1024,
                        max_frames: chunk.range.len(),
                        max_buffered_frames: 8,
                        max_in_flight_requests: 1,
                        temporary_disk_bytes: 0,
                    },
                    CancellationToken::new(),
                )
                .await
                .expect("open");
            while let Some(frame) = stream.next().await {
                let frame = frame.expect("frame");
                update_frame_digest(&mut digest, &frame);
                frames = frames.saturating_add(1);
            }
        }
        assert_eq!(frames, manifest.expected_frames);
        assert_eq!(
            digest.finalize().to_string(),
            manifest.expected_frame_digest
        );
        assert_eq!(source.stats().frames, 10_000);
    }

    #[tokio::test]
    async fn ci_corpora_stream_in_constant_size_chunks_and_match_manifests() {
        for kind in [
            SyntheticCorpusKind::Zero,
            SyntheticCorpusKind::Sparse,
            SyntheticCorpusKind::BlobsLike,
            SyntheticCorpusKind::UniswapLike,
        ] {
            assert_corpus_matches_manifest(kind).await;
        }
    }

    #[test]
    fn corpus_shapes_are_stable_and_materially_distinct() {
        let (_, zero) =
            GeneratedHistorySource::new(SyntheticCorpusKind::Zero, 10_000, 1, 128).expect("zero");
        let (_, sparse) = GeneratedHistorySource::new(SyntheticCorpusKind::Sparse, 10_000, 1, 128)
            .expect("sparse");
        let (_, blobs) =
            GeneratedHistorySource::new(SyntheticCorpusKind::BlobsLike, 10_000, 1, 128)
                .expect("blobs");
        let (_, uniswap) =
            GeneratedHistorySource::new(SyntheticCorpusKind::UniswapLike, 10_000, 1, 128)
                .expect("uniswap");
        let (_, dense) =
            GeneratedHistorySource::new(SyntheticCorpusKind::Dense, 10_000, 1, 128).expect("dense");
        assert_eq!(zero.expected_transactions, 0);
        assert!(sparse.expected_transactions > 0);
        assert!(sparse.expected_logs > 0);
        assert!(blobs.expected_blob_transactions > sparse.expected_blob_transactions);
        assert!(uniswap.expected_logs > sparse.expected_logs);
        assert_eq!(uniswap.expected_blob_transactions, 0);
        assert!(dense.expected_transactions > blobs.expected_transactions);
        assert_ne!(zero.expected_frame_digest, dense.expected_frame_digest);
    }
}
