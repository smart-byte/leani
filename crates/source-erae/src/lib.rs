//! Sparse, bounded reader for execution-history `eraE`/ERE archives.
//!
//! The source reads the e2store dynamic index first and issues HTTP range
//! requests only for the block components required by a request. Archive bytes
//! are never retained after the returned stream is consumed.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    io::Read,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use alloy_consensus::{
    Block, EthereumReceipt, Header, Transaction as _, TxReceipt as _,
    proofs::{calculate_receipt_root, calculate_transaction_root},
    transaction::SignerRecoverable,
};
use alloy_eips::Encodable2718;
use alloy_primitives::{Address as AlloyAddress, U256};
use async_trait::async_trait;
use futures::{StreamExt, stream};
use leani_primitives::{
    Address, BlockFrame, BlockHash, BlockNumber, BlockRange, BlockRef, Capability, CapabilitySet,
    ChainId, CheckStatus, Finality, HeaderEnvelope, Log, Material, MissingReason, ObjectIdentity,
    Provenance, Quantity, ReceiptEnvelope, SourceId, SourceKind, TransactionEnvelope,
    TransactionHash, TrustModel, VerificationCheck, VerificationReport, Withdrawal,
};
use leani_source_api::{
    BlockFrameStream, DataRequest, FieldProjection, FilterSet, FinalityModel, HistorySource,
    Partitioning, SourceAcquisitionMetrics, SourceBudget, SourceChunk, SourceDescriptor,
    SourceError, SourcePlan, VerificationPolicy,
};
use reqwest::{
    Client, StatusCode,
    header::{ACCEPT, ACCEPT_ENCODING, CONTENT_LENGTH, RANGE},
};
use reth_era::{
    e2s::types::{Entry, VERSION},
    ere::types::{
        execution::{
            BlockTuple, COMPRESSED_BODY, COMPRESSED_HEADER, COMPRESSED_SLIM_RECEIPTS,
            CompressedBody, CompressedHeader, CompressedSlimReceipts, SlimReceipt,
        },
        group::{DYNAMIC_BLOCK_INDEX, DynamicBlockIndex},
    },
};
use reth_ethereum_primitives::BlockBody;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use url::Url;

const BLOCKS_PER_FILE: u64 = 8_192;
const MAX_CATALOG_BYTES: u64 = 16 * 1_024 * 1_024;
const MAX_CATALOG_OBJECTS: usize = 16_384;
const RETH_REVISION: &str = "8eb210175687c9f0c889a3b6795c16781d830e3a";

/// Construction settings for one archive mirror.
#[derive(Clone, Debug)]
pub struct EraeConfig {
    pub id: SourceId,
    pub chain_id: ChainId,
    pub network: String,
    pub base_url: Url,
    pub priority: u16,
    pub request_timeout: Duration,
}

impl EraeConfig {
    /// The public ethPandaOps mainnet mirror.
    ///
    /// # Errors
    ///
    /// Returns an error only if a compile-time URL or source ID is invalid.
    pub fn public_mainnet() -> Result<Self, EraeError> {
        Ok(Self {
            id: SourceId::new("erae-mainnet")
                .map_err(|error| EraeError::InvalidConfig(error.to_string()))?,
            chain_id: ChainId(1),
            network: "mainnet".to_owned(),
            base_url: Url::parse("https://data.ethpandaops.io/erae/mainnet/")
                .map_err(EraeError::Url)?,
            priority: 20,
            request_timeout: Duration::from_secs(30),
        })
    }

    fn validate(&self) -> Result<(), EraeError> {
        if self.chain_id.0 == 0
            || self.network.trim().is_empty()
            || self.request_timeout.is_zero()
            || !matches!(self.base_url.scheme(), "http" | "https" | "file")
        {
            return Err(EraeError::InvalidConfig(
                "chain, network, http(s)/file base URL, and timeout are required".to_owned(),
            ));
        }
        if !self.base_url.path().ends_with('/') {
            return Err(EraeError::InvalidConfig(
                "eraE base URL must end with `/`".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct CatalogObject {
    filename: String,
    locator: Url,
    checksum: [u8; 32],
    advertised_range: BlockRange,
    short_last_hash: [u8; 4],
}

#[derive(Debug)]
struct ObjectIndex {
    index_position: u64,
    index: DynamicBlockIndex,
    component_types: Vec<[u8; 2]>,
    initial_input_bytes: u64,
}

#[derive(Clone, Debug)]
pub struct EraeSource {
    config: EraeConfig,
    descriptor: SourceDescriptor,
    client: Client,
    catalog: Arc<tokio::sync::OnceCell<Arc<Vec<CatalogObject>>>>,
    acquisition_metrics: Arc<Mutex<SourceAcquisitionMetrics>>,
}

impl EraeSource {
    /// Construct a lazy source. The catalog is fetched on the first plan.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid settings or HTTP client construction.
    pub fn new(config: EraeConfig) -> Result<Self, EraeError> {
        config.validate()?;
        let client = Client::builder()
            .timeout(config.request_timeout)
            .connect_timeout(config.request_timeout.min(Duration::from_secs(10)))
            .user_agent(concat!("leani/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(EraeError::HttpClient)?;
        let descriptor = SourceDescriptor {
            id: config.id.clone(),
            kind: SourceKind::HistoryArchive,
            chain_id: config.chain_id,
            range: None,
            capabilities: execution_capabilities(),
            complete_capabilities: execution_capabilities(),
            // Canonicality ultimately follows the published catalog. Every
            // execution-layer commitment is checked locally, but the current
            // adapter does not pretend the mirror itself is trustless.
            trust: TrustModel::TrustedDataset,
            finality: FinalityModel::Finalized,
            partitioning: Partitioning::SourceDefined("erae-dynamic-index".to_owned()),
            expected_lag: Duration::from_hours(2),
            schema_version: format!("erae.e2store.v1+reth.{RETH_REVISION}"),
            priority: config.priority,
        };
        Ok(Self {
            config,
            descriptor,
            client,
            catalog: Arc::new(tokio::sync::OnceCell::new()),
            acquisition_metrics: Arc::new(Mutex::new(SourceAcquisitionMetrics::default())),
        })
    }

    fn record_physical_read(&self) {
        let mut metrics = self
            .acquisition_metrics
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        metrics.physical_reads = Some(metrics.physical_reads.unwrap_or(0).saturating_add(1));
    }

    fn record_fetched_bytes(&self, bytes: usize) {
        let mut metrics = self
            .acquisition_metrics
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        metrics.fetched_bytes = Some(
            metrics
                .fetched_bytes
                .unwrap_or(0)
                .saturating_add(u64::try_from(bytes).unwrap_or(u64::MAX)),
        );
    }

    fn record_source_object(&self, bytes: u64) {
        let mut metrics = self
            .acquisition_metrics
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        metrics.source_objects = Some(metrics.source_objects.unwrap_or(0).saturating_add(1));
        metrics.source_object_bytes = Some(
            metrics
                .source_object_bytes
                .unwrap_or(0)
                .saturating_add(bytes),
        );
    }

    fn record_decoded_batch(&self, frames: &[BlockFrame], elapsed: Duration) {
        let normalized_bytes = frames.iter().fold(0_u64, |total, frame| {
            total.saturating_add(frame.estimated_heap_bytes())
        });
        let mut metrics = self
            .acquisition_metrics
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        metrics.acquired_frames = metrics
            .acquired_frames
            .saturating_add(u64::try_from(frames.len()).unwrap_or(u64::MAX));
        metrics.normalized_bytes = metrics.normalized_bytes.saturating_add(normalized_bytes);
        metrics.operation_elapsed_ms = metrics
            .operation_elapsed_ms
            .saturating_add(u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX));
    }

    /// Read and verify a finite range through the same planner and sparse
    /// streaming path used by backfills and on-demand RPC.
    ///
    /// # Errors
    ///
    /// Returns source planning, transport, budget, decoding, or commitment
    /// verification failures.
    pub async fn probe_range(
        &self,
        range: BlockRange,
        budget: SourceBudget,
        cancellation: CancellationToken,
    ) -> Result<(Vec<BlockFrame>, EraeProbeReport), SourceError> {
        let request = DataRequest {
            chain_id: self.config.chain_id,
            range,
            required: CapabilitySet::of(Capability::Header)
                .with(Capability::Transactions)
                .with(Capability::Receipts),
            allow_filtered: false,
            projection: FieldProjection::default(),
            log_fields: leani_primitives::LogFieldSet::NONE,
            filters: FilterSet::default(),
            minimum_finality: Finality::Finalized,
            verification_policy: VerificationPolicy::TrustedDataset,
        };
        let plan = self.plan(&request).await?;
        let mut frames = Vec::new();
        for chunk in &plan.chunks {
            let mut stream = self.open(chunk, budget, cancellation.clone()).await?;
            while let Some(frame) = stream.next().await {
                frames.push(frame?);
            }
        }
        let normalized_bytes = frames.iter().fold(0_u64, |total, frame| {
            total.saturating_add(frame.estimated_heap_bytes())
        });
        let report = EraeProbeReport {
            source: self.descriptor.clone(),
            range,
            frames: u64::try_from(frames.len()).unwrap_or(u64::MAX),
            normalized_bytes,
        };
        Ok((frames, report))
    }

    async fn catalog(&self) -> Result<Arc<Vec<CatalogObject>>, SourceError> {
        self.catalog
            .get_or_try_init(|| async {
                let url = self
                    .config
                    .base_url
                    .join("checksums_sha256.txt")
                    .map_err(|error| SourceError::Unavailable(error.to_string()))?;
                let bytes = self.read_complete(&url, MAX_CATALOG_BYTES).await?;
                parse_catalog(&self.config, &bytes).map(Arc::new)
            })
            .await
            .cloned()
    }

    async fn read_complete(&self, url: &Url, limit: u64) -> Result<Vec<u8>, SourceError> {
        self.record_physical_read();
        if url.scheme() == "file" {
            let path = file_path(url)?;
            let bytes = tokio::task::spawn_blocking(move || std::fs::read(path))
                .await
                .map_err(|error| SourceError::Unavailable(error.to_string()))?
                .map_err(|error| SourceError::Unavailable(error.to_string()))
                .and_then(|bytes| enforce_byte_limit(bytes, limit, "catalog_bytes"))?;
            self.record_fetched_bytes(bytes.len());
            return Ok(bytes);
        }
        let response = self
            .client
            .get(url.clone())
            .send()
            .await
            .map_err(|error| SourceError::Unavailable(error.to_string()))?;
        if !response.status().is_success() {
            return Err(SourceError::Unavailable(format!(
                "{} returned HTTP {}",
                url,
                response.status()
            )));
        }
        if response
            .content_length()
            .is_some_and(|length| length > limit)
        {
            return Err(SourceError::BudgetExceeded {
                resource: "catalog_bytes",
                limit,
                observed: response.content_length().unwrap_or(u64::MAX),
            });
        }
        let bytes = response
            .bytes()
            .await
            .map_err(|error| SourceError::Unavailable(error.to_string()))?
            .to_vec();
        let bytes = enforce_byte_limit(bytes, limit, "catalog_bytes")?;
        self.record_fetched_bytes(bytes.len());
        Ok(bytes)
    }

    async fn object_size(&self, url: &Url) -> Result<u64, SourceError> {
        self.record_physical_read();
        if url.scheme() == "file" {
            let path = file_path(url)?;
            let size = tokio::task::spawn_blocking(move || std::fs::metadata(path))
                .await
                .map_err(|error| SourceError::Unavailable(error.to_string()))?
                .map(|metadata| metadata.len())
                .map_err(|error| SourceError::Unavailable(error.to_string()))?;
            self.record_source_object(size);
            return Ok(size);
        }
        let response = self
            .client
            .head(url.clone())
            .send()
            .await
            .map_err(|error| SourceError::Unavailable(error.to_string()))?;
        if !response.status().is_success() {
            return Err(SourceError::Unavailable(format!(
                "{} returned HTTP {}",
                url,
                response.status()
            )));
        }
        let size = response
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse().ok())
            .ok_or_else(|| SourceError::Protocol("eraE object has no content length".to_owned()))?;
        self.record_source_object(size);
        Ok(size)
    }

    async fn read_range(
        &self,
        url: &Url,
        start: u64,
        end: u64,
        cancellation: &CancellationToken,
    ) -> Result<Vec<u8>, SourceError> {
        if end < start {
            return Err(SourceError::InvalidPlan(
                "invalid eraE byte range".to_owned(),
            ));
        }
        let expected = end.saturating_sub(start).saturating_add(1);
        if cancellation.is_cancelled() {
            return Err(SourceError::Cancelled);
        }
        if url.scheme() == "file" {
            self.record_physical_read();
            let path = file_path(url)?;
            let bytes = tokio::task::spawn_blocking(move || {
                use std::io::{Seek, SeekFrom};
                let mut file = std::fs::File::open(path)?;
                file.seek(SeekFrom::Start(start))?;
                let length = usize::try_from(expected).map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::InvalidInput, "range too large")
                })?;
                let mut bytes = vec![0; length];
                file.read_exact(&mut bytes)?;
                Ok::<_, std::io::Error>(bytes)
            })
            .await
            .map_err(|error| SourceError::Unavailable(error.to_string()))?
            .map_err(|error| SourceError::Unavailable(error.to_string()))?;
            self.record_fetched_bytes(bytes.len());
            return Ok(bytes);
        }
        let mut response = None;
        for attempt in 1..=3 {
            self.record_physical_read();
            let request = self
                .client
                .get(url.clone())
                .header(RANGE, format!("bytes={start}-{end}"))
                .header(ACCEPT, "application/octet-stream")
                .header(ACCEPT_ENCODING, "identity")
                .send();
            let candidate = tokio::select! {
                () = cancellation.cancelled() => return Err(SourceError::Cancelled),
                response = request => response
                    .map_err(|error| SourceError::Unavailable(error.to_string()))?,
            };
            if candidate.status() == StatusCode::PARTIAL_CONTENT {
                response = Some(candidate);
                break;
            }
            let status = candidate.status();
            drop(candidate);
            if attempt == 3 {
                return Err(SourceError::Protocol(format!(
                    "{url} ignored byte range {start}-{end} with HTTP {status}",
                )));
            }
            tokio::select! {
                () = cancellation.cancelled() => return Err(SourceError::Cancelled),
                () = tokio::time::sleep(Duration::from_millis(100)) => {}
            }
        }
        let response = response.ok_or_else(|| {
            SourceError::Protocol("eraE byte-range retry loop ended without a response".to_owned())
        })?;
        let bytes = response
            .bytes()
            .await
            .map_err(|error| SourceError::Unavailable(error.to_string()))?
            .to_vec();
        let observed = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        if observed != expected {
            return Err(SourceError::Protocol(format!(
                "eraE range returned {observed} bytes, expected {expected}"
            )));
        }
        self.record_fetched_bytes(bytes.len());
        Ok(bytes)
    }

    async fn load_index(
        &self,
        object: &CatalogObject,
        budget: SourceBudget,
        cancellation: &CancellationToken,
    ) -> Result<ObjectIndex, SourceError> {
        let file_size = self.object_size(&object.locator).await?;
        if file_size < 32 {
            return Err(SourceError::CorruptFrame(
                "eraE object is too short".to_owned(),
            ));
        }
        let trailer = self
            .read_range(
                &object.locator,
                file_size.saturating_sub(16),
                file_size.saturating_sub(1),
                cancellation,
            )
            .await?;
        let component_count = read_u64(&trailer[..8])?;
        let count = read_u64(&trailer[8..])?;
        if !(2..=5).contains(&component_count) || count == 0 || count > BLOCKS_PER_FILE {
            return Err(SourceError::CorruptFrame(
                "invalid eraE dynamic index trailer".to_owned(),
            ));
        }
        let payload_bytes = count
            .checked_mul(component_count)
            .and_then(|value| value.checked_mul(8))
            .and_then(|value| value.checked_add(24))
            .ok_or_else(|| SourceError::CorruptFrame("eraE index length overflow".to_owned()))?;
        let entry_bytes = payload_bytes.saturating_add(8);
        let index_position = file_size.checked_sub(entry_bytes).ok_or_else(|| {
            SourceError::CorruptFrame("eraE index exceeds object length".to_owned())
        })?;
        let encoded = self
            .read_range(
                &object.locator,
                index_position,
                file_size.saturating_sub(1),
                cancellation,
            )
            .await?;
        let mut encoded_slice = encoded.as_slice();
        let entry = Entry::read(&mut encoded_slice)
            .map_err(|error| SourceError::CorruptFrame(error.to_string()))?
            .ok_or_else(|| SourceError::CorruptFrame("eraE index is absent".to_owned()))?;
        if !encoded_slice.is_empty() || entry.entry_type != DYNAMIC_BLOCK_INDEX {
            return Err(SourceError::CorruptFrame(
                "eraE final entry is not the dynamic block index".to_owned(),
            ));
        }
        let index = DynamicBlockIndex::from_entry(&entry)
            .map_err(|error| SourceError::CorruptFrame(error.to_string()))?;
        if index.starting_number() != object.advertised_range.start().0 {
            return Err(SourceError::CorruptFrame(format!(
                "eraE filename starts at {}, index starts at {}",
                object.advertised_range.start().0,
                index.starting_number()
            )));
        }
        let version = self.read_range(&object.locator, 0, 7, cancellation).await?;
        if version[..2] != VERSION || version[2..] != [0; 6] {
            return Err(SourceError::CorruptFrame(
                "invalid eraE e2store version record".to_owned(),
            ));
        }
        let first_offsets = index
            .offsets_for_block(index.starting_number())
            .ok_or_else(|| SourceError::CorruptFrame("eraE index has no first block".to_owned()))?;
        let mut component_types = Vec::with_capacity(first_offsets.len());
        let mut probe_bytes = 0_u64;
        for offset in first_offsets {
            let position = resolve_offset(index_position, *offset)?;
            let header = self
                .read_range(
                    &object.locator,
                    position,
                    position.saturating_add(7),
                    cancellation,
                )
                .await?;
            component_types.push([header[0], header[1]]);
            probe_bytes = probe_bytes.saturating_add(8);
        }
        validate_component_layout(&component_types)?;
        let initial_input_bytes = 16_u64
            .saturating_add(entry_bytes)
            .saturating_add(8)
            .saturating_add(probe_bytes);
        enforce_budget(initial_input_bytes, budget.max_input_bytes, "input_bytes")?;
        Ok(ObjectIndex {
            index_position,
            index,
            component_types,
            initial_input_bytes,
        })
    }
}

#[async_trait]
impl HistorySource for EraeSource {
    fn descriptor(&self) -> &SourceDescriptor {
        &self.descriptor
    }

    fn acquisition_metrics(&self) -> Option<SourceAcquisitionMetrics> {
        Some(
            self.acquisition_metrics
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
        )
    }

    async fn plan(&self, request: &DataRequest) -> Result<SourcePlan, SourceError> {
        if request.chain_id != self.descriptor.chain_id
            || !self
                .descriptor
                .complete_capabilities
                .with_derivable()
                .contains_all(request.required)
            || !self.descriptor.finality.supports(request.minimum_finality)
        {
            return Err(SourceError::InvalidPlan(
                "eraE cannot satisfy the requested chain, capabilities, or finality".to_owned(),
            ));
        }
        let catalog = self.catalog().await?;
        let mut chunks = Vec::new();
        let mut next = request.range.start().0;
        for object in catalog.iter() {
            if object.advertised_range.end().0 < next
                || object.advertised_range.start().0 > request.range.end().0
            {
                continue;
            }
            if object.advertised_range.start().0 > next {
                break;
            }
            let object_end = object.advertised_range.end().0.min(request.range.end().0);
            while next <= object_end {
                let end = object_end.min(next.saturating_add(1_023));
                chunks.push(SourceChunk {
                    source_id: self.descriptor.id.clone(),
                    range: BlockRange::new(BlockNumber(next), BlockNumber(end))
                        .map_err(|error| SourceError::InvalidPlan(error.to_string()))?,
                    partition: encode_partition(&object.filename, request.required),
                    schema_version: self.descriptor.schema_version.clone(),
                    expected_parent: None,
                    estimated_bytes: None,
                });
                next = end.saturating_add(1);
            }
            if next > request.range.end().0 {
                break;
            }
        }
        if next <= request.range.end().0 {
            return Err(SourceError::MissingRange(
                BlockRange::new(BlockNumber(next), request.range.end())
                    .map_err(|error| SourceError::InvalidPlan(error.to_string()))?,
            ));
        }
        let plan = SourcePlan {
            source_id: self.descriptor.id.clone(),
            request: request.clone(),
            chunks,
            estimated_bytes: None,
            estimated_lag: self.descriptor.expected_lag,
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
    ) -> Result<BlockFrameStream, SourceError> {
        let budget = budget.validate()?;
        if chunk.source_id != self.descriptor.id
            || chunk.schema_version != self.descriptor.schema_version
            || chunk.range.len() > budget.max_frames
        {
            return Err(SourceError::InvalidPlan(
                "eraE chunk identity, schema, or frame bound is invalid".to_owned(),
            ));
        }
        {
            let mut metrics = self
                .acquisition_metrics
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            metrics.opened_chunks = metrics.opened_chunks.saturating_add(1);
            metrics.opened_ranges.push(chunk.range);
        }
        let (filename, required) = decode_partition(&chunk.partition)?;
        let catalog = self.catalog().await?;
        let object = catalog
            .iter()
            .find(|object| object.filename == filename)
            .cloned()
            .ok_or_else(|| SourceError::InvalidPlan("unknown eraE object".to_owned()))?;
        let index = Arc::new(self.load_index(&object, budget, &cancellation).await?);
        let actual_end = index
            .index
            .starting_number()
            .saturating_add(u64::try_from(index.index.block_count()).unwrap_or(u64::MAX))
            .saturating_sub(1);
        if chunk.range.start().0 < index.index.starting_number() || chunk.range.end().0 > actual_end
        {
            return Err(SourceError::MissingRange(chunk.range));
        }
        let state = EraeStreamState {
            source: self.clone(),
            object,
            index,
            required,
            next: chunk.range.start().0,
            end: chunk.range.end().0,
            budget,
            observed_input_bytes: 0,
            previous_hash: None,
            pending: VecDeque::new(),
            cancellation,
        };
        Ok(Box::pin(stream::try_unfold(state, next_stream_frame)))
    }
}

#[derive(Debug)]
struct EraeStreamState {
    source: EraeSource,
    object: CatalogObject,
    index: Arc<ObjectIndex>,
    required: CapabilitySet,
    next: u64,
    end: u64,
    budget: SourceBudget,
    observed_input_bytes: u64,
    previous_hash: Option<BlockHash>,
    pending: VecDeque<BlockFrame>,
    cancellation: CancellationToken,
}

async fn next_stream_frame(
    mut state: EraeStreamState,
) -> Result<Option<(BlockFrame, EraeStreamState)>, SourceError> {
    if let Some(frame) = state.pending.pop_front() {
        return Ok(Some((frame, state)));
    }
    if state.next > state.end {
        return Ok(None);
    }
    if state.cancellation.is_cancelled() {
        return Err(SourceError::Cancelled);
    }
    if state.observed_input_bytes == 0 {
        state.observed_input_bytes = state.index.initial_input_bytes;
    }
    let buffered = u64::try_from(state.budget.max_buffered_frames).unwrap_or(u64::MAX);
    let batch_end = state
        .end
        .min(state.next.saturating_add(buffered.saturating_sub(1)));
    let range = BlockRange::new(BlockNumber(state.next), BlockNumber(batch_end))
        .map_err(|error| SourceError::InvalidPlan(error.to_string()))?;
    let started = Instant::now();
    let (mut frames, input_bytes) = read_sparse_batch(
        &state.source,
        &state.object,
        &state.index,
        range,
        state.required,
        state.budget,
        &state.cancellation,
    )
    .await?;
    state
        .source
        .record_decoded_batch(&frames, started.elapsed());
    state.observed_input_bytes = state.observed_input_bytes.saturating_add(input_bytes);
    enforce_budget(
        state.observed_input_bytes,
        state.budget.max_input_bytes,
        "input_bytes",
    )?;
    if let (Some(previous), Some(first)) = (state.previous_hash, frames.first_mut()) {
        if first.block.parent_hash != previous {
            return Err(SourceError::CorruptFrame(format!(
                "eraE parent continuity failed at block {}",
                first.block.number.0
            )));
        }
        first.verification.parent_continuity = VerificationCheck::VERIFIED;
    }
    state.previous_hash = frames.last().map(|frame| frame.block.hash);
    state.next = batch_end.saturating_add(1);
    state.pending.extend(frames);
    state
        .pending
        .pop_front()
        .map(|frame| Some((frame, state)))
        .ok_or_else(|| SourceError::CorruptFrame("eraE batch returned no frames".to_owned()))
}

#[allow(clippy::too_many_lines)]
async fn read_sparse_batch(
    source: &EraeSource,
    object: &CatalogObject,
    object_index: &ObjectIndex,
    range: BlockRange,
    required: CapabilitySet,
    budget: SourceBudget,
    cancellation: &CancellationToken,
) -> Result<(Vec<BlockFrame>, u64), SourceError> {
    let needs_receipts = required
        .intersection(CapabilitySet::of(Capability::Receipts).with(Capability::Logs))
        .bits()
        != 0;
    let needed_types = if needs_receipts {
        [COMPRESSED_HEADER, COMPRESSED_BODY, COMPRESSED_SLIM_RECEIPTS].as_slice()
    } else {
        [COMPRESSED_HEADER, COMPRESSED_BODY].as_slice()
    };
    let all_positions = sorted_positions(object_index)?;
    let mut targets = BTreeMap::<u64, [u8; 2]>::new();
    for number in range.iter() {
        let offsets = object_index
            .index
            .offsets_for_block(number.0)
            .ok_or_else(|| SourceError::MissingRange(BlockRange::single(number)))?;
        for (offset, entry_type) in offsets.iter().zip(&object_index.component_types) {
            if needed_types.contains(entry_type) {
                targets.insert(
                    resolve_offset(object_index.index_position, *offset)?,
                    *entry_type,
                );
            }
        }
    }
    let intervals =
        coalesce_target_intervals(&targets, &all_positions, object_index.index_position)?;
    let mut fetched = Vec::with_capacity(intervals.len());
    let mut input_bytes = 0_u64;
    for (start, end) in intervals {
        let bytes = source
            .read_range(&object.locator, start, end, cancellation)
            .await?;
        input_bytes = input_bytes.saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        enforce_budget(
            object_index.initial_input_bytes.saturating_add(input_bytes),
            budget.max_input_bytes,
            "input_bytes",
        )?;
        fetched.push((start, bytes));
    }
    let mut by_position = BTreeMap::new();
    for (position, expected_type) in targets {
        let (start, bytes) = fetched
            .iter()
            .find(|(start, bytes)| {
                position >= *start
                    && position
                        < start.saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX))
            })
            .ok_or_else(|| SourceError::CorruptFrame("eraE target was not fetched".to_owned()))?;
        let relative = usize::try_from(position.saturating_sub(*start))
            .map_err(|_| SourceError::CorruptFrame("eraE offset is too large".to_owned()))?;
        let mut slice = &bytes[relative..];
        let entry = Entry::read(&mut slice)
            .map_err(|error| SourceError::CorruptFrame(error.to_string()))?
            .ok_or_else(|| SourceError::CorruptFrame("eraE component is absent".to_owned()))?;
        if entry.entry_type != expected_type {
            return Err(SourceError::CorruptFrame(format!(
                "eraE component type mismatch: expected {expected_type:02x?}, got {:02x?}",
                entry.entry_type
            )));
        }
        by_position.insert(position, entry);
    }
    let mut tuples = Vec::with_capacity(usize::try_from(range.len()).unwrap_or(0));
    for number in range.iter() {
        let offsets = object_index
            .index
            .offsets_for_block(number.0)
            .ok_or_else(|| SourceError::MissingRange(BlockRange::single(number)))?;
        let mut header = None;
        let mut body = None;
        let mut receipts = None;
        for (offset, entry_type) in offsets.iter().zip(&object_index.component_types) {
            if !needed_types.contains(entry_type) {
                continue;
            }
            let position = resolve_offset(object_index.index_position, *offset)?;
            let entry = by_position
                .remove(&position)
                .ok_or_else(|| SourceError::CorruptFrame("eraE component missing".to_owned()))?;
            match *entry_type {
                COMPRESSED_HEADER => {
                    header = Some(
                        CompressedHeader::from_entry(&entry)
                            .map_err(|error| SourceError::CorruptFrame(error.to_string()))?,
                    );
                }
                COMPRESSED_BODY => {
                    body = Some(
                        CompressedBody::from_entry(&entry)
                            .map_err(|error| SourceError::CorruptFrame(error.to_string()))?,
                    );
                }
                COMPRESSED_SLIM_RECEIPTS => {
                    receipts = Some(
                        CompressedSlimReceipts::from_entry(&entry)
                            .map_err(|error| SourceError::CorruptFrame(error.to_string()))?,
                    );
                }
                _ => {}
            }
        }
        let mut tuple = BlockTuple::new(
            header.ok_or_else(|| SourceError::CorruptFrame("eraE header missing".to_owned()))?,
            body.ok_or_else(|| SourceError::CorruptFrame("eraE body missing".to_owned()))?,
        );
        if let Some(receipts) = receipts {
            tuple = tuple.with_receipts(receipts);
        }
        tuples.push(tuple);
    }
    let frames =
        decode_verify_normalize(source, object, object_index, range, tuples, needs_receipts)?;
    Ok((frames, input_bytes))
}

fn decode_verify_normalize(
    source: &EraeSource,
    object: &CatalogObject,
    object_index: &ObjectIndex,
    range: BlockRange,
    tuples: Vec<BlockTuple>,
    needs_receipts: bool,
) -> Result<Vec<BlockFrame>, SourceError> {
    let observed_at_unix_ms = now_milliseconds();
    let mut headers = Vec::with_capacity(tuples.len());
    let mut bodies = Vec::with_capacity(tuples.len());
    let mut receipts = Vec::with_capacity(tuples.len());
    for tuple in tuples {
        headers.push(
            tuple
                .header
                .decode_header()
                .map_err(|error| SourceError::CorruptFrame(error.to_string()))?,
        );
        bodies.push(
            tuple
                .body
                .decode_body::<reth_ethereum_primitives::TransactionSigned, Header>()
                .map_err(|error| SourceError::CorruptFrame(error.to_string()))?,
        );
        if needs_receipts {
            let slim = tuple
                .receipts
                .ok_or_else(|| {
                    SourceError::InvalidPlan(
                        "eraE object omits receipts required by this request".to_owned(),
                    )
                })?
                .decode_receipts()
                .map_err(|error| SourceError::CorruptFrame(error.to_string()))?;
            receipts.push(expand_receipts(slim)?);
        }
    }
    validate_headers(range, &headers)?;
    validate_bodies(&headers, &bodies)?;
    if needs_receipts {
        validate_receipts(&headers, &bodies, &receipts)?;
    }
    let actual_end = object_index
        .index
        .starting_number()
        .saturating_add(u64::try_from(object_index.index.block_count()).unwrap_or(u64::MAX))
        .saturating_sub(1);
    if range.end().0 == actual_end {
        let last = headers
            .last()
            .ok_or_else(|| SourceError::CorruptFrame("eraE batch is empty".to_owned()))?
            .hash_slow();
        if last.as_slice()[..4] != object.short_last_hash {
            return Err(SourceError::CorruptFrame(
                "eraE filename hash does not match its last block".to_owned(),
            ));
        }
    }
    headers
        .iter()
        .zip(&bodies)
        .enumerate()
        .map(|(index, (header, body))| {
            let block_receipts = needs_receipts.then(|| receipts[index].as_slice());
            normalize_block(
                source,
                object,
                header,
                body,
                block_receipts,
                observed_at_unix_ms,
                index > 0,
            )
        })
        .collect()
}

fn expand_receipts(receipts: Vec<SlimReceipt>) -> Result<Vec<EthereumReceipt>, SourceError> {
    receipts
        .into_iter()
        .map(|receipt| {
            let success = receipt.status.as_eip658().ok_or_else(|| {
                SourceError::InvalidPlan(
                    "pre-Byzantium post-state receipts are not representable by the current RPC frame schema"
                        .to_owned(),
                )
            })?;
            Ok(EthereumReceipt {
                tx_type: receipt.tx_type,
                success,
                cumulative_gas_used: receipt.cumulative_gas_used,
                logs: receipt.logs,
            })
        })
        .collect()
}

fn validate_headers(range: BlockRange, headers: &[Header]) -> Result<(), SourceError> {
    if u64::try_from(headers.len()).unwrap_or(u64::MAX) != range.len() {
        return Err(SourceError::CorruptFrame(
            "eraE header count differs from requested range".to_owned(),
        ));
    }
    for (index, header) in headers.iter().enumerate() {
        let expected = range
            .start()
            .0
            .saturating_add(u64::try_from(index).unwrap_or(u64::MAX));
        if header.number != expected {
            return Err(SourceError::CorruptFrame(format!(
                "eraE header number {}, expected {expected}",
                header.number
            )));
        }
    }
    for pair in headers.windows(2) {
        if pair[1].parent_hash != pair[0].hash_slow() {
            return Err(SourceError::CorruptFrame(format!(
                "eraE parent continuity failed at block {}",
                pair[1].number
            )));
        }
    }
    Ok(())
}

fn validate_bodies(headers: &[Header], bodies: &[BlockBody]) -> Result<(), SourceError> {
    if headers.len() != bodies.len() {
        return Err(SourceError::CorruptFrame(
            "eraE header/body count mismatch".to_owned(),
        ));
    }
    for (header, body) in headers.iter().zip(bodies) {
        if calculate_transaction_root(&body.transactions) != header.transactions_root
            || body.calculate_ommers_root() != header.ommers_hash
            || body.calculate_withdrawals_root() != header.withdrawals_root
        {
            return Err(SourceError::CorruptFrame(format!(
                "eraE body commitment mismatch at block {}",
                header.number
            )));
        }
    }
    Ok(())
}

fn validate_receipts(
    headers: &[Header],
    bodies: &[BlockBody],
    receipts: &[Vec<EthereumReceipt>],
) -> Result<(), SourceError> {
    if headers.len() != receipts.len() {
        return Err(SourceError::CorruptFrame(
            "eraE header/receipt count mismatch".to_owned(),
        ));
    }
    for ((header, body), block_receipts) in headers.iter().zip(bodies).zip(receipts) {
        if body.transactions.len() != block_receipts.len() {
            return Err(SourceError::CorruptFrame(format!(
                "eraE transaction/receipt count mismatch at block {}",
                header.number
            )));
        }
        for (transaction, receipt) in body.transactions.iter().zip(block_receipts) {
            if transaction.tx_type() != receipt.tx_type {
                return Err(SourceError::CorruptFrame(format!(
                    "eraE transaction/receipt type mismatch at block {}",
                    header.number
                )));
            }
        }
        let with_bloom = block_receipts
            .iter()
            .map(alloy_consensus::TxReceipt::with_bloom_ref)
            .collect::<Vec<_>>();
        if calculate_receipt_root(&with_bloom) != header.receipts_root {
            return Err(SourceError::CorruptFrame(format!(
                "eraE receipts root mismatch at block {}",
                header.number
            )));
        }
        let bloom = with_bloom
            .iter()
            .fold(alloy_primitives::Bloom::ZERO, |total, receipt| {
                total | receipt.bloom_ref()
            });
        if bloom != header.logs_bloom {
            return Err(SourceError::CorruptFrame(format!(
                "eraE logs bloom mismatch at block {}",
                header.number
            )));
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn normalize_block(
    source: &EraeSource,
    object: &CatalogObject,
    header: &Header,
    body: &BlockBody,
    receipts: Option<&[EthereumReceipt]>,
    observed_at_unix_ms: u64,
    parent_checked: bool,
) -> Result<BlockFrame, SourceError> {
    let hash = header.hash_slow();
    let block_size = alloy_rlp::encode(Block {
        header: header.clone(),
        body: body.clone(),
    })
    .len();
    let mut normalized_transactions = Vec::with_capacity(body.transactions.len());
    let mut normalized_receipts = Vec::new();
    let mut normalized_logs = Vec::new();
    let mut previous_gas = 0_u64;
    let mut block_log_index = 0_u32;
    for (index, transaction) in body.transactions.iter().enumerate() {
        let transaction_index = u32::try_from(index)
            .map_err(|_| SourceError::CorruptFrame("transaction index overflow".to_owned()))?;
        let transaction_hash = TransactionHash::new(transaction.tx_hash().0);
        let sender = transaction
            .recover_signer()
            .map_err(|error| SourceError::CorruptFrame(error.to_string()))?;
        let encoded = transaction.encoded_2718();
        normalized_transactions.push(TransactionEnvelope {
            hash: transaction_hash,
            transaction_type: transaction.tx_type() as u8,
            index: transaction_index,
            encoded: Some(encoded.clone()),
            from: Some(address(sender)),
            to: transaction.to().map(address),
            nonce: Some(transaction.nonce()),
            gas_limit: Some(transaction.gas_limit()),
            value: Some(quantity(transaction.value())),
            input: Some(transaction.input().to_vec()),
            max_fee_per_gas: Some(quantity(U256::from(transaction.max_fee_per_gas()))),
            max_priority_fee_per_gas: transaction
                .max_priority_fee_per_gas()
                .map(U256::from)
                .map(quantity),
            max_fee_per_blob_gas: transaction
                .max_fee_per_blob_gas()
                .map(U256::from)
                .map(quantity),
            blob_versioned_hashes: transaction
                .blob_versioned_hashes()
                .unwrap_or_default()
                .iter()
                .map(|hash| BlockHash::new(hash.0))
                .collect(),
            size_bytes: Some(u32::try_from(encoded.len()).unwrap_or(u32::MAX)),
        });
        if let Some(receipt) = receipts.and_then(|receipts| receipts.get(index)) {
            let gas_used = receipt.cumulative_gas_used.saturating_sub(previous_gas);
            previous_gas = receipt.cumulative_gas_used;
            let mut receipt_logs = Vec::with_capacity(receipt.logs.len());
            for source_log in &receipt.logs {
                let log = Log {
                    address: address(source_log.address),
                    topics: source_log
                        .data
                        .topics()
                        .iter()
                        .map(|topic| topic.0)
                        .collect(),
                    data: source_log.data.data.to_vec(),
                    transaction_hash: Some(transaction_hash),
                    transaction_index,
                    log_index: block_log_index,
                };
                block_log_index = block_log_index.checked_add(1).ok_or_else(|| {
                    SourceError::CorruptFrame("log index overflows u32".to_owned())
                })?;
                receipt_logs.push(log.clone());
                normalized_logs.push(log);
            }
            normalized_receipts.push(ReceiptEnvelope {
                transaction_hash,
                transaction_type: receipt.tx_type as u8,
                transaction_index,
                encoded: Some(receipt.with_bloom_ref().encoded_2718()),
                success: Some(receipt.success),
                gas_used: Some(gas_used),
                effective_gas_price: Some(quantity(U256::from(
                    transaction.effective_gas_price(header.base_fee_per_gas),
                ))),
                blob_gas_used: transaction.blob_gas_used(),
                blob_gas_price: None,
                logs: receipt_logs,
            });
        }
    }
    let withdrawals = body
        .withdrawals
        .as_ref()
        .map(|withdrawals| {
            withdrawals
                .iter()
                .map(|withdrawal| Withdrawal {
                    index: withdrawal.index,
                    validator_index: withdrawal.validator_index,
                    address: address(withdrawal.address),
                    amount_gwei: withdrawal.amount,
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let receipts_material = receipts.map_or_else(
        || Material::Missing(MissingReason::NotRequested),
        |_| Material::Complete(normalized_receipts),
    );
    let logs_material = receipts.map_or_else(
        || Material::Missing(MissingReason::NotRequested),
        |_| Material::Complete(normalized_logs),
    );
    Ok(BlockFrame {
        chain_id: source.config.chain_id,
        block: BlockRef {
            number: BlockNumber(header.number),
            hash: BlockHash::new(hash.0),
            parent_hash: BlockHash::new(header.parent_hash.0),
            timestamp: header.timestamp,
        },
        finality: Finality::Finalized,
        header: Material::Complete(HeaderEnvelope {
            rlp: Some(alloy_rlp::encode(header)),
            transactions_root: Some(BlockHash::new(header.transactions_root.0)),
            receipts_root: Some(BlockHash::new(header.receipts_root.0)),
            withdrawals_root: header.withdrawals_root.map(|hash| BlockHash::new(hash.0)),
            gas_limit: Some(header.gas_limit),
            gas_used: Some(header.gas_used),
            base_fee_per_gas: header.base_fee_per_gas.map(U256::from).map(quantity),
            blob_gas_used: header.blob_gas_used,
            excess_blob_gas: header.excess_blob_gas,
            size_bytes: Some(u64::try_from(block_size).unwrap_or(u64::MAX)),
            consensus_size_bytes: None,
            transaction_count: Some(u32::try_from(body.transactions.len()).unwrap_or(u32::MAX)),
        }),
        transactions: Material::Complete(normalized_transactions),
        receipts: receipts_material,
        logs: logs_material,
        withdrawals: Material::Complete(withdrawals),
        blob_sidecars: Material::Missing(MissingReason::Unsupported),
        traces: Material::Missing(MissingReason::Unsupported),
        state_diffs: Material::Missing(MissingReason::Unsupported),
        provenance: vec![Provenance {
            source_id: source.descriptor.id.clone(),
            source_kind: SourceKind::HistoryArchive,
            trust: TrustModel::TrustedDataset,
            range: Some(BlockRange::single(BlockNumber(header.number))),
            object: Some(ObjectIdentity {
                locator: object.locator.to_string(),
                version: Some(format!("sha256:{}", hex::encode(object.checksum))),
                checksum: Some(object.checksum),
                schema: Some("erae/e2store".to_owned()),
            }),
            observed_at_unix_ms,
            projection: if receipts.is_some() {
                vec![
                    "header".to_owned(),
                    "body".to_owned(),
                    "receipts".to_owned(),
                ]
            } else {
                vec!["header".to_owned(), "body".to_owned()]
            },
        }],
        verification: VerificationReport {
            header_hash: VerificationCheck::VERIFIED,
            parent_continuity: if parent_checked || header.number == 0 {
                VerificationCheck::VERIFIED
            } else {
                VerificationCheck::UNAVAILABLE
            },
            transactions_root: VerificationCheck::VERIFIED,
            receipts_root: if receipts.is_some() {
                VerificationCheck::VERIFIED
            } else {
                VerificationCheck::NOT_CHECKED
            },
            withdrawals_root: VerificationCheck::VERIFIED,
            dataset_checksum: VerificationCheck {
                status: CheckStatus::Unavailable,
                detail: Some(
                    "sparse reads verify execution roots; full-object SHA-256 was not downloaded"
                        .to_owned(),
                ),
            },
            consensus_anchor: None,
        },
    })
}

fn parse_catalog(config: &EraeConfig, bytes: &[u8]) -> Result<Vec<CatalogObject>, SourceError> {
    let text = std::str::from_utf8(bytes).map_err(|error| SourceError::SchemaDrift {
        expected: "sha256sum catalog".to_owned(),
        actual: error.to_string(),
    })?;
    let mut by_era = BTreeMap::new();
    for (line_index, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let mut fields = line.split_whitespace();
        let checksum = fields.next().unwrap_or_default();
        let filename = fields.next().unwrap_or_default().trim_start_matches('*');
        if fields.next().is_some() {
            return Err(SourceError::SchemaDrift {
                expected: "sha256 filename".to_owned(),
                actual: format!("line {} has extra fields", line_index.saturating_add(1)),
            });
        }
        let checksum: [u8; 32] = hex::decode(checksum)
            .ok()
            .and_then(|bytes| bytes.try_into().ok())
            .ok_or_else(|| SourceError::SchemaDrift {
                expected: "32-byte SHA-256".to_owned(),
                actual: format!("line {}", line_index.saturating_add(1)),
            })?;
        let (era, short_last_hash) = parse_filename(filename, &config.network)?;
        let start = era
            .checked_mul(BLOCKS_PER_FILE)
            .ok_or_else(|| SourceError::SchemaDrift {
                expected: "bounded era number".to_owned(),
                actual: filename.to_owned(),
            })?;
        let advertised_range = BlockRange::new(
            BlockNumber(start),
            BlockNumber(start.saturating_add(BLOCKS_PER_FILE - 1)),
        )
        .map_err(|error| SourceError::InvalidPlan(error.to_string()))?;
        let locator = config
            .base_url
            .join(filename)
            .map_err(|error| SourceError::SchemaDrift {
                expected: "safe eraE filename".to_owned(),
                actual: error.to_string(),
            })?;
        let object = CatalogObject {
            filename: filename.to_owned(),
            locator,
            checksum,
            advertised_range,
            short_last_hash,
        };
        if by_era.insert(era, object).is_some() {
            return Err(SourceError::SchemaDrift {
                expected: "one eraE object per era".to_owned(),
                actual: format!("duplicate era {era}"),
            });
        }
        if by_era.len() > MAX_CATALOG_OBJECTS {
            return Err(SourceError::BudgetExceeded {
                resource: "catalog_objects",
                limit: u64::try_from(MAX_CATALOG_OBJECTS).unwrap_or(u64::MAX),
                observed: u64::try_from(by_era.len()).unwrap_or(u64::MAX),
            });
        }
    }
    if by_era.is_empty() {
        return Err(SourceError::SchemaDrift {
            expected: "non-empty eraE checksum catalog".to_owned(),
            actual: "empty catalog".to_owned(),
        });
    }
    Ok(by_era.into_values().collect())
}

#[allow(clippy::case_sensitive_file_extension_comparisons)]
fn parse_filename(filename: &str, network: &str) -> Result<(u64, [u8; 4]), SourceError> {
    if filename.contains('/')
        || filename.contains('\\')
        || !(filename.ends_with(".erae") || filename.ends_with(".ere"))
    {
        return Err(SourceError::SchemaDrift {
            expected: "simple .erae/.ere filename".to_owned(),
            actual: filename.to_owned(),
        });
    }
    let stem = filename
        .strip_suffix(".erae")
        .or_else(|| filename.strip_suffix(".ere"))
        .unwrap_or_default();
    let prefix = format!("{network}-");
    let remainder = stem
        .strip_prefix(&prefix)
        .ok_or_else(|| SourceError::SchemaDrift {
            expected: format!("{network} eraE filename"),
            actual: filename.to_owned(),
        })?;
    let mut parts = remainder.split('-');
    let era_text = parts.next().unwrap_or_default();
    let short_hash_text = parts.next().unwrap_or_default();
    if parts.next().is_some()
        || era_text.len() != 5
        || short_hash_text.len() != 8
        || !era_text.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(SourceError::SchemaDrift {
            expected: "<network>-<5 digit era>-<8 hex hash>.erae".to_owned(),
            actual: filename.to_owned(),
        });
    }
    let era = era_text.parse().map_err(|_| SourceError::SchemaDrift {
        expected: "numeric era".to_owned(),
        actual: filename.to_owned(),
    })?;
    let short_last_hash = hex::decode(short_hash_text)
        .ok()
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| SourceError::SchemaDrift {
            expected: "four-byte filename hash".to_owned(),
            actual: filename.to_owned(),
        })?;
    Ok((era, short_last_hash))
}

fn validate_component_layout(types: &[[u8; 2]]) -> Result<(), SourceError> {
    if types.len() < 2
        || types[0] != COMPRESSED_HEADER
        || types[1] != COMPRESSED_BODY
        || types.iter().copied().collect::<BTreeSet<_>>().len() != types.len()
    {
        return Err(SourceError::CorruptFrame(
            "invalid eraE indexed component layout".to_owned(),
        ));
    }
    Ok(())
}

fn sorted_positions(index: &ObjectIndex) -> Result<Vec<u64>, SourceError> {
    let mut positions = index
        .index
        .offsets()
        .iter()
        .map(|offset| resolve_offset(index.index_position, *offset))
        .collect::<Result<Vec<_>, _>>()?;
    positions.sort_unstable();
    positions.dedup();
    if positions.len() != index.index.offsets().len()
        || positions.first().is_none_or(|position| *position < 8)
        || positions
            .last()
            .is_none_or(|position| *position >= index.index_position)
    {
        return Err(SourceError::CorruptFrame(
            "eraE index offsets overlap or escape component region".to_owned(),
        ));
    }
    Ok(positions)
}

fn coalesce_target_intervals(
    targets: &BTreeMap<u64, [u8; 2]>,
    all_positions: &[u64],
    index_position: u64,
) -> Result<Vec<(u64, u64)>, SourceError> {
    let target_set = targets.keys().copied().collect::<BTreeSet<_>>();
    let mut entries = Vec::with_capacity(targets.len());
    for position in targets.keys().copied() {
        let index = all_positions
            .binary_search(&position)
            .map_err(|_| SourceError::CorruptFrame("eraE target offset is unknown".to_owned()))?;
        let end_exclusive = all_positions
            .get(index.saturating_add(1))
            .copied()
            .unwrap_or(index_position);
        if end_exclusive <= position {
            return Err(SourceError::CorruptFrame(
                "eraE component has an invalid extent".to_owned(),
            ));
        }
        entries.push((position, end_exclusive.saturating_sub(1)));
    }
    let mut output: Vec<(u64, u64)> = Vec::new();
    for interval in entries {
        if let Some(previous) = output.last_mut()
            && previous.1.saturating_add(1) == interval.0
            && target_set.contains(&interval.0)
        {
            previous.1 = interval.1;
        } else {
            output.push(interval);
        }
    }
    Ok(output)
}

fn resolve_offset(index_position: u64, offset: i64) -> Result<u64, SourceError> {
    let position = i128::from(index_position) + i128::from(offset);
    u64::try_from(position).map_err(|_| {
        SourceError::CorruptFrame("eraE relative offset is outside the object".to_owned())
    })
}

fn read_u64(bytes: &[u8]) -> Result<u64, SourceError> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| SourceError::CorruptFrame("expected eight bytes".to_owned()))?;
    Ok(u64::from_le_bytes(bytes))
}

fn encode_partition(filename: &str, required: CapabilitySet) -> Vec<u8> {
    let mut output = filename.as_bytes().to_vec();
    output.push(0);
    output.extend_from_slice(&required.bits().to_be_bytes());
    output
}

fn decode_partition(bytes: &[u8]) -> Result<(&str, CapabilitySet), SourceError> {
    let separator = bytes
        .len()
        .checked_sub(3)
        .ok_or_else(|| SourceError::InvalidPlan("invalid eraE partition".to_owned()))?;
    if bytes.get(separator) != Some(&0) {
        return Err(SourceError::InvalidPlan(
            "invalid eraE partition capability bytes".to_owned(),
        ));
    }
    let filename = std::str::from_utf8(&bytes[..separator])
        .map_err(|_| SourceError::InvalidPlan("invalid eraE partition filename".to_owned()))?;
    let bits = u16::from_be_bytes(
        bytes[separator + 1..]
            .try_into()
            .map_err(|_| SourceError::InvalidPlan("invalid eraE capability bits".to_owned()))?,
    );
    let required = CapabilitySet::from_bits(bits)
        .ok_or_else(|| SourceError::InvalidPlan("unknown eraE capability bits".to_owned()))?;
    Ok((filename, required))
}

const fn execution_capabilities() -> CapabilitySet {
    CapabilitySet::of(Capability::Header)
        .with(Capability::Body)
        .with(Capability::Transactions)
        .with(Capability::Calldata)
        .with(Capability::Receipts)
        .with(Capability::Logs)
        .with(Capability::Withdrawals)
}

fn file_path(url: &Url) -> Result<PathBuf, SourceError> {
    url.to_file_path()
        .map_err(|()| SourceError::Unavailable(format!("invalid file URL {url}")))
}

fn enforce_byte_limit(
    bytes: Vec<u8>,
    limit: u64,
    resource: &'static str,
) -> Result<Vec<u8>, SourceError> {
    enforce_budget(
        u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        limit,
        resource,
    )?;
    Ok(bytes)
}

fn enforce_budget(observed: u64, limit: u64, resource: &'static str) -> Result<(), SourceError> {
    if observed > limit {
        Err(SourceError::BudgetExceeded {
            resource,
            limit,
            observed,
        })
    } else {
        Ok(())
    }
}

fn address(value: AlloyAddress) -> Address {
    Address::new(value.0.0)
}

fn quantity(value: U256) -> Quantity {
    Quantity::new(value.to_be_bytes())
}

fn now_milliseconds() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

/// Adapter construction failures.
#[derive(Debug, thiserror::Error)]
pub enum EraeError {
    #[error("invalid eraE configuration: {0}")]
    InvalidConfig(String),
    #[error("invalid eraE URL: {0}")]
    Url(url::ParseError),
    #[error("cannot construct eraE HTTP client: {0}")]
    HttpClient(reqwest::Error),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EraeProbeReport {
    pub source: SourceDescriptor,
    pub range: BlockRange,
    pub frames: u64,
    pub normalized_bytes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_catalog_lines_are_strict_and_ordered() {
        let config = EraeConfig::public_mainnet().expect("config");
        let objects = parse_catalog(
            &config,
            b"ee0bf48ac736558538b597261beba4c7352e4e06a2cdb090dab5bc68b6149959  mainnet-00000-a6860fef.erae\n5e466b724a0852010dbbe8d22a1a1cd43a17fa65f9ea3371aebb36c1694b87a8  mainnet-00001-05c64fc4.erae\n",
        )
        .expect("catalog");
        assert_eq!(objects.len(), 2);
        assert_eq!(objects[0].advertised_range.start(), BlockNumber(0));
        assert_eq!(objects[1].advertised_range.start(), BlockNumber(8_192));
    }

    #[test]
    fn partitions_preserve_capabilities() {
        let required = CapabilitySet::of(Capability::Header).with(Capability::Receipts);
        let encoded = encode_partition("mainnet-00000-a6860fef.erae", required);
        let (filename, decoded) = decode_partition(&encoded).expect("partition");
        assert_eq!(filename, "mainnet-00000-a6860fef.erae");
        assert_eq!(decoded, required);
    }

    #[test]
    fn component_intervals_coalesce_sections() {
        let targets = BTreeMap::from([
            (100, COMPRESSED_HEADER),
            (110, COMPRESSED_HEADER),
            (200, COMPRESSED_BODY),
        ]);
        let intervals =
            coalesce_target_intervals(&targets, &[100, 110, 120, 200, 220], 300).expect("ranges");
        assert_eq!(intervals, vec![(100, 119), (200, 219)]);
    }
}
