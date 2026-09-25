//! Sparse, bounded reader for execution-history `eraE`/ERE archives.
//!
//! The source reads the e2store dynamic index first and issues HTTP range
//! requests only for the block components required by a request. Archive bytes
//! are never retained after the returned stream is consumed.
//!
//! The mirror and its checksum catalog are the trust root. The catalog names
//! each era's object, and the source checks every execution commitment inside
//! the blocks it reads, but nothing anchors a block hash to consensus.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fmt,
    io::Read,
    net::IpAddr,
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
    header::{
        ACCEPT, ACCEPT_ENCODING, CONTENT_LENGTH, ETAG, HeaderMap, IF_MODIFIED_SINCE, IF_NONE_MATCH,
        LAST_MODIFIED, RANGE,
    },
};
use reth_era::{
    e2s::types::{Entry, VERSION},
    ere::types::{
        execution::{COMPRESSED_BODY, COMPRESSED_HEADER, COMPRESSED_SLIM_RECEIPTS, SlimReceipt},
        group::{DYNAMIC_BLOCK_INDEX, DynamicBlockIndex},
    },
};
use reth_ethereum_primitives::BlockBody;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use url::Url;

const BLOCKS_PER_FILE: u64 = 8_192;
const CATALOG_FILE: &str = "checksums_sha256.txt";
const MAX_CATALOG_BYTES: u64 = 16 * 1_024 * 1_024;
const MAX_CATALOG_OBJECTS: usize = 16_384;
/// A cached checksum catalog is refetched once it is this old, so a
/// long-running source sees the eras its mirror publishes later.
const CATALOG_TTL: Duration = Duration::from_mins(10);
/// A plan that asks past the cached catalog refetches it once the catalog is
/// this old. The interval doubles, up to [`CATALOG_TTL`], each time a refetch
/// still lacks the range, and starts over once a refetch brings new eras.
const CATALOG_MISS_REFRESH: Duration = Duration::from_secs(30);
/// First mainnet block whose receipts carry a status code (EIP-658). Earlier
/// receipts carry an intermediate state root, which frames cannot represent.
const MAINNET_BYZANTIUM_BLOCK: u64 = 4_370_000;
/// Size of an e2store record header: type, payload length, and reserved bytes.
const E2STORE_HEADER_BYTES: usize = 8;
/// Largest decompressed header, body, and receipt list of one block together.
/// They share one decode budget, the frame budget capped at this size, which
/// their normalized frame must meet as well. Bodies stay below 10 MiB even at
/// the highest gas limits, and the frame budget usually binds first.
const MAX_BLOCK_DECODE_BYTES: u64 = 32 * 1_024 * 1_024;
const RETH_REVISION: &str = "5a6940e351fed80458fe6c9da8581cbe4b8bd036";

/// Construction settings for one archive mirror.
#[derive(Clone)]
pub struct EraeConfig {
    pub id: SourceId,
    pub chain_id: ChainId,
    pub network: String,
    pub base_url: Url,
    pub priority: u16,
    pub request_timeout: Duration,
    /// Accept a plain `http` mirror off loopback. Its catalog and archive
    /// bytes then travel unauthenticated, so anyone on the path can replace
    /// them.
    pub allow_insecure_http: bool,
}

impl fmt::Debug for EraeConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EraeConfig")
            .field("id", &self.id)
            .field("chain_id", &self.chain_id)
            .field("network", &self.network)
            .field("base_url", &redacted_url(&self.base_url))
            .field("priority", &self.priority)
            .field("request_timeout", &self.request_timeout)
            .field("allow_insecure_http", &self.allow_insecure_http)
            .finish()
    }
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
            allow_insecure_http: false,
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
        if self.base_url.scheme() == "http"
            && !self.allow_insecure_http
            && !is_loopback(&self.base_url)
        {
            return Err(EraeError::InvalidConfig(format!(
                "eraE mirror {} uses plain http; use https or a loopback mirror, or accept an unauthenticated mirror with allow_insecure_http = true on its [[sources.history]] entry",
                redacted_url(&self.base_url)
            )));
        }
        if !self.base_url.path().ends_with('/') {
            return Err(EraeError::InvalidConfig(
                "eraE base URL must end with `/`".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Whether `url` names this machine, so plain `http` never leaves it.
#[must_use]
pub fn is_loopback(url: &Url) -> bool {
    match url.host() {
        Some(url::Host::Domain(host)) => host.eq_ignore_ascii_case("localhost"),
        Some(url::Host::Ipv4(address)) => address.is_loopback(),
        Some(url::Host::Ipv6(address)) => IpAddr::V6(address).to_canonical().is_loopback(),
        None => false,
    }
}

/// Render a mirror URL for errors and provenance as `scheme://host[:port]`,
/// with `/…` in place of a base path. Mirrors can carry credentials in
/// userinfo, query strings, or paths, so none of them is shown.
fn redacted_url(url: &Url) -> String {
    let mut rendered = format!("{}://", url.scheme());
    if let Some(host) = url.host_str() {
        rendered.push_str(host);
    }
    if let Some(port) = url.port() {
        rendered.push(':');
        rendered.push_str(&port.to_string());
    }
    if !matches!(url.path(), "" | "/") {
        rendered.push_str("/…");
    }
    rendered
}

/// Label a file below the mirror: the redacted mirror and the file name,
/// which comes from the catalog and is never secret.
fn mirror_file_label(base_url: &Url, file: &str) -> String {
    format!("{}/{file}", redacted_url(base_url))
}

#[derive(Clone)]
struct CatalogObject {
    filename: String,
    locator: Url,
    /// The locator without mirror credentials, for errors and provenance.
    label: String,
    /// The catalog's SHA-256 of the whole object. Sparse reads never
    /// recompute it.
    checksum: [u8; 32],
    advertised_range: BlockRange,
    short_last_hash: [u8; 4],
}

impl fmt::Debug for CatalogObject {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CatalogObject")
            .field("filename", &self.filename)
            .field("label", &self.label)
            .field("checksum", &hex::encode(self.checksum))
            .field("advertised_range", &self.advertised_range)
            .field("short_last_hash", &hex::encode(self.short_last_hash))
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct CachedCatalog {
    objects: Arc<Vec<CatalogObject>>,
    fetched_at: tokio::time::Instant,
    /// How old the catalog must be before a plan that misses refetches it.
    miss_refresh: Duration,
    /// The `ETag` and `Last-Modified` the mirror sent with the catalog, so an
    /// unchanged catalog costs a 304.
    validators: HeaderMap,
}

/// Where an object's dynamic block index lies.
#[derive(Clone, Copy, Debug)]
struct IndexLayout {
    component_count: u64,
    /// The index record's size, header included.
    entry_bytes: u64,
    /// The index record's offset in the object.
    position: u64,
}

#[derive(Debug)]
struct ObjectIndex {
    index_position: u64,
    index: DynamicBlockIndex,
    component_types: Vec<[u8; 2]>,
    initial_input_bytes: u64,
}

/// One block's decoded archive components.
struct ArchivedBlock {
    header: Header,
    body: BlockBody,
    receipts: Option<Vec<SlimReceipt>>,
}

#[derive(Clone, Debug)]
pub struct EraeSource {
    config: EraeConfig,
    descriptor: SourceDescriptor,
    client: Client,
    catalog: Arc<tokio::sync::Mutex<Option<CachedCatalog>>>,
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
            // A redirect could lead to any host; the configured mirror is
            // the trust root.
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(EraeError::HttpClient)?;
        let descriptor = SourceDescriptor {
            id: config.id.clone(),
            kind: SourceKind::HistoryArchive,
            chain_id: config.chain_id,
            range: None,
            capabilities: execution_capabilities(),
            complete_capabilities: execution_capabilities(),
            // Canonicality follows the mirror's published catalog. Every
            // execution-layer commitment is checked locally, but nothing
            // anchors a block hash to consensus.
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
            catalog: Arc::new(tokio::sync::Mutex::new(None)),
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

    /// The checksum catalog, fetched again once it is older than `max_age`,
    /// [`CATALOG_TTL`] to plan. Without a limit, as for a chunk already
    /// planned, any cached catalog serves.
    async fn catalog(
        &self,
        max_age: Option<Duration>,
    ) -> Result<Arc<Vec<CatalogObject>>, SourceError> {
        let mut cached = self.catalog.lock().await;
        if let Some(catalog) = cached
            .as_ref()
            .filter(|catalog| max_age.is_none_or(|max_age| catalog.fetched_at.elapsed() < max_age))
        {
            return Ok(Arc::clone(&catalog.objects));
        }
        self.refresh_catalog(&mut cached).await
    }

    /// Plan `request` again after it missed the cached catalog, which is
    /// refetched once it is older than its miss interval: the mirror may have
    /// published the missing eras since. A refetch that still lacks them
    /// doubles the interval, so a stale mirror is not polled every 30 seconds.
    async fn replan_after_miss(
        &self,
        request: &DataRequest,
    ) -> Result<Vec<SourceChunk>, SourceError> {
        let mut cached = self.catalog.lock().await;
        if let Some(catalog) = cached
            .as_ref()
            .filter(|catalog| catalog.fetched_at.elapsed() < catalog.miss_refresh)
        {
            return plan_chunks(&self.descriptor, &catalog.objects, request);
        }
        let objects = self.refresh_catalog(&mut cached).await?;
        let chunks = plan_chunks(&self.descriptor, &objects, request);
        if let (Err(SourceError::MissingRange(_)), Some(catalog)) = (&chunks, cached.as_mut()) {
            catalog.miss_refresh = catalog.miss_refresh.saturating_mul(2).min(CATALOG_TTL);
        }
        chunks
    }

    /// Fetch the catalog into `cached`, or keep the cached one when the
    /// mirror answers that it has not changed.
    async fn refresh_catalog(
        &self,
        cached: &mut Option<CachedCatalog>,
    ) -> Result<Arc<Vec<CatalogObject>>, SourceError> {
        let url = self
            .config
            .base_url
            .join(CATALOG_FILE)
            .map_err(|error| SourceError::Unavailable(error.to_string()))?;
        let label = mirror_file_label(&self.config.base_url, CATALOG_FILE);
        let fetched = self
            .read_catalog(
                &url,
                &label,
                cached.as_ref().map(|catalog| &catalog.validators),
            )
            .await?;
        let now = tokio::time::Instant::now();
        let Some((bytes, validators)) = fetched else {
            let catalog = cached.as_mut().ok_or_else(|| {
                SourceError::Protocol(format!("{label} answered 304 with no catalog cached"))
            })?;
            catalog.fetched_at = now;
            return Ok(Arc::clone(&catalog.objects));
        };
        let objects = Arc::new(parse_catalog(&self.config, &bytes)?);
        // New eras start the miss interval over.
        let miss_refresh = cached
            .as_ref()
            .filter(|previous| {
                previous
                    .objects
                    .iter()
                    .map(|object| &object.filename)
                    .eq(objects.iter().map(|object| &object.filename))
            })
            .map_or(CATALOG_MISS_REFRESH, |previous| previous.miss_refresh);
        *cached = Some(CachedCatalog {
            objects: Arc::clone(&objects),
            fetched_at: now,
            miss_refresh,
            validators,
        });
        Ok(objects)
    }

    /// Read the catalog, or `None` when the mirror answers 304 to the
    /// `validators` of the cached one.
    async fn read_catalog(
        &self,
        url: &Url,
        label: &str,
        validators: Option<&HeaderMap>,
    ) -> Result<Option<(Vec<u8>, HeaderMap)>, SourceError> {
        self.record_physical_read();
        let too_large = |observed| SourceError::BudgetExceeded {
            resource: "catalog_bytes",
            limit: MAX_CATALOG_BYTES,
            observed,
        };
        if url.scheme() == "file" {
            let path = file_path(url, label)?;
            let bytes = tokio::task::spawn_blocking(move || {
                let mut bytes = Vec::new();
                std::fs::File::open(path)?
                    .take(MAX_CATALOG_BYTES.saturating_add(1))
                    .read_to_end(&mut bytes)?;
                Ok::<_, std::io::Error>(bytes)
            })
            .await
            .map_err(|error| SourceError::Unavailable(error.to_string()))?
            .map_err(|error| SourceError::Unavailable(format!("{label}: {error}")))?;
            let observed = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
            if observed > MAX_CATALOG_BYTES {
                return Err(too_large(observed));
            }
            self.record_fetched_bytes(bytes.len());
            return Ok(Some((bytes, HeaderMap::new())));
        }
        let mut request = self.client.get(url.clone());
        for (validator, condition) in [(ETAG, IF_NONE_MATCH), (LAST_MODIFIED, IF_MODIFIED_SINCE)] {
            if let Some(value) = validators.and_then(|validators| validators.get(&validator)) {
                request = request.header(condition, value.clone());
            }
        }
        let response = request
            .send()
            .await
            .map_err(|error| transport_error(label, error))?;
        if response.status() == StatusCode::NOT_MODIFIED && validators.is_some() {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(SourceError::Unavailable(format!(
                "{label} returned HTTP {}",
                response.status()
            )));
        }
        let mut validators = HeaderMap::new();
        for name in [ETAG, LAST_MODIFIED] {
            if let Some(value) = response.headers().get(&name) {
                validators.insert(name, value.clone());
            }
        }
        let bytes = read_capped(response, label, MAX_CATALOG_BYTES, too_large).await?;
        self.record_fetched_bytes(bytes.len());
        Ok(Some((bytes, validators)))
    }

    async fn object_size(&self, object: &CatalogObject) -> Result<u64, SourceError> {
        self.record_physical_read();
        if object.locator.scheme() == "file" {
            let path = file_path(&object.locator, &object.label)?;
            let size = tokio::task::spawn_blocking(move || std::fs::metadata(path))
                .await
                .map_err(|error| SourceError::Unavailable(error.to_string()))?
                .map(|metadata| metadata.len())
                .map_err(|error| SourceError::Unavailable(format!("{}: {error}", object.label)))?;
            self.record_source_object(size);
            return Ok(size);
        }
        let response = self
            .client
            .head(object.locator.clone())
            .send()
            .await
            .map_err(|error| transport_error(&object.label, error))?;
        if !response.status().is_success() {
            return Err(SourceError::Unavailable(format!(
                "{} returned HTTP {}",
                object.label,
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

    /// Read bytes `start..=end` of `object`. Callers bound the length: the
    /// fixed-size index probes, or a batch checked against its budget.
    async fn read_range(
        &self,
        object: &CatalogObject,
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
        let label = &object.label;
        if object.locator.scheme() == "file" {
            self.record_physical_read();
            let path = file_path(&object.locator, label)?;
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
            .map_err(|error| SourceError::Unavailable(format!("{label}: {error}")))?;
            self.record_fetched_bytes(bytes.len());
            return Ok(bytes);
        }
        let mut response = None;
        for attempt in 1..=3 {
            self.record_physical_read();
            let request = self
                .client
                .get(object.locator.clone())
                .header(RANGE, format!("bytes={start}-{end}"))
                .header(ACCEPT, "application/octet-stream")
                .header(ACCEPT_ENCODING, "identity")
                .send();
            let candidate = tokio::select! {
                () = cancellation.cancelled() => return Err(SourceError::Cancelled),
                response = request => response
                    .map_err(|error| transport_error(label, error))?,
            };
            if candidate.status() == StatusCode::PARTIAL_CONTENT {
                response = Some(candidate);
                break;
            }
            let status = candidate.status();
            drop(candidate);
            if attempt == 3 {
                return Err(SourceError::Protocol(format!(
                    "{label} ignored byte range {start}-{end} with HTTP {status}",
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
        let bytes = read_capped(response, label, expected, |observed| {
            SourceError::Protocol(format!(
                "{label} returned at least {observed} bytes for range {start}-{end}, expected {expected}"
            ))
        })
        .await?;
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
        let file_size = self.object_size(object).await?;
        if file_size < 32 {
            return Err(SourceError::CorruptFrame(
                "eraE object is too short".to_owned(),
            ));
        }
        let trailer = self
            .read_range(
                object,
                file_size.saturating_sub(16),
                file_size.saturating_sub(1),
                cancellation,
            )
            .await?;
        let layout = index_layout(file_size, &trailer)?;
        // The trailer, the index, the version record, and one record header
        // per component of the first block.
        let initial_input_bytes = 16_u64
            .saturating_add(layout.entry_bytes)
            .saturating_add(8)
            .saturating_add(layout.component_count.saturating_mul(8));
        enforce_budget(initial_input_bytes, budget.max_input_bytes, "input_bytes")?;
        let encoded = self
            .read_range(
                object,
                layout.position,
                file_size.saturating_sub(1),
                cancellation,
            )
            .await?;
        let (entry_type, payload) = e2store_record(&encoded)?;
        if entry_type != DYNAMIC_BLOCK_INDEX
            || payload.len().saturating_add(E2STORE_HEADER_BYTES) != encoded.len()
        {
            return Err(SourceError::CorruptFrame(
                "eraE final entry is not the dynamic block index".to_owned(),
            ));
        }
        let index = DynamicBlockIndex::from_entry(&Entry::new(entry_type, payload.to_vec()))
            .map_err(|error| SourceError::CorruptFrame(error.to_string()))?;
        if index.starting_number() != object.advertised_range.start().0 {
            return Err(SourceError::CorruptFrame(format!(
                "eraE filename starts at {}, index starts at {}",
                object.advertised_range.start().0,
                index.starting_number()
            )));
        }
        let version = self.read_range(object, 0, 7, cancellation).await?;
        if version[..2] != VERSION || version[2..] != [0; 6] {
            return Err(SourceError::CorruptFrame(
                "invalid eraE e2store version record".to_owned(),
            ));
        }
        let first_offsets = index
            .offsets_for_block(index.starting_number())
            .ok_or_else(|| SourceError::CorruptFrame("eraE index has no first block".to_owned()))?;
        let mut component_types = Vec::with_capacity(first_offsets.len());
        for offset in first_offsets {
            let position = resolve_offset(layout.position, *offset)?;
            let header = self
                .read_range(object, position, position.saturating_add(7), cancellation)
                .await?;
            component_types.push([header[0], header[1]]);
        }
        validate_component_layout(&component_types)?;
        Ok(ObjectIndex {
            index_position: layout.position,
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
        if requires_receipts(request.required)
            && self.config.chain_id == ChainId(1)
            && request.range.start().0 < MAINNET_BYZANTIUM_BLOCK
        {
            return Err(SourceError::InvalidPlan(format!(
                "eraE cannot serve receipts or logs before Byzantium (block {MAINNET_BYZANTIUM_BLOCK}): older receipts carry an intermediate state root instead of a status; request headers and bodies only, or start at block {MAINNET_BYZANTIUM_BLOCK}"
            )));
        }
        let catalog = self.catalog(Some(CATALOG_TTL)).await?;
        let chunks = match plan_chunks(&self.descriptor, &catalog, request) {
            Err(SourceError::MissingRange(_)) => self.replan_after_miss(request).await?,
            chunks => chunks?,
        };
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
        // The chunk was planned from the cached catalog, so its age does not
        // matter here, and a failed refresh cannot fail the chunk.
        let catalog = self.catalog(None).await?;
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

/// Cover `request` with chunks of at most 1,024 blocks from consecutive
/// catalog objects.
fn plan_chunks(
    descriptor: &SourceDescriptor,
    catalog: &[CatalogObject],
    request: &DataRequest,
) -> Result<Vec<SourceChunk>, SourceError> {
    let mut chunks = Vec::new();
    let mut next = request.range.start().0;
    for object in catalog {
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
                source_id: descriptor.id.clone(),
                range: BlockRange::new(BlockNumber(next), BlockNumber(end))
                    .map_err(|error| SourceError::InvalidPlan(error.to_string()))?,
                partition: encode_partition(&object.filename, request.required),
                schema_version: descriptor.schema_version.clone(),
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
    Ok(chunks)
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
        state.observed_input_bytes,
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

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn read_sparse_batch(
    source: &EraeSource,
    object: &CatalogObject,
    object_index: &ObjectIndex,
    range: BlockRange,
    required: CapabilitySet,
    budget: SourceBudget,
    observed_input_bytes: u64,
    cancellation: &CancellationToken,
) -> Result<(Vec<BlockFrame>, u64), SourceError> {
    let needs_receipts = requires_receipts(required);
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
    // The index sizes every component, so the whole batch is checked against
    // the remaining budget before any of it is requested.
    let input_bytes = intervals
        .iter()
        .try_fold(0_u64, |total, (start, end)| {
            end.checked_sub(*start)
                .and_then(|length| length.checked_add(1))
                .and_then(|length| total.checked_add(length))
        })
        .ok_or_else(|| SourceError::CorruptFrame("eraE batch size overflows".to_owned()))?;
    enforce_budget(
        observed_input_bytes.saturating_add(input_bytes),
        budget.max_input_bytes,
        "input_bytes",
    )?;
    let mut fetched = Vec::with_capacity(intervals.len());
    for (start, end) in intervals {
        let bytes = source.read_range(object, start, end, cancellation).await?;
        fetched.push((start, bytes));
    }
    let mut components = BTreeMap::new();
    for (position, expected_type) in targets {
        let (start, bytes) = fetched
            .iter()
            .find(|(start, bytes)| {
                position >= *start
                    && position
                        < start.saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX))
            })
            .ok_or_else(|| SourceError::CorruptFrame("eraE target was not fetched".to_owned()))?;
        let extent = component_end(position, &all_positions, object_index.index_position)?
            .saturating_sub(*start);
        let relative = usize::try_from(position.saturating_sub(*start))
            .map_err(|_| SourceError::CorruptFrame("eraE offset is too large".to_owned()))?;
        let extent = usize::try_from(extent)
            .ok()
            .and_then(|extent| bytes.get(relative..extent))
            .ok_or_else(|| {
                SourceError::CorruptFrame("eraE component was not fetched".to_owned())
            })?;
        let (entry_type, payload) = e2store_record(extent)?;
        if entry_type != expected_type {
            return Err(SourceError::CorruptFrame(format!(
                "eraE component type mismatch: expected {expected_type:02x?}, got {entry_type:02x?}"
            )));
        }
        components.insert(position, payload);
    }
    let decode_limit = budget.max_frame_bytes.min(MAX_BLOCK_DECODE_BYTES);
    let mut blocks = Vec::with_capacity(usize::try_from(range.len()).unwrap_or(0));
    for number in range.iter() {
        let offsets = object_index
            .index
            .offsets_for_block(number.0)
            .ok_or_else(|| SourceError::MissingRange(BlockRange::single(number)))?;
        let mut header = None;
        let mut body = None;
        let mut receipts = None;
        let mut decoded = 0_u64;
        for (offset, entry_type) in offsets.iter().zip(&object_index.component_types) {
            if !needed_types.contains(entry_type) {
                continue;
            }
            let position = resolve_offset(object_index.index_position, *offset)?;
            let component = components
                .remove(&position)
                .ok_or_else(|| SourceError::CorruptFrame("eraE component missing".to_owned()))?;
            let decompressed = decompress_component(component, decoded, decode_limit)?;
            decoded = decoded.saturating_add(u64::try_from(decompressed.len()).unwrap_or(u64::MAX));
            match *entry_type {
                COMPRESSED_HEADER => header = Some(decode_component::<Header>(&decompressed)?),
                COMPRESSED_BODY => body = Some(decode_component::<BlockBody>(&decompressed)?),
                COMPRESSED_SLIM_RECEIPTS => {
                    receipts = Some(decode_component::<Vec<SlimReceipt>>(&decompressed)?);
                }
                _ => {}
            }
        }
        blocks.push(ArchivedBlock {
            header: header
                .ok_or_else(|| SourceError::CorruptFrame("eraE header missing".to_owned()))?,
            body: body.ok_or_else(|| SourceError::CorruptFrame("eraE body missing".to_owned()))?,
            receipts,
        });
    }
    let frames =
        decode_verify_normalize(source, object, object_index, range, blocks, needs_receipts)?;
    for frame in &frames {
        enforce_budget(
            frame.estimated_heap_bytes(),
            budget.max_frame_bytes,
            "frame_bytes",
        )?;
    }
    Ok((frames, input_bytes))
}

fn decode_component<T: alloy_rlp::Decodable>(bytes: &[u8]) -> Result<T, SourceError> {
    alloy_rlp::decode_exact(bytes)
        .map_err(|error| SourceError::CorruptFrame(format!("eraE component RLP: {error}")))
}

fn decode_verify_normalize(
    source: &EraeSource,
    object: &CatalogObject,
    object_index: &ObjectIndex,
    range: BlockRange,
    blocks: Vec<ArchivedBlock>,
    needs_receipts: bool,
) -> Result<Vec<BlockFrame>, SourceError> {
    let observed_at_unix_ms = now_milliseconds();
    let mut headers = Vec::with_capacity(blocks.len());
    let mut bodies = Vec::with_capacity(blocks.len());
    let mut receipts = Vec::with_capacity(blocks.len());
    for block in blocks {
        headers.push(block.header);
        bodies.push(block.body);
        if needs_receipts {
            let slim = block.receipts.ok_or_else(|| {
                SourceError::InvalidPlan(
                    "eraE object omits receipts required by this request".to_owned(),
                )
            })?;
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
                locator: object.label.clone(),
                // The catalog's digest names the object version. Sparse reads
                // never recompute it, so it is not a verified checksum.
                version: Some(format!("sha256:{}", hex::encode(object.checksum))),
                checksum: None,
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
            // The hash is computed from the archived header itself; nothing
            // independent anchors it.
            header_hash: VerificationCheck {
                status: CheckStatus::NotChecked,
                detail: Some(
                    "computed from the archived header; no consensus anchor was checked".to_owned(),
                ),
            },
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
            withdrawals_root: if header.withdrawals_root.is_some() {
                VerificationCheck::VERIFIED
            } else {
                VerificationCheck {
                    status: CheckStatus::Unavailable,
                    detail: Some("blocks before Shanghai commit to no withdrawals".to_owned()),
                }
            },
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
            label: mirror_file_label(&config.base_url, filename),
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
    let mut output: Vec<(u64, u64)> = Vec::new();
    for position in targets.keys().copied() {
        let end = component_end(position, all_positions, index_position)?.saturating_sub(1);
        if let Some(previous) = output.last_mut()
            && previous.1.saturating_add(1) == position
        {
            previous.1 = end;
        } else {
            output.push((position, end));
        }
    }
    Ok(output)
}

/// The exclusive end of the component at `position`: the next indexed
/// component, or the index itself.
fn component_end(
    position: u64,
    all_positions: &[u64],
    index_position: u64,
) -> Result<u64, SourceError> {
    let index = all_positions
        .binary_search(&position)
        .map_err(|_| SourceError::CorruptFrame("eraE target offset is unknown".to_owned()))?;
    let end = all_positions
        .get(index.saturating_add(1))
        .copied()
        .unwrap_or(index_position);
    if end <= position {
        return Err(SourceError::CorruptFrame(
            "eraE component has an invalid extent".to_owned(),
        ));
    }
    Ok(end)
}

/// Split the e2store record at the start of `extent` into its type and
/// payload. The declared length must fit in `extent`, and the payload is
/// borrowed from it, so a hostile length cannot drive an allocation.
fn e2store_record(extent: &[u8]) -> Result<([u8; 2], &[u8]), SourceError> {
    let (header, rest) = extent
        .split_first_chunk::<E2STORE_HEADER_BYTES>()
        .ok_or_else(|| SourceError::CorruptFrame("eraE record header is truncated".to_owned()))?;
    if header[6..] != [0, 0] {
        return Err(SourceError::CorruptFrame(
            "eraE record reserved bytes are not zero".to_owned(),
        ));
    }
    let length = u32::from_le_bytes([header[2], header[3], header[4], header[5]]);
    let payload = usize::try_from(length)
        .ok()
        .and_then(|length| rest.get(..length))
        .ok_or_else(|| {
            SourceError::CorruptFrame(format!(
                "eraE record declares {length} payload bytes, but its extent holds {}",
                rest.len()
            ))
        })?;
    Ok(([header[0], header[1]], payload))
}

/// Locate the dynamic block index from the object's size and its final 16
/// bytes, which hold the component count and the block count.
fn index_layout(file_size: u64, trailer: &[u8]) -> Result<IndexLayout, SourceError> {
    if file_size < 32 {
        return Err(SourceError::CorruptFrame(
            "eraE object is too short".to_owned(),
        ));
    }
    let (components, count) = trailer
        .split_at_checked(8)
        .filter(|(_, count)| count.len() == 8)
        .ok_or_else(|| {
            SourceError::CorruptFrame("eraE index trailer is not 16 bytes".to_owned())
        })?;
    let component_count = read_u64(components)?;
    let count = read_u64(count)?;
    if !(2..=5).contains(&component_count) || count == 0 || count > BLOCKS_PER_FILE {
        return Err(SourceError::CorruptFrame(
            "invalid eraE dynamic index trailer".to_owned(),
        ));
    }
    // Starting number, offsets, component count, and block count.
    let entry_bytes = count
        .checked_mul(component_count)
        .and_then(|value| value.checked_mul(8))
        .and_then(|value| value.checked_add(24))
        .and_then(|value| value.checked_add(E2STORE_HEADER_BYTES as u64))
        .ok_or_else(|| SourceError::CorruptFrame("eraE index length overflow".to_owned()))?;
    let position = file_size
        .checked_sub(entry_bytes)
        .ok_or_else(|| SourceError::CorruptFrame("eraE index exceeds object length".to_owned()))?;
    Ok(IndexLayout {
        component_count,
        entry_bytes,
        position,
    })
}

/// Decompress one framed-Snappy component of a block whose earlier
/// components took `decoded` of its `limit` decompressed bytes, through a
/// reader that stops one byte past the rest, so a small component cannot
/// expand without bound.
fn decompress_component(
    component: &[u8],
    decoded: u64,
    limit: u64,
) -> Result<Vec<u8>, SourceError> {
    let mut decompressed = Vec::new();
    snap::read::FrameDecoder::new(component)
        .take(limit.saturating_sub(decoded).saturating_add(1))
        .read_to_end(&mut decompressed)
        .map_err(|error| {
            SourceError::CorruptFrame(format!("eraE component is not framed Snappy: {error}"))
        })?;
    enforce_budget(
        decoded.saturating_add(u64::try_from(decompressed.len()).unwrap_or(u64::MAX)),
        limit,
        "decompressed_bytes",
    )?;
    Ok(decompressed)
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

fn file_path(url: &Url, label: &str) -> Result<PathBuf, SourceError> {
    url.to_file_path()
        .map_err(|()| SourceError::Unavailable(format!("invalid file URL {label}")))
}

/// A transport failure, described without the request URL, which can carry
/// mirror credentials.
fn transport_error(label: &str, error: reqwest::Error) -> SourceError {
    let error = error.without_url();
    let mut detail = format!("{label}: {error}");
    let mut cause = std::error::Error::source(&error);
    while let Some(inner) = cause {
        detail.push_str(": ");
        detail.push_str(&inner.to_string());
        cause = inner.source();
    }
    SourceError::Unavailable(detail)
}

/// Read a response body of at most `limit` bytes while it streams in.
/// `too_large` reports the bytes seen once they pass the limit.
async fn read_capped(
    mut response: reqwest::Response,
    label: &str,
    limit: u64,
    too_large: impl FnOnce(u64) -> SourceError,
) -> Result<Vec<u8>, SourceError> {
    if let Some(length) = response.content_length().filter(|length| *length > limit) {
        return Err(too_large(length));
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| transport_error(label, error))?
    {
        let observed = u64::try_from(body.len().saturating_add(chunk.len())).unwrap_or(u64::MAX);
        if observed > limit {
            return Err(too_large(observed));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// Whether `required` needs receipts, which logs derive from.
const fn requires_receipts(required: CapabilitySet) -> bool {
    required
        .intersection(CapabilitySet::of(Capability::Receipts).with(Capability::Logs))
        .bits()
        != 0
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

    const CATALOG_DIGEST: &str = "ee0bf48ac736558538b597261beba4c7352e4e06a2cdb090dab5bc68b6149959";
    const ERA_ZERO: &str = "mainnet-00000-a6860fef.erae";
    const SECRETS: [&str; 4] = ["operator", "hunter2", "hunter3", "hunter4"];

    fn range(start: u64, end: u64) -> BlockRange {
        BlockRange::new(BlockNumber(start), BlockNumber(end)).expect("range")
    }

    /// `url` with the fixture operator's password in its userinfo.
    fn with_credentials(url: &str) -> Url {
        let mut url = Url::parse(url).expect("URL");
        url.set_username("operator").expect("username");
        url.set_password(Some("hunter2")).expect("password");
        url
    }

    fn headers_and_bodies() -> CapabilitySet {
        CapabilitySet::of(Capability::Header).with(Capability::Transactions)
    }

    fn request(range: BlockRange, required: CapabilitySet) -> DataRequest {
        DataRequest {
            chain_id: ChainId(1),
            range,
            required,
            allow_filtered: false,
            projection: FieldProjection::default(),
            log_fields: leani_primitives::LogFieldSet::NONE,
            filters: FilterSet::default(),
            minimum_finality: Finality::Finalized,
            verification_policy: VerificationPolicy::TrustedDataset,
        }
    }

    fn budget(max_input_bytes: u64, max_frame_bytes: u64) -> SourceBudget {
        SourceBudget {
            max_input_bytes,
            max_frame_bytes,
            max_frames: 8_192,
            max_buffered_frames: 16,
            max_in_flight_requests: 1,
            temporary_disk_bytes: 1,
        }
    }

    fn catalog_text(names: &[&str]) -> String {
        names
            .iter()
            .map(|name| [CATALOG_DIGEST, "  ", name, "\n"].concat())
            .collect()
    }

    fn write_catalog(directory: &std::path::Path, names: &[&str]) {
        std::fs::write(directory.join("checksums_sha256.txt"), catalog_text(names))
            .expect("write catalog");
    }

    /// A source reading a local `file:` mirror.
    fn file_source(directory: &std::path::Path) -> EraeSource {
        let mut config = EraeConfig::public_mainnet().expect("config");
        config.base_url = Url::from_directory_path(directory).expect("directory URL");
        EraeSource::new(config).expect("file mirror")
    }

    /// Append one e2store record declaring `length` payload bytes.
    fn push_record(bytes: &mut Vec<u8>, entry_type: [u8; 2], length: u32, payload: &[u8]) {
        bytes.extend_from_slice(&entry_type);
        bytes.extend_from_slice(&length.to_le_bytes());
        bytes.extend_from_slice(&[0, 0]);
        bytes.extend_from_slice(payload);
    }

    /// An eraE object: the version record, each block's components in order,
    /// and a dynamic block index starting at `start`.
    fn erae_object(start: u64, blocks: &[Vec<([u8; 2], Vec<u8>)>]) -> Vec<u8> {
        let mut bytes = Vec::new();
        push_record(&mut bytes, VERSION, 0, &[]);
        let mut positions = Vec::new();
        for block in blocks {
            for (entry_type, payload) in block {
                positions.push(i64::try_from(bytes.len()).expect("position"));
                let length = u32::try_from(payload.len()).expect("length");
                push_record(&mut bytes, *entry_type, length, payload);
            }
        }
        let index_position = i64::try_from(bytes.len()).expect("index position");
        let components = u64::try_from(blocks[0].len()).expect("component count");
        DynamicBlockIndex::new(
            start,
            components,
            positions
                .iter()
                .map(|position| position - index_position)
                .collect(),
        )
        .to_entry()
        .write(&mut bytes)
        .expect("index record");
        bytes
    }

    /// A `file:` mirror holding era zero as `object`.
    fn era_zero_mirror(directory: &std::path::Path, object: &[u8]) -> EraeSource {
        std::fs::write(directory.join(ERA_ZERO), object).expect("write object");
        write_catalog(directory, &[ERA_ZERO]);
        file_source(directory)
    }

    /// One scripted HTTP/1.1 exchange: a response head, then `body` bytes
    /// streamed with chunked framing.
    struct Exchange {
        head: String,
        body: usize,
    }

    /// Serve `exchanges` in order, one connection each, on 127.0.0.1. The
    /// handle yields the request head read and the body bytes written for
    /// each exchange served.
    fn loopback_server(
        exchanges: Vec<Exchange>,
    ) -> (Url, std::thread::JoinHandle<Vec<(String, usize)>>) {
        use std::io::Write as _;

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("loopback listener");
        listener
            .set_nonblocking(true)
            .expect("non-blocking listener");
        let address = listener.local_addr().expect("listener address");
        let served = std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut written = Vec::new();
            for exchange in exchanges {
                let mut stream = loop {
                    match listener.accept() {
                        Ok((stream, _)) => break stream,
                        Err(error)
                            if error.kind() == std::io::ErrorKind::WouldBlock
                                && Instant::now() < deadline =>
                        {
                            std::thread::sleep(Duration::from_millis(5));
                        }
                        Err(_) => return written,
                    }
                };
                stream.set_nonblocking(false).expect("blocking stream");
                stream
                    .set_write_timeout(Some(Duration::from_secs(5)))
                    .expect("write timeout");
                let mut request = Vec::new();
                let mut buffer = [0_u8; 1_024];
                while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                    match stream.read(&mut buffer) {
                        Ok(0) | Err(_) => break,
                        Ok(read) => request.extend_from_slice(&buffer[..read]),
                    }
                }
                let _ = stream.write_all(exchange.head.as_bytes());
                let chunk = vec![b'x'; 65_536];
                let mut sent = 0;
                while sent < exchange.body {
                    let length = chunk.len().min(exchange.body - sent);
                    let framed = stream
                        .write_all(format!("{length:x}\r\n").as_bytes())
                        .and_then(|()| stream.write_all(&chunk[..length]))
                        .and_then(|()| stream.write_all(b"\r\n"));
                    if framed.is_err() {
                        break;
                    }
                    sent += length;
                }
                if exchange.body > 0 {
                    let _ = stream.write_all(b"0\r\n\r\n");
                }
                written.push((String::from_utf8_lossy(&request).into_owned(), sent));
            }
            written
        });
        (
            Url::parse(&format!("http://{address}/")).expect("loopback URL"),
            served,
        )
    }

    fn fixed_response(status: &str, body: &str) -> Exchange {
        Exchange {
            head: format!(
                "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            ),
            body: 0,
        }
    }

    fn streamed_response(status: &str, body: usize) -> Exchange {
        Exchange {
            head: format!(
                "HTTP/1.1 {status}\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
            ),
            body,
        }
    }

    #[test]
    fn frames_label_self_computed_hashes_and_unverified_digests_honestly() {
        let source =
            EraeSource::new(EraeConfig::public_mainnet().expect("config")).expect("source");
        let objects =
            parse_catalog(&source.config, catalog_text(&[ERA_ZERO]).as_bytes()).expect("catalog");
        let pre_shanghai = Header {
            number: 1,
            ..Header::default()
        };
        let frame = normalize_block(
            &source,
            &objects[0],
            &pre_shanghai,
            &BlockBody::default(),
            None,
            0,
            false,
        )
        .expect("frame");
        // Audit M-H3: the hash is computed from the archived header itself,
        // and a pre-Shanghai block has no withdrawals root to check.
        assert_eq!(
            frame.verification.header_hash.status,
            CheckStatus::NotChecked
        );
        assert_eq!(
            frame.verification.withdrawals_root.status,
            CheckStatus::Unavailable
        );
        assert_eq!(
            frame.verification.transactions_root,
            VerificationCheck::VERIFIED
        );
        let identity = frame.provenance[0]
            .object
            .as_ref()
            .expect("object identity");
        assert_eq!(
            identity.checksum, None,
            "sparse reads never recompute the catalog digest"
        );

        let post_shanghai = Header {
            number: 2,
            withdrawals_root: Some(alloy_consensus::EMPTY_ROOT_HASH),
            ..Header::default()
        };
        let body = BlockBody {
            withdrawals: Some(alloy_eips::eip4895::Withdrawals::default()),
            ..BlockBody::default()
        };
        let frame = normalize_block(&source, &objects[0], &post_shanghai, &body, None, 0, true)
            .expect("frame");
        assert_eq!(
            frame.verification.withdrawals_root,
            VerificationCheck::VERIFIED
        );
        assert_eq!(
            frame.verification.header_hash.status,
            CheckStatus::NotChecked
        );
    }

    #[test]
    fn plain_http_mirrors_are_refused_off_loopback() {
        let mut config = EraeConfig::public_mainnet().expect("config");
        config.base_url = with_credentials("http://mirror.example/erae/");
        let error = EraeSource::new(config.clone()).expect_err("plain http off loopback");
        assert!(!error.to_string().contains("hunter2"), "{error}");
        // Review M3: the opt-in is a history source setting, which a probe
        // cannot pass.
        assert!(error.to_string().contains("[[sources.history]]"), "{error}");
        for loopback in [
            "http://127.0.0.1:8080/erae/",
            "http://localhost/erae/",
            "http://[::1]/erae/",
        ] {
            config.base_url = Url::parse(loopback).expect("URL");
            EraeSource::new(config.clone()).unwrap_or_else(|error| panic!("{loopback}: {error}"));
        }
    }

    #[test]
    fn plain_http_mirrors_are_accepted_with_an_explicit_opt_in() {
        let mut config = EraeConfig::public_mainnet().expect("config");
        config.base_url = with_credentials("http://mirror.example/erae/");
        assert!(EraeSource::new(config.clone()).is_err());
        config.allow_insecure_http = true;
        let source = EraeSource::new(config).expect("an opted-in plain http mirror");
        let debug = format!("{source:?}");
        assert!(!debug.contains("hunter2"), "{debug}");
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn malformed_archive_structures_fail_closed_without_panicking() {
        // e2store records: the declared payload must fit the bytes already
        // read, and is borrowed from them.
        let record = |length: u32, reserved: [u8; 2], extent: usize| {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&COMPRESSED_HEADER);
            bytes.extend_from_slice(&length.to_le_bytes());
            bytes.extend_from_slice(&reserved);
            bytes.resize(extent.max(8), 0xee);
            bytes.truncate(extent);
            bytes
        };
        for (name, extent) in [
            ("an empty extent", Vec::new()),
            ("a truncated header", record(0, [0, 0], 7)),
            ("a payload past the extent", record(9, [0, 0], 16)),
            ("the largest length", record(u32::MAX, [0, 0], 64)),
            ("non-zero reserved bytes", record(0, [1, 0], 8)),
        ] {
            assert!(e2store_record(&extent).is_err(), "{name}");
        }
        for (length, extent) in [(0, 8), (8, 16), (4, 16)] {
            let bytes = record(length, [0, 0], extent);
            let (entry_type, payload) = e2store_record(&bytes).expect("a record within its extent");
            assert_eq!(entry_type, COMPRESSED_HEADER);
            assert_eq!(payload.len(), usize::try_from(length).expect("length"));
            assert!(
                payload.is_empty() || bytes.as_ptr_range().contains(&payload.as_ptr()),
                "the payload is borrowed from the extent"
            );
        }

        // Index trailers: component count, block count, and the index extent.
        let trailer =
            |components: u64, count: u64| [components.to_le_bytes(), count.to_le_bytes()].concat();
        for (name, file_size, bytes) in [
            ("an object shorter than a minimal index", 31, trailer(2, 1)),
            ("one component", 1 << 20, trailer(1, 1)),
            ("six components", 1 << 20, trailer(6, 1)),
            ("the largest component count", 1 << 20, trailer(u64::MAX, 1)),
            ("no blocks", 1 << 20, trailer(2, 0)),
            ("more blocks than an era", 1 << 20, trailer(2, 8_193)),
            ("the largest block count", 1 << 20, trailer(5, u64::MAX)),
            ("an index larger than its object", 100, trailer(5, 8_192)),
            ("a short trailer", 1 << 20, vec![2; 15]),
        ] {
            assert!(index_layout(file_size, &bytes).is_err(), "{name}");
        }
        let layout = index_layout(1 << 20, &trailer(3, 8_192)).expect("a full era index");
        assert_eq!(layout.entry_bytes, 8 + 8 + 3 * 8 * 8_192 + 16);
        assert_eq!(layout.position, (1 << 20) - layout.entry_bytes);

        // Offsets: every component lies after the version record and before
        // the index, once.
        let object_index = |index_position: u64, offsets: Vec<i64>| ObjectIndex {
            index_position,
            index: DynamicBlockIndex::new(0, 2, offsets),
            component_types: vec![COMPRESSED_HEADER, COMPRESSED_BODY],
            initial_input_bytes: 0,
        };
        for (name, index_position, offsets) in [
            ("inside the version record", 100, vec![-96, -50]),
            ("at the index", 100, vec![-50, 0]),
            ("past the object", 100, vec![-50, 10]),
            ("a duplicate position", 100, vec![-50, -50]),
            ("before the object", 100, vec![-101, -50]),
            ("an overflowing offset", u64::MAX, vec![i64::MAX, -8]),
            ("the most negative offset", 100, vec![i64::MIN, -50]),
        ] {
            assert!(
                sorted_positions(&object_index(index_position, offsets)).is_err(),
                "{name}"
            );
        }
        assert_eq!(
            resolve_offset(u64::MAX, i64::MIN).expect("in range"),
            u64::MAX - (1 << 63)
        );

        // Targets must be known component positions with a non-empty extent.
        assert!(
            coalesce_target_intervals(&BTreeMap::from([(50, COMPRESSED_HEADER)]), &[8, 60], 100)
                .is_err(),
            "an unknown target"
        );
        assert!(
            coalesce_target_intervals(&BTreeMap::from([(60, COMPRESSED_HEADER)]), &[8, 60], 60)
                .is_err(),
            "an empty extent"
        );
        for (name, layout) in [
            ("no body", vec![COMPRESSED_HEADER]),
            ("body first", vec![COMPRESSED_BODY, COMPRESSED_HEADER]),
            (
                "a repeated component",
                vec![COMPRESSED_HEADER, COMPRESSED_BODY, COMPRESSED_BODY],
            ),
        ] {
            assert!(validate_component_layout(&layout).is_err(), "{name}");
        }

        // Compressed components stop at their cap.
        assert!(decompress_component(b"not framed snappy", 0, 1 << 20).is_err());
        let bomb =
            reth_era::common::compression::snappy_compress(&vec![0; 1 << 20]).expect("compress");
        assert!(matches!(
            decompress_component(&bomb, 0, 64 << 10),
            Err(SourceError::BudgetExceeded {
                resource: "decompressed_bytes",
                ..
            })
        ));
        assert_eq!(
            decompress_component(&bomb, 0, 1 << 20)
                .expect("within the cap")
                .len(),
            1 << 20
        );

        // Catalog lines.
        let config = EraeConfig::public_mainnet().expect("config");
        for (name, catalog) in [
            ("not UTF-8", b"\xff\xfe".to_vec()),
            ("empty", Vec::new()),
            (
                "an extra field",
                format!("{CATALOG_DIGEST}  {ERA_ZERO} extra\n").into_bytes(),
            ),
            ("a short digest", format!("ee0b  {ERA_ZERO}\n").into_bytes()),
            (
                "a parent path",
                format!("{CATALOG_DIGEST}  ../{ERA_ZERO}\n").into_bytes(),
            ),
            (
                "a nested path",
                format!("{CATALOG_DIGEST}  nested/{ERA_ZERO}\n").into_bytes(),
            ),
            (
                "another network",
                format!("{CATALOG_DIGEST}  sepolia-00000-a6860fef.erae\n").into_bytes(),
            ),
            (
                "a four-digit era",
                format!("{CATALOG_DIGEST}  mainnet-0000-a6860fef.erae\n").into_bytes(),
            ),
            (
                "a non-hex hash",
                format!("{CATALOG_DIGEST}  mainnet-00000-zzzzzzzz.erae\n").into_bytes(),
            ),
            (
                "a duplicate era",
                catalog_text(&[ERA_ZERO, "mainnet-00000-a6860fef.ere"]).into_bytes(),
            ),
        ] {
            assert!(parse_catalog(&config, &catalog).is_err(), "{name}");
        }
    }

    #[test]
    fn provenance_does_not_reveal_mirror_credentials() {
        let mut config = EraeConfig::public_mainnet().expect("config");
        config.base_url = with_credentials("https://mirror.example/token-hunter3/?key=hunter4");
        let source = EraeSource::new(config).expect("source");
        let objects =
            parse_catalog(&source.config, catalog_text(&[ERA_ZERO]).as_bytes()).expect("catalog");
        let frame = normalize_block(
            &source,
            &objects[0],
            &Header::default(),
            &BlockBody::default(),
            None,
            0,
            false,
        )
        .expect("frame");
        let locator = &frame.provenance[0]
            .object
            .as_ref()
            .expect("object identity")
            .locator;
        for secret in SECRETS {
            assert!(!locator.contains(secret), "{locator}");
        }
        assert!(locator.starts_with("https://mirror.example"), "{locator}");
        assert!(locator.ends_with(ERA_ZERO), "{locator}");
    }

    #[tokio::test]
    async fn transport_errors_do_not_reveal_mirror_credentials() {
        let mut config = EraeConfig::public_mainnet().expect("config");
        // Nothing listens on the loopback discard port.
        config.base_url = with_credentials("http://127.0.0.1:9/token-hunter3/?key=hunter4");
        config.request_timeout = Duration::from_secs(2);
        let source = EraeSource::new(config).expect("loopback mirror");
        let error = source
            .plan(&request(range(0, 0), headers_and_bodies()))
            .await
            .expect_err("no mirror listens");
        for secret in SECRETS {
            assert!(!error.to_string().contains(secret), "{error}");
        }
    }

    #[tokio::test]
    async fn mirrors_are_not_followed_through_redirects() {
        let (target, contacted) =
            loopback_server(vec![fixed_response("200 OK", &catalog_text(&[ERA_ZERO]))]);
        let (origin, _served) = loopback_server(vec![Exchange {
            head: format!(
                "HTTP/1.1 302 Found\r\nLocation: {target}checksums_sha256.txt\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            ),
            body: 0,
        }]);
        let mut config = EraeConfig::public_mainnet().expect("config");
        config.base_url = origin;
        let source = EraeSource::new(config).expect("loopback mirror");
        let error = source
            .plan(&request(range(0, 0), headers_and_bodies()))
            .await
            .expect_err("a redirected catalog is refused");
        assert!(error.to_string().contains("302"), "{error}");
        assert!(
            contacted.join().expect("redirect target").is_empty(),
            "the redirect target was contacted"
        );
    }

    #[tokio::test]
    async fn catalog_bodies_are_capped_while_they_stream() {
        let body = 48 << 20;
        let (origin, served) = loopback_server(vec![streamed_response("200 OK", body)]);
        let mut config = EraeConfig::public_mainnet().expect("config");
        config.base_url = origin;
        let source = EraeSource::new(config).expect("loopback mirror");
        let error = source
            .plan(&request(range(0, 0), headers_and_bodies()))
            .await
            .expect_err("oversized catalog");
        assert!(
            matches!(
                error,
                SourceError::BudgetExceeded {
                    resource: "catalog_bytes",
                    ..
                }
            ),
            "{error}"
        );
        let written = served.join().expect("server");
        assert!(
            written[0].1 < 32 << 20,
            "the client read {} bytes of a catalog capped at {MAX_CATALOG_BYTES}",
            written[0].1
        );
    }

    #[tokio::test]
    async fn range_bodies_are_capped_while_they_stream() {
        let body = 32 << 20;
        let (origin, served) = loopback_server(vec![
            fixed_response("200 OK", &catalog_text(&[ERA_ZERO])),
            // The object's HEAD.
            Exchange {
                head: "HTTP/1.1 200 OK\r\nContent-Length: 4096\r\nConnection: close\r\n\r\n"
                    .to_owned(),
                body: 0,
            },
            streamed_response("206 Partial Content", body),
        ]);
        let mut config = EraeConfig::public_mainnet().expect("config");
        config.base_url = origin;
        let source = EraeSource::new(config).expect("loopback mirror");
        let plan = source
            .plan(&request(range(0, 0), headers_and_bodies()))
            .await
            .expect("plan");
        let Err(error) = source
            .open(
                &plan.chunks[0],
                budget(64 << 20, 1 << 20),
                CancellationToken::new(),
            )
            .await
        else {
            panic!("a range response longer than requested was accepted");
        };
        assert!(matches!(error, SourceError::Protocol(_)), "{error}");
        let written = served.join().expect("server");
        assert!(
            written[2].1 < 16 << 20,
            "the client read {} bytes for a 16-byte range",
            written[2].1
        );
    }

    #[tokio::test]
    async fn sparse_reads_check_the_budget_before_downloading() {
        let directory = tempfile::tempdir().expect("directory");
        let object = erae_object(
            0,
            &[vec![
                (COMPRESSED_HEADER, vec![0xab; 65_536]),
                (COMPRESSED_BODY, vec![0xcd; 65_536]),
            ]],
        );
        let source = era_zero_mirror(directory.path(), &object);
        let plan = source
            .plan(&request(range(0, 0), headers_and_bodies()))
            .await
            .expect("plan");
        let mut stream = source
            .open(
                &plan.chunks[0],
                budget(4_096, 1 << 20),
                CancellationToken::new(),
            )
            .await
            .expect("the index fits the budget");
        let error = stream
            .next()
            .await
            .expect("one result")
            .expect_err("the components exceed the budget");
        assert!(
            matches!(
                error,
                SourceError::BudgetExceeded {
                    resource: "input_bytes",
                    ..
                }
            ),
            "{error}"
        );
        // Audit M-H4: the batch was downloaded before its budget was checked.
        let fetched = source
            .acquisition_metrics()
            .and_then(|metrics| metrics.fetched_bytes)
            .expect("fetched bytes");
        assert!(
            fetched <= 4_096,
            "{fetched} bytes were downloaded under a 4,096-byte budget"
        );
    }

    #[tokio::test]
    async fn component_decompression_stops_at_its_cap() {
        let directory = tempfile::tempdir().expect("directory");
        let bomb =
            reth_era::common::compression::snappy_compress(&vec![0; 4 << 20]).expect("compress");
        let object = erae_object(
            0,
            &[vec![
                (COMPRESSED_HEADER, bomb),
                (COMPRESSED_BODY, vec![0; 16]),
            ]],
        );
        let source = era_zero_mirror(directory.path(), &object);
        let plan = source
            .plan(&request(range(0, 0), headers_and_bodies()))
            .await
            .expect("plan");
        let mut stream = source
            .open(
                &plan.chunks[0],
                budget(64 << 20, 64 << 10),
                CancellationToken::new(),
            )
            .await
            .expect("open");
        let error = stream
            .next()
            .await
            .expect("one result")
            .expect_err("a decompression bomb");
        assert!(
            matches!(
                error,
                SourceError::BudgetExceeded {
                    resource: "decompressed_bytes",
                    ..
                }
            ),
            "{error}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn the_catalog_refreshes_after_a_miss_and_after_its_lifetime() {
        let directory = tempfile::tempdir().expect("directory");
        write_catalog(directory.path(), &[ERA_ZERO]);
        let source = file_source(directory.path());
        let catalog_reads = || {
            source
                .acquisition_metrics()
                .and_then(|metrics| metrics.physical_reads)
                .unwrap_or(0)
        };
        let next_era = request(range(8_192, 8_192), headers_and_bodies());
        assert!(matches!(
            source.plan(&next_era).await,
            Err(SourceError::MissingRange(_))
        ));
        // The mirror publishes the next era. A miss moments later does not
        // refetch the catalog.
        write_catalog(directory.path(), &[ERA_ZERO, "mainnet-00001-05c64fc4.erae"]);
        assert!(matches!(
            source.plan(&next_era).await,
            Err(SourceError::MissingRange(_))
        ));
        // Audit M-H5: the catalog was cached for the life of the source.
        tokio::time::advance(Duration::from_mins(1)).await;
        source
            .plan(&next_era)
            .await
            .expect("a miss refreshes a catalog older than the miss interval");

        let reads = catalog_reads();
        let genesis = request(range(0, 0), headers_and_bodies());
        source.plan(&genesis).await.expect("cached plan");
        assert_eq!(catalog_reads(), reads, "a fresh catalog is reused");
        tokio::time::advance(Duration::from_mins(11)).await;
        source.plan(&genesis).await.expect("refreshed plan");
        assert_eq!(
            catalog_reads(),
            reads + 1,
            "an expired catalog is refetched"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn opening_a_planned_chunk_keeps_the_catalog_it_was_planned_from() {
        let directory = tempfile::tempdir().expect("directory");
        let object = erae_object(
            0,
            &[vec![
                (COMPRESSED_HEADER, vec![0xab; 64]),
                (COMPRESSED_BODY, vec![0xcd; 64]),
            ]],
        );
        let source = era_zero_mirror(directory.path(), &object);
        let plan = source
            .plan(&request(range(0, 0), headers_and_bodies()))
            .await
            .expect("plan");
        // The catalog endpoint fails once the cached catalog has expired.
        std::fs::remove_file(directory.path().join("checksums_sha256.txt"))
            .expect("remove catalog");
        tokio::time::advance(Duration::from_mins(11)).await;
        // Review M2: open refetched the expired catalog and failed the chunk.
        let opened = source
            .open(
                &plan.chunks[0],
                budget(1 << 20, 1 << 20),
                CancellationToken::new(),
            )
            .await;
        assert!(opened.is_ok(), "{:?}", opened.err());
    }

    #[tokio::test]
    async fn pre_byzantium_receipts_and_logs_are_refused_at_plan_time() {
        let directory = tempfile::tempdir().expect("directory");
        write_catalog(directory.path(), &[ERA_ZERO, "mainnet-00533-00000000.erae"]);
        let source = file_source(directory.path());
        for required in [
            CapabilitySet::of(Capability::Receipts),
            CapabilitySet::of(Capability::Logs),
            headers_and_bodies().with(Capability::Receipts),
        ] {
            // Audit M-H6: planning succeeded, and every open then failed.
            let error = source
                .plan(&request(range(4_369_999, 4_370_001), required))
                .await
                .expect_err("pre-Byzantium receipts");
            assert!(
                matches!(&error, SourceError::InvalidPlan(detail) if detail.contains("Byzantium")),
                "{error}"
            );
        }
        source
            .plan(&request(range(0, 10), headers_and_bodies()))
            .await
            .expect("headers and bodies from genesis");
        source
            .plan(&request(
                range(4_370_000, 4_370_010),
                CapabilitySet::of(Capability::Receipts),
            ))
            .await
            .expect("receipts from Byzantium");
    }

    #[tokio::test(start_paused = true)]
    async fn catalog_refetches_back_off_while_a_range_stays_missing() {
        let directory = tempfile::tempdir().expect("directory");
        write_catalog(directory.path(), &[ERA_ZERO]);
        let source = file_source(directory.path());
        let catalog_reads = || {
            source
                .acquisition_metrics()
                .and_then(|metrics| metrics.physical_reads)
                .unwrap_or(0)
        };
        let era_one = request(range(8_192, 8_192), headers_and_bodies());
        assert!(source.plan(&era_one).await.is_err());
        let mut reads = catalog_reads();
        let mut refetched = Vec::new();
        for second in (30..=2_160).step_by(30) {
            tokio::time::advance(Duration::from_secs(30)).await;
            assert!(matches!(
                source.plan(&era_one).await,
                Err(SourceError::MissingRange(_))
            ));
            if catalog_reads() > reads {
                reads = catalog_reads();
                refetched.push(second);
            }
        }
        // Review 2: a range that stayed missing refetched the catalog every
        // 30 seconds.
        assert_eq!(refetched, [30, 90, 210, 450, 930, 1_530, 2_130]);
        // The mirror publishes era one, which the catalog's next refetch
        // finds within its lifetime.
        write_catalog(directory.path(), &[ERA_ZERO, "mainnet-00001-05c64fc4.erae"]);
        let mut waited = 0;
        while source.plan(&era_one).await.is_err() {
            tokio::time::advance(Duration::from_secs(30)).await;
            waited += 30;
            assert!(waited <= 600, "era one was not seen within 10 minutes");
        }
        // New eras start the backoff over.
        let era_two = request(range(16_384, 16_384), headers_and_bodies());
        let reads = catalog_reads();
        assert!(source.plan(&era_two).await.is_err());
        tokio::time::advance(Duration::from_secs(30)).await;
        assert!(source.plan(&era_two).await.is_err());
        assert_eq!(catalog_reads(), reads + 1);
    }

    #[tokio::test]
    async fn an_unchanged_catalog_is_revalidated_instead_of_downloaded() {
        let catalog = catalog_text(&[ERA_ZERO]);
        let (origin, served) = loopback_server(vec![
            Exchange {
                head: format!(
                    "HTTP/1.1 200 OK\r\nETag: \"v1\"\r\nLast-Modified: Wed, 01 Jul 2026 00:00:00 GMT\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{catalog}",
                    catalog.len()
                ),
                body: 0,
            },
            Exchange {
                head: "HTTP/1.1 304 Not Modified\r\nETag: \"v1\"\r\nConnection: close\r\n\r\n"
                    .to_owned(),
                body: 0,
            },
        ]);
        let mut config = EraeConfig::public_mainnet().expect("config");
        config.base_url = origin;
        let source = EraeSource::new(config).expect("loopback mirror");
        let fetched = source.catalog(Some(Duration::ZERO)).await.expect("catalog");
        // Review 2: every refresh downloaded the whole catalog again.
        let revalidated = source
            .catalog(Some(Duration::ZERO))
            .await
            .expect("a 304 keeps the cached catalog");
        assert_eq!(revalidated.len(), fetched.len());
        let requests = served.join().expect("server");
        let revalidation = requests[1].0.to_ascii_lowercase();
        assert!(
            revalidation.contains("if-none-match: \"v1\"")
                && revalidation.contains("if-modified-since: wed, 01 jul 2026 00:00:00 gmt"),
            "{revalidation}"
        );
    }

    /// A `file:` mirror holding era zero as one valid block with a
    /// withdrawal, named after the block's hash, and the sizes of its
    /// decompressed header and body.
    fn one_block_mirror(directory: &std::path::Path) -> (EraeSource, u64, u64) {
        let body = BlockBody {
            withdrawals: Some(alloy_eips::eip4895::Withdrawals::new(vec![
                alloy_eips::eip4895::Withdrawal {
                    index: 0,
                    validator_index: 1,
                    address: AlloyAddress::ZERO,
                    amount: 1,
                },
            ])),
            ..BlockBody::default()
        };
        let header = Header {
            base_fee_per_gas: Some(1),
            withdrawals_root: body.calculate_withdrawals_root(),
            ..Header::default()
        };
        let header_rlp = alloy_rlp::encode(&header);
        let body_rlp = alloy_rlp::encode(&body);
        let compressed =
            |bytes: &[u8]| reth_era::common::compression::snappy_compress(bytes).expect("compress");
        let object = erae_object(
            0,
            &[vec![
                (COMPRESSED_HEADER, compressed(&header_rlp)),
                (COMPRESSED_BODY, compressed(&body_rlp)),
            ]],
        );
        let name = format!(
            "mainnet-00000-{}.erae",
            hex::encode(&header.hash_slow().as_slice()[..4])
        );
        std::fs::write(directory.join(&name), object).expect("write object");
        write_catalog(directory, &[&name]);
        (
            file_source(directory),
            u64::try_from(header_rlp.len()).expect("header size"),
            u64::try_from(body_rlp.len()).expect("body size"),
        )
    }

    #[tokio::test]
    async fn a_normalized_frame_must_fit_the_frame_budget() {
        let directory = tempfile::tempdir().expect("directory");
        let (source, header_bytes, body_bytes) = one_block_mirror(directory.path());
        let object = source.catalog(None).await.expect("catalog")[0].clone();
        let cancellation = CancellationToken::new();
        // The decompressed header and body fit exactly; the frame, which
        // also holds the decoded withdrawal, does not.
        let budget = budget(1 << 20, header_bytes + body_bytes);
        let index = source
            .load_index(&object, budget, &cancellation)
            .await
            .expect("index");
        let result = read_sparse_batch(
            &source,
            &object,
            &index,
            range(0, 0),
            headers_and_bodies(),
            budget,
            index.initial_input_bytes,
            &cancellation,
        )
        .await;
        // External review F5: the frame was queued above the frame budget.
        match result {
            Err(SourceError::BudgetExceeded {
                resource: "frame_bytes",
                limit,
                observed,
            }) => assert!(observed > limit && limit == header_bytes + body_bytes),
            Err(error) => panic!("unexpected error: {error}"),
            Ok((frames, _)) => panic!(
                "a frame of {} bytes passed a frame budget of {}",
                frames[0].estimated_heap_bytes(),
                header_bytes + body_bytes
            ),
        }
    }

    /// The source budget's counters, each tripped at the source's own
    /// boundary: bytes acquired, bytes decoded, and bytes of each frame
    /// emitted.
    #[tokio::test]
    async fn budget_contract_names_each_exceeded_counter() {
        let directory = tempfile::tempdir().expect("directory");
        let (source, header_bytes, body_bytes) = one_block_mirror(directory.path());
        let plan = source
            .plan(&request(range(0, 0), headers_and_bodies()))
            .await
            .expect("plan");
        let decoded = header_bytes + body_bytes;
        for (counter, budget, expected) in [
            ("none", budget(1 << 20, 1 << 20), None),
            ("acquired", budget(100, 1 << 20), Some("input_bytes")),
            (
                "decoded",
                budget(1 << 20, decoded - 1),
                Some("decompressed_bytes"),
            ),
            (
                "emitted frame",
                budget(1 << 20, decoded),
                Some("frame_bytes"),
            ),
        ] {
            let outcome: Result<Vec<BlockFrame>, SourceError> = match source
                .open(&plan.chunks[0], budget, CancellationToken::new())
                .await
            {
                Ok(stream) => stream.collect::<Vec<_>>().await.into_iter().collect(),
                Err(error) => Err(error),
            };
            match (outcome, expected) {
                (Ok(frames), None) => assert_eq!(frames.len(), 1, "{counter}"),
                (
                    Err(SourceError::BudgetExceeded {
                        resource,
                        limit,
                        observed,
                    }),
                    Some(expected),
                ) => {
                    assert_eq!(resource, expected, "{counter}");
                    assert!(observed > limit, "{counter}: {observed} <= {limit}");
                }
                (outcome, _) => panic!("{counter}: {:?}", outcome.map(|frames| frames.len())),
            }
        }
    }
}
