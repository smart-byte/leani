//! Verified Ethereum finality over the standard Beacon API.
//!
//! Beacon endpoints are treated only as untrusted transports. Checkpoint
//! bootstraps, sync-committee transitions, finality branches, BLS aggregate
//! signatures, and execution-payload branches are verified locally by the
//! pinned Helios consensus-core implementation.

use std::{
    collections::BTreeMap,
    fmt,
    ops::Range,
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use alloy_primitives::{B256, FixedBytes};
use async_trait::async_trait;
use futures::{StreamExt, stream, stream::FuturesUnordered};
use helios_consensus_core::{
    apply_bootstrap, apply_finality_update, apply_update, calc_sync_period,
    consensus_spec::{ConsensusSpec, MainnetConsensusSpec},
    errors::ConsensusError,
    get_bits,
    types::{Bootstrap, FinalityUpdate, Fork, Forks, LightClientStore, OptimisticUpdate, Update},
    verify_bootstrap, verify_finality_update, verify_optimistic_update, verify_update,
};
use leani_primitives::{
    BlockHash, BlockNumber, Capability, CapabilitySet, ChainId, SourceId, SourceKind, TrustModel,
};
use leani_source_api::{
    AttestedHead, AttestedHeadPublisher, ConsensusCheckpoint, FinalityEvent, FinalityEventStream,
    FinalityModel, FinalitySource, Partitioning, SourceDescriptor, SourceError,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use tree_hash::TreeHash;
use url::Url;

mod anchor;
mod http;

pub use anchor::{
    AnchorFile, AnchorWriter, CheckpointOrigin, FINALITY_ANCHOR_FILE, PersistedFinalityAnchor,
    StartAnchor, TrustedCheckpoint, is_bootstrap_anchor, persist_finality_anchor,
    read_finality_anchor, resolve_start_anchor, slot_unix_seconds,
};
use http::{BeaconTransport, HttpTransport};
pub use http::{
    endpoint_labels, normalized_endpoint, read_response, redacted_url, request_label, request_url,
};

pub const MAINNET_GENESIS_TIME: u64 = 1_606_824_023;
pub const MAINNET_GENESIS_ROOT: B256 =
    alloy_primitives::b256!("4b363db94e286120d76eb905340fdd4e54bfe9f06bf33ff6cf5ad27f511bfe95");
const MAX_UPDATES_PER_REQUEST: u64 = 128;
const SLOT_SECONDS: u64 = 12;
const SLOTS_PER_EPOCH: u64 = 32;
/// Slots an update may be signed ahead of the local clock.
const CLOCK_SKEW_SLOTS: u64 = 1;
/// How long a poll waits after the first verified endpoint for the others.
const AGREEMENT_GRACE: Duration = Duration::from_secs(2);
const FULU_FORK_EPOCH: u64 = 411_392;
const ELECTRA_FORK_EPOCH: u64 = 364_032;
const MAX_BLOBS_PER_BLOCK_ELECTRA: u64 = 9;
const MAINNET_BLOB_SCHEDULE: [(u64, u64); 2] = [(412_672, 15), (419_072, 21)];
pub const DEFAULT_MAX_CHECKPOINT_AGE: Duration = Duration::from_hours(336);
/// Members of the Ethereum mainnet sync committee.
const SYNC_COMMITTEE_SIZE: u64 = 512;
/// Sync-committee signatures an optimistic update needs before its attested
/// header anchors the live lane: at least two thirds of the committee.
pub const SYNC_COMMITTEE_SUPERMAJORITY: u64 = (2 * SYNC_COMMITTEE_SIZE).div_ceil(3);
const OPTIMISTIC_UPDATE_PATH: &str = "eth/v1/beacon/light_client/optimistic_update";

/// Immutable Helios revision used by this adapter.
pub const HELIOS_REVISION: &str = "204c998a927348e1c000a664f08d5b37b1b0d924";

/// Compute the four-byte mainnet fork digest for a beacon slot.
#[must_use]
pub fn mainnet_fork_digest(slot: u64) -> [u8; 4] {
    let epoch = slot / 32;
    let version = if epoch >= FULU_FORK_EPOCH {
        [0x06, 0, 0, 0]
    } else if epoch >= ELECTRA_FORK_EPOCH {
        [0x05, 0, 0, 0]
    } else if epoch >= 269_568 {
        [0x04, 0, 0, 0]
    } else if epoch >= 194_048 {
        [0x03, 0, 0, 0]
    } else if epoch >= 144_896 {
        [0x02, 0, 0, 0]
    } else if epoch >= 74_240 {
        [0x01, 0, 0, 0]
    } else {
        [0; 4]
    };
    let mut fork_data = [0_u8; 64];
    fork_data[..4].copy_from_slice(&version);
    fork_data[32..].copy_from_slice(MAINNET_GENESIS_ROOT.as_slice());
    let mut digest = Sha256::digest(fork_data);
    if epoch >= FULU_FORK_EPOCH {
        // EIP-7892 folds the active blob parameters into the P2P digest so
        // blob-parameter-only forks separate gossip and req/resp traffic
        // without changing the consensus fork version.
        let (parameter_epoch, maximum_blobs) = MAINNET_BLOB_SCHEDULE
            .iter()
            .rev()
            .copied()
            .find(|(activation, _)| epoch >= *activation)
            .unwrap_or((ELECTRA_FORK_EPOCH, MAX_BLOBS_PER_BLOCK_ELECTRA));
        let mut parameters = [0_u8; 16];
        parameters[..8].copy_from_slice(&parameter_epoch.to_le_bytes());
        parameters[8..].copy_from_slice(&maximum_blobs.to_le_bytes());
        let mask = Sha256::digest(parameters);
        for (byte, mask) in digest.iter_mut().zip(mask) {
            *byte ^= mask;
        }
    }
    [digest[0], digest[1], digest[2], digest[3]]
}

/// Parse a `0x`-prefixed 32-byte checkpoint root.
///
/// # Errors
///
/// Returns an error when the value is not exactly 32 bytes of hexadecimal.
pub fn parse_checkpoint_root(value: &str) -> Result<[u8; 32], BeaconApiError> {
    let encoded = value.strip_prefix("0x").ok_or_else(|| {
        BeaconApiError::InvalidConfig("checkpoint root must start with 0x".to_owned())
    })?;
    if encoded.len() != 64 {
        return Err(BeaconApiError::InvalidConfig(
            "checkpoint root must encode exactly 32 bytes".to_owned(),
        ));
    }
    let mut root = [0; 32];
    hex::decode_to_slice(encoded, &mut root).map_err(|_| {
        BeaconApiError::InvalidConfig("checkpoint root contains invalid hexadecimal".to_owned())
    })?;
    Ok(root)
}

/// Wall clock for checkpoint-age and signature-slot checks. Tests inject a
/// simulated time.
#[derive(Clone)]
pub struct Clock(Arc<dyn Fn() -> SystemTime + Send + Sync>);

impl Clock {
    #[must_use]
    pub fn new(now: impl Fn() -> SystemTime + Send + Sync + 'static) -> Self {
        Self(Arc::new(now))
    }

    #[must_use]
    pub fn now(&self) -> SystemTime {
        (self.0)()
    }
}

impl Default for Clock {
    fn default() -> Self {
        Self::new(SystemTime::now)
    }
}

impl fmt::Debug for Clock {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Clock")
    }
}

#[derive(Clone, Debug)]
pub struct BeaconApiConfig {
    pub endpoints: Vec<Url>,
    pub minimum_agreement: usize,
    pub request_timeout: Duration,
    pub poll_interval: Duration,
    pub max_checkpoint_age: Duration,
    /// Whether the newest verified anchor is read from, and persisted to, a
    /// file for restarts.
    pub anchor: AnchorFile,
}

impl BeaconApiConfig {
    #[must_use]
    pub fn mainnet(endpoints: Vec<Url>) -> Self {
        Self {
            endpoints,
            minimum_agreement: 1,
            request_timeout: Duration::from_secs(30),
            poll_interval: Duration::from_secs(SLOT_SECONDS),
            max_checkpoint_age: DEFAULT_MAX_CHECKPOINT_AGE,
            anchor: AnchorFile::Disabled,
        }
    }

    fn validate(&self) -> Result<(), BeaconApiError> {
        if self.endpoints.is_empty() {
            return Err(BeaconApiError::InvalidConfig(
                "at least one endpoint is required".to_owned(),
            ));
        }
        if self.minimum_agreement == 0 || self.minimum_agreement > self.endpoints.len() {
            return Err(BeaconApiError::InvalidConfig(format!(
                "minimum agreement {} must be within 1..={}",
                self.minimum_agreement,
                self.endpoints.len()
            )));
        }
        if self.request_timeout.is_zero()
            || self.poll_interval.is_zero()
            || self.max_checkpoint_age.is_zero()
        {
            return Err(BeaconApiError::InvalidConfig(
                "timeouts and checkpoint age must be non-zero".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct VerifiedFinalityAnchor {
    pub beacon_slot: u64,
    pub beacon_block_root: [u8; 32],
    pub execution_block_number: u64,
    pub execution_block_hash: BlockHash,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EndpointProbe {
    /// Endpoint without userinfo or query string.
    pub endpoint: String,
    pub verified: bool,
    pub anchor: Option<VerifiedFinalityAnchor>,
    pub checkpoint_anchor: Option<VerifiedFinalityAnchor>,
    /// The head of the endpoint's verified optimistic update, if it served
    /// one that at least two thirds of the sync committee signed.
    #[serde(default)]
    pub attested_head: Option<AttestedHead>,
    /// Why the endpoint's finality verified but its optimistic update gave
    /// no attested head: it served none, it anchors none, or it failed
    /// verification.
    #[serde(default)]
    pub attested_head_error: Option<String>,
    pub updates_verified: u64,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FinalityProbeReport {
    pub verifier: String,
    pub checkpoint_root: [u8; 32],
    /// The verified bootstrap of `checkpoint_root`.
    pub checkpoint_anchor: Option<VerifiedFinalityAnchor>,
    pub minimum_agreement: usize,
    pub accepted: bool,
    pub selected: Option<VerifiedFinalityAnchor>,
    /// The newest attested head any endpoint verified, the one the live
    /// lane follows.
    #[serde(default)]
    pub attested_head: Option<AttestedHead>,
    pub agreeing_endpoints: usize,
    pub disagreements: Vec<String>,
    pub endpoints: Vec<EndpointProbe>,
}

/// Stateful verifier shared by every untrusted light-client transport.
#[derive(Clone, Debug)]
pub struct MainnetLightClientVerifier {
    checkpoint_root: B256,
    checkpoint_anchor: VerifiedFinalityAnchor,
    store: LightClientStore<MainnetConsensusSpec>,
    forks: Forks,
    updates_verified: u64,
}

impl MainnetLightClientVerifier {
    /// Verify a checkpoint bootstrap and initialize the mainnet light-client
    /// store.
    ///
    /// # Errors
    ///
    /// Rejects an invalid bootstrap, a checkpoint without an execution proof,
    /// or a checkpoint older than the configured weak-subjectivity bound.
    pub fn bootstrap(
        checkpoint_root: [u8; 32],
        bootstrap: &Bootstrap<MainnetConsensusSpec>,
        max_checkpoint_age: Duration,
        now: SystemTime,
    ) -> Result<Self, BeaconApiError> {
        let checkpoint = B256::from(checkpoint_root);
        let forks = mainnet_forks();
        verify_bootstrap(bootstrap, checkpoint, &forks)
            .map_err(|error| BeaconApiError::Verification(error.to_string()))?;
        let checkpoint_slot = bootstrap.header().beacon().slot;
        verify_checkpoint_age(checkpoint_slot, max_checkpoint_age, now)?;
        let checkpoint_anchor = anchor_from_header(bootstrap.header(), checkpoint.into())?;
        let mut store = LightClientStore::<MainnetConsensusSpec>::default();
        apply_bootstrap(&mut store, bootstrap);
        Ok(Self {
            checkpoint_root: checkpoint,
            checkpoint_anchor,
            store,
            forks,
            updates_verified: 0,
        })
    }

    #[must_use]
    pub fn checkpoint_root(&self) -> [u8; 32] {
        self.checkpoint_root.into()
    }

    #[must_use]
    pub fn checkpoint_anchor(&self) -> VerifiedFinalityAnchor {
        self.checkpoint_anchor
    }

    /// Return the execution anchor proven by the store's finalized header.
    ///
    /// # Errors
    ///
    /// Returns an error until the finalized header contains a valid execution
    /// payload proof.
    pub fn finalized_anchor(&self) -> Result<VerifiedFinalityAnchor, BeaconApiError> {
        let root: [u8; 32] = self.store.finalized_header.beacon().tree_hash_root().into();
        anchor_from_header(&self.store.finalized_header, root)
    }

    #[must_use]
    pub const fn updates_verified(&self) -> u64 {
        self.updates_verified
    }

    /// Sync-committee periods whose updates must be applied before an update
    /// signed at `signature_slot` verifies. A bootstrap supplies its own
    /// period's committee, and each period's update supplies the next one.
    #[must_use]
    pub fn update_periods_before(&self, signature_slot: u64) -> Range<u64> {
        let store_period =
            calc_sync_period::<MainnetConsensusSpec>(self.store.finalized_header.beacon().slot);
        let first =
            store_period.saturating_add(u64::from(self.store.next_sync_committee.is_some()));
        first..calc_sync_period::<MainnetConsensusSpec>(signature_slot).max(first)
    }

    /// Verify and apply one sync-committee-period update.
    ///
    /// # Errors
    ///
    /// Rejects invalid branches, signatures, fork transitions, and updates
    /// inconsistent with the current light-client store.
    pub fn apply_update(
        &mut self,
        update: &Update<MainnetConsensusSpec>,
        now: SystemTime,
    ) -> Result<(), BeaconApiError> {
        verify_update::<MainnetConsensusSpec>(
            update,
            verification_slot(now),
            &self.store,
            MAINNET_GENESIS_ROOT,
            &self.forks,
        )
        .map_err(|error| {
            update_error(
                error.downcast_ref(),
                error.to_string(),
                *update.signature_slot(),
                now,
            )
        })?;
        apply_update(&mut self.store, update);
        self.updates_verified = self.updates_verified.saturating_add(1);
        Ok(())
    }

    /// Verify and apply the latest finality update.
    ///
    /// # Errors
    ///
    /// Rejects invalid finality/execution branches, signatures, fork
    /// transitions, and updates inconsistent with the current store.
    pub fn apply_finality_update(
        &mut self,
        update: &FinalityUpdate<MainnetConsensusSpec>,
        now: SystemTime,
    ) -> Result<VerifiedFinalityAnchor, BeaconApiError> {
        verify_finality_update::<MainnetConsensusSpec>(
            update,
            verification_slot(now),
            &self.store,
            MAINNET_GENESIS_ROOT,
            &self.forks,
        )
        .map_err(|error| {
            update_error(
                error.downcast_ref(),
                error.to_string(),
                *update.signature_slot(),
                now,
            )
        })?;
        apply_finality_update(&mut self.store, update);
        self.updates_verified = self.updates_verified.saturating_add(1);
        self.finalized_anchor()
    }

    /// Verify an optimistic update and return the execution block its
    /// attested header commits to.
    ///
    /// The sync aggregate must carry at least
    /// [`SYNC_COMMITTEE_SUPERMAJORITY`] signatures. Helios then checks the
    /// aggregate signature over the attested header, with the fork version and
    /// domain of the signature slot, and the header's execution branch. The
    /// store is not changed: finality advances only through finality updates.
    ///
    /// # Errors
    ///
    /// Returns [`BeaconApiError::InsufficientParticipation`] for too few
    /// signatures, [`BeaconApiError::StaleUpdate`] for an update the store has
    /// passed or cannot verify yet, and a verification error otherwise.
    pub fn verify_attested_head(
        &self,
        update: &OptimisticUpdate<MainnetConsensusSpec>,
        now: SystemTime,
    ) -> Result<AttestedHead, BeaconApiError> {
        let participants =
            get_bits::<MainnetConsensusSpec>(&update.sync_aggregate.sync_committee_bits);
        if !has_sync_committee_supermajority(participants) {
            return Err(BeaconApiError::InsufficientParticipation {
                participants,
                required: SYNC_COMMITTEE_SUPERMAJORITY,
            });
        }
        verify_optimistic_update::<MainnetConsensusSpec>(
            update,
            verification_slot(now),
            &self.store,
            MAINNET_GENESIS_ROOT,
            &self.forks,
        )
        .map_err(|error| {
            update_error(
                error.downcast_ref(),
                error.to_string(),
                update.signature_slot,
                now,
            )
        })?;
        let root: [u8; 32] = update.attested_header.beacon().tree_hash_root().into();
        let attested = anchor_from_header(&update.attested_header, root)?;
        Ok(AttestedHead {
            beacon_slot: attested.beacon_slot,
            beacon_block_root: root,
            block_number: BlockNumber(attested.execution_block_number),
            block_hash: attested.execution_block_hash,
        })
    }
}

/// Whether `participants` of the sync committee are at least two thirds of
/// it, enough for their signature to anchor an attested head.
#[must_use]
pub const fn has_sync_committee_supermajority(participants: u64) -> bool {
    participants >= SYNC_COMMITTEE_SUPERMAJORITY
}

#[derive(Clone, Debug)]
pub struct VerifiedBeaconApi {
    config: BeaconApiConfig,
    /// Endpoints as reports and errors show them.
    labels: Vec<String>,
    descriptor: SourceDescriptor,
    transport: Arc<dyn BeaconTransport>,
    clock: Clock,
    verifiers: Arc<tokio::sync::Mutex<VerifierPool>>,
    anchor_writer: AnchorWriter,
    /// Where every accepted poll publishes its newest verified attested head.
    attested_heads: Option<AttestedHeadPublisher>,
}

/// One endpoint's light client.
#[derive(Debug)]
enum EndpointVerifier {
    /// Not bootstrapped; the next poll starts a bootstrap.
    Idle,
    /// A bootstrap in flight. It outlives a poll's grace cutoff and is
    /// collected by a later poll.
    Bootstrapping(tokio::task::JoinHandle<Result<MainnetLightClientVerifier, BeaconApiError>>),
    Ready(Box<MainnetLightClientVerifier>),
}

impl Drop for EndpointVerifier {
    fn drop(&mut self) {
        if let Self::Bootstrapping(task) = self {
            task.abort();
        }
    }
}

/// One light-client verifier per endpoint, bootstrapped once and advanced by
/// every poll, probe, and resubscription.
#[derive(Debug, Default)]
struct VerifierPool {
    /// The checkpoint the pool started for.
    checkpoint: Option<TrustedCheckpoint>,
    start: Option<StartAnchor>,
    /// The verified bootstrap of `start`.
    start_anchor: Option<VerifiedFinalityAnchor>,
    /// The newest agreed epoch-aligned anchor. An endpoint without a
    /// verifier bootstraps from it, so it can join after `start` ages out.
    newest_agreed: Option<VerifiedFinalityAnchor>,
    endpoints: Vec<EndpointVerifier>,
}

impl VerifierPool {
    /// Whether the verifiers already follow `root`, as their configured
    /// checkpoint or as their bootstrap root.
    fn follows(&self, root: [u8; 32]) -> bool {
        self.start
            .is_some_and(|start| start.root == root || start.checkpoint_root == root)
    }

    fn restart(&mut self, checkpoint: TrustedCheckpoint, start: StartAnchor, endpoints: usize) {
        self.checkpoint = Some(checkpoint);
        self.start = Some(start);
        self.start_anchor = None;
        self.newest_agreed = None;
        self.endpoints = (0..endpoints).map(|_| EndpointVerifier::Idle).collect();
    }

    fn any_ready(&self) -> bool {
        self.endpoints
            .iter()
            .any(|endpoint| matches!(endpoint, EndpointVerifier::Ready(_)))
    }
}

impl VerifiedBeaconApi {
    /// Construct a mainnet verifier. No endpoint is contacted here.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid quorum/time limits or an HTTP client that
    /// cannot be initialized.
    pub fn mainnet(config: BeaconApiConfig) -> Result<Self, BeaconApiError> {
        let client = http::client_builder(config.request_timeout)
            .build()
            .map_err(BeaconApiError::HttpClient)?;
        Self::with_transport(config, Arc::new(HttpTransport { client }), Clock::default())
    }

    fn with_transport(
        mut config: BeaconApiConfig,
        transport: Arc<dyn BeaconTransport>,
        clock: Clock,
    ) -> Result<Self, BeaconApiError> {
        config.validate()?;
        config.endpoints = config.endpoints.iter().map(normalized_endpoint).collect();
        let labels = endpoint_labels(&config.endpoints, "finality.endpoints");
        let source_id = SourceId::new("beacon-api-light-client")
            .map_err(|error| BeaconApiError::InvalidConfig(error.to_string()))?;
        Ok(Self {
            descriptor: SourceDescriptor {
                id: source_id,
                kind: SourceKind::BeaconApi,
                chain_id: ChainId(1),
                range: None,
                capabilities: CapabilitySet::of(Capability::ConsensusFinality),
                complete_capabilities: CapabilitySet::of(Capability::ConsensusFinality),
                trust: TrustModel::ProtocolVerified,
                finality: FinalityModel::Finalized,
                partitioning: Partitioning::None,
                expected_lag: Duration::from_secs(2 * SLOT_SECONDS),
                schema_version: format!("beacon-light-client.v1+helios.{HELIOS_REVISION}"),
                priority: 0,
            },
            anchor_writer: AnchorWriter::new(config.anchor.clone()),
            config,
            labels,
            transport,
            clock,
            verifiers: Arc::new(tokio::sync::Mutex::new(VerifierPool::default())),
            attested_heads: None,
        })
    }

    /// Publish the attested head of every accepted poll, each slot, to
    /// `heads`: the execution live lane includes no block above it.
    #[must_use]
    pub fn with_attested_heads(mut self, heads: AttestedHeadPublisher) -> Self {
        self.attested_heads = Some(heads);
        self
    }

    /// Verify all configured endpoints from a checkpoint, or from a newer
    /// persisted anchor that `resolve_start_anchor` accepts for it.
    pub async fn probe_root(&self, checkpoint: TrustedCheckpoint) -> FinalityProbeReport {
        self.start_and_poll(checkpoint).await.0
    }

    /// Follow `checkpoint` and poll every endpoint once. When no endpoint
    /// serves a bootstrap for a persisted start anchor, fall back to the
    /// checkpoint itself.
    async fn start_and_poll(
        &self,
        checkpoint: TrustedCheckpoint,
    ) -> (FinalityProbeReport, StartAnchor) {
        let mut pool = self.verifiers.lock().await;
        let mut start = self.follow(&mut pool, checkpoint);
        let mut report = self.poll(&mut pool).await;
        if !report.accepted && start.persisted && !pool.any_ready() {
            warn!(
                beacon_slot = start.slot,
                "no endpoint served a bootstrap for the persisted finality anchor; bootstrapping from the configured checkpoint"
            );
            let checkpoint = pool.checkpoint.unwrap_or(checkpoint);
            start = StartAnchor::configured(checkpoint);
            pool.restart(checkpoint, start, self.config.endpoints.len());
            report = self.poll(&mut pool).await;
        }
        drop(pool);
        self.persist(&report, start).await;
        (report, start)
    }

    /// The pool's start anchor, restarting it from `checkpoint` when it
    /// follows another trust root.
    fn follow(&self, pool: &mut VerifierPool, checkpoint: TrustedCheckpoint) -> StartAnchor {
        match pool.start {
            Some(start) if pool.follows(checkpoint.root) => start,
            _ => {
                let start = resolve_start_anchor(
                    self.config.anchor.path(),
                    checkpoint,
                    self.config.max_checkpoint_age,
                    self.clock.now(),
                );
                if start.persisted {
                    info!(
                        beacon_slot = start.slot,
                        "bootstrapping Beacon API finality from the persisted verified anchor"
                    );
                }
                pool.restart(checkpoint, start, self.config.endpoints.len());
                start
            }
        }
    }

    async fn poll_current(&self) -> FinalityProbeReport {
        let mut pool = self.verifiers.lock().await;
        let Some(start) = pool.start else {
            return select_endpoint_agreement([0; 32], self.config.minimum_agreement, Vec::new());
        };
        let report = self.poll(&mut pool).await;
        drop(pool);
        self.persist(&report, start).await;
        report
    }

    /// Persist an accepted anchor outside the pool lock.
    async fn persist(&self, report: &FinalityProbeReport, start: StartAnchor) {
        if let Some(selected) = report.selected.filter(|_| report.accepted) {
            self.anchor_writer
                .persist(selected, start.checkpoint_root)
                .await;
        }
    }

    /// Advance every endpoint's verifier once and select the agreed anchor.
    ///
    /// After the first verified endpoint, the others get a short grace period
    /// so a slower endpoint with newer finality is not ignored. A bootstrap
    /// still in flight at the cutoff keeps running for the next poll.
    async fn poll(&self, pool: &mut VerifierPool) -> FinalityProbeReport {
        let Some(start) = pool.start else {
            return select_endpoint_agreement([0; 32], self.config.minimum_agreement, Vec::new());
        };
        let bootstrap_root = pool
            .newest_agreed
            .map_or(start.root, |anchor| anchor.beacon_block_root);
        let mut pending = self
            .config
            .endpoints
            .iter()
            .zip(&self.labels)
            .zip(pool.endpoints.iter_mut())
            .map(|((endpoint, label), verifier)| async move {
                match poll_endpoint(
                    &self.transport,
                    endpoint,
                    verifier,
                    bootstrap_root,
                    self.config.max_checkpoint_age,
                    &self.clock,
                )
                .await
                {
                    Ok(result) => EndpointProbe {
                        endpoint: label.clone(),
                        verified: true,
                        anchor: Some(result.anchor),
                        checkpoint_anchor: Some(result.checkpoint_anchor),
                        attested_head: result.attested_head.as_ref().ok().copied(),
                        attested_head_error: result.attested_head.err(),
                        updates_verified: result.updates_verified,
                        error: None,
                    },
                    Err(error) => EndpointProbe {
                        endpoint: label.clone(),
                        verified: false,
                        anchor: None,
                        checkpoint_anchor: None,
                        attested_head: None,
                        attested_head_error: None,
                        updates_verified: 0,
                        error: Some(error.to_string()),
                    },
                }
            })
            .collect::<FuturesUnordered<_>>();
        let mut completed = Vec::with_capacity(self.config.endpoints.len());
        let grace = tokio::time::sleep(AGREEMENT_GRACE);
        tokio::pin!(grace);
        let mut grace_started = false;
        let mut grace_elapsed = false;
        let mut report = loop {
            if pending.is_empty() || grace_elapsed {
                let report = select_endpoint_agreement(
                    start.root,
                    self.config.minimum_agreement,
                    completed.clone(),
                );
                if report.accepted || pending.is_empty() {
                    break report;
                }
            }
            tokio::select! {
                Some(endpoint) = pending.next() => {
                    if endpoint.verified && !grace_started {
                        grace.as_mut().reset(tokio::time::Instant::now() + AGREEMENT_GRACE);
                        grace_started = true;
                    }
                    completed.push(endpoint);
                }
                () = &mut grace, if grace_started && !grace_elapsed => grace_elapsed = true,
            }
        };
        drop(pending);
        if pool.start_anchor.is_none() {
            pool.start_anchor = completed
                .iter()
                .filter_map(|endpoint| endpoint.checkpoint_anchor)
                .find(|anchor| anchor.beacon_block_root == start.root);
        }
        report.checkpoint_anchor = pool.start_anchor;
        if let Some(selected) = report.selected.filter(|_| report.accepted)
            && is_bootstrap_anchor(&selected)
            && pool
                .newest_agreed
                .is_none_or(|newest| selected.beacon_slot > newest.beacon_slot)
        {
            pool.newest_agreed = Some(selected);
        }
        self.publish_attested_head(&report);
        report
    }

    /// Publish a poll's attested head. Only a poll whose finality was
    /// accepted steers the live lane.
    fn publish_attested_head(&self, report: &FinalityProbeReport) {
        if let (Some(heads), Some(head)) = (
            &self.attested_heads,
            report.attested_head.filter(|_| report.accepted),
        ) && heads.publish(head)
        {
            debug!(
                beacon_slot = head.beacon_slot,
                block = head.block_number.0,
                "published a sync-committee-attested execution head"
            );
        }
    }

    fn validate_checkpoint(
        checkpoint: &ConsensusCheckpoint,
        report: &FinalityProbeReport,
        start: StartAnchor,
    ) -> Result<(), SourceError> {
        let accepted = report.selected.ok_or_else(|| {
            SourceError::Unavailable("no verified finality anchor was accepted".to_owned())
        })?;
        let bootstrap = report.checkpoint_anchor.ok_or_else(|| {
            SourceError::Protocol("verified bootstrap anchor is absent".to_owned())
        })?;
        if checkpoint.beacon_block_root == bootstrap.beacon_block_root {
            if checkpoint.beacon_slot != bootstrap.beacon_slot {
                return Err(SourceError::Protocol(format!(
                    "checkpoint slot mismatch: configured {}, verified {}",
                    checkpoint.beacon_slot, bootstrap.beacon_slot
                )));
            }
            if checkpoint.execution_block_hash != bootstrap.execution_block_hash {
                return Err(SourceError::Protocol(
                    "checkpoint execution hash differs from verified bootstrap proof".to_owned(),
                ));
            }
        } else if !start.persisted || bootstrap.beacon_slot < checkpoint.beacon_slot {
            // Only a newer persisted anchor may replace the checkpoint.
            return Err(SourceError::Protocol(
                "checkpoint beacon root differs from verified bootstrap".to_owned(),
            ));
        }
        if accepted.beacon_slot < checkpoint.beacon_slot {
            return Err(SourceError::Protocol(
                "verified finality regressed behind the checkpoint".to_owned(),
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl FinalitySource for VerifiedBeaconApi {
    fn descriptor(&self) -> &SourceDescriptor {
        &self.descriptor
    }

    async fn subscribe(
        &self,
        checkpoint: ConsensusCheckpoint,
        cancellation: CancellationToken,
    ) -> Result<FinalityEventStream, SourceError> {
        if checkpoint.beacon_block_root == [0; 32] {
            return Err(SourceError::Protocol(
                "weak-subjectivity checkpoint root must not be zero".to_owned(),
            ));
        }
        let source = Arc::new(self.clone());
        // Without a probe first, the checkpoint is the operator's trust root.
        let (initial, start) = source
            .start_and_poll(TrustedCheckpoint {
                root: checkpoint.beacon_block_root,
                slot: Some(checkpoint.beacon_slot),
                origin: CheckpointOrigin::Operator,
            })
            .await;
        Self::validate_checkpoint(&checkpoint, &initial, start)?;
        if !initial.accepted {
            return Err(SourceError::Unavailable(format!(
                "finality quorum not reached: {}",
                initial.disagreements.join("; ")
            )));
        }
        let state = SubscriptionState {
            source,
            cancellation,
            pending: initial.selected,
            last_emitted: None,
            terminal: false,
        };
        Ok(stream::unfold(state, next_finality_event).boxed())
    }
}

#[derive(Debug)]
struct SubscriptionState {
    source: Arc<VerifiedBeaconApi>,
    cancellation: CancellationToken,
    pending: Option<VerifiedFinalityAnchor>,
    last_emitted: Option<VerifiedFinalityAnchor>,
    terminal: bool,
}

async fn next_finality_event(
    mut state: SubscriptionState,
) -> Option<(Result<FinalityEvent, SourceError>, SubscriptionState)> {
    if state.terminal {
        return None;
    }
    loop {
        if state.cancellation.is_cancelled() {
            state.terminal = true;
            return Some((Err(SourceError::Cancelled), state));
        }
        if let Some(anchor) = state.pending.take() {
            match state.last_emitted {
                // An agreement behind the emitted anchor, such as after the
                // newest endpoint drops out, never moves finality back.
                Some(last) if anchor.beacon_slot < last.beacon_slot => {}
                Some(last) if anchor.beacon_slot == last.beacon_slot => {
                    if anchor != last {
                        state.terminal = true;
                        return Some((
                            Err(SourceError::Protocol(format!(
                                "verified finality contradicts the emitted anchor at slot {}",
                                anchor.beacon_slot
                            ))),
                            state,
                        ));
                    }
                }
                _ => {
                    state.last_emitted = Some(anchor);
                    return Some((
                        Ok(FinalityEvent::Finalized {
                            block_number: BlockNumber(anchor.execution_block_number),
                            block_hash: anchor.execution_block_hash,
                            beacon_slot: anchor.beacon_slot,
                            beacon_block_root: anchor.beacon_block_root,
                        }),
                        state,
                    ));
                }
            }
        }
        tokio::select! {
            () = tokio::time::sleep(state.source.config.poll_interval) => {}
            () = state.cancellation.cancelled() => {
                state.terminal = true;
                return Some((Err(SourceError::Cancelled), state));
            }
        }
        let report = state.source.poll_current().await;
        if !report.accepted {
            return Some((
                Err(SourceError::Unavailable(format!(
                    "verified finality quorum lost: {}",
                    report.disagreements.join("; ")
                ))),
                state,
            ));
        }
        state.pending = report.selected;
    }
}

#[derive(Debug)]
struct EndpointSync {
    anchor: VerifiedFinalityAnchor,
    checkpoint_anchor: VerifiedFinalityAnchor,
    attested_head: Result<AttestedHead, String>,
    updates_verified: u64,
}

/// Advance one endpoint's verifier: bootstrap it once, then verify the
/// latest finality update after the sync-committee updates it needs, and the
/// latest optimistic update.
async fn poll_endpoint(
    transport: &Arc<dyn BeaconTransport>,
    endpoint: &Url,
    verifier: &mut EndpointVerifier,
    bootstrap_root: [u8; 32],
    max_checkpoint_age: Duration,
    clock: &Clock,
) -> Result<EndpointSync, BeaconApiError> {
    if matches!(verifier, EndpointVerifier::Idle) {
        let (transport, endpoint, clock) = (transport.clone(), endpoint.clone(), clock.clone());
        *verifier = EndpointVerifier::Bootstrapping(tokio::spawn(async move {
            bootstrap_endpoint(
                transport.as_ref(),
                &endpoint,
                bootstrap_root,
                max_checkpoint_age,
                &clock,
            )
            .await
        }));
    }
    if let EndpointVerifier::Bootstrapping(task) = verifier {
        let bootstrapped = task.await;
        match bootstrapped {
            Ok(Ok(ready)) => *verifier = EndpointVerifier::Ready(Box::new(ready)),
            Ok(Err(error)) => {
                *verifier = EndpointVerifier::Idle;
                return Err(error);
            }
            Err(error) => {
                *verifier = EndpointVerifier::Idle;
                return Err(BeaconApiError::Protocol(format!(
                    "endpoint bootstrap task failed: {error}"
                )));
            }
        }
    }
    let EndpointVerifier::Ready(verifier) = verifier else {
        return Err(BeaconApiError::Protocol(
            "endpoint verifier is not bootstrapped".to_owned(),
        ));
    };
    let transport = transport.as_ref();
    let finality: FinalityUpdateResponse<MainnetConsensusSpec> = get_json(
        transport,
        endpoint,
        "eth/v1/beacon/light_client/finality_update",
    )
    .await?;
    let signature_slot = *finality.data.signature_slot();
    if signature_slot > verification_slot(clock.now()) {
        return Err(BeaconApiError::StaleUpdate(format!(
            "finality update signed at slot {signature_slot} is ahead of the local clock"
        )));
    }
    let mut periods = verifier.update_periods_before(signature_slot);
    while !periods.is_empty() {
        let count = periods
            .end
            .saturating_sub(periods.start)
            .min(MAX_UPDATES_PER_REQUEST);
        let updates: Vec<UpdateResponse<MainnetConsensusSpec>> = get_json(
            transport,
            endpoint,
            &format!(
                "eth/v1/beacon/light_client/updates?start_period={}&count={count}",
                periods.start
            ),
        )
        .await?;
        if u64::try_from(updates.len()).ok() != Some(count) {
            return Err(BeaconApiError::Protocol(format!(
                "endpoint returned {} updates for requested count {count}",
                updates.len()
            )));
        }
        for update in updates {
            verifier.apply_update(&update.data, clock.now())?;
        }
        periods.start = periods.start.saturating_add(count);
    }
    let anchor = verifier.apply_finality_update(&finality.data, clock.now())?;
    Ok(EndpointSync {
        anchor,
        checkpoint_anchor: verifier.checkpoint_anchor(),
        attested_head: attested_endpoint_head(transport, endpoint, verifier, clock).await,
        updates_verified: verifier.updates_verified(),
    })
}

/// Verify an endpoint's latest optimistic update, or say why it gives no
/// attested head. Either way the endpoint's finality stays in place: the live
/// lane waits for another endpoint's or the next slot's head.
async fn attested_endpoint_head(
    transport: &dyn BeaconTransport,
    endpoint: &Url,
    verifier: &MainnetLightClientVerifier,
    clock: &Clock,
) -> Result<AttestedHead, String> {
    let update: OptimisticUpdateResponse<MainnetConsensusSpec> =
        match get_json(transport, endpoint, OPTIMISTIC_UPDATE_PATH).await {
            Ok(update) => update,
            Err(error) => {
                debug!(%error, "Beacon endpoint served no optimistic update");
                return Err(format!("served no optimistic update: {error}"));
            }
        };
    verifier
        .verify_attested_head(&update.data, clock.now())
        .map_err(|error| {
            if error.is_unusable_head() {
                debug!(%error, "Beacon endpoint's optimistic update anchors no head");
                format!("optimistic update anchors no head: {error}")
            } else {
                warn!(
                    endpoint = %redacted_url(endpoint),
                    %error,
                    "Beacon endpoint served an optimistic update that failed verification"
                );
                format!("optimistic update failed verification: {error}")
            }
        })
}

/// Check an endpoint's network, then verify its bootstrap for `root`.
async fn bootstrap_endpoint(
    transport: &dyn BeaconTransport,
    endpoint: &Url,
    root: [u8; 32],
    max_checkpoint_age: Duration,
    clock: &Clock,
) -> Result<MainnetLightClientVerifier, BeaconApiError> {
    verify_mainnet_endpoint(transport, endpoint).await?;
    let bootstrap: BootstrapResponse<MainnetConsensusSpec> = get_json(
        transport,
        endpoint,
        &format!(
            "eth/v1/beacon/light_client/bootstrap/{:#x}",
            B256::from(root)
        ),
    )
    .await?;
    MainnetLightClientVerifier::bootstrap(root, &bootstrap.data, max_checkpoint_age, clock.now())
}

fn anchor_from_header(
    header: &helios_consensus_core::types::LightClientHeader,
    beacon_block_root: [u8; 32],
) -> Result<VerifiedFinalityAnchor, BeaconApiError> {
    let execution = header.execution().map_err(|()| {
        BeaconApiError::Protocol("light-client header has no execution payload proof".to_owned())
    })?;
    let execution_block_hash: [u8; 32] = (*execution.block_hash()).into();
    Ok(VerifiedFinalityAnchor {
        beacon_slot: header.beacon().slot,
        beacon_block_root,
        execution_block_number: *execution.block_number(),
        execution_block_hash: BlockHash::new(execution_block_hash),
    })
}

async fn verify_mainnet_endpoint(
    transport: &dyn BeaconTransport,
    endpoint: &Url,
) -> Result<(), BeaconApiError> {
    let response: GenesisResponse = get_json(transport, endpoint, "eth/v1/beacon/genesis").await?;
    if response.data.genesis_validators_root != MAINNET_GENESIS_ROOT {
        return Err(BeaconApiError::WrongNetwork(
            response.data.genesis_validators_root,
        ));
    }
    Ok(())
}

async fn get_json<T: DeserializeOwned>(
    transport: &dyn BeaconTransport,
    endpoint: &Url,
    path_and_query: &str,
) -> Result<T, BeaconApiError> {
    let bytes = transport.get(endpoint, path_and_query).await?;
    serde_json::from_slice(&bytes).map_err(|source| BeaconApiError::Decode {
        url: request_label(endpoint, path_and_query),
        source,
    })
}

fn verify_checkpoint_age(
    slot: u64,
    max_age: Duration,
    now: SystemTime,
) -> Result<(), BeaconApiError> {
    let checkpoint_time = MAINNET_GENESIS_TIME
        .checked_add(slot.saturating_mul(SLOT_SECONDS))
        .ok_or(BeaconApiError::InvalidCheckpointTime)?;
    let now = now
        .duration_since(UNIX_EPOCH)
        .map_err(|_| BeaconApiError::InvalidCheckpointTime)?
        .as_secs();
    let age = now.saturating_sub(checkpoint_time);
    if age > max_age.as_secs() {
        return Err(BeaconApiError::CheckpointTooOld {
            age_seconds: age,
            maximum_seconds: max_age.as_secs(),
        });
    }
    Ok(())
}

/// The mainnet beacon slot at `now`.
#[must_use]
pub fn current_slot(now: SystemTime) -> u64 {
    now.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .saturating_sub(MAINNET_GENESIS_TIME)
        / SLOT_SECONDS
}

/// Latest slot an update may be signed in: the local clock's slot plus one
/// slot of tolerated clock skew. Both finality sources verify against it.
#[must_use]
pub fn verification_slot(now: SystemTime) -> u64 {
    current_slot(now).saturating_add(CLOCK_SKEW_SLOTS)
}

/// Classify a verification failure. An update the store already passed, one
/// signed ahead of the local clock, or one from a period the store cannot
/// verify yet is stale. Any other failure, including slots that are
/// inconsistent within the update, is invalid.
fn update_error(
    kind: Option<&ConsensusError>,
    error: String,
    signature_slot: u64,
    now: SystemTime,
) -> BeaconApiError {
    match kind {
        Some(ConsensusError::NotRelevant | ConsensusError::InvalidPeriod) => {
            BeaconApiError::StaleUpdate(error)
        }
        Some(ConsensusError::InvalidTimestamp) if signature_slot > verification_slot(now) => {
            BeaconApiError::StaleUpdate(error)
        }
        _ => BeaconApiError::Verification(error),
    }
}

/// Select the highest slot that at least `minimum_agreement` endpoints
/// verified finality at or beyond. Different anchors at one slot fail
/// closed.
fn select_endpoint_agreement(
    checkpoint_root: [u8; 32],
    minimum_agreement: usize,
    endpoints: Vec<EndpointProbe>,
) -> FinalityProbeReport {
    let anchors = endpoints
        .iter()
        .filter_map(|endpoint| endpoint.anchor)
        .collect::<Vec<_>>();
    let mut same_slot = BTreeMap::<u64, Vec<VerifiedFinalityAnchor>>::new();
    for anchor in &anchors {
        same_slot
            .entry(anchor.beacon_slot)
            .or_default()
            .push(*anchor);
    }
    let mut disagreements = Vec::new();
    for (slot, at_slot) in &same_slot {
        if at_slot.windows(2).any(|pair| pair[0] != pair[1]) {
            disagreements.push(format!(
                "verified endpoints returned conflicting roots or execution hashes at slot {slot}"
            ));
        }
    }
    let selection = same_slot.iter().rev().find_map(|(slot, at_slot)| {
        let agreeing = anchors
            .iter()
            .filter(|anchor| anchor.beacon_slot >= *slot)
            .count();
        (agreeing >= minimum_agreement)
            .then_some(at_slot.first().map(|anchor| (*anchor, agreeing)))
            .flatten()
    });
    let (selected, agreeing_endpoints) =
        selection.map_or((None, 0), |(anchor, count)| (Some(anchor), count));
    if selected.is_none() {
        disagreements.push(format!(
            "no verified anchor reached minimum endpoint agreement {minimum_agreement}"
        ));
        disagreements.extend(endpoints.iter().filter_map(|endpoint| {
            endpoint
                .error
                .as_ref()
                .map(|error| format!("{} failed: {error}", endpoint.endpoint))
        }));
    }
    FinalityProbeReport {
        verifier: format!("helios-consensus-core@{HELIOS_REVISION}"),
        checkpoint_root,
        checkpoint_anchor: None,
        minimum_agreement,
        accepted: selected.is_some() && disagreements.is_empty(),
        selected,
        attested_head: select_attested_head(&endpoints),
        agreeing_endpoints,
        disagreements,
        endpoints,
    }
}

/// Select the newest attested head any endpoint verified. Endpoints are only
/// transports: each head carries the signatures of at least two thirds of the
/// sync committee, so one endpoint suffices and a lagging one never holds the
/// head back. Two different heads at the newest slot fail closed: no head is
/// followed from this poll.
fn select_attested_head(endpoints: &[EndpointProbe]) -> Option<AttestedHead> {
    let heads = endpoints
        .iter()
        .filter_map(|endpoint| endpoint.attested_head)
        .collect::<Vec<_>>();
    let newest = heads.iter().copied().max_by_key(|head| head.beacon_slot)?;
    if heads
        .iter()
        .any(|head| head.beacon_slot == newest.beacon_slot && *head != newest)
    {
        warn!(
            beacon_slot = newest.beacon_slot,
            "verified endpoints returned different attested heads at one slot; no head is followed from this poll"
        );
        return None;
    }
    Some(newest)
}

pub fn mainnet_forks() -> Forks {
    Forks {
        genesis: fork(0, "00000000"),
        altair: fork(74_240, "01000000"),
        bellatrix: fork(144_896, "02000000"),
        capella: fork(194_048, "03000000"),
        deneb: fork(269_568, "04000000"),
        electra: fork(364_032, "05000000"),
        fulu: fork(411_392, "06000000"),
    }
}

fn fork(epoch: u64, version: &str) -> Fork {
    Fork {
        epoch,
        fork_version: FixedBytes::<4>::from_str(version).expect("static fork version is valid hex"),
    }
}

#[derive(Debug, Deserialize)]
#[serde(bound = "S: ConsensusSpec")]
struct BootstrapResponse<S: ConsensusSpec> {
    data: Bootstrap<S>,
}

#[derive(Debug, Deserialize)]
#[serde(bound = "S: ConsensusSpec")]
struct UpdateResponse<S: ConsensusSpec> {
    data: Update<S>,
}

#[derive(Debug, Deserialize)]
#[serde(bound = "S: ConsensusSpec")]
struct FinalityUpdateResponse<S: ConsensusSpec> {
    data: FinalityUpdate<S>,
}

#[derive(Debug, Deserialize)]
#[serde(bound = "S: ConsensusSpec")]
struct OptimisticUpdateResponse<S: ConsensusSpec> {
    data: OptimisticUpdate<S>,
}

#[derive(Debug, Deserialize)]
struct GenesisResponse {
    data: GenesisData,
}

#[derive(Debug, Deserialize)]
struct GenesisData {
    genesis_validators_root: B256,
}

#[derive(Debug, Error)]
pub enum BeaconApiError {
    #[error("invalid beacon API configuration: {0}")]
    InvalidConfig(String),
    #[error("failed to initialize HTTP client: {0}")]
    HttpClient(reqwest::Error),
    #[error("invalid endpoint URL: {0}")]
    Url(url::ParseError),
    #[error("beacon API request failed for {url}: {source}")]
    Request {
        url: String,
        #[source]
        source: reqwest::Error,
    },
    #[error("beacon API {url} returned HTTP {status}: {detail}")]
    Status {
        url: String,
        status: u16,
        detail: String,
    },
    #[error("cannot decode beacon API response from {url}: {source}")]
    Decode {
        url: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("expected Ethereum mainnet but endpoint reports genesis validators root {0:#x}")]
    WrongNetwork(B256),
    #[error("light-client proof verification failed: {0}")]
    Verification(String),
    #[error("beacon API protocol violation: {0}")]
    Protocol(String),
    #[error("checkpoint time is invalid")]
    InvalidCheckpointTime,
    #[error(
        "checkpoint is {age_seconds} seconds old; maximum permitted age is {maximum_seconds} seconds"
    )]
    CheckpointTooOld {
        age_seconds: u64,
        maximum_seconds: u64,
    },
    #[error("finality anchor file: {0}")]
    AnchorFile(String),
    #[error("beacon API response from {url} exceeds {maximum} bytes")]
    ResponseTooLarge { url: String, maximum: usize },
    #[error("beacon API {url} answered HTTP {status} with a redirect, which is not followed")]
    Redirect { url: String, status: u16 },
    #[error("light-client update is stale or not yet verifiable: {0}")]
    StaleUpdate(String),
    #[error(
        "optimistic update carries {participants} sync-committee signatures; an attested head needs {required}"
    )]
    InsufficientParticipation { participants: u64, required: u64 },
}

impl BeaconApiError {
    /// Scrub from a server's error detail what `endpoint` keeps secret: each
    /// non-empty base-path segment, the raw query and each query value, and
    /// the username and password become `…`. The server may echo the
    /// request's path and query, where providers put API keys.
    #[must_use]
    pub fn redacting(self, endpoint: &Url) -> Self {
        match self {
            Self::Status {
                url,
                status,
                detail,
            } => Self::Status {
                url,
                status,
                detail: http::redact_endpoint(&detail, endpoint),
            },
            other => other,
        }
    }

    /// Whether an update was refused only because it is older than the
    /// verified store or ahead of the local clock. Such an update is not
    /// evidence of a faulty transport; retry later or elsewhere.
    #[must_use]
    pub const fn is_stale_update(&self) -> bool {
        matches!(self, Self::StaleUpdate(_))
    }

    /// Whether an optimistic update anchors no head without being evidence
    /// of a faulty transport: it is stale, or fewer than two thirds of the
    /// sync committee signed it, as happens while participation is low.
    #[must_use]
    pub const fn is_unusable_head(&self) -> bool {
        matches!(
            self,
            Self::StaleUpdate(_) | Self::InsufficientParticipation { .. }
        )
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        fs,
        ops::Range,
        path::Path,
        sync::{
            Mutex,
            atomic::{AtomicU64, Ordering},
        },
    };

    use alloy_primitives::b256;

    use super::*;

    const BOOTSTRAP_ROOT: B256 =
        b256!("5afc212a7924789b2bc86acad3ab3a6ffb1f6e97253ea50bee7f4f51422c9275");
    const BOOTSTRAP_EXECUTION_HASH: B256 =
        b256!("d131b92cb98455882c2c7b4ebf55dc6d02cc47e0e55a4d9570dea498affd6e74");
    const BOOTSTRAP_SLOT: u64 = 7_069_376;
    const FINALIZED_SLOT: u64 = 7_109_344;
    const OLDER_FINALIZED_SLOT: u64 = 7_104_096;
    const SIGNATURE_SLOT: u64 = 7_109_431;
    /// An earlier checkpoint root that no fixture endpoint serves.
    const UNSERVED_ROOT: [u8; 32] = [0x11; 32];
    const SLOTS_PER_PERIOD: u64 = 8_192;
    const DAY: u64 = 86_400;
    const BOOTSTRAP_JSON: &str = include_str!("../tests/fixtures/helios/bootstrap.json");
    const UPDATES_JSON: &str = include_str!("../tests/fixtures/helios/updates.json");
    const FINALITY_JSON: &str = include_str!("../tests/fixtures/helios/finality.json");
    const OPTIMISTIC_JSON: &str = include_str!("../tests/fixtures/helios/optimistic.json");
    /// The attested header of `optimistic.json`, signed at the next slot.
    const ATTESTED_SLOT: u64 = 7_109_431;
    const ATTESTED_BLOCK: u64 = 17_923_113;
    const ATTESTED_EXECUTION_HASH: B256 =
        b256!("3c015340e234ff7f8e75ecebb11d45154a394cd896ddcfcfffc941a07b314960");
    const FINALIZED_BLOCK: u64 = 17_923_026;

    /// Serves the vendored Helios mainnet responses and records each request.
    #[derive(Debug, Default)]
    struct FixtureTransport {
        requests: Mutex<Vec<String>>,
        /// Finality responses served instead of `finality.json`, by endpoint.
        finality: Mutex<HashMap<String, String>>,
        /// Optimistic responses served instead of `optimistic.json`, by
        /// endpoint.
        optimistic: Mutex<HashMap<String, String>>,
        /// Endpoints that serve no optimistic update, answering HTTP 404.
        no_optimistic: Mutex<Vec<String>>,
        /// Delays before a finality response, by endpoint.
        finality_delays: Mutex<HashMap<String, Duration>>,
        /// Delays before a bootstrap response, by endpoint.
        bootstrap_delays: Mutex<HashMap<String, Duration>>,
        /// Endpoints that answer every request with HTTP 404.
        unavailable: Mutex<Vec<String>>,
    }

    impl FixtureTransport {
        fn count(&self, fragment: &str) -> usize {
            self.requests
                .lock()
                .expect("request log")
                .iter()
                .filter(|request| request.contains(fragment))
                .count()
        }
    }

    #[async_trait]
    impl BeaconTransport for FixtureTransport {
        async fn get(
            &self,
            endpoint: &Url,
            path_and_query: &str,
        ) -> Result<Vec<u8>, BeaconApiError> {
            let request = format!("{endpoint}{path_and_query}");
            self.requests
                .lock()
                .expect("request log")
                .push(request.clone());
            let delays = if path_and_query == "eth/v1/beacon/light_client/finality_update" {
                Some(&self.finality_delays)
            } else if path_and_query.starts_with("eth/v1/beacon/light_client/bootstrap/") {
                Some(&self.bootstrap_delays)
            } else {
                None
            };
            let delay = delays.and_then(|delays| {
                delays
                    .lock()
                    .expect("delays")
                    .get(endpoint.as_str())
                    .copied()
            });
            if let Some(delay) = delay {
                tokio::time::sleep(delay).await;
            }
            let missing = || BeaconApiError::Status {
                url: request_label(endpoint, path_and_query),
                status: 404,
                detail: "not served by the fixture".to_owned(),
            };
            if self
                .unavailable
                .lock()
                .expect("unavailable endpoints")
                .iter()
                .any(|unavailable| unavailable == endpoint.as_str())
            {
                return Err(missing());
            }
            let body = if path_and_query == "eth/v1/beacon/genesis" {
                serde_json::json!({
                    "data": {
                        "genesis_time": MAINNET_GENESIS_TIME.to_string(),
                        "genesis_validators_root": format!("{MAINNET_GENESIS_ROOT:#x}"),
                        "genesis_fork_version": "0x00000000"
                    }
                })
                .to_string()
            } else if let Some(root) =
                path_and_query.strip_prefix("eth/v1/beacon/light_client/bootstrap/")
            {
                if root != format!("{BOOTSTRAP_ROOT:#x}") {
                    return Err(missing());
                }
                BOOTSTRAP_JSON.to_owned()
            } else if let Some(query) =
                path_and_query.strip_prefix("eth/v1/beacon/light_client/updates?")
            {
                let parameter = |name: &str| {
                    query
                        .split('&')
                        .find_map(|pair| pair.strip_prefix(name)?.strip_prefix('='))
                        .and_then(|value| value.parse::<u64>().ok())
                        .expect("update query parameter")
                };
                let start = parameter("start_period");
                fixture_updates(start..start + parameter("count"))
            } else if path_and_query == "eth/v1/beacon/light_client/finality_update" {
                self.finality
                    .lock()
                    .expect("finality overrides")
                    .get(endpoint.as_str())
                    .cloned()
                    .unwrap_or_else(|| FINALITY_JSON.to_owned())
            } else if path_and_query == OPTIMISTIC_UPDATE_PATH {
                if self
                    .no_optimistic
                    .lock()
                    .expect("endpoints without optimistic updates")
                    .iter()
                    .any(|without| without == endpoint.as_str())
                {
                    return Err(missing());
                }
                self.optimistic
                    .lock()
                    .expect("optimistic overrides")
                    .get(endpoint.as_str())
                    .cloned()
                    .unwrap_or_else(|| OPTIMISTIC_JSON.to_owned())
            } else {
                return Err(missing());
            };
            Ok(body.into_bytes())
        }
    }

    /// `optimistic.json` with its attested header or sync aggregate edited.
    fn edited_optimistic_json(edit: impl FnOnce(&mut serde_json::Value)) -> String {
        let mut update: serde_json::Value =
            serde_json::from_str(OPTIMISTIC_JSON).expect("fixture optimistic update");
        edit(&mut update["data"]);
        update.to_string()
    }

    /// A 512-bit sync-committee bitfield with the first `participants` set.
    fn participation_bits(participants: usize) -> String {
        let mut bits = [0_u8; 64];
        for index in 0..participants {
            bits[index / 8] |= 1 << (index % 8);
        }
        format!("0x{}", hex::encode(bits))
    }

    fn fixture_attested_head() -> AttestedHead {
        let update: OptimisticUpdateResponse<MainnetConsensusSpec> =
            serde_json::from_str(OPTIMISTIC_JSON).expect("fixture optimistic update");
        AttestedHead {
            beacon_slot: ATTESTED_SLOT,
            beacon_block_root: update.data.attested_header.beacon().tree_hash_root().into(),
            block_number: BlockNumber(ATTESTED_BLOCK),
            block_hash: BlockHash::new(ATTESTED_EXECUTION_HASH.into()),
        }
    }

    fn attested_slot(update: &serde_json::Value) -> u64 {
        update["data"]["attested_header"]["beacon"]["slot"]
            .as_str()
            .and_then(|slot| slot.parse().ok())
            .expect("attested slot")
    }

    fn fixture_updates(periods: Range<u64>) -> String {
        let updates: Vec<serde_json::Value> =
            serde_json::from_str(UPDATES_JSON).expect("fixture updates");
        serde_json::Value::from(
            updates
                .into_iter()
                .filter(|update| periods.contains(&(attested_slot(update) / SLOTS_PER_PERIOD)))
                .collect::<Vec<_>>(),
        )
        .to_string()
    }

    /// The signed period-867 update served as an older finality update.
    fn older_finality_json() -> String {
        let mut updates: Vec<serde_json::Value> =
            serde_json::from_str(UPDATES_JSON).expect("fixture updates");
        let mut update = updates.swap_remove(5);
        let data = update["data"].as_object_mut().expect("update data");
        data.remove("next_sync_committee");
        data.remove("next_sync_committee_branch");
        update.to_string()
    }

    fn slot_time(slot: u64) -> u64 {
        MAINNET_GENESIS_TIME + slot * SLOT_SECONDS
    }

    /// Wall clock the test sets explicitly.
    fn manual_clock(seconds: &Arc<AtomicU64>) -> Clock {
        let seconds = seconds.clone();
        Clock::new(move || UNIX_EPOCH + Duration::from_secs(seconds.load(Ordering::SeqCst)))
    }

    /// Wall clock that advances with tokio time, so paused-time tests
    /// simulate days of polling instantly.
    fn simulated_clock(start: u64) -> Clock {
        let started = tokio::time::Instant::now();
        Clock::new(move || UNIX_EPOCH + Duration::from_secs(start) + started.elapsed())
    }

    fn fixture_source(
        endpoints: &[&str],
        transport: &Arc<FixtureTransport>,
        clock: Clock,
        anchor: AnchorFile,
    ) -> VerifiedBeaconApi {
        let mut config = BeaconApiConfig::mainnet(
            endpoints
                .iter()
                .map(|endpoint| Url::parse(endpoint).expect("endpoint"))
                .collect(),
        );
        config.anchor = anchor;
        VerifiedBeaconApi::with_transport(config, transport.clone(), clock).expect("source")
    }

    fn read_write(path: &Path) -> AnchorFile {
        AnchorFile::ReadWrite {
            path: path.to_path_buf(),
            write_failures: Arc::default(),
        }
    }

    /// The operator's configured checkpoint, slot unknown.
    fn operator(root: impl Into<[u8; 32]>) -> TrustedCheckpoint {
        TrustedCheckpoint {
            root: root.into(),
            slot: None,
            origin: CheckpointOrigin::Operator,
        }
    }

    fn checkpoint(root: [u8; 32], slot: u64) -> ConsensusCheckpoint {
        ConsensusCheckpoint {
            beacon_slot: slot,
            beacon_block_root: root,
            execution_block_hash: BlockHash::new(BOOTSTRAP_EXECUTION_HASH.into()),
            obtained_at_unix_seconds: 0,
            source: "test checkpoint".to_owned(),
        }
    }

    fn bootstrap_anchor() -> VerifiedFinalityAnchor {
        VerifiedFinalityAnchor {
            beacon_slot: BOOTSTRAP_SLOT,
            beacon_block_root: BOOTSTRAP_ROOT.into(),
            execution_block_number: 17_883_333,
            execution_block_hash: BlockHash::new(BOOTSTRAP_EXECUTION_HASH.into()),
        }
    }

    fn finalized_slot(event: Option<Result<FinalityEvent, SourceError>>) -> u64 {
        match event {
            Some(Ok(FinalityEvent::Finalized { beacon_slot, .. })) => beacon_slot,
            other => panic!("expected a finalized event, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn one_bootstrap_per_endpoint_across_many_polls() {
        let transport = Arc::new(FixtureTransport::default());
        let seconds = Arc::new(AtomicU64::new(slot_time(SIGNATURE_SLOT) + 60));
        let source = fixture_source(
            &["https://a.example/", "https://b.example/"],
            &transport,
            manual_clock(&seconds),
            AnchorFile::Disabled,
        );
        for poll in 0..6 {
            let report = source.probe_root(operator(BOOTSTRAP_ROOT)).await;
            assert!(report.accepted, "poll {poll}: {:?}", report.disagreements);
            assert_eq!(
                report.selected.map(|anchor| anchor.beacon_slot),
                Some(FINALIZED_SLOT)
            );
            seconds.fetch_add(SLOT_SECONDS, Ordering::SeqCst);
        }
        assert_eq!(transport.count("light_client/bootstrap/"), 2);
        assert_eq!(transport.count("light_client/updates?"), 2);
        assert_eq!(transport.count("light_client/finality_update"), 12);
    }

    #[tokio::test(start_paused = true)]
    async fn verified_finality_keeps_polling_past_the_checkpoint_age_limit() {
        let transport = Arc::new(FixtureTransport::default());
        let mut source = fixture_source(
            &["https://a.example/"],
            &transport,
            simulated_clock(slot_time(SIGNATURE_SLOT) + 60),
            AnchorFile::Disabled,
        );
        source.config.poll_interval = Duration::from_secs(DAY);
        let mut events = source
            .subscribe(
                checkpoint(BOOTSTRAP_ROOT.into(), BOOTSTRAP_SLOT),
                CancellationToken::new(),
            )
            .await
            .expect("subscribe");
        assert_eq!(finalized_slot(events.next().await), FINALIZED_SLOT);
        // Daily polls for twenty simulated days run well past the 14-day
        // checkpoint age limit without an error or a second bootstrap.
        let quiet = tokio::time::timeout(Duration::from_secs(20 * DAY), events.next()).await;
        assert!(quiet.is_err(), "finality stream yielded {quiet:?}");
        assert!(transport.count("light_client/finality_update") >= 20);
        assert_eq!(transport.count("light_client/bootstrap/"), 1);
    }

    #[tokio::test]
    async fn startup_bootstraps_from_a_newer_persisted_anchor() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join(FINALITY_ANCHOR_FILE);
        persist_finality_anchor(
            &path,
            &PersistedFinalityAnchor {
                anchor: bootstrap_anchor(),
                checkpoint_root: UNSERVED_ROOT,
            },
        )
        .expect("persist anchor");
        // The configured checkpoint is past the age limit and no longer
        // served; the persisted anchor verified from it is neither.
        let transport = Arc::new(FixtureTransport::default());
        let source = fixture_source(
            &["https://a.example/"],
            &transport,
            Clock::new(move || {
                UNIX_EPOCH + Duration::from_secs(slot_time(BOOTSTRAP_SLOT) + 13 * DAY)
            }),
            read_write(&path),
        );
        let report = source.probe_root(operator(UNSERVED_ROOT)).await;
        assert!(report.accepted, "{:?}", report.disagreements);
        assert_eq!(report.checkpoint_root, <[u8; 32]>::from(BOOTSTRAP_ROOT));
        assert_eq!(
            report.selected.map(|anchor| anchor.beacon_slot),
            Some(FINALIZED_SLOT)
        );
        assert_eq!(
            transport.count(&format!("{:#x}", B256::from(UNSERVED_ROOT))),
            0
        );
        let persisted = read_finality_anchor(&path)
            .expect("read anchor")
            .expect("anchor present");
        assert_eq!(persisted.anchor.beacon_slot, FINALIZED_SLOT);
        assert_eq!(persisted.checkpoint_root, UNSERVED_ROOT);

        // A locally verified checkpoint, such as the embedded subscription's
        // cache, is replaced by a later persisted anchor from any lineage.
        // The fixtures hold one bootstrap, so persist that one again.
        fs::remove_file(&path).expect("reset anchor");
        let transport = Arc::new(FixtureTransport::default());
        persist_finality_anchor(
            &path,
            &PersistedFinalityAnchor {
                anchor: bootstrap_anchor(),
                checkpoint_root: [0x22; 32],
            },
        )
        .expect("persist anchor");
        let source = fixture_source(
            &["https://a.example/"],
            &transport,
            Clock::new(move || {
                UNIX_EPOCH + Duration::from_secs(slot_time(BOOTSTRAP_SLOT) + 13 * DAY)
            }),
            read_write(&path),
        );
        let report = source
            .probe_root(TrustedCheckpoint {
                root: UNSERVED_ROOT,
                slot: Some(BOOTSTRAP_SLOT - SLOTS_PER_PERIOD),
                origin: CheckpointOrigin::LocallyVerified,
            })
            .await;
        assert!(report.accepted, "{:?}", report.disagreements);
        assert_eq!(report.checkpoint_root, <[u8; 32]>::from(BOOTSTRAP_ROOT));
    }

    #[tokio::test]
    async fn a_persisted_anchor_from_another_trust_root_is_ignored() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join(FINALITY_ANCHOR_FILE);
        // Newer and in age, but verified from a checkpoint the operator no
        // longer configures.
        persist_finality_anchor(
            &path,
            &PersistedFinalityAnchor {
                anchor: VerifiedFinalityAnchor {
                    beacon_slot: FINALIZED_SLOT,
                    beacon_block_root: [0x55; 32],
                    ..bootstrap_anchor()
                },
                checkpoint_root: [0x44; 32],
            },
        )
        .expect("persist anchor");
        let clock =
            Clock::new(move || UNIX_EPOCH + Duration::from_secs(slot_time(SIGNATURE_SLOT) + 60));
        let foreign_request = format!("bootstrap/{:#x}", B256::from([0x55; 32]));
        let transport = Arc::new(FixtureTransport::default());
        let source = fixture_source(
            &["https://a.example/"],
            &transport,
            clock.clone(),
            read_write(&path),
        );
        let report = source
            .probe_root(TrustedCheckpoint {
                root: BOOTSTRAP_ROOT.into(),
                slot: Some(BOOTSTRAP_SLOT),
                origin: CheckpointOrigin::Operator,
            })
            .await;
        assert!(report.accepted, "{:?}", report.disagreements);
        assert_eq!(report.checkpoint_root, <[u8; 32]>::from(BOOTSTRAP_ROOT));
        assert_eq!(transport.count(&foreign_request), 0);

        let transport = Arc::new(FixtureTransport::default());
        let source = fixture_source(
            &["https://a.example/"],
            &transport,
            clock,
            read_write(&path),
        );
        let mut events = source
            .subscribe(
                checkpoint(BOOTSTRAP_ROOT.into(), BOOTSTRAP_SLOT),
                CancellationToken::new(),
            )
            .await
            .expect("subscribe from the configured checkpoint");
        assert_eq!(finalized_slot(events.next().await), FINALIZED_SLOT);
        assert_eq!(transport.count(&foreign_request), 0);
    }

    #[tokio::test]
    async fn an_unserved_persisted_anchor_falls_back_to_the_configured_checkpoint() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join(FINALITY_ANCHOR_FILE);
        persist_finality_anchor(
            &path,
            &PersistedFinalityAnchor {
                anchor: VerifiedFinalityAnchor {
                    beacon_slot: FINALIZED_SLOT,
                    beacon_block_root: [0x66; 32],
                    ..bootstrap_anchor()
                },
                checkpoint_root: BOOTSTRAP_ROOT.into(),
            },
        )
        .expect("persist anchor");
        let transport = Arc::new(FixtureTransport::default());
        let source = fixture_source(
            &["https://a.example/", "https://b.example/"],
            &transport,
            Clock::new(move || UNIX_EPOCH + Duration::from_secs(slot_time(SIGNATURE_SLOT) + 60)),
            read_write(&path),
        );
        let report = source.probe_root(operator(BOOTSTRAP_ROOT)).await;
        assert!(report.accepted, "{:?}", report.disagreements);
        assert_eq!(report.checkpoint_root, <[u8; 32]>::from(BOOTSTRAP_ROOT));
        assert_eq!(
            transport.count(&format!("bootstrap/{:#x}", B256::from([0x66; 32]))),
            2
        );
    }

    #[tokio::test]
    async fn a_read_only_anchor_file_is_used_but_never_written() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join(FINALITY_ANCHOR_FILE);
        persist_finality_anchor(
            &path,
            &PersistedFinalityAnchor {
                anchor: bootstrap_anchor(),
                checkpoint_root: UNSERVED_ROOT,
            },
        )
        .expect("persist anchor");
        let persisted = fs::read(&path).expect("anchor file");
        let transport = Arc::new(FixtureTransport::default());
        let source = fixture_source(
            &["https://a.example/"],
            &transport,
            Clock::new(move || {
                UNIX_EPOCH + Duration::from_secs(slot_time(BOOTSTRAP_SLOT) + 13 * DAY)
            }),
            AnchorFile::ReadOnly(path.clone()),
        );
        let report = source.probe_root(operator(UNSERVED_ROOT)).await;
        assert!(report.accepted, "{:?}", report.disagreements);
        assert_eq!(report.checkpoint_root, <[u8; 32]>::from(BOOTSTRAP_ROOT));
        assert_eq!(
            report.selected.map(|anchor| anchor.beacon_slot),
            Some(FINALIZED_SLOT)
        );
        assert_eq!(fs::read(&path).expect("anchor file"), persisted);
    }

    #[tokio::test]
    async fn anchor_write_failures_are_counted_without_failing_finality() {
        let directory = tempfile::tempdir().expect("temporary directory");
        // The anchor file's directory is a regular file, so every write fails.
        let blocker = directory.path().join("not-a-directory");
        fs::write(&blocker, b"file").expect("blocking file");
        let write_failures = Arc::new(AtomicU64::new(0));
        let transport = Arc::new(FixtureTransport::default());
        let source = fixture_source(
            &["https://a.example/"],
            &transport,
            Clock::new(move || UNIX_EPOCH + Duration::from_secs(slot_time(SIGNATURE_SLOT) + 60)),
            AnchorFile::ReadWrite {
                path: blocker.join(FINALITY_ANCHOR_FILE),
                write_failures: write_failures.clone(),
            },
        );
        let report = source.probe_root(operator(BOOTSTRAP_ROOT)).await;
        assert!(report.accepted, "{:?}", report.disagreements);
        assert_eq!(write_failures.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_late_endpoint_bootstraps_from_the_newest_agreed_anchor() {
        let transport = Arc::new(FixtureTransport::default());
        transport
            .unavailable
            .lock()
            .expect("unavailable endpoints")
            .push("https://b.example/".to_owned());
        let source = fixture_source(
            &["https://a.example/", "https://b.example/"],
            &transport,
            simulated_clock(slot_time(SIGNATURE_SLOT) + 60),
            AnchorFile::Disabled,
        );
        let report = source.probe_root(operator(BOOTSTRAP_ROOT)).await;
        assert!(report.accepted, "{:?}", report.disagreements);
        let newest = report.selected.expect("agreed anchor");
        assert_eq!(newest.beacon_slot, FINALIZED_SLOT);

        // Nine days later the configured checkpoint is past the age limit,
        // while the newest agreed anchor is not.
        tokio::time::sleep(Duration::from_secs(9 * DAY)).await;
        transport
            .unavailable
            .lock()
            .expect("unavailable endpoints")
            .clear();
        let report = source.probe_root(operator(BOOTSTRAP_ROOT)).await;
        assert!(report.accepted, "{:?}", report.disagreements);
        let late_bootstraps = |root: [u8; 32]| {
            transport.count(&format!(
                "b.example/eth/v1/beacon/light_client/bootstrap/{:#x}",
                B256::from(root)
            ))
        };
        assert_eq!(late_bootstraps(newest.beacon_block_root), 1);
        assert_eq!(late_bootstraps(BOOTSTRAP_ROOT.into()), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_slow_bootstrap_completes_across_polls_and_then_participates() {
        let transport = Arc::new(FixtureTransport::default());
        transport
            .bootstrap_delays
            .lock()
            .expect("delays")
            .insert("https://slow.example/".to_owned(), Duration::from_secs(5));
        let source = fixture_source(
            &["https://fast.example/", "https://slow.example/"],
            &transport,
            simulated_clock(slot_time(SIGNATURE_SLOT) + 60),
            AnchorFile::Disabled,
        );
        let slow_verified = |report: &FinalityProbeReport| {
            report
                .endpoints
                .iter()
                .any(|endpoint| endpoint.endpoint.contains("slow.example") && endpoint.verified)
        };
        let report = source.probe_root(operator(BOOTSTRAP_ROOT)).await;
        assert!(report.accepted, "{:?}", report.disagreements);
        assert!(
            !slow_verified(&report),
            "the slow bootstrap outlasts the grace period"
        );

        tokio::time::sleep(Duration::from_secs(SLOT_SECONDS)).await;
        let report = source.probe_root(operator(BOOTSTRAP_ROOT)).await;
        assert!(slow_verified(&report), "{:?}", report.endpoints);
        assert_eq!(
            transport.count("slow.example/eth/v1/beacon/light_client/bootstrap/"),
            1
        );
    }

    #[tokio::test(start_paused = true)]
    async fn resubscribing_past_the_checkpoint_age_limit_reuses_the_bootstrapped_verifiers() {
        let transport = Arc::new(FixtureTransport::default());
        let source = fixture_source(
            &["https://a.example/"],
            &transport,
            simulated_clock(slot_time(SIGNATURE_SLOT) + 60),
            AnchorFile::Disabled,
        );
        let mut events = source
            .subscribe(
                checkpoint(BOOTSTRAP_ROOT.into(), BOOTSTRAP_SLOT),
                CancellationToken::new(),
            )
            .await
            .expect("subscribe");
        assert_eq!(finalized_slot(events.next().await), FINALIZED_SLOT);
        drop(events);

        // The runtime resubscribes with the same checkpoint after a stream
        // error, here 15 days after it was configured.
        tokio::time::sleep(Duration::from_secs(15 * DAY)).await;
        let mut events = source
            .subscribe(
                checkpoint(BOOTSTRAP_ROOT.into(), BOOTSTRAP_SLOT),
                CancellationToken::new(),
            )
            .await
            .expect("resubscribe past the checkpoint age limit");
        assert_eq!(finalized_slot(events.next().await), FINALIZED_SLOT);
        assert_eq!(transport.count("light_client/bootstrap/"), 1);
    }

    #[tokio::test]
    async fn startup_ignores_expired_corrupt_or_foreign_persisted_anchors() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join(FINALITY_ANCHOR_FILE);
        let bootstrap_request = format!("bootstrap/{BOOTSTRAP_ROOT:#x}");
        let unserved_request = format!("bootstrap/{:#x}", B256::from(UNSERVED_ROOT));

        // Expired: the persisted anchor is past the age limit, so only the
        // configured checkpoint is tried.
        persist_finality_anchor(
            &path,
            &PersistedFinalityAnchor {
                anchor: bootstrap_anchor(),
                checkpoint_root: UNSERVED_ROOT,
            },
        )
        .expect("persist anchor");
        let transport = Arc::new(FixtureTransport::default());
        let source = fixture_source(
            &["https://a.example/"],
            &transport,
            Clock::new(move || {
                UNIX_EPOCH + Duration::from_secs(slot_time(BOOTSTRAP_SLOT) + 15 * DAY)
            }),
            read_write(&path),
        );
        assert!(!source.probe_root(operator(UNSERVED_ROOT)).await.accepted);
        assert_eq!(transport.count(&bootstrap_request), 0);
        assert_eq!(transport.count(&unserved_request), 1);

        // Corrupt: the configured checkpoint is used and startup succeeds.
        fs::write(&path, b"{not json").expect("corrupt anchor");
        let clock =
            Clock::new(move || UNIX_EPOCH + Duration::from_secs(slot_time(SIGNATURE_SLOT) + 60));
        let transport = Arc::new(FixtureTransport::default());
        let source = fixture_source(
            &["https://a.example/"],
            &transport,
            clock.clone(),
            read_write(&path),
        );
        let report = source.probe_root(operator(BOOTSTRAP_ROOT)).await;
        assert!(report.accepted, "{:?}", report.disagreements);

        // Foreign: an anchor verified from another checkpoint does not
        // replace a configured checkpoint whose slot is unknown.
        persist_finality_anchor(
            &path,
            &PersistedFinalityAnchor {
                anchor: VerifiedFinalityAnchor {
                    beacon_slot: FINALIZED_SLOT + 32,
                    beacon_block_root: [0x33; 32],
                    ..bootstrap_anchor()
                },
                checkpoint_root: [0x44; 32],
            },
        )
        .expect("persist anchor");
        let transport = Arc::new(FixtureTransport::default());
        let source = fixture_source(
            &["https://a.example/"],
            &transport,
            clock,
            read_write(&path),
        );
        let report = source.probe_root(operator(BOOTSTRAP_ROOT)).await;
        assert!(report.accepted, "{:?}", report.disagreements);
        assert_eq!(transport.count(&bootstrap_request), 1);
    }

    #[test]
    fn start_anchor_prefers_only_newer_in_age_persisted_anchors() {
        use CheckpointOrigin::{LocallyVerified, Operator};

        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join(FINALITY_ANCHOR_FILE);
        let slot = 10_000_000;
        let persisted = PersistedFinalityAnchor {
            anchor: VerifiedFinalityAnchor {
                beacon_slot: slot,
                beacon_block_root: [0xaa; 32],
                execution_block_number: 20_000_000,
                execution_block_hash: BlockHash::new([0xbb; 32]),
            },
            checkpoint_root: [0xcc; 32],
        };
        persist_finality_anchor(&path, &persisted).expect("persist anchor");
        let now = UNIX_EPOCH + Duration::from_secs(slot_time(slot) + DAY);
        let checkpoint =
            |root: [u8; 32], slot: Option<u64>, origin| TrustedCheckpoint { root, slot, origin };
        let resolve = |checkpoint: TrustedCheckpoint, now: SystemTime| {
            resolve_start_anchor(Some(&path), checkpoint, DEFAULT_MAX_CHECKPOINT_AGE, now)
        };
        let expected = StartAnchor {
            root: [0xaa; 32],
            slot: Some(slot),
            checkpoint_root: [0xcc; 32],
            persisted: true,
        };
        // Verified from the configured checkpoint, whose slot may be unknown.
        assert_eq!(
            resolve(checkpoint([0xcc; 32], None, Operator), now),
            expected
        );
        assert_eq!(
            resolve(checkpoint([0xcc; 32], Some(slot - 64), Operator), now),
            expected
        );
        // The configured checkpoint is the persisted anchor itself.
        assert_eq!(
            resolve(checkpoint([0xaa; 32], Some(slot), Operator), now),
            expected
        );
        // Another operator trust root is kept, even when it is older.
        for configured in [
            checkpoint([0xdd; 32], Some(slot - 64), Operator),
            checkpoint([0xdd; 32], None, Operator),
        ] {
            assert_eq!(
                resolve(configured, now),
                StartAnchor::configured(configured)
            );
        }
        // A later anchor replaces a locally verified checkpoint.
        assert_eq!(
            resolve(
                checkpoint([0xdd; 32], Some(slot - 64), LocallyVerified),
                now
            ),
            expected
        );
        // An older one does not.
        let newer = checkpoint([0xdd; 32], Some(slot + 64), LocallyVerified);
        assert_eq!(resolve(newer, now), StartAnchor::configured(newer));
        // Past the age limit.
        let expired = UNIX_EPOCH + Duration::from_secs(slot_time(slot) + 15 * DAY);
        let lineage = checkpoint([0xcc; 32], Some(slot - 64), Operator);
        assert_eq!(resolve(lineage, expired), StartAnchor::configured(lineage));
        // Without an anchor file.
        assert_eq!(
            resolve_start_anchor(None, lineage, DEFAULT_MAX_CHECKPOINT_AGE, now),
            StartAnchor::configured(lineage)
        );
    }

    #[test]
    fn anchor_file_is_replaced_atomically_and_never_regresses() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join(FINALITY_ANCHOR_FILE);
        let older = PersistedFinalityAnchor {
            anchor: anchor(100, 1),
            checkpoint_root: [9; 32],
        };
        let newer = PersistedFinalityAnchor {
            anchor: anchor(132, 2),
            checkpoint_root: [9; 32],
        };
        assert!(persist_finality_anchor(&path, &older).expect("first anchor"));
        #[cfg(unix)]
        let first_inode = {
            use std::os::unix::fs::MetadataExt as _;
            fs::metadata(&path).expect("metadata").ino()
        };
        assert!(persist_finality_anchor(&path, &newer).expect("newer anchor"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt as _;
            assert_ne!(
                fs::metadata(&path).expect("metadata").ino(),
                first_inode,
                "a new anchor replaces the file by rename instead of rewriting it in place"
            );
        }
        assert!(!persist_finality_anchor(&path, &older).expect("older anchor"));
        assert!(!persist_finality_anchor(&path, &newer).expect("same anchor"));
        assert_eq!(
            read_finality_anchor(&path)
                .expect("read anchor")
                .expect("anchor present"),
            newer
        );
        assert_eq!(
            fs::read_dir(directory.path()).expect("directory").count(),
            1,
            "no temporary file is left behind"
        );
        let encoded = fs::read_to_string(&path).expect("anchor file");
        assert!(encoded.contains("\"version\": 1"), "{encoded}");
    }

    fn fixture_verifier(now: SystemTime) -> MainnetLightClientVerifier {
        let bootstrap: BootstrapResponse<MainnetConsensusSpec> =
            serde_json::from_str(BOOTSTRAP_JSON).expect("fixture bootstrap");
        let mut verifier = MainnetLightClientVerifier::bootstrap(
            BOOTSTRAP_ROOT.into(),
            &bootstrap.data,
            DEFAULT_MAX_CHECKPOINT_AGE,
            now,
        )
        .expect("verified bootstrap");
        let updates: Vec<UpdateResponse<MainnetConsensusSpec>> =
            serde_json::from_str(&fixture_updates(862..867)).expect("fixture updates");
        for update in updates {
            verifier
                .apply_update(&update.data, now)
                .expect("verified update");
        }
        let finality: FinalityUpdateResponse<MainnetConsensusSpec> =
            serde_json::from_str(FINALITY_JSON).expect("fixture finality");
        verifier
            .apply_finality_update(&finality.data, now)
            .expect("verified finality");
        verifier
    }

    #[test]
    fn stale_and_early_updates_are_retryable_rather_than_invalid() {
        let now = UNIX_EPOCH + Duration::from_secs(slot_time(SIGNATURE_SLOT) + 60);
        let mut verifier = fixture_verifier(now);
        assert_eq!(
            verifier.finalized_anchor().expect("anchor").beacon_slot,
            FINALIZED_SLOT
        );

        // An update the store has already passed is stale, not invalid.
        let older: FinalityUpdateResponse<MainnetConsensusSpec> =
            serde_json::from_str(&older_finality_json()).expect("older finality");
        let error = verifier
            .apply_finality_update(&older.data, now)
            .expect_err("not relevant");
        assert!(error.is_stale_update(), "{error}");

        // One slot of local clock skew is tolerated.
        let finality: FinalityUpdateResponse<MainnetConsensusSpec> =
            serde_json::from_str(FINALITY_JSON).expect("fixture finality");
        let slow_clock = UNIX_EPOCH + Duration::from_secs(slot_time(SIGNATURE_SLOT) - 1);
        verifier
            .apply_finality_update(&finality.data, slow_clock)
            .expect("an update signed one slot ahead verifies");

        // Further ahead of the local clock is retryable, not invalid.
        let behind = UNIX_EPOCH + Duration::from_secs(slot_time(SIGNATURE_SLOT - 10));
        let error = verifier
            .apply_finality_update(&finality.data, behind)
            .expect_err("signed ahead of the local clock");
        assert!(error.is_stale_update(), "{error}");

        // An update signed no later than its attested slot is malformed,
        // whatever the local clock says.
        let mut malformed: serde_json::Value =
            serde_json::from_str(FINALITY_JSON).expect("fixture finality");
        malformed["data"]["signature_slot"] = (SIGNATURE_SLOT - 1).to_string().into();
        let malformed: FinalityUpdateResponse<MainnetConsensusSpec> =
            serde_json::from_value(malformed).expect("malformed finality");
        let error = verifier
            .apply_finality_update(&malformed.data, now)
            .expect_err("signed at its attested slot");
        assert!(!error.is_stale_update(), "{error}");

        // A header the sync committee did not sign is invalid.
        let mut forged: serde_json::Value =
            serde_json::from_str(FINALITY_JSON).expect("fixture finality");
        forged["data"]["attested_header"]["beacon"]["proposer_index"] = "1".into();
        let forged: FinalityUpdateResponse<MainnetConsensusSpec> =
            serde_json::from_value(forged).expect("forged finality");
        let error = verifier
            .apply_finality_update(&forged.data, now)
            .expect_err("forged");
        assert!(!error.is_stale_update(), "{error}");
    }

    fn optimistic_update(encoded: &str) -> OptimisticUpdate<MainnetConsensusSpec> {
        serde_json::from_str::<OptimisticUpdateResponse<MainnetConsensusSpec>>(encoded)
            .expect("optimistic update")
            .data
    }

    #[test]
    fn optimistic_updates_anchor_the_attested_execution_head() {
        let now = UNIX_EPOCH + Duration::from_secs(slot_time(SIGNATURE_SLOT) + 60);
        let verifier = fixture_verifier(now);
        // The execution payload of the header the sync committee signed,
        // proven by the header's execution branch.
        assert_eq!(
            verifier
                .verify_attested_head(&optimistic_update(OPTIMISTIC_JSON), now)
                .expect("verified optimistic update"),
            fixture_attested_head()
        );
        // Signed ahead of the local clock: retryable, not invalid.
        let behind = UNIX_EPOCH + Duration::from_secs(slot_time(ATTESTED_SLOT - 10));
        let error = verifier
            .verify_attested_head(&optimistic_update(OPTIMISTIC_JSON), behind)
            .expect_err("signed ahead of the local clock");
        assert!(error.is_unusable_head(), "{error}");
        // A header the committee did not sign is invalid.
        let forged = optimistic_update(&edited_optimistic_json(|data| {
            data["attested_header"]["beacon"]["proposer_index"] = "1".into();
        }));
        let error = verifier
            .verify_attested_head(&forged, now)
            .expect_err("forged");
        assert!(!error.is_unusable_head(), "{error}");
        // So is an execution payload the signed header does not commit to.
        let unproven = optimistic_update(&edited_optimistic_json(|data| {
            data["attested_header"]["execution"]["block_number"] =
                (ATTESTED_BLOCK + 1).to_string().into();
        }));
        let error = verifier
            .verify_attested_head(&unproven, now)
            .expect_err("unproven execution payload");
        assert!(!error.is_unusable_head(), "{error}");
    }

    #[test]
    fn attested_heads_need_two_thirds_of_the_sync_committee() {
        assert_eq!(SYNC_COMMITTEE_SUPERMAJORITY, 342);
        assert!(has_sync_committee_supermajority(342));
        assert!(has_sync_committee_supermajority(512));
        assert!(!has_sync_committee_supermajority(341));
        assert!(!has_sync_committee_supermajority(1));

        let now = UNIX_EPOCH + Duration::from_secs(slot_time(SIGNATURE_SLOT) + 60);
        let verifier = fixture_verifier(now);
        // Helios accepts any number of signatures. 341 of them are refused
        // before the BLS check, as participation too low to anchor a head,
        // not as a forgery.
        let low = optimistic_update(&edited_optimistic_json(|data| {
            data["sync_aggregate"]["sync_committee_bits"] = participation_bits(341).into();
        }));
        let error = verifier
            .verify_attested_head(&low, now)
            .expect_err("low participation");
        assert!(
            matches!(
                error,
                BeaconApiError::InsufficientParticipation {
                    participants: 341,
                    required: 342
                }
            ),
            "{error}"
        );
        assert!(error.is_unusable_head());
        // 342 pass the count, and the BLS check decides: the aggregate
        // signature is over other members.
        let enough = optimistic_update(&edited_optimistic_json(|data| {
            data["sync_aggregate"]["sync_committee_bits"] = participation_bits(342).into();
        }));
        let error = verifier
            .verify_attested_head(&enough, now)
            .expect_err("signature over other members");
        assert!(matches!(error, BeaconApiError::Verification(_)), "{error}");
    }

    #[tokio::test]
    async fn accepted_polls_publish_the_newest_verified_attested_head() {
        let transport = Arc::new(FixtureTransport::default());
        {
            let mut optimistic = transport.optimistic.lock().expect("optimistic overrides");
            // One endpoint forges its optimistic update, and one serves an
            // update too few committee members signed.
            optimistic.insert(
                "https://b.example/".to_owned(),
                edited_optimistic_json(|data| {
                    data["attested_header"]["beacon"]["proposer_index"] = "1".into();
                }),
            );
            optimistic.insert(
                "https://c.example/".to_owned(),
                edited_optimistic_json(|data| {
                    data["sync_aggregate"]["sync_committee_bits"] = participation_bits(341).into();
                }),
            );
        }
        // And one serves no optimistic update at all.
        transport
            .no_optimistic
            .lock()
            .expect("endpoints without optimistic updates")
            .push("https://d.example/".to_owned());
        let heads = AttestedHeadPublisher::new();
        let source = fixture_source(
            &[
                "https://a.example/",
                "https://b.example/",
                "https://c.example/",
                "https://d.example/",
            ],
            &transport,
            Clock::new(move || UNIX_EPOCH + Duration::from_secs(slot_time(SIGNATURE_SLOT) + 60)),
            AnchorFile::Disabled,
        )
        .with_attested_heads(heads.clone());
        let mut events = source
            .subscribe(
                checkpoint(BOOTSTRAP_ROOT.into(), BOOTSTRAP_SLOT),
                CancellationToken::new(),
            )
            .await
            .expect("subscribe");
        match events.next().await {
            Some(Ok(FinalityEvent::Finalized {
                block_number,
                block_hash,
                beacon_slot,
                ..
            })) => {
                assert_eq!(block_number, BlockNumber(FINALIZED_BLOCK));
                assert_eq!(beacon_slot, FINALIZED_SLOT);
                assert_ne!(block_hash, BlockHash::ZERO);
            }
            other => panic!("expected a finalized event, got {other:?}"),
        }
        assert_eq!(heads.latest(), Some(fixture_attested_head()));
        assert!(transport.count(OPTIMISTIC_UPDATE_PATH) >= 4);
        let report = source.probe_root(operator(BOOTSTRAP_ROOT)).await;
        assert_eq!(report.attested_head, Some(fixture_attested_head()));
        // Each endpoint whose finality verified reports its head, or why it
        // gave none.
        let head_or_reason = |label: &str| {
            let endpoint = report
                .endpoints
                .iter()
                .find(|endpoint| endpoint.endpoint.starts_with(label))
                .unwrap_or_else(|| panic!("{label} is not reported: {:?}", report.endpoints));
            assert!(endpoint.verified, "{endpoint:?}");
            (endpoint.attested_head, endpoint.attested_head_error.clone())
        };
        assert_eq!(
            head_or_reason("https://a.example"),
            (Some(fixture_attested_head()), None)
        );
        for (label, reason) in [
            ("https://b.example", "failed verification"),
            ("https://c.example", "anchors no head"),
            ("https://d.example", "served no optimistic update"),
        ] {
            let (head, error) = head_or_reason(label);
            assert_eq!(head, None, "{label}");
            assert!(
                error.as_deref().is_some_and(|error| error.contains(reason)),
                "{label} reports no reason: {error:?}"
            );
        }
        let encoded = serde_json::to_string(&report).expect("probe report");
        assert!(encoded.contains("attested_head_error"), "{encoded}");
    }

    #[test]
    fn different_attested_heads_at_one_slot_fail_closed() {
        let at = |slot: u64, byte: u8| AttestedHead {
            beacon_slot: slot,
            beacon_block_root: [byte; 32],
            block_number: BlockNumber(slot * 2),
            block_hash: BlockHash::new([byte; 32]),
        };
        let serving = |head| EndpointProbe {
            attested_head: Some(head),
            ..endpoint("https://a.example", Some(anchor(10, 1)))
        };
        // A lagging endpoint never holds the head back.
        assert_eq!(
            select_attested_head(&[serving(at(10, 1)), serving(at(12, 2))]),
            Some(at(12, 2))
        );
        // Two signed heads at one slot contradict each other.
        assert_eq!(
            select_attested_head(&[serving(at(12, 2)), serving(at(12, 3)), serving(at(10, 1))]),
            None
        );
        assert_eq!(
            select_attested_head(&[endpoint("https://a.example", None)]),
            None
        );
    }

    #[test]
    fn agreement_counts_endpoints_at_or_beyond_a_slot() {
        // An endpoint that already verified the next epoch vouches for the
        // slot a lagging endpoint verified, so epoch transitions keep the
        // agreement instead of dropping it.
        let report = select_endpoint_agreement(
            [9; 32],
            2,
            vec![
                endpoint("https://a.example", Some(anchor(64, 1))),
                endpoint("https://b.example", Some(anchor(96, 2))),
            ],
        );
        assert!(report.accepted, "{:?}", report.disagreements);
        assert_eq!(report.selected, Some(anchor(64, 1)));
        assert_eq!(report.agreeing_endpoints, 2);

        let report = select_endpoint_agreement(
            [9; 32],
            1,
            vec![
                endpoint("https://a.example", Some(anchor(64, 1))),
                endpoint("https://b.example", Some(anchor(96, 2))),
            ],
        );
        assert!(report.accepted);
        assert_eq!(report.selected, Some(anchor(96, 2)));
        assert_eq!(report.agreeing_endpoints, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn agreement_waits_for_slower_endpoints_and_finality_never_regresses() {
        let transport = Arc::new(FixtureTransport::default());
        transport
            .finality
            .lock()
            .expect("finality overrides")
            .insert("https://fast.example/".to_owned(), older_finality_json());
        transport
            .finality_delays
            .lock()
            .expect("delays")
            .insert("https://slow.example/".to_owned(), Duration::from_secs(1));
        let source = fixture_source(
            &["https://fast.example/", "https://slow.example/"],
            &transport,
            simulated_clock(slot_time(SIGNATURE_SLOT) + 60),
            AnchorFile::Disabled,
        );
        let mut events = source
            .subscribe(
                checkpoint(BOOTSTRAP_ROOT.into(), BOOTSTRAP_SLOT),
                CancellationToken::new(),
            )
            .await
            .expect("subscribe");
        // The slower endpoint answers within the grace period, so its newer
        // verified anchor wins over the first response.
        assert_eq!(finalized_slot(events.next().await), FINALIZED_SLOT);

        // Without it, the remaining endpoint's older anchor is not emitted.
        transport
            .unavailable
            .lock()
            .expect("unavailable endpoints")
            .push("https://slow.example/".to_owned());
        let quiet =
            tokio::time::timeout(Duration::from_secs(10 * SLOT_SECONDS), events.next()).await;
        assert!(quiet.is_err(), "finality regressed or failed: {quiet:?}");
        assert!(transport.count("fast.example/eth/v1/beacon/light_client/finality_update") > 2);
        // The remaining agreement is at the older slot the stream withheld.
        let report = source.poll_current().await;
        assert!(report.accepted, "{:?}", report.disagreements);
        assert_eq!(
            report.selected.map(|anchor| anchor.beacon_slot),
            Some(OLDER_FINALIZED_SLOT)
        );
    }

    #[tokio::test]
    async fn response_bodies_are_capped_while_reading() {
        let url = Url::parse("https://beacon.example/eth/v1/beacon/genesis").expect("URL");
        let oversized = reqwest::Response::from(
            ::http::Response::builder()
                .status(200)
                .body(vec![b'x'; 65])
                .expect("response"),
        );
        let error = read_response(oversized, url.as_str(), 64)
            .await
            .expect_err("oversized body");
        assert!(
            matches!(error, BeaconApiError::ResponseTooLarge { maximum: 64, .. }),
            "{error}"
        );

        let failed = reqwest::Response::from(
            ::http::Response::builder()
                .status(500)
                .body(vec![b'e'; 100_000])
                .expect("response"),
        );
        match read_response(failed, url.as_str(), 64).await {
            Err(BeaconApiError::Status {
                status: 500,
                detail,
                ..
            }) => assert!(detail.len() <= 512),
            other => panic!("expected a bounded status error, got {other:?}"),
        }

        let bounded = reqwest::Response::from(
            ::http::Response::builder()
                .status(200)
                .body(vec![b'x'; 64])
                .expect("response"),
        );
        assert_eq!(
            read_response(bounded, url.as_str(), 64)
                .await
                .expect("bounded body")
                .len(),
            64
        );
    }

    #[tokio::test]
    async fn redirects_are_refused_without_echoing_their_target() {
        let url = Url::parse("https://beacon.example/eth/v1/beacon/genesis").expect("URL");
        let redirect = reqwest::Response::from(
            ::http::Response::builder()
                .status(302)
                .header("location", "https://elsewhere.example/?token=hunter2")
                .body("moved to https://elsewhere.example/?token=hunter2")
                .expect("response"),
        );
        let error = read_response(redirect, url.as_str(), 64)
            .await
            .expect_err("redirect");
        assert!(
            matches!(error, BeaconApiError::Redirect { status: 302, .. }),
            "{error}"
        );
        assert!(!error.to_string().contains("hunter2"), "{error}");
    }

    /// Serve one HTTP/1.1 exchange on 127.0.0.1 with a canned `response`.
    /// The handle yields the request line of a client that connected within
    /// a second.
    async fn loopback_server(response: String) -> (Url, tokio::task::JoinHandle<Option<String>>) {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("loopback listener");
        let address = listener.local_addr().expect("listener address");
        let served = tokio::spawn(async move {
            let Ok(Ok((mut stream, _))) =
                tokio::time::timeout(Duration::from_secs(1), listener.accept()).await
            else {
                return None;
            };
            let mut request = Vec::new();
            let mut buffer = [0_u8; 1_024];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                match stream.read(&mut buffer).await {
                    Ok(0) | Err(_) => break,
                    Ok(read) => request.extend_from_slice(&buffer[..read]),
                }
            }
            let _ = stream.write_all(response.as_bytes()).await;
            let _ = stream.shutdown().await;
            let request = String::from_utf8_lossy(&request);
            Some(request.lines().next().unwrap_or_default().to_owned())
        });
        (
            Url::parse(&format!("http://{address}/")).expect("loopback URL"),
            served,
        )
    }

    fn loopback_transport() -> HttpTransport {
        HttpTransport {
            client: http::client_builder(Duration::from_secs(5))
                .no_proxy()
                .build()
                .expect("client"),
        }
    }

    #[tokio::test]
    async fn endpoint_query_keys_reach_every_request() {
        // Final review B3: joining the request path dropped the endpoint's
        // query, so a provider keyed by `?apikey=` never authenticated.
        let answer = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}";
        let (origin, served) = loopback_server(answer.to_owned()).await;
        let endpoint = origin.join("?apikey=k").expect("keyed endpoint");
        loopback_transport()
            .get(&endpoint, "eth/v1/beacon/genesis")
            .await
            .expect("keyed request");
        assert_eq!(
            served.await.expect("server").as_deref(),
            Some("GET /eth/v1/beacon/genesis?apikey=k HTTP/1.1")
        );
        // A request with its own query keeps both.
        let (origin, served) = loopback_server(answer.to_owned()).await;
        let endpoint = origin.join("?apikey=k").expect("keyed endpoint");
        loopback_transport()
            .get(
                &endpoint,
                "eth/v1/beacon/light_client/updates?start_period=1&count=1",
            )
            .await
            .expect("keyed request with a query");
        assert_eq!(
            served.await.expect("server").as_deref(),
            Some(
                "GET /eth/v1/beacon/light_client/updates?start_period=1&count=1&apikey=k HTTP/1.1"
            )
        );
    }

    #[tokio::test]
    async fn error_bodies_do_not_echo_endpoint_secrets() {
        // Final review B2: the status detail kept up to 512 characters of
        // the server's body, which may echo the request's path and query.
        let body = "Cannot GET /hunter5/eth/v1/beacon/genesis?apikey=hunter2";
        let (origin, _served) = loopback_server(format!(
            "HTTP/1.1 404 Not Found\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        ))
        .await;
        let endpoint = origin.join("hunter5/?apikey=hunter2").expect("endpoint");
        let error = loopback_transport()
            .get(&endpoint, "eth/v1/beacon/genesis")
            .await
            .expect_err("not found");
        let rendered = error.to_string();
        assert!(
            matches!(error, BeaconApiError::Status { status: 404, .. }),
            "{rendered}"
        );
        assert!(rendered.contains("Cannot GET"), "{rendered}");
        for secret in ["hunter5", "hunter2"] {
            assert!(!rendered.contains(secret), "{rendered}");
        }
    }

    #[tokio::test]
    async fn response_bodies_are_capped_while_streaming() {
        let body = "x".repeat(65);
        let (origin, _served) = loopback_server(format!(
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n41\r\n{body}\r\n0\r\n\r\n"
        ))
        .await;
        let client = http::client_builder(Duration::from_secs(5))
            .no_proxy()
            .build()
            .expect("client");
        let response = client
            .get(origin.join("eth/v1/beacon/genesis").expect("URL"))
            .send()
            .await
            .expect("response");
        assert_eq!(response.content_length(), None, "the body is streamed");
        let error = read_response(response, "loopback", 64)
            .await
            .expect_err("oversized streamed body");
        assert!(
            matches!(error, BeaconApiError::ResponseTooLarge { maximum: 64, .. }),
            "{error}"
        );
    }

    #[tokio::test]
    async fn the_beacon_client_never_follows_redirects() {
        let (target, contacted) = loopback_server(
            "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}".to_owned(),
        )
        .await;
        let (origin, _served) = loopback_server(format!(
            "HTTP/1.1 302 Found\r\nLocation: {target}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        ))
        .await;
        let transport = HttpTransport {
            client: http::client_builder(Duration::from_secs(5))
                .no_proxy()
                .build()
                .expect("client"),
        };
        let error = transport
            .get(&origin, "eth/v1/beacon/genesis")
            .await
            .expect_err("redirect");
        assert!(
            matches!(error, BeaconApiError::Redirect { status: 302, .. }),
            "{error}"
        );
        assert!(
            contacted.await.expect("redirect target").is_none(),
            "the redirect target was contacted"
        );
    }

    #[tokio::test]
    async fn endpoint_paths_keep_their_last_segment() {
        let endpoint =
            normalized_endpoint(&Url::parse("https://gateway.example/beacon").expect("URL"));
        assert_eq!(
            endpoint
                .join("eth/v1/beacon/genesis")
                .expect("join")
                .as_str(),
            "https://gateway.example/beacon/eth/v1/beacon/genesis"
        );

        let transport = Arc::new(FixtureTransport::default());
        let source = fixture_source(
            &["https://gateway.example/beacon"],
            &transport,
            Clock::new(move || UNIX_EPOCH + Duration::from_secs(slot_time(SIGNATURE_SLOT) + 60)),
            AnchorFile::Disabled,
        );
        let report = source.probe_root(operator(BOOTSTRAP_ROOT)).await;
        assert!(report.accepted, "{:?}", report.disagreements);
        assert_eq!(
            transport.count("https://gateway.example/beacon/eth/v1/beacon/genesis"),
            1
        );
    }

    #[tokio::test]
    async fn errors_and_reports_redact_endpoint_secrets() {
        let mut secret = Url::parse("https://beacon.example/v1/?apikey=hunter2").expect("URL");
        secret.set_username("operator").expect("username");
        secret.set_password(Some("hunter3")).expect("password");
        let rendered = redacted_url(&secret);
        for hidden in ["hunter2", "hunter3", "operator", "/v1"] {
            assert!(!rendered.contains(hidden), "{rendered}");
        }
        assert!(rendered.starts_with("https://"), "{rendered}");
        assert!(rendered.contains("beacon.example"), "{rendered}");

        let error = BeaconApiError::Status {
            url: request_label(&secret, "eth/v1/beacon/genesis"),
            status: 401,
            detail: "denied".to_owned(),
        };
        assert!(!error.to_string().contains("hunter"), "{error}");
        assert!(
            error.to_string().contains("eth/v1/beacon/genesis"),
            "{error}"
        );

        // QuickNode, Alchemy, and Ankr put the API key in the path.
        let path_tokens = [
            Url::parse("https://example.quiknode.pro/hunter5/").expect("URL"),
            Url::parse("https://example.quiknode.pro/hunter6/").expect("URL"),
        ];
        let rendered = redacted_url(&path_tokens[0]);
        assert!(!rendered.contains("hunter5"), "{rendered}");
        assert!(
            rendered.starts_with("https://example.quiknode.pro"),
            "{rendered}"
        );
        let label = request_label(&path_tokens[0], "eth/v1/beacon/genesis");
        assert!(!label.contains("hunter5"), "{label}");
        assert!(label.ends_with("eth/v1/beacon/genesis"), "{label}");
        let labels = endpoint_labels(&path_tokens, "finality.endpoints");
        assert_ne!(labels[0], labels[1]);
        assert!(labels[1].contains("finality.endpoints[1]"), "{labels:?}");
        assert!(
            labels.iter().all(|label| !label.contains("hunter")),
            "{labels:?}"
        );
        let transport = Arc::new(FixtureTransport::default());
        transport
            .unavailable
            .lock()
            .expect("unavailable endpoints")
            .push(path_tokens[0].to_string());
        let source = fixture_source(
            &[path_tokens[0].as_str(), path_tokens[1].as_str()],
            &transport,
            Clock::new(move || UNIX_EPOCH + Duration::from_secs(slot_time(SIGNATURE_SLOT) + 60)),
            AnchorFile::Disabled,
        );
        let report = source.probe_root(operator(BOOTSTRAP_ROOT)).await;
        let encoded = serde_json::to_string(&report).expect("probe report");
        assert!(!encoded.contains("hunter"), "{encoded}");

        let report = select_endpoint_agreement(
            [9; 32],
            1,
            vec![EndpointProbe {
                endpoint: redacted_url(&secret),
                verified: false,
                anchor: None,
                checkpoint_anchor: None,
                attested_head: None,
                attested_head_error: None,
                updates_verified: 0,
                error: Some("unavailable".to_owned()),
            }],
        );
        assert!(!report.accepted);
        assert!(
            report
                .disagreements
                .iter()
                .all(|disagreement| !disagreement.contains("hunter")),
            "{:?}",
            report.disagreements
        );
    }

    fn anchor(slot: u64, byte: u8) -> VerifiedFinalityAnchor {
        VerifiedFinalityAnchor {
            beacon_slot: slot,
            beacon_block_root: [byte; 32],
            execution_block_number: slot.saturating_mul(2),
            execution_block_hash: BlockHash::new([byte; 32]),
        }
    }

    fn endpoint(url: &str, anchor: Option<VerifiedFinalityAnchor>) -> EndpointProbe {
        EndpointProbe {
            endpoint: redacted_url(&Url::parse(url).expect("URL")),
            verified: anchor.is_some(),
            anchor,
            checkpoint_anchor: anchor.map(|anchor| VerifiedFinalityAnchor {
                beacon_slot: anchor.beacon_slot.saturating_sub(1),
                ..anchor
            }),
            attested_head: None,
            attested_head_error: None,
            updates_verified: u64::from(anchor.is_some()),
            error: anchor.is_none().then(|| "unavailable".to_owned()),
        }
    }

    #[test]
    fn selects_highest_anchor_with_required_agreement() {
        let report = select_endpoint_agreement(
            [9; 32],
            2,
            vec![
                endpoint("https://a.example", Some(anchor(10, 1))),
                endpoint("https://b.example", Some(anchor(10, 1))),
                endpoint("https://c.example", Some(anchor(9, 2))),
            ],
        );
        assert!(report.accepted);
        assert_eq!(report.selected, Some(anchor(10, 1)));
        assert_eq!(report.agreeing_endpoints, 2);
    }

    #[test]
    fn same_slot_contradiction_fails_closed() {
        let report = select_endpoint_agreement(
            [9; 32],
            1,
            vec![
                endpoint("https://a.example", Some(anchor(10, 1))),
                endpoint("https://b.example", Some(anchor(10, 2))),
            ],
        );
        assert!(!report.accepted);
        assert_eq!(report.disagreements.len(), 1);
    }

    #[test]
    fn failed_quorum_reports_each_transport_error() {
        let report = select_endpoint_agreement(
            [9; 32],
            1,
            vec![endpoint("https://unavailable.example", None)],
        );
        assert!(!report.accepted);
        assert!(report.disagreements.iter().any(|disagreement| {
            disagreement.contains("https://unavailable.example")
                && disagreement.contains("unavailable")
        }));
    }

    #[test]
    fn configuration_rejects_impossible_quorum() {
        let mut config =
            BeaconApiConfig::mainnet(vec![Url::parse("https://a.example").expect("URL")]);
        config.minimum_agreement = 2;
        assert!(matches!(
            VerifiedBeaconApi::mainnet(config),
            Err(BeaconApiError::InvalidConfig(_))
        ));
    }

    #[test]
    fn checkpoint_parser_is_strict() {
        assert_eq!(
            parse_checkpoint_root(&format!("0x{}", "12".repeat(32))).expect("root"),
            [0x12; 32]
        );
        assert!(parse_checkpoint_root(&"12".repeat(32)).is_err());
        assert!(parse_checkpoint_root("0x12").is_err());
    }

    #[test]
    fn genesis_identity_decodes_the_standard_beacon_response() {
        let response: GenesisResponse = serde_json::from_value(serde_json::json!({
            "data": {
                "genesis_time": MAINNET_GENESIS_TIME.to_string(),
                "genesis_validators_root": format!("{MAINNET_GENESIS_ROOT:#x}"),
                "genesis_fork_version": "0x00000000"
            }
        }))
        .expect("standard genesis response");
        assert_eq!(response.data.genesis_validators_root, MAINNET_GENESIS_ROOT);
    }

    #[test]
    fn mainnet_fork_digests_include_blob_parameter_forks() {
        assert_eq!(mainnet_fork_digest(0), [0xb5, 0x30, 0x3f, 0x2a]);
        assert_eq!(mainnet_fork_digest(269_568 * 32), [0x6a, 0x95, 0xa1, 0xa9]);
        assert_eq!(
            mainnet_fork_digest(FULU_FORK_EPOCH * 32),
            [0xcc, 0x2c, 0x5c, 0xdb]
        );
        assert_eq!(
            mainnet_fork_digest(MAINNET_BLOB_SCHEDULE[0].0 * 32),
            [0xcb, 0x0d, 0x1a, 0xcc]
        );
        assert_eq!(
            mainnet_fork_digest(MAINNET_BLOB_SCHEDULE[1].0 * 32),
            [0x8c, 0x9f, 0x62, 0xfe]
        );
    }
}
