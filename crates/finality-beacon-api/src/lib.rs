//! Verified Ethereum finality over the standard Beacon API.
//!
//! Beacon endpoints are treated only as untrusted transports. Checkpoint
//! bootstraps, sync-committee transitions, finality branches, BLS aggregate
//! signatures, and execution-payload branches are verified locally by the
//! pinned Helios consensus-core implementation.

use std::{
    collections::BTreeMap,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use alloy_primitives::{B256, FixedBytes};
use async_trait::async_trait;
use futures::{StreamExt, stream};
use helios_consensus_core::{
    apply_bootstrap, apply_finality_update, apply_update, calc_sync_period,
    consensus_spec::{ConsensusSpec, MainnetConsensusSpec},
    expected_current_slot,
    types::{Bootstrap, FinalityUpdate, Fork, Forks, LightClientStore, Update},
    verify_bootstrap, verify_finality_update, verify_update,
};
use leani_primitives::{
    BlockHash, Capability, CapabilitySet, ChainId, SourceId, SourceKind, TrustModel,
};
use leani_source_api::{
    ConsensusCheckpoint, FinalityEvent, FinalityEventStream, FinalityModel, FinalitySource,
    Partitioning, SourceDescriptor, SourceError,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tree_hash::TreeHash;
use url::Url;

pub const MAINNET_GENESIS_TIME: u64 = 1_606_824_023;
pub const MAINNET_GENESIS_ROOT: B256 =
    alloy_primitives::b256!("4b363db94e286120d76eb905340fdd4e54bfe9f06bf33ff6cf5ad27f511bfe95");
const MAX_UPDATES_PER_REQUEST: u64 = 128;
const SLOT_SECONDS: u64 = 12;
const PRIMED_PROBE_MAX_AGE: Duration = Duration::from_secs(30);
const FULU_FORK_EPOCH: u64 = 411_392;
const ELECTRA_FORK_EPOCH: u64 = 364_032;
const MAX_BLOBS_PER_BLOCK_ELECTRA: u64 = 9;
const MAINNET_BLOB_SCHEDULE: [(u64, u64); 2] = [(412_672, 15), (419_072, 21)];
pub const DEFAULT_MAX_CHECKPOINT_AGE: Duration = Duration::from_hours(336);

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

#[derive(Clone, Debug)]
pub struct BeaconApiConfig {
    pub endpoints: Vec<Url>,
    pub minimum_agreement: usize,
    pub request_timeout: Duration,
    pub poll_interval: Duration,
    pub max_checkpoint_age: Duration,
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
    pub endpoint: Url,
    pub verified: bool,
    pub anchor: Option<VerifiedFinalityAnchor>,
    pub checkpoint_anchor: Option<VerifiedFinalityAnchor>,
    pub updates_verified: u64,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FinalityProbeReport {
    pub verifier: String,
    pub checkpoint_root: [u8; 32],
    pub minimum_agreement: usize,
    pub accepted: bool,
    pub selected: Option<VerifiedFinalityAnchor>,
    pub agreeing_endpoints: usize,
    pub disagreements: Vec<String>,
    pub endpoints: Vec<EndpointProbe>,
}

/// Stateful verifier shared by every untrusted light-client transport.
#[derive(Debug)]
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
    ) -> Result<Self, BeaconApiError> {
        let checkpoint = B256::from(checkpoint_root);
        let forks = mainnet_forks();
        verify_bootstrap(bootstrap, checkpoint, &forks)
            .map_err(|error| BeaconApiError::Verification(error.to_string()))?;
        let checkpoint_slot = bootstrap.header().beacon().slot;
        verify_checkpoint_age(checkpoint_slot, max_checkpoint_age)?;
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

    #[must_use]
    pub fn first_required_period(&self) -> u64 {
        calc_sync_period::<MainnetConsensusSpec>(self.store.finalized_header.beacon().slot)
    }

    #[must_use]
    pub fn current_period() -> u64 {
        calc_sync_period::<MainnetConsensusSpec>(expected_current_slot(
            SystemTime::now(),
            MAINNET_GENESIS_TIME,
        ))
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
    ) -> Result<(), BeaconApiError> {
        let current_slot = expected_current_slot(SystemTime::now(), MAINNET_GENESIS_TIME);
        verify_update::<MainnetConsensusSpec>(
            update,
            current_slot,
            &self.store,
            MAINNET_GENESIS_ROOT,
            &self.forks,
        )
        .map_err(|error| BeaconApiError::Verification(error.to_string()))?;
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
    ) -> Result<VerifiedFinalityAnchor, BeaconApiError> {
        let current_slot = expected_current_slot(SystemTime::now(), MAINNET_GENESIS_TIME);
        verify_finality_update::<MainnetConsensusSpec>(
            update,
            current_slot,
            &self.store,
            MAINNET_GENESIS_ROOT,
            &self.forks,
        )
        .map_err(|error| BeaconApiError::Verification(error.to_string()))?;
        apply_finality_update(&mut self.store, update);
        self.updates_verified = self.updates_verified.saturating_add(1);
        self.finalized_anchor()
    }
}

#[derive(Clone, Debug)]
pub struct VerifiedBeaconApi {
    config: BeaconApiConfig,
    descriptor: SourceDescriptor,
    client: reqwest::Client,
    primed_probe: Arc<tokio::sync::Mutex<Option<PrimedProbe>>>,
}

#[derive(Clone, Debug)]
struct PrimedProbe {
    checkpoint_root: [u8; 32],
    verified_at: Instant,
    report: FinalityProbeReport,
}

impl VerifiedBeaconApi {
    /// Construct a mainnet verifier. No endpoint is contacted here.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid quorum/time limits or an HTTP client that
    /// cannot be initialized.
    pub fn mainnet(config: BeaconApiConfig) -> Result<Self, BeaconApiError> {
        config.validate()?;
        let source_id = SourceId::new("beacon-api-light-client")
            .map_err(|error| BeaconApiError::InvalidConfig(error.to_string()))?;
        let client = reqwest::Client::builder()
            .timeout(config.request_timeout)
            .connect_timeout(config.request_timeout.min(Duration::from_secs(10)))
            .build()
            .map_err(BeaconApiError::HttpClient)?;
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
            config,
            client,
            primed_probe: Arc::new(tokio::sync::Mutex::new(None)),
        })
    }

    /// Verify all configured endpoints from an explicit checkpoint root.
    pub async fn probe_root(&self, checkpoint_root: [u8; 32]) -> FinalityProbeReport {
        let report = self.fetch_probe_root(checkpoint_root).await;
        self.primed_probe.lock().await.replace(PrimedProbe {
            checkpoint_root,
            verified_at: Instant::now(),
            report: report.clone(),
        });
        report
    }

    async fn fetch_probe_root(&self, checkpoint_root: [u8; 32]) -> FinalityProbeReport {
        let checkpoint = B256::from(checkpoint_root);
        let maximum_concurrency = self.config.endpoints.len();
        let maximum_checkpoint_age = self.config.max_checkpoint_age;
        let mut pending = stream::iter(self.config.endpoints.iter().cloned().map(|endpoint| {
            let client = self.client.clone();
            async move {
                match sync_endpoint(&client, &endpoint, checkpoint, maximum_checkpoint_age).await {
                    Ok(result) => EndpointProbe {
                        endpoint,
                        verified: true,
                        anchor: Some(result.anchor),
                        checkpoint_anchor: Some(result.checkpoint_anchor),
                        updates_verified: result.updates_verified,
                        error: None,
                    },
                    Err(error) => EndpointProbe {
                        endpoint,
                        verified: false,
                        anchor: None,
                        checkpoint_anchor: None,
                        updates_verified: 0,
                        error: Some(error.to_string()),
                    },
                }
            }
        }))
        .buffer_unordered(maximum_concurrency);
        let mut completed = Vec::with_capacity(maximum_concurrency);
        while let Some(endpoint) = pending.next().await {
            completed.push(endpoint);
            let report = select_endpoint_agreement(
                checkpoint_root,
                self.config.minimum_agreement,
                completed.clone(),
            );
            if report.accepted {
                return report;
            }
        }
        select_endpoint_agreement(checkpoint_root, self.config.minimum_agreement, completed)
    }

    async fn take_primed_probe(&self, checkpoint_root: [u8; 32]) -> Option<FinalityProbeReport> {
        self.primed_probe
            .lock()
            .await
            .take()
            .filter(|primed| {
                primed.checkpoint_root == checkpoint_root
                    && primed.verified_at.elapsed() <= PRIMED_PROBE_MAX_AGE
            })
            .map(|primed| primed.report)
    }

    fn validate_checkpoint(
        checkpoint: &ConsensusCheckpoint,
        report: &FinalityProbeReport,
    ) -> Result<(), SourceError> {
        let accepted = report.selected.ok_or_else(|| {
            SourceError::Unavailable("no verified finality anchor was accepted".to_owned())
        })?;
        let bootstrap = report
            .endpoints
            .iter()
            .find(|endpoint| endpoint.verified)
            .and_then(|endpoint| endpoint.checkpoint_anchor)
            .ok_or_else(|| {
                SourceError::Protocol("verified bootstrap anchor is absent".to_owned())
            })?;
        if checkpoint.beacon_slot != bootstrap.beacon_slot {
            return Err(SourceError::Protocol(format!(
                "checkpoint slot mismatch: configured {}, verified {}",
                checkpoint.beacon_slot, bootstrap.beacon_slot
            )));
        }
        if checkpoint.beacon_block_root != bootstrap.beacon_block_root {
            return Err(SourceError::Protocol(
                "checkpoint beacon root differs from verified bootstrap".to_owned(),
            ));
        }
        if checkpoint.execution_block_hash != bootstrap.execution_block_hash {
            return Err(SourceError::Protocol(
                "checkpoint execution hash differs from verified bootstrap proof".to_owned(),
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
        let initial = match source.take_primed_probe(checkpoint.beacon_block_root).await {
            Some(initial) => initial,
            None => source.fetch_probe_root(checkpoint.beacon_block_root).await,
        };
        Self::validate_checkpoint(&checkpoint, &initial)?;
        if !initial.accepted {
            return Err(SourceError::Unavailable(format!(
                "finality quorum not reached: {}",
                initial.disagreements.join("; ")
            )));
        }
        let state = SubscriptionState {
            source,
            checkpoint,
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
    checkpoint: ConsensusCheckpoint,
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
        if let Some(anchor) = state.pending.take()
            && state.last_emitted != Some(anchor)
        {
            state.last_emitted = Some(anchor);
            return Some((
                Ok(FinalityEvent::Finalized {
                    block_hash: anchor.execution_block_hash,
                    beacon_slot: anchor.beacon_slot,
                    beacon_block_root: anchor.beacon_block_root,
                }),
                state,
            ));
        }
        tokio::select! {
            () = tokio::time::sleep(state.source.config.poll_interval) => {}
            () = state.cancellation.cancelled() => {
                state.terminal = true;
                return Some((Err(SourceError::Cancelled), state));
            }
        }
        let report = state
            .source
            .fetch_probe_root(state.checkpoint.beacon_block_root)
            .await;
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
    updates_verified: u64,
}

async fn sync_endpoint(
    client: &reqwest::Client,
    endpoint: &Url,
    checkpoint: B256,
    max_checkpoint_age: Duration,
) -> Result<EndpointSync, BeaconApiError> {
    verify_mainnet_endpoint(client, endpoint).await?;
    let bootstrap: BootstrapResponse<MainnetConsensusSpec> = get_json(
        client,
        endpoint,
        &format!("/eth/v1/beacon/light_client/bootstrap/{checkpoint:#x}"),
    )
    .await?;
    let mut verifier = MainnetLightClientVerifier::bootstrap(
        checkpoint.into(),
        &bootstrap.data,
        max_checkpoint_age,
    )?;
    let current_period = MainnetLightClientVerifier::current_period();
    let mut period = verifier.first_required_period();

    // A bootstrap already supplies its current sync committee. Period P's
    // update is needed only to cross into P+1, so do not request the optional
    // current-period update.
    while period < current_period {
        let count = current_period
            .saturating_sub(period)
            .min(MAX_UPDATES_PER_REQUEST);
        let updates: Vec<UpdateResponse<MainnetConsensusSpec>> = get_json(
            client,
            endpoint,
            &format!("/eth/v1/beacon/light_client/updates?start_period={period}&count={count}"),
        )
        .await?;
        if updates.len() != usize::try_from(count).expect("count is at most 128") {
            return Err(BeaconApiError::Protocol(format!(
                "endpoint returned {} updates for requested count {count}",
                updates.len()
            )));
        }
        for update in updates {
            verifier.apply_update(&update.data)?;
        }
        period = period.saturating_add(count);
    }

    let finality: FinalityUpdateResponse<MainnetConsensusSpec> = get_json(
        client,
        endpoint,
        "/eth/v1/beacon/light_client/finality_update",
    )
    .await?;
    let anchor = verifier.apply_finality_update(&finality.data)?;
    Ok(EndpointSync {
        anchor,
        checkpoint_anchor: verifier.checkpoint_anchor(),
        updates_verified: verifier.updates_verified(),
    })
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
    client: &reqwest::Client,
    endpoint: &Url,
) -> Result<(), BeaconApiError> {
    let response: GenesisResponse = get_json(client, endpoint, "/eth/v1/beacon/genesis").await?;
    if response.data.genesis_validators_root != MAINNET_GENESIS_ROOT {
        return Err(BeaconApiError::WrongNetwork(
            response.data.genesis_validators_root,
        ));
    }
    Ok(())
}

async fn get_json<T: DeserializeOwned>(
    client: &reqwest::Client,
    endpoint: &Url,
    path_and_query: &str,
) -> Result<T, BeaconApiError> {
    let url = endpoint
        .join(path_and_query.trim_start_matches('/'))
        .map_err(BeaconApiError::Url)?;
    let response =
        client
            .get(url.clone())
            .send()
            .await
            .map_err(|source| BeaconApiError::Request {
                url: url.clone(),
                source,
            })?;
    let status = response.status();
    let bytes = response
        .bytes()
        .await
        .map_err(|source| BeaconApiError::Request {
            url: url.clone(),
            source,
        })?;
    if !status.is_success() {
        let detail = String::from_utf8_lossy(&bytes);
        return Err(BeaconApiError::Status {
            url,
            status: status.as_u16(),
            detail: detail.chars().take(512).collect(),
        });
    }
    serde_json::from_slice(&bytes).map_err(|source| BeaconApiError::Decode { url, source })
}

fn verify_checkpoint_age(slot: u64, max_age: Duration) -> Result<(), BeaconApiError> {
    let checkpoint_time = MAINNET_GENESIS_TIME
        .checked_add(slot.saturating_mul(SLOT_SECONDS))
        .ok_or(BeaconApiError::InvalidCheckpointTime)?;
    let now = SystemTime::now()
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

fn select_endpoint_agreement(
    checkpoint_root: [u8; 32],
    minimum_agreement: usize,
    endpoints: Vec<EndpointProbe>,
) -> FinalityProbeReport {
    let mut by_slot_and_hash = BTreeMap::<VerifiedFinalityAnchor, usize>::new();
    let mut same_slot = BTreeMap::<u64, Vec<VerifiedFinalityAnchor>>::new();
    for anchor in endpoints.iter().filter_map(|endpoint| endpoint.anchor) {
        *by_slot_and_hash.entry(anchor).or_default() += 1;
        same_slot
            .entry(anchor.beacon_slot)
            .or_default()
            .push(anchor);
    }
    let mut disagreements = Vec::new();
    for (slot, anchors) in same_slot {
        let first = anchors[0];
        if anchors.iter().any(|anchor| *anchor != first) {
            disagreements.push(format!(
                "verified endpoints returned conflicting roots or execution hashes at slot {slot}"
            ));
        }
    }
    let selection = by_slot_and_hash
        .into_iter()
        .filter(|(_, count)| *count >= minimum_agreement)
        .max_by_key(|(anchor, count)| (anchor.beacon_slot, *count));
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
        minimum_agreement,
        accepted: selected.is_some() && disagreements.is_empty(),
        selected,
        agreeing_endpoints,
        disagreements,
        endpoints,
    }
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
        url: Url,
        #[source]
        source: reqwest::Error,
    },
    #[error("beacon API {url} returned HTTP {status}: {detail}")]
    Status {
        url: Url,
        status: u16,
        detail: String,
    },
    #[error("cannot decode beacon API response from {url}: {source}")]
    Decode {
        url: Url,
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
            endpoint: Url::parse(url).expect("URL"),
            verified: anchor.is_some(),
            anchor,
            checkpoint_anchor: anchor.map(|anchor| VerifiedFinalityAnchor {
                beacon_slot: anchor.beacon_slot.saturating_sub(1),
                ..anchor
            }),
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
            disagreement.contains("https://unavailable.example/")
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
