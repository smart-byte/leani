use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{Duration, Instant},
};

use futures::StreamExt;
use leani_primitives::{BlockNumber, BlockRange, Finality, TrustModel};
use leani_source_api::{
    DataRequest, HistorySource, SourceBudget, SourceDescriptor, SourceError, VerificationPolicy,
};
use serde::Serialize;
use thiserror::Error;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::{
    HistoryStore, HistoryStoreError, PendingSegment, RawHistoryJob, RawHistoryJobId,
    RawHistoryJobState, SegmentDescriptor, SegmentError, SegmentId, SegmentOwnerClaim,
    SegmentOwnerKind, SegmentReservation, StorageLimitAction, VerificationClass,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct RawHistorySourcePolicyEntry {
    id: String,
    priority: u16,
    schema_version: String,
    acquisition_identity: Vec<u8>,
}

/// Ordered immutable source set used by raw-history job identity and failover.
#[derive(Clone)]
pub struct RawHistorySourceSet {
    sources: Vec<Arc<dyn HistorySource>>,
    digest: [u8; 32],
}

impl std::fmt::Debug for RawHistorySourceSet {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RawHistorySourceSet")
            .field(
                "sources",
                &self
                    .sources
                    .iter()
                    .map(|source| source.descriptor().id.to_string())
                    .collect::<Vec<_>>(),
            )
            .field("digest", &hex::encode(self.digest))
            .finish()
    }
}

impl RawHistorySourceSet {
    /// Build one deterministic priority-ordered source policy.
    ///
    /// # Errors
    ///
    /// Rejects an empty set or duplicate source identities.
    pub fn new(mut sources: Vec<Arc<dyn HistorySource>>) -> Result<Self, RawHistoryRunError> {
        if sources.is_empty() {
            return Err(RawHistoryRunError::InvalidSourceSet(
                "at least one historical source is required".to_owned(),
            ));
        }
        let mut identities = BTreeSet::new();
        for source in &sources {
            if !identities.insert(source.descriptor().id.to_string()) {
                return Err(RawHistoryRunError::InvalidSourceSet(format!(
                    "duplicate source ID `{}`",
                    source.descriptor().id
                )));
            }
        }
        sources.sort_by(|left, right| {
            let left = left.descriptor();
            let right = right.descriptor();
            (left.priority, left.id.as_str()).cmp(&(right.priority, right.id.as_str()))
        });
        let entries = sources
            .iter()
            .map(|source| {
                let descriptor = source.descriptor();
                RawHistorySourcePolicyEntry {
                    id: descriptor.id.to_string(),
                    priority: descriptor.priority,
                    schema_version: descriptor.schema_version.clone(),
                    acquisition_identity: source.acquisition_identity(),
                }
            })
            .collect::<Vec<_>>();
        let encoded = postcard::to_allocvec(&entries)
            .map_err(|error| RawHistoryRunError::InvalidSourceSet(error.to_string()))?;
        Ok(Self {
            sources,
            digest: *blake3::hash(&encoded).as_bytes(),
        })
    }

    #[must_use]
    pub const fn policy_digest(&self) -> [u8; 32] {
        self.digest
    }

    #[must_use]
    pub fn sources(&self) -> &[Arc<dyn HistorySource>] {
        &self.sources
    }
}

/// Non-terminal stop conditions are returned explicitly so shutdown is not
/// confused with durable cancellation or storage pressure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RawHistoryRunOutcome {
    Complete(RawHistoryJob),
    Cancelled(RawHistoryJob),
    Interrupted(RawHistoryJob),
    StorageBackpressured(RawHistoryJob),
}

#[derive(Debug, Error)]
pub enum RawHistoryRunError {
    #[error("invalid raw-history source set: {0}")]
    InvalidSourceSet(String),
    #[error("raw-history job `{0}` is already running in this process")]
    ConcurrentRun(String),
    #[error("raw-history job source policy changed: expected {expected}, configured {configured}")]
    SourcePolicyChanged {
        expected: String,
        configured: String,
    },
    #[error("no source could satisfy raw-history range {range:?}: {reasons:?}")]
    NoCompatibleSource {
        range: BlockRange,
        reasons: Vec<String>,
    },
    #[error("all compatible sources are temporarily unavailable for {range:?}: {reasons:?}")]
    SourcesUnavailable {
        range: BlockRange,
        reasons: Vec<String>,
    },
    #[error(transparent)]
    Store(#[from] HistoryStoreError),
}

/// A range that only lagging sources lack is retried for at least this long,
/// which covers a dataset published in daily partitions, such as Xatu.
const LAG_RETRY_FLOOR: Duration = Duration::from_hours(24);
/// A lagging source also gets this many of its expected lags to publish a
/// range.
const LAG_RETRY_EXPECTED_LAGS: u32 = 4;

/// Resumable segment-boundary acquisition executor for durable raw jobs.
#[derive(Clone, Debug)]
pub struct RawHistoryRunner {
    store: HistoryStore,
    sources: RawHistorySourceSet,
    source_budget: SourceBudget,
    active: Arc<Mutex<BTreeSet<RawHistoryJobId>>>,
    lagging: Arc<std::sync::Mutex<BTreeMap<RawHistoryJobId, LagClock>>>,
}

/// When this process first found a job's range missing from lagging sources.
#[derive(Clone, Copy, Debug)]
struct LagClock {
    /// Tells the job from an earlier one under the same ID.
    created_at_unix_ms: u64,
    /// The first block of the range that the lagging sources lack.
    start: BlockNumber,
    since: Instant,
}

impl RawHistoryRunner {
    /// Construct a runner with a validated, immutable source budget.
    ///
    /// # Errors
    ///
    /// Rejects any zero hard limit.
    pub fn new(
        store: HistoryStore,
        sources: RawHistorySourceSet,
        source_budget: SourceBudget,
    ) -> Result<Self, RawHistoryRunError> {
        source_budget
            .validate()
            .map_err(|error| RawHistoryRunError::InvalidSourceSet(error.to_string()))?;
        Ok(Self {
            store,
            sources,
            source_budget,
            active: Arc::new(Mutex::new(BTreeSet::new())),
            lagging: Arc::new(std::sync::Mutex::new(BTreeMap::new())),
        })
    }

    /// When this process first found `job`'s range starting at `start`
    /// missing from lagging sources.
    fn lagging_since(&self, job: &RawHistoryJob, start: BlockNumber) -> Instant {
        let mut lagging = self
            .lagging
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match lagging.get(&job.id) {
            Some(clock)
                if clock.created_at_unix_ms == job.created_at_unix_ms && clock.start == start =>
            {
                clock.since
            }
            _ => {
                let since = Instant::now();
                lagging.insert(
                    job.id.clone(),
                    LagClock {
                        created_at_unix_ms: job.created_at_unix_ms,
                        start,
                        since,
                    },
                );
                since
            }
        }
    }

    fn clear_lagging(&self, id: &RawHistoryJobId) {
        self.lagging
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(id);
    }

    /// Move the first sight of `id`'s lagging range `by` into the past.
    #[cfg(test)]
    fn backdate_lag(&self, id: &RawHistoryJobId, by: Duration) {
        if let Some(clock) = self
            .lagging
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_mut(id)
        {
            clock.since = clock.since.checked_sub(by).expect("backdated instant");
        }
    }

    #[must_use]
    pub fn source_policy_digest(&self) -> [u8; 32] {
        self.sources.policy_digest()
    }

    /// Resume one durable job until completion, durable cancellation, storage
    /// backpressure, or process-scoped cancellation.
    ///
    /// # Errors
    ///
    /// Fails durably when the immutable source policy changed, no source can
    /// satisfy a segment, or validated material cannot be published.
    pub async fn run(
        &self,
        id: &RawHistoryJobId,
        cancellation: CancellationToken,
    ) -> Result<RawHistoryRunOutcome, RawHistoryRunError> {
        {
            let mut active = self.active.lock().await;
            if !active.insert(id.clone()) {
                return Err(RawHistoryRunError::ConcurrentRun(id.as_str().to_owned()));
            }
        }
        let result = Box::pin(self.run_exclusive(id, cancellation)).await;
        // Only a job that still waits for its sources keeps its lag clock.
        if !matches!(
            result,
            Ok(RawHistoryRunOutcome::Interrupted(_))
                | Err(RawHistoryRunError::SourcesUnavailable { .. })
        ) {
            self.clear_lagging(id);
        }
        self.active.lock().await.remove(id);
        result
    }

    #[allow(clippy::too_many_lines)]
    async fn run_exclusive(
        &self,
        id: &RawHistoryJobId,
        cancellation: CancellationToken,
    ) -> Result<RawHistoryRunOutcome, RawHistoryRunError> {
        let mut initial = self
            .store
            .raw_history_job(id)
            .await?
            .ok_or_else(|| HistoryStoreError::UnknownJob(id.as_str().to_owned()))?;
        match initial.state {
            RawHistoryJobState::Complete => return Ok(RawHistoryRunOutcome::Complete(initial)),
            RawHistoryJobState::Cancelled => {
                return Ok(RawHistoryRunOutcome::Cancelled(initial));
            }
            RawHistoryJobState::Failed => {
                return Err(HistoryStoreError::JobState {
                    id: id.as_str().to_owned(),
                    state: "failed".to_owned(),
                }
                .into());
            }
            RawHistoryJobState::Queued
            | RawHistoryJobState::Running
            | RawHistoryJobState::StorageBackpressured => {}
        }
        self.store
            .claim_compatible_segments_for_raw_history_job(id)
            .await?;
        initial = self
            .store
            .raw_history_job(id)
            .await?
            .ok_or_else(|| HistoryStoreError::UnknownJob(id.as_str().to_owned()))?;
        if initial.state == RawHistoryJobState::Complete {
            return Ok(RawHistoryRunOutcome::Complete(initial));
        }
        if initial.spec.source_policy_digest != self.sources.policy_digest() {
            let error = RawHistoryRunError::SourcePolicyChanged {
                expected: hex::encode(initial.spec.source_policy_digest),
                configured: hex::encode(self.sources.policy_digest()),
            };
            self.store
                .fail_raw_history_job(id, &error.to_string())
                .await?;
            return Err(error);
        }
        self.store.start_raw_history_job(id).await?;

        loop {
            let job = self
                .store
                .raw_history_job(id)
                .await?
                .ok_or_else(|| HistoryStoreError::UnknownJob(id.as_str().to_owned()))?;
            match job.state {
                RawHistoryJobState::Complete => {
                    return Ok(RawHistoryRunOutcome::Complete(job));
                }
                RawHistoryJobState::Cancelled => {
                    return Ok(RawHistoryRunOutcome::Cancelled(job));
                }
                RawHistoryJobState::Failed => {
                    return Err(HistoryStoreError::JobState {
                        id: id.as_str().to_owned(),
                        state: "failed".to_owned(),
                    }
                    .into());
                }
                RawHistoryJobState::Queued
                | RawHistoryJobState::Running
                | RawHistoryJobState::StorageBackpressured => {}
            }
            if cancellation.is_cancelled() {
                return Ok(RawHistoryRunOutcome::Interrupted(job));
            }
            let Some(remaining) = job.remaining_ranges.first().copied() else {
                let reconciled = self.store.reconcile_raw_history_job(id).await?;
                return Ok(RawHistoryRunOutcome::Complete(reconciled));
            };
            let maximum_blocks = job
                .spec
                .segment
                .target_blocks
                .min(self.source_budget.max_frames)
                .max(1);
            let mut blocks = remaining.len().min(maximum_blocks);
            loop {
                let range = prefix(remaining, blocks)?;
                match Box::pin(self.acquire_segment(&job, range, cancellation.clone())).await {
                    Ok(()) => {
                        self.clear_lagging(id);
                        break;
                    }
                    Err(AcquireError::Resize) if blocks > 1 => {
                        blocks = blocks.div_ceil(2);
                    }
                    Err(AcquireError::Interrupted) => {
                        let current =
                            self.store.raw_history_job(id).await?.ok_or_else(|| {
                                HistoryStoreError::UnknownJob(id.as_str().to_owned())
                            })?;
                        return Ok(RawHistoryRunOutcome::Interrupted(current));
                    }
                    Err(AcquireError::Store(
                        error @ (HistoryStoreError::LogicalBudget { .. }
                        | HistoryStoreError::PhysicalBudget { .. }),
                    )) => {
                        return self.handle_storage_limit(id, &job, error).await;
                    }
                    Err(AcquireError::Store(error)) => {
                        self.store
                            .fail_raw_history_job(id, &error.to_string())
                            .await?;
                        return Err(error.into());
                    }
                    Err(AcquireError::Sources { reasons, retry }) => {
                        return Err(self
                            .handle_source_failures(&job, range, reasons, retry)
                            .await?);
                    }
                    Err(AcquireError::Resize) => {
                        let error = HistoryStoreError::InvalidJob(format!(
                            "one block in range {range:?} exceeds the configured segment limits"
                        ));
                        self.store
                            .fail_raw_history_job(id, &error.to_string())
                            .await?;
                        return Err(error.into());
                    }
                }
            }
        }
    }

    /// Keep a job waiting while its sources can still deliver `range`, with
    /// their reasons as its last error, or fail it.
    async fn handle_source_failures(
        &self,
        job: &RawHistoryJob,
        range: BlockRange,
        mut reasons: Vec<String>,
        retry: SourceRetry,
    ) -> Result<RawHistoryRunError, HistoryStoreError> {
        let id = &job.id;
        let exhausted = match retry {
            SourceRetry::Transient => false,
            SourceRetry::Lagging { limit } => {
                let exhausted = self.lagging_since(job, range.start()).elapsed() >= limit;
                if exhausted {
                    reasons.push(format!(
                        "the range stayed unpublished past its {} hour retry bound",
                        limit.as_secs() / 3_600
                    ));
                }
                exhausted
            }
            SourceRetry::Terminal => true,
        };
        if exhausted {
            let error = RawHistoryRunError::NoCompatibleSource { range, reasons };
            self.store
                .fail_raw_history_job(id, &error.to_string())
                .await?;
            return Ok(error);
        }
        let error = RawHistoryRunError::SourcesUnavailable { range, reasons };
        self.store
            .wait_raw_history_job(id, &error.to_string())
            .await?;
        Ok(error)
    }

    async fn handle_storage_limit(
        &self,
        id: &RawHistoryJobId,
        job: &RawHistoryJob,
        error: HistoryStoreError,
    ) -> Result<RawHistoryRunOutcome, RawHistoryRunError> {
        match job.spec.segment.on_limit {
            StorageLimitAction::Pause => Ok(RawHistoryRunOutcome::StorageBackpressured(
                self.store
                    .backpressure_raw_history_job(id, &error.to_string())
                    .await?,
            )),
            StorageLimitAction::Fail => {
                self.store
                    .fail_raw_history_job(id, &error.to_string())
                    .await?;
                Err(error.into())
            }
        }
    }

    async fn acquire_segment(
        &self,
        job: &RawHistoryJob,
        range: BlockRange,
        cancellation: CancellationToken,
    ) -> Result<(), AcquireError> {
        let request = DataRequest {
            chain_id: job.spec.chain_id,
            range,
            required: job.spec.required_capabilities,
            allow_filtered: job.spec.material.allow_filtered,
            projection: job.spec.material.projection.clone(),
            log_fields: job.spec.material.log_fields,
            filters: job.spec.material.filters.clone(),
            minimum_finality: Finality::Finalized,
            verification_policy: verification_policy(job.spec.verification),
        };
        let mut reasons = Vec::new();
        let mut retry = None;
        for source in self.sources.sources() {
            if cancellation.is_cancelled() {
                return Err(AcquireError::Interrupted);
            }
            let descriptor = source.descriptor();
            // A source that does not advertise the whole range cannot serve
            // it, so its miss says nothing about the range: an archive of
            // older blocks does not fail a range that a catalog has yet to
            // publish. A hole inside an advertised range still counts.
            if let Some(advertised) = descriptor.range.filter(|advertised| {
                !advertised.contains(range.start()) || !advertised.contains(range.end())
            }) {
                reasons.push(format!(
                    "{}: advertises blocks {} to {} only",
                    descriptor.id,
                    advertised.start().0,
                    advertised.end().0
                ));
                continue;
            }
            if descriptor.chain_id != job.spec.chain_id
                || !descriptor.finality.supports(Finality::Finalized)
                || !descriptor
                    .complete_capabilities
                    .with_derivable()
                    .contains_all(job.spec.required_capabilities)
                || descriptor.trust < job.spec.minimum_trust
            {
                reasons.push(format!("{}: incompatible descriptor", descriptor.id));
                continue;
            }
            let plan = match source.plan(&request).await {
                Ok(plan)
                    if plan
                        .complete
                        .with_derivable()
                        .contains_all(job.spec.required_capabilities)
                        && plan.trust >= job.spec.minimum_trust =>
                {
                    plan
                }
                Ok(_) => {
                    reasons.push(format!("{}: incomplete plan", descriptor.id));
                    continue;
                }
                Err(error) => {
                    retry = Some(SourceRetry::of(descriptor, &error).or(retry));
                    reasons.push(format!("{}: {error}", descriptor.id));
                    continue;
                }
            };
            match Box::pin(self.stream_plan_into_segment(
                source,
                job,
                &plan.chunks,
                range,
                plan.trust,
                cancellation.clone(),
            ))
            .await
            {
                Ok(()) => return Ok(()),
                Err(AcquireError::Sources {
                    reasons: mut errors,
                    retry: source_retry,
                }) => {
                    reasons.append(&mut errors);
                    retry = Some(source_retry.or(retry));
                }
                Err(other) => return Err(other),
            }
        }
        Err(AcquireError::Sources {
            reasons,
            retry: retry.unwrap_or(SourceRetry::Terminal),
        })
    }

    #[allow(clippy::too_many_lines)]
    async fn stream_plan_into_segment(
        &self,
        source: &Arc<dyn HistorySource>,
        job: &RawHistoryJob,
        chunks: &[leani_source_api::SourceChunk],
        range: BlockRange,
        trust: TrustModel,
        cancellation: CancellationToken,
    ) -> Result<(), AcquireError> {
        let segment_id = segment_id(job, range)?;
        let mut pending = None;
        for chunk in chunks {
            let mut stream = match source
                .open(chunk, self.source_budget, cancellation.clone())
                .await
            {
                Ok(stream) => stream,
                Err(error) => {
                    Box::pin(abort_pending(&mut pending)).await?;
                    return Err(source_attempt_error(source, &error));
                }
            };
            while let Some(item) = stream.next().await {
                if cancellation.is_cancelled() {
                    Box::pin(abort_pending(&mut pending)).await?;
                    return Err(AcquireError::Interrupted);
                }
                let frame = match item {
                    Ok(frame) => frame,
                    Err(error) => {
                        Box::pin(abort_pending(&mut pending)).await?;
                        return Err(source_attempt_error(source, &error));
                    }
                };
                if let Err(reason) = job.spec.validate_frame_profile(&frame) {
                    Box::pin(abort_pending(&mut pending)).await?;
                    return Err(source_attempt_error(
                        source,
                        &SourceError::InvalidPlan(reason.to_owned()),
                    ));
                }
                if pending.is_none() {
                    let capabilities = frame.capabilities();
                    if !capabilities
                        .complete
                        .with_derivable()
                        .contains_all(job.spec.required_capabilities)
                    {
                        return Err(source_attempt_error(
                            source,
                            &SourceError::InvalidPlan(
                                "first frame lacks required complete capabilities".to_owned(),
                            ),
                        ));
                    }
                    pending = Some(
                        self.store
                            .begin_segment_profiled(
                                segment_id.clone(),
                                SegmentDescriptor {
                                    chain_id: job.spec.chain_id,
                                    range,
                                    material_shape: job.spec.material.shape_id(),
                                    present_capabilities: capabilities.present,
                                    complete_capabilities: capabilities.complete,
                                    verification: job.spec.verification,
                                    trust,
                                },
                                job.spec.segment.compression,
                                SegmentReservation::new(
                                    job.spec.segment.maximum_logical_bytes,
                                    job.spec.segment.maximum_physical_bytes,
                                ),
                                job.spec.profile,
                                job.spec.indexes,
                            )
                            .await?,
                    );
                }
                if let Err(error) = pending
                    .as_mut()
                    .expect("pending segment was initialized")
                    .append(&frame)
                {
                    Box::pin(abort_pending(&mut pending)).await?;
                    if matches!(
                        error,
                        HistoryStoreError::Segment(
                            SegmentError::SegmentLogicalBudget { .. }
                                | SegmentError::SegmentPhysicalBudget { .. }
                                | SegmentError::CapabilityMismatch
                        )
                    ) {
                        return Err(AcquireError::Resize);
                    }
                    return Err(AcquireError::Store(error));
                }
            }
        }
        let pending = pending.ok_or_else(|| {
            source_attempt_error(
                source,
                &SourceError::InvalidPlan("source returned no frames".to_owned()),
            )
        })?;
        pending
            .commit(&[SegmentOwnerClaim {
                kind: SegmentOwnerKind::RawHistoryJob,
                owner_id: job.id.as_str().to_owned(),
            }])
            .await?;
        Ok(())
    }
}

#[derive(Debug)]
enum AcquireError {
    Interrupted,
    Resize,
    Sources {
        reasons: Vec<String>,
        retry: SourceRetry,
    },
    Store(HistoryStoreError),
}

/// Whether the sources that failed a range may still deliver it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SourceRetry {
    /// A transport failure, which can clear.
    Transient,
    /// A source cannot deliver the range, or failed it for good.
    Terminal,
    /// Only lagging sources lack the range; retry it for `limit`.
    Lagging { limit: Duration },
}

impl SourceRetry {
    fn of(descriptor: &SourceDescriptor, error: &SourceError) -> Self {
        match error {
            SourceError::Disconnected(_)
            | SourceError::Unavailable(_)
            | SourceError::Protocol(_) => Self::Transient,
            // A source that publishes with a delay, such as an eraE catalog,
            // may cover the range later.
            SourceError::MissingRange(_) | SourceError::IncompleteRange { .. }
                if !descriptor.expected_lag.is_zero() =>
            {
                Self::Lagging {
                    limit: descriptor
                        .expected_lag
                        .saturating_mul(LAG_RETRY_EXPECTED_LAGS)
                        .max(LAG_RETRY_FLOOR),
                }
            }
            _ => Self::Terminal,
        }
    }

    /// Combine two sources' failures. A transport failure keeps the range
    /// retryable, and a terminal failure outranks lag, so a lagging source
    /// cannot hide another source's error.
    fn or(self, other: Option<Self>) -> Self {
        match (self, other) {
            (Self::Transient, _) | (_, Some(Self::Transient)) => Self::Transient,
            (Self::Terminal, _) | (_, Some(Self::Terminal)) => Self::Terminal,
            (Self::Lagging { limit }, Some(Self::Lagging { limit: other })) => Self::Lagging {
                limit: limit.max(other),
            },
            (lagging @ Self::Lagging { .. }, None) => lagging,
        }
    }
}

impl From<HistoryStoreError> for AcquireError {
    fn from(error: HistoryStoreError) -> Self {
        Self::Store(error)
    }
}

fn source_attempt_error(source: &Arc<dyn HistorySource>, error: &SourceError) -> AcquireError {
    if error == &SourceError::Cancelled {
        AcquireError::Interrupted
    } else {
        AcquireError::Sources {
            reasons: vec![format!("{}: {error}", source.descriptor().id)],
            retry: SourceRetry::of(source.descriptor(), error),
        }
    }
}

async fn abort_pending(pending: &mut Option<PendingSegment>) -> Result<(), HistoryStoreError> {
    if let Some(pending) = pending.take() {
        pending.abort().await?;
    }
    Ok(())
}

fn segment_id(job: &RawHistoryJob, range: BlockRange) -> Result<SegmentId, HistoryStoreError> {
    SegmentId::new(format!(
        "raw-{}-{}-{}",
        hex::encode(&job.identity[..8]),
        range.start().0,
        range.end().0
    ))
    .map_err(HistoryStoreError::Segment)
}

fn prefix(range: BlockRange, blocks: u64) -> Result<BlockRange, HistoryStoreError> {
    let end = range
        .start()
        .0
        .checked_add(blocks.saturating_sub(1))
        .map(|end| end.min(range.end().0))
        .ok_or(HistoryStoreError::ArithmeticOverflow)?;
    BlockRange::new(range.start(), BlockNumber(end))
        .map_err(|error| HistoryStoreError::InvalidJob(error.to_string()))
}

const fn verification_policy(class: VerificationClass) -> VerificationPolicy {
    match class {
        VerificationClass::BestEffort => VerificationPolicy::BestEffort,
        VerificationClass::TrustedDataset => VerificationPolicy::TrustedDataset,
        VerificationClass::Cryptographic => VerificationPolicy::CompleteCryptographic,
    }
}

#[cfg(test)]
mod tests {
    use leani_primitives::{
        BlockFrame, BlockHash, Capability, CapabilitySet, ChainId, Material, TrustModel,
    };
    use leani_source_api::{FieldProjection, FilterSet};
    use leani_testkit::{
        HistoryStep, ScriptedChunk, ScriptedHistorySource, default_source_budget, fixture_frame,
        fixture_source_descriptor,
    };
    use tempfile::tempdir;

    use super::*;
    use crate::{
        Compression, HistoryStoreConfig, RawHistoryIndexPolicy, RawHistoryJobSpec,
        RawHistoryMaterialProfile, RawHistoryRetention, RawHistorySegmentPolicy, StorageBudget,
    };

    fn frames(start: u64, end: u64) -> Vec<BlockFrame> {
        let mut parent = BlockHash::new([0x72; 32]);
        (start..=end)
            .map(|number| {
                let frame = fixture_frame(number, parent);
                parent = frame.block.hash;
                frame
            })
            .collect()
    }

    fn config(root: &std::path::Path) -> HistoryStoreConfig {
        HistoryStoreConfig::new(root).with_budget(StorageBudget {
            maximum_logical_bytes: 32 * 1024 * 1024,
            maximum_physical_bytes: 32 * 1024 * 1024,
            maximum_frame_logical_bytes: 1024 * 1024,
            maximum_segment_logical_bytes: 4 * 1024 * 1024,
            maximum_segment_physical_bytes: 4 * 1024 * 1024,
        })
    }

    fn source_set(source: Arc<dyn HistorySource>) -> RawHistorySourceSet {
        RawHistorySourceSet::new(vec![source]).expect("source set")
    }

    fn spec(range: BlockRange, digest: [u8; 32]) -> RawHistoryJobSpec {
        RawHistoryJobSpec {
            chain_id: ChainId(1),
            ranges: vec![range],
            profile: crate::RawHistoryProfile::ProcessorReuse,
            material: RawHistoryMaterialProfile::default(),
            required_capabilities: CapabilitySet::from_iter([
                Capability::Transactions,
                Capability::Receipts,
            ]),
            verification: VerificationClass::Cryptographic,
            minimum_trust: TrustModel::ProtocolVerified,
            source_policy_digest: digest,
            retention: RawHistoryRetention::Full,
            segment: RawHistorySegmentPolicy {
                target_blocks: 3,
                maximum_logical_bytes: 1024 * 1024,
                maximum_physical_bytes: 1024 * 1024,
                compression: Compression::Snappy,
                on_limit: StorageLimitAction::Pause,
            },
            indexes: RawHistoryIndexPolicy::default(),
        }
    }

    #[tokio::test]
    async fn completes_job_in_segments_and_exposes_reusable_local_coverage() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(config(directory.path()))
            .await
            .expect("store");
        let all = frames(100, 105);
        let range = BlockRange::new(BlockNumber(100), BlockNumber(105)).expect("range");
        let source = Arc::new(ScriptedHistorySource::from_frames(
            fixture_source_descriptor("archive", range),
            all.clone(),
        ));
        let sources = source_set(source.clone());
        let id = RawHistoryJobId::new("raw-complete").expect("job ID");
        store
            .create_raw_history_job(id.clone(), spec(range, sources.policy_digest()))
            .await
            .expect("create");
        let runner =
            RawHistoryRunner::new(store.clone(), sources, default_source_budget()).expect("runner");
        let outcome = Box::pin(runner.run(&id, CancellationToken::new()))
            .await
            .expect("run");
        let RawHistoryRunOutcome::Complete(job) = outcome else {
            panic!("job did not complete")
        };
        assert_eq!(job.committed_segments, 2);
        assert!(job.remaining_ranges.is_empty());
        assert_eq!(source.plan_calls(), 2);
        assert_eq!(source.open_calls(), 2);

        let retained = crate::RetainedHistorySource::new(
            store.clone(),
            crate::RetainedHistorySourceConfig::local(
                ChainId(1),
                RawHistoryMaterialProfile::default().shape_id(),
                all[0].capabilities().complete,
                VerificationClass::Cryptographic,
                TrustModel::ProtocolVerified,
            )
            .expect("retained profile"),
        )
        .expect("retained source");
        let request = DataRequest {
            chain_id: ChainId(1),
            range,
            required: CapabilitySet::of(Capability::Transactions),
            allow_filtered: false,
            projection: FieldProjection::default(),
            log_fields: leani_primitives::LogFieldSet::NONE,
            filters: FilterSet::default(),
            minimum_finality: Finality::Finalized,
            verification_policy: VerificationPolicy::CompleteCryptographic,
        };
        let retained_plan = retained.plan(&request).await.expect("retained plan");
        assert_eq!(retained_plan.chunks.len(), 2);

        let second_id = RawHistoryJobId::new("raw-reuse").expect("job ID");
        let reuse_range = BlockRange::new(BlockNumber(101), BlockNumber(104)).expect("reuse range");
        store
            .create_raw_history_job(
                second_id.clone(),
                spec(reuse_range, runner.source_policy_digest()),
            )
            .await
            .expect("create reuse job");
        assert!(matches!(
            Box::pin(runner.run(&second_id, CancellationToken::new()))
                .await
                .expect("reuse retained material"),
            RawHistoryRunOutcome::Complete(_)
        ));
        assert_eq!(
            source.open_calls(),
            2,
            "reuse must perform zero source opens"
        );
    }

    #[tokio::test]
    async fn capability_fork_boundary_resizes_into_homogeneous_segments() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(config(directory.path()))
            .await
            .expect("store");
        let mut all = frames(120, 122);
        for frame in &mut all[1..] {
            frame.withdrawals = Material::Complete(Vec::new());
        }
        let range = BlockRange::new(BlockNumber(120), BlockNumber(122)).expect("range");
        let mut descriptor = fixture_source_descriptor("fork-boundary", range);
        descriptor.capabilities = descriptor.capabilities.with(Capability::Withdrawals);
        descriptor.complete_capabilities = descriptor
            .complete_capabilities
            .with(Capability::Withdrawals);
        let source = Arc::new(ScriptedHistorySource::from_frames(descriptor, all));
        let sources = source_set(source);
        let id = RawHistoryJobId::new("raw-fork-boundary").expect("job ID");
        store
            .create_raw_history_job(id.clone(), spec(range, sources.policy_digest()))
            .await
            .expect("create");
        let runner =
            RawHistoryRunner::new(store.clone(), sources, default_source_budget()).expect("runner");
        assert!(matches!(
            Box::pin(runner.run(&id, CancellationToken::new()))
                .await
                .expect("run"),
            RawHistoryRunOutcome::Complete(_)
        ));
        let segments = store.segments().await.expect("segments");
        assert_eq!(segments.len(), 2);
        assert_eq!(
            segments[0].metadata.descriptor.range,
            BlockRange::single(BlockNumber(120))
        );
        assert!(
            !segments[0]
                .metadata
                .descriptor
                .complete_capabilities
                .contains(Capability::Withdrawals)
        );
        assert_eq!(
            segments[1].metadata.descriptor.range,
            BlockRange::new(BlockNumber(121), BlockNumber(122)).expect("range")
        );
        assert!(
            segments[1]
                .metadata
                .descriptor
                .complete_capabilities
                .contains(Capability::Withdrawals)
        );
    }

    #[tokio::test]
    async fn interrupted_running_job_resumes_without_reacquiring_committed_segments() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(config(directory.path()))
            .await
            .expect("store");
        let all = frames(200, 204);
        let range = BlockRange::new(BlockNumber(200), BlockNumber(204)).expect("range");
        let source = Arc::new(ScriptedHistorySource::from_frames(
            fixture_source_descriptor("restart-archive", range),
            all,
        ));
        let sources = source_set(source.clone());
        let id = RawHistoryJobId::new("raw-restart").expect("job ID");
        store
            .create_raw_history_job(id.clone(), spec(range, sources.policy_digest()))
            .await
            .expect("create");
        let runner = RawHistoryRunner::new(store.clone(), sources.clone(), default_source_budget())
            .expect("runner");
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        assert!(matches!(
            Box::pin(runner.run(&id, cancelled))
                .await
                .expect("interrupt"),
            RawHistoryRunOutcome::Interrupted(_)
        ));
        assert_eq!(source.open_calls(), 0);
        assert!(matches!(
            Box::pin(runner.run(&id, CancellationToken::new()))
                .await
                .expect("resume"),
            RawHistoryRunOutcome::Complete(_)
        ));
        let job = store
            .raw_history_job(&id)
            .await
            .expect("inspect")
            .expect("job");
        assert_eq!(job.attempts, 2);
        assert_eq!(source.open_calls(), 2);
    }

    #[tokio::test]
    async fn changed_source_policy_fails_before_opening_a_source() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(config(directory.path()))
            .await
            .expect("store");
        let all = frames(300, 300);
        let range = BlockRange::single(BlockNumber(300));
        let source = Arc::new(ScriptedHistorySource::from_frames(
            fixture_source_descriptor("policy-archive", range),
            all,
        ));
        let sources = source_set(source.clone());
        let id = RawHistoryJobId::new("raw-policy").expect("job ID");
        store
            .create_raw_history_job(id.clone(), spec(range, [0x99; 32]))
            .await
            .expect("create");
        let runner =
            RawHistoryRunner::new(store.clone(), sources, default_source_budget()).expect("runner");
        assert!(matches!(
            Box::pin(runner.run(&id, CancellationToken::new())).await,
            Err(RawHistoryRunError::SourcePolicyChanged { .. })
        ));
        assert_eq!(source.open_calls(), 0);
        assert_eq!(
            store
                .raw_history_job(&id)
                .await
                .expect("inspect")
                .expect("job")
                .state,
            RawHistoryJobState::Failed
        );
    }

    /// A source that publishes ranges some time after they finalize, as an
    /// eraE catalog or a Xatu table does.
    fn lagging_descriptor(id: &str, range: BlockRange) -> leani_source_api::SourceDescriptor {
        let mut descriptor = fixture_source_descriptor(id, range);
        descriptor.expected_lag = std::time::Duration::from_hours(2);
        descriptor
    }

    /// A source whose only chunk covers `range` and fails with `error`.
    fn failing_source(
        descriptor: leani_source_api::SourceDescriptor,
        range: BlockRange,
        error: SourceError,
    ) -> Arc<ScriptedHistorySource> {
        let schema_version = descriptor.schema_version.clone();
        Arc::new(ScriptedHistorySource::new(
            descriptor,
            vec![ScriptedChunk {
                range,
                schema_version,
                estimated_bytes: None,
                steps: vec![HistoryStep::Error(error)],
            }],
        ))
    }

    async fn run_new_job(
        store: &HistoryStore,
        name: &str,
        range: BlockRange,
        sources: Vec<Arc<dyn HistorySource>>,
    ) -> (
        Result<RawHistoryRunOutcome, RawHistoryRunError>,
        RawHistoryJob,
    ) {
        let sources = RawHistorySourceSet::new(sources).expect("source set");
        let id = RawHistoryJobId::new(name).expect("job ID");
        store
            .create_raw_history_job(id.clone(), spec(range, sources.policy_digest()))
            .await
            .expect("create");
        let runner =
            RawHistoryRunner::new(store.clone(), sources, default_source_budget()).expect("runner");
        let outcome = Box::pin(runner.run(&id, CancellationToken::new())).await;
        let job = store
            .raw_history_job(&id)
            .await
            .expect("inspect")
            .expect("job");
        (outcome, job)
    }

    #[tokio::test]
    async fn ranges_a_lagging_source_has_not_published_yet_leave_the_job_running() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(config(directory.path()))
            .await
            .expect("store");
        let range = BlockRange::new(BlockNumber(500), BlockNumber(502)).expect("range");
        // The catalog lists only the first block so far.
        let lagging_catalog = Arc::new(ScriptedHistorySource::from_frames(
            lagging_descriptor("lagging-catalog", range),
            frames(500, 500),
        ));
        let lagging_table = failing_source(
            lagging_descriptor("lagging-table", range),
            range,
            SourceError::IncompleteRange {
                range,
                detail: "the dataset has not covered the range yet".to_owned(),
            },
        );
        for (name, source) in [
            (
                "raw-lagging-catalog",
                lagging_catalog as Arc<dyn HistorySource>,
            ),
            ("raw-lagging-table", lagging_table),
        ] {
            let (outcome, job) = run_new_job(&store, name, range, vec![source]).await;
            // Audit M-H5: lag was a terminal job failure.
            assert!(
                matches!(outcome, Err(RawHistoryRunError::SourcesUnavailable { .. })),
                "{name}: {outcome:?}"
            );
            assert_eq!(job.state, RawHistoryJobState::Running, "{name}");
            // Review I1: the job showed no reason while it waited.
            assert!(
                job.last_error
                    .as_deref()
                    .is_some_and(|error| error.contains("lagging-")),
                "{name}: {:?}",
                job.last_error
            );
        }
    }

    #[tokio::test]
    async fn a_lagging_range_fails_its_job_after_the_retry_bound() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(config(directory.path()))
            .await
            .expect("store");
        let range = BlockRange::new(BlockNumber(800), BlockNumber(802)).expect("range");
        let source = Arc::new(ScriptedHistorySource::from_frames(
            lagging_descriptor("stale-catalog", range),
            frames(800, 800),
        ));
        let sources = source_set(source);
        let id = RawHistoryJobId::new("raw-lag-bound").expect("job ID");
        store
            .create_raw_history_job(id.clone(), spec(range, sources.policy_digest()))
            .await
            .expect("create");
        let runner =
            RawHistoryRunner::new(store.clone(), sources, default_source_budget()).expect("runner");
        assert!(matches!(
            Box::pin(runner.run(&id, CancellationToken::new())).await,
            Err(RawHistoryRunError::SourcesUnavailable { .. })
        ));
        // Two hours of expected lag leave the range the 24-hour floor.
        runner.backdate_lag(&id, std::time::Duration::from_hours(23));
        assert!(matches!(
            Box::pin(runner.run(&id, CancellationToken::new())).await,
            Err(RawHistoryRunError::SourcesUnavailable { .. })
        ));
        // Review I1: the range was retried forever.
        runner.backdate_lag(&id, std::time::Duration::from_hours(2));
        match Box::pin(runner.run(&id, CancellationToken::new())).await {
            Err(RawHistoryRunError::NoCompatibleSource { reasons, .. }) => assert!(
                reasons.iter().any(|reason| reason.contains("retry bound")),
                "{reasons:?}"
            ),
            other => panic!("the lagging range was retried past its bound: {other:?}"),
        }
        assert_eq!(
            store
                .raw_history_job(&id)
                .await
                .expect("inspect")
                .expect("job")
                .state,
            RawHistoryJobState::Failed
        );
    }

    #[tokio::test]
    async fn a_range_missing_from_a_source_without_lag_fails_the_job() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(config(directory.path()))
            .await
            .expect("store");
        let range = BlockRange::new(BlockNumber(600), BlockNumber(602)).expect("range");
        // A local archive with a hole never fills it.
        let archive = Arc::new(ScriptedHistorySource::from_frames(
            fixture_source_descriptor("static-archive", range),
            frames(600, 600),
        ));
        let (outcome, job) = run_new_job(&store, "raw-static-gap", range, vec![archive]).await;
        assert!(
            matches!(outcome, Err(RawHistoryRunError::NoCompatibleSource { .. })),
            "{outcome:?}"
        );
        assert_eq!(job.state, RawHistoryJobState::Failed);
    }

    #[tokio::test]
    async fn one_source_lagging_does_not_hide_another_source_failing() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(config(directory.path()))
            .await
            .expect("store");
        let range = BlockRange::new(BlockNumber(700), BlockNumber(702)).expect("range");
        let lagging = Arc::new(ScriptedHistorySource::from_frames(
            lagging_descriptor("a-lagging", range),
            frames(700, 700),
        ));
        let corrupt = failing_source(
            fixture_source_descriptor("b-corrupt", range),
            range,
            SourceError::CorruptFrame("receipts root mismatch".to_owned()),
        );
        let (outcome, job) = run_new_job(&store, "raw-masked", range, vec![lagging, corrupt]).await;
        match outcome {
            Err(RawHistoryRunError::NoCompatibleSource { reasons, .. }) => {
                assert!(
                    reasons
                        .iter()
                        .any(|reason| reason.contains("receipts root mismatch")),
                    "{reasons:?}"
                );
            }
            other => panic!("the corrupt source's failure was masked: {other:?}"),
        }
        assert_eq!(job.state, RawHistoryJobState::Failed);
    }

    #[tokio::test]
    async fn transient_source_outage_leaves_job_running_for_supervisor_retry() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(config(directory.path()))
            .await
            .expect("store");
        let range = BlockRange::single(BlockNumber(400));
        let descriptor = fixture_source_descriptor("temporary-outage", range);
        let source = Arc::new(ScriptedHistorySource::new(
            descriptor.clone(),
            vec![ScriptedChunk {
                range,
                schema_version: descriptor.schema_version,
                estimated_bytes: None,
                steps: vec![HistoryStep::Error(SourceError::Unavailable(
                    "maintenance".to_owned(),
                ))],
            }],
        ));
        let sources = source_set(source);
        let id = RawHistoryJobId::new("raw-transient").expect("job ID");
        store
            .create_raw_history_job(id.clone(), spec(range, sources.policy_digest()))
            .await
            .expect("create");
        let runner =
            RawHistoryRunner::new(store.clone(), sources, default_source_budget()).expect("runner");
        assert!(matches!(
            Box::pin(runner.run(&id, CancellationToken::new())).await,
            Err(RawHistoryRunError::SourcesUnavailable { .. })
        ));
        assert_eq!(
            store
                .raw_history_job(&id)
                .await
                .expect("inspect")
                .expect("job")
                .state,
            RawHistoryJobState::Running
        );
    }

    #[tokio::test]
    async fn a_source_that_does_not_advertise_the_range_leaves_a_lagging_range_waiting() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(config(directory.path()))
            .await
            .expect("store");
        let range = BlockRange::new(BlockNumber(500), BlockNumber(502)).expect("range");
        // The catalog lists only the first block so far.
        let catalog = Arc::new(ScriptedHistorySource::from_frames(
            lagging_descriptor("lagging-catalog", range),
            frames(500, 500),
        ));
        // A local archive of older blocks, which has no expected lag.
        let older = BlockRange::new(BlockNumber(100), BlockNumber(102)).expect("older range");
        let archive = Arc::new(ScriptedHistorySource::from_frames(
            fixture_source_descriptor("older-archive", older),
            frames(100, 102),
        ));
        let (outcome, job) =
            run_new_job(&store, "raw-older-archive", range, vec![catalog, archive]).await;
        // Review 2: the archive's miss past its manifest was terminal, and
        // failed the job while the catalog was still catching up.
        assert!(
            matches!(outcome, Err(RawHistoryRunError::SourcesUnavailable { .. })),
            "{outcome:?}"
        );
        assert_eq!(job.state, RawHistoryJobState::Running);
        assert!(
            job.last_error
                .as_deref()
                .is_some_and(|error| error.contains("lagging-catalog")),
            "{:?}",
            job.last_error
        );
    }

    #[tokio::test]
    async fn a_hole_inside_a_source_range_without_lag_still_fails_a_lagging_range() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(config(directory.path()))
            .await
            .expect("store");
        let range = BlockRange::new(BlockNumber(510), BlockNumber(512)).expect("range");
        let catalog = Arc::new(ScriptedHistorySource::from_frames(
            lagging_descriptor("lagging-catalog", range),
            frames(510, 510),
        ));
        // The archive advertises the range, but lacks two of its blocks.
        let archive = Arc::new(ScriptedHistorySource::from_frames(
            fixture_source_descriptor("holed-archive", range),
            frames(510, 510),
        ));
        let (outcome, job) =
            run_new_job(&store, "raw-holed-archive", range, vec![catalog, archive]).await;
        assert!(
            matches!(outcome, Err(RawHistoryRunError::NoCompatibleSource { .. })),
            "{outcome:?}"
        );
        assert_eq!(job.state, RawHistoryJobState::Failed);
    }

    #[tokio::test]
    async fn a_recreated_job_does_not_inherit_the_lag_clock_of_its_predecessor() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(config(directory.path()))
            .await
            .expect("store");
        let range = BlockRange::new(BlockNumber(1_000), BlockNumber(1_002)).expect("range");
        let sources = source_set(Arc::new(ScriptedHistorySource::from_frames(
            lagging_descriptor("slow-catalog", range),
            frames(1_000, 1_000),
        )));
        let digest = sources.policy_digest();
        let id = RawHistoryJobId::new("raw-reused-id").expect("job ID");
        let runner =
            RawHistoryRunner::new(store.clone(), sources, default_source_budget()).expect("runner");
        store
            .create_raw_history_job(id.clone(), spec(range, digest))
            .await
            .expect("create");
        assert!(matches!(
            Box::pin(runner.run(&id, CancellationToken::new())).await,
            Err(RawHistoryRunError::SourcesUnavailable { .. })
        ));
        runner.backdate_lag(&id, std::time::Duration::from_hours(25));
        // The operator replaces the job while the runner is not running it.
        store.cancel_raw_history_job(&id).await.expect("cancel");
        store.delete_raw_history_job(&id).await.expect("delete");
        // A later creation time tells the jobs apart.
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        store
            .create_raw_history_job(id.clone(), spec(range, digest))
            .await
            .expect("recreate");
        // Review 2: the new job inherited the old job's lag clock and failed
        // on its first run.
        let outcome = Box::pin(runner.run(&id, CancellationToken::new())).await;
        assert!(
            matches!(outcome, Err(RawHistoryRunError::SourcesUnavailable { .. })),
            "{outcome:?}"
        );
    }

    #[tokio::test]
    async fn a_waiting_job_keeps_its_reason_until_a_segment_commits() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(config(directory.path()))
            .await
            .expect("store");
        let range = BlockRange::new(BlockNumber(900), BlockNumber(902)).expect("range");
        let descriptor = lagging_descriptor("slow-catalog", range);
        let (outcome, waiting) = run_new_job(
            &store,
            "raw-visible-reason",
            range,
            vec![Arc::new(ScriptedHistorySource::from_frames(
                descriptor.clone(),
                frames(900, 900),
            ))],
        )
        .await;
        assert!(matches!(
            outcome,
            Err(RawHistoryRunError::SourcesUnavailable { .. })
        ));
        assert!(waiting.last_error.is_some());
        // Review 2: every start cleared the reason, so a status read during
        // the run showed none.
        let started = store
            .start_raw_history_job(&waiting.id)
            .await
            .expect("start");
        assert_eq!(started.last_error, waiting.last_error);
        // The catalog now lists the whole range.
        let runner = RawHistoryRunner::new(
            store.clone(),
            source_set(Arc::new(ScriptedHistorySource::from_frames(
                descriptor,
                frames(900, 902),
            ))),
            default_source_budget(),
        )
        .expect("runner");
        let outcome = Box::pin(runner.run(&waiting.id, CancellationToken::new())).await;
        let Ok(RawHistoryRunOutcome::Complete(job)) = outcome else {
            panic!("the published range completes the job: {outcome:?}");
        };
        assert_eq!(
            job.last_error, None,
            "a committed segment clears the reason"
        );
    }
}
