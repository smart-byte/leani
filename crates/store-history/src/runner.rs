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
/// A range whose sources fail on transport is retried for this long, which
/// outlasts an outage over a weekend but not a mirror that is gone.
const TRANSIENT_RETRY_BOUND: Duration = Duration::from_hours(72);
/// A persisted retry bound fails a job only once this process has itself
/// seen the job's range fail for this long without committing a segment, so
/// time the node was down or the job was paused never counts as its sources
/// failing. Frames delivered without a commit do not restart it: a source
/// that fails partway through every chunk still fails the job.
const RETRY_BOUND_GRACE: Duration = Duration::from_hours(1);

/// Resumable segment-boundary acquisition executor for durable raw jobs.
#[derive(Clone, Debug)]
pub struct RawHistoryRunner {
    store: HistoryStore,
    sources: RawHistorySourceSet,
    source_budget: SourceBudget,
    active: Arc<Mutex<BTreeSet<RawHistoryJobId>>>,
    /// Since when this process has seen each job's range fail, with no
    /// commit since.
    failing: Arc<std::sync::Mutex<BTreeMap<RawHistoryJobId, Instant>>>,
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
            failing: Arc::new(std::sync::Mutex::new(BTreeMap::new())),
        })
    }

    fn failing(&self) -> std::sync::MutexGuard<'_, BTreeMap<RawHistoryJobId, Instant>> {
        self.failing
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Forget `id`'s failures: the job committed a segment, or ended.
    fn progressed(&self, id: &RawHistoryJobId) {
        self.failing().remove(id);
    }

    /// Move the start of `id`'s continuous failure `by` into the past.
    #[cfg(test)]
    fn backdate_failing(&self, id: &RawHistoryJobId, by: Duration) {
        if let Some(since) = self.failing().get_mut(id) {
            *since = since.checked_sub(by).expect("backdated instant");
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
        // Only a range still waiting on its sources keeps its failure start.
        if !matches!(
            result,
            Ok(RawHistoryRunOutcome::Interrupted(_))
                | Err(RawHistoryRunError::SourcesUnavailable { .. })
        ) {
            self.progressed(id);
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
            // An overlapping job may have retained part of the range since
            // the last acquisition; this job adopts it instead.
            match self
                .store
                .claim_compatible_segments_for_raw_history_job(id)
                .await
            {
                Ok(0) => {}
                // It gained coverage, or turned terminal meanwhile.
                Ok(_) | Err(HistoryStoreError::JobState { .. }) => continue,
                Err(error) => return Err(error.into()),
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
                // A segment the store would not admit leaves the job paused
                // before any source is opened, however often it resumes.
                match self
                    .store
                    .check_admission(
                        range,
                        SegmentReservation::new(
                            job.spec.segment.maximum_logical_bytes,
                            job.spec.segment.maximum_physical_bytes,
                        ),
                        job.spec.indexes,
                    )
                    .await
                {
                    Ok(()) => {}
                    Err(
                        error @ (HistoryStoreError::LogicalBudget { .. }
                        | HistoryStoreError::PhysicalBudget { .. }),
                    ) => return self.handle_storage_limit(id, &job, error).await,
                    Err(error) => {
                        self.store
                            .fail_raw_history_job(id, &error.to_string())
                            .await?;
                        return Err(error.into());
                    }
                }
                match Box::pin(self.acquire_segment(&job, range, cancellation.clone())).await {
                    Ok(()) => break,
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
    /// their reasons as its last error, or fail it. The wait is measured from
    /// the job's last progress, which is persisted, so restarts do not reset
    /// it. That wait also counts time the node was down or the job paused,
    /// so it fails the job only once this process has itself seen the range
    /// fail for [`RETRY_BOUND_GRACE`].
    async fn handle_source_failures(
        &self,
        job: &RawHistoryJob,
        range: BlockRange,
        mut reasons: Vec<String>,
        retry: SourceRetry,
    ) -> Result<RawHistoryRunError, HistoryStoreError> {
        let id = &job.id;
        let exhausted = match retry {
            SourceRetry::Terminal => true,
            SourceRetry::Transient { limit } | SourceRetry::Lagging { limit } => {
                let failing_for = self
                    .failing()
                    .entry(id.clone())
                    .or_insert_with(Instant::now)
                    .elapsed();
                let progressed_at = self.store.raw_history_job_progressed_at(job).await?;
                let waited =
                    Duration::from_millis(crate::catalog::unix_ms()?.saturating_sub(progressed_at));
                let exhausted = waited >= limit && failing_for >= RETRY_BOUND_GRACE;
                if exhausted {
                    reasons.push(if matches!(retry, SourceRetry::Transient { .. }) {
                        format!(
                            "the sources stayed unreachable past the {} hour retry bound",
                            limit.as_secs() / 3_600
                        )
                    } else {
                        format!(
                            "the range stayed unpublished past its {} hour retry bound",
                            limit.as_secs() / 3_600
                        )
                    });
                }
                exhausted
            }
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
                // Compressing and writing the frame is blocking file work.
                let mut segment = pending.take().expect("pending segment was initialized");
                let (segment, appended) = crate::catalog::blocking(move || {
                    let appended = segment.append(&frame);
                    (segment, appended)
                })
                .await?;
                pending = Some(segment);
                if let Err(error) = appended {
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
        // The job progressed, so a failure after this starts the grace anew.
        self.progressed(&job.id);
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
    /// A transport failure, which can clear; retry it for `limit`.
    Transient { limit: Duration },
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
            | SourceError::Protocol(_) => Self::Transient {
                limit: TRANSIENT_RETRY_BOUND,
            },
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
    /// retryable for the longer of the two bounds, and a terminal failure
    /// outranks lag, so a lagging source cannot hide another source's error.
    fn or(self, other: Option<Self>) -> Self {
        match (self, other) {
            (
                Self::Transient { limit },
                Some(Self::Transient { limit: other } | Self::Lagging { limit: other }),
            )
            | (Self::Lagging { limit: other }, Some(Self::Transient { limit })) => {
                Self::Transient {
                    limit: limit.max(other),
                }
            }
            (transient @ Self::Transient { .. }, _)
            | (_, Some(transient @ Self::Transient { .. })) => transient,
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
        backdate_job(&store, &id, std::time::Duration::from_hours(23)).await;
        assert!(matches!(
            Box::pin(runner.run(&id, CancellationToken::new())).await,
            Err(RawHistoryRunError::SourcesUnavailable { .. })
        ));
        // Review I1: the range was retried forever.
        backdate_job(&store, &id, std::time::Duration::from_hours(2)).await;
        runner.backdate_failing(&id, RETRY_BOUND_GRACE);
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
        backdate_job(&store, &id, std::time::Duration::from_hours(25)).await;
        // The operator replaces the job while the runner is not running it.
        store.cancel_raw_history_job(&id).await.expect("cancel");
        store.delete_raw_history_job(&id).await.expect("delete");
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

    /// Move `id`'s creation `by` into the past, as if it had waited that long
    /// without a committed segment.
    async fn backdate_job(store: &HistoryStore, id: &RawHistoryJobId, by: std::time::Duration) {
        sqlx::query(
            "UPDATE raw_history_jobs SET created_at_unix_ms = created_at_unix_ms - ?
             WHERE job_id = ?",
        )
        .bind(i64::try_from(by.as_millis()).expect("backdate fits"))
        .bind(id.as_str())
        .execute(&store.inner.pool)
        .await
        .expect("backdate job");
    }

    #[tokio::test]
    async fn a_job_restarted_often_still_fails_at_its_lag_bound() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(config(directory.path()))
            .await
            .expect("store");
        let range = BlockRange::new(BlockNumber(1_100), BlockNumber(1_102)).expect("range");
        let sources = source_set(Arc::new(ScriptedHistorySource::from_frames(
            lagging_descriptor("stale-catalog", range),
            frames(1_100, 1_100),
        )));
        let id = RawHistoryJobId::new("raw-restarted").expect("job ID");
        store
            .create_raw_history_job(id.clone(), spec(range, sources.policy_digest()))
            .await
            .expect("create");
        // Every run starts a new runner, as a node restart does.
        let run_after_restart = || {
            let runner =
                RawHistoryRunner::new(store.clone(), sources.clone(), default_source_budget())
                    .expect("runner");
            let id = id.clone();
            async move { Box::pin(runner.run(&id, CancellationToken::new())).await }
        };
        assert!(matches!(
            run_after_restart().await,
            Err(RawHistoryRunError::SourcesUnavailable { .. })
        ));
        backdate_job(&store, &id, std::time::Duration::from_hours(23)).await;
        assert!(matches!(
            run_after_restart().await,
            Err(RawHistoryRunError::SourcesUnavailable { .. })
        ));
        backdate_job(&store, &id, std::time::Duration::from_hours(2)).await;
        // The node restarts once more and stays up through the grace.
        let runner = RawHistoryRunner::new(store.clone(), sources.clone(), default_source_budget())
            .expect("runner");
        assert!(matches!(
            Box::pin(runner.run(&id, CancellationToken::new())).await,
            Err(RawHistoryRunError::SourcesUnavailable { .. })
        ));
        runner.backdate_failing(&id, RETRY_BOUND_GRACE);
        // Review carry (Task 17): a restart started the lag clock again, so a
        // node restarted daily never reached the bound.
        match Box::pin(runner.run(&id, CancellationToken::new())).await {
            Err(RawHistoryRunError::NoCompatibleSource { reasons, .. }) => assert!(
                reasons.iter().any(|reason| reason.contains("retry bound")),
                "{reasons:?}"
            ),
            other => panic!("the lagging range outlived its bound across restarts: {other:?}"),
        }
        let job = store
            .raw_history_job(&id)
            .await
            .expect("inspect")
            .expect("job");
        assert_eq!(job.state, RawHistoryJobState::Failed);
        assert!(
            job.last_error
                .as_deref()
                .is_some_and(|error| error.contains("retry bound")),
            "{:?}",
            job.last_error
        );
    }

    #[tokio::test]
    async fn a_dead_mirror_does_not_keep_a_job_waiting_forever() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(config(directory.path()))
            .await
            .expect("store");
        let range = BlockRange::new(BlockNumber(1_200), BlockNumber(1_202)).expect("range");
        let lagging = Arc::new(ScriptedHistorySource::from_frames(
            lagging_descriptor("a-lagging", range),
            frames(1_200, 1_200),
        ));
        let dead = failing_source(
            fixture_source_descriptor("b-dead-mirror", range),
            range,
            SourceError::Unavailable("connection refused".to_owned()),
        );
        let sources = RawHistorySourceSet::new(vec![lagging, dead]).expect("source set");
        let id = RawHistoryJobId::new("raw-dead-mirror").expect("job ID");
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
        // Past the lagging source's bound, the mirror may still come back.
        backdate_job(&store, &id, std::time::Duration::from_hours(25)).await;
        assert!(matches!(
            Box::pin(runner.run(&id, CancellationToken::new())).await,
            Err(RawHistoryRunError::SourcesUnavailable { .. })
        ));
        backdate_job(&store, &id, std::time::Duration::from_hours(48)).await;
        runner.backdate_failing(&id, RETRY_BOUND_GRACE);
        // Review carry (Task 17): a transport failure outranked lag, so one
        // unreachable mirror kept the job waiting forever.
        match Box::pin(runner.run(&id, CancellationToken::new())).await {
            Err(RawHistoryRunError::NoCompatibleSource { reasons, .. }) => {
                assert!(
                    reasons
                        .iter()
                        .any(|reason| reason.contains("b-dead-mirror")),
                    "{reasons:?}"
                );
                assert!(
                    reasons.iter().any(|reason| reason.contains("retry bound")),
                    "{reasons:?}"
                );
            }
            other => panic!("the unreachable mirror kept the job waiting: {other:?}"),
        }
        let job = store
            .raw_history_job(&id)
            .await
            .expect("inspect")
            .expect("job");
        assert_eq!(job.state, RawHistoryJobState::Failed);
        assert!(
            job.last_error
                .as_deref()
                .is_some_and(|error| error.contains("b-dead-mirror")),
            "{:?}",
            job.last_error
        );
    }

    #[tokio::test]
    async fn one_failure_after_long_downtime_leaves_the_job_waiting() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(config(directory.path()))
            .await
            .expect("store");
        let range = BlockRange::new(BlockNumber(1_400), BlockNumber(1_402)).expect("range");
        let sources = source_set(failing_source(
            fixture_source_descriptor("blip-mirror", range),
            range,
            SourceError::Unavailable("connection reset".to_owned()),
        ));
        let id = RawHistoryJobId::new("raw-long-downtime").expect("job ID");
        store
            .create_raw_history_job(id.clone(), spec(range, sources.policy_digest()))
            .await
            .expect("create");
        // The node was off for 100 hours.
        backdate_job(&store, &id, std::time::Duration::from_hours(100)).await;
        let runner =
            RawHistoryRunner::new(store.clone(), sources, default_source_budget()).expect("runner");
        let outcome = Box::pin(runner.run(&id, CancellationToken::new())).await;
        // Review I2: the downtime counted as waiting, and one transport error
        // on the first attempt failed the job.
        assert!(
            matches!(outcome, Err(RawHistoryRunError::SourcesUnavailable { .. })),
            "{outcome:?}"
        );
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
    async fn a_job_fails_once_its_sources_failed_through_the_grace() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(config(directory.path()))
            .await
            .expect("store");
        let range = BlockRange::new(BlockNumber(1_410), BlockNumber(1_412)).expect("range");
        let sources = source_set(failing_source(
            fixture_source_descriptor("gone-mirror", range),
            range,
            SourceError::Unavailable("connection refused".to_owned()),
        ));
        let id = RawHistoryJobId::new("raw-through-grace").expect("job ID");
        store
            .create_raw_history_job(id.clone(), spec(range, sources.policy_digest()))
            .await
            .expect("create");
        backdate_job(&store, &id, std::time::Duration::from_hours(100)).await;
        let runner =
            RawHistoryRunner::new(store.clone(), sources, default_source_budget()).expect("runner");
        let outcome = Box::pin(runner.run(&id, CancellationToken::new())).await;
        // Review I2: past the persisted bound, the first failure seen failed
        // the job.
        assert!(
            matches!(outcome, Err(RawHistoryRunError::SourcesUnavailable { .. })),
            "{outcome:?}"
        );
        runner.backdate_failing(&id, RETRY_BOUND_GRACE);
        match Box::pin(runner.run(&id, CancellationToken::new())).await {
            Err(RawHistoryRunError::NoCompatibleSource { reasons, .. }) => assert!(
                reasons
                    .iter()
                    .any(|reason| reason.contains("unreachable past the 72 hour retry bound")),
                "{reasons:?}"
            ),
            other => panic!("the sources failed through the grace: {other:?}"),
        }
        let job = store
            .raw_history_job(&id)
            .await
            .expect("inspect")
            .expect("job");
        assert_eq!(job.state, RawHistoryJobState::Failed);
        assert!(
            job.last_error
                .as_deref()
                .is_some_and(|error| error.contains("retry bound")),
            "{:?}",
            job.last_error
        );
    }

    #[tokio::test]
    async fn frames_without_a_commit_do_not_restart_the_grace() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(config(directory.path()))
            .await
            .expect("store");
        let range = BlockRange::new(BlockNumber(1_420), BlockNumber(1_422)).expect("range");
        let descriptor = fixture_source_descriptor("flaky-mirror", range);
        let schema_version = descriptor.schema_version.clone();
        // The mirror delivers the range's first block, then drops, every time.
        let sources = source_set(Arc::new(ScriptedHistorySource::new(
            descriptor,
            vec![ScriptedChunk {
                range,
                schema_version,
                estimated_bytes: None,
                steps: vec![
                    HistoryStep::Frame(Box::new(frames(1_420, 1_420).remove(0))),
                    HistoryStep::Error(SourceError::Unavailable("connection reset".to_owned())),
                ],
            }],
        )));
        let id = RawHistoryJobId::new("raw-delivering").expect("job ID");
        store
            .create_raw_history_job(id.clone(), spec(range, sources.policy_digest()))
            .await
            .expect("create");
        backdate_job(&store, &id, std::time::Duration::from_hours(100)).await;
        let runner =
            RawHistoryRunner::new(store.clone(), sources, default_source_budget()).expect("runner");
        assert!(matches!(
            Box::pin(runner.run(&id, CancellationToken::new())).await,
            Err(RawHistoryRunError::SourcesUnavailable { .. })
        ));
        runner.backdate_failing(&id, RETRY_BOUND_GRACE);
        // Review 2 N6: each delivered frame restarted the grace, so a mirror
        // that fails partway through every chunk kept the job waiting
        // forever.
        match Box::pin(runner.run(&id, CancellationToken::new())).await {
            Err(RawHistoryRunError::NoCompatibleSource { reasons, .. }) => assert!(
                reasons.iter().any(|reason| reason.contains("retry bound")),
                "{reasons:?}"
            ),
            other => panic!("frames without a commit restarted the grace: {other:?}"),
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
    async fn a_commit_restarts_the_grace() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(config(directory.path()))
            .await
            .expect("store");
        let range = BlockRange::new(BlockNumber(1_440), BlockNumber(1_445)).expect("range");
        let descriptor = fixture_source_descriptor("recovering-mirror", range);
        let schema_version = descriptor.schema_version.clone();
        // Once reachable, the mirror serves the first segment and fails the
        // second.
        let inner = Arc::new(ScriptedHistorySource::new(
            descriptor,
            vec![
                ScriptedChunk {
                    range: BlockRange::new(BlockNumber(1_440), BlockNumber(1_442)).expect("range"),
                    schema_version: schema_version.clone(),
                    estimated_bytes: None,
                    steps: frames(1_440, 1_442)
                        .into_iter()
                        .map(|frame| HistoryStep::Frame(Box::new(frame)))
                        .collect(),
                },
                ScriptedChunk {
                    range: BlockRange::new(BlockNumber(1_443), BlockNumber(1_445)).expect("range"),
                    schema_version,
                    estimated_bytes: None,
                    steps: vec![HistoryStep::Error(SourceError::Unavailable(
                        "connection reset".to_owned(),
                    ))],
                },
            ],
        ));
        let sources = source_set(Arc::new(UnreachableFirst {
            inner,
            failures: std::sync::atomic::AtomicUsize::new(1),
        }));
        let id = RawHistoryJobId::new("raw-recovering").expect("job ID");
        store
            .create_raw_history_job(id.clone(), spec(range, sources.policy_digest()))
            .await
            .expect("create");
        backdate_job(&store, &id, std::time::Duration::from_hours(100)).await;
        let runner =
            RawHistoryRunner::new(store.clone(), sources, default_source_budget()).expect("runner");
        let waiting = |outcome: &Result<RawHistoryRunOutcome, RawHistoryRunError>| {
            matches!(outcome, Err(RawHistoryRunError::SourcesUnavailable { .. }))
        };
        let outcome = Box::pin(runner.run(&id, CancellationToken::new())).await;
        assert!(waiting(&outcome), "{outcome:?}");
        runner.backdate_failing(&id, RETRY_BOUND_GRACE);
        // The first segment commits; the second fails.
        let outcome = Box::pin(runner.run(&id, CancellationToken::new())).await;
        assert!(waiting(&outcome), "{outcome:?}");
        // Long after that commit, the second segment fails once more: the
        // grace began again with the commit, so the job still waits.
        sqlx::query("UPDATE raw_segment_owners SET created_at_unix_ms = created_at_unix_ms - ?")
            .bind(i64::try_from(std::time::Duration::from_hours(100).as_millis()).expect("ms"))
            .execute(&store.inner.pool)
            .await
            .expect("backdate the commit");
        let outcome = Box::pin(runner.run(&id, CancellationToken::new())).await;
        assert!(waiting(&outcome), "{outcome:?}");
    }

    #[tokio::test]
    async fn a_completed_job_reacquires_a_quarantined_segment() {
        use std::io::{Seek, SeekFrom, Write};

        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(config(directory.path()))
            .await
            .expect("store");
        let all = frames(1_500, 1_502);
        let range = BlockRange::new(BlockNumber(1_500), BlockNumber(1_502)).expect("range");
        let source = Arc::new(ScriptedHistorySource::from_frames(
            fixture_source_descriptor("refill-archive", range),
            all.clone(),
        ));
        let sources = source_set(source.clone());
        let id = RawHistoryJobId::new("raw-refill").expect("job ID");
        store
            .create_raw_history_job(id.clone(), spec(range, sources.policy_digest()))
            .await
            .expect("create");
        let runner =
            RawHistoryRunner::new(store.clone(), sources, default_source_budget()).expect("runner");
        assert!(matches!(
            Box::pin(runner.run(&id, CancellationToken::new())).await,
            Ok(RawHistoryRunOutcome::Complete(_))
        ));
        let segment = store.segments().await.expect("segments").remove(0);
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(directory.path().join(&segment.relative_path))
            .expect("open segment");
        file.seek(SeekFrom::Start(81)).expect("seek payload");
        file.write_all(&[0xaa]).expect("corrupt segment");
        file.sync_all().expect("persist corruption");
        assert!(
            store
                .read_block(&segment.metadata.id, BlockNumber(1_500))
                .await
                .is_err()
        );
        // Review I3: the job stayed complete, and no one acquired the range
        // again.
        let job = store
            .raw_history_job(&id)
            .await
            .expect("inspect")
            .expect("job");
        assert_eq!(job.state, RawHistoryJobState::Queued);
        assert_eq!(job.remaining_ranges, vec![range]);
        assert!(matches!(
            Box::pin(runner.run(&id, CancellationToken::new())).await,
            Ok(RawHistoryRunOutcome::Complete(_))
        ));
        assert_eq!(source.open_calls(), 2);
        let refilled = store.segments().await.expect("segments").remove(0);
        assert_eq!(
            store
                .read_block(&refilled.metadata.id, BlockNumber(1_500))
                .await
                .expect("the refilled segment reads")
                .frame,
            all[0]
        );
    }

    #[tokio::test]
    async fn a_segment_over_its_locator_estimate_commits_once() {
        let directory = tempdir().expect("temporary directory");
        let range = BlockRange::new(BlockNumber(1_600), BlockNumber(1_602)).expect("range");
        let mut all = frames(1_600, 1_602);
        for frame in &mut all {
            let block = frame.block.number.0;
            frame.transactions = Material::Complete(
                (0..800_u32)
                    .map(|index| {
                        let mut key = [0; 12];
                        key[..8].copy_from_slice(&block.to_be_bytes());
                        key[8..].copy_from_slice(&index.to_be_bytes());
                        leani_primitives::TransactionEnvelope {
                            hash: leani_primitives::TransactionHash::new(
                                *blake3::hash(&key).as_bytes(),
                            ),
                            transaction_type: 2,
                            index,
                            encoded: None,
                            from: None,
                            to: None,
                            nonce: None,
                            gas_limit: None,
                            value: None,
                            input: None,
                            max_fee_per_gas: None,
                            max_priority_fee_per_gas: None,
                            max_fee_per_blob_gas: None,
                            blob_versioned_hashes: Vec::new(),
                            size_bytes: None,
                        }
                    })
                    .collect(),
            );
            frame.receipts = Material::Missing(leani_primitives::MissingReason::NotRequested);
        }
        let source = Arc::new(ScriptedHistorySource::from_frames(
            fixture_source_descriptor("dense-archive", range),
            all,
        ));
        let sources = source_set(source.clone());
        let mut indexed = spec(range, sources.policy_digest());
        indexed.required_capabilities = CapabilitySet::of(Capability::Transactions);
        indexed.indexes.transaction_hash = true;
        // A budget that admits the segment and its estimated 256 transaction
        // locators per block, not the 800 per block it holds.
        let measured = {
            let store = HistoryStore::open(config(directory.path()))
                .await
                .expect("store");
            store.inner.pool.close().await;
            let store = HistoryStore::open(config(directory.path()))
                .await
                .expect("reopen");
            let total = store.stats().await.expect("stats").total_physical_bytes;
            store.inner.pool.close().await;
            total
        };
        let segment_bytes = 1024 * 1024;
        let estimate = 64 * 1024 + 3 * 256 * 512;
        let mut tight = config(directory.path());
        tight.budget.maximum_physical_bytes = measured + segment_bytes + estimate + 256 * 1024;
        tight.budget.maximum_segment_physical_bytes = segment_bytes;
        let store = HistoryStore::open(tight).await.expect("tight store");
        let id = RawHistoryJobId::new("raw-dense").expect("job ID");
        store
            .create_raw_history_job(id.clone(), indexed)
            .await
            .expect("create");
        let runner =
            RawHistoryRunner::new(store.clone(), sources, default_source_budget()).expect("runner");
        let outcome = Box::pin(runner.run(&id, CancellationToken::new())).await;
        // Review I4: publication refused the extra locators, and the job
        // paused, resumed and acquired the segment again, forever.
        assert!(
            matches!(outcome, Ok(RawHistoryRunOutcome::Complete(_))),
            "{outcome:?}"
        );
        assert_eq!(source.open_calls(), 1);
        assert_eq!(
            store.stats().await.expect("stats").transaction_locators,
            2_400
        );
    }

    #[tokio::test]
    async fn a_failed_job_keeps_its_error_when_it_loses_a_segment() {
        use std::io::{Seek, SeekFrom, Write};

        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(config(directory.path()))
            .await
            .expect("store");
        let range = BlockRange::new(BlockNumber(1_700), BlockNumber(1_705)).expect("range");
        let descriptor = fixture_source_descriptor("half-archive", range);
        let schema_version = descriptor.schema_version.clone();
        // The first segment's blocks arrive; the second's are corrupt.
        let sources = source_set(Arc::new(ScriptedHistorySource::new(
            descriptor,
            vec![
                ScriptedChunk {
                    range: BlockRange::new(BlockNumber(1_700), BlockNumber(1_702)).expect("range"),
                    schema_version: schema_version.clone(),
                    estimated_bytes: None,
                    steps: frames(1_700, 1_702)
                        .into_iter()
                        .map(|frame| HistoryStep::Frame(Box::new(frame)))
                        .collect(),
                },
                ScriptedChunk {
                    range: BlockRange::new(BlockNumber(1_703), BlockNumber(1_705)).expect("range"),
                    schema_version,
                    estimated_bytes: None,
                    steps: vec![HistoryStep::Error(SourceError::CorruptFrame(
                        "bad block 1703".to_owned(),
                    ))],
                },
            ],
        )));
        let id = RawHistoryJobId::new("raw-half-failed").expect("job ID");
        store
            .create_raw_history_job(id.clone(), spec(range, sources.policy_digest()))
            .await
            .expect("create");
        let runner =
            RawHistoryRunner::new(store.clone(), sources, default_source_budget()).expect("runner");
        assert!(
            Box::pin(runner.run(&id, CancellationToken::new()))
                .await
                .is_err()
        );
        let failed = store
            .raw_history_job(&id)
            .await
            .expect("inspect")
            .expect("job");
        assert_eq!(failed.state, RawHistoryJobState::Failed);
        let segment = store.segments().await.expect("segments").remove(0);
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(directory.path().join(&segment.relative_path))
            .expect("open segment");
        file.seek(SeekFrom::Start(81)).expect("seek payload");
        file.write_all(&[0xaa]).expect("corrupt segment");
        file.sync_all().expect("persist corruption");
        assert!(
            store
                .read_block(&segment.metadata.id, BlockNumber(1_700))
                .await
                .is_err()
        );
        // Review M6: losing the segment replaced why the job failed.
        let job = store
            .raw_history_job(&id)
            .await
            .expect("inspect")
            .expect("job");
        assert_eq!(job.state, RawHistoryJobState::Failed);
        assert_eq!(job.last_error, failed.last_error);
    }

    #[tokio::test]
    async fn a_job_recreated_after_its_segment_file_is_lost_acquires_it_again() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(config(directory.path()))
            .await
            .expect("store");
        let range = BlockRange::new(BlockNumber(1_900), BlockNumber(1_902)).expect("range");
        let source = Arc::new(ScriptedHistorySource::from_frames(
            fixture_source_descriptor("lost-archive", range),
            frames(1_900, 1_902),
        ));
        let sources = source_set(source.clone());
        let id = RawHistoryJobId::new("raw-lost-file").expect("job ID");
        let job_spec = spec(range, sources.policy_digest());
        store
            .create_raw_history_job(id.clone(), job_spec.clone())
            .await
            .expect("create");
        let runner = RawHistoryRunner::new(store.clone(), sources.clone(), default_source_budget())
            .expect("runner");
        assert!(matches!(
            Box::pin(runner.run(&id, CancellationToken::new())).await,
            Ok(RawHistoryRunOutcome::Complete(_))
        ));
        let lost = store.segments().await.expect("segments").remove(0);
        store.inner.pool.close().await;
        drop((runner, store));
        std::fs::remove_file(directory.path().join(&lost.relative_path)).expect("lose the file");
        let store = HistoryStore::open(config(directory.path()))
            .await
            .expect("reopen");
        assert_eq!(store.recovery_report().unavailable_segments, 1);
        // The runbook's remedy: delete the job, and create it again.
        store.delete_raw_history_job(&id).await.expect("delete");
        store
            .create_raw_history_job(id.clone(), job_spec)
            .await
            .expect("create again");
        let runner =
            RawHistoryRunner::new(store.clone(), sources, default_source_budget()).expect("runner");
        // Review 2 N2: the new job adopted the segment whose file was gone,
        // and completed without acquiring the blocks.
        assert!(matches!(
            Box::pin(runner.run(&id, CancellationToken::new())).await,
            Ok(RawHistoryRunOutcome::Complete(_))
        ));
        assert_eq!(source.open_calls(), 2);
        let segments = store.segments().await.expect("segments");
        assert_eq!(segments.len(), 1);
        let stats = store.stats().await.expect("stats");
        assert_eq!(
            stats.retained_logical_bytes,
            segments[0].metadata.logical_bytes
        );
        assert_eq!(
            store
                .read_block(&segments[0].metadata.id, BlockNumber(1_901))
                .await
                .expect("the new segment reads")
                .frame
                .block
                .number,
            BlockNumber(1_901)
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn a_recreated_job_replaces_a_lost_segment_another_job_still_owns() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(config(directory.path()))
            .await
            .expect("store");
        let range = BlockRange::new(BlockNumber(1_910), BlockNumber(1_912)).expect("range");
        let source = Arc::new(ScriptedHistorySource::from_frames(
            fixture_source_descriptor("shared-archive", range),
            frames(1_910, 1_912),
        ));
        let sources = source_set(source.clone());
        let id = RawHistoryJobId::new("raw-lost-shared").expect("job ID");
        let job_spec = spec(range, sources.policy_digest());
        // Another job over the same blocks, whose own segments would differ.
        let other = RawHistoryJobId::new("raw-lost-sharer").expect("job ID");
        let mut other_spec = job_spec.clone();
        other_spec.segment.target_blocks = 2;
        let run = |store: &HistoryStore, id: &RawHistoryJobId| {
            let runner =
                RawHistoryRunner::new(store.clone(), sources.clone(), default_source_budget())
                    .expect("runner");
            let id = id.clone();
            async move { Box::pin(runner.run(&id, CancellationToken::new())).await }
        };
        store
            .create_raw_history_job(id.clone(), job_spec.clone())
            .await
            .expect("create");
        assert!(matches!(
            run(&store, &id).await,
            Ok(RawHistoryRunOutcome::Complete(_))
        ));
        // The other job adopts the segment instead of acquiring it.
        store
            .create_raw_history_job(other.clone(), other_spec)
            .await
            .expect("create the other job");
        assert!(matches!(
            run(&store, &other).await,
            Ok(RawHistoryRunOutcome::Complete(_))
        ));
        assert_eq!(source.open_calls(), 1);
        let lost = store.segments().await.expect("segments").remove(0);
        store.inner.pool.close().await;
        drop(store);
        std::fs::remove_file(directory.path().join(&lost.relative_path)).expect("lose the file");
        let store = HistoryStore::open(config(directory.path()))
            .await
            .expect("reopen");
        // Only the first job is deleted and created again; the other still
        // owns the lost segment.
        store.delete_raw_history_job(&id).await.expect("delete");
        store
            .create_raw_history_job(id.clone(), job_spec)
            .await
            .expect("create again");
        assert!(matches!(
            run(&store, &id).await,
            Ok(RawHistoryRunOutcome::Complete(_))
        ));
        assert_eq!(source.open_calls(), 2);
        // Review 3 (1): the new segment took the lost one's ID, so its
        // publication adopted the lost row and deleted its own file.
        let segments = store.segments().await.expect("segments");
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].metadata.id, lost.metadata.id);
        assert!(directory.path().join(&segments[0].relative_path).exists());
        store
            .read_block(&segments[0].metadata.id, BlockNumber(1_911))
            .await
            .expect("the new segment reads");
        // The other job lost its segment, returned to the queue, and adopts
        // the new one without acquiring it.
        assert_eq!(
            store
                .raw_history_job(&other)
                .await
                .expect("inspect")
                .expect("job")
                .state,
            RawHistoryJobState::Queued
        );
        assert!(matches!(
            run(&store, &other).await,
            Ok(RawHistoryRunOutcome::Complete(_))
        ));
        assert_eq!(source.open_calls(), 2);
        let adopted = store
            .raw_history_job(&other)
            .await
            .expect("inspect")
            .expect("job");
        // Review 3 (4): adopting it kept the lost segment's error.
        assert_eq!(adopted.last_error, None);
        assert!(
            store
                .owners(&segments[0].metadata.id)
                .await
                .expect("owners")
                .iter()
                .any(|owner| owner.owner_id == other.as_str())
        );
    }

    #[tokio::test]
    async fn a_job_paused_at_its_storage_limit_opens_no_source_until_space_is_freed() {
        let directory = tempdir().expect("temporary directory");
        let filled = BlockRange::new(BlockNumber(1_950), BlockNumber(1_952)).expect("range");
        let waiting = BlockRange::new(BlockNumber(1_960), BlockNumber(1_962)).expect("range");
        let descriptor = fixture_source_descriptor(
            "paused-archive",
            BlockRange::new(BlockNumber(1_950), BlockNumber(1_962)).expect("range"),
        );
        let chunk = |range: BlockRange, blocks: Vec<BlockFrame>| ScriptedChunk {
            range,
            schema_version: descriptor.schema_version.clone(),
            estimated_bytes: None,
            steps: blocks
                .into_iter()
                .map(|frame| HistoryStep::Frame(Box::new(frame)))
                .collect(),
        };
        let source = Arc::new(ScriptedHistorySource::new(
            descriptor.clone(),
            vec![
                chunk(filled, frames(1_950, 1_952)),
                chunk(waiting, frames(1_960, 1_962)),
            ],
        ));
        let sources = source_set(source.clone());
        // A first job fills the store.
        let retained_logical = {
            let store = HistoryStore::open(config(directory.path()))
                .await
                .expect("store");
            let id = RawHistoryJobId::new("raw-filler").expect("job ID");
            store
                .create_raw_history_job(id.clone(), spec(filled, sources.policy_digest()))
                .await
                .expect("create");
            let runner =
                RawHistoryRunner::new(store.clone(), sources.clone(), default_source_budget())
                    .expect("runner");
            assert!(matches!(
                Box::pin(runner.run(&id, CancellationToken::new())).await,
                Ok(RawHistoryRunOutcome::Complete(_))
            ));
            let retained = store.stats().await.expect("stats").retained_logical_bytes;
            store.inner.pool.close().await;
            retained
        };
        let budget = |maximum_logical_bytes| {
            HistoryStoreConfig::new(directory.path()).with_budget(StorageBudget {
                maximum_logical_bytes,
                maximum_physical_bytes: 32 * 1024 * 1024,
                maximum_frame_logical_bytes: 1024 * 1024,
                maximum_segment_logical_bytes: 1024 * 1024,
                maximum_segment_physical_bytes: 1024 * 1024,
            })
        };
        // One byte short of the next segment's reservation.
        let store = HistoryStore::open(budget(retained_logical + 1024 * 1024 - 1))
            .await
            .expect("full store");
        let id = RawHistoryJobId::new("raw-paused").expect("job ID");
        store
            .create_raw_history_job(id.clone(), spec(waiting, sources.policy_digest()))
            .await
            .expect("create");
        let runner = RawHistoryRunner::new(store.clone(), sources.clone(), default_source_budget())
            .expect("runner");
        let (plans, opens) = (source.plan_calls(), source.open_calls());
        // The supervisor resumes a paused job every five seconds.
        for _ in 0..3 {
            let outcome = Box::pin(runner.run(&id, CancellationToken::new())).await;
            assert!(
                matches!(outcome, Ok(RawHistoryRunOutcome::StorageBackpressured(_))),
                "{outcome:?}"
            );
        }
        // Review 2 (I4): each resume planned and opened the source, fetching
        // the segment again, before its admission failed.
        assert_eq!((source.plan_calls(), source.open_calls()), (plans, opens));
        store.inner.pool.close().await;
        drop((runner, store));
        let store = HistoryStore::open(budget(retained_logical + 4 * 1024 * 1024))
            .await
            .expect("store with room");
        let runner =
            RawHistoryRunner::new(store.clone(), sources, default_source_budget()).expect("runner");
        assert!(matches!(
            Box::pin(runner.run(&id, CancellationToken::new())).await,
            Ok(RawHistoryRunOutcome::Complete(_))
        ));
        assert_eq!(source.open_calls(), opens + 1);
    }

    #[tokio::test]
    async fn a_requeued_job_acquires_again_when_its_quarantine_could_not_move_the_file() {
        use std::io::{Seek, SeekFrom, Write};

        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(config(directory.path()))
            .await
            .expect("store");
        let range = BlockRange::new(BlockNumber(1_980), BlockNumber(1_982)).expect("range");
        let source = Arc::new(ScriptedHistorySource::from_frames(
            fixture_source_descriptor("stuck-archive", range),
            frames(1_980, 1_982),
        ));
        let sources = source_set(source.clone());
        let id = RawHistoryJobId::new("raw-stuck-file").expect("job ID");
        store
            .create_raw_history_job(id.clone(), spec(range, sources.policy_digest()))
            .await
            .expect("create");
        let runner =
            RawHistoryRunner::new(store.clone(), sources, default_source_budget()).expect("runner");
        assert!(matches!(
            Box::pin(runner.run(&id, CancellationToken::new())).await,
            Ok(RawHistoryRunOutcome::Complete(_))
        ));
        let segment = store.segments().await.expect("segments").remove(0);
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(directory.path().join(&segment.relative_path))
            .expect("open segment");
        file.seek(SeekFrom::Start(81)).expect("seek payload");
        file.write_all(&[0xaa]).expect("corrupt segment");
        file.sync_all().expect("persist corruption");
        // The quarantine cannot take the file, which stays where it was.
        let quarantine = directory.path().join("quarantine");
        std::fs::remove_dir(&quarantine).expect("remove quarantine");
        std::fs::write(&quarantine, b"not a directory").expect("block the quarantine");
        assert!(matches!(
            store
                .read_block(&segment.metadata.id, BlockNumber(1_980))
                .await,
            Err(HistoryStoreError::Quarantined { .. })
        ));
        std::fs::remove_file(&quarantine).expect("unblock the quarantine");
        std::fs::create_dir(&quarantine).expect("quarantine directory");
        // Review 2 N3: the stale file collided with the requeued job's
        // segment, and failed the job.
        let outcome = Box::pin(runner.run(&id, CancellationToken::new())).await;
        assert!(
            matches!(outcome, Ok(RawHistoryRunOutcome::Complete(_))),
            "{outcome:?}"
        );
        assert_eq!(source.open_calls(), 2);
        let refilled = store.segments().await.expect("segments").remove(0);
        store
            .read_block(&refilled.metadata.id, BlockNumber(1_980))
            .await
            .expect("the segment acquired again reads");
    }

    /// A source whose first `failures` opens find it unreachable.
    #[derive(Debug)]
    struct UnreachableFirst {
        inner: Arc<ScriptedHistorySource>,
        failures: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl HistorySource for UnreachableFirst {
        fn descriptor(&self) -> &SourceDescriptor {
            self.inner.descriptor()
        }

        async fn plan(
            &self,
            request: &DataRequest,
        ) -> Result<leani_source_api::SourcePlan, SourceError> {
            self.inner.plan(request).await
        }

        async fn open(
            &self,
            chunk: &leani_source_api::SourceChunk,
            budget: SourceBudget,
            cancellation: CancellationToken,
        ) -> Result<leani_source_api::BlockFrameStream, SourceError> {
            if self
                .failures
                .fetch_update(
                    std::sync::atomic::Ordering::SeqCst,
                    std::sync::atomic::Ordering::SeqCst,
                    |left| left.checked_sub(1),
                )
                .is_ok()
            {
                return Err(SourceError::Unavailable("connection refused".to_owned()));
            }
            self.inner.open(chunk, budget, cancellation).await
        }
    }

    /// A source that, as it opens its first chunk, lets another job retain
    /// `retained` as a compatible segment.
    #[derive(Debug)]
    struct RacingSource {
        inner: Arc<ScriptedHistorySource>,
        store: HistoryStore,
        retained: std::sync::Mutex<Option<Vec<BlockFrame>>>,
    }

    #[async_trait::async_trait]
    impl HistorySource for RacingSource {
        fn descriptor(&self) -> &SourceDescriptor {
            self.inner.descriptor()
        }

        async fn plan(
            &self,
            request: &DataRequest,
        ) -> Result<leani_source_api::SourcePlan, SourceError> {
            self.inner.plan(request).await
        }

        async fn open(
            &self,
            chunk: &leani_source_api::SourceChunk,
            budget: SourceBudget,
            cancellation: CancellationToken,
        ) -> Result<leani_source_api::BlockFrameStream, SourceError> {
            let retained = self
                .retained
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            if let Some(frames) = retained {
                let capabilities = frames[0].capabilities();
                let mut pending = self
                    .store
                    .begin_segment(
                        SegmentId::new("retained-by-another-job").expect("segment ID"),
                        SegmentDescriptor {
                            chain_id: ChainId(1),
                            range: BlockRange::new(
                                frames[0].block.number,
                                frames[frames.len() - 1].block.number,
                            )
                            .expect("range"),
                            material_shape: RawHistoryMaterialProfile::default().shape_id(),
                            present_capabilities: capabilities.present,
                            complete_capabilities: capabilities.complete,
                            verification: VerificationClass::Cryptographic,
                            trust: TrustModel::ProtocolVerified,
                        },
                        Compression::Snappy,
                        SegmentReservation::new(1024 * 1024, 1024 * 1024),
                    )
                    .await
                    .expect("begin the other job's segment");
                for frame in &frames {
                    pending.append(frame).expect("append");
                }
                pending
                    .commit(&[SegmentOwnerClaim {
                        kind: SegmentOwnerKind::OperatorPin,
                        owner_id: "pin:other-job".to_owned(),
                    }])
                    .await
                    .expect("commit the other job's segment");
            }
            self.inner.open(chunk, budget, cancellation).await
        }
    }

    #[tokio::test]
    async fn each_acquisition_first_claims_what_other_jobs_retained() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(config(directory.path()))
            .await
            .expect("store");
        let all = frames(1_300, 1_305);
        let range = BlockRange::new(BlockNumber(1_300), BlockNumber(1_305)).expect("range");
        let scripted = Arc::new(ScriptedHistorySource::from_frames(
            fixture_source_descriptor("racing-archive", range),
            all.clone(),
        ));
        let source = Arc::new(RacingSource {
            inner: scripted.clone(),
            store: store.clone(),
            retained: std::sync::Mutex::new(Some(all[3..].to_vec())),
        });
        let sources = source_set(source);
        let id = RawHistoryJobId::new("raw-overlapped").expect("job ID");
        store
            .create_raw_history_job(id.clone(), spec(range, sources.policy_digest()))
            .await
            .expect("create");
        let runner =
            RawHistoryRunner::new(store.clone(), sources, default_source_budget()).expect("runner");
        let outcome = Box::pin(runner.run(&id, CancellationToken::new())).await;
        // Audit M-H2: the job acquired the range again and collided with the
        // other job's segment.
        let Ok(RawHistoryRunOutcome::Complete(job)) = outcome else {
            panic!("the overlapped job did not complete: {outcome:?}");
        };
        assert_eq!(job.committed_segments, 2);
        assert_eq!(
            scripted.open_calls(),
            1,
            "the range another job retained was acquired again"
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
