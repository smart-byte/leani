//! Deterministic scripted source implementations with cancellation and budgets.

use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use futures::{StreamExt, stream};
use leani_primitives::{BlockFrame, BlockRange, TrustModel};
use leani_source_api::{
    BlockFrameStream, ChainEvent, ChainEventStream, ConsensusCheckpoint, DataRequest,
    FinalityEvent, FinalityEventStream, FinalitySource, HistorySource, LiveSource, LiveStart,
    SelectionPolicy, SourceBudget, SourceChunk, SourceDescriptor, SourceError, SourcePlan,
    VerificationPolicy, coverage_gaps, select_source,
};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug)]
pub enum HistoryStep {
    Frame(Box<BlockFrame>),
    Delay(Duration),
    Error(SourceError),
}

#[derive(Clone, Debug)]
pub struct ScriptedChunk {
    pub range: BlockRange,
    pub schema_version: String,
    pub estimated_bytes: Option<u64>,
    pub steps: Vec<HistoryStep>,
}

#[derive(Clone, Debug)]
pub struct ScriptedHistorySource {
    descriptor: SourceDescriptor,
    chunks: Arc<Vec<ScriptedChunk>>,
    plan_calls: Arc<AtomicUsize>,
    open_calls: Arc<AtomicUsize>,
}

impl ScriptedHistorySource {
    #[must_use]
    pub fn new(descriptor: SourceDescriptor, chunks: Vec<ScriptedChunk>) -> Self {
        Self {
            descriptor,
            chunks: Arc::new(chunks),
            plan_calls: Arc::new(AtomicUsize::new(0)),
            open_calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    #[must_use]
    /// Build one scripted partition from ordered frames.
    ///
    /// # Panics
    ///
    /// Panics when non-empty frames are in descending order, or when an empty
    /// frame list is paired with a descriptor that declares no range.
    pub fn from_frames(descriptor: SourceDescriptor, frames: Vec<BlockFrame>) -> Self {
        let range = if let (Some(first), Some(last)) = (frames.first(), frames.last()) {
            BlockRange::new(first.block.number, last.block.number)
                .expect("ordered fixture frame range")
        } else {
            descriptor
                .range
                .expect("empty fixture source needs a declared range")
        };
        let schema_version = descriptor.schema_version.clone();
        Self::new(
            descriptor,
            vec![ScriptedChunk {
                range,
                schema_version,
                estimated_bytes: None,
                steps: frames
                    .into_iter()
                    .map(|frame| HistoryStep::Frame(Box::new(frame)))
                    .collect(),
            }],
        )
    }

    #[must_use]
    pub fn plan_calls(&self) -> usize {
        self.plan_calls.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn open_calls(&self) -> usize {
        self.open_calls.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl HistorySource for ScriptedHistorySource {
    fn descriptor(&self) -> &SourceDescriptor {
        &self.descriptor
    }

    async fn plan(&self, request: &DataRequest) -> Result<SourcePlan, SourceError> {
        self.plan_calls.fetch_add(1, Ordering::Relaxed);
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

        let ranges = self
            .chunks
            .iter()
            .map(|chunk| chunk.range)
            .collect::<Vec<_>>();
        if let Some(gap) = coverage_gaps(request.range, &ranges).into_iter().next() {
            return Err(SourceError::MissingRange(gap));
        }

        let mut ordered = self.chunks.iter().enumerate().collect::<Vec<_>>();
        ordered.sort_by_key(|(_, chunk)| chunk.range.start().0);
        let mut next = request.range.start().0;
        let mut chunks = Vec::new();
        let mut estimated_bytes = Some(0_u64);
        while next <= request.range.end().0 {
            let (index, scripted) = ordered
                .iter()
                .find(|(_, chunk)| chunk.range.contains(next.into()))
                .ok_or_else(|| {
                    SourceError::MissingRange(
                        BlockRange::new(next.into(), request.range.end())
                            .expect("remaining requested range is ordered"),
                    )
                })?;
            if scripted.schema_version != self.descriptor.schema_version {
                return Err(SourceError::SchemaDrift {
                    expected: self.descriptor.schema_version.clone(),
                    actual: scripted.schema_version.clone(),
                });
            }
            let end = scripted.range.end().0.min(request.range.end().0);
            chunks.push(SourceChunk {
                source_id: self.descriptor.id.clone(),
                range: BlockRange::new(next.into(), end.into()).expect("planned range is ordered"),
                partition: u64::try_from(*index)
                    .expect("fixture index fits u64")
                    .to_be_bytes()
                    .to_vec(),
                schema_version: scripted.schema_version.clone(),
                expected_parent: None,
                estimated_bytes: scripted.estimated_bytes,
            });
            estimated_bytes = estimated_bytes
                .zip(scripted.estimated_bytes)
                .and_then(|(total, size)| total.checked_add(size));
            next = end.saturating_add(1);
        }

        let plan = SourcePlan {
            source_id: self.descriptor.id.clone(),
            request: request.clone(),
            chunks,
            estimated_bytes,
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
        self.open_calls.fetch_add(1, Ordering::Relaxed);
        let budget = budget.validate()?;
        if chunk.source_id != self.descriptor.id {
            return Err(SourceError::InvalidPlan(
                "chunk belongs to another source".to_owned(),
            ));
        }
        if chunk.schema_version != self.descriptor.schema_version {
            return Err(SourceError::SchemaDrift {
                expected: self.descriptor.schema_version.clone(),
                actual: chunk.schema_version.clone(),
            });
        }
        let index = decode_partition(&chunk.partition)?;
        let scripted = self
            .chunks
            .get(index)
            .ok_or_else(|| SourceError::InvalidPlan("unknown fixture partition".to_owned()))?;
        if chunk.range.start().0 < scripted.range.start().0
            || chunk.range.end().0 > scripted.range.end().0
        {
            return Err(SourceError::InvalidPlan(
                "chunk exceeds fixture partition".to_owned(),
            ));
        }

        let state = HistoryState {
            steps: scripted.steps.clone().into(),
            range: chunk.range,
            cancellation,
            budget,
            frames: 0,
            bytes: 0,
            terminal: false,
        };
        Ok(stream::unfold(state, next_history).boxed())
    }
}

#[derive(Debug)]
struct HistoryState {
    steps: VecDeque<HistoryStep>,
    range: BlockRange,
    cancellation: CancellationToken,
    budget: SourceBudget,
    frames: u64,
    bytes: u64,
    terminal: bool,
}

async fn next_history(
    mut state: HistoryState,
) -> Option<(Result<BlockFrame, SourceError>, HistoryState)> {
    if state.terminal {
        return None;
    }
    loop {
        if state.cancellation.is_cancelled() {
            state.terminal = true;
            return Some((Err(SourceError::Cancelled), state));
        }
        let step = state.steps.pop_front()?;
        match step {
            HistoryStep::Delay(duration) => {
                tokio::select! {
                    () = tokio::time::sleep(duration) => {}
                    () = state.cancellation.cancelled() => {
                        state.terminal = true;
                        return Some((Err(SourceError::Cancelled), state));
                    }
                }
            }
            HistoryStep::Error(error) => {
                state.terminal = true;
                return Some((Err(error), state));
            }
            HistoryStep::Frame(frame) => {
                if !state.range.contains(frame.block.number) {
                    continue;
                }
                if let Err(error) = frame.validate_shape() {
                    state.terminal = true;
                    return Some((Err(SourceError::CorruptFrame(error.to_owned())), state));
                }
                let frame_bytes = frame.estimated_heap_bytes();
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
                state.frames = state.frames.saturating_add(1);
                state.bytes = state.bytes.saturating_add(frame_bytes);
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
                return Some((Ok(*frame), state));
            }
        }
    }
}

fn decode_partition(partition: &[u8]) -> Result<usize, SourceError> {
    let bytes: [u8; 8] = partition
        .try_into()
        .map_err(|_| SourceError::InvalidPlan("fixture partition must be 8 bytes".to_owned()))?;
    usize::try_from(u64::from_be_bytes(bytes))
        .map_err(|_| SourceError::InvalidPlan("fixture partition is too large".to_owned()))
}

#[derive(Clone, Debug)]
pub enum LiveStep {
    Event(ChainEvent),
    Delay(Duration),
    Error(SourceError),
}

#[derive(Clone, Debug)]
pub struct ScriptedLiveSource {
    descriptor: SourceDescriptor,
    steps: Arc<Vec<LiveStep>>,
}

impl ScriptedLiveSource {
    #[must_use]
    pub fn new(descriptor: SourceDescriptor, steps: Vec<LiveStep>) -> Self {
        Self {
            descriptor,
            steps: Arc::new(steps),
        }
    }
}

#[async_trait]
impl LiveSource for ScriptedLiveSource {
    fn descriptor(&self) -> &SourceDescriptor {
        &self.descriptor
    }

    async fn subscribe(
        &self,
        _request: DataRequest,
        _start: LiveStart,
        budget: SourceBudget,
        cancellation: CancellationToken,
    ) -> Result<ChainEventStream, SourceError> {
        budget.validate()?;
        let state = EventState {
            steps: self.steps.as_ref().clone().into(),
            cancellation,
            terminal: false,
        };
        Ok(stream::unfold(state, next_live).boxed())
    }
}

#[derive(Debug)]
struct EventState<T> {
    steps: VecDeque<T>,
    cancellation: CancellationToken,
    terminal: bool,
}

async fn next_live(
    mut state: EventState<LiveStep>,
) -> Option<(Result<ChainEvent, SourceError>, EventState<LiveStep>)> {
    if state.terminal {
        return None;
    }
    loop {
        if state.cancellation.is_cancelled() {
            state.terminal = true;
            return Some((Err(SourceError::Cancelled), state));
        }
        match state.steps.pop_front()? {
            LiveStep::Event(event) => return Some((Ok(event), state)),
            LiveStep::Error(error) => {
                state.terminal = true;
                return Some((Err(error), state));
            }
            LiveStep::Delay(duration) => {
                tokio::select! {
                    () = tokio::time::sleep(duration) => {}
                    () = state.cancellation.cancelled() => {
                        state.terminal = true;
                        return Some((Err(SourceError::Cancelled), state));
                    }
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
pub enum FinalityStep {
    Event(FinalityEvent),
    Delay(Duration),
    Error(SourceError),
}

#[derive(Clone, Debug)]
pub struct ScriptedFinalitySource {
    descriptor: SourceDescriptor,
    expected_checkpoint: ConsensusCheckpoint,
    steps: Arc<Vec<FinalityStep>>,
}

impl ScriptedFinalitySource {
    #[must_use]
    pub fn new(
        descriptor: SourceDescriptor,
        expected_checkpoint: ConsensusCheckpoint,
        steps: Vec<FinalityStep>,
    ) -> Self {
        Self {
            descriptor,
            expected_checkpoint,
            steps: Arc::new(steps),
        }
    }
}

#[async_trait]
impl FinalitySource for ScriptedFinalitySource {
    fn descriptor(&self) -> &SourceDescriptor {
        &self.descriptor
    }

    async fn subscribe(
        &self,
        checkpoint: ConsensusCheckpoint,
        cancellation: CancellationToken,
    ) -> Result<FinalityEventStream, SourceError> {
        if checkpoint != self.expected_checkpoint {
            return Err(SourceError::Protocol(
                "weak-subjectivity checkpoint mismatch".to_owned(),
            ));
        }
        let state = EventState {
            steps: self.steps.as_ref().clone().into(),
            cancellation,
            terminal: false,
        };
        Ok(stream::unfold(state, next_finality).boxed())
    }
}

async fn next_finality(
    mut state: EventState<FinalityStep>,
) -> Option<(Result<FinalityEvent, SourceError>, EventState<FinalityStep>)> {
    if state.terminal {
        return None;
    }
    loop {
        if state.cancellation.is_cancelled() {
            state.terminal = true;
            return Some((Err(SourceError::Cancelled), state));
        }
        match state.steps.pop_front()? {
            FinalityStep::Event(event) => return Some((Ok(event), state)),
            FinalityStep::Error(error) => {
                state.terminal = true;
                return Some((Err(error), state));
            }
            FinalityStep::Delay(duration) => {
                tokio::select! {
                    () = tokio::time::sleep(duration) => {}
                    () = state.cancellation.cancelled() => {
                        state.terminal = true;
                        return Some((Err(SourceError::Cancelled), state));
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use leani_primitives::{
        BlockHash, BlockNumber, Capability, CapabilitySet, ChainId, Finality, HeaderEnvelope,
        Material,
    };
    use leani_source_api::{
        FieldProjection, FilterSet, FinalityEvent, HistorySource, VerificationPolicy,
    };

    use crate::{default_source_budget, fixture_frame, fixture_source_descriptor};

    use super::*;

    fn request(range: BlockRange) -> DataRequest {
        DataRequest {
            chain_id: ChainId(1),
            range,
            required: CapabilitySet::of(Capability::Logs),
            allow_filtered: false,
            projection: FieldProjection::default(),
            log_fields: leani_primitives::LogFieldSet::ALL,
            filters: FilterSet::default(),
            minimum_finality: Finality::Finalized,
            verification_policy: VerificationPolicy::CompleteCryptographic,
        }
    }

    #[tokio::test]
    async fn history_models_missing_ranges_and_schema_drift() {
        let full = BlockRange::new(BlockNumber(1), BlockNumber(3)).expect("range");
        let descriptor = fixture_source_descriptor("history", full);
        let source = ScriptedHistorySource::new(
            descriptor.clone(),
            vec![ScriptedChunk {
                range: BlockRange::single(BlockNumber(1)),
                schema_version: descriptor.schema_version.clone(),
                estimated_bytes: Some(1),
                steps: vec![HistoryStep::Frame(Box::new(fixture_frame(
                    1,
                    BlockHash::ZERO,
                )))],
            }],
        );
        assert!(matches!(
            source.plan(&request(full)).await,
            Err(SourceError::MissingRange(_))
        ));

        let drifted = ScriptedHistorySource::new(
            descriptor,
            vec![ScriptedChunk {
                range: full,
                schema_version: "fixture-v2".to_owned(),
                estimated_bytes: None,
                steps: Vec::new(),
            }],
        );
        assert!(matches!(
            drifted.plan(&request(full)).await,
            Err(SourceError::SchemaDrift { .. })
        ));
    }

    #[tokio::test]
    async fn history_stream_honors_cancellation() {
        let range = BlockRange::single(BlockNumber(1));
        let descriptor = fixture_source_descriptor("history", range);
        let source = ScriptedHistorySource::new(
            descriptor.clone(),
            vec![ScriptedChunk {
                range,
                schema_version: descriptor.schema_version.clone(),
                estimated_bytes: None,
                steps: vec![
                    HistoryStep::Delay(Duration::from_mins(1)),
                    HistoryStep::Frame(Box::new(fixture_frame(1, BlockHash::ZERO))),
                ],
            }],
        );
        let plan = source.plan(&request(range)).await.expect("plan");
        let cancellation = CancellationToken::new();
        let mut frames = source
            .open(
                &plan.chunks[0],
                default_source_budget(),
                cancellation.clone(),
            )
            .await
            .expect("open");
        cancellation.cancel();
        assert!(matches!(
            frames.next().await,
            Some(Err(SourceError::Cancelled))
        ));
    }

    #[tokio::test]
    async fn history_stream_rejects_corruption_and_budget_overrun() {
        let range = BlockRange::single(BlockNumber(1));
        let descriptor = fixture_source_descriptor("history", range);
        let mut corrupt = fixture_frame(1, BlockHash::ZERO);
        corrupt.chain_id = ChainId(0);
        let source = ScriptedHistorySource::from_frames(descriptor.clone(), vec![corrupt]);
        let plan = source.plan(&request(range)).await.expect("plan");
        let mut frames = source
            .open(
                &plan.chunks[0],
                default_source_budget(),
                CancellationToken::new(),
            )
            .await
            .expect("open");
        assert!(matches!(
            frames.next().await,
            Some(Err(SourceError::CorruptFrame(_)))
        ));

        let mut large = fixture_frame(1, BlockHash::ZERO);
        large.header = Material::Complete(HeaderEnvelope {
            rlp: Some(vec![1, 2]),
            transactions_root: Some(BlockHash::ZERO),
            receipts_root: Some(BlockHash::ZERO),
            withdrawals_root: None,
            gas_limit: None,
            gas_used: None,
            base_fee_per_gas: None,
            blob_gas_used: None,
            excess_blob_gas: None,
            size_bytes: None,
            consensus_size_bytes: None,
            transaction_count: None,
        });
        let source = ScriptedHistorySource::from_frames(descriptor, vec![large]);
        let plan = source.plan(&request(range)).await.expect("plan");
        let mut budget = default_source_budget();
        budget.max_frames = 1;
        budget.max_frame_bytes = 1;
        let mut frames = source
            .open(&plan.chunks[0], budget, CancellationToken::new())
            .await
            .expect("open");
        assert!(matches!(
            frames.next().await,
            Some(Err(SourceError::BudgetExceeded {
                resource: "frame_bytes",
                ..
            }))
        ));
    }

    #[tokio::test]
    async fn live_and_finality_scripts_expose_failures() {
        let range = BlockRange::single(BlockNumber(1));
        let descriptor = fixture_source_descriptor("live", range);
        let live = ScriptedLiveSource::new(
            descriptor.clone(),
            vec![LiveStep::Error(SourceError::Disconnected(
                "fixture disconnect".to_owned(),
            ))],
        );
        let mut events = live
            .subscribe(
                request(range),
                LiveStart::Head,
                default_source_budget(),
                CancellationToken::new(),
            )
            .await
            .expect("subscribe");
        assert!(matches!(
            events.next().await,
            Some(Err(SourceError::Disconnected(_)))
        ));

        let checkpoint = ConsensusCheckpoint {
            beacon_slot: 1,
            beacon_block_root: [1; 32],
            execution_block_hash: BlockHash::new([2; 32]),
            obtained_at_unix_seconds: 1,
            source: "fixture".to_owned(),
        };
        let finality = ScriptedFinalitySource::new(
            descriptor,
            checkpoint.clone(),
            vec![FinalityStep::Event(FinalityEvent::Disagreement {
                first: BlockHash::new([1; 32]),
                second: BlockHash::new([2; 32]),
                beacon_slot: 2,
            })],
        );
        let mut events = finality
            .subscribe(checkpoint, CancellationToken::new())
            .await
            .expect("subscribe");
        assert!(matches!(
            events.next().await,
            Some(Ok(FinalityEvent::Disagreement { .. }))
        ));
    }
}
