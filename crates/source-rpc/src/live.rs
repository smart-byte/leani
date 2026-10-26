use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
    time::Duration,
};

use alloy_primitives::B256;
use async_trait::async_trait;
use futures::{StreamExt, stream};
use leani_primitives::{
    BlockHash, BlockNumber, BlockRange, BlockRef, ChainId, SourceId, SourceKind, TrustModel,
};
use leani_source_api::{
    ChainEvent, ChainEventStream, DataRequest, FinalityModel, LiveSource, LiveStart, Partitioning,
    SourceBudget, SourceChunk, SourceDescriptor, SourceError,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::{
    FetchMode, RpcQuorum, feed::SignedHashes, history::capabilities, history::fetch_range,
};

/// Delivered blocks kept to find a fork point.
const RECENT_BLOCKS: usize = 256;
/// Blocks fetched and quorum-checked together.
const BATCH_BLOCKS: u64 = 32;
/// Blocks fetched together while catching up, spread over the endpoints.
const CATCH_UP_BLOCKS: u64 = 256;
/// How often the endpoints' `safe` block is read for promotions.
const SAFE_POLL: Duration = Duration::from_secs(2);

#[derive(Clone, Debug)]
pub struct RpcLiveSource {
    descriptor: SourceDescriptor,
    quorum: RpcQuorum,
    poll_interval: Duration,
    signed: Option<SignedHashes>,
    /// Hashes proven down from a signed block, every few hundred blocks: a
    /// catch-up batch ends at one, so it is checked before it is delivered.
    proven: Arc<BTreeMap<u64, B256>>,
}

impl RpcLiveSource {
    /// # Errors
    ///
    /// Returns an error for an invalid source id.
    pub fn new(
        id: &str,
        chain_id: ChainId,
        quorum: RpcQuorum,
        poll_interval: Duration,
    ) -> Result<Self, SourceError> {
        Ok(Self {
            descriptor: SourceDescriptor {
                id: SourceId::new(id)
                    .map_err(|error| SourceError::InvalidPlan(error.to_string()))?,
                kind: SourceKind::JsonRpc,
                chain_id,
                range: None,
                capabilities: capabilities(),
                complete_capabilities: capabilities(),
                trust: TrustModel::TrustedDataset,
                finality: FinalityModel::Attested,
                partitioning: Partitioning::None,
                expected_lag: poll_interval,
                schema_version: "json-rpc.v1".to_owned(),
                priority: 0,
            },
            quorum,
            poll_interval,
            signed: None,
            proven: Arc::default(),
        })
    }

    /// Catch up along hashes proven down from a signed block, as
    /// [`crate::RpcQuorum::prove_down`] returns them.
    #[must_use]
    pub fn with_proven_hashes(mut self, proven: BTreeMap<u64, B256>) -> Self {
        self.proven = Arc::new(proven);
        self
    }

    /// Check blocks against the hashes the sequencer signed: a block whose
    /// hash differs is refused, and a signed one attests the blocks it
    /// links to, so the endpoints are trusted for nothing but serving.
    #[must_use]
    pub fn with_signed_hashes(mut self, signed: SignedHashes) -> Self {
        self.descriptor.trust = TrustModel::ProtocolVerified;
        self.signed = Some(signed);
        self
    }
}

#[async_trait]
impl LiveSource for RpcLiveSource {
    fn descriptor(&self) -> &SourceDescriptor {
        &self.descriptor
    }

    async fn subscribe(
        &self,
        request: DataRequest,
        start: LiveStart,
        budget: SourceBudget,
        cancellation: CancellationToken,
    ) -> Result<ChainEventStream, SourceError> {
        budget.validate()?;
        if request.chain_id != self.descriptor.chain_id {
            return Err(SourceError::InvalidPlan(
                "live request belongs to another chain".to_owned(),
            ));
        }
        let (recent, next) = match start {
            LiveStart::Block(block) => (VecDeque::from([block]), block.number.0 + 1),
            LiveStart::RetainedCanonical { canonical } => {
                let next = canonical
                    .last()
                    .map_or(0, |block| block.number.0.saturating_add(1));
                (canonical.into(), next)
            }
            LiveStart::AnchoredOverlap {
                anchor,
                overlap_blocks,
            } => (
                VecDeque::new(),
                anchor
                    .number
                    .0
                    .saturating_sub(overlap_blocks.saturating_sub(1)),
            ),
            LiveStart::Cursor(_) => {
                return Err(SourceError::InvalidPlan(
                    "json-rpc live sources resume from blocks, not cursors".to_owned(),
                ));
            }
            LiveStart::Head => {
                let (head, _) = self.quorum.agreed_tag("latest").await?;
                (VecDeque::new(), head)
            }
        };
        let state = LiveState {
            mode: FetchMode::for_capabilities(request.required),
            source: self.clone(),
            recent,
            next,
            head: 0,
            attested_through: 0,
            safe_polled: None,
            pending: VecDeque::new(),
            cancellation,
            terminal: false,
        };
        Ok(stream::unfold(state, next_event).boxed())
    }
}

struct LiveState {
    mode: FetchMode,
    source: RpcLiveSource,
    recent: VecDeque<BlockRef>,
    next: u64,
    head: u64,
    attested_through: u64,
    safe_polled: Option<tokio::time::Instant>,
    pending: VecDeque<ChainEvent>,
    cancellation: CancellationToken,
    terminal: bool,
}

impl LiveState {
    fn remember(&mut self, block: BlockRef) {
        self.recent.push_back(block);
        while self.recent.len() > RECENT_BLOCKS {
            self.recent.pop_front();
        }
    }

    fn delivered(&self, number: u64) -> Option<BlockRef> {
        self.recent
            .iter()
            .rev()
            .find(|block| block.number.0 == number)
            .copied()
    }

    /// Fetch `first..=last`, linked to what was delivered and checked at its
    /// tip with the quorum, and each against the hash the sequencer signed.
    async fn fetch(
        &self,
        first: u64,
        last: u64,
    ) -> Result<Vec<leani_primitives::BlockFrame>, SourceError> {
        let chunk = SourceChunk {
            source_id: self.source.descriptor.id.clone(),
            range: BlockRange::new(BlockNumber(first), BlockNumber(last))
                .map_err(|error| SourceError::InvalidPlan(error.to_string()))?,
            partition: Vec::new(),
            schema_version: self.source.descriptor.schema_version.clone(),
            expected_parent: None,
            estimated_bytes: None,
        };
        let frames = fetch_range(
            &self.source.quorum,
            &self.source.descriptor,
            &chunk,
            self.mode,
            true,
            self.source.signed.is_none(),
        )
        .await?;
        if let Some(frame) = frames.iter().find(|frame| {
            self.trusted_hash(frame.block.number.0)
                .is_some_and(|hash| hash.0 != frame.block.hash.0)
        }) {
            return Err(SourceError::CorruptFrame(format!(
                "block {} is not on the chain the sequencer signed",
                frame.block.number.0
            )));
        }
        Ok(frames)
    }

    /// The hash the sequencer signed at `number`, or that a proof down from a
    /// signed block found there.
    fn trusted_hash(&self, number: u64) -> Option<B256> {
        self.source
            .signed
            .as_ref()
            .and_then(|signed| signed.get(number))
            .or_else(|| self.source.proven.get(&number).copied())
    }

    /// The newest delivered block on the signed chain: it and every block it
    /// links down to are attested.
    fn signed_through(&self) -> Option<BlockRef> {
        self.recent
            .iter()
            .rev()
            .take_while(|block| block.number.0 > self.attested_through)
            .find(|block| {
                self.trusted_hash(block.number.0)
                    .is_some_and(|hash| hash.0 == block.hash.0)
            })
            .copied()
    }

    /// The highest delivered block the quorum still has at its height: the
    /// fork point a reorg replays from.
    async fn fork_point(&self) -> Result<Option<BlockRef>, SourceError> {
        for block in self.recent.iter().rev() {
            if self.source.quorum.agreed_hash(block.number.0).await?
                == Some(alloy_primitives::B256::from(block.hash.0))
            {
                return Ok(Some(*block));
            }
        }
        Ok(None)
    }

    async fn advance(&mut self) -> Result<(), SourceError> {
        // While catching up, the known head is still ahead: no need to ask.
        if self.next > self.head {
            let latest = self.source.quorum.agreed_tag("latest").await?.0;
            // An endpoint cannot claim a head the sequencer has not signed.
            self.head = match &self.source.signed {
                Some(signed) => signed.newest().map_or(0, |(number, _)| number.min(latest)),
                None => latest,
            };
        }
        let head = self.head;
        if self.next <= head {
            let batch = if head - self.next >= BATCH_BLOCKS {
                CATCH_UP_BLOCKS
            } else {
                BATCH_BLOCKS
            };
            let mut last = head.min(self.next.saturating_add(batch - 1));
            // A catch-up batch ends at a proven block, which checks it all.
            if let Some((tip, _)) = self.source.proven.range(self.next..).next() {
                last = last.min(*tip);
            }
            let frames = self.fetch(self.next, last).await?;
            let links = match (self.recent.back(), frames.first()) {
                (Some(tip), Some(first)) => first.block.parent_hash == tip.hash,
                _ => true,
            };
            if links {
                for frame in frames {
                    self.remember(frame.block);
                    self.next = frame.block.number.0 + 1;
                    self.pending.push_back(ChainEvent::Block(Box::new(frame)));
                }
            } else {
                self.reorg().await?;
            }
        }
        if self.source.signed.is_some() {
            if let Some(block) = self.signed_through() {
                self.attested_through = block.number.0;
                self.pending
                    .push_back(ChainEvent::Attested { through: block });
            }
            return Ok(());
        }
        if self
            .safe_polled
            .is_some_and(|polled| polled.elapsed() < SAFE_POLL)
        {
            return Ok(());
        }
        self.safe_polled = Some(tokio::time::Instant::now());
        let (safe, safe_hash) = self.source.quorum.agreed_tag("safe").await?;
        let through = safe.min(self.next.saturating_sub(1));
        if through > self.attested_through
            && let Some(block) = self.delivered(through)
            && (through != safe || block.hash == BlockHash::new(safe_hash.0))
        {
            self.attested_through = through;
            self.pending
                .push_back(ChainEvent::Attested { through: block });
        }
        Ok(())
    }

    async fn reorg(&mut self) -> Result<(), SourceError> {
        let fork = self.fork_point().await?.ok_or_else(|| {
            SourceError::Disconnected(format!(
                "the endpoints' chain left the last {} delivered blocks",
                self.recent.len()
            ))
        })?;
        let reverted = self
            .recent
            .iter()
            .filter(|block| block.number > fork.number)
            .copied()
            .rev()
            .collect::<Vec<_>>();
        let last = self.head.min(fork.number.0.saturating_add(BATCH_BLOCKS));
        let applied = self.fetch(fork.number.0 + 1, last).await?;
        warn!(
            fork = fork.number.0,
            reverted = reverted.len(),
            applied = applied.len(),
            "json-rpc endpoints reorganized the chain"
        );
        self.recent.retain(|block| block.number <= fork.number);
        for frame in &applied {
            self.remember(frame.block);
            self.next = frame.block.number.0 + 1;
        }
        self.attested_through = self.attested_through.min(fork.number.0);
        self.pending
            .push_back(ChainEvent::Reorg { reverted, applied });
        Ok(())
    }
}

async fn next_event(mut state: LiveState) -> Option<(Result<ChainEvent, SourceError>, LiveState)> {
    if state.terminal {
        return None;
    }
    loop {
        if let Some(event) = state.pending.pop_front() {
            return Some((Ok(event), state));
        }
        if state.cancellation.is_cancelled() {
            state.terminal = true;
            return Some((Err(SourceError::Cancelled), state));
        }
        let cancellation = state.cancellation.clone();
        let advanced = tokio::select! {
            () = cancellation.cancelled() => continue,
            advanced = state.advance() => advanced,
        };
        match advanced {
            Ok(()) if state.pending.is_empty() || state.next > state.head => {
                tokio::select! {
                    () = state.cancellation.cancelled() => {}
                    () = tokio::time::sleep(state.source.poll_interval) => {}
                }
            }
            Ok(()) => {}
            Err(error @ SourceError::Disconnected(_)) => {
                state.terminal = true;
                return Some((Err(error), state));
            }
            Err(error) => {
                // A rate limit or an endpoint falling behind is transient.
                debug!(%error, "json-rpc live poll failed; retrying");
                tokio::select! {
                    () = state.cancellation.cancelled() => {}
                    () = tokio::time::sleep(state.source.poll_interval * 2) => {}
                }
            }
        }
    }
}
