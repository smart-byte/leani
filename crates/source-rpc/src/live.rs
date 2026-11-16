use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
    time::Duration,
};

use alloy_primitives::B256;
use async_trait::async_trait;
use futures::{StreamExt, stream};
use leani_primitives::{
    BlockFrame, BlockHash, BlockNumber, BlockRange, BlockRef, ChainId, Finality, SourceId,
    SourceKind, TrustModel,
};
use leani_source_api::{
    ChainEvent, ChainEventStream, DataRequest, FinalityModel, LiveSource, LiveStart, Partitioning,
    SourceBudget, SourceChunk, SourceDescriptor, SourceError,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::{
    Check, FetchMode, PART_BLOCKS, RpcQuorum, decode_block,
    feed::SignedHashes,
    frame,
    history::{Label, capabilities, decode_chain, fetch_range},
    nitro::{SEQUENCED, sequenced_frame, transactions_root},
    unix_now_ms,
};

/// Delivered blocks kept to find a fork point.
const RECENT_BLOCKS: usize = 256;
/// Blocks fetched and quorum-checked together.
const BATCH_BLOCKS: u64 = 32;
/// Blocks fetched together while catching up, spread over the endpoints.
const CATCH_UP_BLOCKS: u64 = 256;
/// How often the endpoints' `safe` block is read for promotions.
const SAFE_POLL: Duration = Duration::from_secs(2);
/// One in this many blocks made from the feed is checked against the
/// transactions root an endpoint's header names.
const CHECK_EVERY: u64 = 1_000;
/// How long a check waits once an endpoint did not serve the header.
const CHECK_RETRY: Duration = Duration::from_secs(30);

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
    /// links to, so the endpoints are trusted for nothing but serving. Near
    /// the newest signed block, the lane asks a single endpoint for each
    /// block the feed signs: the one that served the last blocks.
    ///
    /// A request that takes filtered material and needs nothing but
    /// transactions' bodies, calldata, and senders gets the blocks the
    /// sequencer built from signed transactions without the endpoints: the
    /// lane makes them from the feed's messages, their transactions declared
    /// complete. Blocks of messages from L1 and of calls to `ArbRetryableTx`
    /// still come from the endpoints, as does the header of one made block
    /// in a thousand, which checks the transactions made.
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
            sequenced: self.signed.is_some()
                && request.allow_filtered
                && SEQUENCED.contains_all(request.required.with_derivable()),
            made: false,
            unchecked: None,
            next_check: 0,
            signings: self.signed.as_ref().map(SignedHashes::changes),
            source: self.clone(),
            recent,
            next,
            head: 0,
            preferred: 0,
            unserved: None,
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
    /// Whether the lane makes blocks from the feed's messages: the request
    /// needs nothing they lack, and no check found a block `ArbOS` made
    /// otherwise.
    sequenced: bool,
    /// Whether the lane made a block from the feed's messages: their feed,
    /// unlike gossip that signs hashes only, keeps them.
    made: bool,
    /// A block made from the feed, its hash and transactions root, and when
    /// an endpoint may be asked for its header to check it.
    unchecked: Option<(u64, B256, B256, tokio::time::Instant)>,
    /// The lowest block the next check takes.
    next_check: u64,
    /// Changes each time the sequencer signs a block.
    signings: Option<tokio::sync::watch::Receiver<()>>,
    source: RpcLiveSource,
    recent: VecDeque<BlockRef>,
    next: u64,
    head: u64,
    /// The endpoint asked first under the feed: the one that served the last
    /// signed blocks, or the next in pool order once it failed or lacked a
    /// signed block twice.
    preferred: usize,
    /// A signed block the preferred endpoint did not serve yet, and when it
    /// was asked for it.
    unserved: Option<(u64, tokio::time::Instant)>,
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
            Label::Unattested,
            self.source.signed.is_none(),
        )
        .await?;
        self.check_signed(&frames)?;
        Ok(frames)
    }

    /// Refuse a block off the chain the sequencer signed.
    fn check_signed(&self, frames: &[BlockFrame]) -> Result<(), SourceError> {
        match frames.iter().find(|frame| {
            self.trusted_hash(frame.block.number.0)
                .is_some_and(|hash| hash.0 != frame.block.hash.0)
        }) {
            Some(frame) => Err(SourceError::CorruptFrame(format!(
                "block {} is not on the chain the sequencer signed",
                frame.block.number.0
            ))),
            None => Ok(()),
        }
    }

    /// The blocks from `next` through `last` that the preferred endpoint
    /// serves, near the newest signed block. A block it lacks is asked for
    /// again, alone, a poll interval later, and the next endpoint is
    /// preferred once it still lacks it then, or fails.
    async fn fetch_signed(&mut self, last: u64) -> Result<Vec<BlockFrame>, SourceError> {
        let next = self.next;
        let last = match self.unserved {
            Some((unserved, asked)) if unserved == next => {
                if asked.elapsed() < self.source.poll_interval {
                    return Ok(Vec::new());
                }
                next
            }
            _ => last,
        };
        let observed = unix_now_ms();
        let asked = tokio::time::Instant::now();
        let served = self
            .source
            .quorum
            .served_blocks(self.preferred, next, last, self.mode)
            .await;
        let (served_by, served) = match served {
            Ok(served) => served,
            Err(error) => {
                debug!(%error, "a json-rpc endpoint failed at the signed head; preferring the next");
                self.preferred = self.preferred.wrapping_add(1);
                self.unserved = None;
                return Ok(Vec::new());
            }
        };
        if served.is_empty() {
            if self.unserved.is_some_and(|(unserved, _)| unserved == next) {
                self.preferred = self.preferred.wrapping_add(1);
                self.unserved = None;
            } else {
                self.unserved = Some((next, asked));
            }
            return Ok(Vec::new());
        }
        match self.signed_frames(&served, next, observed) {
            Ok(frames) => {
                let after = frames.last().map_or(next, |frame| frame.block.number.0 + 1);
                self.preferred = served_by;
                self.unserved = (after <= last).then_some((after, asked));
                Ok(frames)
            }
            Err(error) => {
                self.preferred = served_by.wrapping_add(1);
                self.unserved = None;
                Err(error)
            }
        }
    }

    /// The frames of blocks served from `first` on, each checked against its
    /// number and the hash the sequencer signed.
    fn signed_frames(
        &self,
        served: &[(serde_json::Value, serde_json::Value)],
        first: u64,
        observed: u64,
    ) -> Result<Vec<BlockFrame>, SourceError> {
        let descriptor = &self.source.descriptor;
        let frames = decode_chain(served)?
            .iter()
            .zip(first..)
            .map(|(decoded, number)| {
                if decoded.header.number != number {
                    return Err(SourceError::CorruptFrame(format!(
                        "block {} came for block {number}",
                        decoded.header.number
                    )));
                }
                frame(
                    decoded,
                    descriptor.chain_id,
                    &descriptor.id,
                    Finality::Unattested,
                    observed,
                    None,
                )
                .map_err(Into::into)
            })
            .collect::<Result<Vec<_>, SourceError>>()?;
        self.check_signed(&frames)?;
        Ok(frames)
    }

    /// The blocks from `next` through `last` made from the sequencer's
    /// messages, up to the first whose transactions need the endpoints, each
    /// on the block the sequencer signed below it.
    fn sequenced_frames(&mut self, last: u64) -> Vec<BlockFrame> {
        let Some(signed) = self.source.signed.as_ref().filter(|_| self.sequenced) else {
            return Vec::new();
        };
        let descriptor = &self.source.descriptor;
        let observed = unix_now_ms();
        let mut parent = self.recent.back().copied();
        let mut frames = Vec::new();
        for number in self.next..=last {
            let Some(frame) = parent
                .filter(|parent| {
                    signed
                        .get(parent.number.0)
                        .is_some_and(|hash| hash.0 == parent.hash.0)
                })
                .zip(signed.block(number))
                .and_then(|(parent, block)| {
                    sequenced_frame(
                        &block,
                        &parent,
                        descriptor.chain_id,
                        &descriptor.id,
                        observed,
                    )
                })
            else {
                break;
            };
            parent = Some(frame.block);
            frames.push(frame);
        }
        self.made |= !frames.is_empty();
        if self.unchecked.is_none()
            && let Some(frame) = frames
                .iter()
                .find(|frame| frame.block.number.0 >= self.next_check)
        {
            let number = frame.block.number.0;
            self.unchecked = Some((
                number,
                B256::from(frame.block.hash.0),
                transactions_root(frame),
                tokio::time::Instant::now(),
            ));
            self.next_check = number.saturating_add(CHECK_EVERY);
        }
        frames
    }

    /// Check the block taken for a check against the header an endpoint
    /// serves for it, once the endpoints likely serve it. Another
    /// transactions root there means `ArbOS` made blocks in a way the lane
    /// does not, and the lane fetches every block from then on.
    async fn check_sequenced(&mut self) {
        let Some((number, hash, root, due)) = self.unchecked else {
            return;
        };
        if self.next < number.saturating_add(BATCH_BLOCKS) || tokio::time::Instant::now() < due {
            return;
        }
        // Signed again with another hash, the block made is no longer the
        // chain's, and its header would not match.
        if self
            .trusted_hash(number)
            .is_some_and(|signed| signed != hash)
        {
            self.unchecked = None;
            return;
        }
        let served = self
            .source
            .quorum
            .served_blocks(self.preferred, number, number, FetchMode::Header)
            .await;
        let header = served
            .ok()
            .and_then(|(_, served)| decode_block(&served.first()?.0, &serde_json::Value::Null).ok())
            .filter(|decoded| decoded.check.header == Check::Verified && decoded.hash == hash);
        let Some(header) = header else {
            self.unchecked = Some((
                number,
                hash,
                root,
                tokio::time::Instant::now() + CHECK_RETRY,
            ));
            return;
        };
        self.unchecked = None;
        if header.header.transactions_root != root {
            warn!(
                block = number,
                "a block made from the sequencer feed holds other transactions than its header names; fetching every block from the endpoints"
            );
            self.sequenced = false;
        }
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
            self.head = match &self.source.signed {
                // An endpoint cannot claim a head the sequencer has not
                // signed, and the feed names each block it signs: near the
                // newest, the lane asks for the signed blocks themselves,
                // and further behind it catches up to the newest block an
                // endpoint serves.
                Some(signed) => match signed.newest() {
                    Some((newest, _)) if newest >= self.next.saturating_add(BATCH_BLOCKS) => {
                        newest.min(self.source.quorum.agreed_tag("latest").await?.0)
                    }
                    Some((newest, _)) => newest,
                    None => self.head,
                },
                None => self.source.quorum.agreed_tag("latest").await?.0,
            };
        }
        let head = self.head;
        if self.next <= head {
            let frames = if self.source.signed.is_some() && head - self.next < BATCH_BLOCKS {
                self.check_sequenced().await;
                let last = head.min(self.next.saturating_add(PART_BLOCKS - 1));
                let made = self.sequenced_frames(last);
                if made.is_empty() {
                    // The blocks after one whose message does not make it may
                    // again be made from the feed.
                    let alone = self.sequenced
                        && self
                            .source
                            .signed
                            .as_ref()
                            .is_some_and(|signed| signed.block(self.next).is_some());
                    self.fetch_signed(if alone { self.next } else { last })
                        .await?
                } else {
                    made
                }
            } else {
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
                self.fetch(self.next, last).await?
            };
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
        if let Some(signings) = &mut state.signings {
            signings.borrow_and_update();
        }
        let cancellation = state.cancellation.clone();
        let advanced = tokio::select! {
            () = cancellation.cancelled() => continue,
            advanced = state.advance() => advanced,
        };
        // A lane making blocks from the feed that reached the newest signed
        // block makes the next one as soon as it is signed; one waiting on
        // an endpoint keeps to the poll interval.
        let on_signing =
            advanced.is_ok() && state.sequenced && state.made && state.next > state.head;
        // What a round fetched goes out at once; only a round with nothing
        // new waits before the next, so a moving head is followed round
        // after round.
        let wait = match advanced {
            Ok(()) => state.source.poll_interval,
            Err(error @ SourceError::Disconnected(_)) => {
                state.terminal = true;
                return Some((Err(error), state));
            }
            Err(error) => {
                // A rate limit or an endpoint falling behind is transient.
                debug!(%error, "json-rpc live poll failed; retrying");
                state.source.poll_interval * 2
            }
        };
        if state.pending.is_empty() {
            let signings = state.signings.as_mut().filter(|_| on_signing);
            let signed = async move {
                if let Some(signings) = signings
                    && signings.changed().await.is_ok()
                {
                    return;
                }
                std::future::pending::<()>().await;
            };
            tokio::select! {
                () = state.cancellation.cancelled() => {}
                () = tokio::time::sleep(wait) => {}
                () = signed => {}
            }
        }
    }
}
