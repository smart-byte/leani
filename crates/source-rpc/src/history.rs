use std::{collections::BTreeMap, sync::Arc, time::Duration};

use alloy_primitives::B256;
use async_trait::async_trait;
use futures::{StreamExt, stream};
use leani_primitives::{
    BlockHash, BlockNumber, BlockRange, Capability, CapabilitySet, ChainId, Finality, SourceId,
    SourceKind, TrustModel,
};
use leani_source_api::{
    BlockFrameStream, DataRequest, FinalityModel, HistorySource, Partitioning, SourceBudget,
    SourceChunk, SourceDescriptor, SourceError, SourcePlan,
};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{FetchMode, RpcQuorum, decode_block, frame, unix_now_ms};

/// Blocks per chunk: each chunk is authenticated by its parent links down
/// from one trusted tip. Chunks end on multiples of it, so an anchored
/// source keeps one proven hash per chunk.
const CHUNK_BLOCKS: u64 = 64;

/// Blocks an anchored source proves per step: a failed step loses no more.
const PROOF_STEP: u64 = 65_536;

/// How a fetched range's blocks are labeled.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Label {
    /// Live blocks start unattested; the live source promotes them.
    Unattested,
    /// Finalized up to the endpoints' `finalized` block, attested above.
    Tagged,
    /// Below a finalized anchor.
    Finalized,
}

pub(crate) fn capabilities() -> CapabilitySet {
    CapabilitySet::of(Capability::Header)
        .with(Capability::Body)
        .with(Capability::Transactions)
        .with(Capability::Calldata)
        .with(Capability::Receipts)
        .with(Capability::Logs)
        .with(Capability::Withdrawals)
}

#[derive(Clone, Debug)]
pub struct RpcHistorySource {
    descriptor: SourceDescriptor,
    quorum: RpcQuorum,
    /// For an anchored source, the hashes proven down from its anchor: the
    /// anchor's and each chunk end the proof has reached.
    proven: Option<Arc<tokio::sync::Mutex<BTreeMap<u64, B256>>>>,
}

impl RpcHistorySource {
    /// # Errors
    ///
    /// Returns an error for an invalid source id.
    pub fn new(
        id: &str,
        chain_id: ChainId,
        quorum: RpcQuorum,
        priority: u16,
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
                finality: FinalityModel::Finalized,
                partitioning: Partitioning::FixedBlockSpan(CHUNK_BLOCKS),
                expected_lag: Duration::from_secs(2),
                schema_version: "json-rpc.v1".to_owned(),
                priority,
            },
            quorum,
            proven: None,
        })
    }

    /// History up to `anchor`, a finalized block whose hash is trusted. Each
    /// chunk must lead to a hash proven by parent links down from it, so no
    /// endpoint is trusted.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid source id.
    pub fn anchored(
        id: &str,
        chain_id: ChainId,
        quorum: RpcQuorum,
        priority: u16,
        anchor: (u64, B256),
    ) -> Result<Self, SourceError> {
        let mut source = Self::new(id, chain_id, quorum, priority)?;
        source.descriptor.trust = TrustModel::ProtocolVerified;
        source.descriptor.range = Some(
            BlockRange::new(BlockNumber(0), BlockNumber(anchor.0))
                .map_err(|error| SourceError::InvalidPlan(error.to_string()))?,
        );
        source.proven = Some(Arc::new(tokio::sync::Mutex::new(BTreeMap::from([anchor]))));
        Ok(source)
    }

    /// The proven hash of `number`, a chunk end or the anchor. A chunk below
    /// what the proof reached waits for it to walk further down.
    async fn proven_hash(
        &self,
        proven: &tokio::sync::Mutex<BTreeMap<u64, B256>>,
        number: u64,
    ) -> Result<B256, SourceError> {
        let mut proven = proven.lock().await;
        loop {
            if let Some(hash) = proven.get(&number) {
                return Ok(*hash);
            }
            let (lowest, hash) = proven
                .first_key_value()
                .map(|(number, hash)| (*number, *hash))
                .ok_or_else(|| SourceError::InvalidPlan("no anchor".to_owned()))?;
            if number > lowest {
                return Err(SourceError::InvalidPlan(format!(
                    "block {number} ends no chunk"
                )));
            }
            let from = lowest.saturating_sub(PROOF_STEP).max(number);
            info!(
                from,
                through = lowest,
                "proving JSON-RPC history down from its anchor"
            );
            proven.extend(
                self.quorum
                    .prove_down(lowest, hash, from, CHUNK_BLOCKS)
                    .await?,
            );
        }
    }

    /// An anchored source's chunk, fetched through the next chunk end and
    /// checked against its proven hash.
    async fn proven_range(
        &self,
        proven: &tokio::sync::Mutex<BTreeMap<u64, B256>>,
        chunk: &SourceChunk,
        mode: FetchMode,
    ) -> Result<Vec<leani_primitives::BlockFrame>, SourceError> {
        let anchor = self.descriptor.range.map_or(0, |range| range.end().0);
        if chunk.range.end().0 > anchor {
            return Err(SourceError::MissingRange(chunk.range));
        }
        let top = chunk_end(chunk.range.end().0).min(anchor);
        let trusted = self.proven_hash(proven, top).await?;
        let mut fetched = chunk.clone();
        fetched.range = BlockRange::new(chunk.range.start(), BlockNumber(top))
            .map_err(|error| SourceError::InvalidPlan(error.to_string()))?;
        let mut frames = fetch_range(
            &self.quorum,
            &self.descriptor,
            &fetched,
            mode,
            Label::Finalized,
            false,
        )
        .await?;
        if frames.last().map(|frame| frame.block.hash.0) != Some(trusted.0) {
            return Err(SourceError::CorruptFrame(format!(
                "block {top} is not on the chain down from the anchor"
            )));
        }
        frames.truncate(usize::try_from(chunk.range.len()).unwrap_or(usize::MAX));
        Ok(frames)
    }
}

/// The last block of the chunk holding `number`.
const fn chunk_end(number: u64) -> u64 {
    number | (CHUNK_BLOCKS - 1)
}

#[async_trait]
impl HistorySource for RpcHistorySource {
    fn descriptor(&self) -> &SourceDescriptor {
        &self.descriptor
    }

    async fn plan(&self, request: &DataRequest) -> Result<SourcePlan, SourceError> {
        if request.chain_id != self.descriptor.chain_id {
            return Err(SourceError::InvalidPlan(
                "request belongs to another chain".to_owned(),
            ));
        }
        let mode = FetchMode::for_capabilities(request.required);
        let mut chunks = Vec::new();
        let mut next = request.range.start().0;
        while next <= request.range.end().0 {
            let end = chunk_end(next).min(request.range.end().0);
            chunks.push(SourceChunk {
                source_id: self.descriptor.id.clone(),
                range: BlockRange::new(BlockNumber(next), BlockNumber(end))
                    .map_err(|error| SourceError::InvalidPlan(error.to_string()))?,
                // The fetch mode rides in the partition: `open` sees only the
                // chunk.
                partition: std::iter::once(mode.byte())
                    .chain(next.to_be_bytes())
                    .collect(),
                schema_version: self.descriptor.schema_version.clone(),
                expected_parent: None,
                estimated_bytes: None,
            });
            next = end.saturating_add(1);
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
        if chunk.source_id != self.descriptor.id {
            return Err(SourceError::InvalidPlan(
                "chunk belongs to another source".to_owned(),
            ));
        }
        let mode = FetchMode::from_byte(chunk.partition.first().copied().unwrap_or(u8::MAX));
        let fetched = async {
            match &self.proven {
                Some(proven) => self.proven_range(proven, chunk, mode).await,
                None => {
                    fetch_range(
                        &self.quorum,
                        &self.descriptor,
                        chunk,
                        mode,
                        Label::Tagged,
                        true,
                    )
                    .await
                }
            }
        };
        let frames = tokio::select! {
            () = cancellation.cancelled() => return Err(SourceError::Cancelled),
            frames = fetched => frames?,
        };
        let mut bytes = 0_u64;
        for frame in &frames {
            let frame_bytes = frame.estimated_heap_bytes();
            bytes = bytes.saturating_add(frame_bytes);
            if frame_bytes > budget.max_frame_bytes || bytes > budget.max_input_bytes {
                return Err(SourceError::BudgetExceeded {
                    resource: "input_bytes",
                    limit: budget.max_input_bytes,
                    observed: bytes,
                });
            }
        }
        Ok(stream::iter(frames.into_iter().map(Ok)).boxed())
    }
}

/// Fetch, check, and link a chunk, then, with `quorum_tip`, check its tip with
/// the quorum: the parent links extend the quorum's agreement to every block
/// below it. Without it, the caller checks the blocks against what it trusts.
pub(crate) async fn fetch_range(
    quorum: &RpcQuorum,
    descriptor: &SourceDescriptor,
    chunk: &SourceChunk,
    mode: FetchMode,
    label: Label,
    quorum_tip: bool,
) -> Result<Vec<leani_primitives::BlockFrame>, SourceError> {
    let observed = unix_now_ms();
    let blocks = quorum
        .blocks(chunk.range.start().0, chunk.range.end().0, mode)
        .await?
        .iter()
        .map(|(block, receipts)| decode_block(block, receipts))
        .collect::<Result<Vec<_>, _>>()?;
    for pair in blocks.windows(2) {
        if pair[1].header.parent_hash != pair[0].hash {
            return Err(SourceError::CorruptFrame(format!(
                "block {} does not link to block {}",
                pair[1].header.number, pair[0].header.number
            )));
        }
    }
    if let (Some(first), Some(expected)) = (blocks.first(), &chunk.expected_parent)
        && BlockHash::new(first.header.parent_hash.0) != *expected
    {
        return Err(SourceError::CorruptFrame(format!(
            "block {} does not link to the expected parent",
            first.header.number
        )));
    }
    let tip = blocks
        .last()
        .ok_or_else(|| SourceError::InvalidPlan("empty chunk".to_owned()))?;
    match quorum_tip.then(|| quorum.agreed_hash(tip.header.number)) {
        None => {}
        Some(agreed) => match agreed.await? {
            Some(agreed) if agreed == tip.hash => {}
            Some(agreed) => {
                return Err(SourceError::CorruptFrame(format!(
                    "block {} hash {:#x} is not the quorum's {agreed:#x}",
                    tip.header.number, tip.hash
                )));
            }
            None => {
                return Err(SourceError::Unavailable(format!(
                    "too few endpoints serve block {} yet",
                    tip.header.number
                )));
            }
        },
    }
    let finalized = match label {
        Label::Tagged => quorum
            .agreed_tag("finalized")
            .await
            .ok()
            .map(|(number, _)| number),
        Label::Unattested | Label::Finalized => None,
    };
    blocks
        .iter()
        .map(|decoded| {
            let finality = match (label, finalized) {
                (Label::Unattested, _) => Finality::Unattested,
                (Label::Finalized, _) => Finality::Finalized,
                (Label::Tagged, Some(finalized)) if decoded.header.number <= finalized => {
                    Finality::Finalized
                }
                (Label::Tagged, _) => Finality::Attested,
            };
            frame(
                decoded,
                descriptor.chain_id,
                &descriptor.id,
                finality,
                observed,
                None,
            )
            .map_err(Into::into)
        })
        .collect()
}
