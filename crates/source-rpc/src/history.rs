use std::time::Duration;

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

use crate::{FetchMode, RpcQuorum, decode_block, frame, unix_now_ms};

/// Blocks per chunk: each chunk is authenticated by its parent links down
/// from one quorum-checked tip.
const CHUNK_BLOCKS: u64 = 64;

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
        })
    }
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
            let end = next
                .saturating_add(CHUNK_BLOCKS - 1)
                .min(request.range.end().0);
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
        let frames = tokio::select! {
            () = cancellation.cancelled() => return Err(SourceError::Cancelled),
            frames = fetch_range(
                &self.quorum,
                &self.descriptor,
                chunk,
                FetchMode::from_byte(chunk.partition.first().copied().unwrap_or(u8::MAX)),
                false,
            ) => frames?,
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

/// Fetch, check, and link a chunk, then check its tip with the quorum: the
/// parent links extend the quorum's agreement to every block below it.
pub(crate) async fn fetch_range(
    quorum: &RpcQuorum,
    descriptor: &SourceDescriptor,
    chunk: &SourceChunk,
    mode: FetchMode,
    live: bool,
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
    match quorum.agreed_hash(tip.header.number).await? {
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
    }
    // Live blocks start unattested; the live source promotes them.
    let finalized = if live {
        None
    } else {
        quorum
            .agreed_tag("finalized")
            .await
            .ok()
            .map(|(number, _)| number)
    };
    blocks
        .iter()
        .map(|decoded| {
            let finality = match finalized {
                None if live => Finality::Unattested,
                Some(finalized) if decoded.header.number <= finalized => Finality::Finalized,
                _ => Finality::Attested,
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
