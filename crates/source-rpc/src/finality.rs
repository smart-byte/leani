use std::time::Duration;

use async_trait::async_trait;
use futures::{StreamExt, stream};
use leani_primitives::{
    BlockHash, BlockNumber, Capability, CapabilitySet, ChainId, SourceId, SourceKind, TrustModel,
};
use leani_source_api::{
    ConsensusCheckpoint, FinalityEvent, FinalityEventStream, FinalityModel, FinalitySource,
    Partitioning, SourceDescriptor, SourceError,
};
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::RpcQuorum;

/// The block a quorum of endpoints reports as `finalized`. The contract's
/// beacon fields carry the block number as slot and a zero root.
#[derive(Clone, Debug)]
pub struct RpcFinalitySource {
    descriptor: SourceDescriptor,
    quorum: RpcQuorum,
    poll_interval: Duration,
}

impl RpcFinalitySource {
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
                capabilities: CapabilitySet::of(Capability::ConsensusFinality),
                complete_capabilities: CapabilitySet::of(Capability::ConsensusFinality),
                trust: TrustModel::TrustedDataset,
                finality: FinalityModel::Finalized,
                partitioning: Partitioning::None,
                expected_lag: poll_interval,
                schema_version: "json-rpc.v1".to_owned(),
                priority: 0,
            },
            quorum,
            poll_interval,
        })
    }

    /// The quorum's finalized block, as the checkpoint a finality runtime
    /// starts from.
    ///
    /// # Errors
    ///
    /// Returns an error when no quorum forms.
    pub async fn checkpoint(&self) -> Result<ConsensusCheckpoint, SourceError> {
        let (number, hash) = self.quorum.agreed_tag("finalized").await?;
        Ok(ConsensusCheckpoint {
            beacon_slot: number,
            beacon_block_root: [0; 32],
            execution_block_hash: BlockHash::new(hash.0),
            obtained_at_unix_seconds: crate::unix_now_ms() / 1_000,
            source: format!("json-rpc quorum {}", self.quorum.quorum()),
        })
    }
}

#[async_trait]
impl FinalitySource for RpcFinalitySource {
    fn descriptor(&self) -> &SourceDescriptor {
        &self.descriptor
    }

    async fn subscribe(
        &self,
        checkpoint: ConsensusCheckpoint,
        cancellation: CancellationToken,
    ) -> Result<FinalityEventStream, SourceError> {
        let state = Polling {
            source: self.clone(),
            last: checkpoint.beacon_slot,
            emitted: false,
            cancellation,
        };
        Ok(stream::unfold(state, next_finalized).boxed())
    }
}

struct Polling {
    source: RpcFinalitySource,
    last: u64,
    emitted: bool,
    cancellation: CancellationToken,
}

async fn next_finalized(
    mut state: Polling,
) -> Option<(Result<FinalityEvent, SourceError>, Polling)> {
    loop {
        if state.emitted {
            tokio::select! {
                () = state.cancellation.cancelled() => return None,
                () = tokio::time::sleep(state.source.poll_interval) => {}
            }
        }
        if state.cancellation.is_cancelled() {
            return None;
        }
        match state.source.quorum.agreed_tag("finalized").await {
            Ok((number, hash)) if number > state.last || !state.emitted => {
                state.last = number;
                state.emitted = true;
                let event = FinalityEvent::Finalized {
                    block_number: BlockNumber(number),
                    block_hash: BlockHash::new(hash.0),
                    beacon_slot: number,
                    beacon_block_root: [0; 32],
                };
                return Some((Ok(event), state));
            }
            Ok(_) => {}
            Err(error) => {
                debug!(%error, "json-rpc finality poll failed; retrying");
                if !state.emitted {
                    tokio::select! {
                        () = state.cancellation.cancelled() => return None,
                        () = tokio::time::sleep(state.source.poll_interval) => {}
                    }
                }
            }
        }
    }
}
