//! Ethereum block summaries from verified headers and bodies.

use alloy_primitives::U256;
use async_trait::async_trait;
use leani_primitives::{
    BlockFrame, BlockHash, BlockNumber, Capability, CapabilitySet, ChainId, FilterScope, Finality,
    Material, ProcessorCursor, Quantity,
};
use leani_processor_api::{
    ChangeOperation, DataRequirement, DeliveryOrdering, DomainChange, DomainChanges, EncodedDelta,
    LifecyclePolicies, Processor, ProcessorDescriptor, ProcessorError, ProcessorId,
    ProcessorInstanceId, ProcessorSchemas, PublicationPolicy, ReducerTransaction, ReductionMode,
    RetentionPolicy, StartPoint,
};
use semver::Version;
use serde::{Deserialize, Serialize};

pub const BLOCK_COLLECTION: &str = "ethereum.blocks";
pub const BLOCK_NUMBER_INDEX_COLLECTION: &str = "ethereum.blocks.by-number";
pub const BLOCK_SUMMARY_KIND: &str = "ethereum.block.summary";
pub const BLOCK_SUMMARY_VERSION: &str = "1.1.0";

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BlockSummaryConfig {
    pub start_block: BlockNumber,
}

/// Stable, source-neutral fields available from a verified execution block.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockSummaryEntity {
    pub chain_id: ChainId,
    pub block_number: BlockNumber,
    pub block_hash: BlockHash,
    pub parent_hash: BlockHash,
    pub timestamp: u64,
    pub gas_limit: Option<u64>,
    pub gas_used: Option<u64>,
    pub base_fee_per_gas: Option<Quantity>,
    pub blob_gas_used: Option<u64>,
    pub excess_blob_gas: Option<u64>,
    pub transaction_count: Option<u32>,
    pub size_bytes: Option<u64>,
    pub finality: Finality,
}

#[derive(Clone, Debug)]
pub struct BlockSummaryProcessor {
    descriptor: ProcessorDescriptor,
}

impl BlockSummaryProcessor {
    /// Build a block summary processor that avoids receipt acquisition.
    ///
    /// # Errors
    ///
    /// Returns an error if the immutable processor identity cannot be encoded.
    pub fn new(config: BlockSummaryConfig) -> Result<Self, ProcessorError> {
        let encoded = postcard::to_allocvec(&config)
            .map_err(|error| ProcessorError::Input(error.to_string()))?;
        let id = ProcessorId::new("block-summary")
            .map_err(|error| ProcessorError::Input(error.to_string()))?;
        let version = Version::new(1, 1, 0);
        let config_hash = BlockHash::new(*blake3::hash(&encoded).as_bytes());
        let descriptor = ProcessorDescriptor {
            instance: ProcessorInstanceId::legacy(&id, &version, config_hash),
            id,
            version,
            code_hash: BlockHash::new(
                *blake3::hash(format!("leani/block-summary/{BLOCK_SUMMARY_VERSION}").as_bytes())
                    .as_bytes(),
            ),
            config_hash,
            start: StartPoint::Block(config.start_block),
            requirements: vec![DataRequirement {
                capabilities: CapabilitySet::of(Capability::Header).with(Capability::Body),
                log_fields: leani_primitives::LogFieldSet::NONE,
                allow_filtered: false,
                filter: FilterScope::default(),
                minimum_finality: Finality::Optimistic,
            }],
            mode: ReductionMode::BlockLocal,
            delivery_ordering: DeliveryOrdering::BlockVersionedIdempotent,
            publication: PublicationPolicy::OptimisticAndFinalized,
            lifecycle: LifecyclePolicies::from_legacy(RetentionPolicy::FullOutputHistory),
            schemas: ProcessorSchemas {
                delta_version: 1,
                entity_schema: "ethereum.block-summary.entity.v1".to_owned(),
                change_schema: "ethereum.block-summary.change.v1".to_owned(),
            },
        };
        descriptor
            .validate()
            .map_err(|error| ProcessorError::Input(error.to_owned()))?;
        Ok(Self { descriptor })
    }

    #[must_use]
    pub fn with_contract(
        mut self,
        instance: ProcessorInstanceId,
        publication: PublicationPolicy,
        lifecycle: LifecyclePolicies,
    ) -> Self {
        self.descriptor.instance = instance;
        self.descriptor.publication = publication;
        self.descriptor.lifecycle = lifecycle;
        self
    }
}

#[async_trait]
impl Processor for BlockSummaryProcessor {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn descriptor(&self) -> &ProcessorDescriptor {
        &self.descriptor
    }

    async fn map(&self, block: &BlockFrame) -> Result<EncodedDelta, ProcessorError> {
        self.descriptor.requirements[0]
            .validate_frame(block)
            .map_err(|error| ProcessorError::Input(error.to_owned()))?;
        let header = match &block.header {
            Material::Complete(header) => header,
            Material::Filtered { .. } => {
                return Err(ProcessorError::Input(
                    "execution header projection is filtered".to_owned(),
                ));
            }
            Material::Missing(reason) => {
                return Err(ProcessorError::Input(format!(
                    "execution header is missing: {reason:?}"
                )));
            }
        };
        let transaction_count = block
            .transactions
            .as_complete()
            .and_then(|transactions| u32::try_from(transactions.len()).ok())
            .ok_or_else(|| {
                ProcessorError::Input(
                    "complete execution body transaction count is unavailable".to_owned(),
                )
            })?;
        if let Some(header_count) = header.transaction_count
            && header_count != transaction_count
        {
            return Err(ProcessorError::Input(format!(
                "execution header transaction count {header_count} differs from decoded body count {transaction_count}"
            )));
        }
        let entity = BlockSummaryEntity {
            chain_id: block.chain_id,
            block_number: block.block.number,
            block_hash: block.block.hash,
            parent_hash: block.block.parent_hash,
            timestamp: block.block.timestamp,
            gas_limit: header.gas_limit,
            gas_used: header.gas_used,
            base_fee_per_gas: header.base_fee_per_gas,
            blob_gas_used: header.blob_gas_used,
            excess_blob_gas: header.excess_blob_gas,
            transaction_count: Some(transaction_count),
            size_bytes: header.size_bytes,
            finality: block.finality,
        };
        let payload = postcard::to_allocvec(&entity)
            .map_err(|error| ProcessorError::DeltaPayload(error.to_string()))?;
        Ok(EncodedDelta::new(
            &self.descriptor,
            block.chain_id,
            block.block,
            payload,
        ))
    }

    fn finality_variant_checksums(
        &self,
        delta: &EncodedDelta,
    ) -> Result<Vec<BlockHash>, ProcessorError> {
        delta.validate(&self.descriptor)?;
        let decoded: BlockSummaryEntity = postcard::from_bytes(&delta.payload)
            .map_err(|error| ProcessorError::DeltaPayload(error.to_string()))?;
        let mut checksums = Vec::with_capacity(3);
        for finality in [Finality::Optimistic, Finality::Safe, Finality::Finalized] {
            let mut entity = decoded.clone();
            entity.finality = finality;
            let payload = postcard::to_allocvec(&entity)
                .map_err(|error| ProcessorError::DeltaPayload(error.to_string()))?;
            checksums.push(
                EncodedDelta::new(&self.descriptor, delta.chain_id, delta.block, payload).checksum,
            );
        }
        checksums.sort_unstable();
        checksums.dedup();
        Ok(checksums)
    }

    async fn reduce(
        &self,
        transaction: &mut dyn ReducerTransaction,
        cursor: &ProcessorCursor,
        delta: &EncodedDelta,
    ) -> Result<DomainChanges, ProcessorError> {
        validate_cursor(&self.descriptor, cursor, delta)?;
        let entity: BlockSummaryEntity = postcard::from_bytes(&delta.payload)
            .map_err(|error| ProcessorError::DeltaPayload(error.to_string()))?;
        let key = entity.block_hash.0.to_vec();
        let payload = postcard::to_allocvec(&entity)
            .map_err(|error| ProcessorError::State(error.to_string()))?;
        transaction
            .put(BLOCK_COLLECTION, key.clone(), payload.clone())
            .await?;
        transaction
            .put(
                BLOCK_NUMBER_INDEX_COLLECTION,
                entity.block_number.0.to_be_bytes().to_vec(),
                entity.block_hash.0.to_vec(),
            )
            .await?;
        let change = DomainChange {
            kind: BLOCK_SUMMARY_KIND.to_owned(),
            key,
            operation: ChangeOperation::Upsert,
            payload,
        };
        transaction.emit(change.clone()).await?;
        Ok(DomainChanges {
            changes: vec![change],
        })
    }

    fn change_json(
        &self,
        change: &DomainChange,
    ) -> Result<Option<serde_json::Value>, ProcessorError> {
        if change.kind != BLOCK_SUMMARY_KIND {
            return Ok(None);
        }
        entity_json(&change.payload).map(Some)
    }

    fn entity_json(
        &self,
        collection: &str,
        _key: &[u8],
        value: &[u8],
    ) -> Result<Option<serde_json::Value>, ProcessorError> {
        if collection != BLOCK_COLLECTION {
            return Ok(None);
        }
        entity_json(value).map(Some)
    }
}

fn entity_json(payload: &[u8]) -> Result<serde_json::Value, ProcessorError> {
    let entity: BlockSummaryEntity =
        postcard::from_bytes(payload).map_err(|error| ProcessorError::State(error.to_string()))?;
    Ok(serde_json::json!({
        "chainId": entity.chain_id.0,
        "blockNumber": entity.block_number.0,
        "blockHash": entity.block_hash.to_string(),
        "parentHash": entity.parent_hash.to_string(),
        "timestamp": entity.timestamp,
        "gasLimit": entity.gas_limit,
        "gasUsed": entity.gas_used,
        "baseFeePerGas": entity
            .base_fee_per_gas
            .map(|value| U256::from_be_bytes(value.0).to_string()),
        "blobGasUsed": entity.blob_gas_used,
        "excessBlobGas": entity.excess_blob_gas,
        "transactionCount": entity.transaction_count,
        "sizeBytes": entity.size_bytes,
        "finality": entity.finality.name(),
    }))
}

fn validate_cursor(
    descriptor: &ProcessorDescriptor,
    cursor: &ProcessorCursor,
    delta: &EncodedDelta,
) -> Result<(), ProcessorError> {
    delta.validate(descriptor)?;
    if cursor.processor_id != descriptor.id.as_str()
        || cursor.processor_version != descriptor.version.to_string()
        || cursor.chain_id != delta.chain_id
        || cursor.block_number != delta.block.number
        || cursor.block_hash != delta.block.hash
    {
        return Err(ProcessorError::CursorMismatch);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use leani_primitives::{BlockRef, HeaderEnvelope, TransactionEnvelope, TransactionHash};
    use leani_testkit::{MemoryReducer, fixture_frame};

    use super::*;

    fn processor() -> BlockSummaryProcessor {
        BlockSummaryProcessor::new(BlockSummaryConfig {
            start_block: BlockNumber(0),
        })
        .expect("processor")
    }

    fn frame() -> BlockFrame {
        let mut frame = fixture_frame(42, BlockHash::new([41; 32]));
        frame.finality = Finality::Optimistic;
        frame.transactions = Material::Complete(
            (0..2_u32)
                .map(|index| TransactionEnvelope {
                    hash: TransactionHash::new([u8::try_from(index).expect("small index"); 32]),
                    transaction_type: 2,
                    index,
                    encoded: Some(vec![u8::try_from(index).expect("small index")]),
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
                    size_bytes: Some(1),
                })
                .collect(),
        );
        frame.header = Material::Complete(HeaderEnvelope {
            rlp: None,
            transactions_root: Some(BlockHash::new([2; 32])),
            receipts_root: Some(BlockHash::new([3; 32])),
            withdrawals_root: Some(BlockHash::new([4; 32])),
            gas_limit: Some(60_000_000),
            gas_used: Some(30_000_000),
            base_fee_per_gas: Some(Quantity::new([5; 32])),
            blob_gas_used: Some(393_216),
            excess_blob_gas: Some(786_432),
            size_bytes: None,
            transaction_count: Some(2),
            consensus_size_bytes: None,
        });
        frame
    }

    fn cursor(processor: &BlockSummaryProcessor, frame: &BlockFrame) -> ProcessorCursor {
        ProcessorCursor {
            processor_id: processor.descriptor.id.to_string(),
            processor_version: processor.descriptor.version.to_string(),
            chain_id: frame.chain_id,
            block_number: frame.block.number,
            block_hash: frame.block.hash,
            finality: frame.finality,
            sequence: frame.block.number.0,
        }
    }

    #[tokio::test]
    async fn maps_and_emits_one_summary_from_verified_block_material() {
        let processor = processor();
        let frame = frame();
        let delta = processor.map(&frame).await.expect("map");
        let entity: BlockSummaryEntity =
            postcard::from_bytes(&delta.payload).expect("summary payload");
        assert_eq!(entity.block_number, BlockNumber(42));
        assert_eq!(entity.gas_used, Some(30_000_000));
        assert_eq!(entity.transaction_count, Some(2));

        let mut reducer = MemoryReducer::default();
        let changes = processor
            .reduce(&mut reducer, &cursor(&processor, &frame), &delta)
            .await
            .expect("reduce");
        assert_eq!(changes.changes.len(), 1);
        assert_eq!(changes.changes[0].kind, BLOCK_SUMMARY_KIND);
        let json = processor
            .change_json(&changes.changes[0])
            .expect("change JSON")
            .expect("owned JSON");
        assert_eq!(json["blockNumber"], 42);
        assert_eq!(json["finality"], "optimistic");
        assert_eq!(json["blockHash"], frame.block.hash.to_string());
        assert!(
            reducer
                .entity(BLOCK_COLLECTION, &frame.block.hash.0)
                .is_some()
        );
        assert_eq!(
            reducer
                .entity(
                    BLOCK_NUMBER_INDEX_COLLECTION,
                    &frame.block.number.0.to_be_bytes(),
                )
                .expect("number index"),
            frame.block.hash.0
        );
    }

    #[tokio::test]
    async fn finality_is_the_only_accepted_checksum_variant() {
        let processor = processor();
        let frame = frame();
        let delta = processor.map(&frame).await.expect("map");
        let checksums = processor
            .finality_variant_checksums(&delta)
            .expect("variants");
        assert_eq!(checksums.len(), 3);

        let unrelated = EncodedDelta::new(
            processor.descriptor(),
            ChainId(1),
            BlockRef {
                timestamp: frame.block.timestamp + 1,
                ..frame.block
            },
            delta.payload,
        );
        assert!(!checksums.contains(&unrelated.checksum));
    }

    #[tokio::test]
    async fn block_summaries_are_retained_by_hash_without_a_hot_latest_key() {
        let processor = processor();
        let newest = frame();
        let mut older = frame();
        older.block.number = BlockNumber(41);
        older.block.hash = BlockHash::new([40; 32]);
        older.finality = Finality::Finalized;
        let newest_delta = processor.map(&newest).await.expect("newest map");
        let older_delta = processor.map(&older).await.expect("older map");
        let mut reducer = MemoryReducer::default();
        processor
            .reduce(&mut reducer, &cursor(&processor, &newest), &newest_delta)
            .await
            .expect("newest reduce");
        processor
            .reduce(&mut reducer, &cursor(&processor, &older), &older_delta)
            .await
            .expect("older reduce");

        assert!(
            reducer
                .entity(BLOCK_COLLECTION, &newest.block.hash.0)
                .is_some()
        );
        assert!(
            reducer
                .entity(BLOCK_COLLECTION, &older.block.hash.0)
                .is_some()
        );
    }
}
