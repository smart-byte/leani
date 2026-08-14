//! Synthetic processors and in-memory reducer transaction.

use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use leani_primitives::{
    BlockFrame, BlockHash, Capability, CapabilitySet, FilterScope, Finality, ProcessorCursor,
};
use leani_processor_api::{
    ChangeOperation, DataRequirement, DeliveryOrdering, DomainChange, DomainChanges, EncodedDelta,
    LifecyclePolicies, OutputPolicyMode, Processor, ProcessorDescriptor, ProcessorError,
    ProcessorId, ProcessorInstanceId, ProcessorSchemas, PublicationPolicy, ReducerTransaction,
    ReductionMode, RetentionPolicy, StartPoint,
};
use semver::Version;

#[derive(Clone, Debug, Default)]
pub struct MemoryReducer {
    entities: BTreeMap<(String, Vec<u8>), Vec<u8>>,
    indexes: BTreeSet<(String, Vec<u8>, Vec<u8>)>,
    emitted: Vec<DomainChange>,
}

impl MemoryReducer {
    #[must_use]
    pub fn entity(&self, collection: &str, key: &[u8]) -> Option<&[u8]> {
        self.entities
            .get(&(collection.to_owned(), key.to_vec()))
            .map(Vec::as_slice)
    }

    #[must_use]
    pub fn emitted(&self) -> &[DomainChange] {
        &self.emitted
    }
}

#[async_trait]
impl ReducerTransaction for MemoryReducer {
    async fn get(
        &mut self,
        collection: &str,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, ProcessorError> {
        Ok(self
            .entities
            .get(&(collection.to_owned(), key.to_vec()))
            .cloned())
    }

    async fn put(
        &mut self,
        collection: &str,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), ProcessorError> {
        self.entities.insert((collection.to_owned(), key), value);
        Ok(())
    }

    async fn delete(&mut self, collection: &str, key: &[u8]) -> Result<(), ProcessorError> {
        self.entities.remove(&(collection.to_owned(), key.to_vec()));
        Ok(())
    }

    async fn index_put(
        &mut self,
        index: &str,
        index_key: Vec<u8>,
        entity_key: Vec<u8>,
    ) -> Result<(), ProcessorError> {
        self.indexes
            .insert((index.to_owned(), index_key, entity_key));
        Ok(())
    }

    async fn index_delete(
        &mut self,
        index: &str,
        index_key: &[u8],
        entity_key: &[u8],
    ) -> Result<(), ProcessorError> {
        self.indexes
            .remove(&(index.to_owned(), index_key.to_vec(), entity_key.to_vec()));
        Ok(())
    }

    async fn scan_prefix(
        &mut self,
        collection: &str,
        prefix: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ProcessorError> {
        Ok(self
            .entities
            .iter()
            .filter(|((stored_collection, key), _)| {
                stored_collection == collection && key.starts_with(prefix)
            })
            .take(limit)
            .map(|((_, key), value)| (key.clone(), value.clone()))
            .collect())
    }

    async fn emit(&mut self, change: DomainChange) -> Result<(), ProcessorError> {
        self.emitted.push(change);
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct BlockLocalCounter {
    descriptor: ProcessorDescriptor,
}

impl Default for BlockLocalCounter {
    fn default() -> Self {
        Self {
            descriptor: descriptor(
                "synthetic-counter",
                CapabilitySet::of(Capability::Transactions),
                ReductionMode::BlockLocal,
            ),
        }
    }
}

impl BlockLocalCounter {
    /// Construct an independent counter instance with the same material
    /// requirements.
    ///
    /// # Panics
    ///
    /// Panics when `id` is not a valid portable processor identifier.
    #[must_use]
    pub fn named(id: &str) -> Self {
        Self {
            descriptor: descriptor(
                id,
                CapabilitySet::of(Capability::Transactions),
                ReductionMode::BlockLocal,
            ),
        }
    }

    #[must_use]
    pub fn with_publication(mut self, publication: PublicationPolicy) -> Self {
        self.descriptor.publication = publication;
        self
    }

    #[must_use]
    pub fn with_lifecycle(mut self, lifecycle: LifecyclePolicies) -> Self {
        self.descriptor.lifecycle = lifecycle;
        self
    }

    #[must_use]
    pub fn with_split_delivery(mut self) -> Self {
        self.descriptor.delivery_ordering = DeliveryOrdering::BlockVersionedIdempotent;
        self
    }

    #[must_use]
    pub fn with_output_none(mut self) -> Self {
        self.descriptor.lifecycle.output.mode = OutputPolicyMode::None;
        self
    }

    #[must_use]
    pub fn with_delivery_none(mut self) -> Self {
        self.descriptor.lifecycle.delivery.mode = leani_processor_api::DeliveryPolicyMode::None;
        self.descriptor.lifecycle.delivery.consumers.clear();
        self
    }

    /// Override the live delivery hard limit for resource-boundary tests.
    #[must_use]
    pub const fn with_delivery_max_bytes(mut self, maximum_bytes: u64) -> Self {
        self.descriptor.lifecycle.delivery.max_bytes = maximum_bytes;
        self
    }
}

#[async_trait]
impl Processor for BlockLocalCounter {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn descriptor(&self) -> &ProcessorDescriptor {
        &self.descriptor
    }

    async fn map(&self, block: &BlockFrame) -> Result<EncodedDelta, ProcessorError> {
        validate_requirements(&self.descriptor, block)?;
        let count = block
            .transactions
            .as_complete()
            .ok_or_else(|| ProcessorError::Input("complete transactions required".to_owned()))?
            .len();
        let count = u64::try_from(count)
            .map_err(|_| ProcessorError::Invariant("transaction count exceeds u64".to_owned()))?;
        Ok(EncodedDelta::new(
            &self.descriptor,
            block.chain_id,
            block.block,
            count.to_be_bytes().to_vec(),
        ))
    }

    async fn reduce(
        &self,
        transaction: &mut dyn ReducerTransaction,
        cursor: &ProcessorCursor,
        delta: &EncodedDelta,
    ) -> Result<DomainChanges, ProcessorError> {
        validate_delta_cursor(&self.descriptor, cursor, delta)?;
        let count: [u8; 8] = delta
            .payload
            .as_slice()
            .try_into()
            .map_err(|_| ProcessorError::DeltaPayload("counter must be 8 bytes".to_owned()))?;
        let key = delta
            .block
            .canonical_key(delta.chain_id)
            .encode_ordered()
            .to_vec();
        transaction
            .put("counter.blocks", key.clone(), count.to_vec())
            .await?;
        let change = DomainChange {
            kind: "synthetic.counter".to_owned(),
            key,
            operation: ChangeOperation::Upsert,
            payload: count.to_vec(),
        };
        transaction.emit(change.clone()).await?;
        Ok(DomainChanges {
            changes: vec![change],
        })
    }
}

#[derive(Clone, Debug)]
pub struct OrderedLedgerProcessor {
    descriptor: ProcessorDescriptor,
}

impl Default for OrderedLedgerProcessor {
    fn default() -> Self {
        Self {
            descriptor: descriptor(
                "synthetic-ledger",
                CapabilitySet::of(Capability::Header),
                ReductionMode::OrderedState,
            ),
        }
    }
}

impl OrderedLedgerProcessor {
    /// Construct an independent ordered ledger with the same material shape.
    ///
    /// # Panics
    ///
    /// Panics when `id` is not a valid portable processor identifier.
    #[must_use]
    pub fn named(id: &str) -> Self {
        Self {
            descriptor: descriptor(
                id,
                CapabilitySet::of(Capability::Header),
                ReductionMode::OrderedState,
            ),
        }
    }

    /// Disable delivery while retaining the queryable ordered materialization.
    #[must_use]
    pub fn with_delivery_none(mut self) -> Self {
        self.descriptor.lifecycle.delivery.mode = leani_processor_api::DeliveryPolicyMode::None;
        self.descriptor.lifecycle.delivery.consumers.clear();
        self
    }
}

#[async_trait]
impl Processor for OrderedLedgerProcessor {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn descriptor(&self) -> &ProcessorDescriptor {
        &self.descriptor
    }

    async fn map(&self, block: &BlockFrame) -> Result<EncodedDelta, ProcessorError> {
        validate_requirements(&self.descriptor, block)?;
        Ok(EncodedDelta::new(
            &self.descriptor,
            block.chain_id,
            block.block,
            block.block.hash.0.to_vec(),
        ))
    }

    async fn reduce(
        &self,
        transaction: &mut dyn ReducerTransaction,
        cursor: &ProcessorCursor,
        delta: &EncodedDelta,
    ) -> Result<DomainChanges, ProcessorError> {
        validate_delta_cursor(&self.descriptor, cursor, delta)?;
        let block_hash: [u8; 32] =
            delta.payload.as_slice().try_into().map_err(|_| {
                ProcessorError::DeltaPayload("ledger hash must be 32 bytes".to_owned())
            })?;
        if let Some(last) = transaction.state_get("ledger", b"last-block").await? {
            let last: [u8; 8] = last
                .as_slice()
                .try_into()
                .map_err(|_| ProcessorError::State("last block is corrupt".to_owned()))?;
            let expected = u64::from_be_bytes(last).saturating_add(1);
            if delta.block.number.0 != expected {
                return Err(ProcessorError::Invariant(format!(
                    "ordered reducer expected block {expected}, received {}",
                    delta.block.number.0
                )));
            }
        }
        let previous = transaction
            .state_get("ledger", b"digest")
            .await?
            .unwrap_or_else(|| vec![0; 32]);
        if previous.len() != 32 {
            return Err(ProcessorError::State(
                "ledger digest has an invalid length".to_owned(),
            ));
        }
        let mut hasher = blake3::Hasher::new();
        hasher.update(&previous);
        hasher.update(&block_hash);
        let digest = hasher.finalize().as_bytes().to_vec();
        transaction
            .state_put("ledger", b"digest".to_vec(), digest.clone())
            .await?;
        transaction
            .state_put(
                "ledger",
                b"last-block".to_vec(),
                delta.block.number.0.to_be_bytes().to_vec(),
            )
            .await?;
        transaction
            .put("ledger", b"digest".to_vec(), digest.clone())
            .await?;
        let change = DomainChange {
            kind: "synthetic.ledger".to_owned(),
            key: b"digest".to_vec(),
            operation: ChangeOperation::Upsert,
            payload: digest,
        };
        transaction.emit(change.clone()).await?;
        Ok(DomainChanges {
            changes: vec![change],
        })
    }
}

fn descriptor(id: &str, capabilities: CapabilitySet, mode: ReductionMode) -> ProcessorDescriptor {
    let id = ProcessorId::new(id).expect("synthetic processor ID");
    let schema_prefix = id.to_string();
    let version = Version::new(1, 0, 0);
    let config_hash = BlockHash::new([0x22; 32]);
    ProcessorDescriptor {
        instance: ProcessorInstanceId::legacy(&id, &version, config_hash),
        id,
        version,
        code_hash: BlockHash::new([0x11; 32]),
        config_hash,
        start: StartPoint::Genesis,
        requirements: vec![DataRequirement {
            capabilities,
            log_fields: leani_primitives::LogFieldSet::NONE,
            allow_filtered: false,
            filter: FilterScope::default(),
            minimum_finality: Finality::Optimistic,
        }],
        mode,
        delivery_ordering: if mode == ReductionMode::BlockLocal {
            DeliveryOrdering::BlockVersionedIdempotent
        } else {
            DeliveryOrdering::Canonical
        },
        publication: PublicationPolicy::OptimisticAndFinalized,
        lifecycle: LifecyclePolicies::from_legacy(RetentionPolicy::FullOutputHistory),
        schemas: ProcessorSchemas {
            delta_version: 1,
            entity_schema: format!("{schema_prefix}.entity.v1"),
            change_schema: format!("{schema_prefix}.change.v1"),
        },
    }
}

fn validate_requirements(
    descriptor: &ProcessorDescriptor,
    frame: &BlockFrame,
) -> Result<(), ProcessorError> {
    for requirement in &descriptor.requirements {
        requirement
            .validate_frame(frame)
            .map_err(|error| ProcessorError::Input(error.to_owned()))?;
    }
    Ok(())
}

fn validate_delta_cursor(
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
    use leani_primitives::{HeaderEnvelope, Material, TransactionEnvelope, TransactionHash};

    use crate::fixture_frame;

    use super::*;

    fn cursor(processor: &dyn Processor, block: &BlockFrame, sequence: u64) -> ProcessorCursor {
        ProcessorCursor {
            processor_id: processor.descriptor().id.to_string(),
            processor_version: processor.descriptor().version.to_string(),
            chain_id: block.chain_id,
            block_number: block.block.number,
            block_hash: block.block.hash,
            finality: block.finality,
            sequence,
        }
    }

    #[tokio::test]
    async fn block_local_counter_is_deterministic() {
        let processor = BlockLocalCounter::default();
        let mut frame = fixture_frame(1, BlockHash::ZERO);
        frame.transactions = Material::Complete(vec![
            TransactionEnvelope {
                hash: TransactionHash::new([1; 32]),
                transaction_type: 2,
                index: 0,
                encoded: Some(vec![1]),
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
            },
            TransactionEnvelope {
                hash: TransactionHash::new([2; 32]),
                transaction_type: 3,
                index: 1,
                encoded: Some(vec![2]),
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
            },
        ]);
        let first = processor.map(&frame).await.expect("map");
        let second = processor.map(&frame).await.expect("map");
        assert_eq!(first, second);
        let mut reducer = MemoryReducer::default();
        let changes = processor
            .reduce(&mut reducer, &cursor(&processor, &frame, 1), &first)
            .await
            .expect("reduce");
        assert_eq!(changes.changes[0].payload, 2_u64.to_be_bytes());
    }

    #[tokio::test]
    async fn ordered_ledger_rejects_out_of_order_reduction() {
        let processor = OrderedLedgerProcessor::default();
        let mut first = fixture_frame(10, BlockHash::new([9; 32]));
        first.header = Material::Complete(HeaderEnvelope {
            rlp: Some(vec![10]),
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
        let mut second = fixture_frame(11, first.block.hash);
        second.header = first.header.clone();
        let first_delta = processor.map(&first).await.expect("map first");
        let second_delta = processor.map(&second).await.expect("map second");
        let mut reducer = MemoryReducer::default();
        processor
            .reduce(&mut reducer, &cursor(&processor, &first, 1), &first_delta)
            .await
            .expect("reduce first");
        processor
            .reduce(&mut reducer, &cursor(&processor, &second, 2), &second_delta)
            .await
            .expect("reduce second");

        let out_of_order = fixture_frame(13, second.block.hash);
        let mut out_of_order = out_of_order;
        out_of_order.header = first.header;
        let delta = processor.map(&out_of_order).await.expect("map");
        let error = processor
            .reduce(&mut reducer, &cursor(&processor, &out_of_order, 3), &delta)
            .await
            .expect_err("gap rejected");
        assert!(matches!(error, ProcessorError::Invariant(_)));
    }
}
