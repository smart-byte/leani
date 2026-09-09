//! Constant-size ordered aggregate for transactions from one address to another.

use alloy_primitives::U256;
use async_trait::async_trait;
use leani_primitives::{
    Address, BlockFrame, BlockHash, BlockNumber, Capability, CapabilitySet, Completeness,
    FilterScope, Finality, Material, ProcessorCursor, Quantity,
};
use leani_processor_api::{
    ChangeOperation, DataRequirement, DeliveryOrdering, DomainChange, DomainChanges, EncodedDelta,
    LifecyclePolicies, Processor, ProcessorDescriptor, ProcessorError, ProcessorId,
    ProcessorInstanceId, ProcessorSchemas, PublicationPolicy, ReducerTransaction, ReductionMode,
    RetentionPolicy, StartPoint,
};
use semver::Version;
use serde::{Deserialize, Serialize};

pub const AGGREGATE_COLLECTION: &str = "transaction_stats.aggregate";
pub const AGGREGATE_KIND: &str = "transaction_stats.aggregate";
const AGGREGATE_KEY: &[u8] = b"total";

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TransactionStatsConfig {
    pub start_block: BlockNumber,
    pub from: Address,
    pub to: Address,
    pub complete_from_start: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TransactionStatsEntity {
    pub from: Address,
    pub to: Address,
    pub count: u64,
    pub total_value: Quantity,
    pub as_of_block: BlockNumber,
    pub block_hash: BlockHash,
    pub finality: Finality,
    pub coverage_from: BlockNumber,
    pub complete: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct BlockContribution {
    count: u64,
    total_value: Quantity,
}

#[derive(Clone, Debug)]
pub struct TransactionStatsProcessor {
    config: TransactionStatsConfig,
    descriptor: ProcessorDescriptor,
}

impl TransactionStatsProcessor {
    /// Create a constant-size ordered aggregate processor.
    ///
    /// # Errors
    ///
    /// Returns an error when source and destination are identical or the
    /// immutable config cannot be encoded.
    pub fn new(config: TransactionStatsConfig) -> Result<Self, ProcessorError> {
        if config.from == config.to {
            return Err(ProcessorError::Input(
                "transaction aggregate source and destination must differ".to_owned(),
            ));
        }
        let encoded = postcard::to_allocvec(&config)
            .map_err(|error| ProcessorError::Input(error.to_string()))?;
        let id = ProcessorId::new("transaction-stats")
            .map_err(|error| ProcessorError::Input(error.to_string()))?;
        let version = Version::new(1, 0, 0);
        let config_hash = BlockHash::new(*blake3::hash(&encoded).as_bytes());
        let descriptor = ProcessorDescriptor {
            instance: ProcessorInstanceId::legacy(&id, &version, config_hash),
            id,
            version,
            code_hash: BlockHash::new(*blake3::hash(b"leani/transaction-stats/1.0.0").as_bytes()),
            config_hash,
            start: StartPoint::Block(config.start_block),
            requirements: vec![DataRequirement {
                capabilities: CapabilitySet::of(Capability::Transactions),
                log_fields: leani_primitives::LogFieldSet::NONE,
                allow_filtered: true,
                filter: FilterScope {
                    senders: vec![config.from],
                    recipients: vec![config.to],
                    ..FilterScope::default()
                },
                minimum_finality: Finality::Included,
            }],
            mode: ReductionMode::OrderedState,
            delivery_ordering: DeliveryOrdering::Canonical,
            publication: PublicationPolicy::IncludedAndFinalized,
            lifecycle: LifecyclePolicies::from_legacy(RetentionPolicy::LatestState),
            schemas: ProcessorSchemas {
                delta_version: 1,
                entity_schema: "transaction-stats.entity.v1".to_owned(),
                change_schema: "transaction-stats.change.v1".to_owned(),
            },
        };
        Ok(Self { config, descriptor })
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
impl Processor for TransactionStatsProcessor {
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
        let transactions = accepted_transactions(&block.transactions)?;
        let mut count = 0_u64;
        let mut total = U256::ZERO;
        for transaction in transactions {
            let from = transaction.from.ok_or_else(|| {
                ProcessorError::Input(
                    "transaction sender is required by transaction-stats".to_owned(),
                )
            })?;
            if from != self.config.from || transaction.to != Some(self.config.to) {
                continue;
            }
            let value = transaction.value.ok_or_else(|| {
                ProcessorError::Input(
                    "transaction value is required by transaction-stats".to_owned(),
                )
            })?;
            count = count.checked_add(1).ok_or_else(|| {
                ProcessorError::Invariant("transaction count overflow".to_owned())
            })?;
            total = total
                .checked_add(U256::from_be_bytes(value.0))
                .ok_or_else(|| {
                    ProcessorError::Invariant("transaction value overflow".to_owned())
                })?;
        }
        let payload = postcard::to_allocvec(&BlockContribution {
            count,
            total_value: total.into(),
        })
        .map_err(|error| ProcessorError::DeltaPayload(error.to_string()))?;
        Ok(EncodedDelta::new(
            &self.descriptor,
            block.chain_id,
            block.block,
            payload,
        ))
    }

    async fn reduce(
        &self,
        transaction: &mut dyn ReducerTransaction,
        cursor: &ProcessorCursor,
        delta: &EncodedDelta,
    ) -> Result<DomainChanges, ProcessorError> {
        validate_cursor(&self.descriptor, cursor, delta)?;
        let contribution: BlockContribution = postcard::from_bytes(&delta.payload)
            .map_err(|error| ProcessorError::DeltaPayload(error.to_string()))?;
        let prior = transaction
            .state_get(AGGREGATE_COLLECTION, AGGREGATE_KEY)
            .await?
            .as_deref()
            .map(decode_entity)
            .transpose()?;
        let count = prior
            .as_ref()
            .map_or(0, |entity| entity.count)
            .checked_add(contribution.count)
            .ok_or_else(|| ProcessorError::Invariant("transaction count overflow".to_owned()))?;
        let total = prior
            .as_ref()
            .map_or(U256::ZERO, |entity| {
                U256::from_be_bytes(entity.total_value.0)
            })
            .checked_add(U256::from_be_bytes(contribution.total_value.0))
            .ok_or_else(|| ProcessorError::Invariant("transaction value overflow".to_owned()))?;
        let entity = TransactionStatsEntity {
            from: self.config.from,
            to: self.config.to,
            count,
            total_value: total.into(),
            as_of_block: cursor.block_number,
            block_hash: cursor.block_hash,
            finality: cursor.finality,
            coverage_from: self.config.start_block,
            complete: self.config.complete_from_start,
        };
        let payload = postcard::to_allocvec(&entity)
            .map_err(|error| ProcessorError::State(error.to_string()))?;
        transaction
            .state_put(
                AGGREGATE_COLLECTION,
                AGGREGATE_KEY.to_vec(),
                payload.clone(),
            )
            .await?;
        transaction
            .put(
                AGGREGATE_COLLECTION,
                AGGREGATE_KEY.to_vec(),
                payload.clone(),
            )
            .await?;
        let change = DomainChange {
            kind: AGGREGATE_KIND.to_owned(),
            key: AGGREGATE_KEY.to_vec(),
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
        entity_json(&change.payload).map(Some)
    }

    fn entity_json(
        &self,
        collection: &str,
        _key: &[u8],
        value: &[u8],
    ) -> Result<Option<serde_json::Value>, ProcessorError> {
        if collection != AGGREGATE_COLLECTION {
            return Ok(None);
        }
        entity_json(value).map(Some)
    }
}

fn accepted_transactions(
    transactions: &Material<Vec<leani_primitives::TransactionEnvelope>>,
) -> Result<&[leani_primitives::TransactionEnvelope], ProcessorError> {
    match transactions {
        Material::Complete(transactions)
        | Material::Filtered {
            value: transactions,
            completeness: Completeness::VerifiedPredicate | Completeness::DatasetDeclared,
            ..
        } => Ok(transactions),
        Material::Filtered {
            completeness: Completeness::Partial,
            ..
        } => Err(ProcessorError::Input(
            "transaction projection is partial".to_owned(),
        )),
        Material::Missing(reason) => Err(ProcessorError::Input(format!(
            "transactions are missing: {reason:?}"
        ))),
    }
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

fn decode_entity(bytes: &[u8]) -> Result<TransactionStatsEntity, ProcessorError> {
    postcard::from_bytes(bytes).map_err(|error| ProcessorError::State(error.to_string()))
}

fn entity_json(bytes: &[u8]) -> Result<serde_json::Value, ProcessorError> {
    let entity = decode_entity(bytes)?;
    Ok(serde_json::json!({
        "from": entity.from.to_string(),
        "to": entity.to.to_string(),
        "count": entity.count,
        "totalValueWei": U256::from_be_bytes(entity.total_value.0).to_string(),
        "asOfBlock": entity.as_of_block.0,
        "blockHash": entity.block_hash.to_string(),
        "finality": entity.finality.name(),
        "coverageFrom": entity.coverage_from.0,
        "complete": entity.complete,
    }))
}

#[cfg(test)]
mod tests {
    use leani_primitives::{
        BlockRef, ChainId, Material, TransactionEnvelope, TransactionHash, VerificationReport,
    };
    use leani_testkit::MemoryReducer;

    use super::*;

    #[test]
    fn entity_json_renders_camel_case_hex_public_shape() {
        let mut total = [0_u8; 32];
        total[31] = 0x2a; // 42 wei
        let entity = TransactionStatsEntity {
            from: Address::new([0x11; 20]),
            to: Address::new([0x22; 20]),
            count: 18_421,
            total_value: Quantity::new(total),
            as_of_block: BlockNumber(25_696_396),
            block_hash: BlockHash::new([0xab; 32]),
            finality: Finality::Finalized,
            coverage_from: BlockNumber(15_537_394),
            complete: true,
        };
        let encoded = postcard::to_allocvec(&entity).expect("encode");
        let json = entity_json(&encoded).expect("render");
        assert_eq!(json["from"], format!("0x{}", "11".repeat(20)));
        assert_eq!(json["to"], format!("0x{}", "22".repeat(20)));
        assert_eq!(json["count"], 18_421);
        assert_eq!(json["totalValueWei"], "42");
        assert_eq!(json["asOfBlock"], 25_696_396);
        assert_eq!(json["blockHash"], format!("0x{}", "ab".repeat(32)));
        assert_eq!(json["finality"], "finalized");
        assert_eq!(json["coverageFrom"], 15_537_394);
        assert_eq!(json["complete"], true);
        let object = json.as_object().expect("object");
        assert!(!object.contains_key("total_value"), "snake_case leaked");
    }

    fn processor() -> TransactionStatsProcessor {
        TransactionStatsProcessor::new(TransactionStatsConfig {
            start_block: BlockNumber(1),
            from: Address::new([1; 20]),
            to: Address::new([2; 20]),
            complete_from_start: true,
        })
        .expect("processor")
    }

    fn frame(number: u64, values: &[u64]) -> BlockFrame {
        let transactions = values
            .iter()
            .enumerate()
            .map(|(index, value)| TransactionEnvelope {
                hash: TransactionHash::new([u8::try_from(index + 1).unwrap_or(0); 32]),
                transaction_type: 2,
                index: u32::try_from(index).expect("index"),
                encoded: None,
                from: Some(Address::new([1; 20])),
                to: Some(Address::new([2; 20])),
                nonce: Some(u64::try_from(index).expect("nonce")),
                gas_limit: Some(21_000),
                value: Some(Quantity::from(U256::from(*value))),
                input: Some(Vec::new()),
                max_fee_per_gas: None,
                max_priority_fee_per_gas: None,
                max_fee_per_blob_gas: None,
                blob_versioned_hashes: Vec::new(),
                size_bytes: None,
            })
            .collect();
        BlockFrame {
            chain_id: ChainId(1),
            block: BlockRef {
                number: BlockNumber(number),
                hash: BlockHash::new([u8::try_from(number).unwrap_or(0); 32]),
                parent_hash: BlockHash::new(
                    [u8::try_from(number.saturating_sub(1)).unwrap_or(0); 32],
                ),
                timestamp: number,
            },
            finality: Finality::Included,
            header: Material::Missing(leani_primitives::MissingReason::NotRequested),
            transactions: Material::Complete(transactions),
            receipts: Material::Missing(leani_primitives::MissingReason::NotRequested),
            logs: Material::Missing(leani_primitives::MissingReason::NotRequested),
            withdrawals: Material::Missing(leani_primitives::MissingReason::NotRequested),
            blob_sidecars: Material::Missing(leani_primitives::MissingReason::NotRequested),
            traces: Material::Missing(leani_primitives::MissingReason::NotRequested),
            state_diffs: Material::Missing(leani_primitives::MissingReason::NotRequested),
            provenance: Vec::new(),
            verification: VerificationReport::default(),
        }
    }

    #[tokio::test]
    async fn aggregate_state_is_one_key_across_arbitrary_blocks() {
        let processor = processor();
        let mut transaction = MemoryReducer::default();
        for (number, values) in [(1, vec![3, 4]), (2, vec![5])] {
            let frame = frame(number, &values);
            let delta = processor.map(&frame).await.expect("map");
            let cursor = ProcessorCursor {
                processor_id: processor.descriptor.id.to_string(),
                processor_version: processor.descriptor.version.to_string(),
                chain_id: frame.chain_id,
                block_number: frame.block.number,
                block_hash: frame.block.hash,
                finality: frame.finality,
                sequence: number,
            };
            processor
                .reduce(&mut transaction, &cursor, &delta)
                .await
                .expect("reduce");
        }
        let bytes = transaction
            .entity(AGGREGATE_COLLECTION, AGGREGATE_KEY)
            .expect("aggregate");
        let entity = decode_entity(bytes).expect("entity");
        assert_eq!(entity.count, 3);
        assert_eq!(U256::from_be_bytes(entity.total_value.0), U256::from(12));
    }
}
