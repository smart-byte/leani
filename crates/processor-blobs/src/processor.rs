//! Processor implementation for blobs block and transaction entities.

use std::collections::BTreeMap;

use alloy_primitives::U256;
use async_trait::async_trait;
use leani_primitives::{
    BlockFrame, BlockHash, BlockNumber, Capability, CapabilitySet, Completeness, FilterScope,
    Finality, Material, ProcessorCursor, Quantity, ReceiptEnvelope, TransactionEnvelope,
    TransactionHash,
};
use leani_processor_api::{
    ChangeOperation, DataRequirement, DeliveryOrdering, DomainChange, DomainChanges, EncodedDelta,
    LifecyclePolicies, Processor, ProcessorDescriptor, ProcessorError, ProcessorId,
    ProcessorInstanceId, ProcessorSchemas, PublicationPolicy, ReducerTransaction, ReductionMode,
    RetentionPolicy, StartPoint,
};
use semver::Version;

use crate::{
    BLOB_GAS_PER_BLOB, BlobSchedule, BlobTransactionEntity, BlobsBlockEntity, BlobsDelta,
    calculate_eip7918_floor, get_blob_base_fee, get_blob_base_fee_eip7918, math::checked_mul,
};

pub const BLOCK_COLLECTION: &str = "blobs.blocks";
pub const TRANSACTION_COLLECTION: &str = "blobs.transactions";
pub const TRANSACTION_BLOCK_INDEX: &str = "blobs.transactions.by_block";
pub const TRANSFORM_VERSION: u16 = 3;

#[derive(Clone, Debug)]
pub struct BlobsProcessor {
    descriptor: ProcessorDescriptor,
    schedule: BlobSchedule,
}

impl Default for BlobsProcessor {
    fn default() -> Self {
        Self::new(BlobSchedule::mainnet()).expect("built-in mainnet blob schedule is valid")
    }
}

impl BlobsProcessor {
    /// Construct a processor for an immutable versioned schedule.
    ///
    /// # Errors
    ///
    /// Returns an input error when the schedule is malformed or cannot be
    /// encoded into the descriptor configuration hash.
    pub fn new(schedule: BlobSchedule) -> Result<Self, ProcessorError> {
        let start = BlockNumber(schedule.first_block());
        Self::new_at(schedule, start)
    }

    /// Construct a processor whose declared coverage starts at or after blob
    /// activation while retaining the immutable checked chain schedule.
    ///
    /// # Errors
    ///
    /// Rejects a start before blob activation or an invalid schedule.
    pub fn new_at(schedule: BlobSchedule, start: BlockNumber) -> Result<Self, ProcessorError> {
        schedule
            .validate()
            .map_err(|error| ProcessorError::Input(error.to_owned()))?;
        if start.0 < schedule.first_block() {
            return Err(ProcessorError::Input(format!(
                "blob processor start {} predates activation {}",
                start.0,
                schedule.first_block()
            )));
        }
        let encoded_schedule = postcard::to_allocvec(&schedule)
            .map_err(|error| ProcessorError::Input(error.to_string()))?;
        // A later block-local coverage start changes scheduling but not the
        // transform's output semantics. Keep the schedule-only identity
        // compatible with databases created before starts were reflected in
        // the descriptor.
        let config_hash = BlockHash::new(*blake3::hash(&encoded_schedule).as_bytes());
        let code_hash = BlockHash::new(*blake3::hash(b"leani/blobs-processor/1.4.0").as_bytes());
        let id = ProcessorId::new("blobs-money")
            .map_err(|error| ProcessorError::Input(error.to_string()))?;
        let version = Version::new(1, 4, 0);
        let descriptor = ProcessorDescriptor {
            instance: ProcessorInstanceId::legacy(&id, &version, config_hash),
            id,
            version,
            code_hash,
            config_hash,
            start: StartPoint::Block(start),
            requirements: vec![DataRequirement {
                capabilities: CapabilitySet::of(Capability::Header)
                    .with(Capability::Transactions)
                    .with(Capability::Receipts),
                log_fields: leani_primitives::LogFieldSet::NONE,
                allow_filtered: true,
                filter: FilterScope {
                    transaction_types: vec![3],
                    ..FilterScope::default()
                },
                minimum_finality: Finality::Included,
            }],
            mode: ReductionMode::BlockLocal,
            delivery_ordering: DeliveryOrdering::BlockVersionedIdempotent,
            publication: PublicationPolicy::IncludedAndFinalized,
            lifecycle: LifecyclePolicies::from_legacy(RetentionPolicy::FullOutputHistory),
            schemas: ProcessorSchemas {
                delta_version: 2,
                entity_schema: "blobs.entity.v3".to_owned(),
                change_schema: "blobs.block-bundle.v1".to_owned(),
            },
        };
        Ok(Self {
            descriptor,
            schedule,
        })
    }

    /// Apply immutable operator-owned instance and lifecycle policies.
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

    #[must_use]
    pub fn schedule(&self) -> &BlobSchedule {
        &self.schedule
    }

    /// Derive the stable block-local domain delta without mutating state.
    ///
    /// # Errors
    ///
    /// Rejects incomplete, partial, inconsistent, or out-of-schedule input.
    #[allow(clippy::too_many_lines)]
    pub fn derive(&self, frame: &BlockFrame) -> Result<BlobsDelta, ProcessorError> {
        self.descriptor.requirements[0]
            .validate_frame(frame)
            .map_err(|error| ProcessorError::Input(error.to_owned()))?;
        if frame.chain_id.0 != self.schedule.chain_id {
            return Err(ProcessorError::Input(format!(
                "blob schedule is for chain {}, received {}",
                self.schedule.chain_id, frame.chain_id.0
            )));
        }
        let parameters = self
            .schedule
            .parameters(frame.block.number.0)
            .ok_or_else(|| ProcessorError::Input("block predates blob activation".to_owned()))?;
        let header = accepted_material(&frame.header, "header")?;
        let transactions = accepted_material(&frame.transactions, "transactions")?;
        let receipts = accepted_material(&frame.receipts, "receipts")?;
        validate_transaction_projection(&frame.transactions)?;

        let gas_limit = required(header.gas_limit, "header.gas_limit")?;
        let gas_used = required(header.gas_used, "header.gas_used")?;
        let execution_base_fee = quantity_to_u256(required(
            header.base_fee_per_gas,
            "header.base_fee_per_gas",
        )?);
        let blob_gas_used = required(header.blob_gas_used, "header.blob_gas_used")?;
        let excess_blob_gas = required(header.excess_blob_gas, "header.excess_blob_gas")?;
        let size_bytes = required(header.size_bytes, "header.size_bytes")?;
        let transaction_count = required(header.transaction_count, "header.transaction_count")?;
        if !blob_gas_used.is_multiple_of(BLOB_GAS_PER_BLOB) {
            return Err(ProcessorError::Invariant(
                "block blob gas is not a whole blob count".to_owned(),
            ));
        }
        let blob_count = u32::try_from(blob_gas_used / BLOB_GAS_PER_BLOB)
            .map_err(|_| ProcessorError::Invariant("blob count overflows u32".to_owned()))?;
        if blob_count > parameters.max_blobs_per_block {
            return Err(ProcessorError::Invariant(
                "block exceeds configured maximum blobs".to_owned(),
            ));
        }
        let blob_base_fee = if parameters.eip7918 {
            get_blob_base_fee_eip7918(
                excess_blob_gas,
                execution_base_fee,
                parameters.base_fee_update_fraction,
            )?
        } else {
            get_blob_base_fee(excess_blob_gas, parameters.base_fee_update_fraction)?
        };
        let reserve_fee = parameters
            .eip7918
            .then(|| calculate_eip7918_floor(execution_base_fee))
            .transpose()?;
        let execution_burn = checked_mul(execution_base_fee, U256::from(gas_used))?;
        let blob_burn = (blob_gas_used > 0)
            .then(|| checked_mul(blob_base_fee, U256::from(blob_gas_used)))
            .transpose()?;

        let receipts = receipt_map(receipts)?;
        let mut transaction_entities = Vec::new();
        let mut projected_blob_count = 0_u32;
        for transaction in transactions
            .iter()
            .filter(|transaction| transaction.transaction_type == 3)
        {
            let entity = map_transaction(
                frame,
                transaction,
                receipts.get(&transaction.hash).ok_or_else(|| {
                    ProcessorError::Input(format!(
                        "missing projected receipt for {}",
                        transaction.hash
                    ))
                })?,
                blob_base_fee,
                execution_base_fee,
                &self.schedule.network,
            )?;
            projected_blob_count = projected_blob_count
                .checked_add(entity.blob_count)
                .ok_or_else(|| ProcessorError::Invariant("blob count overflow".to_owned()))?;
            transaction_entities.push(entity);
        }
        if projected_blob_count != blob_count {
            return Err(ProcessorError::Invariant(format!(
                "projected transactions contain {projected_blob_count} blobs, header declares {blob_count}"
            )));
        }
        transaction_entities.sort_by_key(|entity| entity.transaction_hash);

        Ok(BlobsDelta {
            block: BlobsBlockEntity {
                network: self.schedule.network.clone(),
                block_number: frame.block.number.0,
                block_hash: frame.block.hash,
                parent_hash: frame.block.parent_hash,
                timestamp: frame.block.timestamp,
                finality: frame.finality,
                size_bytes,
                blob_count,
                blob_gas_used,
                excess_blob_gas,
                blob_base_fee: blob_base_fee.into(),
                execution_base_fee: execution_base_fee.into(),
                gas_used,
                gas_limit,
                execution_burn: execution_burn.into(),
                blob_burn: blob_burn.map(Into::into),
                reserve_fee: reserve_fee.map(Into::into),
                transaction_count,
                target_blobs_per_block: parameters.target_blobs_per_block,
                max_blobs_per_block: parameters.max_blobs_per_block,
                transform_version: TRANSFORM_VERSION,
            },
            transactions: transaction_entities,
        })
    }
}

#[async_trait]
impl Processor for BlobsProcessor {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn descriptor(&self) -> &ProcessorDescriptor {
        &self.descriptor
    }

    async fn map(&self, block: &BlockFrame) -> Result<EncodedDelta, ProcessorError> {
        let delta = self.derive(block)?;
        let payload = postcard::to_allocvec(&delta)
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
        let decoded: BlobsDelta = postcard::from_bytes(&delta.payload)
            .map_err(|error| ProcessorError::DeltaPayload(error.to_string()))?;
        let mut checksums = Vec::with_capacity(2);
        for finality in [Finality::Included, Finality::Finalized] {
            let mut variant = decoded.clone();
            variant.block.finality = finality;
            let payload = postcard::to_allocvec(&variant)
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
        validate_delta_cursor(&self.descriptor, cursor, delta)?;
        let decoded: BlobsDelta = postcard::from_bytes(&delta.payload)
            .map_err(|error| ProcessorError::DeltaPayload(error.to_string()))?;
        if decoded.block.block_number != delta.block.number.0
            || decoded.block.block_hash != delta.block.hash
        {
            return Err(ProcessorError::DeltaContract);
        }

        let block_key = decoded.block.block_number.to_be_bytes().to_vec();
        let bundle_payload = postcard::to_allocvec(&decoded)
            .map_err(|error| ProcessorError::State(error.to_string()))?;
        let block_payload = postcard::to_allocvec(&decoded.block)
            .map_err(|error| ProcessorError::State(error.to_string()))?;
        transaction
            .put(BLOCK_COLLECTION, block_key.clone(), block_payload.clone())
            .await?;
        let changes = vec![DomainChange {
            kind: "blobs.block".to_owned(),
            key: block_key.clone(),
            operation: ChangeOperation::Upsert,
            payload: bundle_payload,
        }];
        for entity in decoded.transactions {
            let key = entity.transaction_hash.0.to_vec();
            let payload = postcard::to_allocvec(&entity)
                .map_err(|error| ProcessorError::State(error.to_string()))?;
            transaction
                .put(TRANSACTION_COLLECTION, key.clone(), payload.clone())
                .await?;
            transaction
                .index_put(TRANSACTION_BLOCK_INDEX, block_key.clone(), key.clone())
                .await?;
        }
        for change in &changes {
            transaction.emit(change.clone()).await?;
        }
        Ok(DomainChanges { changes })
    }

    fn entity_json(
        &self,
        collection: &str,
        _key: &[u8],
        value: &[u8],
    ) -> Result<Option<serde_json::Value>, ProcessorError> {
        let value = match collection {
            BLOCK_COLLECTION => serde_json::to_value(
                postcard::from_bytes::<BlobsBlockEntity>(value)
                    .map_err(|error| ProcessorError::State(error.to_string()))?,
            ),
            TRANSACTION_COLLECTION => serde_json::to_value(
                postcard::from_bytes::<BlobTransactionEntity>(value)
                    .map_err(|error| ProcessorError::State(error.to_string()))?,
            ),
            _ => return Ok(None),
        }
        .map_err(|error| ProcessorError::State(error.to_string()))?;
        Ok(Some(value))
    }
}

fn accepted_material<'a, T>(
    material: &'a Material<T>,
    label: &str,
) -> Result<&'a T, ProcessorError> {
    match material {
        Material::Complete(value)
        | Material::Filtered {
            value,
            completeness: Completeness::VerifiedPredicate | Completeness::DatasetDeclared,
            ..
        } => Ok(value),
        Material::Filtered {
            completeness: Completeness::Partial,
            ..
        } => Err(ProcessorError::Input(format!(
            "{label} projection is only partial"
        ))),
        Material::Missing(reason) => Err(ProcessorError::Input(format!(
            "{label} material is missing: {reason:?}"
        ))),
    }
}

fn validate_transaction_projection(
    transactions: &Material<Vec<TransactionEnvelope>>,
) -> Result<(), ProcessorError> {
    if let Material::Filtered { scope, .. } = transactions
        && scope.transaction_types.as_slice() != [3]
    {
        return Err(ProcessorError::Input(
            "filtered transactions must declare exactly type 3".to_owned(),
        ));
    }
    Ok(())
}

fn receipt_map(
    receipts: &[ReceiptEnvelope],
) -> Result<BTreeMap<TransactionHash, &ReceiptEnvelope>, ProcessorError> {
    let mut output = BTreeMap::new();
    for receipt in receipts {
        if output.insert(receipt.transaction_hash, receipt).is_some() {
            return Err(ProcessorError::Invariant(
                "duplicate projected receipt".to_owned(),
            ));
        }
    }
    Ok(output)
}

fn map_transaction(
    frame: &BlockFrame,
    transaction: &TransactionEnvelope,
    receipt: &ReceiptEnvelope,
    blob_base_fee: U256,
    execution_base_fee: U256,
    network: &str,
) -> Result<BlobTransactionEntity, ProcessorError> {
    if transaction.hash != receipt.transaction_hash
        || transaction.index != receipt.transaction_index
        || receipt.transaction_type != 3
    {
        return Err(ProcessorError::Invariant(
            "transaction/receipt identity mismatch".to_owned(),
        ));
    }
    let sender = required(transaction.from, "transaction.from")?;
    let blob_count = u32::try_from(transaction.blob_versioned_hashes.len()).map_err(|_| {
        ProcessorError::Invariant("transaction blob count overflows u32".to_owned())
    })?;
    if blob_count == 0 {
        return Err(ProcessorError::Invariant(
            "type-3 transaction has no blob hashes".to_owned(),
        ));
    }
    let expected_blob_gas = u64::from(blob_count).saturating_mul(BLOB_GAS_PER_BLOB);
    if let Some(actual) = receipt.blob_gas_used
        && actual != expected_blob_gas
    {
        return Err(ProcessorError::Invariant(
            "transaction blob gas disagrees with hash count".to_owned(),
        ));
    }
    let receipt_gas_used = required(receipt.gas_used, "receipt.gas_used")?;
    let execution_burn = checked_mul(execution_base_fee, U256::from(receipt_gas_used))?;
    let blob_burn = checked_mul(blob_base_fee, U256::from(expected_blob_gas))?;
    let total_burn = execution_burn
        .checked_add(blob_burn)
        .ok_or_else(|| ProcessorError::Invariant("transaction burn overflow".to_owned()))?;
    Ok(BlobTransactionEntity {
        network: network.to_owned(),
        block_number: frame.block.number.0,
        block_hash: frame.block.hash,
        transaction_hash: transaction.hash,
        transaction_index: transaction.index,
        sender,
        blob_versioned_hashes: transaction.blob_versioned_hashes.clone(),
        blob_count,
        total_burn: total_burn.into(),
        execution_burn: execution_burn.into(),
        blob_burn: blob_burn.into(),
    })
}

fn required<T: Copy>(value: Option<T>, field: &str) -> Result<T, ProcessorError> {
    value.ok_or_else(|| ProcessorError::Input(format!("{field} is required")))
}

fn quantity_to_u256(value: Quantity) -> U256 {
    <U256 as From<Quantity>>::from(value)
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
    use leani_primitives::{
        Address, BlockRef, ChainId, HeaderEnvelope, ReceiptEnvelope, TransactionEnvelope,
        VerificationReport,
    };
    use leani_testkit::MemoryReducer;

    use super::*;

    fn quantity(value: u64) -> Quantity {
        U256::from(value).into()
    }

    fn frame(completeness: Completeness) -> BlockFrame {
        let scope = FilterScope {
            transaction_types: vec![3],
            ..FilterScope::default()
        };
        BlockFrame {
            chain_id: ChainId(1),
            block: BlockRef {
                number: 19_426_589.into(),
                hash: BlockHash::new([2; 32]),
                parent_hash: BlockHash::new([1; 32]),
                timestamp: 1_710_338_159,
            },
            finality: Finality::Finalized,
            header: Material::Filtered {
                value: HeaderEnvelope {
                    rlp: None,
                    transactions_root: None,
                    receipts_root: None,
                    withdrawals_root: None,
                    gas_limit: Some(1_000),
                    gas_used: Some(100),
                    base_fee_per_gas: Some(quantity(10)),
                    blob_gas_used: Some(BLOB_GAS_PER_BLOB),
                    excess_blob_gas: Some(0),
                    size_bytes: Some(500),
                    consensus_size_bytes: None,
                    transaction_count: Some(1),
                },
                scope: FilterScope::default(),
                completeness,
            },
            transactions: Material::Filtered {
                value: vec![TransactionEnvelope {
                    hash: TransactionHash::new([3; 32]),
                    transaction_type: 3,
                    index: 0,
                    encoded: None,
                    from: Some(Address::new([4; 20])),
                    to: None,
                    nonce: None,
                    gas_limit: Some(21_000),
                    value: None,
                    input: None,
                    max_fee_per_gas: None,
                    max_priority_fee_per_gas: None,
                    max_fee_per_blob_gas: Some(quantity(100)),
                    blob_versioned_hashes: vec![BlockHash::new([5; 32])],
                    size_bytes: Some(150),
                }],
                scope: scope.clone(),
                completeness,
            },
            receipts: Material::Filtered {
                value: vec![ReceiptEnvelope {
                    transaction_hash: TransactionHash::new([3; 32]),
                    transaction_type: 3,
                    transaction_index: 0,
                    encoded: None,
                    success: Some(true),
                    gas_used: Some(20),
                    effective_gas_price: Some(quantity(11)),
                    blob_gas_used: Some(BLOB_GAS_PER_BLOB),
                    blob_gas_price: None,
                    logs: Vec::new(),
                }],
                scope,
                completeness,
            },
            logs: Material::Missing(leani_primitives::MissingReason::NotRequested),
            withdrawals: Material::Missing(leani_primitives::MissingReason::NotRequested),
            blob_sidecars: Material::Missing(leani_primitives::MissingReason::NotRequested),
            traces: Material::Missing(leani_primitives::MissingReason::Unsupported),
            state_diffs: Material::Missing(leani_primitives::MissingReason::Unsupported),
            provenance: Vec::new(),
            verification: VerificationReport::default(),
        }
    }

    #[test]
    fn later_coverage_start_preserves_block_local_transform_identity() {
        let standard = BlobsProcessor::default();
        let later = BlobsProcessor::new_at(
            BlobSchedule::mainnet(),
            BlockNumber(standard.schedule().first_block().saturating_add(10_000)),
        )
        .expect("later start");
        assert_eq!(
            later.descriptor().start,
            StartPoint::Block(BlockNumber(
                standard.schedule().first_block().saturating_add(10_000)
            ))
        );
        assert_eq!(
            later.descriptor().config_hash,
            standard.descriptor().config_hash
        );
    }

    #[tokio::test]
    async fn maps_and_reduces_blobs_money_compatible_values() {
        let processor = BlobsProcessor::default();
        let frame = frame(Completeness::DatasetDeclared);
        let encoded = processor.map(&frame).await.expect("map");
        let variants = processor
            .finality_variant_checksums(&encoded)
            .expect("finality variants");
        assert_eq!(variants.len(), 2);
        assert!(variants.contains(&encoded.checksum));
        let decoded: BlobsDelta = postcard::from_bytes(&encoded.payload).expect("decode");
        assert_eq!(quantity_to_u256(decoded.block.blob_base_fee), U256::from(1));
        assert_eq!(
            quantity_to_u256(decoded.block.execution_burn),
            U256::from(1_000)
        );
        assert_eq!(
            quantity_to_u256(decoded.block.blob_burn.expect("blob burn")),
            U256::from(BLOB_GAS_PER_BLOB)
        );
        assert_eq!(
            quantity_to_u256(decoded.transactions[0].total_burn),
            U256::from(BLOB_GAS_PER_BLOB + 200)
        );

        let cursor = ProcessorCursor {
            processor_id: processor.descriptor().id.to_string(),
            processor_version: processor.descriptor().version.to_string(),
            chain_id: frame.chain_id,
            block_number: frame.block.number,
            block_hash: frame.block.hash,
            finality: frame.finality,
            sequence: 1,
        };
        let mut reducer = MemoryReducer::default();
        let changes = processor
            .reduce(&mut reducer, &cursor, &encoded)
            .await
            .expect("reduce");
        assert_eq!(changes.changes.len(), 1);
        assert_eq!(changes.changes[0].kind, "blobs.block");
        let change: BlobsDelta =
            postcard::from_bytes(&changes.changes[0].payload).expect("decode block bundle");
        assert_eq!(change, decoded);
        assert!(
            reducer
                .entity(BLOCK_COLLECTION, &frame.block.number.0.to_be_bytes())
                .is_some()
        );
        assert!(
            reducer
                .entity("blobs.block_bundles", &frame.block.number.0.to_be_bytes())
                .is_none()
        );
    }

    #[tokio::test]
    async fn rejects_partial_dataset_projection() {
        let error = BlobsProcessor::default()
            .map(&frame(Completeness::Partial))
            .await
            .expect_err("partial input fails");
        assert!(matches!(error, ProcessorError::Input(_)));
    }
}
