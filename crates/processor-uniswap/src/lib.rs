//! Exact event-derived Uniswap V2 reserve and V3 square-root-price indexing.

use std::collections::BTreeMap;

use alloy_primitives::keccak256;
use async_trait::async_trait;
use leani_primitives::{
    Address, BlockFrame, BlockHash, BlockNumber, Capability, CapabilitySet, Completeness,
    FilterScope, Finality, Material, ProcessorCursor, Quantity, TopicFilter,
};
use leani_processor_api::{
    ChangeOperation, DataRequirement, DeliveryOrdering, DomainChange, DomainChanges, EncodedDelta,
    LifecyclePolicies, Processor, ProcessorDescriptor, ProcessorError, ProcessorId,
    ProcessorInstanceId, ProcessorSchemas, PublicationPolicy, ReducerTransaction, ReductionMode,
    RetentionPolicy, StartPoint,
};
use semver::Version;
use serde::{Deserialize, Serialize};

pub const CURRENT_COLLECTION: &str = "uniswap.pools.current";
pub const HISTORY_COLLECTION: &str = "uniswap.pools.history";
pub const OBSERVATION_LATEST_COLLECTION: &str = "uniswap.observations.latest";

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PoolKind {
    V2,
    V3,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PoolConfig {
    pub address: Address,
    pub kind: PoolKind,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct UniswapConfig {
    pub start_block: BlockNumber,
    pub pools: Vec<PoolConfig>,
}

/// Exact raw event state. Clients derive rational/decimal prices using token
/// metadata appropriate to their application.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PoolPriceEntity {
    pub pool: Address,
    pub kind: PoolKind,
    pub reserve0: Option<Quantity>,
    pub reserve1: Option<Quantity>,
    /// Signed token0 pool delta from a V3 `Swap`, encoded as an ABI int256 word.
    pub amount0: Option<Quantity>,
    /// Signed token1 pool delta from a V3 `Swap`, encoded as an ABI int256 word.
    pub amount1: Option<Quantity>,
    pub sqrt_price_x96: Option<Quantity>,
    pub block_number: BlockNumber,
    pub block_hash: BlockHash,
    pub log_index: u32,
    pub finality: Finality,
}

/// Stable block-local map artifact shared by the latest and observation
/// reducers.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct UniswapPriceDelta {
    pub observations: Vec<PoolPriceEntity>,
}

#[derive(Clone, Debug)]
pub struct UniswapLatestProcessor {
    pools: BTreeMap<Address, PoolKind>,
    descriptor: ProcessorDescriptor,
}

impl UniswapLatestProcessor {
    /// Build a configured-pool processor.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty/duplicate pool list or invalid config
    /// encoding.
    pub fn new(mut config: UniswapConfig) -> Result<Self, ProcessorError> {
        config.pools.sort_by_key(|pool| pool.address);
        if config.pools.is_empty()
            || config
                .pools
                .windows(2)
                .any(|pair| pair[0].address == pair[1].address)
        {
            return Err(ProcessorError::Input(
                "Uniswap pool list must be non-empty and unique".to_owned(),
            ));
        }
        let pools = config
            .pools
            .iter()
            .map(|pool| (pool.address, pool.kind))
            .collect();
        let encoded = postcard::to_allocvec(&config)
            .map_err(|error| ProcessorError::Input(error.to_string()))?;
        let id = ProcessorId::new("uniswap-latest")
            .map_err(|error| ProcessorError::Input(error.to_string()))?;
        let version = Version::new(2, 0, 0);
        let config_hash = BlockHash::new(*blake3::hash(&encoded).as_bytes());
        let descriptor = ProcessorDescriptor {
            instance: ProcessorInstanceId::legacy(&id, &version, config_hash),
            id,
            version,
            code_hash: BlockHash::new(*blake3::hash(b"leani/uniswap-latest/2.0.0").as_bytes()),
            config_hash,
            start: StartPoint::Block(config.start_block),
            requirements: vec![DataRequirement {
                capabilities: CapabilitySet::of(Capability::Logs),
                log_fields: leani_primitives::LogFieldSet::NONE,
                allow_filtered: true,
                filter: FilterScope {
                    addresses: config.pools.iter().map(|pool| pool.address).collect(),
                    topics: vec![TopicFilter {
                        position: 0,
                        alternatives: vec![v2_sync_topic(), v3_initialize_topic(), v3_swap_topic()],
                    }],
                    ..FilterScope::default()
                },
                minimum_finality: Finality::Optimistic,
            }],
            mode: ReductionMode::OrderedState,
            delivery_ordering: DeliveryOrdering::Canonical,
            publication: PublicationPolicy::OptimisticAndFinalized,
            lifecycle: LifecyclePolicies::from_legacy(RetentionPolicy::LatestState),
            schemas: ProcessorSchemas {
                delta_version: 2,
                entity_schema: "uniswap.price.entity.v2".to_owned(),
                change_schema: "uniswap.price.change.v2".to_owned(),
            },
        };
        Ok(Self { pools, descriptor })
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
}

#[derive(Clone, Debug)]
pub struct UniswapObservationsProcessor {
    pools: BTreeMap<Address, PoolKind>,
    descriptor: ProcessorDescriptor,
}

impl UniswapObservationsProcessor {
    /// Build a block-local immutable observation processor.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty/duplicate pool list or invalid config
    /// encoding.
    pub fn new(mut config: UniswapConfig) -> Result<Self, ProcessorError> {
        config.pools.sort_by_key(|pool| pool.address);
        if config.pools.is_empty()
            || config
                .pools
                .windows(2)
                .any(|pair| pair[0].address == pair[1].address)
        {
            return Err(ProcessorError::Input(
                "Uniswap pool list must be non-empty and unique".to_owned(),
            ));
        }
        let pools = config
            .pools
            .iter()
            .map(|pool| (pool.address, pool.kind))
            .collect();
        let encoded = postcard::to_allocvec(&config)
            .map_err(|error| ProcessorError::Input(error.to_string()))?;
        let id = ProcessorId::new("uniswap-observations")
            .map_err(|error| ProcessorError::Input(error.to_string()))?;
        let version = Version::new(2, 1, 0);
        let config_hash = BlockHash::new(*blake3::hash(&encoded).as_bytes());
        let descriptor = ProcessorDescriptor {
            instance: ProcessorInstanceId::legacy(&id, &version, config_hash),
            id,
            version,
            code_hash: BlockHash::new(
                *blake3::hash(b"leani/uniswap-observations/2.1.0").as_bytes(),
            ),
            config_hash,
            start: StartPoint::Block(config.start_block),
            requirements: vec![uniswap_requirement(&config)],
            mode: ReductionMode::BlockLocal,
            delivery_ordering: DeliveryOrdering::BlockVersionedIdempotent,
            publication: PublicationPolicy::OptimisticAndFinalized,
            lifecycle: LifecyclePolicies::from_legacy(RetentionPolicy::FullOutputHistory),
            schemas: ProcessorSchemas {
                delta_version: 2,
                entity_schema: "uniswap.observation.entity.v2".to_owned(),
                change_schema: "uniswap.observation.change.v2".to_owned(),
            },
        };
        Ok(Self { pools, descriptor })
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

    /// Return the immutable pool scope configured for this processor instance.
    #[must_use]
    pub fn configured_pools(&self) -> Vec<PoolConfig> {
        self.pools
            .iter()
            .map(|(address, kind)| PoolConfig {
                address: *address,
                kind: *kind,
            })
            .collect()
    }
}

#[async_trait]
impl Processor for UniswapLatestProcessor {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn descriptor(&self) -> &ProcessorDescriptor {
        &self.descriptor
    }

    async fn map(&self, block: &BlockFrame) -> Result<EncodedDelta, ProcessorError> {
        map_price_delta(&self.descriptor, &self.pools, block)
    }

    fn finality_variant_checksums(
        &self,
        delta: &EncodedDelta,
    ) -> Result<Vec<BlockHash>, ProcessorError> {
        finality_variant_checksums(&self.descriptor, delta)
    }

    async fn reduce(
        &self,
        transaction: &mut dyn ReducerTransaction,
        cursor: &ProcessorCursor,
        delta: &EncodedDelta,
    ) -> Result<DomainChanges, ProcessorError> {
        validate_cursor(&self.descriptor, cursor, delta)?;
        let delta: UniswapPriceDelta = postcard::from_bytes(&delta.payload)
            .map_err(|error| ProcessorError::DeltaPayload(error.to_string()))?;
        let mut changes = Vec::new();
        let mut current = BTreeMap::new();
        for observation in delta.observations {
            current.insert(observation.pool, observation);
        }
        for (pool, observation) in current {
            write_entity(
                transaction,
                CURRENT_COLLECTION,
                "uniswap.price.current",
                pool.0.to_vec(),
                &observation,
                &mut changes,
            )
            .await?;
        }
        Ok(DomainChanges { changes })
    }

    fn entity_json(
        &self,
        collection: &str,
        _key: &[u8],
        value: &[u8],
    ) -> Result<Option<serde_json::Value>, ProcessorError> {
        if collection != CURRENT_COLLECTION {
            return Ok(None);
        }
        let entity: PoolPriceEntity = postcard::from_bytes(value)
            .map_err(|error| ProcessorError::State(error.to_string()))?;
        serde_json::to_value(entity)
            .map(Some)
            .map_err(|error| ProcessorError::State(error.to_string()))
    }
}

#[async_trait]
impl Processor for UniswapObservationsProcessor {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn descriptor(&self) -> &ProcessorDescriptor {
        &self.descriptor
    }

    async fn map(&self, block: &BlockFrame) -> Result<EncodedDelta, ProcessorError> {
        map_price_delta(&self.descriptor, &self.pools, block)
    }

    fn finality_variant_checksums(
        &self,
        delta: &EncodedDelta,
    ) -> Result<Vec<BlockHash>, ProcessorError> {
        finality_variant_checksums(&self.descriptor, delta)
    }

    async fn reduce(
        &self,
        transaction: &mut dyn ReducerTransaction,
        cursor: &ProcessorCursor,
        delta: &EncodedDelta,
    ) -> Result<DomainChanges, ProcessorError> {
        validate_cursor(&self.descriptor, cursor, delta)?;
        let delta: UniswapPriceDelta = postcard::from_bytes(&delta.payload)
            .map_err(|error| ProcessorError::DeltaPayload(error.to_string()))?;
        let mut changes = Vec::with_capacity(delta.observations.len());
        let mut latest_by_pool = BTreeMap::<Address, PoolPriceEntity>::new();
        for observation in delta.observations {
            write_entity(
                transaction,
                HISTORY_COLLECTION,
                "uniswap.price.observation",
                history_key(&observation),
                &observation,
                &mut changes,
            )
            .await?;
            let replace = latest_by_pool
                .get(&observation.pool)
                .is_none_or(|current| observation.log_index > current.log_index);
            if replace {
                latest_by_pool.insert(observation.pool, observation);
            }
        }
        for (pool, observation) in latest_by_pool {
            let current = transaction
                .get(OBSERVATION_LATEST_COLLECTION, &pool.0)
                .await?
                .map(|encoded| {
                    postcard::from_bytes::<PoolPriceEntity>(&encoded)
                        .map_err(|error| ProcessorError::State(error.to_string()))
                })
                .transpose()?;
            let replace = current.as_ref().is_none_or(|current| {
                (observation.block_number, observation.log_index)
                    >= (current.block_number, current.log_index)
            });
            if replace {
                let encoded = postcard::to_allocvec(&observation)
                    .map_err(|error| ProcessorError::State(error.to_string()))?;
                transaction
                    .put(OBSERVATION_LATEST_COLLECTION, pool.0.to_vec(), encoded)
                    .await?;
            }
        }
        Ok(DomainChanges { changes })
    }

    fn entity_json(
        &self,
        collection: &str,
        _key: &[u8],
        value: &[u8],
    ) -> Result<Option<serde_json::Value>, ProcessorError> {
        if collection != HISTORY_COLLECTION && collection != OBSERVATION_LATEST_COLLECTION {
            return Ok(None);
        }
        let entity: PoolPriceEntity = postcard::from_bytes(value)
            .map_err(|error| ProcessorError::State(error.to_string()))?;
        serde_json::to_value(entity)
            .map(Some)
            .map_err(|error| ProcessorError::State(error.to_string()))
    }
}

fn uniswap_requirement(config: &UniswapConfig) -> DataRequirement {
    DataRequirement {
        capabilities: CapabilitySet::of(Capability::Logs),
        log_fields: leani_primitives::LogFieldSet::NONE,
        allow_filtered: true,
        filter: FilterScope {
            addresses: config.pools.iter().map(|pool| pool.address).collect(),
            topics: vec![TopicFilter {
                position: 0,
                alternatives: vec![v2_sync_topic(), v3_initialize_topic(), v3_swap_topic()],
            }],
            ..FilterScope::default()
        },
        minimum_finality: Finality::Optimistic,
    }
}

fn map_price_delta(
    descriptor: &ProcessorDescriptor,
    pools: &BTreeMap<Address, PoolKind>,
    block: &BlockFrame,
) -> Result<EncodedDelta, ProcessorError> {
    descriptor.requirements[0]
        .validate_frame(block)
        .map_err(|error| ProcessorError::Input(error.to_owned()))?;
    let mut observations = Vec::new();
    for log in accepted_logs(&block.logs)? {
        let Some(kind) = pools.get(&log.address).copied() else {
            continue;
        };
        let Some(topic) = log.topics.first() else {
            continue;
        };
        let (reserve0, reserve1, amount0, amount1, sqrt_price_x96) = match (kind, *topic) {
            (PoolKind::V2, topic) if topic == v2_sync_topic() => (
                Some(word(&log.data, 0)?),
                Some(word(&log.data, 1)?),
                None,
                None,
                None,
            ),
            (PoolKind::V3, topic) if topic == v3_initialize_topic() => {
                (None, None, None, None, Some(word(&log.data, 0)?))
            }
            (PoolKind::V3, topic) if topic == v3_swap_topic() => (
                None,
                None,
                Some(word(&log.data, 0)?),
                Some(word(&log.data, 1)?),
                Some(word(&log.data, 2)?),
            ),
            _ => continue,
        };
        observations.push(PoolPriceEntity {
            pool: log.address,
            kind,
            reserve0,
            reserve1,
            amount0,
            amount1,
            sqrt_price_x96,
            block_number: block.block.number,
            block_hash: block.block.hash,
            log_index: log.log_index,
            finality: block.finality,
        });
    }
    observations.sort_by_key(|value| (value.log_index, value.pool));
    Ok(EncodedDelta::new(
        descriptor,
        block.chain_id,
        block.block,
        postcard::to_allocvec(&UniswapPriceDelta { observations })
            .map_err(|error| ProcessorError::DeltaPayload(error.to_string()))?,
    ))
}

fn finality_variant_checksums(
    descriptor: &ProcessorDescriptor,
    delta: &EncodedDelta,
) -> Result<Vec<BlockHash>, ProcessorError> {
    delta.validate(descriptor)?;
    let decoded: UniswapPriceDelta = postcard::from_bytes(&delta.payload)
        .map_err(|error| ProcessorError::DeltaPayload(error.to_string()))?;
    let mut checksums = Vec::with_capacity(3);
    for finality in [Finality::Optimistic, Finality::Safe, Finality::Finalized] {
        let mut variant = decoded.clone();
        for observation in &mut variant.observations {
            observation.finality = finality;
        }
        let payload = postcard::to_allocvec(&variant)
            .map_err(|error| ProcessorError::DeltaPayload(error.to_string()))?;
        checksums
            .push(EncodedDelta::new(descriptor, delta.chain_id, delta.block, payload).checksum);
    }
    checksums.sort_unstable();
    checksums.dedup();
    Ok(checksums)
}

async fn write_entity(
    transaction: &mut dyn ReducerTransaction,
    collection: &str,
    kind: &str,
    key: Vec<u8>,
    entity: &PoolPriceEntity,
    changes: &mut Vec<DomainChange>,
) -> Result<(), ProcessorError> {
    let payload =
        postcard::to_allocvec(entity).map_err(|error| ProcessorError::State(error.to_string()))?;
    transaction
        .put(collection, key.clone(), payload.clone())
        .await?;
    let change = DomainChange {
        kind: kind.to_owned(),
        key,
        operation: ChangeOperation::Upsert,
        payload,
    };
    transaction.emit(change.clone()).await?;
    changes.push(change);
    Ok(())
}

fn accepted_logs(
    logs: &Material<Vec<leani_primitives::Log>>,
) -> Result<&[leani_primitives::Log], ProcessorError> {
    match logs {
        Material::Complete(logs)
        | Material::Filtered {
            value: logs,
            completeness: Completeness::VerifiedPredicate | Completeness::DatasetDeclared,
            ..
        } => Ok(logs),
        Material::Filtered {
            completeness: Completeness::Partial,
            ..
        } => Err(ProcessorError::Input(
            "Uniswap log projection is partial".to_owned(),
        )),
        Material::Missing(reason) => Err(ProcessorError::Input(format!(
            "Uniswap logs are missing: {reason:?}"
        ))),
    }
}

fn word(data: &[u8], index: usize) -> Result<Quantity, ProcessorError> {
    let start = index.saturating_mul(32);
    let end = start.saturating_add(32);
    Ok(Quantity::new(
        data.get(start..end)
            .ok_or_else(|| ProcessorError::Input("Uniswap event data is truncated".to_owned()))?
            .try_into()
            .map_err(|_| ProcessorError::Input("Uniswap word is invalid".to_owned()))?,
    ))
}

fn history_key(entity: &PoolPriceEntity) -> Vec<u8> {
    let mut key = Vec::with_capacity(56);
    key.extend_from_slice(&entity.pool.0);
    key.extend_from_slice(&entity.block_hash.0);
    key.extend_from_slice(&entity.log_index.to_be_bytes());
    key
}

fn v2_sync_topic() -> [u8; 32] {
    keccak256("Sync(uint112,uint112)").0
}

fn v3_initialize_topic() -> [u8; 32] {
    keccak256("Initialize(uint160,int24)").0
}

fn v3_swap_topic() -> [u8; 32] {
    keccak256("Swap(address,address,int256,int256,uint160,uint128,int24)").0
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
    use alloy_primitives::U256;
    use leani_primitives::{BlockRef, ChainId, Log};
    use leani_testkit::{MemoryReducer, fixture_frame};

    use super::*;

    fn cursor(processor: &UniswapLatestProcessor, frame: &BlockFrame) -> ProcessorCursor {
        ProcessorCursor {
            processor_id: processor.descriptor.id.to_string(),
            processor_version: processor.descriptor.version.to_string(),
            chain_id: ChainId(1),
            block_number: frame.block.number,
            block_hash: frame.block.hash,
            finality: frame.finality,
            sequence: frame.block.number.0,
        }
    }

    fn log(pool: Address, topic: [u8; 32], words: &[U256], index: u32) -> Log {
        Log {
            address: pool,
            topics: vec![topic],
            data: words
                .iter()
                .flat_map(alloy_primitives::Uint::to_be_bytes::<32>)
                .collect(),
            transaction_hash: None,
            transaction_index: 0,
            log_index: index,
        }
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn latest_and_observation_contracts_share_exact_event_decoding() {
        let v2 = Address::new([2; 20]);
        let v3 = Address::new([3; 20]);
        let processor = UniswapLatestProcessor::new(UniswapConfig {
            start_block: BlockNumber(1),
            pools: vec![
                PoolConfig {
                    address: v2,
                    kind: PoolKind::V2,
                },
                PoolConfig {
                    address: v3,
                    kind: PoolKind::V3,
                },
            ],
        })
        .expect("processor");
        let mut frame = fixture_frame(1, BlockHash::ZERO);
        frame.logs = Material::Complete(vec![
            log(v2, v2_sync_topic(), &[U256::from(11), U256::from(22)], 0),
            log(
                v3,
                v3_swap_topic(),
                &[
                    U256::from(7),
                    U256::from(9).wrapping_neg(),
                    U256::from(33),
                    U256::from(44),
                    U256::ZERO,
                ],
                1,
            ),
        ]);
        let delta = processor.map(&frame).await.expect("map");
        let variants = processor
            .finality_variant_checksums(&delta)
            .expect("finality variants");
        assert_eq!(variants.len(), 3);
        assert!(variants.contains(&delta.checksum));
        let mut reducer = MemoryReducer::default();
        let changes = processor
            .reduce(&mut reducer, &cursor(&processor, &frame), &delta)
            .await
            .expect("reduce");
        assert_eq!(changes.changes.len(), 2);
        assert_eq!(processor.descriptor.mode, ReductionMode::OrderedState);
        assert_eq!(
            processor.descriptor.delivery_ordering,
            DeliveryOrdering::Canonical
        );
        let v2_state: PoolPriceEntity = postcard::from_bytes(
            reducer
                .entity(CURRENT_COLLECTION, &v2.0)
                .expect("v2 current"),
        )
        .expect("decode");
        let v3_state: PoolPriceEntity = postcard::from_bytes(
            reducer
                .entity(CURRENT_COLLECTION, &v3.0)
                .expect("v3 current"),
        )
        .expect("decode");
        assert_eq!(
            U256::from_be_bytes(v2_state.reserve0.expect("reserve").0),
            U256::from(11)
        );
        assert_eq!(
            U256::from_be_bytes(v3_state.sqrt_price_x96.expect("sqrt").0),
            U256::from(33)
        );
        assert_eq!(
            U256::from_be_bytes(v3_state.amount0.expect("amount0").0),
            U256::from(7)
        );
        assert_eq!(
            alloy_primitives::I256::from_raw(U256::from_be_bytes(
                v3_state.amount1.expect("amount1").0
            )),
            alloy_primitives::I256::unchecked_from(-9)
        );

        let observations = UniswapObservationsProcessor::new(UniswapConfig {
            start_block: BlockNumber(1),
            pools: vec![
                PoolConfig {
                    address: v2,
                    kind: PoolKind::V2,
                },
                PoolConfig {
                    address: v3,
                    kind: PoolKind::V3,
                },
            ],
        })
        .expect("observations processor");
        let observation_delta = observations.map(&frame).await.expect("map observations");
        let decoded: UniswapPriceDelta =
            postcard::from_bytes(&observation_delta.payload).expect("decode observations");
        let mut observation_reducer = MemoryReducer::default();
        let changes = observations
            .reduce(
                &mut observation_reducer,
                &ProcessorCursor {
                    processor_id: observations.descriptor.id.to_string(),
                    processor_version: observations.descriptor.version.to_string(),
                    chain_id: ChainId(1),
                    block_number: frame.block.number,
                    block_hash: frame.block.hash,
                    finality: frame.finality,
                    sequence: frame.block.number.0,
                },
                &observation_delta,
            )
            .await
            .expect("reduce observations");
        assert_eq!(changes.changes.len(), 2);
        assert_eq!(observations.descriptor.mode, ReductionMode::BlockLocal);
        assert_eq!(
            observations.descriptor.delivery_ordering,
            DeliveryOrdering::BlockVersionedIdempotent
        );
        for observation in decoded.observations {
            assert!(
                observation_reducer
                    .entity(HISTORY_COLLECTION, &history_key(&observation))
                    .is_some()
            );
        }
        let latest: PoolPriceEntity = postcard::from_bytes(
            observation_reducer
                .entity(OBSERVATION_LATEST_COLLECTION, &v3.0)
                .expect("latest v3 observation"),
        )
        .expect("decode latest observation");
        assert_eq!(latest.pool, v3);
        assert_eq!(latest.log_index, 1);

        let older_block = BlockRef {
            number: BlockNumber(0),
            hash: BlockHash::new([0x44; 32]),
            parent_hash: BlockHash::ZERO,
            timestamp: frame.block.timestamp.saturating_sub(1),
        };
        let mut older = latest.clone();
        older.block_number = older_block.number;
        older.block_hash = older_block.hash;
        older.finality = Finality::Finalized;
        let older_delta = EncodedDelta::new(
            observations.descriptor(),
            ChainId(1),
            older_block,
            postcard::to_allocvec(&UniswapPriceDelta {
                observations: vec![older],
            })
            .expect("older observation delta"),
        );
        observations
            .reduce(
                &mut observation_reducer,
                &ProcessorCursor {
                    processor_id: observations.descriptor.id.to_string(),
                    processor_version: observations.descriptor.version.to_string(),
                    chain_id: ChainId(1),
                    block_number: older_block.number,
                    block_hash: older_block.hash,
                    finality: Finality::Finalized,
                    sequence: 2,
                },
                &older_delta,
            )
            .await
            .expect("reduce older finalized observation");
        let latest_after_finality: PoolPriceEntity = postcard::from_bytes(
            observation_reducer
                .entity(OBSERVATION_LATEST_COLLECTION, &v3.0)
                .expect("latest observation after older finality"),
        )
        .expect("decode latest observation after older finality");
        assert_eq!(latest_after_finality.block_number, frame.block.number);
        assert_eq!(latest_after_finality.block_hash, frame.block.hash);
    }
}
