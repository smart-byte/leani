//! Declared event-derived ERC-20 watchlist balances.

use std::collections::{BTreeMap, BTreeSet};

use alloy_primitives::{B256, U256, keccak256};
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

pub const BALANCE_COLLECTION: &str = "erc20.balances";
pub const BALANCE_CHANGE_KIND: &str = "erc20.balance";
const ZERO_ADDRESS: Address = Address::new([0; 20]);

/// Immutable watchlist and completeness declaration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Erc20BalanceConfig {
    pub start_block: BlockNumber,
    pub addresses: Vec<Address>,
    /// Empty means all token contracts.
    pub tokens: Vec<Address>,
    /// True only when the start precedes every relevant initial mint or an
    /// independently validated opening snapshot is loaded.
    pub complete_from_start: bool,
}

/// Durable declared balance.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TokenBalanceEntity {
    pub token: Address,
    pub address: Address,
    pub balance: Quantity,
    pub as_of_block: BlockNumber,
    pub block_hash: BlockHash,
    pub finality: Finality,
    pub coverage_from: BlockNumber,
    pub complete: bool,
    pub method: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct TransferDelta {
    events: Vec<TransferEvent>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct TransferEvent {
    token: Address,
    from: Address,
    to: Address,
    value: Quantity,
    log_index: u32,
}

/// Ordered-state watchlist processor.
#[derive(Clone, Debug)]
pub struct Erc20BalanceProcessor {
    config: Erc20BalanceConfig,
    watched: BTreeSet<Address>,
    tokens: BTreeSet<Address>,
    descriptor: ProcessorDescriptor,
}

impl Erc20BalanceProcessor {
    /// Build a processor with a durable identity derived from its watchlist.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty watchlist or invalid config encoding.
    pub fn new(mut config: Erc20BalanceConfig) -> Result<Self, ProcessorError> {
        config.addresses.sort();
        config.addresses.dedup();
        config.tokens.sort();
        config.tokens.dedup();
        if config.addresses.is_empty() {
            return Err(ProcessorError::Input(
                "ERC-20 watchlist must contain at least one address".to_owned(),
            ));
        }
        let watched = config.addresses.iter().copied().collect();
        let tokens = config.tokens.iter().copied().collect();
        let encoded = postcard::to_allocvec(&config)
            .map_err(|error| ProcessorError::Input(error.to_string()))?;
        let id = ProcessorId::new("erc20-balances")
            .map_err(|error| ProcessorError::Input(error.to_string()))?;
        let version = Version::new(1, 0, 0);
        let config_hash = BlockHash::new(*blake3::hash(&encoded).as_bytes());
        let descriptor = ProcessorDescriptor {
            instance: ProcessorInstanceId::legacy(&id, &version, config_hash),
            id,
            version,
            code_hash: BlockHash::new(*blake3::hash(b"leani/erc20-balances/1.0.0").as_bytes()),
            config_hash,
            start: StartPoint::Block(config.start_block),
            requirements: vec![DataRequirement {
                capabilities: CapabilitySet::of(Capability::Logs),
                log_fields: leani_primitives::LogFieldSet::NONE,
                allow_filtered: true,
                filter: FilterScope {
                    addresses: config.tokens.clone(),
                    topics: vec![TopicFilter {
                        position: 0,
                        alternatives: vec![transfer_topic().0],
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
                delta_version: 1,
                entity_schema: "erc20.balance.entity.v1".to_owned(),
                change_schema: "erc20.balance.change.v1".to_owned(),
            },
        };
        descriptor
            .validate()
            .map_err(|error| ProcessorError::Input(error.to_owned()))?;
        Ok(Self {
            config,
            watched,
            tokens,
            descriptor,
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
}

#[async_trait]
impl Processor for Erc20BalanceProcessor {
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
        let signature = transfer_topic().0;
        let mut events = Vec::new();
        for log in accepted_logs(&block.logs)? {
            if log.topics.first() != Some(&signature)
                || log.topics.len() != 3
                || log.data.len() != 32
                || (!self.tokens.is_empty() && !self.tokens.contains(&log.address))
            {
                continue;
            }
            let from = topic_address(log.topics[1])?;
            let to = topic_address(log.topics[2])?;
            if !self.watched.contains(&from) && !self.watched.contains(&to) {
                continue;
            }
            events.push(TransferEvent {
                token: log.address,
                from,
                to,
                value: Quantity::new(log.data.as_slice().try_into().map_err(|_| {
                    ProcessorError::Input("Transfer value is not 32 bytes".to_owned())
                })?),
                log_index: log.log_index,
            });
        }
        events.sort_by_key(|event| event.log_index);
        let payload = postcard::to_allocvec(&TransferDelta { events })
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
        let delta: TransferDelta = postcard::from_bytes(&delta.payload)
            .map_err(|error| ProcessorError::DeltaPayload(error.to_string()))?;
        let mut balances = BTreeMap::<(Address, Address), U256>::new();
        for event in delta.events {
            for address in [event.from, event.to] {
                if address == ZERO_ADDRESS
                    || !self.watched.contains(&address)
                    || balances.contains_key(&(event.token, address))
                {
                    continue;
                }
                let key = balance_key(event.token, address);
                let balance = transaction
                    .state_get(BALANCE_COLLECTION, &key)
                    .await?
                    .as_deref()
                    .map(decode_balance)
                    .transpose()?
                    .map_or(U256::ZERO, |entity| U256::from_be_bytes(entity.balance.0));
                balances.insert((event.token, address), balance);
            }
            let value = U256::from_be_bytes(event.value.0);
            if event.from != ZERO_ADDRESS && self.watched.contains(&event.from) {
                let balance = balances
                    .get_mut(&(event.token, event.from))
                    .ok_or_else(|| {
                        ProcessorError::State("sender balance was not loaded".to_owned())
                    })?;
                *balance = balance.checked_sub(value).ok_or_else(|| {
                    ProcessorError::State(
                        "event-derived balance underflow; start block or token assumptions are incomplete"
                            .to_owned(),
                    )
                })?;
            }
            if event.to != ZERO_ADDRESS && self.watched.contains(&event.to) {
                let balance = balances.get_mut(&(event.token, event.to)).ok_or_else(|| {
                    ProcessorError::State("recipient balance was not loaded".to_owned())
                })?;
                *balance = balance.checked_add(value).ok_or_else(|| {
                    ProcessorError::State("event-derived balance overflow".to_owned())
                })?;
            }
        }
        let mut changes = Vec::with_capacity(balances.len());
        for ((token, address), balance) in balances {
            let key = balance_key(token, address);
            let entity = TokenBalanceEntity {
                token,
                address,
                balance: balance.into(),
                as_of_block: cursor.block_number,
                block_hash: cursor.block_hash,
                finality: cursor.finality,
                coverage_from: self.config.start_block,
                complete: self.config.complete_from_start,
                method: "erc20_transfer_ledger".to_owned(),
            };
            let payload = postcard::to_allocvec(&entity)
                .map_err(|error| ProcessorError::State(error.to_string()))?;
            transaction
                .state_put(BALANCE_COLLECTION, key.clone(), payload.clone())
                .await?;
            transaction
                .put(BALANCE_COLLECTION, key.clone(), payload.clone())
                .await?;
            let change = DomainChange {
                kind: BALANCE_CHANGE_KIND.to_owned(),
                key,
                operation: ChangeOperation::Upsert,
                payload,
            };
            transaction.emit(change.clone()).await?;
            changes.push(change);
        }
        Ok(DomainChanges { changes })
    }

    fn entity_json(
        &self,
        collection: &str,
        _key: &[u8],
        value: &[u8],
    ) -> Result<Option<serde_json::Value>, ProcessorError> {
        if collection != BALANCE_COLLECTION {
            return Ok(None);
        }
        let entity: TokenBalanceEntity = postcard::from_bytes(value)
            .map_err(|error| ProcessorError::State(error.to_string()))?;
        serde_json::to_value(entity)
            .map(Some)
            .map_err(|error| ProcessorError::State(error.to_string()))
    }
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
            "ERC-20 logs are only partially filtered".to_owned(),
        )),
        Material::Missing(reason) => Err(ProcessorError::Input(format!(
            "ERC-20 logs are missing: {reason:?}"
        ))),
    }
}

fn transfer_topic() -> B256 {
    keccak256("Transfer(address,address,uint256)")
}

fn topic_address(topic: [u8; 32]) -> Result<Address, ProcessorError> {
    if topic[..12].iter().any(|byte| *byte != 0) {
        return Err(ProcessorError::Input(
            "indexed ERC-20 address has non-zero padding".to_owned(),
        ));
    }
    Ok(Address::new(topic[12..].try_into().map_err(|_| {
        ProcessorError::Input("indexed address is invalid".to_owned())
    })?))
}

/// Stable store key for one token/account pair.
#[must_use]
pub fn balance_key(token: Address, address: Address) -> Vec<u8> {
    let mut key = Vec::with_capacity(40);
    key.extend_from_slice(&token.0);
    key.extend_from_slice(&address.0);
    key
}

fn decode_balance(bytes: &[u8]) -> Result<TokenBalanceEntity, ProcessorError> {
    postcard::from_bytes(bytes).map_err(|error| ProcessorError::State(error.to_string()))
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
    use leani_primitives::{ChainId, Log, TransactionHash};
    use leani_testkit::{MemoryReducer, fixture_frame};

    use super::*;

    fn topic(address: Address) -> [u8; 32] {
        let mut topic = [0; 32];
        topic[12..].copy_from_slice(&address.0);
        topic
    }

    fn transfer(token: Address, from: Address, to: Address, value: u64, log_index: u32) -> Log {
        Log {
            address: token,
            topics: vec![transfer_topic().0, topic(from), topic(to)],
            data: U256::from(value).to_be_bytes::<32>().to_vec(),
            transaction_hash: Some(TransactionHash::new([7; 32])),
            transaction_index: 0,
            log_index,
        }
    }

    fn cursor(processor: &Erc20BalanceProcessor, frame: &BlockFrame) -> ProcessorCursor {
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

    #[tokio::test]
    async fn mint_and_transfer_produce_declared_watchlist_balances() {
        let alice = Address::new([1; 20]);
        let bob = Address::new([2; 20]);
        let token = Address::new([3; 20]);
        let processor = Erc20BalanceProcessor::new(Erc20BalanceConfig {
            start_block: BlockNumber(1),
            addresses: vec![alice, bob],
            tokens: vec![token],
            complete_from_start: true,
        })
        .expect("processor");
        let mut reducer = MemoryReducer::default();
        let mut first = fixture_frame(1, BlockHash::ZERO);
        first.logs = Material::Complete(vec![transfer(token, ZERO_ADDRESS, alice, 100, 0)]);
        let delta = processor.map(&first).await.expect("map mint");
        processor
            .reduce(&mut reducer, &cursor(&processor, &first), &delta)
            .await
            .expect("reduce mint");
        let mut second = fixture_frame(2, first.block.hash);
        second.logs = Material::Complete(vec![transfer(token, alice, bob, 40, 0)]);
        let delta = processor.map(&second).await.expect("map transfer");
        processor
            .reduce(&mut reducer, &cursor(&processor, &second), &delta)
            .await
            .expect("reduce transfer");
        let alice_entity = decode_balance(
            reducer
                .entity(BALANCE_COLLECTION, &balance_key(token, alice))
                .expect("alice"),
        )
        .expect("decode");
        let bob_entity = decode_balance(
            reducer
                .entity(BALANCE_COLLECTION, &balance_key(token, bob))
                .expect("bob"),
        )
        .expect("decode");
        assert_eq!(U256::from_be_bytes(alice_entity.balance.0), U256::from(60));
        assert_eq!(U256::from_be_bytes(bob_entity.balance.0), U256::from(40));
        assert!(alice_entity.complete);
        assert_eq!(alice_entity.method, "erc20_transfer_ledger");
    }

    #[tokio::test]
    async fn incomplete_start_fails_on_an_outgoing_underflow() {
        let alice = Address::new([1; 20]);
        let token = Address::new([3; 20]);
        let processor = Erc20BalanceProcessor::new(Erc20BalanceConfig {
            start_block: BlockNumber(10),
            addresses: vec![alice],
            tokens: vec![token],
            complete_from_start: false,
        })
        .expect("processor");
        let mut frame = fixture_frame(10, BlockHash::ZERO);
        frame.logs = Material::Complete(vec![transfer(token, alice, ZERO_ADDRESS, 1, 0)]);
        let delta = processor.map(&frame).await.expect("map");
        assert!(matches!(
            processor
                .reduce(
                    &mut MemoryReducer::default(),
                    &cursor(&processor, &frame),
                    &delta
                )
                .await,
            Err(ProcessorError::State(_))
        ));
    }
}
