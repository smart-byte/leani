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
    ///
    /// With a non-empty `tokens` allowlist, a transfer the ledger cannot
    /// apply then fails the processor (fail-closed). Without an allowlist the
    /// flag only sets `complete` on derived balances, and such a transfer
    /// marks its pair incomplete like any other.
    pub complete_from_start: bool,
}

/// Durable declared balance.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TokenBalanceEntity {
    pub token: Address,
    pub address: Address,
    /// Event-derived balance; frozen at its last derived value once
    /// `incomplete_from` is set.
    pub balance: Quantity,
    pub as_of_block: BlockNumber,
    pub block_hash: BlockHash,
    pub finality: Finality,
    pub coverage_from: BlockNumber,
    pub complete: bool,
    /// Block whose transfer the ledger could not apply: an outgoing transfer
    /// above the derived balance, or an incoming one that overflows it. The
    /// pair is incomplete and no longer derived from this block on.
    pub incomplete_from: Option<BlockNumber>,
    pub method: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct TransferDelta {
    events: Vec<TransferEvent>,
    /// `Transfer` logs skipped because they are not canonical ERC-20
    /// transfers.
    skipped_logs: u32,
}

/// One watched pair's ledger while a block is reduced.
struct PairLedger {
    balance: U256,
    incomplete_from: Option<BlockNumber>,
    /// Marked incomplete by an earlier block: never derived or written again.
    frozen: bool,
}

impl PairLedger {
    fn new(stored: Option<&TokenBalanceEntity>) -> Self {
        let incomplete_from = stored.and_then(|entity| entity.incomplete_from);
        Self {
            balance: stored.map_or(U256::ZERO, |entity| U256::from_be_bytes(entity.balance.0)),
            incomplete_from,
            frozen: incomplete_from.is_some(),
        }
    }
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
        let version = Version::new(1, 1, 0);
        let config_hash = BlockHash::new(*blake3::hash(&encoded).as_bytes());
        let descriptor = ProcessorDescriptor {
            instance: ProcessorInstanceId::legacy(&id, &version, config_hash)
                .map_err(|error| ProcessorError::Input(error.to_string()))?,
            id,
            version,
            code_hash: BlockHash::new(*blake3::hash(b"leani/erc20-balances/1.1.0").as_bytes()),
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
                minimum_finality: Finality::Included,
            }],
            mode: ReductionMode::OrderedState,
            delivery_ordering: DeliveryOrdering::Canonical,
            publication: PublicationPolicy::IncludedAndFinalized,
            lifecycle: LifecyclePolicies::from_legacy(RetentionPolicy::LatestState),
            schemas: ProcessorSchemas {
                delta_version: 2,
                entity_schema: "erc20.balance.entity.v2".to_owned(),
                change_schema: "erc20.balance.change.v2".to_owned(),
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

    /// Record one side of a transfer on a watched pair's ledger.
    ///
    /// A balance the transfer cannot produce proves the ledger incomplete for
    /// the pair, through transfers before the start block or a token that is
    /// not a plain ERC-20 ledger, so the pair stops being derived at `block`.
    /// Only an allowlisted token declared complete from the start fails.
    fn update_balance(
        &self,
        ledger: &mut PairLedger,
        token: Address,
        block: BlockNumber,
        balance: Option<U256>,
        failure: &str,
    ) -> Result<(), ProcessorError> {
        if ledger.incomplete_from.is_some() {
            return Ok(());
        }
        match balance {
            Some(balance) => ledger.balance = balance,
            None if self.config.complete_from_start && self.tokens.contains(&token) => {
                return Err(ProcessorError::State(failure.to_owned()));
            }
            None => ledger.incomplete_from = Some(block),
        }
        Ok(())
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
        let mut skipped_logs = 0_u32;
        for log in accepted_logs(&block.logs)? {
            if log.topics.first() != Some(&signature)
                || (!self.tokens.is_empty() && !self.tokens.contains(&log.address))
            {
                continue;
            }
            // Any contract can emit this signature, for example an ERC-721
            // transfer, so a log that does not decode is counted, not fatal.
            let Some((from, to, value)) = transfer_fields(log) else {
                skipped_logs = skipped_logs.saturating_add(1);
                continue;
            };
            if !self.watched.contains(&from) && !self.watched.contains(&to) {
                continue;
            }
            events.push(TransferEvent {
                token: log.address,
                from,
                to,
                value,
                log_index: log.log_index,
            });
        }
        events.sort_by_key(|event| event.log_index);
        let payload = postcard::to_allocvec(&TransferDelta {
            events,
            skipped_logs,
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
        let delta: TransferDelta = postcard::from_bytes(&delta.payload)
            .map_err(|error| ProcessorError::DeltaPayload(error.to_string()))?;
        let mut ledgers = BTreeMap::<(Address, Address), PairLedger>::new();
        for event in delta.events {
            for address in [event.from, event.to] {
                if address == ZERO_ADDRESS
                    || !self.watched.contains(&address)
                    || ledgers.contains_key(&(event.token, address))
                {
                    continue;
                }
                let key = balance_key(event.token, address);
                let stored = transaction
                    .state_get(BALANCE_COLLECTION, &key)
                    .await?
                    .as_deref()
                    .map(decode_balance)
                    .transpose()?;
                ledgers.insert((event.token, address), PairLedger::new(stored.as_ref()));
            }
            let value = U256::from_be_bytes(event.value.0);
            if event.from != ZERO_ADDRESS && self.watched.contains(&event.from) {
                let ledger = ledgers.get_mut(&(event.token, event.from)).ok_or_else(|| {
                    ProcessorError::State("sender balance was not loaded".to_owned())
                })?;
                let balance = ledger.balance.checked_sub(value);
                self.update_balance(
                    ledger,
                    event.token,
                    cursor.block_number,
                    balance,
                    "event-derived balance underflow; start block or token assumptions are incomplete",
                )?;
            }
            if event.to != ZERO_ADDRESS && self.watched.contains(&event.to) {
                let ledger = ledgers.get_mut(&(event.token, event.to)).ok_or_else(|| {
                    ProcessorError::State("recipient balance was not loaded".to_owned())
                })?;
                let balance = ledger.balance.checked_add(value);
                self.update_balance(
                    ledger,
                    event.token,
                    cursor.block_number,
                    balance,
                    "event-derived balance overflow",
                )?;
            }
        }
        let mut changes = Vec::with_capacity(ledgers.len());
        for ((token, address), ledger) in ledgers {
            if ledger.frozen {
                continue;
            }
            let key = balance_key(token, address);
            let entity = TokenBalanceEntity {
                token,
                address,
                balance: ledger.balance.into(),
                as_of_block: cursor.block_number,
                block_hash: cursor.block_hash,
                finality: cursor.finality,
                coverage_from: self.config.start_block,
                complete: self.config.complete_from_start && ledger.incomplete_from.is_none(),
                incomplete_from: ledger.incomplete_from,
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

/// Sender, recipient, and value of a canonical ERC-20 `Transfer`: exactly
/// three topics with zero-padded addresses and one 32-byte value.
fn transfer_fields(log: &leani_primitives::Log) -> Option<(Address, Address, Quantity)> {
    let [_, from, to] = log.topics.as_slice() else {
        return None;
    };
    Some((
        topic_address(*from)?,
        topic_address(*to)?,
        Quantity::new(log.data.as_slice().try_into().ok()?),
    ))
}

fn topic_address(topic: [u8; 32]) -> Option<Address> {
    if topic[..12].iter().any(|byte| *byte != 0) {
        return None;
    }
    topic[12..].try_into().ok().map(Address::new)
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
    use leani_store_sqlite::{SqliteStore, StoreConfig};
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

    fn included_frame(number: u64, parent: BlockHash, logs: Vec<Log>) -> BlockFrame {
        let mut frame = fixture_frame(number, parent);
        frame.finality = Finality::Included;
        frame.logs = Material::Complete(logs);
        frame
    }

    async fn open_store(directory: &tempfile::TempDir) -> SqliteStore {
        SqliteStore::open(StoreConfig::new(directory.path().join("erc20.sqlite")))
            .await
            .expect("store")
    }

    async fn apply(
        store: &SqliteStore,
        processor: &Erc20BalanceProcessor,
        frame: &BlockFrame,
    ) -> Result<(), leani_store_sqlite::StoreError> {
        let delta = processor.map(frame).await.expect("map");
        store
            .apply(processor, cursor(processor, frame), &delta, &[])
            .await
            .map(drop)
    }

    async fn stored_balance(
        store: &SqliteStore,
        processor: &Erc20BalanceProcessor,
        token: Address,
        holder: Address,
    ) -> TokenBalanceEntity {
        let bytes = store
            .entity(
                processor.descriptor(),
                BALANCE_COLLECTION,
                &balance_key(token, holder),
            )
            .await
            .expect("read balance")
            .expect("stored balance");
        decode_balance(&bytes).expect("decode balance")
    }

    fn memory_balance(
        reducer: &MemoryReducer,
        token: Address,
        holder: Address,
    ) -> TokenBalanceEntity {
        decode_balance(
            reducer
                .entity(BALANCE_COLLECTION, &balance_key(token, holder))
                .expect("balance"),
        )
        .expect("decode balance")
    }

    #[tokio::test]
    async fn unsolicited_transfer_marks_the_pair_incomplete_in_a_real_store() {
        // Audit probe (C4): anyone can emit `Transfer(watched, x, 1)` from a
        // token that is not allowlisted, and the underflow failed the block.
        let holder = Address::new([0x22; 20]);
        let other = Address::new([0x33; 20]);
        let token = Address::new([0x11; 20]);
        let processor = Erc20BalanceProcessor::new(Erc20BalanceConfig {
            start_block: BlockNumber(1),
            addresses: vec![holder],
            tokens: Vec::new(),
            complete_from_start: false,
        })
        .expect("processor");
        let directory = tempfile::tempdir().expect("directory");
        let store = open_store(&directory).await;
        let first = included_frame(
            1,
            BlockHash::ZERO,
            vec![transfer(token, holder, other, 1, 0)],
        );
        apply(&store, &processor, &first)
            .await
            .expect("an unsolicited transfer does not fail the block");
        let marked = stored_balance(&store, &processor, token, holder).await;
        assert_eq!(marked.incomplete_from, Some(BlockNumber(1)));
        assert!(!marked.complete);
        assert_eq!(U256::from_be_bytes(marked.balance.0), U256::ZERO);

        // The pair is no longer derived: a later transfer leaves it as it is.
        let second = included_frame(
            2,
            first.block.hash,
            vec![transfer(token, other, holder, 5, 0)],
        );
        apply(&store, &processor, &second)
            .await
            .expect("apply a later transfer");
        assert_eq!(
            stored_balance(&store, &processor, token, holder).await,
            marked
        );
    }

    #[tokio::test]
    async fn malformed_transfer_logs_are_skipped_and_counted() {
        let alice = Address::new([1; 20]);
        let bob = Address::new([2; 20]);
        let token = Address::new([3; 20]);
        let processor = Erc20BalanceProcessor::new(Erc20BalanceConfig {
            start_block: BlockNumber(1),
            addresses: vec![alice],
            tokens: Vec::new(),
            complete_from_start: false,
        })
        .expect("processor");
        // Audit probe: dirty address padding failed the whole block.
        let mut dirty = transfer(token, bob, alice, 7, 0);
        dirty.topics[1][0] = 1;
        // An ERC-721 `Transfer` shares the signature: four topics, no data.
        let mut erc721 = transfer(token, bob, alice, 8, 1);
        erc721.topics.push([0; 32]);
        erc721.data.clear();
        let mut frame = fixture_frame(1, BlockHash::ZERO);
        frame.logs = Material::Complete(vec![dirty, erc721, transfer(token, bob, alice, 9, 2)]);
        let delta = processor
            .map(&frame)
            .await
            .expect("malformed transfers do not fail the block");
        let decoded: TransferDelta = postcard::from_bytes(&delta.payload).expect("delta");
        assert_eq!(decoded.skipped_logs, 2);
        assert_eq!(decoded.events.len(), 1);
        assert_eq!(decoded.events[0].log_index, 2);
    }

    #[tokio::test]
    async fn only_an_allowlisted_complete_token_fails_closed_on_underflow() {
        let alice = Address::new([1; 20]);
        let token = Address::new([3; 20]);
        let mut frame = fixture_frame(10, BlockHash::ZERO);
        frame.logs = Material::Complete(vec![transfer(token, alice, ZERO_ADDRESS, 1, 0)]);

        // The operator declared this allowlisted token complete from the
        // start, so a missing inflow breaks that declaration.
        let strict = Erc20BalanceProcessor::new(Erc20BalanceConfig {
            start_block: BlockNumber(10),
            addresses: vec![alice],
            tokens: vec![token],
            complete_from_start: true,
        })
        .expect("processor");
        let delta = strict.map(&frame).await.expect("map");
        let error = strict
            .reduce(
                &mut MemoryReducer::default(),
                &cursor(&strict, &frame),
                &delta,
            )
            .await
            .expect_err("an allowlisted complete token fails closed");
        assert!(matches!(&error, ProcessorError::State(detail) if detail.contains("underflow")));

        // The same declaration without an allowlist covers foreign tokens,
        // whose transfers prove nothing about the watched ledger.
        let open = Erc20BalanceProcessor::new(Erc20BalanceConfig {
            start_block: BlockNumber(10),
            addresses: vec![alice],
            tokens: Vec::new(),
            complete_from_start: true,
        })
        .expect("processor");
        let delta = open.map(&frame).await.expect("map");
        let mut reducer = MemoryReducer::default();
        open.reduce(&mut reducer, &cursor(&open, &frame), &delta)
            .await
            .expect("an unlisted token marks the pair incomplete");
        let entity = memory_balance(&reducer, token, alice);
        assert_eq!(entity.incomplete_from, Some(BlockNumber(10)));
        assert!(!entity.complete);
    }

    #[tokio::test]
    async fn an_incomplete_start_marks_underflow_and_overflow_incomplete() {
        let alice = Address::new([1; 20]);
        let bob = Address::new([2; 20]);
        let token = Address::new([3; 20]);
        let processor = Erc20BalanceProcessor::new(Erc20BalanceConfig {
            start_block: BlockNumber(10),
            addresses: vec![alice, bob],
            tokens: vec![token],
            complete_from_start: false,
        })
        .expect("processor");
        let mut overflow = transfer(token, ZERO_ADDRESS, bob, 0, 3);
        overflow.data = U256::MAX.to_be_bytes::<32>().to_vec();
        let mut frame = fixture_frame(10, BlockHash::ZERO);
        frame.logs = Material::Complete(vec![
            transfer(token, ZERO_ADDRESS, alice, 4, 0),
            transfer(token, alice, ZERO_ADDRESS, 5, 1),
            transfer(token, ZERO_ADDRESS, bob, 1, 2),
            overflow,
            // Both pairs stopped being derived; these are ignored.
            transfer(token, ZERO_ADDRESS, alice, 100, 4),
            transfer(token, ZERO_ADDRESS, bob, 100, 5),
        ]);
        let delta = processor.map(&frame).await.expect("map");
        let mut reducer = MemoryReducer::default();
        processor
            .reduce(&mut reducer, &cursor(&processor, &frame), &delta)
            .await
            .expect("an incomplete start never fails the block");
        for (holder, balance) in [(alice, 4_u64), (bob, 1)] {
            let entity = memory_balance(&reducer, token, holder);
            assert_eq!(entity.incomplete_from, Some(BlockNumber(10)));
            assert!(!entity.complete);
            assert_eq!(U256::from_be_bytes(entity.balance.0), U256::from(balance));
        }
    }

    #[tokio::test]
    async fn undoing_the_block_that_marked_a_pair_incomplete_restores_it() {
        let alice = Address::new([1; 20]);
        let bob = Address::new([2; 20]);
        let token = Address::new([3; 20]);
        let processor = Erc20BalanceProcessor::new(Erc20BalanceConfig {
            start_block: BlockNumber(1),
            addresses: vec![alice],
            tokens: Vec::new(),
            complete_from_start: false,
        })
        .expect("processor");
        let directory = tempfile::tempdir().expect("directory");
        let store = open_store(&directory).await;
        let first = included_frame(
            1,
            BlockHash::ZERO,
            vec![transfer(token, ZERO_ADDRESS, alice, 10, 0)],
        );
        apply(&store, &processor, &first).await.expect("mint");
        let derived = stored_balance(&store, &processor, token, alice).await;
        let second = included_frame(
            2,
            first.block.hash,
            vec![transfer(token, alice, bob, 11, 0)],
        );
        apply(&store, &processor, &second)
            .await
            .expect("an underflow marks the pair");
        let marked = stored_balance(&store, &processor, token, alice).await;
        assert_eq!(marked.incomplete_from, Some(BlockNumber(2)));
        assert_eq!(U256::from_be_bytes(marked.balance.0), U256::from(10));

        store
            .undo(
                processor.descriptor(),
                second.chain_id,
                second.block.number,
                second.block.hash,
                &[],
            )
            .await
            .expect("undo the marking block");
        assert_eq!(
            stored_balance(&store, &processor, token, alice).await,
            derived
        );

        // The restored working state derives the replacement block again.
        let mut replacement =
            included_frame(2, first.block.hash, vec![transfer(token, alice, bob, 3, 0)]);
        replacement.block.hash = BlockHash::new([0x42; 32]);
        apply(&store, &processor, &replacement)
            .await
            .expect("replacement block");
        let replaced = stored_balance(&store, &processor, token, alice).await;
        assert_eq!(replaced.incomplete_from, None);
        assert_eq!(U256::from_be_bytes(replaced.balance.0), U256::from(7));
    }
}
