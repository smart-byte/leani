//! Declarative, deterministic EVM event decoding without EVM execution.

mod abi;

use std::collections::{BTreeMap, BTreeSet};

use alloy_primitives::keccak256;
use async_trait::async_trait;
use leani_primitives::{
    Address, BlockFrame, BlockHash, BlockNumber, Capability, CapabilitySet, Completeness,
    FilterScope, Finality, Material, ProcessorCursor, TopicFilter, TransactionHash,
};
use leani_processor_api::{
    ChangeOperation, DataRequirement, DeliveryOrdering, DomainChange, DomainChanges, EncodedDelta,
    LifecyclePolicies, Processor, ProcessorDescriptor, ProcessorError, ProcessorId,
    ProcessorInstanceId, ProcessorSchemas, PublicationPolicy, ReducerTransaction, ReductionMode,
    RetentionPolicy, StartPoint,
};
use semver::Version;
use serde::{Deserialize, Serialize};

pub const EVM_EVENTS_VERSION: &str = "1.1.0";

/// Declarative event processor configuration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EvmEventsConfig {
    pub start_block: BlockNumber,
    /// Empty means every contract emitting one of the configured signatures.
    pub addresses: Vec<Address>,
    pub events: Vec<EventDefinition>,
}

/// One Solidity event ABI fragment and public output mapping.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EventDefinition {
    /// A human-readable fragment such as
    /// `event Sync(uint112 reserve0, uint112 reserve1)`, one JSON ABI event
    /// object, or `log0`, which stores the configured contracts' topicless
    /// logs raw.
    pub abi: String,
    pub output: EventOutput,
}

/// Retained entity/change mapping for one decoded event.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EventOutput {
    pub collection: String,
    pub kind: String,
    /// Field names used to construct a deterministic key. Empty uses
    /// transaction hash plus log index, which no other event shares. A key
    /// from these fields can repeat across blocks, so any keyed output makes
    /// the processor reduce blocks in chain order, and a key holds its
    /// newest event.
    #[serde(default)]
    pub key_fields: Vec<String>,
    /// Optional canonical-block timestamp bucket.
    #[serde(default)]
    pub bucket_seconds: Option<u64>,
}

/// Stable public entity for one decoded log.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DecodedEvent {
    pub schema_version: u16,
    pub contract: Address,
    pub event: String,
    pub signature: String,
    pub block_number: BlockNumber,
    pub block_hash: BlockHash,
    pub block_timestamp: u64,
    pub bucket_start_timestamp: Option<u64>,
    pub transaction_hash: TransactionHash,
    pub transaction_index: u32,
    pub log_index: u32,
    pub finality: Finality,
    pub values: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct EventDelta {
    events: Vec<MappedEvent>,
    /// Logs with a configured signature that do not decode as its event.
    skipped_logs: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct MappedEvent {
    collection: String,
    kind: String,
    key: Vec<u8>,
    entity: DecodedEvent,
}

#[derive(Clone, Debug)]
struct ParsedEvent {
    name: String,
    signature: String,
    /// `None` for the `log0` entry, whose logs are stored raw.
    abi: Option<abi::EventAbi>,
    output: EventOutput,
}

/// The `abi` value that stores topicless logs raw.
const LOG0: &str = "log0";

/// Native declarative EVM event processor.
#[derive(Clone, Debug)]
pub struct EvmEventsProcessor {
    descriptor: ProcessorDescriptor,
    addresses: BTreeSet<Address>,
    events: BTreeMap<[u8; 32], ParsedEvent>,
    log0: Option<ParsedEvent>,
}

impl EvmEventsProcessor {
    /// Parse and validate event ABI fragments.
    ///
    /// Unnamed parameters, anonymous events, unsupported or too deeply
    /// nested types, duplicate signatures, invalid output names, and
    /// impossible key/bucket mappings fail at construction before a source
    /// range is opened.
    ///
    /// # Errors
    ///
    /// Returns a deterministic configuration error.
    pub fn new(mut config: EvmEventsConfig) -> Result<Self, ProcessorError> {
        config.addresses.sort();
        config.addresses.dedup();
        if config.events.is_empty() {
            return Err(input_error("at least one event definition is required"));
        }
        let mut events = BTreeMap::new();
        let mut log0 = None;
        for definition in &config.events {
            let event = parse_event(definition)?;
            if event.abi.is_none() {
                if log0.replace(event).is_some() {
                    return Err(input_error("at most one `log0` entry is allowed"));
                }
            } else if events
                .insert(keccak256(event.signature.as_bytes()).0, event)
                .is_some()
            {
                return Err(input_error("event signatures must be unique"));
            }
        }
        if log0.is_some() && config.addresses.is_empty() {
            return Err(input_error(
                "a `log0` entry requires `addresses`: a topicless log has no signature to select it by",
            ));
        }
        // Events are hashed in configured order, so reordering them creates a
        // new instance identity. Sorting them here would change the identity
        // of every existing instance.
        let normalized = postcard::to_allocvec(&config)
            .map_err(|error| input_error(format!("configuration encoding failed: {error}")))?;
        let id = ProcessorId::new("evm-events").map_err(|error| input_error(error.to_string()))?;
        let version = Version::new(1, 1, 0);
        let config_hash = BlockHash::new(*blake3::hash(&normalized).as_bytes());
        // A topicless log cannot match a topic-zero filter, so a `log0` entry
        // reads every log of the configured contracts.
        let topics = if log0.is_some() {
            Vec::new()
        } else {
            vec![TopicFilter {
                position: 0,
                alternatives: events.keys().copied().collect(),
            }]
        };
        // A configured key can repeat across blocks, so which event it holds
        // depends on the order its blocks reduce in, and an undo restores the
        // value from before the block. Keyed output is therefore ordered, and
        // its live lane waits for its history. The default key is unique to
        // one event, so keyless output stays block-local.
        let (mode, delivery_ordering) = if events
            .values()
            .chain(&log0)
            .any(|event| !event.output.key_fields.is_empty())
        {
            (ReductionMode::OrderedState, DeliveryOrdering::Canonical)
        } else {
            (
                ReductionMode::BlockLocal,
                DeliveryOrdering::BlockVersionedIdempotent,
            )
        };
        let descriptor = ProcessorDescriptor {
            instance: ProcessorInstanceId::legacy(&id, &version, config_hash)
                .map_err(|error| input_error(error.to_string()))?,
            id,
            version,
            code_hash: BlockHash::new(*blake3::hash(b"leani/evm-events/1.1.0").as_bytes()),
            config_hash,
            start: StartPoint::Block(config.start_block),
            requirements: vec![DataRequirement {
                capabilities: CapabilitySet::of(Capability::Logs),
                log_fields: leani_primitives::LogFieldSet::ALL,
                allow_filtered: true,
                filter: FilterScope {
                    addresses: config.addresses.clone(),
                    topics,
                    ..FilterScope::default()
                },
                minimum_finality: Finality::Unattested,
            }],
            mode,
            delivery_ordering,
            publication: PublicationPolicy::Attested,
            lifecycle: LifecyclePolicies::from_legacy(RetentionPolicy::FullOutputHistory),
            schemas: ProcessorSchemas {
                delta_version: 2,
                entity_schema: "evm.event.entity.v1".to_owned(),
                change_schema: "evm.event.change.v1".to_owned(),
            },
        };
        descriptor
            .validate()
            .map_err(|error| input_error(error.to_owned()))?;
        Ok(Self {
            descriptor,
            addresses: config.addresses.into_iter().collect(),
            events,
            log0,
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
impl Processor for EvmEventsProcessor {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn descriptor(&self) -> &ProcessorDescriptor {
        &self.descriptor
    }

    async fn map(&self, block: &BlockFrame) -> Result<EncodedDelta, ProcessorError> {
        self.descriptor.requirements[0]
            .validate_frame(block)
            .map_err(|error| input_error(error.to_owned()))?;
        let logs = accepted_logs(&block.logs)?;
        let mut events = Vec::new();
        let mut skipped_logs = 0_u32;
        for log in logs {
            if !self.addresses.is_empty() && !self.addresses.contains(&log.address) {
                continue;
            }
            let event = match log.topics.first() {
                Some(topic0) => self.events.get(topic0),
                None => self.log0.as_ref(),
            };
            let Some(event) = event else {
                continue;
            };
            let transaction_hash = log.transaction_hash.ok_or_else(|| {
                input_error(format!(
                    "{} log {} omits its required transaction hash",
                    event.signature, log.log_index
                ))
            })?;
            let values = match &event.abi {
                Some(abi) => {
                    // Another contract can emit this signature with another
                    // layout, such as an ERC-721 `Transfer` for the ERC-20
                    // ABI. A log that does not decode is not this event, so
                    // it is counted, not fatal.
                    let Ok(values) = abi.decode(&log.topics, &log.data) else {
                        skipped_logs = skipped_logs.saturating_add(1);
                        continue;
                    };
                    values
                }
                None => {
                    BTreeMap::from([("data".to_owned(), format!("0x{}", hex::encode(&log.data)))])
                }
            };
            let bucket_start_timestamp = event
                .output
                .bucket_seconds
                .map(|seconds| block.block.timestamp / seconds * seconds);
            let entity = DecodedEvent {
                schema_version: 1,
                contract: log.address,
                event: event.name.clone(),
                signature: event.signature.clone(),
                block_number: block.block.number,
                block_hash: block.block.hash,
                block_timestamp: block.block.timestamp,
                bucket_start_timestamp,
                transaction_hash,
                transaction_index: log.transaction_index,
                log_index: log.log_index,
                finality: block.finality,
                values,
            };
            let key = output_key(event, &entity)?;
            events.push(MappedEvent {
                collection: event.output.collection.clone(),
                kind: event.output.kind.clone(),
                key,
                entity,
            });
        }
        events.sort_by_key(|event| {
            (
                event.entity.transaction_index,
                event.entity.log_index,
                event.kind.clone(),
            )
        });
        let payload = postcard::to_allocvec(&EventDelta {
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

    fn finality_variant_checksums(
        &self,
        delta: &EncodedDelta,
    ) -> Result<Vec<BlockHash>, ProcessorError> {
        delta.validate(&self.descriptor)?;
        let decoded: EventDelta = postcard::from_bytes(&delta.payload)
            .map_err(|error| ProcessorError::DeltaPayload(error.to_string()))?;
        let mut checksums = Vec::with_capacity(3);
        for finality in [
            Finality::Unattested,
            Finality::Attested,
            Finality::Finalized,
        ] {
            let mut variant = decoded.clone();
            for event in &mut variant.events {
                event.entity.finality = finality;
            }
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
        validate_cursor(&self.descriptor, cursor, delta)?;
        let delta: EventDelta = postcard::from_bytes(&delta.payload)
            .map_err(|error| ProcessorError::DeltaPayload(error.to_string()))?;
        // Each emitted change must match its key's net mutation in the block,
        // so a key keeps only its last event, in log order. Keyed blocks
        // reduce in chain order, so that event is the key's newest.
        let mut last_event = BTreeMap::new();
        for (index, event) in delta.events.iter().enumerate() {
            last_event.insert((&event.collection, &event.key), index);
        }
        let mut changes = Vec::with_capacity(last_event.len());
        for (index, event) in delta.events.iter().enumerate() {
            if last_event.get(&(&event.collection, &event.key)) != Some(&index) {
                continue;
            }
            let payload = postcard::to_allocvec(&event.entity)
                .map_err(|error| ProcessorError::State(error.to_string()))?;
            transaction
                .put(&event.collection, event.key.clone(), payload.clone())
                .await?;
            let change = DomainChange {
                kind: event.kind.clone(),
                key: event.key.clone(),
                operation: ChangeOperation::Upsert,
                payload,
            };
            transaction.emit(change.clone()).await?;
            changes.push(change);
        }
        Ok(DomainChanges { changes })
    }

    fn change_json(
        &self,
        change: &DomainChange,
    ) -> Result<Option<serde_json::Value>, ProcessorError> {
        decode_entity_json(&change.payload).map(Some)
    }

    fn entity_json(
        &self,
        _collection: &str,
        _key: &[u8],
        value: &[u8],
    ) -> Result<Option<serde_json::Value>, ProcessorError> {
        decode_entity_json(value).map(Some)
    }
}

fn parse_event(definition: &EventDefinition) -> Result<ParsedEvent, ProcessorError> {
    validate_output(&definition.output)?;
    let (name, signature, abi, keys) = if definition.abi.trim() == LOG0 {
        let keys = BTreeSet::from(["data".to_owned()]);
        (LOG0.to_owned(), LOG0.to_owned(), None, keys)
    } else {
        let abi = abi::parse(&definition.abi).map_err(input_error)?;
        let keys = abi.value_keys();
        (abi.name.clone(), abi.signature(), Some(abi), keys)
    };
    for key in &definition.output.key_fields {
        if key != "_bucket_start" && !keys.contains(key) {
            return Err(input_error(format!(
                "output key field `{key}` is not declared by the event ABI; its value keys are {}",
                keys.iter().cloned().collect::<Vec<_>>().join(", ")
            )));
        }
        if key == "_bucket_start" && definition.output.bucket_seconds.is_none() {
            return Err(input_error(
                "_bucket_start key requires output.bucket_seconds",
            ));
        }
    }
    Ok(ParsedEvent {
        name,
        signature,
        abi,
        output: definition.output.clone(),
    })
}

fn validate_output(output: &EventOutput) -> Result<(), ProcessorError> {
    for (field, value) in [
        ("collection", output.collection.as_str()),
        ("kind", output.kind.as_str()),
    ] {
        if value.is_empty()
            || value.len() > 128
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        {
            return Err(input_error(format!(
                "output {field} must be a portable 1-128 character name"
            )));
        }
    }
    if output.bucket_seconds == Some(0) {
        return Err(input_error("output bucket must be greater than zero"));
    }
    let mut keys = BTreeSet::new();
    if output
        .key_fields
        .iter()
        .any(|field| !keys.insert(field.as_str()))
    {
        return Err(input_error("output key fields must be unique"));
    }
    Ok(())
}

fn output_key(event: &ParsedEvent, entity: &DecodedEvent) -> Result<Vec<u8>, ProcessorError> {
    if event.output.key_fields.is_empty() {
        let mut key = Vec::with_capacity(36);
        key.extend_from_slice(&entity.transaction_hash.0);
        key.extend_from_slice(&entity.log_index.to_be_bytes());
        return Ok(key);
    }
    let mut hasher = blake3::Hasher::new();
    for field in &event.output.key_fields {
        let value = if field == "_bucket_start" {
            entity
                .bucket_start_timestamp
                .map(|value| value.to_string())
                .ok_or_else(|| input_error("bucket key is unavailable"))?
        } else {
            entity
                .values
                .get(field)
                .cloned()
                .ok_or_else(|| input_error(format!("key field `{field}` is unavailable")))?
        };
        hasher.update(&u64::try_from(field.len()).unwrap_or(u64::MAX).to_be_bytes());
        hasher.update(field.as_bytes());
        hasher.update(&u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
        hasher.update(value.as_bytes());
    }
    Ok(hasher.finalize().as_bytes().to_vec())
}

fn decode_entity_json(bytes: &[u8]) -> Result<serde_json::Value, ProcessorError> {
    let event: DecodedEvent =
        postcard::from_bytes(bytes).map_err(|error| ProcessorError::State(error.to_string()))?;
    let mut json = serde_json::json!({
        "schemaVersion": event.schema_version,
        "contract": event.contract.to_string(),
        "event": event.event,
        "signature": event.signature,
        "blockNumber": event.block_number.0,
        "blockHash": event.block_hash.to_string(),
        "blockTimestamp": event.block_timestamp,
        "transactionHash": event.transaction_hash.to_string(),
        "transactionIndex": event.transaction_index,
        "logIndex": event.log_index,
        "finality": event.finality.name(),
        "values": event.values,
    });
    if let Some(bucket) = event.bucket_start_timestamp {
        json["bucketStartTimestamp"] = serde_json::Value::from(bucket);
    }
    Ok(json)
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
        } => Err(input_error("event log projection is partial")),
        Material::Missing(reason) => {
            Err(input_error(format!("event logs are missing: {reason:?}")))
        }
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

fn input_error(detail: impl Into<String>) -> ProcessorError {
    ProcessorError::Input(detail.into())
}

#[cfg(test)]
mod tests {
    use leani_primitives::{BlockRef, ChainId, Log, Material, TransactionHash, VerificationReport};
    use leani_store_sqlite::{ChangeDirection, SqliteStore, StoreConfig, StoreError};
    use leani_testkit::{MemoryReducer, fixture_frame};

    use super::*;

    #[test]
    fn change_json_renders_camel_case_hex_public_shape() {
        let mut values = std::collections::BTreeMap::new();
        values.insert(
            "from".to_owned(),
            "0x28c6c06298d514db089934071355e5743bf21d60".to_owned(),
        );
        values.insert("value".to_owned(), "1250000000000000000".to_owned());
        let event = DecodedEvent {
            schema_version: 1,
            contract: Address::new([0xc0; 20]),
            event: "Transfer".to_owned(),
            signature: "Transfer(address,address,uint256)".to_owned(),
            block_number: BlockNumber(25_696_396),
            block_hash: BlockHash::new([0xab; 32]),
            block_timestamp: 1_786_025_147,
            bucket_start_timestamp: None,
            transaction_hash: TransactionHash::new([0xcd; 32]),
            transaction_index: 84,
            log_index: 210,
            finality: Finality::Finalized,
            values,
        };
        let encoded = postcard::to_allocvec(&event).expect("encode");
        let json = decode_entity_json(&encoded).expect("render");
        assert_eq!(json["contract"], format!("0x{}", "c0".repeat(20)));
        assert_eq!(json["blockNumber"], 25_696_396);
        assert_eq!(json["blockHash"], format!("0x{}", "ab".repeat(32)));
        assert_eq!(json["transactionHash"], format!("0x{}", "cd".repeat(32)));
        assert_eq!(json["transactionIndex"], 84);
        assert_eq!(json["logIndex"], 210);
        assert_eq!(json["finality"], "finalized");
        assert_eq!(json["schemaVersion"], 1);
        assert_eq!(json["values"]["value"], "1250000000000000000");
        let object = json.as_object().expect("object");
        assert!(!object.contains_key("block_number"), "snake_case leaked");
        assert!(
            !object.contains_key("bucketStartTimestamp"),
            "None must be omitted"
        );
    }

    fn processor() -> EvmEventsProcessor {
        EvmEventsProcessor::new(EvmEventsConfig {
            start_block: BlockNumber(1),
            addresses: vec![Address::new([0x11; 20])],
            events: vec![EventDefinition {
                abi: "event Transfer(address indexed from, address indexed to, uint256 value)"
                    .to_owned(),
                output: EventOutput {
                    collection: "events.transfers".to_owned(),
                    kind: "events.transfer".to_owned(),
                    key_fields: vec!["from".to_owned(), "to".to_owned()],
                    bucket_seconds: Some(60),
                },
            }],
        })
        .expect("processor")
    }

    fn frame(data: Vec<u8>) -> BlockFrame {
        let processor = processor();
        let topic0 = processor.events.keys().next().copied().expect("topic");
        let mut from = [0_u8; 32];
        from[12..].fill(0x22);
        let mut to = [0_u8; 32];
        to[12..].fill(0x33);
        BlockFrame {
            chain_id: ChainId(1),
            block: BlockRef {
                number: BlockNumber(1),
                hash: BlockHash::new([1; 32]),
                parent_hash: BlockHash::ZERO,
                timestamp: 125,
            },
            finality: Finality::Attested,
            header: Material::Missing(leani_primitives::MissingReason::NotRequested),
            transactions: Material::Missing(leani_primitives::MissingReason::NotRequested),
            receipts: Material::Missing(leani_primitives::MissingReason::NotRequested),
            logs: Material::Complete(vec![Log {
                address: Address::new([0x11; 20]),
                topics: vec![topic0, from, to],
                data,
                transaction_hash: Some(TransactionHash::new([0x44; 32])),
                transaction_index: 0,
                log_index: 7,
            }]),
            withdrawals: Material::Missing(leani_primitives::MissingReason::NotRequested),
            blob_sidecars: Material::Missing(leani_primitives::MissingReason::NotRequested),
            traces: Material::Missing(leani_primitives::MissingReason::NotRequested),
            state_diffs: Material::Missing(leani_primitives::MissingReason::NotRequested),
            provenance: Vec::new(),
            verification: VerificationReport::default(),
        }
    }

    #[tokio::test]
    async fn declarative_transfer_decodes_and_reduces_without_an_evm() {
        let processor = processor();
        let mut value = vec![0_u8; 32];
        value[31] = 9;
        let frame = frame(value);
        let delta = processor.map(&frame).await.expect("map");
        let cursor = ProcessorCursor {
            processor_id: processor.descriptor.id.to_string(),
            processor_version: processor.descriptor.version.to_string(),
            chain_id: frame.chain_id,
            block_number: frame.block.number,
            block_hash: frame.block.hash,
            finality: frame.finality,
            sequence: 1,
        };
        let mut transaction = MemoryReducer::default();
        let changes = processor
            .reduce(&mut transaction, &cursor, &delta)
            .await
            .expect("reduce");
        assert_eq!(changes.changes.len(), 1);
        let decoded: DecodedEvent =
            postcard::from_bytes(&changes.changes[0].payload).expect("event");
        assert_eq!(decoded.values["value"], "9");
        assert_eq!(decoded.bucket_start_timestamp, Some(120));
    }

    #[tokio::test]
    async fn truncated_abi_data_is_skipped_and_counted() {
        let processor = processor();
        let delta = processor
            .map(&frame(vec![0; 31]))
            .await
            .expect("truncated ABI data is not this event");
        let decoded: EventDelta = postcard::from_bytes(&delta.payload).expect("delta");
        assert_eq!(decoded.skipped_logs, 1);
        assert!(decoded.events.is_empty());
    }

    fn transfer_topic() -> [u8; 32] {
        keccak256("Transfer(address,address,uint256)").0
    }

    fn transfer_log(value: u8, log_index: u32) -> Log {
        let mut from = [0_u8; 32];
        from[12..].fill(0x22);
        let mut to = [0_u8; 32];
        to[12..].fill(0x33);
        let mut data = vec![0_u8; 32];
        data[31] = value;
        Log {
            address: Address::new([0x11; 20]),
            topics: vec![transfer_topic(), from, to],
            data,
            transaction_hash: Some(TransactionHash::new([0x44; 32])),
            transaction_index: 0,
            log_index,
        }
    }

    fn transfer_frame(number: u64, parent: BlockHash, logs: Vec<Log>) -> BlockFrame {
        let mut frame = fixture_frame(number, parent);
        frame.finality = Finality::Attested;
        frame.logs = Material::Complete(logs);
        frame
    }

    fn transfers(values: &[u8]) -> Vec<Log> {
        values
            .iter()
            .zip(0..)
            .map(|(value, log_index)| transfer_log(*value, log_index))
            .collect()
    }

    fn cursor(processor: &EvmEventsProcessor, frame: &BlockFrame) -> ProcessorCursor {
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

    async fn open_store(directory: &tempfile::TempDir) -> SqliteStore {
        SqliteStore::open(StoreConfig::new(directory.path().join("events.sqlite")))
            .await
            .expect("store")
    }

    async fn apply(store: &SqliteStore, processor: &EvmEventsProcessor, frame: &BlockFrame) {
        let delta = processor.map(frame).await.expect("map");
        store
            .apply(processor, cursor(processor, frame), &delta, &[])
            .await
            .expect("apply");
    }

    async fn transfer_key(processor: &EvmEventsProcessor) -> Vec<u8> {
        let delta = processor
            .map(&transfer_frame(1, BlockHash::ZERO, transfers(&[1])))
            .await
            .expect("map");
        let decoded: EventDelta = postcard::from_bytes(&delta.payload).expect("delta");
        decoded.events[0].key.clone()
    }

    async fn stored_transfer(
        store: &SqliteStore,
        processor: &EvmEventsProcessor,
        key: &[u8],
    ) -> Option<DecodedEvent> {
        store
            .entity(processor.descriptor(), "events.transfers", key)
            .await
            .expect("read entity")
            .map(|bytes| postcard::from_bytes(&bytes).expect("decode entity"))
    }

    async fn applied_changes(
        store: &SqliteStore,
        processor: &EvmEventsProcessor,
        block: u64,
    ) -> usize {
        store
            .changes(processor.descriptor(), ChainId(1), 0, 1_000)
            .await
            .expect("changes")
            .iter()
            .filter(|record| {
                record.block.number == BlockNumber(block)
                    && record.direction == ChangeDirection::Apply
            })
            .count()
    }

    #[tokio::test]
    async fn an_erc721_transfer_with_the_erc20_signature_is_skipped_and_counted() {
        // Audit probe (H20): with no address filter, an ERC-721 `Transfer`
        // (four topics, no data) shares the ERC-20 topic zero and failed the
        // whole block.
        let processor = EvmEventsProcessor::new(EvmEventsConfig {
            start_block: BlockNumber(1),
            addresses: Vec::new(),
            events: vec![EventDefinition {
                abi: "event Transfer(address indexed from, address indexed to, uint256 value)"
                    .to_owned(),
                output: EventOutput {
                    collection: "events.transfers".to_owned(),
                    kind: "events.transfer".to_owned(),
                    key_fields: Vec::new(),
                    bucket_seconds: None,
                },
            }],
        })
        .expect("processor");
        let mut erc721 = transfer_log(1, 0);
        erc721.topics.push([0; 32]);
        erc721.data.clear();
        let frame = transfer_frame(1, BlockHash::ZERO, vec![erc721, transfer_log(2, 1)]);
        let delta = processor
            .map(&frame)
            .await
            .expect("a foreign log shape does not fail the block");
        let decoded: EventDelta = postcard::from_bytes(&delta.payload).expect("delta");
        assert_eq!(decoded.skipped_logs, 1);
        assert_eq!(decoded.events.len(), 1);
        assert_eq!(decoded.events[0].entity.log_index, 1);
    }

    #[tokio::test]
    async fn an_invalid_bool_word_is_skipped_and_counted() {
        let processor = EvmEventsProcessor::new(EvmEventsConfig {
            start_block: BlockNumber(1),
            addresses: Vec::new(),
            events: vec![EventDefinition {
                abi: "event Paused(address indexed account, bool paused)".to_owned(),
                output: EventOutput {
                    collection: "events.pauses".to_owned(),
                    kind: "events.pause".to_owned(),
                    key_fields: Vec::new(),
                    bucket_seconds: None,
                },
            }],
        })
        .expect("processor");
        let topic0 = processor.events.keys().next().copied().expect("topic");
        let paused = |word: u8, log_index: u32| {
            let mut data = vec![0_u8; 32];
            data[31] = word;
            Log {
                address: Address::new([0x11; 20]),
                topics: vec![topic0, [0; 32]],
                data,
                transaction_hash: Some(TransactionHash::new([0x44; 32])),
                transaction_index: 0,
                log_index,
            }
        };
        let frame = transfer_frame(1, BlockHash::ZERO, vec![paused(2, 0), paused(1, 1)]);
        let delta = processor
            .map(&frame)
            .await
            .expect("an invalid word does not fail the block");
        let decoded: EventDelta = postcard::from_bytes(&delta.payload).expect("delta");
        assert_eq!(decoded.skipped_logs, 1);
        assert_eq!(decoded.events.len(), 1);
        assert_eq!(decoded.events[0].entity.values["paused"], "true");
    }

    #[tokio::test]
    async fn repeated_keys_in_one_block_apply_and_undo_in_a_real_store() {
        // Audit probe (H21): two events for a key that already existed failed
        // the block with "0 matching state mutations"; a new key passed only
        // through the store's inverse-delete fallback.
        let processor = processor();
        let key = transfer_key(&processor).await;
        let directory = tempfile::tempdir().expect("directory");
        let store = open_store(&directory).await;
        let first = transfer_frame(1, BlockHash::ZERO, transfers(&[1, 2]));
        apply(&store, &processor, &first).await;
        let second = transfer_frame(2, first.block.hash, transfers(&[3, 4]));
        apply(&store, &processor, &second).await;
        let latest = stored_transfer(&store, &processor, &key)
            .await
            .expect("latest transfer");
        assert_eq!(latest.values["value"], "4");
        // One change per key and block, matching the key's net mutation.
        assert_eq!(applied_changes(&store, &processor, 1).await, 1);
        assert_eq!(applied_changes(&store, &processor, 2).await, 1);

        store
            .undo(
                processor.descriptor(),
                second.chain_id,
                second.block.number,
                second.block.hash,
                &[],
            )
            .await
            .expect("undo the second block");
        let restored = stored_transfer(&store, &processor, &key)
            .await
            .expect("restored transfer");
        assert_eq!(restored.block_number, BlockNumber(1));
        assert_eq!(restored.values["value"], "2");
        store
            .undo(
                processor.descriptor(),
                first.chain_id,
                first.block.number,
                first.block.hash,
                &[],
            )
            .await
            .expect("undo the first block");
        assert_eq!(stored_transfer(&store, &processor, &key).await, None);
    }

    #[test]
    fn keyed_outputs_reduce_in_chain_order_and_keyless_ones_stay_block_local() {
        // Audit finding F1: a `key_fields` key repeats across blocks, but
        // block-local reduction let the hot and cold lanes apply its blocks
        // in any order, and an undo then restored a stale preimage.
        let output = |collection: &str, key_fields: &[&str]| EventOutput {
            collection: collection.to_owned(),
            kind: collection.to_owned(),
            key_fields: key_fields.iter().map(|field| (*field).to_owned()).collect(),
            bucket_seconds: None,
        };
        let ordering = |transfer_keys: &[&str]| {
            let processor = EvmEventsProcessor::new(EvmEventsConfig {
                start_block: BlockNumber(1),
                addresses: Vec::new(),
                events: vec![
                    EventDefinition {
                        abi: "event Transfer(address indexed from, address indexed to, uint256 value)"
                            .to_owned(),
                        output: output("events.transfers", transfer_keys),
                    },
                    EventDefinition {
                        abi: "event Approval(address indexed owner, address indexed spender, uint256 value)"
                            .to_owned(),
                        output: output("events.approvals", &[]),
                    },
                ],
            })
            .expect("processor");
            (
                processor.descriptor.mode,
                processor.descriptor.delivery_ordering,
            )
        };
        // One keyed output orders the whole processor.
        assert_eq!(
            ordering(&["to"]),
            (ReductionMode::OrderedState, DeliveryOrdering::Canonical)
        );
        assert_eq!(
            ordering(&[]),
            (
                ReductionMode::BlockLocal,
                DeliveryOrdering::BlockVersionedIdempotent
            )
        );
    }

    #[tokio::test]
    async fn a_keyed_instance_stored_as_block_local_is_refused() {
        // The ordering is part of the stored descriptor, though not of the
        // instance ID or its hashes: a keyed instance that an earlier build
        // registered block-local no longer matches its descriptor.
        let processor = processor();
        let mut block_local = processor.descriptor().clone();
        block_local.mode = ReductionMode::BlockLocal;
        block_local.delivery_ordering = DeliveryOrdering::BlockVersionedIdempotent;
        let directory = tempfile::tempdir().expect("directory");
        let store = open_store(&directory).await;
        store
            .register_processor(&block_local)
            .await
            .expect("register the block-local descriptor");
        let error = store
            .register_processor(processor.descriptor())
            .await
            .expect_err("another ordering conflicts with the stored descriptor");
        assert!(matches!(error, StoreError::ProcessorIdentity(_)), "{error}");
    }

    #[tokio::test]
    async fn included_and_finalized_deltas_of_one_block_are_equivalent() {
        // Audit probe (M-P3): the mapped payload embeds finality, but only the
        // exact checksum counted as equivalent on replay.
        let processor = processor();
        let mut value = vec![0_u8; 32];
        value[31] = 9;
        let included = frame(value);
        let mut finalized = included.clone();
        finalized.finality = Finality::Finalized;
        let included_delta = processor.map(&included).await.expect("included");
        let finalized_delta = processor.map(&finalized).await.expect("finalized");
        assert_ne!(included_delta.checksum, finalized_delta.checksum);
        let mut other_value = vec![0_u8; 32];
        other_value[31] = 8;
        let other = processor.map(&frame(other_value)).await.expect("other");
        for delta in [&included_delta, &finalized_delta] {
            let variants = processor
                .finality_variant_checksums(delta)
                .expect("finality variants");
            assert_eq!(variants.len(), 3);
            assert!(variants.contains(&included_delta.checksum));
            assert!(variants.contains(&finalized_delta.checksum));
            assert!(!variants.contains(&other.checksum));
        }
    }

    #[test]
    fn abi_parameters_need_names_and_names_may_contain_keywords() {
        let parse = |abi: &str| {
            EvmEventsProcessor::new(EvmEventsConfig {
                start_block: BlockNumber(1),
                addresses: Vec::new(),
                events: vec![EventDefinition {
                    abi: abi.to_owned(),
                    output: EventOutput {
                        collection: "events.votes".to_owned(),
                        kind: "events.vote".to_owned(),
                        key_fields: Vec::new(),
                        bucket_seconds: None,
                    },
                }],
            })
        };
        // Audit probe: an unnamed indexed parameter parsed as a data field
        // named `indexed`.
        let error = parse("event Deposit(address indexed, uint256 amount)")
            .expect_err("an unnamed parameter is rejected");
        assert!(
            error
                .to_string()
                .contains("must be `type name` or `type indexed name`")
        );
        // Audit probe: a parameter name containing `anonymous` was rejected.
        let processor = parse("event Vote(address indexed anonymousVoter, uint256 weight)")
            .expect("a parameter may be named anonymousVoter");
        let event = processor.events.values().next().expect("event");
        assert_eq!(event.signature, "Vote(address,uint256)");
        let fields = &event.abi.as_ref().expect("event ABI").fields;
        assert_eq!(fields[0].name, "anonymousVoter");
        assert!(fields[0].indexed);
        let error = parse("event Ping(uint256 value) anonymous")
            .expect_err("anonymous events are rejected");
        assert!(
            error
                .to_string()
                .contains("anonymous events are not supported")
        );
    }

    fn parse_one(abi: &str, key_fields: &[&str]) -> Result<EvmEventsProcessor, ProcessorError> {
        EvmEventsProcessor::new(EvmEventsConfig {
            start_block: BlockNumber(1),
            addresses: vec![Address::new([0x11; 20])],
            events: vec![EventDefinition {
                abi: abi.to_owned(),
                output: EventOutput {
                    collection: "events.items".to_owned(),
                    kind: "events.item".to_owned(),
                    key_fields: key_fields.iter().map(|key| (*key).to_owned()).collect(),
                    bucket_seconds: None,
                },
            }],
        })
    }

    #[test]
    fn unsupported_abi_types_fail_before_source_open() {
        for (abi, expected) in [
            ("event Hook(function callback)", "unsupported ABI type"),
            ("event Price(fixed128x18 value)", "invalid type string"),
            ("event Order((uint256, bool) params)", "must be `type name`"),
            (
                "event Order((uint256 amount, bool amount) params)",
                "component names must be unique",
            ),
            (
                "event Deep(uint256[][][][][][][] values)",
                "more than 6 levels deep",
            ),
            // Alloy would build this element's template, a billion values,
            // for every log, even one with an empty array.
            (
                "event Grid(uint256[1024][1024][1024][] values)",
                "array elements may hold at most 64 values",
            ),
            (
                "event Grid(uint256[64][] rows)",
                "array elements may hold at most 64 values",
            ),
            (
                r#"[{"type":"event","name":"Ping","inputs":[],"anonymous":false}]"#,
                "not an ABI array",
            ),
            (
                r#"{"type":"event","name":"Ping","inputs":[],"anonymous":true}"#,
                "`abi = \"log0\"` stores topicless logs raw",
            ),
        ] {
            let error = parse_one(abi, &[]).expect_err(abi);
            assert!(error.to_string().contains(expected), "{abi}: {error}");
        }
        // Six levels still parse, and far deeper fragments fail without
        // exhausting the stack.
        parse_one("event Deep(uint256[][][][][][] values)", &[]).expect("six levels");
        parse_one("event Grid(uint256[63][] rows)", &[]).expect("64 values");
        let tuples = format!(
            "event Deep({}uint256 x{} y)",
            "(".repeat(100_000),
            ") y".repeat(100_000)
        );
        let arrays = format!("event Deep(uint256{} values)", "[]".repeat(100_000));
        for abi in [tuples, arrays] {
            let error = parse_one(&abi, &[]).expect_err("too deep");
            assert!(
                error.to_string().contains("more than 6 levels deep"),
                "{error}"
            );
        }
    }

    #[test]
    fn todays_fragment_syntax_keeps_its_signature() {
        // Parsing moved to Alloy without a version change, so every spelling
        // the earlier parser accepted must keep its signature and fields.
        for (abi, signature, names) in [
            (
                "event Transfer(address indexed from, address indexed to, uint256 value)",
                "Transfer(address,address,uint256)",
                "from,to,value",
            ),
            (
                "Transfer(address indexed from, address indexed to, uint256 value);",
                "Transfer(address,address,uint256)",
                "from,to,value",
            ),
            (
                "  event   Sync( uint112 reserve0 ,uint112   reserve1 ) ;  ",
                "Sync(uint112,uint112)",
                "reserve0,reserve1",
            ),
            (
                "event Mint (uint amount, int delta)",
                "Mint(uint256,int256)",
                "amount,delta",
            ),
            (
                "event Vote(\n    address indexed anonymousVoter,\n\tuint256 weight\n)",
                "Vote(address,uint256)",
                "anonymousVoter,weight",
            ),
            ("event Ping()", "Ping()", ""),
        ] {
            let processor = parse_one(abi, &[]).expect(abi);
            let event = processor.events.values().next().expect("event");
            assert_eq!(event.signature, signature, "{abi}");
            let fields = &event.abi.as_ref().expect("event ABI").fields;
            let parsed = fields
                .iter()
                .map(|field| field.name.as_str())
                .collect::<Vec<_>>()
                .join(",");
            assert_eq!(parsed, names, "{abi}");
        }
    }

    #[tokio::test]
    async fn static_events_keep_their_identity_and_output_bytes() {
        // Golden values captured before dynamic ABI support: version 1.1.0
        // gained it in place, so a static configuration must keep its
        // descriptor and its mapped bytes.
        let processor = EvmEventsProcessor::new(EvmEventsConfig {
            start_block: BlockNumber(1),
            addresses: vec![Address::new([0x11; 20])],
            events: vec![
                EventDefinition {
                    abi: "event Transfer(address indexed from, address indexed to, uint256 value)"
                        .to_owned(),
                    output: EventOutput {
                        collection: "events.transfers".to_owned(),
                        kind: "events.transfer".to_owned(),
                        key_fields: vec!["from".to_owned(), "to".to_owned()],
                        bucket_seconds: Some(60),
                    },
                },
                EventDefinition {
                    abi: "Mixed(int24 indexed tick, uint8 small, int256 delta, bool flag, bytes4 tag, address who);"
                        .to_owned(),
                    output: EventOutput {
                        collection: "events.mixed".to_owned(),
                        kind: "events.mixed".to_owned(),
                        key_fields: Vec::new(),
                        bucket_seconds: None,
                    },
                },
            ],
        })
        .expect("processor");
        let descriptor = postcard::to_allocvec(processor.descriptor()).expect("descriptor");
        assert_eq!(
            blake3::hash(&descriptor).to_hex().as_str(),
            "88595eee52ddc84a6cda3ee3b61e12b176455ceb54b8ae12b430aa9849d5cc31"
        );

        let mixed_topic = keccak256("Mixed(int24,uint8,int256,bool,bytes4,address)").0;
        let mut tick = [0xff_u8; 32];
        tick[29..].copy_from_slice(&[0xff, 0xfe, 0x0c]);
        let mut data = vec![0_u8; 32 * 5];
        data[31] = 200;
        data[32..64].fill(0xff);
        data[95] = 1;
        data[96..100].copy_from_slice(&[0xde, 0xad, 0xbe, 0xef]);
        data[140..160].fill(0x55);
        let mixed = Log {
            address: Address::new([0x11; 20]),
            topics: vec![mixed_topic, tick],
            data,
            transaction_hash: Some(TransactionHash::new([0x44; 32])),
            transaction_index: 1,
            log_index: 9,
        };
        let mut logs = transfers(&[1, 2]);
        logs.push(mixed);
        let delta = processor
            .map(&transfer_frame(1, BlockHash::ZERO, logs))
            .await
            .expect("map");
        assert_eq!(
            hex::encode(&delta.payload),
            "03106576656e74732e7472616e73666572730f6576656e74732e7472616e7366657220e361d2d1b6ec22b125edc9bf64dbad0843b16b33894a2be6dd446f4726e8f07a011111111111111111111111111111111111111111085472616e73666572215472616e7366657228616464726573732c616464726573732c75696e743235362901010101010101010101010101010101010101010101010101010101010101010181e2cfaa0601ece1cfaa064444444444444444444444444444444444444444444444444444444444444444000001030466726f6d2a30783232323232323232323232323232323232323232323232323232323232323232323232323232323202746f2a3078333333333333333333333333333333333333333333333333333333333333333333333333333333330576616c75650131106576656e74732e7472616e73666572730f6576656e74732e7472616e7366657220e361d2d1b6ec22b125edc9bf64dbad0843b16b33894a2be6dd446f4726e8f07a011111111111111111111111111111111111111111085472616e73666572215472616e7366657228616464726573732c616464726573732c75696e743235362901010101010101010101010101010101010101010101010101010101010101010181e2cfaa0601ece1cfaa064444444444444444444444444444444444444444444444444444444444444444000101030466726f6d2a30783232323232323232323232323232323232323232323232323232323232323232323232323232323202746f2a3078333333333333333333333333333333333333333333333333333333333333333333333333333333330576616c756501320c6576656e74732e6d697865640c6576656e74732e6d6978656424444444444444444444444444444444444444444444444444444444444444444400000009011111111111111111111111111111111111111111054d697865642d4d6978656428696e7432342c75696e74382c696e743235362c626f6f6c2c6279746573342c616464726573732901010101010101010101010101010101010101010101010101010101010101010181e2cfaa06004444444444444444444444444444444444444444444444444444444444444444010901060564656c7461022d3104666c6167047472756505736d616c6c03323030037461670a30786465616462656566047469636b042d3530300377686f2a30783535353535353535353535353535353535353535353535353535353535353535353535353535353500"
        );
    }

    fn contract() -> Address {
        Address::new([0x11; 20])
    }

    fn event_log(topics: Vec<[u8; 32]>, data: Vec<u8>, log_index: u32) -> Log {
        Log {
            address: contract(),
            topics,
            data,
            transaction_hash: Some(TransactionHash::new([0x44; 32])),
            transaction_index: 0,
            log_index,
        }
    }

    async fn mapped(processor: &EvmEventsProcessor, logs: Vec<Log>) -> EventDelta {
        let delta = processor
            .map(&transfer_frame(1, BlockHash::ZERO, logs))
            .await
            .expect("map");
        postcard::from_bytes(&delta.payload).expect("delta")
    }

    fn word(byte: u8) -> [u8; 32] {
        let mut word = [0; 32];
        word[12..].fill(byte);
        word
    }

    fn encode(values: Vec<alloy_dyn_abi::DynSolValue>) -> Vec<u8> {
        alloy_dyn_abi::DynSolValue::Tuple(values)
            .abi_encode_sequence()
            .expect("sequence")
    }

    fn alloy_address(byte: u8) -> alloy_dyn_abi::DynSolValue {
        alloy_dyn_abi::DynSolValue::Address(alloy_primitives::Address::repeat_byte(byte))
    }

    fn uint(value: u64, bits: usize) -> alloy_dyn_abi::DynSolValue {
        alloy_dyn_abi::DynSolValue::Uint(alloy_primitives::U256::from(value), bits)
    }

    #[tokio::test]
    async fn a_cow_protocol_trade_decodes_its_dynamic_order_uid() {
        // Mainnet block 26,225,034, transaction
        // 0x7713a8cfe37011c87ff5229d3ad9b226c9e881a6fbfcf09b4f3642d7bd82dab3,
        // log 275 of GPv2Settlement, which evm-events rejected before
        // `bytes` decoded.
        let settlement = Address::new(
            hex::decode("9008d19f58aabd9ed0d60971565aa8510560ab41")
                .expect("hex")
                .try_into()
                .expect("address"),
        );
        let processor = EvmEventsProcessor::new(EvmEventsConfig {
            start_block: BlockNumber(1),
            addresses: vec![settlement],
            events: vec![EventDefinition {
                abi: "event Trade(address indexed owner, address sellToken, address buyToken, uint256 sellAmount, uint256 buyAmount, uint256 feeAmount, bytes orderUid)"
                    .to_owned(),
                output: EventOutput {
                    collection: "cow.trades".to_owned(),
                    kind: "cow.trade".to_owned(),
                    key_fields: vec!["orderUid".to_owned()],
                    bucket_seconds: None,
                },
            }],
        })
        .expect("processor");
        let topic0: [u8; 32] =
            hex::decode("a07a543ab8a018198e99ca0184c93fe9050a79400a0a723441f84de1d972cc17")
                .expect("hex")
                .try_into()
                .expect("topic");
        assert!(
            processor.events.contains_key(&topic0),
            "canonical signature"
        );
        let mut owner = [0_u8; 32];
        owner[12..].copy_from_slice(
            &hex::decode("315790e40794416535fc73457f8aa3197f88e6a1").expect("hex"),
        );
        let data = hex::decode(concat!(
            "000000000000000000000000d11c452fc99cf405034ee446803b6f6c1f6d5ed8",
            "000000000000000000000000eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
            "000000000000000000000000000000000000000000000000004727014a03ae0d",
            "00000000000000000000000000000000000000000000000000590af96c2d3957",
            "0000000000000000000000000000000000000000000000000000000000000000",
            "00000000000000000000000000000000000000000000000000000000000000c0",
            "0000000000000000000000000000000000000000000000000000000000000038",
            "d2c7d8f32b34fa1e68cbb9aa56a97d9a2ce313f34ec41fcc3ed411b2ceb09bbe",
            "315790e40794416535fc73457f8aa3197f88e6a16ad5c9090000000000000000",
        ))
        .expect("hex");
        let mut trade = event_log(vec![topic0, owner], data, 275);
        trade.address = settlement;
        let decoded = mapped(&processor, vec![trade]).await;
        assert_eq!(decoded.skipped_logs, 0);
        let values = &decoded.events[0].entity.values;
        assert_eq!(
            values["owner"],
            "0x315790e40794416535fc73457f8aa3197f88e6a1"
        );
        assert_eq!(
            values["sellToken"],
            "0xd11c452fc99cf405034ee446803b6f6c1f6d5ed8"
        );
        assert_eq!(
            values["buyToken"],
            "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
        );
        assert_eq!(values["sellAmount"], "20027609836662285");
        assert_eq!(values["buyAmount"], "25063339305285975");
        assert_eq!(values["feeAmount"], "0");
        // An order UID is the order digest, the owner, and `validTo`, so the
        // owner inside it checks the decoded offset and length.
        assert_eq!(
            values["orderUid"],
            "0xd2c7d8f32b34fa1e68cbb9aa56a97d9a2ce313f34ec41fcc3ed411b2ceb09bbe315790e40794416535fc73457f8aa3197f88e6a16ad5c909"
        );
        assert_eq!(&values["orderUid"][66..106], &values["owner"][2..]);
    }

    #[tokio::test]
    async fn tuples_flatten_and_arrays_render_as_json_strings() {
        use alloy_dyn_abi::DynSolValue;

        // Maverick V2's `PoolSwap` carries a static struct.
        let processor = parse_one(
            "event PoolSwap(address sender, address recipient, (uint256 amount, bool tokenAIn, bool exactOutput, int32 tickLimit) params, uint256 amountIn, uint256 amountOut)",
            &["params.tokenAIn"],
        )
        .expect("processor");
        let event = processor.events.values().next().expect("event");
        assert_eq!(
            event.signature,
            "PoolSwap(address,address,(uint256,bool,bool,int32),uint256,uint256)"
        );
        let topic0 = keccak256(event.signature.as_bytes()).0;
        let data = encode(vec![
            alloy_address(0x22),
            alloy_address(0x33),
            DynSolValue::Tuple(vec![
                uint(5_000, 256),
                DynSolValue::Bool(true),
                DynSolValue::Bool(false),
                DynSolValue::Int(alloy_primitives::I256::try_from(-887_272).expect("int"), 32),
            ]),
            uint(5_000, 256),
            uint(4_990, 256),
        ]);
        let decoded = mapped(&processor, vec![event_log(vec![topic0], data, 0)]).await;
        let values = &decoded.events[0].entity.values;
        assert_eq!(values["params.amount"], "5000");
        assert_eq!(values["params.tokenAIn"], "true");
        assert_eq!(values["params.exactOutput"], "false");
        assert_eq!(values["params.tickLimit"], "-887272");
        assert_eq!(values["amountOut"], "4990");
        assert!(!values.contains_key("params"));
        let error = parse_one(
            "event PoolSwap((uint256 amount, bool tokenAIn) params)",
            &["params"],
        )
        .expect_err("a tuple is not a value key");
        assert!(
            error.to_string().contains("params.amount, params.tokenAIn"),
            "{error}"
        );

        // ERC-1155's `TransferBatch` carries arrays, and nested arrays of
        // tuples render as JSON objects in ABI order.
        let processor = parse_one(
            "event TransferBatch(address indexed operator, address indexed from, address indexed to, uint256[] ids, uint256[] values)",
            &[],
        )
        .expect("processor");
        let topic0 = keccak256("TransferBatch(address,address,address,uint256[],uint256[])").0;
        let data = encode(vec![
            DynSolValue::Array(vec![uint(1, 256), uint(2, 256)]),
            DynSolValue::Array(vec![uint(10, 256), uint(20, 256)]),
        ]);
        let decoded = mapped(
            &processor,
            vec![event_log(
                vec![topic0, word(0x22), word(0x33), word(0x44)],
                data,
                0,
            )],
        )
        .await;
        let values = &decoded.events[0].entity.values;
        assert_eq!(values["ids"], r#"["1","2"]"#);
        assert_eq!(values["values"], r#"["10","20"]"#);
        assert_eq!(values["to"], format!("0x{}", "44".repeat(20)));

        let processor = parse_one(
            "event Orders((address maker, bytes data)[] orders, string[2] tags, bytes32[][] proofs)",
            &[],
        )
        .expect("processor");
        let topic0 = keccak256("Orders((address,bytes)[],string[2],bytes32[][])").0;
        let data = encode(vec![
            DynSolValue::Array(vec![DynSolValue::Tuple(vec![
                alloy_address(0x55),
                DynSolValue::Bytes(vec![0x01, 0x02]),
            ])]),
            DynSolValue::FixedArray(vec![
                DynSolValue::String("a\"b".to_owned()),
                DynSolValue::String(String::new()),
            ]),
            DynSolValue::Array(vec![
                DynSolValue::Array(vec![DynSolValue::FixedBytes([0xab; 32].into(), 32)]),
                DynSolValue::Array(Vec::new()),
            ]),
        ]);
        let decoded = mapped(&processor, vec![event_log(vec![topic0], data, 0)]).await;
        let values = &decoded.events[0].entity.values;
        assert_eq!(
            values["orders"],
            format!(r#"[{{"maker":"0x{}","data":"0x0102"}}]"#, "55".repeat(20))
        );
        assert_eq!(values["tags"], r#"["a\"b",""]"#);
        assert_eq!(
            values["proofs"],
            format!(r#"[["0x{}"],[]]"#, "ab".repeat(32))
        );
    }

    #[tokio::test]
    async fn json_fragments_decode_like_their_human_readable_form() {
        use alloy_dyn_abi::DynSolValue;

        let json = r#"{"type":"event","name":"PoolCreated","anonymous":false,"inputs":[
            {"name":"pool","type":"address","indexed":true,"internalType":"address"},
            {"name":"tokens","type":"address[]","indexed":false,"internalType":"address[]"},
            {"name":"config","type":"tuple","indexed":false,"internalType":"struct Config",
             "components":[{"name":"fee","type":"uint24","internalType":"uint24"},
                           {"name":"tickSpacing","type":"int24","internalType":"int24"}]},
            {"name":"weights","type":"uint256[]","indexed":false,"internalType":"uint256[]"}]}"#;
        let readable = "event PoolCreated(address indexed pool, address[] tokens, (uint24 fee, int24 tickSpacing) config, uint256[] weights)";
        let topic0 = keccak256("PoolCreated(address,address[],(uint24,int24),uint256[])").0;
        let data = encode(vec![
            DynSolValue::Array(vec![alloy_address(0x66), alloy_address(0x77)]),
            DynSolValue::Tuple(vec![
                uint(3_000, 24),
                DynSolValue::Int(alloy_primitives::I256::try_from(-60).expect("int"), 24),
            ]),
            DynSolValue::Array(vec![uint(1, 256)]),
        ]);
        let mut decoded = Vec::new();
        for abi in [json, readable] {
            let processor = parse_one(abi, &["config.fee"]).expect(abi);
            let delta = mapped(
                &processor,
                vec![event_log(vec![topic0, word(0x88)], data.clone(), 0)],
            )
            .await;
            decoded.push(delta.events[0].entity.values.clone());
        }
        assert_eq!(decoded[0], decoded[1]);
        assert_eq!(decoded[0]["config.tickSpacing"], "-60");
        assert_eq!(
            decoded[0]["tokens"],
            format!(r#"["0x{}","0x{}"]"#, "66".repeat(20), "77".repeat(20))
        );
    }

    #[tokio::test]
    async fn strings_decode_lossily_and_indexed_reference_values_keep_their_hash() {
        use alloy_dyn_abi::DynSolValue;

        // ERC-1155's `URI` logs a string. Solidity does not check UTF-8, so
        // invalid sequences render as U+FFFD instead of dropping the event.
        let processor =
            parse_one("event URI(string value, uint256 indexed id)", &[]).expect("processor");
        let topic0 = keccak256("URI(string,uint256)").0;
        let mut id = [0_u8; 32];
        id[31] = 7;
        let logs = vec![
            event_log(
                vec![topic0, id],
                encode(vec![DynSolValue::String("ipfs://bafy/7.json".to_owned())]),
                0,
            ),
            event_log(
                vec![topic0, id],
                encode(vec![DynSolValue::Bytes(vec![b'o', 0xff, b'k'])]),
                1,
            ),
        ];
        let decoded = mapped(&processor, logs).await;
        assert_eq!(decoded.skipped_logs, 0);
        assert_eq!(
            decoded.events[0].entity.values["value"],
            "ipfs://bafy/7.json"
        );
        assert_eq!(decoded.events[0].entity.values["id"], "7");
        assert_eq!(decoded.events[1].entity.values["value"], "o\u{fffd}k");

        // An indexed string, tuple, or array is only the hash of its value.
        let processor = parse_one(
            "event Named(string indexed name, (uint256 x, uint256 y) indexed point, uint256[] indexed ids, uint256 value)",
            &["point"],
        )
        .expect("processor");
        let topic0 = keccak256("Named(string,(uint256,uint256),uint256[],uint256)").0;
        let decoded = mapped(
            &processor,
            vec![event_log(
                vec![topic0, [0xa1; 32], [0xa2; 32], [0xa3; 32]],
                encode(vec![uint(9, 256)]),
                0,
            )],
        )
        .await;
        let values = &decoded.events[0].entity.values;
        assert_eq!(values["name"], format!("0x{}", "a1".repeat(32)));
        assert_eq!(values["point"], format!("0x{}", "a2".repeat(32)));
        assert_eq!(values["ids"], format!("0x{}", "a3".repeat(32)));
        assert_eq!(values["value"], "9");
    }

    /// Maps one log of `abi` and returns how many logs it skipped.
    async fn skipped(abi: &str, data: Vec<u8>) -> u32 {
        let processor = parse_one(abi, &[]).expect(abi);
        let topic0 = *processor.events.keys().next().expect("event");
        mapped(&processor, vec![event_log(vec![topic0], data, 0)])
            .await
            .skipped_logs
    }

    #[tokio::test]
    async fn non_canonical_layouts_are_skipped_and_counted() {
        use alloy_dyn_abi::DynSolValue;

        let note = "event Note(bytes payload)";
        let payload = encode(vec![DynSolValue::Bytes(vec![0x01])]);
        assert_eq!(skipped(note, payload.clone()).await, 0, "canonical");
        let mut trailing = payload.clone();
        trailing.push(0);
        let mut gap = vec![0_u8; 32];
        gap[31] = 0x40;
        gap.extend_from_slice(&[0; 32]);
        gap.extend_from_slice(&payload[32..]);
        let mut dirty_padding = payload.clone();
        dirty_padding[70] = 0xff;
        for (case, data) in [
            ("trailing byte", trailing),
            ("gap before the tail", gap),
            ("dirty tail padding", dirty_padding),
        ] {
            assert_eq!(skipped(note, data).await, 1, "{case}");
        }

        // Two offsets aliasing one tail.
        let mut aliased = vec![0_u8; 64];
        aliased[31] = 0x40;
        aliased[63] = 0x40;
        aliased.extend_from_slice(&payload[32..]);
        assert_eq!(skipped("event Pair(bytes a, bytes b)", aliased).await, 1);
        // Many array elements aliasing one tail would decode to far more
        // values than the log holds.
        let elements = 512_u64;
        let mut amplified = vec![0_u8; 64];
        amplified[31] = 0x20;
        amplified[56..64].copy_from_slice(&elements.to_be_bytes());
        for _ in 0..elements {
            let mut offset = [0_u8; 32];
            offset[24..].copy_from_slice(&(elements * 32).to_be_bytes());
            amplified.extend_from_slice(&offset);
        }
        let mut length = [0_u8; 32];
        length[30..].copy_from_slice(&4_096_u16.to_be_bytes());
        amplified.extend_from_slice(&length);
        amplified.extend_from_slice(&[0x5a; 4_096]);
        assert_eq!(skipped("event Batch(bytes[] items)", amplified).await, 1);
    }

    #[test]
    fn shared_tails_fail_before_alloy_decodes_them() {
        // Three array levels whose 58 offsets each share one tail: 5,792
        // bytes that would decode to 58³ `bytes` values, over 40 MiB.
        let number = |value: u64| alloy_primitives::U256::from(value).to_be_bytes::<32>();
        let mut data = number(32).to_vec();
        for _ in 0..3 {
            data.extend(number(58));
            for _ in 0..58 {
                data.extend(number(58 * 32));
            }
        }
        data.extend(number(64));
        data.extend([0x5a; 64]);
        let event = abi::parse("event Deep(bytes[][][] items)").expect("abi");
        assert_eq!(
            event.decode(&[[0; 32]], &data),
            Err("log data is not the canonical ABI layout".to_owned())
        );
    }

    #[test]
    fn canonical_layouts_of_every_shape_pass_the_layout_check() {
        use alloy_dyn_abi::DynSolValue;

        let pair = |a: u64, b: bool| DynSolValue::Tuple(vec![uint(a, 8), DynSolValue::Bool(b)]);
        for (fragment, values) in [
            (
                "event Static((uint8 a, bool b)[] pairs, (uint8 a, bool b)[2] twins, uint256[2] range, bytes empty)",
                vec![
                    DynSolValue::Array(vec![pair(1, true), pair(2, false)]),
                    DynSolValue::FixedArray(vec![pair(3, true), pair(4, false)]),
                    DynSolValue::FixedArray(vec![uint(5, 256), uint(6, 256)]),
                    DynSolValue::Bytes(Vec::new()),
                ],
            ),
            (
                "event Nested((bytes a, (string b, uint8[] c) d) outer, string[2][] grid)",
                vec![
                    DynSolValue::Tuple(vec![
                        DynSolValue::Bytes(vec![1; 33]),
                        DynSolValue::Tuple(vec![
                            DynSolValue::String("x".to_owned()),
                            DynSolValue::Array(vec![uint(7, 8)]),
                        ]),
                    ]),
                    DynSolValue::Array(vec![DynSolValue::FixedArray(vec![
                        DynSolValue::String("p".to_owned()),
                        DynSolValue::String(String::new()),
                    ])]),
                ],
            ),
        ] {
            let event = abi::parse(fragment).expect(fragment);
            event.decode(&[[0; 32]], &encode(values)).expect(fragment);
        }
    }

    #[tokio::test]
    async fn value_words_inside_arrays_and_tuples_keep_the_static_checks() {
        use alloy_dyn_abi::DynSolValue;

        let wide = encode(vec![DynSolValue::Array(vec![uint(256, 8)])]);
        assert_eq!(skipped("event Small(uint8[] values)", wide).await, 1);
        let mut dirty_bool = encode(vec![DynSolValue::Array(vec![DynSolValue::Bool(true)])]);
        dirty_bool[95] = 2;
        assert_eq!(skipped("event Flags(bool[] flags)", dirty_bool).await, 1);
        let entry = "event Owned((address owner, uint256 amount) entry)";
        let clean = encode(vec![DynSolValue::Tuple(vec![
            alloy_address(0x22),
            uint(1, 256),
        ])]);
        assert_eq!(skipped(entry, clean.clone()).await, 0, "canonical");
        let mut dirty_address = clean;
        dirty_address[0] = 1;
        assert_eq!(skipped(entry, dirty_address).await, 1);
    }

    #[tokio::test]
    async fn a_log0_entry_stores_topicless_logs_raw() {
        // Ekubo Core logs each swap with LOG0 and a packed, non-ABI layout:
        // mainnet block 26,225,034, transaction
        // 0xedc2606626578d6919f60cc5cfdf5b2d60cb0528f36197acd7dec8eacff31a1a,
        // log 509.
        let core = Address::new(
            hex::decode("00000000000014aa86c5d3c41765bb24e11bd701")
                .expect("hex")
                .try_into()
                .expect("address"),
        );
        let swap = "0cef320d0c5793ade37964e2661a033771e53cd577e86b8f5da17873d7bdd70efc68a06dd3edc1d6e36b5bd65ff45eece33a31e4fffffffffffffffffff9a80b42f7c4fa000000000000000000000000004245634000ced8c72eb7f8994ed4ebfed1595e00000000000000000000d24b97b07f60";
        let output = |collection: &str, key_fields: &[&str]| EventOutput {
            collection: collection.to_owned(),
            kind: collection.to_owned(),
            key_fields: key_fields.iter().map(|key| (*key).to_owned()).collect(),
            bucket_seconds: None,
        };
        let config =
            |addresses: Vec<Address>, abis: &[&str], key_fields: &[&str]| EvmEventsConfig {
                start_block: BlockNumber(1),
                addresses,
                events: abis
                    .iter()
                    .map(|abi| EventDefinition {
                        abi: (*abi).to_owned(),
                        output: output(
                            if *abi == "log0" {
                                "ekubo.swaps"
                            } else {
                                "ekubo.transfers"
                            },
                            if *abi == "log0" { key_fields } else { &[] },
                        ),
                    })
                    .collect(),
            };
        let processor = EvmEventsProcessor::new(config(
            vec![core],
            &[
                "log0",
                "event Transfer(address indexed from, address indexed to, uint256 value)",
            ],
            &[],
        ))
        .expect("processor");
        // A topicless log has no topic zero to filter on.
        assert!(
            processor.descriptor.requirements[0]
                .filter
                .topics
                .is_empty()
        );
        assert_eq!(
            processor.descriptor.requirements[0].filter.addresses,
            vec![core]
        );

        let mut ekubo = event_log(Vec::new(), hex::decode(swap).expect("hex"), 509);
        ekubo.address = core;
        let mut transfer = transfer_log(5, 510);
        transfer.address = core;
        let mut other_topic = transfer_log(6, 511);
        other_topic.address = core;
        other_topic.topics[0] = [0x99; 32];
        let foreign = event_log(Vec::new(), vec![1, 2, 3], 512);
        let decoded = mapped(&processor, vec![ekubo, transfer, other_topic, foreign]).await;
        assert_eq!(decoded.skipped_logs, 0);
        assert_eq!(decoded.events.len(), 2);
        let raw = &decoded.events[0].entity;
        assert_eq!(raw.event, "log0");
        assert_eq!(raw.signature, "log0");
        assert_eq!(raw.values.len(), 1);
        assert_eq!(raw.values["data"], format!("0x{swap}"));
        assert_eq!(decoded.events[1].entity.values["value"], "5");

        // Without the entry, the topic-zero filter stays.
        let processor = EvmEventsProcessor::new(config(
            vec![core],
            &["event Transfer(address indexed from, address indexed to, uint256 value)"],
            &[],
        ))
        .expect("processor");
        assert_eq!(processor.descriptor.requirements[0].filter.topics.len(), 1);

        EvmEventsProcessor::new(config(vec![core], &["log0"], &["data"])).expect("keyed by data");
        for (addresses, abis, key_fields, expected) in [
            (Vec::new(), &["log0"][..], &[][..], "requires `addresses`"),
            (
                vec![core],
                &["log0", " log0 "][..],
                &[][..],
                "at most one `log0` entry",
            ),
            (
                vec![core],
                &["log0"][..],
                &["value"][..],
                "its value keys are data",
            ),
        ] {
            let error =
                EvmEventsProcessor::new(config(addresses, abis, key_fields)).expect_err(expected);
            assert!(error.to_string().contains(expected), "{error}");
        }
    }
}
