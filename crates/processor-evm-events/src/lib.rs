//! Declarative, deterministic EVM event decoding without EVM execution.

use std::collections::{BTreeMap, BTreeSet};

use alloy_primitives::{I256, U256, keccak256};
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
    /// Example: `event Sync(uint112 reserve0, uint112 reserve1)`.
    pub abi: String,
    pub output: EventOutput,
}

/// Retained entity/change mapping for one decoded event.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EventOutput {
    pub collection: String,
    pub kind: String,
    /// Field names used to construct a deterministic key. Empty uses
    /// transaction hash plus log index.
    #[serde(default)]
    pub key_fields: Vec<String>,
    /// Optional canonical-block timestamp bucket.
    #[serde(default)]
    pub bucket_seconds: Option<u64>,
}

/// Stable public entity emitted for every matching log.
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
    topic0: [u8; 32],
    fields: Vec<AbiField>,
    output: EventOutput,
}

#[derive(Clone, Debug)]
struct AbiField {
    name: String,
    kind: AbiKind,
    indexed: bool,
}

#[derive(Clone, Copy, Debug)]
enum AbiKind {
    Address,
    Uint(u16),
    Int(u16),
    Bool,
    FixedBytes(u8),
}

impl AbiKind {
    fn canonical(self) -> String {
        match self {
            Self::Address => "address".to_owned(),
            Self::Uint(bits) => format!("uint{bits}"),
            Self::Int(bits) => format!("int{bits}"),
            Self::Bool => "bool".to_owned(),
            Self::FixedBytes(bytes) => format!("bytes{bytes}"),
        }
    }
}

/// Native declarative EVM event processor.
#[derive(Clone, Debug)]
pub struct EvmEventsProcessor {
    descriptor: ProcessorDescriptor,
    addresses: BTreeSet<Address>,
    events: BTreeMap<[u8; 32], ParsedEvent>,
}

impl EvmEventsProcessor {
    /// Parse and validate static event ABI fragments.
    ///
    /// Dynamic ABI values, anonymous events, duplicate signatures, invalid
    /// output names, and impossible key/bucket mappings fail at construction
    /// before a source range is opened.
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
        let mut parsed = Vec::with_capacity(config.events.len());
        for definition in &config.events {
            parsed.push(parse_event(definition)?);
        }
        parsed.sort_by_key(|event| event.topic0);
        if parsed
            .windows(2)
            .any(|events| events[0].topic0 == events[1].topic0)
        {
            return Err(input_error("event signatures must be unique"));
        }
        let normalized = postcard::to_allocvec(&config)
            .map_err(|error| input_error(format!("configuration encoding failed: {error}")))?;
        let id = ProcessorId::new("evm-events").map_err(|error| input_error(error.to_string()))?;
        let version = Version::new(1, 0, 0);
        let config_hash = BlockHash::new(*blake3::hash(&normalized).as_bytes());
        let topics = parsed.iter().map(|event| event.topic0).collect();
        let descriptor = ProcessorDescriptor {
            instance: ProcessorInstanceId::legacy(&id, &version, config_hash),
            id,
            version,
            code_hash: BlockHash::new(*blake3::hash(b"leani/evm-events/1.0.0").as_bytes()),
            config_hash,
            start: StartPoint::Block(config.start_block),
            requirements: vec![DataRequirement {
                capabilities: CapabilitySet::of(Capability::Logs),
                log_fields: leani_primitives::LogFieldSet::ALL,
                allow_filtered: true,
                filter: FilterScope {
                    addresses: config.addresses.clone(),
                    topics: vec![TopicFilter {
                        position: 0,
                        alternatives: topics,
                    }],
                    ..FilterScope::default()
                },
                minimum_finality: Finality::Included,
            }],
            mode: ReductionMode::BlockLocal,
            delivery_ordering: DeliveryOrdering::BlockVersionedIdempotent,
            publication: PublicationPolicy::IncludedAndFinalized,
            lifecycle: LifecyclePolicies::from_legacy(RetentionPolicy::FullOutputHistory),
            schemas: ProcessorSchemas {
                delta_version: 1,
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
            events: parsed
                .into_iter()
                .map(|event| (event.topic0, event))
                .collect(),
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
        for log in logs {
            if !self.addresses.is_empty() && !self.addresses.contains(&log.address) {
                continue;
            }
            let Some(topic0) = log.topics.first() else {
                continue;
            };
            let Some(event) = self.events.get(topic0) else {
                continue;
            };
            let transaction_hash = log.transaction_hash.ok_or_else(|| {
                input_error(format!(
                    "{} log {} omits its required transaction hash",
                    event.signature, log.log_index
                ))
            })?;
            let values = decode_values(event, log).map_err(|detail| {
                ProcessorError::Input(format!(
                    "{} log {} in transaction {} is invalid: {detail}",
                    event.signature, log.log_index, transaction_hash
                ))
            })?;
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
        let payload = postcard::to_allocvec(&EventDelta { events })
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
        let delta: EventDelta = postcard::from_bytes(&delta.payload)
            .map_err(|error| ProcessorError::DeltaPayload(error.to_string()))?;
        let mut changes = Vec::with_capacity(delta.events.len());
        for event in delta.events {
            let payload = postcard::to_allocvec(&event.entity)
                .map_err(|error| ProcessorError::State(error.to_string()))?;
            transaction
                .put(&event.collection, event.key.clone(), payload.clone())
                .await?;
            let change = DomainChange {
                kind: event.kind,
                key: event.key,
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
    let fragment = definition
        .abi
        .trim()
        .strip_prefix("event ")
        .unwrap_or(definition.abi.trim())
        .trim_end_matches(';')
        .trim();
    if fragment.contains(" anonymous") {
        return Err(input_error("anonymous events are not supported"));
    }
    let open = fragment
        .find('(')
        .ok_or_else(|| input_error("event ABI is missing `(`"))?;
    let close = fragment
        .rfind(')')
        .ok_or_else(|| input_error("event ABI is missing `)`"))?;
    if close <= open || !fragment[close + 1..].trim().is_empty() {
        return Err(input_error("event ABI has trailing or unbalanced syntax"));
    }
    let name = fragment[..open].trim();
    if !valid_identifier(name) {
        return Err(input_error("event name is not a Solidity identifier"));
    }
    let mut fields = Vec::new();
    let parameters = fragment[open + 1..close].trim();
    if !parameters.is_empty() {
        for parameter in parameters.split(',') {
            let tokens = parameter.split_whitespace().collect::<Vec<_>>();
            let (kind, indexed, field_name) = match tokens.as_slice() {
                [kind, field] => (parse_kind(kind)?, false, *field),
                [kind, "indexed", field] => (parse_kind(kind)?, true, *field),
                _ => {
                    return Err(input_error(format!(
                        "event parameter `{parameter}` must be `type name` or `type indexed name`"
                    )));
                }
            };
            if !valid_identifier(field_name) {
                return Err(input_error("event field name is not a Solidity identifier"));
            }
            fields.push(AbiField {
                name: field_name.to_owned(),
                kind,
                indexed,
            });
        }
    }
    let mut names = BTreeSet::new();
    if fields.iter().any(|field| !names.insert(field.name.clone())) {
        return Err(input_error("event field names must be unique"));
    }
    if fields.iter().filter(|field| field.indexed).count() > 3 {
        return Err(input_error(
            "an event cannot contain more than three indexed fields",
        ));
    }
    for key in &definition.output.key_fields {
        if key != "_bucket_start" && !names.contains(key) {
            return Err(input_error(format!(
                "output key field `{key}` is not declared by the event ABI"
            )));
        }
        if key == "_bucket_start" && definition.output.bucket_seconds.is_none() {
            return Err(input_error(
                "_bucket_start key requires output.bucket_seconds",
            ));
        }
    }
    let signature = format!(
        "{name}({})",
        fields
            .iter()
            .map(|field| field.kind.canonical())
            .collect::<Vec<_>>()
            .join(",")
    );
    Ok(ParsedEvent {
        name: name.to_owned(),
        topic0: keccak256(signature.as_bytes()).0,
        signature,
        fields,
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

fn parse_kind(value: &str) -> Result<AbiKind, ProcessorError> {
    match value {
        "address" => Ok(AbiKind::Address),
        "bool" => Ok(AbiKind::Bool),
        "uint" => Ok(AbiKind::Uint(256)),
        "int" => Ok(AbiKind::Int(256)),
        _ if value.starts_with("uint") => parse_integer_width(&value[4..]).map(AbiKind::Uint),
        _ if value.starts_with("int") => parse_integer_width(&value[3..]).map(AbiKind::Int),
        _ if value.starts_with("bytes") => {
            let bytes = value[5..]
                .parse::<u8>()
                .map_err(|_| input_error(format!("unsupported ABI type `{value}`")))?;
            if (1..=32).contains(&bytes) {
                Ok(AbiKind::FixedBytes(bytes))
            } else {
                Err(input_error(format!("unsupported ABI type `{value}`")))
            }
        }
        _ => Err(input_error(format!(
            "unsupported static ABI type `{value}`; arrays, tuples, string, and dynamic bytes are not supported"
        ))),
    }
}

fn parse_integer_width(value: &str) -> Result<u16, ProcessorError> {
    let bits = value
        .parse::<u16>()
        .map_err(|_| input_error("integer ABI width is invalid"))?;
    if (8..=256).contains(&bits) && bits.is_multiple_of(8) {
        Ok(bits)
    } else {
        Err(input_error(
            "integer ABI width must be a multiple of 8 in 8..=256",
        ))
    }
}

fn decode_values(
    event: &ParsedEvent,
    log: &leani_primitives::Log,
) -> Result<BTreeMap<String, String>, String> {
    let indexed = event.fields.iter().filter(|field| field.indexed).count();
    let unindexed = event.fields.len().saturating_sub(indexed);
    if log.topics.len() != indexed.saturating_add(1) {
        return Err(format!(
            "expected {} topics, received {}",
            indexed.saturating_add(1),
            log.topics.len()
        ));
    }
    if log.data.len() != unindexed.saturating_mul(32) {
        return Err(format!(
            "expected {} data bytes, received {}",
            unindexed.saturating_mul(32),
            log.data.len()
        ));
    }
    let mut topic_index = 1;
    let mut data_index = 0;
    let mut values = BTreeMap::new();
    for field in &event.fields {
        let word: [u8; 32] = if field.indexed {
            let word = log.topics[topic_index];
            topic_index += 1;
            word
        } else {
            let start = data_index * 32;
            data_index += 1;
            log.data[start..start + 32]
                .try_into()
                .map_err(|_| "ABI word is truncated".to_owned())?
        };
        values.insert(field.name.clone(), decode_word(field.kind, word)?);
    }
    Ok(values)
}

fn decode_word(kind: AbiKind, word: [u8; 32]) -> Result<String, String> {
    match kind {
        AbiKind::Address => {
            if word[..12].iter().any(|byte| *byte != 0) {
                return Err("address contains non-zero ABI padding".to_owned());
            }
            Ok(format!("0x{}", hex::encode(&word[12..])))
        }
        AbiKind::Uint(bits) => {
            let padding = usize::from((256 - bits) / 8);
            if word[..padding].iter().any(|byte| *byte != 0) {
                return Err(format!("uint{bits} value exceeds its declared width"));
            }
            Ok(U256::from_be_bytes(word).to_string())
        }
        AbiKind::Int(bits) => {
            let padding = usize::from((256 - bits) / 8);
            let sign = word[padding] & 0x80 != 0;
            let expected = if sign { 0xff } else { 0x00 };
            if word[..padding].iter().any(|byte| *byte != expected) {
                return Err(format!("int{bits} value is not sign-extended"));
            }
            Ok(I256::from_raw(U256::from_be_bytes(word)).to_string())
        }
        AbiKind::Bool => match U256::from_be_bytes(word) {
            value if value == U256::ZERO => Ok("false".to_owned()),
            value if value == U256::from(1) => Ok("true".to_owned()),
            _ => Err("bool ABI value must be zero or one".to_owned()),
        },
        AbiKind::FixedBytes(bytes) => {
            let bytes = usize::from(bytes);
            if word[bytes..].iter().any(|byte| *byte != 0) {
                return Err("fixed bytes value contains non-zero ABI padding".to_owned());
            }
            Ok(format!("0x{}", hex::encode(&word[..bytes])))
        }
    }
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

fn valid_identifier(value: &str) -> bool {
    let mut bytes = value.bytes();
    bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn input_error(detail: impl Into<String>) -> ProcessorError {
    ProcessorError::Input(detail.into())
}

#[cfg(test)]
mod tests {
    use leani_primitives::{BlockRef, ChainId, Log, Material, TransactionHash, VerificationReport};
    use leani_testkit::MemoryReducer;

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
            finality: Finality::Included,
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
    async fn malformed_static_abi_data_fails_deterministically() {
        let processor = processor();
        let error = processor
            .map(&frame(vec![0; 31]))
            .await
            .expect_err("truncated ABI data");
        assert!(error.to_string().contains("expected 32 data bytes"));
    }

    #[test]
    fn dynamic_abi_types_fail_before_source_open() {
        let error = EvmEventsProcessor::new(EvmEventsConfig {
            start_block: BlockNumber(1),
            addresses: Vec::new(),
            events: vec![EventDefinition {
                abi: "event Message(string value)".to_owned(),
                output: EventOutput {
                    collection: "events.messages".to_owned(),
                    kind: "events.message".to_owned(),
                    key_fields: Vec::new(),
                    bucket_seconds: None,
                },
            }],
        })
        .expect_err("dynamic ABI rejected");
        assert!(error.to_string().contains("unsupported static ABI type"));
    }
}
