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
    /// Dynamic ABI values, unnamed parameters, anonymous events, duplicate
    /// signatures, invalid output names, and impossible key/bucket mappings
    /// fail at construction before a source range is opened.
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
        // Events are hashed in configured order, so reordering them creates a
        // new instance identity. Sorting them here would change the identity
        // of every existing instance.
        let normalized = postcard::to_allocvec(&config)
            .map_err(|error| input_error(format!("configuration encoding failed: {error}")))?;
        let id = ProcessorId::new("evm-events").map_err(|error| input_error(error.to_string()))?;
        let version = Version::new(1, 1, 0);
        let config_hash = BlockHash::new(*blake3::hash(&normalized).as_bytes());
        let topics = parsed.iter().map(|event| event.topic0).collect();
        // A configured key can repeat across blocks, so which event it holds
        // depends on the order its blocks reduce in, and an undo restores the
        // value from before the block. Keyed output is therefore ordered, and
        // its live lane waits for its history. The default key is unique to
        // one event, so keyless output stays block-local.
        let (mode, delivery_ordering) = if parsed
            .iter()
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
                    topics: vec![TopicFilter {
                        position: 0,
                        alternatives: topics,
                    }],
                    ..FilterScope::default()
                },
                minimum_finality: Finality::Included,
            }],
            mode,
            delivery_ordering,
            publication: PublicationPolicy::IncludedAndFinalized,
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
        let mut skipped_logs = 0_u32;
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
            // Another contract can emit this signature with another layout,
            // such as an ERC-721 `Transfer` for the ERC-20 ABI. A log that
            // does not decode is not this event, so it is counted, not fatal.
            let Ok(values) = decode_values(event, log) else {
                skipped_logs = skipped_logs.saturating_add(1);
                continue;
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
        let mut checksums = Vec::with_capacity(2);
        for finality in [Finality::Included, Finality::Finalized] {
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
    let fragment = definition
        .abi
        .trim()
        .strip_prefix("event ")
        .unwrap_or(definition.abi.trim())
        .trim_end_matches(';')
        .trim();
    let open = fragment
        .find('(')
        .ok_or_else(|| input_error("event ABI is missing `(`"))?;
    let close = fragment
        .rfind(')')
        .ok_or_else(|| input_error("event ABI is missing `)`"))?;
    // `anonymous` is an attribute after the parameter list; a parameter
    // name such as `anonymousVoter` is not.
    match fragment[close + 1..].trim() {
        "anonymous" => return Err(input_error("anonymous events are not supported")),
        trailing if close <= open || !trailing.is_empty() => {
            return Err(input_error("event ABI has trailing or unbalanced syntax"));
        }
        _ => {}
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
                // `type indexed` is an unnamed indexed parameter, not a field
                // named `indexed`; output values need a name.
                [kind, field] if *field != "indexed" => (parse_kind(kind)?, false, *field),
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
        frame.finality = Finality::Included;
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
            assert_eq!(variants.len(), 2);
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
        assert_eq!(event.fields[0].name, "anonymousVoter");
        assert!(event.fields[0].indexed);
        let error = parse("event Ping(uint256 value) anonymous")
            .expect_err("anonymous events are rejected");
        assert!(
            error
                .to_string()
                .contains("anonymous events are not supported")
        );
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
