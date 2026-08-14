//! Deterministic map/reduce and state mutation boundaries.

use std::any::Any;

use async_trait::async_trait;
use leani_primitives::{BlockFrame, BlockHash, BlockRef, ChainId, ProcessorCursor};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{ProcessorDescriptor, ProcessorId};

/// Versioned, checksummed output of the parallel map stage.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EncodedDelta {
    pub processor_id: ProcessorId,
    pub processor_version: String,
    pub config_hash: BlockHash,
    pub schema_version: u16,
    pub chain_id: ChainId,
    pub block: BlockRef,
    pub payload: Vec<u8>,
    pub checksum: BlockHash,
}

impl EncodedDelta {
    /// Create a deterministic delta tied to an exact processor/config/block.
    #[must_use]
    pub fn new(
        descriptor: &ProcessorDescriptor,
        chain_id: ChainId,
        block: BlockRef,
        payload: Vec<u8>,
    ) -> Self {
        let mut delta = Self {
            processor_id: descriptor.id.clone(),
            processor_version: descriptor.version.to_string(),
            config_hash: descriptor.config_hash,
            schema_version: descriptor.schemas.delta_version,
            chain_id,
            block,
            payload,
            checksum: BlockHash::ZERO,
        };
        delta.checksum = delta.calculate_checksum();
        delta
    }

    /// Verify identity, schema, configuration, and payload integrity.
    ///
    /// # Errors
    ///
    /// Returns [`ProcessorError`] when this delta belongs to another processor
    /// contract or its checksum is invalid.
    pub fn validate(&self, descriptor: &ProcessorDescriptor) -> Result<(), ProcessorError> {
        if self.processor_id != descriptor.id
            || self.processor_version != descriptor.version.to_string()
            || self.config_hash != descriptor.config_hash
            || self.schema_version != descriptor.schemas.delta_version
        {
            return Err(ProcessorError::DeltaContract);
        }
        if self.checksum != self.calculate_checksum() {
            return Err(ProcessorError::DeltaChecksum);
        }
        Ok(())
    }

    /// Serialize the delta in the shared versioned durable envelope.
    ///
    /// # Errors
    ///
    /// Returns [`ProcessorError::Durable`] when serialization fails.
    pub fn encode_durable(&self) -> Result<Vec<u8>, ProcessorError> {
        leani_primitives::durable::encode(
            leani_primitives::DurableKind::ProcessorDelta,
            self.schema_version,
            self,
        )
        .map_err(|error| ProcessorError::Durable(error.to_string()))
    }

    /// Decode and validate a delta against an exact descriptor.
    ///
    /// # Errors
    ///
    /// Rejects unknown schemas, corrupt records, checksum failures, and
    /// processor/config/version mismatches.
    pub fn decode_durable(
        descriptor: &ProcessorDescriptor,
        bytes: &[u8],
    ) -> Result<Self, ProcessorError> {
        let delta = leani_primitives::durable::decode(
            leani_primitives::DurableKind::ProcessorDelta,
            descriptor.schemas.delta_version,
            bytes,
        )
        .map_err(|error| ProcessorError::Durable(error.to_string()))?;
        Self::validate(&delta, descriptor)?;
        Ok(delta)
    }

    fn calculate_checksum(&self) -> BlockHash {
        let mut hasher = blake3::Hasher::new();
        hash_bytes(&mut hasher, self.processor_id.as_str().as_bytes());
        hash_bytes(&mut hasher, self.processor_version.as_bytes());
        hasher.update(&self.config_hash.0);
        hasher.update(&self.schema_version.to_be_bytes());
        hasher.update(&self.chain_id.0.to_be_bytes());
        hasher.update(&self.block.number.0.to_be_bytes());
        hasher.update(&self.block.hash.0);
        hasher.update(&self.block.parent_hash.0);
        hasher.update(&self.block.timestamp.to_be_bytes());
        hash_bytes(&mut hasher, &self.payload);
        BlockHash::new(*hasher.finalize().as_bytes())
    }
}

fn hash_bytes(hasher: &mut blake3::Hasher, bytes: &[u8]) {
    hasher.update(&u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_be_bytes());
    hasher.update(bytes);
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ChangeOperation {
    Upsert,
    Delete,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DomainChange {
    pub kind: String,
    pub key: Vec<u8>,
    pub operation: ChangeOperation,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct DomainChanges {
    pub changes: Vec<DomainChange>,
}

/// Only mutation surface available to a processor reducer.
#[async_trait]
pub trait ReducerTransaction: Send {
    /// Read processor-private working state.
    ///
    /// Working state is durable for ordered reducers but is never exposed as a
    /// queryable output collection. The default preserves compatibility with
    /// lightweight test transactions implemented before the state/output
    /// split.
    async fn state_get(
        &mut self,
        namespace: &str,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, ProcessorError> {
        self.get(namespace, key).await
    }

    /// Upsert processor-private working state.
    async fn state_put(
        &mut self,
        namespace: &str,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), ProcessorError> {
        self.put(namespace, key, value).await
    }

    /// Delete processor-private working state.
    async fn state_delete(&mut self, namespace: &str, key: &[u8]) -> Result<(), ProcessorError> {
        self.delete(namespace, key).await
    }

    /// Scan processor-private working state in deterministic key order.
    async fn state_scan_prefix(
        &mut self,
        namespace: &str,
        prefix: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ProcessorError> {
        self.scan_prefix(namespace, prefix, limit).await
    }

    /// Read a public materialized entity.
    async fn get(
        &mut self,
        collection: &str,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, ProcessorError>;

    async fn put(
        &mut self,
        collection: &str,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), ProcessorError>;

    async fn delete(&mut self, collection: &str, key: &[u8]) -> Result<(), ProcessorError>;

    async fn index_put(
        &mut self,
        index: &str,
        index_key: Vec<u8>,
        entity_key: Vec<u8>,
    ) -> Result<(), ProcessorError>;

    async fn index_delete(
        &mut self,
        index: &str,
        index_key: &[u8],
        entity_key: &[u8],
    ) -> Result<(), ProcessorError>;

    async fn scan_prefix(
        &mut self,
        collection: &str,
        prefix: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ProcessorError>;

    async fn emit(&mut self, change: DomainChange) -> Result<(), ProcessorError>;
}

#[async_trait]
pub trait Processor: Send + Sync {
    /// Runtime type access used only by optional typed API adapters. The
    /// scheduler and store remain fully trait-driven.
    fn as_any(&self) -> &dyn Any;

    fn descriptor(&self) -> &ProcessorDescriptor;

    async fn map(&self, block: &BlockFrame) -> Result<EncodedDelta, ProcessorError>;

    /// Return checksums that differ from this durable delta only by processor-
    /// declared publication finality.
    ///
    /// The default is exact-checksum equivalence. Processors whose mapped
    /// payload embeds finality may override this by decoding the validated
    /// delta, rebuilding every finality variant, and returning those checksums.
    /// This hook lets restart recovery prove equivalence after the raw input
    /// frame has been pruned; it must never broaden equivalence to other
    /// payload fields.
    ///
    /// # Errors
    ///
    /// Returns a processor error when the delta is invalid or its payload
    /// cannot be decoded under the processor's declared schema.
    fn finality_variant_checksums(
        &self,
        delta: &EncodedDelta,
    ) -> Result<Vec<BlockHash>, ProcessorError> {
        delta.validate(self.descriptor())?;
        Ok(vec![delta.checksum])
    }

    async fn reduce(
        &self,
        transaction: &mut dyn ReducerTransaction,
        cursor: &ProcessorCursor,
        delta: &EncodedDelta,
    ) -> Result<DomainChanges, ProcessorError>;

    /// Render a domain change as public JSON when the processor owns a stable
    /// JSON representation for its payload.
    ///
    /// The durable payload remains the processor's compact binary encoding.
    /// Returning `None` keeps that payload opaque and lets the API expose it as
    /// schema-labelled hexadecimal bytes. A future Wasm/package factory can
    /// implement the same hook from a packaged schema without changing store
    /// encodings.
    ///
    /// # Errors
    ///
    /// Returns a processor error when a claimed public representation cannot
    /// be decoded or rendered.
    fn change_json(
        &self,
        _change: &DomainChange,
    ) -> Result<Option<serde_json::Value>, ProcessorError> {
        Ok(None)
    }

    /// Render one retained public entity as JSON.
    ///
    /// The default keeps the entity payload opaque. Native and packaged
    /// processors can expose a stable representation without teaching the
    /// generic API about processor-specific binary layouts.
    ///
    /// # Errors
    ///
    /// Returns a processor error when a claimed entity representation cannot
    /// be decoded.
    fn entity_json(
        &self,
        _collection: &str,
        _key: &[u8],
        _value: &[u8],
    ) -> Result<Option<serde_json::Value>, ProcessorError> {
        Ok(None)
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ProcessorError {
    #[error("processor input is missing required material: {0}")]
    Input(String),
    #[error("encoded delta does not match the processor contract")]
    DeltaContract,
    #[error("encoded delta checksum mismatch")]
    DeltaChecksum,
    #[error("encoded delta payload is invalid: {0}")]
    DeltaPayload(String),
    #[error("durable delta is invalid: {0}")]
    Durable(String),
    #[error("processor cursor does not match the delta")]
    CursorMismatch,
    #[error("processor state operation failed: {0}")]
    State(String),
    #[error("processor invariant failed: {0}")]
    Invariant(String),
}

#[cfg(test)]
mod tests {
    use semver::Version;

    use super::*;
    use crate::{
        DataRequirement, DeliveryOrdering, LifecyclePolicies, ProcessorInstanceId,
        ProcessorSchemas, PublicationPolicy, ReductionMode, RetentionPolicy, StartPoint,
    };
    use leani_primitives::{BlockNumber, Capability, CapabilitySet, FilterScope, Finality};

    fn descriptor() -> ProcessorDescriptor {
        let id = ProcessorId::new("fixture").expect("processor ID");
        let version = Version::new(1, 2, 3);
        let config_hash = BlockHash::new([2; 32]);
        ProcessorDescriptor {
            instance: ProcessorInstanceId::legacy(&id, &version, config_hash),
            id,
            version,
            code_hash: BlockHash::new([1; 32]),
            config_hash,
            start: StartPoint::Genesis,
            requirements: vec![DataRequirement {
                capabilities: CapabilitySet::of(Capability::Header),
                log_fields: leani_primitives::LogFieldSet::NONE,
                allow_filtered: false,
                filter: FilterScope::default(),
                minimum_finality: Finality::Optimistic,
            }],
            mode: ReductionMode::BlockLocal,
            delivery_ordering: DeliveryOrdering::BlockVersionedIdempotent,
            publication: PublicationPolicy::FinalizedOnly,
            lifecycle: LifecyclePolicies::from_legacy(RetentionPolicy::FullOutputHistory),
            schemas: ProcessorSchemas {
                delta_version: 1,
                entity_schema: "fixture.entity.v1".to_owned(),
                change_schema: "fixture.change.v1".to_owned(),
            },
        }
    }

    #[test]
    fn delta_detects_corruption_and_contract_drift() {
        let descriptor = descriptor();
        let block = BlockRef {
            number: BlockNumber(4),
            hash: BlockHash::new([4; 32]),
            parent_hash: BlockHash::new([3; 32]),
            timestamp: 4,
        };
        let mut delta = EncodedDelta::new(&descriptor, ChainId(1), block, vec![1, 2, 3]);
        delta.validate(&descriptor).expect("valid delta");
        delta.payload[0] ^= 1;
        assert_eq!(
            delta.validate(&descriptor),
            Err(ProcessorError::DeltaChecksum)
        );
    }

    #[test]
    fn delta_round_trips_through_versioned_durable_envelope() {
        let descriptor = descriptor();
        let block = BlockRef {
            number: BlockNumber(4),
            hash: BlockHash::new([4; 32]),
            parent_hash: BlockHash::new([3; 32]),
            timestamp: 4,
        };
        let delta = EncodedDelta::new(&descriptor, ChainId(1), block, vec![1, 2, 3]);
        let encoded = delta.encode_durable().expect("encode delta");
        assert_eq!(
            EncodedDelta::decode_durable(&descriptor, &encoded).expect("decode delta"),
            delta
        );
        let mut corrupt = encoded;
        *corrupt.last_mut().expect("nonempty record") ^= 1;
        assert!(EncodedDelta::decode_durable(&descriptor, &corrupt).is_err());
    }
}
