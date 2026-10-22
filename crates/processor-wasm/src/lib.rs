//! Processors loaded from sandboxed, metered Wasm modules.
//!
//! A [`WasmProcessor`] is an ordinary [`Processor`]: sources, runtimes, the
//! store, the API, and the SDK treat it exactly as a native processor. Its
//! `map` and `reduce` run in a fresh [wasmi](https://docs.rs/wasmi) instance
//! per call, under fuel and memory limits, with no clock, randomness,
//! network, or filesystem. The guest side of the ABI is `leani-wasm-guest`.
//!
//! Determinism comes from the host, not from guest discipline:
//!
//! - every call starts from a fresh instance, so no state leaks between
//!   blocks;
//! - the guest sees only the material and fields its manifest requires,
//!   reduced to its filter, so every source maps a block alike;
//! - the guest never sees finality, so a block's delta never depends on it;
//! - floats follow the Wasm deterministic profile.

mod frame;
mod host;

use std::{fmt, sync::Arc};

use async_trait::async_trait;
use leani_primitives::{
    BlockFrame, BlockHash, BlockNumber, Capability, CapabilitySet, FilterScope, Finality,
    LogFieldSet, ProcessorCursor, TopicFilter,
};
use leani_processor_api::{
    ChangeOperation, DataRequirement, DeliveryOrdering, DomainChange, DomainChanges, EncodedDelta,
    LifecyclePolicies, Processor, ProcessorDescriptor, ProcessorError, ProcessorId,
    ProcessorInstanceId, ProcessorSchemas, PublicationPolicy, ReducerTransaction, ReductionMode,
    StartPoint,
};
pub use leani_wasm_guest::Manifest;
use leani_wasm_guest::{BlockRef, MapInput, ReduceInput, Reduction};
use semver::Version;
use serde::{Deserialize, Serialize};

/// The processor kind every Wasm instance is configured as.
pub const PROCESSOR_ID: &str = "wasm";

/// An example module the node ships, for playgrounds, docs, and first runs.
#[derive(Clone, Copy, Debug)]
pub struct Template {
    pub name: &'static str,
    pub description: &'static str,
    /// Example parameters, as JSON.
    pub params: &'static str,
    pub module: &'static [u8],
    /// The Rust source the module is built from, its `src/lib.rs`.
    pub source: &'static str,
    /// The same processor in TypeScript, its `assembly/index.ts`, which
    /// publishes the same changes.
    pub typescript: &'static str,
}

/// The example processors of `examples/wasm-processors`, prebuilt by
/// `scripts/build-wasm-processors.sh`.
pub const TEMPLATES: [Template; 4] = [
    Template {
        name: "block-gas",
        description: "Gas used, limit, utilization, and base fee of every block; needs only block headers",
        params: r#"{"minUtilizationPercent":0}"#,
        module: include_bytes!("../modules/block_gas.wasm"),
        source: include_str!("../../../examples/wasm-processors/block-gas/src/lib.rs"),
        typescript: include_str!("../../../examples/wasm-processors/block-gas/assembly/index.ts"),
    },
    Template {
        name: "erc20-transfers",
        description: "ERC-20 transfers of chosen tokens at or above a minimum value; block-local",
        params: r#"{"tokens":["0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"],"minValue":"1000000000"}"#,
        module: include_bytes!("../modules/erc20_transfers.wasm"),
        source: include_str!("../../../examples/wasm-processors/erc20-transfers/src/lib.rs"),
        typescript: include_str!(
            "../../../examples/wasm-processors/erc20-transfers/assembly/index.ts"
        ),
    },
    Template {
        name: "token-volume",
        description: "Running transfer count and volume per token; ordered state",
        params: r#"{"tokens":["0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48","0xdac17f958d2ee523a2206206994597c13d831ec7"]}"#,
        module: include_bytes!("../modules/token_volume.wasm"),
        source: include_str!("../../../examples/wasm-processors/token-volume/src/lib.rs"),
        typescript: include_str!(
            "../../../examples/wasm-processors/token-volume/assembly/index.ts"
        ),
    },
    Template {
        name: "event-decoder",
        description: "Any contract's events, decoded from their Solidity signatures; no code to write",
        params: r#"{"addresses":["0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"],"events":["Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)"]}"#,
        module: include_bytes!("../modules/event_decoder.wasm"),
        source: include_str!("../../../examples/wasm-processors/event-decoder/src/lib.rs"),
        typescript: include_str!(
            "../../../examples/wasm-processors/event-decoder/assembly/index.ts"
        ),
    },
];

/// Resource limits for one module's calls. Exceeding a limit fails the call
/// with a processor error, as a native processor bug would.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct WasmLimits {
    /// Largest accepted module, in bytes.
    pub module_bytes: u64,
    /// Linear memory one instance may grow to, in bytes.
    pub memory_bytes: u64,
    /// Fuel for one describe, map, or reduce call: about one unit per
    /// executed Wasm instruction, and one per byte the host copies between
    /// itself and the module.
    pub fuel: u64,
    /// Largest delta, entity, state value, change payload, or hash input, in
    /// bytes.
    pub value_bytes: u64,
    /// Most changes one block may emit.
    pub changes: u64,
    /// Most state reads, writes, and change emits one block's reduce may make.
    pub host_calls: u64,
}

impl Default for WasmLimits {
    fn default() -> Self {
        Self {
            module_bytes: 16 << 20,
            memory_bytes: 64 << 20,
            fuel: 1_000_000_000,
            value_bytes: 8 << 20,
            changes: 100_000,
            host_calls: 1_000_000,
        }
    }
}

/// A validated module and its content hash, which becomes the processor's
/// `code_hash`: the store refuses a changed module under an existing
/// instance.
#[derive(Clone)]
pub struct WasmModule {
    module: wasmi::Module,
    hash: BlockHash,
}

impl fmt::Debug for WasmModule {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WasmModule")
            .field("hash", &self.hash)
            .finish_non_exhaustive()
    }
}

impl WasmModule {
    /// Validate a module and its ABI v1 imports and exports.
    ///
    /// # Errors
    ///
    /// Rejects an oversized or invalid module, an import outside the
    /// `leani` ABI (such as WASI), and missing or mistyped entry points.
    pub fn new(bytes: &[u8], limits: &WasmLimits) -> Result<Self, ProcessorError> {
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > limits.module_bytes {
            return Err(module_error(format!(
                "module is {} bytes, more than the {}-byte limit",
                bytes.len(),
                limits.module_bytes
            )));
        }
        let module = wasmi::Module::new(&host::ENGINE, bytes)
            .map_err(|error| module_error(format!("invalid Wasm module: {error}")))?;
        host::check_interface(&module).map_err(module_error)?;
        Ok(Self {
            module,
            hash: BlockHash::new(*blake3::hash(bytes).as_bytes()),
        })
    }

    /// The BLAKE3 hash of the module bytes.
    #[must_use]
    pub fn hash(&self) -> BlockHash {
        self.hash
    }
}

/// Operator-owned identity and policies of one configured instance.
#[derive(Clone, Debug)]
pub struct WasmContract {
    pub instance: ProcessorInstanceId,
    pub version: Version,
    pub start_block: BlockNumber,
    pub publication: PublicationPolicy,
    pub lifecycle: LifecyclePolicies,
}

/// A processor whose map and reduce run in a Wasm module.
#[derive(Clone, Debug)]
pub struct WasmProcessor {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    descriptor: ProcessorDescriptor,
    manifest: Manifest,
    module: WasmModule,
    /// Canonical JSON: object keys sorted, as the config hash saw them.
    params: Vec<u8>,
    projection: frame::Projection,
    limits: WasmLimits,
}

impl WasmProcessor {
    /// Describe the module with `params` and build its descriptor.
    ///
    /// # Errors
    ///
    /// Returns an error when the module fails to describe itself or declares
    /// an invalid manifest.
    pub fn new(
        module: WasmModule,
        params: &serde_json::Value,
        contract: WasmContract,
        limits: WasmLimits,
    ) -> Result<Self, ProcessorError> {
        // `serde_json::Value` keeps object keys sorted, so equal parameters
        // encode, and hash, alike however the configuration ordered them.
        let params = serde_json::to_vec(params)
            .map_err(|error| module_error(format!("parameters do not encode: {error}")))?;
        let manifest = host::describe(&module, &params, &limits, contract.instance.as_str())?;
        let projection = frame::Projection::new(&manifest.requires).map_err(module_error)?;
        let name = &manifest.name;
        if manifest.abi != leani_wasm_guest::ABI_VERSION {
            return Err(module_error(format!(
                "module declares ABI {}, this node supports {}",
                manifest.abi,
                leani_wasm_guest::ABI_VERSION
            )));
        }
        if name.is_empty()
            || name.len() > 64
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        {
            return Err(module_error(format!(
                "manifest name `{name}` must be 1-64 ASCII letters, digits, '.', '_' or '-'"
            )));
        }
        let (mode, delivery_ordering) = match manifest.reduction {
            Reduction::BlockLocal => (
                ReductionMode::BlockLocal,
                DeliveryOrdering::BlockVersionedIdempotent,
            ),
            Reduction::Ordered => (ReductionMode::OrderedState, DeliveryOrdering::Canonical),
        };
        let mut config = blake3::Hasher::new();
        config.update(b"leani/wasm/v1\0");
        config.update(&params);
        let descriptor = ProcessorDescriptor {
            id: ProcessorId::new(PROCESSOR_ID).map_err(|error| module_error(error.to_string()))?,
            instance: contract.instance,
            version: contract.version,
            code_hash: module.hash,
            config_hash: BlockHash::new(*config.finalize().as_bytes()),
            start: StartPoint::Block(contract.start_block),
            requirements: vec![projection.requirement()],
            mode,
            delivery_ordering,
            publication: contract.publication,
            lifecycle: contract.lifecycle,
            schemas: ProcessorSchemas {
                delta_version: 1,
                entity_schema: manifest
                    .entity_schema
                    .clone()
                    .unwrap_or_else(|| format!("wasm.{name}.entity.v1")),
                change_schema: manifest
                    .change_schema
                    .clone()
                    .unwrap_or_else(|| format!("wasm.{name}.change.v1")),
            },
        };
        descriptor
            .validate()
            .map_err(|error| module_error(error.to_owned()))?;
        Ok(Self {
            inner: Arc::new(Inner {
                descriptor,
                manifest,
                module,
                params,
                projection,
                limits,
            }),
        })
    }

    /// The manifest the module returned for its parameters.
    #[must_use]
    pub fn manifest(&self) -> &Manifest {
        &self.inner.manifest
    }
}

#[async_trait]
impl Processor for WasmProcessor {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn descriptor(&self) -> &ProcessorDescriptor {
        &self.inner.descriptor
    }

    async fn map(&self, block: &BlockFrame) -> Result<EncodedDelta, ProcessorError> {
        let inner = &self.inner;
        inner.descriptor.requirements[0]
            .validate_frame(block)
            .map_err(|error| ProcessorError::Input(error.to_owned()))?;
        let input = postcard::to_allocvec(&MapInput {
            params: inner.params.clone(),
            block: inner.projection.project(block)?,
        })
        .map_err(|error| ProcessorError::DeltaPayload(error.to_string()))?;
        // The interpreter is CPU-bound: run it on the blocking pool so
        // concurrent maps use several cores rather than one task's thread.
        let task = Arc::clone(inner);
        let payload = tokio::task::spawn_blocking(move || {
            host::map(
                &task.module,
                &input,
                &task.limits,
                task.descriptor.instance.as_str(),
            )
        })
        .await
        .map_err(|error| guest_error("map", &error.to_string()))??;
        Ok(EncodedDelta::new(
            &inner.descriptor,
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
        let descriptor = &self.inner.descriptor;
        delta.validate(descriptor)?;
        if cursor.processor_id != descriptor.id.as_str()
            || cursor.processor_version != descriptor.version.to_string()
            || cursor.chain_id != delta.chain_id
            || cursor.block_number != delta.block.number
            || cursor.block_hash != delta.block.hash
        {
            return Err(ProcessorError::CursorMismatch);
        }
        let input = postcard::to_allocvec(&ReduceInput {
            params: self.inner.params.clone(),
            block: BlockRef {
                chain_id: delta.chain_id.0,
                number: delta.block.number.0,
                hash: delta.block.hash.0,
                parent_hash: delta.block.parent_hash.0,
                timestamp: delta.block.timestamp,
            },
            delta: delta.payload.clone(),
        })
        .map_err(|error| ProcessorError::DeltaPayload(error.to_string()))?;
        let changes = host::reduce(
            &self.inner.module,
            &input,
            &self.inner.limits,
            descriptor.mode == ReductionMode::OrderedState,
            descriptor.instance.as_str(),
            transaction,
        )
        .await?;
        for change in &changes {
            transaction.emit(change.clone()).await?;
        }
        Ok(DomainChanges { changes })
    }

    fn change_json(
        &self,
        change: &DomainChange,
    ) -> Result<Option<serde_json::Value>, ProcessorError> {
        if change.operation == ChangeOperation::Delete && change.payload.is_empty() {
            return Ok(Some(serde_json::Value::Null));
        }
        json(&change.payload).map(Some)
    }

    fn entity_json(
        &self,
        _collection: &str,
        _key: &[u8],
        value: &[u8],
    ) -> Result<Option<serde_json::Value>, ProcessorError> {
        json(value).map(Some)
    }
}

fn json(bytes: &[u8]) -> Result<serde_json::Value, ProcessorError> {
    serde_json::from_slice(bytes).map_err(|error| ProcessorError::State(error.to_string()))
}

fn module_error(message: impl Into<String>) -> ProcessorError {
    ProcessorError::Invariant(format!("wasm module: {}", message.into()))
}

fn guest_error(call: &str, message: &str) -> ProcessorError {
    ProcessorError::Invariant(format!("wasm {call}: {message}"))
}

fn requirement(
    capabilities: CapabilitySet,
    log_transaction_hash: bool,
    filter: FilterScope,
) -> DataRequirement {
    DataRequirement {
        capabilities,
        log_fields: if log_transaction_hash {
            LogFieldSet::ALL
        } else {
            LogFieldSet::NONE
        },
        allow_filtered: true,
        filter,
        minimum_finality: Finality::Unattested,
    }
}

fn capability(capability: leani_wasm_guest::Capability) -> Capability {
    use leani_wasm_guest::Capability as Guest;
    match capability {
        Guest::Header => Capability::Header,
        Guest::Body => Capability::Body,
        Guest::Transactions => Capability::Transactions,
        Guest::Calldata => Capability::Calldata,
        Guest::Senders => Capability::Senders,
        Guest::BlobFields => Capability::BlobFields,
        Guest::BlockSize => Capability::BlockSize,
        Guest::Receipts => Capability::Receipts,
        Guest::ReceiptOutcome => Capability::ReceiptOutcome,
        Guest::Logs => Capability::Logs,
        Guest::Withdrawals => Capability::Withdrawals,
    }
}

fn topic_filters(filter: &leani_wasm_guest::Filter) -> Vec<TopicFilter> {
    filter
        .topics
        .iter()
        .map(|topic| TopicFilter {
            position: topic.position,
            alternatives: topic.alternatives.clone(),
        })
        .collect()
}

#[cfg(test)]
mod tests;
