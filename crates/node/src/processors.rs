//! Public processor factory and registry boundary.
//!
//! The standard binary registers the checked-in native processors and the
//! `wasm` factory, which loads Wasm modules through the same
//! [`ProcessorFactory`] contract. Downstream binaries can register additional
//! factories without forking node assembly.

// The public factory trait deliberately permits IDs and descriptions owned by
// dynamically loaded implementations; built-ins happen to return literals.
#![allow(clippy::unnecessary_literal_bound)]

use std::{collections::BTreeMap, fmt, sync::Arc};

use leani_api::{
    BlobsQueryExtension, BlockSummaryQueryExtension, Erc20QueryExtension, QueryExtension,
    QueryExtensionRegistration, UniswapObservationsQueryExtension, UniswapQueryExtension,
};
use leani_primitives::{Address, BlockNumber};
use leani_processor_api::{
    LifecyclePolicies, OutputPolicyMode, Processor, ProcessorInstanceId, PublicationPolicy,
    ReductionMode, StartPoint,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::config::{Config, ProcessorConfig, ValidationError};

/// Immutable context supplied to every processor factory.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProcessorFactoryContext {
    pub chain_id: u64,
}

/// Processor construction failure suitable for configuration diagnostics.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("{message}")]
pub struct ProcessorFactoryError {
    message: String,
}

/// Stable human-facing metadata supplied by every registered processor factory.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProcessorFactoryMetadata {
    pub id: String,
    pub description: String,
}

/// One processor instance and its optional native read-only query surface.
#[derive(Clone)]
pub struct ProcessorComponents {
    pub processor: Arc<dyn Processor>,
    pub query_extension: Option<Arc<dyn QueryExtension>>,
}

impl ProcessorComponents {
    #[must_use]
    pub fn new(processor: Arc<dyn Processor>) -> Self {
        Self {
            processor,
            query_extension: None,
        }
    }

    #[must_use]
    pub fn with_query_extension(mut self, extension: Arc<dyn QueryExtension>) -> Self {
        self.query_extension = Some(extension);
        self
    }
}

impl fmt::Debug for ProcessorComponents {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProcessorComponents")
            .field("processor", &self.processor.descriptor().instance)
            .field(
                "query_extension",
                &self
                    .query_extension
                    .as_ref()
                    .map(|extension| extension.id()),
            )
            .finish()
    }
}

/// Fully instantiated processor set used to assemble the serving process.
#[derive(Clone)]
pub struct ProcessorAssembly {
    pub processors: Vec<Arc<dyn Processor>>,
    pub query_extensions: Vec<QueryExtensionRegistration>,
}

impl fmt::Debug for ProcessorAssembly {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProcessorAssembly")
            .field(
                "processors",
                &self
                    .processors
                    .iter()
                    .map(|processor| &processor.descriptor().instance)
                    .collect::<Vec<_>>(),
            )
            .field("query_extensions", &self.query_extensions)
            .finish()
    }
}

impl ProcessorFactoryError {
    #[must_use]
    pub fn configuration(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// Constructs one configured processor without opening networks; only the
/// `wasm` factory reads a file, its module.
///
/// Factories are trusted native code. The `wasm` factory runs each module in
/// a sandboxed interpreter behind the same node/runtime boundary.
pub trait ProcessorFactory: Send + Sync {
    fn id(&self) -> &str;

    fn description(&self) -> &str {
        self.id()
    }

    /// Build and validate one immutable processor instance.
    ///
    /// # Errors
    ///
    /// Returns a configuration error for invalid processor-owned settings,
    /// unsupported chains, or inconsistent declared behavior.
    fn create(
        &self,
        configured: &ProcessorConfig,
        context: ProcessorFactoryContext,
    ) -> Result<ProcessorComponents, ProcessorFactoryError>;
}

/// Cloneable registry shared by every command path.
#[derive(Clone, Default)]
pub struct ProcessorRegistry {
    factories: Arc<BTreeMap<String, Arc<dyn ProcessorFactory>>>,
}

impl fmt::Debug for ProcessorRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProcessorRegistry")
            .field("ids", &self.factories.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl ProcessorRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registry used by the distributed `leani` binary.
    #[must_use]
    pub fn standard() -> Self {
        let mut factories: BTreeMap<String, Arc<dyn ProcessorFactory>> = BTreeMap::new();
        for factory in [
            Arc::new(BlockSummaryProcessorFactory) as Arc<dyn ProcessorFactory>,
            Arc::new(BlobsProcessorFactory) as Arc<dyn ProcessorFactory>,
            Arc::new(Erc20ProcessorFactory),
            Arc::new(EvmEventsProcessorFactory),
            Arc::new(RollupTxsProcessorFactory),
            Arc::new(TransactionStatsProcessorFactory),
            Arc::new(UniswapObservationsProcessorFactory),
            Arc::new(UniswapLatestProcessorFactory),
            Arc::new(WasmProcessorFactory),
        ] {
            factories.insert(factory.id().to_owned(), factory);
        }
        Self {
            factories: Arc::new(factories),
        }
    }

    /// Register a native or package-backed processor factory.
    ///
    /// # Errors
    ///
    /// Rejects an empty or duplicate processor ID.
    pub fn register(
        &mut self,
        factory: impl ProcessorFactory + 'static,
    ) -> Result<(), ProcessorRegistryError> {
        self.register_shared(Arc::new(factory))
    }

    /// Register an already shared factory discovered by an application or
    /// future package loader.
    ///
    /// # Errors
    ///
    /// Rejects an empty or duplicate processor ID.
    pub fn register_shared(
        &mut self,
        factory: Arc<dyn ProcessorFactory>,
    ) -> Result<(), ProcessorRegistryError> {
        let id = factory.id().to_owned();
        if id.trim().is_empty() {
            return Err(ProcessorRegistryError::EmptyId);
        }
        let factories = Arc::make_mut(&mut self.factories);
        if factories.contains_key(&id) {
            return Err(ProcessorRegistryError::Duplicate(id));
        }
        factories.insert(id, factory);
        Ok(())
    }

    #[must_use]
    pub fn contains(&self, id: &str) -> bool {
        self.factories.contains_key(id)
    }

    /// Sorted built-in and downstream factory metadata.
    #[must_use]
    pub fn catalog(&self) -> Vec<ProcessorFactoryMetadata> {
        self.factories
            .values()
            .map(|factory| ProcessorFactoryMetadata {
                id: factory.id().to_owned(),
                description: factory.description().to_owned(),
            })
            .collect()
    }

    /// Instantiate and cross-check one configured processor.
    ///
    /// # Errors
    ///
    /// Rejects unknown IDs and descriptor/configuration disagreement.
    pub fn instantiate(
        &self,
        configured: &ProcessorConfig,
        chain_id: u64,
    ) -> Result<Arc<dyn Processor>, ProcessorRegistryError> {
        Ok(self.instantiate_components(configured, chain_id)?.processor)
    }

    /// Instantiate one processor together with its optional query extension.
    ///
    /// # Errors
    ///
    /// Rejects unknown IDs and descriptor/configuration disagreement.
    pub fn instantiate_components(
        &self,
        configured: &ProcessorConfig,
        chain_id: u64,
    ) -> Result<ProcessorComponents, ProcessorRegistryError> {
        let factory = self
            .factories
            .get(&configured.id)
            .ok_or_else(|| ProcessorRegistryError::Unknown(configured.id.clone()))?;
        let components = factory
            .create(configured, ProcessorFactoryContext { chain_id })
            .map_err(|error| ProcessorRegistryError::Factory {
                id: configured.id.clone(),
                error,
            })?;
        validate_instance(configured, components.processor.as_ref())?;
        Ok(components)
    }

    /// Instantiate every configured processor in configuration order.
    ///
    /// # Errors
    ///
    /// Returns the first unknown, invalid, or inconsistent processor.
    pub fn instantiate_all(
        &self,
        config: &Config,
    ) -> Result<Vec<Arc<dyn Processor>>, ProcessorRegistryError> {
        config
            .processors
            .iter()
            .map(|configured| self.instantiate(configured, config.chain.chain_id))
            .collect()
    }

    /// Instantiate every configured processor and preserve all factory-owned
    /// query-extension registrations for HTTP assembly.
    ///
    /// # Errors
    ///
    /// Returns the first unknown, invalid, or inconsistent processor.
    pub fn instantiate_all_with_extensions(
        &self,
        config: &Config,
    ) -> Result<ProcessorAssembly, ProcessorRegistryError> {
        let mut processors = Vec::with_capacity(config.processors.len());
        let mut extensions = Vec::new();
        for configured in &config.processors {
            let components = self.instantiate_components(configured, config.chain.chain_id)?;
            if let Some(extension) = components.query_extension {
                extensions.push(QueryExtensionRegistration::new(
                    components.processor.clone(),
                    extension,
                ));
            }
            processors.push(components.processor);
        }
        Ok(ProcessorAssembly {
            processors,
            query_extensions: extensions,
        })
    }

    pub(crate) fn validation_errors(&self, config: &Config) -> Vec<ValidationError> {
        config
            .processors
            .iter()
            .enumerate()
            .filter_map(|(index, configured)| {
                self.instantiate(configured, config.chain.chain_id)
                    .err()
                    .map(|error| {
                        ValidationError::new(format!("processors[{index}]"), error.to_string())
                    })
            })
            .collect()
    }

    /// Settings that are valid but unlikely to do what they suggest. A
    /// processor that fails to load is a validation error instead.
    pub(crate) fn validation_warnings(&self, config: &Config) -> Vec<String> {
        config
            .processors
            .iter()
            .enumerate()
            .filter_map(|(index, configured)| {
                let processor = self.instantiate(configured, config.chain.chain_id).ok()?;
                output_warning(index, processor.as_ref())
            })
            .collect()
    }
}

/// `latest` keeps the current value of each key, which stays small only for a
/// processor that writes a fixed set of keys, such as an ordered aggregate. A
/// block-local processor usually writes new keys every block, one per event
/// or block, so its output grows as under `full`.
pub(crate) fn output_warning(index: usize, processor: &dyn Processor) -> Option<String> {
    let descriptor = processor.descriptor();
    (descriptor.lifecycle.output.mode == OutputPolicyMode::Latest
        && descriptor.mode == ReductionMode::BlockLocal)
        .then(|| {
            format!(
                "processors[{index}] ({}): output \"latest\" keeps the current value of each key, and this block-local processor usually writes new keys every block, so its output grows as with \"full\"; bound it with output \"window\", or keep one value per key with an ordered processor",
                descriptor.instance
            )
        })
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ProcessorRegistryError {
    #[error("processor factory ID must not be empty")]
    EmptyId,
    #[error("processor factory `{0}` is registered more than once")]
    Duplicate(String),
    #[error("processor `{0}` is not registered in this node binary")]
    Unknown(String),
    #[error("processor `{id}` configuration is invalid: {error}")]
    Factory {
        id: String,
        error: ProcessorFactoryError,
    },
    #[error("processor `{id}` descriptor is inconsistent: {message}")]
    Descriptor { id: String, message: String },
}

fn validate_instance(
    configured: &ProcessorConfig,
    processor: &dyn Processor,
) -> Result<(), ProcessorRegistryError> {
    let descriptor = processor.descriptor();
    descriptor
        .validate()
        .map_err(|message| descriptor_error(configured, message))?;
    if descriptor.id.as_str() != configured.id {
        return Err(descriptor_error(
            configured,
            format!("factory returned ID `{}`", descriptor.id),
        ));
    }
    let expected_instance = ProcessorInstanceId::new(&configured.instance)
        .map_err(|error| descriptor_error(configured, error.to_string()))?;
    if descriptor.instance != expected_instance {
        return Err(descriptor_error(
            configured,
            format!(
                "configured instance {} differs from descriptor {}",
                expected_instance, descriptor.instance
            ),
        ));
    }
    if descriptor.version.to_string() != configured.version {
        return Err(descriptor_error(
            configured,
            format!(
                "configured version {} differs from implementation {}",
                configured.version, descriptor.version
            ),
        ));
    }
    if descriptor.start != StartPoint::Block(BlockNumber(configured.start_block)) {
        return Err(descriptor_error(
            configured,
            format!(
                "configured start block {} differs from descriptor {:?}",
                configured.start_block, descriptor.start
            ),
        ));
    }
    if descriptor.publication != configured.publication_policy() {
        return Err(descriptor_error(
            configured,
            "configured publication policy differs from implementation",
        ));
    }
    let lifecycle = configured
        .lifecycle_policies()
        .map_err(|message| descriptor_error(configured, message))?;
    if descriptor.lifecycle != lifecycle {
        return Err(descriptor_error(
            configured,
            "configured lifecycle policies differ from implementation",
        ));
    }
    if configured.live_gap_fill.is_some() && descriptor.mode != ReductionMode::BlockLocal {
        return Err(descriptor_error(
            configured,
            "live_gap_fill requires a block-local processor; an ordered lane holds later blocks instead of skipping them",
        ));
    }
    Ok(())
}

fn descriptor_error(
    configured: &ProcessorConfig,
    message: impl Into<String>,
) -> ProcessorRegistryError {
    ProcessorRegistryError::Descriptor {
        id: configured.id.clone(),
        message: message.into(),
    }
}

fn configured_contract(
    configured: &ProcessorConfig,
) -> Result<(ProcessorInstanceId, PublicationPolicy, LifecyclePolicies), ProcessorFactoryError> {
    let instance = ProcessorInstanceId::new(&configured.instance)
        .map_err(|error| ProcessorFactoryError::configuration(error.to_string()))?;
    let lifecycle = configured
        .lifecycle_policies()
        .map_err(ProcessorFactoryError::configuration)?;
    Ok((instance, configured.publication_policy(), lifecycle))
}

#[derive(Clone, Copy, Debug)]
struct BlockSummaryProcessorFactory;

impl ProcessorFactory for BlockSummaryProcessorFactory {
    fn id(&self) -> &str {
        "block-summary"
    }

    fn description(&self) -> &str {
        "Ethereum block summaries with transaction counts, from verified or dataset-declared blocks"
    }

    fn create(
        &self,
        configured: &ProcessorConfig,
        _context: ProcessorFactoryContext,
    ) -> Result<ProcessorComponents, ProcessorFactoryError> {
        if !configured.settings.is_empty() {
            return Err(ProcessorFactoryError::configuration(
                "the block-summary processor takes no settings",
            ));
        }
        let processor = leani_processor_block_summary::BlockSummaryProcessor::new(
            leani_processor_block_summary::BlockSummaryConfig {
                start_block: BlockNumber(configured.start_block),
            },
        )
        .map_err(|error| ProcessorFactoryError::configuration(error.to_string()))?;
        let (instance, publication, lifecycle) = configured_contract(configured)?;
        Ok(ProcessorComponents::new(Arc::new(processor.with_contract(
            instance,
            publication,
            lifecycle,
        )))
        .with_query_extension(Arc::new(BlockSummaryQueryExtension)))
    }
}

#[derive(Clone, Copy, Debug)]
struct BlobsProcessorFactory;

impl ProcessorFactory for BlobsProcessorFactory {
    fn id(&self) -> &str {
        "blobs-money"
    }

    fn description(&self) -> &str {
        "Mainnet blob blocks, transactions, fees, burn, capacity, and fork-aware schedule"
    }

    fn create(
        &self,
        configured: &ProcessorConfig,
        context: ProcessorFactoryContext,
    ) -> Result<ProcessorComponents, ProcessorFactoryError> {
        if context.chain_id != 1 {
            return Err(ProcessorFactoryError::configuration(
                "the built-in blobs processor supports Ethereum mainnet only",
            ));
        }
        if !configured.settings.is_empty() {
            return Err(ProcessorFactoryError::configuration(
                "the embedded mainnet blob schedule takes no settings",
            ));
        }
        let processor = leani_processor_blobs::BlobsProcessor::new_at(
            leani_processor_blobs::BlobSchedule::mainnet(),
            BlockNumber(configured.start_block),
        )
        .map_err(|error| ProcessorFactoryError::configuration(error.to_string()))?;
        let (instance, publication, lifecycle) = configured_contract(configured)?;
        let processor = processor.with_contract(instance, publication, lifecycle);
        let erased: Arc<dyn Processor> = Arc::new(processor);
        Ok(ProcessorComponents::new(erased).with_query_extension(Arc::new(BlobsQueryExtension)))
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Erc20Settings {
    addresses: Vec<String>,
    #[serde(default)]
    tokens: Vec<String>,
    #[serde(default)]
    complete_from_start: bool,
}

#[derive(Clone, Copy, Debug)]
struct Erc20ProcessorFactory;

impl ProcessorFactory for Erc20ProcessorFactory {
    fn id(&self) -> &str {
        "erc20-balances"
    }

    fn description(&self) -> &str {
        "Watchlist ERC-20 transfer ledger with current balances"
    }

    fn create(
        &self,
        configured: &ProcessorConfig,
        _context: ProcessorFactoryContext,
    ) -> Result<ProcessorComponents, ProcessorFactoryError> {
        let settings: Erc20Settings = decode_settings(configured)?;
        let processor = leani_processor_erc20::Erc20BalanceProcessor::new(
            leani_processor_erc20::Erc20BalanceConfig {
                start_block: BlockNumber(configured.start_block),
                addresses: settings
                    .addresses
                    .iter()
                    .map(|value| parse_address(value))
                    .collect::<Result<_, _>>()?,
                tokens: settings
                    .tokens
                    .iter()
                    .map(|value| parse_address(value))
                    .collect::<Result<_, _>>()?,
                complete_from_start: settings.complete_from_start,
            },
        )
        .map_err(|error| ProcessorFactoryError::configuration(error.to_string()))?;
        let (instance, publication, lifecycle) = configured_contract(configured)?;
        let processor = processor.with_contract(instance, publication, lifecycle);
        Ok(ProcessorComponents::new(Arc::new(processor))
            .with_query_extension(Arc::new(Erc20QueryExtension)))
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EvmEventsSettings {
    #[serde(default)]
    addresses: Vec<String>,
    events: Vec<ConfiguredEvent>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfiguredEvent {
    abi: String,
    output: ConfiguredEventOutput,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfiguredEventOutput {
    collection: String,
    kind: String,
    #[serde(default)]
    key_fields: Vec<String>,
    #[serde(default)]
    bucket_seconds: Option<u64>,
}

#[derive(Clone, Copy, Debug)]
struct EvmEventsProcessorFactory;

impl ProcessorFactory for EvmEventsProcessorFactory {
    fn id(&self) -> &str {
        "evm-events"
    }

    fn description(&self) -> &str {
        "ABI-decoded EVM logs with configurable entity keys and time buckets"
    }

    fn create(
        &self,
        configured: &ProcessorConfig,
        _context: ProcessorFactoryContext,
    ) -> Result<ProcessorComponents, ProcessorFactoryError> {
        use leani_processor_evm_events::{
            EventDefinition, EventOutput, EvmEventsConfig, EvmEventsProcessor,
        };

        let settings: EvmEventsSettings = decode_settings(configured)?;
        let processor = EvmEventsProcessor::new(EvmEventsConfig {
            start_block: BlockNumber(configured.start_block),
            addresses: settings
                .addresses
                .iter()
                .map(|value| parse_address(value))
                .collect::<Result<_, _>>()?,
            events: settings
                .events
                .into_iter()
                .map(|event| EventDefinition {
                    abi: event.abi,
                    output: EventOutput {
                        collection: event.output.collection,
                        kind: event.output.kind,
                        key_fields: event.output.key_fields,
                        bucket_seconds: event.output.bucket_seconds,
                    },
                })
                .collect(),
        })
        .map_err(|error| ProcessorFactoryError::configuration(error.to_string()))?;
        let (instance, publication, lifecycle) = configured_contract(configured)?;
        Ok(ProcessorComponents::new(Arc::new(processor.with_contract(
            instance,
            publication,
            lifecycle,
        ))))
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TransactionStatsSettings {
    from: String,
    to: String,
    #[serde(default)]
    complete_from_start: bool,
}

#[derive(Clone, Copy, Debug)]
struct TransactionStatsProcessorFactory;

impl ProcessorFactory for TransactionStatsProcessorFactory {
    fn id(&self) -> &str {
        "transaction-stats"
    }

    fn description(&self) -> &str {
        "Directional transaction counts and submitted value totals for one address pair"
    }

    fn create(
        &self,
        configured: &ProcessorConfig,
        _context: ProcessorFactoryContext,
    ) -> Result<ProcessorComponents, ProcessorFactoryError> {
        use leani_processor_transaction_stats::{
            TransactionStatsConfig, TransactionStatsProcessor,
        };

        let settings: TransactionStatsSettings = decode_settings(configured)?;
        let processor = TransactionStatsProcessor::new(TransactionStatsConfig {
            start_block: BlockNumber(configured.start_block),
            from: parse_address(&settings.from)?,
            to: parse_address(&settings.to)?,
            complete_from_start: settings.complete_from_start,
        })
        .map_err(|error| ProcessorFactoryError::configuration(error.to_string()))?;
        let (instance, publication, lifecycle) = configured_contract(configured)?;
        Ok(ProcessorComponents::new(Arc::new(processor.with_contract(
            instance,
            publication,
            lifecycle,
        ))))
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RollupTxsSettings {
    rules: Vec<ConfiguredRollupRule>,
}

/// One `[[processors.settings.rules]]` table: see `leani_processor_rollup_txs::Rule`.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfiguredRollupRule {
    rollup: String,
    purpose: String,
    to: String,
    #[serde(default)]
    from: Option<String>,
    #[serde(default)]
    selector: Option<String>,
    #[serde(default)]
    chain_id_arg: Option<u64>,
    #[serde(default)]
    since: Option<u64>,
    #[serde(default)]
    until: Option<u64>,
}

#[derive(Clone, Copy, Debug)]
struct RollupTxsProcessorFactory;

impl ProcessorFactory for RollupTxsProcessorFactory {
    fn id(&self) -> &str {
        leani_processor_rollup_txs::PROCESSOR_ID
    }

    fn description(&self) -> &str {
        "Rollups' non-blob L1 transactions (calldata batches, state updates, proofs) matched by configured rules, with execution burn and tips"
    }

    fn create(
        &self,
        configured: &ProcessorConfig,
        context: ProcessorFactoryContext,
    ) -> Result<ProcessorComponents, ProcessorFactoryError> {
        use leani_processor_rollup_txs::{Purpose, RollupTxsConfig, RollupTxsProcessor, Rule};

        if context.chain_id != 1 {
            return Err(ProcessorFactoryError::configuration(
                "rollup-txs supports Ethereum mainnet only",
            ));
        }
        let settings: RollupTxsSettings = decode_settings(configured)?;
        let rules = settings
            .rules
            .iter()
            .map(|rule| {
                Ok(Rule {
                    rollup: rule.rollup.clone(),
                    purpose: Purpose::parse(&rule.purpose).ok_or_else(|| {
                        ProcessorFactoryError::configuration(format!(
                            "rule purpose must be data, state or proof, got `{}`",
                            rule.purpose
                        ))
                    })?,
                    to: parse_address(&rule.to)?,
                    from: rule.from.as_deref().map(parse_address).transpose()?,
                    selector: rule.selector.as_deref().map(parse_selector).transpose()?,
                    chain_id_arg: rule.chain_id_arg,
                    since: rule.since,
                    until: rule.until,
                })
            })
            .collect::<Result<Vec<_>, ProcessorFactoryError>>()?;
        let processor = RollupTxsProcessor::new(RollupTxsConfig {
            network: "mainnet".to_owned(),
            start_block: BlockNumber(configured.start_block),
            rules,
        })
        .map_err(|error| ProcessorFactoryError::configuration(error.to_string()))?;
        let (instance, publication, lifecycle) = configured_contract(configured)?;
        Ok(ProcessorComponents::new(Arc::new(processor.with_contract(
            instance,
            publication,
            lifecycle,
        ))))
    }
}

fn parse_selector(value: &str) -> Result<[u8; 4], ProcessorFactoryError> {
    let encoded = value
        .strip_prefix("0x")
        .ok_or_else(|| ProcessorFactoryError::configuration("selector must start with 0x"))?;
    let mut selector = [0_u8; 4];
    hex::decode_to_slice(encoded, &mut selector).map_err(|_| {
        ProcessorFactoryError::configuration(format!(
            "invalid 0x-prefixed 4-byte selector `{value}`"
        ))
    })?;
    Ok(selector)
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ConfiguredPoolKind {
    V2,
    V3,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfiguredPool {
    address: String,
    kind: ConfiguredPoolKind,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct UniswapSettings {
    pools: Vec<ConfiguredPool>,
}

#[derive(Clone, Copy, Debug)]
struct UniswapObservationsProcessorFactory;

impl ProcessorFactory for UniswapObservationsProcessorFactory {
    fn id(&self) -> &str {
        "uniswap-observations"
    }

    fn description(&self) -> &str {
        "Append-only Uniswap V2/V3 pool observations"
    }

    fn create(
        &self,
        configured: &ProcessorConfig,
        _context: ProcessorFactoryContext,
    ) -> Result<ProcessorComponents, ProcessorFactoryError> {
        let processor =
            leani_processor_uniswap::UniswapObservationsProcessor::new(uniswap_config(configured)?)
                .map_err(|error| ProcessorFactoryError::configuration(error.to_string()))?;
        let (instance, publication, lifecycle) = configured_contract(configured)?;
        let processor = processor.with_contract(instance, publication, lifecycle);
        Ok(ProcessorComponents::new(Arc::new(processor))
            .with_query_extension(Arc::new(UniswapObservationsQueryExtension)))
    }
}

#[derive(Clone, Copy, Debug)]
struct UniswapLatestProcessorFactory;

impl ProcessorFactory for UniswapLatestProcessorFactory {
    fn id(&self) -> &str {
        "uniswap-latest"
    }

    fn description(&self) -> &str {
        "Latest materialized Uniswap V2/V3 pool state with typed query routes"
    }

    fn create(
        &self,
        configured: &ProcessorConfig,
        _context: ProcessorFactoryContext,
    ) -> Result<ProcessorComponents, ProcessorFactoryError> {
        let processor =
            leani_processor_uniswap::UniswapLatestProcessor::new(uniswap_config(configured)?)
                .map_err(|error| ProcessorFactoryError::configuration(error.to_string()))?;
        let (instance, publication, lifecycle) = configured_contract(configured)?;
        let processor = Arc::new(processor.with_contract(instance, publication, lifecycle));
        let erased: Arc<dyn Processor> = processor;
        Ok(ProcessorComponents::new(erased).with_query_extension(Arc::new(UniswapQueryExtension)))
    }
}

fn uniswap_config(
    configured: &ProcessorConfig,
) -> Result<leani_processor_uniswap::UniswapConfig, ProcessorFactoryError> {
    use leani_processor_uniswap::{PoolConfig, PoolKind, UniswapConfig};

    let settings: UniswapSettings = decode_settings(configured)?;
    Ok(UniswapConfig {
        start_block: BlockNumber(configured.start_block),
        pools: settings
            .pools
            .iter()
            .map(|pool| {
                Ok(PoolConfig {
                    address: parse_address(&pool.address)?,
                    kind: match pool.kind {
                        ConfiguredPoolKind::V2 => PoolKind::V2,
                        ConfiguredPoolKind::V3 => PoolKind::V3,
                    },
                })
            })
            .collect::<Result<_, ProcessorFactoryError>>()?,
    })
}

/// `[processors.settings]` of a `wasm` processor.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WasmSettings {
    /// Module path, relative to the configuration file.
    module: std::path::PathBuf,
    /// Optional pin: the module's BLAKE3 hash, as the processor's code hash.
    #[serde(default)]
    blake3: Option<String>,
    #[serde(default)]
    params: toml::Table,
    #[serde(default)]
    limits: leani_processor_wasm::WasmLimits,
}

/// Loads `settings.module`, a sandboxed Wasm processor, so a stock binary
/// runs processors it was not compiled with.
#[derive(Clone, Copy, Debug)]
struct WasmProcessorFactory;

impl ProcessorFactory for WasmProcessorFactory {
    fn id(&self) -> &str {
        leani_processor_wasm::PROCESSOR_ID
    }

    fn description(&self) -> &str {
        "A sandboxed Wasm module from settings.module, metered and deterministic, built with leani-wasm-guest"
    }

    fn create(
        &self,
        configured: &ProcessorConfig,
        _context: ProcessorFactoryContext,
    ) -> Result<ProcessorComponents, ProcessorFactoryError> {
        use leani_processor_wasm::{WasmContract, WasmModule, WasmProcessor};

        let settings: WasmSettings = decode_settings(configured)?;
        // A module is a pinned local artifact, so reading it here keeps
        // construction deterministic.
        let bytes = read_module(&settings.module, settings.limits.module_bytes)?;
        let module = WasmModule::new(&bytes, &settings.limits)
            .map_err(|error| ProcessorFactoryError::configuration(error.to_string()))?;
        let hash = hex::encode(module.hash().0);
        if let Some(pin) = &settings.blake3
            && !pin.trim_start_matches("0x").eq_ignore_ascii_case(&hash)
        {
            return Err(ProcessorFactoryError::configuration(format!(
                "module {} has BLAKE3 0x{hash}, not the pinned {pin}",
                settings.module.display()
            )));
        }
        let params = serde_json::to_value(&settings.params)
            .map_err(|error| ProcessorFactoryError::configuration(error.to_string()))?;
        let version = semver::Version::parse(&configured.version)
            .map_err(|error| ProcessorFactoryError::configuration(format!("version: {error}")))?;
        let (instance, publication, lifecycle) = configured_contract(configured)?;
        let processor = WasmProcessor::new(
            module,
            &params,
            WasmContract {
                instance,
                version,
                start_block: BlockNumber(configured.start_block),
                publication,
                lifecycle,
            },
            settings.limits,
        )
        .map_err(|error| ProcessorFactoryError::configuration(error.to_string()))?;
        Ok(ProcessorComponents::new(Arc::new(processor)))
    }
}

fn read_module(path: &std::path::Path, limit: u64) -> Result<Vec<u8>, ProcessorFactoryError> {
    use std::io::Read as _;

    let unreadable = |error: std::io::Error| {
        ProcessorFactoryError::configuration(format!(
            "cannot read Wasm module {}: {error}",
            path.display()
        ))
    };
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .map_err(unreadable)?
        .take(limit.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(unreadable)?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > limit {
        return Err(ProcessorFactoryError::configuration(format!(
            "Wasm module {} exceeds the {limit}-byte module limit",
            path.display()
        )));
    }
    Ok(bytes)
}

fn decode_settings<T: for<'de> Deserialize<'de>>(
    configured: &ProcessorConfig,
) -> Result<T, ProcessorFactoryError> {
    configured
        .decode_settings()
        .map_err(|error| ProcessorFactoryError::configuration(error.to_string()))
}

fn parse_address(value: &str) -> Result<Address, ProcessorFactoryError> {
    let encoded = value
        .strip_prefix("0x")
        .ok_or_else(|| ProcessorFactoryError::configuration("address must start with 0x"))?;
    let mut address = [0_u8; 20];
    hex::decode_to_slice(encoded, &mut address).map_err(|_| {
        ProcessorFactoryError::configuration(format!(
            "invalid 0x-prefixed 20-byte address `{value}`"
        ))
    })?;
    Ok(Address::new(address))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Copy, Debug)]
    struct DuplicateBlobs;

    impl ProcessorFactory for DuplicateBlobs {
        fn id(&self) -> &str {
            "blobs-money"
        }

        fn description(&self) -> &str {
            "duplicate test factory"
        }

        fn create(
            &self,
            _configured: &ProcessorConfig,
            _context: ProcessorFactoryContext,
        ) -> Result<ProcessorComponents, ProcessorFactoryError> {
            unreachable!("duplicate registration fails before construction")
        }
    }

    #[test]
    fn standard_registry_rejects_duplicate_factory() {
        let mut registry = ProcessorRegistry::standard();
        assert_eq!(
            registry.register(DuplicateBlobs),
            Err(ProcessorRegistryError::Duplicate("blobs-money".to_owned()))
        );
    }

    #[test]
    fn wasm_modules_load_beside_their_configuration_and_honor_a_pin() {
        let module = include_bytes!("../../processor-wasm/modules/erc20_transfers.wasm");
        let directory = tempfile::tempdir().expect("directory");
        std::fs::create_dir(directory.path().join("modules")).expect("modules");
        std::fs::write(directory.path().join("modules/transfers.wasm"), module).expect("module");
        let repository = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let base = std::fs::read_to_string(repository.join("config/modes/windowed.toml"))
            .expect("base configuration");
        let path = directory.path().join("leani.toml");
        std::fs::write(
            &path,
            format!(
                "{base}
[[processors]]
id = \"wasm\"
instance = \"usdc-transfers\"
version = \"1.0.0\"
history_mode = \"on_demand\"
start_block = 19000000
publish = \"attested\"
state = {{ mode = \"durable\" }}
output = {{ mode = \"full\" }}
delivery = {{ mode = \"window\" }}
checkpoint = {{ mode = \"automatic\", keep = 3 }}
undo = {{ mode = \"unfinalized\", safety_blocks = 256 }}

[processors.settings]
module = \"modules/transfers.wasm\"

[processors.settings.params]
tokens = [\"0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48\"]
minValue = 1000000
"
            ),
        )
        .expect("configuration");
        let config = Config::load(&path).expect("load");
        let mut configured = config
            .processors
            .iter()
            .find(|processor| processor.id == "wasm")
            .expect("wasm processor")
            .clone();
        assert_eq!(
            configured.settings["module"]
                .as_str()
                .map(std::path::PathBuf::from),
            Some(directory.path().join("modules/transfers.wasm"))
        );
        let registry = ProcessorRegistry::standard();
        let processor = registry
            .instantiate(&configured, config.chain.chain_id)
            .expect("wasm processor");
        let hash = blake3::hash(module);
        assert_eq!(processor.descriptor().code_hash.0, *hash.as_bytes());
        assert_eq!(processor.descriptor().instance.as_str(), "usdc-transfers");

        configured.settings.insert(
            "blake3".to_owned(),
            toml::Value::String(format!("0x{}", hash.to_hex())),
        );
        registry
            .instantiate(&configured, config.chain.chain_id)
            .expect("matching pin");
        configured.settings.insert(
            "blake3".to_owned(),
            toml::Value::String(format!("0x{}", "00".repeat(32))),
        );
        let refused = registry
            .instantiate(&configured, config.chain.chain_id)
            .err()
            .expect("stale pin")
            .to_string();
        assert!(refused.contains("not the pinned"), "{refused}");
        configured.settings.insert(
            "module".to_owned(),
            toml::Value::String("missing.wasm".to_owned()),
        );
        let missing = registry
            .instantiate(&configured, config.chain.chain_id)
            .err()
            .expect("missing module")
            .to_string();
        assert!(
            missing.contains("cannot read Wasm module missing.wasm"),
            "{missing}"
        );
    }

    #[test]
    fn standard_registry_contains_block_summary() {
        let registry = ProcessorRegistry::standard();
        assert!(registry.contains("block-summary"));
        let configured = crate::block_summaries::processor_config(
            "demo-blocks",
            crate::config::PublishMode::Attested,
        );
        let components = registry
            .instantiate_components(&configured, 1)
            .expect("block processor");
        let processor = &components.processor;
        assert_eq!(processor.descriptor().id.as_str(), "block-summary");
        assert_eq!(
            processor.descriptor().requirements[0].capabilities,
            leani_primitives::CapabilitySet::of(leani_primitives::Capability::Header)
                .with(leani_primitives::Capability::Body)
        );
        assert_eq!(
            components
                .query_extension
                .as_ref()
                .map(|extension| extension.id()),
            Some("block-summary-v1")
        );
    }

    /// A `rollup-txs` instance, started after London, with one valid rule
    /// whose fields `fields` add to or override.
    fn rollup_txs_with_rule<const N: usize>(fields: [(&str, toml::Value); N]) -> ProcessorConfig {
        let mut processor = crate::builtin_processors::processor_contract(
            "rollup-txs",
            "rollup-txs-test",
            "1.0.0",
            22_000_000,
            crate::config::PublishMode::Attested,
        );
        let mut rule = toml::Table::from_iter([
            ("rollup".to_owned(), "base".into()),
            ("purpose".to_owned(), "state".into()),
            (
                "to".to_owned(),
                "0x43edb88c4b80fdd2adff2412a7bebf9df42cb40e".into(),
            ),
            ("selector".to_owned(), "0x82ecf2f6".into()),
        ]);
        rule.extend(fields.map(|(key, value)| (key.to_owned(), value)));
        processor.settings.insert(
            "rules".to_owned(),
            toml::Value::Array(vec![toml::Value::Table(rule)]),
        );
        processor
    }

    #[test]
    fn rollup_txs_settings_become_matching_rules() {
        let configured = |purpose: &str| rollup_txs_with_rule([("purpose", purpose.into())]);
        let registry = ProcessorRegistry::standard();
        let components = registry
            .instantiate_components(&configured("state"), 1)
            .expect("rollup-txs");
        let descriptor = components.processor.descriptor();
        assert_eq!(descriptor.id.as_str(), "rollup-txs");
        assert_eq!(descriptor.requirements[0].filter.recipients.len(), 1);
        assert!(matches!(
            registry.instantiate_components(&configured("settlement"), 1),
            Err(ProcessorRegistryError::Factory { .. })
        ));
    }

    #[test]
    fn rollup_txs_rejects_a_selector_that_is_not_four_bytes() {
        let registry = ProcessorRegistry::standard();
        for selector in ["0x82ecf2", "0x82ecf2f6aa"] {
            let error = registry
                .instantiate_components(&rollup_txs_with_rule([("selector", selector.into())]), 1)
                .expect_err("a selector of the wrong length");
            assert!(error.to_string().contains("4-byte selector"), "{error}");
        }
    }

    #[test]
    fn rollup_txs_rejects_an_unknown_settings_key() {
        let registry = ProcessorRegistry::standard();
        // a misspelt rule key must not silently drop the constraint it meant
        let misspelt = rollup_txs_with_rule([("selecter", "0x82ecf2f6".into())]);
        let mut stray = rollup_txs_with_rule([]);
        stray
            .settings
            .insert("network".to_owned(), "sepolia".into());
        for (configured, key) in [(misspelt, "selecter"), (stray, "network")] {
            let error = registry
                .instantiate_components(&configured, 1)
                .expect_err("an unknown key");
            assert!(
                error
                    .to_string()
                    .contains(&format!("unknown field `{key}`")),
                "{error}"
            );
        }
    }

    #[test]
    fn rollup_txs_rejects_a_chain_other_than_mainnet() {
        let registry = ProcessorRegistry::standard();
        // Sepolia and Holesky, which the first implementation named
        for chain_id in [11_155_111, 17_000] {
            let error = registry
                .instantiate_components(&rollup_txs_with_rule([]), chain_id)
                .expect_err("a test network");
            assert!(
                error
                    .to_string()
                    .contains("rollup-txs supports Ethereum mainnet only"),
                "{error}"
            );
        }
    }

    #[test]
    fn rollup_txs_rejects_a_window_that_is_not_in_unix_seconds() {
        let registry = ProcessorRegistry::standard();
        // a block number, and a timestamp in milliseconds: either window would
        // silently match the wrong blocks
        for field in ["since", "until"] {
            for value in [26_000_000_i64, 1_790_000_000_000] {
                let configured = rollup_txs_with_rule([(field, value.into())]);
                let Err(error) = registry.instantiate_components(&configured, 1) else {
                    panic!("{field} {value} was accepted");
                };
                assert!(
                    error.to_string().contains("UNIX timestamps in seconds"),
                    "{field} {value}: {error}"
                );
            }
        }
    }

    #[test]
    fn rollup_txs_contract_example_loads() {
        #[derive(Deserialize)]
        struct ContractExample {
            processors: Vec<ProcessorConfig>,
        }

        let repository = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let path = repository.join("docs/reference/processor-contracts.md");
        let contracts = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("{}: {error}", path.display()));
        let section = contracts
            .split("\n## ")
            .find(|section| section.split_whitespace().next() == Some("rollup-txs"))
            .expect("a `## rollup-txs` section");
        let example = section
            .split_once("```toml\n")
            .and_then(|(_, rest)| rest.split_once("\n```"))
            .map(|(example, _)| example)
            .expect("a TOML example in the rollup-txs section");
        let example: ContractExample =
            toml::from_str(example).unwrap_or_else(|error| panic!("rollup-txs example: {error}"));
        let [configured] = example.processors.as_slice() else {
            panic!("the rollup-txs example configures one processor");
        };
        ProcessorRegistry::standard()
            .instantiate(configured, 1)
            .unwrap_or_else(|error| panic!("rollup-txs example: {error}"));
    }

    #[test]
    fn standard_registry_instantiates_every_operating_mode_profile() {
        let repository = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let registry = ProcessorRegistry::standard();
        for file in [
            "externalized.toml",
            "aggregate-only.toml",
            "job-result.toml",
            "head-only.toml",
            "windowed.toml",
            "full.toml",
        ] {
            let path = repository.join("config/modes").join(file);
            let config =
                Config::load(&path).unwrap_or_else(|error| panic!("{}: {error}", path.display()));
            let errors = registry.validation_errors(&config);
            assert!(errors.is_empty(), "{}: {errors:?}", path.display());
        }
    }

    #[test]
    fn live_gap_fill_requires_a_block_local_processor() {
        let modes = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/modes");
        let registry = ProcessorRegistry::standard();
        let fill = Some(crate::config::LiveGapFillConfig { max_blocks: 64 });

        // transaction-stats is ordered.
        let mut ordered = Config::load(&modes.join("aggregate-only.toml")).expect("profile");
        ordered.processors[0].history_mode = crate::config::ProcessorHistoryMode::OnDemand;
        ordered.processors[0].live_gap_fill = fill;
        let errors = registry.validation_errors(&ordered);
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert_eq!(errors[0].field, "processors[0]");
        assert!(
            errors[0].message.ends_with(
                "live_gap_fill requires a block-local processor; an ordered lane holds later blocks instead of skipping them"
            ),
            "{}",
            errors[0].message
        );

        // blobs-money is block-local.
        let mut block_local = Config::load(&modes.join("externalized.toml")).expect("profile");
        block_local.processors[0].live_gap_fill = fill;
        let errors = registry.validation_errors(&block_local);
        assert!(errors.is_empty(), "{errors:?}");
    }

    #[test]
    fn latest_output_on_a_block_local_processor_is_warned_about() {
        let modes = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/modes");
        let registry = ProcessorRegistry::standard();

        // transaction-stats is ordered: `latest` keeps its one running total.
        let aggregate = Config::load(&modes.join("aggregate-only.toml")).expect("profile");
        assert_eq!(
            registry.validation_warnings(&aggregate),
            Vec::<String>::new()
        );

        // The windowed evm-events instance writes one entity per event.
        let mut events = Config::load(&modes.join("windowed.toml")).expect("profile");
        assert_eq!(registry.validation_warnings(&events), Vec::<String>::new());
        events.processors[0].output.mode = OutputPolicyMode::Latest;
        events.processors[0].output.window = None;
        assert!(registry.validation_errors(&events).is_empty());
        let warnings = registry.validation_warnings(&events);
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(
            warnings[0].starts_with("processors[0] (uniswap-v2-sync-30d): output \"latest\"")
                && warnings[0].contains("block-local"),
            "{}",
            warnings[0]
        );
    }
}
