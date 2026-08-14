//! Public processor factory and registry boundary.
//!
//! The standard binary registers the checked-in native processors. Downstream
//! binaries can register additional factories without forking node assembly.
//! A future Wasm package loader implements the same [`ProcessorFactory`]
//! contract.

use std::{collections::BTreeMap, fmt, sync::Arc};

use leani_api::{
    BlobsQueryExtension, Erc20QueryExtension, QueryExtension, QueryExtensionRegistration,
    UniswapQueryExtension,
};
use leani_primitives::{Address, BlockNumber};
use leani_processor_api::{
    LifecyclePolicies, Processor, ProcessorInstanceId, PublicationPolicy, StartPoint,
};
use serde::Deserialize;
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

/// Constructs one configured processor without opening files or networks.
///
/// Factories are trusted native code in the initial extension model. A Wasm
/// package runtime can later expose one factory per installed package while
/// retaining the same node/runtime boundary.
pub trait ProcessorFactory: Send + Sync {
    fn id(&self) -> &str;

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
            Arc::new(BlobsProcessorFactory) as Arc<dyn ProcessorFactory>,
            Arc::new(Erc20ProcessorFactory),
            Arc::new(EvmEventsProcessorFactory),
            Arc::new(TransactionStatsProcessorFactory),
            Arc::new(UniswapObservationsProcessorFactory),
            Arc::new(UniswapLatestProcessorFactory),
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
            .map_err(|source| ProcessorRegistryError::Factory {
                id: configured.id.clone(),
                source,
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
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ProcessorRegistryError {
    #[error("processor factory ID must not be empty")]
    EmptyId,
    #[error("processor factory `{0}` is registered more than once")]
    Duplicate(String),
    #[error("processor `{0}` is not registered in this node binary")]
    Unknown(String),
    #[error("processor `{id}` configuration is invalid: {source}")]
    Factory {
        id: String,
        #[source]
        source: ProcessorFactoryError,
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
struct BlobsProcessorFactory;

impl ProcessorFactory for BlobsProcessorFactory {
    fn id(&self) -> &'static str {
        "blobs-money"
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
    fn id(&self) -> &'static str {
        "erc20-balances"
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
    fn id(&self) -> &'static str {
        "evm-events"
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
    fn id(&self) -> &'static str {
        "transaction-stats"
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
    fn id(&self) -> &'static str {
        "uniswap-observations"
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
        Ok(ProcessorComponents::new(Arc::new(processor)))
    }
}

#[derive(Clone, Copy, Debug)]
struct UniswapLatestProcessorFactory;

impl ProcessorFactory for UniswapLatestProcessorFactory {
    fn id(&self) -> &'static str {
        "uniswap-latest"
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
        fn id(&self) -> &'static str {
            "blobs-money"
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
    fn standard_registry_instantiates_every_operating_mode_profile() {
        let repository = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let registry = ProcessorRegistry::standard();
        for file in [
            "externalized.toml",
            "aggregate-only.toml",
            "terminal.toml",
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
}
