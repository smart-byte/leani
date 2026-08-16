//! Process configuration, command line interface, diagnostics, and lifecycle.

mod benchmark;
pub mod cli;
pub mod config;
pub mod process;
pub mod processors;

pub use cli::{Cli, Command};
pub use config::{Config, ConfigError, ProcessorConfig, ValidatedConfig};
pub use leani_api::{
    ApiError, QueryContext, QueryExtension, QueryExtensionRegistration, QueryExtensionSummary,
};
pub use process::{Exit, run, run_cli_with_registry, run_with_registry};
pub use processors::{
    ProcessorAssembly, ProcessorComponents, ProcessorFactory, ProcessorFactoryContext,
    ProcessorFactoryError, ProcessorFactoryMetadata, ProcessorRegistry, ProcessorRegistryError,
};
