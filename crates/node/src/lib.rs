//! Process configuration, command line interface, diagnostics, and lifecycle.

mod benchmark;
mod block_summaries;
pub mod cli;
pub mod config;
mod init;
mod local_state;
pub mod process;
pub mod processors;
mod subscribe;
mod uniswap_markets;

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
