//! `leani init`: a compact configuration for a built-in processor preset,
//! or a full one built from requirements.

use std::{
    fs,
    io::Write as _,
    path::{Path, PathBuf},
};

use anyhow::{Context as _, Result, bail};
use leani_chain::{Chain, ChainRegistry, Family};
use url::Url;

use crate::{
    cli::{Follow, Keep, Recipe, SubscribeProtocol},
    config::{HumanBytes, StarterConfig, StarterFinalityConfig},
    config_builder::{BuildContext, History, Limits, ProcessorChoice, Requirements},
    process::Exit,
    processors::ProcessorRegistry,
    subscribe::initialize_checkpoint,
    uniswap_markets::resolve_markets,
};

/// Any of these flags, `--requirements`, or `--json` builds a full
/// configuration instead of the compact one.
pub(crate) struct RequirementFlags {
    pub keep: Option<Keep>,
    pub window_days: Option<u64>,
    pub follow: Option<Follow>,
    pub history_from_block: Option<u64>,
    pub memory: Option<HumanBytes>,
    pub disk: Option<HumanBytes>,
    pub bandwidth: Option<u64>,
    pub tuning: Vec<Recipe>,
    pub requirements: Option<PathBuf>,
    pub json: bool,
}

impl RequirementFlags {
    fn given(&self) -> bool {
        self.keep.is_some()
            || self.window_days.is_some()
            || self.follow.is_some()
            || self.history_from_block.is_some()
            || self.memory.is_some()
            || self.disk.is_some()
            || self.bandwidth.is_some()
            || !self.tuning.is_empty()
            || self.requirements.is_some()
            || self.json
    }
}

pub(crate) struct InitOptions {
    /// The chain entry, or an alias, to follow.
    pub network: String,
    pub protocol: Option<SubscribeProtocol>,
    pub targets: Vec<String>,
    /// `--data-dir`; `None` for `./data` beside the configuration.
    pub data_dir: Option<PathBuf>,
    /// Checkpoint providers; empty for the ones the chain entry names.
    pub checkpoint_urls: Vec<Url>,
    pub checkpoint_quorum: usize,
    pub accept_checkpoint: bool,
    pub config_path: PathBuf,
    pub working_directory: PathBuf,
    pub flags: RequirementFlags,
}

pub(crate) async fn init(options: InitOptions, registry: &ProcessorRegistry) -> Result<Exit> {
    let config_path = absolute_path(&options.working_directory, &options.config_path);
    if config_path.exists() {
        bail!(
            "refusing to overwrite existing configuration {}; choose another path with --config",
            config_path.display()
        );
    }
    if options.protocol == Some(SubscribeProtocol::Blocks) && !options.targets.is_empty() {
        bail!("the blocks preset does not take market arguments");
    }
    let chain = ChainRegistry::standard().get(&options.network)?;
    if options.flags.given() {
        return build(options, &chain, &config_path, registry).await;
    }
    let Some(protocol) = options.protocol else {
        bail!("name a processor preset, or pass --requirements");
    };
    let market_names = match protocol {
        SubscribeProtocol::Blocks => Vec::new(),
        SubscribeProtocol::UniswapV3 => {
            if chain.chain_id != 1 {
                bail!(
                    "Uniswap markets are Ethereum Mainnet's; use the blocks preset on {}",
                    chain.name
                );
            }
            if options.targets.is_empty() {
                bail!("the uniswap-v3 preset requires at least one market");
            }
            resolve_markets(&options.targets)?
                .iter()
                .map(|market| market.symbol.to_owned())
                .collect::<Vec<_>>()
        }
    };
    let (data_dir, runtime_data_dir) = init_data_dir(
        &config_path,
        options.data_dir.as_deref(),
        &options.working_directory,
    );
    let finality = match checkpoint_chain(&chain)? {
        Some(beacon) => Some(checkpoint(&options, &beacon, &runtime_data_dir).await?),
        None => None,
    };
    let config = match protocol {
        SubscribeProtocol::Blocks => StarterConfig::blocks(chain.name.clone(), data_dir, finality),
        SubscribeProtocol::UniswapV3 => StarterConfig::uniswap(
            data_dir,
            market_names.clone(),
            finality.context("Uniswap follows Ethereum Mainnet from a checkpoint")?,
        ),
    };
    let encoded = toml::to_string_pretty(&config).context("render compact configuration")?;
    write_new_config(&config_path, &encoded)?;

    println!("Created {}", config_path.display());
    match protocol {
        SubscribeProtocol::Blocks => println!("Processor: block-summary"),
        SubscribeProtocol::UniswapV3 => {
            println!("Processor: uniswap-observations");
            println!("Markets: {}", market_names.join(", "));
        }
    }
    println!();
    println!("Next:");
    println!("  leani serve");
    match protocol {
        SubscribeProtocol::Blocks => println!("  leani subscribe blocks"),
        SubscribeProtocol::UniswapV3 => {
            println!("  leani subscribe uniswap-v3 {}", market_names.join(" "));
        }
    }
    Ok(Exit::Success)
}

/// The chain whose beacon chain a checkpoint is for: an Ethereum chain's
/// own, or Ethereum Mainnet for a rollup's L1 anchor; `None` when nothing
/// would read one.
fn checkpoint_chain(chain: &std::sync::Arc<Chain>) -> Result<Option<std::sync::Arc<Chain>>> {
    Ok(match chain.family {
        Family::Ethereum => Some(chain.clone()),
        _ if chain.l1_anchor.is_some() => Some(ChainRegistry::standard().get("ethereum-mainnet")?),
        _ => None,
    })
}

/// A checkpoint of `beacon`'s beacon chain that a quorum of providers agrees
/// on: `--checkpoint-url`'s, or else the entry's first Beacon transport and
/// its checkpoint providers.
async fn checkpoint(
    options: &InitOptions,
    beacon: &Chain,
    data_dir: &Path,
) -> Result<StarterFinalityConfig> {
    let providers = if options.checkpoint_urls.is_empty() {
        beacon
            .defaults
            .beacon_endpoints
            .iter()
            .take(1)
            .chain(&beacon.defaults.checkpoint_providers)
            .cloned()
            .collect()
    } else {
        options.checkpoint_urls.clone()
    };
    let checkpoint = initialize_checkpoint(
        providers,
        options.checkpoint_quorum,
        options.accept_checkpoint,
        data_dir,
    )
    .await?;
    Ok(StarterFinalityConfig {
        checkpoint: checkpoint.root,
        checkpoint_slot: checkpoint.slot,
        endpoints: starter_finality_endpoints(
            &beacon.defaults.beacon_endpoints,
            checkpoint.beacon_api_endpoints,
        ),
    })
}

async fn build(
    options: InitOptions,
    chain: &std::sync::Arc<Chain>,
    config_path: &Path,
    registry: &ProcessorRegistry,
) -> Result<Exit> {
    let requirements = match &options.flags.requirements {
        Some(path) => read_requirements(path, &options.working_directory)?,
        None => flag_requirements(options.protocol, &options.targets, &options.flags)?,
    };
    // Before the checkpoint fetch, which may ask to confirm its providers.
    crate::config_builder::check(&requirements, None, chain).map_err(anyhow::Error::msg)?;
    let (data_dir, runtime_data_dir) = init_data_dir(
        config_path,
        options.data_dir.as_deref(),
        &options.working_directory,
    );
    let beacon = checkpoint_chain(chain)?.filter(|_| requirements.follow != Follow::None);
    let finality = match beacon {
        Some(beacon) => {
            let checkpoint = checkpoint(&options, &beacon, &runtime_data_dir).await?;
            Some(
                crate::config_builder::finality(
                    checkpoint.checkpoint,
                    checkpoint.checkpoint_slot,
                    checkpoint.endpoints,
                )
                .map_err(anyhow::Error::msg)?,
            )
        }
        None => None,
    };
    let built = crate::config_builder::build(
        &requirements,
        &BuildContext {
            registry,
            chain: chain.clone(),
            finality,
            head: None,
            data_dir: Some(data_dir),
        },
    )
    .map_err(anyhow::Error::msg)?;
    let valid = built.errors.is_empty();
    if valid {
        write_new_config(config_path, &built.toml)?;
    }
    if options.flags.json {
        let mut report = serde_json::to_value(&built)?;
        report["path"] = serde_json::Value::String(config_path.display().to_string());
        report["written"] = serde_json::Value::Bool(valid);
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else if valid {
        println!("Created {}", config_path.display());
        println!();
        println!("Derived from your requirements:");
        for decision in &built.decisions {
            println!(
                "  {} = {}  ({})",
                decision.setting, decision.value, decision.because
            );
        }
        for warning in &built.warnings {
            println!("warning: {warning}");
        }
        println!();
        println!("Next:");
        for step in &built.next {
            println!("  {step}");
        }
    } else {
        for error in &built.errors {
            eprintln!("error: {}: {}", error.field, error.message);
        }
    }
    Ok(if valid {
        Exit::Success
    } else {
        Exit::InvalidConfiguration
    })
}

/// Requirements as JSON from a file, or from standard input for `-`.
fn read_requirements(path: &Path, working_directory: &Path) -> Result<Requirements> {
    let text = if path == Path::new("-") {
        std::io::read_to_string(std::io::stdin()).context("read requirements from stdin")?
    } else {
        let path = absolute_path(working_directory, path);
        fs::read_to_string(&path)
            .with_context(|| format!("read requirements {}", path.display()))?
    };
    serde_json::from_str(&text).context("requirements are not valid; see `leani guide config`")
}

fn flag_requirements(
    protocol: Option<SubscribeProtocol>,
    targets: &[String],
    flags: &RequirementFlags,
) -> Result<Requirements> {
    let processor = match protocol {
        Some(SubscribeProtocol::Blocks) => ProcessorChoice::Blocks,
        Some(SubscribeProtocol::UniswapV3) => ProcessorChoice::Uniswap {
            markets: targets.to_vec(),
        },
        None => bail!("name a processor preset, or pass --requirements"),
    };
    Ok(Requirements {
        processor,
        keep: flags.keep.unwrap_or_default(),
        window_days: flags.window_days,
        follow: flags.follow.unwrap_or_default(),
        history: flags.history_from_block.map(|block| History {
            from_block: Some(block),
            last_days: None,
        }),
        limits: Limits {
            memory: flags.memory,
            disk: flags.disk,
            bandwidth_mbit: flags.bandwidth,
        },
        tuning: (!flags.tuning.is_empty()).then(|| flags.tuning.clone()),
    })
}

/// Beacon transports for a new configuration: the chain's managed pool plus
/// every responding checkpoint provider that also serves the Beacon
/// light-client API, without normalized duplicates.
pub(crate) fn starter_finality_endpoints(pool: &[Url], responding: Vec<Url>) -> Vec<Url> {
    let mut endpoints = pool.to_vec();
    for endpoint in responding {
        let identity = crate::config::finality_endpoint_identity(&endpoint);
        if !endpoints
            .iter()
            .any(|existing| crate::config::finality_endpoint_identity(existing) == identity)
        {
            endpoints.push(endpoint);
        }
    }
    endpoints
}

/// The `data_dir` to write into the new configuration, and the directory it
/// names. A relative `data_dir` in the file is beside it, while a relative
/// `--data-dir`, like any path flag, is under the working directory: the
/// flag is written as given only where the two coincide.
fn init_data_dir(
    config_path: &Path,
    requested: Option<&Path>,
    working_directory: &Path,
) -> (PathBuf, PathBuf) {
    let directory = config_path.parent().unwrap_or(working_directory);
    let Some(requested) = requested else {
        return (PathBuf::from("./data"), directory.join("data"));
    };
    let prepared = absolute_path(working_directory, requested);
    if requested.is_relative() && directory != working_directory {
        (prepared.clone(), prepared)
    } else {
        (requested.to_path_buf(), prepared)
    }
}

fn absolute_path(working_directory: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        working_directory.join(path)
    }
}

fn write_new_config(path: &Path, contents: &str) -> Result<()> {
    let parent = path
        .parent()
        .context("configuration path has no parent directory")?;
    fs::create_dir_all(parent)
        .with_context(|| format!("create configuration directory {}", parent.display()))?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent).with_context(|| {
        format!(
            "create configuration temporary file in {}",
            parent.display()
        )
    })?;
    temporary.write_all(contents.as_bytes())?;
    temporary.as_file_mut().sync_all()?;
    temporary
        .persist_noclobber(path)
        .map_err(|error| error.error)
        .with_context(|| {
            format!(
                "refusing to overwrite existing configuration {}",
                path.display()
            )
        })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starter_endpoints_keep_the_default_pool_and_add_responding_providers() {
        let pool = crate::config::default_finality_endpoints().expect("default pool");
        let endpoints = starter_finality_endpoints(
            &pool,
            vec![
                Url::parse("https://ethereum-beacon-api.publicnode.com").expect("URL"),
                Url::parse("https://beacon.example/").expect("URL"),
            ],
        );
        assert_eq!(
            endpoints.iter().map(Url::as_str).collect::<Vec<_>>(),
            [
                "https://ethereum-beacon-api.publicnode.com/",
                "https://lodestar-mainnet.chainsafe.io/",
                "https://beacon.example/",
            ]
        );
    }

    #[test]
    fn init_prepares_the_data_dir_its_configuration_names() {
        // Audit Config-8: a configuration's relative `data_dir` is beside the
        // file, so `init` must write, and use, a `data_dir` that names the
        // directory it prepares.
        let working = Path::new("/work");
        let data_dirs = |config: &str, requested: Option<&str>| {
            let (written, prepared) =
                init_data_dir(Path::new(config), requested.map(Path::new), working);
            (
                written.display().to_string(),
                prepared.display().to_string(),
            )
        };
        // By default, `./data` beside the configuration, wherever it is.
        assert_eq!(
            data_dirs("/etc/leani/node.toml", None),
            ("./data".to_owned(), "/etc/leani/data".to_owned())
        );
        assert_eq!(
            data_dirs("/work/leani.toml", None),
            ("./data".to_owned(), "/work/data".to_owned())
        );
        // A relative `--data-dir`, like any path flag, is under the working
        // directory; beside a configuration elsewhere it is written absolute.
        assert_eq!(
            data_dirs("/work/leani.toml", Some("state")),
            ("state".to_owned(), "/work/state".to_owned())
        );
        assert_eq!(
            data_dirs("/etc/leani/node.toml", Some("state")),
            ("/work/state".to_owned(), "/work/state".to_owned())
        );
        assert_eq!(
            data_dirs("/etc/leani/node.toml", Some("/srv/leani")),
            ("/srv/leani".to_owned(), "/srv/leani".to_owned())
        );
    }

    #[test]
    fn configuration_writer_never_overwrites() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("leani.toml");
        write_new_config(&path, "first\n").expect("initial write");
        let error = write_new_config(&path, "second\n").expect_err("overwrite rejected");
        assert!(error.to_string().contains("refusing to overwrite"));
        assert_eq!(fs::read_to_string(path).expect("read config"), "first\n");
    }
}
