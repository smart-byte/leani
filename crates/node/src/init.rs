//! Compact configuration generation for built-in processor presets.

use std::{
    fs,
    io::Write as _,
    path::{Path, PathBuf},
};

use anyhow::{Context as _, Result, bail};
use url::Url;

use crate::{
    cli::SubscribeProtocol, config::StarterConfig, process::Exit, subscribe::initialize_checkpoint,
    uniswap_markets::resolve_markets,
};

pub(crate) struct InitOptions {
    pub protocol: SubscribeProtocol,
    pub targets: Vec<String>,
    /// `--data-dir`; `None` for `./data` beside the configuration.
    pub data_dir: Option<PathBuf>,
    pub checkpoint_urls: Vec<Url>,
    pub checkpoint_quorum: usize,
    pub accept_checkpoint: bool,
    pub config_path: PathBuf,
    pub working_directory: PathBuf,
}

pub(crate) async fn init(options: InitOptions) -> Result<Exit> {
    let config_path = absolute_path(&options.working_directory, &options.config_path);
    if config_path.exists() {
        bail!(
            "refusing to overwrite existing configuration {}; choose another path with --config",
            config_path.display()
        );
    }
    let market_names = match options.protocol {
        SubscribeProtocol::Blocks => {
            if !options.targets.is_empty() {
                bail!("the blocks preset does not take market arguments");
            }
            Vec::new()
        }
        SubscribeProtocol::UniswapV3 => {
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
    let checkpoint = initialize_checkpoint(
        options.checkpoint_urls,
        options.checkpoint_quorum,
        options.accept_checkpoint,
        &runtime_data_dir,
    )
    .await?;
    let endpoints = starter_finality_endpoints(checkpoint.beacon_api_endpoints)?;
    let config = match options.protocol {
        SubscribeProtocol::Blocks => {
            StarterConfig::blocks(data_dir, checkpoint.root, checkpoint.slot, endpoints)
        }
        SubscribeProtocol::UniswapV3 => StarterConfig::uniswap(
            data_dir,
            market_names.clone(),
            checkpoint.root,
            checkpoint.slot,
            endpoints,
        ),
    };
    let encoded = toml::to_string_pretty(&config).context("render compact configuration")?;
    write_new_config(&config_path, &encoded)?;

    println!("Created {}", config_path.display());
    match options.protocol {
        SubscribeProtocol::Blocks => println!("Processor: block-summary"),
        SubscribeProtocol::UniswapV3 => {
            println!("Processor: uniswap-observations");
            println!("Markets: {}", market_names.join(", "));
        }
    }
    println!();
    println!("Next:");
    println!("  leani serve");
    match options.protocol {
        SubscribeProtocol::Blocks => println!("  leani subscribe blocks"),
        SubscribeProtocol::UniswapV3 => {
            println!("  leani subscribe uniswap-v3 {}", market_names.join(" "));
        }
    }
    Ok(Exit::Success)
}

/// Beacon transports for a new compact configuration: the managed default
/// pool plus every responding checkpoint provider that also serves the
/// Beacon light-client API, without normalized duplicates.
pub(crate) fn starter_finality_endpoints(responding: Vec<Url>) -> Result<Vec<Url>> {
    let mut endpoints = crate::config::default_finality_endpoints().map_err(anyhow::Error::msg)?;
    for endpoint in responding {
        let identity = crate::config::finality_endpoint_identity(&endpoint);
        if !endpoints
            .iter()
            .any(|existing| crate::config::finality_endpoint_identity(existing) == identity)
        {
            endpoints.push(endpoint);
        }
    }
    Ok(endpoints)
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
        let endpoints = starter_finality_endpoints(vec![
            Url::parse("https://ethereum-beacon-api.publicnode.com").expect("URL"),
            Url::parse("https://beacon.example/").expect("URL"),
        ])
        .expect("starter endpoints");
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
