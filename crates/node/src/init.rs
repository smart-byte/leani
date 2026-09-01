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
    pub data_dir: PathBuf,
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
    let runtime_data_dir = absolute_path(&options.working_directory, &options.data_dir);
    let checkpoint = initialize_checkpoint(
        options.checkpoint_urls,
        options.checkpoint_quorum,
        options.accept_checkpoint,
        &runtime_data_dir,
    )
    .await?;
    let config = match options.protocol {
        SubscribeProtocol::Blocks => StarterConfig::blocks(
            options.data_dir,
            checkpoint.root,
            checkpoint.slot,
            checkpoint.beacon_api_endpoints,
        ),
        SubscribeProtocol::UniswapV3 => StarterConfig::uniswap(
            options.data_dir,
            market_names.clone(),
            checkpoint.root,
            checkpoint.slot,
            checkpoint.beacon_api_endpoints,
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
    fn configuration_writer_never_overwrites() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("leani.toml");
        write_new_config(&path, "first\n").expect("initial write");
        let error = write_new_config(&path, "second\n").expect_err("overwrite rejected");
        assert!(error.to_string().contains("refusing to overwrite"));
        assert_eq!(fs::read_to_string(path).expect("read config"), "first\n");
    }
}
