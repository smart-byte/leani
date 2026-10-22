//! `leani processor run`: one Wasm processor over finalized history, with
//! its changes printed as JSON lines.
//!
//! The run is an ordinary on-demand backfill of a synthesized `wasm`
//! instance into a throwaway data directory, so it uses the configuration's
//! verified sources, finality, and pipeline unchanged.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use leani_primitives::{BlockNumber, BlockRange, ChainId};
use leani_processor_api::{ChangeOperation, CheckpointPolicyMode, Processor, StatePolicyMode};
use leani_store_sqlite::{ChangeRecord, SqliteStore};

use super::{Exit, backfill::run_standalone, configured_store_config};
use crate::{
    builtin_processors::processor_contract,
    config::{CheckpointPolicyConfig, Config, ProcessorConfig, PublishMode, StatePolicyConfig},
    processors::ProcessorRegistry,
    subscribe::{StdoutClosed, print_row},
};

/// The synthesized instance. Its identity includes the module, parameters,
/// and first block, so a kept `--data-dir` serves only reruns with the same.
const INSTANCE: &str = "run";
const PAGE: usize = 1_000;

#[derive(Debug)]
pub(super) struct RunOptions {
    pub(super) module: Option<PathBuf>,
    pub(super) template: Option<String>,
    pub(super) from_block: u64,
    pub(super) to_block: u64,
    pub(super) params: Vec<String>,
    pub(super) params_json: Option<String>,
    pub(super) data_dir: Option<PathBuf>,
}

pub(super) async fn run(
    config_path: &Path,
    options: RunOptions,
    registry: &ProcessorRegistry,
) -> Result<Exit> {
    BlockRange::new(
        BlockNumber(options.from_block),
        BlockNumber(options.to_block),
    )?;
    let temporary = tempfile::tempdir().context("create a run directory")?;
    let (module, example) = match (&options.module, &options.template) {
        (Some(module), None) => (
            std::fs::canonicalize(module)
                .with_context(|| format!("read Wasm module {}", module.display()))?,
            None,
        ),
        (None, Some(name)) => {
            let template = bundled(name)?;
            let path = temporary.path().join(format!("{name}.wasm"));
            std::fs::write(&path, template.module).context("write the template module")?;
            (path, Some(template.params))
        }
        _ => bail!("give a module file or --template, not both"),
    };
    let params = match example {
        Some(example) if options.params_json.is_none() && options.params.is_empty() => {
            parameters(Some(example), &[])?
        }
        _ => parameters(options.params_json.as_deref(), &options.params)?,
    };
    let mut config = Config::load(config_path).with_context(|| {
        if config_path.exists() {
            format!("load configuration {}", config_path.display())
        } else {
            format!(
                "`leani processor run` takes its sources from {}; create one with `leani init blocks`",
                config_path.display()
            )
        }
    })?;
    config.data_dir = options
        .data_dir
        .unwrap_or_else(|| temporary.path().to_path_buf());
    config.processors = vec![processor_config(&module, params, options.from_block)?];
    let config = config
        .validate()
        .map_err(|errors| anyhow::anyhow!(errors))?
        .into_inner();
    let report = Box::pin(run_standalone(
        config.clone(),
        INSTANCE,
        options.from_block,
        options.to_block,
        registry,
    ))
    .await?;
    eprintln!("{}", serde_json::to_string_pretty(&report)?);

    let processor = registry.instantiate(&config.processors[0], config.chain.chain_id)?;
    let store = SqliteStore::open(
        configured_store_config(&config, config.data_dir.join("leani.sqlite"))
            .with_processors(vec![processor.descriptor().clone()]),
    )
    .await?;
    let mut after = 0;
    loop {
        let page = store
            .changes(
                processor.descriptor(),
                ChainId(config.chain.chain_id),
                after,
                PAGE,
            )
            .await?;
        for record in &page {
            let line = serde_json::to_string(&change_line(processor.as_ref(), record)?)?;
            match print_row(&line) {
                Err(error) if error.downcast_ref::<StdoutClosed>().is_some() => {
                    return Ok(Exit::Success);
                }
                printed => printed?,
            }
            after = record.cursor.sequence;
        }
        if page.len() < PAGE {
            return Ok(Exit::Success);
        }
    }
}

/// The parameters object: `--params` JSON, then each `--param KEY=VALUE`.
fn parameters(
    json: Option<&str>,
    pairs: &[String],
) -> Result<serde_json::Map<String, serde_json::Value>> {
    let mut params = match json
        .map(serde_json::from_str)
        .transpose()
        .context("--params is not valid JSON")?
    {
        None => serde_json::Map::new(),
        Some(serde_json::Value::Object(params)) => params,
        Some(_) => bail!("--params must be a JSON object"),
    };
    for pair in pairs {
        let Some((key, value)) = pair.split_once('=') else {
            bail!("--param `{pair}` is not KEY=VALUE");
        };
        // TOML integers are 64-bit signed: a larger amount stays a decimal
        // string, which the guest SDK's U256 reads.
        let value =
            if value.bytes().all(|byte| byte.is_ascii_digit()) && value.parse::<i64>().is_err() {
                serde_json::Value::String(value.to_owned())
            } else {
                serde_json::from_str(value)
                    .unwrap_or_else(|_| serde_json::Value::String(value.to_owned()))
            };
        params.insert(key.to_owned(), value);
    }
    Ok(params)
}

/// An on-demand, finalized-only instance of `module` starting at `start_block`.
fn processor_config(
    module: &Path,
    params: serde_json::Map<String, serde_json::Value>,
    start_block: u64,
) -> Result<ProcessorConfig> {
    let mut configured = processor_contract(
        leani_processor_wasm::PROCESSOR_ID,
        INSTANCE,
        "0.0.0",
        start_block,
        PublishMode::Finalized,
    );
    // One pass over finalized blocks into a throwaway store: no checkpoints
    // to restore from.
    configured.state = StatePolicyConfig {
        mode: StatePolicyMode::Durable,
    };
    configured.checkpoint = CheckpointPolicyConfig {
        mode: CheckpointPolicyMode::None,
        keep: 0,
    };
    configured.settings.insert(
        "module".to_owned(),
        toml::Value::String(module.to_string_lossy().into_owned()),
    );
    configured.settings.insert(
        "params".to_owned(),
        toml::Value::try_from(serde_json::Value::Object(params)).context(
            "parameters must be TOML-representable: no nulls, and integers past 2^63 as strings",
        )?,
    );
    Ok(configured)
}

fn change_line(processor: &dyn Processor, record: &ChangeRecord) -> Result<serde_json::Value> {
    Ok(serde_json::json!({
        "block": record.block.number.0,
        "blockHash": record.block.hash.to_string(),
        "direction": record.direction,
        "kind": record.change.kind,
        "key": format!("0x{}", hex::encode(&record.change.key)),
        "operation": match record.change.operation {
            ChangeOperation::Upsert => "upsert",
            ChangeOperation::Delete => "delete",
        },
        "value": processor.change_json(&record.change)?,
    }))
}

fn bundled(name: &str) -> Result<&'static leani_processor_wasm::Template> {
    leani_processor_wasm::TEMPLATES
        .iter()
        .find(|template| template.name == name)
        .with_context(|| {
            format!(
                "no template `{name}`; bundled: {}",
                leani_processor_wasm::TEMPLATES
                    .iter()
                    .map(|template| template.name)
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parameters_merge_json_with_typed_pairs() {
        let params = parameters(
            Some(r#"{"tokens":["0xa0b8"],"minValue":"1"}"#),
            &["minValue=5".to_owned(), "label=usdc".to_owned()],
        )
        .expect("parameters");
        assert_eq!(
            serde_json::Value::Object(params),
            serde_json::json!({ "tokens": ["0xa0b8"], "minValue": 5, "label": "usdc" })
        );
        assert!(parameters(Some("[1]"), &[]).is_err());
        assert!(parameters(None, &["novalue".to_owned()]).is_err());
        let invalid = parameters(Some("{"), &[]).expect_err("invalid JSON");
        assert!(format!("{invalid:#}").contains("EOF"), "{invalid:#}");
    }

    #[test]
    fn integers_past_toml_range_stay_decimal_strings() {
        let params = parameters(
            None,
            &[
                "minValue=100000000000000000000".to_owned(),
                "count=9223372036854775807".to_owned(),
            ],
        )
        .expect("parameters");
        assert_eq!(
            serde_json::Value::Object(params.clone()),
            serde_json::json!({ "minValue": "100000000000000000000", "count": 9_223_372_036_854_775_807_i64 })
        );
        assert!(processor_config(Path::new("/x.wasm"), params, 1).is_ok());
    }

    #[test]
    fn the_run_instance_is_a_valid_on_demand_wasm_processor() {
        let configured = processor_config(
            Path::new("/modules/x.wasm"),
            serde_json::Map::new(),
            19_000_000,
        )
        .expect("configuration");
        assert_eq!(configured.id, "wasm");
        assert_eq!(configured.start_block, 19_000_000);
        assert!(matches!(
            configured.history_mode,
            crate::config::ProcessorHistoryMode::OnDemand
        ));
        assert_eq!(configured.publish, Some(PublishMode::Finalized));
        assert_eq!(
            configured.undo.mode,
            leani_processor_api::UndoPolicyMode::None
        );
        assert!(configured.lifecycle_policies().is_ok());
        assert_eq!(
            configured.settings["module"].as_str(),
            Some("/modules/x.wasm")
        );
    }
}
