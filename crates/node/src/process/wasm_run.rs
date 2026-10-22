//! `leani processor run`, one Wasm processor over finalized history with its
//! changes printed as JSON lines, and `leani processor create`.
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

pub(super) fn new_project(
    directory: &Path,
    spec: Option<&Path>,
    template: Option<&str>,
    language: Option<leani_processor_wasm::scaffold::Language>,
    json: bool,
) -> Result<Exit> {
    use leani_processor_wasm::scaffold::{self, Language};
    let name = directory
        .file_name()
        .and_then(|name| name.to_str())
        .context("the directory needs a name")?;
    let generated = match (spec, template) {
        (Some(spec), _) => {
            let text = if spec == Path::new("-") {
                std::io::read_to_string(std::io::stdin()).context("read the spec from stdin")?
            } else {
                std::fs::read_to_string(spec).with_context(|| format!("read {}", spec.display()))?
            };
            let value: serde_json::Value =
                serde_json::from_str(&text).context("the spec is not valid JSON")?;
            // The directory names the project unless the spec does.
            let named = value.get("name").is_some();
            let mut spec: scaffold::Spec =
                serde_json::from_value(value).context("the spec is not valid")?;
            if !named {
                name.clone_into(&mut spec.name);
            }
            if let Some(language) = language {
                spec.language = language;
            }
            scaffold::generate(&spec)
        }
        (None, Some(template)) => {
            let found = bundled(template)?;
            let language = language.unwrap_or_default();
            let source = match language {
                Language::Rust => found.source,
                Language::TypeScript => found.typescript,
            };
            scaffold::project_for(name, language, source)
        }
        (None, None) => scaffold::generate(&scaffold::Spec {
            name: name.to_owned(),
            language: language.unwrap_or_default(),
            ..scaffold::starter()
        }),
    }
    .map_err(anyhow::Error::msg)?;
    if directory.exists()
        && std::fs::read_dir(directory)
            .with_context(|| format!("read {}", directory.display()))?
            .next()
            .is_some()
    {
        bail!("{} exists and is not empty", directory.display());
    }
    for (path, text) in &generated.files {
        let path = directory.join(path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create {}", parent.display()))?;
        }
        std::fs::write(&path, text).with_context(|| format!("write {}", path.display()))?;
    }
    let module = generated.module_path();
    let build = generated.build_command();
    let run = format!(
        "leani processor run {}/{module} --from-block <first> --to-block <last>",
        directory.display()
    );
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "path": directory.display().to_string(),
                "name": generated.name,
                "language": generated.language,
                "source": generated.language.source_path(),
                "files": generated.files.iter().map(|(path, _)| path).collect::<Vec<_>>(),
                // What a spec's `fields`, `by`, and `sum` may name.
                "fields": generated.available,
                "build": build,
                "module": module,
                "run": run,
            }))?
        );
    } else {
        let sdk = match generated.language {
            Language::Rust => "leani-wasm-guest/",
            Language::TypeScript => "assembly/leani/",
        };
        let layout = format!(
            "the processor is {}, and {sdk} is the SDK it builds against",
            generated.language.source_path()
        );
        println!(
            // The build in a subshell, so that `leani processor run` finds the
            // module and the node's configuration from where you are.
            "Created {}: {layout}.\n\nNext:\n  (cd {} && {build})\n  {run}\n  or watch the module in the node's /playground, which applies each rebuild live.",
            directory.display(),
            directory.display()
        );
    }
    Ok(Exit::Success)
}

#[cfg(test)]
mod tests {
    use leani_processor_wasm::scaffold::Language;

    use super::*;

    const VOLUME_SPEC: &str = r#"{"data":{"kind":"events","events":["Transfer(address indexed from, address indexed to, uint256 value)"]},"keep":{"kind":"totals","by":"to","sum":["value"]}}"#;

    fn spec_file(root: &Path, name: &str, spec: &str) -> PathBuf {
        let path = root.join(name);
        std::fs::write(&path, spec).expect("spec");
        path
    }

    fn read(path: &Path) -> String {
        std::fs::read_to_string(path).expect("source")
    }

    #[test]
    fn a_spec_writes_a_project_named_after_its_directory_with_its_sdk() {
        let root = tempfile::tempdir().expect("directory");
        let spec = spec_file(root.path(), "spec.json", VOLUME_SPEC);
        let rust = root.path().join("usdc-volume");
        new_project(&rust, Some(&spec), None, None, true).expect("rust");
        assert!(read(&rust.join("src/lib.rs")).contains("export_processor!(UsdcVolumeProcessor);"));
        assert!(rust.join("leani-wasm-guest/src/solidity.rs").exists());
        let typescript = root.path().join("usdc-volume-ts");
        new_project(
            &typescript,
            Some(&spec),
            None,
            Some(Language::TypeScript),
            true,
        )
        .expect("typescript");
        assert!(
            read(&typescript.join("assembly/index.ts"))
                .contains("register(new UsdcVolumeTsProcessor());")
        );
        assert!(typescript.join("assembly/leani/solidity.ts").exists());
        assert!(typescript.join("package-lock.json").exists());
    }

    #[test]
    fn a_kept_argument_called_name_does_not_name_the_project() {
        let root = tempfile::tempdir().expect("directory");
        let spec = spec_file(
            root.path(),
            "labels.json",
            r#"{"data":{"kind":"events","events":["Named(string name)"],"fields":["name"]}}"#,
        );
        let labels = root.path().join("labels");
        new_project(&labels, Some(&spec), None, None, true).expect("new");
        assert!(read(&labels.join("src/lib.rs")).contains("export_processor!(LabelsProcessor);"));
    }

    #[test]
    fn a_template_writes_its_source_in_either_language() {
        let root = tempfile::tempdir().expect("directory");
        let template = bundled("erc20-transfers").expect("template");
        let rust = root.path().join("transfers");
        new_project(&rust, None, Some(template.name), None, true).expect("rust");
        assert_eq!(read(&rust.join("src/lib.rs")), template.source);
        let typescript = root.path().join("transfers-ts");
        new_project(
            &typescript,
            None,
            Some(template.name),
            Some(Language::TypeScript),
            true,
        )
        .expect("typescript");
        assert_eq!(
            read(&typescript.join("assembly/index.ts")),
            template.typescript
        );
        assert!(new_project(&root.path().join("x"), None, Some("nope"), None, true).is_err());
    }

    #[test]
    fn a_project_never_overwrites_a_directory_with_files() {
        let root = tempfile::tempdir().expect("directory");
        let full = root.path().join("full");
        std::fs::create_dir(&full).expect("directory");
        std::fs::write(full.join("kept"), "").expect("file");
        let refused = new_project(&full, None, None, None, true).expect_err("not empty");
        assert!(refused.to_string().contains("not empty"), "{refused}");
    }

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
