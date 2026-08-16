use std::{fs, path::Path};

use clap::CommandFactory;
use leani::{Cli, ProcessorRegistry};
use serde::Serialize;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CliFixture {
    schema_version: u32,
    command: CommandFixture,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CommandFixture {
    name: String,
    about: Option<String>,
    usage: String,
    arguments: Vec<ArgumentFixture>,
    subcommands: Vec<CommandFixture>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ArgumentFixture {
    id: String,
    long: Option<String>,
    short: Option<char>,
    help: Option<String>,
    required: bool,
    action: String,
    default_values: Vec<String>,
    possible_values: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProcessorCatalogFixture {
    schema_version: u32,
    processors: Vec<leani::ProcessorFactoryMetadata>,
}

fn command_fixture(mut command: clap::Command) -> CommandFixture {
    command.build();
    let usage = command.clone().render_usage().to_string();
    let arguments = command
        .get_arguments()
        .map(|argument| ArgumentFixture {
            id: argument.get_id().to_string(),
            long: argument.get_long().map(ToOwned::to_owned),
            short: argument.get_short(),
            help: argument.get_help().map(ToString::to_string),
            required: argument.is_required_set(),
            action: format!("{:?}", argument.get_action()),
            default_values: argument
                .get_default_values()
                .iter()
                .map(|value| value.to_string_lossy().into_owned())
                .collect(),
            possible_values: argument
                .get_value_parser()
                .possible_values()
                .into_iter()
                .flatten()
                .map(|value| value.get_name().to_owned())
                .collect(),
        })
        .collect();
    let subcommands = command
        .get_subcommands()
        .cloned()
        .map(command_fixture)
        .collect();
    CommandFixture {
        name: command.get_name().to_owned(),
        about: command.get_about().map(ToString::to_string),
        usage,
        arguments,
        subcommands,
    }
}

fn normalized_json(value: &impl Serialize) -> String {
    format!(
        "{}\n",
        serde_json::to_string_pretty(value).expect("serialize documentation fixture")
    )
}

fn check_or_update(path: &Path, expected: &str) {
    if std::env::var_os("LEANI_UPDATE_DOC_FIXTURES").is_some() {
        fs::write(path, expected).unwrap_or_else(|error| panic!("{}: {error}", path.display()));
        return;
    }
    let actual = fs::read_to_string(path).unwrap_or_else(|error| {
        panic!(
            "{}: {error}; run scripts/update-rust-doc-fixtures.sh",
            path.display()
        )
    });
    assert_eq!(
        actual,
        expected,
        "{} is stale; run scripts/update-rust-doc-fixtures.sh",
        path.display()
    );
}

#[test]
fn tracked_rust_documentation_fixtures_are_current() {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let generated = repository.join("docs/reference/generated");
    let cli = CliFixture {
        schema_version: 1,
        command: command_fixture(Cli::command()),
    };
    let processors = ProcessorCatalogFixture {
        schema_version: 1,
        processors: ProcessorRegistry::standard().catalog(),
    };
    check_or_update(&generated.join("cli.json"), &normalized_json(&cli));
    check_or_update(
        &generated.join("processor-catalog.json"),
        &normalized_json(&processors),
    );
}
