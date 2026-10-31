//! Write a Wasm processor as a Rust crate or a TypeScript package: generated
//! from a [`Spec`] of the onchain data to keep, or around any source, such as
//! a template's.
//!
//! A project carries its own copy of the guest SDK, the one this node runs,
//! and a lockfile, so it builds against exactly this ABI.

mod typescript;

use std::fmt::Write as _;

use leani_wasm_guest::{
    parse_hex,
    solidity::{Kind, Signature},
};
use serde::{Deserialize, Serialize};

/// The SDK files a crate carries, beside its `Cargo.toml`.
const GUEST_FILES: [(&str, &str); 7] = [
    ("src/lib.rs", include_str!("../../wasm-guest/src/lib.rs")),
    ("src/abi.rs", include_str!("../../wasm-guest/src/abi.rs")),
    ("src/ffi.rs", include_str!("../../wasm-guest/src/ffi.rs")),
    (
        "src/solidity.rs",
        include_str!("../../wasm-guest/src/solidity.rs"),
    ),
    ("src/u256.rs", include_str!("../../wasm-guest/src/u256.rs")),
    ("README.md", include_str!("../../wasm-guest/README.md")),
    ("LICENSE", include_str!("../../wasm-guest/LICENSE")),
];

/// The TypeScript SDK's files, which a package carries as `assembly/leani/`.
const TYPESCRIPT_FILES: [(&str, &str); 10] = [
    (
        "abi.ts",
        include_str!("../../../packages/wasm-guest/assembly/abi.ts"),
    ),
    (
        "bytes.ts",
        include_str!("../../../packages/wasm-guest/assembly/bytes.ts"),
    ),
    (
        "exports.ts",
        include_str!("../../../packages/wasm-guest/assembly/exports.ts"),
    ),
    (
        "ffi.ts",
        include_str!("../../../packages/wasm-guest/assembly/ffi.ts"),
    ),
    (
        "index.ts",
        include_str!("../../../packages/wasm-guest/assembly/index.ts"),
    ),
    (
        "json.ts",
        include_str!("../../../packages/wasm-guest/assembly/json.ts"),
    ),
    (
        "postcard.ts",
        include_str!("../../../packages/wasm-guest/assembly/postcard.ts"),
    ),
    (
        "solidity.ts",
        include_str!("../../../packages/wasm-guest/assembly/solidity.ts"),
    ),
    (
        "u256.ts",
        include_str!("../../../packages/wasm-guest/assembly/u256.ts"),
    ),
    (
        "LICENSE",
        include_str!("../../../packages/wasm-guest/LICENSE"),
    ),
];

/// The version of `assemblyscript`, the compiler, a package builds with, as
/// the SDK pins it.
pub const ASSEMBLYSCRIPT: &str = "0.28.20";

/// The SDK's lockfile, which pins the compiler and its dependencies.
const TYPESCRIPT_LOCK: &str = include_str!("../../../packages/wasm-guest/package-lock.json");

/// Builds a TypeScript processor reading only it and the SDK beside it:
/// `node build.mjs <processor.ts> <sdk directory> <out.wasm>`.
pub const TYPESCRIPT_BUILD: &str = include_str!("../../../packages/wasm-guest/build.mjs");

/// The examples' lockfile, which pins the SDK's dependencies as this release
/// builds them, and the example crates it also locks.
const EXAMPLES_LOCK: &str = include_str!("../../../examples/wasm-processors/Cargo.lock");
/// Most events or functions one spec decodes.
const MAX_SIGNATURES: usize = 32;
/// Longest project and manifest name.
const MAX_NAME_BYTES: usize = 64;

/// The language a processor is written in.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Language {
    #[default]
    Rust,
    /// As the `assemblyscript` compiler compiles it: TypeScript's syntax,
    /// Wasm's types.
    TypeScript,
}

impl Language {
    /// The processor's source, relative to its project.
    #[must_use]
    pub fn source_path(self) -> &'static str {
        match self {
            Self::Rust => "src/lib.rs",
            Self::TypeScript => "assembly/index.ts",
        }
    }

    #[expect(
        clippy::trivially_copy_pass_by_ref,
        reason = "serde's skip_serializing_if passes a reference"
    )]
    fn is_rust(&self) -> bool {
        *self == Self::Rust
    }
}

/// The spec of a processor to generate: which data, and what to keep of it.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Spec {
    /// At most 64 lowercase letters, digits, and dashes, starting with a
    /// letter: the project and manifest name.
    #[serde(default = "default_name")]
    pub name: String,
    #[serde(default, skip_serializing_if = "Language::is_rust")]
    pub language: Language,
    pub data: Data,
    #[serde(default)]
    pub keep: Keep,
}

fn default_name() -> String {
    "my-processor".to_owned()
}

/// The onchain data a processor reads. Empty `fields` keeps every field.
/// A decoded argument keeps its signature's name, and replaces a field of the
/// data with the same name, such as a transfer's `to` and `value`.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", deny_unknown_fields, rename_all = "camelCase")]
pub enum Data {
    /// Contract events, decoded from their Solidity signatures.
    Events {
        /// Contracts to watch; empty watches every contract.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        contracts: Vec<String>,
        events: Vec<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        fields: Vec<String>,
    },
    /// Transactions, optionally calls of given functions, decoded.
    Transactions {
        /// Recipients to watch; empty watches every transaction.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        to: Vec<String>,
        /// Senders to watch.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        from: Vec<String>,
        /// Function signatures; empty keeps calls of any function.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        functions: Vec<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        fields: Vec<String>,
    },
    /// One row per block, from its header.
    Blocks {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        fields: Vec<String>,
    },
}

/// What the processor keeps of the matches.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(tag = "kind", deny_unknown_fields, rename_all = "camelCase")]
pub enum Keep {
    /// One entity per match; block-local.
    #[default]
    Rows,
    /// A running count, and sums, per value of one field, or of everything;
    /// ordered.
    Totals {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        by: Option<String>,
        /// Unsigned integer fields to add up.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        sum: Vec<String>,
    },
}

/// A generated crate or package: its files, by path relative to its
/// directory.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Project {
    pub name: String,
    pub language: Language,
    pub files: Vec<(String, String)>,
    /// Every field the spec's data offers, kept or not; empty around a
    /// given source.
    pub available: Vec<Available>,
}

/// A field a spec may keep, and whether totals can add it up.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Available {
    pub name: String,
    pub unsigned: bool,
}

impl Project {
    /// The processor's source, `src/lib.rs` or `assembly/index.ts`.
    #[must_use]
    pub fn source(&self) -> &str {
        self.files
            .iter()
            .find(|(path, _)| path == self.language.source_path())
            .map_or("", |(_, source)| source)
    }

    /// The command that builds it, run in its directory.
    #[must_use]
    pub fn build_command(&self) -> &'static str {
        match self.language {
            Language::Rust => "cargo build --release --target wasm32-unknown-unknown",
            Language::TypeScript => "npm install && npm run build",
        }
    }

    /// The module the build writes, relative to the project.
    #[must_use]
    pub fn module_path(&self) -> String {
        match self.language {
            Language::Rust => format!(
                "target/wasm32-unknown-unknown/release/{}.wasm",
                self.name.replace('-', "_")
            ),
            Language::TypeScript => format!("build/{}.wasm", self.name),
        }
    }
}

/// The spec `leani processor create` starts from when given none.
#[must_use]
pub fn starter() -> Spec {
    Spec {
        name: "usdc-transfers".to_owned(),
        language: Language::Rust,
        data: Data::Events {
            contracts: vec!["0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48".to_owned()],
            events: vec![
                "Transfer(address indexed from, address indexed to, uint256 value)".to_owned(),
            ],
            fields: Vec::new(),
        },
        keep: Keep::Rows,
    }
}

/// A project around `source`, as its `src/lib.rs` or `assembly/index.ts`.
///
/// # Errors
///
/// Returns a message for an invalid name.
pub fn project_for(name: &str, language: Language, source: &str) -> Result<Project, String> {
    check_name(name)?;
    let files = match language {
        Language::Rust => rust_files(name, source),
        Language::TypeScript => typescript_files(name, source)?,
    };
    Ok(Project {
        name: name.to_owned(),
        language,
        files,
        available: Vec::new(),
    })
}

fn rust_files(name: &str, source: &str) -> Vec<(String, String)> {
    let mut files = vec![
        ("Cargo.toml".to_owned(), cargo_toml(name)),
        // As the bundled examples build: `cargo build --release` suffices.
        (
            ".cargo/config.toml".to_owned(),
            include_str!("../../../examples/wasm-processors/.cargo/config.toml").to_owned(),
        ),
        ("Cargo.lock".to_owned(), cargo_lock(name)),
        (".gitignore".to_owned(), "/target\n".to_owned()),
        ("src/lib.rs".to_owned(), source.to_owned()),
    ];
    let (leani, sdk) = (env!("CARGO_PKG_VERSION"), leani_wasm_guest::VERSION);
    files.push((
        "leani-wasm-guest/Cargo.toml".to_owned(),
        format!(
            "# The leani-wasm-guest SDK of leani {leani}, which this crate builds against.\n\
             [package]\nname = \"leani-wasm-guest\"\nversion = \"{sdk}\"\nedition = \"2024\"\n\
             license = \"MIT\"\npublish = false\n\n[dependencies]\n\
             postcard = {{ version = \"1.1\", features = [\"alloc\"] }}\n\
             serde = {{ version = \"1.0\", features = [\"derive\"] }}\nserde_json = \"1.0\"\n"
        ),
    ));
    files.extend(
        GUEST_FILES
            .iter()
            .map(|(path, text)| (format!("leani-wasm-guest/{path}"), (*text).to_owned())),
    );
    files
}

fn typescript_files(name: &str, source: &str) -> Result<Vec<(String, String)>, String> {
    let package = serde_json::json!({
        "name": name,
        "version": "0.1.0",
        "private": true,
        "type": "module",
        "scripts": {
            // As the node builds it: the stub runtime, as every call gets a
            // fresh instance, and errors reported through the node.
            "build": format!(
                "asc assembly/index.ts --outFile build/{name}.wasm --runtime stub \
                 --use abort=assembly/leani/ffi/abort --optimizeLevel 3 --shrinkLevel 1"
            ),
        },
        "devDependencies": { "assemblyscript": ASSEMBLYSCRIPT },
    });
    let mut lock: serde_json::Value =
        serde_json::from_str(TYPESCRIPT_LOCK).map_err(|error| error.to_string())?;
    lock["name"] = name.into();
    lock["version"] = "0.1.0".into();
    lock["packages"][""] = serde_json::json!({
        "name": name,
        "version": "0.1.0",
        "devDependencies": { "assemblyscript": ASSEMBLYSCRIPT },
    });
    let pretty = |value: &serde_json::Value| {
        serde_json::to_string_pretty(value)
            .map(|text| text + "\n")
            .map_err(|error| error.to_string())
    };
    let mut files = vec![
        ("package.json".to_owned(), pretty(&package)?),
        ("package-lock.json".to_owned(), pretty(&lock)?),
        (".gitignore".to_owned(), "/node_modules\n/build\n".to_owned()),
        // Editors read AssemblyScript's types, such as `u64`, from here.
        (
            "assembly/tsconfig.json".to_owned(),
            "{\n  \"extends\": \"assemblyscript/std/assembly.json\",\n  \"include\": [\"./**/*.ts\"]\n}\n".to_owned(),
        ),
        ("assembly/index.ts".to_owned(), source.to_owned()),
    ];
    files.extend(
        TYPESCRIPT_FILES
            .iter()
            .map(|(path, text)| (format!("assembly/leani/{path}"), (*text).to_owned())),
    );
    Ok(files)
}

/// Generate the project a spec describes, in its language.
///
/// # Errors
///
/// Returns a message for an invalid name, address, signature, or field.
pub fn generate(spec: &Spec) -> Result<Project, String> {
    check_name(&spec.name)?;
    let generator = Generator::new(spec)?;
    let source = match spec.language {
        Language::Rust => generator.source()?,
        Language::TypeScript => generator.typescript()?,
    };
    let mut generated = project_for(&spec.name, spec.language, &source)?;
    generated.available = generator.all;
    Ok(generated)
}

/// Identifiers Rust source may not name anywhere, so no alias, path, or
/// macro reaches them: what reads files or the environment while it
/// compiles, and what embeds them in the module.
const REFUSED: [&str; 10] = [
    "include",
    "include_str",
    "include_bytes",
    "env",
    "option_env",
    "asm",
    "global_asm",
    "naked_asm",
    "debugger_visualizer",
    "concat_idents",
];

/// Refuse source too large to build, or Rust that reads files or the
/// environment while it compiles: a build runs on the node's host, so such
/// source could expose them. TypeScript cannot; its build script serves
/// the compiler the processor and the SDK only.
///
/// # Errors
///
/// Returns a message naming the construct.
pub fn check_source(source: &str, language: Language) -> Result<(), String> {
    const MAX_BYTES: usize = 256 * 1024;
    if source.len() > MAX_BYTES {
        return Err(format!("source is over {} KiB", MAX_BYTES / 1024));
    }
    if language == Language::TypeScript {
        return Ok(());
    }
    let tokens = code_tokens(source);
    for (index, token) in tokens.iter().enumerate() {
        let next = tokens.get(index + 1).map(String::as_str);
        let reason = match token.as_str() {
            token if REFUSED.contains(&token) => {
                "a build runs on the node, and the source may read no files or environment"
            }
            // `#![feature(...)]`: nightly features, none of which a
            // processor needs.
            "feature" if next == Some("(") => "nightly features are not available",
            // Any other form reads a file, whatever `#[path]`, raw
            // identifier, or macro names it.
            "mod"
                if !(next.is_some_and(is_identifier)
                    && tokens.get(index + 2).is_some_and(|after| after == "{")) =>
            {
                "only inline modules, `mod name { ... }`, are allowed"
            }
            _ => continue,
        };
        return Err(format!("`{token}` is not allowed: {reason}"));
    }
    Ok(())
}

fn is_identifier(token: &str) -> bool {
    token
        .chars()
        .next()
        .is_some_and(|first| first.is_alphabetic() || first == '_')
}

/// Identifiers and punctuation outside comments and literals, lexed as
/// rustc lexes them, so nothing it compiles hides in what this skips.
fn code_tokens(source: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut rest = source;
    while let Some(first) = rest.chars().next() {
        let skip = if rest.starts_with("//") {
            rest.find('\n').unwrap_or(rest.len())
        } else if rest.starts_with("/*") {
            // Comments nest; ending at the first `*/` only shows more code.
            rest.find("*/").map_or(rest.len(), |end| end + 2)
        } else if let Some(skip) = raw_string(rest) {
            skip
        } else if first == '"' {
            quoted(rest, '"')
        } else if first == '\'' {
            character(rest)
        } else if first.is_alphanumeric() || first == '_' {
            let end = rest
                .find(|character: char| !(character.is_alphanumeric() || character == '_'))
                .unwrap_or(rest.len());
            tokens.push(rest[..end].to_owned());
            end
        } else {
            if !first.is_whitespace() {
                tokens.push(first.to_string());
            }
            first.len_utf8()
        };
        rest = &rest[skip..];
    }
    tokens
}

/// The length of the literal `text` opens with `quote`, through its
/// closing quote, skipping escapes.
fn quoted(text: &str, quote: char) -> usize {
    let mut escaped = false;
    for (index, character) in text.char_indices().skip(1) {
        if escaped {
            escaped = false;
        } else if character == '\\' {
            escaped = true;
        } else if character == quote {
            return index + character.len_utf8();
        }
    }
    text.len()
}

/// A char literal's length, or one for the quote of a lifetime or label.
fn character(text: &str) -> usize {
    let mut characters = text.char_indices().skip(1);
    match (characters.next(), characters.next()) {
        (Some((_, '\\')), _) => quoted(text, '\''),
        (Some(_), Some((index, '\''))) => index + 1,
        _ => 1,
    }
}

/// The length of a raw string literal at the start of `text`: `r`, `br`,
/// or `cr`, with or without hashes.
fn raw_string(text: &str) -> Option<usize> {
    let after = text
        .strip_prefix("br")
        .or_else(|| text.strip_prefix("cr"))
        .or_else(|| text.strip_prefix('r'))?;
    let hashes = after.len() - after.trim_start_matches('#').len();
    let body = after[hashes..].strip_prefix('"')?;
    let close = format!("\"{}", "#".repeat(hashes));
    let end = body.find(&close)?;
    Some(text.len() - body.len() + end + close.len())
}

fn check_name(name: &str) -> Result<(), String> {
    let valid = name.len() <= MAX_NAME_BYTES
        && name.starts_with(|character: char| character.is_ascii_lowercase())
        && name.chars().all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '-'
        });
    if valid {
        Ok(())
    } else {
        Err(format!(
            "name `{name}`: use at most {MAX_NAME_BYTES} lowercase letters, digits, and dashes, starting with a letter"
        ))
    }
}

fn cargo_toml(name: &str) -> String {
    format!(
        "[package]\nname = \"{name}\"\nversion = \"0.1.0\"\nedition = \"2024\"\npublish = false\n\n\
         # Its own workspace, wherever it is created.\n[workspace]\n\n\
         [lib]\ncrate-type = [\"cdylib\"]\n\n[dependencies]\n\
         leani-wasm-guest = {{ path = \"leani-wasm-guest\" }}\n\
         serde = {{ version = \"1.0\", features = [\"derive\"] }}\n\n\
         # Small modules: the node interprets them, so less code is less to\n\
         # translate. Build with `cargo build --release --target wasm32-unknown-unknown`.\n\
         [profile.release]\nopt-level = \"s\"\nlto = true\ncodegen-units = 1\npanic = \"abort\"\nstrip = true\n"
    )
}

/// The examples' lockfile with this crate in place of the examples: with its
/// dependencies cached, a crate builds without the registry's index.
fn cargo_lock(name: &str) -> String {
    let mut blocks = EXAMPLES_LOCK.split("[[package]]\n");
    let mut lock = blocks.next().unwrap_or_default().to_owned();
    let mut packages = blocks
        // The example crates are the packages without a source, beside the SDK.
        .filter(|block| {
            block.contains("\nsource = ") || block.starts_with("name = \"leani-wasm-guest\"\n")
        })
        .map(str::to_owned)
        .collect::<Vec<_>>();
    packages.push(format!(
        "name = \"{name}\"\nversion = \"0.1.0\"\ndependencies = [\n \"leani-wasm-guest\",\n \"serde\",\n]\n\n"
    ));
    packages.sort();
    for package in packages {
        lock.push_str("[[package]]\n");
        lock.push_str(&package);
    }
    lock.trim_end().to_owned() + "\n"
}

struct Field {
    name: String,
    unsigned: bool,
}

impl Field {
    fn new(name: &str, unsigned: bool) -> Self {
        Self {
            name: name.to_owned(),
            unsigned,
        }
    }
}

struct Generator<'a> {
    spec: &'a Spec,
    /// The fields of the data, in order; the kept ones when the spec names them.
    available: Vec<Field>,
    all: Vec<Available>,
    signatures: Vec<Signature>,
}

impl<'a> Generator<'a> {
    fn new(spec: &'a Spec) -> Result<Self, String> {
        let (available, signatures) = match &spec.data {
            Data::Events {
                contracts, events, ..
            } => {
                addresses(contracts)?;
                if events.is_empty() || events.len() > MAX_SIGNATURES {
                    return Err(format!("give between 1 and {MAX_SIGNATURES} events"));
                }
                let signatures = parse_all(events)?;
                let mut fields = vec![
                    Field::new("event", false),
                    Field::new("contract", false),
                    Field::new("transactionHash", false),
                    Field::new("logIndex", true),
                ];
                arguments_of(&signatures, &mut fields);
                (fields, signatures)
            }
            Data::Transactions {
                to,
                from,
                functions,
                ..
            } => {
                addresses(to)?;
                addresses(from)?;
                if functions.len() > MAX_SIGNATURES {
                    return Err(format!("give at most {MAX_SIGNATURES} functions"));
                }
                let signatures = parse_all(functions)?;
                let mut fields = vec![
                    Field::new("hash", false),
                    Field::new("from", false),
                    Field::new("to", false),
                    Field::new("value", true),
                    Field::new("success", false),
                    Field::new("gasUsed", true),
                    Field::new("effectiveGasPrice", true),
                ];
                if !signatures.is_empty() {
                    fields.push(Field::new("function", false));
                }
                arguments_of(&signatures, &mut fields);
                (fields, signatures)
            }
            Data::Blocks { .. } => (
                [
                    "gasUsed",
                    "gasLimit",
                    "baseFeePerGas",
                    "blobGasUsed",
                    "transactionCount",
                    "sizeBytes",
                ]
                .iter()
                .map(|name| Field::new(name, true))
                .collect(),
                Vec::new(),
            ),
        };
        let all = available
            .iter()
            .map(|field| Available {
                name: field.name.clone(),
                unsigned: field.unsigned,
            })
            .collect();
        let mut generator = Self {
            spec,
            available,
            all,
            signatures,
        };
        let chosen = match &spec.data {
            Data::Events { fields, .. }
            | Data::Transactions { fields, .. }
            | Data::Blocks { fields } => fields,
        };
        for name in chosen {
            generator.field(name)?;
        }
        if !chosen.is_empty() {
            generator
                .available
                .retain(|field| chosen.contains(&field.name));
        }
        generator.check_keep()?;
        Ok(generator)
    }

    /// Whether the kept fields can be kept so: beside them, a row keeps its
    /// block's number and time, and a total its group, count, and last block.
    fn check_keep(&self) -> Result<(), String> {
        if matches!(self.spec.keep, Keep::Rows) {
            // Only arguments can take these names; renaming one in its
            // signature keeps the signature's hash.
            if let Some(name) = ["blockNumber", "timestamp"]
                .into_iter()
                .find(|name| self.keeps(name))
            {
                return Err(format!(
                    "`{name}` is a name rows use for their block; rename the argument in its signature, which keeps the signature's hash"
                ));
            }
        }
        if let Keep::Totals { by, sum } = &self.spec.keep {
            if let Some(by) = by {
                self.kept(by)?;
                if sum.contains(by) {
                    return Err(format!(
                        "`{by}` cannot both group the totals and be added up"
                    ));
                }
            }
            for (index, name) in sum.iter().enumerate() {
                if !self.kept(name)?.unsigned {
                    return Err(format!(
                        "`{name}` is not an unsigned integer, so it does not add up"
                    ));
                }
                if sum[..index].contains(name) {
                    return Err(format!("`{name}` is added up once; name it once in `sum`"));
                }
            }
            // A total keeps its group, `count`, and `lastBlock` beside the
            // sums, under those names.
            let label = by.as_deref().unwrap_or("group");
            for name in by.iter().chain(sum) {
                if ["count", "lastBlock"].contains(&name.as_str())
                    || (by.is_none() && name == label)
                {
                    return Err(format!(
                        "`{name}` is a name totals use for themselves; totals cannot keep a field of that name"
                    ));
                }
            }
        }
        Ok(())
    }

    fn field(&self, name: &str) -> Result<&Field, String> {
        self.available
            .iter()
            .find(|field| field.name == name)
            .ok_or_else(|| {
                format!(
                    "no field `{name}`; the data has {}",
                    self.available
                        .iter()
                        .map(|field| format!("`{}`", field.name))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            })
    }

    /// A field the totals may group or add up: one the data has, and the
    /// spec's `fields` keeps.
    fn kept(&self, name: &str) -> Result<&Field, String> {
        if self.all.iter().any(|field| field.name == name) && !self.keeps(name) {
            return Err(format!("`{name}` is not among the fields the spec keeps"));
        }
        self.field(name)
    }

    fn keeps(&self, name: &str) -> bool {
        self.available.iter().any(|field| field.name == name)
    }

    /// The argument names to keep, or `None` for all of them.
    fn kept_arguments(&self) -> Option<Vec<&str>> {
        let all = self
            .signatures
            .iter()
            .flat_map(|signature| &signature.inputs)
            .all(|input| self.keeps(&input.name));
        (!all).then(|| {
            let mut names = Vec::new();
            for input in self
                .signatures
                .iter()
                .flat_map(|signature| &signature.inputs)
            {
                if self.keeps(&input.name) && !names.contains(&input.name.as_str()) {
                    names.push(input.name.as_str());
                }
            }
            names
        })
    }

    /// The comment a generated source opens with: what it keeps, and the
    /// spec it was generated from. Rust's is the crate's documentation.
    fn header(&self, comment: &str, build: &str) -> Result<String, String> {
        let spec = self.spec;
        let spec_json = serde_json::to_string_pretty(spec).map_err(|error| error.to_string())?;
        let (what, kind) = match &spec.data {
            Data::Events { .. } => ("decoded events", "event"),
            Data::Transactions { .. } => ("transactions", "transaction"),
            Data::Blocks { .. } => ("block headers", "block"),
        };
        let keeps = match &spec.keep {
            Keep::Rows => format!("one entity per {kind}"),
            Keep::Totals { by: Some(by), .. } => format!("a running total per `{by}`"),
            Keep::Totals { by: None, .. } => "one running total".to_owned(),
        };
        let mut out = String::new();
        let _ = writeln!(out, "{comment} {}: {what}, keeping {keeps}.", spec.name);
        let _ = write!(
            out,
            "{comment}\n{comment} Generated by leani from the spec below. Edit anything: `manifest` says\n\
             {comment} what the node delivers, `map` turns one block into rows, and `reduce`\n\
             {comment} keeps them. Build with `{build}`.\n{comment}\n"
        );
        // Rust documentation fences its code; a TypeScript comment indents it.
        let (open, indent) = if comment == "//!" {
            ("//! ```json\n", "")
        } else {
            ("", "  ")
        };
        out.push_str(open);
        for line in spec_json.lines() {
            let _ = writeln!(out, "{comment} {indent}{line}");
        }
        if comment == "//!" {
            out.push_str("//! ```\n");
        }
        out.push('\n');
        Ok(out)
    }

    fn source(&self) -> Result<String, String> {
        let spec = self.spec;
        let ordered = matches!(spec.keep, Keep::Totals { .. });
        let struct_name = type_name(&spec.name);
        let mut out = self.header(
            "//!",
            "cargo build --release --target wasm32-unknown-unknown",
        )?;
        let mut body = self.constants();
        let _ = write!(
            body,
            "\npub struct {struct_name};\n\n\
             /// One match: its key in the collection, and its fields as JSON. Deltas\n\
             /// travel as postcard, which cannot hold free-form JSON, hence the bytes.\n\
             #[derive(Deserialize, Serialize)]\npub struct Row {{\n    key: Vec<u8>,\n    fields: Vec<u8>,\n}}\n\n\
             impl Processor for {struct_name} {{\n    type Params = Value;\n    type Delta = Vec<Row>;\n\n"
        );
        body.push_str(&self.manifest(ordered));
        body.push_str(&self.map());
        body.push_str(&self.reduce());
        body.push_str("}\n");
        body.push_str(&self.helpers());
        let _ = writeln!(body, "\nexport_processor!({struct_name});");
        out.push_str(&imports(&body));
        out.push_str(&body);
        Ok(out)
    }

    fn constants(&self) -> String {
        let mut text = String::new();
        match &self.spec.data {
            Data::Events {
                contracts, events, ..
            } => {
                text.push_str("/// Contracts whose events count; empty counts every contract.\n");
                text.push_str(&list("CONTRACTS", contracts));
                text.push_str("/// The events to decode, as Solidity declares them.\n");
                text.push_str(&list("EVENTS", events));
            }
            Data::Transactions {
                to,
                from,
                functions,
                ..
            } => {
                text.push_str("/// Recipients to watch; empty watches every transaction.\n");
                text.push_str(&list("TO", to));
                text.push_str("/// Senders to watch; empty watches every sender.\n");
                text.push_str(&list("FROM", from));
                if !functions.is_empty() {
                    text.push_str(
                        "/// The functions to decode calls of, as Solidity declares them.\n",
                    );
                    text.push_str(&list("FUNCTIONS", functions));
                }
            }
            Data::Blocks { .. } => {}
        }
        if let Some(arguments) = self.kept_arguments() {
            text.push_str("/// The arguments each row keeps.\n");
            text.push_str(&list("ARGUMENTS", &arguments));
        }
        if let Keep::Totals { sum, .. } = &self.spec.keep {
            text.push_str("/// The fields each total adds up.\n");
            text.push_str(&list("SUMS", sum));
        }
        text
    }

    fn manifest(&self, ordered: bool) -> String {
        let name = &self.spec.name;
        let ordered = if ordered {
            "\n            // Totals read what earlier blocks added: reduce in chain order.\n            .ordered()"
        } else {
            ""
        };
        match &self.spec.data {
            Data::Events { .. } => {
                let hashes = if self.keeps("transactionHash") {
                    "\n            .logs_with_transaction_hash()"
                } else {
                    "\n            .require(Capability::Logs)"
                };
                format!(
                    "    fn manifest(_: &Value) -> Result<Manifest, Error> {{\n        \
                     let topics = events()?.into_iter().map(|(topic, _)| topic);\n        \
                     // The node keeps only these contracts' logs with these topics.\n        \
                     Ok(Manifest::new(\"{name}\"){hashes}\n            \
                     .addresses(addresses(&CONTRACTS)?)\n            .topic(0, topics){ordered})\n    }}\n\n"
                )
            }
            Data::Transactions {
                functions, from, ..
            } => {
                let requires = self.transaction_requirements(functions, from);
                let mut text = format!(
                    "    fn manifest(_: &Value) -> Result<Manifest, Error> {{\n        \
                     // The node keeps only transactions to and from these accounts.\n        \
                     Ok(Manifest::new(\"{name}\")"
                );
                for (capability, why) in requires {
                    let _ = write!(
                        text,
                        "\n            // {why}\n            .require(Capability::{capability})"
                    );
                }
                let _ = write!(
                    text,
                    "\n            .recipients(addresses(&TO)?)\n            .senders(addresses(&FROM)?){ordered})\n    }}\n\n"
                );
                text
            }
            Data::Blocks { .. } => {
                let size = if self.keeps("transactionCount") || self.keeps("sizeBytes") {
                    "\n            // transaction count and size\n            .require(Capability::BlockSize)"
                } else {
                    ""
                };
                format!(
                    "    fn manifest(_: &Value) -> Result<Manifest, Error> {{\n        \
                     Ok(Manifest::new(\"{name}\")\n            .require(Capability::Header){size}{ordered})\n    }}\n\n"
                )
            }
        }
    }

    fn arguments_loop(&self, signature: &str, values: &str) -> String {
        let filter = if self.kept_arguments().is_some() {
            "\n                if !ARGUMENTS.contains(&input.name.as_str()) {\n                    continue;\n                }"
        } else {
            ""
        };
        format!(
            "            for (input, value) in {signature}.inputs.iter().zip(&{values}) {{{filter}\n                \
             fields.insert(input.name.clone(), value.to_json());\n            }}\n"
        )
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one arm per kind of data, each the text of its map"
    )]
    fn map(&self) -> String {
        let insert = |name: &str, value: &str| {
            if !self.keeps(name) {
                return String::new();
            }
            let line = format!("            fields.insert(\"{name}\".into(), json!({value}));\n");
            // Past rustfmt's 100 columns, one argument per line.
            if line.len() <= 101 {
                line
            } else {
                format!(
                    "            fields.insert(\n                \"{name}\".into(),\n                json!({value}),\n            );\n"
                )
            }
        };
        let mut text =
            String::from("    fn map(_: &Value, block: &Block) -> Result<Vec<Row>, Error> {\n");
        match &self.spec.data {
            Data::Events { .. } => {
                text.push_str(
                    "        let events = events()?;\n        let mut rows = Vec::new();\n        \
                     for log in block.logs.as_deref().unwrap_or_default() {\n            \
                     let Some((_, event)) = events\n                .iter()\n                \
                     .find(|(topic, _)| log.topics.first() == Some(topic))\n            else {\n                \
                     continue;\n            };\n            \
                     // Another contract may emit the topic with another layout: a log\n            \
                     // that does not decode is not this event.\n            \
                     let Some(args) = event.decode_event(&log.topics[1..], &log.data) else {\n                \
                     continue;\n            };\n            let mut fields = Map::new();\n",
                );
                text.push_str(&insert("event", "event.name"));
                text.push_str(&insert("contract", "hex(&log.address)"));
                text.push_str(&insert("logIndex", "log.log_index"));
                if self.keeps("transactionHash") {
                    text.push_str("            fields.insert(\"transactionHash\".into(), json!(log.transaction_hash.map(|hash| hex(&hash))));\n");
                }
                text.push_str(&self.arguments_loop("event", "args"));
                text.push_str(
                    "            // A log index is unique within its block.\n            \
                     let mut key = block.number.to_be_bytes().to_vec();\n            \
                     key.extend_from_slice(&log.log_index.to_be_bytes());\n            \
                     rows.push(Row {\n                key,\n                fields: serde_json::to_vec(&fields)?,\n            });\n        }\n        Ok(rows)\n    }\n\n",
                );
            }
            Data::Transactions { functions, .. } => {
                if !functions.is_empty() {
                    text.push_str("        let functions = functions()?;\n");
                }
                let outcome = self.keeps_outcome();
                if outcome {
                    text.push_str(
                        "        // Receipts belong to the same transactions; match them by index.\n        \
                         let receipts = block.receipts.as_deref().unwrap_or_default();\n",
                    );
                }
                text.push_str(
                    "        let mut rows = Vec::new();\n        \
                     for transaction in block.transactions.as_deref().unwrap_or_default() {\n",
                );
                if !functions.is_empty() {
                    text.push_str(
                        "            let input = transaction.input.as_deref().unwrap_or_default();\n            \
                         let Some((_, function)) = functions\n                .iter()\n                \
                         .find(|(selector, _)| input.get(..4) == Some(selector.as_slice()))\n            else {\n                \
                         continue;\n            };\n            \
                         let Some(args) = function.decode_arguments(&input[4..]) else {\n                \
                         continue;\n            };\n",
                    );
                }
                text.push_str("            let mut fields = Map::new();\n");
                text.push_str(&insert("hash", "hex(&transaction.hash)"));
                text.push_str(&insert("from", "transaction.from.map(|from| hex(&from))"));
                text.push_str(&insert("to", "transaction.to.map(|to| hex(&to))"));
                text.push_str(&insert("value", "transaction.value.map(U256)"));
                if outcome {
                    text.push_str(
                        "            if let Some(receipt) = receipts\n                .iter()\n                \
                         .find(|receipt| receipt.transaction_index == transaction.index)\n            {\n",
                    );
                    for (name, value) in [
                        ("success", "receipt.success"),
                        ("gasUsed", "receipt.gas_used"),
                        ("effectiveGasPrice", "receipt.effective_gas_price.map(U256)"),
                    ] {
                        if self.keeps(name) {
                            let _ = writeln!(
                                text,
                                "                fields.insert(\"{name}\".into(), json!({value}));"
                            );
                        }
                    }
                    text.push_str("            }\n");
                }
                if !functions.is_empty() {
                    text.push_str(&insert("function", "function.name"));
                    text.push_str(&self.arguments_loop("function", "args"));
                }
                text.push_str(
                    "            rows.push(Row {\n                key: transaction.hash.to_vec(),\n                \
                     fields: serde_json::to_vec(&fields)?,\n            });\n        }\n        Ok(rows)\n    }\n\n",
                );
            }
            Data::Blocks { .. } => {
                text.push_str("        let header = block.header.clone().unwrap_or_default();\n        let mut fields = Map::new();\n");
                for (name, value) in [
                    ("gasUsed", "header.gas_used"),
                    ("gasLimit", "header.gas_limit"),
                    ("baseFeePerGas", "header.base_fee_per_gas.map(U256)"),
                    ("blobGasUsed", "header.blob_gas_used"),
                    ("transactionCount", "header.transaction_count"),
                    ("sizeBytes", "header.size_bytes"),
                ] {
                    if self.keeps(name) {
                        let _ = writeln!(
                            text,
                            "        fields.insert(\"{name}\".into(), json!({value}));"
                        );
                    }
                }
                text.push_str(
                    "        Ok(vec![Row {\n            key: block.number.to_be_bytes().to_vec(),\n            \
                     fields: serde_json::to_vec(&fields)?,\n        }])\n    }\n\n",
                );
            }
        }
        text
    }

    fn transaction_requirements(
        &self,
        functions: &[String],
        from: &[String],
    ) -> Vec<(&'static str, &'static str)> {
        let mut requires = vec![("Body", "recipients and values")];
        if self.keeps("from") || !from.is_empty() {
            requires.push(("Senders", "senders"));
        }
        if !functions.is_empty() {
            requires.push(("Calldata", "the input a call decodes from"));
        }
        if self.keeps_outcome() {
            requires.push(("ReceiptOutcome", "status and gas of each receipt"));
        }
        requires
    }

    fn keeps_outcome(&self) -> bool {
        ["success", "gasUsed", "effectiveGasPrice"]
            .iter()
            .any(|field| self.keeps(field))
    }

    /// The collection rows go to, and the kind of change each publishes.
    fn collection(&self) -> (&'static str, &'static str) {
        match &self.spec.data {
            Data::Events { .. } => ("events", "event"),
            Data::Transactions { .. } => ("transactions", "transaction"),
            Data::Blocks { .. } => ("blocks", "block"),
        }
    }

    fn reduce(&self) -> String {
        let (collection, kind) = self.collection();
        let mut text = String::from(
            "    fn reduce(_: &Value, block: &BlockRef, rows: Vec<Row>, store: &mut Store) -> Result<(), Error> {\n        \
             for row in rows {\n",
        );
        match &self.spec.keep {
            Keep::Rows => {
                let _ = write!(
                    text,
                    "            let mut fields: Map<String, Value> = serde_json::from_slice(&row.fields)?;\n            \
                     fields.insert(\"blockNumber\".into(), json!(block.number));\n            \
                     fields.insert(\"timestamp\".into(), json!(block.timestamp));\n            \
                     store.upsert(\"{collection}\", \"{kind}\", &row.key, &fields)?;\n"
                );
            }
            Keep::Totals { by, .. } => {
                text.push_str("            let fields: Map<String, Value> = serde_json::from_slice(&row.fields)?;\n");
                match by {
                    Some(by) => {
                        let _ = write!(
                            text,
                            "            // One total per `{by}`.\n            \
                             let group = fields.get(\"{by}\").map_or_else(|| \"none\".to_owned(), text);\n"
                        );
                    }
                    None => text.push_str("            // One total of every match.\n            let group = \"all\".to_owned();\n"),
                }
                let label = by.as_deref().unwrap_or("group");
                let _ = write!(
                    text,
                    "            let mut total: Map<String, Value> =\n                \
                     store.get_json(\"totals\", group.as_bytes())?.unwrap_or_default();\n            \
                     let count = total.get(\"count\").and_then(Value::as_u64).unwrap_or(0);\n            \
                     total.insert(\"{label}\".into(), json!(group));\n            \
                     total.insert(\"count\".into(), json!(count + 1));\n            \
                     for field in SUMS {{\n                add(&mut total, &fields, field)?;\n            }}\n            \
                     total.insert(\"lastBlock\".into(), json!(block.number));\n            \
                     store.upsert(\"totals\", \"total\", group.as_bytes(), &total)?;\n"
                );
            }
        }
        text.push_str("        }\n        Ok(())\n    }\n");
        text
    }

    fn helpers(&self) -> String {
        let mut text = String::new();
        match &self.spec.data {
            Data::Events { .. } => text.push_str(
                "\n/// Each event's topic 0 and signature.\nfn events() -> Result<Vec<(Hash, Signature)>, Error> {\n    \
                 EVENTS\n        .iter()\n        .map(|text| {\n            let signature = Signature::parse(text)?;\n            \
                 Ok((signature.topic(), signature))\n        })\n        .collect()\n}\n",
            ),
            Data::Transactions { functions, .. } if !functions.is_empty() => text.push_str(
                "\n/// Each function's selector and signature.\nfn functions() -> Result<Vec<([u8; 4], Signature)>, Error> {\n    \
                 FUNCTIONS\n        .iter()\n        .map(|text| {\n            let signature = Signature::parse(text)?;\n            \
                 Ok((signature.selector(), signature))\n        })\n        .collect()\n}\n",
            ),
            _ => {}
        }
        if !matches!(self.spec.data, Data::Blocks { .. }) {
            text.push_str(
                "\nfn addresses(list: &[&str]) -> Result<Vec<[u8; 20]>, Error> {\n    \
                 list.iter()\n        .map(|address| parse_hex::<20>(address).map_err(Error::from))\n        .collect()\n}\n",
            );
        }
        if matches!(self.spec.keep, Keep::Totals { by: Some(_), .. }) {
            text.push_str(
                "\n/// A JSON value as text: a string as it is, anything else as JSON.\nfn text(value: &Value) -> String {\n    \
                 value.as_str().map_or_else(|| value.to_string(), str::to_owned)\n}\n",
            );
        }
        if matches!(self.spec.keep, Keep::Totals { .. }) {
            text.push_str(
                "\n/// Add `fields[field]`, an unsigned integer, to the running sum `total[field]`.\n\
                 fn add(\n    total: &mut Map<String, Value>,\n    fields: &Map<String, Value>,\n    field: &str,\n) -> Result<(), Error> {\n    \
                 let Some(value) = fields.get(field).filter(|value| !value.is_null()) else {\n        return Ok(());\n    };\n    \
                 let value: U256 = serde_json::from_value(value.clone())?;\n    \
                 let sum = match total.get(field) {\n        Some(sum) => serde_json::from_value(sum.clone())?,\n        None => U256::ZERO,\n    };\n    \
                 total.insert(field.to_owned(), json!(sum.saturating_add(value)));\n    Ok(())\n}\n",
            );
        }
        text
    }
}

/// The processor's type: its name in upper camel case, then `Processor`, so
/// it shadows nothing the source names, such as `Vec` or `Block`.
fn type_name(name: &str) -> String {
    let mut upper = name
        .split('-')
        .map(|word| {
            let mut characters = word.chars();
            characters.next().map_or_else(String::new, |first| {
                first.to_ascii_uppercase().to_string() + characters.as_str()
            })
        })
        .collect::<String>();
    upper.push_str("Processor");
    upper
}

fn imports(body: &str) -> String {
    let mut guest = [
        "Block",
        "BlockRef",
        "Error",
        "Manifest",
        "Processor",
        "Store",
        "export_processor",
    ]
    .to_vec();
    for (name, used) in [
        ("Capability", "Capability::"),
        ("Hash", "(Hash,"),
        ("U256", "U256"),
        ("hex", "hex(&"),
        ("parse_hex", "parse_hex::"),
    ] {
        if body.contains(used) {
            guest.push(name);
        }
    }
    // Types, then functions and macros, as rustfmt orders them.
    guest.sort_unstable_by_key(|name| (name.starts_with(char::is_lowercase), *name));
    // As rustfmt wraps them.
    let mut text = String::from("use leani_wasm_guest::{\n");
    let mut line = String::new();
    for name in guest {
        if !line.is_empty() && line.len() + name.len() + 2 > 96 {
            let _ = writeln!(text, "    {}", line.trim_end());
            line.clear();
        }
        let _ = write!(line, "{name}, ");
    }
    let _ = writeln!(text, "    {}", line.trim_end());
    text.push_str("    serde_json::{self, Map, Value, json},\n");
    if body.contains("Signature::parse") {
        text.push_str("    solidity::Signature,\n");
    }
    text.push_str("};\nuse serde::{Deserialize, Serialize};\n\n");
    text
}

fn parse_all(signatures: &[String]) -> Result<Vec<Signature>, String> {
    signatures
        .iter()
        .map(|text| Signature::parse(text).map_err(|error| error.0))
        .collect()
}

fn addresses(list: &[String]) -> Result<(), String> {
    list.iter()
        .try_for_each(|address| parse_hex::<20>(address).map(drop))
}

fn arguments_of(signatures: &[Signature], fields: &mut Vec<Field>) {
    for input in signatures.iter().flat_map(|signature| &signature.inputs) {
        let unsigned = matches!(input.kind, Kind::Uint(_));
        match fields.iter_mut().find(|field| field.name == input.name) {
            // The row keeps the argument under the name, so the name adds
            // up only if everything it may hold is unsigned.
            Some(field) => field.unsigned &= unsigned,
            None => fields.push(Field::new(&input.name, unsigned)),
        }
    }
}

/// `const NAME: [&str; N] = [...];`
fn list(name: &str, items: &[impl AsRef<str>]) -> String {
    let items = items
        .iter()
        .map(|item| format!("{:?}", item.as_ref()))
        .collect::<Vec<_>>();
    let one_line = format!(
        "const {name}: [&str; {}] = [{}];\n",
        items.len(),
        items.join(", ")
    );
    if one_line.len() <= 100 {
        return one_line;
    }
    let mut text = format!("const {name}: [&str; {}] = [\n", items.len());
    for item in items {
        let _ = writeln!(text, "    {item},");
    }
    text.push_str("];\n");
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(json: serde_json::Value) -> Spec {
        serde_json::from_value(json).expect("spec")
    }

    #[test]
    fn the_starter_generates_a_crate_with_its_sdk() {
        let generated = generate(&starter()).expect("generate");
        let paths = generated
            .files
            .iter()
            .map(|(path, _)| path.as_str())
            .collect::<Vec<_>>();
        assert!(paths.contains(&"Cargo.toml"));
        assert!(paths.contains(&"leani-wasm-guest/src/solidity.rs"));
        let lock = &generated.files[paths
            .iter()
            .position(|path| *path == "Cargo.lock")
            .expect("lock")]
        .1;
        assert!(lock.contains("name = \"usdc-transfers\""), "{lock}");
        assert!(
            lock.contains("name = \"leani-wasm-guest\"") && lock.contains("name = \"serde_json\"")
        );
        // `cargo build --locked` works: the lock and the SDK copy agree.
        let version = format!("version = \"{}\"", leani_wasm_guest::VERSION);
        assert!(
            lock.contains(&format!("name = \"leani-wasm-guest\"\n{version}")),
            "{lock}"
        );
        let sdk = &generated.files[paths
            .iter()
            .position(|path| *path == "leani-wasm-guest/Cargo.toml")
            .expect("SDK manifest")]
        .1;
        assert!(sdk.contains(&version), "{sdk}");
        assert!(!lock.contains("name = \"erc20-transfers\""), "{lock}");
        let source = generated.source();
        assert!(source.contains("const EVENTS: [&str; 1]"), "{source}");
        assert!(
            source.contains("export_processor!(UsdcTransfersProcessor);"),
            "{source}"
        );
        assert!(
            source.contains("store.upsert(\"events\", \"event\""),
            "{source}"
        );
        assert!(check_source(source, Language::Rust).is_ok());
    }

    #[test]
    fn totals_add_up_unsigned_fields_and_name_their_group() {
        let generated = generate(&spec(serde_json::json!({
            "name": "usdc-volume",
            "data": { "kind": "events", "contracts": ["0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"],
                      "events": ["Transfer(address indexed from, address indexed to, uint256 value)"] },
            "keep": { "kind": "totals", "by": "to", "sum": ["value"] },
        })))
        .expect("generate");
        let source = generated.source();
        assert!(source.contains(".ordered()"), "{source}");
        assert!(
            source.contains("const SUMS: [&str; 1] = [\"value\"];"),
            "{source}"
        );
        assert!(source.contains("fields.get(\"to\")"), "{source}");

        let error = generate(&spec(serde_json::json!({
            "data": { "kind": "events", "events": ["Transfer(address indexed from, address indexed to, uint256 value)"] },
            "keep": { "kind": "totals", "sum": ["to"] },
        })))
        .expect_err("an address does not add up");
        assert!(error.contains("not an unsigned integer"), "{error}");

        // A field that groups the totals would overwrite its own sum.
        let error = generate(&spec(serde_json::json!({
            "data": { "kind": "blocks" },
            "keep": { "kind": "totals", "by": "gasUsed", "sum": ["gasUsed"] },
        })))
        .expect_err("by is also summed");
        assert!(error.contains("cannot both group"), "{error}");
        // Totals keep `count` and `lastBlock` themselves.
        let error = generate(&spec(serde_json::json!({
            "data": { "kind": "events", "events": ["Tick(uint256 count)"] },
            "keep": { "kind": "totals", "sum": ["count"] },
        })))
        .expect_err("count is the totals' own");
        assert!(error.contains("totals use for themselves"), "{error}");
        // A name another event declares signed may hold a negative value.
        let error = generate(&spec(serde_json::json!({
            "data": { "kind": "events", "events": ["A(uint256 value)", "B(int256 value)"] },
            "keep": { "kind": "totals", "sum": ["value"] },
        })))
        .expect_err("value may be negative");
        assert!(error.contains("not an unsigned integer"), "{error}");
        // A sum named twice would count each value twice.
        let error = generate(&spec(serde_json::json!({
            "data": { "kind": "events", "events": ["A(uint256 value)"] },
            "keep": { "kind": "totals", "sum": ["value", "value"] },
        })))
        .expect_err("value twice");
        assert!(error.contains("name it once"), "{error}");
        // The data has `gasLimit`, but the spec's fields leave it out.
        let error = generate(&spec(serde_json::json!({
            "data": { "kind": "blocks", "fields": ["gasUsed"] },
            "keep": { "kind": "totals", "by": "gasLimit" },
        })))
        .expect_err("gasLimit is not kept");
        assert!(
            error.contains("not among the fields the spec keeps"),
            "{error}"
        );
    }

    #[test]
    fn rows_keep_their_block_under_names_no_kept_field_has() {
        // A row keeps its block's number and time beside its fields, which
        // would overwrite an argument of either name.
        for event in ["Checkpoint(uint256 blockNumber)", "Stamp(uint64 timestamp)"] {
            let error = generate(&spec(serde_json::json!({
                "data": { "kind": "events", "events": [event] },
            })))
            .expect_err("the argument collides with the block's");
            assert!(error.contains("rows use for their block"), "{error}");
        }
        // Renamed in the signature, which keeps its hash, or not kept.
        generate(&spec(serde_json::json!({
            "data": { "kind": "events", "events": ["Checkpoint(uint256 checkpointBlock)"] },
        })))
        .expect("renamed");
        generate(&spec(serde_json::json!({
            "data": { "kind": "events", "events": ["Checkpoint(uint256 blockNumber)"], "fields": ["contract"] },
        })))
        .expect("not kept");
        // Totals keep no block number or time of their own.
        generate(&spec(serde_json::json!({
            "data": { "kind": "events", "events": ["Checkpoint(uint256 blockNumber)"] },
            "keep": { "kind": "totals", "sum": ["blockNumber"] },
        })))
        .expect("totals");
    }

    #[test]
    fn specs_name_the_fields_they_may_keep() {
        let error = generate(&spec(serde_json::json!({
            "data": { "kind": "blocks", "fields": ["gasUsed", "difficulty"] },
        })))
        .expect_err("no such field");
        assert!(
            error.contains("`difficulty`") && error.contains("`baseFeePerGas`"),
            "{error}"
        );
        let calls = generate(&spec(serde_json::json!({
            "name": "usdc-calls",
            "data": { "kind": "transactions", "to": ["0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"],
                      "functions": ["transfer(address to, uint256 amount)"], "fields": ["hash", "function", "amount", "success"] },
        })))
        .expect("generate");
        let source = calls.source();
        assert!(
            source.contains(".require(Capability::Calldata)"),
            "{source}"
        );
        assert!(
            source.contains(".require(Capability::ReceiptOutcome)"),
            "{source}"
        );
        assert!(
            source.contains("const ARGUMENTS: [&str; 1] = [\"amount\"];"),
            "{source}"
        );
        assert!(!source.contains("\"from\".into()"), "{source}");
        assert!(
            generate(&spec(
                serde_json::json!({ "name": "Bad Name", "data": { "kind": "blocks" } })
            ))
            .is_err()
        );
    }

    #[test]
    fn projects_carry_every_source_file_of_their_sdk() {
        let listed = |directory: &str, extension: &str| {
            let mut names =
                std::fs::read_dir(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(directory))
                    .expect("SDK directory")
                    .map(|entry| {
                        entry
                            .expect("entry")
                            .file_name()
                            .into_string()
                            .expect("UTF-8")
                    })
                    .filter(|name| name.ends_with(extension))
                    .collect::<Vec<_>>();
            names.sort();
            names
        };
        let carried = |files: &[(&str, &str)], prefix: &str, extension: &str| {
            let mut names = files
                .iter()
                .filter_map(|(path, _)| path.strip_prefix(prefix))
                .filter(|name| name.ends_with(extension))
                .map(str::to_owned)
                .collect::<Vec<_>>();
            names.sort();
            names
        };
        assert_eq!(
            carried(&GUEST_FILES, "src/", ".rs"),
            listed("../wasm-guest/src", ".rs")
        );
        assert_eq!(
            carried(&TYPESCRIPT_FILES, "", ".ts"),
            listed("../../packages/wasm-guest/assembly", ".ts")
        );
    }

    #[test]
    fn sources_that_read_files_or_the_environment_are_refused() {
        for refused in [
            "const A: &str = include_str!(\"/etc/hosts\");",
            "const B: &[u8] = include_bytes ! (\"key\");",
            "const C: &str = env!(\"HOME\");",
            "mod other;",
            // `#[path]` through `cfg_attr`, with a raw or a macro's name.
            "#[cfg_attr(all(), path = \"/etc/passwd\")] mod r#secret;",
            "macro_rules! m { ($n:ident) => { #[cfg_attr(all(), path = \"/etc/passwd\")] mod $n; } } m!(secret);",
            "macro_rules! m { () => { include!(\"x\") } }",
            // An alias or a raw identifier reaches the same macro.
            "use std::include_str as leak; compile_error!(leak!(\"/etc/hosts\"));",
            "const F: &str = r#include_str!(\"/etc/hosts\");",
            "use core::env as e;",
            // A raw C string ends at its first quote, escapes or not.
            "const S: &core::ffi::CStr = cr\"\\\"; include_str!(\"/etc/hosts\"); // \";",
            "core::arch::global_asm!(\".incbin \\\"/etc/hosts\\\"\");",
            "#![debugger_visualizer(gdb_script_file = \"/etc/hosts\")]",
            "#![feature(concat_idents)]",
        ] {
            assert!(check_source(refused, Language::Rust).is_err(), "{refused}");
        }
        for allowed in [
            "// include_str!(\"/etc/hosts\") in a comment",
            "const D: &str = \"env!(HOME)\";",
            "const E: &str = r#\"include!(\"x\")\"#;",
            "mod inline { pub fn environment() {} }",
            "fn f<'a>(x: &'a str) -> char { let _ = x; 'm' }",
            // Multibyte text lexes on character boundaries.
            "const SYMBOL: char = '€'; const GRÖSSE: u8 = 1; const QUOTE: char = '\\'';",
            "#[cfg(feature = \"extra\")] fn extra() {}",
            "fn f() { let path = 1; let _ = path; }",
            "fn g(v: &[u8], path: usize) -> u8 { v[path] }",
        ] {
            assert!(check_source(allowed, Language::Rust).is_ok(), "{allowed}");
        }
        assert!(check_source("const a = 1; // include_str!(", Language::TypeScript).is_ok());
        assert!(check_source(&"x".repeat(300 * 1024), Language::TypeScript).is_err());
    }

    #[test]
    fn typescript_packages_carry_their_sdk_lockfile_and_build_script() {
        let mut starter = starter();
        starter.language = Language::TypeScript;
        let generated = generate(&starter).expect("generate");
        let file = |wanted: &str| {
            generated
                .files
                .iter()
                .find(|(path, _)| path == wanted)
                .map_or_else(|| panic!("no {wanted}"), |(_, text)| text.as_str())
        };
        let package: serde_json::Value =
            serde_json::from_str(file("package.json")).expect("package");
        assert_eq!(package["name"], "usdc-transfers");
        assert_eq!(package["devDependencies"]["assemblyscript"], ASSEMBLYSCRIPT);
        assert!(
            package["scripts"]["build"]
                .as_str()
                .is_some_and(
                    |build| build.contains("--use abort=assembly/leani/ffi/abort")
                        && build.contains("build/usdc-transfers.wasm")
                ),
            "{package}"
        );
        let lock: serde_json::Value =
            serde_json::from_str(file("package-lock.json")).expect("lock");
        assert_eq!(lock["packages"][""]["name"], "usdc-transfers");
        assert_eq!(
            lock["packages"]["node_modules/assemblyscript"]["version"],
            ASSEMBLYSCRIPT
        );
        // The SDK pins the compiler this module names.
        let sdk: serde_json::Value =
            serde_json::from_str(include_str!("../../../packages/wasm-guest/package.json"))
                .expect("SDK package");
        assert_eq!(sdk["devDependencies"]["assemblyscript"], ASSEMBLYSCRIPT);
        assert!(file("assembly/leani/exports.ts").contains("export function leani_map"));
        let source = generated.source();
        assert_eq!(source, file("assembly/index.ts"));
        assert!(
            source.contains("class UsdcTransfersProcessor extends Processor"),
            "{source}"
        );
        assert!(
            source.contains("store.upsert(\"events\", \"event\", key, fields);"),
            "{source}"
        );
        assert!(
            source.contains("//     \"language\": \"typescript\","),
            "{source}"
        );
        assert_eq!(generated.module_path(), "build/usdc-transfers.wasm");
        assert!(TYPESCRIPT_BUILD.contains("abort=assembly/leani/ffi/abort"));
    }

    #[test]
    fn every_template_passes_the_guard() {
        for template in crate::TEMPLATES {
            assert!(
                check_source(template.source, Language::Rust).is_ok(),
                "{}",
                template.name
            );
            assert!(
                check_source(template.typescript, Language::TypeScript).is_ok(),
                "{}",
                template.name
            );
        }
    }

    #[test]
    fn processor_types_shadow_nothing_the_source_names() {
        assert_eq!(type_name("vec"), "VecProcessor");
        assert_eq!(type_name("block-gas"), "BlockGasProcessor");
        let generated = generate(&spec(
            serde_json::json!({ "name": "store", "language": "typescript", "data": { "kind": "blocks" } }),
        ))
        .expect("generate");
        assert!(
            generated
                .source()
                .contains("register(new StoreProcessor());")
        );
    }
}
