//! `config/schema-v1.json` against the configuration structs it describes.
//!
//! The structs' derived `Deserialize` impls are the configuration contract:
//! a probe deserializer walks them without input and records every table's
//! fields and every enum's values, which the schema must declare exactly.
//! A table of arbitrary keys, a map field or a `flatten`, is reported unless
//! it is listed as free-form. An enum is probed through its first variant
//! only: every variant's name is compared, but no later variant's payload is
//! walked; configuration enums carry no data. Every shipped configuration
//! must then load and use only schema keys.

use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

use serde::{
    Deserialize,
    de::{
        self, DeserializeSeed, Deserializer, EnumAccess, IntoDeserializer, MapAccess, SeqAccess,
        VariantAccess, Visitor,
    },
};
use serde_json::Value;

use super::{Config, StarterConfig};

/// Accepted when decoding only so validation can refuse them and name the
/// replacement; the schema must not offer them. `path=value` names an enum
/// value, a bare path a field.
const REFUSED_BY_VALIDATION: &[&str] = &[
    "processors[].output.finalized_only",
    "processors[].state.mode=ephemeral",
    "sources.history[].kind=parquet",
];

/// Tables of arbitrary keys the schema leaves open on purpose: a processor
/// factory decodes its own settings strictly.
const FREE_FORM_MAPS: &[&str] = &["processors[].settings"];

/// What a type's `Deserialize` impl asks its input for.
#[derive(Debug, Default)]
enum Shape {
    /// A scalar, or a value the schema describes without members.
    #[default]
    Leaf,
    Table(Vec<(&'static str, Shape)>),
    Array(Box<Shape>),
    /// A table of arbitrary keys, such as processor settings.
    Map,
    Enum(&'static str, &'static [&'static str]),
}

fn shape_of<'de, T: Deserialize<'de>>() -> Shape {
    let mut shape = Shape::Leaf;
    if let Err(error) = T::deserialize(Probe { shape: &mut shape }) {
        panic!("probing {}: {error}", std::any::type_name::<T>());
    }
    shape
}

type ProbeError = de::value::Error;

/// Drives a derived `Deserialize` impl without input. Every scalar gets a
/// placeholder the configuration's field types accept: integers for the
/// untagged byte-size and duration forms, a URL for strings.
struct Probe<'a> {
    shape: &'a mut Shape,
}

impl<'de> Deserializer<'de> for Probe<'_> {
    type Error = ProbeError;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ProbeError> {
        visitor.visit_u64(0)
    }

    fn deserialize_bool<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ProbeError> {
        visitor.visit_bool(false)
    }

    fn deserialize_i8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ProbeError> {
        visitor.visit_i64(0)
    }

    fn deserialize_i16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ProbeError> {
        visitor.visit_i64(0)
    }

    fn deserialize_i32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ProbeError> {
        visitor.visit_i64(0)
    }

    fn deserialize_i64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ProbeError> {
        visitor.visit_i64(0)
    }

    fn deserialize_u8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ProbeError> {
        visitor.visit_u64(0)
    }

    fn deserialize_u16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ProbeError> {
        visitor.visit_u64(0)
    }

    fn deserialize_u32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ProbeError> {
        visitor.visit_u64(0)
    }

    fn deserialize_u64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ProbeError> {
        visitor.visit_u64(0)
    }

    fn deserialize_f32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ProbeError> {
        visitor.visit_f64(1.0)
    }

    fn deserialize_f64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ProbeError> {
        visitor.visit_f64(1.0)
    }

    fn deserialize_char<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ProbeError> {
        visitor.visit_char('x')
    }

    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ProbeError> {
        visitor.visit_str("https://probe.invalid/")
    }

    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ProbeError> {
        visitor.visit_str("https://probe.invalid/")
    }

    fn deserialize_bytes<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ProbeError> {
        visitor.visit_bytes(&[])
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ProbeError> {
        visitor.visit_bytes(&[])
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ProbeError> {
        visitor.visit_some(self)
    }

    fn deserialize_unit<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ProbeError> {
        visitor.visit_unit()
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, ProbeError> {
        visitor.visit_unit()
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, ProbeError> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ProbeError> {
        let mut item = [Shape::Leaf];
        let value = visitor.visit_seq(Items {
            shapes: &mut item,
            next: 0,
        })?;
        let [item] = item;
        *self.shape = Shape::Array(Box::new(item));
        Ok(value)
    }

    fn deserialize_tuple<V: Visitor<'de>>(
        self,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, ProbeError> {
        let mut shapes = (0..len).map(|_| Shape::Leaf).collect::<Vec<_>>();
        visitor.visit_seq(Items {
            shapes: &mut shapes,
            next: 0,
        })
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, ProbeError> {
        self.deserialize_tuple(len, visitor)
    }

    fn deserialize_map<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ProbeError> {
        *self.shape = Shape::Map;
        visitor.visit_map(Fields {
            fields: &mut [],
            next: 0,
        })
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, ProbeError> {
        let mut children = fields
            .iter()
            .map(|field| (*field, Shape::Leaf))
            .collect::<Vec<_>>();
        let value = visitor.visit_map(Fields {
            fields: &mut children,
            next: 0,
        })?;
        *self.shape = Shape::Table(children);
        Ok(value)
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, ProbeError> {
        *self.shape = Shape::Enum(name, variants);
        visitor.visit_enum(Variant(variants.first().copied().unwrap_or_default()))
    }

    fn deserialize_identifier<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ProbeError> {
        visitor.visit_str("")
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ProbeError> {
        visitor.visit_unit()
    }

    /// Socket addresses then decode as an enum of integer tuples instead of
    /// text that would have to parse.
    fn is_human_readable(&self) -> bool {
        false
    }
}

/// Every declared field once, each value probed into its own shape.
struct Fields<'a> {
    fields: &'a mut [(&'static str, Shape)],
    next: usize,
}

impl<'de> MapAccess<'de> for Fields<'_> {
    type Error = ProbeError;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, ProbeError> {
        self.fields
            .get(self.next)
            .map(|(name, _)| seed.deserialize((*name).into_deserializer()))
            .transpose()
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(
        &mut self,
        seed: V,
    ) -> Result<V::Value, ProbeError> {
        let (_, shape) = self
            .fields
            .get_mut(self.next)
            .ok_or_else(|| de::Error::custom("a value without its key"))?;
        self.next += 1;
        seed.deserialize(Probe { shape })
    }
}

/// One element per shape: an array's item, or each member of a tuple.
struct Items<'a> {
    shapes: &'a mut [Shape],
    next: usize,
}

impl<'de> SeqAccess<'de> for Items<'_> {
    type Error = ProbeError;

    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, ProbeError> {
        let Some(shape) = self.shapes.get_mut(self.next) else {
            return Ok(None);
        };
        self.next += 1;
        seed.deserialize(Probe { shape }).map(Some)
    }
}

/// An enum's first variant, whatever its form.
struct Variant(&'static str);

impl<'de> EnumAccess<'de> for Variant {
    type Error = ProbeError;
    type Variant = Self;

    fn variant_seed<V: DeserializeSeed<'de>>(
        self,
        seed: V,
    ) -> Result<(V::Value, Self), ProbeError> {
        seed.deserialize(self.0.into_deserializer())
            .map(|value| (value, self))
    }
}

impl<'de> VariantAccess<'de> for Variant {
    type Error = ProbeError;

    fn unit_variant(self) -> Result<(), ProbeError> {
        Ok(())
    }

    fn newtype_variant_seed<T: DeserializeSeed<'de>>(
        self,
        seed: T,
    ) -> Result<T::Value, ProbeError> {
        seed.deserialize(Probe {
            shape: &mut Shape::Leaf,
        })
    }

    fn tuple_variant<V: Visitor<'de>>(
        self,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, ProbeError> {
        Probe {
            shape: &mut Shape::Leaf,
        }
        .deserialize_tuple(len, visitor)
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, ProbeError> {
        Probe {
            shape: &mut Shape::Leaf,
        }
        .deserialize_struct(self.0, fields, visitor)
    }
}

/// `config/schema-v1.json`, with its `$ref`s followed.
struct Schema(Value);

impl Schema {
    fn load(repository: &Path) -> Self {
        let path = repository.join("config/schema-v1.json");
        let bytes = fs::read(&path).unwrap_or_else(|error| panic!("{}: {error}", path.display()));
        Self(serde_json::from_slice(&bytes).expect("configuration schema is JSON"))
    }

    fn definition(&self, name: &str) -> &Value {
        &self.0["$defs"][name]
    }

    fn resolve<'a>(&'a self, mut node: &'a Value) -> &'a Value {
        while let Some(reference) = node.get("$ref").and_then(Value::as_str) {
            let name = reference
                .strip_prefix("#/$defs/")
                .unwrap_or_else(|| panic!("unsupported schema reference {reference}"));
            node = self.definition(name);
            assert!(!node.is_null(), "missing schema definition {reference}");
        }
        node
    }
}

fn member(path: &str, name: &str) -> String {
    if path.is_empty() {
        name.to_owned()
    } else {
        format!("{path}.{name}")
    }
}

fn refused(item: &str) -> bool {
    REFUSED_BY_VALIDATION.contains(&item)
}

/// Every difference between what `shape` decodes and what `node` declares.
fn compare(
    schema: &Schema,
    node: &Value,
    shape: &Shape,
    path: &str,
    differences: &mut Vec<String>,
) {
    let node = schema.resolve(node);
    match shape {
        Shape::Table(fields) => {
            let empty = serde_json::Map::new();
            let properties = match node.get("properties").and_then(Value::as_object) {
                Some(properties) => properties,
                // An empty table, such as the compact `[blocks]`.
                None if node.get("type") == Some(&Value::from("object")) => &empty,
                None => {
                    differences.push(format!("{path}: the schema declares no table"));
                    return;
                }
            };
            for (name, child) in fields {
                let field = member(path, name);
                match (properties.get(*name), refused(&field)) {
                    (Some(_), true) => differences.push(format!(
                        "{field}: declared by the schema, but validation refuses it"
                    )),
                    (Some(property), false) => {
                        compare(schema, property, child, &field, differences);
                    }
                    (None, true) => {}
                    (None, false) => differences.push(format!("{field}: missing from the schema")),
                }
            }
            for name in properties.keys() {
                if !fields.iter().any(|(field, _)| field == name) {
                    differences.push(format!(
                        "{}: declared by the schema, but not a configuration field",
                        member(path, name)
                    ));
                }
            }
        }
        Shape::Array(item) => match node.get("items") {
            Some(items) => compare(schema, items, item, &format!("{path}[]"), differences),
            None => differences.push(format!("{path}: the schema declares no array items")),
        },
        Shape::Enum(name, variants) => {
            let declared = node
                .get("enum")
                .and_then(Value::as_array)
                .map(|values| {
                    values
                        .iter()
                        .filter_map(Value::as_str)
                        .collect::<BTreeSet<_>>()
                })
                .or_else(|| {
                    node.get("const")
                        .and_then(Value::as_str)
                        .map(|value| BTreeSet::from([value]))
                });
            let Some(declared) = declared else {
                // A socket address probes as an enum; the schema describes
                // its text form.
                if *name != "SocketAddr" {
                    differences.push(format!("{path}: the schema declares no values for {name}"));
                }
                return;
            };
            for variant in *variants {
                let value = format!("{path}={variant}");
                match (declared.contains(variant), refused(&value)) {
                    (true, true) => differences.push(format!(
                        "{path}: offers `{variant}`, which validation refuses"
                    )),
                    (false, false) => {
                        differences.push(format!("{path}: `{variant}` is missing from the schema"));
                    }
                    _ => {}
                }
            }
            for value in declared {
                if !variants.contains(&value) {
                    differences.push(format!("{path}: offers `{value}`, which does not decode"));
                }
            }
        }
        Shape::Map if FREE_FORM_MAPS.contains(&path) => {
            if node.get("additionalProperties") != Some(&Value::Bool(true)) {
                differences.push(format!("{path}: the schema does not leave it open"));
            }
        }
        Shape::Map => differences.push(format!(
            "{path}: a table of arbitrary keys, which the schema cannot check field by field"
        )),
        Shape::Leaf => {}
    }
}

/// Every TOML file under `directory` that is a node configuration: one that
/// states its `config_version`, which both configuration shapes require.
/// Node state and build output are not shipped files.
fn node_configurations(directory: &Path, found: &mut Vec<PathBuf>) {
    let entries =
        fs::read_dir(directory).unwrap_or_else(|error| panic!("{}: {error}", directory.display()));
    for entry in entries {
        let path = entry.expect("directory entry").path();
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default();
        if path.is_dir() {
            if !matches!(name, "data" | "node_modules" | "target") {
                node_configurations(&path, found);
            }
        } else if path
            .extension()
            .is_some_and(|extension| extension == "toml")
        {
            let source = fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("{}: {error}", path.display()));
            if toml::from_str::<toml::Table>(&source)
                .unwrap_or_else(|error| panic!("{}: {error}", path.display()))
                .contains_key("config_version")
            {
                found.push(path);
            }
        }
    }
}

fn repository() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

#[test]
fn the_schema_declares_exactly_the_configuration_fields() {
    // Audit Config-1: the schema lacked `processors[].artifacts`, the Xatu
    // tuning fields, and more, while refusing unknown properties, so the
    // shipped example failed it.
    let schema = Schema::load(&repository());
    let mut differences = Vec::new();
    compare(
        &schema,
        schema.definition("advancedConfig"),
        &shape_of::<Config>(),
        "",
        &mut differences,
    );
    compare(
        &schema,
        schema.definition("starterConfig"),
        &shape_of::<StarterConfig>(),
        "",
        &mut differences,
    );
    assert!(
        differences.is_empty(),
        "config/schema-v1.json has drifted from the configuration structs:\n{}",
        differences.join("\n")
    );
}

/// The shape one step below `shape`: a table's field, or `[]` for an array's
/// item.
fn child<'a>(shape: &'a Shape, step: &str) -> Option<&'a Shape> {
    match (shape, step) {
        (Shape::Array(item), "[]") => Some(item),
        (Shape::Table(fields), name) => fields
            .iter()
            .find(|(field, _)| *field == name)
            .map(|(_, shape)| shape),
        _ => None,
    }
}

#[test]
fn the_probe_reaches_nested_tables_and_enum_values() {
    let config = shape_of::<Config>();
    let at = |steps: &[&str]| {
        steps
            .iter()
            .try_fold(&config, |shape, step| child(shape, step))
    };
    assert!(matches!(
        at(&["processors", "[]", "artifacts", "window"]),
        Some(Shape::Table(fields)) if fields.iter().any(|(name, _)| *name == "max_bytes")
    ));
    assert!(matches!(
        at(&["sources", "history", "[]", "kind"]),
        Some(Shape::Enum(_, variants)) if variants.contains(&"era_e")
    ));
    assert!(matches!(
        at(&["rpc", "http_bind"]),
        Some(Shape::Enum("SocketAddr", _))
    ));
    assert!(matches!(
        at(&["processors", "[]", "settings"]),
        Some(Shape::Map)
    ));
}

#[test]
fn a_field_the_schema_lacks_is_reported() {
    let mut schema = Schema::load(&repository());
    schema.0["$defs"]["rpc"]["properties"]
        .as_object_mut()
        .expect("rpc properties")
        .remove("max_log_results");
    let mut differences = Vec::new();
    compare(
        &schema,
        schema.definition("advancedConfig"),
        &shape_of::<Config>(),
        "",
        &mut differences,
    );
    assert!(
        differences.contains(&"rpc.max_log_results: missing from the schema".to_owned()),
        "{differences:?}"
    );
}

#[test]
fn a_table_of_arbitrary_keys_is_reported_unless_it_is_free_form() {
    // Review of Task 19: a map-valued field, or a future `flatten`, decodes
    // through a map, which the schema cannot check field by field.
    let schema = Schema::load(&repository());
    let open = serde_json::json!({ "type": "object", "additionalProperties": true });
    let mut differences = Vec::new();
    compare(
        &schema,
        &open,
        &Shape::Map,
        "processors[].settings",
        &mut differences,
    );
    assert!(differences.is_empty(), "{differences:?}");
    compare(&schema, &open, &Shape::Map, "rpc.extra", &mut differences);
    assert_eq!(differences.len(), 1, "{differences:?}");
}

#[test]
fn the_shipped_file_walk_skips_state_and_build_directories() {
    // Review of Task 19: state beside a profile run in place is not a
    // shipped configuration.
    let temp = tempfile::tempdir().expect("temporary directory");
    for directory in ["data", "target", "node_modules", "profiles"] {
        let directory = temp.path().join(directory);
        fs::create_dir_all(&directory).expect("directory");
        fs::write(directory.join("node.toml"), "config_version = 1\n").expect("configuration");
    }
    let mut found = Vec::new();
    node_configurations(temp.path(), &mut found);
    assert_eq!(found, [temp.path().join("profiles/node.toml")]);
}

/// Violations of one top-level branch. A failed `oneOf` only reports that no
/// branch matched, which hides the offending member.
fn branch_errors(schema: &Value, branch: &str, value: &Value) -> Vec<String> {
    let mut branch_schema = schema.clone();
    let root = branch_schema.as_object_mut().expect("schema object");
    root.remove("oneOf");
    root.insert("$ref".to_owned(), format!("#/$defs/{branch}").into());
    jsonschema::options()
        .should_validate_formats(true)
        .build(&branch_schema)
        .expect("branch schema")
        .iter_errors(value)
        .map(|error| format!("as {branch}, {}: {error}", error.instance_path()))
        .collect()
}

#[test]
fn compact_configurations_validate_against_json_schema() {
    // No shipped file uses the compact shape; `leani init` writes it.
    let schema = Schema::load(&repository());
    let validator = jsonschema::options()
        .should_validate_formats(true)
        .build(&schema.0)
        .expect("valid configuration schema");
    let checkpoint = format!("0x{}", "11".repeat(32));
    let endpoints = vec![url::Url::parse("https://beacon.example/").expect("URL")];
    for starter in [
        StarterConfig::blocks(
            PathBuf::from("./data"),
            checkpoint.clone(),
            15_000_000,
            endpoints.clone(),
        ),
        StarterConfig::uniswap(
            PathBuf::from("./data"),
            vec!["ETH/USDC".to_owned()],
            checkpoint.clone(),
            15_000_000,
            endpoints.clone(),
        ),
    ] {
        let encoded = toml::to_string_pretty(&starter).expect("compact TOML");
        let document = toml::from_str::<toml::Value>(&encoded).expect("compact document");
        let value = serde_json::to_value(&document).expect("JSON configuration");
        let errors = branch_errors(&schema.0, "starterConfig", &value);
        assert!(
            validator.is_valid(&value) && errors.is_empty(),
            "{encoded}\n{}",
            errors.join("\n")
        );
    }
}

#[test]
fn shipped_configurations_load_and_validate_against_json_schema() {
    // Audit Config-1: nothing checked the shipped files against the schema,
    // and the example used properties it refused.
    let repository = repository();
    let schema = Schema::load(&repository);
    let mut configurations = Vec::new();
    for directory in ["config", "deploy", "examples"] {
        node_configurations(&repository.join(directory), &mut configurations);
    }
    configurations.sort();
    let relative = configurations
        .iter()
        .map(|path| {
            path.strip_prefix(&repository)
                .expect("inside the repository")
                .to_string_lossy()
                .into_owned()
        })
        .collect::<Vec<_>>();
    for expected in [
        "config/example.toml",
        "config/defaults/ethereum-mainnet.toml",
        "config/modes/windowed.toml",
        "deploy/container.toml",
        "examples/weth-transfers/node.toml",
    ] {
        assert!(
            relative.iter().any(|path| path == expected),
            "{expected} was not discovered: {relative:?}"
        );
    }
    // Templates whose checkpoint the operator, or compact expansion, supplies.
    let templates = [
        "config/example.toml",
        "config/defaults/ethereum-mainnet.toml",
    ];
    let mut problems = Vec::new();
    for (path, name) in configurations.iter().zip(&relative) {
        match Config::load(path) {
            Ok(config) if !templates.contains(&name.as_str()) => {
                if let Err(errors) = config.validate() {
                    problems.push(format!("{name}: {errors}"));
                }
            }
            Ok(_) => {}
            Err(error) => problems.push(format!("{name}: {error}")),
        }
        let source = fs::read_to_string(path).expect("configuration");
        let document = toml::from_str::<toml::Value>(&source).expect("configuration TOML");
        let mut schema_value = schema.0.clone();
        if name == "config/defaults/ethereum-mainnet.toml" {
            // This is the sole incomplete template: compact expansion supplies
            // processors. Relax only that required member, not its schema.
            schema_value["$defs"]["advancedConfig"]["required"]
                .as_array_mut()
                .unwrap()
                .retain(|field| field != "processors");
        }
        let validator = jsonschema::options()
            .should_validate_formats(true)
            .build(&schema_value)
            .expect("valid configuration schema");
        let value = serde_json::to_value(&document).expect("JSON configuration");
        let mut found = validator
            .iter_errors(&value)
            .map(|error| format!("{}: {error}", error.instance_path()))
            .collect::<Vec<_>>();
        if !found.is_empty() {
            found.extend(branch_errors(&schema_value, "advancedConfig", &value));
        }
        problems.extend(
            found
                .into_iter()
                .map(|violation| format!("{name}: {violation}")),
        );
    }
    assert!(
        problems.is_empty(),
        "shipped configurations disagree with the schema:\n{}",
        problems.join("\n")
    );
}

#[test]
fn schema_validation_checks_scalar_constraints_and_exactly_one_branch() {
    use serde_json::json;
    let schema = Schema::load(&repository());
    let mut source: Value = serde_json::to_value(
        toml::from_str::<toml::Value>(
            &fs::read_to_string(repository().join("config/example.toml")).unwrap(),
        )
        .unwrap(),
    )
    .unwrap();
    let validator = jsonschema::validator_for(&schema.0).unwrap();
    assert!(validator.is_valid(&source));
    for (fill, valid) in [
        (json!({ "max_blocks": 1 }), true),
        (json!({ "max_blocks": 7_200 }), true),
        (json!({ "max_blocks": 0 }), false),
        (json!({ "max_blocks": 7_201 }), false),
        (json!({}), false),
    ] {
        source["processors"][0]["live_gap_fill"] = fill.clone();
        assert_eq!(validator.is_valid(&source), valid, "{fill}");
    }
    source["processors"][0]
        .as_object_mut()
        .unwrap()
        .remove("live_gap_fill");
    for invalid in [json!("many"), json!(0), json!(-1)] {
        source["rpc"]["max_websocket_connections"] = invalid;
        assert!(!validator.is_valid(&source));
    }
    let exact =
        jsonschema::validator_for(&json!({"oneOf": [{"type": "integer"}, {"type": "number"}]}))
            .unwrap();
    assert!(!exact.is_valid(&json!(1)), "two matches must fail oneOf");
    let constraints = jsonschema::validator_for(&json!({"type": "array", "minItems": 1, "uniqueItems": true, "items": {"type": "string", "pattern": "^0x[0-9a-f]{2}$"}})).unwrap();
    for invalid in [json!([]), json!(["bad"]), json!(["0x12", "0x12"])] {
        assert!(!constraints.is_valid(&invalid));
    }
}
