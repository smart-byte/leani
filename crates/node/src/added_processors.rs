//! Processors added to a running node through its API, kept beside its store
//! in `processors.toml`, in the configuration's `[[processors]]` schema, with
//! their Wasm modules in `modules/`, named by hash.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::config::{ConfigError, ProcessorConfig};

pub(crate) const FILE: &str = "processors.toml";
const MODULES: &str = "modules";
const HEADER: &str = "# Processors added through the node's API, POST /admin/v1/processors.\n# leani.toml wins for an instance it configures too.\n\n";

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Document {
    #[serde(default)]
    processors: Vec<ProcessorConfig>,
}

#[derive(Debug)]
pub(crate) struct StoredModule {
    pub(crate) path: PathBuf,
    /// Its BLAKE3 hash, `0x`-prefixed.
    pub(crate) hash: String,
    /// Whether this call wrote it, rather than an earlier one.
    pub(crate) created: bool,
}

/// Keep `bytes` as `modules/<blake3>.wasm`, written once.
pub(crate) fn store_module(data_dir: &Path, bytes: &[u8]) -> std::io::Result<StoredModule> {
    let hash = blake3::hash(bytes).to_hex().to_string();
    let path = data_dir.join(MODULES).join(format!("{hash}.wasm"));
    let created = !path.is_file();
    if created {
        replace(&path, bytes)?;
    }
    Ok(StoredModule {
        path,
        hash: format!("0x{hash}"),
        created,
    })
}

/// The added processors, none without the file. Module paths, relative to
/// the file on disk, come back absolute.
pub(crate) fn read(data_dir: &Path) -> Result<Vec<ProcessorConfig>, ConfigError> {
    let path = data_dir.join(FILE);
    let input = match std::fs::read_to_string(&path) {
        Ok(input) => input,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(source) => return Err(ConfigError::Read { path, source }),
    };
    let mut document: Document =
        toml::from_str(&input).map_err(|source| ConfigError::Parse { path, source })?;
    for module in modules(&mut document.processors) {
        *module = data_dir
            .join(module.as_str())
            .to_string_lossy()
            .into_owned();
    }
    Ok(document.processors)
}

/// Replace the file with `processors`, module paths relative to it.
pub(crate) fn write(data_dir: &Path, processors: &[ProcessorConfig]) -> std::io::Result<()> {
    let mut document = Document {
        processors: processors.to_vec(),
    };
    for module in modules(&mut document.processors) {
        if let Ok(relative) = Path::new(module.as_str()).strip_prefix(data_dir) {
            *module = relative.to_string_lossy().into_owned();
        }
    }
    let text = toml::to_string(&document).map_err(std::io::Error::other)?;
    replace(&data_dir.join(FILE), format!("{HEADER}{text}").as_bytes())
}

fn modules(processors: &mut [ProcessorConfig]) -> impl Iterator<Item = &mut String> {
    processors
        .iter_mut()
        .filter(|processor| processor.id == leani_processor_wasm::PROCESSOR_ID)
        .filter_map(|processor| match processor.settings.get_mut("module") {
            Some(toml::Value::String(module)) => Some(module),
            _ => None,
        })
}

/// Write through a temporary file, so a reader never sees half of it.
fn replace(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let temporary = path.with_extension("tmp");
    std::fs::write(&temporary, bytes)?;
    std::fs::rename(&temporary, path)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::{builtin_processors::processor_contract, config::PublishMode};

    /// A Wasm processor using `module`, and a native one.
    pub(crate) fn entries(module: &Path) -> Vec<ProcessorConfig> {
        let mut wasm = processor_contract(
            leani_processor_wasm::PROCESSOR_ID,
            "gas-tracker",
            "1.0.0",
            21_000_000,
            PublishMode::Finalized,
        );
        wasm.settings.insert(
            "module".to_owned(),
            toml::Value::String(module.to_string_lossy().into_owned()),
        );
        let native = processor_contract(
            "block-summary",
            "summary",
            "1.0.0",
            21_000_000,
            PublishMode::Finalized,
        );
        vec![wasm, native]
    }

    #[test]
    fn no_file_means_no_added_processors() {
        let data = tempfile::tempdir().expect("data directory");
        assert!(read(data.path()).expect("read").is_empty());
    }

    #[test]
    fn a_module_is_stored_once_under_its_hash() {
        let data = tempfile::tempdir().expect("data directory");
        let stored = store_module(data.path(), b"\0asm one").expect("stored");
        let expected = blake3::hash(b"\0asm one").to_hex().to_string();
        assert_eq!(stored.hash, format!("0x{expected}"));
        assert_eq!(
            stored.path,
            data.path().join(format!("modules/{expected}.wasm"))
        );
        assert!(stored.created);
        assert_eq!(std::fs::read(&stored.path).expect("module"), b"\0asm one");
        let again = store_module(data.path(), b"\0asm one").expect("again");
        assert_eq!(again.path, stored.path);
        assert!(!again.created);
        assert_ne!(
            store_module(data.path(), b"\0asm two").expect("other").path,
            stored.path
        );
    }

    #[test]
    fn module_paths_are_relative_on_disk_and_absolute_when_read() {
        let data = tempfile::tempdir().expect("data directory");
        let module = store_module(data.path(), b"\0asm").expect("stored").path;
        let written = entries(&module);
        write(data.path(), &written).expect("written");
        let text = std::fs::read_to_string(data.path().join(FILE)).expect("file");
        assert!(text.contains("module = \"modules/"), "{text}");
        assert!(!text.contains(&*data.path().to_string_lossy()), "{text}");
        let read = read(data.path()).expect("read");
        assert_eq!(read.len(), 2);
        assert_eq!(read[0].instance, "gas-tracker");
        assert_eq!(
            read[0].settings["module"].as_str(),
            Some(&*module.to_string_lossy())
        );
        assert_eq!(read[1].instance, "summary");
    }

    #[test]
    fn a_malformed_file_is_refused_with_its_path() {
        let data = tempfile::tempdir().expect("data directory");
        std::fs::write(data.path().join(FILE), "[[processors]]\nid = 1\n").expect("file");
        let refused = read(data.path()).expect_err("malformed");
        assert!(
            matches!(&refused, ConfigError::Parse { path, .. } if path.ends_with(FILE)),
            "{refused:?}"
        );
    }
}
