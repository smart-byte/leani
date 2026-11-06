//! Processors added to and removed from a running node through its API. A
//! change validates the processor as the configuration file would, keeps it
//! in the data directory, and publishes the new processor set, which
//! restarts the network or JSON-RPC lanes in-process.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use async_trait::async_trait;
use leani_api::{ProcessorAdminError, ProcessorChange};
use leani_processor_api::{ArtifactPolicyMode, Processor, StatePolicyMode};
use leani_store_sqlite::SqliteStore;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use super::{
    ProcessorSet, bootstrap_processor, maintained_processors, supervise_artifact_compaction,
    supervise_processor_maintenance,
};
use crate::{
    added_processors,
    builtin_processors::processor_contract,
    config::{
        ArtifactStorageBackend, Config, ProcessorConfig, ProcessorHistoryControl,
        ProcessorHistoryMode, PublishMode,
    },
    processors::ProcessorRegistry,
};

pub(super) struct NodeProcessorAdmin {
    registry: ProcessorRegistry,
    store: SqliteStore,
    sets: tokio::sync::watch::Sender<Arc<ProcessorSet>>,
    /// One change at a time, from reading the set to publishing the next.
    changes: tokio::sync::Mutex<()>,
    /// Ends an added processor's maintenance when it is removed.
    maintenance: std::sync::Mutex<BTreeMap<String, CancellationToken>>,
    cancellation: CancellationToken,
}

impl NodeProcessorAdmin {
    pub(super) fn new(
        registry: ProcessorRegistry,
        store: SqliteStore,
        sets: tokio::sync::watch::Sender<Arc<ProcessorSet>>,
        cancellation: CancellationToken,
    ) -> Self {
        Self {
            registry,
            store,
            sets,
            changes: tokio::sync::Mutex::default(),
            maintenance: std::sync::Mutex::default(),
            cancellation,
        }
    }

    async fn admit(
        &self,
        current: &ProcessorSet,
        entry: ProcessorConfig,
    ) -> Result<ProcessorChange, ProcessorAdminError> {
        let mut config = current.config.clone();
        config.processors.push(entry.clone());
        let config = config
            .validate()
            .map_err(|errors| ProcessorAdminError::Invalid(errors.to_string()))?
            .into_inner();
        let components = self
            .registry
            .instantiate_components(&entry, config.chain.chain_id)
            .map_err(|error| ProcessorAdminError::Invalid(format!("{error:#}")))?;
        if components.query_extension.is_some() {
            return Err(ProcessorAdminError::Invalid(format!(
                "{} adds query routes, which the node mounts only as it starts: configure it in the configuration file",
                entry.id
            )));
        }
        let processor = components.processor;
        let descriptor = processor.descriptor();
        // The live lane refuses two of one kind, version, and parameters.
        if let Some(twin) = current.processors.iter().find(|running| {
            let running = running.descriptor();
            running.id == descriptor.id
                && running.version == descriptor.version
                && running.config_hash == descriptor.config_hash
        }) {
            return Err(ProcessorAdminError::Conflict(format!(
                "{} runs the same kind, version, and parameters: give this one another version or other parameters",
                twin.descriptor().instance
            )));
        }
        bootstrap_processor(&self.store, processor.as_ref())
            .await
            .map_err(internal)?;
        let mut added = added_processors::read(&config.data_dir).map_err(internal)?;
        added.push(entry.clone());
        added_processors::write(&config.data_dir, &added).map_err(internal)?;
        let mut processors = current.processors.clone();
        processors.push(processor.clone());
        let change = change(&config, &processors, entry.instance.clone());
        self.start_maintenance(&config, &entry, &processor);
        self.sets
            .send_replace(Arc::new(ProcessorSet { config, processors }));
        Ok(change)
    }

    /// Prune and compact an added processor as the node does the ones it
    /// started with.
    fn start_maintenance(
        &self,
        config: &Config,
        entry: &ProcessorConfig,
        processor: &Arc<dyn Processor>,
    ) {
        let cancellation = self.cancellation.child_token();
        let mut own = config.clone();
        own.processors = vec![entry.clone()];
        for (processor, verification_segment_blocks) in
            maintained_processors(&own, std::slice::from_ref(processor))
        {
            tokio::spawn(supervise_processor_maintenance(
                self.store.clone(),
                processor,
                verification_segment_blocks,
                cancellation.clone(),
            ));
        }
        if config.artifact_storage.backend == ArtifactStorageBackend::TieredSegments
            && processor.descriptor().lifecycle.artifacts.mode == ArtifactPolicyMode::Full
        {
            tokio::spawn(supervise_artifact_compaction(
                self.store.clone(),
                vec![processor.clone()],
                config.artifact_storage,
                cancellation.clone(),
            ));
        }
        self.maintenance
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(entry.instance.clone(), cancellation);
    }
}

#[async_trait]
impl leani_api::ProcessorAdmin for NodeProcessorAdmin {
    async fn add(
        &self,
        processor: Value,
        module: Option<Vec<u8>>,
    ) -> Result<ProcessorChange, ProcessorAdminError> {
        let _turn = self.changes.lock().await;
        let current = self.sets.borrow().clone();
        let mut entry = entry(processor)?;
        if current
            .config
            .processors
            .iter()
            .any(|configured| configured.instance == entry.instance)
        {
            return Err(ProcessorAdminError::Conflict(format!(
                "processor {} exists already",
                entry.instance
            )));
        }
        if entry.settings.contains_key("module") {
            return Err(ProcessorAdminError::Invalid(
                "send the module's bytes as module, or name a template: the node keeps the file"
                    .to_owned(),
            ));
        }
        let stored = match (entry.id == leani_processor_wasm::PROCESSOR_ID, module) {
            (true, Some(bytes)) => {
                let stored = added_processors::store_module(&current.config.data_dir, &bytes)
                    .map_err(internal)?;
                entry.settings.insert(
                    "module".to_owned(),
                    toml::Value::String(stored.path.to_string_lossy().into_owned()),
                );
                entry.settings.insert(
                    "blake3".to_owned(),
                    toml::Value::String(stored.hash.clone()),
                );
                Some(stored)
            }
            (true, None) => {
                return Err(ProcessorAdminError::Invalid(
                    "a wasm processor needs its module: send module or template".to_owned(),
                ));
            }
            (false, Some(_)) => {
                return Err(ProcessorAdminError::Invalid(format!(
                    "{} takes no module; only wasm processors do",
                    entry.id
                )));
            }
            (false, None) => None,
        };
        let admitted = self.admit(&current, entry).await;
        if admitted.is_err()
            && let Some(stored) = stored.filter(|stored| stored.created)
        {
            let _ = std::fs::remove_file(stored.path);
        }
        admitted
    }

    async fn remove(&self, instance: &str) -> Result<ProcessorChange, ProcessorAdminError> {
        let _turn = self.changes.lock().await;
        let current = self.sets.borrow().clone();
        let mut added = added_processors::read(&current.config.data_dir).map_err(internal)?;
        let running = current
            .config
            .processors
            .iter()
            .position(|configured| configured.instance == instance);
        let kept = added.iter().position(|entry| entry.instance == instance);
        let (index, kept) = match (running, kept) {
            (Some(index), Some(kept))
                if same_entry(&current.config.processors[index], &added[kept]) =>
            {
                (index, kept)
            }
            (Some(_), _) => {
                return Err(ProcessorAdminError::Conflict(format!(
                    "the configuration file configures processor {instance}: remove it there"
                )));
            }
            (None, _) => {
                return Err(ProcessorAdminError::NotFound(format!(
                    "processor {instance} was not added through the API"
                )));
            }
        };
        added.remove(kept);
        added_processors::write(&current.config.data_dir, &added).map_err(internal)?;
        let mut config = current.config.clone();
        config.processors.remove(index);
        let mut processors = current.processors.clone();
        processors.remove(index);
        if let Some(maintenance) = self
            .maintenance
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(instance)
        {
            maintenance.cancel();
        }
        let change = change(&config, &processors, instance.to_owned());
        self.sets
            .send_replace(Arc::new(ProcessorSet { config, processors }));
        Ok(change)
    }
}

/// The configuration entry for `processor`, which needs only its
/// `instance`, `start_block`, and settings: the rest defaults as the
/// playground's configuration snippet does, `id = "wasm"`, version 1.0.0,
/// on-demand history, unattested publication, durable state.
fn entry(processor: Value) -> Result<ProcessorConfig, ProcessorAdminError> {
    let invalid = |message: &str| ProcessorAdminError::Invalid(message.to_owned());
    let Value::Object(fields) = processor else {
        return Err(invalid("processor must be a configuration entry object"));
    };
    let instance = fields
        .get("instance")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("processor.instance is required"))?;
    let start_block = fields
        .get("start_block")
        .and_then(Value::as_u64)
        .ok_or_else(|| invalid("processor.start_block is required, a block number"))?;
    let publish = fields
        .get("publish")
        .map(|publish| serde_json::from_value::<PublishMode>(publish.clone()))
        .transpose()
        .map_err(|error| ProcessorAdminError::Invalid(format!("processor.publish: {error}")))?
        .unwrap_or(PublishMode::Unattested);
    let mut defaults = processor_contract(
        fields
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or(leani_processor_wasm::PROCESSOR_ID),
        instance,
        "1.0.0",
        start_block,
        publish,
    );
    defaults.state.mode = StatePolicyMode::Durable;
    let mut merged = serde_json::to_value(&defaults).map_err(internal)?;
    merge(&mut merged, Value::Object(fields));
    serde_json::from_value(merged)
        .map_err(|error| ProcessorAdminError::Invalid(format!("processor: {error}")))
}

fn merge(base: &mut Value, overlay: Value) {
    match (base, overlay) {
        (Value::Object(base), Value::Object(overlay)) => {
            for (key, value) in overlay {
                merge(base.entry(key).or_insert(Value::Null), value);
            }
        }
        (base, overlay) => *base = overlay,
    }
}

/// Whether the running entry is the one `processors.toml` holds, not one
/// the configuration file sets for the same instance.
fn same_entry(running: &ProcessorConfig, kept: &ProcessorConfig) -> bool {
    serde_json::to_value(running).ok() == serde_json::to_value(kept).ok()
}

fn change(config: &Config, processors: &[Arc<dyn Processor>], instance: String) -> ProcessorChange {
    let instances = |keep: fn(&ProcessorConfig) -> bool| {
        config
            .processors
            .iter()
            .filter(|configured| keep(configured))
            .map(|configured| configured.instance.clone())
            .collect::<BTreeSet<_>>()
    };
    ProcessorChange {
        instance,
        processors: processors.to_vec(),
        on_demand: instances(|configured| {
            configured.history_mode == ProcessorHistoryMode::OnDemand
        }),
        subscriptions: instances(|configured| {
            configured.history_control == ProcessorHistoryControl::ApplicationSubscriptions
        }),
    }
}

fn internal(error: impl std::fmt::Display) -> ProcessorAdminError {
    ProcessorAdminError::Internal(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, path::PathBuf};

    use leani_api::{ProcessorAdmin as _, ProcessorAdminError};
    use leani_store_sqlite::{SqliteStore, StoreConfig};
    use serde_json::{Value, json};

    use super::*;
    use crate::{builtin_processors::processor_contract, config::PublishMode};

    struct Node {
        _data: tempfile::TempDir,
        data_dir: PathBuf,
        admin: NodeProcessorAdmin,
        sets: tokio::sync::watch::Receiver<Arc<ProcessorSet>>,
        store: SqliteStore,
    }

    async fn node(configured: &[ProcessorConfig]) -> Node {
        let data = tempfile::tempdir().expect("data directory");
        let store = SqliteStore::open(StoreConfig::new(data.path().join("leani.sqlite")))
            .await
            .expect("store");
        let registry = ProcessorRegistry::standard();
        let mut config: Config =
            toml::from_str(crate::config::VALID_CONFIG_TOML).expect("configuration fixture");
        config.data_dir = data.path().to_path_buf();
        config.processors = configured.to_vec();
        let processors = configured
            .iter()
            .map(|entry| {
                registry
                    .instantiate(entry, config.chain.chain_id)
                    .expect("instantiated")
            })
            .collect();
        let (sender, sets) =
            tokio::sync::watch::channel(Arc::new(ProcessorSet { config, processors }));
        Node {
            data_dir: data.path().to_path_buf(),
            _data: data,
            admin: NodeProcessorAdmin::new(
                registry,
                store.clone(),
                sender,
                CancellationToken::new(),
            ),
            sets,
            store,
        }
    }

    fn block_gas() -> (Vec<u8>, Value) {
        let template = leani_processor_wasm::TEMPLATES
            .iter()
            .find(|template| template.name == "block-gas")
            .expect("block-gas");
        (
            template.module.to_vec(),
            serde_json::from_str(template.params).expect("params"),
        )
    }

    fn request(instance: &str, params: &Value) -> Value {
        json!({ "instance": instance, "start_block": 21_000_000, "settings": { "params": params } })
    }

    fn summary() -> ProcessorConfig {
        processor_contract(
            "block-summary",
            "summary",
            "1.3.0",
            21_000_000,
            PublishMode::Finalized,
        )
    }

    #[tokio::test]
    async fn an_added_processor_runs_now_and_after_a_restart_with_the_same_identity() {
        let mut node = node(&[]).await;
        let (module, params) = block_gas();
        let change = node
            .admin
            .add(request("gas", &params), Some(module))
            .await
            .expect("added");
        assert_eq!(change.instance, "gas");
        assert_eq!(change.on_demand, BTreeSet::from(["gas".to_owned()]));
        assert!(node.sets.has_changed().expect("sender"));
        let set = node.sets.borrow_and_update().clone();
        let added = set
            .processors
            .iter()
            .find(|processor| processor.descriptor().instance.as_str() == "gas")
            .expect("in the set");
        node.store
            .processor_runtime_state(added.descriptor())
            .await
            .expect("registered");
        // What the next start loads: processors.toml and the module by hash.
        let stored = crate::added_processors::read(&node.data_dir).expect("read");
        assert_eq!(stored.len(), 1);
        assert!(
            stored[0].settings["module"].as_str().is_some_and(
                |module| module.starts_with(&*node.data_dir.join("modules").to_string_lossy())
            ),
            "{:?}",
            stored[0].settings
        );
        let restarted = ProcessorRegistry::standard()
            .instantiate(&stored[0], set.config.chain.chain_id)
            .expect("instantiated");
        assert_eq!(restarted.descriptor(), added.descriptor());
    }

    #[tokio::test]
    async fn refused_additions_change_nothing() {
        let mut node = node(&[summary()]).await;
        let (module, params) = block_gas();
        node.admin
            .add(request("gas", &params), Some(module.clone()))
            .await
            .expect("added");
        node.sets.borrow_and_update();
        let file = std::fs::read_to_string(node.data_dir.join(crate::added_processors::FILE))
            .expect("file");
        let modules = std::fs::read_dir(node.data_dir.join("modules"))
            .expect("modules")
            .count();
        for (request, bytes, expected) in [
            (
                request("summary", &params),
                Some(module.clone()),
                "Conflict",
            ),
            (request("gas", &params), Some(module.clone()), "Conflict"),
            // Another instance of the same kind, version, and parameters.
            (
                request("gas-again", &params),
                Some(module.clone()),
                "Conflict",
            ),
            (
                request("broken", &params),
                Some(b"nope".to_vec()),
                "Invalid",
            ),
            (request("bare", &params), None, "Invalid"),
            (
                json!({ "instance": "pathed", "start_block": 1, "settings": { "module": "/etc/hosts", "params": params } }),
                Some(module.clone()),
                "Invalid",
            ),
            (json!({ "start_block": 1 }), Some(module.clone()), "Invalid"),
        ] {
            let refused = node
                .admin
                .add(request, bytes)
                .await
                .map(|change| change.instance)
                .expect_err("refused");
            assert!(format!("{refused:?}").starts_with(expected), "{refused:?}");
        }
        assert!(!node.sets.has_changed().expect("sender"));
        assert_eq!(
            std::fs::read_to_string(node.data_dir.join(crate::added_processors::FILE))
                .expect("file"),
            file
        );
        assert_eq!(
            std::fs::read_dir(node.data_dir.join("modules"))
                .expect("modules")
                .count(),
            modules,
            "a refused module is not kept"
        );
    }

    #[tokio::test]
    async fn only_added_processors_can_be_removed() {
        let mut node = node(&[summary()]).await;
        let (module, params) = block_gas();
        node.admin
            .add(request("gas", &params), Some(module))
            .await
            .expect("added");
        node.sets.borrow_and_update();
        let configured = node
            .admin
            .remove("summary")
            .await
            .map(|_| ())
            .expect_err("configured");
        assert!(
            matches!(configured, ProcessorAdminError::Conflict(_)),
            "{configured:?}"
        );
        let unknown = node
            .admin
            .remove("other")
            .await
            .map(|_| ())
            .expect_err("unknown");
        assert!(
            matches!(unknown, ProcessorAdminError::NotFound(_)),
            "{unknown:?}"
        );
        assert!(!node.sets.has_changed().expect("sender"));
        let change = node.admin.remove("gas").await.expect("removed");
        assert_eq!(change.instance, "gas");
        let set = node.sets.borrow_and_update().clone();
        assert_eq!(set.processors.len(), 1);
        assert_eq!(set.config.processors.len(), 1);
        assert_eq!(set.processors[0].descriptor().instance.as_str(), "summary");
        assert!(
            crate::added_processors::read(&node.data_dir)
                .expect("read")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn concurrent_additions_take_turns() {
        let node = node(&[]).await;
        let (module, params) = block_gas();
        let mut first = request("gas-a", &params);
        first["version"] = json!("1.0.0");
        let mut second = request("gas-b", &params);
        second["version"] = json!("1.0.1");
        let (first, second) = tokio::join!(
            node.admin.add(first, Some(module.clone())),
            node.admin.add(second, Some(module)),
        );
        first.expect("first");
        second.expect("second");
        let stored = crate::added_processors::read(&node.data_dir).expect("read");
        assert_eq!(
            stored
                .iter()
                .map(|entry| entry.instance.as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["gas-a", "gas-b"])
        );
        assert_eq!(node.sets.borrow().processors.len(), 2);
    }
}
