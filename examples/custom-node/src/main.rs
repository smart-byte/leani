//! Minimal downstream binary that adds a native processor without forking the
//! Leani node.

use std::sync::Arc;

use async_trait::async_trait;
use axum::{
    Json, Router,
    extract::{Path, State},
    routing::get,
};
use leani::{
    ApiError, Exit, ProcessorComponents, ProcessorConfig, ProcessorFactory,
    ProcessorFactoryContext, ProcessorFactoryError, ProcessorRegistry, QueryContext,
    QueryExtension, run_with_registry,
};
use leani_primitives::{
    BlockFrame, BlockHash, BlockNumber, Capability, CapabilitySet, FilterScope, Finality,
    ProcessorCursor,
};
use leani_processor_api::{
    ChangeOperation, DataRequirement, DeliveryOrdering, DomainChange, DomainChanges, EncodedDelta,
    Processor, ProcessorDescriptor, ProcessorError, ProcessorId, ProcessorInstanceId,
    ProcessorSchemas, ReducerTransaction, ReductionMode, StartPoint,
};
use semver::Version;
use serde::{Deserialize, Serialize};

const COLLECTION: &str = "example.block_summaries";

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct BlockSummarySettings {
    #[serde(default = "default_change_kind")]
    change_kind: String,
}

fn default_change_kind() -> String {
    "example.block_summary".to_owned()
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct BlockSummary {
    block_number: u64,
    timestamp: u64,
}

#[derive(Clone, Debug)]
struct BlockSummaryProcessor {
    descriptor: ProcessorDescriptor,
    change_kind: String,
}

impl BlockSummaryProcessor {
    fn new(
        configured: &ProcessorConfig,
        settings: BlockSummarySettings,
    ) -> Result<Self, ProcessorFactoryError> {
        if settings.change_kind.trim().is_empty() {
            return Err(ProcessorFactoryError::configuration(
                "settings.change_kind must not be empty",
            ));
        }
        let encoded_settings = serde_json::to_vec(&settings)
            .map_err(|error| ProcessorFactoryError::configuration(error.to_string()))?;
        let id = ProcessorId::new("example-block-summary")
            .map_err(|error| ProcessorFactoryError::configuration(error.to_string()))?;
        let version = Version::new(1, 0, 0);
        let config_hash = BlockHash::new(*blake3::hash(&encoded_settings).as_bytes());
        let instance = ProcessorInstanceId::new(&configured.instance)
            .map_err(|error| ProcessorFactoryError::configuration(error.to_string()))?;
        Ok(Self {
            descriptor: ProcessorDescriptor {
                id,
                instance,
                version,
                code_hash: BlockHash::new(
                    *blake3::hash(b"custom-node/example-block-summary/1.0.0").as_bytes(),
                ),
                config_hash,
                start: StartPoint::Block(BlockNumber(configured.start_block)),
                requirements: vec![DataRequirement {
                    capabilities: CapabilitySet::of(Capability::Header),
                    log_fields: leani_primitives::LogFieldSet::NONE,
                    allow_filtered: false,
                    filter: FilterScope::default(),
                    minimum_finality: Finality::Optimistic,
                }],
                mode: ReductionMode::BlockLocal,
                delivery_ordering: DeliveryOrdering::BlockVersionedIdempotent,
                publication: configured.publication_policy(),
                lifecycle: configured
                    .lifecycle_policies()
                    .map_err(ProcessorFactoryError::configuration)?,
                schemas: ProcessorSchemas {
                    delta_version: 1,
                    entity_schema: "example.block-summary.entity.v1".to_owned(),
                    change_schema: "example.block-summary.change.v1".to_owned(),
                },
            },
            change_kind: settings.change_kind,
        })
    }
}

#[async_trait]
impl Processor for BlockSummaryProcessor {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn descriptor(&self) -> &ProcessorDescriptor {
        &self.descriptor
    }

    async fn map(&self, block: &BlockFrame) -> Result<EncodedDelta, ProcessorError> {
        self.descriptor.requirements[0]
            .validate_frame(block)
            .map_err(|error| ProcessorError::Input(error.to_owned()))?;
        let payload = serde_json::to_vec(&BlockSummary {
            block_number: block.block.number.0,
            timestamp: block.block.timestamp,
        })
        .map_err(|error| ProcessorError::DeltaPayload(error.to_string()))?;
        Ok(EncodedDelta::new(
            &self.descriptor,
            block.chain_id,
            block.block,
            payload,
        ))
    }

    async fn reduce(
        &self,
        transaction: &mut dyn ReducerTransaction,
        cursor: &ProcessorCursor,
        delta: &EncodedDelta,
    ) -> Result<DomainChanges, ProcessorError> {
        delta.validate(&self.descriptor)?;
        validate_cursor(cursor, delta)?;
        let key = delta.block.number.0.to_be_bytes().to_vec();
        transaction
            .put(COLLECTION, key.clone(), delta.payload.clone())
            .await?;
        let change = DomainChange {
            kind: self.change_kind.clone(),
            key,
            operation: ChangeOperation::Upsert,
            payload: delta.payload.clone(),
        };
        transaction.emit(change.clone()).await?;
        Ok(DomainChanges {
            changes: vec![change],
        })
    }

    fn change_json(
        &self,
        change: &DomainChange,
    ) -> Result<Option<serde_json::Value>, ProcessorError> {
        if change.kind != self.change_kind {
            return Ok(None);
        }
        serde_json::from_slice::<BlockSummary>(&change.payload)
            .map_err(|error| ProcessorError::State(error.to_string()))
            .and_then(|summary| {
                serde_json::to_value(summary)
                    .map(Some)
                    .map_err(|error| ProcessorError::State(error.to_string()))
            })
    }
}

fn validate_cursor(cursor: &ProcessorCursor, delta: &EncodedDelta) -> Result<(), ProcessorError> {
    if cursor.processor_id != delta.processor_id.as_str()
        || cursor.processor_version != delta.processor_version
        || cursor.chain_id != delta.chain_id
        || cursor.block_number != delta.block.number
        || cursor.block_hash != delta.block.hash
    {
        return Err(ProcessorError::CursorMismatch);
    }
    Ok(())
}

#[derive(Clone, Copy, Debug)]
struct BlockSummaryFactory;

#[derive(Clone, Copy, Debug)]
struct BlockSummaryQueryExtension;

impl QueryExtension for BlockSummaryQueryExtension {
    fn id(&self) -> &'static str {
        "block-summary-v1"
    }

    fn alias(&self) -> Option<&str> {
        Some("block-summaries")
    }

    fn routes(&self) -> Router<QueryContext> {
        Router::new().route("/{number}", get(get_block_summary))
    }
}

async fn get_block_summary(
    State(context): State<QueryContext>,
    Path(number): Path<u64>,
) -> Result<Json<BlockSummary>, ApiError> {
    let value = context
        .entity(COLLECTION, &number.to_be_bytes())
        .await?
        .ok_or_else(|| ApiError::not_found("block summary is not indexed"))?;
    let summary = serde_json::from_slice(&value).map_err(|error| {
        ApiError::internal(&format!("stored block summary is invalid: {error}"))
    })?;
    Ok(Json(summary))
}

impl ProcessorFactory for BlockSummaryFactory {
    fn id(&self) -> &'static str {
        "example-block-summary"
    }

    fn create(
        &self,
        configured: &ProcessorConfig,
        _context: ProcessorFactoryContext,
    ) -> Result<ProcessorComponents, ProcessorFactoryError> {
        let settings = configured
            .decode_settings::<BlockSummarySettings>()
            .map_err(|error| ProcessorFactoryError::configuration(error.to_string()))?;
        Ok(
            ProcessorComponents::new(Arc::new(BlockSummaryProcessor::new(configured, settings)?))
                .with_query_extension(Arc::new(BlockSummaryQueryExtension)),
        )
    }
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let mut registry = ProcessorRegistry::new();
    if let Err(error) = registry.register(BlockSummaryFactory) {
        eprintln!("error: {error}");
        return Exit::Failure.into();
    }
    match run_with_registry(registry).await {
        Ok(exit) => exit.into(),
        Err(error) => {
            eprintln!("error: {error:#}");
            Exit::Failure.into()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    use super::*;

    #[test]
    fn downstream_registry_instantiates_custom_processor() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("node.toml");
        let config = leani::Config::load(&path)
            .expect("example config")
            .validate()
            .expect("valid base configuration")
            .into_inner();
        let mut registry = ProcessorRegistry::new();
        registry
            .register(BlockSummaryFactory)
            .expect("custom registration");
        let assembly = registry
            .instantiate_all_with_extensions(&config)
            .expect("custom processor");
        let processors = assembly.processors;
        let extensions = assembly.query_extensions;
        assert_eq!(processors.len(), 1);
        assert_eq!(extensions.len(), 1);
        assert_eq!(extensions[0].extension().id(), "block-summary-v1");
        assert_eq!(
            processors[0].descriptor().id.as_str(),
            "example-block-summary"
        );
    }

    #[tokio::test]
    async fn custom_query_extension_is_mounted_at_the_instance_scoped_route() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("node.toml");
        let config = leani::Config::load(&path)
            .expect("example config")
            .validate()
            .expect("valid base configuration")
            .into_inner();
        let mut registry = ProcessorRegistry::new();
        registry
            .register(BlockSummaryFactory)
            .expect("custom registration");
        let assembly = registry
            .instantiate_all_with_extensions(&config)
            .expect("custom processor");
        let processors = assembly.processors;
        let extensions = assembly.query_extensions;
        let instance = processors[0].descriptor().instance.to_string();
        let directory = tempfile::tempdir().expect("tempdir");
        let store = leani_store_sqlite::SqliteStore::open(leani_store_sqlite::StoreConfig::new(
            directory.path().join("node.sqlite"),
        ))
        .await
        .expect("store");
        let app = leani_api::router_with_processors(
            store,
            processors,
            extensions,
            leani_api::ApiConfig::default(),
        )
        .expect("router");

        let response = app
            .oneshot(
                Request::get(format!("/v1/processors/{instance}/query/42"))
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn custom_processor_renders_typed_json_change() {
        let configured = ProcessorConfig {
            id: "example-block-summary".to_owned(),
            instance: "example-block-summary-test".to_owned(),
            version: "1.0.0".to_owned(),
            history_mode: leani::config::ProcessorHistoryMode::Automatic,
            history_control: leani::config::ProcessorHistoryControl::NodeOwned,
            require_retained_input: false,
            start_block: 1,
            publish: leani::config::PublishMode::OptimisticAndFinalized,
            state: leani::config::StatePolicyConfig {
                mode: leani_processor_api::StatePolicyMode::Durable,
            },
            artifacts: leani::config::ArtifactPolicyConfig::default(),
            output: leani::config::OutputPolicyConfig {
                mode: leani_processor_api::OutputPolicyMode::Full,
                window: None,
                finalized_only: false,
            },
            delivery: leani::config::DeliveryPolicyConfig {
                mode: leani_processor_api::DeliveryPolicyMode::Window,
                max_bytes: leani::config::HumanBytes::from_bytes(1 << 30),
                max_age: leani::config::HumanDuration::from_seconds(24 * 60 * 60),
                on_limit: leani_processor_api::DeliveryLimitAction::Pause,
                pruning: leani::config::DeliveryPruningConfig::default(),
                consumers: Vec::new(),
            },
            checkpoint: leani::config::CheckpointPolicyConfig {
                mode: leani_processor_api::CheckpointPolicyMode::Automatic,
                keep: 3,
            },
            undo: leani::config::UndoPolicyConfig {
                mode: leani_processor_api::UndoPolicyMode::Unfinalized,
                safety_blocks: 256,
            },
            coverage: leani::config::ProcessorCoverageConfig::default(),
            settings: [(
                "change_kind".to_owned(),
                toml::Value::String("example.block_summary".to_owned()),
            )]
            .into_iter()
            .collect(),
        };
        let components = BlockSummaryFactory
            .create(&configured, ProcessorFactoryContext { chain_id: 1 })
            .expect("processor");
        let payload = serde_json::to_vec(&BlockSummary {
            block_number: 42,
            timestamp: 1_700_000_000,
        })
        .expect("payload");
        let rendered = components
            .processor
            .change_json(&DomainChange {
                kind: "example.block_summary".to_owned(),
                key: 42_u64.to_be_bytes().to_vec(),
                operation: ChangeOperation::Upsert,
                payload,
            })
            .expect("render")
            .expect("JSON");
        assert_eq!(rendered["blockNumber"], 42);
    }
}
