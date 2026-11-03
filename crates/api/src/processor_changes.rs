//! Processors added to and removed from a running node through its API,
//! when the node allows it.

use std::{collections::BTreeSet, sync::Arc};

use async_trait::async_trait;
use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use leani_processor_api::Processor;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::{ApiError, ApiState};

/// The node's side of processor changes.
#[async_trait]
pub trait ProcessorAdmin: Send + Sync {
    /// Add one processor: `processor` is a `[[processors]]` configuration
    /// entry as JSON, `module` a Wasm processor's bytes.
    ///
    /// # Errors
    ///
    /// Returns [`ProcessorAdminError`] for an entry the node refuses, before
    /// it changes anything.
    async fn add(
        &self,
        processor: Value,
        module: Option<Vec<u8>>,
    ) -> Result<ProcessorChange, ProcessorAdminError>;

    /// Remove `instance`, which [`ProcessorAdmin::add`] added.
    ///
    /// # Errors
    ///
    /// Returns [`ProcessorAdminError::NotFound`] for an instance it did not
    /// add, and [`ProcessorAdminError::Conflict`] for one the configuration
    /// file names.
    async fn remove(&self, instance: &str) -> Result<ProcessorChange, ProcessorAdminError>;
}

/// The node's processors after a change, in configuration order.
pub struct ProcessorChange {
    /// The processor added or removed.
    pub instance: String,
    pub processors: Vec<Arc<dyn Processor>>,
    /// Instances whose history runs only on request.
    pub on_demand: BTreeSet<String>,
    /// Instances whose history application subscriptions own.
    pub subscriptions: BTreeSet<String>,
}

impl std::fmt::Debug for ProcessorChange {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProcessorChange")
            .field("instance", &self.instance)
            .field(
                "processors",
                &self
                    .processors
                    .iter()
                    .map(|processor| processor.descriptor().instance.to_string())
                    .collect::<Vec<_>>(),
            )
            .field("on_demand", &self.on_demand)
            .field("subscriptions", &self.subscriptions)
            .finish()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ProcessorAdminError {
    /// The entry or module is not a valid processor.
    #[error("{0}")]
    Invalid(String),
    /// It conflicts with the node's configuration.
    #[error("{0}")]
    Conflict(String),
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    Internal(String),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AddProcessorRequest {
    processor: Value,
    /// A Wasm processor's module, as hex; or name a `template` instead.
    #[serde(default)]
    module: Option<String>,
    #[serde(default)]
    template: Option<String>,
}

pub(crate) async fn add_processor(
    State(state): State<ApiState>,
    Json(request): Json<AddProcessorRequest>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let admin = admin(&state)?;
    let _turn = state.processor_changes.lock().await;
    let mut processor = request.processor;
    let module = if request.module.is_some() || request.template.is_some() {
        // A template's example parameters, unless the entry sets its own.
        let params = processor.pointer("/settings/params").cloned();
        let (bytes, params) = crate::wasm_runs::module_and_params(
            request.module.as_deref(),
            request.template.as_deref(),
            params.as_ref(),
        )?;
        let settings = processor
            .as_object_mut()
            .ok_or_else(|| ApiError::invalid("processor must be a configuration entry object"))?
            .entry("settings")
            .or_insert_with(|| json!({}));
        settings
            .as_object_mut()
            .ok_or_else(|| ApiError::invalid("processor.settings must be an object"))?
            .insert("params".to_owned(), params);
        Some(bytes)
    } else {
        None
    };
    let change = admin.add(processor, module).await.map_err(admin_error)?;
    state.replace_processors(&change)?;
    let summary = crate::processor_summaries_with_extensions(
        &state.processors().map,
        &state.query_extensions,
    )
    .into_iter()
    .find(|summary| summary.instance == change.instance)
    .ok_or_else(|| ApiError::internal("the node did not report the processor it added"))?;
    Ok((
        StatusCode::CREATED,
        Json(
            serde_json::to_value(summary)
                .map_err(|error| ApiError::internal(&error.to_string()))?,
        ),
    ))
}

pub(crate) async fn remove_processor(
    State(state): State<ApiState>,
    Path(instance): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let admin = admin(&state)?;
    let _turn = state.processor_changes.lock().await;
    let change = admin.remove(&instance).await.map_err(admin_error)?;
    state.replace_processors(&change)?;
    Ok(Json(
        json!({ "instance": change.instance, "removed": true }),
    ))
}

fn admin(state: &ApiState) -> Result<Arc<dyn ProcessorAdmin>, ApiError> {
    state.config.processor_admin.clone().ok_or_else(|| {
        ApiError::not_found(
            "this node does not change its processors through the API: set api.allow_processor_changes = true in its configuration",
        )
    })
}

fn admin_error(error: ProcessorAdminError) -> ApiError {
    match error {
        ProcessorAdminError::Invalid(message) => ApiError::invalid(&message),
        ProcessorAdminError::Conflict(message) => {
            ApiError::conflict("processor_conflict", &message)
        }
        ProcessorAdminError::NotFound(message) => ApiError::not_found(&message),
        ProcessorAdminError::Internal(message) => ApiError::internal(&message),
    }
}
