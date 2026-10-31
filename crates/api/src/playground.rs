//! `GET /playground`: a dependency-free page to try Wasm processors on the
//! node's live blocks and build a node configuration from requirements. It
//! calls the authenticated API with the token the visitor enters.

use axum::{Json, extract::State, http::StatusCode, response::Html};
use serde::Deserialize;

use crate::{ApiError, ApiState};

pub(crate) async fn page() -> Html<&'static str> {
    Html(include_str!("playground.html"))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CheckRequest {
    toml: String,
}

pub(crate) async fn check_config(
    State(state): State<ApiState>,
    Json(request): Json<CheckRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let check = state
        .config
        .config_check
        .clone()
        .ok_or_else(|| unavailable("config_check_unavailable", "check"))?;
    // Checking reads and parses files: keep it off the async workers.
    tokio::task::spawn_blocking(move || check(&request.toml))
        .await
        .map(Json)
        .map_err(|error| ApiError::internal(&error.to_string()))
}

pub(crate) async fn build_config(
    State(state): State<ApiState>,
    Json(requirements): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let build = state
        .config
        .config_build
        .clone()
        .ok_or_else(|| unavailable("config_build_unavailable", "build"))?;
    // History in days counts back from the newest block the node retains.
    let head = state
        .store
        .recent_canonical_bounds(state.config.chain_id)
        .await?
        .map(|bounds| bounds.end().0);
    // Building validates processors, which may read files.
    tokio::task::spawn_blocking(move || build(&requirements, head))
        .await
        .map_err(|error| ApiError::internal(&error.to_string()))?
        .map(Json)
        .map_err(|message| ApiError::invalid(&message))
}

fn unavailable(code: &'static str, action: &str) -> ApiError {
    ApiError::new(
        StatusCode::SERVICE_UNAVAILABLE,
        code,
        &format!("this node does not {action} configurations"),
        false,
    )
}
