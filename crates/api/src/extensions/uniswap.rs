use axum::{
    Json, Router,
    extract::{Path, State},
    routing::get,
};
use leani_processor_uniswap::{
    CURRENT_COLLECTION, PoolKind, PoolPriceEntity, UniswapObservationsProcessor,
};
use serde::Serialize;

use crate::{ApiError, QueryContext, QueryExtension, UniswapPoolPrice, address_hex, parse_address};

/// Read-only configuration discovery for a Uniswap observation stream.
#[derive(Clone, Copy, Debug, Default)]
pub struct UniswapObservationsQueryExtension;

impl QueryExtension for UniswapObservationsQueryExtension {
    fn id(&self) -> &'static str {
        "uniswap-observations-v1"
    }

    fn routes(&self) -> Router<QueryContext> {
        Router::new().route("/pools", get(configured_pools))
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ConfiguredPoolsResponse {
    data: Vec<ConfiguredPool>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ConfiguredPool {
    address: String,
    kind: &'static str,
}

async fn configured_pools(
    State(context): State<QueryContext>,
) -> Result<Json<ConfiguredPoolsResponse>, ApiError> {
    let processor = context.processor();
    let processor = processor
        .as_any()
        .downcast_ref::<UniswapObservationsProcessor>()
        .ok_or_else(|| {
            ApiError::internal(
                "Uniswap observations query extension is mounted on an incompatible processor",
            )
        })?;
    let data = processor
        .configured_pools()
        .into_iter()
        .map(|pool| ConfiguredPool {
            address: address_hex(pool.address),
            kind: match pool.kind {
                PoolKind::V2 => "v2",
                PoolKind::V3 => "v3",
            },
        })
        .collect();
    Ok(Json(ConfiguredPoolsResponse { data }))
}

/// Typed latest-price queries backed by one Uniswap processor instance.
#[derive(Clone, Copy, Debug, Default)]
pub struct UniswapQueryExtension;

impl QueryExtension for UniswapQueryExtension {
    fn id(&self) -> &'static str {
        "uniswap-v2"
    }

    fn alias(&self) -> Option<&str> {
        Some("uniswap")
    }

    fn routes(&self) -> Router<QueryContext> {
        Router::new().route("/pools/{address}", get(get_pool))
    }
}

async fn get_pool(
    State(context): State<QueryContext>,
    Path(address): Path<String>,
) -> Result<Json<UniswapPoolPrice>, ApiError> {
    let address = parse_address(&address)?;
    let value = context
        .entity(CURRENT_COLLECTION, &address.0)
        .await?
        .ok_or_else(|| ApiError::not_found("Uniswap pool is not indexed"))?;
    let entity: PoolPriceEntity = postcard::from_bytes(&value)
        .map_err(|error| ApiError::internal(&format!("stored Uniswap pool is invalid: {error}")))?;
    Ok(Json(UniswapPoolPrice::from(&entity)))
}
