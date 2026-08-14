use axum::{
    Json, Router,
    extract::{Path, State},
    routing::get,
};
use leani_processor_uniswap::{CURRENT_COLLECTION, PoolPriceEntity};

use crate::{ApiError, QueryContext, QueryExtension, UniswapPoolPrice, parse_address};

/// Typed latest-price queries backed by one Uniswap processor instance.
#[derive(Clone, Copy, Debug, Default)]
pub struct UniswapQueryExtension;

impl QueryExtension for UniswapQueryExtension {
    fn id(&self) -> &'static str {
        "uniswap-v1"
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
