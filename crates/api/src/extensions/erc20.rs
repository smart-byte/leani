use axum::{
    Json, Router,
    extract::{Path, State},
    routing::get,
};
use leani_processor_erc20::{
    BALANCE_COLLECTION, TokenBalanceEntity, balance_key as erc20_balance_key,
};

use crate::{ApiError, Erc20Balance, QueryContext, QueryExtension, parse_address};

/// Typed ERC-20 balance queries backed by one processor instance.
#[derive(Clone, Copy, Debug, Default)]
pub struct Erc20QueryExtension;

impl QueryExtension for Erc20QueryExtension {
    fn id(&self) -> &'static str {
        "erc20-v1"
    }

    fn alias(&self) -> Option<&str> {
        Some("erc20")
    }

    fn routes(&self) -> Router<QueryContext> {
        Router::new().route("/balances/{token}/{address}", get(get_balance))
    }
}

async fn get_balance(
    State(context): State<QueryContext>,
    Path((token, address)): Path<(String, String)>,
) -> Result<Json<Erc20Balance>, ApiError> {
    let token = parse_address(&token)?;
    let address = parse_address(&address)?;
    let value = context
        .entity(BALANCE_COLLECTION, &erc20_balance_key(token, address))
        .await?
        .ok_or_else(|| ApiError::not_found("ERC-20 balance is not indexed"))?;
    let entity: TokenBalanceEntity = postcard::from_bytes(&value).map_err(|error| {
        ApiError::internal(&format!("stored ERC-20 balance is invalid: {error}"))
    })?;
    Ok(Json(Erc20Balance::from(&entity)))
}
