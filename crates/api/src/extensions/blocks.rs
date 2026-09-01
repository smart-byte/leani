//! Typed query surface for the latest processed Ethereum block summary.

use axum::{Json, Router, extract::State, routing::get};
use leani_processor_block_summary::{
    BLOCK_LATEST_COLLECTION, BLOCK_LATEST_KEY, BlockSummaryEntity, BlockSummaryProcessor,
};
use serde::Serialize;

use crate::{ApiError, QueryContext, QueryExtension};

#[derive(Clone, Copy, Debug, Default)]
pub struct BlockSummaryQueryExtension;

impl QueryExtension for BlockSummaryQueryExtension {
    fn id(&self) -> &'static str {
        "block-summary-v1"
    }

    fn routes(&self) -> Router<QueryContext> {
        Router::new().route("/latest", get(latest))
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct LatestBlockSummaryResponse {
    data: PublicBlockSummary,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PublicBlockSummary {
    chain_id: u64,
    block_number: u64,
    block_hash: String,
    parent_hash: String,
    timestamp: u64,
    gas_limit: Option<u64>,
    gas_used: Option<u64>,
    base_fee_per_gas: Option<String>,
    blob_gas_used: Option<u64>,
    excess_blob_gas: Option<u64>,
    transaction_count: Option<u32>,
    size_bytes: Option<u64>,
    finality: &'static str,
}

impl From<BlockSummaryEntity> for PublicBlockSummary {
    fn from(entity: BlockSummaryEntity) -> Self {
        Self {
            chain_id: entity.chain_id.0,
            block_number: entity.block_number.0,
            block_hash: entity.block_hash.to_string(),
            parent_hash: entity.parent_hash.to_string(),
            timestamp: entity.timestamp,
            gas_limit: entity.gas_limit,
            gas_used: entity.gas_used,
            base_fee_per_gas: entity.base_fee_per_gas.map(|value| value.to_string()),
            blob_gas_used: entity.blob_gas_used,
            excess_blob_gas: entity.excess_blob_gas,
            transaction_count: entity.transaction_count,
            size_bytes: entity.size_bytes,
            finality: entity.finality.name(),
        }
    }
}

async fn latest(
    State(context): State<QueryContext>,
) -> Result<Json<LatestBlockSummaryResponse>, ApiError> {
    context
        .processor()
        .as_any()
        .downcast_ref::<BlockSummaryProcessor>()
        .ok_or_else(|| {
            ApiError::internal(
                "block-summary query extension is mounted on an incompatible processor",
            )
        })?;
    let value = context
        .entity(BLOCK_LATEST_COLLECTION, BLOCK_LATEST_KEY)
        .await?
        .ok_or_else(|| ApiError::not_found("no Ethereum block summary has been indexed"))?;
    let entity: BlockSummaryEntity = postcard::from_bytes(&value).map_err(|error| {
        ApiError::internal(&format!(
            "stored Ethereum block summary is invalid: {error}"
        ))
    })?;
    Ok(Json(LatestBlockSummaryResponse {
        data: entity.into(),
    }))
}
