//! Typed query surface for the latest processed Ethereum block summary.

use alloy_primitives::U256;
use axum::{
    Json, Router,
    extract::{Path, State},
    routing::get,
};
use leani_processor_block_summary::{
    BLOCK_COLLECTION, BLOCK_NUMBER_INDEX_COLLECTION, BlockSummaryEntity, BlockSummaryProcessor,
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
        Router::new()
            .route("/latest", get(latest))
            .route("/blocks/{number}", get(by_number))
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
            base_fee_per_gas: entity
                .base_fee_per_gas
                .map(|value| U256::from_be_bytes(value.0).to_string()),
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
    let cursor = context
        .processor_cursor()
        .await?
        .ok_or_else(|| ApiError::not_found("no Ethereum block summary has been indexed"))?;
    let value = context
        .entity(BLOCK_COLLECTION, &cursor.block_hash.0)
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

async fn by_number(
    State(context): State<QueryContext>,
    Path(number): Path<u64>,
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
    let hash = context
        .entity(BLOCK_NUMBER_INDEX_COLLECTION, &number.to_be_bytes())
        .await?
        .ok_or_else(|| ApiError::not_found("Ethereum block summary is not indexed"))?;
    let value = context
        .entity(BLOCK_COLLECTION, &hash)
        .await?
        .ok_or_else(|| ApiError::internal("block summary number index is inconsistent"))?;
    let entity: BlockSummaryEntity = postcard::from_bytes(&value).map_err(|error| {
        ApiError::internal(&format!(
            "stored Ethereum block summary is invalid: {error}"
        ))
    })?;
    Ok(Json(LatestBlockSummaryResponse {
        data: entity.into(),
    }))
}
