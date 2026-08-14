use axum::{
    Json, Router,
    extract::{Path, Query, State},
    routing::get,
};
use leani_primitives::{BlockNumber, BlockRange};
use leani_processor_blobs::{
    BLOCK_COLLECTION, BlobsBlockEntity, BlobsProcessor, TRANSACTION_BLOCK_INDEX,
    TRANSACTION_COLLECTION,
};
use serde::Deserialize;

use crate::{
    ApiError, BlobScheduleResponse, BlobTransaction, BlobsBlock, BlobsSnapshotEntry,
    CoverageResponse, Page, QueryContext, QueryExtension, decode_block, decode_transaction,
    parse_hash,
};

const BLOCK_CURSOR_NAMESPACE: &str = "blobs.blocks";

/// Typed blobs-money queries backed by one processor instance.
#[derive(Clone, Copy, Debug, Default)]
pub struct BlobsQueryExtension;

impl QueryExtension for BlobsQueryExtension {
    fn id(&self) -> &'static str {
        "blobs-v1"
    }

    fn alias(&self) -> Option<&str> {
        Some("blobs")
    }

    fn routes(&self) -> Router<QueryContext> {
        Router::new()
            .route("/blocks", get(list_blocks))
            .route("/blocks/{number}", get(get_block))
            .route("/snapshot", get(list_snapshot))
            .route("/transactions", get(list_transactions))
            .route("/transactions/{hash}", get(get_transaction))
            .route("/schedule", get(schedule))
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ListBlocksQuery {
    from_block: u64,
    to_block: u64,
    limit: Option<usize>,
    cursor: Option<String>,
    #[serde(default)]
    allow_partial: bool,
}

async fn list_blocks(
    State(context): State<QueryContext>,
    Query(query): Query<ListBlocksQuery>,
) -> Result<Json<Page<BlobsBlock>>, ApiError> {
    let (entities, next_cursor, coverage) = scan_blocks(&context, &query).await?;
    Ok(Json(Page::new(
        entities.iter().map(BlobsBlock::from).collect(),
        next_cursor,
        coverage,
    )))
}

async fn scan_blocks(
    context: &QueryContext,
    query: &ListBlocksQuery,
) -> Result<(Vec<BlobsBlockEntity>, Option<String>, CoverageResponse), ApiError> {
    let requested = BlockRange::new(BlockNumber(query.from_block), BlockNumber(query.to_block))
        .map_err(|error| ApiError::invalid(&error.to_string()))?;
    let coverage = if query.allow_partial {
        context.coverage(Some(requested)).await?
    } else {
        context.require_complete_coverage(requested).await?
    };
    let limit = context.page_limit(query.limit)?;
    let scope = block_range_scope(query.from_block, query.to_block);
    let after = if let Some(cursor) = query.cursor.as_deref() {
        context.decode_scoped_scan_cursor(BLOCK_CURSOR_NAMESPACE, &scope, cursor)?
    } else if query.from_block == 0 {
        Vec::new()
    } else {
        query.from_block.saturating_sub(1).to_be_bytes().to_vec()
    };
    let rows = context
        .scan(BLOCK_COLLECTION, Some(&after), limit.saturating_add(1))
        .await?;
    let mut entities = Vec::with_capacity(rows.len());
    for (_, value) in rows {
        let entity = decode_block(&value)?;
        if entity.block_number > query.to_block {
            break;
        }
        if requested.contains(BlockNumber(entity.block_number)) {
            entities.push(entity);
        }
    }
    let has_more = entities.len() > limit;
    entities.truncate(limit);
    let next_cursor = if has_more {
        entities
            .last()
            .map(|entity| {
                context.encode_scoped_scan_cursor(
                    BLOCK_CURSOR_NAMESPACE,
                    &scope,
                    &entity.block_number.to_be_bytes(),
                )
            })
            .transpose()?
    } else {
        None
    };
    Ok((entities, next_cursor, coverage))
}

async fn list_snapshot(
    State(context): State<QueryContext>,
    Query(query): Query<ListBlocksQuery>,
) -> Result<Json<Page<BlobsSnapshotEntry>>, ApiError> {
    let (entities, next_cursor, coverage) = scan_blocks(&context, &query).await?;
    let mut data = Vec::with_capacity(entities.len());
    for entity in entities {
        let keys = context
            .index_keys(
                TRANSACTION_BLOCK_INDEX,
                &entity.block_number.to_be_bytes(),
                1_025,
            )
            .await?;
        if keys.len() > 1_024 {
            return Err(ApiError::too_expensive(
                "one blob snapshot block exceeds the 1024-transaction response limit",
            ));
        }
        let values = context.entities(TRANSACTION_COLLECTION, &keys).await?;
        let mut transactions = Vec::with_capacity(values.len());
        for value in values {
            let value = value.ok_or_else(|| {
                ApiError::internal("transaction index points to a missing entity")
            })?;
            transactions.push(BlobTransaction::from(&decode_transaction(&value)?));
        }
        transactions.sort_by_key(|transaction| transaction.transaction_index);
        data.push(BlobsSnapshotEntry {
            block: BlobsBlock::from(&entity),
            transactions,
        });
    }
    Ok(Json(Page::new(data, next_cursor, coverage)))
}

async fn get_block(
    State(context): State<QueryContext>,
    Path(number): Path<u64>,
) -> Result<Json<BlobsBlock>, ApiError> {
    let value = context
        .entity(BLOCK_COLLECTION, &number.to_be_bytes())
        .await?
        .ok_or_else(|| ApiError::not_found("blob block is not indexed"))?;
    Ok(Json(BlobsBlock::from(&decode_block(&value)?)))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ListTransactionsQuery {
    block_number: u64,
    limit: Option<usize>,
    cursor: Option<String>,
}

async fn list_transactions(
    State(context): State<QueryContext>,
    Query(query): Query<ListTransactionsQuery>,
) -> Result<Json<Page<BlobTransaction>>, ApiError> {
    let limit = context.page_limit(query.limit)?;
    let scope = query.block_number.to_be_bytes();
    let after = query
        .cursor
        .as_deref()
        .map(|cursor| context.decode_scoped_scan_cursor("blobs.transactions", &scope, cursor))
        .transpose()?;
    let mut keys = context
        .index_keys_after(
            TRANSACTION_BLOCK_INDEX,
            &scope,
            after.as_deref(),
            limit.saturating_add(1),
        )
        .await?;
    let has_more = keys.len() > limit;
    keys.truncate(limit);
    let next_cursor = if has_more {
        keys.last()
            .map(|key| context.encode_scoped_scan_cursor("blobs.transactions", &scope, key))
            .transpose()?
    } else {
        None
    };
    let values = context.entities(TRANSACTION_COLLECTION, &keys).await?;
    let mut data = Vec::with_capacity(values.len());
    for value in values {
        let value = value
            .ok_or_else(|| ApiError::internal("transaction index points to a missing entity"))?;
        data.push(BlobTransaction::from(&decode_transaction(&value)?));
    }
    data.sort_by_key(|transaction| transaction.transaction_index);
    let coverage = context
        .coverage(Some(BlockRange::single(BlockNumber(query.block_number))))
        .await?;
    Ok(Json(Page::new(data, next_cursor, coverage)))
}

async fn get_transaction(
    State(context): State<QueryContext>,
    Path(hash): Path<String>,
) -> Result<Json<BlobTransaction>, ApiError> {
    let key = parse_hash(&hash)?;
    let value = context
        .entity(TRANSACTION_COLLECTION, &key.0)
        .await?
        .ok_or_else(|| ApiError::not_found("blob transaction is not indexed"))?;
    Ok(Json(BlobTransaction::from(&decode_transaction(&value)?)))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ScheduleQuery {
    at_block: Option<u64>,
}

async fn schedule(
    State(context): State<QueryContext>,
    Query(query): Query<ScheduleQuery>,
) -> Result<Json<BlobScheduleResponse>, ApiError> {
    let owner = context.processor();
    let processor = owner
        .as_any()
        .downcast_ref::<BlobsProcessor>()
        .ok_or_else(|| ApiError::internal("blobs query extension owner has the wrong type"))?;
    let block = if let Some(block) = query.at_block {
        block
    } else {
        context
            .processor_cursor()
            .await?
            .map_or(processor.schedule().first_block(), |cursor| {
                cursor.block_number.0
            })
    };
    let fork = processor
        .schedule()
        .forks
        .iter()
        .rev()
        .find(|fork| fork.activation_block <= block)
        .ok_or_else(|| ApiError::not_found("blob schedule is inactive at this block"))?;
    Ok(Json(BlobScheduleResponse::from(fork)))
}

fn block_range_scope(from_block: u64, to_block: u64) -> [u8; 16] {
    let mut scope = [0_u8; 16];
    scope[..8].copy_from_slice(&from_block.to_be_bytes());
    scope[8..].copy_from_slice(&to_block.to_be_bytes());
    scope
}
