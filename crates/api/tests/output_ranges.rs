use std::sync::Arc;

use async_trait::async_trait;
use axum::{
    body::{Body, to_bytes},
    http::Request,
};
use leani_api::{ApiConfig, router_with_processors};
use leani_primitives::{BlockFrame, BlockHash, ProcessorCursor};
use leani_processor_api::{
    DomainChanges, EncodedDelta, LifecyclePolicies, OutputPolicyMode, Processor,
    ProcessorDescriptor, ProcessorError, ReducerTransaction,
};
use leani_store_sqlite::{SqliteStore, StoreConfig};
use leani_testkit::{BlockLocalCounter, fixture_frame};
use serde_json::{Value, json};
use tower::ServiceExt;

// Empty blocks still commit coverage. Only odd blocks emit an entity.
struct SparseCounter(BlockLocalCounter);

#[async_trait]
impl Processor for SparseCounter {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn descriptor(&self) -> &ProcessorDescriptor {
        self.0.descriptor()
    }
    async fn map(&self, block: &BlockFrame) -> Result<EncodedDelta, ProcessorError> {
        self.0.map(block).await
    }
    async fn reduce(
        &self,
        transaction: &mut dyn ReducerTransaction,
        cursor: &ProcessorCursor,
        delta: &EncodedDelta,
    ) -> Result<DomainChanges, ProcessorError> {
        if delta.block.number.0.is_multiple_of(2) {
            Ok(DomainChanges { changes: vec![] })
        } else {
            self.0.reduce(transaction, cursor, delta).await
        }
    }
}

fn processor(blocks: u64) -> Arc<SparseCounter> {
    let mut lifecycle = LifecyclePolicies::default();
    lifecycle.output.mode = OutputPolicyMode::Window;
    lifecycle.output.window = Some(leani_processor_api::OutputWindow {
        max_blocks: Some(blocks),
        max_age_seconds: None,
        max_rows: None,
        max_bytes: None,
    });
    Arc::new(SparseCounter(
        BlockLocalCounter::default().with_lifecycle(lifecycle),
    ))
}

async fn commit(
    store: &SqliteStore,
    processor: &SparseCounter,
    n: u64,
    parent: BlockHash,
) -> BlockHash {
    let frame = fixture_frame(n, parent);
    let delta = processor.map(&frame).await.unwrap();
    let d = processor.descriptor();
    store
        .apply(
            processor,
            ProcessorCursor {
                processor_id: d.id.to_string(),
                processor_version: d.version.to_string(),
                chain_id: frame.chain_id,
                block_number: frame.block.number,
                block_hash: frame.block.hash,
                finality: frame.finality,
                sequence: n,
            },
            &delta,
            &[],
        )
        .await
        .unwrap();
    frame.block.hash
}

const COLLECTION: &str = "/v1/processors/synthetic-counter/collections/counter.blocks";

async fn query(app: axum::Router, suffix: &str) -> (u16, Value) {
    let response = app
        .oneshot(
            Request::get(format!("{COLLECTION}/entities?{suffix}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}

#[tokio::test]
async fn sparse_coverage_distinguishes_empty_unindexed_and_pruned_ranges() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteStore::open(StoreConfig::new(dir.path().join("db")))
        .await
        .unwrap();
    let processor = processor(10);
    let mut parent = BlockHash::ZERO;
    for n in 10..=14 {
        parent = commit(&store, &processor, n, parent).await;
    }
    let app = router_with_processors(
        store.clone(),
        vec![processor.clone()],
        vec![],
        ApiConfig::default(),
    )
    .unwrap();

    let (status, empty) = query(app.clone(), "fromBlock=10&toBlock=10").await;
    let expected: Value =
        serde_json::from_str(include_str!("../fixtures/covered-empty-output.json")).unwrap();
    assert_eq!(
        json!({
            "status": status, "data": empty["data"],
            "requested": empty["coverage"]["requested"],
            "complete": empty["coverage"]["complete"],
        }),
        expected
    );

    for range in ["fromBlock=10&toBlock=10", "fromBlock=14&toBlock=14"] {
        let (status, body) = query(app.clone(), range).await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["data"], json!([]));
        assert_eq!(body["coverage"]["complete"], true);
    }
    let (status, body) = query(app.clone(), "fromBlock=10&toBlock=14").await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["rowCount"], "2");
    assert_eq!(
        body["coverage"]["requested"],
        json!({"fromBlock":10,"toBlock":14})
    );

    for range in [
        "fromBlock=1&toBlock=9",
        "fromBlock=15&toBlock=20",
        "fromBlock=15",
    ] {
        let (status, body) = query(app.clone(), range).await;
        assert_eq!(status, 409, "{body}");
        assert_eq!(body["error"]["code"], "range_incomplete");
        assert_eq!(body["error"]["details"]["complete"], false);
    }
    commit(&store, &processor, 16, parent).await;
    let (status, body) = query(app.clone(), "fromBlock=14&toBlock=16").await;
    assert_eq!(status, 409, "{body}"); // 15 is an internal gap.
    let (status, _) = query(app, "fromBlock=14&toBlock=10").await;
    assert_eq!(status, 400);
}

#[tokio::test]
async fn block_window_rejects_pruned_output_but_allows_covered_empty_suffix() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteStore::open(StoreConfig::new(dir.path().join("db")))
        .await
        .unwrap();
    let processor = processor(2);
    let mut parent = BlockHash::ZERO;
    for n in 1..=6 {
        parent = commit(&store, &processor, n, parent).await;
    }
    let app = router_with_processors(store, vec![processor], vec![], ApiConfig::default()).unwrap();
    let (status, body) = query(app.clone(), "fromBlock=1&toBlock=6").await;
    assert_eq!(status, 410, "{body}");
    assert_eq!(body["error"]["code"], "output_not_retained");
    let (status, body) = query(app.clone(), "fromBlock=5&toBlock=6").await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["rowCount"], "1");
    let (status, body) = query(app, "fromBlock=6&toBlock=6").await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["rowCount"], "0");
}

#[tokio::test]
async fn snapshot_pages_keep_the_resolved_request_across_restart_and_new_commits() {
    let dir = tempfile::tempdir().unwrap();
    let config = StoreConfig::new(dir.path().join("db"));
    let store = SqliteStore::open(config.clone()).await.unwrap();
    let processor = processor(10);
    let mut parent = BlockHash::ZERO;
    for n in 10..=14 {
        parent = commit(&store, &processor, n, parent).await;
    }
    let app = router_with_processors(
        store.clone(),
        vec![processor.clone()],
        vec![],
        ApiConfig::default(),
    )
    .unwrap();
    let (status, first) = query(app.clone(), "fromBlock=10&limit=1").await;
    assert_eq!(status, 200, "{first}");
    commit(&store, &processor, 15, parent).await;
    drop(app);
    drop(store);
    let store = SqliteStore::open(config).await.unwrap();
    let app = router_with_processors(store, vec![processor], vec![], ApiConfig::default()).unwrap();
    let (status, second) = query(
        app,
        &format!("cursor={}", first["nextCursor"].as_str().unwrap()),
    )
    .await;
    assert_eq!(status, 200, "{second}");
    assert_eq!(second["rowCount"], "2");
    assert_eq!(
        second["coverage"]["requested"],
        json!({"fromBlock":10,"toBlock":14})
    );
    assert_eq!(second["coverage"]["complete"], true);
}

#[tokio::test]
async fn snapshot_admission_rechecks_retention_after_a_preflight_query() {
    use leani_primitives::{BlockNumber, BlockRange};
    use leani_store_sqlite::{OutputQuery, QuerySnapshotLimits, StoreError};

    let dir = tempfile::tempdir().unwrap();
    let store = SqliteStore::open(StoreConfig::new(dir.path().join("db")))
        .await
        .unwrap();
    let processor = processor(2);
    let parent = commit(&store, &processor, 1, BlockHash::ZERO).await;
    let parent = commit(&store, &processor, 2, parent).await;
    let range = BlockRange::new(BlockNumber(1), BlockNumber(2)).unwrap();
    assert_eq!(
        store.coverage(processor.descriptor(), range).await.unwrap(),
        vec![range]
    );

    // Ingestion advances after HTTP preflight, pruning the requested output.
    commit(&store, &processor, 3, parent).await;
    let result = store
        .create_covered_query_snapshot(
            processor.descriptor(),
            "counter.blocks",
            OutputQuery {
                from_block: Some(range.start()),
                to_block: Some(range.end()),
                ..Default::default()
            },
            QuerySnapshotLimits::default(),
        )
        .await;
    assert!(matches!(result, Err(StoreError::OutputRangePruned)));
}
