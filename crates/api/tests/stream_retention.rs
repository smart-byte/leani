use axum::{
    body::{Body, to_bytes},
    http::Request,
};
use futures::StreamExt;
use leani_api::{ApiConfig, router_with_processors};
use leani_primitives::{BlockHash, ProcessorCursor};
use leani_processor_api::Processor;
use leani_store_sqlite::{SqliteStore, StoreConfig};
use leani_testkit::{BlockLocalCounter, fixture_frame};
use serde_json::Value;
use std::sync::Arc;
use tower::ServiceExt;

async fn commit(
    store: &SqliteStore,
    processor: &BlockLocalCounter,
    n: u64,
    parent: BlockHash,
) -> BlockHash {
    let f = fixture_frame(n, parent);
    let d = processor.map(&f).await.unwrap();
    let p = processor.descriptor();
    store
        .apply(
            processor,
            ProcessorCursor {
                processor_id: p.id.to_string(),
                processor_version: p.version.to_string(),
                chain_id: f.chain_id,
                block_number: f.block.number,
                block_hash: f.block.hash,
                finality: f.finality,
                sequence: n,
            },
            &d,
            &[],
        )
        .await
        .unwrap();
    f.block.hash
}
async fn get(router: axum::Router, path: &str) -> (u16, Value) {
    let r = router
        .oneshot(Request::get(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    (
        r.status().as_u16(),
        serde_json::from_slice(&to_bytes(r.into_body(), usize::MAX).await.unwrap()).unwrap(),
    )
}

#[tokio::test]
async fn retained_output_without_delivery_is_queryable_after_restart() {
    use leani_processor_api::{DeliveryPolicyMode, LifecyclePolicies, OutputPolicyMode};

    let dir = tempfile::tempdir().unwrap();
    let config = StoreConfig::new(dir.path().join("db"));
    let store = SqliteStore::open(config.clone()).await.unwrap();
    let mut lifecycle = LifecyclePolicies::default();
    lifecycle.output.mode = OutputPolicyMode::Full;
    lifecycle.delivery.mode = DeliveryPolicyMode::None;
    lifecycle.delivery.consumers.clear();
    let processor = Arc::new(BlockLocalCounter::default().with_lifecycle(lifecycle));
    let parent = commit(&store, &processor, 1, BlockHash::ZERO).await;
    commit(&store, &processor, 2, parent).await;
    drop(store);
    let store = SqliteStore::open(config).await.unwrap();
    let router =
        router_with_processors(store, vec![processor], vec![], ApiConfig::default()).unwrap();
    let path = "/v1/processors/synthetic-counter/collections/counter.blocks";
    let (status, first) = get(router.clone(), &format!("{path}/entities?limit=1")).await;
    assert_eq!(status, 200, "{first}");
    assert_eq!(first["rowCount"], "2");
    assert_eq!(first["data"].as_array().unwrap().len(), 1);
    assert!(first["boundaryCursor"].is_null());
    assert!(first["recovery"]["follow"].is_null());
    let (status, second) = get(
        router.clone(),
        &format!(
            "{path}/entities?cursor={}",
            first["nextCursor"].as_str().unwrap()
        ),
    )
    .await;
    assert_eq!(status, 200, "{second}");
    assert_eq!(second["data"].as_array().unwrap().len(), 1);
    assert_ne!(first["data"][0]["key"], second["data"][0]["key"]);
    let follow = router
        .clone()
        .oneshot(
            Request::post(format!("{path}/query-and-follow"))
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(follow.status(), 409);
    for endpoint in ["stream", "changes", "changes/head"] {
        let (status, error) = get(
            router.clone(),
            &format!("/v1/processors/synthetic-counter/{endpoint}"),
        )
        .await;
        assert_eq!(status, 409);
        assert_eq!(error["error"]["code"], "delivery_disabled");
        assert_eq!(error["error"]["retryable"], false);
    }
}

#[tokio::test]
async fn explicit_zero_snapshot_boundary_requires_reset_after_pruning() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteStore::open(StoreConfig::new(dir.path().join("db")))
        .await
        .unwrap();
    let p = Arc::new(BlockLocalCounter::default());
    store.register_processor(p.descriptor()).await.unwrap();
    let router =
        router_with_processors(store.clone(), vec![p.clone()], vec![], ApiConfig::default())
            .unwrap();
    let (_, snapshot) = get(
        router.clone(),
        "/v1/processors/synthetic-counter/collections/counter.blocks/entities",
    )
    .await;
    let cursor = snapshot["boundaryCursor"].as_str().unwrap();
    let h = commit(&store, &p, 1, BlockHash::ZERO).await;
    let h = commit(&store, &p, 2, h).await;
    commit(&store, &p, 3, h).await;
    let pruned = store.prune_changes_before(p.descriptor(), 4).await.unwrap();
    assert_eq!(pruned.deleted, 2);
    let (status, result) = get(
        router,
        &format!("/v1/processors/synthetic-counter/changes?after={cursor}"),
    )
    .await;

    assert_eq!(status, 410);
    assert_eq!(result["error"]["code"], "cursor_expired");
}
#[tokio::test]
async fn open_stream_requires_reset_when_pruning_overtakes_it() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteStore::open(StoreConfig::new(dir.path().join("db")))
        .await
        .unwrap();
    let p = Arc::new(BlockLocalCounter::default());
    let h = commit(&store, &p, 1, BlockHash::ZERO).await;
    let router =
        router_with_processors(store.clone(), vec![p.clone()], vec![], ApiConfig::default())
            .unwrap();
    let (_, first) = get(
        router.clone(),
        "/v1/processors/synthetic-counter/changes?limit=1",
    )
    .await;
    let cursor = first["data"][0]["cursor"].as_str().unwrap();
    let response = router
        .oneshot(
            Request::get(format!(
                "/v1/processors/synthetic-counter/stream?after={cursor}"
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    let mut body = response.into_body().into_data_stream();
    let hello = body.next().await.unwrap().unwrap();
    assert!(
        std::str::from_utf8(&hello)
            .unwrap()
            .contains("event: hello")
    );
    let h = commit(&store, &p, 2, h).await;
    let h = commit(&store, &p, 3, h).await;
    store.prune_changes_before(p.descriptor(), 4).await.unwrap();
    commit(&store, &p, 4, h).await;
    let event = tokio::time::timeout(std::time::Duration::from_secs(2), body.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let event = std::str::from_utf8(&event).unwrap();
    assert!(event.contains("event: reset_required"));
    assert!(!event.contains("event: apply"));
    assert!(body.next().await.is_none());
}

#[tokio::test]
async fn snapshot_capacity_returns_retryable_error_and_release_restores_admission() {
    let directory = tempfile::tempdir().unwrap();
    let store = SqliteStore::open(StoreConfig::new(directory.path().join("db")))
        .await
        .unwrap();
    let processor = Arc::new(BlockLocalCounter::default());
    store
        .register_processor(processor.descriptor())
        .await
        .unwrap();
    let config = ApiConfig {
        query_snapshot_max_total_snapshots: 1,
        ..ApiConfig::default()
    };
    let router = router_with_processors(store, vec![processor], vec![], config).unwrap();
    let path = "/v1/processors/synthetic-counter/collections/counter.blocks/entities";
    let (status, first) = get(router.clone(), path).await;
    assert_eq!(status, 200);
    let (status, error) = get(router.clone(), path).await;
    assert_eq!(status, 503);
    assert_eq!(error["error"]["code"], "query_snapshot_capacity");
    assert_eq!(error["error"]["retryable"], true);
    let snapshot = first["snapshotId"].as_str().unwrap();
    let released = router
        .clone()
        .oneshot(
            Request::delete(format!(
                "/v1/processors/synthetic-counter/query-snapshots/{snapshot}"
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(released.status(), 204);
    assert_eq!(get(router, path).await.0, 200);
}

#[tokio::test]
async fn initially_empty_stream_does_not_skip_changes_pruned_before_its_first_poll() {
    let directory = tempfile::tempdir().unwrap();
    let store = SqliteStore::open(StoreConfig::new(directory.path().join("db")))
        .await
        .unwrap();
    let processor = Arc::new(BlockLocalCounter::default());
    store
        .register_processor(processor.descriptor())
        .await
        .unwrap();
    let router = router_with_processors(
        store.clone(),
        vec![processor.clone()],
        vec![],
        ApiConfig::default(),
    )
    .unwrap();
    let response = router
        .oneshot(
            Request::get("/v1/processors/synthetic-counter/stream")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let mut body = response.into_body().into_data_stream();
    let _hello = body.next().await.unwrap().unwrap();
    let parent = commit(&store, &processor, 1, BlockHash::ZERO).await;
    commit(&store, &processor, 2, parent).await;
    store
        .prune_changes_before(processor.descriptor(), 3)
        .await
        .unwrap();
    let event = tokio::time::timeout(std::time::Duration::from_secs(2), body.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        std::str::from_utf8(&event)
            .unwrap()
            .contains("event: reset_required")
    );
    assert!(body.next().await.is_none());
}
