use std::sync::Arc;

use axum::{
    Router,
    body::Body,
    http::{Method, Request, StatusCode},
};
use futures::StreamExt;
use leani_primitives::{
    Address, BlockFrame, BlockHash, Finality, HeaderEnvelope, Log, Material, MissingReason,
    TransactionHash,
};
use leani_processor_api::Processor;
use leani_source_api::{ChainEvent, LiveStart};
use leani_store_sqlite::{SqliteStore, StoreConfig};
use serde_json::{Value, json};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

use crate::{ApiConfig, LiveFeed, router_with_processors};

const USDC: [u8; 20] = [0xaa; 20];

fn frame(number: u64, parent: BlockHash, salt: u8, values: &[u64]) -> BlockFrame {
    let mut frame = leani_testkit::fixture_frame(number, parent);
    if salt != 0 {
        frame.block.hash.0[0] ^= salt;
    }
    frame.finality = Finality::Attested;
    frame.header = Material::Complete(HeaderEnvelope {
        rlp: None,
        transactions_root: None,
        receipts_root: None,
        withdrawals_root: None,
        gas_limit: Some(30_000_000),
        gas_used: Some(1),
        base_fee_per_gas: None,
        blob_gas_used: None,
        excess_blob_gas: None,
        size_bytes: None,
        transaction_count: Some(0),
        consensus_size_bytes: None,
    });
    let topic = |byte| {
        let mut topic = [0; 32];
        topic[31] = byte;
        topic
    };
    frame.logs = Material::Complete(
        values
            .iter()
            .zip(0_u32..)
            .map(|(value, index)| {
                let mut data = [0; 32];
                data[24..].copy_from_slice(&value.to_be_bytes());
                Log {
                    address: Address::new(USDC),
                    topics: vec![
                        *alloy_primitives::keccak256(b"Transfer(address,address,uint256)"),
                        topic(1),
                        topic(2),
                    ],
                    data: data.to_vec(),
                    transaction_hash: Some(TransactionHash::new([salt.wrapping_add(7); 32])),
                    transaction_index: 0,
                    log_index: index,
                }
            })
            .collect(),
    );
    frame
}

async fn node(frames: &[BlockFrame], config: ApiConfig) -> (tempfile::TempDir, Router) {
    let directory = tempfile::tempdir().expect("directory");
    let store = SqliteStore::open(StoreConfig::new(directory.path().join("node.sqlite")))
        .await
        .expect("store");
    for frame in frames {
        store.store_recent_frame(frame).await.expect("recent frame");
    }
    let processor: Arc<dyn Processor> = Arc::new(leani_testkit::OrderedLedgerProcessor::default());
    let router =
        router_with_processors(store, vec![processor], Vec::new(), config).expect("router");
    (directory, router)
}

fn run_request(body: &Value) -> Request<Body> {
    Request::builder()
        .method(Method::POST)
        .uri("/admin/v1/wasm/runs")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .expect("request")
}

async fn start(app: &Router, body: &Value) -> axum::response::Response {
    app.clone()
        .oneshot(run_request(body))
        .await
        .expect("response")
}

fn events(body: &str) -> Vec<(String, Value)> {
    body.split("\n\n")
        .filter_map(|block| {
            let name = block
                .lines()
                .find_map(|line| line.strip_prefix("event: "))?;
            let data = block.lines().find_map(|line| line.strip_prefix("data: "))?;
            Some((
                name.to_owned(),
                serde_json::from_str(data).expect("event JSON"),
            ))
        })
        .collect()
}

fn params() -> Value {
    json!({ "tokens": [Address::new(USDC).to_string()] })
}

fn swap_request(run: &str, body: &Value) -> Request<Body> {
    Request::builder()
        .method(Method::PUT)
        .uri(format!("/admin/v1/wasm/runs/{run}/processor"))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .expect("request")
}

async fn swap(app: &Router, run: &str, body: &Value) -> StatusCode {
    app.clone()
        .oneshot(swap_request(run, body))
        .await
        .expect("response")
        .status()
}

async fn read_until(
    body: &mut axum::body::BodyDataStream,
    text: &mut String,
    enough: impl Fn(&[(String, Value)]) -> bool,
) -> Vec<(String, Value)> {
    while !enough(&events(text)) {
        let chunk = tokio::time::timeout(std::time::Duration::from_secs(20), body.next())
            .await
            .unwrap_or_else(|_| panic!("no further events after:\n{text}"))
            .expect("more events")
            .expect("chunk");
        text.push_str(std::str::from_utf8(&chunk).expect("UTF-8"));
    }
    events(text)
}

fn count(events: &[(String, Value)], name: &str) -> usize {
    events.iter().filter(|(event, _)| event == name).count()
}

fn names(events: &[(String, Value)]) -> Vec<&str> {
    events.iter().map(|(name, _)| name.as_str()).collect()
}

fn values(event: &(String, Value)) -> Vec<Value> {
    event.1["changes"]
        .as_array()
        .expect("changes")
        .iter()
        .map(|change| change["value"]["value"].clone())
        .collect()
}

/// Stays open after the script, as a followed feed does.
fn scripted_feed(script: Vec<ChainEvent>) -> (LiveFeed, Arc<std::sync::Mutex<Vec<LiveStart>>>) {
    let starts = Arc::new(std::sync::Mutex::new(Vec::new()));
    let recorded = starts.clone();
    let feed: LiveFeed = Arc::new(move |_processor, start, _cancellation| {
        recorded.lock().expect("starts").push(start);
        let script = script.clone();
        Box::pin(async move {
            Ok(futures::stream::iter(script.into_iter().map(Ok))
                .chain(futures::stream::pending())
                .boxed())
        })
    });
    (feed, starts)
}

fn without_logs(frame: &BlockFrame) -> BlockFrame {
    let mut frame = frame.clone();
    frame.logs = Material::Missing(MissingReason::NotRequested);
    frame.receipts = Material::Missing(MissingReason::NotRequested);
    frame
}

#[tokio::test]
async fn a_template_runs_over_the_retained_window() {
    let first = frame(1, BlockHash::ZERO, 0, &[5]);
    let second = frame(2, first.block.hash, 0, &[6, 7]);
    let third = frame(3, second.block.hash, 0, &[]);
    let (_directory, app) = node(&[first, second, third], ApiConfig::default()).await;
    let response = start(
        &app,
        &json!({
            "template": "erc20-transfers",
            "params": params(),
            "blocks": 2,
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let events = events(std::str::from_utf8(&body).expect("UTF-8"));
    assert_eq!(names(&events), ["ready", "block", "block", "done"]);
    assert_eq!(
        events[0].1["window"],
        json!({ "fromBlock": 2, "toBlock": 3 })
    );
    assert_eq!(events[0].1["manifest"]["name"], "erc20-transfers");
    assert_eq!(values(&events[1]), [json!("6"), json!("7")]);
    assert_eq!(events[2].1["changes"], json!([]));
    assert_eq!(events[3].1["blocks"], 2);
    assert_eq!(events[3].1["changes"], 2);

    let listed = app
        .oneshot(
            Request::get("/v1/wasm/templates")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    let body = axum::body::to_bytes(listed.into_body(), usize::MAX)
        .await
        .expect("body");
    let templates: Value = serde_json::from_slice(&body).expect("templates");
    let names = templates["data"]
        .as_array()
        .expect("templates")
        .iter()
        .map(|template| template["name"].clone())
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        [
            "block-gas",
            "erc20-transfers",
            "token-volume",
            "event-decoder"
        ]
    );
}

#[tokio::test]
async fn a_block_sends_every_change_however_many_pages_they_fill() {
    let transfers = vec![1; super::CHANGE_PAGE + 1];
    let (_directory, app) = node(
        &[frame(1, BlockHash::ZERO, 0, &transfers)],
        ApiConfig::default(),
    )
    .await;
    let response = start(
        &app,
        &json!({ "template": "erc20-transfers", "params": params(), "blocks": 1 }),
    )
    .await;
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let events = events(std::str::from_utf8(&body).expect("UTF-8"));
    assert_eq!(
        names(&events),
        ["ready", "block", "done"],
        "{:?}",
        events.last()
    );
    assert_eq!(values(&events[1]).len(), transfers.len());
}

#[tokio::test]
async fn a_followed_run_applies_live_blocks_and_undoes_a_reorg() {
    let first = frame(1, BlockHash::ZERO, 0, &[5]);
    let (live, _) = broadcast::channel(16);
    let shutdown = CancellationToken::new();
    let (_directory, app) = node(
        std::slice::from_ref(&first),
        ApiConfig {
            live_events: Some(live.clone()),
            shutdown: shutdown.clone(),
            ..ApiConfig::default()
        },
    )
    .await;
    let response = start(
        &app,
        &json!({
            "template": "token-volume",
            "params": params(),
            "blocks": 1,
            "follow": true,
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body().into_data_stream();
    let mut text = String::new();
    read_until(&mut body, &mut text, |events| count(events, "ready") == 1).await;
    let replaced = frame(2, first.block.hash, 0, &[100]);
    let replacement = frame(2, first.block.hash, 9, &[1]);
    // A reorg below the run's window reverts nothing the run applied.
    live.send(ChainEvent::Reorg {
        reverted: vec![leani_testkit::fixture_frame(0, BlockHash::ZERO).block],
        applied: Vec::new(),
    })
    .expect("old reorg");
    live.send(ChainEvent::Block(Box::new(replaced.clone())))
        .expect("live block");
    live.send(ChainEvent::Reorg {
        reverted: vec![replaced.block],
        applied: vec![replacement],
    })
    .expect("reorg");
    while let Some(chunk) = body.next().await {
        text.push_str(std::str::from_utf8(&chunk.expect("chunk")).expect("UTF-8"));
        if events(&text)
            .iter()
            .filter(|(name, _)| name == "block")
            .count()
            == 3
        {
            shutdown.cancel();
        }
    }
    let events = events(&text);
    assert_eq!(
        names(&events),
        ["ready", "block", "block", "undo", "block", "done"]
    );
    let volume = |index: usize| events[index].1["changes"][0]["value"]["volume"].clone();
    assert_eq!(volume(1), json!("5"));
    assert_eq!(volume(2), json!("105"));
    // The undo restores block 1's total; the replacement block adds to it.
    assert_eq!(events[3].1["changes"][0]["undo"], true);
    assert_eq!(volume(3), json!("5"));
    assert_eq!(volume(4), json!("6"));
}

#[tokio::test]
async fn a_run_the_live_lane_cannot_serve_follows_its_own_feed() {
    let first = frame(1, BlockHash::ZERO, 0, &[]);
    let second = frame(2, first.block.hash, 0, &[]);
    let replacement = frame(2, first.block.hash, 9, &[]);
    // This lane keeps no headers, which the block-gas template needs.
    let mut retained = first.clone();
    retained.header = Material::Missing(MissingReason::NotRequested);
    let starts = Arc::new(std::sync::Mutex::new(Vec::new()));
    let feed: LiveFeed = {
        let starts = starts.clone();
        let script = vec![
            ChainEvent::Block(Box::new(first.clone())),
            ChainEvent::Block(Box::new(second.clone())),
            ChainEvent::Reorg {
                reverted: vec![second.block],
                applied: vec![replacement],
            },
        ];
        Arc::new(move |_processor, start, _cancellation| {
            starts.lock().expect("starts").push(start);
            let script = script.clone();
            Box::pin(async move { Ok(futures::stream::iter(script.into_iter().map(Ok)).boxed()) })
        })
    };
    let (live, _) = broadcast::channel(16);
    let (_directory, app) = node(
        &[retained],
        ApiConfig {
            live_events: Some(live),
            live_feed: Some(feed),
            ..ApiConfig::default()
        },
    )
    .await;
    let response = start(
        &app,
        &json!({
            "template": "block-gas",
            "blocks": 1,
            "follow": true,
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let text = std::str::from_utf8(&body).expect("UTF-8");
    let events = events(text);
    assert_eq!(
        names(&events),
        ["ready", "notice", "block", "block", "undo", "block", "done"],
        "{text}"
    );
    assert_eq!(events[0].1["feed"], "own");
    assert_eq!(
        events[0].1["window"],
        json!({ "fromBlock": 1, "toBlock": 1 })
    );
    assert_eq!(
        starts.lock().expect("starts").as_slice(),
        [LiveStart::AnchoredOverlap {
            anchor: first.block,
            overlap_blocks: 1,
        }]
    );
    let blocks = [2, 3, 5].map(|index| events[index].1["number"].clone());
    assert_eq!(blocks, [json!(1), json!(2), json!(2)]);
}

#[tokio::test]
async fn a_swap_reruns_the_kept_blocks_and_follows_with_the_new_processor() {
    let first = frame(1, BlockHash::ZERO, 0, &[5]);
    let second = frame(2, first.block.hash, 0, &[6, 7]);
    let (live, _) = broadcast::channel(16);
    let shutdown = CancellationToken::new();
    let (_directory, app) = node(
        &[first, second.clone()],
        ApiConfig {
            live_events: Some(live.clone()),
            shutdown: shutdown.clone(),
            ..ApiConfig::default()
        },
    )
    .await;
    let response = start(
        &app,
        &json!({ "template": "erc20-transfers", "params": params(), "blocks": 2, "follow": true }),
    )
    .await;
    let mut body = response.into_body().into_data_stream();
    let mut text = String::new();
    let seen = read_until(&mut body, &mut text, |events| count(events, "block") == 2).await;
    assert_eq!(seen[0].1["version"], 1);
    let run = seen[0].1["run"].as_str().expect("run id").to_owned();

    let raised = json!({ "tokens": [Address::new(USDC).to_string()], "minValue": "7" });
    assert_eq!(
        swap(
            &app,
            &run,
            &json!({ "template": "erc20-transfers", "params": raised })
        )
        .await,
        StatusCode::ACCEPTED
    );
    read_until(&mut body, &mut text, |events| count(events, "block") == 4).await;
    live.send(ChainEvent::Block(Box::new(frame(
        3,
        second.block.hash,
        0,
        &[9, 1],
    ))))
    .expect("live block");
    let seen = read_until(&mut body, &mut text, |events| count(events, "block") == 5).await;
    shutdown.cancel();
    assert_eq!(
        names(&seen),
        [
            "ready", "block", "block", "reset", "block", "block", "block"
        ],
        "{text}"
    );
    let reset = &seen[3].1;
    assert_eq!(reset["version"], 2);
    assert_eq!(reset["run"], run.as_str());
    assert_eq!(reset["feed"], "lane");
    assert_eq!(reset["window"], json!({ "fromBlock": 1, "toBlock": 2 }));
    // The same blocks again, now without the transfers below 7.
    assert_eq!(seen[4].1["number"], 1);
    assert_eq!(values(&seen[4]), Vec::<Value>::new());
    assert_eq!(values(&seen[5]), [json!("7")]);
    assert_eq!(values(&seen[6]), [json!("9")]);
}

#[tokio::test]
async fn a_swap_to_material_the_lane_lacks_opens_the_runs_own_feed() {
    let first = frame(1, BlockHash::ZERO, 0, &[5]);
    let second = frame(2, first.block.hash, 0, &[6]);
    let (feed, starts) = scripted_feed(vec![
        ChainEvent::Block(Box::new(first.clone())),
        ChainEvent::Block(Box::new(second.clone())),
    ]);
    let (live, _) = broadcast::channel(16);
    let shutdown = CancellationToken::new();
    let (_directory, app) = node(
        &[without_logs(&first), without_logs(&second)],
        ApiConfig {
            live_events: Some(live),
            live_feed: Some(feed),
            shutdown: shutdown.clone(),
            ..ApiConfig::default()
        },
    )
    .await;
    let response = start(
        &app,
        &json!({ "template": "block-gas", "blocks": 2, "follow": true }),
    )
    .await;
    let mut body = response.into_body().into_data_stream();
    let mut text = String::new();
    let seen = read_until(&mut body, &mut text, |events| count(events, "block") == 2).await;
    assert_eq!(seen[0].1["feed"], "lane");
    let run = seen[0].1["run"].as_str().expect("run id").to_owned();
    assert!(starts.lock().expect("starts").is_empty());

    assert_eq!(
        swap(
            &app,
            &run,
            &json!({ "template": "erc20-transfers", "params": params() })
        )
        .await,
        StatusCode::ACCEPTED
    );
    let seen = read_until(&mut body, &mut text, |events| count(events, "block") == 4).await;
    shutdown.cancel();
    assert_eq!(
        names(&seen),
        [
            "ready", "block", "block", "reset", "notice", "block", "block"
        ],
        "{text}"
    );
    assert_eq!(seen[3].1["feed"], "own");
    assert_eq!(seen[3].1["manifest"]["name"], "erc20-transfers");
    assert_eq!(
        starts.lock().expect("starts").as_slice(),
        [LiveStart::AnchoredOverlap {
            anchor: second.block,
            overlap_blocks: 2,
        }]
    );
    assert_eq!(values(&seen[5]), [json!("5")]);
    assert_eq!(values(&seen[6]), [json!("6")]);
}

#[tokio::test]
async fn an_own_feed_run_swaps_over_its_kept_blocks_without_fetching_again() {
    let first = frame(1, BlockHash::ZERO, 0, &[5]);
    let second = frame(2, first.block.hash, 0, &[6, 7]);
    let (feed, starts) = scripted_feed(vec![
        ChainEvent::Block(Box::new(first.clone())),
        ChainEvent::Block(Box::new(second.clone())),
    ]);
    let (live, _) = broadcast::channel(16);
    let shutdown = CancellationToken::new();
    let (_directory, app) = node(
        &[without_logs(&first), without_logs(&second)],
        ApiConfig {
            live_events: Some(live),
            live_feed: Some(feed),
            shutdown: shutdown.clone(),
            ..ApiConfig::default()
        },
    )
    .await;
    let response = start(
        &app,
        &json!({ "template": "erc20-transfers", "params": params(), "blocks": 2, "follow": true }),
    )
    .await;
    let mut body = response.into_body().into_data_stream();
    let mut text = String::new();
    let seen = read_until(&mut body, &mut text, |events| count(events, "block") == 2).await;
    assert_eq!(seen[0].1["feed"], "own");
    let run = seen[0].1["run"].as_str().expect("run id").to_owned();

    let raised = json!({ "tokens": [Address::new(USDC).to_string()], "minValue": "7" });
    assert_eq!(
        swap(
            &app,
            &run,
            &json!({ "template": "erc20-transfers", "params": raised })
        )
        .await,
        StatusCode::ACCEPTED
    );
    let seen = read_until(&mut body, &mut text, |events| count(events, "block") == 4).await;
    shutdown.cancel();
    assert_eq!(
        names(&seen),
        [
            "ready", "notice", "block", "block", "reset", "block", "block"
        ],
        "{text}"
    );
    assert_eq!(seen[4].1["feed"], "own");
    assert_eq!(seen[4].1["window"], json!({ "fromBlock": 1, "toBlock": 2 }));
    assert_eq!(values(&seen[5]), Vec::<Value>::new());
    assert_eq!(values(&seen[6]), [json!("7")]);
    assert_eq!(starts.lock().expect("starts").len(), 1, "fetched again");
}

#[tokio::test]
async fn swaps_are_refused_for_unknown_runs_and_invalid_processors() {
    let first = frame(1, BlockHash::ZERO, 0, &[5]);
    let (live, _) = broadcast::channel(16);
    let shutdown = CancellationToken::new();
    let (_directory, app) = node(
        std::slice::from_ref(&first),
        ApiConfig {
            live_events: Some(live.clone()),
            shutdown: shutdown.clone(),
            ..ApiConfig::default()
        },
    )
    .await;
    let template = json!({ "template": "erc20-transfers", "params": params() });
    assert_eq!(swap(&app, "0123", &template).await, StatusCode::NOT_FOUND);

    let response = start(
        &app,
        &json!({ "template": "erc20-transfers", "params": params(), "blocks": 1, "follow": true }),
    )
    .await;
    let mut body = response.into_body().into_data_stream();
    let mut text = String::new();
    let seen = read_until(&mut body, &mut text, |events| count(events, "block") == 1).await;
    let run = seen[0].1["run"].as_str().expect("run id").to_owned();
    for (invalid, status) in [
        (
            json!({ "template": "erc20-transfers", "params": { "tokens": ["0x12"] } }),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({ "template": "erc20-transfers", "module": "00" }),
            StatusCode::BAD_REQUEST,
        ),
        (json!({ "template": "nope" }), StatusCode::BAD_REQUEST),
    ] {
        assert_eq!(swap(&app, &run, &invalid).await, status, "{invalid}");
    }
    live.send(ChainEvent::Block(Box::new(frame(
        2,
        first.block.hash,
        0,
        &[8],
    ))))
    .expect("live block");
    let seen = read_until(&mut body, &mut text, |events| count(events, "block") == 2).await;
    shutdown.cancel();
    assert_eq!(names(&seen), ["ready", "block", "block"], "{text}");
    assert_eq!(values(&seen[2]), [json!("8")]);
}

#[tokio::test]
async fn malformed_runs_are_refused_before_they_start() {
    let (_directory, app) = node(&[], ApiConfig::default()).await;
    for (body, status) in [
        (json!({}), StatusCode::BAD_REQUEST),
        (
            json!({ "template": "erc20-transfers", "module": "00" }),
            StatusCode::BAD_REQUEST,
        ),
        (json!({ "template": "nope" }), StatusCode::BAD_REQUEST),
        (json!({ "module": "zz" }), StatusCode::BAD_REQUEST),
        (
            json!({ "module": "0061736d01000000" }),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({ "template": "erc20-transfers", "follow": true }),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({ "template": "erc20-transfers", "params": { "tokens": ["0x12"] } }),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({ "template": "block-gas", "blocks": 0 }),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({ "template": "block-gas", "blocks": 1025 }),
            StatusCode::BAD_REQUEST,
        ),
    ] {
        assert_eq!(start(&app, &body).await.status(), status, "{body}");
    }
    let response = start(&app, &json!({ "template": "erc20-transfers" })).await;
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let text = std::str::from_utf8(&body).expect("UTF-8");
    let events = events(text);
    assert_eq!(names(&events), ["error"], "{text}");
    let error = &events[0].1["error"];
    assert_eq!(error["code"], "wasm_run_failed");
    assert!(
        error["message"]
            .as_str()
            .is_some_and(|message| message.contains("retains no recent blocks")),
        "{text}"
    );
}

#[tokio::test]
async fn runs_past_the_node_limit_are_refused_until_one_ends() {
    let first = frame(1, BlockHash::ZERO, 0, &[5]);
    let (live, _) = broadcast::channel(16);
    let shutdown = CancellationToken::new();
    let (_directory, app) = node(
        &[first],
        ApiConfig {
            live_events: Some(live),
            shutdown: shutdown.clone(),
            ..ApiConfig::default()
        },
    )
    .await;
    let following = json!({ "template": "block-gas", "blocks": 1, "follow": true });
    let mut open = Vec::new();
    for _ in 0..super::MAX_RUNS {
        let response = start(&app, &following).await;
        assert_eq!(response.status(), StatusCode::OK);
        open.push(response);
    }
    let refused = start(&app, &following).await;
    assert_eq!(refused.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = axum::body::to_bytes(refused.into_body(), usize::MAX)
        .await
        .expect("body");
    let refused: Value = serde_json::from_slice(&body).expect("JSON");
    assert_eq!(refused["error"]["code"], "wasm_run_capacity");
    assert_eq!(refused["error"]["retryable"], true);
    shutdown.cancel();
    for response in open {
        axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
    }
    assert_eq!(start(&app, &following).await.status(), StatusCode::OK);
}

async fn post(app: Router, uri: &str, body: Value) -> (StatusCode, Value) {
    post_from(app, uri, body, None, &[]).await
}

/// `post` from a peer, as a server with connect info sees it, with headers.
async fn post_from(
    app: Router,
    uri: &str,
    body: Value,
    peer: Option<&str>,
    headers: &[(&str, &str)],
) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header("content-type", "application/json");
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    let mut request = request.body(Body::from(body.to_string())).expect("request");
    if let Some(peer) = peer {
        request
            .extensions_mut()
            .insert(axum::extract::ConnectInfo::<std::net::SocketAddr>(
                peer.parse().expect("peer"),
            ));
    }
    let response = app.oneshot(request).await.expect("response");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    (status, serde_json::from_slice(&body).expect("JSON"))
}

#[tokio::test]
async fn specs_compose_to_source_and_builds_use_the_node_hook() {
    let (_directory, app) = node(&[], ApiConfig::default()).await;
    let (status, composed) = post(
        app.clone(),
        "/v1/wasm/compose",
        json!({ "name": "gas", "data": { "kind": "blocks", "fields": ["gasUsed"] } }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{composed}");
    assert!(
        composed["source"]
            .as_str()
            .is_some_and(|source| source.contains("export_processor!(GasProcessor);"))
    );
    assert_eq!(
        composed["fields"][0],
        json!({ "name": "gasUsed", "unsigned": true })
    );
    assert_eq!(
        (&composed["language"], &composed["path"]),
        (&json!("rust"), &json!("src/lib.rs"))
    );
    let (status, typescript) = post(
        app.clone(),
        "/v1/wasm/compose",
        json!({ "name": "gas", "language": "typescript", "data": { "kind": "blocks", "fields": ["gasUsed"] } }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{typescript}");
    assert_eq!(typescript["path"], "assembly/index.ts");
    assert!(
        typescript["source"]
            .as_str()
            .is_some_and(|source| source.contains("register(new GasProcessor());"))
    );
    let (status, refused) = post(
        app.clone(),
        "/v1/wasm/compose",
        json!({ "data": { "kind": "blocks", "fields": ["difficulty"] } }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        refused["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("`difficulty`"))
    );
    let (status, off) = post(app, "/admin/v1/wasm/build", json!({ "source": "" })).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(off["error"]["code"], "wasm_builds_disabled");
    assert!(
        off["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("api.allow_wasm_builds")
                && message.contains("leani processor create")),
        "{off}"
    );

    let (_directory, with) = node(
        &[],
        ApiConfig {
            wasm_build: Some(Arc::new(|source: String, language| {
                Box::pin(async move {
                    if source.contains("refuse") {
                        Err("not allowed".to_owned())
                    } else {
                        Ok(json!({ "built": true, "bytes": source.len(), "language": language }))
                    }
                })
            })),
            ..ApiConfig::default()
        },
    )
    .await;
    let local = Some("127.0.0.1:52000");
    assert_eq!(
        post_from(
            with.clone(),
            "/admin/v1/wasm/build",
            json!({ "source": "abc" }),
            local,
            &[]
        )
        .await,
        (
            StatusCode::OK,
            json!({ "built": true, "bytes": 3, "language": "rust" })
        )
    );
    assert_eq!(
        post_from(
            with.clone(),
            "/admin/v1/wasm/build",
            json!({ "source": "refuse" }),
            local,
            &[]
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn rust_builds_only_for_this_machine_and_typescript_for_any_client() {
    let (_directory, with) = node(
        &[],
        ApiConfig {
            wasm_build: Some(Arc::new(|source: String, language| {
                Box::pin(async move {
                    Ok(json!({ "built": true, "bytes": source.len(), "language": language }))
                })
            })),
            ..ApiConfig::default()
        },
    )
    .await;
    for (peer, headers) in [
        (None, vec![]),
        (Some("203.0.113.7:4000"), vec![]),
        (
            Some("127.0.0.1:52000"),
            vec![("x-forwarded-for", "203.0.113.7")],
        ),
        (
            Some("127.0.0.1:52000"),
            vec![("forwarded", "for=203.0.113.7")],
        ),
        // Addressed to another name: a proxy passing on the client's host.
        (Some("[::1]:52000"), vec![("host", "10.0.0.5:8080")]),
    ] {
        let (status, refused) = post_from(
            with.clone(),
            "/admin/v1/wasm/build",
            json!({ "source": "abc" }),
            peer,
            &headers,
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{peer:?} {headers:?}");
        assert!(
            refused["error"]["message"]
                .as_str()
                .is_some_and(|message| message.contains("its own machine")),
            "{refused}"
        );
        assert_eq!(
            post_from(
                with.clone(),
                "/admin/v1/wasm/build",
                json!({ "source": "abc", "language": "typescript" }),
                peer,
                &headers,
            )
            .await
            .1["language"],
            "typescript"
        );
    }
    assert_eq!(
        post_from(
            with,
            "/admin/v1/wasm/build",
            json!({ "source": "abc" }),
            Some("[::1]:52000"),
            &[("host", "localhost:8080")],
        )
        .await
        .0,
        StatusCode::OK,
        "an SSH tunnel arrives from loopback, addressed to localhost"
    );
}

#[tokio::test]
async fn templates_carry_their_rust_and_typescript_source() {
    let (_directory, app) = node(&[], ApiConfig::default()).await;
    let response = app
        .oneshot(
            Request::get("/v1/wasm/templates")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let templates: Value = serde_json::from_slice(&body).expect("JSON");
    for template in templates["data"].as_array().expect("templates") {
        assert!(
            template["sources"]["rust"]
                .as_str()
                .is_some_and(|source| source.contains("export_processor!")),
            "{}",
            template["name"]
        );
        assert!(
            template["sources"]["typescript"]
                .as_str()
                .is_some_and(|source| source.contains("export * from \"./leani/exports\";")),
            "{}",
            template["name"]
        );
    }
}
