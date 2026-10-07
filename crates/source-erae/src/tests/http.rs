//! HTTP behavior of bounded lookahead through the public source interface.

use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use super::*;

struct StallingMirror {
    url: Url,
    blocked: Arc<tokio::sync::Notify>,
    closed: Arc<tokio::sync::Notify>,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for StallingMirror {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl StallingMirror {
    async fn new(name: &str, object: Vec<u8>) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listen");
        let url = Url::parse(&format!(
            "http://{}/",
            listener.local_addr().expect("address")
        ))
        .expect("URL");
        let blocked = Arc::new(tokio::sync::Notify::new());
        let closed = Arc::new(tokio::sync::Notify::new());
        let signals = (Arc::clone(&blocked), Arc::clone(&closed));
        let catalog = catalog_text(&[name]);
        let object = Arc::new(object);
        let server = tokio::spawn(async move {
            let reads = Arc::new(AtomicUsize::new(0));
            let mut clients = tokio::task::JoinSet::new();
            while let Ok((stream, _)) = listener.accept().await {
                let object = Arc::clone(&object);
                let catalog = catalog.clone();
                let reads = Arc::clone(&reads);
                let signals = (Arc::clone(&signals.0), Arc::clone(&signals.1));
                clients.spawn(serve(stream, object, catalog, reads, signals));
            }
        });
        Self {
            url,
            blocked,
            closed,
            server,
        }
    }
}

async fn serve(
    mut stream: tokio::net::TcpStream,
    object: Arc<Vec<u8>>,
    catalog: String,
    reads: Arc<AtomicUsize>,
    (blocked, closed): (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>),
) {
    let mut request = Vec::new();
    let mut buffer = [0; 1_024];
    while !request.windows(4).any(|window| window == b"\r\n\r\n") {
        let read = stream.read(&mut buffer).await.expect("request");
        assert!(read > 0, "request ended before its headers");
        request.extend_from_slice(&buffer[..read]);
    }
    let request = String::from_utf8(request)
        .expect("HTTP text")
        .to_ascii_lowercase();
    let head = request.starts_with("head ");
    let range = request
        .lines()
        .find_map(|line| line.strip_prefix("range: bytes="));
    let (status, body, stall) = if let Some(range) = range {
        let (start, end) = range.split_once('-').expect("range");
        let start = start.parse::<usize>().expect("start");
        let end = end.parse::<usize>().expect("end");
        let layout = index_layout(
            u64::try_from(object.len()).expect("length"),
            &object[object.len() - 16..],
        )
        .expect("index");
        let data_read = end - start + 1 > 8 && u64::try_from(end).expect("end") < layout.position;
        let stall = data_read && reads.fetch_add(1, Ordering::SeqCst) > 0;
        ("206 Partial Content", &object[start..=end], stall)
    } else if request.contains(CATALOG_FILE) {
        ("200 OK", catalog.as_bytes(), false)
    } else {
        ("200 OK", object.as_slice(), false)
    };
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream
        .write_all(response.as_bytes())
        .await
        .expect("headers");
    if stall {
        blocked.notify_one();
        assert_eq!(stream.read(&mut buffer).await.expect("cancelled read"), 0);
        closed.notify_one();
    } else if !head {
        stream.write_all(body).await.expect("body");
    }
}

#[tokio::test]
async fn dropping_a_stream_cancels_lookahead_while_its_http_body_is_stalled() {
    let compressed_body =
        reth_era::common::compression::snappy_compress(&alloy_rlp::encode(BlockBody::default()))
            .expect("body");
    let mut parent_hash = alloy_primitives::B256::ZERO;
    let blocks = (0..512)
        .map(|number| {
            let header = Header {
                number,
                parent_hash,
                ..Header::default()
            };
            parent_hash = header.hash_slow();
            vec![
                (
                    COMPRESSED_HEADER,
                    reth_era::common::compression::snappy_compress(&alloy_rlp::encode(header))
                        .expect("header"),
                ),
                (COMPRESSED_BODY, compressed_body.clone()),
            ]
        })
        .collect::<Vec<_>>();
    let name = format!(
        "mainnet-00000-{}.erae",
        hex::encode(&parent_hash.as_slice()[..4])
    );
    let mirror = StallingMirror::new(&name, erae_object(0, &blocks)).await;
    let mut config = EraeConfig::public_mainnet().expect("config");
    config.base_url = mirror.url.clone();
    let source = EraeSource::new(config).expect("source");
    let plan = source
        .plan(&request(range(0, 511), headers_and_bodies()))
        .await
        .expect("plan");
    let cancellation = CancellationToken::new();
    let mut frames = source
        .open(
            &plan.chunks[0],
            SourceBudget {
                max_buffered_frames: 1,
                ..budget(64 << 20, 1 << 20)
            },
            cancellation.clone(),
        )
        .await
        .expect("open");
    let first = frames.try_next().await.expect("read").expect("first frame");
    assert_eq!(first.block.number, BlockNumber(0));
    tokio::time::timeout(Duration::from_secs(5), mirror.blocked.notified())
        .await
        .expect("next download overlaps a paused consumer");
    drop(frames);
    tokio::time::timeout(Duration::from_secs(5), mirror.closed.notified())
        .await
        .expect("dropping the stream closes the stalled response");
    assert!(
        !cancellation.is_cancelled(),
        "a stream only cancels its child token"
    );
}
