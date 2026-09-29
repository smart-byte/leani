//! `leani subscribe` attached to a loopback stand-in for a node's native API.

use std::{
    fs,
    io::{BufRead as _, BufReader, Write as _},
    net::{TcpListener, TcpStream},
    path::Path,
    process::{Child, Command, ExitStatus, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde_json::{Value, json};

/// Answers the routes `leani subscribe blocks` uses, as a node for
/// `chain_id` whose stream sends a hello and then one fresh block.
struct FakeNode {
    address: String,
    stop: Arc<AtomicBool>,
}

impl FakeNode {
    fn start(chain_id: u64) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("loopback listener");
        listener
            .set_nonblocking(true)
            .expect("non-blocking listener");
        let address = listener.local_addr().expect("listener address").to_string();
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = Arc::clone(&stop);
        thread::spawn(move || {
            while !stopped.load(Ordering::Relaxed) {
                if let Ok((stream, _)) = listener.accept() {
                    let stopped = Arc::clone(&stopped);
                    thread::spawn(move || answer(stream, chain_id, &stopped));
                } else {
                    thread::sleep(Duration::from_millis(10));
                }
            }
        });
        Self { address, stop }
    }

    fn url(&self) -> String {
        format!("http://{}/", self.address)
    }
}

impl Drop for FakeNode {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

fn answer(stream: TcpStream, chain_id: u64, stopped: &AtomicBool) {
    let _ = stream.set_nonblocking(false);
    let Ok(request_stream) = stream.try_clone() else {
        return;
    };
    let mut request = BufReader::new(request_stream);
    let mut request_line = String::new();
    if request.read_line(&mut request_line).is_err() {
        return;
    }
    loop {
        let mut header = String::new();
        match request.read_line(&mut header) {
            Ok(0) | Err(_) => break,
            Ok(_) if header == "\r\n" => break,
            Ok(_) => {}
        }
    }
    let path = request_line
        .split_whitespace()
        .nth(1)
        .unwrap_or("/")
        .to_owned();
    let mut stream = stream;
    if path.starts_with("/v1/processors/block-summary/stream") {
        let _ = write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n"
        );
        let _ = stream.write_all(sse_event("hello", &hello(chain_id)).as_bytes());
        let _ = stream.write_all(sse_event("apply", &block_envelope()).as_bytes());
        let _ = stream.flush();
        // Stay open like a node between blocks.
        let deadline = Instant::now() + Duration::from_secs(30);
        while !stopped.load(Ordering::Relaxed) && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(50));
        }
        return;
    }
    let (status, body) = match path.as_str() {
        "/health/live" => ("200 OK", json!({ "status": "live" })),
        "/v1/processors/block-summary/changes/head" => (
            "200 OK",
            json!({ "earliestSequence": "1", "latestSequence": "1", "cursor": "cursor-1" }),
        ),
        _ => (
            "404 Not Found",
            json!({ "error": { "code": "not_found", "message": "not found", "retryable": false } }),
        ),
    };
    let body = body.to_string();
    let _ = write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
}

fn sse_event(name: &str, data: &Value) -> String {
    format!("event: {name}\ndata: {data}\n\n")
}

fn hello(chain_id: u64) -> Value {
    json!({
        "apiVersion": "v1",
        "chainId": chain_id,
        "processor": { "id": "block-summary", "instance": "block-summary", "version": "1.1.0" },
        "coverage": { "chainId": chain_id, "finalizedThrough": null },
    })
}

fn block_envelope() -> Value {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_secs();
    let hash = format!("0x{}", "ab".repeat(32));
    let parent = format!("0x{}", "00".repeat(32));
    json!({
        "sequence": "2",
        "cursor": "cursor-2",
        "operation": "apply",
        "block": { "number": 100, "hash": hash, "parentHash": parent, "timestamp": now },
        "finality": "included",
        "kind": "ethereum.block.summary.put",
        "key": hash,
        "data": {
            "chainId": 1,
            "blockNumber": 100,
            "blockHash": hash,
            "parentHash": parent,
            "timestamp": now,
            "gasLimit": 60_000_000,
            "gasUsed": 30_000_000,
            "baseFeePerGas": null,
            "blobGasUsed": null,
            "excessBlobGas": null,
            "transactionCount": 3,
            "sizeBytes": null,
            "finality": "included",
        },
    })
}

/// A compact node configuration whose API listens on `address`.
fn starter_config(address: &str) -> String {
    format!(
        "config_version = 1\n\
         network = \"ethereum-mainnet\"\n\
         data_dir = \"./data\"\n\
         \n\
         [finality]\n\
         checkpoint = \"0x{}\"\n\
         checkpoint_slot = 1\n\
         endpoints = [\"https://beacon.example/\"]\n\
         \n\
         [blocks]\n\
         \n\
         [api]\n\
         bind = \"{address}\"\n",
        "11".repeat(32)
    )
}

fn subscribe(directory: &Path, arguments: &[&str]) -> Child {
    let mut command = Command::new(env!("CARGO_BIN_EXE_leani"));
    command
        .arg("subscribe")
        .args(arguments)
        .current_dir(directory)
        .env_remove("LEANI_CONFIG")
        .env_remove("LEANI_API_TOKEN")
        .env("XDG_DATA_HOME", directory.join("xdg"))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    // The stand-in node is on loopback; never route it through a proxy.
    for proxy in [
        "HTTP_PROXY",
        "http_proxy",
        "HTTPS_PROXY",
        "https_proxy",
        "ALL_PROXY",
        "all_proxy",
    ] {
        command.env_remove(proxy);
    }
    command.spawn().expect("run leani subscribe")
}

struct Finished {
    status: ExitStatus,
    stdout: String,
    stderr: String,
}

fn drain(stream: Option<impl std::io::Read + Send + 'static>) -> thread::JoinHandle<String> {
    thread::spawn(move || {
        let mut text = String::new();
        if let Some(mut stream) = stream {
            let _ = stream.read_to_string(&mut text);
        }
        text
    })
}

fn finish(mut child: Child) -> Finished {
    let stdout = drain(child.stdout.take());
    let stderr = drain(child.stderr.take());
    let deadline = Instant::now() + Duration::from_mins(1);
    let status = loop {
        if let Some(status) = child.try_wait().expect("poll leani subscribe") {
            break status;
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!(
                "leani subscribe did not exit:\n{}",
                stderr.join().unwrap_or_default()
            );
        }
        thread::sleep(Duration::from_millis(20));
    };
    Finished {
        status,
        stdout: stdout.join().unwrap_or_default(),
        stderr: stderr.join().unwrap_or_default(),
    }
}

#[test]
fn client_mode_with_an_endpoint_needs_no_local_config() {
    // Audit CLI-2: `--mode client --endpoint` loaded ./leani.toml first, so an
    // unrelated or broken local configuration stopped a remote subscription.
    let node = FakeNode::start(1);
    let directory = tempfile::tempdir().expect("working directory");
    fs::write(
        directory.path().join("leani.toml"),
        "not a Leani configuration",
    )
    .expect("broken configuration");
    let run = finish(subscribe(
        directory.path(),
        &[
            "blocks",
            "--mode",
            "client",
            "--endpoint",
            &node.url(),
            "--json",
            "--once",
        ],
    ));
    assert!(run.status.success(), "{}", run.stderr);
    let row: Value =
        serde_json::from_str(run.stdout.lines().next().expect("one row")).expect("a JSON row");
    assert_eq!(row["blockNumber"], 100, "{}", run.stdout);
}

#[test]
fn a_closed_stdout_ends_the_subscription_cleanly() {
    // Audit CLI-1: `println!` panicked when the reader went away, as with
    // `leani subscribe blocks | head -1`.
    let node = FakeNode::start(1);
    let directory = tempfile::tempdir().expect("working directory");
    let mut child = subscribe(
        directory.path(),
        &[
            "blocks",
            "--mode",
            "client",
            "--endpoint",
            &node.url(),
            "--format",
            "json",
        ],
    );
    drop(child.stdout.take());
    let run = finish(child);
    assert!(
        run.status.success(),
        "exit {:?}:\n{}",
        run.status,
        run.stderr
    );
    assert!(!run.stderr.contains("panicked"), "{}", run.stderr);
}

#[test]
fn auto_mode_refuses_a_node_serving_another_chain() {
    // Audit CLI-5: auto mode attached to whatever answered /health/live and
    // printed its stream without checking the hello.
    let node = FakeNode::start(5);
    let directory = tempfile::tempdir().expect("working directory");
    fs::write(
        directory.path().join("leani.toml"),
        starter_config(&node.address),
    )
    .expect("node configuration");
    let run = finish(subscribe(
        directory.path(),
        &["blocks", "--format", "json", "--once"],
    ));
    assert!(!run.status.success(), "{}", run.stdout);
    assert!(run.stdout.is_empty(), "{}", run.stdout);
    assert!(run.stderr.contains("chain 5"), "{}", run.stderr);
}

/// A node that accepts connections and never answers, so no update ever
/// arrives. The receiver hears of each connection.
fn silent_node() -> (String, std::sync::mpsc::Receiver<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("loopback listener");
    let url = format!(
        "http://{}/",
        listener.local_addr().expect("listener address")
    );
    let (connected, connections) = std::sync::mpsc::channel();
    thread::spawn(move || {
        let mut held = Vec::new();
        for stream in listener.incoming().flatten() {
            held.push(stream);
            let _ = connected.send(());
        }
    });
    (url, connections)
}

#[test]
fn a_timeout_ends_a_follow_but_fails_an_unsatisfied_once() {
    let (node, _connections) = silent_node();
    let directory = tempfile::tempdir().expect("working directory");
    for (once, code) in [(true, 124), (false, 0)] {
        let mut arguments = vec![
            "blocks",
            "--mode",
            "client",
            "--endpoint",
            &node,
            "--json",
            "--timeout",
            "1s",
        ];
        if once {
            arguments.push("--once");
        }
        let run = finish(subscribe(directory.path(), &arguments));
        assert_eq!(run.status.code(), Some(code), "once={once}: {}", run.stderr);
        assert!(run.stdout.is_empty(), "{}", run.stdout);
    }
}

#[cfg(unix)]
#[test]
fn sigterm_stops_an_unsatisfied_once_as_interrupted() {
    let (node, connections) = silent_node();
    let directory = tempfile::tempdir().expect("working directory");
    let child = subscribe(
        directory.path(),
        &["blocks", "--mode", "client", "--endpoint", &node, "--once"],
    );
    // Its first request comes after the signal handlers are installed.
    connections
        .recv_timeout(Duration::from_mins(1))
        .expect("leani subscribe connects");
    let killed = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .expect("send SIGTERM");
    assert!(killed.success());
    let run = finish(child);
    assert_eq!(run.status.code(), Some(130), "{}", run.stderr);
}
