//! `leani serve` shuts down promptly while clients hold streams open.
#![cfg(unix)]

use std::{
    fs,
    io::{Read as _, Write as _},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    thread,
    time::{Duration, Instant},
};

/// Kills the node when a test ends before it exited.
struct Node {
    child: Child,
    log: PathBuf,
}

impl Node {
    fn log(&self) -> String {
        fs::read_to_string(&self.log).unwrap_or_default()
    }

    fn wait_for_exit(&mut self, within: Duration) -> Option<ExitStatus> {
        let deadline = Instant::now() + within;
        while Instant::now() < deadline {
            if let Some(status) = self.child.try_wait().expect("node status") {
                return Some(status);
            }
            thread::sleep(Duration::from_millis(20));
        }
        None
    }

    fn interrupt(&self) {
        let status = Command::new("kill")
            .args(["-INT", &self.child.id().to_string()])
            .status()
            .expect("send SIGINT");
        assert!(status.success(), "kill -INT failed");
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("port probe")
        .local_addr()
        .expect("probed address")
        .port()
}

/// A node serving one block-summary processor from its own store: no live
/// source, no finality, and no RPC history, so it never leaves the machine.
fn write_config(directory: &Path, api: u16, rpc: u16, websocket: u16) -> PathBuf {
    let path = directory.join("leani.toml");
    fs::write(
        &path,
        format!(
            r#"config_version = 1
data_dir = "{data_dir}"

[chain]
name = "ethereum-mainnet"
chain_id = 1

[budgets]
memory_bytes = 67108864
temporary_disk_bytes = 67108864
pending_delta_bytes = 16777216
recent_raw_soft_bytes = 16777216
recent_raw_hard_bytes = 33554432
source_concurrency = 2
mapper_concurrency = 2

[[sources.history]]
id = "xatu"
kind = "xatu"
priority = 10
trust = "trusted_dataset"

[sources.live]
kind = "disabled"
minimum_peers = 0

[finality]
kind = "disabled"
checkpoint = ""

[[processors]]
id = "block-summary"
instance = "shutdown-blocks"
version = "1.1.0"
history_mode = "on_demand"
start_block = 0
publish = "included_and_finalized"

[processors.state]
mode = "checkpointed"

[processors.output]
mode = "full"

[processors.delivery]
mode = "window"

[processors.checkpoint]
mode = "automatic"
keep = 3

[processors.undo]
mode = "unfinalized"
safety_blocks = 256

[rpc]
http_bind = "127.0.0.1:{rpc}"
ws_bind = "127.0.0.1:{websocket}"
historical_mode = "disabled"
transaction_locator = false
minimum_recent_blocks = 128

[api]
bind = "127.0.0.1:{api}"
"#,
            data_dir = directory.join("data").display(),
        ),
    )
    .expect("write configuration");
    path
}

fn start_node(directory: &Path) -> (Node, u16, u16) {
    let (api, rpc, websocket) = (free_port(), free_port(), free_port());
    let config = write_config(directory, api, rpc, websocket);
    let log = directory.join("node.log");
    let output = fs::File::create(&log).expect("node log");
    let child = Command::new(env!("CARGO_BIN_EXE_leani"))
        .arg("--config")
        .arg(&config)
        .arg("serve")
        .stdin(Stdio::null())
        .stdout(output.try_clone().expect("node log"))
        .stderr(output)
        .spawn()
        .expect("start leani serve");
    let mut node = Node { child, log };
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(status) = node.child.try_wait().expect("node status") {
            panic!("leani serve exited with {status}:\n{}", node.log());
        }
        if let Ok(mut stream) = TcpStream::connect(("127.0.0.1", api)) {
            let request = format!(
                "GET /health/live HTTP/1.1\r\nHost: 127.0.0.1:{api}\r\nConnection: close\r\n\r\n"
            );
            let mut response = String::new();
            if stream.write_all(request.as_bytes()).is_ok()
                && stream.read_to_string(&mut response).is_ok()
                && response.starts_with("HTTP/1.1 200")
            {
                return (node, api, websocket);
            }
        }
        assert!(
            Instant::now() < deadline,
            "leani serve never became live:\n{}",
            node.log()
        );
        thread::sleep(Duration::from_millis(50));
    }
}

/// Read from `stream` until what it sent contains `marker`.
fn read_until(stream: &mut TcpStream, marker: &str) -> String {
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("read timeout");
    let mut received = Vec::new();
    let mut buffer = [0_u8; 4_096];
    while !String::from_utf8_lossy(&received).contains(marker) {
        let read = stream.read(&mut buffer).expect("read from the node");
        assert_ne!(read, 0, "the node closed the connection before {marker:?}");
        received.extend_from_slice(&buffer[..read]);
    }
    String::from_utf8_lossy(&received).into_owned()
}

#[test]
fn a_shutdown_signal_ends_open_streams_and_the_node_exits_promptly() {
    // Audit M-N2: an open change stream held the graceful shutdown forever,
    // because its body ignored the node's cancellation.
    let directory = tempfile::tempdir().expect("temporary directory");
    let (mut node, api, websocket) = start_node(directory.path());

    let mut changes = TcpStream::connect(("127.0.0.1", api)).expect("connect to the API");
    write!(
        changes,
        "GET /v1/processors/shutdown-blocks/stream HTTP/1.1\r\nHost: 127.0.0.1:{api}\r\nAccept: text/event-stream\r\n\r\n"
    )
    .expect("open the change stream");
    read_until(&mut changes, "event: hello");

    let mut rpc = TcpStream::connect(("127.0.0.1", websocket)).expect("connect to RPC");
    write!(
        rpc,
        "GET / HTTP/1.1\r\nHost: 127.0.0.1:{websocket}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
    )
    .expect("open a WebSocket");
    let upgrade = read_until(&mut rpc, "\r\n\r\n");
    assert!(upgrade.starts_with("HTTP/1.1 101"), "{upgrade}");

    node.interrupt();

    // The change stream ends: its response completes and the connection
    // closes.
    changes
        .set_read_timeout(Some(Duration::from_secs(8)))
        .expect("read timeout");
    let mut rest = Vec::new();
    let ended = changes.read_to_end(&mut rest);
    assert!(
        ended.is_ok(),
        "the change stream stayed open after the shutdown signal: {ended:?}\n{}",
        node.log()
    );

    // The WebSocket connection gets a going-away close frame.
    rpc.set_read_timeout(Some(Duration::from_secs(5)))
        .expect("read timeout");
    let mut frame = [0_u8; 4];
    rpc.read_exact(&mut frame)
        .unwrap_or_else(|error| panic!("no WebSocket close frame: {error}\n{}", node.log()));
    assert_eq!(frame[0], 0x88, "not a final close frame: {frame:?}");
    assert_eq!(u16::from_be_bytes([frame[2], frame[3]]), 1001, "{frame:?}");

    // The node exits successfully, well within its shutdown deadline.
    let status = node
        .wait_for_exit(Duration::from_secs(5))
        .unwrap_or_else(|| panic!("leani serve did not exit:\n{}", node.log()));
    assert!(status.success(), "{status}\n{}", node.log());
}
