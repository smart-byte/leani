//! The container profile starts on a volume an earlier release left behind.
#![cfg(unix)]

use std::{
    fs,
    io::{Read as _, Write as _},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use leani::{Config, ProcessorRegistry};
use leani_store_sqlite::{JobRecord, JobState, ProcessorRunState, SqliteStore, StoreConfig};

/// The job an rc.1 container left queued for its blobs processor.
const RC1_JOB: &str = "materialization:blobs-container:rc1";

/// Kills the node when a test ends before it exited.
struct Node {
    child: Child,
    log: PathBuf,
}

impl Node {
    fn log(&self) -> String {
        fs::read_to_string(&self.log).unwrap_or_default()
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn repository() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// `deploy/container.toml` with its data in `directory` and its listeners on
/// free loopback ports. Its on-demand history and disabled RPC history keep
/// the node off the network.
fn container_config(directory: &Path, instance: Option<&str>) -> (PathBuf, u16) {
    let profile = fs::read_to_string(repository().join("deploy/container.toml"))
        .expect("read the container profile");
    let mut profile: toml::Table = toml::from_str(&profile).expect("container profile");
    // Hold all three probes until the ports are chosen: dropping one before
    // binding the next can give two listeners the same port.
    let probes = [0; 3].map(|_| TcpListener::bind("127.0.0.1:0").expect("port probe"));
    let [api, http, ws] = probes
        .each_ref()
        .map(|probe| probe.local_addr().expect("probed address").port());
    profile["data_dir"] = directory.join("data").display().to_string().into();
    if let Some(instance) = instance {
        profile["processors"].as_array_mut().expect("processors")[0]
            .as_table_mut()
            .expect("processor")
            .insert("instance".to_owned(), instance.into());
    }
    let rpc = profile["rpc"].as_table_mut().expect("rpc");
    rpc.insert("http_bind".to_owned(), format!("127.0.0.1:{http}").into());
    rpc.insert("ws_bind".to_owned(), format!("127.0.0.1:{ws}").into());
    rpc.insert("historical_mode".to_owned(), "disabled".into());
    profile["api"]
        .as_table_mut()
        .expect("api")
        .insert("bind".to_owned(), format!("127.0.0.1:{api}").into());
    let path = directory.join(format!("{}.toml", instance.unwrap_or("container")));
    fs::write(&path, toml::to_string(&profile).expect("encode profile")).expect("write profile");
    (path, api)
}

fn live(api: u16) -> bool {
    let Ok(mut stream) = TcpStream::connect(("127.0.0.1", api)) else {
        return false;
    };
    let request =
        format!("GET /health/live HTTP/1.1\r\nHost: 127.0.0.1:{api}\r\nConnection: close\r\n\r\n");
    let mut response = String::new();
    stream.write_all(request.as_bytes()).is_ok()
        && stream.read_to_string(&mut response).is_ok()
        && response.starts_with("HTTP/1.1 200")
}

#[test]
fn a_missing_api_token_refuses_before_the_store_opens() {
    // Opening the store migrates an rc.1 volume, which the rc.1 image then
    // refuses. A start refused for its token must not have touched it.
    let directory = tempfile::tempdir().expect("temporary directory");
    let (config_path, _) = container_config(directory.path(), None);
    let output = Command::new(env!("CARGO_BIN_EXE_leani"))
        .arg("--config")
        .arg(&config_path)
        .arg("serve")
        .env_remove("LEANI_API_TOKEN")
        .stdin(Stdio::null())
        .output()
        .expect("run leani serve");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "{stderr}");
    assert!(stderr.contains("LEANI_API_TOKEN"), "{stderr}");
    assert!(!directory.path().join("data/leani.sqlite").exists());
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn the_container_profile_starts_beside_the_instance_an_earlier_release_left() {
    // Final review U1: the profile kept rc.1's `blobs-container` instance at
    // a new processor version, which the store refuses. The container then
    // restarted forever, and its volume opened with neither image.
    let directory = tempfile::tempdir().expect("temporary directory");
    let (config_path, api) = container_config(directory.path(), None);
    let config = Config::load(&config_path)
        .expect("container profile loads")
        .validate()
        .expect("container profile validates")
        .into_inner();
    let configured = &config.processors[0];
    assert_ne!(
        configured.instance, "blobs-container",
        "the profile reuses rc.1's instance for another processor version"
    );
    // A Xatu backfill holds about 450 MiB per active chunk, and the compose
    // file limits the container to 2 GiB.
    let active_chunks =
        u64::try_from(config.budgets.history_pipeline.maximum_active_chunks).expect("chunks");
    assert!(
        active_chunks * 450 * 1024 * 1024 <= config.budgets.memory_bytes,
        "the profile's backfill reads outgrow its memory budget"
    );

    // What rc.1 left in the volume: its blobs processor at 1.4.0 under
    // `blobs-container`, with a queued materialization job.
    let (earlier_path, _) = container_config(directory.path(), Some("blobs-container"));
    let earlier_config = Config::load(&earlier_path)
        .expect("earlier profile loads")
        .validate()
        .expect("earlier profile validates")
        .into_inner();
    let earlier_processor = ProcessorRegistry::standard()
        .instantiate(&earlier_config.processors[0], 1)
        .expect("earlier processor");
    let mut earlier = earlier_processor.descriptor().clone();
    earlier.version = "1.4.0".parse().expect("version");
    earlier.code_hash = leani_primitives::BlockHash::new([0x14; 32]);
    let database = config.data_dir.join("leani.sqlite");
    let store = SqliteStore::open(StoreConfig::new(&database))
        .await
        .expect("volume store");
    store
        .register_processor(&earlier)
        .await
        .expect("rc.1 registration");
    let job = leani_runtime::BackfillJob::for_processor(
        RC1_JOB,
        earlier_processor.as_ref(),
        leani_primitives::ChainId(1),
        leani_primitives::BlockRange::new(
            leani_primitives::BlockNumber(19_426_589),
            leani_primitives::BlockNumber(19_426_600),
        )
        .expect("range"),
        leani_source_api::VerificationPolicy::TrustedDataset,
    )
    .expect("rc.1 job");
    let queued = JobRecord {
        id: RC1_JOB.to_owned(),
        kind: leani_runtime::HistoricalJobOwner::Materialization
            .job_kind()
            .to_owned(),
        state: JobState::Queued,
        payload: serde_json::to_vec(&job).expect("job payload"),
        checkpoint: None,
        attempts: 0,
        updated_at_unix_ms: 1,
    };
    store.save_job(&queued).await.expect("rc.1 job");
    drop(store);

    let log = directory.path().join("node.log");
    let output = fs::File::create(&log).expect("node log");
    let child = Command::new(env!("CARGO_BIN_EXE_leani"))
        .arg("--config")
        .arg(&config_path)
        .arg("serve")
        .env("LEANI_API_TOKEN", "container-upgrade-test-token-0123456789")
        .stdin(Stdio::null())
        .stdout(output.try_clone().expect("node log"))
        .stderr(output)
        .spawn()
        .expect("start leani serve");
    let mut node = Node { child, log };
    let deadline = Instant::now() + Duration::from_secs(30);
    // Live, and past the scheduler's first look at the stored jobs.
    while !(live(api) && node.log().contains(RC1_JOB)) {
        if let Some(status) = node.child.try_wait().expect("node status") {
            panic!("leani serve exited with {status}:\n{}", node.log());
        }
        assert!(
            Instant::now() < deadline,
            "leani serve never became live:\n{}",
            node.log()
        );
        thread::sleep(Duration::from_millis(50));
    }
    let status = Command::new("kill")
        .args(["-INT", &node.child.id().to_string()])
        .status()
        .expect("send SIGINT");
    assert!(status.success(), "kill -INT failed");
    let deadline = Instant::now() + Duration::from_secs(30);
    while node.child.try_wait().expect("node status").is_none() {
        assert!(Instant::now() < deadline, "leani serve did not stop");
        thread::sleep(Duration::from_millis(20));
    }
    let logged = node.log();
    assert!(
        !logged.contains("conflicts with its stored descriptor"),
        "{logged}"
    );

    // The earlier instance stayed inert: registered as it was, never run.
    let store = SqliteStore::open(StoreConfig::new(&database))
        .await
        .expect("reopen the volume store");
    let state = store
        .processor_runtime_state(&earlier)
        .await
        .expect("earlier runtime state");
    assert_eq!(state.state, ProcessorRunState::Running);
    assert!(
        store
            .processor_cursor(&earlier)
            .await
            .expect("cursor")
            .is_none()
    );
    let untouched = store.job(RC1_JOB).await.expect("job").expect("job kept");
    assert_eq!(
        (
            untouched.state,
            untouched.attempts,
            untouched.updated_at_unix_ms
        ),
        (JobState::Queued, 0, 1)
    );
}
