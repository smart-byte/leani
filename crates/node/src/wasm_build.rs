//! Build a processor's source into a Wasm module on this host, for the
//! playground's editor: a development feature, off unless
//! `api.allow_wasm_builds` is set.
//!
//! One project per language in the data directory takes each source in
//! turn, so a rebuild compiles only the processor. Builds run one at a time
//! with the host's own toolchain: cargo for Rust, Node.js for TypeScript.

use std::{
    path::{Path, PathBuf},
    process::{Output, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};

use leani_processor_wasm::scaffold::{self, Language};
use serde_json::{Value, json};
use tokio::sync::Mutex;

/// A first build also fetches and compiles the SDK's dependencies.
const BUILD_TIMEOUT: Duration = Duration::from_secs(300);
const NAME: &str = "playground";
const NO_NODE: &str = "Building TypeScript needs Node.js 20 or later, with npm, on the node's host: install it from https://nodejs.org.";

#[derive(Clone, Debug)]
pub(crate) struct WasmBuilder {
    directory: PathBuf,
    lock: Arc<Mutex<()>>,
}

impl WasmBuilder {
    pub(crate) fn new(data_dir: &Path) -> Self {
        // Absolute, as the compilers run inside it and write beneath it.
        let data_dir = std::path::absolute(data_dir).unwrap_or_else(|_| data_dir.to_path_buf());
        Self {
            directory: data_dir.join("playground"),
            lock: Arc::default(),
        }
    }

    /// Build `source`: the module as hex when it compiles, the compiler's
    /// diagnostics either way, or why this host cannot build.
    ///
    /// # Errors
    ///
    /// Returns a message for source this host refuses to compile.
    pub(crate) async fn build(&self, source: String, language: Language) -> Result<Value, String> {
        scaffold::check_source(&source, language)?;
        let _one_at_a_time = self.lock.lock().await;
        let started = Instant::now();
        let directory = self.directory.join(match language {
            Language::Rust => "wasm-build",
            Language::TypeScript => "wasm-build-ts",
        });
        let mut files = scaffold::project_for(NAME, language, &source)?.files;
        if language == Language::TypeScript {
            files.push((
                "build.mjs".to_owned(),
                scaffold::TYPESCRIPT_BUILD.to_owned(),
            ));
        }
        for (path, text) in &files {
            write_if_changed(&directory.join(path), text)
                .map_err(|error| format!("write the build project: {error}"))?;
        }
        let mut result = match language {
            Language::Rust => cargo(&directory).await,
            Language::TypeScript => typescript(&directory).await,
        }
        .unwrap_or_else(|problem| problem);
        result["elapsedMs"] = u64::try_from(started.elapsed().as_millis())
            .unwrap_or(u64::MAX)
            .into();
        Ok(result)
    }
}

async fn cargo(directory: &Path) -> Result<Value, Value> {
    let target = directory.join("target");
    let mut command =
        tokio::process::Command::new(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()));
    command
        .args([
            "build",
            "--release",
            "--target",
            "wasm32-unknown-unknown",
            "--message-format=json",
        ])
        .current_dir(directory)
        .env("CARGO_TARGET_DIR", &target)
        // Faster rebuilds than the crate's release profile, for a module
        // that is only tried out.
        .env("CARGO_PROFILE_RELEASE_LTO", "false")
        .env("CARGO_PROFILE_RELEASE_CODEGEN_UNITS", "16")
        .env("RUSTFLAGS", "-C link-arg=-zstack-size=262144");
    let output = run(
        command,
        "Building needs Rust's cargo on the node's host: install Rust from https://rustup.rs, then run `rustup target add wasm32-unknown-unknown`.",
        BUILD_TIMEOUT,
    )
    .await?;
    let diagnostics = diagnostics(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    if !output.status.success() {
        if stderr.contains("wasm32-unknown-unknown")
            && (stderr.contains("may not be installed")
                || stderr.contains("can't find crate for `core`"))
        {
            return Err(toolchain(
                "This host's Rust lacks the Wasm target: run `rustup target add wasm32-unknown-unknown`.",
            ));
        }
        return Err(not_built(&diagnostics, "cargo", &stderr));
    }
    module(
        &target.join(format!("wasm32-unknown-unknown/release/{NAME}.wasm")),
        &diagnostics,
    )
}

async fn typescript(directory: &Path) -> Result<Value, Value> {
    let installed =
        std::fs::read_to_string(directory.join("node_modules/assemblyscript/package.json"))
            .ok()
            .and_then(|text| serde_json::from_str::<Value>(&text).ok())
            .is_some_and(|package| package["version"] == scaffold::ASSEMBLYSCRIPT);
    if !installed {
        let mut install = tokio::process::Command::new("npm");
        install
            .args(["ci", "--ignore-scripts", "--no-audit", "--no-fund"])
            .current_dir(directory);
        let output = run(install, NO_NODE, BUILD_TIMEOUT).await?;
        if !output.status.success() {
            // Remote clients may build TypeScript, and npm's output names
            // this host's paths: it goes to the node's log only.
            tracing::warn!(
                stderr = %tail(&String::from_utf8_lossy(&output.stderr)),
                "npm could not install the AssemblyScript compiler"
            );
            return Err(toolchain(
                "npm could not install the AssemblyScript compiler; the node's log has npm's output",
            ));
        }
    }
    let out = directory.join("build").join(format!("{NAME}.wasm"));
    let _ = std::fs::remove_file(&out);
    let mut command = tokio::process::Command::new("node");
    command
        // Remote clients may build TypeScript: bound the compiler's heap.
        .args([
            "--max-old-space-size=2048",
            "build.mjs",
            "assembly/index.ts",
            "assembly/leani",
        ])
        .arg(&out)
        .current_dir(directory);
    std::fs::create_dir_all(directory.join("build"))
        .map_err(|error| toolchain(&format!("create the build directory: {error}")))?;
    let output = run(command, NO_NODE, BUILD_TIMEOUT).await?;
    let report = String::from_utf8_lossy(&output.stdout)
        .lines()
        .rev()
        .find_map(|line| serde_json::from_str::<Value>(line).ok());
    let Some(report) = report else {
        tracing::warn!(
            stderr = %tail(&String::from_utf8_lossy(&output.stderr)),
            "the AssemblyScript compiler stopped without a report"
        );
        return Err(not_built(
            &[],
            "compiler",
            "the compiler stopped without a report; the node's log has its output",
        ));
    };
    let diagnostics = report["diagnostics"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    if report["built"] != true {
        return Err(not_built(&diagnostics, "compiler", ""));
    }
    module(&out, &diagnostics)
}

async fn run(
    mut command: tokio::process::Command,
    missing: &str,
    limit: Duration,
) -> Result<Output, Value> {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(unix)]
    command.process_group(0);
    let program = command
        .as_std()
        .get_program()
        .to_string_lossy()
        .into_owned();
    let child = command.spawn().map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            toolchain(missing)
        } else {
            toolchain(&format!("start {program}: {error}"))
        }
    })?;
    let mut group = Group(child.id());
    match tokio::time::timeout(limit, child.wait_with_output()).await {
        Ok(Ok(output)) => {
            // It ended, and with it everything it started.
            group.0 = None;
            Ok(output)
        }
        Ok(Err(error)) => Err(toolchain(&format!("run {program}: {error}"))),
        Err(_) => Err(json!({
            "built": false,
            "problem": "timeout",
            "message": format!("the build took over {} s", limit.as_secs()),
            "diagnostics": [],
        })),
    }
}

/// A build's process group, which a build that ends early, on a timeout or
/// when its request goes away, takes down whole: killing the command alone
/// would leave what it started, such as cargo's rustc, running beside the
/// next build.
struct Group(Option<u32>);

impl Drop for Group {
    fn drop(&mut self) {
        #[cfg(unix)]
        if let Some(group) = self
            .0
            .and_then(|id| rustix::process::Pid::from_raw(i32::try_from(id).ok()?))
        {
            // A group already gone is fine.
            let _ = rustix::process::kill_process_group(group, rustix::process::Signal::KILL);
        }
    }
}

fn module(path: &Path, diagnostics: &[Value]) -> Result<Value, Value> {
    let module = std::fs::read(path)
        .map_err(|error| toolchain(&format!("read the built module: {error}")))?;
    Ok(json!({
        "built": true,
        "module": hex::encode(&module),
        "moduleHash": format!("0x{}", blake3::hash(&module).to_hex()),
        "moduleBytes": module.len(),
        "diagnostics": diagnostics,
    }))
}

fn not_built(diagnostics: &[Value], problem: &str, stderr: &str) -> Value {
    let compiled = diagnostics
        .iter()
        .any(|diagnostic| diagnostic["level"] == "error");
    let message = if compiled {
        "the source does not compile".to_owned()
    } else if stderr.trim().is_empty() {
        "the build failed without a message".to_owned()
    } else {
        tail(stderr)
    };
    json!({
        "built": false,
        "problem": if compiled { "compile" } else { problem },
        "message": message,
        "diagnostics": diagnostics,
    })
}

fn toolchain(message: &str) -> Value {
    json!({ "built": false, "problem": "toolchain", "message": message, "diagnostics": [] })
}

/// Write `text` unless the file holds it already, which keeps the
/// compilers' fingerprints of unchanged files.
fn write_if_changed(path: &Path, text: &str) -> std::io::Result<()> {
    if std::fs::read_to_string(path).is_ok_and(|current| current == text) {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, text)
}

fn diagnostics(stdout: &[u8]) -> Vec<Value> {
    stdout
        .split(|byte| *byte == b'\n')
        .filter_map(|line| serde_json::from_slice::<Value>(line).ok())
        .filter(|message| {
            message["reason"] == "compiler-message"
                && message["target"]["name"] == NAME
                && matches!(
                    message["message"]["level"].as_str(),
                    Some("error" | "warning")
                )
        })
        .map(|message| {
            let message = &message["message"];
            let span = message["spans"]
                .as_array()
                .and_then(|spans| spans.iter().find(|span| span["is_primary"] == true));
            json!({
                "level": message["level"],
                "message": message["message"],
                "line": span.map(|span| span["line_start"].clone()),
                "column": span.map(|span| span["column_start"].clone()),
                "rendered": message["rendered"],
            })
        })
        .collect()
}

fn tail(stderr: &str) -> String {
    let lines = stderr.lines().collect::<Vec<_>>();
    lines[lines.len().saturating_sub(12)..].join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diagnostics_keep_the_processors_own_errors_and_warnings() {
        let stdout = [
            json!({ "reason": "compiler-message", "target": { "name": "serde" },
                    "message": { "level": "warning", "message": "theirs", "spans": [], "rendered": "" } }),
            json!({ "reason": "compiler-message", "target": { "name": "playground" },
                    "message": { "level": "error", "message": "cannot find value `x`", "rendered": "error: ...",
                                 "spans": [{ "is_primary": true, "line_start": 7, "column_start": 13 }] } }),
            json!({ "reason": "build-finished", "success": false }),
        ]
        .iter()
        .map(Value::to_string)
        .collect::<Vec<_>>()
        .join("\n");
        let found = diagnostics(stdout.as_bytes());
        assert_eq!(found.len(), 1, "{found:?}");
        assert_eq!(found[0]["line"], 7);
        assert_eq!(found[0]["level"], "error");
    }

    #[tokio::test]
    async fn sources_that_read_host_files_are_refused_before_cargo_runs() {
        let directory = tempfile::tempdir().expect("directory");
        let builder = WasmBuilder::new(directory.path());
        // Aliased, the macro still reads the file into the diagnostics.
        let refused = builder
            .build(
                "use std::include_str as leak; compile_error!(leak!(\"/etc/hosts\"));".to_owned(),
                Language::Rust,
            )
            .await
            .expect_err("refused");
        assert!(refused.contains("`include_str`"), "{refused}");
        assert!(
            !directory.path().join("playground").exists(),
            "nothing written"
        );
    }

    #[test]
    fn a_relative_data_directory_builds_in_an_absolute_one() {
        let builder = WasmBuilder::new(Path::new("data"));
        assert!(
            builder.directory.is_absolute(),
            "{}",
            builder.directory.display()
        );
        assert!(builder.directory.ends_with("data/playground"));
    }

    #[test]
    fn a_failure_without_errors_reports_the_toolchains_last_lines() {
        let failed = not_built(&[], "compiler", "one\ntwo\nnpm ERR! offline");
        assert_eq!(failed["problem"], "compiler");
        assert!(
            failed["message"]
                .as_str()
                .is_some_and(|message| message.ends_with("offline"))
        );
        let compiled = not_built(
            &[json!({ "level": "error", "message": "bad" })],
            "compiler",
            "",
        );
        assert_eq!(compiled["problem"], "compile");
    }

    #[tokio::test]
    #[ignore = "builds with Node.js and the compiler the SDK pins"]
    async fn typescript_builds_report_errors_where_the_source_has_them() {
        let directory = tempfile::tempdir().expect("directory");
        let builder = WasmBuilder::new(directory.path());
        let template = leani_processor_wasm::TEMPLATES[0].typescript;
        let built = builder
            .build(template.to_owned(), Language::TypeScript)
            .await
            .expect("built");
        assert_eq!(built["built"], true, "{built:#}");
        let broken = template.replacen(
            "const used = gasUsed.value;",
            "const used = gasUsed.valu;",
            1,
        );
        assert_ne!(broken, template);
        let failed = builder
            .build(broken, Language::TypeScript)
            .await
            .expect("a result");
        assert_eq!(failed["problem"], "compile", "{failed:#}");
        let error = &failed["diagnostics"][0];
        let line = template
            .lines()
            .position(|line| line.contains("const used = gasUsed.value;"))
            .expect("line")
            + 1;
        assert_eq!(error["line"], line, "{failed:#}");
        assert!(
            error["message"]
                .as_str()
                .is_some_and(|message| message.contains("valu")),
            "{failed:#}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_timed_out_build_ends_the_processes_it_started() {
        let directory = tempfile::tempdir().expect("directory");
        let started = directory.path().join("started.pid");
        let mut command = tokio::process::Command::new("sh");
        command.arg("-c").arg(format!(
            "sleep 60 & echo $! > '{}'; wait",
            started.display()
        ));
        // Long enough for `sh` to start its child on a loaded host.
        let timeout = run(command, "", Duration::from_secs(3))
            .await
            .expect_err("times out");
        assert_eq!(timeout["problem"], "timeout");
        let pid: i32 = std::fs::read_to_string(&started)
            .expect("the started process")
            .trim()
            .parse()
            .expect("a process id");
        let pid = rustix::process::Pid::from_raw(pid).expect("a process id");
        // A killed process lingers until its reaper takes it.
        for _ in 0..50 {
            if rustix::process::test_kill_process(pid).is_err() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("the build's own process {pid:?} outlived its timeout");
    }
}
