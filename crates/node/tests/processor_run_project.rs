#![cfg(unix)]
//! `leani processor run <project>` builds before it runs, and keeps every
//! build byte off stdout, which carries the changes. Fake toolchains write a
//! module that is not Wasm, so each run stops when it loads the module,
//! before any source is asked for blocks.

use std::{os::unix::fs::PermissionsExt, path::Path, process::Command};

fn script(path: &Path, body: &str) {
    std::fs::write(path, format!("#!/bin/sh\n{body}\n")).expect("script");
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).expect("mode");
}

// The quickstart's configuration; the run replaces its processors and data
// directory.
const CONFIG: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../config/modes/windowed.toml"
);

fn leani(directory: &Path, project: &str, bin: &Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_leani"))
        .args([
            "--config",
            CONFIG,
            "processor",
            "run",
            project,
            "--from-block",
            "1",
            "--to-block",
            "1",
        ])
        .current_dir(directory)
        .env(
            "PATH",
            format!(
                "{}:{}",
                bin.display(),
                std::env::var("PATH").unwrap_or_default()
            ),
        )
        .env("CARGO", bin.join("cargo"))
        .output()
        .expect("leani runs")
}

#[test]
fn a_project_is_built_off_stdout_before_its_module_runs() {
    let root = tempfile::tempdir().expect("directory");
    let bin = root.path().join("bin");
    std::fs::create_dir(&bin).expect("bin");

    // TypeScript: npm reports on stdout, which the run must not pass on.
    let package = root.path().join("app/demo");
    std::fs::create_dir_all(&package).expect("package");
    std::fs::write(package.join("package.json"), r#"{"name":"demo"}"#).expect("package.json");
    script(
        &bin.join("npm"),
        "echo \"npm says $*\"\nmkdir -p build && printf nope > build/demo.wasm",
    );
    let run = leani(root.path(), "app/demo", &bin);
    let stderr = String::from_utf8_lossy(&run.stderr);
    assert!(!run.status.success(), "{stderr}");
    assert!(
        run.stdout.is_empty(),
        "stdout: {}",
        String::from_utf8_lossy(&run.stdout)
    );
    assert!(
        stderr.contains("npm says install") && stderr.contains("npm says run build"),
        "{stderr}"
    );
    assert!(stderr.contains("invalid Wasm module"), "{stderr}");

    // Rust, from another directory: the artifact message names a module
    // outside the project, as under a shared CARGO_TARGET_DIR.
    let crate_dir = root.path().join("app/gas");
    std::fs::create_dir_all(&crate_dir).expect("crate");
    std::fs::write(crate_dir.join("Cargo.toml"), "").expect("Cargo.toml");
    let module = root.path().join("shared-target/gas.wasm");
    script(
        &bin.join("cargo"),
        &format!(
            "mkdir -p '{dir}' && printf nope > '{module}'\necho 'Compiling gas' >&2\necho '{{\"reason\":\"compiler-artifact\",\"target\":{{\"kind\":[\"cdylib\"]}},\"filenames\":[\"{module}\"]}}'",
            dir = module.parent().expect("dir").display(),
            module = module.display()
        ),
    );
    let run = leani(&root.path().join("app"), "gas", &bin);
    let stderr = String::from_utf8_lossy(&run.stderr);
    assert!(!run.status.success(), "{stderr}");
    assert!(
        run.stdout.is_empty(),
        "stdout: {}",
        String::from_utf8_lossy(&run.stdout)
    );
    assert!(stderr.contains("Compiling gas"), "{stderr}");
    assert!(stderr.contains("invalid Wasm module"), "{stderr}");

    // A failed build runs nothing.
    script(&bin.join("npm"), "echo 'asc: error' >&2\nexit 1");
    let run = leani(root.path(), "app/demo", &bin);
    let stderr = String::from_utf8_lossy(&run.stderr);
    assert!(!run.status.success(), "{stderr}");
    assert!(run.stdout.is_empty());
    assert!(stderr.contains("asc: error"), "{stderr}");
    assert!(!stderr.contains("invalid Wasm module"), "{stderr}");
}
