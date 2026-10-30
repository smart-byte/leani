//! A processor project for `leani processor run`: built with the project's
//! own toolchain before the run. Every build byte goes to stderr, as stdout
//! carries the run's changes.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde_json::Value;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Kind {
    Rust,
    TypeScript,
}

fn kind(directory: &Path) -> Result<Kind> {
    if directory.join("Cargo.toml").is_file() {
        Ok(Kind::Rust)
    } else if directory.join("package.json").is_file() {
        Ok(Kind::TypeScript)
    } else {
        bail!(
            "{} has neither a Cargo.toml nor a package.json: give a Wasm module, or a project that `leani processor create` writes",
            directory.display()
        )
    }
}

/// The `.wasm` of the one `cdylib` in cargo's JSON messages, wherever
/// `CARGO_TARGET_DIR` put it.
fn cargo_module(stdout: &[u8]) -> Result<PathBuf> {
    let modules: Vec<PathBuf> = stdout
        .split(|byte| *byte == b'\n')
        .filter_map(|line| serde_json::from_slice::<Value>(line).ok())
        .filter(|message| {
            message["reason"] == "compiler-artifact"
                && message["target"]["kind"]
                    .as_array()
                    .is_some_and(|kinds| kinds.iter().any(|kind| kind == "cdylib"))
        })
        .flat_map(|message| message["filenames"].as_array().cloned().unwrap_or_default())
        .filter_map(|file| file.as_str().map(PathBuf::from))
        .filter(|file| {
            file.extension()
                .is_some_and(|extension| extension == "wasm")
        })
        .collect();
    match modules.as_slice() {
        [module] => Ok(module.clone()),
        [] => bail!(
            "the build wrote no Wasm module: the crate needs `crate-type = [\"cdylib\"]`, as `leani processor create` writes it"
        ),
        _ => bail!(
            "the build wrote several Wasm modules ({}): give the one to run",
            modules
                .iter()
                .map(|module| module.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

/// `build/<name>.wasm`, where the generated build script writes it.
fn typescript_module(directory: &Path) -> Result<PathBuf> {
    let path = directory.join("package.json");
    let package: Value = serde_json::from_str(
        &std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?,
    )
    .with_context(|| format!("{} is not valid JSON", path.display()))?;
    let name = package["name"]
        .as_str()
        .with_context(|| format!("{} has no name", path.display()))?;
    Ok(directory.join("build").join(format!("{name}.wasm")))
}

const RUST: &str =
    "install Rust from https://rustup.rs, then run `rustup target add wasm32-unknown-unknown`";
const NODE: &str = "install Node.js 20 or later, with npm, from https://nodejs.org";

pub(super) async fn build(directory: &Path) -> Result<PathBuf> {
    let module = match kind(directory)? {
        Kind::Rust => cargo(directory).await?,
        Kind::TypeScript => typescript(directory).await?,
    };
    if !module.is_file() {
        bail!(
            "the build reported {}, which does not exist",
            module.display()
        );
    }
    Ok(module)
}

async fn cargo(directory: &Path) -> Result<PathBuf> {
    // Spawned, not `output()`, which would capture the rendered diagnostics
    // on stderr too.
    let output =
        tokio::process::Command::new(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
            .args([
                "build",
                "--release",
                "--target",
                "wasm32-unknown-unknown",
                "--message-format=json-render-diagnostics",
            ])
            .current_dir(directory)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
            .spawn()
            .map_err(|error| missing(error, "cargo", RUST))?
            .wait_with_output()
            .await
            .context("run cargo")?;
    if !output.status.success() {
        bail!("cargo could not build {}", directory.display());
    }
    cargo_module(&output.stdout)
}

async fn typescript(directory: &Path) -> Result<PathBuf> {
    let module = typescript_module(directory)?;
    if !directory.join("node_modules/.bin/asc").exists() {
        let install = if directory.join("package-lock.json").is_file() {
            "ci"
        } else {
            "install"
        };
        npm(directory, &[install, "--no-audit", "--no-fund"]).await?;
    }
    npm(directory, &["run", "build"]).await?;
    Ok(module)
}

async fn npm(directory: &Path, args: &[&str]) -> Result<()> {
    let status = tokio::process::Command::new("npm")
        .args(args)
        .current_dir(directory)
        .stdout(std::io::stderr())
        .status()
        .await
        .map_err(|error| missing(error, "npm", NODE))?;
    if !status.success() {
        bail!("`npm {}` failed in {}", args.join(" "), directory.display());
    }
    Ok(())
}

fn missing(error: std::io::Error, program: &str, install: &str) -> anyhow::Error {
    if error.kind() == std::io::ErrorKind::NotFound {
        anyhow::anyhow!("building this project needs {program}: {install}")
    } else {
        anyhow::Error::new(error).context(format!("start {program}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn artifact(kind: &str, files: &[&str]) -> String {
        serde_json::json!({ "reason": "compiler-artifact", "target": { "kind": [kind], "name": "x" },
                            "filenames": files })
        .to_string()
    }

    #[test]
    fn a_project_is_a_crate_or_a_package() {
        let root = tempfile::tempdir().expect("directory");
        assert!(
            kind(root.path())
                .unwrap_err()
                .to_string()
                .contains("leani processor create")
        );
        std::fs::write(root.path().join("package.json"), "{}").expect("package");
        assert_eq!(kind(root.path()).expect("kind"), Kind::TypeScript);
        std::fs::write(root.path().join("Cargo.toml"), "").expect("crate");
        assert_eq!(kind(root.path()).expect("kind"), Kind::Rust);
    }

    #[test]
    fn the_module_is_the_one_cdylib_wasm_wherever_the_target_is() {
        let stdout = [
            artifact(
                "lib",
                &["/t/wasm32-unknown-unknown/release/deps/libserde.rlib"],
            ),
            r#"{"reason":"compiler-message","message":{"level":"warning"}}"#.to_owned(),
            artifact(
                "cdylib",
                &["/shared/target/wasm32-unknown-unknown/release/usdc_volume.wasm"],
            ),
            r#"{"reason":"build-finished","success":true}"#.to_owned(),
        ]
        .join("\n");
        assert_eq!(
            cargo_module(stdout.as_bytes()).expect("module"),
            PathBuf::from("/shared/target/wasm32-unknown-unknown/release/usdc_volume.wasm")
        );
        let none = cargo_module(artifact("lib", &["/t/libx.rlib"]).as_bytes()).unwrap_err();
        assert!(none.to_string().contains("cdylib"), "{none}");
        let two = [
            artifact("cdylib", &["/t/a.wasm"]),
            artifact("cdylib", &["/t/b.wasm"]),
        ]
        .join("\n");
        let several = cargo_module(two.as_bytes()).unwrap_err();
        assert!(
            several.to_string().contains("a.wasm") && several.to_string().contains("b.wasm"),
            "{several}"
        );
    }

    #[test]
    fn a_package_builds_into_its_named_module() {
        let root = tempfile::tempdir().expect("directory");
        std::fs::write(
            root.path().join("package.json"),
            r#"{"name":"usdc-volume-ts"}"#,
        )
        .expect("package");
        assert_eq!(
            typescript_module(root.path()).expect("module"),
            root.path().join("build/usdc-volume-ts.wasm")
        );
        std::fs::write(root.path().join("package.json"), "{}").expect("package");
        assert!(
            typescript_module(root.path())
                .unwrap_err()
                .to_string()
                .contains("name")
        );
    }

    #[tokio::test]
    #[ignore = "builds with cargo's wasm32 target, Node.js, and npm"]
    async fn generated_projects_build_into_their_modules() {
        use leani_processor_wasm::scaffold::{self, Language};
        let root = tempfile::tempdir().expect("directory");
        for (name, language) in [
            ("gas-rust", Language::Rust),
            ("gas-ts", Language::TypeScript),
        ] {
            let generated = scaffold::generate(&scaffold::Spec {
                name: name.to_owned(),
                language,
                ..scaffold::starter()
            })
            .expect("generate");
            let directory = root.path().join(name);
            for (path, text) in &generated.files {
                let path = directory.join(path);
                std::fs::create_dir_all(path.parent().expect("parent")).expect("directory");
                std::fs::write(path, text).expect("write");
            }
            let module = build(&directory).await.expect("built");
            let bytes = std::fs::read(&module).expect("module");
            assert_eq!(&bytes[..4], b"\0asm", "{}", module.display());
            // A second build of an unchanged project reuses the install.
            assert_eq!(build(&directory).await.expect("rebuilt"), module);
        }
    }
}
