//! Resolution and explicit deletion of Leani-owned local runtime state.

use std::{
    fs::{self, File, OpenOptions},
    io::{self, IsTerminal, Write},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};

use crate::{config::Config, process::Exit};

pub(crate) struct ResetAllOptions {
    pub confirmed: bool,
    pub config_path: Option<PathBuf>,
    pub data_dir: Option<PathBuf>,
    pub working_directory: PathBuf,
}

const RUNTIME_LOCK_FILE: &str = ".leani.lock";
const RUNTIME_STATE_ENTRIES: &[&str] = &[
    "leani.sqlite",
    "leani.sqlite-shm",
    "leani.sqlite-wal",
    "raw-history",
    "processor-artifacts",
    "execution-network.sqlite",
    "execution-network.sqlite-shm",
    "execution-network.sqlite-wal",
    "execution-p2p-secret",
    "checkpoint.json",
    "subscriptions",
];

#[derive(Debug)]
pub(crate) struct RuntimeDirectoryLock {
    _file: File,
}

pub(crate) fn lock_runtime_directory(data_dir: &Path) -> Result<RuntimeDirectoryLock> {
    fs::create_dir_all(data_dir)
        .with_context(|| format!("create runtime data directory {}", data_dir.display()))?;
    let path = data_dir.join(RUNTIME_LOCK_FILE);
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&path)
        .with_context(|| format!("open runtime lock {}", path.display()))?;
    file.try_lock().with_context(|| {
        format!(
            "runtime data directory {} is already in use by another Leani process",
            data_dir.display()
        )
    })?;
    Ok(RuntimeDirectoryLock { _file: file })
}

pub(crate) fn configured_path(
    requested: Option<&Path>,
    working_directory: &Path,
) -> Option<PathBuf> {
    requested.map(Path::to_path_buf).or_else(|| {
        let local = working_directory.join("leani.toml");
        local.is_file().then_some(local)
    })
}

pub(crate) fn runtime_data_dir(
    config_path: Option<&Path>,
    working_directory: &Path,
) -> Result<PathBuf> {
    if let Some(path) = config_path {
        return Ok(Config::load(path)?.data_dir);
    }
    if let Some(path) = std::env::var_os("XDG_DATA_HOME") {
        return Ok(PathBuf::from(path).join("leani"));
    }
    if let Some(path) = std::env::var_os("HOME") {
        return Ok(PathBuf::from(path).join(".local/share/leani"));
    }
    Ok(working_directory.join(".leani"))
}

pub(crate) fn reset_all(options: &ResetAllOptions) -> Result<Exit> {
    let data_dir = options.data_dir.clone().map_or_else(
        || runtime_data_dir(options.config_path.as_deref(), &options.working_directory),
        Ok,
    )?;
    reset_runtime_directory(
        &data_dir,
        options.config_path.as_deref(),
        &options.working_directory,
        options.confirmed,
    )?;
    Ok(Exit::Success)
}

fn reset_runtime_directory(
    data_dir: &Path,
    config_path: Option<&Path>,
    working_directory: &Path,
    confirmed: bool,
) -> Result<bool> {
    let target = if data_dir.is_absolute() {
        data_dir.to_path_buf()
    } else {
        working_directory.join(data_dir)
    };
    if !target.exists() {
        eprintln!(
            "leani: no local runtime state exists at {}",
            target.display()
        );
        return Ok(false);
    }
    let target = validated_reset_target(
        &target,
        config_path,
        working_directory,
        "runtime data",
        false,
    )?;

    eprintln!("leani: full local-state cold-start reset");
    eprintln!("  directory: {}", target.display());
    eprintln!(
        "  removes:   node database, raw history, processor artifacts, checkpoints, peer cache, P2P identity, and all embedded subscriptions"
    );
    if let Some(config_path) = config_path {
        eprintln!("  preserves: {}", config_path.display());
    } else {
        eprintln!("  preserves: configuration and the Leani binary");
    }
    eprintln!("Stop every Leani process using this data directory before continuing.");
    if !confirmed {
        if !io::stdin().is_terminal() {
            bail!("full state reset requires an interactive terminal or --yes");
        }
        eprint!("Reset all reconstructible local state? [y/N] ");
        io::stderr().flush()?;
        let mut answer = String::new();
        io::stdin().read_line(&mut answer)?;
        if !matches!(answer.trim().to_ascii_lowercase().as_str(), "y" | "yes") {
            bail!("full state reset was not confirmed");
        }
    }
    let _lock = lock_runtime_directory(&target)?;
    let unknown = remove_known_runtime_state(&target)?;
    for path in unknown {
        eprintln!("leani: preserved unknown entry {}", path.display());
    }
    eprintln!("leani: all local runtime state reset; the next run is cold");
    Ok(true)
}

pub(crate) fn validated_reset_target(
    target: &Path,
    config_path: Option<&Path>,
    working_directory: &Path,
    subject: &str,
    require_runtime_identity: bool,
) -> Result<PathBuf> {
    let metadata = fs::symlink_metadata(target)
        .with_context(|| format!("inspect {subject} directory {}", target.display()))?;
    if metadata.file_type().is_symlink() {
        bail!(
            "refusing to reset symlinked {subject} directory {}",
            target.display()
        );
    }
    if !metadata.is_dir() {
        bail!("{subject} path is not a directory: {}", target.display());
    }

    let target = target
        .canonicalize()
        .with_context(|| format!("resolve {subject} directory {}", target.display()))?;
    let working_directory = working_directory
        .canonicalize()
        .with_context(|| format!("resolve working directory before resetting {subject}"))?;
    if target.parent().is_none() || working_directory.starts_with(&target) {
        bail!(
            "refusing to reset broad {subject} directory {}",
            target.display()
        );
    }
    if let Some(home) = std::env::var_os("HOME")
        && let Ok(home) = PathBuf::from(home).canonicalize()
        && target == home
    {
        bail!(
            "refusing to reset home directory configured as {subject}: {}",
            target.display()
        );
    }
    if let Some(config_path) = config_path
        && let Ok(config_path) = config_path.canonicalize()
        && config_path.starts_with(&target)
    {
        bail!(
            "refusing to reset {subject} directory {} because it contains configuration {}",
            target.display(),
            config_path.display()
        );
    }
    if require_runtime_identity && !is_runtime_directory(&target) {
        bail!(
            "refusing to reset directory without Leani runtime identity {}",
            target.display()
        );
    }
    Ok(target)
}

pub(crate) fn is_runtime_directory(target: &Path) -> bool {
    target.join(RUNTIME_LOCK_FILE).is_file()
}

pub(crate) fn remove_known_runtime_state(target: &Path) -> Result<Vec<PathBuf>> {
    let mut unknown = Vec::new();
    for entry in fs::read_dir(target)
        .with_context(|| format!("read runtime data directory {}", target.display()))?
    {
        let entry = entry?;
        let name = entry.file_name();
        if name == RUNTIME_LOCK_FILE {
            continue;
        }
        let Some(name_str) = name.to_str() else {
            unknown.push(entry.path());
            continue;
        };
        if !RUNTIME_STATE_ENTRIES.contains(&name_str) {
            unknown.push(entry.path());
            continue;
        }
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.is_dir() && !metadata.file_type().is_symlink() {
            fs::remove_dir_all(&path)
                .with_context(|| format!("remove runtime directory {}", path.display()))?;
        } else {
            fs::remove_file(&path)
                .with_context(|| format!("remove runtime file {}", path.display()))?;
        }
    }
    Ok(unknown)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_reset_removes_only_the_data_root_and_preserves_configuration() {
        let root = tempfile::tempdir().expect("temporary directory");
        let working_directory = root.path().join("workspace");
        let data_dir = root.path().join("leani-data");
        let config_path = root.path().join("leani.toml");
        fs::create_dir_all(&working_directory).expect("working directory");
        fs::create_dir_all(data_dir.join("subscriptions/0123456789abcdef")).expect("runtime data");
        fs::write(data_dir.join("leani.sqlite"), b"fixture").expect("database fixture");
        fs::write(&config_path, b"configuration").expect("config fixture");

        assert!(
            reset_runtime_directory(&data_dir, Some(&config_path), &working_directory, true)
                .expect("reset all state")
        );
        assert!(data_dir.join(RUNTIME_LOCK_FILE).is_file());
        assert!(!data_dir.join("leani.sqlite").exists());
        assert!(!data_dir.join("subscriptions").exists());
        assert!(config_path.is_file());
        assert!(working_directory.is_dir());
    }

    #[test]
    fn full_reset_rejects_broad_or_configuration_owning_directories() {
        let root = tempfile::tempdir().expect("temporary directory");
        let working_directory = root.path().join("workspace");
        fs::create_dir_all(&working_directory).expect("working directory");
        let config_path = root.path().join("leani.toml");
        fs::write(&config_path, b"configuration").expect("config fixture");

        assert!(
            reset_runtime_directory(&working_directory, None, &working_directory, true).is_err()
        );
        assert!(
            reset_runtime_directory(root.path(), Some(&config_path), &working_directory, true)
                .is_err()
        );
    }

    #[test]
    fn runtime_directory_lock_excludes_other_process_contexts() {
        let root = tempfile::tempdir().expect("temporary directory");
        let first = lock_runtime_directory(root.path()).expect("first lock");
        let error = lock_runtime_directory(root.path()).expect_err("second lock is refused");
        assert!(error.to_string().contains("already in use"));
        drop(first);
        lock_runtime_directory(root.path()).expect("lock can be reacquired");
    }
}
