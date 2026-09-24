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
/// Marks a directory `leani subscribe` created for embedded subscription
/// state, the only kind `leani reset subscription` deletes.
const SUBSCRIPTION_MARKER_FILE: &str = ".leani-subscription";
/// Embedded subscriptions' state under a runtime data directory, one locked
/// directory per subscription.
const SUBSCRIPTIONS_DIRECTORY: &str = "subscriptions";
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
    leani_finality_beacon_api::FINALITY_ANCHOR_FILE,
];

#[derive(Debug)]
pub(crate) struct RuntimeDirectoryLock {
    _file: File,
}

/// Lock `data_dir` for an embedded subscription. A new directory, or the
/// derived per-subscription directory, is marked as subscription state; an
/// existing directory with other Leani state, such as a node's `data_dir`,
/// is never marked, so `leani reset subscription` never deletes it.
pub(crate) fn lock_subscription_directory(
    data_dir: &Path,
    derived: bool,
) -> Result<RuntimeDirectoryLock> {
    let adopt = derived || !holds_runtime_state(data_dir);
    let lock = lock_directory(data_dir)?;
    if adopt && !is_subscription_directory(data_dir) {
        let marker = data_dir.join(SUBSCRIPTION_MARKER_FILE);
        fs::write(&marker, b"Leani embedded subscription state\n")
            .with_context(|| format!("mark subscription state {}", marker.display()))?;
    }
    Ok(lock)
}

/// Lock marked embedded subscription state to reset it; it stays marked.
/// The marker is checked again once the lock is held: a node that took the
/// directory over since the caller last looked removed it.
pub(crate) fn lock_subscription_state(data_dir: &Path) -> Result<RuntimeDirectoryLock> {
    let lock = lock_directory(data_dir)?;
    if !is_subscription_directory(data_dir) {
        bail!(
            "refusing to reset {}: it no longer carries its embedded subscription marker, so a node may have taken it over",
            data_dir.display()
        );
    }
    Ok(lock)
}

pub(crate) fn is_subscription_directory(data_dir: &Path) -> bool {
    data_dir.join(SUBSCRIPTION_MARKER_FILE).is_file()
}

/// Whether `data_dir` already holds a lock file or any known runtime state.
fn holds_runtime_state(data_dir: &Path) -> bool {
    std::iter::once(RUNTIME_LOCK_FILE)
        .chain(RUNTIME_STATE_ENTRIES.iter().copied())
        .chain(std::iter::once(SUBSCRIPTIONS_DIRECTORY))
        .any(|entry| fs::symlink_metadata(data_dir.join(entry)).is_ok())
}

/// Lock `data_dir` for a node. A directory the node takes over becomes node
/// state, so it loses any embedded subscription marker: `leani reset
/// subscription` must never delete a node's store.
pub(crate) fn lock_runtime_directory(data_dir: &Path) -> Result<RuntimeDirectoryLock> {
    let lock = lock_directory(data_dir)?;
    let marker = data_dir.join(SUBSCRIPTION_MARKER_FILE);
    match fs::remove_file(&marker) {
        Err(error) if error.kind() != io::ErrorKind::NotFound => {
            Err(error).with_context(|| format!("remove subscription marker {}", marker.display()))
        }
        _ => Ok(lock),
    }
}

/// Retry `locked` while a lock released just before is still held: a
/// child process another test spawns keeps the inherited lock until it
/// execs, for a few milliseconds.
#[cfg(test)]
pub(crate) fn after_release<T>(mut locked: impl FnMut() -> Result<T>) -> Result<T> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        match locked() {
            Err(error)
                if format!("{error:#}").contains("already in use")
                    && std::time::Instant::now() < deadline =>
            {
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
            result => return result,
        }
    }
}

/// Lock `data_dir` without changing what kind of state it holds.
fn lock_directory(data_dir: &Path) -> Result<RuntimeDirectoryLock> {
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
    // Resetting keeps what kind of state the directory holds: a marked
    // subscription directory stays one.
    let _lock = lock_directory(&target)?;
    // Each running embedded subscriber holds its directory's lock: take
    // them all before deleting anything.
    let subscriptions = target.join(SUBSCRIPTIONS_DIRECTORY);
    let locked = lock_subscription_directories(&subscriptions)?;
    let mut unknown = remove_subscription_directories(&subscriptions, locked)?;
    unknown.extend(remove_known_runtime_state(&target)?);
    for path in unknown {
        eprintln!("leani: preserved unknown entry {}", path.display());
    }
    eprintln!("leani: all local runtime state reset; the next run is cold");
    Ok(true)
}

/// Lock every embedded subscription directory under `subscriptions`. This
/// fails, before anything is deleted, while a subscriber runs.
fn lock_subscription_directories(
    subscriptions: &Path,
) -> Result<Vec<(PathBuf, RuntimeDirectoryLock)>> {
    if !fs::symlink_metadata(subscriptions).is_ok_and(|metadata| metadata.is_dir()) {
        return Ok(Vec::new());
    }
    let mut locked = Vec::new();
    for entry in fs::read_dir(subscriptions)
        .with_context(|| format!("read subscription state root {}", subscriptions.display()))?
    {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let path = entry.path();
        let lock = lock_directory(&path).with_context(|| {
            format!(
                "stop the embedded subscriber using {} before resetting all local state",
                path.display()
            )
        })?;
        locked.push((path, lock));
    }
    Ok(locked)
}

/// Delete the locked subscription directories, other entries, and then
/// `subscriptions` itself. A directory that appeared during the reset, as
/// a subscriber starting now creates, is preserved and returned.
fn remove_subscription_directories(
    subscriptions: &Path,
    locked: Vec<(PathBuf, RuntimeDirectoryLock)>,
) -> Result<Vec<PathBuf>> {
    let metadata = match fs::symlink_metadata(subscriptions) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(error).with_context(|| {
                format!("inspect subscription state {}", subscriptions.display())
            });
        }
    };
    if !metadata.is_dir() {
        // A symlink is removed itself, never followed.
        fs::remove_file(subscriptions)
            .with_context(|| format!("remove runtime file {}", subscriptions.display()))?;
        return Ok(Vec::new());
    }
    for (path, lock) in locked {
        fs::remove_dir_all(&path)
            .with_context(|| format!("remove subscription state {}", path.display()))?;
        drop(lock);
    }
    let mut preserved = Vec::new();
    for entry in fs::read_dir(subscriptions)
        .with_context(|| format!("read subscription state root {}", subscriptions.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            preserved.push(path);
        } else {
            fs::remove_file(&path)
                .with_context(|| format!("remove runtime file {}", path.display()))?;
        }
    }
    if preserved.is_empty() {
        fs::remove_dir(subscriptions).with_context(|| {
            format!("remove subscription state root {}", subscriptions.display())
        })?;
    }
    Ok(preserved)
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
        if name == RUNTIME_LOCK_FILE || name == SUBSCRIPTION_MARKER_FILE {
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
    fn a_full_reset_of_a_subscription_directory_keeps_its_marker() {
        // Review 2, N3: `reset all` locked its target as a node, which
        // removes the marker. Its lock file survives the reset, so `leani
        // subscribe` would never mark the directory again, and `reset
        // subscription` would refuse it for good.
        let root = tempfile::tempdir().expect("temporary directory");
        let working_directory = root.path().join("workspace");
        fs::create_dir_all(&working_directory).expect("working directory");
        let feed = root.path().join("feed");
        drop(lock_subscription_directory(&feed, false).expect("a subscription directory"));
        fs::write(feed.join("leani.sqlite"), b"feed").expect("feed database");

        assert!(
            after_release(|| reset_runtime_directory(&feed, None, &working_directory, true))
                .expect("reset all")
        );
        assert!(!feed.join("leani.sqlite").exists());
        assert!(is_subscription_directory(&feed));
    }

    #[test]
    fn subscription_state_locks_only_while_it_is_marked() {
        // Review 2, N4: the subscription reset checked the marker before its
        // prompt but locked only after it, so a node that took the directory
        // over in between left node state that the reset then deleted. The
        // lock checks the marker again once it holds the directory.
        let root = tempfile::tempdir().expect("temporary directory");
        let feed = root.path().join("feed");
        drop(lock_subscription_directory(&feed, false).expect("a subscription directory"));
        drop(after_release(|| lock_subscription_state(&feed)).expect("marked state locks"));
        assert!(is_subscription_directory(&feed));
        drop(after_release(|| lock_runtime_directory(&feed)).expect("a node takes it over"));

        let error = after_release(|| lock_subscription_state(&feed))
            .expect_err("node state never locks as a feed");
        assert!(format!("{error:#}").contains("marker"), "{error:#}");
    }

    #[test]
    fn only_new_or_derived_directories_become_subscription_state() {
        // Audit CLI-7: `reset subscription` deletes only marked directories,
        // so a node's data_dir must never be marked.
        let root = tempfile::tempdir().expect("temporary directory");
        let fresh = root.path().join("fresh");
        drop(lock_subscription_directory(&fresh, false).expect("a new directory"));
        assert!(is_subscription_directory(&fresh));

        let node = root.path().join("node");
        fs::create_dir_all(&node).expect("node directory");
        fs::write(node.join("leani.sqlite"), b"node").expect("node database");
        drop(lock_subscription_directory(&node, false).expect("an explicit node directory"));
        assert!(!is_subscription_directory(&node));

        // An earlier release's derived directory is adopted by its location.
        let derived = root.path().join("subscriptions/0123456789abcdef");
        fs::create_dir_all(&derived).expect("derived directory");
        fs::write(derived.join("leani.sqlite"), b"feed").expect("feed database");
        drop(lock_subscription_directory(&derived, true).expect("a derived directory"));
        assert!(is_subscription_directory(&derived));
    }

    #[test]
    fn full_reset_refuses_while_an_embedded_subscription_runs() {
        // Audit CLI-6: `reset all` deleted `subscriptions/` from under a
        // running embedded subscriber.
        let root = tempfile::tempdir().expect("temporary directory");
        let working_directory = root.path().join("workspace");
        let data_dir = root.path().join("leani-data");
        let subscription = data_dir.join("subscriptions/0123456789abcdef");
        fs::create_dir_all(&working_directory).expect("working directory");
        fs::create_dir_all(&subscription).expect("subscription state");
        fs::write(data_dir.join("leani.sqlite"), b"node").expect("node database");
        fs::write(subscription.join("leani.sqlite"), b"subscription").expect("feed database");
        let running = lock_runtime_directory(&subscription).expect("a running subscriber");

        let error = reset_runtime_directory(&data_dir, None, &working_directory, true)
            .expect_err("a running subscriber refuses the reset");
        assert!(format!("{error:#}").contains("in use"), "{error:#}");
        assert!(data_dir.join("leani.sqlite").is_file());
        assert!(subscription.join("leani.sqlite").is_file());

        drop(running);
        // See runtime_directory_lock_excludes_other_process_contexts: a
        // concurrently spawned child can hold the released lock briefly.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while reset_runtime_directory(&data_dir, None, &working_directory, true).is_err() {
            assert!(
                std::time::Instant::now() < deadline,
                "the reset proceeds once the subscriber stops"
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        assert!(!data_dir.join("leani.sqlite").exists());
        assert!(!data_dir.join("subscriptions").exists());
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
        // The lock is a flock, which travels with the open file description.
        // A child process another test spawns concurrently (benchmark helpers
        // run `git` and `ps`) inherits that description until its exec
        // completes, so an immediate reacquire can briefly still see the lock
        // held. Measured window: a few milliseconds.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            match lock_runtime_directory(root.path()) {
                Ok(_) => break,
                Err(_) if std::time::Instant::now() < deadline => {
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
                Err(error) => panic!("lock can be reacquired: {error:#}"),
            }
        }
    }
}
