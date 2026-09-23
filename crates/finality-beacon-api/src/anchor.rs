//! The persisted finality anchor: the newest verified finalized anchor kept
//! under the data directory, and the choice of bootstrap root at startup.

use std::{
    fs,
    io::Write as _,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};

use alloy_primitives::B256;
use leani_primitives::BlockHash;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::{
    BeaconApiError, MAINNET_GENESIS_TIME, SLOT_SECONDS, SLOTS_PER_EPOCH, VerifiedFinalityAnchor,
    verify_checkpoint_age,
};

/// File under the data directory that holds the newest verified finality
/// anchor.
pub const FINALITY_ANCHOR_FILE: &str = "finality-anchor.json";
const FINALITY_ANCHOR_VERSION: u32 = 1;

/// Newest verified finalized anchor, persisted under the data directory so a
/// restart re-bootstraps from it instead of the configured checkpoint. It
/// holds no secrets.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PersistedFinalityAnchor {
    pub anchor: VerifiedFinalityAnchor,
    /// Configured weak-subjectivity checkpoint the anchor was verified from.
    pub checkpoint_root: [u8; 32],
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct FinalityAnchorFile {
    version: u32,
    beacon_slot: u64,
    beacon_block_root: B256,
    execution_block_number: u64,
    execution_block_hash: B256,
    checkpoint_root: B256,
}

/// Where a checkpoint comes from, which decides when a persisted anchor may
/// replace it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckpointOrigin {
    /// The operator's trust root: `finality.checkpoint`, or a provider quorum
    /// the operator accepted. Only an anchor verified from it replaces it.
    Operator,
    /// An anchor this node verified earlier, such as the embedded
    /// subscription's cached checkpoint. A persisted anchor at a later slot
    /// replaces it.
    LocallyVerified,
}

/// A checkpoint a finality source starts from.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TrustedCheckpoint {
    pub root: [u8; 32],
    /// Beacon slot of `root`, when known.
    pub slot: Option<u64>,
    pub origin: CheckpointOrigin,
}

/// How a finality source uses the persisted anchor file.
#[derive(Clone, Debug, Default)]
pub enum AnchorFile {
    /// Keep verified anchors in memory only.
    #[default]
    Disabled,
    /// Start from the file when it applies, but never write it.
    ReadOnly(PathBuf),
    /// Start from the file when it applies, and persist newer anchors.
    /// `write_failures` counts writes that failed.
    ReadWrite {
        path: PathBuf,
        write_failures: Arc<AtomicU64>,
    },
}

impl AnchorFile {
    #[must_use]
    pub fn path(&self) -> Option<&Path> {
        match self {
            Self::Disabled => None,
            Self::ReadOnly(path) | Self::ReadWrite { path, .. } => Some(path),
        }
    }
}

/// Root a light client bootstraps from.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StartAnchor {
    pub root: [u8; 32],
    /// Beacon slot of `root`, when known before the bootstrap.
    pub slot: Option<u64>,
    /// Configured weak-subjectivity checkpoint `root` descends from.
    pub checkpoint_root: [u8; 32],
    /// Whether `root` is a persisted anchor rather than the configured
    /// checkpoint itself.
    pub persisted: bool,
}

impl StartAnchor {
    #[must_use]
    pub const fn configured(checkpoint: TrustedCheckpoint) -> Self {
        Self {
            root: checkpoint.root,
            slot: checkpoint.slot,
            checkpoint_root: checkpoint.root,
            persisted: false,
        }
    }
}

/// Choose the bootstrap root for a checkpoint.
///
/// A persisted anchor within `max_age` replaces the checkpoint when it was
/// verified from that checkpoint (or is that checkpoint). It also replaces a
/// locally verified checkpoint when it is at a later slot. An operator's
/// checkpoint is never replaced by an anchor verified from another trust
/// root: the operator may have re-anchored on purpose. A missing, unreadable,
/// or corrupt anchor file falls back to the checkpoint with a warning.
#[must_use]
pub fn resolve_start_anchor(
    anchor_path: Option<&Path>,
    checkpoint: TrustedCheckpoint,
    max_age: Duration,
    now: SystemTime,
) -> StartAnchor {
    let configured = StartAnchor::configured(checkpoint);
    let Some(path) = anchor_path else {
        return configured;
    };
    let persisted = match read_finality_anchor(path) {
        Ok(Some(persisted)) => persisted,
        Ok(None) => return configured,
        Err(error) => {
            warn!(%error, "ignoring the unreadable persisted finality anchor");
            return configured;
        }
    };
    let anchor = persisted.anchor;
    let same_lineage =
        persisted.checkpoint_root == checkpoint.root || anchor.beacon_block_root == checkpoint.root;
    let not_older = checkpoint
        .slot
        .is_none_or(|slot| anchor.beacon_slot >= slot);
    let later = checkpoint
        .slot
        .is_some_and(|slot| anchor.beacon_slot > slot);
    let applies = (same_lineage && not_older)
        || (checkpoint.origin == CheckpointOrigin::LocallyVerified && later);
    if !applies {
        if checkpoint.origin == CheckpointOrigin::Operator && !same_lineage {
            warn!(
                persisted_checkpoint = %B256::from(persisted.checkpoint_root),
                configured_checkpoint = %B256::from(checkpoint.root),
                "ignoring a persisted finality anchor verified from another checkpoint than the configured one"
            );
        } else {
            debug!(
                beacon_slot = anchor.beacon_slot,
                "persisted finality anchor is not newer than the checkpoint"
            );
        }
        return configured;
    }
    if let Err(error) = verify_checkpoint_age(anchor.beacon_slot, max_age, now) {
        warn!(%error, "ignoring a persisted finality anchor past the checkpoint age limit");
        return configured;
    }
    StartAnchor {
        root: anchor.beacon_block_root,
        slot: Some(anchor.beacon_slot),
        checkpoint_root: persisted.checkpoint_root,
        persisted: true,
    }
}

/// Read a persisted finality anchor. A missing file is `Ok(None)`.
///
/// # Errors
///
/// Rejects an unreadable file, invalid JSON, or an unsupported version.
pub fn read_finality_anchor(
    path: &Path,
) -> Result<Option<PersistedFinalityAnchor>, BeaconApiError> {
    let encoded = match fs::read(path) {
        Ok(encoded) => encoded,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(BeaconApiError::AnchorFile(format!(
                "read {}: {error}",
                path.display()
            )));
        }
    };
    let file = serde_json::from_slice::<FinalityAnchorFile>(&encoded).map_err(|error| {
        BeaconApiError::AnchorFile(format!("decode {}: {error}", path.display()))
    })?;
    if file.version != FINALITY_ANCHOR_VERSION {
        return Err(BeaconApiError::AnchorFile(format!(
            "{} has unsupported version {}",
            path.display(),
            file.version
        )));
    }
    Ok(Some(PersistedFinalityAnchor {
        anchor: VerifiedFinalityAnchor {
            beacon_slot: file.beacon_slot,
            beacon_block_root: file.beacon_block_root.into(),
            execution_block_number: file.execution_block_number,
            execution_block_hash: BlockHash::new(file.execution_block_hash.into()),
        },
        checkpoint_root: file.checkpoint_root.into(),
    }))
}

/// Persist `anchor` unless the file already holds one at the same or a newer
/// slot. Returns whether the file was replaced.
///
/// # Errors
///
/// Returns filesystem failures; the previous file is then left intact.
pub fn persist_finality_anchor(
    path: &Path,
    anchor: &PersistedFinalityAnchor,
) -> Result<bool, BeaconApiError> {
    if let Ok(Some(current)) = read_finality_anchor(path)
        && current.anchor.beacon_slot >= anchor.anchor.beacon_slot
    {
        return Ok(false);
    }
    let file = FinalityAnchorFile {
        version: FINALITY_ANCHOR_VERSION,
        beacon_slot: anchor.anchor.beacon_slot,
        beacon_block_root: anchor.anchor.beacon_block_root.into(),
        execution_block_number: anchor.anchor.execution_block_number,
        execution_block_hash: anchor.anchor.execution_block_hash.0.into(),
        checkpoint_root: anchor.checkpoint_root.into(),
    };
    let failed = |error: std::io::Error| {
        BeaconApiError::AnchorFile(format!("write {}: {error}", path.display()))
    };
    let directory = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    // Temp file, fsync, rename, then fsync the directory: a crash leaves
    // either the previous anchor or the new one, never a partial file.
    let mut temporary = tempfile::NamedTempFile::new_in(directory).map_err(failed)?;
    serde_json::to_writer_pretty(temporary.as_file_mut(), &file)
        .map_err(|error| BeaconApiError::AnchorFile(error.to_string()))?;
    temporary.as_file_mut().write_all(b"\n").map_err(failed)?;
    temporary.as_file().sync_all().map_err(failed)?;
    temporary
        .persist(path)
        .map_err(|error| failed(error.error))?;
    #[cfg(unix)]
    fs::File::open(directory)
        .and_then(|directory| directory.sync_all())
        .map_err(failed)?;
    Ok(true)
}

/// Persists newer epoch-aligned anchors off the async runtime, one write at a
/// time, and counts failed writes. Clones share the newest written slot.
#[derive(Clone, Debug, Default)]
pub struct AnchorWriter {
    file: AnchorFile,
    persisted_slot: Arc<tokio::sync::Mutex<Option<u64>>>,
}

impl AnchorWriter {
    #[must_use]
    pub fn new(file: AnchorFile) -> Self {
        Self {
            file,
            persisted_slot: Arc::default(),
        }
    }

    /// Persist `anchor`, verified from `checkpoint_root`, when the file is
    /// writable and the anchor is epoch-aligned and newer than any written.
    /// A failure is logged and counted; it never fails finality.
    pub async fn persist(&self, anchor: VerifiedFinalityAnchor, checkpoint_root: [u8; 32]) {
        let AnchorFile::ReadWrite {
            path,
            write_failures,
        } = &self.file
        else {
            return;
        };
        if !is_bootstrap_anchor(&anchor) {
            return;
        }
        let mut persisted_slot = self.persisted_slot.lock().await;
        if persisted_slot.is_some_and(|slot| anchor.beacon_slot <= slot) {
            return;
        }
        let path = path.clone();
        let written = tokio::task::spawn_blocking(move || {
            persist_finality_anchor(
                &path,
                &PersistedFinalityAnchor {
                    anchor,
                    checkpoint_root,
                },
            )
        })
        .await;
        match written {
            Ok(Ok(_)) => *persisted_slot = Some(anchor.beacon_slot),
            Ok(Err(error)) => {
                write_failures.fetch_add(1, Ordering::Relaxed);
                warn!(%error, "could not persist the verified finality anchor");
            }
            Err(error) => {
                write_failures.fetch_add(1, Ordering::Relaxed);
                warn!(%error, "the finality anchor write task failed");
            }
        }
    }
}

/// Unix time at which a mainnet beacon slot starts.
#[must_use]
pub const fn slot_unix_seconds(slot: u64) -> u64 {
    MAINNET_GENESIS_TIME.saturating_add(slot.saturating_mul(SLOT_SECONDS))
}

/// Whether a restart can re-bootstrap from `anchor`. Beacon nodes serve
/// bootstraps for epoch-boundary blocks, so, like Helios, only epoch-aligned
/// finalized anchors are persisted.
#[must_use]
pub const fn is_bootstrap_anchor(anchor: &VerifiedFinalityAnchor) -> bool {
    anchor.beacon_slot.is_multiple_of(SLOTS_PER_EPOCH)
}
