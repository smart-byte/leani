//! Disposable, bounded execution-peer state backed by `SQLite`.

use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    str::FromStr as _,
    sync::Mutex,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use alloy_primitives::B512;
use reth_network_peers::NodeRecord;
use sqlx::{
    Row as _, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use thiserror::Error;
use tokio::sync::OnceCell;
use tracing::warn;

use super::{PeerMaterialKind, PeerQualification};

const SCHEMA_VERSION: i64 = 1;
const CHAIN_ID_MAINNET: i64 = 1;

const SCHEMA: &str = r"
CREATE TABLE IF NOT EXISTS execution_peer_candidates (
    chain_id INTEGER NOT NULL,
    peer_id BLOB NOT NULL CHECK(length(peer_id) = 64),
    record TEXT NOT NULL,
    last_seen_unix_ms INTEGER NOT NULL,
    PRIMARY KEY (chain_id, peer_id)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS execution_peer_candidates_recent
ON execution_peer_candidates(chain_id, last_seen_unix_ms DESC);

CREATE TABLE IF NOT EXISTS execution_peer_quality (
    chain_id INTEGER NOT NULL,
    peer_id BLOB NOT NULL CHECK(length(peer_id) = 64),
    fork_compatible INTEGER NOT NULL DEFAULT 0,
    highest_served_block INTEGER,
    last_header_success_unix_ms INTEGER,
    last_body_success_unix_ms INTEGER,
    last_receipt_success_unix_ms INTEGER,
    response_latency_ms INTEGER,
    last_failure_reason TEXT,
    last_failure_unix_ms INTEGER,
    qualification INTEGER,
    PRIMARY KEY (chain_id, peer_id)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS execution_peer_quality_body
ON execution_peer_quality(chain_id, last_body_success_unix_ms DESC);
";

#[derive(Debug, Error)]
pub enum ExecutionPeerStoreError {
    #[error("execution peer store path has no parent: {0}")]
    MissingParent(PathBuf),
    #[error("create execution peer store directory {path}: {source}")]
    CreateDirectory {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("unsupported execution peer store schema {observed}; expected {expected}")]
    UnsupportedSchema { observed: i64, expected: i64 },
    #[error("invalid execution peer id persisted in SQLite")]
    InvalidPeerId,
    #[error("execution peer store integer is outside SQLite's signed range")]
    IntegerRange,
    #[error(transparent)]
    Sqlite(#[from] sqlx::Error),
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct PeerQualityRank {
    body_serving: bool,
    available_after_last_failure: bool,
    last_body_success_unix_ms: u64,
    receipt_serving: bool,
    last_material_success_unix_ms: u64,
    highest_served_block: u64,
    inverse_latency_ms: std::cmp::Reverse<u64>,
}

impl Default for PeerQualityRank {
    fn default() -> Self {
        Self {
            body_serving: false,
            available_after_last_failure: false,
            last_body_success_unix_ms: 0,
            receipt_serving: false,
            last_material_success_unix_ms: 0,
            highest_served_block: 0,
            inverse_latency_ms: std::cmp::Reverse(u64::MAX),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct PeerQualityEvidence {
    fork_compatible: bool,
    highest_served_block: Option<u64>,
    last_header_success_unix_ms: Option<u64>,
    last_body_success_unix_ms: Option<u64>,
    last_receipt_success_unix_ms: Option<u64>,
    response_latency_ms: Option<u64>,
    last_failure_reason: Option<String>,
    last_failure_unix_ms: Option<u64>,
    qualification: Option<PeerQualification>,
}

impl From<&PeerQualityEvidence> for PeerQualityRank {
    fn from(evidence: &PeerQualityEvidence) -> Self {
        let last_material_success_unix_ms = evidence
            .last_header_success_unix_ms
            .into_iter()
            .chain(evidence.last_body_success_unix_ms)
            .chain(evidence.last_receipt_success_unix_ms)
            .max()
            .unwrap_or_default();
        Self {
            body_serving: evidence.last_body_success_unix_ms.is_some(),
            available_after_last_failure: evidence
                .last_failure_unix_ms
                .is_none_or(|failure| failure < last_material_success_unix_ms),
            last_body_success_unix_ms: evidence.last_body_success_unix_ms.unwrap_or_default(),
            receipt_serving: evidence.last_receipt_success_unix_ms.is_some(),
            last_material_success_unix_ms,
            highest_served_block: evidence.highest_served_block.unwrap_or_default(),
            inverse_latency_ms: std::cmp::Reverse(evidence.response_latency_ms.unwrap_or(u64::MAX)),
        }
    }
}

#[derive(Clone, Debug)]
struct CandidateRecord {
    record: NodeRecord,
    last_seen_unix_ms: u64,
}

#[derive(Debug)]
pub(super) struct ExecutionPeerStore {
    path: Option<PathBuf>,
    maximum_entries: usize,
    pool: OnceCell<SqlitePool>,
    initialized: OnceCell<()>,
    candidates: Mutex<HashMap<B512, CandidateRecord>>,
    quality: Mutex<HashMap<B512, PeerQualityEvidence>>,
    dirty_quality: Mutex<HashSet<B512>>,
}

impl ExecutionPeerStore {
    pub(super) fn new(path: Option<PathBuf>, maximum_entries: usize) -> Self {
        Self {
            path,
            maximum_entries,
            pool: OnceCell::new(),
            initialized: OnceCell::new(),
            candidates: Mutex::new(HashMap::new()),
            quality: Mutex::new(HashMap::new()),
            dirty_quality: Mutex::new(HashSet::new()),
        }
    }

    pub(super) async fn initialize(&self) -> Result<(), ExecutionPeerStoreError> {
        self.initialized
            .get_or_try_init(|| async {
                let Some(_) = self.path else {
                    return Ok(());
                };
                let pool = self.pool().await?;
                let mut candidates = load_candidates(pool, self.maximum_entries).await?;
                let mut quality = load_quality(pool, self.maximum_entries).await?;
                let mut in_memory_candidates = self
                    .candidates
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                for (peer_id, candidate) in std::mem::take(&mut *in_memory_candidates) {
                    candidates.insert(peer_id, candidate);
                }
                *in_memory_candidates = candidates;
                drop(in_memory_candidates);
                let mut in_memory_quality = self
                    .quality
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                for (peer_id, incoming) in std::mem::take(&mut *in_memory_quality) {
                    match quality.entry(peer_id) {
                        std::collections::hash_map::Entry::Vacant(slot) => {
                            slot.insert(incoming);
                        }
                        std::collections::hash_map::Entry::Occupied(mut slot) => {
                            merge_quality_evidence(slot.get_mut(), incoming);
                        }
                    }
                }
                *in_memory_quality = quality;
                drop(in_memory_quality);
                self.prune_memory();
                Ok(())
            })
            .await
            .copied()
    }

    async fn pool(&self) -> Result<&SqlitePool, ExecutionPeerStoreError> {
        self.pool
            .get_or_try_init(|| async {
                let path = self
                    .path
                    .as_deref()
                    .expect("persistent store path is checked before opening");
                open_database(path, true).await
            })
            .await
    }

    pub(super) fn record_success(
        &self,
        peer_id: B512,
        kind: PeerMaterialKind,
        block: u64,
        elapsed: Duration,
    ) {
        let now = observed_at_unix_ms();
        let latency = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
        let mut quality = self
            .quality
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let evidence = quality.entry(peer_id).or_default();
        evidence.fork_compatible = true;
        evidence.highest_served_block =
            Some(evidence.highest_served_block.unwrap_or_default().max(block));
        match kind {
            PeerMaterialKind::Header => evidence.last_header_success_unix_ms = Some(now),
            PeerMaterialKind::Body => evidence.last_body_success_unix_ms = Some(now),
            PeerMaterialKind::Receipts => evidence.last_receipt_success_unix_ms = Some(now),
        }
        evidence.response_latency_ms = Some(evidence.response_latency_ms.map_or(latency, |old| {
            old.saturating_mul(3).saturating_add(latency) / 4
        }));
        if matches!(kind, PeerMaterialKind::Body) {
            evidence.qualification = Some(PeerQualification::BodyServing);
        }
        self.finish_quality_update(&mut quality, peer_id);
    }

    pub(super) fn record_qualification(
        &self,
        peer_id: B512,
        qualification: PeerQualification,
        detail: Option<&str>,
    ) {
        let mut quality = self
            .quality
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let evidence = quality.entry(peer_id).or_default();
        evidence.qualification = Some(qualification);
        evidence.fork_compatible |= !matches!(qualification, PeerQualification::Rejected);
        if !matches!(qualification, PeerQualification::BodyServing) {
            evidence.last_failure_reason = detail.map(bounded_quality_detail);
            evidence.last_failure_unix_ms = Some(observed_at_unix_ms());
        }
        self.finish_quality_update(&mut quality, peer_id);
    }

    pub(super) fn record_failure(&self, peer_id: B512, detail: &str) {
        let mut quality = self
            .quality
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let evidence = quality.entry(peer_id).or_default();
        evidence.last_failure_reason = Some(bounded_quality_detail(detail));
        evidence.last_failure_unix_ms = Some(observed_at_unix_ms());
        self.finish_quality_update(&mut quality, peer_id);
    }

    fn finish_quality_update(
        &self,
        quality: &mut HashMap<B512, PeerQualityEvidence>,
        peer_id: B512,
    ) {
        let evicted = prune_quality_map(quality, self.maximum_entries);
        let retained = quality.contains_key(&peer_id);
        let mut dirty = self
            .dirty_quality
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(evicted) = evicted {
            dirty.remove(&evicted);
        }
        if retained {
            dirty.insert(peer_id);
        }
    }

    pub(super) fn rank(&self, peer_id: B512) -> PeerQualityRank {
        self.quality
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&peer_id)
            .map_or_else(PeerQualityRank::default, PeerQualityRank::from)
    }

    pub(super) fn is_available_body_server(&self, peer_id: B512) -> bool {
        let quality = self
            .quality
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(evidence) = quality.get(&peer_id) else {
            return false;
        };
        let Some(body_success) = evidence.last_body_success_unix_ms else {
            return false;
        };
        evidence
            .last_failure_unix_ms
            .is_none_or(|failure| failure < body_success)
    }

    pub(super) fn candidates(&self) -> Vec<NodeRecord> {
        let quality = self
            .quality
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut candidates = self
            .candidates
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| {
            quality
                .get(&right.record.id)
                .map_or_else(PeerQualityRank::default, PeerQualityRank::from)
                .cmp(
                    &quality
                        .get(&left.record.id)
                        .map_or_else(PeerQualityRank::default, PeerQualityRank::from),
                )
                .then_with(|| right.last_seen_unix_ms.cmp(&left.last_seen_unix_ms))
                .then_with(|| left.record.id.as_slice().cmp(right.record.id.as_slice()))
        });
        candidates
            .into_iter()
            .take(self.maximum_entries)
            .map(|candidate| candidate.record)
            .collect()
    }

    pub(super) async fn persist(
        &self,
        records: Vec<NodeRecord>,
    ) -> Result<(), ExecutionPeerStoreError> {
        self.initialize().await?;
        let now = observed_at_unix_ms();
        {
            let mut candidates = self
                .candidates
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for record in &records {
                candidates.insert(
                    record.id,
                    CandidateRecord {
                        record: *record,
                        last_seen_unix_ms: now,
                    },
                );
            }
        }
        self.prune_memory();
        let dirty_ids = {
            let mut dirty = self
                .dirty_quality
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            std::mem::take(&mut *dirty)
        };
        let dirty_quality = {
            let quality = self
                .quality
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            dirty_ids
                .iter()
                .filter_map(|peer_id| quality.get(peer_id).cloned().map(|value| (*peer_id, value)))
                .collect::<Vec<_>>()
        };
        let Some(_) = self.path else {
            return Ok(());
        };
        let result = self.persist_rows(&records, now, &dirty_quality).await;
        if result.is_err() {
            self.dirty_quality
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .extend(dirty_ids);
        }
        result
    }

    async fn persist_rows(
        &self,
        records: &[NodeRecord],
        now: u64,
        quality: &[(B512, PeerQualityEvidence)],
    ) -> Result<(), ExecutionPeerStoreError> {
        let pool = self.pool().await?;
        let mut transaction = pool.begin().await?;
        for record in records {
            sqlx::query(
                "INSERT INTO execution_peer_candidates(chain_id, peer_id, record, last_seen_unix_ms)
                 VALUES (?, ?, ?, ?)
                 ON CONFLICT(chain_id, peer_id) DO UPDATE SET
                    record = excluded.record,
                    last_seen_unix_ms = excluded.last_seen_unix_ms",
            )
            .bind(CHAIN_ID_MAINNET)
            .bind(record.id.as_slice())
            .bind(record.to_string())
            .bind(to_i64(now)?)
            .execute(&mut *transaction)
            .await?;
        }
        for (peer_id, evidence) in quality {
            upsert_quality(&mut transaction, *peer_id, evidence).await?;
        }
        prune_database(&mut transaction, self.maximum_entries).await?;
        transaction.commit().await?;
        Ok(())
    }

    fn prune_memory(&self) {
        let quality = self
            .quality
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut candidates = self
            .candidates
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if candidates.len() > self.maximum_entries {
            let mut retained = candidates.values().cloned().collect::<Vec<_>>();
            retained.sort_by(|left, right| {
                quality
                    .get(&right.record.id)
                    .map_or_else(PeerQualityRank::default, PeerQualityRank::from)
                    .cmp(
                        &quality
                            .get(&left.record.id)
                            .map_or_else(PeerQualityRank::default, PeerQualityRank::from),
                    )
                    .then_with(|| right.last_seen_unix_ms.cmp(&left.last_seen_unix_ms))
                    .then_with(|| left.record.id.as_slice().cmp(right.record.id.as_slice()))
            });
            retained.truncate(self.maximum_entries);
            *candidates = retained
                .into_iter()
                .map(|candidate| (candidate.record.id, candidate))
                .collect();
        }
        drop(candidates);
        drop(quality);

        let mut quality = self
            .quality
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while prune_quality_map(&mut quality, self.maximum_entries).is_some() {}
    }

    #[cfg(test)]
    pub(super) fn has_material_success(&self, peer_id: B512, kind: PeerMaterialKind) -> bool {
        self.quality
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&peer_id)
            .is_some_and(|evidence| match kind {
                PeerMaterialKind::Header => evidence.last_header_success_unix_ms.is_some(),
                PeerMaterialKind::Body => evidence.last_body_success_unix_ms.is_some(),
                PeerMaterialKind::Receipts => evidence.last_receipt_success_unix_ms.is_some(),
            })
    }

    #[cfg(test)]
    pub(super) fn candidate_ids(&self) -> HashSet<B512> {
        self.candidates
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys()
            .copied()
            .collect()
    }
}

fn prune_quality_map(
    quality: &mut HashMap<B512, PeerQualityEvidence>,
    maximum_entries: usize,
) -> Option<B512> {
    if quality.len() <= maximum_entries {
        return None;
    }
    let evicted = quality
        .iter()
        .min_by(|(left_id, left), (right_id, right)| {
            PeerQualityRank::from(*left)
                .cmp(&PeerQualityRank::from(*right))
                .then_with(|| right_id.as_slice().cmp(left_id.as_slice()))
        })
        .map(|(peer_id, _)| *peer_id)?;
    quality.remove(&evicted);
    Some(evicted)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionPeerStoreMerge {
    pub total: usize,
    pub imported: usize,
}

/// Merge bounded candidates and monotonic material evidence from sibling
/// Mainnet peer stores into `destination`.
///
/// # Errors
///
/// Returns an error when the destination cannot be opened, its schema is not
/// supported, or the merged transaction cannot be committed. Unreadable
/// sibling stores are skipped because they are disposable operational inputs.
pub async fn merge_execution_peer_stores(
    destination: &Path,
    sources: &[PathBuf],
    maximum_entries: usize,
) -> Result<Option<ExecutionPeerStoreMerge>, ExecutionPeerStoreError> {
    let destination_store =
        ExecutionPeerStore::new(Some(destination.to_path_buf()), maximum_entries);
    destination_store.initialize().await?;
    let destination_ids = destination_store
        .candidates
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .keys()
        .copied()
        .collect::<HashSet<_>>();
    let mut candidates = destination_store
        .candidates
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    let mut quality = destination_store
        .quality
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    let mut changed = false;

    for source in sources
        .iter()
        .filter(|source| source.as_path() != destination && source.is_file())
    {
        let source_store = ExecutionPeerStore::new(Some(source.clone()), maximum_entries);
        if let Err(error) = source_store.initialize().await {
            warn!(path = %source.display(), %error, "ignoring unreadable sibling execution peer store");
            continue;
        }
        for (peer_id, incoming) in source_store
            .candidates
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
        {
            match candidates.entry(*peer_id) {
                std::collections::hash_map::Entry::Vacant(slot) => {
                    slot.insert(incoming.clone());
                    changed = true;
                }
                std::collections::hash_map::Entry::Occupied(mut slot)
                    if incoming.last_seen_unix_ms > slot.get().last_seen_unix_ms =>
                {
                    slot.insert(incoming.clone());
                    changed = true;
                }
                std::collections::hash_map::Entry::Occupied(_) => {}
            }
        }
        for (peer_id, incoming) in source_store
            .quality
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
        {
            match quality.entry(*peer_id) {
                std::collections::hash_map::Entry::Vacant(slot) => {
                    slot.insert(incoming.clone());
                    changed = true;
                }
                std::collections::hash_map::Entry::Occupied(mut slot) => {
                    let before = slot.get().clone();
                    merge_quality_evidence(slot.get_mut(), incoming.clone());
                    changed |= *slot.get() != before;
                }
            }
        }
    }
    if candidates.is_empty() {
        return Ok(None);
    }
    *destination_store
        .candidates
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = candidates;
    *destination_store
        .quality
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = quality;
    destination_store.prune_memory();
    if changed {
        destination_store.replace_database().await?;
    }
    let retained = destination_store
        .candidates
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let imported = retained
        .keys()
        .filter(|peer_id| !destination_ids.contains(*peer_id))
        .count();
    Ok(Some(ExecutionPeerStoreMerge {
        total: retained.len(),
        imported,
    }))
}

impl ExecutionPeerStore {
    async fn replace_database(&self) -> Result<(), ExecutionPeerStoreError> {
        let candidates = self
            .candidates
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let quality = self
            .quality
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let pool = self.pool().await?;
        let mut transaction = pool.begin().await?;
        sqlx::query("DELETE FROM execution_peer_candidates WHERE chain_id = ?")
            .bind(CHAIN_ID_MAINNET)
            .execute(&mut *transaction)
            .await?;
        sqlx::query("DELETE FROM execution_peer_quality WHERE chain_id = ?")
            .bind(CHAIN_ID_MAINNET)
            .execute(&mut *transaction)
            .await?;
        for candidate in candidates.values() {
            sqlx::query(
                "INSERT INTO execution_peer_candidates(chain_id, peer_id, record, last_seen_unix_ms)
                 VALUES (?, ?, ?, ?)",
            )
            .bind(CHAIN_ID_MAINNET)
            .bind(candidate.record.id.as_slice())
            .bind(candidate.record.to_string())
            .bind(to_i64(candidate.last_seen_unix_ms)?)
            .execute(&mut *transaction)
            .await?;
        }
        for (peer_id, evidence) in &quality {
            upsert_quality(&mut transaction, *peer_id, evidence).await?;
        }
        transaction.commit().await?;
        Ok(())
    }
}

async fn open_database(path: &Path, create: bool) -> Result<SqlitePool, ExecutionPeerStoreError> {
    let parent = path
        .parent()
        .ok_or_else(|| ExecutionPeerStoreError::MissingParent(path.to_path_buf()))?;
    if create {
        std::fs::create_dir_all(parent).map_err(|source| {
            ExecutionPeerStoreError::CreateDirectory {
                path: parent.to_path_buf(),
                source,
            }
        })?;
    }
    let options = SqliteConnectOptions::from_str(&format!("sqlite://{}", path.to_string_lossy()))?
        .create_if_missing(create)
        .foreign_keys(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .busy_timeout(Duration::from_secs(5));
    let pool = SqlitePoolOptions::new()
        .min_connections(1)
        .max_connections(1)
        .connect_with(options)
        .await?;
    let version: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(&pool)
        .await?;
    if !matches!(version, 0 | SCHEMA_VERSION) {
        return Err(ExecutionPeerStoreError::UnsupportedSchema {
            observed: version,
            expected: SCHEMA_VERSION,
        });
    }
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;
    if version == 0 {
        sqlx::raw_sql("PRAGMA user_version = 1")
            .execute(&pool)
            .await?;
    }
    Ok(pool)
}

async fn load_candidates(
    pool: &SqlitePool,
    maximum_entries: usize,
) -> Result<HashMap<B512, CandidateRecord>, ExecutionPeerStoreError> {
    let rows = sqlx::query(
        "SELECT candidate.peer_id, candidate.record, candidate.last_seen_unix_ms
         FROM execution_peer_candidates AS candidate
         LEFT JOIN execution_peer_quality AS quality
           ON quality.chain_id = candidate.chain_id
          AND quality.peer_id = candidate.peer_id
         WHERE candidate.chain_id = ?
         ORDER BY
           (quality.last_body_success_unix_ms IS NOT NULL) DESC,
           (quality.peer_id IS NOT NULL AND (
               quality.last_failure_unix_ms IS NULL OR
               quality.last_failure_unix_ms < MAX(
                   COALESCE(quality.last_header_success_unix_ms, 0),
                   COALESCE(quality.last_body_success_unix_ms, 0),
                   COALESCE(quality.last_receipt_success_unix_ms, 0)
               )
           )) DESC,
           COALESCE(quality.last_body_success_unix_ms, 0) DESC,
           (quality.last_receipt_success_unix_ms IS NOT NULL) DESC,
           MAX(
               COALESCE(quality.last_header_success_unix_ms, 0),
               COALESCE(quality.last_body_success_unix_ms, 0),
               COALESCE(quality.last_receipt_success_unix_ms, 0)
           ) DESC,
           COALESCE(quality.highest_served_block, 0) DESC,
           COALESCE(quality.response_latency_ms, 9223372036854775807) ASC,
           candidate.last_seen_unix_ms DESC,
           candidate.peer_id ASC
         LIMIT ?",
    )
    .bind(CHAIN_ID_MAINNET)
    .bind(i64::try_from(maximum_entries).map_err(|_| ExecutionPeerStoreError::IntegerRange)?)
    .fetch_all(pool)
    .await?;
    let mut candidates = HashMap::with_capacity(rows.len());
    for row in rows {
        let peer_id = decode_peer_id(row.try_get::<Vec<u8>, _>("peer_id")?)?;
        let record = row.try_get::<String, _>("record")?;
        let Ok(record) = record.parse::<NodeRecord>() else {
            continue;
        };
        if record.id != peer_id {
            continue;
        }
        candidates.insert(
            peer_id,
            CandidateRecord {
                record,
                last_seen_unix_ms: from_i64(row.try_get("last_seen_unix_ms")?)?,
            },
        );
    }
    Ok(candidates)
}

async fn load_quality(
    pool: &SqlitePool,
    maximum_entries: usize,
) -> Result<HashMap<B512, PeerQualityEvidence>, ExecutionPeerStoreError> {
    let rows = sqlx::query(
        "SELECT peer_id, fork_compatible, highest_served_block,
                last_header_success_unix_ms, last_body_success_unix_ms,
                last_receipt_success_unix_ms, response_latency_ms,
                last_failure_reason, last_failure_unix_ms, qualification
         FROM execution_peer_quality
         WHERE chain_id = ?
         ORDER BY
           (last_body_success_unix_ms IS NOT NULL) DESC,
           (last_failure_unix_ms IS NULL OR last_failure_unix_ms < MAX(
               COALESCE(last_header_success_unix_ms, 0),
               COALESCE(last_body_success_unix_ms, 0),
               COALESCE(last_receipt_success_unix_ms, 0)
           )) DESC,
           COALESCE(last_body_success_unix_ms, 0) DESC,
           (last_receipt_success_unix_ms IS NOT NULL) DESC,
           MAX(
               COALESCE(last_header_success_unix_ms, 0),
               COALESCE(last_body_success_unix_ms, 0),
               COALESCE(last_receipt_success_unix_ms, 0)
           ) DESC,
           COALESCE(highest_served_block, 0) DESC,
           COALESCE(response_latency_ms, 9223372036854775807) ASC,
           peer_id ASC
         LIMIT ?",
    )
    .bind(CHAIN_ID_MAINNET)
    .bind(i64::try_from(maximum_entries).map_err(|_| ExecutionPeerStoreError::IntegerRange)?)
    .fetch_all(pool)
    .await?;
    let mut quality = HashMap::with_capacity(rows.len());
    for row in rows {
        quality.insert(
            decode_peer_id(row.try_get::<Vec<u8>, _>("peer_id")?)?,
            PeerQualityEvidence {
                fork_compatible: row.try_get::<i64, _>("fork_compatible")? != 0,
                highest_served_block: optional_u64(&row, "highest_served_block")?,
                last_header_success_unix_ms: optional_u64(&row, "last_header_success_unix_ms")?,
                last_body_success_unix_ms: optional_u64(&row, "last_body_success_unix_ms")?,
                last_receipt_success_unix_ms: optional_u64(&row, "last_receipt_success_unix_ms")?,
                response_latency_ms: optional_u64(&row, "response_latency_ms")?,
                last_failure_reason: row.try_get("last_failure_reason")?,
                last_failure_unix_ms: optional_u64(&row, "last_failure_unix_ms")?,
                qualification: decode_qualification(row.try_get("qualification")?)?,
            },
        );
    }
    Ok(quality)
}

async fn upsert_quality(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    peer_id: B512,
    evidence: &PeerQualityEvidence,
) -> Result<(), ExecutionPeerStoreError> {
    sqlx::query(
        "INSERT INTO execution_peer_quality(
            chain_id, peer_id, fork_compatible, highest_served_block,
            last_header_success_unix_ms, last_body_success_unix_ms,
            last_receipt_success_unix_ms, response_latency_ms,
            last_failure_reason, last_failure_unix_ms, qualification
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(chain_id, peer_id) DO UPDATE SET
            fork_compatible = excluded.fork_compatible,
            highest_served_block = excluded.highest_served_block,
            last_header_success_unix_ms = excluded.last_header_success_unix_ms,
            last_body_success_unix_ms = excluded.last_body_success_unix_ms,
            last_receipt_success_unix_ms = excluded.last_receipt_success_unix_ms,
            response_latency_ms = excluded.response_latency_ms,
            last_failure_reason = excluded.last_failure_reason,
            last_failure_unix_ms = excluded.last_failure_unix_ms,
            qualification = excluded.qualification",
    )
    .bind(CHAIN_ID_MAINNET)
    .bind(peer_id.as_slice())
    .bind(i64::from(evidence.fork_compatible))
    .bind(optional_i64(evidence.highest_served_block)?)
    .bind(optional_i64(evidence.last_header_success_unix_ms)?)
    .bind(optional_i64(evidence.last_body_success_unix_ms)?)
    .bind(optional_i64(evidence.last_receipt_success_unix_ms)?)
    .bind(optional_i64(evidence.response_latency_ms)?)
    .bind(evidence.last_failure_reason.as_deref())
    .bind(optional_i64(evidence.last_failure_unix_ms)?)
    .bind(encode_qualification(evidence.qualification))
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn prune_database(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    maximum_entries: usize,
) -> Result<(), ExecutionPeerStoreError> {
    let maximum =
        i64::try_from(maximum_entries).map_err(|_| ExecutionPeerStoreError::IntegerRange)?;
    sqlx::query(
        "DELETE FROM execution_peer_candidates
         WHERE chain_id = ? AND peer_id NOT IN (
            SELECT candidate.peer_id
            FROM execution_peer_candidates AS candidate
            LEFT JOIN execution_peer_quality AS quality
              ON quality.chain_id = candidate.chain_id
             AND quality.peer_id = candidate.peer_id
            WHERE candidate.chain_id = ?
            ORDER BY
              (quality.last_body_success_unix_ms IS NOT NULL) DESC,
              (quality.peer_id IS NOT NULL AND (
                  quality.last_failure_unix_ms IS NULL OR
                  quality.last_failure_unix_ms < MAX(
                      COALESCE(quality.last_header_success_unix_ms, 0),
                      COALESCE(quality.last_body_success_unix_ms, 0),
                      COALESCE(quality.last_receipt_success_unix_ms, 0)
                  )
              )) DESC,
              COALESCE(quality.last_body_success_unix_ms, 0) DESC,
              (quality.last_receipt_success_unix_ms IS NOT NULL) DESC,
              MAX(
                  COALESCE(quality.last_header_success_unix_ms, 0),
                  COALESCE(quality.last_body_success_unix_ms, 0),
                  COALESCE(quality.last_receipt_success_unix_ms, 0)
              ) DESC,
              COALESCE(quality.highest_served_block, 0) DESC,
              COALESCE(quality.response_latency_ms, 9223372036854775807) ASC,
              candidate.last_seen_unix_ms DESC,
              candidate.peer_id ASC
            LIMIT ?
         )",
    )
    .bind(CHAIN_ID_MAINNET)
    .bind(CHAIN_ID_MAINNET)
    .bind(maximum)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "DELETE FROM execution_peer_quality
         WHERE chain_id = ? AND peer_id NOT IN (
            SELECT peer_id FROM execution_peer_quality
            WHERE chain_id = ?
            ORDER BY
              (last_body_success_unix_ms IS NOT NULL) DESC,
              (last_failure_unix_ms IS NULL OR last_failure_unix_ms < MAX(
                  COALESCE(last_header_success_unix_ms, 0),
                  COALESCE(last_body_success_unix_ms, 0),
                  COALESCE(last_receipt_success_unix_ms, 0)
              )) DESC,
              COALESCE(last_body_success_unix_ms, 0) DESC,
              (last_receipt_success_unix_ms IS NOT NULL) DESC,
              MAX(
                  COALESCE(last_header_success_unix_ms, 0),
                  COALESCE(last_body_success_unix_ms, 0),
                  COALESCE(last_receipt_success_unix_ms, 0)
              ) DESC,
              COALESCE(highest_served_block, 0) DESC,
              COALESCE(response_latency_ms, 9223372036854775807) ASC,
              peer_id ASC
            LIMIT ?
         )",
    )
    .bind(CHAIN_ID_MAINNET)
    .bind(CHAIN_ID_MAINNET)
    .bind(maximum)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

fn merge_quality_evidence(retained: &mut PeerQualityEvidence, incoming: PeerQualityEvidence) {
    let retained_success = PeerQualityRank::from(&*retained).last_material_success_unix_ms;
    let incoming_success = PeerQualityRank::from(&incoming).last_material_success_unix_ms;
    retained.fork_compatible |= incoming.fork_compatible;
    retained.highest_served_block = retained
        .highest_served_block
        .into_iter()
        .chain(incoming.highest_served_block)
        .max();
    retained.last_header_success_unix_ms = retained
        .last_header_success_unix_ms
        .into_iter()
        .chain(incoming.last_header_success_unix_ms)
        .max();
    retained.last_body_success_unix_ms = retained
        .last_body_success_unix_ms
        .into_iter()
        .chain(incoming.last_body_success_unix_ms)
        .max();
    retained.last_receipt_success_unix_ms = retained
        .last_receipt_success_unix_ms
        .into_iter()
        .chain(incoming.last_receipt_success_unix_ms)
        .max();
    if incoming_success >= retained_success {
        retained.response_latency_ms = incoming.response_latency_ms;
        retained.qualification = incoming.qualification;
    }
    if incoming.last_failure_unix_ms >= retained.last_failure_unix_ms {
        retained.last_failure_unix_ms = incoming.last_failure_unix_ms;
        retained.last_failure_reason = incoming.last_failure_reason;
    }
}

fn decode_peer_id(encoded: Vec<u8>) -> Result<B512, ExecutionPeerStoreError> {
    let bytes: [u8; 64] = encoded
        .try_into()
        .map_err(|_| ExecutionPeerStoreError::InvalidPeerId)?;
    Ok(B512::from(bytes))
}

fn optional_u64(
    row: &sqlx::sqlite::SqliteRow,
    column: &str,
) -> Result<Option<u64>, ExecutionPeerStoreError> {
    row.try_get::<Option<i64>, _>(column)?
        .map(from_i64)
        .transpose()
}

fn optional_i64(value: Option<u64>) -> Result<Option<i64>, ExecutionPeerStoreError> {
    value.map(to_i64).transpose()
}

fn to_i64(value: u64) -> Result<i64, ExecutionPeerStoreError> {
    i64::try_from(value).map_err(|_| ExecutionPeerStoreError::IntegerRange)
}

fn from_i64(value: i64) -> Result<u64, ExecutionPeerStoreError> {
    u64::try_from(value).map_err(|_| ExecutionPeerStoreError::IntegerRange)
}

fn encode_qualification(value: Option<PeerQualification>) -> Option<i64> {
    value.map(|value| match value {
        PeerQualification::BodyServing => 1,
        PeerQualification::HeadersOnly => 2,
        PeerQualification::Lagging => 3,
        PeerQualification::Rejected => 4,
        PeerQualification::TimedOut => 5,
    })
}

fn decode_qualification(
    value: Option<i64>,
) -> Result<Option<PeerQualification>, ExecutionPeerStoreError> {
    value
        .map(|value| match value {
            1 => Ok(PeerQualification::BodyServing),
            2 => Ok(PeerQualification::HeadersOnly),
            3 => Ok(PeerQualification::Lagging),
            4 => Ok(PeerQualification::Rejected),
            5 => Ok(PeerQualification::TimedOut),
            _ => Err(ExecutionPeerStoreError::IntegerRange),
        })
        .transpose()
}

fn observed_at_unix_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

fn bounded_quality_detail(detail: &str) -> String {
    detail.chars().take(256).collect()
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    use secp256k1::SecretKey;

    use super::*;

    fn node_record(marker: u8) -> NodeRecord {
        let secret = SecretKey::from_slice(&[marker; 32]).expect("valid test secret");
        NodeRecord::from_secret_key(
            SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::new(127, 0, 0, marker),
                30_300 + u16::from(marker),
            )),
            &secret,
        )
    }

    #[tokio::test]
    async fn persistent_pruning_is_bounded_and_keeps_proven_body_peers() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("execution-network.sqlite");
        let store = ExecutionPeerStore::new(Some(path.clone()), 2);
        let proven = node_record(1);
        let second = node_record(2);
        let excess = node_record(3);
        store.record_success(
            proven.id,
            PeerMaterialKind::Body,
            25_000_000,
            Duration::from_millis(12),
        );
        store
            .persist(vec![proven, second, excess])
            .await
            .expect("persist bounded store");

        let reopened = ExecutionPeerStore::new(Some(path), 2);
        reopened.initialize().await.expect("reopen bounded store");
        let retained = reopened.candidate_ids();
        assert_eq!(retained.len(), 2);
        assert!(retained.contains(&proven.id));
    }

    #[tokio::test]
    async fn reopening_with_a_smaller_bound_keeps_proven_body_peers() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("execution-network.sqlite");
        let store = ExecutionPeerStore::new(Some(path.clone()), 3);
        let proven = node_record(1);
        store.record_success(
            proven.id,
            PeerMaterialKind::Body,
            25_000_000,
            Duration::from_millis(12),
        );
        store
            .persist(vec![proven, node_record(2), node_record(3)])
            .await
            .expect("persist peer store");

        let reopened = ExecutionPeerStore::new(Some(path), 1);
        reopened.initialize().await.expect("reopen peer store");
        assert_eq!(reopened.candidate_ids(), HashSet::from([proven.id]));
        assert!(reopened.has_material_success(proven.id, PeerMaterialKind::Body));
    }

    #[test]
    fn volatile_quality_state_stays_bounded() {
        let store = ExecutionPeerStore::new(None, 2);
        for marker in 1..=8 {
            store.record_failure(node_record(marker).id, "unavailable");
        }
        assert_eq!(
            store
                .quality
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len(),
            2
        );
        assert_eq!(
            store
                .dirty_quality
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn unsupported_schema_fails_without_rewriting_the_store() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("execution-network.sqlite");
        let pool = open_database(&path, true).await.expect("create peer store");
        sqlx::raw_sql("PRAGMA user_version = 2")
            .execute(&pool)
            .await
            .expect("change schema version");
        pool.close().await;

        let store = ExecutionPeerStore::new(Some(path), 16);
        assert!(matches!(
            store.initialize().await,
            Err(ExecutionPeerStoreError::UnsupportedSchema {
                observed: 2,
                expected: 1
            })
        ));
    }
}
