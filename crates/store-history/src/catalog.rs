use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use leani_primitives::{
    BlockHash, BlockNumber, BlockRange, CapabilitySet, ChainId, TransactionHash, TrustModel,
};
use serde::{Deserialize, Serialize};
use sqlx::{
    Row, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use thiserror::Error;
use tokio::sync::Mutex;

use crate::segment::{
    Compression, FORMAT_VERSION, FRAME_ENCODING_VERSION, MaterialShapeId, SegmentDescriptor,
    SegmentError, SegmentId, SegmentLimits, SegmentMetadata, SegmentRead, SegmentReader,
    SegmentWriter, VerificationClass,
};

const CATALOG_SCHEMA_VERSION: i64 = 5;
/// Catalog bytes reserved for a segment's own rows and owners.
const CATALOG_RESERVATION_OVERHEAD_BYTES: u64 = 64 * 1024;
/// Catalog bytes reserved per locator row: its table and index entries, page
/// slack, and its copy in the write-ahead log. Measured at 185 to 365 bytes.
const CATALOG_LOCATOR_BYTES: u64 = 512;
/// Transaction locators reserved per block when a segment is admitted. A
/// segment with more publishes them anyway, so the catalog may outgrow the
/// physical budget by one segment's excess locators, which the next admission
/// counts.
const RESERVED_TRANSACTION_LOCATORS_PER_BLOCK: u64 = 256;
/// When more than this percentage of the catalogued segments' files are gone
/// at open, the segment directory is taken as unavailable, such as a mount not
/// yet ready or a restore in progress, rather than emptied: every row is kept
/// and reported, so the files serve again once they return.
const UNAVAILABLE_MISSING_SEGMENTS_PERCENT: u64 = 50;
/// Most segments whose verified checksum this process remembers; a segment
/// beyond it is verified again when it is next read.
const VERIFIED_SEGMENT_CACHE_CAPACITY: usize = 4_096;
/// Default cap on the bytes kept in `quarantine/`; the oldest quarantined
/// files are deleted first once it is exceeded. The cap is never below the
/// largest segment the budget admits, and the newest file is always kept.
pub const DEFAULT_QUARANTINE_MAXIMUM_BYTES: u64 = 1024 * 1024 * 1024;
const SCHEMA: &str = r"
CREATE TABLE IF NOT EXISTS catalog_meta (
    key TEXT PRIMARY KEY,
    value INTEGER NOT NULL
) WITHOUT ROWID, STRICT;

INSERT OR IGNORE INTO catalog_meta(key, value) VALUES ('schema_version', 5);

CREATE TABLE IF NOT EXISTS raw_segment_reservations (
    segment_id TEXT PRIMARY KEY,
    logical_bytes INTEGER NOT NULL CHECK(logical_bytes > 0),
    physical_bytes INTEGER NOT NULL CHECK(physical_bytes > 0),
    partial_name TEXT NOT NULL UNIQUE,
    index_name TEXT NOT NULL UNIQUE,
    final_name TEXT NOT NULL UNIQUE,
    created_at_unix_ms INTEGER NOT NULL
) WITHOUT ROWID, STRICT;

CREATE TABLE IF NOT EXISTS raw_segments (
    segment_id TEXT PRIMARY KEY,
    chain_id INTEGER NOT NULL CHECK(chain_id > 0),
    start_block INTEGER NOT NULL CHECK(start_block >= 0),
    end_block INTEGER NOT NULL CHECK(end_block >= start_block),
    material_shape BLOB NOT NULL CHECK(length(material_shape) = 32),
    present_capabilities INTEGER NOT NULL CHECK(present_capabilities >= 0),
    complete_capabilities INTEGER NOT NULL CHECK(complete_capabilities >= 0),
    verification_class INTEGER NOT NULL CHECK(verification_class BETWEEN 0 AND 2),
    trust_model INTEGER NOT NULL CHECK(trust_model BETWEEN 0 AND 3),
    format_version INTEGER NOT NULL CHECK(format_version = 1),
    frame_encoding_version INTEGER NOT NULL CHECK(frame_encoding_version = 1),
    compression INTEGER NOT NULL CHECK(compression BETWEEN 0 AND 2),
    history_profile INTEGER NOT NULL CHECK(history_profile IN (0, 1)),
    merge_block INTEGER CHECK(merge_block IS NULL OR merge_block >= 0),
    block_hash_indexed INTEGER NOT NULL CHECK(block_hash_indexed IN (0, 1)),
    transaction_hash_indexed INTEGER NOT NULL CHECK(transaction_hash_indexed IN (0, 1)),
    relative_path TEXT NOT NULL UNIQUE,
    logical_bytes INTEGER NOT NULL CHECK(logical_bytes > 0),
    physical_bytes INTEGER NOT NULL CHECK(physical_bytes > 0),
    first_parent_hash BLOB NOT NULL CHECK(length(first_parent_hash) = 32),
    last_hash BLOB NOT NULL CHECK(length(last_hash) = 32),
    ordered_hash_digest BLOB NOT NULL CHECK(length(ordered_hash_digest) = 32),
    records_checksum BLOB NOT NULL CHECK(length(records_checksum) = 32),
    content_checksum BLOB NOT NULL CHECK(length(content_checksum) = 32),
    state TEXT NOT NULL CHECK(state IN ('closed', 'deleting')),
    created_at_unix_ms INTEGER NOT NULL,
    CHECK(
        (history_profile = 0 AND merge_block IS NULL)
        OR (history_profile = 1 AND merge_block IS NOT NULL)
    )
) STRICT;

CREATE INDEX IF NOT EXISTS raw_segments_range_lookup
    ON raw_segments(chain_id, start_block, end_block, state);

CREATE UNIQUE INDEX IF NOT EXISTS raw_segments_material_identity
    ON raw_segments(
        chain_id, start_block, end_block, material_shape,
        present_capabilities, complete_capabilities,
        verification_class, trust_model, history_profile,
        COALESCE(merge_block, -1), block_hash_indexed, transaction_hash_indexed
    );

CREATE TABLE IF NOT EXISTS raw_segment_owners (
    segment_id TEXT NOT NULL
        REFERENCES raw_segments(segment_id) ON DELETE CASCADE,
    owner_kind TEXT NOT NULL
        CHECK(owner_kind IN ('raw_history_job', 'processor_job', 'operator_pin')),
    owner_id TEXT NOT NULL,
    created_at_unix_ms INTEGER NOT NULL,
    PRIMARY KEY(segment_id, owner_kind, owner_id)
) WITHOUT ROWID, STRICT;

CREATE INDEX IF NOT EXISTS raw_segment_owners_by_owner
    ON raw_segment_owners(owner_kind, owner_id, segment_id);

CREATE TABLE IF NOT EXISTS raw_block_hash_locators (
    chain_id INTEGER NOT NULL CHECK(chain_id > 0),
    block_hash BLOB NOT NULL CHECK(length(block_hash) = 32),
    segment_id TEXT NOT NULL
        REFERENCES raw_segments(segment_id) ON DELETE CASCADE,
    block_number INTEGER NOT NULL CHECK(block_number >= 0),
    PRIMARY KEY(chain_id, block_hash, segment_id)
) WITHOUT ROWID, STRICT;

CREATE INDEX IF NOT EXISTS raw_block_hash_locators_by_segment
    ON raw_block_hash_locators(segment_id);

CREATE TABLE IF NOT EXISTS raw_transaction_locators (
    chain_id INTEGER NOT NULL CHECK(chain_id > 0),
    transaction_hash BLOB NOT NULL CHECK(length(transaction_hash) = 32),
    segment_id TEXT NOT NULL
        REFERENCES raw_segments(segment_id) ON DELETE CASCADE,
    block_number INTEGER NOT NULL CHECK(block_number >= 0),
    transaction_index INTEGER NOT NULL CHECK(transaction_index >= 0),
    PRIMARY KEY(chain_id, transaction_hash, segment_id)
) WITHOUT ROWID, STRICT;

CREATE INDEX IF NOT EXISTS raw_transaction_locators_by_segment
    ON raw_transaction_locators(segment_id);

CREATE TABLE IF NOT EXISTS raw_history_jobs (
    job_id TEXT PRIMARY KEY,
    identity BLOB NOT NULL UNIQUE CHECK(length(identity) = 32),
    spec BLOB NOT NULL,
    state TEXT NOT NULL
        CHECK(state IN ('queued', 'running', 'storage_backpressured', 'complete', 'cancelled', 'failed')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts >= 0),
    committed_segments INTEGER NOT NULL DEFAULT 0 CHECK(committed_segments >= 0),
    committed_logical_bytes INTEGER NOT NULL DEFAULT 0 CHECK(committed_logical_bytes >= 0),
    committed_physical_bytes INTEGER NOT NULL DEFAULT 0 CHECK(committed_physical_bytes >= 0),
    last_error TEXT,
    created_at_unix_ms INTEGER NOT NULL,
    updated_at_unix_ms INTEGER NOT NULL
) WITHOUT ROWID, STRICT;

CREATE INDEX IF NOT EXISTS raw_history_jobs_state
    ON raw_history_jobs(state, updated_at_unix_ms, job_id);

CREATE TABLE IF NOT EXISTS raw_history_job_ranges (
    job_id TEXT NOT NULL
        REFERENCES raw_history_jobs(job_id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
    start_block INTEGER NOT NULL CHECK(start_block >= 0),
    end_block INTEGER NOT NULL CHECK(end_block >= start_block),
    PRIMARY KEY(job_id, ordinal),
    UNIQUE(job_id, start_block, end_block)
) WITHOUT ROWID, STRICT;
";

/// Independent logical and physical ceilings for retained raw history.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StorageBudget {
    pub maximum_logical_bytes: u64,
    pub maximum_physical_bytes: u64,
    pub maximum_frame_logical_bytes: u64,
    pub maximum_segment_logical_bytes: u64,
    pub maximum_segment_physical_bytes: u64,
}

impl Default for StorageBudget {
    fn default() -> Self {
        Self {
            maximum_logical_bytes: u64::MAX,
            maximum_physical_bytes: u64::MAX,
            maximum_frame_logical_bytes: 64 * 1024 * 1024,
            maximum_segment_logical_bytes: 4 * 1024 * 1024 * 1024,
            maximum_segment_physical_bytes: 4 * 1024 * 1024 * 1024,
        }
    }
}

impl StorageBudget {
    fn validate(self) -> Result<Self, HistoryStoreError> {
        if self.maximum_logical_bytes == 0
            || self.maximum_physical_bytes == 0
            || self.maximum_frame_logical_bytes == 0
            || self.maximum_segment_logical_bytes == 0
            || self.maximum_segment_physical_bytes == 0
            || self.maximum_frame_logical_bytes > self.maximum_segment_logical_bytes
            || self.maximum_segment_logical_bytes > self.maximum_logical_bytes
            || self.maximum_segment_physical_bytes > self.maximum_physical_bytes
        {
            return Err(HistoryStoreError::InvalidConfig(
                "history budgets must be non-zero and frame <= segment <= store".to_owned(),
            ));
        }
        Ok(self)
    }
}

/// Raw-history store location and admission policy.
#[derive(Clone, Debug)]
pub struct HistoryStoreConfig {
    pub root: PathBuf,
    pub reader_connections: u32,
    pub budget: StorageBudget,
    /// Bytes of quarantined segment files kept for inspection; never less
    /// than the largest segment the budget admits, and the newest file is
    /// kept whatever its size.
    pub quarantine_maximum_bytes: u64,
}

impl HistoryStoreConfig {
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            reader_connections: 4,
            budget: StorageBudget::default(),
            quarantine_maximum_bytes: DEFAULT_QUARANTINE_MAXIMUM_BYTES,
        }
    }

    #[must_use]
    pub const fn with_budget(mut self, budget: StorageBudget) -> Self {
        self.budget = budget;
        self
    }
}

/// Upper bounds reserved before a partial segment is opened.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentReservation {
    pub maximum_logical_bytes: u64,
    pub maximum_physical_bytes: u64,
}

impl SegmentReservation {
    #[must_use]
    pub const fn new(maximum_logical_bytes: u64, maximum_physical_bytes: u64) -> Self {
        Self {
            maximum_logical_bytes,
            maximum_physical_bytes,
        }
    }
}

/// Why a retained segment must not be deleted.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SegmentOwnerKind {
    RawHistoryJob,
    ProcessorJob,
    OperatorPin,
}

impl SegmentOwnerKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::RawHistoryJob => "raw_history_job",
            Self::ProcessorJob => "processor_job",
            Self::OperatorPin => "operator_pin",
        }
    }

    fn parse(value: &str) -> Result<Self, HistoryStoreError> {
        match value {
            "raw_history_job" => Ok(Self::RawHistoryJob),
            "processor_job" => Ok(Self::ProcessorJob),
            "operator_pin" => Ok(Self::OperatorPin),
            other => Err(HistoryStoreError::CatalogIntegrity(format!(
                "unknown owner kind `{other}`"
            ))),
        }
    }
}

/// Durable segment ownership record.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SegmentOwner {
    pub segment_id: SegmentId,
    pub kind: SegmentOwnerKind,
    pub owner_id: String,
    pub created_at_unix_ms: u64,
}

/// Ownership installed atomically with a newly closed segment.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct SegmentOwnerClaim {
    pub kind: SegmentOwnerKind,
    pub owner_id: String,
}

/// Catalog representation of one closed segment.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SegmentRecord {
    pub metadata: SegmentMetadata,
    pub profile: crate::RawHistoryProfile,
    pub indexes: crate::RawHistoryIndexPolicy,
    pub relative_path: String,
    pub created_at_unix_ms: u64,
}

/// Durable result of a canonical block-hash lookup.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BlockHashLocator {
    pub segment_id: SegmentId,
    pub block_number: BlockNumber,
}

/// Durable result of a canonical transaction-hash lookup.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TransactionLocator {
    pub segment_id: SegmentId,
    pub block_number: BlockNumber,
    pub transaction_index: u32,
}

/// Cleanup and validation work performed before the store becomes readable.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RecoveryReport {
    pub removed_partial_files: u64,
    pub cleared_reservations: u64,
    pub completed_deletions: u64,
    /// Closed files no catalog row claims, left by a crash before their
    /// publication committed.
    pub quarantined_closed_files: u64,
    /// Catalogued segments whose file no longer had its closed length; their
    /// coverage was dropped.
    pub quarantined_corrupt_files: u64,
    /// Catalogued segments whose file was gone; their coverage was dropped.
    pub missing_segments: u64,
    /// Catalogued segments whose file was gone when most were, which kept
    /// their rows: the segment directory looked unavailable, not emptied.
    pub unavailable_segments: u64,
    /// Quarantined files deleted to keep the quarantine within its cap.
    pub collected_quarantine_files: u64,
}

/// Observable retained, reserved, quarantined, and catalog storage.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HistoryStoreStats {
    pub closed_segments: u64,
    pub owners: u64,
    pub block_hash_locators: u64,
    pub transaction_locators: u64,
    pub retained_logical_bytes: u64,
    pub retained_segment_physical_bytes: u64,
    pub reserved_logical_bytes: u64,
    pub reserved_physical_bytes: u64,
    pub catalog_physical_bytes: u64,
    pub temporary_physical_bytes: u64,
    pub quarantine_physical_bytes: u64,
    pub total_physical_bytes: u64,
}

#[derive(Debug)]
pub(crate) struct HistoryStoreInner {
    pub(crate) pool: SqlitePool,
    root: PathBuf,
    segments: PathBuf,
    quarantine: PathBuf,
    catalog_path: PathBuf,
    budget: StorageBudget,
    quarantine_maximum_bytes: u64,
    pub(crate) lifecycle: Mutex<()>,
    recovery: RecoveryReport,
    /// Segments whose whole-file checksum this process verified.
    verified: std::sync::Mutex<VerifiedSegments>,
    /// A lock per segment whose whole file is being verified, so concurrent
    /// first reads of one segment hash it once, and a file that stalls holds
    /// up only its own readers.
    verifying: std::sync::Mutex<BTreeMap<SegmentId, Arc<Mutex<()>>>>,
    /// Whole-segment checksum validations performed by this process.
    validations: AtomicU64,
}

impl HistoryStoreInner {
    fn is_verified(&self, key: &VerifiedKey) -> bool {
        self.verified
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .touch(key)
    }

    fn remember_verified(&self, key: VerifiedKey) {
        self.verified
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(key);
    }

    fn forget_verified(&self, id: &SegmentId) {
        self.verified
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .forget(id);
    }

    /// The lock held while `id`'s whole file is verified.
    fn verification_lock(&self, id: &SegmentId) -> Arc<Mutex<()>> {
        let mut locks = self
            .verifying
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // A lock no reader holds any more is dropped as others are taken.
        locks.retain(|_, lock| Arc::strong_count(lock) > 1);
        locks.entry(id.clone()).or_default().clone()
    }
}

type VerifiedKey = (SegmentId, [u8; 32]);

/// Segments, by ID and content checksum, whose whole-file checksum this
/// process verified; past its capacity, the least recently read is dropped.
#[derive(Debug, Default)]
struct VerifiedSegments {
    last_use: BTreeMap<VerifiedKey, u64>,
    by_use: BTreeMap<u64, VerifiedKey>,
    uses: u64,
}

impl VerifiedSegments {
    /// Whether `key` was verified, marking it as just read.
    fn touch(&mut self, key: &VerifiedKey) -> bool {
        let Some(last_use) = self.last_use.get_mut(key) else {
            return false;
        };
        self.by_use.remove(last_use);
        self.uses += 1;
        *last_use = self.uses;
        self.by_use.insert(self.uses, key.clone());
        true
    }

    fn insert(&mut self, key: VerifiedKey) {
        if self.touch(&key) {
            return;
        }
        self.uses += 1;
        self.last_use.insert(key.clone(), self.uses);
        self.by_use.insert(self.uses, key);
        while self.last_use.len() > VERIFIED_SEGMENT_CACHE_CAPACITY {
            let Some((_, oldest)) = self.by_use.pop_first() else {
                break;
            };
            self.last_use.remove(&oldest);
        }
    }

    fn forget(&mut self, id: &SegmentId) {
        let forgotten = self
            .last_use
            .range((id.clone(), [0; 32])..=(id.clone(), [u8::MAX; 32]))
            .map(|(key, last_use)| (key.clone(), *last_use))
            .collect::<Vec<_>>();
        for (key, last_use) in forgotten {
            self.last_use.remove(&key);
            self.by_use.remove(&last_use);
        }
    }
}

/// Cloneable raw-history catalog and immutable segment owner.
#[derive(Clone, Debug)]
pub struct HistoryStore {
    pub(crate) inner: Arc<HistoryStoreInner>,
}

impl HistoryStore {
    /// Open the catalog and recover interrupted publications and deletions.
    /// Segments are verified lazily: a catalogued segment whose file is gone
    /// or no longer has its closed length loses its coverage, the latter
    /// quarantined, and every other segment's whole-file checksum is
    /// verified the first time it is read. When most segment files are gone,
    /// the directory is taken as unavailable and every row is kept, reported
    /// as [`RecoveryReport::unavailable_segments`]. A store already over its
    /// budget opens, and admits no new segment until it is back under it.
    ///
    /// # Errors
    ///
    /// Fails on an invalid configuration, catalog I/O, or a malformed
    /// catalog.
    pub async fn open(config: HistoryStoreConfig) -> Result<Self, HistoryStoreError> {
        if config.reader_connections == 0 {
            return Err(HistoryStoreError::InvalidConfig(
                "reader_connections must be greater than zero".to_owned(),
            ));
        }
        let budget = config.budget.validate()?;
        fs::create_dir_all(&config.root)?;
        let segments = config.root.join("segments");
        let quarantine = config.root.join("quarantine");
        fs::create_dir_all(&segments)?;
        fs::create_dir_all(&quarantine)?;
        let catalog_path = config.root.join("catalog.sqlite");
        // A plain filename, not a `sqlite://` URL: URL parsing percent-decodes
        // the path and treats `?` as the start of connection parameters.
        let options = SqliteConnectOptions::new()
            .filename(&catalog_path)
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full);
        let pool = SqlitePoolOptions::new()
            .min_connections(1)
            .max_connections(config.reader_connections.saturating_add(1))
            .connect_with(options)
            .await?;
        sqlx::raw_sql(SCHEMA).execute(&pool).await?;
        let version: i64 =
            sqlx::query_scalar("SELECT value FROM catalog_meta WHERE key = 'schema_version'")
                .fetch_one(&pool)
                .await?;
        if version != CATALOG_SCHEMA_VERSION {
            return Err(HistoryStoreError::InvalidConfig(format!(
                "history catalog schema {version} is unsupported"
            )));
        }
        // The quarantine holds at least one segment of the largest size the
        // budget admits, so a quarantined segment stays for inspection.
        let quarantine_maximum_bytes = config
            .quarantine_maximum_bytes
            .max(budget.maximum_segment_physical_bytes);
        let recovery = {
            let (pool, segments, quarantine) = (pool.clone(), segments.clone(), quarantine.clone());
            let runtime = tokio::runtime::Handle::current();
            // Recovery reads, moves, and deletes files, so it runs off the
            // async runtime.
            blocking(move || {
                runtime.block_on(recover(
                    &pool,
                    &segments,
                    &quarantine,
                    quarantine_maximum_bytes,
                ))
            })
            .await??
        };
        crate::job::reconcile_all_jobs(&pool).await?;
        let inner = Arc::new(HistoryStoreInner {
            pool,
            root: config.root,
            segments,
            quarantine,
            catalog_path,
            budget,
            quarantine_maximum_bytes,
            lifecycle: Mutex::new(()),
            recovery,
            verified: std::sync::Mutex::default(),
            verifying: std::sync::Mutex::default(),
            validations: AtomicU64::new(0),
        });
        Ok(Self { inner })
    }

    #[must_use]
    pub fn recovery_report(&self) -> RecoveryReport {
        self.inner.recovery
    }

    /// Reserve store-wide capacity and open a streaming partial segment.
    ///
    /// # Errors
    ///
    /// Rejects duplicate IDs, invalid reservations, and any request that would
    /// exceed the independently configured logical or physical budget.
    pub async fn begin_segment(
        &self,
        id: SegmentId,
        descriptor: SegmentDescriptor,
        compression: Compression,
        reservation: SegmentReservation,
    ) -> Result<PendingSegment, HistoryStoreError> {
        self.begin_segment_indexed(
            id,
            descriptor,
            compression,
            reservation,
            crate::RawHistoryIndexPolicy::default(),
        )
        .await
    }

    /// Reserve capacity and open a segment whose selected locators publish in
    /// the same catalog transaction as the closed segment.
    ///
    /// # Errors
    ///
    /// Applies the same admission and identity checks as [`Self::begin_segment`].
    #[allow(clippy::too_many_lines)]
    pub async fn begin_segment_indexed(
        &self,
        id: SegmentId,
        descriptor: SegmentDescriptor,
        compression: Compression,
        reservation: SegmentReservation,
        indexes: crate::RawHistoryIndexPolicy,
    ) -> Result<PendingSegment, HistoryStoreError> {
        self.begin_segment_profiled(
            id,
            descriptor,
            compression,
            reservation,
            crate::RawHistoryProfile::ProcessorReuse,
            indexes,
        )
        .await
    }

    /// Open a segment with an explicit durable product-profile certification.
    /// The runner uses this path after validating every frame against the job
    /// profile; generic store callers default to processor-reuse only.
    ///
    /// # Errors
    ///
    /// Applies the same admission and identity checks as [`Self::begin_segment`].
    #[allow(clippy::too_many_lines)]
    pub async fn begin_segment_profiled(
        &self,
        id: SegmentId,
        descriptor: SegmentDescriptor,
        compression: Compression,
        reservation: SegmentReservation,
        profile: crate::RawHistoryProfile,
        indexes: crate::RawHistoryIndexPolicy,
    ) -> Result<PendingSegment, HistoryStoreError> {
        let _guard = self.inner.lifecycle.lock().await;
        if indexes.logs {
            return Err(HistoryStoreError::InvalidJob(
                "raw log indexes are not implemented".to_owned(),
            ));
        }
        self.validate_reservation(reservation)?;
        let partial_name = format!("{}.partial", id.as_str());
        let index_name = format!("{}.idxpartial", id.as_str());
        let final_name = format!("{}.idxraw", id.as_str());
        let partial_path = self.inner.segments.join(&partial_name);
        let index_path = self.inner.segments.join(&index_name);
        let final_path = self.inner.segments.join(&final_name);
        let paths = [partial_path.clone(), index_path.clone(), final_path.clone()];
        if blocking(move || paths.iter().any(|path| path.exists())).await? {
            self.clear_stale_files(
                &id,
                [partial_path.clone(), index_path.clone()],
                final_path.clone(),
            )
            .await?;
        }
        let reserved_physical_bytes = self.admit(descriptor.range, reservation, indexes).await?;

        let now = unix_ms()?;
        sqlx::query(
            "INSERT INTO raw_segment_reservations(
                segment_id, logical_bytes, physical_bytes,
                partial_name, index_name, final_name, created_at_unix_ms
             ) VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(id.as_str())
        .bind(u64_i64(
            reservation.maximum_logical_bytes,
            "reserved logical bytes",
        )?)
        .bind(u64_i64(reserved_physical_bytes, "reserved physical bytes")?)
        .bind(&partial_name)
        .bind(&index_name)
        .bind(&final_name)
        .bind(u64_i64(now, "reservation creation time")?)
        .execute(&self.inner.pool)
        .await?;

        let limits = SegmentLimits {
            maximum_frame_logical_bytes: self
                .inner
                .budget
                .maximum_frame_logical_bytes
                .min(reservation.maximum_logical_bytes),
            maximum_segment_logical_bytes: reservation.maximum_logical_bytes,
            maximum_segment_physical_bytes: reservation.maximum_physical_bytes,
        };
        let created = {
            let (partial_path, index_path, id) =
                (partial_path.clone(), index_path.clone(), id.clone());
            blocking(move || {
                SegmentWriter::create(
                    partial_path,
                    index_path,
                    id,
                    descriptor,
                    compression,
                    limits,
                )
            })
            .await
        };
        let writer = match created.and_then(|created| created.map_err(HistoryStoreError::from)) {
            Ok(writer) => writer,
            Err(error) => {
                sqlx::query("DELETE FROM raw_segment_reservations WHERE segment_id = ?")
                    .bind(id.as_str())
                    .execute(&self.inner.pool)
                    .await?;
                return Err(error);
            }
        };
        Ok(PendingSegment {
            store: self.clone(),
            id,
            writer: Some(writer),
            partial_path,
            index_path,
            final_path,
            final_name,
            reservation,
            closed: false,
            profile,
            indexes,
            block_locators: Vec::new(),
            transaction_locators: Vec::new(),
        })
    }

    /// Whether the store would admit a segment of `reservation` over `range`
    /// now. Nothing is reserved, so a job can wait for room without opening
    /// a source.
    ///
    /// # Errors
    ///
    /// Returns the error the segment's admission would: an invalid
    /// reservation, or a budget it would exceed.
    pub(crate) async fn check_admission(
        &self,
        range: BlockRange,
        reservation: SegmentReservation,
        indexes: crate::RawHistoryIndexPolicy,
    ) -> Result<(), HistoryStoreError> {
        self.validate_reservation(reservation)?;
        self.admit(range, reservation, indexes).await.map(|_| ())
    }

    /// The physical bytes a segment of `reservation` over `range` reserves,
    /// once the budgets admit it beside everything the store holds and has
    /// reserved.
    async fn admit(
        &self,
        range: BlockRange,
        reservation: SegmentReservation,
        indexes: crate::RawHistoryIndexPolicy,
    ) -> Result<u64, HistoryStoreError> {
        // The catalog grows by the segment's rows and one locator row per
        // indexed block and transaction, so that is reserved too.
        let estimated_locators = if indexes.block_hash { range.len() } else { 0 }
            .checked_add(if indexes.transaction_hash {
                range
                    .len()
                    .checked_mul(RESERVED_TRANSACTION_LOCATORS_PER_BLOCK)
                    .ok_or(HistoryStoreError::ArithmeticOverflow)?
            } else {
                0
            })
            .ok_or(HistoryStoreError::ArithmeticOverflow)?;
        let catalog_reservation = catalog_growth(estimated_locators)?;
        let reserved_physical_bytes = reservation
            .maximum_physical_bytes
            .checked_add(catalog_reservation)
            .ok_or(HistoryStoreError::ArithmeticOverflow)?;
        let (retained_logical, reserved_logical, reserved_physical) =
            catalog_capacity(&self.inner.pool).await?;
        let root = self.inner.root.clone();
        let root_physical = blocking(move || directory_bytes(&root)).await??;
        let temporary_physical =
            reservation_temporary_bytes(&self.inner.pool, &self.inner.segments).await?;
        let projected_logical = retained_logical
            .checked_add(reserved_logical)
            .and_then(|value| value.checked_add(reservation.maximum_logical_bytes))
            .ok_or(HistoryStoreError::ArithmeticOverflow)?;
        let effective_physical = root_physical
            .saturating_sub(temporary_physical)
            .checked_add(reserved_physical)
            .and_then(|value| value.checked_add(reserved_physical_bytes))
            .ok_or(HistoryStoreError::ArithmeticOverflow)?;
        if projected_logical > self.inner.budget.maximum_logical_bytes {
            return Err(HistoryStoreError::LogicalBudget {
                limit: self.inner.budget.maximum_logical_bytes,
                observed: projected_logical,
            });
        }
        if effective_physical > self.inner.budget.maximum_physical_bytes {
            return Err(HistoryStoreError::PhysicalBudget {
                limit: self.inner.budget.maximum_physical_bytes,
                observed: effective_physical,
            });
        }
        Ok(reserved_physical_bytes)
    }

    /// Clear the files of segment `id` that no catalog row or reservation
    /// claims, such as a closed file a failed quarantine left behind: the
    /// closed file moves to the quarantine, and partial files are deleted.
    /// Files something claims collide.
    async fn clear_stale_files(
        &self,
        id: &SegmentId,
        partials: [PathBuf; 2],
        closed: PathBuf,
    ) -> Result<(), HistoryStoreError> {
        let claimed: i64 = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM raw_segments WHERE segment_id = ?)
                 OR EXISTS(SELECT 1 FROM raw_segment_reservations WHERE segment_id = ?)",
        )
        .bind(id.as_str())
        .bind(id.as_str())
        .fetch_one(&self.inner.pool)
        .await?;
        if claimed != 0 {
            return Err(HistoryStoreError::PathCollision(id.clone()));
        }
        let segments = self.inner.segments.clone();
        let quarantine = self.inner.quarantine.clone();
        let maximum_bytes = self.inner.quarantine_maximum_bytes;
        blocking(move || {
            for partial in &partials {
                remove_if_exists(partial)?;
            }
            if fs::symlink_metadata(&closed).is_ok() {
                quarantine_file(&closed, &quarantine, true)?;
                sync_directory(&quarantine)?;
                collect_quarantine(&quarantine, maximum_bytes)?;
            }
            sync_directory(&segments)
        })
        .await?
    }

    /// Whether `record`'s file is gone.
    pub(crate) async fn segment_file_missing(
        &self,
        record: &SegmentRecord,
    ) -> Result<bool, HistoryStoreError> {
        let path = self.resolve_relative_path(&record.relative_path)?;
        blocking(move || match fs::symlink_metadata(&path) {
            Ok(_) => Ok(false),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(true),
            Err(error) => Err(error.into()),
        })
        .await?
    }

    /// Drop `record`, whose file is gone, from the catalog, so its blocks
    /// read as missing and the jobs that owned it acquire them again.
    pub(crate) async fn drop_missing_segment(
        &self,
        record: &SegmentRecord,
    ) -> Result<(), HistoryStoreError> {
        let _guard = self.inner.lifecycle.lock().await;
        self.inner.forget_verified(&record.metadata.id);
        drop_segment(&self.inner.pool, &record.metadata, "is missing from disk").await?;
        Ok(())
    }

    /// List every readable closed segment in canonical range order.
    ///
    /// # Errors
    ///
    /// Returns an error if catalog rows are malformed or unavailable.
    pub async fn segments(&self) -> Result<Vec<SegmentRecord>, HistoryStoreError> {
        let rows = sqlx::query(
            "SELECT * FROM raw_segments
             WHERE state = 'closed'
             ORDER BY chain_id, start_block, end_block, segment_id",
        )
        .fetch_all(&self.inner.pool)
        .await?;
        rows.iter().map(segment_from_row).collect()
    }

    /// Inspect one readable segment.
    ///
    /// # Errors
    ///
    /// Returns an error if catalog rows are malformed or unavailable.
    pub async fn segment(
        &self,
        id: &SegmentId,
    ) -> Result<Option<SegmentRecord>, HistoryStoreError> {
        let row =
            sqlx::query("SELECT * FROM raw_segments WHERE segment_id = ? AND state = 'closed'")
                .bind(id.as_str())
                .fetch_optional(&self.inner.pool)
                .await?;
        row.as_ref().map(segment_from_row).transpose()
    }

    /// Open one catalogued segment. Its whole-file checksum is verified the
    /// first time this process opens it, or when it wrote it; later opens
    /// only check the file's length, and every read checks its record.
    ///
    /// # Errors
    ///
    /// Returns an error if the segment is unknown or unreadable, and
    /// [`HistoryStoreError::Quarantined`] if it failed validation.
    pub async fn reader(&self, id: &SegmentId) -> Result<SegmentReader, HistoryStoreError> {
        let record = self
            .segment(id)
            .await?
            .ok_or_else(|| HistoryStoreError::UnknownSegment(id.clone()))?;
        self.open_record(&record).await
    }

    /// Seek and decode one block from a named segment.
    ///
    /// # Errors
    ///
    /// Returns an error if the segment or requested record is unreadable,
    /// and [`HistoryStoreError::Quarantined`] if either failed validation.
    pub async fn read_block(
        &self,
        id: &SegmentId,
        block: BlockNumber,
    ) -> Result<SegmentRead, HistoryStoreError> {
        let record = self
            .segment(id)
            .await?
            .ok_or_else(|| HistoryStoreError::UnknownSegment(id.clone()))?;
        let mut reader = self.open_record(&record).await?;
        match blocking(move || reader.read_block(block)).await? {
            Ok(read) => Ok(read),
            Err(error) => Err(self.segment_failure(&record, error).await),
        }
    }

    /// Whole-segment checksum validations this process has performed.
    #[must_use]
    pub fn segment_validations(&self) -> u64 {
        self.inner.validations.load(Ordering::Relaxed)
    }

    /// Open `record`'s file off the async runtime, verifying its whole-file
    /// checksum unless this process already did.
    async fn open_record(
        &self,
        record: &SegmentRecord,
    ) -> Result<SegmentReader, HistoryStoreError> {
        let path = self.resolve_relative_path(&record.relative_path)?;
        let key = (record.metadata.id.clone(), record.metadata.content_checksum);
        let metadata = record.metadata.clone();
        let opened = if self.inner.is_verified(&key) {
            blocking(move || SegmentReader::open_verified(path, metadata)).await?
        } else {
            let verifying = self
                .inner
                .verification_lock(&record.metadata.id)
                .lock_owned()
                .await;
            let inner = self.inner.clone();
            blocking(move || {
                // The lock is held, and the result recorded, until the hash
                // finishes, even when the reader stops waiting for it.
                let _verifying = verifying;
                if inner.is_verified(&key) {
                    return SegmentReader::open_verified(path, metadata);
                }
                inner.validations.fetch_add(1, Ordering::Relaxed);
                let reader = SegmentReader::open(path, metadata.id.clone())?;
                if reader.metadata() != &metadata {
                    return Err(SegmentError::CatalogMismatch);
                }
                inner.remember_verified(key);
                Ok(reader)
            })
            .await?
        };
        match opened {
            Ok(reader) => Ok(reader),
            Err(error) => Err(self.segment_failure(record, error).await),
        }
    }

    /// Report a failed read of a segment opened through [`Self::reader`],
    /// quarantining it when its bytes failed validation.
    pub(crate) async fn segment_read_failed(
        &self,
        metadata: &SegmentMetadata,
        error: SegmentError,
    ) -> HistoryStoreError {
        match self.segment(&metadata.id).await {
            Ok(Some(record)) if record.metadata == *metadata => {
                self.segment_failure(&record, error).await
            }
            // Another reader already dropped it.
            Ok(_) if error.is_corruption() || error.is_missing_file() => {
                HistoryStoreError::Quarantined {
                    id: metadata.id.clone(),
                    reason: error.to_string(),
                }
            }
            Ok(_) => error.into(),
            Err(lookup) => lookup,
        }
    }

    /// The error a failed open or read of `record` returns. A file whose
    /// bytes failed validation is quarantined, dropping the segment's
    /// coverage. A transient I/O failure, or a file that is gone, as when its
    /// mount went away, leaves the segment in place; opening the store again
    /// drops a segment whose file is still gone.
    async fn segment_failure(
        &self,
        record: &SegmentRecord,
        error: SegmentError,
    ) -> HistoryStoreError {
        if !error.is_corruption() {
            return error.into();
        }
        self.quarantine(record, error.to_string()).await
    }

    /// Drop `record`'s coverage and move its file to the quarantine, which is
    /// then collected down to its cap. Returns
    /// [`HistoryStoreError::Quarantined`] once the coverage is dropped, even
    /// if the file cannot be moved: it is then quarantined as an orphan when
    /// the store next opens.
    async fn quarantine(&self, record: &SegmentRecord, reason: String) -> HistoryStoreError {
        let _guard = self.inner.lifecycle.lock().await;
        self.inner.forget_verified(&record.metadata.id);
        let id = record.metadata.id.clone();
        let detail = format!("failed validation and was quarantined: {reason}");
        match drop_segment(&self.inner.pool, &record.metadata, &detail).await {
            Ok(true) => {}
            // Another reader already dropped it.
            Ok(false) => return HistoryStoreError::Quarantined { id, reason },
            Err(error) => return error,
        }
        tracing::warn!(
            segment = id.as_str(),
            %reason,
            "quarantined a retained segment that failed validation; its blocks are no longer retained"
        );
        let moved = match self.resolve_relative_path(&record.relative_path) {
            Ok(path) => {
                let segments = self.inner.segments.clone();
                let quarantine = self.inner.quarantine.clone();
                let maximum_bytes = self.inner.quarantine_maximum_bytes;
                blocking(move || {
                    match fs::symlink_metadata(&path) {
                        Ok(_) => quarantine_file(&path, &quarantine, false)?,
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(error) => return Err(error.into()),
                    }
                    sync_directory(&segments)?;
                    sync_directory(&quarantine)?;
                    collect_quarantine(&quarantine, maximum_bytes)?;
                    Ok(())
                })
                .await
                .and_then(|moved| moved)
            }
            Err(error) => Err(error),
        };
        if let Err(error) = moved {
            tracing::warn!(
                segment = id.as_str(),
                %error,
                "could not move a quarantined segment's file; it is quarantined when the store next opens"
            );
        }
        HistoryStoreError::Quarantined { id, reason }
    }

    /// Resolve one canonical block hash to a closed retained segment.
    ///
    /// # Errors
    ///
    /// Returns an error if locator metadata is malformed or unavailable.
    pub async fn block_hash_locators(
        &self,
        chain_id: ChainId,
        hash: BlockHash,
    ) -> Result<Vec<BlockHashLocator>, HistoryStoreError> {
        let rows = sqlx::query(
            "SELECT locator.segment_id, locator.block_number
             FROM raw_block_hash_locators AS locator
             JOIN raw_segments AS segment ON segment.segment_id = locator.segment_id
             WHERE locator.chain_id = ? AND locator.block_hash = ? AND segment.state = 'closed'
             ORDER BY segment.verification_class DESC, segment.trust_model DESC, locator.segment_id",
        )
        .bind(u64_i64(chain_id.0, "chain ID")?)
        .bind(hash.as_array().as_slice())
        .fetch_all(&self.inner.pool)
        .await?;
        rows.iter()
            .map(|row| {
                Ok(BlockHashLocator {
                    segment_id: SegmentId::new(row.try_get::<String, _>("segment_id")?)?,
                    block_number: BlockNumber(i64_u64(
                        row.try_get("block_number")?,
                        "block locator number",
                    )?),
                })
            })
            .collect()
    }

    /// Resolve one canonical transaction hash to its retained block and index.
    ///
    /// # Errors
    ///
    /// Returns an error if locator metadata is malformed or unavailable.
    pub async fn transaction_locators(
        &self,
        chain_id: ChainId,
        hash: TransactionHash,
    ) -> Result<Vec<TransactionLocator>, HistoryStoreError> {
        let rows = sqlx::query(
            "SELECT locator.segment_id, locator.block_number, locator.transaction_index
             FROM raw_transaction_locators AS locator
             JOIN raw_segments AS segment ON segment.segment_id = locator.segment_id
             WHERE locator.chain_id = ? AND locator.transaction_hash = ? AND segment.state = 'closed'
             ORDER BY segment.verification_class DESC, segment.trust_model DESC, locator.segment_id",
        )
        .bind(u64_i64(chain_id.0, "chain ID")?)
        .bind(hash.as_array().as_slice())
        .fetch_all(&self.inner.pool)
        .await?;
        rows.iter()
            .map(|row| {
                Ok(TransactionLocator {
                    segment_id: SegmentId::new(row.try_get::<String, _>("segment_id")?)?,
                    block_number: BlockNumber(i64_u64(
                        row.try_get("block_number")?,
                        "transaction locator block number",
                    )?),
                    transaction_index: i64_u32(
                        row.try_get("transaction_index")?,
                        "transaction locator index",
                    )?,
                })
            })
            .collect()
    }

    /// Add an explicit durable owner. Duplicate ownership is idempotent.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid owners, unknown segments, or catalog I/O.
    pub async fn add_owner(
        &self,
        id: &SegmentId,
        kind: SegmentOwnerKind,
        owner_id: &str,
    ) -> Result<(), HistoryStoreError> {
        let _guard = self.inner.lifecycle.lock().await;
        validate_owner_id(owner_id)?;
        let record = self
            .segment(id)
            .await?
            .ok_or_else(|| HistoryStoreError::UnknownSegment(id.clone()))?;
        let claim = SegmentOwnerClaim {
            kind,
            owner_id: owner_id.to_owned(),
        };
        crate::job::validate_initial_owner_claims(
            self,
            &record.metadata.descriptor,
            &BTreeSet::from([claim]),
        )
        .await?;
        let now = unix_ms()?;
        let mut transaction = self.inner.pool.begin().await?;
        let result = sqlx::query(
            "INSERT OR IGNORE INTO raw_segment_owners(
                segment_id, owner_kind, owner_id, created_at_unix_ms
             )
             SELECT segment_id, ?, ?, ? FROM raw_segments
             WHERE segment_id = ? AND state = 'closed'",
        )
        .bind(kind.as_str())
        .bind(owner_id)
        .bind(u64_i64(now, "owner creation time")?)
        .bind(id.as_str())
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 0 {
            let exists: i64 = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM raw_segment_owners
                  WHERE segment_id = ? AND owner_kind = ? AND owner_id = ?)",
            )
            .bind(id.as_str())
            .bind(kind.as_str())
            .bind(owner_id)
            .fetch_one(&mut *transaction)
            .await?;
            if exists == 0 {
                return Err(HistoryStoreError::UnknownSegment(id.clone()));
            }
        }
        if kind == SegmentOwnerKind::RawHistoryJob {
            // The job progressed, so whatever held it up has cleared.
            sqlx::query(
                "UPDATE raw_history_jobs SET last_error = NULL
                 WHERE job_id = ? AND state IN ('queued', 'running', 'storage_backpressured')",
            )
            .bind(owner_id)
            .execute(&mut *transaction)
            .await?;
            crate::job::refresh_job_progress_tx(
                &mut transaction,
                &crate::RawHistoryJobId::new(owner_id.to_owned())?,
                now,
            )
            .await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    /// Remove one explicit owner. Missing ownership is idempotent.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid owner IDs or catalog I/O.
    pub async fn remove_owner(
        &self,
        id: &SegmentId,
        kind: SegmentOwnerKind,
        owner_id: &str,
    ) -> Result<(), HistoryStoreError> {
        let _guard = self.inner.lifecycle.lock().await;
        validate_owner_id(owner_id)?;
        if kind == SegmentOwnerKind::RawHistoryJob {
            return Err(HistoryStoreError::InvalidJob(
                "raw-history job ownership is released by deleting the terminal job".to_owned(),
            ));
        }
        sqlx::query(
            "DELETE FROM raw_segment_owners
             WHERE segment_id = ? AND owner_kind = ? AND owner_id = ?",
        )
        .bind(id.as_str())
        .bind(kind.as_str())
        .bind(owner_id)
        .execute(&self.inner.pool)
        .await?;
        Ok(())
    }

    /// List durable owners for one segment.
    ///
    /// # Errors
    ///
    /// Returns an error if owner metadata is malformed or unavailable.
    pub async fn owners(&self, id: &SegmentId) -> Result<Vec<SegmentOwner>, HistoryStoreError> {
        let rows = sqlx::query(
            "SELECT segment_id, owner_kind, owner_id, created_at_unix_ms
             FROM raw_segment_owners WHERE segment_id = ?
             ORDER BY owner_kind, owner_id",
        )
        .bind(id.as_str())
        .fetch_all(&self.inner.pool)
        .await?;
        rows.iter()
            .map(|row| {
                Ok(SegmentOwner {
                    segment_id: SegmentId::new(row.try_get::<String, _>("segment_id")?)?,
                    kind: SegmentOwnerKind::parse(row.try_get("owner_kind")?)?,
                    owner_id: row.try_get("owner_id")?,
                    created_at_unix_ms: i64_u64(
                        row.try_get("created_at_unix_ms")?,
                        "owner creation time",
                    )?,
                })
            })
            .collect()
    }

    /// Delete one unowned segment using a restart-safe `deleting` state.
    ///
    /// # Errors
    ///
    /// Owned segments are never removed implicitly.
    pub async fn delete_unowned(&self, id: &SegmentId) -> Result<(), HistoryStoreError> {
        let _guard = self.inner.lifecycle.lock().await;
        let mut transaction = self.inner.pool.begin().await?;
        let owner_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM raw_segment_owners WHERE segment_id = ?")
                .bind(id.as_str())
                .fetch_one(&mut *transaction)
                .await?;
        if owner_count > 0 {
            return Err(HistoryStoreError::OwnedSegment {
                id: id.clone(),
                owners: i64_u64(owner_count, "owner count")?,
            });
        }
        let relative_path: Option<String> = sqlx::query_scalar(
            "UPDATE raw_segments SET state = 'deleting'
             WHERE segment_id = ? AND state = 'closed'
             RETURNING relative_path",
        )
        .bind(id.as_str())
        .fetch_optional(&mut *transaction)
        .await?;
        let relative_path =
            relative_path.ok_or_else(|| HistoryStoreError::UnknownSegment(id.clone()))?;
        transaction.commit().await?;
        self.inner.forget_verified(id);
        let path = self.resolve_relative_path(&relative_path)?;
        let segments = self.inner.segments.clone();
        blocking(move || {
            remove_if_exists(&path)?;
            sync_directory(&segments)
        })
        .await??;
        sqlx::query("DELETE FROM raw_segments WHERE segment_id = ? AND state = 'deleting'")
            .bind(id.as_str())
            .execute(&self.inner.pool)
            .await?;
        Ok(())
    }

    /// Measure catalog, segment, temporary, and quarantine bytes from disk.
    ///
    /// # Errors
    ///
    /// Returns an error if catalog counters or filesystem metadata cannot be
    /// read exactly.
    pub async fn stats(&self) -> Result<HistoryStoreStats, HistoryStoreError> {
        let row = sqlx::query(
            "SELECT COUNT(*) AS segments,
                    COALESCE(SUM(logical_bytes), 0) AS logical_bytes,
                    COALESCE(SUM(physical_bytes), 0) AS physical_bytes
             FROM raw_segments",
        )
        .fetch_one(&self.inner.pool)
        .await?;
        let reservation = sqlx::query(
            "SELECT COALESCE(SUM(logical_bytes), 0) AS logical_bytes,
                    COALESCE(SUM(physical_bytes), 0) AS physical_bytes
             FROM raw_segment_reservations",
        )
        .fetch_one(&self.inner.pool)
        .await?;
        let owners: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM raw_segment_owners")
            .fetch_one(&self.inner.pool)
            .await?;
        let block_hash_locators: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM raw_block_hash_locators")
                .fetch_one(&self.inner.pool)
                .await?;
        let transaction_locators: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM raw_transaction_locators")
                .fetch_one(&self.inner.pool)
                .await?;
        let temporary_physical_bytes =
            reservation_temporary_bytes(&self.inner.pool, &self.inner.segments).await?;
        let (quarantine, catalog_path, root) = (
            self.inner.quarantine.clone(),
            self.inner.catalog_path.clone(),
            self.inner.root.clone(),
        );
        let (quarantine_physical_bytes, catalog_physical_bytes, total_physical_bytes) =
            blocking(move || {
                Ok::<_, HistoryStoreError>((
                    directory_bytes(&quarantine)?,
                    sqlite_family_bytes(&catalog_path)?,
                    directory_bytes(&root)?,
                ))
            })
            .await??;
        Ok(HistoryStoreStats {
            closed_segments: i64_u64(row.try_get("segments")?, "segment count")?,
            owners: i64_u64(owners, "owner count")?,
            block_hash_locators: i64_u64(block_hash_locators, "block hash locator count")?,
            transaction_locators: i64_u64(transaction_locators, "transaction locator count")?,
            retained_logical_bytes: i64_u64(
                row.try_get("logical_bytes")?,
                "retained logical bytes",
            )?,
            retained_segment_physical_bytes: i64_u64(
                row.try_get("physical_bytes")?,
                "retained segment physical bytes",
            )?,
            reserved_logical_bytes: i64_u64(
                reservation.try_get("logical_bytes")?,
                "reserved logical bytes",
            )?,
            reserved_physical_bytes: i64_u64(
                reservation.try_get("physical_bytes")?,
                "reserved physical bytes",
            )?,
            catalog_physical_bytes,
            temporary_physical_bytes,
            quarantine_physical_bytes,
            total_physical_bytes,
        })
    }

    fn validate_reservation(
        &self,
        reservation: SegmentReservation,
    ) -> Result<(), HistoryStoreError> {
        if reservation.maximum_logical_bytes == 0
            || reservation.maximum_physical_bytes == 0
            || reservation.maximum_logical_bytes > self.inner.budget.maximum_segment_logical_bytes
            || reservation.maximum_physical_bytes > self.inner.budget.maximum_segment_physical_bytes
        {
            return Err(HistoryStoreError::InvalidReservation);
        }
        Ok(())
    }

    fn resolve_relative_path(&self, relative: &str) -> Result<PathBuf, HistoryStoreError> {
        let path = Path::new(relative);
        if path.components().count() != 2
            || path.parent() != Some(Path::new("segments"))
            || path.file_name().is_none()
        {
            return Err(HistoryStoreError::CatalogIntegrity(format!(
                "invalid relative segment path `{relative}`"
            )));
        }
        Ok(self.inner.root.join(path))
    }
}

/// A reserved partial segment. Publication closes the file before committing
/// its catalog row; recovery quarantines the possible crash-window orphan.
#[derive(Debug)]
pub struct PendingSegment {
    store: HistoryStore,
    id: SegmentId,
    writer: Option<SegmentWriter>,
    partial_path: PathBuf,
    index_path: PathBuf,
    final_path: PathBuf,
    final_name: String,
    reservation: SegmentReservation,
    /// Whether this publication closed the file at `final_path`.
    closed: bool,
    profile: crate::RawHistoryProfile,
    indexes: crate::RawHistoryIndexPolicy,
    block_locators: Vec<(BlockHash, BlockNumber)>,
    transaction_locators: Vec<(TransactionHash, BlockNumber, u32)>,
}

impl PendingSegment {
    /// Append one validated finalized frame.
    ///
    /// # Errors
    ///
    /// Returns an error when the frame violates segment identity/order or a
    /// reserved hard limit.
    pub fn append(
        &mut self,
        frame: &leani_primitives::BlockFrame,
    ) -> Result<(), HistoryStoreError> {
        let transaction_locators = if self.indexes.transaction_hash {
            let transactions = frame.transactions.as_complete().ok_or_else(|| {
                HistoryStoreError::CatalogIntegrity(
                    "transaction locators require complete transaction material".to_owned(),
                )
            })?;
            Some(
                transactions
                    .iter()
                    .map(|transaction| (transaction.hash, frame.block.number, transaction.index))
                    .collect::<Vec<_>>(),
            )
        } else {
            None
        };
        self.writer
            .as_mut()
            .ok_or(HistoryStoreError::PendingConsumed)?
            .append(frame)?;
        if self.indexes.block_hash {
            self.block_locators
                .push((frame.block.hash, frame.block.number));
        }
        if let Some(transaction_locators) = transaction_locators {
            self.transaction_locators.extend(transaction_locators);
        }
        Ok(())
    }

    /// Fsync/rename the file, then atomically replace its reservation with a
    /// closed catalog record. When the catalog already holds a segment of the
    /// same material, as an overlapping job may have published first, the
    /// owners adopt that segment and this file is discarded.
    ///
    /// # Errors
    ///
    /// Returns an error for incomplete/corrupt output, locators the physical
    /// budget no longer admits, a segment that disagrees with the retained
    /// one of the same material, or a catalog failure. A failure removes this
    /// segment's files and releases its reservation. A crash between closing
    /// the file and the catalog commit is recovered on restart.
    pub async fn commit(
        mut self,
        owners: &[SegmentOwnerClaim],
    ) -> Result<SegmentRecord, HistoryStoreError> {
        match Box::pin(self.publish(owners)).await {
            Ok(Publication::Inserted(record)) => Ok(record),
            Ok(Publication::Adopted(record)) => {
                // The catalog commit stands. A file this fails to remove is
                // quarantined as an orphan when the store next opens.
                if let Err(error) = self.discard().await {
                    tracing::warn!(
                        segment = self.id.as_str(),
                        %error,
                        "could not remove the files of a publication that adopted a retained segment"
                    );
                }
                Ok(record)
            }
            Err(error) => {
                // The publication's own error is the one returned.
                if let Err(cleanup) = self.discard().await {
                    tracing::warn!(
                        segment = self.id.as_str(),
                        error = %cleanup,
                        "could not remove the files of a failed publication"
                    );
                }
                Err(error)
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn publish(
        &mut self,
        owners: &[SegmentOwnerClaim],
    ) -> Result<Publication, HistoryStoreError> {
        let owners = normalize_owner_claims(owners)?;
        let descriptor = self
            .writer
            .as_ref()
            .ok_or(HistoryStoreError::PendingConsumed)?
            .descriptor();
        crate::job::validate_initial_owner_claims(&self.store, descriptor, &owners).await?;
        let writer = self
            .writer
            .take()
            .ok_or(HistoryStoreError::PendingConsumed)?;
        let final_path = self.final_path.clone();
        let finished = blocking(move || writer.finish(final_path)).await;
        // A file already at the final path is another segment's.
        self.closed = !matches!(finished, Ok(Err(SegmentError::FinalPathExists(_))));
        let metadata = finished??;
        if metadata.logical_bytes > self.reservation.maximum_logical_bytes
            || metadata.physical_bytes > self.reservation.maximum_physical_bytes
        {
            return Err(HistoryStoreError::ReservationExceeded);
        }
        // A retained segment of the same material is verified before it is
        // adopted: one that fails is quarantined, and one whose file is gone
        // dropped, and this copy published instead. One that cannot be opened
        // now for another reason is adopted as before, and verified when next
        // read.
        let retained = {
            let mut connection = self.store.inner.pool.acquire().await?;
            same_material(&mut connection, &metadata, self.profile, self.indexes).await?
        };
        if let Some(retained) = retained {
            // A retained row under this segment's own ID names the file just
            // closed at its path, so its own file was already gone: the
            // close would otherwise have found the path taken.
            if retained.metadata.id == self.id {
                self.store.drop_missing_segment(&retained).await?;
            } else {
                match self.store.open_record(&retained).await {
                    // Its file is gone, so this copy replaces it.
                    Err(HistoryStoreError::Segment(error)) if error.is_missing_file() => {
                        self.store.drop_missing_segment(&retained).await?;
                    }
                    _ => {}
                }
            }
        }
        let _guard = self.store.inner.lifecycle.lock().await;
        let created_at_unix_ms = unix_ms()?;
        let mut transaction = self.store.inner.pool.begin().await?;
        let reservation_exists: i64 = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM raw_segment_reservations WHERE segment_id = ?)",
        )
        .bind(self.id.as_str())
        .fetch_one(&mut *transaction)
        .await?;
        if reservation_exists == 0 {
            return Err(HistoryStoreError::MissingReservation(self.id.clone()));
        }
        let publication = if let Some(retained) =
            same_material(&mut transaction, &metadata, self.profile, self.indexes).await?
        {
            if retained.metadata.first_parent_hash != metadata.first_parent_hash
                || retained.metadata.last_hash != metadata.last_hash
                || retained.metadata.ordered_hash_digest != metadata.ordered_hash_digest
            {
                return Err(HistoryStoreError::CatalogIntegrity(format!(
                    "segment `{}` disagrees with retained segment `{}` over the same finalized blocks",
                    self.id.as_str(),
                    retained.metadata.id.as_str()
                )));
            }
            insert_owners(
                &mut transaction,
                &retained.metadata.id,
                &owners,
                created_at_unix_ms,
            )
            .await?;
            Publication::Adopted(retained)
        } else {
            // Locators beyond the estimate admitted with the segment are
            // published anyway: refusing them would only make the job acquire
            // the segment again. The excess is at most this segment's own
            // locator rows, and the next admission counts it.
            let relative_path = format!("segments/{}", self.final_name);
            insert_segment(
                &mut transaction,
                &metadata,
                self.profile,
                self.indexes,
                &relative_path,
                created_at_unix_ms,
            )
            .await?;
            for (hash, block_number) in &self.block_locators {
                sqlx::query(
                    "INSERT INTO raw_block_hash_locators(
                        chain_id, block_hash, segment_id, block_number
                     ) VALUES (?, ?, ?, ?)",
                )
                .bind(u64_i64(metadata.descriptor.chain_id.0, "chain ID")?)
                .bind(hash.as_array().as_slice())
                .bind(self.id.as_str())
                .bind(u64_i64(block_number.0, "block locator number")?)
                .execute(&mut *transaction)
                .await?;
            }
            for (hash, block_number, transaction_index) in &self.transaction_locators {
                sqlx::query(
                    "INSERT INTO raw_transaction_locators(
                        chain_id, transaction_hash, segment_id, block_number, transaction_index
                     ) VALUES (?, ?, ?, ?, ?)",
                )
                .bind(u64_i64(metadata.descriptor.chain_id.0, "chain ID")?)
                .bind(hash.as_array().as_slice())
                .bind(self.id.as_str())
                .bind(u64_i64(block_number.0, "transaction locator block number")?)
                .bind(i64::from(*transaction_index))
                .execute(&mut *transaction)
                .await?;
            }
            insert_owners(&mut transaction, &self.id, &owners, created_at_unix_ms).await?;
            Publication::Inserted(SegmentRecord {
                metadata,
                profile: self.profile,
                indexes: self.indexes,
                relative_path,
                created_at_unix_ms,
            })
        };
        sqlx::query("DELETE FROM raw_segment_reservations WHERE segment_id = ?")
            .bind(self.id.as_str())
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        if let Publication::Inserted(record) = &publication {
            // The writer hashed every byte it wrote.
            self.store
                .inner
                .remember_verified((record.metadata.id.clone(), record.metadata.content_checksum));
        }
        Ok(publication)
    }

    /// Remove partial files and release capacity without publishing coverage.
    ///
    /// # Errors
    ///
    /// Returns an error if temporary files or the reservation cannot be
    /// removed.
    pub async fn abort(mut self) -> Result<(), HistoryStoreError> {
        self.discard().await
    }

    /// Remove this segment's partial files, and its closed file when this
    /// publication closed it, then release its reservation.
    async fn discard(&mut self) -> Result<(), HistoryStoreError> {
        self.writer.take();
        let mut paths = vec![self.partial_path.clone(), self.index_path.clone()];
        if self.closed {
            paths.push(self.final_path.clone());
        }
        let segments = self.store.inner.segments.clone();
        blocking(move || {
            for path in &paths {
                remove_if_exists(path)?;
            }
            sync_directory(&segments)
        })
        .await??;
        self.clear_reservation().await
    }

    async fn clear_reservation(&self) -> Result<(), HistoryStoreError> {
        sqlx::query("DELETE FROM raw_segment_reservations WHERE segment_id = ?")
            .bind(self.id.as_str())
            .execute(&self.store.inner.pool)
            .await?;
        Ok(())
    }
}

#[allow(clippy::too_many_lines)]
async fn recover(
    pool: &SqlitePool,
    segments: &Path,
    quarantine: &Path,
    quarantine_maximum_bytes: u64,
) -> Result<RecoveryReport, HistoryStoreError> {
    let mut report = RecoveryReport::default();
    let reservation_rows =
        sqlx::query("SELECT partial_name, index_name FROM raw_segment_reservations")
            .fetch_all(pool)
            .await?;
    report.cleared_reservations = usize_u64(reservation_rows.len())?;
    for row in reservation_rows {
        for column in ["partial_name", "index_name"] {
            let name: String = row.try_get(column)?;
            let path = safe_child(segments, &name)?;
            if remove_if_exists(&path)? {
                report.removed_partial_files += 1;
            }
        }
    }
    sqlx::query("DELETE FROM raw_segment_reservations")
        .execute(pool)
        .await?;

    for entry in fs::read_dir(segments)? {
        let entry = entry?;
        if entry.file_type()?.is_file()
            && matches!(
                entry.path().extension().and_then(|value| value.to_str()),
                Some("partial" | "idxpartial")
            )
            && remove_if_exists(&entry.path())?
        {
            report.removed_partial_files += 1;
        }
    }

    let deleting =
        sqlx::query("SELECT segment_id, relative_path FROM raw_segments WHERE state = 'deleting'")
            .fetch_all(pool)
            .await?;
    for row in deleting {
        let id: String = row.try_get("segment_id")?;
        let relative: String = row.try_get("relative_path")?;
        let name = Path::new(&relative)
            .file_name()
            .and_then(|value| value.to_str())
            .ok_or_else(|| {
                HistoryStoreError::CatalogIntegrity(format!("invalid deleting path `{relative}`"))
            })?;
        remove_if_exists(&safe_child(segments, name)?)?;
        sqlx::query("DELETE FROM raw_segments WHERE segment_id = ?")
            .bind(id)
            .execute(pool)
            .await?;
        report.completed_deletions += 1;
    }

    // Only each file's presence and closed length are checked here; its
    // whole-file checksum is verified when it is first read.
    let rows = sqlx::query("SELECT * FROM raw_segments WHERE state = 'closed'")
        .fetch_all(pool)
        .await?;
    let mut catalogued = BTreeSet::new();
    let mut missing = Vec::new();
    for row in &rows {
        let record = segment_from_row(row)?;
        let name = Path::new(&record.relative_path)
            .file_name()
            .and_then(|value| value.to_str())
            .ok_or_else(|| {
                HistoryStoreError::CatalogIntegrity(format!(
                    "invalid segment path `{}`",
                    record.relative_path
                ))
            })?;
        let path = safe_child(segments, name)?;
        match fs::metadata(&path) {
            Ok(metadata)
                if metadata.is_file() && metadata.len() == record.metadata.physical_bytes =>
            {
                catalogued.insert(name.to_owned());
            }
            Ok(_) => {
                if drop_segment(
                    pool,
                    &record.metadata,
                    "failed validation and was quarantined: it no longer has its closed length",
                )
                .await?
                {
                    quarantine_file(&path, quarantine, false)?;
                    report.quarantined_corrupt_files += 1;
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                missing.push(record.metadata);
            }
            Err(error) => return Err(error.into()),
        }
    }
    let missing_count = usize_u64(missing.len())?;
    if missing_count.saturating_mul(100)
        > usize_u64(rows.len())?.saturating_mul(UNAVAILABLE_MISSING_SEGMENTS_PERCENT)
    {
        report.unavailable_segments = missing_count;
        tracing::warn!(
            missing = missing_count,
            catalogued = rows.len(),
            directory = %segments.display(),
            "most retained segment files are missing, so the segment directory is taken as unavailable and their catalog rows are kept; reads of them fail over until the files return"
        );
    } else {
        for metadata in &missing {
            if drop_segment(pool, metadata, "is missing from disk").await? {
                report.missing_segments += 1;
            }
        }
    }

    // A closed file without a catalog row was never published, whatever its
    // content, so it is quarantined unread.
    for entry in fs::read_dir(segments)? {
        let entry = entry?;
        if !entry.file_type()?.is_file()
            || entry.path().extension().and_then(|value| value.to_str()) != Some("idxraw")
        {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if catalogued.contains(&name) {
            continue;
        }
        quarantine_file(&entry.path(), quarantine, true)?;
        report.quarantined_closed_files += 1;
    }
    sync_directory(segments)?;
    sync_directory(quarantine)?;
    report.collected_quarantine_files = collect_quarantine(quarantine, quarantine_maximum_bytes)?;
    Ok(report)
}

/// Run blocking file work off the async runtime.
pub(crate) async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> T + Send + 'static,
) -> Result<T, HistoryStoreError> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|error| HistoryStoreError::Task(error.to_string()))
}

/// Catalog bytes a segment with `locators` locator rows may add.
fn catalog_growth(locators: u64) -> Result<u64, HistoryStoreError> {
    locators
        .checked_mul(CATALOG_LOCATOR_BYTES)
        .and_then(|bytes| bytes.checked_add(CATALOG_RESERVATION_OVERHEAD_BYTES))
        .ok_or(HistoryStoreError::ArithmeticOverflow)
}

/// Drop a catalogued segment whose file failed validation or is gone, with
/// its owners and locators, so its coverage reads as missing, and record why
/// on the raw-history jobs that owned it. A complete job goes back to the
/// queue to acquire the blocks again; a failed or cancelled one keeps the
/// reason it ended. Returns `false` when the catalog no longer holds this
/// exact segment.
async fn drop_segment(
    pool: &SqlitePool,
    metadata: &SegmentMetadata,
    reason: &str,
) -> Result<bool, HistoryStoreError> {
    let now = unix_ms()?;
    let mut transaction = pool.begin().await?;
    let owners: Vec<String> = sqlx::query_scalar(
        "SELECT owner_id FROM raw_segment_owners
         WHERE segment_id = ? AND owner_kind = 'raw_history_job'
         ORDER BY owner_id",
    )
    .bind(metadata.id.as_str())
    .fetch_all(&mut *transaction)
    .await?;
    let dropped = sqlx::query(
        "DELETE FROM raw_segments
         WHERE segment_id = ? AND content_checksum = ? AND state = 'closed'",
    )
    .bind(metadata.id.as_str())
    .bind(metadata.content_checksum.as_slice())
    .execute(&mut *transaction)
    .await?
    .rows_affected();
    if dropped == 0 {
        return Ok(false);
    }
    let error = format!(
        "retained segment `{}` for blocks {} to {} {reason}; those blocks are no longer retained",
        metadata.id.as_str(),
        metadata.descriptor.range.start().0,
        metadata.descriptor.range.end().0
    );
    for owner in owners {
        sqlx::query(
            "UPDATE raw_history_jobs
             SET last_error = ?,
                 state = CASE state WHEN 'complete' THEN 'queued' ELSE state END
             WHERE job_id = ? AND state NOT IN ('failed', 'cancelled')",
        )
        .bind(&error)
        .bind(&owner)
        .execute(&mut *transaction)
        .await?;
        crate::job::refresh_job_progress_tx(
            &mut transaction,
            &crate::RawHistoryJobId::new(owner)?,
            now,
        )
        .await?;
    }
    transaction.commit().await?;
    Ok(true)
}

/// Delete the oldest quarantined files until the quarantine holds at most
/// `maximum_bytes`. Returns how many were deleted.
fn collect_quarantine(quarantine: &Path, maximum_bytes: u64) -> Result<u64, HistoryStoreError> {
    let mut files = Vec::new();
    let mut total = 0_u64;
    for entry in fs::read_dir(quarantine)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if !metadata.is_file() {
            continue;
        }
        total = total
            .checked_add(metadata.len())
            .ok_or(HistoryStoreError::ArithmeticOverflow)?;
        files.push((
            metadata.modified().unwrap_or(UNIX_EPOCH),
            entry.path(),
            metadata.len(),
        ));
    }
    files.sort();
    // The newest file is kept whatever its size, so the latest quarantine can
    // always be inspected.
    let newest = files.len().saturating_sub(1);
    let mut collected = 0_u64;
    for (index, (_, path, bytes)) in files.into_iter().enumerate() {
        if total <= maximum_bytes || index == newest {
            break;
        }
        if remove_if_exists(&path)? {
            collected += 1;
        }
        total = total.saturating_sub(bytes);
    }
    if collected > 0 {
        sync_directory(quarantine)?;
    }
    Ok(collected)
}

/// How a commit published its segment.
#[derive(Debug)]
enum Publication {
    Inserted(SegmentRecord),
    /// The catalog held a segment of the same material, which gained this
    /// commit's owners.
    Adopted(SegmentRecord),
}

/// Add `owners` to `segment`. The segment advances each raw-history job that
/// owns it, so its progress is refreshed and its last error cleared.
async fn insert_owners(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    segment: &SegmentId,
    owners: &BTreeSet<SegmentOwnerClaim>,
    created_at_unix_ms: u64,
) -> Result<(), HistoryStoreError> {
    for owner in owners {
        sqlx::query(
            "INSERT OR IGNORE INTO raw_segment_owners(
                segment_id, owner_kind, owner_id, created_at_unix_ms
             ) VALUES (?, ?, ?, ?)",
        )
        .bind(segment.as_str())
        .bind(owner.kind.as_str())
        .bind(&owner.owner_id)
        .bind(u64_i64(created_at_unix_ms, "owner creation time")?)
        .execute(&mut **transaction)
        .await?;
        if owner.kind == SegmentOwnerKind::RawHistoryJob {
            // The job progressed, so whatever held it up has cleared.
            sqlx::query(
                "UPDATE raw_history_jobs SET last_error = NULL
                 WHERE job_id = ? AND state IN ('queued', 'running', 'storage_backpressured')",
            )
            .bind(&owner.owner_id)
            .execute(&mut **transaction)
            .await?;
            crate::job::refresh_job_progress_tx(
                transaction,
                &crate::RawHistoryJobId::new(owner.owner_id.clone())?,
                created_at_unix_ms,
            )
            .await?;
        }
    }
    Ok(())
}

/// The catalogued segment with `metadata`'s material identity, which the
/// catalog holds at most once.
async fn same_material(
    connection: &mut sqlx::SqliteConnection,
    metadata: &SegmentMetadata,
    profile: crate::RawHistoryProfile,
    indexes: crate::RawHistoryIndexPolicy,
) -> Result<Option<SegmentRecord>, HistoryStoreError> {
    let (history_profile, merge_block) = profile_columns(profile)?;
    let row = sqlx::query(
        "SELECT * FROM raw_segments
         WHERE chain_id = ? AND start_block = ? AND end_block = ? AND material_shape = ?
           AND present_capabilities = ? AND complete_capabilities = ?
           AND verification_class = ? AND trust_model = ? AND history_profile = ?
           AND COALESCE(merge_block, -1) = ?
           AND block_hash_indexed = ? AND transaction_hash_indexed = ?",
    )
    .bind(u64_i64(metadata.descriptor.chain_id.0, "chain ID")?)
    .bind(u64_i64(metadata.descriptor.range.start().0, "start block")?)
    .bind(u64_i64(metadata.descriptor.range.end().0, "end block")?)
    .bind(metadata.descriptor.material_shape.0.as_slice())
    .bind(i64::from(metadata.descriptor.present_capabilities.bits()))
    .bind(i64::from(metadata.descriptor.complete_capabilities.bits()))
    .bind(i64::from(metadata.descriptor.verification as u8))
    .bind(i64::from(metadata.descriptor.trust as u8))
    .bind(history_profile)
    .bind(merge_block.unwrap_or(-1))
    .bind(i64::from(indexes.block_hash))
    .bind(i64::from(indexes.transaction_hash))
    .fetch_optional(&mut *connection)
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    if row.try_get::<String, _>("state")? != "closed" {
        return Err(HistoryStoreError::CatalogIntegrity(format!(
            "segment `{}` of the same material is being deleted",
            row.try_get::<String, _>("segment_id")?
        )));
    }
    segment_from_row(&row).map(Some)
}

fn profile_columns(
    profile: crate::RawHistoryProfile,
) -> Result<(i64, Option<i64>), HistoryStoreError> {
    Ok(match profile {
        crate::RawHistoryProfile::ProcessorReuse => (0, None),
        crate::RawHistoryProfile::PostMergeExecutionRpc { merge_block } => {
            (1, Some(u64_i64(merge_block.0, "Merge block")?))
        }
    })
}

async fn insert_segment(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    metadata: &SegmentMetadata,
    profile: crate::RawHistoryProfile,
    indexes: crate::RawHistoryIndexPolicy,
    relative_path: &str,
    created_at_unix_ms: u64,
) -> Result<(), HistoryStoreError> {
    let (history_profile, merge_block) = profile_columns(profile)?;
    sqlx::query(
        "INSERT INTO raw_segments(
            segment_id, chain_id, start_block, end_block, material_shape,
            present_capabilities, complete_capabilities, verification_class, trust_model,
            format_version, frame_encoding_version, compression,
            history_profile, merge_block,
            block_hash_indexed, transaction_hash_indexed, relative_path,
            logical_bytes, physical_bytes, first_parent_hash, last_hash,
            ordered_hash_digest, records_checksum, content_checksum, state,
            created_at_unix_ms
         ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?,
            ?, ?,
            ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?,
            'closed', ?
         )",
    )
    .bind(metadata.id.as_str())
    .bind(u64_i64(metadata.descriptor.chain_id.0, "chain ID")?)
    .bind(u64_i64(metadata.descriptor.range.start().0, "start block")?)
    .bind(u64_i64(metadata.descriptor.range.end().0, "end block")?)
    .bind(metadata.descriptor.material_shape.0.as_slice())
    .bind(i64::from(metadata.descriptor.present_capabilities.bits()))
    .bind(i64::from(metadata.descriptor.complete_capabilities.bits()))
    .bind(i64::from(metadata.descriptor.verification as u8))
    .bind(i64::from(metadata.descriptor.trust as u8))
    .bind(i64::from(FORMAT_VERSION))
    .bind(i64::from(FRAME_ENCODING_VERSION))
    .bind(i64::from(metadata.compression as u8))
    .bind(history_profile)
    .bind(merge_block)
    .bind(i64::from(indexes.block_hash))
    .bind(i64::from(indexes.transaction_hash))
    .bind(relative_path)
    .bind(u64_i64(metadata.logical_bytes, "logical bytes")?)
    .bind(u64_i64(metadata.physical_bytes, "physical bytes")?)
    .bind(metadata.first_parent_hash.as_array().as_slice())
    .bind(metadata.last_hash.as_array().as_slice())
    .bind(metadata.ordered_hash_digest.as_slice())
    .bind(metadata.records_checksum.as_slice())
    .bind(metadata.content_checksum.as_slice())
    .bind(u64_i64(created_at_unix_ms, "segment creation time")?)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

fn segment_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<SegmentRecord, HistoryStoreError> {
    let format_version = i64_u16(row.try_get("format_version")?, "segment format version")?;
    if format_version != FORMAT_VERSION {
        return Err(HistoryStoreError::CatalogIntegrity(format!(
            "unsupported segment format version {format_version}"
        )));
    }
    let frame_encoding_version = i64_u16(
        row.try_get("frame_encoding_version")?,
        "frame encoding version",
    )?;
    if frame_encoding_version != FRAME_ENCODING_VERSION {
        return Err(HistoryStoreError::CatalogIntegrity(format!(
            "unsupported frame encoding version {frame_encoding_version}"
        )));
    }
    let id = SegmentId::new(row.try_get::<String, _>("segment_id")?)?;
    let chain_id = ChainId(i64_u64(row.try_get("chain_id")?, "chain ID")?);
    let range = BlockRange::new(
        BlockNumber(i64_u64(row.try_get("start_block")?, "start block")?),
        BlockNumber(i64_u64(row.try_get("end_block")?, "end block")?),
    )
    .map_err(|error| HistoryStoreError::CatalogIntegrity(error.to_string()))?;
    let material_shape = blob32(row.try_get("material_shape")?, "material shape")?;
    let present_bits = i64_u16(row.try_get("present_capabilities")?, "present capabilities")?;
    let complete_bits = i64_u16(
        row.try_get("complete_capabilities")?,
        "complete capabilities",
    )?;
    let present_capabilities = CapabilitySet::from_bits(present_bits).ok_or_else(|| {
        HistoryStoreError::CatalogIntegrity("unknown present capability bits".to_owned())
    })?;
    let complete_capabilities = CapabilitySet::from_bits(complete_bits).ok_or_else(|| {
        HistoryStoreError::CatalogIntegrity("unknown complete capability bits".to_owned())
    })?;
    let verification = VerificationClass::from_byte(i64_u8(
        row.try_get("verification_class")?,
        "verification class",
    )?)?;
    let trust = decode_trust(i64_u8(row.try_get("trust_model")?, "trust model")?)?;
    let compression = Compression::from_byte(i64_u8(row.try_get("compression")?, "compression")?)?;
    let profile = match row.try_get::<i64, _>("history_profile")? {
        0 => crate::RawHistoryProfile::ProcessorReuse,
        1 => crate::RawHistoryProfile::PostMergeExecutionRpc {
            merge_block: BlockNumber(i64_u64(
                row.try_get::<Option<i64>, _>("merge_block")?
                    .ok_or_else(|| {
                        HistoryStoreError::CatalogIntegrity(
                            "post-Merge execution-RPC segment lacks Merge block".to_owned(),
                        )
                    })?,
                "Merge block",
            )?),
        },
        other => {
            return Err(HistoryStoreError::CatalogIntegrity(format!(
                "unknown raw-history profile {other}"
            )));
        }
    };
    Ok(SegmentRecord {
        metadata: SegmentMetadata {
            id,
            descriptor: SegmentDescriptor {
                chain_id,
                range,
                material_shape: MaterialShapeId(material_shape),
                present_capabilities,
                complete_capabilities,
                verification,
                trust,
            },
            compression,
            logical_bytes: i64_u64(row.try_get("logical_bytes")?, "logical bytes")?,
            physical_bytes: i64_u64(row.try_get("physical_bytes")?, "physical bytes")?,
            first_parent_hash: BlockHash::new(blob32(
                row.try_get("first_parent_hash")?,
                "first parent hash",
            )?),
            last_hash: BlockHash::new(blob32(row.try_get("last_hash")?, "last hash")?),
            ordered_hash_digest: blob32(
                row.try_get("ordered_hash_digest")?,
                "ordered hash digest",
            )?,
            records_checksum: blob32(row.try_get("records_checksum")?, "records checksum")?,
            content_checksum: blob32(row.try_get("content_checksum")?, "content checksum")?,
        },
        profile,
        indexes: crate::RawHistoryIndexPolicy {
            block_hash: row.try_get::<i64, _>("block_hash_indexed")? != 0,
            transaction_hash: row.try_get::<i64, _>("transaction_hash_indexed")? != 0,
            logs: false,
        },
        relative_path: row.try_get("relative_path")?,
        created_at_unix_ms: i64_u64(row.try_get("created_at_unix_ms")?, "segment creation time")?,
    })
}

async fn catalog_capacity(pool: &SqlitePool) -> Result<(u64, u64, u64), HistoryStoreError> {
    let row = sqlx::query(
        "SELECT
            COALESCE((SELECT SUM(logical_bytes) FROM raw_segments), 0) AS retained_logical,
            COALESCE((SELECT SUM(logical_bytes) FROM raw_segment_reservations), 0) AS reserved_logical,
            COALESCE((SELECT SUM(physical_bytes) FROM raw_segment_reservations), 0) AS reserved_physical",
    )
    .fetch_one(pool)
    .await?;
    Ok((
        i64_u64(row.try_get("retained_logical")?, "retained logical bytes")?,
        i64_u64(row.try_get("reserved_logical")?, "reserved logical bytes")?,
        i64_u64(row.try_get("reserved_physical")?, "reserved physical bytes")?,
    ))
}

async fn reservation_temporary_bytes(
    pool: &SqlitePool,
    segments: &Path,
) -> Result<u64, HistoryStoreError> {
    let rows = sqlx::query("SELECT partial_name, index_name FROM raw_segment_reservations")
        .fetch_all(pool)
        .await?;
    let mut paths = Vec::with_capacity(rows.len().saturating_mul(2));
    for row in &rows {
        for column in ["partial_name", "index_name"] {
            paths.push(safe_child(segments, &row.try_get::<String, _>(column)?)?);
        }
    }
    blocking(move || {
        paths.iter().try_fold(0_u64, |total, path| {
            total
                .checked_add(file_bytes(path)?)
                .ok_or(HistoryStoreError::ArithmeticOverflow)
        })
    })
    .await?
}

fn validate_owner_id(value: &str) -> Result<(), HistoryStoreError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        return Err(HistoryStoreError::InvalidOwnerId(value.to_owned()));
    }
    Ok(())
}

fn normalize_owner_claims(
    owners: &[SegmentOwnerClaim],
) -> Result<BTreeSet<SegmentOwnerClaim>, HistoryStoreError> {
    owners
        .iter()
        .map(|owner| {
            validate_owner_id(&owner.owner_id)?;
            Ok(owner.clone())
        })
        .collect()
}

fn safe_child(parent: &Path, name: &str) -> Result<PathBuf, HistoryStoreError> {
    let path = Path::new(name);
    if path.components().count() != 1 || path.file_name().is_none() {
        return Err(HistoryStoreError::CatalogIntegrity(format!(
            "unsafe catalog file name `{name}`"
        )));
    }
    Ok(parent.join(path))
}

fn quarantine_file(path: &Path, quarantine: &Path, orphan: bool) -> Result<(), HistoryStoreError> {
    let name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| HistoryStoreError::CatalogIntegrity("non-UTF8 segment name".to_owned()))?;
    let status = if orphan { "orphan" } else { "corrupt" };
    let mut target = quarantine.join(format!("{name}.{status}"));
    let mut suffix = 0_u64;
    while target.exists() {
        suffix = suffix
            .checked_add(1)
            .ok_or(HistoryStoreError::ArithmeticOverflow)?;
        target = quarantine.join(format!("{name}.{status}.{suffix}"));
    }
    fs::rename(path, &target)?;
    // Collection deletes the longest-quarantined files first.
    if fs::symlink_metadata(&target)?.is_file() {
        fs::OpenOptions::new()
            .write(true)
            .open(&target)?
            .set_modified(SystemTime::now())?;
    }
    Ok(())
}

fn remove_if_exists(path: &Path) -> Result<bool, HistoryStoreError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn directory_bytes(path: &Path) -> Result<u64, HistoryStoreError> {
    let mut total = 0_u64;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        // An entry removed since the listing, such as another segment's
        // partial file, holds nothing.
        let metadata = match entry.metadata() {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        let bytes = if metadata.is_dir() {
            match directory_bytes(&entry.path()) {
                Err(HistoryStoreError::Io(error))
                    if error.kind() == std::io::ErrorKind::NotFound =>
                {
                    0
                }
                bytes => bytes?,
            }
        } else if metadata.is_file() {
            metadata.len()
        } else {
            0
        };
        total = total
            .checked_add(bytes)
            .ok_or(HistoryStoreError::ArithmeticOverflow)?;
    }
    Ok(total)
}

fn sqlite_family_bytes(path: &Path) -> Result<u64, HistoryStoreError> {
    let mut total = file_bytes(path)?;
    for suffix in ["-wal", "-shm"] {
        let sibling = PathBuf::from(format!("{}{suffix}", path.to_string_lossy()));
        total = total
            .checked_add(file_bytes(&sibling)?)
            .ok_or(HistoryStoreError::ArithmeticOverflow)?;
    }
    Ok(total)
}

fn file_bytes(path: &Path) -> Result<u64, HistoryStoreError> {
    match fs::metadata(path) {
        Ok(metadata) => Ok(metadata.len()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(error.into()),
    }
}

fn sync_directory(path: &Path) -> Result<(), HistoryStoreError> {
    fs::File::open(path)?.sync_all()?;
    Ok(())
}

pub(crate) fn unix_ms() -> Result<u64, HistoryStoreError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| HistoryStoreError::ClockBeforeEpoch)?
        .as_millis()
        .try_into()
        .map_err(|_| HistoryStoreError::ArithmeticOverflow)
}

pub(crate) fn u64_i64(value: u64, name: &'static str) -> Result<i64, HistoryStoreError> {
    i64::try_from(value).map_err(|_| HistoryStoreError::NumericRange(name))
}

pub(crate) fn i64_u64(value: i64, name: &'static str) -> Result<u64, HistoryStoreError> {
    u64::try_from(value).map_err(|_| HistoryStoreError::NumericRange(name))
}

fn i64_u16(value: i64, name: &'static str) -> Result<u16, HistoryStoreError> {
    u16::try_from(value).map_err(|_| HistoryStoreError::NumericRange(name))
}

fn i64_u8(value: i64, name: &'static str) -> Result<u8, HistoryStoreError> {
    u8::try_from(value).map_err(|_| HistoryStoreError::NumericRange(name))
}

fn i64_u32(value: i64, name: &'static str) -> Result<u32, HistoryStoreError> {
    u32::try_from(value).map_err(|_| HistoryStoreError::NumericRange(name))
}

fn decode_trust(value: u8) -> Result<TrustModel, HistoryStoreError> {
    match value {
        0 => Ok(TrustModel::Untrusted),
        1 => Ok(TrustModel::TrustedManifest),
        2 => Ok(TrustModel::TrustedDataset),
        3 => Ok(TrustModel::ProtocolVerified),
        other => Err(HistoryStoreError::CatalogIntegrity(format!(
            "unknown trust model {other}"
        ))),
    }
}

fn usize_u64(value: usize) -> Result<u64, HistoryStoreError> {
    u64::try_from(value).map_err(|_| HistoryStoreError::ArithmeticOverflow)
}

pub(crate) fn blob32(value: Vec<u8>, name: &'static str) -> Result<[u8; 32], HistoryStoreError> {
    value
        .try_into()
        .map_err(|_| HistoryStoreError::CatalogIntegrity(format!("{name} must be 32 bytes")))
}

#[derive(Debug, Error)]
pub enum HistoryStoreError {
    #[error("invalid history-store configuration: {0}")]
    InvalidConfig(String),
    #[error("invalid segment reservation")]
    InvalidReservation,
    #[error("invalid owner ID `{0}`")]
    InvalidOwnerId(String),
    #[error("invalid raw-history job: {0}")]
    InvalidJob(String),
    #[error("raw-history job identity conflicts with existing job `{0}`")]
    JobConflict(String),
    #[error("unknown raw-history job `{0}`")]
    UnknownJob(String),
    #[error("raw-history job `{id}` cannot transition from {state}")]
    JobState { id: String, state: String },
    #[error("raw-history logical budget {limit} bytes would be exceeded ({observed} bytes)")]
    LogicalBudget { limit: u64, observed: u64 },
    #[error("raw-history physical budget {limit} bytes would be exceeded ({observed} bytes)")]
    PhysicalBudget { limit: u64, observed: u64 },
    #[error("segment path already exists for `{0:?}`")]
    PathCollision(SegmentId),
    #[error("unknown or unreadable segment `{0:?}`")]
    UnknownSegment(SegmentId),
    #[error("segment `{id:?}` is unusable, and its coverage was dropped: {reason}")]
    Quarantined { id: SegmentId, reason: String },
    #[error("missing reservation for closed segment `{0:?}`")]
    MissingReservation(SegmentId),
    #[error("segment exceeded its durable reservation")]
    ReservationExceeded,
    #[error("pending segment has already been committed or aborted")]
    PendingConsumed,
    #[error("segment `{id:?}` still has {owners} durable owners")]
    OwnedSegment { id: SegmentId, owners: u64 },
    #[error("raw-history catalog integrity failure: {0}")]
    CatalogIntegrity(String),
    #[error("numeric value `{0}` cannot be represented by SQLite")]
    NumericRange(&'static str),
    #[error("system clock precedes the Unix epoch")]
    ClockBeforeEpoch,
    #[error("raw-history size arithmetic overflow")]
    ArithmeticOverflow,
    #[error(transparent)]
    Segment(#[from] SegmentError),
    #[error("history-store I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("history-store blocking task failed: {0}")]
    Task(String),
    #[error("history catalog failed: {0}")]
    Sql(#[from] sqlx::Error),
}

#[cfg(test)]
mod tests {
    use std::{
        fs::{self, OpenOptions},
        io::{Seek, SeekFrom, Write},
    };

    use leani_primitives::{
        BlockFrame, BlockHash, BlockRange, Material, MissingReason, TransactionEnvelope,
        TransactionHash,
    };
    use leani_testkit::fixture_frame;
    use tempfile::tempdir;

    use super::*;

    fn frames(start: u64, end: u64) -> Vec<BlockFrame> {
        let mut parent = BlockHash::new([0x22; 32]);
        (start..=end)
            .map(|number| {
                let frame = fixture_frame(number, parent);
                parent = frame.block.hash;
                frame
            })
            .collect()
    }

    fn descriptor(frames: &[BlockFrame]) -> SegmentDescriptor {
        let capabilities = frames[0].capabilities();
        SegmentDescriptor {
            chain_id: frames[0].chain_id,
            range: BlockRange::new(
                frames[0].block.number,
                frames.last().expect("non-empty fixture").block.number,
            )
            .expect("ordered fixture"),
            material_shape: MaterialShapeId([0x55; 32]),
            present_capabilities: capabilities.present,
            complete_capabilities: capabilities.complete,
            verification: VerificationClass::TrustedDataset,
            trust: TrustModel::TrustedDataset,
        }
    }

    fn test_config(root: &Path) -> HistoryStoreConfig {
        HistoryStoreConfig::new(root).with_budget(StorageBudget {
            maximum_logical_bytes: 32 * 1024 * 1024,
            maximum_physical_bytes: 32 * 1024 * 1024,
            maximum_frame_logical_bytes: 1024 * 1024,
            maximum_segment_logical_bytes: 4 * 1024 * 1024,
            maximum_segment_physical_bytes: 4 * 1024 * 1024,
        })
    }

    async fn publish(store: &HistoryStore, id: &str, frames: &[BlockFrame]) -> SegmentRecord {
        let mut pending = store
            .begin_segment(
                SegmentId::new(id).expect("segment ID"),
                descriptor(frames),
                Compression::Snappy,
                SegmentReservation::new(1024 * 1024, 1024 * 1024),
            )
            .await
            .expect("begin segment");
        for frame in frames {
            pending.append(frame).expect("append frame");
        }
        pending.commit(&[]).await.expect("commit segment")
    }

    async fn close(store: &HistoryStore) {
        store.inner.pool.close().await;
    }

    #[tokio::test]
    async fn closed_segments_survive_restart_and_seek_one_record() {
        let directory = tempdir().expect("temporary directory");
        let expected = frames(100, 109);
        let store = HistoryStore::open(test_config(directory.path()))
            .await
            .expect("open store");
        let record = Box::pin(publish(&store, "restart", &expected)).await;
        let stats = store.stats().await.expect("stats");
        assert_eq!(stats.closed_segments, 1);
        assert_eq!(stats.retained_logical_bytes, record.metadata.logical_bytes);
        assert_eq!(
            stats.retained_segment_physical_bytes,
            record.metadata.physical_bytes
        );
        assert!(stats.total_physical_bytes >= stats.retained_segment_physical_bytes);
        close(&store).await;
        drop(store);

        let reopened = HistoryStore::open(test_config(directory.path()))
            .await
            .expect("reopen store");
        assert_eq!(reopened.segments().await.expect("segments").len(), 1);
        let read = reopened
            .read_block(&record.metadata.id, BlockNumber(106))
            .await
            .expect("seek block");
        assert_eq!(read.frame, expected[6]);
        assert_eq!(read.records_decoded, 1);
        assert!(read.stored_bytes_read < record.metadata.physical_bytes);
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn selected_locators_publish_atomically_and_delete_with_segment() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(test_config(directory.path()))
            .await
            .expect("open store");
        let mut expected = frames(110, 111);
        let transaction_hashes = [
            TransactionHash::new([0xa1; 32]),
            TransactionHash::new([0xa2; 32]),
        ];
        for (frame, hash) in expected.iter_mut().zip(transaction_hashes) {
            frame.transactions = Material::Complete(vec![TransactionEnvelope {
                hash,
                transaction_type: 2,
                index: 0,
                encoded: Some(vec![0x02, 0xc0]),
                from: None,
                to: None,
                nonce: Some(0),
                gas_limit: Some(21_000),
                value: None,
                input: Some(Vec::new()),
                max_fee_per_gas: None,
                max_priority_fee_per_gas: None,
                max_fee_per_blob_gas: None,
                blob_versioned_hashes: Vec::new(),
                size_bytes: Some(2),
            }]);
            frame.receipts = Material::Missing(MissingReason::NotRequested);
        }
        let mut pending = store
            .begin_segment_indexed(
                SegmentId::new("atomic-locators").expect("segment ID"),
                descriptor(&expected),
                Compression::Snappy,
                SegmentReservation::new(1024 * 1024, 1024 * 1024),
                crate::RawHistoryIndexPolicy {
                    block_hash: true,
                    transaction_hash: true,
                    logs: false,
                },
            )
            .await
            .expect("begin indexed segment");
        for frame in &expected {
            pending.append(frame).expect("append indexed frame");
        }
        assert!(
            store
                .block_hash_locators(ChainId(1), expected[0].block.hash)
                .await
                .expect("pre-commit lookup")
                .is_empty()
        );
        let record = pending.commit(&[]).await.expect("publish locators");

        let mut alternate_descriptor = descriptor(&expected);
        alternate_descriptor.material_shape = MaterialShapeId([0x56; 32]);
        let mut alternate = store
            .begin_segment_indexed(
                SegmentId::new("atomic-locators-alternate").expect("segment ID"),
                alternate_descriptor,
                Compression::Snappy,
                SegmentReservation::new(1024 * 1024, 1024 * 1024),
                crate::RawHistoryIndexPolicy {
                    block_hash: true,
                    transaction_hash: true,
                    logs: false,
                },
            )
            .await
            .expect("begin alternate shape");
        for frame in &expected {
            alternate.append(frame).expect("append alternate frame");
        }
        let alternate = alternate
            .commit(&[])
            .await
            .expect("publish alternate locators");

        let blocks = store
            .block_hash_locators(ChainId(1), expected[1].block.hash)
            .await
            .expect("block locators");
        assert_eq!(blocks.len(), 2);
        let block = blocks
            .iter()
            .find(|locator| locator.segment_id == record.metadata.id)
            .expect("indexed block in first shape");
        assert_eq!(block.segment_id, record.metadata.id);
        assert_eq!(block.block_number, BlockNumber(111));
        let transactions = store
            .transaction_locators(ChainId(1), transaction_hashes[0])
            .await
            .expect("transaction locators");
        assert_eq!(transactions.len(), 2);
        let transaction = transactions
            .iter()
            .find(|locator| locator.segment_id == record.metadata.id)
            .expect("indexed transaction in first shape");
        assert_eq!(transaction.segment_id, record.metadata.id);
        assert_eq!(transaction.block_number, BlockNumber(110));
        assert_eq!(transaction.transaction_index, 0);
        let stats = store.stats().await.expect("stats");
        assert_eq!(stats.block_hash_locators, 4);
        assert_eq!(stats.transaction_locators, 4);

        store
            .delete_unowned(&record.metadata.id)
            .await
            .expect("delete segment");
        assert_eq!(
            store
                .transaction_locators(ChainId(1), transaction_hashes[0])
                .await
                .expect("post-delete lookup")
                .len(),
            1
        );
        store
            .delete_unowned(&alternate.metadata.id)
            .await
            .expect("delete alternate segment");
        assert!(
            store
                .transaction_locators(ChainId(1), transaction_hashes[0])
                .await
                .expect("post-delete lookup")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn ownership_blocks_deletion_until_explicit_release() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(test_config(directory.path()))
            .await
            .expect("open store");
        let record = Box::pin(publish(&store, "owned", &frames(10, 12))).await;
        store
            .add_owner(
                &record.metadata.id,
                SegmentOwnerKind::OperatorPin,
                "job:raw-1",
            )
            .await
            .expect("add owner");
        store
            .add_owner(
                &record.metadata.id,
                SegmentOwnerKind::OperatorPin,
                "job:raw-1",
            )
            .await
            .expect("duplicate owner is idempotent");
        assert_eq!(
            store
                .owners(&record.metadata.id)
                .await
                .expect("owners")
                .len(),
            1
        );
        assert!(matches!(
            store.delete_unowned(&record.metadata.id).await,
            Err(HistoryStoreError::OwnedSegment { owners: 1, .. })
        ));
        store
            .remove_owner(
                &record.metadata.id,
                SegmentOwnerKind::OperatorPin,
                "job:raw-1",
            )
            .await
            .expect("remove owner");
        store
            .delete_unowned(&record.metadata.id)
            .await
            .expect("delete unowned segment");
        assert!(
            store
                .segment(&record.metadata.id)
                .await
                .expect("lookup")
                .is_none()
        );
    }

    #[tokio::test]
    async fn initial_ownership_commits_atomically_with_segment() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(test_config(directory.path()))
            .await
            .expect("open store");
        let expected = frames(13, 14);
        let mut pending = store
            .begin_segment(
                SegmentId::new("atomically-owned").expect("ID"),
                descriptor(&expected),
                Compression::None,
                SegmentReservation::new(1024 * 1024, 1024 * 1024),
            )
            .await
            .expect("begin segment");
        for frame in &expected {
            pending.append(frame).expect("append frame");
        }
        let claim = SegmentOwnerClaim {
            kind: SegmentOwnerKind::OperatorPin,
            owner_id: "pin:atomic".to_owned(),
        };
        let record = pending
            .commit(&[claim.clone(), claim])
            .await
            .expect("commit segment and deduplicated owner");
        let owners = store.owners(&record.metadata.id).await.expect("owners");
        assert_eq!(owners.len(), 1);
        assert_eq!(owners[0].owner_id, "pin:atomic");
    }

    #[tokio::test]
    async fn reservations_admit_before_open_and_release_on_abort() {
        let directory = tempdir().expect("temporary directory");
        let config = HistoryStoreConfig::new(directory.path()).with_budget(StorageBudget {
            maximum_logical_bytes: 1_500,
            maximum_physical_bytes: 8 * 1024 * 1024,
            maximum_frame_logical_bytes: 500,
            maximum_segment_logical_bytes: 1_000,
            maximum_segment_physical_bytes: 1024 * 1024,
        });
        let store = HistoryStore::open(config).await.expect("open store");
        let frame_a = frames(1, 1);
        let pending = store
            .begin_segment(
                SegmentId::new("reservation-a").expect("ID"),
                descriptor(&frame_a),
                Compression::None,
                SegmentReservation::new(1_000, 1024 * 1024),
            )
            .await
            .expect("first reservation");
        let frame_b = frames(2, 2);
        let rejected = store
            .begin_segment(
                SegmentId::new("reservation-b").expect("ID"),
                descriptor(&frame_b),
                Compression::None,
                SegmentReservation::new(1_000, 1024 * 1024),
            )
            .await;
        assert!(matches!(
            rejected,
            Err(HistoryStoreError::LogicalBudget { .. })
        ));
        pending.abort().await.expect("release reservation");
        let admitted = store
            .begin_segment(
                SegmentId::new("reservation-b").expect("ID"),
                descriptor(&frame_b),
                Compression::None,
                SegmentReservation::new(1_000, 1024 * 1024),
            )
            .await
            .expect("reservation after release");
        admitted.abort().await.expect("abort second reservation");
    }

    #[tokio::test]
    async fn restart_removes_partial_files_and_stale_reservations() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(test_config(directory.path()))
            .await
            .expect("open store");
        let expected = frames(20, 22);
        let mut pending = store
            .begin_segment(
                SegmentId::new("partial-crash").expect("ID"),
                descriptor(&expected),
                Compression::None,
                SegmentReservation::new(1024 * 1024, 1024 * 1024),
            )
            .await
            .expect("begin segment");
        pending.append(&expected[0]).expect("append frame");
        drop(pending);
        close(&store).await;
        drop(store);

        let reopened = HistoryStore::open(test_config(directory.path()))
            .await
            .expect("recover store");
        assert_eq!(reopened.recovery_report().cleared_reservations, 1);
        assert_eq!(reopened.recovery_report().removed_partial_files, 2);
        assert!(reopened.segments().await.expect("segments").is_empty());
    }

    #[tokio::test]
    async fn close_before_catalog_commit_is_quarantined_on_restart() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(test_config(directory.path()))
            .await
            .expect("open store");
        let expected = frames(30, 32);
        let mut pending = store
            .begin_segment(
                SegmentId::new("orphan-crash").expect("ID"),
                descriptor(&expected),
                Compression::None,
                SegmentReservation::new(1024 * 1024, 1024 * 1024),
            )
            .await
            .expect("begin segment");
        for frame in &expected {
            pending.append(frame).expect("append frame");
        }
        pending
            .writer
            .take()
            .expect("writer")
            .finish(&pending.final_path)
            .expect("close before catalog commit");
        drop(pending);
        close(&store).await;
        drop(store);

        let reopened = HistoryStore::open(test_config(directory.path()))
            .await
            .expect("recover store");
        let recovery = reopened.recovery_report();
        assert_eq!(recovery.cleared_reservations, 1);
        assert_eq!(recovery.quarantined_closed_files, 1);
        assert_eq!(recovery.quarantined_corrupt_files, 0);
        assert!(reopened.segments().await.expect("segments").is_empty());
        assert!(
            reopened
                .stats()
                .await
                .expect("stats")
                .quarantine_physical_bytes
                > 0
        );
    }

    #[tokio::test]
    async fn a_corrupt_segment_is_quarantined_when_read_and_the_store_still_opens() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(test_config(directory.path()))
            .await
            .expect("open store");
        let record = Box::pin(publish(&store, "catalog-corrupt", &frames(40, 42))).await;
        close(&store).await;
        drop(store);
        let path = directory.path().join(&record.relative_path);
        let mut file = OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("open segment");
        file.seek(SeekFrom::Start(81)).expect("seek payload");
        file.write_all(&[0xaa]).expect("corrupt segment");
        file.sync_all().expect("persist corruption");
        // Audit M-H1: one corrupt segment refused the whole store.
        let reopened = HistoryStore::open(test_config(directory.path()))
            .await
            .expect("a corrupt segment does not refuse the store");
        let error = reopened
            .read_block(&record.metadata.id, BlockNumber(41))
            .await
            .expect_err("a corrupt segment is not served");
        assert!(
            matches!(error, HistoryStoreError::Quarantined { .. }),
            "{error}"
        );
        assert!(
            reopened
                .segment(&record.metadata.id)
                .await
                .expect("lookup")
                .is_none(),
            "the quarantined segment's coverage reads as missing"
        );
        assert!(!path.exists());
        assert!(
            reopened
                .stats()
                .await
                .expect("stats")
                .quarantine_physical_bytes
                >= record.metadata.physical_bytes
        );
        close(&reopened).await;
        drop(reopened);
        let clean = HistoryStore::open(test_config(directory.path()))
            .await
            .expect("reopen after the quarantine");
        assert!(clean.segments().await.expect("segments").is_empty());
    }

    #[tokio::test]
    async fn a_missing_segment_reads_as_missing_coverage() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(test_config(directory.path()))
            .await
            .expect("open store");
        let expected = frames(50, 58);
        let record = Box::pin(publish(&store, "catalog-missing", &expected[..3])).await;
        Box::pin(publish(&store, "catalog-present", &expected[3..6])).await;
        Box::pin(publish(&store, "catalog-present-too", &expected[6..])).await;
        close(&store).await;
        drop(store);
        fs::remove_file(directory.path().join(&record.relative_path)).expect("remove segment");
        // Audit M-H1: a missing segment refused the whole store.
        let reopened = HistoryStore::open(test_config(directory.path()))
            .await
            .expect("a missing segment does not refuse the store");
        assert_eq!(reopened.recovery_report().missing_segments, 1);
        assert_eq!(reopened.segments().await.expect("segments").len(), 2);
        assert!(matches!(
            reopened
                .read_block(&record.metadata.id, BlockNumber(51))
                .await,
            Err(HistoryStoreError::UnknownSegment(_))
        ));
    }

    #[tokio::test]
    async fn a_segment_directory_gone_at_open_keeps_its_rows() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(test_config(directory.path()))
            .await
            .expect("open store");
        let expected = frames(200, 208);
        let mut records = Vec::new();
        for (index, id) in ["away-a", "away-b", "away-c"].into_iter().enumerate() {
            let blocks = &expected[index * 3..index * 3 + 3];
            records.push(Box::pin(publish(&store, id, blocks)).await);
        }
        close(&store).await;
        drop(store);
        let segments = directory.path().join("segments");
        let away = directory.path().join("segments-away");
        fs::rename(&segments, &away).expect("unmount the segments");
        // Review M1: every row was dropped, so the files, once back, were
        // quarantined as orphans and collected.
        let reopened = HistoryStore::open(test_config(directory.path()))
            .await
            .expect("open without the segments");
        let recovery = reopened.recovery_report();
        assert_eq!(
            (recovery.unavailable_segments, recovery.missing_segments),
            (3, 0)
        );
        assert_eq!(reopened.segments().await.expect("segments").len(), 3);
        let unavailable = reopened
            .read_block(&records[0].metadata.id, BlockNumber(200))
            .await;
        assert!(
            matches!(unavailable, Err(HistoryStoreError::Segment(_))),
            "{unavailable:?}"
        );
        assert_eq!(reopened.segments().await.expect("segments").len(), 3);
        close(&reopened).await;
        drop(reopened);
        fs::remove_dir(&segments).expect("remove the empty mount point");
        fs::rename(&away, &segments).expect("mount the segments again");
        let restored = HistoryStore::open(test_config(directory.path()))
            .await
            .expect("reopen");
        assert_eq!(restored.recovery_report(), RecoveryReport::default());
        for (record, frame) in records.iter().zip(expected.iter().step_by(3)) {
            assert_eq!(
                &restored
                    .read_block(&record.metadata.id, frame.block.number)
                    .await
                    .expect("read restored segment")
                    .frame,
                frame
            );
        }
    }

    #[tokio::test]
    async fn a_segment_file_gone_while_open_keeps_its_row() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(test_config(directory.path()))
            .await
            .expect("open store");
        let record = Box::pin(publish(&store, "gone-while-open", &frames(210, 212))).await;
        let path = directory.path().join(&record.relative_path);
        let away = directory.path().join("gone-while-open.idxraw");
        fs::rename(&path, &away).expect("take the file away");
        let unavailable = store
            .read_block(&record.metadata.id, BlockNumber(211))
            .await;
        // Review M1: a read dropped the row, so a mount that went away lost
        // the coverage of every segment read meanwhile.
        assert!(
            matches!(unavailable, Err(HistoryStoreError::Segment(_))),
            "{unavailable:?}"
        );
        assert!(
            store
                .segment(&record.metadata.id)
                .await
                .expect("lookup")
                .is_some()
        );
        fs::rename(&away, &path).expect("bring the file back");
        store
            .read_block(&record.metadata.id, BlockNumber(211))
            .await
            .expect("readable again");
    }

    #[tokio::test]
    async fn a_truncated_segment_is_quarantined_at_open() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(test_config(directory.path()))
            .await
            .expect("open store");
        let record = Box::pin(publish(&store, "catalog-truncated", &frames(54, 56))).await;
        close(&store).await;
        drop(store);
        let path = directory.path().join(&record.relative_path);
        OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("open segment")
            .set_len(record.metadata.physical_bytes - 1)
            .expect("truncate segment");
        // Audit M-H1: a truncated segment refused the whole store.
        let reopened = HistoryStore::open(test_config(directory.path()))
            .await
            .expect("a truncated segment does not refuse the store");
        assert!(reopened.segments().await.expect("segments").is_empty());
        assert!(!path.exists());
        assert!(
            reopened
                .stats()
                .await
                .expect("stats")
                .quarantine_physical_bytes
                > 0
        );
    }

    #[tokio::test]
    async fn a_store_over_its_budget_opens_and_refuses_new_segments() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(test_config(directory.path()))
            .await
            .expect("open store");
        let expected = frames(60, 65);
        let first = Box::pin(publish(&store, "budget-first", &expected[..3])).await;
        Box::pin(publish(&store, "budget-second", &expected[3..])).await;
        let retained = store.stats().await.expect("stats").retained_logical_bytes;
        close(&store).await;
        drop(store);
        // The operator lowers the budget below what is already retained.
        let smaller = retained - 1;
        let lowered = HistoryStoreConfig::new(directory.path()).with_budget(StorageBudget {
            maximum_logical_bytes: smaller,
            maximum_physical_bytes: 32 * 1024 * 1024,
            maximum_frame_logical_bytes: smaller,
            maximum_segment_logical_bytes: smaller,
            maximum_segment_physical_bytes: 4 * 1024 * 1024,
        });
        // Audit M-H1: a budget overshoot refused to open the store.
        let reopened = HistoryStore::open(lowered)
            .await
            .expect("an over-budget store opens");
        assert_eq!(
            reopened
                .read_block(&first.metadata.id, BlockNumber(61))
                .await
                .expect("retained history stays readable")
                .frame,
            expected[1]
        );
        let refused = reopened
            .begin_segment(
                SegmentId::new("budget-third").expect("ID"),
                descriptor(&frames(66, 66)),
                Compression::None,
                SegmentReservation::new(smaller.min(1024), 1024),
            )
            .await;
        assert!(
            matches!(refused, Err(HistoryStoreError::LogicalBudget { .. })),
            "{refused:?}"
        );
    }

    /// Put `names` in `root`'s quarantine, `bytes` each, the first the
    /// longest quarantined.
    fn quarantined_files(root: &Path, names: &[&str], bytes: u64) {
        let quarantine = root.join("quarantine");
        fs::create_dir_all(&quarantine).expect("quarantine directory");
        let now = SystemTime::now();
        for (age, name) in (1..=names.len()).rev().zip(names) {
            let file = fs::File::create(quarantine.join(name)).expect("quarantined file");
            file.set_len(bytes).expect("size quarantined file");
            let age = std::time::Duration::from_secs(60 * u64::try_from(age).expect("age"));
            file.set_modified(now - age).expect("age quarantined file");
        }
    }

    fn quarantine_names(root: &Path) -> Vec<String> {
        let mut names = fs::read_dir(root.join("quarantine"))
            .expect("quarantine directory")
            .map(|entry| {
                entry
                    .expect("quarantine entry")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    #[tokio::test]
    async fn quarantine_keeps_its_newest_files_within_its_cap() {
        let directory = tempdir().expect("temporary directory");
        quarantined_files(
            directory.path(),
            &["a-oldest.corrupt", "b-middle.corrupt", "c-newest.corrupt"],
            1_000,
        );
        let mut config = test_config(directory.path());
        config.quarantine_maximum_bytes = 2_000;
        config.budget.maximum_segment_physical_bytes = 1_000;
        let store = HistoryStore::open(config).await.expect("open store");
        // Audit M-H1: quarantined files were never collected.
        assert_eq!(store.recovery_report().collected_quarantine_files, 1);
        // Review M9: the test counted the files kept, not which.
        assert_eq!(
            quarantine_names(directory.path()),
            ["b-middle.corrupt", "c-newest.corrupt"]
        );
    }

    #[tokio::test]
    async fn the_quarantine_holds_at_least_the_largest_segment() {
        let directory = tempdir().expect("temporary directory");
        quarantined_files(
            directory.path(),
            &["a-older.corrupt", "b-newer.corrupt"],
            1_000,
        );
        let mut config = test_config(directory.path());
        // Less than the 4 MiB one segment may take.
        config.quarantine_maximum_bytes = 500;
        HistoryStore::open(config).await.expect("open store");
        // Review M5: a cap below one segment deleted every quarantined
        // segment before anyone could inspect it.
        assert_eq!(
            quarantine_names(directory.path()),
            ["a-older.corrupt", "b-newer.corrupt"]
        );
    }

    #[tokio::test]
    async fn the_newest_quarantined_file_outlives_the_cap() {
        let directory = tempdir().expect("temporary directory");
        quarantined_files(
            directory.path(),
            &["a-older.corrupt", "b-newer.corrupt"],
            1_000,
        );
        let mut config = test_config(directory.path());
        config.quarantine_maximum_bytes = 100;
        config.budget.maximum_segment_physical_bytes = 100;
        HistoryStore::open(config).await.expect("open store");
        // Review M5: a file larger than the cap was deleted as soon as it was
        // quarantined.
        assert_eq!(quarantine_names(directory.path()), ["b-newer.corrupt"]);
    }

    #[tokio::test]
    async fn locator_growth_is_reserved_at_admission() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(test_config(directory.path()))
            .await
            .expect("open store");
        let pending = store
            .begin_segment_indexed(
                SegmentId::new("reserved-locators").expect("ID"),
                descriptor(&frames(80, 82)),
                Compression::Snappy,
                SegmentReservation::new(1024 * 1024, 1024 * 1024),
                crate::RawHistoryIndexPolicy {
                    block_hash: true,
                    transaction_hash: true,
                    logs: false,
                },
            )
            .await
            .expect("begin indexed segment");
        let reserved = store.stats().await.expect("stats").reserved_physical_bytes;
        // Audit M-H1: the catalog growth of a segment's locators was not
        // reserved.
        assert!(
            reserved > 1024 * 1024 + CATALOG_RESERVATION_OVERHEAD_BYTES,
            "{reserved}"
        );
        pending.abort().await.expect("abort");
    }

    #[tokio::test]
    async fn admission_refuses_a_segment_whose_locators_would_not_fit() {
        let directory = tempdir().expect("temporary directory");
        let measured = {
            let store = HistoryStore::open(test_config(directory.path()))
                .await
                .expect("open store");
            let total = store.stats().await.expect("stats").total_physical_bytes;
            close(&store).await;
            total
        };
        // Room for the segment and the catalog's fixed overhead, not for a
        // locator per block across thousands of blocks.
        let physical = measured + 1024 * 1024 + CATALOG_RESERVATION_OVERHEAD_BYTES + 256 * 1024;
        let store = HistoryStore::open(HistoryStoreConfig::new(directory.path()).with_budget(
            StorageBudget {
                maximum_logical_bytes: 32 * 1024 * 1024,
                maximum_physical_bytes: physical,
                maximum_frame_logical_bytes: 1024 * 1024,
                maximum_segment_logical_bytes: 4 * 1024 * 1024,
                maximum_segment_physical_bytes: 1024 * 1024,
            },
        ))
        .await
        .expect("reopen with a tight budget");
        let mut wide = descriptor(&frames(1, 1));
        wide.range = BlockRange::new(BlockNumber(1), BlockNumber(4_096)).expect("range");
        store
            .begin_segment(
                SegmentId::new("unindexed-wide").expect("ID"),
                wide.clone(),
                Compression::None,
                SegmentReservation::new(1024 * 1024, 1024 * 1024),
            )
            .await
            .expect("a segment without locators fits")
            .abort()
            .await
            .expect("abort");
        let refused = store
            .begin_segment_indexed(
                SegmentId::new("indexed-wide").expect("ID"),
                wide,
                Compression::None,
                SegmentReservation::new(1024 * 1024, 1024 * 1024),
                crate::RawHistoryIndexPolicy {
                    block_hash: true,
                    transaction_hash: true,
                    logs: false,
                },
            )
            .await;
        assert!(
            matches!(refused, Err(HistoryStoreError::PhysicalBudget { .. })),
            "{refused:?}"
        );
    }

    fn frames_with_transactions(start: u64, end: u64, per_block: u32) -> Vec<BlockFrame> {
        let mut frames = frames(start, end);
        for frame in &mut frames {
            let block = frame.block.number.0;
            frame.transactions = Material::Complete(
                (0..per_block)
                    .map(|index| {
                        // Spread like real transaction hashes.
                        let mut key = [0; 12];
                        key[..8].copy_from_slice(&block.to_be_bytes());
                        key[8..].copy_from_slice(&index.to_be_bytes());
                        TransactionEnvelope {
                            hash: TransactionHash::new(*blake3::hash(&key).as_bytes()),
                            transaction_type: 2,
                            index,
                            encoded: None,
                            from: None,
                            to: None,
                            nonce: None,
                            gas_limit: None,
                            value: None,
                            input: None,
                            max_fee_per_gas: None,
                            max_priority_fee_per_gas: None,
                            max_fee_per_blob_gas: None,
                            blob_versioned_hashes: Vec::new(),
                            size_bytes: None,
                        }
                    })
                    .collect(),
            );
            frame.receipts = Material::Missing(MissingReason::NotRequested);
        }
        frames
    }

    #[tokio::test]
    async fn locators_beyond_the_estimate_commit_and_the_next_admission_refuses() {
        let directory = tempdir().expect("temporary directory");
        let expected = frames_with_transactions(90, 92, 1_200);
        let indexes = crate::RawHistoryIndexPolicy {
            block_hash: true,
            transaction_hash: true,
            logs: false,
        };
        let begin = |store: HistoryStore| {
            let segment = descriptor(&expected);
            async move {
                store
                    .begin_segment_indexed(
                        SegmentId::new("outgrown-locators").expect("ID"),
                        segment,
                        Compression::Snappy,
                        SegmentReservation::new(1024 * 1024, 1024 * 1024),
                        indexes,
                    )
                    .await
            }
        };
        close(
            &HistoryStore::open(test_config(directory.path()))
                .await
                .expect("create store"),
        )
        .await;
        // Measured on a reopened catalog, as the tight budget below sees it.
        let (measured, reserved) = {
            let store = HistoryStore::open(test_config(directory.path()))
                .await
                .expect("open store");
            let total = store.stats().await.expect("stats").total_physical_bytes;
            let pending = begin(store.clone()).await.expect("begin");
            let reserved = store.stats().await.expect("stats").reserved_physical_bytes;
            pending.abort().await.expect("abort");
            close(&store).await;
            (total, reserved)
        };
        let store = HistoryStore::open(HistoryStoreConfig::new(directory.path()).with_budget(
            StorageBudget {
                maximum_logical_bytes: 32 * 1024 * 1024,
                maximum_physical_bytes: measured
                    + reserved
                    + CATALOG_RESERVATION_OVERHEAD_BYTES
                    + 64 * 1024,
                maximum_frame_logical_bytes: 1024 * 1024,
                maximum_segment_logical_bytes: 4 * 1024 * 1024,
                maximum_segment_physical_bytes: 1024 * 1024,
            },
        ))
        .await
        .expect("reopen with a tight budget");
        let mut pending = begin(store.clone())
            .await
            .expect("the reserved estimate fits");
        for frame in &expected {
            pending.append(frame).expect("append frame");
        }
        let final_path = pending.final_path.clone();
        // Review I4: publication refused the locators beyond the estimate, so
        // a job near its budget paused, resumed, and acquired the same
        // segment again forever.
        pending
            .commit(&[])
            .await
            .expect("one segment's excess locators are accepted");
        assert!(final_path.exists());
        assert_eq!(
            store.stats().await.expect("stats").transaction_locators,
            3 * 1_200
        );
        // The next admission sees the excess and refuses before any download.
        let next = store
            .begin_segment_indexed(
                SegmentId::new("after-the-excess").expect("ID"),
                descriptor(&frames(93, 95)),
                Compression::Snappy,
                SegmentReservation::new(1024 * 1024, 1024 * 1024),
                indexes,
            )
            .await;
        assert!(
            matches!(next, Err(HistoryStoreError::PhysicalBudget { .. })),
            "{next:?}"
        );
    }

    #[tokio::test]
    async fn repeated_reads_validate_a_segment_once() {
        let directory = tempdir().expect("temporary directory");
        let expected = frames(130, 134);
        let store = HistoryStore::open(test_config(directory.path()))
            .await
            .expect("open store");
        let record = Box::pin(publish(&store, "validated-once", &expected)).await;
        for frame in &expected {
            let read = store
                .read_block(&record.metadata.id, frame.block.number)
                .await
                .expect("read block");
            assert_eq!(&read.frame, frame);
        }
        // The commit verified what it wrote.
        assert_eq!(store.segment_validations(), 0);
        close(&store).await;
        drop(store);

        let reopened = HistoryStore::open(test_config(directory.path()))
            .await
            .expect("reopen store");
        for _ in 0..3 {
            for frame in &expected {
                reopened
                    .read_block(&record.metadata.id, frame.block.number)
                    .await
                    .expect("read block");
            }
        }
        // Audit H17: every lookup hashed the whole segment again.
        assert_eq!(reopened.segment_validations(), 1);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn segment_reads_leave_the_runtime_free_while_their_file_blocks() {
        use std::{
            process::Command,
            sync::{atomic::AtomicBool, mpsc},
            time::Duration,
        };

        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(test_config(directory.path()))
            .await
            .expect("open store");
        let record = Box::pin(publish(&store, "blocking-read", &frames(120, 122))).await;
        close(&store).await;
        drop(store);
        let store = HistoryStore::open(test_config(directory.path()))
            .await
            .expect("reopen store");
        // A FIFO in place of the segment blocks whoever opens it for reading
        // until a writer arrives, as a stalled disk would.
        let path = directory.path().join(&record.relative_path);
        fs::remove_file(&path).expect("remove segment");
        assert!(
            Command::new("mkfifo")
                .arg(&path)
                .status()
                .expect("run mkfifo")
                .success()
        );
        let released = Arc::new(AtomicBool::new(false));
        let (done, finished) = mpsc::channel::<()>();
        let watchdog = {
            let released = released.clone();
            let path = path.clone();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_secs(3));
                released.store(true, Ordering::SeqCst);
                // Opening a FIFO for reading and writing never blocks, and
                // gives the blocked read its writer.
                let fifo = OpenOptions::new().read(true).write(true).open(&path);
                let _ = finished.recv_timeout(Duration::from_secs(30));
                drop(fifo);
            })
        };
        let read = tokio::spawn({
            let store = store.clone();
            let id = record.metadata.id.clone();
            async move { store.read_block(&id, BlockNumber(121)).await }
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        // Audit H17: the read held the runtime thread until its file became
        // readable.
        let blocked = released.load(Ordering::SeqCst);
        let _ = read.await;
        let _ = done.send(());
        watchdog.join().expect("watchdog");
        assert!(
            !blocked,
            "a segment read blocked the runtime until its file became readable"
        );
    }

    #[tokio::test]
    async fn a_commit_that_fails_after_closing_its_file_leaves_nothing_behind() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(test_config(directory.path()))
            .await
            .expect("open store");
        let expected = frames(100, 102);
        let mut pending = store
            .begin_segment(
                SegmentId::new("late-failure").expect("ID"),
                descriptor(&expected),
                Compression::Snappy,
                SegmentReservation::new(1024 * 1024, 1024 * 1024),
            )
            .await
            .expect("begin segment");
        for frame in &expected {
            pending.append(frame).expect("append frame");
        }
        let final_path = pending.final_path.clone();
        // The catalog insert fails after the writer closed the file.
        sqlx::query("DELETE FROM raw_segment_reservations WHERE segment_id = 'late-failure'")
            .execute(&store.inner.pool)
            .await
            .expect("drop the reservation");
        let failed = pending.commit(&[]).await;
        assert!(
            matches!(failed, Err(HistoryStoreError::MissingReservation(_))),
            "{failed:?}"
        );
        // Audit M-H2: the closed file stayed behind until a restart.
        assert!(!final_path.exists());
        assert_eq!(
            fs::read_dir(directory.path().join("segments"))
                .expect("segments directory")
                .count(),
            0
        );
    }

    #[tokio::test]
    async fn a_second_publication_of_the_same_material_adopts_the_first() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(test_config(directory.path()))
            .await
            .expect("open store");
        let expected = frames(110, 112);
        let mut first = store
            .begin_segment(
                SegmentId::new("overlap-first").expect("ID"),
                descriptor(&expected),
                Compression::Snappy,
                SegmentReservation::new(1024 * 1024, 1024 * 1024),
            )
            .await
            .expect("begin first");
        let mut second = store
            .begin_segment(
                SegmentId::new("overlap-second").expect("ID"),
                descriptor(&expected),
                Compression::Snappy,
                SegmentReservation::new(1024 * 1024, 1024 * 1024),
            )
            .await
            .expect("begin second");
        for frame in &expected {
            first.append(frame).expect("append first");
            second.append(frame).expect("append second");
        }
        let second_path = second.final_path.clone();
        let pin = |owner: &str| SegmentOwnerClaim {
            kind: SegmentOwnerKind::OperatorPin,
            owner_id: owner.to_owned(),
        };
        let first = first
            .commit(&[pin("pin:first")])
            .await
            .expect("commit first");
        // Audit M-H2: the second publication hit the unique material identity
        // and failed.
        let adopted = second
            .commit(&[pin("pin:second")])
            .await
            .expect("the second publication adopts the first");
        assert_eq!(adopted.metadata.id, first.metadata.id);
        assert_eq!(store.segments().await.expect("segments").len(), 1);
        assert_eq!(
            store
                .owners(&first.metadata.id)
                .await
                .expect("owners")
                .into_iter()
                .map(|owner| owner.owner_id)
                .collect::<Vec<_>>(),
            vec!["pin:first".to_owned(), "pin:second".to_owned()]
        );
        assert!(!second_path.exists());
        let stats = store.stats().await.expect("stats");
        assert_eq!(stats.reserved_physical_bytes, 0);
        assert_eq!(stats.temporary_physical_bytes, 0);
    }

    #[tokio::test]
    async fn a_publication_that_disagrees_with_a_retained_segment_is_not_adopted() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(test_config(directory.path()))
            .await
            .expect("open store");
        let expected = frames(114, 116);
        Box::pin(publish(&store, "retained-first", &expected)).await;
        // The same blocks on top of another parent.
        let mut parent = BlockHash::new([0x23; 32]);
        let forked = (114..=116)
            .map(|number| {
                let frame = fixture_frame(number, parent);
                parent = frame.block.hash;
                frame
            })
            .collect::<Vec<_>>();
        let mut pending = store
            .begin_segment(
                SegmentId::new("retained-fork").expect("ID"),
                descriptor(&forked),
                Compression::Snappy,
                SegmentReservation::new(1024 * 1024, 1024 * 1024),
            )
            .await
            .expect("begin fork");
        for frame in &forked {
            pending.append(frame).expect("append fork");
        }
        let fork_path = pending.final_path.clone();
        let refused = pending.commit(&[]).await;
        assert!(
            matches!(refused, Err(HistoryStoreError::CatalogIntegrity(_))),
            "{refused:?}"
        );
        assert!(!fork_path.exists());
        assert_eq!(store.segments().await.expect("segments").len(), 1);
    }

    #[tokio::test]
    async fn catalog_paths_are_opened_literally() {
        let directory = tempdir().expect("temporary directory");
        let root = directory.path().join("history%41");
        // Audit Store-2: the catalog URL percent-decoded its path.
        let store = HistoryStore::open(test_config(&root))
            .await
            .expect("open a percent-named store");
        assert!(root.join("catalog.sqlite").exists());
        assert!(!directory.path().join("historyA").exists());
        close(&store).await;
    }

    #[tokio::test]
    async fn restart_completes_interrupted_unowned_deletion() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(test_config(directory.path()))
            .await
            .expect("open store");
        let record = Box::pin(publish(&store, "deleting-crash", &frames(60, 62))).await;
        sqlx::query("UPDATE raw_segments SET state = 'deleting' WHERE segment_id = ?")
            .bind(record.metadata.id.as_str())
            .execute(&store.inner.pool)
            .await
            .expect("mark deleting");
        close(&store).await;
        drop(store);

        let reopened = HistoryStore::open(test_config(directory.path()))
            .await
            .expect("finish deletion");
        assert_eq!(reopened.recovery_report().completed_deletions, 1);
        assert!(!directory.path().join(record.relative_path).exists());
        assert!(reopened.segments().await.expect("segments").is_empty());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_first_read_that_stalls_does_not_hold_up_another_segment() {
        use std::{process::Command, time::Duration};

        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(test_config(directory.path()))
            .await
            .expect("open store");
        let stalled = Box::pin(publish(&store, "first-read-stalled", &frames(220, 222))).await;
        let free = Box::pin(publish(&store, "first-read-free", &frames(223, 225))).await;
        close(&store).await;
        drop(store);
        // Reopened, neither segment is verified yet.
        let store = HistoryStore::open(test_config(directory.path()))
            .await
            .expect("reopen store");
        let path = directory.path().join(&stalled.relative_path);
        fs::remove_file(&path).expect("remove segment");
        assert!(
            Command::new("mkfifo")
                .arg(&path)
                .status()
                .expect("run mkfifo")
                .success()
        );
        let blocked = tokio::spawn({
            let store = store.clone();
            let id = stalled.metadata.id.clone();
            async move { store.read_block(&id, BlockNumber(221)).await }
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        let other = tokio::time::timeout(
            Duration::from_secs(2),
            store.read_block(&free.metadata.id, BlockNumber(224)),
        )
        .await;
        // Opening a FIFO for reading and writing never blocks, and gives the
        // stalled read its writer.
        let writer = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("open the FIFO");
        tokio::time::sleep(Duration::from_millis(200)).await;
        drop(writer);
        let _ = tokio::time::timeout(Duration::from_secs(10), blocked).await;
        // Review M2: one lock serialized every segment's first verification,
        // so one stalled file held up reads of all others.
        assert!(matches!(other, Ok(Ok(_))), "{other:?}");
    }

    #[tokio::test]
    async fn a_record_corrupted_after_verification_fails_its_checksum() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(test_config(directory.path()))
            .await
            .expect("open store");
        // The commit verified the file; its first record changes afterwards.
        let record = Box::pin(publish(&store, "late-corrupt", &frames(230, 232))).await;
        let mut file = OpenOptions::new()
            .write(true)
            .open(directory.path().join(&record.relative_path))
            .expect("open segment");
        file.seek(SeekFrom::Start(81)).expect("seek payload");
        file.write_all(&[0xaa]).expect("corrupt record");
        file.sync_all().expect("persist corruption");
        let error = store
            .read_block(&record.metadata.id, BlockNumber(230))
            .await
            .expect_err("a corrupt record is not served");
        assert!(
            matches!(&error, HistoryStoreError::Quarantined { reason, .. } if reason.contains("checksum")),
            "{error}"
        );
        assert!(
            store
                .segment(&record.metadata.id)
                .await
                .expect("lookup")
                .is_none()
        );
    }

    #[tokio::test]
    async fn a_seek_entry_corrupted_after_verification_fails_its_bounds() {
        use std::io::Read;

        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(test_config(directory.path()))
            .await
            .expect("open store");
        let record = Box::pin(publish(&store, "late-entry", &frames(234, 236))).await;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(directory.path().join(&record.relative_path))
            .expect("open segment");
        let mut trailer = [0; 56];
        file.seek(SeekFrom::End(-56)).expect("seek trailer");
        file.read_exact(&mut trailer).expect("read trailer");
        let directory_offset =
            u64::from_be_bytes(trailer[..8].try_into().expect("directory offset"));
        // The second entry's stored length now reaches past the payload.
        file.seek(SeekFrom::Start(directory_offset + 16 + 120 + 16))
            .expect("seek entry");
        file.write_all(&u32::MAX.to_be_bytes())
            .expect("corrupt entry");
        file.sync_all().expect("persist corruption");
        let error = store
            .read_block(&record.metadata.id, BlockNumber(235))
            .await
            .expect_err("a record outside the payload is not served");
        assert!(
            matches!(&error, HistoryStoreError::Quarantined { reason, .. } if reason.contains("outside the payload")),
            "{error}"
        );
    }

    #[tokio::test]
    async fn a_quarantine_whose_move_fails_still_reports_the_quarantine() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(test_config(directory.path()))
            .await
            .expect("open store");
        let record = Box::pin(publish(&store, "unmovable", &frames(240, 242))).await;
        let mut file = OpenOptions::new()
            .write(true)
            .open(directory.path().join(&record.relative_path))
            .expect("open segment");
        file.seek(SeekFrom::Start(81)).expect("seek payload");
        file.write_all(&[0xaa]).expect("corrupt record");
        file.sync_all().expect("persist corruption");
        let quarantine = directory.path().join("quarantine");
        fs::remove_dir(&quarantine).expect("remove quarantine");
        fs::write(&quarantine, b"not a directory").expect("block the quarantine");
        let error = store
            .read_block(&record.metadata.id, BlockNumber(240))
            .await
            .expect_err("a corrupt record is not served");
        // Review M7: the catalog dropped the segment, then the failed move
        // returned an I/O error, which callers retry as transient.
        assert!(
            matches!(error, HistoryStoreError::Quarantined { .. }),
            "{error}"
        );
        assert!(
            store
                .segment(&record.metadata.id)
                .await
                .expect("lookup")
                .is_none()
        );
    }

    #[tokio::test]
    async fn a_publication_replaces_a_retained_segment_whose_file_is_gone() {
        let directory = tempdir().expect("temporary directory");
        let expected = frames(260, 262);
        let store = HistoryStore::open(test_config(directory.path()))
            .await
            .expect("open store");
        let retained = Box::pin(publish(&store, "gone-retained", &expected)).await;
        fs::remove_file(directory.path().join(&retained.relative_path))
            .expect("lose the retained file");
        let mut pending = store
            .begin_segment(
                SegmentId::new("replacing-copy").expect("ID"),
                descriptor(&expected),
                Compression::Snappy,
                SegmentReservation::new(1024 * 1024, 1024 * 1024),
            )
            .await
            .expect("begin the replacing copy");
        for frame in &expected {
            pending.append(frame).expect("append frame");
        }
        let published = pending.commit(&[]).await.expect("publish");
        // Review 2 N4: the copy was discarded for a retained segment whose
        // file was gone.
        assert_eq!(published.metadata.id.as_str(), "replacing-copy");
        assert_eq!(store.segments().await.expect("segments").len(), 1);
        assert_eq!(
            store
                .read_block(&published.metadata.id, BlockNumber(261))
                .await
                .expect("read the replacing copy")
                .frame,
            expected[1]
        );
    }

    #[tokio::test]
    async fn adoption_verifies_the_retained_segment_first() {
        let directory = tempdir().expect("temporary directory");
        let expected = frames(250, 252);
        let store = HistoryStore::open(test_config(directory.path()))
            .await
            .expect("open store");
        let retained = Box::pin(publish(&store, "adopt-retained", &expected)).await;
        close(&store).await;
        drop(store);
        let mut file = OpenOptions::new()
            .write(true)
            .open(directory.path().join(&retained.relative_path))
            .expect("open segment");
        file.seek(SeekFrom::Start(81)).expect("seek payload");
        file.write_all(&[0xaa]).expect("corrupt record");
        file.sync_all().expect("persist corruption");
        // Reopened, the retained segment is not verified yet.
        let store = HistoryStore::open(test_config(directory.path()))
            .await
            .expect("reopen store");
        let mut pending = store
            .begin_segment(
                SegmentId::new("adopt-fresh").expect("ID"),
                descriptor(&expected),
                Compression::Snappy,
                SegmentReservation::new(1024 * 1024, 1024 * 1024),
            )
            .await
            .expect("begin fresh copy");
        for frame in &expected {
            pending.append(frame).expect("append frame");
        }
        let fresh_path = pending.final_path.clone();
        let published = pending.commit(&[]).await.expect("publish");
        // Review M8: the fresh copy was discarded for a retained one that no
        // longer verifies.
        assert_eq!(published.metadata.id.as_str(), "adopt-fresh");
        assert!(fresh_path.exists());
        assert_eq!(store.segments().await.expect("segments").len(), 1);
        assert_eq!(
            store
                .read_block(&published.metadata.id, BlockNumber(251))
                .await
                .expect("read the fresh copy")
                .frame,
            expected[1]
        );
    }
}
