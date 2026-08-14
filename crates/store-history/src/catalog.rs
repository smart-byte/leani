use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
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
const CATALOG_RESERVATION_OVERHEAD_BYTES: u64 = 64 * 1024;
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
}

impl HistoryStoreConfig {
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            reader_connections: 4,
            budget: StorageBudget::default(),
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
    pub quarantined_closed_files: u64,
    pub quarantined_corrupt_files: u64,
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
    pub(crate) lifecycle: Mutex<()>,
    recovery: RecoveryReport,
}

/// Cloneable raw-history catalog and immutable segment owner.
#[derive(Clone, Debug)]
pub struct HistoryStore {
    pub(crate) inner: Arc<HistoryStoreInner>,
}

impl HistoryStore {
    /// Open the catalog, recover interrupted publications/deletions, and
    /// validate every committed segment before returning.
    ///
    /// # Errors
    ///
    /// Fails closed if a catalogued segment is missing or corrupt.
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
        let options = SqliteConnectOptions::from_str(&format!(
            "sqlite://{}",
            catalog_path.to_string_lossy()
        ))?
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
        let recovery = recover(&pool, &segments, &quarantine).await?;
        crate::job::reconcile_all_jobs(&pool).await?;
        let inner = Arc::new(HistoryStoreInner {
            pool,
            root: config.root,
            segments,
            quarantine,
            catalog_path,
            budget,
            lifecycle: Mutex::new(()),
            recovery,
        });
        let store = Self { inner };
        let stats = store.stats().await?;
        if stats.retained_logical_bytes > budget.maximum_logical_bytes
            || stats.total_physical_bytes > budget.maximum_physical_bytes
        {
            return Err(HistoryStoreError::ExistingBudgetExceeded {
                logical_limit: budget.maximum_logical_bytes,
                logical_observed: stats.retained_logical_bytes,
                physical_limit: budget.maximum_physical_bytes,
                physical_observed: stats.total_physical_bytes,
            });
        }
        Ok(store)
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
        if partial_path.exists() || index_path.exists() || final_path.exists() {
            return Err(HistoryStoreError::PathCollision(id));
        }

        let (retained_logical, reserved_logical, reserved_physical) =
            catalog_capacity(&self.inner.pool).await?;
        let root_physical = directory_bytes(&self.inner.root)?;
        let temporary_physical =
            reservation_temporary_bytes(&self.inner.pool, &self.inner.segments).await?;
        let projected_logical = retained_logical
            .checked_add(reserved_logical)
            .and_then(|value| value.checked_add(reservation.maximum_logical_bytes))
            .ok_or(HistoryStoreError::ArithmeticOverflow)?;
        let effective_physical = root_physical
            .saturating_sub(temporary_physical)
            .checked_add(reserved_physical)
            .and_then(|value| value.checked_add(reservation.maximum_physical_bytes))
            .and_then(|value| value.checked_add(CATALOG_RESERVATION_OVERHEAD_BYTES))
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
        .bind(u64_i64(
            reservation.maximum_physical_bytes,
            "reserved physical bytes",
        )?)
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
        let writer = match SegmentWriter::create(
            &partial_path,
            &index_path,
            id.clone(),
            descriptor,
            compression,
            limits,
        ) {
            Ok(writer) => writer,
            Err(error) => {
                sqlx::query("DELETE FROM raw_segment_reservations WHERE segment_id = ?")
                    .bind(id.as_str())
                    .execute(&self.inner.pool)
                    .await?;
                return Err(error.into());
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
            profile,
            indexes,
            block_locators: Vec::new(),
            transaction_locators: Vec::new(),
        })
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

    /// Open and validate one catalogued segment.
    ///
    /// # Errors
    ///
    /// Returns an error if the segment is unknown, corrupt, or differs from
    /// its catalog metadata.
    pub async fn reader(&self, id: &SegmentId) -> Result<SegmentReader, HistoryStoreError> {
        let record = self
            .segment(id)
            .await?
            .ok_or_else(|| HistoryStoreError::UnknownSegment(id.clone()))?;
        let reader = SegmentReader::open(
            self.resolve_relative_path(&record.relative_path)?,
            id.clone(),
        )?;
        ensure_metadata_matches(&record.metadata, reader.metadata())?;
        Ok(reader)
    }

    /// Seek and decode one block from a named segment.
    ///
    /// # Errors
    ///
    /// Returns an error if the segment or requested record fails validation.
    pub async fn read_block(
        &self,
        id: &SegmentId,
        block: BlockNumber,
    ) -> Result<SegmentRead, HistoryStoreError> {
        Ok(self.reader(id).await?.read_block(block)?)
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
        let path = self.resolve_relative_path(&relative_path)?;
        if let Err(error) = fs::remove_file(&path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            return Err(error.into());
        }
        sync_directory(&self.inner.segments)?;
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
        let quarantine_physical_bytes = directory_bytes(&self.inner.quarantine)?;
        let catalog_physical_bytes = sqlite_family_bytes(&self.inner.catalog_path)?;
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
            total_physical_bytes: directory_bytes(&self.inner.root)?,
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
    /// closed catalog record.
    ///
    /// # Errors
    ///
    /// Returns an error for incomplete/corrupt output or catalog publication
    /// failure. A close-before-catalog crash window is recovered on restart.
    #[allow(clippy::too_many_lines)]
    pub async fn commit(
        mut self,
        owners: &[SegmentOwnerClaim],
    ) -> Result<SegmentRecord, HistoryStoreError> {
        let owners = normalize_owner_claims(owners)?;
        let descriptor = self
            .writer
            .as_ref()
            .ok_or(HistoryStoreError::PendingConsumed)?
            .descriptor();
        if let Err(error) =
            crate::job::validate_initial_owner_claims(&self.store, descriptor, &owners).await
        {
            self.writer.take();
            self.cleanup_files(true)?;
            self.clear_reservation().await?;
            return Err(error);
        }
        let writer = self
            .writer
            .take()
            .ok_or(HistoryStoreError::PendingConsumed)?;
        let metadata = match writer.finish(&self.final_path) {
            Ok(metadata) => metadata,
            Err(error) => {
                self.cleanup_files(true)?;
                self.clear_reservation().await?;
                return Err(error.into());
            }
        };
        if metadata.logical_bytes > self.reservation.maximum_logical_bytes
            || metadata.physical_bytes > self.reservation.maximum_physical_bytes
        {
            return Err(HistoryStoreError::ReservationExceeded);
        }
        let created_at_unix_ms = unix_ms()?;
        let relative_path = format!("segments/{}", self.final_name);
        let mut transaction = self.store.inner.pool.begin().await?;
        let reservation_exists: i64 = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM raw_segment_reservations WHERE segment_id = ?)",
        )
        .bind(self.id.as_str())
        .fetch_one(&mut *transaction)
        .await?;
        if reservation_exists == 0 {
            return Err(HistoryStoreError::MissingReservation(self.id));
        }
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
        for owner in owners {
            sqlx::query(
                "INSERT INTO raw_segment_owners(
                    segment_id, owner_kind, owner_id, created_at_unix_ms
                 ) VALUES (?, ?, ?, ?)",
            )
            .bind(self.id.as_str())
            .bind(owner.kind.as_str())
            .bind(&owner.owner_id)
            .bind(u64_i64(created_at_unix_ms, "owner creation time")?)
            .execute(&mut *transaction)
            .await?;
            if owner.kind == SegmentOwnerKind::RawHistoryJob {
                crate::job::refresh_job_progress_tx(
                    &mut transaction,
                    &crate::RawHistoryJobId::new(owner.owner_id.clone())?,
                    created_at_unix_ms,
                )
                .await?;
            }
        }
        sqlx::query("DELETE FROM raw_segment_reservations WHERE segment_id = ?")
            .bind(self.id.as_str())
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(SegmentRecord {
            metadata,
            profile: self.profile,
            indexes: self.indexes,
            relative_path,
            created_at_unix_ms,
        })
    }

    /// Remove partial files and release capacity without publishing coverage.
    ///
    /// # Errors
    ///
    /// Returns an error if temporary files or the reservation cannot be
    /// removed.
    pub async fn abort(mut self) -> Result<(), HistoryStoreError> {
        self.writer.take();
        self.cleanup_files(true)?;
        self.clear_reservation().await
    }

    fn cleanup_files(&self, include_final: bool) -> Result<(), HistoryStoreError> {
        for path in [&self.partial_path, &self.index_path] {
            if let Err(error) = fs::remove_file(path)
                && error.kind() != std::io::ErrorKind::NotFound
            {
                return Err(error.into());
            }
        }
        if include_final
            && let Err(error) = fs::remove_file(&self.final_path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            return Err(error.into());
        }
        sync_directory(&self.store.inner.segments)?;
        Ok(())
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

    let rows = sqlx::query("SELECT * FROM raw_segments WHERE state = 'closed'")
        .fetch_all(pool)
        .await?;
    let mut catalogued = BTreeSet::new();
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
        let reader = SegmentReader::open(&path, record.metadata.id.clone()).map_err(|error| {
            HistoryStoreError::CatalogIntegrity(format!(
                "catalogued segment `{}` failed validation: {error}",
                record.metadata.id.as_str()
            ))
        })?;
        ensure_metadata_matches(&record.metadata, reader.metadata())?;
        catalogued.insert(name.to_owned());
    }

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
        let id_text = name.strip_suffix(".idxraw").unwrap_or(&name);
        let valid = SegmentId::new(id_text)
            .ok()
            .is_some_and(|id| SegmentReader::open(entry.path(), id).is_ok());
        quarantine_file(&entry.path(), quarantine, valid)?;
        report.quarantined_closed_files += 1;
        if !valid {
            report.quarantined_corrupt_files += 1;
        }
    }
    sync_directory(segments)?;
    sync_directory(quarantine)?;
    Ok(report)
}

async fn insert_segment(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    metadata: &SegmentMetadata,
    profile: crate::RawHistoryProfile,
    indexes: crate::RawHistoryIndexPolicy,
    relative_path: &str,
    created_at_unix_ms: u64,
) -> Result<(), HistoryStoreError> {
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
    .bind(match profile {
        crate::RawHistoryProfile::ProcessorReuse => 0_i64,
        crate::RawHistoryProfile::PostMergeExecutionRpc { .. } => 1_i64,
    })
    .bind(match profile {
        crate::RawHistoryProfile::ProcessorReuse => None,
        crate::RawHistoryProfile::PostMergeExecutionRpc { merge_block } => {
            Some(u64_i64(merge_block.0, "Merge block")?)
        }
    })
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

fn ensure_metadata_matches(
    expected: &SegmentMetadata,
    actual: &SegmentMetadata,
) -> Result<(), HistoryStoreError> {
    if expected != actual {
        return Err(HistoryStoreError::CatalogIntegrity(format!(
            "catalog metadata differs from closed segment `{}`",
            expected.id.as_str()
        )));
    }
    Ok(())
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
    let mut total = 0_u64;
    for row in &rows {
        let partial: String = row.try_get("partial_name")?;
        let index: String = row.try_get("index_name")?;
        let partial_bytes = file_bytes(&safe_child(segments, &partial)?)?;
        let index_bytes = file_bytes(&safe_child(segments, &index)?)?;
        total = total
            .checked_add(partial_bytes)
            .and_then(|value| value.checked_add(index_bytes))
            .ok_or(HistoryStoreError::ArithmeticOverflow)?;
    }
    Ok(total)
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

fn quarantine_file(path: &Path, quarantine: &Path, valid: bool) -> Result<(), HistoryStoreError> {
    let name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| HistoryStoreError::CatalogIntegrity("non-UTF8 segment name".to_owned()))?;
    let status = if valid { "orphan" } else { "corrupt" };
    let mut target = quarantine.join(format!("{name}.{status}"));
    let mut suffix = 0_u64;
    while target.exists() {
        suffix = suffix
            .checked_add(1)
            .ok_or(HistoryStoreError::ArithmeticOverflow)?;
        target = quarantine.join(format!("{name}.{status}.{suffix}"));
    }
    fs::rename(path, target)?;
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
        let metadata = entry.metadata()?;
        let bytes = if metadata.is_dir() {
            directory_bytes(&entry.path())?
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
    #[error(
        "existing raw history exceeds configured budgets: logical {logical_observed}/{logical_limit}, physical {physical_observed}/{physical_limit}"
    )]
    ExistingBudgetExceeded {
        logical_limit: u64,
        logical_observed: u64,
        physical_limit: u64,
        physical_observed: u64,
    },
    #[error("segment path already exists for `{0:?}`")]
    PathCollision(SegmentId),
    #[error("unknown or unreadable segment `{0:?}`")]
    UnknownSegment(SegmentId),
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
    async fn corrupt_catalogued_segment_prevents_store_reopen() {
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
            .open(path)
            .expect("open segment");
        file.seek(SeekFrom::Start(81)).expect("seek payload");
        file.write_all(&[0xaa]).expect("corrupt segment");
        file.sync_all().expect("persist corruption");
        assert!(matches!(
            HistoryStore::open(test_config(directory.path())).await,
            Err(HistoryStoreError::CatalogIntegrity(_))
        ));
    }

    #[tokio::test]
    async fn missing_catalogued_segment_prevents_store_reopen() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(test_config(directory.path()))
            .await
            .expect("open store");
        let record = Box::pin(publish(&store, "catalog-missing", &frames(50, 52))).await;
        close(&store).await;
        drop(store);
        fs::remove_file(directory.path().join(&record.relative_path)).expect("remove segment");
        assert!(matches!(
            HistoryStore::open(test_config(directory.path())).await,
            Err(HistoryStoreError::CatalogIntegrity(_))
        ));
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
}
