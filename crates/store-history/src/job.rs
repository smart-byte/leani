use std::collections::BTreeSet;

use alloy_consensus::{
    Header as ConsensusHeader, ReceiptEnvelope as ConsensusReceiptEnvelope, TxEnvelope,
    constants::EMPTY_OMMER_ROOT_HASH, transaction::SignerRecoverable,
};
use alloy_eips::eip2718::Decodable2718;
use alloy_primitives::{B256, U256};
use alloy_rlp::Decodable;
use leani_primitives::{
    BlockFrame, BlockNumber, BlockRange, Capability, CapabilitySet, ChainId, HeaderEnvelope,
    LogFieldSet, Material, ReceiptEnvelope, TransactionEnvelope, TrustModel,
};
use leani_source_api::{DataRequest, FieldProjection, FilterSet};
use serde::{Deserialize, Serialize};
use sqlx::{Row, Sqlite, Transaction};

use crate::{
    Compression, HistoryStore, HistoryStoreError, MaterialShapeId, SegmentDescriptor,
    SegmentOwnerClaim, SegmentOwnerKind, VerificationClass,
    catalog::{blob32, i64_u64, u64_i64, unix_ms},
};

/// Portable caller-visible raw-history job identifier.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct RawHistoryJobId(String);

impl RawHistoryJobId {
    /// Validate one portable job identifier.
    ///
    /// # Errors
    ///
    /// Rejects empty, overlong, or path-like identifiers.
    pub fn new(value: impl Into<String>) -> Result<Self, HistoryStoreError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 96
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        {
            return Err(HistoryStoreError::InvalidJob(format!(
                "invalid job ID `{value}`"
            )));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for RawHistoryJobId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

/// Whether storage pressure pauses resumable work or fails it terminally.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageLimitAction {
    Pause,
    Fail,
}

/// Raw segment retention independent from processor/output retention.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RawHistoryRetention {
    Full,
    Window { blocks: u64 },
}

/// Optional locator families built in later RPC phases.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RawHistoryIndexPolicy {
    pub block_hash: bool,
    pub transaction_hash: bool,
    pub logs: bool,
}

impl RawHistoryIndexPolicy {
    #[must_use]
    pub const fn contains(self, required: Self) -> bool {
        (!required.block_hash || self.block_hash)
            && (!required.transaction_hash || self.transaction_hash)
            && (!required.logs || self.logs)
    }
}

/// Durable product contract attached to retained raw material.
///
/// Processor reuse makes no claim that full Ethereum block RPC can be
/// reconstructed. The execution-RPC profile carries its chain-specific Merge
/// boundary so that exact post-Merge support cannot silently extend into
/// blocks whose ommers are not retained by the current frame schema.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RawHistoryProfile {
    #[default]
    ProcessorReuse,
    PostMergeExecutionRpc {
        merge_block: BlockNumber,
    },
}

impl RawHistoryProfile {
    #[must_use]
    pub const fn satisfies(self, required: Self) -> bool {
        match required {
            Self::ProcessorReuse => true,
            Self::PostMergeExecutionRpc {
                merge_block: required_merge,
            } => matches!(
                self,
                Self::PostMergeExecutionRpc { merge_block }
                    if merge_block.0 == required_merge.0
            ),
        }
    }
}

/// Immutable segment sizing/compression snapshot for one job.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RawHistorySegmentPolicy {
    pub target_blocks: u64,
    pub maximum_logical_bytes: u64,
    pub maximum_physical_bytes: u64,
    pub compression: Compression,
    pub on_limit: StorageLimitAction,
}

/// Durable projection and predicate semantics for retained raw frames.
///
/// Capabilities remain separate so complete supersets can later satisfy a
/// narrower consumer without changing projection/filter identity.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RawHistoryMaterialProfile {
    pub allow_filtered: bool,
    pub projection: FieldProjection,
    pub log_fields: LogFieldSet,
    pub filters: FilterSet,
}

impl Default for RawHistoryMaterialProfile {
    fn default() -> Self {
        Self {
            allow_filtered: false,
            projection: FieldProjection::default(),
            log_fields: LogFieldSet::ALL,
            filters: FilterSet::default(),
        }
    }
}

impl RawHistoryMaterialProfile {
    #[must_use]
    pub fn from_request(request: &DataRequest) -> Self {
        Self {
            allow_filtered: request.allow_filtered,
            projection: request.projection.clone(),
            log_fields: request.log_fields,
            filters: request.filters.clone(),
        }
    }

    /// Stable catalog identity for projection and predicate semantics.
    ///
    /// # Panics
    ///
    /// Panics only if serde cannot encode this in-memory profile into its
    /// infallible postcard representation.
    #[must_use]
    pub fn shape_id(&self) -> MaterialShapeId {
        if self == &Self::default() {
            return MaterialShapeId::COMPLETE_EXECUTION;
        }
        let encoded = postcard::to_allocvec(self)
            .expect("raw material profiles have an infallible durable encoding");
        MaterialShapeId(*blake3::hash(&encoded).as_bytes())
    }

    fn normalize(&mut self) {
        normalize_strings(&mut self.projection.header_fields);
        normalize_strings(&mut self.projection.transaction_fields);
        normalize_strings(&mut self.projection.receipt_fields);
        normalize_strings(&mut self.projection.log_fields);
        self.filters.senders.sort();
        self.filters.senders.dedup();
        self.filters.recipients.sort();
        self.filters.recipients.dedup();
    }
}

/// Canonical idempotency identity for one durable raw-history request.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RawHistoryJobSpec {
    pub chain_id: ChainId,
    pub ranges: Vec<BlockRange>,
    pub profile: RawHistoryProfile,
    pub material: RawHistoryMaterialProfile,
    pub required_capabilities: CapabilitySet,
    pub verification: VerificationClass,
    pub minimum_trust: TrustModel,
    pub source_policy_digest: [u8; 32],
    pub retention: RawHistoryRetention,
    pub segment: RawHistorySegmentPolicy,
    pub indexes: RawHistoryIndexPolicy,
}

impl RawHistoryJobSpec {
    #[allow(clippy::too_many_lines)]
    fn normalize(mut self) -> Result<Self, HistoryStoreError> {
        if self.chain_id.0 == 0 {
            return Err(HistoryStoreError::InvalidJob(
                "chain ID must be non-zero".to_owned(),
            ));
        }
        self.ranges = normalize_ranges(self.ranges)?;
        self.material.normalize();
        if !self.material.allow_filtered && self.material.filters != FilterSet::default() {
            return Err(HistoryStoreError::InvalidJob(
                "unfiltered raw material cannot carry source predicates".to_owned(),
            ));
        }
        if self.required_capabilities == CapabilitySet::NONE
            || self.required_capabilities.contains(Capability::Mempool)
        {
            return Err(HistoryStoreError::InvalidJob(
                "historical capabilities must be non-empty and cannot include mempool".to_owned(),
            ));
        }
        if self.indexes.logs {
            return Err(HistoryStoreError::InvalidJob(
                "raw log indexes are not implemented; use receipt scans or disable indexes.logs"
                    .to_owned(),
            ));
        }
        if self.indexes.transaction_hash
            && (self.material.allow_filtered
                || !self
                    .required_capabilities
                    .contains(Capability::Transactions))
        {
            return Err(HistoryStoreError::InvalidJob(
                "transaction-hash locators require complete unfiltered transaction material"
                    .to_owned(),
            ));
        }
        if let RawHistoryProfile::PostMergeExecutionRpc { merge_block } = self.profile {
            let first = self
                .ranges
                .first()
                .expect("normalized raw-history jobs always contain a range")
                .start();
            if first < merge_block {
                return Err(HistoryStoreError::InvalidJob(format!(
                    "post_merge_execution_rpc starts at Merge block {}; requested range starts at {}",
                    merge_block.0, first.0
                )));
            }
            if self.material != RawHistoryMaterialProfile::default() {
                return Err(HistoryStoreError::InvalidJob(
                    "post_merge_execution_rpc requires complete unfiltered material".to_owned(),
                ));
            }
            let execution = CapabilitySet::from_iter([
                Capability::Header,
                Capability::Transactions,
                Capability::Receipts,
            ]);
            if !self
                .required_capabilities
                .with_derivable()
                .contains_all(execution)
            {
                return Err(HistoryStoreError::InvalidJob(
                    "post_merge_execution_rpc requires complete headers, transactions, and receipts"
                        .to_owned(),
                ));
            }
            if self.verification < VerificationClass::TrustedDataset
                || self.minimum_trust < TrustModel::TrustedDataset
            {
                return Err(HistoryStoreError::InvalidJob(
                    "post_merge_execution_rpc requires trusted-dataset verification and trust or stronger"
                        .to_owned(),
                ));
            }
        }
        if self.segment.target_blocks == 0
            || self.segment.maximum_logical_bytes == 0
            || self.segment.maximum_physical_bytes == 0
        {
            return Err(HistoryStoreError::InvalidJob(
                "segment limits and target block span must be non-zero".to_owned(),
            ));
        }
        if matches!(self.retention, RawHistoryRetention::Window { blocks: 0 }) {
            return Err(HistoryStoreError::InvalidJob(
                "window retention must keep at least one block".to_owned(),
            ));
        }
        let required_trust = match self.verification {
            VerificationClass::BestEffort => TrustModel::Untrusted,
            VerificationClass::TrustedDataset => TrustModel::TrustedDataset,
            VerificationClass::Cryptographic => TrustModel::ProtocolVerified,
        };
        if self.minimum_trust < required_trust {
            return Err(HistoryStoreError::InvalidJob(
                "minimum trust is weaker than the requested verification class".to_owned(),
            ));
        }
        Ok(self)
    }

    pub(crate) fn validate_frame_profile(&self, frame: &BlockFrame) -> Result<(), &'static str> {
        let RawHistoryProfile::PostMergeExecutionRpc { merge_block } = self.profile else {
            return Ok(());
        };
        if frame.block.number < merge_block {
            return Err("execution-RPC frame precedes the configured Merge block");
        }
        let Material::Complete(header) = &frame.header else {
            return Err("execution-RPC frame lacks a complete canonical header");
        };
        if header.rlp.is_none()
            || header.transactions_root.is_none()
            || header.receipts_root.is_none()
            || header.gas_limit.is_none()
            || header.gas_used.is_none()
            || header.base_fee_per_gas.is_none()
            || header.size_bytes.is_none()
            || header.transaction_count.is_none()
        {
            return Err("execution-RPC frame lacks required canonical header fields");
        }
        validate_execution_header(frame, header)?;
        let Material::Complete(transactions) = &frame.transactions else {
            return Err("execution-RPC frame lacks complete transactions");
        };
        let Material::Complete(receipts) = &frame.receipts else {
            return Err("execution-RPC frame lacks complete receipts");
        };
        if transactions.len() != receipts.len()
            || usize::try_from(header.transaction_count.unwrap_or(u32::MAX)).ok()
                != Some(transactions.len())
        {
            return Err("execution-RPC transaction counts disagree");
        }
        validate_execution_transactions(transactions, receipts)?;
        if header.withdrawals_root.is_some() && !matches!(frame.withdrawals, Material::Complete(_))
        {
            return Err("execution-RPC frame lacks withdrawals committed by its header");
        }
        Ok(())
    }
}

fn validate_execution_header(
    frame: &BlockFrame,
    header: &HeaderEnvelope,
) -> Result<(), &'static str> {
    let encoded = header
        .rlp
        .as_deref()
        .expect("canonical header presence was checked above");
    let mut input = encoded;
    let decoded = ConsensusHeader::decode(&mut input)
        .map_err(|_| "execution-RPC canonical header RLP is invalid")?;
    if !input.is_empty()
        || decoded.hash_slow() != B256::from(*frame.block.hash.as_array())
        || decoded.number != frame.block.number.0
        || decoded.parent_hash != B256::from(*frame.block.parent_hash.as_array())
        || decoded.timestamp != frame.block.timestamp
        || decoded.ommers_hash != EMPTY_OMMER_ROOT_HASH
        || header
            .transactions_root
            .map(|hash| B256::from(*hash.as_array()))
            != Some(decoded.transactions_root)
        || header
            .receipts_root
            .map(|hash| B256::from(*hash.as_array()))
            != Some(decoded.receipts_root)
        || header
            .withdrawals_root
            .map(|hash| B256::from(*hash.as_array()))
            != decoded.withdrawals_root
        || header.gas_limit != Some(decoded.gas_limit)
        || header.gas_used != Some(decoded.gas_used)
        || header
            .base_fee_per_gas
            .map(|value| U256::from_be_bytes(*value.as_array()))
            != decoded.base_fee_per_gas.map(U256::from)
        || header.blob_gas_used != decoded.blob_gas_used
        || header.excess_blob_gas != decoded.excess_blob_gas
    {
        return Err("execution-RPC canonical header conflicts with normalized fields");
    }
    Ok(())
}

fn validate_execution_transactions(
    transactions: &[TransactionEnvelope],
    receipts: &[ReceiptEnvelope],
) -> Result<(), &'static str> {
    for (index, (transaction, receipt)) in transactions.iter().zip(receipts).enumerate() {
        let Ok(index) = u32::try_from(index) else {
            return Err("execution-RPC transaction index exceeds u32");
        };
        if transaction.index != index
            || transaction.encoded.is_none()
            || transaction.from.is_none()
            || transaction.nonce.is_none()
            || receipt.transaction_hash != transaction.hash
            || receipt.transaction_index != index
            || receipt.encoded.is_none()
            || receipt.gas_used.is_none()
            || receipt.effective_gas_price.is_none()
        {
            return Err("execution-RPC transaction or receipt material is incomplete");
        }
        let encoded = transaction
            .encoded
            .as_deref()
            .expect("canonical transaction presence was checked above");
        let decoded = TxEnvelope::decode_2718_exact(encoded)
            .map_err(|_| "execution-RPC canonical transaction is invalid")?;
        if *decoded.tx_hash() != B256::from(*transaction.hash.as_array())
            || decoded
                .recover_signer()
                .map_err(|_| "execution-RPC transaction signer recovery failed")?
                != alloy_primitives::Address::from(
                    transaction
                        .from
                        .expect("transaction sender presence was checked above"),
                )
        {
            return Err("execution-RPC canonical transaction conflicts with normalized fields");
        }
        ConsensusReceiptEnvelope::decode_2718_exact(
            receipt
                .encoded
                .as_deref()
                .expect("canonical receipt presence was checked above"),
        )
        .map_err(|_| "execution-RPC canonical receipt is invalid")?;
    }
    Ok(())
}

/// Durable lifecycle state for resumable raw acquisition.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RawHistoryJobState {
    Queued,
    Running,
    StorageBackpressured,
    Complete,
    Cancelled,
    Failed,
}

impl RawHistoryJobState {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::StorageBackpressured => "storage_backpressured",
            Self::Complete => "complete",
            Self::Cancelled => "cancelled",
            Self::Failed => "failed",
        }
    }

    fn parse(value: &str) -> Result<Self, HistoryStoreError> {
        match value {
            "queued" => Ok(Self::Queued),
            "running" => Ok(Self::Running),
            "storage_backpressured" => Ok(Self::StorageBackpressured),
            "complete" => Ok(Self::Complete),
            "cancelled" => Ok(Self::Cancelled),
            "failed" => Ok(Self::Failed),
            other => Err(HistoryStoreError::CatalogIntegrity(format!(
                "unknown raw-history job state `{other}`"
            ))),
        }
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Complete | Self::Cancelled | Self::Failed)
    }
}

/// Inspectable durable raw-history job and derived committed progress.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RawHistoryJob {
    pub id: RawHistoryJobId,
    pub identity: [u8; 32],
    pub spec: RawHistoryJobSpec,
    pub state: RawHistoryJobState,
    pub attempts: u64,
    pub committed_segments: u64,
    pub committed_logical_bytes: u64,
    pub committed_physical_bytes: u64,
    pub remaining_ranges: Vec<BlockRange>,
    pub last_error: Option<String>,
    pub created_at_unix_ms: u64,
    pub updated_at_unix_ms: u64,
}

/// Metadata released by explicit terminal job deletion.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RawHistoryJobDeletion {
    pub jobs: u64,
    pub ranges: u64,
    pub owners: u64,
    pub released_segments: Vec<String>,
}

impl HistoryStore {
    /// Create or idempotently resolve a normalized raw-history job.
    ///
    /// # Errors
    ///
    /// Returns a conflict when the same ID names another immutable spec.
    pub async fn create_raw_history_job(
        &self,
        id: RawHistoryJobId,
        spec: RawHistoryJobSpec,
    ) -> Result<RawHistoryJob, HistoryStoreError> {
        let _guard = self.inner.lifecycle.lock().await;
        let spec = spec.normalize()?;
        let encoded = postcard::to_allocvec(&spec)
            .map_err(|error| HistoryStoreError::InvalidJob(error.to_string()))?;
        let identity = *blake3::hash(&encoded).as_bytes();
        if let Some(existing) = self.raw_history_job(&id).await? {
            if existing.identity == identity {
                return Ok(existing);
            }
            return Err(HistoryStoreError::JobConflict(existing.id.0));
        }
        if let Some(existing_id) = sqlx::query_scalar::<_, String>(
            "SELECT job_id FROM raw_history_jobs WHERE identity = ?",
        )
        .bind(identity.as_slice())
        .fetch_optional(&self.inner.pool)
        .await?
        {
            return self
                .raw_history_job(&RawHistoryJobId::new(existing_id)?)
                .await?
                .ok_or_else(|| {
                    HistoryStoreError::CatalogIntegrity(
                        "idempotent raw-history job disappeared".to_owned(),
                    )
                });
        }
        let now = unix_ms()?;
        let mut transaction = self.inner.pool.begin().await?;
        sqlx::query(
            "INSERT INTO raw_history_jobs(
                job_id, identity, spec, state, created_at_unix_ms, updated_at_unix_ms
             ) VALUES (?, ?, ?, 'queued', ?, ?)",
        )
        .bind(id.as_str())
        .bind(identity.as_slice())
        .bind(&encoded)
        .bind(u64_i64(now, "job creation time")?)
        .bind(u64_i64(now, "job update time")?)
        .execute(&mut *transaction)
        .await?;
        for (ordinal, range) in spec.ranges.iter().enumerate() {
            sqlx::query(
                "INSERT INTO raw_history_job_ranges(
                    job_id, ordinal, start_block, end_block
                 ) VALUES (?, ?, ?, ?)",
            )
            .bind(id.as_str())
            .bind(u64_i64(
                u64::try_from(ordinal).map_err(|_| HistoryStoreError::ArithmeticOverflow)?,
                "range ordinal",
            )?)
            .bind(u64_i64(range.start().0, "range start")?)
            .bind(u64_i64(range.end().0, "range end")?)
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        self.raw_history_job(&id)
            .await?
            .ok_or_else(|| HistoryStoreError::UnknownJob(id.0))
    }

    /// Inspect one raw-history job.
    ///
    /// # Errors
    ///
    /// Returns an error if durable identity or progress metadata is corrupt.
    pub async fn raw_history_job(
        &self,
        id: &RawHistoryJobId,
    ) -> Result<Option<RawHistoryJob>, HistoryStoreError> {
        let row = sqlx::query("SELECT * FROM raw_history_jobs WHERE job_id = ?")
            .bind(id.as_str())
            .fetch_optional(&self.inner.pool)
            .await?;
        match row {
            Some(row) => Ok(Some(job_from_row(self, &row).await?)),
            None => Ok(None),
        }
    }

    /// List jobs newest first.
    ///
    /// # Errors
    ///
    /// Returns an error if durable identity or progress metadata is corrupt.
    pub async fn raw_history_jobs(&self) -> Result<Vec<RawHistoryJob>, HistoryStoreError> {
        let rows =
            sqlx::query("SELECT * FROM raw_history_jobs ORDER BY created_at_unix_ms DESC, job_id")
                .fetch_all(&self.inner.pool)
                .await?;
        let mut jobs = Vec::with_capacity(rows.len());
        for row in rows {
            jobs.push(job_from_row(self, &row).await?);
        }
        Ok(jobs)
    }

    /// Mark queued or resumable work running and increment its attempt count.
    ///
    /// # Errors
    ///
    /// Terminal and unknown jobs cannot be started.
    pub async fn start_raw_history_job(
        &self,
        id: &RawHistoryJobId,
    ) -> Result<RawHistoryJob, HistoryStoreError> {
        transition_running(self, id).await?;
        required_job(self, id).await
    }

    /// Persist storage backpressure without discarding resumable coverage.
    ///
    /// # Errors
    ///
    /// Terminal and unknown jobs cannot be backpressured.
    pub async fn backpressure_raw_history_job(
        &self,
        id: &RawHistoryJobId,
        error: &str,
    ) -> Result<RawHistoryJob, HistoryStoreError> {
        transition_nonterminal(
            self,
            id,
            RawHistoryJobState::StorageBackpressured,
            Some(error),
        )
        .await?;
        required_job(self, id).await
    }

    /// Mark a non-terminal job failed.
    ///
    /// # Errors
    ///
    /// Terminal and unknown jobs cannot be failed again.
    pub async fn fail_raw_history_job(
        &self,
        id: &RawHistoryJobId,
        error: &str,
    ) -> Result<RawHistoryJob, HistoryStoreError> {
        transition_nonterminal(self, id, RawHistoryJobState::Failed, Some(error)).await?;
        required_job(self, id).await
    }

    /// Cancel work without deleting its retained segments.
    ///
    /// # Errors
    ///
    /// Unknown jobs fail; terminal cancellation is idempotent.
    pub async fn cancel_raw_history_job(
        &self,
        id: &RawHistoryJobId,
    ) -> Result<RawHistoryJob, HistoryStoreError> {
        let now = unix_ms()?;
        let affected = sqlx::query(
            "UPDATE raw_history_jobs
             SET state = 'cancelled', updated_at_unix_ms = ?
             WHERE job_id = ? AND state IN ('queued', 'running', 'storage_backpressured')",
        )
        .bind(u64_i64(now, "job update time")?)
        .bind(id.as_str())
        .execute(&self.inner.pool)
        .await?
        .rows_affected();
        if affected == 0 {
            return required_job(self, id).await;
        }
        required_job(self, id).await
    }

    /// Delete explicit terminal job metadata and release, but do not delete,
    /// its raw segments.
    ///
    /// # Errors
    ///
    /// Active jobs must first be cancelled.
    pub async fn delete_raw_history_job(
        &self,
        id: &RawHistoryJobId,
    ) -> Result<RawHistoryJobDeletion, HistoryStoreError> {
        let _guard = self.inner.lifecycle.lock().await;
        let job = required_job(self, id).await?;
        if !job.state.is_terminal() {
            return Err(HistoryStoreError::JobState {
                id: id.0.clone(),
                state: job.state.as_str().to_owned(),
            });
        }
        let mut transaction = self.inner.pool.begin().await?;
        let segment_rows = sqlx::query(
            "SELECT segment_id FROM raw_segment_owners
             WHERE owner_kind = 'raw_history_job' AND owner_id = ?
             ORDER BY segment_id",
        )
        .bind(id.as_str())
        .fetch_all(&mut *transaction)
        .await?;
        let released_segments = segment_rows
            .iter()
            .map(|row| row.try_get("segment_id"))
            .collect::<Result<Vec<String>, _>>()?;
        let owners = sqlx::query(
            "DELETE FROM raw_segment_owners
             WHERE owner_kind = 'raw_history_job' AND owner_id = ?",
        )
        .bind(id.as_str())
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        let ranges = sqlx::query("DELETE FROM raw_history_job_ranges WHERE job_id = ?")
            .bind(id.as_str())
            .execute(&mut *transaction)
            .await?
            .rows_affected();
        let jobs = sqlx::query("DELETE FROM raw_history_jobs WHERE job_id = ?")
            .bind(id.as_str())
            .execute(&mut *transaction)
            .await?
            .rows_affected();
        transaction.commit().await?;
        Ok(RawHistoryJobDeletion {
            jobs,
            ranges,
            owners,
            released_segments,
        })
    }

    /// Recompute counters and completion from owned closed-segment coverage.
    ///
    /// # Errors
    ///
    /// Returns an error for unknown jobs or corrupt owned coverage.
    pub async fn reconcile_raw_history_job(
        &self,
        id: &RawHistoryJobId,
    ) -> Result<RawHistoryJob, HistoryStoreError> {
        let now = unix_ms()?;
        let mut transaction = self.inner.pool.begin().await?;
        refresh_job_progress_tx(&mut transaction, id, now).await?;
        transaction.commit().await?;
        required_job(self, id).await
    }

    /// Attach already-retained compatible segments to a non-terminal job.
    ///
    /// This is the durable reuse path: overlapping raw work never copies or
    /// reacquires material that the catalog can already prove compatible.
    ///
    /// # Errors
    ///
    /// Returns an error for unknown/terminal jobs or corrupt catalog state.
    pub async fn claim_compatible_segments_for_raw_history_job(
        &self,
        id: &RawHistoryJobId,
    ) -> Result<u64, HistoryStoreError> {
        let job = required_job(self, id).await?;
        if job.state.is_terminal() {
            return Err(HistoryStoreError::JobState {
                id: id.as_str().to_owned(),
                state: job.state.as_str().to_owned(),
            });
        }
        let mut claimed = 0_u64;
        for record in self.segments().await? {
            let descriptor = &record.metadata.descriptor;
            if descriptor.chain_id == job.spec.chain_id
                && descriptor.material_shape == job.spec.material.shape_id()
                && descriptor
                    .complete_capabilities
                    .with_derivable()
                    .contains_all(job.spec.required_capabilities)
                && descriptor.verification >= job.spec.verification
                && descriptor.trust >= job.spec.minimum_trust
                && record.profile.satisfies(job.spec.profile)
                && record.indexes.contains(job.spec.indexes)
                && job
                    .spec
                    .ranges
                    .iter()
                    .any(|range| ranges_overlap(*range, descriptor.range))
                && !self.owners(&record.metadata.id).await?.iter().any(|owner| {
                    owner.kind == SegmentOwnerKind::RawHistoryJob && owner.owner_id == id.as_str()
                })
            {
                self.add_owner(
                    &record.metadata.id,
                    SegmentOwnerKind::RawHistoryJob,
                    id.as_str(),
                )
                .await?;
                claimed = claimed
                    .checked_add(1)
                    .ok_or(HistoryStoreError::ArithmeticOverflow)?;
            }
        }
        Ok(claimed)
    }
}

pub(crate) async fn validate_initial_owner_claims(
    store: &HistoryStore,
    descriptor: &SegmentDescriptor,
    owners: &BTreeSet<SegmentOwnerClaim>,
) -> Result<(), HistoryStoreError> {
    for owner in owners {
        if owner.kind != SegmentOwnerKind::RawHistoryJob {
            continue;
        }
        let id = RawHistoryJobId::new(owner.owner_id.clone())?;
        let job = required_job(store, &id).await?;
        if job.state.is_terminal()
            || descriptor.chain_id != job.spec.chain_id
            || descriptor.material_shape != job.spec.material.shape_id()
            || !descriptor
                .complete_capabilities
                .with_derivable()
                .contains_all(job.spec.required_capabilities)
            || descriptor.verification < job.spec.verification
            || descriptor.trust < job.spec.minimum_trust
            || !job
                .spec
                .ranges
                .iter()
                .any(|range| ranges_overlap(*range, descriptor.range))
        {
            return Err(HistoryStoreError::InvalidJob(format!(
                "segment is incompatible with raw-history job `{}`",
                id.as_str()
            )));
        }
    }
    Ok(())
}

pub(crate) async fn refresh_job_progress_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    id: &RawHistoryJobId,
    now: u64,
) -> Result<(), HistoryStoreError> {
    let row = sqlx::query("SELECT spec, state FROM raw_history_jobs WHERE job_id = ?")
        .bind(id.as_str())
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or_else(|| HistoryStoreError::UnknownJob(id.0.clone()))?;
    let encoded_spec: Vec<u8> = row.try_get("spec")?;
    let spec = decode_spec(&encoded_spec)?;
    let state = RawHistoryJobState::parse(row.try_get("state")?)?;
    let segment_rows = sqlx::query(
        "SELECT s.start_block, s.end_block, s.logical_bytes, s.physical_bytes
         FROM raw_segments s
         JOIN raw_segment_owners o ON o.segment_id = s.segment_id
         WHERE o.owner_kind = 'raw_history_job' AND o.owner_id = ? AND s.state = 'closed'
         ORDER BY s.start_block, s.end_block",
    )
    .bind(id.as_str())
    .fetch_all(&mut **transaction)
    .await?;
    let mut covered = Vec::with_capacity(segment_rows.len());
    let mut logical = 0_u64;
    let mut physical = 0_u64;
    for row in &segment_rows {
        covered.push(
            BlockRange::new(
                BlockNumber(i64_u64(row.try_get("start_block")?, "segment start")?),
                BlockNumber(i64_u64(row.try_get("end_block")?, "segment end")?),
            )
            .map_err(|error| HistoryStoreError::CatalogIntegrity(error.to_string()))?,
        );
        logical = logical
            .checked_add(i64_u64(row.try_get("logical_bytes")?, "logical bytes")?)
            .ok_or(HistoryStoreError::ArithmeticOverflow)?;
        physical = physical
            .checked_add(i64_u64(row.try_get("physical_bytes")?, "physical bytes")?)
            .ok_or(HistoryStoreError::ArithmeticOverflow)?;
    }
    let next_state = if requested_gaps(&spec.ranges, &covered).is_empty()
        && !matches!(
            state,
            RawHistoryJobState::Cancelled | RawHistoryJobState::Failed
        ) {
        RawHistoryJobState::Complete
    } else {
        state
    };
    sqlx::query(
        "UPDATE raw_history_jobs
         SET state = ?, committed_segments = ?, committed_logical_bytes = ?,
             committed_physical_bytes = ?, updated_at_unix_ms = ?
         WHERE job_id = ?",
    )
    .bind(next_state.as_str())
    .bind(u64_i64(
        u64::try_from(segment_rows.len()).map_err(|_| HistoryStoreError::ArithmeticOverflow)?,
        "committed segment count",
    )?)
    .bind(u64_i64(logical, "committed logical bytes")?)
    .bind(u64_i64(physical, "committed physical bytes")?)
    .bind(u64_i64(now, "job update time")?)
    .bind(id.as_str())
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

pub(crate) async fn reconcile_all_jobs(pool: &sqlx::SqlitePool) -> Result<(), HistoryStoreError> {
    let rows = sqlx::query("SELECT job_id FROM raw_history_jobs ORDER BY job_id")
        .fetch_all(pool)
        .await?;
    for row in rows {
        let id = RawHistoryJobId::new(row.try_get::<String, _>("job_id")?)?;
        let mut transaction = pool.begin().await?;
        refresh_job_progress_tx(&mut transaction, &id, unix_ms()?).await?;
        transaction.commit().await?;
    }
    Ok(())
}

async fn transition_running(
    store: &HistoryStore,
    id: &RawHistoryJobId,
) -> Result<(), HistoryStoreError> {
    let now = unix_ms()?;
    let result = sqlx::query(
        "UPDATE raw_history_jobs
         SET state = 'running', attempts = attempts + 1, last_error = NULL,
             updated_at_unix_ms = ?
         WHERE job_id = ? AND state IN ('queued', 'running', 'storage_backpressured')",
    )
    .bind(u64_i64(now, "job update time")?)
    .bind(id.as_str())
    .execute(&store.inner.pool)
    .await?;
    if result.rows_affected() == 0 {
        let job = required_job(store, id).await?;
        return Err(HistoryStoreError::JobState {
            id: id.0.clone(),
            state: job.state.as_str().to_owned(),
        });
    }
    Ok(())
}

async fn transition_nonterminal(
    store: &HistoryStore,
    id: &RawHistoryJobId,
    state: RawHistoryJobState,
    error: Option<&str>,
) -> Result<(), HistoryStoreError> {
    let now = unix_ms()?;
    let result = sqlx::query(
        "UPDATE raw_history_jobs SET state = ?, last_error = ?, updated_at_unix_ms = ?
         WHERE job_id = ? AND state IN ('queued', 'running', 'storage_backpressured')",
    )
    .bind(state.as_str())
    .bind(error)
    .bind(u64_i64(now, "job update time")?)
    .bind(id.as_str())
    .execute(&store.inner.pool)
    .await?;
    if result.rows_affected() == 0 {
        let job = required_job(store, id).await?;
        return Err(HistoryStoreError::JobState {
            id: id.0.clone(),
            state: job.state.as_str().to_owned(),
        });
    }
    Ok(())
}

async fn required_job(
    store: &HistoryStore,
    id: &RawHistoryJobId,
) -> Result<RawHistoryJob, HistoryStoreError> {
    store
        .raw_history_job(id)
        .await?
        .ok_or_else(|| HistoryStoreError::UnknownJob(id.0.clone()))
}

async fn job_from_row(
    store: &HistoryStore,
    row: &sqlx::sqlite::SqliteRow,
) -> Result<RawHistoryJob, HistoryStoreError> {
    let id = RawHistoryJobId::new(row.try_get::<String, _>("job_id")?)?;
    let identity = blob32(row.try_get("identity")?, "job identity")?;
    let encoded_spec: Vec<u8> = row.try_get("spec")?;
    let spec = decode_spec(&encoded_spec)?;
    let actual_identity = *blake3::hash(
        &postcard::to_allocvec(&spec)
            .map_err(|error| HistoryStoreError::CatalogIntegrity(error.to_string()))?,
    )
    .as_bytes();
    if identity != actual_identity {
        return Err(HistoryStoreError::CatalogIntegrity(format!(
            "raw-history job `{}` identity mismatch",
            id.as_str()
        )));
    }
    let covered_rows = sqlx::query(
        "SELECT s.start_block, s.end_block
         FROM raw_segments s
         JOIN raw_segment_owners o ON o.segment_id = s.segment_id
         WHERE o.owner_kind = 'raw_history_job' AND o.owner_id = ? AND s.state = 'closed'",
    )
    .bind(id.as_str())
    .fetch_all(&store.inner.pool)
    .await?;
    let covered = covered_rows
        .iter()
        .map(|row| {
            BlockRange::new(
                BlockNumber(i64_u64(row.try_get("start_block")?, "segment start")?),
                BlockNumber(i64_u64(row.try_get("end_block")?, "segment end")?),
            )
            .map_err(|error| HistoryStoreError::CatalogIntegrity(error.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(RawHistoryJob {
        id,
        identity,
        remaining_ranges: requested_gaps(&spec.ranges, &covered),
        spec,
        state: RawHistoryJobState::parse(row.try_get("state")?)?,
        attempts: i64_u64(row.try_get("attempts")?, "job attempts")?,
        committed_segments: i64_u64(row.try_get("committed_segments")?, "committed segments")?,
        committed_logical_bytes: i64_u64(
            row.try_get("committed_logical_bytes")?,
            "committed logical bytes",
        )?,
        committed_physical_bytes: i64_u64(
            row.try_get("committed_physical_bytes")?,
            "committed physical bytes",
        )?,
        last_error: row.try_get("last_error")?,
        created_at_unix_ms: i64_u64(row.try_get("created_at_unix_ms")?, "job creation time")?,
        updated_at_unix_ms: i64_u64(row.try_get("updated_at_unix_ms")?, "job update time")?,
    })
}

fn decode_spec(encoded: &[u8]) -> Result<RawHistoryJobSpec, HistoryStoreError> {
    postcard::from_bytes::<RawHistoryJobSpec>(encoded)
        .map_err(|error| HistoryStoreError::CatalogIntegrity(error.to_string()))?
        .normalize()
        .map_err(|error| HistoryStoreError::CatalogIntegrity(error.to_string()))
}

fn normalize_ranges(mut ranges: Vec<BlockRange>) -> Result<Vec<BlockRange>, HistoryStoreError> {
    if ranges.is_empty() || ranges.len() > 1_024 {
        return Err(HistoryStoreError::InvalidJob(
            "raw-history job requires 1..=1024 ranges".to_owned(),
        ));
    }
    ranges.sort_by_key(|range| (range.start().0, range.end().0));
    let mut normalized: Vec<BlockRange> = Vec::with_capacity(ranges.len());
    for range in ranges {
        if let Some(last) = normalized.last_mut()
            && range.start().0 <= last.end().0.saturating_add(1)
        {
            *last = BlockRange::new(last.start(), BlockNumber(last.end().0.max(range.end().0)))
                .map_err(|error| HistoryStoreError::InvalidJob(error.to_string()))?;
        } else {
            normalized.push(range);
        }
    }
    Ok(normalized)
}

fn normalize_strings(values: &mut Vec<String>) {
    values.sort();
    values.dedup();
}

fn requested_gaps(requested: &[BlockRange], covered: &[BlockRange]) -> Vec<BlockRange> {
    requested
        .iter()
        .flat_map(|range| range_gaps(*range, covered))
        .collect()
}

fn range_gaps(requested: BlockRange, covered: &[BlockRange]) -> Vec<BlockRange> {
    let mut relevant = covered
        .iter()
        .copied()
        .filter(|range| {
            range.end().0 >= requested.start().0 && range.start().0 <= requested.end().0
        })
        .collect::<Vec<_>>();
    relevant.sort_by_key(|range| range.start().0);
    let mut gaps = Vec::new();
    let mut next = requested.start().0;
    for range in relevant {
        let start = range.start().0.max(requested.start().0);
        let end = range.end().0.min(requested.end().0);
        if start > next {
            gaps.push(
                BlockRange::new(BlockNumber(next), BlockNumber(start - 1))
                    .expect("ordered gap bounds"),
            );
        }
        next = next.max(end.saturating_add(1));
        if next > requested.end().0 {
            return gaps;
        }
    }
    if next <= requested.end().0 {
        gaps.push(
            BlockRange::new(BlockNumber(next), requested.end()).expect("ordered trailing gap"),
        );
    }
    gaps
}

const fn ranges_overlap(left: BlockRange, right: BlockRange) -> bool {
    left.start().0 <= right.end().0 && right.start().0 <= left.end().0
}

#[cfg(test)]
mod tests {
    use leani_primitives::{BlockFrame, BlockHash, Finality, HeaderEnvelope, Quantity};
    use leani_testkit::fixture_frame;
    use tempfile::tempdir;

    use super::*;
    use crate::{HistoryStoreConfig, SegmentId, SegmentReservation, StorageBudget};

    fn config(root: &std::path::Path) -> HistoryStoreConfig {
        HistoryStoreConfig::new(root).with_budget(StorageBudget {
            maximum_logical_bytes: 32 * 1024 * 1024,
            maximum_physical_bytes: 32 * 1024 * 1024,
            maximum_frame_logical_bytes: 1024 * 1024,
            maximum_segment_logical_bytes: 4 * 1024 * 1024,
            maximum_segment_physical_bytes: 4 * 1024 * 1024,
        })
    }

    fn frames(start: u64, end: u64) -> Vec<BlockFrame> {
        let mut parent = BlockHash::new([0x61; 32]);
        (start..=end)
            .map(|number| {
                let frame = fixture_frame(number, parent);
                parent = frame.block.hash;
                frame
            })
            .collect()
    }

    fn execution_rpc_frame(number: BlockNumber) -> BlockFrame {
        let header = ConsensusHeader {
            number: number.0,
            timestamp: 1_700_000_000 + number.0,
            base_fee_per_gas: Some(1),
            ..Default::default()
        };
        let mut frame = fixture_frame(number.0, BlockHash::new(header.parent_hash.0));
        frame.block.hash = BlockHash::new(header.hash_slow().0);
        frame.header = Material::Complete(HeaderEnvelope {
            rlp: Some(alloy_rlp::encode(&header)),
            transactions_root: Some(BlockHash::new(header.transactions_root.0)),
            receipts_root: Some(BlockHash::new(header.receipts_root.0)),
            withdrawals_root: header.withdrawals_root.map(|hash| BlockHash::new(hash.0)),
            gas_limit: Some(header.gas_limit),
            gas_used: Some(header.gas_used),
            base_fee_per_gas: header
                .base_fee_per_gas
                .map(U256::from)
                .map(|value| Quantity::new(value.to_be_bytes())),
            blob_gas_used: header.blob_gas_used,
            excess_blob_gas: header.excess_blob_gas,
            size_bytes: Some(u64::try_from(alloy_rlp::encode(&header).len()).unwrap_or(u64::MAX)),
            transaction_count: Some(0),
            consensus_size_bytes: None,
        });
        frame
    }

    fn spec(ranges: Vec<BlockRange>) -> RawHistoryJobSpec {
        RawHistoryJobSpec {
            chain_id: ChainId(1),
            ranges,
            profile: RawHistoryProfile::ProcessorReuse,
            material: RawHistoryMaterialProfile::default(),
            required_capabilities: CapabilitySet::of(Capability::Transactions),
            verification: VerificationClass::TrustedDataset,
            minimum_trust: TrustModel::TrustedDataset,
            source_policy_digest: [0x99; 32],
            retention: RawHistoryRetention::Full,
            segment: RawHistorySegmentPolicy {
                target_blocks: 3,
                maximum_logical_bytes: 1024 * 1024,
                maximum_physical_bytes: 1024 * 1024,
                compression: Compression::Snappy,
                on_limit: StorageLimitAction::Pause,
            },
            indexes: RawHistoryIndexPolicy::default(),
        }
    }

    #[test]
    fn raw_material_identity_includes_optional_log_fields() {
        let complete = RawHistoryMaterialProfile::default();
        let mut slim = complete.clone();
        slim.log_fields = LogFieldSet::NONE;
        assert_ne!(complete.shape_id(), slim.shape_id());
        assert_eq!(complete, RawHistoryMaterialProfile::default());
    }

    async fn commit_range(
        store: &HistoryStore,
        job: &RawHistoryJobId,
        id: &str,
        frames: &[BlockFrame],
    ) {
        let capabilities = frames[0].capabilities();
        let mut pending = store
            .begin_segment(
                SegmentId::new(id).expect("segment ID"),
                SegmentDescriptor {
                    chain_id: ChainId(1),
                    range: BlockRange::new(
                        frames[0].block.number,
                        frames.last().expect("frames").block.number,
                    )
                    .expect("range"),
                    material_shape: MaterialShapeId::COMPLETE_EXECUTION,
                    present_capabilities: capabilities.present,
                    complete_capabilities: capabilities.complete,
                    verification: VerificationClass::TrustedDataset,
                    trust: TrustModel::TrustedDataset,
                },
                Compression::Snappy,
                SegmentReservation::new(1024 * 1024, 1024 * 1024),
            )
            .await
            .expect("begin segment");
        for frame in frames {
            assert_eq!(frame.finality, Finality::Finalized);
            pending.append(frame).expect("append frame");
        }
        pending
            .commit(&[SegmentOwnerClaim {
                kind: SegmentOwnerKind::RawHistoryJob,
                owner_id: job.as_str().to_owned(),
            }])
            .await
            .expect("commit job segment");
    }

    #[tokio::test]
    async fn creation_normalizes_ranges_and_is_idempotent_by_identity() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(config(directory.path()))
            .await
            .expect("store");
        let requested = spec(vec![
            BlockRange::new(BlockNumber(10), BlockNumber(20)).expect("range"),
            BlockRange::new(BlockNumber(1), BlockNumber(5)).expect("range"),
            BlockRange::new(BlockNumber(6), BlockNumber(9)).expect("range"),
        ]);
        let first_id = RawHistoryJobId::new("normalized-job").expect("ID");
        let first = store
            .create_raw_history_job(first_id.clone(), requested.clone())
            .await
            .expect("create");
        assert_eq!(
            first.spec.ranges,
            vec![BlockRange::new(BlockNumber(1), BlockNumber(20)).expect("range")]
        );
        assert_eq!(first.remaining_ranges, first.spec.ranges);
        assert_eq!(first.state, RawHistoryJobState::Queued);

        let same = store
            .create_raw_history_job(first_id.clone(), requested.clone())
            .await
            .expect("same ID and identity");
        assert_eq!(same.identity, first.identity);
        let alias = store
            .create_raw_history_job(
                RawHistoryJobId::new("idempotent-alias").expect("ID"),
                requested,
            )
            .await
            .expect("same identity");
        assert_eq!(alias.id, first_id);
        assert_eq!(store.raw_history_jobs().await.expect("jobs").len(), 1);

        let mut conflicting = first.spec.clone();
        conflicting.source_policy_digest = [0x01; 32];
        assert!(matches!(
            store
                .create_raw_history_job(first.id.clone(), conflicting)
                .await,
            Err(HistoryStoreError::JobConflict(_))
        ));
    }

    #[tokio::test]
    async fn post_merge_execution_profile_rejects_pre_merge_ranges() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(config(directory.path()))
            .await
            .expect("store");
        let mut requested = spec(vec![
            BlockRange::new(BlockNumber(15_537_393), BlockNumber(15_537_395)).expect("range"),
        ]);
        requested.profile = RawHistoryProfile::PostMergeExecutionRpc {
            merge_block: BlockNumber(15_537_394),
        };
        requested.required_capabilities = CapabilitySet::from_iter([
            Capability::Header,
            Capability::Transactions,
            Capability::Receipts,
        ]);
        let error = store
            .create_raw_history_job(
                RawHistoryJobId::new("pre-merge-rpc").expect("ID"),
                requested,
            )
            .await
            .expect_err("pre-Merge range must be rejected");
        assert!(error.to_string().contains("starts at Merge block 15537394"));
    }

    #[tokio::test]
    async fn post_merge_execution_profile_accepts_exact_minimum_material() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(config(directory.path()))
            .await
            .expect("store");
        let mut requested = spec(vec![
            BlockRange::new(BlockNumber(15_537_394), BlockNumber(15_537_395)).expect("range"),
        ]);
        requested.profile = RawHistoryProfile::PostMergeExecutionRpc {
            merge_block: BlockNumber(15_537_394),
        };
        requested.required_capabilities = CapabilitySet::from_iter([
            Capability::Header,
            Capability::Transactions,
            Capability::Receipts,
        ]);
        let job = store
            .create_raw_history_job(
                RawHistoryJobId::new("post-merge-rpc").expect("ID"),
                requested,
            )
            .await
            .expect("post-Merge execution profile");
        assert_eq!(job.state, RawHistoryJobState::Queued);
    }

    #[test]
    fn post_merge_execution_profile_requires_rpc_reconstructable_frame_fields() {
        let merge = BlockNumber(15_537_394);
        let mut requested = spec(vec![BlockRange::single(merge)]);
        requested.profile = RawHistoryProfile::PostMergeExecutionRpc { merge_block: merge };
        requested.required_capabilities = CapabilitySet::from_iter([
            Capability::Header,
            Capability::Transactions,
            Capability::Receipts,
        ]);
        let mut frame = execution_rpc_frame(merge);
        assert!(requested.validate_frame_profile(&frame).is_ok());
        let Material::Complete(header) = &mut frame.header else {
            panic!("complete header")
        };
        header.rlp = None;
        assert_eq!(
            requested.validate_frame_profile(&frame),
            Err("execution-RPC frame lacks required canonical header fields")
        );
    }

    #[tokio::test]
    async fn compatible_reuse_requires_the_requested_locator_policy() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(config(directory.path()))
            .await
            .expect("store");
        let expected = frames(80, 81);
        let capabilities = expected[0].capabilities();
        let mut pending = store
            .begin_segment(
                SegmentId::new("unindexed-reuse").expect("segment ID"),
                SegmentDescriptor {
                    chain_id: ChainId(1),
                    range: BlockRange::new(BlockNumber(80), BlockNumber(81)).expect("range"),
                    material_shape: MaterialShapeId::COMPLETE_EXECUTION,
                    present_capabilities: capabilities.present,
                    complete_capabilities: capabilities.complete,
                    verification: VerificationClass::TrustedDataset,
                    trust: TrustModel::TrustedDataset,
                },
                Compression::Snappy,
                SegmentReservation::new(1024 * 1024, 1024 * 1024),
            )
            .await
            .expect("begin segment");
        for frame in &expected {
            pending.append(frame).expect("append frame");
        }
        pending.commit(&[]).await.expect("commit unindexed segment");

        let id = RawHistoryJobId::new("indexed-reuse-job").expect("ID");
        let mut requested = spec(vec![
            BlockRange::new(BlockNumber(80), BlockNumber(81)).expect("range"),
        ]);
        requested.indexes.block_hash = true;
        store
            .create_raw_history_job(id.clone(), requested)
            .await
            .expect("create indexed job");
        assert_eq!(
            store
                .claim_compatible_segments_for_raw_history_job(&id)
                .await
                .expect("claim compatible"),
            0
        );
        assert_eq!(
            store
                .raw_history_job(&id)
                .await
                .expect("job")
                .expect("present")
                .remaining_ranges,
            vec![BlockRange::new(BlockNumber(80), BlockNumber(81)).expect("range")]
        );
    }

    #[tokio::test]
    async fn exact_rpc_job_does_not_claim_processor_reuse_segments() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(config(directory.path()))
            .await
            .expect("store");
        let merge = BlockNumber(15_537_394);
        let frame = execution_rpc_frame(merge);
        let capabilities = frame.capabilities();
        let mut pending = store
            .begin_segment_indexed(
                SegmentId::new("processor-only-rpc-shape").expect("segment ID"),
                SegmentDescriptor {
                    chain_id: ChainId(1),
                    range: BlockRange::single(merge),
                    material_shape: MaterialShapeId::COMPLETE_EXECUTION,
                    present_capabilities: capabilities.present,
                    complete_capabilities: capabilities.complete,
                    verification: VerificationClass::TrustedDataset,
                    trust: TrustModel::TrustedDataset,
                },
                Compression::Snappy,
                SegmentReservation::new(1024 * 1024, 1024 * 1024),
                RawHistoryIndexPolicy {
                    block_hash: true,
                    transaction_hash: true,
                    logs: false,
                },
            )
            .await
            .expect("begin processor segment");
        pending.append(&frame).expect("append frame");
        pending.commit(&[]).await.expect("commit processor segment");

        let id = RawHistoryJobId::new("exact-rpc-reuse-job").expect("ID");
        let mut requested = spec(vec![BlockRange::single(merge)]);
        requested.profile = RawHistoryProfile::PostMergeExecutionRpc { merge_block: merge };
        requested.required_capabilities = CapabilitySet::from_iter([
            Capability::Header,
            Capability::Transactions,
            Capability::Receipts,
        ]);
        requested.indexes = RawHistoryIndexPolicy {
            block_hash: true,
            transaction_hash: true,
            logs: false,
        };
        store
            .create_raw_history_job(id.clone(), requested)
            .await
            .expect("create exact job");
        assert_eq!(
            store
                .claim_compatible_segments_for_raw_history_job(&id)
                .await
                .expect("claim compatible"),
            0
        );
        let mut certified = store
            .begin_segment_profiled(
                SegmentId::new("certified-rpc-shape").expect("segment ID"),
                SegmentDescriptor {
                    chain_id: ChainId(1),
                    range: BlockRange::single(merge),
                    material_shape: MaterialShapeId::COMPLETE_EXECUTION,
                    present_capabilities: capabilities.present,
                    complete_capabilities: capabilities.complete,
                    verification: VerificationClass::TrustedDataset,
                    trust: TrustModel::TrustedDataset,
                },
                Compression::Snappy,
                SegmentReservation::new(1024 * 1024, 1024 * 1024),
                RawHistoryProfile::PostMergeExecutionRpc { merge_block: merge },
                RawHistoryIndexPolicy {
                    block_hash: true,
                    transaction_hash: true,
                    logs: false,
                },
            )
            .await
            .expect("begin certified segment");
        certified.append(&frame).expect("append certified frame");
        certified
            .commit(&[])
            .await
            .expect("commit certified segment");
        assert_eq!(
            store
                .claim_compatible_segments_for_raw_history_job(&id)
                .await
                .expect("claim certified segment"),
            1
        );
    }

    #[tokio::test]
    async fn owned_segment_commits_advance_gaps_and_complete_atomically() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(config(directory.path()))
            .await
            .expect("store");
        let id = RawHistoryJobId::new("coverage-job").expect("ID");
        store
            .create_raw_history_job(
                id.clone(),
                spec(vec![
                    BlockRange::new(BlockNumber(100), BlockNumber(105)).expect("range"),
                ]),
            )
            .await
            .expect("create job");
        let running = store.start_raw_history_job(&id).await.expect("start job");
        assert_eq!(running.attempts, 1);
        Box::pin(commit_range(&store, &id, "coverage-a", &frames(100, 102))).await;
        let partial = store
            .raw_history_job(&id)
            .await
            .expect("job")
            .expect("present");
        assert_eq!(partial.state, RawHistoryJobState::Running);
        assert_eq!(partial.committed_segments, 1);
        assert_eq!(
            partial.remaining_ranges,
            vec![BlockRange::new(BlockNumber(103), BlockNumber(105)).expect("range")]
        );
        Box::pin(commit_range(&store, &id, "coverage-b", &frames(103, 105))).await;
        let complete = store
            .raw_history_job(&id)
            .await
            .expect("job")
            .expect("present");
        assert_eq!(complete.state, RawHistoryJobState::Complete);
        assert!(complete.remaining_ranges.is_empty());
        assert_eq!(complete.committed_segments, 2);
        assert!(complete.committed_logical_bytes > 0);

        let deletion = store
            .delete_raw_history_job(&id)
            .await
            .expect("delete terminal job");
        assert_eq!(deletion.jobs, 1);
        assert_eq!(deletion.ranges, 1);
        assert_eq!(deletion.owners, 2);
        assert_eq!(deletion.released_segments.len(), 2);
        assert_eq!(store.segments().await.expect("segments").len(), 2);
    }

    #[tokio::test]
    async fn restart_reconciles_progress_and_preserves_cancelled_state() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(config(directory.path()))
            .await
            .expect("store");
        let id = RawHistoryJobId::new("restart-job").expect("ID");
        store
            .create_raw_history_job(
                id.clone(),
                spec(vec![
                    BlockRange::new(BlockNumber(200), BlockNumber(202)).expect("range"),
                ]),
            )
            .await
            .expect("create");
        store.start_raw_history_job(&id).await.expect("start");
        Box::pin(commit_range(
            &store,
            &id,
            "restart-coverage",
            &frames(200, 202),
        ))
        .await;
        sqlx::query(
            "UPDATE raw_history_jobs
             SET state = 'running', committed_segments = 0,
                 committed_logical_bytes = 0, committed_physical_bytes = 0
             WHERE job_id = ?",
        )
        .bind(id.as_str())
        .execute(&store.inner.pool)
        .await
        .expect("simulate stale checkpoint");
        store.inner.pool.close().await;
        drop(store);

        let reopened = HistoryStore::open(config(directory.path()))
            .await
            .expect("reopen");
        let reconciled = reopened
            .raw_history_job(&id)
            .await
            .expect("job")
            .expect("present");
        assert_eq!(reconciled.state, RawHistoryJobState::Complete);
        assert_eq!(reconciled.committed_segments, 1);

        let cancelled_id = RawHistoryJobId::new("cancelled-job").expect("ID");
        reopened
            .create_raw_history_job(
                cancelled_id.clone(),
                spec(vec![
                    BlockRange::new(BlockNumber(300), BlockNumber(302)).expect("range"),
                ]),
            )
            .await
            .expect("create cancelled");
        let cancelled = reopened
            .cancel_raw_history_job(&cancelled_id)
            .await
            .expect("cancel");
        assert_eq!(cancelled.state, RawHistoryJobState::Cancelled);
        assert_eq!(
            reopened
                .cancel_raw_history_job(&cancelled_id)
                .await
                .expect("idempotent cancel")
                .state,
            RawHistoryJobState::Cancelled
        );
    }

    #[tokio::test]
    async fn incompatible_raw_job_owner_fails_before_publication_and_releases_capacity() {
        let directory = tempdir().expect("temporary directory");
        let store = HistoryStore::open(config(directory.path()))
            .await
            .expect("store");
        let id = RawHistoryJobId::new("shape-job").expect("ID");
        store
            .create_raw_history_job(
                id.clone(),
                spec(vec![
                    BlockRange::new(BlockNumber(400), BlockNumber(400)).expect("range"),
                ]),
            )
            .await
            .expect("create");
        store.start_raw_history_job(&id).await.expect("start");
        let expected = frames(400, 400);
        let capabilities = expected[0].capabilities();
        let mut pending = store
            .begin_segment(
                SegmentId::new("wrong-shape").expect("ID"),
                SegmentDescriptor {
                    chain_id: ChainId(1),
                    range: BlockRange::single(BlockNumber(400)),
                    material_shape: MaterialShapeId([0xff; 32]),
                    present_capabilities: capabilities.present,
                    complete_capabilities: capabilities.complete,
                    verification: VerificationClass::TrustedDataset,
                    trust: TrustModel::TrustedDataset,
                },
                Compression::None,
                SegmentReservation::new(1024 * 1024, 1024 * 1024),
            )
            .await
            .expect("begin");
        pending.append(&expected[0]).expect("append");
        assert!(matches!(
            pending
                .commit(&[SegmentOwnerClaim {
                    kind: SegmentOwnerKind::RawHistoryJob,
                    owner_id: id.as_str().to_owned(),
                }])
                .await,
            Err(HistoryStoreError::InvalidJob(_))
        ));
        let stats = store.stats().await.expect("stats");
        assert_eq!(stats.reserved_logical_bytes, 0);
        assert_eq!(stats.temporary_physical_bytes, 0);
        assert!(store.segments().await.expect("segments").is_empty());
    }
}
