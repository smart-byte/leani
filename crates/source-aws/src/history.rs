//! The history source: one chunk per published day.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use alloy_primitives::{B256, Bloom, BloomInput, U256};
use async_trait::async_trait;
use futures::{StreamExt, stream};
use leani_chain::{DatasetLayout, PublicDataset};
use leani_primitives::{
    Address, BlockFrame, BlockHash, BlockNumber, BlockRange, BlockRef, Capability, CapabilitySet,
    ChainId, Completeness, FilterScope, Finality, HeaderEnvelope, Log, Material, MissingReason,
    ObjectIdentity, Provenance, Quantity, ReceiptEnvelope, SourceId, SourceKind, TransactionHash,
    TrustModel, VerificationCheck, VerificationReport,
};
use leani_source_api::{
    BlockFrameStream, DataRequest, FinalityModel, HistorySource, Partitioning, SourceBudget,
    SourceChunk, SourceDescriptor, SourceError, SourcePlan, VerificationPolicy,
};
use object_store::{ObjectStore, http::HttpBuilder};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::{
    AWS_PUBLIC_BLOCKCHAIN, AwsError,
    catalog::{Catalog, Listing, S3Listing},
    layout::Table,
    read::{self, BlockRow, InputBudget, LogRow, ReceiptRow, TableRead},
    verify,
};

const SCHEMA_VERSION: &str = "aws-public.v1";
const IDENTITY_VERSION: u8 = 1;

/// Header and complete receipts, with their logs and outcomes.
const CAPABILITIES: CapabilitySet =
    CapabilitySet::of(Capability::Header).with(Capability::Receipts);

#[derive(Clone, Debug)]
pub struct AwsPublicConfig {
    pub id: String,
    pub chain_id: u64,
    pub dataset: PublicDataset,
    /// The bucket's virtual-hosted URL, or a mirror with the same layout.
    pub bucket: Url,
    pub priority: u16,
    /// Parquet rows decoded per Arrow batch.
    pub batch_rows: usize,
    /// Most blocks one chunk holds in memory. Each chunk reads its whole
    /// day again, so splitting a day trades downloads for memory.
    pub chunk_blocks: Option<u64>,
}

impl AwsPublicConfig {
    /// The chain's dataset on AWS Open Data, or `None` when the chain has
    /// none.
    #[must_use]
    pub fn for_chain(id: &str, chain: &leani_chain::Chain) -> Option<Self> {
        Some(Self {
            id: id.to_owned(),
            chain_id: chain.chain_id,
            dataset: chain.public_dataset.clone()?,
            bucket: Url::parse(AWS_PUBLIC_BLOCKCHAIN).ok()?,
            priority: 30,
            batch_rows: 8_192,
            chunk_blocks: None,
        })
    }
}

#[derive(Clone, Debug)]
pub struct AwsPublicSource {
    descriptor: SourceDescriptor,
    layout: DatasetLayout,
    prefix: String,
    catalog: Arc<Catalog>,
    batch_rows: usize,
    chunk_blocks: u64,
}

impl AwsPublicSource {
    /// Read the dataset anonymously over HTTPS.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid id or bucket URL.
    pub fn new(config: &AwsPublicConfig) -> Result<Self, AwsError> {
        let store = HttpBuilder::new()
            .with_url(config.bucket.as_str().trim_end_matches('/'))
            .build()
            .map_err(|error| AwsError::Store(error.to_string()))?;
        let listing = S3Listing::new(config.bucket.clone())?;
        Self::with_store(config, Arc::new(store), Arc::new(listing))
    }

    /// Read the dataset from `store`, listed by `listing`.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid id.
    pub fn with_store(
        config: &AwsPublicConfig,
        store: Arc<dyn ObjectStore>,
        listing: Arc<dyn Listing>,
    ) -> Result<Self, AwsError> {
        let id = SourceId::new(&config.id).map_err(|error| AwsError::Store(error.to_string()))?;
        let range = BlockRange::new(
            BlockNumber(config.dataset.first_block),
            BlockNumber(u64::MAX),
        )
        .map_err(|error| AwsError::Store(error.to_string()))?;
        let layout = config.dataset.layout;
        Ok(Self {
            descriptor: SourceDescriptor {
                id,
                kind: SourceKind::PublicDataset,
                chain_id: ChainId(config.chain_id),
                range: Some(range),
                capabilities: CAPABILITIES,
                complete_capabilities: CAPABILITIES,
                trust: TrustModel::TrustedDataset,
                finality: FinalityModel::Finalized,
                partitioning: Partitioning::SourceDefined("utc-day".to_owned()),
                expected_lag: match layout {
                    DatasetLayout::AwsEthereumV1 => Duration::from_hours(24),
                    DatasetLayout::SonarxEvmV1 => Duration::from_hours(24 * 8),
                },
                schema_version: SCHEMA_VERSION.to_owned(),
                priority: config.priority,
            },
            layout,
            prefix: config.dataset.prefix.trim_matches('/').to_owned(),
            catalog: Arc::new(Catalog::new(layout, &config.dataset.prefix, listing, store)),
            batch_rows: config.batch_rows.max(1),
            chunk_blocks: config.chunk_blocks.unwrap_or(u64::MAX).max(1),
        })
    }
}

/// What `open` needs of the request, recorded in the chunk.
#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ChunkIdentity {
    version: u8,
    date: String,
    range: BlockRange,
    required: CapabilitySet,
    /// The log predicate the request accepts filtered logs for.
    scope: Option<FilterScope>,
}

#[async_trait]
impl HistorySource for AwsPublicSource {
    fn descriptor(&self) -> &SourceDescriptor {
        &self.descriptor
    }

    async fn plan(&self, request: &DataRequest) -> Result<SourcePlan, SourceError> {
        if request.chain_id != self.descriptor.chain_id {
            return Err(SourceError::InvalidPlan(
                "request belongs to another chain".to_owned(),
            ));
        }
        if request.range.start().0 < self.descriptor.range.map_or(0, |range| range.start().0) {
            return Err(SourceError::MissingRange(request.range));
        }
        if !CAPABILITIES.with_derivable().contains_all(request.required) {
            return Err(SourceError::InvalidPlan(
                "AWS public datasets serve headers, receipts, and logs; transaction bodies are lossy there".to_owned(),
            ));
        }
        if matches!(
            request.verification_policy,
            VerificationPolicy::CompleteCryptographic
        ) {
            return Err(SourceError::InvalidPlan(
                "AWS public datasets name the canonical chain themselves: they need the trusted-dataset verification policy".to_owned(),
            ));
        }
        let days = self
            .catalog
            .days(request.range.start().0, request.range.end().0)
            .await
            .map_err(|error| source_error(&error, request.range))?;
        // A log predicate is applied after each block's receipts are checked,
        // which needs all of them: transaction predicates are not applied.
        let scope = request
            .filter_scope()
            .filter(|scope| scope.constrains_logs() && !scope.constrains_transactions());
        let mut chunks = Vec::with_capacity(days.len());
        for day in days {
            let last = day.last.min(request.range.end().0);
            let mut first = day.first.max(request.range.start().0);
            loop {
                let end = first.saturating_add(self.chunk_blocks - 1).min(last);
                let range = BlockRange::new(BlockNumber(first), BlockNumber(end))
                    .map_err(|error| SourceError::InvalidPlan(error.to_string()))?;
                let identity = ChunkIdentity {
                    version: IDENTITY_VERSION,
                    date: day.date.clone(),
                    range,
                    required: request.required,
                    scope: scope.clone(),
                };
                chunks.push(SourceChunk {
                    source_id: self.descriptor.id.clone(),
                    range,
                    partition: postcard::to_allocvec(&identity)
                        .map_err(|error| SourceError::InvalidPlan(error.to_string()))?,
                    schema_version: SCHEMA_VERSION.to_owned(),
                    expected_parent: None,
                    estimated_bytes: None,
                });
                if end == last {
                    break;
                }
                first = end + 1;
            }
        }
        let plan = SourcePlan {
            source_id: self.descriptor.id.clone(),
            request: request.clone(),
            chunks,
            estimated_bytes: None,
            estimated_lag: self.descriptor.expected_lag,
            supplied: request
                .required
                .with_derivable()
                .intersection(CAPABILITIES.with_derivable()),
            complete: CAPABILITIES,
            trust: TrustModel::TrustedDataset,
            schema_version: SCHEMA_VERSION.to_owned(),
            physical_plan: Vec::new(),
        };
        plan.validate()?;
        Ok(plan)
    }

    async fn open(
        &self,
        chunk: &SourceChunk,
        budget: SourceBudget,
        cancellation: CancellationToken,
    ) -> Result<BlockFrameStream, SourceError> {
        let budget = budget.validate()?;
        let identity: ChunkIdentity = postcard::from_bytes(&chunk.partition)
            .map_err(|error| SourceError::InvalidPlan(format!("invalid AWS chunk: {error}")))?;
        if chunk.source_id != self.descriptor.id
            || chunk.schema_version != SCHEMA_VERSION
            || identity.version != IDENTITY_VERSION
            || identity.range != chunk.range
        {
            return Err(SourceError::InvalidPlan(
                "chunk belongs to another source".to_owned(),
            ));
        }
        if chunk.range.len() > budget.max_frames {
            return Err(SourceError::BudgetExceeded {
                resource: "frames",
                limit: budget.max_frames,
                observed: chunk.range.len(),
            });
        }
        let frames = self
            .read_day(&identity, chunk.expected_parent, &budget, &cancellation)
            .await
            .map_err(|error| source_error(&error, chunk.range))?;
        for frame in &frames {
            let bytes = frame.estimated_heap_bytes();
            if bytes > budget.max_frame_bytes {
                return Err(SourceError::BudgetExceeded {
                    resource: "frame_bytes",
                    limit: budget.max_frame_bytes,
                    observed: bytes,
                });
            }
        }
        Ok(stream::iter(frames.into_iter().map(Ok)).boxed())
    }
}

impl AwsPublicSource {
    #[allow(clippy::too_many_lines)]
    async fn read_day(
        &self,
        identity: &ChunkIdentity,
        expected_parent: Option<BlockHash>,
        budget: &SourceBudget,
        cancellation: &CancellationToken,
    ) -> Result<Vec<BlockFrame>, AwsError> {
        let input = InputBudget::new("input_bytes", budget.max_input_bytes);
        let resident = InputBudget::new("resident_bytes", budget.max_resident_bytes);
        let store = self.catalog.store();
        let range = identity.range;
        let (day_first, day_last) = self.catalog.span(&identity.date).await?;
        let day = day_last.saturating_sub(day_first).saturating_add(1);
        let read = |table: Table,
                    objects: Vec<(String, u64)>,
                    keep: Arc<dyn Fn(u64) -> bool + Send + Sync>,
                    kept: u64| {
            let store = Arc::clone(&store);
            let (input, resident) = (&input, &resident);
            async move {
                read::read_table(TableRead {
                    store: &store,
                    layout: self.layout,
                    table,
                    objects: &objects,
                    keep,
                    batch_rows: self.batch_rows,
                    concurrency: budget.max_in_flight_requests,
                    budget: input,
                    resident,
                    share: (kept, day),
                    cancellation,
                })
                .await
            }
        };
        let in_range: Arc<dyn Fn(u64) -> bool + Send + Sync> =
            Arc::new(move |number| range.contains(BlockNumber(number)));
        let block_batches = read(
            Table::Blocks,
            self.catalog.objects(Table::Blocks, &identity.date).await?,
            Arc::clone(&in_range),
            range.len(),
        )
        .await?;
        let mut blocks = read::blocks(self.layout, &block_batches)?;
        drop(block_batches);
        blocks.sort_by_key(|block| block.number);
        let numbers = blocks.iter().map(|block| block.number).collect::<Vec<_>>();
        if numbers.len() as u64 != range.len()
            || numbers.first() != Some(&range.start().0)
            || numbers.windows(2).any(|pair| pair[1] != pair[0] + 1)
        {
            return Err(AwsError::NotPublished(range));
        }
        // Only blocks whose bloom may hold a matching log need their
        // receipts: the rest provably have none.
        let needs_receipts = identity.required.contains(Capability::Receipts)
            || identity.required.contains(Capability::ReceiptOutcome);
        let checked: Option<BTreeSet<u64>> = match &identity.scope {
            Some(scope) if !needs_receipts => Some(
                blocks
                    .iter()
                    .filter(|block| may_match(&block.bloom, scope))
                    .map(|block| block.number)
                    .collect(),
            ),
            _ => None,
        };
        let keep: Arc<dyn Fn(u64) -> bool + Send + Sync> = match checked.clone() {
            Some(checked) => Arc::new(move |number| checked.contains(&number)),
            None => Arc::clone(&in_range),
        };
        let kept = checked
            .as_ref()
            .map_or(range.len(), |checked| checked.len() as u64);
        let (receipt_batches, type_batches, log_batches) = if kept > 0 {
            let types = async {
                if self.layout != DatasetLayout::SonarxEvmV1 {
                    return Ok(Vec::new());
                }
                let mut batches = Vec::new();
                // A day without failed transactions has no directory.
                for table in [Table::TransactionTypes, Table::FailedTransactionTypes] {
                    let objects = self.catalog.objects(table, &identity.date).await?;
                    batches.extend(read(table, objects, Arc::clone(&keep), kept).await?);
                }
                Ok::<_, AwsError>(batches)
            };
            let receipts = async {
                read(
                    Table::Receipts,
                    self.catalog
                        .objects(Table::Receipts, &identity.date)
                        .await?,
                    Arc::clone(&keep),
                    kept,
                )
                .await
            };
            let logs = async {
                read(
                    Table::Logs,
                    self.catalog.objects(Table::Logs, &identity.date).await?,
                    Arc::clone(&keep),
                    kept,
                )
                .await
            };
            tokio::try_join!(receipts, types, logs)?
        } else {
            (Vec::new(), Vec::new(), Vec::new())
        };
        let layout = self.layout;
        let context = FrameContext {
            chain_id: self.descriptor.chain_id,
            source_id: self.descriptor.id.clone(),
            locator: format!("{}/{{table}}/{}", self.prefix, identity.date),
            layout,
            required: identity.required,
            scope: identity.scope.clone(),
            observed_at_unix_ms: unix_now_ms(),
        };
        tokio::task::spawn_blocking(move || {
            let receipts = read::receipts(layout, &receipt_batches)?;
            let types = read::transaction_types(layout, &type_batches)?;
            let logs = read::logs(layout, &log_batches)?;
            drop((receipt_batches, type_batches, log_batches));
            assemble(
                &context,
                blocks,
                checked.as_ref(),
                receipts,
                &types,
                logs,
                expected_parent,
            )
        })
        .await
        .map_err(|error| AwsError::Store(error.to_string()))?
    }
}

struct FrameContext {
    chain_id: ChainId,
    source_id: SourceId,
    locator: String,
    layout: DatasetLayout,
    required: CapabilitySet,
    scope: Option<FilterScope>,
    observed_at_unix_ms: u64,
}

/// Check each block's rows against its commitments and build its frame.
/// `checked` names the blocks whose receipts were read when a log
/// predicate let the others go unread.
fn assemble(
    context: &FrameContext,
    blocks: Vec<BlockRow>,
    checked: Option<&BTreeSet<u64>>,
    receipts: Vec<ReceiptRow>,
    types: &[(u64, u32, u8)],
    logs: Vec<LogRow>,
    expected_parent: Option<BlockHash>,
) -> Result<Vec<BlockFrame>, AwsError> {
    let types = types
        .iter()
        .map(|(number, index, kind)| ((*number, *index), *kind))
        .collect::<HashMap<_, _>>();
    let mut receipts_by_block = BTreeMap::<u64, Vec<ReceiptRow>>::new();
    for receipt in receipts {
        receipts_by_block
            .entry(receipt.number)
            .or_default()
            .push(receipt);
    }
    let mut logs_by_block = BTreeMap::<u64, Vec<LogRow>>::new();
    for log in logs {
        logs_by_block.entry(log.number).or_default().push(log);
    }
    let mut frames = Vec::with_capacity(blocks.len());
    let mut parent = expected_parent.map(|hash| B256::from(hash.0));
    for block in blocks {
        let mismatch = |detail: String| AwsError::Mismatch {
            block: block.number,
            detail,
        };
        if let Some(parent) = parent
            && block.parent_hash != parent
        {
            return Err(mismatch("does not link to the block before it".to_owned()));
        }
        parent = Some(block.hash);
        let rebuilt = match verify::header_hash_matches(&block) {
            Some(false) => {
                return Err(mismatch(
                    "header fields do not hash to the block hash".to_owned(),
                ));
            }
            Some(true) => true,
            None => false,
        };
        let read = checked.is_none_or(|checked| checked.contains(&block.number));
        let (receipts, logs) = if read {
            let rows = receipts_by_block.remove(&block.number).unwrap_or_default();
            let logs = logs_by_block.remove(&block.number).unwrap_or_default();
            check_receipts(context, &block, rows, logs, &types)?
        } else {
            (Vec::new(), Vec::new())
        };
        frames.push(frame(context, &block, rebuilt, read, receipts, logs));
    }
    Ok(frames)
}

/// The block's receipts, each with its logs, after their root and bloom
/// match the header's.
fn check_receipts(
    context: &FrameContext,
    block: &BlockRow,
    mut rows: Vec<ReceiptRow>,
    mut logs: Vec<LogRow>,
    types: &HashMap<(u64, u32), u8>,
) -> Result<(Vec<ReceiptEnvelope>, Vec<Log>), AwsError> {
    let mismatch = |detail: String| AwsError::Mismatch {
        block: block.number,
        detail,
    };
    rows.sort_by_key(|receipt| receipt.index);
    logs.sort_by_key(|log| log.log_index);
    if let Some(count) = block.transaction_count
        && rows.len() != count as usize
    {
        return Err(mismatch(format!(
            "{} receipt rows for {count} transactions",
            rows.len()
        )));
    }
    if rows
        .iter()
        .enumerate()
        .any(|(position, receipt)| receipt.index as usize != position)
    {
        return Err(mismatch("receipt rows skip a transaction index".to_owned()));
    }
    let mut logs_by_transaction = BTreeMap::<u32, Vec<LogRow>>::new();
    for log in logs {
        logs_by_transaction
            .entry(log.transaction_index)
            .or_default()
            .push(log);
    }
    let mut encoded = Vec::with_capacity(rows.len());
    let mut bloom = Bloom::ZERO;
    let mut envelopes = Vec::with_capacity(rows.len());
    let mut all_logs = Vec::new();
    let mut previous_gas = 0_u64;
    for receipt in &rows {
        let kind = receipt
            .transaction_type
            .or_else(|| types.get(&(receipt.number, receipt.index)).copied())
            .ok_or_else(|| mismatch(format!("transaction {} has no type", receipt.index)))?;
        if receipt.success.is_none() {
            return Err(mismatch("receipt has no status".to_owned()));
        }
        let receipt_logs = logs_by_transaction
            .remove(&receipt.index)
            .unwrap_or_default();
        let (bytes, receipt_bloom) = verify::encode_receipt(receipt, kind, &receipt_logs);
        bloom |= receipt_bloom;
        let hash = TransactionHash::new(receipt.hash.0);
        let logs = receipt_logs
            .iter()
            .map(|log| Log {
                address: Address::new(log.address.0.0),
                topics: log.topics.iter().map(|topic| topic.0).collect(),
                data: log.data.to_vec(),
                transaction_hash: Some(hash),
                transaction_index: receipt.index,
                log_index: log.log_index,
            })
            .collect::<Vec<_>>();
        all_logs.extend(logs.iter().cloned());
        envelopes.push(ReceiptEnvelope {
            transaction_hash: hash,
            transaction_type: kind,
            transaction_index: receipt.index,
            encoded: Some(bytes.clone()),
            success: receipt.success,
            gas_used: Some(
                receipt
                    .gas_used
                    .unwrap_or_else(|| receipt.cumulative_gas.saturating_sub(previous_gas)),
            ),
            effective_gas_price: receipt.effective_gas_price.map(quantity),
            blob_gas_used: None,
            blob_gas_price: None,
            logs,
        });
        previous_gas = receipt.cumulative_gas;
        encoded.push(bytes);
    }
    if !logs_by_transaction.is_empty() {
        return Err(mismatch(
            "log rows name a transaction without a receipt".to_owned(),
        ));
    }
    if verify::receipts_root(&encoded) != block.receipts_root {
        return Err(mismatch(format!(
            "receipt and log rows do not rebuild the receipts root ({} layout)",
            match context.layout {
                DatasetLayout::AwsEthereumV1 => "aws-ethereum-v1",
                DatasetLayout::SonarxEvmV1 => "sonarx-evm-v1",
            }
        )));
    }
    if bloom != block.bloom {
        return Err(mismatch(
            "log rows do not rebuild the logs bloom".to_owned(),
        ));
    }
    Ok((envelopes, all_logs))
}

fn frame(
    context: &FrameContext,
    block: &BlockRow,
    rebuilt: bool,
    read: bool,
    receipts: Vec<ReceiptEnvelope>,
    logs: Vec<Log>,
) -> BlockFrame {
    let required = context.required.with_derivable();
    let wanted = |capability| required.contains(capability);
    let logs = match &context.scope {
        Some(scope) => Material::Filtered {
            value: logs
                .into_iter()
                .filter(|log| scope.matches_log(&log.address, &log.topics))
                .collect(),
            scope: scope.clone(),
            // A block left unread has a bloom no matching log fits.
            completeness: Completeness::VerifiedPredicate,
        },
        None if wanted(Capability::Logs) => Material::Complete(logs),
        None => Material::Missing(MissingReason::NotRequested),
    };
    let header = HeaderEnvelope {
        rlp: rebuilt
            .then(|| block.header.as_ref().map(alloy_rlp::encode))
            .flatten(),
        transactions_root: Some(BlockHash::new(block.transactions_root.0)),
        receipts_root: Some(BlockHash::new(block.receipts_root.0)),
        withdrawals_root: block.withdrawals_root.map(|root| BlockHash::new(root.0)),
        gas_limit: Some(block.gas_limit),
        gas_used: Some(block.gas_used),
        base_fee_per_gas: block.base_fee.map(|fee| quantity(u128::from(fee))),
        blob_gas_used: block.blob_gas_used,
        excess_blob_gas: block.excess_blob_gas,
        size_bytes: block.size,
        transaction_count: block.transaction_count,
        consensus_size_bytes: None,
    };
    let checked = |verified: bool| {
        if verified {
            VerificationCheck::VERIFIED
        } else {
            VerificationCheck::NOT_CHECKED
        }
    };
    BlockFrame {
        chain_id: context.chain_id,
        block: BlockRef {
            number: BlockNumber(block.number),
            hash: BlockHash::new(block.hash.0),
            parent_hash: BlockHash::new(block.parent_hash.0),
            timestamp: block.timestamp,
        },
        finality: Finality::Finalized,
        header: if wanted(Capability::Header) {
            Material::Complete(header)
        } else {
            Material::Missing(MissingReason::NotRequested)
        },
        transactions: Material::Missing(MissingReason::Unsupported),
        receipts: if wanted(Capability::Receipts) || wanted(Capability::ReceiptOutcome) {
            Material::Complete(receipts)
        } else {
            Material::Missing(MissingReason::NotRequested)
        },
        logs,
        withdrawals: Material::Missing(MissingReason::Unsupported),
        blob_sidecars: Material::Missing(MissingReason::Unsupported),
        traces: Material::Missing(MissingReason::Unsupported),
        state_diffs: Material::Missing(MissingReason::Unsupported),
        provenance: vec![Provenance {
            source_id: context.source_id.clone(),
            source_kind: SourceKind::PublicDataset,
            trust: TrustModel::TrustedDataset,
            range: Some(BlockRange::single(BlockNumber(block.number))),
            object: Some(ObjectIdentity {
                locator: context.locator.clone(),
                version: None,
                checksum: Some(block.hash.0),
                schema: Some(SCHEMA_VERSION.to_owned()),
            }),
            observed_at_unix_ms: context.observed_at_unix_ms,
            projection: vec!["header".to_owned(), "receipts".to_owned()],
        }],
        verification: VerificationReport {
            header_hash: checked(rebuilt),
            parent_continuity: VerificationCheck::VERIFIED,
            transactions_root: VerificationCheck::NOT_CHECKED,
            receipts_root: checked(read),
            withdrawals_root: VerificationCheck::NOT_CHECKED,
            dataset_checksum: VerificationCheck::NOT_CHECKED,
            consensus_anchor: None,
        },
    }
}

/// Whether a block with `bloom` may hold a log `scope` selects.
fn may_match(bloom: &Bloom, scope: &FilterScope) -> bool {
    (scope.addresses.is_empty()
        || scope
            .addresses
            .iter()
            .any(|address| bloom.contains_input(BloomInput::Raw(address.as_array()))))
        && scope.topics.iter().all(|filter| {
            filter
                .alternatives
                .iter()
                .any(|topic| bloom.contains_input(BloomInput::Raw(topic)))
        })
}

fn quantity(value: u128) -> Quantity {
    Quantity::new(U256::from(value).to_be_bytes())
}

fn source_error(error: &AwsError, range: BlockRange) -> SourceError {
    match *error {
        AwsError::Cancelled => SourceError::Cancelled,
        // Another source may hold what the dataset has not published.
        AwsError::NotPublished(missing) => SourceError::MissingRange(missing),
        AwsError::Budget {
            resource,
            observed,
            limit,
        } => SourceError::BudgetExceeded {
            resource,
            limit,
            observed,
        },
        AwsError::Mismatch { .. } | AwsError::Malformed { .. } | AwsError::MissingColumn { .. } => {
            SourceError::CorruptFrame(format!("{range:?}: {error}"))
        }
        AwsError::Store(_) | AwsError::Listing { .. } | AwsError::Parquet(_) => {
            SourceError::Unavailable(error.to_string())
        }
    }
}

fn unix_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
        })
}

#[cfg(test)]
mod tests {
    use object_store::{ObjectStoreExt, memory::InMemory, path::Path};

    use super::*;
    use crate::StoreListing;

    const PREFIX: &str = "v1.1/sonarx/base";
    const DATE: &str = "date=2023-07-15";
    /// Base blocks 1,306,481–1,306,482: before Canyon, one failed
    /// transaction, three logs.
    const TABLES: &[(&str, &[u8])] = &[
        (
            "blocks",
            include_bytes!("fixtures/base-1306481/blocks.parquet"),
        ),
        (
            "receipts",
            include_bytes!("fixtures/base-1306481/receipts.parquet"),
        ),
        (
            "transactions",
            include_bytes!("fixtures/base-1306481/transactions.parquet"),
        ),
        (
            "transactions_failed",
            include_bytes!("fixtures/base-1306481/transactions_failed.parquet"),
        ),
        ("logs", include_bytes!("fixtures/base-1306481/logs.parquet")),
    ];

    async fn source(tables: &[(&str, &[u8])], extra_logs: bool) -> AwsPublicSource {
        let store = Arc::new(InMemory::new());
        for (table, bytes) in tables {
            store
                .put(
                    &Path::from(format!("{PREFIX}/{table}/{DATE}/part-0.parquet")),
                    bytes.to_vec().into(),
                )
                .await
                .unwrap();
        }
        if extra_logs {
            store
                .put(
                    &Path::from(format!("{PREFIX}/logs/{DATE}/part-1.parquet")),
                    TABLES[4].1.to_vec().into(),
                )
                .await
                .unwrap();
        }
        let config = AwsPublicConfig {
            id: "aws-public".to_owned(),
            chain_id: 8453,
            dataset: PublicDataset {
                layout: DatasetLayout::SonarxEvmV1,
                prefix: PREFIX.to_owned(),
                first_block: 0,
            },
            bucket: Url::parse(AWS_PUBLIC_BLOCKCHAIN).unwrap(),
            priority: 0,
            batch_rows: 2,
            chunk_blocks: None,
        };
        let listing = Arc::new(StoreListing(store.clone()));
        AwsPublicSource::with_store(&config, store, listing).unwrap()
    }

    fn request(required: CapabilitySet, addresses: Vec<Address>) -> DataRequest {
        let mut filters = leani_source_api::FilterSet::default();
        filters.scope.addresses = addresses;
        DataRequest {
            chain_id: ChainId(8453),
            range: BlockRange::new(BlockNumber(1_306_481), BlockNumber(1_306_482)).unwrap(),
            required,
            log_fields: leani_primitives::LogFieldSet::NONE,
            allow_filtered: true,
            projection: leani_source_api::FieldProjection::default(),
            filters,
            minimum_finality: Finality::Finalized,
            verification_policy: VerificationPolicy::TrustedDataset,
        }
    }

    fn budget() -> SourceBudget {
        SourceBudget {
            max_input_bytes: 1 << 30,
            max_frame_bytes: 1 << 24,
            max_frames: 10,
            max_buffered_frames: 1,
            max_in_flight_requests: 2,
            temporary_disk_bytes: 1,
            max_resident_bytes: 1 << 30,
        }
    }

    async fn frames(
        source: &AwsPublicSource,
        request: &DataRequest,
    ) -> Result<Vec<BlockFrame>, SourceError> {
        let plan = source.plan(request).await?;
        assert_eq!(plan.chunks.len(), 1, "both blocks are one day");
        let stream = source
            .open(&plan.chunks[0], budget(), CancellationToken::new())
            .await?;
        stream.collect::<Vec<_>>().await.into_iter().collect()
    }

    #[tokio::test]
    async fn real_rows_check_against_their_roots() {
        let source = source(TABLES, false).await;
        let all = CapabilitySet::of(Capability::Header).with(Capability::Receipts);
        let frames = frames(&source, &request(all, Vec::new())).await.unwrap();
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[1].block.parent_hash, frames[0].block.hash);
        let mut outcomes = Vec::new();
        let mut logs = 0;
        for frame in &frames {
            frame.validate_shape().unwrap();
            assert_eq!(frame.verification.header_hash, VerificationCheck::VERIFIED);
            assert_eq!(
                frame.verification.receipts_root,
                VerificationCheck::VERIFIED
            );
            let Material::Complete(receipts) = &frame.receipts else {
                panic!("complete receipts");
            };
            outcomes.extend(receipts.iter().map(|receipt| receipt.success));
            let Material::Complete(values) = &frame.logs else {
                panic!("complete logs");
            };
            logs += values.len();
        }
        assert_eq!(outcomes.len(), 6);
        assert_eq!(
            outcomes
                .iter()
                .filter(|success| **success == Some(false))
                .count(),
            1
        );
        assert_eq!(logs, 3);
    }

    #[tokio::test]
    async fn filtered_logs_are_complete_for_their_predicate() {
        let source = source(TABLES, false).await;
        let all = CapabilitySet::of(Capability::Header).with(Capability::Receipts);
        let complete = frames(&source, &request(all, Vec::new())).await.unwrap();
        let Material::Complete(first) = &complete[1].logs else {
            panic!("complete logs");
        };
        let wanted = first[0].address;
        let matching = complete
            .iter()
            .flat_map(|frame| match &frame.logs {
                Material::Complete(logs) => logs.clone(),
                _ => Vec::new(),
            })
            .filter(|log| log.address == wanted)
            .count();
        let logs_only = CapabilitySet::of(Capability::Header).with(Capability::Logs);
        let filtered = frames(&source, &request(logs_only, vec![wanted]))
            .await
            .unwrap();
        let mut selected = 0;
        for frame in &filtered {
            frame.validate_shape().unwrap();
            let Material::Filtered {
                value,
                completeness,
                ..
            } = &frame.logs
            else {
                panic!("filtered logs");
            };
            assert_eq!(*completeness, Completeness::VerifiedPredicate);
            assert!(value.iter().all(|log| log.address == wanted));
            selected += value.len();
        }
        assert_eq!(selected, matching);
    }

    #[tokio::test]
    async fn rows_that_miss_the_roots_are_refused() {
        // Every log twice: the receipts no longer rebuild the root.
        let doubled = source(TABLES, true).await;
        let all = CapabilitySet::of(Capability::Header).with(Capability::Receipts);
        let error = frames(&doubled, &request(all, Vec::new()))
            .await
            .unwrap_err();
        assert!(matches!(error, SourceError::CorruptFrame(_)), "{error}");
        // Without the failed transactions the failed receipt has no type.
        let missing = source(
            &TABLES[..3]
                .iter()
                .chain(&TABLES[4..])
                .copied()
                .collect::<Vec<_>>(),
            false,
        )
        .await;
        let error = frames(&missing, &request(all, Vec::new()))
            .await
            .unwrap_err();
        assert!(matches!(error, SourceError::CorruptFrame(_)), "{error}");
    }

    #[tokio::test]
    async fn a_chunk_over_the_memory_budget_is_refused() {
        let source = source(TABLES, false).await;
        let all = CapabilitySet::of(Capability::Header).with(Capability::Receipts);
        let plan = source.plan(&request(all, Vec::new())).await.unwrap();
        let tight = SourceBudget {
            max_resident_bytes: 64,
            ..budget()
        };
        let Err(error) = source
            .open(&plan.chunks[0], tight, CancellationToken::new())
            .await
        else {
            panic!("a 64-byte budget cannot hold the day's rows");
        };
        assert!(
            matches!(
                error,
                SourceError::BudgetExceeded {
                    resource: "resident_bytes",
                    ..
                }
            ),
            "{error}"
        );
    }

    #[tokio::test]
    async fn a_day_one_table_lacks_is_not_served() {
        let source = source(&TABLES[..4], false).await;
        let all = CapabilitySet::of(Capability::Header).with(Capability::Receipts);
        let error = source.plan(&request(all, Vec::new())).await.unwrap_err();
        assert!(matches!(error, SourceError::MissingRange(_)), "{error}");
    }
}
