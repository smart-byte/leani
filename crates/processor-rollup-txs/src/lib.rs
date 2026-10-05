//! Rollups' non-blob L1 transactions: batch submissions posted as calldata,
//! state root / output proposals, and proof submissions.
//!
//! Each block is matched against operator-configured rules (the rollups'
//! contracts, senders and function selectors). Blob (type-3) transactions are
//! skipped: the blobs processor already reports them. The processor emits one
//! change per block, also when nothing matched, so a consumer can tell a block
//! without rollup activity from one that was never covered.

use std::collections::BTreeMap;

use alloy_primitives::U256;
use async_trait::async_trait;
use leani_primitives::{
    Address, BlockFrame, BlockHash, BlockNumber, Capability, CapabilitySet, Completeness,
    FilterScope, Finality, Material, ProcessorCursor, Quantity, ReceiptEnvelope, TransactionHash,
};
use leani_processor_api::{
    ChangeOperation, DataRequirement, DeliveryOrdering, DomainChange, DomainChanges, EncodedDelta,
    LifecyclePolicies, Processor, ProcessorDescriptor, ProcessorError, ProcessorId,
    ProcessorInstanceId, ProcessorSchemas, PublicationPolicy, ReducerTransaction, ReductionMode,
    RetentionPolicy, StartPoint,
};
use semver::Version;
use serde::{Deserialize, Serialize};

pub const PROCESSOR_ID: &str = "rollup-txs";
pub const BLOCK_COLLECTION: &str = "rollup_txs.blocks";
pub const CHANGE_KIND: &str = "rollups.block";

/// What a rollup's L1 transaction pays for.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub enum Purpose {
    /// Posting data: batches submitted as calldata.
    Data,
    /// State root or output proposals.
    State,
    /// Validity or fault proof submissions.
    Proof,
}

impl Purpose {
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Data => "data",
            Self::State => "state",
            Self::Proof => "proof",
        }
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "data" => Some(Self::Data),
            "state" => Some(Self::State),
            "proof" => Some(Self::Proof),
            _ => None,
        }
    }
}

/// One way a rollup's L1 transaction is recognized.
///
/// A transaction matches when it is sent to `to`, from `from` when set, calls
/// `selector` when set, carries `chain_id_arg` as its first ABI word when set
/// (shared settlement contracts serving many chains), in a block whose
/// timestamp lies in `[since, until)`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Rule {
    pub rollup: String,
    pub purpose: Purpose,
    pub to: Address,
    pub from: Option<Address>,
    pub selector: Option<[u8; 4]>,
    pub chain_id_arg: Option<u64>,
    pub since: Option<u64>,
    pub until: Option<u64>,
}

impl Rule {
    fn matches(&self, from: Address, input: &[u8], timestamp: u64) -> bool {
        if self.from.is_some_and(|expected| expected != from) {
            return false;
        }
        if self.since.is_some_and(|since| timestamp < since)
            || self.until.is_some_and(|until| timestamp >= until)
        {
            return false;
        }
        if let Some(selector) = self.selector
            && input.get(..4) != Some(selector.as_slice())
        {
            return false;
        }
        if let Some(chain_id) = self.chain_id_arg {
            let Some(word) = input.get(4..36) else {
                return false;
            };
            if word[..24].iter().any(|byte| *byte != 0) || word[24..] != chain_id.to_be_bytes() {
                return false;
            }
        }
        true
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RollupTxsConfig {
    /// Network name reported with every row, e.g. `mainnet`.
    pub network: String,
    pub start_block: BlockNumber,
    /// Most specific first per recipient: the first matching rule wins.
    pub rules: Vec<Rule>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TrackedBlock {
    pub network: String,
    pub block_number: u64,
    pub block_hash: BlockHash,
    pub parent_hash: BlockHash,
    pub timestamp: u64,
    pub finality: Finality,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TrackedTransaction {
    pub transaction_hash: TransactionHash,
    pub transaction_index: u32,
    pub rollup: String,
    pub purpose: Purpose,
    pub from: Address,
    pub to: Address,
    pub selector: Option<[u8; 4]>,
    pub gas_used: u64,
    /// Base fee × gas used.
    pub execution_burn: Quantity,
    /// Priority fee paid to the block proposer: (effective price − base fee) × gas used.
    pub tip: Quantity,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RollupTxsDelta {
    pub block: TrackedBlock,
    pub transactions: Vec<TrackedTransaction>,
}

#[derive(Clone, Debug)]
pub struct RollupTxsProcessor {
    config: RollupTxsConfig,
    /// Rule indexes per recipient, in configured order.
    by_recipient: BTreeMap<Address, Vec<usize>>,
    descriptor: ProcessorDescriptor,
}

impl RollupTxsProcessor {
    /// Create a processor for an immutable rule set.
    ///
    /// # Errors
    ///
    /// Rejects an empty rule set, a rule without a rollup id, an empty time
    /// window, or a configuration that cannot be encoded.
    pub fn new(config: RollupTxsConfig) -> Result<Self, ProcessorError> {
        if config.rules.is_empty() {
            return Err(ProcessorError::Input(
                "rollup-txs needs at least one rule".to_owned(),
            ));
        }
        let mut by_recipient = BTreeMap::<Address, Vec<usize>>::new();
        for (index, rule) in config.rules.iter().enumerate() {
            if rule.rollup.is_empty() {
                return Err(ProcessorError::Input(format!(
                    "rule {index} has no rollup id"
                )));
            }
            if let (Some(since), Some(until)) = (rule.since, rule.until)
                && since >= until
            {
                return Err(ProcessorError::Input(format!(
                    "rule {index} for {} has an empty time window",
                    rule.rollup
                )));
            }
            by_recipient.entry(rule.to).or_default().push(index);
        }
        // The network and rules decide the output; like the blobs processor,
        // a later coverage start only changes scheduling, not the identity.
        let identity = postcard::to_allocvec(&(&config.network, &config.rules))
            .map_err(|error| ProcessorError::Input(error.to_string()))?;
        let config_hash = BlockHash::new(*blake3::hash(&identity).as_bytes());
        let id = ProcessorId::new(PROCESSOR_ID)
            .map_err(|error| ProcessorError::Input(error.to_string()))?;
        let version = Version::new(1, 0, 0);
        let descriptor = ProcessorDescriptor {
            instance: ProcessorInstanceId::legacy(&id, &version, config_hash)
                .map_err(|error| ProcessorError::Input(error.to_string()))?,
            id,
            version,
            code_hash: BlockHash::new(
                *blake3::hash(b"leani/rollup-txs-processor/1.0.0").as_bytes(),
            ),
            config_hash,
            start: StartPoint::Block(config.start_block),
            requirements: vec![DataRequirement {
                capabilities: CapabilitySet::of(Capability::Header)
                    .with(Capability::Transactions)
                    .with(Capability::Calldata)
                    .with(Capability::Receipts),
                log_fields: leani_primitives::LogFieldSet::NONE,
                allow_filtered: true,
                filter: FilterScope {
                    recipients: by_recipient.keys().copied().collect(),
                    ..FilterScope::default()
                },
                minimum_finality: Finality::Included,
            }],
            mode: ReductionMode::BlockLocal,
            delivery_ordering: DeliveryOrdering::BlockVersionedIdempotent,
            publication: PublicationPolicy::IncludedAndFinalized,
            lifecycle: LifecyclePolicies::from_legacy(RetentionPolicy::FullOutputHistory),
            schemas: ProcessorSchemas {
                delta_version: 1,
                entity_schema: "rollup-txs.entity.v1".to_owned(),
                change_schema: "rollups.block-bundle.v1".to_owned(),
            },
        };
        Ok(Self {
            config,
            by_recipient,
            descriptor,
        })
    }

    /// Apply immutable operator-owned instance and lifecycle policies.
    #[must_use]
    pub fn with_contract(
        mut self,
        instance: ProcessorInstanceId,
        publication: PublicationPolicy,
        lifecycle: LifecyclePolicies,
    ) -> Self {
        self.descriptor.instance = instance;
        self.descriptor.publication = publication;
        self.descriptor.lifecycle = lifecycle;
        self
    }

    /// Derive the block's matched transactions without mutating state.
    ///
    /// # Errors
    ///
    /// Rejects incomplete or partial material and inconsistent receipts.
    pub fn derive(&self, frame: &BlockFrame) -> Result<RollupTxsDelta, ProcessorError> {
        self.descriptor.requirements[0]
            .validate_frame(frame)
            .map_err(|error| ProcessorError::Input(error.to_owned()))?;
        let header = accepted_material(&frame.header, "header")?;
        let transactions = accepted_material(&frame.transactions, "transactions")?;
        let receipts = receipt_map(accepted_material(&frame.receipts, "receipts")?)?;
        let base_fee = wei(required(
            header.base_fee_per_gas,
            "header.base_fee_per_gas",
        )?);

        let mut matched = Vec::new();
        for transaction in transactions {
            // Blob transactions are the blobs processor's; contract creations have no recipient.
            if transaction.transaction_type == 3 {
                continue;
            }
            let Some(to) = transaction.to else { continue };
            let Some(candidates) = self.by_recipient.get(&to) else {
                continue;
            };
            let from = required(transaction.from, "transaction.from")?;
            let input = transaction
                .input
                .as_deref()
                .ok_or_else(|| ProcessorError::Input("transaction.input is required".to_owned()))?;
            let Some(rule) = candidates
                .iter()
                .map(|&index| &self.config.rules[index])
                .find(|rule| rule.matches(from, input, frame.block.timestamp))
            else {
                continue;
            };
            let receipt = receipts.get(&transaction.hash).ok_or_else(|| {
                ProcessorError::Input(format!("missing receipt for {}", transaction.hash))
            })?;
            if receipt.transaction_index != transaction.index {
                return Err(ProcessorError::Invariant(
                    "transaction/receipt identity mismatch".to_owned(),
                ));
            }
            let gas_used = required(receipt.gas_used, "receipt.gas_used")?;
            let effective_price = wei(required(
                receipt.effective_gas_price,
                "receipt.effective_gas_price",
            )?);
            let gas = U256::from(gas_used);
            let execution_burn = base_fee
                .checked_mul(gas)
                .ok_or_else(|| ProcessorError::Invariant("execution burn overflow".to_owned()))?;
            let tip = effective_price
                .saturating_sub(base_fee)
                .checked_mul(gas)
                .ok_or_else(|| ProcessorError::Invariant("tip overflow".to_owned()))?;
            matched.push(TrackedTransaction {
                transaction_hash: transaction.hash,
                transaction_index: transaction.index,
                rollup: rule.rollup.clone(),
                purpose: rule.purpose,
                from,
                to,
                selector: input.get(..4).and_then(|bytes| bytes.try_into().ok()),
                gas_used,
                execution_burn: execution_burn.into(),
                tip: tip.into(),
            });
        }
        matched.sort_by_key(|transaction| transaction.transaction_index);

        Ok(RollupTxsDelta {
            block: TrackedBlock {
                network: self.config.network.clone(),
                block_number: frame.block.number.0,
                block_hash: frame.block.hash,
                parent_hash: frame.block.parent_hash,
                timestamp: frame.block.timestamp,
                finality: frame.finality,
            },
            transactions: matched,
        })
    }
}

#[async_trait]
impl Processor for RollupTxsProcessor {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn descriptor(&self) -> &ProcessorDescriptor {
        &self.descriptor
    }

    async fn map(&self, block: &BlockFrame) -> Result<EncodedDelta, ProcessorError> {
        let delta = self.derive(block)?;
        let payload = postcard::to_allocvec(&delta)
            .map_err(|error| ProcessorError::DeltaPayload(error.to_string()))?;
        Ok(EncodedDelta::new(
            &self.descriptor,
            block.chain_id,
            block.block,
            payload,
        ))
    }

    /// An included block re-delivered as finalized differs only in its finality.
    fn finality_variant_checksums(
        &self,
        delta: &EncodedDelta,
    ) -> Result<Vec<BlockHash>, ProcessorError> {
        delta.validate(&self.descriptor)?;
        let decoded: RollupTxsDelta = postcard::from_bytes(&delta.payload)
            .map_err(|error| ProcessorError::DeltaPayload(error.to_string()))?;
        let mut checksums = Vec::with_capacity(2);
        for finality in [Finality::Included, Finality::Finalized] {
            let mut variant = decoded.clone();
            variant.block.finality = finality;
            let payload = postcard::to_allocvec(&variant)
                .map_err(|error| ProcessorError::DeltaPayload(error.to_string()))?;
            checksums.push(
                EncodedDelta::new(&self.descriptor, delta.chain_id, delta.block, payload).checksum,
            );
        }
        checksums.sort_unstable();
        checksums.dedup();
        Ok(checksums)
    }

    async fn reduce(
        &self,
        transaction: &mut dyn ReducerTransaction,
        cursor: &ProcessorCursor,
        delta: &EncodedDelta,
    ) -> Result<DomainChanges, ProcessorError> {
        validate_delta_cursor(&self.descriptor, cursor, delta)?;
        let decoded: RollupTxsDelta = postcard::from_bytes(&delta.payload)
            .map_err(|error| ProcessorError::DeltaPayload(error.to_string()))?;
        if decoded.block.block_number != delta.block.number.0
            || decoded.block.block_hash != delta.block.hash
        {
            return Err(ProcessorError::DeltaContract);
        }
        let key = decoded.block.block_number.to_be_bytes().to_vec();
        let payload = postcard::to_allocvec(&decoded)
            .map_err(|error| ProcessorError::State(error.to_string()))?;
        transaction
            .put(BLOCK_COLLECTION, key.clone(), payload.clone())
            .await?;
        let change = DomainChange {
            kind: CHANGE_KIND.to_owned(),
            key,
            operation: ChangeOperation::Upsert,
            payload,
        };
        transaction.emit(change.clone()).await?;
        Ok(DomainChanges {
            changes: vec![change],
        })
    }

    fn change_json(
        &self,
        change: &DomainChange,
    ) -> Result<Option<serde_json::Value>, ProcessorError> {
        if change.kind != CHANGE_KIND {
            return Ok(None);
        }
        bundle_json(&change.payload).map(Some)
    }

    fn entity_json(
        &self,
        collection: &str,
        _key: &[u8],
        value: &[u8],
    ) -> Result<Option<serde_json::Value>, ProcessorError> {
        if collection != BLOCK_COLLECTION {
            return Ok(None);
        }
        bundle_json(value).map(Some)
    }
}

/// The public block bundle: camelCase, hex hashes and addresses, wei as decimal strings.
fn bundle_json(bytes: &[u8]) -> Result<serde_json::Value, ProcessorError> {
    let delta: RollupTxsDelta =
        postcard::from_bytes(bytes).map_err(|error| ProcessorError::State(error.to_string()))?;
    let block = &delta.block;
    let transactions = delta
        .transactions
        .iter()
        .map(|transaction| {
            serde_json::json!({
                "network": block.network,
                "blockNumber": block.block_number,
                "blockHash": block.block_hash.to_string(),
                "txHash": transaction.transaction_hash.to_string(),
                "transactionIndex": transaction.transaction_index,
                "rollupId": transaction.rollup,
                "purpose": transaction.purpose.name(),
                "fromAddress": transaction.from.to_string(),
                "toAddress": transaction.to.to_string(),
                "selector": transaction.selector.map(|selector| format!("0x{}", hex::encode(selector))),
                "gasUsed": transaction.gas_used.to_string(),
                "executionBurnedWei": wei(transaction.execution_burn).to_string(),
                "tipWei": wei(transaction.tip).to_string(),
            })
        })
        .collect::<Vec<_>>();
    Ok(serde_json::json!({
        "block": {
            "network": block.network,
            "blockNumber": block.block_number,
            "blockHash": block.block_hash.to_string(),
            "parentHash": block.parent_hash.to_string(),
            "timestamp": block.timestamp,
        },
        "transactions": transactions,
    }))
}

fn accepted_material<'a, T>(
    material: &'a Material<T>,
    label: &str,
) -> Result<&'a T, ProcessorError> {
    match material {
        Material::Complete(value)
        | Material::Filtered {
            value,
            completeness: Completeness::VerifiedPredicate | Completeness::DatasetDeclared,
            ..
        } => Ok(value),
        Material::Filtered {
            completeness: Completeness::Partial,
            ..
        } => Err(ProcessorError::Input(format!(
            "{label} projection is only partial"
        ))),
        Material::Missing(reason) => Err(ProcessorError::Input(format!(
            "{label} material is missing: {reason:?}"
        ))),
    }
}

fn receipt_map(
    receipts: &[ReceiptEnvelope],
) -> Result<BTreeMap<TransactionHash, &ReceiptEnvelope>, ProcessorError> {
    let mut output = BTreeMap::new();
    for receipt in receipts {
        if output.insert(receipt.transaction_hash, receipt).is_some() {
            return Err(ProcessorError::Invariant(
                "duplicate projected receipt".to_owned(),
            ));
        }
    }
    Ok(output)
}

fn wei(value: Quantity) -> U256 {
    <U256 as From<Quantity>>::from(value)
}

fn required<T: Copy>(value: Option<T>, field: &str) -> Result<T, ProcessorError> {
    value.ok_or_else(|| ProcessorError::Input(format!("{field} is required")))
}

fn validate_delta_cursor(
    descriptor: &ProcessorDescriptor,
    cursor: &ProcessorCursor,
    delta: &EncodedDelta,
) -> Result<(), ProcessorError> {
    delta.validate(descriptor)?;
    if cursor.processor_id != descriptor.id.as_str()
        || cursor.processor_version != descriptor.version.to_string()
        || cursor.chain_id != delta.chain_id
        || cursor.block_number != delta.block.number
        || cursor.block_hash != delta.block.hash
    {
        return Err(ProcessorError::CursorMismatch);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use leani_primitives::{
        BlockRef, ChainId, HeaderEnvelope, MissingReason, TransactionEnvelope, VerificationReport,
    };
    use leani_testkit::MemoryReducer;

    use super::*;

    const INBOX: Address = Address::new([0xaa; 20]);
    const BATCHER: Address = Address::new([0xbb; 20]);
    const GAME_FACTORY: Address = Address::new([0xcc; 20]);
    const SHARED_VALIDATOR: Address = Address::new([0xdd; 20]);
    const BASE_FEE: u64 = 7;

    fn rule(rollup: &str, purpose: Purpose, to: Address) -> Rule {
        Rule {
            rollup: rollup.to_owned(),
            purpose,
            to,
            from: None,
            selector: None,
            chain_id_arg: None,
            since: None,
            until: None,
        }
    }

    fn processor() -> RollupTxsProcessor {
        RollupTxsProcessor::new(RollupTxsConfig {
            network: "mainnet".to_owned(),
            start_block: BlockNumber(1),
            rules: vec![
                // calldata batches: a transfer from the batcher to the inbox
                Rule {
                    from: Some(BATCHER),
                    ..rule("base", Purpose::Data, INBOX)
                },
                // output proposals through the dispute game factory
                Rule {
                    selector: Some([0x82, 0xec, 0xf2, 0xf6]),
                    until: Some(1_000),
                    ..rule("base", Purpose::State, GAME_FACTORY)
                },
                // one shared settlement contract, told apart by the chain id argument
                Rule {
                    selector: Some([0x11, 0x22, 0x33, 0x44]),
                    chain_id_arg: Some(324),
                    ..rule("zksync", Purpose::Proof, SHARED_VALIDATOR)
                },
            ],
        })
        .expect("processor")
    }

    fn chain_word(chain_id: u64) -> Vec<u8> {
        let mut word = vec![0_u8; 24];
        word.extend_from_slice(&chain_id.to_be_bytes());
        word
    }

    struct Tx {
        kind: u8,
        from: Address,
        to: Address,
        input: Vec<u8>,
        gas: u64,
        price: u64,
    }

    fn frame(timestamp: u64, txs: &[Tx]) -> BlockFrame {
        let transactions = txs
            .iter()
            .enumerate()
            .map(|(index, tx)| TransactionEnvelope {
                hash: TransactionHash::new([u8::try_from(index + 1).expect("index"); 32]),
                transaction_type: tx.kind,
                index: u32::try_from(index).expect("index"),
                encoded: None,
                from: Some(tx.from),
                to: Some(tx.to),
                nonce: None,
                gas_limit: None,
                value: None,
                input: Some(tx.input.clone()),
                max_fee_per_gas: None,
                max_priority_fee_per_gas: None,
                max_fee_per_blob_gas: None,
                blob_versioned_hashes: Vec::new(),
                size_bytes: None,
            })
            .collect::<Vec<_>>();
        let receipts = txs
            .iter()
            .enumerate()
            .map(|(index, tx)| ReceiptEnvelope {
                transaction_hash: TransactionHash::new(
                    [u8::try_from(index + 1).expect("index"); 32],
                ),
                transaction_type: tx.kind,
                transaction_index: u32::try_from(index).expect("index"),
                encoded: None,
                success: Some(true),
                gas_used: Some(tx.gas),
                effective_gas_price: Some(Quantity::from(U256::from(tx.price))),
                blob_gas_used: None,
                blob_gas_price: None,
                logs: Vec::new(),
            })
            .collect();
        BlockFrame {
            chain_id: ChainId(1),
            block: BlockRef {
                number: BlockNumber(42),
                hash: BlockHash::new([0x42; 32]),
                parent_hash: BlockHash::new([0x41; 32]),
                timestamp,
            },
            finality: Finality::Included,
            header: Material::Complete(HeaderEnvelope {
                rlp: None,
                transactions_root: None,
                receipts_root: None,
                withdrawals_root: None,
                gas_limit: Some(30_000_000),
                gas_used: Some(1_000_000),
                base_fee_per_gas: Some(Quantity::from(U256::from(BASE_FEE))),
                blob_gas_used: Some(0),
                excess_blob_gas: Some(0),
                size_bytes: Some(1),
                transaction_count: Some(u32::try_from(txs.len()).expect("count")),
                consensus_size_bytes: None,
            }),
            transactions: Material::Complete(transactions),
            receipts: Material::Complete(receipts),
            logs: Material::Missing(MissingReason::NotRequested),
            withdrawals: Material::Missing(MissingReason::NotRequested),
            blob_sidecars: Material::Missing(MissingReason::NotRequested),
            traces: Material::Missing(MissingReason::NotRequested),
            state_diffs: Material::Missing(MissingReason::NotRequested),
            provenance: Vec::new(),
            verification: VerificationReport::default(),
        }
    }

    #[test]
    fn matches_transfers_calls_and_chain_arguments_and_skips_the_rest() {
        let stranger = Address::new([0xee; 20]);
        let mut zk_call = vec![0x11, 0x22, 0x33, 0x44];
        zk_call.extend(chain_word(324));
        let mut other_chain = vec![0x11, 0x22, 0x33, 0x44];
        other_chain.extend(chain_word(7777));
        let delta = processor()
            .derive(&frame(
                500,
                &[
                    Tx {
                        kind: 2,
                        from: BATCHER,
                        to: INBOX,
                        input: vec![0x00, 0x01],
                        gas: 100,
                        price: 9,
                    },
                    Tx {
                        kind: 3,
                        from: BATCHER,
                        to: INBOX,
                        input: Vec::new(),
                        gas: 50,
                        price: 9,
                    },
                    Tx {
                        kind: 2,
                        from: stranger,
                        to: INBOX,
                        input: Vec::new(),
                        gas: 50,
                        price: 9,
                    },
                    Tx {
                        kind: 2,
                        from: stranger,
                        to: GAME_FACTORY,
                        input: vec![0x82, 0xec, 0xf2, 0xf6, 0x01],
                        gas: 300,
                        price: 7,
                    },
                    Tx {
                        kind: 2,
                        from: stranger,
                        to: GAME_FACTORY,
                        input: vec![0xde, 0xad, 0xbe, 0xef],
                        gas: 300,
                        price: 7,
                    },
                    Tx {
                        kind: 2,
                        from: stranger,
                        to: SHARED_VALIDATOR,
                        input: zk_call,
                        gas: 1_000,
                        price: 8,
                    },
                    Tx {
                        kind: 2,
                        from: stranger,
                        to: SHARED_VALIDATOR,
                        input: other_chain,
                        gas: 1_000,
                        price: 8,
                    },
                ],
            ))
            .expect("derive");
        let found = delta
            .transactions
            .iter()
            .map(|t| {
                (
                    t.transaction_index,
                    t.rollup.as_str(),
                    t.purpose,
                    t.gas_used,
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            found,
            vec![
                (0, "base", Purpose::Data, 100),
                (3, "base", Purpose::State, 300),
                (5, "zksync", Purpose::Proof, 1_000),
            ]
        );
        let batch = &delta.transactions[0];
        assert_eq!(wei(batch.execution_burn), U256::from(100 * BASE_FEE));
        assert_eq!(wei(batch.tip), U256::from(100 * (9 - BASE_FEE)));
        assert_eq!(
            delta.transactions[1].selector,
            Some([0x82, 0xec, 0xf2, 0xf6])
        );
    }

    #[test]
    fn a_rule_stops_matching_at_its_until_timestamp() {
        let call = Tx {
            kind: 2,
            from: BATCHER,
            to: GAME_FACTORY,
            input: vec![0x82, 0xec, 0xf2, 0xf6],
            gas: 1,
            price: 7,
        };
        let processor = processor();
        assert_eq!(
            processor
                .derive(&frame(999, std::slice::from_ref(&call)))
                .expect("derive")
                .transactions
                .len(),
            1
        );
        assert!(
            processor
                .derive(&frame(1_000, &[call]))
                .expect("derive")
                .transactions
                .is_empty()
        );
    }

    #[tokio::test]
    async fn every_block_emits_one_bundle_change_rendered_as_public_json() {
        let processor = processor();
        let frame = frame(
            500,
            &[Tx {
                kind: 2,
                from: BATCHER,
                to: INBOX,
                input: vec![0x00],
                gas: 21_000,
                price: 9,
            }],
        );
        let delta = processor.map(&frame).await.expect("map");
        let cursor = ProcessorCursor {
            processor_id: processor.descriptor.id.to_string(),
            processor_version: processor.descriptor.version.to_string(),
            chain_id: frame.chain_id,
            block_number: frame.block.number,
            block_hash: frame.block.hash,
            finality: frame.finality,
            sequence: 1,
        };
        let mut reducer = MemoryReducer::default();
        let changes = processor
            .reduce(&mut reducer, &cursor, &delta)
            .await
            .expect("reduce");
        assert_eq!(changes.changes.len(), 1);
        let json = processor
            .change_json(&changes.changes[0])
            .expect("render")
            .expect("bundle");
        assert_eq!(json["block"]["blockNumber"], 42);
        assert_eq!(json["block"]["network"], "mainnet");
        let tx = &json["transactions"][0];
        assert_eq!(tx["rollupId"], "base");
        assert_eq!(tx["purpose"], "data");
        assert_eq!(tx["fromAddress"], format!("0x{}", "bb".repeat(20)));
        assert_eq!(tx["gasUsed"], "21000");
        assert_eq!(tx["executionBurnedWei"], (21_000 * BASE_FEE).to_string());
        assert_eq!(tx["tipWei"], (21_000 * (9 - BASE_FEE)).to_string());

        // a block without rollup activity still emits its (empty) bundle
        let empty = processor.derive(&self::frame(501, &[])).expect("derive");
        assert!(empty.transactions.is_empty());
    }
}
