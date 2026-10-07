//! Normalize projected material after checking the complete execution
//! commitments. Filtered material records a verified predicate over those inputs.

use alloy_consensus::{
    Block, EthereumReceipt, Header, Transaction as _, TxReceipt as _,
    transaction::SignerRecoverable,
};
use alloy_eips::Encodable2718;
use alloy_primitives::U256;
use leani_primitives::{
    BlockFrame, BlockHash, BlockNumber, BlockRange, BlockRef, Capability, CapabilitySet,
    CheckStatus, Completeness, FilterScope, Finality, HeaderEnvelope, Log, LogField, LogFieldSet,
    Material, MissingReason, ObjectIdentity, Provenance, ReceiptEnvelope, SourceKind,
    TransactionEnvelope, TransactionHash, TrustModel, VerificationCheck, VerificationReport,
    Withdrawal,
};
use leani_source_api::{DataRequest, SourceError};
use reth_ethereum_primitives::BlockBody;
use serde::{Deserialize, Serialize};

use super::{CatalogObject, EraeSource, address, quantity};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(super) struct EraeProjection {
    pub(super) required: CapabilitySet,
    pub(super) scope: Option<FilterScope>,
    pub(super) log_fields: LogFieldSet,
}

impl EraeProjection {
    pub(super) fn from_request(request: &DataRequest) -> Self {
        let mut scope = request.filters.scope.clone();
        if scope.senders.is_empty() {
            scope.senders.clone_from(&request.filters.senders);
        }
        if scope.recipients.is_empty() {
            scope.recipients.clone_from(&request.filters.recipients);
        }
        Self {
            required: request.required,
            scope: (request.allow_filtered && scope != FilterScope::default()).then_some(scope),
            log_fields: request.log_fields,
        }
    }

    pub(super) fn needs_body(&self) -> bool {
        self.transactions_requested()
            || self.required.contains(Capability::Receipts)
            || self.required.contains(Capability::Withdrawals)
            || (self.required.contains(Capability::Logs)
                && (self.log_fields.contains(LogField::TransactionHash)
                    || self.scope.as_ref().is_some_and(|scope| {
                        !scope.transaction_types.is_empty()
                            || !scope.transaction_hashes.is_empty()
                            || !scope.senders.is_empty()
                            || !scope.recipients.is_empty()
                    })))
    }

    fn transactions_requested(&self) -> bool {
        self.required.contains(Capability::Transactions)
            || self.required.contains(Capability::Body)
            || self.required.contains(Capability::Calldata)
    }
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(super) fn normalize_block(
    source: &EraeSource,
    object: &CatalogObject,
    header: &Header,
    body: Option<&BlockBody>,
    receipts: Option<&[EthereumReceipt]>,
    observed_at_unix_ms: u64,
    parent_checked: bool,
    projection: &EraeProjection,
) -> Result<BlockFrame, SourceError> {
    let hash = header.hash_slow();
    let header_requested = projection.required.contains(Capability::Header);
    let transactions_requested = projection.transactions_requested();
    let receipts_requested = projection.required.contains(Capability::Receipts);
    let logs_requested = projection.required.contains(Capability::Logs);
    // Execution RPC and raw-history consumers need the withdrawals committed
    // by a complete body even when their request does not name the capability.
    let withdrawals_requested =
        projection.required.contains(Capability::Withdrawals) || body.is_some();
    let scope = projection.scope.as_ref();
    let transaction_filtered = scope.is_some_and(|scope| {
        scope.block_range.is_some()
            || !scope.transaction_types.is_empty()
            || !scope.transaction_hashes.is_empty()
            || !scope.senders.is_empty()
            || !scope.recipients.is_empty()
    });
    let logs_filtered = transaction_filtered
        || scope.is_some_and(|scope| !scope.addresses.is_empty() || !scope.topics.is_empty());
    let body_present = body.is_some();
    let block_size = body
        .filter(|_| header_requested)
        .map(|body| Block::rlp_length_for(header, body));
    let empty_body = BlockBody::default();
    let body = body.unwrap_or(&empty_body);
    let mut normalized_transactions = Vec::new();
    let mut normalized_receipts = Vec::new();
    let mut normalized_logs = Vec::new();
    let mut previous_gas = 0_u64;
    let mut block_log_index = 0_u32;
    let transaction_count = if transactions_requested || receipts_requested || logs_requested {
        body.transactions.len().max(receipts.map_or(0, <[_]>::len))
    } else {
        0
    };
    for index in 0..transaction_count {
        let transaction = body.transactions.get(index);
        let transaction_index = u32::try_from(index)
            .map_err(|_| SourceError::CorruptFrame("transaction index overflow".to_owned()))?;
        let preliminary_match = scope.is_none_or(|scope| {
            scope
                .block_range
                .is_none_or(|range| range.contains(BlockNumber(header.number)))
                && (scope.transaction_types.is_empty()
                    || transaction.is_some_and(|transaction| {
                        scope
                            .transaction_types
                            .contains(&(transaction.tx_type() as u8))
                    }))
                && (scope.transaction_hashes.is_empty()
                    || transaction.is_some_and(|transaction| {
                        scope
                            .transaction_hashes
                            .contains(&TransactionHash::new(transaction.tx_hash().0))
                    }))
                && (scope.recipients.is_empty()
                    || transaction
                        .and_then(alloy_consensus::Transaction::to)
                        .map(address)
                        .is_some_and(|to| scope.recipients.contains(&to)))
        });
        let sender = if preliminary_match
            && (projection.required.contains(Capability::Transactions)
                || scope.is_some_and(|scope| !scope.senders.is_empty()))
        {
            Some(
                transaction
                    .ok_or_else(|| {
                        SourceError::CorruptFrame("eraE sender requires a body".to_owned())
                    })?
                    .recover_signer()
                    .map_err(|error| SourceError::CorruptFrame(error.to_string()))?,
            )
        } else {
            None
        };
        let transaction_matches = preliminary_match
            && scope.is_none_or(|scope| {
                scope.senders.is_empty()
                    || sender
                        .map(address)
                        .is_some_and(|sender| scope.senders.contains(&sender))
            });
        let transaction_hash = transaction
            .filter(|_| {
                transaction_matches && projection.log_fields.contains(LogField::TransactionHash)
            })
            .map(|transaction| TransactionHash::new(transaction.tx_hash().0));
        if let Some(transaction) =
            transaction.filter(|_| transactions_requested && transaction_matches)
        {
            let encoded = transaction.encoded_2718();
            let size_bytes = u32::try_from(encoded.len()).unwrap_or(u32::MAX);
            normalized_transactions.push(TransactionEnvelope {
                hash: TransactionHash::new(transaction.tx_hash().0),
                transaction_type: transaction.tx_type() as u8,
                index: transaction_index,
                encoded: Some(encoded),
                from: sender.map(address),
                to: transaction.to().map(address),
                nonce: Some(transaction.nonce()),
                gas_limit: Some(transaction.gas_limit()),
                value: Some(quantity(transaction.value())),
                input: Some(transaction.input().to_vec()),
                max_fee_per_gas: Some(quantity(U256::from(transaction.max_fee_per_gas()))),
                max_priority_fee_per_gas: transaction
                    .max_priority_fee_per_gas()
                    .map(U256::from)
                    .map(quantity),
                max_fee_per_blob_gas: transaction
                    .max_fee_per_blob_gas()
                    .map(U256::from)
                    .map(quantity),
                blob_versioned_hashes: transaction
                    .blob_versioned_hashes()
                    .unwrap_or_default()
                    .iter()
                    .map(|hash| BlockHash::new(hash.0))
                    .collect(),
                size_bytes: Some(size_bytes),
            });
        }
        if let Some(receipt) = receipts.and_then(|receipts| receipts.get(index)) {
            let gas_used = receipt.cumulative_gas_used.saturating_sub(previous_gas);
            previous_gas = receipt.cumulative_gas_used;
            let mut receipt_logs = Vec::new();
            for source_log in &receipt.logs {
                let selected_for_receipt = receipts_requested && transaction_matches;
                let selected_for_logs =
                    logs_requested && transaction_matches && source_log_matches(scope, source_log);
                if selected_for_receipt || selected_for_logs {
                    let log = Log {
                        address: address(source_log.address),
                        topics: source_log
                            .data
                            .topics()
                            .iter()
                            .map(|topic| topic.0)
                            .collect(),
                        data: source_log.data.data.to_vec(),
                        transaction_hash,
                        transaction_index,
                        log_index: block_log_index,
                    };
                    if selected_for_receipt {
                        receipt_logs.push(log.clone());
                    }
                    if selected_for_logs {
                        normalized_logs.push(log);
                    }
                }
                block_log_index = block_log_index.checked_add(1).ok_or_else(|| {
                    SourceError::CorruptFrame("log index overflows u32".to_owned())
                })?;
            }
            if receipts_requested && transaction_matches {
                let transaction = transaction.ok_or_else(|| {
                    SourceError::CorruptFrame("eraE receipt envelope requires a body".to_owned())
                })?;
                normalized_receipts.push(ReceiptEnvelope {
                    transaction_hash: TransactionHash::new(transaction.tx_hash().0),
                    transaction_type: receipt.tx_type as u8,
                    transaction_index,
                    encoded: Some(receipt.with_bloom_ref().encoded_2718()),
                    success: Some(receipt.success),
                    gas_used: Some(gas_used),
                    effective_gas_price: Some(quantity(U256::from(
                        transaction.effective_gas_price(header.base_fee_per_gas),
                    ))),
                    blob_gas_used: transaction.blob_gas_used(),
                    blob_gas_price: None,
                    logs: receipt_logs,
                });
            }
        }
    }
    let withdrawals = body
        .withdrawals
        .as_ref()
        .filter(|_| withdrawals_requested)
        .map(|withdrawals| {
            withdrawals
                .iter()
                .map(|withdrawal| Withdrawal {
                    index: withdrawal.index,
                    validator_index: withdrawal.validator_index,
                    address: address(withdrawal.address),
                    amount_gwei: withdrawal.amount,
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    Ok(BlockFrame {
        chain_id: source.config.chain_id,
        block: BlockRef {
            number: BlockNumber(header.number),
            hash: BlockHash::new(hash.0),
            parent_hash: BlockHash::new(header.parent_hash.0),
            timestamp: header.timestamp,
        },
        finality: Finality::Finalized,
        header: if header_requested {
            Material::Complete(HeaderEnvelope {
                rlp: Some(alloy_rlp::encode(header)),
                transactions_root: Some(BlockHash::new(header.transactions_root.0)),
                receipts_root: Some(BlockHash::new(header.receipts_root.0)),
                withdrawals_root: header.withdrawals_root.map(|hash| BlockHash::new(hash.0)),
                gas_limit: Some(header.gas_limit),
                gas_used: Some(header.gas_used),
                base_fee_per_gas: header.base_fee_per_gas.map(U256::from).map(quantity),
                blob_gas_used: header.blob_gas_used,
                excess_blob_gas: header.excess_blob_gas,
                size_bytes: block_size.map(|size| u64::try_from(size).unwrap_or(u64::MAX)),
                consensus_size_bytes: None,
                transaction_count: body_present
                    .then(|| u32::try_from(body.transactions.len()).unwrap_or(u32::MAX)),
            })
        } else {
            Material::Missing(MissingReason::NotRequested)
        },
        transactions: normalized_material(
            normalized_transactions,
            transactions_requested,
            transaction_filtered,
            scope,
        ),
        receipts: normalized_material(
            normalized_receipts,
            receipts_requested,
            transaction_filtered,
            scope,
        ),
        logs: normalized_material(normalized_logs, logs_requested, logs_filtered, scope),
        withdrawals: normalized_material(withdrawals, withdrawals_requested, false, None),
        blob_sidecars: Material::Missing(MissingReason::Unsupported),
        traces: Material::Missing(MissingReason::Unsupported),
        state_diffs: Material::Missing(MissingReason::Unsupported),
        provenance: vec![Provenance {
            source_id: source.descriptor.id.clone(),
            source_kind: SourceKind::HistoryArchive,
            trust: TrustModel::TrustedDataset,
            range: Some(BlockRange::single(BlockNumber(header.number))),
            object: Some(ObjectIdentity {
                locator: object.label.clone(),
                // The catalog's digest names the object version. Sparse reads
                // never recompute it, so it is not a verified checksum.
                version: Some(format!("sha256:{}", hex::encode(object.checksum))),
                checksum: None,
                schema: Some("erae/e2store".to_owned()),
            }),
            observed_at_unix_ms,
            projection: projection
                .required
                .iter()
                .map(|capability| format!("{capability:?}").to_lowercase())
                .collect(),
        }],
        verification: VerificationReport {
            // The hash is computed from the archived header itself; nothing
            // independent anchors it.
            header_hash: VerificationCheck {
                status: CheckStatus::NotChecked,
                detail: Some(
                    "computed from the archived header; no consensus anchor was checked".to_owned(),
                ),
            },
            parent_continuity: if parent_checked || header.number == 0 {
                VerificationCheck::VERIFIED
            } else {
                VerificationCheck::UNAVAILABLE
            },
            transactions_root: if body_present {
                VerificationCheck::VERIFIED
            } else {
                VerificationCheck::NOT_CHECKED
            },
            receipts_root: if receipts.is_some() {
                VerificationCheck::VERIFIED
            } else {
                VerificationCheck::NOT_CHECKED
            },
            withdrawals_root: if header.withdrawals_root.is_some() {
                if body_present {
                    VerificationCheck::VERIFIED
                } else {
                    VerificationCheck::NOT_CHECKED
                }
            } else {
                VerificationCheck {
                    status: CheckStatus::Unavailable,
                    detail: Some("blocks before Shanghai commit to no withdrawals".to_owned()),
                }
            },
            dataset_checksum: VerificationCheck {
                status: CheckStatus::Unavailable,
                detail: Some(
                    "sparse reads verify execution roots; full-object SHA-256 was not downloaded"
                        .to_owned(),
                ),
            },
            consensus_anchor: None,
        },
    })
}

fn source_log_matches(scope: Option<&FilterScope>, log: &alloy_primitives::Log) -> bool {
    scope.is_none_or(|scope| {
        (scope.addresses.is_empty() || scope.addresses.contains(&address(log.address)))
            && scope.topics.iter().all(|filter| {
                log.data
                    .topics()
                    .get(usize::from(filter.position))
                    .is_some_and(|topic| filter.alternatives.contains(&topic.0))
            })
    })
}

fn normalized_material<T>(
    value: T,
    requested: bool,
    filtered: bool,
    scope: Option<&FilterScope>,
) -> Material<T> {
    if !requested {
        Material::Missing(MissingReason::NotRequested)
    } else if filtered {
        Material::Filtered {
            value,
            scope: scope.cloned().unwrap_or_default(),
            completeness: Completeness::VerifiedPredicate,
        }
    } else {
        Material::Complete(value)
    }
}
