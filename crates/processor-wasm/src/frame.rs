//! Project a source frame to exactly what a manifest requires.
//!
//! Sources differ in what they supply beyond a requirement: a P2P frame has
//! every receipt, a filtered archive only the matching logs, and optional
//! fields vary. The guest sees the same block from all of them: only the
//! required materials and field groups, reduced to the manifest filter the
//! way sources apply it (a log matches when its transaction matches the
//! transaction predicates and it matches the log predicates).

use std::collections::BTreeSet;

use leani_primitives::{BlockFrame, Capability, CapabilitySet, FilterScope};
use leani_processor_api::{DataRequirement, ProcessorError};
use leani_wasm_guest as abi;

use crate::{capability, requirement, topic_filters};

#[derive(Clone, Debug)]
pub(crate) struct Projection {
    capabilities: CapabilitySet,
    /// The declared capabilities plus the field groups their bundles stand for.
    groups: CapabilitySet,
    log_transaction_hash: bool,
    filter: FilterScope,
}

const TRANSACTION_GROUPS: [Capability; 5] = [
    Capability::Body,
    Capability::Transactions,
    Capability::Calldata,
    Capability::Senders,
    Capability::BlobFields,
];

impl Projection {
    pub(crate) fn new(requires: &abi::Requirements) -> Result<Self, String> {
        let declared = requires
            .capabilities
            .iter()
            .map(|required| capability(*required))
            .collect::<CapabilitySet>();
        let groups = declared.with_derivable();
        // The block's number, hash, and timestamp still need a frame.
        let capabilities = if declared == CapabilitySet::NONE {
            CapabilitySet::of(Capability::Header)
        } else {
            declared
        };
        let filter = FilterScope {
            addresses: requires
                .filter
                .addresses
                .iter()
                .copied()
                .map(Into::into)
                .collect(),
            topics: topic_filters(&requires.filter),
            transaction_types: requires.filter.transaction_types.clone(),
            senders: requires
                .filter
                .senders
                .iter()
                .copied()
                .map(Into::into)
                .collect(),
            recipients: requires
                .filter
                .recipients
                .iter()
                .copied()
                .map(Into::into)
                .collect(),
            ..FilterScope::default()
        };
        let has_transactions = TRANSACTION_GROUPS
            .iter()
            .any(|group| groups.contains(*group));
        if filter.constrains_transactions() && !has_transactions {
            return Err(
                "transaction filters need transaction material: require body, calldata, senders, or transactions"
                    .to_owned(),
            );
        }
        if !filter.senders.is_empty() && !groups.contains(Capability::Senders) {
            return Err("a sender filter needs the senders capability".to_owned());
        }
        if !filter.recipients.is_empty() && !groups.contains(Capability::Body) {
            return Err("a recipient filter needs the body capability".to_owned());
        }
        if filter.constrains_logs() && !groups.contains(Capability::Logs) {
            return Err(
                "address and topic filters select logs: require logs or receipts".to_owned(),
            );
        }
        if requires.log_transaction_hash && !groups.contains(Capability::Logs) {
            return Err("log transaction hashes need logs or receipts".to_owned());
        }
        Ok(Self {
            capabilities,
            groups,
            log_transaction_hash: requires.log_transaction_hash,
            filter,
        })
    }

    pub(crate) fn requirement(&self) -> DataRequirement {
        requirement(
            self.capabilities,
            self.log_transaction_hash,
            self.filter.clone(),
        )
    }

    pub(crate) fn project(&self, frame: &BlockFrame) -> Result<abi::Block, ProcessorError> {
        let groups = self.groups;
        let has = |capability| groups.contains(capability);
        // Block order, whatever order a source delivered.
        let transactions = TRANSACTION_GROUPS
            .iter()
            .any(|group| has(*group))
            .then(|| self.transactions(frame))
            .transpose()?
            .map(|mut transactions| {
                transactions.sort_by_key(|transaction| transaction.index);
                transactions
            });
        // Receipts and logs belong to matching transactions only.
        let kept = transactions.as_ref().and_then(|transactions| {
            self.filter.constrains_transactions().then(|| {
                transactions
                    .iter()
                    .map(|transaction| transaction.index)
                    .collect::<BTreeSet<_>>()
            })
        });
        let kept_index = |index: u32| kept.as_ref().is_none_or(|kept| kept.contains(&index));
        let receipts = (has(Capability::Receipts) || has(Capability::ReceiptOutcome))
            .then(|| {
                let receipts = required(frame.receipts.as_present(), "receipts")?;
                let mut receipts = receipts
                    .iter()
                    .filter(|receipt| kept_index(receipt.transaction_index))
                    .map(|receipt| self.receipt(receipt))
                    .collect::<Vec<_>>();
                receipts.sort_by_key(|receipt| receipt.transaction_index);
                Ok::<_, ProcessorError>(receipts)
            })
            .transpose()?;
        let logs = has(Capability::Logs)
            .then(|| {
                let logs: Vec<&leani_primitives::Log> = match frame.logs.as_present() {
                    Some(logs) => logs.iter().collect(),
                    // Receipts losslessly derive the block's logs.
                    None => required(frame.receipts.as_present(), "logs")?
                        .iter()
                        .flat_map(|receipt| &receipt.logs)
                        .collect(),
                };
                let mut logs = logs
                    .into_iter()
                    .filter(|log| kept_index(log.transaction_index))
                    .filter(|log| self.filter.matches_log(&log.address, &log.topics))
                    .map(|log| self.log(log))
                    .collect::<Vec<_>>();
                logs.sort_by_key(|log| log.log_index);
                Ok::<_, ProcessorError>(logs)
            })
            .transpose()?;
        Ok(abi::Block {
            chain_id: frame.chain_id.0,
            number: frame.block.number.0,
            hash: frame.block.hash.0,
            parent_hash: frame.block.parent_hash.0,
            timestamp: frame.block.timestamp,
            header: (has(Capability::Header) || has(Capability::BlockSize))
                .then(|| self.header(frame))
                .flatten(),
            transactions,
            receipts,
            logs,
            withdrawals: has(Capability::Withdrawals)
                .then(|| {
                    required(frame.withdrawals.as_present(), "withdrawals").map(|withdrawals| {
                        withdrawals
                            .iter()
                            .map(|withdrawal| abi::Withdrawal {
                                index: withdrawal.index,
                                validator_index: withdrawal.validator_index,
                                address: withdrawal.address.0,
                                amount_gwei: withdrawal.amount_gwei,
                            })
                            .collect()
                    })
                })
                .transpose()?,
        })
    }

    fn header(&self, frame: &BlockFrame) -> Option<abi::Header> {
        let header = frame.header.as_present()?;
        let fields = self.groups.contains(Capability::Header);
        let size = self.groups.contains(Capability::BlockSize);
        Some(abi::Header {
            gas_limit: header.gas_limit.filter(|_| fields),
            gas_used: header.gas_used.filter(|_| fields),
            base_fee_per_gas: header.base_fee_per_gas.map(|fee| fee.0).filter(|_| fields),
            blob_gas_used: header.blob_gas_used.filter(|_| fields),
            excess_blob_gas: header.excess_blob_gas.filter(|_| fields),
            size_bytes: header.size_bytes.filter(|_| size),
            transaction_count: header.transaction_count.filter(|_| size),
        })
    }

    fn transactions(&self, frame: &BlockFrame) -> Result<Vec<abi::Transaction>, ProcessorError> {
        let has = |capability| self.groups.contains(capability);
        let (body, calldata, senders, blobs) = (
            has(Capability::Body),
            has(Capability::Calldata),
            has(Capability::Senders),
            has(Capability::BlobFields),
        );
        Ok(required(frame.transactions.as_present(), "transactions")?
            .iter()
            .filter(|transaction| {
                self.filter.matches_transaction(
                    transaction.transaction_type,
                    transaction.to.as_ref(),
                    || transaction.hash,
                ) && self.filter.matches_sender(transaction.from.as_ref())
            })
            .map(|transaction| abi::Transaction {
                hash: transaction.hash.0,
                index: transaction.index,
                transaction_type: transaction.transaction_type,
                from: transaction.from.map(|from| from.0).filter(|_| senders),
                to: transaction.to.map(|to| to.0).filter(|_| body),
                nonce: transaction.nonce.filter(|_| body),
                gas_limit: transaction.gas_limit.filter(|_| body),
                value: transaction.value.map(|value| value.0).filter(|_| body),
                input: transaction.input.clone().filter(|_| calldata),
                max_fee_per_blob_gas: transaction
                    .max_fee_per_blob_gas
                    .map(|fee| fee.0)
                    .filter(|_| blobs),
                blob_versioned_hashes: if blobs {
                    transaction
                        .blob_versioned_hashes
                        .iter()
                        .map(|hash| hash.0)
                        .collect()
                } else {
                    Vec::new()
                },
            })
            .collect())
    }

    fn receipt(&self, receipt: &leani_primitives::ReceiptEnvelope) -> abi::Receipt {
        let has = |capability| self.groups.contains(capability);
        let outcome = has(Capability::ReceiptOutcome);
        let blobs = has(Capability::BlobFields);
        abi::Receipt {
            transaction_hash: receipt.transaction_hash.0,
            transaction_index: receipt.transaction_index,
            transaction_type: receipt.transaction_type,
            success: receipt.success.filter(|_| outcome),
            gas_used: receipt.gas_used.filter(|_| outcome),
            effective_gas_price: receipt
                .effective_gas_price
                .map(|price| price.0)
                .filter(|_| outcome),
            blob_gas_used: receipt.blob_gas_used.filter(|_| blobs),
            logs: if has(Capability::Receipts) {
                receipt
                    .logs
                    .iter()
                    .filter(|log| self.filter.matches_log(&log.address, &log.topics))
                    .map(|log| self.log(log))
                    .collect()
            } else {
                Vec::new()
            },
        }
    }

    fn log(&self, log: &leani_primitives::Log) -> abi::Log {
        abi::Log {
            address: log.address.0,
            topics: log.topics.clone(),
            data: log.data.clone(),
            transaction_hash: log
                .transaction_hash
                .map(|hash| hash.0)
                .filter(|_| self.log_transaction_hash),
            transaction_index: log.transaction_index,
            log_index: log.log_index,
        }
    }
}

fn required<'a, T>(material: Option<&'a T>, name: &str) -> Result<&'a T, ProcessorError> {
    material.ok_or_else(|| ProcessorError::Input(format!("frame lacks required {name}")))
}
