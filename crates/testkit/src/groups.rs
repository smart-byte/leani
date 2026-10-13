//! Field-group conformance: a processor reads only the fields its declared
//! groups promise.

use leani_primitives::{BlockFrame, Capability, CapabilitySet, Material, MissingReason};

/// A copy of `frame` without every field `groups` (after derivation) does not
/// promise.
///
/// A processor that reads only what it declares maps the copy exactly like
/// `frame`: compare the two deltas in a conformance test.
#[must_use]
pub fn restrict_to_groups(frame: &BlockFrame, groups: CapabilitySet) -> BlockFrame {
    let groups = groups.with_derivable();
    let keeps = |group| groups.contains(group);
    let not_requested = || MissingReason::NotRequested;
    let mut restricted = frame.clone();
    restricted.header = if keeps(Capability::Header) {
        frame.header.clone().map(|mut header| {
            header.rlp = None;
            header.transactions_root = None;
            header.receipts_root = None;
            header.withdrawals_root = None;
            header.consensus_size_bytes = None;
            if !keeps(Capability::BlockSize) {
                header.size_bytes = None;
                if !keeps(Capability::Body) {
                    header.transaction_count = None;
                }
            }
            header
        })
    } else {
        Material::Missing(not_requested())
    };
    let transaction_groups = [
        Capability::Body,
        Capability::Calldata,
        Capability::Senders,
        Capability::BlobFields,
    ];
    restricted.transactions = if transaction_groups.into_iter().any(keeps) {
        frame.transactions.clone().map(|mut transactions| {
            for transaction in &mut transactions {
                transaction.encoded = None;
                transaction.max_fee_per_gas = None;
                transaction.max_priority_fee_per_gas = None;
                transaction.size_bytes = None;
                if !keeps(Capability::Senders) {
                    transaction.from = None;
                }
                if !keeps(Capability::Calldata) {
                    transaction.input = None;
                }
                if !keeps(Capability::Body) {
                    transaction.to = None;
                    transaction.nonce = None;
                    transaction.gas_limit = None;
                    transaction.value = None;
                }
                if !keeps(Capability::BlobFields) {
                    transaction.blob_versioned_hashes.clear();
                    transaction.max_fee_per_blob_gas = None;
                }
            }
            transactions
        })
    } else {
        Material::Missing(not_requested())
    };
    restricted.receipts = if keeps(Capability::ReceiptOutcome) {
        frame.receipts.clone().map(|mut receipts| {
            for receipt in &mut receipts {
                receipt.encoded = None;
                receipt.blob_gas_price = None;
                // only the whole receipt bundle promises receipt logs
                if !keeps(Capability::Receipts) {
                    receipt.logs.clear();
                }
                if !keeps(Capability::BlobFields) {
                    receipt.blob_gas_used = None;
                }
            }
            receipts
        })
    } else {
        Material::Missing(not_requested())
    };
    if !keeps(Capability::Logs) {
        restricted.logs = Material::Missing(not_requested());
    }
    if !keeps(Capability::Withdrawals) {
        restricted.withdrawals = Material::Missing(not_requested());
    }
    if !keeps(Capability::BlobSidecars) {
        restricted.blob_sidecars = Material::Missing(not_requested());
    }
    if !keeps(Capability::Traces) {
        restricted.traces = Material::Missing(not_requested());
    }
    if !keeps(Capability::StateDiffs) {
        restricted.state_diffs = Material::Missing(not_requested());
    }
    restricted
}

#[cfg(test)]
mod tests {
    use leani_primitives::{
        Address, BlockHash, Capability, HeaderEnvelope, Log, Material, MissingReason, Quantity,
        ReceiptEnvelope, TransactionEnvelope, TransactionHash,
    };

    use super::*;
    use crate::fixture_frame;

    fn quantity(value: u8) -> Quantity {
        let mut bytes = [0; 32];
        bytes[31] = value;
        Quantity::new(bytes)
    }

    /// A block whose only transaction, a type-3 one, fills every field.
    fn full_frame() -> BlockFrame {
        let hash = TransactionHash::new([7; 32]);
        let log = Log {
            address: Address::new([4; 20]),
            topics: vec![[5; 32]],
            data: vec![6],
            transaction_hash: Some(hash),
            transaction_index: 0,
            log_index: 0,
        };
        let mut frame = fixture_frame(42, BlockHash::new([41; 32]));
        frame.header = Material::Complete(HeaderEnvelope {
            rlp: Some(vec![1]),
            transactions_root: Some(BlockHash::new([2; 32])),
            receipts_root: Some(BlockHash::new([3; 32])),
            withdrawals_root: None,
            gas_limit: Some(30_000_000),
            gas_used: Some(21_000),
            base_fee_per_gas: Some(quantity(7)),
            blob_gas_used: Some(131_072),
            excess_blob_gas: Some(0),
            size_bytes: Some(600),
            consensus_size_bytes: Some(700),
            transaction_count: Some(1),
        });
        frame.transactions = Material::Complete(vec![TransactionEnvelope {
            hash,
            transaction_type: 3,
            index: 0,
            encoded: Some(vec![3]),
            from: Some(Address::new([1; 20])),
            to: Some(Address::new([2; 20])),
            nonce: Some(9),
            gas_limit: Some(21_000),
            value: Some(quantity(1)),
            input: Some(vec![0xab]),
            max_fee_per_gas: Some(quantity(10)),
            max_priority_fee_per_gas: Some(quantity(1)),
            max_fee_per_blob_gas: Some(quantity(2)),
            blob_versioned_hashes: vec![BlockHash::new([1; 32])],
            size_bytes: Some(200),
        }]);
        frame.receipts = Material::Complete(vec![ReceiptEnvelope {
            transaction_hash: hash,
            transaction_type: 3,
            transaction_index: 0,
            encoded: Some(vec![4]),
            success: Some(false),
            gas_used: Some(21_000),
            effective_gas_price: Some(quantity(8)),
            blob_gas_used: Some(131_072),
            blob_gas_price: Some(quantity(1)),
            logs: vec![log.clone()],
        }]);
        frame.logs = Material::Complete(vec![log]);
        frame
    }

    fn groups(capabilities: &[Capability]) -> CapabilitySet {
        capabilities.iter().copied().collect()
    }

    #[test]
    fn restriction_keeps_exactly_the_promised_fields() {
        use Capability::{
            BlobFields, BlockSize, Body, Calldata, Header, Logs, ReceiptOutcome, Receipts, Senders,
            Transactions,
        };
        let frame = full_frame();

        let senders = restrict_to_groups(&frame, groups(&[Header, Senders]));
        let transaction = &senders.transactions.as_present().expect("rows")[0];
        assert_eq!(transaction.from, Some(Address::new([1; 20])));
        assert_eq!(transaction.hash, TransactionHash::new([7; 32]));
        assert!(transaction.input.is_none() && transaction.to.is_none());
        assert!(transaction.nonce.is_none() && transaction.value.is_none());
        assert!(transaction.encoded.is_none() && transaction.max_fee_per_gas.is_none());
        assert!(transaction.blob_versioned_hashes.is_empty() && transaction.size_bytes.is_none());
        assert_eq!(
            senders.receipts,
            Material::Missing(MissingReason::NotRequested)
        );
        assert_eq!(senders.logs, Material::Missing(MissingReason::NotRequested));
        let header = senders.header.as_present().expect("header");
        assert_eq!(header.gas_used, Some(21_000));
        assert!(header.size_bytes.is_none() && header.transaction_count.is_none());
        assert!(header.rlp.is_none() && header.transactions_root.is_none());

        // without Header, nothing of the header is promised
        let body = restrict_to_groups(&frame, groups(&[Body]));
        assert_eq!(body.header, Material::Missing(MissingReason::NotRequested));

        let outcome = restrict_to_groups(&frame, groups(&[Header, ReceiptOutcome, BlobFields]));
        let receipt = &outcome.receipts.as_present().expect("receipts")[0];
        assert_eq!(receipt.success, Some(false));
        assert_eq!(receipt.blob_gas_used, Some(131_072));
        assert!(receipt.logs.is_empty() && receipt.encoded.is_none());
        assert!(receipt.blob_gas_price.is_none());

        // the bundles keep every field their groups promise
        let bundles = groups(&[Header, Transactions, Receipts]);
        let kept = restrict_to_groups(&frame, bundles);
        let transaction = &kept.transactions.as_present().expect("rows")[0];
        assert!(transaction.from.is_some() && transaction.input.is_some());
        assert_eq!(
            kept.receipts.as_present().expect("receipts")[0].logs.len(),
            1
        );
        assert_eq!(
            kept.header.as_present().expect("header").size_bytes,
            Some(600)
        );

        for restriction in [
            groups(&[Header, Senders]),
            groups(&[Header, Body, Senders, Calldata, ReceiptOutcome]),
            groups(&[Header, Senders, BlobFields, BlockSize, ReceiptOutcome]),
            groups(&[Body, Senders]),
            groups(&[Logs]),
            bundles,
        ] {
            assert_eq!(
                restrict_to_groups(&frame, restriction)
                    .group_violation(restriction.with_derivable()),
                None,
                "{restriction:?}"
            );
        }
    }
}
