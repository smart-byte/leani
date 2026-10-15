//! Source-neutral normalized-frame comparison helpers.

use leani_primitives::{
    BlockFrame, BlockNumber, Capability, CapabilitySet, Finality, Material, MissingReason,
    VerificationReport,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Stable digest of the canonical material selected for one block.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FrameFingerprint {
    pub block_number: BlockNumber,
    pub digest: [u8; 32],
}

/// One source disagreement found by [`compare_frame_sequences`].
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum FrameDifference {
    FrameCount {
        left: usize,
        right: usize,
    },
    InvalidFrame {
        source: String,
        index: usize,
        detail: String,
    },
    BlockIdentity {
        index: usize,
        left: leani_primitives::BlockRef,
        right: leani_primitives::BlockRef,
    },
    Material {
        block_number: BlockNumber,
        left_digest: [u8; 32],
        right_digest: [u8; 32],
    },
}

/// Machine-readable result of comparing two normalized source projections.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CrossSourceReport {
    pub left_source: String,
    pub right_source: String,
    pub capabilities: CapabilitySet,
    pub compared_frames: usize,
    pub differences: Vec<FrameDifference>,
}

impl CrossSourceReport {
    /// Whether the two source projections are canonically equivalent.
    #[must_use]
    pub fn is_equivalent(&self) -> bool {
        self.differences.is_empty()
    }
}

/// A frame cannot be reduced to the requested source-neutral comparison view.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ConformanceError {
    #[error("invalid normalized frame: {0}")]
    InvalidFrame(&'static str),
    #[error("frame {block_number} lacks complete {capability:?} material")]
    IncompleteMaterial {
        block_number: BlockNumber,
        capability: Capability,
    },
    #[error("mempool material is not represented by a block frame")]
    UnsupportedMempool,
    #[error("failed to encode normalized comparison frame")]
    Encoding,
}

/// Fingerprint only canonical material, excluding source provenance and
/// transport-specific verification evidence.
///
/// A field group selects the whole component holding its fields, and those
/// fields must be filled. Finality and consensus-anchor evidence are
/// compared only when `ConsensusFinality` is selected.
///
/// # Errors
///
/// Returns an error for an invalid frame, incomplete requested material,
/// unsupported mempool comparison, or deterministic encoding failure.
pub fn frame_fingerprint(
    frame: &BlockFrame,
    capabilities: CapabilitySet,
) -> Result<FrameFingerprint, ConformanceError> {
    let normalized = comparison_frame(frame, capabilities)?;
    let bytes = postcard::to_allocvec(&normalized).map_err(|_| ConformanceError::Encoding)?;
    Ok(FrameFingerprint {
        block_number: frame.block.number,
        digest: *blake3::hash(&bytes).as_bytes(),
    })
}

/// Compare ordered source projections after stripping source-specific
/// provenance and verification evidence.
///
/// Invalid frames and missing complete material are reported as differences
/// instead of aborting the remaining comparison.
#[must_use]
pub fn compare_frame_sequences(
    left_source: impl Into<String>,
    left: &[BlockFrame],
    right_source: impl Into<String>,
    right: &[BlockFrame],
    capabilities: CapabilitySet,
) -> CrossSourceReport {
    let left_source = left_source.into();
    let right_source = right_source.into();
    let mut differences = Vec::new();
    if left.len() != right.len() {
        differences.push(FrameDifference::FrameCount {
            left: left.len(),
            right: right.len(),
        });
    }
    let mut compared_frames = 0;
    for (index, (left_frame, right_frame)) in left.iter().zip(right).enumerate() {
        if left_frame.block != right_frame.block {
            differences.push(FrameDifference::BlockIdentity {
                index,
                left: left_frame.block,
                right: right_frame.block,
            });
            continue;
        }
        let left_fingerprint = match frame_fingerprint(left_frame, capabilities) {
            Ok(fingerprint) => fingerprint,
            Err(error) => {
                differences.push(FrameDifference::InvalidFrame {
                    source: left_source.clone(),
                    index,
                    detail: error.to_string(),
                });
                continue;
            }
        };
        let right_fingerprint = match frame_fingerprint(right_frame, capabilities) {
            Ok(fingerprint) => fingerprint,
            Err(error) => {
                differences.push(FrameDifference::InvalidFrame {
                    source: right_source.clone(),
                    index,
                    detail: error.to_string(),
                });
                continue;
            }
        };
        compared_frames += 1;
        if left_fingerprint.digest != right_fingerprint.digest {
            differences.push(FrameDifference::Material {
                block_number: left_frame.block.number,
                left_digest: left_fingerprint.digest,
                right_digest: right_fingerprint.digest,
            });
        }
    }
    CrossSourceReport {
        left_source,
        right_source,
        capabilities,
        compared_frames,
        differences,
    }
}

fn comparison_frame(
    frame: &BlockFrame,
    capabilities: CapabilitySet,
) -> Result<BlockFrame, ConformanceError> {
    frame
        .validate_shape()
        .map_err(ConformanceError::InvalidFrame)?;
    if capabilities.contains(Capability::Mempool) {
        return Err(ConformanceError::UnsupportedMempool);
    }
    for capability in capabilities.iter() {
        if capability == Capability::ConsensusFinality {
            if frame.verification.consensus_anchor.is_none() {
                return Err(ConformanceError::IncompleteMaterial {
                    block_number: frame.block.number,
                    capability,
                });
            }
        } else {
            let selected = CapabilitySet::of(capability);
            if !frame.capabilities().satisfies(selected, false)
                || frame.group_violation(selected.with_derivable()).is_some()
            {
                return Err(ConformanceError::IncompleteMaterial {
                    block_number: frame.block.number,
                    capability,
                });
            }
        }
    }

    // A selected field group compares the whole component holding its fields.
    let selects = |group: &[Capability]| group.iter().any(|group| capabilities.contains(*group));
    let mut normalized = frame.clone();
    if !selects(&[Capability::Header, Capability::BlockSize]) {
        normalized.header = Material::Missing(MissingReason::NotRequested);
    }
    if !selects(&[
        Capability::Transactions,
        Capability::Body,
        Capability::Calldata,
        Capability::Senders,
        Capability::BlobFields,
    ]) {
        normalized.transactions = Material::Missing(MissingReason::NotRequested);
    }
    if !selects(&[Capability::Receipts, Capability::ReceiptOutcome]) {
        normalized.receipts = Material::Missing(MissingReason::NotRequested);
    }
    if !capabilities.contains(Capability::Logs) {
        normalized.logs = Material::Missing(MissingReason::NotRequested);
    }
    if !capabilities.contains(Capability::Withdrawals) {
        normalized.withdrawals = Material::Missing(MissingReason::NotRequested);
    }
    if !capabilities.contains(Capability::BlobSidecars) {
        normalized.blob_sidecars = Material::Missing(MissingReason::NotRequested);
    }
    if !capabilities.contains(Capability::Traces) {
        normalized.traces = Material::Missing(MissingReason::NotRequested);
    }
    if !capabilities.contains(Capability::StateDiffs) {
        normalized.state_diffs = Material::Missing(MissingReason::NotRequested);
    }
    normalized.provenance.clear();
    if capabilities.contains(Capability::ConsensusFinality) {
        let consensus_anchor = normalized.verification.consensus_anchor.take();
        normalized.verification = VerificationReport {
            consensus_anchor,
            ..VerificationReport::default()
        };
    } else {
        normalized.finality = Finality::Attested;
        normalized.verification = VerificationReport::default();
    }
    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use leani_primitives::{
        BlockHash, BlockRef, ChainId, CheckStatus, Log, SourceId, SourceKind, TransactionEnvelope,
        TrustModel, VerificationCheck,
    };

    use super::*;

    fn fixture_frame(number: u64, parent_hash: BlockHash) -> BlockFrame {
        BlockFrame {
            chain_id: ChainId(1),
            block: BlockRef {
                number: BlockNumber(number),
                hash: BlockHash::new([u8::try_from(number).unwrap_or(u8::MAX); 32]),
                parent_hash,
                timestamp: 1_700_000_000 + number,
            },
            finality: Finality::Finalized,
            header: Material::Missing(MissingReason::NotRequested),
            transactions: Material::Complete(Vec::new()),
            receipts: Material::Complete(Vec::new()),
            logs: Material::Complete(Vec::new()),
            withdrawals: Material::Missing(MissingReason::Unsupported),
            blob_sidecars: Material::Missing(MissingReason::NotRequested),
            traces: Material::Missing(MissingReason::Unsupported),
            state_diffs: Material::Missing(MissingReason::Unsupported),
            provenance: Vec::new(),
            verification: VerificationReport::default(),
        }
    }

    #[test]
    fn transport_evidence_is_excluded_from_material_comparison() {
        let left = fixture_frame(1, leani_primitives::BlockHash::ZERO);
        let mut right = left.clone();
        right.finality = Finality::Finalized;
        right.provenance.push(leani_primitives::Provenance {
            source_id: SourceId::new("other-source").expect("source ID"),
            source_kind: SourceKind::PublicDataset,
            trust: TrustModel::TrustedDataset,
            range: None,
            object: None,
            observed_at_unix_ms: 42,
            projection: vec!["logs".to_owned()],
        });
        right.verification.dataset_checksum = VerificationCheck {
            status: CheckStatus::Verified,
            detail: Some("transport checksum".to_owned()),
        };
        let report = compare_frame_sequences(
            "xatu",
            &[left],
            "erae",
            &[right],
            CapabilitySet::of(Capability::Logs),
        );
        assert!(report.is_equivalent(), "{report:?}");
        assert_eq!(report.compared_frames, 1);
    }

    #[test]
    fn selected_material_disagreement_is_reported() {
        let left = fixture_frame(1, leani_primitives::BlockHash::ZERO);
        let mut right = left.clone();
        right.logs = Material::Complete(vec![Log {
            address: leani_primitives::Address::new([1; 20]),
            topics: Vec::new(),
            data: vec![7],
            transaction_hash: Some(leani_primitives::TransactionHash::new([2; 32])),
            transaction_index: 0,
            log_index: 0,
        }]);
        let report = compare_frame_sequences(
            "left",
            &[left],
            "right",
            &[right],
            CapabilitySet::of(Capability::Logs),
        );
        assert!(matches!(
            report.differences.as_slice(),
            [FrameDifference::Material {
                block_number: BlockNumber(1),
                ..
            }]
        ));
    }

    #[test]
    fn incomplete_selected_material_fails_closed() {
        let frame = fixture_frame(1, leani_primitives::BlockHash::ZERO);
        let error = frame_fingerprint(&frame, CapabilitySet::of(Capability::Header))
            .expect_err("header is absent");
        assert_eq!(
            error,
            ConformanceError::IncompleteMaterial {
                block_number: BlockNumber(1),
                capability: Capability::Header,
            }
        );
    }

    /// A block whose only transaction, a type-3 one, fills every field group.
    fn filled_frame() -> BlockFrame {
        use leani_primitives::{
            Address, HeaderEnvelope, Quantity, ReceiptEnvelope, TransactionHash,
        };
        let hash = TransactionHash::new([7; 32]);
        let mut frame = fixture_frame(1, BlockHash::ZERO);
        frame.header = Material::Complete(HeaderEnvelope {
            rlp: None,
            transactions_root: None,
            receipts_root: None,
            withdrawals_root: None,
            gas_limit: Some(30_000_000),
            gas_used: Some(21_000),
            base_fee_per_gas: None,
            blob_gas_used: Some(131_072),
            excess_blob_gas: Some(0),
            size_bytes: Some(600),
            consensus_size_bytes: None,
            transaction_count: Some(1),
        });
        frame.transactions = Material::Complete(vec![TransactionEnvelope {
            hash,
            transaction_type: 3,
            index: 0,
            encoded: None,
            from: Some(Address::new([1; 20])),
            to: Some(Address::new([2; 20])),
            nonce: Some(0),
            gas_limit: Some(21_000),
            value: Some(Quantity::new([0; 32])),
            input: Some(Vec::new()),
            max_fee_per_gas: None,
            max_priority_fee_per_gas: None,
            max_fee_per_blob_gas: Some(Quantity::new([0; 32])),
            blob_versioned_hashes: vec![BlockHash::new([1; 32])],
            size_bytes: None,
        }]);
        frame.receipts = Material::Complete(vec![ReceiptEnvelope {
            transaction_hash: hash,
            transaction_type: 3,
            transaction_index: 0,
            encoded: None,
            success: Some(true),
            gas_used: Some(21_000),
            effective_gas_price: Some(Quantity::new([0; 32])),
            blob_gas_used: Some(131_072),
            blob_gas_price: None,
            logs: Vec::new(),
        }]);
        frame
    }

    fn change_transaction(frame: &mut BlockFrame, change: impl FnOnce(&mut TransactionEnvelope)) {
        if let Material::Complete(transactions) = &mut frame.transactions {
            change(&mut transactions[0]);
        }
    }

    #[test]
    fn field_group_disagreement_is_reported() {
        let left = filled_frame();
        let mut reverted = left.clone();
        if let Material::Complete(receipts) = &mut reverted.receipts {
            receipts[0].success = Some(false);
        }
        let mut other_sender = left.clone();
        change_transaction(&mut other_sender, |transaction| {
            transaction.from = Some(leani_primitives::Address::new([9; 20]));
        });
        let mut other_blob = left.clone();
        change_transaction(&mut other_blob, |transaction| {
            transaction.blob_versioned_hashes = vec![BlockHash::new([9; 32])];
        });
        let mut other_size = left.clone();
        if let Material::Complete(header) = &mut other_size.header {
            header.size_bytes = Some(601);
        }
        for (group, right) in [
            (Capability::ReceiptOutcome, reverted),
            (Capability::Senders, other_sender),
            (Capability::BlobFields, other_blob),
            (Capability::BlockSize, other_size),
        ] {
            let report = compare_frame_sequences(
                "left",
                std::slice::from_ref(&left),
                "right",
                &[right],
                CapabilitySet::of(group),
            );
            assert!(
                matches!(
                    report.differences.as_slice(),
                    [FrameDifference::Material { .. }]
                ),
                "{group:?}: {report:?}"
            );
        }

        // a group whose fields are unfilled is incomplete material
        let mut senderless = left.clone();
        change_transaction(&mut senderless, |transaction| transaction.from = None);
        assert_eq!(
            frame_fingerprint(&senderless, CapabilitySet::of(Capability::Senders)),
            Err(ConformanceError::IncompleteMaterial {
                block_number: BlockNumber(1),
                capability: Capability::Senders,
            })
        );
        // the header alone carries the block size
        let mut header_only = left;
        header_only.transactions = Material::Missing(MissingReason::NotRequested);
        header_only.receipts = Material::Missing(MissingReason::NotRequested);
        assert!(frame_fingerprint(&header_only, CapabilitySet::of(Capability::BlockSize)).is_ok());
    }
}
