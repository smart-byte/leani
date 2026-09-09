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
/// Body and calldata capabilities select the canonical transaction material
/// from which they are derived. Finality and consensus-anchor evidence are
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
        } else if !frame
            .capabilities()
            .complete
            .with_derivable()
            .contains(capability)
        {
            return Err(ConformanceError::IncompleteMaterial {
                block_number: frame.block.number,
                capability,
            });
        }
    }

    let mut normalized = frame.clone();
    if !capabilities.contains(Capability::Header) {
        normalized.header = Material::Missing(MissingReason::NotRequested);
    }
    if !capabilities.contains(Capability::Transactions)
        && !capabilities.contains(Capability::Body)
        && !capabilities.contains(Capability::Calldata)
    {
        normalized.transactions = Material::Missing(MissingReason::NotRequested);
    }
    if !capabilities.contains(Capability::Receipts) {
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
        normalized.finality = Finality::Included;
        normalized.verification = VerificationReport::default();
    }
    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use leani_primitives::{
        BlockHash, BlockRef, ChainId, CheckStatus, Log, SourceId, SourceKind, TrustModel,
        VerificationCheck,
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
}
