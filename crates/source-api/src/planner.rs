//! Pure capability, trust, finality, and range selection.

use std::cmp::Ordering;

use leani_primitives::{BlockNumber, BlockRange, SourceId, TrustModel};
use thiserror::Error;

use crate::{DataRequest, SourceDescriptor, VerificationPolicy};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SelectionPolicy {
    pub minimum_trust: TrustModel,
    pub prefer_complete: bool,
}

impl Default for SelectionPolicy {
    fn default() -> Self {
        Self {
            minimum_trust: TrustModel::TrustedDataset,
            prefer_complete: true,
        }
    }
}

/// Select the lowest-transfer viable source deterministically.
///
/// The returned descriptor is borrowed from `sources`; no source is opened.
///
/// # Errors
///
/// Returns [`PlanError`] with per-source reasons when no descriptor can satisfy
/// the complete request.
pub fn select_source<'a>(
    sources: &'a [SourceDescriptor],
    request: &DataRequest,
    policy: SelectionPolicy,
) -> Result<&'a SourceDescriptor, PlanError> {
    let mut viable = Vec::new();
    let mut rejected = Vec::new();
    for source in sources {
        let reason = rejection_reason(source, request, policy);
        if let Some(reason) = reason {
            rejected.push((source.id.clone(), reason));
        } else {
            viable.push(source);
        }
    }
    viable.sort_by(|left, right| compare_sources(left, right, request, policy));
    viable
        .into_iter()
        .next()
        .ok_or(PlanError::NoViableSource { rejected })
}

fn rejection_reason(
    source: &SourceDescriptor,
    request: &DataRequest,
    policy: SelectionPolicy,
) -> Option<String> {
    if source.chain_id != request.chain_id {
        return Some("wrong chain".to_owned());
    }
    if let Some(range) = source.range
        && (range.start().0 > request.range.start().0 || range.end().0 < request.range.end().0)
    {
        return Some("range is not fully covered".to_owned());
    }
    let supplied = if request.allow_filtered {
        source.capabilities
    } else {
        source.complete_capabilities
    }
    .with_derivable();
    if !supplied.contains_all(request.required) {
        return Some(format!(
            "missing capabilities 0x{:04x}",
            request.required.bits() & !supplied.bits()
        ));
    }
    if !source.finality.supports(request.minimum_finality) {
        return Some("minimum finality is unavailable".to_owned());
    }
    let required_trust = match request.verification_policy {
        VerificationPolicy::CompleteCryptographic => TrustModel::ProtocolVerified,
        VerificationPolicy::TrustedDataset => TrustModel::TrustedDataset,
        VerificationPolicy::BestEffort => TrustModel::Untrusted,
    };
    if source.trust < required_trust || source.trust < policy.minimum_trust {
        return Some("trust policy is not met".to_owned());
    }
    None
}

fn compare_sources(
    left: &SourceDescriptor,
    right: &SourceDescriptor,
    request: &DataRequest,
    policy: SelectionPolicy,
) -> Ordering {
    if policy.prefer_complete {
        let left_complete = left
            .complete_capabilities
            .with_derivable()
            .contains_all(request.required);
        let right_complete = right
            .complete_capabilities
            .with_derivable()
            .contains_all(request.required);
        match right_complete.cmp(&left_complete) {
            Ordering::Equal => {}
            ordering => return ordering,
        }
    }
    left.priority
        .cmp(&right.priority)
        .then_with(|| left.expected_lag.cmp(&right.expected_lag))
        .then_with(|| left.id.cmp(&right.id))
}

/// Return uncovered inclusive ranges after normalizing arbitrary intervals.
#[must_use]
pub fn coverage_gaps(requested: BlockRange, covered: &[BlockRange]) -> Vec<BlockRange> {
    let mut ranges = covered
        .iter()
        .copied()
        .filter(|range| {
            range.end().0 >= requested.start().0 && range.start().0 <= requested.end().0
        })
        .collect::<Vec<_>>();
    ranges.sort_by_key(|range| range.start().0);

    let mut gaps = Vec::new();
    let mut next = requested.start().0;
    for range in ranges {
        let start = range.start().0.max(requested.start().0);
        let end = range.end().0.min(requested.end().0);
        if start > next
            && let Ok(gap) = BlockRange::new(BlockNumber(next), BlockNumber(start - 1))
        {
            gaps.push(gap);
        }
        next = next.max(end.saturating_add(1));
        if next > requested.end().0 {
            break;
        }
    }
    if next <= requested.end().0
        && let Ok(gap) = BlockRange::new(BlockNumber(next), requested.end())
    {
        gaps.push(gap);
    }
    gaps
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum PlanError {
    #[error("no source can satisfy the request: {rejected:?}")]
    NoViableSource { rejected: Vec<(SourceId, String)> },
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use leani_primitives::{Capability, CapabilitySet, ChainId, Finality, SourceKind};

    use super::*;
    use crate::{FieldProjection, FilterSet, FinalityModel, Partitioning};

    fn request() -> DataRequest {
        DataRequest {
            chain_id: ChainId(1),
            range: BlockRange::new(BlockNumber(10), BlockNumber(20)).expect("range"),
            required: CapabilitySet::of(Capability::Logs),
            allow_filtered: false,
            projection: FieldProjection::default(),
            log_fields: leani_primitives::LogFieldSet::ALL,
            filters: FilterSet::default(),
            minimum_finality: Finality::Finalized,
            verification_policy: VerificationPolicy::TrustedDataset,
        }
    }

    fn source(id: &str, complete: CapabilitySet) -> SourceDescriptor {
        SourceDescriptor {
            id: SourceId::new(id).expect("ID"),
            kind: SourceKind::Synthetic,
            chain_id: ChainId(1),
            range: Some(BlockRange::new(BlockNumber(1), BlockNumber(100)).expect("range")),
            capabilities: CapabilitySet::of(Capability::Logs),
            complete_capabilities: complete,
            trust: TrustModel::TrustedDataset,
            finality: FinalityModel::Finalized,
            partitioning: Partitioning::None,
            expected_lag: Duration::ZERO,
            schema_version: "1".to_owned(),
            priority: 1,
        }
    }

    #[test]
    fn impossible_complete_requirement_is_rejected_before_open() {
        let source = source("filtered", CapabilitySet::NONE);
        let error = select_source(&[source], &request(), SelectionPolicy::default())
            .expect_err("filtered material is insufficient");
        assert!(matches!(error, PlanError::NoViableSource { .. }));
    }

    #[test]
    fn gaps_merge_overlaps_and_adjacency() {
        let requested = BlockRange::new(BlockNumber(10), BlockNumber(20)).expect("requested range");
        let covered = [
            BlockRange::new(BlockNumber(9), BlockNumber(12)).expect("range"),
            BlockRange::new(BlockNumber(12), BlockNumber(15)).expect("range"),
            BlockRange::new(BlockNumber(18), BlockNumber(30)).expect("range"),
        ];
        assert_eq!(
            coverage_gaps(requested, &covered),
            vec![BlockRange::new(BlockNumber(16), BlockNumber(17)).expect("gap")]
        );
    }
}
