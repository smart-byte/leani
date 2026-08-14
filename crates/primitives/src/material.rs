//! Material presence and completeness semantics.

use serde::{Deserialize, Serialize};

use crate::{Address, BlockRange, TransactionHash};

/// Exact predicate applied before source material reached the node.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct FilterScope {
    pub block_range: Option<BlockRange>,
    pub addresses: Vec<Address>,
    pub topics: Vec<TopicFilter>,
    pub transaction_hashes: Vec<TransactionHash>,
    pub transaction_types: Vec<u8>,
    #[serde(default)]
    pub senders: Vec<Address>,
    #[serde(default)]
    pub recipients: Vec<Address>,
}

/// Topic predicate at an exact log topic position.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TopicFilter {
    pub position: u8,
    pub alternatives: Vec<[u8; 32]>,
}

/// What the source can claim about the filtered result.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum Completeness {
    /// All values matching the recorded predicate are present and the claim was
    /// independently verified.
    VerifiedPredicate,
    /// The dataset declares all matching values are present; the node could not
    /// independently reconstruct the full committed material.
    DatasetDeclared,
    /// A best-effort subset that must never satisfy a processor requiring
    /// complete or predicate-complete material.
    Partial,
}

/// Why a component is absent. Empty complete vectors use `Complete(vec![])`.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum MissingReason {
    NotRequested,
    Unsupported,
    NotAvailable,
    TemporarilyUnavailable,
    Pruned,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaterialKind {
    Complete,
    Filtered,
    Missing,
}

/// Material state with no ambiguous `Option<T>`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum Material<T> {
    Complete(T),
    Filtered {
        value: T,
        scope: FilterScope,
        completeness: Completeness,
    },
    Missing(MissingReason),
}

impl<T> Material<T> {
    #[must_use]
    pub const fn kind(&self) -> MaterialKind {
        match self {
            Self::Complete(_) => MaterialKind::Complete,
            Self::Filtered { .. } => MaterialKind::Filtered,
            Self::Missing(_) => MaterialKind::Missing,
        }
    }

    #[must_use]
    pub const fn is_present(&self) -> bool {
        !matches!(self, Self::Missing(_))
    }

    #[must_use]
    pub const fn is_complete(&self) -> bool {
        matches!(self, Self::Complete(_))
    }

    #[must_use]
    pub const fn as_complete(&self) -> Option<&T> {
        match self {
            Self::Complete(value) => Some(value),
            Self::Filtered { .. } | Self::Missing(_) => None,
        }
    }

    #[must_use]
    pub const fn as_present(&self) -> Option<&T> {
        match self {
            Self::Complete(value) | Self::Filtered { value, .. } => Some(value),
            Self::Missing(_) => None,
        }
    }

    #[must_use]
    pub fn map<U>(self, mapper: impl FnOnce(T) -> U) -> Material<U> {
        match self {
            Self::Complete(value) => Material::Complete(mapper(value)),
            Self::Filtered {
                value,
                scope,
                completeness,
            } => Material::Filtered {
                value: mapper(value),
                scope,
                completeness,
            },
            Self::Missing(reason) => Material::Missing(reason),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_complete_is_not_missing_or_filtered() {
        let complete: Material<Vec<u8>> = Material::Complete(Vec::new());
        let missing: Material<Vec<u8>> = Material::Missing(MissingReason::NotRequested);
        let filtered = Material::Filtered {
            value: Vec::<u8>::new(),
            scope: FilterScope::default(),
            completeness: Completeness::DatasetDeclared,
        };

        assert_eq!(complete.kind(), MaterialKind::Complete);
        assert_eq!(missing.kind(), MaterialKind::Missing);
        assert_eq!(filtered.kind(), MaterialKind::Filtered);
        assert!(complete.as_complete().is_some());
        assert!(filtered.as_complete().is_none());
    }
}
