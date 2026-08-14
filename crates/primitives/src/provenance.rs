//! Source identity, trust declarations, and structured verification evidence.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{BlockHash, BlockRange, Finality};

#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct SourceId(String);

impl SourceId {
    /// Validate a stable source identifier.
    ///
    /// # Errors
    ///
    /// Returns [`SourceIdError`] for an empty, too-long, or non-portable ID.
    pub fn new(value: impl Into<String>) -> Result<Self, SourceIdError> {
        let value = value.into();
        if value.is_empty() || value.len() > 64 {
            return Err(SourceIdError(value));
        }
        if !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        {
            return Err(SourceIdError(value));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SourceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for SourceId {
    type Err = SourceIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("invalid source ID `{0}`; use 1-64 ASCII letters, digits, '.', '_' or '-'")]
pub struct SourceIdError(String);

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum SourceKind {
    PublicDataset,
    HistoryArchive,
    RetainedHistory,
    ExecutionP2p,
    ConsensusP2p,
    BeaconApi,
    Synthetic,
}

/// Trust needed beyond cryptographic checks represented in the report.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub enum TrustModel {
    Untrusted,
    TrustedManifest,
    TrustedDataset,
    ProtocolVerified,
}

/// Reproducible identity for one dataset or network response.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ObjectIdentity {
    pub locator: String,
    pub version: Option<String>,
    pub checksum: Option<[u8; 32]>,
    pub schema: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Provenance {
    pub source_id: SourceId,
    pub source_kind: SourceKind,
    pub trust: TrustModel,
    pub range: Option<BlockRange>,
    pub object: Option<ObjectIdentity>,
    pub observed_at_unix_ms: u64,
    pub projection: Vec<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum CheckStatus {
    Verified,
    Failed,
    NotChecked,
    Unavailable,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct VerificationCheck {
    pub status: CheckStatus,
    pub detail: Option<String>,
}

impl VerificationCheck {
    pub const VERIFIED: Self = Self {
        status: CheckStatus::Verified,
        detail: None,
    };
    pub const NOT_CHECKED: Self = Self {
        status: CheckStatus::NotChecked,
        detail: None,
    };
    pub const UNAVAILABLE: Self = Self {
        status: CheckStatus::Unavailable,
        detail: None,
    };

    #[must_use]
    pub const fn failed(&self) -> bool {
        matches!(self.status, CheckStatus::Failed)
    }
}

/// Consensus-derived execution anchor.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ConsensusAnchor {
    pub finality: Finality,
    pub execution_block_hash: BlockHash,
    pub beacon_slot: u64,
    pub beacon_block_root: [u8; 32],
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct VerificationReport {
    pub header_hash: VerificationCheck,
    pub parent_continuity: VerificationCheck,
    pub transactions_root: VerificationCheck,
    pub receipts_root: VerificationCheck,
    pub withdrawals_root: VerificationCheck,
    pub dataset_checksum: VerificationCheck,
    pub consensus_anchor: Option<ConsensusAnchor>,
}

impl Default for VerificationReport {
    fn default() -> Self {
        Self {
            header_hash: VerificationCheck::NOT_CHECKED,
            parent_continuity: VerificationCheck::NOT_CHECKED,
            transactions_root: VerificationCheck::NOT_CHECKED,
            receipts_root: VerificationCheck::NOT_CHECKED,
            withdrawals_root: VerificationCheck::NOT_CHECKED,
            dataset_checksum: VerificationCheck::NOT_CHECKED,
            consensus_anchor: None,
        }
    }
}

impl VerificationReport {
    #[must_use]
    pub fn has_failures(&self) -> bool {
        [
            &self.header_hash,
            &self.parent_continuity,
            &self.transactions_root,
            &self.receipts_root,
            &self.withdrawals_root,
            &self.dataset_checksum,
        ]
        .into_iter()
        .any(VerificationCheck::failed)
    }

    #[must_use]
    pub fn checked_count(&self) -> usize {
        [
            &self.header_hash,
            &self.parent_continuity,
            &self.transactions_root,
            &self.receipts_root,
            &self.withdrawals_root,
            &self.dataset_checksum,
        ]
        .into_iter()
        .filter(|check| matches!(check.status, CheckStatus::Verified | CheckStatus::Failed))
        .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_ids_are_portable() {
        assert!(SourceId::new("xatu-mainnet.v1").is_ok());
        assert!(SourceId::new("").is_err());
        assert!(SourceId::new("has spaces").is_err());
    }

    #[test]
    fn failed_checks_are_never_hidden() {
        let report = VerificationReport {
            receipts_root: VerificationCheck {
                status: CheckStatus::Failed,
                detail: Some("mismatch".to_owned()),
            },
            ..VerificationReport::default()
        };
        assert!(report.has_failures());
        assert_eq!(report.checked_count(), 1);
    }
}
