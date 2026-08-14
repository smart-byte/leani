//! Explicit source and frame capability vocabulary.

use serde::{Deserialize, Serialize};

/// A normalized input component a processor may require.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[repr(u8)]
pub enum Capability {
    Header = 0,
    Body = 1,
    Transactions = 2,
    Calldata = 3,
    Receipts = 4,
    Logs = 5,
    Withdrawals = 6,
    BlobSidecars = 7,
    Traces = 8,
    StateDiffs = 9,
    ConsensusFinality = 10,
    Mempool = 11,
}

impl Capability {
    const fn bit(self) -> u16 {
        1 << (self as u8)
    }
}

/// Stable bit representation used in descriptors and durable records.
#[derive(
    Clone, Copy, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(transparent)]
pub struct CapabilitySet(u16);

impl CapabilitySet {
    pub const NONE: Self = Self(0);
    pub const ALL: Self = Self((1 << 12) - 1);

    #[must_use]
    pub const fn of(capability: Capability) -> Self {
        Self(capability.bit())
    }

    #[must_use]
    pub const fn with(self, capability: Capability) -> Self {
        Self(self.0 | capability.bit())
    }

    #[must_use]
    pub const fn without(self, capability: Capability) -> Self {
        Self(self.0 & !capability.bit())
    }

    #[must_use]
    pub const fn contains(self, capability: Capability) -> bool {
        self.0 & capability.bit() != 0
    }

    #[must_use]
    pub const fn contains_all(self, required: Self) -> bool {
        self.0 & required.0 == required.0
    }

    #[must_use]
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    #[must_use]
    pub const fn intersection(self, other: Self) -> Self {
        Self(self.0 & other.0)
    }

    /// Add capabilities that are losslessly derivable from supplied material.
    #[must_use]
    pub const fn with_derivable(self) -> Self {
        let mut bits = self.0;
        if self.contains(Capability::Transactions) {
            bits |= Capability::Body.bit() | Capability::Calldata.bit();
        }
        if self.contains(Capability::Receipts) {
            bits |= Capability::Logs.bit();
        }
        Self(bits)
    }

    #[must_use]
    pub const fn bits(self) -> u16 {
        self.0
    }

    /// Construct a set when every bit belongs to a known capability.
    #[must_use]
    pub const fn from_bits(bits: u16) -> Option<Self> {
        if bits & !Self::ALL.0 == 0 {
            Some(Self(bits))
        } else {
            None
        }
    }

    pub fn iter(self) -> impl Iterator<Item = Capability> {
        const VALUES: [Capability; 12] = [
            Capability::Header,
            Capability::Body,
            Capability::Transactions,
            Capability::Calldata,
            Capability::Receipts,
            Capability::Logs,
            Capability::Withdrawals,
            Capability::BlobSidecars,
            Capability::Traces,
            Capability::StateDiffs,
            Capability::ConsensusFinality,
            Capability::Mempool,
        ];
        VALUES
            .into_iter()
            .filter(move |capability| self.contains(*capability))
    }
}

impl FromIterator<Capability> for CapabilitySet {
    fn from_iter<T: IntoIterator<Item = Capability>>(iter: T) -> Self {
        iter.into_iter().fold(Self::NONE, Self::with)
    }
}

/// Separates presence from independently complete material.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct FrameCapabilityReport {
    pub present: CapabilitySet,
    pub complete: CapabilitySet,
}

impl FrameCapabilityReport {
    #[must_use]
    pub const fn satisfies(self, required: CapabilitySet, allow_filtered: bool) -> bool {
        if allow_filtered {
            self.present.with_derivable().contains_all(required)
        } else {
            self.complete.with_derivable().contains_all(required)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derivation_is_explicit() {
        let receipts = CapabilitySet::of(Capability::Receipts).with_derivable();
        assert!(receipts.contains(Capability::Logs));
        assert!(!receipts.contains(Capability::Transactions));
    }

    #[test]
    fn filtered_and_complete_requirements_differ() {
        let report = FrameCapabilityReport {
            present: CapabilitySet::of(Capability::Logs),
            complete: CapabilitySet::NONE,
        };
        assert!(report.satisfies(CapabilitySet::of(Capability::Logs), true));
        assert!(!report.satisfies(CapabilitySet::of(Capability::Logs), false));
    }
}
