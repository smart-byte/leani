//! Explicit source and frame capability vocabulary.

use serde::{Deserialize, Serialize};

/// A normalized input component a processor may require.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[repr(u8)]
pub enum Capability {
    /// Header material: guarantees gas limit and gas used.
    Header = 0,
    /// Transaction-body fields: `to`, nonce, gas limit, and value of every
    /// transaction, and the header's transaction count.
    ///
    /// [`crate::TransactionEnvelope::from`] may be absent: require
    /// [`Self::Senders`] when consuming senders. Sources still evaluate active
    /// sender predicates before filtering Body or Calldata material.
    Body = 1,
    /// Every transaction field group: [`Self::Body`], [`Self::Calldata`],
    /// [`Self::Senders`], [`Self::BlobFields`], and [`Self::BlockSize`].
    Transactions = 2,
    /// Transaction input bytes. Like [`Self::Body`], this does not require
    /// recovered sender addresses.
    Calldata = 3,
    /// Complete receipts: [`Self::ReceiptOutcome`], [`Self::Logs`], and the
    /// rest of the raw receipt.
    Receipts = 4,
    Logs = 5,
    Withdrawals = 6,
    BlobSidecars = 7,
    Traces = 8,
    StateDiffs = 9,
    ConsensusFinality = 10,
    Mempool = 11,
    /// Recovered sender of every transaction.
    Senders = 12,
    /// Versioned hashes and blob fee cap of every type-3 transaction, and the
    /// blob gas used of every type-3 receipt.
    BlobFields = 13,
    /// Status, gas used, and effective gas price of every receipt, one receipt
    /// per transaction when both are present.
    ReceiptOutcome = 14,
    /// The execution block's RLP size and transaction count: whoever measures
    /// the block holds its body.
    BlockSize = 15,
}

impl Capability {
    const fn bit(self) -> u16 {
        1 << (self as u8)
    }
}

/// Stable bit representation used in descriptors and durable records.
///
/// Decoding rejects bits that name no [`Capability`].
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct CapabilitySet(u16);

impl<'de> Deserialize<'de> for CapabilitySet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let bits = u16::deserialize(deserializer)?;
        Self::from_bits(bits).ok_or_else(|| {
            serde::de::Error::custom(format!(
                "unknown capability bits 0x{:04x}",
                bits & !Self::ALL.0
            ))
        })
    }
}

impl CapabilitySet {
    pub const NONE: Self = Self(0);
    pub const ALL: Self = Self(u16::MAX);

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

    /// Add capabilities that are losslessly derivable from supplied material:
    /// the field groups each bundle stands for.
    #[must_use]
    pub const fn with_derivable(self) -> Self {
        let mut bits = self.0;
        if self.contains(Capability::Transactions) {
            bits |= Capability::Body.bit()
                | Capability::Calldata.bit()
                | Capability::Senders.bit()
                | Capability::BlobFields.bit()
                | Capability::BlockSize.bit();
        }
        if self.contains(Capability::Receipts) {
            bits |= Capability::Logs.bit() | Capability::ReceiptOutcome.bit();
        }
        Self(bits)
    }

    #[must_use]
    pub const fn bits(self) -> u16 {
        self.0
    }

    /// Construct a set when every bit belongs to a known capability.
    ///
    /// Every `u16` bit names a capability, so this cannot fail today; it stays
    /// fallible for callers decoding durable bits if the set ever widens.
    #[must_use]
    pub const fn from_bits(bits: u16) -> Option<Self> {
        Some(Self(bits))
    }

    pub fn iter(self) -> impl Iterator<Item = Capability> {
        const VALUES: [Capability; 16] = [
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
            Capability::Senders,
            Capability::BlobFields,
            Capability::ReceiptOutcome,
            Capability::BlockSize,
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
    /// Whether material for every `required` capability is complete or, with
    /// `allow_filtered`, merely present.
    ///
    /// Presence says nothing about which predicate filtered the material or
    /// how complete it claims to be. Consumers that accept filtered material
    /// must also check its [`crate::Completeness`] and that its
    /// [`crate::FilterScope`] covers their own filter, as
    /// `DataRequirement::validate_frame` in `leani-processor-api` does.
    ///
    /// The block size lives in the header, so header material supplies
    /// [`Capability::BlockSize`] without transactions. Only here: a header
    /// requirement still promises no size, and the report itself stays the
    /// component set retained segments record.
    #[must_use]
    pub const fn satisfies(self, required: CapabilitySet, allow_filtered: bool) -> bool {
        let components = if allow_filtered {
            self.present
        } else {
            self.complete
        };
        let mut supplied = components.with_derivable();
        if components.contains(Capability::Header) {
            supplied = supplied.with(Capability::BlockSize);
        }
        supplied.contains_all(required)
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

    #[test]
    fn bundles_derive_their_field_groups() {
        let transactions = CapabilitySet::of(Capability::Transactions).with_derivable();
        for group in [
            Capability::Body,
            Capability::Calldata,
            Capability::Senders,
            Capability::BlobFields,
            Capability::BlockSize,
        ] {
            assert!(transactions.contains(group), "{group:?}");
        }
        let receipts = CapabilitySet::of(Capability::Receipts).with_derivable();
        assert!(receipts.contains(Capability::Logs));
        assert!(receipts.contains(Capability::ReceiptOutcome));
        assert!(!receipts.contains(Capability::Senders));
        // field groups derive nothing, and the header derives no block size
        let narrow = CapabilitySet::of(Capability::Senders).with(Capability::Header);
        assert_eq!(narrow.with_derivable(), narrow);
    }

    #[test]
    fn field_groups_take_the_last_four_bits() {
        assert_eq!(CapabilitySet::ALL.bits(), u16::MAX);
        assert_eq!(CapabilitySet::ALL.iter().count(), 16);
        let groups = CapabilitySet::of(Capability::BlockSize).with(Capability::ReceiptOutcome);
        let encoded = postcard::to_allocvec(&groups).expect("encode");
        assert_eq!(
            postcard::from_bytes::<CapabilitySet>(&encoded).expect("decode"),
            groups
        );
    }

    #[test]
    fn decoding_rejects_unknown_capability_bits() {
        // every u16 bit names a capability, so only wider values are unknown
        let unknown = u32::from(CapabilitySet::ALL.bits()) + 1;
        let encoded = postcard::to_allocvec(&unknown).expect("encode");
        assert!(postcard::from_bytes::<CapabilitySet>(&encoded).is_err());
        let deserializer: serde::de::value::U32Deserializer<serde::de::value::Error> =
            serde::de::IntoDeserializer::into_deserializer(unknown);
        assert!(CapabilitySet::deserialize(deserializer).is_err());

        let encoded = postcard::to_allocvec(&CapabilitySet::ALL).expect("encode");
        assert_eq!(
            postcard::from_bytes::<CapabilitySet>(&encoded).expect("decode"),
            CapabilitySet::ALL
        );
    }
}
