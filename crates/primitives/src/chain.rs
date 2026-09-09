//! Canonical chain identity and ordering types.

use std::{fmt, ops::RangeInclusive};

use serde::{Deserialize, Serialize};
use thiserror::Error;

macro_rules! numeric_newtype {
    ($name:ident) => {
        #[derive(
            Clone,
            Copy,
            Debug,
            Default,
            Deserialize,
            Eq,
            Hash,
            Ord,
            PartialEq,
            PartialOrd,
            Serialize,
        )]
        #[serde(transparent)]
        pub struct $name(pub u64);

        impl From<u64> for $name {
            fn from(value: u64) -> Self {
                Self(value)
            }
        }

        impl From<$name> for u64 {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

numeric_newtype!(ChainId);
numeric_newtype!(BlockNumber);

macro_rules! fixed_bytes_newtype {
    ($name:ident, $size:expr) => {
        #[derive(
            Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
        )]
        #[serde(transparent)]
        pub struct $name(pub [u8; $size]);

        impl $name {
            pub const ZERO: Self = Self([0; $size]);

            #[must_use]
            pub const fn new(bytes: [u8; $size]) -> Self {
                Self(bytes)
            }

            #[must_use]
            pub const fn as_array(&self) -> &[u8; $size] {
                &self.0
            }
        }

        impl From<[u8; $size]> for $name {
            fn from(value: [u8; $size]) -> Self {
                Self(value)
            }
        }

        impl From<$name> for [u8; $size] {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "0x{}", hex::encode(self.0))
            }
        }
    };
}

fixed_bytes_newtype!(BlockHash, 32);
fixed_bytes_newtype!(TransactionHash, 32);
fixed_bytes_newtype!(Address, 20);
fixed_bytes_newtype!(Quantity, 32);

/// Inclusive canonical block interval.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct BlockRange {
    start: BlockNumber,
    end: BlockNumber,
}

impl BlockRange {
    /// Build an inclusive interval.
    ///
    /// # Errors
    ///
    /// Returns [`BlockRangeError`] when `end` precedes `start`.
    pub const fn new(start: BlockNumber, end: BlockNumber) -> Result<Self, BlockRangeError> {
        if start.0 <= end.0 {
            Ok(Self { start, end })
        } else {
            Err(BlockRangeError { start, end })
        }
    }

    #[must_use]
    pub const fn single(block: BlockNumber) -> Self {
        Self {
            start: block,
            end: block,
        }
    }

    #[must_use]
    pub const fn start(self) -> BlockNumber {
        self.start
    }

    #[must_use]
    pub const fn end(self) -> BlockNumber {
        self.end
    }

    #[must_use]
    pub const fn contains(self, block: BlockNumber) -> bool {
        block.0 >= self.start.0 && block.0 <= self.end.0
    }

    #[must_use]
    pub const fn len(self) -> u64 {
        self.end.0 - self.start.0 + 1
    }

    #[must_use]
    pub const fn is_empty(self) -> bool {
        false
    }

    pub fn iter(self) -> impl Iterator<Item = BlockNumber> {
        RangeInclusive::new(self.start.0, self.end.0).map(BlockNumber)
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("invalid block range {start}..={end}: end precedes start")]
pub struct BlockRangeError {
    pub start: BlockNumber,
    pub end: BlockNumber,
}

/// Identity and parent linkage required to order a block without retaining it.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct BlockRef {
    pub number: BlockNumber,
    pub hash: BlockHash,
    pub parent_hash: BlockHash,
    pub timestamp: u64,
}

impl BlockRef {
    #[must_use]
    pub const fn canonical_key(self, chain_id: ChainId) -> CanonicalKey {
        CanonicalKey {
            chain_id,
            number: self.number,
            hash: self.hash,
        }
    }
}

/// Stable key for a particular block on a chain.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct CanonicalKey {
    pub chain_id: ChainId,
    pub number: BlockNumber,
    pub hash: BlockHash,
}

impl CanonicalKey {
    pub const ENCODED_LEN: usize = 48;

    /// Big-endian encoding whose lexicographic order follows chain then height.
    #[must_use]
    pub fn encode_ordered(self) -> [u8; Self::ENCODED_LEN] {
        let mut output = [0; Self::ENCODED_LEN];
        output[..8].copy_from_slice(&self.chain_id.0.to_be_bytes());
        output[8..16].copy_from_slice(&self.number.0.to_be_bytes());
        output[16..].copy_from_slice(&self.hash.0);
        output
    }

    /// Decode the fixed-width ordered key.
    ///
    /// # Errors
    ///
    /// Returns an error when the input length is not exactly 48 bytes.
    pub fn decode_ordered(bytes: &[u8]) -> Result<Self, &'static str> {
        if bytes.len() != Self::ENCODED_LEN {
            return Err("canonical key must be exactly 48 bytes");
        }
        let mut chain_id = [0; 8];
        chain_id.copy_from_slice(&bytes[..8]);
        let mut number = [0; 8];
        number.copy_from_slice(&bytes[8..16]);
        let mut hash = [0; 32];
        hash.copy_from_slice(&bytes[16..]);
        Ok(Self {
            chain_id: ChainId(u64::from_be_bytes(chain_id)),
            number: BlockNumber(u64::from_be_bytes(number)),
            hash: BlockHash(hash),
        })
    }
}

/// Chain-confidence status of a block. Ordering is intentional.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[repr(u8)]
pub enum Finality {
    /// A peer supplied the block; its connection to the chain is still being
    /// checked. Only the embedded CLI emits this; it is never persisted.
    Preview = 0,
    /// The block is on the currently followed chain. A reorg can remove it.
    Included = 1,
    /// Ethereum consensus has finalized the block.
    Finalized = 2,
}

impl Finality {
    #[must_use]
    pub const fn satisfies(self, required: Self) -> bool {
        self as u8 >= required as u8
    }

    /// Lowercase wire name used by every public JSON surface.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Preview => "preview",
            Self::Included => "included",
            Self::Finalized => "finalized",
        }
    }
}

/// Canonicality transition consumed by the runtime and change stream.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum CanonicalChainEvent {
    Applied { block: BlockRef, finality: Finality },
    Reverted { block: BlockRef },
    FinalityAdvanced { block: BlockRef, finality: Finality },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inclusive_range_has_checked_order_and_iteration() {
        let range = BlockRange::new(BlockNumber(4), BlockNumber(6)).expect("valid range");
        assert_eq!(range.len(), 3);
        assert_eq!(
            range.iter().collect::<Vec<_>>(),
            vec![BlockNumber(4), BlockNumber(5), BlockNumber(6)]
        );
        assert!(BlockRange::new(BlockNumber(7), BlockNumber(6)).is_err());
    }

    #[test]
    fn finality_is_monotonic_and_named() {
        assert!(Finality::Finalized.satisfies(Finality::Included));
        assert!(Finality::Included.satisfies(Finality::Preview));
        assert!(!Finality::Preview.satisfies(Finality::Included));
        assert!(!Finality::Included.satisfies(Finality::Finalized));
        assert_eq!(Finality::Preview.name(), "preview");
        assert_eq!(Finality::Included.name(), "included");
        assert_eq!(Finality::Finalized.name(), "finalized");
    }

    #[test]
    fn canonical_key_encoding_preserves_height_order() {
        let low = CanonicalKey {
            chain_id: ChainId(1),
            number: BlockNumber(9),
            hash: BlockHash::new([0xff; 32]),
        };
        let high = CanonicalKey {
            chain_id: ChainId(1),
            number: BlockNumber(10),
            hash: BlockHash::ZERO,
        };
        assert!(low.encode_ordered() < high.encode_ordered());
        assert_eq!(
            CanonicalKey::decode_ordered(&low.encode_ordered()).expect("decode"),
            low
        );
    }
}
