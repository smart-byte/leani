//! Minimal unsigned 256-bit arithmetic for token amounts.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

/// An unsigned 256-bit integer stored as big-endian bytes, as Ethereum
/// encodes quantities. Serializes as a decimal string in JSON, and as its 32
/// bytes in binary formats such as the postcard deltas.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct U256(pub [u8; 32]);

impl U256 {
    pub const ZERO: Self = Self([0; 32]);
    pub const MAX: Self = Self([u8::MAX; 32]);

    #[must_use]
    pub fn from_u128(value: u128) -> Self {
        let mut bytes = [0; 32];
        bytes[16..].copy_from_slice(&value.to_be_bytes());
        Self(bytes)
    }

    #[must_use]
    pub fn is_zero(&self) -> bool {
        self.0 == [0; 32]
    }

    /// The sum, or `None` on overflow.
    #[must_use]
    pub fn checked_add(self, other: Self) -> Option<Self> {
        let mut sum = [0; 32];
        let mut carry = 0_u16;
        for index in (0..32).rev() {
            let total = u16::from(self.0[index]) + u16::from(other.0[index]) + carry;
            sum[index] = total.to_be_bytes()[1];
            carry = total >> 8;
        }
        (carry == 0).then_some(Self(sum))
    }

    /// The sum, or [`Self::MAX`] on overflow.
    #[must_use]
    pub fn saturating_add(self, other: Self) -> Self {
        self.checked_add(other).unwrap_or(Self::MAX)
    }

    /// The difference, or `None` when `other` is larger.
    #[must_use]
    pub fn checked_sub(self, other: Self) -> Option<Self> {
        if self < other {
            return None;
        }
        let mut difference = [0; 32];
        let mut borrow = 0_i16;
        for index in (0..32).rev() {
            let mut value = i16::from(self.0[index]) - i16::from(other.0[index]) - borrow;
            borrow = i16::from(value < 0);
            if value < 0 {
                value += 256;
            }
            difference[index] = u8::try_from(value).unwrap_or(0);
        }
        Some(Self(difference))
    }

    /// Ten times the value plus a decimal digit; `None` on overflow.
    fn shift_in_digit(self, digit: u8) -> Option<Self> {
        let mut product = [0; 32];
        let mut carry = u16::from(digit);
        for index in (0..32).rev() {
            let total = u16::from(self.0[index]) * 10 + carry;
            product[index] = total.to_be_bytes()[1];
            carry = total >> 8;
        }
        (carry == 0).then_some(Self(product))
    }
}

impl fmt::Display for U256 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Divide by 10^19, the largest power of ten in a u64, so a value
        // takes at most five divisions of four 64-bit limbs.
        const CHUNK: u128 = 10_000_000_000_000_000_000;
        let mut limbs = [0_u64; 4];
        for (limb, bytes) in limbs.iter_mut().zip(self.0.as_chunks::<8>().0) {
            *limb = u64::from_be_bytes(*bytes);
        }
        let mut chunks = Vec::with_capacity(5);
        loop {
            let mut remainder = 0_u128;
            for limb in &mut limbs {
                let current = remainder << 64 | u128::from(*limb);
                *limb = u64::try_from(current / CHUNK).unwrap_or(u64::MAX);
                remainder = current % CHUNK;
            }
            chunks.push(remainder);
            if limbs == [0; 4] {
                break;
            }
        }
        let mut chunks = chunks.iter().rev();
        write!(formatter, "{}", chunks.next().unwrap_or(&0))?;
        chunks.try_for_each(|chunk| write!(formatter, "{chunk:019}"))
    }
}

impl FromStr for U256 {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(format!("`{text}` is not an unsigned decimal integer"));
        }
        text.bytes().try_fold(Self::ZERO, |value, digit| {
            value
                .shift_in_digit(digit - b'0')
                .ok_or_else(|| format!("`{text}` does not fit 256 bits"))
        })
    }
}

impl Serialize for U256 {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            serializer.collect_str(self)
        } else {
            self.0.serialize(serializer)
        }
    }
}

/// A decimal string, or an integer small enough for JSON numbers; the 32
/// bytes in a binary format.
impl<'de> Deserialize<'de> for U256 {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Decimal;

        impl de::Visitor<'_> for Decimal {
            type Value = U256;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a decimal string, or an integer below 2^64")
            }

            fn visit_u64<E: de::Error>(self, value: u64) -> Result<U256, E> {
                Ok(U256::from_u128(u128::from(value)))
            }

            fn visit_str<E: de::Error>(self, text: &str) -> Result<U256, E> {
                text.parse().map_err(E::custom)
            }
        }

        if deserializer.is_human_readable() {
            deserializer.deserialize_any(Decimal)
        } else {
            <[u8; 32]>::deserialize(deserializer).map(Self)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_round_trip_and_arithmetic_at_the_edges() {
        let max = "115792089237316195423570985008687907853269984665640564039457584007913129639935";
        assert_eq!(max.parse::<U256>(), Ok(U256::MAX));
        assert_eq!(U256::MAX.to_string(), max);
        assert!(format!("{max}0").parse::<U256>().is_err());
        assert_eq!(U256::ZERO.to_string(), "0");
        let a = U256::from_u128(u128::MAX);
        let sum = a.checked_add(U256::from_u128(1)).expect("no overflow");
        assert_eq!(sum.to_string(), "340282366920938463463374607431768211456");
        assert_eq!(sum.checked_sub(U256::from_u128(1)), Some(a));
        assert_eq!(U256::ZERO.checked_sub(U256::from_u128(1)), None);
        assert_eq!(U256::MAX.checked_add(U256::from_u128(1)), None);
        assert_eq!(U256::MAX.saturating_add(U256::from_u128(1)), U256::MAX);
        assert_eq!(
            U256::from_u128(2).saturating_add(U256::from_u128(3)),
            U256::from_u128(5)
        );
        assert!(U256::from_u128(2) > U256::from_u128(1));
        assert!(U256::ZERO.is_zero() && !U256::from_u128(1).is_zero());
        // A chunk of all zeros between nonzero ones keeps its width.
        let inner_zeros = U256::from_u128(10_000_000_000_000_000_005);
        assert_eq!(inner_zeros.to_string(), "10000000000000000005");
        // Deltas are postcard, which cannot decode a string-or-number.
        let value = Some(U256::from_u128(1_234));
        let encoded = postcard::to_allocvec(&value).expect("postcard");
        assert_eq!(encoded.len(), 33);
        assert_eq!(postcard::from_bytes::<Option<U256>>(&encoded), Ok(value));
    }

    #[test]
    fn json_takes_decimal_strings_and_integers_below_2_to_the_64() {
        assert_eq!(
            serde_json::from_str::<U256>("7").ok(),
            Some(U256::from_u128(7))
        );
        assert_eq!(
            serde_json::from_str::<U256>("\"18446744073709551616\"").ok(),
            Some(U256::from_u128(1 << 64))
        );
        for refused in ["-1", "18446744073709551616", "1.5"] {
            let error = serde_json::from_str::<U256>(refused).expect_err(refused);
            assert!(
                error
                    .to_string()
                    .contains("expected a decimal string, or an integer below 2^64"),
                "{refused}: {error}"
            );
        }
        assert!(
            serde_json::from_str::<U256>("\"0x10\"")
                .expect_err("hex")
                .to_string()
                .contains("is not an unsigned decimal integer")
        );
    }
}
