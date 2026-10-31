//! Solidity signatures, and the ABI encoding of event and call arguments.
//!
//! Parse a signature as Solidity writes it, then decode a log or a call with
//! it:
//!
//! ```no_run
//! use leani_wasm_guest::{Block, solidity::Signature};
//!
//! # fn map(block: &Block) -> Result<(), leani_wasm_guest::Error> {
//! let transfer = Signature::parse("Transfer(address indexed from, address indexed to, uint256 value)")?;
//! let topic = transfer.topic();
//! for log in block.logs.as_deref().unwrap_or_default() {
//!     if log.topics.first() != Some(&topic) {
//!         continue;
//!     }
//!     // Another contract may emit the topic with another layout.
//!     let Some(args) = transfer.decode_event(&log.topics[1..], &log.data) else {
//!         continue;
//!     };
//! }
//! # Ok(())
//! # }
//! ```
//!
//! Supported argument types: `address`, `bool`, `uint<N>`, `int<N>`,
//! `bytes<N>`, `bytes`, and `string`; tuples and arrays are not. An indexed
//! `bytes` or `string` decodes to its hash.

use serde::{Deserialize, Serialize};

use crate::{Address, Error, Hash, U256, hex, keccak256};

/// An event or function signature.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Signature {
    pub name: String,
    /// As hashed, without names: `Transfer(address,address,uint256)`.
    pub canonical: String,
    pub inputs: Vec<Input>,
}

/// One declared argument.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Input {
    /// The declared name, or `arg<position>` for an unnamed argument.
    pub name: String,
    pub kind: Kind,
    /// Only events index arguments.
    pub indexed: bool,
}

/// A supported argument type.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Kind {
    Address,
    Bool,
    /// `uint<bits>`.
    Uint(u16),
    /// `int<bits>`.
    Int(u16),
    /// `bytes<length>`.
    FixedBytes(u8),
    Bytes,
    String,
}

/// A decoded argument. Serde carries it from `map` to `reduce` in a delta;
/// [`Self::to_json`] renders it as the node serves JSON.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum Value {
    Address(Address),
    Bool(bool),
    Uint(U256),
    /// A signed integer, as its sign and magnitude.
    Int {
        negative: bool,
        magnitude: U256,
    },
    /// `bytes<N>`, `bytes`, or the hash of an indexed `bytes` or `string`.
    Bytes(Vec<u8>),
    String(String),
}

impl Value {
    /// The value as the node serves JSON: addresses and bytes as `0x` hex,
    /// integers as decimal strings, which JSON numbers cannot hold exactly.
    #[must_use]
    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Self::Address(address) => hex(address).into(),
            Self::Bool(flag) => (*flag).into(),
            Self::Uint(value) => value.to_string().into(),
            Self::Int {
                negative,
                magnitude,
            } => format!("{}{magnitude}", if *negative { "-" } else { "" }).into(),
            Self::Bytes(bytes) => hex(bytes).into(),
            Self::String(text) => text.clone().into(),
        }
    }

    /// An unsigned integer, or a non-negative signed one.
    #[must_use]
    pub fn as_uint(&self) -> Option<U256> {
        match self {
            Self::Uint(value)
            | Self::Int {
                negative: false,
                magnitude: value,
            } => Some(*value),
            _ => None,
        }
    }

    /// The address, or `None` for any other value.
    #[must_use]
    pub fn as_address(&self) -> Option<Address> {
        match self {
            Self::Address(address) => Some(*address),
            _ => None,
        }
    }
}

impl Signature {
    /// Parse `Name(type [indexed] [name], ...)`, with or without a leading
    /// `event` or `function` and whatever follows the closing parenthesis,
    /// such as `external returns (bool)`, but `anonymous`.
    ///
    /// # Errors
    ///
    /// Returns a message naming what does not parse.
    pub fn parse(text: &str) -> Result<Self, Error> {
        let invalid = |reason: &str| Error::new(format!("signature `{text}`: {reason}"));
        let body = text.trim().trim_end_matches(';').trim();
        let body = body
            .strip_prefix("event ")
            .or_else(|| body.strip_prefix("function "))
            .unwrap_or(body)
            .trim();
        let (name, rest) = body.split_once('(').ok_or_else(|| invalid("missing `(`"))?;
        let (arguments, after) = rest
            .split_once(')')
            .ok_or_else(|| invalid("missing the closing `)`"))?;
        // Its logs carry no signature topic to find them by.
        if after.split_whitespace().any(|word| word == "anonymous") {
            return Err(invalid("anonymous events are not supported"));
        }
        let name = name.trim();
        if !identifier(name) {
            return Err(invalid("the name is not an identifier"));
        }
        if arguments.contains(['(', '[', ']']) {
            return Err(invalid("tuples and arrays are not supported"));
        }
        let mut inputs = Vec::new();
        for (position, argument) in arguments
            .split(',')
            .map(str::trim)
            .filter(|argument| !argument.is_empty())
            .enumerate()
        {
            let mut words = argument.split_whitespace();
            let kind =
                Kind::parse(words.next().unwrap_or_default()).map_err(|reason| invalid(&reason))?;
            let mut indexed = false;
            let mut name = format!("arg{position}");
            for word in words {
                match word {
                    "indexed" => indexed = true,
                    // Data locations of function arguments.
                    "memory" | "calldata" | "storage" => {}
                    word if identifier(word) => word.clone_into(&mut name),
                    word => return Err(invalid(&format!("unexpected `{word}`"))),
                }
            }
            inputs.push(Input {
                name,
                kind,
                indexed,
            });
        }
        if inputs.iter().filter(|input| input.indexed).count() > 3 {
            return Err(invalid("an event has at most three indexed arguments"));
        }
        let canonical = format!(
            "{name}({})",
            inputs
                .iter()
                .map(|input| input.kind.canonical())
                .collect::<Vec<_>>()
                .join(",")
        );
        Ok(Self {
            name: name.to_owned(),
            canonical,
            inputs,
        })
    }

    /// An event's topic 0: the Keccak-256 hash of the canonical signature.
    ///
    /// # Panics
    ///
    /// Panics outside a Leani Wasm host, which computes the hash.
    #[must_use]
    pub fn topic(&self) -> Hash {
        keccak256(self.canonical.as_bytes())
    }

    /// A function's selector: the first four bytes of that hash.
    ///
    /// # Panics
    ///
    /// Panics outside a Leani Wasm host, which computes the hash.
    #[must_use]
    pub fn selector(&self) -> [u8; 4] {
        let [a, b, c, d, ..] = self.topic();
        [a, b, c, d]
    }

    /// Decode an event's arguments, in declaration order, from its indexed
    /// `topics` (those after topic 0) and its `data`, or `None` when the log
    /// does not have this layout.
    #[must_use]
    pub fn decode_event(&self, topics: &[Hash], data: &[u8]) -> Option<Vec<Value>> {
        let indexed = self.inputs.iter().filter(|input| input.indexed).count();
        if topics.len() != indexed {
            return None;
        }
        let mut topics = topics.iter();
        let mut head = 0;
        let mut values = Vec::with_capacity(self.inputs.len());
        for input in &self.inputs {
            values.push(if input.indexed {
                let topic = topics.next()?;
                match input.kind {
                    // Indexed dynamic values are stored as their hash.
                    Kind::Bytes | Kind::String => Value::Bytes(topic.to_vec()),
                    kind => word(kind, topic)?,
                }
            } else {
                let value = argument(input.kind, data, head)?;
                head += 32;
                value
            });
        }
        Some(values)
    }

    /// Decode a call's arguments from its input after the four-byte
    /// selector, or `None` when the input does not have this layout.
    #[must_use]
    pub fn decode_arguments(&self, data: &[u8]) -> Option<Vec<Value>> {
        self.inputs
            .iter()
            .enumerate()
            .map(|(position, input)| argument(input.kind, data, position * 32))
            .collect()
    }
}

impl Kind {
    fn parse(text: &str) -> Result<Self, String> {
        let width = |digits: &str, maximum: u16, step: u16| {
            digits
                .parse::<u16>()
                .ok()
                .filter(|bits| *bits >= step && *bits <= maximum && bits % step == 0)
                .ok_or_else(|| format!("`{text}` has an invalid width"))
        };
        Ok(match text {
            "address" => Self::Address,
            "bool" => Self::Bool,
            "bytes" => Self::Bytes,
            "string" => Self::String,
            "uint" => Self::Uint(256),
            "int" => Self::Int(256),
            _ => {
                if let Some(digits) = text.strip_prefix("uint") {
                    Self::Uint(width(digits, 256, 8)?)
                } else if let Some(digits) = text.strip_prefix("int") {
                    Self::Int(width(digits, 256, 8)?)
                } else if let Some(digits) = text.strip_prefix("bytes") {
                    Self::FixedBytes(
                        u8::try_from(width(digits, 32, 1)?).map_err(|error| error.to_string())?,
                    )
                } else {
                    return Err(format!("unsupported type `{text}`"));
                }
            }
        })
    }

    /// The name the canonical signature uses.
    #[must_use]
    pub fn canonical(self) -> String {
        match self {
            Self::Address => "address".to_owned(),
            Self::Bool => "bool".to_owned(),
            Self::Uint(bits) => format!("uint{bits}"),
            Self::Int(bits) => format!("int{bits}"),
            Self::FixedBytes(bytes) => format!("bytes{bytes}"),
            Self::Bytes => "bytes".to_owned(),
            Self::String => "string".to_owned(),
        }
    }
}

/// The non-indexed argument whose head slot starts at `head` in `data`.
fn argument(kind: Kind, data: &[u8], head: usize) -> Option<Value> {
    let slot = data.get(head..head.checked_add(32)?)?;
    match kind {
        Kind::Bytes | Kind::String => dynamic(kind, data, slot),
        kind => word(kind, slot.try_into().ok()?),
    }
}

/// A static value from one 32-byte word, checking its ABI padding.
fn word(kind: Kind, word: &[u8; 32]) -> Option<Value> {
    let zero_above = |bytes: usize| word[..32 - bytes].iter().all(|byte| *byte == 0);
    Some(match kind {
        Kind::Address => {
            zero_above(20).then_some(())?;
            Value::Address(word[12..].try_into().ok()?)
        }
        Kind::Bool => match U256(*word) {
            value if value == U256::ZERO => Value::Bool(false),
            value if value == U256::from_u128(1) => Value::Bool(true),
            _ => return None,
        },
        Kind::Uint(bits) => {
            zero_above(usize::from(bits / 8)).then_some(())?;
            Value::Uint(U256(*word))
        }
        Kind::Int(bits) => {
            let bytes = usize::from(bits / 8);
            let negative = word[32 - bytes] & 0x80 != 0;
            let fill = if negative { 0xff } else { 0 };
            word[..32 - bytes]
                .iter()
                .all(|byte| *byte == fill)
                .then_some(())?;
            let magnitude = if negative {
                // Two's complement: invert and add one.
                U256(word.map(|byte| !byte)).checked_add(U256::from_u128(1))?
            } else {
                U256(*word)
            };
            Value::Int {
                negative,
                magnitude,
            }
        }
        Kind::FixedBytes(bytes) => {
            let bytes = usize::from(bytes);
            word[bytes..].iter().all(|byte| *byte == 0).then_some(())?;
            Value::Bytes(word[..bytes].to_vec())
        }
        Kind::Bytes | Kind::String => return None,
    })
}

/// A `bytes` or `string` value from the tail its head slot points at.
fn dynamic(kind: Kind, data: &[u8], slot: &[u8]) -> Option<Value> {
    let offset = small(slot)?;
    let len = small(data.get(offset..offset.checked_add(32)?)?)?;
    let start = offset.checked_add(32)?;
    let value = data.get(start..start.checked_add(len)?)?;
    Some(match kind {
        Kind::String => Value::String(String::from_utf8_lossy(value).into_owned()),
        _ => Value::Bytes(value.to_vec()),
    })
}

/// A word that fits a usize below 2^32, as offsets and lengths must.
fn small(word: &[u8]) -> Option<usize> {
    let (high, low) = word.split_at(28);
    high.iter()
        .all(|byte| *byte == 0)
        .then(|| usize::try_from(u32::from_be_bytes(low.try_into().ok()?)).ok())
        .flatten()
}

fn identifier(text: &str) -> bool {
    let mut bytes = text.bytes();
    bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_' || byte == b'$')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'$')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn word_of(value: u128) -> [u8; 32] {
        U256::from_u128(value).0
    }

    fn address_word(byte: u8) -> [u8; 32] {
        let mut word = [0; 32];
        word[12..].fill(byte);
        word
    }

    #[test]
    fn signatures_parse_to_their_canonical_form() {
        let transfer = Signature::parse(
            "event Transfer(address indexed from, address indexed to, uint256 value);",
        )
        .expect("parse");
        assert_eq!(transfer.name, "Transfer");
        assert_eq!(transfer.canonical, "Transfer(address,address,uint256)");
        assert_eq!(transfer.inputs[1].name, "to");
        assert!(transfer.inputs[1].indexed);
        let call = Signature::parse(
            "function approve(address spender, uint amount) external returns (bool)",
        )
        .expect("parse");
        assert_eq!(call.canonical, "approve(address,uint256)");
        let unnamed = Signature::parse("Sync(uint112,uint112)").expect("parse");
        assert_eq!(unnamed.inputs[1].name, "arg1");
        for invalid in [
            "Transfer",
            "Transfer(address",
            "1x(uint256)",
            "Swap((uint256,uint256))",
            "Batch(uint256[] ids)",
            "Odd(uint7 value)",
            "Many(uint8 indexed a, uint8 indexed b, uint8 indexed c, uint8 indexed d)",
            // No signature topic to find its logs by.
            "event Hidden(uint256 value) anonymous;",
        ] {
            assert!(Signature::parse(invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn events_decode_topics_and_data_and_refuse_other_layouts() {
        let transfer =
            Signature::parse("Transfer(address indexed from, address indexed to, uint256 value)")
                .expect("parse");
        let topics = [address_word(0x11), address_word(0x22)];
        let values = transfer
            .decode_event(&topics, &word_of(1_000))
            .expect("a transfer");
        assert_eq!(values[0], Value::Address([0x11; 20]));
        assert_eq!(values[2].to_json(), serde_json::json!("1000"));
        // ERC-721 indexes the third argument: three topics, no data.
        assert!(
            transfer
                .decode_event(&[address_word(1), address_word(2), word_of(3)], &[])
                .is_none()
        );
        // Dirty address padding is not an address.
        assert!(
            transfer
                .decode_event(&[[0xff; 32], topics[1]], &word_of(1))
                .is_none()
        );
    }

    #[test]
    fn calls_decode_static_signed_and_dynamic_arguments() {
        let call = Signature::parse("note(int24 tick, string memo, bool flag)").expect("parse");
        let mut data = Vec::new();
        data.extend_from_slice(&[0xff; 32]); // -1
        data.extend_from_slice(&word_of(96)); // offset of the string
        data.extend_from_slice(&word_of(1));
        data.extend_from_slice(&word_of(2));
        let mut text = [0; 32];
        text[..2].copy_from_slice(b"hi");
        data.extend_from_slice(&text);
        let values = call.decode_arguments(&data).expect("decodes");
        assert_eq!(values[0].to_json(), serde_json::json!("-1"));
        assert_eq!(values[1], Value::String("hi".to_owned()));
        assert_eq!(values[2], Value::Bool(true));
        assert_eq!(values[0].as_uint(), None);
        assert!(call.decode_arguments(&data[..64]).is_none(), "truncated");
    }

    #[test]
    fn static_words_must_be_padded_as_their_type_requires() {
        let decode = |kind, value: [u8; 32]| word(kind, &value);
        let mut minimum = [0; 32];
        minimum[0] = 0x80;
        assert_eq!(
            decode(Kind::Int(256), minimum).map(|value| value.to_json()),
            Some(serde_json::json!(
                "-57896044618658097711785492504343953926634992332820282019728792003956564819968"
            ))
        );
        let mut tick = [0xff; 32];
        tick[29..].copy_from_slice(&[0xf2, 0x76, 0x18]);
        assert_eq!(
            decode(Kind::Int(24), tick).map(|value| value.to_json()),
            Some(serde_json::json!("-887272"))
        );
        // A positive int24 whose padding is not its sign.
        let mut dirty = word_of(1);
        dirty[0] = 0xff;
        assert_eq!(decode(Kind::Int(24), dirty), None);
        assert_eq!(decode(Kind::Uint(8), word_of(256)), None);
        assert_eq!(decode(Kind::Bool, word_of(2)), None);
        let mut selector = [0; 32];
        selector[..4].copy_from_slice(&[1, 2, 3, 4]);
        assert_eq!(
            decode(Kind::FixedBytes(4), selector),
            Some(Value::Bytes(vec![1, 2, 3, 4]))
        );
        selector[4] = 5;
        assert_eq!(decode(Kind::FixedBytes(4), selector), None);
        let address = decode(Kind::Address, address_word(0x33)).expect("address");
        assert_eq!(address.as_address(), Some([0x33; 20]));
        assert_eq!(address.as_uint(), None);
    }

    #[test]
    fn dynamic_values_stay_within_their_data() {
        let call = Signature::parse("note(bytes memo)").expect("parse");
        let encoded = |offset: u128, len: u128| {
            let mut data = Vec::new();
            data.extend_from_slice(&word_of(offset));
            data.extend_from_slice(&word_of(len));
            data.extend_from_slice(&[0xab; 32]);
            data
        };
        assert_eq!(
            call.decode_arguments(&encoded(32, 2)),
            Some(vec![Value::Bytes(vec![0xab; 2])])
        );
        for (offset, len) in [(32, 33), (96, 1), (1 << 32, 1), (32, 1 << 40)] {
            assert_eq!(
                call.decode_arguments(&encoded(offset, len)),
                None,
                "{offset} {len}"
            );
        }
        // An indexed string is stored as its hash.
        let named = Signature::parse("Named(string indexed name)").expect("parse");
        assert_eq!(
            named.decode_event(&[[7; 32]], &[]),
            Some(vec![Value::Bytes(vec![7; 32])])
        );
    }

    #[test]
    fn values_cross_from_map_to_reduce_unchanged() {
        let values = vec![
            Value::Address([1; 20]),
            Value::Bool(true),
            Value::Uint(U256::MAX),
            Value::Int {
                negative: true,
                magnitude: U256::from_u128(5),
            },
            Value::Bytes(vec![1, 2]),
            Value::String("hi".to_owned()),
        ];
        let delta = postcard::to_allocvec(&values).expect("encode");
        assert_eq!(postcard::from_bytes::<Vec<Value>>(&delta), Ok(values));
    }
}
