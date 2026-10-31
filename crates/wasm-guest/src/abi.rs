//! Leani Wasm ABI version 1: the data a host passes to a guest, and the
//! manifest a guest returns. The host decodes and encodes these exact types.
//!
//! Inputs are [postcard](https://postcard.jamesmunns.com/wire-format)
//! encoded. Fixed-size hashes, addresses, and 256-bit quantities are plain
//! big-endian byte arrays. The manifest is JSON.

use serde::{Deserialize, Serialize};

/// The ABI version a manifest declares.
pub const ABI_VERSION: u32 = 1;

/// A 20-byte account address.
pub type Address = [u8; 20];
/// A 32-byte hash or log topic.
pub type Hash = [u8; 32];
/// An unsigned 256-bit big-endian quantity, such as a value or a fee.
pub type Word = [u8; 32];
/// Key-value pairs in key order, as a scan returns them (postcard encoded).
pub type Entries = Vec<(Vec<u8>, Vec<u8>)>;

/// One canonical execution block, projected to the material the processor's
/// manifest requires and to its filter.
///
/// A material the manifest does not require is `None`. Logs and
/// transactions are reduced to those matching the manifest filter whatever
/// the source supplied, so a block maps the same from every source.
/// Transactions, receipts, and logs are in block order. The block's finality
/// is deliberately absent: a delta must not depend on it.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct Block {
    pub chain_id: u64,
    pub number: u64,
    pub hash: Hash,
    pub parent_hash: Hash,
    pub timestamp: u64,
    pub header: Option<Header>,
    pub transactions: Option<Vec<Transaction>>,
    pub receipts: Option<Vec<Receipt>>,
    pub logs: Option<Vec<Log>>,
    pub withdrawals: Option<Vec<Withdrawal>>,
}

/// Header fields. A field is `None` when the manifest does not require its
/// group, [`Capability::Header`] for gas and fees or [`Capability::BlockSize`]
/// for the size and transaction count, or the source could not supply it.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct Header {
    pub gas_limit: Option<u64>,
    pub gas_used: Option<u64>,
    pub base_fee_per_gas: Option<Word>,
    pub blob_gas_used: Option<u64>,
    pub excess_blob_gas: Option<u64>,
    pub size_bytes: Option<u64>,
    pub transaction_count: Option<u32>,
}

/// A transaction of the block. A field is `None`, or empty, when the
/// manifest does not require its group or the source could not supply it.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct Transaction {
    pub hash: Hash,
    pub index: u32,
    pub transaction_type: u8,
    /// Present when the manifest requires senders or transactions.
    pub from: Option<Address>,
    /// Also `None` for a contract creation.
    pub to: Option<Address>,
    pub nonce: Option<u64>,
    pub gas_limit: Option<u64>,
    pub value: Option<Word>,
    /// Present when the manifest requires calldata or transactions.
    pub input: Option<Vec<u8>>,
    pub max_fee_per_blob_gas: Option<Word>,
    pub blob_versioned_hashes: Vec<Hash>,
}

/// A receipt of the block. A field is `None`, or empty, when the manifest
/// does not require its group or the source could not supply it.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct Receipt {
    pub transaction_hash: Hash,
    pub transaction_index: u32,
    pub transaction_type: u8,
    pub success: Option<bool>,
    pub gas_used: Option<u64>,
    pub effective_gas_price: Option<Word>,
    pub blob_gas_used: Option<u64>,
    pub logs: Vec<Log>,
}

/// A log the block emitted.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct Log {
    pub address: Address,
    pub topics: Vec<Hash>,
    pub data: Vec<u8>,
    /// Present when the manifest sets `log_transaction_hash`.
    pub transaction_hash: Option<Hash>,
    pub transaction_index: u32,
    pub log_index: u32,
}

/// A validator withdrawal the block processed.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct Withdrawal {
    pub index: u64,
    pub validator_index: u64,
    pub address: Address,
    pub amount_gwei: u64,
}

/// The block a delta belongs to, as `leani_reduce` receives it.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct BlockRef {
    pub chain_id: u64,
    pub number: u64,
    pub hash: Hash,
    pub parent_hash: Hash,
    pub timestamp: u64,
}

/// `leani_map` input.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct MapInput {
    /// The processor's parameters as JSON.
    pub params: Vec<u8>,
    pub block: Block,
}

/// `leani_reduce` input.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReduceInput {
    /// The processor's parameters as JSON.
    pub params: Vec<u8>,
    pub block: BlockRef,
    /// The bytes `leani_map` output for this block.
    pub delta: Vec<u8>,
}

/// What a processor needs and how it reduces, returned as JSON by
/// `leani_describe`. It may depend on the parameters, such as a filter on a
/// configured contract.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    /// [`ABI_VERSION`].
    pub abi: u32,
    /// 1-64 ASCII letters, digits, '.', '_' or '-'; also the default schema
    /// prefix.
    pub name: String,
    pub reduction: Reduction,
    pub requires: Requirements,
    /// Defaults to `wasm.<name>.entity.v1`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entity_schema: Option<String>,
    /// Defaults to `wasm.<name>.change.v1`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub change_schema: Option<String>,
}

impl Manifest {
    /// A block-local processor with no requirements yet.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            abi: ABI_VERSION,
            name: name.into(),
            reduction: Reduction::BlockLocal,
            requires: Requirements::default(),
            entity_schema: None,
            change_schema: None,
        }
    }

    /// Reduce blocks in chain order, reading state earlier blocks wrote.
    #[must_use]
    pub fn ordered(mut self) -> Self {
        self.reduction = Reduction::Ordered;
        self
    }

    /// Require a material or field group, as [`Capability`] names them.
    #[must_use]
    pub fn require(mut self, capability: Capability) -> Self {
        if !self.requires.capabilities.contains(&capability) {
            self.requires.capabilities.push(capability);
        }
        self
    }

    /// Require logs with their transaction hashes.
    #[must_use]
    pub fn logs_with_transaction_hash(mut self) -> Self {
        self.requires.log_transaction_hash = true;
        self.require(Capability::Logs)
    }

    /// Keep only logs emitted by these contracts.
    #[must_use]
    pub fn addresses(mut self, addresses: impl IntoIterator<Item = Address>) -> Self {
        self.requires.filter.addresses.extend(addresses);
        self
    }

    /// Keep only transactions to these accounts, and their logs. Needs
    /// [`Capability::Body`].
    #[must_use]
    pub fn recipients(mut self, recipients: impl IntoIterator<Item = Address>) -> Self {
        self.requires.filter.recipients.extend(recipients);
        self
    }

    /// Keep only transactions from these accounts, and their logs. Needs
    /// [`Capability::Senders`].
    #[must_use]
    pub fn senders(mut self, senders: impl IntoIterator<Item = Address>) -> Self {
        self.requires.filter.senders.extend(senders);
        self
    }

    /// Keep only logs whose topic at `position` is one of `alternatives`;
    /// position 0 is the event signature.
    #[must_use]
    pub fn topic(mut self, position: u8, alternatives: impl IntoIterator<Item = Hash>) -> Self {
        self.requires.filter.topics.push(TopicFilter {
            position,
            alternatives: alternatives.into_iter().collect(),
        });
        self
    }
}

/// How the node reduces a processor's blocks.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Reduction {
    /// Each block reduces alone: no state reads, and keys unique to a block.
    BlockLocal,
    /// Blocks reduce in chain order and may read state.
    Ordered,
}

/// The material a processor needs, and the filter that reduces it.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Requirements {
    pub capabilities: Vec<Capability>,
    #[serde(default)]
    pub log_transaction_hash: bool,
    #[serde(default)]
    pub filter: Filter,
}

/// A material or field group of a block, as named in node configuration.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Capability {
    /// Gas limit, gas used, base fee, and blob gas of the block.
    Header,
    /// `to`, nonce, gas limit, and value of every transaction.
    Body,
    /// Every transaction field group.
    Transactions,
    /// Transaction input bytes.
    Calldata,
    /// Recovered sender of every transaction.
    Senders,
    /// Blob versioned hashes and fee caps.
    BlobFields,
    /// The block's size in bytes and its transaction count.
    BlockSize,
    /// Complete receipts with their logs.
    Receipts,
    /// Status, gas used, and effective gas price of every receipt.
    ReceiptOutcome,
    /// The block's logs, without the rest of the receipts.
    Logs,
    /// The validator withdrawals the block processed.
    Withdrawals,
}

/// The operations of the `leani.write` import.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum WriteOperation {
    Put = 0,
    Delete = 1,
    StatePut = 2,
    StateDelete = 3,
    IndexPut = 4,
    IndexDelete = 5,
}

impl TryFrom<u32> for WriteOperation {
    type Error = u32;

    fn try_from(code: u32) -> Result<Self, u32> {
        Ok(match code {
            0 => Self::Put,
            1 => Self::Delete,
            2 => Self::StatePut,
            3 => Self::StateDelete,
            4 => Self::IndexPut,
            5 => Self::IndexDelete,
            other => return Err(other),
        })
    }
}

/// The material to keep. An empty list matches everything.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Filter {
    #[serde(default, skip_serializing_if = "Vec::is_empty", with = "hex_list")]
    pub addresses: Vec<Address>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub topics: Vec<TopicFilter>,
    #[serde(default, skip_serializing_if = "Vec::is_empty", with = "hex_list")]
    pub senders: Vec<Address>,
    #[serde(default, skip_serializing_if = "Vec::is_empty", with = "hex_list")]
    pub recipients: Vec<Address>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transaction_types: Vec<u8>,
}

/// One topic position and the topics allowed there.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TopicFilter {
    pub position: u8,
    #[serde(with = "hex_list")]
    pub alternatives: Vec<Hash>,
}

/// `0x`-prefixed lowercase hex.
#[must_use]
pub fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut text = String::with_capacity(2 + bytes.len() * 2);
    text.push_str("0x");
    for byte in bytes {
        text.push(char::from(DIGITS[usize::from(byte >> 4)]));
        text.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    text
}

/// Decode exactly `N` bytes of hex, with or without `0x`.
///
/// # Errors
///
/// Returns a message for a wrong length or a non-hex digit.
pub fn parse_hex<const N: usize>(text: &str) -> Result<[u8; N], String> {
    let digits = text.strip_prefix("0x").unwrap_or(text).as_bytes();
    if digits.len() != N * 2 {
        return Err(format!("`{text}` is not {N} bytes of hex"));
    }
    let mut bytes = [0_u8; N];
    for (byte, pair) in bytes.iter_mut().zip(digits.as_chunks::<2>().0) {
        let nibble = |digit: u8| {
            char::from(digit)
                .to_digit(16)
                .and_then(|value| u8::try_from(value).ok())
                .ok_or_else(|| format!("`{text}` contains a non-hex digit"))
        };
        *byte = nibble(pair[0])? << 4 | nibble(pair[1])?;
    }
    Ok(bytes)
}

mod hex_list {
    use serde::{Deserialize, Deserializer, Serializer, de::Error as _, ser::SerializeSeq};

    pub(super) fn serialize<S: Serializer, const N: usize>(
        values: &[[u8; N]],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(values.len()))?;
        for value in values {
            sequence.serialize_element(&super::hex(value))?;
        }
        sequence.end()
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>, const N: usize>(
        deserializer: D,
    ) -> Result<Vec<[u8; N]>, D::Error> {
        Vec::<String>::deserialize(deserializer)?
            .iter()
            .map(|text| super::parse_hex(text).map_err(D::Error::custom))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_json_round_trips_with_hex_filters() {
        let manifest = Manifest::new("transfers")
            .logs_with_transaction_hash()
            .addresses([[0xab; 20]])
            .topic(0, [[0x01; 32]]);
        let json = serde_json::to_string(&manifest).expect("encode");
        assert!(json.contains(&format!("\"{}\"", hex(&[0xab; 20]))));
        assert_eq!(
            serde_json::from_str::<Manifest>(&json).expect("decode"),
            manifest
        );
    }

    #[test]
    fn a_manifest_with_an_unknown_field_is_refused() {
        let json = serde_json::to_string(&Manifest::new("transfers")).expect("encode");
        let error = serde_json::from_str::<Manifest>(&json.replacen('{', "{\"extra\":1,", 1))
            .expect_err("unknown field");
        assert!(
            error.to_string().contains("unknown field `extra`"),
            "{error}"
        );
    }

    #[test]
    fn write_operations_round_trip_through_their_codes() {
        for operation in [
            WriteOperation::Put,
            WriteOperation::Delete,
            WriteOperation::StatePut,
            WriteOperation::StateDelete,
            WriteOperation::IndexPut,
            WriteOperation::IndexDelete,
        ] {
            assert_eq!(WriteOperation::try_from(operation as u32), Ok(operation));
        }
        assert_eq!(WriteOperation::try_from(6), Err(6));
    }

    #[test]
    fn hex_rejects_wrong_lengths_and_digits() {
        assert_eq!(parse_hex::<2>("0x0aFf"), Ok([0x0a, 0xff]));
        assert!(parse_hex::<2>("0x0a").is_err());
        assert!(parse_hex::<2>("0x0g00").is_err());
    }
}
