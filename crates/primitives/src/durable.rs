//! Checksummed, versioned durable envelope independent of Rust memory layout.

use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;

const MAGIC: &[u8; 4] = b"IDXN";
const ENVELOPE_VERSION: u16 = 1;
const HEADER_LEN: usize = 16;
const CHECKSUM_LEN: usize = 32;

/// Current durable payload schema for [`crate::BlockFrame`].
pub const BLOCK_FRAME_SCHEMA_VERSION: u16 = 1;

/// Stable durable record discriminator.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u16)]
pub enum DurableKind {
    BlockRef = 1,
    BlockFrame = 2,
    ProcessorCursor = 10,
    ChangeCursor = 11,
    SourceCursor = 12,
    ProcessorDelta = 20,
    ProcessorArtifactExport = 21,
}

/// Encode one schema-versioned value with length and BLAKE3 integrity.
///
/// # Errors
///
/// Returns [`DurableError::Encode`] if the value cannot be serialized or
/// [`DurableError::Oversized`] if its payload exceeds `u32::MAX`.
pub fn encode<T: Serialize>(
    kind: DurableKind,
    schema_version: u16,
    value: &T,
) -> Result<Vec<u8>, DurableError> {
    let payload = postcard::to_allocvec(value).map_err(DurableError::Encode)?;
    let payload_len = u32::try_from(payload.len()).map_err(|_| DurableError::Oversized)?;
    let mut output = Vec::with_capacity(HEADER_LEN + payload.len() + CHECKSUM_LEN);
    output.extend_from_slice(MAGIC);
    output.extend_from_slice(&ENVELOPE_VERSION.to_be_bytes());
    output.extend_from_slice(&(kind as u16).to_be_bytes());
    output.extend_from_slice(&schema_version.to_be_bytes());
    output.extend_from_slice(&0_u16.to_be_bytes());
    output.extend_from_slice(&payload_len.to_be_bytes());
    output.extend_from_slice(&payload);
    let checksum = blake3::hash(&output);
    output.extend_from_slice(checksum.as_bytes());
    Ok(output)
}

/// Decode an exact durable kind and schema version.
///
/// # Errors
///
/// Rejects malformed, corrupt, wrong-kind, or unknown-version values.
pub fn decode<T: DeserializeOwned>(
    expected_kind: DurableKind,
    expected_schema_version: u16,
    bytes: &[u8],
) -> Result<T, DurableError> {
    if bytes.len() < HEADER_LEN + CHECKSUM_LEN {
        return Err(DurableError::Truncated);
    }
    if &bytes[..4] != MAGIC {
        return Err(DurableError::Magic);
    }
    let envelope_version = u16::from_be_bytes([bytes[4], bytes[5]]);
    if envelope_version != ENVELOPE_VERSION {
        return Err(DurableError::EnvelopeVersion(envelope_version));
    }
    let kind = u16::from_be_bytes([bytes[6], bytes[7]]);
    if kind != expected_kind as u16 {
        return Err(DurableError::Kind {
            expected: expected_kind as u16,
            actual: kind,
        });
    }
    let schema_version = u16::from_be_bytes([bytes[8], bytes[9]]);
    if schema_version != expected_schema_version {
        return Err(DurableError::SchemaVersion {
            expected: expected_schema_version,
            actual: schema_version,
        });
    }
    if bytes[10..12] != [0, 0] {
        return Err(DurableError::Reserved);
    }
    let payload_len = u32::from_be_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]) as usize;
    let expected_len = HEADER_LEN
        .checked_add(payload_len)
        .and_then(|length| length.checked_add(CHECKSUM_LEN))
        .ok_or(DurableError::Oversized)?;
    if bytes.len() != expected_len {
        return Err(DurableError::Length {
            declared: payload_len,
            actual: bytes.len().saturating_sub(HEADER_LEN + CHECKSUM_LEN),
        });
    }
    let checksum_start = HEADER_LEN + payload_len;
    let expected_checksum = blake3::hash(&bytes[..checksum_start]);
    if bytes[checksum_start..] != expected_checksum.as_bytes()[..] {
        return Err(DurableError::Checksum);
    }
    postcard::from_bytes(&bytes[HEADER_LEN..checksum_start]).map_err(DurableError::Decode)
}

#[derive(Debug, Error)]
pub enum DurableError {
    #[error("durable value is truncated")]
    Truncated,
    #[error("durable value has an invalid magic prefix")]
    Magic,
    #[error("unsupported durable envelope version {0}")]
    EnvelopeVersion(u16),
    #[error("durable kind mismatch: expected {expected}, received {actual}")]
    Kind { expected: u16, actual: u16 },
    #[error("durable schema mismatch: expected {expected}, received {actual}")]
    SchemaVersion { expected: u16, actual: u16 },
    #[error("durable reserved header bits are non-zero")]
    Reserved,
    #[error("durable payload length mismatch: declared {declared}, actual {actual}")]
    Length { declared: usize, actual: usize },
    #[error("durable checksum mismatch")]
    Checksum,
    #[error("durable payload is too large")]
    Oversized,
    #[error("failed to encode durable payload: {0}")]
    Encode(postcard::Error),
    #[error("failed to decode durable payload: {0}")]
    Decode(postcard::Error),
}

#[cfg(test)]
mod tests {
    use crate::{BlockHash, BlockNumber, BlockRef};

    use super::*;

    fn block() -> BlockRef {
        BlockRef {
            number: BlockNumber(42),
            hash: BlockHash::new([0x11; 32]),
            parent_hash: BlockHash::new([0x22; 32]),
            timestamp: 1_700_000_000,
        }
    }

    #[test]
    fn round_trip_and_corruption_detection() {
        let encoded = encode(DurableKind::BlockRef, 1, &block()).expect("encode");
        let decoded: BlockRef =
            decode(DurableKind::BlockRef, 1, &encoded).expect("decode exact schema");
        assert_eq!(decoded, block());

        let mut corrupt = encoded;
        corrupt[HEADER_LEN] ^= 1;
        assert!(matches!(
            decode::<BlockRef>(DurableKind::BlockRef, 1, &corrupt),
            Err(DurableError::Checksum)
        ));
    }

    #[test]
    fn unknown_schema_and_kind_fail_closed() {
        let encoded = encode(DurableKind::BlockRef, 1, &block()).expect("encode");
        assert!(matches!(
            decode::<BlockRef>(DurableKind::BlockRef, 2, &encoded),
            Err(DurableError::SchemaVersion { .. })
        ));
        assert!(matches!(
            decode::<BlockRef>(DurableKind::BlockFrame, 1, &encoded),
            Err(DurableError::Kind { .. })
        ));
    }
}
