//! Durable internal cursors and checksummed opaque external representation.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    BlockHash, BlockNumber, ChainId, Finality, SourceId,
    durable::{self, DurableError, DurableKind},
};

const CURSOR_SCHEMA_VERSION: u16 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CursorKind {
    Processor,
    Change,
    Source,
}

impl CursorKind {
    const fn durable(self) -> DurableKind {
        match self {
            Self::Processor => DurableKind::ProcessorCursor,
            Self::Change => DurableKind::ChangeCursor,
            Self::Source => DurableKind::SourceCursor,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProcessorCursor {
    pub processor_id: String,
    pub processor_version: String,
    pub chain_id: ChainId,
    pub block_number: BlockNumber,
    pub block_hash: BlockHash,
    pub finality: Finality,
    pub sequence: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChangeCursor {
    pub chain_id: ChainId,
    pub processor_id: String,
    pub sequence: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SourceCursor {
    pub source_id: SourceId,
    pub chain_id: ChainId,
    pub block_number: BlockNumber,
    pub block_hash: BlockHash,
    pub source_position: Vec<u8>,
}

/// Text-safe cursor. The representation is intentionally opaque to clients.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpaqueCursor(String);

impl OpaqueCursor {
    /// Encode a versioned cursor as lowercase hexadecimal.
    ///
    /// # Errors
    ///
    /// Returns [`CursorError`] if serialization fails.
    pub fn encode<T: Serialize>(kind: CursorKind, value: &T) -> Result<Self, CursorError> {
        let bytes = durable::encode(kind.durable(), CURSOR_SCHEMA_VERSION, value)?;
        Ok(Self(hex::encode(bytes)))
    }

    /// Decode an exact cursor type and reject corrupt or cross-endpoint values.
    ///
    /// # Errors
    ///
    /// Returns [`CursorError`] for invalid text, checksum, kind, schema, or
    /// payload.
    pub fn decode<T: serde::de::DeserializeOwned>(
        &self,
        kind: CursorKind,
    ) -> Result<T, CursorError> {
        let bytes = hex::decode(&self.0).map_err(CursorError::Text)?;
        durable::decode(kind.durable(), CURSOR_SCHEMA_VERSION, &bytes).map_err(Into::into)
    }

    /// Parse without validating a cursor kind. Validation always occurs at
    /// decode time.
    ///
    /// # Errors
    ///
    /// Returns [`CursorError::Text`] if the value is not even-length
    /// hexadecimal.
    pub fn parse(value: impl Into<String>) -> Result<Self, CursorError> {
        let value = value.into();
        hex::decode(&value).map_err(CursorError::Text)?;
        Ok(Self(value))
    }

    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Error)]
pub enum CursorError {
    #[error("cursor text is invalid: {0}")]
    Text(hex::FromHexError),
    #[error(transparent)]
    Durable(#[from] DurableError),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cursor() -> ChangeCursor {
        ChangeCursor {
            chain_id: ChainId(1),
            processor_id: "fixture".to_owned(),
            sequence: 77,
        }
    }

    #[test]
    fn cursor_round_trip_is_kind_bound() {
        let opaque = OpaqueCursor::encode(CursorKind::Change, &cursor()).expect("encode cursor");
        assert_eq!(
            opaque
                .decode::<ChangeCursor>(CursorKind::Change)
                .expect("decode cursor"),
            cursor()
        );
        assert!(opaque.decode::<ChangeCursor>(CursorKind::Source).is_err());
    }

    #[test]
    fn changed_cursor_text_is_rejected() {
        let opaque = OpaqueCursor::encode(CursorKind::Change, &cursor()).expect("encode cursor");
        let mut bytes = opaque.expose().as_bytes().to_vec();
        let last = bytes.last_mut().expect("non-empty");
        *last = if *last == b'0' { b'1' } else { b'0' };
        let changed = OpaqueCursor::parse(String::from_utf8(bytes).expect("ASCII")).expect("hex");
        assert!(changed.decode::<ChangeCursor>(CursorKind::Change).is_err());
    }

    #[test]
    fn all_cursor_payloads_round_trip() {
        let processor = ProcessorCursor {
            processor_id: "fixture".to_owned(),
            processor_version: "1.0.0".to_owned(),
            chain_id: ChainId(1),
            block_number: BlockNumber(12),
            block_hash: BlockHash::new([0x12; 32]),
            finality: Finality::Finalized,
            sequence: 9,
        };
        let source = SourceCursor {
            source_id: SourceId::new("fake").expect("source ID"),
            chain_id: ChainId(1),
            block_number: BlockNumber(12),
            block_hash: BlockHash::new([0x12; 32]),
            source_position: vec![1, 2, 3],
        };
        let encoded =
            OpaqueCursor::encode(CursorKind::Processor, &processor).expect("processor cursor");
        assert_eq!(
            encoded
                .decode::<ProcessorCursor>(CursorKind::Processor)
                .expect("decode processor cursor"),
            processor
        );
        let encoded = OpaqueCursor::encode(CursorKind::Source, &source).expect("source cursor");
        assert_eq!(
            encoded
                .decode::<SourceCursor>(CursorKind::Source)
                .expect("decode source cursor"),
            source
        );
    }
}
