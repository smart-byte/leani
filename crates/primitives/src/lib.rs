//! Source-neutral chain, capability, provenance, verification, and cursor types.
//!
//! Source adapters may convert to and from ecosystem-specific representations,
//! but RPC, dataset, networking, and database types do not cross this crate's
//! public contract.

pub mod alloy;
pub mod capability;
pub mod chain;
pub mod cursor;
pub mod durable;
pub mod frame;
pub mod material;
pub mod provenance;

pub use capability::{Capability, CapabilitySet, FrameCapabilityReport};
pub use chain::{
    Address, BlockHash, BlockNumber, BlockRange, BlockRangeError, BlockRef, CanonicalChainEvent,
    CanonicalKey, ChainId, Finality, Quantity, TransactionHash,
};
pub use cursor::{
    ChangeCursor, CursorError, CursorKind, OpaqueCursor, ProcessorCursor, SourceCursor,
};
pub use durable::{BLOCK_FRAME_SCHEMA_VERSION, DurableError, DurableKind};
pub use frame::{
    BlobSidecar, BlockFrame, HeaderEnvelope, Log, LogField, LogFieldSet, ReceiptEnvelope,
    StateDiff, Trace, TransactionEnvelope, Withdrawal,
};
pub use material::{Completeness, FilterScope, Material, MaterialKind, MissingReason, TopicFilter};
pub use provenance::{
    CheckStatus, ConsensusAnchor, ObjectIdentity, Provenance, SourceId, SourceIdError, SourceKind,
    TrustModel, VerificationCheck, VerificationReport,
};

/// The public project name used in status and client-version responses.
pub const PROJECT_NAME: &str = "leani";
