//! Scripted source, fork, finality, crash, and API fixtures.

mod benchmark;
mod fixtures;
mod processors;
mod sources;

pub use benchmark::{
    GeneratedHistorySource, GeneratedSourceStats, SyntheticCorpusKind, SyntheticCorpusManifest,
    synthetic_corpus_manifest, uniswap_weth_usdc_pool, update_frame_digest,
};
pub use fixtures::{default_source_budget, fixture_frame, fixture_source_descriptor};
pub use leani_source_api::{
    ConformanceError, CrossSourceReport, FrameDifference, FrameFingerprint,
    compare_frame_sequences, frame_fingerprint,
};
pub use processors::{BlockLocalCounter, MemoryReducer, OrderedLedgerProcessor};
pub use sources::{
    FinalityStep, HistoryStep, LiveStep, ScriptedChunk, ScriptedFinalitySource,
    ScriptedHistorySource, ScriptedLiveSource,
};
