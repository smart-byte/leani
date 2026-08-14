# Leani changelog

All notable changes are documented here. The format follows Keep a Changelog,
and releases use semantic versioning for native API, SDK, processor, durable
record, and documented RPC contracts.

## [Unreleased]

### Changed

- Execution-P2P history fallback now admits every gap at or after the
  processor's configured start block by default. The optional
  `sources.live.history_fallback_blocks` setting applies a hard finalized
  suffix bound only when operators configure it explicitly.
- Query-extension aliases now live under the collision-free `/v1/q/{alias}`
  namespace. Typed SDK helpers accept canonical capability `basePath`
  overrides for ambiguous multi-instance deployments.
- Query-extension scan cursors use the pre-launch v2 encoding and bind
  pagination to extension-defined request scopes. Cursors emitted by the
  unreleased v1 implementation are intentionally not accepted.
- Change envelopes no longer repeat connection-scoped metadata: the
  `processor` summary is gone and `coverage` remains only on
  `reset_required` as the coverage hint. Streams now open every connection
  with a synthesized `hello` frame (`{apiVersion, chainId, processor,
  coverage}`, no SSE id); the SDK exposes it via the `onHello` subscribe
  callback. Poll pages keep their single top-level `coverage`.
- `evm-events` and `transaction-stats` public change/entity JSON now uses
  the same camelCase, `0x`-hex, decimal-string conventions as the other
  processors instead of raw serde output (snake_case keys, byte-array
  addresses, PascalCase finality). SDK gains `EvmEvent` and
  `TransactionStats` types.
- `emittedAt` is now RFC 3339 UTC with millisecond precision
  (`2026-08-06T14:05:51.194Z`) instead of the nonstandard `unix-ms:` prefix
  encoding.

### Added

- Processor-owned native query extensions with canonical instance-scoped
  routes, collision-safe optional aliases, bounded read-only contexts,
  capability discovery, a safe same-origin SDK request primitive, and a
  complete downstream custom-processor example.
- Independent Rust workspace and Bun-compatible TypeScript SDK with strict
  configuration, diagnostics, lifecycle, CI, dual licensing, release
  artifacts, container/Compose/systemd deployment, and operations contracts.
- Source-neutral chain frames with explicit material completeness,
  capabilities, provenance, trust, verification, finality, checksummed
  durable encodings, and interchangeable source interfaces.
- Direct bounded Xatu Parquet projection, checksummed local normalized
  archives, and sparse EraE history reads with local execution-commitment
  verification and no full archive retention.
- Persistent Reth execution P2P ingestion, verified Beacon API finality, and
  a self-contained consensus-P2P light client with embedded Mainnet bootnodes.
- Bounded restartable cold runtime, capability/trust selection, source
  failover from durable coverage, shared live fanout, canonical reorg
  undo/apply, finality advancement, and deterministic recovery.
- SQLite WAL state, entities/indexes, undo, coverage, change streams, outbox,
  jobs, recent canonical frames, pruning, attribution, integrity checks,
  backup, restore, and compaction.
- blobs.money, ERC-20 watchlist, and Uniswap V2/V3 processors with typed
  coverage-aware API queries and resumable SSE changes.
- Exact recent and bounded on-demand EVM-free Ethereum block, transaction,
  receipt, and log RPC methods, plus post-commit WebSocket `newHeads` and log
  subscriptions with removed-log reorg delivery.
- Final EIP-7910 `eth_config` backed by exact checked Mainnet activation
  timestamps, execution blocks, fork hashes, precompiles, system contracts,
  and blob parameters.
- Cross-source frame/processor conformance and exact RPC differential tools,
  benchmark reports, readiness, Prometheus metrics, runbooks, and threat
  model.
- One process-wide Reth execution network with a stable node identity,
  bounded authoritative peer metadata, Discv5/inbound configuration,
  peer-capacity-aware request gating, adaptive material batches, incremental
  backfill progress, and durable canonical live resume.
- Late interchangeable-archive reconciliation of P2P-derived processor output
  using durable per-block delta checksums, without extending raw-frame
  retention.

### Fixed

- Correct processor schema OpenAPI string types, reject unsafe processor
  instance route segments, range-scope blob block cursors, and paginate blob
  transaction lists without silent truncation.
- Derive dashboard processor lag from the observed execution peer head rather
  than an in-flight next-block probe, and expose both values independently.
- Clear live readiness before a material-failure reconnect waits for peers.
- Keep Xatu beacon `block_total_bytes` separate from Ethereum execution-block
  RLP size. Derive the latter exactly from fork-aware header, transaction, and
  withdrawal inputs and fail closed on unknown future formats.
- Replace approximate blobs.money BPO block boundaries with the exact BPO1
  block `23,975,778` and BPO2 block `24,179,383`.
- Omit the obsolete `totalDifficulty` extension from canonical post-Merge RPC
  blocks.

## [0.1.0] - Unreleased

The first tag remains gated on the release candidate checks and current
preview limitations.
