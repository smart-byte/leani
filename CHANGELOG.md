# Leani changelog

All notable changes are documented here. The format follows Keep a Changelog,
and releases use semantic versioning for native API, SDK, processor, durable
record, and documented RPC contracts.

## [Unreleased]

### Security

- Updated Reth's transitive `imbl` dependency to 7.0.2 and
  `imbl-sized-chunks` to 0.2.0 to address RUSTSEC-2026-0292. This also removes
  the unmaintained `bitmaps` dependency and its policy exceptions.

## [0.1.0-rc.1] - 2026-09-20

The first public release is a preview. Review the documented preview
limitations before deploying it.

### Changed

- Prepared the processor-authoring Rust libraries for crates.io with explicit
  package contents, MIT notices, READMEs, and matching prerelease dependency
  versions. Other workspace crates remain private packages.
- Standardized Leani's code, packages, and release metadata on the MIT license.
  Dependencies and third-party data retain their respective licenses.
- Release publication now requires passing CI for the exact release commit.
  Native binary releases reuse an inspected candidate, exercise the offline
  fixture on all four target architectures, and distinguish prereleases from
  stable releases. SDK package checks cover both public entry points.
- Updated `h2` to 0.4.16 and `rustls` to 0.23.45 for security fixes, and
  replaced the yanked `chacha20` 0.10.1 dependency with 0.10.2.
- Chain-confidence vocabulary is now `preview`, `included`, and `finalized` on
  every `finality` field: CLI output, HTTP API, SSE, SDK types, and
  configuration. `optimistic` becomes `included`; `safe` is removed because no
  source produced it. `publish = "optimistic_and_finalized"` is now
  `included_and_finalized`, and `leani subscribe --finality` accepts `included`
  (default) or `finalized`. Rendered `data.finality` always equals the envelope
  `finality`. Node stores (schema 21) and embedded subscription directories
  created before this change are refused and must be deleted.
- Execution peer discovery now has one Discv4 owner: Reth's network manager.
  Leani continues to feed independently decoded EIP-1459 DNS records into the
  same bounded peer manager without running a second Discv4 crawler.
- `LiveSource::subscribe` now receives the compiled `DataRequest`, allowing
  sources to acquire only the material required by the active processor set.
- Uniswap 2.1 public entity/change JSON now uses camelCase fields, `0x` address
  and hash strings, decimal quantities, and signed decimal swap amounts.
- Compact finality endpoint lists replace the managed defaults when supplied,
  and runtime data directories are exclusively locked across processes.
- The CLI now discovers `./leani.toml` by default; `LEANI_CONFIG` and
  `--config` remain explicit overrides.
- Execution peer candidates and verified service evidence now use the bounded
  `execution-network.sqlite` WAL store with background incremental persistence;
  the pre-launch JSON peer-state formats are intentionally not migrated.
- Execution peer qualification is now a background ranking signal rather than
  a startup gate. Requests begin at the configured connected-peer floor and
  continue to commitment-check every response.
- `ProcessorFactory::id` accepts an ordinary borrowed string again, and
  `description` has a backwards-compatible default for downstream factories.
- Public documentation and the Astro landing page now live in this monorepo,
  render from verified examples and tracked Rust-owned contract fixtures, and
  promote production docs only from an exact release commit.
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

- `leani subscribe blocks` and the built-in `block-summary` 1.1.0 processor,
  including latest/by-number native queries and typed SDK helpers.
- `leani subscribe uniswap-v3 MARKET...`, embedded/attached auto-detection,
  exact V3 price and base-token volume output, stable JSON/raw formats, and
  scoped cold-start reset commands.
- Processor-owned native query extensions with canonical instance-scoped
  routes, collision-safe optional aliases, bounded read-only contexts,
  capability discovery, a safe same-origin SDK request primitive, and a
  complete downstream custom-processor example.
- Independent Rust workspace and Bun-compatible TypeScript SDK with strict
  configuration, diagnostics, lifecycle, CI, MIT licensing, release
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

- `publish = "finalized_only"` is now enforced. Included-block changes are held
  in the store until verified finality promotes them, their bytes count against
  the delivery budget from admission, undo records are never published, and the
  change stream carries only finalized `apply` records and `system.finality`
  markers. `leani subscribe --finality finalized` now honors that promise in
  both embedded and attached mode; attached subscriptions against an
  `included_and_finalized` node print each block once a finality marker covers
  it. Previously every finalized-only processor published included apply and
  undo records, and the attached CLI never printed anything under
  `--finality finalized`. `query-and-follow` now rejects finalized-only
  processors with `finalized_only_snapshot`, because retained state includes
  blocks whose changes are still unpublished and the stream never emits an
  undo for them.
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
