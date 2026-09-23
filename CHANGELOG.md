# Leani changelog

All notable changes are documented here. The format follows Keep a Changelog,
and releases use semantic versioning for native API, SDK, processor, durable
record, and documented RPC contracts.

## [Unreleased]

### Security

- Updated Reth's transitive `imbl` dependency to 7.0.2 and
  `imbl-sized-chunks` to 0.2.0 to address RUSTSEC-2026-0292. This also removes
  the unmaintained `bitmaps` dependency and its policy exceptions.

### Changed

- `THIRD_PARTY_LICENSES.txt` is now committed, verified against `Cargo.lock`
  in CI, and linked from the README and site. It adds the Apache-2.0 NOTICE
  files of the Arrow, Parquet, object_store, and Moka dependencies, which the
  generated listing previously omitted. The Homebrew package now also installs
  Leani's own `LICENSE`, and the container image declares OCI license metadata.
- Node stores move to schema 22, which adds coverage lookup indexes. A
  schema-21 store upgrades in place on its next start; earlier binaries then
  refuse it, so keep a backup if you might roll back.
- Breaking for processor authors (`leani-processor-api`):
  `DataRequirement::validate_frame` is stricter for requirements with
  `allow_filtered`. Each filtered component that can supply a required
  capability, directly or by derivation, must claim predicate completeness
  (`Completeness::Partial` is rejected), and its `FilterScope` must cover the
  requirement's `filter` at the frame's block. Material filtered for another
  consumer's predicate no longer passes.
- Breaking (`leani-processor-api`): `ProcessorInstanceId::legacy` returns
  `Result<ProcessorInstanceId, ProcessorInstanceIdError>` instead of panicking
  when the derived key is invalid, which happens for a version with `+` build
  metadata or when the ID and version exceed 192 bytes together. Decoding a
  descriptor without an `instance` reports that as an error.
- Breaking (`leani-primitives`, `leani-processor-api`): decoding validates
  values the way their constructors do. A `BlockRange` whose end precedes its
  start, `CapabilitySet` or `LogFieldSet` bits that name no capability or log
  field, and a `ProcessorId` that `ProcessorId::new` rejects all fail to
  decode. Encodings are unchanged; new golden fixtures pin the `BlockFrame`,
  `ProcessorCursor`, and `EncodedDelta` durable encodings.
- `leani-primitives`: `BlockRange::len` saturates at `u64::MAX` for the full
  `0..=u64::MAX` range instead of overflowing, and the new
  `BlockRange::checked_len` returns `None` for it. The new
  `FilterScope::covers` and `FilterScope::covers_at` check whether material
  filtered by one scope is complete for another filter.

### Fixed

- New node stores enable SQLite incremental auto-vacuum, so pruning shrinks
  the database file and storage admission recovers instead of staying at the
  high-water mark. Existing stores log a warning at startup until one
  `leani db compact` converts them.
- The node database path is used literally: `%` and `?` in `data_dir` no
  longer make SQLite open a different file.
- A new store's schema is created in one transaction, so an interrupted first
  start no longer leaves a store that every later start refuses.
- Retained recent frames report their block's current finality after
  finality promotes it. Recompute backfills over the retained recent window no
  longer fail with "historical microbatch must be contiguous, finalized, and
  single-chain", and live-lane replays apply promoted blocks as finalized.
- `publish = "finalized_only"` processors with canonical delivery ordering
  publish their held blocks before a block applied directly as finalized, such
  as after gap recovery, so the stream stays in block order.
- Undoing a block restores each output entity's previous block, timestamp,
  finality, and write time instead of stamping the reverted block. Undo
  records written by earlier versions keep the old stamping.
- Rows that a window output policy pruned while applying an unfinalized block
  come back when that block is undone.
- A full artifact budget no longer stalls finality for every processor.
  Finality commits, finalized artifact candidates stay pending with a warning,
  and a later finality advance promotes them once the retained budget has
  room.
- The undo journal no longer grows forever. Blocks applied as already
  finalized record no undo, and each finality advance deletes finalized
  records more than `[processors.undo] safety_blocks` below the finalized
  height, or all of them with `mode = "none"`. Unfinalized blocks keep their
  records, so reorgs undo as before. A backlog from earlier versions drains
  by at most 10,000 records per finality advance.
- Automatic recovery checkpoints are taken at most once per 1,000 finalized
  blocks instead of on every finalized block and finality event, and they
  stream processor state instead of loading it whole. `keep` is unchanged. A
  completed historical job also checkpoints its processor at rest, and a
  restore accepts a checkpoint whose block was finalized after it was taken.
  A restore still needs a checkpoint at the current cursor, so between
  checkpoints use a portable savepoint or a full-store backup.
- Checkpoints and portable savepoints stay valid after lifecycle edits such
  as raising a limit: their identity is the processor contract without its
  lifecycle policies. Snapshots written by earlier versions stay valid while
  the descriptor is unchanged.
- Each processor instance keeps at most 16 portable savepoints, and a new one
  is admitted against the physical store budget. The API answers a full quota
  with `409 savepoint_limit`.
- Coverage that arrives out of order, such as an older backfill range that
  finishes after a newer one, is now owned and therefore compacted. Adjacent
  owner ranges coalesce.
- Re-applying a block that coverage compaction already folded into a compact
  interval is an idempotent no-op instead of re-running the reducer and
  publishing duplicate changes. Hot/cold handoff verification accepts
  compacted blocks through their segment's end hash. Compaction leaves the
  overlap of a running handoff exact until it is verified, logging that once
  per handoff, and a new handoff fails any unverified one left by an
  interrupted run.
- Reducer prefix scans (`scan_prefix`, `state_scan_prefix`) keep reading
  until `limit` rows survive the reducer's own uncommitted deletes, so pages
  are no longer short and an uncommitted key no longer skips ahead of unread
  committed keys.
- A `required` consumer is rejected on a live stream whose delivery mode is
  not `until_acknowledged`, where it would pin window retention indefinitely.
  Backfill streams still accept it. Consumers registered earlier keep their
  role, and re-creating one still returns `409 consumer_exists`. A store that
  holds such consumers logs a startup warning listing them: revoke each one
  (`DELETE /v1/processors/{processor}/consumers/{consumer}`) and register a
  `best_effort` consumer under a new ID, or switch the processor to
  `until_acknowledged` delivery. The durable-consumer SDK example now
  registers a best-effort consumer, matching the window-retained stream of
  the PostgreSQL guide.
- Deleting terminal backfill work removes its subscription ranges and coverage
  ownership even when the job ID differs from the subscription ID.
- A late microbatch commit or completion no longer overwrites a cancelled or
  failed job, and a late state change no longer revives a cancelled, failed,
  or reclaimable subscription.
- Store stats and `/metrics` report changes held for `finalized_only`
  processors: `deferred_changes`, `deferred_change_bytes`,
  `leani_store_deferred_changes`, and `leani_store_deferred_change_bytes`.
- Refusing an older store schema names the actual reason instead of always
  claiming the store predates the included/finalized vocabulary.
- `/health/live` and `/health/ready` no longer count whole store tables on
  every probe, which could exceed the container health check's 3-second
  timeout on large stores. They answer from in-memory readiness plus a trivial
  SQLite query. `/metrics` and `/v1/network/status` reuse store-wide and
  per-processor row and byte counts for up to 10 seconds, and `/v1/status`
  reads the database size from page counts.
- A processor's finalized height and coverage head are found with index
  searches instead of scanning all of its coverage on each metrics scrape,
  status poll, or undo, and block-hash coverage lookups are indexed.
- Read-only API paths no longer queue for the store writer, where each one
  held back history writers (they yield to every waiting live writer), so
  steady API traffic could starve backfill. Consumer change pages, change
  bounds, and delivery stream listings and statistics read without it and no
  longer register an unknown processor as a side effect, and re-registering
  an unchanged processor writes nothing. Query snapshots check coverage,
  retention, and size caps before taking the writer and stop counting once a
  cap is exceeded, so `query_too_expensive` reports at least that many rows
  and bytes; only the copy itself waits for the writer.
- Historical backfills no longer reuse retained recent frames whose material
  was filtered for other processors, which could silently produce incomplete
  results. Such frames now miss and the job reads from its source.
- A paused processor lane that resumes no longer replays retained frames
  filtered for the processors that stayed live meanwhile. Live-gap replay
  treats such a frame as missing: it recovers a finalized block from the
  configured history source. At an unfinalized block, as for a block that was
  never retained, only that processor's lane pauses
  (`unfinalized_gap_waiting_for_finality`); the first drain after finality
  reaches the block recovers it from history, with no operator reset. A
  mapping failure during block-local gap replay now also fails only that
  processor's lane (`processor_live_mapping_failed`), as ordered replay already
  did, instead of stopping shared live ingestion. The replay-miss log is now
  at debug level, since a waiting lane retries on every drain.
- A source request compiled for several filtering requirements no longer
  narrows a field that one requirement leaves unfiltered. Previously an
  unfiltered field such as `addresses` took another requirement's list, so the
  source could omit material the unfiltered requirement needed.
- `coverage_gaps` in `leani-source-api` no longer reports a range covered
  through block `u64::MAX` as missing.
- A processor whose reducer rejects a block no longer stops live ingestion for
  every processor, and a restart no longer loops on the same block. Reducer
  errors, and the store's per-apply invariant errors, fail only that
  processor's live lane (`processor_live_reduce_failed`) while the others keep
  following. This holds for every processor mode and delivery ordering,
  including block-local processors with the default canonical ordering, which
  used to stop the whole lane. A delta that conflicts with the one already
  applied for its block (`processor_live_delta_conflict`), and a mapping or
  finality-variant failure while replaying (`processor_live_mapping_failed`),
  likewise fail only that lane. Live commits and every live-gap replay path
  apply this one isolation policy, so a replay after a restart or an operator
  reset parks the lane again instead of failing the live lane. A failed lane
  keeps its first failure: it is mapped only to move its first unapplied block
  across a reorg, a later failure does not change its reason, and a later park
  does not move its first unapplied block. Only a finality contradiction
  (`processor_finality_conflict`) replaces the reason. The store applies this
  rule atomically, so a park that races another component failing the lane,
  such as finality, cannot unfail it; the store still records the first
  unapplied block of a lane it fails itself for a delivery limit. The live-lane
  reset route (`POST /admin/v1/processors/{processor}/lanes/live/reset`) now
  accepts processors with canonical delivery ordering, which include every
  ordered processor, instead of answering
  `409 processor_has_no_split_live_stream`.
- A failed automatic cold backfill, or a failed hot/cold overlap verification,
  no longer ends the network lanes for every processor. That processor's
  handoff is recorded as failed and its live lane pauses at the live tip
  (`hot_cold_handoff_failed`); the other handoffs and live lanes continue. A
  block-local lane replays and resumes at once. An ordered lane resumes once a
  later backfill passes its gap: ordered replay now moves past a parked block
  that history has already applied, instead of waiting for it forever.
- A lane whose gap crosses the finalized anchor the node seeds at startup no
  longer fails the whole live lane on every restart. The seeded canonical row
  has no parent hash until the block's frame arrives; retaining that frame now
  fills in its parent and timestamp, and a gap check treats a missing parent as
  unknown instead of as a broken chain. A gap whose next canonical block truly
  does not descend from it fails only that lane
  (`live_gap_canonical_identity_changed`).
- The live source request now covers every configured processor, including
  lanes that are paused when the live lane subscribes. Frames retained while a
  lane is paused carry its material, so it resumes by replaying them. A reorg no
  longer fails a paused lane that cannot map the replacement block: the lane
  stays paused, its gap moves to the replacement, and its own replay maps it.
  A reorg that replaces blocks at or below a parked lane's gap moves the gap
  down to the first replacement block. Before, an ordered lane stalled and a
  block-local lane skipped the replacement blocks below its gap, or, after a
  reorg to a shorter branch, resumed with a hole. A reorg without a
  replacement branch no longer leaves a parked lane's gap on a block the chain
  no longer has: the gap moves onto the new tip, which the lane has applied,
  so a paused lane resumes at once, and a failed lane's gap moves onto the
  tip's successor once that block arrives, where a reset replays it. Before,
  a block-local lane skipped the next block or failed
  (`live_gap_canonical_identity_changed`), and an ordered lane stalled. A gap
  found off the canonical chain, as earlier versions could leave one, moves
  back to the last canonical block the lane applied, instead of being
  completed past blocks the lane never applied or failing the lane.
- The shared live lane checks continuity before any processor maps a block. A
  block must extend the canonical tip or repeat a block already retained, and a
  reorg must first revert the canonical tip. A source that breaks this fails
  the lane with `LiveGap` or `InvalidReorg`, and the supervisor reconnects,
  instead of the block being applied.
- The recent-frame hard limit, `budgets.recent_raw_hard_bytes`, now applies
  when live ingestion retains a frame, not only when finality prunes. At the
  limit live ingestion waits with readiness down (`recent_storage_full` in the
  log) and resumes once finality prunes; no unfinalized frame is dropped. After
  15 minutes at the limit the live lane fails, so the supervisor restarts it
  from a fresh finalized anchor that finality can prune through. Finality no
  longer fails its lane when pruning leaves frames above the hard limit; it
  logs a warning.
- One processor's failed finality update no longer stops finality for the
  others or recent-frame pruning. It is logged, reported under
  `failed_processors` in the shared finality report, and retried at the next
  finalized anchor. A processor whose coverage holds the finalized hash at
  another height contradicts the canonical chain: its lane fails
  (`processor_finality_conflict`), and that reason replaces any earlier failure
  reason. The reset route refuses that lane with
  `409 live_lane_requires_rebuild`, since only rebuilding the processor as a
  replacement instance repairs it. Finality skips any failed lane until it is
  reset, so no later anchor finalizes a contradicted lane through the
  contradicted height. A gap replay that finds its lane failed meanwhile, for
  example by finality, leaves the lane failed instead of failing the whole
  live lane.
- Backfills that reuse retained recent frames read them in bounded batches, one
  query each: at most `budgets.history_pipeline.commit.maximum_blocks` blocks
  and `maximum_mapped_bytes` encoded bytes, each holding an active-chunk slot.
  A long gap no longer loads the whole recent window into memory with one
  query per block.
- The live lane's gap drains, live commits, and the hot/cold handoff's
  reconciliation no longer race each other over one lane's gap. Two drains of
  the same gap could fail the live lane with "live gap marker changed".
- A processor's retained raw-history source now uses the filter its own
  backfill requests carry, which covers every requirement, instead of the first
  requirement's filter. With several differently filtered requirements, the
  source's material shape never matched those requests, and its filter could
  omit material another requirement needed.
- Network telemetry no longer panics when it shortens a long error message that
  contains non-ASCII text, such as one from a Beacon API endpoint. The panic
  stopped the network supervisor. Messages are now cut at a character boundary.
- A backfill paused by delivery or artifact backpressure, such as a
  subscription whose consumer stopped acknowledging, no longer holds history
  resources while it waits. It used to keep its node-wide active-chunk slots
  (`budgets.history_pipeline.maximum_active_chunks`), the chunks it had read
  ahead, its place in shared source reads, and the mapped blocks of the commit
  batch it could not deliver, counted against
  `budgets.history_pipeline.maximum_mapped_bytes`. So it could stop every other
  historical job, including the cold backfills a hot/cold handoff waits for,
  and hold jobs that shared a source read with it to its pace. A paused job now
  keeps only the one mapped block whose commit was refused. Once it commits,
  the job resumes from its durable progress and reads and maps the blocks it
  dropped again.
- Chunks that a backfill opens ahead of the one it is reading can use at most
  half of `budgets.history_material.memory_bytes`. The rest stays free for
  chunks being read. Before, read-ahead chunks could fill the whole budget
  while the chunk the job needed next waited for memory, and the job stalled
  for good.
- A historical backfill no longer stalls when shared material memory, or room
  in a shared read's frame buffer, is freed just as its reader finds it full.
  The reader now registers for the wakeup before it checks.
- A history source that panics while streaming fails its read with a
  retryable source error. Its backfills retry, on another source when one is
  configured. Before, every backfill waiting on that read hung.
- Transient history-source errors are retried per gap, not per job, and a gap
  that committed blocks before failing starts a fresh retry budget. A long
  backfill with a few scattered transient errors no longer fails. Each gap is
  read from the highest-priority source, and the job moves to the next source
  only after an error. Before, every gap attempt moved on to the next source,
  even after a success.
- A historical job whose task stops just after the node-wide commit scheduler
  picks it no longer leaves every other historical job waiting for a commit
  turn.

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
