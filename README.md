# Leani

Status: pre-release preview.

Leani is a lean, state-minimized Ethereum indexing node. Its Rust workspace
and Bun-compatible SDK compile and test. Historical Xatu,
checksummed normalized archives, and a native sparse EraE reader feed a durable
bounded runtime and SQLite reducer store. The EraE reader fetches only selected
e2store ranges, verifies transaction, receipt, withdrawal, and ommer
commitments, then discards raw bytes. Built-in blobs.money, ERC-20 watchlist,
and Uniswap V2/V3 processors expose coverage-aware queries and resumable
changes. The node also ships a capability-honest JSON-RPC metadata profile,
verified Beacon API and native consensus-P2P finality backends, a persistent Reth P2P source adapter,
bounded recent-frame storage, shallow-fork reconstruction, a shared
hot/cold/finality supervisor, bounded recent `eth_getLogs`, benchmark reports,
health, Prometheus metrics, reorg-correct `newHeads`/log WebSocket
subscriptions, and deployment artifacts.

The product, executable, Rust crates, SDK, configuration environment, metrics,
protocol media types, container, and deployment assets all use the Leani
identity. The CLI command is `leani`.

Trustless consensus anchoring of sparse EraE material, broad cross-source
production evidence, and the required Mainnet soaks are not complete. Bounded
Xatu/EraE blobs differentials now pass across Dencun and all later scheduled
fork formats. Recent and bounded on-demand canonical block,
transaction, receipt, and log methods decode source protocol bytes into Alloy's
canonical RPC types. On-demand calls use only a capability-complete configured
history source and discard frames after the response. Unsupported methods
continue to fail explicitly, and live readiness is only true while a
peer-backed subscription is usable. See
[preview limitations](docs/operations/preview-limitations.md) before deploying it.

## Mission

Leani is built to:

- backfill application-specific data much faster and more cheaply than JSON-RPC;
- follow the chain head while a historical backfill is still running;
- run deterministic, subscribable aggregations over blocks, transactions, receipts, and logs;
- expose the common Ethereum RPC methods that do not require EVM execution or historical state;
- swap between public datasets, history archives, and P2P without changing processors;
- retain no raw historical chain data by default.

The intended storage equation is:

```text
disk =
  application outputs
  + processor state/checkpoints
  + a short recent-block/reorg window
  + one bounded transient input chunk
  + optional indexes explicitly enabled by the operator
```

This is not a fully stateless system: useful aggregations such as token balances have application state. It is *Ethereum-state-minimized*: it does not maintain the EVM state trie or replay every transaction.

## Architecture

```text
             Historical lane                         Live lane
    Xatu Parquet / EraE / AWS / Google       Ethereum P2P + CL finality
                       \                       /
                        Source adapters
                              |
                  normalized BlockEnvelope
                              |
                 validation + canonicality
                              |
        parallel per-block map -> ordered state reduce
                              |
             processor stores and change stream
                              |
     RPC facade / HTTP+SSE SDK API / SQL sinks
```

The first reference processor uses the blobs.money data model:

1. Backfill requested Dencun-and-later ranges from public historical sources.
2. Produce equivalent blobs block/transaction entities in the node without
   retaining raw historical blocks.
3. Follow the head through an isolated Reth-based P2P source.
4. Verify roots, handle reorgs, and join the historical and live lanes exactly once.
5. Compare output against blobs.money-compatible fixtures as a correctness and
   performance benchmark.

The blobs.money application now has an application-owned reconciliation and
durable consumer path for Leani processor output, plus a local compatibility
RPC fallback. Production shadow parity, failure recovery, sustained staging,
and rollback evidence are still required before removing its managed-provider
fallback.

Token balances and Uniswap prices are the next generality tests. Both can be derived from receipt logs without an EVM, subject to the semantic limits documented in [RPC compatibility](docs/reference/ethereum-json-rpc.md).

## Follow Ethereum one block at a time

The smallest continuous Leani demo needs no configuration and does not wait for
a particular contract event:

```bash
leani subscribe blocks
```

The built-in `block-summary` processor requests verified execution headers and
bodies, but no receipts. It prints every fresh optimistic block with its
timestamp, block number, transaction count, gas usage, base fee, blob count,
and finality:

```text
2026-09-01T09:14:35Z  block=25881412  txs=187  gas=32.47M / 60.00M (54.1%)  base_fee=0.143592817 gwei  blobs=6  optimistic
```

Use `--format json` for NDJSON or `--once` to measure time to first data. To
prepare a reusable node, run `leani init blocks` once and `leani serve` in one
terminal; the same `leani subscribe blocks` command in another terminal detects
the local API, prints a stable latest snapshot immediately, and follows live
changes. See the [live block subscription guide](docs/guides/live-block-subscriptions.md)
for the compact configuration and recording workflow.

## Stream a live Uniswap price

The CLI can activate the embedded Uniswap V3 observation processor without a
separately running Leani node or a hand-written configuration:

```bash
leani subscribe uniswap-v3 ETH/USDC
```

This naked form is the standalone example: when no configured node is
reachable, it starts the lightweight processor itself. A cold first run can
spend a few minutes discovering public execution peers and catching up from a
verified finalized anchor. Leani reports that phase, then prints the newest
still-fresh price captured during verified startup and continues with
subsequent swaps. The checkpoint and useful peer records are cached, so later
runs normally become ready much faster. When a local `leani.toml` exists, an
embedded fallback also seeds its private peer cache from that node context
without sharing the node's P2P identity or writable database.

When this attaches to a running node, every requested market must already be in
that node's Uniswap processor configuration. Leani checks the processor's pool
catalog before opening the stream and reports the configured markets on a
mismatch. An embedded subscription instead builds its processor scope directly
from the markets on the command line.

On first use, Leani requires two of three independent checkpoint providers to
agree on the exact finalized consensus slot and root, then asks you to accept
that quorum. Execution follows native P2P peers, while ongoing Beacon API
responses are proof-checked from the accepted checkpoint. Once Leani has
locally verified a newer light-client finality anchor and its
consensus-committed execution hash, that anchor is persisted and reused by
later runs without another provider request while it remains fresh.
`subscribe` never modifies configuration files.

Use consensus P2P for finality updates as well:

```bash
leani subscribe uniswap-v3 ETH/USDC --finality-source p2p
```

This still needs the quorum-provided root on a fresh installation, but does not
need an ongoing Beacon API connection after bootstrap. The default provider set
and `2`-provider threshold can be changed with repeatable `--checkpoint-url`
flags and `--checkpoint-quorum`.

Subscribe to several pools and pipe stable NDJSON to another Unix tool:

```bash
leani subscribe uniswap-v3 ETH/USDC ETH/USDT --format json |
  jq --unbuffered '{market, price, baseVolume, blockNumber, finality}'
```

Each update includes the swap volume in the market's first token: ETH for
`ETH/USDC`, WBTC for `WBTC/ETH`, and so on.

From a directory with a project-local `leani.toml`, run `leani serve` in one
terminal and the same naked subscription command in another. Auto mode detects
the configured loopback API and attaches; `--mode client --endpoint URL` remains
available as a fail-closed remote-node override. `--config leani.toml` reuses a
node profile's network and finality settings for embedded mode without starting
its listeners or other processors. See the
[CLI and SDK walkthrough](docs/guides/uniswap-price-subscriptions.md) for the
output contract, exact price math, and TypeScript example.

Create that prepared node without hand-writing the full operational schema:

```bash
leani init uniswap-v3 ETH/USDC
leani serve
```

`init` obtains the same two-provider checkpoint quorum used by embedded
subscriptions, asks before trusting it, and writes a compact `leani.toml`. It
refuses to overwrite an existing file. Use `--yes` only when accepting the
quorum non-interactively is intentional, and use global `--config PATH` to
select another output path. The compact file exposes the network, data path,
trusted checkpoint, markets, and API bind; `serve` expands everything else to
documented Mainnet defaults. Advanced full configurations remain supported.

## Important boundary

Leani can replace a managed RPC for event indexing, recent block/receipt
access, common chain metadata, and application-specific derived queries that
its configured material can answer exactly. Without an EVM and state source,
it cannot honestly implement arbitrary:

- `eth_call`
- `eth_getBalance`
- `eth_getCode`
- `eth_getStorageAt`
- `eth_estimateGas`
- debug/trace methods

Those methods can eventually use an optional forwarding or stateless-execution extension, but they are not part of the EVM-free core.

## User documentation

Start at [leani.dev/docs](https://leani.dev/docs/) for the task-oriented
documentation, including the [Mainnet quickstart](https://leani.dev/docs/getting-started/),
[configuration reference](https://leani.dev/docs/reference/configuration/),
[live block subscriptions](https://leani.dev/docs/guides/live-block-subscriptions/),
[live Uniswap price subscriptions](https://leani.dev/docs/guides/uniswap-price-subscriptions/),
[operations guidance](https://leani.dev/docs/operations/preview-limitations/),
and [processor-author guide](https://leani.dev/docs/guides/custom-processor/).

Repository contributors can also read the Markdown sources under [`docs/`](docs/)
and the [changelog](CHANGELOG.md) directly on GitHub.

## Run diagnostics

For the complete application-style workflow—including selecting the last
10,000 finalized blocks, concurrent catch-up and head following, consuming the
`blobs-money` transformation stream, querying RPC, and testing restart/cursor
resume—follow [manual testing](docs/contributing/manual-verification.md).

The checked-in example deliberately contains a finality checkpoint placeholder,
so `doctor` reports it as non-operational without opening a database or network
connection:

```bash
cargo run -- doctor --config config/example.toml
```

Replace the checkpoint and its finalized beacon slot with a recent,
independently obtained weak-subjectivity checkpoint before using `serve`.

Probe one bounded EraE range without downloading or retaining its complete
8,192-block object:

```bash
cargo run -- --config config/example.toml source probe erae \
  --from-block 19426589 --to-block 19426589
```

The default native finality probe discovers consensus peers from embedded
official bootnodes and verifies all light-client proofs locally:

```bash
cargo run -- --config config/example.toml source probe finality \
  --checkpoint 0x... \
  --checkpoint-slot 14894752
```

The optional `beacon_api` profile uses untrusted HTTP transports and can
require agreement across several of them:

```bash
cargo run -- --config config/example.toml source probe finality \
  --checkpoint 0x... \
  --endpoint https://beacon-a.example \
  --endpoint https://beacon-b.example \
  --minimum-agreement 2
```

The recent execution probe uses the pinned Reth networking libraries directly,
retains no execution database, and validates all returned commitments. Bind it
to the execution hash from a verified finality report:

```bash
cargo run -- source probe p2p \
  --config config/example.toml \
  --from-block 25656660 \
  --to-block 25656660 \
  --expected-tip 0xf3fc94346619c19caef04425fd2a7925f9dc4191aeae5ed933e4a1cec418a914
```

Once a valid operational configuration is present, the durable store can be
inspected, verified, backed up, and compacted without starting network actors.
From the repository root, Leani discovers `config/example.toml`; installed
workflows normally keep the selected profile at `./leani.toml`:

```bash
cargo run -- db inspect
cargo run -- db verify
cargo run -- db backup ./backup.sqlite
cargo run -- db compact
cargo run -- db prune-changes --processor blobs-money --before 100000
```

The built-in blobs processor can be backfilled through the generic durable
runtime. Xatu execution timestamps select the necessary beacon-day partitions
automatically:

```bash
cargo run -- backfill \
  --from 19426589 \
  --to 19427588
```

The command resumes from committed coverage, not from downloaded files. It
keeps only processor output, metadata, deltas needed for restart, undo, and
public changes; historical Parquet/block material is released after commit.
When several configured history sources satisfy the processor, attempts rotate
by priority after a retryable failure and restart at the first block not
already covered by the durable store.

Compare two normalized exports or a source-derived RPC response against a
reference client without mutating node state:

```bash
cargo run -- conformance processor \
  --processor blobs-money \
  --left-source xatu --left xatu.json \
  --right-source erae --right erae.json

cargo run -- conformance rpc \
  --frames erae.json \
  --expected reference-rpc-snapshots.json \
  --report rpc-differential.json
```

`conformance frames` provides a lower-level capability-selected fingerprint
comparison. It strips provenance and transport verification evidence but
fails closed if selected canonical material is incomplete.

Measure acquisition, isolated materialization, durable delivery, or their
combined path against deterministic streaming corpora. `blobs-like` runs the
production blobs transform and `uniswap-like` runs the block-local WETH/USDC
observation transform; zero/sparse/dense remain control and limit shapes.
Reports are immutable and versioned. Report v19 records the stage and product
profile independently, declares destination/query row semantics, verifies
canonical logical output bytes, attributes logical storage layers, and reports
final node/destination physical bytes plus `storageAmplificationMilli` (the
physical/logical ratio multiplied by 1,000). It also includes the current
128-block/50-ms commit baseline and samples SQLite/WAL/segment physical storage
throughout the run. Delivery measurements distinguish producer completion,
consumer completion, and post-producer drain time, with producer/consumer
block rates plus domain-event, logical-payload, and transmitted-byte rates.
Raw-retaining profiles additionally separate raw
acquisition from retained-local reads, identify whether that read verifies
frames, replays a processor, or feeds delivery, report raw segment/catalog
bytes, and fail if the local phase performs any further external source reads:

```bash
cargo run -- benchmark \
  --mode end-to-end \
  --profile externalized \
  --corpus blobs-like \
  --blocks 10000 \
  --chunk-blocks 2048 \
  --warmups 1 \
  --runs 3 \
  --samples-report benchmark-samples.ndjson \
  --report benchmark.json
```

`--mode` selects only the measured stage. Pair `acquire` with
`--profile acquire-discard` or `raw-only`; `process` with `--profile materialized`,
`compact-artifact`, `compact-artifact-segment`, `compact-artifact-tiered`, or
`raw-artifact`, `raw-materialized`, or `raw-artifact-materialized`; and
`deliver`/`end-to-end` with `--profile externalized` or `raw-externalized`.
The compact-artifact profiles retain one immutable, replayable map result per
finalized block. Every `raw-*` combination first retains complete
processor-reuse frames and then performs processing or verification from the
local raw store. Product profiles own storage and delivery policy; selecting a
stage never rewrites that policy.
`--consumer-delay-ms` adds deterministic destination latency. SDK/PostgreSQL
runs can use `--consumer-reconnect-every-batches N` to close and independently
fence a new stream session after each N acknowledged batches, and
`--consumer-drop-ack-response-once` deterministically exercises recovery when
the destination commit and node ACK succeed but the response is lost. The same
seeded generator supports one-million-block controlled runs without retaining
the corpus in memory. Add `--concurrent-live-blocks N` to an end-to-end
SDK/PostgreSQL run to commit a finalized live tail while history is still
acquired and delivered; `--live-block-interval-ms` controls its cadence. The
report verifies independent history/live digests and durable cursors and
records live commit-latency percentiles. Summary reports stay compact;
high-frequency resource samples are written only when `--samples-report` is
supplied, as immutable NDJSON with a content hash recorded in the summary.

For consumer-neutral delivery capacity, use `--mode end-to-end --profile
externalized --destination rust-http`. With the default zero consumer delay,
this measures source acquisition, production processor execution, durable
delivery publication, real HTTP streaming, payload validation, immediate ACK,
and pruning without adding an application database. Add a fixed
`--consumer-delay-ms` to the same command to measure backpressure from a slower
consumer. In contrast, `--mode deliver --destination rust-http` pre-fills the
node before starting its timer and is the delivery-only transport ceiling;
`--destination sdk-postgres` measures the concrete SDK-to-PostgreSQL path.

Generated benchmark reports and samples are intentionally ignored by Git.
Synthetic runs measure the local source/coordinator path, not Xatu, EraE, or
P2P transport. Keep machine, source, commit, configuration, and cold/warm-run
context with any result shared outside the local checkout.

Performance evidence is never collected or uploaded by GitHub Actions. CI
tests benchmark logic as ordinary Rust code, but it does not execute benchmark
commands. Run the checked suites explicitly on a quiet, controlled host; the
runner builds a release binary, records host/tool/commit context, executes
cases sequentially to avoid cross-case contention, and writes only beneath an
ignored local directory:

```bash
# Fast resumability and report-contract check.
scripts/run-controlled-benchmarks.sh smoke

# The complete synthetic matrix formerly represented by CI configuration.
scripts/run-controlled-benchmarks.sh synthetic-million

# Select a smaller controlled comparison or an external evidence mount.
LEANI_BENCHMARK_CASES="materialized end-to-end-fast-consumer" \
LEANI_BENCHMARK_CORPORA="uniswap-like blobs-like" \
scripts/run-controlled-benchmarks.sh synthetic-million /mnt/leani-evidence/run-01
```

The default destination is a timestamped directory under
`.private/evidence/performance`. Passing the same explicit output directory
resumes completed cases and refuses ambiguous partial output. The `all` and
`delivery-faults` suites include SDK/PostgreSQL cases and therefore require a
dedicated disposable database:

```bash
docker compose --profile benchmark up -d benchmark-postgres
export LEANI_BENCHMARK_POSTGRES_URL=postgres://leani_benchmark:leani_benchmark@127.0.0.1:55432/leani_benchmark
scripts/run-controlled-benchmarks.sh delivery-faults
```

Run or resume one selected-stage candidate sweep with:

```bash
cargo run -- benchmark sweep \
  --manifest config/benchmark-sweep.example.json \
  --output-directory benchmark-sweep-results
```

The manifest holds common benchmark arguments and explicit candidate
arguments. Each successful candidate produces content-addressed immutable
summary/sample files; rerunning skips complete candidates and rebuilds
`sweep-summary.json` with correctness/resource-gate decisions and the
throughput/RSS/storage Pareto frontier. Concurrent-live sweeps can also set
`maximumLiveP95CommitLatencyUs`; live p95 then becomes a required gate and a
Pareto dimension, preventing a history-throughput win from hiding live writer
starvation. Keep a tuned argument out of `baseArguments` and set it once per
candidate.

Delivery defaults to the real HTTP path with a Rust discard sink. Use
`--destination rust-direct` for the durable-store protocol ceiling. The full
SDK/PostgreSQL fixture uses a dedicated disposable database because it
truncates its benchmark tables:

```bash
docker compose --profile benchmark up -d benchmark-postgres
LEANI_BENCHMARK_POSTGRES_URL=postgres://leani_benchmark:leani_benchmark@127.0.0.1:55432/leani_benchmark \
  cargo run -- benchmark --mode end-to-end --profile externalized --destination sdk-postgres \
  --corpus blobs-like --blocks 10000 --warmups 1 --runs 3
```

That default writes the stable one-event-per-row protocol fixture. Add
`--postgres-schema blobs-application` with `--corpus blobs-like` to write and
measure application-shaped block and blob-transaction tables instead; the
report records the selected schema, its independent row semantics, table/index
bytes, WAL bytes, and canonical output digest.

The historical-only container profile starts without network credentials:

```bash
docker compose up --build
curl http://127.0.0.1:8080/health/live
open http://127.0.0.1:8080/debug/network
curl http://127.0.0.1:8080/metrics
```

`/debug/network` is a dependency-free operational dashboard backed by
`/v1/network/status`. Live following, history bridging, and probes share one
process-wide Reth network manager with a stable persisted peer identity. The
dashboard shows its phase, peer counts, requested block range, attempts, last
error, material-request outcomes, and local queue wait alongside processor
coverage. Recently stopped managers remain in the bounded view so an actual
network-task restart cannot disappear from diagnostics.
Stored-range contiguity and live readiness are separate signals: a processor
can have no internal coverage gaps while it is still blocks behind the active
P2P catch-up target. The dashboard reports that target and the remaining block
count explicitly. It also reports required-network-lane failures and the
supervisor retry countdown instead of presenting backoff periods as an
unexplained absence of active sessions.

## Development checks

```bash
cargo fmt --all --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings

cd packages/sdk
bun test
```

The default Rust suite includes a deterministic 10,000-block hot/cold restart
and convergence test. A separate production-adapter Mainnet runner verifies a
real historical catch-up, exact overlap, additional head following, freshness,
and SQLite integrity while writing a pass/fail evidence report. See
[end-to-end verification](docs/contributing/automated-verification.md).

## Current networking architecture

Leani consumes isolated networking crates from Reth 2.4.1 rather than
forking SHiNode or running a complete execution client. One manager persists
for the process lifetime, uses a stable key in the data directory, shares a
bounded peer cache across live/history work, enables Discv4, Discv5, and DNS
discovery, and hedges warm startup across cached and newly discovered peers.
Only the most recent capability-proven, currently viable body servers are
dialed immediately; the broader cache is admitted in refill-paced batches so
stale records cannot monopolize the first dial window. Cache refreshes merge
the current Reth view into that bounded discovery set, so a short failed run
cannot erase the next run's alternatives. Leani multiplexes at most four
bounded material requests per connected peer under one global, live-priority
admission gate. Body and receipt requests
start at an evidence-backed eight-block ceiling, shrink independently after a
partial/failed response, and grow again only after sustained success. Finalized
P2P history is a protocol-verified, operator-bounded fallback after retained
and archive sources: Leani proves ancestry to a beacon-derived execution anchor
before emitting commitment-checked material. Filtered log-only processors use
verified header blooms to avoid negative-block receipts and can omit bodies
when transaction hash is not part of their declared output identity; generic
RPC and broad processors retain the full-material path. Durable processor
coverage and retained parent-linked canonical material remain the restart
authorities.

The native application interface is a TypeScript SDK with HTTP queries and a resumable SSE
change stream, tested with Bun and Node. Standard Ethereum clients use the
separate JSON-RPC endpoint. blobs.money now consumes already-derived blob
changes through the native API; pointing its legacy ethers/fetch code at the
local RPC remains a compatibility and emergency-fallback option.

Release validation follows the documented
[end-to-end checks](docs/contributing/automated-verification.md). blobs.money remains a reference
processor, parity benchmark, and pre-launch integration. Its production
cutover remains separately gated by shadow parity, failure testing, staging
evidence, and a tested rollback path.
