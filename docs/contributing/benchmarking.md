---
title: Benchmarking
description: Measure acquisition, processing, and durable delivery against deterministic corpora, run the controlled suites, and sweep candidates on a quiet host.
section: contributing
order: 45
audience:
  - contributor
status: preview
---

Performance evidence is collected on controlled hosts, never in CI. The
network-free correctness gates are described in
[automated verification](/docs/contributing/automated-verification/); this page
covers the `benchmark` command, the controlled suites, and candidate sweeps.

## Measure one stage or the complete path

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

## Controlled suites

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

## Candidate sweeps

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

## SDK and PostgreSQL destinations

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
