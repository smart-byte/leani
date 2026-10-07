# EraE backfill benchmark

This comparison alternates two release binaries over the same mainnet range,
using `benchmark real-source` to measure acquisition through durable delivery,
acknowledgement, and pruning. Every run uses fresh storage and must match the
first baseline run's delivered-output digest for its processor.

```sh
LEANI_BENCHMARK_BASELINE_BINARY=/path/to/baseline/leani \
LEANI_BENCHMARK_BINARY=/path/to/candidate/leani \
scripts/run-controlled-benchmarks.sh erae-backfill
```

Both binaries must support the chosen processors in `benchmark real-source`.
For a baseline predating block-summary benchmark support, apply only the
benchmark harness change to that checkout before building, or select
`LEANI_BENCHMARK_PROCESSORS="blobs-money uniswap-observations"`.

The default is three alternating pairs for each of `block-summary`,
`blobs-money`, and `uniswap-observations`, over blocks
`19,426,589..=19,430,684`. Set `LEANI_BENCHMARK_RUNS`,
`LEANI_BENCHMARK_FROM_BLOCK`, `LEANI_BENCHMARK_TO_BLOCK`,
`LEANI_BENCHMARK_PROCESSORS`, `LEANI_BENCHMARK_CONFIG`, or
`LEANI_BENCHMARK_TIMEOUT_SECONDS` to override these settings. A configuration
with a `file:` or loopback HTTP EraE endpoint can isolate local processing
from public-mirror behavior.

Run on a quiet host, without overlapping builds or other benchmarks. Public
bandwidth and mirror cache state remain uncontrolled. Reports keep physical
reads and fetched bytes separate from normalized frame bytes. The summary
uses medians; three runs do not establish tail latency.

The manifest pins binary and configuration hashes, range, run settings, and
host. Reusing an output directory resumes completed, correctness-checked
reports only when this identity matches. A partial run is left for inspection.
Generated reports, logs, SQLite stores, and summaries stay in the ignored
`.private/evidence/performance/` tree and do not belong in Git.

For source-only measurements, see
[`crates/source-erae/examples/throughput.rs`](../../crates/source-erae/examples/throughput.rs)
and the [benchmarking guide](../../docs/contributing/benchmarking.md).
