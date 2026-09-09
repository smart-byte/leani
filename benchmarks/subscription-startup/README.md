# Subscription startup benchmark

This benchmark measures `leani subscribe blocks --mode embedded --once` from
process start to the first block-summary line. Each comparison alternates the
binary order and runs a cold start followed by a warm start against the same
data directory.

Run it through the controlled benchmark entry point:

```sh
LEANI_BENCHMARK_BASELINE_BINARY=/path/to/baseline/leani \
LEANI_BENCHMARK_BINARY=/path/to/candidate/leani \
scripts/run-controlled-benchmarks.sh subscription-startup
```

Useful overrides are `LEANI_BENCHMARK_RUNS` (default `10`) and
`LEANI_BENCHMARK_CUTOFF_SECONDS` (default `90`). The output contains a run
manifest, append-only `results.ndjson`, per-run logs and data directories, and
a generated `summary.md`. Reusing the same output directory resumes completed
runs after verifying the binary hashes and benchmark settings.

Benchmark evidence does not belong in Git. The runner writes it under the
ignored `.private/evidence/performance/` tree unless an explicit output path is
provided.
