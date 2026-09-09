#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Run Leani performance measurements explicitly on a controlled host.

Usage:
  scripts/run-controlled-benchmarks.sh SUITE [OUTPUT_DIRECTORY]

Suites:
  subscription-startup
                     Alternating cold/warm block-subscription startup comparison.
  smoke              Small, resumable two-candidate sweep.
  synthetic-million  Full synthetic processor/delivery matrix (sequential).
  delivery-faults    SDK/PostgreSQL reconnect and lost-ACK matrix.
  all                synthetic-million followed by delivery-faults.

Outputs default to an ignored timestamped directory under
.private/evidence/performance. Set an explicit OUTPUT_DIRECTORY, or
LEANI_BENCHMARK_OUTPUT_DIRECTORY, to resume a controlled run in place.

Useful environment overrides:
  LEANI_BENCHMARK_BASELINE_BINARY
                               Baseline binary for subscription-startup (required).
  LEANI_BENCHMARK_BINARY       Existing binary; otherwise build target/release/leani.
  LEANI_BENCHMARK_CASES        Space-separated synthetic case filter.
  LEANI_BENCHMARK_CORPORA      Space-separated corpus filter.
  LEANI_BENCHMARK_BLOCKS       Synthetic block count (default: 1000000).
  LEANI_BENCHMARK_CHUNK_BLOCKS Source chunk size (default: min(blocks, 8192)).
  LEANI_BENCHMARK_WARMUPS      Synthetic warm-up count (default: 1).
  LEANI_BENCHMARK_RUNS         Startup pairs (default: 10) or synthetic runs (default: 3).
  LEANI_BENCHMARK_CUTOFF_SECONDS
                               Subscription startup cutoff (default: 90).
  LEANI_BENCHMARK_FAULT_BLOCKS Delivery-fault block count (default: 10000).

SDK/PostgreSQL cases require LEANI_BENCHMARK_POSTGRES_URL pointing to a
dedicated disposable database. The benchmark truncates its own tables.
EOF
}

fail() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

script_directory=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repository_root=$(cd "$script_directory/.." && pwd)
cd "$repository_root"

case "${1:-}" in
  -h|--help)
    usage
    exit 0
    ;;
  subscription-startup|smoke|synthetic-million|delivery-faults|all)
    suite=$1
    ;;
  '')
    usage >&2
    exit 2
    ;;
  *)
    fail "unknown benchmark suite '$1'"
    ;;
esac

timestamp=$(date -u +%Y%m%dT%H%M%SZ)
output_directory=${2:-${LEANI_BENCHMARK_OUTPUT_DIRECTORY:-.private/evidence/performance/${timestamp}-${suite}}}
case "$output_directory" in
  /*) ;;
  *) output_directory="$repository_root/$output_directory" ;;
esac

if [ -n "${LEANI_BENCHMARK_BINARY:-}" ]; then
  benchmark_binary=$LEANI_BENCHMARK_BINARY
  case "$benchmark_binary" in
    /*) ;;
    *) benchmark_binary="$repository_root/$benchmark_binary" ;;
  esac
  [ -x "$benchmark_binary" ] || fail "benchmark binary is not executable: $benchmark_binary"
else
  CARGO_INCREMENTAL=0 LEANI_GIT_COMMIT="$(git rev-parse HEAD)" \
    cargo build --release --locked -p leani
  benchmark_binary="$repository_root/target/release/leani"
fi

prepare_output_directory() {
  local directory=$1
  local context
  local dirty=false

  mkdir -p "$directory"
  context="$directory/run-context.txt"
  if [ -e "$context" ]; then
    return
  fi
  if ! git diff --quiet || ! git diff --cached --quiet ||
    [ -n "$(git ls-files --others --exclude-standard)" ]; then
    dirty=true
  fi
  umask 077
  {
    printf 'suite=%s\n' "$suite"
    printf 'started_utc=%s\n' "$timestamp"
    printf 'git_commit=%s\n' "$(git rev-parse HEAD)"
    printf 'git_dirty=%s\n' "$dirty"
    printf 'host=%s\n' "$(uname -a)"
    printf 'binary=%s\n' "$benchmark_binary"
    "$benchmark_binary" --version
    rustc --version --verbose
    cargo --version
    if command -v bun >/dev/null 2>&1; then
      printf 'bun=%s\n' "$(bun --version)"
    fi
  } > "$context"
}

require_postgres() {
  [ -n "${LEANI_BENCHMARK_POSTGRES_URL:-}" ] || fail \
    "SDK/PostgreSQL cases require LEANI_BENCHMARK_POSTGRES_URL; start the dedicated benchmark-postgres Compose profile first"
  command -v bun >/dev/null 2>&1 || fail "SDK/PostgreSQL cases require Bun"
}

run_measurement() {
  local directory=$1
  local case_id=$2
  local mode=$3
  local profile=$4
  local destination=$5
  local corpus=$6
  local postgres_schema=$7
  local consumer_delay_ms=$8
  shift 8

  local report="$directory/benchmark-${case_id}-${corpus}.json"
  local samples="$directory/benchmark-${case_id}-${corpus}-samples.ndjson"
  if [ -s "$report" ] && [ -f "$samples" ]; then
    printf 'resume: keeping completed %s / %s\n' "$case_id" "$corpus"
    return
  fi
  if [ -e "$report" ] || [ -e "$samples" ]; then
    fail "partial output exists for $case_id / $corpus; inspect it and choose a new output directory before retrying"
  fi

  printf 'run: %s / %s\n' "$case_id" "$corpus"
  "$benchmark_binary" benchmark \
    --mode "$mode" \
    --profile "$profile" \
    --destination "$destination" \
    --postgres-schema "$postgres_schema" \
    --corpus "$corpus" \
    --consumer-delay-ms "$consumer_delay_ms" \
    --samples-report "$samples" \
    --report "$report" \
    "$@"
}

run_smoke() {
  local directory=$1
  prepare_output_directory "$directory"
  "$benchmark_binary" benchmark sweep \
    --manifest config/benchmark-sweep.smoke.json \
    --output-directory "$directory"
}

run_subscription_startup() {
  local directory=$1
  local baseline_binary=${LEANI_BENCHMARK_BASELINE_BINARY:-}
  local runs=${LEANI_BENCHMARK_RUNS:-10}
  local cutoff_seconds=${LEANI_BENCHMARK_CUTOFF_SECONDS:-90}

  [ -n "$baseline_binary" ] || fail \
    "subscription-startup requires LEANI_BENCHMARK_BASELINE_BINARY"
  case "$baseline_binary" in
    /*) ;;
    *) baseline_binary="$repository_root/$baseline_binary" ;;
  esac
  [ -x "$baseline_binary" ] || fail "baseline binary is not executable: $baseline_binary"

  prepare_output_directory "$directory"
  python3 benchmarks/subscription-startup/benchmark.py \
    --baseline-binary "$baseline_binary" \
    --candidate-binary "$benchmark_binary" \
    --output-directory "$directory" \
    --runs "$runs" \
    --cutoff-seconds "$cutoff_seconds"
}

run_synthetic_million() {
  local directory=$1
  local cases=${LEANI_BENCHMARK_CASES:-"acquire raw-only materialized compact-artifact compact-artifact-segment compact-artifact-tiered raw-artifact raw-materialized raw-artifact-materialized deliver end-to-end-fast-consumer end-to-end-slow-consumer end-to-end-postgres raw-externalized end-to-end-blobs-application"}
  local corpora=${LEANI_BENCHMARK_CORPORA:-"zero uniswap-like blobs-like"}
  local blocks=${LEANI_BENCHMARK_BLOCKS:-1000000}
  local chunk_blocks=${LEANI_BENCHMARK_CHUNK_BLOCKS:-}
  local warmups=${LEANI_BENCHMARK_WARMUPS:-1}
  local runs=${LEANI_BENCHMARK_RUNS:-3}
  local case_id corpus mode profile destination postgres_schema consumer_delay_ms

  if [ -z "$chunk_blocks" ]; then
    chunk_blocks=8192
    if (( blocks < chunk_blocks )); then
      chunk_blocks=$blocks
    fi
  fi

  for case_id in $cases; do
    case "$case_id" in
      end-to-end-postgres|raw-externalized|end-to-end-blobs-application)
        require_postgres
        ;;
      acquire|raw-only|materialized|compact-artifact|compact-artifact-segment|compact-artifact-tiered|raw-artifact|raw-materialized|raw-artifact-materialized|deliver|end-to-end-fast-consumer|end-to-end-slow-consumer)
        ;;
      *) fail "unknown synthetic case '$case_id'" ;;
    esac
  done
  for corpus in $corpora; do
    case "$corpus" in
      zero|uniswap-like|blobs-like) ;;
      *) fail "unsupported controlled corpus '$corpus'" ;;
    esac
  done

  prepare_output_directory "$directory"
  for case_id in $cases; do
    for corpus in $corpora; do
      mode=process
      profile=materialized
      destination=rust-direct
      postgres_schema=generic-event-log
      consumer_delay_ms=0
      case "$case_id" in
        acquire) mode=acquire; profile=acquire-discard ;;
        raw-only) mode=acquire; profile=raw-only ;;
        materialized) ;;
        compact-artifact) profile=compact-artifact ;;
        compact-artifact-segment) profile=compact-artifact-segment ;;
        compact-artifact-tiered) profile=compact-artifact-tiered ;;
        raw-artifact) profile=raw-artifact ;;
        raw-materialized) profile=raw-materialized ;;
        raw-artifact-materialized) profile=raw-artifact-materialized ;;
        deliver) mode=deliver; profile=externalized; destination=rust-http ;;
        end-to-end-fast-consumer) mode=end-to-end; profile=externalized; destination=rust-http ;;
        end-to-end-slow-consumer) mode=end-to-end; profile=externalized; destination=rust-http; consumer_delay_ms=100 ;;
        end-to-end-postgres) mode=end-to-end; profile=externalized; destination=sdk-postgres ;;
        raw-externalized) mode=end-to-end; profile=raw-externalized; destination=sdk-postgres ;;
        end-to-end-blobs-application)
          [ "$corpus" = blobs-like ] || continue
          mode=end-to-end
          profile=externalized
          destination=sdk-postgres
          postgres_schema=blobs-application
          ;;
      esac
      run_measurement "$directory" "$case_id" "$mode" "$profile" "$destination" \
        "$corpus" "$postgres_schema" "$consumer_delay_ms" \
        --blocks "$blocks" \
        --chunk-blocks "$chunk_blocks" \
        --artifact-segment-blocks 2048 \
        --artifact-compaction-interval-ms 100 \
        --artifact-compaction-maximum-segments-per-cycle 4 \
        --warmups "$warmups" \
        --runs "$runs" \
        --sample-interval-ms 200
    done
  done
}

run_delivery_faults() {
  local directory=$1
  local corpora=${LEANI_BENCHMARK_CORPORA:-"uniswap-like blobs-like"}
  local blocks=${LEANI_BENCHMARK_FAULT_BLOCKS:-10000}
  local chunk_blocks=${LEANI_BENCHMARK_CHUNK_BLOCKS:-}
  local corpus

  if [ -z "$chunk_blocks" ]; then
    chunk_blocks=2048
    if (( blocks < chunk_blocks )); then
      chunk_blocks=$blocks
    fi
  fi

  require_postgres
  prepare_output_directory "$directory"
  for corpus in $corpora; do
    case "$corpus" in
      uniswap-like|blobs-like) ;;
      *) fail "delivery-faults supports only uniswap-like and blobs-like corpora" ;;
    esac
    run_measurement "$directory" delivery-faults end-to-end externalized \
      sdk-postgres "$corpus" generic-event-log 25 \
      --blocks "$blocks" \
      --chunk-blocks "$chunk_blocks" \
      --warmups 0 \
      --runs 1 \
      --sample-interval-ms 100 \
      --consumer-reconnect-every-batches 2 \
      --consumer-drop-ack-response-once \
      --concurrent-live-blocks 100 \
      --live-block-interval-ms 5
  done
}

case "$suite" in
  subscription-startup) run_subscription_startup "$output_directory" ;;
  smoke) run_smoke "$output_directory" ;;
  synthetic-million) run_synthetic_million "$output_directory" ;;
  delivery-faults) run_delivery_faults "$output_directory" ;;
  all)
    run_synthetic_million "$output_directory/synthetic-million"
    run_delivery_faults "$output_directory/delivery-faults"
    ;;
esac

printf 'benchmark evidence remains local at %s\n' "$output_directory"
