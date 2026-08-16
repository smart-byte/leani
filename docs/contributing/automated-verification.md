---
title: Automated verification
description: Run Leani's network-free fixtures, deterministic fault matrix, bounded evidence gates, and optional Mainnet staging checks.
section: contributing
order: 40
audience:
  - contributor
  - operator
status: preview
---

The project has two separate end-to-end gates. They answer different
questions and neither substitutes for the other.

## Network-free binary fixture

For a clean-host/native/container smoke test:

```bash
leani e2e fixture \
  --data-dir /tmp/leani-fixture \
  --blocks 128 \
  --report /tmp/leani-fixture/report.json
```

The command rejects an existing database, creates a deterministic finalized
chain, runs the real historical runtime and SQLite reducer, retains changes
and bounded checkpoints, verifies continuous coverage, runs SQLite integrity
checks, and emits a versioned JSON report. It proves packaging and local
durability without claiming current upstream interoperability.

## Deterministic 10,000-block gate

Run:

```bash
cargo test -p leani-runtime \
  ten_thousand_block_hot_cold_restart_converges_and_serves_coverage \
  --locked -- --nocapture
```

This test uses real runtime, SQLite, reducer, handoff, and native API code with
deterministic source adapters. It:

1. creates a 10,000-block historical chain;
2. starts a 64-block live overlap and follows additional head blocks;
3. runs both block-local and ordered-state processors;
4. interrupts both historical lanes after a committed prefix;
5. closes and reopens the SQLite database;
6. replays the live overlap and resumes historical coverage;
7. compares every live/historical overlap hash;
8. persists a `verified` handoff verdict;
9. drains ordered live deltas without waiting for another head;
10. proves exact continuous coverage with no pending deltas or duplicate
    applied blocks;
11. verifies that only the overlap and live head remain in recent raw storage;
12. runs SQLite integrity verification; and
13. queries processor coverage through the native API router.

It is part of the default workspace test suite and therefore runs in CI on
Linux and macOS.

## Deterministic fault matrix

The default workspace suite injects the alpha failure classes at source,
runtime, store, and API boundaries:

| Failure | Evidence test |
|---|---|
| disconnect/source outage and failover | `historical_run_fails_over_at_a_committed_chunk_boundary` |
| source corruption and input budget overrun | `history_stream_rejects_corruption_and_budget_overrun` |
| reorg apply/undo and removed logs | `shared_live_runtime_fans_out_and_reorgs_one_source_subscription`, `websocket_reorg_marks_old_logs_removed_before_replacements` |
| process interruption/restart | `historical_run_resumes_from_durable_coverage_after_process_reopen`, the 10,000-block gate |
| checkpoint corruption/restore, retention, and portable savepoint integrity | `automatic_checkpoints_are_bounded_and_savepoints_are_explicit` |
| slow required consumer | `required_consumer_remains_protective_after_lease_lapse` |
| disk/delivery pressure | `delivery_limit_pauses_and_auto_resumes_below_low_water` |
| acknowledgement/pruning/upgrade replay faults | `acknowledgement_cannot_exceed_the_consumers_delivered_head`, `delivery_pruner_applies_finality_age_and_bounded_batch_limits`, `unacknowledged_changes_survive_a_v6_to_v7_upgrade` |
| stable query during concurrent commits | `query_snapshot_keeps_values_and_stream_boundary_stable`, `generic_query_and_follow_snapshot_is_stable_across_new_commits` |
| terminal publication | `terminal_historical_job_spools_only_its_final_result` |
| backup/restore | `backup_reopens_as_a_complete_verified_restore` |

These are deterministic correctness gates. Filesystem exhaustion, process
kill timing, clean-host packaging, and multi-day storage behavior remain
staging evidence and must be recorded separately.

## Public Mainnet staging gate

This gate uses the production history, execution-P2P, and finality adapters.
It does not delete or replace an existing database.

Public datasets may lag the verified finalized anchor. The network supervisor
therefore tries the configured history backends first and may fill only the
remaining finalized suffix through the consensus-anchored execution-P2P
history fallback. By default every gap at or after the processor's configured
start block is eligible. Setting `sources.live.history_fallback_blocks` applies
a hard maximum suffix ending at the finalized anchor. The fallback fetches
bounded header batches concurrently, retains only one 32-byte hash per proven
block, and validates the assembled chain through the finalized anchor before
yielding any body or receipt. It reuses the process-wide persistent peer pool
and fails if an uncovered gap falls outside an explicitly configured bound.
Request failure penalizes or rotates the
responsible Reth peer without restarting discovery or disconnecting unrelated
peers. Body and receipt material starts as one-block probes and grows
adaptively to four-block batches after success. Effective concurrency is
bounded by connected peers, the global request gate, and the source budget so
ordinary Mainnet responses remain below the ETH protocol's 2 MiB soft response
limit. Each successful four-block-or-smaller history unit is emitted before
the next unit, successful bodies are retained while receipts retry, and a
queued one-block head request takes the next available local request slot
before normal catch-up work.

On process or lane restart, the node validates the finalized anchor against
the retained canonical SQLite suffix. If that suffix is complete and
parent-linked, live following resumes after its durable tip without
redownloading the handoff overlap.

When an archive later covers the maximum possible P2P bridge suffix, the
reconciliation lane remaps sequential finalized batches and compares their
processor-delta checksums with the checksums committed live. This audit does
not require retaining the original raw frames.

First obtain a recent weak-subjectivity checkpoint root and its finalized
beacon slot independently. Put them in a copy of `config/example.toml`.
The command refuses the checked-in placeholder.

Choose a recent start block. For a 10,000-block test, subtract 9,999 from the
finalized execution block reported by:

```bash
cargo run --locked --release -- \
  --config config/mainnet-e2e.toml \
  source probe finality \
  --report evidence/finality.json
```

Then run:

```bash
cargo run --locked --release -- \
  --config config/mainnet-e2e.toml \
  e2e mainnet \
  --processor blobs-money \
  --from-block <FINALIZED_EXECUTION_BLOCK_MINUS_9999> \
  --data-dir ./data/mainnet-e2e \
  --minimum-follow-blocks 16 \
  --stable-seconds 300 \
  --max-head-age-seconds 60 \
  --timeout-seconds 7200 \
  --report evidence/mainnet-e2e.json
```

Pass `--resume` after an intentional interruption. Without it, an existing
`leani.sqlite` is rejected.

The command passes only after all of these remain true for the stable window:

- the historical/live overlap verdict is durably `verified` for every
  participating processor;
- processor coverage is one continuous range from `--from-block` through its
  current cursor;
- the cursor is at least `--minimum-follow-blocks` beyond the initial
  finalized handoff anchor;
- no ordered live deltas remain pending;
- the latest cursor has retained canonical material;
- its execution timestamp is at most `--max-head-age-seconds` old; and
- the network lane has not exited.

Peer availability remains part of the evidence. Lowering `minimum_peers` for a
diagnostic run must be recorded and does not satisfy a stricter production
profile.

On completion or a handled failure, timeout, or interrupt, it cancels the
network lane, verifies SQLite, and writes the JSON report. The report contains
the exact binary commit, bounds, durable handoff, cursor, coverage,
latest-block age, logical store attribution, elapsed time, and failure
details.

## Evidence policy

The deterministic gate proves scheduling, restart, reconciliation, storage,
and API invariants. The Mainnet gate proves current external interoperability.
A production claim requires both, followed by a fault-injected staging run and
an independent soak whose duration and acceptance criteria are recorded by the
release operator.

Do not commit a weak-subjectivity checkpoint as an evergreen trusted value.
Record the checkpoint source and acquisition time alongside each evidence
report.
