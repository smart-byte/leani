---
title: Automated verification
description: Run Leani's network-free correctness fixtures and keep performance, Mainnet, fault-injection, and soak evidence on controlled hosts.
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
| process interruption/restart, and startup reconciliation of what it left | `historical_run_resumes_from_durable_coverage_after_process_reopen`, the 10,000-block gate, `startup_reconciliation_deletes_pending_deltas_no_lane_can_apply`, `startup_reconciliation_replays_retained_frames_to_lanes_that_applied_none`, `startup_reconciliation_replays_retained_frames_above_a_hole`, `startup_reconciliation_leaves_a_consistent_store_untouched`, `a_hole_below_the_retained_window_is_left_to_the_cold_backfill`, `a_lane_the_store_paused_at_the_tip_resumes_after_a_restart_without_a_hole`, `a_lane_the_store_failed_at_the_tip_can_be_reset_after_a_restart`, `a_retained_tip_the_finalized_anchor_proves_is_kept_not_undone`, `a_proof_from_above_a_finalized_head_keeps_and_finalizes_the_retained_tail`, `a_branch_reorged_away_during_downtime_is_undone_only_above_the_fork`, `an_anchored_overlap_redelivers_proven_blocks_without_applying_them_twice`, `a_failed_or_stalled_retained_proof_falls_back_to_an_empty_ancestry`, `a_cancelled_retained_proof_seeds_nothing` |
| crash injected into a live commit, reorg, or backfill commit, then startup reconciliation | `startup_reconciliation_replays_a_block_a_crash_kept_from_some_lanes`, `startup_reconciliation_applies_a_delta_a_crash_left_pending`, `startup_reconciliation_undoes_a_branch_a_crashed_reorg_left_applied`, `startup_reconciliation_records_where_a_crash_paused_a_lane_at_its_delivery_limit`, `startup_reconciliation_records_where_a_crash_failed_a_lane_at_its_delivery_limit`, `startup_reconciliation_records_where_a_crash_stopped_a_ledger_at_its_delivery_limit`, `startup_reconciliation_deletes_a_delta_a_crashed_backfill_left_pending` |
| checkpoint cadence and retention, restore of corrupted state at an exact checkpoint cursor, and portable savepoint integrity | `automatic_checkpoints_are_bounded_and_savepoints_are_explicit`, `portable_savepoints_are_bounded_per_processor` |
| restorable checkpoints at job completion and after finality promotion | `completed_jobs_leave_a_restorable_checkpoint`, `checkpoint_restore_accepts_later_finality_promotion` |
| undo retention past finality | `ordered_undo_rows_stay_within_the_safety_depth`, `finalized_applies_write_no_undo_rows` |
| compacted coverage re-apply and handoff, interrupted handoffs | `compacted_coverage_counts_as_already_applied`, `hot_cold_handoff_verifies_across_compacted_coverage`, `running_handoffs_hold_back_only_their_overlap`, `a_new_handoff_supersedes_an_unfinished_one` |
| one processor's reducer, delta, finality, or cold-backfill failure | `reducer_failure_parks_only_its_processor_while_shared_ingestion_continues`, `a_canonical_block_local_reducer_failure_parks_only_its_lane`, `a_lane_parked_by_its_reducer_does_not_fail_the_live_lane_after_restart`, `a_conflicting_pending_delta_fails_only_its_lane`, `a_delta_conflict_lane_resumes_after_a_restart_and_a_reset`, `one_processor_finality_failure_does_not_stop_the_others_or_pruning`, `a_finality_contradiction_is_recorded_for_a_lane_failed_for_another_reason`, `one_failed_cold_backfill_parks_only_its_processor`, `a_lane_parked_below_a_seeded_anchor_walks_past_it_once_history_passes`, `a_lane_failed_outside_the_live_lane_keeps_its_failure_and_is_not_remapped`, `a_failed_canonical_delivery_lane_resets_through_the_api_and_resumes`, `a_failed_lane_keeps_its_first_failure_unless_finality_contradicts_it`, `a_park_racing_a_finality_failure_leaves_the_failed_lane_alone`, `pausing_a_lane_that_another_component_failed_is_benign`, `resetting_a_contradicted_live_lane_answers_that_it_needs_a_rebuild` |
| parked lanes across subscribe, reorg, finality, and concurrent drains | `a_lane_paused_at_subscribe_receives_material_it_requires_after_resuming`, `a_reorg_leaves_a_paused_lane_paused`, `an_ordered_gapped_lane_resumes_after_a_reorg_below_its_marker`, `a_block_local_gapped_lane_applies_a_shortening_reorg_without_a_hole`, `unfinalized_gap_replay_pauses_until_finality_then_recovers_from_history`, `concurrent_drains_of_one_live_gap_do_not_fail_the_lane`, `rewind_probe_block_local_gap_above_lowest_reverted`, `rewind_probe_ordered_gap_above_lowest_reverted`, `rewind_probe_block_local_gap_at_lowest_reverted`, `gapped_lanes_rewound_without_a_replacement_branch_stay_on_the_chain`, `a_rewind_without_a_replacement_branch_settles_a_paused_lane_at_once`, `a_gapped_lane_whose_whole_history_is_reverted_resumes_at_its_start`, `a_gap_marker_off_the_canonical_chain_is_re_pointed_instead_of_completed`, `a_rewind_onto_a_seeded_anchor_a_paused_lane_never_applied_completes_its_gap`, `an_ordered_replay_onto_a_seeded_anchor_checks_its_parent_link` |
| blocks an on-demand live lane skips after downtime, announced or filled before the next block | `an_on_demand_block_local_lane_announces_the_blocks_it_skips_before_applying`, `a_lane_that_replays_past_skipped_blocks_at_startup_announces_them`, `a_live_consumer_sees_blocks_the_live_lane_skipped`, `an_on_demand_lane_that_fills_live_gaps_delivers_the_skipped_blocks_before_the_next_one`, `a_live_gap_larger_than_the_fill_limit_is_announced_without_fetching_history`, `a_live_gap_fill_whose_history_fails_announces_the_whole_range`, `a_live_gap_fill_announces_the_rest_at_a_frame_that_does_not_descend_from_the_last_one`, `a_live_gap_fill_never_applies_a_last_block_the_following_block_does_not_name`, `a_live_gap_fill_waits_for_delivery_capacity_without_announcing_or_refetching`, `a_replayed_live_gap_fill_refused_above_the_resume_mark_waits_without_refetching`, `a_live_gap_fill_that_runs_out_of_time_announces_the_rest`, `a_live_gap_fill_abandons_a_history_request_that_outlasts_its_time_limit`, `a_startup_replay_past_a_hole_fills_it_when_configured`, `a_live_gap_above_the_finalized_head_is_announced_not_filled`, `a_storage_paused_fill_resumes_once_the_store_has_room`, `a_live_gap_fill_held_at_a_node_wide_delivery_budget_resumes_once_the_budget_has_room`, `a_live_gap_fill_refused_at_a_node_wide_delivery_budget_waits_for_room_without_refetching`, `live_delivery_refusal_decides_as_an_apply_without_writing` |
| non-contiguous live blocks and reorgs | `shared_live_rejects_a_block_that_does_not_extend_the_canonical_tip`, `shared_live_rejects_a_reorg_that_does_not_revert_the_tip` |
| recent storage at its hard limit | `live_ingestion_waits_at_the_recent_hard_limit_until_finality_prunes`, `live_ingestion_fails_after_waiting_too_long_at_the_recent_hard_limit` |
| slow required consumer | `required_consumer_remains_protective_after_lease_lapse` |
| disk/delivery pressure | `delivery_limit_pauses_and_auto_resumes_below_low_water`, `a_known_unavailable_lane_the_store_paused_without_a_marker_is_mapped_again`, `a_failed_ledger_behind_its_history_gets_no_marker_ahead_of_its_cursor` |
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
redownloading the handoff overlap. Before any lane opens, startup
reconciliation brings each processor back onto that suffix: it undoes blocks
an interrupted reorg left applied and replays retained blocks an interrupted
commit skipped.

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

GitHub Actions runs only deterministic correctness and contract checks. It
does not run synthetic or real-source benchmarks, Mainnet evidence gates,
fault-injected delivery measurements, or soaks, and it does not upload their
reports. Performance evidence must be collected explicitly on a controlled
host with `scripts/run-controlled-benchmarks.sh`; its default output is under
the ignored `.private/evidence/performance` tree. Real-source and Mainnet runs
remain manual because source availability, trust inputs, host state, and
network conditions are part of the result.

Do not commit a weak-subjectivity checkpoint as an evergreen trusted value.
Record the checkpoint source and acquisition time alongside each evidence
report.
