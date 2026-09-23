---
title: Operations runbook
description: Deploy, observe, back up, restore, compact, and recover a Leani node using its explicit health and storage contracts.
section: operations
order: 20
audience:
  - operator
status: preview
---

## Deployment profiles

`deploy/container.toml` is a safe historical-only profile. It starts the query
API plus HTTP and WebSocket RPC on all container interfaces, but Docker Compose
publishes them only on loopback. It does not claim live or finality readiness.
Run a bounded `backfill` command against the same data volume before querying a
range. Subscriptions remain quiet until a live source commits events.

For a direct binary deployment:

1. install the binary as `/usr/local/bin/leani`;
2. create an `leani` system user;
3. place a validated config at `/etc/leani/node.toml`;
4. install `deploy/leani.service`;
5. give the service user write access only to the configured data directory.

Keep bearer tokens in `/etc/leani/environment`, not in TOML. Public
exposure should go through a TLS reverse proxy with request and connection
limits.

## Health and metrics

- `GET /health/live` checks that the process serves requests and SQLite
  answers a trivial query. It reads no tables, so it stays fast on large
  stores.
- `GET /health/ready` additionally checks every required live/finality actor
  from in-memory readiness state.
- `GET /debug/network` serves the built-in P2P and coverage dashboard.
- `GET /v1/network/status` returns the dashboard's machine-readable snapshot.
- `GET /metrics` returns bounded-cardinality Prometheus text metrics.
  Store-wide and per-processor row and byte counts are recounted at most
  every 10 seconds and can lag by that much. `/v1/network/status` shares the
  per-processor counts.
- `GET /v1/processors/{id}/status` reports coverage independently of process
  health.

A historical-only node may be ready while a requested range is incomplete;
the range query still fails with `range_incomplete` unless the caller opts
into partial data. A live profile remains not ready until its independently
anchored live and finality actors report ready. Live readiness also drops
while retained recent frames sit at `budgets.recent_raw_hard_bytes`: live
ingestion waits there, logging `recent_storage_full`, until finality prunes
older frames (see [Storage pressure](#storage-pressure)). A parked processor
lane does not affect readiness (see
[Parked processor live lane](#parked-processor-live-lane)).

With `sources.live.kind = "p2p"` and either supported finality backend, `serve`
verifies the configured weak-subjectivity checkpoint, seeds the finalized
execution anchor, starts one shared P2P subscription, and launches cold
backfills for every processor that has a capable history source. A source
failure drops readiness immediately and the supervisor retries with bounded
exponential backoff.

The network view reports the process-wide execution P2P manager. Live, history,
and probe requests share its identity, discovery state, peer pool, request
gate, and Reth reputation state. Each row includes:

- its current phase and active/stopped state;
- connected and known-peer counts;
- the requested block range and attempt count;
- how recently it changed; and
- the most recent bounded error message.

The view deliberately omits peer identities and retains only 32 stopped
manager records. It also reports cumulative material requests by outcome and
the time spent waiting for local peer-capacity slots. Physical peer sessions
are counted by establishment and disconnect, with disconnects grouped by
Reth's protocol-level reason. A high queue wait is a local scheduling signal;
a timeout occurs only after dispatch capacity was obtained. Prometheus exposes
the same state under the `leani_p2p_*` metric prefix.

The execution identity is stored as `execution-p2p-secret` in `data_dir`.
Treat it as node identity material: keep it across restarts, restrict its
permissions, and never publish it. `execution-network.sqlite` is a disposable,
bounded operational store capped by `sources.live.peer_store_max_entries`. It
contains both broad discovery candidates and Leani's independently observed
service evidence: verified header/body/receipt success times, highest served
block, smoothed response latency, last failure classification/time, fork
compatibility, and latest anchor qualification.

Reth's known-peer snapshots and new service observations are committed by a
dedicated background writer, so SQLite I/O never stops the network-manager
event loop. Incremental upserts retain candidates absent from one short or
failed run; indexed quality ordering decides which records survive the bound.
Standalone feeds merge the SQLite evidence across sibling local Mainnet
contexts, so a body- or receipt-serving peer learned by one processor is
preferred by another without sharing processor data or the local P2P identity
secret. The peer store is reconstructible and intentionally separate from the
authoritative processor database and its backups.

Warm startup uses two cache tiers. A small hot tier contains recent body-serving
peers whose last verified body success is newer than their last recorded
failure, with IPv4 `/16` and IPv6 `/32` diversity preferred. Those peers are
dialed immediately. All other records stay in the broad tier and enter Reth's
dialer in `body_serving_peer_target`-sized batches at the configured peer refill
interval. The segment-correct EIP-1459 DNS seeder and Reth's native
Discv4/Discv5 discovery run concurrently from the beginning. Reth owns the only
Discv4 service and the bounded dial scheduler.

`/v1/network/status` groups candidate admissions, established sessions, and
qualification outcomes by `cached_hot`, `cached_broad`, `dns_tree`, `trusted`,
or `discv4_or_5`. Prometheus exports the same origin labels through
`leani_p2p_peer_candidates_admitted_total`,
`leani_p2p_peer_sessions_by_origin_total`, and
`leani_p2p_peer_qualifications_total`. Use these counters when comparing cold
and warm startup instead of treating cache size as evidence of usefulness.

Every new ETH session is immediately tested against the current independently
verified execution anchor. Leani races qualifications until the configured
body-serving target is ready, then continues one qualification at a time at
normal priority. Qualification ranks proven serving peers first but does not
exclude a newly connected peer: each material response is independently
verified before use, so fresh peers remain immediate fallbacks. Proven serving
peers are tried before untested discovery records on the next start.

`sources.live.material_request_concurrency` bounds simultaneous physical
requests together with the global source budget and connected-peer capacity.
The default ceiling is 32, but each source acquisition's
`max_in_flight_requests` and the four-request per-peer pipeline usually impose
a lower effective limit. More concurrency helps only when the peer pool and
link have spare capacity; it also multiplies decoded response buffers. Treat
`budgets.source_concurrency` and `budgets.history_pipeline.maximum_active_chunks`
as memory-affecting settings and tune them together while watching RSS and the
material high-water metrics.

`material_request_blocks` controls adaptive body/receipt request width. The
evidence-backed default is eight; partial or failed responses reduce the
working width automatically, and eight successful material windows are needed
before it grows again. Raise the ceiling only with repeated real-source
evidence for the deployment's peer and bandwidth profile.

For address/topic-filtered log processors, P2P checks the verified header bloom
before requesting receipts. A negative avoids the receipt bundle; a positive
still downloads all receipts for that block because the ETH protocol has no
server-filtered receipt request. Bodies are fetched only when the descriptor's
log identity requires transaction hash. Therefore processor requirements, pool
activity, bloom false positives, and receipt density can matter more than the
configured concurrency ceiling.

For an inbound-capable deployment, set `sources.live.listener_port` and
`sources.live.discovery_port` to `30303`, set
`sources.live.discv5_port` to a distinct fixed UDP port such as `30304`,
expose all three sockets, leave `enable_discv5 = true`, and configure
`sources.live.nat` for the environment. Zero ports remain useful for isolated
tests. Inbound reachability improves diversity but is not required for
verification.
`sources.live.trusted_peers` may contain stable operator/community `enode://`
seeds that supplement public discovery without turning the pool into an
allowlist.
`sources.live.minimum_peers` is the hard connected-session floor before
execution requests may start and should normally remain `1`. Capability
qualification does not gate startup: it ranks proven peers first while
commitment validation remains mandatory for every response.
`body_serving_peer_target` controls how many peers Leani concurrently proves
can serve commitment-valid bodies; reaching it does not delay startup after
the minimum is available. `preferred_peers` is a soft operational target and
never blocks ingestion. The Reth manager continues filling connections beyond
that target in the background up to `max_outbound_peers`.
Header, body, and receipt failures are isolated to their material lane. A
transient miss therefore cools that lane without disconnecting a session that
may still be useful to another processor; cryptographically invalid material
continues to ban the peer globally.
If the pool remains at zero connected peers for
`sources.live.peer_recovery_timeout_seconds`, the node rebuilds the complete
execution network manager and restarts Discv4, Discv5, and DNS bootstrap. This
recovers stale sockets after host sleep and retries a failed cold-start
bootstrap without operator action. The default is five minutes: discovery and
outbound dialing continue throughout that window, allowing Reth's per-peer
backoff and candidate rotation to work before the underlying sockets are
recycled. The mainnet DNS root is also resubmitted every 15 seconds so a
transient resolver failure during the first lookup does not wait for the
whole-manager watchdog.

Processor `coverage.complete` means only that the stored range from the
configured start through `processedThrough` has no internal gaps. It does not
mean that `processedThrough` has reached the live source. The network status
adds `storedRangeContiguous`, `syncTargetBlock`, and `blocksRemaining`; use
those fields together with `readiness.liveReady` when diagnosing catch-up.
`syncTargetBlock` is derived from the live session's `observedHeadBlock`;
`fromBlock` and `toBlock` describe only its current material request. A
speculative request for the block after the observed head therefore does not
create artificial processor lag.
`blocksRemaining` counts every missing block from the configured start through
the observed head, including internal gaps; it is not merely the distance
between the highest stored block and that head.
`readiness.ready` means the required live and finality network lanes are
usable. It can be true while a processor backfills older gaps, so the
dashboard reports that combined `live + backfill` state explicitly.
The `network.supervisor` object reports `running`, `backing_off`, or `stopped`,
including its bounded last error, cumulative failure count, and retry
countdown. A zero active-session count during `backing_off` is a supervised
retry, not an idle healthy node.

The live/archive overlap may map the same canonical block at different
finality levels. The runtime validates all finality-only checksum variants,
keeps the strongest stored finality, and removes the redundant pending delta.
Any difference that cannot be reproduced by changing finality still fails
closed as a real transform or source conflict. Restart recovery performs this
local reconciliation before opening any network lane, so archive availability
or a slow P2P catch-up cannot delay cleanup. `processors[].pendingDeltas`
exposes any durable rows that remain afterward.

`finality.kind = "consensus_p2p"` is the self-contained profile. Configure both
the independently obtained checkpoint root and its finalized beacon slot.
The node uses signed embedded mainnet bootnode ENRs unless `finality.bootnodes`
overrides them, binds discv5 to `finality.discovery_port` (`0` selects an
ephemeral port), performs the mandatory Status exchange, and locally verifies
bootstrap, sync-committee, BLS, finality, and execution-payload proofs. Permit
outbound TCP and UDP; an inbound UDP mapping improves discovery but is not a
trust requirement.

`finality.kind = "beacon_api"` remains available when HTTP consensus
transports are operationally preferable. Configure
`finality.minimum_agreement` above one when independent transports are
available.

On-demand RPC shares the configured source-concurrency ceiling. The node
divides its memory and temporary-disk budgets across those request slots,
enforces a one-minute request timeout and the advertised maximum log range,
tries an alternate capable source only before returning a response, and never
stores fetched frames in the recent cache. Put additional connection and
request-rate limits at the reverse proxy when exposing this potentially
expensive path.

Suggested alerts:

- liveness failure: page immediately;
- required live/finality readiness false for 5 minutes: page;
- processor head stops advancing for 3 expected block intervals: warn;
- pending-delta or outbox bytes exceed 80% of configured budget: warn;
- database size approaches the filesystem limit: warn at 70%, page at 90%;
- finalized cursor does not advance for 20 minutes: warn;
- any finalized contradiction or store-integrity failure: stop publication
  and page immediately.

Import `observability/grafana/leani.json` for the release dashboard and
load `observability/prometheus/alerts.yaml` into the Prometheus-compatible rule
evaluator. The checked rules cover missing scrapes, required-component
readiness, stalled processor/finality cursors, and a non-draining outbox.
Filesystem percentage alerts remain the host operator's responsibility because
the node exports its database/recent-cache byte counts but not host capacity.

## Backup, restore, and compact

Stop the node and any embedded subscription or standalone backfill using this
data directory before running `leani db` commands (including `inspect`, `verify`,
and `backup`). They open the store with migrations enabled and therefore take
the same `.leani.lock` as other writers. Use the API and `/metrics` for inspection
while the node is running.

Create a consistent backup after stopping those writers:

```bash
leani db backup /backups/leani-$(date +%s).sqlite \
  --config /etc/leani/node.toml
```

That command is intentionally SQLite-only. When
`artifact_storage.backend = "tiered_segments"` and any segments exist, it
fails before writing a backup rather than producing an incomplete restore.
Until a versioned bundle-backup command ships, stop the node and snapshot the
complete `data_dir` (SQLite database/WAL plus `processor-artifacts`) or use an
atomic filesystem/volume snapshot.

Verify the backup using an otherwise identical config whose `data_dir` points
to an isolated directory. Never overwrite the active database. Stop the
service, move the active database aside, copy the verified backup into place,
then start the previous compatible binary. Retain the moved database until
coverage, processor counts, cursors, and subscriptions are verified.

`db compact` checkpoints the WAL and vacuums the active database; immutable
artifact segments need no rewrite. The rewrite also turns on SQLite
incremental auto-vacuum, which new stores have from creation, so pages freed by
pruning return to the filesystem. A store created before auto-vacuum was
enabled logs a warning at every start and never shrinks below its high-water
mark; run `db compact` on it once. Schedule it only with sufficient free disk,
because the vacuum rewrites the whole database, and lower ingestion load.
`db prune-changes` is lease-aware; expired consumers must perform the
documented snapshot/reset flow.

## Failure runbooks

### History schema or source failure

Stop the affected backfill, keep the last committed coverage, run the source
probe, and compare its schema/source plan with the last benchmark report.
Switch to another capable configured source at the missing range boundary.
Never mark failed chunks complete.

### P2P outage or invalid material

Readiness must become false when the live session is unavailable. Open
`/debug/network` or inspect `/v1/network/status` before treating every
zero-peer message as a live outage. Preserve the recent and undo windows, stop
included publication before a hard storage limit, and compare local queue
wait with peer timeouts. The production source keeps one persistent broad peer
pool, penalizes invalid material through Reth, retries without discarding
healthy connections, and caps recovery backoff. Invalid commitments are
permanent evidence against that response, not retryable data.

Archive lag is not a live outage. The reconciliation cursor remains at the
first unverified finalized batch and tries interchangeable history sources
again. A durable `failed` archive reconciliation is a correctness incident:
preserve the database and source object, stop publication, and compare the
processor version/configuration and block identity before resuming.

### Finality outage or disagreement

Continue only explicitly included publication. Do not advance or prune the
finalized cursor. A disagreement or contradiction is fail-closed and requires
operator review of checkpoint provenance and independent endpoints.

### Storage pressure

Inspect per-processor attribution with `db inspect` and `/metrics`. Prune
change history only through `db prune-changes`, respecting active leases.
Never evict rollback data required by unfinalized blocks. Add capacity or stop
included advancement before the hard limit. If the database file stays at its
high-water mark after pruning, check the startup log for the auto-vacuum
warning and run `db compact` once.

Retained recent frames are the reorg and replay input, so the node never drops
an unfinalized one to make room. When they reach
`budgets.recent_raw_hard_bytes`, live ingestion stops before the next block,
live readiness drops, and the log reports `recent_storage_full`. Ingestion
resumes once a finality advance prunes older frames. After 15 minutes at the
limit the live lane fails, and the supervisor restarts the network lanes from
a fresh finalized anchor, which lets finality prune again. A node that keeps
reaching the limit needs working finality, or a hard limit above the
`rpc.minimum_recent_blocks` window plus the unfinalized tail.

### Parked processor live lane

A failure that belongs to one processor parks only that processor's live
lane; shared ingestion, finality, and every other processor continue. This
holds for every processor mode and delivery ordering.
`/v1/processors/{id}/status` reports the lane as `paused` or `failed`.
`/v1/network/status` adds its `runState`, `pauseReason`, and `liveGap` (the
first unapplied block), and `leani_processor_run_state` carries the state. A
paused lane resumes by itself. A failed lane is frozen: it commits no live
block, and finality does not advance it. It is still mapped for a reorg's
replacement blocks, only to move its first unapplied block onto the new
branch. Its first failure stands: a later failure does not change its reason,
and a later park does not move its first unapplied block. Only
`processor_finality_conflict` replaces the reason. A failed lane waits for an
operator: fix the cause, then call
`POST /admin/v1/processors/{id}/lanes/live/reset`, which works for every
delivery ordering and pauses the lane so that it replays from its first
unapplied block. A lane that fails again on the same block parks again; it
does not stop the node.

A reorg never leaves a parked lane's first unapplied block on a block the
chain no longer has. A reorg that replaces blocks at or below it moves it down
to the first replacement block, so the lane replays the whole new branch. A
reorg without a replacement branch moves it onto the new tip, which the lane
has applied: a paused lane resumes at once, and a failed lane's first
unapplied block moves onto the new tip's successor once that block arrives. A
first unapplied block found off the canonical chain, as an earlier version
could leave one, moves back to the last canonical block the lane applied; the
lane then replays from there instead of skipping blocks or failing.

| Reason | State | Resumes when |
|---|---|---|
| `delivery_spool_hard_limit`, `physical_store_hard_limit`, `artifact_store_hard_limit`, `pending_delta_hard_limit` | paused | Consumers acknowledge or storage frees up. |
| `unfinalized_gap_waiting_for_finality` | paused | Finality reaches the first unapplied block, which then comes from the history source. No retained frame can serve that block, for example because it was retained without this processor's material. |
| `finalized_gap_waiting_for_history_source`, `finalized_gap_recovery_unavailable`, `finalized_gap_recovery_incomplete` | paused | A history source can serve the finalized gap. |
| `operator_reset_pending_replay` | paused | The replay after an operator reset reaches the live tip. |
| `hot_cold_handoff_failed` | paused | The processor's automatic cold backfill or its overlap verification failed; the handoff is recorded as failed. A block-local lane replays and resumes at once. An ordered lane resumes once a later backfill, such as the one at the next start, passes its gap. |
| `processor_live_reduce_failed` | failed | Reset after fixing the processor or its input. Its reducer, or the store's check of what the reducer wrote, rejected the block. |
| `processor_live_mapping_failed` | failed | Reset after fixing the processor or its input. Mapping the block, or checking a pending delta's finality variants, failed. |
| `processor_live_delta_conflict` | failed | Investigate before resetting: a pending delta for an applied block carries different content, so the processor's transform is not deterministic or its inputs differed. |
| `single_block_exceeds_delivery_limit`, `single_delta_exceeds_pending_delta_limit`, `live_gap_marker_exceeds_pending_delta_budget` | failed | Reset after raising the limit. |
| `live_gap_canonical_identity_changed` and the other `finalized_gap_recovery_*` reasons | failed | Reset after checking the history source against the canonical chain. |
| `processor_finality_conflict` | failed | Rebuild the processor as a replacement instance; the reset route refuses this lane with `409 live_lane_requires_rebuild`. The same instance cannot be rebuilt in place today: configure a replacement instance, with a new processor version or configuration, and move to it as in [Processor rebuild or rollback](#processor-rebuild-or-rollback). Its coverage holds a finalized block hash at another height, so it contradicts the canonical chain, and finality stops advancing it. This reason replaces any earlier failure reason. |

### SQLite failure

Stop writers, run `db verify`, preserve the database and WAL for diagnosis,
and restore a previously verified backup into a new path. Do not run a
destructive downgrade or hand-edit schema metadata.

### Expired stream cursor

The API returns `410 cursor_expired` or a terminal `reset_required` SSE event.
Query a fresh snapshot, atomically replace consumer state, then resume from
the returned retained cursor.

### Processor rebuild or rollback

Processor code/config identities create separate immutable instances. Backfill
the replacement instance in parallel, compare coverage and outputs, then move
consumers. Rollback selects the prior binary/store pair or restores its backup;
it never mutates a newer instance in place.
