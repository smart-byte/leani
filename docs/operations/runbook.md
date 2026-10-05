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
Its processor uses on-demand history, so nothing is indexed until you run a
bounded `backfill` against the same data volume. `serve` holds the data
directory's lock, and a backfill beside it fails with `runtime data directory
/var/lib/leani is already in use by another Leani process`, so stop the service
first. `docker compose run` mounts the service's volume, and its arguments
replace the image's `serve` command, so name the profile with `--config`:

```bash
docker compose stop leani
docker compose run --rm leani backfill --config /etc/leani/node.toml \
  --processor blobs-money --from-block 19426589 --to-block 19427588
docker compose up -d leani
```

Keep `LEANI_API_TOKEN` exported for these commands too: Compose resolves it
for every command. The processor starts at block 19,426,589 and is
block-local, so later ranges may follow in any order. Then query the range.
Subscriptions remain quiet until a live source commits events.

A live-enabled node with a verified finalized head also accepts the CLI over
its native API, without stopping it:

```bash
leani backfill --endpoint http://127.0.0.1:18080 \
  --processor blobs-money --from-block 19426589 --to-block 19427588
```

The standalone form attaches the same verified P2P history fallback when
`sources.live.kind = "p2p"`; the historical-only container profile above
leaves P2P disabled. Use a live-enabled configuration when missing Xatu
partitions must be recovered from peers.

The profile protects the API with a bearer token from `LEANI_API_TOKEN`:
`serve` fails to start without one of at least 16 printable ASCII characters
without spaces, so export it, for example
`export LEANI_API_TOKEN="$(openssl rand -hex 32)"`, before `docker compose up`,
which stops with an error when it is unset, or pass it with
`docker run --env LEANI_API_TOKEN`. The
container's health check calls `/health/live`, which needs no token. The
profile opts JSON-RPC, which has no authentication, into its all-interface
binds with `rpc.allow_unauthenticated_remote`; keep its ports on loopback or
behind an authenticating proxy. The API answers only `Host` names in
`api.allowed_hosts`, besides `localhost` and IP addresses; the profile lists
the compose service name `leani`, so other services can call
`http://leani:8080`. To add the names your clients use, mount a copy of the
profile with them over `/etc/leani/node.toml`.

The profile runs `blobs-money` as the instance `blobs-container-1-5`. A new
processor version that changes the processor's identity takes a new instance
ID, because the store refuses another version under an instance it holds.
Version 0.1.0-rc.1 ran `blobs-money` 1.4.0 as `blobs-container`, so on a
volume from rc.1 the new image starts beside that instance and leaves it
inert: nothing indexes or serves it, but its rows stay in the volume and
count toward `[budgets.store] maximum_physical_bytes`. This upgrade is itself
an instance replacement: backfill subscriptions on `blobs-container` show
`processorConfigured: false` and keep counting against
`[budgets.delivery] maximum_history_retained_bytes` until you settle them as
step 2 of [Processor rebuild or rollback](#processor-rebuild-or-rollback)
describes. For a clean slate, remove the volume with `docker compose down -v`
before `docker compose up --build`, or restore a snapshot of the volume.

The profile runs at most two history chunks at once
(`[budgets.history_pipeline] maximum_active_chunks = 2`): a Xatu backfill
holds about 450 MiB per active chunk, so two fit its 1 GiB `memory_bytes`
budget and leave the rest of the compose file's 2 GiB container limit to the
node.

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
exponential backoff (see [Shutdown and supervision](#shutdown-and-supervision)).

The live lane includes blocks only up to the newest attested head: the
execution header of a light-client optimistic update that at least two thirds
of the sync committee signed, which the finality source verifies each slot.
Included blocks therefore trail the chain tip by one to two slots. When no
new attested head arrives for four slots, for example while the finality
transports are down or sync-committee participation stays below two thirds,
the lane reports itself disconnected and live readiness drops until one does.
Configuration refuses `sources.live.kind = "p2p"` with
`finality.kind = "disabled"`, because nothing would verify those heads; use
`sources.live.kind = "disabled"` for finalized-dataset backfills.

When live readiness drops this way while finality stays ready, run
`leani source probe finality`. Each Beacon endpoint whose finality verified
reports its `attested_head`, or in `attested_head_error` why it gave none: it
served no optimistic update, its update anchors no head, or its update failed
verification. The lane needs one endpoint that serves
`/eth/v1/beacon/light_client/optimistic_update`; replace endpoints that serve
none. With `finality.kind = "consensus_p2p"`, peers must serve the
`light_client_optimistic_update` request; the debug log names each peer that
served none.

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
verified execution anchor. Leani qualifies several sessions concurrently until
the configured body-serving target is ready, then one at a time. Qualification
is background work at normal priority throughout: it never uses the request
slots reserved for the live lane. Qualification ranks proven serving peers
first but does not exclude a newly connected peer: each material response is
independently verified before use, so fresh peers remain immediate fallbacks.
Proven serving peers are tried before untested discovery records on the next
start.

`sources.live.material_request_concurrency` bounds simultaneous physical
requests together with the global source budget and connected-peer capacity.
Every execution P2P request shares one process-wide limit: the larger of
`material_request_concurrency` and `history_header_request_concurrency`, 32 by
default. History, probes, and peer qualification leave four of those slots
free for the live lane (half the limit when it is below eight), and a waiting
live request gets the next free slot first. Each source acquisition's
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
whole-manager watchdog. Each rebuild logs `building the execution P2P network
manager` and then `built the execution P2P network manager` or, at warn
level, `failed to build the execution P2P network manager` with its error.
While the live lane reconnects, it logs each failed attempt at warn level in
`the live lane could not reconnect to the execution network; retrying`. A
fixed Discv5 port can stay busy for up to a minute after a rebuild, while the
ended manager's last discovery lookup finishes; the lane keeps retrying
meanwhile.

Processor `coverage.complete` means only that the stored range from the
configured start through `processedThrough` has no internal gaps. It does not
mean that `processedThrough` has reached the live source. The network status
adds `storedRangeContiguous`, `syncTargetBlock`, and `blocksRemaining`; use
those fields together with `readiness.liveReady` when diagnosing catch-up.
`syncTargetBlock` is derived from the live session's `observedHeadBlock`: the
newest header the live lane has validated, never a head a peer only claimed.
It reports validated progress, so while the node catches up it trails the
network head. `fromBlock` and `toBlock` describe only its current material
request. A speculative request for the block after the observed head therefore
does not create artificial processor lag.
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
closed as a real transform or source conflict. At a restart, startup
reconciliation instead deletes every pending delta for a block the processor
already applied, without comparing it, before opening any network lane, so
archive availability or a slow P2P catch-up cannot delay cleanup; its warning
counts them in `discarded_pending_deltas`. `processors[].pendingDeltas`
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
available. Each poll waits about two seconds after the first verified endpoint
for the others, then follows the highest finalized slot that
`minimum_agreement` endpoints have reached or passed, so a lagging endpoint
neither holds finality back nor drops readiness at an epoch transition.
Finality never moves backwards, and two verified endpoints that report
different roots at one slot fail closed. Endpoints that name one transport
twice, for example with and without a trailing slash, fail validation because
each would count toward agreement.

See [Checkpoint lifecycle](#checkpoint-lifecycle) for how the configured
checkpoint is used and when it must be refreshed.

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
- `leani_finality_anchor_expiry_seconds` below three days: warn;
- any finalized contradiction or store-integrity failure: stop publication
  and page immediately.

Import `observability/grafana/leani.json` for the release dashboard and
load `observability/prometheus/alerts.yaml` into the Prometheus-compatible rule
evaluator. The checked rules cover missing scrapes, required-component
readiness, stalled processor/finality cursors, a non-draining outbox, and a
finality anchor close to its age limit.
The stalled-cursor rules apply only where the profile requires the live
source (`leani_live_required`) or finality (`leani_finality_required`), so a
historical-only node whose backfill has finished does not alert. They join
processor series to those gauges on the scrape target's `job` and `instance`
labels. Keep `honor_labels` disabled, the default, for the Leani scrape job:
Prometheus then stores each processor's own `instance` label as
`exported_instance`.
Filesystem percentage alerts remain the host operator's responsibility because
the node exports its database/recent-cache byte counts but not host capacity.

## Shutdown and supervision

`serve` stops at Ctrl-C or SIGTERM. Open change streams (SSE) and consumer
delivery streams (NDJSON) end after a whole event or record, and JSON-RPC
WebSocket connections get a `1001` going-away close frame; clients reconnect
from their last cursor as after any closed connection. The node then waits at
most 10 seconds for open connections and its background work, and at most 5
more for blocking work such as a file write, abandons whatever is still
open, and exits 0. A second signal during the first 10 seconds exits at once
with status 130; startup reconciliation repairs a commit it interrupted (see
[Crash or interrupted commit](#crash-or-interrupted-commit)).
`deploy/leani.service` (`TimeoutStopSec`) and `compose.yaml`
(`stop_grace_period`) allow 30 seconds for the stop; give another container
setup more than Docker's default 10 seconds.

Background work runs in supervised tasks: the durable backfill and
raw-history schedulers, the network lane supervisor, artifact compaction,
query snapshot cleanup, and each processor's delivery pruning and coverage
compaction. Each runs until the shutdown. A task that stops before it, by
returning, failing, or panicking, shuts the node down: the log names the task
in `a background task ended; shutting the node down` at error level, and the
node exits with status 1 after the same bounded shutdown. The systemd unit
(`Restart=on-failure`) and `compose.yaml` (`restart: unless-stopped`) start it
again. A persistent execution P2P source that cannot be built, for example
for an invalid `sources.live.nat`, stops the node the same way.

The network lane supervisor restarts failed runs of the network lanes
itself, and drops readiness whenever the lanes do not run, a panic included.
A failed run restarts after 1 second, and each further failure doubles the
wait up to a minute; after a run that stayed up for at least a minute, the
wait starts at 1 second again. A run that ends waits for its cold backfills
to stop before the next one starts; every 10 seconds of that wait, the log
names the ones still running in
`cancelled automatic cold backfills are still running` at warn level. When
the lanes halt (see
[Finality contradicts the followed chain](#finality-contradicts-the-followed-chain)),
they stay stopped and not ready while the node keeps serving what it stored.

A live lane that stays not ready while verified finality is ready for
`sources.live.stall_timeout_seconds`, ten minutes by default, within one run
of the lanes is treated as stalled. Usually its execution P2P network is
wedged, which a lane restart cannot heal; it can also be a finality endpoint
that stops sending attested heads, which a restart may not fix. The lanes
then stop, the log says `the live lane stalled while verified finality stayed
ready; the node exits so that it restarts` at error level, and the node exits
with status 1 for systemd or Docker to start it again. While finality is not
ready either, the network itself is likely down and the node keeps waiting,
and a wait at the recent-frame hard limit does not count (see
[Storage pressure](#storage-pressure)). A run that fails quickly, as when
another process holds the P2P ports or `persistent_retries = false` gives up,
never reaches the timeout: the supervisor restarts it with backoff and logs
each failure. Run the node under a supervisor that restarts it, or raise the
timeout, up to 30 days.

## Checkpoint lifecycle

`finality.checkpoint`, with `finality.checkpoint_slot`, is the trust root, and
it is needed only for the first start. That start verifies a light-client
bootstrap for the checkpoint, which must be at most 14 days (336 hours) older
than its slot.

From then on, the finality source keeps one bootstrapped light client, one per
endpoint for `beacon_api`, and advances it with every finality update, plus
the sync-committee update when a period ends. Nothing is bootstrapped again,
and no age check runs while the node keeps running. A `beacon_api` endpoint
that was down at start, or bootstraps slowly, bootstraps later from the newest
anchor the endpoints agreed on, so it can still join after the configured
checkpoint has aged out. Its bootstrap keeps running across polls instead of
restarting each time.

The node persists the newest verified finalized anchor as
`finality-anchor.json` in `data_dir`. It writes a temporary file, syncs it to
disk, and renames it over the old one, so a crash leaves either the previous
anchor or the new one. The file records the anchor's beacon slot and root, its
execution block number and hash, the configured checkpoint it was verified
from, and a format `version`. It holds no secrets. Only anchors at an epoch's
first slot are written, because Beacon nodes serve bootstraps for
epoch-boundary blocks, so the file trails finality by about one epoch. An
older anchor never replaces a newer one.

Every start, and every restart of the network lanes, bootstraps from the
persisted anchor instead of the configured checkpoint when both hold:

- the anchor was verified from the configured checkpoint, so it is newer by
  construction; and
- the anchor is at most 14 days old.

An anchor verified from another checkpoint is never used: changing
`finality.checkpoint` re-anchors the node on the new root, and the node logs a
warning naming both roots. `leani doctor` reports the mismatch too. The one
exception is an embedded subscription (`leani subscribe` without a running
node) that reuses a cached checkpoint, `checkpoint.json`, whose trust is
`locally_verified`: an anchor the subscription verified itself on an earlier
run. A persisted anchor at a later slot replaces it. A checkpoint from a
provider quorum follows the rule above, whether the subscription has just
accepted it or reuses it from the cache with trust `provider_quorum`.

The bootstrap is still verified against the anchor's block root, and the
configured checkpoint's own age is not checked. A missing, corrupt, or
unreadable file, or an anchor past the limit, falls back to the configured
checkpoint with a warning. So does a persisted anchor whose bootstrap no
endpoint or peer serves. Startup fails only when the configured checkpoint is
unusable too, for example because it is older than 14 days.
`leani source probe finality` and the benchmark's history probe start from
the persisted anchor by the same rules, but never write the file.

The persisted anchor stops advancing while verified finality stalls, while the
node is stopped, and while writing the file fails. A failed write is logged
and counted, and never fails finality itself. Watch these:

- `leani_finality_anchor_age_seconds` is the age of the newest verified
  anchor;
- `leani_finality_anchor_expiry_seconds` is the time left before a restart
  can no longer bootstrap from it, and turns negative once it has passed;
- `leani_finality_anchor_write_failures_total` counts failed writes of
  `finality-anchor.json`: while it grows, a restart may start from an older
  anchor than these gauges show;
- the `LeaniFinalityAnchorExpiring` alert fires when fewer than three days
  remain;
- `leani doctor` warns when the anchor a restart would use expires within
  three days: the persisted anchor, or the configured checkpoint if none
  applies.

To refresh the trust root, take a recent finalized checkpoint root and slot
from independent providers, set `finality.checkpoint` and
`finality.checkpoint_slot`, and restart: the persisted anchor verified from
the old root is then ignored. `leani reset all` removes the file with the rest
of the runtime state.

Errors, logs, and probe reports show endpoint and provider URLs as
`scheme://host[:port]`, with `/…` in place of any path, because providers put
API keys in userinfo, query strings, and paths. Request errors add the API
path, such as `eth/v1/beacon/genesis`. Endpoints that would show alike carry
their index, such as `finality.endpoints[1]`.

## Backup, restore, and compact

Stop the node and any embedded subscription or standalone backfill using this
data directory before running `leani db` commands (including `inspect`, `verify`,
and `backup`). They take the same `.leani.lock` as other writers, and every
one but `backup` opens the store with migrations enabled, which upgrades a
store from an earlier release. Use the API and `/metrics` for inspection
while the node is running.

Create a consistent backup after stopping those writers:

```bash
leani db backup /backups/leani-$(date +%s).sqlite \
  --config /etc/leani/node.toml
```

`db backup` reads the store through a read-only connection and never
upgrades it: the backup keeps the store's schema, so the backup of a store
from an earlier release restores with that release's binary. It refuses a
store whose schema is newer than the binary supports, and a data directory
without a store.

That command is intentionally SQLite-only. When any processor artifact
segments exist, as with `artifact_storage.backend = "tiered_segments"`, it
fails before writing a backup rather than producing an incomplete restore.
Until a versioned bundle-backup command ships, stop the node and snapshot the
complete `data_dir` (SQLite database/WAL plus `processor-artifacts`) or use an
atomic filesystem/volume snapshot.

Verify the backup using an otherwise identical config whose `data_dir` points
to an isolated directory, with the binary that will open it: a newer binary
upgrades the copy. Never overwrite the active database. Stop the service, move
the active database aside with its `leani.sqlite-wal` and `leani.sqlite-shm`
files, since SQLite would replay a leftover WAL onto the restored copy, copy
the verified backup into place, then start the previous compatible binary. Retain the moved database until
coverage, processor counts, cursors, and subscriptions are verified.

`db compact` checkpoints the WAL and vacuums the active database; immutable
artifact segments need no rewrite. The rewrite also turns on SQLite
incremental auto-vacuum, which new stores have from creation, so pages freed by
pruning return to the filesystem. A store created before auto-vacuum was
enabled logs a warning at every start and never shrinks below its high-water
mark; run `db compact` on it once. Like every `leani db` command, it runs only
while the node is stopped, and it needs free disk for a second copy of the
database, because the vacuum rewrites the whole database.
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
operator review of checkpoint provenance and independent endpoints. A single
bad transport is not an outage: consensus P2P bans a peer whose light-client
material fails verification, asks the next one, and neither asks nor dials it
again, across reconnects, until the lanes restart. Stale updates are retried,
so neither ends the finality stream. An exhausted peer set logs a warning at
most every five minutes.

### Network fork this release does not support

Each release verifies the consensus forks it knows; Glamsterdam's Gloas is
not among them yet. Once mainnet activates a fork a release does not know,
finality stays unavailable and the live lane stops, fail-closed: consensus
peers answer with a fork digest the node does not know, which it logs once as
`consensus peers serve a fork this release cannot verify`, and a Beacon API
endpoint fails with an error naming the fork, such as
``serves `gloas` light-client data``. No peer is banned for it. Upgrade to a
release that supports the fork before its mainnet activation. Upgraded
within 14 days, the node resumes from its persisted finality anchor; later,
it needs a fresh `finality.checkpoint`.

### Live lane disconnected during a finality apply

Symptom: right after finality advances, the live lane reports itself
disconnected with `no sync-committee-attested execution head above block …
for … seconds`, live readiness drops, and no block is included, although the
finality sources are healthy. It is most likely every 1,000 finalized blocks,
when each processor with automatic checkpoints copies its whole state into a
recovery checkpoint, and with large processor states.

Cause: both finality sources verify and publish attested heads only while
the node reads their finality stream, and the node applies each verified
finality update, checkpoints included, before it reads the next. An apply that
takes longer than about 36 seconds leaves the live lane without a new attested
head for its 48-second grace, four slots.

What to expect: nothing is lost or reverted. When the apply completes, the
next attested head arrives, the lane catches up on the blocks produced
meanwhile, and readiness returns. This is a known limitation of this
release; a later release publishes attested heads independently of finality
applies.

### Finality contradicts the followed chain

When verified finality names another block at a height than the retained
canonical chain holds, or a retained block below the finalized block does not
link to it, the node neither finalizes either chain nor defers forever.

A contradiction with retained *unfinalized* blocks, for example after the lane
stalled across a reorg of an attested head, heals itself. The node logs
`verified finality contradicts retained unfinalized blocks; restarting the
network lanes to revert them` as a warning and restarts the network lanes
after the supervisor's backoff. The restart reverts only the retained
unfinalized blocks that are not on the newly verified finalized chain, and
undoes processor coverage of them as a reorg, logging `retained unfinalized
blocks do not link to the verified finalized anchor; reverting them as a
reorg`. Those blocks are never finalized; the cold backfill and the live lane
fetch the finalized chain again.

Every start, and every restart of the network lanes, decides which retained
blocks are on that chain the same way. Where parent links from them do not
reach the anchor, as after a contradiction or an outage longer than the
finality lag, the node proves the blocks on the anchor's chain with
execution headers from its peers that link by parent hash down from the
anchor, keeps and finalizes them, and logs `proved retained unfinalized
blocks against the verified finalized anchor` with the blocks kept and
reverted. When it cannot prove them within 60 seconds, for example without
execution peers, it logs `could not prove retained unfinalized blocks against
the verified finalized anchor; reverting them` as a warning; when the proof
would span more than 8,192 blocks, `retained unfinalized blocks are too far
below the verified finalized anchor to prove; reverting them`. It then reverts
every retained unfinalized block that does not link to the anchor.

The network lanes halt instead when the contradiction is with *finalized*
history, which no revert repairs, or when the same unfinalized contradiction
comes back a third time before finality moves to another block, so two
restarts did not repair it. The node then logs `verified finality contradicts
the retained canonical chain; network lanes halt until the node restarts` at
error level: readiness stays false, `network.supervisor` reports `stopped`
with the contradiction as its last error, and nothing retries it. The API
keeps serving what is stored.

1. Preserve the database and logs. Check the checkpoint provenance and the
   finality transports: a halt means the node followed another chain than the
   one consensus finalized and could not revert it, or a trust root is wrong.
2. If the error says `verified finality contradicts finalized canonical
   history`, finalized history in the store is wrong and no revert repairs
   it: rebuild in a new `data_dir`.
3. If it says the contradiction `recurred after 2 restarts of the network
   lanes`, restart the node once the finality transports check out: startup
   reverts the unfinalized blocks as above. If it halts the same way again,
   rebuild in a new `data_dir`.

### Storage pressure

Inspect per-processor attribution with `db inspect` and `/metrics`. Prune
change history only through `db prune-changes`, respecting active leases.
Never evict rollback data required by unfinalized blocks. Add capacity or stop
included advancement before the hard limit. If the database file stays at its
high-water mark after pruning, check the startup log for the auto-vacuum
warning, stop the node, and run `db compact` once.

Retained recent frames are the reorg and replay input, so the node never drops
an unfinalized one to make room. When they reach
`budgets.recent_raw_hard_bytes`, live ingestion stops before the next block,
live readiness drops, and the log reports `recent_storage_full`. Ingestion
resumes once a finality advance prunes older frames. After 15 minutes at the
limit the live lane fails, and the supervisor restarts the network lanes from
a fresh finalized anchor, which lets finality prune again; the live stall
timeout does not count this wait. A node that keeps
reaching the limit needs working finality, or a hard limit above the
`rpc.minimum_recent_blocks` window plus the unfinalized tail.

A processor's delivery log is pruned in the background by its
`delivery.pruning` policy: finalized changes older than its window, and,
while the log exceeds its byte limit or the processor is paused at it, older
finalized changes whatever their age, never one a required consumer has not
acknowledged. A paused processor resumes once the log is below 90 percent of
its limit. Embedded subscriptions (`leani subscribe` without a node) prune
the same way while they run, so their built-in processors keep within 64 MiB
and 24 hours of changes.

Retained raw history has its own `[raw_history]` budgets. A raw-history store
over either of them, for example after lowering one, still opens and serves
its segments, but admits no new segment. A job created with
`segment.onLimit: pause` waits at the storage limit, with the budget named in
`lastError`, and resumes once the node runs with a larger budget; one created
with `fail` fails.

### Retained raw-history segment failure

The node verifies each retained segment's whole-file checksum the first time
it reads the segment after starting, and each block's record checksum on
every read. At startup it only checks that each segment file exists with its
closed length, and logs what it recovered. A segment that fails never stops
the node: it leaves the catalog, so its blocks read as missing and RPC and
backfills use another source, its file moves to
`data_dir/raw-history/quarantine` with a `.corrupt` suffix, and the node logs
a warning. A segment whose file is missing at startup leaves the catalog the
same way. A closed file no catalog row claims, left by a crash before its
publication committed, moves to the quarantine with an `.orphan` suffix.

1. Check the raw-history jobs: each job that owned the segment names it and
   its blocks in `lastError`. A running job acquires those blocks again, and
   a completed one returns to the queue and does too. A failed or cancelled
   job keeps the error it ended with.
2. Check the disk and filesystem before trusting the node with more writes:
   a corrupt segment usually means a failing disk.
3. The quarantine keeps at most 1 GiB, or one segment of
   `raw_history.maximum_segment_physical_bytes` when that is larger. Past
   that, the files quarantined longest ago are deleted first, at startup and
   after each new quarantine, but the newest is always kept. Copy a file
   elsewhere to keep it for inspection; delete the others whenever you like.

### Retained raw-history segments unavailable

When more than half of the catalogued segment files are missing at startup,
as when the volume holding `data_dir/raw-history/segments` is not mounted yet
or a restore is still running, the node takes the directory as unavailable
rather than emptied. It keeps every catalog row, logs a warning, and counts
them as `unavailable_segments` in its startup recovery log. A segment file
that disappears while the node runs is treated the same way. Reads of those
segments fail and fall back to other sources, and the jobs that own them do
not acquire them again. Do not create raw-history jobs until the directory
is back: a new job drops the rows of the missing segments nothing owns, and
may replace others with segments it acquires itself.

1. Mount or restore the directory, then restart the node: the segments whose
   rows were kept serve again. A returning file whose row a new job dropped
   meanwhile is quarantined as an orphan.
2. If the files are gone for good, delete every raw-history job that owns
   them, cancelling running ones first, then create each again with the same
   request. The new job skips a segment whose file is gone, removes its
   catalog row once nothing owns it, which also frees its share of the
   budgets, and acquires the blocks from its sources.

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
`POST /admin/v1/processors/{id}/lanes/live/reset` with the header
`x-leani-request: 1`, which works for every
delivery ordering and pauses the lane so that it replays from its first
unapplied block. A lane that fails again on the same block parks again; it
does not stop the node.

A reorg never leaves a parked lane's first unapplied block on a block the
chain no longer has. A reorg that replaces blocks at or below it moves it down
to the first replacement block, so the lane replays the whole new branch. A
reorg without a replacement branch moves it onto the new tip. When the lane
has applied that tip, a paused lane resumes at once, and a failed lane's first
unapplied block moves onto the new tip's successor once that block arrives. If
the lane never applied the new tip, such as the finalized anchor the node
seeds after downtime, a paused block-local lane's gap simply completes, while
an ordered lane waits there for that block from history. A failed block-local
lane keeps its first unapplied block on that tip, so after a reset it waits
for a history source to serve it (`finalized_gap_waiting_for_history_source`),
a block it does not need: make sure a history source can serve the finalized
anchor, and the lane applies it and resumes by itself. A first unapplied
block found off the canonical chain, as an earlier version could leave one,
moves back to the last canonical block the lane applied; the lane then
replays from there instead of skipping blocks or failing.

| Reason | State | Resumes when |
|---|---|---|
| `delivery_spool_hard_limit`, `physical_store_hard_limit`, `artifact_store_hard_limit`, `pending_delta_hard_limit` | paused | Consumers acknowledge or storage frees up. |
| `unfinalized_gap_waiting_for_finality` | paused | Finality reaches the first unapplied block, which then comes from the history source. No retained frame can serve that block, for example because it was retained without this processor's material. |
| `finalized_gap_waiting_for_history_source`, `finalized_gap_recovery_unavailable`, `finalized_gap_recovery_incomplete` | paused | A history source can serve the finalized gap. |
| `operator_reset_pending_replay` | paused | The replay after an operator reset reaches the live tip. |
| `startup_reconciliation_replay` | paused | The replay of retained frames that [startup reconciliation](#crash-or-interrupted-commit) started reaches the live tip, normally before live blocks resume. |
| `hot_cold_handoff_failed` | paused | The processor's automatic cold backfill or its overlap verification failed; the handoff is recorded as failed. A block-local lane replays and resumes at once. An ordered lane resumes once a later backfill passes its gap. A start's backfill runs only to the finalized head, so restart the node once `chainFinalizedHead` in `/v1/network/status` has reached the lane's `liveGap`; an earlier restart leaves the lane paused. |
| `processor_live_reduce_failed` | failed | Reset after fixing the processor or its input. Its reducer, or the store's check of what the reducer wrote, rejected the block. |
| `processor_live_mapping_failed` | failed | Reset after fixing the processor or its input. Mapping the block, or checking a pending delta's finality variants, failed. |
| `processor_live_delta_conflict` | failed | Investigate before resetting: a pending delta for an applied block carries different content, so the processor's transform is not deterministic or its inputs differed. The applied block stands. Restart the node, whose [startup reconciliation](#crash-or-interrupted-commit) deletes that pending delta, then reset the lane. |
| `single_block_exceeds_delivery_limit`, `single_delta_exceeds_pending_delta_limit`, `live_gap_marker_exceeds_pending_delta_budget` | failed | Reset after raising the limit. |
| `live_gap_canonical_identity_changed` and the other `finalized_gap_recovery_*` reasons | failed | Reset after checking the history source against the canonical chain. For an ordered lane, `finalized_gap_recovery_canonical_mismatch` can also mean that the block history returned does not descend from the lane's own last applied block, which is then off the canonical chain: restart the node, whose [startup reconciliation](#crash-or-interrupted-commit) undoes an unfinalized off-chain block, then reset. The lane then waits in `operator_reset_pending_replay` until the automatic cold backfill of the next start fills the undone heights, so restart the node once more; an on-demand processor waits for a backfill request instead. A finalized off-chain block needs a rebuild. |
| `processor_finality_conflict` | failed | Rebuild the processor as a replacement instance; the reset route refuses this lane with `409 live_lane_requires_rebuild`. The same instance cannot be rebuilt in place today: configure a replacement instance, with a new processor version or configuration, and move to it as in [Processor rebuild or rollback](#processor-rebuild-or-rollback). Its coverage holds a finalized block hash at another height, or another block at a finalized height, so it contradicts the canonical chain, and finality stops advancing it. This reason replaces any earlier failure reason. |

### Crash or interrupted commit

A live block commits in steps: the node first retains its frame as the new
canonical block, then each processor applies it in a transaction of its own.
A reorg likewise switches the canonical chain first and then undoes each
processor's reverted blocks. When an apply reaches a processor's delivery
limit, the store pauses or fails the lane first and the runtime then records
its first unapplied block. A crash, a kill, the embedded subscription
runtime's shutdown abort, or a network-lane restart can stop between those
steps. That leaves a processor behind the canonical chain or on a reverted
branch, a pending delta that nothing applies, or a stopped lane with no record
of where it stopped. Every network-lane start therefore runs startup
reconciliation for each processor before any lane opens:

1. It undoes the processor's unfinalized blocks that are not the canonical
   block at their height, newest first, and publishes their undo changes as
   the reorg would have.
2. It deletes pending deltas the processor can never apply: those below its
   start, for a block that is not canonical, and for a block it already
   applied. For a block-local processor that includes any below its first
   unapplied block, such as one an interrupted cold-backfill commit left; the
   backfill maps that block again.
3. It pauses a running lane that trails the retained canonical frames at its
   first unapplied one, with reason `startup_reconciliation_replay`, and
   replays those frames through the normal gap path before live blocks resume.
   A lane that has applied nothing yet, such as a newly configured block-local
   processor, replays the retained frames too. A block-local lane whose next
   block falls in a hole in the retained frames, such as blocks never retained
   over downtime, replays from the first retained frame above the hole; its
   cold backfill covers the hole.
4. A lane the store paused or failed at its delivery limit, but whose first
   unapplied block was never recorded, gets that block recorded and keeps its
   state and reason. A paused lane replays from there once consumers
   acknowledge and delivery capacity frees up, instead of skipping the blocks
   it missed, and a failed lane can be reset. The recorded block carries no
   required delivery size, so for `single_block_exceeds_delivery_limit` the
   reset route cannot refuse a limit that is still too small; the replay then
   fails the lane again on the same block.

The store can also pause or fail a lane at its delivery limit while the lane
is at the live tip, for example in its cold backfill's commit on the lane's
live stream. That records no first unapplied block either, and at a restart
reconciliation has no retained block to record yet. The live lane handles it
with its next block: a paused lane commits that block as usual, so the store's
refusal parks the lane there, or applies it once capacity has freed up. A
failed lane records that block, which a block-local lane has not applied or
which follows an ordered lane's last applied block, so a reset replays from
there. A failed ordered lane still behind its history records nothing, and
its history cannot apply while it is failed. Startup reconciliation records
its first unapplied block, the one after its cursor, only while that block is
among the retained frames. Once finality has pruned it, nothing records it:
the reset route refuses the lane with `400 invalid_request`, since it has no
durable live gap, and only rebuilding the processor as a replacement instance
recovers it, as in [Processor rebuild or rollback](#processor-rebuild-or-rollback).

A paused or failed lane whose first unapplied block is already recorded
replays from there as usual, and a lane failed for another reason, such as
`processor_finality_conflict`, is left alone. Reconciliation replays nothing
below the retained frames. A processor's automatic cold backfill fills those
blocks up to the finalized anchor; an on-demand processor waits for a backfill
request. Before its live lane applies a block past those blocks, an on-demand
block-local processor logs "the live lane skips blocks this processor never
processed" at warn level, with `from_block`, `to_block`, and `announced`, and
its live stream announces the range in a `system.live_gap.put` change for the
application to request. A startup replay from the first retained frame above
a hole announces it the same way.

Each processor that needed a repair logs one warning, "startup reconciliation
repaired processor state against the canonical chain", with the fields
`undone_blocks`, `highest_undone_block`, `discarded_pending_deltas`, and
`replay_from_block`. The info line "startup reconciliation completed" then
reports, per processor, the blocks applied and reverted and the pending deltas
left. A consistent store is left untouched, so the warning appears only after
an interruption or for a lane that trails the retained frames. No operator
action is needed.

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

The old instance's backfill subscriptions do not move. Their retained records
count against `[budgets.delivery] maximum_history_retained_bytes` until they
are deleted, and once the node no longer configures the old instance, their
stream and acknowledgement routes refuse (normally `404 not_found`), so their
consumers can neither read nor acknowledge them. Settle them around the switch:

1. Before the configuration drops the old instance, let each of its history
   subscriptions finish, with its consumer acknowledging the
   `backfill_complete` record, or cancel it. Delete each once its consumer has
   acknowledged what it was delivered.
2. After the node restarts without the old instance, list the subscriptions
   with `GET /admin/v1/backfill-subscriptions`: `processorConfigured: false`
   marks those of an instance the node does not configure. That can be
   temporary, as when a restart's configuration leaves an instance out by
   mistake: configure it again instead. If the old instance will not return,
   cancel each of its subscriptions with
   `POST /admin/v1/backfill-subscriptions/{id}/cancel` and delete it with
   `DELETE /admin/v1/backfill-subscriptions/{id}?discardUnacknowledged=true`,
   which discards its unacknowledged records. Both requests need the header
   `x-leani-request: 1`.

The SDK's backfill client makes the same calls with `list()`, `cancel(id)`,
and `delete(id, { discardUnacknowledged: true })`.

A store upgrades to the binary's schema on the first start that opens it, and
the upgrade cannot be undone. A start that the store refuses for a processor
under an existing instance ID (`processor instance … conflicts with its stored
descriptor`) refuses before that upgrade, so the store stays usable by the
binary that wrote it. Store migrations are forward only: rolling an upgraded
store back means restoring the backup taken before its upgrade.
