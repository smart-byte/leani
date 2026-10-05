---
title: Native API and delivery protocol
description: Understand snapshots, retained queries, durable consumers, change envelopes, errors, and capability discovery.
section: reference
order: 70
audience:
  - app-developer
  - operator
status: preview
---

Status: implemented v1 contract baseline, 2026-08-01.

Leani exposes two related interfaces:

1. Ethereum JSON-RPC for applications expecting an execution-client-shaped
   endpoint.
2. A native aggregate API for processor status, queries, and durable change
   streams.

The TypeScript SDK targets the native API. Existing Ethereum libraries such as
ethers or viem can use the node's JSON-RPC URL directly.

## 1. Why HTTP plus SSE first

blobs.money runs TypeScript on Bun and already uses `fetch`, ethers, and
WebSockets. The first aggregate transport should therefore be:

- ordinary versioned HTTP/JSON for queries;
- server-sent events over streaming `fetch` for a one-way durable change feed;
- OpenAPI for machine-readable query/request/response types.

SSE fits the required direction, works through common proxies, has a simple
sequence/resume model, and does not require the application to implement a
bidirectional protocol. The SDK uses streaming `fetch` rather than the browser
`EventSource` API so it can send authorization headers and work consistently in
Bun, Node, and modern browsers.

Standard `eth_subscribe` remains WebSocket JSON-RPC. gRPC can be added for
high-throughput non-JavaScript consumers without changing change semantics.

## 2. Interface layout

Default listeners:

```text
http://127.0.0.1:8080/v1/...     aggregate API
http://127.0.0.1:8545            Ethereum JSON-RPC
ws://127.0.0.1:8546              Ethereum subscriptions
```

The current binary serves all three listeners. The WebSocket endpoint accepts
ordinary JSON-RPC calls plus `eth_subscribe`/`eth_unsubscribe` for `newHeads`
and filtered `logs`. Subscription events are published only after the live
branch commits; reorged logs are sent with `removed: true` before replacement
logs. A client that falls behind the bounded event channel, or leaves more than
`rpc.max_subscription_event_bytes` of notifications unread, is closed with code
1013 and must reconnect. `eth_subscribe` limits apply per connection: at most
`rpc.max_subscriptions_per_connection` subscriptions, whose notifications for
one block or reorg may take at most `rpc.max_subscription_event_bytes`. A
connection past that is closed with code 1008; reconnect with fewer or
narrower subscriptions, or spread them over several connections. When the node
shuts down, every connection is closed with code 1001. See
[Ethereum JSON-RPC limits](https://leani.dev/docs/reference/ethereum-json-rpc/#transport-rules-and-limits).

Aggregate routes:

```text
GET  /health/live
GET  /health/ready
GET  /debug/network
GET  /metrics

GET  /v1/status
GET  /v1/network/status
GET  /v1/capabilities
GET  /v1/processors
GET  /v1/processors/{processor}/status
GET  /v1/processors/{processor}/schema
GET  /v1/processors/{processor}/changes/head
GET  /v1/processors/{processor}/changes
GET  /v1/processors/{processor}/stream
GET  /v1/processors/{processor}/collections
GET  /v1/processors/{processor}/collections/{collection}/entities
POST /v1/processors/{processor}/collections/{collection}/entities
GET  /v1/processors/{processor}/collections/{collection}/entities/{key}
POST /v1/processors/{processor}/collections/{collection}/query-and-follow
DELETE /v1/processors/{processor}/query-snapshots/{snapshot}
GET  /v1/processors/{processor}/checkpoints
POST /v1/processors/{processor}/checkpoints/{checkpoint}/restore
GET  /v1/processors/{processor}/savepoints
POST /v1/processors/{processor}/savepoints
GET  /v1/processors/{processor}/savepoints/{savepoint}
DELETE /v1/processors/{processor}/savepoints/{savepoint}
GET  /v1/processors/{processor}/consumers
POST /v1/processors/{processor}/consumers
GET  /v1/processors/{processor}/consumers/{consumer}
DELETE /v1/processors/{processor}/consumers/{consumer}
POST /v1/processors/{processor}/consumers/{consumer}/lease
GET  /v1/processors/{processor}/consumers/{consumer}/changes
POST /v1/processors/{processor}/consumers/{consumer}/ack

GET  /v1/processors/{processor}/query/*

GET  /v1/q/blobs/blocks
GET  /v1/q/blobs/blocks/{number}
GET  /v1/q/blobs/snapshot
GET  /v1/q/blobs/transactions
GET  /v1/q/blobs/transactions/{hash}
GET  /v1/q/blobs/schedule
GET  /v1/q/erc20/balances/{token}/{address}
GET  /v1/q/uniswap/pools/{address}
```

Health distinguishes process/store liveness from required live/finality
readiness and processor range coverage. The operational routes, `/health/*`,
`/debug/network`, `/metrics`, and `/v1/network/status`, are not behind the
optional API bearer token so a protected local supervisor can probe them.
When a bearer token is configured, every other route above, reads and writes
alike, needs it, as the `/admin/v1` routes below do: `/v1` names the API
version, not a public tier. Neither health route scans store tables: liveness
runs a trivial SQLite query, and readiness adds in-memory actor state.
`/metrics` and `/v1/network/status` reuse store row and byte counts for up to
10 seconds.
ERC-20 and Uniswap streams share the generic processor cursor contract; their
typed queries and changes encode large quantities as lossless decimal strings.
The `/query/*` subtree is supplied by the selected processor's optional native
query extension. It is always scoped to that exact processor instance. The
short `/v1/q/blobs`, `/v1/q/erc20`, and `/v1/q/uniswap` forms are aliases installed
only when a single configured extension claims the name; canonical paths are
reported by `/v1/capabilities`.

Local administration, intentionally separate from the public SDK:

```text
POST /admin/v1/materialization-jobs
GET  /admin/v1/materialization-jobs
GET  /admin/v1/materialization-jobs/{id}
POST /admin/v1/materialization-jobs/{id}/cancel
DELETE /admin/v1/materialization-jobs/{id}
POST /admin/v1/backfill-subscriptions
GET  /admin/v1/backfill-subscriptions
GET  /admin/v1/backfill-subscriptions/{id}
POST /admin/v1/backfill-subscriptions/{id}/cancel
POST /admin/v1/backfill-subscriptions/{id}/retry
DELETE /admin/v1/backfill-subscriptions/{id}
POST /admin/v1/raw-history-jobs
GET  /admin/v1/raw-history-jobs
GET  /admin/v1/raw-history-jobs/{id}
POST /admin/v1/raw-history-jobs/{id}/cancel
DELETE /admin/v1/raw-history-jobs/{id}
POST /admin/v1/processors/{processor}/lanes/live/reset
DELETE /admin/v1/processors/{processor}/artifacts
POST /admin/v1/processors/{processor}/artifacts/replay
```

There is no route that rebuilds a processor instance in place. A processor
whose coverage contradicts the canonical chain is rebuilt as a replacement
instance; its live-lane reset answers `409 live_lane_requires_rebuild` (see
the operations runbook).

The first administration client can be the Rust CLI over a Unix socket or
localhost-only HTTP. Starting or cancelling a backfill changes durable state
and should not be mixed with public queries.

Processor historical scheduling is independent from RPC history. With
`history_mode = "on_demand"`, the canonical live lane and normal JSON-RPC/head
subscriptions still start immediately, while that processor waits for an
explicit historical job instead of automatically filling from its semantic
`start_block`.

Create an idempotent node-owned range-to-SQLite job:

```http
POST /admin/v1/materialization-jobs
content-type: application/json

{
  "processor": "blobs-production",
  "fromBlock": 25600000,
  "toBlock": 25610000,
  "mode": "fill_missing",
  "idempotencyKey": "local-analysis-25600000-25610000-v1"
}
```

`history_control = "node_owned"` plus `history_mode = "on_demand"` permits
these jobs. They process missing finalized coverage, retain output according to
the processor's SQLite output policy, and require no consumer or acknowledgement.
The request range is not artificially capped; bounded source chunks and runtime
budgets keep memory independent from total range length.

Application database repair instead uses `POST
/admin/v1/backfill-subscriptions` on an
`application_subscriptions + on_demand` processor. That operation creates its
required consumer and independent history delivery stream atomically. Its
`fill_missing` and `recompute` publication semantics are consumer-facing and do
not grant the application scheduling authority over a node-owned processor.
All control routes share protected API authorization and do not alter JSON-RPC
or WebSocket availability.

A `fill_missing` subscription owns the creation-time gaps in its requested
ranges. If another overlapping job covers one of those blocks first, the
subscription still republishes that verified block into its independent stream;
shared processor coverage cannot silently satisfy another subscription's
delivery obligation. Completed, failed, and cancelled jobs are terminal and are
never retried implicitly; re-submitting a request with the same idempotency key
returns the terminal job's status.

A failed subscription is retried explicitly with `POST
/admin/v1/backfill-subscriptions/{id}/retry`, for example after a transient
outage or after upgrading past the fault that failed it. It resumes from its
committed progress with the same history stream, consumer, credential, and
acknowledgement, and its error is cleared. Retrying a subscription that is still
active returns its status unchanged; a completed or cancelled subscription gets
`409 backfill_conflict`. A failed subscription whose processor instance is no
longer configured gets `400 invalid_request` and stays as it was. A failed
materialization job is not retried: delete it and create it again, which is
what `leani backfill` does with a fresh idempotency key.

A failed or cancelled subscription's stream still delivers every record its job
committed, then holds its session, with heartbeats, until the consumer has
acknowledged what it was delivered, and only then ends with `backfill_failed`
or `backfill_cancelled`. Deleting such a subscription is refused with `409
backfill_conflict` while its required consumer has unacknowledged records: read
the stream to that end and acknowledge, or delete with
`?discardUnacknowledged=true` when the application does not need them. The
parameter never applies to a subscription that is still running. Deleting a job
right after cancelling it waits up to 10 seconds for its task to stop, and
answers `503 backfill_unavailable`, which is retryable, if it has not; a retry
waits the same way for a failed job's task.

A subscription whose job has finished is `draining` until its required
consumer acknowledges the `backfill_complete` record, which makes it
`complete_reclaimable`; its stream ends once that acknowledgement lands. A
stream opened after that sends the same `backfill_complete` record again, with
the same `cursor`, and ends at once instead of sending heartbeats
indefinitely. Acknowledging that cursor again with the stream's session token
succeeds even after the stream has ended, until another stream replaces that
session. Should the record no longer be readable, the stream ends with
`backfill_completed` instead.

A draining subscription can be cancelled as well, for example when its
application will never read it to the end. It becomes `cancelled` and keeps
its job's report. Its stream still serves the records committed before the
cancellation, `backfill_complete` among them, but acknowledging them leaves the
subscription `cancelled`, and a stream opened after that ends with
`backfill_cancelled`. Delete it once they are acknowledged, or with
`?discardUnacknowledged=true` to abandon them. Cancelling it and deleting it
with `?discardUnacknowledged=true` work even after its processor instance was
removed from the configuration, when its stream and acknowledgement routes
refuse (normally `404 not_found`).

A subscription's status names its required consumer in `consumer`, so an
application finds its own subscriptions in `GET
/admin/v1/backfill-subscriptions` by that field instead of parsing the opaque
`id`; a materialization job's `consumer` is `null`. `processorConfigured` says
whether the node configures the processor instance named in `processor` now:
exactly that instance, not another instance of the same kind. While it is
`false`, the stream and acknowledgement routes refuse the subscription. That
can be temporary, as when a restart's configuration leaves the instance out by
mistake. If the instance will not return, cancel the subscription and delete
it with `?discardUnacknowledged=true`, which discards its unacknowledged
records; [Processor rebuild or rollback](/docs/operations/runbook/#processor-rebuild-or-rollback)
gives the steps around replacing an instance.

## 3. Common response model

Every processor query includes coverage:

```ts
export interface ProcessorCoverage {
  chainId: number;
  chainFinalizedHead: {
    number: number;
    hash: `0x${string}`;
  } | null;
  requested?: {
    fromBlock: number;
    toBlock: number;
  } | null;
  available: Array<{
    fromBlock: number;
    toBlock: number;
    finality: "preview" | "included" | "finalized";
  }>;
  configuredStartBlock: number;
  processedThrough: number | null;
  finalizedThrough: number | null;
  complete: boolean;
  state:
    | "starting"
    | "backfilling"
    | "catching_up"
    | "live"
    | "degraded"
    | "paused"
    | "failed";
}
```

`paused` and `failed` report the processor's live lane; `/v1/network/status`
gives its `pauseReason` and first unapplied block (`liveGap`). Otherwise
`starting` means nothing is applied yet and `backfilling` that the processor's
range has gaps. `catching_up` means it has none but the processor has not
reached the chain's finalized head (`chainFinalizedHead`), or the node is not
ready yet. Only then is it `live`.

Every `available` interval is inclusive and has one exact finality for every
block it contains. Intervals split at both coverage gaps and finality
boundaries, so consumers selecting finalized material must filter these ranges
rather than treating `finalizedThrough` as a contiguous watermark.
`finalizedThrough` is only the highest covered block recorded as finalized;
lower gaps or non-finalized coverage can still exist.

For a block-local processor, `available` can contain several intervals while a
backfill and head follower operate concurrently. `processedThrough` is the
last block of the highest interval, even when earlier ones leave gaps;
`complete` says whether the requested range, or the range from the configured
start to the processor's cursor, has none.

All list responses use opaque pagination cursors:

```ts
export interface Page<T> {
  data: T[];
  nextCursor: string | null;
  coverage: ProcessorCoverage;
}
```

Clients must not parse a cursor or construct one from a block number.

Materialized-view consumers bootstrap without racing the live stream through
the generic query-and-follow operation:

1. `POST
   /v1/processors/{processor}/collections/{collection}/query-and-follow`;
2. import every page, reading the next one with `GET .../entities?cursor=`
   and the page's opaque `nextCursor`; and
3. follow `/v1/processors/{processor}/changes` or `/stream` after the returned
   `boundaryCursor`.

Snapshot rows and the change boundary are captured in one SQLite transaction.
Concurrent commits therefore fall strictly after the boundary. Pages remain
stable for the advertised TTL even while new blocks commit. The response
reports actual `rowCount`/`valueBytes`, retained bounds, coverage, and an
explicit recovery action.

The typed `/v1/q/blobs/snapshot` route remains a built-in convenience wrapper.

### Durable consumer registration

Required or best-effort consumers have a server-side cumulative delivered and
acknowledged watermark. A `required` consumer also holds back pruning until it
acknowledges, so only a stream that retains until acknowledged accepts one: a
live stream with `until_acknowledged` delivery, or a backfill stream. A
`window` or `best_effort` stream prunes by its own limits and rejects a new
`required` consumer with `400`. Re-creating a consumer that already exists
returns `409 consumer_exists` whatever its role. Creation always names a
start position:

```http
POST /v1/processors/usdc-weth-latest/consumers
content-type: application/json

{
  "id": "prices-api",
  "role": "best_effort",
  "start": { "position": "earliest_retained" },
  "leaseTtlSeconds": 300,
  "credential": "<random per-consumer secret>"
}
```

`position` is `earliest_retained`, `current_head`, or `cursor` with a `cursor`
field. An old, wrong-store, wrong-instance, or incompatible cursor returns
`reset_required`; the node never substitutes another position.
`leaseTtlSeconds` is 1 to 3,153,600,000 (100 years), for a backfill
subscription's consumer too; another value gets `400`.

A consumer created with a credential needs it on every consumer-scoped call,
in addition to any bearer token: the lease, change, and acknowledgement
routes below, and its live and backfill streams with their lease and
acknowledgement routes:

```http
x-leani-consumer-credential: <random per-consumer secret>
```

A missing or wrong credential gets `403`, whatever the bearer token. The
node compares credentials in constant time and stores each as a BLAKE3 hash
keyed with a random per-store secret. A backfill subscription's credential
also enters the subscription's idempotency identity, an unkeyed BLAKE3 hash
of the whole request that the store keeps, so use a random credential. A new
credential holds 32 to 512 printable ASCII characters without spaces, such
as the output of `openssl rand -hex 32`. A consumer or backfill subscription
created before that rule keeps its credential: re-creating the consumer
still gets `409 consumer_exists`, and re-submitting the subscription's
request unchanged its status. A consumer declared in the node configuration
has no credential and takes none.

Credentials isolate applications on these consumer-scoped delivery routes
only. Listing consumers, inspecting one with
`GET .../consumers/{consumer}`, and revoking one with `DELETE` on that path
are administrative operations that only the bearer token protects, whether
or not the consumer has a credential. The list already shows what an
inspection does, an operator must be able to revoke an abandoned consumer
whose credential is lost, and without a bearer token every client that
reaches the API is trusted with the administrative routes, such as
live-lane resets, anyway.

Read a batch:

```http
GET /v1/processors/usdc-weth-latest/consumers/prices-api/changes?limit=500
x-leani-consumer-credential: ...
```

After committing the destination transaction and its cursor atomically:

```http
POST /v1/processors/usdc-weth-latest/consumers/prices-api/ack
x-leani-consumer-credential: ...
content-type: application/json

{ "cursor": "<highest durably committed cursor>" }
```

Acknowledgement is monotonic and idempotent: repeating the acknowledged
cursor succeeds and moves no progress. The session token of a stream that has
ended may still repeat it, without renewing the lease, until another stream
replaces that session. A page read or a stream batch records the last
sequence it hands out as the consumer's delivered sequence
(`deliveredSequence`), and a cursor beyond it gets `409
acknowledgement_beyond_delivered`, so a buggy client cannot let retention
prune changes it never received. A live stream batch holds one block's whole
commit, and a live acknowledgement must end one, as each batch's
`acknowledgeableCursor` does (`409 acknowledgement_mid_block` otherwise). A
backfill acknowledgement must be a batch's `acknowledgeableCursor` or the
completion's `cursor`.

A processor with a split live stream (`block_versioned_idempotent` delivery
ordering, such as `blobs-money`) fences renewal and acknowledgement by
streaming session. Its consumers renew and acknowledge through
`/streams/live/consumers/{consumer}/lease` and `/ack` with the
`x-leani-consumer-session` token from the stream's `hello`, and the generic
lease and acknowledgement routes answer `409 consumer_session_required`. Once
another stream replaces a session, its token gets `409 consumer_session_lost`;
a session that has only ended may still repeat its acknowledged cursor. A
session token carries a MAC keyed with the per-store secret; one the node
cannot verify, such as a token from before an upgrade, gets `409
consumer_session_lost`, and the client opens a new stream. The live stream's
`hello` also carries the processor's `coverage` as the session starts, so an
application can diff its own copy on every connection without polling the
status route.

`commit_then_ack` in the Rust API helper does not
construct or execute the acknowledgement future until the destination commit
succeeds. The TypeScript SDK exposes the equivalent
`commitThenAcknowledge(() => destinationCommit(), cursor =>
client.processors.consumers.acknowledge(...))`; its acknowledgement callback
is likewise never invoked after a failed destination commit.

### Generic retained queries

Entity queries accept `fromBlock`, `toBlock`, `fromTimestamp`, `toTimestamp`,
and `limit`. Entity keys are URL-safe hex. Every row contains the processor
schema, decoded JSON data, and block/timestamp/finality metadata. There are
two ways to page through a collection:

- `GET .../entities` with the query as URL parameters reads current retained
  output in entity-key order and creates nothing. Its page holds `data`,
  `nextCursor`, `retainedBounds`, and `coverage`; pass `nextCursor` as
  `cursor` to read on. Later pages see later commits: each row appears as it
  was when its page was read, and a row added behind the read position is
  not returned.
- `POST .../entities` with the query as a JSON body copies the matching rows
  into a stable snapshot that counts against the node-wide snapshot limits,
  and returns its first page with the snapshot ID, change boundary, row and
  byte counts, and expiry. `GET .../entities?cursor=` with its `nextCursor`
  reads the following pages, which stay stable while new blocks commit.

```http
POST /v1/processors/{processor}/collections/{collection}/entities
content-type: application/json

{ "fromBlock": 17000000, "toBlock": 17000999, "limit": 500 }
```

Explicit block bounds require complete processor coverage. Missing blocks return
HTTP 409 `range_incomplete`, including the requested interval and coverage.
The resolved block range is kept with the cursor, or the snapshot, across
pagination and node restart, and each GET page checks coverage again. A
block-count window uses its retention policy boundary, rather than
the first matching entity, so covered blocks without events remain queryable.
`retainedBounds` describes the entities present, not the limits of processor
coverage. Timestamp filters select retained rows; include block bounds when
you need a completeness check for a specific interval.

`output = "none"` or a range outside the retained window returns
`output_not_retained` with a rebuild/source-scan action. It never returns a
successful empty page for unavailable local history.

The TypeScript SDK's
`client.processors.queryAndFollow(processor, collection, query)` returns this
snapshot and its `boundaryCursor` in one operation. Continue with
`client.processors.subscribe(processor, { after: page.boundaryCursor })`.

With delivery disabled, ordinary entity queries still work, and snapshots
return `boundaryCursor: null` and `recovery.follow: null`. Query-and-follow, change-head,
change-page, and stream requests return non-retryable HTTP 409
`delivery_disabled`; enable a delivery policy before subscribing.

A `finalized_only` processor's snapshots also return `boundaryCursor: null`
and `recovery.follow: null`, and query-and-follow returns HTTP 409
`finalized_only_snapshot`: its retained state already holds included blocks
whose changes are published only once finalized, and the stream never undoes
a block it never published, so a copy followed from a boundary could keep
rows of a reorged block.

Release a no-longer-needed snapshot early:

```http
DELETE /v1/processors/{processor}/query-snapshots/{snapshotId}
x-leani-request: 1
```

### Compact processor artifacts

When `[processors.artifacts]` is `window` or `full`, the node retains the
finalized checksummed map result independently from queryable entities and
delivery. Inspect its bounds and logical footprint with:

```http
GET /v1/processors/{processor}/artifacts/status
GET /v1/processors/{processor}/artifacts/{blockNumber}
```

Portable export is paged by block range:

```http
GET /v1/processors/{processor}/artifacts/export?fromBlock=1&toBlock=1000&limit=1000
```

The response uses
`application/vnd.leani.processor-artifacts`. Its versioned durable
envelope and `x-leani-artifact-digest` cover the immutable map identity,
delta schema, canonical ordering, payload checksums, and requested/exported
ranges. `x-leani-artifact-complete` is false when another page is needed or
the retained range has a gap.

Replay reduces artifacts into a distinct configured processor instance with
the same immutable map/delta contract:

```http
POST /admin/v1/processors/{source}/artifacts/replay
content-type: application/json

{
  "targetProcessor": "analysis-view-v1",
  "fromBlock": 1,
  "toBlock": 1000,
  "limit": 1000
}
```

Artifact deletion is explicit and owner-scoped. Releasing one owner preserves
rows protected by another processor job or operator pin:

```http
DELETE /admin/v1/processors/{processor}/artifacts?fromBlock=1&toBlock=1000
```

The default releases the processor-instance owner. `ownerKind` and `ownerId`
select `processor_job` or `operator_pin` ownership. Delivery acknowledgement,
output pruning, raw-history deletion, and checkpoint pruning never delete
artifacts.

`[budgets.artifacts]` caps retained finalized bytes and pending included-block
candidates across all processors. Admission is atomic: exceeding either limit
rolls back artifacts, processor mutations, coverage, and cursor advancement.
The metrics endpoint reports current and maximum artifact bytes separately;
window pruning, explicit owner release, or a larger configured budget lets
storage-backpressured work resume.

### Checkpoints and portable savepoints

Automatic recovery checkpoints are read-only through
`GET /v1/processors/{processor}/checkpoints`, are taken at most once per
1,000 finalized blocks and when a historical job completes, and obey
configured count retention. `POST .../checkpoints/{checkpoint}/restore`
atomically repairs private processor state only when the processor's cursor
sits exactly on the checkpoint; finality reaching that block afterwards does
not matter. Between checkpoints no checkpoint can be restored: restore a
full-store backup or rebuild the processor. Older checkpoints are rejected
because rewinding state without also rewinding independent output, delivery,
coverage, and undo storage would be unsafe; use a deterministic rebuild or a
full-store backup for that case. Portable savepoints are operator-created
exports, which the node cannot restore:

```http
POST /v1/processors/{processor}/savepoints
content-type: application/json

{ "id": "before-v2-upgrade" }
```

Each processor instance keeps at most 16 savepoints; creating another returns
`409 savepoint_limit` until one is deleted. A savepoint is also admitted
against the physical store budget and gets `507 physical_storage_limit` when
it does not fit. `GET .../savepoints/{id}` exports a
checksummed, versioned `application/vnd.leani.savepoint` archive. Deletion is
explicit with `DELETE`; acknowledgement or checkpoint pruning never deletes a
portable savepoint. Checkpoints and savepoints are bound to the processor
contract without its lifecycle policies, so raising a limit does not
invalidate them; a different processor version does.

## 4. Change protocol

### 4.1 Envelope

```ts
export interface ChangeEnvelope<T = unknown> {
  apiVersion: "1";
  sequence: string;
  cursor: string;
  operation: "apply" | "undo" | "finalized" | "reset_required";
  originKind: "live" | "live_recovery" | "historical_backfill" | "recompute";
  originId: string;
  publicationRevision: string;
  chainId: number;
  block: {
    number: number;
    hash: `0x${string}`;
    parentHash: `0x${string}`;
    timestamp: number;
  } | null;
  finality: "preview" | "included" | "finalized";
  kind: string;
  schema: string;
  key: string | null;
  /** When the payload carries its own finality field, it equals the envelope finality. */
  data: T | null;
  /** Present only on reset_required, as the current coverage hint. */
  coverage?: ProcessorCoverage;
  emittedAt: string;
}
```

Envelopes carry change-scoped facts, including the processing lane's origin and
publication revision. `originKind: "live_recovery"` marks a finalized block
that a live lane applied late, from retained frames or finalized history, as in
a gap replay or a live gap fill (section 4.2). Connection-scoped processor
identity and node coverage are served by the stream's `hello` frame (section
4.3) and by `/v1/status`; poll responses continue to carry one top-level
`coverage` per page.

`sequence` is a decimal string because a long-running stream can exceed
JavaScript's safe integer range. It is monotonically increasing in one store
epoch, including across reorgs.

`cursor` is an opaque base64url token that includes at least:

```text
cursor format version
store epoch
chain ID
processor instance
change sequence
block number/hash when applicable
checksum
```

The checksum detects accidental corruption, not malicious tampering. A cursor
from another store, chain, processor version, or expired epoch is rejected.

### 4.2 Operations

`apply`

- introduces or updates the keyed domain value;
- is emitted only after the state transaction commits;
- may be included but not yet finalized.

`undo`

- reverses a previously committed `apply`;
- contains the inverse value/change required by downstream stores;
- has its own newer sequence/cursor.

`finalized`

- advances finality for a processor/chain, as kind `system.finality.put`
  with `data: { throughBlock }`;
- need not repeat every entity;
- permits a consumer to mark earlier changes durable.

A `finalized` change of kind `system.live_gap.put` instead announces blocks
the live lane skipped and did not fill, which the processor never processed,
as after a restart at a later finalized anchor: `data: { fromBlock, toBlock,
reason: "not_filled" }`. Only block-local processors with `history_mode =
"on_demand"` announce them, before the block that follows the range, live or
replayed, and in a batch of their own. After a crash between the two, the
next block past the cursor announces again, the same range or a longer one;
a retry the stream's last notice already covers adds none. A processor that
fills live gaps (below) may instead fill the range then, after a crash or a
restart of its live lane, so `live_recovery` batches can follow a notice for
the same blocks.
Deduplicate batches by cursor: the notice and the following block share a
block number.
A processor with
[`[processors.live_gap_fill]`](/docs/operations/configuration-guide/#live-gap-fill)
first fills a range of at most `max_blocks` blocks from history. The live
stream delivers those blocks before the following block, one batch each, in
block order, as `apply` changes with `originKind: "live_recovery"` and
`finality: "finalized"`, and announces nothing for them. It announces a longer
range, a range history cannot serve yet, and whatever a fill leaves when it
fails or runs out of time, from the first block it did not fill.
Otherwise Leani does not fill the range: request it as history, such as with a
`fill_missing` backfill subscription (see
[Keep your application in sync](/docs/guides/keep-your-application-in-sync/)).

`reset_required`

- means the requested cursor is no longer resumable or belongs to an
  incompatible store/processor epoch;
- is the only operation whose envelope carries `coverage`, as the current
  coverage/snapshot hint;
- causes the stream to close after the event.

The protocol is at-least-once across network reconnects. Consumers deduplicate
by cursor/sequence and apply changes transactionally. It is exactly-once in the
node's own state, not magically exactly-once across an HTTP connection.

### 4.3 Poll and stream

Polling:

```http
GET /v1/processors/blobs-money/changes?after=<cursor>&limit=500
```

Streaming:

```http
GET /v1/processors/blobs-money/stream?after=<cursor>
Accept: text/event-stream
```

Every change includes the processor descriptor's `schema`. Built-in and custom
native processors may render typed JSON in `data`; processors without a public
renderer expose schema-labelled `{ "encoding": "hex", "value": "..." }`.
Consumers inspect `finality` on each event rather than filtering it through a
subscription query parameter.

Every stream connection — initial and reconnect — opens with one `hello`
frame before any change events:

```text
event: hello
data: {"apiVersion":"1","chainId":1,"processor":{...},"coverage":{...}}

```

The hello is synthesized at accept time from live state; it is never stored,
carries **no `id:` line** (it has no position in the change log, so it can
never become a resume cursor), and is never acknowledged. The SDK surfaces
it through the optional `onHello` subscribe callback.

Change events follow as before:

```text
id: <opaque cursor>
event: apply
data: {"apiVersion":"1",...}

```

A browser `EventSource` reconnects on its own and sends the last event's
`id:`, its cursor, as `Last-Event-ID`; without `after`, the stream resumes
after that cursor. An explicit `after` takes precedence.

The server sends comments as heartbeats. When the node shuts down, it ends the
stream after a whole event, with no error event, and the consumer delivery
streams after a whole record. An `error` event carries the error body below
and ends the stream; its `retryable` says whether reconnecting can help. A
stored change the node cannot render is not retryable. The SDK reconnects
with the last fully yielded cursor, uses exponential backoff with jitter, and
treats these differently:

- clean server close, such as the node's shutdown: reconnect;
- transient network/5xx: reconnect;
- no bytes, keep-alive comments included, for `idleTimeoutMs`: reconnect;
- `error` event with `retryable: true`: reconnect, and after five in a row
  without a new change, surface the last one;
- `error` event with `retryable: false`: surface error;
- authentication/validation 4xx, or a redirect: surface error;
- `reset_required`: stop and require a snapshot/resubscription decision;
- abort signal: stop without error.

Slow consumers are disconnected before they exhaust server memory. Durable
resume comes from the change log, not an unbounded per-connection buffer.

## 5. Errors

HTTP errors use a stable machine-readable body:

```ts
export interface LeaniErrorBody {
  error: {
    code:
      | "invalid_request"
      | "unsupported_capability"
      | "range_incomplete"
      | "source_unavailable"
      | "query_too_expensive"
      | "cursor_invalid"
      | "cursor_expired"
      | "processor_rebuilding"
      | "finality_unavailable"
      | "internal";
    message: string;
    retryable: boolean;
    details?: Record<string, unknown>;
    requestId: string;
  };
}
```

Relevant status mappings:

| Status | Meaning |
|---|---|
| `400` | malformed parameters or invalid cursor |
| `401` / `403` | authentication/authorization |
| `404` | entity or processor does not exist |
| `409` | requested processor version/coverage is rebuilding or incomplete |
| `410` | stream cursor expired; snapshot reset required |
| `413` | requested range/result exceeds configured limit |
| `422` | source exists but cannot provide the required capability |
| `429` | concurrency/query budget exceeded |
| `503` | source/finality temporarily unavailable |
| `507` | the physical store budget refuses the write (`physical_storage_limit`) |

Standard JSON-RPC uses standard errors where applicable and a documented custom
server-error range for unsupported capability, incomplete coverage, and
resource limits. Unsupported state methods must not return `null` or zero.

## 6. Capability discovery

`GET /v1/capabilities` reports actual configured capability, not everything the
software could theoretically implement:

```json
{
  "chainId": 1,
  "ethereumRpc": {
    "methods": {
      "eth_call": {
        "supported": false,
        "reason": "evm_state_unavailable"
      }
    }
  },
  "processors": [
    {
      "id": "blobs-money",
      "instance": "blobs-production",
      "version": "1.5.0",
      "codeHash": "0x...",
      "configHash": "0x...",
      "genericApi": "processor-v1",
      "changeSchema": "blobs-money.change.v1",
      "queryExtensions": [
        {
          "id": "blobs-v1",
          "basePath": "/v1/processors/blobs-production/query",
          "aliasPath": "/v1/q/blobs"
        }
      ],
      "subscriptions": true,
      "artifactRetention": "none",
      "deliveryOrdering": "block_versioned_idempotent"
    }
  ],
  "queryExtensions": [
    {
      "processor": "blobs-production",
      "id": "blobs-v1",
      "basePath": "/v1/processors/blobs-production/query",
      "aliasPath": "/v1/q/blobs"
    }
  ]
}
```

`genericApi` identifies the common status/query/change contract and
`changeSchema` identifies stream payloads. `queryExtensions` separately
describes optional typed read wrappers. Applications should persist or select
the processor instance and use `basePath`; aliases are conveniences and may be
absent when several instances use the same extension.

`ethereumRpc` names only `eth_call`, which the node never serves. The
JSON-RPC method `leani_getCapabilities` reports the support and coverage of
each RPC method on the RPC listener itself, so an Ethereum-library integration
can inspect the endpoint without knowing the aggregate API port.

## 7. Blobs API

### 7.1 Types

The native API should expose chain-derived values using lossless strings for
wei and other 256-bit quantities:

```ts
export interface BlobsBlock {
  network: string;
  blockNumber: number;
  blockHash: `0x${string}`;
  parentHash: `0x${string}`;
  timestamp: number;
  size: string | null;
  blobCount: number;
  blobGasUsed: string;
  excessBlobGas: string;
  blobBaseFee: string;
  executionBaseFee: string | null;
  gasUsed: string | null;
  gasLimit: string | null;
  executionEthBurnedWei: string | null;
  blobEthBurnedWei: string;
  reserveFeeWei: string | null;
  transactionCount: number;
  targetBlobsPerBlock: number;
  maxBlobsPerBlock: number;
  transformVersion: number;
  finality: "preview" | "included" | "finalized";
}

export interface BlobTransaction {
  network: string;
  blockNumber: number;
  blockHash: `0x${string}`;
  txHash: `0x${string}`;
  senderAddress: `0x${string}`;
  blobVersionedHashes: `0x${string}`[];
  blobCount: number;
  totalBurnedWei: string;
  executionBurnedWei: string;
  blobBurnedWei: string;
}

export interface BlobSchedule {
  name: string;
  activationBlock: number;
  activationTimestamp: number;
  forkId: `0x${string}`;
  targetBlobsPerBlock: number;
  maxBlobsPerBlock: number;
  baseFeeUpdateFraction: string;
  eip7918: boolean;
  source: "chain_spec" | "eth_config" | "configured";
}
```

`blobBaseFee` is the protocol blob base fee in wei per blob gas: the EIP-4844
fee for the block's excess blob gas under the fork active at its timestamp.
`blobEthBurnedWei` and each transaction's `blobBurnedWei` multiply it by the
blob gas used. `reserveFeeWei` is the EIP-7918 reserve price from Fusaka on
(`null` before); it only changes how excess blob gas evolves and is never
applied to `blobBaseFee`. A schedule's parameters apply from its
`activationTimestamp`; `activationBlock` is the block-number label `atBlock`
lookups use, and for Dencun it is 19,426,589, the first block the processor
covers.

The API should not use decimal floating point for ETH values. Presentation as
ETH belongs in blobs.money.

### 7.2 Query examples

```http
GET /v1/q/blobs/blocks/21000000
GET /v1/q/blobs/blocks?fromBlock=21000000&toBlock=21001000&limit=500
GET /v1/q/blobs/transactions?blockNumber=21000000
GET /v1/q/blobs/transactions/0x...
GET /v1/q/blobs/schedule?atBlock=...
```

Range bounds are inclusive. Results sort by block number and, for transactions,
canonical transaction index. Pagination cursors preserve this order.

Querying a block-local range with a gap returns:

- `409 range_incomplete` by default;
- partial data plus explicit coverage only when `allowPartial=true`.

The default prevents a dashboard from silently treating a backfill gap as a
zero-blob period.

### 7.3 Change kinds

The blobs stream emits:

```text
blobs.block.put
blobs.block.delete
blobs.schedule.put
```

One Ethereum block produces one logical `blobs.block.put` containing its blob
transactions, so a consumer can update its database atomically. Blocks and
transactions remain independently queryable through their retained entity
collections when processor output retention is enabled.

Example domain payload:

```ts
export interface BlobsBlockChange {
  block: BlobsBlock;
  transactions: BlobTransaction[];
}
```

On a reorg, `undo` contains the previous block plus transactions so a consumer
can delete or restore them by canonical keys. The replacement branch then
arrives as ordinary `apply` changes. A reorg of several blocks, like the revert
at a restart after a long outage, undoes them newest first, and the live
stream delivers each reverted block's `undo` as a batch of its own.
[Undo runs](/docs/concepts/coverage-finality-and-reorgs/#undo-runs) shows how
to rebuild derived aggregates once per such run instead of once per block.

## 8. TypeScript SDK

Package:

```text
@smart-byte/leani-sdk
```

The SDK release workflow builds the JavaScript and type declarations and runs
packed-package smoke tests in Node and Bun before publication. Prerelease
versions use npm's `latest` tag until the first stable release, and `next`
after it.

Runtime support:

- Bun, first-class and tested in the blobs.money repository;
- current Node LTS with native `fetch`;
- modern browsers for public read-only endpoints;
- no Node-only APIs in the core client.

Public shape:

```ts
import { createLeaniClient } from "@smart-byte/leani-sdk";

const node = createLeaniClient({
  baseUrl: "http://127.0.0.1:8080",
  token: process.env.LEANI_TOKEN,
  timeoutMs: 15_000,
});

const status = await node.status();
const capabilities = await node.capabilities();

const page = await node.blobs.listBlocks({
  fromBlock: 21_000_000,
  toBlock: 21_001_000,
});

for await (const event of node.blobs.subscribe({
  after: savedCursor,
  signal: abortController.signal,
})) {
  await database.transaction(async (tx) => {
    await applyBlobChange(tx, event);
    await saveCursor(tx, event.cursor);
  });
}
```

The change stream currently publishes every retained finality transition.
Consumers inspect or filter `event.finality`; `SubscribeOptions` has no
finality filter, only `after`, `signal`, `onHello`, and `idleTimeoutMs`.

Core client modules:

```ts
client.status()
client.capabilities()
client.request<T>(path, params, options)
client.processors.list()
client.processors.status(id)
client.processors.changeHead(id)
client.processors.changes(id, options)
client.processors.subscribe<T>(id, options)
client.blobs.getBlock(number)
client.blobs.listBlocks(query)
client.blobs.listSnapshot(query)
client.blobs.getTransaction(hash)
client.blobs.listTransactions(query)
client.blobs.getSchedule(query)
client.blobs.subscribe(options)
```

Typed helpers default to the `/v1/q/*` aliases. In a multi-instance setup,
select the desired canonical `basePath` from `client.capabilities()` and pass
it through `queryBasePaths` when constructing the client. This keeps typed
responses while making the processor instance explicit.

`request<T>` is the safe primitive for custom query extensions. Every path
resolves under the base URL's path: `v1/processors/<instance>/query/summary`
and a capabilities `basePath` such as `/v1/q/my-protocol/summary` both keep a
prefixed base URL's prefix. Schemes, protocol-relative paths, segments that
decode to `.` or `..`, backslashes, encoded slashes, backslashes, and percent
signs, and control characters are rejected before the bearer token is
attached, and redirects are refused rather than followed. A custom extension
whose path parameter can hold an encoded `/` or `%` must take that value in
the query string, through `request`'s `params`, instead.
Built-in query namespaces use this same primitive.

SDK responsibilities:

- URL and query encoding;
- abortable deadlines;
- typed error mapping;
- JSON validation at trust boundaries where practical;
- SSE parsing across arbitrary byte chunk boundaries;
- heartbeat handling;
- reconnect/backoff/jitter;
- cursor persistence hooks;
- duplicate cursor suppression;
- exposure of coverage on every query/stream event.

SDK non-responsibilities:

- automatically discarding `undo`;
- silently resetting an expired cursor;
- storing application checkpoints;
- converting wei to floating point;
- wrapping the complete Ethereum JSON-RPC API;
- hiding incomplete coverage.

The SDK exports pure helpers:

```ts
import {
  applyEntityChange,
  compareSequences,
  createInMemoryCursorStore, // Tests and development only.
  isResetRequired,
} from "@smart-byte/leani-sdk";
```

Application database integration remains explicit because domain changes must
commit before the application advances Leani's durable acknowledgement. A
crash between those steps can replay a change, so destination writes must be
idempotent; Leani remains the resume authority.

## 9. OpenAPI and versioning

`api/openapi.yaml` is normative for HTTP queries and error bodies. The workflow:

1. update the OpenAPI document;
2. validate it in CI;
3. generate TypeScript wire types;
4. compile the Rust route response types against schema snapshots/tests;
5. handwrite the ergonomic SDK around generated wire types;
6. run SDK contract tests against the real Rust server.

Do not generate the whole SDK as a thin collection of awkward endpoint
functions. Generated wire types prevent drift; the handwritten client owns
streaming, retries, errors, and domain ergonomics.

Compatibility rules within `/v1`:

- fields are added only as optional unless a new media/API version is created;
- enum expansion must be handled as an unknown value at SDK trust boundaries;
- field meaning cannot change;
- processor output schemas have their own semantic version;
- cursors are always opaque and may change representation;
- a processor rebuild that changes semantics receives a new instance/version.

## 10. Authentication and limits

Localhost is the default trust boundary. The current optional API bearer token
protects all native query, stream, consumer, and `/admin/v1` routes on one
listener, and a bind beyond loopback requires it unless
`api.allow_unauthenticated_remote` is set. A consumer's credential, when it
has one, is required as well on that consumer's delivery routes, its lease,
change, and acknowledgement routes and its streams, and fences that
consumer's lease and cursor; it does not replace the listener bearer when one
is configured. Listing, inspecting, and revoking consumers need only the
bearer. `/health/live`, `/health/ready`, `/metrics`, `/v1/network/status`, and
`/debug/network` remain unauthenticated operational routes. The JSON-RPC
listeners have no built-in authentication.

Every route, operational ones included, applies these checks, in this order,
to keep web pages from reading or changing a loopback node:

| Request | Answer |
|---|---|
| `Host` name other than `localhost` or one in `api.allowed_hosts`; IP addresses pass | 421 `host_not_allowed` |
| browser `Origin` other than a loopback one or one in `api.allowed_origins` | 403 `origin_not_allowed` |
| GET or HEAD without an `Origin`, with `Sec-Fetch-Site: cross-site` or `same-site` and a `Sec-Fetch-Mode` other than `navigate`, such as another page's image load | 403 `cross_site_request` |
| request to a consumer's live or backfill `stream` route without an allowed `Origin`, with `Sec-Fetch-Site: cross-site` or `same-site` in any mode, navigations included | 403 `cross_site_request` |
| POST or DELETE without `content-type: application/json` or `x-leani-request: 1` | 415 `unsupported_media_type` |

The SDK, curl, and other non-browser clients send no `Origin` or
`Sec-Fetch-*` headers, and their `Host` is the name or address in the URL
they use, so their requests pass the first four checks whenever that is an IP
address, `localhost`, or a name in `api.allowed_hosts`.
So do navigations to other routes, including from another site's page or
into a frame, and requests from browsers that send no Fetch Metadata. A page
cannot read those responses, and they create no query snapshot. Opening a
consumer's stream takes its session lease, so the stream routes refuse
navigations from other sites too; a browser without Fetch Metadata can still
open one, which the bearer token and the consumer's credential prevent. The
SDK sends `x-leani-request: 1` on its requests without a JSON body; other
clients must add it, for example `curl -X DELETE -H 'x-leani-request: 1' ...`.

When remotely exposed:

- use TLS, authentication, and request/connection limits at a reverse proxy;
- restrict operational routes to the monitoring network;
- keep the API and RPC listeners off the public Internet unless an edge policy
  explicitly protects them;
- use the built-in range, response-byte, concurrent-scan, stream, and storage
  limits as resource bounds, not as per-tenant authorization;
- do not assume separate admin credentials or listeners until that isolation
  is implemented.

The SDK never logs tokens or complete URLs containing secrets.

## 11. blobs.money integration

blobs.money now carries the native consumer and application-owned
reconciliation described below. The remaining work is production validation,
shared-SDK adoption, cutover, and rollback evidence.

### Path A: JSON-RPC compatibility

Point the existing ethers/fetch clients at the node and support:

```text
eth_blockNumber
eth_getBlockByNumber(number, true)
eth_getBlockReceipts
eth_getTransactionReceipt
eth_subscribe(newHeads)
eth_config, from the node's checked/versioned chain schedule
```

This minimizes initial application changes and proves RPC parity. It still
causes blobs.money to fetch and transform raw blocks, so it is a bridge rather
than the final design.

### Path B: native SDK

Add a small ingestion adapter in blobs.money:

```text
SDK blobs stream
  -> one database transaction
       upsert/delete blocks
       upsert/delete blob_transactions
  -> acknowledge the Leani-owned durable cursor
  -> existing application analytics
```

The implemented integration derives required historical ranges from
PostgreSQL gaps, creates bounded backfill subscriptions, consumes atomic
history batches and live changes, commits application rows, then acknowledges
the Leani-owned durable cursor. A crash between those steps causes an
idempotent replay rather than data loss; PostgreSQL does not maintain a second
resume checkpoint. `full_blocks` remains a legacy/fallback concern rather than
a required native-consumer input.

The node owns:

- Ethereum input acquisition;
- fork/reorg/finality handling;
- blob schedule and fee formulas;
- chain-derived block and blob-transaction projections.

blobs.money continues to own:

- rollup address attribution unless deliberately moved into a processor;
- ETH/USD presentation and staleness policy, while a slim Leani Uniswap
  processor supplies the indexed WETH/USDC pool observation;
- API caching and presentation;
- product-specific daily/weekly aggregations;
- its public website schema.

### Cutover sequence

1. Replace the application-local client with the published
   `@smart-byte/leani-sdk` package.
2. Start Leani from application-requested ranges derived from PostgreSQL state.
3. Compare Leani output with existing `blocks` and `blob_transactions`.
4. Run the native consumer into shadow PostgreSQL tables.
5. Verify reorg, restart, expired-cursor, and backfill-gap behavior.
6. Make the SDK consumer primary while retaining Alchemy as a disabled-by-
   default emergency switch.
7. Observe CU usage and parity.
8. remove normal Alchemy block, receipt, and head traffic.
9. Remove `full_blocks` only after its remaining application readers are gone
   and a recoverable migration is approved.

The application can also keep the local JSON-RPC configured for development.
That removes the tendency for ordinary developer startup to consume Alchemy CUs.

## 12. API acceptance criteria

- Every response validates against the checked-in OpenAPI v1 schema.
- Every processor query exposes coverage and finality.
- A resumed stream produces no unreported gap.
- Reorg apply/undo events drive a test PostgreSQL projection to the same final
  state as a clean replay.
- A cursor committed with an application transaction resumes safely after a
  process kill.
- An expired cursor produces `reset_required`, never a silently truncated
  stream.
- SDK tests pass on Bun and Node.
- blobs.money can ingest a golden range using only the SDK.
- JSON-RPC capability discovery accurately reports configured indexes and
  unsupported state methods.
