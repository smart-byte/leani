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
logs. A client that falls behind the bounded event channel is closed with code
1013 and must reconnect.

Public aggregate routes:

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
readiness and processor range coverage. The operational routes are not behind
the optional API bearer token so a protected local supervisor can probe them.
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
DELETE /admin/v1/backfill-subscriptions/{id}
POST /admin/v1/processors/{processor}/rebuild
POST /admin/v1/sources/{source}/probe
```

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
never retried implicitly. After correcting a permanent failure, explicitly
delete the terminal job before recreating the request; deletion is the retry
boundary.

## 3. Common response model

Every processor query includes coverage:

```ts
export interface ProcessorCoverage {
  chainId: number;
  requested?: {
    fromBlock: number;
    toBlock: number;
  };
  available: Array<{
    fromBlock: number;
    toBlock: number;
    finality: "optimistic" | "safe" | "finalized";
  }>;
  configuredStartBlock: number;
  historicalTargetBlock: number | null;
  liveHeadBlock: number | null;
  processedThrough: number | null;
  finalizedThrough: number | null;
  complete: boolean;
  state:
    | "starting"
    | "backfilling"
    | "catching_up"
    | "live"
    | "degraded"
    | "failed";
}
```

Every `available` interval is inclusive and has one exact finality for every
block it contains. Intervals split at both coverage gaps and finality
boundaries, so consumers selecting finalized material must filter these ranges
rather than treating `finalizedThrough` as a contiguous watermark.
`finalizedThrough` is only the highest covered block recorded as finalized;
lower gaps or non-finalized coverage can still exist.

For a block-local processor, `available` can contain several intervals while a
backfill and head follower operate concurrently. `processedThrough` is only set
when the complete interval begins at the configured start and has no gaps.

For an ordered-state processor, coverage also reports:

```ts
export interface OrderedStateCoverage {
  reduceCursor: BlockCursor | null;
  pendingThrough: number | null;
  seededFromCheckpoint: boolean;
}
```

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
2. import every page using its opaque `nextCursor`; and
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
acknowledged watermark. Creation always names a start position:

```http
POST /v1/processors/blobs-money/consumers
content-type: application/json

{
  "id": "blobs-api",
  "role": "required",
  "start": { "position": "earliest_retained" },
  "leaseTtlSeconds": 300,
  "credential": "<random per-consumer secret>"
}
```

`position` is `earliest_retained`, `current_head`, or `cursor` with a `cursor`
field. An old, wrong-store, wrong-instance, or incompatible cursor returns
`reset_required`; the node never substitutes another position.

Consumer-scoped renew, change-read, and acknowledgement calls send:

```http
x-leani-consumer-credential: <random per-consumer secret>
```

The secret is hashed at rest. A consumer credential cannot inspect or
acknowledge another consumer. Administrative bearer authorization remains
available when configured.

Read a batch:

```http
GET /v1/processors/blobs-money/consumers/blobs-api/changes?limit=500
x-leani-consumer-credential: ...
```

After committing the destination transaction and its cursor atomically:

```http
POST /v1/processors/blobs-money/consumers/blobs-api/ack
x-leani-consumer-credential: ...
content-type: application/json

{ "cursor": "<highest durably committed cursor>" }
```

Acknowledgement is monotonic and idempotent. A cursor beyond the highest
sequence delivered to that consumer is rejected, preventing a buggy client
from pruning unseen changes. `commit_then_ack` in the Rust API helper does not
construct or execute the acknowledgement future until the destination commit
succeeds. The TypeScript SDK exposes the equivalent
`commitThenAcknowledge(() => destinationCommit(), cursor =>
client.processors.consumers.acknowledge(...))`; its acknowledgement callback
is likewise never invoked after a failed destination commit.

### Generic retained queries

Entity queries accept `fromBlock`, `toBlock`, `fromTimestamp`, `toTimestamp`,
`limit`, and an opaque `cursor`. Entity keys are URL-safe hex. Every result
contains the processor schema, decoded JSON data, block/timestamp/finality
metadata, stable snapshot ID, change boundary, retained bounds, and coverage.

`output = "none"` or a range outside the retained window returns
`output_not_retained` with a rebuild/source-scan action. It never returns a
successful empty page for unavailable local history.

The TypeScript SDK's
`client.processors.queryAndFollow(processor, collection, query)` returns this
snapshot and its `boundaryCursor` in one operation. Continue with
`client.processors.subscribe(processor, { after: page.boundaryCursor })`.

Release a no-longer-needed snapshot early:

```http
DELETE /v1/processors/{processor}/query-snapshots/{snapshotId}
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

`[budgets.artifacts]` caps retained finalized bytes and pending optimistic
candidates across all processors. Admission is atomic: exceeding either limit
rolls back artifacts, processor mutations, coverage, and cursor advancement.
The metrics endpoint reports current and maximum artifact bytes separately;
window pruning, explicit owner release, or a larger configured budget lets
storage-backpressured work resume.

### Checkpoints and portable savepoints

Automatic recovery checkpoints are read-only through
`GET /v1/processors/{processor}/checkpoints` and obey configured count
retention. `POST .../checkpoints/{checkpoint}/restore` atomically repairs
private processor state only when the checkpoint is at the exact current
cursor. Older checkpoints are rejected because rewinding state without also
rewinding independent output, delivery, coverage, and undo storage would be
unsafe; use a deterministic rebuild or a full-store backup for that case.
Portable savepoints are operator-created:

```http
POST /v1/processors/{processor}/savepoints
content-type: application/json

{ "id": "before-v2-upgrade" }
```

`GET .../savepoints/{id}` exports a checksummed, versioned
`application/vnd.leani.savepoint` archive. Deletion is explicit with
`DELETE`; acknowledgement or checkpoint pruning never deletes a portable
savepoint.

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
  finality: "optimistic" | "safe" | "finalized";
  kind: string;
  schema: string;
  key: string | null;
  data: T | null;
  /** Present only on reset_required, as the current coverage hint. */
  coverage?: ProcessorCoverage;
  emittedAt: string;
}
```

Envelopes carry change-scoped facts, including the processing lane's origin and
publication revision. Connection-scoped processor identity and node coverage
are served by the stream's `hello` frame (section 4.3) and by `/v1/status`;
poll responses continue to carry one top-level `coverage` per page.

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
- may be optimistic.

`undo`

- reverses a previously committed `apply`;
- contains the inverse value/change required by downstream stores;
- has its own newer sequence/cursor.

`finalized`

- advances finality for a processor/chain;
- need not repeat every entity;
- permits a consumer to mark earlier changes durable.

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

The server sends comments as heartbeats. The SDK reconnects with the last fully
yielded cursor, uses exponential backoff with jitter, and treats these
differently:

- clean server close: reconnect;
- transient network/5xx: reconnect;
- authentication/validation 4xx: surface error;
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
    "http": "http://127.0.0.1:8545",
    "webSocket": "ws://127.0.0.1:8546",
    "methods": {
      "eth_getBlockByNumber": {
        "recent": true,
        "historical": "on_demand_by_number"
      },
      "eth_getTransactionByHash": {
        "recent": true,
        "historical": false,
        "reason": "transaction_locator_disabled"
      },
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
      "version": "1.4.0",
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

Also expose an equivalent custom JSON-RPC method,
`leani_getCapabilities`, so an Ethereum-library integration can inspect the
endpoint without knowing the aggregate API port.

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
  finality: "optimistic" | "safe" | "finalized";
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
arrives as ordinary `apply` changes.

## 8. TypeScript SDK

Package:

```text
@leani/sdk
```

The public package name is reserved but not published yet. The pre-release
workspace package remains private until its JavaScript/type build and packed
Node/Bun smoke tests are part of the release workflow.

Runtime support:

- Bun, first-class and tested in the blobs.money repository;
- current Node LTS with native `fetch`;
- modern browsers for public read-only endpoints;
- no Node-only APIs in the core client.

Public shape:

```ts
import { createLeaniClient } from "@leani/sdk";

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
Consumers inspect or filter `event.finality`; `SubscribeOptions` contains only
`after` and `signal`.

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

`request<T>` is the safe primitive for custom query extensions. Relative paths
such as `v1/processors/<instance>/query/summary` preserve any base-URL path;
root paths such as `/v1/q/my-protocol/summary` target the configured origin.
Schemes, protocol-relative paths, and cross-origin resolution are rejected
before the bearer token is attached. Built-in query namespaces use this same
primitive.

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
applyEntityChange(...)
isResetRequired(...)
compareSequences(...)
createInMemoryCursorStore() // tests/development only
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
listener. Consumer credentials additionally fence one consumer's lease and
cursor; they do not replace the listener bearer when it is configured.
`/health/live`, `/health/ready`, `/metrics`, `/v1/network/status`, and
`/debug/network` remain unauthenticated operational routes. The JSON-RPC
listeners have no built-in authentication.

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

1. Publish the shared `@leani/sdk` package and replace the application-local
   client once their contracts match.
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
