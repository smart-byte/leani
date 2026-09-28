---
title: Configure a node
description: Compose data sources, processor identities, lifecycle policies, API settings, credentials, and operating profiles.
section: operations
order: 10
audience:
  - operator
  - processor-author
status: preview
---

Leani reads strict, versioned TOML. Unknown fields are errors. Relative paths
in a configuration file, such as `data_dir` or an archive source's
`manifest`, are relative to that file; path flags on the command line stay
relative to the working directory. Version 1 has two first-class document
shapes: a compact processor-oriented setup with sane network defaults, and
the fully explicit advanced schema. Both become the same validated runtime
configuration. The machine-readable contract is rendered in
the [configuration field reference](https://leani.dev/docs/reference/configuration/); six
complete advanced lifecycle profiles are checked under `config/modes`.

For a continuous block-following demo, generate the compact form and a current
trusted checkpoint together:

```bash
leani init blocks
leani doctor
```

For a local Uniswap node, select one or more markets instead:

```bash
leani init uniswap-v3 ETH/USDC
leani doctor
```

`init` requires a strict majority of the configured checkpoint providers, at
least two of the three defaults, to agree on one finalized slot and root, and
fails closed if a responding provider reports another root at that slot. It
shows every provider's answer, asks before accepting the checkpoint, writes
`leani.toml`, and never overwrites an existing file. A global `--config PATH` selects another output
path. The node keeps its state in `data`, beside the configuration file,
unless `--data-dir` names another directory; like any path flag, a relative
`--data-dir` is relative to the working directory. The generated document
contains only the choices an operator normally needs:

```toml
config_version = 1
network = "ethereum-mainnet"
data_dir = "./data"

[finality]
checkpoint = "0x..."
checkpoint_slot = 15100000
endpoints = [
    "https://ethereum-beacon-api.publicnode.com/",
    "https://lodestar-mainnet.chainsafe.io/",
]

[blocks]

[api]
bind = "127.0.0.1:18080"
```

The checkpoint and slot above are illustrative; the generator writes the live
quorum result. The checkpoint is the trust root. PublicNode
(`https://ethereum-beacon-api.publicnode.com/`) and Lodestar
(`https://lodestar-mainnet.chainsafe.io/`) are ordinary, untrusted Beacon API
transports in the compact profile's managed transport pool; they do not provide
execution blocks or Uniswap data. `init` writes that pool plus every responding
checkpoint provider that also serves the Beacon light-client API, without
duplicates. Leani queries the pool concurrently and verifies every response
locally from the pinned checkpoint. After the first verified response it waits
about two seconds for the others, then follows the highest finalized slot that
`minimum_agreement` endpoints have reached or passed. With the compact
profile's default agreement of one, the most recent verified finality wins,
and a stalled transport does not hold up node startup. Listing endpoints in
the compact document replaces the managed defaults, which lets operators use
only self-hosted transports.
Compact Ethereum Mainnet configurations expand to:

| Concern | Default |
|---|---|
| execution | native P2P, one-peer availability floor, 16-peer healthy target, 100-peer cap |
| history | public Xatu source, on-demand processor scheduling |
| processor | selected built-in feed, included and finalized output, checkpointed state, 256-block undo safety |
| resources | 512 MiB memory, 2 GiB temporary disk, four source and mapper tasks |
| retention | full query output; 64 MiB or 24 hours of change delivery |
| listeners | ephemeral P2P ports, RPC `127.0.0.1:18545`/`18546`, API `127.0.0.1:18080` |

The reviewed expansion is checked in as
[`config/defaults/ethereum-mainnet.toml`](https://github.com/smart-byte/leani/blob/main/config/defaults/ethereum-mainnet.toml).
Use the advanced schema below when any expanded field needs an override.

Validate a file without opening its database or any network connection:

```bash
cp config/modes/windowed.toml leani.toml
leani doctor --json
```

Without `--config`, Leani uses `./leani.toml`. Set `LEANI_CONFIG` when a service
or shell should consistently use a configuration elsewhere. An explicit
`--config` remains the highest-precedence override. Compact documents
default operational tuning but never the chain identity, checkpoint trust
root, or selected processor targets. Advanced documents keep every choice explicit.

## Advanced root fields

| Field | Type | Meaning |
|---|---|---|
| `config_version` | integer | Must be `1`. |
| `data_dir` | path | Writable node state. Use a distinct directory per deployment or test. A relative path is relative to the directory of the configuration file, not the working directory. |
| `chain` | table | Portable name and non-zero EIP-155 `chain_id`. |
| `artifact_storage` | table | Physical backend and bounded compaction policy for retained processor artifacts. |
| `budgets` | table | Hard memory, temporary disk, pending-delta, recent-input, and concurrency limits. |
| `sources` | table | Priority-ordered history sources and the optional live source. |
| `finality` | table | `consensus_p2p`, `beacon_api`, or `disabled`. |
| `processors` | array | One immutable processor kind/instance contract per entry. |
| `rpc` | table | HTTP/WS binds, browser origins, request and response limits, and bounded historical lookup behavior. |
| `api` | table | Native API bind, bearer-token environment variable, allowed hosts and browser origins, and delivery batching. |

`budgets.recent_raw_soft_bytes` must not exceed
`budgets.recent_raw_hard_bytes`. All byte budgets and concurrency values are
positive. Each finality advance prunes finalized recent frames toward the soft
limit. Live ingestion enforces the hard limit as it retains each frame: at the
limit it waits, with live readiness down, until finality prunes (see the
operations runbook's storage pressure section).

Two budgets bound a history source read's input. `budgets.temporary_disk_bytes`
bounds what one read may acquire over its life: the objects, byte ranges,
and responses it fetches, and the frames it builds from them. A read that
would acquire more fails with an `input_bytes` budget error, and a local
archive object larger than it is refused before it is read.
`budgets.memory_bytes` bounds the raw input one read holds in memory at once:
one line of a local archive object, which it verifies whole and then reads a
line at a time; the selected columns of one Xatu Parquet row group; one batch
of EraE byte ranges, fetched a few blocks at a time when a batch would not
fit; one window of execution P2P frames; or one retained raw-history record.
Its frames may be at most `memory_bytes`, or 32 MiB when that is less. A read
that would hold more fails with a `resident_bytes` budget error naming
`budgets.memory_bytes`, and a backfill then tries its next configured source.
A historical RPC request, which holds everything it returns, may acquire and
hold at most `memory_bytes` divided by `budgets.source_concurrency`.
Raw-history jobs acquire one segment's worth per read,
`raw_history.maximum_segment_logical_bytes`.

The bound is per read, not for the node. At once, the node runs up to
`budgets.history_pipeline.maximum_active_chunks` backfill reads, one archive
reconciliation read, the live lane, and one read per running raw-history
job, and each may hold up to `memory_bytes`. What a read decodes is not
counted here: decoded frames and mapped deltas have the
`budgets.history_material` and pipeline budgets below, and `memory_bytes` is
not a process RSS limit. On mainnet, a Xatu read of a 2023–24 transaction
object, 1,000 blocks around blocks 18,000,000 to 19,430,000, holds about
80–110 MiB of compressed columns and decodes about 230–340 MiB. Keep
`memory_bytes` above the largest of those, as the shipped 512 MiB is, and
plan RAM for about `maximum_active_chunks` × 450 MiB during Xatu backfills.

`[budgets.history_material]` is the sole budget for shared immutable source
frames and acquisition reorder buffers. Chunks read ahead of the one a job is
reading reserve memory only up to half of its `memory_bytes`, so size it to at
least twice the largest expected frame. The half is checked as each frame is
reserved: a chunk that another job joins, or that its reading job pauses and
leaves while others wait for it, becomes read-ahead with the frames it already
holds, so read-ahead can hold more than half. `[budgets.history_pipeline]` is
separate: `maximum_active_chunks` caps physical history reads node-wide and
`maximum_mapped_bytes` caps processor-owned mapped deltas across every active
job. Its nested `commit` table flushes a contiguous SQLite transaction at the
first configured block, domain-change, stable-encoded-byte, or elapsed-time
limit. `target_writer_hold` controls the adaptive block limit: the runtime
halves the current limit when observed p95 historical writer hold time exceeds
the target and cautiously grows it again below half the target. A single
mapped delta or single-block commit that exceeds its byte limit fails
immediately because waiting cannot make that item fit. A backfill that reuses
retained recent frames reads them in batches of at most `commit.maximum_blocks`
blocks and `maximum_mapped_bytes` encoded bytes, each holding one active-chunk
slot like a physical history read.

`[budgets.store].maximum_physical_bytes` is the emergency node-store admission
bound shared by SQLite, its WAL, and configured processor-artifact segments.
It is deliberately not nested under delivery: artifacts, materialized views,
correctness metadata, and delivery share the same physical capacity. It
measures allocated file bytes. Stores use SQLite incremental auto-vacuum, so
pruning returns pages to the filesystem and admission recovers; a store created
before that needs one `leani db compact` (see the operations runbook).

`[budgets.artifacts]` independently caps immutable finalized artifact bytes and
included-block candidate bytes awaiting finality. A transaction that would exceed
either logical limit rolls back without advancing processor coverage. Finality
is the exception: when the retained limit is full, finality still advances,
the finalized candidates stay pending with a logged warning, and a later
finality advance promotes them once there is room. A processor's `window`
policy may prune its own instance below a tighter bound; the node-wide
artifact budget still protects against the aggregate of many processors.

`[artifact_storage] backend = "sqlite"` retains those immutable artifacts as
KV rows. The opt-in `tiered_segments` backend uses SQLite as the durable write
buffer and continuously compacts complete `segment_target_blocks` ranges into
checksummed seekable files under `data_dir/processor-artifacts`. Compression,
single-item/segment hard limits, compaction cadence, and maximum segments per
cycle are explicit. Only processors using `artifacts.mode = "full"` are
eligible in the first tiered implementation; rolling windows remain in
SQLite. Segment bytes do not receive a second budget: they count against the
same `[budgets.store].maximum_physical_bytes` ceiling.

`[raw_history]` keeps finalized source frames in checksummed segment files
under `data_dir/raw-history`, beside their own catalog. `maximum_logical_bytes`
caps the frames retained, and `maximum_physical_bytes` the bytes on disk:
segments, the partial files of segments being written, the catalog and its
write-ahead log, and quarantined files. Before a segment is written, its
maximum size and the catalog rows it will add are reserved: a fixed 64 KiB,
and 512 bytes per locator row, one per block with block-hash locators and 256
per block with transaction-hash locators. A segment with more transactions
still publishes its extra rows, so the catalog can exceed
`maximum_physical_bytes` by that one segment's extra rows, which the next
segment's admission counts. Reads of `maximum_segment_logical_bytes`, one
segment's worth, feed a raw-history job. A store already over either budget,
for example after lowering it, still opens and serves its history, but
admits no new segment until it is back under: its jobs pause at the storage
limit, or fail when created with `segment.onLimit: fail`. A segment that
fails verification moves to `data_dir/raw-history/quarantine`, which keeps
at most 1 GiB, or one segment of `maximum_segment_physical_bytes` when that
is larger: past that, the longest-quarantined files are deleted first, but
never the newest (see the operations runbook).

`[budgets.delivery]` supplies the shared delivery envelope above independent
per-stream limits. `maximum_retained_bytes` caps exact delivery payload bytes
across live and history streams. `maximum_history_retained_bytes` is a lower,
non-borrowable history ceiling so stalled backfills cannot consume live-lane
headroom. The two values must satisfy `history <= retained`. Reaching a shared
bound backpressures only the committing lane; ACK, pruning, queries,
networking, and unrelated processors remain available.

## Sources and finality

`[[sources.history]]` requires `id`, `kind`, `priority`, and `trust`.
Supported kinds are `xatu`, `era_e`, and `archive`. `xatu` reads public
Parquet history; validation refuses `parquet`, which no source reads. `archive`
requires `manifest`; `era_e` alone accepts an `https` or `file` `endpoint`
ending in `/`. A plain `http` endpoint is accepted on loopback, or elsewhere
only with `allow_insecure_http = true` on that source, which accepts a mirror
whose catalog and archive bytes anyone on the path can replace. `xatu`
sources are Ethereum mainnet only: another `chain.chain_id` fails
validation.

Xatu additionally accepts three acquisition-tuning controls. `chunk_blocks`
sets the generic header, transaction, and log span, while
`blobs_chunk_blocks` sets the wider span used by the five-table blobs join.
Both must be non-zero multiples of Xatu's physical 1,000-block execution
partition. `batch_rows` controls the bounded Arrow/Parquet decode batch and is
independent of history-source concurrency. The measured defaults are 1,000,
8,000, and 8,192 respectively. Increasing a span can reduce repeated daily
object reads, but it also increases per-chunk memory and is not universally
faster; tune it per projection rather than as a global block-size knob.

`[sources.live] kind = "p2p"` enables the shared persistent Reth peer manager.
It requires verified `beacon_api` or `consensus_p2p` finality. Stable
production ports normally use `listener_port = 30303`,
`discovery_port = 30303`, and `discv5_port = 30304`. Discv4 and Discv5
require distinct fixed UDP ports; zero selects an isolated ephemeral port for
that protocol.

Reth owns the bounded dial queue and receives TCP/RLPx failures directly. A
terminal failure immediately releases its slot to another eligible peer, while
Reth's per-peer backoff prevents rapid redials. Newly authenticated DNS/Discv
candidates are added to that same queue. `max_concurrent_dials` remains the
hard simultaneous dial ceiling; `peer_refill_interval_ms` is only a short
safety-net poll for newly added candidates, not a network request rate.
`minimum_peers` is the connected-session startup gate. A peer need not finish
capability qualification before requests may use it: every response is still
commitment-checked, and qualification ranks proven peers first. Background
qualification continues concurrently until
`body_serving_peer_target` distinct peers have served a
commitment-valid body at the current target; the default target is four.
Request health is tracked independently for headers, bodies, and receipts. A
timeout or incomplete response cools only that material lane, so a peer that
cannot serve one processor's body request remains available to processors that
can use its headers or receipts. Commitment-invalid data still bans the peer.

`bootstrap_dns_tree` may point at an optional EIP-1459 `enrtree://` peer-hint
tree. The public key embedded in that URL authenticates its ENRs. It does not
make those peers trusted sources: Leani still commitment-checks every header,
body, and receipt. This is the deployment hook for a future Leani/community
pool of execution nodes prepared to accept indexing traffic; compact and
advanced configurations leave it unset by default.

Finality checkpoints are bootstrap trust anchors, not evergreen config
defaults. The first start needs a checkpoint at most 14 days old; later starts
bootstrap from the newest verified anchor the node persisted (see the
runbook's [checkpoint lifecycle](/docs/operations/runbook/#checkpoint-lifecycle)).
Enabled finality requires a recent 32-byte checkpoint root other than all
zeros. `consensus_p2p` also requires its non-zero finalized beacon slot and a
`finality.minimum_peers` of at most 24, the consensus peers dialed at once.
`beacon_api` requires one or more endpoints, each transport listed once, and
`minimum_agreement` within their count. Disabling finality is suitable for bounded finalized-dataset
backfills, not verified head following: the live lane includes a block only
once its ancestry reaches an execution header the sync committee attested,
which the finality source verifies each slot, so validation refuses
`sources.live.kind = "p2p"` with `finality.kind = "disabled"`.

## Processor identity and publication

Every modern entry declares:

```toml
[[processors]]
id = "evm-events"              # registered processor kind
instance = "weth-transfer-v1"  # stable operator-selected identity
version = "1.1.0"
start_block = 12965000
publish = "included_and_finalized"
```

Processor code, semantic version, settings, start point, and source identity
are part of an instance's immutable identity. The node refuses a change to any
of them under an `instance` its store holds (`processor instance … conflicts
with its stored descriptor`), before it upgrades an older store, so it never
mutates an existing cursor namespace. Give the changed processor a new
`instance` ID: it indexes from its `start_block` beside the old instance,
whose rows stay in the store. Lifecycle policies, such as retention and delivery
limits, are not part of the identity and can change in place.

Publication values are:

- `finalized_only`: publish only finalized `apply` records. Included-block
  changes wait in the store for verified finality, count against the delivery
  budget meanwhile, and undo is never published.
- `included_and_finalized`: publish apply/undo/finality transitions and keep
  an unfinalized undo window.
- `terminal_only`: a bounded backfill publishes its terminal aggregate and
  then becomes complete/reclaimable. Deletion remains explicit.

`[processors.settings]` belongs to the selected factory and is decoded
strictly. `doctor` constructs every configured processor, so malformed ABI,
address, pool, or custom settings fail before source or store I/O.

## Orthogonal lifecycle policies

Every processor has six independent lifecycle policies. The pre-launch
`retention` shorthand has been removed so artifact, output, delivery, state,
checkpoint, and undo semantics cannot be inferred from one overloaded setting.

### State

`[processors.state] mode` is `durable` or `checkpointed`. The store persists
processor state in both modes, and `[processors.checkpoint]` alone decides
whether recovery checkpoints are taken: `checkpointed` requires
`[processors.checkpoint] mode = "automatic"`. `ephemeral` is rejected, because
no mode discards processor state.

### Artifacts

`[processors.artifacts] mode` is `none`, `window`, or `full`. Artifacts are
immutable finalized map results used for bulk scan, export, and compatible
replay. They are independent of queryable output and delivery records; the
default is `none`.

`window` requires exactly one `[processors.artifacts.window]` limit:
`max_blocks`, `max_age`, or `max_bytes`. Full retention is removed only by an
explicit artifact deletion operation.

### Output

`[processors.output] mode` is:

- `none`: retain no queryable entity material;
- `latest`: retain the current entity value per key;
- `window`: retain the declared block, wall-clock age, row, or byte window;
- `full`: retain all selected queryable output.

`window` requires a `[processors.output.window]` table with exactly one
limit: `max_blocks`, `max_age`, `max_rows`, or `max_bytes`. Validation
refuses `[processors.output] finalized_only`, which was never applied; to
publish only finalized blocks, set `publish = "finalized_only"`.

### Delivery

`[processors.delivery] mode` is `best_effort`, `window`, or
`until_acknowledged`. Byte sizes accept integers or `B`, `KiB`, `MiB`, `GiB`,
`KB`, `MB`, and `GB` strings. Durations accept integer seconds or `s`, `m`,
`h`, and `d` strings.

`until_acknowledged` requires at least one required
`[[processors.delivery.consumers]]`, each with a `lease_ttl` from one second
to 100 years. Required consumers protect pruning until
they are explicitly expired/reset; a temporarily lapsed lease does not
silently advance the watermark. `on_limit` is `pause`, `fail`, or
`expire_and_reset`. Paused instances automatically resume after
acknowledgement/pruning moves below the low-water mark.

Pruning is cumulative and block-aligned:

```toml
[processors.delivery.pruning]
interval = "30s"
minimum_batch_blocks = 64
minimum_batch_changes = 10000
maximum_delete_changes = 10000
retain_finalized_blocks = 256
retain_acknowledged_age = "1h"
```

`max_age` and `retain_acknowledged_age` use durable node wall-clock
enqueue/acknowledgement time, not Ethereum block timestamps.

### Checkpoint and undo

`[processors.checkpoint] mode` is `none` or `automatic`. Automatic mode
checkpoints processor state at most once per 1,000 finalized blocks, and again
when a historical job completes, and keeps the newest positive `keep` count.
A restore repairs state only when the processor's cursor sits exactly on a
checkpoint: a cadence block or the end of a completed job, even if finality
reached that block later. A processor between checkpoints cannot be restored
from one; restore a full-store backup or rebuild the processor. Portable
savepoints are exports, created and deleted explicitly through the API; the
node cannot restore them, and automatic checkpoint pruning does not cover
them. Each processor instance keeps at most 16, and each new one counts
against the physical store budget, which answers 507 when it does not fit.
Editing lifecycle policies, for example to raise a limit, leaves existing
checkpoints and savepoints valid.

`[processors.undo] mode` is `none` or `unfinalized`, and
`included_and_finalized` requires `unfinalized`. Unfinalized blocks always
keep their undo records, so a reorg can revert them in either mode. Once
finality passes a block, `unfinalized` keeps its record for another
`safety_blocks` blocks and `none` deletes it at once. Blocks applied as
already finalized record no undo.

## API and credentials

Set `api.bearer_token_env` to the name of an environment variable, never a
token value. When configured, that one bearer protects native read, stream,
consumer, and management routes on the API listener; operational health,
metrics, and network-dashboard routes remain unauthenticated. `serve` fails
to start when the variable is unset, holds fewer than 16 characters, or holds
anything but printable ASCII without spaces; use a random value such as
`openssl rand -hex 32`. Clients send
`Authorization: Bearer <token>`, with the scheme in any case, and the node
compares tokens in constant time. A consumer created through the API carries
its own credential, and a backfill subscription's consumer can; a new one
holds 32 to 512 printable ASCII characters without spaces, such as
`openssl rand -hex 32`. Every consumer-scoped call for a consumer with a
credential, its streams included, then needs it in
`x-leani-consumer-credential` besides the bearer, and gets 403 without it, so
one client cannot move another consumer's cursor. Listing, inspecting, and
revoking consumers stay administrative: only the bearer protects them, so an
operator can revoke a consumer whose credential is lost. Consumers
declared under `[[processors.delivery.consumers]]` have no credential and
need only the bearer. The store keeps credential hashes and session-token
keys under a random per-store secret, which backups carry with the store. A
backfill subscription's credential also enters its idempotency identity, an
unkeyed hash of the whole request, so use random credentials.

The native API, HTTP RPC, and WebSocket RPC binds must all differ. RPC has no
built-in authentication. A bind beyond loopback, such as `0.0.0.0`, fails
validation unless it is protected or explicitly opted in:

| Listener | Beyond loopback requires |
|---|---|
| `api.bind` | `api.bearer_token_env`, or `api.allow_unauthenticated_remote = true` |
| `rpc.http_bind`, `rpc.ws_bind` | `rpc.allow_unauthenticated_remote = true` |

Opt in only behind a trusted network or an authenticating proxy. Put remote
listeners behind a TLS/authenticating proxy and restrict the unauthenticated
operational routes to a monitoring network. The compact configuration has no
bearer setting, so its `[api] bind` stays on loopback; use the advanced
schema to serve beyond it.

### Browser access

The listeners refuse most requests a web page could make through a visitor's
browser:

- The native API answers a `Host` that is an IP address, `localhost`, or a
  name in `api.allowed_hosts`, and answers any other name with 421. A page
  that points its own name at 127.0.0.1 still sends that name; an address
  cannot be repointed, so health probes and scrapers that call the node by
  IP keep working. List every name clients use to reach the API, such as a
  compose service name or the name a reverse proxy forwards, without a
  scheme or port: `allowed_hosts = ["leani", "indexer.internal"]`.
- Browsers send an `Origin` header on CORS requests, such as `fetch()` calls
  to another origin, event streams, and WebSocket upgrades, and on POST and
  DELETE requests. They send none on navigations or on no-cors GET and HEAD
  requests, such as an image or script load. Both listeners accept requests
  without an `Origin`, and requests from loopback origins on any port. Any
  other origin gets 403. A browser application served from another origin,
  including one behind the same reverse proxy, needs its origin in
  `api.allowed_origins` or `rpc.allowed_origins`, such as
  `["https://app.example"]`. The node sets no CORS headers; a cross-origin
  application still needs a proxy that adds them and allows the
  `content-type` and `x-leani-request` request headers.
- The native API refuses, with 403 `cross_site_request`, a GET or HEAD
  without an `Origin` that the browser's Fetch Metadata marks as coming from
  another site's page (`Sec-Fetch-Site: cross-site` or `same-site`) and not
  as a navigation (`Sec-Fetch-Mode` other than `navigate`). A consumer's
  live or backfill stream route refuses such a request even as a navigation,
  since opening the stream takes the consumer's session lease. Current
  browsers send Fetch Metadata only to potentially trustworthy URLs, which
  are HTTPS, loopback addresses, and `localhost`; a node reached over plain
  HTTP by another name or address, such as a LAN IP with
  `allow_unauthenticated_remote`, gets none, and neither refusal applies
  there. The SDK and curl send none either, and pass.
- A native API POST or DELETE needs `content-type: application/json` or the
  header `x-leani-request: 1`, and gets 415 otherwise. JSON-RPC calls need
  `content-type: application/json`. The SDK sends both. A cross-site page
  can send neither without a CORS preflight, which the node never grants.
- Only `POST /v1/processors/{processor}/collections/{collection}/entities`
  creates a query snapshot. A GET without a cursor reads current output
  without one.

What still reaches the native API from a page: navigations from another
site, including into a frame, to routes other than consumer streams, and
GET or HEAD requests without Fetch Metadata: from browsers without it, or to
a node served over plain HTTP by a name or address other than loopback or
`localhost`. The page cannot read the responses, but without Fetch Metadata
a GET that opens a consumer's live or backfill stream takes that consumer's
session lease. A consumer's credential keeps pages from opening its stream,
and `api.bearer_token_env` keeps them from opening any; a browser adds
neither header on its own.

When upgrading a relative `data_dir`, Leani refuses to open another location
if its former working-directory location still contains a database or
embedded subscriptions. This also applies when both locations have state.
Set an absolute path to select the intended store, or move the old state.
Equivalent paths and symlink aliases of the same directory are accepted.

### Historical CLI and node backfill

`leani backfill --processor INSTANCE --from N --to M` uses the same history
source assembly, configured pipeline, material coordinator and verified P2P
fallback as node jobs. When live P2P is enabled, the standalone command
verifies the configured finality anchor and opens its own persistent peer
pool. It checks the finalized upper bound and honors
`sources.live.history_fallback_blocks`. No live listeners or processors are
started. Disabled P2P and retained-input-only policies remain in force.

To use a running node and its existing peer pool:

```bash
leani backfill --endpoint http://127.0.0.1:18080 \
  --processor INSTANCE --from N --to M
```

The command sends an authenticated materialization request and waits for its
status. `LEANI_API_TOKEN` supplies the token without putting it in shell
arguments. An optional endpoint path prefix is preserved. Ctrl-C cancels
the submitted job. Without `--endpoint`, stop any process holding the data
directory before starting standalone backfill. Ctrl-C cancels a standalone
run, so the next `serve` does not resume it; a second Ctrl-C exits at once.

A standalone job is named by processor, chain and range. When an earlier run
of the same range recorded another verification policy, for example before
`raw_history` was enabled, the store refuses the new input as a different
job. Delete the earlier one with `DELETE /admin/v1/materialization-jobs/{id}`,
or submit through `--endpoint`, which uses a fresh key per submission.

Xatu exports have table-specific coverage. In particular, a recent beacon
file does not prove that the execution-transaction file needed for receipts
has been published. Missing or incomplete partitions can fall back to the
verified P2P bridge; dataset-only configurations need another available
history source. Finality or peer failures are reported rather than silently
removing the configured bridge.

### JSON-RPC limits

Each limit has a `rpc` setting:

| Setting | Default | When exceeded |
|---|---|---|
| `max_batch_requests` | 100 | the batch is refused whole with one `-32600` error |
| `max_response_bytes` | `16MiB` | each call past the limit is answered with `-32005` |
| `max_log_results` | 10000 | `eth_getLogs` fails with `-32005` and a smaller block range to retry |
| `max_log_addresses` | 1000 | the filter fails with `-32602` |
| `max_log_topic_alternatives` | 1000 | the filter fails with `-32602` |
| `max_subscriptions_per_connection` | 128 | `eth_subscribe` fails with `-32005` |
| `max_websocket_connections` | 256 | the WebSocket upgrade gets HTTP 503 |
| `max_outbound_bytes` | `64MiB` | shared across WebSocket listeners and connections; response work waits for capacity, and notifications close the connection with `1013` when no room remains |
| `max_subscription_event_bytes` | `16MiB` | a WebSocket connection whose subscriptions produce more notification bytes for one chain event is closed with `1008`, and one that leaves more unread with `1013` |

A closed WebSocket connection releases its slot and its subscriptions.
Notifications are queued one at a time, so while earlier ones are still
unsent, a connection can be closed with `1013` before one event's
notifications pass the limit that would close it with `1008`.
Each connection retains its own notification limit and one response slot.
All WebSocket listeners share `max_outbound_bytes` of queued or sending
payloads (64 MiB by default). Response encoding reserves capacity first;
notifications acquire capacity before entering the queue. Sending, closing,
and cancellation release it. A write that remains blocked for 30 seconds
closes the connection. These are encoded-payload limits, not total process
RSS limits. `max_outbound_bytes` must be at least `max_response_bytes` and
at most 4,294,967,295 bytes.

`rpc.transaction_locator` is deprecated and ignored: nothing reads it. The
node still accepts it, with a warning in its log and in `leani doctor`;
remove it.

### Delivery batching

History and live delivery use independent transport limits under
`api.delivery.history_batches` and `api.delivery.live_batches`. The defaults
are byte-first (`4MiB` history, `64KiB` live), with hard encoded-byte, event,
processed-block, and maximum-delay bounds. History subscriptions may request
smaller effective limits; those values are persisted with the subscription and
returned by its status endpoint. `maximum_buffered_batches` and
`maximum_buffered_bytes` independently cap the per-connection encoder queue;
backpressure stops the encoder producer before either bound is exceeded.
SDK delivery sessions refuse an NDJSON record over 65 MiB with a
non-retryable `invalid_response` error. A record carries up to
`maximum_encoded_bytes` of changes plus its own fields, so for SDK clients
keep each lane's `maximum_encoded_bytes` at or below 64 MiB; raising it, and
`maximum_buffered_bytes` with it, beyond that lets a record exceed what the
SDK accepts.
Opening a history stream may request only stricter byte/event/block/delay
limits through query parameters; these are connection-local and do not mutate
the durable subscription. A live stream, including one the SDK's
`streamLive()` opens, takes no such parameters and uses `live_batches` as
configured. The SDK's `latency`, `balanced`, and `throughput`
profiles are convenience functions that expand to those concrete numbers and
are resent after reconnect.
`compression = "gzip"` is negotiated through
`Accept-Encoding`; the response is one continuous gzip member with an
explicit sync flush after every NDJSON record, so batches and heartbeats are
observable immediately by streaming decoders. Set it to `"none"` to disable
compression for that lane.

## Pre-launch schema policy

The project is not live yet and has no compatibility contract for existing
consumers, config files, or databases. Configuration, SDK, API, and SQLite
schemas may be replaced when that produces a cleaner final design; development
databases should be recreated after an announced incompatible change. Durable
delivery and processor encodings remain explicitly versioned so upgrade-safe
replay can become a launch requirement without changing the public cursor
model.
