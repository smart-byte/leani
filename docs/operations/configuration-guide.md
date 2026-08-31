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

Leani reads strict, versioned TOML. Unknown fields are errors. Version 1 has
two first-class document shapes: a compact processor-oriented setup with sane
network defaults, and the fully explicit advanced schema. Both become the same
validated runtime configuration. The machine-readable contract is rendered in
the [configuration field reference](https://leani.dev/docs/reference/configuration/); six
complete advanced lifecycle profiles are checked under `config/modes`.

For a local Uniswap node, generate the compact form and a current trusted
checkpoint together:

```bash
leani init uniswap-v3 ETH/USDC
leani doctor
```

`init` requires two configured checkpoint providers to agree on one finalized
slot and root, asks before accepting them, writes `leani.toml`, and never
overwrites an existing file. A global `--config PATH` selects another output
path. The generated document contains only the choices an operator normally
needs:

```toml
config_version = 1
network = "ethereum-mainnet"
data_dir = "./data"

[finality]
checkpoint = "0x..."
checkpoint_slot = 15100000
endpoints = ["https://ethereum-beacon-api.publicnode.com/"]

[uniswap]
markets = ["ETH/USDC"]

[api]
bind = "127.0.0.1:18080"
```

The checkpoint and slot above are illustrative; the generator writes the live
quorum result. Compact Ethereum Mainnet Uniswap configurations expand to:

| Concern | Default |
|---|---|
| execution | native P2P, one-peer availability floor, 16-peer healthy target, 100-peer cap |
| history | public Xatu source, on-demand processor scheduling |
| processor | built-in pool catalog, optimistic and finalized output, checkpointed state, 256-block undo safety |
| resources | 512 MiB memory, 2 GiB temporary disk, four source and mapper tasks |
| retention | full query output; 64 MiB or 24 hours of change delivery |
| listeners | ephemeral P2P ports, RPC `127.0.0.1:18545`/`18546`, API `127.0.0.1:18080` |

The reviewed expansion is checked in as
[`config/defaults/ethereum-mainnet-uniswap.toml`](https://github.com/smart-byte/leani/blob/main/config/defaults/ethereum-mainnet-uniswap.toml).
Use the advanced schema below when any expanded field needs an override.

Validate a file without opening its database or any network connection:

```bash
cp config/modes/windowed.toml leani.toml
leani doctor --json
```

Without `--config`, Leani looks for `./leani.toml` and then
`./config/example.toml` (the source-workspace fallback). Set `LEANI_CONFIG` when a
service or shell should consistently use a configuration elsewhere. An
explicit `--config` remains the highest-precedence override. Compact documents
default operational tuning but never the chain identity, checkpoint trust
root, or selected markets. Advanced documents keep every choice explicit.

## Advanced root fields

| Field | Type | Meaning |
|---|---|---|
| `config_version` | integer | Must be `1`. |
| `data_dir` | path | Writable node state. Use a distinct directory per deployment or test. |
| `chain` | table | Portable name and non-zero EIP-155 `chain_id`. |
| `artifact_storage` | table | Physical backend and bounded compaction policy for retained processor artifacts. |
| `budgets` | table | Hard memory, temporary disk, pending-delta, recent-input, and concurrency limits. |
| `sources` | table | Priority-ordered history sources and the optional live source. |
| `finality` | table | `consensus_p2p`, `beacon_api`, or `disabled`. |
| `processors` | array | One immutable processor kind/instance contract per entry. |
| `rpc` | table | HTTP/WS binds and bounded historical lookup behavior. |
| `api` | table | Native API bind and optional bearer-token environment variable. |

`budgets.recent_raw_soft_bytes` must not exceed
`budgets.recent_raw_hard_bytes`. All byte budgets and concurrency values are
positive.

`[budgets.history_material]` is the sole budget for shared immutable source
frames and acquisition reorder buffers. `[budgets.history_pipeline]` is
separate: `maximum_active_chunks` caps physical history reads node-wide and
`maximum_mapped_bytes` caps processor-owned mapped deltas across every active
job. Its nested `commit` table flushes a contiguous SQLite transaction at the
first configured block, domain-change, stable-encoded-byte, or elapsed-time
limit. `target_writer_hold` controls the adaptive block limit: the runtime
halves the current limit when observed p95 historical writer hold time exceeds
the target and cautiously grows it again below half the target. A single
mapped delta or single-block commit that exceeds its byte limit fails
immediately because waiting cannot make that item fit.

`[budgets.store].maximum_physical_bytes` is the emergency node-store admission
bound shared by SQLite, its WAL, and configured processor-artifact segments.
It is deliberately not nested under delivery: artifacts, materialized views,
correctness metadata, and delivery share the same physical capacity.

`[budgets.artifacts]` independently caps immutable finalized artifact bytes and
optimistic candidate bytes awaiting finality. A transaction that would exceed
either logical limit rolls back without advancing processor coverage. A
processor's `window` policy may prune its own instance below a tighter bound;
the node-wide artifact budget still protects against the aggregate of many
processors.

`[artifact_storage] backend = "sqlite"` retains those immutable artifacts as
KV rows. The opt-in `tiered_segments` backend uses SQLite as the durable write
buffer and continuously compacts complete `segment_target_blocks` ranges into
checksummed seekable files under `data_dir/processor-artifacts`. Compression,
single-item/segment hard limits, compaction cadence, and maximum segments per
cycle are explicit. Only processors using `artifacts.mode = "full"` are
eligible in the first tiered implementation; rolling windows remain in
SQLite. Segment bytes do not receive a second budget: they count against the
same `[budgets.store].maximum_physical_bytes` ceiling.

`[budgets.delivery]` supplies the shared delivery envelope above independent
per-stream limits. `maximum_retained_bytes` caps exact delivery payload bytes
across live and history streams. `maximum_history_retained_bytes` is a lower,
non-borrowable history ceiling so stalled backfills cannot consume live-lane
headroom. The two values must satisfy `history <= retained`. Reaching a shared
bound backpressures only the committing lane; ACK, pruning, queries,
networking, and unrelated processors remain available.

## Sources and finality

`[[sources.history]]` requires `id`, `kind`, `priority`, and `trust`.
Supported kinds are `xatu`, `era_e`, `parquet`, and `archive`. `archive`
requires `manifest`; `era_e` alone accepts an `http`, `https`, or `file`
`endpoint` ending in `/`.

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

Finality checkpoints are bootstrap trust anchors, not evergreen config
defaults. `consensus_p2p` requires a recent 32-byte checkpoint root and its
non-zero finalized beacon slot. `beacon_api` requires one or more endpoints
and `minimum_agreement` within their count. Disabling finality is suitable for
bounded finalized-dataset backfills, not verified head following.

## Processor identity and publication

Every modern entry declares:

```toml
[[processors]]
id = "evm-events"              # registered processor kind
instance = "weth-transfer-v1"  # stable operator-selected identity
version = "1.0.0"
start_block = 12965000
publish = "optimistic_and_finalized"
```

Changing processor code, semantic version, settings, start point, or source
identity creates a new immutable processor instance. It does not mutate an
existing cursor namespace.

Publication values are:

- `finalized_only`: publish only finalized results; no normal reorg undo is
  exposed.
- `optimistic_and_finalized`: publish apply/undo/finality transitions and keep
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

`[processors.state] mode` is `ephemeral`, `durable`, or `checkpointed`.
Optimistic publication cannot use ephemeral state.

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

`window` requires exactly one non-empty `[processors.output.window]` table.
Supported limits are `max_blocks`, `max_age`, `max_rows`, and `max_bytes`;
multiple limits are allowed and pruning uses the first exceeded safe boundary.
`finalized_only = true` prevents unfinalized output from satisfying historical
queries.

### Delivery

`[processors.delivery] mode` is `best_effort`, `window`, or
`until_acknowledged`. Byte sizes accept integers or `B`, `KiB`, `MiB`, `GiB`,
`KB`, `MB`, and `GB` strings. Durations accept integer seconds or `s`, `m`,
`h`, and `d` strings.

`until_acknowledged` requires at least one required
`[[processors.delivery.consumers]]`. Required consumers protect pruning until
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

`[processors.checkpoint] mode` is `none` or `automatic`; automatic mode keeps
the newest positive `keep` count. Portable savepoints are created and deleted
explicitly through the API and are not covered by automatic checkpoint
pruning.

`[processors.undo] mode` is `none` or `unfinalized`.
`optimistic_and_finalized` requires `unfinalized`; `safety_blocks` extends the
bounded correctness window.

## API and credentials

Set `api.bearer_token_env` to the name of an environment variable, never a
token value. When configured, that one bearer protects native read, stream,
consumer, and management routes on the API listener; operational health,
metrics, and network-dashboard routes remain unauthenticated. Consumer
creation can issue a separate credential; subsequent
renew, read, and acknowledgement calls use
`x-leani-consumer-credential`, so one consumer cannot move another
consumer's cursor.

The native API, HTTP RPC, and WebSocket RPC binds must all differ. RPC has no
built-in authentication. Put remote listeners behind a TLS/authenticating
proxy and restrict the unauthenticated operational routes to a monitoring
network.

History and live delivery use independent transport limits under
`api.delivery.history_batches` and `api.delivery.live_batches`. The defaults
are byte-first (`4MiB` history, `64KiB` live), with hard encoded-byte, event,
processed-block, and maximum-delay bounds. History subscriptions may request
smaller effective limits; those values are persisted with the subscription and
returned by its status endpoint. `maximum_buffered_batches` and
`maximum_buffered_bytes` independently cap the per-connection encoder queue;
backpressure stops the encoder producer before either bound is exceeded.
Opening a history stream may request only stricter byte/event/block/delay
limits through query parameters; these are connection-local and do not mutate
the durable subscription. The SDK's `latency`, `balanced`, and `throughput`
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
