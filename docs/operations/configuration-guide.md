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
| processor | selected built-in feed, unattested publication, checkpointed state, 256-block undo safety |
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

## Build a configuration from requirements

Instead of copying a profile and editing it, state what the application needs
and let `init` build a complete advanced document:

```bash
leani init uniswap-v3 ETH/USDC --keep window --window-days 30 \
  --follow finalized --memory 1GiB --disk 20GiB --bandwidth 100
```

`--keep` is `everything`, `window`, `latest`, `result`, `stream`, or `live`;
`--follow` is `head`, `finalized`, or `none`, which builds a history-only node
without a live lane or finality; `--history-from-block` starts a backfill;
`--memory`, `--disk`, and `--bandwidth` (Mbit/s) state the host's limits; and
`--tuning` replaces the recipes the limits call for. `--requirements FILE`
takes the same needs as JSON for any processor, including contract events and
Wasm or custom processors. The builder writes every setting and comments each
one it derived with the reason, using only the shipped profiles' storage
settings and the measured recipes of
[Sizing for bandwidth, memory, and disk](#sizing-for-bandwidth-memory-and-disk).
`--json` prints the configuration, the derived settings with their reasons,
warnings, and next steps. `leani guide config` describes the requirements, and
a running node builds the same document at `POST /admin/v1/config/build`,
which its `/playground` uses.

## Advanced root fields

| Field | Type | Meaning |
|---|---|---|
| `config_version` | integer | Must be `1`. |
| `data_dir` | path | Writable node state. Use a distinct directory per deployment or test. A relative path is relative to the directory of the configuration file, not the working directory. |
| `chain` | table | Portable name and non-zero EIP-155 `chain_id`. |
| `artifact_storage` | table | Physical backend and bounded compaction policy for retained processor artifacts. |
| `budgets` | table | Hard memory, temporary disk, pending-delta, recent-input, and concurrency limits. |
| `sources` | table | Priority-ordered history sources and the optional live source. |
| `finality` | table | `consensus_p2p`, `beacon_api`, `json_rpc`, `op_gossip`, or `disabled`. |
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
of EraE byte ranges, including a prefetched window when both fit the budget;
one window of execution P2P frames; or one retained raw-history record.
EraE normally targets 16 MiB compressed windows independently of the decoded
frame buffer, and shrinks them when the resident budget is smaller.
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
plan RAM for about `maximum_active_chunks` × 450 MiB during Xatu backfills,
3.6 GiB at the default 8.
The allocator keeps memory a finished read freed for the next, so resident
memory after a burst of reads stays near its peak: set a container's memory
limit for that peak, not for the budgets. On Linux, glibc gives each thread
that allocates its own 64 MiB arena and returns little of what they free, so
a long-running node grows: an rc.4 node on a 20-thread host held 5.7 GB, 4.7
GB of it in 57 such arenas. Over a 100,000-block `blobs-money` backfill from
Xatu, glibc reached 1.5 GiB and kept it. The container image preloads
jemalloc, which returns freed memory: the same backfill held 711 MiB and fell
to 130 MiB when it finished. `deploy/leani.service` sets `MALLOC_ARENA_MAX=2`,
which held it at 0.8–1.0 GiB. Wherever you run the binary yourself, set
`MALLOC_ARENA_MAX=2` or preload jemalloc (Debian's `libjemalloc2` with
`LD_PRELOAD=libjemalloc.so.2`).

`[budgets.history_material]` is the sole budget for shared immutable source
frames and acquisition reorder buffers. Chunks read ahead of the one a job is
reading reserve memory only up to half of its `memory_bytes`, so size it to at
least twice the largest expected frame. The half is checked as each frame is
reserved: a chunk that another job joins, or that its reading job pauses and
leaves while others wait for it, becomes read-ahead with the frames it already
holds, so read-ahead can hold more than half. `[budgets.history_pipeline]` is
separate: `maximum_active_chunks` caps physical history reads node-wide and
`maximum_mapped_bytes` caps processor-owned mapped deltas across every active
job. A job reads only its own planned chunks, so a backfill subscription
smaller than `maximum_active_chunks` source chunks leaves read slots idle; the
node logs a hint with a size that fills them, once per processor, and
`leani_history_material_active_acquisitions` against
`leani_history_material_active_acquisitions_limit` shows the slots in use. Its nested `commit` table flushes a contiguous SQLite transaction at the
first configured block, domain-change, stable-encoded-byte, or elapsed-time
limit: by default 128 blocks, 10,000 changes, 16 MiB, or 50 ms. A batch
publishes its blocks' delivery changes in the same transaction. Blocks not
yet finalized, or that the live lane already applied or holds, commit one at a
time, as do the blocks of a batch whose reducer reads windowed output or
rewrites it unchanged. A source error or invalid frame ends a batch after the
blocks before it commit.
`target_writer_hold`, 250 ms by default, controls the adaptive block limit:
the runtime halves the current limit when observed p95 historical writer hold
time exceeds the target and cautiously grows it again below half the target.
A hold includes the commit's WAL sync, which a smaller batch does not shorten,
so on a disk whose syncs are slow a low target shrinks batches to a block or
two, and every commit rewrites the same pages for little data. On a Hetzner
EX44 (RAID 1 over two NVMe drives, about 8 ms per sync), the former 20 ms
target ran a `rollup-txs` backfill from Xatu at 53 blocks/s and wrote 104 KiB
per block; the 250 ms target ran it at 799 blocks/s and wrote 11 KiB per
block. A job holds at most `maximum_buffered_frames_per_acquisition` frames
of `[budgets.history_material]`, 128 by default, that it has not committed,
so a longer `maximum_delay` makes acquisition wait instead of filling larger
batches: a one-second delay ran the same backfill at 127 blocks/s. A single
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
unfinalized-block candidate bytes awaiting finality. A transaction that would exceed
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
networking, and unrelated processors remain available. The backfill
subscriptions of a replaced processor instance keep counting against the
history ceiling until they are deleted (see the runbook's
[Processor rebuild or rollback](/docs/operations/runbook/#processor-rebuild-or-rollback)).

## Sources and finality

`[[sources.history]]` requires `id`, `kind`, `priority`, and `trust`.
Supported kinds are `xatu`, `era_e`, `archive`, `json_rpc`, and `aws_public`.
`xatu` reads public Parquet history; validation refuses `parquet`, which no
source reads. `archive` requires `manifest`; `era_e` accepts an `https` or
`file` `endpoint` ending in `/`, and `aws_public` an `https` bucket mirror. A plain `http` endpoint is accepted on loopback, or elsewhere
only with `allow_insecure_http = true` on that source, which accepts a mirror
whose catalog and archive bytes anyone on the path can replace. `xatu`
sources are Ethereum mainnet only: another `chain.chain_id` fails
validation.

`aws_public` reads the chain entry's public dataset on AWS Open Data:
Ethereum mainnet from AWS's own export, about a day behind, and Base and
Arbitrum One from SonarX's, about a week behind. It serves headers, receipts,
and logs. Transaction bodies are left to other sources, because the exports
store values as floating-point numbers. Rows are unsorted within a day, so each
chunk downloads its whole UTC day: in October 2026 about 2 GB for Ethereum,
1.5 GB for Arbitrum One, and 9 GB for Base. That download counts against
`budgets.temporary_disk_bytes`, and a chunk whose rows would decode to more
than `budgets.memory_bytes` is refused before it downloads: an unfiltered
Ethereum day took about 8.7 GB of memory. `chunk_blocks` caps the blocks one
chunk holds, and each chunk reads its day again. With a log filter, only
blocks whose logs bloom admits it are decoded.

### History source order

A backfill splits its range into gaps of missing coverage and tries the
configured history sources for each gap in `priority` order, lowest first. A
source that fails on part of a gap passes only those blocks to the next
source: a dataset hole, from its first to its last block in the chunk read, or
the chunk whose read failed. It keeps the blocks before them, and the first
source takes the gap back after them. A source that has not published the
blocks, whose schema changed, or that fails again before committing a block
passes the rest of the gap instead, and one that cannot fill the processor's
field groups is skipped for the job.
Each backfill retries up to three times the number of sources, at least five
times, before it fails. When every history source fails and the live source is execution
P2P, P2P can serve the gap within `sources.live.history_fallback_blocks`.

The shipped profiles list eraE at priority 10 and Xatu at priority 20:

| Source | Serves | Checks each block for | Notes |
|---|---|---|---|
| `era_e` | Whole blocks and receipts up to its mirror's last catalog update | Transactions and receipts roots against the header | The public mirror's catalog ended at block 24,518,655 (2026-02-26) when this was written |
| `xatu` | Columnar projections, about one day behind the chain, until the day Glamsterdam activates | Transaction rows against the beacon payload count; log rows without a gap | Cannot prove a block's last logs complete; Ethereum mainnet only |
| `aws_public` (opt-in) | Headers, receipts, and logs, one UTC day per chunk | Receipts root and logs bloom against the header; the block hash, where the export has every header field (Base, Arbitrum One) | The export names the canonical chain; no transaction bodies |
| P2P (live source) | Recent and live blocks from peers | Commitments of every header, body, and receipt | Slowest for old history; peers may no longer serve very old blocks |

So for blocks the eraE mirror covers, a backfill reads verified complete
blocks; for later blocks, Xatu; and P2P fills what neither serves. Put Xatu
first when bandwidth, not completeness, limits a backfill: its projections
transfer fewer bytes per block for processors that read a few columns.

### Sizing for bandwidth, memory, and disk

Measured with this release on a Hetzner EX44 (Intel i5-13500, 20 threads,
64 GB, RAID 1 over two NVMe drives, 1 Gbit/s) that other workloads share,
25,000-block backfills with one consumer and `MALLOC_ARENA_MAX=2`:

| Backfill | Source | Downloaded per block | 4 reads, 2 chunks | 8 reads, 4 chunks |
|---|---|---|---|---|
| `rollup-txs`, Feb 2026 | eraE | 122 KiB | 250–300 blocks/s, 170 MiB | 250–330 blocks/s, 280 MiB |
| `blobs-money`, Feb 2026 | eraE | 122 KiB | 280 blocks/s, 160 MiB | 250–300 blocks/s, 250 MiB |
| `rollup-txs`, Feb 2026 | Xatu | 61 KiB | 550–560 blocks/s, 230 MiB | 730–780 blocks/s, 340 MiB |
| `blobs-money`, Feb 2026 | Xatu | 72 KiB | 410–420 blocks/s, 560 MiB | 500–570 blocks/s, 730 MiB |
| `rollup-txs`, Oct 2026 | Xatu | 56 KiB | 490 blocks/s, 290 MiB | 620–890 blocks/s, 420 MiB |
| `blobs-money`, Oct 2026 | Xatu | 46 KiB | 570 blocks/s, 420 MiB | 470–650 blocks/s, 620 MiB |

"4 reads, 2 chunks" is `source_concurrency = 4` and `maximum_active_chunks =
2`, as `deploy/container.toml` ships; the other shipped profiles read with 8
and keep the built-in 8 chunks. Each cell gives the speed and the peak
memory. Ranges span repeated runs, which differed by up to a third with the
archives' throughput and the shared host's load. With jemalloc, which the
container image preloads, peak memory stayed within a fifth of these and fell
back once a backfill finished. Execution P2P served 5,000 recent blocks at
43–55 blocks/s.

More reads and chunks made most Xatu backfills faster, at more memory, and
left eraE unchanged. An eraE backfill is limited by the frames it may hold
uncommitted: with `maximum_buffered_frames_per_acquisition = 512` and
`memory_bytes = "512MiB"` in `[budgets.history_material]`, eraE backfills ran
at 380–410 blocks/s, a quarter to a half faster, at the same peak memory.
Profiles:

- **Default:** the shipped profiles, which try eraE before Xatu and read up
  to 8 chunks at once with 8 reads each.
- **Faster eraE backfills:** set those two `[budgets.history_material]`
  values. Concurrent backfills share that budget, so each can then hold more
  of it.
- **Less memory:** set `maximum_active_chunks = 4`, the "8 reads, 4 chunks"
  column. Xatu backfills then hold about half as much, and execution P2P
  backfills fetch about half as much at once.
- **Limited memory**, such as a container under 1 GiB: use 4 reads and 2
  chunks. eraE needs the least memory.
- **Limited bandwidth:** a backfill downloads blocks per second times bytes
  per block, about 35 MiB/s from eraE at 300 blocks/s and 25–50 MiB/s from
  Xatu at the speeds above. On a slower link, the link sets the speed: at
  100 Mbit/s, about 12 MiB/s, eraE reaches about 100 blocks/s and Xatu
  170–270. There, list Xatu first for processors that read only
  transactions, such as `rollup-txs` and `transaction-stats`, and keep eraE
  first for processors that read logs, which Xatu cannot prove complete.
- **Slow disk syncs**, such as network-attached volumes: keep
  `target_writer_hold` at 250 ms or raise it. A lower target shrinks batches
  (see [Advanced root fields](#advanced-root-fields)).

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
It requires verified `beacon_api` or `consensus_p2p` finality, or `op_gossip`
on an op-stack chain. Stable
production ports normally use `listener_port = 30303`,
`discovery_port = 30303`, and `discv5_port = 30304`. Discv4 and Discv5
require distinct fixed UDP ports; zero selects an isolated ephemeral port for
that protocol. `stall_timeout_seconds`, ten minutes by default and six minutes
to 30 days, bounds how long the live lane may stay not ready while verified
finality is ready, within one run of the network lanes: after it, the node
exits with status 1 so that its process supervisor restarts it. A wait at the
recent-frame hard limit does not count.

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

On an op-stack chain, such as Base, `finality.kind = "op_gossip"` follows the
sequencer's signed block gossip and needs no checkpoint. Every block the
chain entry's sequencer key signed becomes the attested head the P2P live lane
follows, so blocks reach `attested` about as fast as `unattested`. Nothing
finalizes a block without an L1 anchor, so recent frames are pruned
`finality.settle_blocks` below the tip instead, about one hour of blocks when
omitted; settled blocks keep their level. Gossip peers find the node through
its discovery record: fixed, reachable `finality.listen_port` (TCP) and
`finality.discovery_port` (UDP) let them dial in, which Base needs because
its busy consensus nodes refuse new outbound connections. Peers whose
handshake head lies far behind the signed head are syncing and are dropped
while fewer than `body_serving_peer_target` peers serve the head.

### L1 anchor

A rollup's own rules decide when its blocks are final, and they run on
Ethereum. The opt-in `[l1_anchor]` table reads them: verified Ethereum
finality (`[l1_anchor.finality]`, `beacon_api` or `consensus_p2p`, as for an
Ethereum node) names finalized L1 blocks, execution P2P serves their receipts
checked against their roots, and the settlement contracts in the chain
entry's `[l1_anchor]` say which rollup block is final. Nothing is fetched from
an RPC endpoint.

- Nitro chains (Arbitrum One and Nova, Robinhood Chain): a confirmed BoLD
  assertion names its block hash; that block is `finalized` once this node
  holds it. Confirmation takes about 6.4 days. The endpoints' `finalized`
  tag no longer finalizes anything, and recent frames are pruned
  `finality.settle_blocks` (default 3,600) below the tip.
- Op-stack chains with output-root games (Base): a game of the respected type
  is checked when it is created against the output root of the block the
  sequencer signed, and that block is `finalized` once the anchor state
  registry moves to the game, about five days later on Base. OP Mainnet's
  games claim super roots, which the anchor does not read.

The lane reads the receipts of the finalized L1 blocks whose logs bloom may
hold the contracts' logs, at most about 1 GB a day. Base's game creations
also need their call data, so on Base it reads every block's receipts and
body, about 2 GB a day. Where
it stopped survives restarts (`l1-anchor-cursor.json`); a cold start reads
`lookback_blocks` (600 by default) below the first finalized block. Its
execution P2P uses its own ports, `listener_port`, `discovery_port`, and
`discv5_port`, and its own peer store.

## Processor identity and publication

Every modern entry declares:

```toml
[[processors]]
id = "evm-events"              # registered processor kind
instance = "weth-transfer-v1"  # stable operator-selected identity
version = "1.1.0"
start_block = 12965000
```

Processor code, semantic version, settings, start point, and source identity
are part of an instance's immutable identity. The node refuses a change to any
of them under an `instance` its store holds (`processor instance … conflicts
with its stored descriptor`), before it upgrades an older store, so it never
mutates an existing cursor namespace. Give the changed processor a new
`instance` ID: it indexes from its `start_block` beside the old instance,
whose rows stay in the store. Settle the old instance's backfill subscriptions
before and after the switch, as the runbook's
[Processor rebuild or rollback](/docs/operations/runbook/#processor-rebuild-or-rollback)
describes. Lifecycle policies, such as retention and delivery limits, are not
part of the identity and can change in place.

Publication values are:

- `unattested` (default): publish apply/undo/finality transitions from the
  first level the live lane delivers, and keep an unfinalized undo window.
- `attested`: publish apply/undo/finality transitions from `attested` on, and
  keep an unfinalized undo window.
- `finalized`: publish only finalized `apply` records. Unfinalized changes
  wait in the store for verified finality, count against the delivery budget
  meanwhile, and undo is never published.

`[processors.delivery] emit = "job_result"` replaces `publish` for a bounded
backfill: it publishes its terminal aggregate and then becomes
complete/reclaimable. Deletion remains explicit.

`[processors.settings]` belongs to the selected factory and is decoded
strictly. `doctor` constructs every configured processor, so malformed ABI,
address, pool, or custom settings fail before source or store I/O.

### Live gap fill

After downtime the live lane resumes at the newest verified finalized anchor,
so a block-local processor skips the blocks between its cursor and that
anchor. Automatic history refills them. With `history_mode = "on_demand"` the
lane only announces them, in a `system.live_gap.put` change, and whoever
requests history fills them. `[processors.live_gap_fill]` lets the lane fill a
short range itself:

```toml
[[processors]]
id = "blobs-money"
instance = "blobs-production"
history_control = "application_subscriptions"
history_mode = "on_demand"
# ...

[processors.delivery]
mode = "until_acknowledged"
on_limit = "pause"   # the default, and the only value a fill accepts
# ...

[processors.live_gap_fill]
max_blocks = 7200    # fill a skipped range of up to about a day; announce longer ones
```

Before the lane applies the block that follows a skipped range, live or
replayed at startup, it recovers the range from the processor's history
sources, 128 blocks at a time, and applies each block, oldest first, as a
finalized `apply` with `originKind: "live_recovery"`. Then it applies the
block itself. The live stream stays contiguous and in block order, announces
nothing for the range, and the processor's coverage holds the filled blocks as
finalized. The
[change protocol](/docs/reference/native-api-and-delivery/#42-operations)
describes what a consumer sees, and
[Keep your application in sync](/docs/guides/keep-your-application-in-sync/)
what an application still does.

`max_blocks` is required and takes 1 to 7,200, about a day of slots. The table
has no other setting. Validation refuses it for a processor that is not
`history_mode = "on_demand"`, because automatic history refills skipped
blocks, and for an ordered processor, such as `transaction-stats` or
`uniswap-latest`, which holds later blocks instead of skipping them. It also
requires `[processors.delivery] on_limit = "pause"`, which is the default:
validation refuses `fail` and `expire_and_reset`, because a fill's burst of
blocks could fail the lane, or reset its consumers, where announcing the
blocks would not. It is allowed under both `history_control` values.

The lane announces a range as it does without the table, and fetches nothing,
when the range is longer than `max_blocks`, when no history source serves
finalized blocks, or when the range is not finalized yet. The limit is all or
nothing, because blocks filled at the top of a range could not link to the
cursor. A fill that starts stops at the first block it cannot fill, and the
lane announces from there. That happens when history fails or serves too few
blocks; when a block is not finalized, does not link by parent hash to the one
before it, is not the canonical block at its height, or lacks material the
processor requires; and when the time limit passes. The limit is five
minutes and is fixed. The fill starts no request after it and gives up a
request still running at it, so a fill runs for at most five minutes plus
applying the blocks of its last request. A filled block is as trusted as the
same block delivered by a `fill_missing` subscription, which uses the same
sources and verification policy. The filled blocks link by parent hash from
the cursor's block through the range and, when the fill completes, to the
block that follows.

The sources are the processor's history sources, in the order a history
request tries them: retained raw history (`[raw_history]`, when enabled), the
configured `[[sources.history]]` by `priority`, and last the execution P2P
bridge, within `sources.live.history_fallback_blocks`. The bridge is available
to a fill during the replay that follows a restart, too. Bandwidth depends on
the source. A dataset source such as Xatu serves only the material the processor
needs, about a kilobyte per block for `blobs-money`. Execution P2P transfers
whole bodies and receipts, on the order of a megabyte per block on Mainnet
today, so a few thousand blocks are gigabytes. Configure a dataset source for
a processor that sets the table. Requests run one after another and do not
coalesce, so a fill is slower than a pipelined backfill subscription over the
same range. `sources.live.kind = "disabled"` leaves no live lane, so the table
does nothing, and `require_retained_input = true` limits a fill to retained
raw history, which usually does not hold the range: the lane announces it.

The live lane is shared. While a fill runs, it takes no other block: every
processor's live lane waits, the head that JSON-RPC reports stops advancing,
and WebSocket subscriptions publish nothing new. That happens after the live
lane was not running for a while, and `max_blocks` and the time limit bound
it. Several processors with the table fill one after another, and each
fetches the blocks again.

Filled blocks are ordinary delivery records under the processor's delivery
policy, and count against `delivery.max_bytes` and the node-wide
`[budgets.delivery]` and `[budgets.store]` limits. A lane that reaches one of
those limits mid-fill stops there. It holds the block after the range and
announces nothing. While the store would refuse that block, because the lane
is paused above its stream's resume mark (90 % of `max_bytes`), a node-wide
budget has no room for it, or the lane is paused at a storage limit while
storage stays above its low-water mark, the lane fetches no history for the
fill, as long as the block after the range is among the retained frames.
Once finality prunes that frame during a long hold, the lane recovers it from
history on every live block, as any paused lane recovers its first unapplied
block. After a restart of the live lanes, a lane held at a node-wide budget
can fetch once more before it waits again. The lane continues the fill once
the store would take the block: once consumers acknowledge and pruning brings
its stream down to the resume mark, once pruning any stream gives a node-wide
budget room, or once storage has room. The unfilled rest is recomputed from
the processor's cursor on resume, so a history commit that an application's
subscription lands above the cursor meanwhile can shorten what the lane fills
or announces. That is rare, and idempotent writes and the application's
coverage diff cover it. A single filled block larger than the stream's
`max_bytes` fails the lane, as an equally large live block would: raise
`max_bytes` or remove the table. A fill also makes a pause at a delivery or
storage limit more likely, and a held block's delta counts against the shared
`[budgets] pending_delta_bytes`; when it does not fit, the lane fails
(`live_gap_marker_exceeds_pending_delta_budget`) until the budget is raised and
the lane reset. The
[runbook](/docs/operations/runbook/#crash-or-interrupted-commit) lists the log
lines and the recovery.

The table is not part of the instance's identity, so add, change, or remove it
in place, as with the lifecycle policies below. A release that predates the
table refuses a configuration that has it, so remove it before rolling back.

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
publish only finalized blocks, set `publish = "finalized"`.

How much `latest` keeps depends on the processor's keys, not on the mode: an
ordered aggregate such as `transaction-stats` writes one key and keeps one
row, a per-pool processor keeps one row per pool, and a block-local processor
such as `evm-events` usually writes new keys every block, one per event, so
under `latest` its output grows as under `full`. `leani doctor` warns about
`latest` on a block-local processor; use `window` to bound such output.

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
enqueue/acknowledgement time, not Ethereum block timestamps, so the changes a
backfill enqueues count as new.

A pass deletes at most `maximum_delete_changes`. A pass that deletes that many
is followed at once by the next, and a stream that pauses at its limit is
pruned at once, without waiting for `interval`.

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
`unattested` and `attested` publication require `unfinalized`. Unfinalized blocks always
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

When upgrading a relative `data_dir`, Leani refuses to start if its former
working-directory location contains a database or embedded subscriptions
while the location beside the configuration holds none: it would otherwise
start on an empty store. Move the old state, or set an absolute path to
select the intended store. When both locations hold state, it uses the one
beside the configuration and warns, naming both. Equivalent paths and
symlink aliases of the same directory count as one.

### Node-side Wasm builds

`api.allow_wasm_builds = true` lets the node compile processor source sent to
`POST /admin/v1/wasm/build`, which the playground's editor uses; the compact
profile takes it under `[api]` too. It is off by default and a development
feature, not for production: rustc can read the host's files while it
compiles, and only a denylist of source constructs guards it. With it on,
the node builds Rust only for requests from its own machine, an SSH tunnel
included, and TypeScript for any client the API admits; it logs a warning
when it starts, and `leani doctor` reports one. A request relayed with a
`Forwarded`, `X-Forwarded-For`, `X-Forwarded-Host`, or `X-Real-IP` header, or
addressed to another host name, is not the node's own, but behind a reverse
proxy on the same machine that sets none of them, a remote client's request
looks local. In production, build modules where you write them, or in CI, and
configure the module file with its `blake3` hash.

### Historical CLI and node backfill

`leani backfill --processor INSTANCE --from-block N --to-block M` uses the
same history source assembly, configured pipeline, material coordinator and
verified P2P fallback as node jobs. `--processor` defaults to the
configuration's only processor. When live P2P is enabled, the standalone command
verifies the configured finality anchor and opens its own persistent peer
pool. It checks the finalized upper bound and honors
`sources.live.history_fallback_blocks`. No live listeners or processors are
started. Disabled P2P and retained-input-only policies remain in force.

To use a running node and its existing peer pool:

```bash
leani backfill --endpoint http://127.0.0.1:18080 \
  --processor INSTANCE --from-block N --to-block M
```

The command sends an authenticated materialization request and waits for its
status. `LEANI_API_TOKEN` supplies the token without putting it in shell
arguments. The token travels in an `Authorization` header, so across hosts use
HTTPS, for example through a TLS-terminating reverse proxy; plain HTTP suits
loopback or a private container network, and the command warns when it sends
the token over plain HTTP beyond loopback. An optional endpoint path prefix is preserved. Ctrl-C cancels
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
| `max_outbound_bytes` | `64MiB` | queued notifications of all WebSocket connections; when full, the connection holding the most is closed with `1013` |
| `max_subscription_event_bytes` | `16MiB` | a WebSocket connection whose subscriptions produce more notification bytes for one chain event is closed with `1008`, and one that leaves more unread with `1013` |

A closed WebSocket connection releases its slot and its subscriptions.
Notifications are queued one at a time, so while earlier ones are still
unsent, a connection can be closed with `1013` before one event's
notifications pass the limit that would close it with `1008`.
Each connection holds at most `max_subscription_event_bytes` of queued
notifications and one response to a call; it reads its next call only once
that response is sent, so a slow reader slows only itself. A connection that
keeps more than a quarter of its notification room queued for 30 seconds is
closed with `1013`: it reads slower than its subscriptions produce. All
WebSocket listeners also share `max_outbound_bytes` of queued or sending
notifications (64 MiB by default). When that runs out, the connection
holding the most of it is closed with `1013`, not the one that notifies
next; responses never use it. A send may take 30 seconds plus the time its
payload needs at 64 KiB/s before the connection is closed. These are
encoded-payload limits, not process RSS limits: at the defaults, responses
add at most `max_websocket_connections` × `max_response_bytes`.
`max_outbound_bytes` must be at least `max_subscription_event_bytes` and at
most 4,294,967,295 bytes.

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
