---
title: History and live data sources
description: Understand how Leani selects, verifies, and combines archive, RPC, Beacon API, and native P2P sources.
section: concepts
order: 10
audience:
  - operator
  - processor-author
status: preview
---

Schema rechecked against the upstream Xatu schema on 2026-08-04. Public
availability remains operational evidence and must still be probed before a
large backfill.

## 1. Recommended source portfolio

| Source | Role | Strength | Main limitation |
|---|---|---|---|
| Xatu public Parquet | Default specialized backfill | Free, canonical, columnar, rich tables, filter/project before retaining | Donated hosting; filtered rows are trusted data; Ethereum mainnet only |
| EraE archives | General raw historical source | Sparse native reader; complete block bodies and receipts with locally checked execution commitments | The mirror and its catalog remain the canonicality trust root; receipts only from Byzantium |
| Ethereum P2P/Reth | Recent and live | No vendor key, follows head, bodies/receipts | Old history is thinning; consensus finality remains separate |
| Beacon light client/CL | Canonicality/finality | Anchors post-Merge execution payloads | Adds a consensus component |
| AWS public blockchain | Secondary Parquet source | Free public S3, daily updates | Validate schema completeness for each fork/use case |
| Google Blockchain Analytics | Development/fallback SQL | Broad tables and easy SQL | Hosted dependency; first 1 TB/month free, then paid |
| Portal Network | Future decentralized history/state | Designed for low-resource history access | Completeness/maturity must be tested |
| JSON-RPC | Development/rare fallback | Broad compatibility | Reintroduces cost/trust; never default core |

## 2. Xatu

[Xatu Data](https://github.com/ethpandaops/xatu-data) is currently the strongest first backend.

Properties:

- public Parquet is available without access to EthPandaOps' private ClickHouse;
- canonical execution tables are partitioned in 1,000-block chunks;
- canonical beacon tables are generally partitioned daily;
- the repository is CC BY 4.0;
- files can be queried directly or read through Arrow/ClickHouse/DataFusion.

Relevant canonical execution tables include:

- blocks and transactions;
- receipts-derived logs;
- traces;
- contracts and address appearances;
- balance diffs and reads;
- ERC-20 and ERC-721 transfers;
- native transfers;
- nonce and storage diffs/reads.

Relevant canonical beacon tables include:

- execution block hash/number/base fee;
- blob gas used and excess blob gas;
- execution transactions with type, sender, blob gas fee cap, and blob hashes;
- blob sidecars;
- withdrawals.

The implemented `XatuHistorySource` plans four fail-closed physical
projections:

1. headers from minimal execution/beacon block columns;
2. transactions with sender, recipient, exact value, calldata, nonce, gas
   limit, and type;
3. logs with address and all four topic positions; and
4. the existing blob/type-3 projection.

Xatu history is Ethereum mainnet only. Its projections stamp every frame as
chain 1, so the adapter refuses any Xatu network but `mainnet`, and
configuration validation refuses a `xatu` history source on another chain.

The current Xatu projections require Beacon execution payloads for parent
hashes, so Mainnet requests must begin at block **15,537,394** (the Merge) or
later. Earlier ranges fail during planning. A processor deployment/start block
before the Merge does not make that range available from this adapter; use a
source that explicitly advertises the required historical range.

Parquet reads fetch only the byte ranges of the projected columns. Ranges
at most 1 MiB apart are merged into one request, which also downloads the
bytes between them, while the input budget covers those bytes; no larger gap
is downloaded. Every requested byte, footers and merged gaps included, counts
against the source budget's `max_input_bytes` for the whole chunk, and is
charged before it is requested. A chunk's `fetched_bytes` reports those
bytes, and its `projected_compressed_bytes` the selected column chunks alone.
The reader holds one row group's selected columns at a time, so an object
whose largest such row group exceeds the budget's `max_resident_bytes` is
refused before any column is requested.
At most the source budget's `max_in_flight_requests` requests are in flight
per object, and the node sets it from `budgets.source_concurrency`.

Every request carries `If-Match` with the ETag the object had at HEAD, so an
object rewritten during a read fails the chunk, which is retried against the
new version, instead of mixing two versions. `If-Match` never matches a weak
ETag, so an object without a strong ETag is refused as a schema drift: a
backfill tries its next configured source, and fails when none can serve the
range; `leani source probe xatu` reports each object's ETag and flags such
objects. A range response of another length, and a footer whose metadata
range exceeds 64 MiB, are refused too. Public hosting can still retry or
stall; source availability and throughput need dated operational evidence.

Xatu publishes each partition some time after it closes, so the object for
the newest blocks may not exist yet, and its reads get 404 Not Found. The
source reports such a chunk as a range it does not cover, as eraE reports a
range its catalog does not list yet, not as an unavailable source: a job
moves on to its next configured source, and the node logs
`xatu has not published this object yet` with the object's location at
debug level. Only a 404 is reported this way; other store failures, such as
a 5xx response or a timeout, are not.

Every immutable chunk encodes its projection kind and exact filters. `open`
therefore cannot execute a different physical projection from the one that
was planned. The source plan reports logical reader, table, selected columns,
predicates, trust, derived status, and completeness. Unsupported combinations
fail before object metadata or Parquet data is downloaded.

Transaction aggregates select `canonical_execution_transaction` without
receipts. Declarative events select `canonical_execution_logs` without beacon
transactions, withdrawals, or blob fields. Both read the minimal execution
and beacon block columns needed for timestamps, canonical hash, and parent
identity.

Each projected column has a declared encoding that fixes the Arrow types it
may arrive as, and a column of any other type fails the object before a row
is decoded, as a schema drift, which a backfill answers with its next
configured source. Hashes, addresses, and byte strings are `0x`-prefixed
hexadecimal text, or raw bytes only in a fixed-width binary column of exactly
their size.

The projections check what the dataset declares:

- a transaction projection that no filter narrows must hold every
  transaction the beacon payload counts, with indices `0..count`; a filtered
  one may be shorter, but never longer or outside the payload;
- the blobs projection proves every block's withdrawals, on which its
  execution size depends, from the global withdrawal index: between two slots
  with rows the indices must continue exactly, so a slot between them
  without rows had none. The nearest slots with rows before and after the
  chunk, in the daily objects it reads, prove its edges the same way, and a
  slot with 16 rows, the most a payload holds, lacks none. An edge they do
  not prove, a jump in the indices, such as a partly exported slot, or more
  than 16 rows in one payload leaves the chunk incomplete. Xatu exports no
  withdrawals root or count per block to check against;
- logs may carry no topics (`LOG0`), whether the export leaves `topic0`
  null, empty, or NUL-padded;
- blob receipts carry no logs, so blob frames declare their logs a partial
  projection, and a log requirement cannot be met through those receipts.

Source-native transfer/trace/state-diff readers remain optional breadth work;
their stronger derived-table semantics must be explicit before they can
satisfy a processor capability.

For blobs.money, combine:

```text
canonical_beacon_block
canonical_beacon_block_execution_transaction
canonical_beacon_block_withdrawal
canonical_execution_block
canonical_execution_transaction
```

and use canonical execution logs only if rollup attribution later needs emitted events.

Trust note: Xatu's canonical tables are derived by an execution pipeline. Checksums verify downloaded files, not Ethereum execution. Full receipts can be checked against a receipt root; filtered transfer/log/state-diff rows cannot prove their completeness in isolation.

Sources:

- [Xatu repository and Parquet examples](https://github.com/ethpandaops/xatu-data)
- [Canonical beacon schema](https://github.com/ethpandaops/xatu-data/blob/master/schema/canonical_beacon_.md)
- [Canonical execution schema](https://github.com/ethpandaops/xatu-data/blob/master/schema/canonical_execution_.md)

## 3. EraE

[EthPandaOps history endpoints](https://ethpandaops.io/data/history/) describe EraE as complete execution-layer history from genesis, covering pre- and post-Merge blocks and intended for client bootstrapping/verification.

Implemented usage:

1. fetch and validate the checksum catalog;
2. read the e2store version and dynamic block index with bounded range requests;
3. fetch only the selected compressed headers, bodies, and receipts;
4. decode requested components and verify their transaction, receipt, ommer, withdrawal, and logs-bloom commitments;
5. map configured processors;
6. discard archive bytes;
7. checkpoint and continue.

This gives bounded memory/network use, needs no temporary archive file, and
avoids relying on old execution P2P peers. The reader supports HTTPS mirrors,
local `file:` catalogs, and plain `http` mirrors on loopback, or elsewhere
only with `allow_insecure_http = true` on the history source. It never
follows a redirect. Compressed input windows are independent of the decoded
frame buffer: buffering four frames does not limit an HTTP range to four
blocks. A chunk starts with up to 32 blocks, then acquires up to 256 blocks
per window, targeting at most 16 MiB of compressed input. Adjacent components
share a byte-range request, and independent ranges are fetched concurrently
up to the source's in-flight request limit. When the resident budget permits,
the next window downloads while the current one is decoded and consumed:

- each batch's byte ranges are checked against the remaining input budget
  before they are requested;
- the current and prefetched compressed windows together must fit the
  resident budget; smaller budgets disable lookahead or shrink a window,
  and one large block may exceed the 16 MiB target but never the budget;
- a block that exceeds the remaining prefetch room is read sequentially after
  the current compressed window is released;
- the checksum catalog is read with a 16 MiB cap, and each range response
  with a cap at its requested length, as the bodies stream in;
- an e2store record may not declare more bytes than its indexed extent
  holds;
- a block's compressed header, body, and receipt list are decompressed
  through one shared cap, the frame budget, at most 32 MiB;
- each normalized frame is checked against the frame budget before it is
  queued, as the other sources check theirs.

Snappy decoding, commitment checks, and normalization run on blocking
workers so they do not stall the async network workers. Dropping a stream
cancels its downloads and signals its decoding worker.
Transaction requests use Alloy consensus's native `secp256k1` recovery
backend, retaining its signature validation checks.

Header-only requests read only headers. Body requests verify the complete
body without recovering senders unless transactions or a sender predicate
require them. Acquired bodies also retain complete withdrawals for execution
RPC and raw-history consumers, even without an explicit withdrawal request.
Log-only requests can read headers and receipts without
transaction bodies when neither transaction hashes nor transaction filters
are needed. Receipt roots and logs blooms are still checked over every
receipt before log filtering. Requested transaction, receipt, and log
predicates are applied before material is built for downstream processors;
transaction and log indices retain their original positions. A request with
`allow_filtered = false` receives complete requested material.

Errors, logs, and frame provenance show the mirror as
`scheme://host[:port]/…/<file>`, so credentials in its userinfo, query, or
path are not printed.

EraE serves headers and bodies from genesis, but receipts and logs only from
Byzantium (block 4,370,000). Earlier receipts carry an intermediate state
root instead of a status, which frames cannot represent, so a plan that needs
receipts or logs before Byzantium fails with an error that says so.

The checksum catalog is cached for 10 minutes. A plan that asks past it
fetches it again once the catalog is 30 seconds old; each refetch that still
lacks the range doubles that interval, up to the 10 minutes, and a refetch
that brings new eras starts it over. Each refetch sends the `ETag` and
`Last-Modified` the mirror last sent, so an unchanged catalog costs a 304. A
chunk already planned opens from the cached catalog whatever its age.
Validated dynamic indexes are shared by concurrent chunks and kept in an
eight-entry cache keyed by object filename and catalog SHA-256. Compressed
archive data is retained only for active streams.

The direct mainnet catalog was populated on 2026-08-01 even though the rendered
history webpage's generated counter was stale. A live sparse probe decoded and
verified post-Dencun block `19,426,589` from a 967 MB source object, retaining
111,181 bytes of normalized frame data. Catalog continuity, mirror behavior,
and sustained throughput still require release evidence.

A later conformance run verified all 300 blocks in
`19,426,589..=19,426,888` (123,856,697 normalized in-memory bytes) without
retaining the 967 MB EraE object. Exporting canonical byte arrays as pretty
JSON was intentionally not retained: it expanded past 2 GB. Operational
backfills map and commit bounded frames directly; raw JSON is a diagnostic
format, not a storage format.

The mirror and its checksum catalog are the trust root. The catalog
advertises each full object's SHA-256; sparse mode records it as the
object's version, not as a checksum, because it never downloads the whole
object to recompute it. Local commitment checks establish internal Ethereum
execution consistency for acquired components: transaction, receipt, ommer,
withdrawal, and logs-bloom commitments, and the parent hashes within an
object. Checks for omitted bodies or receipts are reported as `NotChecked`.
The block
hash is computed from the archived header itself, so frames report
`header_hash` as not checked, and `withdrawals_root` as unavailable before
Shanghai. Canonicality follows the mirror's catalog until it is anchored
through independently verified consensus material.

### EraE versus Xatu

Xatu should usually win for a known aggregation:

- it is columnar and already split into logs, transfers, transactions, traces, and state-diff tables;
- the reader can project and filter before most bytes enter the processor;
- it can provide executed artifacts that do not exist in raw receipts;
- it should make blobs, token-transfer, and Uniswap backfills substantially faster.

EraE should win when reproducibility and raw protocol coverage matter:

- it provides block bodies and receipts rather than a provider-defined analytical projection;
- transaction and receipt roots can be recomputed from complete block inputs;
- a new event processor can be run without waiting for a public dataset to expose a new table or field;
- files can theoretically be mirrored by multiple independent hosts;
- it is a useful audit source for sampled or full Xatu ranges.

EraE still does not contain EVM call traces or state diffs, and it normally requires downloading far more data than a filtered Xatu query. “Independent history path” therefore means an interchangeable raw fallback and verification path, not that EraE must be the default or that its current host is trustless.

## 4. Ethereum P2P, Reth, and SHiNode

[SHiNode](https://github.com/vicnaum/shinode) demonstrates that Reth/devp2p can retrieve headers, transaction bodies, receipts, and logs without EVM state and follow the head.

SHiNode context from the research snapshot:

- Rust, MIT/Apache-2.0;
- current documented release v0.3.0;
- claims over 1,000 blocks/second and roughly six hours for full Mainnet on a modern SSD;
- stores roughly 250 GB in events-only mode or under 800 GB with transactions;
- implements `eth_chainId`, `eth_blockNumber`, limited `eth_getBlockByNumber`, and `eth_getLogs`;
- its own transaction receipt RPC and WebSocket subscriptions were planned,
  not implemented at the snapshot date;
- calldata is not currently retained;
- receipt verification and consensus-layer integration are planned;
- its README explicitly warns against production use;
- it already recognizes that EIP-4444 makes pure P2P history unreliable and lists archive/Portal support as future work.

Leani should assess SHiNode as a source implementation, not inherit its storage policy. Our P2P path should map and discard old raw material rather than sealing permanent event shards.

SHiNode is active enough to be valuable upstream work, but its small maintainer base and production warning mean the fork must be independently tested and audited.

The preferred implementation boundary has changed since SHiNode's original
design. [Reth 2.2](https://github.com/paradigmxyz/reth/releases) exposes a
`ReceiptsClient` trait and P2P receipt downloading. Reth also documents
individually usable crates, including networking APIs, eth-wire messages, and
downloaders in its
[repository layout](https://github.com/paradigmxyz/reth/blob/main/docs/repo/layout.md).

Therefore:

1. build `source-p2p` directly on the smallest upstream Reth crate set;
2. keep Reth types and version churn inside that adapter;
3. use SHiNode as a reference for scheduling, peer behavior, and failure tests;
4. contribute a missing general hook upstream where practical;
5. use a narrow isolated fork only if the boundary spike proves it necessary.

This avoids adopting SHiNode's permanent event-shard store, incomplete RPC
surface, and release cadence while preserving the important stateless-history
idea.

The independent node now implements exact receipt/block RPC reconstruction,
post-commit WebSocket subscriptions, and a bounded recent raw-material window;
the bullets above describe SHiNode, not this repository.

## 5. P2P history expiry

[EIP-4444](https://eips.ethereum.org/EIPS/eip-4444) specifies that clients should not serve block bodies and receipts older than one year over execution P2P and may prune them. The [Ethereum Foundation's partial history expiry announcement](https://blog.ethereum.org/2025/07/08/partial-history-exp) states that execution clients support removing pre-Merge history.

Consequences:

- P2P is the best live/recent input, not the only historical source.
- A full-genesis P2P sync may work opportunistically but is not a dependable product contract.
- Archive files, public datasets, Portal, and independent mirrors must be interchangeable.

Leani therefore places its finalized execution-P2P history adapter after
retained history, Xatu, EraE, and configured archives. By default it may plan
every gap at or after the processor's configured start block. An explicitly
configured `history_fallback_blocks` value limits that eligibility to a suffix
ending at the finalized anchor. It verifies the complete header ancestry to
that anchor, checks body and receipt commitments, streams only
processor-requested normalized fields, and resumes material acquisition from
durable processor coverage.
Live requests share the manager but have dispatch priority over queued history
requests.

An on-demand job keeps the sources it starts with. A job whose range ends in
the eligible blocks therefore does not start until the node's finalized P2P
anchor covers its last block, typically seconds after the node starts; without
`history_fallback_blocks`, that is every job. It waits two minutes at most: a
node without such an anchor by then, for example while verified finality is
unavailable, starts the job without the P2P fallback, where a recent range
fails visibly. A job that ends before the eligible suffix starts at once.

This is a resilient fallback, not a cheaper archive transport. A recent
receipt-complete block can carry hundreds of kilobytes before devp2p Snappy,
and public peers may be full, slow, or missing old history. Keep the persistent
node identity and scored peer cache across restarts; use archive sources for
predictable large backfills and P2P when those sources lag or fail.

## 6. Portal Network

The [Portal Network documentation](https://ethereum.org/developers/docs/networking-layer/portal-network/) targets lightweight access to:

- beacon light-client data;
- headers;
- block bodies;
- receipts;
- eventually state and transaction lookup.

It is a natural future backend because its goal aligns with this project. It should be introduced after measuring:

- content availability for random historical blocks;
- retrieval throughput for sequential backfills;
- proof/canonicality semantics;
- operational reliability;
- Rust client integration.

Portal is not the MVP's only source.

## 7. AWS public blockchain data

[AWS Public Blockchain Data](https://registry.opendata.aws/aws-public-blockchain/) provides free, date-partitioned Parquet in public S3 and states that new data is delivered daily.

Use it as:

- an independent comparison source;
- a fallback for conventional block/transaction/log/trace data;
- a potential secondary mirror.

Before using it for a processor, inspect a current partition rather than assuming its schema has every recent fork field. Earlier inspection for this project found blob-specific coverage less complete than Xatu.

## 8. Google Blockchain Analytics and Ethereum ETL

[Google Blockchain Analytics](https://docs.cloud.google.com/blockchain-analytics/docs) exposes Ethereum blocks, transactions, receipts, logs, decoded events, token transfers, traces, and account-state-oriented tables in BigQuery. [Pricing](https://cloud.google.com/blockchain-analytics/pricing) follows BigQuery and includes the first 1 TB of processed query data per month.

It is useful for:

- SQL validation;
- one-off research;
- filling a missing backend capability;
- cross-source correctness tests.

It is not the fully free/self-sovereign production core.

[Ethereum ETL](https://github.com/blockchain-etl/ethereum-etl) is extraction/transformation tooling, not itself a free source of Ethereum history. Its schema and decoders are useful references, but running it traditionally still requires a node or RPC. The project now documents blob header and transaction fields, so its schemas can help normalize alternative datasets.

## 9. Firehose, Substreams, and subgraphs

[Firehose data flow](https://firehose.streamingfast.io/architecture/data-flow) accepts protobuf blocks from a reader process and serves a cursor-based historical/live stream. [Firehose storage](https://firehose.streamingfast.io/firehose/architecture/data-storage) normally retains one-block and merged 100-block files. Substreams adds parallel Wasm transformation and module-output caches.

Relevance:

- useful protobuf/cursor conventions;
- potential ecosystem compatibility;
- a SHiNode/EraE adapter can theoretically emit `FIRE BLOCK`;
- event-only processors need only headers/transactions/receipts/logs.

Conflict:

- normal Firehose durability keeps raw chain data;
- Ethereum Firehose blocks often include call traces and state changes unavailable from receipt-only sources;
- running all Firehose/Substreams services is operationally heavier than the desired default.

The project should implement a compatibility experiment after the core normalized stream works, not start by deploying a full Firehose stack.

Traditional subgraphs are another possible processor/output compatibility layer. Event handlers and topic filters fit the receipt-only core. Subgraphs that use contract calls do not: [The Graph's documentation](https://thegraph.com/docs/en/subgraphs/developing/creating/advanced/) shows that mappings can declare `eth_call`, which requires EVM state at the indexed block. A future adapter could emit entity/database changes or feed an event-only Graph mapping, but “subgraph compatible” must carry the same capability restrictions as Substreams.

## 10. Source-selection policy

For each requested range:

1. filter sources by required capabilities;
2. reject sources whose lag/finality violates the processor policy;
3. prefer projection/predicate pushdown for specialized backfills;
4. prefer root-verifiable full data when trust policy requires it;
5. minimize expected downloaded bytes, then latency;
6. schedule an overlapping source at transitions;
7. record source and verification per committed range;
8. fail over at chunk boundaries when possible.

Live execution data and finality are planned separately. A Reth P2P peer can
supply a valid body and receipts for a branch; it does not by itself prove the
post-Merge canonical/finalized branch. The initial finality adapter consumes
verified beacon light-client updates through an untrusted Beacon API endpoint.
The self-contained profile instead discovers consensus peers directly and
uses the standard light-client bootstrap/update/finality req/resp protocols.
Both transports share the same local proof verifier and explicit
weak-subjectivity checkpoint boundary.

A raw-history job retries a range that only lagging sources lack, those with
a non-zero expected lag, such as an EraE catalog that has not caught up. It
keeps their reasons in the job's `last_error` while it waits, until it
commits a segment, and fails once it has gone without progress for four
times the longest expected lag, and at least 24 hours. That time counts from
the newest segment the job retained, or else from its creation, both kept in
the catalog, so restarting the node does not start it again; a new job under
the same ID counts from its own creation. A transport failure, such as an
unreachable mirror, also keeps the job waiting, for at most 72 hours without
progress, or the lag bound when that is longer; then the job fails with the
reasons in `last_error`. Those hours include time the node was down or the
job paused at its storage limit, so either bound fails a job only once the
running node has itself seen the range fail for an hour without committing
a segment; after a restart the sources get that hour again, and frames a
source delivers without a commit do not restart it. A source whose
advertised range does not cover the
range, such as a local archive of older blocks, is passed over. A range
missing inside the advertised range of a source without expected lag, such as
a gap in a local archive, fails the job at once, and so does a terminal
error, such as a schema drift, from any source that could serve it, which
another source's lag no longer hides.

Retained raw history is itself a source. The node verifies a segment's
whole-file BLAKE3 checksum once per process, when it writes the segment or
when it first reads it after a restart, and each block read checks its own
record's checksum against the segment's seek directory; segment reads never
run on the async runtime's threads. A segment that fails either check leaves
the catalog, so its blocks read as missing and another source serves them;
its file moves to `raw-history/quarantine/`. Each raw-history job that owned
it records why in `last_error`, and acquires the blocks again: a running job
next, and a completed one once it is back in the queue. A segment whose file
is missing when the node starts leaves the catalog the same way, unless most
files are missing, which the node takes as a segment directory not yet
available and keeps their rows; a file that disappears while the node runs
fails its reads, which fall back to other sources, until the file returns. A
job adopting retained segments skips one whose file is gone, and removes its
row once nothing owns it.
Jobs whose ranges overlap share segments: before each acquisition a job
adopts the compatible segments other jobs retained, and a job that publishes
a segment another job has just published adopts that one, once it verifies,
instead of failing.

An operator must be able to choose:

- `fast/trusted`: Xatu filtered tables;
- `verify/full`: EraE or full P2P receipts;
- `independent`: multiple sources and sampled/full cross-checking.
