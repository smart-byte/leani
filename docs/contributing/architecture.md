---
title: Architecture
description: Learn Leani's invariants, component boundaries, normalized data model, planning, lanes, storage classes, and reorg model.
section: contributing
order: 10
audience:
  - contributor
  - processor-author
status: preview
---

## 1. Terminology

**Protocol state** is Ethereum account/storage/code state required to execute the EVM. The core node does not retain it.

**Index state** is application-defined state such as balances for watched addresses, the latest Uniswap pool price, or blobs.money aggregates. This is the storage the operator intentionally chooses.

**Raw history** is block headers, bodies, transactions, receipts, logs, traces, and state diffs before projection.

**Delta** is the compact deterministic result of mapping one canonical block for one processor.

**Checkpoint** records a processor version, canonical block hash, state cursor, and source/verification metadata.

## 2. Architectural invariants

1. Processors consume a source-neutral representation.
2. Missing capabilities cause an error, never silently empty fields.
3. Raw historical material is transient unless retention is explicitly enabled.
4. State transitions are applied in canonical block order.
5. Every included state transition is reversible until finalized.
6. Historical and live lanes overlap and verify before handoff.
7. Outputs carry provenance, processor version, and a canonical cursor.
8. Source trust and cryptographic verification are different dimensions.
9. Resource budgets are enforced in bytes, not only in block counts.
10. An RPC response is served only when its completeness semantics are known.

## 3. Component view

```text
                         +----------------------+
                         | Processor manifest   |
                         | filters/capabilities |
                         | retention/finality   |
                         +----------+-----------+
                                    |
                                    v
+--------------+   +-------------------------------+   +----------------+
| Xatu/Parquet |-->|                               |<--| Ethereum P2P   |
| EraE         |-->| Source planner + coordinators |<--| CL finality    |
| AWS/Google   |-->|                               |<--| optional RPC   |
+--------------+   +---------------+---------------+   +----------------+
                                    |
                                    v
                         +----------------------+
                         | Normalizer           |
                         | BlockEnvelope        |
                         | capabilities/trust   |
                         +----------+-----------+
                                    |
                                    v
                         +----------------------+
                         | Verifier + fork graph|
                         +----------+-----------+
                                    |
                  +-----------------+-----------------+
                  |                                   |
                  v                                   v
       +----------------------+             +----------------------+
       | Parallel mappers     |             | Recent raw window    |
       | compact BlockDelta   |             | recent RPC/reorg     |
       +----------+-----------+             +----------------------+
                  |
                  v
       +----------------------+
       | Ordered reducers     |
       | apply/undo/checkpoint|
       +----------+-----------+
                  |
        +---------+----------+----------------+
        |                    |                |
        v                    v                v
 +-------------+      +-------------+   +-------------+
 | OutputStore |      | ChangeStream|   | RPC facade  |
 | SQL/embedded|      | HTTP/SSE    |   | JSON-RPC    |
 +-------------+      +-------------+   +-------------+
```

## 4. Source contracts

A source must declare rather than imply what it can provide.

```rust
struct SourceDescriptor {
    id: String,
    kind: SourceKind,
    range: BlockRange,
    capabilities: CapabilitySet,
    trust: TrustModel,
    finality: FinalityModel,
    partitioning: Partitioning,
    expected_lag: Duration,
}

trait HistorySource {
    fn describe(&self) -> SourceDescriptor;
    async fn plan(&self, request: RangeRequest) -> Vec<SourceChunk>;
    async fn stream(&self, chunk: SourceChunk) -> Stream<BlockEnvelope>;
}

trait LiveSource {
    fn describe(&self) -> SourceDescriptor;
    async fn subscribe(&self, from: Cursor) -> Stream<ChainEvent>;
}
```

Initial capabilities:

```text
HEADER
BODY
TRANSACTIONS
CALLDATA
RECEIPTS
LOGS
WITHDRAWALS
BLOB_SIDECARS
TRACES
STATE_DIFFS
CONSENSUS_FINALITY
MEMPOOL
```

Some fields can be derived from stronger capabilities. For example, `LOGS` can be decoded from full `RECEIPTS`. Derived capabilities retain the provenance of the source material.

## 5. Normalized data

The normalized envelope should be field-oriented and cheaply projectable. It should not force every source to create a trace-rich Ethereum Firehose block.

```text
BlockEnvelope
  chain_id
  block_ref
    number
    hash
    parent_hash
    timestamp
  finality
    included | finalized
  header?
  body?
  transactions?
  receipts?
  logs?
  withdrawals?
  blob_sidecars?
  traces?
  state_diffs?
  capabilities
  provenance[]
  verification
```

`verification` should be structured:

```text
header_hash       verified | failed | not_checked
parent_continuity verified | failed | not_checked
transactions_root verified | failed | unavailable
receipts_root     verified | failed | unavailable
consensus_anchor  finalized | included | unavailable
dataset_checksum  verified | failed | unavailable
```

Filtered Xatu logs may be canonical and operationally trustworthy but cannot be independently checked for completeness without all receipts. That is represented as `trusted_dataset`, not `receipts_root=verified`.

## 6. Planning and pushdown

Processor manifests are compiled into a `DataRequest`:

```text
block range
required capabilities
addresses
topic filters
transaction types
fields
finality policy
```

Backends push down whatever they can:

- Xatu reads only required Parquet tables/columns and filters address/topics.
- A predecoded transfer table can bypass general log decoding.
- EraE reads complete bodies and receipts but only emits selected normalized fields.
- P2P retrieves complete bodies and/or receipts when the requirement needs
  them. Predicate-complete log-only requests use verified header blooms to
  skip definite negatives, fetch whole-block receipts for positives, and
  exact-filter logs. Bodies are omitted when the processor's declared log
  identity does not include transaction hash.

The planner may combine sources within one processor. Example:

- beacon Parquet for blob header/transaction fields;
- canonical execution transaction Parquet for gas used/status;
- P2P for the current head.

At source boundaries it requests an overlap and compares block hashes and, where possible, roots.

### Architecture payoff: work follows processor requirements

The processor descriptor controls both acquisition and output. A narrowly
filtered processor can request less source material, perform less mapping, and
publish fewer bytes without changing the source, coordinator, delivery, or ACK
contracts.

The blobs transform emits a block delta for every block and validates and
joins projected transactions and receipts while calculating blob fee/burn
fields. The Uniswap transform scans only matching pool logs and emits nothing
for blocks without a matching observation. Equal block ranges therefore do
not imply equal input, compute, or output work.

The built-in benchmark command can compare these shapes across acquisition,
processing, durable delivery, and SDK/PostgreSQL destinations. Its generated
reports are local artifacts and must retain machine, source, commit,
configuration, and cold/warm-run context when results are shared.

## 7. Cold and hot lanes

### Cold lane

The cold lane:

1. snapshots a target block near the current finalized head;
2. plans historical chunks;
3. downloads/streams chunks with bounded concurrency;
4. maps blocks in parallel;
5. writes compact deltas tagged by block hash;
6. lets the reducer apply them in order;
7. discards the source chunk after verification and map completion.

### Hot lane

The hot lane starts immediately:

1. observes head/fork/finality events;
2. retrieves block bodies and receipts;
3. validates and maps them;
4. retains recent raw envelopes for RPC/reorg;
5. retains compact deltas until the cold reducer catches up;
6. optionally publishes block-local provisional results.

### Joining

The lanes must overlap by at least the configured reorg window or a smaller finalized overlap:

```text
cold: ... A B C D E
hot:          C D E F G H ...
                   ^
              verified handoff
```

The scheduler deduplicates by `(chain_id, block_number, block_hash, processor_version)`. A block number alone is never an identity.

### Stateful outputs while backfilling

There are three valid modes:

1. **Block-local:** publish hot outputs immediately because history is irrelevant.
2. **Checkpoint-seeded:** load a verified aggregate checkpoint and continue at the head.
3. **Catch-up:** capture hot deltas, mark the aggregate incomplete, and publish complete state only when the ordered reducer reaches the head.

The runtime must not present an empty-seeded token balance as a complete current balance.

## 8. Processor model

The first processor API should be deliberately small:

```rust
trait Processor {
    fn descriptor(&self) -> ProcessorDescriptor;
    fn map_block(&self, block: &BlockView) -> Result<BlockDelta>;
}

trait Reducer {
    fn apply(
        &self,
        tx: &mut ReducerTransaction,
        cursor: Cursor,
        delta: BlockDelta,
    ) -> Result<OutputChanges>;
}
```

`ReducerTransaction` is a core-owned namespaced state/index API. It captures
preimages and constructs undo automatically, then atomically commits state,
changes, cursor/coverage, provenance, and sink outbox rows. A native processor
does not receive a raw SQLite transaction.

Separating map from reduce enables:

- parallel historical decode/filter work;
- canonical ordered state;
- small live catch-up buffers;
- reuse of map output across multiple sinks;
- deterministic source equivalence tests.

Processor descriptors include:

- semantic version and code hash;
- start block;
- required capabilities;
- address/topic filters;
- output schema;
- current-only, bucketed, or full-history retention;
- included/finalized publication policy;
- whether deltas are commutative or strictly ordered.

The concrete first-release modes are:

- `block_local`: block N cannot depend on prior output, so hot and historical
  coverage can be filled concurrently;
- `ordered_state`: mapping is parallel but one reducer applies blocks in order.

The blobs.money processor is block-local. ERC-20 balances and Uniswap pool
state are ordered-state processors.

## 9. Storage classes

### Mandatory

- processor state chosen by the application;
- committed cursor and processor version;
- source/checksum/provenance metadata;
- unfinalized undo records;
- bounded hot deltas while cold reduction catches up.

### Bounded

- recent raw block/receipt window;
- one or a few Parquet/EraE chunks;
- mapper work queue;
- response cache for on-demand RPC;
- checkpoints.

### Optional and potentially large

- complete log history;
- all token-holder balances;
- per-transfer balance history;
- transaction hash to block locator;
- raw block/receipt history;
- Firehose merged blocks;
- Substreams module caches.

Default pruning rules:

1. discard mapped historical input immediately;
2. prune raw live envelopes after finality plus the configured RPC window;
3. prune undo records only after finality;
4. keep checkpoints according to processor policy;
5. expose byte use by storage class and processor.

## 10. Reorg and finality model

Post-Merge canonicality is anchored to consensus safe/finalized execution payloads. A future pre-Merge/genesis profile must additionally validate canonical total difficulty or an appropriate history accumulator/checkpoint; parent and trie-root validity alone is insufficient to select the canonical proof-of-work branch.

Every included block application is one atomic transaction:

```text
validate parent
persist delta
apply output changes
persist undo
advance included cursor
commit
publish apply event
```

On reorg:

```text
identify common ancestor
undo old branch in reverse order
publish undo events
apply new branch in forward order
publish apply events
```

When consensus finality advances:

- mark matching outputs finalized;
- publish finality events;
- remove undo/raw data older than policy;
- detect and halt on any finalized-chain contradiction.

An operator may choose finalized-only output for simpler downstream semantics at the cost of roughly two epochs of latency.

## 11. Lean processing runtime and ecosystem adapters

Firehose provides a useful cursor/block transport and ecosystem format. Its normal architecture also persists one-block and merged 100-block files, while Substreams caches module outputs. Permanent raw storage conflicts with this project's default.

The project can implement the parts needed for its use cases directly:

1. canonical, fork-aware historical/live block coordination;
2. capability-aware source planning and filter pushdown;
3. deterministic per-block mapping;
4. canonical ordered state reduction;
5. transactional apply/undo and finality;
6. checkpoints and resumable cursors;
7. output queries and subscriptions;
8. pluggable database sinks.

This is much smaller than recreating distributed Firehose and Substreams. The core does not need a permanent raw-block object store, tier-1/tier-2 workers, shared module caches, or the ability to execute arbitrary third-party packages.

Compatibility should be layered rather than built into the internal model:

1. **Output compatibility:** optionally emit standard database/entity change protobufs. This is the cheapest and may enable existing sinks.
2. **Firehose reader compatibility:** adapt a complete enough `BlockEnvelope` into `FIRE INIT`/`FIRE BLOCK`, allowing an operator to run upstream Firecore and accept its storage model.
3. **Firehose gRPC compatibility:** implement the downstream historical/live service. This is more work because cursor, fork, and retention behavior must match.
4. **Substreams package compatibility:** execute event-only `.spkg` modules. This is the most expensive layer because it requires the Wasm host API, module DAG/store semantics, protobuf versions, parameters, caches, and package validation.

Proposed compatibility path:

1. keep the internal envelope minimal and capability-aware;
2. use versioned protobufs and deterministic cursor/apply/undo semantics internally;
3. keep processors independent of storage and transport;
4. implement native Rust processors first;
5. experiment with standard output-change protobufs and a Firehose reader adapter;
6. populate an Ethereum `sf.ethereum.type.v2.Block` only with fields genuinely available;
7. test known event-only Substreams packages;
8. reject packages that require calls, traces, or state changes;
9. adopt a Substreams package runtime only if real reusable packages justify its complexity.

Full compatibility is not an MVP dependency. The architecture should make adapters possible, but it should not make `sf.ethereum.type.v2.Block`, Firehose cursor internals, or the Substreams Wasm ABI the project's foundational representation. Ethereum packages can reasonably assume richer executed-block data than receipts provide.

## 12. Example manifest

Illustrative only:

```toml
manifest_version = 1

[chain]
name = "ethereum-mainnet"
chain_id = 1

[history]
backends = ["xatu", "erae"]
start_block = 19426589

[live]
backend = "p2p"
finality_source = "beacon-api"
reorg_window_blocks = 128

[budgets]
temporary_disk = "5 GiB"
recent_raw = "1 GiB"
memory = "1 GiB"

[[processors]]
id = "blobs-money"
version = "0.1.0"
requires = ["HEADER", "TRANSACTIONS", "RECEIPTS"]
retention = "full-output-history"
publish = "included_and_finalized"

[processors.sink]
kind = "embedded"

[[sinks]]
kind = "postgres"
processor = "blobs-money"
enabled = false

[rpc]
recent_window_blocks = 128
historical_mode = "on-demand"
transaction_locator_index = false
```
