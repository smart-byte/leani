---
title: blobs.money integration case study
description: See how a real application splits historical indexing, live following, query serving, and fallback responsibilities.
section: guides
order: 30
audience:
  - app-developer
  - operator
status: preview
---

The blobs domain is the first processor, correctness corpus, and performance
benchmark for Leani. blobs.money now contains a native sidecar consumer,
application-owned PostgreSQL gap reconciliation, a local compatibility RPC
path, and indexed WETH/USDC price consumption. This is pre-launch integration,
not a completed production cutover: shadow parity, failure recovery, staging,
and rollback evidence remain required before removing the managed-provider
fallback.

## 1. Why it is the right first processor

The current blobs.money backend needs block headers, full type-3 transaction envelopes, and transaction receipts. Its persistent product data does not require arbitrary contract state, internal traces, or EVM execution.

The retained legacy RPC path uses a managed provider for:

- live block-number notifications over WebSocket;
- `eth_getBlockByNumber` with transactions;
- `eth_getBlockReceipts`;
- occasional individual transaction receipt fallback;
- current block number and recovery/catch-up;
- a separate `eth_config` call for current/future blob parameters.

Relevant legacy paths live in the
[blobs.money repository](https://github.com/smart-byte/blobs.money):

- `api/src/rpc/fetch.ts`
- `api/src/rpc/client.ts`
- `api/src/monitoring/websocket.ts`
- `api/src/monitoring/process.ts`
- `api/src/history/worker.ts`
- `api/src/transform/blocks.ts`
- `api/src/db/schema.ts`

## 2. Inputs and outputs

### Input mapping

| blobs.money input | Candidate historical source | Candidate live source |
|---|---|---|
| number/hash/parent/timestamp | Xatu beacon/execution block | P2P header + CL |
| size, gas used/limit, base fee | Xatu execution/beacon block plus tx sizes and withdrawals | P2P header/body |
| blob gas used/excess blob gas | Xatu canonical beacon block | P2P post-Dencun header |
| tx hash/from/type/blob hashes | Xatu beacon execution tx | P2P block body |
| tx gas used/status/blob gas | Xatu canonical execution tx | P2P receipts |
| finalized canonical block | Xatu canonical tables | beacon finality |
| blob fork schedule | checked-in exact chain config | same local schedule |

Xatu's `block_total_bytes` is consensus-block size and is retained only as
`consensus_size_bytes`. The execution `size` used by blobs.money is derived as
the exact fork-aware RLP length from execution header fields, transaction
encodings, and withdrawals. Unknown future header formats fail closed.

### Existing useful outputs

`blocks`:

- number/hash/timestamp/size;
- blob count/gas/excess/base fee;
- execution base fee/gas used/gas limit/burn;
- blob burn and reserve fee;
- transaction count;
- target/max blobs and transform version.

`blob_transactions`:

- transaction hash;
- block number;
- sender;
- blob count;
- execution, blob, and total burn.

The processor stores source-neutral blobs entities in Leani's embedded
output store and exposes them through the v1 API/change stream. A comparison
adapter maps those values into the shapes of the existing `blocks` and
`blob_transactions` tables for parity testing. It does not write the
application database during the core build.

## 3. Historical processor

Implemented flow:

1. Read canonical beacon blocks for Dencun-to-target dates.
2. Read beacon execution transactions, filtered to type 3 or non-empty blob hashes.
3. Read canonical execution transactions for matching hashes to obtain gas used and success.
4. Normalize into `BlobsBlockInput`.
5. Run the existing tested fee formulas or port them exactly to Rust.
6. Apply the node's embedded blobs block and transaction entities.
7. Store a range checkpoint with source URLs/checksums and processor version.
8. Discard downloaded/streamed Parquet material.

No blob contents or sidecars are required for current blobs.money metrics. Sidecars remain an optional processor capability.

## 4. Live processor

For each new execution block:

1. receive/observe the candidate header;
2. fetch the full body and receipts over P2P;
3. verify header hash, transaction root, receipt root, and parent;
4. map type-3 transactions;
5. apply the blobs output transaction atomically;
6. publish an optimistic update;
7. retain raw material and undo through the reorg window;
8. mark output finalized when the consensus source finalizes it;
9. prune raw material/undo according to policy.

The node replaces the current WebSocket block notification plus subsequent HTTP block/receipt calls with one internally coordinated stream.

## 5. Hot/cold handoff

Example startup:

```text
t0:
  target finalized block = B
  start P2P capture at B - 128
  start Xatu backfill at Dencun

t1:
  Xatu mapper/reducer reaches B - 128
  compare hashes over overlap

t2:
  consume buffered P2P deltas through current head
  switch reducer to live
```

Leani is tested independently through its API, SDK, RPC, and exported parity
reports. The blobs.money repository owns the separate PostgreSQL transaction,
range-reconciliation, deployment, and rollback concerns.

There are two integration paths:

1. **Compatibility RPC:** point the current ethers/fetch and WebSocket code at
   the node's local JSON-RPC. This proves method parity with minimal application
   change but still transports and transforms raw blocks in blobs.money.
2. **Native consumer:** consume already-derived atomic blobs block events,
   commit application rows, and then advance the Leani-owned durable
   acknowledgement. Replays across that boundary are idempotent.

The second path is implemented and is the intended primary integration. The
first remains a compatibility, development, and emergency-fallback path until
production evidence supports a cutover. blobs.money currently carries a local
client; adopting the published `@leani/sdk` should happen once that package is
released and its contract is identical.

## 6. `eth_config`

The compatibility path can use `eth_config` to discover current/future blob
schedules. This traffic is tiny compared with blocks and receipts.

The node now maintains a versioned Mainnet schedule with exact activation
timestamps/blocks and fork hashes, and serves the final EIP-7910 response
locally. Future changes must be reconciled against protocol announcements and
client chain specs before release. The native blobs processor carries the
checked schedule in its versioned contract.

## 7. Reference validation and production cutover

Before production cutover, compare Leani output with a read-only export of the
existing pipeline:

- compare every `blocks` field;
- compare every blob transaction and sender;
- investigate current rows with missing/fallback receipts;
- compare rollup attribution;
- verify no gap at restart and source handoff;
- inject reorg fixtures;
- record source lag and finality.

Use these cutover stages:

1. shadow writes to comparison tables;
2. report differences without serving new data;
3. make leani the historical backfill source;
4. validate the local JSON-RPC compatibility path;
5. run the native SDK stream into shadow tables;
6. make the SDK stream the live primary; add an application-owned PostgreSQL
   adapter only if the integration proves it necessary;
7. disable Alchemy block/receipt/head usage;
8. keep emergency fallback explicit, metered, and off by default;
9. remove `full_blocks` only in a separately approved recoverable migration.

## 8. Acceptance criteria

- Full Dencun-to-head backfill without paid RPC.
- Exact parity with the current formulas and schema over the agreed range.
- No permanent raw block/receipt storage.
- Live optimistic lag normally within two blocks.
- Finalized cursor exposed separately.
- Restart, source outage, and shallow reorg tested.
- Native API, SDK, JSON-RPC, and blobs.money integration fixtures pass.
- Generic Bun SDK consumer can resume from a cursor and handle
  apply/undo/finality.

Production integration has separate acceptance criteria:

- production shadow parity;
- rollback tested;
- Alchemy CU attributable to block, receipt, and head monitoring reduced to
  zero in normal operation;
- remaining external requests itemized;
- any `full_blocks` removal explicitly approved and recoverable.
