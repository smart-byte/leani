---
title: Ethereum JSON-RPC compatibility
description: See exactly which Ethereum JSON-RPC methods Leani implements, refuses, or exposes only through native APIs.
section: reference
order: 60
audience:
  - app-developer
  - operator
status: preview
---

Status: metadata, bounded recent-material HTTP, and live WebSocket profiles
are implemented. Canonical header/transaction/receipt bytes are decoded into
Alloy RPC types. Bounded on-demand number/range queries are implemented for
configured sources that declare every required capability complete.

## 1. Product promise

Leani can replace a paid RPC for the common methods its configured sources can
answer exactly. It is not a full execution/archive node.

Three answer paths exist:

- **Recent:** serve from the rolling raw block/receipt cache.
- **On-demand:** fetch the relevant historical chunk from Xatu/EraE/another backend, answer slowly, and discard it.
- **Materialized:** serve from an operator-enabled durable index.

The response should expose a non-standard diagnostic header or companion method indicating source, finality, and whether an answer was recent, on-demand, or materialized.

## 2. Method matrix

The currently shipped `leani-evm-free-v1` profile implements
`web3_clientVersion`, `net_version`, `eth_chainId`, `eth_blockNumber`,
`eth_syncing`, `eth_config`, and `leani_getCapabilities` over HTTP,
including JSON-RPC batches and notifications. The WebSocket endpoint also
accepts regular calls and implements `newHeads` and filtered `logs`
subscriptions from post-commit live events. Methods needing raw blocks,
receipts, or logs return the explicit server error `-32004` until that
material is present; state/EVM methods remain unsupported. The capability
response is the machine-readable authority for a running node.

`eth_config` follows final EIP-7910: it accepts no parameters and returns
`current`, `next`, and `last` fork configurations with JSON-number blob
parameters, chain ID, fork hash, precompiles, and system contracts. Its
checked Mainnet schedule uses exact activation timestamps; it does not call an
upstream configuration RPC. `current` is the fork active at the timestamp of
the newest retained raw canonical block. This timestamp can be unavailable
even when `eth_blockNumber` knows a newer canonical anchor.
Forks activate by timestamp, so a node that retains no head frame, such as an
empty one or one that has only backfilled, fails with `-32004`
(`eth_config_head_timestamp_unavailable`) instead of choosing a fork by block
number or taking the newest one. A head older than the first scheduled fork
fails with `eth_config_head_predates_schedule`, and a node whose chain the
schedule is not for fails with `eth_config_chain_schedule_unavailable`.

Blob transaction receipts report `blobGasPrice` as the protocol blob base fee
of the fork active at the block's timestamp, from the same schedule and fee
function as the `blobs-money` processor. Receipts are built for a whole block,
so when that price cannot be computed, every receipt of a block with a blob
transaction fails with `-32004`, the receipts of its other transactions
included, from `eth_getBlockReceipts` and `eth_getTransactionReceipt` alike,
rather than carrying a guessed price. `error.data.reason` says why:

- `blob_gas_price_schedule_unavailable`: no scheduled fork covers the block's
  timestamp, or the schedule is for another chain. A node on a chain other
  than Mainnet without a blobs processor uses the checked Mainnet schedule, so
  the receipts of every block with a blob transaction fail this way.
- `header_excess_blob_gas_missing`: the block header has no excess blob gas.
- `quantity_exceeds_u128`: the price does not fit in 128 bits. Only an excess
  blob gas that no valid chain reaches, as a forged header may claim, gets
  there.

`eth_getLogs` can use Xatu's predicate-complete projection under the
trusted-dataset policy. Every requested block and required log field must be
present; a partial projection, a narrower address/topic scope, or a broken
parent link fails explicitly. Complete-cryptographic policy still requires
complete material. Retained raw-log projections can serve the same requests
without upstream access when their scope and fields cover the query. Decoded
processor entities alone are not a lossless raw-log store: enable on-demand
history or retain suitable raw material for historical RPC.

`eth_blockNumber`, `latest`, and `eth_syncing.currentBlock` report the
canonical tip, independently of raw-frame pruning, or the highest block the
progress processor has finalized when that is higher or no tip is known, as
on a node without live following. Finalized blocks are canonical, so the
head never names a block a reorg could remove. A node that knows no block
reports `0x0`, as a fresh Ethereum client does; `eth_blockNumber` never
fails for want of a head. A reported head can lag the network, while
disconnected or on a historical-only node; consult `eth_syncing`, which
reports `liveReady: false` then, before treating it as the network tip.

For a number/range request outside the recent window, the router selects the
lowest-priority viable `HistorySource`, enforces finality/trust and hard
byte/frame/concurrency/timeout limits, validates ordered parent-linked frames,
and fails over only at the whole-request boundary. Frames are discarded after
serialization. A range crossing into the recent window must have an exact
parent-hash handoff. Hash-only transaction/receipt history still requires a
durable locator and therefore remains recent-only by default.

| Method | EVM-free support | Default behavior | Caveat |
|---|---|---|---|
| `web3_clientVersion` | Exact | Local | — |
| `net_version` | Exact | Local | — |
| `eth_chainId` | Exact | Local | — |
| `eth_blockNumber` | Exact for the known canonical chain | Canonical tip or finalized coverage | `0x0` before any block is known; never an unfinalized processor cursor |
| `eth_syncing` | Exact for this node | Canonical head and live readiness | A disconnected or historical-only node does not claim live readiness |
| `eth_getBlockByNumber` | Exact with body source | Recent/on-demand | Historical latency depends on backend |
| `eth_getBlockByHash` | Exact with locator | Recent; optional index | Hash-to-chunk lookup otherwise expensive |
| `eth_getBlockTransactionCount*` | Exact with body | Recent/on-demand | — |
| `eth_getTransactionByBlock*` | Exact with body | Recent/on-demand | — |
| `eth_getTransactionByHash` | Exact with locator | Recent; optional index | Global tx locator consumes storage |
| `eth_getTransactionReceipt` | Exact with receipt locator | Recent; optional index | Same lookup issue |
| `eth_getBlockReceipts` | Exact with receipts | Recent/on-demand by block | Excellent fit for P2P/EraE |
| `eth_getLogs` | Exact with complete logs | Recent/on-demand/materialized | Filtered datasets are trusted for completeness |
| `eth_feeHistory` | Not shipped | Explicit unavailable error | Requires a separately tested client-compatible policy |
| `eth_gasPrice` | Planned | Unsupported | Client policy semantics differ |
| `eth_maxPriorityFeePerGas` | Planned | Unsupported | Client policy semantics differ |
| `eth_subscribe(newHeads)` | Exact | Live | Replacement heads are emitted after branch commit |
| `eth_subscribe(logs)` | Exact | Live | Reorged logs emit `removed: true` before replacements |
| filter polling methods | Feasible | Later | Stateful server-side filters |
| `eth_sendRawTransaction` | Feasible transport | Optional | P2P broadcast or fallback, not indexing core |
| `eth_getBalance` | Not general | Unsupported | Requires executed state, including internal effects |
| `eth_getTransactionCount` | Not general | Unsupported | Contract nonces/internal creation require execution |
| `eth_getCode` | No | Unsupported | Requires state |
| `eth_getStorageAt` | No | Unsupported | Requires state |
| `eth_call` | No | Unsupported/optional forward | Requires historical code/storage and EVM |
| `eth_estimateGas` | No | Unsupported/optional forward | Requires EVM |
| debug/trace methods | No from receipts | Unsupported | May be served only by a trace-capable dataset/backend |

“On-demand” cannot make every hash-only lookup cheap. Block-number/range queries map naturally to chunked archives. Transaction-hash queries need a locator service or a compact local `tx_hash -> block_number` index.

### Requests, block tags, and missing blocks

A call whose `id` is `null` gets a response with a `null` ID; only a call
without an `id` member is a notification. An `id` that is an object, an
array, or a boolean gets `-32600` with a `null` ID. A call must be a JSON
object: an array in its place, such as `["2.0", 1, "eth_chainId"]`, gets
`-32600` with a `null` ID too.

A notification runs like any other call, side effects included, and gets
no response, as JSON-RPC 2.0 requires: an `eth_unsubscribe` notification
ends its subscription, and an `eth_subscribe` notification opens one whose
ID the client never learns. A batch answers only its calls with IDs, and a
batch of only notifications gets no response at all (HTTP 204).

Block numbers and transaction indexes are QUANTITY values: `0x` and one or
more hexadecimal digits, without leading zeros except in `0x0`, at most 64
bits. Anything else, such as `0x+1`, `0x01`, or `0x`, gets `-32602`.

Wherever a block number is accepted, including `fromBlock` and `toBlock` of
`eth_getLogs`, these tags are too:

| Tag | Block |
|---|---|
| `latest` | the head `eth_blockNumber` reports |
| `finalized` | the newest canonical block the node has verified as finalized |
| `safe` | the same block as `finalized` |
| `earliest` | block 0 |
| `pending` | none: Leani builds no pending block, so `-32004` (`pending_block_unavailable`) |

Until the node has verified a finalized block, `finalized` and `safe` fail
with `-32004` (`finalized_block_unavailable`). Leani does not track the
justified checkpoint that execution clients call safe, so `safe` names the
finalized head, which is never newer. A client that waits for `safe` blocks
to avoid reorgs therefore stays safe, but it sees an older block than other
clients report.

Block lookups return `null` only for a number above every block the node
knows exists: its canonical tip and the progress processor's indexed height. Ethereum clients read `null` as "no such block", and a
reorg checker concludes that a block which turned `null` was reorged out.
So a block at or below the head that neither the recent window nor
on-demand history serves fails with `-32004` instead: `block_not_retained`,
or the history source's reason when history was asked. A block hash that
the recent window does not hold fails with `block_not_retained` unless
on-demand history offers a block-hash lookup, in which case `null` means
that lookup does not know the hash. This applies to `eth_getBlockByNumber`,
`eth_getBlockByHash`, `eth_getBlockReceipts`,
`eth_getBlockTransactionCountBy*`, and
`eth_getTransactionByBlock*AndIndex`, which still returns `null` for an
index past the last transaction of a block it serves. `latest` resolves the
canonical tip and may fetch its raw material on demand; without a known tip
it fails with `block_not_retained`. `earliest` names block 0, so it requires
retained or historical material for block 0.

`eth_getLogs` with `blockHash` returns only logs of the block with that
hash. If a reorg replaces that block between resolving the hash and reading
the block, the call fails with `-32004` rather than return the
replacement's logs, as it does once the reorg has happened
(`block_hash_not_retained`). An empty alternatives array at a topic
position, `[]`, matches any topic there, as `null` does.

### Transport rules and limits

HTTP calls need `content-type: application/json`, with any parameters, and
get HTTP 415 otherwise. A browser `Origin` other than a loopback one
(`http(s)://localhost`, `127.0.0.1`, or `[::1]` on any port) or one listed
in `rpc.allowed_origins` gets HTTP 403, on HTTP calls and WebSocket upgrades
alike; requests without an `Origin` header pass. A cross-site page therefore
cannot post a `text/plain` batch or open a WebSocket to a loopback node.

A request body that is not valid UTF-8 JSON gets HTTP 200 with a `-32700`
parse error and a `null` ID. A body over 1 MiB gets HTTP 413 with a JSON-RPC
error, not plain text: `-32005` with a `null` ID, and `error.data` holding
`reason` `request_size_limit_exceeded` and the `limit` in bytes.

| Limit | Default | Setting | When exceeded |
|---|---|---|---|
| Request body or WebSocket message | 1 MiB | — | HTTP 413 with a `-32005` error; a longer WebSocket message ends the connection |
| Calls per batch | 100 | `rpc.max_batch_requests` | one `-32600` error object for the whole batch |
| Response bytes | 16 MiB | `rpc.max_response_bytes` | `-32005` for the call that passes the limit, or whose `eth_getLogs` result would, and for every later call in the batch, which do not run; later notifications still run |
| `eth_getLogs` results | 10,000 | `rpc.max_log_results` | `-32005` "query returned more than 10000 results. Try with this block range [0x…, 0x…]." |
| Filter addresses | 1,000 | `rpc.max_log_addresses` | `-32602` |
| Alternatives per topic position | 1,000 | `rpc.max_log_topic_alternatives` | `-32602` |
| Subscriptions per WebSocket connection | 128 | `rpc.max_subscriptions_per_connection` | `-32005` from `eth_subscribe` |
| WebSocket connections | 256 | `rpc.max_websocket_connections` | HTTP 503 on the upgrade |
| Subscription notifications of one chain event, per connection | 16 MiB | `rpc.max_subscription_event_bytes` | the connection is closed with code `1008` |
| Unsent subscription notifications, per connection | 16 MiB | `rpc.max_subscription_event_bytes` | the connection is closed with code `1013` |
| Queued or sending notifications, all WebSocket connections | 64 MiB | `rpc.max_outbound_bytes` | the connection holding the most is closed with code `1013` |
| Notifications kept queued past a quarter of `rpc.max_subscription_event_bytes`, per connection | 30 s | — | the connection is closed with code `1013` |

The `eth_getLogs` hint names the range from `fromBlock` up to the block
before the one where the results passed the limit, and `error.data` carries
it as `from`, `to`, and `limit`. The hint is missing when the first block
alone has more matching logs than the limit; read that block with
`eth_getBlockReceipts` or raise the limit. The filter limits also apply to
`eth_subscribe("logs")` filters. Closing a WebSocket connection frees its
slot and ends its subscriptions.

The subscriptions of one WebSocket connection share an output budget per
chain event: the notifications that one block, or one reorg with its
removed and replacement logs, produces for the connection may take at most
`rpc.max_subscription_event_bytes` encoded bytes. The node serializes a log
or head once, however many of the connection's subscriptions it matches,
but each notification counts in full. A connection whose subscriptions
pass the budget is closed with code `1008` (policy violation), without that
event's notifications, and with a close reason that names the setting. Its
subscriptions would pass the budget again on a like block, so reconnect
with fewer or narrower subscriptions, or spread them over several
connections.

The node also holds at most `rpc.max_subscription_event_bytes` of a
connection's notifications unsent. A client that reads more slowly than
its subscriptions produce notifications is closed with code `1013` (try
again later), as it is when it falls behind the node's bounded event
channel, and may reconnect. Notifications are queued one at a time, so while
earlier ones are still unsent, a connection can get `1013` before one event's
notifications pass the budget that would close it with `1008`. Notifications
still unsent when a connection is closed are dropped, and a connection that
cannot take its close frame within a second is dropped without one. Other
connections are unaffected.
A call's response is sent before the connection's next message is read,
so a client that stops reading stops being answered too.

## 3. RPC profiles

### Minimal

- chain metadata;
- current head;
- recent blocks, transactions, receipts, logs;
- recent fee history;
- new-head and log subscriptions;
- custom aggregate queries/subscriptions.

Storage is aggregate output plus a small recent window.

### Indexer

Minimal plus:

- historical `eth_getLogs`;
- block-number-based historical blocks/receipts;
- configurable log materialization;
- longer recent window.

Historical answers may be slow unless their data is materialized.

### RPC-heavy

Indexer plus:

- transaction locator;
- transaction/receipt index;
- larger raw window;
- response cache;
- optional forwarding for state methods.

This profile intentionally uses more storage.

## 4. ERC-20 balance semantics

### What can be derived

For a token where every balance change produces a standard:

```solidity
Transfer(address indexed from, address indexed to, uint256 value)
```

the node can maintain:

```text
balance[token, from] -= value
balance[token, to]   += value
```

with zero-address handling for mint and burn. Logs from reverted transactions do not appear in the canonical receipt and therefore do not change the derived balance.

### What can break the model

- rebasing or reflection changes balances without per-holder `Transfer` events;
- deliberately non-standard tokens;
- unusual proxy upgrades that change balance semantics;
- tokens whose historical starting state predates the selected start block;
- metadata changes or contracts that revert on metadata calls;
- migrations that create balances outside normal transfer events.

The API should call this an `event-derived token balance`, include the processed block, and expose its assumptions. “Declared” means that the processor manifest and query result declare:

- the chain, token/address scope, and starting block;
- the derivation method, such as `erc20_transfer_ledger`;
- the highest processed and finalized blocks;
- whether historical backfill and the live handoff are complete;
- the token behavior assumptions under which the value is exact.

For a conventional ERC-20 indexed from before its initial mint, this value should be exact. The declaration exists to distinguish that result from generic EVM state for rebasing or non-standard tokens. The node should not transparently intercept arbitrary `eth_call(balanceOf)` and claim universal equivalence.

Illustrative response metadata:

```json
{
  "token": "0x...",
  "address": "0x...",
  "balance": "123000000",
  "asOfBlock": 25620123,
  "finalizedThrough": 25620064,
  "method": "erc20_transfer_ledger",
  "coverageFrom": 1234567,
  "complete": true
}
```

A custom `idx_getTokenBalances` method should be the canonical API. An optional `alchemy_getTokenBalances` compatibility adapter could be added for migrations, but it must return only the declared indexed coverage and completeness metadata.

### Storage modes

| Mode | Retained state | New arbitrary address |
|---|---|---|
| Address watchlist | Only watched address/token pairs | Requires targeted backfill |
| Token allowlist | All holders for selected tokens | Immediate within allowed tokens |
| Global current ledger | All non-zero token/address pairs | Immediate, larger database |
| Full balance history | Every change/checkpoint | Largest; opt-in |

For minimal deployments, watchlist mode is the default. Xatu's predecoded ERC-20 transfer Parquet table makes a targeted historical backfill possible without querying a paid RPC. The live source still sees all receipt logs but retains only relevant deltas.

Adding a watch address should be a first-class job:

1. atomically register address and start cursor;
2. capture new live deltas for it immediately;
3. backfill historical incoming/outgoing transfers;
4. reconcile at an overlapping block;
5. mark the derived balance complete.

This is the same cold/hot handoff problem in miniature.

### Historical balance retention

Current balance requires one row per retained `(token, address)`. Historical `balance at block N` requires more:

- every transfer delta;
- periodic snapshots plus later deltas;
- or re-running a public-data query on demand.

The default should keep current balances and a small undo window. Historical balance queries can be slower and replay public transfer data unless explicitly materialized.

## 5. Native ETH balances

Native balances are not equivalent to top-level transaction value transfers. They also change through:

- gas charges and priority fees;
- internal calls;
- contract creation;
- withdrawals;
- protocol rewards in older history;
- other execution-level balance changes.

Xatu currently publishes `canonical_execution_balance_diffs` and native transfer tables, so it can act as a trusted historical executed-state source. That does not solve low-latency live native balances through receipt-only P2P: generating complete live balance diffs requires EVM execution or another executed-data provider.

Therefore:

- `idx_getNativeBalance` may be offered when a `STATE_DIFFS` backend is configured and sufficiently current;
- standard `eth_getBalance` remains unsupported until the node has an exact state source;
- responses must report backend lag and trust.

## 6. NFT balances

- ERC-721 ownership is generally derivable from `Transfer`.
- ERC-1155 requires both `TransferSingle` and `TransferBatch`.
- The same non-standard/rebase caveats and retention modes apply.

These should follow ERC-20 support rather than complicate the first implementation.

## 7. Aggregate subscriptions

Native aggregate subscriptions should use durable cursors rather than
transient WebSocket connection state:

```text
processor_id
processor_version
block_number
block_hash
change_sequence
finality
```

Event kinds:

- `apply`
- `undo`
- `finalized`
- `reset_required`
- `processor_complete`

Clients resume from a cursor. If the node has pruned the required change history, it returns `reset_required` with the latest queryable checkpoint.

The v1 native transport is HTTP query plus SSE, with a TypeScript SDK that
handles reconnect and opaque cursors. Standard `eth_subscribe` remains
WebSocket JSON-RPC and follows Ethereum's subscription semantics. See
[native API and delivery protocol](https://leani.dev/docs/reference/native-api-and-delivery/).
