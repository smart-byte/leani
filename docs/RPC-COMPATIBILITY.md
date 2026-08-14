# Leani RPC compatibility and derived balances

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
checked Mainnet schedule uses exact activation timestamps and execution
blocks; it does not call an upstream configuration RPC.

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
| `eth_blockNumber` | Exact | Live source | Defines optimistic head |
| `eth_syncing` | Exact for this node | Local progress | Report both cold reducer and hot source |
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
[API-SDK.md](API-SDK.md).
