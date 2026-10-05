---
title: Processor contracts
description: Reference the configuration, state, output, change, and query contracts of Leani's built-in processors.
section: reference
order: 55
audience:
  - app-developer
  - operator
  - processor-author
status: preview
---

These processors ship in the standard binary. Downstream projects can assemble
a custom binary and register independent native processors through the public
factory/registry API; see [Build a custom processor](https://leani.dev/docs/guides/custom-processor/).

## blobs-money 1.5.0

Block-local Dencun-and-later blob economics and type-3 transaction output. It
accepts the filtered Xatu projection or complete normalized execution frames.
All wei quantities are lossless integers.

`history_mode = "on_demand"` disables only automatic historical processor
coverage. It does not disable canonical live processing, normal JSON-RPC, or
WebSocket head subscriptions.

The processor emits one atomic block change containing the block and all of
its blob transactions. Materialized output stores only the block entity,
transaction entities, and transaction-by-block index; snapshot queries
reconstruct that same bundle without retaining a second permanent copy. The
processor retains the exact post-Fusaka BPO activation schedule and keeps
consensus-block size distinct from the exact execution-block RLP size.

Each block uses the parameters of the fork active at its timestamp. The blob
base fee and every blob burn use the protocol fee, EIP-4844's
`fake_exponential(1, excess_blob_gas, update_fraction)`, under every fork.
From Fusaka on, the EIP-7918 reserve price (execution base fee × 2^13 / 2^17)
is reported separately as `reserveFeeWei` and is not applied to the fee.
Version 1.5 fixes the fee and burn that 1.4 overstated wherever the reserve
bound; blobs.money's own formula still clamps the fee to the reserve.

## evm-events 1.1.0

A declarative event decoder. Configuration supplies contract
addresses, static Solidity event ABI fragments, and output mappings. The
factory parses and validates the ABI before a source is opened, derives exact
topic-zero signatures, pushes address/topic predicates into capable sources,
and decodes without EVM execution.

Supported ABI values are `address`, `bool`, fixed `bytes1..bytes32`, and
`uint`/`int` widths from 8 through 256. Every parameter needs a name
(`type name` or `type indexed name`), because decoded values are keyed by
name. Dynamic values, tuples, arrays, unnamed parameters, anonymous events,
duplicate signatures, malformed keys, and more than three indexed fields fail
deterministically during `doctor`. The order of the configured events is part
of the configuration hash, so reordering them creates a new processor
instance.

Any contract can emit a log with a configured event's topic zero. A log whose
topics or data do not decode as that event, such as an ERC-721 `Transfer`
(four topics, no data) against the ERC-20 `Transfer` ABI or a `bool` word
other than 0 or 1, is not that event: it is skipped rather than failing the
block, and the block's mapped delta counts the skipped logs.

Without `key_fields`, an event's key is its transaction hash and log index,
which no other event shares, so the processor is block-local: its history and
live blocks apply independently. A `key_fields` key can repeat across blocks,
and its entity holds the key's newest event, so any keyed output makes the
processor ordered, like `erc20-balances` (`ordered_state` reduction and
`canonical` delivery ordering). It reduces blocks strictly in chain order: a
block emits one change per key, for its last event, and undoing a block
restores the key's event from before it. Its live lane therefore waits for its
cold backfill, holding live blocks as pending deltas within
`[budgets] pending_delta_bytes` until history reaches them. Consumers read a
keyed instance's stream through the generic consumer routes. Application
subscriptions and `POST /admin/v1/materialization-jobs` require a block-local
processor, so they refuse a keyed instance. With `history_mode = "on_demand"`,
its history comes from the `backfill` command instead, which applies an
ordered processor's history contiguously from `start_block` upward: each range
must start at the block after the last one applied, and the command refuses
any other start. Finalized-coverage compaction is block-local only, so a keyed
instance keeps one coverage row per block it applies, without bound, as other
ordered processors do.

`publish = "finalized_only"` holds included-block changes in the store until
verified finality promotes them. The stream then contains only finalized
`apply` records and `system.finality` markers; no `undo` is ever published.
Held bytes count against `[processors.delivery] max_bytes` from admission.

```toml
[[processors]]
id = "evm-events"
instance = "weth-transfers"
version = "1.1.0"
start_block = 12965000
publish = "finalized_only"

[processors.state]
mode = "durable"

[processors.artifacts]
mode = "none"

[processors.output]
mode = "window"

[processors.output.window]
max_blocks = 216000

[processors.delivery]
mode = "none"

[processors.checkpoint]
mode = "automatic"
keep = 3

[processors.undo]
mode = "none"
safety_blocks = 0

[processors.settings]
addresses = ["0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"]

[[processors.settings.events]]
abi = "event Transfer(address indexed from, address indexed to, uint256 value)"

[processors.settings.events.output]
collection = "weth.transfers"
kind = "weth.transfer"
key_fields = []
```

Every entity/change uses `evm.event.entity.v1`/`evm.event.change.v1`, includes
block and transaction identity as camelCase `0x`-hex fields, and renders
decoded values as exact decimal or hex strings. `bucket_seconds` plus `_bucket_start` in `key_fields`
supports deterministic wall-clock buckets from canonical block timestamps.

## transaction-stats 1.0.0

An ordered A-to-B aggregate that retains constant-sized private working state:
transaction count, exact total value, cursor, checkpoints, and the bounded
unfinalized contribution journal. It requests transaction envelopes and
sender/recipient predicates without receipts, so it cannot tell whether a
transaction reverted: `count` and `totalValueWei` cover every submitted
transaction from the sender to the recipient. `totalValueWei` is the
submitted value, not the value the recipient received.

```toml
[[processors]]
id = "transaction-stats"
instance = "treasury-flow-total"
version = "1.0.0"
start_block = 15537394
publish = "finalized_only"

[processors.state]
mode = "checkpointed"

[processors.artifacts]
mode = "none"

[processors.output]
mode = "latest"

[processors.delivery]
mode = "none"

[processors.checkpoint]
mode = "automatic"
keep = 3

[processors.undo]
mode = "none"
safety_blocks = 0

[processors.settings]
from = "0x1111111111111111111111111111111111111111"
to = "0x2222222222222222222222222222222222222222"
complete_from_start = true
```

Use `output = "latest"` for a continuing aggregate or
`publish = "terminal_only"` for a bounded backfill. Terminal jobs commit state
and coverage for every block but spool only the final aggregate; after
acknowledgement the job is complete/reclaimable and deletion remains an
explicit operator action.

## rollup-txs 1.0.0

A block-local processor for rollups' L1 transactions other than blobs:
calldata batches, state root and output proposals, and proof submissions. It
requests header, transaction, calldata, and receipt material filtered to the
rules' recipients. A transaction matches a rule when it is sent to `to`, from
`from` when set, calls `selector` when set, carries `chain_id_arg` as its first
ABI word when set (shared settlement contracts serving several chains), in a
block whose timestamp lies in `[since, until)`. The first matching rule per
recipient wins, so list the more specific rules first. Type-3 transactions
never match: `blobs-money` reports them. Only top-level calls match; a call
made through a multisig or another contract is not seen.

Every block emits one `rollups.block` change (`rollups.block-bundle.v1`), also
when nothing matched, so a consumer can tell a block without rollup activity
from one it never received. The bundle is camelCase JSON: `block`
(`network`, `blockNumber`, `blockHash`, `parentHash`, `timestamp`) and
`transactions` (`txHash`, `transactionIndex`, `rollupId`, `purpose` =
`data`/`state`/`proof`, `fromAddress`, `toAddress`, `selector`, `gasUsed`,
`executionBurnedWei` = base fee × gas used, `tipWei` = (effective price − base
fee) × gas used). The network and rules are the processor's configuration
identity: a rule change needs a new `instance`.

```toml
[[processors]]
id = "rollup-txs"
instance = "rollup-txs-1"
version = "1.0.0"
history_control = "application_subscriptions"
history_mode = "on_demand"
start_block = 26000000
publish = "included_and_finalized"

[processors.state]
mode = "durable"

[processors.output]
mode = "none"

[processors.delivery]
mode = "until_acknowledged"

[[processors.delivery.consumers]]
id = "app"
required = true

[[processors.settings.rules]]
rollup = "base"
purpose = "state"
to = "0x43edb88c4b80fdd2adff2412a7bebf9df42cb40e"
selector = "0x82ecf2f6"
```

Xatu serves receipt material only for type-3 transactions, so it cannot plan
this processor's history: a job moves past it to the next configured source
(eraE, then execution-P2P fallback).

## erc20-balances 1.1.0

An ordered watchlist ledger derived from canonical ERC-20 `Transfer` logs.
Configuration declares watched addresses, an optional token allowlist, a start
block, and whether the opening coverage is complete.

The stored method is explicitly `erc20_transfer_ledger`. Rebasing,
reflection, and non-standard tokens are not represented as general EVM
balances. Any contract can emit a `Transfer` log, so a log with that
signature that is not a canonical ERC-20 transfer (three topics, zero-padded
addresses, and a 32-byte value), such as an ERC-721 transfer, is skipped
rather than failing the block; the block's mapped delta counts the skipped
logs.

A transfer the ledger cannot apply proves it incomplete for that token and
holder: an outgoing transfer above the derived balance means transfers before
the start block or outside the ERC-20 model, such as a rebasing or forged
token, and an incoming one can overflow it. The pair is then marked
incomplete: `incompleteFrom` holds that block, `complete` is `false`,
`balance` keeps its last derived value, and later transfers for the pair are
ignored. Undoing that block restores the derived pair. Only a token in the
`tokens` allowlist with `complete_from_start = true` fails the processor
instead, because the configuration declares that ledger complete.

```toml
[[processors]]
id = "erc20-balances"
instance = "erc20-watchlist"
version = "1.1.0"
start_block = 1234567
publish = "included_and_finalized"

[processors.state]
mode = "durable"

[processors.output]
mode = "latest"

[processors.delivery]
mode = "window"

[processors.checkpoint]
mode = "automatic"
keep = 3

[processors.undo]
mode = "unfinalized"
safety_blocks = 256

[processors.coverage]
verification_segment_blocks = 8192

[processors.settings]
addresses = ["0x1111111111111111111111111111111111111111"]
tokens = ["0x2222222222222222222222222222222222222222"]
complete_from_start = true
```

## block-summary 1.2.0

A receipt-free, block-local view of every Ethereum execution block. It
requests headers and bodies, then publishes timestamp, parent/hash identity,
gas limit and usage, decimal base fee, blob gas fields, transaction count, exact
execution-block size when available, and finality. Blocks come from verified
material or, under the trusted-dataset policy, from Xatu's dataset-declared
header and transaction projections, whose body indices and payload
transaction count must agree. Dataset rows carry no execution-block size, so
`sizeBytes` is `null` for them. A block a dataset summarized first matches the
same block from verified material, as when the live lane overlaps a Xatu
backfill, and keeps `null`. The reverse, remapping a block that verified
material summarized from a dataset, as a recompute may, is refused as a
conflicting apply. Summaries are retained by
block hash with a canonical block-number index; the query extension exposes
`/latest` and `/blocks/{number}`. It never executes the EVM and does not request
receipts.

The compact `[blocks]` preset supplies the standard checkpointed lifecycle,
full query output, 64 MiB/24 hour delivery window, and undo records kept until
256 blocks past finality. Advanced configurations can override those policies
explicitly.

## uniswap-observations 2.1.0 / uniswap-latest 2.0.0

Two configured-pool contracts share the same exact event decoder:

- Uniswap V2 `Sync(uint112,uint112)` reserve observations;
- Uniswap V3 `Initialize(uint160,int24)` and
  `Swap(address,address,int256,int256,uint160,uint128,int24)` square-root-price
  observations.

They store exact reserves and `sqrtPriceX96`, never floating-point prices. V3
swap observations also retain `amount0` and `amount1` as signed decimal int256
values, preserving the exact pool deltas needed to derive volume. Applications
combine these values with verified token metadata.
`uniswap-observations` is block-local and identifies immutable observations by
pool, block hash, and log index; it supports independent history/live delivery
and full or windowed SQLite retention. Version 2.1 also maintains a query-only
latest-observation pointer per pool so new clients can bootstrap immediately
before following the immutable stream. Transaction hashes are deliberately not
part of this processor's output, allowing archive and P2P sources to omit
transaction bodies when acquiring filtered pool logs.
`uniswap-latest` is ordered/canonical and retains only the latest observation
per pool for slim price queries.

```toml
[[processors]]
id = "uniswap-observations" # or "uniswap-latest"
instance = "usdc-weth-observations"
version = "2.1.0" # use 2.0.0 with uniswap-latest
start_block = 12369621
publish = "included_and_finalized"
history_control = "node_owned"
history_mode = "on_demand"

[processors.state]
mode = "checkpointed"

[processors.artifacts]
mode = "none"

[processors.output]
mode = "full"

[processors.delivery]
mode = "none"

[processors.checkpoint]
mode = "automatic"
keep = 3

[processors.undo]
mode = "unfinalized"
safety_blocks = 256

[processors.settings]

[[processors.settings.pools]]
address = "0x3333333333333333333333333333333333333333"
kind = "v3"
```

Pool factory discovery is intentionally separate from event reduction. A
catalog adapter can discover pools and render these immutable processor
configs; each processor identity then commits to the exact pool set.
