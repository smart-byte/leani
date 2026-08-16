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
factory/registry API; see [Build a custom processor](/docs/guides/custom-processor/).

## blobs-money 1.4.0

Block-local Dencun-and-later blob economics and type-3 transaction output. It
accepts the filtered Xatu projection or complete normalized execution frames.
All wei quantities are lossless integers.

`history_mode = "on_demand"` disables only automatic historical processor
coverage. It does not disable canonical live processing, normal JSON-RPC, or
WebSocket head subscriptions.

Version 1.4 emits one atomic block change containing the block and all of its
blob transactions. Materialized output stores only the block entity,
transaction entities, and transaction-by-block index; snapshot queries
reconstruct that same bundle without retaining a second permanent copy. The
processor retains the exact post-Fusaka BPO activation schedule and keeps
consensus-block size distinct from the exact execution-block RLP size.

## evm-events 1.0.0

A block-local declarative event decoder. Configuration supplies contract
addresses, static Solidity event ABI fragments, and output mappings. The
factory parses and validates the ABI before a source is opened, derives exact
topic-zero signatures, pushes address/topic predicates into capable sources,
and decodes without EVM execution.

Supported ABI values are `address`, `bool`, fixed `bytes1..bytes32`, and
`uint`/`int` widths from 8 through 256. Dynamic values, tuples, arrays,
anonymous events, duplicate signatures, malformed keys, and more than three
indexed fields fail deterministically during `doctor`.

```toml
[[processors]]
id = "evm-events"
instance = "weth-transfers"
version = "1.0.0"
start_block = 12965000
publish = "finalized_only"

[processors.state]
mode = "durable"

[processors.artifacts]
mode = "none"

[processors.output]
mode = "window"
finalized_only = true

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
sender/recipient predicates without receipts.

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

## erc20-balances 1.0.0

An ordered watchlist ledger derived from canonical ERC-20 `Transfer` logs.
Configuration declares watched addresses, an optional token allowlist, a start
block, and whether the opening coverage is complete.

The stored method is explicitly `erc20_transfer_ledger`. Outgoing underflow
fails the processor because it proves that the selected start/snapshot is
incomplete. Rebasing, reflection, and non-standard tokens are not represented
as general EVM balances.

```toml
[[processors]]
id = "erc20-balances"
instance = "erc20-watchlist"
version = "1.0.0"
start_block = 1234567
publish = "optimistic_and_finalized"

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

## uniswap-observations / uniswap-latest 1.0.0

Two configured-pool contracts share the same exact event decoder:

- Uniswap V2 `Sync(uint112,uint112)` reserve observations;
- Uniswap V3 `Initialize(uint160,int24)` and
  `Swap(address,address,int256,int256,uint160,uint128,int24)` square-root-price
  observations.

They store exact reserves and `sqrtPriceX96`, never floating-point prices.
Applications combine these values with verified token metadata.
`uniswap-observations` is block-local and identifies immutable observations by
pool, block hash, and log index; it supports independent history/live delivery
and full or windowed SQLite retention. Transaction hashes are deliberately not
part of this processor's output, allowing archive and P2P sources to omit
transaction bodies when acquiring filtered pool logs.
`uniswap-latest` is ordered/canonical and retains only the latest observation
per pool for slim price queries.

```toml
[[processors]]
id = "uniswap-observations" # or "uniswap-latest"
instance = "usdc-weth-observations"
version = "1.0.0"
start_block = 12369621
publish = "optimistic_and_finalized"
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
