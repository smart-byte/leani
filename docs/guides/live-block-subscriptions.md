---
title: Follow Ethereum blocks from the CLI
description: Print one verified summary for every new Ethereum block from an embedded runtime or a prepared Leani node.
section: guides
order: 12
audience:
  - app-developer
  - operator
status: preview
---

The built-in `block-summary` processor is the smallest continuous Leani demo.
It requests verified execution headers and bodies, so it can report the exact
transaction count without waiting for a particular contract event. It does not
download receipts.

Run it without configuring or starting a node:

```bash
leani subscribe blocks
```

First output can take several minutes while Leani finds serving peers and
validates a current block. The included head is printed when it is available,
followed by one line per new block:

```text
2026-09-01T09:14:35Z  block=25881412  txs=187  gas=32.47M / 60.00M (54.1%)  base_fee=0.143592817 gwei  blobs=6  included
```

The summary contains the block number, hash and parent, timestamp, transaction
count, encoded block size, gas limit and usage, base fee, blob gas, excess blob
gas, and finality. Leani verifies the transaction and withdrawals roots from
the body while keeping receipt acquisition out of this latency-sensitive path.

Use newline-delimited JSON for scripts:

```bash
leani subscribe blocks --format json |
  jq '{block: .blockNumber, transactions: .transactionCount, gas: .gasUsed, baseFee: .baseFeePerGas}'
```

Embedded startup can print a header-commitment-checked peer preview before the
verified processor lane is ready. Those rows say `preview`; JSON reports
`finality: "preview"`. Rows from the verified lane say `included` or
`finalized`. In embedded mode, use `--finality finalized` when the first result
must be a block Ethereum consensus has finalized. Attached output follows the
node's configured source trust; a dataset's finality label does not become a
cryptographic proof simply by subscribing to it. Against a node that publishes
included blocks, an attached `--finality finalized` subscription prints each
block once the node's finality marker covers it.

During a reorg, each reverted block prints again with `"operation": "undo"`
(an `undo` suffix in the terminal), however late the undo arrives. The undo
of a block the subscription saw but did not print, such as an older block
of the embedded startup scan, stays hidden. The undo of a block it never
saw, because it was applied before the run started, prints as a
`leani.subscription-undo.v1` row with its `blockNumber`, `blockHash`, and
change `key` while it is fresh. JSON output mixes such rows, and the gap rows
described below, with `leani.block-summary.v1` rows, so scripts should
dispatch on each row's `schema`.

`--once` exits after the first summary, including a labelled preview, which is
useful for startup timing. An undo or gap row never ends it:

```bash
time leani subscribe blocks --once
```

## Attach to a prepared node

Create the compact block-summary configuration once:

```bash
mkdir leani-block-demo
cd leani-block-demo
leani init blocks
```

The generated product configuration is intentionally small:

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

Start the node in one terminal:

```bash
leani serve
```

Then subscribe in another terminal from the same directory:

```bash
leani subscribe blocks
```

Auto mode discovers the local config, detects the running API, reads a stable
latest-block snapshot, and follows its resumable change stream. If no node is
reachable, the same command starts the embedded runtime. Use `--mode client`
or `--mode embedded` only when you need to force one behavior. With
`--endpoint URL`, client and auto mode attach to that node without reading a
local configuration.

The subscription prints nothing until the stream's `hello` shows an Ethereum
mainnet node (chain 1) serving `block-summary` under the requested
`--processor`, and it checks every reconnect again. A node for another chain
or processor, or another service answering at `api.bind`, ends the command
with an error instead of printing its stream. A stream error the node marks
`retryable` reconnects from the last cursor.

`--finality finalized` replays the node's retained change log, so a block
that was included but not yet final when the subscription connected still
prints once finality covers it, however late that is; blocks already final
at that point are skipped. The subscription holds at most 1,024 unfinalized
blocks. If the node stops reporting finality for longer, the oldest held
blocks are dropped. Finalized output then has a hole there, reported as one
row for the dropped range right before the next finalized row:

```json
{"schema":"leani.subscription-gap.v1","protocol":"blocks","operation":"gap","fromBlock":25881412,"toBlock":25881420,"timestamp":1788254075,"reason":"finality_gate_full"}
```

The terminal prints a `gap` line instead. If the node pruned the changes of
blocks that were not yet final before the subscription connected, those
blocks are reported the same way with `"reason": "change_log_pruned"`. A gap
row never ends `--once`.

Reset just the embedded block feed when measuring a true cold start:

```bash
leani reset subscription blocks --yes
```

The configured node and embedded subscription use separate state directories,
so both can be tested from the same project directory. A subscription reset
only removes a directory `leani subscribe` marked as subscription state and
no node has taken over since (see
[the Uniswap guide](/docs/guides/uniswap-price-subscriptions/#cli)), and `leani reset all`
refuses while an embedded subscription is running.
