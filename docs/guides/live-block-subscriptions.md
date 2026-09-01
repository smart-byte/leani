---
title: Follow Ethereum blocks from the CLI
description: Print one verified, header-only summary for every new Ethereum block from an embedded runtime or a prepared Leani node.
section: guides
order: 12
audience:
  - app-developer
  - operator
status: preview
---

The built-in `block-summary` processor is the smallest continuous Leani demo.
It requests verified execution headers only, so it does not wait for a
particular contract event or download block bodies and receipts.

Run it without configuring or starting a node:

```bash
leani subscribe blocks
```

The optimistic head is printed as soon as it is available, followed by one
line per new block:

```text
2026-09-01T09:14:35Z  block=25881412  gas=32.47M / 60.00M (54.1%)  base_fee=0.143592817 gwei  blobs=6  optimistic
```

The summary contains the block number, hash and parent, timestamp, gas limit
and usage, base fee, blob gas, excess blob gas, and finality. Transaction count
and encoded block size are optional fields: header-only P2P following leaves
them empty instead of delaying the feed to fetch a block body.

Use newline-delimited JSON for scripts:

```bash
leani subscribe blocks --format json |
  jq '{block: .blockNumber, gas: .gasUsed, baseFee: .baseFeePerGasWei}'
```

`--once` exits after the first summary, which is useful for startup timing:

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
endpoints = ["https://ethereum-beacon-api.publicnode.com/"]

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
or `--mode embedded` only when you need to force one behavior.

Reset just the embedded block feed when measuring a true cold start:

```bash
leani reset subscription blocks --yes
```

The configured node and embedded subscription use separate state directories,
so both can be tested from the same project directory.
