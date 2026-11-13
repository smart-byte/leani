---
title: Follow a rollup
description: Run a node for Base, OP Mainnet, Arbitrum One, or another bundled rollup, and read how final each of its blocks is.
section: guides
order: 13
audience:
  - app-developer
  - operator
status: preview
---

Leani follows a rollup with the same binary, processors, HTTP API, and SDK as
Ethereum. Pick the chain by name when you write the configuration:

```bash
leani init blocks --network base
leani serve
```

`--network` takes a built-in chain entry or its alias, such as `base`, `op`,
`arbitrum-one`, or `robinhood`. The entries live in
[`config/chains`](https://github.com/smart-byte/leani/tree/main/config/chains).
One node follows one chain, so run one node per chain you need.

## Where the blocks come from

The chain's family picks the sources. Nothing needs an API key.

| Family | Live blocks | History |
|---|---|---|
| OP Stack: OP Mainnet, Base, Unichain, and others | The sequencer's signed block gossip over execution P2P, and the entry's public JSON-RPC endpoints while no peer serves it | Through the live lane |
| Arbitrum Nitro: Arbitrum One, Robinhood Chain, Arbitrum Nova | The sequencer feed, over the entry's public JSON-RPC endpoints | The sources the chain entry names |

An op-stack node gossips on port 9222. Make that port reachable so that peers
can dial in.

## How final a rollup block is

Every block carries the level it has reached, and a change stream tells you
when it rises.

| Level | What happened | Typical | Can it be undone? |
|---|---|---|---|
| `attested` | The chain's sequencer signed the block | about 1 s | Only if the sequencer reorgs its own chain |
| `finalized` | The Ethereum block that carries the block's batch is finalized, so its data can no longer change | 15–30 min | No |
| `settled` | Ethereum accepted a claim covering the block, after its challenge window, so withdrawals can complete | 5–7 days | No |

`finalized` and `settled` come from the L1 anchor, which reads finalized
Ethereum blocks. On a rollup, `[finality]` holds the Ethereum Mainnet
checkpoint that anchor starts from; see
[the configuration guide](/docs/operations/configuration-guide/#l1-anchor).
[Finality, live](https://leani.dev/finality/) shows the levels on four rollups at once.

## Wait for the level you need

A processor publishes from the level its `publish` names, and the SDK reports
each rise as its own operation:

```ts
import { createLeaniClient } from "@smart-byte/leani-sdk";

const leani = createLeaniClient({ baseUrl: "http://127.0.0.1:18080" });
for await (const change of leani.blocks.subscribe()) {
  switch (change.operation) {
    case "apply":     show(change.data, change.finality); break;
    case "attested":
    case "finalized":
    case "settled":   promote(change.data, change.operation); break;
    case "undo":      remove(change.block); break;
  }
}
```

[Choose a finality level](/docs/guides/finality-levels/) compares the levels
across Ethereum and rollups.
