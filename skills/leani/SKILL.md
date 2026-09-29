---
name: leani
description: Runs and queries leani, a self-hosted Ethereum indexing node and CLI. It follows Mainnet blocks over P2P with no API key, backfills historical ranges, and extracts contract events and logs, ERC-20 balances, Uniswap prices, and blob data into a local HTTP API, TypeScript SDK, and JSON-RPC subset. Use this skill whenever a task mentions the `leani` command, a leani.toml, leani.dev, or @smart-byte/leani-sdk, or needs live or historical Ethereum blocks, contract events, or prices without a hosted RPC provider, including writing custom Rust processors for leani.
license: MIT
---

# Leani

Leani is an Ethereum Mainnet indexing node you run yourself. It verifies
consensus with a light client, reads execution data from P2P peers or public
datasets, runs *processors* that turn blocks into application data, and keeps
only their results. One binary provides the CLI, the node (`leani serve`), a
native HTTP API with streaming, and a JSON-RPC subset. It is alpha software.

## When to use it, and when not

Use leani to follow new blocks or Uniswap prices live, to index contract events
or ERC-20 transfers over a block range, to serve those results to an
application, or to build a custom processor in Rust.

Leani keeps no EVM state, so it refuses `eth_call`, `eth_getBalance`,
`eth_getCode`, `eth_getStorageAt`, `eth_estimateGas`, and traces. Use an
execution provider for those. Derived balances and prices are processor
outputs with a documented scope, not a replacement for arbitrary state.

## Rules for running the CLI

`leani <command> --help` is the source of truth for flags; check it rather
than guessing, because flags change between releases. If
`leani subscribe --help` lacks `--timeout`, the binary predates this skill.

1. **Ask for JSON.** `subscribe --json` prints NDJSON, `doctor --json` prints a
   report, and `backfill` and `db` always print JSON. Progress and logs go to
   stderr, so stdout stays parseable.
2. **Never block on a stream.** `subscribe` follows until interrupted and a cold
   start can take minutes while peers are discovered. For one answer use
   `--once --timeout 5m`. Run `serve` or an open-ended `subscribe` in the
   background and poll `/health/ready` on the node's `[api] bind` (port 18080
   for a node made by `leani init`).
3. **Checkpoint trust is the user's decision.** `init`, and a `subscribe` that
   runs its own embedded runtime (no node to attach to) without a cached
   anchor, fetch a finalized checkpoint that at least two public providers
   agree on; it anchors all later verification. Without a terminal they print
   the checkpoint summary to stderr and stop with `checkpoint trust requires an
   interactive terminal or --yes`. Show the user that summary and rerun with
   `--yes` only after they approve it.
4. **Respect the finality label.** Rows say `preview` (a peer's block, not yet
   verified; only printed while following, or with `--finality preview`),
   `included` (verified, can still be reorged), or `finalized`. Do not present
   `preview` or `included` data as final; pass `--finality finalized` when
   correctness matters more than latency. `--once` returns the first row that
   meets `--finality` (default `included`).
5. **Dispatch JSON rows on `schema`.** A stream mixes
   `leani.block-summary.v1` or `leani.market-price.v2` rows with
   `leani.subscription-undo.v1` and `leani.subscription-gap.v1`. A row with
   `"operation": "undo"` reverts one printed earlier.
6. **Read exit codes.** 0 success; 1 runtime failure; 2 usage error; 3 `doctor`
   found the configuration unreadable or invalid; 124 `--once` hit `--timeout`
   before a match; 130 interrupted before `--once` matched.
7. **Ask before deleting state.** `leani reset all`, `leani reset subscription`,
   and `leani db prune-changes` remove data. `db` commands take the node's lock,
   so stop `serve` first. Never point two nodes at one data directory.

## Install check

Run `leani --version`. If it is missing, ask how the user wants to install it:
`brew install smart-byte/tap/leani`, a release archive from
https://github.com/smart-byte/leani/releases, the container image
`ghcr.io/smart-byte/leani:v<version>`, or a source build
(`cargo build --locked -p leani` in a checkout, then add `target/debug` to
`PATH` and export `LEANI_SOURCE` as the checkout path). Keep the binary, the
SDK, and configuration on the same version.

## Common tasks

Latest block or price, one row (a block row has `blockNumber`, `blockHash`,
`timestamp`, and `finality`):

```bash
leani subscribe blocks --json --once --timeout 5m
leani subscribe uniswap-v3 ETH/USDC --json --once --timeout 5m   # also ETH/USDT, WBTC/ETH
```

Reusable node with an HTTP API, SDK subscriptions, and JSON-RPC:

```bash
mkdir leani-blocks && cd leani-blocks
leani init blocks                  # or: leani init uniswap-v3 ETH/USDC
leani serve                        # background it; API 127.0.0.1:18080, JSON-RPC 127.0.0.1:18545
curl -s http://127.0.0.1:18080/v1/capabilities
leani subscribe blocks --json      # from this directory, attaches to the running node
```

Index contract events or another historical range: read
[references/processors.md](references/processors.md) first. It covers choosing
a built-in processor, the event-decoding configuration and its limits,
backfills, coverage checks, and when a custom Rust processor is needed.

## HTTP API and SDK

- `GET /v1/capabilities` lists the configured processors; check it instead of
  assuming a route works. `GET /v1/processors/{processor}/status` reports
  coverage.
- `GET /v1/processors/{processor}/collections/{collection}/entities?limit=100`
  pages results; follow `nextCursor`. Add `fromBlock`/`toBlock` for a range.
  `range_incomplete` means the range is not indexed (not "no events");
  `output_not_retained` means it was indexed and later pruned.
- `GET /v1/processors/{processor}/stream` is the server-sent-event change
  stream; the TypeScript SDK wraps it:
  `createLeaniClient({ baseUrl }).blocks.subscribe()` from
  `@smart-byte/leani-sdk`. Install the SDK version that matches the node exactly;
  prereleases are not `latest` on npm.

## Documentation

With a source checkout, read the Markdown under `$LEANI_SOURCE/docs/`; it is
what the site renders. Otherwise start at https://leani.dev/llms.txt, which
links one file per section: `https://leani.dev/_llms-txt/getting-started.txt`,
`guides.txt`, `reference.txt`, `concepts.txt`, and `operations.txt`. Fetch the
section you need rather than guessing page URLs.
