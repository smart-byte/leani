# Live Uniswap prices from the CLI and SDK

Leani's `subscribe` command turns the built-in Uniswap V3 observation
processor into a small live-price utility. It follows Ethereum directly and
prints only matching pool updates to stdout; lifecycle and peer messages go to
stderr, so stdout is safe to pipe.

## CLI

After installing the binary, subscribe to the ETH/USDC 0.05% Mainnet pool:

```bash
leani subscribe uniswap-v3 ETH/USDC
```

That exact command is also the standalone demo. Auto mode first checks for a
reachable node from the explicit endpoint or local configuration; when there
is none, it starts the embedded runtime. Use `--mode embedded` only when a
reachable local node should intentionally be ignored.

The first run asks three independent checkpoint providers for a recent
finalized weak-subjectivity checkpoint. At least two must return the exact same
slot and block root before Leani asks you to accept it. Execution then follows
native P2P peers, while later Beacon API responses are proof-checked from that
agreed root. For an intentional non-interactive invocation, use `--yes`.

After the light client has verified a newer finality anchor and its
consensus-committed execution hash, Leani persists that locally verified
anchor. Ordinary restarts reuse it without contacting the checkpoint providers
while it remains fresh. The transport can also be fully peer-to-peer after
bootstrap:

```bash
leani subscribe uniswap-v3 ETH/USDC --finality-source p2p
```

P2P finality still needs an initial trust root. It reuses the local verified
anchor when available; otherwise it performs the same checkpoint quorum once,
then obtains and verifies light-client updates over consensus P2P instead of a
Beacon API. Override the provider set with repeatable `--checkpoint-url` flags
(or `LEANI_CHECKPOINT_URLS`) and the threshold with `--checkpoint-quorum`.

The default pretty stream looks like this:

```text
2026-08-28T19:22:11Z  ETH/USDC 2439.03084279  volume=0.601207569812525017 ETH  block=25855798  optimistic
```

Volume is the absolute swap amount in the first, or base, token of the market.
For example, `ETH/USDC` reports ETH volume while `WBTC/ETH` reports WBTC
volume.

One embedded runtime can filter several pools at once:

```bash
leani subscribe uniswap-v3 ETH/USDC ETH/USDT WBTC/ETH
```

Use the stable `leani.market-price.v2` NDJSON contract for scripts. Its
`baseVolume` string uses exact base-token decimal units:

```bash
leani subscribe uniswap-v3 ETH/USDC --format json |
  jq --unbuffered 'select(.operation == "apply") | [.market, .price, .baseVolume] | @tsv'
```

`--format raw` exposes the underlying change envelope, and `--once` exits after
one matching update. `--finality finalized` builds an embedded finalized-only
processor. The default `optimistic` mode emits head updates and marks undo
records during a reorg.

Embedded mode does not bind API or RPC ports, activate unrelated processors, or
start a historical backfill. It starts from an independently verified finalized
execution anchor, follows native execution P2P to the current head, and waits
for fresh swaps. Its small SQLite store, locally verified checkpoint, and peer
cache live under the platform data directory; use `--data-dir PATH` to select
an explicit location. A completely cold run can take a few minutes because
public peer discovery and the finalized-to-head catch-up are network dependent;
subsequent runs reuse verified canonical material and the peer cache. If a
local configuration exists, embedded fallback seeds its private peer cache from
the configured node context, but it never copies the node's P2P identity or
shares its writable database. Once verified startup is complete, Leani prints
the newest still-fresh observation it acquired during that startup—one per
requested market—instead of silently discarding those prices. It then continues
with new swaps. Consequently, embedded `--once` returns the newest fresh
startup price when one exists.

When a node is already running from a project-local `leani.toml`, run the same
command from another terminal in that directory:

```bash
# One-time setup
leani init uniswap-v3 ETH/USDC

# Terminal 1
leani serve

# Terminal 2
leani subscribe uniswap-v3 ETH/USDC
```

Auto mode discovers the reachable `api.bind` in `./leani.toml` and attaches;
otherwise it starts the embedded runtime. Use `--mode client --endpoint URL`
when attaching to a remote node should be mandatory rather than automatic. The
client takes a head cursor before opening SSE and applies the same timestamp
freshness gate as embedded mode, so retained history and in-progress node
catch-up are not presented as live prices. Before opening the stream, it reads
the processor's immutable pool catalog and rejects any requested market that
the node does not process. Running-node subscriptions never mutate processor
scope; all clients reuse the configured durable stream.

The initializer fetches a current two-provider checkpoint quorum and pins it
in a compact configuration. It asks before accepting a new trust root and
refuses to overwrite an existing file. `leani --config demo.toml init ...`
writes to a different path; `--yes` is available for an intentional
non-interactive setup. A generated ETH/USDC file has this shape (the root and
slot are live values):

```toml
config_version = 1
network = "ethereum-mainnet"
data_dir = "./data"

[finality]
checkpoint = "0x..."
checkpoint_slot = 15100000
endpoints = ["https://ethereum-beacon-api.publicnode.com/"]

[uniswap]
markets = ["ETH/USDC"]

[api]
bind = "127.0.0.1:18080"
```

The compact document expands to the standard local-node profile: native
execution P2P with an opportunistic minimum of one peer and a 16-peer healthy
target, Xatu history fallback, bounded 512 MiB memory and 2 GiB temporary-disk
budgets, on-demand Uniswap history, optimistic/finalized publication, a 256
block undo window, 64 MiB/24 hour delivery retention, ephemeral P2P listener
ports, RPC on `18545`/`18546`, and the native API on `18080`. Pool addresses,
fee tiers, and the earliest processor block come from the same built-in market
catalog as `subscribe`. Operators who need to tune any of these can use the
full configuration schema instead.

## TypeScript SDK

The node API deliberately publishes exact raw pool state. This example derives
USDC per ETH with `bigint`, preserving the V3 ratio until the final decimal
rendering:

```ts
import { createLeaniClient } from "@leani/sdk";

const leani = createLeaniClient({ baseUrl: "http://127.0.0.1:8080" });
const Q192 = 1n << 192n;

function decimalRatio(numerator: bigint, denominator: bigint, places = 8) {
  const scale = 10n ** BigInt(places);
  const rounded = (numerator * scale + denominator / 2n) / denominator;
  const digits = rounded.toString().padStart(places + 1, "0");
  return `${digits.slice(0, -places)}.${digits.slice(-places)}`;
}

function usdcPerEth(sqrtPriceX96: string) {
  const sqrt = BigInt(sqrtPriceX96);
  // Pool token0 is USDC (6 decimals), token1 is WETH (18 decimals).
  return decimalRatio(Q192 * 10n ** 18n, sqrt * sqrt * 10n ** 6n);
}

function tokenAmount(rawAmount: string, decimals: number) {
  const signed = BigInt(rawAmount);
  const amount = signed < 0n ? -signed : signed;
  const digits = amount.toString().padStart(decimals + 1, "0");
  const integer = digits.slice(0, -decimals);
  const fraction = digits.slice(-decimals).replace(/0+$/, "");
  return fraction ? `${integer}.${fraction}` : integer;
}

for await (const change of leani.uniswap.subscribe()) {
  if (
    change.operation === "apply" &&
    change.data?.pool.toLowerCase() ===
      "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640" &&
    change.data.sqrtPriceX96 &&
    change.data.amount1
  ) {
    console.log({
      price: usdcPerEth(change.data.sqrtPriceX96),
      baseVolume: tokenAmount(change.data.amount1, 18),
    });
  }
}
```

The CLI uses the same integer formulas. It does not convert `sqrtPriceX96` or
swap amounts to floating-point numbers.

## Built-in Mainnet markets

| Market | Fee tier | Pool |
| --- | ---: | --- |
| ETH/USDC | 0.05% | `0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640` |
| ETH/USDT | 0.30% | `0x4e68Ccd3E89f51C3074ca5072bbAC773960dFa36` |
| WBTC/ETH | 0.30% | `0xCBCdF9626bC03E24f779434178A73a0B4bad62eD` |
