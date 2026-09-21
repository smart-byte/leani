# Leani

Lean, state-minimized Ethereum indexing node. Leani backfills application
data from public datasets, follows the chain head over native Ethereum P2P,
and keeps no raw history. No API key, no archive node.

Status: pre-release preview (`v0.1.0-rc.1`). Read the
[preview limitations](docs/operations/preview-limitations.md) before deploying.

## Install

```bash
brew install smart-byte/tap/leani
```

Or take a checksummed archive from
[GitHub releases](https://github.com/smart-byte/leani/releases), run the
container image `ghcr.io/smart-byte/leani:v0.1.0-rc.1`, or build from source:

```bash
git clone https://github.com/smart-byte/leani.git && cd leani
cargo build --locked --release -p leani   # target/release/leani
```

See the [install guide](https://leani.dev/docs/getting-started/install/).

## Follow Ethereum blocks

No configuration file:

```bash
leani subscribe blocks
```

On first use Leani asks independent public providers for a finalized
checkpoint and lets you accept it once at least two agree. Peer discovery can
take a few minutes. After that, every new block streams to stdout:

```text
2026-09-07T12:11:59Z  block=25925396  txs=381  gas=43.17M / 60.00M (72.0%)  base_fee=0.069459109 gwei  blobs=3  included
```

Add `--finality finalized` to print only blocks Ethereum consensus has
finalized, or `--format json` for NDJSON.

## Follow a processor

Built-in processors turn blocks into application data. A live Uniswap V3 price
uses the same command shape:

```bash
leani subscribe uniswap-v3 ETH/USDC
```

Several markets, piped as NDJSON into another Unix tool:

```bash
leani subscribe uniswap-v3 ETH/USDC ETH/USDT --format json |
  jq --unbuffered '{market, price, baseVolume, blockNumber, finality}'
```

Keep a node running so several consumers can share it:

```bash
leani init uniswap-v3 ETH/USDC   # writes a compact leani.toml after the checkpoint quorum
leani serve                       # HTTP API on :18080, JSON-RPC on :18545
```

The same `leani subscribe` command in another terminal detects the local node
and attaches to it instead of starting its own runtime. Guides:
[block subscriptions](https://leani.dev/docs/guides/live-block-subscriptions/),
[Uniswap price subscriptions](https://leani.dev/docs/guides/uniswap-price-subscriptions/).

## Index history without an archive node

Backfill a block range from public Xatu datasets, then query the result. The
`windowed` profile decodes Uniswap V2 `Sync` events into hourly aggregates:

```bash
cp config/modes/windowed.toml leani.toml
leani backfill --processor uniswap-v2-sync-30d --from 17000000 --to 17000999
leani serve
```

```bash
curl -s 'http://127.0.0.1:8080/v1/processors/uniswap-v2-sync-30d/collections/uniswap_v2.sync_hourly/entities?limit=3'
```

Any contract event can be indexed from its ABI alone. The complete
[`examples/weth-transfers/node.toml`](examples/weth-transfers/node.toml)
configures the built-in `evm-events` processor like this:

```toml
[processors.settings]
addresses = ["0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"]

[[processors.settings.events]]
abi = "event Transfer(address indexed from, address indexed to, uint256 value)"

[processors.settings.events.output]
collection = "weth.transfers"
kind = "weth.transfer"
```

Backfills resume from committed coverage, and historical Parquet material is
discarded after each commit. A backfill can run while the live lane already
follows the head. See the
[bounded Mainnet guide](https://leani.dev/docs/getting-started/bounded-mainnet/),
the [processor catalog](https://leani.dev/docs/reference/processors/), and
[backfill while following live](https://leani.dev/docs/guides/backfill-while-following-live/).

## Drop-in JSON-RPC

A node prepared with `leani init` serves JSON-RPC on port 18545:

```bash
curl -s http://127.0.0.1:18545 -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}'
# {"jsonrpc":"2.0","id":1,"result":"0x1"}
```

```ts
import { createPublicClient, http } from "viem";

const eth = createPublicClient({ transport: http("http://127.0.0.1:18545") });
console.log(await eth.getChainId()); // 1
```

Leani answers chain metadata, recent and bounded historical blocks,
transactions, receipts, and `eth_getLogs`, plus reorg-correct `newHeads` and
`logs` WebSocket subscriptions. Without an EVM and state it does not implement
`eth_call`, `eth_getBalance`, `eth_getCode`, `eth_getStorageAt`,
`eth_estimateGas`, or trace methods; those fail explicitly instead of
answering wrongly. See the [method matrix](docs/reference/ethereum-json-rpc.md).

## TypeScript SDK

```bash
bun add @smart-byte/leani-sdk
```

```ts
import { createLeaniClient } from "@smart-byte/leani-sdk";

const leani = createLeaniClient({ baseUrl: "http://127.0.0.1:18080" });

for await (const change of leani.blocks.subscribe()) {
  if (change.operation === "apply" && change.data) {
    console.log(change.data.blockNumber);
  }
}
```

The client exposes typed queries and subscriptions for every built-in
processor, a resumable SSE change stream, and durable named consumers with
leases and acknowledgements.
[Query then follow](https://leani.dev/docs/guides/query-then-follow/) seeds
application state and handles reorgs;
[durable consumer](https://leani.dev/docs/guides/durable-consumer/) commits
the Leani cursor inside your own database transaction.

## Your own processor

Processors are Rust. [`examples/custom-node`](examples/custom-node) adds a
native processor and a query extension to the stock node through
`leani::run_with_registry`, without forking it. See the
[processor-author guide](https://leani.dev/docs/guides/custom-processor/).

## How it works

```text
             Historical lane                         Live lane
    Xatu Parquet / EraE / AWS / Google       Ethereum P2P + CL finality
                       \                       /
                        Source adapters
                              |
                  normalized BlockEnvelope
                              |
                 validation + canonicality
                              |
        parallel per-block map -> ordered state reduce
                              |
             processor stores and change stream
                              |
     RPC facade / HTTP+SSE SDK API / SQL sinks
```

Historical sources are public Xatu Parquet datasets and EraE archives, read
range by range and verified against block commitments. The live lane follows
execution peers through isolated Reth networking crates and verifies finality
from the Beacon API or consensus P2P light-client updates. Both lanes join
exactly once at a hash-verified boundary. Disk holds only:

```text
disk =
  application outputs
  + processor state/checkpoints
  + a short recent-block/reorg window
  + one bounded transient input chunk
  + optional indexes explicitly enabled by the operator
```

Read [how Leani works](https://leani.dev/docs/concepts/how-leani-works/),
[coverage, finality, and reorgs](https://leani.dev/docs/concepts/coverage-finality-and-reorgs/),
and the [architecture notes](docs/contributing/architecture.md).

## Documentation

- [leani.dev/docs](https://leani.dev/docs/): quickstart, guides, CLI, configuration, HTTP API, and SDK references.
- [`docs/`](docs/): the Markdown sources, including [ADRs](docs/adr/README.md).
- [CHANGELOG.md](CHANGELOG.md) and [SECURITY.md](SECURITY.md).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for the local checks and publication
guards. Verification and performance evidence are described in
[automated verification](docs/contributing/automated-verification.md),
[manual verification](docs/contributing/manual-verification.md), and
[benchmarking](docs/contributing/benchmarking.md).

## License

Leani is licensed under the [MIT License](LICENSE).
[THIRD_PARTY_LICENSES.txt](THIRD_PARTY_LICENSES.txt) lists the license text
and notices of every dependency compiled into the node and ships with every
release archive, the container image, and the Homebrew package. Historical
demonstration data and benchmark evidence derive from
[Xatu](https://github.com/ethpandaops/xatu-data) by ethPandaOps (CC BY 4.0).
