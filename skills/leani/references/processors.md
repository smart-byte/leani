# Leani processors

Read this before configuring a node to extract data. Contents:

1. Choose a built-in processor
2. Decode contract events with `evm-events`
3. Backfill, serve, and check coverage
4. When to write a custom Rust processor

A processor is configured as a `[[processors]]` table in leani.toml. Its `id`
picks the built-in kind and `instance` names this configuration. Version,
settings (including the order of configured events), `start_block`, and
source form an instance's immutable identity: once the store holds an
instance, the node refuses a changed one (`processor instance … conflicts with
its stored descriptor`). Give a changed processor a new `instance` name; it
indexes from its `start_block` beside the old one. The old instance's backfill
subscriptions stay behind and count against the history delivery budget until
deleted: settle them before and after the switch as "Processor rebuild or
rollback" in the operations runbook describes
(`$LEANI_SOURCE/docs/operations/runbook.md`, or its section in
https://leani.dev/_llms-txt/operations.txt). Retention and delivery limits can
change in place. `leani doctor --json` validates the whole file
without touching state; fix every entry in `errors` before running anything
else.

## 1. Choose a built-in processor

| `id` | Use it for |
|---|---|
| `block-summary` | One summary per block: counts, gas, base fee, blobs. `leani init blocks` configures it. |
| `evm-events` | Decoding logs of chosen contracts and events into queryable entities. The general tool for "extract these events". |
| `erc20-balances` | Transfer ledger and current balances for a watchlist of token and holder addresses. |
| `transaction-stats` | Transaction counts and submitted value between one address pair. |
| `uniswap-observations` | Append-only Uniswap V2/V3 pool observations. `leani init uniswap-v3 ETH/USDC` configures it. |
| `uniswap-latest` | Latest state per Uniswap pool, with typed query routes. |
| `blobs-money` | Blob blocks and type-3 transactions: fees, burn, capacity. |

Each processor's settings, output collections, and ordering rules are in the
"Processor contracts" reference: `$LEANI_SOURCE/docs/reference/processor-contracts.md`,
or its section in https://leani.dev/_llms-txt/reference.txt.

## 2. Decode contract events with `evm-events`

Start from a complete configuration instead of writing one from scratch; a
node config has many required tables (budgets, sources, finality, API) that
`doctor` checks. `$LEANI_SOURCE/examples/weth-transfers/node.toml` indexes
historical WETH transfers from the public Xatu dataset with no API key and no
live source. Without a checkout, download that file from the GitHub tag of the
installed version, `v` plus what `leani --version` prints
(`https://raw.githubusercontent.com/smart-byte/leani/v0.1.0-rc.3/examples/weth-transfers/node.toml`
for 0.1.0-rc.3).
Copy it to `leani.toml` in an empty directory and edit its processor:

```toml
[[processors]]
id = "evm-events"
instance = "usdc-transfers"        # new name for a new configuration
start_block = 17000000             # first block this instance covers
# ...keep the example's other [processors.*] tables...

[processors.settings]
addresses = ["0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"]

[[processors.settings.events]]
abi = "event Transfer(address indexed from, address indexed to, uint256 value)"

[processors.settings.events.output]
collection = "usdc.transfers"
kind = "usdc.transfer"
key_fields = []                    # empty: one entity per log
```

Limits `doctor` enforces:

- Supported ABI types: `address`, `bool`, `bytes1`..`bytes32`, and
  `uint`/`int` from 8 to 256 bits. Dynamic types (`string`, `bytes`), arrays,
  tuples, and anonymous events are rejected; so are unnamed parameters,
  because decoded values are keyed by parameter name.
- At most three `indexed` parameters, and no duplicate event signatures.
- Several events can share one processor: add more
  `[[processors.settings.events]]` tables. Their order is part of the
  instance's identity.

Keys shape the output:

- Empty `key_fields` keys each entity by transaction hash and log index. The
  processor is block-local: any block range can be backfilled independently.
- Non-empty `key_fields` keeps only the newest event per key, which makes the
  processor ordered: backfills must run contiguously upward from
  `start_block`, and live blocks wait until history reaches them.
- `bucket_seconds` in the output table (any positive number, such as 3600 or
  86400) stamps each entity with the start of its bucket
  (`bucketStartTimestamp`); it does not aggregate values.

There is no filter on argument values, indexed or not: every configured event
from the configured addresses is stored, so filter when you query. Decoded
values are exact decimal or `0x` strings; apply token decimals yourself. A log
from the configured address whose data does not decode as the event is
skipped, not an error.

## 3. Backfill, serve, and check coverage

```bash
leani doctor --json                                   # expect "valid": true
leani backfill --from-block 17000000 --to-block 17000999
leani serve                                           # background it
curl -s 'http://127.0.0.1:8081/v1/processors/usdc-transfers/status'
curl -s 'http://127.0.0.1:8081/v1/processors/usdc-transfers/collections/usdc.transfers/entities?limit=100'
```

- `--from-block` and `--to-block` are inclusive. `backfill` uses the
  configuration's only processor unless `--processor` names one, and prints a
  JSON report. Results are read through the HTTP API, so start `serve`
  afterwards. `backfill` refuses to run while `serve` holds the data
  directory; with a node already running, add
  `--endpoint http://127.0.0.1:<api-port>` to submit the range to it.
- The example's API port is 8081; read `[api] bind` for yours.
- Coverage: the status response's `available` intervals must include the
  requested range. A high `processedThrough` alone does not prove earlier
  blocks were indexed, and an unindexed range returns `range_incomplete`
  rather than an empty result.
- Following the chain live needs `[sources.live]` and `[finality]` settings
  instead of the example's `disabled`; start from
  `$LEANI_SOURCE/config/modes/` profiles or the "Configure a node" operations
  guide rather than editing those tables blind.

## 4. When to write a custom Rust processor

Stay with configuration when a built-in covers the data. Write a custom
processor when you need what configuration cannot express: dynamic ABI types
or arrays, state derived across events or contracts, custom aggregates, or a
typed query route of your own.

A custom processor is a Rust binary that depends on the `leani` crate,
implements a processor and its factory, registers the factory in a
`ProcessorRegistry`, and starts the node through the library. The API evolves
between releases, so work from the maintained, compiled example rather than
from memory:

- Guide: `$LEANI_SOURCE/docs/guides/custom-processor.mdx`, or "Build a custom
  node in Rust" in https://leani.dev/_llms-txt/guides.txt.
- Example crate: `$LEANI_SOURCE/examples/custom-node/`; check it with
  `cargo test --locked -p leani-custom-example` from the checkout. The guide's
  "Move it into your application" section sets up a crate of your own.
