# Leani processors

Read this before configuring a node to extract data. Contents:

1. Choose a built-in processor
2. Decode contract events with `evm-events`
3. Backfill, serve, and check coverage
4. Write your own processor

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
| `rollup-txs` | Rollups' non-blob L1 transactions (calldata batches, state and output proposals, proofs) matched by configured rules, with gas, execution burn, and tips. Blob transactions stay with `blobs-money`. |

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
(`https://raw.githubusercontent.com/smart-byte/leani/v0.1.1/examples/weth-transfers/node.toml`
for 0.1.1).
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

- `abi` is a human-readable fragment or one JSON ABI event object, as a block
  explorer shows it. Every ABI type events use works except `function` and
  fixed-point: `address`, `bool`, `bytes1`..`bytes32`, `uint`/`int` from 8 to
  256 bits, `bytes`, `string`, arrays, and tuples, nested at most six levels.
  One element of a dynamic array holds at most 64 values, counting each
  nested array and tuple as one.
  Write a tuple with its component names:
  `(uint256 amount, bool exactOutput) params`.
- Every parameter and tuple component needs a name, because decoded values
  are keyed by name. Anonymous events are rejected.
- At most three `indexed` parameters, and no duplicate event signatures.
- Several events can share one processor: add more
  `[[processors.settings.events]]` tables. Their order is part of the
  instance's identity.
- `abi = "log0"` stores every topicless log of the configured contracts raw,
  as `values.data` in `0x` hex, for contracts that log a packed layout no ABI
  describes, such as Ekubo Core's swaps. It requires `addresses`, and the
  processor then reads every log of those contracts.

Values are strings:

- Integers are decimal; addresses, `bytesN`, and `bytes` are `0x` hex;
  strings are text. Apply token decimals yourself.
- A tuple flattens to one key per component, such as `params.amount`.
- An array is one JSON string, such as `["1","2"]`; parse it before use.
- An indexed `string`, `bytes`, array, or tuple is only its 32-byte keccak
  hash on chain, so its value is that hash.

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
from the configured addresses is stored, so filter when you query, or write a
processor of your own (section 4) when the answer is an aggregate. A log from the
configured address whose data does not decode as the event, including data
that is not the canonical ABI encoding, is skipped, not an error.

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

## 4. Write your own processor

When no built-in produces the answer directly, write a processor; that is how
Leani is meant to be extended. Typical reasons: an aggregate (distinct count,
sum, top-N, daily totals), transactions or their ETH value rather than logs,
state derived across events or contracts, a filter on argument values,
fields of packed, non-ABI log data, or a typed query route of your own.
Exporting every raw event to count it client-side is the sign a processor is
missing.

The quickest is a Wasm processor, which the stock binary runs with no node of
your own to build. `leani processor create` writes its crate from a spec of the
data to keep, or from a bundled template's source (`--template`), with the SDK
of the installed binary; edit `src/lib.rs` where the spec falls short:

```bash
leani processor create usdc-volume --spec - --json <<'EOF'
{"data": {"kind": "events", "contracts": ["0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"],
          "events": ["Transfer(address indexed from, address indexed to, uint256 value)"]},
 "keep": {"kind": "totals", "by": "to", "sum": ["value"]}}
EOF
(cd usdc-volume && cargo build --release --target wasm32-unknown-unknown)
leani processor run usdc-volume/target/wasm32-unknown-unknown/release/usdc_volume.wasm --from-block 21000000 --to-block 21000099
```

`data.kind` is `events` (`contracts`, `events`), `transactions` (`to`,
`from`, and `functions`, whose calls it decodes), or `blocks`; `fields` picks
what to keep, all by default. `keep.kind` is `rows`, or `totals` with `by` (one
field, or none for one total) and `sum` (unsigned fields). Install the target
once with `rustup target add wasm32-unknown-unknown`.

`--language typescript` writes the same processor as a TypeScript package
instead (AssemblyScript: TypeScript syntax, Wasm types such as `u64`), edited
in `assembly/index.ts` and built with `npm install && npm run build` into
`build/<name>.wasm`; it needs Node.js 20 or later. Each bundled template has a
TypeScript twin that publishes the same changes
(`--template <name> --language typescript`).

The contract, in short: `manifest` names the reduction (`block_local`, each
block alone, or `ordered`, reading what earlier blocks wrote) and the
capabilities and filter the processor needs; `map` turns one block, holding
only that material, into a delta; `reduce` writes entities and publishes
changes with `upsert` and `remove`. To keep it running, configure a processor
with `id = "wasm"`, `settings.module` (the file, relative to the
configuration), `settings.params` (JSON for the module), and optionally
`settings.blake3` (a pin of the module bytes) and `settings.limits`;
`leani init --requirements` builds that configuration with the `custom`
preset (`leani guide config`).

A running node also generates the source (`POST /v1/wasm/compose`) and, with
`api.allow_wasm_builds = true`, builds it (`POST /admin/v1/wasm/build`).
Node-side builds are a development feature: in production, build the module
locally or in CI and configure it. A native processor, below, suits typed
query routes and the hottest paths.

The descriptor declares the block material the processor reads, such as
`Capability::Header`, `Body` (transaction `to`, value, nonce), `Senders`, or
`Logs`; request only what `map` uses, since that decides which sources can
serve it. `map` turns one block into a compact, deterministic delta; `reduce`
folds deltas in block order into state and emits changes.

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
