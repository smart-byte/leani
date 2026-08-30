---
title: Manual verification
description: Rehearse catch-up, live following, subscriptions, JSON-RPC behavior, restart, and recovery against explicit acceptance checks.
section: contributing
order: 50
audience:
  - contributor
  - operator
status: preview
---

This guide exercises Leani the way an application such as blobs.money uses its
native integration:

1. start from a fresh database 10,000 blocks behind the finalized head;
2. backfill application-specific output while following the head;
3. consume the durable `blobs-money` transformation stream;
4. query the materialized native API and Ethereum JSON-RPC facade; and
5. interrupt, restart, and resume from a saved change cursor.

For repeatable automated gates and their evidence requirements, see
[Automated verification](https://leani.dev/docs/contributing/automated-verification/).

## Test modes

| Mode | History | Follows head | Serves APIs | Primary purpose |
| --- | --- | --- | --- | --- |
| Deterministic runtime test | Synthetic | Synthetic | In-process API | Fast scheduling, crash, handoff, and storage verification |
| Historical-only `backfill` | Public datasets | No | After starting `serve` | Isolate dataset ingestion and transformations |
| Continuous `serve` | Public datasets plus bounded P2P bridge | Yes | Yes | Application integration rehearsal |
| `e2e mainnet` | Production adapters | Yes, for a bounded gate | No | Produce explicit pass/fail Mainnet evidence |

Use a separate `data_dir` for each mode. Do not point a manual test at a
production or otherwise valuable database.

## Build the binary

```bash
cd leani
cargo build --locked --release
mkdir -p data/manual-mainnet evidence
```

The commands below use `./target/release/leani` so the release binary
is built only once.

## Prepare a recent finality anchor

Copy the operational template:

```bash
cp config/example.toml config/manual-mainnet.toml
```

Edit at least these fields:

```toml
data_dir = "./data/manual-mainnet"

[finality]
kind = "consensus_p2p"
checkpoint = "<RECENT_FINALIZED_BEACON_BLOCK_ROOT>"
checkpoint_slot = <ITS_FINALIZED_BEACON_SLOT>
minimum_peers = 2
discovery_port = 0
```

Obtain the finalized root and slot independently. For example, query two
independently operated Beacon APIs and require the responses to agree:

```bash
curl -s "$BEACON_API/eth/v1/beacon/headers/finalized" \
  | jq '{root: .data.root, slot: .data.header.message.slot}'
```

Do not commit this checkpoint as an evergreen value. Record its sources and
acquisition time with the test evidence. The checkpoint is the bootstrap trust
anchor; `consensus_p2p` verifies subsequent light-client updates locally over
P2P.

Validate the configuration without opening the database:

```bash
./target/release/leani \
  --config config/manual-mainnet.toml \
  doctor
```

Then exercise the configured finality transport:

```bash
./target/release/leani \
  --config config/manual-mainnet.toml \
  source probe finality \
  --report evidence/manual-finality.json
```

Derive an inclusive 10,000-block range from the verified finalized execution
anchor:

```bash
jq -e '.accepted == true' evidence/manual-finality.json

TO=$(jq -r '.selected.execution_block_number' evidence/manual-finality.json)
FROM=$((TO - 9999))

echo "Testing blocks $FROM through $TO"
```

Set the configured processor start to the numeric value printed as `FROM`:

```toml
[[processors]]
id = "blobs-money"
version = "1.4.0"
start_block = <FROM>
publish = "optimistic_and_finalized"
retention = "full_output_history"
```

`serve` uses this configured value. The finite `e2e mainnet` command overrides
it with its `--from-block` argument.

## Recommended rehearsal: catch up and follow head

Start the node in terminal 1:

```bash
./target/release/leani \
  --config config/manual-mainnet.toml \
  --log-filter 'info,leani_source_p2p=debug' \
  serve
```

This launches the following work concurrently:

- verify the consensus checkpoint and seed the finalized execution anchor;
- start the execution-P2P head follower;
- backfill every configured processor through the finalized anchor;
- use the bounded P2P history bridge for the archive-to-head gap;
- compare the hot/cold overlap and persist a verified handoff; and
- expose the native API, HTTP JSON-RPC, and WebSocket JSON-RPC listeners.

The example configuration permits a 1,048,576-block P2P history fallback, so
the complete 10,000-block range is bridgeable even when the public archives
lag. The normal path still prefers capable retained/archive sources in
configured priority order; execution P2P remains the probabilistic last
resort.

In terminal 2, inspect process and processor readiness:

```bash
curl -s http://127.0.0.1:8080/health/ready | jq
```

```bash
curl -s http://127.0.0.1:8080/v1/network/status | jq
```

Open `http://127.0.0.1:8080/debug/network` for the same information as a
live-updating dashboard. It distinguishes the long-lived `live` session from
short-lived `history` sessions used to bridge dataset gaps. A zero-peer history
row does not mean the head follower is disconnected.

```bash
curl -s \
  http://127.0.0.1:8080/v1/processors/blobs-money/status \
  | jq
```

`/health/ready` reports the required network actors. It does not by itself
prove that the requested historical processor range is complete. The processor
status should eventually show:

- `configuredStartBlock` equal to `FROM`;
- one continuous `available` interval;
- `complete: true`;
- no missing range between `FROM` and `processedThrough`; and
- a `processedThrough` cursor that continues advancing with the head.

`complete: true` by itself is only the no-internal-gaps assertion. During
catch-up, `/v1/network/status` can therefore report
`storedRangeContiguous: true` and a non-zero `blocksRemaining` while
`readiness.liveReady` remains false. The dashboard renders this as “contiguous
so far” plus the current target instead of calling the node complete.
If the required network lane exits, inspect `network.supervisor.lastError` and
`retryInSeconds`; the dashboard displays the same failure and retry countdown
above the processor coverage.

The API and RPC listeners are:

```text
native HTTP/SSE:      http://127.0.0.1:8080
Ethereum JSON-RPC:    http://127.0.0.1:8545
Ethereum WebSocket:   ws://127.0.0.1:8546
```

## Consume blobs.money transformations

Open the stream while catch-up is still running:

```bash
curl -N --no-buffer \
  -H 'Accept: text/event-stream' \
  http://127.0.0.1:8080/v1/processors/blobs-money/stream
```

Without `after`, the native stream begins with the first retained durable
change, replays the backfill, and then remains attached to live changes. A
typical envelope contains:

```json
{
  "sequence": "12345",
  "cursor": "<opaque cursor>",
  "operation": "apply",
  "finality": "optimistic",
  "kind": "blobs.block.put",
  "block": {
    "number": 123,
    "hash": "0x..."
  },
  "data": {}
}
```

The blobs processor emits one `blobs.block.put` bundle per block. Its data
contains the block and all blob transactions for an atomic destination
update. The envelope operation communicates `apply`, `undo`, `finalized`, or
`reset_required`; consumers must not infer reorg semantics only from the
domain-change suffix.

To resume from the cursor of the last atomically committed event:

```bash
CURSOR='<opaque cursor from the previous event>'

curl -N --no-buffer \
  -H 'Accept: text/event-stream' \
  --get \
  --data-urlencode "after=$CURSOR" \
  http://127.0.0.1:8080/v1/processors/blobs-money/stream
```

The stream is at-least-once. A production consumer must apply the domain
change and save its opaque cursor in the same database transaction.

The current API and SDK do not accept a `finality` subscription query option.
Inspect or filter `event.finality` in the consumer. Do not append
`?finality=optimistic`; unknown change-stream query fields are rejected.

## Exercise the TypeScript SDK

The SDK can be imported directly from this workspace for a manual Bun
consumer:

```ts
import {
  createLeaniClient,
} from "./packages/sdk/src/index.ts";

const node = createLeaniClient({
  baseUrl: "http://127.0.0.1:8080",
  timeoutMs: 15_000,
});

let savedCursor: string | undefined;

for await (const event of node.blobs.subscribe({
  after: savedCursor,
})) {
  console.log({
    sequence: event.sequence,
    operation: event.operation,
    finality: event.finality,
    kind: event.kind,
    block: event.block?.number,
  });

  // The real application boundary should be:
  //
  // await database.transaction(async (transaction) => {
  //   await applyBlobChange(transaction, event);
  //   await saveCursor(transaction, event.cursor);
  // });

  savedCursor = event.cursor;
}
```

Run a saved script from the repository root with:

```bash
bun run ./manual-consumer.ts
```

The generic equivalent is
`node.processors.subscribe("blobs-money", { after })`.

## Query transformed output

Fetch a page of materialized blob blocks:

```bash
curl -s \
  "http://127.0.0.1:8080/v1/q/blobs/blocks?fromBlock=${FROM}&toBlock=${TO}&limit=3" \
  | jq
```

Inspect the first durable transformation records:

```bash
curl -s \
  "http://127.0.0.1:8080/v1/processors/blobs-money/changes?limit=5" \
  | jq '.data[] | {
      sequence,
      operation,
      finality,
      kind,
      block: .block.number,
      data
    }'
```

Range queries reject incomplete coverage by default. This prevents an
application from silently treating an archive or catch-up gap as an empty
result.

## Exercise Ethereum JSON-RPC

Query the current block:

```bash
curl -s http://127.0.0.1:8545 \
  -H 'content-type: application/json' \
  --data '{
    "jsonrpc":"2.0",
    "id":1,
    "method":"eth_blockNumber",
    "params":[]
  }' \
  | jq
```

Fetch the current full block:

```bash
curl -s http://127.0.0.1:8545 \
  -H 'content-type: application/json' \
  --data '{
    "jsonrpc":"2.0",
    "id":1,
    "method":"eth_getBlockByNumber",
    "params":["latest",true]
  }' \
  | jq
```

WebSocket JSON-RPC supports raw `newHeads` and filtered `logs`. Application
aggregations such as blobs output use the native durable SSE/SDK stream rather
than `eth_subscribe`.

## Historical-only mode

This mode isolates free public datasets from P2P and head following. Copy
`deploy/container.toml` to `config/manual-history.toml`, then configure local
bind addresses and a fresh data directory:

```toml
data_dir = "./data/manual-history-10k"

[sources.live]
kind = "disabled"
minimum_peers = 0
handoff_overlap_blocks = 32

[finality]
kind = "disabled"
checkpoint = ""
endpoints = []
minimum_agreement = 1

[[processors]]
id = "blobs-money"
version = "1.4.0"
start_block = <FROM>
publish = "optimistic_and_finalized"
retention = "full_output_history"

[rpc]
http_bind = "127.0.0.1:8545"
ws_bind = "127.0.0.1:8546"
historical_mode = "on_demand"
transaction_locator = false
minimum_recent_blocks = 128

[api]
bind = "127.0.0.1:8080"
```

Backfill the bounded range and verify the database:

```bash
./target/release/leani \
  --config config/manual-history.toml \
  backfill \
  --processor blobs-money \
  --from "$FROM" \
  --to "$TO"

./target/release/leani \
  --config config/manual-history.toml \
  db verify

./target/release/leani \
  --config config/manual-history.toml \
  db inspect
```

Start `serve` against the same database to query the result and replay its
durable native change stream:

```bash
./target/release/leani \
  --config config/manual-history.toml \
  serve
```

The native processor stream can replay committed historical transformations.
JSON-RPC WebSocket head subscriptions remain quiet because this profile has no
live source.

A historical-only backfill may fail near the current head when no configured
public dataset has published the complete requested range. That result
isolates archive availability; the continuous profile is responsible for
bridging the bounded archive-to-head gap over P2P.

## Controlled synthetic benchmarks

Synthetic performance evidence is deliberately excluded from GitHub Actions.
Run the checked suites on a quiet controlled host; cases execute sequentially
and record the exact commit, dirty state, host, binary, Rust, Cargo, and Bun
versions beside their reports:

```bash
scripts/run-controlled-benchmarks.sh smoke
scripts/run-controlled-benchmarks.sh synthetic-million
```

Outputs default to an ignored timestamped directory under
`.private/evidence/performance`. Set `LEANI_BENCHMARK_CASES` and
`LEANI_BENCHMARK_CORPORA` to select a comparison, or pass an explicit second
argument when evidence belongs on a dedicated local volume. The runner keeps
complete results when resuming and stops on partial output instead of silently
overwriting an interrupted measurement.

The `delivery-faults` and `all` suites require
`LEANI_BENCHMARK_POSTGRES_URL` to reference a dedicated disposable database.
Start the checked local service with:

```bash
docker compose --profile benchmark up -d benchmark-postgres
export LEANI_BENCHMARK_POSTGRES_URL=postgres://leani_benchmark:leani_benchmark@127.0.0.1:55432/leani_benchmark
scripts/run-controlled-benchmarks.sh delivery-faults
```

## Real-source delivery benchmark

`benchmark real-source` measures the production fixed-range path end to end:
real archive/dataset acquisition, processor mapping and durable publication,
gzip HTTP delivery, canonical event decoding/hashing, acknowledgement, and
pruning. Its built-in consumer intentionally does no application-database
work, so the result answers “how fast can the node index and deliver this
processor output to an immediately acknowledging consumer?”

Run benchmarks with the release binary, a fresh data directory, and an
immutable report path:

```bash
./target/release/leani \
  --config config/benchmarks/real-source.toml \
  benchmark real-source \
  --processor blobs-money \
  --source-policy xatu-only \
  --from-block 19426589 \
  --to-block 20426588 \
  --data-dir ./data/benchmark-xatu-blobs-1m \
  --timeout-seconds 7200 \
  --report evidence/benchmark-xatu-blobs-1m.json
```

The inclusive example contains exactly one million post-Dencun blocks. The
checked-in benchmark config also defines the single Uniswap V3 USDC/WETH 0.05%
pool processor and enables EraE. Select `erae-only`, use `p2p-only` to measure
the finalized execution-P2P fallback, or use `history-portfolio` to exercise
normal priority and failover. By default a P2P range may extend back to the
processor start block. If `sources.live.history_fallback_blocks` is configured,
the range must fit that explicit cost bound.

P2P results are highly sensitive to public-peer availability and block
payload size. Record cold and persisted-peer-cache runs separately, preserve
`execution-p2p-secret` plus `execution-peers.json` for the latter, and never
compare a recent receipt-complete run directly with a header-only or
state-snapshot sync headline. `material_request_blocks` is intentionally
tunable: the evidence-backed default is eight; partial/failed responses shrink
the working width, and larger ceilings must earn promotion through repeated
real-network runs.

For an immediate versus slow-consumer comparison, repeat the exact same range
and build with a fresh directory/report and add, for example,
`--consumer-delay-ms 100`. To make cross-source equivalence machine-checked,
pass the first successful report's `outputDigest` as
`--expected-output-digest` on the second run.

The report separates:

- source operation time, attempts/failures, assigned chunk ranges, frames,
  and normalized in-memory bytes;
- exact EraE successful response bytes/read attempts;
- Xatu source-object size, projected compressed column bytes, logical Parquet
  byte-range requests, and bytes returned to the reader (coalescing, caching,
  retries, HTTP headers, and failed partial responses remain below that
  adapter and are not mislabeled as physical HTTP requests);
- P2P aggregate and anchored-proof header requests, body/receipt requests,
  sparse bloom avoidance, retries, elapsed request time, and RLP response
  payload bytes before request-id framing and Snappy compression;
- sampled time to first source frame, first processor commit, and source
  completion, plus exact first HTTP batch, first acknowledgement, producer
  completion, and consumer completion timings;
- canonical payload bytes, HTTP encoded/transmitted/observed bytes, batches,
  acknowledgements, retained-delivery high-water, pruning, SQLite/WAL size,
  and peak RSS; and
- processor output digest, coverage, pending-output state, and optional digest
  agreement with another source run.

The wall-clock result includes real public-source network latency and local
processing in parallel. It is intentionally separate from deterministic
synthetic benchmarks, which establish stable processor and delivery ceilings.

## Finite Mainnet evidence gate

The Mainnet E2E command runs production ingestion and verification actors but
does not open the API listeners:

```bash
./target/release/leani \
  --config config/manual-mainnet.toml \
  e2e mainnet \
  --processor blobs-money \
  --from-block "$FROM" \
  --data-dir ./data/mainnet-e2e \
  --minimum-follow-blocks 16 \
  --stable-seconds 300 \
  --max-head-age-seconds 60 \
  --timeout-seconds 7200 \
  --report evidence/mainnet-e2e.json
```

After an intentional interruption, reuse that isolated state explicitly:

```bash
./target/release/leani \
  --config config/manual-mainnet.toml \
  e2e mainnet \
  --processor blobs-money \
  --from-block "$FROM" \
  --data-dir ./data/mainnet-e2e \
  --resume \
  --minimum-follow-blocks 16 \
  --stable-seconds 300 \
  --max-head-age-seconds 60 \
  --timeout-seconds 7200 \
  --report evidence/mainnet-e2e.json
```

For a quick diagnostic, two follow blocks and a 30-second stable window shorten
the run. They do not satisfy the stricter production evidence profile.

## Restart and cursor-resume exercise

For the most representative manual failure test:

1. start `serve` against a fresh 10,000-block configuration;
2. attach the blobs stream and save an event cursor;
3. stop the node with Ctrl-C during catch-up or live following;
4. restart `serve` with the same configuration and `data_dir`;
5. resume the stream with `after=<saved cursor>`;
6. confirm sequences remain monotonic and coverage remains continuous;
7. stop the node and run `db verify`; and
8. inspect storage attribution with `db inspect`.

The SDK handles URL encoding, reconnect backoff, duplicate cursor suppression,
and SSE framing. The application remains responsible for atomically applying
events and saving its cursor.

## Deterministic 10,000-block gate

Before spending time on public network behavior, run:

```bash
cargo test -p leani-runtime \
  ten_thousand_block_hot_cold_restart_converges_and_serves_coverage \
  --locked -- --nocapture
```

This uses the real runtime, SQLite store, reducers, handoff logic, restart
path, and native API with deterministic source adapters. It proves the internal
10,000-block invariants without depending on public peers or datasets.

## Diagnosing a stalled public run

Execution-peer availability is part of the test, not an implementation detail.
If no blocks arrive, check the logs for the connected execution-peer count
before investigating processors or the API.

The production example requires three execution peers. Lowering
`sources.live.minimum_peers` to two can be useful for a diagnostic run, but
record that change and do not treat it as evidence for a three-peer profile.
The finality P2P source has its own independent `finality.minimum_peers`.

Permit outbound TCP and UDP. For the configured fixed execution P2P port,
publish both TCP and UDP through Docker and the host firewall. Inbound
reachability improves discovery and peer diversity but is not a trust
requirement. Restricted NAT, firewalls, or peers that do not serve receipts can
delay the history bridge even when headers are available.

Use `/debug/network` to compare:

- connected peers and the manager age;
- successful, timed-out, and failed material requests;
- average local request-gate wait; and
- the current requested range and phase.

High queue wait means local work is competing for the available peer slots.
Low queue wait plus repeated timeouts points to remote serving quality. The
manager and its node identity should remain stable across these failures.

## Manual acceptance checklist

A successful application rehearsal demonstrates:

- verified finality bootstrap and a live execution-P2P lane;
- continuous processor coverage from `FROM` through the current cursor;
- a durable verified hot/cold handoff;
- transformed historical changes followed by new live changes on one stream;
- successful native blob queries for the tested range;
- supported recent JSON-RPC methods returning canonical material;
- restart without losing committed coverage;
- cursor resume without a gap;
- no pending ordered deltas after handoff;
- bounded recent raw storage; and
- a successful SQLite integrity check.
