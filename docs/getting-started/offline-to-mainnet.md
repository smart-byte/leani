---
title: Offline proof to Mainnet
description: Prove a Leani release without network access, then move to a bounded Mainnet range and durable consumer.
section: getting-started
order: 20
audience:
  - app-developer
  - operator
status: preview
---

The first run is deterministic and network-free. It exercises the distributed
binary, historical runtime, processor reduction, schema migrations, delivery
spool, checkpoints, coverage, and SQLite integrity.

## Native

Requirements: the pinned Rust toolchain and a writable temporary directory.

```bash
git clone https://github.com/smart-byte/leani.git leani
cd leani
cargo build --locked -p leani
mkdir -p /tmp/leani-quickstart
./target/debug/leani e2e fixture \
  --data-dir /tmp/leani-quickstart \
  --blocks 128 \
  --report /tmp/leani-quickstart/report.json
```

Success prints a JSON report with `"status": "passed"`,
`"databaseVerified": true`, one continuous `1..128` coverage interval, and
128 changes. The command refuses an existing database so an accidental rerun
cannot overwrite evidence; choose another directory for a second run.

Validate a real lifecycle profile without opening a database or network:

```bash
./target/debug/leani \
  --config config/modes/windowed.toml \
  doctor --json
```

## Docker

```bash
docker build --tag leani:local .
docker volume create leani-quickstart
docker run --rm \
  --volume leani-quickstart:/var/lib/leani \
  leani:local \
  e2e fixture \
  --data-dir /var/lib/leani/fixture \
  --blocks 128 \
  --report /var/lib/leani/fixture/report.json
```

The runtime image runs as UID/GID 10001 and the named volume remains available
for inspecting `report.json` and `leani.sqlite`. Remove it explicitly
when finished:

```bash
docker volume rm leani-quickstart
```

The image also contains all six lifecycle examples:

```bash
docker run --rm leani:local \
  doctor --config /etc/leani/examples/externalized.toml --json
```

## Bounded public event example

This step uses the public Xatu dataset and therefore requires outbound HTTPS.
It retains an hourly-bucketed window of Uniswap V2 `Sync` events without
running an EVM:

```bash
cargo run --locked --release -p leani -- \
  --config config/modes/windowed.toml \
  backfill --processor evm-events --from 10000835 --to 10001834
```

Start the historical-only API over the committed database:

```bash
cargo run --locked --release -p leani -- \
  --config config/modes/windowed.toml serve
```

In another terminal:

```bash
curl -s http://127.0.0.1:8080/v1/processors/evm-events/collections
curl -s \
  'http://127.0.0.1:8080/v1/processors/evm-events/collections/uniswap_v2.sync_hourly/entities?limit=10'
```

The collection route returns retained-window metadata and the entity route
returns a stable snapshot cursor, query/follow boundary, actual row/byte cost,
coverage, and decoded JSON entities. A range outside the retained window
returns `output_not_retained` with an explicit rebuild/source-scan action; it
never masquerades as an empty result.

The profile intentionally disables head following. To continue at verified
head, copy the execution-P2P and independently sourced finality settings from
`config/example.toml`, use a fresh `data_dir`, run `doctor`, and then `serve`.

## Durable external consumer

For the externalized profile, register the configured required consumer before
backfill using `earliest_retained`. Store the returned consumer credential
outside TOML. Read changes with that credential, commit each destination
transaction and cursor atomically, then acknowledge the cursor. See
[native API and delivery protocol](/docs/reference/native-api-and-delivery/) for
exact semantics and the commit-then-ack helper.
