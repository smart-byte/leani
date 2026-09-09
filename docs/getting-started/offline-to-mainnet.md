---
title: Offline and container diagnostics
description: Diagnose an installation offline and exercise the local container image.
section: getting-started
order: 60
audience:
  - app-developer
  - operator
status: preview
---

Use this page when you need to distinguish an installation problem from an
upstream network problem. For your first run, start with
[Install Leani](/docs/getting-started/install/) and
[follow a block](/docs/getting-started/).

## Native

The [offline fixture](/docs/getting-started/offline-proof/) uses deterministic
blocks with the production runtime and SQLite store. It reports 128 changes,
continuous coverage, and database integrity without requiring a network.

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

After the offline check passes, follow
[Index a bounded Mainnet range](/docs/getting-started/bounded-mainnet/).
It includes a matching source configuration, decoded event queries, and
coverage checks.

## Durable external consumer

For a complete destination setup, use
[Consume changes into PostgreSQL](/docs/guides/postgres-consumer/).
The guide specifies its node profile, database, registration secret, and
restart check.
