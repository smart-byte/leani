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

The image's default command serves `/etc/leani/node.toml`, which protects the
API with a bearer token from `LEANI_API_TOKEN` and refuses to start without
one of at least 16 printable ASCII characters without spaces:

```bash
export LEANI_API_TOKEN="$(openssl rand -hex 32)"
docker run --rm --env LEANI_API_TOKEN \
  --publish 127.0.0.1:8080:8080 \
  --volume leani-data:/var/lib/leani \
  leani:local
```

Send it as `Authorization: Bearer $LEANI_API_TOKEN` to every route except the
operational ones: `/health/*`, `/metrics`, `/v1/network/status`, and
`/debug/network`. See the [runbook](/docs/operations/runbook/) for the rest
of the container profile.

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
