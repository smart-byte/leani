# Contributing to Leani

The project is built as a sequence of independently runnable vertical
checkpoints. Keep `main` buildable and make commits small enough that a
correctness or storage regression can be bisected.

## Before changing a contract

Public source, processor, store, API, SDK, cursor, or durable encoding changes
need an ADR in `docs/adr/`. Compatibility changes must include migration,
golden-fixture, and downgrade behavior. Do not broaden RPC compatibility
without updating `docs/reference/ethereum-json-rpc.md` and adding differential tests.

## Local checks

```bash
cargo fmt --all --check
cargo test --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo deny check

cd packages/sdk
bun install --frozen-lockfile
bun run typecheck
bun test

cd ../../site
bun install --frozen-lockfile
bun run check
bun run examples:check
bun run build
```

Public documentation lives in `docs/`; the Astro frontend lives in `site/`.
Site preparation consumes tracked Rust-generated fixtures and requires only Bun.
After changing CLI help, processor metadata, configuration schema, or SDK
contracts, run `bun run docs:update` from `site/`; fixture regeneration also
requires the pinned Rust toolchain. Keep private plans and raw evidence under
the ignored `.private/` tree.

Tests that require public datasets or Ethereum peers must be marked as
integration tests, use bounded ranges, record the exact source/schema, and
produce a reusable report. Unit and default workspace tests stay offline.

## Commit and review convention

Use imperative conventional subjects (`feat:`, `fix:`, `test:`, `docs:`,
`chore:`). A commit may implement one work package without requiring a pull
request. Record material architectural decisions before or alongside the code.

Never commit API keys, private RPC URLs, live database exports, or unredacted
peer identity material. Report suspected vulnerabilities privately as
described in `SECURITY.md`.
