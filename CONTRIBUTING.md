# Contributing to Leani

The project is built as a sequence of independently runnable vertical
checkpoints. Keep `main` buildable and make commits small enough that a
correctness or storage regression can be bisected.

## Contribution license

By submitting a contribution, you agree to license it under the
[MIT License](LICENSE). Third-party material must retain its applicable
license and attribution notices.

## Before changing a contract

Public source, processor, store, API, SDK, cursor, or durable encoding changes
need an ADR in `docs/adr/`. Compatibility changes must include migration,
golden-fixture, and downgrade behavior. Do not broaden RPC compatibility
without updating `docs/reference/ethereum-json-rpc.md` and adding differential tests.

## Local checks

Install the local publication guards once per clone before committing or
pushing:

```bash
scripts/install-gitleaks.sh .private/tools/bin
git config core.hooksPath "$PWD/scripts/git-hooks"
```

The hooks check staged files and commit messages, and inspect the complete
history of every pushed branch or tag. Private plans, agent instructions,
design explorations, raw reports, credentials, and local user paths belong
under ignored `.private/`. Deleting a file in a later commit does not remove
it from history. Never push archive, backup, or private refs; keep recovery
bundles under `.private/archives/`. Hooks can be bypassed, so release and CI
checks enforce the same policy independently. Pushes also run the pinned
credential scanner with redacted diagnostics; missing tooling fails closed.

```bash
cargo fmt --all --check
cargo test --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo deny check
python3 scripts/generate-third-party-licenses.py --check

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

`THIRD_PARTY_LICENSES.txt` is generated from `Cargo.lock` and shipped with
every binary distribution, so CI rejects a stale copy. After any dependency
change, install the pinned generator once with
`sh scripts/install-cargo-about.sh ~/.local/bin`, run
`scripts/generate-third-party-licenses.py`, and commit the result.

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
