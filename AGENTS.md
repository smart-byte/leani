# AGENTS.md

Guidance for AI coding agents working in this repository. Everything in
[CONTRIBUTING.md](CONTRIBUTING.md) applies as well.

## Project

Leani is a self-hosted Ethereum indexing node and CLI. It follows Mainnet over
P2P, backfills history, runs processors over blocks, transactions, and logs,
and serves the results through a local HTTP API, a TypeScript SDK, and a
JSON-RPC subset. It is pre-v1.

- `crates/node` (package `leani`): configuration, CLI, and process lifecycle.
- `crates/runtime`: historical scheduling, mapping, reduction, and restart.
- `crates/source-*`, `crates/finality-*`: history, live, and finality sources;
  `source-api` holds their contracts.
- `crates/processor-*`: built-in processors; `processor-api` holds the contract,
  and `processor-wasm` runs sandboxed Wasm processors.
- `crates/wasm-guest`, `packages/wasm-guest`: Rust and TypeScript SDKs for
  writing Wasm processors.
- `crates/store-*`, `crates/api`, `crates/rpc`: storage, HTTP API, JSON-RPC.
- `crates/primitives`, `crates/testkit`: shared types, fixtures, conformance helpers.
- `packages/sdk`: TypeScript client SDK. `site/`: Astro site. `docs/`: public docs.
- `skills/leani`: the agent skill for people using leani, not for working on it.

Architecture: [docs/contributing/architecture.md](docs/contributing/architecture.md).

## Commands

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test -p leani-runtime --locked   # affected packages first
cargo test --workspace --locked
```

SDK, site, license, and dependency checks are listed in CONTRIBUTING.md. A
failing check is caused by your change until you show it also fails on `main`.

## Changes

- Correctness comes first, reviewer fatigue second.
- Keep the diff minimal. Do not touch unrelated code: no drive-by renames,
  reformatting, reworded comments, or typo fixes.
- Prefer the design that removes code. When a change leaves code unused,
  delete it.
- Add code only with a caller in the same change: no unused functions,
  parameters, configuration keys, CLI flags, or features.
- Keep one source of truth. Search the workspace for an existing helper, type,
  or constant before adding one.
- Treat data from peers, datasets, and RPC endpoints as untrusted: validate it,
  and bound every allocation, queue, and wait it can drive.
- Public source, processor, store, API, SDK, cursor, and durable-encoding
  contracts are versioned. Before v1, prefer a clean break with a documented
  migration or rebuild path over compatibility shims, aliases, or legacy
  fallbacks. Code that was never released needs no compatibility at all.
- Never edit generated output by hand. After changing CLI help, processor
  metadata, configuration schema, or SDK contracts, run `bun run docs:update`
  in `site/`. After changing the Wasm guest SDK or `examples/wasm-processors`,
  rebuild the prebuilt modules with `sh scripts/build-wasm-processors.sh`
  (needs Docker).

## Ask first

- Before a change that makes existing nodes re-index or rebuild, such as a new
  durable format, processor identity, or instance hash.
- Before adding, removing, or upgrading a dependency. Update only what the task
  needs (`cargo update -p <crate>`), never the whole lockfile.
- Before editing CI or release workflows, and before tagging, publishing, or
  dispatching a workflow.

## Tests

- Reproduce a bug with a failing test before fixing it, and keep that test.
- Never skip, ignore, or weaken a failing or flaky test to get a green run. Fix
  the cause, or report it with a reproduction.
- Default workspace tests stay offline. Tests that need public datasets or
  Ethereum peers are integration tests with bounded block ranges.
- Experiment and benchmark on small block ranges and extrapolate. Never start
  a full backfill or a download of tens of gigabytes locally.
- Wait on conditions instead of sleeping.
- Before deleting a test as redundant, show that another test fails when the
  line it guards breaks.

## Comments

- Prefer self-documenting code. Comment only what the code cannot say: the why
  behind a non-obvious choice, an invariant, a workaround, a spec or EIP
  reference, an edge case.
- A comment states one non-obvious fact. Do not restate the code or narrate
  the change.
- When in doubt, leave the comment out.
- Comments must make sense on `main` to a reader who never saw your session:
  no references to the task, a review, the previous version, or what is "new",
  and no dates, incident stories, or lists of callers.
- Never commit commented-out code.
- Keep existing comments and TODOs when moving code. When behavior changes,
  update everything that describes it: doc comments, error and `expect`
  messages, docs, and the changelog.
- Document new public items: when they are called, ordering guarantees, and
  what `None`, `false`, or an error means.

## Privacy

- Plans, raw evidence, design explorations, and scratch files go under the
  ignored `.private/`. Hooks and CI reject them elsewhere, and deleting a
  committed file does not remove it from history.
- Never commit local user paths, credentials, private RPC URLs, database
  exports, or peer identity material. Never bypass the hooks with
  `--no-verify`.

## Commits and pull requests

- Conventional, imperative subjects with a short scope, e.g.
  `fix(p2p): reuse history proofs across detours`. Mark a breaking change with
  `!`, as in `feat(blobs,xatu)!:`. The body explains why.
- Write commit messages and pull request descriptions for readers who never
  saw your session: no prompts, iteration history, or "as requested".
- User-visible changes get an entry under `[Unreleased]` in `CHANGELOG.md`.
- Fill in the pull request template. Back performance claims with measurements.

## This file

Add a rule only when agents got something wrong more than once and the rule is
specific. Do not edit this file as part of other work; suggest additions in
your summary.
