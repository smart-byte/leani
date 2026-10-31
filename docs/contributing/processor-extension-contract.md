---
title: Processor extension contract
description: Understand the native processor factory, settings, map/reduce, query extension, generic API, and the Wasm processor boundary.
section: contributing
order: 20
audience:
  - processor-author
  - contributor
status: preview
---

The node is an embeddable Rust library as well as a standard binary. Downstream
projects can register application-specific native processors and compile their
own binary without forking or modifying node assembly.

The same boundary hosts sandboxed Wasm modules, which the standard binary
loads at runtime through its `wasm` factory:

```text
ProcessorRegistry
    +-- native ProcessorFactory (one per built-in or downstream processor)
    +-- WasmProcessorFactory   (`id = "wasm"`, one instance per configured module)
```

The source planner, historical/live runtimes, SQLite reducer, undo journal,
finality handling, generic API, and durable subscriptions operate on the
`Processor` trait and do not branch on how a processor was constructed.

## Current extension contract

An external binary supplies a `ProcessorRegistry`:

```rust
use leani::{ProcessorRegistry, run_with_registry};

let mut processors = ProcessorRegistry::standard();
processors.register(MyProcessorFactory)?;
run_with_registry(processors).await?;
```

`ProcessorRegistry::standard()` includes the checked-in blobs.money, ERC-20,
Uniswap, and Wasm factories. `ProcessorRegistry::new()` starts empty, allowing a
product-specific binary with no built-in application processor.
Package loaders can register discovered `Arc<dyn ProcessorFactory>` values
through `register_shared`.

A factory constructs an immutable processor from the common instance
configuration and its processor-owned settings table:

```rust
pub trait ProcessorFactory: Send + Sync {
    fn id(&self) -> &str;

    fn description(&self) -> &str {
        self.id()
    }

    fn create(
        &self,
        configured: &ProcessorConfig,
        context: ProcessorFactoryContext,
    ) -> Result<ProcessorComponents, ProcessorFactoryError>;
}
```

`ProcessorComponents` always contains the processor and may contain one
processor-owned `QueryExtension`. Returning them together prevents query
routes from being registered for a different processor instance.

Construction is deterministic and performs no network I/O; only the `wasm`
factory reads a file, its module. The registry rejects:

- unknown or duplicate processor IDs;
- invalid processor-owned settings;
- a returned descriptor with another ID or version;
- descriptor/configuration disagreement about start block, publication, or
  retention; and
- invalid descriptor requirements or schemas.

`doctor`, `serve`, `backfill`, `benchmark`, conformance, database inspection,
and the Mainnet E2E command all use the same supplied registry.

## Processor-owned settings

Common configuration remains strict:

```toml
[[processors]]
id = "my-protocol"
version = "1.0.0"
start_block = 18000000
retention = "full_output_history"

[processors.settings]
contracts = ["0x..."]
bucket_seconds = 3600
```

The node preserves `[processors.settings]` as a `toml::Table`. The selected
factory calls `ProcessorConfig::decode_settings<T>()` using a
processor-specific Serde type, normally with `deny_unknown_fields`.

The processor hashes the canonical meaning of its immutable settings into its
descriptor `config_hash`. Changing output-affecting settings therefore creates
a distinct durable processor instance.

Code works the same way: `version` and `code_hash` name what `map` and
`reduce` compute. Bump both whenever a change alters a delta, a reduced value,
or an emitted change, and run the result as a new instance, as in a
[processor rebuild](/docs/operations/runbook/#processor-rebuild-or-rollback).
The store cannot see code. A binary that maps blocks differently under an
unchanged identity fails only when it maps a block again that the old binary
already holds, such as a pending live delta at the next start's hot/cold
overlap, possibly hours later: the backfill then fails with `already has a
different pending delta`.

## Map, reduce, and changes

Every processor implements:

```rust
#[async_trait]
pub trait Processor: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn descriptor(&self) -> &ProcessorDescriptor;

    async fn map(&self, block: &BlockFrame)
        -> Result<EncodedDelta, ProcessorError>;

    fn finality_variant_checksums(
        &self,
        delta: &EncodedDelta,
    ) -> Result<Vec<BlockHash>, ProcessorError>;

    async fn reduce(
        &self,
        transaction: &mut dyn ReducerTransaction,
        cursor: &ProcessorCursor,
        delta: &EncodedDelta,
    ) -> Result<DomainChanges, ProcessorError>;

    fn change_json(
        &self,
        change: &DomainChange,
    ) -> Result<Option<serde_json::Value>, ProcessorError>;
}
```

`map` is deterministic and may execute in parallel. It converts requested
source material into a compact, versioned delta.

`finality_variant_checksums` defaults to the exact delta checksum. A processor
whose mapped payload embeds included/finalized state must override it by
decoding the validated delta and returning only the checksums produced by
changing that finality field. This lets restart recovery prove archive/live
equivalence even after the raw frame has been pruned; it must not treat any
other payload difference as equivalent.

`reduce` applies that delta through the core-owned transaction. Processors can
read and mutate namespaced collections/indexes and emit domain changes, but
cannot access SQLite directly. The core captures preimages and commits state,
coverage, cursor, undo, and changes atomically.

The core derives each change's undo from its key's mutation, so emitted
changes must describe each key's net mutation in the block: at most one change
per key, an upsert of the key's final value or a delete of a removed key. A
key the block created may carry a different change payload, because its undo
is a delete. A reducer that writes a key more than once in a block emits one
change, for the last write; the store rejects an unfinalized block whose
changes do not match its mutations.

A mapper must not fail a block because of a log another contract emitted. Any
contract can emit any topic zero, so a log that does not decode as the
expected event is skipped, and preferably counted in the delta, rather than
treated as invalid input.

`change_json` is an optional public rendering hook. Durable output remains the
processor's compact bytes. A processor with a stable JSON representation can
render those bytes for HTTP/SSE clients; otherwise the API publishes
schema-labelled hexadecimal data.

Every change envelope includes the descriptor's `change_schema`.

## Query extensions

Processors that need a domain-specific read API can return a native query
extension from their factory:

```rust
use std::sync::Arc;

use axum::{Router, routing::get};
use leani::{ProcessorComponents, QueryContext, QueryExtension};

#[derive(Debug)]
struct MyQueryExtension;

impl QueryExtension for MyQueryExtension {
    fn id(&self) -> &str {
        "my-protocol-v1"
    }

    fn alias(&self) -> Option<&str> {
        Some("my-protocol")
    }

    fn routes(&self) -> Router<QueryContext> {
        Router::new().route("/summary", get(summary))
    }
}

let processor = Arc::new(MyProcessor::new(configured)?);
let components = ProcessorComponents::new(processor)
    .with_query_extension(Arc::new(MyQueryExtension));
```

The canonical mount is always instance-scoped:

```text
GET /v1/processors/{instance}/query/summary
```

The optional `GET /v1/q/my-protocol/summary` alias is mounted only when exactly
one configured extension requests that alias. With multiple instances, every
canonical route remains available and the ambiguous alias is omitted.
`GET /v1/capabilities` reports the canonical and active alias paths.

`QueryContext` gives the extension bounded, read-only access to its owning
processor's entities and indexes, coverage, committed cursor, page limits,
and extension-scoped opaque cursors. It cannot read another processor's
namespace. Extension routes inherit the aggregate API's bearer authentication
and standard error envelope.

Entity scans observe latest committed state and are suitable for ordinary
queries. A consumer that needs a stable multi-page bootstrap followed by an
exact change boundary must use the generic `query-and-follow` operation.

## Generic API

All configured processors automatically receive:

```text
GET /v1/processors/{instance}/status
GET /v1/processors/{instance}/schema
GET /v1/processors/{instance}/changes
GET /v1/processors/{instance}/stream
```

Typed aliases are conditional. For example, `/v1/q/blobs/*` exists only when a
single configured blobs extension can claim that alias. A custom-only binary
remains a valid node. JSON-RPC progress is independent of query-extension
registration.

The SDK consumes custom changes through:

```ts
for await (const event of node.processors.subscribe<MyChange>(
  "my-protocol",
  { after: savedCursor },
)) {
  await database.transaction(async (transaction) => {
    await applyChange(transaction, event);
    await saveCursor(transaction, event.cursor);
  });
}
```

The delivery contract remains at-least-once. Applying the change and saving
the cursor in one consumer transaction is required.

## Complete example

The complete [`examples/custom-node` walkthrough](https://leani.dev/docs/guides/custom-processor/) is a separate downstream
binary inside the workspace. It:

- defines its own strict settings type;
- implements a block-summary processor;
- registers a native factory without changing `crates/node`;
- configures only that custom processor, with no blobs processor;
- exposes typed JSON changes through the generic stream; and
- exposes `GET /{number}` through a processor-owned query extension; and
- reuses the standard CLI and every node command.

Validate it:

```bash
cargo run -p leani-custom-example -- \
  --config examples/custom-node/node.toml \
  doctor --json
```

Run a bounded backfill:

```bash
cargo run -p leani-custom-example --release -- \
  --config examples/custom-node/node.toml \
  backfill \
  --processor example-block-summary \
  --from-block 19426589 \
  --to-block 19426688
```

Start the query service:

```bash
cargo run -p leani-custom-example --release -- \
  --config examples/custom-node/node.toml \
  serve
```

Consume its changes:

```bash
curl -N --no-buffer \
  http://127.0.0.1:9080/v1/processors/example-block-summary/stream
```

The processor instance is discoverable through `GET /v1/processors`. Query a
stored summary through its canonical route:

```bash
curl http://127.0.0.1:9080/v1/processors/<instance>/query/19426589
```

Because the example has one instance, its shorter
`/v1/q/block-summaries/19426589` alias is also available.

Running the standard binary against this configuration fails `doctor` with an
explicit unregistered-processor diagnostic. This proves that processor
availability belongs to the assembled binary, not a global hard-coded list.

## Security model

Native processors are trusted code linked into the operator's binary. They
provide maximum performance and the simplest debugging experience, but they
are not a multi-tenant sandbox.

Do not load native `.so`, `.dylib`, or `.dll` processors dynamically. Rust has
no stable native ABI for this contract, such libraries are platform-sensitive
and unsandboxed, and loading them would undermine the workspace's
`unsafe_code = "forbid"` policy.

The supported choices are:

1. statically linked native Rust processors; and
2. sandboxed, metered Wasm modules loaded by the standard binary.

## Wasm processors

`crates/processor-wasm` implements `Processor` over a Wasm module run by the
[wasmi](https://github.com/wasmi-labs/wasmi) interpreter;
`crates/wasm-guest` and `packages/wasm-guest` are the Rust and TypeScript
SDKs for writing one. The [Wasm processors guide](/docs/guides/wasm-processors/)
covers authoring, `leani processor run`, the playground, configuration,
limits, and measured performance, and `crates/wasm-guest/src/ffi.rs` documents
the ABI. What the host guarantees, so the runtime and store need no Wasm
special cases:

- **Identity.** `code_hash` is the module's BLAKE3 hash and `config_hash`
  covers the canonical parameters, so the store's drift check refuses a
  changed module or parameters under an existing instance. An optional
  `settings.blake3` pin refuses other module bytes at load.
- **Determinism.** Every describe, map, and reduce call runs in a fresh
  instance. The guest sees only the materials and field groups its manifest
  requires, reduced to its filter the way sources apply it, so every source
  maps a block alike, and never the block's finality, so the default
  exact-checksum `finality_variant_checksums` is correct. Floats follow the
  Wasm deterministic profile; no clock, randomness, network, filesystem, or
  WASI import exists.
- **Reduction.** `block_local` modules cannot read state, which keeps the
  block-versioned delivery contract honest; `ordered` modules read through
  the same `ReducerTransaction`, suspending the interpreter for each read.
  Writes queue in the host and apply in order before the next read and at the
  end, and the host publishes one change per key, the last emitted.
- **Outputs.** Entities and change payloads must be JSON, so
  `entity_json`/`change_json` render them without schemas; private state is
  opaque bytes.
- **Limits.** Module size, instance memory, fuel per call, value size,
  changes per block, and state accesses per block fail the call as a native
  bug would; the lane halts visibly rather than skip a block.

The `wasm` factory reads its module file during construction; the file is a
pinned local artifact, resolved relative to the configuration file, so
construction stays deterministic.

Not supported:

- adding or removing a persistent Wasm instance on a running node: a
  configuration change and a restart do it, and ad-hoc runs need neither;
- a JIT engine, such as wasmtime, or pooled instances;
- the component-model ABI;
- module composition and Substreams packages;
- signed packages and per-tenant resource accounting.
