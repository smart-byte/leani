# Leani custom processors

The node is an embeddable Rust library as well as a standard binary. Downstream
projects can register application-specific native processors and compile their
own binary without forking or modifying node assembly.

This is the first extension model. It is also the host boundary for a future
sandboxed Wasm package runtime:

```text
ProcessorRegistry
    +-- native ProcessorFactory
    +-- future WasmProcessorFactory
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
and Uniswap factories. `ProcessorRegistry::new()` starts empty, allowing a
product-specific binary with no built-in application processor.
Package loaders can register discovered `Arc<dyn ProcessorFactory>` values
through `register_shared`.

A factory constructs an immutable processor from the common instance
configuration and its processor-owned settings table:

```rust
pub trait ProcessorFactory: Send + Sync {
    fn id(&self) -> &str;

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

Construction is deterministic and performs no file or network I/O. The
registry rejects:

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
publish = "optimistic_and_finalized"
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
whose mapped payload embeds optimistic/safe/finalized state must override it by
decoding the validated delta and returning only the checksums produced by
changing that finality field. This lets restart recovery prove archive/live
equivalence even after the raw frame has been pruned; it must not treat any
other payload difference as equivalent.

`reduce` applies that delta through the core-owned transaction. Processors can
read and mutate namespaced collections/indexes and emit domain changes, but
cannot access SQLite directly. The core captures preimages and commits state,
coverage, cursor, undo, and changes atomically.

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

[`examples/custom-node`](../examples/custom-node) is a separate downstream
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
  --from 19426589 \
  --to 19426688
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

1. statically linked native Rust processors now; and
2. versioned, metered, sandboxed Wasm packages later.

## Wasm product path

A hosted product can implement `WasmProcessorFactory` over this registry. A
package will need:

- a signed/versioned manifest and content-addressed artifact;
- declared capabilities, filters, start point, schemas, publication, and
  retention;
- a stable component ABI for map/reduce/state/change operations;
- deterministic execution without ambient network, filesystem, clock, or
  randomness;
- memory, instruction/fuel, mutation, delta, and emitted-byte limits;
- schema-aware JSON/Protobuf rendering;
- package/version migrations and reproducible rebuilds; and
- per-tenant resource accounting and observability.

The first Wasm release should support one map/reduce processor per package.
Module DAG composition and restricted event-only Substreams package import can
follow after the package ABI and storage behavior are proven.

General Substreams compatibility is not implied. Packages requiring calls,
traces, state diffs, arbitrary host functions, or retained Firehose history
must fail capability negotiation when the configured node sources cannot
supply those inputs.
