# `@smart-byte/leani-sdk`

Typed queries and resumable live/history delivery for [Leani](https://github.com/smart-byte/leani).

```ts
import { createLeaniClient } from "@smart-byte/leani-sdk";

const leani = createLeaniClient({ baseUrl: "http://127.0.0.1:8080" });
const status = await leani.status();
```

Processor status and query responses expose inclusive `available` ranges split
at coverage gaps and exact finality boundaries. Filter those intervals when
selecting finalized material: `finalizedThrough` is only the highest covered
block recorded as finalized, not a guarantee that every lower block is
finalized or present.

Processor-owned query extensions are discoverable through
`leani.capabilities()`. Call a custom extension through the same authenticated,
same-origin request path used by the built-in wrappers:

```ts
interface Summary {
  indexedBlocks: string;
}

const summary = await leani.request<Summary>(
  "v1/processors/my-instance/query/summary",
);
```

The client rejects absolute and protocol-relative URLs, path segments that
decode to `.` or `..`, backslashes, encoded slashes, backslashes, and percent
signs, and control characters before attaching a bearer token. Every path
resolves under the base URL's path, one starting with `/` included, and the SDK
does not follow redirects. Prefer the canonical instance-scoped `basePath` from
capability discovery; short aliases may be absent when several instances share
one extension.

When an alias is absent, pass the discovered canonical path to the typed
helper instead of giving up its response types:

```ts
const capabilities = await leani.capabilities();
const blobs = capabilities.queryExtensions.find(
  (extension) => extension.id === "blobs-v1",
);
const scoped = createLeaniClient({
  baseUrl: "http://127.0.0.1:8080",
  queryBasePaths: { blobs: blobs?.basePath },
});
```

Historical subscription orchestration and unified live/history iterators are
available from `@smart-byte/leani-sdk/backfill`. Required consumers should commit the
application transaction before acknowledging the Leani-owned durable cursor.
Destination writes must be idempotent because a crash between those steps can
replay a change. Acknowledge a batch's `ackCursor` (or `acknowledgeableCursor`),
never a change's own cursor; a backfill session's `deliveryBatches()` yields
every such boundary with its changes, while `events()` yields changes only.

The browser build assumes a same-origin deployment or a reverse proxy that
adds the application's CORS policy. Leani does not enable cross-origin access
on its native API by default, and refuses a browser origin that is neither
loopback nor listed in `api.allowed_origins`. The SDK's mutations without a
JSON body send `x-leani-request: 1`, which the node requires.

Typed subscriptions accept a configured `processor` instance when a kind is
ambiguous. `blobs.subscribe()` yields a `{ block, transactions }` payload on
apply. Check `operation` before reading processor data: a finalized event has
`{ throughBlock }` instead. Pass an `AbortSignal` to stop an idle stream;
leaving the loop cancels its response body.

`queryAndFollow()` returns the first snapshot page and an atomic stream boundary.
Read every `nextCursor` with `queryEntities()`, release the snapshot in `finally`,
and then subscribe from `boundaryCursor`. See the executable
[query/follow example](../../examples/sdk-stream/index.ts) and its
[setup guide](https://leani.dev/docs/guides/query-then-follow/).

When delivery is disabled, `queryEntities()` still returns retained output,
with `boundaryCursor` and `recovery.follow` set to `null`. `queryAndFollow()`
requires delivery and otherwise returns HTTP 409 `delivery_disabled`.

Both package entry points use `LeaniError` for HTTP failures, including status,
code, retryability, and request ID. A failure below HTTP, such as a refused or
reset connection or a silent stream, is a retryable `TransportError`, a
`LeaniError` with status 0. ResetRequiredError is a LeaniError with
`code: "cursor_expired"`; rebuild application state from a fresh snapshot.

## License

[MIT](LICENSE).

Development uses the TypeScript 7 native compiler through `@typescript/native`.
The pinned `typescript` 5.9 package supplies the JavaScript compiler API required
by `openapi-typescript`; build and typecheck scripts invoke the native compiler
explicitly so installing both packages cannot change which compiler runs.
