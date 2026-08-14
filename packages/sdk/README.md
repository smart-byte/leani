# `@leani/sdk`

Typed queries and resumable live/history delivery for [Leani](https://github.com/smart-byte/leani).

```ts
import { createLeaniClient } from "@leani/sdk";

const leani = createLeaniClient({ baseUrl: "http://127.0.0.1:8080" });
const status = await leani.status();
```

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

The client rejects absolute and protocol-relative URLs before attaching a
bearer token. Prefer the canonical instance-scoped `basePath` from capability
discovery; short aliases may be absent when several instances share one
extension.

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
available from `@leani/sdk/backfill`. Required consumers should commit the
application transaction before acknowledging the Leani-owned durable cursor.
Destination writes must be idempotent because a crash between those steps can
replay a change.

The browser build assumes a same-origin deployment or a reverse proxy that
adds the application's CORS policy. Leani does not enable cross-origin access
on its native API by default.
