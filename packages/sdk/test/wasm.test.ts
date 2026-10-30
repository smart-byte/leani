import { describe, expect, test } from "bun:test";

import { LeaniError, createLeaniClient } from "../src/index.ts";

interface Captured {
  method: string;
  url: string;
  headers: Record<string, string>;
  body: unknown;
}

function jsonResponse(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "content-type": "application/json" },
  });
}

/** A client whose node answers with `respond`, recording each request. */
function recording(respond: (request: Captured) => Response | Promise<Response>) {
  const requests: Captured[] = [];
  const client = createLeaniClient({
    baseUrl: "http://node.test",
    token: "secret-token-for-tests",
    fetch: async (input, init) => {
      const request = {
        method: init?.method ?? "GET",
        url: String(input),
        headers: Object.fromEntries(new Headers(init?.headers).entries()),
        body: typeof init?.body === "string" ? JSON.parse(init.body) : undefined,
      };
      requests.push(request);
      return respond(request);
    },
  });
  return { client, requests };
}

describe("Wasm routes", () => {
  test("templates, compose, and swap reach the node's Wasm routes", async () => {
    const { client, requests } = recording((request) => {
      if (request.url.endsWith("/v1/wasm/templates")) {
        return jsonResponse({
          data: [
            {
              name: "block-gas",
              description: "Gas per block",
              params: {},
              moduleHash: "0x01",
              moduleBytes: 4,
              sources: { rust: "fn x() {}", typescript: "export {}" },
            },
          ],
        });
      }
      if (request.url.endsWith("/v1/wasm/compose")) {
        return jsonResponse({
          name: "gas",
          language: "rust",
          path: "src/lib.rs",
          source: "// gas",
          files: ["Cargo.toml", "src/lib.rs"],
          fields: ["number"],
        });
      }
      return jsonResponse({ version: 2 }, 202);
    });

    const templates = await client.wasm.templates();
    expect(templates.map((template) => template.name)).toEqual(["block-gas"]);
    expect(templates[0]?.sources.typescript).toBe("export {}");

    const composed = await client.wasm.compose({
      name: "gas",
      data: { kind: "blocks" },
    });
    expect(composed.source).toBe("// gas");
    expect(requests[1]).toMatchObject({
      method: "POST",
      body: { name: "gas", data: { kind: "blocks" } },
      headers: { "content-type": "application/json" },
    });

    const swapped = await client.wasm.swap("run-1", {
      template: "erc20-transfers",
      params: { tokens: ["0xa0b8"] },
    });
    expect(swapped.version).toBe(2);
    expect(requests[2]).toMatchObject({
      method: "PUT",
      url: "http://node.test/admin/v1/wasm/runs/run-1/processor",
      body: { template: "erc20-transfers", params: { tokens: ["0xa0b8"] } },
      headers: { authorization: "Bearer secret-token-for-tests" },
    });
  });
});

describe("processor changes", () => {
  test("add posts the entry and module, and remove sends the request guard", async () => {
    const { client, requests } = recording((request) =>
      request.method === "POST"
        ? jsonResponse(
            {
              id: "wasm",
              instance: "gas",
              version: "1.0.0",
              codeHash: "0x01",
              configHash: "0x02",
              genericApi: "processor-v1",
              changeSchema: "wasm.block-gas.change.v1",
              queryExtensions: [],
            },
            201,
          )
        : jsonResponse({ instance: "gas", removed: true }),
    );

    const added = await client.processors.add({
      processor: { instance: "gas", start_block: 21_000_000 },
      template: "block-gas",
    });
    expect(added.instance).toBe("gas");
    expect(requests[0]).toMatchObject({
      method: "POST",
      url: "http://node.test/admin/v1/processors",
      body: {
        processor: { instance: "gas", start_block: 21_000_000 },
        template: "block-gas",
      },
    });

    const removed = await client.processors.remove("gas");
    expect(removed).toEqual({ instance: "gas", removed: true });
    expect(requests[1]).toMatchObject({
      method: "DELETE",
      url: "http://node.test/admin/v1/processors/gas",
      headers: { "x-leani-request": "1" },
    });
  });

  test("a refused change throws the node's error", async () => {
    const { client } = recording(() =>
      jsonResponse(
        {
          error: {
            code: "not_found",
            message:
              "this node does not change its processors through the API: set api.allow_processor_changes = true in its configuration",
            retryable: false,
          },
        },
        404,
      ),
    );
    const refused = await client.processors
      .add({ processor: { instance: "gas", start_block: 1 }, template: "block-gas" })
      .then(
        () => undefined,
        (error: unknown) => error,
      );
    expect(refused).toBeInstanceOf(LeaniError);
    expect((refused as LeaniError).code).toBe("not_found");
    expect((refused as LeaniError).message).toContain("allow_processor_changes");
  });
});

function events(...messages: [string, unknown][]): string {
  return messages
    .map(([event, data]) => `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`)
    .join("");
}

const READY = {
  run: "run-1",
  version: 1,
  manifest: { abi: 1 },
  moduleHash: "0x01",
  window: { fromBlock: 21_000_000, toBlock: 21_000_015 },
  follow: false,
  feed: "lane",
};
const BLOCK = {
  number: 21_000_000,
  hash: "0xaa",
  timestamp: 1_730_000_000,
  finality: "finalized",
  changes: [
    { kind: "block.gas.put", key: "0x01", operation: "upsert", undo: false, value: { gasUsed: 1 } },
  ],
};

function streaming(body: string | ReadableStream<Uint8Array>, status = 200) {
  return recording(() =>
    new Response(body, {
      status,
      headers: { "content-type": status === 200 ? "text/event-stream" : "application/json" },
    }),
  );
}

describe("Wasm runs", () => {
  test("a run yields its events in order and ends with done", async () => {
    const { client, requests } = streaming(
      events(
        ["ready", READY],
        ["block", BLOCK],
        ["attested", { throughBlock: 21_000_000, hash: "0xaa" }],
        ["done", { blocks: 1, changes: 1, elapsedMs: 12 }],
      ),
    );
    const seen = [];
    for await (const event of client.wasm.run({ template: "block-gas", blocks: 1 })) {
      seen.push(event);
    }
    expect(seen.map((event) => event.event)).toEqual(["ready", "block", "attested", "done"]);
    const block = seen[1];
    expect(block?.event === "block" && block.data.changes[0]?.value).toEqual({ gasUsed: 1 });
    expect(requests[0]).toMatchObject({
      method: "POST",
      url: "http://node.test/admin/v1/wasm/runs",
      body: { template: "block-gas", blocks: 1 },
      headers: { accept: "text/event-stream", "content-type": "application/json" },
    });
  });

  test("an error event rejects with the node's error", async () => {
    const { client } = streaming(
      events(
        ["ready", READY],
        ["error", { error: { code: "wasm_run_failed", message: "out of fuel", retryable: false } }],
      ),
    );
    const seen: string[] = [];
    const failed = await (async () => {
      for await (const event of client.wasm.run({ template: "block-gas" })) {
        seen.push(event.event);
      }
    })().then(
      () => undefined,
      (error: unknown) => error,
    );
    expect(seen).toEqual(["ready"]);
    expect(failed).toBeInstanceOf(LeaniError);
    expect((failed as LeaniError).code).toBe("wasm_run_failed");
  });

  test("a refused start rejects before any event", async () => {
    const { client } = streaming(
      JSON.stringify({
        error: { code: "wasm_run_capacity", message: "two runs are active", retryable: true },
      }),
      503,
    );
    const iterator = client.wasm.run({ template: "block-gas" });
    const refused = await iterator.next().then(
      () => undefined,
      (error: unknown) => error,
    );
    expect((refused as LeaniError).code).toBe("wasm_run_capacity");
  });

  test("aborting ends a following run quietly", async () => {
    const encoder = new TextEncoder();
    const controller = new AbortController();
    const body = new ReadableStream<Uint8Array>({
      start(stream) {
        // A following run stays open after its first block.
        stream.enqueue(encoder.encode(events(["ready", READY], ["block", BLOCK])));
      },
    });
    const { client } = streaming(body);
    const seen: string[] = [];
    for await (const event of client.wasm.run(
      { template: "block-gas", follow: true },
      { signal: controller.signal },
    )) {
      seen.push(event.event);
      if (event.event === "block") {
        controller.abort();
      }
    }
    expect(seen).toEqual(["ready", "block"]);
  });

  test("a stream that ends before done is an error", async () => {
    const { client } = streaming(events(["ready", READY]));
    const broken = await (async () => {
      for await (const _event of client.wasm.run({ template: "block-gas" })) {
        // The node goes away after ready.
      }
    })().then(
      () => undefined,
      (error: unknown) => error,
    );
    expect(String(broken)).toContain("before it was done");
  });
});
