import { describe, expect, test } from "bun:test";
import type { Finality, OpenApiComponents } from "../src/index.ts";

import {
  LeaniError,
  applyEntityChange,
  commitThenAcknowledge,
  compareSequences,
  createInMemoryCursorStore,
  createLeaniClient,
  parseSse,
  SDK_VERSION,
  type ChangeEnvelope,
  type StreamHello,
} from "../src/index.ts";

function jsonResponse(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function change(sequence: string, cursor = `cursor-${sequence}`): ChangeEnvelope & { operation: "apply" } {
  return {
    apiVersion: "1",
    sequence,
    cursor,
    operation: "apply",
    originKind: "live",
    originId: "blobs-production",
    publicationRevision: "0",
    chainId: 1,
    block: null,
    finality: "finalized",
    kind: "blobs.block.put",
    schema: "blobs.change.v1",
    key: "0x01",
    data: { blockNumber: 1 },
    emittedAt: "1970-01-01T00:00:00.001Z",
  };
}

describe("createLeaniClient", () => {
  test("normalizes the base URL and exposes a version", () => {
    const client = createLeaniClient({
      baseUrl: "http://127.0.0.1:8080/prefix",
      fetch: async () => jsonResponse({}),
    });

    expect(client.baseUrl.href).toBe("http://127.0.0.1:8080/prefix/");
    expect(SDK_VERSION).toBe("0.1.0-rc.1");
  });

  test("extension requests preserve base paths without leaking credentials", async () => {
    const requests: Request[] = [];
    const client = createLeaniClient({
      baseUrl: "http://node.test/prefix",
      token: "secret",
      fetch: async (input, init) => {
        requests.push(new Request(input, init));
        return jsonResponse({ ok: true });
      },
    });

    await client.request<{ ok: boolean }>("v1/custom", {
      included: 1,
      skipped: undefined,
    });
    await client.request<{ ok: boolean }>("/v1/custom", { enabled: true });

    expect(requests[0]?.url).toBe(
      "http://node.test/prefix/v1/custom?included=1",
    );
    expect(requests[1]?.url).toBe("http://node.test/v1/custom?enabled=true");
    expect(requests[0]?.headers.get("authorization")).toBe("Bearer secret");
    await expect(
      client.request("https://attacker.invalid/steal"),
    ).rejects.toThrow("must not contain an origin");
    await expect(client.request("//attacker.invalid/steal")).rejects.toThrow(
      "must not contain an origin",
    );
    expect(requests).toHaveLength(2);
  });

  test("extension requests propagate cancellation", async () => {
    const controller = new AbortController();
    const client = createLeaniClient({
      baseUrl: "http://node.test",
      fetch: async (_input, init) =>
        await new Promise<Response>((_resolve, reject) => {
          init?.signal?.addEventListener(
            "abort",
            () => reject(init.signal?.reason),
            { once: true },
          );
        }),
    });

    const request = client.request("/v1/custom", undefined, {
      signal: controller.signal,
    });
    controller.abort(new Error("cancelled"));
    await expect(request).rejects.toThrow("cancelled");
  });

  test("encodes blob queries and authorization without leaking abstractions", async () => {
    let request: Request | undefined;
    const client = createLeaniClient({
      baseUrl: "http://node.test",
      token: "secret",
      fetch: async (input, init) => {
        request = new Request(input, init);
        return jsonResponse({ data: [], nextCursor: null, coverage: {} });
      },
    });

    await client.blobs.listBlocks({
      fromBlock: 10,
      toBlock: 20,
      limit: 5,
      allowPartial: true,
    });

    expect(request?.url).toBe(
      "http://node.test/v1/q/blobs/blocks?fromBlock=10&toBlock=20&limit=5&allowPartial=true",
    );
    expect(request?.headers.get("authorization")).toBe("Bearer secret");
  });

  test("maps stable error bodies", async () => {
    const client = createLeaniClient({
      baseUrl: "http://node.test",
      fetch: async () =>
        jsonResponse(
          {
            error: {
              code: "range_incomplete",
              message: "gap",
              retryable: true,
              requestId: "req-1",
            },
          },
          409,
        ),
    });

    const error = await client.blobs
      .getBlock(1)
      .catch((caught: unknown) => caught);
    expect(error).toBeInstanceOf(LeaniError);
    expect((error as LeaniError).code).toBe("range_incomplete");
    expect((error as LeaniError).requestId).toBe("req-1");
  });

  test("supports canonical query-extension base paths and transaction cursors", async () => {
    const requests: string[] = [];
    const client = createLeaniClient({
      baseUrl: "http://node.test/prefix/",
      queryBasePaths: {
        blobs: "/v1/processors/blobs-production/query/",
      },
      fetch: async (input) => {
        requests.push(new Request(input).url);
        return jsonResponse({ data: [], nextCursor: null, coverage: {} });
      },
    });

    await client.blobs.getBlock(10);
    await client.blobs.listTransactions({
      blockNumber: 10,
      limit: 1,
      cursor: "next-page",
    });

    expect(requests).toEqual([
      "http://node.test/v1/processors/blobs-production/query/blocks/10",
      "http://node.test/v1/processors/blobs-production/query/transactions?blockNumber=10&limit=1&cursor=next-page",
    ]);
  });

  test("exposes operational health separately from processor queries", async () => {
    let requested = "";
    const client = createLeaniClient({
      baseUrl: "http://node.test",
      fetch: async (input) => {
        requested = new Request(input).url;
        return jsonResponse({
          status: "ready",
          uptimeSeconds: 1,
          database: "available",
          readiness: {
            liveRequired: false,
            liveReady: false,
            finalityRequired: false,
            finalityReady: false,
            ready: true,
          },
          reasons: [],
        });
      },
    });

    const health = await client.health.ready();
    expect(requested).toBe("http://node.test/health/ready");
    expect(health.status).toBe("ready");
  });

  test("provides typed ERC-20 and Uniswap query paths", async () => {
    const requests: string[] = [];
    const client = createLeaniClient({
      baseUrl: "http://node.test",
      fetch: async (input) => {
        requests.push(new Request(input).url);
        return jsonResponse({});
      },
    });
    const token = `0x${"11".repeat(20)}` as const;
    const account = `0x${"22".repeat(20)}` as const;
    await client.erc20.getBalance(token, account);
    await client.uniswap.getPool(token);
    await client.uniswap.getLatestObservation(token);
    expect(requests).toEqual([
      `http://node.test/v1/q/erc20/balances/${token}/${account}`,
      `http://node.test/v1/q/uniswap/pools/${token}`,
      `http://node.test/v1/processors/uniswap-observations/query/pools/${token}/latest`,
    ]);
  });

  test("exposes race-free snapshot bootstrap endpoints", async () => {
    const requests: string[] = [];
    const client = createLeaniClient({
      baseUrl: "http://node.test",
      fetch: async (input) => {
        requests.push(new Request(input).url);
        return jsonResponse({
          data: [],
          nextCursor: null,
          coverage: {},
        });
      },
    });

    await client.processors.changeHead("blobs-money");
    await client.blobs.listSnapshot({
      fromBlock: 10,
      toBlock: 20,
      limit: 500,
    });

    expect(requests).toEqual([
      "http://node.test/v1/processors/blobs-money/changes/head",
      "http://node.test/v1/q/blobs/snapshot?fromBlock=10&toBlock=20&limit=500",
    ]);
  });

  test("posts generic query-and-follow and scopes consumer acknowledgements", async () => {
    const requests: Request[] = [];
    const client = createLeaniClient({
      baseUrl: "http://node.test",
      fetch: async (input, init) => {
        requests.push(new Request(input, init));
        return jsonResponse({});
      },
    });

    await client.processors.queryAndFollow("prices-mainnet", "prices", {
      fromBlock: 10,
      limit: 25,
    });
    await client.processors.consumers.acknowledge(
      "prices-mainnet",
      "warehouse",
      "consumer-secret",
      "opaque-cursor",
    );

    expect(requests[0]?.method).toBe("POST");
    expect(requests[0]?.url).toBe(
      "http://node.test/v1/processors/prices-mainnet/collections/prices/query-and-follow",
    );
    expect(await requests[0]?.json()).toEqual({
      fromBlock: 10,
      limit: 25,
    });
    expect(requests[1]?.headers.get("x-leani-consumer-credential")).toBe(
      "consumer-secret",
    );
    expect(await requests[1]?.json()).toEqual({ cursor: "opaque-cursor" });
  });
});

describe("SSE", () => {
  test("parses events split across arbitrary UTF-8 chunks", async () => {
    const encoded = new TextEncoder().encode(
      ': heartbeat\n\nid: c1\nevent: apply\ndata: {"sequence":"1"}\n\n',
    );
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(encoded.slice(0, 7));
        controller.enqueue(encoded.slice(7, 31));
        controller.enqueue(encoded.slice(31));
        controller.close();
      },
    });

    const messages = [];
    for await (const message of parseSse(body)) {
      messages.push(message);
    }
    expect(messages).toEqual([
      { id: "c1", event: "apply", data: '{"sequence":"1"}' },
    ]);
  });

  test("keeps CRLF atomic when it is split across chunks", async () => {
    const encoder = new TextEncoder();
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(encoder.encode("data: first\r"));
        controller.enqueue(encoder.encode("\ndata: second\r"));
        controller.enqueue(encoder.encode("\r"));
        controller.enqueue(encoder.encode("\n"));
        controller.close();
      },
    });

    const messages = [];
    for await (const message of parseSse(body)) {
      messages.push(message);
    }
    expect(messages).toEqual([{ data: "first\nsecond" }]);
  });

  test("stream surfaces hello via onHello and does not yield it", async () => {
    const controller = new AbortController();
    const hello = {
      apiVersion: "1",
      chainId: 1,
      processor: {
        id: "blobs-money",
        instance: "blobs-production",
        version: "1.3.0",
        codeHash: "0x01",
        configHash: "0x02",
        genericApi: "processor-v1",
        changeSchema: "blobs.block-bundle.v1",
        queryExtensions: [
          {
            id: "blobs-v1",
            basePath: "/v1/processors/blobs-production/query",
            aliasPath: "/v1/q/blobs",
          },
        ],
        subscriptions: true,
        artifactRetention: "none",
        deliveryOrdering: "block_versioned_idempotent",
      },
      coverage: {
        chainId: 1,
        available: [],
        configuredStartBlock: 19_426_589,
        processedThrough: 1,
        finalizedThrough: 1,
        complete: true,
        state: "live",
      },
    };
    const client = createLeaniClient({
      baseUrl: "http://node.test",
      reconnect: { initialDelayMs: 0, maxDelayMs: 0, jitter: 0 },
      fetch: async () => {
        const payload =
          `event: hello\ndata: ${JSON.stringify(hello)}\n\n` +
          `id: ${change("1").cursor}\nevent: apply\ndata: ${JSON.stringify(change("1"))}\n\n`;
        return new Response(payload, {
          headers: { "content-type": "text/event-stream" },
        });
      },
    });

    const hellos: StreamHello[] = [];
    const received: string[] = [];
    for await (const event of client.blobs.subscribe({
      signal: controller.signal,
      onHello: (h) => hellos.push(h),
    })) {
      received.push(event.sequence);
      controller.abort();
    }
    expect(hellos.length).toBe(1);
    expect(hellos[0]!.processor.id).toBe("blobs-money");
    expect(received).toEqual(["1"]);
  });

  test("subscription suppresses duplicate sequences and advances resume cursor", async () => {
    const controller = new AbortController();
    let calls = 0;
    const client = createLeaniClient({
      baseUrl: "http://node.test",
      reconnect: { initialDelayMs: 0, maxDelayMs: 0, jitter: 0 },
      fetch: async () => {
        calls += 1;
        const events =
          calls === 1
            ? [change("1"), change("1"), change("2")]
            : [change("3")];
        const payload = events
          .map(
            (event) =>
              `id: ${event.cursor}\nevent: apply\ndata: ${JSON.stringify(event)}\n\n`,
          )
          .join("");
        return new Response(payload, {
          headers: { "content-type": "text/event-stream" },
        });
      },
    });

    const received: string[] = [];
    for await (const event of client.blobs.subscribe({
      signal: controller.signal,
    })) {
      received.push(event.sequence);
      if (received.length === 3) {
        controller.abort();
      }
    }
    expect(received).toEqual(["1", "2", "3"]);
    expect(calls).toBe(2);
  });
});

describe("helpers", () => {
  test("compares sequences outside JavaScript's safe integer range", () => {
    expect(compareSequences("900719925474099300", "900719925474099299")).toBe(1);
    expect(compareSequences("1", "1")).toBe(0);
    expect(() => compareSequences("-1", "0")).toThrow();
  });

  test("applies put/delete changes and stores cursors explicitly", async () => {
    const values = new Map<string, unknown>();
    const target = {
      put: (key: string, value: unknown) => {
        values.set(key, value);
      },
      delete: (key: string) => {
        values.delete(key);
      },
    };
    await applyEntityChange(target, change("1"));
    expect(values.has("0x01")).toBe(true);
    await applyEntityChange(target, {
      ...change("2"),
      kind: "blobs.block.delete",
      data: null,
    });
    expect(values.has("0x01")).toBe(false);

    const cursors = createInMemoryCursorStore();
    cursors.set("c1");
    expect(cursors.get()).toBe("c1");
    cursors.clear();
    expect(cursors.get()).toBeUndefined();
  });

  test("commits destination state before acknowledging", async () => {
    const order: string[] = [];
    const result = await commitThenAcknowledge(
      async () => {
        order.push("commit");
        return "cursor-7";
      },
      async (cursor) => {
        order.push(`ack:${cursor}`);
      },
    );
    expect(result).toBe("cursor-7");
    expect(order).toEqual(["commit", "ack:cursor-7"]);

    let acknowledged = false;
    await expect(
      commitThenAcknowledge(
        async () => {
          throw new Error("destination failed");
        },
        async () => {
          acknowledged = true;
        },
      ),
    ).rejects.toThrow("destination failed");
    expect(acknowledged).toBe(false);
  });
});

describe("stream recovery and resource ownership", () => {
  const frame = (value: unknown) => new TextEncoder().encode(`event: apply\ndata: ${JSON.stringify(value)}\n\n`);

  test("reconnects from the last complete event when EOF truncates a frame", async () => {
    const urls: string[] = [];
    const client = createLeaniClient({
      baseUrl: "http://node.test",
      reconnect: { initialDelayMs: 0, maxDelayMs: 0, jitter: 0 },
      fetch: async (url) => {
        urls.push(String(url));
        return new Response(urls.length === 1
          ? `data: ${JSON.stringify(change("1"))}\n\ndata: {"sequence":"2"`
          : frame(change("2")));
      },
    });
    const events = client.blocks.subscribe();
    expect((await events.next()).value?.sequence).toBe("1");
    expect((await events.next()).value?.sequence).toBe("2");
    expect(urls[1]).toContain("after=cursor-1");
    await events.return(undefined);
  });

  test("does not dispatch unterminated data even when its JSON is complete", async () => {
    for (const ending of ["", "\n", "\r", "\r\n"]) {
      const messages = [];
      for await (const message of parseSse(new Response(`data: {}${ending}`).body!)) {
        messages.push(message);
      }
      expect(messages).toEqual([]);
    }
  });

  test("reconnects after a socket reset during the response body", async () => {
    let calls = 0;
    let first!: ReadableStreamDefaultController<Uint8Array>;
    const urls: string[] = [];
    const client = createLeaniClient({
      baseUrl: "http://node.test",
      reconnect: { initialDelayMs: 0, maxDelayMs: 0, jitter: 0 },
      fetch: async (url) => {
        urls.push(String(url));
        calls++;
        return new Response(new ReadableStream({
          start(controller) {
            if (calls === 1) first = controller;
            controller.enqueue(frame(change(String(calls))));
          },
        }));
      },
    });
    const events = client.processors.subscribe("pool-a");
    expect((await events.next()).value?.sequence).toBe("1");
    first.error(new TypeError("connection reset"));
    expect((await events.next()).value?.sequence).toBe("2");
    expect(urls[1]).toContain("after=cursor-1");
    await events.return(undefined);
  });

  test("cancels the response when a consumer exits early", async () => {
    let cancelled = false;
    const client = createLeaniClient({
      baseUrl: "http://node.test",
      fetch: async () => new Response(new ReadableStream({
        start(controller) { controller.enqueue(frame(change("1"))); },
        cancel() { cancelled = true; },
      })),
    });
    const events = client.blobs.subscribe();
    await events.next();
    await events.return(undefined);
    expect(cancelled).toBe(true);
  });

  test("abort interrupts a read even with a custom fetch implementation", async () => {
    const stop = new AbortController();
    const client = createLeaniClient({
      baseUrl: "http://node.test",
      fetch: async () => new Response(new ReadableStream()),
    });
    const events = client.blocks.subscribe({ signal: stop.signal });
    const next = events.next();
    stop.abort();
    expect((await next).done).toBe(true);
  });

  test("malformed protocol data does not reconnect indefinitely", async () => {
    let calls = 0;
    const client = createLeaniClient({
      baseUrl: "http://node.test",
      fetch: async () => { calls++; return new Response("data: {}\n\n"); },
    });
    await expect(client.blocks.subscribe().next()).rejects.toBeInstanceOf(LeaniError);
    expect(calls).toBe(1);
  });

  test("retries transient HTTP responses and respects terminal errors", async () => {
    for (const status of [408, 429, 503]) {
      let calls = 0;
      const client = createLeaniClient({
        baseUrl: "http://node.test",
        reconnect: { initialDelayMs: 0, maxDelayMs: 0, jitter: 0 },
        fetch: async () => ++calls === 1
          ? new Response("upstream busy", { status })
          : new Response(frame(change("1"))),
      });
      const events = client.blocks.subscribe();
      expect((await events.next()).value?.sequence).toBe("1");
      expect(calls).toBe(2);
      await events.return(undefined);
    }
    const terminal = createLeaniClient({
      baseUrl: "http://node.test",
      fetch: async () => jsonResponse({
        error: { code: "failed", message: "operator action required", retryable: false },
      }, 503),
    });
    await expect(terminal.blocks.subscribe().next()).rejects.toMatchObject({ code: "failed" });
  });

  test("typed subscriptions select an instance and preserve the blobs bundle", async () => {
    const urls: string[] = [];
    const client = createLeaniClient({
      baseUrl: "http://node.test",
      fetch: async (url) => {
        urls.push(String(url));
        return new Response(frame({ ...change("1"), data: { block: { blockNumber: 42 }, transactions: [] } }));
      },
    });
    const events = client.blobs.subscribe({ processor: "blobs-secondary" });
    const event = (await events.next()).value!;
    expect(event.data?.block.blockNumber).toBe(42);
    expect(urls[0]).toContain("/processors/blobs-secondary/stream");
    await events.return(undefined);
  });

  test("unstructured JSON errors keep their HTTP status", async () => {
    const client = createLeaniClient({
      baseUrl: "http://node.test",
      fetch: async () => jsonResponse({}, 502),
    });
    await expect(client.status()).rejects.toMatchObject({ name: "LeaniError", status: 502, retryable: true });
  });
});

test("typed blobs subscription consumes the production serializer fixture", async () => {
  const fixture = (await import("./fixtures/blobs-change.json")).default;
  const client = createLeaniClient({
    baseUrl: "http://node.test",
    fetch: async () => new Response(`event: apply\ndata: ${JSON.stringify(fixture)}\n\n`),
  });
  for await (const event of client.blobs.subscribe()) {
    if (event.operation !== "apply" || !event.data) throw new Error("expected block apply");
    expect(event.data.block.blockNumber).toBe(19_430_000);
    expect(event.data.transactions.length).toBeGreaterThan(0);
    expect(event.data.transactions[0]?.blockHash).toBe(event.data.block.blockHash);
    break;
  }
});

test("finality vocabulary matches the OpenAPI enum", () => {
  const values: Finality[] = ["preview", "included", "finalized"];
  const wire: OpenApiComponents["schemas"]["Finality"][] = values;
  expect(wire).toEqual(["preview", "included", "finalized"]);
});
