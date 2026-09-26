import { afterEach, describe, expect, test } from "bun:test";
import {
  createServer,
  type IncomingHttpHeaders,
  type IncomingMessage,
  type ServerResponse,
} from "node:http";
import type { AddressInfo, Socket } from "node:net";

import { LeaniError, createLeaniClient } from "../src/index.ts";
import { createBackfillSubscriptionClient } from "../src/backfill.ts";

// Every test runs against real local HTTP servers, which close after it.

interface LocalServer {
  url: string;
  requests: Array<{ method: string; url: string; headers: IncomingHttpHeaders }>;
  /** Refuse new connections; open ones continue. */
  pause(): void;
  /** Accept connections on the same port again. */
  resume(): Promise<void>;
  close(): Promise<void>;
}

const servers: LocalServer[] = [];

afterEach(async () => {
  await Promise.all(servers.splice(0).map((server) => server.close()));
});

async function listen(
  handler: (request: IncomingMessage, response: ServerResponse) => void,
): Promise<LocalServer> {
  const sockets = new Set<Socket>();
  const requests: LocalServer["requests"] = [];
  const server = createServer((request, response) => {
    requests.push({
      method: request.method ?? "",
      url: request.url ?? "",
      headers: request.headers,
    });
    handler(request, response);
  });
  server.on("connection", (socket) => {
    sockets.add(socket);
    socket.on("close", () => sockets.delete(socket));
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address() as AddressInfo;
  const local: LocalServer = {
    url: `http://127.0.0.1:${port}`,
    requests,
    pause: () => {
      server.close();
    },
    resume: () =>
      new Promise((resolve) => server.listen(port, "127.0.0.1", () => resolve())),
    close: async () => {
      for (const socket of sockets) socket.destroy();
      if (server.listening) server.close();
    },
  };
  servers.push(local);
  return local;
}

/** The URL of a port nothing listens on. */
async function refusedUrl(): Promise<string> {
  const server = await listen(() => undefined);
  await server.close();
  return server.url;
}

const processor = {
  id: "prices",
  instance: "prices",
  version: "1.0.0",
  codeHash: "0x01",
  configHash: "0x02",
  genericApi: "processor-v1",
  changeSchema: "prices.change.v1",
  queryExtensions: [],
  subscriptions: true,
  artifactRetention: "none",
  deliveryOrdering: "block_versioned_idempotent",
};

function liveHello(overrides: Record<string, string> = {}): string {
  return `${JSON.stringify({
    type: "hello",
    apiVersion: "1",
    chainId: 1,
    processor,
    streamId: "prices:live",
    streamKind: "live",
    acknowledgedCursor: "live-0",
    storeEpoch: "00",
    heartbeatIntervalMs: "15000",
    sessionToken: "live-session",
    sessionExpiresAtUnixMs: "1000",
    leaseTtlMs: "300000",
    ...overrides,
  })}\n`;
}

function liveBatch(block: number): string {
  return `${JSON.stringify({
    type: "batch",
    streamId: "prices:live",
    originKind: "live",
    originId: "prices",
    publicationRevision: "0",
    fromBlock: block,
    throughBlock: block,
    processedBlockCount: "1",
    domainChangeCount: "1",
    progressUnitCount: "1",
    rawPayloadBytes: "8",
    uncompressedEncodedBytes: "32",
    transmittedBytes: "32",
    buildDelayMs: "0",
    firstCursor: `live-${block}`,
    lastCursor: `live-${block}`,
    acknowledgeableCursor: `live-block-${block}`,
    changes: [{ kind: "price.live" }],
  })}\n`;
}

/**
 * Answer a live lane's lease and acknowledgement routes, and hand its stream
 * requests, with their 1-based connection number, to `stream`.
 */
function liveLane(
  stream: (connection: number, response: ServerResponse) => void,
  lease: (response: ServerResponse) => void = (response) => json(response, 200, {}),
): (request: IncomingMessage, response: ServerResponse) => void {
  let connections = 0;
  return (request, response) => {
    if (request.url?.endsWith("/lease")) {
      if (request.method === "DELETE") {
        response.writeHead(204).end();
      } else {
        lease(response);
      }
      return;
    }
    if (request.url?.endsWith("/ack")) {
      json(response, 200, { acknowledgedSequence: "1" });
      return;
    }
    connections += 1;
    // A reconnect must open a new connection rather than reuse this one.
    response.writeHead(200, {
      "content-type": "application/x-ndjson",
      connection: "close",
    });
    stream(connections, response);
  };
}

function json(response: ServerResponse, status: number, body: unknown): void {
  response.writeHead(status, { "content-type": "application/json" });
  response.end(JSON.stringify(body));
}

function change(sequence: number): string {
  return `id: cursor-${sequence}\nevent: apply\ndata: ${JSON.stringify({
    apiVersion: "1",
    sequence: String(sequence),
    cursor: `cursor-${sequence}`,
    operation: "apply",
    originKind: "live",
    originId: "blocks",
    publicationRevision: "0",
    chainId: 1,
    block: null,
    finality: "finalized",
    kind: "block.put",
    schema: "block.change.v1",
    key: "0x01",
    data: { blockNumber: sequence },
    emittedAt: "1970-01-01T00:00:00.001Z",
  })}\n\n`;
}

function errorEvent(retryable: boolean): string {
  return `event: error\ndata: ${JSON.stringify({
    error: { code: "internal", message: "store busy", retryable, requestId: "req-1" },
  })}\n\n`;
}

const instantReconnect = { initialDelayMs: 0, maxDelayMs: 0, jitter: 0 };

/** A retryable HTTP error body. */
const busy = {
  error: { code: "busy", message: "try later", retryable: true, requestId: "req-3" },
};

describe("transport failures", () => {
  test("a refused connection is a retryable TransportError", async () => {
    // Audit M-D1: Bun's ConnectionRefused matched no message pattern.
    const url = await refusedUrl();
    for (const request of [
      createLeaniClient({ baseUrl: url }).status(),
      createBackfillSubscriptionClient({ baseUrl: url }).list(),
    ]) {
      const error = await request.catch((caught: unknown) => caught);
      expect(error).toBeInstanceOf(LeaniError);
      expect(error).toMatchObject({
        name: "TransportError",
        status: 0,
        code: "transport",
        retryable: true,
      });
      expect((error as Error).cause).toBeDefined();
    }
  });

  test("a socket reset while a body arrives is a retryable TransportError", async () => {
    const server = await listen((_request, response) => {
      response.writeHead(200, {
        "content-type": "application/json",
        "content-length": "100",
      });
      response.write('{"apiVersion":');
      setTimeout(() => response.socket?.destroy(), 20);
    });
    const error = await createLeaniClient({ baseUrl: server.url })
      .status()
      .catch((caught: unknown) => caught);
    expect(error).toMatchObject({ name: "TransportError", retryable: true });
  });

  test("the caller's own abort is passed through", async () => {
    const server = await listen(() => undefined);
    const stop = new AbortController();
    const request = createLeaniClient({ baseUrl: server.url }).status({
      signal: stop.signal,
    });
    setTimeout(() => stop.abort(new Error("cancelled by the caller")), 20);
    await expect(request).rejects.toThrow("cancelled by the caller");
  });

  test("a live lane reconnects after its socket resets", async () => {
    const server = await listen(
      liveLane((connection, response) => {
        response.write(liveHello());
        if (connection === 1) {
          setTimeout(() => response.socket?.destroy(), 20);
        } else {
          response.write(liveBatch(7));
        }
      }),
    );
    const delivery = await createBackfillSubscriptionClient({
      baseUrl: server.url,
    }).subscribe({ processor: "prices", consumer: "destination", lanes: { live: true } });
    try {
      for await (const batch of delivery.batches()) {
        expect(batch.ackCursor).toBe("live-block-7");
        break;
      }
    } finally {
      await delivery.close();
    }
    expect(server.requests.filter((request) => request.url.endsWith("/stream"))).toHaveLength(2);
  });

  test("a live lane reconnects while the node refuses connections", async () => {
    let server!: LocalServer;
    server = await listen(
      liveLane((connection, response) => {
        response.write(liveHello());
        if (connection === 1) {
          // Stop listening, then end the stream: the reconnect is refused
          // until the node listens again.
          server.pause();
          setTimeout(() => response.end(), 20);
          setTimeout(() => void server.resume(), 150);
        } else {
          response.write(liveBatch(8));
        }
      }),
    );
    const delivery = await createBackfillSubscriptionClient({
      baseUrl: server.url,
    }).subscribe({ processor: "prices", consumer: "destination", lanes: { live: true } });
    try {
      for await (const batch of delivery.batches()) {
        expect(batch.ackCursor).toBe("live-block-8");
        break;
      }
    } finally {
      await delivery.close();
    }
  });

  test("closing a reconnecting lane after the caller's own abort resolves", async () => {
    // Review: close() rethrew an abort reason that was not a DOMException.
    let streams = 0;
    const lane = liveLane((_connection, response) => response.end(liveHello()));
    const server = await listen((request, response) => {
      if (!request.url?.endsWith("/stream")) {
        lane(request, response);
        return;
      }
      streams += 1;
      if (streams === 1) {
        lane(request, response);
      } else {
        json(response, 503, busy);
      }
    });
    const caller = new AbortController();
    const delivery = await createBackfillSubscriptionClient({
      baseUrl: server.url,
    }).subscribe({
      processor: "prices",
      consumer: "destination",
      lanes: { live: true },
      signal: caller.signal,
    });
    const next = delivery.batches()[Symbol.asyncIterator]().next()
      .catch((error: unknown) => error);
    while (streams < 3) {
      await Bun.sleep(5);
    }
    caller.abort(new Error("stopped by the application"));
    await expect(delivery.close()).resolves.toBeUndefined();
    expect(await next).toMatchObject({ message: "stopped by the application" });
  });
});

describe("silent streams", () => {
  test("a delivery session fails with a TransportError after three heartbeat intervals of silence", async () => {
    // Audit M-D2: a stream that stopped sending, heartbeats included, hung
    // its iterator forever.
    const server = await listen(
      liveLane((_connection, response) => {
        response.write(liveHello({ heartbeatIntervalMs: "20" }));
      }),
    );
    const session = await createBackfillSubscriptionClient({
      baseUrl: server.url,
    }).streamLive("prices", "destination");
    const started = Date.now();
    let caught: unknown;
    try {
      for await (const _record of session) {
        // Nothing arrives.
      }
    } catch (error) {
      caught = error;
    }
    expect(caught).toMatchObject({ name: "TransportError", retryable: true });
    expect(Date.now() - started).toBeLessThan(2_000);
  });

  test("the idle watchdog pauses while the application handles a record", async () => {
    const server = await listen(
      liveLane((_connection, response) => {
        response.write(liveHello({ heartbeatIntervalMs: "20" }));
        response.write(liveBatch(1));
        setTimeout(() => response.end(liveBatch(2)), 150);
      }),
    );
    const session = await createBackfillSubscriptionClient({
      baseUrl: server.url,
    }).streamLive("prices", "destination");
    const blocks: number[] = [];
    for await (const record of session) {
      if (record.type !== "batch") continue;
      blocks.push(record.fromBlock);
      // Longer than three heartbeat intervals, while the stream is quiet.
      await Bun.sleep(200);
    }
    expect(blocks).toEqual([1, 2]);
  });

  test("a unified lane reconnects after a silent connection", async () => {
    const server = await listen(
      liveLane((connection, response) => {
        response.write(liveHello({ heartbeatIntervalMs: "20" }));
        if (connection > 1) response.write(liveBatch(9));
      }),
    );
    const delivery = await createBackfillSubscriptionClient({
      baseUrl: server.url,
    }).subscribe({ processor: "prices", consumer: "destination", lanes: { live: true } });
    try {
      for await (const batch of delivery.batches()) {
        expect(batch.ackCursor).toBe("live-block-9");
        break;
      }
    } finally {
      await delivery.close();
    }
  });

  test("a failed lease renewal ends the session at once", async () => {
    // Audit M-D2: the renewal loop stopped at its first failure, which the
    // iterator noticed only when another record arrived.
    const server = await listen(
      liveLane(
        (_connection, response) => {
          response.write(liveHello({ leaseTtlMs: "150", heartbeatIntervalMs: "60000" }));
        },
        (response) =>
          json(response, 409, {
            error: {
              code: "consumer_session_lost",
              message: "consumer streaming session is no longer current",
              retryable: false,
              requestId: "req-2",
            },
          }),
      ),
    );
    const session = await createBackfillSubscriptionClient({
      baseUrl: server.url,
    }).streamLive("prices", "destination");
    const started = Date.now();
    let caught: unknown;
    try {
      for await (const _record of session) {
        // The stream stays silent; only the renewal fails.
      }
    } catch (error) {
      caught = error;
    }
    expect(caught).toMatchObject({ name: "LeaniError", code: "consumer_session_lost" });
    // The first renewal, 50 ms in, ends the session.
    expect(Date.now() - started).toBeLessThan(1_000);
  });

  test("a transient renewal failure is retried while the lease holds", async () => {
    // Review: one failed renewal ended a healthy session.
    let renewals = 0;
    const server = await listen(
      liveLane(
        (_connection, response) => {
          response.write(liveHello({ leaseTtlMs: "300", heartbeatIntervalMs: "60000" }));
          setTimeout(() => response.write(liveBatch(3)), 600);
        },
        (response) => {
          renewals += 1;
          json(response, renewals <= 2 ? 503 : 200, renewals <= 2 ? busy : {});
        },
      ),
    );
    const session = await createBackfillSubscriptionClient({
      baseUrl: server.url,
    }).streamLive("prices", "destination");
    try {
      for await (const record of session) {
        if (record.type === "batch") {
          expect(record.fromBlock).toBe(3);
          break;
        }
      }
    } finally {
      await session.close();
    }
    expect(renewals).toBeGreaterThanOrEqual(3);
  });

  test("a renewal that keeps failing ends the session before the lease lapses", async () => {
    const server = await listen(
      liveLane(
        (_connection, response) => {
          response.write(liveHello({ leaseTtlMs: "300", heartbeatIntervalMs: "60000" }));
        },
        (response) => json(response, 503, busy),
      ),
    );
    const session = await createBackfillSubscriptionClient({
      baseUrl: server.url,
    }).streamLive("prices", "destination");
    const started = Date.now();
    let caught: unknown;
    try {
      for await (const _record of session) {
        // Only the renewals fail.
      }
    } catch (error) {
      caught = error;
    }
    expect(caught).toMatchObject({ name: "LeaniError", code: "busy", retryable: true });
    expect(Date.now() - started).toBeLessThan(1_000);
  });

  test("a stream without a hello fails after the request deadline", async () => {
    const silent = await listen(() => undefined);
    const headersOnly = await listen((_request, response) => {
      response.writeHead(200, { "content-type": "application/x-ndjson" });
      response.flushHeaders();
    });
    for (const server of [silent, headersOnly]) {
      const started = Date.now();
      const error = await createBackfillSubscriptionClient({
        baseUrl: server.url,
        timeoutMs: 100,
      })
        .streamLive("prices", "destination")
        .catch((caught: unknown) => caught);
      expect(error).toMatchObject({ name: "TransportError", retryable: true });
      expect(Date.now() - started).toBeLessThan(1_000);
    }
  });

  test("an acknowledgement without an answer fails after the request deadline", async () => {
    const lane = liveLane((_connection, response) => {
      response.write(liveHello());
      response.write(liveBatch(1));
    });
    const server = await listen((request, response) => {
      // The acknowledgement never answers.
      if (!request.url?.endsWith("/ack")) lane(request, response);
    });
    const session = await createBackfillSubscriptionClient({
      baseUrl: server.url,
      timeoutMs: 100,
    }).streamLive("prices", "destination");
    try {
      const started = Date.now();
      await expect(session.acknowledge("live-block-1")).rejects.toMatchObject({
        name: "TransportError",
        retryable: true,
      });
      expect(Date.now() - started).toBeLessThan(1_000);
    } finally {
      await session.close();
    }
  });

  test("SSE subscribe reconnects after its idle timeout", async () => {
    let connections = 0;
    const server = await listen((_request, response) => {
      connections += 1;
      response.writeHead(200, { "content-type": "text/event-stream" });
      response.write('event: hello\ndata: {"apiVersion":"1"}\n\n');
      if (connections > 1) response.write(change(1));
    });
    const client = createLeaniClient({ baseUrl: server.url, reconnect: instantReconnect });
    const events = client.blocks.subscribe({ idleTimeoutMs: 100 });
    try {
      expect((await events.next()).value?.sequence).toBe("1");
    } finally {
      await events.return(undefined);
    }
    expect(connections).toBe(2);
  });

  test("SSE subscribe resets its backoff once a stream opens", async () => {
    // Review: the delay grown by earlier drops also delayed every reconnect
    // of a quiet but healthy stream.
    const opened: number[] = [];
    const server = await listen((_request, response) => {
      opened.push(performance.now());
      if (opened.length <= 3) {
        json(response, 503, busy);
        return;
      }
      response.writeHead(200, { "content-type": "text/event-stream", connection: "close" });
      if (opened.length === 4) {
        response.write('event: hello\ndata: {"apiVersion":"1"}\n\n');
        setTimeout(() => response.end(), 30);
      } else {
        response.end(change(1));
      }
    });
    const client = createLeaniClient({
      baseUrl: server.url,
      reconnect: { initialDelayMs: 40, maxDelayMs: 5_000, jitter: 0 },
    });
    const events = client.blocks.subscribe();
    try {
      expect((await events.next()).value?.sequence).toBe("1");
    } finally {
      await events.return(undefined);
    }
    // Three drops grew the delay to 320 ms; the quiet stream reset it to 40.
    expect(opened[4]! - opened[3]! - 30).toBeLessThan(200);
  });
});

describe("SSE error events", () => {
  test("a retryable error event reconnects from the last cursor", async () => {
    // Audit M-D3: every error event ended the subscription.
    const server = await listen((request, response) => {
      response.writeHead(200, { "content-type": "text/event-stream" });
      const first = !request.url?.includes("after=");
      response.end(first ? change(1) + errorEvent(true) : change(2));
    });
    const client = createLeaniClient({ baseUrl: server.url, reconnect: instantReconnect });
    const events = client.blocks.subscribe();
    try {
      expect((await events.next()).value?.sequence).toBe("1");
      expect((await events.next()).value?.sequence).toBe("2");
    } finally {
      await events.return(undefined);
    }
    expect(server.requests[1]?.url).toContain("after=cursor-1");
  });

  test("a terminal error event throws without reconnecting", async () => {
    const server = await listen((_request, response) => {
      response.writeHead(200, { "content-type": "text/event-stream" });
      response.end(errorEvent(false));
    });
    const client = createLeaniClient({ baseUrl: server.url, reconnect: instantReconnect });
    await expect(client.blocks.subscribe().next()).rejects.toMatchObject({
      name: "LeaniError",
      code: "internal",
      retryable: false,
    });
    expect(server.requests).toHaveLength(1);
  });

  test("retryable error events without progress stop after five reconnects", async () => {
    const server = await listen((_request, response) => {
      response.writeHead(200, { "content-type": "text/event-stream" });
      response.end(errorEvent(true));
    });
    const client = createLeaniClient({ baseUrl: server.url, reconnect: instantReconnect });
    await expect(client.blocks.subscribe().next()).rejects.toMatchObject({
      code: "internal",
      retryable: true,
    });
    expect(server.requests).toHaveLength(6);
  });
});

describe("paths under a prefixed base URL", () => {
  test("every request stays under the base path", async () => {
    // Audit M-D4: absolute paths and dot segments left the prefix.
    const server = await listen((request, response) => {
      if (request.url?.endsWith("/stream")) {
        response.writeHead(200, { "content-type": "text/event-stream" });
        response.end(change(1));
        return;
      }
      json(response, 200, { data: [], nextCursor: null, coverage: {} });
    });
    const client = createLeaniClient({
      baseUrl: `${server.url}/prefix`,
      queryBasePaths: { blobs: "/v1/processors/blobs-production/query" },
    });
    await client.request("/v1/custom");
    await client.request("v1/relative");
    await client.blobs.getBlock(10);
    const events = client.processors.subscribe("pool-a");
    await events.next();
    await events.return(undefined);
    await createBackfillSubscriptionClient({ baseUrl: `${server.url}/prefix` }).list();
    expect(server.requests.map((request) => request.url)).toEqual([
      "/prefix/v1/custom",
      "/prefix/v1/relative",
      "/prefix/v1/processors/blobs-production/query/blocks/10",
      "/prefix/v1/processors/pool-a/stream",
      "/prefix/admin/v1/backfill-subscriptions",
    ]);
  });

  test("dot segments and backslashes are refused before any request", async () => {
    const server = await listen((_request, response) => json(response, 200, {}));
    const client = createLeaniClient({ baseUrl: `${server.url}/prefix` });
    const backfill = createBackfillSubscriptionClient({ baseUrl: `${server.url}/prefix` });
    for (const attempt of [
      () => client.request("../admin/v1/processors"),
      () => client.request("v1/%2e%2e/%2E%2e/admin"),
      () => client.request("v1/.%2e/admin"),
      () => client.request("v1\\..\\admin"),
      () => client.processors.status(".."),
      () => client.processors.status("."),
      () => client.processors.consumers.inspect("prices", ".."),
      () => backfill.inspect(".."),
      () => backfill.stream("..", "destination"),
      () => backfill.streamLive("prices", "."),
    ]) {
      await expect(attempt()).rejects.toBeInstanceOf(TypeError);
    }
    expect(server.requests).toHaveLength(0);
  });

  test("encoded separators, encoded percent signs, and control characters are refused", async () => {
    // Review: a proxy that decodes `%2F` or `%25` before it resolves dot
    // segments routed these outside the prefix, and URL parsing drops a tab.
    const server = await listen((_request, response) => json(response, 200, {}));
    const client = createLeaniClient({ baseUrl: `${server.url}/prefix` });
    const backfill = createBackfillSubscriptionClient({ baseUrl: `${server.url}/prefix` });
    for (const attempt of [
      () => client.request("v1/..%2Fadmin"),
      () => client.request("v1/%2e%2e%2fadmin"),
      () => client.request("v1/%252e%252e/admin"),
      () => client.request("v1/.\t./admin"),
      () => client.request("v1/..%5Cadmin"),
      () => client.request("v1/%c0%ae%c0%ae/admin"),
      () => client.processors.status("../../../other"),
      () => client.processors.queryEntities("prices", "../../admin"),
      () => backfill.inspect("../../../other"),
      () => backfill.stream("../../../other", "destination"),
    ]) {
      await expect(attempt()).rejects.toBeInstanceOf(TypeError);
    }
    expect(server.requests).toHaveLength(0);
  });
});

describe("redirects", () => {
  async function redirectingNode(): Promise<{ node: LocalServer; target: LocalServer }> {
    const target = await listen((_request, response) => json(response, 200, {}));
    const node = await listen((_request, response) => {
      response.writeHead(302, { location: `${target.url}/stolen` });
      response.end();
    });
    return { node, target };
  }

  test("a cross-origin redirect is refused, not followed", async () => {
    // Audit SDK-2: following it sent the node's credentials to another origin.
    const { node, target } = await redirectingNode();
    const error = await createLeaniClient({ baseUrl: node.url, token: "node-secret" })
      .status()
      .catch((caught: unknown) => caught);
    expect(error).toBeInstanceOf(LeaniError);
    expect(error).toMatchObject({
      status: 302,
      code: "redirect_refused",
      retryable: false,
      details: { location: `${target.url}/stolen` },
    });
    expect((error as Error).message).toContain(`${target.url}/stolen`);
    const backfill = createBackfillSubscriptionClient({ baseUrl: node.url, token: "node-secret" });
    await expect(backfill.list()).rejects.toMatchObject({ code: "redirect_refused" });
    await expect(
      backfill.streamLive("prices", "destination", { credential: "consumer-secret" }),
    ).rejects.toMatchObject({ code: "redirect_refused" });
    expect(target.requests).toHaveLength(0);
  });

  test("SSE subscribe fails on a redirecting base URL instead of retrying", async () => {
    const { node, target } = await redirectingNode();
    const client = createLeaniClient({ baseUrl: node.url, reconnect: instantReconnect });
    await expect(client.blocks.subscribe().next()).rejects.toMatchObject({
      code: "redirect_refused",
      retryable: false,
    });
    expect(node.requests).toHaveLength(1);
    expect(target.requests).toHaveLength(0);
  });

  test("a response that a custom fetch reached by following a redirect is refused", async () => {
    const { node } = await redirectingNode();
    const client = createLeaniClient({
      baseUrl: node.url,
      // This fetch ignores the SDK's `redirect: "manual"`.
      fetch: (input, init) => fetch(input, { ...init, redirect: "follow" }),
    });
    await expect(client.status()).rejects.toMatchObject({
      code: "redirect_refused",
      retryable: false,
    });
  });
});

describe("abort listeners", () => {
  /** Count `abort` listeners that can still fire, as registered from JavaScript. */
  function trackAbortListeners(): { active(signal?: AbortSignal): number; restore(): void } {
    const add = EventTarget.prototype.addEventListener;
    const remove = EventTarget.prototype.removeEventListener;
    const listeners = new Map<AbortSignal, Set<unknown>>();
    EventTarget.prototype.addEventListener = function (
      this: EventTarget,
      ...args: Parameters<EventTarget["addEventListener"]>
    ) {
      const [type, listener] = args;
      if (this instanceof AbortSignal && type === "abort" && listener) {
        const registered = listeners.get(this) ?? new Set();
        registered.add(listener);
        listeners.set(this, registered);
      }
      return add.apply(this, args);
    };
    EventTarget.prototype.removeEventListener = function (
      this: EventTarget,
      ...args: Parameters<EventTarget["removeEventListener"]>
    ) {
      const [type, listener] = args;
      if (this instanceof AbortSignal && type === "abort") {
        listeners.get(this)?.delete(listener);
      }
      return remove.apply(this, args);
    };
    return {
      // An aborted signal has dropped its once-listeners and fires no more.
      active: (signal) =>
        [...listeners]
          .filter(([target]) => !target.aborted && (!signal || target === signal))
          .reduce((count, [, registered]) => count + registered.size, 0),
      restore: () => {
        EventTarget.prototype.addEventListener = add;
        EventTarget.prototype.removeEventListener = remove;
      },
    };
  }

  test("sessions, renewals, and subscriptions leave no listeners behind", async () => {
    // Audit SDK-1: each renewal delay, and each session that failed to
    // open, left an abort listener on a long-lived signal.
    const tracker = trackAbortListeners();
    try {
      const caller = new AbortController();
      const refused = createBackfillSubscriptionClient({ baseUrl: await refusedUrl() });
      for (let attempt = 0; attempt < 10; attempt += 1) {
        await refused
          .streamLive("prices", "destination", { signal: caller.signal })
          .catch(() => undefined);
      }
      expect(tracker.active(caller.signal)).toBe(0);

      const server = await listen(
        liveLane((_connection, response) => {
          response.write(liveHello({ leaseTtlMs: "150", heartbeatIntervalMs: "60000" }));
        }),
      );
      const session = await createBackfillSubscriptionClient({
        baseUrl: server.url,
      }).streamLive("prices", "destination");
      // About eight renewals, one every 50 ms.
      await Bun.sleep(420);
      const renewals = server.requests.filter(
        (request) => request.method === "POST" && request.url.endsWith("/lease"),
      ).length;
      expect(renewals).toBeGreaterThanOrEqual(5);
      // The stream read and the next renewal delay, not one per renewal.
      expect(tracker.active()).toBeLessThanOrEqual(2);
      await session.close();

      const sse = await listen((_request, response) => {
        response.writeHead(200, { "content-type": "text/event-stream" });
        response.write(change(1));
      });
      const client = createLeaniClient({ baseUrl: sse.url });
      for (let attempt = 0; attempt < 10; attempt += 1) {
        const events = client.blocks.subscribe({ signal: caller.signal });
        await events.next();
        await events.return(undefined);
      }
      expect(tracker.active(caller.signal)).toBe(0);
    } finally {
      tracker.restore();
    }
  });
});
