import { expect, test } from "bun:test";

import type { ProcessorCoverage } from "@smart-byte/leani-sdk";
import type { BackfillRange } from "@smart-byte/leani-sdk/backfill";

import { coverageHoles, keepInSync } from "./live-sync.ts";

function coverage(
  available: Array<[number, number]>,
  processedThrough: number | null,
): ProcessorCoverage {
  return {
    chainId: 1,
    chainFinalizedHead: { number: 120, hash: "0x00" },
    available: available.map(([fromBlock, toBlock]) => ({
      fromBlock,
      toBlock,
      finality: "finalized",
    })),
    configuredStartBlock: 100,
    processedThrough,
    finalizedThrough: processedThrough,
    complete: false,
    state: "live",
  };
}

test("a processor without blocks lacks everything from its start to the finalized head", () => {
  expect(coverageHoles(coverage([], null), 90)).toEqual([{ fromBlock: 100, toBlock: 120 }]);
});

test("the holes are the uncovered blocks through the last processed one", () => {
  expect(coverageHoles(coverage([[100, 104], [110, 115]], 115), 102)).toEqual([
    { fromBlock: 105, toBlock: 109 },
  ]);
});

test("a request that keeps failing holds no live batch back", async () => {
  const hello = {
    type: "hello", apiVersion: "1", chainId: 1,
    processor: { id: "blobs-money", instance: "blobs-production", version: "1.5.0", codeHash: "0x01", configHash: "0x02", genericApi: "processor-v1", changeSchema: "blobs-money.change.v1", queryExtensions: [], subscriptions: true, artifactRetention: "none", deliveryOrdering: "block_versioned_idempotent" },
    streamId: "blobs-production:live", streamKind: "live", acknowledgedCursor: "cursor-0", storeEpoch: "00",
    heartbeatIntervalMs: "15000", sessionToken: "live-session", sessionExpiresAtUnixMs: "1000", leaseTtlMs: "300000",
    coverage: coverage([[100, 200]], 200),
  };
  const batch = (cursor: string, block: number, change: object) => ({
    type: "batch", streamId: "blobs-production:live", originKind: "live", originId: "blobs-production",
    publicationRevision: "0", fromBlock: block, throughBlock: block, processedBlockCount: "1",
    domainChangeCount: "1", progressUnitCount: "1", rawPayloadBytes: "8", uncompressedEncodedBytes: "64",
    transmittedBytes: "64", buildDelayMs: "0", firstCursor: cursor, lastCursor: cursor,
    acknowledgeableCursor: cursor, changes: [{ cursor, ...change }],
  });
  const records = [
    hello,
    batch("cursor-1", 251, { operation: "finalized", kind: "system.live_gap.put", key: "0x00", data: { fromBlock: 201, toBlock: 250, reason: "not_filled" } }),
    batch("cursor-2", 251, { operation: "apply", kind: "blobs.block.put", key: "0xfb", data: { blockNumber: 251 } }),
  ];
  const acknowledged: string[] = [];
  const controller = new AbortController();
  const original = globalThis.fetch;
  globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    const request = new Request(input, init);
    if (request.method === "DELETE") return new Response(null, { status: 204 });
    if (request.url.endsWith("/admin/v1/backfill-subscriptions")) {
      return Response.json(
        { error: { code: "conflict", message: "idempotency key already used", retryable: false } },
        { status: 409 },
      );
    }
    if (request.url.endsWith("/ack")) {
      acknowledged.push(((await request.json()) as { cursor: string }).cursor);
      if (acknowledged.length === 2) controller.abort();
      return Response.json({ acknowledgedSequence: "2" });
    }
    return new Response(records.map((record) => `${JSON.stringify(record)}\n`).join(""), {
      headers: { "content-type": "application/x-ndjson" },
    });
  }) as typeof fetch;
  const rows = new Map<string, unknown>();
  const gaps: BackfillRange[] = [];
  try {
    await keepInSync(
      {
        put: (key: string, value: unknown) => { rows.set(key, value); },
        delete: (key: string) => { rows.delete(key); },
        rememberGap: async (range: BackfillRange) => { gaps.push(range); },
        gaps: async () => [...gaps],
        forgetGap: async (range: BackfillRange) => {
          gaps.splice(gaps.findIndex((gap) => gap.fromBlock === range.fromBlock), 1);
        },
      },
      {
        baseUrl: "http://node.test",
        processor: "blobs-production",
        consumer: "blobs-api",
        credential: "consumer-secret",
        fromBlock: 100,
        onBackfill: () => {},
        signal: controller.signal,
      },
    );
  } finally {
    globalThis.fetch = original;
  }
  expect(acknowledged).toEqual(["cursor-1", "cursor-2"]);
  expect(rows.get("0xfb")).toEqual({ blockNumber: 251 });
  expect(gaps.map(({ fromBlock, toBlock }) => [fromBlock, toBlock])).toEqual([[201, 250]]);
});

test("a connection that fails in transit reconnects and checks coverage again", async () => {
  const hello = {
    type: "hello", apiVersion: "1", chainId: 1,
    processor: { id: "blobs-money", instance: "blobs-production", version: "1.5.0", codeHash: "0x01", configHash: "0x02", genericApi: "processor-v1", changeSchema: "blobs-money.change.v1", queryExtensions: [], subscriptions: true, artifactRetention: "none", deliveryOrdering: "block_versioned_idempotent" },
    streamId: "blobs-production:live", streamKind: "live", acknowledgedCursor: "cursor-0", storeEpoch: "00",
    heartbeatIntervalMs: "15000", sessionToken: "live-session", sessionExpiresAtUnixMs: "1000", leaseTtlMs: "300000",
    coverage: coverage([[100, 150], [160, 200]], 200),
  };
  const batch = {
    type: "batch", streamId: "blobs-production:live", originKind: "live", originId: "blobs-production",
    publicationRevision: "0", fromBlock: 201, throughBlock: 201, processedBlockCount: "1",
    domainChangeCount: "1", progressUnitCount: "1", rawPayloadBytes: "8", uncompressedEncodedBytes: "64",
    transmittedBytes: "64", buildDelayMs: "0", firstCursor: "cursor-1", lastCursor: "cursor-1",
    acknowledgeableCursor: "cursor-1",
    changes: [{ cursor: "cursor-1", operation: "apply", kind: "blobs.block.put", key: "0xc9", data: { blockNumber: 201 } }],
  };
  let connections = 0;
  const acknowledged: string[] = [];
  const controller = new AbortController();
  const original = globalThis.fetch;
  globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    const request = new Request(input, init);
    if (request.method === "DELETE") return new Response(null, { status: 204 });
    if (request.url.endsWith("/admin/v1/backfill-subscriptions")) {
      return Response.json(
        { error: { code: "conflict", message: "idempotency key already used", retryable: false } },
        { status: 409 },
      );
    }
    if (request.url.endsWith("/ack")) {
      acknowledged.push(((await request.json()) as { cursor: string }).cursor);
      controller.abort();
      return Response.json({ acknowledgedSequence: "1" });
    }
    connections += 1;
    if (connections === 1) throw new TypeError("fetch failed");
    return new Response([hello, batch].map((record) => `${JSON.stringify(record)}\n`).join(""), {
      headers: { "content-type": "application/x-ndjson" },
    });
  }) as typeof fetch;
  const gaps: BackfillRange[] = [];
  try {
    await keepInSync(
      {
        put: () => {},
        delete: () => {},
        rememberGap: async (range: BackfillRange) => { gaps.push(range); },
        gaps: async () => [...gaps],
        forgetGap: async () => {},
      },
      {
        baseUrl: "http://node.test",
        processor: "blobs-production",
        consumer: "blobs-api",
        credential: "consumer-secret",
        fromBlock: 100,
        onBackfill: () => {},
        signal: controller.signal,
      },
    );
  } finally {
    globalThis.fetch = original;
  }
  expect(connections).toBe(2);
  expect(acknowledged).toEqual(["cursor-1"]);
  expect(gaps.map(({ fromBlock, toBlock }) => [fromBlock, toBlock])).toEqual([[151, 159]]);
});
