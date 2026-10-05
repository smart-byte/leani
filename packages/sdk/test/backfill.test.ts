import { describe, expect, test } from "bun:test";

import {
  BackfillStreamError,
  TransportError,
  backfillBatchingProfile,
  createBackfillSubscriptionClient,
  liveGapBackfill,
  parseNdjson,
  runBackfill,
  type BackfillState,
  type BackfillStreamRecord,
  type CreateBackfillSubscription,
  type HistoryDeliveryBatch,
  type LiveStreamRecord,
} from "../src/backfill.ts";
import { isLiveGapChange } from "../src/index.ts";

function chunkedBody(chunks: string[]): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder();
  return new ReadableStream({
    start(controller) {
      for (const chunk of chunks) {
        controller.enqueue(encoder.encode(chunk));
      }
      controller.close();
    },
  });
}

describe("backfill subscriptions", () => {
  test("creation sends application-owned consumer and work-ahead limits", async () => {
    let requestBody: unknown;
    const client = createBackfillSubscriptionClient({
      baseUrl: "http://node.test",
      fetch: async (_input, init) => {
        requestBody = JSON.parse(String(init?.body));
        return Response.json({
          id: "repair-1",
          processor: "blobs-production",
          processorConfigured: true,
          consumer: "blobs-api",
          deliveryStreamId: "history-1",
          fromBlock: 1,
          toBlock: 6,
          ranges: [
            { fromBlock: 1, toBlock: 2 },
            { fromBlock: 5, toBlock: 6 },
          ],
          requestedBlocks: 4,
          capturedFinalizedTarget: 100,
          mode: "fill_missing",
          state: "queued",
          attempts: 0,
          updatedAtUnixMs: 1,
          report: null,
          lastError: null,
        });
      },
    });
    await client.create({
      processor: "blobs-production",
      ranges: [
        { fromBlock: 1, toBlock: 2 },
        { fromBlock: 5, toBlock: 6 },
      ],
      idempotencyKey: "repair-1",
      consumer: {
        id: "blobs-api",
        role: "required",
        leaseTtlSeconds: 300,
        credential: "consumer-secret",
      },
      limits: {
        maxUnacknowledgedBlocks: 16_384,
        maxUnacknowledgedBytes: 512 * 1024 * 1024,
        resumeBelowRatio: 0.75,
      },
      batching: {
        targetEncodedBytes: 1024 * 1024,
        maximumProcessedBlocks: 2048,
        maximumDelayMs: 25,
        compression: "gzip",
      },
    });
    expect(requestBody).toMatchObject({
      ranges: [
        { fromBlock: 1, toBlock: 2 },
        { fromBlock: 5, toBlock: 6 },
      ],
      consumer: {
        id: "blobs-api",
        role: "required",
        leaseTtlSeconds: 300,
        credential: "consumer-secret",
      },
      limits: {
        maxUnacknowledgedBlocks: 16_384,
        maxUnacknowledgedBytes: 512 * 1024 * 1024,
        resumeBelowRatio: 0.75,
      },
      batching: {
        targetEncodedBytes: 1024 * 1024,
        maximumProcessedBlocks: 2048,
        maximumDelayMs: 25,
        compression: "gzip",
      },
    });
  });

  test("terminal deletion reports reclaimed and retained objects", async () => {
    let request: Request | undefined;
    const client = createBackfillSubscriptionClient({
      baseUrl: "http://node.test",
      fetch: async (input, init) => {
        request = new Request(input, init);
        return Response.json({
          id: "repair-1",
          owner: "subscription",
          removedJobs: 2,
          removedSubscriptionRanges: 1,
          removedConsumers: 1,
          removedDeliveryRecords: 100,
          removedDeliveryStreams: 1,
          retainedProcessorOutput: true,
          retainedLiveStream: true,
        });
      },
    });
    const result = await client.delete("repair-1");
    expect(request?.method).toBe("DELETE");
    expect(request?.url).toBe(
      "http://node.test/admin/v1/backfill-subscriptions/repair-1",
    );
    expect(request?.headers.get("x-leani-request")).toBe("1");
    expect(result).toMatchObject({
      removedDeliveryRecords: 100,
      retainedProcessorOutput: true,
      retainedLiveStream: true,
    });
  });

  test("deletion can discard unacknowledged records", async () => {
    let request: Request | undefined;
    const client = createBackfillSubscriptionClient({
      baseUrl: "http://node.test",
      fetch: async (input, init) => {
        request = new Request(input, init);
        return Response.json({ id: "repair-1", owner: "subscription" });
      },
    });
    await client.delete("repair-1", { discardUnacknowledged: true });
    expect(request?.method).toBe("DELETE");
    expect(request?.url).toBe(
      "http://node.test/admin/v1/backfill-subscriptions/repair-1?discardUnacknowledged=true",
    );
    expect(request?.headers.get("x-leani-request")).toBe("1");
  });

  test("retry re-queues a failed subscription", async () => {
    let request: Request | undefined;
    const client = createBackfillSubscriptionClient({
      baseUrl: "http://node.test",
      fetch: async (input, init) => {
        request = new Request(input, init);
        return Response.json({ id: "repair-1", state: "queued" });
      },
    });
    const status = await client.retry("repair-1");
    expect(request?.method).toBe("POST");
    expect(request?.url).toBe(
      "http://node.test/admin/v1/backfill-subscriptions/repair-1/retry",
    );
    expect(request?.headers.get("x-leani-request")).toBe("1");
    expect(status.state).toBe("queued");
  });

  test("body-less cancellation is marked as a Leani request", async () => {
    let request: Request | undefined;
    const client = createBackfillSubscriptionClient({
      baseUrl: "http://node.test",
      fetch: async (input, init) => {
        request = new Request(input, init);
        return Response.json({ id: "repair-1" });
      },
    });
    await client.cancel("repair-1");
    expect(request?.method).toBe("POST");
    expect(request?.url).toBe(
      "http://node.test/admin/v1/backfill-subscriptions/repair-1/cancel",
    );
    expect(request?.headers.get("x-leani-request")).toBe("1");
  });

  test("creation accepts an atomic finalized upper bound", async () => {
    let requestBody: { toBlock?: unknown } | undefined;
    const client = createBackfillSubscriptionClient({
      baseUrl: "http://node.test",
      fetch: async (_input, init) => {
        requestBody = JSON.parse(String(init?.body));
        return Response.json({
          id: "through-finalized",
          owner: "subscription",
          processor: "blobs-production",
          processorConfigured: true,
          consumer: "blobs-api",
          fromBlock: 1,
          toBlock: 10,
          ranges: [{ fromBlock: 1, toBlock: 10 }],
          requestedBlocks: 10,
          capturedFinalizedTarget: 10,
          mode: "fill_missing",
          state: "queued",
          attempts: 0,
          updatedAtUnixMs: 1,
          report: null,
          lastError: null,
        });
      },
    });
    await client.create({
      processor: "blobs-production",
      fromBlock: 1,
      toBlock: "finalized",
      idempotencyKey: "through-finalized",
    });
    expect(requestBody?.toBlock).toBe("finalized");
  });

  test("NDJSON parser preserves records split across transport chunks", async () => {
    const records: unknown[] = [];
    for await (const record of parseNdjson(
      chunkedBody([
        '{"type":"hello","api',
        'Version":"1"}\n\n{"type":"heartbeat",',
        '"emittedAt":"2026-08-07T00:00:00.000Z"}\n',
      ]),
    )) {
      records.push(record);
    }
    expect(records).toEqual([
      { type: "hello", apiVersion: "1" },
      {
        type: "heartbeat",
        emittedAt: "2026-08-07T00:00:00.000Z",
      },
    ]);
  });

  test("NDJSON parser reads a large record split into small chunks in linear time", async () => {
    // Review: every chunk rescanned the whole buffered record.
    const payload = "x".repeat(8 * 1024 * 1024);
    const encoded = new TextEncoder().encode(
      `${JSON.stringify({ type: "batch", payload })}\n`,
    );
    let offset = 0;
    const body = new ReadableStream<Uint8Array>({
      pull(controller) {
        if (offset >= encoded.length) {
          controller.close();
          return;
        }
        controller.enqueue(encoded.slice(offset, offset + 1024));
        offset += 1024;
      },
    });
    const started = performance.now();
    const records: unknown[] = [];
    for await (const record of parseNdjson(body)) {
      records.push(record);
    }
    expect(performance.now() - started).toBeLessThan(1_000);
    expect(records).toHaveLength(1);
    expect((records[0] as { payload: string }).payload.length).toBe(payload.length);
  });

  test("NDJSON parser refuses a record over 65 MiB", async () => {
    // Review: a record without its newline grew the buffer without bound.
    const chunk = new Uint8Array(1024 * 1024).fill(0x78);
    let sent = 0;
    const body = new ReadableStream<Uint8Array>({
      pull(controller) {
        if (sent++ < 70) {
          controller.enqueue(chunk);
        } else {
          controller.close();
        }
      },
    });
    await expect(parseNdjson(body).next()).rejects.toMatchObject({
      name: "LeaniError",
      code: "invalid_response",
      retryable: false,
    });
  });

  test("client opens a fenced session without exposing its token", async () => {
    const requests: Request[] = [];
    const client = createBackfillSubscriptionClient({
      baseUrl: "http://node.test",
      token: "node-token",
      fetch: async (input, init) => {
        const request = new Request(input, init);
        requests.push(request);
        if (request.method === "DELETE") {
          return new Response(null, { status: 204 });
        }
        return new Response(
          chunkedBody([
            '{"type":"hello","apiVersion":"1","chainId":1,"processor":{"id":"blobs-money","instance":"blobs-production","version":"1.0.0","codeHash":"0x01","configHash":"0x02","genericApi":"processor-v1","changeSchema":"blobs-money.change.v1","queryExtensions":[],"subscriptions":true,"artifactRetention":"none","deliveryOrdering":"block_versioned_idempotent"},"subscriptionId":"repair-1","streamId":"history-1","streamKind":"backfill","publicationRevision":"7","ranges":[{"fromBlock":1,"toBlock":2}],"acknowledgedCursor":"cursor-0","storeEpoch":"00","heartbeatIntervalMs":"15000","sessionToken":"history-session","sessionExpiresAtUnixMs":"1000","leaseTtlMs":"300000"}\n',
            '{"type":"batch","streamId":"history-1","originKind":"historical_backfill","originId":"repair-1","publicationRevision":"7","fromBlock":1,"throughBlock":2,"processedBlockCount":"2","domainChangeCount":"0","progressUnitCount":"1","rawPayloadBytes":"0","uncompressedEncodedBytes":"2","transmittedBytes":"512","buildDelayMs":"0","firstCursor":null,"lastCursor":null,"acknowledgeableCursor":"cursor-1","changes":[]}\n',
            '{"type":"backfill_complete","subscriptionId":"repair-1","streamId":"history-1","ranges":[{"fromBlock":1,"toBlock":2}],"throughBlock":2,"cursor":"cursor-2","mode":"fill_missing","disposition":"already_covered_noop","requestedBlockCount":"2","coveredBeforeRequestBlockCount":"2","newlyProcessedBlockCount":"0","republishedBlockCount":"0","domainChangeCount":"0","preexistingCoverageSkipped":true,"repairHint":"recompute"}\n',
          ]),
          {
            headers: { "content-type": "application/x-ndjson" },
          },
        );
      },
    });

    const session = await client.stream(
      "repair-1",
      "blobs-api",
      {
        credential: "consumer-secret",
        batching: backfillBatchingProfile("latency"),
      },
    );
    expect(session.hello).toMatchObject({
      type: "hello",
      streamKind: "backfill",
      streamId: "history-1",
    });
    expect("sessionToken" in session.hello).toBe(false);
    const records: BackfillStreamRecord[] = [];
    for await (const record of session) {
      records.push(record);
    }
    expect(records.map((record) => record.type)).toEqual([
      "batch",
      "backfill_complete",
    ]);
    expect(records[0]).toMatchObject({ publicationRevision: "7" });
    expect(requests[0]?.url).toBe(
      "http://node.test/v1/backfill-subscriptions/repair-1/consumers/blobs-api/stream?targetEncodedBytes=262144&maximumEncodedBytes=1048576&maximumEvents=2000&maximumProcessedBlocks=512&maximumDelayMs=10",
    );
    expect(requests[0]?.headers.get("authorization")).toBe("Bearer node-token");
    expect(
      requests[0]?.headers.get("x-leani-consumer-credential"),
    ).toBe("consumer-secret");
    expect(requests[0]?.headers.get("accept")).toBe("application/x-ndjson");
    expect(requests[1]?.method).toBe("DELETE");
    expect(requests[1]?.url).toBe(
      "http://node.test/v1/backfill-subscriptions/repair-1/consumers/blobs-api/lease",
    );
    expect(requests[1]?.headers.get("x-leani-consumer-session")).toBe(
      "history-session",
    );
    expect(requests[1]?.headers.get("x-leani-request")).toBe("1");
  });

  test("events convenience flattens history batches intentionally", async () => {
    const client = createBackfillSubscriptionClient({
      baseUrl: "http://node.test",
      fetch: async () =>
        new Response(
          chunkedBody([
            '{"type":"hello","apiVersion":"1","chainId":1,"processor":{"id":"blobs-money","instance":"blobs-production","version":"1.0.0","codeHash":"0x01","configHash":"0x02","genericApi":"processor-v1","changeSchema":"blobs-money.change.v1","queryExtensions":[],"subscriptions":true,"artifactRetention":"none","deliveryOrdering":"block_versioned_idempotent"},"subscriptionId":"repair-1","streamId":"history-1","streamKind":"backfill","publicationRevision":"0","ranges":[{"fromBlock":1,"toBlock":1}],"acknowledgedCursor":"cursor-0","storeEpoch":"00","heartbeatIntervalMs":"15000","sessionToken":"history-session","sessionExpiresAtUnixMs":"1000","leaseTtlMs":"300000"}\n',
            '{"type":"batch","streamId":"history-1","originKind":"historical_backfill","originId":"repair-1","publicationRevision":"0","fromBlock":1,"throughBlock":1,"processedBlockCount":"1","domainChangeCount":"1","progressUnitCount":"1","rawPayloadBytes":"8","uncompressedEncodedBytes":"32","transmittedBytes":"32","buildDelayMs":"0","firstCursor":"event-1","lastCursor":"event-1","acknowledgeableCursor":"boundary-1","changes":[{"kind":"pool.price.put"}]}\n',
            '{"type":"backfill_complete","subscriptionId":"repair-1","streamId":"history-1","ranges":[{"fromBlock":1,"toBlock":1}],"throughBlock":1,"cursor":"cursor-2","mode":"fill_missing","disposition":"published_all","requestedBlockCount":"1","coveredBeforeRequestBlockCount":"0","newlyProcessedBlockCount":"1","republishedBlockCount":"1","domainChangeCount":"1","preexistingCoverageSkipped":false}\n',
          ]),
          { headers: { "content-type": "application/x-ndjson" } },
        ),
    });
    const session = await client.stream("repair-1", "destination");
    const kinds: string[] = [];
    for await (const event of session.events()) {
      kinds.push(event.kind);
    }
    expect(kinds).toEqual(["pool.price.put"]);
  });

  test("delivery batches carry every acknowledgement boundary with their changes", async () => {
    // Audit SDK-3: events() drops the batch acknowledgement cursor, and a
    // batch without changes or the completion yields nothing at all.
    const acknowledged: string[] = [];
    const client = createBackfillSubscriptionClient({
      baseUrl: "http://node.test",
      fetch: async (input, init) => {
        const request = new Request(input, init);
        if (request.method === "DELETE") {
          return new Response(null, { status: 204 });
        }
        if (request.url.endsWith("/ack")) {
          acknowledged.push(((await request.json()) as { cursor: string }).cursor);
          return Response.json({ acknowledgedSequence: "1" });
        }
        return new Response(
          chunkedBody([
            '{"type":"hello","apiVersion":"1","chainId":1,"processor":{"id":"blobs-money","instance":"blobs-production","version":"1.0.0","codeHash":"0x01","configHash":"0x02","genericApi":"processor-v1","changeSchema":"blobs-money.change.v1","queryExtensions":[],"subscriptions":true,"artifactRetention":"none","deliveryOrdering":"block_versioned_idempotent"},"subscriptionId":"repair-1","streamId":"history-1","streamKind":"backfill","publicationRevision":"0","ranges":[{"fromBlock":1,"toBlock":3}],"acknowledgedCursor":"cursor-0","storeEpoch":"00","heartbeatIntervalMs":"15000","sessionToken":"history-session","sessionExpiresAtUnixMs":"1000","leaseTtlMs":"300000"}\n',
            '{"type":"batch","streamId":"history-1","originKind":"historical_backfill","originId":"repair-1","publicationRevision":"0","fromBlock":1,"throughBlock":1,"processedBlockCount":"1","domainChangeCount":"1","progressUnitCount":"1","rawPayloadBytes":"8","uncompressedEncodedBytes":"32","transmittedBytes":"32","buildDelayMs":"0","firstCursor":"event-1","lastCursor":"event-1","acknowledgeableCursor":"boundary-1","changes":[{"kind":"pool.price.put","cursor":"event-1"}]}\n',
            '{"type":"batch","streamId":"history-1","originKind":"historical_backfill","originId":"repair-1","publicationRevision":"0","fromBlock":2,"throughBlock":3,"processedBlockCount":"2","domainChangeCount":"0","progressUnitCount":"1","rawPayloadBytes":"0","uncompressedEncodedBytes":"2","transmittedBytes":"2","buildDelayMs":"0","firstCursor":null,"lastCursor":null,"acknowledgeableCursor":"boundary-3","changes":[]}\n',
            '{"type":"backfill_complete","subscriptionId":"repair-1","streamId":"history-1","ranges":[{"fromBlock":1,"toBlock":3}],"throughBlock":3,"cursor":"complete-4","mode":"fill_missing","disposition":"published_all","requestedBlockCount":"3","coveredBeforeRequestBlockCount":"0","coveredBeforeRequestRanges":[],"newlyProcessedBlockCount":"3","republishedBlockCount":"3","domainChangeCount":"1","preexistingCoverageSkipped":false}\n',
          ]),
          { headers: { "content-type": "application/x-ndjson" } },
        );
      },
    });
    const session = await client.stream<{ kind: string }>("repair-1", "destination");
    const observed: Array<[string, string[], boolean]> = [];
    for await (const batch of session.deliveryBatches()) {
      observed.push([
        batch.ackCursor,
        batch.events.map((event) => event.kind),
        batch.completion !== undefined,
      ]);
      await session.acknowledge(batch.ackCursor);
    }
    await session.close();
    expect(observed).toEqual([
      ["boundary-1", ["pool.price.put"], false],
      ["boundary-3", [], false],
      ["complete-4", [], true],
    ]);
    expect(acknowledged).toEqual(["boundary-1", "boundary-3", "complete-4"]);
  });

  test("unified live acknowledgements use the batch boundary, not its last change", async () => {
    // Audit M-A3: the node refuses a live acknowledgement inside a block.
    const acknowledged: string[] = [];
    const client = createBackfillSubscriptionClient({
      baseUrl: "http://node.test",
      fetch: async (input, init) => {
        const request = new Request(input, init);
        if (request.method === "DELETE") {
          return new Response(null, { status: 204 });
        }
        if (request.url.endsWith("/ack")) {
          acknowledged.push(((await request.json()) as { cursor: string }).cursor);
          return Response.json({ acknowledgedSequence: "2" });
        }
        return new Response(
          chunkedBody([
            '{"type":"hello","apiVersion":"1","chainId":1,"processor":{"id":"prices","instance":"prices","version":"1.0.0","codeHash":"0x01","configHash":"0x02","genericApi":"processor-v1","changeSchema":"prices.change.v1","queryExtensions":[],"subscriptions":true,"artifactRetention":"none","deliveryOrdering":"block_versioned_idempotent"},"streamId":"prices:live","streamKind":"live","acknowledgedCursor":"live-0","storeEpoch":"00","heartbeatIntervalMs":"15000","sessionToken":"live-session","sessionExpiresAtUnixMs":"1000","leaseTtlMs":"300000"}\n',
            '{"type":"batch","streamId":"prices:live","originKind":"live","originId":"prices","publicationRevision":"0","fromBlock":20,"throughBlock":20,"processedBlockCount":"1","domainChangeCount":"2","progressUnitCount":"1","rawPayloadBytes":"16","uncompressedEncodedBytes":"64","transmittedBytes":"64","buildDelayMs":"0","firstCursor":"live-event-1","lastCursor":"live-event-2","acknowledgeableCursor":"live-block-20","changes":[{"kind":"price.live","cursor":"live-event-1"},{"kind":"price.live","cursor":"live-event-2"}]}\n',
          ]),
          { headers: { "content-type": "application/x-ndjson" } },
        );
      },
    });
    const delivery = await client.subscribe<{ kind: string }>({
      processor: "prices",
      consumer: "destination",
      lanes: { live: true },
    });
    for await (const batch of delivery.batches()) {
      expect(batch.events.map((event) => event.cursor)).toEqual([
        "live-event-1",
        "live-event-2",
      ]);
      await delivery.acknowledge(batch);
      break;
    }
    await delivery.close();
    expect(acknowledged).toEqual(["live-block-20"]);
  });

  test("one subscription merges ready live before history and routes lane acknowledgements", async () => {
    const acknowledged: string[] = [];
    const client = createBackfillSubscriptionClient({
      baseUrl: "http://node.test",
      fetch: async (input, init) => {
        const request = new Request(input, init);
        if (request.method === "DELETE") {
          return new Response(null, { status: 204 });
        }
        if (request.url.endsWith("/ack")) {
          acknowledged.push(request.url);
          return Response.json({ acknowledgedSequence: "1" });
        }
        if (request.url.includes("/streams/live/")) {
          return new Response(
            chunkedBody([
              '{"type":"hello","apiVersion":"1","chainId":1,"processor":{"id":"prices","instance":"prices","version":"1.0.0","codeHash":"0x01","configHash":"0x02","genericApi":"processor-v1","changeSchema":"prices.change.v1","queryExtensions":[],"subscriptions":true,"artifactRetention":"none","deliveryOrdering":"block_versioned_idempotent"},"streamId":"prices:live","streamKind":"live","acknowledgedCursor":"live-0","storeEpoch":"00","heartbeatIntervalMs":"15000","sessionToken":"live-session","sessionExpiresAtUnixMs":"1000","leaseTtlMs":"300000"}\n',
              '{"type":"batch","streamId":"prices:live","originKind":"live","originId":"prices","publicationRevision":"0","fromBlock":20,"throughBlock":20,"processedBlockCount":"1","domainChangeCount":"1","progressUnitCount":"1","rawPayloadBytes":"8","uncompressedEncodedBytes":"32","transmittedBytes":"32","buildDelayMs":"0","firstCursor":"live-1","lastCursor":"live-1","acknowledgeableCursor":"live-1","changes":[{"kind":"price.live"}]}\n',
            ]),
            { headers: { "content-type": "application/x-ndjson" } },
          );
        }
        return new Response(
          chunkedBody([
            '{"type":"hello","apiVersion":"1","chainId":1,"processor":{"id":"prices","instance":"prices","version":"1.0.0","codeHash":"0x01","configHash":"0x02","genericApi":"processor-v1","changeSchema":"prices.change.v1","queryExtensions":[],"subscriptions":true,"artifactRetention":"none","deliveryOrdering":"block_versioned_idempotent"},"subscriptionId":"repair-1","streamId":"history-1","streamKind":"backfill","publicationRevision":"0","ranges":[{"fromBlock":1,"toBlock":1}],"acknowledgedCursor":"history-0","storeEpoch":"00","heartbeatIntervalMs":"15000","sessionToken":"history-session","sessionExpiresAtUnixMs":"1000","leaseTtlMs":"300000"}\n',
            '{"type":"batch","streamId":"history-1","originKind":"historical_backfill","originId":"repair-1","publicationRevision":"0","fromBlock":1,"throughBlock":1,"processedBlockCount":"1","domainChangeCount":"1","progressUnitCount":"1","rawPayloadBytes":"8","uncompressedEncodedBytes":"32","transmittedBytes":"32","buildDelayMs":"0","firstCursor":"history-event-1","lastCursor":"history-event-1","acknowledgeableCursor":"history-boundary-1","changes":[{"kind":"price.history"}]}\n',
            '{"type":"backfill_complete","subscriptionId":"repair-1","streamId":"history-1","ranges":[{"fromBlock":1,"toBlock":1}],"throughBlock":1,"cursor":"history-complete-2","mode":"fill_missing","disposition":"published_all","requestedBlockCount":"1","coveredBeforeRequestBlockCount":"0","coveredBeforeRequestRanges":[],"newlyProcessedBlockCount":"1","republishedBlockCount":"1","domainChangeCount":"1","preexistingCoverageSkipped":false}\n',
          ]),
          { headers: { "content-type": "application/x-ndjson" } },
        );
      },
    });
    const delivery = await client.subscribe<{ kind: string }>({
      processor: "prices",
      consumer: "destination",
      lanes: { live: true, history: [{ subscriptionId: "repair-1" }] },
      view: "live_priority",
    });
    const observed: string[] = [];
    for await (const batch of delivery.batches()) {
      observed.push(batch.completion ? "history.complete" : batch.events[0]!.kind);
      await delivery.acknowledge(batch);
      if (batch.completion) break;
    }
    expect(observed).toEqual([
      "price.live",
      "price.history",
      "history.complete",
    ]);
    expect(acknowledged).toEqual([
      "http://node.test/v1/processors/prices/streams/live/consumers/destination/ack",
      "http://node.test/v1/backfill-subscriptions/repair-1/consumers/destination/ack",
      "http://node.test/v1/backfill-subscriptions/repair-1/consumers/destination/ack",
    ]);
    await delivery.close();
  });

  test("lane-specific view consumes and acknowledges several history lanes independently", async () => {
    const acknowledged: string[] = [];
    const client = createBackfillSubscriptionClient({
      baseUrl: "http://node.test",
      fetch: async (input, init) => {
        const request = new Request(input, init);
        if (request.method === "DELETE") {
          return new Response(null, { status: 204 });
        }
        if (request.url.endsWith("/ack")) {
          acknowledged.push(request.url);
          return Response.json({ acknowledgedSequence: "2" });
        }
        const path = new URL(request.url).pathname.split("/");
        const subscription = path[3]!;
        const suffix = subscription === "history-a" ? "a" : "b";
        return new Response(
          chunkedBody([
            `{"type":"hello","apiVersion":"1","chainId":1,"processor":{"id":"prices","instance":"prices","version":"1.0.0","codeHash":"0x01","configHash":"0x02","genericApi":"processor-v1","changeSchema":"prices.change.v1","queryExtensions":[],"subscriptions":true,"artifactRetention":"none","deliveryOrdering":"block_versioned_idempotent"},"subscriptionId":"${subscription}","streamId":"stream-${suffix}","streamKind":"backfill","publicationRevision":"0","ranges":[{"fromBlock":1,"toBlock":1}],"acknowledgedCursor":"${suffix}-0","storeEpoch":"00","heartbeatIntervalMs":"15000","sessionToken":"session-${suffix}","sessionExpiresAtUnixMs":"1000","leaseTtlMs":"300000"}\n`,
            `{"type":"batch","streamId":"stream-${suffix}","originKind":"historical_backfill","originId":"${subscription}","publicationRevision":"0","fromBlock":1,"throughBlock":1,"processedBlockCount":"1","domainChangeCount":"1","progressUnitCount":"1","rawPayloadBytes":"8","uncompressedEncodedBytes":"32","transmittedBytes":"32","buildDelayMs":"0","firstCursor":"${suffix}-event","lastCursor":"${suffix}-event","acknowledgeableCursor":"${suffix}-boundary","changes":[{"kind":"price.${suffix}"}]}\n`,
            `{"type":"backfill_complete","subscriptionId":"${subscription}","streamId":"stream-${suffix}","ranges":[{"fromBlock":1,"toBlock":1}],"throughBlock":1,"cursor":"${suffix}-complete","mode":"fill_missing","disposition":"published_all","requestedBlockCount":"1","coveredBeforeRequestBlockCount":"0","coveredBeforeRequestRanges":[],"newlyProcessedBlockCount":"1","republishedBlockCount":"1","domainChangeCount":"1","preexistingCoverageSkipped":false}\n`,
          ]),
          { headers: { "content-type": "application/x-ndjson" } },
        );
      },
    });
    const delivery = await client.subscribe<{ kind: string }>({
      processor: "prices",
      consumer: "destination",
      lanes: {
        history: [
          { subscriptionId: "history-a" },
          { subscriptionId: "history-b" },
        ],
      },
      view: "lane_specific",
    });
    const consume = async (subscription: string): Promise<string[]> => {
      const observed: string[] = [];
      for await (const batch of delivery.historyBatches(subscription)) {
        observed.push(batch.completion ? `${subscription}.complete` : batch.events[0]!.kind);
        await delivery.acknowledge(batch);
      }
      return observed;
    };
    const [first, second] = await Promise.all([
      consume("history-a"),
      consume("history-b"),
    ]);
    expect(first).toEqual(["price.a", "history-a.complete"]);
    expect(second).toEqual(["price.b", "history-b.complete"]);
    expect(acknowledged.sort()).toEqual([
      "http://node.test/v1/backfill-subscriptions/history-a/consumers/destination/ack",
      "http://node.test/v1/backfill-subscriptions/history-a/consumers/destination/ack",
      "http://node.test/v1/backfill-subscriptions/history-b/consumers/destination/ack",
      "http://node.test/v1/backfill-subscriptions/history-b/consumers/destination/ack",
    ]);
    await delivery.close();
  });

  test("canonical processors reject concurrent live and history interleaving", async () => {
    let releases = 0;
    const client = createBackfillSubscriptionClient({
      baseUrl: "http://node.test",
      fetch: async (input, init) => {
        const request = new Request(input, init);
        if (request.method === "DELETE") {
          releases += 1;
          return new Response(null, { status: 204 });
        }
        const live = request.url.includes("/streams/live/");
        const hello = live
          ? '{"type":"hello","apiVersion":"1","chainId":1,"processor":{"id":"ordered","instance":"ordered","version":"1.0.0","codeHash":"0x01","configHash":"0x02","genericApi":"processor-v1","changeSchema":"ordered.change.v1","queryExtensions":[],"subscriptions":true,"artifactRetention":"none","deliveryOrdering":"canonical"},"streamId":"ordered:canonical","streamKind":"live","acknowledgedCursor":"live-0","storeEpoch":"00","heartbeatIntervalMs":"15000","sessionToken":"live-session","sessionExpiresAtUnixMs":"1000","leaseTtlMs":"300000"}\n'
          : '{"type":"hello","apiVersion":"1","chainId":1,"processor":{"id":"ordered","instance":"ordered","version":"1.0.0","codeHash":"0x01","configHash":"0x02","genericApi":"processor-v1","changeSchema":"ordered.change.v1","queryExtensions":[],"subscriptions":true,"artifactRetention":"none","deliveryOrdering":"canonical"},"subscriptionId":"history-1","streamId":"ordered:history","streamKind":"backfill","publicationRevision":"0","ranges":[{"fromBlock":1,"toBlock":1}],"acknowledgedCursor":"history-0","storeEpoch":"00","heartbeatIntervalMs":"15000","sessionToken":"history-session","sessionExpiresAtUnixMs":"1000","leaseTtlMs":"300000"}\n';
        return new Response(chunkedBody([hello]), {
          headers: { "content-type": "application/x-ndjson" },
        });
      },
    });
    let caught: unknown;
    try {
      await client.subscribe({
        processor: "ordered",
        consumer: "destination",
        lanes: { live: true, history: [{ subscriptionId: "history-1" }] },
      });
    } catch (error) {
      caught = error;
    }
    expect(caught).toBeInstanceOf(TypeError);
    expect((caught as Error).message).toContain(
      "canonical processors cannot interleave",
    );
    expect(releases).toBe(2);
  });

  test("a history lane reconnects independently and resumes from its durable acknowledgement", async () => {
    let streamConnections = 0;
    let sessionReleases = 0;
    const acknowledged: string[] = [];
    const client = createBackfillSubscriptionClient({
      baseUrl: "http://node.test",
      fetch: async (input, init) => {
        const request = new Request(input, init);
        if (request.url.endsWith("/lease") && request.method === "DELETE") {
          sessionReleases += 1;
          return new Response(null, { status: 204 });
        }
        if (request.url.endsWith("/ack")) {
          acknowledged.push(String((await request.json() as { cursor: string }).cursor));
          return Response.json({ acknowledgedSequence: "1" });
        }
        streamConnections += 1;
        const hello = `{"type":"hello","apiVersion":"1","chainId":1,"processor":{"id":"prices","instance":"prices","version":"1.0.0","codeHash":"0x01","configHash":"0x02","genericApi":"processor-v1","changeSchema":"prices.change.v1","queryExtensions":[],"subscriptions":true,"artifactRetention":"none","deliveryOrdering":"block_versioned_idempotent"},"subscriptionId":"repair-1","streamId":"history-1","streamKind":"backfill","publicationRevision":"0","ranges":[{"fromBlock":1,"toBlock":2}],"acknowledgedCursor":"history-${streamConnections - 1}","storeEpoch":"00","heartbeatIntervalMs":"15000","sessionToken":"history-session-${streamConnections}","sessionExpiresAtUnixMs":"1000","leaseTtlMs":"300000"}\n`;
        if (streamConnections === 1) {
          return new Response(
            chunkedBody([
              hello,
              '{"type":"batch","streamId":"history-1","originKind":"historical_backfill","originId":"repair-1","publicationRevision":"0","fromBlock":1,"throughBlock":1,"processedBlockCount":"1","domainChangeCount":"1","progressUnitCount":"1","rawPayloadBytes":"8","uncompressedEncodedBytes":"32","transmittedBytes":"32","buildDelayMs":"0","firstCursor":"history-event-1","lastCursor":"history-event-1","acknowledgeableCursor":"history-boundary-1","changes":[{"kind":"price.one"}]}\n',
            ]),
            { headers: { "content-type": "application/x-ndjson" } },
          );
        }
        return new Response(
          chunkedBody([
            hello,
            '{"type":"batch","streamId":"history-1","originKind":"historical_backfill","originId":"repair-1","publicationRevision":"0","fromBlock":2,"throughBlock":2,"processedBlockCount":"1","domainChangeCount":"1","progressUnitCount":"1","rawPayloadBytes":"8","uncompressedEncodedBytes":"32","transmittedBytes":"32","buildDelayMs":"0","firstCursor":"history-event-2","lastCursor":"history-event-2","acknowledgeableCursor":"history-boundary-2","changes":[{"kind":"price.two"}]}\n',
            '{"type":"backfill_complete","subscriptionId":"repair-1","streamId":"history-1","ranges":[{"fromBlock":1,"toBlock":2}],"throughBlock":2,"cursor":"history-complete-3","mode":"fill_missing","disposition":"published_all","requestedBlockCount":"2","coveredBeforeRequestBlockCount":"0","coveredBeforeRequestRanges":[],"newlyProcessedBlockCount":"2","republishedBlockCount":"2","domainChangeCount":"2","preexistingCoverageSkipped":false}\n',
          ]),
          { headers: { "content-type": "application/x-ndjson" } },
        );
      },
    });
    const delivery = await client.subscribe<{ kind: string }>({
      processor: "prices",
      consumer: "destination",
      lanes: { history: [{ subscriptionId: "repair-1" }] },
    });
    const observed: string[] = [];
    for await (const batch of delivery.batches()) {
      observed.push(batch.completion ? "complete" : batch.events[0]!.kind);
      await delivery.acknowledge(batch);
    }
    expect(observed).toEqual(["price.one", "price.two", "complete"]);
    expect(acknowledged).toEqual([
      "history-boundary-1",
      "history-boundary-2",
      "history-complete-3",
    ]);
    expect(streamConnections).toBe(2);
    await delivery.close();
    expect(sessionReleases).toBe(2);
  });

  test("unified acknowledgement retries transparently after a successful response is lost", async () => {
    let streamConnections = 0;
    let acknowledgementAttempts = 0;
    const client = createBackfillSubscriptionClient({
      baseUrl: "http://node.test",
      fetch: async (input, init) => {
        const request = new Request(input, init);
        if (request.method === "DELETE") {
          return new Response(null, { status: 204 });
        }
        if (request.url.endsWith("/ack")) {
          acknowledgementAttempts += 1;
          if (acknowledgementAttempts === 1) {
            throw new TypeError(
              "fetch failed after the server applied the acknowledgement",
            );
          }
          return Response.json({ acknowledgedSequence: "2" });
        }
        streamConnections += 1;
        const hello = `{"type":"hello","apiVersion":"1","chainId":1,"processor":{"id":"prices","instance":"prices","version":"1.0.0","codeHash":"0x01","configHash":"0x02","genericApi":"processor-v1","changeSchema":"prices.change.v1","queryExtensions":[],"subscriptions":true,"artifactRetention":"none","deliveryOrdering":"block_versioned_idempotent"},"subscriptionId":"repair-1","streamId":"history-1","streamKind":"backfill","publicationRevision":"0","ranges":[{"fromBlock":1,"toBlock":1}],"acknowledgedCursor":"history-${streamConnections - 1}","storeEpoch":"00","heartbeatIntervalMs":"15000","sessionToken":"session-${streamConnections}","sessionExpiresAtUnixMs":"1000","leaseTtlMs":"300000"}\n`;
        const records = streamConnections === 1
          ? [
              hello,
              '{"type":"batch","streamId":"history-1","originKind":"historical_backfill","originId":"repair-1","publicationRevision":"0","fromBlock":1,"throughBlock":1,"processedBlockCount":"1","domainChangeCount":"1","progressUnitCount":"1","rawPayloadBytes":"8","uncompressedEncodedBytes":"32","transmittedBytes":"32","buildDelayMs":"0","firstCursor":"event-1","lastCursor":"event-1","acknowledgeableCursor":"boundary-1","changes":[{"kind":"price.one"}]}\n',
            ]
          : [
              hello,
              '{"type":"backfill_complete","subscriptionId":"repair-1","streamId":"history-1","ranges":[{"fromBlock":1,"toBlock":1}],"throughBlock":1,"cursor":"complete-2","mode":"fill_missing","disposition":"published_all","requestedBlockCount":"1","coveredBeforeRequestBlockCount":"0","coveredBeforeRequestRanges":[],"newlyProcessedBlockCount":"1","republishedBlockCount":"1","domainChangeCount":"1","preexistingCoverageSkipped":false}\n',
            ];
        return new Response(chunkedBody(records), {
          headers: { "content-type": "application/x-ndjson" },
        });
      },
    });
    const delivery = await client.subscribe<{ kind: string }>({
      processor: "prices",
      consumer: "destination",
      lanes: { history: [{ subscriptionId: "repair-1" }] },
    });
    const observed: string[] = [];
    for await (const batch of delivery.batches()) {
      observed.push(batch.completion ? "complete" : batch.events[0]!.kind);
      await delivery.acknowledge(batch);
    }
    expect(observed).toEqual(["price.one", "complete"]);
    expect(streamConnections).toBe(2);
    expect(acknowledgementAttempts).toBe(3);
    await delivery.close();
  });

  test("a live gap notice becomes an idempotent fill_missing subscription", async () => {
    const created: unknown[] = [];
    const client = createBackfillSubscriptionClient({
      baseUrl: "http://node.test",
      fetch: async (input, init) => {
        const request = new Request(input, init);
        if (request.method === "DELETE") {
          return new Response(null, { status: 204 });
        }
        if (request.url.endsWith("/admin/v1/backfill-subscriptions")) {
          created.push(await request.json());
          return Response.json({ id: "subscription-1" });
        }
        return new Response(
          chunkedBody([
            '{"type":"hello","apiVersion":"1","chainId":1,"processor":{"id":"blobs-money","instance":"blobs-production","version":"1.0.0","codeHash":"0x01","configHash":"0x02","genericApi":"processor-v1","changeSchema":"blobs-money.change.v1","queryExtensions":[],"subscriptions":true,"artifactRetention":"none","deliveryOrdering":"block_versioned_idempotent"},"streamId":"blobs-production:live","streamKind":"live","acknowledgedCursor":"cursor-0","storeEpoch":"00","heartbeatIntervalMs":"15000","sessionToken":"live-session","sessionExpiresAtUnixMs":"1000","leaseTtlMs":"300000","coverage":{"chainId":1,"chainFinalizedHead":null,"available":[{"fromBlock":26107000,"toBlock":26107398,"finality":"finalized"}],"configuredStartBlock":26107000,"processedThrough":26107398,"finalizedThrough":26107398,"complete":true,"state":"live"}}\n',
            '{"type":"batch","streamId":"blobs-production:live","originKind":"live","originId":"blobs-production","publicationRevision":"0","fromBlock":26109950,"throughBlock":26109950,"processedBlockCount":"1","domainChangeCount":"0","progressUnitCount":"1","rawPayloadBytes":"32","uncompressedEncodedBytes":"300","transmittedBytes":"300","buildDelayMs":"0","firstCursor":"cursor-1","lastCursor":"cursor-2","acknowledgeableCursor":"cursor-2","changes":[{"operation":"finalized","kind":"system.finality.put","cursor":"cursor-1","data":{"throughBlock":26109949}},{"operation":"finalized","kind":"system.live_gap.put","cursor":"cursor-2","data":{"fromBlock":26107399,"toBlock":26109949,"reason":"not_filled"}}]}\n',
          ]),
          { headers: { "content-type": "application/x-ndjson" } },
        );
      },
    });
    const delivery = await client.subscribe({
      processor: "blobs-production",
      consumer: "blobs-api",
      lanes: { live: true },
    });
    for await (const batch of delivery.batches()) {
      for (const change of batch.events) {
        if (isLiveGapChange(change)) {
          // A notice delivered again requests the same subscription.
          for (let attempt = 0; attempt < 2; attempt += 1) {
            await client.create(
              liveGapBackfill(change.data, {
                processor: "blobs-production",
                consumer: { id: "blobs-api", role: "required", leaseTtlSeconds: 300 },
              }),
            );
          }
        }
      }
      break;
    }
    await delivery.close();
    const request = {
      processor: "blobs-production",
      fromBlock: 26107399,
      toBlock: 26109949,
      mode: "fill_missing",
      consumer: { id: "blobs-api", role: "required", leaseTtlSeconds: 300 },
      idempotencyKey: "live-gap:blobs-api:26107399-26109949",
    };
    expect(created).toEqual([request, request]);
  });

  test("client consumes the processor live lane independently", async () => {
    const requests: Request[] = [];
    const client = createBackfillSubscriptionClient({
      baseUrl: "http://node.test",
      token: "node-token",
      fetch: async (input, init) => {
        const request = new Request(input, init);
        requests.push(request);
        if (request.method === "DELETE") {
          return new Response(null, { status: 204 });
        }
        if (request.url.endsWith("/ack")) {
          return Response.json({ acknowledgedSequence: "1" });
        }
        return new Response(
          chunkedBody([
            '{"type":"hello","apiVersion":"1","chainId":1,"processor":{"id":"blobs-money","instance":"blobs-production","version":"1.0.0","codeHash":"0x01","configHash":"0x02","genericApi":"processor-v1","changeSchema":"blobs-money.change.v1","queryExtensions":[],"subscriptions":true,"artifactRetention":"none","deliveryOrdering":"block_versioned_idempotent"},"streamId":"live-1","streamKind":"live","acknowledgedCursor":"cursor-0","storeEpoch":"00","heartbeatIntervalMs":"15000","sessionToken":"live-session","sessionExpiresAtUnixMs":"1000","leaseTtlMs":"300000"}\n',
            '{"type":"batch","streamId":"live-1","originKind":"live","originId":"blobs-production","publicationRevision":"0","fromBlock":10,"throughBlock":10,"processedBlockCount":"1","domainChangeCount":"1","progressUnitCount":"1","rawPayloadBytes":"32","uncompressedEncodedBytes":"32","transmittedBytes":"32","buildDelayMs":"0","firstCursor":"cursor-1","lastCursor":"cursor-1","acknowledgeableCursor":"cursor-1","changes":[]}\n',
          ]),
          {
            headers: { "content-type": "application/x-ndjson" },
          },
        );
      },
    });

    const session = await client.streamLive(
      "blobs-production",
      "blobs-api",
      { credential: "consumer-secret" },
    );
    expect(session.hello).toMatchObject({
      type: "hello",
      streamKind: "live",
      streamId: "live-1",
    });
    const records: LiveStreamRecord[] = [];
    for await (const record of session) {
      records.push(record);
      if (record.type === "batch") {
        await session.acknowledge(record.acknowledgeableCursor);
      }
    }
    expect(records.map((record) => record.type)).toEqual(["batch"]);
    expect(records[0]).toMatchObject({
      originKind: "live",
      fromBlock: 10,
      throughBlock: 10,
    });
    expect(requests[0]?.url).toBe(
      "http://node.test/v1/processors/blobs-production/streams/live/consumers/blobs-api/stream",
    );
    expect(requests[1]?.url).toBe(
      "http://node.test/v1/processors/blobs-production/streams/live/consumers/blobs-api/ack",
    );
    expect(await requests[1]?.json()).toEqual({ cursor: "cursor-1" });
    expect(requests[1]?.headers.get("x-leani-consumer-session")).toBe(
      "live-session",
    );
    expect(requests[0]?.headers.get("authorization")).toBe("Bearer node-token");
    expect(
      requests[1]?.headers.get("x-leani-consumer-credential"),
    ).toBe("consumer-secret");
    expect(requests[0]?.headers.get("accept")).toBe("application/x-ndjson");
    expect(requests[2]?.method).toBe("DELETE");
    expect(requests[2]?.headers.get("x-leani-consumer-session")).toBe(
      "live-session",
    );
  });

  test("an active iterator renews its session before the lease expires", async () => {
    const encoder = new TextEncoder();
    const renewals: Request[] = [];
    const client = createBackfillSubscriptionClient({
      baseUrl: "http://node.test",
      fetch: async (input, init) => {
        const request = new Request(input, init);
        if (request.url.endsWith("/lease") && request.method === "POST") {
          renewals.push(request);
          return Response.json({ leaseActive: true });
        }
        if (request.url.endsWith("/lease") && request.method === "DELETE") {
          return new Response(null, { status: 204 });
        }
        return new Response(
          new ReadableStream<Uint8Array>({
            start(controller) {
              controller.enqueue(
                encoder.encode(
                  '{"type":"hello","sessionToken":"live-session","leaseTtlMs":"180","heartbeatIntervalMs":"15000"}\n',
                ),
              );
              setTimeout(() => {
                controller.enqueue(
                  encoder.encode(
                    '{"type":"heartbeat","emittedAt":"2026-08-08T00:00:00.000Z"}\n',
                  ),
                );
              }, 90);
              setTimeout(() => controller.close(), 140);
            },
          }),
        );
      },
    });

    const session = await client.streamLive(
      "blobs-production",
      "blobs-api",
      { credential: "consumer-secret" },
    );
    for await (const _record of session) {
      // Keep the iterator open until the fixture closes after one renewal.
    }
    expect(renewals.length).toBeGreaterThanOrEqual(1);
    expect(renewals[0]?.headers.get("x-leani-consumer-session")).toBe(
      "live-session",
    );
    expect(renewals[0]?.headers.get("x-leani-consumer-credential")).toBe(
      "consumer-secret",
    );
    expect(renewals[0]?.headers.get("x-leani-request")).toBe("1");
  });

  test("reset_required terminates the stream with retained bounds", async () => {
    const client = createBackfillSubscriptionClient({
      baseUrl: "http://node.test",
      fetch: async () =>
        new Response(
          '{"type":"reset_required","code":"reset_required","message":"cursor was pruned","earliestAvailableSequence":"10","latestAvailableSequence":"20"}\n',
        ),
    });

    let caught: unknown;
    try {
      const session = await client.stream(
        "repair-1",
        "blobs-api",
        { credential: "secret" },
      );
      for await (const _record of session) {
        // The reset record is terminal and is not yielded as application data.
      }
    } catch (error) {
      caught = error;
    }
    expect(caught).toBeInstanceOf(BackfillStreamError);
    expect((caught as BackfillStreamError).code).toBe("reset_required");
    expect(
      (caught as BackfillStreamError).earliestAvailableSequence,
    ).toBe("10");
  });
});

test("backfill HTTP errors share the main client's error contract", async () => {
  const { LeaniError } = await import("../src/index.ts");
  const { LeaniError: BackfillError } = await import("../src/backfill.ts");
  expect(BackfillError).toBe(LeaniError);
  for (const payload of [{}, { error: { code: "busy", message: "try later", retryable: true, requestId: "r-1" } }]) {
    const client = createBackfillSubscriptionClient({
      baseUrl: "http://node.test",
      fetch: async () => Response.json(payload, { status: 503 }),
    });
    try {
      await client.list();
      throw new Error("expected failure");
    } catch (error) {
      expect(error).toBeInstanceOf(LeaniError);
      expect((error as InstanceType<typeof LeaniError>).status).toBe(503);
      expect((error as InstanceType<typeof LeaniError>).retryable).toBe(true);
    }
  }
});

type HistoryStep =
  | "batch"
  | "complete"
  | "hang"
  | "session_active"
  | "backfill_failed"
  | "backfill_cancelled"
  | "consumer_session_lost"
  | "reset_required";

/**
 * A node with one application-owned subscription, `sub-1`, whose history is
 * two one-block batches and a completion. Each stream connection sends its
 * hello and then its steps: "batch" is the next batch after the last one
 * acknowledged, as the node redelivers what was not acknowledged; a code is
 * that stream record; "hang" keeps the connection open; and "session_active"
 * refuses the connection while another session holds the lease. Like the
 * node, it refuses a consumer request without the consumer's credential.
 * `log` records each request the node handles.
 */
function fakeHistoryNode(
  connections: HistoryStep[][],
  initialState: BackfillState = "queued",
) {
  const log: string[] = [];
  const encoder = new TextEncoder();
  let state = initialState;
  let lastError = state === "failed" ? "source unavailable" : null;
  let acknowledged = 0;
  let opened = 0;
  const admin = "/admin/v1/backfill-subscriptions";
  const consumer = "/v1/backfill-subscriptions/sub-1/consumers/blobs-api";
  const failure = (status: number, code: string, message: string) =>
    Response.json(
      { error: { code, message, retryable: false, requestId: "request-1" } },
      { status },
    );
  const status = () => ({
    id: "sub-1",
    owner: "subscription",
    processor: "blobs-production",
    deliveryStreamId: "history-1",
    fromBlock: 1,
    toBlock: 2,
    ranges: [{ fromBlock: 1, toBlock: 2 }],
    requestedBlocks: 2,
    processedBlocks: acknowledged,
    remainingBlocks: 2 - acknowledged,
    mode: "fill_missing",
    state,
    attempts: opened,
    updatedAtUnixMs: 1,
    report: null,
    lastError,
  });
  const hello = () => ({
    type: "hello",
    apiVersion: "1",
    chainId: 1,
    processor: { id: "blobs-money", instance: "blobs-production", version: "1.0.0", codeHash: "0x01", configHash: "0x02", genericApi: "processor-v1", changeSchema: "blobs-money.change.v1", queryExtensions: [], subscriptions: true, artifactRetention: "none", deliveryOrdering: "block_versioned_idempotent" },
    subscriptionId: "sub-1",
    streamId: "history-1",
    streamKind: "backfill",
    publicationRevision: "0",
    ranges: [{ fromBlock: 1, toBlock: 2 }],
    acknowledgedCursor: `boundary-${acknowledged}`,
    storeEpoch: "00",
    heartbeatIntervalMs: "15000",
    sessionToken: `session-${opened}`,
    sessionExpiresAtUnixMs: "1000",
    leaseTtlMs: "300000",
  });
  const batch = (block: number) => ({
    type: "batch",
    streamId: "history-1",
    originKind: "historical_backfill",
    originId: "sub-1",
    publicationRevision: "0",
    fromBlock: block,
    throughBlock: block,
    processedBlockCount: "1",
    domainChangeCount: "1",
    progressUnitCount: "1",
    rawPayloadBytes: "8",
    uncompressedEncodedBytes: "64",
    transmittedBytes: "64",
    buildDelayMs: "0",
    firstCursor: `event-${block}`,
    lastCursor: `event-${block}`,
    acknowledgeableCursor: `boundary-${block}`,
    changes: [
      {
        apiVersion: "1",
        sequence: String(block),
        cursor: `event-${block}`,
        originKind: "historical_backfill",
        originId: "sub-1",
        publicationRevision: "0",
        chainId: 1,
        block: { number: block, hash: `0x0${block}`, parentHash: `0x0${block - 1}`, timestamp: 1_700_000_000 + block * 12 },
        finality: "finalized",
        kind: "price.put",
        schema: "blobs-money.change.v1",
        key: `0x0${block}`,
        operation: "apply",
        data: { block },
        emittedAt: "2026-10-05T00:00:00.000Z",
      },
    ],
  });
  const completion = {
    type: "backfill_complete",
    subscriptionId: "sub-1",
    streamId: "history-1",
    ranges: [{ fromBlock: 1, toBlock: 2 }],
    throughBlock: 2,
    cursor: "complete",
    mode: "fill_missing",
    disposition: "published_all",
    requestedBlockCount: "2",
    coveredBeforeRequestBlockCount: "0",
    coveredBeforeRequestRanges: [],
    newlyProcessedBlockCount: "2",
    republishedBlockCount: "2",
    domainChangeCount: "2",
    preexistingCoverageSkipped: false,
  };
  const streamFailure = (code: string, message: string) => ({
    type: code === "reset_required" ? "reset_required" : "error",
    code,
    message,
    earliestAvailableSequence: null,
    latestAvailableSequence: null,
  });
  const stream = (): Response => {
    const steps = connections[opened];
    opened += 1;
    if (steps === undefined) {
      log.push("unexpected stream");
      return failure(404, "not_found", "no further stream connection");
    }
    if (steps[0] === "session_active") {
      return failure(409, "consumer_session_active", "another session holds the lease");
    }
    const records: unknown[] = [hello()];
    let next = acknowledged + 1;
    for (const step of steps) {
      if (step === "batch") {
        records.push(batch(next++));
      } else if (step === "complete") {
        records.push(completion);
      } else if (step === "backfill_failed") {
        state = "failed";
        lastError = `connection ${opened} failed`;
        records.push(streamFailure(step, lastError));
      } else if (step === "backfill_cancelled") {
        state = "cancelled";
        records.push(streamFailure(step, "the historical subscription was cancelled"));
      } else if (step !== "hang") {
        records.push(streamFailure(step, `${step} on connection ${opened}`));
      }
    }
    return new Response(
      new ReadableStream<Uint8Array>({
        start(controller) {
          for (const record of records) {
            controller.enqueue(encoder.encode(`${JSON.stringify(record)}\n`));
          }
          if (!steps.includes("hang")) {
            controller.close();
          }
        },
      }),
      { headers: { "content-type": "application/x-ndjson" } },
    );
  };
  const fetch = async (
    input: RequestInfo | URL,
    init?: RequestInit,
  ): Promise<Response> => {
    // Like fetch, an aborted request never reaches the node.
    init?.signal?.throwIfAborted();
    const request = new Request(input, init);
    const { pathname, search } = new URL(request.url);
    if (
      pathname.startsWith(consumer) &&
      request.headers.get("x-leani-consumer-credential") !== "consumer-secret"
    ) {
      log.push(`unauthorized ${pathname}`);
      return failure(401, "consumer_unauthorized", "consumer credential required");
    }
    switch (`${request.method} ${pathname}`) {
      case `POST ${admin}`: {
        const body = (await request.json()) as { idempotencyKey: string };
        log.push(`create ${body.idempotencyKey}`);
        return Response.json(status());
      }
      case `GET ${admin}/sub-1`:
        log.push("inspect");
        return Response.json(status());
      case `POST ${admin}/sub-1/retry`:
        log.push("retry");
        state = "queued";
        return Response.json(status());
      case `DELETE ${admin}/sub-1`:
        log.push(
          search === "?discardUnacknowledged=true"
            ? "delete discarding unacknowledged"
            : "delete",
        );
        return Response.json({
          id: "sub-1",
          owner: "subscription",
          removedJobs: 1,
          removedSubscriptionRanges: 1,
          removedConsumers: 1,
          removedDeliveryRecords: 3,
          removedDeliveryStreams: 1,
          removedCoverageIntervals: 0,
          removedCoverageSegments: 0,
          removedExactCoverage: 0,
          removedAppliedBlocks: 0,
          removedFinalizedUndo: 0,
          retainedProcessorOutput: true,
          retainedLiveStream: true,
        });
      case `GET ${consumer}/stream`:
        log.push(`stream${search}`);
        return stream();
      case `POST ${consumer}/ack`: {
        const { cursor } = (await request.json()) as { cursor: string };
        log.push(`ack ${cursor}`);
        if (cursor === "complete") {
          state = "complete_reclaimable";
        } else {
          acknowledged = Number(cursor.replace("boundary-", ""));
        }
        return Response.json({
          id: "blobs-api",
          processorInstance: "blobs-production",
          streamId: "history-1",
          role: "required",
          state: "active",
          acknowledgedSequence: String(acknowledged),
          deliveredSequence: String(acknowledged),
          acknowledgedCursor: cursor,
          deliveredCursor: cursor,
          leaseGeneration: String(opened),
          leaseTtlMs: "300000",
          leaseExpiresAtUnixMs: "1000",
          leaseActive: true,
          lagChanges: "0",
          lagBlocks: "0",
          lagBytes: "0",
          lagAgeMs: "0",
          createdAtUnixMs: "1",
          updatedAtUnixMs: "1",
        });
      }
      case `DELETE ${consumer}/lease`:
        log.push("release");
        return new Response(null, { status: 204 });
    }
    log.push(`unexpected ${request.method} ${pathname}`);
    return failure(404, "not_found", "no such route");
  };
  return { fetch, log };
}

function nodeClient(node: ReturnType<typeof fakeHistoryNode>) {
  return createBackfillSubscriptionClient({
    baseUrl: "http://node.test",
    fetch: node.fetch,
  });
}

/** Commit a batch after a moment, as a destination does, then log it. */
function applyTo(log: string[]) {
  return async (batch: HistoryDeliveryBatch): Promise<void> => {
    await new Promise((resolve) => setTimeout(resolve, 5));
    log.push(`apply ${batch.ackCursor}`);
  };
}

const historyRequest: CreateBackfillSubscription = {
  processor: "blobs-production",
  fromBlock: 1,
  toBlock: 2,
  idempotencyKey: "repair-1",
  consumer: {
    id: "blobs-api",
    role: "required",
    leaseTtlSeconds: 300,
    credential: "consumer-secret",
  },
};

describe("runBackfill", () => {
  test.each([
    ["no consumer", { ...historyRequest, consumer: undefined }, {}, "request.consumer"],
    ["negative maxRetries", historyRequest, { maxRetries: -1 }, "maxRetries"],
    ["NaN maxRetries", historyRequest, { maxRetries: Number.NaN }, "maxRetries"],
  ] as const)(
    "a run with %s is refused before any request",
    async (_case, request, options, field) => {
      const node = fakeHistoryNode([]);
      const refusal = await runBackfill(nodeClient(node), request, {
        onBatch: applyTo(node.log),
        ...options,
      }).catch((error: unknown) => error);
      expect(refusal).toBeInstanceOf(TypeError);
      expect((refusal as TypeError).message).toContain(field);
      expect(node.log).toEqual([]);
    },
  );

  test("applies each batch before acknowledging it and deletes the completed subscription", async () => {
    const node = fakeHistoryNode([["batch", "batch", "complete"]]);
    const result = await runBackfill(nodeClient(node), historyRequest, {
      onBatch: applyTo(node.log),
      batching: { maximumEvents: 100 },
    });
    expect(node.log).toEqual([
      "create repair-1",
      "stream?maximumEvents=100",
      "apply boundary-1",
      "ack boundary-1",
      "apply boundary-2",
      "ack boundary-2",
      "ack complete",
      "release",
      "inspect",
      "delete",
    ]);
    expect(result).toMatchObject({
      outcome: "completed",
      subscription: { id: "sub-1", state: "complete_reclaimable" },
      completion: { type: "backfill_complete", cursor: "complete", throughBlock: 2 },
      deletion: { id: "sub-1", removedJobs: 1 },
    });
  });

  test.each([
    ["complete_reclaimable", "throw", "completed", "delete"],
    ["completed", "throw", "completed", "delete"],
    ["cancelled", "throw", "cancelled", "delete discarding unacknowledged"],
    ["failed", "discard", "failed", "delete discarding unacknowledged"],
  ] as const)(
    "a subscription %s at creation is deleted without opening its stream",
    async (state, onFailure, outcome, deletion) => {
      const node = fakeHistoryNode([], state);
      const result = await runBackfill(nodeClient(node), historyRequest, {
        onBatch: applyTo(node.log),
        onFailure,
      });
      expect(result).toMatchObject({ outcome, subscription: { id: "sub-1", state } });
      expect(result.completion).toBeUndefined();
      expect(node.log).toEqual(["create repair-1", deletion]);
    },
  );

  test.each([
    ["backfill_cancelled", "retry", { outcome: "cancelled", subscription: { state: "cancelled" } }],
    ["backfill_failed", "discard", { outcome: "failed", subscription: { state: "failed", lastError: "connection 1 failed" } }],
  ] as const)(
    "a stream ending with %s under onFailure %s deletes the subscription and its unacknowledged records",
    async (code, onFailure, expected) => {
      const node = fakeHistoryNode([["batch", code]]);
      const result = await runBackfill(nodeClient(node), historyRequest, {
        onBatch: applyTo(node.log),
        onFailure,
      });
      expect(result).toMatchObject(expected);
      expect(node.log).toEqual([
        "create repair-1",
        "stream",
        "apply boundary-1",
        "ack boundary-1",
        "release",
        "inspect",
        "delete discarding unacknowledged",
      ]);
    },
  );

  test("a failed subscription is retried and resumes after its last acknowledgement", async () => {
    const node = fakeHistoryNode([["batch", "backfill_failed"], ["batch", "complete"]]);
    const result = await runBackfill(nodeClient(node), historyRequest, {
      onBatch: applyTo(node.log),
    });
    expect(result.outcome).toBe("completed");
    expect(node.log).toEqual([
      "create repair-1",
      "stream",
      "apply boundary-1",
      "ack boundary-1",
      "release",
      "inspect",
      "retry",
      "stream",
      "apply boundary-2",
      "ack boundary-2",
      "ack complete",
      "release",
      "inspect",
      "delete",
    ]);
  });

  test("a subscription that keeps failing is left failed once its retries run out", async () => {
    const node = fakeHistoryNode([["backfill_failed"], ["backfill_failed"]], "failed");
    const run = runBackfill(nodeClient(node), historyRequest, {
      onBatch: applyTo(node.log),
      maxRetries: 2,
    });
    await expect(run).rejects.toMatchObject({
      name: "BackfillStreamError",
      code: "backfill_failed",
      message: "connection 2 failed",
    });
    expect(node.log).toEqual([
      "create repair-1",
      "retry",
      "stream",
      "release",
      "inspect",
      "retry",
      "stream",
      "release",
      "inspect",
    ]);
  });

  test("with onFailure throw, a failed subscription's error is thrown and the subscription kept", async () => {
    const node = fakeHistoryNode([], "failed");
    const run = runBackfill(nodeClient(node), historyRequest, {
      onBatch: applyTo(node.log),
      onFailure: "throw",
    });
    await expect(run).rejects.toMatchObject({
      name: "BackfillStreamError",
      code: "backfill_failed",
      message: "source unavailable",
    });
    expect(node.log).toEqual(["create repair-1"]);
  });

  // Before opening the stream again, the run reads whether the subscription
  // ended meanwhile.
  const resumed = [
    "create repair-1",
    "stream",
    "apply boundary-1",
    "ack boundary-1",
    "release",
    "inspect",
    "stream",
    "apply boundary-2",
    "ack boundary-2",
    "ack complete",
    "release",
    "inspect",
    "delete",
  ];
  test.each([
    ["a lost session", [["batch", "consumer_session_lost"], ["batch", "complete"]], resumed],
    ["a stream that ends early", [["batch"], ["batch", "complete"]], resumed],
    [
      "a session another connection still holds",
      [["session_active"], ["batch", "batch", "complete"]],
      ["create repair-1", "stream", "inspect", "stream", "apply boundary-1", "ack boundary-1", "apply boundary-2", "ack boundary-2", "ack complete", "release", "inspect", "delete"],
    ],
  ] as Array<[string, HistoryStep[][], string[]]>)(
    "%s is opened again after a backoff, without applying a batch twice",
    async (_case, connections, transcript) => {
      const node = fakeHistoryNode(connections);
      const started = performance.now();
      const result = await runBackfill(nodeClient(node), historyRequest, {
        onBatch: applyTo(node.log),
      });
      expect(performance.now() - started).toBeGreaterThanOrEqual(900);
      expect(result.outcome).toBe("completed");
      expect(node.log).toEqual(transcript);
    },
  );

  test("a reset_required stream is thrown and the subscription kept", async () => {
    const node = fakeHistoryNode([["reset_required"]]);
    const run = runBackfill(nodeClient(node), historyRequest, {
      onBatch: applyTo(node.log),
    });
    await expect(run).rejects.toMatchObject({
      name: "BackfillStreamResetError",
      code: "reset_required",
    });
    expect(node.log).toEqual(["create repair-1", "stream", "release"]);
  });

  test("an onBatch failure closes the session unacknowledged and is rethrown, even a retryable one", async () => {
    const node = fakeHistoryNode([["batch", "batch", "complete"]]);
    const failure = new TransportError("destination unreachable");
    const run = runBackfill(nodeClient(node), historyRequest, {
      onBatch: async () => {
        throw failure;
      },
    });
    await expect(run).rejects.toBe(failure);
    expect(node.log).toEqual(["create repair-1", "stream", "release"]);
  });

  test.each([
    ["the run's signal", undefined, "stop"],
    ["the request's signal", "stop", undefined],
    ["the request's signal beside the run's", "stop", "idle"],
  ] as const)(
    "an abort through %s while streaming closes the session and keeps the subscription",
    async (_case, requestSignal, runSignal) => {
      const node = fakeHistoryNode([["batch", "hang"]]);
      const stop = new AbortController();
      const signals = { stop: stop.signal, idle: new AbortController().signal };
      const reason = new Error("shutting down");
      const leani = createBackfillSubscriptionClient({
        baseUrl: "http://node.test",
        fetch: async (input, init) => {
          const response = await node.fetch(input, init);
          // Stop once the batch is acknowledged, while the run waits for more.
          if (String(input).endsWith("/ack")) stop.abort(reason);
          return response;
        },
      });
      const run = runBackfill(
        leani,
        { ...historyRequest, signal: requestSignal && signals[requestSignal] },
        {
          onBatch: applyTo(node.log),
          signal: runSignal && signals[runSignal],
        },
      );
      await expect(run).rejects.toBe(reason);
      expect(node.log).toEqual([
        "create repair-1",
        "stream",
        "apply boundary-1",
        "ack boundary-1",
        "release",
      ]);
    },
  );

  test("a completion whose acknowledgement answer is lost ends completed without reopening", async () => {
    const node = fakeHistoryNode([["batch", "batch", "complete"]]);
    const leani = createBackfillSubscriptionClient({
      baseUrl: "http://node.test",
      fetch: async (input, init) => {
        const response = await node.fetch(input, init);
        // The node applies the acknowledgement, but its answer never arrives.
        if (node.log.at(-1) === "ack complete") throw new TypeError("fetch failed");
        return response;
      },
    });
    const result = await runBackfill(leani, historyRequest, {
      onBatch: applyTo(node.log),
    });
    expect(result).toMatchObject({
      outcome: "completed",
      subscription: { state: "complete_reclaimable" },
      completion: { type: "backfill_complete", cursor: "complete" },
    });
    expect(node.log).toEqual([
      "create repair-1",
      "stream",
      "apply boundary-1",
      "ack boundary-1",
      "apply boundary-2",
      "ack boundary-2",
      "ack complete",
      "release",
      "inspect",
      "delete",
    ]);
  });

  test("an aborted run creates nothing", async () => {
    const node = fakeHistoryNode([]);
    const reason = new Error("shutting down");
    const run = runBackfill(nodeClient(node), historyRequest, {
      onBatch: applyTo(node.log),
      signal: AbortSignal.abort(reason),
    });
    await expect(run).rejects.toBe(reason);
    expect(node.log).toEqual([]);
  });

  test("a rerun after an interruption resumes the same subscription and deletes it once", async () => {
    const node = fakeHistoryNode([["batch", "batch", "complete"], ["batch", "complete"]]);
    const crash = new Error("process killed");
    let crashed = false;
    const onBatch = async (batch: HistoryDeliveryBatch): Promise<void> => {
      if (batch.ackCursor === "boundary-2" && !crashed) {
        crashed = true;
        throw crash;
      }
      await applyTo(node.log)(batch);
    };
    await expect(
      runBackfill(nodeClient(node), historyRequest, { onBatch }),
    ).rejects.toBe(crash);
    const result = await runBackfill(nodeClient(node), historyRequest, { onBatch });
    expect(result).toMatchObject({ outcome: "completed", subscription: { id: "sub-1" } });
    expect(node.log).toEqual([
      "create repair-1",
      "stream",
      "apply boundary-1",
      "ack boundary-1",
      "release",
      "create repair-1",
      "stream",
      "apply boundary-2",
      "ack boundary-2",
      "ack complete",
      "release",
      "inspect",
      "delete",
    ]);
  });
});
