import { describe, expect, test } from "bun:test";

import {
  BackfillStreamError,
  backfillBatchingProfile,
  createBackfillSubscriptionClient,
  parseNdjson,
  type BackfillStreamRecord,
  type LiveStreamRecord,
} from "../src/backfill.ts";

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
    expect(result).toMatchObject({
      removedDeliveryRecords: 100,
      retainedProcessorOutput: true,
      retainedLiveStream: true,
    });
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
                  '{"type":"hello","sessionToken":"live-session","leaseTtlMs":"180"}\n',
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
