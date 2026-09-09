import { expect, test } from "bun:test";
import { createLeaniClient, LeaniError, type UniswapPoolPrice } from "@leani/sdk";
import { followPrices } from "./index.ts";

const entity = (index: number) => ({
  key: `0x${index.toString(16).padStart(40, "0")}`,
  data: { pool: `0x${index}`, reserve0: String(index) },
});
const page = (data: unknown[], nextCursor: string | null) => ({
  data, nextCursor, snapshotId: "snapshot-1", boundaryCursor: "boundary-7",
});

test("runnable example seeds all 101 rows and applies a forward and inverse mutation", async () => {
  const stop = new AbortController();
  const last = entity(100);
  const edits: string[] = [];
  const prices = new class extends Map<string, UniswapPoolPrice> {
    override set(key: string, value: UniswapPoolPrice): this {
      if (key === last.key) {
        edits.push(value.reserve0!);
        if (edits.length === 3) stop.abort();
      }
      return super.set(key, value);
    }
  }();
  let released = false;
  let resumedAt: string | null = null;
  const client = createLeaniClient({ baseUrl: "http://example.test", fetch: async (input, init) => {
    const url = new URL(String(input));
    if (init?.method === "DELETE") {
      released = true;
      return new Response(null, { status: 204 });
    }
    if (url.pathname.endsWith("query-and-follow")) {
      return Response.json(page(Array.from({ length: 100 }, (_, i) => entity(i)), "page-2"));
    }
    if (url.pathname.endsWith("entities")) {
      expect(url.searchParams.get("cursor")).toBe("page-2");
      return Response.json(page([last], null));
    }
    expect(released).toBe(true);
    resumedAt = url.searchParams.get("after");
    const frames = ["apply", "undo"].map((operation, i) => `event: change\ndata: ${JSON.stringify({
      apiVersion: "1", sequence: String(8 + i), cursor: `cursor-${8 + i}`,
      operation, kind: "uniswap.pool.put", schema: "uniswap.pool-price.v2", key: last.key,
      data: { ...last.data, reserve0: operation === "apply" ? "200" : "100" },
    })}\n\n`).join("");
    return new Response(frames);
  } });
  await followPrices(client, prices, stop.signal);
  expect(prices.size).toBe(101);
  expect(edits).toEqual(["100", "200", "100"]);
  expect(resumedAt).toBe("boundary-7");
});

test("a failed later page releases the snapshot and preserves the previous complete seed", async () => {
  let released = false;
  const old = { reserve0: "previous" } as UniswapPoolPrice;
  const prices = new Map([["old", old]]);
  const client = createLeaniClient({ baseUrl: "http://example.test", fetch: async (input, init) => {
    if (init?.method === "DELETE") { released = true; return new Response(null, { status: 204 }); }
    if (String(input).endsWith("query-and-follow")) return Response.json(page([entity(0)], "page-2"));
    return Response.json({}, { status: 503 });
  } });
  await expect(followPrices(client, prices)).rejects.toBeInstanceOf(LeaniError);
  expect(released).toBe(true);
  expect([...prices]).toEqual([["old", old]]);
});
