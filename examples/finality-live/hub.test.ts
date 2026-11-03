import { expect, test } from "bun:test";
import type { BlobsSnapshotEntry, BlockSummary, ChangeEnvelope } from "@smart-byte/leani-sdk";
import { unpack } from "../../site/src/lib/live/feed.ts";
import type { HubEvent } from "../../site/src/lib/live/model.ts";
import { inboxBatch, pack, postings, toEvent } from "./hub.ts";

const change = (operation: string, number: number, data: unknown, finality = "unattested") => ({
  apiVersion: "1", sequence: String(number), cursor: `c${number}`, operation, originKind: "live",
  originId: "live", publicationRevision: "1", chainId: 8453, kind: "block-summary.put", schema: "s", key: null,
  block: { number, hash: `0x${number.toString(16)}`, parentHash: "0x", timestamp: 1_000 + number },
  finality, data, emittedAt: new Date(1_000_000 + number * 1_000).toISOString(),
}) as unknown as ChangeEnvelope<BlockSummary>;

test("changes become the page's events, markers carrying their block's time", () => {
  const times = new Map<number, number>();
  const summary = { transactionCount: 7, gasUsed: 30, gasLimit: 60 } as BlockSummary;
  expect(toEvent("base", change("apply", 5, summary), times)).toEqual({
    type: "block", chain: "base", number: 5, hash: "0x5", time: 1_005, seen: 1_005_000,
    level: "unattested", txs: 7, gas: 0.5,
  });
  expect(toEvent("base", change("finalized", 6, { throughBlock: 5 }), times)).toMatchObject({
    type: "level", level: "finalized", through: 5, time: 1_005,
  });
  expect(toEvent("base", change("undo", 5, summary), times)).toMatchObject({ type: "undo", number: 5, hash: "0x5" });
});

test("an Ethereum block counts its blobs by who posted them, and finds the batches to decode", () => {
  const blob = (from: string, to: string, blobCount: number, transactionIndex = 0) => ({
    senderAddress: from, toAddress: to, blobCount, transactionIndex,
    blobVersionedHashes: Array.from({ length: blobCount }, (_, index) => `0x01${index}`),
  });
  const entry = {
    block: { blockNumber: 9, blockHash: "0x9", timestamp: 1_009, transactionCount: 120, gasUsed: "15", gasLimit: "60" },
    transactions: [
      blob("0x6887246668A3B87F54DEB3B94BA47A6F63F32985", "0xFF00000000000000000000000000000000000010", 5),
      blob("0x5050f69a9786f081509234f1a7f4684b5e5b76c9", "0xff00000000000000000000000000000000008453", 6),
      blob("0x6887246668a3b87f54deb3b94ba47a6f63f32985", "0x0000000000000000000000000000000000000010", 1),
      blob("0xDAA526086787D9DEBE1D7F3FFDB1FE50CF8687F4", "0xbd0d173eeb87d57a09521c24388a12789f33ba96", 3, 7),
    ],
  };
  const applied = change("apply", 9, entry) as unknown as ChangeEnvelope<BlobsSnapshotEntry>;
  expect(toEvent("ethereum-mainnet", applied, new Map())).toMatchObject({
    type: "block", number: 9, txs: 120, gas: 0.25, blobs: { "op-mainnet": 5, "base-mainnet": 6, other: 1, "robinhood-mainnet": 3 },
  });
  // A transaction to another address is not a batch.
  expect(postings(applied).map((posting) => [posting.chain, posting.hashes.length, posting.index])).toEqual([
    ["op-mainnet", 5, 0],
    ["base-mainnet", 6, 0],
    ["robinhood-mainnet", 3, 7],
  ]);
});

test("a Nitro batch post names the messages it carries", () => {
  const word = (value: number) => value.toString(16).padStart(64, "0");
  const call = (selector: string, proof = "") =>
    `0x${selector}${word(370_623)}${word(41)}${"0".repeat(24)}${"ab".repeat(20)}${word(105_149_075)}${word(105_149_238)}${proof}`;
  const batch = { sequence: 370_623, before: 105_149_075, after: 105_149_238 };
  expect(inboxBatch(call("3e5aa082"))).toEqual(batch);
  // The delay-proof variant appends its proof to the same arguments.
  expect(inboxBatch(call("917cf8ac", word(1).repeat(8)))).toEqual(batch);
  expect(inboxBatch(call("8f111f3c"))).toBeNull();
  expect(inboxBatch(call("3e5aa082").slice(0, 200))).toBeNull();
  expect(inboxBatch(`0x3e5aa082${word(1)}${word(0)}${word(0)}${word(9)}${word(8)}`)).toBeNull();
});

test("a packed recording unpacks to the same events, earlier ones arriving at its start", () => {
  const events = [
    { type: "block", chain: "a", number: 10, hash: "", time: 100, seen: 5_000, level: "attested", txs: 3, gas: 0.25 },
    { type: "block", chain: "b", number: 7, hash: "", time: 90, seen: 6_000, level: "unattested", txs: null, gas: null },
    { type: "block", chain: "a", number: 11, hash: "", time: 112, seen: 7_000, level: "unattested", txs: 4, gas: 0.5 },
    { type: "block", chain: "b", number: 8, hash: "", time: 102, seen: 7_500, level: "unattested", txs: 90, gas: 0.5,
      blobs: { "op-mainnet": 5, other: 2 } },
    { type: "batch", chain: "a", l1: 8, l1Hash: "", l1Time: 103, at: 7_600, blobs: 5, first: 9, last: 11, firstTime: 98,
      lastTime: 102, txs: 30, bytes: 600_000, raw: 1_700_000, verified: true, source: "blobs" },
    { type: "level", chain: "b", level: "confirmed", through: 8, time: 102, at: 7_700 },
    { type: "level", chain: "a", level: "finalized", through: 10, time: 100, at: 8_000 },
    { type: "undo", chain: "a", number: 11, hash: "", at: 9_000 },
  ] as const;
  const early = { ...events[4], l1: 7, first: 6, last: 8, at: 5_500 };
  const replay = unpack(pack([early, ...events], 6_000, 8_000));
  // Events from before the window arrive at its start, keeping their own times.
  expect(replay.shift()).toEqual({ at: 6_000, event: early });
  expect(replay.map(({ at }) => at)).toEqual([6_000, 6_000, 7_000, 7_500, 7_600, 7_700, 8_000]);
  expect(replay.map(({ event }) => event)).toEqual(events.slice(0, 7));
});

test("a packed recording keeps two blocks a second of a chain, and from before its window only what the page still draws", () => {
  // A node catching up delivers a burst of blocks at once: ten of them, five per second of chain time.
  const fast: HubEvent[] = Array.from({ length: 10 }, (_, index) => ({
    type: "block", chain: "fast", number: 100 + index, hash: "", time: 3_000 + Math.floor(index / 5), seen: 3_000_000,
    level: "attested", txs: 1, gas: 0.1,
  }));
  const markers: HubEvent[] = fast.map((block) => ({
    type: "level", chain: "fast", level: "attested", through: (block as { number: number }).number, time: 3_000,
    at: 3_000_000 + ((block as { number: number }).number - 100) * 100,
  }));
  const stale: HubEvent = { type: "block", chain: "slow", number: 5, hash: "", time: 1, seen: 1_000, level: "attested", txs: 0, gas: 0 };
  const replay = unpack(pack([stale, ...fast, ...markers], 3_000_000, 3_001_000));
  const through = (type: string) =>
    replay.filter(({ event }) => event.type === type).map(({ event }) => ("number" in event ? event.number : (event as { through: number }).through));
  expect(through("block")).toEqual([100, 105]);
  expect(through("level")).toEqual([100, 105]);
});
