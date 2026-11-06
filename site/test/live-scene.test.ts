import { expect, test } from 'bun:test';
import type { Level } from '../src/lib/live/model';
import { applyLevel, bornAt, landingBlocks, turnsAt } from '../src/lib/live/scene';

const block = (number: number, time: number) => ({
  number, time, born: time, seen: (time + 3) * 1000, level: 'attested' as Level, levelAt: (time + 3) * 1000,
  attestedAt: (time + 3) * 1000, confirmedAt: null as number | null, finalizedAt: null as number | null, undoneAt: null,
});

test('a level first seen on connect marks older blocks without timing them', () => {
  const lane = {
    blocks: [block(100, 1_000), block(101, 1_012), block(102, 1_024), block(103, 1_036)],
    attested: 102, confirmed: 0, finalized: 0, finalizedTime: null as number | null, marked: new Set<Level>(),
    attestedAfter: [] as number[], confirmedAfter: [] as number[], finalizedAfter: [] as number[],
  };
  // The page connects an hour on: Ethereum finality first, then its first confirmation.
  applyLevel(lane, { level: 'finalized', through: 100, time: 1_000, at: 4_600_000 });
  applyLevel(lane, { level: 'confirmed', through: 101, time: 1_012, at: 4_612_000 });
  expect(lane.blocks.map((one) => one.level)).toEqual(['finalized', 'confirmed', 'attested', 'attested']);
  expect(lane.blocks[1].confirmedAt).toBeNull();
  expect(lane.confirmedAfter).toEqual([]);
  expect(lane.finalizedAfter).toEqual([]);
  applyLevel(lane, { level: 'confirmed', through: 102, time: 1_024, at: 1_036_600 });
  expect(lane.blocks[2].confirmedAt).toBe(1_036_600);
  expect(lane.confirmedAfter).toHaveLength(1);
  expect(lane.confirmedAfter[0]).toBeCloseTo(12.6);
  // Finality times when blocks became final, not when they were confirmed.
  applyLevel(lane, { level: 'finalized', through: 103, time: 1_036, at: 2_000_000 });
  expect(lane.blocks[3].finalizedAt).toBe(2_000_000);
  expect(lane.blocks[3].confirmedAt).toBeNull();
  expect(lane.blocks[1].confirmedAt).toBeNull();
  expect(lane.finalizedAfter).toHaveLength(3);
});

test("a batch lands in its carrier, and in the blocks just before that hold the rest of its blobs", () => {
  const block = (number: number, base: number) => ({ number, blobs: { "base-mainnet": base, other: 2 }, consumed: new Map<string, number>() });
  const blocks = [block(8, 3), block(9, 6), block(10, 0), block(11, 1)];
  const numbers = (landed: { number: number }[]) => landed.map((one) => one.number);
  // A channel of seven blobs: six went out a block earlier.
  expect(numbers(landingBlocks(blocks, blocks[3], "base-mainnet", 7))).toEqual([11, 9]);
  // A block whose cells another batch filled belongs to that batch.
  blocks[1].consumed.set("base-mainnet", 1);
  expect(numbers(landingBlocks(blocks, blocks[3], "base-mainnet", 7))).toEqual([11]);
  // More than four blocks back is another channel's.
  const far = [block(5, 6), block(11, 1)];
  expect(numbers(landingBlocks(far, far[1], "base-mainnet", 7))).toEqual([11]);
});

test("blocks that share a second spread across it by their place in it", () => {
  // Robinhood Chain: ten blocks a second, all stamped with the same second.
  expect(bornAt(1_000, 100, 100, 0.1)).toBe(1_000);
  expect(bornAt(1_000, 105, 100, 0.1)).toBeCloseTo(1_000.5);
  expect(bornAt(1_000, 130, 100, 0.1)).toBeCloseTo(1_000.999);
  // A chain with a second or more per block, or one not measured yet, keeps its timestamps.
  expect(bornAt(1_000, 7, 6, 2)).toBe(1_000);
  expect(bornAt(1_000, 105, 100, null)).toBe(1_000);
});

test('an Ethereum block turns upright once confirmed, and at 40 s at the latest', () => {
  const capsule = (level: Level, levelAt: number, confirmedAt: number | null) => ({
    born: 1_000, level, levelAt: levelAt * 1000, confirmedAt: confirmedAt === null ? null : confirmedAt * 1000,
  });
  // The fast confirmation rule confirmed it 13 s after its slot.
  expect(turnsAt(capsule('confirmed', 1_013, 1_013))).toBeCloseTo(13);
  // Not confirmed yet, or confirmed late.
  expect(turnsAt(capsule('attested', 1_003, null))).toBe(40);
  expect(turnsAt(capsule('confirmed', 1_090, 1_090))).toBe(40);
  // Confirmed by the first marker the page saw, which does not time it: it turns when the page learns.
  expect(turnsAt(capsule('confirmed', 1_020, null))).toBeCloseTo(20);
  // Final without a confirmation the page saw: finality comes long after 40 s.
  expect(turnsAt(capsule('finalized', 1_900, null))).toBe(40);
});
