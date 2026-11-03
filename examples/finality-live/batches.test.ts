import { expect, test } from "bun:test";
import { brotliCompressSync } from "node:zlib";
import { BLOB_BYTES, Channels, blobData, channelBatches, frames } from "./batches.ts";

function encodeBlob(data: Uint8Array): Uint8Array {
  const blob = new Uint8Array(BLOB_BYTES);
  let read = 0;
  let write = 0;
  const read1 = () => (read < data.length ? data[read++] : 0);
  const write32 = (high: number, low: Uint8Array) => {
    blob[write] = high;
    blob.set(low, write + 1);
    write += 32;
  };
  const read31 = () => {
    const low = data.slice(read, read + 31);
    read += low.length;
    const padded = new Uint8Array(31);
    padded.set(low);
    return padded;
  };
  for (let round = 0; round < 1_024 && read < data.length; round += 1) {
    let first: Uint8Array;
    if (round === 0) {
      first = new Uint8Array(31);
      first.set([0, data.length >> 16, (data.length >> 8) & 0xff, data.length & 0xff]);
      const head = data.slice(0, 27);
      first.set(head, 4);
      read = head.length;
    } else {
      first = read31();
    }
    const x = read1();
    write32(x & 0x3f, first);
    const second = read31();
    const y = read1();
    write32((y & 0x0f) | ((x & 0xc0) >> 2), second);
    const third = read31();
    const z = read1();
    write32(z & 0x3f, third);
    write32(((z & 0xc0) >> 2) | ((y & 0xf0) >> 4), read31());
  }
  return blob;
}

const uvarint = (value: number) => {
  const bytes: number[] = [];
  while (value >= 0x80) {
    bytes.push((value % 0x80) | 0x80);
    value = Math.floor(value / 0x80);
  }
  bytes.push(value);
  return bytes;
};

test("a batcher's blobs decode to the blocks of their span batch", () => {
  // A span batch's prefix and per-block counts, then bytes standing in for
  // its transactions, enough to fill many rounds of field elements.
  const noise = Uint8Array.from({ length: 6_000 }, (_, index) => (index * 7919 + (index >> 3)) & 0xff);
  const span = Uint8Array.from([
    1, ...uvarint(2_000), ...uvarint(26_333_240), ...new Array(20).fill(0xab), ...new Array(20).fill(0xcd),
    ...uvarint(3), 0b0000_0101, ...uvarint(48), ...uvarint(0), ...uvarint(130), ...noise,
  ]);
  const rlp = Uint8Array.from([0xb9, span.length >> 8, span.length & 0xff, ...span]);
  const channel = Uint8Array.from([1, ...brotliCompressSync(rlp)]);
  const id = new Array(16).fill(0x42);
  const frame = (number: number, part: Uint8Array, last: boolean) =>
    Uint8Array.from([0, ...id, number >> 8, number & 0xff, 0, 0, part.length >> 8, part.length & 0xff, ...part, last ? 1 : 0]);
  const half = channel.length >> 1;
  const blobs = [frame(0, channel.subarray(0, half), false), frame(1, channel.subarray(half), true)].map(encodeBlob);

  const channels = new Channels();
  const completed = blobs.flatMap((blob) => frames(blobData(blob))).map((part) => channels.add(part));
  expect(completed[0]).toBeNull();
  expect(completed[1]?.frames).toBe(2);
  const { batches, bytes } = channelBatches(completed[1]!.bytes, { genesisTime: 0, blockSeconds: 2 });
  expect(bytes).toBe(rlp.length);
  expect(batches).toEqual([
    { relTimestamp: 2_000, l1Origin: 26_333_240, parentCheck: `0x${"ab".repeat(20)}`, transactions: [48, 0, 130] },
  ]);
});

const rlpBytes = (bytes: number[]): number[] =>
  bytes.length === 1 && bytes[0] < 0x80 ? bytes : [...rlpLength(0x80, bytes.length), ...bytes];
const rlpList = (items: number[][]): number[] => {
  const body = items.flat();
  return [...rlpLength(0xc0, body.length), ...body];
};
const rlpLength = (offset: number, length: number): number[] => {
  if (length < 56) return [offset + length];
  const size = [];
  for (let rest = length; rest > 0; rest = Math.floor(rest / 256)) size.unshift(rest % 256);
  return [offset + 55 + size.length, ...size];
};
const rlpUint = (value: number): number[] => {
  const bytes = [];
  for (let rest = value; rest > 0; rest = Math.floor(rest / 256)) bytes.unshift(rest % 256);
  return rlpBytes(bytes);
};

test("singular batches of consecutive blocks join into one run", () => {
  const genesisTime = 1_686_789_347;
  // Version 0: [parent hash, L1 origin number, L1 origin hash, timestamp, transactions].
  const singular = (timestamp: number, transactions: number) =>
    rlpBytes([0, ...rlpList([
      rlpBytes(new Array(32).fill(0xab)), rlpUint(26_334_930), rlpBytes(new Array(32).fill(0xcd)),
      rlpUint(timestamp), rlpList(Array.from({ length: transactions }, () => rlpBytes([2, 0xee, 0xff]))),
    ])]);
  const raw = Uint8Array.from([
    ...singular(genesisTime + 100, 2), ...singular(genesisTime + 102, 0), ...singular(genesisTime + 106, 1),
  ]);
  const channel = Uint8Array.from([1, ...brotliCompressSync(raw)]);
  const { batches, bytes } = channelBatches(channel, { genesisTime, blockSeconds: 2 });
  expect(bytes).toBe(raw.length);
  const parentCheck = `0x${"ab".repeat(20)}`;
  expect(batches).toEqual([
    { relTimestamp: 100, l1Origin: 26_334_930, parentCheck, transactions: [2, 0] },
    { relTimestamp: 106, l1Origin: 26_334_930, parentCheck, transactions: [1] },
  ]);
});
