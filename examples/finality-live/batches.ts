// Decodes OP Stack batcher posts as the derivation spec reads them; every
// input is untrusted and bounded.
import { brotliDecompressSync, inflateSync } from "node:zlib";

export const BLOB_BYTES = 131_072;
/** 4 field elements of 31 bytes and 6 bits each, less the 4-byte header. */
const MAX_BLOB_DATA = (4 * 31 + 3) * 1_024 - 4;
const MAX_FRAME_BYTES = 1_000_000;
const MAX_CHANNEL_BYTES = 64 * 1_024 * 1_024;
const MAX_SPAN_BLOCKS = 10_000_000;

const hex = (bytes: Uint8Array) => `0x${Buffer.from(bytes).toString("hex")}`;

export function blobData(blob: Uint8Array): Uint8Array {
  if (blob.length !== BLOB_BYTES) throw new Error(`a blob has ${BLOB_BYTES} bytes, not ${blob.length}`);
  if (blob[1] !== 0) throw new Error(`unknown blob encoding version ${blob[1]}`);
  const length = (blob[2] << 16) | (blob[3] << 8) | blob[4];
  if (length > MAX_BLOB_DATA) throw new Error(`blob data of ${length} bytes exceeds ${MAX_BLOB_DATA}`);
  const output = new Uint8Array(MAX_BLOB_DATA);
  output.set(blob.subarray(5, 32), 0);
  // Each round's 4 field elements give 4 × 31 bytes and, from their high
  // bytes, 3 more, written into the gaps left after the first three.
  let at = 28;
  let element = 32;
  const high = [blob[0], 0, 0, 0];
  const next = () => {
    const byte = blob[element];
    if (byte & 0b1100_0000) throw new Error(`field element ${element / 32} is not canonical`);
    output.set(blob.subarray(element + 1, element + 32), at);
    at += 32;
    element += 32;
    return byte;
  };
  const reassemble = () => {
    at -= 1;
    output[at - 96] = (high[0] & 0x3f) | ((high[1] & 0x30) << 2);
    output[at - 64] = (high[1] & 0x0f) | ((high[3] & 0x0f) << 4);
    output[at - 32] = (high[2] & 0x3f) | ((high[3] & 0x30) << 2);
  };
  for (let index = 1; index < 4; index += 1) high[index] = next();
  reassemble();
  for (let round = 1; round < 1_024 && at < length; round += 1) {
    for (let index = 0; index < 4; index += 1) high[index] = next();
    reassemble();
  }
  if (output.subarray(length).some((byte) => byte !== 0) || blob.subarray(element).some((byte) => byte !== 0)) {
    throw new Error("blob has data past its length");
  }
  return output.subarray(0, length);
}

export interface Frame {
  channel: string;
  number: number;
  data: Uint8Array;
  last: boolean;
}

export function frames(data: Uint8Array): Frame[] {
  if (data[0] !== 0) throw new Error(`unknown derivation version ${data[0]}`);
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const found: Frame[] = [];
  let at = 1;
  while (at < data.length) {
    if (at + 23 > data.length) throw new Error("truncated frame header");
    const length = view.getUint32(at + 18);
    if (length > MAX_FRAME_BYTES || at + 23 + length > data.length) throw new Error(`frame of ${length} bytes`);
    const last = data[at + 22 + length];
    if (last > 1) throw new Error(`frame end marker ${last}`);
    found.push({
      channel: hex(data.subarray(at, at + 16)),
      number: view.getUint16(at + 16),
      data: data.subarray(at + 22, at + 22 + length),
      last: last === 1,
    });
    at += 23 + length;
  }
  return found;
}

export class Channels {
  private readonly open = new Map<string, Map<number, Frame>>();

  /** The compressed channel `frame` completes and how many frames it took, or `null`. */
  add(frame: Frame): { bytes: Uint8Array; frames: number } | null {
    const channel = this.open.get(frame.channel) ?? new Map<number, Frame>();
    this.open.delete(frame.channel);
    this.open.set(frame.channel, channel);
    channel.set(frame.number, frame);
    // The oldest of 16 open channels gives way; a batcher keeps one or two open.
    if (this.open.size > 16) this.open.delete(this.open.keys().next().value!);
    const end = [...channel.values()].find((candidate) => candidate.last);
    if (!end) return null;
    const parts: Uint8Array[] = [];
    for (let number = 0; number <= end.number; number += 1) {
      const part = channel.get(number);
      if (!part) return null;
      parts.push(part.data);
    }
    this.open.delete(frame.channel);
    const bytes = parts.reduce((total, part) => total + part.length, 0);
    if (bytes > MAX_CHANNEL_BYTES) throw new Error(`channel of ${bytes} bytes`);
    return { bytes: Buffer.concat(parts), frames: parts.length };
  }
}

/** A run of consecutive L2 blocks a channel carries: one span batch, or singular batches in a row. */
export interface Batch {
  /** Seconds after the chain's L2 genesis of the first block. */
  relTimestamp: number;
  l1Origin: number;
  /** The first 20 bytes of the first block's parent hash. */
  parentCheck: string;
  /** Transactions of each block, deposits left out. */
  transactions: number[];
}

export interface Clock {
  genesisTime: number;
  blockSeconds: number;
}

/** The batches of one compressed channel, and its decompressed size. */
export function channelBatches(channel: Uint8Array, clock: Clock): { batches: Batch[]; bytes: number } {
  const options = { maxOutputLength: MAX_CHANNEL_BYTES };
  let raw: Uint8Array;
  if (channel[0] === 1) raw = brotliDecompressSync(channel.subarray(1), options);
  else if ((channel[0] & 0x0f) === 8 || (channel[0] & 0x0f) === 15) raw = inflateSync(channel, options);
  else throw new Error(`unknown channel compression ${channel[0]}`);
  const batches: Batch[] = [];
  let at = 0;
  while (at < raw.length) {
    const item = rlpItem(raw, at);
    if (item.list) throw new Error("a channel holds RLP byte strings");
    if (raw[item.start] === 1) batches.push(spanBatch(raw.subarray(item.start + 1, item.end)));
    else if (raw[item.start] === 0) {
      const block = singularBatch(raw.subarray(item.start + 1, item.end));
      const relTimestamp = block.timestamp - clock.genesisTime;
      const previous = batches.at(-1);
      if (previous && relTimestamp === previous.relTimestamp + previous.transactions.length * clock.blockSeconds) {
        previous.transactions.push(block.transactions);
      } else {
        batches.push({ relTimestamp, l1Origin: block.l1Origin, parentCheck: block.parentCheck, transactions: [block.transactions] });
      }
    }
    at = item.end;
  }
  return { batches, bytes: raw.length };
}

function rlpItem(bytes: Uint8Array, at: number): { start: number; end: number; list: boolean } {
  const prefix = bytes[at];
  if (prefix === undefined) throw new Error("truncated batch");
  if (prefix < 0x80) return { start: at, end: at + 1, list: false };
  const list = prefix >= 0xc0;
  const short = prefix - (list ? 0xc0 : 0x80);
  let start = at + 1;
  let length = short;
  if (short > 55) {
    const size = short - 55;
    start += size;
    length = 0;
    for (let index = 0; index < size; index += 1) length = length * 256 + bytes[at + 1 + index];
  }
  if (start + length > bytes.length) throw new Error("truncated batch");
  return { start, end: start + length, list };
}

/** A singular batch, version 0: one block's parent hash, L1 origin, timestamp, and transactions. */
function singularBatch(bytes: Uint8Array) {
  const outer = rlpItem(bytes, 0);
  if (!outer.list) throw new Error("a singular batch is an RLP list");
  const fields: ReturnType<typeof rlpItem>[] = [];
  for (let at = outer.start; at < outer.end && fields.length < 5; at = fields[fields.length - 1].end) {
    fields.push(rlpItem(bytes, at));
  }
  if (fields.length < 5 || !fields[4].list) throw new Error("truncated singular batch");
  const uint = ({ start, end }: ReturnType<typeof rlpItem>) => {
    let value = 0;
    for (let index = start; index < end; index += 1) value = value * 256 + bytes[index];
    return value;
  };
  let transactions = 0;
  for (let at = fields[4].start; at < fields[4].end; at = rlpItem(bytes, at).end) transactions += 1;
  return {
    parentCheck: hex(bytes.subarray(fields[0].start, fields[0].start + 20)),
    l1Origin: uint(fields[1]),
    timestamp: uint(fields[3]),
    transactions,
  };
}

function spanBatch(bytes: Uint8Array): Batch {
  let at = 0;
  const varint = () => {
    let value = 0;
    for (let shift = 0; shift < 64; shift += 7) {
      if (at >= bytes.length) throw new Error("truncated span batch");
      const byte = bytes[at];
      at += 1;
      value += (byte & 0x7f) * 2 ** shift;
      if ((byte & 0x80) === 0) return value;
    }
    throw new Error("span batch varint overflows");
  };
  const relTimestamp = varint();
  const l1Origin = varint();
  const parentCheck = hex(bytes.subarray(at, at + 20));
  at += 40;
  const blocks = varint();
  if (blocks === 0 || blocks > MAX_SPAN_BLOCKS) throw new Error(`span batch of ${blocks} blocks`);
  at += Math.ceil(blocks / 8);
  const transactions = Array.from({ length: blocks }, varint);
  return { relTimestamp, l1Origin, parentCheck, transactions };
}
