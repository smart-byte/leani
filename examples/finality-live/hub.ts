// The finality page's hub: follows each node with the SDK, decodes the
// rollups' batches, and streams every chain to the page. Usage: README.md.
import {
  createLeaniClient,
  type BlobsSnapshotEntry,
  type BlockSummary,
  type ChangeEnvelope,
} from "@smart-byte/leani-sdk";
import { POSTERS, type HubEvent, type Level } from "../../site/src/lib/live/model.ts";
import { Channels, blobData, channelBatches, frames, type Batch as Run, type Clock } from "./batches.ts";

type Block = Extract<HubEvent, { type: "block" }>;
type Batch = Extract<HubEvent, { type: "batch" }>;

/** The hub's view of one chain, replayed to every new page. */
interface Lane {
  blocks: Block[];
  /** Recent level markers, so a new page learns when each block rose. */
  levels: Extract<HubEvent, { type: "level" }>[];
  batches: Batch[];
  connected: boolean;
}

/** About 45 minutes of a 2-second chain, which the page draws block by block. */
const RECENT_BLOCKS = 1_400;
/** About 45 minutes of Ethereum's markers, one a slot. */
const RECENT_LEVELS = 240;
/** The page draws 45 minutes: older postings a node replays are not worth their blobs. */
const DECODED_POSTING_SECONDS = 45 * 60;
/** What a replay keeps from before its window: the page draws nothing older. */
const PACKED_HISTORY_MS = DECODED_POSTING_SECONDS * 1_000;
/** A replay keeps a chain's blocks, and each level's markers, at most this often: faster chains only fill their row. */
const PACKED_SPACING_MS = 500;
const ETHEREUM = "ethereum-mainnet";
const BEACON_GENESIS = 1_606_824_023;
const OP_STACK: Record<string, OpStackClock> = {
  "op-mainnet": { genesisBlock: 105_235_063, genesisTime: 1_686_068_903, blockSeconds: 2 },
  // Denim's 200 ms blocks keep whole-second timestamps, so from then on only a batch's parent check names its blocks.
  "base-mainnet": { genesisBlock: 0, genesisTime: 1_686_789_347, blockSeconds: 2, until: 1_793_901_600 },
};

interface OpStackClock extends Clock {
  genesisBlock: number;
  until?: number;
}
/**
 * Nitro chains whose batch posts the hub reads: block `genesisBlock` + n is
 * message n, as NodeInterface's `findBatchContainingBlock` agrees at batch
 * boundaries.
 */
const NITRO: Record<string, { genesisBlock: number }> = {
  "arbitrum-one": { genesisBlock: 22_207_817 },
  "robinhood-mainnet": { genesisBlock: 0 },
};
/** SequencerInbox's blob batch posts, plain and with a delay proof; both start with the same five words. */
const INBOX_BATCH_SELECTORS = ["0x3e5aa082", "0x917cf8ac"];
const INBOX_WAIT_MS = 120_000;

export interface Posting {
  chain: string;
  l1: number;
  l1Hash: string;
  timestamp: number;
  index: number;
  hashes: string[];
}

export function toEvent(
  chain: string,
  change: ChangeEnvelope<BlockSummary | BlobsSnapshotEntry>,
  times: Map<number, number>,
): HubEvent | null {
  const at = Date.parse(change.emittedAt);
  if (change.operation === "apply" && change.block && change.data) {
    times.set(change.block.number, change.block.timestamp);
    const event: Block = {
      type: "block",
      chain,
      number: change.block.number,
      hash: change.block.hash,
      time: change.block.timestamp,
      seen: at,
      level: change.finality,
      txs: null,
      gas: null,
    };
    if ("transactions" in change.data) {
      const { block, transactions } = change.data as BlobsSnapshotEntry;
      const blobs: Record<string, number> = {};
      for (const transaction of transactions) {
        const poster = posterOf(transaction.senderAddress, transaction.toAddress) ?? "other";
        blobs[poster] = (blobs[poster] ?? 0) + transaction.blobCount;
      }
      const gasLimit = Number(block.gasLimit);
      return { ...event, txs: block.transactionCount, gas: gasLimit ? Number(block.gasUsed) / gasLimit : null, blobs };
    }
    const summary = change.data as BlockSummary;
    return {
      ...event,
      txs: summary.transactionCount,
      gas: summary.gasUsed !== null && summary.gasLimit ? summary.gasUsed / summary.gasLimit : null,
    };
  }
  if ((change.operation === "attested" || change.operation === "finalized" || change.operation === "settled") && change.data && "throughBlock" in change.data) {
    const through = change.data.throughBlock;
    return { type: "level", chain, level: change.operation, through, time: times.get(through) ?? null, at };
  }
  if (change.operation === "undo" && change.block) {
    return { type: "undo", chain, number: change.block.number, hash: change.block.hash, at };
  }
  return null;
}

const posterOf = (from: string, to: string) =>
  POSTERS.find((poster) => poster.from === from.toLowerCase() && poster.to === to.toLowerCase())?.chain;

export function postings(change: ChangeEnvelope<BlobsSnapshotEntry>): Posting[] {
  if (change.operation !== "apply" || !change.data) return [];
  const { block, transactions } = change.data;
  return transactions.flatMap((transaction) => {
    const chain = posterOf(transaction.senderAddress, transaction.toAddress);
    return chain && (OP_STACK[chain] || NITRO[chain])
      ? [{
        chain, l1: block.blockNumber, l1Hash: block.blockHash, timestamp: block.timestamp,
        index: transaction.transactionIndex, hashes: transaction.blobVersionedHashes,
      }]
      : [];
  });
}

/** Fetch `hashes`' blobs of the block at `timestamp`, of at most their size. */
async function fetchBlobs(beacon: string, timestamp: number, hashes: string[]): Promise<Uint8Array[]> {
  const slot = (timestamp - BEACON_GENESIS) / 12;
  const url = new URL(`eth/v1/beacon/blobs/${slot}`, beacon.endsWith("/") ? beacon : `${beacon}/`);
  for (const hash of hashes) url.searchParams.append("versioned_hashes", hash);
  const response = await fetch(url, { headers: { accept: "application/json" } });
  if (!response.ok || !response.body) throw new Error(`blobs of slot ${slot}: HTTP ${response.status}`);
  const limit = hashes.length * (2 * 131_072 + 8) + 4_096;
  const chunks: Uint8Array[] = [];
  let length = 0;
  for await (const chunk of response.body) {
    length += chunk.length;
    if (length > limit) throw new Error(`blobs of slot ${slot}: more than ${limit} bytes`);
    chunks.push(chunk);
  }
  const body = JSON.parse(Buffer.concat(chunks).toString()) as { data: string[] };
  if (body.data.length !== hashes.length) throw new Error(`slot ${slot}: ${body.data.length} of ${hashes.length} blobs`);
  return body.data.map((blob) => Buffer.from(blob.replace(/^0x/, ""), "hex"));
}

export interface Placed {
  first: number;
  last: number;
  firstTime: number;
  lastTime: number;
  txs: number;
  verified: boolean | null;
}

/**
 * Number a channel's runs: each starts after the block whose hash its parent
 * check begins, or, while the chain's block time holds, at the block its
 * timestamp names, checked against `hashOf` that block's parent. A single
 * block that follows the run before it joins that run.
 */
export function placeBatches(
  runs: Run[],
  clock: OpStackClock,
  numberOf: (parentCheck: string) => number | undefined,
  hashOf: (number: number) => string | undefined,
): Placed[] {
  const placed: Placed[] = [];
  for (const run of runs) {
    const firstTime = clock.genesisTime + run.relTimestamp;
    const timed = clock.until === undefined || firstTime < clock.until;
    const parent = numberOf(run.parentCheck);
    if (parent === undefined && !timed) continue;
    const first = parent === undefined ? clock.genesisBlock + Math.floor(run.relTimestamp / clock.blockSeconds) : parent + 1;
    const parentHash = parent === undefined ? hashOf(first - 1) : undefined;
    const verified = parent !== undefined ? true : parentHash === undefined ? null : parentHash.toLowerCase().startsWith(run.parentCheck);
    const last = first + run.transactions.length - 1;
    const lastTime = timed ? firstTime + (run.transactions.length - 1) * clock.blockSeconds : firstTime;
    const txs = run.transactions.reduce((total, count) => total + count, 0);
    const previous = placed.at(-1);
    if (previous && run.transactions.length === 1 && previous.verified === verified && first === previous.last + 1) {
      Object.assign(previous, { last, lastTime, txs: previous.txs + txs });
    } else {
      placed.push({ first, last, firstTime, lastTime, txs, verified });
    }
  }
  return placed;
}

/** Decode one posting at a time, so a channel's frames arrive in order. */
function batcher(
  beacon: string,
  emit: (event: HubEvent) => void,
  numberOf: (chain: string, parentCheck: string) => number | undefined,
  hashOf: (chain: string, number: number) => string | undefined,
) {
  const channels = new Map<string, Channels>();
  let queue = Promise.resolve();
  const decode = async (posting: Posting) => {
    let blobs: Uint8Array[] = [];
    for (let attempt = 0; ; attempt += 1) {
      try {
        blobs = await fetchBlobs(beacon, posting.timestamp, posting.hashes);
        break;
      } catch (error) {
        if (attempt === 2) throw error;
        await Bun.sleep(4_000);
      }
    }
    const open = channels.get(posting.chain) ?? new Channels();
    channels.set(posting.chain, open);
    const clock = OP_STACK[posting.chain];
    for (const blob of blobs) {
      for (const frame of frames(blobData(blob))) {
        const channel = open.add(frame);
        if (!channel) continue;
        const { batches, bytes } = channelBatches(channel.bytes, clock);
        const placed = placeBatches(
          batches,
          clock,
          (parentCheck) => numberOf(posting.chain, parentCheck),
          (number) => hashOf(posting.chain, number),
        );
        for (const batch of placed) {
          emit({
            type: "batch",
            chain: posting.chain,
            l1: posting.l1,
            l1Hash: posting.l1Hash,
            l1Time: posting.timestamp,
            at: Date.now(),
            blobs: channel.frames,
            ...batch,
            bytes: channel.bytes.length,
            raw: bytes,
            source: "blobs",
          });
        }
      }
    }
  };
  return (posting: Posting) => {
    queue = queue
      .then(() => decode(posting))
      .catch((error) => console.error(`${posting.chain} batch in ${posting.l1}: ${error instanceof Error ? error.message : error}`));
  };
}

/** One JSON-RPC call to `url`, of at most a megabyte back. */
async function rpc<T>(url: string, method: string, params: unknown[]): Promise<T> {
  const response = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }),
  });
  const text = await response.text();
  if (!response.ok || text.length > 1_048_576) throw new Error(`${method}: HTTP ${response.status}, ${text.length} bytes`);
  const body = JSON.parse(text) as { result?: T; error?: { message: string } };
  if (body.error || body.result === undefined) throw new Error(`${method}: ${body.error?.message ?? "no result"}`);
  return body.result;
}

/** The sequence number and message counts before and after a SequencerInbox blob batch post, or `null`. */
export function inboxBatch(input: string): { sequence: number; before: number; after: number } | null {
  if (!/^0x[0-9a-fA-F]*$/.test(input) || input.length < 10 + 64 * 5) return null;
  if (!INBOX_BATCH_SELECTORS.includes(input.slice(0, 10).toLowerCase())) return null;
  const word = (index: number) => BigInt(`0x${input.slice(10 + 64 * index, 10 + 64 * (index + 1))}`);
  const [sequence, before, after] = [word(0), word(3), word(4)];
  if (after <= before || after > BigInt(Number.MAX_SAFE_INTEGER) || sequence > BigInt(Number.MAX_SAFE_INTEGER)) return null;
  return { sequence: Number(sequence), before: Number(before), after: Number(after) };
}

/**
 * Batches of Nitro chains, read from their SequencerInbox posts, which the
 * Ethereum node's JSON-RPC serves. A batch waits until the hub holds its
 * newest block, so its times are its blocks' own.
 */
function inbox(rpcUrl: string, emit: (event: HubEvent) => void, blocksOf: (chain: string) => Block[]) {
  const waiting: { posting: Posting; first: number; last: number; since: number }[] = [];
  setInterval(() => {
    for (const entry of [...waiting]) {
      const { posting, first, last } = entry;
      const blocks = blocksOf(posting.chain);
      // A batch from before the hub's oldest block, as a replayed Ethereum block carries, has nothing to wait for.
      const before = blocks.length > 0 && blocks[0].number > last;
      if (!before && (blocks.at(-1)?.number ?? 0) < last && Date.now() - entry.since < INBOX_WAIT_MS) continue;
      waiting.splice(waiting.indexOf(entry), 1);
      const range = blocks.filter((block) => block.number >= first && block.number <= last);
      if (range.length === 0) {
        if (!before) console.error(`${posting.chain} batch in ${posting.l1}: its blocks never reached the hub`);
        continue;
      }
      emit({
        type: "batch",
        chain: posting.chain,
        l1: posting.l1,
        l1Hash: posting.l1Hash,
        l1Time: posting.timestamp,
        at: Date.now(),
        blobs: posting.hashes.length,
        first,
        last,
        firstTime: range[0].time,
        lastTime: range.at(-1)!.time,
        txs: range.reduce((total, block) => total + (block.txs ?? 0), 0),
        bytes: 0,
        raw: 0,
        verified: null,
        source: "inbox",
      });
    }
  }, 1_000);
  return async (posting: Posting) => {
    try {
      const transaction = await rpc<{ input: string } | null>(rpcUrl, "eth_getTransactionByBlockNumberAndIndex", [
        `0x${posting.l1.toString(16)}`, `0x${posting.index.toString(16)}`,
      ]);
      const batch = transaction && inboxBatch(transaction.input);
      if (!batch) return;
      const { genesisBlock } = NITRO[posting.chain];
      if (waiting.length >= 64) waiting.shift();
      waiting.push({ posting, first: genesisBlock + batch.before, last: genesisBlock + batch.after - 1, since: Date.now() });
    } catch (error) {
      console.error(`${posting.chain} batch in ${posting.l1}: ${error instanceof Error ? error.message : error}`);
    }
  };
}

async function fastConfirmations(
  beacon: string,
  numberAt: (time: number) => number | undefined,
  emit: (event: HubEvent) => void,
  signal: AbortSignal,
) {
  const url = new URL("eth/v1/events?topics=fast_confirmation", beacon.endsWith("/") ? beacon : `${beacon}/`);
  while (!signal.aborted) {
    try {
      const response = await fetch(url, { headers: { accept: "text/event-stream" }, signal });
      if (!response.ok || !response.body) throw new Error(`HTTP ${response.status}`);
      let buffered = "";
      for await (const chunk of response.body.pipeThrough(new TextDecoderStream())) {
        buffered += chunk;
        if (buffered.length > 1_048_576) throw new Error("event larger than a megabyte");
        let end: number;
        while ((end = buffered.indexOf("\n\n")) >= 0) {
          const message = buffered.slice(0, end);
          buffered = buffered.slice(end + 2);
          const data = message.split("\n").find((line) => line.startsWith("data:"))?.slice(5);
          if (!data || !message.includes("event: fast_confirmation")) continue;
          const time = BEACON_GENESIS + 12 * Number((JSON.parse(data) as { slot: string }).slot);
          const number = numberAt(time);
          if (number !== undefined) {
            emit({ type: "level", chain: ETHEREUM, level: "confirmed", through: number, time, at: Date.now() });
          }
        }
      }
    } catch (error) {
      if (signal.aborted) return;
      console.error(`fast confirmations: ${error instanceof Error ? error.message : error}`);
    }
    await Bun.sleep(3_000);
  }
}

async function follow(
  chain: string,
  url: string,
  emit: (event: HubEvent) => void,
  post: ((posting: Posting) => void) | null,
  signal: AbortSignal,
) {
  const leani = createLeaniClient({ baseUrl: url });
  const times = new Map<number, number>();
  while (!signal.aborted) {
    try {
      const options = { signal, onHello: () => emit({ type: "status", chain, connected: true, at: Date.now() }) };
      const changes = chain === ETHEREUM ? leani.blobs.subscribe(options) : leani.blocks.subscribe(options);
      for await (const change of changes) {
        const event = toEvent(chain, change, times);
        if (event) emit(event);
        if (post && chain === ETHEREUM) for (const posting of postings(change as ChangeEnvelope<BlobsSnapshotEntry>)) post(posting);
        if (times.size > 20_000) times.delete(times.keys().next().value!);
      }
    } catch (error) {
      if (signal.aborted) return;
      console.error(`${chain}: ${error instanceof Error ? error.message : error}`);
    }
    emit({ type: "status", chain, connected: false, at: Date.now() });
    await Bun.sleep(3_000);
  }
}

function serve(
  chains: Map<string, string>,
  port: number,
  record: string | null,
  beacon: string | null,
  ethereumRpc: string | null,
  fcr: string | null,
) {
  // A rollup without a node of its own still shows the batches Ethereum carries for it.
  const shown = new Set([...chains.keys(), ...(beacon ? Object.keys(OP_STACK) : [])]);
  const lanes = new Map<string, Lane>(
    [...shown].map((chain) => [chain, { blocks: [], levels: [], batches: [], connected: false }]),
  );
  // Each chain's blocks by their hash's first 20 bytes, as long as a posting
  // may still name them: a faster chain outruns the lanes' replay window.
  const parents = new Map<string, Map<string, { number: number; time: number }>>();
  const clients = new Set<ReadableStreamDefaultController<Uint8Array>>();
  const encoder = new TextEncoder();
  const frame = (name: string, data: unknown) => encoder.encode(`event: ${name}\ndata: ${JSON.stringify(data)}\n\n`);
  const recording = record ? Bun.file(record).writer() : null;
  const startedAt = Date.now();
  const emit = (event: HubEvent) => {
    const lane = lanes.get(event.chain);
    if (!lane) return;
    if (event.type === "block") {
      lane.blocks.push(event);
      if (lane.blocks.length > RECENT_BLOCKS) lane.blocks.shift();
      const index = parents.get(event.chain) ?? new Map<string, { number: number; time: number }>();
      parents.set(event.chain, index);
      index.set(event.hash.slice(0, 42).toLowerCase(), { number: event.number, time: event.time });
      for (const [check, block] of index) {
        if (block.time >= event.time - DECODED_POSTING_SECONDS) break;
        index.delete(check);
      }
    } else if (event.type === "level") {
      lane.levels.push(event);
      if (lane.levels.length > RECENT_LEVELS) lane.levels.shift();
    } else if (event.type === "status") {
      lane.connected = event.connected;
    } else if (event.type === "batch") {
      lane.batches.push(event);
      if (lane.batches.length > 16) lane.batches.shift();
    }
    recording?.write(`${JSON.stringify(event)}\n`);
    for (const client of clients) {
      try {
        client.enqueue(frame(event.type, event));
      } catch {
        clients.delete(client);
      }
    }
  };
  const hashOf = (chain: string, number: number) =>
    lanes.get(chain)?.blocks.find((block) => block.number === number)?.hash;
  const numberOf = (chain: string, parentCheck: string) => parents.get(chain)?.get(parentCheck)?.number;
  const decode = beacon ? batcher(beacon, emit, numberOf, hashOf) : null;
  const read = ethereumRpc ? inbox(ethereumRpc, emit, (chain) => lanes.get(chain)?.blocks ?? []) : null;
  // A node replays its retained window when the stream reconnects: each posting is read once.
  const seenPostings = new Set<string>();
  const post = (posting: Posting) => {
    const key = `${posting.l1Hash} ${posting.index}`;
    if (Date.now() / 1000 - posting.timestamp > DECODED_POSTING_SECONDS || seenPostings.has(key)) return;
    seenPostings.add(key);
    if (seenPostings.size > 4_096) seenPostings.delete(seenPostings.values().next().value!);
    void (OP_STACK[posting.chain] ? decode : read)?.(posting);
  };
  const stop = new AbortController();
  for (const [chain, url] of chains) void follow(chain, url, emit, post, stop.signal);
  if (fcr) {
    const numberAt = (time: number) => lanes.get(ETHEREUM)?.blocks.find((block) => block.time === time)?.number;
    void fastConfirmations(fcr, numberAt, emit, stop.signal);
  }
  setInterval(() => {
    recording?.flush();
    for (const client of clients) client.enqueue(encoder.encode(": keep-alive\n\n"));
  }, 15_000);
  Bun.serve({
    port,
    idleTimeout: 0,
    fetch(request) {
      const url = new URL(request.url);
      // A public page may read a hub on this machine: browsers ask first.
      const headers = { "access-control-allow-origin": "*", "access-control-allow-private-network": "true" };
      if (request.method === "OPTIONS") return new Response(null, { status: 204, headers });
      if (url.pathname !== "/events") return new Response("not found", { status: 404, headers });
      let controller!: ReadableStreamDefaultController<Uint8Array>;
      const body = new ReadableStream<Uint8Array>({
        start(started) {
          controller = started;
          controller.enqueue(frame("hello", { chains: [...lanes.keys()], now: Date.now(), startedAt }));
          for (const [chain, lane] of lanes) {
            controller.enqueue(frame("status", { type: "status", chain, connected: lane.connected, at: Date.now() }));
            for (const block of lane.blocks) controller.enqueue(frame("block", block));
            for (const level of lane.levels) controller.enqueue(frame("level", level));
          }
          for (const lane of lanes.values()) for (const batch of lane.batches) controller.enqueue(frame("batch", batch));
          clients.add(controller);
        },
        cancel() {
          clients.delete(controller);
        },
      });
      return new Response(body, {
        headers: { ...headers, "content-type": "text/event-stream", "cache-control": "no-cache" },
      });
    },
  });
  console.error(`hub: http://127.0.0.1:${port}/events for ${[...lanes.keys()].join(", ")}`);
}

const LEVELS: Level[] = ["unattested", "attested", "finalized", "confirmed", "settled"];
const SOURCES = [...POSTERS.map((poster) => poster.chain), "other"];

/**
 * Where a recording's rows all have their history: the latest moment from
 * which every chain has blocks and every rollup has batches. A hub that
 * connects to running nodes gets the history each kept, which starts at
 * different times.
 */
function sharedStart(events: HubEvent[]): number {
  const earliest = new Map<string, number>();
  for (const event of events) {
    const made = event.type === "block" ? event.time : event.type === "batch" ? event.firstTime : null;
    if (made === null) continue;
    const key = `${event.chain} ${event.type}`;
    earliest.set(key, Math.min(earliest.get(key) ?? Infinity, made * 1_000));
  }
  return Math.max(-Infinity, ...earliest.values());
}

/** When the chain made what `event` draws, in milliseconds. */
function made(event: HubEvent) {
  switch (event.type) {
    case "block":
      return event.time * 1_000;
    case "batch":
      return event.lastTime * 1_000;
    case "level":
      return event.time === null ? event.at : event.time * 1_000;
    default:
      return event.at;
  }
}

/**
 * A recording's events as deltas, the shape the page's replay reads. Events
 * before `from` all arrive as the replay starts, keeping their own times, so
 * it opens on the state a page connecting then would have seen, every row
 * from the same moment on; those after `to` are left out.
 */
export function pack(events: HubEvent[], from = -Infinity, to = Infinity) {
  const time = (event: HubEvent) => (event.type === "block" ? event.seen : event.at);
  const at = (event: HubEvent) => Math.max(from, time(event));
  const since = sharedStart(events);
  const history = (event: HubEvent) =>
    time(event) >= from || (time(event) >= from - PACKED_HISTORY_MS && made(event) >= since);
  const kept = new Map<string, number>();
  const sorted = events
    .filter((event) => at(event) <= to && (event.type === "status" || history(event)))
    .sort((a, b) => at(a) - at(b))
    .filter((event) => {
      if (event.type !== "block" && event.type !== "level") return true;
      // Blocks by their own time: a node catching up delivers a burst at once.
      const [key, stamp] = event.type === "block" ? [event.chain, event.time * 1_000] : [`${event.chain} ${event.level}`, event.at];
      const previous = kept.get(key);
      if (previous !== undefined && stamp - previous < PACKED_SPACING_MS) return false;
      kept.set(key, stamp);
      return true;
    });
  const chains = [...new Set(sorted.map((event) => event.chain))];
  const recordedAt = sorted.length ? at(sorted[0]) : Date.now();
  const last = new Map<number, { number: number; time: number }>();
  let previous = recordedAt;
  const packed = sorted.map((event) => {
    const index = chains.indexOf(event.chain);
    const delta = at(event) - previous;
    const lag = at(event) - time(event);
    previous = at(event);
    switch (event.type) {
      case "block": {
        const before = last.get(index) ?? { number: 0, time: 0 };
        last.set(index, { number: event.number, time: event.time });
        const blobs = event.blobs ? SOURCES.map((source) => event.blobs![source] ?? 0) : [];
        return [delta, index, 0, lag, event.number - before.number, event.time - before.time, LEVELS.indexOf(event.level),
          event.txs ?? -1, event.gas === null ? -1 : Math.round(event.gas * 1000), ...blobs];
      }
      case "level":
        return [delta, index, 1, lag, LEVELS.indexOf(event.level), event.through, event.time ?? 0];
      case "undo":
        return [delta, index, 2, lag, event.number];
      case "status":
        return [delta, index, 3, lag, event.connected ? 1 : 0];
      default:
        return [delta, index, 4, lag, event.l1, event.l1Time, event.blobs, event.first, event.last - event.first,
          event.txs, event.bytes, event.raw, event.verified === null ? -1 : Number(event.verified),
          event.l1Time - event.firstTime, event.l1Time - event.lastTime, event.source === "inbox" ? 1 : 0];
    }
  });
  return { recordedAt, durationMs: previous - recordedAt, chains, sources: SOURCES, events: packed };
}

if (import.meta.main) {
  const args = Bun.argv.slice(2);
  if (args[0] === "pack") {
    const lines = (await Bun.file(args[1]).text()).split("\n").filter(Boolean);
    const [from, to] = [args[2], args[3]].map((value) => (value ? Number(value) : undefined));
    console.log(JSON.stringify(pack(lines.map((line) => JSON.parse(line) as HubEvent), from, to)));
  } else {
    const chains = new Map<string, string>();
    let port = 8787;
    let record: string | null = null;
    let beacon: string | null = null;
    let fcr: string | null = null;
    let ethereumRpc: string | null = null;
    for (let index = 0; index < args.length; index += 1) {
      const [flag, value] = [args[index], args[index + 1]];
      if (flag === "--chain") {
        const [chain, url] = value.split("=", 2);
        chains.set(chain, url);
      } else if (flag === "--port") port = Number(value);
      else if (flag === "--record") record = value;
      else if (flag === "--beacon") beacon = value;
      else if (flag === "--fcr") fcr = value;
      else if (flag === "--ethereum-rpc") ethereumRpc = value;
      else throw new Error(`unknown argument ${flag}`);
      index += 1;
    }
    if (chains.size === 0) throw new Error("name at least one --chain NAME=URL");
    serve(chains, port, record, beacon, ethereumRpc, fcr);
  }
}
