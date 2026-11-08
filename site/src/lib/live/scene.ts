// The settlement picture: a row per rollup above Ethereum, drifting right
// with age on a log axis. A rollup's posted blocks drop as a blob into the
// Ethereum block carrying them and are final once that block is.

import { CHAINS, POSTERS, RANK, type Chain, type HubEvent, type Level } from './model';

/** What the axis spans: the realtime window, and with the days of settlement open. */
const WINDOW = 30 * 60;
const DAYS = 8 * 86400;
/** The axis is logarithmic past this many seconds, so the newest get the most room. */
const SPREAD = 8;
/** The settlement lane's bed, where every Ethereum block leaves a tick in its level's colour. */
const BED = 8;
/** An Ethereum block rides the surface for a slot, then settles into a bar on the bed over SETTLE_SECONDS. */
const SETTLE_AFTER = 12;
const SETTLE_SECONDS = 60;
const BLOB_PX = 2.5;
/** Below this width each row's name moves into a strip along its top, so the graph gets the full width. */
const NARROW = 640;
const NAME_STRIP = 16;
/** A block turns upright once confirmed, at this age at the latest, then narrows into its bar while its border fades. */
const ROTATE_AFTER = 40;
const ROTATE_SECONDS = 5;
const SHAPE_SECONDS = 15;
const BORDER_SECONDS = 20;

const phase = (age: number, from: number, seconds: number) => {
  const t = Math.min(1, Math.max(0, (age - from) / seconds));
  return t * t * (3 - 2 * t);
};
/** Blocks older than this leave the picture; their batches stay. */
const KEEP_SECONDS = 45 * 60;
const MAX_BLOCKS = 1600;
const ENTER_MS = 600;
const GATHER_MS = 900;
const DROP_MS = 1400;
/** A batch the page learns of later than this after its Ethereum block arrived gets no blob flying in. */
const LATE_MS = 6_000;
const FILL_MS = 450;
const FILL_STAGGER_MS = 70;
const FADE_MS = 700;
const SLOT_COLOR = 'rgba(154, 168, 255, 0.45)';
const OTHER_BLOBS = '#55635c';
const BLOB_ORDER = [...POSTERS.map((poster) => poster.chain), 'other'];
const ETHEREUM = 'ethereum-mainnet';
const TICKS: [number, string][] = [
  [0, 'now'], [15, '15 s'], [60, '1 min'], [300, '5 min'], [900, '15 min'], [1800, '30 min'],
  [3 * 3600, '3 h'], [86400, '1 day'], [7 * 86400, '1 week'],
];

interface Batch {
  chain: string;
  l1: number;
  l1Hash: string;
  /** Seconds: when the Ethereum block carrying it was made. */
  l1Time: number;
  at: number;
  blobs: number;
  first: number;
  last: number;
  /** Seconds: when its first and last block were made. */
  firstTime: number;
  lastTime: number;
  txs: number;
  bytes: number;
  raw: number;
  verified: boolean | null;
  source: 'blobs' | 'inbox';
}

interface Block {
  number: number;
  hash: string;
  /** Seconds: the block's own timestamp. */
  time: number;
  /** Seconds: when the block was made, spread within its second. */
  born: number;
  seen: number;
  level: Level;
  levelAt: number;
  /** Milliseconds on the feed's clock when the block reached each level. */
  attestedAt: number | null;
  confirmedAt: number | null;
  finalizedAt: number | null;
  txs: number | null;
  gas: number | null;
  blobs: Record<string, number> | null;
  batch: Batch | null;
  jitter: number;
  undoneAt: number | null;
  /** An Ethereum block's: when each rollup's batch landed in it, in ms on the feed's clock; its cells fill then. */
  consumed: Map<string, number>;
}

interface Lane {
  chain: Chain;
  blocks: Block[];
  batches: Batch[];
  attested: number;
  confirmed: number;
  finalized: number;
  /** Block time of the newest finalized block, in seconds. */
  finalizedTime: number | null;
  settled: number;
  /** Block time of the newest settled block, in seconds. */
  settledTime: number | null;
  marked: Set<Level>;
  /** The lowest block number seen with each recent timestamp. */
  seconds: Map<number, number>;
  connected: boolean;
  reorgs: number;
  seen: number[];
  attestedAfter: number[];
  confirmedAfter: number[];
  finalizedAfter: number[];
  top: number;
  height: number;
}

/** A rollup block's way to Ethereum so far: times in milliseconds on the feed's clock. */
export interface Journey {
  chain: Chain;
  number: number;
  txs: number | null;
  /** Seconds: when the block was made. */
  born: number;
  seen: number;
  attestedAt: number | null;
  batch: Batch | null;
  l1: { number: number; level: Level; confirmedAt: number | null; finalizedAt: number | null } | null;
}

export interface Passage {
  chain: Chain;
  waiting: number;
  carried: number;
  confirmed: number;
  final: number;
  /** Seconds from a block to its sequencer's signature. */
  signedAfter: number | null;
  /** Seconds from a batch's newest and oldest block to the Ethereum block carrying it. */
  postedAfter: [number, number] | null;
  /** Seconds between postings. */
  every: number | null;
  /** Seconds from the Ethereum block carrying a batch to its fast confirmation. */
  confirmedAfterPosting: number | null;
  /** Seconds from a batch's newest and oldest block to the finality of its Ethereum block. */
  finalAfter: [number, number] | null;
  /** Which of those come from Ethereum's own times, before a batch seen live reached them. */
  estimated: { confirmed: boolean; final: boolean };
  lastBatch: Batch | null;
  /** Whether the lane has blocks of its own, or only the batches Ethereum carries. */
  live: boolean;
}

export interface Hover {
  chain: Chain;
  block: { number: number; hash: string; level: Level; age: number; txs: number | null; gas: number | null; blobs: Record<string, number> | null } | null;
  followed: boolean;
  batch: Batch | null;
  x: number;
  y: number;
}

export interface SceneOptions {
  now(): number;
  onHover(hover: Hover | null): void;
  onExpand(expanded: boolean): void;
  reducedMotion: boolean;
}

const median = (values: number[]) => {
  if (values.length === 0) return null;
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.floor(sorted.length / 2)];
};

const remember = (values: number[], value: number) => {
  values.push(value);
  if (values.length > 64) values.shift();
};

export const duration = (seconds: number) => {
  if (seconds < 10) return `${seconds.toFixed(1)} s`;
  if (seconds < 90) return `${Math.round(seconds)} s`;
  if (seconds < 5400) return `${Math.round(seconds / 60)} min`;
  if (seconds < 2 * 86400) return `${(seconds / 3600).toFixed(1)} h`;
  return `${(seconds / 86400).toFixed(1)} d`;
};

/** A stable spread across a row, so a block keeps its place. */
const jitterOf = (number: number) => {
  const x = Math.sin(number * 12.9898) * 43758.5453;
  return x - Math.floor(x);
};

/** Whether a node caught up on `block`: it reached its levels late, so it times none of them. */
const caughtUp = (block: Pick<Block, 'seen' | 'time'>) => block.seen / 1000 - block.time >= 60;

const blobCount = (block: { blobs: Record<string, number> | null }) =>
  Object.values(block.blobs ?? {}).reduce((total, count) => total + count, 0);

const blobColor = (source: string) => CHAINS.find((chain) => chain.id === source)?.color ?? OTHER_BLOBS;

const cubic = (from: number, first: number, second: number, to: number, t: number) =>
  (1 - t) ** 3 * from + 3 * (1 - t) ** 2 * t * first + 3 * (1 - t) * t * t * second + t ** 3 * to;

/**
 * When a block was made, in seconds: its timestamp, spread across that second
 * by its place in it on a chain whose blocks share seconds. `first` is the
 * lowest number seen with this timestamp; `blockSeconds` the chain's block
 * time, `null` while unknown.
 */
export function bornAt(time: number, number: number, first: number, blockSeconds: number | null) {
  if (blockSeconds === null || blockSeconds >= 1) return time;
  return time + Math.min(0.999, (number - first) * blockSeconds);
}

/**
 * The Ethereum blocks a batch of `blobs` blobs of `chain` lands in: `carrier`,
 * and, while blobs remain, the blocks just before whose cells of `chain` no
 * batch filled yet, where a channel's first frames went. `blocks` are in
 * number order.
 */
export function landingBlocks<B extends { number: number; blobs: Record<string, number> | null; consumed: Map<string, number> }>(
  blocks: B[],
  carrier: B,
  chain: string,
  blobs: number,
): B[] {
  const landed = [carrier];
  let rest = blobs - (carrier.blobs?.[chain] ?? 0);
  for (let index = blocks.indexOf(carrier) - 1; index >= 0 && rest > 0 && carrier.number - blocks[index].number <= 4; index -= 1) {
    const count = blocks[index].blobs?.[chain] ?? 0;
    if (count === 0) continue;
    if (blocks[index].consumed.has(chain)) break;
    landed.push(blocks[index]);
    rest -= count;
  }
  return landed;
}

/**
 * The age in seconds at which an Ethereum block turns upright: when it was
 * confirmed, or when the page learned it was, and at ROTATE_AFTER at the latest.
 */
export function turnsAt(block: Pick<Block, 'born' | 'level' | 'levelAt' | 'confirmedAt'>) {
  const confirmed = block.confirmedAt ?? (RANK[block.level] >= RANK.confirmed ? block.levelAt : null);
  return confirmed === null ? ROTATE_AFTER : Math.min(ROTATE_AFTER, confirmed / 1000 - block.born);
}

interface LevelLane {
  blocks: Pick<Block, 'number' | 'time' | 'born' | 'seen' | 'level' | 'levelAt' | 'attestedAt' | 'confirmedAt' | 'finalizedAt' | 'undoneAt'>[];
  attested: number;
  confirmed: number;
  finalized: number;
  finalizedTime: number | null;
  settled: number;
  settledTime: number | null;
  marked: Set<Level>;
  attestedAfter: number[];
  confirmedAfter: number[];
  finalizedAfter: number[];
}

/**
 * Raise a lane's blocks through `event.through` to its level. Only blocks
 * above an earlier marker of the same level are timed: the first one a page
 * sees sweeps blocks that reached the level at some unknown time before, even
 * where a higher level's marker already raised them.
 */
export function applyLevel(lane: LevelLane, event: Omit<Extract<HubEvent, { type: 'level' }>, 'type' | 'chain'>) {
  const { through, level } = event;
  const timed = lane.marked.has(level);
  lane.marked.add(level);
  if (through <= lane[level]) return;
  lane[level] = through;
  if (RANK[level] >= RANK.confirmed) lane.attested = Math.max(lane.attested, through);
  if (RANK[level] >= RANK.finalized) lane.confirmed = Math.max(lane.confirmed, through);
  if (level === 'finalized' && event.time !== null) lane.finalizedTime = event.time;
  if (level === 'settled') {
    lane.finalized = Math.max(lane.finalized, through);
    if (event.time !== null) lane.settledTime = event.time;
  }
  for (const block of lane.blocks) {
    if (block.number > through || block.undoneAt !== null || RANK[block.level] >= RANK[level]) continue;
    const live = timed && !caughtUp(block);
    const after = Math.max(0, event.at / 1000 - block.time);
    // A higher level implies the lower ones, but not when the block reached them.
    if (timed && level === 'attested') block.attestedAt ??= event.at;
    else if (timed && level === 'confirmed') block.confirmedAt ??= event.at;
    else if (timed && level === 'finalized') block.finalizedAt = event.at;
    if (live && level !== 'settled') remember(level === 'attested' ? lane.attestedAfter : level === 'confirmed' ? lane.confirmedAfter : lane.finalizedAfter, after);
    if (level === 'finalized' && (lane.finalizedTime === null || block.number === through)) lane.finalizedTime = block.born;
    if (level === 'settled' && (lane.settledTime === null || block.number === through)) lane.settledTime = block.born;
    block.level = level;
    block.levelAt = event.at;
  }
}

export class Scene {
  private readonly context: CanvasRenderingContext2D;
  private lanes: Lane[] = [];
  private visible: Set<string>;
  private width = 0;
  private height = 0;
  private gx = 0;
  private gw = 0;
  private edge = 0;
  private axisY = 0;
  private window = WINDOW;
  private expanded = false;
  private hovered: string | null = null;
  private frontiers = new Map<string, { value: number; previous: number | null; since: number }>();
  private followed: { chain: string; number: number } | null = null;
  /** Whether the last pointer was a finger, which needs bigger targets and has no hover. */
  private touch = false;

  constructor(private readonly canvas: HTMLCanvasElement, private readonly options: SceneOptions) {
    const context = canvas.getContext('2d');
    if (!context) throw new Error('canvas 2d is unavailable');
    this.context = context;
    this.visible = new Set(CHAINS.map((chain) => chain.id));
    this.reset();
    canvas.addEventListener('pointerdown', (event) => {
      this.touch = event.pointerType === 'touch';
    });
    canvas.addEventListener('pointermove', (event) => this.pointer(event));
    canvas.addEventListener('pointerleave', () => {
      this.hovered = null;
      options.onHover(null);
    });
    canvas.addEventListener('click', (event) => this.click(event.offsetX, event.offsetY));
    new ResizeObserver(() => this.layout()).observe(canvas);
    this.layout();
    const loop = () => {
      this.draw();
      if (options.reducedMotion) window.setTimeout(() => requestAnimationFrame(loop), 500);
      else requestAnimationFrame(loop);
    };
    loop();
  }

  show(chains: string[]) {
    this.visible = new Set(chains);
    this.layout();
  }

  unfollow() {
    this.followed = null;
  }

  /** Open or fold the days of settlement. */
  expand(expanded: boolean) {
    if (this.expanded === expanded) return;
    this.expanded = expanded;
    if (this.options.reducedMotion) this.window = expanded ? DAYS : WINDOW;
    this.layout();
    this.options.onExpand(expanded);
  }

  reset() {
    this.lanes = CHAINS.map((chain) => ({
      chain, blocks: [], batches: [], attested: 0, confirmed: 0, finalized: 0, finalizedTime: null, settled: 0, settledTime: null, marked: new Set(), seconds: new Map(), connected: false,
      reorgs: 0, seen: [], attestedAfter: [], confirmedAfter: [], finalizedAfter: [], top: 0, height: 0,
    }));
    this.followed = null;
    this.frontiers.clear();
    this.layout();
  }

  summaries() {
    return this.lanes.map((lane) => {
      const head = lane.blocks.at(-1);
      const first = lane.blocks.at(-Math.min(lane.blocks.length, 400));
      const blockSeconds = head && first && head.number > first.number && head.time > first.time
        ? (head.time - first.time) / (head.number - first.number)
        : null;
      return {
        chain: lane.chain,
        connected: lane.connected,
        head: head?.number ?? lane.batches.at(-1)?.last ?? null,
        blockSeconds,
        seen: median(lane.seen),
        attested: median(lane.attestedAfter),
        confirmed: median(lane.confirmedAfter),
        finalized: median(lane.finalizedAfter),
        finalizedAge: lane.finalizedTime === null ? null : this.options.now() / 1000 - lane.finalizedTime,
        settledAge: lane.settledTime === null ? null : this.options.now() / 1000 - lane.settledTime,
        reorgs: lane.reorgs,
      };
    });
  }

  passage(chain: string): Passage | null {
    const lane = this.lane(chain);
    if (!lane || lane.chain.family === 'ethereum') return null;
    const counts = { waiting: 0, carried: 0, confirmed: 0, final: 0 };
    for (const block of lane.blocks) {
      if (block.undoneAt !== null || this.postedUnseen(lane, block)) continue;
      if (!block.batch) counts.waiting += 1;
      else {
        const level = this.carrierLevel(block.batch);
        if (level === 'finalized') counts.final += 1;
        else if (RANK[level] >= RANK.confirmed) counts.confirmed += 1;
        else counts.carried += 1;
      }
    }
    const batches = lane.batches;
    const postedNewest = batches.map((batch) => batch.l1Time - batch.lastTime);
    const postedOldest = batches.map((batch) => batch.l1Time - batch.firstTime);
    const gaps = batches.slice(1).map((batch, index) => batch.l1Time - batches[index].l1Time);
    const confirmedAfter: number[] = [];
    const finalNewest: number[] = [];
    const finalOldest: number[] = [];
    for (const batch of batches) {
      const carrier = this.carrier(batch);
      if (!carrier || caughtUp(carrier)) continue;
      if (carrier.confirmedAt) confirmedAfter.push(Math.max(0, carrier.confirmedAt / 1000 - batch.l1Time));
      if (carrier.finalizedAt) {
        finalNewest.push(carrier.finalizedAt / 1000 - batch.lastTime);
        finalOldest.push(carrier.finalizedAt / 1000 - batch.firstTime);
      }
    }
    const pair = (newest: number[], oldest: number[]): [number, number] | null =>
      newest.length ? [median(newest)!, median(oldest)!] : null;
    // Until a batch seen live reaches a level, Ethereum's own times stand in.
    const ethereum = this.lane(ETHEREUM);
    const posted = pair(postedNewest, postedOldest);
    const ethereumFinal = median(ethereum?.finalizedAfter ?? []);
    const finalAfter = pair(finalNewest, finalOldest);
    return {
      chain: lane.chain,
      ...counts,
      signedAfter: median(lane.attestedAfter),
      postedAfter: posted,
      every: median(gaps),
      confirmedAfterPosting: median(confirmedAfter) ?? median(ethereum?.confirmedAfter ?? []),
      finalAfter: finalAfter ?? (posted && ethereumFinal !== null ? [posted[0] + ethereumFinal, posted[1] + ethereumFinal] : null),
      estimated: { confirmed: confirmedAfter.length === 0, final: finalAfter === null },
      lastBatch: batches.at(-1) ?? null,
      live: lane.blocks.length > 0,
    };
  }

  /** The journey of the block a click follows, `null` while none is followed or it left the picture. */
  journey(): Journey | null {
    const lane = this.followed && this.lane(this.followed.chain);
    const block = lane && lane.blocks.find((candidate) => candidate.number === this.followed!.number);
    if (!lane || !block) return null;
    const carrier = block.batch ? this.carrier(block.batch) : null;
    return {
      chain: lane.chain,
      number: block.number,
      txs: block.txs,
      born: block.born,
      seen: block.seen,
      attestedAt: block.attestedAt,
      batch: block.batch,
      l1: block.batch
        ? {
          number: block.batch.l1,
          level: this.carrierLevel(block.batch),
          confirmedAt: carrier?.confirmedAt ?? null,
          finalizedAt: carrier?.finalizedAt ?? null,
        }
        : null,
    };
  }

  handle(event: HubEvent) {
    const lane = this.lane(event.chain);
    if (!lane) return;
    switch (event.type) {
      case 'status':
        lane.connected = event.connected;
        return;
      case 'block': {
        lane.connected = true;
        const seen = event.seen / 1000;
        lane.seconds.set(event.time, Math.min(lane.seconds.get(event.time) ?? event.number, event.number));
        if (lane.seconds.size > 64) lane.seconds.delete(lane.seconds.keys().next().value!);
        const oldest = lane.blocks[0];
        const newest = lane.blocks.at(-1);
        const blockSeconds = oldest && newest && newest.number > oldest.number && newest.time > oldest.time
          ? (newest.time - oldest.time) / (newest.number - oldest.number)
          : null;
        const born = bornAt(event.time, event.number, lane.seconds.get(event.time)!, blockSeconds);
        const block: Block = {
          number: event.number, hash: event.hash, time: event.time, born, seen: event.seen, level: event.level,
          levelAt: event.seen, attestedAt: RANK[event.level] >= RANK.attested ? event.seen : null,
          confirmedAt: RANK[event.level] >= RANK.confirmed ? event.seen : null,
          finalizedAt: RANK[event.level] >= RANK.finalized ? event.seen : null, txs: event.txs, gas: event.gas,
          blobs: event.blobs ?? null, batch: null, jitter: jitterOf(event.number), undoneAt: null, consumed: new Map(),
        };
        if (event.number <= lane.settled) block.level = 'settled';
        else if (event.number <= lane.finalized && RANK[block.level] < RANK.finalized) block.level = 'finalized';
        else if (event.number <= lane.confirmed && RANK[block.level] < RANK.confirmed) block.level = 'confirmed';
        else if (event.number <= lane.attested && block.level === 'unattested') block.level = 'attested';
        block.batch = lane.batches.find((batch) => batch.first <= event.number && event.number <= batch.last) ?? null;
        const index = lane.blocks.findIndex((existing) => existing.number >= event.number);
        if (index === -1) lane.blocks.push(block);
        else if (lane.blocks[index].number === event.number) lane.blocks[index] = block;
        else lane.blocks.splice(index, 0, block);
        if (seen - event.time < 120) remember(lane.seen, Math.max(0, seen - event.time));
        if (event.level !== 'unattested' && seen - event.time < 120) remember(lane.attestedAfter, Math.max(0, seen - event.time));
        if (lane.blocks.length > MAX_BLOCKS) lane.blocks.splice(0, lane.blocks.length - MAX_BLOCKS);
        if (lane.chain.id === ETHEREUM) {
          for (const rollup of this.lanes) for (const batch of rollup.batches) if (this.carrier(batch) === block) this.consume(block, batch);
        }
        return;
      }
      case 'level':
        applyLevel(lane, event);
        return;
      case 'undo': {
        const block = lane.blocks.find((candidate) => candidate.number === event.number && (!event.hash || candidate.hash === event.hash));
        if (block && block.undoneAt === null) {
          block.undoneAt = event.at;
          lane.reorgs += 1;
        }
        return;
      }
      case 'batch': {
        if (lane.batches.some((existing) => existing.first === event.first && existing.l1 === event.l1)) return;
        const batch: Batch = { ...event };
        lane.batches.push(batch);
        lane.batches.sort((a, b) => a.first - b.first);
        if (lane.batches.length > 120) lane.batches.shift();
        for (const block of lane.blocks) if (block.number >= batch.first && block.number <= batch.last) block.batch = batch;
        const carrier = this.carrier(batch);
        if (carrier) this.consume(carrier, batch);
      }
    }
  }

  private lane(chain: string) {
    return this.lanes.find((candidate) => candidate.chain.id === chain);
  }

  private rollups() {
    return this.lanes.filter((lane) => lane.chain.family !== 'ethereum' && this.visible.has(lane.chain.id));
  }

  /**
   * Whether `block` went to Ethereum in a batch the page never saw: batches
   * post in order, so every block below the oldest one the page has was
   * posted before it.
   */
  private postedUnseen(lane: Lane, block: Block) {
    return !block.batch && lane.batches.length > 0 && block.number < lane.batches[0].first;
  }

  /** How long a lane's posted blocks gather before their blob flies: rows without blocks have none to gather. */
  private gather(lane: Lane) {
    return lane.blocks.length > 0 && !this.options.reducedMotion ? GATHER_MS : 0;
  }

  /** Fill `batch`'s cells once its blob lands, at once when no blob flies in. */
  private consume(carrier: Block, batch: Batch) {
    const lands = this.late(batch) || this.options.reducedMotion ? batch.at : batch.at + this.gather(this.lane(batch.chain)!) + DROP_MS;
    for (const block of landingBlocks(this.lane(ETHEREUM)!.blocks, carrier, batch.chain, batch.blobs)) {
      block.consumed.set(batch.chain, Math.min(block.consumed.get(batch.chain) ?? Infinity, lands));
    }
  }

  /** Whether the page learned of `batch` well after its Ethereum block arrived, as after a hub restart. */
  private late(batch: Batch) {
    return batch.at - (this.carrier(batch)?.seen ?? batch.l1Time * 1000) > LATE_MS;
  }

  private carrier(batch: Batch) {
    return this.lane(ETHEREUM)?.blocks.find(
      (block) => block.number === batch.l1 && (!block.hash || !batch.l1Hash || block.hash === batch.l1Hash),
    ) ?? null;
  }

  private carrierLevel(batch: Batch): Level {
    const ethereum = this.lane(ETHEREUM);
    const carrier = this.carrier(batch);
    if (carrier) return carrier.level;
    if (ethereum && batch.l1 <= ethereum.finalized) return 'finalized';
    if (ethereum && batch.l1 <= ethereum.confirmed) return 'confirmed';
    if (ethereum && batch.l1 <= ethereum.attested) return 'attested';
    return 'unattested';
  }

  private layout() {
    const ratio = window.devicePixelRatio || 1;
    const rect = this.canvas.getBoundingClientRect();
    this.width = rect.width;
    this.height = rect.height;
    this.canvas.width = Math.round(rect.width * ratio);
    this.canvas.height = Math.round(rect.height * ratio);
    this.context.setTransform(ratio, 0, 0, ratio, 0, 0);
    const narrow = this.narrow;
    this.gx = narrow ? 8 : 132;
    // A narrow canvas has no room for the days edge; the legend's button opens them.
    this.edge = narrow ? 8 : this.expanded ? 16 : 132;
    this.gw = Math.max(120, this.width - this.gx - this.edge);
    const rollups = this.rollups();
    const laneHeight = narrow ? 76 : 88;
    const ethereumHeight = narrow ? 112 : 116;
    // Room between the rows and Ethereum for its frontiers' labels.
    const gap = rollups.length ? 14 : 0;
    const wanted = 12 + rollups.length * laneHeight + gap + ethereumHeight + 36;
    // The canvas grows and shrinks with its lanes; the resize lays it out again.
    if (Math.abs(rect.height - wanted) > 1) {
      this.canvas.style.height = `${wanted}px`;
      return;
    }
    this.axisY = this.height - 22;
    let y = 12;
    for (const lane of this.lanes) lane.height = 0;
    for (const lane of rollups) {
      lane.top = y;
      lane.height = laneHeight - 6;
      y += laneHeight;
    }
    const ethereum = this.lane(ETHEREUM)!;
    ethereum.top = y + gap;
    ethereum.height = ethereumHeight;
  }

  private get narrow() {
    return this.width < NARROW;
  }

  /** How far a row's drawing starts below its top: past its name strip on a narrow canvas. */
  private get strip() {
    return this.narrow ? NAME_STRIP : 0;
  }

  private share(age: number) {
    return Math.min(1, Math.log1p(Math.max(0, age) / SPREAD) / Math.log1p(this.window / SPREAD));
  }

  private x(age: number) {
    return this.gx + this.gw * this.share(age);
  }

  private carries(block: Block) {
    return Object.entries(block.blobs ?? {}).some(([source, count]) => count > 0 && this.visible.has(source));
  }

  /**
   * An Ethereum block with `blobs` blobs at `age`: a capsule on the surface
   * that sinks, slower and slower, turns upright at `turns`, and narrows into
   * a bar standing on the bed, a slot wide and as tall as its blobs, while its
   * border fades. `length` and `thickness` are the slab's own, along its
   * cells and across them; `width` and `height` are on screen.
   */
  private slab(age: number, blobs: number, turns = ROTATE_AFTER) {
    const ethereum = this.lane(ETHEREUM)!;
    const capsuleLength = this.narrow ? 32 : 46;
    const capsuleThickness = this.narrow ? 12 : 16;
    const bed = ethereum.top + ethereum.height - 4 - BED;
    // A slot's width on the log axis at this age, so settled bars stand side by side.
    const barWidth = Math.min(10, Math.max(1.5, this.x(age + 6) - this.x(age - 6) - 1));
    const t = Math.min(1, Math.max(0, (age - SETTLE_AFTER) / SETTLE_SECONDS));
    const settle = 1 - (1 - t) ** 3;
    const angle = (Math.PI / 2) * phase(age, turns, ROTATE_SECONDS);
    const shape = phase(age, turns + ROTATE_SECONDS, SHAPE_SECONDS);
    const length = capsuleLength + (blobs * BLOB_PX - capsuleLength) * shape;
    const thickness = capsuleThickness + (barWidth - capsuleThickness) * shape;
    const width = Math.abs(Math.cos(angle)) * length + Math.abs(Math.sin(angle)) * thickness;
    const height = Math.abs(Math.cos(angle)) * thickness + Math.abs(Math.sin(angle)) * length;
    const surface = ethereum.top + this.strip + 6 + capsuleThickness / 2;
    const slots = Math.max(blobs, 9) - (Math.max(blobs, 9) - blobs) * shape;
    return {
      x: this.x(age),
      y: surface + (bed - 1 - height / 2 - surface) * settle,
      width,
      height,
      length,
      thickness,
      angle,
      /** How much of its border, and of its empty slots' outlines, remains. */
      border: 1 - phase(age, turns + ROTATE_SECONDS, BORDER_SECONDS),
      /** How many blob places it spans: at least nine on a capsule, its blobs on a bar. */
      slots,
      settle,
      bed,
    };
  }

  private capsule(block: Block, now: number) {
    return this.slab(now - block.born, blobCount(block), turnsAt(block));
  }

  private blockPoint(lane: Lane, block: Block, now: number) {
    return { x: this.x(now - block.born), y: lane.top + this.strip + 8 + (lane.height - this.strip - 16) * block.jitter };
  }

  private bracket(lane: Lane, batch: Batch, now: number) {
    return { from: this.x(now - batch.lastTime), to: this.x(now - batch.firstTime), y: lane.top + (lane.height + this.strip) / 2 };
  }

  private regionAt(y: number) {
    const ethereum = this.lane(ETHEREUM)!;
    if (y >= ethereum.top - 10) return ethereum;
    return this.rollups().find((lane) => y >= lane.top - 4 && y <= lane.top + lane.height + 4) ?? null;
  }

  private found(lane: Lane, x: number, y: number) {
    const now = this.options.now() / 1000;
    if (lane.chain.id === ETHEREUM) {
      let best: { block: Block; distance: number } | null = null;
      for (const block of lane.blocks) {
        const slab = this.capsule(block, now);
        const distance = Math.abs(slab.x - x);
        const inside = y >= slab.y - slab.height / 2 - 4 && y <= slab.bed + BED + 2;
        if (inside && distance < Math.max(this.touch ? 12 : 3, slab.width / 2 + 1) && (!best || distance < best.distance)) best = { block, distance };
      }
      return { block: best?.block ?? null, batch: null };
    }
    let best: { block: Block; distance: number } | null = null;
    for (const block of lane.blocks) {
      if (block.batch || this.postedUnseen(lane, block)) continue;
      const point = this.blockPoint(lane, block, now);
      const distance = Math.hypot(point.x - x, point.y - y);
      if (distance < (this.touch ? 22 : 10) && (!best || distance < best.distance)) best = { block, distance };
    }
    if (best) return { block: best.block, batch: null };
    const batch = lane.batches.find((candidate) => {
      const bracket = this.bracket(lane, candidate, now);
      return Math.abs(y - bracket.y) < (this.touch ? 16 : 9) && x >= bracket.from - 3 && x <= bracket.to + 3;
    }) ?? null;
    return { block: null, batch };
  }

  private click(x: number, y: number) {
    this.followed = null;
    if (!this.narrow && !this.expanded && x > this.width - this.edge) {
      this.expand(true);
      return;
    }
    if (!this.narrow && this.expanded && x > this.width - 110 && y < 30) {
      this.expand(false);
      return;
    }
    const lane = this.regionAt(y);
    const block = lane && lane.chain.id !== ETHEREUM ? this.found(lane, x, y).block : null;
    if (lane && block) this.followed = { chain: lane.chain.id, number: block.number };
    // A finger has no hover: its tap shows what it hit until the next one.
    if (this.touch) this.hover(x, y);
  }

  private pointer(event: PointerEvent) {
    this.canvas.style.cursor = !this.narrow && !this.expanded && event.offsetX > this.width - this.edge ? 'pointer' : 'crosshair';
    this.hover(event.offsetX, event.offsetY);
  }

  private hover(x: number, y: number) {
    const lane = this.regionAt(y);
    this.hovered = lane?.chain.id ?? null;
    if (!lane) {
      this.options.onHover(null);
      return;
    }
    const { block, batch } = this.found(lane, x, y);
    if (!block && !batch) {
      this.options.onHover(null);
      return;
    }
    const now = this.options.now() / 1000;
    this.options.onHover({
      chain: lane.chain,
      block: block && {
        number: block.number, hash: block.hash, level: block.level, age: now - block.born,
        txs: block.txs, gas: block.gas, blobs: block.blobs,
      },
      followed: block !== null && this.followed?.chain === lane.chain.id && this.followed.number === block.number,
      batch: batch ?? block?.batch ?? null,
      x,
      y,
    });
  }

  private draw() {
    const context = this.context;
    const now = this.options.now() / 1000;
    const clock = performance.now();
    if (!this.options.reducedMotion) {
      const target = this.expanded ? DAYS : WINDOW;
      const step = (Math.log(target) - Math.log(this.window)) * 0.12;
      this.window = Math.abs(step) < 0.002 ? target : Math.exp(Math.log(this.window) + step);
    }
    context.clearRect(0, 0, this.width, this.height);
    context.fillStyle = '#0b0f0d';
    context.fillRect(0, 0, this.width, this.height);
    this.drawGrid();
    const ethereum = this.lane(ETHEREUM)!;
    for (const lane of this.rollups()) this.drawRow(lane);
    this.drawBand(ethereum);
    for (const lane of this.rollups()) this.drawBatches(lane, now, clock);
    for (const lane of this.rollups()) this.drawBlocks(lane, now, clock);
    this.drawCapsules(ethereum, now, clock);
    for (const lane of this.rollups()) this.drawDrops(lane, now, clock);
    this.drawFrontiers(now);
    this.drawFollowed(now);
    this.drawEdge(now);
    this.drawAxis();
    this.drawLabels();
  }

  private drawGrid() {
    const context = this.context;
    for (const [age] of TICKS) {
      if (age > this.window * 1.001) continue;
      const x = Math.round(this.x(age)) + 0.5;
      context.strokeStyle = age === 0 ? 'rgba(62, 224, 127, 0.3)' : 'rgba(143, 168, 154, 0.08)';
      context.lineWidth = 1;
      context.beginPath();
      context.moveTo(x, 8);
      context.lineTo(x, this.axisY - 8);
      context.stroke();
    }
  }

  private drawAxis() {
    const context = this.context;
    context.font = '500 10px "JetBrains Mono", ui-monospace, monospace';
    context.textBaseline = 'middle';
    context.textAlign = 'center';
    let previous = -Infinity;
    for (const [age, label] of TICKS) {
      if (age > this.window * 1.001) continue;
      const x = Math.min(Math.max(this.gx + 12, this.x(age)), this.width - context.measureText(label).width / 2 - 4);
      if (x - previous < 44) continue;
      previous = x;
      context.fillStyle = age === 0 ? '#3ee07f' : '#8fa89a';
      context.fillText(label, x, this.axisY + 8);
    }
  }

  /** A row's name and `lines` under it, or on a narrow canvas its name and first line in the strip along its top. */
  private label(lane: Lane, title: string, lines: string[], color: string) {
    const context = this.context;
    const narrow = this.narrow;
    const x = narrow ? this.gx + 2 : 12;
    const y = narrow ? lane.top + NAME_STRIP / 2 + 1 : lane.top + Math.min(18, lane.height / 2);
    context.fillStyle = color;
    context.fillRect(x, y - 6, 3, 12);
    context.textAlign = 'left';
    context.textBaseline = 'middle';
    context.fillStyle = lane.connected || lane.batches.length ? '#e6f0ea' : '#536b5e';
    context.font = `650 ${narrow ? 10.5 : 12}px "Inter Variable", system-ui, sans-serif`;
    if (narrow) {
      const after = x + 14 + context.measureText(title).width;
      const name = context.fillStyle;
      context.font = '400 9.5px "JetBrains Mono", ui-monospace, monospace';
      context.fillStyle = 'rgba(11, 15, 13, 0.8)';
      context.fillRect(x + 5, y - 7, after - x - 1 + context.measureText(lines[0]).width, 14);
      context.fillStyle = '#8fa89a';
      context.fillText(lines[0], after, y);
      context.font = '650 10.5px "Inter Variable", system-ui, sans-serif';
      context.fillStyle = name;
      context.fillText(title, x + 8, y);
      return;
    }
    context.fillText(title, x + 9, y, this.gx - x - 14);
    context.font = '400 10px "JetBrains Mono", ui-monospace, monospace';
    context.fillStyle = '#8fa89a';
    lines.forEach((line, index) => context.fillText(line, x + 9, y + 15 + index * 13, this.gx - x - 14));
  }

  private drawRow(lane: Lane) {
    const context = this.context;
    context.fillStyle = this.hovered === lane.chain.id ? 'rgba(230, 240, 234, 0.035)' : 'rgba(230, 240, 234, 0.014)';
    context.beginPath();
    context.roundRect(this.gx - 6, lane.top, this.gw + 12, lane.height, 6);
    context.fill();
    if (lane.blocks.length === 0 && lane.batches.length && lane.height > 40) {
      context.font = '400 10px "Inter Variable", system-ui, sans-serif';
      context.fillStyle = '#536b5e';
      context.textAlign = 'left';
      context.textBaseline = 'middle';
      const note = this.narrow ? 'its blocks as Ethereum carries them' : 'its blocks as Ethereum carries them: live gossip needs a public host';
      context.fillText(note, this.gx + 6, lane.top + this.strip + 10, this.gw - 12);
    }
  }

  private drawBand(lane: Lane) {
    const context = this.context;
    context.fillStyle = 'rgba(154, 168, 255, 0.03)';
    context.beginPath();
    context.roundRect(this.gx - 6, lane.top, this.gw + 12, lane.height, 8);
    context.fill();
    context.strokeStyle = 'rgba(154, 168, 255, 0.18)';
    context.lineWidth = 1;
    context.stroke();
    context.fillStyle = 'rgba(154, 168, 255, 0.06)';
    context.fillRect(this.gx - 6, lane.top + lane.height - 4 - BED, this.gw + 12, BED);
  }

  /** Each row's name and head, drawn last so the threads and frontiers crossing a narrow canvas's strips pass behind them. */
  private drawLabels() {
    for (const lane of [...this.rollups(), this.lane(ETHEREUM)!]) {
      const head = lane.blocks.at(-1)?.number ?? lane.batches.at(-1)?.last;
      const lines = [head ? `#${head.toLocaleString('en-US')}` : lane.connected ? '…' : 'offline'];
      if (lane.chain.id === ETHEREUM) lines.push('settlement layer');
      else if (lane.height > 54) lines.push(lane.chain.family === 'arbitrum' ? 'feed-signed' : 'sequencer-signed');
      this.label(lane, lane.chain.label, lines, lane.chain.color);
    }
  }

  private drawCapsules(lane: Lane, now: number, clock: number) {
    const context = this.context;
    lane.blocks = lane.blocks.filter((block) => now - block.born < KEEP_SECONDS
      && (block.undoneAt === null || clock - this.performanceOf(block.undoneAt) < 1400));
    for (const block of lane.blocks) {
      const age = now - block.born;
      if (age > this.window) continue;
      const capsule = this.capsule(block, now);
      let { x } = capsule;
      // Blocks no shown rollup posted in fade as they settle.
      let alpha = this.carries(block) ? 1 : 1 - 0.78 * capsule.settle;
      const since = clock - this.performanceOf(block.seen);
      if (!this.options.reducedMotion && since >= 0 && since < ENTER_MS) {
        const t = 1 - (1 - since / ENTER_MS) ** 3;
        x = this.gx - 30 + (x - this.gx + 30) * t;
        alpha *= t;
      }
      if (block.undoneAt !== null) {
        this.drawUndo(x, capsule.y, Math.max(8, capsule.height), clock - this.performanceOf(block.undoneAt));
        continue;
      }
      this.drawTick(x, capsule.bed, Math.max(1.5, Math.min(capsule.width, 10)), block.level);
      context.globalAlpha = alpha;
      if (capsule.length >= 1) this.drawCapsule(x, capsule.y, capsule, block, clock);
      context.globalAlpha = 1;
    }
  }

  private drawTick(x: number, top: number, width: number, level: Level) {
    const context = this.context;
    context.fillStyle = RANK[level] >= RANK.finalized ? '#3ee07fb3' : level === 'confirmed' ? '#d6dbff8c' : level === 'attested' ? '#9aa8ff59' : '#9aa8ff2e';
    context.fillRect(x - width / 2, top + 1, width, BED - 2);
  }

  /**
   * An Ethereum block: a slab whose cells are its blobs, grey for others'
   * and filled with a rollup's colour once its batch landed. Drawn in its
   * own frame: turned upright, its first cell is at the bed.
   */
  private drawCapsule(x: number, y: number, slab: ReturnType<Scene['slab']>, block: Block, clock: number) {
    const context = this.context;
    const { length, thickness, angle, border, slots } = slab;
    const level = block.level;
    const blobs = block.blobs ?? {};
    const cells = BLOB_ORDER.flatMap((source) => Array.from({ length: blobs[source] ?? 0 }, (_, nth) => ({ source, nth })));
    const now = this.options.now();
    // How full a cell is: a shown rollup's blob fills once its batch lands in this block.
    const fill = ({ source, nth }: { source: string; nth: number }) => {
      const lands = block.consumed.get(source);
      return lands === undefined ? 0 : Math.min(1, Math.max(0, (now - lands - nth * FILL_STAGGER_MS) / FILL_MS));
    };
    const alpha = context.globalAlpha;
    context.save();
    context.translate(x, y);
    context.rotate(-angle);
    const left = -length / 2;
    const top = -thickness / 2;
    if (border > 0) {
      context.globalAlpha = alpha * border;
      context.beginPath();
      context.roundRect(left, top, length, thickness, Math.min(4, thickness / 3));
      if (RANK[level] >= RANK.finalized) {
        context.fillStyle = '#2b3c33';
        context.fill();
        context.strokeStyle = '#3ee07f';
        context.lineWidth = 1.1;
      } else {
        context.fillStyle = level === 'confirmed' ? '#9aa8ff4d' : level === 'attested' ? '#9aa8ff2a' : '#9aa8ff10';
        context.fill();
        context.strokeStyle = level === 'confirmed' ? '#d6dbff' : '#9aa8ff';
        context.lineWidth = level === 'confirmed' ? 1.6 : 1.1;
      }
      context.stroke();
    }
    const pad = 2 * border;
    const cell = (length - 2 * pad) / Math.max(1, slots);
    const cellLength = Math.max(0.6, cell - (cell > 2 ? 0.6 : 0));
    const cellThickness = Math.max(0.6, thickness - 2 * pad);
    // A final capsule dims its cells; a bar shows them whole.
    const cellAlpha = alpha * (RANK[level] >= RANK.finalized ? 1 - 0.45 * border : 1);
    cells.forEach((blob, index) => {
      const cellLeft = left + pad + index * cell;
      context.globalAlpha = cellAlpha;
      if (blob.source === 'other') {
        context.fillStyle = OTHER_BLOBS;
        context.fillRect(cellLeft, top + pad, cellLength, cellThickness);
        return;
      }
      const full = fill(blob);
      if (full < 1) {
        // An empty slot: outlined on a capsule, a faint mark on a bar.
        context.globalAlpha = cellAlpha * border;
        context.strokeStyle = SLOT_COLOR;
        context.lineWidth = 0.8;
        context.strokeRect(cellLeft + 0.4, top + pad + 0.4, cellLength - 0.8, cellThickness - 0.8);
        context.globalAlpha = cellAlpha * 0.35 * (1 - border);
        context.fillStyle = SLOT_COLOR;
        context.fillRect(cellLeft, top + pad, cellLength, cellThickness);
      }
      if (full > 0) {
        context.globalAlpha = cellAlpha;
        context.fillStyle = blobColor(blob.source);
        context.fillRect(cellLeft, top + pad + cellThickness * (1 - full), cellLength, cellThickness * full);
      }
    });
    context.restore();
    context.globalAlpha = alpha;
    this.flash(x, y, slab.width, slab.height, block, RANK[level] >= RANK.finalized ? '#3ee07f' : '#c9d0ff', clock);
  }

  private drawBlocks(lane: Lane, now: number, clock: number) {
    const context = this.context;
    lane.blocks = lane.blocks.filter((block) => now - block.born < KEEP_SECONDS
      && (block.undoneAt === null || clock - this.performanceOf(block.undoneAt) < 1400));
    const base = Math.min(lane.height * 0.16, 9);
    for (const block of lane.blocks) {
      const age = now - block.born;
      if (age > this.window || this.postedUnseen(lane, block)) continue;
      let { x, y } = this.blockPoint(lane, block, now);
      let alpha = 1;
      let scale = 1;
      if (block.batch) {
        const since = clock - this.performanceOf(block.batch.at);
        if (since >= GATHER_MS || this.options.reducedMotion || since < 0) continue;
        const bracket = this.bracket(lane, block.batch, now);
        const t = (1 - Math.cos((since / GATHER_MS) * Math.PI)) / 2;
        x += (bracket.from - x) * t;
        y += (bracket.y - y) * t;
        alpha = 1 - t * 0.5;
        scale = 1 - t * 0.7;
      }
      const since = clock - this.performanceOf(block.seen);
      if (!this.options.reducedMotion && since >= 0 && since < ENTER_MS) {
        const t = 1 - (1 - since / ENTER_MS) ** 3;
        x = this.gx - 12 + (x - this.gx + 12) * t;
        alpha = Math.min(alpha, t);
      }
      const size = base * scale * (1 - 0.5 * this.share(age)) * (0.6 + 0.4 * Math.min(1, block.gas ?? 0.5));
      context.globalAlpha = alpha;
      if (block.undoneAt !== null) this.drawUndo(x, y, size, clock - this.performanceOf(block.undoneAt));
      else {
        context.fillStyle = block.level === 'unattested' ? `${lane.chain.color}55` : lane.chain.color;
        context.fillRect(x - size / 2, y - size / 2, size, size);
      }
      context.globalAlpha = 1;
    }
  }

  private drawBatches(lane: Lane, now: number, clock: number) {
    const context = this.context;
    for (const batch of lane.batches) {
      const since = clock - this.performanceOf(batch.at);
      const gathers = lane.blocks.length > 0 && !this.options.reducedMotion;
      if (gathers && since < GATHER_MS) continue;
      const bracket = this.bracket(lane, batch, now);
      if (bracket.from > this.gx + this.gw) continue;
      const level = this.carrierLevel(batch);
      const appear = gathers ? Math.min(1, Math.max(0, since - GATHER_MS) / 500) : 1;
      const to = Math.min(bracket.to, this.gx + this.gw);
      context.fillStyle = RANK[level] >= RANK.finalized ? '#3ee07f' : lane.chain.color;
      context.globalAlpha = appear * (RANK[level] >= RANK.finalized ? 0.75 : RANK[level] >= RANK.confirmed ? 0.9 : 0.5);
      context.fillRect(bracket.from, bracket.y - 2, Math.max(2, to - bracket.from), 4);
      context.globalAlpha = appear;
      const width = to - bracket.from;
      context.font = '500 9px "JetBrains Mono", ui-monospace, monospace';
      const blocks = `${(batch.last - batch.first + 1).toLocaleString('en-US')} blocks`;
      const text = [`${blocks} → #${batch.l1.toLocaleString('en-US')}`, blocks].find((candidate) => context.measureText(candidate).width < width - 4);
      if (text && lane.height > 40) {
        context.textAlign = 'left';
        context.textBaseline = 'bottom';
        context.fillStyle = RANK[level] >= RANK.finalized ? '#9ff0c4' : '#c9d6cf';
        context.fillText(text, bracket.from + 2, bracket.y - 4);
      }
      const carrier = this.carrier(batch);
      const age = carrier ? now - carrier.born : now - batch.l1Time;
      const dropped = this.options.reducedMotion || this.late(batch) || since > this.gather(lane) + DROP_MS;
      if (dropped && age <= this.window) {
        const capsule = carrier ? this.capsule(carrier, now) : this.slab(age, batch.blobs);
        const highlight = this.hovered === lane.chain.id;
        context.strokeStyle = RANK[level] >= RANK.finalized ? '#3ee07f' : lane.chain.color;
        // Older threads step back, so the newest postings lead.
        const recent = 1 - 0.75 * this.share(age);
        context.globalAlpha = appear * (highlight ? 0.75 : (RANK[level] >= RANK.finalized ? 0.3 : 0.45) * recent);
        context.lineWidth = highlight ? 1.4 : 1;
        if (level === 'unattested') context.setLineDash([3, 4]);
        context.beginPath();
        context.moveTo(bracket.from, bracket.y + 2);
        context.bezierCurveTo(bracket.from, bracket.y + 50, capsule.x, capsule.y - 50, capsule.x, capsule.y - capsule.height / 2);
        context.stroke();
        context.setLineDash([]);
      }
      context.globalAlpha = 1;
    }
  }

  private drawDrops(lane: Lane, now: number, clock: number) {
    if (this.options.reducedMotion) return;
    const context = this.context;
    const gather = this.gather(lane);
    for (const batch of lane.batches) {
      const since = clock - this.performanceOf(batch.at) - gather;
      if (this.late(batch)) continue;
      if (since < -gather || since > DROP_MS + 600) continue;
      const carrier = this.carrier(batch);
      const bracket = this.bracket(lane, batch, now);
      const capsule = carrier ? this.capsule(carrier, now) : this.slab(now - batch.l1Time, batch.blobs);
      const from = { x: Math.min(bracket.from, this.gx + this.gw), y: bracket.y };
      const to = { x: capsule.x, y: capsule.y - capsule.height / 2 };
      if (since > DROP_MS) {
        this.glow(to.x, to.y + capsule.height / 2, 20 * (1 - (since - DROP_MS) / 600) + 4, lane.chain.color);
        continue;
      }
      const t = Math.max(0, since) / DROP_MS;
      const eased = t < 0.5 ? 2 * t * t : 1 - (-2 * t + 2) ** 2 / 2;
      const point = (value: number) => ({
        x: cubic(from.x, from.x, to.x, to.x, value),
        y: cubic(from.y, from.y + 50, to.y - 50, to.y, value),
      });
      for (let trail = 1; trail <= 6; trail += 1) {
        const back = point(Math.max(0, eased - trail * 0.03));
        context.globalAlpha = 0.16 * (1 - trail / 7);
        context.fillStyle = lane.chain.color;
        context.beginPath();
        context.arc(back.x, back.y, 4.5, 0, Math.PI * 2);
        context.fill();
      }
      const { x, y } = point(eased);
      context.globalAlpha = since < 0 ? 1 + since / gather : 1;
      this.glow(x, y, 16, lane.chain.color);
      const label = batch.blobs ? `${batch.blobs} blob${batch.blobs === 1 ? '' : 's'}` : 'batch';
      context.font = '600 10px "JetBrains Mono", ui-monospace, monospace';
      const textWidth = context.measureText(label).width;
      context.fillStyle = '#0b0f0d';
      context.beginPath();
      context.roundRect(x - textWidth / 2 - 7, y - 9, textWidth + 14, 18, 9);
      context.fill();
      context.strokeStyle = lane.chain.color;
      context.lineWidth = 1.5;
      context.stroke();
      context.fillStyle = '#f2f5f3';
      context.textAlign = 'center';
      context.textBaseline = 'middle';
      context.fillText(label, x, y + 0.5);
      context.globalAlpha = 1;
    }
  }

  /** Lines at the newest block of each level: solid for leani's own, dashed for what the page reads elsewhere. */
  private drawFrontiers(now: number) {
    const ethereum = this.lane(ETHEREUM)!;
    const confirmed = ethereum.blocks.filter((block) => block.level === 'confirmed').at(-1);
    if (confirmed) {
      for (const { value, alpha } of this.crossfade('confirmed', confirmed.born)) {
        this.frontier(this.x(now - value), ethereum.top, ethereum.top + ethereum.height, `confirmed · ${duration(now - value)}`, '#d6dbff', true, ethereum.top - 18, alpha);
      }
    }
    if (ethereum.finalizedTime !== null) {
      for (const { value, alpha } of this.crossfade('final', ethereum.finalizedTime)) {
        if (now - value > this.window) continue;
        this.frontier(this.x(now - value), ethereum.top, ethereum.top + ethereum.height, `final · ${duration(now - value)}`, '#3ee07f', false, ethereum.top - 18, alpha);
      }
    }
    for (const lane of this.rollups()) {
      const final = lane.batches.filter((batch) => this.carrierLevel(batch) === 'finalized').at(-1);
      if (final && now - final.lastTime <= this.window) {
        this.frontier(this.x(now - final.lastTime), lane.top, lane.top + lane.height, `${this.narrow ? 'final' : 'final on Ethereum'} · ${duration(now - final.lastTime)}`, '#3ee07f', true, lane.top + 4);
      }
      if (this.expanded) this.drawSettlement(lane, now);
    }
  }

  private crossfade(key: string, value: number) {
    const clock = performance.now();
    const state = this.frontiers.get(key);
    if (state?.value !== value) this.frontiers.set(key, { value, previous: state?.value ?? null, since: state ? clock : -Infinity });
    const { previous, since } = this.frontiers.get(key)!;
    const t = this.options.reducedMotion ? 1 : Math.min(1, (clock - since) / FADE_MS);
    return previous === null || t === 1 ? [{ value, alpha: 1 }] : [{ value: previous, alpha: 1 - t }, { value, alpha: t }];
  }

  private frontier(x: number, top: number, bottom: number, label: string, color: string, dashed: boolean, labelY: number, alpha = 1) {
    const context = this.context;
    const outer = context.globalAlpha;
    context.strokeStyle = color;
    context.globalAlpha = outer * alpha * 0.85;
    context.lineWidth = dashed ? 1.2 : 1.6;
    if (dashed) context.setLineDash([4, 4]);
    context.beginPath();
    context.moveTo(x, top + 2);
    context.lineTo(x, bottom - 2);
    context.stroke();
    context.setLineDash([]);
    context.globalAlpha = outer * alpha;
    context.font = '600 9.5px "JetBrains Mono", ui-monospace, monospace';
    const textWidth = context.measureText(label).width;
    const left = Math.min(x + 6, this.gx + this.gw - textWidth - 12);
    const y = labelY;
    context.fillStyle = 'rgba(11, 15, 13, 0.92)';
    context.beginPath();
    context.roundRect(left, y, textWidth + 10, 16, 8);
    context.fill();
    context.strokeStyle = color;
    context.lineWidth = 1;
    if (dashed) context.setLineDash([3, 2]);
    context.stroke();
    context.setLineDash([]);
    context.fillStyle = color;
    context.textAlign = 'left';
    context.textBaseline = 'middle';
    context.fillText(label, left + 5, y + 8.5);
    context.globalAlpha = outer;
  }

  private drawSettlement(lane: Lane, now: number) {
    const settlement = lane.chain.settlement;
    if (!settlement) return;
    if (lane.settledTime !== null) {
      this.frontier(this.x(now - lane.settledTime), lane.top, lane.top + lane.height, `settled · ${duration(now - lane.settledTime)}`, '#3ee07f', false, lane.top + 24);
    } else {
      this.frontier(this.x(settlement.seconds), lane.top, lane.top + lane.height, `settles ${settlement.typical}`, 'rgba(62, 224, 127, 0.6)', true, lane.top + 24);
    }
  }

  private drawEdge(now: number) {
    const context = this.context;
    if (this.narrow) return;
    if (this.expanded) {
      context.font = '600 10px "JetBrains Mono", ui-monospace, monospace';
      context.fillStyle = '#9ff0c4';
      context.textAlign = 'right';
      context.textBaseline = 'middle';
      context.fillText('◂ last 30 min', this.width - 12, 14);
      return;
    }
    const left = this.width - this.edge + 10;
    context.fillStyle = 'rgba(230, 240, 234, 0.025)';
    context.strokeStyle = 'rgba(62, 224, 127, 0.25)';
    context.setLineDash([3, 4]);
    context.beginPath();
    context.roundRect(left, 8, this.edge - 16, this.axisY - 16, 8);
    context.fill();
    context.stroke();
    context.setLineDash([]);
    context.textAlign = 'left';
    context.textBaseline = 'middle';
    context.font = '600 10.5px "JetBrains Mono", ui-monospace, monospace';
    context.fillStyle = '#9ff0c4';
    context.fillText('days of settlement ▸', left + 8, 22, this.edge - 28);
    context.font = '400 10px "Inter Variable", system-ui, sans-serif';
    for (const lane of this.rollups()) {
      const text = lane.settledTime !== null ? `settled ${duration(now - lane.settledTime)} ago` : `settles ${lane.chain.settlement?.typical ?? ''}`;
      context.fillStyle = '#8fa89a';
      context.fillText(text, left + 8, lane.top + lane.height / 2, this.edge - 28);
    }
    const ethereum = this.lane(ETHEREUM)!;
    context.fillStyle = '#8fa89a';
    context.fillText('final is settled', left + 8, ethereum.top + ethereum.height / 2, this.edge - 28);
  }

  /** `at` (ms on the feed's clock) as `performance.now()` time. */
  private performanceOf(at: number) {
    return performance.now() - (this.options.now() - at);
  }

  private flash(x: number, y: number, width: number, height: number, block: Block, color: string, clock: number) {
    const flash = clock - this.performanceOf(block.levelAt);
    if (this.options.reducedMotion || flash < 0 || flash >= 900 || block.levelAt === block.seen) return;
    const context = this.context;
    const alpha = context.globalAlpha;
    context.strokeStyle = color;
    context.globalAlpha *= 1 - flash / 900;
    context.lineWidth = 2;
    context.beginPath();
    const grow = Math.min(width, height) * (0.25 + flash / 1800);
    context.roundRect(x - width / 2 - grow, y - height / 2 - grow, width + grow * 2, height + grow * 2, 4);
    context.stroke();
    context.globalAlpha = alpha;
  }

  private glow(x: number, y: number, radius: number, color: string) {
    const context = this.context;
    const gradient = context.createRadialGradient(x, y, 0, x, y, radius);
    gradient.addColorStop(0, color);
    gradient.addColorStop(1, 'rgba(0, 0, 0, 0)');
    context.fillStyle = gradient;
    context.beginPath();
    context.arc(x, y, radius, 0, Math.PI * 2);
    context.fill();
  }

  private drawFollowed(now: number) {
    if (!this.followed) return;
    const lane = this.lane(this.followed.chain);
    const block = lane?.blocks.find((candidate) => candidate.number === this.followed!.number);
    if (!lane || lane.height === 0 || !block) return;
    const context = this.context;
    context.strokeStyle = '#ffffff';
    context.lineWidth = 1.5;
    if (!block.batch) {
      const { x, y } = this.blockPoint(lane, block, now);
      context.beginPath();
      context.arc(x, y, 8, 0, Math.PI * 2);
      context.stroke();
      return;
    }
    const bracket = this.bracket(lane, block.batch, now);
    const x = this.x(now - block.born);
    context.beginPath();
    context.moveTo(x, bracket.y - 7);
    context.lineTo(x, bracket.y + 7);
    context.stroke();
    const carrier = this.carrier(block.batch);
    if (carrier) {
      const capsule = this.capsule(carrier, now);
      context.beginPath();
      context.roundRect(capsule.x - capsule.width / 2 - 4, capsule.y - capsule.height / 2 - 4, capsule.width + 8, capsule.height + 8, 5);
      context.stroke();
    }
  }

  private drawUndo(x: number, y: number, size: number, since: number) {
    const context = this.context;
    const t = Math.min(1, since / 1400);
    context.fillStyle = `rgba(255, 92, 92, ${1 - t})`;
    for (let index = 0; index < 6; index += 1) {
      const angle = (index / 6) * Math.PI * 2;
      const distance = t * size * 1.8;
      const shard = size / 3;
      context.fillRect(x + Math.cos(angle) * distance - shard / 2, y + Math.sin(angle) * distance + t * t * 30 - shard / 2, shard, shard);
    }
  }
}
