import type { Hello, HubEvent, Level } from './model';

export interface Feed {
  readonly mode: 'live' | 'replay';
  /** Milliseconds on the clock the events' times use. */
  now(): number;
  close(): void;
}

export interface FeedHandlers {
  event(event: HubEvent): void;
  reset(): void;
  state(connected: boolean): void;
  chains(chains: string[]): void;
}

const KINDS = ['block', 'level', 'undo', 'status', 'batch'] as const;

/** Follow a hub's `/events` stream; its clock is the hub's. */
export function liveFeed(hub: string, handlers: FeedHandlers): Feed {
  let offset = 0;
  const source = new EventSource(new URL('events', hub.endsWith('/') ? hub : `${hub}/`));
  source.addEventListener('hello', (message) => {
    const hello = JSON.parse((message as MessageEvent<string>).data) as Hello;
    offset = hello.now - Date.now();
    handlers.reset();
    handlers.chains(hello.chains);
    handlers.state(true);
  });
  for (const kind of KINDS) {
    source.addEventListener(kind, (message) => {
      handlers.event(JSON.parse((message as MessageEvent<string>).data) as HubEvent);
    });
  }
  source.addEventListener('error', () => handlers.state(false));
  return {
    mode: 'live',
    now: () => Date.now() + offset,
    close: () => source.close(),
  };
}

export interface Recording {
  recordedAt: number;
  durationMs: number;
  chains: string[];
  /** Who posted an Ethereum block's blobs, in the order its counts follow. */
  sources: string[];
  /** `[ms since the previous event, chain index, kind, ...fields]`. */
  events: number[][];
}

const LEVELS: Level[] = ['unattested', 'attested', 'finalized', 'confirmed'];

export function unpack(recording: Recording): { at: number; event: HubEvent }[] {
  let at = recording.recordedAt;
  const last = new Map<number, { number: number; time: number }>();
  return recording.events.map(([delta, index, kind, lag, ...fields]) => {
    at += delta;
    // When it happened, for an event from before the recording's window.
    const own = at - lag;
    const chain = recording.chains[index];
    switch (KINDS[kind]) {
      case 'block': {
        const [numberDelta, timeDelta, level, txs, gas, ...counts] = fields;
        const previous = last.get(index) ?? { number: 0, time: 0 };
        const block = { number: previous.number + numberDelta, time: previous.time + timeDelta };
        last.set(index, block);
        const event: Extract<HubEvent, { type: 'block' }> = {
          type: 'block', chain, number: block.number, hash: '', time: block.time, seen: own,
          level: LEVELS[level], txs: txs < 0 ? null : txs, gas: gas < 0 ? null : gas / 1000,
        };
        if (counts.length) {
          event.blobs = Object.fromEntries(
            recording.sources.flatMap((source, position) => (counts[position] ? [[source, counts[position]]] : [])),
          );
        }
        return { at, event };
      }
      case 'level': {
        const [level, through, time] = fields;
        return {
          at,
          event: { type: 'level', chain, level: LEVELS[level] as Exclude<Level, 'unattested'>, through, time: time || null, at: own },
        };
      }
      case 'undo':
        return { at, event: { type: 'undo', chain, number: fields[0], hash: '', at: own } };
      case 'status':
        return { at, event: { type: 'status', chain, connected: fields[0] === 1, at: own } };
      default: {
        const [l1, l1Time, blobs, first, span, txs, bytes, raw, verified, firstAgo, lastAgo, inbox] = fields;
        return {
          at,
          event: {
            type: 'batch', chain, l1, l1Hash: '', l1Time, at: own, blobs, first, last: first + span,
            firstTime: l1Time - firstAgo, lastTime: l1Time - lastAgo, txs, bytes, raw,
            verified: verified < 0 ? null : verified === 1, source: inbox === 1 ? 'inbox' : 'blobs',
          },
        };
      }
    }
  });
}

/** Play `recording` in a loop on its own clock, starting over at its end. */
export function replayFeed(recording: Recording, handlers: FeedHandlers): Feed {
  const events = unpack(recording);
  let started = performance.now();
  let next = 0;
  let timer = 0;
  const now = () => recording.recordedAt + (performance.now() - started);
  const pump = () => {
    const clock = now();
    while (next < events.length && events[next].at <= clock) {
      handlers.event(events[next].event);
      next += 1;
    }
    if (clock >= recording.recordedAt + recording.durationMs) {
      started = performance.now();
      next = 0;
      handlers.reset();
    }
    timer = window.setTimeout(pump, 50);
  };
  handlers.reset();
  handlers.chains(recording.chains);
  handlers.state(true);
  pump();
  return { mode: 'replay', now, close: () => window.clearTimeout(timer) };
}
