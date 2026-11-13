import { Scene, duration, type Journey } from './scene';
import { liveFeed, replayFeed, type Feed, type Recording } from './feed';
import { CHAINS, type Chain, type Level } from './model';

export interface Stage {
  scene: Scene;
  /** The chains the feed carries, in the page's order. */
  present(): string[];
}

const number = (value: number) => value.toLocaleString('en-US');
const time = (seconds: number) => (seconds < 0.1 ? '<0.1 s' : duration(seconds));
const label = (id: string) => CHAINS.find((chain) => chain.id === id)?.label ?? id;
const levelName = (chain: Chain, level: Level) => ({
  unattested: 'seen, unattested',
  attested: chain.family === 'ethereum' ? 'attested by the sync committee' : 'signed by the sequencer',
  confirmed: 'confirmed by the fast confirmation rule',
  finalized: chain.family === 'ethereum' ? 'finalized' : 'final on Ethereum',
  settled: 'settled on Ethereum',
})[level];

/**
 * Wire the picture inside `root`: the canvas, its hover card, the strip about a followed block, and the days
 * toggle. The feed is the hub `?hub=` names, else the recorded replay. The page's one `[data-status]` line, if
 * any, says which. Drawing stops while the canvas is off screen.
 */
export async function mountStage(root: ParentNode): Promise<Stage> {
  const canvas = root.querySelector<HTMLCanvasElement>('[data-canvas]')!;
  const tip = root.querySelector<HTMLElement>('[data-tip]')!;
  const expand = root.querySelector<HTMLButtonElement>('[data-expand]')!;
  const status = document.querySelector<HTMLElement>('[data-status]');
  const dot = document.querySelector<HTMLElement>('[data-status-dot]');
  let feed: Feed | null = null;
  let present: string[] = CHAINS.map((chain) => chain.id);
  let onScreen = true;
  new IntersectionObserver(([entry]) => {
    onScreen = entry!.isIntersecting;
  }).observe(canvas);

  const scene = new Scene(canvas, {
    now: () => feed?.now() ?? Date.now(),
    reducedMotion: window.matchMedia('(prefers-reduced-motion: reduce)').matches,
    paused: () => !onScreen,
    onExpand: (expanded) => {
      expand.setAttribute('aria-pressed', String(expanded));
      expand.textContent = expanded ? '◂ last 30 min' : 'days of settlement ▸';
    },
    onHover: (hover) => {
      if (!hover) {
        tip.hidden = true;
        return;
      }
      const { block, chain, batch } = hover;
      const lines = [`<b style="color:${chain.color}">${chain.label}</b> ${block ? `#${number(block.number)}` : 'batch'}`];
      if (block) {
        lines.push(`${levelName(chain, block.level)} · ${duration(block.age)} old`);
        if (block.txs !== null) lines.push(`${block.txs} txs${block.gas === null ? '' : ` · ${Math.round(block.gas * 100)}% gas`}`);
        const blobs = Object.entries(block.blobs ?? {});
        if (blobs.length) lines.push(blobs.map(([source, count]) => `${count} blob${count === 1 ? '' : 's'} ${source === 'other' ? 'from others' : `from ${label(source)}`}`).join('<br>'));
        if (chain.family !== 'ethereum' && !hover.followed) lines.push('<span style="color:#8fa89a">click to follow it</span>');
      }
      if (batch) {
        lines.push(`blocks #${number(batch.first)}–${number(batch.last)}: ${number(batch.last - batch.first + 1)}${batch.txs ? `, ${number(batch.txs)} txs` : ''}`);
        lines.push(`${batch.blobs ? `${batch.blobs} blobs on ` : 'in '}Ethereum #${number(batch.l1)}${batch.bytes ? ` · ${(batch.raw / batch.bytes).toFixed(1)}× compressed` : ''}`);
        if (batch.source === 'inbox') lines.push('named by its SequencerInbox call');
        if (batch.verified) lines.push('its parent hash matches the block leani has');
      }
      if (block?.hash) lines.push(`${block.hash.slice(0, 10)}…${block.hash.slice(-6)}`);
      tip.hidden = false;
      tip.innerHTML = lines.join('<br>');
      const left = Math.max(4, Math.min(hover.x + 14, canvas.clientWidth - tip.offsetWidth - 4));
      const top = Math.min(hover.y + 14, canvas.clientHeight - tip.offsetHeight - 8);
      tip.style.transform = `translate(${left}px, ${top}px)`;
    },
  });
  expand.addEventListener('click', () => scene.expand(expand.getAttribute('aria-pressed') !== 'true'));

  const handlers = {
    event: (event: Parameters<Scene['handle']>[0]) => scene.handle(event),
    reset: () => scene.reset(),
    state: (connected: boolean) => dot?.classList.toggle('on', connected),
    chains: (chains: string[]) => {
      present = CHAINS.map((chain) => chain.id).filter((chain) => chains.includes(chain));
      scene.show(present);
    },
  };
  const hub = new URLSearchParams(location.search).get('hub');
  let source = '';
  if (hub) {
    feed = liveFeed(hub, handlers);
    source = `live from ${new URL(hub).host}`;
  } else {
    try {
      const response = await fetch('/finality/replay.json.gz');
      if (!response.ok) throw new Error(String(response.status));
      const bytes = new Uint8Array(await response.arrayBuffer());
      // A server may send it as `Content-Encoding: gzip`, which the browser has unpacked already.
      const packed = bytes[0] === 0x1f && bytes[1] === 0x8b;
      const text = packed
        ? await new Response(new Blob([bytes]).stream().pipeThrough(new DecompressionStream('gzip'))).text()
        : new TextDecoder().decode(bytes);
      const recording = JSON.parse(text) as Recording;
      feed = replayFeed(recording, handlers);
      source = `replay of ${new Date(recording.recordedAt).toISOString().slice(0, 16).replace('T', ' ')} UTC`;
    } catch {
      source = 'no live hub and no recording';
    }
  }

  const follow = root.querySelector<HTMLElement>('[data-follow]')!;
  const followTitle = root.querySelector<HTMLElement>('[data-follow-title]')!;
  const followStages = root.querySelector<HTMLElement>('[data-follow-stages]')!;
  const showJourney = (journey: Journey | null) => {
    follow.hidden = !journey;
    if (!journey) return;
    const at = (ms: number | null) => (ms === null ? null : Math.max(0, ms / 1000 - journey.born));
    const rows: [string, number | null, string][] = [
      ['signed', at(journey.attestedAt ?? journey.seen), ''],
      ['in a blob', journey.batch ? Math.max(0, journey.batch.l1Time - journey.born) : null, journey.batch ? `Ethereum #${number(journey.batch.l1)}` : 'waiting for the batcher'],
      ['confirmed', at(journey.l1?.confirmedAt ?? null), ''],
      ['final', at(journey.l1?.finalizedAt ?? null), ''],
      ['settled', null, journey.chain.settlement?.typical ?? '—'],
    ];
    followTitle.style.color = journey.chain.color;
    const title = `${journey.chain.label} #${number(journey.number)}${journey.txs === null ? '' : ` · ${journey.txs} txs`}`;
    if (followTitle.textContent !== title) followTitle.textContent = title;
    const html = rows.map(([name, seconds, detail]) =>
      `<li class="${seconds === null ? 'pending' : 'done'}">${name}${seconds === null ? '' : ` <b>${seconds < 0.1 ? time(seconds) : `+${time(seconds)}`}</b>`}${detail ? ` <em>${detail}</em>` : ''}</li>`).join('');
    if (followStages.innerHTML !== html) followStages.innerHTML = html;
  };
  // A click outside the picture lets the followed block go too, except on the strip about it.
  document.addEventListener('click', (event) => {
    if (event.target !== canvas && !follow.contains(event.target as Node)) scene.unfollow();
    showJourney(scene.journey());
  });

  const refresh = () => {
    expand.hidden = !scene.settles();
    const following = scene.summaries().filter((summary) => summary.head !== null).length;
    if (status) status.textContent = `${source} · ${following} of ${present.length} chains`;
    showJourney(scene.journey());
  };
  refresh();
  window.setInterval(refresh, 1000);
  return { scene, present: () => present };
}
