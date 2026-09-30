// Plays the Backfill-meets-live seam: backfill fills the coverage gap, the
// cursor sweeps history in block order, the hash seam zips shut, and live
// blocks arrive and are acknowledged one by one before the loop fades and
// restarts. Loaded lazily by Delivery.astro. The server-rendered markup is the
// final frame, so stopping always returns to it.

type State = { fill: number; zip: boolean; checks: number; live: number; ack: number; pub: number };
type Anchor = [block: number, x: number];

export function startSeam(chart: HTMLElement): void {
  const num = (key: string) => Number(chart.dataset[key]);
  const startBlock = num('start');
  const gapStart = num('gap');
  const last = num('last');
  const head = num('head');
  const first = last + 1;
  const seqOf = (block: number) => num('seq') + block - last;

  const [z1, z2, z3] = ['.z1', '.z2', '.z3'].map((q) => chart.querySelector<HTMLElement>(q)!) as [HTMLElement, HTMLElement, HTMLElement];
  const gaps = [...z1.querySelectorAll<HTMLElement>('.gap i')];
  const checks = [...z2.querySelectorAll<HTMLElement>('.chk')];
  const lives = [...z3.querySelectorAll<HTMLElement>('.lt')];
  const hb = z2.querySelector<HTMLElement>('.hb')!;
  const lb = z2.querySelector<HTMLElement>('.lb')!;
  const cursor = chart.querySelector<HTMLElement>('.cur')!;
  const pill = cursor.querySelector<HTMLElement>('.pill')!;
  const seqLabel = cursor.querySelector<HTMLElement>('.seq')!;
  const headmark = chart.querySelector<HTMLElement>('.headmark')!;
  const zones = [z1, z2, z3].map((zone) => ({
    zone,
    track: zone.querySelector<HTMLElement>('.track')!,
    ticks: [...zone.querySelectorAll<HTMLElement>('.tick')],
  }));
  const START: State = { fill: 0, zip: false, checks: 0, live: 1, ack: gapStart - 1, pub: gapStart - 1 };
  const FINAL: State = { fill: gaps.length, zip: true, checks: checks.length, live: lives.length, ack: first, pub: head };
  let s: State = { ...FINAL };
  let timers: number[] = [];
  let raf = 0;

  const offsetIn = (el: HTMLElement, zone: HTMLElement) => {
    let x = 0;
    for (let n: HTMLElement | null = el; n && n !== zone; n = n.offsetParent as HTMLElement | null) x += n.offsetLeft;
    return x;
  };
  const center = (el: HTMLElement, zone: HTMLElement, track: HTMLElement) =>
    offsetIn(el, zone) + el.offsetWidth / 2 - offsetIn(track, zone);
  // Each zone maps block numbers onto its own track, so the cursor can walk
  // one chain across three differently sized zones.
  const anchors = (): Anchor[][] => {
    const [t1, t2, t3] = zones.map(({ track }) => track) as [HTMLElement, HTMLElement, HTMLElement];
    const c = lives.map((el) => center(el, z3, t3));
    const lastX = c.at(-1)!;
    return [
      [[startBlock - 0.5, 0], [last - 0.5, t1.clientWidth]],
      [[last - 0.5, 0], [last, center(hb, z2, t2)], [first, center(lb, z2, t2)], [first + 0.5, t2.clientWidth]],
      [[first + 0.5, 0], ...c.map((x, i): Anchor => [first + 1 + i, x]), [head + 0.5, lastX + (lastX - c.at(-2)!) / 2]],
    ];
  };
  const at = (pts: Anchor[], block: number) => {
    if (block <= pts[0]![0]) return pts[0]![1];
    for (let i = 1; i < pts.length; i++) {
      const [b1, x1] = pts[i]!;
      if (block <= b1) {
        const [b0, x0] = pts[i - 1]!;
        return x0 + ((block - b0) / (b1 - b0)) * (x1 - x0);
      }
    }
    return pts.at(-1)![1];
  };

  const render = () => {
    const a = anchors();
    gaps.forEach((g, i) => g.classList.toggle('is-bf', i < s.fill));
    z1.classList.toggle('is-complete', s.fill >= gaps.length);
    z2.classList.toggle('is-zipped', s.zip);
    checks.forEach((c, i) => c.classList.toggle('is-ok', i < s.checks));
    lives.forEach((tile, i) => {
      tile.classList.toggle('is-in', i < s.live);
      tile.classList.toggle('is-head', i === s.live - 1);
    });
    const headTile = lives[s.live - 1];
    if (headTile && headmark.parentElement !== headTile) headTile.append(headmark);
    z3.style.setProperty('--hx', `${at(a[2]!, first + s.live)}px`);
    let owned = false;
    zones.forEach(({ track, ticks }, i) => {
      const pts = a[i]!;
      const cut = at(pts, s.ack);
      track.style.setProperty('--cut', `${cut}px`);
      track.style.setProperty('--pub', `${Math.max(cut, at(pts, s.pub))}px`);
      ticks.forEach((tick) => {
        const block = Number(tick.dataset.b);
        tick.style.left = `${at(pts, block)}px`;
        tick.classList.toggle('is-on', s.pub >= block);
        tick.classList.toggle('is-acked', s.ack >= block);
      });
      if (s.ack <= pts[0]![0] || s.ack >= pts.at(-1)![0]) return;
      owned = true;
      // Moving the one cursor into another zone's track must not animate
      // from the old zone's coordinates.
      const moved = cursor.parentElement !== track;
      if (moved) {
        cursor.style.transition = pill.style.transition = 'none';
        track.append(cursor);
      }
      seqLabel.textContent = `seq ${seqOf(Math.floor(s.ack + 1e-6))}`;
      const width = pill.offsetWidth;
      const left = Math.min(Math.max(0, cut - width / 2), track.clientWidth - width);
      cursor.style.setProperty('--x', `${cut}px`);
      cursor.style.setProperty('--px', `${left - cut}px`);
      if (moved) {
        void cursor.offsetWidth;
        cursor.style.transition = pill.style.transition = '';
      }
    });
    cursor.classList.toggle('is-on', owned);
  };

  const later = (ms: number, fn: () => void) => timers.push(window.setTimeout(fn, ms));
  const set = (patch: Partial<State>) => {
    Object.assign(s, patch);
    render();
  };
  const pulse = () => {
    if (!cursor.classList.contains('is-on')) return;
    cursor.classList.remove('is-ack');
    void cursor.offsetWidth;
    cursor.classList.add('is-ack');
    later(760, () => cursor.classList.remove('is-ack'));
  };
  const sweep = (from: number, to: number, ms: number, done: () => void) => {
    const t0 = performance.now();
    chart.classList.add('is-sweep');
    const step = (now: number) => {
      const k = Math.min(1, (now - t0) / ms);
      s.ack = from + (to - from) * (k < 0.5 ? 2 * k * k : 1 - Math.pow(-2 * k + 2, 2) / 2);
      render();
      if (k < 1) {
        raf = requestAnimationFrame(step);
      } else {
        chart.classList.remove('is-sweep');
        done();
      }
    };
    raf = requestAnimationFrame(step);
  };
  // One pass is about 15 seconds: the gap fills, delivery catches up through
  // history, the seam closes, and the live blocks arrive and are acknowledged.
  const cycle = () => {
    chart.classList.add('is-reset');
    s = { ...START };
    render();
    void chart.offsetWidth;
    chart.classList.remove('is-reset');
    raf = requestAnimationFrame(() => chart.classList.remove('is-fading'));
    gaps.forEach((_, i) => later(700 + i * 260, () => set({ fill: i + 1 })));
    later(2900, () => set({ pub: last }));
    later(3200, () => sweep(gapStart - 1, last - 1, 1200, () => {
      set({ ack: last });
      pulse();
    }));
    later(5000, () => set({ zip: true }));
    checks.forEach((_, i) => later(5800 + i * 300, () => set({ checks: i + 1 })));
    later(6900, () => set({ pub: first + 1 }));
    later(7500, () => { set({ ack: first }); pulse(); });
    later(8500, () => { set({ ack: first + 1 }); pulse(); });
    later(9700, () => set({ live: 2, pub: first + 2 }));
    later(10700, () => { set({ ack: first + 2 }); pulse(); });
    later(11900, () => set({ live: 3, pub: first + 3 }));
    later(14900, () => chart.classList.add('is-fading'));
    later(15400, cycle);
  };
  const stop = () => {
    timers.forEach(clearTimeout);
    timers = [];
    cancelAnimationFrame(raf);
    chart.classList.remove('is-sweep', 'is-fading');
    cursor.classList.remove('is-ack');
    s = { ...FINAL };
    render();
  };
  const play = () => {
    stop();
    chart.classList.add('is-fading');
    later(420, cycle);
  };

  const reduced = matchMedia('(prefers-reduced-motion: reduce)');
  let visible = false;
  let running = false;
  const sync = () => {
    const should = visible && !document.hidden && !reduced.matches;
    if (should === running) return;
    running = should;
    if (running) play();
    else stop();
  };
  new IntersectionObserver(([entry]) => {
    visible = Boolean(entry?.isIntersecting);
    sync();
  }).observe(chart);
  document.addEventListener('visibilitychange', sync);
  reduced.addEventListener('change', sync);
  new ResizeObserver(() => render()).observe(chart);
}
