import session from '../data/follow-session.json';
import { blockRowHtml } from './block-row';
import { hlShell } from './highlight';

// Replays the recorded `leani subscribe blocks` session 4× faster. Raw chain
// bytes stream toward the terminal; grey ones dissolve before they reach it,
// and a few in-flight bytes turn green and land just as each recorded row
// prints. Loaded lazily by Quickstart.astro, so none of this is entry JS.

type Pt = [x: number, y: number, curve: number];
type Path = { pts: Pt[]; cum: number[]; len: number };
type Layer = { size: number; speed: number; alpha: number; color: string };
type Lane = { grey: Path; keep: Path; split: number; layer: Layer; next: number };
type Part = { lane: Lane; layer: Layer; path: Path; s: number; k: number; keep: boolean; off: number; txt: string; col: string };

const SPEEDUP = 4;
const SHOWN = 6; // rows the server already rendered
const FONT = '"JetBrains Mono", ui-monospace, monospace';
const LAYERS: Layer[] = [
  { size: 9, speed: 40, alpha: 0.55, color: '#3f6150' },
  { size: 11, speed: 62, alpha: 0.66, color: '#557a64' },
  { size: 13, speed: 88, alpha: 0.8, color: '#6c907b' },
];
const COMMON = ['00', '00', '00', '00', '00', 'a0', 'a0', 'f8', 'f9', '94', '80', '01', '02', 'ff', 'b9', 'c0', '83', '84', '88', '0f'];

const ss = (a: number, b: number, x: number) => {
  const t = Math.min(1, Math.max(0, (x - a) / (b - a)));
  return t * t * (3 - 2 * t);
};

function track(pts: Pt[]): Path {
  const cum = [0];
  for (let i = 1; i < pts.length; i++) {
    cum.push(cum[i - 1]! + Math.hypot(pts[i]![0] - pts[i - 1]![0], pts[i]![1] - pts[i - 1]![1]));
  }
  return { pts, cum, len: cum[cum.length - 1]! };
}

export function startFirehose(root: HTMLElement): void {
  const canvas = root.querySelector('canvas');
  const frame = root.querySelector<HTMLElement>('[data-live-term]');
  const body = frame?.querySelector<HTMLElement>('.term-body');
  const list = root.querySelector<HTMLElement>('[data-lines]');
  const ctx = canvas?.getContext('2d');
  const gctx = document.createElement('canvas').getContext('2d');
  if (!canvas || !frame || !body || !list || !ctx || !gctx) return;
  const guide = gctx.canvas;

  const rows = session.rows;
  const times = rows.map((row) => Date.parse(row.slice(0, 20)));
  const narrow = matchMedia('(max-width: 1199px)');
  const reduced = matchMedia('(prefers-reduced-motion: reduce)');
  const prompt = hlShell(`$ ${session.command}`);

  const halo = document.createElement('canvas');
  halo.width = halo.height = 32;
  const hctx = halo.getContext('2d')!;
  const haloFill = hctx.createRadialGradient(16, 16, 0, 16, 16, 16);
  haloFill.addColorStop(0, 'rgba(62,224,127,0.42)');
  haloFill.addColorStop(1, 'rgba(62,224,127,0)');
  hctx.fillStyle = haloFill;
  hctx.fillRect(0, 0, 32, 32);

  let seed = 1;
  const rnd = () => (seed = (seed * 48271) % 2147483647) / 2147483647;
  const hex = () => (rnd() < 0.5 ? COMMON[(rnd() * COMMON.length) | 0]! : ((rnd() * 256) | 0).toString(16).padStart(2, '0'));
  const pick = () => {
    const r = rnd();
    return LAYERS[r < 0.42 ? 0 : r < 0.8 ? 1 : 2]!;
  };
  const pos = (el: HTMLElement) => {
    let x = 0;
    let y = 0;
    for (let n: HTMLElement | null = el; n && n !== root; n = n.offsetParent as HTMLElement | null) {
      x += n.offsetLeft;
      y += n.offsetTop;
    }
    return { x, y, w: el.offsetWidth, h: el.offsetHeight };
  };

  let W = 0, H = 0, dpr = 1, top = false, cap = 300;
  let lanes: Lane[] = [];
  let parts: Part[] = [];
  let intake = { u: 0, a: 0, b: 0 };
  let bloom: { x: number; y: number; sx: number; sy: number; fill: CanvasGradient } | null = null;
  let glow = 0, shown = -1, raf = 0, last = 0, due = 0, index = SHOWN, primed = false;
  let phase: 'row' | 'stop' | 'prompt' = 'row';
  let visible = false, running = false;
  let X = 0, Y = 0, C = 0, A = 0, S = 1, B = 0;

  // Desktop: lanes run in from the left edge and converge on the terminal's
  // left side. Narrow: they run across a strip above it, and kept bytes drop in.
  const build = () => {
    lanes = [];
    if (!top) {
      const n = 16, y0 = 24, y1 = H - 24;
      for (let i = 0; i < n; i++) {
        const v0 = y0 + ((y1 - y0) * (i + 0.5)) / n + (rnd() - 0.5) * 8;
        const v1 = intake.a + ((intake.b - intake.a) * i) / (n - 1);
        const bend = Math.min(intake.u + 30, 110 + Math.abs(v1 - v0) * 1.2);
        const pts: Pt[] = [];
        for (let k = 0; k <= 90; k++) {
          const u = -30 + ((intake.u + 30) * k) / 90;
          const c = ss(intake.u - bend, intake.u, u);
          pts.push([u, v0 + (v1 - v0) * c, c]);
        }
        const path = track(pts);
        lanes.push({ grey: path, keep: path, split: path.len, layer: pick(), next: 0 });
      }
      return;
    }
    const n = W <= 600 ? 6 : 9, y0 = 18, y1 = intake.u - 24;
    for (let i = 0; i < n; i++) {
      const v0 = y0 + ((y1 - y0) * (i + 0.5)) / n;
      const tx = intake.a + (intake.b - intake.a) * rnd();
      const peel = -30 + (tx + 30) * (0.2 + 0.55 * rnd());
      const drop = intake.u - v0;
      const pts: Pt[] = [[-30, v0, 0]];
      for (let k = 0; k <= 40; k++) {
        const t = k / 40, m = 1 - t;
        pts.push([
          m * m * m * peel + 3 * m * m * t * tx + 3 * m * t * t * tx + t * t * t * tx,
          m * m * m * v0 + 3 * m * m * t * v0 + 3 * m * t * t * (v0 + drop * 0.45) + t * t * t * intake.u,
          t,
        ]);
      }
      lanes.push({ grey: track([[-30, v0, 0], [W + 30, v0, 0]]), keep: track(pts), split: peel + 30, layer: pick(), next: 0 });
    }
  };

  const packet = (lane: Lane, head: number) => {
    const layer = lane.layer, n = 3 + ((rnd() * 6) | 0), gap = layer.size * 1.8, off = (rnd() - 0.5) * 6;
    for (let j = 0; j < n && parts.length < cap; j++) {
      parts.push({ lane, layer, path: lane.grey, s: head - j * gap, k: 0, keep: false, off, txt: hex(), col: layer.color });
    }
    return n * gap;
  };

  const fill = () => {
    parts = [];
    for (const lane of lanes) {
      let s = lane.grey.len - rnd() * 240;
      while (s > 0) s -= packet(lane, s) + 70 + rnd() * 230;
      lane.next = -s / lane.layer.speed;
    }
  };

  // Turn a few in-flight bytes green so they land when the next row prints.
  const choose = (seconds: number) => {
    let picked = 0;
    for (const p of parts) {
      if (picked === 3) return;
      if (p.keep || p.s < 0 || p.s > p.lane.split || rnd() < 0.5) continue;
      if (Math.abs((p.lane.keep.len - p.s) / p.layer.speed - seconds) > 0.35) continue;
      p.keep = true;
      p.path = p.lane.keep;
      p.k = 0;
      p.col = rnd() < 0.7 ? '#3ee07f' : '#9ff0c4';
      picked++;
    }
  };

  const locate = (p: Part) => {
    const { pts, cum } = p.path;
    let k = p.k;
    while (k < cum.length - 2 && cum[k + 1]! < p.s) k++;
    p.k = k;
    const a = pts[k]!, b = pts[k + 1]!;
    const t = Math.min(1, Math.max(0, (p.s - cum[k]!) / (cum[k + 1]! - cum[k]! || 1)));
    X = a[0] + (b[0] - a[0]) * t;
    Y = a[1] + (b[1] - a[1]) * t;
    C = a[2] + (b[2] - a[2]) * t;
  };

  const shade = (p: Part) => {
    const r = p.path.len - p.s;
    S = 1;
    B = 0;
    if (p.keep) {
      B = top ? C : ss(540, 110, r);
      A = 0.2 + 0.8 * B;
    } else if (top) {
      A = p.layer.alpha * 0.36;
      if (r < 60) A *= r / 60;
    } else {
      A = p.layer.alpha * (0.34 + 0.66 * ss(500, 150, r));
      if (r < 96) {
        const f = r / 96;
        A *= f * f;
        S = 0.4 + 0.6 * f;
      }
    }
    if (!top) A *= ss(-30, 150, X);
  };

  const guides = () => {
    gctx.setTransform(1, 0, 0, 1, 0, 0);
    gctx.clearRect(0, 0, guide.width, guide.height);
    if (top) return;
    gctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    const g = gctx.createLinearGradient(0, 0, intake.u, 0);
    g.addColorStop(0, 'rgba(83,118,96,0.025)');
    g.addColorStop(0.5, 'rgba(83,118,96,0.045)');
    g.addColorStop(0.86, 'rgba(83,118,96,0.14)');
    g.addColorStop(1, 'rgba(62,224,127,0.34)');
    gctx.strokeStyle = g;
    gctx.lineWidth = 1;
    for (const lane of lanes) {
      gctx.beginPath();
      lane.grey.pts.forEach(([x, y], i) => (i ? gctx.lineTo(x, y) : gctx.moveTo(x, y)));
      gctx.stroke();
    }
  };

  const draw = () => {
    ctx.setTransform(1, 0, 0, 1, 0, 0);
    ctx.globalAlpha = 1;
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    ctx.drawImage(guide, 0, 0);
    if (bloom) {
      ctx.setTransform(dpr * bloom.sx, 0, 0, dpr * bloom.sy, dpr * bloom.x, dpr * bloom.y);
      ctx.globalAlpha = 0.07 + 0.26 * glow;
      ctx.fillStyle = bloom.fill;
      ctx.fillRect(-1, -1, 2, 2);
    }
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    for (const layer of LAYERS) {
      ctx.font = `${layer.size}px ${FONT}`;
      ctx.fillStyle = layer.color;
      for (const p of parts) {
        if (p.keep || p.layer !== layer || p.s < 0) continue;
        locate(p);
        shade(p);
        if (A < 0.012) continue;
        ctx.globalAlpha = A;
        ctx.setTransform(dpr * S, 0, 0, dpr * S, dpr * X, dpr * (Y + (top ? p.off : p.off * (1 - 0.85 * C))));
        ctx.fillText(p.txt, 0, 0);
      }
    }
    for (const p of parts) {
      if (!p.keep || p.s < 0) continue;
      locate(p);
      shade(p);
      const y = Y + (top ? 0 : p.off * (1 - 0.85 * C));
      if (A * B > 0.02) {
        ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
        ctx.globalAlpha = A * B;
        ctx.drawImage(halo, X - 16, y - 16);
      }
      ctx.font = `500 ${p.layer.size}px ${FONT}`;
      ctx.globalAlpha = A;
      ctx.fillStyle = p.col;
      ctx.setTransform(dpr, 0, 0, dpr, dpr * X, dpr * y);
      ctx.fillText(p.txt, 0, 0);
    }
    ctx.setTransform(1, 0, 0, 1, 0, 0);
    ctx.globalAlpha = 1;
    if (Math.abs(glow - shown) > 0.01) {
      shown = glow;
      frame.style.setProperty('--glow', glow.toFixed(3));
    }
  };

  // Bottom-anchored like a real terminal: append, then slide the list up.
  const print = (html: string) => {
    const line = document.createElement('span');
    line.className = 'ln is-new';
    line.innerHTML = html;
    list.append(line);
    list.style.transition = 'none';
    list.style.transform = `translateY(${line.offsetHeight}px)`;
    void list.offsetHeight;
    list.style.transition = '';
    list.style.transform = '';
    while (list.children.length > 24) list.firstElementChild?.remove();
  };

  const gapBefore = (i: number) => {
    const a = times[i - 1], b = times[i];
    return a !== undefined && b !== undefined && b > a ? Math.max(1200, (b - a) / SPEEDUP) : 1200;
  };

  const schedule = (now: number, wait: number) => {
    due = now + wait;
    choose(wait / 1000);
  };

  // When the recording runs out, stop it the way a person would and start over.
  const advance = (now: number) => {
    if (phase === 'row') {
      print(blockRowHtml(rows[index]!));
      glow = Math.min(1, glow + 0.5);
      index++;
      if (index < rows.length) schedule(now, gapBefore(index));
      else {
        phase = 'stop';
        due = now + 2200;
      }
    } else if (phase === 'stop') {
      print('<span class="br-key">^C</span>');
      phase = 'prompt';
      due = now + 700;
    } else {
      print(prompt);
      phase = 'row';
      index = 0;
      schedule(now, 2600);
    }
  };

  const step = (dt: number) => {
    for (const lane of lanes) {
      lane.next -= dt;
      while (lane.next <= 0) lane.next += (packet(lane, -lane.next * lane.layer.speed) + 70 + rnd() * 230) / lane.layer.speed;
    }
    for (let i = parts.length - 1; i >= 0; i--) {
      const p = parts[i]!;
      p.s += p.layer.speed * dt;
      if (p.s < p.path.len) continue;
      if (p.keep) glow = Math.min(1, glow + 0.42);
      parts[i] = parts[parts.length - 1]!;
      parts.pop();
    }
    glow = Math.max(0, glow - dt * 1.3);
  };

  const loop = (now: number) => {
    step(Math.min(0.05, (now - last) / 1000));
    last = now;
    if (now >= due) {
      if (index >= rows.length && phase === 'row') phase = 'stop';
      advance(now);
    }
    draw();
    raf = requestAnimationFrame(loop);
  };

  const sync = () => {
    const should = visible && !document.hidden && !reduced.matches && W > 0;
    if (should === running) return;
    running = should;
    if (!running) {
      cancelAnimationFrame(raf);
      return;
    }
    last = performance.now();
    if (!primed) {
      primed = true;
      schedule(last, index < rows.length ? gapBefore(index) : 2200);
    } else if (due < last) schedule(last, 900);
    raf = requestAnimationFrame(loop);
  };

  const layout = () => {
    const w = root.clientWidth, h = root.clientHeight;
    if (!w || !h) return;
    W = w;
    H = h;
    top = narrow.matches;
    cap = W <= 600 ? 110 : 300;
    dpr = Math.min(2, devicePixelRatio || 1);
    canvas.width = guide.width = Math.round(W * dpr);
    canvas.height = guide.height = Math.round(H * dpr);
    const t = pos(frame), b = pos(body);
    intake = top ? { u: t.y, a: t.x + t.w * 0.22, b: t.x + t.w * 0.78 } : { u: t.x, a: b.y + 10, b: b.y + b.h - 10 };
    const fillGradient = ctx.createRadialGradient(0, 0, 0, 0, 0, 1);
    fillGradient.addColorStop(0, 'rgba(62,224,127,0.9)');
    fillGradient.addColorStop(0.45, 'rgba(62,224,127,0.2)');
    fillGradient.addColorStop(1, 'rgba(62,224,127,0)');
    bloom = top
      ? { x: t.x + t.w / 2, y: intake.u, sx: t.w * 0.42, sy: 60, fill: fillGradient }
      : { x: intake.u, y: (intake.a + intake.b) / 2, sx: 120, sy: (intake.b - intake.a) / 2 + 70, fill: fillGradient };
    seed = 20260929;
    build();
    fill();
    guides();
    draw();
    sync();
  };

  root.classList.add('is-live');
  root.querySelector<HTMLElement>('[data-replay]')?.removeAttribute('hidden');
  new IntersectionObserver(([entry]) => {
    visible = Boolean(entry?.isIntersecting);
    sync();
  }).observe(root);
  document.addEventListener('visibilitychange', sync);
  reduced.addEventListener('change', sync);
  new ResizeObserver(layout).observe(root);
  void document.fonts?.ready.then(layout);
}
