export interface TyperLine {
  text: string;
  delayMs?: number;
  charMs?: number;
  className?: string;
  /** Optional highlighted form; swapped in when the line finishes typing. */
  html?: string;
}

export interface TyperStep { atMs: number; lineIndex: number; chars: number }

export function buildTimeline(lines: TyperLine[]): TyperStep[] {
  const steps: TyperStep[] = [];
  let clock = 0;
  lines.forEach((line, lineIndex) => {
    clock += line.delayMs ?? 0;
    const charMs = line.charMs ?? 14;
    if (line.text.length === 0) return;
    if (charMs === 0) {
      steps.push({ atMs: clock, lineIndex, chars: line.text.length });
      return;
    }
    for (let chars = 1; chars <= line.text.length; chars++) {
      clock += charMs;
      steps.push({ atMs: clock, lineIndex, chars });
    }
  });
  return steps;
}

const reducedMotion = () =>
  typeof matchMedia === 'function' && matchMedia('(prefers-reduced-motion: reduce)').matches;

/** Renders lines into `el` as one <span> per line, revealing text per buildTimeline. */
export function playTyper(
  el: HTMLElement,
  lines: TyperLine[],
  opts: { loop?: boolean; loopPauseMs?: number } = {},
): () => void {
  const spans = lines.map((line) => {
    const span = document.createElement('span');
    span.className = `t-line ${line.className ?? ''}`.trim();
    el.append(span);
    return span;
  });
  const showAll = () =>
    lines.forEach((line, i) => {
      if (line.html) spans[i]!.innerHTML = line.html;
      else spans[i]!.textContent = line.text;
    });

  if (reducedMotion()) {
    showAll();
    return () => {};
  }

  const steps = buildTimeline(lines);
  let timer = 0;
  let cancelled = false;
  const run = () => {
    spans.forEach((s) => (s.textContent = ''));
    const started = performance.now();
    let next = 0;
    const tick = () => {
      if (cancelled) return;
      const elapsed = performance.now() - started;
      while (next < steps.length && steps[next]!.atMs <= elapsed) {
        const step = steps[next]!;
        const line = lines[step.lineIndex]!;
        if (step.chars === line.text.length && line.html) {
          spans[step.lineIndex]!.innerHTML = line.html;
        } else {
          spans[step.lineIndex]!.textContent = line.text.slice(0, step.chars);
        }
        next++;
      }
      if (next < steps.length) {
        timer = requestAnimationFrame(tick);
      } else if (opts.loop) {
        timer = window.setTimeout(run, opts.loopPauseMs ?? 3000) as unknown as number;
      }
    };
    timer = requestAnimationFrame(tick);
  };
  run();
  return () => {
    cancelled = true;
    cancelAnimationFrame(timer);
    clearTimeout(timer);
    showAll();
  };
}

export function onVisible(el: Element, cb: () => void): void {
  if (!('IntersectionObserver' in globalThis)) return cb();
  const io = new IntersectionObserver(
    (entries) => {
      if (entries.some((e) => e.isIntersecting)) {
        io.disconnect();
        cb();
      }
    },
    { threshold: 0.25 },
  );
  io.observe(el);
}
