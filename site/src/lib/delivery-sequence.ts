// Replays the durable-boundary figure on the durable delivery docs page. The
// first pass tells the whole story; later passes step through the remaining
// recorded blocks faster, with history already committed. Loaded lazily by
// DeliverySequence.astro, and stopped (back to the full static frame) while
// the figure is off screen or the tab is hidden.
type Pass = { num: string; time: string; parent: string; prev: string; seq: string; kicker: string };

const STAGE_STARTS = [0, 2.5, 4.8, 6.1]; // seconds into the first pass
const PASS_END = 10.4;
const FAST = 0.6; // later passes run at this fraction of the first pass's timing

export function replayDelivery(figure: HTMLElement): void {
  const passes = JSON.parse(figure.dataset.passes ?? '[]') as Pass[];
  if (!passes.length) return;
  const timed = [...figure.querySelectorAll<HTMLElement>('[data-at]')];
  const stages = [...figure.querySelectorAll<HTMLElement>('[data-stage]')];
  const slots = new Map<string, HTMLElement[]>();
  figure.querySelectorAll<HTMLElement>('[data-slot]').forEach((element) => {
    const key = element.dataset.slot!;
    slots.set(key, [...(slots.get(key) ?? []), element]);
  });
  const put = (key: string, value: string) => slots.get(key)?.forEach((element) => { element.textContent = value; });

  let pass = 0;
  let t = 0;
  let frame = 0;
  let last = 0;
  let visible = false;

  const load = (index: number, playing: boolean) => {
    const current = passes[index]!;
    put('num', current.num);
    put('time', current.time);
    put('parent', current.parent);
    put('prev', current.prev);
    put('kicker', current.kicker);
    put('seq', current.seq);
    put('status', `${playing ? 'replaying ' : ''}${current.num} · ${current.seq}`);
    figure.classList.toggle('is-later', index > 0);
  };
  const render = () => {
    const pace = pass ? FAST : 1;
    timed.forEach((element) => element.classList.toggle(
      'is-on',
      (pass > 0 && element.classList.contains('hist')) || t >= Number(element.dataset.at) * pace,
    ));
    const now = STAGE_STARTS.reduce((stage, at, i) => (t >= at * pace ? i : stage), 0);
    stages.forEach((element, i) => element.classList.toggle('is-now', i === now));
  };
  const tick = (ms: number) => {
    t += Math.min(0.1, (ms - last) / 1000);
    last = ms;
    if (t >= PASS_END * (pass ? FAST : 1)) {
      pass = (pass + 1) % passes.length;
      t = pass ? STAGE_STARTS[1]! * FAST : 0;
      load(pass, true);
    }
    render();
    frame = requestAnimationFrame(tick);
  };
  const start = () => {
    if (frame) return;
    figure.classList.add('is-anim');
    pass = 0;
    t = 0;
    load(0, true);
    render();
    last = performance.now();
    frame = requestAnimationFrame(tick);
  };
  const stop = () => {
    cancelAnimationFrame(frame);
    frame = 0;
    figure.classList.remove('is-anim', 'is-later');
    load(0, false);
    timed.forEach((element) => element.classList.remove('is-on'));
    stages.forEach((element) => element.classList.remove('is-now'));
  };
  const sync = () => (visible && !document.hidden ? start() : stop());

  new IntersectionObserver(([entry]) => {
    visible = entry!.isIntersecting;
    sync();
  }, { threshold: 0.2 }).observe(figure);
  document.addEventListener('visibilitychange', sync);
}
