import { existsSync } from 'node:fs';
import { mkdir, rename, rm, writeFile } from 'node:fs/promises';
import { createServer } from 'node:net';
import { basename, resolve } from 'node:path';

const repositoryRoot = resolve(import.meta.dir, '../..');
const siteRoot = resolve(repositoryRoot, 'site');
const docsDirectory = resolve(repositoryRoot, 'docs/contributing');
const probe = `watch-probe-${process.pid}`;
const firstPath = resolve(docsDirectory, `${probe}.mdx`);
const renamedPath = resolve(docsDirectory, `${probe}-renamed.mdx`);
const firstRoute = `/docs/contributing/${probe}/`;
const renamedRoute = `/docs/contributing/${probe}-renamed/`;
const sentinels: string[] = [];
const logs: string[] = [];

// Fixed budgets cover the cold start. Every later step waits for a multiple
// of the measured watcher round trip, within bounds: a slow runner gets more
// time, and a broken watcher still fails within minutes.
const STARTUP_TIMEOUT_MS = 90_000;
const READINESS_TIMEOUT_MS = 90_000;
const SENTINEL_WINDOW_MS = 5_000;
const STEP_TIMEOUT_FACTOR = 10;
const MIN_STEP_TIMEOUT_MS = 20_000;
const MAX_STEP_TIMEOUT_MS = 60_000;
const POLL_INTERVAL_MS = 250;
const REQUEST_TIMEOUT_MS = 15_000;
const STOP_TIMEOUT_MS = 10_000;
const LOG_LINES = 40;

interface Observation {
  status?: number;
  body?: string;
  error?: string;
}

let roundTripMs: number | undefined;
let stepTimeoutMs = MAX_STEP_TIMEOUT_MS;

function document(title: string, body: string): string {
  return `---\ntitle: ${title}\ndescription: Temporary cross-root watcher integration probe.\nsection: contributing\norder: 99999\nstatus: preview\n---\n\n${body}\n`;
}

function seconds(milliseconds: number): string {
  return `${(milliseconds / 1000).toFixed(1)} s`;
}

// Asks the OS for a free port, so Astro cannot silently move to another one.
async function freePort(): Promise<number> {
  const server = createServer();
  await new Promise<void>((done, fail) => {
    server.once('error', fail);
    server.listen(0, '127.0.0.1', done);
  });
  const address = server.address();
  await new Promise<void>((done) => server.close(() => done()));
  if (!address || typeof address === 'string') throw new Error('could not reserve a local port');
  return address.port;
}

async function drain(stream: ReadableStream<Uint8Array>): Promise<void> {
  const decoder = new TextDecoder();
  for await (const chunk of stream) {
    logs.push(decoder.decode(chunk, { stream: true }));
    if (logs.length > 500) logs.splice(0, logs.length - 500);
  }
}

const origin = `http://127.0.0.1:${await freePort()}`;

async function observe(route: string): Promise<Observation> {
  try {
    const response = await fetch(`${origin}${route}`, { signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS) });
    return { status: response.status, body: await response.text() };
  } catch (error) {
    return { error: error instanceof Error ? error.message : String(error) };
  }
}

function contains(observation: Observation, text: string): boolean {
  return observation.status === 200 && observation.body?.includes(text) === true;
}

function diagnostics(summary: string, route: string, last: Observation): string {
  const files = [firstPath, renamedPath, ...sentinels].map(
    (path) => `${basename(path)} ${existsSync(path) ? 'present' : 'absent'}`,
  );
  const response = last.error ? `request failed: ${last.error}` : `HTTP ${last.status}`;
  return [
    summary,
    `last response for ${route}: ${response}`,
    `watcher round trip: ${roundTripMs === undefined ? 'not measured' : seconds(roundTripMs)}; step deadline: ${seconds(stepTimeoutMs)}`,
    `probe files: ${files.join(', ')}`,
    `astro dev output (last ${LOG_LINES} lines):`,
    ...logs.join('').split('\n').slice(-LOG_LINES),
  ].join('\n');
}

await mkdir(docsDirectory, { recursive: true });
await rm(firstPath, { force: true });
await rm(renamedPath, { force: true });

// Astro 7 moves `astro dev` into a detached background server when it detects
// an AI agent, and reuses any server recorded in its lock file. The variable
// Astro sets for that background process keeps this one in the foreground,
// where this check owns and stops it; --ignore-lock keeps it independent of
// other dev servers and leaves no lock file behind.
const astro = Bun.spawn(
  [
    resolve(siteRoot, 'node_modules/.bin/astro'),
    'dev',
    '--host',
    '127.0.0.1',
    '--port',
    new URL(origin).port,
    '--ignore-lock',
  ],
  {
    cwd: siteRoot,
    env: { ...process.env, ASTRO_DEV_BACKGROUND: '1', LEANI_SITE_MODE: 'preview', LEANI_DOCS_REF: 'main' },
    stdout: 'pipe',
    stderr: 'pipe',
  },
);
void drain(astro.stdout);
void drain(astro.stderr);

// Polls until accept() holds; returns the elapsed milliseconds, or the last
// observation once the budget is spent.
async function poll(
  route: string,
  accept: (observation: Observation) => boolean,
  budgetMs: number,
): Promise<{ elapsedMs?: number; last: Observation }> {
  const started = Date.now();
  for (;;) {
    const last = await observe(route);
    if (accept(last)) return { elapsedMs: Date.now() - started, last };
    if (astro.exitCode !== null) {
      throw new Error(diagnostics(`astro dev exited with code ${astro.exitCode}`, route, last));
    }
    if (Date.now() - started >= budgetMs) return { last };
    await Bun.sleep(POLL_INTERVAL_MS);
  }
}

async function waitFor(
  description: string,
  route: string,
  accept: (observation: Observation) => boolean,
  budgetMs = stepTimeoutMs,
): Promise<number> {
  const { elapsedMs, last } = await poll(route, accept, budgetMs);
  if (elapsedMs !== undefined) return elapsedMs;
  throw new Error(diagnostics(`timed out after ${seconds(budgetMs)} waiting for ${description}`, route, last));
}

// The watcher may still be starting when the server first answers. Write
// fresh sentinel pages until one is served: that proves docs changes reach
// the server, and times the round trip.
async function awaitWatcher(): Promise<number> {
  const deadline = Date.now() + READINESS_TIMEOUT_MS;
  let route = '';
  let last: Observation = { error: 'no request completed' };
  for (let attempt = 1; Date.now() < deadline; attempt += 1) {
    const path = resolve(docsDirectory, `${probe}-ready-${attempt}.mdx`);
    const marker = `watcher ready ${attempt}`;
    route = `/docs/contributing/${probe}-ready-${attempt}/`;
    sentinels.push(path);
    await writeFile(path, document('Watcher readiness probe', marker), 'utf8');
    const budgetMs = Math.max(0, Math.min(SENTINEL_WINDOW_MS * attempt, deadline - Date.now()));
    const result = await poll(route, (observation) => contains(observation, marker), budgetMs);
    if (result.elapsedMs !== undefined) return result.elapsedMs;
    last = result.last;
  }
  throw new Error(
    diagnostics(`no sentinel page was served within ${seconds(READINESS_TIMEOUT_MS)}`, route, last),
  );
}

async function stop(): Promise<void> {
  astro.kill();
  const stopped = await Promise.race([
    astro.exited.then(() => true),
    Bun.sleep(STOP_TIMEOUT_MS).then(() => false),
  ]);
  if (!stopped) {
    astro.kill('SIGKILL');
    await astro.exited;
  }
}

try {
  const startupMs = await waitFor('the Astro dev server', '/', (observation) => observation.status === 200, STARTUP_TIMEOUT_MS);
  roundTripMs = await awaitWatcher();
  stepTimeoutMs = Math.min(MAX_STEP_TIMEOUT_MS, Math.max(MIN_STEP_TIMEOUT_MS, roundTripMs * STEP_TIMEOUT_FACTOR));
  console.log(
    `dev server answered after ${seconds(startupMs)}; watcher round trip ${seconds(roundTripMs)}; step deadline ${seconds(stepTimeoutMs)}`,
  );

  await writeFile(firstPath, document('Watcher add probe', 'add was observed'), 'utf8');
  await waitFor('the added docs route', firstRoute, (observation) => contains(observation, 'add was observed'));

  await writeFile(firstPath, document('Watcher edit probe', 'edit was observed'), 'utf8');
  await waitFor('the edited docs content', firstRoute, (observation) => contains(observation, 'edit was observed'));

  await rename(firstPath, renamedPath);
  await waitFor('the renamed docs route', renamedRoute, (observation) => contains(observation, 'edit was observed'));
  await waitFor('the old route to disappear', firstRoute, (observation) => observation.status === 404);

  await rm(renamedPath, { force: true });
  await waitFor('the deleted route to disappear', renamedRoute, (observation) => observation.status === 404);

  console.log('cross-root docs add/edit/rename/delete watching passed');
} finally {
  await Promise.all([firstPath, renamedPath, ...sentinels].map((path) => rm(path, { force: true })));
  await stop();
}
