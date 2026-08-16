import { mkdir, rename, rm, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';

const repositoryRoot = resolve(import.meta.dir, '../..');
const siteRoot = resolve(repositoryRoot, 'site');
const docsDirectory = resolve(repositoryRoot, 'docs/contributing');
const probe = `watch-probe-${process.pid}`;
const firstPath = resolve(docsDirectory, `${probe}.mdx`);
const renamedPath = resolve(docsDirectory, `${probe}-renamed.mdx`);
const firstRoute = `/docs/contributing/${probe}/`;
const renamedRoute = `/docs/contributing/${probe}-renamed/`;
const port = 4400 + (process.pid % 1000);
const origin = `http://127.0.0.1:${port}`;
const logs: string[] = [];

function document(title: string, body: string): string {
  return `---\ntitle: ${title}\ndescription: Temporary cross-root watcher integration probe.\nsection: contributing\norder: 99999\nstatus: preview\n---\n\n${body}\n`;
}

async function waitFor(description: string, predicate: () => Promise<boolean>): Promise<void> {
  const deadline = Date.now() + 25_000;
  while (Date.now() < deadline) {
    if (await predicate().catch(() => false)) return;
    await Bun.sleep(200);
  }
  throw new Error(`timed out waiting for ${description}\n${logs.slice(-30).join('')}`);
}

async function response(path: string): Promise<Response | undefined> {
  return fetch(`${origin}${path}`).catch(() => undefined);
}

async function contains(path: string, text: string): Promise<boolean> {
  const result = await response(path);
  return result?.status === 200 && (await result.text()).includes(text);
}

async function drain(stream: ReadableStream<Uint8Array> | null): Promise<void> {
  if (!stream) return;
  for await (const chunk of stream) logs.push(new TextDecoder().decode(chunk));
}

await mkdir(docsDirectory, { recursive: true });
await rm(firstPath, { force: true });
await rm(renamedPath, { force: true });

const astro = Bun.spawn(
  [resolve(siteRoot, 'node_modules/.bin/astro'), 'dev', '--host', '127.0.0.1', '--port', String(port)],
  {
    cwd: siteRoot,
    env: { ...process.env, LEANI_SITE_MODE: 'preview', LEANI_DOCS_REF: 'main' },
    stdout: 'pipe',
    stderr: 'pipe',
  },
);
void drain(astro.stdout);
void drain(astro.stderr);

try {
  await waitFor('Astro dev server', async () => (await response('/'))?.status === 200);

  await writeFile(firstPath, document('Watcher add probe', 'add was observed'), 'utf8');
  await waitFor('added docs route', () => contains(firstRoute, 'add was observed'));

  await writeFile(firstPath, document('Watcher edit probe', 'edit was observed'), 'utf8');
  await waitFor('edited docs content', () => contains(firstRoute, 'edit was observed'));

  await rename(firstPath, renamedPath);
  await waitFor('renamed docs route', () => contains(renamedRoute, 'edit was observed'));
  await waitFor('old route removal', async () => (await response(firstRoute))?.status === 404);

  await rm(renamedPath, { force: true });
  await waitFor('deleted route removal', async () => (await response(renamedRoute))?.status === 404);

  console.log('cross-root docs add/edit/rename/delete watching passed');
} finally {
  await rm(firstPath, { force: true });
  await rm(renamedPath, { force: true });
  astro.kill();
  await astro.exited;
}
