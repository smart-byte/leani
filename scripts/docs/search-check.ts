import { readdir, readFile, stat } from 'node:fs/promises';
import { resolve } from 'node:path';

const repositoryRoot = resolve(import.meta.dir, '../..');
const dist = resolve(repositoryRoot, 'site/dist');

async function filesUnder(path: string): Promise<string[]> {
  const files: string[] = [];
  for (const entry of await readdir(path, { withFileTypes: true })) {
    const child = resolve(path, entry.name);
    if (entry.isDirectory()) files.push(...(await filesUnder(child)));
    else if (entry.isFile()) files.push(child);
  }
  return files;
}

for (const required of [
  'pagefind/pagefind.js',
  'pagefind/pagefind-entry.json',
  'docs/index.html',
  'docs/getting-started/index.html',
]) {
  const path = resolve(dist, required);
  if (!(await stat(path).catch(() => undefined))?.isFile()) {
    throw new Error(`missing built search/docs asset: ${required}`);
  }
}

const files = await filesUnder(dist);
const html = (
  await Promise.all(
    files.filter((path) => path.endsWith('.html')).map((path) => readFile(path, 'utf8')),
  )
).join('\n');

for (const term of ['configuration', '@leani/sdk', 'JSON-RPC', 'processor']) {
  if (!html.toLocaleLowerCase('en-US').includes(term.toLocaleLowerCase('en-US'))) {
    throw new Error(`representative search term is absent from built documentation: ${term}`);
  }
}

for (const forbidden of ['/.private/', '/AGENTS.md']) {
  if (html.includes(forbidden)) {
    throw new Error(`private or local-only marker leaked into built HTML: ${forbidden}`);
  }
}

const pagefindFiles = files.filter((path) => path.startsWith(resolve(dist, 'pagefind')));
if (pagefindFiles.length < 4) throw new Error('Pagefind produced an unexpectedly small index');

console.log(`validated static Pagefind output (${pagefindFiles.length} files)`);
