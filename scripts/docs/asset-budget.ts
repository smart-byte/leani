import { readdir, readFile, stat } from 'node:fs/promises';
import { extname, resolve } from 'node:path';

const repositoryRoot = resolve(import.meta.dir, '../..');
const dist = resolve(repositoryRoot, 'site/dist');

interface Asset {
  path: string;
  bytes: number;
}

async function collect(directory: string, prefix = ''): Promise<Asset[]> {
  const assets: Asset[] = [];
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const path = prefix ? `${prefix}/${entry.name}` : entry.name;
    const absolute = resolve(directory, entry.name);
    if (entry.isDirectory()) assets.push(...(await collect(absolute, path)));
    else if (entry.isFile()) assets.push({ path, bytes: (await stat(absolute)).size });
  }
  return assets;
}

function total(assets: Asset[], extensions: string[]): number {
  return assets
    .filter((asset) => extensions.includes(extname(asset.path)))
    .reduce((sum, asset) => sum + asset.bytes, 0);
}

function enforce(name: string, actual: number, maximum: number): void {
  if (actual > maximum) {
    throw new Error(`${name} is ${actual} bytes; budget is ${maximum} bytes`);
  }
}

const allAssets = await collect(dist);
// Agent-facing llms.txt exports never load in a browser, so they get their own budget.
const isLlmsText = (asset: Asset) => /^(_llms-txt\/|llms[^/]*\.txt$)/.test(asset.path);
const assets = allAssets.filter((asset) => !isLlmsText(asset));
const llmsText = allAssets.filter(isLlmsText);
const html = assets.filter((asset) => extname(asset.path) === '.html');
const landing = await readFile(resolve(dist, 'index.html'), 'utf8');
// The analytics tag loads deferred from its own origin; this budget covers the site's own bundles.
const entryScripts = Array.from(landing.matchAll(/<script[^>]+src="([^"]+)"/g), (match) => match[1]!)
  .filter((src) => !/^https?:\/\//.test(src))
  .map((src) => src.replace(/^\//, ''));
const landingScriptBytes = entryScripts.reduce((sum, path) => {
  const asset = assets.find((candidate) => candidate.path === path);
  if (!asset) throw new Error(`landing entry script is missing from dist: ${path}`);
  return sum + asset.bytes;
}, 0);

enforce('complete static site', assets.reduce((sum, asset) => sum + asset.bytes, 0), 5 * 1024 * 1024);
enforce('llms.txt exports', llmsText.reduce((sum, asset) => sum + asset.bytes, 0), 2 * 1024 * 1024);
enforce('all JavaScript including Pagefind', total(assets, ['.js']), 768 * 1024);
enforce('all CSS including Pagefind', total(assets, ['.css']), 256 * 1024);
enforce('all webfonts', total(assets, ['.woff', '.woff2']), 512 * 1024);
// The hero repeats section 3's engine markup, which gzip mostly deduplicates.
enforce('landing HTML', Buffer.byteLength(landing), 192 * 1024);
enforce('landing entry JavaScript', landingScriptBytes, 16 * 1024);
for (const asset of html) enforce(`HTML route ${asset.path}`, asset.bytes, 256 * 1024);

console.log(
  `asset budgets pass (${assets.length} files, ${landingScriptBytes} landing JS bytes, ${Buffer.byteLength(landing)} landing HTML bytes)`,
);
