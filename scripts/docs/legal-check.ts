// German law requires the legal notice (§ 5 DDG) and the privacy policy
// (Art. 13 GDPR) to be reachable from every page, and loading resources from
// third-party hosts discloses visitors' IP addresses to them (LG München I,
// 3 O 17493/20). Every built page must link both documents and fetch nothing
// on load from hosts other than the site and its self-hosted analytics.
import { readdir, readFile } from 'node:fs/promises';
import { extname, relative, resolve } from 'node:path';
import { UMAMI_ORIGIN } from '../../site/src/lib/analytics';

const dist = resolve(import.meta.dir, '../../site/dist');
const allowedOrigins = new Set(['https://leani.dev', UMAMI_ORIGIN]);
const requiredLinks = ['/legal-notice/', '/privacy/'];

async function filesUnder(path: string): Promise<string[]> {
  const files: string[] = [];
  for (const entry of await readdir(path, { withFileTypes: true })) {
    const child = resolve(path, entry.name);
    if (entry.isDirectory()) files.push(...(await filesUnder(child)));
    else if (entry.isFile()) files.push(child);
  }
  return files;
}

// URLs a browser fetches while rendering: src attributes, stylesheet-like
// links, and CSS url()/@import. Navigation links and meta tags fetch nothing.
function loadedUrls(text: string): string[] {
  const urls = [
    ...Array.from(text.matchAll(/\ssrc="(https?:\/\/[^"]+)"/g), (match) => match[1]!),
    ...Array.from(text.matchAll(/url\(\s*['"]?(https?:\/\/[^'")\s]+)/g), (match) => match[1]!),
    ...Array.from(text.matchAll(/@import\s+['"](https?:\/\/[^'"]+)/g), (match) => match[1]!),
  ];
  for (const [tag] of text.matchAll(/<link\b[^>]*>/g)) {
    if (!/\srel="[^"]*\b(stylesheet|preload|modulepreload|preconnect|dns-prefetch|icon|manifest)\b/.test(tag)) continue;
    const href = /\shref="(https?:\/\/[^"]+)"/.exec(tag)?.[1];
    if (href) urls.push(href);
  }
  return urls;
}

const failures: string[] = [];
let pages = 0;
for (const file of await filesUnder(dist)) {
  const extension = extname(file);
  if (extension !== '.html' && extension !== '.css') continue;
  const text = await readFile(file, 'utf8');
  const path = relative(dist, file);
  if (extension === '.html') {
    pages += 1;
    for (const link of requiredLinks) {
      if (!text.includes(`href="${link}"`)) failures.push(`${path}: no link to ${link}`);
    }
  }
  for (const url of loadedUrls(text)) {
    if (!allowedOrigins.has(new URL(url).origin)) failures.push(`${path}: loads ${url}`);
  }
}

if (failures.length) {
  throw new Error(`legal check failed:\n${failures.slice(0, 30).join('\n')}${failures.length > 30 ? `\n… ${failures.length - 30} more` : ''}`);
}
console.log(`legal links and first-party loading verified on ${pages} pages`);
