import { readdir, readFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { relative, resolve } from 'node:path';
import {
  DOC_SECTIONS,
  docHref,
  docIdFromEntry,
  type DocSection,
} from '../../site/src/lib/docs';

const repositoryRoot = resolve(import.meta.dir, '../..');
const docsRoot = resolve(repositoryRoot, 'docs');
const siteRequire = createRequire(resolve(repositoryRoot, 'site/package.json'));
const { parse } = siteRequire('yaml') as { parse: (input: string) => unknown };
const publicRoots = [
  resolve(docsRoot, 'index.mdx'),
  ...DOC_SECTIONS.map((section) => resolve(docsRoot, section)),
];

interface PublicDoc {
  absolutePath: string;
  relativePath: string;
  id: string;
  href: string;
  title: string;
  section: DocSection;
  order: number;
  contents: string;
  anchors: Set<string>;
}

function markdownAnchors(contents: string): Set<string> {
  const anchors = new Set(['_top']);
  const counts = new Map<string, number>();
  for (const match of contents.matchAll(/^#{2,6}\s+(.+?)\s*#*$/gm)) {
    const base = match[1]!
      .replace(/<[^>]+>/g, '')
      .replace(/[`*_~]/g, '')
      .trim()
      .toLocaleLowerCase('en-US')
      .replace(/[^\p{L}\p{N}\s-]/gu, '')
      .replace(/\s+/g, '-')
      .replace(/-+/g, '-');
    if (!base) continue;
    const count = counts.get(base) ?? 0;
    counts.set(base, count + 1);
    anchors.add(count === 0 ? base : `${base}-${count}`);
  }
  return anchors;
}

function linksIn(contents: string): string[] {
  const prose = contents
    .replace(/```[\s\S]*?```/g, '')
    .replace(/`[^`\n]+`/g, '');
  return [
    ...Array.from(prose.matchAll(/\[[^\]]*\]\(([^)\s]+)(?:\s+[^)]*)?\)/g), (match) => match[1]!),
    ...Array.from(prose.matchAll(/\bhref=["']([^"']+)["']/g), (match) => match[1]!),
  ];
}

async function collectMarkdown(path: string): Promise<string[]> {
  const entries = await readdir(path, { withFileTypes: true }).catch((error: NodeJS.ErrnoException) => {
    if (error.code === 'ENOENT') return [];
    throw error;
  });
  const files: string[] = [];
  for (const entry of entries) {
    if (entry.name.startsWith('_') || entry.name.startsWith('.')) continue;
    const absolutePath = resolve(path, entry.name);
    if (entry.isDirectory()) files.push(...(await collectMarkdown(absolutePath)));
    else if (entry.isFile() && /\.mdx?$/i.test(entry.name)) files.push(absolutePath);
  }
  return files;
}

function frontmatterOf(contents: string, relativePath: string): Record<string, unknown> {
  const match = contents.match(/^---\r?\n([\s\S]*?)\r?\n---(?:\r?\n|$)/);
  if (!match) throw new Error(`${relativePath}: missing YAML frontmatter`);
  const data = parse(match[1]!) as unknown;
  if (!data || typeof data !== 'object' || Array.isArray(data)) {
    throw new Error(`${relativePath}: frontmatter must be a mapping`);
  }
  return data as Record<string, unknown>;
}

function requiredString(data: Record<string, unknown>, key: string, path: string): string {
  const value = data[key];
  if (typeof value !== 'string' || value.trim() === '') {
    throw new Error(`${path}: ${key} must be a non-empty string`);
  }
  return value.trim();
}

export async function validatePublicDocs(): Promise<PublicDoc[]> {
  const files: string[] = [];
  for (const path of publicRoots) {
    if (/\.mdx?$/i.test(path)) {
      await readFile(path, 'utf8');
      files.push(path);
    } else {
      files.push(...(await collectMarkdown(path)));
    }
  }
  files.sort();

  const docs: PublicDoc[] = [];
  const titles = new Map<string, string>();
  const routes = new Map<string, string>();
  const orders = new Map<string, string>();

  for (const absolutePath of files) {
    const relativePath = relative(docsRoot, absolutePath).replaceAll('\\', '/');
    const contents = await readFile(absolutePath, 'utf8');
    const data = frontmatterOf(contents, relativePath);
    const title = requiredString(data, 'title', relativePath);
    requiredString(data, 'description', relativePath);
    const section = requiredString(data, 'section', relativePath) as DocSection;
    if (!DOC_SECTIONS.includes(section)) {
      throw new Error(`${relativePath}: unknown section ${JSON.stringify(section)}`);
    }
    const order = data.order;
    if (!Number.isInteger(order) || (order as number) < 0) {
      throw new Error(`${relativePath}: order must be a non-negative integer`);
    }

    const titleKey = title.toLocaleLowerCase('en-US');
    const duplicateTitle = titles.get(titleKey);
    if (duplicateTitle) {
      throw new Error(`${relativePath}: duplicate title ${JSON.stringify(title)} (also in ${duplicateTitle})`);
    }
    titles.set(titleKey, relativePath);

    const id = docIdFromEntry(relativePath);
    const href = docHref(id);
    const duplicateRoute = routes.get(href);
    if (duplicateRoute) {
      throw new Error(`${relativePath}: duplicate route ${href} (also in ${duplicateRoute})`);
    }
    routes.set(href, relativePath);

    const orderKey = `${section}:${order}`;
    const duplicateOrder = orders.get(orderKey);
    if (duplicateOrder) {
      throw new Error(`${relativePath}: duplicate order ${order} in ${section} (also in ${duplicateOrder})`);
    }
    orders.set(orderKey, relativePath);

    if (/\/(?:Users|home)\/[^\s)`"']+/.test(contents)) {
      throw new Error(`${relativePath}: contains a local absolute home path`);
    }

    docs.push({
      absolutePath,
      relativePath,
      id,
      href,
      title,
      section,
      order: order as number,
      contents,
      anchors: markdownAnchors(contents),
    });
  }

  if (docs.length === 0) throw new Error('no public documentation pages were found');

  const byHref = new Map(docs.map((doc) => [doc.href, doc]));
  for (const doc of docs) {
    for (const link of linksIn(doc.contents)) {
      if (/^(?:https?:|mailto:|tel:)/.test(link)) continue;
      const resolved = new URL(link, `https://leani.dev${doc.href}`);
      if (!resolved.pathname.startsWith('/docs')) continue;
      const href = resolved.pathname.endsWith('/') ? resolved.pathname : `${resolved.pathname}/`;
      const target = byHref.get(href);
      if (!target) throw new Error(`${doc.relativePath}: broken internal documentation link ${link}`);
      const fragment = decodeURIComponent(resolved.hash.slice(1));
      if (fragment && !target.anchors.has(fragment)) {
        throw new Error(`${doc.relativePath}: missing fragment ${resolved.hash} in ${target.relativePath}`);
      }
    }
  }

  return docs;
}

if (import.meta.main) {
  const docs = await validatePublicDocs();
  console.log(`validated ${docs.length} public documentation pages`);
}
