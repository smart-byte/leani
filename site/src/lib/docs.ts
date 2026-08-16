export const DOC_SECTIONS = [
  'getting-started',
  'concepts',
  'guides',
  'operations',
  'reference',
  'contributing',
] as const;

export type DocSection = (typeof DOC_SECTIONS)[number];

export const DOC_SECTION_LABELS: Record<DocSection, string> = {
  'getting-started': 'Getting started',
  concepts: 'Concepts',
  guides: 'Guides',
  operations: 'Operations',
  reference: 'Reference',
  contributing: 'Contributing',
};

export const PUBLIC_DOC_PATTERNS = [
  'index.mdx',
  ...DOC_SECTIONS.map((section) => `${section}/**/*.{md,mdx}`),
  '!**/_*/**',
  '!**/_*',
] as const;

export function docIdFromEntry(entry: string): string {
  let path = entry.replaceAll('\\', '/').replace(/\.(?:md|mdx)$/i, '');
  path = path.replace(/(^|\/)index$/, '').replace(/^\/+|\/+$/g, '');
  return path ? `docs/${path}` : 'docs';
}

export function docHref(id: string): string {
  return `/${id.replace(/^\/+|\/+$/g, '')}/`;
}
