export const DOC_SECTIONS = [
  'getting-started',
  'concepts',
  'guides',
  'operations',
  'reference',
  'contributing',
] as const;

export type DocSection = (typeof DOC_SECTIONS)[number];

// The alpha's default learning path. Other pages remain available in the
// expandable reference groups and search.
export const DOC_STARTER_PAGES = [
  { id: 'docs', label: 'Overview' },
  { id: 'docs/getting-started/install', label: 'Install Leani' },
  { id: 'docs/getting-started', label: 'Follow Ethereum blocks' },
  { id: 'docs/getting-started/bounded-mainnet', label: 'Index contract events' },
  { id: 'docs/guides/query-then-follow', label: 'Use the TypeScript SDK' },
  { id: 'docs/guides/postgres-consumer', label: 'Persist to PostgreSQL' },
  { id: 'docs/guides/custom-processor', label: 'Build a custom node in Rust' },
  { id: 'docs/operations/preview-limitations', label: 'Alpha limitations' },
] as const;

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
