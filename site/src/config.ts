import buildMetadata from './generated/build-metadata.json';

// Single source for public project links.
export const GITHUB_URL = 'https://github.com/smart-byte/leani';
export const DOCS_URL = '/docs/';
export const QUICKSTART_URL = '/docs/getting-started/offline-to-mainnet/';
export const LIMITATIONS_URL = '/docs/operations/preview-limitations/';
export const EVIDENCE_URL = '/docs/contributing/manual-verification/#real-source-delivery-benchmark';
export const BLOBS_MONEY_URL = 'https://blobs.money';

export function sourceUrl(path: string, view: 'blob' | 'tree' = 'blob'): string {
  const encodedPath = path.split('/').map(encodeURIComponent).join('/');
  return `${GITHUB_URL}/${view}/${encodeURIComponent(buildMetadata.docsRef)}/${encodedPath}`;
}
