import buildMetadata from './generated/build-metadata.json';

// Single source for public project links.
export const GITHUB_URL = 'https://github.com/smart-byte/leani';
export const DOCS_URL = '/docs/';
export const QUICKSTART_URL = '/docs/getting-started/';
export const LIMITATIONS_URL = '/docs/operations/preview-limitations/';
export const INSTALL_URL = '/docs/getting-started/install/';
export const EVIDENCE_URL = '/docs/contributing/manual-verification/#real-source-delivery-benchmark';
export const BLOBS_MONEY_URL = 'https://blobs.money';

// The published preview's install command, as docs/getting-started/install.mdx documents it.
export const HOMEBREW_INSTALL = 'brew install smart-byte/tap/leani';

export function sourceUrl(path: string, view: 'blob' | 'tree' | 'edit' = 'blob'): string {
  const encodedPath = path.split('/').map(encodeURIComponent).join('/');
  return `${GITHUB_URL}/${view}/${encodeURIComponent(buildMetadata.docsRef)}/${encodedPath}`;
}

// The site's operator, as smart-byte.com's legal notice lists it. Every page
// links the legal notice (§ 5 DDG) and the privacy policy (Art. 13 GDPR).
export const COMPANY = {
  name: 'Smart-Byte GmbH',
  url: 'https://www.smart-byte.com',
  street: 'Landshuter Str. 29',
  city: '85435 Erding',
  managingDirector: 'Mario Heubach',
  email: 'info@smart-byte.com',
  phone: '+49 8122 5676-141',
  register: 'HRB 225435',
  vatId: 'DE 305 932 873',
} as const;
export const LEGAL_NOTICE_URL = '/legal-notice/';
export const PRIVACY_URL = '/privacy/';
