import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import starlightLlmsTxt from 'starlight-llms-txt';
import { fileURLToPath } from 'node:url';
import { DOC_SECTIONS, DOC_SECTION_LABELS, DOC_STARTER_PAGES } from './src/lib/docs';
import { SOCIAL_IMAGE } from './src/lib/social';

const site = 'https://leani.dev';

const repositoryRoot = fileURLToPath(new URL('../', import.meta.url));

export default defineConfig({
  site,
  vite: {
    resolve: {
      // MDX resolves bare imports from the canonical root docs directory,
      // which is a sibling of site/node_modules rather than its parent.
      alias: [
        {
          find: /^@docs\/(.*)$/,
          replacement: fileURLToPath(new URL('./src/$1', import.meta.url)),
        },
        {
          find: /^@astrojs\/starlight\/components$/,
          replacement: fileURLToPath(import.meta.resolve('@astrojs/starlight/components')),
        },
      ],
    },
    server: {
      // Public docs intentionally live at the repository root. Polling is
      // required for reliable add/rename/delete discovery across this boundary
      // on macOS; edits alone work with Vite's default watcher.
      fs: { allow: [repositoryRoot] },
      // Astro rewrites its content store through temporary files and rename.
      // Normalize those atomic writes into change events while polling.
      watch: { usePolling: process.platform === 'darwin', interval: 250, atomic: true },
    },
  },
  integrations: [
    starlight({
      title: 'Leani',
      description: 'Run the Ethereum data processors your application needs, and keep only their durable results.',
      favicon: '/favicon.svg',
      logo: {
        src: './src/assets/leani-mark.svg',
        alt: 'Leani',
      },
      pagefind: true,
      // Starlight already declares a large Twitter card; give it the image.
      head: [
        ['og:image', new URL(SOCIAL_IMAGE.path, site).href],
        ['og:image:width', String(SOCIAL_IMAGE.width)],
        ['og:image:height', String(SOCIAL_IMAGE.height)],
        ['og:image:alt', SOCIAL_IMAGE.alt],
      ].map(([property, content]) => ({ tag: 'meta', attrs: { property, content } })),
      plugins: [
        // Serves /llms.txt, /llms-full.txt, and /llms-small.txt for AI agents.
        starlightLlmsTxt({
          details: [
            'Leani is alpha software built from source; the `leani` CLI is the primary interface.',
            '',
            '- `leani subscribe blocks` follows Ethereum Mainnet blocks over P2P with no API key and runs until interrupted. Add `--json` for NDJSON on stdout (progress goes to stderr), `--once` to exit after the first verified result, and `--timeout 5m` to bound the run; an unsatisfied `--once` exits 124 on timeout.',
            '- First use asks the user to accept a checkpoint, the trust anchor for verification. Without a terminal the command fails unless `--yes` is passed. Agents should pass `--yes` only after the user has approved the checkpoint.',
            '- Rows are labelled `preview` (unverified peer data shown during embedded startup), `included` (can still be reorged), or `finalized`. `--once` never returns a preview unless `--finality preview` is passed. JSON output mixes row schemas, so dispatch on each row\'s `schema` field.',
            '- `leani init blocks` then `leani serve` runs a reusable node: HTTP API at http://127.0.0.1:18080, JSON-RPC subset at http://127.0.0.1:18545.',
          ].join('\n'),
          promote: DOC_STARTER_PAGES.map((page) => page.id),
          demote: ['docs/contributing/**'],
          exclude: ['docs/contributing/**'],
          customSets: DOC_SECTIONS.filter((section) => section !== 'contributing').map((section) => ({
            label: DOC_SECTION_LABELS[section],
            paths: [`docs/${section}`, `docs/${section}/**`],
          })),
        }),
      ],
      social: [
        {
          icon: 'github',
          label: 'Leani on GitHub',
          href: 'https://github.com/smart-byte/leani',
        },
      ],
      customCss: [
        '@fontsource-variable/inter',
        '@fontsource/jetbrains-mono/400.css',
        '@fontsource/jetbrains-mono/700.css',
        './src/styles/docs.css',
      ],
      components: {
        Banner: './src/components/docs/PreviewBanner.astro',
        EditLink: './src/components/docs/EditLink.astro',
        Pagination: './src/components/docs/Pagination.astro',
        Sidebar: './src/components/docs/Sidebar.astro',
      },
    }),
  ],
});
