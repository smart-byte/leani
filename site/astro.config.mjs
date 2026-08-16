import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import { fileURLToPath } from 'node:url';

const repositoryRoot = fileURLToPath(new URL('../', import.meta.url));

export default defineConfig({
  site: 'https://leani.dev',
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
          replacement: fileURLToPath(
            new URL('./node_modules/@astrojs/starlight/components.ts', import.meta.url),
          ),
        },
      ],
    },
    server: {
      // Public docs intentionally live at the repository root. Polling is
      // required for reliable add/rename/delete discovery across this boundary
      // on macOS; edits alone work with Vite's default watcher.
      fs: { allow: [repositoryRoot] },
      watch: { usePolling: true, interval: 250 },
    },
  },
  integrations: [
    starlight({
      title: 'Leani',
      description: 'Run the Ethereum data processors your application needs, and keep only their durable results.',
      favicon: '/favicon.svg',
      pagefind: true,
      lastUpdated: true,
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
