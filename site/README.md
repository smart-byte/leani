# Leani site

Landing page and documentation frontend for Leani. This package lives in the
`site/` directory of the `smart-byte/leani` monorepo and builds as an Astro
static site.

- `bun install`
- `bun run dev` — local dev at http://localhost:4321
- `bun run check` — astro check + unit tests
- `bun run build` — static output in `dist/`

Deploy: Cloudflare Pages with root directory `site`, build command
`bun run build`, and output directory `dist`. Pull requests and `main` are
preview-only; production is built from the release-pinned `site-production`
branch.

## Launch contract

- [x] Node binary, crates, SDK API, metrics, and deployment assets use the
      Leani identity and the `leani` command.
- [x] Checksummed macOS artifacts exist for both Intel and Apple silicon.
- [x] Homebrew tap `leani-dev/tap/leani` is published (hero + quickstart
      show `brew install`).
- [x] SDK published to npm as `@leani/sdk` (hero SDK tab imports it; the
      package in `packages/sdk` must ship under that name).
- [x] `GITHUB_URL` in `src/config.ts` points at the public
      `smart-byte/leani` repository; processor documentation links derive from it.
- [x] `leani.dev` is the canonical production domain in `astro.config.mjs`.
- [x] Processor snippets use current IDs, versions, lifecycle tables, and
      change-envelope provenance fields.
- [x] Storage claims cite bounded benchmark evidence and do not generalize a
      workload-specific database size into a host or cost promise.

Leani remains a public preview. Production-readiness, trust, RPC-availability,
and security limitations must stay visible even when distribution artifacts are
published.

Historical data shown in demos: [Xatu by ethPandaOps](https://github.com/ethpandaops/xatu-data), CC BY 4.0.
