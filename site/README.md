# Leani site

Astro landing page and Starlight documentation for the Leani monorepo.
Canonical content lives in `../docs`; verified snippets live in `../examples`.

```bash
cd site
bun install --frozen-lockfile
bun run dev
```

`docs:prepare` runs automatically before development and builds. It turns the
tracked Rust-owned fixtures under `../docs/reference/generated` plus the example
manifest into ignored render inputs under `src/generated`. Run
`bun run docs:update` after CLI, processor catalog, schema, or SDK contract
changes; that command requires the repository Rust toolchain. Normal site builds
require Bun only and make no network requests.

Brand exports come from `src/assets/leani-mark.svg`. Run `bun run brand:update`
after changing it to regenerate the SVG favicon, ICO, Apple touch icon, and
social-media avatar under `public`.

Use `bun run check`, `bun run examples:check`, and `bun run build` before review.
`LEANI_SITE_MODE` and `LEANI_DOCS_REF` override automatically derived preview or
release metadata when reproducing CI locally.

Cloudflare Pages project `leani` uses `site` as its root,
`bun install --frozen-lockfile && bun run build` as its command, and `dist` as
its output. `main` creates previews at `main.leani.pages.dev`. Production follows
the release-pinned `site-production` branch after the promotion workflow
validates the tagged site itself. The production domain is `leani.dev`, with
`leani.pages.dev` available for deployment checks.

Configure these Pages build variables:

| Variable | Production | Preview |
| --- | --- | --- |
| `BUN_VERSION` | Match `.bun-version` | Match `.bun-version` |
| `NODE_VERSION` | `24` | `24` |
| `SKIP_DEPENDENCY_INSTALL` | `1` | `1` |
| `LEANI_SITE_MODE` | `production` | `preview` |
| `LEANI_DOCS_REF` | Promoted release tag, initially `v0.1.0-rc.1` | `main` |

Set the production `LEANI_DOCS_REF` to the release being promoted before running
the promotion workflow. The explicit value avoids selecting an SDK tag when
the SDK and node tags point to the same commit. Git integration manages builds
without a Cloudflare token in GitHub Actions.
