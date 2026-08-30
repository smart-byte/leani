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

Use `bun run check`, `bun run examples:check`, and `bun run build` before review.
`LEANI_SITE_MODE` and `LEANI_DOCS_REF` override automatically derived preview or
release metadata when reproducing CI locally.

Cloudflare Pages uses `site` as its root, `bun run build` as its command, and
`dist` as its output. `main` creates previews. Production follows the
release-pinned `site-production` branch after the promotion workflow validates
the tagged site itself.
