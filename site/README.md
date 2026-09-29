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

The pinned `typescript` 5.9 package provides the JavaScript compiler API used
by Astro and the executable documentation generator. TypeScript examples use
the native TypeScript 7 compiler through `@typescript/native`. Keep these
dependencies separate until the API consumers support a newer compiler API.

Use `bun run check`, `bun run examples:check`, and `bun run build` before review.
`bun run docs:watch-check` verifies that the dev server picks up added, edited,
renamed, and deleted docs. It runs its own foreground dev server on a free
port, beside any other dev server.
`LEANI_SITE_MODE` and `LEANI_DOCS_REF` override automatically derived preview or
release metadata when reproducing CI locally. Without `LEANI_SITE_MODE`, only
a `v`-prefixed release tag on the checked-out commit or the `site-production`
branch selects production; an SDK tag does not.

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
| `LEANI_DOCS_REF` | Promoted release tag, initially `v0.1.0-rc.1` | Unset (branch-derived) |

Set the production `LEANI_DOCS_REF` to the release being promoted before running
the promotion workflow. A production build fails unless its documentation ref,
`LEANI_DOCS_REF` or else the commit's exact `v*` tag, equals `v` followed by the
workspace version, so a stale value fails the deployment instead of linking to
an earlier release. Leave the preview value unset: each preview then links to
its own branch, `main` for the main preview. Git integration manages builds
without a Cloudflare token in GitHub Actions.
