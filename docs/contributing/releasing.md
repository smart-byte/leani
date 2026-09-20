---
title: Release process
description: Cut and verify coordinated Leani binaries, crates, SDK, container, Homebrew, and release-pinned documentation.
section: contributing
order: 60
audience:
  - contributor
status: preview
---

1. Keep release verification results in ignored local storage.
2. Run the Rust, SDK, dependency-policy, and publication checks from a clean
   checkout. Test restart, replay, and backup/restore with the packaged binary.
3. Update `CHANGELOG.md`, OpenAPI/SDK compatibility tables, durable schema
   versions, and processor version declarations.
4. Build Linux and macOS `x86_64` and `aarch64` artifacts and both Linux container
   architectures from the same reviewed commit.
5. Verify checksums and a clean-host smoke test.
6. Create an annotated semantic-version tag. Release candidates use
   `-rc.N`; a stable release requires every artifact listed above.

## Verify publication history

Install the local commit and push guards as described in `CONTRIBUTING.md`.
Run these checks on the exact commit being released:

```bash
scripts/check-publication.rb HEAD
scripts/check-secrets.sh HEAD
```

The first check examines every reachable commit, including files deleted in
later commits. The second uses a pinned credential scanner with redacted
output. Private plans, agent instructions, design explorations, and raw reports
must stay in ignored local storage. Do not push recovery or archive refs.

## Prepare and publish binaries

Run `Prepare release` with the version matching the workspace manifest and
`publish=false`. It executes the offline fixture on native Linux and macOS
runners for both architectures, then uploads a release candidate containing
the archives, checksums, portable SBOM files, dependency license notices, and
Homebrew formula. Build paths are remapped and the extracted binaries are
checked for private content before their fixture tests run.

Inspect and test that candidate. Once the exact commit is tagged and its CI,
publication/security, site, and container checks have passed, run the workflow
from the tag with `publish=true` and the successful preparation run's numeric
`candidate_run_id`. The publisher verifies that the candidate belongs to the
same commit and downloads its existing assets; it does not rebuild them.
Candidates expire after 14 days, so prepare a new candidate if necessary.

If a required workflow has no run for the release commit because of path
filters, dispatch it explicitly on the release ref. A successful run for an
older commit does not satisfy the gate.

Release assets are attached to a draft before it is published. Prerelease
versions are marked as prereleases and do not replace the latest stable
release. Enable GitHub release immutability in repository settings if that
protection is required; the workflow name alone does not enable it.

## SDK, container, Homebrew, and site

The SDK workflow requires `sdk-v<package-version>` for publication and passing
CI for the same commit. Prerelease SDK versions use npm's `next` tag; stable
versions use `latest`. The workflow tests and scans a local npm archive, then
publishes that archive. A dry run uploads it for manual inspection.

For the first publication, a maintainer with access to the `@leani` scope must
configure a short-lived granular `NPM_TOKEN` repository secret with publication
access and permission to bypass 2FA. Dispatch `sdk-release.yml` from the SDK tag
with `publish=true` and `bootstrap=true`. The GitHub repository must be public
for npm provenance. Once the package exists, configure its npm trusted publisher:
GitHub owner `smart-byte`, repository `leani`, workflow `sdk-release.yml`, and
permission to publish directly with `npm publish`. Subsequent publications use
`bootstrap=false` and need no npm secret. Revoke the bootstrap token and delete
the GitHub secret after switching. See [npm trusted publishing](https://docs.npmjs.com/trusted-publishers/).

The container workflow builds, runs, scans, and saves both architectures on
native runners. Publication requires the matching node release tag, that
successful preparation run's `candidate_run_id`, and passing Rust, security,
and site workflows. It loads and verifies the saved images before publishing
them, then assembles the multiarchitecture tag from their registry digests.
GitHub's automatic `GITHUB_TOKEN` supplies GHCR authentication. Verify anonymous
pulls for both architectures before announcing the release.

Copy the candidate's `leani.rb` into the Homebrew tap only after its public
archive URLs exist. Run the tap's installation and formula tests. The site
promotion workflow requires a published GitHub release before advancing
`site-production` to the exact tagged commit.

## Rust libraries

The crates.io release surface is `leani-primitives`, `leani-processor-api`,
`leani-source-api`, and `leani-testkit`. Other workspace crates have publication
disabled. Keep these libraries on the workspace release version; their internal
prerelease dependencies use exact version requirements.

Use the repository's pinned Cargo version to rehearse all four packages together:

```bash
cargo publish --dry-run --locked \
  -p leani-primitives \
  -p leani-processor-api \
  -p leani-source-api \
  -p leani-testkit
```

Cargo verifies the packaged sources against a temporary registry containing
the selected local packages. This allows verification before the first upload.
Inspect each archive and its normalized manifest, license, README, lockfile,
and source contents. Run the publication/privacy checks and credential scanner
on the exact release commit and scan the extracted archives too. Validate a
standalone consumer against the packages, then against crates.io after upload.

Run `crates-release.yml` with `publish=false` to test, package, and inspect all
four libraries in CI. It checks their clean commit provenance, MIT license,
archive paths, and contents before uploading the dry-run packages.

The first publication requires a crates.io account with a verified email and
an API token with permission to create the four crates. Store it as the
`CARGO_REGISTRY_TOKEN` repository secret, then dispatch `crates-release.yml`
from the node release tag with `publish=true` and `bootstrap=true`. Cargo
publishes the explicit package selection in dependency order. Add the intended
maintainers or GitHub team as owners afterward.

Configure [trusted publishing](https://crates.io/docs/trusted-publishing) on
each crate with GitHub owner `smart-byte`, repository `leani`, and workflow
`crates-release.yml`. Subsequent publications use `bootstrap=false`; the
official crates.io authentication action supplies a short-lived token. Revoke
the bootstrap token and remove its GitHub secret after switching.

The full `leani` node is distributed through native archives, Homebrew, and
containers. Its Git-only networking and consensus dependencies prevent
publication on crates.io. Publishing the authoring libraries does not enable
dynamic processor loading; custom native processors are compiled into a node.

Rollback means deploying the previous binary with a store format it supports
or restoring the pre-migration backup. Every irreversible migration must be
preceded by a tested export/rebuild path and an ADR.
