---
title: Release process
description: Cut and verify coordinated Leani binaries, crates, SDK, container, Homebrew, and release-pinned documentation.
section: contributing
order: 60
audience:
  - contributor
status: preview
---

1. Complete the release-readiness checklist and attach the local evidence report.
2. Run the full Rust, SDK, dependency-policy, migration, backup/restore, and
   reproducibility suites from a clean checkout.
3. Update `CHANGELOG.md`, OpenAPI/SDK compatibility tables, durable schema
   versions, and processor version declarations.
4. Build Linux `x86_64` and `aarch64` artifacts and the OCI image from the same
   signed commit.
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
the archives, checksums, SBOM, and Homebrew formula.

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
versions use `latest`. The first package publication and npm trusted-publisher
configuration must be completed by a maintainer with access to the scope.

The container publisher requires the matching node release tag, its current
successful image scan, and passing Rust, security, and site workflows. Verify
both published architectures on clean hosts before announcing the release.

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

The first publication requires a crates.io account with a verified email and
an API token configured locally through `cargo login`. Publish from the clean,
reviewed release commit using the same explicit package selection without
`--dry-run`. Dependency order is primitives, the processor/source APIs, then
testkit. Add the intended maintainers or GitHub team as owners afterward.
Configure [trusted publishing](https://crates.io/docs/trusted-publishing) for
subsequent releases once the crates exist.

The full `leani` node is distributed through native archives, Homebrew, and
containers. Its Git-only networking and consensus dependencies prevent
publication on crates.io. Publishing the authoring libraries does not enable
dynamic processor loading; custom native processors are compiled into a node.

Rollback means deploying the previous binary with a store format it supports
or restoring the pre-migration backup. Every irreversible migration must be
preceded by a tested export/rebuild path and an ADR.
