---
title: Release process
description: How Leani builds, protects, and publishes its binaries, crates, SDK, container, and release-pinned documentation from one reviewed commit.
section: contributing
order: 60
audience:
  - contributor
status: preview
---

Every Leani release publishes native binaries, four Rust authoring libraries,
the TypeScript SDK, a multiarchitecture container, a Homebrew formula, and
release-pinned documentation from one reviewed commit. This page explains how
releases work and what contributors must keep intact. Users check a release
with [Verify a release](/docs/operations/verify-releases/). Release maintainers
follow the [maintainer runbook](https://github.com/smart-byte/leani/blob/main/docs/maintainers/releasing.md)
for repository settings and publication steps.

## Release flow

The manually dispatched **Coordinate release** workflow
(`release-coordinate.yml`) runs from `main` in two steps:

1. **prepare** opens a pull request that updates workspace and SDK versions,
   exact internal crate requirements, lockfile versions, license notices,
   current install pins, and the changelog heading. It is reviewed and merged
   like any other change.
2. **release** runs after one maintainer approval. It reserves annotated
   `v<version>` and `sdk-v<version>` tags on the merged commit, which are never
   moved, builds and tests every artifact once, and records the runs and
   checksums in a release plan. It then dispatches the publishers in their
   protected environments, checks each publication against the plan, promotes
   the documentation site, and waits until leani.dev serves the release.
   Publication is resumable rather than atomic: a retry reuses matching
   publications, and different bytes stop it.

## Release-facing changes

Update `CHANGELOG.md`, OpenAPI/SDK compatibility tables, durable schema
versions, and processor version declarations with the change that needs them.
When the SDK version in `packages/sdk/package.json` changes, pin every
documented SDK install, such as the `bun add` commands in `README.md` and the
install guide, to that exact version in the same commit. CI's
`scripts/check-sdk-pins.rb` fails on a documented install that names another
version, a range, a tag, or none.

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

## Protected publication

Every publishing workflow separates building from publishing. Its build and
test jobs run with a read-only token, without secrets or an OIDC token, and
upload the artifact they tested. One publish job per workflow runs in a
protected GitHub environment, downloads that artifact, and publishes it
without building or installing project code:

| Workflow | Environment | Publish job permissions | Dispatch from |
| --- | --- | --- | --- |
| `release.yml` | `release-github` | `contents: write`, `actions: read`, `id-token: write`, `attestations: write` | `v*` tag |
| `container.yml` | `release-ghcr` | `packages: write`, `actions: read`, `id-token: write`, `attestations: write` | `v*` tag |
| `crates-release.yml` | `release-crates` | `contents: read`, `actions: read`, `id-token: write` | `v*` tag |
| `sdk-release.yml` | `release-npm` | `actions: read`, `id-token: write` | `sdk-v*` tag |
| `site-promote.yml` | `site-production` | `contents: write` | `v*` tag |
| `release-coordinate.yml` preparation PR | `release-control` | `contents: read` | `main` |
| `release-coordinate.yml` coordination | `release-control` | `contents: read`, `actions: read` | `main` |

No other job may request a write permission, `id-token: write` included, read
a secret, or keep its checkout credentials. The crates.io publish job checks out
the repository because Cargo cannot upload a prepared archive. The coordinator
checks out reviewed control scripts, without building or installing project
dependencies, and writes only with its `RELEASE_CONTROL_TOKEN` secret. npm
publishes a local archive file named right after `publish`. No workflow
may run on `pull_request_target` or `workflow_run`: both run with the base
repository's token, secrets, and cache on behalf of events a pull request
controls.
`ruby scripts/check-release-ci.rb --workflows` enforces this, and CI runs it
through `scripts/test-release-ci.rb`. Change its policy table only together
with this page.

## Provenance and dependency updates

Release workflows attest the build provenance of every archive and container
image. CycloneDX SBOMs ship with the GitHub release for archives, whose
publication stops if the candidate lacks the node's SBOM, `leani.cdx.json`.
Container SBOMs are bound to each image digest instead, so they verify against
the exact image and the container job needs no `contents: write`.
[Verify a release](/docs/operations/verify-releases/) shows how to check them.

The Dockerfile pins its base images to multiarchitecture digests. Dependabot
proposes weekly updates for GitHub Actions, Cargo, Bun (`packages/sdk` and
`site`), and those image digests. CI requires the Dockerfile's Rust image to
match `rust-toolchain.toml`, so merge a Rust image update only together with
the same toolchain update. The weekly `Dependency audit` workflow runs
the cargo-deny advisory check and `bun audit` against both Bun lockfiles.
Releases do not wait for it, so review its failures when they arrive.

Linux archives are built on Ubuntu 24.04, and their build fails if a binary
needs a newer glibc than the documented floor, 2.39. Raising the floor changes
the [supported distributions](/docs/getting-started/install/#published-releases).

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

The full `leani` node is distributed through native archives, Homebrew, and
containers. Its Git-only networking and consensus dependencies prevent
publication on crates.io. Publishing the authoring libraries does not enable
dynamic processor loading; custom native processors are compiled into a node.

## Rollback and migrations

Rollback means deploying the previous binary with a store format it supports
or restoring the pre-migration backup. Every irreversible migration must be
preceded by a tested export/rebuild path. Store migrations are forward only,
and a start the store refuses for a processor identity leaves an older store
at its schema.
