# ADR 0019: Coordinated release preparation and publication

Status: proposed

Date: 2026-09-30

## Context

Leani releases binaries, four Rust authoring libraries, an npm SDK, and a
multiarchitecture container from one source revision. Each publisher already
has a protected environment and tested artifacts, but maintainers must coordinate
version changes, tags, candidate run IDs, publication order, and partial failures.
The release gate also treated an in-progress container publication as a failed
preparation, making publication depend on dispatch order.

## Decision

Add a manually dispatched coordinator on `main` with prepare, candidate, and
publish actions. Preparation opens a version PR. Candidate generation freezes
annotated node and SDK tags, runs all validations and packaging, and records the
source revisions, candidate run IDs, and artifact hashes. Publication consumes
that verified plan and dispatches the existing publishers in order.

Keep publisher workflow filenames, environments, and registry OIDC identities.
Use separate `workflow_dispatch` runs rather than moving registry publication
into a reusable workflow with a different caller identity. The controller gets
Contents/Actions write access only in `release-control`, with required reviewers,
self-review and administrator bypass disabled, and deployment limited to `main`.
It has no registry OIDC access and does not build packages with its write token.
Automatic tags use a maintainer credential allowed by the existing tag ruleset;
GitHub Actions is not added to that bypass list.

Every publisher verifies an explicit successful container preparation ID and its
unexpired artifacts. Actual CI, security, and site gates still require their
newest exact-commit validations to pass. SDK and Rust publication can consume
existing preparation artifacts; Cargo's required repackaging must remain
byte-identical to those tested archives.

## Consequences

Release versions and upgrades remain reviewed decisions. The workflow handles
coordination, artifact transfer, and publication; environment approvals remain
explicit. A controller run records the released commit separately from its own
reviewed revision, permitting candidate regeneration after `main` advances.

Publication is resumable, not atomic across registries. Existing package versions,
assets, and image identities must match the candidate before they are reused.
Different bytes stop publication. Rust retries select missing crates, GitHub
drafts can finish missing uploads, and site promotion follows verified publication.
Homebrew updates retain their separate prerelease policy and review.

## Evidence

`scripts/test-coordinated-release.py` exercises preparation against a Git checkout,
full publication order, partial recovery, candidate records, altered artifacts,
conflicting published bytes, moved tags, and controller-source restrictions.
`scripts/test-release-ci.rb` covers exact-commit authorization, candidate expiration,
container publication independence, and the protected workflow permission policy.
The generated RC2 manifests and lockfile were also validated with locked Cargo
metadata for all 24 workspace packages and the SDK's standalone package smoke test.

## Compatibility and rollback

No node, API, processor, or durable schema contract changes. Standalone publisher
dispatch remains available, now requiring an explicit container preparation ID;
SDK and Rust candidate reuse is optional. Disabling the coordinator restores
individual dispatch without moving release tags or altering registry trust.
Already uploaded versions remain published if a later step fails; recovery
verifies and resumes them rather than deleting or replacing them.
