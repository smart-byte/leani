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
   versions, and processor version declarations. When the SDK version in
   `packages/sdk/package.json` changes, pin every documented SDK install, such
   as the `bun add` commands in `README.md` and the install guide, to that
   exact version in the same commit. CI's `scripts/check-sdk-pins.rb` fails
   on a documented install that names another version, a range, a tag, or
   none.
4. Build Linux and macOS `x86_64` and `aarch64` artifacts and both Linux container
   architectures from the same reviewed commit.
5. Verify checksums and a clean-host smoke test.
6. Create an annotated semantic-version tag. Release candidates use
   `-rc.N`; a stable release requires every artifact listed above.
7. On the first container release, make the `leani` GHCR package public in
   its package settings: GitHub creates it private, and pulling the
   documented image fails until then. Then check that
   `docker buildx imagetools inspect ghcr.io/smart-byte/leani:v<version>` lists
   the per-architecture digests that the publication summary shows and the job
   attested. A runner that pushes each architecture as a one-entry index would
   attest those index digests instead of the images.

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
| `crates-release.yml` | `release-crates` | `contents: read`, `id-token: write` | `v*` tag |
| `sdk-release.yml` | `release-npm` | `id-token: write` | `sdk-v*` tag |
| `site-promote.yml` | `site-production` | `contents: write` | `v*` tag |

No other job may request a write permission, `id-token: write` included, read
a secret, or keep its checkout credentials. Only the crates.io publish job
checks out the repository, because Cargo cannot upload a prepared archive, and
npm publishes a local archive file named right after `publish`. No workflow
may run on `pull_request_target` or `workflow_run`: both run with the base
repository's token, secrets, and cache on behalf of events a pull request
controls.
`ruby scripts/check-release-ci.rb --workflows` enforces this, and CI runs it
through `scripts/test-release-ci.rb`. Change its policy table only together
with this page.

### Required repository settings

These settings live on GitHub, not in the repository. Configure them before
the next publication; each publish job waits for its environment's approval.

Environments (Settings → Environments): create `release-github`,
`release-ghcr`, `release-crates`, and `release-npm`, and edit the existing
`site-production`. For each of the five:

- Required reviewers: at least one release maintainer. Enable
  "Prevent self-review" once two or more maintainers can approve.
- Clear "Allow administrators to bypass configured protection rules".
- Deployment branches and tags: choose "Selected branches and tags" and add a
  single tag rule, `sdk-v*` for `release-npm` and `v*` for the other four.

Secrets (Settings → Secrets and variables → Actions): delete the repository
secrets `NPM_TOKEN` and `CARGO_REGISTRY_TOKEN`, and revoke both tokens on
npmjs.com and crates.io. Every job can read a repository secret, and both
registries now use trusted publishing. Should a bootstrap token be needed
again, store it for that one publication as an environment secret, `NPM_TOKEN`
on `release-npm` or `CARGO_REGISTRY_TOKEN` on `release-crates`, and delete it
afterward. A `SITE_PROMOTION_TOKEN`, when promotion needs one, is likewise an
environment secret on `site-production` and never a repository secret: a
workflow on any branch pushed to this repository can read repository secrets.
See [SDK, container, Homebrew, and site](#sdk-container-homebrew-and-site).

Trusted publishers: set the environment of the npm trusted publisher for
`@smart-byte/leani-sdk` to `release-npm`, and that of each crate's crates.io
trusted publisher to `release-crates`. The registries then issue a token only
to the protected job.

Rulesets (Settings → Rules → Rulesets):

- A branch ruleset named `main`: enforcement Active, target the default
  branch, and leave the bypass list empty. Enable "Restrict deletions",
  "Block force pushes", and "Require a pull request before merging" with one
  required approval, "Dismiss stale pull request approvals when new commits
  are pushed", and "Require conversation resolution before merging". A single
  maintainer sets the required approvals to 0. Enable "Require status checks
  to pass" with `Rust (ubuntu-24.04)`, `Rust (macos-15)`, `TypeScript SDK`,
  `API and deployment contracts`, `Dependency policy`,
  `Public tree and history policy`, `Credential history scan`,
  `Build and scan (amd64)`, and `Build and scan (arm64)`. Do not require the
  path-filtered `Bun-only static site` check: a pull request that skips it
  could never merge.
- A tag ruleset named `release tags`: enforcement Active, target the tag
  patterns `v*` and `sdk-v*`, and add only the release maintainers, for
  example the Repository admin role, to the bypass list. Enable
  "Restrict creations", "Restrict updates", and "Restrict deletions". The
  environments trust these tags, so only maintainers may create or move them.
- A branch ruleset named `site-production`: enforcement Active, target the
  branch `site-production`. Enable "Restrict deletions" and
  "Block force pushes"; promotion only fast-forwards, so neither blocks it.
  Cloudflare deploys this branch to production, and without the ruleset
  anyone with write access can move it and skip the `site-production`
  approval. If anyone besides the release maintainers has write access, also
  enable "Restrict creations" and "Restrict updates" and add only the release
  maintainers to the bypass list. Promotion then needs a
  `SITE_PROMOTION_TOKEN` owned by a release maintainer, because the workflow
  token acts as GitHub Actions. Never add GitHub Actions, or any other app
  that every workflow runs as, to this bypass list. A writer's pushed branch
  could run a workflow that moves `site-production` without approval, and a
  bypass actor also skips the force-push and deletion rules.

### Publication order

The release, crates.io, and npm workflows start with `check-release-ci.rb`,
which requires the newest `container.yml` run for the release commit to have
succeeded. A container publication is such a run. While it waits for approval,
and after it is rejected or fails, those gates fail. Publish in this order:

1. `release.yml` from the `v*` tag, approved in `release-github`;
2. `crates-release.yml` from the `v*` tag and `sdk-release.yml` from the
   `sdk-v*` tag, approved in `release-crates` and `release-npm`;
3. `container.yml` from the `v*` tag, approved in `release-ghcr`;
4. `site-promote.yml` from the `v*` tag once the GitHub release is published,
   approved in `site-production`.

A workflow checks its gate when it starts, so a run that has passed its gate
can wait for approval while the next one starts. To recover when a container
publication is rejected or fails, or when a gate must run while one waits,
cancel or finish that run. Then dispatch `container.yml` from the `v*` tag with
`publish=false`. Its successful run rebuilds and scans both images, satisfies
the gates again, and can serve as the `candidate_run_id` of a new container
publication.

### Provenance and dependency updates

The GitHub release job attests the build provenance of each archive before
attaching it. The attestation names the release tag, its commit, and the
publication run, which checked that the candidate came from a successful
preparation run of that commit. The release also carries `SHA256SUMS` and the
CycloneDX SBOMs; publication stops if the candidate lacks the node's SBOM,
`leani.cdx.json`.

The container job attests the provenance of the multiarchitecture image and
of each architecture's image, and attaches each architecture's CycloneDX SBOM
to that image's digest. The attestations are stored with the images in GHCR
as well. The SBOMs are bound to the image digests instead of attached to the
GitHub release: a digest-bound attestation can be verified against the exact
image, and the container job needs no `contents: write`. Its job summary lists
the published digests. Verify before deploying:

```bash
gh attestation verify leani-v0.1.0-x86_64-unknown-linux-gnu.tar.gz --repo smart-byte/leani
gh attestation verify oci://ghcr.io/smart-byte/leani:v0.1.0 --repo smart-byte/leani
gh attestation verify oci://ghcr.io/smart-byte/leani@sha256:<amd64-digest> --repo smart-byte/leani
gh attestation verify oci://ghcr.io/smart-byte/leani@sha256:<amd64-digest> \
  --repo smart-byte/leani --predicate-type https://cyclonedx.org/bom
```

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

## Prepare and publish binaries

Run `Prepare release` with the version matching the workspace manifest and
`publish=false`. It executes the offline fixture on native Linux and macOS
runners for both architectures, then uploads a release candidate containing
the archives, checksums, portable SBOM files, the committed
`THIRD_PARTY_LICENSES.txt` dependency notices, and Homebrew formula. The
notices file is regenerated from `Cargo.lock` and must match the commit, so
an outdated copy fails the candidate. Build paths are remapped and the
extracted binaries are checked for private content before their fixture tests
run.

Inspect and test that candidate. Once the exact commit is tagged and its CI,
publication/security, site, and container checks have passed, run the workflow
from the tag with `publish=true` and the successful preparation run's numeric
`candidate_run_id`. The publisher verifies that the candidate belongs to the
same commit and downloads its existing assets; it does not rebuild them. Its
publish job then waits for approval in the `release-github` environment.
Candidates expire after 14 days, so prepare a new candidate if necessary.

If a required workflow has no run for the release commit because of path
filters, dispatch it explicitly on the release ref. A successful run for an
older commit does not satisfy the gate.

Release assets are attached to a draft before it is published. Prerelease
versions are marked as prereleases and do not replace the latest stable
release. Enable GitHub release immutability in repository settings if that
protection is required; the workflow name alone does not enable it.

## SDK, container, Homebrew, and site

The SDK is published as `@smart-byte/leani-sdk`. Its workflow requires
`sdk-v<package-version>` for publication and passing CI for the same commit.
Prerelease SDK versions use npm's `next` tag; stable versions use `latest`.
Until the first stable release, `latest` stays on the last prerelease that
received it: npm gave `latest` to the package's first version, `0.1.0-rc.1`,
and later prereleases move only `next`. An unversioned install therefore
resolves to that prerelease; install a newer prerelease by exact version or
with `@next`. The first stable release moves `latest` to itself.
The package job tests and scans a local npm archive, checks that the archive's
`SDK_VERSION` matches its manifest, and uploads it. With
`publish=true`, the `release-npm` job downloads that archive, checks that its
version matches the tag, and publishes it with provenance; a dry run stops
after the upload for manual inspection.

For the first publication, a maintainer with access to the `@smart-byte` scope
must configure a short-lived granular `NPM_TOKEN` environment secret on
`release-npm`. Grant the token read/write publication access to `@smart-byte`
under packages and scopes, and permission to bypass 2FA. Organization-management
access alone does not grant package publication rights. Dispatch
`sdk-release.yml` from the SDK tag with `publish=true` and `bootstrap=true`.
The GitHub repository must be public for npm provenance. Once the package
exists, configure its npm trusted publisher: GitHub owner `smart-byte`,
repository `leani`, workflow `sdk-release.yml`, environment `release-npm`, and
permission to publish directly with `npm publish`. Subsequent publications use
`bootstrap=false` and need no npm secret. Revoke the bootstrap token and delete
the GitHub secret after switching. See [npm trusted publishing](https://docs.npmjs.com/trusted-publishers/).

The container workflow builds, runs, scans, and saves both architectures on
native runners. Publication requires the matching node release tag, that
successful preparation run's `candidate_run_id`, and passing Rust, security,
and site workflows; a read-only job checks them. The `release-ghcr` job then
loads and verifies the saved images before publishing them, assembles the
multiarchitecture tag from their registry digests, and attests the result.
GitHub's automatic `GITHUB_TOKEN` supplies GHCR authentication. Verify
anonymous pulls for both architectures before announcing the release.

Copy the candidate's `leani.rb` to `Formula/leani.rb` in the shared
`smart-byte/homebrew-tap` repository only after its public archive URLs exist.
Run the tap's installation and formula tests. Keep `v0.1.0-rc.1` available
through this formula for initial testing. When the first stable release ships,
switch the formula to stable releases by default; publishing a later prerelease
through it requires an explicit decision. Rendering the formula fails if
`SHA256SUMS` lacks an archive or holds a malformed checksum.

Before promoting the site, set the Cloudflare Pages project's production
`LEANI_DOCS_REF` build variable to the node release tag being promoted. A
production build fails unless its documentation ref equals `v` followed by the
workspace version of the commit it builds, so a stale value fails the
deployment instead of linking to an earlier release. SDK tags never select a
production build.

Dispatch `site-promote.yml` from the release tag. Its first job checks that
the release is published and builds the production site with a read-only
token. The `promote` job then waits for approval in the `site-production`
environment and fast-forwards `site-production` to the tag's commit through
the GitHub API, which refuses anything but a fast-forward. It uses the
workflow token unless the `site-production` environment defines a
`SITE_PROMOTION_TOKEN` secret. Add that secret when GitHub rejects the workflow
token. The workflow token cannot change workflow files, so a promotion whose
range changes them can fail. And a `site-production` ruleset that restricts
updates admits only its bypass list. Use a fine-grained token for this
repository with Contents and Workflows read and write access, owned by a
release maintainer on that bypass list. Store it as an environment secret on
`site-production`, never as a repository secret, which a workflow on any
branch could read.
Cloudflare's Git integration then builds production from `site-production`;
`main` builds previews.

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
archive paths, and contents before uploading the dry-run packages. With
`publish=true`, the `release-crates` job packages the same commit again without
compiling and stops unless it produced exactly the tested archives: the same
names, as many, byte for byte. It then publishes with
`cargo publish --no-verify`, because Cargo cannot upload a prepared archive.

The first publication requires a crates.io account with a verified email and
an API token with permission to create the four crates. Store it as the
`CARGO_REGISTRY_TOKEN` environment secret on `release-crates`, then dispatch
`crates-release.yml` from the node release tag with `publish=true` and
`bootstrap=true`. Cargo publishes the explicit package selection in dependency
order. Add the intended maintainers or GitHub team as owners afterward.

Configure [trusted publishing](https://crates.io/docs/trusted-publishing) on
each crate with GitHub owner `smart-byte`, repository `leani`, workflow
`crates-release.yml`, and environment `release-crates`. Subsequent
publications use `bootstrap=false`; the official crates.io authentication
action supplies a short-lived token. Revoke the bootstrap token and remove its
GitHub secret after switching.

The full `leani` node is distributed through native archives, Homebrew, and
containers. Its Git-only networking and consensus dependencies prevent
publication on crates.io. Publishing the authoring libraries does not enable
dynamic processor loading; custom native processors are compiled into a node.

Rollback means deploying the previous binary with a store format it supports
or restoring the pre-migration backup. Every irreversible migration must be
preceded by a tested export/rebuild path and an ADR. Store migrations are
forward only, and a start the store refuses for a processor identity leaves
an older store at its schema; ADR 0018 records the schema 22 and 23
migrations and this policy.
