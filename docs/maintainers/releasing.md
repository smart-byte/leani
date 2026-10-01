# Maintainer release runbook

This runbook is for release maintainers who approve the protected environments
and change repository settings; the documentation site does not publish it.
The [release process](../contributing/releasing.md) explains the release flow
and its protections, and [Verify a release](../operations/verify-releases.md)
covers the checks users run.

## Coordinated releases

Use **Actions → Coordinate release** (`release-coordinate.yml`), dispatched
from `main`. Enter an unprefixed version such as `0.1.0-rc.2`. It has three actions:

1. **prepare** generates a release PR with workspace and SDK versions, exact
   internal crate requirements, lockfile versions, license notices, current
   install pins, and the changelog release heading. Historical migration notes, processor versions,
   schema numbers, and the OpenAPI contract version stay intact. Review the
   upgrade instructions and merge the PR after CI passes.
2. **candidate** creates annotated `v<version>` and `sdk-v<version>` tags on the
   dispatch's reviewed `main` commit. Existing tags must already name that commit;
   tags are never moved. Before creating them, it waits for the CI, security,
   site, and container runs that `main` already started for that commit and
   stops if the latest one failed, so a known failure never reserves a version.
   It then starts or reuses exact-commit CI, security, site, container, binary,
   Rust-library, and SDK preparation runs. After all pass, it verifies their
   artifacts and uploads `leani-release-plan-<version>` with run IDs and file
   checksums. Keep this successful **Coordinate release run ID**; its summary
   ends with the publication deadline.
3. **publish** takes that coordinator run ID as `candidate_run_id`. It checks
   plan provenance, tags, checksums, and current CI, then dispatches the binary,
   Rust-library, SDK, and container publishers in order. Each retains its own
   protected environment and registry identity. Approve deployments as they
   appear. Select `promote_site` to promote the site after every artifact is
   published and verified.

The prepare and candidate actions do not publish release artifacts; candidate
reserves both release tags. Plans expire after 14 days. A reused preparation
run's artifacts can expire sooner, so publish before the deadline in the
candidate summary. To regenerate older candidates, run candidate from `main`
with `release_commit` set to the original release's exact SHA. That commit must
be on `main`, its package versions must match, and its existing tags must still
point to it. Publishing an existing plan also remains possible after `main`
advances. The plan binds both the released source and the reviewed coordinator
revision. Publishers run the workflow files of the release tag, so the
coordinator can only publish releases whose commit already contains it
(`0.1.0-rc.2` onward).

If publication fails, rerun **publish** with the same candidate run ID. It checks
GitHub asset hashes, crates.io checksums, npm archive integrity, and GHCR image
identities before writing. Matching publications are reused; different bytes
stop the release. A partial Rust-library publication uploads only missing crates,
and a partial GitHub draft can finish uploading missing assets. Registries are
not a transaction: completed publications remain available if a later step fails.
The run summary links each workflow and records publication progress.
If the coordinator times out while a publisher awaits approval, a retry rejoins
that same publisher for the recorded candidate instead of starting another one.

The Ubuntu coordinator uses Python 3.11+, GitHub CLI, and Docker Buildx. It does
not compile packages or receive registry OIDC permissions. Homebrew tap updates
remain a separate reviewed PR, following the [Homebrew](#homebrew) policy below.

### Coordinator setup

Create `release-control` with the same required reviewers, self-review protection,
and administrator bypass restriction as the publishing environments below. Allow
only the `main` **branch**. Its write jobs are also restricted to `main` by YAML
and the release workflow policy checker.

Add the required `RELEASE_CONTROL_TOKEN` as an environment secret on
`release-control`. Use a repository-scoped fine-grained token owned by a release
maintainer already allowed by the tag ruleset, with Contents, Pull requests,
Actions, and Workflows read/write permissions. Do not add GitHub Actions to the
tag ruleset's bypass list. The coordinator never falls back to
`GITHUB_TOKEN`, which stays read-only: a pull request or tag it creates starts
no workflow runs, so the preparation PR would never receive CI, and the tag
ruleset refuses its tags.

Registries still trust `crates-release.yml` and `sdk-release.yml`; no registry
token belongs in `release-control`. Its approval authorizes coordination and
does not replace the publishers' environment approvals.

Make the `leani` GHCR package public before the first coordinated publication
(step 7 of the [release checklist](#release-checklist)). The coordinator checks
published images anonymously and stops when GHCR refuses access.

Site promotion has its own prerequisites; see [Site promotion](#site-promotion).

## Release checklist

1. Keep release verification results in ignored local storage.
2. Run the Rust, SDK, dependency-policy, and publication checks from a clean
   checkout. Test restart, replay, and backup/restore with the packaged binary.
3. Confirm that the [release-facing changes](../contributing/releasing.md#release-facing-changes)
   landed with the version update.
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

## Required repository settings

These settings live on GitHub, not in the repository. Configure them before
the next publication; each publish job waits for its environment's approval.

Environments (Settings → Environments): create `release-github`,
`release-ghcr`, `release-crates`, and `release-npm`, and edit the existing
`site-production`. Configure `release-control` separately as above. For each of
the five publishing environments:

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
See [Site promotion](#site-promotion).

Trusted publishers: set the environment of the npm trusted publisher for
`@smart-byte/leani-sdk` to `release-npm`, and that of each crate's crates.io
trusted publisher to `release-crates`. The registries then issue a token only
to the protected job.

Rulesets (Settings → Rules → Rulesets):

- A branch ruleset named `main`: enforcement Active, target the default
  branch, and leave the bypass list empty. Enable "Restrict deletions",
  "Block force pushes", and "Require a pull request before merging" with one
  required approval, "Dismiss stale pull request approvals when new commits
  are pushed", and "Require conversation resolution before merging". Enable
  "Require status checks to pass" with `Rust (ubuntu-24.04)`, `Rust (macos-15)`,
  `TypeScript SDK`, `API and deployment contracts`, `Dependency policy`,
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

## Publication order

The release, crates.io, and npm workflows start with `check-release-ci.rb`, which
verifies the explicit `container_candidate_run_id` for the release commit. It
must be successful and carry both unexpired container artifacts. A later
container publication waiting for approval or failing does not invalidate that
preparation. CI, security, and site still require their newest exact-commit
validation runs to succeed. The coordinator publishes in this order:

1. `release.yml` from the `v*` tag, approved in `release-github`;
2. `crates-release.yml` from the `v*` tag and `sdk-release.yml` from the
   `sdk-v*` tag, approved in `release-crates` and `release-npm`;
3. `container.yml` from the `v*` tag, approved in `release-ghcr`;
4. `site-promote.yml` from the `v*` tag once the GitHub release is published,
   approved in `site-production`.

A standalone publisher must also supply `container_candidate_run_id`; container
publication uses its own `candidate_run_id`. To regenerate expired images,
dispatch `container.yml` from the `v*` tag with `publish=false` and use the new
preparation run ID for subsequent publications. Builds and publications of a
ref use separate concurrency groups, so such a build never cancels a
publication awaiting approval.

## Binaries

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
When dispatching this publisher directly, also supply the successful
`container_candidate_run_id`; the coordinator supplies both IDs automatically.

If a required workflow has no run for the release commit because of path
filters, dispatch it explicitly on the release ref. A successful run for an
older commit does not satisfy the gate.

Release assets are attached to a draft before it is published. Prerelease
versions are marked as prereleases and do not replace the latest stable
release. Enable GitHub release immutability in repository settings if that
protection is required; the workflow name alone does not enable it.

## TypeScript SDK

The SDK is published as `@smart-byte/leani-sdk`. Its workflow requires
`sdk-v<package-version>` for publication and passing CI for the same commit.
Its npm dist-tags follow the [release channels](../operations/verify-releases.md#release-channels).
The package job tests and scans a local npm archive, checks that the archive's
`SDK_VERSION` matches its manifest, and uploads it. With
`publish=true`, the `release-npm` job downloads that archive, checks that its
version matches the tag, and publishes it with provenance; a dry run stops
after the upload for manual inspection.
The coordinator supplies the SDK preparation `candidate_run_id` and
`container_candidate_run_id`. With a candidate ID, the publisher verifies and
downloads that existing archive instead of building it again.

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

## Container

The container workflow builds, runs, scans, and saves both architectures on
native runners. Publication requires the matching node release tag, that
successful preparation run's `candidate_run_id`, and passing Rust, security,
and site workflows; a read-only job checks them. The `release-ghcr` job then
loads and verifies the saved images before publishing them, assembles the
multiarchitecture tag from their registry digests, and attests the result.
Its job summary lists the published digests.
GitHub's automatic `GITHUB_TOKEN` supplies GHCR authentication. Verify
anonymous pulls for both architectures before announcing the release.

## Homebrew

Copy the candidate's `leani.rb` to `Formula/leani.rb` in the shared
`smart-byte/homebrew-tap` repository only after its public archive URLs exist.
Run the tap's installation and formula tests. Keep `v0.1.0-rc.1` available
through this formula for initial testing. When the first stable release ships,
switch the formula to stable releases by default; publishing a later prerelease
through it requires an explicit decision. Rendering the formula fails if
`SHA256SUMS` lacks an archive or holds a malformed checksum.

## Site promotion

Cloudflare's Git integration builds production from `site-production`; `main`
builds previews. Its build variables are documented in
[`site/README.md`](../../site/README.md). Leave the production `LEANI_DOCS_REF`
unset so each promotion derives its documentation ref from the workspace
version; a stale explicit value fails the production build. SDK tags never
select a production build.

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

## Rust libraries

Rehearse the packages locally as described in the
[release process](../contributing/releasing.md#rust-libraries).

Run `crates-release.yml` with `publish=false` to test, package, and inspect all
four libraries in CI. It checks their clean commit provenance, MIT license,
archive paths, and contents before uploading the dry-run packages. With
`publish=true`, the `release-crates` job packages the same commit again without
compiling and stops unless it produced exactly the tested archives: the same
names, as many, byte for byte. It then publishes with
`cargo publish --no-verify`, because Cargo cannot upload a prepared archive.
With `candidate_run_id`, the publisher reuses successful preparation archives
instead of rerunning tests. It still requires byte-identical repackaging and
`container_candidate_run_id`. Retries verify registry checksums and upload only
missing crates.

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
