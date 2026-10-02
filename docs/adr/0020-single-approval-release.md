# ADR 0020: Single-approval release

Status: accepted

Date: 2026-10-02

## Context

ADR 0019 introduced the release coordinator with separate prepare, candidate,
and publish actions, and kept a required approval on every publishing
environment. A release therefore took three coordinator dispatches and eight
approvals across six environments, and while releasing 0.1.0-rc.2 a
disconnected Cloudflare Git integration left the site unchanged while the run
reported success.

## Decision

Add a `release` action that runs candidate and publish in one coordinator run
and always promotes the site. A release is the reviewed version PR followed by
one `release` dispatch, approved once in `release-control`. The candidate and
publish actions remain for releasing in two stages.

Remove required reviewers from the five publishing environments. Each keeps its
deployment rule for the protected `v*` or `sdk-v*` tags, which only the
maintainers in the release tag ruleset can create, and the registries keep
trusting only those environments through trusted publishing. This supersedes
ADR 0019's explicit per-environment publication approvals.

After promoting the site, the coordinator waits until leani.dev serves
documentation pinned to the release tag and fails otherwise.

## Consequences

The `release-control` approval authorizes tagging, building, publishing, and
site promotion for that run. Anyone with write access can still dispatch a
publisher from an existing release tag without approval, but only with a
successful candidate run of that tag's commit, so it can publish nothing but
that release's tested artifacts. Keep write access limited to release
maintainers.

A failed release resumes by dispatching `release` again: candidate reuses the
tags and successful preparation runs, and publication skips verified artifacts.
A site that does not serve the release fails the run instead of passing
silently.

## Compatibility and rollback

No node, API, processor, or durable schema contract changes. Restoring required
reviewers on the publishing environments brings back per-publisher approvals
without code changes; the `release` action then pauses at each publisher.
