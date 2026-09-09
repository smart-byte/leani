# ADR 0016: Preserve requested coverage in output snapshots

Status: accepted

Date: 2026-09-07

## Context

The Mainnet documentation walkthrough found that generic entity queries used
the first matching entity as the beginning of a block-count retention window.
A covered block without an event could be rejected, while a query above the
indexed head returned an empty page with global `complete: true` metadata.
Pagination also discarded the original range when computing coverage.

## Decision

Explicit block-range queries require complete processor coverage. Resolve open
block bounds once, and return `range_incomplete` for missing coverage. For a
block-count output window, use the configured retention boundary independently
of sparse entity occurrence. Reject pruned output with `output_not_retained`.

Recheck coverage and retention under the SQLite writer lock when admitting the
snapshot, so ingestion or pruning between HTTP preflight and copying cannot
turn unavailable data into an empty success. The lower-level unvalidated
snapshot API remains available for internal row-selection use cases.

Schema version 20 adds nullable block and timestamp filter columns to
`query_snapshots`. Store the resolved selection there and reuse it when serving
subsequent pages. The public page shape and opaque cursor format are unchanged.
Timestamp-only filters remain row selectors; callers supply block bounds for
an explicit interval completeness check. Non-block-count retention windows keep
their existing conservative lower-bound checks.

## Consequences

Bounded output queries can distinguish a known empty result from missing input.
Snapshot storage gains four nullable selection columns. Snapshot admission does
additional coverage reads while holding the writer lock; queries remain subject
to the existing snapshot row, byte, and lifetime limits.

## Compatibility and rollback

Migration 0020 upgrades existing databases without modifying processor output,
delivery acknowledgements, or snapshot rows. Old snapshots receive null filter
columns and keep their previous unbounded coverage behavior until expiration.
Binaries supporting schema 19 reject a schema-20 database; downgrade requires
restoring a pre-upgrade backup rather than changing its version marker.

## Evidence

The covered-empty response has a golden fixture. Regression tests cover sparse
empty blocks, uncovered intervals on either side and internal gaps, pruned
output, retention moving after preflight, and pagination across ingestion and
restart. The existing migration test continues verifying unacknowledged delivery
through an upgrade. A copy of the real schema-19 Mainnet walkthrough database
was also upgraded and its formerly failing queries returned the expected results.
