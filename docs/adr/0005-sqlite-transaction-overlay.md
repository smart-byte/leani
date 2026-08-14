# SQLite transaction overlay

Status: accepted

Date: 2026-08-01

## Context

Processors must update arbitrary application state without receiving a raw
database connection, bypassing undo capture, or publishing changes before
their state is committed. The first deployment profile has one authoritative
writer and needs simple backup and recovery.

## Decision

Use SQLx 0.8 and SQLite in WAL mode with foreign keys, a bounded busy timeout,
one process-wide writer lock, and a small connection pool. The default
durability profile is `synchronous=FULL`.

Processors only receive the `ReducerTransaction` interface. Its implementation
is an in-memory overlay which lazily captures each first-touched entity or
index preimage and provides read-your-writes behavior. Once reduction
succeeds, the core commits:

- entity and index mutations;
- core-generated inverse mutations and inverse domain changes;
- processor cursor and per-block coverage;
- applied-delta identity for idempotency;
- monotonic public change-log entries;
- sink outbox references;

in one SQLite transaction.

Undo loads the core-generated journal, restores mutations in reverse order,
restores the prior cursor, removes coverage/application identity, and appends
new monotonic undo changes in another atomic transaction. Finalized journal
entries cannot be undone through the normal runtime path.

Migrations are embedded in the binary. Operational commands expose logical
statistics, integrity/durable-record verification, `VACUUM INTO` backup, and
WAL checkpoint/compaction without exposing SQL to processors.

## Consequences

Block reduction is serialized at commit safety boundaries, while mapping,
source reads, API reads, and reductions for independent input can still be
prepared concurrently. SQLite signed integers bound stored numeric fields;
the adapter rejects larger unsigned values rather than truncating them.

The first schema records individual covered blocks. Coverage interval
compaction and journal/change pruning are runtime maintenance operations, not
processor concerns.

## Evidence

Tests exercise fresh migration, atomic entity/index/cursor/change/outbox
application, exact duplicate idempotency, apply-then-undo restoration,
monotonic undo changes, finalized-undo rejection, durable verification, and
prefix ordering. Clippy treats warnings as errors for all store targets.

## Compatibility and rollback

The migration is additive for the unreleased schema. Future schema migrations
are forward-only during normal startup and require a verified backup before
destructive changes. Another store backend can implement the same core
transaction contract without changing processor code or durable deltas.
