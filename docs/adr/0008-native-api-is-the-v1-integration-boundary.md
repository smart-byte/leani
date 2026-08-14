# ADR 0008: Native API is the v1 integration boundary

Status: accepted

Date: 2026-08-01

Follow-up, 2026-08-10: the boundary decision still holds—Leani does not own a
generic PostgreSQL sink. blobs.money implemented its application-owned native
consumer and PostgreSQL reconciliation before Leani v1 because both products
are still pre-launch. References below to a separate post-v1 project describe
the original sequencing decision, not the current integration status.

## Context

The initial workspace contained an empty `sink-postgres` crate. A useful
PostgreSQL sink is not just a transport: it must choose a target schema,
conflict rules, migrations, ownership, and cutover behavior. Those decisions
belong to the consuming application. For blobs.money they are explicitly part
of the separate post-v1 integration project.

The independent node already commits processor state, change records, undo
records, and generic sink-outbox references atomically. It exposes those
outputs through a versioned query API and resumable SSE stream with a tested
TypeScript SDK.

Shipping an empty or generic PostgreSQL writer as though it were a supported
integration would create a second public contract without a real consumer or
schema acceptance test.

## Decision

The native HTTP/SSE API and SDK are the only external projection integration
boundary shipped in independent v1.

The empty `sink-postgres` crate is removed from the workspace. The generic
SQLite outbox tables and atomic write path remain because they are tested,
small, and preserve the option to add an application-owned sink later.

A PostgreSQL adapter must be introduced with:

- an explicit target schema and migration owner;
- idempotency and ordering rules for apply, undo, and finality changes;
- bounded retry, dead-letter, and acknowledgement behavior;
- outage/restart tests;
- an actual consuming application integration test.

## Consequences

- The independent node has no placeholder deployable component.
- blobs.money can choose the stable SDK stream first without coupling node v1
  to its database schema.
- A later sink does not require changing processor or store transaction
  contracts.
- Operators must not configure sink IDs until a concrete sink adapter is
  installed.

## Evidence

The store tests cover atomic change/outbox creation. The API and SDK tests
cover resumable cursor delivery, duplicate suppression, and reset-required
handling.

## Compatibility and rollback

No released sink API existed, so removal has no compatibility impact. A future
adapter can restore a separate crate and consume the unchanged generic outbox
contract.
