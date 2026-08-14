# Durable bounded historical runtime

Status: accepted

Date: 2026-08-01

## Context

Historical sources have different partitioning and failure modes. A backfill
must remain bounded, restart from committed work, never mark downloaded but
unreduced data complete, and give ordered-state processors a durable delta
queue.

## Decision

The historical runtime operates on `HistorySource`, `Processor`, and
`SqliteStore` contracts only. For each job it:

1. stores immutable job input and a mutable checkpoint;
2. computes missing ranges from durable processor coverage;
3. asks the selected source for an exact chunk plan;
4. opens one independently retryable chunk under hard source budgets;
5. validates shape, order, parent linkage, capabilities, trust, and finality;
6. maps frames concurrently through an order-preserving bounded buffer;
7. durably persists each versioned delta;
8. atomically reduces the delta, records coverage/change/outbox/cursor state,
   and removes the pending delta;
9. advances the job checkpoint only after that commit;
10. recalculates coverage after restart or a transient source failure.

Unavailable/disconnected/protocol source failures use capped exponential
backoff. Corrupt data, schema/plan/budget failures, processor failures, and
store failures fail closed. Cancellation is durable and propagated to the
source.

The Xatu adapter implements the same history contract for the blobs projection.
It chunks by 1,000 execution blocks and derives required UTC beacon partitions
from execution-block timestamps, keeping Xatu-specific dates out of the
generic scheduler contract.

## Consequences

Source download and mapping can run concurrently within explicit item and byte
limits. Commits remain serialized by the store. A crash after a processor
commit but before the job checkpoint merely causes an idempotent retry.

The current adapter materializes at most one 1,000-block projected Xatu chunk
before streaming its normalized frames. Future sources may stream directly;
the runtime contract and budgets are unchanged.

## Evidence

Synthetic tests cover exact coverage merge, concurrent mapping, durable
job completion, idempotent rerun, pending-delta consumption, partial chunks,
and the rule that incomplete input never claims complete coverage. Xatu tests
cover exact chunk plans, partition identity, strict trust-policy rejection,
and UTC date derivation including leap days.

A live mainnet run started with an empty store, fetched and projected Dencun
block `19426589` from the public Xatu objects, reduced the blobs processor, and
committed exact coverage in 24.7 seconds. The resulting SQLite file used
106,496 bytes and contained two entities, one index entry, one undo record,
and two change records. A second invocation performed zero source reads/maps
and returned complete coverage immediately; `db verify` passed.

## Compatibility and rollback

Jobs store source-neutral request/checkpoint JSON. Changing immutable job input
requires a new job ID. Durable processor deltas retain their exact processor
version/config/checksum contract. Another history source can replace Xatu
without changing scheduling, processors, storage, API, or SDK.
