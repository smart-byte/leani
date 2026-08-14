# Persistent execution P2P and late archive reconciliation

Status: accepted

Date: 2026-08-02

## Context

The node must follow Ethereum without a paid RPC even when public execution
peers are uneven receipt servers. Historical datasets are cheap and fast but
may lag the head. Treating either side as the sole source creates the wrong
failure mode: bounded P2P cohort retries terminate during ordinary peer churn,
while waiting for an archive adds avoidable latency.

Retaining raw live blocks until an archive catches up would also make storage
proportional to archive lag rather than to the configured aggregations.

## Decision

The execution source is a persistent supervisor:

- maintain a configurable broad outbound cohort and aggressively refill open
  slots;
- cache usable peer metadata under the node data directory;
- penalize invalid responses and replace a cohort after bounded per-session
  request retries;
- retry fresh cohorts with capped exponential backoff until cancellation in
  production;
- request four-block body and receipt batches concurrently, bounded by both
  source configuration and the per-open source budget;
- preserve canonical output ordering and verify every header commitment before
  a frame reaches a processor.

Archives remain ordered, interchangeable `HistorySource` implementations.
After the initial hot/cold handoff, a background lane revisits the entire
maximum P2P history-bridge suffix and then advances toward finalized head in
small sequential batches. It remaps each archive frame with the exact
processor version/configuration and compares the resulting delta checksum with
the checksum committed from live P2P.

Only the 32-byte durable delta checksum and reconciliation verdict are needed
for this later audit. Raw recent frames may follow the normal reorg/RPC
retention policy.

Source unavailability or lag is retried with another archive or on the next
interval. A block identity, input-shape, processor-requirement, or delta
checksum disagreement is durably recorded and fails closed.

## Consequences

Temporary public-peer scarcity becomes delayed progress rather than a terminal
provider dependency. Archive lag no longer delays head ingestion, and later
archive reproduction adds independent evidence without increasing raw-chain
retention.

The node still depends on public network capacity for timeliness. A matching
processor delta proves the configured aggregation was reproduced; it does not
turn a filtered trusted dataset into a general-purpose complete block archive.
Production cutover still requires sustained Mainnet and fault-injection
evidence.

The persistent diagnostic probe remains bounded. Operators may disable
persistent retries only for diagnostics.
