# Two-tier execution peer startup

Status: accepted

Date: 2026-09-01

Supersedes ADR 0011 only for peer-cache authority and startup admission. Its
process-wide manager decision remains active.

## Context

Warm-start benchmarks showed that a large cache was not a reliable predictor
of time to first verified block. Loading every retained record into Reth and
force-dialing up to the concurrent dial ceiling let stale cached candidates
dominate the first connection window. A short or unsuccessful manager run
could then replace the cache with only its tiny current peer set, removing
alternatives that a later run still needed.

The separate Leani quality document contains stronger capability evidence than
a handshake or discovery record: verified header, body, and receipt responses,
their recency and latency, and failures observed after them.

## Decision

- the peer cache is a bounded broad discovery set, merged with Reth's current
  export instead of replaced by it;
- a hot tier contains at most the body-serving target's worth of cached peers
  with verified body success newer than their last recorded failure;
- hot selection prefers distinct IPv4 `/16` and IPv6 `/32` networks before
  selecting a second peer from the same network;
- hot peers are submitted and dialed immediately;
- the broad tier is admitted in body-target-sized batches at Reth's configured
  refill interval;
- DNS and Discv discovery start concurrently and freshly discovered candidates
  are admitted immediately;
- Reth does not separately load the cache file, preserving one startup
  admission policy and one bounded dial scheduler;
- identity-free telemetry attributes admissions, established sessions,
  qualification outcomes, and admission-to-qualification time to the first
  known candidate origin.

## Consequences

A failed run no longer collapses the next run's candidate set. Previously
useful but currently failing peers lose immediate-dial eligibility while their
capability evidence remains available for broad ranking. Fresh discovery gets
an early opportunity on every warm start without discarding the independent
value of a stable node identity and peer history.

The broad cache can retain unavailable records until quality ordering evicts
them at the configured bound. Reth remains responsible for connection-attempt
limits, per-peer backoff, and session lifecycle. The origin counters measure
the policy directly and are the basis for subsequent randomized cold/warm
benchmarks.

## Evidence

Ten paired block-subscription runs on the previous policy produced only 4/10
cold and 2/10 warm first-block results under 60 seconds. Warm cache files ranged
from 1 to 478 loaded records without predicting success, while failed exits
sometimes retained only 0–3 records. Focused tests cover hot-tier capability
eligibility, subnet diversity, and broad-cache preservation. Origin telemetry
was added so the replacement policy can be evaluated directly in the next
randomized benchmark.

## Compatibility and rollback

The peer-cache JSON and quality-document formats do not change. The node can be
rolled back by stopping it and running the previous binary against the same
data directory. A previous binary may replace the broad cache with its current
Reth export again, so preserve the file separately if the new retention
behavior must survive a rollback run.
