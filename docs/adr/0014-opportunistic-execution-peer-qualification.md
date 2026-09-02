# Opportunistic execution peer qualification

Status: accepted

Date: 2026-09-02

Supersedes the startup-gating interpretation of `minimum_peers` used by the
execution P2P source. ADR 0012 remains active for peer-store tiering, admission,
and capability ranking.

## Context

Every newly connected execution peer is probed at the current independently
verified execution target. The probe records whether the peer served a matching
header and commitment-valid body. The source previously waited for the
configured number of successful probes before issuing any processor request.

That made a useful optimization into a second availability gate. A target
change cleared current qualification outcomes, header-only processors waited
for body capability, and a connected peer could not serve a real request until
the separate probe completed. Startup benchmarks consequently included runs
with active execution sessions that produced no data before the timeout.

Qualification is not the integrity boundary. Requested headers are bound to
the expected hash or verified ancestry, bodies are checked against their header
commitments, and receipts are checked against both the receipt root and body.

## Decision

- `minimum_peers` is the physical connected-session floor before requests may
  start; it does not require prior capability qualification;
- the default floor remains one, while operators may require more connected
  sessions for deployment-level redundancy;
- qualification continues concurrently in the background and proven service
  remains the first peer-selection ranking input;
- when no proven peer is available, a newly connected peer may immediately
  receive a real material request;
- successful material requests update the same persistent capability evidence
  as qualification probes;
- failures cool only the affected header, body, or receipt lane, while
  commitment-invalid responses retain the existing global ban behavior;
- `body_serving_peer_target` is independent from `minimum_peers` and controls
  the background qualification concurrency target.

## Consequences

Connected peers can contribute useful data without waiting for a redundant
probe, and a qualification-target update no longer pauses all processor lanes.
Cold and warm startup tails should improve when the first useful peer has not
yet completed background qualification.

An unproven peer can consume one bounded request slot and fail before the
ranking learns its capability. Existing request concurrency, per-material
cooldowns, Reth dial backoff, and commitment validation bound that cost. The
network status distinguishes connected sessions from qualified body servers so
operators can still evaluate pool health.

## Evidence

The audit of `feat/subscribe-uniswap-cli` identified qualification gating as a
shared cause of target-change outages and header-only startup delays. The
pre-change benchmark also produced timeout runs with one or two connected
execution peers but zero qualified body servers. Focused tests assert that
connected and qualified targets are independent and that an unqualified
connected peer is eligible for verified material requests.

A subsequent interleaved A/B used ten isolated cold/warm pairs per build and a
90-second time-to-first-block cutoff. The gated build completed 4/10 cold and
7/10 warm runs; opportunistic qualification completed 7/10 cold and 8/10 warm
runs. With censored runs counted as 90 seconds, cold median improved from 90 to
78.254 seconds and warm median from 68.184 to 53.780 seconds. Public-peer
variance remains large, but both completion rate and median moved in the
expected direction.

## Compatibility and rollback

This is a pre-launch semantic clarification of `minimum_peers`; no compatibility
shim is provided. Rollback consists of restoring the qualification wait in the
execution source's connection path. The SQLite peer store and accumulated
service evidence remain usable across that rollback.
