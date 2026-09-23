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
- failures cool only the affected header, body, or receipt lane. Only an
  invalid response bans the peer and records a persisted failure: material
  that breaks its commitments, structure, or block numbers, or that
  contradicts a verified expectation, meaning the consensus-verified finalized
  anchor or a hash proven to link to it;
- a response that contradicts only an unverified expectation, such as a head
  taken from peers' handshake statuses, is a disagreement, as is an empty or
  short reply: no ban and no persisted failure, at most a short local cooldown.
  Honest peers on another branch, or behind a claimed head, answer this way;
- polling for the next block asks one or two peers per poll, taken from those
  not yet asked for that block; once every eligible peer has been asked, the
  rotation starts over. An empty reply there is not a failure, but once
  another peer has served the block, the peers that answered "not yet" get
  the normal lane cooldown. Requests for blocks that exist, such as a
  catch-up range or the minimum live head, race the eligible peers, and an
  empty reply to them cools the peer's lane;
- qualification probes each peer with real requests at the current target,
  never from its handshake head, and compares targets by block number and
  hash. Lagging and timed-out probes are not persisted as failures and never
  clear a session's verified lanes;
- qualification is background work: it never occupies the material request
  slots reserved for the live lane;
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

A peer that answers from another branch is never banned for it, so a head
that one peer claims cannot get honest peers banned, and an orphaned head costs
them at most a short cooldown. Such peers may be asked again sooner;
commitment validation still rejects any material they serve that does not
match, and a verified expectation still bans a peer that contradicts it.

Peers that withhold the next block cannot stall the live lane either: each
poll asks peers not yet asked for the block, and a peer that answered "not
yet" for a block another peer served cools like any lagging peer. A slightly
late but honest peer pays the same short cooldown, and a success resets it.

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
