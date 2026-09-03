# Single-owner execution Discv4 discovery

Status: accepted

Date: 2026-09-03

## Context

The execution source previously ran two Discv4 services: Reth's native
Discv4/Discv5 discovery and a supplemental Leani crawler. The supplemental
crawler used its own UDP socket, identity, routing table, and random-lookup
loop, then submitted compatible candidates to Reth. Reth still owned all peer
sessions, backoff, reputation, dialing, and material requests.

The duplicate discovery service was added to improve startup, but it also
increased the runtime's complexity and failure surface. The subscribe CLI audit
therefore proposed making Reth the sole Discv4 owner if startup and large
processor-ingestion benchmarks showed no material regression.

Leani separately decodes EIP-1459 DNS trees because the pinned Reth 2.4.1 DNS
resolver does not correctly join segmented TXT records. That seeder is not a
second discovery or dialing service: it verifies and decodes DNS records and
submits candidates to Reth's peer manager.

## Decision

- Reth's network manager is the sole owner of Discv4 discovery.
- Keep Leani's segment-correct EIP-1459 DNS seeder. It admits decoded records
  through Reth's bounded peer manager; Reth owns dialing, backoff, sessions,
  reputation, and request dispatch.
- Keep Reth's default native discovery lookup interval.
- Do not inject the broad cached-peer tier into Reth's permanent bootstrap set.
  Cached peers retain their existing bounded hot/broad admission paths.
- Remove the supplemental crawler, its direct `reth-discv4` dependency, and the
  `discv4_crawler` telemetry origin. No compatibility alias is retained because
  indexing-node is pre-launch.

## Consequences

Execution discovery now has one socket, identity, routing table, and lookup
loop. This removes a task and a failure mode while retaining all independent
sources of candidates: native Discv4, native Discv5, authenticated DNS trees,
and the SQLite peer cache.

Candidate admission does not alter Leani's trust model. Headers, bodies, and
receipts remain independently commitment-verified before processors use them.

The measured Reth-only build completed fewer warm-start samples within the
90-second cutoff than the dual-discovery build. We accept that caution because
Reth-only had better censored cold and warm medians, won the full ingestion
benchmark, and the experiment did not establish that the supplemental crawler
caused the completion-count difference. Startup reliability should be improved
inside the single-owner design rather than by restoring duplicate discovery.

Two alternative Reth-only variants were rejected. Shortening the native lookup
interval to ten seconds degraded the measured warm median, while adding broad
cached records to Reth's bootstrap table overemphasized stale peers.

## Evidence

All real-source runs used the checkpoint at beacon slot `15129984`, execution
block `25892246`, and the pinned inclusive execution range
`25792247..=25892246`.

The processor was `uniswap-observations`. Bloom-positive blocks fetched
complete receipts; all material was commitment-verified; and output passed the
processor, HTTP delivery, acknowledgement, pruning, and database-verification
path. Both variants mapped and committed all 100,000 frames with the identical
output digest
`8cf9c62338832c13e22858487960167ee2b5f2eb3b0a501e5fe422860d477dff`.

| Variant | Elapsed | Throughput | First source frame |
| --- | ---: | ---: | ---: |
| Dual Discv4 | 284.144s | 352.302 blocks/s | 40.102s |
| Reth-only Discv4 | 225.654s | 443.935 blocks/s | 15.026s |

Startup used twenty interleaved cold/warm pairs per primary variant, an actual
`leani subscribe blocks --once` event, and a 90-second censored cutoff:

| Variant | Phase | Completed | Censored median | Censored p90 |
| --- | --- | ---: | ---: | ---: |
| Dual Discv4 | cold | 11/20 | 83.117s | 90.000s |
| Dual Discv4 | warm | 17/20 | 51.326s | 90.000s |
| Reth-only Discv4 | cold | 12/20 | 70.228s | 90.000s |
| Reth-only Discv4 | warm | 13/20 | 47.994s | 90.000s |

Public-peer conditions remain noisy, and the sequential 100,000-block pair is
not a throughput distribution. The evidence is sufficient to reject a large
ingestion regression and supports the simpler single-owner architecture; it
does not prove that discovery ownership alone caused the throughput difference.

## Compatibility and rollback

This intentionally removes the `discv4_crawler` telemetry label before launch;
no compatibility shim or migration is provided. Rollback would restore the
supplemental crawler, its telemetry origin, and the direct `reth-discv4`
dependency.
