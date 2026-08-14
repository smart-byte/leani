# Direct Reth P2P boundary

Status: accepted

Date: 2026-08-01

## Context

The live lane needs recent headers, bodies, transactions, receipts, and logs
without running an execution database or calling a paid execution RPC. Reth
exposes its Ethereum wire clients as libraries, but its internal crate graph is
large and changes between releases.

## Decision

Use only Reth's network, P2P request, chain-spec, Ethereum primitive, and task
crates behind the isolated `source-p2p` adapter. Pin Reth 2.4.1 at revision
`8eb210175687c9f0c889a3b6795c16781d830e3a`.

The adapter starts a network manager with a no-op provider and no Reth
database, node builder, RPC server, transaction pool, execution pipeline, or
permanent history. It:

- announces a recent consensus-derived execution head in the Ethereum status
  handshake;
- discovers mainnet peers and waits for a small peer pool;
- requests a bounded contiguous header range, bodies, and receipts;
- checks block numbering, parent continuity, and an optional
  consensus-verified tip hash;
- recomputes transaction, ommer, withdrawal, and receipt roots;
- checks receipt counts/types, logs bloom, and cumulative gas;
- penalizes peers that return invalid material;
- converts verified material to the source-neutral `BlockFrame`;
- applies byte/frame/range budgets before returning.

Public peers are capacity constrained, so a one-shot process can spend minutes
finding available sessions. The production live actor must retain its network
manager, peer table, and connections across requests. The diagnostic command
therefore defaults to three peers, a five-minute acquisition window, bounded
request retries, and retry backoff.

The source adapter now retains that network manager and fetch client for the
life of a subscription. It polls peer head declarations, fetches bounded
contiguous ranges, and verifies header/body/receipt commitments before
emitting frames. Subscriptions require an explicit independently sourced block
anchor; `head` is rejected because an execution peer declaration is not a
trustworthy bootstrap anchor.

On a parent mismatch, the adapter requests a bounded descending header branch
from the declared head, validates every link, finds an exact retained common
ancestor, and then fetches and verifies complete replacement material bound to
that tip. It emits a tip-first revert plus ancestor-first apply event. A fork
deeper than the configured window becomes a terminal `reset`; it is never
guessed.

## Consequences

The source has no permanent chain-age-dependent storage and no EVM execution.
It can fully supply receipt/event-driven processors and recent block RPC
methods. It cannot derive arbitrary state changes, traces, or `eth_call`.

Although the adapter imports a narrow Reth surface, upstream default features
still pull transitive execution-related crates into the build graph. This is a
binary footprint and upgrade concern, not a runtime dependency on a Reth
database or EVM. Each Reth upgrade is explicit and must rerun the fixed-range,
bad-response, and live-peer probes.

Post-Merge peer responses are not themselves finality evidence. The P2P lane
only promotes a block when its hash is joined to a locally verified consensus
anchor from the finality boundary.

## Evidence

The live diagnostic established public discovery, negotiated mainnet
capabilities, and fetched the header for consensus-verified execution block
`25656660` with hash
`0xf3fc94346619c19caef04425fd2a7925f9dc4191aeae5ed933e4a1cec418a914`.
The first available peer stalled on its body response, which validates why a
long-lived peer pool and retry policy are part of the production boundary.
Full header/body/receipt evidence remains a release-gate item until the
multi-peer probe completes.

## Compatibility and rollback

No Reth type crosses `source-p2p`. Replacing the implementation with an
upstream API change, narrow fork, another devp2p client, or a test source does
not change processors, storage, API, SDK, or normalized durable types.
