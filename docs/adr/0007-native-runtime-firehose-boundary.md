# Native runtime first; Firehose compatibility at the edge

Status: accepted
Date: 2026-08-01

## Context

The product needs deterministic map/reduce processors, ordered state, undo,
finality, replay, and resumable subscriptions. Firehose/Substreams offers
similar concepts, but its protobuf types, module graph, host functions, and
storage topology would make an external framework part of the core model. The
low-storage node does not retain the Firehose archive a general package may
assume.

## Decision

Implement the required semantics natively over `BlockFrame`, processor
descriptors, encoded deltas, reducer transactions, undo records, and the
durable change log. These contracts contain no Firehose types.

Future compatibility is an edge adapter. Complete frames may be converted to
a versioned reader block when every requested field exists. Block-local
consumers can receive apply/undo/finality from the change stream. Packages
requiring traces, state diffs, arbitrary host calls, dynamic data sources, or
retained Firehose history fail capability negotiation before execution.

## Consequences

The node implements the useful semantic subset without the storage and
operational cost of a general Firehose stack. No general Substreams package
compatibility is claimed without protobuf/host-function conformance and
benchmark evidence.

## Evidence

The native blobs, ERC-20, and Uniswap processors exercise block-local and
ordered-state reduction, restart, undo, and durable subscription paths without
Firehose-specific types.

## Compatibility and rollback

An optional adapter can be added in a leaf crate without changing core
encodings. Removing an adapter leaves native processors and stored state
unchanged.
