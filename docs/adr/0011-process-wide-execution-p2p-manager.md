# Process-wide execution P2P manager

Status: accepted

Date: 2026-08-04

## Context

The first production adapter treated request recovery as a reason to stop the
embedded Reth `NetworkManager` and create another manager with a random node
identity and ephemeral ports. Receipt-serving peers are scarce enough that
this discarded healthy connections, in-memory reputation, discovery progress,
and queued work at exactly the point they were most valuable. A union-merged
peer cache also resurrected records that Reth had intentionally omitted.

Live and history lanes can address different block ranges, but they participate
in the same Ethereum execution network and should not own separate networking
stacks.

## Decision

- one `RethP2pSource` owns one lazily started `NetworkManager`;
- all clones, history bridges, live subscriptions, and probes share it;
- lane/request retries never shut down the manager; only manager termination or
  process shutdown recreates it;
- the stable secp256k1 identity lives in `data_dir/execution-p2p-secret`;
- Reth's latest peer export is authoritative, deduplicated, quality ordered,
  and capped instead of unioned with stale records;
- Discv4, Discv5, and DNS discovery run together, with configurable fixed
  listener/discovery ports and NAT advertisement;
- a global material gate starts request timeouts only after connected-peer
  capacity is available across all lanes, and queued live-head work takes the
  next available slot before normal catch-up work;
- body and receipt batches start at one block, grow to two and four after
  success, and reset independently after failure;
- history emits verified material in at most four-block units and retains
  successful bodies while receipts retry;
- a retained parent-linked canonical suffix may seed live reorg context and
  skip redundant overlap downloads after restart.

## Consequences

Transient material failure no longer changes the node identity or disconnects
unrelated peers. Peer reputation and discovery become meaningful over time,
local queue pressure becomes distinguishable from remote timeout behavior, and
successful backfill work survives more failure boundaries.

The embedded manager still uses a no-op serving provider. Serving the retained
recent window is a separate enhancement because it requires a Reth provider
adapter and accurate protocol range advertisement.

## Evidence

- source-level tests prove stable identity reuse, bounded quality-ordered peer
  compaction, peer-aware concurrency, and retained-suffix validation;
- node tests prove a restart selects the retained canonical tip instead of an
  anchored overlap;
- the deterministic workspace suite remains the offline correctness gate;
- a sustained public-Mainnet run remains required before production cutover.

## Compatibility and rollback

Existing configuration remains valid because ports default to ephemeral,
Discv5 defaults on, NAT defaults off, and the peer-cache bound has a default.
The first oversized-cache compaction retains a recoverable
`.pre-compact` copy.

Rollback requires stopping the node and restoring the previous binary. The
stable secret is forward- and backward-compatible node identity material.
The compacted Reth peer cache is valid for the previous adapter; the preserved
pre-compaction file may be restored while the process is stopped if needed.
