# SQLite execution peer store

Status: accepted

Date: 2026-09-02

Supersedes ADR 0011 and ADR 0012 only for persistent peer-state storage. Their
process-wide manager and two-tier startup decisions remain active.

## Context

Execution candidates and independently observed service evidence were stored
in separate JSON files. Periodic persistence cloned, sorted, serialized, and
atomically replaced each whole document on the same Tokio task that polled the
Reth network manager. This made flush cost proportional to every retained peer,
required temp-file and merge machinery, and kept the live quality map and
cross-process behavior separate from the durable bound.

Peer state is reconstructible operational data. It should not share failure,
backup, or write-contention boundaries with authoritative processor output.

## Decision

- store candidates and header/body/receipt evidence in the dedicated
  `data_dir/execution-network.sqlite` database;
- retain `execution-p2p-secret` as a permission-restricted file because it is
  stable node identity rather than disposable peer evidence;
- use WAL mode, one connection, incremental upserts, and bounded indexed
  pruning rather than whole-document replacement;
- collect known peers directly from Reth instead of exporting persistent JSON;
- treat Reth's broad known-peer snapshot only as candidate input; unlike Reth's
  legacy JSON export it can include temporarily backed-off peers, so Leani's
  independently verified success/failure evidence—not snapshot presence or
  Reth reputation—controls hot admission, ranking, and pruning;
- keep an allocation-free, bounded in-memory quality snapshot for request-path
  ranking;
- bound candidate admission bookkeeping and discovery progress counters as
  well as the durable candidate and quality tables;
- send periodic and explicit flush snapshots to a dedicated persistence task,
  keeping SQLite work out of the network-manager event loop;
- flush after stopping qualification work during graceful shutdown;
- merge sibling standalone Mainnet stores before networking starts, preserving
  the newest candidate and monotonic verified material evidence;
- keep the network database separate from `leani.sqlite` and processor backups.

## Consequences

Persistence cost is proportional to changed quality rows and the current Reth
snapshot, updates are crash-atomic, `sources.live.peer_store_max_entries`
applies in memory and on disk, and warm-start ranking no longer allocates
hexadecimal peer-id strings. SQLite also makes future peer diagnostics and
service-specific selection measurable without adding another file format.

The first run after this change is cold because the pre-launch JSON files are
not migrated. Deleting `execution-network.sqlite` is always safe and forces a
fresh discovery run; deleting `execution-p2p-secret` also changes the node's
network identity.

## Evidence

Offline tests cover restart persistence, bounded pruning, preference for proven
body servers, subnet-diverse hot selection, sibling-store evidence merging, and
unsupported-schema failure. Public-Mainnet cold/warm benchmarks remain the
performance gate.

## Compatibility and rollback

Leani is pre-launch and peer state is disposable, so there is no JSON import or
dual-write compatibility path. Rollback requires a fresh peer discovery run by
the older binary; processor data is unaffected.
