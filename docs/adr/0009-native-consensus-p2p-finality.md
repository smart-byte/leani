# Native consensus P2P finality

Status: accepted

Date: 2026-08-01

## Context

The verified Beacon API backend removes trust in an RPC provider, but it still
requires an HTTP consensus transport. The independent deployment profile needs
to derive post-Merge execution finality from Ethereum consensus without a
paid, centralized, or separately operated API.

## Decision

Add a mainnet-only `FinalitySource` that implements the standardized consensus
light-client P2P surface: discv5 discovery, libp2p Noise/Yamux, immediate
Status exchange, Ping and Goodbye control handling, and SSZ-snappy
bootstrap/update/finality request-response protocols. Signed official
bootnodes are embedded and can be replaced by operator-provided ENRs.

Peers remain untrusted transports. The adapter shares the pinned
`helios-consensus-core` state machine with the Beacon API backend and locally
verifies the weak-subjectivity bootstrap, sync committees, BLS aggregate
signatures, finality branches, and execution-payload branches. Request and
wire sizes, discovery, connection attempts, retries, and time are bounded.
Fulu EIP-7892 blob-parameter-only fork digests are calculated from the pinned
mainnet schedule.

The configuration must include both the checkpoint root and its finalized
beacon slot. That pair provides an internally consistent initial Status
message. After verification advances, reconnects advertise the latest locally
verified anchor.

## Consequences

The self-contained profile needs outbound UDP for discovery and outbound TCP
for libp2p, but no Beacon API or execution RPC. Peer support for light-client
serving is uneven, so the client validates peers immediately, fails over,
redials, and performs one bounded network reconnect after an operation-level
failure. It never converts transport failure into finality.

Mainnet constants and blob schedules are pinned. Supporting another network or
future fork requires explicit constants, schemas, digest vectors, and
conformance evidence.

## Evidence

Unit tests cover signed/dialable embedded ENRs, strict protobuf varints,
SSZ-snappy limits, Status layout, response codes, and fork-context handling.
A real Mainnet probe using a recent explicit Nimbus ENR verified checkpoint
slot `14894752` and advanced to finalized slot `14894880`, execution block
`25657968`. A separate probe using only embedded official bootnodes verified
the same checkpoint and advanced to slot `14894912`, execution block
`25658000`. The second probe exercised bounded reconnect after the initial
peer set dropped.

These are bounded protocol probes. Period-transition, fault-injection,
seven-day staging, and fourteen-day independent soak evidence remain release
gates.

## Compatibility and rollback

The backend implements the existing `FinalitySource` contract, so processors,
storage, runtime, API, and SDK do not depend on its P2P types. Operators can
switch back to `beacon_api` without migrating durable processor data.
