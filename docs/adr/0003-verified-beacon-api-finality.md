# Verified Beacon API finality

Status: accepted

Date: 2026-08-01

## Context

The execution P2P network can provide blocks, bodies, and receipts, but
post-Merge canonicality and finality come from Ethereum consensus. Trusting a
Beacon API response or an endpoint quorum without validating its light-client
proofs would silently turn the endpoint into a consensus authority.

## Decision

The first finality backend uses standard Beacon API light-client routes only
as transport. It pins Helios consensus-core revision
`204c998a927348e1c000a664f08d5b37b1b0d924` and locally verifies:

- the explicit weak-subjectivity checkpoint root and sync-committee branch;
- checkpoint freshness;
- sync-committee transitions and BLS aggregate signatures;
- finalized-header Merkle branches;
- execution-payload-header branches and the resulting execution block hash.

Each configured endpoint is verified independently. Agreement is evaluated
after proof validation, and any same-slot root or execution-hash contradiction
fails closed. The required endpoint agreement is configurable; one
cryptographically verified endpoint is sufficient for integrity, while
multiple endpoints improve availability and detect inconsistent transports.

## Consequences

This development/staging backend requires an operator-supplied recent
checkpoint and at least one free or local Beacon API. It does not require a
paid or trusted RPC provider. The pinned verifier adds BLS and SSZ
dependencies, but those remain isolated in `finality-beacon-api`.

The adapter currently implements mainnet constants explicitly. Other networks
must add their genesis root/time and complete fork schedule before they can be
selected.

## Evidence

Two independent checkpoint providers reported finalized root
`0x9d9680a07a3cca62f39cc2a1bd31b7cfd3bb06ed27630142a593276fdd106619`.
The probe then used a separate public Beacon API transport and verified the
checkpoint bootstrap, a period update, and a finality update. It selected
beacon slot `14893472`, execution block `25656565`, and execution hash
`0x8ae554c545b50c663537e32494b11fef5887d8d090600b63873caa22c6a2019b`.
The same run advanced through two verified light-client updates to finalized
beacon slot `14893568`, execution block `25656660`, and execution hash
`0xf3fc94346619c19caef04425fd2a7925f9dc4191aeae5ed933e4a1cec418a914`.

## Compatibility and rollback

`FinalitySource`, `FinalityEvent`, and `ConsensusCheckpoint` contain no Helios
or HTTP types. A consensus-P2P light client can replace this adapter without
changing processors, the fork graph, storage, API, or SDK. Endpoint failure
never causes fabricated safe/finalized progress.
