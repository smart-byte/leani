# Architecture decision records

Use four-digit sequential filenames such as
`0001-durable-envelope-encoding.md`. An ADR is immutable after acceptance;
superseding decisions link to the old record.

Required sections:

```text
# Title
Status: proposed | accepted | superseded
Date: YYYY-MM-DD

## Context
## Decision
## Consequences
## Evidence
## Compatibility and rollback
```

Accepted records:

- [0001 — License and contract versioning](0001-license-and-contract-versioning.md)
- [0002 — Direct Arrow Xatu reader](0002-direct-arrow-xatu-reader.md)
- [0003 — Verified Beacon API finality](0003-verified-beacon-api-finality.md)
- [0004 — Direct Reth P2P boundary](0004-direct-reth-p2p-boundary.md)
- [0005 — SQLite transaction overlay](0005-sqlite-transaction-overlay.md)
- [0006 — Durable bounded historical runtime](0006-durable-bounded-historical-runtime.md)
- [0007 — Native runtime first; Firehose compatibility at the edge](0007-native-runtime-firehose-boundary.md)
- [0008 — Native API is the v1 integration boundary](0008-native-api-is-the-v1-integration-boundary.md)
- [0009 — Native consensus P2P finality](0009-native-consensus-p2p-finality.md)
- [0010 — Persistent execution P2P and late archive reconciliation](0010-persistent-p2p-and-late-archive-reconciliation.md)
- [0011 — Process-wide execution P2P manager](0011-process-wide-execution-p2p-manager.md)
- [0012 — Two-tier execution peer startup](0012-two-tier-execution-peer-startup.md)
- [0013 — SQLite execution peer store](0013-sqlite-execution-peer-store.md)
- [0014 — Opportunistic execution peer qualification](0014-opportunistic-execution-peer-qualification.md)
- [0015 — Single-owner execution Discv4 discovery](0015-single-owner-execution-discv4.md)
- [0016 — Requested coverage in output snapshots](0016-query-snapshot-requested-coverage.md)
- [0017 — MIT license](0017-mit-license.md)
