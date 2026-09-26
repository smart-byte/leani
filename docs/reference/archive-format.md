---
title: Normalized frame archive
description: Reference the deterministic, checksummed archive format used to move normalized Leani frame segments.
section: reference
order: 80
audience:
  - operator
  - contributor
status: preview
---

Status: implemented interchange format v1.

The `archive` history source lets the node backfill from local, checksummed
objects without retaining raw chain history. It is deliberately source-neutral:
an Amazon Parquet, Xatu, EraE, BigQuery, or custom adapter can project its input
to normalized `BlockFrame` JSON Lines and then use the same runtime and
processors.

Example history configuration:

```toml
[[sources.history]]
id = "offline-frames"
kind = "archive"
priority = 5
trust = "trusted_dataset"
manifest = "./archives/mainnet/manifest.json"
```

Manifest:

```json
{
  "format_version": 1,
  "id": "mainnet-public-data-2026-08-01",
  "chain_id": 1,
  "schema_version": "normalized-frame-jsonl.v1",
  "capabilities": ["Header", "Transactions", "Receipts", "Logs"],
  "complete_capabilities": ["Header", "Transactions", "Receipts", "Logs"],
  "finality": "Finalized",
  "objects": [
    {
      "from_block": 19426589,
      "to_block": 19427588,
      "path": "19426589-19427588.jsonl",
      "blake3": "lowercase-64-hex-digits",
      "bytes": 1234567
    }
  ]
}
```

Each non-empty object line is one JSON-encoded `BlockFrame`. Objects must:

- use simple relative paths (no absolute paths or `..`);
- have the exact declared byte length and BLAKE3 digest;
- contain exactly one frame per block in their declared range;
- be ordered, contiguous, and parent-linked;
- match the manifest chain, finality, and declared material capabilities;
- be reached without crossing a symbolic link below the manifest's
  directory.

Reads enforce the normal source byte, frame, and frame-count budgets, and
hold one line at a time. The adapter verifies the complete object's length
and BLAKE3 digest before it yields any frame; a length or checksum mismatch
fails the read then. It then streams the frames from the same open file,
hashing it again as it goes, and adds immutable object identity to their
provenance. A structural failure, such as a truncated frame, a block out of
order, missing, or repeated, a parent mismatch, a schema mismatch, or a
chain or finality mismatch, fails the read as corrupt where it is found,
after the frames before it may already have been applied; a later source or
a rerun resumes after them. An object edited in place between the two passes
fails with the checksum error only after its frames.

Frames report only the checks the archive made: the object's BLAKE3 digest,
and the parent link to the previous frame of the same object. A frame's own
verification claims and consensus anchor are dropped, and the trust of its
earlier provenance is capped at `trusted_dataset`.

The archive is a trusted-dataset source, not automatically a cryptographic raw
history source. An EraE adapter must additionally decode raw bodies/receipts,
verify their commitment roots, and anchor canonicality before advertising
`verified_material`.

Backfill any configured processor:

```text
leani backfill \
  --processor erc20-balances \
  --from 12345678 \
  --to 12346677
```

The runtime records coverage and discards input frames after reduction. Only
processor output, undo/change journals, and job metadata remain in SQLite.
An ordered processor such as `erc20-balances` applies its history
contiguously from `start_block` upward, so `--from` must be its `start_block`
or the block after the last one it applied.

When `rpc.historical_mode = "on_demand"`, the same source can answer bounded
historical RPC without importing frames into SQLite. Exact block and receipt
responses additionally require complete canonical header RLP, EIP-2718
transaction bytes, typed receipt bytes, senders, receipt gas fields, and
withdrawals whenever the header commits to a withdrawals root. The manifest
must declare those capabilities complete. A blobs-only Xatu projection is
correctly rejected for generic RPC because its predicate-filtered transaction
material is not a complete block body.
