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
- match the manifest chain and declared material capabilities.

Reads enforce the normal source byte, frame, and frame-count budgets. The
adapter verifies the complete object before yielding frames and adds immutable
object identity to frame provenance. A missing line, checksum mismatch,
truncated frame, parent mismatch, schema mismatch, or range gap fails closed.

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

When `rpc.historical_mode = "on_demand"`, the same source can answer bounded
historical RPC without importing frames into SQLite. Exact block and receipt
responses additionally require complete canonical header RLP, EIP-2718
transaction bytes, typed receipt bytes, senders, receipt gas fields, and
withdrawals whenever the header commits to a withdrawals root. The manifest
must declare those capabilities complete. A blobs-only Xatu projection is
correctly rejected for generic RPC because its predicate-filtered transaction
material is not a complete block body.
