---
title: Preview limitations
description: Know Leani's current production boundaries, unsupported behavior, and explicit operator obligations before deploying it.
section: operations
order: 0
audience:
  - app-developer
  - operator
  - processor-author
status: preview
sidebar:
  badge:
    text: Read first
    variant: caution
---

Leani is pre-release software. The first public preview is intended for
evaluation, processor development, reproducible backfills, and guarded
application integrations with an independent fallback.

- It is not a complete Ethereum archive node and does not maintain EVM state.
  Arbitrary `eth_call`, balances, storage, proofs, tracing, and state-diff RPC
  are not served by the EVM-free core.
- Xatu rows are trusted public dataset input. Sparse EraE material verifies
  execution commitments but still depends on an external catalog until the
  planned consensus-root anchoring is complete. Source trust is exposed and
  must not be described as fully trustless history.
- Raw RPC history is post-merge exact. Ommer-aware pre-merge exactness is
  deferred.
- Only configured processor material and bounded recent/raw-history profiles
  are queryable. Pruned material must be reacquired.
- The native browser SDK requires same-origin hosting or an application-owned
  reverse proxy with an explicit CORS policy.
- Mainnet multi-day soak, chaos, abrupt-power-loss, broad cross-source parity,
  and every advertised host sweep remain release gates for a production claim.
- API bearer authentication is optional and JSON-RPC has no built-in access
  control. Non-loopback deployments must use a trusted network or authenticated
  reverse proxy and must not expose admin/debug endpoints publicly.

Treat this page and the [release process](https://leani.dev/docs/contributing/releasing/) as the public scope
boundary for the current preview.
