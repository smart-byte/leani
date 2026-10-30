# Sepolia Gloas light-client fixtures

Real, signed Ethereum Sepolia light-client responses from after the Gloas
fork, in the standard Beacon API JSON format. Gloas headers prove their
execution block by hash alone, through an execution block hash branch. The
finality tests of `leani-finality-beacon-api` verify them end to end with the
pinned Helios verifier.

- Source: the Lodestar Sepolia Beacon API, `https://lodestar-sepolia.chainsafe.io/`
- Files are the responses as served, unmodified:

| File | SHA-256 | Content |
|---|---|---|
| `bootstrap.json` | `335e7227d77f91f08019a442656c677dc58cd6ccf2f93090f960cfb1e7810d57` | Bootstrap for block root `0xc6729545da5712f6cfc14e95d1f14e012ceea0d4568ffad921a16353d342910a`, slot 11,472,352 (sync-committee period 1,400), execution block hash `0x78965ab07ff409e5799e5134046d25e81788e21c26e79f22ab208b6aa862ae89` |
| `finality.json` | `04d2767f27cdae9cc4580baad7f62feaa553061bac120cd7d3a4d47956361090` | Finality update signed at slot 11,472,417, attesting slot 11,472,416 and finalizing slot 11,472,352 |
| `optimistic.json` | `1aa60456d40070be9af5692011989c25f411444c0db684cb0810977568e306f9` | Optimistic update signed at slot 11,472,427 by 495 of 512 sync-committee members, attesting slot 11,472,426, execution block hash `0x028a03f7955a376ffd20dde8af1fa2b6bd473e053dbbb57d3acf74970ec5e70f` |
