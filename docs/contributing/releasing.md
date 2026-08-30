---
title: Release process
description: Cut and verify coordinated Leani binaries, crates, SDK, container, Homebrew, and release-pinned documentation.
section: contributing
order: 60
audience:
  - contributor
status: preview
---

1. Complete the release-readiness checklist and attach the local evidence report.
2. Run the full Rust, SDK, dependency-policy, migration, backup/restore, and
   reproducibility suites from a clean checkout.
3. Update `CHANGELOG.md`, OpenAPI/SDK compatibility tables, durable schema
   versions, and processor version declarations.
4. Build Linux `x86_64` and `aarch64` artifacts and the OCI image from the same
   signed commit.
5. Verify checksums and a clean-host smoke test.
6. Create an annotated semantic-version tag. Release candidates use
   `-rc.N`; a stable release requires every artifact listed above.

Rollback means deploying the previous binary with a store format it supports
or restoring the pre-migration backup. Every irreversible migration must be
preceded by a tested export/rebuild path and an ADR.
