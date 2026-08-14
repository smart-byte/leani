# Release process

1. Complete the milestone gate and attach its evidence report.
2. Run the full Rust, SDK, dependency-policy, migration, backup/restore, and
   reproducibility suites from a clean checkout.
3. Update `CHANGELOG.md`, OpenAPI/SDK compatibility tables, durable schema
   versions, and processor version declarations.
4. Build Linux `x86_64` and `aarch64` artifacts and the OCI image from the same
   signed commit.
5. Verify checksums and a clean-host smoke test.
6. Create an annotated semantic-version tag. Release candidates use
   `-rc.N`; v1 requires every Gate 5 artifact.

Rollback means deploying the previous binary with a store format it supports
or restoring the pre-migration backup. Every irreversible migration must be
preceded by a tested export/rebuild path and an ADR.
