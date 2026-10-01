---
title: Verify a release
description: Check Leani archives, container images, SBOMs, and packages against their published checksums and build provenance before deploying them.
section: operations
order: 15
audience:
  - operator
  - app-developer
status: preview
---

Each release is built once from a tagged commit and published without being
rebuilt. Check what you download against that build before deploying it.

## Release archives

The [GitHub release](https://github.com/smart-byte/leani/releases) carries the
native archives, `SHA256SUMS`, the CycloneDX SBOMs including the node's
`leani.cdx.json`, and the `THIRD_PARTY_LICENSES.txt` dependency notices. Check a
downloaded archive against the checksum list and its build provenance:

```bash
sha256sum --check --ignore-missing SHA256SUMS
gh attestation verify leani-v0.1.0-x86_64-unknown-linux-gnu.tar.gz --repo smart-byte/leani
```

The attestation names the release tag, its commit, and the publication run,
which checked that the archive came from a successful preparation run of that
commit.

## Container images

Pin a release's image digest for deployment. The publication job attests the
multiarchitecture image and each architecture's image. It binds each
architecture's CycloneDX SBOM to that image's digest instead of attaching it to
the GitHub release, so the SBOM can be verified against the exact image. The
attestations are stored with the images in GHCR as well:

```bash
gh attestation verify oci://ghcr.io/smart-byte/leani:v0.1.0 --repo smart-byte/leani
gh attestation verify oci://ghcr.io/smart-byte/leani@sha256:<amd64-digest> --repo smart-byte/leani
gh attestation verify oci://ghcr.io/smart-byte/leani@sha256:<amd64-digest> \
  --repo smart-byte/leani --predicate-type https://cyclonedx.org/bom
```

## Packages

`@smart-byte/leani-sdk` is published to npm with provenance from the
repository's `sdk-release.yml` workflow; `npm audit signatures` checks it in a
project that installed it. The four Rust authoring libraries are published to
crates.io through trusted publishing from `crates-release.yml`.

## Release channels

Prerelease versions are marked as GitHub prereleases and do not replace the
latest stable release. Prerelease SDK versions use npm's `next` tag; stable
versions use `latest`. Until the first stable release, `latest` stays on the
last prerelease that received it: npm gave `latest` to the package's first
version, `0.1.0-rc.1`, and later prereleases move only `next`. An unversioned
install therefore resolves to that prerelease; install a newer prerelease by
exact version or with `@next`. The first stable release moves `latest` to
itself. The Homebrew formula is updated in a separate reviewed step after each
release's archives are public.
