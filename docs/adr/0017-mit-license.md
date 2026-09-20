# MIT license

Status: accepted

Date: 2026-09-20

## Context

Leani's engine, Rust libraries, built-in processors, TypeScript SDK, and site
should use one permissive license with consistent package and release metadata.

## Decision

Leani-authored code is licensed under the MIT License in the repository's
root `LICENSE` file. This supersedes the licensing portion of
[ADR 0001](0001-license-and-contract-versioning.md).
The contract-versioning rules in ADR 0001 remain in effect.

## Consequences

Contributions to Leani are licensed under MIT. Distribution must preserve the
MIT copyright and permission notice. Dependencies and third-party data retain
their own licenses and applicable attribution requirements.

The Rust workspace, SDK, site, Homebrew formula, and native release archives
declare or include the MIT license consistently.

## Evidence

The workspace and package manifests declare `MIT`. The root and SDK license
texts match, and the native release workflow includes the root `LICENSE`.

## Compatibility and rollback

This decision changes no runtime, API, or data-format contract. Versions
already distributed under earlier license terms retain those grants.
