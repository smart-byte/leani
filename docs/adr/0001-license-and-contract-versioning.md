# Dual license and contract versioning

Status: accepted

Date: 2026-08-01

The licensing decision below is superseded by
[0017 — MIT license](0017-mit-license.md). The contract-versioning decision
remains in effect.

## Context

The project is a Rust infrastructure component intended to integrate with
Ethereum ecosystem libraries and applications. Its durable data and client
contracts will evolve before v1.

## Decision

Source code is offered under MIT or Apache-2.0 at the recipient's option.
Native API, SDK, processor descriptors, cursors, and durable records are
versioned explicitly. Pre-v1 compatibility may change only with fixtures and a
documented migration or rebuild path; v1 public contracts follow semantic
versioning.

## Consequences

Contributors must accept dual licensing. Binary distribution must preserve both
license notices. Persisted data never relies only on a Rust type layout.

## Evidence

CI runs dependency source, advisory, and license policy checks. Durable
encoding golden tests are a release gate.

## Compatibility and rollback

License terms are not rolled back. Contract migrations retain an export or
rebuild path, and unsupported schema versions fail closed.
