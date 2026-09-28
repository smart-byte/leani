# ADR 0018: Forward-only store migrations to schema 23

Status: accepted

Date: 2026-09-27

## Context

The release after 0.1.0-rc.1 moves node stores from schema 21 to schema 23,
and the release process requires an ADR before every irreversible migration.

- Schema 22 adds three lookup indexes, so health, metrics, and status reads
  find a processor's finalized height and a block hash's coverage with index
  searches instead of table scans.
- Schema 23 adds `node_secrets`: one random 32-byte secret per store, drawn
  from the operating system inside the migration transaction. Consumer
  session tokens carry a MAC keyed from it, and consumer credential hashes
  are keyed with it. The migration keys the bare credential digests schema 22
  stored, so existing credentials keep working. It also raises every durable
  consumer's delivered sequence to its stream head, because acknowledgements
  may no longer pass what a consumer was delivered, and changes delivered
  before the upgrade were never recorded.

Neither step can be reversed from the upgraded store alone: the keyed
credential hashes cannot be turned back into the bare digests, and the
delivered sequences they replaced are gone. A binary that supports only an
earlier schema refuses the upgraded store.

The upgrade also meets processor identity changes of the same release,
such as `blobs-money` 1.5.0 or `evm-events` 1.1.0 under an instance that
holds 1.4.0 or 1.0.0. The store refuses those at registration. Until this
decision, a start migrated the store first and refused afterwards, so a
refused start still left a store that the previous binary could not open,
and `leani db backup` upgraded the store it copied.

## Decision

Store migrations are forward only.

- Each schema adds one numbered migration,
  `crates/store-sqlite/migrations/00NN_*.sql`. Opening a store applies every
  missing migration and the schema version in one `IMMEDIATE` transaction, so
  an interrupted upgrade leaves the earlier schema.
- There are no down migrations. A store newer than the binary is refused, and
  so is one older than the oldest upgradable schema, 21, which must be
  re-initialized.
- Before a command that registers processors (`serve`, `backfill`, the
  runtime of `leani subscribe`, `leani e2e mainnet --resume`) upgrades an
  older store, it reads the store through a read-only connection and applies
  registration's identity rule to each processor it will register. A
  processor the store holds under a conflicting identity is refused with
  registration's error, and the store keeps its schema. The rule stays in one
  place, `check_stored_identity`; only lifecycle policy may differ.
- `leani db backup` never opens the store as a store. It copies the file at
  its schema with `VACUUM INTO` through a read-only connection, refuses a
  schema newer than the binary supports, and refuses a store that references
  processor artifact segments, which a SQLite copy would omit.

## Consequences

An operator can always return to the previous release with the store it
wrote, as long as that store was not upgraded: after a refused start, and
with any backup taken before the first successful start. After the upgrade,
rolling back means restoring the pre-upgrade backup with the previous binary
and losing what the node indexed since; the upgraded store itself cannot be
downgraded.

The preflight opens one read-only connection whenever an existing store is
opened with configured processors, to read its schema version, and adds one
point lookup per configured processor only when that schema is older than the
binary's. Other `leani db` commands, such as `inspect`, `verify`, and
`compact`, still upgrade the store they open.

## Evidence

Store tests cover the upgrade and rollback paths:
`schema_21_stores_upgrade_in_place_with_coverage_indexes`,
`schema_22_stores_upgrade_with_delivered_watermarks_and_a_node_secret`,
`an_identity_refusal_leaves_an_older_store_at_its_schema`,
`a_backup_leaves_an_older_store_and_its_copy_at_their_schema`, and
`backup_reopens_as_a_complete_verified_restore`. The node test
`the_container_profile_starts_beside_the_instance_an_earlier_release_left`
starts the shipped container profile on a store that holds rc.1's
`blobs-container` instance.

## Compatibility and rollback

Before upgrading from 0.1.0-rc.1, stop the node and every `leani subscribe`
run on the data directory, then copy the data directory or run
`leani db backup` with the rc.1 binary; this release's `db backup` also keeps
the schema. Tiered artifact segments need a copy of the whole data directory.
Rebuild processors whose identity changed under new `instance` IDs; the old
instances' rows stay in the store and count toward its physical budget. To
roll back after the upgrade, stop the node, move the upgraded store aside
with its `-wal` and `-shm` files, and restore the backup with the rc.1
binary.
