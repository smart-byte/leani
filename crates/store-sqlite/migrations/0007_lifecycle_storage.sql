CREATE TABLE processor_state (
    instance TEXT NOT NULL
        REFERENCES processor_instances(instance) ON DELETE CASCADE,
    namespace TEXT NOT NULL,
    state_key BLOB NOT NULL,
    value BLOB NOT NULL,
    PRIMARY KEY (instance, namespace, state_key)
) WITHOUT ROWID, STRICT;

CREATE TABLE output_entity_meta (
    instance TEXT NOT NULL,
    collection TEXT NOT NULL,
    entity_key BLOB NOT NULL,
    block_number INTEGER NOT NULL,
    block_timestamp INTEGER NOT NULL,
    finality INTEGER NOT NULL CHECK (finality IN (0, 1, 2)),
    written_at_unix_ms INTEGER NOT NULL,
    value_bytes INTEGER NOT NULL,
    PRIMARY KEY (instance, collection, entity_key),
    FOREIGN KEY (instance, collection, entity_key)
        REFERENCES entities(instance, collection, entity_key)
        ON DELETE CASCADE
) WITHOUT ROWID, STRICT;

INSERT INTO output_entity_meta(
    instance, collection, entity_key, block_number, block_timestamp,
    finality, written_at_unix_ms, value_bytes
)
SELECT
    entities.instance,
    entities.collection,
    entities.entity_key,
    COALESCE(processor_cursors.block_number, 0),
    0,
    COALESCE(processor_cursors.finality, 0),
    processor_instances.created_at_unix_ms,
    length(entities.value)
FROM entities
JOIN processor_instances
  ON processor_instances.instance = entities.instance
LEFT JOIN processor_cursors
  ON processor_cursors.instance = entities.instance;

CREATE TABLE recovery_checkpoints (
    checkpoint_id INTEGER PRIMARY KEY AUTOINCREMENT,
    instance TEXT NOT NULL
        REFERENCES processor_instances(instance) ON DELETE CASCADE,
    block_number INTEGER NOT NULL,
    block_hash BLOB NOT NULL CHECK (length(block_hash) = 32),
    processor_cursor BLOB NOT NULL,
    state_snapshot BLOB NOT NULL,
    state_checksum BLOB NOT NULL CHECK (length(state_checksum) = 32),
    created_at_unix_ms INTEGER NOT NULL,
    UNIQUE (instance, block_number, block_hash)
) STRICT;

CREATE INDEX recovery_checkpoints_instance_block
    ON recovery_checkpoints(instance, block_number DESC, checkpoint_id DESC);

CREATE TABLE portable_savepoints (
    instance TEXT NOT NULL
        REFERENCES processor_instances(instance) ON DELETE CASCADE,
    savepoint_id TEXT NOT NULL,
    block_number INTEGER NOT NULL,
    block_hash BLOB NOT NULL CHECK (length(block_hash) = 32),
    processor_cursor BLOB NOT NULL,
    state_snapshot BLOB NOT NULL,
    state_checksum BLOB NOT NULL CHECK (length(state_checksum) = 32),
    created_at_unix_ms INTEGER NOT NULL,
    PRIMARY KEY (instance, savepoint_id)
) WITHOUT ROWID, STRICT;

CREATE TABLE query_snapshots (
    snapshot_id BLOB PRIMARY KEY NOT NULL CHECK (length(snapshot_id) = 16),
    instance TEXT NOT NULL
        REFERENCES processor_instances(instance) ON DELETE CASCADE,
    collection TEXT NOT NULL,
    boundary_sequence INTEGER NOT NULL,
    row_count INTEGER NOT NULL,
    value_bytes INTEGER NOT NULL,
    created_at_unix_ms INTEGER NOT NULL,
    expires_at_unix_ms INTEGER NOT NULL
) WITHOUT ROWID, STRICT;

CREATE TABLE query_snapshot_entities (
    snapshot_id BLOB NOT NULL
        REFERENCES query_snapshots(snapshot_id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL,
    entity_key BLOB NOT NULL,
    value BLOB NOT NULL,
    block_number INTEGER NOT NULL,
    block_timestamp INTEGER NOT NULL,
    finality INTEGER NOT NULL CHECK (finality IN (0, 1, 2)),
    PRIMARY KEY (snapshot_id, ordinal),
    UNIQUE (snapshot_id, entity_key)
) WITHOUT ROWID, STRICT;

ALTER TABLE durable_consumers
    ADD COLUMN credential_hash BLOB
    CHECK (credential_hash IS NULL OR length(credential_hash) = 32);

UPDATE node_meta
SET value = X'00000007'
WHERE key = 'schema_version';
