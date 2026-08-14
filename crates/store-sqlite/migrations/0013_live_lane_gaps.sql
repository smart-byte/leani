CREATE TABLE live_lane_gaps (
    instance TEXT PRIMARY KEY NOT NULL
        REFERENCES processor_instances(instance) ON DELETE CASCADE,
    first_unapplied_block INTEGER NOT NULL,
    first_unapplied_hash BLOB NOT NULL CHECK (length(first_unapplied_hash) = 32),
    first_unapplied_parent_hash BLOB NOT NULL CHECK (length(first_unapplied_parent_hash) = 32),
    first_unapplied_timestamp INTEGER NOT NULL,
    required_delivery_bytes INTEGER NOT NULL DEFAULT 0,
    reason TEXT NOT NULL,
    created_at_unix_ms INTEGER NOT NULL,
    updated_at_unix_ms INTEGER NOT NULL
) WITHOUT ROWID, STRICT;

UPDATE node_meta
SET value = X'0000000D'
WHERE key = 'schema_version';
