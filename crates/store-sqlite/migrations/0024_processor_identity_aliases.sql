-- Snapshot identities an instance had before a requirement narrowing. A
-- narrowed processor reads the same fields it declared before, so checkpoints
-- and savepoints taken under the superseded descriptor stay restorable.
CREATE TABLE processor_identity_aliases (
    instance TEXT NOT NULL
        REFERENCES processor_instances(instance) ON DELETE CASCADE,
    descriptor_hash BLOB NOT NULL CHECK(length(descriptor_hash) = 32),
    created_at_unix_ms INTEGER NOT NULL,
    PRIMARY KEY (instance, descriptor_hash)
) WITHOUT ROWID;

UPDATE node_meta SET value = X'00000018' WHERE key = 'schema_version';
