CREATE TABLE processor_artifact_candidates (
    instance TEXT NOT NULL
        REFERENCES processor_instances(instance) ON DELETE CASCADE,
    chain_id INTEGER NOT NULL,
    block_number INTEGER NOT NULL,
    block_hash BLOB NOT NULL CHECK (length(block_hash) = 32),
    parent_hash BLOB NOT NULL CHECK (length(parent_hash) = 32),
    block_timestamp INTEGER NOT NULL,
    delta_schema_version INTEGER NOT NULL,
    delta_checksum BLOB NOT NULL CHECK (length(delta_checksum) = 32),
    encoded_delta BLOB NOT NULL,
    staged_at_unix_ms INTEGER NOT NULL,
    PRIMARY KEY (instance, block_number, block_hash)
) WITHOUT ROWID, STRICT;

CREATE INDEX processor_artifact_candidates_instance_block
    ON processor_artifact_candidates(instance, block_number);

UPDATE node_meta
SET value = X'0000000F'
WHERE key = 'schema_version';
