CREATE TABLE processor_artifacts (
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
    retained_at_unix_ms INTEGER NOT NULL,
    PRIMARY KEY (instance, block_number)
) WITHOUT ROWID, STRICT;

CREATE INDEX processor_artifacts_instance_timestamp
    ON processor_artifacts(instance, block_timestamp, block_number);

CREATE TABLE processor_artifact_owners (
    instance TEXT NOT NULL,
    block_number INTEGER NOT NULL,
    owner_kind TEXT NOT NULL
        CHECK(owner_kind IN ('processor_instance', 'processor_job', 'operator_pin')),
    owner_id TEXT NOT NULL,
    created_at_unix_ms INTEGER NOT NULL,
    PRIMARY KEY (instance, block_number, owner_kind, owner_id),
    FOREIGN KEY (instance, block_number)
        REFERENCES processor_artifacts(instance, block_number) ON DELETE CASCADE
) WITHOUT ROWID, STRICT;

CREATE INDEX processor_artifact_owners_by_owner
    ON processor_artifact_owners(owner_kind, owner_id, instance, block_number);

UPDATE node_meta
SET value = X'0000000E'
WHERE key = 'schema_version';
