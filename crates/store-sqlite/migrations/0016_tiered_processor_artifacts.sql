ALTER TABLE processor_artifacts
ADD COLUMN payload_tier TEXT NOT NULL DEFAULT 'inline'
    CHECK(payload_tier IN ('inline', 'segment'));

ALTER TABLE processor_artifacts
ADD COLUMN encoded_bytes INTEGER NOT NULL DEFAULT 0 CHECK(encoded_bytes >= 0);

UPDATE processor_artifacts SET encoded_bytes = length(encoded_delta);

CREATE TABLE processor_artifact_segments (
    segment_id TEXT PRIMARY KEY,
    instance TEXT NOT NULL
        REFERENCES processor_instances(instance) ON DELETE CASCADE,
    chain_id INTEGER NOT NULL,
    from_block INTEGER NOT NULL,
    to_block INTEGER NOT NULL CHECK(to_block >= from_block),
    relative_path TEXT NOT NULL UNIQUE,
    artifacts INTEGER NOT NULL CHECK(artifacts > 0),
    logical_bytes INTEGER NOT NULL CHECK(logical_bytes > 0),
    physical_bytes INTEGER NOT NULL CHECK(physical_bytes > 0),
    records_checksum BLOB NOT NULL CHECK(length(records_checksum) = 32),
    state TEXT NOT NULL DEFAULT 'active' CHECK(state IN ('active', 'deleting')),
    created_at_unix_ms INTEGER NOT NULL,
    UNIQUE(instance, from_block, to_block)
) STRICT;

CREATE INDEX processor_artifact_segments_by_range
ON processor_artifact_segments(instance, from_block, to_block);

UPDATE node_meta
SET value = X'00000010'
WHERE key = 'schema_version';
