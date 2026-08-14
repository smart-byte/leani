CREATE TABLE processor_artifact_segment_owners (
    segment_id TEXT NOT NULL
        REFERENCES processor_artifact_segments(segment_id) ON DELETE CASCADE,
    owner_kind TEXT NOT NULL
        CHECK(owner_kind IN ('processor_instance', 'processor_job', 'operator_pin')),
    owner_id TEXT NOT NULL,
    from_block INTEGER NOT NULL,
    to_block INTEGER NOT NULL CHECK(to_block >= from_block),
    created_at_unix_ms INTEGER NOT NULL,
    PRIMARY KEY(segment_id, owner_kind, owner_id, from_block, to_block)
) WITHOUT ROWID, STRICT;

CREATE INDEX processor_artifact_segment_owners_by_owner
ON processor_artifact_segment_owners(owner_kind, owner_id, segment_id);

-- V16 was never released. Preserve any development database by copying exact
-- block claims before removing the redundant per-block segment metadata.
INSERT INTO processor_artifact_segment_owners(
    segment_id, owner_kind, owner_id, from_block, to_block, created_at_unix_ms
)
SELECT
    segments.segment_id, owners.owner_kind, owners.owner_id,
    owners.block_number, owners.block_number, owners.created_at_unix_ms
FROM processor_artifact_segments AS segments
JOIN processor_artifact_owners AS owners
  ON owners.instance = segments.instance
 AND owners.block_number BETWEEN segments.from_block AND segments.to_block
WHERE segments.state = 'active';

DELETE FROM processor_artifacts WHERE payload_tier = 'segment';

UPDATE node_meta
SET value = X'00000011'
WHERE key = 'schema_version';
