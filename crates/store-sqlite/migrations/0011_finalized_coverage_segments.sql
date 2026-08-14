CREATE TABLE finalized_coverage_intervals (
    instance TEXT NOT NULL
        REFERENCES processor_instances(instance) ON DELETE CASCADE,
    start_block INTEGER NOT NULL,
    end_block INTEGER NOT NULL,
    segment_size INTEGER NOT NULL CHECK(segment_size > 0),
    encoding_version INTEGER NOT NULL CHECK(encoding_version = 1),
    metadata_digest BLOB NOT NULL CHECK(length(metadata_digest) = 32),
    created_at_unix_ms INTEGER NOT NULL,
    PRIMARY KEY (instance, start_block),
    CHECK(start_block <= end_block)
) WITHOUT ROWID, STRICT;

CREATE INDEX finalized_coverage_intervals_lookup
    ON finalized_coverage_intervals(instance, end_block, start_block);

CREATE TABLE finalized_coverage_segments (
    instance TEXT NOT NULL,
    interval_start INTEGER NOT NULL,
    segment_start INTEGER NOT NULL,
    segment_end INTEGER NOT NULL,
    start_parent_hash BLOB NOT NULL CHECK(length(start_parent_hash) = 32),
    end_hash BLOB NOT NULL CHECK(length(end_hash) = 32),
    metadata_digest BLOB NOT NULL CHECK(length(metadata_digest) = 32),
    PRIMARY KEY (instance, interval_start, segment_start),
    FOREIGN KEY (instance, interval_start)
        REFERENCES finalized_coverage_intervals(instance, start_block)
        ON DELETE CASCADE,
    CHECK(interval_start <= segment_start),
    CHECK(segment_start <= segment_end)
) WITHOUT ROWID, STRICT;

CREATE INDEX finalized_coverage_segments_lookup
    ON finalized_coverage_segments(instance, segment_end, segment_start);

CREATE TABLE finalized_coverage_owners (
    instance TEXT NOT NULL
        REFERENCES processor_instances(instance) ON DELETE CASCADE,
    owner_kind TEXT NOT NULL CHECK(owner_kind IN ('shared', 'subscription')),
    owner_id TEXT NOT NULL,
    from_block INTEGER NOT NULL,
    to_block INTEGER NOT NULL,
    PRIMARY KEY (instance, owner_kind, owner_id, from_block),
    CHECK(from_block <= to_block)
) WITHOUT ROWID, STRICT;

CREATE INDEX finalized_coverage_owners_lookup
    ON finalized_coverage_owners(instance, to_block, from_block);

ALTER TABLE backfill_subscriptions
    ADD COLUMN published_domain_changes INTEGER NOT NULL DEFAULT 0;

UPDATE node_meta
SET value = X'0000000b'
WHERE key = 'schema_version';
