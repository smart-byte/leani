-- Stored finality becomes the level's rank: unattested 0, attested 1,
-- confirmed 2, finalized 3, settled 4. Finalized rows move from 2 to 3 and
-- the change log's settled rows from 3 to 4; blocks and outputs never hold
-- settled. The CHECK constraints, which SQLite cannot alter, are widened by
-- rebuilding their tables.

-- Dropping change_log cascades into sink_outbox, so its rows are set aside
-- and emptied first: the cascade then finds no child for any change.
CREATE TEMP TABLE sink_outbox_kept AS SELECT * FROM sink_outbox;
DELETE FROM sink_outbox;

CREATE TABLE change_log_ranked (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    chain_id INTEGER NOT NULL,
    instance TEXT NOT NULL
        REFERENCES processor_instances(instance) ON DELETE CASCADE,
    block_number INTEGER NOT NULL,
    block_hash BLOB NOT NULL CHECK (length(block_hash) = 32),
    direction TEXT NOT NULL CHECK (direction IN ('apply', 'undo', 'finalized', 'reset_required')),
    kind TEXT NOT NULL,
    entity_key BLOB NOT NULL,
    operation INTEGER NOT NULL CHECK (operation IN (0, 1)),
    payload BLOB NOT NULL,
    created_at_unix_ms INTEGER NOT NULL,
    parent_hash BLOB NOT NULL
        DEFAULT X'0000000000000000000000000000000000000000000000000000000000000000'
        CHECK (length(parent_hash) = 32),
    block_timestamp INTEGER NOT NULL DEFAULT 0,
    finality INTEGER NOT NULL DEFAULT 0 CHECK (finality BETWEEN 0 AND 4),
    encoding_version INTEGER NOT NULL DEFAULT 1 CHECK (encoding_version = 1),
    stream_id TEXT NOT NULL DEFAULT '',
    stream_sequence INTEGER NOT NULL DEFAULT 0,
    origin_kind TEXT NOT NULL DEFAULT 'live'
        CHECK (origin_kind IN ('live', 'live_recovery', 'historical_backfill', 'recompute')),
    origin_id TEXT NOT NULL DEFAULT '',
    publication_revision INTEGER NOT NULL DEFAULT 0 CHECK (publication_revision >= 0)
) STRICT;

INSERT INTO change_log_ranked(
    sequence, chain_id, instance, block_number, block_hash, direction, kind,
    entity_key, operation, payload, created_at_unix_ms, parent_hash,
    block_timestamp, finality, encoding_version, stream_id, stream_sequence,
    origin_kind, origin_id, publication_revision
)
SELECT
    sequence, chain_id, instance, block_number, block_hash, direction, kind,
    entity_key, operation, payload, created_at_unix_ms, parent_hash,
    block_timestamp, CASE finality WHEN 2 THEN 3 WHEN 3 THEN 4 ELSE finality END,
    encoding_version, stream_id, stream_sequence, origin_kind, origin_id,
    publication_revision
FROM change_log;

-- Sequences are never reused, even past changes pruning deleted: carry the
-- AUTOINCREMENT high-water mark, which can exceed the largest kept sequence.
DELETE FROM sqlite_sequence WHERE name = 'change_log_ranked';
INSERT INTO sqlite_sequence(name, seq)
SELECT 'change_log_ranked', seq FROM sqlite_sequence WHERE name = 'change_log';

DROP TABLE change_log;
ALTER TABLE change_log_ranked RENAME TO change_log;

CREATE INDEX change_log_instance_sequence
    ON change_log(instance, sequence);
CREATE UNIQUE INDEX change_log_stream_sequence_unique
    ON change_log(stream_id, stream_sequence);
CREATE UNIQUE INDEX change_log_backfill_completion
    ON change_log(stream_id)
    WHERE kind = 'system.backfill_complete';
CREATE INDEX change_log_stream_origin_sequence
    ON change_log(stream_id, origin_kind, origin_id, publication_revision, stream_sequence);
CREATE INDEX change_log_stream_created
    ON change_log(stream_id, created_at_unix_ms, stream_sequence);
CREATE INDEX change_log_stream_block
    ON change_log(stream_id, block_number, stream_sequence);

INSERT INTO sink_outbox SELECT * FROM sink_outbox_kept;
DROP TABLE sink_outbox_kept;

CREATE TABLE output_entity_meta_ranked (
    instance TEXT NOT NULL,
    collection TEXT NOT NULL,
    entity_key BLOB NOT NULL,
    block_number INTEGER NOT NULL,
    block_timestamp INTEGER NOT NULL,
    finality INTEGER NOT NULL CHECK (finality IN (0, 1, 2, 3)),
    written_at_unix_ms INTEGER NOT NULL,
    value_bytes INTEGER NOT NULL,
    PRIMARY KEY (instance, collection, entity_key),
    FOREIGN KEY (instance, collection, entity_key)
        REFERENCES entities(instance, collection, entity_key)
        ON DELETE CASCADE
) WITHOUT ROWID, STRICT;

INSERT INTO output_entity_meta_ranked(
    instance, collection, entity_key, block_number, block_timestamp,
    finality, written_at_unix_ms, value_bytes
)
SELECT
    instance, collection, entity_key, block_number, block_timestamp,
    CASE finality WHEN 2 THEN 3 ELSE finality END, written_at_unix_ms,
    value_bytes
FROM output_entity_meta;

DROP TABLE output_entity_meta;
ALTER TABLE output_entity_meta_ranked RENAME TO output_entity_meta;

CREATE INDEX output_entity_meta_by_block
    ON output_entity_meta(instance, collection, block_number, block_timestamp);
-- Partial indexes are usable only for the literal of their WHERE clause.
CREATE INDEX output_entity_meta_unattested
    ON output_entity_meta(instance, block_number)
    WHERE finality = 0;
CREATE INDEX output_entity_meta_unfinalized
    ON output_entity_meta(instance, block_number)
    WHERE finality < 3;

CREATE TABLE query_snapshot_entities_ranked (
    snapshot_id BLOB NOT NULL
        REFERENCES query_snapshots(snapshot_id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL,
    entity_key BLOB NOT NULL,
    value BLOB NOT NULL,
    block_number INTEGER NOT NULL,
    block_timestamp INTEGER NOT NULL,
    finality INTEGER NOT NULL CHECK (finality IN (0, 1, 2, 3)),
    PRIMARY KEY (snapshot_id, ordinal),
    UNIQUE (snapshot_id, entity_key)
) WITHOUT ROWID, STRICT;

INSERT INTO query_snapshot_entities_ranked(
    snapshot_id, ordinal, entity_key, value, block_number, block_timestamp,
    finality
)
SELECT
    snapshot_id, ordinal, entity_key, value, block_number, block_timestamp,
    CASE finality WHEN 2 THEN 3 ELSE finality END
FROM query_snapshot_entities;

DROP TABLE query_snapshot_entities;
ALTER TABLE query_snapshot_entities_ranked RENAME TO query_snapshot_entities;

UPDATE processor_cursors SET finality = 3 WHERE finality = 2;
UPDATE canonical_blocks SET finality = 3 WHERE finality = 2;

DROP INDEX processor_coverage_finalized;
UPDATE processor_coverage SET finality = 3 WHERE finality = 2;
-- Finalized exact coverage only; 3 is the stored Finalized finality.
CREATE INDEX processor_coverage_finalized
    ON processor_coverage(instance, block_number)
    WHERE finality = 3;
-- Rows below confirmed only. Confirmed promotion runs every slot, and this
-- holds the few rows it promotes instead of every row at or below the block.
CREATE INDEX IF NOT EXISTS processor_coverage_unconfirmed
    ON processor_coverage(instance, block_number)
    WHERE finality < 2;

UPDATE node_meta SET value = X'0000001E' WHERE key = 'schema_version';
