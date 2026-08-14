ALTER TABLE change_log
    ADD COLUMN origin_kind TEXT NOT NULL DEFAULT 'live'
    CHECK (origin_kind IN ('live', 'live_recovery', 'historical_backfill', 'recompute'));

ALTER TABLE change_log
    ADD COLUMN origin_id TEXT NOT NULL DEFAULT '';

ALTER TABLE change_log
    ADD COLUMN publication_revision INTEGER NOT NULL DEFAULT 0
    CHECK (publication_revision >= 0);

UPDATE change_log
SET origin_id = instance;

UPDATE change_log
SET origin_kind = CASE
        WHEN (
            SELECT mode FROM backfill_subscriptions
            WHERE history_stream_id = change_log.stream_id
        ) = 'recompute' THEN 'recompute'
        ELSE 'historical_backfill'
    END,
    origin_id = COALESCE(
        (
            SELECT subscription_id FROM backfill_subscriptions
            WHERE history_stream_id = change_log.stream_id
        ),
        instance
    ),
    publication_revision = COALESCE(
        (
            SELECT publication_revision FROM backfill_subscriptions
            WHERE history_stream_id = change_log.stream_id
        ),
        0
    )
WHERE stream_id IN (
    SELECT history_stream_id FROM backfill_subscriptions
);

CREATE INDEX change_log_stream_origin_sequence
    ON change_log(stream_id, origin_kind, origin_id, publication_revision, stream_sequence);

UPDATE node_meta
SET value = X'0000000C'
WHERE key = 'schema_version';
