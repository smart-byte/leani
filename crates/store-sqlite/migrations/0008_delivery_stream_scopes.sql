ALTER TABLE durable_consumers RENAME TO legacy_durable_consumers;

ALTER TABLE delivery_streams RENAME TO legacy_delivery_streams;

CREATE TABLE delivery_streams (
    stream_id TEXT PRIMARY KEY NOT NULL,
    instance TEXT NOT NULL
        REFERENCES processor_instances(instance) ON DELETE CASCADE,
    stream_kind TEXT NOT NULL
        CHECK (stream_kind IN ('canonical', 'live', 'backfill')),
    subscription_id TEXT,
    next_sequence INTEGER NOT NULL DEFAULT 1 CHECK (next_sequence > 0),
    pruned_through_sequence INTEGER NOT NULL DEFAULT 0,
    live_bytes INTEGER NOT NULL DEFAULT 0,
    format_version INTEGER NOT NULL DEFAULT 1 CHECK (format_version = 1),
    created_at_unix_ms INTEGER NOT NULL,
    CHECK (
        (stream_kind = 'backfill' AND subscription_id IS NOT NULL)
        OR (stream_kind != 'backfill' AND subscription_id IS NULL)
    ),
    UNIQUE (instance, stream_kind, subscription_id)
) WITHOUT ROWID, STRICT;

INSERT INTO delivery_streams(
    stream_id, instance, stream_kind, subscription_id,
    next_sequence, pruned_through_sequence, live_bytes, format_version,
    created_at_unix_ms
)
SELECT
    instance || ':canonical', instance, 'canonical', NULL,
    COALESCE(
        (
            SELECT MAX(sequence) + 1
            FROM change_log
            WHERE change_log.instance = legacy_delivery_streams.instance
        ),
        1
    ),
    pruned_through_sequence, live_bytes, format_version, 0
FROM legacy_delivery_streams;

CREATE TABLE durable_consumers (
    stream_id TEXT NOT NULL
        REFERENCES delivery_streams(stream_id) ON DELETE CASCADE,
    instance TEXT NOT NULL
        REFERENCES processor_instances(instance) ON DELETE CASCADE,
    consumer_id TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('required', 'best_effort')),
    state TEXT NOT NULL CHECK (state IN ('active', 'reset_required', 'revoked')),
    acknowledged_sequence INTEGER NOT NULL,
    delivered_sequence INTEGER NOT NULL,
    acknowledged_work_blocks INTEGER NOT NULL DEFAULT 0,
    lease_ttl_ms INTEGER NOT NULL CHECK (lease_ttl_ms > 0),
    lease_expires_at_unix_ms INTEGER NOT NULL,
    lease_generation INTEGER NOT NULL DEFAULT 0,
    created_at_unix_ms INTEGER NOT NULL,
    updated_at_unix_ms INTEGER NOT NULL,
    credential_hash BLOB CHECK (credential_hash IS NULL OR length(credential_hash) = 32),
    PRIMARY KEY (stream_id, consumer_id),
    CHECK (acknowledged_sequence <= delivered_sequence)
) WITHOUT ROWID, STRICT;

INSERT INTO durable_consumers(
    stream_id, instance, consumer_id, role, state, acknowledged_sequence,
    delivered_sequence, lease_ttl_ms, lease_expires_at_unix_ms,
    created_at_unix_ms, updated_at_unix_ms, credential_hash
)
SELECT
    instance || ':canonical', instance, consumer_id, role, state, acknowledged_sequence,
    delivered_sequence, lease_ttl_ms, lease_expires_at_unix_ms,
    created_at_unix_ms, updated_at_unix_ms, credential_hash
FROM legacy_durable_consumers;

DROP TABLE legacy_durable_consumers;
DROP TABLE legacy_delivery_streams;

CREATE INDEX durable_consumers_instance
    ON durable_consumers(instance, consumer_id);

ALTER TABLE change_log
    ADD COLUMN stream_id TEXT NOT NULL DEFAULT '';

ALTER TABLE change_log
    ADD COLUMN stream_sequence INTEGER NOT NULL DEFAULT 0;

UPDATE change_log
SET stream_id = instance || ':canonical',
    stream_sequence = sequence;

CREATE INDEX change_log_stream_sequence
    ON change_log(stream_id, stream_sequence);

CREATE UNIQUE INDEX change_log_stream_sequence_unique
    ON change_log(stream_id, stream_sequence);

CREATE UNIQUE INDEX change_log_backfill_completion
    ON change_log(stream_id)
    WHERE kind = 'system.backfill_complete';

CREATE TABLE backfill_subscriptions (
    subscription_id TEXT PRIMARY KEY NOT NULL,
    job_id TEXT NOT NULL UNIQUE,
    instance TEXT NOT NULL
        REFERENCES processor_instances(instance) ON DELETE CASCADE,
    history_stream_id TEXT NOT NULL UNIQUE
        REFERENCES delivery_streams(stream_id) ON DELETE CASCADE,
    mode TEXT NOT NULL CHECK (mode IN ('fill_missing', 'recompute')),
    publication_revision INTEGER NOT NULL DEFAULT 0,
    live_stream_id TEXT
        REFERENCES delivery_streams(stream_id) ON DELETE RESTRICT,
    live_merge_cut_sequence INTEGER,
    initial_sequence INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL CHECK (
        state IN (
            'waiting_for_consumer', 'queued', 'running', 'backpressured',
            'draining', 'complete_reclaimable', 'cancelled', 'failed'
        )
    ),
    consumer_id TEXT NOT NULL,
    from_block INTEGER NOT NULL,
    to_block INTEGER NOT NULL,
    captured_finalized_target INTEGER NOT NULL,
    idempotency_key TEXT NOT NULL,
    effective_block_limit INTEGER NOT NULL CHECK (effective_block_limit > 0),
    effective_byte_limit INTEGER NOT NULL CHECK (effective_byte_limit > 0),
    resume_below_ratio_millionths INTEGER NOT NULL
        CHECK (
            resume_below_ratio_millionths > 0
            AND resume_below_ratio_millionths < 1000000
        ),
    completion_sequence INTEGER,
    processed_work_blocks INTEGER NOT NULL DEFAULT 0,
    created_at_unix_ms INTEGER NOT NULL,
    updated_at_unix_ms INTEGER NOT NULL,
    last_error TEXT,
    UNIQUE (instance, idempotency_key),
    CHECK (from_block <= to_block),
    CHECK (to_block <= captured_finalized_target)
) WITHOUT ROWID, STRICT;

CREATE INDEX backfill_subscriptions_instance_state
    ON backfill_subscriptions(instance, state, created_at_unix_ms);

CREATE TABLE backfill_subscription_ranges (
    subscription_id TEXT NOT NULL
        REFERENCES backfill_subscriptions(subscription_id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL,
    from_block INTEGER NOT NULL,
    to_block INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (
        state IN ('queued', 'running', 'committed', 'cancelled', 'failed')
    ),
    committed_blocks INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (subscription_id, ordinal),
    CHECK (from_block <= to_block)
) WITHOUT ROWID, STRICT;

UPDATE node_meta
SET value = X'00000008'
WHERE key = 'schema_version';
