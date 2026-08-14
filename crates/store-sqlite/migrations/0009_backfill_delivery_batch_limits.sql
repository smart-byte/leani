ALTER TABLE backfill_subscriptions
    ADD COLUMN delivery_target_encoded_bytes INTEGER NOT NULL DEFAULT 4194304;

ALTER TABLE backfill_subscriptions
    ADD COLUMN delivery_maximum_encoded_bytes INTEGER NOT NULL DEFAULT 16777216;

ALTER TABLE backfill_subscriptions
    ADD COLUMN delivery_maximum_events INTEGER NOT NULL DEFAULT 20000;

ALTER TABLE backfill_subscriptions
    ADD COLUMN delivery_maximum_processed_blocks INTEGER NOT NULL DEFAULT 8192;

ALTER TABLE backfill_subscriptions
    ADD COLUMN delivery_maximum_delay_ms INTEGER NOT NULL DEFAULT 50;

ALTER TABLE backfill_subscriptions
    ADD COLUMN delivery_maximum_buffered_batches INTEGER NOT NULL DEFAULT 4;

ALTER TABLE backfill_subscriptions
    ADD COLUMN delivery_maximum_buffered_bytes INTEGER NOT NULL DEFAULT 67108864;

ALTER TABLE backfill_subscriptions
    ADD COLUMN delivery_compression TEXT NOT NULL DEFAULT 'gzip';

CREATE TABLE historical_work_identities (
    job_id TEXT PRIMARY KEY NOT NULL
        REFERENCES jobs(job_id) ON DELETE CASCADE,
    request_identity_hash BLOB NOT NULL CHECK(length(request_identity_hash) = 32)
) WITHOUT ROWID, STRICT;

CREATE TABLE backfill_subscription_preexisting_ranges (
    subscription_id TEXT NOT NULL
        REFERENCES backfill_subscriptions(subscription_id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL,
    from_block INTEGER NOT NULL,
    to_block INTEGER NOT NULL,
    PRIMARY KEY (subscription_id, ordinal),
    CHECK (from_block <= to_block)
) WITHOUT ROWID, STRICT;

UPDATE node_meta
SET value = X'00000009'
WHERE key = 'schema_version';
