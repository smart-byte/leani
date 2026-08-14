ALTER TABLE consumer_leases RENAME TO legacy_consumer_leases;

CREATE TABLE durable_consumers (
    instance TEXT NOT NULL
        REFERENCES processor_instances(instance) ON DELETE CASCADE,
    consumer_id TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('required', 'best_effort')),
    state TEXT NOT NULL CHECK (state IN ('active', 'reset_required', 'revoked')),
    acknowledged_sequence INTEGER NOT NULL,
    delivered_sequence INTEGER NOT NULL,
    lease_ttl_ms INTEGER NOT NULL CHECK (lease_ttl_ms > 0),
    lease_expires_at_unix_ms INTEGER NOT NULL,
    created_at_unix_ms INTEGER NOT NULL,
    updated_at_unix_ms INTEGER NOT NULL,
    PRIMARY KEY (instance, consumer_id),
    CHECK (acknowledged_sequence <= delivered_sequence)
) WITHOUT ROWID, STRICT;

CREATE TABLE delivery_streams (
    instance TEXT PRIMARY KEY NOT NULL
        REFERENCES processor_instances(instance) ON DELETE CASCADE,
    pruned_through_sequence INTEGER NOT NULL DEFAULT 0,
    live_bytes INTEGER NOT NULL DEFAULT 0,
    format_version INTEGER NOT NULL DEFAULT 1 CHECK (format_version = 1)
) WITHOUT ROWID, STRICT;

INSERT INTO delivery_streams(instance, live_bytes)
SELECT
    instance,
    (
        SELECT COALESCE(SUM(length(entity_key) + length(payload)), 0)
        FROM change_log
        WHERE change_log.instance = processor_instances.instance
    )
FROM processor_instances;

CREATE TABLE processor_runtime_state (
    instance TEXT PRIMARY KEY NOT NULL
        REFERENCES processor_instances(instance) ON DELETE CASCADE,
    state TEXT NOT NULL CHECK (state IN ('running', 'paused', 'failed')),
    reason TEXT,
    updated_at_unix_ms INTEGER NOT NULL
) WITHOUT ROWID, STRICT;

INSERT INTO processor_runtime_state(instance, state, updated_at_unix_ms)
SELECT instance, 'running', 0 FROM processor_instances;

INSERT INTO durable_consumers(
    instance, consumer_id, role, state, acknowledged_sequence,
    delivered_sequence, lease_ttl_ms, lease_expires_at_unix_ms,
    created_at_unix_ms, updated_at_unix_ms
)
SELECT
    instance, consumer_id, 'required', 'active', acknowledged_sequence,
    acknowledged_sequence, 300000, expires_at_unix_ms,
    expires_at_unix_ms - 300000, expires_at_unix_ms - 300000
FROM legacy_consumer_leases;

DROP TABLE legacy_consumer_leases;

ALTER TABLE change_log
    ADD COLUMN encoding_version INTEGER NOT NULL DEFAULT 1
    CHECK (encoding_version = 1);

UPDATE node_meta
SET value = X'00000006'
WHERE key = 'schema_version';
