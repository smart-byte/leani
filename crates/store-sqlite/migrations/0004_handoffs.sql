CREATE TABLE IF NOT EXISTS hot_cold_handoffs (
    handoff_id TEXT PRIMARY KEY NOT NULL,
    chain_id INTEGER NOT NULL,
    processor_instance TEXT NOT NULL
        REFERENCES processor_instances(instance) ON DELETE CASCADE,
    overlap_from INTEGER NOT NULL,
    overlap_to INTEGER NOT NULL,
    anchor_hash BLOB NOT NULL CHECK (length(anchor_hash) = 32),
    state TEXT NOT NULL CHECK (state IN ('running', 'verified', 'failed')),
    compared_blocks INTEGER NOT NULL DEFAULT 0,
    failure TEXT,
    updated_at_unix_ms INTEGER NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS hot_cold_handoffs_processor
ON hot_cold_handoffs(processor_instance, overlap_to);

UPDATE node_meta
SET value = X'00000004'
WHERE key = 'schema_version';
