CREATE TABLE IF NOT EXISTS archive_reconciliations (
    reconciliation_id TEXT PRIMARY KEY NOT NULL,
    chain_id INTEGER NOT NULL,
    processor_instance TEXT NOT NULL
        REFERENCES processor_instances(instance) ON DELETE CASCADE,
    source_id TEXT NOT NULL,
    overlap_from INTEGER NOT NULL,
    overlap_to INTEGER NOT NULL,
    anchor_hash BLOB NOT NULL CHECK (length(anchor_hash) = 32),
    state TEXT NOT NULL CHECK (state IN ('running', 'verified', 'failed')),
    compared_blocks INTEGER NOT NULL DEFAULT 0,
    failure TEXT,
    updated_at_unix_ms INTEGER NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS archive_reconciliations_processor
ON archive_reconciliations(processor_instance, overlap_to);

CREATE INDEX IF NOT EXISTS archive_reconciliations_source
ON archive_reconciliations(processor_instance, source_id, overlap_to);

UPDATE node_meta
SET value = X'00000005'
WHERE key = 'schema_version';
