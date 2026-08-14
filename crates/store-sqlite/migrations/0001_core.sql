CREATE TABLE IF NOT EXISTS node_meta (
    key TEXT PRIMARY KEY NOT NULL,
    value BLOB NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS processor_instances (
    instance TEXT PRIMARY KEY NOT NULL,
    processor_id TEXT NOT NULL,
    processor_version TEXT NOT NULL,
    code_hash BLOB NOT NULL CHECK (length(code_hash) = 32),
    config_hash BLOB NOT NULL CHECK (length(config_hash) = 32),
    descriptor_json TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1)),
    created_at_unix_ms INTEGER NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS processor_cursors (
    instance TEXT PRIMARY KEY NOT NULL
        REFERENCES processor_instances(instance) ON DELETE CASCADE,
    cursor TEXT NOT NULL,
    block_number INTEGER NOT NULL,
    block_hash BLOB NOT NULL CHECK (length(block_hash) = 32),
    finality INTEGER NOT NULL,
    sequence INTEGER NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS entities (
    instance TEXT NOT NULL
        REFERENCES processor_instances(instance) ON DELETE CASCADE,
    collection TEXT NOT NULL,
    entity_key BLOB NOT NULL,
    value BLOB NOT NULL,
    PRIMARY KEY (instance, collection, entity_key)
) WITHOUT ROWID, STRICT;

CREATE TABLE IF NOT EXISTS entity_indexes (
    instance TEXT NOT NULL
        REFERENCES processor_instances(instance) ON DELETE CASCADE,
    index_name TEXT NOT NULL,
    index_key BLOB NOT NULL,
    entity_key BLOB NOT NULL,
    PRIMARY KEY (instance, index_name, index_key, entity_key)
) WITHOUT ROWID, STRICT;

CREATE TABLE IF NOT EXISTS pending_deltas (
    instance TEXT NOT NULL
        REFERENCES processor_instances(instance) ON DELETE CASCADE,
    block_number INTEGER NOT NULL,
    block_hash BLOB NOT NULL CHECK (length(block_hash) = 32),
    encoded_delta BLOB NOT NULL,
    inserted_at_unix_ms INTEGER NOT NULL,
    PRIMARY KEY (instance, block_number, block_hash)
) WITHOUT ROWID, STRICT;

CREATE TABLE IF NOT EXISTS canonical_blocks (
    chain_id INTEGER NOT NULL,
    block_number INTEGER NOT NULL,
    block_hash BLOB NOT NULL CHECK (length(block_hash) = 32),
    parent_hash BLOB NOT NULL CHECK (length(parent_hash) = 32),
    timestamp INTEGER NOT NULL,
    finality INTEGER NOT NULL,
    PRIMARY KEY (chain_id, block_number)
) WITHOUT ROWID, STRICT;

CREATE TABLE IF NOT EXISTS recent_blocks (
    chain_id INTEGER NOT NULL,
    block_number INTEGER NOT NULL,
    block_hash BLOB NOT NULL CHECK (length(block_hash) = 32),
    encoded_frame BLOB NOT NULL,
    bytes INTEGER NOT NULL,
    PRIMARY KEY (chain_id, block_number, block_hash)
) WITHOUT ROWID, STRICT;

CREATE TABLE IF NOT EXISTS processor_coverage (
    instance TEXT NOT NULL
        REFERENCES processor_instances(instance) ON DELETE CASCADE,
    block_number INTEGER NOT NULL,
    block_hash BLOB NOT NULL CHECK (length(block_hash) = 32),
    finality INTEGER NOT NULL,
    PRIMARY KEY (instance, block_number)
) WITHOUT ROWID, STRICT;

CREATE TABLE IF NOT EXISTS applied_blocks (
    instance TEXT NOT NULL
        REFERENCES processor_instances(instance) ON DELETE CASCADE,
    block_number INTEGER NOT NULL,
    block_hash BLOB NOT NULL CHECK (length(block_hash) = 32),
    delta_checksum BLOB NOT NULL CHECK (length(delta_checksum) = 32),
    applied_at_unix_ms INTEGER NOT NULL,
    PRIMARY KEY (instance, block_number, block_hash)
) WITHOUT ROWID, STRICT;

CREATE TABLE IF NOT EXISTS undo_journal (
    instance TEXT NOT NULL
        REFERENCES processor_instances(instance) ON DELETE CASCADE,
    block_number INTEGER NOT NULL,
    block_hash BLOB NOT NULL CHECK (length(block_hash) = 32),
    encoded_undo BLOB NOT NULL,
    finalized INTEGER NOT NULL DEFAULT 0 CHECK (finalized IN (0, 1)),
    PRIMARY KEY (instance, block_number, block_hash)
) WITHOUT ROWID, STRICT;

CREATE TABLE IF NOT EXISTS change_log (
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
    created_at_unix_ms INTEGER NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS change_log_instance_sequence
    ON change_log(instance, sequence);

CREATE TABLE IF NOT EXISTS consumer_leases (
    consumer_id TEXT PRIMARY KEY NOT NULL,
    instance TEXT NOT NULL
        REFERENCES processor_instances(instance) ON DELETE CASCADE,
    acknowledged_sequence INTEGER NOT NULL,
    expires_at_unix_ms INTEGER NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS sink_outbox (
    sink_id TEXT NOT NULL,
    change_sequence INTEGER NOT NULL
        REFERENCES change_log(sequence) ON DELETE CASCADE,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_unix_ms INTEGER NOT NULL DEFAULT 0,
    delivered_at_unix_ms INTEGER,
    PRIMARY KEY (sink_id, change_sequence)
) WITHOUT ROWID, STRICT;

CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY NOT NULL,
    kind TEXT NOT NULL,
    state TEXT NOT NULL,
    payload BLOB NOT NULL,
    checkpoint BLOB,
    attempts INTEGER NOT NULL DEFAULT 0,
    updated_at_unix_ms INTEGER NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS source_ranges (
    source_id TEXT NOT NULL,
    chain_id INTEGER NOT NULL,
    from_block INTEGER NOT NULL,
    to_block INTEGER NOT NULL,
    provenance BLOB NOT NULL,
    PRIMARY KEY (source_id, chain_id, from_block, to_block)
) WITHOUT ROWID, STRICT;

INSERT INTO node_meta(key, value)
VALUES ('schema_version', X'00000001')
ON CONFLICT(key) DO NOTHING;
