-- A rollup's settled head: its settlement contracts accepted a claim
-- covering this block. Stored blocks and outputs stay finalized; a finalized
-- block at or below the head is settled.
CREATE TABLE chain_settlement (
    chain_id INTEGER PRIMARY KEY NOT NULL,
    block_number INTEGER NOT NULL,
    block_hash BLOB NOT NULL CHECK (length(block_hash) = 32)
) STRICT;

-- How far each processor instance's consumers have heard of settlement.
CREATE TABLE processor_settlement (
    instance TEXT PRIMARY KEY NOT NULL
        REFERENCES processor_instances(instance) ON DELETE CASCADE,
    block_number INTEGER NOT NULL
) WITHOUT ROWID, STRICT;

UPDATE node_meta SET value = X'0000001C' WHERE key = 'schema_version';
