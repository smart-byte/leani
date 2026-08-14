CREATE TABLE IF NOT EXISTS recent_transaction_locator (
    chain_id INTEGER NOT NULL,
    transaction_hash BLOB NOT NULL CHECK (length(transaction_hash) = 32),
    block_number INTEGER NOT NULL,
    block_hash BLOB NOT NULL CHECK (length(block_hash) = 32),
    transaction_index INTEGER NOT NULL,
    PRIMARY KEY (chain_id, transaction_hash)
) WITHOUT ROWID, STRICT;

CREATE INDEX IF NOT EXISTS recent_transaction_locator_block
ON recent_transaction_locator(chain_id, block_number, block_hash);

UPDATE node_meta
SET value = X'00000003'
WHERE key = 'schema_version';
