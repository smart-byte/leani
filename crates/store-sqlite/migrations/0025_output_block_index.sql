-- Retained output in block order per collection: retained bounds, output
-- window pruning, and pages of a block range become index searches instead of
-- scans of every row an instance retains. The block timestamp, which follows
-- the block number, makes those searches read the index alone.
CREATE INDEX IF NOT EXISTS output_entity_meta_by_block
    ON output_entity_meta(instance, collection, block_number, block_timestamp);

UPDATE node_meta SET value = X'00000019' WHERE key = 'schema_version';
