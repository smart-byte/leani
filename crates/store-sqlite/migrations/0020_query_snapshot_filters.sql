-- Keep the original selection with every immutable snapshot so later pages
-- report coverage for the same requested range.
ALTER TABLE query_snapshots ADD COLUMN from_block INTEGER;
ALTER TABLE query_snapshots ADD COLUMN to_block INTEGER;
ALTER TABLE query_snapshots ADD COLUMN from_timestamp INTEGER;
ALTER TABLE query_snapshots ADD COLUMN to_timestamp INTEGER;

UPDATE node_meta SET value = X'00000014' WHERE key = 'schema_version';
