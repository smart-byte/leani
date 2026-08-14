ALTER TABLE processor_coverage
    ADD COLUMN parent_hash BLOB CHECK(parent_hash IS NULL OR length(parent_hash) = 32);

UPDATE node_meta
SET value = X'0000000a'
WHERE key = 'schema_version';
