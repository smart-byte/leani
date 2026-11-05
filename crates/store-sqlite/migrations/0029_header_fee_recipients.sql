-- Block frame schema 2 adds each header's fee recipient. The same
-- transaction re-encodes the retained recent frames, reading it from their
-- header RLP.
UPDATE node_meta SET value = X'0000001D' WHERE key = 'schema_version';
