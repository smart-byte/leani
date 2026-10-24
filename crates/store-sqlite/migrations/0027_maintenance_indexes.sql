-- Delivery pruning finds the newest change past its age and the changes of a
-- block by index search instead of walking every change a stream retains.
CREATE INDEX IF NOT EXISTS change_log_stream_created
    ON change_log(stream_id, created_at_unix_ms, stream_sequence);

CREATE INDEX IF NOT EXISTS change_log_stream_block
    ON change_log(stream_id, block_number, stream_sequence);

-- change_log_stream_sequence_unique holds the same keys.
DROP INDEX IF EXISTS change_log_stream_sequence;

UPDATE node_meta SET value = X'0000001B' WHERE key = 'schema_version';
