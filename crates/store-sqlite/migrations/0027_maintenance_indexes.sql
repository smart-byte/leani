-- Delivery pruning finds the newest change past its age and the changes of a
-- block by index search instead of walking every change a stream retains.
CREATE INDEX IF NOT EXISTS change_log_stream_created
    ON change_log(stream_id, created_at_unix_ms, stream_sequence);

CREATE INDEX IF NOT EXISTS change_log_stream_block
    ON change_log(stream_id, block_number, stream_sequence);

-- change_log_stream_sequence_unique holds the same keys.
DROP INDEX IF EXISTS change_log_stream_sequence;

-- Unfinalized rows only; 2 is the stored Finalized finality. History arrives
-- finalized, so finality promotion, which holds the writer, searches the few
-- blocks above the finalized head instead of the whole output window.
CREATE INDEX IF NOT EXISTS output_entity_meta_unfinalized
    ON output_entity_meta(instance, block_number)
    WHERE finality < 2;

CREATE INDEX IF NOT EXISTS undo_journal_unfinalized
    ON undo_journal(instance, block_number)
    WHERE finalized = 0;

-- Sizes and bounds of retained frames without reading the frames: in this
-- WITHOUT ROWID table a range search loads every whole record it passes.
CREATE INDEX IF NOT EXISTS recent_blocks_sizes
    ON recent_blocks(chain_id, block_number, block_hash, bytes);

UPDATE node_meta SET value = X'0000001B' WHERE key = 'schema_version';
