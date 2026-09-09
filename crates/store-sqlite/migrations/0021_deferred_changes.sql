-- Hold included-block changes of finalized_only processors until verified
-- finality promotes them into the change log. bytes is the delivery-budget
-- measure (key + payload length) charged at admission.
CREATE TABLE deferred_changes (
    id INTEGER PRIMARY KEY,
    instance TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    chain_id INTEGER NOT NULL,
    block_number INTEGER NOT NULL,
    block_hash BLOB NOT NULL,
    block BLOB NOT NULL,
    origin BLOB NOT NULL,
    changes BLOB NOT NULL,
    sink_ids BLOB NOT NULL,
    bytes INTEGER NOT NULL
);
CREATE INDEX deferred_changes_instance_block
    ON deferred_changes(instance, block_number, id);
CREATE INDEX deferred_changes_stream
    ON deferred_changes(stream_id);

UPDATE node_meta SET value = X'00000015' WHERE key = 'schema_version';
