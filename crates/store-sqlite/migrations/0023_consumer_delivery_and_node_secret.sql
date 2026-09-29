-- Keys for consumer session tokens and consumer-credential hashes derive from
-- one secret per store. The migration to this schema fills it once from the
-- operating system's random source, in the same transaction, and keys the
-- credential hashes earlier schemas stored bare; nothing rewrites it later.
CREATE TABLE node_secrets (
    name TEXT PRIMARY KEY NOT NULL,
    value BLOB NOT NULL CHECK (length(value) = 32)
) WITHOUT ROWID, STRICT;

-- Acknowledgements may no longer pass what was delivered to a consumer, and
-- delivery now records that watermark. Consumers may hold changes delivered
-- before this upgrade, so their watermark starts at the stream head.
UPDATE durable_consumers
SET delivered_sequence = MAX(
    delivered_sequence,
    COALESCE(
        (
            SELECT MAX(stream_sequence) FROM change_log
            WHERE change_log.stream_id = durable_consumers.stream_id
        ),
        (
            SELECT pruned_through_sequence FROM delivery_streams
            WHERE delivery_streams.stream_id = durable_consumers.stream_id
        ),
        0
    )
);

UPDATE node_meta SET value = X'00000017' WHERE key = 'schema_version';
