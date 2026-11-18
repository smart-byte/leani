-- Each consumer keeps its lag: the count, distinct blocks, and key and payload
-- bytes of its stream's retained changes after its acknowledged sequence.
-- Appends, acknowledgements and pruning move the counters, so a read never
-- counts the backlog; this migration counts it once.
ALTER TABLE durable_consumers ADD COLUMN lag_changes INTEGER NOT NULL DEFAULT 0;
ALTER TABLE durable_consumers ADD COLUMN lag_blocks INTEGER NOT NULL DEFAULT 0;
ALTER TABLE durable_consumers ADD COLUMN lag_bytes INTEGER NOT NULL DEFAULT 0;

UPDATE durable_consumers
SET lag_changes = lag.changes, lag_blocks = lag.blocks, lag_bytes = lag.bytes
FROM (
    SELECT consumer.stream_id, consumer.consumer_id,
           COUNT(*) AS changes,
           COUNT(DISTINCT change.block_number) AS blocks,
           SUM(length(change.entity_key) + length(change.payload)) AS bytes
    FROM durable_consumers AS consumer
    JOIN change_log AS change
      ON change.stream_id = consumer.stream_id
     AND change.stream_sequence > consumer.acknowledged_sequence
    GROUP BY consumer.stream_id, consumer.consumer_id
) AS lag
WHERE durable_consumers.stream_id = lag.stream_id
  AND durable_consumers.consumer_id = lag.consumer_id;

UPDATE node_meta SET value = X'0000001F' WHERE key = 'schema_version';
