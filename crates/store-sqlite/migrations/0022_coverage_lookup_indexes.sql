-- Coverage probes behind health, metrics, and status reads become single
-- index searches instead of walking every exact coverage row or proof
-- segment of an instance.
CREATE INDEX processor_coverage_by_hash
    ON processor_coverage(instance, block_hash);

-- Finalized exact coverage only; 2 is the stored Finalized finality. Included
-- rows, which grow without bound while finality is disabled, stay out.
CREATE INDEX processor_coverage_finalized
    ON processor_coverage(instance, block_number)
    WHERE finality = 2;

CREATE INDEX finalized_coverage_segments_by_hash
    ON finalized_coverage_segments(instance, end_hash, segment_end);

UPDATE node_meta SET value = X'00000016' WHERE key = 'schema_version';
