-- Unattested rows only; 0 is the stored Unattested finality. Attested
-- promotion runs every slot, and these hold the few blocks above the attested
-- head instead of every row at or below it.
CREATE INDEX IF NOT EXISTS processor_coverage_unattested
    ON processor_coverage(instance, block_number)
    WHERE finality = 0;

CREATE INDEX IF NOT EXISTS output_entity_meta_unattested
    ON output_entity_meta(instance, block_number)
    WHERE finality = 0;

CREATE INDEX IF NOT EXISTS canonical_blocks_unattested
    ON canonical_blocks(chain_id, block_number)
    WHERE finality = 0;

UPDATE node_meta SET value = X'0000001A' WHERE key = 'schema_version';
