CREATE TABLE processor_artifact_bulk_accounting (
    instance TEXT PRIMARY KEY
        REFERENCES processor_instances(instance) ON DELETE CASCADE
) WITHOUT ROWID, STRICT;

DROP TRIGGER processor_artifact_totals_artifact_insert;
DROP TRIGGER processor_artifact_totals_artifact_delete;
DROP TRIGGER processor_artifact_totals_owner_insert;
DROP TRIGGER processor_artifact_totals_owner_delete;

CREATE TRIGGER processor_artifact_totals_artifact_insert
AFTER INSERT ON processor_artifacts
WHEN NOT EXISTS (
    SELECT 1 FROM processor_artifact_bulk_accounting WHERE instance = NEW.instance
)
BEGIN
    UPDATE processor_artifact_totals
    SET retained_artifacts = retained_artifacts + 1,
        retained_bytes = retained_bytes + NEW.encoded_bytes
    WHERE instance = NEW.instance;
END;

CREATE TRIGGER processor_artifact_totals_artifact_delete
AFTER DELETE ON processor_artifacts
WHEN NOT EXISTS (
    SELECT 1 FROM processor_artifact_bulk_accounting WHERE instance = OLD.instance
)
BEGIN
    UPDATE processor_artifact_totals
    SET retained_artifacts = retained_artifacts - 1,
        retained_bytes = retained_bytes - OLD.encoded_bytes
    WHERE instance = OLD.instance;
END;

CREATE TRIGGER processor_artifact_totals_owner_insert
AFTER INSERT ON processor_artifact_owners
WHEN NOT EXISTS (
    SELECT 1 FROM processor_artifact_bulk_accounting WHERE instance = NEW.instance
)
BEGIN
    UPDATE processor_artifact_totals
    SET retained_owners = retained_owners + 1
    WHERE instance = NEW.instance;
END;

CREATE TRIGGER processor_artifact_totals_owner_delete
AFTER DELETE ON processor_artifact_owners
WHEN NOT EXISTS (
    SELECT 1 FROM processor_artifact_bulk_accounting WHERE instance = OLD.instance
)
BEGIN
    UPDATE processor_artifact_totals
    SET retained_owners = retained_owners - 1
    WHERE instance = OLD.instance;
END;

UPDATE node_meta
SET value = X'00000013'
WHERE key = 'schema_version';
