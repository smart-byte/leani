CREATE TABLE processor_artifact_totals (
    instance TEXT PRIMARY KEY
        REFERENCES processor_instances(instance) ON DELETE CASCADE,
    retained_artifacts INTEGER NOT NULL DEFAULT 0 CHECK(retained_artifacts >= 0),
    retained_bytes INTEGER NOT NULL DEFAULT 0 CHECK(retained_bytes >= 0),
    retained_owners INTEGER NOT NULL DEFAULT 0 CHECK(retained_owners >= 0),
    pending_artifacts INTEGER NOT NULL DEFAULT 0 CHECK(pending_artifacts >= 0),
    pending_bytes INTEGER NOT NULL DEFAULT 0 CHECK(pending_bytes >= 0)
) WITHOUT ROWID, STRICT;

INSERT INTO processor_artifact_totals(
    instance, retained_artifacts, retained_bytes, retained_owners,
    pending_artifacts, pending_bytes
)
SELECT
    processors.instance,
    (SELECT COUNT(*) FROM processor_artifacts AS artifacts
     WHERE artifacts.instance = processors.instance) +
      (SELECT COALESCE(SUM(segments.artifacts), 0)
       FROM processor_artifact_segments AS segments
       WHERE segments.instance = processors.instance AND segments.state = 'active'),
    (SELECT COALESCE(SUM(artifacts.encoded_bytes), 0)
     FROM processor_artifacts AS artifacts
     WHERE artifacts.instance = processors.instance) +
      (SELECT COALESCE(SUM(segments.logical_bytes), 0)
       FROM processor_artifact_segments AS segments
       WHERE segments.instance = processors.instance AND segments.state = 'active'),
    (SELECT COUNT(*) FROM processor_artifact_owners AS owners
     WHERE owners.instance = processors.instance) +
      (SELECT COALESCE(SUM(owners.to_block - owners.from_block + 1), 0)
       FROM processor_artifact_segment_owners AS owners
       JOIN processor_artifact_segments AS segments
         ON segments.segment_id = owners.segment_id
       WHERE segments.instance = processors.instance AND segments.state = 'active'),
    (SELECT COUNT(*) FROM processor_artifact_candidates AS candidates
     WHERE candidates.instance = processors.instance),
    (SELECT COALESCE(SUM(length(candidates.encoded_delta)), 0)
     FROM processor_artifact_candidates AS candidates
     WHERE candidates.instance = processors.instance)
FROM processor_instances AS processors;

CREATE TRIGGER processor_artifact_totals_processor_insert
AFTER INSERT ON processor_instances
BEGIN
    INSERT OR IGNORE INTO processor_artifact_totals(instance) VALUES (NEW.instance);
END;

CREATE TRIGGER processor_artifact_totals_artifact_insert
AFTER INSERT ON processor_artifacts
BEGIN
    UPDATE processor_artifact_totals
    SET retained_artifacts = retained_artifacts + 1,
        retained_bytes = retained_bytes + NEW.encoded_bytes
    WHERE instance = NEW.instance;
END;

CREATE TRIGGER processor_artifact_totals_artifact_delete
AFTER DELETE ON processor_artifacts
BEGIN
    UPDATE processor_artifact_totals
    SET retained_artifacts = retained_artifacts - 1,
        retained_bytes = retained_bytes - OLD.encoded_bytes
    WHERE instance = OLD.instance;
END;

CREATE TRIGGER processor_artifact_totals_candidate_insert
AFTER INSERT ON processor_artifact_candidates
BEGIN
    UPDATE processor_artifact_totals
    SET pending_artifacts = pending_artifacts + 1,
        pending_bytes = pending_bytes + length(NEW.encoded_delta)
    WHERE instance = NEW.instance;
END;

CREATE TRIGGER processor_artifact_totals_candidate_delete
AFTER DELETE ON processor_artifact_candidates
BEGIN
    UPDATE processor_artifact_totals
    SET pending_artifacts = pending_artifacts - 1,
        pending_bytes = pending_bytes - length(OLD.encoded_delta)
    WHERE instance = OLD.instance;
END;

CREATE TRIGGER processor_artifact_totals_owner_insert
AFTER INSERT ON processor_artifact_owners
BEGIN
    UPDATE processor_artifact_totals
    SET retained_owners = retained_owners + 1
    WHERE instance = NEW.instance;
END;

CREATE TRIGGER processor_artifact_totals_owner_delete
AFTER DELETE ON processor_artifact_owners
BEGIN
    UPDATE processor_artifact_totals
    SET retained_owners = retained_owners - 1
    WHERE instance = OLD.instance;
END;

CREATE TRIGGER processor_artifact_totals_segment_insert
AFTER INSERT ON processor_artifact_segments
WHEN NEW.state = 'active'
BEGIN
    UPDATE processor_artifact_totals
    SET retained_artifacts = retained_artifacts + NEW.artifacts,
        retained_bytes = retained_bytes + NEW.logical_bytes
    WHERE instance = NEW.instance;
END;

CREATE TRIGGER processor_artifact_totals_segment_delete
AFTER DELETE ON processor_artifact_segments
WHEN OLD.state = 'active'
BEGIN
    UPDATE processor_artifact_totals
    SET retained_artifacts = retained_artifacts - OLD.artifacts,
        retained_bytes = retained_bytes - OLD.logical_bytes,
        retained_owners = retained_owners - COALESCE((
            SELECT SUM(to_block - from_block + 1)
            FROM processor_artifact_segment_owners
            WHERE segment_id = OLD.segment_id
        ), 0)
    WHERE instance = OLD.instance;
END;

CREATE TRIGGER processor_artifact_totals_segment_update
AFTER UPDATE OF state, artifacts, logical_bytes ON processor_artifact_segments
BEGIN
    UPDATE processor_artifact_totals
    SET retained_artifacts = retained_artifacts
          + CASE WHEN NEW.state = 'active' THEN NEW.artifacts ELSE 0 END
          - CASE WHEN OLD.state = 'active' THEN OLD.artifacts ELSE 0 END,
        retained_bytes = retained_bytes
          + CASE WHEN NEW.state = 'active' THEN NEW.logical_bytes ELSE 0 END
          - CASE WHEN OLD.state = 'active' THEN OLD.logical_bytes ELSE 0 END,
        retained_owners = retained_owners
          + (CASE WHEN NEW.state = 'active' THEN 1 ELSE 0 END
             - CASE WHEN OLD.state = 'active' THEN 1 ELSE 0 END) * COALESCE((
                SELECT SUM(to_block - from_block + 1)
                FROM processor_artifact_segment_owners
                WHERE segment_id = NEW.segment_id
             ), 0)
    WHERE instance = NEW.instance;
END;

CREATE TRIGGER processor_artifact_totals_segment_owner_insert
AFTER INSERT ON processor_artifact_segment_owners
WHEN (SELECT state FROM processor_artifact_segments
      WHERE segment_id = NEW.segment_id) = 'active'
BEGIN
    UPDATE processor_artifact_totals
    SET retained_owners = retained_owners + NEW.to_block - NEW.from_block + 1
    WHERE instance = (SELECT instance FROM processor_artifact_segments
                      WHERE segment_id = NEW.segment_id);
END;

CREATE TRIGGER processor_artifact_totals_segment_owner_delete
AFTER DELETE ON processor_artifact_segment_owners
WHEN (SELECT state FROM processor_artifact_segments
      WHERE segment_id = OLD.segment_id) = 'active'
BEGIN
    UPDATE processor_artifact_totals
    SET retained_owners = retained_owners - (OLD.to_block - OLD.from_block + 1)
    WHERE instance = (SELECT instance FROM processor_artifact_segments
                      WHERE segment_id = OLD.segment_id);
END;

UPDATE node_meta
SET value = X'00000012'
WHERE key = 'schema_version';
