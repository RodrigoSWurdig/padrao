-- ============================================================================
-- ENG-1914 Infrastructure: Migration Metadata Tracking Table
-- ============================================================================

DROP TABLE IF EXISTS derived.eng1914_migration_metadata CASCADE;

CREATE TABLE derived.eng1914_migration_metadata (
  id BIGINT IDENTITY(1,1) PRIMARY KEY,
  migration_step VARCHAR(255) NOT NULL,
  records_affected BIGINT,
  status VARCHAR(50) NOT NULL,
  notes VARCHAR(65535),
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)
DISTSTYLE AUTO
SORTKEY (created_at);

INSERT INTO derived.eng1914_migration_metadata
(migration_step, records_affected, status, notes)
VALUES (
  'metadata_table_created',
  0,
  'success',
  'Migration tracking infrastructure established'
);
